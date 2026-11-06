#!/usr/bin/env bash
# Deterministic smoke for the Atrius-authored CDS services (synthetic prefetch,
# no chart seeding needed). Asserts card summaries / counts per service:
#
#   atrius-critical-labs            patient-view   critical potassium fires, normal K is silent
#   atrius-preventive-care          patient-view   colorectal screening + flu vaccine due
#   sepsis-bundle                   encounter-start SIRS x2 + infection -> hour-1 bundle
#   atrius-ddi-check                order-sign     active statin + draft clarithromycin
#   hf-admission-protocol           encounter-start HF condition -> bundle + BNP cards
#   atrius-imaging-appropriateness  order-select   acute LBP + draft lumbar MRI; red flag is silent
#   atrius-renal-dosing             order-sign     draft gabapentin + eGFR 25 -> adjusted dose
#   surgical-safety-checklist-rule  encounter-start surgical encounter without checklist QR fires; completed QR is silent
#   atrius-vte-prophylaxis          encounter-start adult inpatient without LMWH fires; with enoxaparin silent
#   atrius-ot-prophylaxis           encounter-start surgical enc without antibiotic fires; with ceftriaxone silent
#   atrius-discharge-readiness      patient-view    pending lab Task fires; empty blockers silent
#
# Prerequisites: KR (:8079), HTS (:9091), sidecar (:8088), clinical HFS (:8082),
# cds-server (:8095) with the Atrius KR content imported
# (import-atrius-kr-libraries.py --clinical-reasoning) and the manifest
# regenerated (generate-cds-hooks-manifest.py [--upload-binary-id ...]).
# Seeds Patient/{*-smoke} on clinical HFS (idempotent PUT) so CQL Patient-context
# expressions (AgeInYears) resolve.
#
# Usage:
#   ./scripts/cds-atrius-services-smoke.sh
#   CDS_SERVER_URL=http://127.0.0.1:8095 CLINICAL_HFS_URL=http://127.0.0.1:8082 ./scripts/cds-atrius-services-smoke.sh
#
# Auth (when clinical HFS has HFS_AUTH_ENABLED=true):
#   Sources cds-smoke-auth.sh — client_credentials against Keycloak, then seeds
#   Patients with Bearer + X-Tenant-ID and injects fhirAuthorization on invoke.
#   CDS_SMOKE_SKIP_AUTH=1 to skip (auth-disabled HFS only).
#
# Per-service wrappers: cds-atrius-vte-prophylaxis-smoke.sh,
# cds-atrius-ot-prophylaxis-smoke.sh, cds-atrius-discharge-readiness-smoke.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=cds-smoke-auth.sh
source "$SCRIPT_DIR/cds-smoke-auth.sh"

CDS="${CDS_SERVER_URL:-http://127.0.0.1:8095}"
CLINICAL="${CLINICAL_HFS_URL:-http://127.0.0.1:8082}"
cds="${CDS%/}"
clinical="${CLINICAL%/}"
PASS=0
FAIL=0

cds_smoke_auth_init || {
  echo "FATAL: could not obtain clinical HFS OAuth token (set CDS_SMOKE_* or CDS_SMOKE_SKIP_AUTH=1)" >&2
  exit 1
}
cds_smoke_preflight_search || {
  echo "FATAL: clinical Observation search is unavailable — fix Elasticsearch before re-running." >&2
  exit 1
}

# Empty ValueSet $expand results used to stick in the sidecar process cache and make
# ObservationVitalSigns retrieves (sepsis SIRS) silently empty. Clear at smoke start;
# sidecar ≥ current also refuses to cache empty expands.
SIDECAR_URL="${SIDECAR_URL:-http://127.0.0.1:8088}"
if curl -sS -o /dev/null -w '%{http_code}' -X POST "${SIDECAR_URL%/}/v1/admin/cache/libraries/clear" | grep -qE '200|204'; then
  echo "cds-smoke: cleared sidecar library/expand caches" >&2
fi

patient() {
  jq -n --arg id "$1" --arg birth "${2:-1960-01-01}" '{
    resourceType: "Patient", id: $id, gender: "unknown", birthDate: $birth,
    meta: {profile: ["https://atrius.in/fhir/r4/atrius-in/StructureDefinition/atrius-in-patient"]},
    identifier: [{system: "https://atrius.in/smoke", value: $id}],
    name: [{family: "Smoke", given: [$id]}]
  }'
}

# Sidecar resolves the CQL Patient context (e.g. AgeInYears) from the clinical
# server, not only prefetch — seed smoke patients idempotently.
seed_patient() { # id [birthDate]
  local code
  local -a auth=()
  if [[ -n "${CDS_SMOKE_ACCESS_TOKEN:-}" ]]; then
    auth=(-H "Authorization: Bearer ${CDS_SMOKE_ACCESS_TOKEN}" -H "X-Tenant-ID: ${CDS_SMOKE_TENANT_ID}")
  fi
  code="$(patient "$1" "${2:-1960-01-01}" | curl -sS -o /dev/null -w '%{http_code}' \
    -X PUT -H 'Content-Type: application/fhir+json' "${auth[@]}" --data-binary @- "$clinical/Patient/$1")"
  if [[ "$code" != "200" && "$code" != "201" ]]; then
    echo "FATAL: seeding Patient/$1 on $clinical returned HTTP $code (AgeInYears needs server birthDate)" >&2
    exit 1
  fi
}

bundle() { jq -n --argjson entries "${1:-[]}" '{resourceType: "Bundle", type: "searchset", entry: [$entries[] | {resource: .}]}'; }

lab_obs() { # id loinc display value unit ucum patient effective
  jq -n --arg id "$1" --arg code "$2" --arg disp "$3" --argjson val "$4" --arg unit "$5" --arg ucum "$6" --arg pt "$7" --arg eff "$8" '{
    resourceType: "Observation", id: $id, status: "final",
    meta: {profile: ["https://atrius.in/fhir/r4/atrius-in/StructureDefinition/atrius-in-observation"]},
    category: [{coding: [{system: "http://terminology.hl7.org/CodeSystem/observation-category", code: "laboratory"}]}],
    code: {coding: [{system: "http://loinc.org", code: $code, display: $disp}]},
    subject: {reference: ("Patient/" + $pt)},
    effectiveDateTime: $eff,
    valueQuantity: {value: $val, unit: $unit, system: "http://unitsofmeasure.org", code: $ucum}
  }'
}

# Sepsis CQL retrieves ObservationVitalSigns — stamp vital-signs profile/category.
vital_obs() {
  lab_obs "$@" | jq '
    .meta.profile = ["https://atrius.in/fhir/r4/atrius-in/StructureDefinition/atrius-in-observation-vital-signs"]
    | .category = [{coding: [{system: "http://terminology.hl7.org/CodeSystem/observation-category", code: "vital-signs"}]}]'
}

condition() { # id sct display patient onset
  jq -n --arg id "$1" --arg code "$2" --arg disp "$3" --arg pt "$4" --arg onset "$5" '{
    resourceType: "Condition", id: $id,
    meta: {profile: ["https://atrius.in/fhir/r4/atrius-in/StructureDefinition/atrius-in-condition-encounter-diagnosis"]},
    clinicalStatus: {coding: [{system: "http://terminology.hl7.org/CodeSystem/condition-clinical", code: "active"}]},
    verificationStatus: {coding: [{system: "http://terminology.hl7.org/CodeSystem/condition-ver-status", code: "confirmed"}]},
    category: [{coding: [{system: "http://terminology.hl7.org/CodeSystem/condition-category", code: "encounter-diagnosis"}]}],
    code: {coding: [{system: "http://snomed.info/sct", code: $code, display: $disp}]},
    subject: {reference: ("Patient/" + $pt)},
    onsetDateTime: $onset
  }'
}

med_request() { # sct display status patient
  jq -n --arg code "$1" --arg disp "$2" --arg status "$3" --arg pt "$4" '{
    resourceType: "MedicationRequest", status: $status, intent: "order",
    meta: {profile: ["https://atrius.in/fhir/r4/atrius-in/StructureDefinition/atrius-in-medicationrequest"]},
    medicationCodeableConcept: {coding: [{system: "http://snomed.info/sct", code: $code, display: $disp}], text: $disp},
    subject: {reference: ("Patient/" + $pt)},
    authoredOn: (now | todate),
    dosageInstruction: [{text: ($disp + " per protocol")}]
  }'
}

invoke() { # service payload
  local svc="$1" payload="$2" tmp code
  tmp="$(mktemp)"
  payload="$(printf '%s' "$payload" | cds_smoke_inject_fhir_auth)"
  code="$(curl -sS -o "$tmp" -w '%{http_code}' -X POST "$cds/cds-services/$svc" \
    -H 'Content-Type: application/json' -d "$payload")"
  if [[ "$code" != "200" ]]; then
    echo "  HTTP $code from $svc:" >&2
    cat "$tmp" >&2
    rm -f "$tmp"
    return 1
  fi
  cat "$tmp"
  rm -f "$tmp"
}

check() { # label jq-assertion response
  local label="$1" assertion="$2" response="$3"
  # cds-server emits "{title} — active" critical cards when $apply conditions error out.
  if echo "$response" | jq -e '[.cards[]?.summary // empty] | any(endswith(" — active"))' >/dev/null 2>&1; then
    echo "FAIL  $label (cds-server fallback card — clinical \$apply/condition likely failed: auth, ES, or prefetch)"
    echo "$response" | jq '[.cards[]? | {summary, indicator}]' >&2
    FAIL=$((FAIL + 1))
    return
  fi
  if echo "$response" | jq -e "$assertion" >/dev/null 2>&1; then
    echo "PASS  $label"
    PASS=$((PASS + 1))
  else
    echo "FAIL  $label"
    echo "$response" | jq '[.cards[]? | {summary, indicator}]' >&2
    FAIL=$((FAIL + 1))
  fi
}

now="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

# --- 1. Critical labs -------------------------------------------------------
pt=crit-smoke
seed_patient $pt
hi_k="$(lab_obs k1 2823-3 'Potassium [Moles/volume] in Serum or Plasma' 6.8 'mmol/L' 'mmol/L' $pt "$now")"
ok_k="$(lab_obs k2 2823-3 'Potassium [Moles/volume] in Serum or Plasma' 4.2 'mmol/L' 'mmol/L' $pt "$now")"
mk_crit() {
  jq -n --argjson patient "$(patient $pt)" --argjson obs "$(bundle "[$1]")" '{
    hook: "patient-view", hookInstance: "crit-smoke",
    context: {patientId: "'$pt'", userId: "Practitioner/smoke"},
    prefetch: {patient: $patient, "observation-1": $obs, observations: $obs}
  }'
}
resp="$(invoke atrius-critical-labs "$(mk_crit "$hi_k")")"
check "critical-labs: K 6.8 fires critical card" \
  '.cards | length >= 1 and (map(.summary) | any(test("potassium"; "i")))' "$resp"
resp="$(invoke atrius-critical-labs "$(mk_crit "$ok_k")")"
check "critical-labs: K 4.2 is silent" '.cards | length == 0' "$resp"

# --- 2. Preventive care -----------------------------------------------------
pt=prev-smoke
seed_patient $pt 1968-01-01
prev_payload="$(jq -n --argjson patient "$(patient $pt 1968-01-01)" --argjson empty "$(bundle '[]')" '{
  hook: "patient-view", hookInstance: "prev-smoke",
  context: {patientId: "'$pt'", userId: "Practitioner/smoke"},
  prefetch: {patient: $patient, "procedure-1": $empty, "observation-2": $empty, "immunization-3": $empty,
             procedures: $empty, observations: $empty, immunizations: $empty}
}')"
resp="$(invoke atrius-preventive-care "$prev_payload")"
check "preventive-care: 58yo with empty chart gets screening + flu cards" \
  '.cards | length >= 2' "$resp"

# --- 3. Sepsis bundle -------------------------------------------------------
pt=sepsis-smoke
seed_patient $pt
temp="$(vital_obs t1 8310-5 'Body temperature' 38.9 'Cel' 'Cel' $pt "$now")"
# UCUM display "{beats}/min" is more reliable for FHIRHelpers quantity compare than "/min".
hr="$(vital_obs h1 8867-4 'Heart rate' 118 '{beats}/min' '/min' $pt "$now")"
rr="$(vital_obs r1 9279-1 'Respiratory rate' 24 '{breaths}/min' '/min' $pt "$now")"
infection="$(condition c1 233604007 'Pneumonia' $pt "$now")"
sepsis_payload="$(jq -n \
  --argjson patient "$(patient $pt)" \
  --argjson obs "$(bundle "[$temp,$hr,$rr]")" \
  --argjson conds "$(bundle "[$infection]")" '{
  hook: "encounter-start", hookInstance: "sepsis-smoke",
  context: {patientId: "'$pt'", encounterId: "sepsis-smoke-enc", userId: "Practitioner/smoke"},
  prefetch: {patient: $patient, "observation-1": $obs, "condition-2": $conds,
             observations: $obs, conditions: $conds}
}')"
resp="$(invoke sepsis-bundle "$sepsis_payload")"
# One retry after cache clear — covers sidecars that still cache empty expands.
if ! echo "$resp" | jq -e '.cards | map(.summary) | any(test("Hour-1"; "i"))' >/dev/null 2>&1; then
  curl -sS -X POST "${SIDECAR_URL%/}/v1/admin/cache/libraries/clear" >/dev/null 2>&1 || true
  resp="$(invoke sepsis-bundle "$sepsis_payload")"
fi
check "sepsis: SIRS x2 + pneumonia fires hour-1 bundle" \
  '.cards | map(.summary) | any(test("Hour-1"; "i"))' "$resp"

# --- 4. DDI check -----------------------------------------------------------
pt=ddi-smoke
seed_patient $pt
statin="$(med_request 387584000 Simvastatin active $pt | jq '.id = "ddi-statin"')"
clarithro_draft="$(med_request 387487009 Clarithromycin draft $pt)"
ddi_payload="$(jq -n \
  --argjson patient "$(patient $pt)" \
  --argjson meds "$(bundle "[$statin]")" \
  --argjson draft "$clarithro_draft" '{
  hook: "order-sign", hookInstance: "ddi-smoke",
  context: {patientId: "'$pt'", userId: "Practitioner/smoke",
            draftOrders: {resourceType: "Bundle", entry: [{resource: $draft}]}},
  prefetch: {patient: $patient, "medicationrequest-1": $meds, medicationRequests: $meds}
}')"
resp="$(invoke atrius-ddi-check "$ddi_payload")"
check "ddi: statin + draft clarithromycin fires interaction card" \
  '.cards | map(.summary) | any(test("macrolide"; "i"))' "$resp"

# --- 5. HF admission --------------------------------------------------------
pt=hf-smoke2
seed_patient $pt 1950-01-01
hf_cond="$(condition hf1 84114007 'Heart failure' $pt "$now")"
hf_payload="$(jq -n \
  --argjson patient "$(patient $pt 1950-01-01)" \
  --argjson conds "$(bundle "[$hf_cond]")" \
  --argjson empty "$(bundle '[]')" '{
  hook: "encounter-start", hookInstance: "hf-smoke",
  context: {patientId: "'$pt'", encounterId: "hf-smoke-enc", userId: "Practitioner/smoke"},
  prefetch: {patient: $patient, "condition-1": $conds, "observation-2": $empty,
             conditions: $conds, observations: $empty}
}')"
resp="$(invoke hf-admission-protocol "$hf_payload")"
check "hf-admission: active HF fires bundle + BNP cards" \
  '.cards | length >= 3
   and (map(.summary) | any(test("HF admission bundle"; "i")))
   and (map(.summary) | any(test("natriuretic|BNP"; "i")))' "$resp"
check "hf-admission: CarePlan shipped in response extension" \
  '.extension["https://atrius.in/fhir/extension/care-plan"].resourceType == "CarePlan"' "$resp"

# --- 6. Imaging appropriateness ---------------------------------------------
pt=img-smoke
seed_patient $pt 1980-01-01
lbp_recent="$(condition lbp1 279039007 'Low back pain' $pt "$(date -u -v-14d +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -d '14 days ago' +%Y-%m-%dT%H:%M:%SZ)")"
red_flag="$(condition ces1 192970008 'Cauda equina syndrome' $pt "$now")"
draft_mri='{"resourceType":"ServiceRequest","status":"draft","intent":"order","meta":{"profile":["https://atrius.in/fhir/r4/atrius-in/StructureDefinition/atrius-in-servicerequest"]},"code":{"coding":[{"system":"http://loinc.org","code":"24968-0","display":"MR Lumbar spine"}]},"subject":{"reference":"Patient/'$pt'"},"authoredOn":"'$now'"}'
mk_img() {
  jq -n --argjson patient "$(patient $pt 1980-01-01)" --argjson conds "$(bundle "$1")" \
    --argjson empty "$(bundle '[]')" --argjson draft "$draft_mri" '{
    hook: "order-select", hookInstance: "img-smoke",
    context: {patientId: "'$pt'", userId: "Practitioner/smoke",
              selections: ["ServiceRequest/draft"],
              draftOrders: {resourceType: "Bundle", entry: [{resource: $draft}]}},
    prefetch: {patient: $patient, "condition-1": $conds, conditions: $conds,
               "servicerequest-2": $empty, serviceRequests: $empty}
  }'
}
resp="$(invoke atrius-imaging-appropriateness "$(mk_img "[$lbp_recent]")")"
check "imaging: acute LBP + draft MRI fires advisory with X-ray suggestion" \
  '.cards | length == 1 and (.[0].suggestions | length >= 1)' "$resp"
resp="$(invoke atrius-imaging-appropriateness "$(mk_img "[$lbp_recent,$red_flag]")")"
check "imaging: red flag suppresses advisory" '.cards | length == 0' "$resp"

# --- 7. Renal dosing --------------------------------------------------------
pt=renal-smoke
seed_patient $pt
egfr_low="$(lab_obs g1 98979-8 'eGFR (CKD-EPI 2021)' 25 'mL/min/1.73m2' 'mL/min/{1.73_m2}' $pt "$now")"
gaba_draft="$(med_request 386845007 Gabapentin draft $pt)"
renal_payload="$(jq -n \
  --argjson patient "$(patient $pt)" \
  --argjson obs "$(bundle "[$egfr_low]")" \
  --argjson empty "$(bundle '[]')" \
  --argjson draft "$gaba_draft" '{
  hook: "order-sign", hookInstance: "renal-smoke",
  context: {patientId: "'$pt'", userId: "Practitioner/smoke",
            draftOrders: {resourceType: "Bundle", entry: [{resource: $draft}]}},
  prefetch: {patient: $patient, "observation-1": $obs, observations: $obs,
             "medicationrequest-2": $empty, medicationRequests: $empty}
}')"
resp="$(invoke atrius-renal-dosing "$renal_payload")"
check "renal-dosing: gabapentin + eGFR 25 fires dose-adjusted suggestion" \
  '.cards | map(.suggestions[]?.actions[]?.resource.dosageInstruction[0].text // empty) | any(test("eGFR 15-29"))' "$resp"

# --- 8. Surgical safety checklist -------------------------------------------
pt=surg-smoke
seed_patient $pt
surg_enc='{"resourceType":"Encounter","id":"surg-smoke-enc","status":"in-progress",
  "meta":{"profile":["https://atrius.in/fhir/r4/atrius-in/StructureDefinition/atrius-in-encounter"]},
  "class":{"system":"http://terminology.hl7.org/CodeSystem/v3-ActCode","code":"IMP"},
  "type":[{"coding":[{"system":"http://snomed.info/sct","code":"387713003","display":"Surgical procedure"}]}],
  "subject":{"reference":"Patient/'$pt'"}}'
checklist_qr='{"resourceType":"QuestionnaireResponse","id":"surg-smoke-qr","status":"completed",
  "questionnaire":"https://atrius.in/fhir/r4/atrius-in/Questionnaire/surgical-safety-checklist|0.1.0",
  "subject":{"reference":"Patient/'$pt'"},"encounter":{"reference":"Encounter/surg-smoke-enc"},
  "authored":"'$now'",
  "item":[{"linkId":"sign-in","item":[{"linkId":"sign-in-identity","answer":[{"valueBoolean":true}]}]}]}'
mk_surg() { # qr-entries
  jq -n --argjson patient "$(patient $pt)" --argjson encs "$(bundle "[$surg_enc]")" \
    --argjson qrs "$(bundle "$1")" '{
    hook: "encounter-start", hookInstance: "surg-smoke",
    context: {patientId: "'$pt'", encounterId: "surg-smoke-enc", userId: "Practitioner/smoke"},
    prefetch: {patient: $patient, "encounter-1": $encs, encounters: $encs,
               "questionnaireresponse-2": $qrs, questionnaireResponses: $qrs}
  }'
}
resp="$(invoke surgical-safety-checklist-rule "$(mk_surg '[]')")"
check "surgical-safety: surgical encounter without checklist fires reminder" \
  '.cards | length == 1 and (.[0].summary | test("checklist"; "i"))' "$resp"
resp="$(invoke surgical-safety-checklist-rule "$(mk_surg "[$checklist_qr]")")"
check "surgical-safety: completed checklist QR is silent" '.cards | length == 0' "$resp"

# --- 9. VTE prophylaxis -----------------------------------------------------
pt=vte-smoke
seed_patient $pt 1960-01-01
vte_enc='{"resourceType":"Encounter","id":"vte-smoke-enc","status":"in-progress",
  "meta":{"profile":["https://atrius.in/fhir/r4/atrius-in/StructureDefinition/atrius-in-encounter"]},
  "class":{"system":"http://terminology.hl7.org/CodeSystem/v3-ActCode","code":"IMP"},
  "type":[{"coding":[{"system":"http://snomed.info/sct","code":"32485007","display":"Hospital admission"}]}],
  "subject":{"reference":"Patient/'$pt'"},
  "period":{"start":"'"$now"'"}}'
enox="$(med_request 372562003 Enoxaparin active $pt | jq '.id = "vte-enox"')"
mk_vte() { # med-entries
  jq -n --argjson patient "$(patient $pt 1960-01-01)" --argjson encs "$(bundle "[$vte_enc]")" \
    --argjson meds "$(bundle "$1")" --argjson empty "$(bundle '[]')" '{
    hook: "encounter-start", hookInstance: "vte-smoke",
    context: {patientId: "'$pt'", encounterId: "vte-smoke-enc", userId: "Practitioner/smoke"},
    prefetch: {patient: $patient, "encounter-1": $encs, encounters: $encs,
               "condition-2": $empty, conditions: $empty,
               "medicationrequest-3": $meds, medicationRequests: $meds}
  }'
}
resp="$(invoke atrius-vte-prophylaxis "$(mk_vte '[]')")"
check "vte-prophylaxis: adult inpatient without LMWH fires reminder" \
  '.cards | length >= 1 and (map(.summary) | any(test("VTE|prophylaxis|enoxaparin"; "i")))' "$resp"
resp="$(invoke atrius-vte-prophylaxis "$(mk_vte "[$enox]")")"
check "vte-prophylaxis: active enoxaparin is silent" '.cards | length == 0' "$resp"

# --- 10. OT antibiotic prophylaxis ------------------------------------------
pt=otpx-smoke
seed_patient $pt
ot_enc='{"resourceType":"Encounter","id":"otpx-smoke-enc","status":"in-progress",
  "meta":{"profile":["https://atrius.in/fhir/r4/atrius-in/StructureDefinition/atrius-in-encounter"]},
  "class":{"system":"http://terminology.hl7.org/CodeSystem/v3-ActCode","code":"IMP"},
  "type":[{"coding":[{"system":"http://snomed.info/sct","code":"387713003","display":"Surgical procedure"}]}],
  "subject":{"reference":"Patient/'$pt'"}}'
ctx="$(med_request 372670001 Ceftriaxone active $pt | jq '.id = "otpx-ctx"')"
mk_otpx() { # med-entries
  jq -n --argjson patient "$(patient $pt)" --argjson encs "$(bundle "[$ot_enc]")" \
    --argjson meds "$(bundle "$1")" '{
    hook: "encounter-start", hookInstance: "otpx-smoke",
    context: {patientId: "'$pt'", encounterId: "otpx-smoke-enc", userId: "Practitioner/smoke"},
    prefetch: {patient: $patient, "encounter-1": $encs, encounters: $encs,
               "medicationrequest-2": $meds, medicationRequests: $meds}
  }'
}
resp="$(invoke atrius-ot-prophylaxis "$(mk_otpx '[]')")"
check "ot-prophylaxis: surgical encounter without antibiotic fires reminder" \
  '.cards | length >= 1 and (map(.summary) | any(test("antibiotic|prophylaxis|ceftriaxone"; "i")))' "$resp"
resp="$(invoke atrius-ot-prophylaxis "$(mk_otpx "[$ctx]")")"
check "ot-prophylaxis: active ceftriaxone is silent" '.cards | length == 0' "$resp"

# --- 11. Discharge readiness ------------------------------------------------
pt=dcr-smoke
seed_patient $pt 1960-01-01
dcr_enc='{"resourceType":"Encounter","id":"dcr-smoke-enc","status":"in-progress",
  "meta":{"profile":["https://atrius.in/fhir/r4/atrius-in/StructureDefinition/atrius-in-encounter"]},
  "class":{"system":"http://terminology.hl7.org/CodeSystem/v3-ActCode","code":"IMP"},
  "type":[{"coding":[{"system":"http://snomed.info/sct","code":"32485007","display":"Hospital admission"}]}],
  "subject":{"reference":"Patient/'$pt'"},
  "period":{"start":"'"$now"'"}}'
pending_task='{"resourceType":"Task","id":"dcr-lab-task","status":"in-progress","intent":"order",
  "meta":{"profile":["https://atrius.in/fhir/r4/atrius-in/StructureDefinition/atrius-in-task"]},
  "code":{"coding":[{"system":"http://hl7.org/fhir/CodeSystem/task-code","code":"fulfill"}]},
  "for":{"reference":"Patient/'$pt'"},
  "encounter":{"reference":"Encounter/dcr-smoke-enc"}}'
mk_dcr() { # task-entries
  jq -n --argjson patient "$(patient $pt 1960-01-01)" --argjson encs "$(bundle "[$dcr_enc]")" \
    --argjson tasks "$(bundle "$1")" --argjson empty "$(bundle '[]')" '{
    hook: "patient-view", hookInstance: "dcr-smoke",
    context: {patientId: "'$pt'", userId: "Practitioner/smoke"},
    prefetch: {patient: $patient, "encounter-1": $encs, encounters: $encs,
               "task-2": $tasks, tasks: $tasks,
               "medicationrequest-3": $empty, medicationRequests: $empty,
               "questionnaireresponse-4": $empty, questionnaireResponses: $empty}
  }'
}
resp="$(invoke atrius-discharge-readiness "$(mk_dcr "[$pending_task]")")"
check "discharge-readiness: pending lab Task fires advisory" \
  '.cards | length >= 1 and (map(.summary) | any(test("lab|discharge|pending"; "i")))' "$resp"
resp="$(invoke atrius-discharge-readiness "$(mk_dcr '[]')")"
check "discharge-readiness: no blockers is silent" '.cards | length == 0' "$resp"

echo
echo "Atrius CDS services smoke: $PASS passed, $FAIL failed"
[[ "$FAIL" -eq 0 ]]
