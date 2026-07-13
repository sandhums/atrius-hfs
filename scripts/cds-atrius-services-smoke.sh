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

set -euo pipefail

CDS="${CDS_SERVER_URL:-http://127.0.0.1:8095}"
CLINICAL="${CLINICAL_HFS_URL:-http://127.0.0.1:8082}"
cds="${CDS%/}"
clinical="${CLINICAL%/}"
PASS=0
FAIL=0

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
  code="$(patient "$1" "${2:-1960-01-01}" | curl -sS -o /dev/null -w '%{http_code}' \
    -X PUT -H 'Content-Type: application/fhir+json' --data-binary @- "$clinical/Patient/$1")"
  if [[ "$code" != "200" && "$code" != "201" ]]; then
    echo "warn: seeding Patient/$1 on $clinical returned HTTP $code" >&2
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
temp="$(lab_obs t1 8310-5 'Body temperature' 38.9 'Cel' 'Cel' $pt "$now")"
hr="$(lab_obs h1 8867-4 'Heart rate' 118 '/min' '/min' $pt "$now")"
infection="$(condition c1 233604007 'Pneumonia' $pt "$now")"
sepsis_payload="$(jq -n \
  --argjson patient "$(patient $pt)" \
  --argjson obs "$(bundle "[$temp,$hr]")" \
  --argjson conds "$(bundle "[$infection]")" '{
  hook: "encounter-start", hookInstance: "sepsis-smoke",
  context: {patientId: "'$pt'", encounterId: "sepsis-smoke-enc", userId: "Practitioner/smoke"},
  prefetch: {patient: $patient, "observation-1": $obs, "condition-2": $conds,
             observations: $obs, conditions: $conds}
}')"
resp="$(invoke sepsis-bundle "$sepsis_payload")"
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
check "hf-admission: active HF fires bundle + BNP cards" '.cards | length == 2' "$resp"
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

echo
echo "Atrius CDS services smoke: $PASS passed, $FAIL failed"
[[ "$FAIL" -eq 0 ]]
