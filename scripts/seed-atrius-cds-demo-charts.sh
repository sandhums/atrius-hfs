#!/usr/bin/env bash
# Seed clinical HFS chart data so BFF prefetch + clinical-ui can demo the Atrius
# CDS services (same patient/encounter ids as cds-atrius-services-smoke.sh UI presets).
#
# Prerequisites: Clinical HFS at CLINICAL_HFS_URL (default http://127.0.0.1:8082).
#
# Usage:
#   ./scripts/seed-atrius-cds-demo-charts.sh
#   CLINICAL_HFS_URL=http://127.0.0.1:8082 ./scripts/seed-atrius-cds-demo-charts.sh
#
# Then in clinical-ui Dev simulator: pick a demo preset and launch.

set -euo pipefail

CLINICAL="${CLINICAL_HFS_URL:-http://127.0.0.1:8082}"
clinical="${CLINICAL%/}"
now="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

put() { # path json — print OperationOutcome diagnostics on failure
  local code body
  body="$(mktemp)"
  code="$(curl -sS -o "$body" -w '%{http_code}' -X PUT -H 'Content-Type: application/fhir+json' \
    --data-binary "$2" "$clinical/$1")"
  if [[ "$code" != "200" && "$code" != "201" ]]; then
    echo "FAIL PUT $1 → HTTP $code" >&2
    if command -v jq >/dev/null 2>&1; then
      jq -r '.issue[]? | "\(.severity // "?"): \(.diagnostics // .details.text // ".")"' "$body" >&2 || cat "$body" >&2
    else
      cat "$body" >&2
    fi
    rm -f "$body"
    exit 1
  fi
  rm -f "$body"
  echo "OK   $1"
}

# Encounter-diagnosis Condition conforming to atrius-in-condition-encounter-diagnosis
# (category slice patternCodeableConcept + required encounter).
condition_ed() { # id code display patient encounter onset
  jq -n --arg id "$1" --arg code "$2" --arg disp "$3" --arg pt "$4" --arg enc "$5" --arg onset "$6" '{
    resourceType: "Condition", id: $id,
    meta: {profile: ["https://atrius.in/fhir/r4/atrius-in/StructureDefinition/atrius-in-condition-encounter-diagnosis"]},
    text: {status: "generated", div: ("<div xmlns=\"http://www.w3.org/1999/xhtml\">" + $disp + "</div>")},
    clinicalStatus: {coding: [{system: "http://terminology.hl7.org/CodeSystem/condition-clinical", code: "active"}]},
    verificationStatus: {coding: [{system: "http://terminology.hl7.org/CodeSystem/condition-ver-status", code: "confirmed"}]},
    category: [{coding: [{system: "http://terminology.hl7.org/CodeSystem/condition-category", code: "encounter-diagnosis"}]}],
    code: {coding: [{system: "http://snomed.info/sct", code: $code, display: $disp}], text: $disp},
    subject: {reference: ("Patient/" + $pt)},
    encounter: {reference: ("Encounter/" + $enc)},
    onsetDateTime: $onset,
    recordedDate: ($onset | split("T")[0])
  }'
}

echo "Seeding Atrius CDS demo charts on $clinical"

# --- HF admission (hf-smoke2 / hf-smoke-enc) ---------------------------------
put "Patient/hf-smoke2" "$(jq -n --arg now "$now" '{
  resourceType:"Patient", id:"hf-smoke2", gender:"male", birthDate:"1950-01-01",
  meta:{profile:["https://atrius.in/fhir/r4/atrius-in/StructureDefinition/atrius-in-patient"]},
  identifier:[{system:"https://atrius.in/smoke", value:"hf-smoke2"}],
  name:[{family:"Smoke", given:["HF"]}]
}')"
put "Encounter/hf-smoke-enc" "$(jq -n '{
  resourceType:"Encounter", id:"hf-smoke-enc", status:"in-progress",
  meta:{profile:["https://atrius.in/fhir/r4/atrius-in/StructureDefinition/atrius-in-encounter"]},
  class:{system:"http://terminology.hl7.org/CodeSystem/v3-ActCode", code:"IMP"},
  subject:{reference:"Patient/hf-smoke2"}
}')"
put "Condition/hf-smoke-cond" "$(condition_ed hf-smoke-cond 84114007 'Heart failure' hf-smoke2 hf-smoke-enc "$now")"

# --- Acute breathlessness (dyspnea-smoke / dyspnea-smoke-enc) ----------------
put "Patient/dyspnea-smoke" "$(jq -n '{
  resourceType:"Patient", id:"dyspnea-smoke", gender:"female", birthDate:"1955-06-15",
  meta:{profile:["https://atrius.in/fhir/r4/atrius-in/StructureDefinition/atrius-in-patient"]},
  identifier:[{system:"https://atrius.in/smoke", value:"dyspnea-smoke"}],
  name:[{family:"Smoke", given:["Dyspnea"]}]
}')"
put "Encounter/dyspnea-smoke-enc" "$(jq -n '{
  resourceType:"Encounter", id:"dyspnea-smoke-enc", status:"in-progress",
  meta:{profile:["https://atrius.in/fhir/r4/atrius-in/StructureDefinition/atrius-in-encounter"]},
  class:{system:"http://terminology.hl7.org/CodeSystem/v3-ActCode", code:"EMER", display:"emergency"},
  subject:{reference:"Patient/dyspnea-smoke"},
  reasonCode:[{
    coding:[
      {system:"https://atrius.in/fhir/r4/atrius-in/CodeSystem/atrius-in-reason-for-encounter-cs", code:"dyspnea", display:"Dyspnea"},
      {system:"http://snomed.info/sct", code:"267036007", display:"Dyspnea"}
    ],
    text:"Dyspnea"
  }]
}')"
put "Condition/dyspnea-smoke-cond" "$(condition_ed dyspnea-smoke-cond 267036007 'Dyspnea' dyspnea-smoke dyspnea-smoke-enc "$now")"

# --- Sepsis (sepsis-smoke / sepsis-smoke-enc) --------------------------------
put "Patient/sepsis-smoke" "$(jq -n '{
  resourceType:"Patient", id:"sepsis-smoke", gender:"female", birthDate:"1960-01-01",
  meta:{profile:["https://atrius.in/fhir/r4/atrius-in/StructureDefinition/atrius-in-patient"]},
  identifier:[{system:"https://atrius.in/smoke", value:"sepsis-smoke"}],
  name:[{family:"Smoke", given:["Sepsis"]}]
}')"
put "Encounter/sepsis-smoke-enc" "$(jq -n '{
  resourceType:"Encounter", id:"sepsis-smoke-enc", status:"in-progress",
  meta:{profile:["https://atrius.in/fhir/r4/atrius-in/StructureDefinition/atrius-in-encounter"]},
  class:{system:"http://terminology.hl7.org/CodeSystem/v3-ActCode", code:"IMP"},
  subject:{reference:"Patient/sepsis-smoke"}
}')"
put "Condition/sepsis-smoke-pna" "$(condition_ed sepsis-smoke-pna 233604007 'Pneumonia' sepsis-smoke sepsis-smoke-enc "$now")"
for row in \
  'sepsis-temp|8310-5|Body temperature|38.9|Cel|Cel' \
  'sepsis-hr|8867-4|Heart rate|118|/min|/min'; do
  IFS='|' read -r id code disp val unit ucum <<<"$row"
  put "Observation/$id" "$(jq -n --arg id "$id" --arg code "$code" --arg disp "$disp" \
    --argjson val "$val" --arg unit "$unit" --arg ucum "$ucum" --arg now "$now" '{
    resourceType:"Observation", id:$id, status:"final",
    meta:{profile:["https://atrius.in/fhir/r4/atrius-in/StructureDefinition/atrius-in-observation"]},
    category:[{coding:[{system:"http://terminology.hl7.org/CodeSystem/observation-category", code:"vital-signs"}]}],
    code:{coding:[{system:"http://loinc.org", code:$code, display:$disp}]},
    subject:{reference:"Patient/sepsis-smoke"},
    encounter:{reference:"Encounter/sepsis-smoke-enc"},
    effectiveDateTime:$now,
    valueQuantity:{value:$val, unit:$unit, system:"http://unitsofmeasure.org", code:$ucum}
  }')"
done

# --- Surgical safety (surg-smoke / surg-smoke-enc) ---------------------------
put "Patient/surg-smoke" "$(jq -n '{
  resourceType:"Patient", id:"surg-smoke", gender:"male", birthDate:"1975-01-01",
  meta:{profile:["https://atrius.in/fhir/r4/atrius-in/StructureDefinition/atrius-in-patient"]},
  identifier:[{system:"https://atrius.in/smoke", value:"surg-smoke"}],
  name:[{family:"Smoke", given:["Surgical"]}]
}')"
put "Encounter/surg-smoke-enc" "$(jq -n '{
  resourceType:"Encounter", id:"surg-smoke-enc", status:"in-progress",
  meta:{profile:["https://atrius.in/fhir/r4/atrius-in/StructureDefinition/atrius-in-encounter"]},
  class:{system:"http://terminology.hl7.org/CodeSystem/v3-ActCode", code:"IMP"},
  type:[{coding:[{system:"http://snomed.info/sct", code:"387713003", display:"Surgical procedure"}]}],
  subject:{reference:"Patient/surg-smoke"}
}')"

# --- Critical labs (crit-ui-smoke) -------------------------------------------
put "Patient/crit-ui-smoke" "$(jq -n '{
  resourceType:"Patient", id:"crit-ui-smoke", gender:"female", birthDate:"1965-06-01",
  meta:{profile:["https://atrius.in/fhir/r4/atrius-in/StructureDefinition/atrius-in-patient"]},
  identifier:[{system:"https://atrius.in/smoke", value:"crit-ui-smoke"}],
  name:[{family:"Smoke", given:["CriticalLabs"]}]
}')"
put "Encounter/crit-ui-smoke-enc" "$(jq -n '{
  resourceType:"Encounter", id:"crit-ui-smoke-enc", status:"in-progress",
  meta:{profile:["https://atrius.in/fhir/r4/atrius-in/StructureDefinition/atrius-in-encounter"]},
  class:{system:"http://terminology.hl7.org/CodeSystem/v3-ActCode", code:"AMB"},
  subject:{reference:"Patient/crit-ui-smoke"}
}')"
put "Observation/crit-ui-k" "$(jq -n --arg now "$now" '{
  resourceType:"Observation", id:"crit-ui-k", status:"final",
  meta:{profile:["https://atrius.in/fhir/r4/atrius-in/StructureDefinition/atrius-in-observation"]},
  category:[{coding:[{system:"http://terminology.hl7.org/CodeSystem/observation-category", code:"laboratory"}]}],
  code:{coding:[{system:"http://loinc.org", code:"2823-3", display:"Potassium"}]},
  subject:{reference:"Patient/crit-ui-smoke"},
  encounter:{reference:"Encounter/crit-ui-smoke-enc"},
  effectiveDateTime:$now,
  valueQuantity:{value:6.8, unit:"mmol/L", system:"http://unitsofmeasure.org", code:"mmol/L"}
}')"

# --- Preventive care (prev-ui-smoke, ~58yo) ----------------------------------
put "Patient/prev-ui-smoke" "$(jq -n '{
  resourceType:"Patient", id:"prev-ui-smoke", gender:"male", birthDate:"1968-01-01",
  meta:{profile:["https://atrius.in/fhir/r4/atrius-in/StructureDefinition/atrius-in-patient"]},
  identifier:[{system:"https://atrius.in/smoke", value:"prev-ui-smoke"}],
  name:[{family:"Smoke", given:["Preventive"]}]
}')"
put "Encounter/prev-ui-smoke-enc" "$(jq -n '{
  resourceType:"Encounter", id:"prev-ui-smoke-enc", status:"in-progress",
  meta:{profile:["https://atrius.in/fhir/r4/atrius-in/StructureDefinition/atrius-in-encounter"]},
  class:{system:"http://terminology.hl7.org/CodeSystem/v3-ActCode", code:"AMB"},
  subject:{reference:"Patient/prev-ui-smoke"}
}')"

# --- DDI / renal chart background (optional order-sign demos) ----------------
put "Patient/ddi-smoke" "$(jq -n '{
  resourceType:"Patient", id:"ddi-smoke", gender:"male", birthDate:"1960-01-01",
  meta:{profile:["https://atrius.in/fhir/r4/atrius-in/StructureDefinition/atrius-in-patient"]},
  identifier:[{system:"https://atrius.in/smoke", value:"ddi-smoke"}],
  name:[{family:"Smoke", given:["DDI"]}]
}')"
put "Encounter/ddi-smoke-enc" "$(jq -n '{
  resourceType:"Encounter", id:"ddi-smoke-enc", status:"in-progress",
  meta:{profile:["https://atrius.in/fhir/r4/atrius-in/StructureDefinition/atrius-in-encounter"]},
  class:{system:"http://terminology.hl7.org/CodeSystem/v3-ActCode", code:"AMB"},
  subject:{reference:"Patient/ddi-smoke"}
}')"
put "MedicationRequest/ddi-statin" "$(jq -n --arg now "$now" '{
  resourceType:"MedicationRequest", id:"ddi-statin", status:"active", intent:"order",
  meta:{profile:["https://atrius.in/fhir/r4/atrius-in/StructureDefinition/atrius-in-medicationrequest"]},
  text:{status:"generated", div:"<div xmlns=\"http://www.w3.org/1999/xhtml\">Simvastatin</div>"},
  medicationCodeableConcept:{
    coding:[{system:"http://snomed.info/sct", code:"387584000", display:"Simvastatin"}],
    text:"Simvastatin"
  },
  subject:{reference:"Patient/ddi-smoke"},
  encounter:{reference:"Encounter/ddi-smoke-enc"},
  authoredOn:$now,
  requester:{reference:"Practitioner/dr-patel"},
  dosageInstruction:[{text:"Simvastatin 20 mg orally once daily"}]
}')"

put "Patient/renal-smoke" "$(jq -n '{
  resourceType:"Patient", id:"renal-smoke", gender:"female", birthDate:"1955-01-01",
  meta:{profile:["https://atrius.in/fhir/r4/atrius-in/StructureDefinition/atrius-in-patient"]},
  identifier:[{system:"https://atrius.in/smoke", value:"renal-smoke"}],
  name:[{family:"Smoke", given:["Renal"]}]
}')"
put "Encounter/renal-smoke-enc" "$(jq -n '{
  resourceType:"Encounter", id:"renal-smoke-enc", status:"in-progress",
  meta:{profile:["https://atrius.in/fhir/r4/atrius-in/StructureDefinition/atrius-in-encounter"]},
  class:{system:"http://terminology.hl7.org/CodeSystem/v3-ActCode", code:"AMB"},
  subject:{reference:"Patient/renal-smoke"}
}')"
put "Observation/renal-egfr" "$(jq -n --arg now "$now" '{
  resourceType:"Observation", id:"renal-egfr", status:"final",
  meta:{profile:["https://atrius.in/fhir/r4/atrius-in/StructureDefinition/atrius-in-observation"]},
  category:[{coding:[{system:"http://terminology.hl7.org/CodeSystem/observation-category", code:"laboratory"}]}],
  code:{coding:[{system:"http://loinc.org", code:"98979-8", display:"eGFR (CKD-EPI 2021)"}]},
  subject:{reference:"Patient/renal-smoke"},
  encounter:{reference:"Encounter/renal-smoke-enc"},
  effectiveDateTime:$now,
  valueQuantity:{value:25, unit:"mL/min/1.73m2", system:"http://unitsofmeasure.org", code:"mL/min/{1.73_m2}"}
}')"

echo
echo "Done. Dev simulator presets:"
echo "  HF admission     → hf-smoke2 / hf-smoke-enc"
echo "  Breathlessness   → dyspnea-smoke / dyspnea-smoke-enc"
echo "  Sepsis           → sepsis-smoke / sepsis-smoke-enc"
echo "  Surgical safety  → surg-smoke / surg-smoke-enc"
echo "  Critical labs    → crit-ui-smoke / crit-ui-smoke-enc  (OPD)"
echo "  Preventive care  → prev-ui-smoke / prev-ui-smoke-enc  (OPD)"
echo "  DDI background   → ddi-smoke / ddi-smoke-enc"
echo "  Renal background → renal-smoke / renal-smoke-enc"
