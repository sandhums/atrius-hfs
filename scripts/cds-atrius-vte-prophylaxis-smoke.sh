#!/usr/bin/env bash
# Per-service smoke: atrius-vte-prophylaxis (encounter-start).
# Thin wrapper around the VTE section of cds-atrius-services-smoke.sh helpers.
#
# Usage:
#   ./scripts/cds-atrius-vte-prophylaxis-smoke.sh
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
SVC=atrius-vte-prophylaxis
pt=vte-smoke
now="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

cds_smoke_auth_init || { echo "FATAL: OAuth token failed" >&2; exit 1; }
cds_smoke_preflight_search || { echo "FATAL: Observation search unavailable (ES?)" >&2; exit 1; }

patient() {
  jq -n --arg id "$1" --arg birth "${2:-1960-01-01}" '{
    resourceType: "Patient", id: $id, gender: "unknown", birthDate: $birth,
    meta: {profile: ["https://atrius.in/fhir/r4/atrius-in/StructureDefinition/atrius-in-patient"]},
    identifier: [{system: "https://atrius.in/smoke", value: $id}],
    name: [{family: "Smoke", given: [$id]}]
  }'
}
seed_patient() {
  local code
  local -a auth=()
  if [[ -n "${CDS_SMOKE_ACCESS_TOKEN:-}" ]]; then
    auth=(-H "Authorization: Bearer ${CDS_SMOKE_ACCESS_TOKEN}" -H "X-Tenant-ID: ${CDS_SMOKE_TENANT_ID}")
  fi
  code="$(patient "$1" "${2:-1960-01-01}" | curl -sS -o /dev/null -w '%{http_code}' \
    -X PUT -H 'Content-Type: application/fhir+json' "${auth[@]}" --data-binary @- "$clinical/Patient/$1")"
  if [[ "$code" != "200" && "$code" != "201" ]]; then
    echo "FATAL: seed Patient/$1 HTTP $code" >&2; exit 1
  fi
}
bundle() { jq -n --argjson entries "${1:-[]}" '{resourceType: "Bundle", type: "searchset", entry: [$entries[] | {resource: .}]}'; }
med_request() {
  jq -n --arg code "$1" --arg disp "$2" --arg status "$3" --arg pt "$4" '{
    resourceType: "MedicationRequest", status: $status, intent: "order",
    meta: {profile: ["https://atrius.in/fhir/r4/atrius-in/StructureDefinition/atrius-in-medicationrequest"]},
    medicationCodeableConcept: {coding: [{system: "http://snomed.info/sct", code: $code, display: $disp}], text: $disp},
    subject: {reference: ("Patient/" + $pt)}, authoredOn: (now | todate)
  }'
}
invoke() {
  local tmp code payload
  tmp="$(mktemp)"
  payload="$(printf '%s' "$2" | cds_smoke_inject_fhir_auth)"
  code="$(curl -sS -o "$tmp" -w '%{http_code}' -X POST "$cds/cds-services/$1" \
    -H 'Content-Type: application/json' -d "$payload")"
  if [[ "$code" != "200" ]]; then echo "HTTP $code from $1:" >&2; cat "$tmp" >&2; rm -f "$tmp"; return 1; fi
  cat "$tmp"; rm -f "$tmp"
}
check() {
  if echo "$3" | jq -e '[.cards[]?.summary // empty] | any(endswith(" — active"))' >/dev/null 2>&1; then
    echo "FAIL  $1 (cds-server fallback card)"; echo "$3" | jq '[.cards[]? | {summary, indicator}]' >&2; FAIL=$((FAIL+1)); return
  fi
  if echo "$3" | jq -e "$2" >/dev/null 2>&1; then echo "PASS  $1"; PASS=$((PASS+1))
  else echo "FAIL  $1"; echo "$3" | jq '[.cards[]? | {summary, indicator}]' >&2; FAIL=$((FAIL+1)); fi
}

seed_patient "$pt" 1960-01-01
vte_enc='{"resourceType":"Encounter","id":"vte-smoke-enc","status":"in-progress",
  "meta":{"profile":["https://atrius.in/fhir/r4/atrius-in/StructureDefinition/atrius-in-encounter"]},
  "class":{"system":"http://terminology.hl7.org/CodeSystem/v3-ActCode","code":"IMP"},
  "type":[{"coding":[{"system":"http://snomed.info/sct","code":"32485007","display":"Hospital admission"}]}],
  "subject":{"reference":"Patient/'"$pt"'"},
  "period":{"start":"'"$now"'"}}'
enox="$(med_request 372562003 Enoxaparin active "$pt" | jq '.id = "vte-enox"')"
mk() {
  jq -n --argjson patient "$(patient "$pt" 1960-01-01)" --argjson encs "$(bundle "[$vte_enc]")" \
    --argjson meds "$(bundle "$1")" --argjson empty "$(bundle '[]')" '{
    hook: "encounter-start", hookInstance: "vte-smoke",
    context: {patientId: "'"$pt"'", encounterId: "vte-smoke-enc", userId: "Practitioner/smoke"},
    prefetch: {patient: $patient, "encounter-1": $encs, encounters: $encs,
               "condition-2": $empty, conditions: $empty,
               "medicationrequest-3": $meds, medicationRequests: $meds}
  }'
}

resp="$(invoke "$SVC" "$(mk '[]')")"
check "$SVC: adult inpatient without LMWH fires" \
  '.cards | length >= 1' "$resp"
resp="$(invoke "$SVC" "$(mk "[$enox]")")"
check "$SVC: active enoxaparin silent" '.cards | length == 0' "$resp"

echo; echo "$SVC smoke: $PASS passed, $FAIL failed"
[[ "$FAIL" -eq 0 ]]
