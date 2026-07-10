#!/usr/bin/env bash
# CDS Hooks CMS165 smoke test with populated prefetch (production-style).
#
# Fetches Patient + compartment searches from cr-fhir-bridge, POSTs to cds-server,
# prints CDS cards JSON. Avoids fragile shell JSON interpolation.
#
# Prerequisites: HTS, clinical HFS, bridge, KR, sidecar, cds-server (see startup-guide.md).
#
# Usage:
#   ./scripts/cds-cms165-prefetch-smoke.sh
#   ./scripts/cds-cms165-prefetch-smoke.sh --apply
#   ./scripts/cds-cms165-prefetch-smoke.sh --print-payload-only > /tmp/cms165-invoke.json
#   TEST_PATIENT_ID=cms165-demo HFS_BRIDGE_URL=http://127.0.0.1:8081 ./scripts/cds-cms165-prefetch-smoke.sh
#
# --apply uses the PlanDefinition-first manifest service id (sidecar POST /v1/plandefinition/apply).
# Requires manifest rows with planDefinitionId — run ./scripts/setup-plandefinition-cds-catalog.sh first.
#
# Measurement Period is set on the hook context for each POST /cds-services/{id} invoke
# (the eCQM reporting window — not derived per patient).

set -euo pipefail

PRINT_PAYLOAD_ONLY=false
APPLY_MODE=false
for arg in "$@"; do
  case "$arg" in
    --print-payload-only) PRINT_PAYLOAD_ONLY=true ;;
    --apply) APPLY_MODE=true ;;
  esac
done

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BRIDGE="${HFS_BRIDGE_URL:-http://127.0.0.1:8081}"
CDS="${CDS_SERVER_URL:-http://127.0.0.1:8095}"
PATIENT="${TEST_PATIENT_ID:-cms165-demo}"
if [[ "$APPLY_MODE" == "true" ]]; then
  SERVICE_ID="${CDS_SERVICE_ID:-cms165fhircontrollinghighbloodpressure}"
else
  SERVICE_ID="${CDS_SERVICE_ID:-atriuscms165controllinghighbp}"
fi
MP_LOW="${CDS_MEASUREMENT_PERIOD_LOW:-2026-01-01}"
MP_HIGH="${CDS_MEASUREMENT_PERIOD_HIGH:-2026-12-31}"

bridge="${BRIDGE%/}"

fetch_json() {
  local url="$1"
  local label="$2"
  if ! curl -sfS -H 'Accept: application/fhir+json' "$url"; then
    echo "error: failed to fetch $label from $url" >&2
    echo "hint: start cr-fhir-bridge + clinical HFS; run ./scripts/import-cms165-demo.py --verify" >&2
    exit 1
  fi
}

echo "Fetching standard chart prefetch from $bridge for patient $PATIENT ..." >&2
echo "CDS service: $SERVICE_ID (cds-server uses PlanDefinition/\$apply when the manifest row has planDefinitionId)" >&2
echo "Measurement period (hook context): $MP_LOW .. $MP_HIGH" >&2

payload="$(
  jq -n \
    --argjson patientResource "$(fetch_json "$bridge/Patient/$PATIENT" "Patient")" \
    --argjson conditions "$(fetch_json "$bridge/Condition?subject=Patient/$PATIENT" "Condition search")" \
    --argjson encounters "$(fetch_json "$bridge/Encounter?subject=Patient/$PATIENT" "Encounter search")" \
    --argjson observations "$(fetch_json "$bridge/Observation?subject=Patient/$PATIENT" "Observation search")" \
    --argjson procedures "$(fetch_json "$bridge/Procedure?patient=$PATIENT" "Procedure search")" \
    --argjson medicationRequests "$(fetch_json "$bridge/MedicationRequest?patient=$PATIENT" "MedicationRequest search")" \
    --argjson immunizations "$(fetch_json "$bridge/Immunization?patient=$PATIENT" "Immunization search")" \
    --argjson diagnosticReports "$(fetch_json "$bridge/DiagnosticReport?patient=$PATIENT" "DiagnosticReport search")" \
    --argjson serviceRequests "$(fetch_json "$bridge/ServiceRequest?patient=$PATIENT" "ServiceRequest search")" \
    --argjson allergies "$(fetch_json "$bridge/AllergyIntolerance?patient=$PATIENT" "AllergyIntolerance search")" \
    --argjson coverage "$(fetch_json "$bridge/Coverage?beneficiary=Patient/$PATIENT" "Coverage search")" \
    --arg patientId "$PATIENT" \
    --arg mpLow "$MP_LOW" \
    --arg mpHigh "$MP_HIGH" \
    '{
      hook: "patient-view",
      hookInstance: "prefetch-smoke",
      context: {
        patientId: $patientId,
        userId: "Practitioner/example",
        measurementPeriod: {low: $mpLow, high: $mpHigh}
      },
      prefetch: {
        patient: $patientResource,
        conditions: $conditions,
        encounters: $encounters,
        observations: $observations,
        procedures: $procedures,
        medicationRequests: $medicationRequests,
        immunizations: $immunizations,
        diagnosticReports: $diagnosticReports,
        serviceRequests: $serviceRequests,
        allergies: $allergies,
        coverage: $coverage
      }
    }'
)"

if [[ "$PRINT_PAYLOAD_ONLY" == "true" ]]; then
  echo "$payload" | jq .
  exit 0
fi

tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT
http_code="$(
  curl -sS -o "$tmp" -w '%{http_code}' \
    -X POST "${CDS%/}/cds-services/$SERVICE_ID" \
    -H 'Content-Type: application/json' \
    -d "$payload"
)"

body="$(cat "$tmp")"

if [[ "$http_code" != "200" ]]; then
  echo "cds-server HTTP $http_code (response is plain text, not JSON — do not pipe to jq):" >&2
  echo "$body" >&2
  exit 1
fi

echo "$body" | jq .

if [[ "$APPLY_MODE" == "true" ]]; then
  card_count="$(echo "$body" | jq '.cards | length')"
  if [[ "$card_count" -lt 1 ]]; then
    echo "error: --apply expected at least one CDS card from RequestGroup mapping" >&2
    exit 1
  fi
  echo "ok: $card_count card(s) from PlanDefinition/\$apply path" >&2
fi
