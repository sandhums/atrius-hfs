#!/usr/bin/env bash
# CDS Hooks ER chest pain pathway smoke (encounter-start + PlanDefinition/$apply).
#
# Simulates a CDS client: fetches Patient + chart prefetch from clinical HFS,
# POSTs encounter-start invoke to cds-server for service er-chest-pain-pathway.
#
# Prerequisites: HTS, clinical HFS, KR (PlanDefinition + Library + ADs),
# JVM sidecar, cds-server with CDS_SERVICES_MANIFEST_PATH pointing at this manifest.
#
# Usage:
#   ./scripts/cds-er-chest-pain-smoke.sh
#   ./scripts/cds-er-chest-pain-smoke.sh --print-payload-only > /tmp/er-chest-pain-invoke.json
#   TEST_PATIENT_ID=my-patient TEST_ENCOUNTER_ID=Encounter/abc ./scripts/cds-er-chest-pain-smoke.sh
#
# Import Atrius KR content first (from AtriusIGDraft):
#   ./scripts/import-atrius-kr-libraries.py --clinical-reasoning

set -euo pipefail

PRINT_PAYLOAD_ONLY=false
for arg in "$@"; do
  case "$arg" in
    --print-payload-only) PRINT_PAYLOAD_ONLY=true ;;
  esac
done

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLINICAL_HFS="${CLINICAL_HFS_URL:-http://127.0.0.1:8082}"
CDS="${CDS_SERVER_URL:-http://127.0.0.1:8095}"
KR="${KR_HFS_URL:-http://127.0.0.1:8079}"
PATIENT="${TEST_PATIENT_ID:-er-chest-pain-smoke-patient}"
ENCOUNTER="${TEST_ENCOUNTER_ID:-enc-er-demo}"
PRACTITIONER="${TEST_PRACTITIONER_ID:-Practitioner/example}"
SERVICE_ID="${CDS_SERVICE_ID:-er-chest-pain-pathway}"
PLAN_DEFINITION_ID="${PLAN_DEFINITION_ID:-er-chest-pain-pathway}"
SKIP_READY="${SKIP_READY:-false}"

hfs="${CLINICAL_HFS%/}"
cds="${CDS%/}"
kr="${KR%/}"

fetch_json() {
  local url="$1"
  local label="$2"
  if ! curl -sfS -H 'Accept: application/fhir+json' "$url"; then
    echo "error: failed to fetch $label from $url" >&2
    echo "hint: start clinical HFS; import ER demo patient data if needed" >&2
    exit 1
  fi
}

fetch_json_optional() {
  local url="$1"
  curl -sfS -H 'Accept: application/fhir+json' "$url" 2>/dev/null || echo '{"resourceType":"Bundle","type":"searchset","total":0,"entry":[]}'
}

if [[ "$SKIP_READY" != "true" && "$PRINT_PAYLOAD_ONLY" != "true" ]]; then
  echo "Checking cds-server /ready ..." >&2
  if ! ready="$(curl -sfS "$cds/ready" 2>/dev/null)"; then
    echo "warn: cds-server /ready failed — is cds-server running with sidecar URL?" >&2
  else
    echo "$ready" | jq '{status, krPlanDefinitions, planDefinitionPins}' >&2
    if [[ "$(echo "$ready" | jq -r '.krPlanDefinitions // empty')" != "ok" ]]; then
      echo "warn: KR PlanDefinition probe not ok on /ready — check CDS_VALIDATE_KR_LIBRARIES and KR import" >&2
    fi
  fi

  echo "Checking KR PlanDefinition/$PLAN_DEFINITION_ID ..." >&2
  if ! curl -sfS -H 'Accept: application/fhir+json' "$kr/PlanDefinition/$PLAN_DEFINITION_ID" >/dev/null; then
    echo "warn: GET $kr/PlanDefinition/$PLAN_DEFINITION_ID failed — run import-atrius-kr-libraries.py" >&2
  fi
fi

echo "Fetching ER chest pain prefetch from $hfs for patient $PATIENT (encounter-start) ..." >&2
echo "CDS service: $SERVICE_ID (PlanDefinition/\$apply via sidecar)" >&2

payload="$(
  jq -n \
    --argjson patientResource "$(fetch_json "$hfs/Patient/$PATIENT" "Patient")" \
    --argjson encounter1 "$(fetch_json "$hfs/Encounter?patient=$PATIENT" "Encounter search")" \
    --argjson condition2 "$(fetch_json "$hfs/Condition?patient=$PATIENT" "Condition search")" \
    --argjson observation3 "$(fetch_json "$hfs/Observation?patient=$PATIENT" "Observation search")" \
    --argjson questionnaireresponse4 "$(fetch_json_optional "$hfs/QuestionnaireResponse?patient=$PATIENT" "QuestionnaireResponse search")" \
    --argjson servicerequest5 "$(fetch_json_optional "$hfs/ServiceRequest?patient=$PATIENT" "ServiceRequest search")" \
    --argjson medicationrequest6 "$(fetch_json_optional "$hfs/MedicationRequest?patient=$PATIENT" "MedicationRequest search")" \
    --argjson diagnosticreport7 "$(fetch_json_optional "$hfs/DiagnosticReport?patient=$PATIENT" "DiagnosticReport search")" \
    --argjson medicationadministration8 "$(fetch_json_optional "$hfs/MedicationAdministration?patient=$PATIENT" "MedicationAdministration search")" \
    --argjson conditions "$(fetch_json "$hfs/Condition?patient=$PATIENT" "Condition search")" \
    --argjson encounters "$(fetch_json "$hfs/Encounter?patient=$PATIENT" "Encounter search")" \
    --argjson observations "$(fetch_json "$hfs/Observation?patient=$PATIENT" "Observation search")" \
    --argjson procedures "$(fetch_json_optional "$hfs/Procedure?patient=$PATIENT" "Procedure search")" \
    --argjson medicationRequests "$(fetch_json_optional "$hfs/MedicationRequest?patient=$PATIENT" "MedicationRequest search")" \
    --argjson immunizations "$(fetch_json_optional "$hfs/Immunization?patient=$PATIENT" "Immunization search")" \
    --argjson diagnosticReports "$(fetch_json_optional "$hfs/DiagnosticReport?patient=$PATIENT" "DiagnosticReport search")" \
    --argjson serviceRequests "$(fetch_json_optional "$hfs/ServiceRequest?patient=$PATIENT" "ServiceRequest search")" \
    --argjson allergies "$(fetch_json_optional "$hfs/AllergyIntolerance?patient=$PATIENT" "AllergyIntolerance search")" \
    --argjson coverage "$(fetch_json_optional "$hfs/Coverage?beneficiary=Patient/$PATIENT" "Coverage search")" \
    --arg patientId "$PATIENT" \
    --arg encounterId "$ENCOUNTER" \
    --arg userId "$PRACTITIONER" \
    '{
      hook: "encounter-start",
      hookInstance: "er-chest-pain-smoke",
      context: {
        patientId: $patientId,
        encounterId: $encounterId,
        userId: $userId
      },
      prefetch: {
        patient: $patientResource,
        "encounter-1": $encounter1,
        "condition-2": $condition2,
        "observation-3": $observation3,
        "questionnaireresponse-4": $questionnaireresponse4,
        "servicerequest-5": $servicerequest5,
        "medicationrequest-6": $medicationrequest6,
        "diagnosticreport-7": $diagnosticreport7,
        "medicationadministration-8": $medicationadministration8,
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
    -X POST "$cds/cds-services/$SERVICE_ID" \
    -H 'Content-Type: application/json' \
    -d "$payload"
)"

body="$(cat "$tmp")"

if [[ "$http_code" != "200" ]]; then
  echo "cds-server HTTP $http_code (response may be plain text — do not pipe to jq):" >&2
  echo "$body" >&2
  exit 1
fi

echo "$body" | jq .

card_count="$(echo "$body" | jq '.cards | length')"
if [[ "$card_count" -lt 1 ]]; then
  echo "warn: zero CDS cards — pathway may be out of scope (CQL In Scope) or classification not set" >&2
  echo "hint: ensure ED encounter + chest pain chief complaint exist for patient $PATIENT" >&2
else
  echo "ok: $card_count card(s) from encounter-start PlanDefinition/\$apply" >&2
fi

