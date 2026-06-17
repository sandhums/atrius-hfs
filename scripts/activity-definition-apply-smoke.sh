#!/usr/bin/env bash
# Smoke test: ActivityDefinition/$apply via cr-fhir-bridge (FHIR Parameters → sidecar).
#
# Prerequisites: bridge :8081, sidecar :8088, KR with catalog ActivityDefinitions imported.
#
# Usage:
#   ./scripts/activity-definition-apply-smoke.sh
#   ./scripts/activity-definition-apply-smoke.sh atrius-ad-lab-troponin-cardiac
#   TEST_PATIENT_ID=p1 TEST_ENCOUNTER_ID=e1 ./scripts/activity-definition-apply-smoke.sh atrius-ad-med-aspirin

set -euo pipefail

BRIDGE="${HFS_BRIDGE_URL:-http://127.0.0.1:8081}"
PATIENT="${TEST_PATIENT_ID:-er-chest-pain-smoke-patient}"
ENCOUNTER="${TEST_ENCOUNTER_ID:-er-chest-pain-smoke-encounter}"
PRACTITIONER="${TEST_PRACTITIONER_ID:-er-chest-pain-smoke-practitioner}"
AD_ID="${1:-atrius-ad-lab-troponin-cardiac}"

bridge="${BRIDGE%/}"

apply_payload="$(jq -n \
  --arg subject "Patient/$PATIENT" \
  --arg encounter "Encounter/$ENCOUNTER" \
  --arg practitioner "Practitioner/$PRACTITIONER" \
  '{
    resourceType: "Parameters",
    parameter: [
      {name: "subject", valueString: $subject},
      {name: "encounter", valueString: $encounter},
      {name: "practitioner", valueString: $practitioner}
    ]
  }')"

echo "ActivityDefinition/$AD_ID/\$apply via $bridge ..." >&2

tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT
http_code="$(
  curl -sS -o "$tmp" -w '%{http_code}' \
    -X POST "$bridge/ActivityDefinition/$AD_ID/\$apply" \
    -H 'Content-Type: application/fhir+json' \
    -H 'Accept: application/fhir+json' \
    -d "$apply_payload"
)"

body="$(cat "$tmp")"

if [[ "$http_code" != "200" ]]; then
  echo "HTTP $http_code:" >&2
  echo "$body" >&2
  exit 1
fi

echo "$body" | jq '
  .parameter[] | select(.name == "return") | .resource |
  {resourceType, id, status, intent, code, medicationCodeableConcept}
'

resource_type="$(echo "$body" | jq -r '.parameter[] | select(.name == "return") | .resource.resourceType')"
echo "ok: \$apply returned $resource_type for ActivityDefinition/$AD_ID" >&2
