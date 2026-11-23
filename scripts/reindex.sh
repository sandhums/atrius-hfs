#!/usr/bin/env bash
# Fetch a backend token and kick off POST /$reindex on clinical HFS.
#
#   ./scripts/reindex.sh
#   HFS_BASE_URL=http://127.0.0.1:8082 ./scripts/reindex.sh
#
# Auth: do NOT send X-Tenant-ID unless the JWT carries tenant_id (HFS_AUTH_TENANT_CLAIM).
# With hfs-backend-client and no tenant claim, HFS uses HFS_DEFAULT_TENANT (atrius-hospitals).
# Sending X-Tenant-ID: default (or any id != default) without a JWT claim → 403.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ENV_FILE:-${ROOT}/deploy/env/hfs-clinical.env}"
if [[ -f "${ENV_FILE}" ]]; then
  # shellcheck disable=SC1090
  set -a && source "${ENV_FILE}" && set +a
fi

export KEYCLOAK_URL="${KEYCLOAK_URL:-https://localhost:8443}"
export REALM="${REALM:-fhir}"
export CLIENT_ID="${CLIENT_ID:-hfs-backend-client}"
export CLIENT_SECRET="${CLIENT_SECRET:-hfs-backend-secret}"
export SCOPE="${SCOPE:-system/*.cruds}"
HFS_BASE_URL="${HFS_BASE_URL:-http://127.0.0.1:8082}"
HFS_BASE_URL="${HFS_BASE_URL%/}"

TOKEN_URL="${KEYCLOAK_URL}/realms/${REALM}/protocol/openid-connect/token"

echo "Token: ${TOKEN_URL} (client=${CLIENT_ID})" >&2
RESP=$(curl -skS -X POST "${TOKEN_URL}" \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  --data-urlencode 'grant_type=client_credentials' \
  --data-urlencode "client_id=${CLIENT_ID}" \
  --data-urlencode "client_secret=${CLIENT_SECRET}" \
  --data-urlencode "scope=${SCOPE}")

TOKEN=$(echo "$RESP" | jq -r '.access_token // empty')
if [[ -z "$TOKEN" ]]; then
  echo "Token request failed:" >&2
  echo "$RESP" | jq . >&2
  exit 1
fi
echo "Token OK (expires_in=$(echo "$RESP" | jq -r '.expires_in'))" >&2
echo "$TOKEN" | awk -F. '{print $2}' | tr '_-' '/+' | base64 -d 2>/dev/null \
  | jq '{scp, roles, tenant_id}' >&2 || true

AUTH=(-H "Authorization: Bearer ${TOKEN}")
# Intentionally no X-Tenant-ID: claimless backend token → HFS_DEFAULT_TENANT

echo "POST ${HFS_BASE_URL}/\$reindex" >&2
KICK=$(curl -sS -w '\n%{http_code}' -X POST "${HFS_BASE_URL}/\$reindex" \
  "${AUTH[@]}" \
  -H 'Content-Type: application/fhir+json' \
  -d '{}')

BODY=$(echo "$KICK" | sed '$d')
CODE=$(echo "$KICK" | tail -1)
echo "HTTP ${CODE}"
echo "$BODY" | jq .

if [[ "$CODE" != "202" ]]; then
  exit 1
fi

JOB_ID=$(echo "$BODY" | jq -r '.parameter[]? | select(.name=="jobId") | .valueString')
if [[ -z "$JOB_ID" || "$JOB_ID" == "null" ]]; then
  echo "No jobId in response" >&2
  exit 1
fi

echo "Polling job ${JOB_ID} …" >&2
while true; do
  sleep 2
  STATUS=$(curl -sS "${AUTH[@]}" "${HFS_BASE_URL}/\$reindex-status/${JOB_ID}")
  # Poll body uses valueCode; kick-off 202 uses valueString "queued"
  STATE=$(echo "$STATUS" | jq -r '.parameter[]? | select(.name=="status") | (.valueCode // .valueString) // empty' | head -1)
  PROC=$(echo "$STATUS" | jq -r '.parameter[]? | select(.name=="processed") | .valueInteger // empty' | head -1)
  TOTAL=$(echo "$STATUS" | jq -r '.parameter[]? | select(.name=="total") | .valueInteger // empty' | head -1)
  ERR=$(echo "$STATUS" | jq -r '.parameter[]? | select(.name=="errorCount") | .valueInteger // empty' | head -1)
  echo "  status=${STATE:-?} processed=${PROC:-?} total=${TOTAL:-?} errorCount=${ERR:-?}"
  case "${STATE}" in
    completed|failed|cancelled)
      echo "$STATUS" | jq .
      [[ "$STATE" == "completed" ]]
      exit $?
      ;;
  esac
done