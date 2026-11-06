#!/usr/bin/env bash
# Shared OAuth helpers for CDS smokes against auth-enabled clinical HFS.
#
# Source from other scripts:
#   # shellcheck source=cds-smoke-auth.sh
#   source "$(dirname "$0")/cds-smoke-auth.sh"
#   cds_smoke_auth_init
#   seed_patient …   # uses CLINICAL_AUTH_HEADER / TENANT
#   invoke …        # injects fhirServer + fhirAuthorization
#
# Env (defaults match local Keycloak + cds-backend-client):
#   CDS_SMOKE_TOKEN_URL     (default: https://localhost:8443/realms/fhir/protocol/openid-connect/token)
#   CDS_SMOKE_CLIENT_ID     (default: cds-backend-client)
#   CDS_SMOKE_CLIENT_SECRET (default: cds-backend-secret)
#   CDS_SMOKE_TENANT_ID     (default: atrius-hospitals)
#   CLINICAL_HFS_URL        (default: http://127.0.0.1:8082)
#   CDS_SMOKE_SKIP_AUTH=1   skip token fetch (only works if HFS_AUTH_ENABLED=false)

cds_smoke_auth_init() {
  CLINICAL="${CLINICAL_HFS_URL:-http://127.0.0.1:8082}"
  clinical="${CLINICAL%/}"
  CDS_SMOKE_TENANT_ID="${CDS_SMOKE_TENANT_ID:-atrius-hospitals}"
  CDS_SMOKE_TOKEN_URL="${CDS_SMOKE_TOKEN_URL:-https://localhost:8443/realms/fhir/protocol/openid-connect/token}"
  CDS_SMOKE_CLIENT_ID="${CDS_SMOKE_CLIENT_ID:-cds-backend-client}"
  CDS_SMOKE_CLIENT_SECRET="${CDS_SMOKE_CLIENT_SECRET:-cds-backend-secret}"
  CDS_SMOKE_SCOPE="${CDS_SMOKE_SCOPE:-system/*.cruds}"
  CDS_SMOKE_SUBJECT="${CDS_SMOKE_SUBJECT:-$CDS_SMOKE_CLIENT_ID}"
  CDS_SMOKE_ACCESS_TOKEN=""
  CDS_SMOKE_EXPIRES_IN=300

  if [[ "${CDS_SMOKE_SKIP_AUTH:-0}" == "1" ]]; then
    echo "cds-smoke-auth: CDS_SMOKE_SKIP_AUTH=1 — no bearer token" >&2
    return 0
  fi

  # Probe: unauthenticated Patient read. 401/403 → need token; 404/200 → auth off.
  local probe
  probe="$(curl -sS -o /dev/null -w '%{http_code}' "$clinical/Patient/cds-smoke-auth-probe" || true)"
  if [[ "$probe" != "401" && "$probe" != "403" ]]; then
    echo "cds-smoke-auth: clinical HFS returned HTTP $probe without auth — skipping token" >&2
    return 0
  fi

  local resp
  resp="$(curl -skS -X POST "$CDS_SMOKE_TOKEN_URL" \
    -H 'Content-Type: application/x-www-form-urlencoded' \
    --data-urlencode "grant_type=client_credentials" \
    --data-urlencode "client_id=${CDS_SMOKE_CLIENT_ID}" \
    --data-urlencode "client_secret=${CDS_SMOKE_CLIENT_SECRET}" \
    --data-urlencode "scope=${CDS_SMOKE_SCOPE}" 2>&1)" || {
    echo "cds-smoke-auth: token request failed" >&2
    echo "$resp" >&2
    return 1
  }

  CDS_SMOKE_ACCESS_TOKEN="$(echo "$resp" | jq -r '.access_token // empty')"
  CDS_SMOKE_EXPIRES_IN="$(echo "$resp" | jq -r '.expires_in // 300')"
  if [[ -z "$CDS_SMOKE_ACCESS_TOKEN" ]]; then
    echo "cds-smoke-auth: no access_token in response:" >&2
    echo "$resp" | jq . >&2 || echo "$resp" >&2
    return 1
  fi
  echo "cds-smoke-auth: got Bearer token (expires_in=${CDS_SMOKE_EXPIRES_IN}s, tenant=${CDS_SMOKE_TENANT_ID})" >&2
}

# Merge fhirServer + fhirAuthorization into a CDS Hooks request JSON object (stdin → stdout).
cds_smoke_inject_fhir_auth() {
  if [[ -z "${CDS_SMOKE_ACCESS_TOKEN:-}" ]]; then
    cat
    return 0
  fi
  jq --arg tok "$CDS_SMOKE_ACCESS_TOKEN" \
     --arg server "$clinical" \
     --arg scope "$CDS_SMOKE_SCOPE" \
     --arg sub "$CDS_SMOKE_SUBJECT" \
     --argjson exp "${CDS_SMOKE_EXPIRES_IN:-300}" \
     '.
      + {
          fhirServer: $server,
          fhirAuthorization: {
            access_token: $tok,
            token_type: "Bearer",
            expires_in: $exp,
            scope: $scope,
            subject: $sub
          }
        }'
}

# Soft preflight: Observation type search must not 503 (Elasticsearch required for CQL retrieve fallthrough).
cds_smoke_preflight_search() {
  local code
  local -a auth=()
  if [[ -n "${CDS_SMOKE_ACCESS_TOKEN:-}" ]]; then
    auth=(-H "Authorization: Bearer ${CDS_SMOKE_ACCESS_TOKEN}" -H "X-Tenant-ID: ${CDS_SMOKE_TENANT_ID}")
  fi
  code="$(curl -sS -o /dev/null -w '%{http_code}' "${auth[@]}" \
    "$clinical/Observation?_count=1" || true)"
  if [[ "$code" == "503" ]]; then
    echo "cds-smoke-auth: WARN clinical Observation search returned HTTP 503." >&2
    echo "  Elasticsearch is likely down; PlanDefinition/\$apply CQL retrieve falls through to HFS search" >&2
    echo "  and conditions fail → cds-server emits critical fallback cards ('… — active')." >&2
    echo "  Start ES (or the HFS compose stack) and re-run." >&2
    return 1
  fi
  return 0
}
