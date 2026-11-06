#!/usr/bin/env bash
# Staging/local proof: HFS_VALIDATION_MODE=enforce rejects a non-conformant write with 422.
#
#   HFS_BASE_URL=http://127.0.0.1:8082 ./scripts/prove-validation-enforce.sh
#
# Optional: HFS_BEARER, HFS_TENANT (default: default).
# A 201 here means write-path validation is off or the overlay is not loaded.

set -euo pipefail

BASE="${HFS_BASE_URL:-http://127.0.0.1:8082}"
BASE="${BASE%/}"
TENANT="${HFS_TENANT:-default}"

headers=(-H "Content-Type: application/fhir+json" -H "X-Tenant-ID: ${TENANT}")
if [[ -n "${HFS_BEARER:-}" ]]; then
  headers+=(-H "Authorization: Bearer ${HFS_BEARER}")
fi

body='{"resourceType":"Patient","id":"validation-enforce-probe","bogusElement":true}'
tmp="$(mktemp)"
trap 'rm -f "${tmp}"' EXIT

code="$(
  curl -sS -o "${tmp}" -w "%{http_code}" \
    -X POST "${BASE}/Patient" \
    "${headers[@]}" \
    --data "${body}"
)"

echo "POST ${BASE}/Patient → HTTP ${code}"
if [[ -s "${tmp}" ]]; then
  python3 -c 'import json,sys; print(json.dumps(json.load(open(sys.argv[1])), indent=2)[:2000])' "${tmp}" 2>/dev/null \
    || head -c 500 "${tmp}"
  echo
fi

if [[ "${code}" == "422" ]]; then
  echo "OK: enforce mode rejected a non-conformant write with 422."
  exit 0
fi

echo "FAIL: expected 422 Unprocessable Entity (HFS_VALIDATION_MODE=enforce + IG overlay)." >&2
echo "Got ${code}. Check HFS_VALIDATION_MODE, HFS_FHIR_PACKAGES, and that this is clinical HFS." >&2
exit 1
