#!/usr/bin/env bash
# Verify manifest libraryVersion + planDefinitionId pins exist on KR HFS.
#
# Usage:
#   ./scripts/validate-manifest-kr-pins.sh
#   KR_HFS_URL=http://127.0.0.1:8079 ./scripts/validate-manifest-kr-pins.sh
#
# Exit 0 when all pins resolve; exit 1 on any missing resource.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
KR="${KR_HFS_URL:-http://127.0.0.1:8079}"
MANIFEST="${MANIFEST:-$REPO_ROOT/manifests/cds-services-kr-ecqm.json}"
kr="${KR%/}"

if [[ ! -f "$MANIFEST" ]]; then
  echo "error: manifest not found: $MANIFEST" >&2
  exit 1
fi

missing=0
checked=0

while IFS=$'\t' read -r lib_id lib_ver pd_id; do
  [[ -z "$lib_id" ]] && continue
  checked=$((checked + 1))

  lib_json="$(curl -sfS -H 'Accept: application/fhir+json' "$kr/Library/$lib_id" 2>/dev/null || echo '{}')"
  lib_version="$(echo "$lib_json" | jq -r '.version // empty')"
  if [[ "$lib_version" != "$lib_ver" ]]; then
    echo "MISSING or VERSION MISMATCH Library/$lib_id (manifest $lib_ver, KR ${lib_version:-none})" >&2
    missing=$((missing + 1))
  fi

  if [[ -n "$pd_id" ]]; then
    if ! curl -sfS -H 'Accept: application/fhir+json' "$kr/PlanDefinition/$pd_id" >/dev/null 2>&1; then
      echo "MISSING PlanDefinition/$pd_id" >&2
      missing=$((missing + 1))
    fi
  fi
done < <(jq -r '.services[] | [.libraryId, .libraryVersion, .planDefinitionId // ""] | @tsv' "$MANIFEST")

echo "Checked $checked manifest service(s); missing pins: $missing" >&2
if [[ "$missing" -gt 0 ]]; then
  exit 1
fi
echo "ok: all manifest pins found on KR at $kr" >&2
