#!/usr/bin/env bash
# P0 #4 — import NDHM + Atrius terminology into HTS, then seed HIS encounter ValueSets.
#
# Prerequisites:
#   - HTS running (default http://127.0.0.1:9091)
#   - Atrius IG built: cd "$ATRIUS_IG_DRAFT" && ./_build.sh
#   - Local NDHM mirror at NDHM_DIR (default ~/Downloads/package_ndhm)
#
# Usage:
#   export HTS_URL=http://127.0.0.1:9091
#   ./scripts/p0-import-terminology.sh

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HTS_URL="${HTS_URL:-http://127.0.0.1:9091}"
NDHM_DIR="${NDHM_DIR:-/Users/sandhu/Downloads/package_ndhm}"
ATRIUS_IG_DRAFT="${ATRIUS_IG_DRAFT:-/Users/sandhu/AtriusIGDraft}"
HIS_ROOT="${ATRIUS_HIS_ROOT:-$(cd "${ROOT}/../atrius-his" 2>/dev/null && pwd || true)}"

echo "P0 terminology import → ${HTS_URL}"
curl -sf "${HTS_URL}/health" >/dev/null || {
  echo "HTS not reachable at ${HTS_URL}/health" >&2
  exit 1
}

python3 "${ROOT}/scripts/import-ndhm-atrius-terminology.py" \
  --hts-url "${HTS_URL}" \
  --ndhm-dir "${NDHM_DIR}" \
  --atrius-ig-output "${ATRIUS_IG_DRAFT}/output"

if [[ -n "${HIS_ROOT}" && -f "${HIS_ROOT}/scripts/seed-atrius-terminology.py" ]]; then
  python3 "${HIS_ROOT}/scripts/seed-atrius-terminology.py" --hts-url "${HTS_URL}"
else
  echo "Skip seed-atrius-terminology.py (atrius-his not found at ${HIS_ROOT})"
fi

echo "Done. Set HFS_TERMINOLOGY_SERVER=${HTS_URL} on clinical HFS."
echo "Then run ./scripts/setup-atrius-profile-registry.sh to seed the FHIR package"
echo "cache; it prints the exact HFS_FHIR_PACKAGE_CACHE / HFS_FHIR_PACKAGES /"
echo "HFS_VALIDATION_MODE=enforce values to use (name@version from the IG package.json)."
