#!/usr/bin/env bash
# P0 #4 — import NDHM + Atrius IG terminology into HTS.
#
# Prerequisites:
#   - HTS running (default http://127.0.0.1:9091), bootstrapped with the HL7 core
#     and THO packages (HTS_BOOTSTRAP_DIR)
#   - Atrius IG built: cd AtriusIGDraft && ./scripts/ci-ig-build.sh
#   - Local NDHM mirror (default ~/Downloads/ndhm, or ATRIUS_NDHM_DIR)
#
# Usage:
#   export HTS_URL=http://127.0.0.1:9091
#   ./scripts/p0-import-terminology.sh [--fhir-version r4|r5]
#
# ATRIUS_IG_OUTPUT overrides the IG output directory (default ../../AtriusIGDraft/output).

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HTS_URL="${HTS_URL:-http://127.0.0.1:9091}"

echo "P0 terminology import → ${HTS_URL}"
curl -sf "${HTS_URL}/health" >/dev/null || {
  echo "HTS not reachable at ${HTS_URL}/health" >&2
  exit 1
}

python3 "${ROOT}/scripts/import-ndhm-atrius-terminology.py" --hts-url "${HTS_URL}" "$@"

echo "Done. Set HFS_TERMINOLOGY_SERVER=${HTS_URL} on clinical HFS."
echo "Then run ./scripts/setup-atrius-profile-registry.sh to seed the FHIR package"
echo "cache; it prints the exact HFS_FHIR_PACKAGE_CACHE / HFS_FHIR_PACKAGES /"
echo "HFS_VALIDATION_MODE=enforce values to use (name@version from the IG package.json)."
