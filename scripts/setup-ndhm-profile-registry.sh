#!/usr/bin/env bash
# Seed HFS_FHIR_PACKAGE_CACHE with published ndhm.in@6.5.0 for the ABDM export validator.
#
# Clinical HFS must **not** list this package. HIS export `$validate` uses a
# dedicated HFS (deploy/clinical/.env.abdm.example) that overlays ndhm.in only.
#
# Default source: ~/.fhir/packages/ndhm.in#6.5.0/package
# Override: NDHM_IG_EXPANDED=/path/to/expanded/package
#
# Usage:
#   ./scripts/setup-ndhm-profile-registry.sh
#   HFS_FHIR_PACKAGE_CACHE=/opt/atrius/data/fhir-packages ./scripts/setup-ndhm-profile-registry.sh

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"

CACHE_ROOT="${HFS_FHIR_PACKAGE_CACHE:-${ROOT}/data/fhir-packages}"
SRC="${NDHM_IG_EXPANDED:-${HOME}/.fhir/packages/ndhm.in#6.5.0/package}"
PKG_JSON="${SRC}/package.json"

if [[ ! -f "${PKG_JSON}" ]]; then
  echo "ndhm.in package not found at ${SRC}" >&2
  echo "Install/export ndhm.in 6.5.0, or set NDHM_IG_EXPANDED to an expanded package/ directory." >&2
  exit 1
fi

PKG_NAME="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["name"])' "${PKG_JSON}")"
PKG_VERSION="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["version"])' "${PKG_JSON}")"
PKG_REF="${PKG_NAME}@${PKG_VERSION}"
DEST="${CACHE_ROOT}/${PKG_NAME}/${PKG_VERSION}"

echo "==> Seeding NDHM FHIR package cache: ${DEST}"
mkdir -p "${CACHE_ROOT}/${PKG_NAME}"
rm -rf "${DEST}"
mkdir -p "${DEST}"
if command -v rsync >/dev/null 2>&1; then
  rsync -a --delete "${SRC}/" "${DEST}/"
else
  cp -a "${SRC}/." "${DEST}/"
fi

python3 - "${DEST}" "${PKG_REF}" <<'PY'
import json
import os
import sys

dest, pkg_ref = sys.argv[1], sys.argv[2]
pkg_json = os.path.join(dest, "package.json")
meta = json.load(open(pkg_json, encoding="utf-8"))
expected = f"{meta['name']}@{meta['version']}"
if expected != pkg_ref:
    raise SystemExit(f"package.json id {expected!r} != {pkg_ref!r}")

sds = sorted(
    name
    for name in os.listdir(dest)
    if name.startswith("StructureDefinition-") and name.endswith(".json")
)
for needle in (
    "StructureDefinition-Patient.json",
    "StructureDefinition-Encounter.json",
    "StructureDefinition-Claim.json",
):
    if needle not in sds:
        raise SystemExit(f"seeded package missing required profile {needle}")

print(f"OK: seeded {pkg_ref} ({len(sds)} top-level StructureDefinitions) → {dest}")
PY

echo
echo "Start the NDHM export validator with:"
echo "  HFS_FHIR_PACKAGE_CACHE=${CACHE_ROOT}"
echo "  HFS_FHIR_PACKAGES=${PKG_REF}"
echo "  HFS_VALIDATION_MODE=enforce"
echo "  ./scripts/run-hfs-ndhm.sh"
echo "Do not add ${PKG_REF} to clinical HFS_FHIR_PACKAGES."
