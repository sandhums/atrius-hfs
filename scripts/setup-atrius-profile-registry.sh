#!/usr/bin/env bash
# Fetch the published Atrius IG package and seed HFS_FHIR_PACKAGE_CACHE.
#
# Default source: https://atrius.in/fhir/r4/atrius-in/package.tgz
# Expands into manifests/atrius-ig-package/, optionally writes audit manifests,
# then seeds {HFS_FHIR_PACKAGE_CACHE}/{name}/{version}/ (default: data/fhir-packages).
#
# HFS does **not** read the JSON manifests under manifests/ — only the seeded
# package cache + HFS_FHIR_PACKAGES. Audit manifests are optional (SKIP_MANIFESTS=1).
#
# Usage:
#   ./scripts/setup-atrius-profile-registry.sh
#   ATRIUS_IG_SOURCE=local ./scripts/setup-atrius-profile-registry.sh
#   ATRIUS_IG_PACKAGE_TGZ=/path/to/package.tgz ./scripts/setup-atrius-profile-registry.sh
#   HFS_FHIR_PACKAGE_CACHE=/opt/atrius/fhir-packages ./scripts/setup-atrius-profile-registry.sh
#   SKIP_MANIFESTS=1 ./scripts/setup-atrius-profile-registry.sh
#
# See: crates/fhir-validator/docs/packages.md and docs/validation-cutover.md

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"

CACHE_ROOT="${HFS_FHIR_PACKAGE_CACHE:-${ROOT}/data/fhir-packages}"
PKG_DIR="${ROOT}/manifests/atrius-ig-package"
PKG_JSON="${PKG_DIR}/package.json"

if [[ "${SKIP_MANIFESTS:-0}" == "1" ]]; then
  echo "==> Expanding IG package (SKIP_MANIFESTS=1 — no audit JSON)"
  if [[ -n "${ATRIUS_IG_EXPANDED:-}" ]]; then
    echo "IG source: ATRIUS_IG_EXPANDED=${ATRIUS_IG_EXPANDED}"
  elif [[ -n "${ATRIUS_IG_PACKAGE_TGZ:-}" || -n "${ATRIUS_IG_PACKAGE_URL:-}" || "${ATRIUS_IG_SOURCE:-}" != "local" ]]; then
    export ATRIUS_IG_EXPANDED="${PKG_DIR}"
    "${ROOT}/scripts/load-atrius-ig-package.sh" >/dev/null
  else
    # ATRIUS_IG_SOURCE=local — expect draft output; copy/symlink not handled here.
    echo "ATRIUS_IG_SOURCE=local with SKIP_MANIFESTS requires ATRIUS_IG_EXPANDED=${PKG_DIR} (or leave SKIP_MANIFESTS unset)" >&2
    exit 1
  fi
else
  echo "==> Building audit manifests + expanding IG package"
  "${ROOT}/scripts/build-atrius-profile-manifest.sh"
fi

if [[ ! -f "${PKG_JSON}" ]]; then
  echo "Expanded package missing package.json at ${PKG_JSON}" >&2
  echo "Re-run without SKIP_MANIFESTS, or set ATRIUS_IG_PACKAGE_TGZ / ATRIUS_IG_PACKAGE_URL" >&2
  exit 1
fi

# name@version come from the published IG package.json (IG publisher), not invented here.
PKG_NAME="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["name"])' "${PKG_JSON}")"
PKG_VERSION="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["version"])' "${PKG_JSON}")"
PKG_REF="${PKG_NAME}@${PKG_VERSION}"
DEST="${CACHE_ROOT}/${PKG_NAME}/${PKG_VERSION}"

echo "==> Seeding FHIR package cache: ${DEST}"
mkdir -p "${CACHE_ROOT}/${PKG_NAME}"
rm -rf "${DEST}"
mkdir -p "${DEST}"
if command -v rsync >/dev/null 2>&1; then
  rsync -a --delete "${PKG_DIR}/" "${DEST}/"
else
  cp -a "${PKG_DIR}/." "${DEST}/"
fi

echo "==> Verifying seeded package (what HFS actually loads)"
python3 - "${DEST}" "${PKG_REF}" <<'PY'
import json
import os
import sys

dest, pkg_ref = sys.argv[1], sys.argv[2]
pkg_json = os.path.join(dest, "package.json")
if not os.path.isfile(pkg_json):
    raise SystemExit(f"missing {pkg_json}")

meta = json.load(open(pkg_json, encoding="utf-8"))
expected = f"{meta['name']}@{meta['version']}"
if expected != pkg_ref:
    raise SystemExit(f"package.json id {expected!r} != {pkg_ref!r}")

sds = sorted(
    name
    for name in os.listdir(dest)
    if name.startswith("StructureDefinition-") and name.endswith(".json")
)
if len(sds) < 100:
    raise SystemExit(f"expected ≥100 top-level StructureDefinitions in cache, got {len(sds)}")

# Spot-check a few profiles clinical HFS cares about.
for needle in (
    "StructureDefinition-atrius-in-patient.json",
    "StructureDefinition-atrius-in-encounter.json",
):
    if needle not in sds:
        raise SystemExit(f"seeded package missing required profile {needle}")

print(f"OK: seeded {pkg_ref} ({len(sds)} top-level StructureDefinitions) → {dest}")
print("    (this is what HFS_FHIR_PACKAGES loads; audit manifests under manifests/ are optional)")
PY

echo
echo "Done. Start clinical HFS with:"
echo "  HFS_FHIR_PACKAGE_CACHE=${CACHE_ROOT}"
echo "  HFS_FHIR_PACKAGES=${PKG_REF}"
echo "  HFS_VALIDATION_MODE=enforce"
echo "  HFS_TERMINOLOGY_SERVER=http://127.0.0.1:9091"
echo "See: crates/fhir-validator/docs/packages.md and docs/validation-cutover.md"
