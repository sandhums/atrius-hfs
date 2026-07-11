#!/usr/bin/env bash
# Fetch the published Atrius IG package and regenerate HFS ProfileRegistry manifests.
#
# Default source: https://atrius.in/fhir/r4/atrius-in/package.tgz
# Expands into manifests/atrius-ig-package/ and writes relative-path manifests.
#
# Usage:
#   ./scripts/setup-atrius-profile-registry.sh
#   ATRIUS_IG_SOURCE=local ./scripts/setup-atrius-profile-registry.sh
#   ATRIUS_IG_PACKAGE_TGZ=/path/to/package.tgz ./scripts/setup-atrius-profile-registry.sh
#
# Then start clinical HFS with:
#   HFS_PROFILE_MANIFEST=manifests/atrius-r4-profile-manifest-core.json
#   HFS_PROFILE_VALIDATION_MODE=strict
#
# See: crates/fhir-validation/docs/Profile_registry_and_IG_materialization.md

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"

echo "==> Building Atrius profile manifests from published (or local) package"
"${ROOT}/scripts/build-atrius-profile-manifest.sh"

CORE="${ROOT}/manifests/atrius-r4-profile-manifest-core.json"
echo "==> Verifying ProfileRegistry load from ${CORE}"

python3 - "${CORE}" <<'PY'
import json
import os
import sys

manifest_path = sys.argv[1]
base = os.path.dirname(os.path.realpath(manifest_path))
with open(manifest_path, encoding="utf-8") as f:
    manifest = json.load(f)

files = manifest.get("structure_definition_files") or []
if len(files) < 100:
    raise SystemExit(f"expected ≥100 StructureDefinitions, got {len(files)}")

missing = []
for entry in files:
    path = entry if os.path.isabs(entry) else os.path.join(base, entry)
    if not os.path.isfile(path):
        missing.append(entry)

if missing:
    raise SystemExit(
        f"{len(missing)} StructureDefinition path(s) missing (first: {missing[0]})"
    )

rel = sum(1 for e in files if not os.path.isabs(e))
print(
    f"OK: {len(files)} StructureDefinitions "
    f"({rel} relative to manifests/, {len(files) - rel} absolute)"
)
pkg = os.path.join(base, "atrius-ig-package", "package.json")
if os.path.isfile(pkg):
    with open(pkg, encoding="utf-8") as f:
        meta = json.load(f)
    print(
        f"OK: package {meta.get('name')}@{meta.get('version')} "
        f"canonical={meta.get('canonical') or meta.get('url')}"
    )
PY

echo "Done. Point HFS at: manifests/atrius-r4-profile-manifest-core.json"
echo "  HFS_PROFILE_MANIFEST=${ROOT}/manifests/atrius-r4-profile-manifest-core.json"
echo "  HFS_PROFILE_VALIDATION_MODE=strict"
