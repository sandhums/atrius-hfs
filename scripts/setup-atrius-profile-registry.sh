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
# Then seed the Helios package cache and start clinical HFS with:
#   HFS_FHIR_PACKAGE_CACHE=<cache-root>
#   HFS_FHIR_PACKAGES=atrius.in.r4@<version>
#   HFS_VALIDATION_MODE=enforce
# (legacy HFS_PROFILE_MANIFEST / fhir-validation crate removed — use package layers)
#
# Verifies the HL7 datatype pack excludes abstract base types (Element, …) — including
# Element breaks registry load (no derivation) and silently disables write validation.
#
# See: crates/fhir-validator/docs/packages.md and docs/validation-cutover.md

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

dt_dir = os.path.join(base, "deps", "hl7-r4-datatypes")
sq = os.path.join(dt_dir, "StructureDefinition-SimpleQuantity.json")
if not os.path.isfile(sq):
    raise SystemExit(f"missing datatype pack SimpleQuantity at {sq}")
dt_count = sum(
    1
    for name in os.listdir(dt_dir)
    if name.startswith("StructureDefinition-") and name.endswith(".json")
)
if dt_count < 58:
    raise SystemExit(f"expected ≥58 HL7 datatype SDs in {dt_dir}, got {dt_count}")
for banned in ("Element", "BackboneElement", "Resource", "DomainResource"):
    banned_path = os.path.join(dt_dir, f"StructureDefinition-{banned}.json")
    if os.path.isfile(banned_path):
        raise SystemExit(f"base type {banned} must not be in datatype pack: {banned_path}")
print(f"OK: {dt_count} HL7 R4 datatype StructureDefinitions (incl. SimpleQuantity; base types excluded)")

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
echo "  # Prefer HFS_FHIR_PACKAGE_CACHE + HFS_FHIR_PACKAGES (see crates/fhir-validator/docs/packages.md)"
echo "  # Legacy manifest still at: ${ROOT}/manifests/atrius-r4-profile-manifest-core.json"
echo "  HFS_PROFILE_VALIDATION_MODE=strict"
