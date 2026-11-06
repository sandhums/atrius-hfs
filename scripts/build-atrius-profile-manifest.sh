#!/usr/bin/env bash
# Regenerate optional Atrius IG audit manifests (file inventories under manifests/).
# HFS write-path validation does **not** read these — use
# ./scripts/setup-atrius-profile-registry.sh to seed HFS_FHIR_PACKAGE_CACHE.
#
# HL7 datatype/extension StructureDefinitions are **not** listed here: the
# helios-fhir-validator embedded R4 core pack already supplies them
# (SimpleQuantity, patient-nationality, …). See docs/validation-cutover.md.
#
# Default: fetches the published NPM package from atrius.in, expands into
# manifests/atrius-ig-package/, and writes relative-path inventories.
#
# Usage:
#   ./scripts/build-atrius-profile-manifest.sh
#   ./scripts/setup-atrius-profile-registry.sh   # expand + seed package cache
#   ATRIUS_IG_SOURCE=local ./scripts/build-atrius-profile-manifest.sh
#   ATRIUS_IG_PACKAGE_TGZ=/path/to/package.tgz ./scripts/build-atrius-profile-manifest.sh
#
# Outputs (audit only):
#   manifests/atrius-ig-package/                    — expanded published package (gitignored)
#   manifests/atrius-r4-profile-manifest-core.json  — top-level package StructureDefinitions
#   manifests/atrius-r4-profile-manifest.json       — + nested SDs + CodeSystems + ValueSets

set -euo pipefail

ROOT="${ATRIUS_HFS_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
IG="${ATRIUS_IG_DRAFT:-/Users/sandhu/AtriusIGDraft}"
PUBLISHED_PACKAGE_URL="${ATRIUS_IG_PACKAGE_URL:-https://atrius.in/fhir/r4/atrius-in/package.tgz}"
MANIFEST_DIR="${ROOT}/manifests"
DEFAULT_EXPANDED="${MANIFEST_DIR}/atrius-ig-package"
CORE_OUT="${MANIFEST_DIR}/atrius-r4-profile-manifest-core.json"
FULL_OUT="${MANIFEST_DIR}/atrius-r4-profile-manifest.json"

if [[ -n "${ATRIUS_IG_EXPANDED:-}" ]]; then
  IG_OUTPUT="${ATRIUS_IG_EXPANDED}"
  echo "IG source: ATRIUS_IG_EXPANDED=${IG_OUTPUT}" >&2
elif [[ -n "${ATRIUS_IG_PACKAGE_TGZ:-}" || -n "${ATRIUS_IG_PACKAGE_URL:-}" ]]; then
  export ATRIUS_IG_EXPANDED="${DEFAULT_EXPANDED}"
  IG_OUTPUT="$("${ROOT}/scripts/load-atrius-ig-package.sh")"
  echo "IG source: package (${ATRIUS_IG_PACKAGE_TGZ:-${ATRIUS_IG_PACKAGE_URL}})" >&2
elif [[ "${ATRIUS_IG_SOURCE:-}" == "local" ]]; then
  IG_OUTPUT="${IG}/output"
  echo "IG source: local draft ${IG_OUTPUT}" >&2
else
  export ATRIUS_IG_PACKAGE_URL="${PUBLISHED_PACKAGE_URL}"
  export ATRIUS_IG_EXPANDED="${DEFAULT_EXPANDED}"
  echo "IG source: published package ${PUBLISHED_PACKAGE_URL}" >&2
  IG_OUTPUT="$("${ROOT}/scripts/load-atrius-ig-package.sh")"
fi

if [[ ! -d "${IG_OUTPUT}" ]]; then
  echo "Atrius IG source not found: ${IG_OUTPUT}" >&2
  echo "Use ATRIUS_IG_SOURCE=local after building AtriusIGDraft, or set ATRIUS_IG_PACKAGE_TGZ / ATRIUS_IG_PACKAGE_URL" >&2
  exit 1
fi

if [[ ! -f "${IG_OUTPUT}/StructureDefinition-atrius-in-patient.json" \
  && -f "${IG_OUTPUT}/package/StructureDefinition-atrius-in-patient.json" ]]; then
  IG_OUTPUT="${IG_OUTPUT}/package"
fi

echo "IG output: ${IG_OUTPUT}"
python3 - "${IG_OUTPUT}" "${CORE_OUT}" "${FULL_OUT}" <<'PY'
"""Write package-only audit inventories (no HL7 deps — embedded core pack covers those)."""
import glob
import json
import os
import sys

ig_output, core_out, full_out = sys.argv[1:4]
manifest_dir = os.path.dirname(os.path.realpath(core_out))


def abs_posix(path: str) -> str:
    return os.path.realpath(path).replace("\\", "/")


def rel_to_manifest(path: str) -> str:
    return os.path.relpath(abs_posix(path), manifest_dir).replace("\\", "/")


def classify(path: str) -> set[str]:
    try:
        with open(path, encoding="utf-8") as f:
            v = json.load(f)
    except Exception:
        return set()
    kinds: set[str] = set()
    rt = v.get("resourceType")
    if rt in ("StructureDefinition", "CodeSystem", "ValueSet"):
        kinds.add(rt)
    elif rt == "Bundle":
        for entry in v.get("entry") or []:
            rrt = (entry.get("resource") or {}).get("resourceType")
            if rrt in ("StructureDefinition", "CodeSystem", "ValueSet"):
                kinds.add(rrt)
    return kinds


core_paths = sorted(
    abs_posix(p) for p in glob.glob(os.path.join(ig_output, "StructureDefinition-*.json"))
)
if len(core_paths) < 100:
    raise SystemExit(
        f"expected ≥100 top-level StructureDefinitions in {ig_output}, got {len(core_paths)}"
    )

sd, cs, vs = [], [], []
for root, _dirs, files in os.walk(ig_output):
    for name in files:
        if not name.endswith(".json") or name in ("package.json", ".index.json"):
            continue
        path = os.path.join(root, name)
        kinds = classify(path)
        rel = rel_to_manifest(path)
        if "StructureDefinition" in kinds:
            sd.append(rel)
        if "CodeSystem" in kinds:
            cs.append(rel)
        if "ValueSet" in kinds:
            vs.append(rel)

core_manifest = {
    "structure_definition_files": [rel_to_manifest(p) for p in core_paths],
    "code_system_files": [],
    "value_set_files": [],
}
full_manifest = {
    "structure_definition_files": sorted(set(sd)),
    "code_system_files": sorted(set(cs)),
    "value_set_files": sorted(set(vs)),
}

for path, manifest in ((core_out, core_manifest), (full_out, full_manifest)):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)
        f.write("\n")

print(
    f"Wrote {core_out} ({len(core_manifest['structure_definition_files'])} "
    f"top-level package StructureDefinitions)"
)
print(
    f"Wrote {full_out} ({len(full_manifest['structure_definition_files'])} StructureDefinitions, "
    f"{len(full_manifest['code_system_files'])} CodeSystems, "
    f"{len(full_manifest['value_set_files'])} ValueSets)"
)
print(
    "NOTE: audit only — HFS loads IG profiles from HFS_FHIR_PACKAGE_CACHE / "
    "HFS_FHIR_PACKAGES. HL7 datatypes/extensions come from the embedded R4 core pack."
)
PY

echo "Done. Wrote optional audit manifests under manifests/ (not read by HFS)."
echo "  Seed the package cache with: ./scripts/setup-atrius-profile-registry.sh"
