#!/usr/bin/env bash
# Regenerate Atrius IG profile manifests for HFS (HFS_PROFILE_MANIFEST).
#
# Default: fetches the published NPM package from atrius.in (prod path),
# expands into manifests/atrius-ig-package/, and writes **relative** paths
# (resolved by HFS against the manifest parent directory).
#
# Local IG draft output: ATRIUS_IG_SOURCE=local ./scripts/build-atrius-profile-manifest.sh
#
# Usage:
#   ./scripts/build-atrius-profile-manifest.sh
#   ./scripts/setup-atrius-profile-registry.sh   # build + verify
#
#   # Local AtriusIGDraft/output (dev, no network):
#   ATRIUS_IG_SOURCE=local ./scripts/build-atrius-profile-manifest.sh
#
#   # Explicit package path or URL:
#   ATRIUS_IG_PACKAGE_TGZ=/path/to/package.tgz ./scripts/build-atrius-profile-manifest.sh
#   ATRIUS_IG_PACKAGE_URL=https://atrius.in/fhir/r4/atrius-in/package.tgz \
#     ./scripts/build-atrius-profile-manifest.sh
#
# Outputs:
#   manifests/atrius-ig-package/               — expanded published package (gitignored)
#   manifests/deps/hl7-r4-extensions/*.json    — HL7 core extension SDs (Patient slices)
#   manifests/atrius-r4-profile-manifest-core.json  — atrius-in-* + HL7 extension deps
#   manifests/atrius-r4-profile-manifest.json       — core + en/ duplicates + deps (debug/audit)
#
# See: crates/fhir-validation/docs/Profile_registry_and_IG_materialization.md

set -euo pipefail

ROOT="${ATRIUS_HFS_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
IG="${ATRIUS_IG_DRAFT:-/Users/sandhu/AtriusIGDraft}"
PUBLISHED_PACKAGE_URL="${ATRIUS_IG_PACKAGE_URL:-https://atrius.in/fhir/r4/atrius-in/package.tgz}"
MANIFEST_DIR="${ROOT}/manifests"
DEFAULT_EXPANDED="${MANIFEST_DIR}/atrius-ig-package"

# IG source: expanded dir, explicit tarball/URL, local draft output, or published package (default).
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
HL7_EXT_DIR="${MANIFEST_DIR}/deps/hl7-r4-extensions"
HL7_CORE_TGZ="${HL7_CORE_TGZ:-${ROOT}/crates/hts/terminology-data/hl7.fhir.r4.core.tgz}"
CORE_OUT="${MANIFEST_DIR}/atrius-r4-profile-manifest-core.json"
FULL_OUT="${MANIFEST_DIR}/atrius-r4-profile-manifest.json"
TMP_FULL="$(mktemp)"

# HL7 R4 extension StructureDefinitions referenced by atrius-in-patient extension slices.
HL7_EXT_IDS=(
  Extension
  patient-birthPlace
  patient-nationality
  patient-birthTime
  patient-interpreterRequired
  patient-citizenship
  patient-cadavericDonor
  patient-importance
  patient-religion
)

if [[ ! -d "${IG_OUTPUT}" ]]; then
  echo "Atrius IG source not found: ${IG_OUTPUT}" >&2
  echo "Use ATRIUS_IG_SOURCE=local after building AtriusIGDraft, or set ATRIUS_IG_PACKAGE_TGZ / ATRIUS_IG_PACKAGE_URL" >&2
  exit 1
fi

# NPM package layout uses package/ prefix in some tarballs; expanded tree is flat.
if [[ ! -f "${IG_OUTPUT}/StructureDefinition-atrius-in-patient.json" \
  && -f "${IG_OUTPUT}/package/StructureDefinition-atrius-in-patient.json" ]]; then
  IG_OUTPUT="${IG_OUTPUT}/package"
fi

mkdir -p "${HL7_EXT_DIR}"

echo "Materializing HL7 R4 extension StructureDefinitions → ${HL7_EXT_DIR}"
need_hl7_extract=false
for id in "${HL7_EXT_IDS[@]}"; do
  if [[ ! -f "${HL7_EXT_DIR}/StructureDefinition-${id}.json" ]]; then
    need_hl7_extract=true
    break
  fi
done

if [[ "${need_hl7_extract}" == true ]]; then
  if [[ ! -f "${HL7_CORE_TGZ}" ]]; then
    echo "HL7 core package not found: ${HL7_CORE_TGZ}" >&2
    echo "Set HL7_CORE_TGZ to hl7.fhir.r4.core-4.0.1.tgz" >&2
    exit 1
  fi
  for id in "${HL7_EXT_IDS[@]}"; do
    dest="${HL7_EXT_DIR}/StructureDefinition-${id}.json"
    if [[ ! -f "${dest}" ]]; then
      tar -xzf "${HL7_CORE_TGZ}" -C "${HL7_EXT_DIR}" "package/StructureDefinition-${id}.json"
      mv "${HL7_EXT_DIR}/package/StructureDefinition-${id}.json" "${dest}"
      rmdir "${HL7_EXT_DIR}/package" 2>/dev/null || true
    fi
  done
else
  echo "HL7 extension deps already present — skipping ${HL7_CORE_TGZ} extract"
fi

# Not shipped in hl7.fhir.r4.core 4.0.1; published in HL7 FHIR Extensions pack.
RSG="${HL7_EXT_DIR}/StructureDefinition-individual-recordedSexOrGender.json"
if [[ ! -f "${RSG}" ]]; then
  echo "Fetching individual-recordedSexOrGender from hl7.org/fhir/extensions …"
  curl -sfL \
    "https://hl7.org/fhir/extensions/StructureDefinition-individual-recordedSexOrGender.json" \
    -o "${RSG}"
fi

# Optional NDHM package directory (terminology / cross-reference only; Atrius profiles parent FHIR R4).
NDHM_PACKAGE="${NDHM_PACKAGE:-/Users/sandhu/Downloads/ndhm}"
# Optional extra NDHM StructureDefinition filenames under NDHM_PACKAGE (empty = none).
NDHM_RECORD_PROFILES=("${NDHM_RECORD_PROFILES[@]:-}")

NDHM_EXT_PATHS=()
if [[ -d "${NDHM_PACKAGE}" && ${#NDHM_RECORD_PROFILES[@]} -gt 0 ]]; then
  for id in "${NDHM_RECORD_PROFILES[@]}"; do
    [[ -z "${id}" ]] && continue
    path="${NDHM_PACKAGE}/${id}"
    if [[ -f "${path}" ]]; then
      NDHM_EXT_PATHS+=("$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]).replace(chr(92), "/"))' "${path}")")
    fi
  done
fi

HL7_EXT_PATHS=()
for id in "${HL7_EXT_IDS[@]}" individual-recordedSexOrGender; do
  path="${HL7_EXT_DIR}/StructureDefinition-${id}.json"
  if [[ ! -f "${path}" ]]; then
    echo "Missing HL7 extension SD: ${path}" >&2
    exit 1
  fi
  HL7_EXT_PATHS+=("$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]).replace(chr(92), "/"))' "${path}")")
done

echo "IG output: ${IG_OUTPUT}"
echo "Generating full scan manifest..."
(
  cd "${ROOT}"
  cargo run -q -p fhir-validation --example build_ig_profile_manifest -- \
    "${IG_OUTPUT}" "${TMP_FULL}" --relative
)

# macOS / bash 3.2: empty arrays are "unbound" under `set -u`.
set +u
python3 - "${IG_OUTPUT}" "${CORE_OUT}" "${FULL_OUT}" "${TMP_FULL}" "${HL7_EXT_PATHS[@]}" -- "${NDHM_EXT_PATHS[@]}" <<'PY'
import glob
import json
import os
import sys

args = sys.argv[1:]
try:
    sep = args.index("--")
    main_args = args[:sep]
    ndhm_ext_paths = args[sep + 1 :]
except ValueError:
    main_args = args
    ndhm_ext_paths = []

ig_output, core_out, full_out, tmp_full = main_args[0:4]
hl7_ext_paths = main_args[4:]
manifest_dir = os.path.dirname(os.path.realpath(core_out))
tmp_parent = os.path.dirname(os.path.realpath(tmp_full))

def abs_posix(path: str) -> str:
    return os.path.realpath(path).replace("\\", "/")

def rel_to_manifest(path: str) -> str:
    """Paths relative to manifests/ so HFS can resolve regardless of CWD."""
    return os.path.relpath(abs_posix(path), manifest_dir).replace("\\", "/")

def normalize_scanned(entry: str) -> str:
    path = entry if os.path.isabs(entry) else os.path.join(tmp_parent, entry)
    return rel_to_manifest(path)

# Core: top-level atrius-in-* StructureDefinitions (no output/en/ duplicates).
core_glob = os.path.join(ig_output, "StructureDefinition-atrius-in-*.json")
core_paths = sorted(abs_posix(p) for p in glob.glob(core_glob))

# Clinical-reasoning ActivityDefinition profiles (not under atrius-in-* filename prefix).
ad_glob = os.path.join(ig_output, "StructureDefinition-atrius-*-activitydefinition.json")
ad_paths = sorted(abs_posix(p) for p in glob.glob(ad_glob))

core_paths.extend(p for p in ad_paths if p not in core_paths)
core_paths.extend(p for p in sorted(hl7_ext_paths) if p not in core_paths)
core_paths.extend(p for p in sorted(ndhm_ext_paths) if p not in core_paths)
core_paths.sort()

core_manifest = {
    "structure_definition_files": [rel_to_manifest(p) for p in core_paths],
    "code_system_files": [],
    "value_set_files": [],
}

with open(tmp_full, encoding="utf-8") as f:
    scanned = json.load(f)

# Full manifest: core + localized en/ copies (legacy layout for diff/audit).
en_glob = os.path.join(ig_output, "en", "StructureDefinition-atrius-in-*.json")
en_paths = sorted(abs_posix(p) for p in glob.glob(en_glob))

full_sd = sorted(set(core_paths) | set(en_paths))
full_manifest = {
    "structure_definition_files": [rel_to_manifest(p) for p in full_sd],
    "code_system_files": sorted(
        normalize_scanned(p) for p in scanned.get("code_system_files", [])
    ),
    "value_set_files": sorted(
        normalize_scanned(p) for p in scanned.get("value_set_files", [])
    ),
}

for path, manifest in ((core_out, core_manifest), (full_out, full_manifest)):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)
        f.write("\n")

hl7_count = len(hl7_ext_paths)
ndhm_count = len(ndhm_ext_paths)
atrius_count = len(core_manifest["structure_definition_files"]) - hl7_count - ndhm_count
print(
    f"Wrote {core_out} ({atrius_count} Atrius + {hl7_count} HL7 extension + {ndhm_count} NDHM StructureDefinitions)"
)
print(
    f"Wrote {full_out} ({len(full_manifest['structure_definition_files'])} StructureDefinitions, "
    f"{len(full_manifest['code_system_files'])} CodeSystems, "
    f"{len(full_manifest['value_set_files'])} ValueSets)"
)
missing = [
    p
    for p in core_manifest["structure_definition_files"]
    if not os.path.isfile(os.path.join(manifest_dir, p) if not os.path.isabs(p) else p)
]
if missing:
    raise SystemExit(f"ERROR: {len(missing)} core paths missing on disk (IG rebuild required)")
PY
set -u

rm -f "${TMP_FULL}"
echo "Done. Point HFS at: manifests/atrius-r4-profile-manifest-core.json"
echo "  (or run ./scripts/setup-atrius-profile-registry.sh to build + verify)"
