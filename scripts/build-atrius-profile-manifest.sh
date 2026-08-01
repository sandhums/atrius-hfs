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
#   manifests/deps/hl7-r4-datatypes/*.json     — R4 datatype SDs from fhir-gen profiles-types.json
#                                               (primitives + complex types + SimpleQuantity/MoneyQuantity;
#                                               excludes abstract base types such as Element)
#   manifests/atrius-r4-profile-manifest-core.json  — atrius-in-* + HL7 extension + datatype deps
#   manifests/atrius-r4-profile-manifest.json       — core + en/ duplicates + deps (debug/audit)
#
# See: crates/fhir-validator/docs/packages.md and docs/validation-cutover.md

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
HL7_DT_DIR="${MANIFEST_DIR}/deps/hl7-r4-datatypes"
# Same R4 types bundle helios-fhir-gen uses — authoritative, offline, always in-repo.
HL7_PROFILES_TYPES="${HL7_PROFILES_TYPES:-${ROOT}/crates/fhir-gen/resources/R4/profiles-types.json}"
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

# R4 datatype StructureDefinitions (primitive + complex + SimpleQuantity / MoneyQuantity).
# Source: fhir-gen profiles-types.json (same package used for helios-fhir codegen) — not a
# selective whitelist of clinical datatypes. Abstract FHIR *base* types (Element, …) are
# excluded: they are not useful type.profile targets and Element lacks `derivation`, which
# aborts the HFS profile registry loader. Resource SDs stay out; Atrius snapshots cover
# resource bases.
mkdir -p "${HL7_DT_DIR}"
echo "Materializing HL7 R4 datatype StructureDefinitions → ${HL7_DT_DIR}"
if [[ ! -f "${HL7_PROFILES_TYPES}" ]]; then
  echo "HL7 profiles-types.json not found: ${HL7_PROFILES_TYPES}" >&2
  exit 1
fi
python3 - "${HL7_PROFILES_TYPES}" "${HL7_DT_DIR}" <<'PY'
import json
import os
import sys

# Abstract / infrastructure roots — not clinical datatypes for the Atrius profile registry.
#
# Why exclude:
# - Element / BackboneElement / Resource / DomainResource are abstract FHIR base types, not
#   useful targets for snapshot type.profile resolution (we want Quantity, Coding, …).
# - Element has no StructureDefinition.derivation (and no baseDefinition). The HFS
#   ProfileRegistry extractor requires derivation on every SD; one missing value aborts the
#   whole manifest load. helios-rest then boots with write validation disabled, so Strict
#   mode silently returns 201 for invalid Atrius resources.
# Keep this list in sync with the banned-file checks later in this script and in
# setup-atrius-profile-registry.sh.
EXCLUDED_BASE_TYPE_IDS = frozenset(
    {
        "Element",
        "BackboneElement",
        "Resource",
        "DomainResource",
    }
)

src, dest_dir = sys.argv[1], sys.argv[2]
with open(src, encoding="utf-8") as f:
    bundle = json.load(f)

written = 0
ids = []
skipped = []
for entry in bundle.get("entry") or []:
    resource = entry.get("resource") or {}
    if resource.get("resourceType") != "StructureDefinition":
        continue
    sd_id = resource.get("id")
    if not sd_id:
        continue
    # Prefer explicit id allow/deny; also skip abstract roots with no derivation (Element).
    if sd_id in EXCLUDED_BASE_TYPE_IDS or (
        resource.get("abstract") is True
        and not resource.get("derivation")
        and not resource.get("baseDefinition")
    ):
        skipped.append(sd_id)
        continue
    ids.append(sd_id)
    path = os.path.join(dest_dir, f"StructureDefinition-{sd_id}.json")
    with open(path, "w", encoding="utf-8") as out:
        json.dump(resource, out, indent=2)
        out.write("\n")
    written += 1

# Drop stale files from older runs that are no longer in the kept set (e.g. Element).
expected = {f"StructureDefinition-{i}.json" for i in ids}
for name in os.listdir(dest_dir):
    if name.startswith("StructureDefinition-") and name.endswith(".json") and name not in expected:
        os.remove(os.path.join(dest_dir, name))

print(f"Wrote {written} datatype StructureDefinitions from {src}")
if skipped:
    print(f"Excluded abstract/base types: {', '.join(sorted(skipped))}")
if written < 58:
    raise SystemExit(f"expected ≥58 R4 datatype SDs after base-type exclusions, got {written}")
PY

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

HL7_DT_PATHS=()
while IFS= read -r path; do
  HL7_DT_PATHS+=("$path")
done < <(python3 - "${HL7_DT_DIR}" <<'PY'
import glob, os, sys
dt_dir = sys.argv[1]
paths = sorted(
    os.path.realpath(p).replace("\\", "/")
    for p in glob.glob(os.path.join(dt_dir, "StructureDefinition-*.json"))
)
for p in paths:
    print(p)
if len(paths) < 58:
    raise SystemExit(f"expected ≥58 datatype SDs in {dt_dir}, got {len(paths)}")
# Abstract base types must stay out of the pack (registry loader requires derivation).
for banned in ("Element", "BackboneElement", "Resource", "DomainResource"):
    banned_path = os.path.join(dt_dir, f"StructureDefinition-{banned}.json")
    if os.path.isfile(banned_path):
        raise SystemExit(f"base type {banned} must not be in datatype pack: {banned_path}")
PY
)

echo "IG output: ${IG_OUTPUT}"
echo "Generating full scan manifest..."
python3 - "${IG_OUTPUT}" "${TMP_FULL}" <<'PY'
"""Scan an expanded IG tree for StructureDefinition / CodeSystem / ValueSet JSON."""
import json
import os
import sys

ig_root, out_path = sys.argv[1], sys.argv[2]
base = os.path.dirname(os.path.realpath(out_path))

def classify(path):
    try:
        with open(path, encoding="utf-8") as f:
            v = json.load(f)
    except Exception:
        return set()
    kinds = set()
    rt = v.get("resourceType")
    if rt in ("StructureDefinition", "CodeSystem", "ValueSet"):
        kinds.add(rt)
    elif rt == "Bundle":
        for entry in v.get("entry") or []:
            res = entry.get("resource") or {}
            rrt = res.get("resourceType")
            if rrt in ("StructureDefinition", "CodeSystem", "ValueSet"):
                kinds.add(rrt)
    return kinds

sd, cs, vs = [], [], []
for root, _dirs, files in os.walk(ig_root):
    for name in files:
        if not name.endswith(".json") or name in ("package.json", ".index.json"):
            continue
        path = os.path.join(root, name)
        kinds = classify(path)
        rel = os.path.relpath(path, base)
        if "StructureDefinition" in kinds:
            sd.append(rel)
        if "CodeSystem" in kinds:
            cs.append(rel)
        if "ValueSet" in kinds:
            vs.append(rel)

manifest = {
    "structure_definition_files": sorted(set(sd)),
    "code_system_files": sorted(set(cs)),
    "value_set_files": sorted(set(vs)),
}
with open(out_path, "w", encoding="utf-8") as f:
    json.dump(manifest, f, indent=2)
    f.write("\n")
print(f"Wrote {out_path} ({len(manifest['structure_definition_files'])} SDs)")
PY

# macOS / bash 3.2: empty arrays are "unbound" under `set -u`.
set +u
python3 - "${IG_OUTPUT}" "${CORE_OUT}" "${FULL_OUT}" "${TMP_FULL}" \
  "${#HL7_EXT_PATHS[@]}" "${HL7_EXT_PATHS[@]}" \
  -- \
  "${#HL7_DT_PATHS[@]}" "${HL7_DT_PATHS[@]}" \
  -- \
  "${NDHM_EXT_PATHS[@]}" <<'PY'
import glob
import json
import os
import sys

args = sys.argv[1:]

def take_counted(seq):
    """Pop N then N paths from seq (bash-friendly counted lists)."""
    if not seq:
        return [], seq
    n = int(seq[0])
    return list(seq[1 : 1 + n]), seq[1 + n :]

ig_output, core_out, full_out, tmp_full = args[0:4]
rest = args[4:]
hl7_ext_paths, rest = take_counted(rest)
if rest and rest[0] == "--":
    rest = rest[1:]
hl7_dt_paths, rest = take_counted(rest)
if rest and rest[0] == "--":
    rest = rest[1:]
ndhm_ext_paths = rest

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
core_paths.extend(p for p in sorted(hl7_dt_paths) if p not in core_paths)
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

hl7_ext_count = len(hl7_ext_paths)
hl7_dt_count = len(hl7_dt_paths)
ndhm_count = len(ndhm_ext_paths)
atrius_count = (
    len(core_manifest["structure_definition_files"])
    - hl7_ext_count
    - hl7_dt_count
    - ndhm_count
)
print(
    f"Wrote {core_out} ({atrius_count} Atrius + {hl7_ext_count} HL7 extension "
    f"+ {hl7_dt_count} HL7 datatype + {ndhm_count} NDHM StructureDefinitions)"
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

sq = os.path.join(manifest_dir, "deps/hl7-r4-datatypes/StructureDefinition-SimpleQuantity.json")
if not os.path.isfile(sq):
    raise SystemExit("ERROR: SimpleQuantity datatype SD missing after materialize")
PY
set -u

rm -f "${TMP_FULL}"
echo "Done. Point HFS at: manifests/atrius-r4-profile-manifest-core.json"
echo "  (or run ./scripts/setup-atrius-profile-registry.sh to build + verify)"
