#!/usr/bin/env bash
# Slice 3: synthesize PlanDefinitions (not in eCQM NPM), upload to KR, regenerate manifest.
# DO NOT USE  -- NOW WE ARE AUTHORING
# The eCQM package (ecqm-content-qicore-2025) ships Library + Measure only — no
# PlanDefinition. This script generates CDSHooksServicePlanDefinition from those
# libraries, PUTs them to KR, then builds a PlanDefinition-first CDS manifest.
#
# Prerequisites: KR HFS running (./scripts/run-kr-hfs.sh).
#
# Usage:
#   ./scripts/setup-plandefinition-cds-catalog.sh
#   ECQM_IMPORT_LIBS=1 ./scripts/setup-plandefinition-cds-catalog.sh   # also import NPM libraries
#   ECQM_FROM_PACKAGE=1 ./scripts/setup-plandefinition-cds-catalog.sh  # read Library/Measure from .tgz
#   KR_BASE_URL=http://127.0.0.1:8079 ./scripts/setup-plandefinition-cds-catalog.sh
#   MANIFEST_OUT=manifests/cds-services-kr-ecqm.json ./scripts/setup-plandefinition-cds-catalog.sh

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PYTHONPATH="${REPO_ROOT}/scripts${PYTHONPATH:+:${PYTHONPATH}}"
KR_BASE="${KR_BASE_URL:-http://127.0.0.1:8079}"
MANIFEST_OUT="${MANIFEST_OUT:-$REPO_ROOT/manifests/cds-services-kr-ecqm.json}"
ECQM_IMPORT_LIBS="${ECQM_IMPORT_LIBS:-0}"
ECQM_FROM_PACKAGE="${ECQM_FROM_PACKAGE:-0}"

cd "$REPO_ROOT"

echo "Note: eCQM NPM package has Library + Measure only — PlanDefinitions are synthesized." >&2

kr_counts() {
  python3 - "$KR_BASE" <<'PY'
import sys
from ecqm_cds_common import kr_resource_count

base = sys.argv[1]
try:
    libs = kr_resource_count(base, "Library")
    plans = kr_resource_count(base, "PlanDefinition")
except OSError as exc:
    print(f"error: cannot reach KR at {base}: {exc}", file=sys.stderr)
    sys.exit(2)
print(f"{libs} {plans}")
PY
}

if ! read -r LIB_COUNT PLAN_COUNT < <(kr_counts); then
  echo "hint: start KR with ./scripts/run-kr-hfs.sh" >&2
  exit 1
fi

if [[ "$LIB_COUNT" -eq 0 ]] || [[ "$ECQM_IMPORT_LIBS" == "1" ]]; then
  if [[ "$LIB_COUNT" -eq 0 ]]; then
    echo "KR has no Libraries — importing eCQM NPM package ..." >&2
  else
    echo "ECQM_IMPORT_LIBS=1 — re-importing eCQM NPM package ..." >&2
  fi
  python3 scripts/import-ecqm-kr-libraries.py \
    --download \
    --kr-base-url "$KR_BASE" \
    --batch-size 1
  read -r LIB_COUNT PLAN_COUNT < <(kr_counts)
fi

if [[ "$LIB_COUNT" -eq 0 ]]; then
  echo "error: KR still has no Library resources after import" >&2
  exit 1
fi

echo "KR: $LIB_COUNT Library(ies), $PLAN_COUNT PlanDefinition(s) before synthesis" >&2

GEN_ARGS=(--kr-base-url "$KR_BASE" --upload)
if [[ "$ECQM_FROM_PACKAGE" == "1" ]]; then
  GEN_ARGS=(--download "${GEN_ARGS[@]}")
  echo "Synthesizing PlanDefinitions from eCQM package tarball ..." >&2
else
  echo "Synthesizing PlanDefinitions from KR Libraries ($LIB_COUNT) ..." >&2
fi

python3 scripts/generate-ecqm-plandefinitions.py "${GEN_ARGS[@]}"

read -r _ PLAN_COUNT < <(kr_counts)
if [[ "$PLAN_COUNT" -eq 0 ]]; then
  echo "error: no PlanDefinitions on KR after upload — check generate-ecqm-plandefinitions.py output" >&2
  exit 1
fi

echo "Regenerating CDS manifest (PlanDefinition-first) -> $MANIFEST_OUT ..." >&2
python3 scripts/generate-cds-hooks-manifest.py --from-plandefinition \
  --kr-base-url "$KR_BASE" \
  --output "$MANIFEST_OUT"

echo "Done ($PLAN_COUNT PlanDefinition(s) on KR)." >&2
echo "Restart cds-server with CDS_SERVICES_MANIFEST_PATH=$MANIFEST_OUT" >&2
echo "Smoke (legacy evaluate): ./scripts/cds-cms165-prefetch-smoke.sh" >&2
echo "Smoke (PlanDefinition \$apply): ./scripts/cds-cms165-prefetch-smoke.sh --apply" >&2
