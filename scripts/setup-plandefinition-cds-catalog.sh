#!/usr/bin/env bash
# Orchestrator: ensure Atrius PlanDefinitions are on KR, then regenerate the
# cds-server catalog (manifests/cds-services-kr.json).
#
# Roles:
#   setup-plandefinition-cds-catalog.sh  — import from AtriusIGDraft if needed,
#                                          then call generate-cds-hooks-manifest.py
#   generate-cds-hooks-manifest.py       — the actual writer (KR → JSON)
#
# Prerequisites: KR HFS running (./scripts/run-kr-hfs.sh).
# For import: AtriusIGDraft with translated Libraries (./scripts/translate-cql.sh)
# and sushi output (--clinical-reasoning).
#
# Usage:
#   ./scripts/setup-plandefinition-cds-catalog.sh
#   IMPORT_ATRIUS=1 ./scripts/setup-plandefinition-cds-catalog.sh
#   ATRIUS_IG_ROOT=/path/to/AtriusIGDraft ./scripts/setup-plandefinition-cds-catalog.sh
#   KR_BASE_URL=http://127.0.0.1:8079 MANIFEST_OUT=manifests/cds-services-kr.json \
#     ./scripts/setup-plandefinition-cds-catalog.sh

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PYTHONPATH="${REPO_ROOT}/scripts${PYTHONPATH:+:${PYTHONPATH}}"
KR_BASE="${KR_BASE_URL:-http://127.0.0.1:8079}"
MANIFEST_OUT="${MANIFEST_OUT:-$REPO_ROOT/manifests/cds-services-kr.json}"
IMPORT_ATRIUS="${IMPORT_ATRIUS:-0}"

cd "$REPO_ROOT"

resolve_atrius_ig() {
  if [[ -n "${ATRIUS_IG_ROOT:-}" ]]; then
    echo "$ATRIUS_IG_ROOT"
    return 0
  fi
  local candidate
  for candidate in \
    "$REPO_ROOT/../AtriusIGDraft" \
    "$REPO_ROOT/../../AtriusIGDraft" \
    "$HOME/AtriusIGDraft"; do
    if [[ -f "$candidate/scripts/import-atrius-kr-libraries.py" ]]; then
      echo "$(cd "$candidate" && pwd)"
      return 0
    fi
  done
  return 1
}

kr_counts() {
  python3 - "$KR_BASE" <<'PY'
import sys
from cds_manifest_common import kr_resource_count

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

echo "Atrius catalog: PlanDefinitions on KR → $MANIFEST_OUT" >&2

if [[ "$PLAN_COUNT" -eq 0 ]] || [[ "$IMPORT_ATRIUS" == "1" ]] || [[ "$LIB_COUNT" -eq 0 ]]; then
  if ! ATRIUS_IG="$(resolve_atrius_ig)"; then
    echo "error: no PlanDefinitions on KR and AtriusIGDraft not found" >&2
    echo "  Set ATRIUS_IG_ROOT=/path/to/AtriusIGDraft, or import first:" >&2
    echo "  (cd \$ATRIUS_IG && ./scripts/translate-cql.sh && ./scripts/import-atrius-kr-libraries.py --clinical-reasoning)" >&2
    exit 1
  fi
  if [[ "$PLAN_COUNT" -eq 0 ]] || [[ "$LIB_COUNT" -eq 0 ]]; then
    echo "KR has Library=$LIB_COUNT PlanDefinition=$PLAN_COUNT — importing from $ATRIUS_IG ..." >&2
  else
    echo "IMPORT_ATRIUS=1 — re-importing from $ATRIUS_IG ..." >&2
  fi
  python3 "$ATRIUS_IG/scripts/import-atrius-kr-libraries.py" \
    --kr-base-url "$KR_BASE" \
    --clinical-reasoning
  read -r LIB_COUNT PLAN_COUNT < <(kr_counts)
fi

if [[ "$PLAN_COUNT" -eq 0 ]]; then
  echo "error: no PlanDefinitions on KR" >&2
  echo "  Author PlanDefinitions in AtriusIGDraft, then:" >&2
  echo "  IMPORT_ATRIUS=1 ./scripts/setup-plandefinition-cds-catalog.sh" >&2
  exit 1
fi

echo "KR: $LIB_COUNT Library(ies), $PLAN_COUNT PlanDefinition(s)" >&2
echo "Regenerating CDS manifest -> $MANIFEST_OUT ..." >&2
python3 scripts/generate-cds-hooks-manifest.py \
  --kr-base-url "$KR_BASE" \
  --output "$MANIFEST_OUT"

echo "Done ($PLAN_COUNT PlanDefinition(s) on KR)." >&2
echo "Restart cds-server with CDS_SERVICES_MANIFEST_PATH=$MANIFEST_OUT" >&2
echo "Smoke: ./scripts/cds-cms165-prefetch-smoke.sh --apply" >&2
