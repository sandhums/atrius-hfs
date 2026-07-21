#!/usr/bin/env bash
# Build release binaries for the local clinical-reasoning stack.
#
# Usage:
#   ./scripts/build-clinical-reasoning.sh
#   ./scripts/build-clinical-reasoning.sh --skip-hts   # HFS + cds only
#
# Unsets CARGO_TARGET_DIR so binaries land in ./target/release (not a sandbox cache).

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"
unset CARGO_TARGET_DIR || true

SKIP_HTS=false
for arg in "$@"; do
  case "$arg" in
    --skip-hts) SKIP_HTS=true ;;
    -h|--help)
      sed -n '2,12p' "$0"
      exit 0
      ;;
  esac
done

echo "Building clinical-reasoning release binaries in ${ROOT}/target/release ..."

if [[ "${SKIP_HTS}" != "true" ]]; then
  cargo build --release -p helios-hts --bin hts
fi

# postgres matches deploy/env/hfs-*.env; redis optional for JTI when auth enabled
cargo build --release -p helios-hfs --bin hfs --features postgres,redis,R4
cargo build --release -p cds-server --bin cds-server

echo
echo "Built:"
ls -la target/release/hts target/release/hfs target/release/cds-server 2>/dev/null || true
echo
echo "Next (separate terminals):"
echo "  ./scripts/run-hts.sh"
echo "  ./scripts/run-hfs.sh"
echo "  ./scripts/run-kr-hfs.sh"
echo "  ./scripts/run-cql-sidecar.sh"
echo "  ./scripts/run-cds-server.sh"
