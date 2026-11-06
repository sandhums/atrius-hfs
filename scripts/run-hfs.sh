#!/usr/bin/env bash
# Run Clinical HFS with deploy/env/hfs-clinical.env (release binary).
#
#   ./scripts/build-clinical-reasoning.sh
#   ./scripts/run-hfs.sh
#
# Override: ENV_FILE=/path/to/env ./scripts/run-hfs.sh

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=lib/common.sh
source "${ROOT}/scripts/lib/common.sh"

ATRIUS_HFS_PATH="${ATRIUS_HFS_PATH:-${ROOT}}"
ENV_FILE="${ENV_FILE:-${ROOT}/deploy/env/hfs-clinical.env}"
BUILD_HINT="cargo build --release -p helios-hfs --bin hfs --features postgres,redis,R4"

source_env_file "${ENV_FILE}"
HFS_BIN="$(require_release_bin "${ATRIUS_HFS_PATH}" hfs "${BUILD_HINT}")"

if [[ -n "${RUST_LOG:-}" ]]; then
  echo "Note: RUST_LOG=${RUST_LOG} overrides HFS_LOG_LEVEL (${HFS_LOG_LEVEL:-info})." >&2
fi

mkdir -p "${ATRIUS_HFS_PATH}/data"
cd "${ATRIUS_HFS_PATH}"
echo "Starting Clinical HFS (env: ${ENV_FILE}) on port ${HFS_SERVER_PORT:-8082}..."
echo "  auth=${HFS_AUTH_ENABLED:-false} tenant_default=${HFS_DEFAULT_TENANT:-default} log=${HFS_LOG_LEVEL:-info}"
exec "${HFS_BIN}"
