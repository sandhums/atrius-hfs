#!/usr/bin/env bash
# Run Clinical HFS with deploy/env/hfs-clinical.env
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ATRIUS_HFS_PATH="${ATRIUS_HFS_PATH:-${ROOT}}"
ENV_FILE="${ENV_FILE:-${ROOT}/deploy/env/hfs-clinical.env}"
HFS_BIN="${ATRIUS_HFS_PATH}/target/release/hfs"

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "Missing ${ENV_FILE}. Copy from deploy/env/hfs-clinical.env.example first." >&2
  exit 1
fi

if [[ ! -x "${HFS_BIN}" ]]; then
  echo "Release binary not found at ${HFS_BIN}." >&2
  echo "Build with: cargo build --release --bin hfs --features postgres,redis,R4" >&2
  exit 1
fi

export ATRIUS_HFS_PATH
set -a
# shellcheck disable=SC1090
source "${ENV_FILE}"
set +a

if [[ -n "${RUST_LOG:-}" ]]; then
  echo "Note: RUST_LOG=${RUST_LOG} overrides HFS_LOG_LEVEL (${HFS_LOG_LEVEL:-info})." >&2
  echo "      Include hfs=info,helios_auth=info in RUST_LOG to see auth startup logs." >&2
fi

mkdir -p "${ATRIUS_HFS_PATH}/data"

cd "${ATRIUS_HFS_PATH}"
echo "Starting Clinical HFS from ${ATRIUS_HFS_PATH} (env: ${ENV_FILE}) on port ${HFS_SERVER_PORT:-8082}..."
echo "  auth=${HFS_AUTH_ENABLED:-false} jti_revocation=${HFS_AUTH_JTI_REVOCATION:-false} log=${HFS_LOG_LEVEL:-info}"
exec "${HFS_BIN}"
