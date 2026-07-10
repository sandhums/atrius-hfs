#!/usr/bin/env bash
# Run Knowledge Repository HFS with deploy/env/hfs-kr.env (release binary).
#
#   ./scripts/build-clinical-reasoning.sh
#   ./scripts/run-kr-hfs.sh
#
# Override: ENV_FILE=/path/to/env ./scripts/run-kr-hfs.sh
# (KR_ENV_FILE is accepted as an alias for ENV_FILE.)

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=lib/common.sh
source "${ROOT}/scripts/lib/common.sh"

ATRIUS_HFS_PATH="${ATRIUS_HFS_PATH:-${ROOT}}"
ENV_FILE="${ENV_FILE:-${KR_ENV_FILE:-${ROOT}/deploy/env/hfs-kr.env}}"
BUILD_HINT="cargo build --release -p helios-hfs --bin hfs --features postgres,redis,R4"

source_env_file "${ENV_FILE}"
HFS_BIN="$(require_release_bin "${ATRIUS_HFS_PATH}" hfs "${BUILD_HINT}")"

mkdir -p "${ATRIUS_HFS_PATH}/data"
cd "${ATRIUS_HFS_PATH}"
echo "Starting KR HFS (env: ${ENV_FILE}) on port ${HFS_SERVER_PORT:-8079}..."
echo "  db=${HFS_DATABASE_URL:-unset} terminology=${HFS_TERMINOLOGY_SERVER:-none}"
exec "${HFS_BIN}"
