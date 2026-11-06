#!/usr/bin/env bash
# Run HTS (terminology server) with deploy/env/hts.env (release binary).
#
#   cp deploy/env/hts.env.example deploy/env/hts.env   # first time; edit paths
#   ./scripts/build-clinical-reasoning.sh
#   ./scripts/run-hts.sh

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=lib/common.sh
source "${ROOT}/scripts/lib/common.sh"

ATRIUS_HFS_PATH="${ATRIUS_HFS_PATH:-${ROOT}}"
ENV_FILE="${ENV_FILE:-${ROOT}/deploy/env/hts.env}"
BUILD_HINT="cargo build --release -p helios-hts --bin hts"

if [[ ! -f "${ENV_FILE}" ]]; then
  if [[ "${ENV_FILE}" == "${ROOT}/deploy/env/hts.env" && -f "${ROOT}/deploy/env/hts.env.example" ]]; then
    echo "Missing ${ENV_FILE} — creating from hts.env.example (local paths)." >&2
    sed \
      -e 's|^HTS_DATABASE_URL=.*|HTS_DATABASE_URL=./data/hts.db|' \
      -e 's|^# HTS_BOOTSTRAP_DIR=.*||' \
      "${ROOT}/deploy/env/hts.env.example" > "${ENV_FILE}"
  else
    echo "Missing ${ENV_FILE}. Copy deploy/env/hts.env.example or set ENV_FILE." >&2
    exit 1
  fi
fi

source_env_file "${ENV_FILE}"
HTS_BIN="$(require_release_bin "${ATRIUS_HFS_PATH}" hts "${BUILD_HINT}")"

mkdir -p "${ATRIUS_HFS_PATH}/data"
cd "${ATRIUS_HFS_PATH}"
echo "Starting HTS (env: ${ENV_FILE}) on port ${HTS_SERVER_PORT:-9091}..."
echo "  db=${HTS_DATABASE_URL:-unset}"
exec "${HTS_BIN}"
