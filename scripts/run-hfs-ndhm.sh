#!/usr/bin/env bash
# Run the dedicated NDHM/ABDM export validator HFS (ndhm.in overlay, :8083).
#
#   ./scripts/setup-ndhm-profile-registry.sh
#   ./scripts/run-hfs-ndhm.sh
#
# Override: ENV_FILE=/path/to/env ./scripts/run-hfs-ndhm.sh

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=lib/common.sh
source "${ROOT}/scripts/lib/common.sh"

ATRIUS_HFS_PATH="${ATRIUS_HFS_PATH:-${ROOT}}"
if [[ -z "${ENV_FILE:-}" ]]; then
  if [[ -f "${ROOT}/deploy/env/hfs-ndhm-validate.env" ]]; then
    ENV_FILE="${ROOT}/deploy/env/hfs-ndhm-validate.env"
  else
    ENV_FILE="${ROOT}/deploy/clinical/.env.abdm.example"
  fi
fi
BUILD_HINT="cargo build --release -p helios-hfs --bin hfs --features postgres,elasticsearch,R4,subscriptions,otel"

source_env_file "${ENV_FILE}"
HFS_BIN="$(require_release_bin "${ATRIUS_HFS_PATH}" hfs "${BUILD_HINT}")"

mkdir -p "${ATRIUS_HFS_PATH}/data"
cd "${ATRIUS_HFS_PATH}"
echo "Starting NDHM export validator HFS (env: ${ENV_FILE}) on port ${HFS_SERVER_PORT:-8083}..."
echo "  packages=${HFS_FHIR_PACKAGES:-unset} mode=${HFS_VALIDATION_MODE:-off}"
exec "${HFS_BIN}"
