#!/usr/bin/env bash
# Run cr-fhir-bridge with deploy/env/cr-fhir-bridge.env (release binary).
#
#   cp deploy/env/cr-fhir-bridge.env.example deploy/env/cr-fhir-bridge.env
#   ./scripts/build-clinical-reasoning.sh
#   ./scripts/run-cr-fhir-bridge.sh
#
# Prerequisites: clinical HFS (:8082) and KR (:8079) should be up.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=lib/common.sh
source "${ROOT}/scripts/lib/common.sh"

ATRIUS_HFS_PATH="${ATRIUS_HFS_PATH:-${ROOT}}"
ENV_FILE="${ENV_FILE:-${ROOT}/deploy/env/cr-fhir-bridge.env}"
BUILD_HINT="cargo build --release -p cr-fhir-bridge --bin cr-fhir-bridge"

if [[ ! -f "${ENV_FILE}" ]]; then
  if [[ "${ENV_FILE}" == "${ROOT}/deploy/env/cr-fhir-bridge.env" && -f "${ROOT}/deploy/env/cr-fhir-bridge.env.example" ]]; then
    echo "Missing ${ENV_FILE} — copying from cr-fhir-bridge.env.example." >&2
    cp "${ROOT}/deploy/env/cr-fhir-bridge.env.example" "${ENV_FILE}"
  else
    echo "Missing ${ENV_FILE}. Copy deploy/env/cr-fhir-bridge.env.example or set ENV_FILE." >&2
    exit 1
  fi
fi

source_env_file "${ENV_FILE}"
BRIDGE_BIN="$(require_release_bin "${ATRIUS_HFS_PATH}" cr-fhir-bridge "${BUILD_HINT}")"

cd "${ATRIUS_HFS_PATH}"
echo "Starting cr-fhir-bridge (env: ${ENV_FILE}) on port ${CR_FHIR_BRIDGE_PORT:-8081}..."
echo "  upstream=${CR_FHIR_BRIDGE_UPSTREAM_URL:-unset} kr=${CR_FHIR_BRIDGE_KR_URL:-unset} tenant=${CR_FHIR_BRIDGE_DEFAULT_TENANT:-none}"
exec "${BRIDGE_BIN}"
