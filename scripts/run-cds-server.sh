#!/usr/bin/env bash
# Run cds-server with deploy/env/cds-server.env (release binary).
#
#   cp deploy/env/cds-server.env deploy/env/cds-server.env  # then fix manifest path
#   ./scripts/build-clinical-reasoning.sh
#   ./scripts/run-cds-server.sh
#
# Prerequisites: bridge (:8081), KR (:8079), HTS (:9091), sidecar (:8088).

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=lib/common.sh
source "${ROOT}/scripts/lib/common.sh"

ATRIUS_HFS_PATH="${ATRIUS_HFS_PATH:-${ROOT}}"
ENV_FILE="${ENV_FILE:-${ROOT}/deploy/env/cds-server.env}"
BUILD_HINT="cargo build --release -p cds-server --bin cds-server"

if [[ ! -f "${ENV_FILE}" ]]; then
  if [[ "${ENV_FILE}" == "${ROOT}/deploy/env/cds-server.env" && -f "${ROOT}/deploy/env/cds-server.env.example" ]]; then
    echo "Missing ${ENV_FILE} — creating local cds-server.env from example." >&2
    sed \
      -e "s|^CDS_SERVICES_MANIFEST_PATH=.*|CDS_SERVICES_MANIFEST_PATH=${ROOT}/manifests/cds-services-kr.json|" \
      -e 's|^CDS_REQUIRE_LIBRARY_VERSION=true|CDS_REQUIRE_LIBRARY_VERSION=false|' \
      -e 's|^CDS_VALIDATE_KR_LIBRARIES=true|CDS_VALIDATE_KR_LIBRARIES=false|' \
      "${ROOT}/deploy/env/cds-server.env.example" > "${ENV_FILE}"
  else
    echo "Missing ${ENV_FILE}. Copy deploy/env/cds-server.env.example or set ENV_FILE." >&2
    exit 1
  fi
fi

source_env_file "${ENV_FILE}"
CDS_BIN="$(require_release_bin "${ATRIUS_HFS_PATH}" cds-server "${BUILD_HINT}")"

cd "${ATRIUS_HFS_PATH}"
echo "Starting cds-server (env: ${ENV_FILE}) on port ${CDS_SERVER_PORT:-8095}..."
echo "  hfs(bridge)=${CDS_HFS_BASE_URL:-unset} kr=${CDS_LIBRARY_BASE_URL:-unset} sidecar=${CDS_CLINICAL_REASONING_URL:-unset}"
echo "  local manifest=${CDS_SERVICES_MANIFEST_PATH:-unset}"
echo "  loading manifest from KR Binary=${CDS_KR_SERVICES_BINARY_ID:-no KR Binary}"
exec "${CDS_BIN}"
