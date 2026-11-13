#!/usr/bin/env bash
# Run registry hub HFS (tenant atrius-group, audience hfs-hub) for ADR 013 local dev.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${HFS_HUB_ENV:-${ROOT}/deploy/env/hfs-hub.env}"

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "Missing ${ENV_FILE}." >&2
  echo "Copy deploy/env/hfs-hub.env.example to deploy/env/hfs-hub.env and set HFS_DATABASE_URL." >&2
  exit 1
fi

set -a
# shellcheck disable=SC1090
source "${ENV_FILE}"
set +a

mkdir -p "${ROOT}/data"

# helios-hfs defaults to sqlite; HFS_STORAGE_BACKEND=postgres needs --features postgres.
# Match clinical with: export HFS_CARGO_FEATURES=postgres,elasticsearch,subscriptions
CARGO_FEATURES="${HFS_CARGO_FEATURES:-postgres}"

echo "Starting registry hub HFS from ${ROOT} on port ${HFS_SERVER_PORT:-8083} (tenant ${HFS_DEFAULT_TENANT:-atrius-group}, features ${CARGO_FEATURES})..."
exec cargo run --release --bin hfs --features "${CARGO_FEATURES}"
