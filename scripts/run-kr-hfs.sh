#!/usr/bin/env bash
# Run Helios HFS configured as a Knowledge Repository (KR).
#
#   cp deploy/kr/.env.kr.example deploy/kr/.env.kr
#   edit deploy/kr/.env.kr
#   ./scripts/run-kr-hfs.sh
#
# Override env file: KR_ENV_FILE=/path/to/.env.kr ./scripts/run-kr-hfs.sh
# Extra args are passed to `cargo run -p helios-hfs -- ...` (e.g. --log-level debug).

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${KR_ENV_FILE:-$ROOT/deploy/kr/.env.kr}"

if [[ ! -f "$ENV_FILE" ]]; then
	echo "Missing env file: $ENV_FILE" >&2
	echo "Copy deploy/kr/.env.kr.example to deploy/kr/.env.kr or set KR_ENV_FILE." >&2
	exit 1
fi

set -a
# shellcheck source=/dev/null
source "$ENV_FILE"
set +a

cd "$ROOT"
exec cargo run -p helios-hfs -- "$@"
