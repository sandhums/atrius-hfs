#!/usr/bin/env bash
# Run the JVM clinical-reasoning sidecar (local dev).
#
# Prefers a packaged jar when SIDECAR_JAR is set; otherwise runs from the
# JVMsidecar Maven project via `mvn exec:java`.
#
#   # Option A — jar (production-like)
#   export SIDECAR_JAR=/path/to/cql-sidecar.jar
#   ./scripts/run-cql-sidecar.sh
#
#   # Option B — Maven project (default for local)
#   export JVMSIDECAR_HOME=~/IdeaProjects/JVMsidecar
#   ./scripts/run-cql-sidecar.sh
#
# Env file (optional): deploy/env/cql-sidecar.env

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=lib/common.sh
source "${ROOT}/scripts/lib/common.sh"

ENV_FILE="${ENV_FILE:-${ROOT}/deploy/env/cql-sidecar.env}"
if [[ -f "${ENV_FILE}" ]]; then
  source_env_file "${ENV_FILE}"
fi

SIDECAR_PORT="${SIDECAR_PORT:-8088}"
JAVA_OPTS="${JAVA_OPTS:--Xmx1024m}"
export SIDECAR_PORT

if [[ -n "${SIDECAR_JAR:-}" && -f "${SIDECAR_JAR}" ]]; then
  echo "Starting CQL sidecar from jar ${SIDECAR_JAR} on port ${SIDECAR_PORT}..."
  # shellcheck disable=SC2086
  exec java ${JAVA_OPTS} -jar "${SIDECAR_JAR}"
fi

JVMSIDECAR_HOME="${JVMSIDECAR_HOME:-${HOME}/IdeaProjects/JVMsidecar}"
if [[ ! -d "${JVMSIDECAR_HOME}" ]]; then
  echo "JVMsidecar project not found at ${JVMSIDECAR_HOME}." >&2
  echo "Set JVMSIDECAR_HOME or SIDECAR_JAR (see deploy/env/cql-sidecar.env.example)." >&2
  exit 1
fi

echo "Starting CQL sidecar via Maven from ${JVMSIDECAR_HOME} on port ${SIDECAR_PORT}..."
echo "  (rebuild after code changes: cd ${JVMSIDECAR_HOME} && mvn -q -DskipTests compile)"
cd "${JVMSIDECAR_HOME}"
exec mvn -q exec:java -Dexec.mainClass=com.atrius.sidecar.MainKt
