#!/usr/bin/env bash
# Launch JVM CQL sidecar — installed to /opt/atrius/bin/run-cql-sidecar.sh
# Environment from /etc/atrius/cql-sidecar.env (SIDECAR_JAR, JAVA_OPTS, SIDECAR_PORT).

set -euo pipefail

: "${SIDECAR_JAR:?SIDECAR_JAR must be set (see /etc/atrius/cql-sidecar.env)}"

if [[ ! -f "$SIDECAR_JAR" ]]; then
	echo "Sidecar jar not found: $SIDECAR_JAR" >&2
	echo "Build JVMsidecar and install the jar (see docs/clinical-reasoning/production-deployment.md)." >&2
	exit 1
fi

JAVA_OPTS="${JAVA_OPTS:--Xmx1024m}"
exec /usr/bin/java ${JAVA_OPTS} -jar "$SIDECAR_JAR"
