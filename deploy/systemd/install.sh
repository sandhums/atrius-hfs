#!/usr/bin/env bash
# Install Atrius clinical reasoning stack systemd units and env templates.
#
# Prerequisites:
#   - Release binaries in target/release/ (or set BIN_SRC)
#   - JVM sidecar jar at SIDECAR_JAR_SRC (optional; skip sidecar unit if missing)
#   - sudo for systemd install
#
# Usage:
#   ./deploy/systemd/install.sh
#   ATRIUS_HOME=/opt/atrius ATRIUS_USER=atrius ./deploy/systemd/install.sh
#   BIN_SRC=./target/release SIDECAR_JAR_SRC=~/JVMsidecar/target/sidecar.jar ./deploy/systemd/install.sh
#
# After install:
#   sudo cp deploy/env/*.example → edit → /etc/atrius/*.env (script seeds if missing)
#   sudo systemctl enable --now atrius-clinical-reasoning.target

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ATRIUS_HOME="${ATRIUS_HOME:-/opt/atrius}"
ATRIUS_USER="${ATRIUS_USER:-atrius}"
ATRIUS_GROUP="${ATRIUS_GROUP:-$ATRIUS_USER}"
ENV_DIR="${ENV_DIR:-/etc/atrius}"
SYSTEMD_DIR="${SYSTEMD_DIR:-/etc/systemd/system}"
BIN_SRC="${BIN_SRC:-$ROOT/target/release}"
SIDECAR_JAR_SRC="${SIDECAR_JAR_SRC:-}"

need_sudo() {
	if [[ "$(id -u)" -ne 0 ]]; then
		echo sudo
	fi
}

SUDO="$(need_sudo)"

echo "Installing Atrius clinical reasoning stack to ${ATRIUS_HOME} (user: ${ATRIUS_USER})"

if ! id -u "$ATRIUS_USER" &>/dev/null; then
	echo "Creating system user ${ATRIUS_USER} ..."
	$SUDO useradd --system --home-dir "$ATRIUS_HOME" --shell /usr/sbin/nologin "$ATRIUS_USER" 2>/dev/null \
		|| $SUDO useradd --system --home-dir "$ATRIUS_HOME" --shell /bin/false "$ATRIUS_USER"
fi

$SUDO mkdir -p "$ATRIUS_HOME"/{bin,lib,data,manifests,share/docs}
$SUDO mkdir -p "$ENV_DIR"

install_bin() {
	local name="$1"
	local src="$BIN_SRC/$name"
	if [[ ! -x "$src" && ! -f "$src" ]]; then
		echo "error: missing binary $src — run: cargo build --release -p helios-hfs --bin hfs && cargo build --release --bin hts -p cds-server" >&2
		exit 1
	fi
	echo "  bin/$name"
	$SUDO install -m 0755 "$src" "$ATRIUS_HOME/bin/$name"
}

echo "Installing Rust binaries from ${BIN_SRC} ..."
install_bin hts
install_bin hfs
install_bin cds-server

echo "Installing sidecar launcher ..."
$SUDO install -m 0755 "$ROOT/deploy/bin/run-cql-sidecar.sh" "$ATRIUS_HOME/bin/run-cql-sidecar.sh"

if [[ -n "$SIDECAR_JAR_SRC" && -f "$SIDECAR_JAR_SRC" ]]; then
	echo "Installing sidecar jar ..."
	$SUDO install -m 0644 "$SIDECAR_JAR_SRC" "$ATRIUS_HOME/lib/cql-sidecar.jar"
else
	echo "note: SIDECAR_JAR_SRC not set or missing — install jar to ${ATRIUS_HOME}/lib/cql-sidecar.jar before starting atrius-cql-sidecar"
fi

echo "Installing manifests and data dir skeleton ..."
if [[ -d "$ROOT/manifests" ]]; then
	$SUDO cp -a "$ROOT/manifests/." "$ATRIUS_HOME/manifests/"
fi
if [[ -d "$ROOT/data" ]]; then
	# Copy only non-db artifacts (search params, etc.) — never overwrite production DBs
	for f in "$ROOT/data"/*; do
		[[ -e "$f" ]] || continue
		case "$f" in
		*.db|*.db-shm|*.db-wal) continue ;;
		esac
		$SUDO cp -a "$f" "$ATRIUS_HOME/data/" 2>/dev/null || true
	done
fi

if [[ -f "$ROOT/docs/clinical-reasoning/production-deployment.md" ]]; then
	$SUDO cp "$ROOT/docs/clinical-reasoning/production-deployment.md" "$ATRIUS_HOME/share/docs/"
fi

echo "Installing env templates to ${ENV_DIR} (skip if already present) ..."
for example in "$ROOT/deploy/env/"*.env.example; do
	base="$(basename "$example" .env.example)"
	dest="$ENV_DIR/${base}.env"
	if [[ -f "$dest" ]]; then
		echo "  keep existing $dest"
	else
		echo "  seed $dest from example"
		$SUDO cp "$example" "$dest"
		$SUDO chmod 0640 "$dest"
		$SUDO chown root:"$ATRIUS_GROUP" "$dest" 2>/dev/null || $SUDO chown root:root "$dest"
	fi
done

echo "Installing systemd units to ${SYSTEMD_DIR} ..."
for unit in "$ROOT/deploy/systemd/"atrius-*.service "$ROOT/deploy/systemd/"atrius-*.target; do
	[[ -f "$unit" ]] || continue
	echo "  $(basename "$unit")"
	$SUDO cp "$unit" "$SYSTEMD_DIR/"
done

$SUDO chown -R "$ATRIUS_USER:$ATRIUS_GROUP" "$ATRIUS_HOME/data" "$ATRIUS_HOME/lib" 2>/dev/null || true
$SUDO chown -R "$ATRIUS_USER:$ATRIUS_GROUP" "$ATRIUS_HOME"

echo "Reloading systemd ..."
$SUDO systemctl daemon-reload

echo ""
echo "Done. Next steps:"
echo "  1. Edit env files in ${ENV_DIR}/ (especially URLs and DB paths)"
echo "  2. Import terminology + clinical + KR data (see production-deployment.md)"
echo "  3. sudo systemctl enable --now atrius-clinical-reasoning.target"
echo "  4. systemctl status 'atrius-*'"
