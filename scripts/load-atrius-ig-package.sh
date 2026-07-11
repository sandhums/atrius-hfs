#!/usr/bin/env bash
# Expand Atrius IG FHIR NPM package (.tgz) for ProfileRegistry manifest generation.
#
# Usage:
#   # From local tarball (CI artifact or post-publish-package.sh output)
#   ATRIUS_IG_PACKAGE_TGZ=/path/to/package.tgz ./scripts/load-atrius-ig-package.sh
#
#   # From published URL (default: atrius.in canonical package)
#   ATRIUS_IG_PACKAGE_URL=https://atrius.in/fhir/r4/atrius-in/package.tgz \
#     ./scripts/load-atrius-ig-package.sh
#
# Prints expanded directory path on stdout (for ATRIUS_IG_EXPANDED).

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARBALL="${ATRIUS_IG_PACKAGE_TGZ:-}"
URL="${ATRIUS_IG_PACKAGE_URL:-}"
# Default under manifests/ so relative HFS_PROFILE_MANIFEST entries stay portable.
EXPAND_DIR="${ATRIUS_IG_EXPANDED:-${ROOT}/manifests/atrius-ig-package}"

if [[ -z "${TARBALL}" && -z "${URL}" ]]; then
  URL="https://atrius.in/fhir/r4/atrius-in/package.tgz"
fi

mkdir -p "${EXPAND_DIR}"
rm -rf "${EXPAND_DIR:?}/"*

if [[ -n "${TARBALL}" ]]; then
  if [[ ! -f "${TARBALL}" ]]; then
    echo "ATRIUS_IG_PACKAGE_TGZ not found: ${TARBALL}" >&2
    exit 1
  fi
  tar -xzf "${TARBALL}" -C "${EXPAND_DIR}" --strip-components=1
else
  echo "Fetching ${URL} …" >&2
  curl -fsSL "${URL}" | tar -xz -C "${EXPAND_DIR}" --strip-components=1
fi

if [[ ! -f "${EXPAND_DIR}/package.json" ]]; then
  echo "Expanded package missing package.json in ${EXPAND_DIR}" >&2
  exit 1
fi

echo "${EXPAND_DIR}"
