#!/usr/bin/env bash
# Expand Atrius IG FHIR NPM package (.tgz) for manifest generation / cache seeding.
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
# Prefer ./scripts/setup-atrius-profile-registry.sh, which also seeds
# HFS_FHIR_PACKAGE_CACHE from this expansion.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARBALL="${ATRIUS_IG_PACKAGE_TGZ:-}"
URL="${ATRIUS_IG_PACKAGE_URL:-}"
# Default under manifests/ (gitignored); setup-atrius-profile-registry.sh copies
# into data/fhir-packages/{name}/{version}/ for HFS_FHIR_PACKAGE_CACHE.
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
