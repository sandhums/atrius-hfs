# Shared helpers for scripts/run-*.sh (source from those scripts only).
# shellcheck shell=bash

# Source an env file with set -a.
source_env_file() {
  local env_file="$1"
  if [[ ! -f "${env_file}" ]]; then
    echo "Missing ${env_file}" >&2
    echo "Copy the matching deploy/env/*.env.example (or set ENV_FILE)." >&2
    return 1
  fi
  set -a
  # shellcheck disable=SC1090
  source "${env_file}"
  set +a
}

# Print path to release binary or fail with a build hint.
# Args: atrius_hfs_path  bin_name  build_hint
require_release_bin() {
  local root="$1"
  local bin_name="$2"
  local build_hint="$3"
  local bin_path="${root}/target/release/${bin_name}"
  if [[ ! -x "${bin_path}" ]]; then
    echo "Release binary not found at ${bin_path}." >&2
    echo "Build with: ${build_hint}" >&2
    echo "Or: ./scripts/build-clinical-reasoning.sh" >&2
    return 1
  fi
  printf '%s' "${bin_path}"
}
