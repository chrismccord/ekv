#!/usr/bin/env bash
set -euo pipefail

APP_NAME="ekv-bench"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
CONFIG_PATH="priv/bench_web/fly.toml"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --app)
      APP_NAME="$2"
      shift 2
      ;;
    --config)
      CONFIG_PATH="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1" >&2
      echo "Usage: priv/bench_web/deploy_fly.sh [--app APP] [--config CONFIG]" >&2
      exit 1
      ;;
  esac
done

if command -v flyctl >/dev/null 2>&1; then
  FLY=(flyctl)
elif command -v fly >/dev/null 2>&1; then
  FLY=(fly)
else
  echo "flyctl (or fly) was not found in PATH" >&2
  exit 1
fi

generate_hex_secret() {
  local bytes="$1"

  if command -v openssl >/dev/null 2>&1; then
    openssl rand -hex "${bytes}"
    return 0
  fi

  # Fallback keeps script functional on minimal environments.
  od -An -N "${bytes}" -tx1 /dev/urandom | tr -d ' \n'
}

ensure_secret() {
  local key="$1"
  local value="$2"
  local existing="$3"

  if printf '%s\n' "${existing}" | awk -v k="${key}" '$1 == k { found = 1 } END { exit(found ? 0 : 1) }'; then
    echo "==> Secret ${key} already present"
    return 0
  fi

  echo "==> Setting missing secret ${key}"
  "${FLY[@]}" secrets set "${key}=${value}" --app "${APP_NAME}" >/dev/null
}

cd "${REPO_ROOT}"

SECRET_NAMES="$("${FLY[@]}" secrets list --app "${APP_NAME}" 2>/dev/null | awk 'NR > 1 { print $1 }' || true)"

ensure_secret "SECRET_KEY_BASE" "$(generate_hex_secret 64)" "${SECRET_NAMES}"
ensure_secret "RELEASE_COOKIE" "$(generate_hex_secret 32)" "${SECRET_NAMES}"

"${FLY[@]}" deploy --app "${APP_NAME}" --config "${CONFIG_PATH}"
