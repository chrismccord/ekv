#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Bootstrap a Fly app for EKV benchmarks.

Usage:
  bench/setup_fly.sh --app APP_NAME [options]

Options:
  --app NAME                 Fly app name (required)
  --org ORG                  Fly org/slug to create app in
  --regions R1,R2,...        Regions for volume bootstrap (default: iad,ord,sjc)
  --primary-region REGION    Fly primary_region in fly.toml (default: first from --regions)
  --volume-name NAME         Volume name/mount source (default: ekv_data)
  --volume-size-gb N         Volume size in GB (default: 1)
  --machines-per-region N    Target volumes per region (default: 1)
  --config PATH              fly.toml output path (default: bench/fly.toml)
  -h, --help                 Show this help

Example:
  bench/setup_fly.sh \
    --app ekv-bench-prod \
    --org personal \
    --regions iad,ord,sjc,ams
EOF
}

APP_NAME=""
ORG=""
REGIONS_CSV="iad,ord,sjc"
PRIMARY_REGION=""
VOLUME_NAME="ekv_data"
VOLUME_SIZE_GB="1"
MACHINES_PER_REGION="1"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_PATH="${SCRIPT_DIR}/fly.toml"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --app)
      APP_NAME="$2"
      shift 2
      ;;
    --org)
      ORG="$2"
      shift 2
      ;;
    --regions)
      REGIONS_CSV="$2"
      shift 2
      ;;
    --primary-region)
      PRIMARY_REGION="$2"
      shift 2
      ;;
    --volume-name)
      VOLUME_NAME="$2"
      shift 2
      ;;
    --volume-size-gb)
      VOLUME_SIZE_GB="$2"
      shift 2
      ;;
    --machines-per-region)
      MACHINES_PER_REGION="$2"
      shift 2
      ;;
    --config)
      CONFIG_PATH="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ -z "${APP_NAME}" ]]; then
  echo "--app is required" >&2
  usage
  exit 1
fi

if ! [[ "${VOLUME_SIZE_GB}" =~ ^[0-9]+$ ]] || [[ "${VOLUME_SIZE_GB}" -lt 1 ]]; then
  echo "--volume-size-gb must be an integer >= 1" >&2
  exit 1
fi

if ! [[ "${MACHINES_PER_REGION}" =~ ^[0-9]+$ ]] || [[ "${MACHINES_PER_REGION}" -lt 1 ]]; then
  echo "--machines-per-region must be an integer >= 1" >&2
  exit 1
fi

if command -v flyctl >/dev/null 2>&1; then
  FLY=(flyctl)
elif command -v fly >/dev/null 2>&1; then
  FLY=(fly)
else
  echo "flyctl (or fly) was not found in PATH" >&2
  exit 1
fi

if ! "${FLY[@]}" auth whoami >/dev/null 2>&1; then
  echo "Not authenticated with Fly. Run: ${FLY[*]} auth login" >&2
  exit 1
fi

MACHINE_LIST_CMD=()
if "${FLY[@]}" machine list --help >/dev/null 2>&1; then
  MACHINE_LIST_CMD=(machine list)
elif "${FLY[@]}" machines list --help >/dev/null 2>&1; then
  MACHINE_LIST_CMD=(machines list)
fi

if [[ "${#MACHINE_LIST_CMD[@]}" -eq 0 ]]; then
  echo "Your Fly CLI does not support 'machine list' or 'machines list'." >&2
  exit 1
fi

IFS=',' read -r -a REGIONS <<< "${REGIONS_CSV}"
if [[ "${#REGIONS[@]}" -eq 0 ]]; then
  echo "--regions must contain at least one region" >&2
  exit 1
fi

if [[ -z "${PRIMARY_REGION}" ]]; then
  PRIMARY_REGION="${REGIONS[0]}"
fi

echo "==> Ensuring app exists: ${APP_NAME}"
if "${FLY[@]}" "${MACHINE_LIST_CMD[@]}" --app "${APP_NAME}" >/dev/null 2>&1; then
  echo "    app already exists"
else
  if [[ -n "${ORG}" ]]; then
    "${FLY[@]}" apps create "${APP_NAME}" --org "${ORG}"
  else
    "${FLY[@]}" apps create "${APP_NAME}"
  fi
fi

echo "==> Writing Fly config: ${CONFIG_PATH}"
mkdir -p "$(dirname "${CONFIG_PATH}")"
cat > "${CONFIG_PATH}" <<EOF
app = "${APP_NAME}"
primary_region = "${PRIMARY_REGION}"

[mounts]
  source = "${VOLUME_NAME}"
  destination = "/data"

[[vm]]
  size = "performance-6x"
EOF

echo "==> Ensuring volumes (small: ${VOLUME_SIZE_GB}GB)"
for region in "${REGIONS[@]}"; do
  region="${region// /}"
  [[ -z "${region}" ]] && continue

  existing_count="$(
    "${FLY[@]}" volumes list --app "${APP_NAME}" 2>/dev/null \
      | awk -v vol="${VOLUME_NAME}" -v reg="${region}" '
          NR > 1 && $3 == vol && $5 == reg { c++ }
          END { print c + 0 }
        '
  )"

  missing=$((MACHINES_PER_REGION - existing_count))
  if (( missing <= 0 )); then
    echo "    ${region}: ${existing_count}/${MACHINES_PER_REGION} volumes already present"
    continue
  fi

  echo "    ${region}: creating ${missing} volume(s)"
  for _ in $(seq 1 "${missing}"); do
    "${FLY[@]}" volumes create "${VOLUME_NAME}" \
      --app "${APP_NAME}" \
      --region "${region}" \
      --size "${VOLUME_SIZE_GB}" \
      --require-unique-zone=false \
      --yes
  done
done

if [[ "${#MACHINE_LIST_CMD[@]}" -gt 0 ]] && \
   "${FLY[@]}" "${MACHINE_LIST_CMD[@]}" --app "${APP_NAME}" >/dev/null 2>&1; then
  machine_count="$(
    "${FLY[@]}" "${MACHINE_LIST_CMD[@]}" --app "${APP_NAME}" \
      | awk 'NR > 1 && NF > 0 { c++ } END { print c + 0 }'
  )"

  if (( machine_count > 0 )); then
    echo "==> Ensuring existing machines use performance-6x"
    "${FLY[@]}" scale vm performance-6x --app "${APP_NAME}"
  fi
fi

cat <<EOF

Bootstrap complete.

App:            ${APP_NAME}
Regions:        ${REGIONS_CSV}
Primary region: ${PRIMARY_REGION}
Volume name:    ${VOLUME_NAME}
Volume size:    ${VOLUME_SIZE_GB}GB
VM size:        performance-6x
Config file:    ${CONFIG_PATH}

Next steps:
  1) Deploy benchmark image/release with:
     ${FLY[*]} deploy --app ${APP_NAME} --config ${CONFIG_PATH}
  2) Verify placement and resources:
     ${FLY[*]} status --app ${APP_NAME}
     ${FLY[*]} volumes list --app ${APP_NAME}
     ${FLY[*]} machines list --app ${APP_NAME}
EOF
