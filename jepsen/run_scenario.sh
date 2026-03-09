#!/usr/bin/env bash
set -euo pipefail

SCENARIO="${1:-}"
SEED="${2:-1}"
RUN_TAG_RAW="${3:-${JEPSEN_RUN_TAG:-$(date -u +%Y%m%dT%H%M%SZ)-$$-$RANDOM}}"
RUN_TAG="$(printf '%s' "$RUN_TAG_RAW" | tr -cs 'A-Za-z0-9._-' '_')"

if [[ -z "$SCENARIO" ]]; then
  cat <<USAGE
Usage: ./run_scenario.sh <scenario> [seed] [run_tag]

Scenarios:
  register-3n-none
  register-3n-partition-flap
  register-3n-restart-one-small
  register-5n-none
  lock-3n-none
  lock-3n-partition-flap
  lock-3n-restart-one-small
  lock-3n-partition-restart
  lock-5n-none
  lock-5n-partition-restart

Notes:
- Histories are written to unique per-run files by default.
- Set JEPSEN_RUN_TAG (or pass run_tag) for deterministic artifact names.
USAGE
  exit 1
fi

if ! [[ "$SEED" =~ ^[0-9]+$ ]]; then
  echo "seed must be an integer" >&2
  exit 2
fi

if [[ -z "$RUN_TAG" ]]; then
  RUN_TAG="$(date -u +%Y%m%dT%H%M%SZ)-$$"
fi

workers=8
ops=0
cluster_nodes=0
mode=""
profile=""

case "$SCENARIO" in
  register-3n-none)
    ops=2000
    cluster_nodes=3
    mode="none"
    profile="register"
    ;;
  register-3n-partition-flap)
    ops=2000
    cluster_nodes=3
    mode="partition_flap"
    profile="register"
    ;;
  register-3n-restart-one-small)
    ops=800
    cluster_nodes=3
    mode="restart_one"
    profile="register"
    ;;
  register-5n-none)
    ops=2000
    cluster_nodes=5
    mode="none"
    profile="register"
    ;;
  lock-3n-none)
    ops=1200
    cluster_nodes=3
    mode="none"
    profile="lock"
    ;;
  lock-3n-partition-flap)
    ops=1500
    cluster_nodes=3
    mode="partition_flap"
    profile="lock"
    ;;
  lock-3n-restart-one-small)
    ops=700
    cluster_nodes=3
    mode="restart_one"
    profile="lock"
    ;;
  lock-3n-partition-restart)
    ops=1200
    cluster_nodes=3
    mode="partition_restart"
    profile="lock"
    ;;
  lock-5n-none)
    ops=1200
    cluster_nodes=5
    mode="none"
    profile="lock"
    ;;
  lock-5n-partition-restart)
    ops=1200
    cluster_nodes=5
    mode="partition_restart"
    profile="lock"
    ;;
  *)
    echo "Unknown scenario: $SCENARIO" >&2
    exit 2
    ;;
esac

history_base="${SCENARIO//-/_}"
history_path="results/history_${history_base}_seed${SEED}_${RUN_TAG}.edn"

exec lein run "$history_path" "$workers" "$ops" "$cluster_nodes" "$mode" "$profile" "$SEED"
