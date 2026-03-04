#!/usr/bin/env bash
set -euo pipefail

SEED="${1:-1}"
RUN_ID="${2:-all-$(date -u +%Y%m%dT%H%M%SZ)-$$}"

SCENARIOS=(
  register-3n-none
  register-3n-partition-flap
  register-3n-restart-one-small
  register-5n-none
  lock-3n-partition-restart
)

for scenario in "${SCENARIOS[@]}"; do
  echo "\n=== Running ${scenario} (seed=${SEED}) ==="
  ./run_scenario.sh "$scenario" "$SEED" "${RUN_ID}_${scenario}_seed${SEED}"
done
