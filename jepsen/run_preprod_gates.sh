#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   ./run_preprod_gates.sh            # seeds 1..20
#   ./run_preprod_gates.sh 1 10       # seeds 1..10
#   ./run_preprod_gates.sh 5 25       # seeds 5..25
#   ./run_preprod_gates.sh 1 20 soakA # explicit run id/tag

START_SEED="${1:-1}"
END_SEED="${2:-20}"
RUN_ID="${3:-preprod-$(date -u +%Y%m%dT%H%M%SZ)-$$}"

if ! [[ "$START_SEED" =~ ^[0-9]+$ && "$END_SEED" =~ ^[0-9]+$ ]]; then
  echo "start/end seed must be integers" >&2
  exit 2
fi
if (( START_SEED > END_SEED )); then
  echo "start seed must be <= end seed" >&2
  exit 2
fi

SCENARIOS=(
  register-3n-partition-flap
  lock-3n-partition-restart
  lock-5n-partition-restart
)

mkdir -p results results/preprod_logs
LOG_DIR="results/preprod_logs/${RUN_ID}"
mkdir -p "$LOG_DIR"
SUMMARY_FILE="results/PREPROD_GATES_SUMMARY_${RUN_ID}.md"
now_utc="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

{
  echo "# Preprod Jepsen Gates"
  echo
  echo "- Generated: ${now_utc}"
  echo "- Run ID: ${RUN_ID}"
  echo "- Seed range: ${START_SEED}..${END_SEED}"
  echo "- Scenarios: ${SCENARIOS[*]}"
  echo
  echo "| Scenario | Seed | Valid | Exit | History | Log |"
  echo "|---|---:|---|---:|---|---|"
} > "$SUMMARY_FILE"

fail_count=0
unknown_count=0

for scenario in "${SCENARIOS[@]}"; do
  for ((seed=START_SEED; seed<=END_SEED; seed++)); do
    run_tag="${RUN_ID}_${scenario}_seed${seed}"
    log_file="${LOG_DIR}/${scenario}_seed${seed}.log"

    echo "=== Running ${scenario} seed=${seed} ==="
    set +e
    ./run_scenario.sh "$scenario" "$seed" "$run_tag" > "$log_file" 2>&1
    exit_code=$?
    set -e

    valid="error"
    history_path="(none)"

    if grep -q "valid?:" "$log_file"; then
      valid="$(grep -E "valid\?:" "$log_file" | tail -1 | awk '{print $2}')"
    fi

    if grep -q "history path:" "$log_file"; then
      history_path="$(grep -E "history path:" "$log_file" | tail -1 | sed 's/.*history path:[[:space:]]*//')"
    fi

    if [[ "$valid" == "false" || "$valid" == "error" || $exit_code -ne 0 ]]; then
      fail_count=$((fail_count + 1))
    fi
    if [[ "$valid" == ":unknown" ]]; then
      unknown_count=$((unknown_count + 1))
    fi

    printf '| %s | %s | %s | %s | %s | %s |\n' \
      "$scenario" "$seed" "$valid" "$exit_code" "$history_path" "$log_file" >> "$SUMMARY_FILE"
  done

done

{
  echo
  echo "## Totals"
  echo
  echo "- Failing/error runs: ${fail_count}"
  echo "- Unknown runs: ${unknown_count}"
} >> "$SUMMARY_FILE"

echo "Wrote ${SUMMARY_FILE}"
echo "Logs: ${LOG_DIR}"
if (( fail_count > 0 )); then
  exit 1
fi
