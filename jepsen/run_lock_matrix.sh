#!/usr/bin/env bash
set -euo pipefail

# Usage: ./run_lock_matrix.sh [seeds]
# seeds can be comma-separated (e.g. 1,2,3) or a single integer.
SEEDS_RAW="${1:-1,2,3}"
RUN_ID="${2:-lock-matrix-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
IFS=',' read -r -a SEEDS <<< "$SEEDS_RAW"

SCENARIOS=(
  lock-3n-none
  lock-3n-partition-flap
  lock-3n-restart-one-small
  lock-3n-partition-restart
  lock-5n-none
)

mkdir -p results
SUMMARY_FILE="results/LOCK_MATRIX_SUMMARY_${RUN_ID}.md"
DETAILS_DIR="results/lock_matrix_logs/${RUN_ID}"
mkdir -p "$DETAILS_DIR"

now_utc="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

{
  echo "# Lock Jepsen Matrix"
  echo
  echo "- Generated: ${now_utc}"
  echo "- Run ID: ${RUN_ID}"
  echo "- Seeds: ${SEEDS_RAW}"
  echo "- Scenarios: ${SCENARIOS[*]}"
  echo
  echo "| Scenario | Seed | Valid | Exit | History | Log |"
  echo "|---|---:|---|---:|---|---|"
} > "$SUMMARY_FILE"

for scenario in "${SCENARIOS[@]}"; do
  for seed in "${SEEDS[@]}"; do
    run_tag="${RUN_ID}_${scenario}_seed${seed}"
    log_file="${DETAILS_DIR}/${scenario}_seed${seed}.log"

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

    printf '| %s | %s | %s | %s | %s | %s |\n' \
      "$scenario" "$seed" "$valid" "$exit_code" "$history_path" "$log_file" >> "$SUMMARY_FILE"
  done

done

{
  echo
  echo "## Notes"
  echo
  echo "- \`valid=true\` means checker accepted the history."
  echo "- \`valid=false\` means checker found a violation."
  echo "- \`valid=:unknown\` means checker was inconclusive (often search/resource bound)."
  echo "- \`valid=error\` means scenario failed before checker output; inspect log."
} >> "$SUMMARY_FILE"

echo "Wrote ${SUMMARY_FILE}"
echo "Logs: ${DETAILS_DIR}"
