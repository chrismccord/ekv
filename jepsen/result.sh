#!/usr/bin/env bash

# Exactly one canonical verdict is required. Missing, malformed, and duplicate
# verdicts are errors, even if the subprocess exited successfully.
jepsen_result() {
  awk '
    /^EKV_JEPSEN_RESULT=/ { count++; result = $0 }
    END {
      if (count == 1 && result ~ /^EKV_JEPSEN_RESULT=(true|false|unknown)$/) {
        sub(/^EKV_JEPSEN_RESULT=/, "", result)
        print result
      } else {
        print "error"
      }
    }
  ' "$1"
}

# Display-only metadata: never used to decide whether the run passed.
jepsen_history_path() {
  awk '
    /^  history path:/ { path = $0; sub(/^  history path:[[:space:]]*/, "", path) }
    END { print (path == "" ? "(none)" : path) }
  ' "$1"
}
