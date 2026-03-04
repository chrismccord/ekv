# EKV Jepsen Harness (Local)

This directory contains a local Jepsen/Knossos verification harness for EKV.

It runs a concurrent workload against EKV consistent reads/writes, emits a
Jepsen-format history, and checks linearizability with Jepsen's
`checker/linearizable`.

## What this checks

- Single-key register semantics on `EKV.get(..., consistent: true)` and
  `EKV.put(..., consistent: true)` (`profile=register`).
- Single-key lock ownership semantics using CAS acquire/renew/release
  (`profile=lock`).
- Concurrency/interleaving correctness via a Jepsen linearizability checker.
- Workload executes across a real 3-node local Erlang cluster (via `:peer`).

## What this does not check

- Multi-key transactional properties.
- Byzantine/malicious node behavior.

## Run

From this directory:

```bash
export PATH="/opt/homebrew/opt/openjdk/bin:$PATH"
lein run
```

Custom run:

```bash
export PATH="/opt/homebrew/opt/openjdk/bin:$PATH"
lein run results/history.edn 8 1000 3 none register 1
```

Partition/heal flapping during workload:

```bash
export PATH="/opt/homebrew/opt/openjdk/bin:$PATH"
lein run results/history_partition_flap_3n.edn 8 3000 3 partition_flap register 1
```

Single-node restart churn during workload:

```bash
export PATH="/opt/homebrew/opt/openjdk/bin:$PATH"
lein run results/history_restart_one_3n_small.edn 8 800 3 restart_one register 1
```

5-node steady-state quorum:

```bash
export PATH="/opt/homebrew/opt/openjdk/bin:$PATH"
lein run results/history_5n_none.edn 8 2000 5 none register 1
```

Combined partition + restart lock profile:

```bash
export PATH="/opt/homebrew/opt/openjdk/bin:$PATH"
lein run results/history_lock_3n_partition_restart.edn 8 1200 3 partition_restart lock 1
```

Named scenario wrappers (repeatable):

```bash
export PATH="/opt/homebrew/opt/openjdk/bin:$PATH"
./run_scenario.sh register-3n-none 1
./run_scenario.sh register-3n-partition-flap 1
./run_scenario.sh register-3n-restart-one-small 1
./run_scenario.sh register-5n-none 1
./run_scenario.sh lock-3n-partition-restart 1
# optional explicit run tag (for deterministic artifact names)
./run_scenario.sh lock-5n-partition-restart 23 lock-repro-seed23
```

Lock-usecase matrix runner (multi-scenario + multi-seed summary):

```bash
export PATH="/opt/homebrew/opt/openjdk/bin:$PATH"
./run_lock_matrix.sh 1,2,3
# optional explicit run id
./run_lock_matrix.sh 1,2,3 lock-matrix-smoke
# writes results/LOCK_MATRIX_SUMMARY_<run_id>.md and results/lock_matrix_logs/<run_id>/
```

Arguments:

1. `history_path` (default: `results/history.edn`)
2. `workers` (default: `4`)
3. `ops` (default: `200`)
4. `cluster_nodes` (default: `3`, minimum `3`)
5. `mode` (default: `none`, one of `none|partition_flap|restart_one|partition_restart`)
6. `profile` (default: `register`, one of `register|lock`)
7. `seed` (default: `1`)

Start with the defaults, then increase gradually (`workers` and `ops`) as
the checker cost rises quickly with high overlap.

## Harness hardening notes

- `run_scenario.sh` now writes to unique history files by default (no overwrite).
  Use an explicit run tag (3rd arg, or `JEPSEN_RUN_TAG`) when deterministic
  naming is needed.
- Lock profile runs now establish a `nil` baseline for the lock key before
  workload generation, so checker initial-state assumptions match runtime state.
