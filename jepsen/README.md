# EKV Jepsen Harness (Local)

This directory contains a local Jepsen/Knossos verification harness for EKV.

It runs a concurrent workload against EKV consistent reads/writes, emits a
Jepsen-format history, and checks linearizability with Jepsen's
`checker/linearizable` and a strict register model. Coverage and lock-version
checks must also pass; a linearizable but unexercised workload is not a pass.

## What this checks

- Single-key register semantics on `EKV.get(..., consistent: true)` and
  `EKV.put(..., consistent: true)` (`profile=register`).
- Single-key lock ownership and token transitions using CAS acquire/renew/release
  (`profile=lock`). Expected and returned VSNs must agree with observed
  value/version bindings.
- Concurrency/interleaving correctness via a Jepsen linearizability checker.
- Workload executes across real 3- or 5-node local Erlang clusters (via `:peer`).
- Successful `nil` reads mean absence, not an unknown value.
- Fault runs require two completed fault cycles with successful operations
  invoked and completed while each fault is active.
- After healing, the last concurrent state is read through every member before
  new writes can mask data loss. A fault-free write/lock lifecycle and final
  member reads must also succeed.

## What this does not check

- Multi-key transactional properties.
- Byzantine/malicious node behavior.
- TTL/lease expiry or fencing of an external protected resource. The lock has
  no TTL: renewal replaces its token using the current VSN.
- LWW convergence, GC, clock skew, client/observer routing, and blue-green
  handoff. These profiles exercise member-mode CAS on one key.
- Host power loss or physical network faults. Crash modes abruptly halt a BEAM
  VM without shutdown callbacks; the OS and its page cache remain alive.

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

Single-member **graceful supervisor restart** during workload:

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

Abrupt VM crash/restart, with the same database and logical member identity:

```bash
lein run results/history_lock_3n_partition_crash.edn 8 1200 3 partition_crash lock 1
./run_scenario.sh register-3n-crash-one 1
./run_scenario.sh lock-3n-partition-crash 1
./run_scenario.sh lock-5n-partition-crash 1
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
3. `ops` (default: `200`, minimum total workload operations)
4. `cluster_nodes` (default: `3`, minimum `3`)
5. `mode` (default: `none`, one of `none|partition_flap|restart_one|partition_restart|crash_one|partition_crash`)
6. `profile` (default: `register`, one of `register|lock`)
7. `seed` (default: `1`)

Start with the defaults, then increase gradually (`workers` and `ops`) as
the checker cost rises quickly with high overlap. Workers continue beyond
their operation minimum until both fault cycles have completed. Setup,
reconciliation, and recovery add history events as well.

## Harness hardening notes

- Every runner requires exit status zero and one explicit
  `EKV_JEPSEN_RESULT=true` verdict. Inconclusive (`unknown`), missing, malformed,
  or duplicate verdicts fail closed. Matrix runners finish collecting their
  summaries before returning a failure status.
- Run harness regression checks with `lein test` here and
  `mix test test/jepsen_runner_test.exs` from the repository root.
- `run_scenario.sh` now writes to unique history files by default (no overwrite).
  Use an explicit run tag (3rd arg, or `JEPSEN_RUN_TAG`) when deterministic
  naming is needed.
- Each run allocates fresh databases and verifies its absent baseline with
  consistent reads. It never silently clears or reuses an old database.
- Lock workers retain tokens for ambiguous writes. They record each resolution
  read separately and recover versions only from exact-token lookups. Eventual
  lookup observations constrain version bindings, not linearizable read order.
  Workers also deliberately attempt renewal/release using older VSNs, including
  versions from previous acquisitions by the same owner.
- Both phases (`workload` and `recovery`) require successful reads and writes;
  for locks, acquire, renew, and release must each succeed. A coverage failure
  means insufficient exercise or recovery, not necessarily a safety violation.
- Partitions block implicit reconnects with different per-peer distribution
  cookies, without blocking coordinator access. The topology is checked before
  and after the active interval. Combined modes restart/crash the isolated member.
- The history records node, phase, event order/time, fault start/healing/end,
  and completed recovery. All fault and worker tasks are joined or stopped
  before teardown; partial histories survive generator failures.
- Databases are retained alongside the history as `<history>.data-<run-id>/`,
  including on failures. Remove unneeded run artifacts periodically; matrices
  retain one data directory per run. They belong under ignored `results/`.
- Seeds repeat worker random choices and fault targets, not OS scheduling or
  an exact concurrent execution.
- Harness regressions: `lein test` here, and
  `mix test test/jepsen_workload_test.exs test/jepsen_cluster_test.exs` from the root.
