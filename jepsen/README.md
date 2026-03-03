# EKV Jepsen Harness (Local)

This directory contains a local Jepsen/Knossos verification harness for EKV.

It runs a concurrent workload against EKV consistent reads/writes, emits a
Jepsen-format history, and checks linearizability with Jepsen's
`checker/linearizable`.

## What this checks

- Single-key register semantics on `EKV.get(..., consistent: true)` and
  `EKV.put(..., consistent: true)`.
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
lein run results/history.edn 8 1000 3 none
```

Partition/heal flapping during workload:

```bash
export PATH="/opt/homebrew/opt/openjdk/bin:$PATH"
lein run results/history_partition_flap_3n.edn 8 3000 3 partition_flap
```

Single-node restart churn during workload:

```bash
export PATH="/opt/homebrew/opt/openjdk/bin:$PATH"
lein run results/history_restart_one_3n_small.edn 8 800 3 restart_one
```

5-node steady-state quorum:

```bash
export PATH="/opt/homebrew/opt/openjdk/bin:$PATH"
lein run results/history_5n_none.edn 8 2000 5 none
```

Arguments:

1. `history_path` (default: `results/history.edn`)
2. `workers` (default: `4`)
3. `ops` (default: `200`)
4. `cluster_nodes` (default: `3`, minimum `3`)
5. `mode` (default: `none`, one of `none|partition_flap|restart_one`)

Start with the defaults, then increase gradually (`workers` and `ops`) as
the checker cost rises quickly with high overlap.
