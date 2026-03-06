# EKV Agent Context (CAS + LWW)

## Purpose
This file is for AI agents working on EKV from a cold start.
It is not end-user docs.
Use it as a high signal map of:
- safety invariants
- where correctness can break
- which tests to run for each kind of change

Assumption scope: non-malicious failures only (crash, restart, partition, delay, reordering). Byzantine behavior is out of scope.

## First files to read
1. `lib/ekv.ex` (public API and semantic contract)
2. `lib/ekv/replica.ex` (`_archdoc` plus CAS, sync, GC, quorum, partition handling)
3. `lib/ekv/store.ex` (`_archdoc`, schema, stale db checks, meta persistence)
4. `README.md` and `OPERATORS.md` (must stay aligned with semantics)
5. `test/` and `jepsen/` (regression coverage and external checker harness)

## Current system model
- Default mode is eventual consistency with LWW conflict resolution.
- CAS mode is opt-in per write call (`if_vsn`, `consistent: true`, `update`).
- Key mode policy:
  - `LWW -> CAS` is allowed.
  - After a key becomes CAS-managed, eventual `put/delete` on that key is rejected with `{:error, :cas_managed_key}`.
- Reads:
  - Eventual read: `get/2`, `lookup/2`, `scan/2`, `keys/2`
  - Consistent read: `get(name, key, consistent: true)` runs CAS read path and acts as a barrier (linearizable read for the key).

## Public API return shapes (do not drift)
- `EKV.put`:
  - eventual path: `:ok` or `{:error, :cas_managed_key}`
  - CAS path: `{:ok, vsn}` | `{:error, :conflict}` | `{:error, :unconfirmed}` | `{:error, :unavailable}` (only when `resolve_unconfirmed: true`)
- `EKV.delete`:
  - eventual path: `:ok` or `{:error, :cas_managed_key}`
  - CAS path: `{:ok, vsn}` | `{:error, :conflict}` | `{:error, :unconfirmed}` | `{:error, :unavailable}` (only when `resolve_unconfirmed: true`)
- `EKV.update`: `{:ok, new_value, vsn}` | `{:error, :conflict}` | `{:error, :unconfirmed}` | `{:error, :unavailable}` (only when `resolve_unconfirmed: true`)
- `EKV.get(..., consistent: true)` returns value/nil and raises if consistent read fails.
- `EKV.scan` yields `{key, value, vsn}`.
- `EKV.keys` yields `{key, vsn}`.

`resolve_unconfirmed` is currently opt-in (default false).

## Safety invariants to preserve

### CASPaxos state separation
- `paxos_accept` writes only to `kv_paxos`, not `kv` and not `kv_oplog`.
- Value becomes visible only after `paxos_promote` commit into `kv` and `kv_oplog`.
- No events are emitted on pure accept state.
- This prevents phantom writes from failed CAS attempts.

### Consistent read barrier
- `consistent: true` read must go through CAS read flow and close in-flight accepted state.
- Read recovery must preserve accepted metadata (`expires_at`, `deleted_at`) from raw accepted row.
- If key is absent, read recovery can propose tombstone marker to close ambiguity.

### CAS error semantics
- `:conflict` means definitive non-application for this attempt.
- `:unconfirmed` means write entered accept phase and caller outcome is ambiguous.
- Retry policy:
  - automatic retry only for definite conflicts where configured
  - do not blind-retry ambiguous accept-phase writes as if they definitely failed

### Monotonic CAS timestamps
- CAS commit timestamps must be strictly greater than current value timestamp for the key.
- This avoids a post-heal LWW overwrite of a CAS-chosen value by older state with a larger stale timestamp.

### CAS key ownership guard
- Eventual put/delete must reject CAS-managed keys.
- This guard is enforced in replica write handlers (`Store.cas_managed_key?/2`).

### Quorum math and identity
- Quorum is based on distinct `node_id`s, not Erlang node names.
- `count_alive_node_ids` drives quorum and overflow checks.
- If distinct reachable node_ids exceeds `cluster_size`, CAS returns `{:error, :cluster_overflow}`.
- `node_id` is persisted on volume and reused; configured mismatches are ignored in favor of persisted value.

### Sync and HWM correctness
- `kv_peer_hwm` is monotonic (`MAX` semantics).
- Sender records peer HWM as sender snapshot `my_seq` after sync completion, not remote sequence.
- Delta sync allowed only when remote hwm is within local oplog window.
- Out-of-window hwm must force full sync.
- Chunked sync:
  - intermediate chunks send seq `0`
  - final chunk must send non-zero terminal seq
  - exact-multiple chunk sizes still require final non-zero seq behavior

### Long partition safety
- Startup stale-db protection:
  - if db idle age exceeds `tombstone_ttl - gc_interval`, wipe and rebuild from full sync
- Live long partition protection:
  - with default `partition_ttl_policy: :quarantine`, reconnect after downtime > `tombstone_ttl` blocks replication
  - down-since markers are persisted in `kv_meta` keyed by node_id when possible (fallback node name)

### Blue-green constraints
- Blue-green overlap is valid only when old/new instances share the same filesystem volume.
- Both sides must represent the same logical member (`node_id`).
- Handoff should drain pending CAS and fail callers with `{:error, :shutting_down}` rather than silently dropping.

## Core code map
- `lib/ekv.ex`
  - API validation, option parsing, return shaping, docs contract
- `lib/ekv/supervisor.ex`
  - config assembly, node_id persistence resolution, blue-green startup handoff
- `lib/ekv/replica.ex`
  - write paths, CAS protocol, sync protocol, nodeup/nodedown handling, quorum checks, GC
- `lib/ekv/store.ex`
  - sqlite schema and all persistence primitives (kv, kv_oplog, kv_peer_hwm, kv_meta, kv_paxos)
- `c_src/ekv_sqlite3_nif.c`
  - NIF-side transactional primitives, including paxos_prepare/accept/promote

## Test map and what to run

### Fast baseline
```bash
mix test
```

### When touching CAS protocol or CAS API semantics
```bash
mix test test/cas_distributed_test.exs
mix test test/stress_test.exs
mix test test/linearizability_pure_elixir_test.exs
```

### When touching sync, HWM, chunking, reconnect, quarantine
```bash
mix test test/distributed_test.exs
mix test test/adversarial_verification_test.exs
mix test test/ekv_test.exs
```

### Known regression areas already covered by tests
- long partition quarantine and restart persistence
- chunked sync terminal-seq correctness
- CAS-managed key rejecting eventual writes
- prepare/select behavior with tombstones and accepted state
- stale DB wipe on startup
- 5-node quorum and partition behavior
- handoff/blue-green CAS continuity

### Jepsen harness (external checker)
`jepsen/` includes repeatable scenario scripts and lock/register profiles.
Typical runs:
```bash
cd jepsen
lein run
./run_scenario.sh register-3n-partition-flap 1
./run_scenario.sh lock-5n-partition-restart 1
./run_lock_matrix.sh 1,2,3
```
Do not commit `jepsen/results/`, `jepsen/target/`, `jepsen/store/`, `.nrepl-port` artifacts.

## Bench map
- CLI bench project: `priv/bench`
  - CAS bench driver: `priv/bench/run_cas.sh`
  - implementation: `priv/bench/lib/bench/cas.ex`
- Fly/Phoenix orchestrator: `priv/bench_web`
  - useful for multi-region runs and scenario toggles

## Common failure patterns and debugging hints
- Symptom: CAS timeout or no quorum during contention.
  - Check distinct node_id reachability, not only node process reachability.
- Symptom: `{:error, :cluster_overflow}`.
  - Extra logical member(s) are reachable beyond configured cluster_size.
- Symptom: stale values after partitions.
  - Confirm whether read path was eventual or consistent barrier read.
- Symptom: post-heal divergence.
  - Inspect hwm window, forced full-sync path, and quarantine branch.
- Symptom: lock workflow ambiguity.
  - If `:unconfirmed` is returned, resolve with consistent read or use `resolve_unconfirmed: true`.

## Agent workflow rules for this repo
- Keep API docs, README, and OPERATORS in sync with behavior changes.
- Preserve and extend adversarial tests for every bug fix.
- Prefer minimal, targeted changes in `replica.ex` for safety-critical fixes.
- If changing CAS/LWW boundary behavior, update both unit/distributed tests and Jepsen scenarios.
- For contentious tests, avoid asserting immediate eventual convergence with tiny timeouts; separate "chosen value exists" checks from replication lag checks.
