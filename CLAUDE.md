# EKV — Eventually Consistent Durable KV Store

## What This Is

EKV is a standalone Elixir library that provides an eventually consistent, durable key-value store backed by SQLite. Data survives node restarts, node death, and network partitions. Replication is peer-to-peer across all connected Erlang nodes using delta sync via per-shard oplogs.

Zero runtime deps. SQLite is vendored as a C NIF (`c_src/sqlite3.c` amalgamation).

## Quick Reference

```elixir
# Start
{EKV, name: :my_kv, data_dir: "/data/ekv"}

# API
EKV.put(:my_kv, "user/1", %{name: "Alice"})
EKV.put(:my_kv, "key", value, ttl: :timer.minutes(30))
EKV.get(:my_kv, "user/1")          # => %{name: "Alice"} | nil
EKV.delete(:my_kv, "user/1")
EKV.scan(:my_kv, "user/")          # => %{"user/1" => val}
EKV.keys(:my_kv, "user/")          # => ["user/1"] sorted

# Subscribe to changes (matches at "/" boundaries, "" = all keys)
EKV.subscribe(:my_kv, "user/")
# => receive {:ekv, [%EKV.Event{type: :put, key: "user/1", value: val}], %{name: :my_kv}}
EKV.unsubscribe(:my_kv, "user/")

# Isolation: start multiple EKV instances with different names.
# Each is fully independent (own SQLite, peer mesh, oplog).
```

## Commands

```bash
mix deps.get
mix compile --warnings-as-errors
mix test                           # 85 tests (50 unit + 35 distributed)
mix test test/ekv_test.exs         # unit only
mix test test/distributed_test.exs # distributed only

# Benchmarks (from priv/bench/)
cd priv/bench && bash run_local.sh        # single-node benchmarks
cd priv/bench && bash run_distributed.sh  # 2-node replication benchmarks
```

## Architecture

### Supervision Tree

```
EKV.Supervisor (rest_for_one)
├── EKV.SubTracker              atomics subscriber count + process monitors
├── Registry (keys: :duplicate, listeners: [sub_tracker])
├── EKV.SubDispatcher.Supervisor (one_for_one)
│   ├── EKV.SubDispatcher 0     async event fan-out per shard
│   ├── EKV.SubDispatcher 1
│   └── ...
├── EKV.Replica.Supervisor (one_for_one)
│   ├── EKV.Replica 0           shard GenServer (writes + replication + SQLite)
│   ├── EKV.Replica 1
│   └── ...                     N shards (default 8)
└── EKV.GC                      periodic timer, sends :gc to each shard
```

`rest_for_one`: SubTracker crash restarts everything. Registry crash restarts Dispatchers + Replicas. Single Replica crash → only that shard restarts. GC is downstream of Replicas.

### Modules

| Module | Role |
|--------|------|
| `EKV` | Public API. `put/get/delete/scan/keys/subscribe/unsubscribe`. Reads go direct to SQLite via per-scheduler read connections (no GenServer hop). Writes route to shard via `GenServer.call`. |
| `EKV.Event` | Struct for change notifications: `%EKV.Event{type: :put | :delete, key: key, value: value}`. |
| `EKV.Replica` | Shard GenServer. Owns one SQLite writer db + N read connections. Handles writes, replication, peer sync, GC. Registered as `:"#{name}_ekv_replica_#{shard}"`. |
| `EKV.Store` | Pure function module for all SQLite operations. Called inside Replica and from read connections. |
| `EKV.SubTracker` | Registry listener + process monitor. Maintains atomics subscriber count. Handles cleanup on process death. |
| `EKV.SubDispatcher` | Async event fan-out per shard. Receives `{:dispatch, events}` from Replica, does prefix-decomposition lookup via `Registry.lookup`, sends to matching subscribers. |
| `EKV.GC` | Periodic timer. Sends `{:gc, now, tombstone_cutoff}` to each shard. |
| `EKV.Supervisor` | Top-level supervisor. Stores config in `persistent_term` keyed by `{EKV, name}`. |
| `EKV.Sqlite3` | Thin Elixir wrapper over the NIF (9 functions). |
| `EKV.Sqlite3NIF` | NIF stubs with `@on_load`. |

### Storage: SQLite Only

Each shard has a single SQLite database (WAL mode, `synchronous=NORMAL`):
- File: `#{data_dir}/shard_#{i}.db`
- Tables: `kv` (current state, PK `key`), `kv_oplog` (append-only mutation log, AUTOINCREMENT seq), `kv_peer_hwm` (per-peer high-water marks), `kv_meta` (liveness tracking)
- `kv` + `kv_oplog` writes are atomic (`BEGIN IMMEDIATE` / `COMMIT`)

**Read connections**: Each shard opens `System.schedulers_online()` read connections stored as a tuple in `persistent_term` keyed by `{EKV, name, :readers, shard_index}`. Reads pick a connection by `rem(scheduler_id - 1, num_readers)` — zero contention, no pool, no GenServer hop. WAL mode ensures readers don't block the writer.

### Sharding

`shard = :erlang.phash2(key, num_shards)`

Each shard is fully independent. Prefix scans fan out to all shards (prefix doesn't determine hash).

**Shard count is immutable** — persisted in `kv_meta` on first open. Changing `shards:` after data exists raises `ArgumentError` at startup. Peer connections from nodes with mismatched shard counts are rejected with a logged error (without crashing the local shard).

### Conflict Resolution: Last-Writer-Wins (LWW)

LWW is pushed into SQL via `ON CONFLICT ... WHERE` clause:

```sql
ON CONFLICT(key) DO UPDATE SET ...
WHERE excluded.timestamp > kv.timestamp
  OR (excluded.timestamp = kv.timestamp AND excluded.origin_node > kv.origin_node)
```

`sqlite3_changes() == 0` after the upsert means LWW lost — the transaction is rolled back and no oplog entry is written. Equivalent to:

```elixir
lww_wins?(incoming_ts, incoming_origin, existing_ts, existing_origin)
  incoming_ts > existing_ts
  OR (incoming_ts == existing_ts AND incoming_origin > existing_origin)
```

- Nanosecond timestamps + origin_node atom as deterministic tiebreaker
- Used in ALL write paths: local put/delete, remote replication, bulk sync, GC TTL expiry
- Delete is just an entry with `deleted_at` set — same LWW applies

### HWM Monotonicity

`set_hwm` uses `MAX(kv_peer_hwm.last_seq, excluded.last_seq)` to ensure HWMs never regress. If sync messages arrive out of order (e.g. seq=100 then seq=50), the HWM stays at 100.

### Peer Discovery & Replication

Self-contained — uses `:net_kernel.monitor_nodes/1` and `Node.list/0` directly.

**Tracking**: `state.remote_shards :: %{node() => pid()}` — confirmed live counterpart shard processes.

**Steady-state messages** (fire-and-forget to all peers):
- `{:ekv_put, key, value_binary, timestamp, origin_node, expires_at}`
- `{:ekv_delete, key, timestamp, origin_node}`

**Peer handshake** (on nodeup / init):
- `{:ekv_peer_connect, pid, shard_index, num_shards, my_max_seq}`
- `{:ekv_peer_connect_ack, pid, shard_index, num_shards, my_max_seq}`

**Bulk sync** (after handshake):
- `{:ekv_sync, from_node, shard_index, entries, sender_max_seq}`
- entries: `[{key, value_binary, timestamp, origin_node, expires_at, deleted_at}]`

### Delta Sync vs Full Sync

| | Delta | Full |
|---|---|---|
| **Condition** | HWM exists for peer AND oplog not truncated past it | No HWM or oplog truncated |
| **Query** | `SELECT * FROM kv_oplog WHERE seq > peer_hwm` | `SELECT * FROM kv WHERE deleted_at IS NULL OR deleted_at > cutoff` |
| **Use case** | Brief disconnects | First contact, long partitions |

### GC (3 phases per tick)

1. **Expire TTL entries** → convert to tombstones, broadcast deletes
2. **Purge old tombstones** → hard delete from SQLite where `deleted_at < now - tombstone_ttl`
3. **Truncate oplog** → `DELETE FROM kv_oplog WHERE seq < MIN(all peer HWMs)`
4. **Bump liveness** → `Store.touch_last_active(db)` updates `kv_meta.last_active_at`

### Stale DB Protection

On `Store.open/3`, if an existing db file's `last_active_at` is older than `tombstone_ttl`, the db is wiped (deleted). This prevents zombie resurrection of entries that were deleted while the node was away — peers will have GC'd those tombstones, so a stale db would never learn about them. After wipe, full sync from peers rebuilds from scratch.

`last_active_at` is bumped on every GC tick and on initial open.

### Config

Stored in `persistent_term` keyed by `{EKV, name}`:

```elixir
%{
  num_shards: 8,            # :shards option (default 8)
  data_dir: "...",           # required
  log: :info,                # :info (default), false (silent), :verbose (all shards)
  tombstone_ttl: 604_800_000, # 7 days in ms (default)
  gc_interval: 300_000,      # 5 minutes in ms (default)
  registry: :"#{name}_ekv_registry",
  sub_count: atomics_ref     # 1 cell: total subscriber count
}
```

### Logging

- `log/2` (normal), `log_verbose/2` (verbose only), `log_once/2` (shard 0 only) — helpers in Replica
- All output uses `Logger.info` — `:verbose` is EKV's own flag, not Logger level

## Custom SQLite NIF

We vendor sqlite3.c (amalgamation) and wrote a minimal NIF instead of depending on exqlite. Zero runtime deps.

### NIF API (10 functions)

```
EKV.Sqlite3.open(path)                              → {:ok, db}   | {:error, msg}
EKV.Sqlite3.close(db)                               → :ok
EKV.Sqlite3.execute(db, sql)                         → :ok          | {:error, msg}
EKV.Sqlite3.prepare(db, sql)                         → {:ok, stmt}  | {:error, msg}
EKV.Sqlite3.bind(stmt, args)                         → :ok          | {:error, msg}
EKV.Sqlite3.step(db, stmt)                           → {:row, list} | :done | {:error, msg}
EKV.Sqlite3.release(db, stmt)                        → :ok
EKV.Sqlite3.write_entry(db, kv, oplog, kv_a, op_a)  → {:ok, true|false} | {:error, msg}
EKV.Sqlite3.read_entry(db, stmt, args)               → {:ok, [cols]|nil} | {:error, msg}
EKV.Sqlite3.fetch_all(db, sql, args)                → {:ok, [[col, ...]]} | {:error, msg}
```

`write_entry` does BEGIN IMMEDIATE + kv upsert (with SQL LWW) + oplog insert + COMMIT in a single dirty IO bounce. Returns `{:ok, false}` if LWW lost (rollback, no oplog).

`read_entry` does reset + bind + step on a cached prepared statement in a single dirty IO bounce. Returns the row or nil.

`fetch_all` does prepare + bind + step-all-rows + finalize in a single dirty IO bounce. Returns all rows as a list of lists. Used for multi-row queries (prefix scans, oplog reads, full state, expired entries).

### C NIF design (c_src/ekv_sqlite3_nif.c, ~650 lines)

Two resource types:
- `connection_t` — wraps `sqlite3*` + `ErlNifMutex*`
- `statement_t` — wraps `sqlite3_stmt*` + back-ref to `connection_t`

Resource destructors handle cleanup. Statement holds connection alive via `enif_keep_resource`. All IO-touching NIFs use `ERL_NIF_DIRTY_JOB_IO_BOUND`. `bind` runs on normal scheduler.

Single `bind/2` takes a list — dispatches per element on type (int64/double/text/null).

### Build

`Makefile` compiles `ekv_sqlite3_nif.c` + `sqlite3.c` into `ekv_sqlite3_nif.so`. Platform detection for Darwin vs Linux linker flags. Supports `CROSSCOMPILE` prefix for cc_precompiler.

`mix.exs` uses `elixir_make` + `cc_precompiler` (both compile-time only). `make_force_build: true` in dev/test to skip precompiled downloads.

### Precompilation for Hex

See `RELEASE.md` for full workflow. Key: tag → CI builds for all targets → upload to GitHub release → `mix elixir_make.checksum --all` → include `checksum-ekv.exs` in hex package.

## Test Infrastructure

- Uses OTP `:peer` module for real Erlang nodes
- `EKV.TestCluster` in `test/support/` — compiled to beam via `elixirc_paths(:test)`
- `test_helper.exs` starts distribution with `Node.start/2`
- Partition tests use 3 nodes, isolating 1 from the other 2 (erlang dist is fully meshed — 2-node partitions are unreliable because the test node bridges them)
- Helper: `assert_eventually/2` for async convergence checks

### Unit tests (50 in `test/ekv_test.exs`)

Core: put/get, delete, TTL, prefix scan, restart rehydration, LWW conflicts (tiebreaker, atom ordering, delete vs put, resurrection, oplog not written on loss).

Pathological: HWM monotonicity, oplog seq gaps on LWW loss, oplog truncation, GC TTL expiry into tombstones, GC tombstone purge, GC oplog truncation at min HWM, stale DB detection/wipe, shard count mismatch protection, concurrent put during GC, full_state excludes purged tombstones, delta sync correctness.

Subscribe: put notification, delete notification (with previous value), delete missing key (nil value), prefix filtering, empty prefix matches all, LWW rejection no event, multiple subscribers with different prefixes, subscriber death cleanup, unsubscribe stops events, re-subscribe idempotency, overlapping prefixes dedup, unsubscribe one prefix keeps another, GC TTL expiry event, bulk sync batched events, remote put event, remote delete event, tombstone purge no event.

### Distributed tests (35 in `test/distributed_test.exs`)

Core: put/delete/complex value replication, late joiner sync, node restart persistence, empty storage re-sync, LWW concurrent puts, partition with disjoint/conflicting keys, delete vs put across partition, many-key partition convergence, rapid fire partition writes, delete-during-partition resurrection, prefix scan after partition, 3-way split, double partition cycles, node death + replacement with tombstones, write-during-heal race.

Pathological (Jepsen-style): shard crash recovery, write during active sync (300 keys), GC during partition (tombstone survives sync), GC tombstone purge + late joiner, oplog truncation forces full sync, rapid partition/heal flapping (5 cycles), concurrent deletes both sides, put/delete interleaving, multi-shard consistency (4 shards), duplicate peer_connect handling, write during node shutdown (atomicity), large dataset full sync (5000 keys), three-way conflict resolution.

Subscribe: replication triggers subscriber on remote node, distributed delete triggers subscriber with previous value, partition heal sync triggers subscriber events.

## Key Design Decisions

- **No cluster concept** — removed. If you want isolation, start multiple EKV instances with different names on the nodes that should form isolated replication groups.
- **No connection pool** — one SQLite writer connection per shard GenServer + per-scheduler read connections. Writes serialized through GenServer, reads go direct.
- **Values stored as `:erlang.term_to_binary/1`** — encoding in public EKV module, Replica/Store only see binaries.
- **Tombstone TTL** (default 7 days) — keeps tombstones long enough for partitioned nodes to learn about deletes on reconnect. After TTL, tombstones are hard-deleted.
- **Oplog truncation** — bounded by slowest peer's HWM. Peers that fall behind the oplog get full sync instead.

## Benchmarks

Separate Mix project in `priv/bench/`. Starts real Erlang nodes for distributed benchmarks.

```bash
cd priv/bench
bash run_local.sh                  # single-node: get/put/delete/scan/TTL/value-size/shard-scaling/subscribe/10k-scale
bash run_distributed.sh            # 2-node: replication latency, bulk sync, partition/heal,
                                   #         value size replication, busy app simulation, subscribe
bash run_distributed.sh --shards 4 # override shard count
```

`run_distributed.sh` starts 2 replica nodes + 1 coordinator node, runs benchmarks, then cleans up. The coordinator drives writes via `:erpc` and measures end-to-end replication latency.

Key modules: `Bench.Local` (single-node), `Bench.Distributed` (coordinator), `Bench.Replica` (helper functions loaded on replica nodes), `Bench.Helpers` (timing/reporting utilities).

## File Layout

```
lib/
  ekv.ex                  Public API (put/get/delete/scan/keys/subscribe/unsubscribe)
  ekv/
    event.ex              Change notification struct (type, key, value)
    application.ex        Empty app supervisor
    supervisor.ex         Top-level rest_for_one supervisor, config in persistent_term
    replica.ex            Shard GenServer (writes, replication, peer sync, GC, read connections)
    replica/supervisor.ex One-for-one supervisor for N shard replicas
    sub_tracker.ex        Registry listener + process monitor, atomics subscriber count
    sub_dispatcher.ex     Async event fan-out per shard, prefix-decomposition lookup
    sub_dispatcher/supervisor.ex  One-for-one supervisor for N dispatchers
    store.ex              SQLite operations (open, CRUD, oplog, HWM, GC, liveness)
    gc.ex                 Periodic GC timer
    sqlite3.ex            Thin Elixir wrapper over NIF
    sqlite3_nif.ex        NIF stubs (@on_load)
c_src/
    sqlite3.c             Vendored SQLite 3.47.2 amalgamation (~9MB)
    sqlite3.h             Vendored SQLite header
    ekv_sqlite3_nif.c     Custom NIF (~650 lines)
Makefile                  elixir_make build rules
mix.exs                   deps: elixir_make + cc_precompiler (both runtime: false)
test/
    ekv_test.exs          50 unit tests
    distributed_test.exs  35 distributed tests (partition, sync, failover, Jepsen-style)
    support/test_cluster.ex  Peer node helpers
    test_helper.exs       Starts distribution
priv/bench/
    lib/bench/local.ex    Single-node benchmarks (12 suites)
    lib/bench/distributed.ex  2-node replication benchmarks (7 suites)
    lib/bench/replica.ex  Helper functions for replica nodes
    lib/bench/helpers.ex  Timing, formatting, reporting utilities
    run_local.sh          Run local benchmarks
    run_distributed.sh    Run distributed benchmarks
    RESULTS.md            Benchmark results archive
RELEASE.md               Hex publish workflow with precompiled NIFs
```
