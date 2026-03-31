# EKV Agent Context

This file is for AI agents working on EKV from a cold start.
It is not user-facing documentation.

Use it as the shortest accurate map of:
- current semantics
- safety invariants
- control-plane structure
- where the sharp edges are
- which tests to run before claiming a fix

Failure model in scope: non-malicious failures only.
Assume crash, restart, network partition, message delay/reordering, and operator mistakes.
Do not analyze Byzantine/malicious behavior unless explicitly asked.

## Read First
1. `lib/ekv.ex`
   Public API, return shapes, startup options, and user-visible semantics.
2. `lib/ekv/replica.ex`
   `_archdoc`, CASPaxos flow, sync/HWM logic, replication, GC, quorum, partition handling.
3. `lib/ekv/store.ex`
   SQLite schema, persisted metadata, stale-db checks, CAS/LWW persistence details.
4. `lib/ekv/supervisor.ex`
   Runtime mode split, scoped `:pg`, startup gates, blue-green handoff, persisted `node_id`.
5. `README.md` and `OPERATORS.md`
   Must stay aligned with real behavior.

## Current Product Model
- EKV is mixed-consistency.
- Default operations are eventual/LWW.
- CAS writes plus `consistent: true` reads provide per-key linearizable semantics when quorum is available.
- Modes are per key, not per store:
  - `LWW -> CAS` is allowed.
  - `CAS -> LWW` writes are rejected with `{:error, :cas_managed_key}`.
- `LWW -> CAS` is an operational migration, not a partition-safe fenced mode switch.
- A stale/partitioned node that has not yet learned CAS ownership for a key can still accept an eventual write for that key during cutover.
- Eventual reads on CAS-managed keys are still allowed.
- `consistent: true` is a barrier read, not a fast-path heuristic.

## Runtime Modes

### `mode: :member`
- Durable node.
- Runs SQLite shards, replication, GC, CAS proposer/acceptor logic.
- May run:
  - `wait_for_quorum`
  - `anti_entropy_interval`
  - `shutdown_barrier`
  - `blue_green`
  - `wire_compression_threshold`

### `mode: :client`
- Stateless node.
- Does not run SQLite, replicas, GC, sync, or blue-green machinery.
- Uses the same public API by routing to members.
- Supports:
  - `wait_for_route`
  - `wait_for_quorum` (via selected member)
  - `shutdown_barrier`
  - `wire_compression_threshold` config is accepted for shared child-spec simplicity, but
    current wire compression is member-to-member only.

Important:
- Client mode rejects member-only options like `:blue_green`, `:cluster_size`, `:node_id`, `:data_dir`, `:shards`, `:partition_ttl_policy`.

## CAS Configuration Reality
- CAS requires `cluster_size`.
- Member mode always resolves a stable `node_id` from disk, config, or first-boot random generation.
- CAS uses that same stable `node_id`; callers do not need to pass one explicitly unless they want to control the logical identity.
- Persisted `node_id` on disk wins over a conflicting configured `node_id`.
- Quorum math is by distinct logical `node_id`, not Erlang node name.

## Public API Contracts That Must Not Drift

### Eventual writes
- `EKV.put/4` eventual path: `:ok` or `{:error, :cas_managed_key}`
- `EKV.delete/3` eventual path: `:ok` or `{:error, :cas_managed_key}`

### CAS writes
- `EKV.put/4` CAS path:
  - `{:ok, vsn}`
  - `{:error, :conflict}`
  - `{:error, :unconfirmed}`
  - `{:error, :unavailable}` only when `resolve_unconfirmed: true` and the barrier resolution read cannot complete
  - operational failures may also surface as `{:error, :no_quorum}`, `{:error, :quorum_timeout}`, `{:error, :cluster_overflow}`, `{:error, :shutting_down}`, `{:error, :cas_not_configured}`
- `EKV.delete/3` CAS path:
  - same error model as CAS put
  - success shape is `{:ok, vsn}`
- `EKV.update/4`:
  - `{:ok, new_value, vsn}`
  - same error model as CAS put/delete

### Reads
- `EKV.get/2` is eventual.
- `EKV.lookup/2` is eventual and returns vsn.
- `EKV.get(name, key, consistent: true)` is a barrier/linearizable read for the key.
  - it returns `value | nil`
  - it raises if the consistent read itself cannot complete

### Streams
- `EKV.scan/2` yields `{key, value, vsn}`
- `EKV.keys/2` yields `{key, vsn}`
- In client mode these are still local Elixir streams, backed by paged RPC.

### `resolve_unconfirmed`
- Current default is `false`.
- If enabled on CAS writes, EKV does one internal barrier read on ambiguous accept-phase failure and resolves to:
  - the original success if the committed state matches the attempted write
  - `{:error, :conflict}` if it does not
  - `{:error, :unavailable}` if resolution itself fails

## Control Plane

### Scoped `:pg` is mandatory
- Do not use the default global `:pg` scope for EKV runtime behavior.
- Each EKV instance owns its own `:pg` scope via `EKV.Supervisor.pg_scope(name)`.
- All routing, distributed subscriptions, and shutdown coordination must use that scoped mesh.
- This isolation matters because multiple EKV instances can coexist on the same cluster.

### Member presence
- Ready members advertise themselves in scoped `:pg` region groups:
  - `{:ekv_members, name, region}`
- This is pinned by `EKV.MemberPresence`.
- New clients should discover members through this path, not by raw `Node.list/0`.

### Wire compression
- `wire_compression_threshold` defaults to `256 * 1024`.
- Member-to-member wire traffic now uses a versioned envelope:
  - `{:ekv, 1, kind, payload, meta}`
- Required fields live in `payload`.
- Optional/extensible fields live in `meta`.
- Compression is field-level only on v1 replication messages:
  - `:put`
  - `:accept`
  - full-payload `:cas_committed`
  - the value field may be tagged as `{:ekv_wire_compressed, binary}`
- Receivers inflate before normal processing.
- SQLite storage and normal reads remain uncompressed.
- Features are negotiated in `:member_connect` / `:member_connect_ack` meta.
  - Current advertised features:
    - `:live_progress`
    - `:wire_compression`
    - `:replication_batch`
  - `:live_progress` now means the peer understands progress summaries,
    `:summary_probe` / `:summary_reply`, `:sync_request`, and sync-settlement
    `:progress_ack`. It does not imply per-write live progress acks.
  - `:replication_batch` means the peer accepts live LWW replication batches
    over the wire and can apply them in one SQLite transaction while preserving
    per-entry subscriber semantics.
- Version determines parse shape.
- Features determine which optional send-side behaviors are allowed.
- There is still no support for unversioned/old mixed-member overlap.
  - This is a fresh protocol reset, not a bridge.
  - Intended rollout model is cold deploy or a cluster rename/reset.

### Client routing
- `EKV.ClientRouter` is the client control plane.
- Hot path:
  - read cached backend from named ETS
  - no per-op `GenServer.call`
- Cold path:
  - region-group membership comes from `:pg.monitor/2`
  - waiters are event-driven, not polling
  - failed/current-invalid backends are cooled down in ETS
- Candidate selection:
  - first available member in `region_routing` order
  - stable ordering within a region
  - cold-path validation RPC rejects stale/outgoing blue-green candidates by checking `EKV.MemberPresence.advertised?/1`
- There is still a fallback path that probes connected nodes for member info if region groups are empty/stale.
  - guard it carefully; not every node in `Node.list()` runs EKV.

### Client subscription delivery
- Member-local subscribers still use `Registry`.
- Client subscribers join scoped `:pg` groups directly via `EKV.ClientSubscriptions`.
- Members dispatch events to:
  - local registry subscribers
  - matching client `:pg` subscribers
- There is also a coarse client “any subscribers exist” group used as a hot-path gate.

### Shutdown barrier
- `EKV.ShutdownBarrier` is an opt-in last child in both member and client trees.
- It exists to help coordinated graceful shutdowns preserve quorum and allow final writes/replication to complete.
- It uses scoped `:pg` groups:
  - `{:ekv_shutdown_live, name}`
  - `{:ekv_shutdown_terminal, name}`
  - `{:ekv_shutdown_live_member, name, node_id}`
  - `{:ekv_shutdown_terminal_member, name, node_id}`
- Members count quorum by logical `node_id`.
- Blue-green overlap must still count as one logical voter.
- Outgoing proxy-mode blue-green members skip barrier waiting.

## Startup Gates

### `wait_for_route`
- Client mode only.
- Blocks startup until the first reachable member in `region_routing` order is selected.
- This is about routing readiness, not quorum.

### `wait_for_quorum`
- Member mode:
  - blocks startup until this member can reach CAS quorum
- Client mode:
  - first waits for a route
  - then asks the selected member whether quorum is reachable
- Do not reintroduce polling via `:sys.get_state`; the current gates are explicit processes.

## Blue-Green Model
- Valid only when old and new instances share the same filesystem volume.
- Old and new must represent the same logical member (`node_id`).
- Startup handoff:
  - only attempts handoff if the outgoing marker node is reachable now
  - stale/dead marker skips handoff
- `EKV.BlueGreenMarker` exists only to clean the on-disk `current` marker on graceful non-handoff shutdown.
- When outgoing member receives a real handoff:
  - it marks handoff performed
  - it leaves `MemberPresence`
  - it enters proxy mode
  - pending CAS waiters get `{:error, :shutting_down}`
- New clients should route to the incoming node, but stale `:pg` visibility is possible briefly; the cold-path route validation RPC is what makes this reliable.

## Safety Invariants

### CASPaxos state separation
- `paxos_accept` writes only to `kv_paxos`.
- Accepted state is invisible to normal reads and scans.
- Only `paxos_promote` writes committed CAS state into `kv` and `kv_oplog`.
- No subscriber events on pure accept state.
- This prevents phantom visibility.
- Commit dissemination is optimized:
  - members that already accepted for a unique `node_id` may receive slim commit (`entry_tuple = nil`)
  - members that may have missed accept receive the full `entry_tuple`
  - full commit payloads may have a wire-compressed value field

### Consistent read barrier
- `consistent: true` must always go through the CAS read/repair path.
- Recovery from accepted state must preserve the accepted row metadata exactly:
  - `expires_at`
  - `deleted_at`
  - timestamp/origin tuple
- Do not reconstruct accepted metadata from partial current-value state.

### CAS error semantics
- `:conflict` means definitive non-application for this attempt.
- `:unconfirmed` means accept phase started and final outcome is ambiguous to the caller.
- Do not “simplify” `:unconfirmed` into `:conflict`.
- Automatic retries are acceptable for definite conflicts, not for ambiguous accept-phase outcomes.

### Monotonic CAS timestamps
- A CAS commit timestamp must be strictly greater than the current key timestamp.
- This protects against older healed LWW state later overwriting a chosen CAS value.

### CAS key ownership
- Eventual writes must reject CAS-managed keys.
- Reads may still be eventual or consistent.
- Document the migration caveat:
  - during `LWW -> CAS` cutover, stale/off-quorum nodes may not yet know the key is CAS-managed
  - such nodes can still accept an eventual LWW write
  - on heal, that write can win by LWW timestamp ordering if it is newer than what the CAS quorum saw
  - this is an accepted mixed-mode limitation, not a steady-state CAS bug

### Quorum and membership
- Quorum is `floor(cluster_size / 2) + 1`.
- Distinct logical `node_id`s drive quorum and overflow checks.
- If visible logical members exceed `cluster_size`, CAS must fail with `{:error, :cluster_overflow}`.

### Sync / replay-progress correctness
- Persisted member/origin identity should use the CAS logical-member model.
  - `node_id` is the stable identity for storage, replay origins, member
    progress, down markers, and quorum/member accounting.
  - Erlang `node()` names are transport/routing identities only.
- `kv_origin_progress` is local applied progress per origin stream.
- `kv_member_progress` is peer progress per origin stream.
- `kv_oplog` stores retained replay history, but uses `kv_keyrefs` so replay
  rows reference deduplicated keys instead of repeating full key strings.
  - `kv_keyrefs.oplog_refs` is maintained by SQLite triggers on `kv_oplog`
    insert/delete so normal writes do not pay a second refcount statement.
  - GC prunes orphan keyrefs only after replay for that key is gone and the
    key no longer exists in `kv`.
- Progress is exact, not monotonic-by-max.
  - If a peer restarts/full-syncs to a lower authoritative cursor, the stored peer progress must be allowed to move lower too.
- Delta sync is only valid when the requester is behind a live origin stream and the requested range is still inside the retained replay window.
- Otherwise serve full sync.
- Already-connected members now also run periodic anti-entropy by default.
  - The tick is a summary probe/reply exchange, not unsolicited sender push.
  - The behind receiver decides whether to request delta or full repair.
  - The goal is to heal missed replication without waiting for reconnect.
  - Each shard keeps at most one summary probe in flight per peer and one
    full-sync source in flight at a time, so bootstrap repair cannot fan out
    into duplicate full snapshots from every eligible peer.
  - Summary-probe and sync in-flight suppression is time-bounded.
    - stale inflight entries expire after a short timeout
    - a silently dropped sync request must not freeze peer-progress refresh
      or oplog truncation indefinitely
- Live LWW and CAS replication both carry `(origin_node, origin_seq)` directly.
  - `origin_node` is persisted as a string; with member identity configured it
    should be the stable `node_id`, not a transient blue/green node name.
  - Receivers advance local contiguous progress in the same write/promote transaction.
- Upgraded peers may batch live LWW replication fanout per destination shard.
  - Local writes still commit and reply one at a time; only the live
    member-to-member fanout is buffered briefly.
  - Batches are same-origin by construction, bounded by time/count/bytes, and
    applied in one SQLite transaction on the receiver.
  - Receiver progress must refresh against the final batch high-water mark; the
    single-entry "advance by one" fast path is not sufficient for batches.
  - Subscriber delivery must stay ordered and only emit applied entries.
    Deletes need pre-batch values only for keys that appear in delete ops.
- Full sync rebuilds `kv` only.
  - Receivers settle progress from the terminal advertised summary.
  - Receivers must not append full-sync rows back into `kv_oplog`.
- Replay and CAS on the same shard are mailbox-serialized.
- Chunked sync safety is not just "different tables":
  - tentative CAS state is in `kv_paxos`
  - sync applies committed `kv`
  - once a ballot is accepted, prepares/promotes must read `kv_paxos`
  - Gaps trigger explicit pull repair from the live origin instead of live progress-ack chatter.
- Local write/promote hot paths still use a single DB NIF hop.
  - The NIF now caches helper statements for local origin-seq allocation and
    progress maintenance instead of preparing/finalizing them per write.
  - Self-origin writes/promotes use a safe fast path: once the shard allocates
    the next `origin_seq` in the same transaction, local contiguous self
    progress can advance directly to that seq without scanning `kv_oplog`.
- Normal delta sync is origin-ordered, but any live peer with retained oplog can relay that origin stream.
- Disconnected members keep anchoring replay retention for
  `:member_progress_retention_ttl` (default: `min(tombstone_ttl, 6 hours)`),
  keyed by stable `node_id` when known, so serious but finite partitions can
  still prefer delta over full sync without retaining oplog history for the
  full tombstone/quarantine window by default.
- Quarantined origins can force immediate full sync from a live peer.
- If a peer is behind on data from a known member origin that is merely
  disconnected/unavailable, EKV requests relayed delta from a live peer immediately.
- Full sync happens only if that live peer cannot serve the requested retained range.
- Unknown/synthetic origins still fall back immediately because there is no
  live origin stream to wait for.
- Missing shard handshake alone is not enough to trigger that full-sync fallback.
- Chunked sync rules matter:
  - intermediate chunks use `progress=nil`
  - final chunk must send the real terminal progress map
  - `:progress_ack` is sync-settlement only; it is not part of the hot live-write path
  - empty delta still needs a terminal sync/ack settlement so the requester can advance to the sender's head without replaying extra chunks

### Long partition protection
- Startup stale-db rejection:
  - if idle age exceeds roughly `tombstone_ttl - gc_interval`, startup fails closed by default
  - operator must either wipe that node's data dir or explicitly set `allow_stale_startup: true`
- Startup schema guard:
  - each shard DB persists `kv_meta.schema_version`
  - fresh DBs stamp the current version on first open
  - initialized DBs with missing or mismatched `schema_version` fail startup closed
  - fresh shard DBs also set SQLite `auto_vacuum=INCREMENTAL`; existing DBs are not rebuilt on normal startup to change that mode
- Live long partition protection:
  - default `partition_ttl_policy: :quarantine`
  - reconnect after downtime > `tombstone_ttl` blocks replication for that member pair
- Down-since markers live in `kv_meta`, keyed by `node_id` when available, otherwise node name.
- Anti-entropy periodically retries `member_connect` to current `MemberPresence`
  members missing from `remote_shards`, so transient false down-markers should
  usually self-heal before they age into quarantine.
- Current presence does not bypass overdue quarantine. Once a down-marker is
  older than `tombstone_ttl`, the reconnect gate still quarantines until an
  operator rebuilds one side or deliberately widens the safety window.
- Pending live replication batches should not strand durable state during
  orderly shutdown.
  - Flush pending batches on normal terminate and blue-green handoff.
  - It is acceptable to drop per-destination in-memory batches on peer loss or
    quarantine; anti-entropy must repair any missed live writes afterward.

## Common Failure Patterns
- CAS returns `{:error, :no_quorum}` or `{:error, :quorum_timeout}`
  - check distinct `node_id` reachability, not just connected Erlang nodes
- CAS returns `{:error, :cluster_overflow}`
  - too many logical members are visible for configured `cluster_size`
- Eventual reads look stale after client failover
  - this is expected; use `consistent: true` if fresh reads matter
- Blue-green new client binds to outgoing node
  - suspect stale region-group visibility or candidate validation
- Post-heal divergence
  - inspect HWM bounds, forced full sync path, quarantine logic, and monotonic CAS timestamps

## Code Map
- `lib/ekv.ex`
  - public API
  - client/member branching
  - stream wrappers
  - docs contract
- `lib/ekv/supervisor.ex`
  - runtime mode split
  - per-instance scoped `:pg`
  - persisted `node_id` resolution
  - blue-green startup handoff
  - live replication batch knob validation/defaults
- `lib/ekv/replica.ex`
  - shard process
  - LWW path
  - live replication batch enqueue/flush/receive path
  - CASPaxos prepare/accept/promote
  - sync/HWM logic
  - quarantine
  - handoff/proxy mode
- `lib/ekv/store.ex`
  - schema and persistence primitives
  - single-entry and batched replication writes
- `lib/ekv/sqlite3.ex`
  - Elixir wrapper around SQLite NIFs
- `lib/ekv/sqlite3_nif.ex`
  - NIF stubs and load boundary
- `lib/ekv/client_router.ex`
  - client route selection, ETS cache, waiters, cooldowns
- `lib/ekv/member_presence.ex`
  - member advertisement in scoped `:pg`
- `lib/ekv/client_subscriptions.ex`
  - client-side subscription bookkeeping
- `lib/ekv/shutdown_barrier.ex`
  - coordinated graceful shutdown logic
- `lib/ekv/blue_green_marker.ex`
  - stale marker cleanup for graceful non-handoff shutdown
- `c_src/ekv_sqlite3_nif.c`
  - combined SQLite transactional primitives, including CAS NIFs
  - cached helper statements for local origin seq/progress bookkeeping
  - single-hop local write/promote path with self-origin fast path

## Tests To Run

### Baseline
```bash
mix test
```

### If touching CAS protocol, CAS API semantics, or blue-green CAS continuity
```bash
mix test test/cas_distributed_test.exs
mix test test/stress_test.exs
mix test test/linearizability_pure_elixir_test.exs
```

### If touching sync, HWM, reconnect, quarantine, stale-db rejection
```bash
mix test test/anti_entropy_test.exs
mix test test/distributed_test.exs
mix test test/adversarial_verification_test.exs
mix test test/ekv_test.exs
```

### If touching live replication batching or replication wire features
```bash
mix test test/anti_entropy_test.exs
mix test test/ekv_test.exs
mix test test/distributed_test.exs
```

### If touching client mode, routing, subscriptions, scoped `:pg`, startup gates
```bash
mix test test/client_mode_distributed_test.exs
mix test test/ekv_test.exs
```

### If touching shutdown barrier
```bash
mix test test/cas_distributed_test.exs
mix test test/client_mode_distributed_test.exs
mix test test/ekv_test.exs
```

## Jepsen
- `jepsen/` contains repeatable register/lock scenarios.
- Use it for external linearizability/lock verification after safety-critical changes.
- Typical commands:
```bash
cd jepsen
./run_scenario.sh register-3n-partition-flap 1
./run_scenario.sh lock-3n-partition-restart 1
./run_scenario.sh lock-5n-partition-restart 1
./run_lock_matrix.sh 1,2,3
./run_preprod_gates.sh 1 3 smoke
```
- Do not commit generated artifacts under:
  - `jepsen/results/`
  - `jepsen/target/`
  - `jepsen/store/`
  - `.nrepl-port`

## Bench
- Local CLI bench:
  - `bench/run_cas.sh`
  - implementation in `bench/lib/bench/cas.ex`
- Fly/Phoenix orchestrator:
  - `bench/bench_web`
  - use for multi-region tests and scenario selection

## Workflow Rules For Agents
- Keep `README.md`, `OPERATORS.md`, and API docs aligned with behavior changes.
- Add or update adversarial/distributed tests for every safety bug fix.
- Prefer the smallest coherent change in `replica.ex` or routing code when touching correctness-sensitive logic.
- Do not reintroduce:
  - default global `:pg` use
  - per-op client routing through `GenServer.call`
  - fake “consistent read” fast paths
  - CAS/LWW mixing on CAS-managed keys
- For flaky distributed tests, separate:
  - “the chosen/committed value exists”
  - “all eventual replicas have converged”
- If a change affects correctness semantics, update Jepsen and the pure Elixir linearizability tests, not just unit tests.
