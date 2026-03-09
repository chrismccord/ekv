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
- Eventual reads on CAS-managed keys are still allowed.
- `consistent: true` is a barrier read, not a fast-path heuristic.

## Runtime Modes

### `mode: :member`
- Durable node.
- Runs SQLite shards, replication, GC, CAS proposer/acceptor logic.
- May run:
  - `wait_for_quorum`
  - `shutdown_barrier`
  - `blue_green`

### `mode: :client`
- Stateless node.
- Does not run SQLite, replicas, GC, sync, or blue-green machinery.
- Uses the same public API by routing to members.
- Supports:
  - `wait_for_route`
  - `wait_for_quorum` (via selected member)
  - `shutdown_barrier`

Important:
- Client mode rejects member-only options like `:blue_green`, `:cluster_size`, `:node_id`, `:data_dir`, `:shards`, `:partition_ttl_policy`.

## CAS Configuration Reality
- CAS requires `cluster_size`.
- `node_id` is logically required for CAS identity, but current code auto-generates and persists one if `cluster_size` is set and `node_id` is omitted.
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

### Quorum and membership
- Quorum is `floor(cluster_size / 2) + 1`.
- Distinct logical `node_id`s drive quorum and overflow checks.
- If visible logical members exceed `cluster_size`, CAS must fail with `{:error, :cluster_overflow}`.

### Sync / HWM correctness
- `kv_peer_hwm` is monotonic.
- Sender stores peer HWM as sender snapshot `my_seq`, not remote sequence.
- Delta sync is only valid when the peer cursor is still inside the local oplog window.
- Otherwise force full sync.
- Chunked sync rules matter:
  - intermediate chunks use seq `0`
  - final chunk must send a non-zero terminal seq
  - exact-multiple chunk sizes still require a final non-zero seq chunk

### Long partition protection
- Startup stale-db rejection:
  - if idle age exceeds roughly `tombstone_ttl - gc_interval`, startup fails closed by default
  - operator must either wipe that node's data dir or explicitly set `allow_stale_startup: true`
- Live long partition protection:
  - default `partition_ttl_policy: :quarantine`
  - reconnect after downtime > `tombstone_ttl` blocks replication for that peer pair
- Down-since markers live in `kv_meta`, keyed by `node_id` when available, otherwise node name.

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
- `lib/ekv/replica.ex`
  - shard process
  - LWW path
  - CASPaxos prepare/accept/promote
  - sync/HWM logic
  - quarantine
  - handoff/proxy mode
- `lib/ekv/store.ex`
  - schema and persistence primitives
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
mix test test/distributed_test.exs
mix test test/adversarial_verification_test.exs
mix test test/ekv_test.exs
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
  - `priv/bench/run_cas.sh`
  - implementation in `priv/bench/lib/bench/cas.ex`
- Fly/Phoenix orchestrator:
  - `priv/bench_web`
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
