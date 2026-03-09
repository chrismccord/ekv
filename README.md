# EKV

Eventually consistent durable KV store for Elixir with opt-in per-key linearizable CAS, with zero runtime dependencies.

Data survives node restarts, node death, and network partitions. Member nodes replicate directly across all connected Erlang nodes using delta sync via per-shard oplogs. Storage is backed by SQLite (vendored, compiled as a NIF) with zero runtime dependencies.

## Installation

```elixir
def deps do
  [
    {:ekv, "~> 0.1.5"}
  ]
end
```

EKV uses sqlite as the storage layer. Precompiled NIF binaries are available for common platforms. If a precompiled binary isn't available for your system, it will compile from source (requires a C compiler).

## Usage

Add EKV to your supervision tree:

```elixir
children = [
  {EKV, name: :my_kv, data_dir: "data/ekv/my_kv"}
]
```

Or start a stateless client that routes to members by region preference:

```elixir
children = [
  {EKV,
   name: :my_kv_client,
   mode: :client,
   region: "ord",
   region_routing: ["iad", "dfw", "lhr"],
   wait_for_route: :timer.seconds(10),
   wait_for_quorum: :timer.seconds(10)}
]
```

Then use the API:

```elixir
# Put / Get / Delete
EKV.put(:my_kv, "user/1", %{name: "Alice", role: :admin})
EKV.get(:my_kv, "user/1")
#=> %{name: "Alice", role: :admin}

EKV.delete(:my_kv, "user/1")
EKV.get(:my_kv, "user/1")
#=> nil

# TTL
EKV.put(:my_kv, "session/abc", token, ttl: :timer.minutes(30))

# Prefix scans
EKV.put(:my_kv, "user/1", %{name: "Alice"})
EKV.put(:my_kv, "user/2", %{name: "Bob"})

EKV.scan(:my_kv, "user/") |> Enum.to_list()
#=> [
#=>   {"user/1", %{name: "Alice"}, {ts, origin_node}},
#=>   {"user/2", %{name: "Bob"}, {ts, origin_node}}
#=> ]

EKV.keys(:my_kv, "user/")
#=> [{"user/1", {ts, origin_node}}, {"user/2", {ts, origin_node}}]

# Subscribe to a key
EKV.subscribe(:my_kv, "room/1")
EKV.put(:my_kv, "room/1", %{title: "Elixir"})

    # => receive
    {:ekv, [%EKV.Event{type: :put, key: "room/1", value: %{title: "Elixir"}}], %{name: :my_kv}}

# Subscribe to a prefix
EKV.subscribe(:my_kv, "room/")

    # => receive
    {:ekv, [
     %EKV.Event{type: :put, key: "room/1", value: %{title: "Elixir"}},
     %EKV.Event{type: :put, key: "room/2", value: %{title: "Phoenix"}}
    ], %{name: :my_kv}}

EKV.unsubscribe(:my_kv, "room/")
```

Values can be any Erlang term (stored via `:erlang.term_to_binary/1`). Keys are strings.

## Options

| Option | Default | Description |
|--------|---------|-------------|
| `:name` | *required* | Atom identifying this EKV instance |
| `:mode` | `:member` | `:member` stores/replicates data and participates in CAS quorum. `:client` is stateless and routes requests to members. |
| `:region` | `"default"` | Region label exposed by members and used by clients for routing preference. |
| `:region_routing` | `nil` | Client mode only. Ordered list of preferred member regions. |
| `:wait_for_route` | `false` | Client mode only. Optional startup gate. Blocks startup until the first reachable member in `:region_routing` order is selected. |
| `:data_dir` | *required in `:member`* | Directory for SQLite database files |
| `:cluster_size` | `nil` | Member mode only. Required for CAS (`if_vsn`, `consistent: true`, `update/4`). Total number of logical voting members. |
| `:node_id` | `nil` | Member mode only. Required for CAS. Stable logical member id used in ballots, quorum accounting, and blue-green overlap. |
| `:shards` | `8` | Member mode only. Number of shards (each is an independent GenServer + SQLite db) |
| `:tombstone_ttl` | `604_800_000` (7 days) | Member mode only. How long tombstones are retained in milliseconds |
| `:gc_interval` | `300_000` (5 min) | Member mode only. GC tick interval in milliseconds |
| `:wait_for_quorum` | `false` | Optional startup gate. In member mode, waits for this EKV member to reach CAS quorum. In client mode, waits for the selected backend member to report CAS quorum reachable. |
| `:shutdown_barrier` | `false` | Optional graceful-shutdown barrier. Keeps EKV serving during coordinated shutdown for up to the configured timeout so members can finish final writes and replication. |
| `:allow_stale_startup` | `false` | Member mode only. Dangerous recovery override. If `true`, EKV trusts on-disk data even when stale-db detection would normally refuse startup. Intended only for explicit disaster recovery / full cold-cluster restore cases. |
| `:blue_green` | `false` | Member mode only. Enable blue-green deployment handoff for shared-volume replacement nodes. |
| `:log` | `:info` | `:info`, `false` (silent), or `:verbose` |
| `:partition_ttl_policy` | `:quarantine` | Member mode only. Policy when a member identity reconnects after being disconnected longer than `tombstone_ttl`. `:quarantine` blocks replication with that member until operator intervention. `:ignore` disables that quarantine and allows reconnect/sync anyway. |

### Client mode

Client mode keeps the EKV API but does not start SQLite, replication, GC, or
blue-green machinery on that node.

- Eventual reads become remote reads against the selected member.
- `wait_for_route` can hold startup until a backend route is selected.
- `wait_for_quorum` can additionally hold startup until that backend reports CAS quorum reachable.
- `scan/2` and `keys/2` still return Elixir streams, but are backed by paged RPC.
- `subscribe/2` works in client mode; client subscribers are delivered cluster-wide.
- Routing, subscriptions, and shutdown coordination use an EKV-instance-specific
  `:pg` scope, so multiple EKV instances can share a cluster without mixing
  control traffic.
- After backend failover, eventual reads may observe an older replica view.
  Use `consistent: true` when freshness matters.

## How It Works

### Storage

Each shard has a single SQLite database (WAL mode) as its sole storage layer — no data is held in memory so your dataset is not bound by available system memory. Writes go through the shard GenServer which atomically updates both the `kv` table and `kv_oplog` in a single NIF call. Reads go directly to SQLite via per-scheduler read connections stored in `persistent_term`.

Data survives restarts automatically since SQLite is the source of truth.

### Replication

Every write is broadcast to the counterpart shard on all connected members. Member
discovery is self-contained by monitoring connected Erlang nodes going up
and down. Client routing, client subscriptions, and shutdown coordination are
separate and use an EKV-instance-specific `:pg` scope.

*Note: Node connection is left up to the user, ie either explicit `Node.connect/1`/`sys.config`, or using a library like `DNSCluster`, or `libcluster`.

When a node connects (or reconnects), each shard pair exchanges a handshake. Based on high-water marks (HWMs), they decide:

- **Delta sync** if the oplog still has entries since the member's last known position (efficient for brief disconnects).
- **Full sync** if the oplog has been truncated past that point or the member is new (sends all live entries + recent tombstones).

### Conflict resolution

Last-Writer-Wins with nanosecond timestamps. Ties are broken deterministically by comparing origin node atoms, so all nodes converge to the same result without coordination.

A delete is just an entry with `deleted_at` set. Same LWW rules apply -- a put with a higher timestamp beats a delete, and vice versa.

### Consistency Modes - LWW vs CAS (Compare-And-Swap)

EKV supports two write modes:

- **Eventual/LWW mode**: default `EKV.put/4` and `EKV.delete/3` without CAS options.
- **CAS mode**: `EKV.put/4` with `if_vsn:` or `consistent: true`, `EKV.delete/3` with `if_vsn:`, and `EKV.update/4`.

Use consistent mode as **key ownership**:

- Different keys may use different consistency modes in the same EKV instance.
- A key may start in eventual/LWW mode and later transition to CAS mode
  (`LWW -> CAS` is supported).
- Once a key is CAS-managed, eventual writes on that key are rejected
  (`CAS -> LWW` is not supported for writes).
- Keys managed via CAS should keep using CAS **write** APIs.
- Reads for CAS-managed keys can be eventual (`EKV.get/2`, `EKV.lookup/2`) for
  lower latency, or consistent (`EKV.get/3, consistent: true`) when freshness
  matters. `consistent: true` is a barrier/linearizable read.
- `EKV.keys/2` returns `{key, vsn}` tuples so callers can pipeline scans into
  CAS writes (`if_vsn:`) without fetching full values.
- CAS write APIs return committed VSNs on success (`{:ok, vsn}` for
  `put/delete`, `{:ok, value, vsn}` for `update`) so callers can chain
  later `if_vsn:` guards without an extra lookup.
- Eventual writes on CAS-managed keys return `{:error, :cas_managed_key}`.

### CAS write error semantics

CAS writes (`put` with `if_vsn:` or `consistent: true`, `delete` with `if_vsn:`,
`update`) can return:

- `{:error, :conflict}`: write was rejected before a deciding accept phase
  (for example: version mismatch or pre-accept contention).
- `{:error, :unconfirmed}`: write entered accept phase, but the caller
    could not confirm final outcome. The write may already be committed,
    or it may have lost to a competing ballot and never committed.

On `:unconfirmed`, resolve with `EKV.get(name, key, consistent: true)` before
taking follow-up actions.

You can opt in to internal resolution per call with
`resolve_unconfirmed: true` on CAS write APIs. In that mode, EKV performs one
barrier read when an ambiguous accept outcome occurs and returns resolved
current-state outcomes (`{:ok, ...}` or `{:error, :conflict}`) when possible,
or `{:error, :unavailable}` if the resolution read itself cannot complete.

### Mixed-mode note

Transition rule per key:

- `LWW -> CAS`: allowed.
- `CAS -> LWW` (eventual `put/delete`): rejected with
  `{:error, :cas_managed_key}`.

Keep lock/ownership keyspaces CAS-write-only after transition.

### Garbage collection

A periodic GC timer runs three phases per tick:

1. **Expire TTL entries** -- converts expired entries to tombstones and broadcasts deletes
2. **Purge old tombstones** -- hard-deletes tombstones older than `tombstone_ttl` from SQLite
3. **Truncate oplog** -- removes oplog entries below the minimum member HWM

### Stale database protection

If a node goes away longer than `tombstone_ttl` and comes back with an old database on disk, other members will have already GC'd the tombstones for entries deleted during the absence. EKV detects this by checking a `last_active_at` timestamp stored in the database. If the database is too stale, EKV fails startup by default instead of trusting that on-disk state. Operators can then wipe that node's data dir so it rebuilds from members, or explicitly set `allow_stale_startup: true` when they intend to trust the old on-disk cluster state.

### Long live-partition protection

A different edge case is when nodes stay up but are partitioned longer than
`tombstone_ttl`. In that window, one side can purge delete tombstones before
reconnect.

With the default `partition_ttl_policy: :quarantine`, EKV detects reconnects
after a downtime longer than `tombstone_ttl` and quarantines that member pair
instead of syncing potentially unsafe state. Replication stays blocked for
that member until an operator rebuilds one side.

Down-since markers are persisted in `kv_meta`, keyed by `node_id` when
available (fallback: node name), so restart does not clear quarantine history.
This also means node-name churn does not bypass quarantine when `node_id` is
stable.

Fallback name-based markers are bounded: EKV prunes very old entries and caps
the retained set per shard to avoid unbounded growth over long periods.

## Multiple instances

Each EKV instance (identified by `:name`) is fully independent -- its own
SQLite files, shard GenServers, member mesh, and scoped `:pg` control
plane for routing, subscriptions, and shutdown coordination. To isolate
replication between groups of nodes, start separate EKV instances with
different names on the nodes that should form each group.

```elixir
# Only nodes in the US region start this:
{EKV, name: :us_data, data_dir: "/data/ekv/us"}

# Only nodes in the EU region start this:
{EKV, name: :eu_data, data_dir: "/data/ekv/eu"}
```

## License

MIT
