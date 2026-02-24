# EKV

Eventually consistent durable KV store for Elixir with zero runtime dependencies.

Data survives node restarts, node death, and network partitions. Replication is peer-to-peer across all connected Erlang nodes using delta sync via per-shard oplogs. Storage is backed by SQLite (vendored, compiled as a NIF) with zero runtime dependencies.

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

EKV.scan(:my_kv, "user/")
#=> %{"user/1" => %{name: "Alice"}, "user/2" => %{name: "Bob"}}

EKV.keys(:my_kv, "user/")
#=> ["user/1", "user/2"]

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
| `:data_dir` | *required* | Directory for SQLite database files |
| `:shards` | `8` | Number of shards (each is an independent GenServer + SQLite db) |
| `:tombstone_ttl` | `604_800_000` (7 days) | How long tombstones are retained in milliseconds |
| `:gc_interval` | `300_000` (5 min) | GC tick interval in milliseconds |
| `:log` | `:info` | `:info`, `false` (silent), or `:verbose` |

## How It Works

### Storage

Each shard has a single SQLite database (WAL mode) as its sole storage layer — no data is held in memory so your dataset is not bound by available system memory. Writes go through the shard GenServer which atomically updates both the `kv` table and `kv_oplog` in a single NIF call. Reads go directly to SQLite via per-scheduler read connections stored in `persistent_term`.

Data survives restarts automatically since SQLite is the source of truth.

### Replication

Every write is broadcast to the counterpart shard on all connected peers. Peer discovery is self-contained by monitoring nodes going up an ddown.

*Note: Node connection is left up to the user, ie either explicit `Node.connect/1`/`sys.config`, or using a library like `DNSCluster`, or `libcluster`.

When a node connects (or reconnects), each shard pair exchanges a handshake. Based on high-water marks (HWMs), they decide:

- **Delta sync** if the oplog still has entries since the peer's last known position (efficient for brief disconnects).
- **Full sync** if the oplog has been truncated past that point or the peer is new (sends all live entries + recent tombstones).

### Conflict resolution

Last-Writer-Wins with nanosecond timestamps. Ties are broken deterministically by comparing origin node atoms, so all nodes converge to the same result without coordination.

A delete is just an entry with `deleted_at` set. Same LWW rules apply -- a put with a higher timestamp beats a delete, and vice versa.

### Garbage collection

A periodic GC timer runs three phases per tick:

1. **Expire TTL entries** -- converts expired entries to tombstones and broadcasts deletes
2. **Purge old tombstones** -- hard-deletes tombstones older than `tombstone_ttl` from SQLite
3. **Truncate oplog** -- removes oplog entries below the minimum peer HWM

### Stale database protection

If a node goes away longer than `tombstone_ttl` and comes back with an old database on disk, peers will have already GC'd the tombstones for entries deleted during the absence. EKV detects this by checking a `last_active_at` timestamp stored in the database. If the database is staler than `tombstone_ttl`, it's wiped on startup and rebuilt from scratch via full sync.

## Multiple instances

Each EKV instance (identified by `:name`) is fully independent -- its own SQLite files, shard GenServers, and peer mesh. To isolate replication between groups of nodes, start separate EKV instances with different names on the nodes that should form each group.

```elixir
# Only nodes in the US region start this:
{EKV, name: :us_data, data_dir: "/data/ekv/us"}

# Only nodes in the EU region start this:
{EKV, name: :eu_data, data_dir: "/data/ekv/eu"}
```

## License

MIT
