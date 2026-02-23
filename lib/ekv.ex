defmodule EKV do
  @moduledoc """
  Eventually consistent durable key-value store with zero runtime dependencies.

  EKV stores data on disk (survives restarts and node death) and replicates
  across all connected Erlang nodes automatically. There is no leader — every
  node accepts reads and writes at all times, including during network
  partitions. When connectivity is restored, nodes converge to the same state.

  ## Quick Start

      # In your supervision tree
      {EKV, name: :my_kv, data_dir: "/var/data/ekv"}

      # Basic operations
      EKV.put(:my_kv, "user/1", %{name: "Alice"})
      EKV.get(:my_kv, "user/1")          #=> %{name: "Alice"}
      EKV.delete(:my_kv, "user/1")
      EKV.get(:my_kv, "user/1")          #=> nil

      # Prefix scans
      EKV.scan(:my_kv, "user/")          #=> %{"user/1" => %{name: "Alice"}}
      EKV.keys(:my_kv, "user/")          #=> ["user/1"]

      # TTL — entry expires after 30 minutes
      EKV.put(:my_kv, "session/abc", token, ttl: :timer.minutes(30))

      # Subscribe to changes
      EKV.subscribe(:my_kv, "rooms/")
      EKV.put(:my_kv, "rooms/1", %{title: "Elixir"})
      # receive
      # => {:ekv, [%EKV.Event{type: :put, key: "rooms/1", value: %{title: ...}}], %{name: :my_kv}}

  Values can be any Erlang/Elixir term. They are stored as `:erlang.term_to_binary/1`
  internally and deserialized with `:erlang.binary_to_term/1` on read.
  *Note*: Avoid storing structs or anonymous functions within values.
  See `Value Serialization Caveats` below for more information.

  ## Consistency Guarantees

  EKV provides **eventual consistency**: all nodes will converge to the same
  state given sufficient time and connectivity. It does **not** provide:

  - **Read-your-writes across nodes.** A write on node A is not immediately
    visible on node B. Replication is asynchronous. On the *local* node, writes
    are immediately visible.

  - **Causal ordering.** If node A writes key "x" then key "y", another node
    may see "y" before "x" (or "y" without "x" during a partition).

  - **Transactions.** There is no way to atomically read-modify-write, or to
    write multiple keys as a single unit. Each key is independent.

  ### Conflict Resolution: Last-Writer-Wins (LWW)

  When two nodes write the same key concurrently, EKV keeps the write with the
  **highest nanosecond timestamp**. If timestamps are identical (unlikely but
  possible with clock sync), the write from the **lexicographically greater
  node name** wins as a deterministic tiebreaker.

  This means:

  - The "latest" write always wins, where "latest" is determined by the
    writer's local `System.system_time(:nanosecond)`.

  - **Clock skew matters.** If node A's clock is 5 seconds ahead of node B,
    then A's writes will beat B's writes for the same key even if B wrote
    "after" A in wall-clock time. For best results, run NTP or a similar time
    sync service on all nodes. Clock skew of a few milliseconds (typical for
    NTP) is fine — conflicts at that granularity are rare. Clock skew of
    seconds or more will cause surprising results.

  - **Deletes are writes too.** A delete is a timestamped tombstone. If you
    delete a key on node A at time T1 and write the same key on node B at time
    T2 > T1, the write wins and the key reappears. This is by design — the
    higher timestamp always wins regardless of whether the operation was a put
    or a delete.

  ### Reads During Partitions

  During a network partition, each side of the partition continues to serve
  reads and accept writes independently. When the partition heals, the nodes
  sync and converge using LWW. Data is never lost — the "losing" write is
  simply overwritten by the "winning" one.

  ## Configuration

  All options are passed when starting EKV:

      {EKV,
        name: :my_kv,
        data_dir: "/var/data/ekv",
        shards: 8,
        tombstone_ttl: :timer.hours(24 * 7),
        gc_interval: :timer.minutes(5),
        log: :info}

  | Option | Default | Description |
  |--------|---------|-------------|
  | `:name` | *required* | Atom identifying this EKV instance. Used to register processes and as the first argument to all API functions. |
  | `:data_dir` | *required* | Directory where SQLite database files are stored. Created automatically if it doesn't exist. Each shard gets its own file (`shard_0.db`, `shard_1.db`, etc.). |
  | `:shards` | `8` | Number of shards. See "Choosing a Shard Count" below. |
  | `:tombstone_ttl` | `604_800_000` (7 days) | How long tombstones (deleted entries) are kept before being permanently purged, in milliseconds. See "Tombstone Lifetime" below. |
  | `:gc_interval` | `300_000` (5 min) | How often garbage collection runs, in milliseconds. GC expires TTL entries, purges old tombstones, and truncates the replication oplog. |
  | `:log` | `:info` | Logging level. `:info` logs cluster events (connects, syncs). `false` disables logging. `:verbose` logs per-shard detail. |

  ### Choosing a Shard Count

  The shard count controls write parallelism. Each shard is an independent
  SQLite database with its own writer process. Writes to different shards
  execute in parallel; writes to the same shard are serialized.

  - **8 shards** (default) — good for most workloads. Provides 8-way write
    parallelism, which saturates typical NVMe drives.

  - **1–2 shards** — appropriate for low-write-volume use cases (configuration
    stores, feature flags) where simplicity matters more than throughput.

  - **16–32 shards** — consider this for write-heavy workloads (>10k writes/sec)
    on fast storage, or when you have many cores and want to reduce lock
    contention.

  - **>32 shards** — rarely needed. Each shard opens multiple SQLite
    connections and file descriptors. More shards means more memory and more
    files.

  **The shard count is permanent.** It is persisted in each database file on
  first open. If you later start EKV with a different shard count against the
  same `data_dir`, it will raise an `ArgumentError`. To change the shard
  count, you must delete the existing data directory and start fresh. All
  replicas in the cluster must use the same shard count — a node with a
  mismatched count will have its replication connections rejected by peers.

  ### Tombstone Lifetime

  When you delete a key, EKV doesn't immediately erase it. Instead, it writes
  a timestamped tombstone that replicates to all peers, ensuring every node
  learns about the delete. Tombstones are permanently purged after
  `tombstone_ttl` (default 7 days).

  If a node is offline for longer than the tombstone TTL, it may miss deletes
  that have already been purged from other nodes. EKV handles this with
  **stale database detection**: on startup, if the database's last activity
  timestamp is older than the tombstone TTL, the database is automatically
  wiped and rebuilt via full sync from a peer. This prevents "zombie" keys
  from reappearing.

  Reduce `tombstone_ttl` if storage is tight and your nodes are rarely offline
  for long. Increase it if nodes may be offline for extended maintenance
  windows.

  ## Multiple Instances

  You can run multiple independent EKV instances in the same BEAM by giving
  each a different `:name`. Each instance has its own SQLite databases, its
  own replication mesh, and its own configuration. They do not interact.

      children = [
        {EKV, name: :users, data_dir: "/data/users"},
        {EKV, name: :sessions, data_dir: "/data/sessions"}
      ]

  ## Replication

  Replication is automatic and requires no configuration beyond Erlang
  distribution. When a new node connects (via `Node.connect/1` or a cluster
  manager like `DNSCluster`), EKV discovers peer shards and syncs:

  - **Delta sync** — if the nodes were recently connected and the replication
    log hasn't been truncated, only the missed entries are sent.

  - **Full sync** — if this is the first connection or the node was away long
    enough for the oplog to be truncated, a full state transfer is performed.

  After the initial sync, every local write is replicated to all connected
  peers in real time (fire-and-forget, async). Consistency is maintained by
  LWW, not by delivery order.

  ## TTL

  Entries can be given a time-to-live:

      EKV.put(:my_kv, "session/abc", token, ttl: :timer.minutes(30))

  Expired entries are not returned by `get/2`, `scan/2`, or `keys/2`. They are
  converted to tombstones by the periodic GC and then replicated as deletes,
  so expiry is eventually consistent across nodes. The GC interval (default 5
  minutes) determines the maximum delay before an expired entry is tombstoned
  and its deletion broadcast.

  ### Value Serialization Caveats

  Because values are persisted to disk and may be deserialized by a different
  version of your application, **avoid storing terms that are tied to the
  running code**:

  - **Structs** — a struct is a map with a `__struct__` key pointing to a
    module atom. If the module is renamed, removed, or its fields change,
    deserialization will produce a bare map or a struct with missing/extra
    keys. Prefer plain maps (e.g. `%{type: "user", name: "Alice"}`) for
    durable storage.

  - **Anonymous functions** — an anonymous function captures a reference to
    the module and function clause that created it. After a code deploy, that
    reference is invalid and `:erlang.binary_to_term/1` will fail. Never
    store anonymous functions within values.

  - **PIDs, ports, references** — these are ephemeral identifiers that are
    meaningless after a restart.

  If you need to evolve your value schema over time, **version-stamp your
  values**:

      # Write
      EKV.put(:my_kv, "user/1", {2, %{name: "Alice", email: "a@example.com"}})

      # Read with migration
      case EKV.get(:my_kv, "user/1") do
        {2, data} -> data
        {1, data} -> Map.put(data, :email, nil)   # migrate v1 → v2
        nil       -> nil
      end

  This way old values written before a schema change are migrated on read
  without needing to backfill every key.
  """

  alias EKV.Replica

  # ===========================================================================
  # Startup
  # ===========================================================================

  def child_spec(opts) do
    name = Keyword.fetch!(opts, :name)
    %{id: {__MODULE__, name}, start: {EKV.Supervisor, :start_link, [opts]}, type: :supervisor}
  end

  def start_link(opts), do: EKV.Supervisor.start_link(opts)

  @doc """
  Put a key-value pair.

  ## Options

  - `:ttl` — time-to-live in milliseconds. Entry expires after this duration.
  """
  def put(name, key, value, opts \\ []) do
    opts = Keyword.validate!(opts, [:ttl])
    value_binary = :erlang.term_to_binary(value)
    config = get_config(name)
    shard_index = Replica.shard_index_for(key, config.num_shards)
    GenServer.call(Replica.shard_name(name, shard_index), {:put, key, value_binary, opts})
  end

  @doc """
  Get a value by key. Returns `nil` for missing, expired, or deleted entries.

  Reads directly from SQLite shard via per-scheduler read connection.
  """
  def get(name, key) do
    config = get_config(name)
    shard_index = Replica.shard_index_for(key, config.num_shards)
    {db, get_stmt} = read_conn(name, shard_index)

    case EKV.Store.get_cached(db, get_stmt, key) do
      nil ->
        nil

      {_value_binary, _ts, _origin, _expires_at, deleted_at} when is_integer(deleted_at) ->
        nil

      {value_binary, _ts, _origin, expires_at, nil} when is_integer(expires_at) ->
        now = System.system_time(:nanosecond)

        if expires_at <= now do
          nil
        else
          :erlang.binary_to_term(value_binary)
        end

      {value_binary, _ts, _origin, _expires_at, nil} ->
        :erlang.binary_to_term(value_binary)
    end
  end

  @doc """
  Delete a key.

  Writes a tombstone that replicates to all peers.
  """
  def delete(name, key) do
    config = get_config(name)
    shard_index = Replica.shard_index_for(key, config.num_shards)
    GenServer.call(Replica.shard_name(name, shard_index), {:delete, key})
  end

  @scan_sql """
  SELECT key, value FROM kv
  WHERE key >= ?1 AND key < ?2
    AND (deleted_at IS NULL OR deleted_at > ?3)
    AND (expires_at IS NULL OR expires_at > ?3)
  """

  @keys_sql """
  SELECT key FROM kv
  WHERE key >= ?1 AND key < ?2
    AND (deleted_at IS NULL OR deleted_at > ?3)
    AND (expires_at IS NULL OR expires_at > ?3)
  ORDER BY key
  """

  @doc """
  Scan key-value pairs matching a prefix.

  Scans all shards.

  Returns `%{key => value}`.
  """
  def scan(name, prefix) do
    config = get_config(name)
    now = System.system_time(:nanosecond)
    prefix_end = EKV.Store.next_binary_prefix(prefix)

    Enum.reduce(0..(config.num_shards - 1), %{}, fn shard, acc ->
      {db, _} = read_conn(name, shard)
      {:ok, rows} = EKV.Sqlite3.fetch_all(db, @scan_sql, [prefix, prefix_end, now])

      Enum.reduce(rows, acc, fn [key, value_binary], map ->
        Map.put(map, key, :erlang.binary_to_term(value_binary))
      end)
    end)
  end

  @doc """
  List keys matching a prefix. Scans all shards.

  Returns `[key]` sorted.
  """
  def keys(name, prefix) do
    config = get_config(name)
    now = System.system_time(:nanosecond)
    prefix_end = EKV.Store.next_binary_prefix(prefix)

    for shard <- 0..(config.num_shards - 1),
        {db, _} = read_conn(name, shard),
        {:ok, rows} = EKV.Sqlite3.fetch_all(db, @keys_sql, [prefix, prefix_end, now]),
        [key] <- rows do
      key
    end
    |> Enum.sort()
  end

  # ===========================================================================
  # Subscribe / Unsubscribe
  # ===========================================================================

  @doc """
  Subscribe the calling process to change events for keys matching `prefix`.

  The subscriber receives messages of the form:

      {:ekv, [%EKV.Event{type: :put | :delete, key: key, value: value}], %{name: name}}

  - `:put` events contain the new value (decoded Elixir term).
  - `:delete` events contain the previous value before deletion (or `nil`).

  ## Prefix Matching

  Prefixes match at `"/"` boundaries:

  - `""` — matches **all** keys (wildcard). This is the default.
  - `"user/"` — matches `"user/1"`, `"user/abc/xyz"`, etc.
  - `"user/1"` — matches **exactly** `"user/1"` (no trailing `/` = exact key match).

  A subscription to `"foo"` does **not** match `"foobar"`. To match all
  keys under a namespace, use a trailing slash: `"foo/"`.

  ## Delivery

  Events are dispatched asynchronously — the write returns to the caller
  before subscribers are notified. Under load, multiple writes may be
  batched into a single message to each subscriber.

  A process subscribed to overlapping prefixes (e.g. both `""` and
  `"user/"`) receives each event exactly once.

  Delivery is best-effort. Events may be lost if a dispatcher process
  crashes between receiving the dispatch and sending to subscribers.
  """
  def subscribe(name, prefix \\ "") do
    config = get_config(name)

    case Registry.register(config.registry, prefix, nil) do
      {:ok, _} ->
        :atomics.add(config.sub_count, 1, 1)
        :ok

      {:error, {:already_registered, _}} ->
        :ok
    end
  end

  @doc """
  Unsubscribe the calling process from events for the given prefix.
  """
  def unsubscribe(name, prefix \\ "") do
    config = get_config(name)
    Registry.unregister(config.registry, prefix)
    :ok
  end

  def get_config(name) do
    :persistent_term.get({EKV, name})
  end

  defp read_conn(name, shard_index) do
    readers = :persistent_term.get({EKV, name, :readers, shard_index})
    sid = :erlang.system_info(:scheduler_id)
    elem(readers, rem(sid - 1, tuple_size(readers)))
  end
end
