defmodule EKV do
  @moduledoc """
  Eventually consistent durable key-value store with zero runtime dependencies.

  EKV stores data on disk (survives restarts and node death) and replicates
  across all connected Erlang nodes automatically. There is no leader for
  eventual mode — every node serves reads and eventual writes at all times,
  including during network partitions. CAS writes (`if_vsn:`, `consistent: true`,
  `update/4`) require quorum and may fail when quorum is unavailable. When
  connectivity is restored, nodes converge to the same state.

  ## Quick Start

      # In your supervision tree
      {EKV, name: :my_kv, data_dir: "/var/data/ekv"}

      # Basic operations
      EKV.put(:my_kv, "user/1", %{name: "Alice"})
      EKV.get(:my_kv, "user/1")          #=> %{name: "Alice"}
      EKV.delete(:my_kv, "user/1")
      EKV.get(:my_kv, "user/1")          #=> nil

      # Prefix scans
      EKV.scan(:my_kv, "user/") |> Enum.to_list()
      #=> [{"user/1", %{name: "Alice"}, {ts, origin_node}}]

      EKV.keys(:my_kv, "user/") |> Enum.to_list() #=> [{"user/1", {ts, origin_node}}]

      # TTL — entry expires after 30 minutes
      EKV.put(:my_kv, "session/abc", token, ttl: :timer.minutes(30))

      # Subscribe to changes
      EKV.subscribe(:my_kv, "rooms/")
      EKV.put(:my_kv, "rooms/1", %{title: "Elixir"})
      # receive
      # => {:ekv, [%EKV.Event{type: :put, key: "rooms/1", value: %{title: ...}}], %{name: :my_kv}}

      # CAS setup (requires cluster_size + node_id)
      {EKV, name: :my_kv_cas, data_dir: "/var/data/ekv_cas", cluster_size: 3, node_id: 1}

      # CAS put via lookup + if_vsn
      case EKV.lookup(:my_kv_cas, "lock/job-123") do
        nil ->
          EKV.put(:my_kv_cas, "lock/job-123", %{owner: "node-a"}, if_vsn: nil)

        {_value, vsn} ->
          EKV.put(:my_kv_cas, "lock/job-123", %{owner: "node-a"}, if_vsn: vsn)
      end

      # Handling ambiguous CAS result explicitly
      case EKV.put(:my_kv_cas, "lock/job-123", %{owner: "node-a"}, if_vsn: nil) do
        {:ok, vsn} ->
          {:locked, vsn}

        {:error, :conflict} ->
          :already_locked

        {:error, :unconfirmed} ->
          # Resolve by reading through the CAS barrier
          EKV.get(:my_kv_cas, "lock/job-123", consistent: true)
      end

      # Or opt in to internal ambiguity resolution for this call
      EKV.put(:my_kv_cas, "lock/job-123", %{owner: "node-a"},
        if_vsn: nil,
        resolve_unconfirmed: true
      )

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

  - **Multi-key transactions.** There is no way to atomically write multiple
    keys as a single unit. Per-key atomic read-modify-write is available via
    `update/4` (CAS mode).

  ### Conflict Resolution: Last-Writer-Wins (LWW)

  For eventual-mode writes (`put/4` and `delete/3` without CAS options), EKV
  keeps the write with the **highest nanosecond timestamp**. If timestamps are
  identical (unlikely but possible with clock sync), the write from the
  **lexicographically greater node name** wins as a deterministic tiebreaker.

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

  CAS-mode writes do not use LWW ordering; they are ordered by CASPaxos ballots.

  ### Reads During Partitions

  During a network partition, each side of the partition continues to serve
  reads and accept eventual writes independently. CAS writes continue only on
  partitions that can reach quorum; minority partitions return CAS errors such
  as `{:error, :no_quorum}` or `{:error, :quorum_timeout}`. When the partition
  heals, nodes sync and converge. For eventual writes, LWW determines the
  surviving value for conflicting writes on the same key.

  ### Consistency Modes Per Key (Important)

  EKV supports two write modes:

  - **Eventual/LWW mode** — default `put/4` and `delete/3` without CAS options.
  - **CAS mode** — `put/4` with `if_vsn:` or `consistent: true`, `delete/3`
    with `if_vsn:`, and `update/4`.

  For predictable semantics, treat mode as **key ownership**:

  - Different keys may use different modes in the same EKV instance.
  - A key may start in eventual/LWW mode and later transition to CAS mode
    (`LWW -> CAS` is supported).
  - Once a key is CAS-managed, eventual writes on that key are rejected
    (`CAS -> LWW` is not supported for writes).
  - Keys managed via CAS should continue to use CAS **write** APIs.
  - Reads for CAS-managed keys may be eventual (`get/2`, `lookup/2`) when
    staleness is acceptable, or consistent (`get/3, consistent: true`) when
    fresh linearizable reads are required.
  - After transition to CAS mode, do not issue eventual writes on that key.

  ### CAS Outcomes and `:unconfirmed`

  CAS write APIs (`put/4` with `if_vsn:` or `consistent: true`, `delete/3`
  with `if_vsn:`, and `update/4`) can return:

  - `{:ok, ...}` — write committed; returns the committed VSN.
  - `{:error, :conflict}` — definite non-application for this attempt
    (for example stale `if_vsn`).
  - `{:error, :unconfirmed}` — write reached accept phase, so final outcome is
    ambiguous from the caller's perspective.

  On `:unconfirmed`, callers can issue `get(name, key, consistent: true)` to
  resolve current committed state before taking follow-up actions.

  For convenience, pass `resolve_unconfirmed: true` on CAS writes to make EKV
  perform one internal barrier read and map ambiguous outcomes to current-state
  results: `{:ok, ...}` / `{:error, :conflict}` when resolvable, or
  `{:error, :unavailable}` if the resolution read cannot complete.

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
  | `:cluster_size` | `nil` | Logical cluster size for CAS quorum math. Required for CAS operations (`if_vsn:`, `consistent: true`, `update/4`). |
  | `:node_id` | `nil` | Stable logical member identity used by CAS ballots. Required for CAS operations. Should remain stable for each logical cluster member. |
  | `:tombstone_ttl` | `604_800_000` (7 days) | How long tombstones (deleted entries) are kept before being permanently purged, in milliseconds. See "Tombstone Lifetime" below. |
  | `:gc_interval` | `300_000` (5 min) | How often garbage collection runs, in milliseconds. GC expires TTL entries, purges old tombstones, and truncates the replication oplog. |
  | `:log` | `:info` | Logging level. `:info` logs cluster events (connects, syncs). `false` disables logging. `:verbose` logs per-shard detail. |
  | `:partition_ttl_policy` | `:quarantine` | Policy for reconnects after downtime longer than `tombstone_ttl`. `:quarantine` blocks replication with that peer identity until operator rebuild. `:ignore` keeps legacy behavior. |
  | `:blue_green` | `false` | Enable blue-green deployment mode. See "Blue-Green Deployment" below. |

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

  ### Long Live-Partition Protection

  Startup stale-db detection covers nodes that were down and restarted. A
  separate edge case is a very long network partition where nodes stay up past
  `tombstone_ttl`.

  By default (`partition_ttl_policy: :quarantine`), EKV detects reconnects
  after downtime longer than `tombstone_ttl` and quarantines that peer pair
  instead of syncing potentially unsafe state. Replication remains blocked for
  that peer until an operator rebuilds one side.

  Down-since markers are persisted in `kv_meta`, keyed by `node_id` when
  available (fallback: node name). This preserves quarantine history across
  restarts and prevents node-name churn from bypassing quarantine when
  `node_id` is stable.

  Fallback name-based markers are bounded: EKV periodically prunes very old
  entries and caps retained fallback markers per shard.

  ## Blue-Green Deployment

  When deploying with a blue-green strategy on a single machine, two BEAM VMs
  run side by side briefly — the old release and the new one. If both VMs open
  the same SQLite files simultaneously, the result is corruption. The
  `:blue_green` option solves this with a synchronized handoff — the new VM
  coordinates with the old VM to drain operations, flush WAL, and close the
  writer before opening the same database files.

      {EKV, name: :my_kv, data_dir: "/var/data/ekv", blue_green: true}

  **Startup behavior:**

  - **First boot** (no marker file) — writes the marker and operates normally.

  - **Same node restart** (marker node matches `node()`) — no handoff needed.
    This is a normal EKV restart within the same VM.

  - **Different node** (marker node does not match `node()`) — synchronized
    handoff. The new VM sends a handoff request to each shard on the old VM.
    The old VM drains pending CAS operations, persists its ballot counter,
    checkpoints the WAL, and closes its writer connection. Only then does the
    new VM open the database files. The old VM enters proxy mode, forwarding
    any remaining write requests to the new VM.

  If the old VM is dead when the new VM starts, the handoff requests time out
  after 5 seconds and the new VM opens the files directly. SQLite's WAL
  recovery handles any incomplete writes.

  **Requirements:**

  - Each deploy must use a **different node name** (e.g. include a timestamp
    or release version in the name). Both VMs must share the same `node_id`.

  - The `data_dir` must be on a **shared filesystem** accessible to both VMs.

  **Disk space:** No additional storage — both VMs use the same database files
  (sequentially, never simultaneously).

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
  Look up a key's value and version.

  Returns `{value, vsn}` where `vsn` is `{timestamp, origin_node}`,
  or `nil` for missing, deleted, or expired keys.

  The vsn can be passed to `put/4` with `if_vsn:` for compare-and-swap.
  This is a local read (eventually consistent, no GenServer hop).
  """
  def lookup(name, key) do
    config = get_config(name)
    shard_index = Replica.shard_index_for(key, config.num_shards)
    {db, get_stmt} = read_conn(name, shard_index)

    case EKV.Store.get_cached(db, get_stmt, key) do
      nil ->
        nil

      {_value_binary, _ts, _origin, _expires_at, deleted_at} when is_integer(deleted_at) ->
        nil

      {value_binary, ts, origin, expires_at, nil} when is_integer(expires_at) ->
        now = System.system_time(:nanosecond)

        if expires_at <= now do
          nil
        else
          {:erlang.binary_to_term(value_binary), {ts, origin}}
        end

      {value_binary, ts, origin, _expires_at, nil} ->
        {:erlang.binary_to_term(value_binary), {ts, origin}}
    end
  end

  @doc """
  Put a key-value pair.

  ## Options

  - `:ttl` — positive integer time-to-live in milliseconds. Entry expires
    after this duration.
  - `:if_vsn` — compare-and-swap. Only succeeds if the key's current version
    matches. Use `nil` for insert-if-absent.
    Returns `{:error, :conflict}` if the expected version does not match and
    the write was not applied.
    Returns `{:error, :unconfirmed}` when the write entered accept phase but the
    caller could not confirm final outcome; in this case, issue
    `get(name, key, consistent: true)` to resolve the committed value.
    Requires `cluster_size` and `node_id` config.
    Can be used on an existing LWW key to transition it to CAS-managed
    (`LWW -> CAS`).
  - `:consistent` — when `true`, uses CASPaxos consensus for the write
    (quorum-backed CAS write). Mutually exclusive with `:if_vsn`. Requires
    `cluster_size` and `node_id` config.
    For strict behavior, keep the key in CAS mode after transition and do not
    mix with eventual `put`/`delete` on the same key (`CAS -> LWW` writes are
    rejected).
    Eventual writes to CAS-managed keys are rejected with
    `{:error, :cas_managed_key}`.
  - `:retries` — non-negative integer max CAS retries on conflict (default 5).
    Only for `:consistent` and `:if_vsn` paths.
  - `:backoff` — `{min_ms, max_ms}` backoff range in ms where both are
    non-negative integers and `min_ms <= max_ms` (default `{10, 60}`).
  - `:timeout` — positive integer call timeout in ms (or `:infinity`, default
    10_000).
  - `:resolve_unconfirmed` — boolean (default `false`). When `true`, if a CAS
    write enters accept phase but cannot confirm final outcome, EKV performs one
    internal barrier read to resolve current state and returns
    `{:ok, vsn}`/`{:error, :conflict}` when possible, or
    `{:error, :unavailable}` if that resolution cannot complete.

  ## Returns

  - Eventual put (`put` without CAS options): `:ok` or
    `{:error, :cas_managed_key}` when the key is CAS-managed
  - CAS put (`if_vsn:` or `consistent: true`): `{:ok, vsn}` where
    `vsn` is `{timestamp, origin_node}`
  - With `resolve_unconfirmed: true`, CAS put may also return
    `{:error, :unavailable}` if ambiguity resolution cannot complete.
  """
  def put(name, key, value, opts \\ []) do
    opts =
      Keyword.validate!(opts, [
        :ttl,
        :if_vsn,
        :consistent,
        :retries,
        :backoff,
        :timeout,
        :resolve_unconfirmed
      ])

    validate_ttl_opt!(opts)
    validate_retries_opt!(opts)
    validate_backoff_opt!(opts)
    validate_timeout_opt!(opts)
    consistent? = validate_boolean_opt!(opts, :consistent)
    _resolve_unconfirmed? = validate_boolean_opt!(opts, :resolve_unconfirmed)
    config = get_config(name)
    shard_index = Replica.shard_index_for(key, config.num_shards)
    timeout = Keyword.get(opts, :timeout, 10_000)

    case {Keyword.fetch(opts, :if_vsn), consistent?} do
      {_, true} ->
        if Keyword.has_key?(opts, :if_vsn) do
          raise ArgumentError, "EKV: :consistent and :if_vsn are mutually exclusive"
        end

        validate_cas_config!(config)

        update_opts =
          Keyword.take(opts, [:ttl, :retries, :backoff, :timeout, :resolve_unconfirmed])

        case update(name, key, fn _ -> value end, update_opts) do
          {:ok, _new_value, vsn} -> {:ok, vsn}
          error -> error
        end

      {{:ok, expected_vsn}, false} ->
        validate_cas_config!(config)
        value_binary = :erlang.term_to_binary(value)

        GenServer.call(
          Replica.shard_name(name, shard_index),
          {:cas_put, key, value_binary, expected_vsn, opts},
          timeout
        )

      {:error, false} ->
        value_binary = :erlang.term_to_binary(value)
        GenServer.call(Replica.shard_name(name, shard_index), {:put, key, value_binary, opts})
    end
  end

  @doc """
  Get a value by key. Returns `nil` for missing, expired, or deleted entries.

  By default, reads directly from SQLite via per-scheduler read connection
  (eventually consistent, no GenServer hop).

  ## Options

  - `:consistent` — when `true`, performs a CASPaxos consensus read
    (barrier/linearizable for CAS-managed keys). This read always goes through
    CAS accept+commit to resolve any in-flight accepted value before replying.
    Requires `cluster_size` and `node_id` config.
  - `:retries` — non-negative integer max CAS retries (default 5). Only for
    `consistent: true`.
  - `:backoff` — `{min_ms, max_ms}` backoff range in ms where both are
    non-negative integers and `min_ms <= max_ms` (default `{10, 60}`).
  - `:timeout` — positive integer call timeout in ms (or `:infinity`, default
    10_000).
  """
  def get(name, key, opts \\ []) do
    opts = Keyword.validate!(opts, [:consistent, :retries, :backoff, :timeout])
    validate_retries_opt!(opts)
    validate_backoff_opt!(opts)
    validate_timeout_opt!(opts)
    consistent? = validate_boolean_opt!(opts, :consistent)

    if consistent? do
      config = get_config(name)
      validate_cas_config!(config)
      shard_index = Replica.shard_index_for(key, config.num_shards)
      timeout = Keyword.get(opts, :timeout, 10_000)
      cas_opts = Keyword.take(opts, [:retries, :backoff])

      case GenServer.call(
             Replica.shard_name(name, shard_index),
             {:cas_read, key, cas_opts},
             timeout
           ) do
        {:ok, value} -> value
        {:ok, value, _vsn} -> value
        {:error, reason} -> raise "EKV: consistent read failed: #{inspect(reason)}"
      end
    else
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
  end

  @doc """
  Delete a key.

  Writes a tombstone that replicates to all peers.

  ## Options

  - `:if_vsn` — compare-and-swap delete. Only succeeds if the key's current
    version matches.
    Returns `{:error, :conflict}` if the expected version does not match and
    the delete was not applied.
    Returns `{:error, :unconfirmed}` when the delete entered accept phase but the
    caller could not confirm final outcome; issue `get(name, key, consistent: true)`
    to resolve.
    Requires `cluster_size` and `node_id` config.
    For strict behavior, keep the key in CAS mode after transition and do not
    mix with eventual `put`/`delete` on the same key (`CAS -> LWW` writes are
    rejected).
    Eventual deletes on CAS-managed keys are rejected with
    `{:error, :cas_managed_key}`.
  - `:resolve_unconfirmed` — boolean (default `false`). When `true`, if a CAS
    delete enters accept phase but cannot confirm final outcome, EKV performs
    one internal barrier read to resolve current state and returns
    `{:ok, vsn}`/`{:error, :conflict}` when possible, or
    `{:error, :unavailable}` if that resolution cannot complete.

  ## Returns

  - Eventual delete (`delete` without CAS options): `:ok` or
    `{:error, :cas_managed_key}` when the key is CAS-managed
  - CAS delete (`if_vsn:`): `{:ok, vsn}` where `vsn` is `{timestamp, origin_node}`
  - With `resolve_unconfirmed: true`, CAS delete may also return
    `{:error, :unavailable}` if ambiguity resolution cannot complete.
  """
  def delete(name, key, opts \\ []) do
    opts = Keyword.validate!(opts, [:if_vsn, :timeout, :resolve_unconfirmed])
    validate_timeout_opt!(opts)
    _resolve_unconfirmed? = validate_boolean_opt!(opts, :resolve_unconfirmed)
    config = get_config(name)
    shard_index = Replica.shard_index_for(key, config.num_shards)
    timeout = Keyword.get(opts, :timeout, 10_000)

    case Keyword.fetch(opts, :if_vsn) do
      {:ok, expected_vsn} ->
        validate_cas_config!(config)

        GenServer.call(
          Replica.shard_name(name, shard_index),
          {:cas_delete, key, expected_vsn, opts},
          timeout
        )

      :error ->
        GenServer.call(Replica.shard_name(name, shard_index), {:delete, key})
    end
  end

  @doc """
  Atomic read-modify-write.

  Reads the current value, applies `fun`, and writes the result using CASPaxos.
  Auto-retries on conflict (up to 5 times with random backoff).

  Returns `{:ok, new_value, vsn}` on success where
  `vsn` is `{timestamp, origin_node}` for the committed value.
  Returns `{:error, :conflict}` when retries are exhausted before entering an
  accept phase that could decide the write.
  Returns `{:error, :unconfirmed}` when accept phase started but the caller could
  not confirm final outcome; issue `get(name, key, consistent: true)` to
  resolve.

  Requires `cluster_size` and `node_id` config.
  Can be used to move a key from LWW to CAS-managed mode. After transition,
  do not mix with eventual writes on the same key (`CAS -> LWW` writes are
  rejected).

  ## Options

  - `:ttl` — positive integer time-to-live in milliseconds for the new value.
  - `:retries` — non-negative integer max CAS retries on conflict (default 5).
  - `:backoff` — `{min_ms, max_ms}` backoff range in ms where both are
    non-negative integers and `min_ms <= max_ms` (default `{10, 60}`).
  - `:timeout` — positive integer call timeout in ms (or `:infinity`, default
    10_000).
  - `:resolve_unconfirmed` — boolean (default `false`). When `true`, if an
    update enters accept phase but cannot confirm final outcome, EKV performs
    one internal barrier read to resolve current state and returns
    `{:ok, new_value, vsn}`/`{:error, :conflict}` when possible, or
    `{:error, :unavailable}` if that resolution cannot complete.
  """
  def update(name, key, fun, opts \\ []) when is_function(fun, 1) do
    opts = Keyword.validate!(opts, [:ttl, :retries, :backoff, :timeout, :resolve_unconfirmed])
    validate_ttl_opt!(opts)
    validate_retries_opt!(opts)
    validate_backoff_opt!(opts)
    validate_timeout_opt!(opts)
    _resolve_unconfirmed? = validate_boolean_opt!(opts, :resolve_unconfirmed)
    config = get_config(name)
    validate_cas_config!(config)
    shard_index = Replica.shard_index_for(key, config.num_shards)
    timeout = Keyword.get(opts, :timeout, 10_000)

    GenServer.call(
      Replica.shard_name(name, shard_index),
      {:update, key, fun, opts},
      timeout
    )
  end

  @scan_chunk_size 500

  @scan_first_chunk_sql """
  SELECT key, value, timestamp, origin_node FROM kv
  WHERE key >= ?1 AND key < ?2
    AND (deleted_at IS NULL OR deleted_at > ?3)
    AND (expires_at IS NULL OR expires_at > ?3)
  ORDER BY key LIMIT ?4
  """

  @scan_next_chunk_sql """
  SELECT key, value, timestamp, origin_node FROM kv
  WHERE key > ?1 AND key < ?2
    AND (deleted_at IS NULL OR deleted_at > ?3)
    AND (expires_at IS NULL OR expires_at > ?3)
  ORDER BY key LIMIT ?4
  """

  @keys_first_chunk_sql """
  SELECT key, timestamp, origin_node FROM kv
  WHERE key >= ?1 AND key < ?2
    AND (deleted_at IS NULL OR deleted_at > ?3)
    AND (expires_at IS NULL OR expires_at > ?3)
  ORDER BY key LIMIT ?4
  """

  @keys_next_chunk_sql """
  SELECT key, timestamp, origin_node FROM kv
  WHERE key > ?1 AND key < ?2
    AND (deleted_at IS NULL OR deleted_at > ?3)
    AND (expires_at IS NULL OR expires_at > ?3)
  ORDER BY key LIMIT ?4
  """

  @doc """
  Scan key-value pairs matching a prefix.

  Scans all shards using cursor-based streaming. Returns a `Stream` of
  `{key, value, vsn}` tuples where `vsn` is `{timestamp, origin_node}`.

  Results are sorted by key within each shard but not globally sorted
  across shards.
  """
  def scan(name, prefix) do
    config = get_config(name)
    prefix_end = EKV.Store.next_binary_prefix(prefix)

    shard_streams =
      for shard <- 0..(config.num_shards - 1) do
        Stream.resource(
          fn -> {prefix, :first} end,
          fn
            :done ->
              {:halt, :done}

            {cursor, phase} ->
              now = System.system_time(:nanosecond)
              {db, _} = read_conn(name, shard)

              {sql, args} =
                case phase do
                  :first ->
                    {@scan_first_chunk_sql, [cursor, prefix_end, now, @scan_chunk_size]}

                  :next ->
                    {@scan_next_chunk_sql, [cursor, prefix_end, now, @scan_chunk_size]}
                end

              {:ok, rows} = EKV.Sqlite3.fetch_all(db, sql, args)

              if rows == [] do
                {:halt, :done}
              else
                items =
                  Enum.map(rows, fn [key, value_binary, ts, origin_str] ->
                    {key, :erlang.binary_to_term(value_binary), {ts, String.to_atom(origin_str)}}
                  end)

                if length(rows) < @scan_chunk_size do
                  {items, :done}
                else
                  [last_key | _] = List.last(rows)
                  {items, {last_key, :next}}
                end
              end
          end,
          fn _ -> :ok end
        )
      end

    Stream.concat(shard_streams)
  end

  @doc """
  List keys (with versions) matching a prefix. Scans all shards using
  cursor-based streaming.

  Returns a `Stream` of `{key, vsn}` tuples where `vsn` is
  `{timestamp, origin_node}`.

  This avoids decoding values while still allowing CAS workflows to chain
  `if_vsn:` operations from scan output.

  Results are sorted by key within each shard but not globally sorted across
  shards.
  """
  def keys(name, prefix) do
    config = get_config(name)
    prefix_end = EKV.Store.next_binary_prefix(prefix)

    shard_streams =
      for shard <- 0..(config.num_shards - 1) do
        Stream.resource(
          fn -> {prefix, :first} end,
          fn
            :done ->
              {:halt, :done}

            {cursor, phase} ->
              now = System.system_time(:nanosecond)
              {db, _} = read_conn(name, shard)

              {sql, args} =
                case phase do
                  :first ->
                    {@keys_first_chunk_sql, [cursor, prefix_end, now, @scan_chunk_size]}

                  :next ->
                    {@keys_next_chunk_sql, [cursor, prefix_end, now, @scan_chunk_size]}
                end

              {:ok, rows} = EKV.Sqlite3.fetch_all(db, sql, args)

              if rows == [] do
                {:halt, :done}
              else
                items =
                  Enum.map(rows, fn [key, ts, origin_str] ->
                    {key, {ts, String.to_atom(origin_str)}}
                  end)

                if length(rows) < @scan_chunk_size do
                  {items, :done}
                else
                  [last_key | _] = List.last(rows)
                  {items, {last_key, :next}}
                end
              end
          end,
          fn _ -> :ok end
        )
      end

    Stream.concat(shard_streams)
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

  @doc """
  Create a backup of all shards to `dest_dir`.

  Uses SQLite's online backup API — safe to call while EKV is running.
  Returns `:ok` on success or `{:error, reason}` on failure.
  """
  def backup(name, dest_dir) do
    config = get_config(name)
    File.mkdir_p!(dest_dir)

    0..(config.num_shards - 1)
    |> Task.async_stream(
      fn shard -> EKV.Store.backup_shard(config.data_dir, dest_dir, shard) end,
      ordered: false
    )
    |> Enum.reduce(:ok, fn
      {:ok, :ok}, :ok -> :ok
      {:ok, {:error, _} = err}, _ -> err
      _, acc -> acc
    end)
  end

  @doc """
  Return cluster status information.

  Returns a map with node_id, cluster_size, shards, data_dir, and connected peers.
  """
  def info(name) do
    config = get_config(name)
    shard_state = :sys.get_state(Replica.shard_name(name, 0))

    peers =
      for {node, _pid} <- shard_state.remote_shards do
        %{node: node, node_id: Map.get(shard_state.peer_node_ids, node)}
      end

    %{
      name: name,
      node_id: config.node_id,
      cluster_size: config.cluster_size,
      shards: config.num_shards,
      data_dir: config.data_dir,
      connected_peers: peers
    }
  end

  def get_config(name) do
    :persistent_term.get({EKV, name})
  end

  defp validate_cas_config!(%{cluster_size: nil}) do
    raise ArgumentError,
          "EKV: CAS operations require :cluster_size and :node_id to be configured"
  end

  defp validate_cas_config!(_config), do: :ok

  defp validate_boolean_opt!(opts, key) do
    case Keyword.get(opts, key, false) do
      value when is_boolean(value) ->
        value

      other ->
        raise ArgumentError, "EKV: #{inspect(key)} must be boolean, got: #{inspect(other)}"
    end
  end

  defp validate_ttl_opt!(opts) do
    case Keyword.fetch(opts, :ttl) do
      :error ->
        :ok

      {:ok, ttl} when is_integer(ttl) and ttl > 0 ->
        :ok

      {:ok, other} ->
        raise ArgumentError, "EKV: :ttl must be a positive integer, got: #{inspect(other)}"
    end
  end

  defp validate_retries_opt!(opts) do
    case Keyword.fetch(opts, :retries) do
      :error ->
        :ok

      {:ok, retries} when is_integer(retries) and retries >= 0 ->
        :ok

      {:ok, other} ->
        raise ArgumentError,
              "EKV: :retries must be a non-negative integer, got: #{inspect(other)}"
    end
  end

  defp validate_backoff_opt!(opts) do
    case Keyword.fetch(opts, :backoff) do
      :error ->
        :ok

      {:ok, {min_ms, max_ms}}
      when is_integer(min_ms) and is_integer(max_ms) and min_ms >= 0 and max_ms >= min_ms ->
        :ok

      {:ok, other} ->
        raise ArgumentError,
              "EKV: :backoff must be {min_ms, max_ms} with non-negative integers and min <= max, got: #{inspect(other)}"
    end
  end

  defp validate_timeout_opt!(opts) do
    case Keyword.fetch(opts, :timeout) do
      :error ->
        :ok

      {:ok, :infinity} ->
        :ok

      {:ok, timeout} when is_integer(timeout) and timeout > 0 ->
        :ok

      {:ok, other} ->
        raise ArgumentError,
              "EKV: :timeout must be a positive integer or :infinity, got: #{inspect(other)}"
    end
  end

  defp read_conn(name, shard_index) do
    readers = :persistent_term.get({EKV, name, :readers, shard_index})
    sid = :erlang.system_info(:scheduler_id)
    elem(readers, rem(sid - 1, tuple_size(readers)))
  end
end
