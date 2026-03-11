defmodule EKV.Supervisor do
  @moduledoc false
  use Supervisor

  require Logger

  _archdoc = ~S"""
  Top-level EKV supervisor.

  Builds config map and stores it in `:persistent_term`.
  Each EKV instance also owns its own `:pg` scope (`:"#{name}_ekv_pg_scope"`),
  so subscriptions, member routing, and shutdown coordination stay isolated
  from other EKV instances and from unrelated default-scope `:pg` traffic.

  `EKV.Supervisor` is also where runtime mode splits happen:

  - `:member` mode starts durable storage/replication/CAS children
  - `:client` mode starts only routing/subscription/readiness children

  Member mode child order:

      :pg scope
      BlueGreenMarker?
      SubTracker
      Registry
      SubDispatcher.Supervisor
      Replica.Supervisor
      QuorumGate?
      MemberPresence
      GC
      ShutdownBarrier?

  Client mode child order:

      :pg scope
      ClientRouter
      RouteGate?
      QuorumGate?
      ClientSubscriptions
      ShutdownBarrier?

  This ordering matters:

  - `RouteGate` / `QuorumGate` block startup before the instance is considered
    ready
  - `MemberPresence` is started only after member readiness, so new clients do
    not route to an unready member
  - `ShutdownBarrier` is last so supervisor shutdown reaches it first while the
    rest of EKV is still alive

  ## Startup Safety

  Member mode opens shard databases fail-closed by default. If on-disk state is
  older than the tombstone safety window, startup is rejected unless
  `allow_stale_startup: true` is explicitly set.

  Optional startup gates:

  - `wait_for_quorum` — member mode, and client mode via the selected backend
  - `wait_for_route` — client mode only

  These are readiness aids only. They do not guarantee the route or quorum will
  remain available after startup completes.

  ## Blue-Green Deployment (Synchronized Handoff)

  When `blue_green: true`, the supervisor performs a synchronized handoff
  with the old VM before starting children. Both VMs share the same
  `data_dir` and `node_id` — from the cluster's perspective, it's a single
  member that briefly restarted.

  ### Handoff Protocol

      New VM (m1b)                          Old VM (m1a)
      ────────────                          ────────────
      Supervisor.init
        read_marker(data_dir)
        => discovers m1a has running EKV

        perform_handoff (parallel per shard):
          send {:ekv_handoff_request}  ──>  Replica receives request
                                              1. Fail all pending_cas
                                              2. Persist ballot_counter
                                              3. PRAGMA wal_checkpoint(TRUNCATE)
                                              4. Close writer db
          receive {:ekv_handoff_ack}   <──    5. Send ack
                                              6. Leave MemberPresence
                                                 route + enter proxy mode

        Start children:
          Replica.init opens SAME db files
          MemberPresence joins :pg             Proxy calls to m1b
          Members discover via nodeup          Readers alive until shutdown
          CAS works immediately                New clients bind to m1b

  ### Marker File

  The `current` marker is a single line: `"node_name\n"`. Written atomically
  via write-to-tmp + `File.rename!/2`. Read on every startup to discover the
  old node for handoff. On graceful shutdown without a completed handoff,
  `BlueGreenMarker` clears the old VM's marker.

  ### Handoff Resolution (`perform_handoff/3`)

  1. **No marker** → first boot. Write marker, proceed normally.

  2. **Marker node == `node()`** → same-VM restart. No handoff needed.

  3. **Marker node != `node()`** → potential blue-green deploy. If the marker
     node is reachable now, send handoff request to old VM's shards in
     parallel (5s timeout per shard). If the marker node is unreachable, treat
     the marker as stale and proceed without handoff.

  ### Key Invariants

  - Single writer: old writer closed before new writer opens
  - Ballot monotonicity: persisted before close, max(now, persisted+1) on init
  - Quorum safety: same node_id → count_alive_node_ids counts as 1
  - kv_paxos durable: promise/accept state survives in SQLite
  - No data loss: WAL checkpoint flushes everything before close
  - Client routing safety: incoming member is advertised only after startup;
    outgoing member leaves its :pg route before proxy mode
  - Shutdown safety: graceful coordinated shutdown keeps members serving until
    other members have also entered terminal state, or timeout; outgoing
    proxy-mode blue-green members skip this wait

  ## Graceful Shutdown Barrier

  When `shutdown_barrier: timeout_ms` is enabled, EKV adds a final
  `EKV.ShutdownBarrier` child at the bottom of the tree. Supervisor shutdown
  happens in reverse child order, so the barrier blocks first while replicas,
  routing, and subscriptions are still alive.

  The barrier publishes shutdown state through `:pg`:

  - `{:ekv_shutdown_live, name}` — all live EKV instances
  - `{:ekv_shutdown_terminal, name}` — EKV instances currently terminating
  - `{:ekv_shutdown_live_member, name, node_id}` — live logical voting members
  - `{:ekv_shutdown_terminal_member, name, node_id}` — terminating logical members

  Member mode waits only when it is useful:

  - coordinated shutdown is already in progress, or
  - exiting now would drop live logical members below quorum while other
    snapshotted members are still non-terminal

  Client mode participates in terminal coordination, but it does not count
  toward quorum. Forced exits (`kill -9`, VM crash) bypass the barrier.
  """

  @valid_opts [
    :name,
    :mode,
    :region,
    :region_routing,
    :data_dir,
    :shards,
    :log,
    :tombstone_ttl,
    :gc_interval,
    :blue_green,
    :cluster_size,
    :node_id,
    :sync_chunk_size,
    :allow_stale_startup,
    :partition_ttl_policy,
    :wire_compression_threshold,
    :wait_for_quorum,
    :wait_for_route,
    :shutdown_barrier
  ]

  def start_link(opts) do
    opts = Keyword.validate!(opts, @valid_opts)
    name = Keyword.fetch!(opts, :name)
    Supervisor.start_link(__MODULE__, opts, name: :"#{name}_ekv_sup")
  end

  @doc false
  def get_config(name) do
    :persistent_term.get({EKV, name})
  end

  def pg_scope(name), do: :"#{name}_ekv_pg_scope"

  @doc false
  def client_sub_group(name, prefix), do: {:ekv_sub, name, prefix}

  @doc false
  def client_any_sub_group(name), do: {:ekv_sub_any, name}

  @doc false
  def client_subscribers?(name) do
    case :pg.get_members(pg_scope(name), client_any_sub_group(name)) do
      [] -> false
      members when is_list(members) -> true
    end
  rescue
    _ -> false
  end

  def pg_scope_child(name) do
    scope = pg_scope(name)
    %{id: scope, start: {:pg, :start_link, [scope]}}
  end

  @impl true
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    mode = Keyword.get(opts, :mode, :member)
    region = Keyword.get(opts, :region, "default")
    log = Keyword.get(opts, :log, :info)
    validate_mode!(mode)
    validate_region!(region, :region)

    case mode do
      :member ->
        init_member(name, region, log, opts)

      :client ->
        init_client(name, region, log, opts)
    end
  end

  defp init_member(name, region, log, opts) do
    data_dir = Keyword.fetch!(opts, :data_dir)
    num_shards = Keyword.get(opts, :shards, 8)
    blue_green = Keyword.get(opts, :blue_green, false)
    tombstone_ttl = Keyword.get(opts, :tombstone_ttl, :timer.hours(24 * 7))
    gc_interval = Keyword.get(opts, :gc_interval, :timer.minutes(5))
    cluster_size = Keyword.get(opts, :cluster_size)
    node_id = Keyword.get(opts, :node_id)
    sync_chunk_size = Keyword.get(opts, :sync_chunk_size, 500)
    allow_stale_startup = Keyword.get(opts, :allow_stale_startup, false)
    partition_ttl_policy = Keyword.get(opts, :partition_ttl_policy, :quarantine)
    wire_compression_threshold = Keyword.get(opts, :wire_compression_threshold, 256 * 1024)
    wait_for_quorum = Keyword.get(opts, :wait_for_quorum, false)
    wait_for_route = Keyword.get(opts, :wait_for_route, false)
    shutdown_barrier = Keyword.get(opts, :shutdown_barrier, false)

    validate_cas_config!(cluster_size, node_id)
    validate_partition_ttl_policy!(partition_ttl_policy)
    validate_wire_compression_threshold!(wire_compression_threshold)
    validate_wait_for_quorum!(wait_for_quorum, cluster_size)
    validate_wait_for_route!(wait_for_route, :member)
    validate_shutdown_barrier!(shutdown_barrier)
    validate_allow_stale_startup!(allow_stale_startup)

    node_id =
      case node_id do
        nil -> nil
        id when is_integer(id) -> Integer.to_string(id)
        id when is_binary(id) -> id
      end

    if blue_green, do: perform_handoff(name, data_dir, num_shards)

    effective_node_id =
      if cluster_size do
        persisted = EKV.Store.read_node_id(data_dir)

        cond do
          persisted != nil and node_id != nil and persisted != node_id ->
            Logger.warning(
              "[EKV #{name}] configured node_id #{inspect(node_id)} differs from " <>
                "persisted #{inspect(persisted)} (volume identity) — using persisted"
            )

            persisted

          persisted != nil ->
            persisted

          node_id != nil ->
            node_id

          true ->
            generated = :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower)
            Logger.info("[EKV #{name}] generated node_id: #{generated}")
            generated
        end
      else
        nil
      end

    registry_name = :"#{name}_ekv_registry"
    sub_tracker_name = :"#{name}_ekv_sub_tracker"
    sub_count = :atomics.new(1, signed: true)

    config = %{
      mode: :member,
      region: region,
      num_shards: num_shards,
      data_dir: data_dir,
      log: log,
      tombstone_ttl: tombstone_ttl,
      gc_interval: gc_interval,
      registry: registry_name,
      sub_count: sub_count,
      cluster_size: cluster_size,
      node_id: effective_node_id,
      sync_chunk_size: sync_chunk_size,
      allow_stale_startup: allow_stale_startup,
      partition_ttl_policy: partition_ttl_policy,
      wire_compression_threshold: wire_compression_threshold
    }

    :persistent_term.put({EKV, name}, config)

    children =
      [
        pg_scope_child(name),
        if(blue_green, do: {EKV.BlueGreenMarker, name: name, data_dir: data_dir, log: log}),
        {EKV.SubTracker, name: sub_tracker_name, sub_count: sub_count},
        {Registry, keys: :duplicate, name: registry_name, listeners: [sub_tracker_name]},
        {EKV.SubDispatcher.Supervisor, name: name, num_shards: num_shards},
        {EKV.Replica.Supervisor, name: name, num_shards: num_shards, data_dir: data_dir},
        if(is_integer(wait_for_quorum),
          do: {EKV.QuorumGate, name: name, timeout: wait_for_quorum, log: log}
        ),
        {EKV.MemberPresence, name: name, region: region},
        {EKV.GC, name: name},
        if(is_integer(shutdown_barrier),
          do:
            Supervisor.child_spec(
              {EKV.ShutdownBarrier, name: name, timeout: shutdown_barrier, log: log},
              shutdown: shutdown_barrier + 1_000
            )
        )
      ]
      |> Enum.filter(& &1)

    Supervisor.init(children, strategy: :rest_for_one)
  end

  defp init_client(name, region, log, opts) do
    region_routing = Keyword.get(opts, :region_routing)
    wait_for_route = Keyword.get(opts, :wait_for_route, false)
    wait_for_quorum = Keyword.get(opts, :wait_for_quorum, false)
    shutdown_barrier = Keyword.get(opts, :shutdown_barrier, false)
    wire_compression_threshold = Keyword.get(opts, :wire_compression_threshold, 256 * 1024)

    validate_client_opts!(
      opts,
      region_routing,
      wait_for_route,
      wait_for_quorum,
      shutdown_barrier,
      wire_compression_threshold
    )

    config = %{
      mode: :client,
      region: region,
      region_routing: region_routing,
      log: log,
      wire_compression_threshold: wire_compression_threshold,
      cluster_size: nil,
      node_id: nil,
      num_shards: nil,
      data_dir: nil
    }

    :persistent_term.put({EKV, name}, config)

    children =
      [
        pg_scope_child(name),
        {EKV.ClientRouter, name: name},
        if(is_integer(wait_for_route),
          do: {EKV.RouteGate, name: name, timeout: wait_for_route, log: log}
        ),
        if(is_integer(wait_for_quorum),
          do: {EKV.QuorumGate, name: name, timeout: wait_for_quorum, log: log}
        ),
        {EKV.ClientSubscriptions, name: name},
        if(is_integer(shutdown_barrier),
          do:
            Supervisor.child_spec(
              {EKV.ShutdownBarrier, name: name, timeout: shutdown_barrier, log: log},
              shutdown: shutdown_barrier + 1_000
            )
        )
      ]
      |> Enum.filter(& &1)

    Supervisor.init(children, strategy: :rest_for_one)
  end

  # =====================================================================
  # CAS config validation
  # =====================================================================

  defp validate_cas_config!(nil, _node_id), do: :ok

  defp validate_cas_config!(cluster_size, node_id) do
    unless is_integer(cluster_size) and cluster_size >= 1 do
      raise ArgumentError,
            "EKV: :cluster_size must be a positive integer, got: #{inspect(cluster_size)}"
    end

    # node_id is optional (auto-generated if nil)
    # If provided, must be a string or positive integer (converted to string)
    case node_id do
      nil ->
        :ok

      id when is_binary(id) and byte_size(id) > 0 ->
        :ok

      id when is_integer(id) and id >= 1 ->
        :ok

      _ ->
        raise ArgumentError,
              "EKV: :node_id must be a non-empty string or positive integer, got: #{inspect(node_id)}"
    end
  end

  defp validate_partition_ttl_policy!(policy)
       when policy in [:quarantine, :ignore],
       do: :ok

  defp validate_partition_ttl_policy!(policy) do
    raise ArgumentError,
          "EKV: :partition_ttl_policy must be :quarantine or :ignore, got: #{inspect(policy)}"
  end

  defp validate_wait_for_quorum!(false, _cluster_size), do: :ok
  defp validate_wait_for_quorum!(nil, _cluster_size), do: :ok

  defp validate_wait_for_quorum!(_timeout, nil) do
    raise ArgumentError,
          "EKV: :wait_for_quorum requires CAS configuration (:cluster_size and :node_id)"
  end

  defp validate_wait_for_quorum!(timeout, _cluster_size)
       when is_integer(timeout) and timeout >= 0,
       do: :ok

  defp validate_wait_for_quorum!(timeout, _cluster_size) do
    raise ArgumentError,
          "EKV: :wait_for_quorum must be false/nil or a non-negative timeout in ms, got: #{inspect(timeout)}"
  end

  defp validate_wait_for_route!(false, _mode), do: :ok
  defp validate_wait_for_route!(nil, _mode), do: :ok

  defp validate_wait_for_route!(timeout, _mode)
       when is_integer(timeout) and timeout >= 0,
       do: :ok

  defp validate_wait_for_route!(timeout, _mode) do
    raise ArgumentError,
          "EKV: :wait_for_route must be false/nil or a non-negative timeout in ms, got: #{inspect(timeout)}"
  end

  defp validate_shutdown_barrier!(false), do: :ok
  defp validate_shutdown_barrier!(nil), do: :ok

  defp validate_shutdown_barrier!(timeout)
       when is_integer(timeout) and timeout >= 0,
       do: :ok

  defp validate_shutdown_barrier!(timeout) do
    raise ArgumentError,
          "EKV: :shutdown_barrier must be false/nil or a non-negative timeout in ms, got: #{inspect(timeout)}"
  end

  defp validate_mode!(mode) when mode in [:member, :client], do: :ok

  defp validate_mode!(mode) do
    raise ArgumentError, "EKV: :mode must be :member or :client, got: #{inspect(mode)}"
  end

  defp validate_region!(region, _key) when is_binary(region) and byte_size(region) > 0, do: :ok

  defp validate_region!(region, key) do
    raise ArgumentError,
          "EKV: #{inspect(key)} must be a non-empty string, got: #{inspect(region)}"
  end

  defp validate_client_opts!(
         opts,
         region_routing,
         wait_for_route,
         wait_for_quorum,
         shutdown_barrier,
         wire_compression_threshold
       ) do
    validate_region_routing!(region_routing)
    validate_wait_for_route!(wait_for_route, :client)
    validate_wait_for_quorum!(wait_for_quorum, 1)
    validate_shutdown_barrier!(shutdown_barrier)
    validate_wire_compression_threshold!(wire_compression_threshold)

    reject_client_opt!(opts, :blue_green, [false, nil])
    reject_client_opt!(opts, :data_dir, [nil])
    reject_client_opt!(opts, :cluster_size, [nil])
    reject_client_opt!(opts, :node_id, [nil])
    reject_client_opt!(opts, :shards, [nil])
    reject_client_opt!(opts, :gc_interval, [nil])
    reject_client_opt!(opts, :tombstone_ttl, [nil])
    reject_client_opt!(opts, :sync_chunk_size, [nil])
    reject_client_opt!(opts, :allow_stale_startup, [nil, false])
    reject_client_opt!(opts, :partition_ttl_policy, [nil])
  end

  defp validate_wire_compression_threshold!(false), do: :ok
  defp validate_wire_compression_threshold!(nil), do: :ok

  defp validate_wire_compression_threshold!(threshold)
       when is_integer(threshold) and threshold >= 0,
       do: :ok

  defp validate_wire_compression_threshold!(threshold) do
    raise ArgumentError,
          "EKV: :wire_compression_threshold must be false/nil or a non-negative byte threshold, got: #{inspect(threshold)}"
  end

  defp validate_allow_stale_startup!(value) when is_boolean(value), do: :ok
  defp validate_allow_stale_startup!(nil), do: :ok

  defp validate_allow_stale_startup!(value) do
    raise ArgumentError,
          "EKV: :allow_stale_startup must be boolean, got: #{inspect(value)}"
  end

  defp validate_region_routing!(region_routing)
       when is_list(region_routing) and region_routing != [] do
    Enum.each(region_routing, &validate_region!(&1, :region_routing))
  end

  defp validate_region_routing!(region_routing) do
    raise ArgumentError,
          "EKV: :region_routing must be a non-empty list of non-empty strings, got: #{inspect(region_routing)}"
  end

  defp reject_client_opt!(opts, key, allowed_values) do
    value = Keyword.get(opts, key)

    unless value in allowed_values do
      raise ArgumentError,
            "EKV: #{inspect(key)} is not supported in :client mode, got: #{inspect(value)}"
    end
  end

  # =====================================================================
  # Blue-green handoff
  # =====================================================================

  defp perform_handoff(name, data_dir, num_shards) do
    File.mkdir_p!(data_dir)
    old_node_str = read_marker(data_dir)

    cond do
      # No marker — first boot
      old_node_str == nil ->
        write_marker(data_dir, node())

      # Same node restart — no handoff needed
      old_node_str == Atom.to_string(node()) ->
        :ok

      # Different node — attempt handoff
      true ->
        old_node = String.to_atom(old_node_str)

        if handoff_reachable?(old_node) do
          Logger.info("[EKV #{name}] handoff: requesting from #{old_node}")

          0..(num_shards - 1)
          |> Task.async_stream(
            fn shard_index ->
              shard_name = EKV.Replica.shard_name(name, shard_index)
              ref = make_ref()
              send({shard_name, old_node}, {:ekv_handoff_request, ref, node(), self()})

              receive do
                {:ekv_handoff_ack, ^ref} -> :ok
              after
                5_000 -> :timeout
              end
            end,
            ordered: false,
            timeout: 10_000
          )
          |> Stream.run()
        else
          Logger.info("[EKV #{name}] handoff: stale marker #{old_node}, skipping handoff")
        end

        # Update marker to current node
        write_marker(data_dir, node())
    end
  end

  defp handoff_reachable?(old_node) do
    old_node in Node.list() or Node.connect(old_node)
  end

  defp read_marker(data_dir) do
    case File.read(Path.join(data_dir, "current")) do
      {:ok, contents} ->
        trimmed = String.trim(contents)
        if trimmed == "", do: nil, else: trimmed

      {:error, _} ->
        nil
    end
  end

  defp write_marker(data_dir, node_name) do
    marker_path = Path.join(data_dir, "current")
    tmp_path = marker_path <> ".tmp"
    File.write!(tmp_path, "#{node_name}\n")
    File.rename!(tmp_path, marker_path)
  end
end
