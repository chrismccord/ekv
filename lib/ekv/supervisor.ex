defmodule EKV.Supervisor do
  @moduledoc false
  use Supervisor

  _archdoc = ~S"""
  Top-level supervisor. Builds config map and stores it in `persistent_term`.

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
          receive {:ekv_handoff_ack}   <──  5. Send ack
                                              6. Enter proxy mode

        Start children:
          Replica.init opens SAME db files
          Peers discover via nodeup            Proxy calls to m1b
          CAS works immediately                Readers alive until shutdown

  ### Marker File

  The `current` marker is a single line: `"node_name\n"`. Written atomically
  via write-to-tmp + `File.rename!/2`. Read on every startup to discover the
  old node for handoff.

  ### Handoff Resolution (`perform_handoff/3`)

  1. **No marker** → first boot. Write marker, proceed normally.

  2. **Marker node == `node()`** → same-VM restart. No handoff needed.

  3. **Marker node != `node()`** → blue-green deploy. Send handoff request
     to old VM's shards in parallel (5s timeout per shard). If old VM is
     dead, requests timeout and new VM opens files directly (WAL recovery).

  ### Key Invariants

  - Single writer: old writer closed before new writer opens
  - Ballot monotonicity: persisted before close, max(now, persisted+1) on init
  - Quorum safety: same node_id → count_alive_node_ids counts as 1
  - kv_paxos durable: promise/accept state survives in SQLite
  - No data loss: WAL checkpoint flushes everything before close
  """

  @valid_opts [
    :name,
    :data_dir,
    :shards,
    :log,
    :tombstone_ttl,
    :gc_interval,
    :blue_green,
    :cluster_size,
    :node_id,
    :sync_chunk_size,
    :skip_stale_check,
    :partition_ttl_policy,
    :wait_for_quorum
  ]

  def start_link(opts) do
    opts = Keyword.validate!(opts, @valid_opts)
    name = Keyword.fetch!(opts, :name)
    Supervisor.start_link(__MODULE__, opts, name: :"#{name}_ekv_sup")
  end

  @impl true
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    data_dir = Keyword.fetch!(opts, :data_dir)
    num_shards = Keyword.get(opts, :shards, 8)
    blue_green = Keyword.get(opts, :blue_green, false)
    log = Keyword.get(opts, :log, :info)
    tombstone_ttl = Keyword.get(opts, :tombstone_ttl, :timer.hours(24 * 7))
    gc_interval = Keyword.get(opts, :gc_interval, :timer.minutes(5))
    cluster_size = Keyword.get(opts, :cluster_size)
    node_id = Keyword.get(opts, :node_id)
    sync_chunk_size = Keyword.get(opts, :sync_chunk_size, 500)
    skip_stale_check = Keyword.get(opts, :skip_stale_check, false)
    partition_ttl_policy = Keyword.get(opts, :partition_ttl_policy, :quarantine)
    wait_for_quorum = Keyword.get(opts, :wait_for_quorum, false)

    validate_cas_config!(cluster_size, node_id)
    validate_partition_ttl_policy!(partition_ttl_policy)
    validate_wait_for_quorum!(wait_for_quorum, cluster_size)

    # Normalize node_id to string early
    node_id =
      case node_id do
        nil -> nil
        id when is_integer(id) -> Integer.to_string(id)
        id when is_binary(id) -> id
      end

    if blue_green, do: perform_handoff(name, data_dir, num_shards)

    # Auto-persist node_id resolution
    effective_node_id =
      if cluster_size do
        persisted = EKV.Store.read_node_id(data_dir)

        cond do
          persisted != nil and node_id != nil and persisted != node_id ->
            require Logger

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

            require Logger
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
      skip_stale_check: skip_stale_check,
      partition_ttl_policy: partition_ttl_policy
    }

    :persistent_term.put({EKV, name}, config)

    gate_children =
      case wait_for_quorum do
        timeout when is_integer(timeout) ->
          [{EKV.QuorumGate, name: name, timeout: timeout, log: log}]

        _ ->
          []
      end

    children =
      [
        {EKV.SubTracker, name: sub_tracker_name, sub_count: sub_count},
        {Registry, keys: :duplicate, name: registry_name, listeners: [sub_tracker_name]},
        {EKV.SubDispatcher.Supervisor, name: name, num_shards: num_shards},
        {EKV.Replica.Supervisor, name: name, num_shards: num_shards, data_dir: data_dir},
        {EKV.GC, name: name}
      ]
      |> List.insert_at(4, gate_children)
      |> List.flatten()

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

        require Logger
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

        # Update marker to current node
        write_marker(data_dir, node())
    end
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
