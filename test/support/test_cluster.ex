defmodule EKV.TestCluster do
  @moduledoc false

  @doc "Start N peer nodes with EKV and Group apps loaded and ready"
  def start_peers(count, opts \\ []) do
    cookie = Keyword.get(opts, :cookie, Node.get_cookie())
    code_paths = :code.get_path()

    args =
      [~c"-setcookie", ~c"#{cookie}", ~c"-kernel", ~c"prevent_overlapping_partitions", ~c"false"] ++
        Enum.flat_map(code_paths, fn p -> [~c"-pa", p] end)

    for _i <- 1..count do
      name = :"peer#{System.unique_integer([:positive])}"
      {:ok, pid, node} = :peer.start(%{name: name, args: args})
      {:ok, _} = :erpc.call(node, :application, :ensure_all_started, [:elixir])
      {:ok, _} = :erpc.call(node, :application, :ensure_all_started, [:ekv])
      {pid, node}
    end
  end

  def stop_peers(peers) do
    Enum.each(peers, fn {pid, _node} ->
      if pid do
        try do
          :peer.stop(pid)
        catch
          :exit, _ -> :ok
        end
      end
    end)
  end

  def rpc!(node, mod, fun, args) do
    :erpc.call(node, mod, fun, args)
  end

  @doc "Start EKV on a remote node"
  def start_ekv(node, opts) do
    opts = Keyword.put_new(opts, :log, false)

    :erpc.call(node, fn ->
      {:ok, pid} = EKV.start_link(opts)
      Process.unlink(pid)
      {:ok, pid}
    end)
  end

  @doc "Start EKV on a remote node and return the raw result"
  def start_ekv_result(node, opts) do
    opts = Keyword.put_new(opts, :log, false)

    :erpc.call(node, fn ->
      try do
        case EKV.start_link(opts) do
          {:ok, pid} ->
            Process.unlink(pid)
            {:ok, pid}

          other ->
            other
        end
      catch
        :exit, reason ->
          {:exit, reason}
      end
    end)
  end

  @doc "Stop EKV on a remote node by supervisor name"
  def stop_ekv(node, name, timeout \\ 5_000) do
    :erpc.call(node, __MODULE__, :do_stop_ekv, [name, timeout])
  end

  @doc "Stop the EKV replica supervisor on a remote node"
  def stop_replica_sup(node, name, timeout \\ 5_000) do
    :erpc.call(node, __MODULE__, :do_stop_replica_sup, [name, timeout])
  end

  @doc "Terminate one EKV replica shard child on a remote node without restarting it"
  def terminate_replica_shard(node, name, shard_index \\ 0) do
    :erpc.call(node, __MODULE__, :do_terminate_replica_shard, [name, shard_index])
  end

  @doc false
  def do_stop_ekv(name, timeout) do
    sup_name = :"#{name}_ekv_sup"

    case Process.whereis(sup_name) do
      nil -> :ok
      pid -> Supervisor.stop(pid, :shutdown, timeout)
    end
  end

  @doc false
  def do_stop_replica_sup(name, timeout) do
    sup_name = :"#{name}_ekv_replica_sup"

    case Process.whereis(sup_name) do
      nil -> :ok
      pid -> Supervisor.stop(pid, :shutdown, timeout)
    end
  end

  @doc false
  def do_terminate_replica_shard(name, shard_index) do
    sup_name = :"#{name}_ekv_replica_sup"
    child_id = {EKV.Replica, shard_index}

    case Process.whereis(sup_name) do
      nil ->
        :ok

      _pid ->
        _ = Supervisor.terminate_child(sup_name, child_id)
        :ok
    end
  end

  @doc "Suspend a shard on a remote node while performing EKV.put/4"
  def suspend_shard_and_put(node, name, shard_index, key, value, opts) do
    :erpc.call(node, __MODULE__, :do_suspend_shard_and_put, [
      name,
      shard_index,
      key,
      value,
      opts
    ])
  end

  @doc false
  def do_suspend_shard_and_put(name, shard_index, key, value, opts) do
    shard = EKV.Replica.shard_name(name, shard_index)
    :sys.suspend(shard)

    try do
      EKV.put(name, key, value, opts)
    after
      :sys.resume(shard)
    end
  end

  @doc "True when no registered EKV names remain for the given instance"
  def ekv_stopped?(node, name) do
    prefix = "#{name}_ekv_"
    :erpc.call(node, __MODULE__, :registered_prefix_clear?, [prefix])
  end

  @doc false
  def registered_prefix_clear?(prefix) when is_binary(prefix) do
    not Enum.any?(Process.registered(), fn name ->
      Atom.to_string(name) |> String.starts_with?(prefix)
    end)
  end

  @doc "Wait for a condition to become true, with retries"
  def assert_eventually(fun, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 2000)
    interval = Keyword.get(opts, :interval, 50)
    deadline = System.monotonic_time(:millisecond) + timeout

    do_assert_eventually(fun, interval, deadline)
  end

  defp do_assert_eventually(fun, interval, deadline) do
    case fun.() do
      true ->
        true

      false ->
        if System.monotonic_time(:millisecond) >= deadline do
          raise "assert_eventually timed out"
        end

        Process.sleep(interval)
        do_assert_eventually(fun, interval, deadline)
    end
  end

  @doc "Disconnect two peer nodes from each other"
  def disconnect_nodes(node_a, node_b) do
    # Set MISMATCHED cookies BEFORE disconnect to prevent Erlang auto-reconnect.
    # Both sides must disagree — if both used the same cookie (e.g. :partition),
    # the handshake would succeed and the partition would silently heal when
    # any process does send({name, node}, msg) before processing nodedown.
    rpc!(node_a, :erlang, :set_cookie, [node_b, :partition_a])
    rpc!(node_b, :erlang, :set_cookie, [node_a, :partition_b])
    rpc!(node_a, :erlang, :disconnect_node, [node_b])
  end

  @doc "Reconnect two peer nodes"
  def reconnect_nodes(node_a, node_b) do
    cookie = Node.get_cookie()
    rpc!(node_a, :erlang, :set_cookie, [node_b, cookie])
    rpc!(node_b, :erlang, :set_cookie, [node_a, cookie])
    rpc!(node_a, Node, :connect, [node_b])
  end

  @doc "Monitor nodedown events from a remote node, forwarding to caller"
  def monitor_nodes_on(node, target_pid) do
    :erpc.call(node, fn ->
      spawn(fn ->
        :net_kernel.monitor_nodes(true)
        forward_nodedown(target_pid)
      end)
    end)
  end

  defp forward_nodedown(target_pid) do
    receive do
      {:nodedown, node} ->
        send(target_pid, {:nodedown_on_remote, node})
        forward_nodedown(target_pid)

      {:nodeup, _node} ->
        forward_nodedown(target_pid)
    after
      30_000 -> :ok
    end
  end

  @doc "Subscribe to EKV events on a remote node, forwarding to caller"
  def subscribe_on(node, ekv_name, prefix, target_pid) do
    :erpc.call(node, __MODULE__, :start_subscriber, [ekv_name, prefix, target_pid])
  end

  @doc false
  def start_subscriber(ekv_name, prefix, target_pid) do
    spawn(fn ->
      :ok = EKV.subscribe(ekv_name, prefix)
      subscriber_loop(target_pid)
    end)
  end

  def start_collecting_subscriber_on(node, ekv_name, prefix, target_pid, collect_timeout) do
    :erpc.call(node, __MODULE__, :start_collecting_subscriber, [
      ekv_name,
      prefix,
      target_pid,
      collect_timeout
    ])
  end

  @doc false
  def start_collecting_subscriber(ekv_name, prefix, target_pid, collect_timeout) do
    spawn(fn ->
      :ok = EKV.subscribe(ekv_name, prefix)
      collect_loop(target_pid, [], collect_timeout)
    end)
  end

  defp subscriber_loop(target_pid) do
    receive do
      {:ekv, events, meta} ->
        send(target_pid, {:remote_ekv_event, events, meta})
        subscriber_loop(target_pid)
    after
      30_000 -> :ok
    end
  end

  defp collect_loop(target_pid, acc, timeout) do
    receive do
      {:ekv, events, _meta} ->
        collect_loop(target_pid, acc ++ events, 1000)
    after
      timeout ->
        send(target_pid, {:collected_events, acc})
    end
  end

  @doc "Materialize keys stream on remote node, return sorted list"
  def keys_sorted(node, name, prefix) do
    rpc!(node, __MODULE__, :do_keys_sorted, [name, prefix])
  end

  @doc "Count keys on remote node"
  def keys_count(node, name, prefix) do
    rpc!(node, __MODULE__, :do_keys_count, [name, prefix])
  end

  @doc "Count scan results on remote node"
  def scan_count(node, name, prefix) do
    rpc!(node, __MODULE__, :do_scan_count, [name, prefix])
  end

  @doc "Read raw kv row from a remote shard db, including tombstones"
  def store_get(node, name, key) do
    rpc!(node, __MODULE__, :do_store_get, [name, key])
  end

  @doc "Inject accepted-but-not-promoted CAS state into a remote shard"
  def inject_paxos_accept(node, name, key, value, ballot_c, ballot_n, opts \\ []) do
    rpc!(node, __MODULE__, :do_inject_paxos_accept, [name, key, value, ballot_c, ballot_n, opts])
  end

  @doc "Inject committed kv + oplog state into a remote shard without broadcasting it"
  def inject_committed_entry(node, name, key, value, timestamp, opts \\ []) do
    rpc!(node, __MODULE__, :do_inject_committed_entry, [name, key, value, timestamp, opts])
  end

  @doc "Force only the local kv row on a remote shard, without touching oplog or HWM"
  def force_local_kv_row(node, name, key, value, timestamp, opts \\ []) do
    rpc!(node, __MODULE__, :do_force_local_kv_row, [name, key, value, timestamp, opts])
  end

  @doc "Read a replica shard state on a remote node"
  def replica_state(node, name, shard_index \\ 0) do
    rpc!(node, __MODULE__, :do_replica_state, [name, shard_index])
  end

  @doc "Read local applied progress for an origin stream from a remote shard db"
  def local_progress(node, name, origin_node, shard_index \\ 0) do
    rpc!(node, __MODULE__, :do_local_progress, [name, origin_node, shard_index])
  end

  @doc "Force local applied progress for an origin stream on a remote shard db"
  def set_local_progress(node, name, origin_node, seq, shard_index \\ 0) do
    rpc!(node, __MODULE__, :do_set_local_progress, [name, origin_node, seq, shard_index])
  end

  def force_local_progress(node, name, origin_node, seq, shard_index \\ 0) do
    rpc!(node, __MODULE__, :do_force_local_progress, [name, origin_node, seq, shard_index])
  end

  @doc "Read cached durable peer progress for a remote member/origin pair"
  def peer_progress(node, name, peer_node, origin_node, shard_index \\ 0) do
    rpc!(node, __MODULE__, :do_peer_progress, [name, peer_node, origin_node, shard_index])
  end

  @doc "Force cached durable peer progress for a remote member/origin pair"
  def set_peer_progress(node, name, peer_node, origin_node, seq, shard_index \\ 0) do
    rpc!(node, __MODULE__, :do_set_peer_progress, [name, peer_node, origin_node, seq, shard_index])
  end

  @doc "Read a shard's max oplog seq on a remote node"
  def max_seq(node, name, shard_index \\ 0) do
    rpc!(node, __MODULE__, :do_max_seq, [name, shard_index])
  end

  @doc "Read raw oplog rows from a remote shard"
  def oplog_since(node, name, last_seq, limit, shard_index \\ 0) do
    rpc!(node, __MODULE__, :do_oplog_since, [name, last_seq, limit, shard_index])
  end

  @doc "Count oplog rows on a remote shard"
  def oplog_count(node, name, shard_index \\ 0) do
    rpc!(node, __MODULE__, :do_oplog_count, [name, shard_index])
  end

  @doc "Read oplog retention stats on a remote shard"
  def oplog_retention_stats(node, name, shard_index \\ 0, limit \\ 5) do
    rpc!(node, __MODULE__, :do_oplog_retention_stats, [name, shard_index, limit])
  end

  @doc "Read a shard's min oplog seq on a remote node"
  def min_seq(node, name, shard_index \\ 0) do
    rpc!(node, __MODULE__, :do_min_seq, [name, shard_index])
  end

  @doc "Mutate a replica's cached remote progress for one origin stream on a remote node"
  def set_cached_remote_progress(node, name, remote_node, origin_node, seq, shard_index \\ 0) do
    rpc!(node, __MODULE__, :do_set_cached_remote_progress, [
      name,
      remote_node,
      origin_node,
      seq,
      shard_index
    ])
  end

  @doc "Force a stale sync_inflight marker age for one remote member on a remote shard"
  def set_sync_inflight_age(node, name, remote_node, age_ms, shard_index \\ 0) do
    rpc!(node, __MODULE__, :do_set_sync_inflight_age, [name, remote_node, age_ms, shard_index])
  end

  @doc "Force a stale summary_probe_inflight marker age for one remote member on a remote shard"
  def set_summary_probe_inflight_age(node, name, remote_node, age_ms, shard_index \\ 0) do
    rpc!(node, __MODULE__, :do_set_summary_probe_inflight_age, [
      name,
      remote_node,
      age_ms,
      shard_index
    ])
  end

  @doc "Force a delta_origin_inflight marker age for one origin stream via one remote member"
  def set_delta_origin_inflight_age(
        node,
        name,
        origin_node,
        remote_node,
        age_ms,
        shard_index \\ 0
      ) do
    rpc!(node, __MODULE__, :do_set_delta_origin_inflight_age, [
      name,
      origin_node,
      remote_node,
      age_ms,
      shard_index
    ])
  end

  @doc "Trigger one anti-entropy tick on a remote shard"
  def trigger_anti_entropy(node, name, shard_index \\ 0) do
    rpc!(node, __MODULE__, :do_trigger_anti_entropy, [name, shard_index])
  end

  @doc "Drop one remote shard mapping from a replica state without simulating a full node down"
  def drop_remote_shard(node, name, remote_node, shard_index \\ 0) do
    rpc!(node, __MODULE__, :do_drop_remote_shard, [name, remote_node, shard_index])
  end

  @doc "Delete one in-memory member-node identity mapping from a remote shard state"
  def clear_member_node_id(node, name, remote_node, shard_index \\ 0) do
    rpc!(node, __MODULE__, :do_clear_member_node_id, [name, remote_node, shard_index])
  end

  @doc "Set one in-memory member-node identity mapping on a remote shard state"
  def set_member_node_id(node, name, remote_node, remote_node_id, shard_index \\ 0) do
    rpc!(node, __MODULE__, :do_set_member_node_id, [
      name,
      remote_node,
      remote_node_id,
      shard_index
    ])
  end

  @doc "Read the persisted member-node identity for one remote shard"
  def member_node_identity(node, name, remote_node, shard_index \\ 0) do
    rpc!(node, __MODULE__, :do_member_node_identity, [name, remote_node, shard_index])
  end

  @doc "Delete the persisted member-node identity for one remote shard"
  def clear_member_node_identity(node, name, remote_node, shard_index \\ 0) do
    rpc!(node, __MODULE__, :do_clear_member_node_identity, [name, remote_node, shard_index])
  end

  @doc "Read a raw member-down marker from shard metadata"
  def member_down_marker(node, name, marker_key, shard_index \\ 0) do
    rpc!(node, __MODULE__, :do_member_down_marker, [name, marker_key, shard_index])
  end

  @doc "Set a raw member-down marker in shard metadata and cache"
  def set_member_down_marker(node, name, marker_key, down_since_ms, shard_index \\ 0) do
    rpc!(node, __MODULE__, :do_set_member_down_marker, [
      name,
      marker_key,
      down_since_ms,
      shard_index
    ])
  end

  @doc "Clear a raw member-down marker from shard metadata and cache"
  def clear_member_down_marker(node, name, marker_key, shard_index \\ 0) do
    rpc!(node, __MODULE__, :do_clear_member_down_marker, [name, marker_key, shard_index])
  end

  @doc "Delete retained oplog rows below keep_from_seq for one origin on a remote shard"
  def prune_origin_replay(node, name, origin_node, keep_from_seq, shard_index \\ 0) do
    rpc!(node, __MODULE__, :do_prune_origin_replay, [
      name,
      origin_node,
      keep_from_seq,
      shard_index
    ])
  end

  @doc "Set a replica shard's handoff_node on a remote node"
  def set_handoff_node(node, name, handoff_node, shard_index \\ 0) do
    rpc!(node, __MODULE__, :do_set_handoff_node, [name, handoff_node, shard_index])
  end

  @doc "Enable send tracing on a remote shard, forwarding trace events to target_pid"
  def trace_shard_sends(node, name, target_pid, shard_index \\ 0) do
    rpc!(node, __MODULE__, :do_trace_shard_sends, [name, target_pid, shard_index])
  end

  @doc "Disable send tracing on a remote shard"
  def untrace_shard_sends(node, name, shard_index \\ 0) do
    rpc!(node, __MODULE__, :do_untrace_shard_sends, [name, shard_index])
  end

  @doc "Materialize scan stream on remote node, return %{key => value} map"
  def scan_to_map(node, name, prefix) do
    rpc!(node, __MODULE__, :do_scan_to_map, [name, prefix])
  end

  @doc false
  def do_keys_sorted(name, prefix),
    do: EKV.keys(name, prefix) |> Enum.map(fn {key, _vsn} -> key end) |> Enum.sort()

  def do_keys_count(name, prefix), do: EKV.keys(name, prefix) |> Enum.count()
  def do_scan_count(name, prefix), do: EKV.scan(name, prefix) |> Enum.count()

  def do_store_get(name, key) do
    config = EKV.Supervisor.get_config(name)
    shard = EKV.Replica.shard_index_for(key, config.num_shards)
    shard_name = EKV.Replica.shard_name(name, shard)
    %{db: db} = :sys.get_state(shard_name)
    EKV.Store.get(db, key)
  end

  def do_inject_paxos_accept(name, key, value, ballot_c, ballot_n, opts) do
    config = EKV.Supervisor.get_config(name)
    shard = EKV.Replica.shard_index_for(key, config.num_shards)
    shard_name = EKV.Replica.shard_name(name, shard)
    %{db: db} = :sys.get_state(shard_name)
    value_binary = :erlang.term_to_binary(value)
    timestamp = Keyword.get(opts, :timestamp, System.system_time(:nanosecond))
    origin = Keyword.get(opts, :origin, Atom.to_string(node()))
    expires_at = Keyword.get(opts, :expires_at)
    deleted_at = Keyword.get(opts, :deleted_at)

    {:ok, true} =
      EKV.Store.paxos_accept(db, key, ballot_c, to_string(ballot_n), [
        value_binary,
        timestamp,
        origin,
        expires_at,
        deleted_at
      ])

    :ok
  end

  def do_inject_committed_entry(name, key, value, timestamp, opts) do
    config = EKV.Supervisor.get_config(name)
    shard = EKV.Replica.shard_index_for(key, config.num_shards)
    shard_name = EKV.Replica.shard_name(name, shard)
    state = :sys.get_state(shard_name)
    %{db: db, stmts: stmts, local_origin_seq: local_origin_seq} = state
    value_binary = :erlang.term_to_binary(value)
    origin = Keyword.get(opts, :origin, node())
    origin = replay_origin_id(origin, state)
    local_origin = replay_origin_id(node(), state)

    origin_seq =
      Keyword.get_lazy(opts, :origin_seq, fn ->
        if origin == local_origin do
          local_origin_seq + 1
        else
          db
          |> EKV.Store.local_progress_summary()
          |> Map.get(origin, 0)
          |> Kernel.+(1)
        end
      end)

    expires_at = Keyword.get(opts, :expires_at)
    deleted_at = Keyword.get(opts, :deleted_at)

    {:ok, true, seq, local_progress_seq} =
      EKV.Store.write_entry(
        db,
        stmts.kv_upsert,
        stmts.keyref_upsert,
        stmts.oplog_insert,
        key,
        value_binary,
        timestamp,
        origin,
        expires_at,
        deleted_at,
        origin_seq,
        origin == local_origin
      )

    :sys.replace_state(shard_name, fn state ->
      local_progress =
        Map.update(state.local_progress, origin, local_progress_seq, &max(&1, local_progress_seq))

      if origin == replay_origin_id(node(), state) do
        %{
          state
          | local_progress: local_progress,
            local_origin_seq: max(state.local_origin_seq, seq)
        }
      else
        %{state | local_progress: local_progress}
      end
    end)

    :ok
  end

  def do_force_local_kv_row(name, key, value, timestamp, opts) do
    config = EKV.Supervisor.get_config(name)
    shard = EKV.Replica.shard_index_for(key, config.num_shards)
    shard_name = EKV.Replica.shard_name(name, shard)
    %{db: db} = :sys.get_state(shard_name)
    value_binary = :erlang.term_to_binary(value)
    origin = Keyword.get(opts, :origin, node())
    origin = replay_origin_id(origin, :sys.get_state(shard_name))
    expires_at = Keyword.get(opts, :expires_at)
    deleted_at = Keyword.get(opts, :deleted_at)
    origin_str = origin

    {:ok, stmt} =
      EKV.Sqlite3.prepare(
        db,
        """
        INSERT INTO kv (key, value, timestamp, origin_node, expires_at, deleted_at, expired_at)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL)
        ON CONFLICT(key) DO UPDATE SET
          value = excluded.value,
          timestamp = excluded.timestamp,
          origin_node = excluded.origin_node,
          expires_at = excluded.expires_at,
          deleted_at = excluded.deleted_at,
          expired_at = NULL
        """
      )

    :ok =
      EKV.Sqlite3.bind(stmt, [key, value_binary, timestamp, origin_str, expires_at, deleted_at])

    :done = EKV.Sqlite3.step(db, stmt)
    :ok = EKV.Sqlite3.release(db, stmt)
    :ok
  end

  def do_replica_state(name, shard_index) do
    shard_name = EKV.Replica.shard_name(name, shard_index)

    shard_name
    |> :sys.get_state()
    |> alias_replica_state()
  end

  def do_local_progress(name, origin_node, shard_index) do
    shard_name = EKV.Replica.shard_name(name, shard_index)
    state = :sys.get_state(shard_name)
    %{db: db, local_origin_seq: local_origin_seq} = state
    origin_node = replay_origin_id(origin_node, state)

    if origin_node == replay_origin_id(node(), state) do
      local_origin_seq
    else
      db
      |> EKV.Store.local_progress_summary()
      |> Map.get(origin_node, 0)
    end
  end

  def do_set_local_progress(name, origin_node, seq, shard_index) do
    shard_name = EKV.Replica.shard_name(name, shard_index)
    state = :sys.get_state(shard_name)
    %{db: db} = state
    origin_node = replay_origin_id(origin_node, state)
    :ok = EKV.Store.merge_local_progress(db, origin_node, seq)

    :sys.replace_state(shard_name, fn state ->
      local_progress =
        Map.update(state.local_progress, origin_node, seq, &max(&1, seq))

      if origin_node == replay_origin_id(node(), state) do
        %{
          state
          | local_progress: local_progress,
            local_origin_seq: max(state.local_origin_seq, seq)
        }
      else
        %{state | local_progress: local_progress}
      end
    end)

    :ok
  end

  def do_force_local_progress(name, origin_node, seq, shard_index) do
    shard_name = EKV.Replica.shard_name(name, shard_index)
    state = :sys.get_state(shard_name)
    %{db: db} = state
    origin_node = replay_origin_id(origin_node, state)

    progress =
      db
      |> EKV.Store.local_progress_summary()
      |> Map.put(origin_node, seq)

    :ok = EKV.Store.replace_local_progress_summary(db, progress)

    :sys.replace_state(shard_name, fn state ->
      local_progress = Map.put(state.local_progress, origin_node, seq)

      if origin_node == replay_origin_id(node(), state) do
        %{state | local_progress: local_progress, local_origin_seq: seq}
      else
        %{state | local_progress: local_progress}
      end
    end)

    :ok
  end

  def do_peer_progress(name, peer_node, origin_node, shard_index) do
    shard_name = EKV.Replica.shard_name(name, shard_index)
    state = :sys.get_state(shard_name)
    %{db: db} = state
    peer_node = member_progress_id(peer_node, state)
    origin_node = replay_origin_id(origin_node, state)

    db
    |> EKV.Store.get_peer_progress(peer_node)
    |> Map.get(origin_node, 0)
  end

  def do_set_peer_progress(name, peer_node, origin_node, seq, shard_index) do
    shard_name = EKV.Replica.shard_name(name, shard_index)
    state = :sys.get_state(shard_name)
    %{db: db} = state
    peer_node = member_progress_id(peer_node, state)
    origin_node = replay_origin_id(origin_node, state)

    progress =
      db
      |> EKV.Store.get_peer_progress(peer_node)
      |> Map.put(origin_node, seq)

    EKV.Store.replace_peer_progress(db, peer_node, progress)
  end

  def do_max_seq(name, shard_index) do
    shard_name = EKV.Replica.shard_name(name, shard_index)
    %{db: db} = :sys.get_state(shard_name)
    EKV.Store.max_seq(db)
  end

  def do_oplog_since(name, last_seq, limit, shard_index) do
    shard_name = EKV.Replica.shard_name(name, shard_index)
    state = :sys.get_state(shard_name)
    %{db: db} = state

    db
    |> EKV.Store.oplog_since_chunk(last_seq, limit)
    |> Enum.map(fn {seq, key, value, ts, origin, origin_seq, expires_at, is_delete} ->
      {seq, key, value, ts, alias_origin(origin, state), origin_seq, expires_at, is_delete}
    end)
  end

  def do_min_seq(name, shard_index) do
    shard_name = EKV.Replica.shard_name(name, shard_index)
    %{db: db} = :sys.get_state(shard_name)
    EKV.Store.min_seq(db)
  end

  def do_oplog_count(name, shard_index) do
    shard_name = EKV.Replica.shard_name(name, shard_index)
    %{db: db} = :sys.get_state(shard_name)

    case EKV.Sqlite3.fetch_all(db, "SELECT COUNT(*) FROM kv_oplog", []) do
      {:ok, [[count]]} -> count
      _ -> 0
    end
  end

  def do_oplog_retention_stats(name, shard_index, limit) do
    shard_name = EKV.Replica.shard_name(name, shard_index)
    %{db: db} = :sys.get_state(shard_name)
    EKV.Store.oplog_retention_stats(db, limit)
  end

  def do_set_cached_remote_progress(name, remote_node, origin_node, seq, shard_index) do
    shard_name = EKV.Replica.shard_name(name, shard_index)
    state = :sys.get_state(shard_name)
    origin_node = replay_origin_id(origin_node, state)

    :sys.replace_state(shard_name, fn state ->
      remote_progress = Map.get(state.remote_member_progress, remote_node, %{})
      updated_progress = Map.put(remote_progress, origin_node, seq)

      %{
        state
        | remote_member_progress:
            Map.put(state.remote_member_progress, remote_node, updated_progress),
          remote_member_hwms: Map.put(state.remote_member_hwms, remote_node, seq)
      }
    end)

    :ok
  end

  def do_set_sync_inflight_age(name, remote_node, age_ms, shard_index) do
    shard_name = EKV.Replica.shard_name(name, shard_index)

    :sys.replace_state(shard_name, fn state ->
      sent_at_ms = System.monotonic_time(:millisecond) - max(age_ms, 0)
      %{state | sync_inflight: Map.put(state.sync_inflight, remote_node, sent_at_ms)}
    end)

    :ok
  end

  def do_set_summary_probe_inflight_age(name, remote_node, age_ms, shard_index) do
    shard_name = EKV.Replica.shard_name(name, shard_index)

    :sys.replace_state(shard_name, fn state ->
      sent_at_ms = System.monotonic_time(:millisecond) - max(age_ms, 0)

      %{
        state
        | summary_probe_inflight: Map.put(state.summary_probe_inflight, remote_node, sent_at_ms)
      }
    end)

    :ok
  end

  def do_set_delta_origin_inflight_age(name, origin_node, remote_node, age_ms, shard_index) do
    shard_name = EKV.Replica.shard_name(name, shard_index)
    state = :sys.get_state(shard_name)
    origin_node = replay_origin_id(origin_node, state)

    :sys.replace_state(shard_name, fn state ->
      activity_at_ms = System.monotonic_time(:millisecond) - max(age_ms, 0)

      %{
        state
        | delta_origin_inflight:
            Map.put(state.delta_origin_inflight, origin_node, {remote_node, activity_at_ms})
      }
    end)

    :ok
  end

  defp replay_origin_id(origin, _state) when is_binary(origin), do: origin

  defp replay_origin_id(origin, state) when is_atom(origin) do
    cond do
      origin == node() and is_binary(state.node_id) and byte_size(state.node_id) > 0 ->
        state.node_id

      is_binary(Map.get(state.member_node_ids, origin)) ->
        Map.fetch!(state.member_node_ids, origin)

      match?(%{db: _}, state) and is_binary(EKV.Store.member_node_identity_get(state.db, origin)) ->
        EKV.Store.member_node_identity_get(state.db, origin)

      true ->
        Atom.to_string(origin)
    end
  end

  defp replay_origin_id(origin, _state), do: to_string(origin)

  defp member_progress_id(member_node, state), do: replay_origin_id(member_node, state)

  def do_trigger_anti_entropy(name, shard_index) do
    shard_name = EKV.Replica.shard_name(name, shard_index)
    send(shard_name, :anti_entropy_tick)
    :ok
  end

  def do_drop_remote_shard(name, remote_node, shard_index) do
    shard_name = EKV.Replica.shard_name(name, shard_index)

    :sys.replace_state(shard_name, fn state ->
      state = %{
        state
        | remote_shards: Map.delete(state.remote_shards, remote_node),
          summary_probe_inflight: Map.delete(state.summary_probe_inflight, remote_node),
          sync_inflight: Map.delete(state.sync_inflight, remote_node)
      }

      if state.full_sync_inflight == remote_node do
        %{state | full_sync_inflight: nil}
      else
        state
      end
    end)

    :ok
  end

  def do_clear_member_node_id(name, remote_node, shard_index) do
    shard_name = EKV.Replica.shard_name(name, shard_index)

    :sys.replace_state(shard_name, fn state ->
      %{state | member_node_ids: Map.delete(state.member_node_ids, remote_node)}
    end)

    :ok
  end

  def do_set_member_node_id(name, remote_node, remote_node_id, shard_index) do
    shard_name = EKV.Replica.shard_name(name, shard_index)

    :sys.replace_state(shard_name, fn state ->
      %{state | member_node_ids: Map.put(state.member_node_ids, remote_node, remote_node_id)}
    end)

    :ok
  end

  def do_member_node_identity(name, remote_node, shard_index) do
    shard_name = EKV.Replica.shard_name(name, shard_index)
    %{db: db} = :sys.get_state(shard_name)
    EKV.Store.member_node_identity_get(db, remote_node)
  end

  def do_clear_member_node_identity(name, remote_node, shard_index) do
    shard_name = EKV.Replica.shard_name(name, shard_index)
    %{db: db} = :sys.get_state(shard_name)

    {:ok, stmt} =
      EKV.Sqlite3.prepare(db, "DELETE FROM kv_meta WHERE key = ?1")

    :ok = EKV.Sqlite3.bind(stmt, ["member_node_id:" <> Atom.to_string(remote_node)])
    :done = EKV.Sqlite3.step(db, stmt)
    :ok = EKV.Sqlite3.release(db, stmt)
    :ok
  end

  def do_member_down_marker(name, marker_key, shard_index) do
    shard_name = EKV.Replica.shard_name(name, shard_index)
    %{db: db} = :sys.get_state(shard_name)
    EKV.Store.member_down_marker_get(db, marker_key)
  end

  def do_set_member_down_marker(name, marker_key, down_since_ms, shard_index) do
    shard_name = EKV.Replica.shard_name(name, shard_index)
    %{db: db} = :sys.get_state(shard_name)
    EKV.Store.member_down_marker_put(db, marker_key, down_since_ms)

    :sys.replace_state(shard_name, fn state ->
      %{state | member_down_at: Map.put(state.member_down_at, marker_key, down_since_ms)}
    end)

    :ok
  end

  def do_clear_member_down_marker(name, marker_key, shard_index) do
    shard_name = EKV.Replica.shard_name(name, shard_index)
    %{db: db} = :sys.get_state(shard_name)
    EKV.Store.member_down_marker_clear(db, marker_key)

    :sys.replace_state(shard_name, fn state ->
      %{state | member_down_at: Map.delete(state.member_down_at, marker_key)}
    end)

    :ok
  end

  def do_prune_origin_replay(name, origin_node, keep_from_seq, shard_index) do
    shard_name = EKV.Replica.shard_name(name, shard_index)
    state = :sys.get_state(shard_name)
    %{db: db} = state
    origin_node = replay_origin_id(origin_node, state)

    {:ok, stmt} =
      EKV.Sqlite3.prepare(
        db,
        "DELETE FROM kv_oplog WHERE origin_node = ?1 AND origin_seq < ?2"
      )

    :ok = EKV.Sqlite3.bind(stmt, [origin_node, keep_from_seq])
    :done = EKV.Sqlite3.step(db, stmt)
    :ok = EKV.Sqlite3.release(db, stmt)
    :ok
  end

  def do_set_handoff_node(name, handoff_node, shard_index) do
    shard_name = EKV.Replica.shard_name(name, shard_index)

    :sys.replace_state(shard_name, fn state ->
      %{state | handoff_node: handoff_node}
    end)

    :ok
  end

  def do_trace_shard_sends(name, target_pid, shard_index) do
    shard_name = EKV.Replica.shard_name(name, shard_index)

    case Process.whereis(shard_name) do
      nil ->
        {:error, :noproc}

      pid ->
        state = :sys.get_state(shard_name)

        tracer =
          spawn(fn ->
            forward_trace_events(target_pid, state)
          end)

        :erlang.trace(pid, true, [:send, {:tracer, tracer}])
        :ok
    end
  end

  def do_untrace_shard_sends(name, shard_index) do
    shard_name = EKV.Replica.shard_name(name, shard_index)

    case Process.whereis(shard_name) do
      nil ->
        :ok

      pid ->
        :erlang.trace(pid, false, [:send])
        :ok
    end
  end

  defp forward_trace_events(target_pid, state) do
    receive do
      {:trace, _pid, :send, _message, _destination} = trace ->
        send(target_pid, normalize_trace_event(trace, state))
        forward_trace_events(target_pid, state)
    after
      30_000 -> :ok
    end
  end

  defp alias_replica_state(state) do
    %{
      state
      | local_progress: normalize_progress_map(state.local_progress, state),
        remote_member_progress:
          Map.new(state.remote_member_progress, fn {remote_node, progress} ->
            {remote_node, normalize_progress_map(progress, state)}
          end)
    }
  end

  defp normalize_progress_map(progress, state) when is_map(progress) do
    Map.new(progress, fn {origin, seq} -> {alias_origin(origin, state), seq} end)
  end

  defp normalize_progress_map(progress, _state), do: progress

  defp alias_origin(origin, state) when is_binary(origin) do
    cond do
      is_binary(state.node_id) and origin == state.node_id ->
        node()

      true ->
        Enum.find_value(state.member_node_ids, origin, fn {member_node, member_node_id} ->
          if member_node_id == origin or Atom.to_string(member_node) == origin, do: member_node
        end) || origin
    end
  end

  defp alias_origin(origin, _state), do: origin

  defp normalize_trace_event(
         {:trace, pid, :send, {:ekv_sync_request, remote_pid, shard, {:delta, origin, seq}},
          destination},
         state
       ) do
    {:trace, pid, :send,
     {:ekv_sync_request, remote_pid, shard, {:delta, alias_origin(origin, state), seq}},
     destination}
  end

  defp normalize_trace_event(
         {:trace, pid, :send,
          {:ekv, version, :sync_request, {remote_pid, shard, {:delta, origin, seq}}, meta},
          destination},
         state
       ) do
    {:trace, pid, :send,
     {:ekv, version, :sync_request,
      {remote_pid, shard, {:delta, alias_origin(origin, state), seq}}, meta}, destination}
  end

  defp normalize_trace_event(
         {:trace, pid, :send, {:ekv_sync, from_node, shard, mode, entries, progress},
          destination},
         state
       ) do
    {:trace, pid, :send,
     {:ekv_sync, from_node, shard, mode, alias_sync_entries(entries, state),
      normalize_progress_map(progress, state)}, destination}
  end

  defp normalize_trace_event(
         {:trace, pid, :send,
          {:ekv, version, :sync, {from_node, shard, mode, entries, progress}, meta}, destination},
         state
       ) do
    {:trace, pid, :send,
     {:ekv, version, :sync,
      {from_node, shard, mode, alias_sync_entries(entries, state),
       normalize_progress_map(progress, state)}, meta}, destination}
  end

  defp normalize_trace_event(trace, _state), do: trace

  defp alias_sync_entries(entries, state) when is_list(entries) do
    Enum.map(entries, fn {key, value_binary, ts, origin, origin_seq, expires_at, deleted_at} ->
      {key, value_binary, ts, alias_origin(origin, state), origin_seq, expires_at, deleted_at}
    end)
  end

  def do_scan_to_map(name, prefix),
    do: EKV.scan(name, prefix) |> Map.new(fn {k, v, _vsn} -> {k, v} end)

  # CAS helpers — named functions that can be called across nodes
  def cas_increment(nil), do: 1
  def cas_increment(n), do: n + 1

  def cas_upcase(v), do: String.upcase(v)

  @doc "Kill a process by registered name (for cross-node RPC)"
  def kill_registered(name) do
    case Process.whereis(name) do
      nil -> :ok
      pid -> Process.exit(pid, :kill)
    end
  end

  @doc "Flush all EKV shard GenServers on a remote node"
  def flush_shards(node, name) do
    num_shards = rpc!(node, EKV.Supervisor, :get_config, [name]).num_shards

    for shard <- 0..(num_shards - 1) do
      rpc!(node, :sys, :get_state, [:"#{name}_ekv_replica_#{shard}"])
    end

    :ok
  end

  @doc "Suspend all EKV shard GenServers on a remote node (simulates latency/freeze)"
  def suspend_shards(node, name) do
    num_shards = rpc!(node, EKV.Supervisor, :get_config, [name]).num_shards

    for shard <- 0..(num_shards - 1) do
      rpc!(node, :sys, :suspend, [:"#{name}_ekv_replica_#{shard}"])
    end

    :ok
  end

  @doc "Resume all EKV shard GenServers on a remote node"
  def resume_shards(node, name) do
    num_shards = rpc!(node, EKV.Supervisor, :get_config, [name]).num_shards

    for shard <- 0..(num_shards - 1) do
      rpc!(node, :sys, :resume, [:"#{name}_ekv_replica_#{shard}"])
    end

    :ok
  end

  def cas_append(v, suffix), do: to_string(v) <> suffix
end
