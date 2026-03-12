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

  @doc "Read a replica shard state on a remote node"
  def replica_state(node, name, shard_index \\ 0) do
    rpc!(node, __MODULE__, :do_replica_state, [name, shard_index])
  end

  @doc "Read a member HWM row from a remote shard db"
  def member_hwm(node, name, member_node, shard_index \\ 0) do
    rpc!(node, __MODULE__, :do_member_hwm, [name, member_node, shard_index])
  end

  @doc "Set a member HWM row on a remote shard db"
  def set_member_hwm(node, name, member_node, seq, shard_index \\ 0) do
    rpc!(node, __MODULE__, :do_set_member_hwm, [name, member_node, seq, shard_index])
  end

  @doc "Read a shard's max oplog seq on a remote node"
  def max_seq(node, name, shard_index \\ 0) do
    rpc!(node, __MODULE__, :do_max_seq, [name, shard_index])
  end

  @doc "Read a shard's min oplog seq on a remote node"
  def min_seq(node, name, shard_index \\ 0) do
    rpc!(node, __MODULE__, :do_min_seq, [name, shard_index])
  end

  @doc "Mutate a replica's cached remote_member_hwm on a remote node"
  def set_cached_remote_hwm(node, name, remote_node, seq, shard_index \\ 0) do
    rpc!(node, __MODULE__, :do_set_cached_remote_hwm, [name, remote_node, seq, shard_index])
  end

  @doc "Trigger one anti-entropy tick on a remote shard"
  def trigger_anti_entropy(node, name, shard_index \\ 0) do
    rpc!(node, __MODULE__, :do_trigger_anti_entropy, [name, shard_index])
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
    %{db: db, stmts: stmts} = :sys.get_state(shard_name)
    value_binary = :erlang.term_to_binary(value)
    origin = Keyword.get(opts, :origin, node())
    expires_at = Keyword.get(opts, :expires_at)
    deleted_at = Keyword.get(opts, :deleted_at)

    {:ok, true} =
      EKV.Store.write_entry(
        db,
        stmts.kv_upsert,
        stmts.oplog_insert,
        key,
        value_binary,
        timestamp,
        origin,
        expires_at,
        deleted_at
      )

    :ok
  end

  def do_replica_state(name, shard_index) do
    shard_name = EKV.Replica.shard_name(name, shard_index)
    :sys.get_state(shard_name)
  end

  def do_member_hwm(name, member_node, shard_index) do
    shard_name = EKV.Replica.shard_name(name, shard_index)
    %{db: db} = :sys.get_state(shard_name)
    EKV.Store.get_hwm(db, member_node)
  end

  def do_set_member_hwm(name, member_node, seq, shard_index) do
    shard_name = EKV.Replica.shard_name(name, shard_index)
    %{db: db} = :sys.get_state(shard_name)
    EKV.Store.set_hwm(db, member_node, seq)
  end

  def do_max_seq(name, shard_index) do
    shard_name = EKV.Replica.shard_name(name, shard_index)
    %{db: db} = :sys.get_state(shard_name)
    EKV.Store.max_seq(db)
  end

  def do_min_seq(name, shard_index) do
    shard_name = EKV.Replica.shard_name(name, shard_index)
    %{db: db} = :sys.get_state(shard_name)
    EKV.Store.min_seq(db)
  end

  def do_set_cached_remote_hwm(name, remote_node, seq, shard_index) do
    shard_name = EKV.Replica.shard_name(name, shard_index)

    :sys.replace_state(shard_name, fn state ->
      %{state | remote_member_hwms: Map.put(state.remote_member_hwms, remote_node, seq)}
    end)

    :ok
  end

  def do_trigger_anti_entropy(name, shard_index) do
    shard_name = EKV.Replica.shard_name(name, shard_index)
    send(shard_name, :anti_entropy_tick)
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
        tracer =
          spawn(fn ->
            forward_trace_events(target_pid)
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

  defp forward_trace_events(target_pid) do
    receive do
      {:trace, _pid, :send, _message, _destination} = trace ->
        send(target_pid, trace)
        forward_trace_events(target_pid)
    after
      30_000 -> :ok
    end
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
