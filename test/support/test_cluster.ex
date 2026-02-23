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
    # Set wrong cookie BEFORE disconnect to prevent Erlang auto-reconnect.
    # Without this, a send({name, node}, msg) on either side can silently
    # re-establish the distribution connection, defeating the partition.
    rpc!(node_a, :erlang, :set_cookie, [node_b, :partition])
    rpc!(node_b, :erlang, :set_cookie, [node_a, :partition])
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

  @doc "Flush all EKV shard GenServers on a remote node"
  def flush_shards(node, name) do
    num_shards = rpc!(node, EKV, :get_config, [name]).num_shards

    for shard <- 0..(num_shards - 1) do
      rpc!(node, :sys, :get_state, [:"#{name}_ekv_replica_#{shard}"])
    end

    :ok
  end
end
