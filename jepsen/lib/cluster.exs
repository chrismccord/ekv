defmodule EkvJepsen.Cluster do
  @moduledoc false
  alias EkvJepsen.History

  @name :jepsen_kv
  @modes [:none, :partition_flap, :restart_one, :partition_restart, :crash_one, :partition_crash]

  def modes, do: @modes

  def start_link(count, data_dir) do
    nonce = "#{System.system_time(:microsecond)}_#{System.unique_integer([:positive])}"

    unless Node.alive?() do
      # Unlike --sname at VM boot, Node.start/2 does not start EPMD.
      {_, 0} = System.cmd("epmd", ["-daemon"])

      {:ok, _} =
        Node.start(:"jepsen_coordinator_#{nonce}", :shortnames)
    end

    # Never silently reuse an old database as an initially empty register.
    :ok = File.mkdir(data_dir)
    {:ok, cluster} = Agent.start_link(fn -> [] end)

    try do
      for i <- 1..count do
        peer_opts = %{
          name: :"jepsen_peer_#{nonce}_#{i}",
          connection: :standard_io,
          peer_down: :continue,
          args:
            [
              ~c"-setcookie",
              ~c"#{Node.get_cookie()}",
              ~c"-kernel",
              ~c"prevent_overlapping_partitions",
              ~c"false",
              ~c"connect_all",
              ~c"false"
            ] ++
              Enum.flat_map(:code.get_path(), &[~c"-pa", &1])
        }

        opts = [
          name: @name,
          data_dir: Path.join(data_dir, "#{i}"),
          shards: 8,
          log: false,
          gc_interval: :timer.hours(1),
          cluster_size: count,
          node_id: "jepsen-node-#{i}"
        ]

        {:ok, pid, node} = :peer.start(peer_opts)
        member = %{pid: pid, node: node, peer_opts: peer_opts, opts: opts}
        Agent.update(cluster, &(&1 ++ [member]))
        boot(member)
      end

      heal(cluster)
      cluster
    rescue
      e ->
        stop(cluster)
        reraise e, __STACKTRACE__
    catch
      kind, reason ->
        stop(cluster)
        :erlang.raise(kind, reason, __STACKTRACE__)
    end
  end

  def members(cluster), do: Agent.get(cluster, & &1)
  def nodes(cluster), do: Enum.map(members(cluster), & &1.node)

  def stop(cluster) do
    # Peer controllers are tracked immediately after start, including replacement
    # VMs. Stop every owned controller even if another one has already died.
    Enum.each(members(cluster), fn member ->
      if Process.alive?(member.pid) do
        try do
          :peer.stop(member.pid)
        catch
          :exit, reason -> IO.warn("peer cleanup failed for #{member.node}: #{inspect(reason)}")
        end
      end
    end)

    Agent.stop(cluster)
  end

  def heal(cluster) do
    nodes = nodes(cluster)

    for a <- nodes, b <- nodes, a != b do
      true = call(a, Node, :set_cookie, [b, Node.get_cookie()])
    end

    for a <- nodes, b <- nodes, a != b, do: true = call(a, Node, :connect, [b])
    wait_ready(cluster)
  end

  def wait_ready(cluster) do
    nodes = nodes(cluster)

    wait!("EKV member mesh", fn ->
      Enum.all?(nodes, fn node ->
        info = call(node, EKV, :info, [@name])
        length(info.connected_members) == length(nodes) - 1
      end)
    end)
  end

  def run_faults(cluster, _history, :none, _seed), do: heal(cluster)

  def run_faults(cluster, history, mode, seed) when mode in @modes do
    for cycle <- 0..1 do
      Process.sleep(250)
      target = Enum.at(nodes(cluster), rem(seed + cycle, length(nodes(cluster))))
      partition? = mode in [:partition_flap, :partition_restart, :partition_crash]
      restart? = mode in [:restart_one, :partition_restart, :crash_one, :partition_crash]
      crash? = mode in [:crash_one, :partition_crash]

      if partition?, do: partition(cluster, target)
      old_member = Enum.find(members(cluster), &(&1.node == target))

      if restart? do
        if crash? do
          # halt/1 bypasses OTP shutdown callbacks and SQLite connection cleanup.
          :erpc.cast(target, :erlang, :halt, [137])
          wait!("abrupt VM death", fn -> match?({:down, _}, :peer.get_state(old_member.pid)) end)
        else
          :ok = call(target, EKV.JepsenHelper, :stop_ekv, [@name])
          nil = call(target, Process, :whereis, [:jepsen_kv_ekv_sup])
        end
      end

      fault = %{
        process: :nemesis,
        type: :info,
        value: mode,
        cycle: cycle,
        node: to_string(target)
      }

      History.log(history, Map.put(fault, :f, :fault_start))
      Process.sleep(400)
      History.log(history, Map.put(fault, :f, :fault_healing))

      if restart? do
        if crash? do
          :peer.stop(old_member.pid)
          {:ok, pid, ^target} = :peer.start(old_member.peer_opts)
          member = %{old_member | pid: pid}

          Agent.update(cluster, fn ms ->
            Enum.map(ms, &if(&1.node == target, do: member, else: &1))
          end)

          if partition?, do: partition(cluster, target)
          boot(member)
        else
          {:ok, _} = call(target, EKV.JepsenHelper, :start_ekv, [old_member.opts])
        end
      end

      if partition?, do: assert_partition!(cluster, target)
      heal(cluster)
      History.log(history, Map.put(fault, :f, :fault_end))
      Process.sleep(250)
    end
  end

  def partition(cluster, target) do
    for other <- nodes(cluster), other != target do
      # Different per-peer cookies prevent implicit distribution reconnects.
      # Coordinator RPC remains available and the majority remains connected.
      true = call(target, Node, :set_cookie, [other, :jepsen_blocked_from_minority])
      true = call(other, Node, :set_cookie, [target, :jepsen_blocked_from_majority])
      call(target, Node, :disconnect, [other])
      call(other, Node, :disconnect, [target])
    end

    assert_partition!(cluster, target)
  end

  def assert_partition!(cluster, target) do
    for other <- nodes(cluster), other != target do
      false = other in call(target, Node, :list, [])
      false = target in call(other, Node, :list, [])
    end

    :ok
  end

  defp boot(member) do
    {:ok, _} = call(member.node, :application, :ensure_all_started, [:elixir])
    {:ok, _} = call(member.node, :application, :ensure_all_started, [:ekv])
    {:ok, _} = call(member.node, EKV.JepsenHelper, :start_ekv, [member.opts])
  end

  defp call(node, mod, fun, args), do: :erpc.call(node, mod, fun, args, 5_000)

  defp wait!(label, fun), do: wait!(label, fun, System.monotonic_time(:millisecond) + 30_000)

  defp wait!(label, fun, deadline) do
    unless fun.() do
      if System.monotonic_time(:millisecond) >= deadline,
        do: raise("timeout waiting for #{label}")

      Process.sleep(50)
      wait!(label, fun, deadline)
    end
  end
end
