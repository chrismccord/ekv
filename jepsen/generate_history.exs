defmodule EkvJepsen.HistoryGen do
  @moduledoc false

  @ekv_name :jepsen_kv
  @key "jepsen/register"
  @valid_modes [:none, :partition_flap, :restart_one]

  def run(path, workers, total_ops, cluster_nodes, mode)
      when workers > 0 and total_ops > 0 and cluster_nodes >= 3 and mode in @valid_modes do
    ensure_distributed!()

    IO.puts("starting #{cluster_nodes}-node peer cluster (mode=#{mode})...")
    {peers, nodes} = start_peers(cluster_nodes)
    IO.puts("peer nodes: #{Enum.map_join(nodes, ", ", &Atom.to_string/1)}")

    IO.puts("starting EKV on peer nodes...")
    cluster_meta = start_ekv_cluster(nodes)

    IO.puts("connecting peer mesh...")
    connect_full_mesh(nodes)
    wait_for_node_mesh!(nodes, 15_000)

    wait_for_ekv_mesh!(nodes, 30_000)

    {:ok, events} = Agent.start_link(fn -> [] end)
    event_counter = :atomics.new(1, [])
    progress_counter = :atomics.new(1, [])
    progress_every = max(div(total_ops, 20), 1_000)

    log = fn op ->
      idx = :atomics.add_get(event_counter, 1, 1)
      Agent.update(events, fn acc -> [{idx, op} | acc] end)
    end

    nemesis_pid = start_nemesis(mode, nodes, cluster_meta)

    try do
      seed_node = hd(nodes)
      IO.puts("seeding initial register value on #{seed_node}...")
      seed_initial_value(seed_node, log)

      per_worker = div(total_ops, workers)
      extra = rem(total_ops, workers)

      assignments =
        for worker <- 0..(workers - 1) do
          {worker, per_worker + if(worker < extra, do: 1, else: 0)}
        end

      IO.puts("running workload: workers=#{workers}, ops=#{total_ops}...")

      Task.async_stream(
        assignments,
        fn {worker, n_ops} ->
          run_worker(nodes, worker, n_ops, log, progress_counter, progress_every, total_ops)
        end,
        max_concurrency: workers,
        ordered: false,
        timeout: :infinity
      )
      |> Stream.run()

      history =
        events
        |> Agent.get(& &1)
        |> Enum.sort_by(fn {idx, _} -> idx end)
        |> Enum.map(fn {_idx, op} -> op end)

      stop_nemesis(nemesis_pid)
      write_history!(path, history)
      IO.puts("history written: #{path}")
    after
      Agent.stop(events)
      stop_nemesis(nemesis_pid)
      stop_ekv_cluster(nodes)
      stop_peers(peers)
      Enum.each(cluster_meta, fn {_node, meta} -> File.rm_rf!(meta.data_dir) end)
    end
  end

  defp start_peers(count) do
    cookie = Node.get_cookie()
    code_paths = :code.get_path()

    args =
      [~c"-setcookie", ~c"#{cookie}", ~c"-kernel", ~c"prevent_overlapping_partitions", ~c"false"] ++
        Enum.flat_map(code_paths, fn p -> [~c"-pa", p] end)

    peers =
      for _i <- 1..count do
        name = :"jepsen_peer#{System.unique_integer([:positive])}"
        {:ok, pid, node} = :peer.start(%{name: name, args: args})
        {:ok, _} = :erpc.call(node, :application, :ensure_all_started, [:elixir])
        {:ok, _} = :erpc.call(node, :application, :ensure_all_started, [:ekv])
        {pid, node}
      end

    {peers, Enum.map(peers, &elem(&1, 1))}
  end

  defp ensure_distributed! do
    if Node.alive?() do
      :ok
    else
      name = :"jepsen_coordinator_#{System.unique_integer([:positive])}"

      case Node.start(name, :shortnames) do
        {:ok, _pid} ->
          :ok

        {:error, {:already_started, _pid}} ->
          :ok

        {:error, reason} ->
          raise "failed to start distributed coordinator node: #{inspect(reason)}"
      end
    end
  end

  defp connect_full_mesh(nodes) do
    for node <- nodes, other <- nodes, node != other do
      _ = :erpc.call(node, Node, :connect, [other])
    end
  end

  defp wait_for_node_mesh!(nodes, timeout_ms) do
    wait_until!("node mesh", timeout_ms, fn ->
      Enum.all?(nodes, fn node ->
        connected = :erpc.call(node, Node, :list, [])
        expected = Enum.reject(nodes, &(&1 == node))
        Enum.all?(expected, &(&1 in connected))
      end)
    end)
  end

  defp start_ekv_cluster(nodes) do
    cluster_size = length(nodes)

    Enum.with_index(nodes, 1)
    |> Enum.map(fn {node, idx} ->
      data_dir = "/tmp/ekv_jepsen_#{idx}_#{System.unique_integer([:positive])}"

      opts = [
        name: @ekv_name,
        data_dir: data_dir,
        shards: 8,
        log: false,
        gc_interval: :timer.hours(1),
        cluster_size: cluster_size,
        node_id: "jepsen-node-#{idx}"
      ]

      {:ok, _pid} = :erpc.call(node, EKV.JepsenHelper, :start_ekv, [opts])

      {node, %{opts: opts, data_dir: data_dir}}
    end)
    |> Map.new()
  end

  defp wait_for_ekv_mesh!(nodes, timeout_ms) do
    cluster_size = length(nodes)

    try do
      wait_until!("EKV mesh", timeout_ms, fn ->
        Enum.all?(nodes, fn node ->
          try do
            info = :erpc.call(node, EKV, :info, [@ekv_name])
            length(info.connected_peers) == cluster_size - 1
          rescue
            _ -> false
          catch
            :exit, _ -> false
          end
        end)
      end)
    rescue
      e ->
        IO.puts("EKV mesh diagnostics:")

        Enum.each(nodes, fn node ->
          node_list = safe_erpc(node, Node, :list, [])
          ekv_info = safe_erpc(node, EKV, :info, [@ekv_name])
          IO.puts("  #{node} Node.list=#{inspect(node_list)} EKV.info=#{inspect(ekv_info)}")
        end)

        reraise e, __STACKTRACE__
    end
  end

  defp stop_ekv_cluster(nodes) do
    Enum.each(nodes, fn node ->
      try do
        :ok = :erpc.call(node, EKV.JepsenHelper, :stop_ekv, [@ekv_name])
      catch
        :exit, _ -> :ok
      end
    end)
  end

  defp stop_peers(peers) do
    Enum.each(peers, fn {pid, _node} ->
      try do
        :peer.stop(pid)
      catch
        :exit, _ -> :ok
      end
    end)
  end

  defp start_nemesis(:none, _nodes, _cluster_meta), do: nil

  defp start_nemesis(:partition_flap, nodes, _cluster_meta) do
    IO.puts("starting nemesis: partition_flap")

    spawn_link(fn ->
      partition_flap_loop(nodes, 0)
    end)
  end

  defp start_nemesis(:restart_one, nodes, cluster_meta) do
    IO.puts("starting nemesis: restart_one")

    spawn_link(fn ->
      restart_one_loop(nodes, cluster_meta, 0)
    end)
  end

  defp stop_nemesis(nil), do: :ok

  defp stop_nemesis(pid) when is_pid(pid) do
    send(pid, :stop)
    :ok
  end

  defp partition_flap_loop(nodes, iter) do
    receive do
      :stop ->
        :ok
    after
      350 ->
        isolated = Enum.at(nodes, rem(iter, length(nodes)))
        majority = Enum.reject(nodes, &(&1 == isolated))

        Enum.each(majority, fn node ->
          disconnect_pair(isolated, node)
        end)

        Process.sleep(250)
        connect_full_mesh(nodes)
        Process.sleep(250)
        partition_flap_loop(nodes, iter + 1)
    end
  end

  defp restart_one_loop(nodes, cluster_meta, iter) do
    receive do
      :stop ->
        :ok
    after
      500 ->
        target = Enum.at(nodes, rem(iter, length(nodes)))
        %{opts: opts} = Map.fetch!(cluster_meta, target)

        :ok = safe_stop_ekv(target)
        Process.sleep(400)
        :ok = start_ekv_with_retry(target, opts, 12)
        connect_full_mesh(nodes)
        wait_for_ekv_node!(target, length(nodes))
        Process.sleep(300)
        restart_one_loop(nodes, cluster_meta, iter + 1)
    end
  end

  defp disconnect_pair(a, b) do
    _ = safe_erpc(a, Node, :disconnect, [b])
    _ = safe_erpc(b, Node, :disconnect, [a])
    :ok
  end

  defp safe_stop_ekv(node) do
    try do
      :ok = :erpc.call(node, EKV.JepsenHelper, :stop_ekv, [@ekv_name])
    catch
      :exit, _ -> :ok
    end
  end

  defp start_ekv_with_retry(node, opts, attempts_left) when attempts_left > 0 do
    case safe_erpc(node, EKV.JepsenHelper, :start_ekv, [opts]) do
      {:ok, _pid} ->
        :ok

      {:error, _reason} ->
        Process.sleep(250)
        start_ekv_with_retry(node, opts, attempts_left - 1)
    end
  end

  defp start_ekv_with_retry(_node, _opts, 0), do: raise("failed to restart EKV after retries")

  defp wait_for_ekv_node!(node, cluster_size) do
    wait_until!("EKV restart #{node}", 20_000, fn ->
      case safe_erpc(node, EKV, :info, [@ekv_name]) do
        %{connected_peers: peers} -> length(peers) >= cluster_size - 1
        _ -> false
      end
    end)
  end

  defp seed_initial_value(seed_node, log) do
    log.(%{process: -1, type: :invoke, f: :write, value: 0})

    case :erpc.call(seed_node, EKV, :put, [@ekv_name, @key, 0, [consistent: true, timeout: 30_000]]) do
      {:ok, _vsn} ->
        log.(%{process: -1, type: :ok, f: :write, value: 0})

      {:error, reason} ->
        log.(%{process: -1, type: :fail, f: :write, value: 0, error: "seed_failed: #{inspect(reason)}"})
    end
  end

  defp run_worker(_nodes, _worker, 0, _log, _counter, _every, _total), do: :ok

  defp run_worker(nodes, worker, n_ops, log, counter, every, total) do
    seed = {
      rem(System.unique_integer([:positive]), 1_000_000),
      rem(System.monotonic_time(:microsecond), 1_000_000),
      worker + 1
    }

    :rand.seed(:exsplus, seed)

    Enum.each(1..n_ops, fn i ->
      process_id = worker * 10_000_000 + i

      if :rand.uniform(100) <= 50 do
        do_read(nodes, process_id, i, log)
      else
        do_write(nodes, process_id, i, log)
      end

      maybe_report_progress(counter, every, total)
    end)
  end

  defp do_read(nodes, process_id, i, log) do
    target = random_node(nodes)
    log.(%{process: process_id, type: :invoke, f: :read, value: nil})

    try do
      value = :erpc.call(target, EKV, :get, [@ekv_name, @key, [consistent: true, timeout: 30_000]])
      log.(%{process: process_id, type: :ok, f: :read, value: value})
    rescue
      e ->
        log.(%{
          process: process_id,
          type: :fail,
          f: :read,
          value: nil,
          error: "read_failed_#{i}@#{target}: #{Exception.message(e)}"
        })
    catch
      :exit, reason ->
        log.(%{
          process: process_id,
          type: :fail,
          f: :read,
          value: nil,
          error: "read_exit_#{i}@#{target}: #{inspect(reason)}"
        })
    end
  end

  defp do_write(nodes, process_id, i, log) do
    target = random_node(nodes)
    value = process_id
    log.(%{process: process_id, type: :invoke, f: :write, value: value})

    try do
      case :erpc.call(target, EKV, :put, [@ekv_name, @key, value, [consistent: true, timeout: 30_000]]) do
        {:ok, _vsn} ->
          log.(%{process: process_id, type: :ok, f: :write, value: value})

        {:error, :unconfirmed} ->
          resolver = random_node(nodes)

          resolved =
            :erpc.call(resolver, EKV, :get, [@ekv_name, @key, [consistent: true, timeout: 30_000]])

          if resolved == value do
            log.(%{process: process_id, type: :ok, f: :write, value: value})
          else
            log.(%{
              process: process_id,
              type: :info,
              f: :write,
              value: value,
              error: "unconfirmed_not_observed@#{resolver}: resolved=#{inspect(resolved)}"
            })
          end

        {:error, reason} ->
          log.(%{
            process: process_id,
            type: :info,
            f: :write,
            value: value,
            error: "write_failed_#{i}@#{target}: #{inspect(reason)}"
          })
      end
    rescue
      e ->
        log.(%{
          process: process_id,
          type: :info,
          f: :write,
          value: value,
          error: "write_exception_#{i}@#{target}: #{Exception.message(e)}"
        })
    catch
      :exit, reason ->
        log.(%{
          process: process_id,
          type: :info,
          f: :write,
          value: value,
          error: "write_exit_#{i}@#{target}: #{inspect(reason)}"
        })
    end
  end

  defp random_node(nodes) do
    Enum.at(nodes, :rand.uniform(length(nodes)) - 1)
  end

  defp maybe_report_progress(counter, every, total) do
    n = :atomics.add_get(counter, 1, 1)

    if rem(n, every) == 0 or n == total do
      pct = Float.round(n * 100.0 / total, 1)
      IO.puts("history generation progress: #{n}/#{total} (#{pct}%)")
    end
  end

  defp safe_erpc(node, mod, fun, args) do
    try do
      :erpc.call(node, mod, fun, args)
    rescue
      e -> {:error, {:exception, Exception.message(e)}}
    catch
      :exit, reason -> {:error, {:exit, reason}}
    end
  end

  defp wait_until!(label, timeout_ms, fun) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_until(label, deadline, fun)
  end

  defp do_wait_until(label, deadline, fun) do
    if fun.() do
      :ok
    else
      if System.monotonic_time(:millisecond) >= deadline do
        raise "timeout waiting for #{label}"
      end

      Process.sleep(100)
      do_wait_until(label, deadline, fun)
    end
  end

  defp write_history!(path, history) do
    File.mkdir_p!(Path.dirname(path))

    rows =
      history
      |> Enum.map(&encode_edn/1)
      |> Enum.join("\n ")

    File.write!(path, "[\n #{rows}\n]\n")
  end

  defp encode_edn(%{} = map) do
    body =
      map
      |> Enum.map(fn {k, v} -> "#{encode_key(k)} #{encode_value(v)}" end)
      |> Enum.join(", ")

    "{#{body}}"
  end

  defp encode_key(k) when is_atom(k), do: ":#{Atom.to_string(k)}"
  defp encode_key(k) when is_binary(k), do: encode_string(k)

  defp encode_value(nil), do: "nil"
  defp encode_value(v) when is_integer(v), do: Integer.to_string(v)
  defp encode_value(v) when is_boolean(v), do: if(v, do: "true", else: "false")
  defp encode_value(v) when is_atom(v), do: ":#{Atom.to_string(v)}"
  defp encode_value(v) when is_binary(v), do: encode_string(v)
  defp encode_value(v), do: encode_string(inspect(v))

  defp encode_string(v) do
    escaped =
      v
      |> String.replace("\\", "\\\\")
      |> String.replace("\"", "\\\"")
      |> String.replace("\n", "\\n")

    "\"#{escaped}\""
  end

  def parse_mode!("none"), do: :none
  def parse_mode!("partition_flap"), do: :partition_flap
  def parse_mode!("restart_one"), do: :restart_one

  def parse_mode!(other) do
    raise "invalid mode #{inspect(other)} (expected none|partition_flap|restart_one)"
  end
end

[path, workers, total_ops, cluster_nodes, mode] =
  case System.argv() do
    [p, w, t, c, m] ->
      [p, String.to_integer(w), String.to_integer(t), String.to_integer(c), EkvJepsen.HistoryGen.parse_mode!(m)]

    [p, w, t, c] ->
      [p, String.to_integer(w), String.to_integer(t), String.to_integer(c), :none]

    [p, w, t] ->
      [p, String.to_integer(w), String.to_integer(t), 3, :none]

    [p] ->
      [p, 4, 200, 3, :none]

    _ ->
      ["jepsen/results/history.edn", 4, 200, 3, :none]
  end

EkvJepsen.HistoryGen.run(path, workers, total_ops, cluster_nodes, mode)
