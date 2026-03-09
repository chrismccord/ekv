defmodule EkvJepsen.HistoryGen do
  @moduledoc false

  @ekv_name :jepsen_kv
  @key "jepsen/register"
  @cas_timeout 30_000

  @valid_modes [:none, :partition_flap, :restart_one, :partition_restart]
  @valid_profiles [:register, :lock]

  def run(path, workers, total_ops, cluster_nodes, mode, profile, seed)
      when workers > 0 and total_ops > 0 and cluster_nodes >= 3 and mode in @valid_modes and
             profile in @valid_profiles and is_integer(seed) do
    ensure_distributed!()
    run_nonce = "#{seed}-#{System.unique_integer([:positive])}"

    IO.puts(
      "starting #{cluster_nodes}-node peer cluster (mode=#{mode}, profile=#{profile}, seed=#{seed})..."
    )

    {peers, nodes} = start_peers(cluster_nodes)
    IO.puts("peer nodes: #{Enum.map_join(nodes, ", ", &Atom.to_string/1)}")

    IO.puts("starting EKV on peer nodes...")
    cluster_meta = start_ekv_cluster(nodes, run_nonce)

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

    nemesis_pids = start_nemesis(mode, nodes, cluster_meta)

    try do
      seed_node = hd(nodes)
      seed_initial_value(seed_node, log, profile)

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
          run_worker(
            nodes,
            worker,
            n_ops,
            log,
            progress_counter,
            progress_every,
            total_ops,
            profile,
            seed
          )
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

      write_history!(path, history)
      IO.puts("history written: #{path}")
    after
      Agent.stop(events)
      stop_nemesis(nemesis_pids)
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
      _ = safe_erpc(node, Node, :connect, [other])
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

  defp start_ekv_cluster(nodes, run_nonce) do
    cluster_size = length(nodes)

    Enum.with_index(nodes, 1)
    |> Enum.map(fn {node, idx} ->
      data_dir = "/tmp/ekv_jepsen_#{run_nonce}_#{idx}"

      opts = [
        name: @ekv_name,
        data_dir: data_dir,
        shards: 8,
        log: false,
        gc_interval: :timer.hours(1),
        cluster_size: cluster_size,
        node_id: "jepsen-#{run_nonce}-node-#{idx}"
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
            length(info.connected_members) == cluster_size - 1
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

  defp start_nemesis(:none, _nodes, _cluster_meta), do: []

  defp start_nemesis(:partition_flap, nodes, _cluster_meta) do
    IO.puts("starting nemesis: partition_flap")
    [spawn_link(fn -> partition_flap_loop(nodes, 0) end)]
  end

  defp start_nemesis(:restart_one, nodes, cluster_meta) do
    IO.puts("starting nemesis: restart_one")
    [spawn_link(fn -> restart_one_loop(nodes, cluster_meta, 0) end)]
  end

  defp start_nemesis(:partition_restart, nodes, cluster_meta) do
    IO.puts("starting nemesis: partition_flap + restart_one")

    [
      spawn_link(fn -> partition_flap_loop(nodes, 0) end),
      spawn_link(fn -> restart_one_loop(nodes, cluster_meta, 0) end)
    ]
  end

  defp stop_nemesis([]), do: :ok

  defp stop_nemesis(pids) when is_list(pids) do
    Enum.each(pids, fn pid ->
      if is_pid(pid) and Process.alive?(pid), do: send(pid, :stop)
    end)

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
        %{connected_members: peers} -> length(peers) >= cluster_size - 1
        _ -> false
      end
    end)
  end

  defp seed_initial_value(seed_node, log, :register) do
    log.(%{process: -1, type: :invoke, f: :write, value: 0})

    case :erpc.call(seed_node, EKV, :put, [@ekv_name, @key, 0, [consistent: true, timeout: @cas_timeout]]) do
      {:ok, _vsn} ->
        log.(%{process: -1, type: :ok, f: :write, value: 0})

      {:error, reason} ->
        log.(%{process: -1, type: :fail, f: :write, value: 0, error: "seed_failed: #{inspect(reason)}"})
    end
  end

  defp seed_initial_value(seed_node, _log, :lock) do
    ensure_lock_key_nil!(seed_node, 15)
  end

  defp ensure_lock_key_nil!(_seed_node, 0) do
    raise "failed to establish nil baseline for lock key"
  end

  defp ensure_lock_key_nil!(seed_node, attempts_left) do
    _delete_result =
      safe_erpc(seed_node, EKV, :delete, [@ekv_name, @key, [consistent: true, timeout: @cas_timeout]])

    case safe_erpc(seed_node, EKV, :get, [@ekv_name, @key, [consistent: true, timeout: @cas_timeout]]) do
      nil ->
        :ok

      _other ->
        Process.sleep(100)
        ensure_lock_key_nil!(seed_node, attempts_left - 1)
    end
  end

  defp run_worker(_nodes, _worker, 0, _log, _counter, _every, _total, _profile, _seed), do: :ok

  defp run_worker(nodes, worker, n_ops, log, counter, every, total, profile, seed) do
    seed_worker(seed, worker)

    case profile do
      :register ->
        run_worker_register(nodes, worker, n_ops, log, counter, every, total)

      :lock ->
        run_worker_lock(nodes, worker, n_ops, log, counter, every, total)
    end
  end

  defp run_worker_register(nodes, worker, n_ops, log, counter, every, total) do
    Enum.each(1..n_ops, fn i ->
      process_id = worker * 10_000_000 + i

      if :rand.uniform(100) <= 50 do
        do_register_read(nodes, process_id, i, log)
      else
        do_register_write(nodes, process_id, i, log)
      end

      maybe_report_progress(counter, every, total)
    end)
  end

  defp run_worker_lock(nodes, worker, n_ops, log, counter, every, total) do
    initial = %{
      owner: "owner-#{worker}",
      token: nil,
      vsn: nil
    }

    _state =
      Enum.reduce(1..n_ops, initial, fn i, state ->
        process_id = worker * 10_000_000 + i

        next_state =
          if state.vsn == nil do
            if :rand.uniform(100) <= 65 do
              do_lock_acquire(nodes, process_id, i, log, state)
            else
              do_lock_read(nodes, process_id, i, log, state)
            end
          else
            case :rand.uniform(100) do
              n when n <= 45 -> do_lock_renew(nodes, process_id, i, log, state)
              n when n <= 70 -> do_lock_release(nodes, process_id, i, log, state)
              _ -> do_lock_read(nodes, process_id, i, log, state)
            end
          end

        maybe_report_progress(counter, every, total)
        next_state
      end)

    :ok
  end

  defp do_register_read(nodes, process_id, i, log) do
    target = random_node(nodes)
    log.(%{process: process_id, type: :invoke, f: :read, value: nil})

    try do
      value = :erpc.call(target, EKV, :get, [@ekv_name, @key, [consistent: true, timeout: @cas_timeout]])
      log.(%{process: process_id, type: :ok, f: :read, value: value})
    rescue
      e ->
        log.(%{process: process_id, type: :fail, f: :read, value: nil, error: "read_failed_#{i}@#{target}: #{Exception.message(e)}"})
    catch
      :exit, reason ->
        log.(%{process: process_id, type: :fail, f: :read, value: nil, error: "read_exit_#{i}@#{target}: #{inspect(reason)}"})
    end
  end

  defp do_register_write(nodes, process_id, i, log) do
    target = random_node(nodes)
    value = process_id * 1_000_000 + i
    log.(%{process: process_id, type: :invoke, f: :write, value: value})

    try do
      case :erpc.call(target, EKV, :put, [@ekv_name, @key, value, [consistent: true, timeout: @cas_timeout]]) do
        {:ok, _vsn} ->
          log.(%{process: process_id, type: :ok, f: :write, value: value})

        {:error, :unconfirmed} ->
          resolver = random_node(nodes)
          resolved = :erpc.call(resolver, EKV, :get, [@ekv_name, @key, [consistent: true, timeout: @cas_timeout]])

          if resolved == value do
            log.(%{process: process_id, type: :ok, f: :write, value: value})
          else
            log.(%{process: process_id, type: :info, f: :write, value: value, error: "unconfirmed_not_observed@#{resolver}: resolved=#{inspect(resolved)}"})
          end

        {:error, reason} ->
          log.(%{process: process_id, type: :info, f: :write, value: value, error: "write_failed_#{i}@#{target}: #{inspect(reason)}"})
      end
    rescue
      e ->
        log.(%{process: process_id, type: :info, f: :write, value: value, error: "write_exception_#{i}@#{target}: #{Exception.message(e)}"})
    catch
      :exit, reason ->
        log.(%{process: process_id, type: :info, f: :write, value: value, error: "write_exit_#{i}@#{target}: #{inspect(reason)}"})
    end
  end

  defp do_lock_read(nodes, process_id, i, log, state) do
    target = random_node(nodes)
    log.(%{process: process_id, type: :invoke, f: :read, value: nil})

    try do
      value = :erpc.call(target, EKV, :get, [@ekv_name, @key, [consistent: true, timeout: @cas_timeout]])
      owner = lock_owner(value)
      log.(%{process: process_id, type: :ok, f: :read, value: owner})

      if state.vsn != nil and owner != state.owner do
        %{state | token: nil, vsn: nil}
      else
        state
      end
    rescue
      e ->
        log.(%{process: process_id, type: :fail, f: :read, value: nil, error: "lock_read_failed_#{i}@#{target}: #{Exception.message(e)}"})
        state
    catch
      :exit, reason ->
        log.(%{process: process_id, type: :fail, f: :read, value: nil, error: "lock_read_exit_#{i}@#{target}: #{inspect(reason)}"})
        state
    end
  end

  defp do_lock_acquire(nodes, process_id, i, log, state) do
    target = random_node(nodes)
    token = "acq/#{state.owner}/#{i}/#{System.unique_integer([:positive])}"
    lock_value = %{owner: state.owner, token: token}
    cas_value = [nil, state.owner]

    log.(%{process: process_id, type: :invoke, f: :cas, value: cas_value})

    try do
      case :erpc.call(target, EKV, :put, [@ekv_name, @key, lock_value, [if_vsn: nil, timeout: @cas_timeout]]) do
        {:ok, vsn} ->
          log.(%{process: process_id, type: :ok, f: :cas, value: cas_value})
          %{state | token: token, vsn: vsn}

        {:error, :conflict} ->
          log.(%{process: process_id, type: :fail, f: :cas, value: cas_value})
          state

        {:error, :unconfirmed} ->
          resolve_unconfirmed_acquire(nodes, process_id, i, log, state, token, cas_value)

        {:error, reason} ->
          log.(%{
            process: process_id,
            type: :info,
            f: :cas,
            value: cas_value,
            error: "acquire_failed_#{i}@#{target}: #{inspect(reason)}"
          })

          %{state | token: nil, vsn: nil}
      end
    rescue
      e ->
        log.(%{
          process: process_id,
          type: :info,
          f: :cas,
          value: cas_value,
          error: "acquire_exception_#{i}@#{target}: #{Exception.message(e)}"
        })

        %{state | token: nil, vsn: nil}
    catch
      :exit, reason ->
        log.(%{
          process: process_id,
          type: :info,
          f: :cas,
          value: cas_value,
          error: "acquire_exit_#{i}@#{target}: #{inspect(reason)}"
        })

        %{state | token: nil, vsn: nil}
    end
  end

  defp do_lock_renew(nodes, process_id, i, log, state) do
    target = random_node(nodes)
    new_token = "ren/#{state.owner}/#{i}/#{System.unique_integer([:positive])}"
    lock_value = %{owner: state.owner, token: new_token}
    cas_value = [state.owner, state.owner]

    log.(%{process: process_id, type: :invoke, f: :cas, value: cas_value})

    try do
      case :erpc.call(target, EKV, :put, [@ekv_name, @key, lock_value, [if_vsn: state.vsn, timeout: @cas_timeout]]) do
        {:ok, vsn} ->
          log.(%{process: process_id, type: :ok, f: :cas, value: cas_value})
          %{state | token: new_token, vsn: vsn}

        {:error, :conflict} ->
          log.(%{process: process_id, type: :fail, f: :cas, value: cas_value})
          %{state | token: nil, vsn: nil}

        {:error, :unconfirmed} ->
          resolve_unconfirmed_renew(nodes, process_id, i, log, state, new_token, cas_value)

        {:error, reason} ->
          log.(%{
            process: process_id,
            type: :info,
            f: :cas,
            value: cas_value,
            error: "renew_failed_#{i}@#{target}: #{inspect(reason)}"
          })

          %{state | token: nil, vsn: nil}
      end
    rescue
      e ->
        log.(%{
          process: process_id,
          type: :info,
          f: :cas,
          value: cas_value,
          error: "renew_exception_#{i}@#{target}: #{Exception.message(e)}"
        })

        %{state | token: nil, vsn: nil}
    catch
      :exit, reason ->
        log.(%{
          process: process_id,
          type: :info,
          f: :cas,
          value: cas_value,
          error: "renew_exit_#{i}@#{target}: #{inspect(reason)}"
        })

        %{state | token: nil, vsn: nil}
    end
  end

  defp do_lock_release(nodes, process_id, i, log, state) do
    target = random_node(nodes)
    cas_value = [state.owner, nil]

    log.(%{process: process_id, type: :invoke, f: :cas, value: cas_value})

    try do
      case :erpc.call(target, EKV, :delete, [@ekv_name, @key, [if_vsn: state.vsn, timeout: @cas_timeout]]) do
        {:ok, _vsn} ->
          log.(%{process: process_id, type: :ok, f: :cas, value: cas_value})
          %{state | token: nil, vsn: nil}

        {:error, :conflict} ->
          log.(%{process: process_id, type: :fail, f: :cas, value: cas_value})
          %{state | token: nil, vsn: nil}

        {:error, :unconfirmed} ->
          resolve_unconfirmed_release(nodes, process_id, i, log, state, cas_value)

        {:error, reason} ->
          log.(%{
            process: process_id,
            type: :info,
            f: :cas,
            value: cas_value,
            error: "release_failed_#{i}@#{target}: #{inspect(reason)}"
          })

          %{state | token: nil, vsn: nil}
      end
    rescue
      e ->
        log.(%{
          process: process_id,
          type: :info,
          f: :cas,
          value: cas_value,
          error: "release_exception_#{i}@#{target}: #{Exception.message(e)}"
        })

        %{state | token: nil, vsn: nil}
    catch
      :exit, reason ->
        log.(%{
          process: process_id,
          type: :info,
          f: :cas,
          value: cas_value,
          error: "release_exit_#{i}@#{target}: #{inspect(reason)}"
        })

        %{state | token: nil, vsn: nil}
    end
  end

  defp resolve_unconfirmed_acquire(nodes, process_id, i, log, state, token, cas_value) do
    resolver = random_node(nodes)

    try do
      resolved = :erpc.call(resolver, EKV, :get, [@ekv_name, @key, [consistent: true, timeout: @cas_timeout]])

      case resolved do
        %{owner: owner, token: ^token} when owner == state.owner ->
          log.(%{process: process_id, type: :ok, f: :cas, value: cas_value})

          case recover_vsn_for_token(nodes, token, 8) do
            {:ok, vsn} -> %{state | token: token, vsn: vsn}
            :error -> %{state | token: nil, vsn: nil}
          end

        %{owner: _owner, token: _other} ->
          log.(%{
            process: process_id,
            type: :info,
            f: :cas,
            value: cas_value,
            error: "acquire_unconfirmed_not_matched_#{i}@#{resolver}"
          })

          %{state | token: nil, vsn: nil}

        nil ->
          log.(%{
            process: process_id,
            type: :info,
            f: :cas,
            value: cas_value,
            error: "acquire_unconfirmed_nil_#{i}@#{resolver}"
          })

          %{state | token: nil, vsn: nil}

        other ->
          log.(%{
            process: process_id,
            type: :info,
            f: :cas,
            value: cas_value,
            error: "acquire_unconfirmed_unknown_#{i}@#{resolver}: #{inspect(other)}"
          })

          %{state | token: nil, vsn: nil}
      end
    rescue
      e ->
        log.(%{
          process: process_id,
          type: :info,
          f: :cas,
          value: cas_value,
          error: "acquire_unconfirmed_exception_#{i}@#{resolver}: #{Exception.message(e)}"
        })

        %{state | token: nil, vsn: nil}
    catch
      :exit, reason ->
        log.(%{
          process: process_id,
          type: :info,
          f: :cas,
          value: cas_value,
          error: "acquire_unconfirmed_exit_#{i}@#{resolver}: #{inspect(reason)}"
        })

        %{state | token: nil, vsn: nil}
    end
  end

  defp resolve_unconfirmed_renew(nodes, process_id, i, log, state, new_token, cas_value) do
    resolver = random_node(nodes)

    try do
      resolved = :erpc.call(resolver, EKV, :get, [@ekv_name, @key, [consistent: true, timeout: @cas_timeout]])

      case resolved do
        %{owner: owner, token: ^new_token} when owner == state.owner ->
          log.(%{process: process_id, type: :ok, f: :cas, value: cas_value})

          case recover_vsn_for_token(nodes, new_token, 8) do
            {:ok, vsn} -> %{state | token: new_token, vsn: vsn}
            :error -> %{state | token: nil, vsn: nil}
          end

        %{owner: owner, token: token} when owner == state.owner ->
          log.(%{
            process: process_id,
            type: :info,
            f: :cas,
            value: cas_value,
            error: "renew_unconfirmed_old_token_#{i}@#{resolver}: #{token}"
          })

          case recover_vsn_for_token(nodes, token, 8) do
            {:ok, vsn} -> %{state | token: token, vsn: vsn}
            :error -> %{state | token: nil, vsn: nil}
          end

        _other ->
          log.(%{
            process: process_id,
            type: :info,
            f: :cas,
            value: cas_value,
            error: "renew_unconfirmed_not_matched_#{i}@#{resolver}"
          })

          %{state | token: nil, vsn: nil}
      end
    rescue
      e ->
        log.(%{
          process: process_id,
          type: :info,
          f: :cas,
          value: cas_value,
          error: "renew_unconfirmed_exception_#{i}@#{resolver}: #{Exception.message(e)}"
        })

        %{state | token: nil, vsn: nil}
    catch
      :exit, reason ->
        log.(%{
          process: process_id,
          type: :info,
          f: :cas,
          value: cas_value,
          error: "renew_unconfirmed_exit_#{i}@#{resolver}: #{inspect(reason)}"
        })

        %{state | token: nil, vsn: nil}
    end
  end

  defp resolve_unconfirmed_release(nodes, process_id, i, log, state, cas_value) do
    resolver = random_node(nodes)

    try do
      resolved = :erpc.call(resolver, EKV, :get, [@ekv_name, @key, [consistent: true, timeout: @cas_timeout]])

      case resolved do
        nil ->
          log.(%{process: process_id, type: :ok, f: :cas, value: cas_value})
          %{state | token: nil, vsn: nil}

        %{owner: owner, token: token} when owner == state.owner ->
          log.(%{
            process: process_id,
            type: :info,
            f: :cas,
            value: cas_value,
            error: "release_unconfirmed_owner_still_present_#{i}@#{resolver}: #{token}"
          })

          case recover_vsn_for_token(nodes, token, 8) do
            {:ok, vsn} -> %{state | token: token, vsn: vsn}
            :error -> %{state | token: nil, vsn: nil}
          end

        _other ->
          log.(%{
            process: process_id,
            type: :info,
            f: :cas,
            value: cas_value,
            error: "release_unconfirmed_not_matched_#{i}@#{resolver}"
          })

          %{state | token: nil, vsn: nil}
      end
    rescue
      e ->
        log.(%{
          process: process_id,
          type: :info,
          f: :cas,
          value: cas_value,
          error: "release_unconfirmed_exception_#{i}@#{resolver}: #{Exception.message(e)}"
        })

        %{state | token: nil, vsn: nil}
    catch
      :exit, reason ->
        log.(%{
          process: process_id,
          type: :info,
          f: :cas,
          value: cas_value,
          error: "release_unconfirmed_exit_#{i}@#{resolver}: #{inspect(reason)}"
        })

        %{state | token: nil, vsn: nil}
    end
  end

  defp recover_vsn_for_token(_nodes, _token, 0), do: :error

  defp recover_vsn_for_token(nodes, token, attempts) do
    found =
      Enum.find_value(nodes, fn node ->
        case safe_erpc(node, EKV, :lookup, [@ekv_name, @key]) do
          {%{token: ^token}, vsn} ->
            vsn

          _ ->
            nil
        end
      end)

    if found do
      {:ok, found}
    else
      Process.sleep(50)
      recover_vsn_for_token(nodes, token, attempts - 1)
    end
  end

  defp lock_owner(%{owner: owner}) when is_binary(owner), do: owner
  defp lock_owner(_), do: nil

  defp seed_worker(seed_base, worker) do
    h = :erlang.phash2({seed_base, worker}, 2_147_483_647)
    a = rem(h, 30_269) + 1
    b = rem(div(h, 30_269), 30_307) + 1
    c = rem(div(h, 30_269 * 30_307), 30_323) + 1
    :rand.seed(:exsplus, {a, b, c})
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
  defp encode_value(v) when is_list(v), do: "[#{Enum.map_join(v, " ", &encode_value/1)}]"
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
  def parse_mode!("partition_restart"), do: :partition_restart

  def parse_mode!(other) do
    raise "invalid mode #{inspect(other)} (expected none|partition_flap|restart_one|partition_restart)"
  end

  def parse_profile!("register"), do: :register
  def parse_profile!("lock"), do: :lock

  def parse_profile!(other) do
    raise "invalid profile #{inspect(other)} (expected register|lock)"
  end
end

[path, workers, total_ops, cluster_nodes, mode, profile, seed] =
  case System.argv() do
    [p, w, t, c, m, pr, s] ->
      [
        p,
        String.to_integer(w),
        String.to_integer(t),
        String.to_integer(c),
        EkvJepsen.HistoryGen.parse_mode!(m),
        EkvJepsen.HistoryGen.parse_profile!(pr),
        String.to_integer(s)
      ]

    [p, w, t, c, m, pr] ->
      [
        p,
        String.to_integer(w),
        String.to_integer(t),
        String.to_integer(c),
        EkvJepsen.HistoryGen.parse_mode!(m),
        EkvJepsen.HistoryGen.parse_profile!(pr),
        1
      ]

    [p, w, t, c, m] ->
      [p, String.to_integer(w), String.to_integer(t), String.to_integer(c), EkvJepsen.HistoryGen.parse_mode!(m), :register, 1]

    [p, w, t, c] ->
      [p, String.to_integer(w), String.to_integer(t), String.to_integer(c), :none, :register, 1]

    [p, w, t] ->
      [p, String.to_integer(w), String.to_integer(t), 3, :none, :register, 1]

    [p] ->
      [p, 4, 200, 3, :none, :register, 1]

    _ ->
      ["jepsen/results/history.edn", 4, 200, 3, :none, :register, 1]
  end

EkvJepsen.HistoryGen.run(path, workers, total_ops, cluster_nodes, mode, profile, seed)
