defmodule Bench.CAS do
  import Bench.Helpers

  @name :bench
  @default_remote_timeout_ms 120_000
  @default_replicas [:"replica1@127.0.0.1", :"replica2@127.0.0.1", :"replica3@127.0.0.1"]
  @all_scenarios Enum.to_list(1..9)

  def run(opts \\ []) do
    ctx = build_context(opts)
    shards = Keyword.get(opts, :shards, 8)

    IO.puts("\nEKV CAS Benchmarks")
    IO.puts("  coordinator : #{node()}")
    IO.puts("  cluster_size: #{ctx.cluster_size}")
    IO.puts("  replicas    : #{Enum.map_join(ctx.replicas, ", ", &to_string/1)}")
    IO.puts("  shards      : #{shards}")
    IO.puts("  data_root   : #{ctx.data_root}")
    IO.puts("  run_id      : #{ctx.run_id}")
    IO.puts("  mode        : #{if(ctx.quick, do: "quick", else: "full")}")
    IO.puts("  scenarios   : #{Enum.join(Enum.map(ctx.scenarios, &Integer.to_string/1), ", ")}")

    Enum.each(ctx.replicas, &wait_for_connection/1)
    IO.puts("  status      : all nodes connected\n")

    Enum.each(ctx.scenarios, fn scenario ->
      run_scenario(scenario, shards, ctx)
    end)
  end

  defp run_scenario(1, shards, ctx), do: cas_put_latency(shards, ctx)
  defp run_scenario(2, shards, ctx), do: consistent_read_latency(shards, ctx)
  defp run_scenario(3, shards, ctx), do: update_latency(shards, ctx)
  defp run_scenario(4, shards, ctx), do: parallel_cas_throughput(shards, ctx)
  defp run_scenario(5, shards, ctx), do: hot_key_contention(shards, ctx)
  defp run_scenario(6, shards, ctx), do: cas_replication_latency(shards, ctx)
  defp run_scenario(7, shards, ctx), do: config_store_simulation(shards, ctx)
  defp run_scenario(8, shards, ctx), do: session_lifecycle(shards, ctx)
  defp run_scenario(9, shards, ctx), do: cas_vs_lww(shards, ctx)

  # ---------------------------------------------------------------------------
  # 1. CAS Put Latency — single proposer, various put styles
  # ---------------------------------------------------------------------------

  defp cas_put_latency(shards, ctx) do
    header("1. CAS Put Latency (single proposer)")

    with_remote_ekv(shards, ctx, "s1", fn ->
      n = if ctx.quick, do: 300, else: 1_000
      warmup_n = if ctx.quick, do: 20, else: 50

      warmup(warmup_n, fn ->
        key = "cas_warm/#{:rand.uniform(100_000)}"

        rpc(r1(ctx), Bench.Replica, :cas_update, [
          @name,
          key,
          &Bench.Replica.cas_increment/1
        ])
      end)

      # Insert-if-absent (lookup + put if_vsn: nil)
      insert_samples =
        collect_remote_samples(n, r1(ctx), fn i ->
          key = "cas_insert/#{i}"
          nil = rpc(r1(ctx), Bench.Replica, :cas_lookup, [@name, key])
          :ok = rpc(r1(ctx), Bench.Replica, :cas_put, [@name, key, %{i: i}, nil])
        end)

      report_latency(
        "#{format_number(n)} insert-if-absent (lookup + put if_vsn: nil)",
        insert_samples
      )

      # Conditional update (lookup + put if_vsn: vsn)
      cond_samples =
        collect_remote_samples(n, r1(ctx), fn i ->
          key = "cas_insert/#{i}"
          {_val, vsn} = rpc(r1(ctx), Bench.Replica, :cas_lookup, [@name, key])
          :ok = rpc(r1(ctx), Bench.Replica, :cas_put, [@name, key, %{i: i, v: 2}, vsn])
        end)

      report_latency(
        "#{format_number(n)} conditional updates (lookup + put if_vsn: vsn)",
        cond_samples
      )

      # Consistent put (single call, no manual lookup)
      consistent_samples =
        collect_remote_samples(n, r1(ctx), fn i ->
          key = "cas_cons/#{i}"
          rpc(r1(ctx), Bench.Replica, :consistent_put, [@name, key, %{i: i}])
        end)

      report_latency(
        "#{format_number(n)} consistent puts (put consistent: true)",
        consistent_samples
      )
    end)
  end

  # ---------------------------------------------------------------------------
  # 2. Consistent Read Latency — get(consistent: true) vs eventual get
  # ---------------------------------------------------------------------------

  defp consistent_read_latency(shards, ctx) do
    header("2. Consistent Read vs Eventual Read")

    with_remote_ekv(shards, ctx, "s2", fn ->
      n = if ctx.quick, do: 300, else: 1_000
      seed = if ctx.quick, do: 200, else: 500

      # Seed data via CAS
      for i <- 1..seed do
        rpc(r1(ctx), Bench.Replica, :cas_update, [
          @name,
          "cread/#{i}",
          &Bench.Replica.cas_increment/1
        ])
      end

      Process.sleep(300)

      # Eventual reads (direct SQLite, no GenServer)
      eventual_samples =
        collect_remote_samples(n, r1(ctx), fn _i ->
          rpc(r1(ctx), Bench.Replica, :single_get, [
            @name,
            "cread/#{:rand.uniform(seed)}"
          ])
        end)

      report_latency("#{format_number(n)} eventual reads (local SQLite)", eventual_samples)

      # Consistent reads (CASPaxos round)
      consistent_samples =
        collect_remote_samples(n, r1(ctx), fn _i ->
          rpc(r1(ctx), Bench.Replica, :consistent_get, [
            @name,
            "cread/#{:rand.uniform(seed)}"
          ])
        end)

      report_latency("#{format_number(n)} consistent reads (quorum)", consistent_samples)

      ev_p50 = percentile(eventual_samples, 50)
      cas_p50 = percentile(consistent_samples, 50)
      ratio = if ev_p50 > 0, do: Float.round(cas_p50 / ev_p50, 1), else: 0
      IO.puts("\n  consistent/eventual p50 ratio: #{ratio}x")
    end)
  end

  # ---------------------------------------------------------------------------
  # 3. Update Latency — atomic read-modify-write
  # ---------------------------------------------------------------------------

  defp update_latency(shards, ctx) do
    header("3. Update Latency (atomic read-modify-write)")

    with_remote_ekv(shards, ctx, "s3", fn ->
      n = if ctx.quick, do: 300, else: 1_000

      # Different keys — no contention
      distinct_samples =
        collect_remote_samples(n, r1(ctx), fn i ->
          rpc(r1(ctx), Bench.Replica, :cas_update, [
            @name,
            "upd/#{i}",
            &Bench.Replica.cas_increment/1
          ])
        end)

      report_latency(
        "#{format_number(n)} updates on distinct keys (no contention)",
        distinct_samples
      )

      # Same key — sequential increments (ballot escalation, no concurrent conflict)
      same_key_samples =
        collect_remote_samples(n, r1(ctx), fn _i ->
          rpc(r1(ctx), Bench.Replica, :cas_update, [
            @name,
            "upd/hot",
            &Bench.Replica.cas_increment/1
          ])
        end)

      report_latency("#{format_number(n)} sequential updates on same key", same_key_samples)

      # With TTL
      ttl_samples =
        collect_remote_samples(n, r1(ctx), fn i ->
          rpc(r1(ctx), Bench.Replica, :cas_update_with_ttl, [
            @name,
            "upd_ttl/#{i}",
            &Bench.Replica.cas_increment/1,
            :timer.minutes(30)
          ])
        end)

      report_latency("#{format_number(n)} updates with TTL (30 min)", ttl_samples)
    end)
  end

  # ---------------------------------------------------------------------------
  # 4. Parallel CAS Throughput — concurrent callers, distinct keys
  # ---------------------------------------------------------------------------

  defp parallel_cas_throughput(shards, ctx) do
    header("4. Parallel CAS Throughput (concurrent callers)")

    with_remote_ekv(shards, ctx, "s4", fn ->
      proposer_count = length(ctx.replicas)

      cases =
        if ctx.quick, do: [{4, 80}, {8, 60}, {16, 40}], else: [{4, 200}, {8, 150}, {16, 100}]

      for {workers_per_node, n_per_worker} <- cases do
        total = workers_per_node * n_per_worker * proposer_count

        subheader(
          "#{workers_per_node} workers/node across #{proposer_count} proposers, " <>
            "#{format_number(total)} total updates"
        )

        {wall_us, _} =
          time_us(fn ->
            ctx.replicas
            |> Enum.with_index(1)
            |> Enum.map(fn {node, node_ix} ->
              Task.async(fn ->
                run_parallel_cas(node, workers_per_node, n_per_worker, "par_n#{node_ix}", ctx)
              end)
            end)
            |> Enum.each(&Task.await(&1, 120_000))
          end)

        report_throughput(
          "CAS updates from #{workers_per_node} workers/node across #{proposer_count} proposers",
          total,
          wall_us
        )
      end
    end)
  end

  # ---------------------------------------------------------------------------
  # 5. Hot-Key Contention — proposers racing on same key via update
  # ---------------------------------------------------------------------------

  defp hot_key_contention(shards, ctx) do
    header("5. Hot-Key Contention (update same counter)")

    with_remote_ekv(shards, ctx, "s5", fn ->
      proposer_count = length(ctx.replicas)
      cases = if ctx.quick, do: [150, 600], else: [300, 1_500]

      for n <- cases do
        per_node = div(n, proposer_count)
        remainder = rem(n, proposer_count)

        subheader(
          "#{format_number(n)} increments across #{proposer_count} proposers " <>
            "(~#{per_node}/node, concurrent)"
        )

        key = "counter/#{ctx.run_id}/#{n}"

        {wall_us, _} =
          time_us(fn ->
            all =
              ctx.replicas
              |> Enum.with_index(1)
              |> Enum.map(fn {node, ix} ->
                ops_for_node = per_node + if(ix <= remainder, do: 1, else: 0)

                Task.async(fn ->
                  for _ <- 1..ops_for_node do
                    rpc(node, Bench.Replica, :cas_update, [
                      @name,
                      key,
                      &Bench.Replica.cas_increment/1
                    ])
                  end
                end)
              end)
              |> Enum.flat_map(&Task.await(&1, 120_000))

            successes = Enum.count(all, &match?({:ok, _}, &1))
            conflicts = Enum.count(all, &match?({:error, :conflict}, &1))

            IO.puts("    successes : #{successes}")
            IO.puts("    conflicts : #{conflicts} (retries exhausted)")
          end)

        final_val = rpc(r1(ctx), Bench.Replica, :consistent_get, [@name, key])
        report_throughput("contested counter increments", n, wall_us)
        IO.puts("    final value (consistent read) : #{inspect(final_val)}")
      end
    end)
  end

  # ---------------------------------------------------------------------------
  # 6. CAS Replication Latency — CAS write on R1, poll until visible on R2
  # ---------------------------------------------------------------------------

  defp cas_replication_latency(shards, ctx) do
    header("6. CAS Replication Latency (CAS write R1 → visible on R2)")

    with_remote_ekv(shards, ctx, "s6", fn ->
      n = if ctx.quick, do: 200, else: 500
      warmup_n = if ctx.quick, do: 20, else: 50
      watcher = rpc(r2(ctx), Bench.Replica, :start_watcher, [@name, self()])

      warmup(warmup_n, fn ->
        key = "cas_rep_warm/#{:rand.uniform(100_000)}"
        ref = make_ref()
        send(watcher, {:watch, key, ref})

        receive do
          {:watching, ^ref} -> :ok
        end

        rpc(r1(ctx), Bench.Replica, :cas_update, [
          @name,
          key,
          &Bench.Replica.cas_increment/1
        ])

        receive do
          {:found, ^ref} -> :ok
        after
          10_000 -> raise "warmup timeout"
        end
      end)

      samples =
        Enum.map(1..n, fn i ->
          key = "cas_rep/#{i}"
          ref = make_ref()
          send(watcher, {:watch, key, ref})

          receive do
            {:watching, ^ref} -> :ok
          end

          {us, _} =
            time_us(fn ->
              rpc(r1(ctx), Bench.Replica, :cas_update, [
                @name,
                key,
                &Bench.Replica.cas_increment/1
              ])

              receive do
                {:found, ^ref} -> :ok
              after
                10_000 -> raise "replication timeout"
              end
            end)

          us
        end)
        |> Enum.sort()

      send(watcher, :stop)
      report_latency("#{format_number(n)} CAS updates on R1 → visible on R2", samples)
    end)
  end

  # ---------------------------------------------------------------------------
  # 7. Config Store Simulation — 90% consistent reads, 10% updates
  # ---------------------------------------------------------------------------

  defp config_store_simulation(shards, ctx) do
    header("7. Config Store Simulation (90% reads / 10% updates)")

    with_remote_ekv(shards, ctx, "s7", fn ->
      seed = if ctx.quick, do: 120, else: 200
      workers_per_node = if ctx.quick, do: 3, else: 4
      ops_per_worker = if ctx.quick, do: 80, else: 150
      proposer_count = length(ctx.replicas)
      workers = workers_per_node * proposer_count
      ops = workers * ops_per_worker

      read_opts =
        if ctx.quick, do: [retries: 20, backoff: {3, 20}], else: [retries: 12, backoff: {5, 25}]

      # Seed config keys
      for i <- 1..seed do
        rpc(r1(ctx), Bench.Replica, :cas_update, [
          @name,
          "cfg/#{i}",
          &Bench.Replica.cas_increment/1
        ])
      end

      Process.sleep(300)

      for {read_pct, label} <- [{90, "90% reads / 10% writes"}, {50, "50% reads / 50% writes"}] do
        subheader(label)

        {wall_us, _} =
          time_us(fn ->
            tasks =
              for node <- ctx.replicas, worker_ix <- 1..workers_per_node do
                Task.async(fn ->
                  for op_ix <- 1..ops_per_worker do
                    key = "cfg/#{:rand.uniform(seed)}"

                    if :rand.uniform(100) <= read_pct do
                      rpc(node, Bench.Replica, :consistent_get_result, [@name, key, read_opts])
                    else
                      rpc(node, Bench.Replica, :cas_update, [
                        @name,
                        "#{key}/n#{worker_ix}/#{op_ix}",
                        &Bench.Replica.cas_increment/1
                      ])
                    end
                  end
                end)
              end

            results = Enum.flat_map(tasks, &Task.await(&1, 120_000))
            conflicts = Enum.count(results, &match?({:error, :conflict}, &1))

            if conflicts > 0 do
              IO.puts("    conflicts : #{conflicts}")
            end
          end)

        report_throughput("#{format_number(ops)} ops across #{workers} workers", ops, wall_us)
      end
    end)
  end

  # ---------------------------------------------------------------------------
  # 8. Session Lifecycle — create → read → update → delete (full CRUD)
  # ---------------------------------------------------------------------------

  defp session_lifecycle(shards, ctx) do
    header("8. Session Lifecycle (insert → read → update → delete)")

    with_remote_ekv(shards, ctx, "s8", fn ->
      n = if ctx.quick, do: 200, else: 500
      warmup_n = if ctx.quick, do: 10, else: 20

      warmup(warmup_n, fn ->
        key = "sess_warm/#{:rand.uniform(100_000)}"
        rpc(r1(ctx), Bench.Replica, :session_lifecycle, [@name, key])
      end)

      # Full lifecycle on single node
      single_samples =
        collect_remote_samples(n, r1(ctx), fn i ->
          rpc(r1(ctx), Bench.Replica, :session_lifecycle, [
            @name,
            "sess/#{i}"
          ])
        end)

      report_latency("#{format_number(n)} full lifecycles (single node)", single_samples)

      # Lifecycle spanning 2 nodes: create on R1, read on R2, update on R2, delete on R1
      cross_samples =
        collect_remote_samples(n, r1(ctx), fn i ->
          key = "xsess/#{i}"
          # Create on R1
          :ok =
            rpc(r1(ctx), Bench.Replica, :consistent_put, [
              @name,
              key,
              %{user: i, created: true}
            ])

          # Read on R2 (consistent — sees the write)
          rpc(r2(ctx), Bench.Replica, :consistent_get, [@name, key])

          # Update on R2
          {:ok, _} =
            rpc(r2(ctx), Bench.Replica, :cas_update, [
              @name,
              key,
              &Bench.Replica.session_activate/1
            ])

          # Delete on R1
          {_val, vsn} = rpc(r1(ctx), Bench.Replica, :cas_lookup, [@name, key])
          rpc(r1(ctx), Bench.Replica, :cas_delete, [@name, key, vsn])
        end)

      report_latency("#{format_number(n)} cross-node lifecycles (R1→R2→R2→R1)", cross_samples)
    end)
  end

  # ---------------------------------------------------------------------------
  # 9. CAS vs LWW Comparison — head-to-head put latency
  # ---------------------------------------------------------------------------

  defp cas_vs_lww(shards, ctx) do
    header("9. CAS vs LWW Put Comparison")

    with_remote_ekv(shards, ctx, "s9", fn ->
      n = if ctx.quick, do: 300, else: 1_000

      # LWW puts (single GenServer call, no quorum)
      lww_samples =
        collect_remote_samples(n, r1(ctx), fn i ->
          rpc(r1(ctx), Bench.Replica, :single_put, [@name, "cmp_lww/#{i}", %{i: i}])
        end)

      report_latency("#{format_number(n)} LWW puts (fire-and-forget)", lww_samples)

      # CAS updates (prepare + accept + commit quorum)
      cas_samples =
        collect_remote_samples(n, r1(ctx), fn i ->
          rpc(r1(ctx), Bench.Replica, :cas_update, [
            @name,
            "cmp_cas/#{i}",
            &Bench.Replica.cas_increment/1
          ])
        end)

      report_latency("#{format_number(n)} CAS updates (quorum round-trip)", cas_samples)

      lww_p50 = percentile(lww_samples, 50)
      cas_p50 = percentile(cas_samples, 50)
      ratio = if lww_p50 > 0, do: Float.round(cas_p50 / lww_p50, 1), else: 0

      IO.puts("\n  CAS/LWW p50 ratio: #{ratio}x")
    end)
  end

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  defp collect_remote_samples(n, _node, fun) do
    Enum.map(1..n, fn i ->
      {us, _} = time_us(fn -> fun.(i) end)
      us
    end)
    |> Enum.sort()
  end

  defp run_parallel_cas(node, workers, n_per_worker, prefix, _ctx) do
    tasks =
      for w <- 1..workers do
        Task.async(fn ->
          for i <- 1..n_per_worker do
            rpc(node, Bench.Replica, :cas_update, [
              @name,
              "#{prefix}/w#{w}/#{i}",
              &Bench.Replica.cas_increment/1
            ])
          end
        end)
      end

    Enum.each(tasks, &Task.await(&1, 120_000))
  end

  defp with_remote_ekv(shards, ctx, scope, fun) do
    node_specs =
      Enum.with_index(ctx.replicas, 1)
      |> Enum.map(fn {node, node_id} ->
        {node, node_id, run_data_dir(ctx, scope, node_id)}
      end)

    try do
      Enum.each(node_specs, fn {node, node_id, data_dir} ->
        rpc(node, File, :rm_rf, [data_dir])
        rpc(node, File, :mkdir_p, [data_dir])

        start_ekv_on(node,
          name: @name,
          data_dir: data_dir,
          shards: shards,
          cluster_size: ctx.cluster_size,
          node_id: node_id
        )
      end)

      # Let peers discover each other and exchange connect/ack
      Process.sleep(500)
      fun.()
    after
      Enum.each(node_specs, fn {node, _node_id, data_dir} ->
        stop_ekv_on(node)
        rpc(node, File, :rm_rf, [data_dir])
      end)
    end
  end

  defp start_ekv_on(node, opts) do
    full_opts = Keyword.merge([log: false, gc_interval: :timer.hours(1)], opts)
    rpc(node, Bench.Replica, :start_ekv, [full_opts])
  end

  defp stop_ekv_on(node) do
    try do
      rpc(node, Bench.Replica, :stop_ekv, [@name])
    catch
      _, _ -> :ok
    end
  end

  defp run_data_dir(ctx, scope, node_id) do
    Path.join([ctx.data_root, "cas_runs", ctx.run_id, scope, "r#{node_id}"])
  end

  defp build_context(opts) do
    replicas =
      opts
      |> Keyword.get(:replicas, @default_replicas)
      |> Enum.map(&normalize_node/1)
      |> Enum.uniq()

    if length(replicas) < 3 do
      raise ArgumentError, "Bench.CAS requires at least 3 replicas"
    end

    cluster_size = Keyword.get(opts, :cluster_size, length(replicas))

    cond do
      cluster_size < 3 ->
        raise ArgumentError, "Bench.CAS cluster_size must be >= 3"

      cluster_size > length(replicas) ->
        raise ArgumentError,
              "Bench.CAS cluster_size (#{cluster_size}) exceeds replica count (#{length(replicas)})"

      true ->
        :ok
    end

    selected = Enum.take(replicas, cluster_size)
    [replica1, replica2, replica3 | _] = selected
    scenarios = parse_scenarios(Keyword.get(opts, :scenarios, @all_scenarios))
    quick = Keyword.get(opts, :quick, false)
    run_id = Keyword.get(opts, :run_id, default_run_id())
    data_root = Keyword.get(opts, :data_root, default_data_root())

    %{
      replica1: replica1,
      replica2: replica2,
      replica3: replica3,
      replicas: selected,
      cluster_size: cluster_size,
      scenarios: scenarios,
      quick: quick,
      run_id: run_id,
      data_root: data_root
    }
  end

  defp parse_scenarios(raw) do
    raw
    |> List.wrap()
    |> Enum.map(fn
      n when is_integer(n) and n in 1..9 ->
        n

      s when is_binary(s) ->
        case Integer.parse(s) do
          {n, ""} when n in 1..9 -> n
          _ -> raise ArgumentError, "invalid scenario #{inspect(s)}"
        end

      other ->
        raise ArgumentError, "invalid scenario #{inspect(other)}"
    end)
    |> Enum.uniq()
    |> Enum.sort()
    |> case do
      [] -> raise ArgumentError, "at least one scenario must be selected"
      scenarios -> scenarios
    end
  end

  defp default_run_id do
    ts = System.system_time(:millisecond)
    rand = Base.url_encode64(:crypto.strong_rand_bytes(6), padding: false)
    "#{ts}-#{rand}"
  end

  defp default_data_root do
    if File.dir?("/data") do
      "/data/ekv_bench"
    else
      "/tmp/ekv_bench"
    end
  end

  defp normalize_node(node) when is_atom(node), do: node
  defp normalize_node(node) when is_binary(node), do: String.to_atom(node)
  defp normalize_node(node), do: raise(ArgumentError, "invalid node #{inspect(node)}")

  defp r1(%{replica1: node}), do: node
  defp r2(%{replica2: node}), do: node

  defp rpc(node, module, func, args) do
    :erpc.call(node, module, func, args, @default_remote_timeout_ms)
  end

  defp wait_for_connection(node, attempts \\ 50) do
    if Node.connect(node) do
      :ok
    else
      if attempts > 0 do
        Process.sleep(200)
        wait_for_connection(node, attempts - 1)
      else
        raise "Could not connect to #{node}"
      end
    end
  end
end
