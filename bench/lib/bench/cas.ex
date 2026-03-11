defmodule Bench.CAS do
  import Bench.Helpers

  @name :bench
  @default_remote_timeout_ms 120_000
  @default_replicas [:"replica1@127.0.0.1", :"replica2@127.0.0.1", :"replica3@127.0.0.1"]
  @all_scenarios Enum.to_list(1..10)

  def run(opts \\ []) do
    ctx = build_context(opts)
    shards = Keyword.get(opts, :shards, 8)

    IO.puts("\nEKV CAS Benchmarks")
    IO.puts("  coordinator : #{node()}")
    IO.puts("  cluster_size: #{ctx.cluster_size}")
    IO.puts("  members     : #{Enum.map_join(ctx.members, ", ", &to_string/1)}")

    if ctx.clients != [] do
      IO.puts("  clients     : #{Enum.map_join(ctx.clients, ", ", &to_string/1)}")
      IO.puts("  requesters  : #{Enum.map_join(ctx.request_nodes, ", ", &to_string/1)}")

      Enum.each(ctx.client_specs, fn spec ->
        IO.puts(
          "    route #{spec.region} #{to_string(spec.node)} -> #{Enum.join(spec.region_routing, ", ")}"
        )
      end)
    end

    IO.puts("  shards      : #{shards}")
    IO.puts("  data_root   : #{ctx.data_root}")
    IO.puts("  run_id      : #{ctx.run_id}")
    IO.puts("  mode        : #{if(ctx.quick, do: "quick", else: "full")}")
    IO.puts("  scenarios   : #{Enum.join(Enum.map(ctx.scenarios, &Integer.to_string/1), ", ")}")

    Enum.each(ctx.request_nodes, &wait_for_connection/1)
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
  defp run_scenario(10, shards, ctx), do: large_payload_puts(shards, ctx)

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

        {:ok, _, _} =
          rpc(r1(ctx), Bench.Replica, :cas_update, [
            @name,
            key,
            &Bench.Replica.cas_increment/1
          ])
      end)

      # Insert-if-absent (lookup + put if_vsn: nil)
      insert_samples =
        collect_remote_samples(n, requester(ctx), fn i ->
          key = "cas_insert/#{i}"
          nil = rpc(requester(ctx), Bench.Replica, :cas_lookup, [@name, key])
          {:ok, _} = rpc(requester(ctx), Bench.Replica, :cas_put, [@name, key, %{i: i}, nil])
        end)

      report_latency(
        "#{format_number(n)} insert-if-absent (lookup + put if_vsn: nil)",
        insert_samples
      )

      # Conditional update (lookup + put if_vsn: vsn)
      cond_samples =
        collect_remote_samples(n, requester(ctx), fn i ->
          key = "cas_insert/#{i}"
          {_val, vsn} = rpc(requester(ctx), Bench.Replica, :cas_lookup, [@name, key])

          {:ok, _} =
            rpc(requester(ctx), Bench.Replica, :cas_put, [@name, key, %{i: i, v: 2}, vsn])
        end)

      report_latency(
        "#{format_number(n)} conditional updates (lookup + put if_vsn: vsn)",
        cond_samples
      )

      # Consistent put (single call, no manual lookup)
      consistent_samples =
        collect_remote_samples(n, requester(ctx), fn i ->
          key = "cas_cons/#{i}"
          {:ok, _} = rpc(requester(ctx), Bench.Replica, :consistent_put, [@name, key, %{i: i}])
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
        {:ok, _, _} =
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
      {consistent_samples, read_error_counts} =
        collect_remote_samples_with_results(n, r1(ctx), fn _i ->
          rpc(r1(ctx), Bench.Replica, :consistent_get_result, [
            @name,
            "cread/#{:rand.uniform(seed)}"
          ])
        end)

      report_latency("#{format_number(n)} consistent reads (quorum)", consistent_samples)

      print_error_counts(read_error_counts, "    read_errors")

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
        collect_remote_samples(n, requester(ctx), fn i ->
          {:ok, _, _} =
            rpc(requester(ctx), Bench.Replica, :cas_update, [
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
        collect_remote_samples(n, requester(ctx), fn _i ->
          {:ok, _, _} =
            rpc(requester(ctx), Bench.Replica, :cas_update, [
              @name,
              "upd/hot",
              &Bench.Replica.cas_increment/1
            ])
        end)

      report_latency("#{format_number(n)} sequential updates on same key", same_key_samples)

      # With TTL
      ttl_samples =
        collect_remote_samples(n, requester(ctx), fn i ->
          {:ok, _, _} =
            rpc(requester(ctx), Bench.Replica, :cas_update_with_ttl, [
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
      requester_count = length(ctx.request_nodes)
      base_ops_per_node = if ctx.quick, do: 2_048, else: 4_096
      min_ops_per_worker = if ctx.quick, do: 8, else: 16

      worker_tiers =
        if ctx.quick do
          [64, 128, 256, 512, 1024, 1536, 2048]
        else
          [64, 128, 256, 512, 1024, 1536, 2048]
        end

      for workers_per_node <- worker_tiers do
        ops_per_node = max(base_ops_per_node, workers_per_node * min_ops_per_worker)
        total = ops_per_node * requester_count

        subheader(
          "#{workers_per_node} workers/node across #{requester_count} request nodes, " <>
            "#{format_number(ops_per_node)} ops/node (#{min_ops_per_worker} ops/worker floor) " <>
            "(#{format_number(total)} total updates)"
        )

        {wall_us, summaries} =
          time_us(fn ->
            ctx.request_nodes
            |> Enum.with_index(1)
            |> Enum.map(fn {node, node_ix} ->
              Task.async(fn ->
                run_parallel_cas(node, workers_per_node, ops_per_node, "par_n#{node_ix}")
              end)
            end)
            |> Enum.map(&Task.await(&1, 240_000))
          end)

        attempted = Enum.sum(Enum.map(summaries, & &1.attempted))
        succeeded = Enum.sum(Enum.map(summaries, & &1.ok))

        error_counts =
          summaries
          |> Enum.map(& &1.errors)
          |> Enum.reduce(%{}, &merge_error_counts/2)

        if map_size(error_counts) > 0 do
          IO.puts("    successes : #{format_number(succeeded)}")
          IO.puts("    failures  : #{format_number(attempted - succeeded)}")
          print_error_counts(error_counts)
        end

        report_throughput(
          "CAS updates from #{workers_per_node} workers/node across #{requester_count} request nodes",
          succeeded,
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
      requester_count = length(ctx.request_nodes)
      cases = if ctx.quick, do: [150, 600], else: [300, 1_500]

      for n <- cases do
        per_node = div(n, requester_count)
        remainder = rem(n, requester_count)

        subheader(
          "#{format_number(n)} increments across #{requester_count} request nodes " <>
            "(~#{per_node}/node, concurrent)"
        )

        key = "counter/#{ctx.run_id}/#{n}"

        {wall_us, _} =
          time_us(fn ->
            all =
              ctx.request_nodes
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

            successes = Enum.count(all, &match?({:ok, _, _}, &1))
            error_counts = count_errors(all)
            conflicts = Map.get(error_counts, :conflict, 0)
            unconfirmed = Map.get(error_counts, :unconfirmed, 0)

            IO.puts("    successes : #{successes}")
            IO.puts("    conflicts : #{conflicts} (retries exhausted)")

            if unconfirmed > 0,
              do: IO.puts("    unconfirmed : #{unconfirmed} (accept outcome ambiguous)")

            print_error_counts(Map.drop(error_counts, [:conflict, :unconfirmed]))
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

        {:ok, _, _} =
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
              {:ok, _, _} =
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
      requester_count = length(ctx.request_nodes)
      workers = workers_per_node * requester_count
      ops = workers * ops_per_worker

      read_opts =
        if ctx.quick, do: [retries: 20, backoff: {3, 20}], else: [retries: 12, backoff: {5, 25}]

      # Seed config keys
      for i <- 1..seed do
        {:ok, _, _} =
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
              for node <- ctx.request_nodes, worker_ix <- 1..workers_per_node do
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
            error_counts = count_errors(results)
            conflicts = Map.get(error_counts, :conflict, 0)
            unconfirmed = Map.get(error_counts, :unconfirmed, 0)

            if conflicts > 0 do
              IO.puts("    conflicts : #{conflicts}")
            end

            if unconfirmed > 0 do
              IO.puts("    unconfirmed : #{unconfirmed}")
            end

            print_error_counts(Map.drop(error_counts, [:conflict, :unconfirmed]))
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
        collect_remote_samples(n, requester(ctx), fn i ->
          rpc(requester(ctx), Bench.Replica, :session_lifecycle, [
            @name,
            "sess/#{i}"
          ])
        end)

      report_latency("#{format_number(n)} full lifecycles (single node)", single_samples)

      # Lifecycle spanning 2 nodes: create on R1, read on R2, update on R2, delete on R1
      cross_samples =
        collect_remote_samples(n, requester(ctx), fn i ->
          key = "xsess/#{i}"
          # Create on R1
          {:ok, _} =
            rpc(requester(ctx), Bench.Replica, :consistent_put, [
              @name,
              key,
              %{user: i, created: true}
            ])

          # Read on node2 (consistent — sees the write)
          rpc(cross_node(ctx), Bench.Replica, :consistent_get, [@name, key])

          # Update on node2
          {:ok, _, updated_vsn} =
            rpc(cross_node(ctx), Bench.Replica, :cas_update, [
              @name,
              key,
              &Bench.Replica.session_activate/1
            ])

          # Delete on node1
          {:ok, _} = rpc(requester(ctx), Bench.Replica, :cas_delete, [@name, key, updated_vsn])
        end)

      report_latency(
        "#{format_number(n)} cross-node lifecycles (node1→node2→node2→node1)",
        cross_samples
      )
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
        collect_remote_samples(n, requester(ctx), fn i ->
          rpc(requester(ctx), Bench.Replica, :single_put, [@name, "cmp_lww/#{i}", %{i: i}])
        end)

      report_latency("#{format_number(n)} LWW puts (fire-and-forget)", lww_samples)

      # CAS updates (prepare + accept + commit quorum)
      cas_samples =
        collect_remote_samples(n, requester(ctx), fn i ->
          {:ok, _, _} =
            rpc(requester(ctx), Bench.Replica, :cas_update, [
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
  # 10. Large Payload Put Latency — compare wire compression off vs on
  # ---------------------------------------------------------------------------

  defp large_payload_puts(shards, ctx) do
    header("10. Large Payload Put Latency")

    payload_target = if ctx.quick, do: 1_000_000, else: 4_000_000
    requester_node = requester(ctx)
    n = if ctx.quick, do: 30, else: 75

    for {label, member_opts} <- [
          {"wire compression disabled", [wire_compression_threshold: false]},
          {"wire compression enabled", [wire_compression_threshold: 256 * 1024]}
        ] do
      with_remote_ekv(shards, ctx, "s10_#{slug(label)}", member_opts, [], fn ->
        payload_label = "payload/#{ctx.run_id}/#{slug(label)}"

        payload_info =
          rpc(requester_node, Bench.Replica, :prepare_large_payload, [
            payload_label,
            payload_target
          ])

        IO.puts("  --- #{label} ---")
        IO.puts("    raw bytes  : #{format_number(payload_info.raw_bytes)}")
        IO.puts("    wire bytes : #{format_number(payload_info.wire_bytes)}")
        IO.puts("    ratio      : #{payload_info.compression_ratio}x")

        lww_samples =
          collect_remote_samples(n, requester_node, fn i ->
            rpc(requester_node, Bench.Replica, :single_put_prepared_payload, [
              @name,
              "large_lww/#{slug(label)}/#{i}",
              payload_label
            ])
          end)

        report_latency("#{format_number(n)} LWW puts (prepared large payload)", lww_samples)

        cas_samples =
          collect_remote_samples(n, requester_node, fn i ->
            {:ok, _} =
              rpc(requester_node, Bench.Replica, :consistent_put_prepared_payload, [
                @name,
                "large_cas/#{slug(label)}/#{i}",
                payload_label
              ])
          end)

        report_latency(
          "#{format_number(n)} consistent puts (prepared large payload)",
          cas_samples
        )

        rpc(requester_node, Bench.Replica, :clear_large_payload, [payload_label])
      end)
    end
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

  defp collect_remote_samples_with_results(n, _node, fun) do
    {samples, error_counts} =
      Enum.reduce(1..n, {[], %{}}, fn i, {acc_samples, acc_errors} ->
        {us, result} = time_us(fn -> fun.(i) end)
        {[us | acc_samples], accumulate_error(acc_errors, result)}
      end)

    {Enum.sort(samples), error_counts}
  end

  defp count_errors(results) do
    Enum.reduce(results, %{}, fn result, acc ->
      accumulate_error(acc, result)
    end)
  end

  defp accumulate_error(error_counts, {:error, reason}) do
    Map.update(error_counts, reason, 1, &(&1 + 1))
  end

  defp accumulate_error(error_counts, _result), do: error_counts

  defp print_error_counts(error_counts, label_prefix \\ "    errors") do
    if map_size(error_counts) > 0 do
      error_counts
      |> Enum.sort_by(fn {reason, _count} -> inspect(reason) end)
      |> Enum.each(fn {reason, count} ->
        IO.puts("#{label_prefix}[#{inspect(reason)}] : #{count}")
      end)
    end
  end

  defp run_parallel_cas(node, workers, total_ops, prefix) do
    rpc(node, Bench.Replica, :run_parallel_cas_batch, [@name, workers, total_ops, prefix])
  end

  defp merge_error_counts(left, right) do
    Map.merge(left, right, fn _reason, a, b -> a + b end)
  end

  defp with_remote_ekv(shards, ctx, scope, fun) do
    with_remote_ekv(shards, ctx, scope, [], [], fun)
  end

  defp with_remote_ekv(shards, ctx, scope, member_extra_opts, client_extra_opts, fun) do
    member_specs =
      Enum.with_index(ctx.member_specs, 1)
      |> Enum.map(fn {spec, node_id} ->
        Map.merge(spec, %{node_id: node_id, data_dir: run_data_dir(ctx, scope, node_id)})
      end)

    try do
      Enum.each(member_specs, fn %{node: node, data_dir: data_dir} ->
        rpc(node, File, :rm_rf, [data_dir])
        rpc(node, File, :mkdir_p, [data_dir])
      end)

      start_many_ekv(member_specs, fn spec ->
        start_ekv_on(
          spec.node,
          Keyword.merge(
            [
              mode: :member,
              name: @name,
              data_dir: spec.data_dir,
              region: spec.region,
              shards: shards,
              cluster_size: ctx.cluster_size,
              node_id: spec.node_id,
              wait_for_quorum: 30_000
            ],
            member_extra_opts
          )
        )
      end)

      Enum.each(ctx.client_specs, fn spec ->
        start_ekv_on(
          spec.node,
          Keyword.merge(
            [
              mode: :client,
              name: @name,
              region: spec.region,
              region_routing: spec.region_routing,
              wait_for_route: 30_000,
              wait_for_quorum: 30_000
            ],
            client_extra_opts
          )
        )
      end)

      fun.()
    after
      Enum.each(ctx.client_specs, fn spec ->
        stop_ekv_on(spec.node)
      end)

      Enum.each(member_specs, fn %{node: node, data_dir: data_dir} ->
        stop_ekv_on(node)
        rpc(node, File, :rm_rf, [data_dir])
      end)
    end
  end

  defp start_ekv_on(node, opts) do
    defaults =
      case Keyword.get(opts, :mode, :member) do
        :client -> [log: false]
        _member -> [log: false, gc_interval: :timer.hours(1)]
      end

    full_opts = Keyword.merge(defaults, opts)
    rpc(node, Bench.Replica, :start_ekv, [full_opts])
  end

  defp start_many_ekv(specs, starter) do
    specs
    |> Task.async_stream(
      fn spec -> starter.(spec) end,
      ordered: false,
      timeout: :infinity,
      max_concurrency: length(specs)
    )
    |> Enum.each(fn
      {:ok, {:ok, _pid}} -> :ok
      {:ok, other} -> raise "unexpected EKV start result: #{inspect(other)}"
      {:exit, reason} -> exit(reason)
    end)
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
    {member_specs, client_specs, cluster_size} =
      case {Keyword.get(opts, :members), Keyword.get(opts, :clients)} do
        {nil, nil} ->
          replicas =
            opts
            |> Keyword.get(:replicas, @default_replicas)
            |> Enum.map(&normalize_node/1)
            |> Enum.uniq()

          if length(replicas) < 3 do
            raise ArgumentError, "Bench.CAS requires at least 3 member nodes"
          end

          legacy_cluster_size = Keyword.get(opts, :cluster_size, length(replicas))

          cond do
            legacy_cluster_size < 3 ->
              raise ArgumentError, "Bench.CAS cluster_size must be >= 3"

            legacy_cluster_size > length(replicas) ->
              raise ArgumentError,
                    "Bench.CAS cluster_size (#{legacy_cluster_size}) exceeds replica count (#{length(replicas)})"

            true ->
              :ok
          end

          selected = Enum.take(replicas, legacy_cluster_size)

          {
            Enum.map(selected, fn node -> %{node: node, region: "default"} end),
            [],
            legacy_cluster_size
          }

        {members, clients} ->
          member_specs = normalize_member_specs(members || [])
          client_specs = normalize_client_specs(clients || [])

          if length(member_specs) < 3 do
            raise ArgumentError, "Bench.CAS requires at least 3 member nodes"
          end

          overlapping =
            MapSet.intersection(
              MapSet.new(Enum.map(member_specs, & &1.node)),
              MapSet.new(Enum.map(client_specs, & &1.node))
            )

          if MapSet.size(overlapping) > 0 do
            raise ArgumentError,
                  "Bench.CAS member/client overlap: #{Enum.map_join(overlapping, ", ", &inspect/1)}"
          end

          cluster_size = Keyword.get(opts, :cluster_size, length(member_specs))

          if cluster_size != length(member_specs) do
            raise ArgumentError,
                  "Bench.CAS cluster_size (#{cluster_size}) must equal member count (#{length(member_specs)}) when members are provided explicitly"
          end

          {member_specs, client_specs, cluster_size}
      end

    members = Enum.map(member_specs, & &1.node)
    clients = Enum.map(client_specs, & &1.node)
    request_nodes = members ++ clients
    [member1, member2, member3 | _] = members
    scenarios = parse_scenarios(Keyword.get(opts, :scenarios, @all_scenarios))
    quick = Keyword.get(opts, :quick, false)
    run_id = Keyword.get(opts, :run_id, default_run_id())
    data_root = Keyword.get(opts, :data_root, default_data_root())

    %{
      member1: member1,
      member2: member2,
      member3: member3,
      members: members,
      clients: clients,
      request_nodes: request_nodes,
      member_specs: member_specs,
      client_specs: client_specs,
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
      n when is_integer(n) and n in 1..10 ->
        n

      s when is_binary(s) ->
        case Integer.parse(s) do
          {n, ""} when n in 1..10 -> n
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

  defp normalize_member_specs(specs) do
    specs
    |> Enum.map(fn
      %{node: node, region: region} ->
        %{node: normalize_node(node), region: normalize_region(region)}

      spec when is_list(spec) ->
        %{
          node: normalize_node(Keyword.fetch!(spec, :node)),
          region: normalize_region(Keyword.fetch!(spec, :region))
        }

      other ->
        raise ArgumentError, "invalid member spec #{inspect(other)}"
    end)
    |> uniq_specs(:member)
  end

  defp normalize_client_specs(specs) do
    specs
    |> Enum.map(fn
      %{node: node, region: region, region_routing: region_routing} ->
        %{
          node: normalize_node(node),
          region: normalize_region(region),
          region_routing: normalize_region_routing(region_routing)
        }

      spec when is_list(spec) ->
        %{
          node: normalize_node(Keyword.fetch!(spec, :node)),
          region: normalize_region(Keyword.fetch!(spec, :region)),
          region_routing: normalize_region_routing(Keyword.fetch!(spec, :region_routing))
        }

      other ->
        raise ArgumentError, "invalid client spec #{inspect(other)}"
    end)
    |> uniq_specs(:client)
  end

  defp uniq_specs(specs, label) do
    specs
    |> Enum.uniq_by(& &1.node)
    |> case do
      [] -> []
      uniq when length(uniq) == length(specs) -> uniq
      _ -> raise ArgumentError, "duplicate #{label} nodes are not allowed"
    end
  end

  defp normalize_region(region) do
    region = region |> to_string() |> String.trim()

    if region == "" do
      raise ArgumentError, "region must be a non-empty string"
    else
      region
    end
  end

  defp normalize_region_routing(region_routing) do
    region_routing
    |> List.wrap()
    |> Enum.map(&normalize_region/1)
    |> Enum.uniq()
    |> case do
      [] -> raise ArgumentError, "region_routing must include at least one region"
      routes -> routes
    end
  end

  defp r1(%{member1: node}), do: node
  defp r2(%{member2: node}), do: node
  defp requester(%{clients: [node | _]}), do: node
  defp requester(%{member1: node}), do: node
  defp cross_node(%{request_nodes: [_first, second | _]}), do: second
  defp cross_node(%{member2: node}), do: node
  defp slug(label), do: label |> String.downcase() |> String.replace(~r/[^a-z0-9]+/u, "_")

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
