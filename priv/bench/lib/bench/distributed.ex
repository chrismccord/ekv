defmodule Bench.Distributed do
  import Bench.Helpers

  @name :bench
  @replica1 :"replica1@127.0.0.1"
  @replica2 :"replica2@127.0.0.1"

  def run(opts \\ []) do
    shards = Keyword.get(opts, :shards, 8)

    IO.puts("\nEKV Distributed Benchmarks")
    IO.puts("  coordinator : #{node()}")
    IO.puts("  replica1    : #{@replica1}")
    IO.puts("  replica2    : #{@replica2}")
    IO.puts("  shards      : #{shards}")

    wait_for_connection(@replica1)
    wait_for_connection(@replica2)
    IO.puts("  status      : all nodes connected\n")

    replication_latency(shards)
    bulk_sync(shards)
    concurrent_cross_node_writes(shards)
    delete_replication(shards)
    partition_and_heal(shards)
    value_size_replication(shards)
    busy_simulation(shards)
    subscribe_replication_latency(shards)
    subscribe_overhead_distributed(shards)
  end

  # ---------------------------------------------------------------------------
  # 1. Replication latency: put on replica1, poll until visible on replica2
  # ---------------------------------------------------------------------------

  defp replication_latency(shards) do
    header("1. Replication Latency (put → visible on peer)")

    with_remote_ekv(shards, fn ->
      n = 1_000
      watcher = :erpc.call(@replica2, Bench.Replica, :start_watcher, [@name, self()])

      warmup(50, fn ->
        key = "warm/#{:rand.uniform(10_000)}"
        ref = make_ref()
        send(watcher, {:watch, key, ref})
        receive do {:watching, ^ref} -> :ok end
        :erpc.call(@replica1, Bench.Replica, :single_put, [@name, key, :warmup])
        receive do {:found, ^ref} -> :ok after 10_000 -> raise "warmup timeout" end
      end)

      samples =
        Enum.map(1..n, fn i ->
          key = "rep/#{i}"
          ref = make_ref()
          send(watcher, {:watch, key, ref})
          receive do {:watching, ^ref} -> :ok end

          {us, _} =
            time_us(fn ->
              :erpc.call(@replica1, Bench.Replica, :single_put, [@name, key, %{i: i}])
              receive do {:found, ^ref} -> :ok after 10_000 -> raise "replication timeout" end
            end)

          us
        end)
        |> Enum.sort()

      send(watcher, :stop)
      report_latency("#{format_number(n)} puts on replica1 → visible on replica2", samples)
    end)
  end

  # ---------------------------------------------------------------------------
  # 2. Bulk sync: new peer catches up with existing data
  # ---------------------------------------------------------------------------

  defp bulk_sync(shards) do
    header("2. Bulk Sync (new peer catches up)")

    for n <- [1_000, 10_000] do
      subheader("#{format_number(n)} keys")

      # Start only replica1 with data
      data_dir1 = tmp_data_dir("bulk_sync_r1")
      data_dir2 = tmp_data_dir("bulk_sync_r2")

      try do
        start_ekv_on(@replica1, name: @name, data_dir: data_dir1, shards: shards)
        :erpc.call(@replica1, Bench.Replica, :bulk_put, [@name, n, "bulk/"])
        :erpc.call(@replica1, Bench.Replica, :flush_shards, [@name])

        # Now start replica2 and measure sync time
        {wall_us, _} =
          time_us(fn ->
            start_ekv_on(@replica2, name: @name, data_dir: data_dir2, shards: shards)

            poll_until(
              fn -> :erpc.call(@replica2, Bench.Replica, :count_keys, [@name, "bulk/"]) == n end,
              30_000
            )
          end)

        keys_sec = if wall_us > 0, do: trunc(n / (wall_us / 1_000_000)), else: 0

        IO.puts("  sync time : #{format_number(trunc(wall_us / 1000))} ms")
        IO.puts("  keys/sec  : #{format_number(keys_sec)}")
      after
        stop_ekv_on(@replica1)
        stop_ekv_on(@replica2)
        File.rm_rf(data_dir1)
        File.rm_rf(data_dir2)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # 3. Concurrent cross-node writes
  # ---------------------------------------------------------------------------

  defp concurrent_cross_node_writes(shards) do
    header("3. Concurrent Cross-Node Writes")

    with_remote_ekv(shards, fn ->
      n = 5_000
      total = n * 2

      {wall_us, _} =
        time_us(fn ->
          t1 = Task.async(fn -> :erpc.call(@replica1, Bench.Replica, :bulk_put, [@name, n, "r1/"]) end)
          t2 = Task.async(fn -> :erpc.call(@replica2, Bench.Replica, :bulk_put, [@name, n, "r2/"]) end)
          Task.await(t1, 30_000)
          Task.await(t2, 30_000)

          # Wait for convergence
          poll_until(
            fn ->
              c1 = :erpc.call(@replica1, Bench.Replica, :count_keys, [@name, "r1/"]) +
                   :erpc.call(@replica1, Bench.Replica, :count_keys, [@name, "r2/"])
              c2 = :erpc.call(@replica2, Bench.Replica, :count_keys, [@name, "r1/"]) +
                   :erpc.call(@replica2, Bench.Replica, :count_keys, [@name, "r2/"])
              c1 == total and c2 == total
            end,
            30_000
          )
        end)

      report_throughput("#{format_number(total)} keys (#{format_number(n)} per node), both converged", total, wall_us)
    end)
  end

  # ---------------------------------------------------------------------------
  # 4. Delete replication
  # ---------------------------------------------------------------------------

  defp delete_replication(shards) do
    header("4. Delete Replication")

    with_remote_ekv(shards, fn ->
      for n <- [1_000, 5_000] do
        subheader("#{format_number(n)} deletes")

        :erpc.call(@replica1, Bench.Replica, :bulk_put, [@name, n, "delsync#{n}/"])

        poll_until(
          fn -> :erpc.call(@replica2, Bench.Replica, :count_keys, [@name, "delsync#{n}/"]) == n end,
          15_000
        )

        {wall_us, _} =
          time_us(fn ->
            :erpc.call(@replica1, Bench.Replica, :bulk_delete, [@name, n, "delsync#{n}/"])

            poll_until(
              fn -> :erpc.call(@replica2, Bench.Replica, :count_keys, [@name, "delsync#{n}/"]) == 0 end,
              15_000
            )
          end)

        ops_sec = if wall_us > 0, do: trunc(n / (wall_us / 1_000_000)), else: 0
        IO.puts("  delete + converge : #{format_number(trunc(wall_us / 1000))} ms")
        IO.puts("  deletes/sec       : #{format_number(ops_sec)}")
      end
    end)
  end

  # ---------------------------------------------------------------------------
  # 5. Partition and heal
  # ---------------------------------------------------------------------------

  defp partition_and_heal(shards) do
    header("5. Network Partition & Heal")

    with_remote_ekv(shards, fn ->
      n = 2_000

      # Write initial data on both sides
      :erpc.call(@replica1, Bench.Replica, :bulk_put, [@name, n, "pre/"])
      poll_until(fn -> :erpc.call(@replica2, Bench.Replica, :count_keys, [@name, "pre/"]) == n end, 15_000)

      # Partition: disconnect replica2 from replica1
      IO.puts("\n  Partitioning replica2 from replica1...")
      :erpc.call(@replica2, :erlang, :disconnect_node, [@replica1])
      Process.sleep(500)

      # Write on both sides during partition
      writes_per_side = 1_000
      t1 = Task.async(fn -> :erpc.call(@replica1, Bench.Replica, :bulk_put, [@name, writes_per_side, "part_r1/"]) end)
      t2 = Task.async(fn -> :erpc.call(@replica2, Bench.Replica, :bulk_put, [@name, writes_per_side, "part_r2/"]) end)
      Task.await(t1, 15_000)
      Task.await(t2, 15_000)

      # Verify isolation
      r1_sees_r2 = :erpc.call(@replica1, Bench.Replica, :count_keys, [@name, "part_r2/"])
      r2_sees_r1 = :erpc.call(@replica2, Bench.Replica, :count_keys, [@name, "part_r1/"])
      IO.puts("  during partition: replica1 sees #{r1_sees_r2} of replica2's keys (expected 0)")
      IO.puts("  during partition: replica2 sees #{r2_sees_r1} of replica1's keys (expected 0)")

      # Heal partition and measure convergence time
      IO.puts("  Healing partition...")
      {heal_us, _} =
        time_us(fn ->
          :erpc.call(@replica2, Node, :connect, [@replica1])

          poll_until(
            fn ->
              r1_r1 = :erpc.call(@replica1, Bench.Replica, :count_keys, [@name, "part_r1/"])
              r1_r2 = :erpc.call(@replica1, Bench.Replica, :count_keys, [@name, "part_r2/"])
              r2_r1 = :erpc.call(@replica2, Bench.Replica, :count_keys, [@name, "part_r1/"])
              r2_r2 = :erpc.call(@replica2, Bench.Replica, :count_keys, [@name, "part_r2/"])

              r1_r1 == writes_per_side and r1_r2 == writes_per_side and
                r2_r1 == writes_per_side and r2_r2 == writes_per_side
            end,
            30_000
          )
        end)

      report_sync("convergence after heal (#{format_number(writes_per_side * 2)} keys to sync)", heal_us)
    end)
  end

  # ---------------------------------------------------------------------------
  # 6. Value size replication
  # ---------------------------------------------------------------------------

  defp value_size_replication(shards) do
    header("6. Value Size Replication Latency")

    with_remote_ekv(shards, fn ->
      watcher = :erpc.call(@replica2, Bench.Replica, :start_watcher, [@name, self()])

      for {label, size} <- [{"64 B", 64}, {"1 KB", 1024}, {"10 KB", 10_240}, {"100 KB", 102_400}] do
        n = 500
        value = :crypto.strong_rand_bytes(size)

        samples =
          Enum.map(1..n, fn i ->
            key = "vsz#{size}/#{i}"
            ref = make_ref()
            send(watcher, {:watch, key, ref})
            receive do {:watching, ^ref} -> :ok end

            {us, _} =
              time_us(fn ->
                :erpc.call(@replica1, Bench.Replica, :single_put, [@name, key, value])
                receive do {:found, ^ref} -> :ok after 10_000 -> raise "replication timeout" end
              end)

            us
          end)
          |> Enum.sort()

        report_latency("#{label} x #{format_number(n)}", samples)
      end

      send(watcher, :stop)
    end)
  end

  # ---------------------------------------------------------------------------
  # 7. Busy app simulation
  # ---------------------------------------------------------------------------

  defp busy_simulation(shards) do
    header("7. Busy App Simulation")

    with_remote_ekv(shards, fn ->
      workers = 10
      keys_per_worker = 200
      churn_rounds = 5
      total_keys = workers * keys_per_worker * 2  # both replicas

      IO.puts("  workers/node  : #{workers}")
      IO.puts("  keys/worker   : #{keys_per_worker}")
      IO.puts("  churn rounds  : #{churn_rounds}")
      IO.puts("")

      {total_us, _} =
        time_us(fn ->
          # Phase 1: Initial load on both replicas
          {load_us, _} =
            time_us(fn ->
              t1 = Task.async(fn ->
                run_workers(@replica1, workers, keys_per_worker, "r1")
              end)
              t2 = Task.async(fn ->
                run_workers(@replica2, workers, keys_per_worker, "r2")
              end)
              Task.await(t1, 30_000)
              Task.await(t2, 30_000)

              # Wait for convergence
              poll_until(
                fn ->
                  c1 = :erpc.call(@replica1, Bench.Replica, :count_all_keys, [@name])
                  c2 = :erpc.call(@replica2, Bench.Replica, :count_all_keys, [@name])
                  c1 == total_keys and c2 == total_keys
                end,
                30_000
              )
            end)

          report_throughput("initial load + converge", total_keys, load_us)

          # Phase 2: Churn — delete and re-write keys
          for round <- 1..churn_rounds do
            {churn_us, _} =
              time_us(fn ->
                churn_n = div(keys_per_worker, 2)

                t1 = Task.async(fn ->
                  run_churn(@replica1, workers, churn_n, "r1", round)
                end)
                t2 = Task.async(fn ->
                  run_churn(@replica2, workers, churn_n, "r2", round)
                end)
                Task.await(t1, 30_000)
                Task.await(t2, 30_000)

                # Wait for convergence after churn
                poll_until(
                  fn ->
                    c1 = :erpc.call(@replica1, Bench.Replica, :count_all_keys, [@name])
                    c2 = :erpc.call(@replica2, Bench.Replica, :count_all_keys, [@name])
                    c1 == c2
                  end,
                  30_000
                )
              end)

            ops = workers * div(keys_per_worker, 2) * 2 * 2  # delete + put, both nodes
            report_throughput("churn round #{round} (#{format_number(ops)} ops + converge)", ops, churn_us)
          end

          # Phase 3: Read verification
          {read_us, _} =
            time_us(fn ->
              reads = 10_000

              1..reads
              |> Task.async_stream(
                fn _ ->
                  key_i = :rand.uniform(keys_per_worker)
                  prefix = if(:rand.uniform(2) == 1, do: "r1", else: "r2")
                  worker_i = :rand.uniform(workers)
                  node = if(:rand.uniform(2) == 1, do: @replica1, else: @replica2)
                  :erpc.call(node, Bench.Replica, :single_get, [@name, "#{prefix}/w#{worker_i}/#{key_i}"])
                end,
                max_concurrency: 20,
                ordered: false
              )
              |> Stream.run()
            end)

          report_throughput("cross-node reads", 10_000, read_us)
        end)

      IO.puts("\n  total wall time : #{format_number(trunc(total_us / 1000))} ms")

      # Final consistency check
      c1 = :erpc.call(@replica1, Bench.Replica, :count_all_keys, [@name])
      c2 = :erpc.call(@replica2, Bench.Replica, :count_all_keys, [@name])
      IO.puts("  final keys      : replica1=#{c1}, replica2=#{c2}, match=#{c1 == c2}")
    end)
  end

  # ---------------------------------------------------------------------------
  # 8. Subscribe replication event latency
  # ---------------------------------------------------------------------------

  defp subscribe_replication_latency(shards) do
    header("8. Subscribe Replication Event Latency (put on R1 → event on R2)")

    with_remote_ekv(shards, fn ->
      n = 1_000

      # Start a subscriber on replica2 that forwards event signals to coordinator
      sub = :erpc.call(@replica2, Bench.Replica, :start_event_subscriber, [@name, "sub_lat/", self()])

      Process.sleep(100)

      # Warm up
      warmup(50, fn ->
        key = "sub_lat/warm/#{:rand.uniform(10_000)}"
        :erpc.call(@replica1, Bench.Replica, :single_put, [@name, key, :warmup])
        receive do {:sub_event, _, _} -> :ok after 5_000 -> :ok end
      end)

      # Drain any leftover warmup events
      drain_mailbox()

      # Measure round-trip on coordinator: put on R1 → replication → subscriber on R2 → message back
      # All timing done on coordinator's monotonic clock (no cross-node clock comparison)
      samples =
        Enum.map(1..n, fn i ->
          t0 = System.monotonic_time(:microsecond)
          :erpc.call(@replica1, Bench.Replica, :single_put, [@name, "sub_lat/#{i}", %{i: i}])

          receive do
            {:sub_event, _remote_time, _count} ->
              System.monotonic_time(:microsecond) - t0
          after
            10_000 -> raise "subscribe replication event timeout"
          end
        end)
        |> Enum.sort()

      send(sub, :stop)
      report_latency("#{format_number(n)} puts on R1 → event signal back to coordinator", samples)
    end)
  end

  # ---------------------------------------------------------------------------
  # 9. Subscribe overhead on distributed writes
  # ---------------------------------------------------------------------------

  defp subscribe_overhead_distributed(shards) do
    header("9. Subscribe Overhead on Distributed Writes")

    n = 2_000

    for {label, sub_count} <- [{"0 subscribers", 0}, {"10 subscribers", 10}, {"100 subscribers", 100}] do
      with_remote_ekv(shards, fn ->
        # Spawn drain subscribers on replica2
        subs =
          for _ <- 1..sub_count//1 do
            :erpc.call(@replica2, Bench.Replica, :start_drain_subscriber, [@name, "sub_dist/"])
          end

        if sub_count > 0, do: Process.sleep(50)

        # Measure replication throughput: put on replica1, wait for convergence on replica2
        {wall_us, _} =
          time_us(fn ->
            :erpc.call(@replica1, Bench.Replica, :bulk_put, [@name, n, "sub_dist/"])

            poll_until(
              fn -> :erpc.call(@replica2, Bench.Replica, :count_keys, [@name, "sub_dist/"]) == n end,
              30_000
            )
          end)

        report_throughput("#{label}: #{format_number(n)} puts R1 → converge R2", n, wall_us)

        Enum.each(subs, fn pid -> send(pid, :stop) end)
      end)
    end
  end

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  defp run_workers(node, workers, keys_per_worker, prefix) do
    tasks =
      for w <- 1..workers do
        Task.async(fn ->
          :erpc.call(node, Bench.Replica, :bulk_put, [
            @name,
            keys_per_worker,
            "#{prefix}/w#{w}/"
          ])
        end)
      end

    Enum.each(tasks, &Task.await(&1, 30_000))
  end

  defp run_churn(node, workers, churn_n, prefix, round) do
    tasks =
      for w <- 1..workers do
        Task.async(fn ->
          # Delete some keys
          for i <- 1..churn_n do
            :erpc.call(node, Bench.Replica, :single_put, [
              @name,
              "#{prefix}/w#{w}/#{i}",
              nil
            ])
          end

          # Re-write with new values
          for i <- 1..churn_n do
            :erpc.call(node, Bench.Replica, :single_put, [
              @name,
              "#{prefix}/w#{w}/#{i}",
              %{round: round, worker: w}
            ])
          end
        end)
      end

    Enum.each(tasks, &Task.await(&1, 30_000))
  end

  defp with_remote_ekv(shards, fun) do
    data_dir1 = tmp_data_dir("r1")
    data_dir2 = tmp_data_dir("r2")

    try do
      start_ekv_on(@replica1, name: @name, data_dir: data_dir1, shards: shards)
      start_ekv_on(@replica2, name: @name, data_dir: data_dir2, shards: shards)
      # Let peers discover each other
      Process.sleep(500)
      fun.()
    after
      stop_ekv_on(@replica1)
      stop_ekv_on(@replica2)
      File.rm_rf(data_dir1)
      File.rm_rf(data_dir2)
    end
  end

  defp start_ekv_on(node, opts) do
    full_opts = Keyword.merge([log: false, gc_interval: :timer.hours(1)], opts)
    :erpc.call(node, Bench.Replica, :start_ekv, [full_opts])
  end

  defp stop_ekv_on(node) do
    try do
      :erpc.call(node, Bench.Replica, :stop_ekv, [@name])
    catch
      _, _ -> :ok
    end
  end

  defp tmp_data_dir(suffix) do
    "/tmp/ekv_bench_#{suffix}_#{System.unique_integer([:positive])}"
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

  defp poll_until(fun, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_poll(fun, deadline)
  end

  defp drain_mailbox do
    receive do
      _ -> drain_mailbox()
    after
      0 -> :ok
    end
  end

  defp do_poll(fun, deadline) do
    if fun.() do
      :ok
    else
      if System.monotonic_time(:millisecond) >= deadline do
        raise "poll_until timed out"
      end

      Process.sleep(5)
      do_poll(fun, deadline)
    end
  end
end
