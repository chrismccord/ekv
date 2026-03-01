defmodule Bench.CAS do
  import Bench.Helpers

  @name :bench
  @replica1 :"replica1@127.0.0.1"
  @replica2 :"replica2@127.0.0.1"
  @replica3 :"replica3@127.0.0.1"

  def run(opts \\ []) do
    shards = Keyword.get(opts, :shards, 8)

    IO.puts("\nEKV CAS Benchmarks")
    IO.puts("  coordinator : #{node()}")
    IO.puts("  replica1    : #{@replica1}")
    IO.puts("  replica2    : #{@replica2}")
    IO.puts("  replica3    : #{@replica3}")
    IO.puts("  shards      : #{shards}")

    wait_for_connection(@replica1)
    wait_for_connection(@replica2)
    wait_for_connection(@replica3)
    IO.puts("  status      : all nodes connected\n")

    cas_put_latency(shards)
    update_latency(shards)
    cas_throughput(shards)
    concurrent_contention(shards)
    update_contention(shards)
    cas_replication_latency(shards)
    cas_vs_lww(shards)
  end

  # ---------------------------------------------------------------------------
  # 1. CAS Put Latency — single proposer, fetch + conditional put
  # ---------------------------------------------------------------------------

  defp cas_put_latency(shards) do
    header("1. CAS Put Latency (fetch → put if_vsn)")

    with_remote_ekv(shards, fn ->
      n = 1_000

      warmup(50, fn ->
        key = "cas_warm/#{:rand.uniform(100_000)}"
        :erpc.call(@replica1, Bench.Replica, :cas_update, [@name, key, &Bench.Replica.cas_increment/1])
      end)

      # Insert-if-absent (if_vsn: nil)
      insert_samples =
        Enum.map(1..n, fn i ->
          key = "cas_insert/#{i}"
          {us, _} =
            time_us(fn ->
              {:ok, nil, nil} = :erpc.call(@replica1, Bench.Replica, :cas_fetch, [@name, key])
              :ok = :erpc.call(@replica1, Bench.Replica, :cas_put, [@name, key, %{i: i}, nil])
            end)
          us
        end)
        |> Enum.sort()

      report_latency("#{format_number(n)} insert-if-absent (fetch + put if_vsn: nil)", insert_samples)

      # Conditional update (fetch → put if_vsn: vsn)
      update_samples =
        Enum.map(1..n, fn i ->
          key = "cas_insert/#{i}"
          {us, _} =
            time_us(fn ->
              {:ok, _val, vsn} = :erpc.call(@replica1, Bench.Replica, :cas_fetch, [@name, key])
              :ok = :erpc.call(@replica1, Bench.Replica, :cas_put, [@name, key, %{i: i, v: 2}, vsn])
            end)
          us
        end)
        |> Enum.sort()

      report_latency("#{format_number(n)} conditional updates (fetch + put if_vsn: vsn)", update_samples)
    end)
  end

  # ---------------------------------------------------------------------------
  # 2. Update Latency — atomic read-modify-write
  # ---------------------------------------------------------------------------

  defp update_latency(shards) do
    header("2. Update Latency (atomic read-modify-write)")

    with_remote_ekv(shards, fn ->
      n = 1_000

      # Different keys — no contention
      no_contention_samples =
        Enum.map(1..n, fn i ->
          key = "upd/#{i}"
          {us, _} =
            time_us(fn ->
              :erpc.call(@replica1, Bench.Replica, :cas_update, [@name, key, &Bench.Replica.cas_increment/1])
            end)
          us
        end)
        |> Enum.sort()

      report_latency("#{format_number(n)} updates on distinct keys (no contention)", no_contention_samples)

      # Same key — sequential (no concurrent contention, but ballot incrementing)
      same_key_samples =
        Enum.map(1..n, fn _i ->
          {us, _} =
            time_us(fn ->
              :erpc.call(@replica1, Bench.Replica, :cas_update, [@name, "upd/single", &Bench.Replica.cas_increment/1])
            end)
          us
        end)
        |> Enum.sort()

      report_latency("#{format_number(n)} sequential updates on same key", same_key_samples)
    end)
  end

  # ---------------------------------------------------------------------------
  # 3. CAS Throughput — many keys, CAS puts from both sides
  # ---------------------------------------------------------------------------

  defp cas_throughput(shards) do
    header("3. CAS Throughput (concurrent proposers, distinct keys)")

    with_remote_ekv(shards, fn ->
      for n <- [1_000, 5_000] do
        subheader("#{format_number(n)} keys per node (#{format_number(n * 2)} total)")

        {wall_us, _} =
          time_us(fn ->
            t1 = Task.async(fn ->
              for i <- 1..n do
                :erpc.call(@replica1, Bench.Replica, :cas_update, [
                  @name, "thr_r1/#{i}", &Bench.Replica.cas_increment/1
                ])
              end
            end)

            t2 = Task.async(fn ->
              for i <- 1..n do
                :erpc.call(@replica2, Bench.Replica, :cas_update, [
                  @name, "thr_r2/#{i}", &Bench.Replica.cas_increment/1
                ])
              end
            end)

            Task.await(t1, 60_000)
            Task.await(t2, 60_000)
          end)

        report_throughput("CAS updates on distinct keys from 2 nodes", n * 2, wall_us)
      end
    end)
  end

  # ---------------------------------------------------------------------------
  # 4. Concurrent Contention — 2 proposers racing on same key
  # ---------------------------------------------------------------------------

  defp concurrent_contention(shards) do
    header("4. CAS Put Contention (2 proposers, same key)")

    with_remote_ekv(shards, fn ->
      rounds = 200

      results =
        Enum.map(1..rounds, fn i ->
          key = "contend/#{i}"
          # Seed with initial value
          :erpc.call(@replica1, Bench.Replica, :cas_update, [@name, key, &Bench.Replica.cas_increment/1])
          Process.sleep(5)

          # Both nodes fetch then try to CAS-put
          {us, {r1, r2}} =
            time_us(fn ->
              # Fetch from both sides
              {:ok, _v1, vsn1} = :erpc.call(@replica1, Bench.Replica, :cas_fetch, [@name, key])
              {:ok, _v2, vsn2} = :erpc.call(@replica2, Bench.Replica, :cas_fetch, [@name, key])

              t1 = Task.async(fn ->
                :erpc.call(@replica1, Bench.Replica, :cas_put, [@name, key, %{winner: :r1, i: i}, vsn1])
              end)
              t2 = Task.async(fn ->
                :erpc.call(@replica2, Bench.Replica, :cas_put, [@name, key, %{winner: :r2, i: i}, vsn2])
              end)

              {Task.await(t1, 10_000), Task.await(t2, 10_000)}
            end)

          {us, r1, r2}
        end)

      times = Enum.map(results, fn {us, _, _} -> us end) |> Enum.sort()
      both_ok = Enum.count(results, fn {_, r1, r2} -> r1 == :ok and r2 == :ok end)
      one_ok = Enum.count(results, fn {_, r1, r2} -> (r1 == :ok) != (r2 == :ok) end)
      neither = Enum.count(results, fn {_, r1, r2} -> r1 != :ok and r2 != :ok end)

      report_latency("#{format_number(rounds)} contested CAS rounds", times)
      IO.puts("    exactly 1 wins : #{one_ok}/#{rounds}")
      IO.puts("    both win       : #{both_ok}/#{rounds} (safety violation if > 0!)")
      IO.puts("    neither wins   : #{neither}/#{rounds} (mutual nack, safe)")
    end)
  end

  # ---------------------------------------------------------------------------
  # 5. Update Contention — 2 proposers incrementing same counter
  # ---------------------------------------------------------------------------

  defp update_contention(shards) do
    header("5. Update Contention (2 proposers, same counter)")

    with_remote_ekv(shards, fn ->
      for n <- [50, 200] do
        per_node = div(n, 2)
        subheader("#{format_number(n)} total increments (#{per_node} per node)")

        key = "counter/#{n}"

        {wall_us, _} =
          time_us(fn ->
            t1 = Task.async(fn ->
              for _ <- 1..per_node do
                :erpc.call(@replica1, Bench.Replica, :cas_update, [
                  @name, key, &Bench.Replica.cas_increment/1
                ])
              end
            end)

            t2 = Task.async(fn ->
              for _ <- 1..per_node do
                :erpc.call(@replica2, Bench.Replica, :cas_update, [
                  @name, key, &Bench.Replica.cas_increment/1
                ])
              end
            end)

            r1 = Task.await(t1, 120_000)
            r2 = Task.await(t2, 120_000)

            successes = Enum.count(r1 ++ r2, fn
              {:ok, _} -> true
              _ -> false
            end)

            conflicts = Enum.count(r1 ++ r2, fn
              {:error, :conflict} -> true
              _ -> false
            end)

            IO.puts("    successes : #{successes}")
            IO.puts("    conflicts : #{conflicts} (retries exhausted)")
          end)

        {:ok, final_val, _vsn} = :erpc.call(@replica1, Bench.Replica, :cas_fetch, [@name, key])
        report_throughput("contested counter increments", n, wall_us)
        IO.puts("    final value : #{inspect(final_val)} (expected: sum of successes)")
      end
    end)
  end

  # ---------------------------------------------------------------------------
  # 6. CAS Replication Latency — CAS put on R1, poll until visible on R2
  # ---------------------------------------------------------------------------

  defp cas_replication_latency(shards) do
    header("6. CAS Replication Latency (CAS put R1 → visible on R2)")

    with_remote_ekv(shards, fn ->
      n = 500
      watcher = :erpc.call(@replica2, Bench.Replica, :start_watcher, [@name, self()])

      warmup(50, fn ->
        key = "cas_rep_warm/#{:rand.uniform(100_000)}"
        ref = make_ref()
        send(watcher, {:watch, key, ref})
        receive do {:watching, ^ref} -> :ok end
        :erpc.call(@replica1, Bench.Replica, :cas_update, [@name, key, &Bench.Replica.cas_increment/1])
        receive do {:found, ^ref} -> :ok after 10_000 -> raise "warmup timeout" end
      end)

      samples =
        Enum.map(1..n, fn i ->
          key = "cas_rep/#{i}"
          ref = make_ref()
          send(watcher, {:watch, key, ref})
          receive do {:watching, ^ref} -> :ok end

          {us, _} =
            time_us(fn ->
              :erpc.call(@replica1, Bench.Replica, :cas_update, [@name, key, &Bench.Replica.cas_increment/1])
              receive do {:found, ^ref} -> :ok after 10_000 -> raise "replication timeout" end
            end)

          us
        end)
        |> Enum.sort()

      send(watcher, :stop)
      report_latency("#{format_number(n)} CAS updates on R1 → visible on R2", samples)
    end)
  end

  # ---------------------------------------------------------------------------
  # 7. CAS vs LWW put comparison
  # ---------------------------------------------------------------------------

  defp cas_vs_lww(shards) do
    header("7. CAS vs LWW Put Comparison")

    with_remote_ekv(shards, fn ->
      n = 1_000

      # LWW puts (fire-and-forget, no quorum)
      lww_samples =
        Enum.map(1..n, fn i ->
          key = "cmp_lww/#{i}"
          {us, _} =
            time_us(fn ->
              :erpc.call(@replica1, Bench.Replica, :single_put, [@name, key, %{i: i}])
            end)
          us
        end)
        |> Enum.sort()

      report_latency("#{format_number(n)} LWW puts (fire-and-forget)", lww_samples)

      # CAS updates (quorum round-trip)
      cas_samples =
        Enum.map(1..n, fn i ->
          key = "cmp_cas/#{i}"
          {us, _} =
            time_us(fn ->
              :erpc.call(@replica1, Bench.Replica, :cas_update, [@name, key, &Bench.Replica.cas_increment/1])
            end)
          us
        end)
        |> Enum.sort()

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

  defp with_remote_ekv(shards, fun) do
    data_dir1 = tmp_data_dir("cas_r1")
    data_dir2 = tmp_data_dir("cas_r2")
    data_dir3 = tmp_data_dir("cas_r3")

    try do
      start_ekv_on(@replica1, name: @name, data_dir: data_dir1, shards: shards,
                    cluster_size: 3, node_id: 1)
      start_ekv_on(@replica2, name: @name, data_dir: data_dir2, shards: shards,
                    cluster_size: 3, node_id: 2)
      start_ekv_on(@replica3, name: @name, data_dir: data_dir3, shards: shards,
                    cluster_size: 3, node_id: 3)
      # Let peers discover each other
      Process.sleep(500)
      fun.()
    after
      stop_ekv_on(@replica1)
      stop_ekv_on(@replica2)
      stop_ekv_on(@replica3)
      File.rm_rf(data_dir1)
      File.rm_rf(data_dir2)
      File.rm_rf(data_dir3)
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
end
