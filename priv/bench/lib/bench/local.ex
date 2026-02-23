defmodule Bench.Local do
  import Bench.Helpers

  @name :bench

  def run(opts \\ []) do
    shards = Keyword.get(opts, :shards, System.schedulers_online())

    IO.puts("\nEKV Local Benchmarks")
    IO.puts("  schedulers: #{System.schedulers_online()}")
    IO.puts("  shards:     #{shards}")

    get_throughput(shards)
    put_throughput(shards)
    put_get_mixed(shards)
    delete_throughput(shards)
    prefix_scan_throughput(shards)
    ttl_put_throughput(shards)
    value_size_scaling(shards)
    shard_scaling()
    subscribe_overhead(shards)
    subscribe_fan_out(shards)
    subscribe_single_key(shards)
    subscribe_event_latency(shards)
  end

  # ---------------------------------------------------------------------------
  # 1. Get throughput (SQLite read path, no GenServer)
  # ---------------------------------------------------------------------------

  defp get_throughput(shards) do
    header("1. Get Throughput (SQLite read path, no GenServer)")

    with_ekv([name: @name, shards: shards], fn ->
      n = 10_000
      reads = 100_000

      # Seed data
      for i <- 1..n, do: EKV.put(@name, "key/#{i}", %{i: i})

      warmup(1_000, fn -> EKV.get(@name, "key/#{:rand.uniform(n)}") end)

      # Sequential reads
      samples = collect_samples(reads, fn -> EKV.get(@name, "key/#{:rand.uniform(n)}") end)
      report_latency("sequential (#{format_number(reads)} reads over #{format_number(n)} keys)", samples)

      # Parallel reads
      {wall_us, _} =
        time_us(fn ->
          1..reads
          |> Task.async_stream(
            fn _ -> EKV.get(@name, "key/#{:rand.uniform(n)}") end,
            max_concurrency: System.schedulers_online() * 2,
            ordered: false
          )
          |> Stream.run()
        end)

      report_throughput("parallel (#{System.schedulers_online() * 2} workers)", reads, wall_us)
    end)
  end

  # ---------------------------------------------------------------------------
  # 2. Put throughput
  # ---------------------------------------------------------------------------

  defp put_throughput(shards) do
    header("2. Put Throughput (GenServer call per write)")

    with_ekv([name: @name, shards: shards], fn ->
      n = 50_000

      # Sequential puts
      samples = collect_samples(n, fn -> EKV.put(@name, "seq/#{:rand.uniform(n)}", :ok) end)
      report_latency("sequential (#{format_number(n)} puts)", samples)

      # Parallel puts
      {wall_us, _} =
        time_us(fn ->
          1..n
          |> Task.async_stream(
            fn i -> EKV.put(@name, "par/#{i}", :ok) end,
            max_concurrency: System.schedulers_online() * 2,
            ordered: false
          )
          |> Stream.run()
        end)

      report_throughput("parallel (#{System.schedulers_online() * 2} workers)", n, wall_us)
    end)
  end

  # ---------------------------------------------------------------------------
  # 3. Mixed put/get workload (80% reads, 20% writes)
  # ---------------------------------------------------------------------------

  defp put_get_mixed(shards) do
    header("3. Mixed Workload (80% reads, 20% writes)")

    with_ekv([name: @name, shards: shards], fn ->
      seed = 5_000
      ops = 100_000

      for i <- 1..seed, do: EKV.put(@name, "mix/#{i}", %{i: i})

      {wall_us, _} =
        time_us(fn ->
          1..ops
          |> Task.async_stream(
            fn _ ->
              if :rand.uniform(100) <= 80 do
                EKV.get(@name, "mix/#{:rand.uniform(seed)}")
              else
                EKV.put(@name, "mix/#{:rand.uniform(seed)}", %{updated: true})
              end
            end,
            max_concurrency: System.schedulers_online() * 2,
            ordered: false
          )
          |> Stream.run()
        end)

      report_throughput("#{format_number(ops)} ops (80/20 read/write)", ops, wall_us)
    end)
  end

  # ---------------------------------------------------------------------------
  # 4. Delete throughput
  # ---------------------------------------------------------------------------

  defp delete_throughput(shards) do
    header("4. Delete Throughput")

    with_ekv([name: @name, shards: shards], fn ->
      n = 20_000

      for i <- 1..n, do: EKV.put(@name, "del/#{i}", :ok)

      samples = collect_samples(n, fn -> EKV.delete(@name, "del/#{:rand.uniform(n)}") end)
      report_latency("sequential (#{format_number(n)} deletes)", samples)
    end)
  end

  # ---------------------------------------------------------------------------
  # 5. Prefix scan throughput (fans out to all shards)
  # ---------------------------------------------------------------------------

  defp prefix_scan_throughput(shards) do
    header("5. Prefix Scan (fan-out to all #{shards} shards)")

    with_ekv([name: @name, shards: shards], fn ->
      for size <- [100, 1_000, 10_000] do
        for i <- 1..size, do: EKV.put(@name, "scan#{size}/#{i}", %{i: i})

        subheader("#{format_number(size)} keys")

        scan_samples =
          collect_samples(1_000, fn ->
            EKV.scan(@name, "scan#{size}/")
          end)

        report_latency("scan/2", scan_samples)

        keys_samples =
          collect_samples(1_000, fn ->
            EKV.keys(@name, "scan#{size}/")
          end)

        report_latency("keys/2", keys_samples)
      end
    end)
  end

  # ---------------------------------------------------------------------------
  # 6. TTL put throughput
  # ---------------------------------------------------------------------------

  defp ttl_put_throughput(shards) do
    header("6. TTL Put Throughput")

    with_ekv([name: @name, shards: shards], fn ->
      n = 50_000

      samples_no_ttl = collect_samples(n, fn -> EKV.put(@name, "nottl/#{:rand.uniform(n)}", :ok) end)
      report_latency("without TTL (#{format_number(n)} puts)", samples_no_ttl)

      samples_ttl = collect_samples(n, fn -> EKV.put(@name, "ttl/#{:rand.uniform(n)}", :ok, ttl: :timer.minutes(30)) end)
      report_latency("with TTL    (#{format_number(n)} puts)", samples_ttl)
    end)
  end

  # ---------------------------------------------------------------------------
  # 7. Value size scaling
  # ---------------------------------------------------------------------------

  defp value_size_scaling(shards) do
    header("7. Value Size Scaling")

    with_ekv([name: @name, shards: shards], fn ->
      n = 10_000

      for {label, size} <- [{"64 B", 64}, {"1 KB", 1024}, {"10 KB", 10_240}, {"100 KB", 102_400}] do
        value = :crypto.strong_rand_bytes(size)

        {wall_us, _} =
          time_us(fn ->
            for i <- 1..n, do: EKV.put(@name, "sz#{size}/#{i}", value)
          end)

        report_throughput("put #{label} x #{format_number(n)}", n, wall_us)

        warmup(100, fn -> EKV.get(@name, "sz#{size}/#{:rand.uniform(n)}") end)

        read_samples = collect_samples(n, fn -> EKV.get(@name, "sz#{size}/#{:rand.uniform(n)}") end)
        report_latency("get #{label}", read_samples)
      end
    end)
  end

  # ---------------------------------------------------------------------------
  # 8. Shard scaling
  # ---------------------------------------------------------------------------

  defp shard_scaling do
    header("8. Shard Scaling (parallel puts)")

    n = 50_000
    shard_counts = [1, 2, 4, 8, System.schedulers_online()]
    shard_counts = shard_counts |> Enum.uniq() |> Enum.sort()

    for shards <- shard_counts do
      with_ekv([name: @name, shards: shards], fn ->
        {wall_us, _} =
          time_us(fn ->
            1..n
            |> Task.async_stream(
              fn i -> EKV.put(@name, "shard_scale/#{i}", :ok) end,
              max_concurrency: System.schedulers_online() * 4,
              ordered: false
            )
            |> Stream.run()
          end)

        report_throughput("#{shards} shards, #{System.schedulers_online() * 4} workers", n, wall_us)
      end)
    end
  end

  # ---------------------------------------------------------------------------
  # 9. Subscribe overhead — how much does dispatching slow down writes?
  # ---------------------------------------------------------------------------

  defp subscribe_overhead(shards) do
    header("9. Subscribe Overhead on Writes")

    n = 20_000

    for {label, sub_count} <- [{"0 subscribers", 0}, {"1 subscriber", 1}, {"10 subscribers", 10}, {"100 subscribers", 100}] do
      with_ekv([name: @name, shards: shards], fn ->
        # Spawn subscribers that drain their mailboxes
        subs =
          for _ <- 1..sub_count//1 do
            spawn(fn ->
              EKV.subscribe(@name, "sub_overhead/")
              drain_loop()
            end)
          end

        # Give Registry time to register all
        if sub_count > 0, do: Process.sleep(20)

        samples = collect_samples(n, fn -> EKV.put(@name, "sub_overhead/#{:rand.uniform(n)}", :ok) end)
        report_latency("#{label} (#{format_number(n)} puts)", samples)

        Enum.each(subs, &Process.exit(&1, :kill))
      end)
    end
  end

  # ---------------------------------------------------------------------------
  # 10. Subscribe fan-out — many subscribers, same prefix
  # ---------------------------------------------------------------------------

  defp subscribe_fan_out(shards) do
    header("10. Subscribe Fan-out (all subscribers match)")

    n = 5_000

    for sub_count <- [1, 10, 50, 200] do
      with_ekv([name: @name, shards: shards], fn ->
        subs =
          for _ <- 1..sub_count do
            spawn(fn ->
              EKV.subscribe(@name)
              drain_loop()
            end)
          end

        Process.sleep(20)

        samples = collect_samples(n, fn -> EKV.put(@name, "fanout/#{:rand.uniform(n)}", :ok) end)
        report_latency("#{sub_count} subscribers (#{format_number(n)} puts)", samples)

        Enum.each(subs, &Process.exit(&1, :kill))
      end)
    end
  end

  # ---------------------------------------------------------------------------
  # 11. Subscribe at scale — 10k subscribers, random keys vs single key
  # ---------------------------------------------------------------------------

  defp subscribe_single_key(shards) do
    header("11. Subscribe at Scale (10,000 subscribers)")

    n = 5_000
    sub_count = 10_000

    # Scenario A: 10k subscribers on random keys
    with_ekv([name: @name, shards: shards], fn ->
      subs =
        for i <- 1..sub_count do
          spawn(fn ->
            EKV.subscribe(@name, "rnd/#{i}/")
            drain_loop()
          end)
        end

      Process.sleep(100)

      samples = collect_samples(n, fn -> EKV.put(@name, "rnd/#{:rand.uniform(sub_count)}/k", :ok) end)
      report_latency("10k subs, random keys (#{format_number(n)} puts)", samples)

      Enum.each(subs, &Process.exit(&1, :kill))
    end)

    # Scenario B: 10k subscribers on the same key
    with_ekv([name: @name, shards: shards], fn ->
      subs =
        for _ <- 1..sub_count do
          spawn(fn ->
            EKV.subscribe(@name)
            drain_loop()
          end)
        end

      Process.sleep(100)

      samples = collect_samples(n, fn -> EKV.put(@name, "hot/key", :ok) end)
      report_latency("10k subs, same key (#{format_number(n)} puts)", samples)

      Enum.each(subs, &Process.exit(&1, :kill))
    end)
  end

  # ---------------------------------------------------------------------------
  # 12. Subscribe event latency — time from put to event received
  # ---------------------------------------------------------------------------

  defp subscribe_event_latency(shards) do
    header("12. Subscribe Event Latency (put → event received)")

    with_ekv([name: @name, shards: shards], fn ->
      n = 10_000
      parent = self()

      # Spawn a subscriber that timestamps each received event
      sub =
        spawn(fn ->
          EKV.subscribe(@name, "evt_lat/")
          event_latency_loop(parent)
        end)

      Process.sleep(20)

      samples =
        Enum.map(1..n, fn i ->
          t0 = System.monotonic_time(:microsecond)
          EKV.put(@name, "evt_lat/#{i}", :ok)

          receive do
            {:event_received, t1} -> t1 - t0
          after
            5000 -> raise "event latency timeout"
          end
        end)
        |> Enum.sort()

      report_latency("#{format_number(n)} puts (put call → event in subscriber mailbox)", samples)

      Process.exit(sub, :kill)
    end)
  end

  defp event_latency_loop(parent) do
    receive do
      {:ekv, _events, _meta} ->
        send(parent, {:event_received, System.monotonic_time(:microsecond)})
        event_latency_loop(parent)
    end
  end

  defp drain_loop do
    receive do
      _ -> drain_loop()
    end
  end
end
