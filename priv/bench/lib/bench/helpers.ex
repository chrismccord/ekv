defmodule Bench.Helpers do
  @name :bench

  # ---------------------------------------------------------------------------
  # Timing
  # ---------------------------------------------------------------------------

  def time_us(fun) do
    t0 = System.monotonic_time(:microsecond)
    result = fun.()
    elapsed = System.monotonic_time(:microsecond) - t0
    {elapsed, result}
  end

  def collect_samples(n, fun) do
    Enum.map(1..n, fn _ ->
      {us, _} = time_us(fun)
      us
    end)
    |> Enum.sort()
  end

  # ---------------------------------------------------------------------------
  # Percentile
  # ---------------------------------------------------------------------------

  def percentile(_sorted, p) when p < 0 or p > 100, do: raise("bad percentile")

  def percentile(sorted, p) do
    len = length(sorted)
    idx = max(0, ceil(len * p / 100) - 1)
    Enum.at(sorted, idx)
  end

  # ---------------------------------------------------------------------------
  # Formatting
  # ---------------------------------------------------------------------------

  def format_number(n) when is_float(n), do: :erlang.float_to_binary(n, decimals: 1)

  def format_number(n) when is_integer(n) do
    n
    |> Integer.to_string()
    |> String.graphemes()
    |> Enum.reverse()
    |> Enum.chunk_every(3)
    |> Enum.map_join(",", &Enum.join/1)
    |> String.reverse()
  end

  def header(text) do
    bar = String.duplicate("=", 60)
    IO.puts("\n#{bar}")
    IO.puts("  #{text}")
    IO.puts(bar)
  end

  def subheader(text) do
    IO.puts("\n  --- #{text} ---")
  end

  # ---------------------------------------------------------------------------
  # Reporting
  # ---------------------------------------------------------------------------

  def report_latency(label, sorted_us) do
    count = length(sorted_us)
    total = Enum.sum(sorted_us)
    ops = if total > 0, do: trunc(count / (total / 1_000_000)), else: 0

    IO.puts("  #{label}")
    IO.puts("    ops/sec : #{format_number(ops)}")
    IO.puts("    p50     : #{format_number(percentile(sorted_us, 50))} us")
    IO.puts("    p99     : #{format_number(percentile(sorted_us, 99))} us")
    IO.puts("    max     : #{format_number(Enum.max(sorted_us))} us")
  end

  def report_throughput(label, count, wall_us) do
    ops = if wall_us > 0, do: trunc(count / (wall_us / 1_000_000)), else: 0

    IO.puts("  #{label}")
    IO.puts("    total   : #{format_number(count)} ops in #{format_number(trunc(wall_us / 1000))} ms")
    IO.puts("    ops/sec : #{format_number(ops)}")
  end

  def report_sync(label, wall_us) do
    IO.puts("  #{label}")
    IO.puts("    time    : #{format_number(trunc(wall_us / 1000))} ms")
  end

  # ---------------------------------------------------------------------------
  # Setup / teardown
  # ---------------------------------------------------------------------------

  def with_ekv(opts, fun) do
    name = Keyword.get(opts, :name, @name)
    data_dir = Keyword.get(opts, :data_dir, "/tmp/ekv_bench_#{name}_#{System.unique_integer([:positive])}")

    all_opts =
      Keyword.merge(
        [name: name, data_dir: data_dir, log: false, gc_interval: :timer.hours(1)],
        opts
      )

    {:ok, pid} = EKV.start_link(all_opts)
    Process.unlink(pid)

    try do
      fun.()
    after
      Supervisor.stop(pid, :normal, 5000)
      File.rm_rf!(data_dir)
    end
  end

  def warmup(n, fun) do
    for _ <- 1..n, do: fun.()
    :ok
  end
end
