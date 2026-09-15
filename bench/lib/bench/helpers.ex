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

    IO.puts(
      "    total   : #{format_number(count)} ops in #{format_number(trunc(wall_us / 1000))} ms"
    )

    IO.puts("    ops/sec : #{format_number(ops)}")
  end

  # Match the summary returned by Bench.Replica.run_parallel_cas_batch/4.
  # Only acknowledged successes contribute to successful throughput.
  def summarize_results(results) do
    Enum.reduce(results, %{attempted: 0, ok: 0, errors: %{}}, fn result, acc ->
      acc = %{acc | attempted: acc.attempted + 1}

      case result do
        {:ok, _value_or_vsn} ->
          %{acc | ok: acc.ok + 1}

        {:ok, _value, _vsn} ->
          %{acc | ok: acc.ok + 1}

        {:error, reason} ->
          %{acc | errors: Map.update(acc.errors, reason, 1, &(&1 + 1))}

        other ->
          raise "unexpected benchmark result: #{inspect(other)}"
      end
    end)
  end

  def report_operation_throughput(label, summary, wall_us) do
    report_throughput("#{label} (successful)", summary.ok, wall_us)
    report_throughput("#{label} (attempted)", summary.attempted, wall_us)
    print_error_counts(summary.errors)
  end

  def print_error_counts(error_counts, label_prefix \\ "    errors") do
    error_counts
    |> Enum.sort_by(fn {reason, _count} -> inspect(reason) end)
    |> Enum.each(fn {reason, count} ->
      IO.puts("#{label_prefix}[#{inspect(reason)}] : #{count}")
    end)
  end

  # This is a post-workload sanity bound, not a linearizability checker.
  # Every acknowledged increment must exist; each ambiguous attempt may add one.
  def validate_counter!(value, %{ok: successes, errors: errors}) do
    ambiguous =
      Enum.reduce(errors, 0, fn
        {reason, count}, acc when reason in [:unconfirmed, :unavailable] ->
          acc + count

        {reason, _count}, acc
        when reason in [
               :conflict,
               :no_quorum,
               :quorum_timeout,
               :cluster_overflow,
               :shutting_down,
               :cas_not_configured
             ] ->
          acc

        {reason, _count}, _acc ->
          raise "cannot validate counter after #{inspect(reason)}"
      end)

    count = if is_nil(value), do: 0, else: value

    unless is_integer(count) and count >= successes and count <= successes + ambiguous do
      raise "counter #{inspect(value)} is outside acknowledged/ambiguous bounds " <>
              "#{successes}..#{successes + ambiguous}"
    end

    :ok
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

    data_dir =
      Keyword.get(opts, :data_dir, "/tmp/ekv_bench_#{name}_#{System.unique_integer([:positive])}")

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
