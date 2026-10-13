defmodule EKV.LinearizabilityPureElixirTest do
  use ExUnit.Case

  @moduletag :capture_log
  @moduletag timeout: 300_000

  alias EKV.{RegisterHistory, TestCluster}

  @workers 6
  @total_ops 400
  @retries_per_seed 3
  @known_bad_seeds [
    1_475,
    1_443,
    4_070_22
  ]

  defp unique_name(prefix) do
    :"#{prefix}_#{System.unique_integer([:positive])}"
  end

  defp start_cas_cluster(peers, ekv_name) do
    peers
    |> Enum.with_index(1)
    |> Enum.each(fn {{_pid, node}, node_id} ->
      data_dir = "/tmp/ekv_lin_pure_#{node}_#{ekv_name}"
      TestCluster.rpc!(node, File, :rm_rf!, [data_dir])

      TestCluster.start_ekv(
        node,
        name: ekv_name,
        data_dir: data_dir,
        shards: 1,
        log: false,
        gc_interval: :timer.hours(1),
        tombstone_ttl: :timer.hours(24 * 7),
        cluster_size: length(peers),
        node_id: node_id
      )
    end)
  end

  defp cleanup_data(peers, ekv_name) do
    for {_pid, node} <- peers do
      data_dir = "/tmp/ekv_lin_pure_#{node}_#{ekv_name}"

      try do
        TestCluster.rpc!(node, File, :rm_rf!, [data_dir])
      catch
        _, _ -> :ok
      end
    end
  end

  # BUG: under concurrent `consistent: true` reads/writes on one key, we can
  # produce histories with no valid linearization.
  #
  # Violation pattern seen in failing runs:
  # - write(W_old) completes
  # - write(W_new) completes later
  # - a `consistent: true` read invoked after W_new can still return W_old
  #
  # This cannot be explained by a single total order that respects real-time
  # completion and therefore violates linearizability.
  #
  # This test is intentionally a red regression test before the fix: it uses
  # seeded workloads + retries and fails if any run is non-linearizable.
  test "pure Elixir checker: concurrent consistent read/write history is linearizable" do
    peers = TestCluster.start_peers(3)
    on_exit(fn -> TestCluster.stop_peers(peers) end)

    nodes = Enum.map(peers, &elem(&1, 1))
    ekv_name = unique_name(:lin_pure)
    key = "lin/pure/register"

    start_cas_cluster(peers, ekv_name)
    on_exit(fn -> cleanup_data(peers, ekv_name) end)
    Process.sleep(300)

    seeds = configured_seeds()

    results =
      for seed <- seeds, retry <- 1..@retries_per_seed do
        # A timed-out write can outlive its worker. Never reuse its key in a
        # subsequent history whose initial state is independently seeded.
        key = "#{key}/#{System.unique_integer([:positive])}"

        case TestCluster.rpc!(hd(nodes), EKV, :put, [ekv_name, key, 0, [consistent: true]]) do
          {:ok, _vsn} -> :ok
          other -> flunk("seed write failed for seed=#{seed} retry=#{retry}: #{inspect(other)}")
        end

        events = run_workload(nodes, ekv_name, key, @workers, @total_ops, seed)
        result = RegisterHistory.check(events, 0)

        history_path =
          if RegisterHistory.passes?(result) do
            nil
          else
            path =
              Path.join(
                System.tmp_dir!(),
                "ekv_lin_pure_seed#{seed}_retry#{retry}_#{System.unique_integer([:positive])}.history"
              )

            dump_history(path, events)
            path
          end

        Map.merge(result, %{
          seed: seed,
          retry: retry,
          total_events: length(events),
          history_path: history_path
        })
      end

    invalid = Enum.reject(results, &RegisterHistory.passes?/1)

    assert invalid == [],
           """
           expected all pure-Elixir linearizability checks to pass, but found #{length(invalid)} invalid run(s).

           #{Enum.map_join(results, "\n", fn r -> "seed=#{r.seed} retry=#{r.retry} completed_ops=#{r.completed_ops} reads=#{r.successful_reads} writes=#{r.successful_writes} info_writes=#{r.info_writes} events=#{r.total_events} linearizable?=#{r.linearizable?} history=#{r.history_path || "-"}" end)}
           """
  end

  defp run_workload(nodes, ekv_name, key, workers, total_ops, base_seed) do
    {:ok, events} = Agent.start_link(fn -> [] end)
    counter = :atomics.new(1, [])

    log = fn op ->
      idx = :atomics.add_get(counter, 1, 1)
      Agent.update(events, fn acc -> [{idx, op} | acc] end)
    end

    per_worker = div(total_ops, workers)
    extra = rem(total_ops, workers)

    assignments =
      for worker <- 0..(workers - 1) do
        {worker, per_worker + if(worker < extra, do: 1, else: 0)}
      end

    try do
      Task.async_stream(
        assignments,
        fn {worker, n_ops} ->
          run_worker(nodes, ekv_name, key, worker, n_ops, base_seed, log)
        end,
        max_concurrency: workers,
        ordered: false,
        timeout: :infinity
      )
      |> Stream.run()

      events
      |> Agent.get(& &1)
      |> Enum.sort_by(fn {idx, _op} -> idx end)
      |> Enum.map(fn {_idx, op} -> op end)
    after
      Agent.stop(events)
    end
  end

  defp run_worker(nodes, ekv_name, key, worker, n_ops, base_seed, log) do
    :rand.seed(:exsplus, worker_seed(base_seed, worker))

    Enum.each(1..n_ops, fn i ->
      process_id = worker * 10_000_000 + i
      target = Enum.at(nodes, :rand.uniform(length(nodes)) - 1)

      if :rand.uniform(100) <= 50 do
        log.(%{process: process_id, type: :invoke, f: :read, value: nil})

        try do
          value = TestCluster.rpc!(target, EKV, :get, [ekv_name, key, [consistent: true]])
          log.(%{process: process_id, type: :ok, f: :read, value: value})
        rescue
          _ -> log.(%{process: process_id, type: :fail, f: :read, value: nil})
        catch
          _, _ -> log.(%{process: process_id, type: :fail, f: :read, value: nil})
        end
      else
        value = process_id

        RegisterHistory.record_write(log, process_id, value, fn ->
          TestCluster.rpc!(target, EKV, :put, [ekv_name, key, value, [consistent: true]])
        end)
      end

      if rem(i, 20) == 0, do: Process.sleep(1)
    end)
  end

  defp configured_seeds do
    case System.get_env("EKV_LIN_SEED") do
      nil ->
        @known_bad_seeds

      value ->
        value
        |> String.split(",", trim: true)
        |> Enum.map(&parse_seed!/1)
    end
  end

  defp parse_seed!(seed_text) do
    case Integer.parse(String.trim(seed_text)) do
      {seed, ""} -> seed
      _ -> raise ArgumentError, "invalid EKV_LIN_SEED entry: #{inspect(seed_text)}"
    end
  end

  defp worker_seed(base_seed, worker) do
    {
      :erlang.phash2({base_seed, worker, 1}, 2_147_483_646) + 1,
      :erlang.phash2({base_seed, worker, 2}, 2_147_483_646) + 1,
      :erlang.phash2({base_seed, worker, 3}, 2_147_483_646) + 1
    }
  end

  defp dump_history(path, events) do
    body =
      events
      |> Enum.map(fn event -> inspect(event) end)
      |> Enum.join("\n")

    File.write!(path, body <> "\n")
  end
end
