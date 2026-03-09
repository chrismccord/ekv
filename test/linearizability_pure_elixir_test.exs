defmodule EKV.LinearizabilityPureElixirTest do
  use ExUnit.Case
  import Bitwise

  @moduletag :capture_log
  @moduletag timeout: 300_000

  alias EKV.TestCluster

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
        case TestCluster.rpc!(hd(nodes), EKV, :put, [ekv_name, key, 0, [consistent: true]]) do
          {:ok, _vsn} -> :ok
          other -> flunk("seed write failed for seed=#{seed} retry=#{retry}: #{inspect(other)}")
        end

        events = run_workload(nodes, ekv_name, key, @workers, @total_ops, seed)
        info_writes = Enum.count(events, &(&1.type == :info and &1.f == :write))
        ops = completed_ops(events)
        linearizable? = linearizable_register_history?(ops)

        history_path =
          if linearizable? do
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

        %{
          seed: seed,
          retry: retry,
          total_events: length(events),
          completed_ops: length(ops),
          info_writes: info_writes,
          linearizable?: linearizable?,
          history_path: history_path
        }
      end

    invalid =
      Enum.filter(results, fn r ->
        not r.linearizable? and r.info_writes == 0
      end)

    assert invalid == [],
           """
           expected all pure-Elixir linearizability checks to pass, but found #{length(invalid)} invalid run(s).

           #{Enum.map_join(results, "\n", fn r -> "seed=#{r.seed} retry=#{r.retry} completed_ops=#{r.completed_ops} info_writes=#{r.info_writes} events=#{r.total_events} linearizable?=#{r.linearizable?} history=#{r.history_path || "-"}" end)}
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
        log.(%{process: process_id, type: :invoke, f: :write, value: value})

        try do
          case TestCluster.rpc!(target, EKV, :put, [ekv_name, key, value, [consistent: true]]) do
            {:ok, _} ->
              log.(%{process: process_id, type: :ok, f: :write, value: value})

            {:error, :unconfirmed} ->
              resolver = Enum.at(nodes, :rand.uniform(length(nodes)) - 1)

              resolved =
                TestCluster.rpc!(resolver, EKV, :get, [ekv_name, key, [consistent: true]])

              if resolved == value do
                # Unknown client outcome, but value is now linearizably visible.
                log.(%{process: process_id, type: :ok, f: :write, value: value})
              else
                log.(%{process: process_id, type: :info, f: :write, value: value})
              end

            _ ->
              log.(%{process: process_id, type: :fail, f: :write, value: value})
          end
        rescue
          _ -> log.(%{process: process_id, type: :fail, f: :write, value: value})
        catch
          _, _ -> log.(%{process: process_id, type: :fail, f: :write, value: value})
        end
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

  defp completed_ops(events) do
    events
    |> Enum.with_index()
    |> Enum.reduce(%{}, fn {event, idx}, acc ->
      key = event.process

      case event.type do
        :invoke ->
          Map.put(acc, key, %{id: key, f: event.f, invoke_idx: idx, invoke_value: event.value})

        :ok ->
          case Map.get(acc, key) do
            %{f: f} = existing ->
              op =
                existing
                |> Map.put(:ok_idx, idx)
                |> Map.put(:ok_value, event.value)
                |> Map.put(:kind, f)
                |> Map.put(:value, if(f == :write, do: existing.invoke_value, else: event.value))

              Map.put(acc, key, op)

            _ ->
              acc
          end

        _ ->
          acc
      end
    end)
    |> Map.values()
    |> Enum.filter(fn op ->
      Map.has_key?(op, :invoke_idx) and Map.has_key?(op, :ok_idx) and op.kind in [:read, :write]
    end)
  end

  defp linearizable_register_history?(ops) when ops == [], do: true

  defp linearizable_register_history?(ops) do
    ops = Enum.sort_by(ops, & &1.ok_idx)
    n = length(ops)
    indexed = Enum.with_index(ops)

    pred_masks =
      for {op_i, i} <- indexed do
        Enum.reduce(indexed, 0, fn
          {op_j, j}, acc when j != i ->
            if op_j.ok_idx < op_i.invoke_idx do
              acc ||| 1 <<< j
            else
              acc
            end

          _, acc ->
            acc
        end)
      end

    full_mask = (1 <<< n) - 1
    initial_values = [nil, 0] ++ Enum.uniq(Enum.map(ops, & &1.value))
    ops_t = List.to_tuple(ops)
    pred_t = List.to_tuple(pred_masks)

    Enum.any?(initial_values, fn init ->
      memo = :ets.new(:lin_memo, [:set, :private])

      try do
        do_linearizable?(ops_t, pred_t, n, full_mask, 0, init, memo)
      after
        :ets.delete(memo)
      end
    end)
  end

  defp do_linearizable?(_ops_t, _pred_t, _n, full_mask, full_mask, _state, _memo), do: true

  defp do_linearizable?(ops_t, pred_t, n, full_mask, done_mask, state, memo) do
    key = {done_mask, state}

    case :ets.lookup(memo, key) do
      [{^key, result}] ->
        result

      [] ->
        result = try_candidates(ops_t, pred_t, n, full_mask, done_mask, state, memo, 0)

        :ets.insert(memo, {key, result})
        result
    end
  end

  defp try_candidates(_ops_t, _pred_t, n, _full_mask, _done_mask, _state, _memo, i) when i >= n,
    do: false

  defp try_candidates(ops_t, pred_t, n, full_mask, done_mask, state, memo, i) do
    bit = 1 <<< i
    pred_mask = :erlang.element(i + 1, pred_t)

    result =
      if (done_mask &&& bit) == 0 and (pred_mask &&& done_mask) == pred_mask do
        op = :erlang.element(i + 1, ops_t)

        case op.kind do
          :write ->
            do_linearizable?(ops_t, pred_t, n, full_mask, done_mask ||| bit, op.value, memo)

          :read ->
            op.value == state and
              do_linearizable?(ops_t, pred_t, n, full_mask, done_mask ||| bit, state, memo)
        end
      else
        false
      end

    if result do
      true
    else
      try_candidates(ops_t, pred_t, n, full_mask, done_mask, state, memo, i + 1)
    end
  end
end
