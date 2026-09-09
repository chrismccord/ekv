defmodule EKV.RegisterHistory do
  @moduledoc false
  import Bitwise

  # Initialization is outside the workload, so the caller must supply its exact
  # acknowledged value. Workload coverage is separate from linearizability:
  # an empty history is linearizable, but cannot authorize a successful run.
  def check(events, initial_value) do
    ops = completed_ops(events)
    reads = Enum.count(ops, &(&1.kind == :read))
    writes = Enum.count(ops, &(&1.kind == :write))
    linearizable? = linearizable?(ops, initial_value)

    %{
      completed_ops: length(ops),
      successful_reads: reads,
      successful_writes: writes,
      linearizable?: linearizable?,
      covered?: reads > 0 and writes > 0
    }
  end

  def passes?(result), do: result.linearizable? and result.covered?

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

  defp linearizable?([], _initial_value), do: true

  defp linearizable?(ops, initial_value) do
    ops = Enum.sort_by(ops, & &1.ok_idx)
    n = length(ops)
    indexed = Enum.with_index(ops)

    pred_masks =
      for {op_i, i} <- indexed do
        Enum.reduce(indexed, 0, fn
          {op_j, j}, acc when j != i ->
            if op_j.ok_idx < op_i.invoke_idx, do: acc ||| 1 <<< j, else: acc

          _, acc ->
            acc
        end)
      end

    full_mask = (1 <<< n) - 1
    ops_t = List.to_tuple(ops)
    pred_t = List.to_tuple(pred_masks)
    memo = :ets.new(:lin_memo, [:set, :private])

    try do
      do_linearizable?(ops_t, pred_t, n, full_mask, 0, initial_value, memo)
    after
      :ets.delete(memo)
    end
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
