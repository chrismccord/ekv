defmodule EKV.RegisterHistory do
  @moduledoc false
  import Bitwise

  @definite_errors [
    :conflict,
    :no_quorum,
    :quorum_timeout,
    :cluster_overflow,
    :shutting_down,
    :cas_not_configured
  ]

  # Exceptions at the RPC boundary do not establish whether the write applied.
  # Classify outside the catch so unexpected API results fail the harness loudly.
  def record_write(log, id, value, write) do
    op = %{process: id, f: :write, value: value}
    log.(Map.put(op, :type, :invoke))

    result =
      try do
        {:returned, write.()}
      rescue
        error -> {:exception, Exception.format(:error, error, __STACKTRACE__)}
      catch
        kind, reason -> {:exception, {kind, reason}}
      end

    type =
      case result do
        {:returned, {:ok, _vsn}} -> :ok
        {:returned, {:error, reason}} when reason in @definite_errors -> :fail
        {:returned, {:error, :unconfirmed}} -> :info
        {:exception, _} -> :info
        other -> raise "unexpected write result: #{inspect(other)}"
      end

    log.(Map.merge(op, %{type: type, result: inspect(result)}))
  end

  # Initialization is outside the workload, so the caller must supply its exact
  # acknowledged value. Workload coverage is separate from linearizability:
  # an empty history is linearizable, but cannot authorize a successful run.
  def check(events, initial_value) do
    ops = operations(events)
    reads = Enum.count(ops, &(&1.kind == :read and &1.outcome == :ok))
    writes = Enum.count(ops, &(&1.kind == :write and &1.outcome == :ok))
    linearizable? = linearizable?(ops, initial_value)

    %{
      completed_ops: reads + writes,
      info_writes: Enum.count(ops, & &1.optional?),
      successful_reads: reads,
      successful_writes: writes,
      linearizable?: linearizable?,
      covered?: reads > 0 and writes > 0
    }
  end

  def passes?(result), do: result.linearizable? and result.covered?

  defp operations(events) do
    horizon = length(events)

    {pending, ops} =
      events
      |> Enum.with_index()
      |> Enum.reduce({%{}, []}, fn {event, idx}, {pending, ops} ->
        case event.type do
          :invoke ->
            if Map.has_key?(pending, event.process), do: raise("overlapping process invocations")
            unless event.f in [:read, :write], do: raise("unknown operation: #{inspect(event)}")

            op = %{
              id: event.process,
              kind: event.f,
              invoke_idx: idx,
              end_idx: horizon,
              value: event.value,
              outcome: :info,
              optional?: true
            }

            {Map.put(pending, event.process, op), ops}

          outcome when outcome in [:ok, :fail, :info] ->
            op = Map.fetch!(pending, event.process)
            unless op.kind == event.f, do: raise("mismatched completion: #{inspect(event)}")
            pending = Map.delete(pending, event.process)

            case outcome do
              :ok ->
                op = %{
                  op
                  | end_idx: idx,
                    value: if(op.kind == :read, do: event.value, else: op.value),
                    outcome: :ok,
                    optional?: false
                }

                {pending, [op | ops]}

              :info when op.kind == :write ->
                # The timeout/unconfirmed response is not a completion bound.
                # This write may take effect any time after invocation.
                {pending, [op | ops]}

              _ ->
                {pending, ops}
            end
        end
      end)

    # A caller that never recorded a response also leaves an optional write.
    ops ++ Enum.filter(Map.values(pending), &(&1.kind == :write))
  end

  defp linearizable?([], _initial_value), do: true

  defp linearizable?(ops, initial_value) do
    ops = Enum.sort_by(ops, &{&1.end_idx, &1.invoke_idx})
    n = length(ops)
    indexed = Enum.with_index(ops)

    pred_masks =
      for {op_i, i} <- indexed do
        Enum.reduce(indexed, 0, fn
          {op_j, j}, acc when j != i ->
            if op_j.end_idx < op_i.invoke_idx, do: acc ||| 1 <<< j, else: acc

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
            (op.optional? and
               do_linearizable?(ops_t, pred_t, n, full_mask, done_mask ||| bit, state, memo)) or
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
