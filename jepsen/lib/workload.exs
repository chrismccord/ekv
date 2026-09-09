defmodule EkvJepsen.Workload do
  @moduledoc false
  alias EkvJepsen.History

  @name :jepsen_kv
  @key "jepsen/register"
  @timeout 2_000
  @definite_errors [
    :conflict,
    :no_quorum,
    :quorum_timeout,
    :cluster_overflow,
    :shutting_down,
    :cas_not_configured
  ]

  # Keep the boundary injectable so regressions can exercise the same recording
  # and ownership-recovery code without replacing EKV in a real scenario.
  def context(nodes, history, phase, rpc \\ &rpc/4) do
    %{nodes: nodes, history: history, phase: phase, rpc: rpc}
  end

  def state(owner),
    do: %{owner: owner, value: nil, vsn: nil, pending: false, tokens: MapSet.new(), stale: nil}

  def rpc(node, mod, fun, args) do
    try do
      {:returned, :erpc.call(node, mod, fun, args, @timeout + 2_000)}
    rescue
      e -> {:exception, Exception.format(:error, e, __STACKTRACE__)}
    catch
      :exit, reason -> {:exception, inspect(reason)}
    end
  end

  def outcome({:returned, {:ok, vsn}}), do: {:ok, vsn}
  def outcome({:returned, {:error, reason}}) when reason in @definite_errors, do: {:fail, reason}
  def outcome({:returned, {:error, :unconfirmed}}), do: {:info, :unconfirmed}
  def outcome({:exception, reason}), do: {:info, reason}
  def outcome(other), do: raise("unexpected write result: #{inspect(other)}")

  def run(ctx, worker, n_ops, profile, seed, faults_done) do
    :rand.seed(:exsss, {:erlang.phash2({seed, worker}), seed + 1, worker + 1})
    loop(ctx, state("owner-#{worker}"), n_ops, profile, faults_done)
  end

  defp loop(ctx, state, left, profile, faults_done) do
    if left <= 0 and :atomics.get(faults_done, 1) == 1 do
      state
    else
      next =
        case profile do
          :register ->
            if :rand.uniform(2) == 1, do: read(ctx), else: write(ctx)
            state

          :lock ->
            cond do
              state.pending ->
                reconcile(ctx, state)

              state.vsn == nil ->
                if :rand.uniform(100) <= 65,
                  do: cas(ctx, state, :acquire),
                  else: reconcile(ctx, state)

              true ->
                case :rand.uniform(100) do
                  n when n <= 10 and state.stale != nil -> probe_stale(ctx, state)
                  n when n <= 45 -> cas(ctx, state, :renew)
                  n when n <= 70 -> cas(ctx, state, :release)
                  _ -> reconcile(ctx, state)
                end
            end
        end

      # Bound checker cost while ensuring fixed-operation runs span the faults.
      Process.sleep(5)
      loop(ctx, next, left - 1, profile, faults_done)
    end
  end

  def read(ctx, target \\ nil) do
    target = target || random_node(ctx)
    op = invoke(ctx, :read, nil, target)

    case ctx.rpc.(target, EKV, :get, [@name, @key, [consistent: true, timeout: @timeout]]) do
      {:returned, value} ->
        complete(ctx, op, :ok, value)
        {:ok, value}

      {:exception, error} ->
        complete(ctx, op, :fail, nil, %{error: error})
        :error
    end
  end

  def write(ctx, target \\ nil) do
    target = target || random_node(ctx)
    value = unique_token("write")
    op = invoke(ctx, :write, value, target)

    result =
      ctx.rpc.(target, EKV, :put, [@name, @key, value, [consistent: true, timeout: @timeout]])

    record_write(ctx, op, result)
  end

  def cas(ctx, state, action, opts \\ []) do
    target = random_node(ctx)

    value =
      if action == :release,
        do: nil,
        else: %{owner: state.owner, token: unique_token(Atom.to_string(action))}

    expected = if action == :acquire, do: nil, else: state.value
    expected_vsn = if action == :acquire, do: nil, else: state.vsn

    op =
      invoke(ctx, :cas, [expected, value], target, %{
        action: action,
        expected_vsn: expected_vsn,
        stale: Keyword.get(opts, :stale, false)
      })

    result =
      if action == :release do
        ctx.rpc.(target, EKV, :delete, [@name, @key, [if_vsn: expected_vsn, timeout: @timeout]])
      else
        ctx.rpc.(target, EKV, :put, [
          @name,
          @key,
          value,
          [if_vsn: expected_vsn, timeout: @timeout]
        ])
      end

    # Unconfirmed writes stay :info. Resolution reads are separate observations,
    # never hidden inside an expanded write interval.
    case record_write(ctx, op, result) do
      {:ok, vsn} ->
        %{
          state
          | value: value,
            vsn: if(value, do: vsn),
            pending: false,
            tokens: remember(state.tokens, value),
            stale: if(state.value, do: {state.value, state.vsn}, else: state.stale)
        }

      {:fail, _} ->
        %{state | pending: true}

      {:info, _} ->
        %{state | pending: true, tokens: remember(state.tokens, value)}
    end
  end

  def probe_stale(ctx, %{stale: {value, vsn}} = state) do
    # Exercise both stale renewal and stale release, including after reacquisition
    # by the same owner. A wrongly successful mutation must fail the token model.
    result =
      cas(ctx, %{state | value: value, vsn: vsn}, Enum.random([:renew, :release]), stale: true)

    %{state | pending: true, tokens: result.tokens}
  end

  def reconcile(ctx, state) do
    target = random_node(ctx)

    case read(ctx, target) do
      {:ok, %{owner: owner, token: token} = value} when owner == state.owner ->
        unless MapSet.member?(state.tokens, token), do: raise("observed an unissued lock token")

        # lookup is eventual: use it only to recover a VSN for this exact token.
        # Record its mapping for the version checker, not as a linearizable read.
        case ctx.rpc.(target, EKV, :lookup, [@name, @key]) do
          {:returned, {^value, vsn}} ->
            History.log(ctx.history, %{
              process: :observer,
              type: :info,
              f: :lookup,
              value: value,
              vsn: vsn,
              phase: ctx.phase,
              node: to_string(target)
            })

            %{state | value: value, vsn: vsn, pending: false}

          {:returned, _other} ->
            %{state | pending: true}

          {:exception, _error} ->
            %{state | pending: true}
        end

      {:ok, _not_ours} ->
        %{state | value: nil, vsn: nil, pending: false}

      :error ->
        %{state | pending: true}
    end
  end

  # Run only after faults have healed. A worker must not exit while abandoning
  # a possibly committed acquisition; no TTL is assumed by the lock model.
  def drain(ctx, state, attempts \\ 20)
  def drain(_ctx, _state, 0), do: raise("could not settle and release lock after healing")

  def drain(ctx, state, attempts) do
    state = reconcile(ctx, state)

    cond do
      state.pending ->
        Process.sleep(25)
        drain(ctx, state, attempts - 1)

      state.vsn == nil ->
        :ok

      true ->
        next = cas(ctx, state, :release)
        if next.pending, do: drain(ctx, next, attempts - 1), else: :ok
    end
  end

  defp record_write(ctx, op, result) do
    {type, detail} = outcome(result)
    extra = if type == :ok, do: %{vsn: detail}, else: %{error: inspect(detail)}
    complete(ctx, op, type, op.value, extra)
    {type, detail}
  end

  defp invoke(ctx, f, value, target, extra \\ %{}) do
    op =
      Map.merge(
        %{
          process: System.unique_integer([:positive]),
          type: :invoke,
          f: f,
          value: value,
          node: to_string(target),
          phase: ctx.phase
        },
        extra
      )

    History.log(ctx.history, op)
    op
  end

  defp complete(ctx, op, type, value, extra \\ %{}) do
    History.log(ctx.history, op |> Map.merge(extra) |> Map.merge(%{type: type, value: value}))
  end

  defp remember(tokens, nil), do: tokens
  defp remember(tokens, %{token: token}), do: MapSet.put(tokens, token)
  defp random_node(ctx), do: Enum.random(ctx.nodes)
  defp unique_token(prefix), do: "#{prefix}/#{System.unique_integer([:positive, :monotonic])}"
end
