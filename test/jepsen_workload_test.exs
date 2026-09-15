Code.require_file("../jepsen/lib/history.exs", __DIR__)
Code.require_file("../jepsen/lib/workload.exs", __DIR__)

defmodule EKV.JepsenWorkloadTest do
  use ExUnit.Case, async: true
  alias EkvJepsen.{History, Workload}

  test "definite rejection is not confused with an ambiguous write" do
    assert {:fail, :conflict} = Workload.outcome({:returned, {:error, :conflict}})
    assert {:fail, :no_quorum} = Workload.outcome({:returned, {:error, :no_quorum}})
    assert {:info, :unconfirmed} = Workload.outcome({:returned, {:error, :unconfirmed}})
    assert {:info, :timeout} = Workload.outcome({:exception, :timeout})

    assert_raise RuntimeError, ~r/unexpected write result/, fn ->
      Workload.outcome({:returned, {:error, :misspelled_error}})
    end
  end

  test "ambiguous acquisition survives a failed resolver and is later released" do
    {:ok, history} = History.start_link()
    on_exit(fn -> if Process.alive?(history), do: Agent.stop(history) end)
    Process.put(:stored, nil)
    Process.put(:failed_read, false)

    rpc = fn
      _, EKV, :put, [_, _, value, opts] ->
        previous = Process.get(:stored)
        vsn = {System.unique_integer([:positive, :monotonic]), "member"}
        assert opts[:if_vsn] == if(previous, do: elem(previous, 1))
        Process.put(:stored, {value, vsn})
        if previous, do: {:returned, {:ok, vsn}}, else: {:returned, {:error, :unconfirmed}}

      _, EKV, :get, _ ->
        if Process.get(:failed_read) do
          {:returned, elem(Process.get(:stored), 0)}
        else
          Process.put(:failed_read, true)
          {:exception, :unavailable}
        end

      _, EKV, :lookup, _ ->
        {:returned, Process.get(:stored)}

      _, EKV, :delete, [_, _, opts] ->
        assert opts[:if_vsn] == elem(Process.get(:stored), 1)
        Process.put(:stored, nil)
        {:returned, {:ok, {999, "member"}}}
    end

    ctx = Workload.context([:local], history, :workload, rpc)
    state = Workload.cas(ctx, Workload.state("a"), :acquire)
    assert state.pending
    assert MapSet.size(state.tokens) == 1
    state = Workload.reconcile(ctx, state)
    assert state.pending
    state = Workload.reconcile(ctx, state)
    refute state.pending
    assert state.vsn != nil
    state = Workload.cas(ctx, state, :renew)
    assert state.value.token =~ "renew/"
    state = Workload.cas(ctx, state, :release)
    assert state.value == nil
    assert Process.get(:stored) == nil

    completed = Enum.filter(History.events(history), &(&1.type in [:info, :ok, :fail]))

    assert Enum.map(completed, &{&1.f, &1.type}) ==
             [
               {:cas, :info},
               {:read, :fail},
               {:read, :ok},
               {:lookup, :info},
               {:cas, :ok},
               {:cas, :ok}
             ]
  end

  test "EDN preserves nested tokens, nil, and version tuples without lossy inspect" do
    assert History.edn([nil, %{owner: "a", token: "a\t\"b"}, {42, "member"}]) =~
             ~s(:token "a\\t\\"b")

    assert History.edn({42, "member"}) == ~s([42 "member"])
    assert History.edn(nil) == "nil"
  end

  test "stale probes send the older version and retain current ownership for reconciliation" do
    {:ok, history} = History.start_link()
    old = %{owner: "a", token: "old"}
    current = %{owner: "a", token: "current"}
    state = %{Workload.state("a") | value: current, vsn: {2, "a"}, stale: {old, {1, "a"}}}

    rpc = fn _, EKV, _, args ->
      assert List.last(args)[:if_vsn] == {1, "a"}
      {:returned, {:error, :conflict}}
    end

    ctx = Workload.context([:local], history, :workload, rpc)
    next = Workload.probe_stale(ctx, state)
    assert next.value == current
    assert next.pending
    assert [%{stale: true, value: [^old, _]}, %{type: :fail}] = History.events(history)
    Agent.stop(history)
  end
end
