defmodule EKV.RegisterHistoryTest do
  use ExUnit.Case, async: true

  alias EKV.RegisterHistory

  test "only the acknowledged initial value can explain the first read" do
    for value <- [nil, 999] do
      events = operation(1, :read, value) ++ operation(2, :write, 999)
      result = RegisterHistory.check(events, 0)
      assert result.covered?
      refute result.linearizable?
      refute RegisterHistory.passes?(result)
    end

    for initial <- [nil, 0, 999] do
      events = operation(1, :read, initial) ++ operation(2, :write, 1)
      assert RegisterHistory.passes?(RegisterHistory.check(events, initial))
    end
  end

  test "successful reads compare absence and stale values exactly" do
    for value <- [nil, 0] do
      events = operation(1, :write, 7) ++ operation(2, :read, value)
      refute RegisterHistory.check(events, 0).linearizable?
    end

    events = operation(1, :write, 7) ++ operation(2, :read, 7)
    assert RegisterHistory.passes?(RegisterHistory.check(events, 0))
  end

  test "empty, failed-only, and single-operation-kind workloads cannot pass" do
    histories = [
      [],
      operation(1, :read, nil, :fail) ++ operation(2, :write, 7, :fail),
      operation(1, :read, 0),
      operation(1, :write, 7)
    ]

    for events <- histories do
      result = RegisterHistory.check(events, 0)
      assert result.linearizable?
      refute result.covered?
      refute RegisterHistory.passes?(result)
    end
  end

  test "overlapping operations can linearize in either order" do
    [write_start, write_end] = operation(1, :write, 7)

    for value <- [0, 7] do
      [read_start, read_end] = operation(2, :read, value)
      events = [write_start, read_start, read_end, write_end]
      assert RegisterHistory.passes?(RegisterHistory.check(events, 0))
    end
  end

  test "an ambiguous write may apply, be omitted, or apply after its timeout" do
    prefix = operation(1, :write, 1) ++ operation(2, :write, 2, :info)

    for reads <- [[1], [2], [1, 2]] do
      events =
        prefix ++
          (reads
           |> Enum.with_index(3)
           |> Enum.flat_map(fn {value, id} -> operation(id, :read, value) end))

      result = RegisterHistory.check(events, 0)
      assert result.info_writes == 1
      assert RegisterHistory.passes?(result)
    end
  end

  test "an unrelated ambiguous write cannot excuse a definite violation" do
    events =
      operation(1, :write, 1) ++
        operation(2, :write, 2, :info) ++ operation(3, :read, 999)

    result = RegisterHistory.check(events, 0)
    assert result.covered?
    assert result.info_writes == 1
    refute result.linearizable?
    refute RegisterHistory.passes?(result)
  end

  test "an ambiguous write applies at most once and never before its invocation" do
    histories = [
      operation(1, :write, 1) ++
        operation(2, :write, 2, :info) ++
        operation(3, :read, 2) ++ operation(4, :read, 1) ++ operation(5, :read, 2),
      operation(1, :write, 1) ++
        operation(2, :read, 2) ++ operation(3, :write, 2, :info)
    ]

    for events <- histories do
      refute RegisterHistory.check(events, 0).linearizable?
    end
  end

  test "failed writes cannot explain observations but pending writes can" do
    prefix = operation(1, :write, 1)
    [invoke, _] = operation(2, :write, 2)
    read = operation(3, :read, 2)

    assert RegisterHistory.passes?(RegisterHistory.check(prefix ++ [invoke] ++ read, 0))

    refute RegisterHistory.check(
             prefix ++ operation(2, :write, 2, :fail) ++ read,
             0
           ).linearizable?
  end

  test "optional writes do not substitute for successful workload coverage" do
    events = operation(1, :write, 2, :info) ++ operation(2, :read, 2)
    result = RegisterHistory.check(events, 0)
    assert result.linearizable?
    assert result.successful_writes == 0
    refute RegisterHistory.passes?(result)
  end

  test "the RPC recorder preserves ambiguous outcomes and definitive rejections" do
    log = fn event -> send(self(), {:event, event}) end

    cases = [
      {fn -> {:ok, {1, "member"}} end, :ok},
      {fn -> {:error, :unconfirmed} end, :info},
      {fn -> exit(:timeout) end, :info},
      {fn -> :erlang.error({:erpc, :noconnection}) end, :info},
      {fn -> throw(:transport_failure) end, :info}
    ]

    definite = [
      :conflict,
      :no_quorum,
      :quorum_timeout,
      :cluster_overflow,
      :shutting_down,
      :cas_not_configured
    ]

    for {write, outcome} <- cases ++ Enum.map(definite, &{fn -> {:error, &1} end, :fail}) do
      RegisterHistory.record_write(log, 1, 7, write)
      assert_receive {:event, %{type: :invoke, process: 1, f: :write, value: 7}}
      assert_receive {:event, %{type: ^outcome, process: 1, f: :write, value: 7}}
      refute_received {:event, _}
    end

    assert_raise RuntimeError, ~r/unexpected write result/, fn ->
      RegisterHistory.record_write(log, 2, 7, fn -> {:error, :misspelled_error} end)
    end

    assert_receive {:event, %{type: :invoke, process: 2}}
    refute_received {:event, %{process: 2}}
  end

  test "multiple optional writes must admit one coherent ordering" do
    prefix =
      operation(1, :write, 1) ++
        operation(2, :write, 2, :info) ++ operation(3, :write, 3, :info)

    for values <- [[1, 2, 3], [1, 3, 2], [1, 1, 1]] do
      reads =
        values
        |> Enum.with_index(4)
        |> Enum.flat_map(fn {value, id} -> operation(id, :read, value) end)

      assert RegisterHistory.passes?(RegisterHistory.check(prefix ++ reads, 0))
    end

    reads = operation(4, :read, 2) ++ operation(5, :read, 3) ++ operation(6, :read, 2)
    refute RegisterHistory.check(prefix ++ reads, 0).linearizable?
  end

  test "malformed completions fail the checker rather than disappearing" do
    [invoke, completion] = operation(1, :write, 1)

    assert_raise KeyError, fn -> RegisterHistory.check([completion], 0) end

    assert_raise RuntimeError, ~r/mismatched completion/, fn ->
      RegisterHistory.check([invoke, %{completion | f: :read}], 0)
    end

    assert_raise RuntimeError, ~r/overlapping process invocations/, fn ->
      RegisterHistory.check([invoke, invoke], 0)
    end
  end

  defp operation(id, kind, value, outcome \\ :ok) do
    [
      %{process: id, type: :invoke, f: kind, value: if(kind == :read, do: nil, else: value)},
      %{process: id, type: outcome, f: kind, value: value}
    ]
  end
end
