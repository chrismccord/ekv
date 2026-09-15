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

  defp operation(id, kind, value, outcome \\ :ok) do
    [
      %{process: id, type: :invoke, f: kind, value: if(kind == :read, do: nil, else: value)},
      %{process: id, type: outcome, f: kind, value: value}
    ]
  end
end
