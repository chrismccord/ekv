defmodule EKV.Sqlite3FastReadTest do
  @moduledoc """
  The point-read NIFs that run on normal schedulers (`read_entry_fast`,
  `fetch_each_fast`): the same answers as the dirty NIFs, and a hand-off to
  the dirty NIFs, never a wait, when a reader is busy or the read is too big.
  """
  use ExUnit.Case, async: false

  alias EKV.Sqlite3

  @moduletag :capture_log

  # Holds a connection's lock for a few hundred ms of SQLite work.
  @slow "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 3000000) SELECT count(*) FROM c"

  setup do
    name = :"ekv_fast_read_#{System.unique_integer([:positive])}"
    dir = Path.join(System.tmp_dir!(), "#{name}")
    File.rm_rf!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)

    start_supervised!(
      {EKV,
       name: name,
       data_dir: dir,
       shards: 4,
       reader_connections: 2,
       log: false,
       gc_interval: :timer.hours(1),
       tombstone_ttl: :timer.hours(24 * 7)}
    )

    :ok = EKV.put(name, "a/live", %{n: 1})
    :ok = EKV.put(name, "a/deleted", "gone")
    :ok = EKV.delete(name, "a/deleted")
    :ok = EKV.put(name, "a/expired", "old", ttl: 1)
    :ok = EKV.put(name, "a/ttl_live", "fresh", ttl: :timer.minutes(5))
    :ok = EKV.put(name, "a/big", :binary.copy("b", 200_000))
    for i <- 1..40, do: :ok = EKV.put(name, "k/#{i}", i)
    Process.sleep(10)

    %{name: name}
  end

  defp shard(key), do: EKV.Replica.shard_index_for(key, 4)
  defp readers(name, key), do: :persistent_term.get({EKV, name, :readers, shard(key)})

  defp dirty_get(name, key) do
    {db, stmt} = elem(readers(name, key), 0)
    Sqlite3.read_entry(db, stmt, [key])
  end

  defp dirty_get_many(name, keys) do
    dbs = Enum.map(keys, &(name |> readers(&1) |> elem(0) |> elem(0)))
    Sqlite3.fetch_all_each(dbs, EKV.Store.get_sql(), Enum.map(keys, &[&1]))
  end

  defp fast_get_many(name, keys) do
    Sqlite3.fetch_each_fast(
      Enum.map(keys, &readers(name, &1)),
      EKV.Store.get_sql(),
      Enum.map(keys, &[&1])
    )
  end

  @point_keys ["a/live", "a/deleted", "a/expired", "a/ttl_live", "a/big", "missing", "k/1"]

  describe "equivalence with the dirty NIFs" do
    test "read_entry_fast answers what read_entry answers", %{name: name} do
      for key <- @point_keys do
        assert Sqlite3.read_entry_fast(readers(name, key), [key]) == dirty_get(name, key), key
      end

      assert {:ok, nil} = Sqlite3.read_entry_fast(readers(name, "missing"), ["missing"])

      assert {:ok, [_, _, _, nil, del]} =
               Sqlite3.read_entry_fast(readers(name, "a/deleted"), ["a/deleted"])

      assert is_integer(del)

      assert {:ok, [_, _, _, exp, nil]} =
               Sqlite3.read_entry_fast(readers(name, "a/expired"), ["a/expired"])

      assert is_integer(exp)
    end

    test "fetch_each_fast answers what fetch_all_each answers", %{name: name} do
      [a | _] = keys = for i <- 1..40, do: "k/#{i}"
      same = Enum.find(keys, &(&1 != a and shard(&1) == shard(a)))
      diff = Enum.find(keys, &(shard(&1) != shard(a)))

      batches = [
        [a],
        [a, same],
        [a, diff],
        [a, a],
        ["missing", a],
        ["a/live", "a/deleted", "a/expired", "a/ttl_live"],
        ["a/big", a],
        Enum.take(keys, 8),
        Enum.take(keys, 9),
        keys ++ @point_keys ++ keys,
        []
      ]

      for batch <- batches do
        assert fast_get_many(name, batch) == dirty_get_many(name, batch), inspect(batch)
      end
    end

    test "get, lookup and get_many decode as before", %{name: name} do
      keys = @point_keys ++ ["a/live", "missing"] ++ for(i <- 1..12, do: "k/#{i}")

      big = :binary.copy("b", 200_000)

      expected =
        [%{n: 1}, nil, nil, "fresh", big, nil, 1, %{n: 1}, nil] ++ Enum.to_list(1..12)

      assert Enum.map(keys, &EKV.get(name, &1)) == expected
      assert EKV.get_many(name, keys) == expected

      for n <- [1, 2, 8, 9] do
        assert EKV.get_many(name, Enum.take(keys, n)) == Enum.take(expected, n)
      end

      assert {%{n: 1}, {_, _}} = EKV.lookup(name, "a/live")
      assert EKV.lookup(name, "a/deleted") == nil
      assert EKV.lookup(name, "a/expired") == nil
    end

    test "error shapes match", %{name: name} do
      r = readers(name, "a/live")
      {db, stmt} = elem(r, 0)

      # Bind errors.
      assert Sqlite3.read_entry_fast(r, ["a", "b"]) == Sqlite3.read_entry(db, stmt, ["a", "b"])
      assert {:error, _} = Sqlite3.read_entry_fast(r, ["a", "b"])

      assert Sqlite3.fetch_each_fast([r], "SELECT ?1", [[1, 2]]) ==
               Sqlite3.fetch_all_each([db], "SELECT ?1", [[1, 2]])

      assert_raise ArgumentError, fn -> Sqlite3.read_entry_fast(r, [self()]) end

      assert_raise ArgumentError, fn ->
        Sqlite3.fetch_each_fast([r], EKV.Store.get_sql(), [[self()]])
      end

      # SQL errors (a statement that is not cached goes to the dirty NIF).
      assert Sqlite3.fetch_each_fast([r], "SELECT * FROM nope", [[]]) ==
               Sqlite3.fetch_all_each([db], "SELECT * FROM nope", [[]])

      # The first error, in key order.
      assert Sqlite3.fetch_each_fast([r, r], EKV.Store.get_sql(), [["a/live"], [1, 2]]) ==
               Sqlite3.fetch_all_each([db, db], EKV.Store.get_sql(), [["a/live"], [1, 2]])

      # Malformed arguments.
      assert_raise ArgumentError, fn -> Sqlite3.read_entry_fast({}, ["a"]) end
      assert_raise ArgumentError, fn -> Sqlite3.read_entry_fast({:x}, ["a"]) end
      assert_raise ArgumentError, fn -> Sqlite3.fetch_each_fast([r], EKV.Store.get_sql(), []) end

      assert_raise ArgumentError, fn ->
        Sqlite3.fetch_each_fast([{}], EKV.Store.get_sql(), [["a"]])
      end

      assert_raise ArgumentError, fn ->
        Sqlite3.fetch_each_fast(
          List.duplicate({{:x, :y}}, 9),
          EKV.Store.get_sql(),
          List.duplicate(["a"], 9)
        )
      end

      # A statement of another connection.
      {db2, _} = elem(r, 1)

      assert Sqlite3.read_entry_fast({{db2, stmt}}, ["a/live"]) ==
               Sqlite3.read_entry(db2, stmt, ["a/live"])

      assert {:error, _} = Sqlite3.read_entry_fast({{db2, stmt}}, ["a/live"])
    end

    test "closed connections answer as the dirty NIFs do" do
      dir = Path.join(System.tmp_dir!(), "ekv_fast_closed_#{System.unique_integer([:positive])}")
      File.mkdir_p!(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      {:ok, db} = Sqlite3.open(Path.join(dir, "db.sqlite"))
      :ok = Sqlite3.execute(db, "CREATE TABLE t (k TEXT PRIMARY KEY, v BLOB)")
      {:ok, stmt} = Sqlite3.prepare(db, "SELECT v FROM t WHERE k = ?1")
      {:ok, []} = Sqlite3.fetch_all(db, "SELECT v FROM t WHERE k = ?1", ["x"])
      :ok = Sqlite3.close(db)

      assert Sqlite3.read_entry_fast({{db, stmt}}, ["x"]) == Sqlite3.read_entry(db, stmt, ["x"])

      assert Sqlite3.fetch_each_fast([{{db, stmt}}], "SELECT v FROM t WHERE k = ?1", [["x"]]) ==
               Sqlite3.fetch_all_each([db], "SELECT v FROM t WHERE k = ?1", [["x"]])
    end
  end

  describe "busy readers hand off to the dirty NIFs" do
    # Holds every listed reader connection's lock with a slow query, runs fun
    # in a process watched for long schedules, and answers {result, ms,
    # long_schedule events for that process}.
    defp with_readers_held(conns, fun) do
      holders = for db <- conns, do: Task.async(fn -> Sqlite3.fetch_all(db, @slow, []) end)
      Process.sleep(50)

      :erlang.system_monitor(self(), [{:long_schedule, 1}])

      try do
        caller = Task.async(fn -> :timer.tc(fun) end)
        {us, result} = Task.await(caller, 30_000)
        for h <- holders, do: assert({:ok, [[3_000_000]]} = Task.await(h, 30_000))
        :erlang.system_monitor(:undefined)
        {result, div(us, 1000), long_schedules_of(caller.pid)}
      after
        :erlang.system_monitor(:undefined)
      end
    end

    defp long_schedules_of(pid) do
      receive do
        {:monitor, ^pid, :long_schedule, info} -> [info | long_schedules_of(pid)]
      after
        0 -> []
      end
    end

    defp all_conns(name, key),
      do: name |> readers(key) |> Tuple.to_list() |> Enum.map(&elem(&1, 0))

    test "get waits for a reader on a dirty scheduler, not on its own", %{name: name} do
      expected = dirty_get(name, "a/live")

      {result, ms, longs} =
        with_readers_held(all_conns(name, "a/live"), fn ->
          Sqlite3.read_entry_fast(readers(name, "a/live"), ["a/live"])
        end)

      assert result == expected
      assert ms >= 30, "answered in #{ms} ms while every reader was held"
      assert longs == []

      {value, _ms, longs} =
        with_readers_held(all_conns(name, "a/live"), fn -> EKV.get(name, "a/live") end)

      assert value == %{n: 1}
      assert longs == []
    end

    test "get_many waits for readers on a dirty scheduler, not on its own", %{name: name} do
      keys = ["a/live", "k/1", "missing", "a/deleted"]
      expected = dirty_get_many(name, keys)
      conns = keys |> Enum.flat_map(&all_conns(name, &1)) |> Enum.uniq()

      {result, ms, longs} = with_readers_held(conns, fn -> fast_get_many(name, keys) end)
      assert result == expected
      assert ms >= 30
      assert longs == []

      # Only one key's shard held.
      {result, ms, longs} =
        with_readers_held(all_conns(name, "k/1"), fn -> fast_get_many(name, keys) end)

      assert result == expected
      assert ms >= 30
      assert longs == []

      many = for i <- 1..20, do: "k/#{i}"

      {values, _ms, longs} =
        with_readers_held(Enum.uniq(Enum.flat_map(many, &all_conns(name, &1))), fn ->
          EKV.get_many(name, many)
        end)

      assert values == Enum.to_list(1..20)
      assert longs == []
    end

    test "a busy reader is skipped for a free one of the same shard", %{name: name} do
      [held, _free] = all_conns(name, "a/live")
      expected = dirty_get(name, "a/live")

      {results, ms, longs} =
        with_readers_held([held], fn ->
          for _ <- 1..50, do: Sqlite3.read_entry_fast(readers(name, "a/live"), ["a/live"])
        end)

      assert Enum.uniq(results) == [expected]
      assert ms < 100, "took #{ms} ms with a free reader"
      assert longs == []

      # Statements not cached yet go to the dirty NIF, which may pick the
      # held reader for the keys after the first.
      for db <- all_conns(name, "a/live"),
          do: {:ok, _} = Sqlite3.fetch_all(db, EKV.Store.get_sql(), ["a/live"])

      {results, ms, _} =
        with_readers_held([held], fn ->
          for _ <- 1..50, do: fast_get_many(name, ["a/live", "a/live"])
        end)

      assert Enum.uniq(results) == [dirty_get_many(name, ["a/live", "a/live"])]
      assert ms < 100
    end
  end

  # A NIF that held its normal scheduler shows as a long schedule of the
  # process that called it (long_schedule cannot name the NIF: its in/out
  # locations are where the process was scheduled, often :undefined). The
  # load workers do nothing else that runs for milliseconds: writes are
  # calls, and scans run on dirty schedulers. The probes call the fast NIFs
  # directly under a :running trace: a call with no schedule-out inside it
  # ran on the normal scheduler throughout, so its duration is the NIF's.
  #
  # The bounds are wall clock, so they also count time the OS took the
  # scheduler thread away: on a 2-vCPU CI runner under this load, worker
  # long schedules reach about 6 ms and the probes' p99 about 400 us, against
  # under 1 ms and about 50 us on an 8-core laptop. A fast read that waited on
  # a reader lock or SQLite's busy handler would hold its scheduler far past
  # @stuck_ms; one that did real work there would push the p99 past @p99_us.
  @stuck_ms 50
  @p99_us 1_000

  test "point reads never hold a normal scheduler during a mixed load", %{name: name} do
    for i <- 1..2_000,
        do: :ok = EKV.put(name, "s/#{rem(i, 20)}/#{i}", %{i: i, pad: :binary.copy("x", 300)})

    keys = for(i <- 1..40, do: "k/#{i}") ++ ["a/live", "a/deleted", "missing"]
    prefixes = for i <- 0..19, do: "s/#{i}/"
    tracer = spawn_link(&collect_outs/0)

    :erlang.system_monitor(self(), [{:long_schedule, 1}])
    deadline = System.monotonic_time(:millisecond) + 3_000

    results =
      try do
        1..64
        |> Task.async_stream(
          fn
            id when id <= 4 -> probe(name, keys, deadline, tracer)
            id -> load(name, keys, prefixes, deadline, id)
          end,
          max_concurrency: 64,
          timeout: :infinity
        )
        |> Enum.map(fn {:ok, result} -> result end)
      after
        :erlang.system_monitor(:undefined)
      end

    send(tracer, {:done, self()})
    outs = receive do: ({:outs, outs} -> outs)

    {probes, loads} = Enum.split_with(results, &match?({:probe, _, _}, &1))
    workers = MapSet.new(results, &elem(&1, 1))
    longs = Enum.filter(drain_long_schedules(), fn {pid, _} -> MapSet.member?(workers, pid) end)
    too_long = Enum.filter(longs, fn {_, info} -> info[:timeout] >= @stuck_ms end)

    {on_sched, off_sched} =
      probes
      |> Enum.flat_map(fn {:probe, pid, calls} -> classify(calls, Map.get(outs, pid, [])) end)
      |> Enum.split_with(&match?({:on, _}, &1))

    sorted = on_sched |> Enum.map(fn {:on, us} -> us end) |> Enum.sort()
    p99 = Enum.at(sorted, trunc(length(sorted) * 0.99), 0)
    max_us = List.last(sorted) || 0
    loaded = loads |> Enum.map(&elem(&1, 2)) |> Enum.sum()

    IO.puts(
      "\nmixed load: #{loaded} load-worker point reads; long_schedule in workers: " <>
        "#{length(longs)} >= 1 ms, #{length(too_long)} >= #{@stuck_ms} ms; " <>
        "probe NIF calls: #{length(on_sched)} on the normal scheduler (p99 #{p99} us, max #{max_us} us), " <>
        "#{length(off_sched)} descheduled (dirty hand-off or preempted)"
    )

    assert loaded > 0
    assert length(on_sched) > 1_000
    assert too_long == []
    assert p99 < @p99_us
  end

  defp load(name, keys, prefixes, deadline, id) do
    Enum.reduce_while(Stream.iterate(0, &(&1 + 1)), 0, fn n, reads ->
      if System.monotonic_time(:millisecond) > deadline do
        {:halt, {:load, self(), reads}}
      else
        case rem(n + id, 8) do
          r when r < 4 ->
            EKV.get_many(name, Enum.take(Enum.drop(keys, rem(n, 40)), 1 + rem(n, 3)))
            {:cont, reads + 1}

          4 ->
            EKV.get(name, Enum.at(keys, rem(n, 43)))
            {:cont, reads + 1}

          5 ->
            EKV.scan_many(name, Enum.take(Enum.drop(prefixes, rem(n, 18)), 2))
            {:cont, reads}

          _ ->
            :ok = EKV.put(name, "w/#{id}/#{rem(n, 20)}", %{n: n})
            {:cont, reads}
        end
      end
    end)
  end

  defp probe(name, keys, deadline, tracer) do
    :erlang.trace(self(), true, [:running, :monotonic_timestamp, {:tracer, tracer}])
    sql = EKV.Store.get_sql()
    keys = List.to_tuple(keys)

    calls =
      Enum.reduce_while(Stream.iterate(0, &(&1 + 1)), [], fn n, calls ->
        if n >= 30_000 or System.monotonic_time(:millisecond) > deadline do
          {:halt, calls}
        else
          key = elem(keys, rem(n, tuple_size(keys)))
          key2 = elem(keys, rem(n * 7, tuple_size(keys)))
          r1 = readers(name, key)
          r2 = readers(name, key2)

          t0 = :erlang.monotonic_time()

          _ =
            if rem(n, 2) == 0,
              do: EKV.Sqlite3NIF.ekv_read_entry_fast(r1, [key]),
              else: EKV.Sqlite3NIF.ekv_fetch_each_fast([r1, r2], sql, [[key], [key2]])

          t1 = :erlang.monotonic_time()
          {:cont, [{t0, t1} | calls]}
        end
      end)

    :erlang.trace(self(), false, [:running])
    {:probe, self(), Enum.reverse(calls)}
  end

  defp collect_outs(outs \\ %{}) do
    receive do
      {:trace_ts, pid, :out, _, ts} ->
        collect_outs(Map.update(outs, pid, [ts], &[ts | &1]))

      {:trace_ts, _, _, _, _} ->
        collect_outs(outs)

      {:done, from} ->
        send(from, {:outs, Map.new(outs, fn {pid, ts} -> {pid, Enum.reverse(ts)} end)})
    end
  end

  # Calls and schedule-outs both in time order.
  defp classify([], _outs), do: []

  defp classify([{t0, _} | _] = calls, [out | outs]) when out < t0, do: classify(calls, outs)

  defp classify([{t0, t1} | calls], [out | _] = outs) when out <= t1,
    do: [{:off, t1 - t0} | classify(calls, outs)]

  defp classify([{t0, t1} | calls], outs),
    do: [{:on, System.convert_time_unit(t1 - t0, :native, :microsecond)} | classify(calls, outs)]

  defp drain_long_schedules do
    receive do
      {:monitor, pid, :long_schedule, info} -> [{pid, info} | drain_long_schedules()]
    after
      0 -> []
    end
  end
end
