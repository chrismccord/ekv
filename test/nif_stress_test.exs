defmodule EKV.NifStressTest do
  @moduledoc """
  Concurrency stress for the SQLite NIF's read and statement paths. Excluded
  by default; `scripts/nif_asan_stress.sh` runs it against an AddressSanitizer
  build. Run it plainly with:

      EKV_NIF_STRESS_SECS=30 mix test --only nif_stress
  """
  use ExUnit.Case, async: false

  alias EKV.Sqlite3

  @moduletag :nif_stress
  @moduletag :capture_log
  @moduletag timeout: :infinity

  @secs String.to_integer(System.get_env("EKV_NIF_STRESS_SECS", "10"))
  @concurrency String.to_integer(System.get_env("EKV_NIF_STRESS_CONCURRENCY", "64"))

  setup do
    name = :"ekv_nif_stress_#{System.unique_integer([:positive])}"
    dir = Path.join(System.tmp_dir!(), "#{name}")
    File.rm_rf!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)

    start_supervised!(
      {EKV,
       name: name,
       data_dir: dir,
       shards: 8,
       log: false,
       gc_interval: :timer.hours(1),
       tombstone_ttl: :timer.hours(24 * 7)}
    )

    %{name: name, dir: dir}
  end

  test "reads, scans, writes and statement churn race without corrupting anything",
       %{name: name, dir: dir} do
    values =
      for i <- 1..400, into: %{}, do: {"r/#{i}", %{i: i, blob: :binary.copy("x", rem(i, 700))}}

    Enum.each(values, fn {k, v} -> :ok = EKV.put(name, k, v) end)

    shard = &EKV.Replica.shard_index_for(&1, 8)
    keys = Map.keys(values)
    [a | _] = keys
    same = Enum.find(keys, &(&1 != a and shard.(&1) == shard.(a)))
    diff = Enum.find(keys, &(shard.(&1) != shard.(a)))

    batches = [
      [a, same],
      [a, diff],
      [a, "missing/1"],
      ["missing/2", same],
      [a, a],
      [a, same, diff, "missing/3", a, same]
    ]

    # A connection of its own for prepared-statement churn and raw fetch_all.
    {:ok, raw} = Sqlite3.open(Path.join(dir, "raw.sqlite"))
    {:ok, [["wal"]]} = Sqlite3.fetch_all(raw, "PRAGMA journal_mode=WAL", [])
    :ok = Sqlite3.execute(raw, "CREATE TABLE t (k TEXT PRIMARY KEY, v BLOB)")

    deadline = System.monotonic_time(:millisecond) + @secs * 1000

    step = fn id, n ->
      case rem(n + id, 12) do
        r when r < 5 ->
          batch = Enum.at(batches, rem(n, length(batches)))
          assert EKV.get_many(name, batch) == Enum.map(batch, &values[&1])

        5 ->
          assert EKV.get(name, a) == values[a]

        6 ->
          assert [[_ | _], [_ | _]] = EKV.scan_many(name, ["r/19", "r/7"])

        7 ->
          assert [_ | _] = Enum.to_list(EKV.scan(name, "r/3"))

        8 ->
          :ok =
            EKV.put(name, "w/#{id}/#{rem(n, 50)}", %{n: n, blob: :binary.copy("z", rem(n, 900))})

        9 ->
          k = "k#{rem(n, 100)}"

          {:ok, []} =
            Sqlite3.fetch_all(raw, "INSERT OR REPLACE INTO t (k, v) VALUES (?1, ?2)", [
              k,
              :binary.copy("v", rem(n, 300))
            ])

          {:ok, _} =
            Sqlite3.fetch_all_each([raw, raw], "SELECT k, v FROM t WHERE k >= ?1 LIMIT 5", [
              [k],
              ["k"]
            ])

        10 ->
          # Prepared statement used and released while others may hold it.
          {:ok, stmt} = Sqlite3.prepare(raw, "SELECT ?1, ?2")
          :ok = Sqlite3.bind(stmt, [n, "s"])
          {:row, [^n, "s"]} = Sqlite3.step(raw, stmt)
          :ok = Sqlite3.release(raw, stmt)
          {:error, _} = Sqlite3.bind(stmt, [1])

        11 ->
          # Errors on a shared connection, bind errors among them.
          {:error, _} = Sqlite3.fetch_all(raw, "SELECT * FROM missing_table", [])

          {:error, "column index out of range"} =
            Sqlite3.fetch_all_each([raw], "SELECT ?1", [[1, 2]])
      end
    end

    counts =
      1..@concurrency
      |> Task.async_stream(
        fn id ->
          Enum.reduce_while(Stream.iterate(0, &(&1 + 1)), 0, fn n, _ ->
            if rem(n, 32) == 0 and System.monotonic_time(:millisecond) > deadline do
              {:halt, n}
            else
              step.(id, n)
              {:cont, n}
            end
          end)
        end,
        max_concurrency: @concurrency,
        timeout: :infinity
      )
      |> Enum.map(fn {:ok, n} -> n end)

    Sqlite3.close(raw)
    assert Enum.sum(counts) > 0
  end
end
