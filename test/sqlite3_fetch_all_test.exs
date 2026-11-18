defmodule EKV.Sqlite3FetchAllTest do
  use ExUnit.Case, async: true

  alias EKV.Sqlite3

  setup do
    dir = Path.join(System.tmp_dir!(), "ekv_fetch_all_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)

    path = Path.join(dir, "db.sqlite")
    {:ok, writer} = Sqlite3.open(path)
    {:ok, [["wal"]]} = Sqlite3.fetch_all(writer, "PRAGMA journal_mode=WAL", [])
    :ok = Sqlite3.execute(writer, "CREATE TABLE t (k TEXT PRIMARY KEY, v INTEGER)")
    {:ok, reader} = Sqlite3.open(path)

    on_exit(fn ->
      Sqlite3.close(reader)
      Sqlite3.close(writer)
    end)

    %{writer: writer, reader: reader}
  end

  @scan "SELECT k, v FROM t WHERE k >= ?1 AND k < ?2 ORDER BY k"

  test "a repeated query sees every write made between its calls", %{writer: w, reader: r} do
    for i <- 1..50 do
      {:ok, []} = Sqlite3.fetch_all(w, "INSERT INTO t (k, v) VALUES (?1, ?2)", ["k#{pad(i)}", i])
      {:ok, rows} = Sqlite3.fetch_all(r, @scan, ["k", "l"])
      assert length(rows) == i
      assert List.last(rows) == ["k#{pad(i)}", i]
    end

    {:ok, []} = Sqlite3.fetch_all(w, "DELETE FROM t WHERE k = ?1", ["k#{pad(1)}"])
    {:ok, rows} = Sqlite3.fetch_all(r, @scan, ["k", "l"])
    assert length(rows) == 49
    refute ["k#{pad(1)}", 1] in rows
  end

  test "a cached statement holds no read transaction between calls", %{writer: w, reader: r} do
    {:ok, []} = Sqlite3.fetch_all(w, "INSERT INTO t (k, v) VALUES ('a', 1)", [])
    {:ok, [["a", 1]]} = Sqlite3.fetch_all(r, @scan, ["a", "b"])
    {:ok, []} = Sqlite3.fetch_all(w, "INSERT INTO t (k, v) VALUES ('b', 2)", [])

    # A reader still inside a read transaction would hold the WAL back.
    assert {:ok, [[0, _log, _checkpointed]]} =
             Sqlite3.fetch_all(w, "PRAGMA wal_checkpoint(TRUNCATE)", [])
  end

  test "more distinct queries than the cache holds all answer correctly", %{reader: r} do
    for _round <- 1..3, n <- 1..40 do
      assert {:ok, [[value]]} = Sqlite3.fetch_all(r, "SELECT ?1 + #{n}", [1])
      assert value == n + 1
    end
  end

  test "bindings do not carry over to the next call", %{reader: r} do
    assert {:ok, [[1, 2]]} = Sqlite3.fetch_all(r, "SELECT ?1, ?2", [1, 2])
    assert {:ok, [[3, nil]]} = Sqlite3.fetch_all(r, "SELECT ?1, ?2", [3])
  end

  test "a failed call leaves the connection and its cached statement usable", %{reader: r} do
    assert {:ok, [[1]]} = Sqlite3.fetch_all(r, "SELECT ?1", [1])
    assert_raise ArgumentError, fn -> Sqlite3.fetch_all(r, "SELECT ?1", [make_ref()]) end
    assert {:ok, [[2]]} = Sqlite3.fetch_all(r, "SELECT ?1", [2])

    assert {:error, _} = Sqlite3.fetch_all(r, "SELECT FROM nowhere", [])
    assert {:error, _} = Sqlite3.fetch_all(r, "", [])
    assert {:ok, [[3]]} = Sqlite3.fetch_all(r, "SELECT ?1", [3])
  end

  test "statements follow a schema change", %{writer: w, reader: r} do
    {:ok, []} = Sqlite3.fetch_all(w, "INSERT INTO t (k, v) VALUES ('a', 1)", [])
    assert {:ok, [["a", 1]]} = Sqlite3.fetch_all(r, "SELECT * FROM t", [])
    :ok = Sqlite3.execute(w, "ALTER TABLE t ADD COLUMN extra INTEGER DEFAULT 7")
    assert {:ok, [["a", 1, 7]]} = Sqlite3.fetch_all(r, "SELECT * FROM t", [])
  end

  test "fetch_all_each runs the query on each connection in order", %{writer: w, reader: r} do
    {:ok, []} = Sqlite3.fetch_all(w, "INSERT INTO t (k, v) VALUES ('a', 1), ('b', 2)", [])

    assert {:ok, [[["a", 1]], [["b", 2]], []]} =
             Sqlite3.fetch_all_each([r, w, r], "SELECT k, v FROM t WHERE k = ?1", [
               ["a"],
               ["b"],
               ["c"]
             ])
  end

  test "fetch_all_each answers the first error", %{reader: r} do
    assert {:error, _} = Sqlite3.fetch_all_each([r, r], "SELECT FROM nowhere", [[], []])
    assert_raise ArgumentError, fn -> Sqlite3.fetch_all_each([r, r], "SELECT ?1", [[1]]) end
    assert {:ok, [[[1]], [[2]]]} = Sqlite3.fetch_all_each([r, r], "SELECT ?1", [[1], [2]])
  end

  defp pad(i), do: String.pad_leading(Integer.to_string(i), 3, "0")
end
