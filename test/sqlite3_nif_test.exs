defmodule EKV.Sqlite3NIFTest do
  use ExUnit.Case, async: true

  alias EKV.Sqlite3

  test "repeated explicit cleanup tolerates a closed connection and released statement" do
    for _ <- 1..100 do
      {:ok, db} = Sqlite3.open(":memory:")
      {:ok, stmt} = Sqlite3.prepare(db, "SELECT ?1")
      assert :ok = Sqlite3.bind(stmt, ["value"])
      assert {:row, ["value"]} = Sqlite3.step(db, stmt)
      assert :ok = Sqlite3.close(db)
      assert :ok = Sqlite3.close(db)
      assert {:error, _} = Sqlite3.step(db, stmt)
      assert {:error, _} = Sqlite3.execute(db, "SELECT 1")
      assert {:error, _} = Sqlite3.prepare(db, "SELECT 1")
      assert :ok = Sqlite3.release(db, stmt)
      assert {:error, _} = Sqlite3.bind(stmt, ["again"])
      assert {:error, _} = Sqlite3.release(db, stmt)
    end
  end

  test "a statement keeps its connection alive after the opening process exits" do
    parent = self()

    for _ <- 1..100 do
      {pid, ref} =
        spawn_monitor(fn ->
          {:ok, db} = Sqlite3.open(":memory:")
          {:ok, stmt} = Sqlite3.prepare(db, "SELECT ?1")
          send(parent, {self(), stmt})
        end)

      assert_receive {^pid, stmt}
      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}
      :erlang.garbage_collect()
      # No db term crossed the process boundary. Only the statement's native
      # reference keeps the connection (and its mutex) alive here.
      assert :ok = Sqlite3.bind(stmt, ["still alive"])
    end

    :erlang.garbage_collect()
  end

  test "process exit and GC clean up unreleased statements, including active transactions" do
    for close_first? <- [false, true], _ <- 1..50 do
      Task.async(fn ->
        {:ok, db} = Sqlite3.open(":memory:")

        assert :ok =
                 Sqlite3.execute(db, "CREATE TABLE t (value); BEGIN; INSERT INTO t VALUES (1)")

        {:ok, stmt} = Sqlite3.prepare(db, "SELECT value FROM t")
        assert {:row, [1]} = Sqlite3.step(db, stmt)
        if close_first?, do: assert(:ok = Sqlite3.close(db))
        # Deliberately omit release/close. Resource destructors own cleanup.
        :ok
      end)
      |> Task.await()
    end

    :erlang.garbage_collect()
  end

  test "wrong-connection operations leave the original statement usable" do
    {:ok, db} = Sqlite3.open(":memory:")
    {:ok, other} = Sqlite3.open(":memory:")
    {:ok, stmt} = Sqlite3.prepare(db, "SELECT 42")

    assert {:error, _} = Sqlite3.step(other, stmt)
    assert {:error, _} = Sqlite3.release(other, stmt)
    assert {:row, [42]} = Sqlite3.step(db, stmt)
    assert :ok = Sqlite3.release(db, stmt)
    assert :ok = Sqlite3.close(db)
    assert :ok = Sqlite3.close(other)
  end

  test "binding and reading boundary values repeatedly resets and reuses a statement" do
    {:ok, db} = Sqlite3.open(":memory:")
    {:ok, stmt} = Sqlite3.prepare(db, "SELECT ?1")

    values = [
      "",
      <<0, 255, 0, 128>>,
      :binary.copy(<<0, 255>>, 512 * 1024),
      -9_223_372_036_854_775_808,
      9_223_372_036_854_775_807,
      1.5,
      nil
    ]

    for _ <- 1..10, value <- values do
      assert :ok = Sqlite3.bind(stmt, [value])
      assert {:row, [^value]} = Sqlite3.step(db, stmt)
      assert :done = Sqlite3.step(db, stmt)
    end

    assert :ok = Sqlite3.release(db, stmt)
    assert :ok = Sqlite3.close(db)
  end

  test "SQL and argument failures leave the connection and statement usable" do
    assert_raise ArgumentError, fn -> Sqlite3.open(:not_a_path) end
    {:ok, db} = Sqlite3.open(":memory:")
    {:ok, stmt} = Sqlite3.prepare(db, "SELECT ?1")

    for _ <- 1..50 do
      assert {:error, _} = Sqlite3.execute(db, "not SQL")
      assert {:error, _} = Sqlite3.prepare(db, "SELECT FROM")
      assert {:error, _} = Sqlite3.fetch_all(db, "SELECT FROM", [])
      assert_raise ArgumentError, fn -> Sqlite3.bind(stmt, [:not_nil]) end
      assert_raise ArgumentError, fn -> Sqlite3.bind(stmt, [%{}]) end
      assert_raise ArgumentError, fn -> Sqlite3.bind(stmt, [9_223_372_036_854_775_808]) end
      assert {:error, _} = Sqlite3.bind(stmt, [1, 2])
      assert :ok = Sqlite3.bind(stmt, [42])
      assert {:row, [42]} = Sqlite3.step(db, stmt)
      assert :done = Sqlite3.step(db, stmt)
    end

    assert :ok = Sqlite3.release(db, stmt)
    assert :ok = Sqlite3.close(db)
  end

  test "mid-transaction argument and SQL failures roll back progress replacement" do
    {:ok, db} = Sqlite3.open(":memory:")

    assert :ok =
             Sqlite3.execute(db, """
             CREATE TABLE kv_origin_progress (origin_node TEXT PRIMARY KEY, last_seq INTEGER);
             """)

    assert :ok = Sqlite3.replace_local_progress_summary(db, [{"original", 7}])

    for _ <- 1..50 do
      assert_raise ArgumentError, fn ->
        Sqlite3.replace_local_progress_summary(db, [{"partial", 1}, :invalid])
      end

      assert {:ok, [["original", 7]]} =
               Sqlite3.fetch_all(db, "SELECT origin_node, last_seq FROM kv_origin_progress", [])

      assert {:error, _} =
               Sqlite3.replace_local_progress_summary(db, [{"duplicate", 1}, {"duplicate", 2}])

      assert {:ok, [["original", 7]]} =
               Sqlite3.fetch_all(db, "SELECT origin_node, last_seq FROM kv_origin_progress", [])

      # A new transaction must also succeed: no leaked transaction or mutex.
      assert :ok = Sqlite3.replace_local_progress_summary(db, [{"original", 7}])
    end

    assert :ok = Sqlite3.close(db)
  end

  test "multiple processes use independent statements on a shared connection" do
    {:ok, db} = Sqlite3.open(":memory:")

    1..8
    |> Task.async_stream(
      fn worker ->
        for i <- 1..50 do
          {:ok, stmt} = Sqlite3.prepare(db, "SELECT ?1, ?2")
          assert :ok = Sqlite3.bind(stmt, [worker, i])
          assert {:row, [^worker, ^i]} = Sqlite3.step(db, stmt)
          assert :ok = Sqlite3.release(db, stmt)
        end
      end,
      max_concurrency: 8
    )
    |> Enum.each(fn result -> assert {:ok, _} = result end)

    assert :ok = Sqlite3.close(db)
  end
end
