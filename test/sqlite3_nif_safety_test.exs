defmodule EKV.Sqlite3NifSafetyTest do
  use ExUnit.Case, async: true

  alias EKV.Sqlite3

  setup do
    dir = Path.join(System.tmp_dir!(), "ekv_nif_safety_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)

    {:ok, db} = Sqlite3.open(Path.join(dir, "db.sqlite"))
    on_exit(fn -> Sqlite3.close(db) end)
    %{db: db}
  end

  # Holds the connection's lock for a while (a few hundred ms of SQLite work).
  @slow "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 2000000) SELECT count(*) FROM c"

  test "bind racing release on the same statement answers instead of crashing", %{db: db} do
    for _ <- 1..20 do
      {:ok, stmt} = Sqlite3.prepare(db, "SELECT ?1")
      holder = Task.async(fn -> Sqlite3.fetch_all(db, @slow, []) end)
      Process.sleep(20)

      # Both block on the connection lock the slow query holds; the binders,
      # queued behind the release, find the statement released.
      releaser = Task.async(fn -> Sqlite3.release(db, stmt) end)
      Process.sleep(5)
      binders = for _ <- 1..4, do: Task.async(fn -> Sqlite3.bind(stmt, [1]) end)

      assert {:ok, [[2_000_000]]} = Task.await(holder, 30_000)
      assert :ok = Task.await(releaser, 30_000)

      for b <- binders do
        assert Task.await(b, 30_000) in [:ok, {:error, ~c"statement finalized"}]
      end

      assert {:error, _} = Sqlite3.bind(stmt, [1])
      assert {:error, _} = Sqlite3.step(db, stmt)
    end
  end

  test "a bind error reports its own message while the connection fails otherwise", %{db: db} do
    # Another caller failing on the same connection must not change (or
    # free) the message a bind error is built from.
    stop = :atomics.new(1, [])

    noise =
      for _ <- 1..4 do
        Task.async(fn ->
          Stream.repeatedly(fn ->
            Sqlite3.fetch_all(db, "SELECT * FROM no_such_table_with_a_long_name", [])
          end)
          |> Enum.find(fn _ -> :atomics.get(stop, 1) == 1 end)
        end)
      end

    results =
      for _ <- 1..4 do
        Task.async(fn ->
          for _ <- 1..3_000 do
            Sqlite3.fetch_all_each([db], "SELECT ?1", [[1, 2]])
          end
        end)
      end
      |> Enum.flat_map(&Task.await(&1, 60_000))

    :atomics.put(stop, 1, 1)
    Enum.each(noise, &Task.await(&1, 60_000))

    assert Enum.uniq(results) == [{:error, "column index out of range"}]
  end
end
