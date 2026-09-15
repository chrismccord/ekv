defmodule EKV.SchemaMigrationTest do
  use ExUnit.Case, async: true

  alias EKV.{Sqlite3, Store}

  setup do
    data_dir = Path.join(System.tmp_dir!(), "ekv_migration_#{System.unique_integer([:positive])}")
    File.mkdir_p!(data_dir)
    on_exit(fn -> File.rm_rf!(data_dir) end)

    {:ok, db} = open(data_dir)
    stmts = Store.prepare_cached_stmts(db)
    ts = System.system_time(:nanosecond)
    value = :erlang.term_to_binary(%{kept: true})
    expires_at = ts + :timer.hours(1) * 1_000_000

    for {key, binary, expires, deleted} <- [
          {"live", value, expires_at, nil},
          {"deleted", nil, nil, ts}
        ] do
      assert {:ok, true, _, _} =
               Store.write_entry(
                 db,
                 stmts.kv_upsert,
                 stmts.keyref_upsert,
                 stmts.oplog_insert,
                 key,
                 binary,
                 ts,
                 "B",
                 expires,
                 deleted
               )
    end

    assert {:ok, true} =
             Store.paxos_accept(db, "accepted", 5, "B", [value, ts, "A", expires_at, nil])

    Store.persist_node_id(db, "B")
    Store.set_meta(db, "ballot_counter", 5)
    Store.merge_local_progress(db, "A", 999)
    Store.update_peer_progress(db, "C", "A", 999)
    Store.release_stmts(db, stmts)

    # Exact v3 layout, including a cursor poisoned by foreign-origin recovery.
    :ok = Sqlite3.execute(db, "ALTER TABLE kv_oplog DROP COLUMN value_origin")
    :ok = Store.set_meta(db, "schema_version", 3)
    before = %{kv: rows(db, "kv"), paxos: rows(db, "kv_paxos"), oplog: rows(db, "kv_oplog")}
    :ok = Store.close(db)
    %{data_dir: data_dir, before: before}
  end

  test "v3 upgrades once without changing committed or accepted state", context do
    %{data_dir: data_dir, before: before} = context
    {:ok, db} = open(data_dir)

    assert Store.get_meta(db, "schema_version") == 4
    assert rows(db, "kv") == before.kv
    assert rows(db, "kv_paxos") == before.paxos
    assert rows(db, "kv_oplog") == []
    assert Store.local_progress_summary(db) == %{}
    assert Store.get_peer_progress(db, "C") == %{}
    assert Store.get_meta(db, "local_origin_seq") == 2
    assert Store.get_meta(db, "ballot_counter") == 5
    assert {:ok, [[0], [0]]} = Sqlite3.fetch_all(db, "SELECT oplog_refs FROM kv_keyrefs", [])

    stmts = Store.prepare_cached_stmts(db)

    assert {:ok, _, _, "A", _, _, _, 3, 3} =
             Store.paxos_promote(
               db,
               stmts.kv_force_upsert,
               stmts.keyref_upsert,
               stmts.oplog_insert,
               "accepted",
               5,
               "B"
             )

    assert Store.local_progress_summary(db) == %{"B" => 3}

    assert {:ok, [["B", 3, "A"]]} =
             Sqlite3.fetch_all(
               db,
               "SELECT origin_node, origin_seq, value_origin FROM kv_oplog",
               []
             )

    Store.release_stmts(db, stmts)
    Store.close(db)

    {:ok, db} = open(data_dir)
    assert Store.get_meta(db, "schema_version") == 4
    assert length(rows(db, "kv_oplog")) == 1
    Store.close(db)
  end

  test "normal startup restores the local head after clearing v3 replay", %{data_dir: data_dir} do
    name = :"migration_startup_#{System.unique_integer([:positive])}"
    start_supervised!({EKV, name: name, data_dir: data_dir, shards: 1, node_id: "B", log: false})
    shard = EKV.Replica.shard_name(name, 0)
    state = :sys.get_state(shard)
    assert state.local_origin_seq == 2
    assert Store.local_progress_summary(state.db) == %{"B" => 2}
    assert EKV.get(name, "live") == %{kept: true}
    assert :ok = EKV.put(name, "after_upgrade", :kept)
    assert :sys.get_state(shard).local_origin_seq == 3
    assert Store.max_origin_seq(state.db, "B") == 3
  end

  test "failed migration rolls back DDL and data and can be retried", context do
    %{data_dir: data_dir, before: before} = context
    path = Path.join(data_dir, "shard_0.db")
    {:ok, db} = Sqlite3.open(path)

    :ok =
      Sqlite3.execute(db, """
      CREATE TRIGGER block_migration BEFORE DELETE ON kv_oplog BEGIN
        SELECT RAISE(ABORT, 'migration blocked');
      END
      """)

    Store.close(db)

    assert_raise MatchError, fn -> open(data_dir) end

    {:ok, db} = Sqlite3.open(path)
    assert Store.get_meta(db, "schema_version") == 3
    assert rows(db, "kv") == before.kv
    assert rows(db, "kv_paxos") == before.paxos
    assert rows(db, "kv_oplog") == before.oplog
    assert Store.local_progress_summary(db)["A"] == 999
    {:ok, columns} = Sqlite3.fetch_all(db, "PRAGMA table_info(kv_oplog)", [])
    refute Enum.any?(columns, &(Enum.at(&1, 1) == "value_origin"))

    :ok = Sqlite3.execute(db, "DROP TRIGGER block_migration")
    Store.close(db)
    {:ok, db} = open(data_dir)
    assert Store.get_meta(db, "schema_version") == 4
    Store.close(db)
  end

  defp open(data_dir), do: Store.open(data_dir, 0, :timer.hours(24 * 7), 1, :timer.minutes(5))

  defp rows(db, table) do
    {:ok, rows} = Sqlite3.fetch_all(db, "SELECT * FROM #{table}", [])
    rows
  end
end
