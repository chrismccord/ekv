defmodule EKV.WALCheckpointerTest do
  use ExUnit.Case, async: false

  @moduletag :capture_log

  setup do
    name = :"ekv_wal_#{System.unique_integer([:positive])}"
    data_dir = Path.join(System.tmp_dir!(), "ekv_wal_test_#{name}")

    on_exit(fn -> File.rm_rf!(data_dir) end)

    %{name: name, data_dir: data_dir}
  end

  test "writer commits do not run foreground checkpoints", %{name: name, data_dir: data_dir} do
    start_ekv(name, data_dir)
    writer = :sys.get_state(EKV.Replica.shard_name(name, 0)).db

    assert {:ok, [[0]]} =
             EKV.Sqlite3.fetch_all(writer, "PRAGMA wal_autocheckpoint", [])

    assert {:ok, [[67_108_864]]} =
             EKV.Sqlite3.fetch_all(writer, "PRAGMA journal_size_limit", [])
  end

  test "independent checkpointer advances the WAL without changing data", %{
    name: name,
    data_dir: data_dir
  } do
    start_ekv(name, data_dir)

    Enum.each(1..250, fn i ->
      :ok = EKV.put(name, "checkpoint/#{i}", :crypto.strong_rand_bytes(512))
    end)

    assert {:ok,
            %{
              busy: 0,
              log_frames: log_frames,
              checkpointed_frames: log_frames,
              complete?: true,
              wal_bytes: wal_bytes
            }} = EKV.WALCheckpointer.checkpoint(name, 0)

    assert log_frames > 0
    assert wal_bytes > 0
    assert is_binary(EKV.get(name, "checkpoint/250"))

    assert %{
             status: :open,
             checkpoint_count: 1,
             starved?: false
           } = EKV.WALCheckpointer.stats(name, 0)
  end

  test "reader-pinned WAL growth is reported and recovers", %{name: name, data_dir: data_dir} do
    start_ekv(name, data_dir, wal_size_limit: 4_096)
    :ok = EKV.put(name, "pinned/seed", "seed")
    assert {:ok, %{complete?: true}} = EKV.WALCheckpointer.checkpoint(name, 0)

    db_path = Path.join(data_dir, "shard_0.db")
    {:ok, reader} = EKV.Store.open_reader(db_path)
    :ok = EKV.Sqlite3.execute(reader, "BEGIN")
    {:ok, stmt} = EKV.Sqlite3.prepare(reader, "SELECT value FROM kv WHERE key = ?1")
    :ok = EKV.Sqlite3.bind(stmt, ["pinned/seed"])
    assert {:row, [_value]} = EKV.Sqlite3.step(reader, stmt)

    Enum.each(1..100, fn i ->
      :ok = EKV.put(name, "pinned/#{i}", :crypto.strong_rand_bytes(512))
    end)

    assert {:ok, %{complete?: false, wal_bytes: wal_bytes}} =
             EKV.WALCheckpointer.checkpoint(name, 0)

    assert wal_bytes > 4_096
    assert %{starved?: true} = EKV.WALCheckpointer.stats(name, 0)

    :ok = EKV.Sqlite3.release(reader, stmt)
    :ok = EKV.Sqlite3.execute(reader, "COMMIT")
    :ok = EKV.Store.close(reader)

    assert {:ok, %{complete?: true}} = EKV.WALCheckpointer.checkpoint(name, 0)
    assert %{starved?: false} = EKV.WALCheckpointer.stats(name, 0)
  end

  test "completed checkpoint bounds the retained WAL on reset", %{
    name: name,
    data_dir: data_dir
  } do
    wal_size_limit = 64 * 1024
    start_ekv(name, data_dir, wal_size_limit: wal_size_limit)

    Enum.each(1..50, fn i ->
      :ok = EKV.put(name, "limit/#{i}", :crypto.strong_rand_bytes(512))
    end)

    wal_path = Path.join(data_dir, "shard_0.db-wal")
    assert File.stat!(wal_path).size > wal_size_limit
    assert {:ok, %{complete?: true}} = EKV.WALCheckpointer.checkpoint(name, 0)

    :ok = EKV.put(name, "limit/reset", "reset")
    assert File.stat!(wal_path).size == wal_size_limit
  end

  test "checkpointer restart leaves shard replicas running", %{name: name, data_dir: data_dir} do
    start_ekv(name, data_dir)
    replica = GenServer.whereis(EKV.Replica.shard_name(name, 0))
    checkpointer = GenServer.whereis(EKV.WALCheckpointer.process_name(name))
    monitor = Process.monitor(checkpointer)

    Process.exit(checkpointer, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^checkpointer, :killed}

    _ = :sys.get_state(:"#{name}_ekv_replica_sup")
    restarted = GenServer.whereis(EKV.WALCheckpointer.process_name(name))

    assert is_pid(restarted)
    refute restarted == checkpointer
    assert GenServer.whereis(EKV.Replica.shard_name(name, 0)) == replica
    assert %{status: :open} = EKV.WALCheckpointer.stats(name, 0)
  end

  test "background coordinator checkpoints every shard round-robin", %{
    name: name,
    data_dir: data_dir
  } do
    start_ekv(name, data_dir, shards: 3)
    checkpointer = GenServer.whereis(EKV.WALCheckpointer.process_name(name))

    Enum.each(1..3, fn _ -> send(checkpointer, :checkpoint) end)
    _ = :sys.get_state(checkpointer)

    for shard_index <- 0..2 do
      assert %{checkpoint_count: 1, status: :open} =
               EKV.WALCheckpointer.stats(name, shard_index)
    end
  end

  defp start_ekv(name, data_dir, opts \\ []) do
    opts =
      Keyword.merge(
        [
          name: name,
          data_dir: data_dir,
          shards: 1,
          log: false,
          gc_interval: :timer.hours(1),
          wal_checkpoint_interval: :timer.hours(1)
        ],
        opts
      )

    start_supervised!({EKV, opts})
  end
end
