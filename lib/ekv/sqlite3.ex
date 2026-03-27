defmodule EKV.Sqlite3 do
  @moduledoc false

  alias EKV.Sqlite3NIF

  def open(path), do: Sqlite3NIF.ekv_open(path)
  def close(db), do: Sqlite3NIF.ekv_close(db)
  def execute(db, sql), do: Sqlite3NIF.ekv_execute(db, sql)
  def prepare(db, sql), do: Sqlite3NIF.ekv_prepare(db, sql)
  def bind(stmt, args), do: Sqlite3NIF.ekv_bind(stmt, args)
  def step(db, stmt), do: Sqlite3NIF.ekv_step(db, stmt)
  def release(db, stmt), do: Sqlite3NIF.ekv_release(db, stmt)

  def write_entry(
        db,
        kv_stmt,
        keyref_stmt,
        oplog_stmt,
        kv_args,
        oplog_args,
        local_origin,
        reject_cas_managed
      ),
      do:
        Sqlite3NIF.ekv_write_entry(
          db,
          kv_stmt,
          keyref_stmt,
          oplog_stmt,
          kv_args,
          oplog_args,
          local_origin,
          reject_cas_managed
        )

  def write_snapshot_entry(db, kv_stmt, kv_args),
    do: Sqlite3NIF.ekv_write_snapshot_entry(db, kv_stmt, kv_args)

  def read_entry(db, stmt, args), do: Sqlite3NIF.ekv_read_entry(db, stmt, args)
  def fetch_all(db, sql, args), do: Sqlite3NIF.ekv_fetch_all(db, sql, args)
  def backup(source_path, dest_path), do: Sqlite3NIF.ekv_backup(source_path, dest_path)

  def merge_local_progress_summary(db, entries),
    do: Sqlite3NIF.ekv_merge_local_progress_summary(db, entries)

  def replace_local_progress_summary(db, entries),
    do: Sqlite3NIF.ekv_replace_local_progress_summary(db, entries)

  def replace_peer_progress(db, member_node, entries),
    do: Sqlite3NIF.ekv_replace_peer_progress(db, member_node, entries)

  def paxos_prepare(db, key, ballot_counter, ballot_node),
    do: Sqlite3NIF.ekv_paxos_prepare(db, key, ballot_counter, ballot_node)

  def paxos_accept(db, key, ballot_c, ballot_n, value_args),
    do: Sqlite3NIF.ekv_paxos_accept(db, key, ballot_c, ballot_n, value_args)

  def paxos_promote(
        db,
        kv_force_stmt,
        keyref_stmt,
        oplog_stmt,
        key,
        ballot_c,
        ballot_n,
        origin_seq \\ nil
      ),
      do:
        Sqlite3NIF.ekv_paxos_promote(
          db,
          kv_force_stmt,
          keyref_stmt,
          oplog_stmt,
          key,
          ballot_c,
          ballot_n,
          origin_seq
        )
end
