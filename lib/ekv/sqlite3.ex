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

  def write_entry(db, kv_stmt, oplog_stmt, kv_args, oplog_args),
    do: Sqlite3NIF.ekv_write_entry(db, kv_stmt, oplog_stmt, kv_args, oplog_args)

  def read_entry(db, stmt, args), do: Sqlite3NIF.ekv_read_entry(db, stmt, args)
  def fetch_all(db, sql, args), do: Sqlite3NIF.ekv_fetch_all(db, sql, args)
  def backup(source_path, dest_path), do: Sqlite3NIF.ekv_backup(source_path, dest_path)
end
