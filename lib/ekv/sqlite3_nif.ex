defmodule EKV.Sqlite3NIF do
  @moduledoc false
  @on_load :load_nif

  def load_nif do
    priv =
      case :code.priv_dir(:ekv) do
        {:error, _} ->
          # App not loaded yet — resolve from this module's beam location
          :code.which(__MODULE__)
          |> :filename.dirname()
          |> :filename.dirname()
          |> :filename.join(~c"priv")

        dir ->
          dir
      end

    path = :filename.join(priv, ~c"ekv_sqlite3_nif")

    case :erlang.load_nif(path, 0) do
      :ok ->
        :ok

      {:error, {reason, text}} ->
        :logger.error(%{
          msg: {"EKV NIF failed to load: ~s ~s (path: ~s)", [reason, text, path]},
          domain: [:ekv]
        })

        {:error, {reason, text}}
    end
  end

  def ekv_open(_path), do: :erlang.nif_error(:not_loaded)
  def ekv_close(_db), do: :erlang.nif_error(:not_loaded)
  def ekv_execute(_db, _sql), do: :erlang.nif_error(:not_loaded)
  def ekv_prepare(_db, _sql), do: :erlang.nif_error(:not_loaded)
  def ekv_bind(_stmt, _args), do: :erlang.nif_error(:not_loaded)
  def ekv_step(_db, _stmt), do: :erlang.nif_error(:not_loaded)
  def ekv_release(_db, _stmt), do: :erlang.nif_error(:not_loaded)

  def ekv_write_entry(_db, _kv_stmt, _oplog_stmt, _kv_args, _oplog_args),
    do: :erlang.nif_error(:not_loaded)

  def ekv_read_entry(_db, _stmt, _args), do: :erlang.nif_error(:not_loaded)
  def ekv_fetch_all(_db, _sql, _args), do: :erlang.nif_error(:not_loaded)
  def ekv_backup(_source_path, _dest_path), do: :erlang.nif_error(:not_loaded)
end
