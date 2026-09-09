defmodule EKV.PropertyHelpers do
  @moduledoc false

  # Each generated example (including each shrink) gets fresh durable state.
  def in_directory(root, fun) do
    directory = Path.join(root, Integer.to_string(System.unique_integer([:positive])))
    File.mkdir_p!(directory)

    try do
      fun.(directory)
    after
      File.rm_rf!(directory)
    end
  end

  # Close before reopening, including when the last command is :reopen.
  # Check both on open and after every command; no handle survives a failed assertion.
  def sessions(commands, model, open, close, step, check) do
    {session, rest} = Enum.split_while(commands, &(&1 != :reopen))
    resource = open.()

    model =
      try do
        check.(resource, model)

        Enum.reduce(session, model, fn command, model ->
          model = step.(resource, command, model)
          check.(resource, model)
          model
        end)
      after
        close.(resource)
      end

    case rest do
      [] -> model
      [:reopen | rest] -> sessions(rest, model, open, close, step, check)
    end
  end

  def open_store(directory) do
    {:ok, db} = EKV.Store.open(directory, 0, :timer.hours(24), 1, :timer.hours(1))
    {db, EKV.Store.prepare_cached_stmts(db)}
  end

  def close_store({db, stmts}) do
    :ok = EKV.Store.release_stmts(db, stmts)
    :ok = EKV.Store.close(db)
  end

  def open_member(directory, name) do
    {:ok, pid} =
      EKV.start_link(
        name: name,
        data_dir: directory,
        shards: 1,
        reader_connections: 1,
        cluster_size: 1,
        node_id: "property-member",
        anti_entropy_interval: :timer.hours(1),
        gc_interval: :timer.hours(1),
        tombstone_ttl: :timer.hours(24),
        log: false
      )

    {name, pid}
  end

  def close_member({_name, pid}), do: Supervisor.stop(pid)
end
