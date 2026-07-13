defmodule EKV.WALCheckpointer do
  @moduledoc false

  use GenServer

  require Logger

  defstruct [
    :name,
    :tick_interval,
    :log,
    :next_shard,
    connections: %{}
  ]

  def start_link(opts) do
    opts =
      Keyword.validate!(opts, [
        :name,
        :num_shards,
        :data_dir,
        :interval,
        :wal_size_limit,
        :log
      ])

    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: process_name(name))
  end

  @doc false
  def checkpoint(name, shard_index) do
    GenServer.call(process_name(name), {:checkpoint, shard_index}, :infinity)
  end

  @doc false
  def close(name, shard_index) do
    GenServer.call(process_name(name), {:close, shard_index}, :infinity)
  end

  @doc false
  def stats(name, shard_index) do
    GenServer.call(process_name(name), {:stats, shard_index})
  end

  @doc false
  def process_name(name), do: :"#{name}_ekv_wal_checkpointer"

  @impl true
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    num_shards = Keyword.fetch!(opts, :num_shards)
    data_dir = Keyword.fetch!(opts, :data_dir)
    interval = Keyword.fetch!(opts, :interval)
    wal_size_limit = Keyword.fetch!(opts, :wal_size_limit)
    log = Keyword.fetch!(opts, :log)

    connections =
      Map.new(0..(num_shards - 1), fn shard_index ->
        path = Path.join(data_dir, "shard_#{shard_index}.db")
        {:ok, db} = EKV.Store.open_checkpointer(path, wal_size_limit)
        {:ok, [[page_size]]} = EKV.Sqlite3.fetch_all(db, "PRAGMA page_size", [])

        {shard_index,
         %{
           db: db,
           page_size: page_size,
           wal_size_limit: wal_size_limit,
           checkpoint_count: 0,
           last_checkpoint: nil,
           last_error: nil,
           starved?: false
         }}
      end)

    state = %__MODULE__{
      name: name,
      tick_interval: max(div(interval, num_shards), 1),
      log: log,
      next_shard: 0,
      connections: connections
    }

    {:ok, schedule_checkpoint(state, initial_delay(state))}
  end

  @impl true
  def handle_call({:checkpoint, shard_index}, _from, state) do
    case Map.fetch!(state.connections, shard_index) do
      %{db: nil} ->
        {:reply, {:error, :closed}, state}

      connection ->
        {result, connection} = run_checkpoint(state, shard_index, connection)
        {:reply, result, put_connection(state, shard_index, connection)}
    end
  end

  def handle_call({:close, shard_index}, _from, state) do
    connection =
      state.connections
      |> Map.fetch!(shard_index)
      |> close_connection()

    {:reply, :ok, put_connection(state, shard_index, connection)}
  end

  def handle_call({:stats, shard_index}, _from, state) do
    connection = Map.fetch!(state.connections, shard_index)
    {:reply, public_stats(connection), state}
  end

  @impl true
  def handle_info(:checkpoint, state) do
    shard_index = state.next_shard
    connection = Map.fetch!(state.connections, shard_index)

    state =
      if connection.db do
        {_result, connection} = run_checkpoint(state, shard_index, connection)
        put_connection(state, shard_index, connection)
      else
        state
      end

    next_shard = rem(shard_index + 1, map_size(state.connections))
    state = %{state | next_shard: next_shard}

    {:noreply, schedule_checkpoint(state, state.tick_interval)}
  end

  @impl true
  def terminate(_reason, state) do
    Enum.each(state.connections, fn {_shard_index, connection} ->
      _connection = close_connection(connection)
    end)

    :ok
  end

  defp run_checkpoint(state, shard_index, connection) do
    started_at = System.monotonic_time(:microsecond)

    result =
      case EKV.Sqlite3.fetch_all(connection.db, "PRAGMA wal_checkpoint(PASSIVE)", []) do
        {:ok, [[busy, log_frames, checkpointed_frames]]}
        when is_integer(busy) and is_integer(log_frames) and is_integer(checkpointed_frames) ->
          duration_us = System.monotonic_time(:microsecond) - started_at
          wal_bytes = wal_bytes(connection.page_size, log_frames)

          {:ok,
           %{
             busy: busy,
             log_frames: log_frames,
             checkpointed_frames: checkpointed_frames,
             wal_bytes: wal_bytes,
             duration_us: duration_us,
             complete?: busy == 0 and checkpointed_frames == log_frames
           }}

        {:error, reason} ->
          {:error, reason}
      end

    connection = record_result(state, shard_index, connection, result)
    {result, connection}
  end

  defp record_result(state, shard_index, connection, {:ok, stats}) do
    starved? = stats.wal_bytes > connection.wal_size_limit and not stats.complete?

    if starved? and not connection.starved? and state.log do
      Logger.warning(
        "[EKV #{state.name}] shard #{shard_index} WAL checkpoint is starved " <>
          "wal_bytes=#{stats.wal_bytes} log_frames=#{stats.log_frames} " <>
          "checkpointed_frames=#{stats.checkpointed_frames} busy=#{stats.busy}"
      )
    end

    %{
      connection
      | checkpoint_count: connection.checkpoint_count + 1,
        last_checkpoint: stats,
        last_error: nil,
        starved?: starved?
    }
  end

  defp record_result(state, shard_index, connection, {:error, reason}) do
    if state.log and connection.last_error != reason do
      Logger.warning(
        "[EKV #{state.name}] shard #{shard_index} WAL checkpoint failed: #{inspect(reason)}"
      )
    end

    %{
      connection
      | checkpoint_count: connection.checkpoint_count + 1,
        last_checkpoint: %{error: reason},
        last_error: reason
    }
  end

  defp close_connection(%{db: nil} = connection), do: connection

  defp close_connection(connection) do
    :ok = EKV.Store.close(connection.db)
    %{connection | db: nil}
  end

  defp schedule_checkpoint(state, delay) do
    _timer = Process.send_after(self(), :checkpoint, delay)
    state
  end

  defp initial_delay(state) do
    1 + :erlang.phash2(state.name, state.tick_interval)
  end

  defp put_connection(state, shard_index, connection) do
    %{state | connections: Map.put(state.connections, shard_index, connection)}
  end

  defp wal_bytes(_page_size, 0), do: 0
  defp wal_bytes(page_size, frames), do: 32 + frames * (page_size + 24)

  defp public_stats(connection) do
    %{
      status: if(connection.db, do: :open, else: :closed),
      checkpoint_count: connection.checkpoint_count,
      last_checkpoint: connection.last_checkpoint,
      starved?: connection.starved?
    }
  end
end
