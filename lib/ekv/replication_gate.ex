defmodule EKV.ReplicationGate do
  @moduledoc false

  use GenServer

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: process_name(name))
  end

  @doc false
  def process_name(name), do: :"#{name}_ekv_replication_gate"

  @impl true
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    num_shards = Keyword.fetch!(opts, :num_shards)

    for shard_index <- 0..(num_shards - 1) do
      shard_name = EKV.Replica.shard_name(name, shard_index)

      case GenServer.whereis(shard_name) do
        pid when is_pid(pid) -> :ok = GenServer.call(pid, :start_replication, :infinity)
        nil -> exit({:replica_not_ready, shard_name})
      end
    end

    {:ok, %{name: name}}
  end

  @impl true
  def handle_info({:replica_ready, shard_index, pid}, state)
      when is_integer(shard_index) and is_pid(pid) do
    if GenServer.whereis(EKV.Replica.shard_name(state.name, shard_index)) == pid do
      :ok = GenServer.call(pid, :start_replication, :infinity)
    end

    {:noreply, state}
  end

  def handle_info(_message, state), do: {:noreply, state}
end
