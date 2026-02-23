defmodule EKV.GC do
  @moduledoc false
  use GenServer

  def start_link(opts) do
    opts = Keyword.validate!(opts, [:name])
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: :"#{name}_ekv_gc")
  end

  @impl true
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    config = EKV.get_config(name)
    schedule_gc(config.gc_interval)
    {:ok, %{name: name}}
  end

  @impl true
  def handle_info(:gc_tick, state) do
    config = EKV.get_config(state.name)
    now = System.system_time(:nanosecond)
    tombstone_cutoff = now - config.tombstone_ttl * 1_000_000

    for shard <- 0..(config.num_shards - 1) do
      send(
        EKV.Replica.shard_name(state.name, shard),
        {:gc, now, tombstone_cutoff}
      )
    end

    schedule_gc(config.gc_interval)
    {:noreply, state}
  end

  def handle_info(_msg, state) do
    {:noreply, state}
  end

  defp schedule_gc(interval) do
    Process.send_after(self(), :gc_tick, interval)
  end
end
