defmodule EKV.Supervisor do
  @moduledoc false
  use Supervisor

  @valid_opts [:name, :data_dir, :shards, :log, :tombstone_ttl, :gc_interval]

  def start_link(opts) do
    opts = Keyword.validate!(opts, @valid_opts)
    name = Keyword.fetch!(opts, :name)
    Supervisor.start_link(__MODULE__, opts, name: :"#{name}_ekv_sup")
  end

  @impl true
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    data_dir = Keyword.fetch!(opts, :data_dir)
    num_shards = Keyword.get(opts, :shards, 8)
    log = Keyword.get(opts, :log, :info)
    tombstone_ttl = Keyword.get(opts, :tombstone_ttl, :timer.hours(24 * 7))
    gc_interval = Keyword.get(opts, :gc_interval, :timer.minutes(5))

    registry_name = :"#{name}_ekv_registry"
    sub_tracker_name = :"#{name}_ekv_sub_tracker"
    sub_count = :atomics.new(1, signed: true)

    config = %{
      num_shards: num_shards,
      data_dir: data_dir,
      log: log,
      tombstone_ttl: tombstone_ttl,
      gc_interval: gc_interval,
      registry: registry_name,
      sub_count: sub_count
    }

    :persistent_term.put({EKV, name}, config)

    children = [
      {EKV.SubTracker, name: sub_tracker_name, sub_count: sub_count},
      {Registry, keys: :duplicate, name: registry_name, listeners: [sub_tracker_name]},
      {EKV.SubDispatcher.Supervisor, name: name, num_shards: num_shards},
      {EKV.Replica.Supervisor, name: name, num_shards: num_shards, data_dir: data_dir},
      {EKV.GC, name: name}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
