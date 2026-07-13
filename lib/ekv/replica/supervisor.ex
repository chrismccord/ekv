defmodule EKV.Replica.Supervisor do
  @moduledoc false

  use Supervisor

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    Supervisor.start_link(__MODULE__, opts, name: :"#{name}_ekv_replica_sup")
  end

  @impl true
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    num_shards = Keyword.fetch!(opts, :num_shards)
    data_dir = Keyword.fetch!(opts, :data_dir)
    config = EKV.Supervisor.get_config(name)

    replicas =
      for i <- 0..(num_shards - 1) do
        %{
          id: {EKV.Replica, i},
          start:
            {EKV.Replica, :start_link,
             [[name: name, shard_index: i, num_shards: num_shards, data_dir: data_dir]]}
        }
      end

    checkpointer =
      {EKV.WALCheckpointer,
       name: name,
       num_shards: num_shards,
       data_dir: data_dir,
       interval: config.wal_checkpoint_interval,
       wal_size_limit: config.wal_size_limit,
       log: config.log}

    # Checkpointers start only after every shard database is initialized. They
    # are listed last so normal shutdown closes their independent SQLite
    # connections before Replica writer connections.
    Supervisor.init(replicas ++ [checkpointer], strategy: :one_for_one)
  end
end
