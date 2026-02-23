defmodule EKV.SubDispatcher.Supervisor do
  @moduledoc false

  use Supervisor

  def start_link(opts) do
    opts = Keyword.validate!(opts, [:name, :num_shards])
    name = Keyword.fetch!(opts, :name)
    Supervisor.start_link(__MODULE__, opts, name: :"#{name}_ekv_sub_dispatcher_sup")
  end

  @impl true
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    num_shards = Keyword.fetch!(opts, :num_shards)

    children =
      for i <- 0..(num_shards - 1) do
        %{
          id: {EKV.SubDispatcher, i},
          start: {EKV.SubDispatcher, :start_link, [[name: name, shard_index: i]]}
        }
      end

    Supervisor.init(children, strategy: :one_for_one)
  end
end
