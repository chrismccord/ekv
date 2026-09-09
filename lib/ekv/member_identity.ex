defmodule EKV.MemberIdentity do
  @moduledoc false

  # Identity is needed during replica startup, before the node is safe to route
  # client requests to. The supervisor starts MemberPresence after the replicas.
  use GenServer

  def start_link(opts) do
    opts = Keyword.validate!(opts, [:name, :node_id])
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: server_name(name))
  end

  defp server_name(name), do: :"#{name}_ekv_member_identity"

  def leave(name) do
    case Process.whereis(server_name(name)) do
      nil -> :ok
      pid -> GenServer.call(pid, :leave)
    end
  end

  @impl true
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    scope = EKV.Supervisor.pg_scope(name)
    group = EKV.MemberPresence.member_id_group(name, Keyword.fetch!(opts, :node_id))
    :ok = :pg.join(scope, group, self())
    {:ok, {scope, group}}
  end

  @impl true
  def handle_call(:leave, _from, nil), do: {:reply, :ok, nil}

  def handle_call(:leave, _from, {scope, group}) do
    :ok = :pg.leave(scope, group, self())
    {:reply, :ok, nil}
  end
end
