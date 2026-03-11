defmodule EKV.MemberPresence do
  @moduledoc false

  _archdoc = ~S"""
  Publishes a ready EKV member into the EKV instance's scoped `:pg` mesh so
  clients can discover it by region.

  - decouples client routing discoverability from replica shard processes
  - advertises only after member startup is complete
  - stops advertising during blue-green handoff before the old node enters proxy mode

  Design:
  - one long-lived process per member EKV instance
  - joins `{:ekv_members, name, region}` in the instance-local `:pg` scope
  - `ClientRouter` monitors those groups to build its regional candidate set
  - `advertised?/1` is the cold-path validation check used to reject stale
    blue-green candidates before caching them as a backend
  """

  use GenServer

  def start_link(opts) do
    opts = Keyword.validate!(opts, [:name, :region])
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: server_name(name))
  end

  def server_name(name), do: :"#{name}_ekv_member_presence"

  def region_group(name, region), do: {:ekv_members, name, region}

  def advertised?(name) do
    case Process.whereis(server_name(name)) do
      nil -> false
      pid -> GenServer.call(pid, :advertised?)
    end
  end

  def member_nodes(name) do
    scope = EKV.Supervisor.pg_scope(name)

    scope
    |> :pg.which_groups()
    |> Enum.flat_map(fn
      {:ekv_members, ^name, _region} = group ->
        :pg.get_members(scope, group)

      _other ->
        []
    end)
    |> Enum.map(&node/1)
    |> Enum.uniq()
  end

  def leave(name) do
    case Process.whereis(server_name(name)) do
      nil -> :ok
      pid -> GenServer.call(pid, :leave)
    end
  end

  @impl true
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    region = Keyword.fetch!(opts, :region)
    state = %{name: name, region: region, joined?: false}
    {:ok, join_groups(state)}
  end

  @impl true
  def handle_call(:advertised?, _from, state) do
    {:reply, state.joined?, state}
  end

  @impl true
  def handle_call(:leave, _from, state) do
    {:reply, :ok, leave_groups(state)}
  end

  defp join_groups(state) do
    :ok =
      :pg.join(
        EKV.Supervisor.pg_scope(state.name),
        region_group(state.name, state.region),
        self()
      )

    %{state | joined?: true}
  end

  defp leave_groups(%{joined?: false} = state), do: state

  defp leave_groups(state) do
    :ok =
      :pg.leave(
        EKV.Supervisor.pg_scope(state.name),
        region_group(state.name, state.region),
        self()
      )

    %{state | joined?: false}
  end
end
