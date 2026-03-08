defmodule EKV.MemberPresence do
  @moduledoc false

  use GenServer

  def start_link(opts) do
    opts = Keyword.validate!(opts, [:name, :region])
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: server_name(name))
  end

  def server_name(name), do: :"#{name}_ekv_member_presence"

  def region_group(name, region), do: {:ekv_members, name, region}

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
  def handle_call(:leave, _from, state) do
    {:reply, :ok, leave_groups(state)}
  end

  @impl true
  def terminate(_reason, state) do
    leave_groups(state)
    :ok
  end

  defp join_groups(state) do
    :ok = :pg.join(region_group(state.name, state.region), self())
    %{state | joined?: true}
  end

  defp leave_groups(%{joined?: false} = state), do: state

  defp leave_groups(state) do
    :ok = :pg.leave(region_group(state.name, state.region), self())
    %{state | joined?: false}
  end
end
