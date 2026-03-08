defmodule EKV.ClientSubscriptions do
  @moduledoc false

  use GenServer

  defstruct [:name, subscriptions: %{}]

  def start_link(opts) do
    opts = Keyword.validate!(opts, [:name])
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: server_name(name))
  end

  def server_name(name), do: :"#{name}_ekv_client_subscriptions"

  def subscribe(name, prefix), do: GenServer.call(server_name(name), {:subscribe, self(), prefix})

  def unsubscribe(name, prefix),
    do: GenServer.call(server_name(name), {:unsubscribe, self(), prefix})

  @impl true
  def init(opts) do
    {:ok, %__MODULE__{name: Keyword.fetch!(opts, :name)}}
  end

  @impl true
  def handle_call({:subscribe, pid, prefix}, _from, %__MODULE__{} = state) do
    entry = Map.get(state.subscriptions, pid, %{monitor: nil, prefixes: MapSet.new()})

    if MapSet.member?(entry.prefixes, prefix) do
      {:reply, :ok, state}
    else
      if entry.monitor == nil do
        :pg.join(EKV.client_any_sub_group(state.name), pid)
      end

      :pg.join(EKV.client_sub_group(state.name, prefix), pid)

      entry =
        entry
        |> ensure_monitor(pid)
        |> Map.update!(:prefixes, &MapSet.put(&1, prefix))

      {:reply, :ok, put_in(state.subscriptions[pid], entry)}
    end
  end

  def handle_call({:unsubscribe, pid, prefix}, _from, %__MODULE__{} = state) do
    case Map.get(state.subscriptions, pid) do
      nil ->
        {:reply, :ok, state}

      %{monitor: monitor, prefixes: prefixes} = entry ->
        if MapSet.member?(prefixes, prefix) do
          :pg.leave(EKV.client_sub_group(state.name, prefix), pid)
        end

        prefixes = MapSet.delete(prefixes, prefix)

        if MapSet.size(prefixes) == 0 do
          :pg.leave(EKV.client_any_sub_group(state.name), pid)
          Process.demonitor(monitor, [:flush])
          {:reply, :ok, %{state | subscriptions: Map.delete(state.subscriptions, pid)}}
        else
          entry = %{entry | prefixes: prefixes}
          {:reply, :ok, put_in(state.subscriptions[pid], entry)}
        end
    end
  end

  @impl true
  def handle_info({:DOWN, monitor, :process, pid, _reason}, %__MODULE__{} = state) do
    state =
      case Map.get(state.subscriptions, pid) do
        %{monitor: ^monitor} ->
          %{state | subscriptions: Map.delete(state.subscriptions, pid)}

        _ ->
          state
      end

    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp ensure_monitor(%{monitor: nil} = entry, pid),
    do: %{entry | monitor: Process.monitor(pid)}

  defp ensure_monitor(entry, _pid), do: entry
end
