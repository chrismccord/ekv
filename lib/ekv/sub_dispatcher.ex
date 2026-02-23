defmodule EKV.SubDispatcher do
  @moduledoc false
  alias EKV.SubDispatcher

  use GenServer

  defstruct [:name, :shard_index, :registry]

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    shard_index = Keyword.fetch!(opts, :shard_index)
    GenServer.start_link(__MODULE__, opts, name: dispatcher_name(name, shard_index))
  end

  def dispatcher_name(name, shard_index), do: :"#{name}_ekv_sub_dispatcher_#{shard_index}"

  @impl true
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    shard_index = Keyword.fetch!(opts, :shard_index)
    config = EKV.get_config(name)

    {:ok, %SubDispatcher{name: name, shard_index: shard_index, registry: config.registry}}
  end

  @max_drain 1000

  @impl true
  def handle_info({:dispatch, events}, %SubDispatcher{} = state) do
    all_events = drain_events(events, @max_drain)
    fan_out(state, all_events)
    {:noreply, state}
  end

  def handle_info(_msg, %SubDispatcher{} = state) do
    {:noreply, state}
  end

  defp drain_events(acc, 0), do: acc

  defp drain_events(acc, remaining) do
    receive do
      {:dispatch, events} -> drain_events(acc ++ events, remaining - 1)
    after
      0 -> acc
    end
  end

  defp fan_out(_state, []), do: :ok

  defp fan_out(%SubDispatcher{} = state, events) do
    %{registry: registry, name: name} = state

    # For each event, find matching pids via prefix decomposition at "/"
    # boundaries, then collect per-pid event lists.
    pid_events =
      Enum.reduce(events, %{}, fn %EKV.Event{} = event, acc ->
        pids = matching_pids(registry, event.key)

        Enum.reduce(pids, acc, fn pid, inner ->
          Map.update(inner, pid, [event], &[event | &1])
        end)
      end)

    for {pid, evts} <- pid_events do
      send(pid, {:ekv, Enum.reverse(evts), %{name: name}})
    end

    :ok
  end

  # Find all subscriber pids whose prefix matches key.
  #
  # Subscriptions match at "/" boundaries only:
  #   ""           → matches all keys
  #   "user/"      → matches "user/1", "user/abc/xyz", etc.
  #   "user/1"     → matches exactly "user/1" (no trailing / = exact match)
  #
  # For key "user/123/data", we check: "", "user/", "user/123/", "user/123/data"
  # Each check is a direct Registry.lookup (ETS hash lookup), not a scan.
  defp matching_pids(registry, key) do
    # Start with "" (match-all), then each "/" boundary, then exact key
    pids = collect_pids(registry, "", MapSet.new())
    pids = collect_slash_prefixes(registry, key, 0, pids)
    collect_pids(registry, key, pids)
  end

  defp collect_slash_prefixes(registry, key, from, acc) do
    case :binary.match(key, "/", scope: {from, byte_size(key) - from}) do
      {pos, 1} ->
        prefix = binary_part(key, 0, pos + 1)
        acc = collect_pids(registry, prefix, acc)
        collect_slash_prefixes(registry, key, pos + 1, acc)

      :nomatch ->
        acc
    end
  end

  defp collect_pids(registry, prefix, acc) do
    Enum.reduce(Registry.lookup(registry, prefix), acc, fn {pid, _}, set ->
      MapSet.put(set, pid)
    end)
  end
end
