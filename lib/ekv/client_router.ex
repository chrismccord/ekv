defmodule EKV.ClientRouter do
  @moduledoc false

  use GenServer

  @cooldown_ms 1_000
  @await_timeout_margin 1_000
  @route_probe_timeout 500

  defstruct [:name, :region_routing, waiters: %{}, monitors: %{}, members_by_region: %{}]

  def start_link(opts) do
    opts = Keyword.validate!(opts, [:name])
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: router_name(name))
  end

  def router_name(name), do: :"#{name}_ekv_client_router"
  def table_name(name), do: :"#{name}_ekv_client_table"

  def backend(name) do
    case cached_backend(name) do
      {:ok, backend} -> {:ok, backend}
      :error -> GenServer.call(router_name(name), :backend)
    end
  end

  def current_backend(name) do
    case backend(name) do
      {:ok, backend} -> backend
      {:error, :unavailable} -> nil
    end
  end

  def await_backend(name, timeout_ms) do
    GenServer.call(
      router_name(name),
      {:await_backend, timeout_ms},
      timeout_ms + @await_timeout_margin
    )
  end

  def mark_backend_failed(name, node) do
    table = table_name(name)

    unless cooled_down?(table, node) do
      send(router_name(name), {:mark_backend_failed, node})
    end

    :ok
  rescue
    ArgumentError -> :ok
  end

  @impl true
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    config = EKV.Supervisor.get_config(name)

    :ok = :net_kernel.monitor_nodes(true)

    :ets.new(table_name(name), [
      :named_table,
      :set,
      :public,
      read_concurrency: true,
      write_concurrency: true
    ])

    {monitors, members_by_region} = monitor_region_groups(name, config.region_routing)

    {:ok,
     %__MODULE__{
       name: name,
       region_routing: config.region_routing,
       monitors: monitors,
       members_by_region: members_by_region
     }}
  end

  @impl true
  def handle_call(:backend, _from, state) do
    {reply, new_state} = ensure_backend(state)
    {:reply, reply, reply_waiters_if_ready(reply, new_state)}
  end

  def handle_call({:await_backend, timeout_ms}, from, state) do
    case ensure_backend(state) do
      {{:ok, backend}, new_state} ->
        {:reply, {:ok, backend}, reply_waiters_if_ready({:ok, backend}, new_state)}

      {{:error, :unavailable}, new_state} ->
        if timeout_ms == 0 do
          {:reply, {:error, :timeout}, new_state}
        else
          {:noreply, add_waiter(new_state, from, timeout_ms)}
        end
    end
  end

  @impl true
  def handle_info({:mark_backend_failed, node}, state) do
    {:noreply, mark_backend_failed_in_router(state, node)}
  end

  def handle_info({:nodedown, node}, state) do
    {:noreply, handle_nodedown(state, node)}
  end

  def handle_info({ref, :join, _group, pids}, state) do
    {:noreply, handle_pg_membership(state, ref, pids, :join)}
  end

  def handle_info({ref, :leave, _group, pids}, state) do
    {:noreply, handle_pg_membership(state, ref, pids, :leave)}
  end

  def handle_info({:await_backend_timeout, mon_ref}, state) do
    case Map.pop(state.waiters, mon_ref) do
      {nil, _waiters} ->
        {:noreply, state}

      {waiter, waiters} ->
        GenServer.reply(waiter.from, {:error, :timeout})
        cleanup_waiter(waiter)
        {:noreply, %{state | waiters: waiters}}
    end
  end

  def handle_info({:DOWN, mon_ref, :process, _pid, _reason}, state) do
    case Map.pop(state.waiters, mon_ref) do
      {nil, _waiters} ->
        {:noreply, state}

      {waiter, waiters} ->
        cleanup_waiter(waiter)
        {:noreply, %{state | waiters: waiters}}
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    Enum.each(state.waiters, fn {_mon_ref, waiter} ->
      cleanup_waiter(waiter)
    end)

    :ok
  end

  defp monitor_region_groups(name, region_routing) do
    scope = EKV.Supervisor.pg_scope(name)

    Enum.reduce(region_routing, {%{}, %{}}, fn region, {monitors, members_by_region} ->
      group = EKV.MemberPresence.region_group(name, region)
      {ref, members} = :pg.monitor(scope, group)
      nodes = members |> Enum.map(&node/1) |> MapSet.new()

      {
        Map.put(monitors, ref, %{region: region}),
        Map.put(members_by_region, region, nodes)
      }
    end)
  end

  defp ensure_backend(%__MODULE__{name: name} = state) do
    case cached_backend(name) do
      {:ok, backend} -> {{:ok, backend}, state}
      :error -> select_backend(state)
    end
  end

  defp add_waiter(state, from, timeout_ms) do
    {pid, _tag} = from
    mon_ref = Process.monitor(pid)
    timer_ref = Process.send_after(self(), {:await_backend_timeout, mon_ref}, timeout_ms)
    waiter = %{from: from, mon_ref: mon_ref, timer_ref: timer_ref}
    %{state | waiters: Map.put(state.waiters, mon_ref, waiter)}
  end

  defp maybe_reply_waiters(%__MODULE__{waiters: waiters} = state) when map_size(waiters) == 0,
    do: state

  defp maybe_reply_waiters(state) do
    case ensure_backend(state) do
      {{:ok, backend}, new_state} -> reply_waiters_if_ready({:ok, backend}, new_state)
      {{:error, :unavailable}, new_state} -> new_state
    end
  end

  defp reply_waiters_if_ready({:ok, backend}, %__MODULE__{waiters: waiters} = state)
       when map_size(waiters) > 0 do
    Enum.each(waiters, fn {_mon_ref, waiter} ->
      GenServer.reply(waiter.from, {:ok, backend})
      cleanup_waiter(waiter)
    end)

    %{state | waiters: %{}}
  end

  defp reply_waiters_if_ready(_reply, state), do: state

  defp select_backend(%__MODULE__{} = state) do
    case pick_valid_backend(state, true) do
      {:ok, backend} ->
        put_backend(state.name, backend)
        prime_member_visibility(state.name, backend)
        {{:ok, backend}, state}

      :error ->
        case pick_valid_backend(state, false) do
          {:ok, backend} ->
            put_backend(state.name, backend)
            prime_member_visibility(state.name, backend)
            {{:ok, backend}, state}

          :error ->
            clear_backend_for_name(state.name)
            {{:error, :unavailable}, state}
        end
    end
  end

  defp pick_valid_backend(%__MODULE__{} = state, respect_cooldown?) do
    state
    |> candidate_nodes(respect_cooldown?)
    |> Enum.find_value(:error, fn node ->
      if route_candidate?(state.name, node) do
        {:ok, node}
      else
        mark_candidate_invalid(state, node)
        false
      end
    end)
  end

  defp candidate_nodes(%__MODULE__{} = state, respect_cooldown?) do
    table = table_name(state.name)

    case region_candidates(state, table, respect_cooldown?) do
      [] -> connected_member_candidates(state, table, respect_cooldown?)
      candidates -> candidates
    end
  end

  defp route_candidate?(name, node) do
    try do
      :erpc.call(node, EKV.MemberPresence, :advertised?, [name], @route_probe_timeout)
    catch
      :exit, _reason -> false
    end
  end

  defp prime_member_visibility(name, backend) do
    try do
      backend
      |> :erpc.call(EKV.MemberPresence, :member_nodes, [name], @route_probe_timeout)
      |> Enum.each(fn node ->
        if node != node() do
          Node.connect(node)
        end
      end)
    catch
      :exit, _reason -> :ok
    end
  end

  defp handle_pg_membership(state, ref, pids, op) do
    case Map.fetch(state.monitors, ref) do
      :error ->
        state

      {:ok, %{region: region}} ->
        current = Map.get(state.members_by_region, region, MapSet.new())

        updated =
          case op do
            :join -> Enum.reduce(pids, current, fn pid, acc -> MapSet.put(acc, node(pid)) end)
            :leave -> Enum.reduce(pids, current, fn pid, acc -> MapSet.delete(acc, node(pid)) end)
          end

        state
        |> Map.put(:members_by_region, Map.put(state.members_by_region, region, updated))
        |> maybe_reply_waiters()
    end
  end

  defp handle_nodedown(state, node) do
    table = table_name(state.name)

    if cached_backend_from_table(table) == {:ok, node} do
      clear_backend_from_table(table)
      maybe_reply_waiters(state)
    else
      state
    end
  end

  defp cached_backend(name) do
    table = table_name(name)

    case cached_backend_from_table(table) do
      {:ok, backend} ->
        if cooled_down?(table, backend) or not connected?(backend) do
          clear_backend_for_name(name)
          :error
        else
          {:ok, backend}
        end

      :error ->
        :error
    end
  end

  defp cached_backend_from_table(table) do
    case :ets.lookup(table, :current_backend) do
      [{:current_backend, backend}] -> {:ok, backend}
      _ -> :error
    end
  rescue
    ArgumentError -> :error
  end

  defp put_backend(name, backend) do
    :ets.insert(table_name(name), {:current_backend, backend})
  rescue
    ArgumentError -> :ok
  end

  defp clear_backend_for_name(name) do
    clear_backend_from_table(table_name(name))
  rescue
    ArgumentError -> :ok
  end

  defp clear_backend_from_table(table) do
    :ets.delete(table, :current_backend)
  rescue
    ArgumentError -> :ok
  end

  defp mark_backend_failed_in_router(state, node) do
    table = table_name(state.name)

    if cooled_down?(table, node) do
      state
    else
      :ets.insert(table, {{:cooldown_until, node}, now_ms() + @cooldown_ms})

      if cached_backend_from_table(table) == {:ok, node} do
        clear_backend_from_table(table)
      end

      maybe_reply_waiters(state)
    end
  rescue
    ArgumentError -> state
  end

  defp mark_candidate_invalid(state, node) do
    table = table_name(state.name)

    :ets.insert(table, {{:cooldown_until, node}, now_ms() + @cooldown_ms})

    if cached_backend_from_table(table) == {:ok, node} do
      clear_backend_from_table(table)
    end

    :ok
  rescue
    ArgumentError -> :ok
  end

  defp cleanup_waiter(waiter) do
    Process.cancel_timer(waiter.timer_ref)
    Process.demonitor(waiter.mon_ref, [:flush])
    :ok
  end

  defp cooled_down?(table, node) do
    case :ets.lookup(table, {:cooldown_until, node}) do
      [{{:cooldown_until, ^node}, deadline}] -> deadline > now_ms()
      _ -> false
    end
  rescue
    ArgumentError -> false
  end

  defp connected?(node), do: node in Node.list()

  defp region_candidates(state, table, respect_cooldown?) do
    state.region_routing
    |> Enum.find_value([], fn region ->
      candidates =
        state.members_by_region
        |> Map.get(region, MapSet.new())
        |> Enum.reject(fn node ->
          respect_cooldown? and cooled_down?(table, node)
        end)
        |> Enum.sort_by(&Atom.to_string/1)

      if candidates == [], do: false, else: candidates
    end)
  end

  defp connected_member_candidates(state, table, respect_cooldown?) do
    Node.list()
    |> Task.async_stream(
      fn node ->
        case probe_member_info(node, state.name) do
          {:ok, %{mode: :member, region: region}} -> {:ok, node, region}
          _ -> :error
        end
      end,
      ordered: false,
      timeout: @route_probe_timeout + 100,
      on_timeout: :kill_task
    )
    |> Enum.reduce(%{}, fn
      {:ok, {:ok, node, region}}, acc ->
        Map.update(acc, region, [node], &[node | &1])

      _, acc ->
        acc
    end)
    |> then(fn by_region ->
      state.region_routing
      |> Enum.find_value([], fn region ->
        candidates =
          by_region
          |> Map.get(region, [])
          |> Enum.uniq()
          |> Enum.reject(fn node ->
            respect_cooldown? and cooled_down?(table, node)
          end)
          |> Enum.sort_by(&Atom.to_string/1)

        if candidates == [], do: false, else: candidates
      end)
    end)
  end

  defp probe_member_info(node, name) do
    try do
      case :erpc.call(node, EKV, :__client_invoke__, [:info, [name]], @route_probe_timeout) do
        {:ok, result} -> {:ok, result}
        {:raise, _exception} -> :error
        {:exit, _reason} -> :error
      end
    catch
      :exit, _reason -> :error
    end
  end

  defp now_ms, do: System.monotonic_time(:millisecond)
end
