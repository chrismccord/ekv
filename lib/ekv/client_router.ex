defmodule EKV.ClientRouter do
  @moduledoc false

  _archdoc = ~S"""
  Client-side backend selector for `mode: :client`.

  - keeps the hot path cheap by caching the selected backend in ETS
  - discovers candidate members from the EKV instance's scoped `:pg` region groups
  - applies region-order routing and short cooldowns after transport failures
  - coordinates cold-path waits for `wait_for_route`

  Design:
  - `backend/1` is the optimized hot path:
    - read `current_backend` from ETS
    - if missing, ask the GenServer to select one
  - the GenServer is the cold path:
    - monitors `:pg` group membership per preferred region
    - updates in-memory `members_by_region`
    - handles failure notifications and route waiters
  - candidate selection is local and deterministic:
    - first configured region with available members
    - stable node-name ordering within the region
  - route discovery only trusts EKV's scoped `:pg` membership
    - it does not probe arbitrary `Node.list/0` peers
  """

  use GenServer

  alias EKV.ClientRouter

  @cooldown_ms 1_000
  @await_timeout_margin 1_000
  @route_probe_timeout 2_000

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

  def await_backend(name, timeout_ms) do
    GenServer.call(
      router_name(name),
      {:await_backend, timeout_ms},
      timeout_ms + @await_timeout_margin
    )
  end

  def next_backend(name, failed_backend) do
    GenServer.call(router_name(name), {:backend_after_failure, failed_backend})
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
     %ClientRouter{
       name: name,
       region_routing: config.region_routing,
       monitors: monitors,
       members_by_region: members_by_region
     }}
  end

  @impl true
  def handle_call(:backend, _from, %ClientRouter{} = state) do
    {reply, new_state} = ensure_backend(state)
    {:reply, reply, reply_waiters_if_ready(new_state, reply)}
  end

  def handle_call({:backend_after_failure, failed_backend}, _from, %ClientRouter{} = state) do
    state = mark_backend_failed_in_router(state, failed_backend)
    {reply, new_state} = ensure_backend(state)
    {:reply, reply, reply_waiters_if_ready(new_state, reply)}
  end

  def handle_call({:await_backend, timeout_ms}, from, %ClientRouter{} = state) do
    case ensure_backend(state) do
      {{:ok, backend}, %ClientRouter{} = new_state} ->
        {:reply, {:ok, backend}, reply_waiters_if_ready(new_state, {:ok, backend})}

      {{:error, :unavailable}, %ClientRouter{} = new_state} ->
        if timeout_ms == 0 do
          {:reply, {:error, :timeout}, new_state}
        else
          {:noreply, add_waiter(new_state, from, timeout_ms)}
        end
    end
  end

  @impl true
  def handle_info({:mark_backend_failed, node}, %ClientRouter{} = state) do
    {:noreply, mark_backend_failed_in_router(state, node)}
  end

  def handle_info({:prime_member_visibility, backend}, %ClientRouter{} = state) do
    prime_member_visibility(state.name, backend)
    {:noreply, state}
  end

  def handle_info({:nodedown, node}, %ClientRouter{} = state) do
    {:noreply, handle_nodedown(state, node)}
  end

  def handle_info({ref, :join, _group, pids}, %ClientRouter{} = state) do
    {:noreply, handle_pg_membership(state, ref, pids, :join)}
  end

  def handle_info({ref, :leave, _group, pids}, %ClientRouter{} = state) do
    {:noreply, handle_pg_membership(state, ref, pids, :leave)}
  end

  def handle_info({:await_backend_timeout, mon_ref}, %ClientRouter{} = state) do
    case Map.pop(state.waiters, mon_ref) do
      {nil, _waiters} ->
        {:noreply, state}

      {waiter, waiters} ->
        GenServer.reply(waiter.from, {:error, :timeout})
        cleanup_waiter(waiter)
        {:noreply, %{state | waiters: waiters}}
    end
  end

  def handle_info({:DOWN, mon_ref, :process, _pid, _reason}, %ClientRouter{} = state) do
    case Map.pop(state.waiters, mon_ref) do
      {nil, _waiters} ->
        {:noreply, state}

      {waiter, waiters} ->
        cleanup_waiter(waiter)
        {:noreply, %{state | waiters: waiters}}
    end
  end

  def handle_info(_msg, %ClientRouter{} = state), do: {:noreply, state}

  defp monitor_region_groups(name, region_routing) do
    scope = EKV.Supervisor.pg_scope(name)

    Enum.reduce(region_routing, {%{}, %{}}, fn region, {monitors, members_by_region} ->
      group = EKV.MemberPresence.region_group(name, region)
      {ref, members} = :pg.monitor(scope, group)
      nodes = members |> Enum.map(&node/1) |> MapSet.new()
      new_monitors = Map.put(monitors, ref, %{region: region})
      new_members = Map.put(members_by_region, region, nodes)

      {new_monitors, new_members}
    end)
  end

  defp ensure_backend(%ClientRouter{name: name} = state) do
    case cached_backend(name) do
      {:ok, backend} -> {{:ok, backend}, state}
      :error -> select_backend(state)
    end
  end

  defp add_waiter(%ClientRouter{} = state, from, timeout_ms) do
    {pid, _tag} = from
    mon_ref = Process.monitor(pid)
    timer_ref = Process.send_after(self(), {:await_backend_timeout, mon_ref}, timeout_ms)
    waiter = %{from: from, mon_ref: mon_ref, timer_ref: timer_ref}
    %{state | waiters: Map.put(state.waiters, mon_ref, waiter)}
  end

  defp maybe_reply_waiters(%ClientRouter{waiters: waiters} = state) when map_size(waiters) == 0,
    do: state

  defp maybe_reply_waiters(%ClientRouter{} = state) do
    case ensure_backend(state) do
      {{:ok, backend}, new_state} -> reply_waiters_if_ready(new_state, {:ok, backend})
      {{:error, :unavailable}, new_state} -> new_state
    end
  end

  defp reply_waiters_if_ready(%ClientRouter{waiters: waiters} = state, {:ok, backend})
       when map_size(waiters) > 0 do
    Enum.each(waiters, fn {_mon_ref, waiter} ->
      GenServer.reply(waiter.from, {:ok, backend})
      cleanup_waiter(waiter)
    end)

    %{state | waiters: %{}}
  end

  defp reply_waiters_if_ready(%ClientRouter{} = state, _reply), do: state

  defp select_backend(%ClientRouter{} = state) do
    case pick_valid_backend(state, true) do
      {:ok, backend} ->
        put_backend(state.name, backend)
        send(self(), {:prime_member_visibility, backend})
        {{:ok, backend}, state}

      :error ->
        case pick_valid_backend(state, false) do
          {:ok, backend} ->
            put_backend(state.name, backend)
            send(self(), {:prime_member_visibility, backend})
            {{:ok, backend}, state}

          :error ->
            clear_backend_for_name(state.name)
            {{:error, :unavailable}, state}
        end
    end
  end

  defp pick_valid_backend(%ClientRouter{} = state, respect_cooldown?) do
    state
    |> candidate_nodes(respect_cooldown?)
    |> Enum.find_value(:error, fn node ->
      case route_candidate?(state.name, node) do
        :valid ->
          {:ok, node}

        :invalid ->
          mark_candidate_invalid(state, node)
          false

        :unknown ->
          # Scoped :pg already says this node is a candidate. If the validation
          # RPC times out during a cold start, prefer availability over
          # blacklisting the member as invalid.
          {:ok, node}
      end
    end)
  end

  defp candidate_nodes(%ClientRouter{} = state, respect_cooldown?) do
    table = table_name(state.name)
    region_candidates(state, table, respect_cooldown?)
  end

  defp route_candidate?(name, node) do
    try do
      if :erpc.call(node, EKV.MemberPresence, :advertised?, [name], @route_probe_timeout) do
        :valid
      else
        :invalid
      end
    catch
      :exit, {{:erpc, :noconnection}, _stack} -> :invalid
      :exit, {{:nodedown, _node}, _stack} -> :invalid
      :exit, {{:erpc, :timeout}, _stack} -> :unknown
      :exit, _reason -> :unknown
    end
  end

  defp prime_member_visibility(name, backend) do
    try do
      backend
      |> :erpc.call(EKV.MemberPresence, :member_nodes, [name], @route_probe_timeout)
      |> Enum.reject(&(&1 == node()))
      |> Task.async_stream(
        fn member_node -> Node.connect(member_node) end,
        ordered: false,
        timeout: @route_probe_timeout,
        on_timeout: :kill_task
      )
      |> Stream.run()
    catch
      _, _reason -> :ok
    end
  end

  defp handle_pg_membership(%ClientRouter{} = state, ref, pids, op)
       when is_reference(ref) and is_list(pids) do
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

  defp handle_nodedown(%ClientRouter{} = state, node) do
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

  defp mark_backend_failed_in_router(%ClientRouter{} = state, node) do
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

  defp mark_candidate_invalid(%ClientRouter{} = state, node) do
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
    |> Enum.flat_map(fn region ->
      state.members_by_region
      |> Map.get(region, MapSet.new())
      |> Enum.reject(fn node ->
        respect_cooldown? and cooled_down?(table, node)
      end)
      |> Enum.sort_by(&Atom.to_string/1)
    end)
  end

  defp now_ms, do: System.monotonic_time(:millisecond)
end
