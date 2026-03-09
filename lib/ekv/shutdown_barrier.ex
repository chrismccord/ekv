defmodule EKV.ShutdownBarrier do
  @moduledoc false

  use GenServer

  require Logger

  alias EKV.ShutdownBarrier

  @coordination_grace_ms 250
  @poll_interval_ms 50
  @shutdown_margin_ms 1_000

  defstruct [
    :name,
    :mode,
    :timeout,
    :log,
    :member_identity,
    :cluster_size
  ]

  def start_link(opts) do
    opts = Keyword.validate!(opts, [:name, :timeout, :log])
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: server_name(name))
  end

  def child_spec(opts) do
    name = Keyword.fetch!(opts, :name)
    timeout = Keyword.fetch!(opts, :timeout)

    %{
      id: server_name(name),
      start: {__MODULE__, :start_link, [opts]},
      restart: :permanent,
      shutdown: timeout + @shutdown_margin_ms,
      type: :worker
    }
  end

  def server_name(name), do: :"#{name}_ekv_shutdown_barrier"

  def live_all_group(name), do: {:ekv_shutdown_live, name}
  def terminal_all_group(name), do: {:ekv_shutdown_terminal, name}

  def live_member_group(name, identity), do: {:ekv_shutdown_live_member, name, identity}
  def terminal_member_group(name, identity), do: {:ekv_shutdown_terminal_member, name, identity}

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    name = Keyword.fetch!(opts, :name)
    timeout = Keyword.fetch!(opts, :timeout)
    log = Keyword.get(opts, :log, :info)
    config = EKV.Supervisor.get_config(name)

    state = %ShutdownBarrier{
      name: name,
      mode: config.mode,
      timeout: timeout,
      log: log,
      member_identity: member_identity(config),
      cluster_size: config.cluster_size
    }

    join_live_groups(state)
    {:ok, state}
  end

  @impl true
  def handle_info(_msg, %ShutdownBarrier{} = state), do: {:noreply, state}

  @impl true
  def terminate(reason, %ShutdownBarrier{} = state) do
    if graceful_shutdown?(reason) do
      maybe_log(state, :info, "shutdown barrier entered terminal state")
      join_terminal_groups(state)
      snapshot = live_snapshot(state.name)

      if should_wait?(state, snapshot) do
        maybe_log(state, :info, "shutdown barrier waiting for members to enter terminal state")
        wait_for_snapshot(state, snapshot)
      end
    end

    :ok
  end

  defp join_live_groups(%ShutdownBarrier{} = state) do
    scope = EKV.Supervisor.pg_scope(state.name)
    :ok = :pg.join(scope, live_all_group(state.name), self())

    if state.mode == :member do
      :ok = :pg.join(scope, live_member_group(state.name, state.member_identity), self())
    end

    :ok
  end

  defp join_terminal_groups(%ShutdownBarrier{} = state) do
    scope = EKV.Supervisor.pg_scope(state.name)
    :ok = :pg.join(scope, terminal_all_group(state.name), self())

    if state.mode == :member do
      :ok = :pg.join(scope, terminal_member_group(state.name, state.member_identity), self())
    end

    :ok
  end

  defp should_wait?(%ShutdownBarrier{mode: :member} = state, snapshot) do
    cond do
      proxy_mode?(state.name) ->
        false

      coordinated_shutdown_detected?(state, snapshot) ->
        true

      quorum_sensitive_wait?(state, snapshot) ->
        true

      true ->
        coordinated_shutdown_within_grace?(state, snapshot)
    end
  end

  defp should_wait?(%ShutdownBarrier{} = state, snapshot) do
    coordinated_shutdown_detected?(state, snapshot) or
      coordinated_shutdown_within_grace?(state, snapshot)
  end

  defp coordinated_shutdown_detected?(%ShutdownBarrier{} = state, snapshot) do
    terminal = current_terminal_set(state.name)

    Enum.any?(snapshot, fn pid ->
      pid != self() and MapSet.member?(terminal, pid)
    end)
  end

  defp coordinated_shutdown_within_grace?(%ShutdownBarrier{} = state, snapshot) do
    deadline = now_ms() + @coordination_grace_ms
    wait_until(deadline, fn -> coordinated_shutdown_detected?(state, snapshot) end)
  end

  defp quorum_sensitive_wait?(
         %ShutdownBarrier{cluster_size: cluster_size, mode: :member} = state,
         snapshot
       )
       when is_integer(cluster_size) do
    nonterminal_snapshot_remaining?(state.name, snapshot) and exiting_breaks_quorum?(state)
  end

  defp quorum_sensitive_wait?(%ShutdownBarrier{} = _state, _snapshot), do: false

  defp nonterminal_snapshot_remaining?(name, snapshot) do
    live = current_live_set(name)
    terminal = current_terminal_set(name)

    Enum.any?(snapshot, fn pid ->
      pid != self() and MapSet.member?(live, pid) and not MapSet.member?(terminal, pid)
    end)
  end

  defp exiting_breaks_quorum?(%ShutdownBarrier{cluster_size: cluster_size} = state) do
    remaining_members = live_member_count_after_exit(state)
    remaining_members < quorum_size(cluster_size)
  end

  defp live_member_count_after_exit(%ShutdownBarrier{} = state) do
    identities = live_member_identities(state.name)

    if logical_identity_survives_exit?(state) do
      MapSet.size(identities)
    else
      max(MapSet.size(identities) - 1, 0)
    end
  end

  defp logical_identity_survives_exit?(%ShutdownBarrier{
         mode: :member,
         name: name,
         member_identity: id
       }) do
    pg_members(name, live_member_group(name, id))
    |> Enum.any?(&(&1 != self()))
  end

  defp logical_identity_survives_exit?(%ShutdownBarrier{} = _state), do: false

  defp live_member_identities(name) do
    :pg.which_groups(EKV.Supervisor.pg_scope(name))
    |> Enum.reduce(MapSet.new(), fn
      {:ekv_shutdown_live_member, ^name, identity} = group, acc ->
        if pg_members(name, group) == [] do
          acc
        else
          MapSet.put(acc, identity)
        end

      _group, acc ->
        acc
    end)
  rescue
    _ -> MapSet.new()
  end

  defp wait_for_snapshot(%ShutdownBarrier{} = state, snapshot) do
    deadline = now_ms() + state.timeout

    wait_until(deadline, fn ->
      snapshot_terminal_or_gone?(state.name, snapshot)
    end)
  end

  defp snapshot_terminal_or_gone?(name, snapshot) do
    live = current_live_set(name)
    terminal = current_terminal_set(name)

    Enum.all?(snapshot, fn pid ->
      MapSet.member?(terminal, pid) or not MapSet.member?(live, pid)
    end)
  end

  defp wait_until(deadline, predicate) do
    cond do
      predicate.() ->
        true

      now_ms() >= deadline ->
        false

      true ->
        Process.sleep(@poll_interval_ms)
        wait_until(deadline, predicate)
    end
  end

  defp current_live_set(name), do: MapSet.new(pg_members(name, live_all_group(name)))
  defp current_terminal_set(name), do: MapSet.new(pg_members(name, terminal_all_group(name)))

  defp live_snapshot(name), do: MapSet.new(pg_members(name, live_all_group(name)))

  defp pg_members(name, group) do
    :pg.get_members(EKV.Supervisor.pg_scope(name), group)
  rescue
    _ -> []
  end

  defp proxy_mode?(name) do
    case Process.whereis(EKV.Replica.shard_name(name, 0)) do
      nil ->
        false

      pid ->
        try do
          match?(%{handoff_node: handoff_node} when not is_nil(handoff_node), :sys.get_state(pid))
        rescue
          _ -> false
        end
    end
  end

  defp member_identity(%{mode: :member, node_id: nil}), do: {:node, node()}
  defp member_identity(%{mode: :member, node_id: node_id}), do: node_id
  defp member_identity(_config), do: nil

  defp quorum_size(cluster_size), do: div(cluster_size, 2) + 1

  defp graceful_shutdown?(reason),
    do: reason in [:shutdown, :normal] or match?({:shutdown, _}, reason)

  defp maybe_log(%__MODULE__{log: false}, _level, _message), do: :ok

  defp maybe_log(%__MODULE__{name: name}, :info, message) do
    Logger.info("[EKV #{name}] #{message}")
  end

  defp now_ms, do: System.monotonic_time(:millisecond)
end
