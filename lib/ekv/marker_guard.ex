defmodule EKV.MarkerGuard do
  @moduledoc false

  _archdoc = """
  Exists to clean `blue_green: true` file marker from disk if outgoing
  node shuts down gracefully without ever having handed off (ie cold deploy).
  """

  use GenServer

  require Logger

  def start_link(opts) do
    opts = Keyword.validate!(opts, [:name, :data_dir, :log])
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: :"#{name}_ekv_marker_guard")
  end

  def mark_handoff_performed(name) do
    case Process.whereis(:"#{name}_ekv_marker_guard") do
      nil -> :ok
      pid -> GenServer.cast(pid, :handoff_performed)
    end
  end

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    {:ok,
     %{
       name: Keyword.fetch!(opts, :name),
       data_dir: Keyword.fetch!(opts, :data_dir),
       log: Keyword.get(opts, :log, :info),
       node_name: Atom.to_string(node()),
       handoff_performed: false
     }}
  end

  @impl true
  def handle_cast(:handoff_performed, state) do
    {:noreply, %{state | handoff_performed: true}}
  end

  @impl true
  def terminate(reason, state) do
    if graceful_shutdown?(reason) do
      cleanup_marker(state)
    end

    :ok
  end

  defp graceful_shutdown?(reason),
    do: reason in [:shutdown, :normal] or match?({:shutdown, _}, reason)

  defp cleanup_marker(%{handoff_performed: true}), do: :ok

  defp cleanup_marker(%{handoff_performed: false} = state) do
    marker_path = Path.join(state.data_dir, "current")

    case File.read(marker_path) do
      {:ok, contents} ->
        if String.trim(contents) == state.node_name do
          case File.rm(marker_path) do
            :ok ->
              if state.log do
                Logger.info("[EKV #{state.name}] cleared blue-green marker on graceful shutdown")
              end

            {:error, :enoent} ->
              :ok

            {:error, reason} ->
              if state.log do
                Logger.warning(
                  "[EKV #{state.name}] failed to clear blue-green marker: #{inspect(reason)}"
                )
              end
          end
        end

      {:error, :enoent} ->
        :ok

      {:error, reason} ->
        if state.log do
          Logger.warning(
            "[EKV #{state.name}] failed to read blue-green marker during shutdown: #{inspect(reason)}"
          )
        end
    end
  end
end
