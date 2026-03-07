defmodule EKV.QuorumGate do
  @moduledoc false

  use GenServer

  require Logger

  def start_link(opts) do
    opts = Keyword.validate!(opts, [:name, :timeout, :log])
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: :"#{name}_ekv_quorum_gate")
  end

  @impl true
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    timeout = Keyword.fetch!(opts, :timeout)
    log = Keyword.get(opts, :log, :info)

    if log do
      Logger.info("[EKV #{name}] waiting for CAS quorum during startup (timeout=#{timeout}ms)")
    end

    started_at = System.monotonic_time(:millisecond)

    case EKV.await_quorum(name, timeout) do
      :ok ->
        if log do
          elapsed = System.monotonic_time(:millisecond) - started_at
          Logger.info("[EKV #{name}] startup quorum reached after #{elapsed}ms")
        end

        {:ok, %{name: name}}

      {:error, reason} ->
        if log do
          Logger.error("[EKV #{name}] startup quorum wait failed: #{inspect(reason)}")
        end

        {:stop, reason}
    end
  end
end
