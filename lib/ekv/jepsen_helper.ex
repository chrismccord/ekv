defmodule EKV.JepsenHelper do
  @moduledoc false

  def start_ekv(opts) do
    {:ok, pid} = EKV.start_link(opts)
    Process.unlink(pid)
    {:ok, pid}
  end

  def stop_ekv(name) do
    sup_name = :"#{name}_ekv_sup"

    case Process.whereis(sup_name) do
      nil -> :ok
      pid -> Supervisor.stop(pid, :normal, 5_000)
    end
  end
end
