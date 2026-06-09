defmodule EKV.Transports.Dist do
  @moduledoc """
  Default EKV transport using Erlang distribution.
  """

  @behaviour EKV.Transport

  @impl true
  def init(_opts), do: {:ok, nil}

  @impl true
  def send(_state, target, message, opts) when is_list(opts) do
    if Keyword.get(opts, :best_effort?, false) do
      case :erlang.send_nosuspend(target, message, [:noconnect]) do
        true -> :ok
        false -> {:error, :busy_dist}
      end
    else
      Kernel.send(target, message)
      :ok
    end
  catch
    :exit, reason -> {:error, reason}
  end

  @impl true
  def rpc(_state, node, module, function, args, opts)
      when is_atom(node) and is_atom(module) and is_atom(function) and is_list(args) and
             is_list(opts) do
    timeout = Keyword.get(opts, :timeout, :infinity)

    try do
      {:ok, :erpc.call(node, module, function, args, timeout)}
    catch
      :exit, reason -> {:error, reason}
    end
  end
end
