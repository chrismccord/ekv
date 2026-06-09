defmodule EKV.Transports.SocketDist do
  @moduledoc """
  Adapter for externally supervised SocketDist-compatible transports.

  The adapter expects the configured SocketDist module to expose
  `connection/1`, `send_msg/3`, and `rpc/6`. EKV does not start or supervise
  the SocketDist instance.
  """

  @behaviour EKV.Transport

  @default_socket_dist_module C3.SocketDist

  @impl true
  def init(opts) when is_list(opts) do
    with {:ok, name} when is_atom(name) <- Keyword.fetch(opts, :name),
         socket_dist_module <- Keyword.get(opts, :socket_dist_module, @default_socket_dist_module),
         :ok <- validate_socket_dist_module(socket_dist_module) do
      {:ok, %{module: socket_dist_module, dist: apply(socket_dist_module, :connection, [name])}}
    else
      :error -> {:error, :missing_socket_dist_name}
      {:ok, other} -> {:error, {:invalid_socket_dist_name, other}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp validate_socket_dist_module(module) do
    if is_atom(module) and Code.ensure_loaded?(module) and
         function_exported?(module, :connection, 1) and function_exported?(module, :send_msg, 3) and
         function_exported?(module, :rpc, 6) do
      :ok
    else
      {:error, {:invalid_socket_dist_module, module}}
    end
  end

  @impl true
  def send(%{module: module, dist: dist}, target, message, _opts) do
    case apply(module, :send_msg, [dist, target, message]) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
      other -> {:error, {:bad_socket_dist_send_return, other}}
    end
  catch
    :exit, reason -> {:error, reason}
  end

  @impl true
  def rpc(%{module: module, dist: dist}, node, remote_module, function, args, opts) do
    case apply(module, :rpc, [dist, node, remote_module, function, args, opts]) do
      {:ok, _result} = ok -> ok
      {:raise, _exception} = raised -> raised
      {:throw, _value} = thrown -> thrown
      {:exit, _reason} = exited -> exited
      {:error, _reason} = error -> error
      other -> {:error, {:bad_socket_dist_rpc_return, other}}
    end
  catch
    :exit, reason -> {:error, reason}
  end
end
