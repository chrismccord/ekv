defmodule EKV.TestTransport do
  @moduledoc false

  @behaviour EKV.Transport

  @impl true
  def init(opts) do
    owner = Keyword.fetch!(opts, :owner)
    send(owner, {:ekv_test_transport_init, self(), opts})

    {:ok,
     %{
       owner: owner,
       fail_best_effort?: Keyword.get(opts, :fail_best_effort?, false),
       fail_must_send?: Keyword.get(opts, :fail_must_send?, false),
       rpc_result: Keyword.get(opts, :rpc_result)
     }}
  end

  @impl true
  def send(state, target, message, opts) do
    send(state.owner, {:ekv_test_transport_send, self(), target, message, opts})

    cond do
      Keyword.get(opts, :best_effort?, false) and state.fail_best_effort? ->
        {:error, :fake_best_effort_failure}

      not Keyword.get(opts, :best_effort?, false) and state.fail_must_send? ->
        {:error, :fake_must_send_failure}

      true ->
        :ok
    end
  end

  @impl true
  def rpc(state, node, module, function, args, opts) do
    send(state.owner, {:ekv_test_transport_rpc, self(), node, module, function, args, opts})

    case state.rpc_result do
      nil -> {:ok, apply(module, function, args)}
      result -> result
    end
  end
end
