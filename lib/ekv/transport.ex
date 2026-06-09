defmodule EKV.Transport do
  @moduledoc """
  Minimal adapter boundary for EKV data-plane traffic.

  EKV uses this boundary for member shard sends and routed client RPC. The
  default adapter keeps Erlang distribution behavior; callers may provide a
  volatile ordered lane such as SocketDist without EKV owning that lane's
  supervision.
  """

  @type config :: %{module: module(), opts: keyword()}
  @type handle :: {module(), term()}
  @type target :: pid() | atom() | {atom(), node()}

  @callback init(keyword()) :: {:ok, term()} | {:error, term()}
  @callback send(term(), target(), term(), keyword()) :: :ok | {:error, term()}
  @callback rpc(term(), node(), module(), atom(), [term()], keyword()) ::
              {:ok, term()}
              | {:raise, term()}
              | {:throw, term()}
              | {:exit, term()}
              | {:error, term()}

  @doc false
  def default_config, do: %{module: EKV.Transports.Dist, opts: []}

  @doc false
  def normalize_config(nil), do: default_config()
  def normalize_config(false), do: default_config()

  def normalize_config(module) when is_atom(module) do
    validate_config!(%{module: module, opts: []})
  end

  def normalize_config({module, opts}) when is_atom(module) and is_list(opts) do
    validate_config!(%{module: module, opts: opts})
  end

  def normalize_config(opts) when is_list(opts) do
    case Keyword.fetch(opts, :module) do
      {:ok, module} when is_atom(module) ->
        adapter_opts =
          case Keyword.fetch(opts, :opts) do
            {:ok, nested_opts} when is_list(nested_opts) ->
              nested_opts

            {:ok, other} ->
              raise ArgumentError,
                    "EKV: :transport :opts must be a keyword list, got: #{inspect(other)}"

            :error ->
              Keyword.delete(opts, :module)
          end

        validate_config!(%{module: module, opts: adapter_opts})

      _ ->
        raise ArgumentError,
              "EKV: :transport keyword config requires an adapter :module"
    end
  end

  def normalize_config(other) do
    raise ArgumentError,
          "EKV: :transport must be nil, a module, {module, opts}, or a keyword config, got: #{inspect(other)}"
  end

  @doc false
  def validate_config!(%{module: module, opts: opts} = config)
      when is_atom(module) and is_list(opts) do
    unless Code.ensure_loaded?(module) and function_exported?(module, :init, 1) and
             function_exported?(module, :send, 4) and function_exported?(module, :rpc, 6) do
      raise ArgumentError,
            "EKV: transport adapter #{inspect(module)} must implement init/1, send/4, and rpc/6"
    end

    config
  end

  @doc false
  def init(%{module: module, opts: opts}) do
    case module.init(opts) do
      {:ok, adapter_state} -> {:ok, {module, adapter_state}}
      {:error, reason} -> {:error, reason}
      other -> {:error, {:bad_transport_init_return, module, other}}
    end
  end

  @doc false
  def send({module, adapter_state}, target, message, opts) when is_list(opts) do
    module.send(adapter_state, target, message, opts)
  end

  @doc false
  def rpc({module, adapter_state}, node, module_arg, function, args, opts)
      when is_atom(node) and is_atom(module_arg) and is_atom(function) and is_list(args) and
             is_list(opts) do
    module.rpc(adapter_state, node, module_arg, function, args, opts)
  end
end
