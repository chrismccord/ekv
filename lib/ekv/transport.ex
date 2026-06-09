defmodule EKV.Transport do
  @moduledoc """
  Minimal adapter boundary for EKV data-plane traffic.

  EKV uses this boundary for member shard sends and routed client RPC. The
  default adapter keeps Erlang distribution behavior; callers may provide a
  volatile ordered lane without EKV owning that lane's supervision.

  Public transport config is `nil` or `{Module, opts}`. EKV validates `Module`
  implements this behaviour and passes `opts` directly to `Module.init/1`;
  adapter option keys are adapter-owned and not interpreted by EKV.

  `init/1` prepares a lightweight adapter handle from static configuration.
  EKV calls it once per replica shard process for member traffic and may call
  it on the routed-client-RPC hot path. Adapter implementations must not start
  per-call transport workers from `init/1`; long-lived transport processes
  should be supervised by the application that owns the transport.
  """

  @type config :: %{module: module(), opts: keyword()}
  @type external_config :: nil | {module(), keyword()}
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

  def normalize_config({module, opts}) when is_atom(module) do
    unless Keyword.keyword?(opts) do
      raise ArgumentError,
            "EKV: :transport opts must be a keyword list, got: #{inspect(opts)}"
    end

    validate_config!(%{module: module, opts: opts})
  end

  def normalize_config(other) do
    raise ArgumentError,
          "EKV: :transport must be nil or {Module, opts}, got: #{inspect(other)}"
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
