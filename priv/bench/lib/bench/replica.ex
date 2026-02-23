defmodule Bench.Replica do
  @moduledoc """
  Helper functions called on replica VMs via :erpc.call.
  All functions are named (no lambdas across nodes).
  """

  def start do
    Process.sleep(:infinity)
  end

  def start_ekv(opts) do
    {:ok, pid} = EKV.start_link(opts)
    Process.unlink(pid)
    {:ok, pid}
  end

  def stop_ekv(name) do
    sup_name = :"#{name}_ekv_sup"

    case Process.whereis(sup_name) do
      nil -> :ok
      pid -> Supervisor.stop(pid, :normal, 5000)
    end
  end

  def bulk_put(name, n, prefix) do
    for i <- 1..n do
      EKV.put(name, "#{prefix}#{i}", %{i: i, data: :crypto.strong_rand_bytes(64)})
    end

    :ok
  end

  def bulk_put_binary(name, n, prefix, value_size) do
    value = :crypto.strong_rand_bytes(value_size)

    for i <- 1..n do
      EKV.put(name, "#{prefix}#{i}", value)
    end

    :ok
  end

  def single_put(name, key, value) do
    EKV.put(name, key, value)
  end

  def single_get(name, key) do
    EKV.get(name, key)
  end

  def bulk_delete(name, n, prefix) do
    for i <- 1..n do
      EKV.delete(name, "#{prefix}#{i}")
    end

    :ok
  end

  def count_keys(name, prefix) when prefix != "" do
    length(EKV.keys(name, prefix))
  end

  def count_all_keys(name) do
    config = EKV.get_config(name)
    now = System.system_time(:nanosecond)

    for shard <- 0..(config.num_shards - 1) do
      readers = :persistent_term.get({EKV, name, :readers, shard})
      {db, _stmt} = elem(readers, 0)
      EKV.Store.count_live(db, now)
    end
    |> Enum.sum()
  end

  def list_keys(name, prefix) do
    EKV.keys(name, prefix)
  end

  def start_watcher(name, notify_pid) do
    spawn(fn -> watcher_loop(name, notify_pid) end)
  end

  def watcher_loop(name, notify_pid) do
    receive do
      {:watch, key, ref} ->
        send(notify_pid, {:watching, ref})
        busy_poll_local(name, key)
        send(notify_pid, {:found, ref})
        watcher_loop(name, notify_pid)

      :stop ->
        :ok
    end
  end

  defp busy_poll_local(name, key) do
    case EKV.get(name, key) do
      nil -> busy_poll_local(name, key)
      _ -> :ok
    end
  end

  def flush_shards(name) do
    config = EKV.get_config(name)

    for shard <- 0..(config.num_shards - 1) do
      shard_name = :"#{name}_ekv_replica_#{shard}"
      :sys.get_state(shard_name)
    end

    :ok
  end

  @doc "Start a subscriber that forwards event timestamps to a remote pid"
  def start_event_subscriber(name, prefix, notify_pid) do
    spawn(fn ->
      EKV.subscribe(name, prefix)
      event_notify_loop(notify_pid)
    end)
  end

  defp event_notify_loop(notify_pid) do
    receive do
      {:ekv, events, _meta} ->
        t = System.monotonic_time(:microsecond)
        send(notify_pid, {:sub_event, t, length(events)})
        event_notify_loop(notify_pid)

      :stop ->
        :ok
    end
  end

  @doc "Start a subscriber that just drains its mailbox (for overhead benchmarks)"
  def start_drain_subscriber(name, prefix) do
    spawn(fn ->
      EKV.subscribe(name, prefix)
      drain_loop()
    end)
  end

  defp drain_loop do
    receive do
      _ -> drain_loop()
    end
  end
end
