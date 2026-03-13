defmodule EKV.AdversarialVerificationTest do
  use ExUnit.Case

  @moduletag :capture_log
  @moduletag timeout: 120_000

  alias EKV.TestCluster

  defp unique_name(prefix) do
    :"#{prefix}_#{System.unique_integer([:positive])}"
  end

  defp start_cluster(peers, ekv_name, opts) do
    gc_interval = Keyword.get(opts, :gc_interval, 300)
    tombstone_ttl = Keyword.get(opts, :tombstone_ttl, 1_200)

    peers
    |> Enum.with_index(1)
    |> Enum.each(fn {{_pid, node}, node_id} ->
      data_dir = "/tmp/ekv_adversarial_#{node}_#{ekv_name}"
      TestCluster.rpc!(node, File, :rm_rf!, [data_dir])

      TestCluster.start_ekv(
        node,
        name: ekv_name,
        data_dir: data_dir,
        shards: 1,
        log: false,
        gc_interval: gc_interval,
        tombstone_ttl: tombstone_ttl,
        cluster_size: length(peers),
        node_id: node_id
      )
    end)
  end

  defp cleanup_data(peers, ekv_name) do
    for {_pid, node} <- peers do
      data_dir = "/tmp/ekv_adversarial_#{node}_#{ekv_name}"

      try do
        TestCluster.rpc!(node, File, :rm_rf!, [data_dir])
      catch
        _, _ -> :ok
      end
    end
  end

  defp restart_cluster_ekv(peers, ekv_name, opts) do
    gc_interval = Keyword.get(opts, :gc_interval, 300)
    tombstone_ttl = Keyword.get(opts, :tombstone_ttl, 1_200)

    peers
    |> Enum.with_index(1)
    |> Enum.each(fn {{_pid, node}, node_id} ->
      TestCluster.stop_ekv(node, ekv_name)

      TestCluster.assert_eventually(fn ->
        TestCluster.ekv_stopped?(node, ekv_name)
      end)

      data_dir = "/tmp/ekv_adversarial_#{node}_#{ekv_name}"

      TestCluster.start_ekv(
        node,
        name: ekv_name,
        data_dir: data_dir,
        shards: 1,
        log: false,
        gc_interval: gc_interval,
        tombstone_ttl: tombstone_ttl,
        cluster_size: length(peers),
        node_id: node_id
      )

      TestCluster.assert_eventually(fn ->
        not TestCluster.ekv_stopped?(node, ekv_name)
      end)
    end)
  end

  defp assert_tombstone_purged(node, ekv_name, key, timeout \\ 2_000) do
    TestCluster.assert_eventually(
      fn ->
        TestCluster.store_get(node, ekv_name, key) == nil
      end,
      timeout: timeout
    )
  end

  test "long partition beyond tombstone_ttl quarantines member instead of syncing unsafe state" do
    peers = TestCluster.start_peers(2)
    on_exit(fn -> TestCluster.stop_peers(peers) end)

    [{_, n1}, {_, n2}] = peers
    ekv_name = unique_name(:ttl_partition)

    start_cluster(peers, ekv_name, gc_interval: 100, tombstone_ttl: 400)
    on_exit(fn -> cleanup_data(peers, ekv_name) end)

    key = "adversarial/zombie"

    :ok = TestCluster.rpc!(n1, EKV, :put, [ekv_name, key, "alive"])

    TestCluster.assert_eventually(fn ->
      TestCluster.rpc!(n2, EKV, :get, [ekv_name, key]) == "alive"
    end)

    TestCluster.disconnect_nodes(n1, n2)
    Process.sleep(400)

    :ok = TestCluster.rpc!(n1, EKV, :delete, [ekv_name, key])

    TestCluster.assert_eventually(fn ->
      TestCluster.rpc!(n1, EKV, :get, [ekv_name, key]) == nil and
        TestCluster.rpc!(n2, EKV, :get, [ekv_name, key]) == "alive"
    end)

    assert_tombstone_purged(n1, ekv_name, key)

    TestCluster.reconnect_nodes(n1, n2)

    TestCluster.assert_eventually(
      fn ->
        TestCluster.rpc!(n1, EKV, :get, [ekv_name, key]) == nil and
          TestCluster.rpc!(n2, EKV, :get, [ekv_name, key]) == "alive"
      end,
      timeout: 3_000
    )

    shard_name = :"#{ekv_name}_ekv_replica_0"

    TestCluster.assert_eventually(fn ->
      state1 = TestCluster.rpc!(n1, :sys, :get_state, [shard_name])
      state2 = TestCluster.rpc!(n2, :sys, :get_state, [shard_name])

      not Map.has_key?(state1.remote_shards, n2) and
        not Map.has_key?(state2.remote_shards, n1)
    end)
  end

  test "short partition below tombstone_ttl reconnects and syncs normally" do
    peers = TestCluster.start_peers(2)
    on_exit(fn -> TestCluster.stop_peers(peers) end)

    [{_, n1}, {_, n2}] = peers
    ekv_name = unique_name(:ttl_short_partition)

    start_cluster(peers, ekv_name, gc_interval: 300, tombstone_ttl: 5_000)
    on_exit(fn -> cleanup_data(peers, ekv_name) end)

    key = "adversarial/short"
    :ok = TestCluster.rpc!(n1, EKV, :put, [ekv_name, key, "v1"])

    TestCluster.assert_eventually(fn ->
      TestCluster.rpc!(n2, EKV, :get, [ekv_name, key]) == "v1"
    end)

    TestCluster.disconnect_nodes(n1, n2)
    Process.sleep(500)
    :ok = TestCluster.rpc!(n1, EKV, :delete, [ekv_name, key])
    TestCluster.reconnect_nodes(n1, n2)

    TestCluster.assert_eventually(fn ->
      TestCluster.rpc!(n1, EKV, :get, [ekv_name, key]) == nil and
        TestCluster.rpc!(n2, EKV, :get, [ekv_name, key]) == nil
    end)
  end

  test "long partition quarantine survives EKV restart on both sides (node_id markers)" do
    peers = TestCluster.start_peers(2)
    on_exit(fn -> TestCluster.stop_peers(peers) end)

    [{_, n1}, {_, n2}] = peers
    ekv_name = unique_name(:ttl_partition_both_restart)

    start_cluster(peers, ekv_name, gc_interval: 100, tombstone_ttl: 400)
    on_exit(fn -> cleanup_data(peers, ekv_name) end)

    key = "adversarial/restart_quarantine"

    :ok = TestCluster.rpc!(n1, EKV, :put, [ekv_name, key, "alive"])

    TestCluster.assert_eventually(fn ->
      TestCluster.rpc!(n2, EKV, :get, [ekv_name, key]) == "alive"
    end)

    TestCluster.disconnect_nodes(n1, n2)
    Process.sleep(400)
    :ok = TestCluster.rpc!(n1, EKV, :delete, [ekv_name, key])

    TestCluster.assert_eventually(fn ->
      TestCluster.rpc!(n1, EKV, :get, [ekv_name, key]) == nil and
        TestCluster.rpc!(n2, EKV, :get, [ekv_name, key]) == "alive"
    end)

    assert_tombstone_purged(n1, ekv_name, key)

    # Restart both EKV supervisors while still partitioned.
    restart_cluster_ekv(peers, ekv_name, gc_interval: 100, tombstone_ttl: 400)

    # Heal after both sides forgot in-memory state. Persisted down markers should still quarantine.
    TestCluster.reconnect_nodes(n1, n2)

    TestCluster.assert_eventually(
      fn ->
        TestCluster.rpc!(n1, EKV, :get, [ekv_name, key]) == nil and
          TestCluster.rpc!(n2, EKV, :get, [ekv_name, key]) == "alive"
      end,
      timeout: 3_000
    )

    shard_name = :"#{ekv_name}_ekv_replica_0"

    TestCluster.assert_eventually(fn ->
      state1 = TestCluster.rpc!(n1, :sys, :get_state, [shard_name])
      state2 = TestCluster.rpc!(n2, :sys, :get_state, [shard_name])

      not Map.has_key?(state1.remote_shards, n2) and
        not Map.has_key?(state2.remote_shards, n1)
    end)
  end

  test "chunked full sync should emit a final non-zero seq even when entry count is exact multiple of chunk size" do
    name = unique_name(:chunk_exact_multiple)
    data_dir = Path.join(System.tmp_dir!(), "ekv_adversarial_#{name}")

    {:ok, pid} =
      EKV.start_link(
        name: name,
        data_dir: data_dir,
        shards: 1,
        log: false,
        sync_chunk_size: 10,
        gc_interval: :timer.hours(1),
        tombstone_ttl: :timer.hours(24 * 7)
      )

    on_exit(fn ->
      Process.exit(pid, :shutdown)
      File.rm_rf!(data_dir)
    end)

    for i <- 1..20 do
      key = String.pad_leading("#{i}", 3, "0")
      :ok = EKV.put(name, "sync/#{key}", "v#{i}")
    end

    shard_name = :"#{name}_ekv_replica_0"
    fake_node = :chunk_peer@fake

    :sys.replace_state(shard_name, fn state ->
      %{state | remote_shards: Map.put(state.remote_shards, fake_node, self())}
    end)

    :erlang.trace(Process.whereis(shard_name), true, [:send])

    config = EKV.Supervisor.get_config(name)
    tombstone_cutoff = System.system_time(:nanosecond) - config.tombstone_ttl * 1_000_000
    state = :sys.get_state(shard_name)
    my_seq = EKV.Store.max_seq(state.db)

    send(
      shard_name,
      {:continue_full_sync, fake_node, nil, tombstone_cutoff, my_seq, config.sync_chunk_size}
    )

    Process.sleep(200)
    :sys.get_state(shard_name)

    :erlang.trace(Process.whereis(shard_name), false, [:send])

    sync_details = collect_trace_sync_details()

    assert Enum.any?(sync_details, fn {_count, seq} -> seq > 0 end),
           "expected at least one final chunk with seq>0, got #{inspect(sync_details)}"
  end

  test "chunked delta sync should emit a final non-zero seq even when oplog count is exact multiple of chunk size" do
    name = unique_name(:delta_exact_multiple)
    data_dir = Path.join(System.tmp_dir!(), "ekv_adversarial_#{name}")

    {:ok, pid} =
      EKV.start_link(
        name: name,
        data_dir: data_dir,
        shards: 1,
        log: false,
        sync_chunk_size: 10,
        gc_interval: :timer.hours(1),
        tombstone_ttl: :timer.hours(24 * 7)
      )

    on_exit(fn ->
      Process.exit(pid, :shutdown)
      File.rm_rf!(data_dir)
    end)

    for i <- 1..20 do
      :ok = EKV.put(name, "delta/#{i}", "v#{i}")
    end

    shard_name = :"#{name}_ekv_replica_0"
    fake_node = :delta_peer@fake

    :sys.replace_state(shard_name, fn state ->
      %{state | remote_shards: Map.put(state.remote_shards, fake_node, self())}
    end)

    :erlang.trace(Process.whereis(shard_name), true, [:send])

    config = EKV.Supervisor.get_config(name)
    state = :sys.get_state(shard_name)
    my_seq = EKV.Store.max_seq(state.db)

    send(shard_name, {:continue_delta_sync, fake_node, 0, my_seq, config.sync_chunk_size})

    Process.sleep(200)
    :sys.get_state(shard_name)

    :erlang.trace(Process.whereis(shard_name), false, [:send])

    sync_details = collect_trace_sync_details()

    assert Enum.any?(sync_details, fn {_count, seq} -> seq > 0 end),
           "expected at least one final delta chunk with seq>0, got #{inspect(sync_details)}"
  end

  defp collect_trace_sync_details do
    collect_trace_sync_details([])
  end

  defp collect_trace_sync_details(acc) do
    receive do
      {:trace, _, :send, {:ekv_sync, _, _, entries, seq}, _} ->
        collect_trace_sync_details([{length(entries), seq} | acc])

      {:trace, _, :send, {:ekv, 1, :sync, {_, _, entries, seq}, _meta}, _} ->
        collect_trace_sync_details([{length(entries), seq} | acc])

      {:trace, _, :send, _, _} ->
        collect_trace_sync_details(acc)
    after
      100 -> Enum.reverse(acc)
    end
  end
end
