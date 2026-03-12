defmodule EKV.AntiEntropyTest do
  use ExUnit.Case

  @moduletag :capture_log
  @moduletag timeout: 60_000

  alias EKV.TestCluster

  defp unique_name(prefix) do
    :"#{prefix}_#{System.unique_integer([:positive])}"
  end

  defp data_dir(node, ekv_name) do
    "/tmp/ekv_anti_entropy_test_#{node}_#{ekv_name}"
  end

  defp start_member(node, ekv_name, node_id, opts) do
    shards = Keyword.get(opts, :shards, 1)
    anti_entropy_interval = Keyword.get(opts, :anti_entropy_interval, false)
    sync_chunk_size = Keyword.get(opts, :sync_chunk_size, 500)
    gc_interval = Keyword.get(opts, :gc_interval, :timer.hours(1))
    tombstone_ttl = Keyword.get(opts, :tombstone_ttl, :timer.hours(24 * 7))
    cluster_size = Keyword.fetch!(opts, :cluster_size)

    TestCluster.start_ekv(
      node,
      name: ekv_name,
      data_dir: data_dir(node, ekv_name),
      shards: shards,
      log: false,
      gc_interval: gc_interval,
      tombstone_ttl: tombstone_ttl,
      sync_chunk_size: sync_chunk_size,
      cluster_size: cluster_size,
      node_id: node_id,
      anti_entropy_interval: anti_entropy_interval
    )
  end

  defp key_for_shard(prefix, shard_index, num_shards) do
    Stream.iterate(1, &(&1 + 1))
    |> Enum.find_value(fn i ->
      key = "#{prefix}/#{shard_index}/#{i}"
      if EKV.Replica.shard_index_for(key, num_shards) == shard_index, do: key
    end)
  end

  defp trigger_gc(node, ekv_name, shard_index, tombstone_ttl) do
    now = System.system_time(:nanosecond)
    tombstone_cutoff = now - tombstone_ttl * 1_000_000

    TestCluster.rpc!(node, :erlang, :send, [
      EKV.Replica.shard_name(ekv_name, shard_index),
      {:gc, now, tombstone_cutoff}
    ])
  end

  defp start_cluster(peers, ekv_name, opts) do
    cluster_size = Keyword.get(opts, :cluster_size, length(peers))

    peers
    |> Enum.with_index(1)
    |> Enum.each(fn {{_pid, node}, node_id} ->
      TestCluster.rpc!(node, File, :rm_rf!, [data_dir(node, ekv_name)])
      start_member(node, ekv_name, node_id, Keyword.put(opts, :cluster_size, cluster_size))
    end)

    Enum.each(peers, fn {_pid, node} ->
      assert :ok == TestCluster.rpc!(node, EKV, :await_quorum, [ekv_name, 5_000])
    end)
  end

  defp cleanup_data(peers, ekv_name) do
    for {_pid, node} <- peers do
      try do
        TestCluster.rpc!(node, File, :rm_rf!, [data_dir(node, ekv_name)])
      catch
        _, _ -> :ok
      end
    end
  end

  defp write_many(node, name, prefix, count) do
    Enum.each(1..count, fn i ->
      assert :ok == TestCluster.rpc!(node, EKV, :put, [name, "#{prefix}/#{i}", "v#{i}"])
    end)
  end

  defp await_all(node_list, fun, opts \\ []) do
    TestCluster.assert_eventually(
      fn -> Enum.all?(node_list, fun) end,
      opts
    )
  end

  defp collect_sync_messages(acc, timeout) do
    receive do
      {:trace, _pid, :send, {:ekv_sync, from_node, shard, entries, seq}, destination} ->
        collect_sync_messages(
          [
            {from_node, shard, Enum.map(entries, &elem(&1, 0)), length(entries), seq, destination}
            | acc
          ],
          timeout
        )

      {:trace, _pid, :send, _msg, _destination} ->
        collect_sync_messages(acc, timeout)
    after
      timeout -> Enum.reverse(acc)
    end
  end

  defp assert_no_sync_messages(timeout \\ 250) do
    assert collect_sync_messages([], timeout) == []
  end

  describe "anti-entropy healing" do
    test "connected stale member converges without reconnect or consistent read" do
      peers = TestCluster.start_peers(3)
      [{_, node_a}, {_, node_b}, {_, node_c}] = peers
      ekv_name = unique_name(:anti_entropy)
      on_exit(fn -> TestCluster.stop_peers(peers) end)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)

      start_cluster(peers, ekv_name, anti_entropy_interval: 200)

      assert {:ok, vsn1} =
               TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "heal/1", "v1", [if_vsn: nil]])

      await_all([node_a, node_b, node_c], fn node ->
        TestCluster.rpc!(node, EKV, :get, [ekv_name, "heal/1"]) == "v1"
      end)

      fresh_ts = elem(vsn1, 0) + 1_000

      for node <- [node_a, node_b] do
        assert :ok =
                 TestCluster.inject_committed_entry(
                   node,
                   ekv_name,
                   "heal/1",
                   "v2",
                   fresh_ts,
                   origin: node_a
                 )
      end

      assert TestCluster.rpc!(node_c, EKV, :get, [ekv_name, "heal/1"]) == "v1"

      TestCluster.assert_eventually(
        fn -> TestCluster.rpc!(node_c, EKV, :get, [ekv_name, "heal/1"]) == "v2" end,
        timeout: 5_000
      )
    end

    test "anti-entropy disabled does not heal a connected stale member" do
      peers = TestCluster.start_peers(3)
      [{_, node_a}, {_, node_b}, {_, node_c}] = peers
      ekv_name = unique_name(:anti_entropy_disabled)
      on_exit(fn -> TestCluster.stop_peers(peers) end)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)

      start_cluster(peers, ekv_name, anti_entropy_interval: false)

      assert {:ok, vsn1} =
               TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "disabled/1", "v1", [if_vsn: nil]])

      await_all([node_a, node_b, node_c], fn node ->
        TestCluster.rpc!(node, EKV, :get, [ekv_name, "disabled/1"]) == "v1"
      end)

      fresh_ts = elem(vsn1, 0) + 1_000

      for node <- [node_a, node_b] do
        assert :ok =
                 TestCluster.inject_committed_entry(
                   node,
                   ekv_name,
                   "disabled/1",
                   "v2",
                   fresh_ts,
                   origin: node_a
                 )
      end

      Process.sleep(1_000)
      assert TestCluster.rpc!(node_c, EKV, :get, [ekv_name, "disabled/1"]) == "v1"

      assert :ok = TestCluster.trigger_anti_entropy(node_a, ekv_name)

      TestCluster.assert_eventually(
        fn -> TestCluster.rpc!(node_c, EKV, :get, [ekv_name, "disabled/1"]) == "v2" end,
        timeout: 5_000
      )
    end

    test "multi-shard connected stale member converges on all affected shards" do
      peers = TestCluster.start_peers(3)
      [{_, node_a}, {_, node_b}, {_, node_c}] = peers
      ekv_name = unique_name(:anti_entropy_multishard)
      on_exit(fn -> TestCluster.stop_peers(peers) end)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)

      start_cluster(peers, ekv_name, anti_entropy_interval: 200, shards: 4)

      key0 = key_for_shard("multi", 0, 4)
      key1 = key_for_shard("multi", 1, 4)

      assert {:ok, vsn0} =
               TestCluster.rpc!(node_a, EKV, :put, [ekv_name, key0, "v1-s0", [if_vsn: nil]])

      assert {:ok, vsn1} =
               TestCluster.rpc!(node_a, EKV, :put, [ekv_name, key1, "v1-s1", [if_vsn: nil]])

      await_all([node_a, node_b, node_c], fn node ->
        TestCluster.rpc!(node, EKV, :get, [ekv_name, key0]) == "v1-s0" and
          TestCluster.rpc!(node, EKV, :get, [ekv_name, key1]) == "v1-s1"
      end)

      fresh_ts0 = elem(vsn0, 0) + 1_000
      fresh_ts1 = elem(vsn1, 0) + 2_000

      for node <- [node_a, node_b] do
        assert :ok =
                 TestCluster.inject_committed_entry(node, ekv_name, key0, "v2-s0", fresh_ts0,
                   origin: node_a
                 )

        assert :ok =
                 TestCluster.inject_committed_entry(node, ekv_name, key1, "v2-s1", fresh_ts1,
                   origin: node_a
                 )
      end

      assert TestCluster.rpc!(node_c, EKV, :get, [ekv_name, key0]) == "v1-s0"
      assert TestCluster.rpc!(node_c, EKV, :get, [ekv_name, key1]) == "v1-s1"

      TestCluster.assert_eventually(
        fn ->
          TestCluster.rpc!(node_c, EKV, :get, [ekv_name, key0]) == "v2-s0" and
            TestCluster.rpc!(node_c, EKV, :get, [ekv_name, key1]) == "v2-s1"
        end,
        timeout: 5_000
      )
    end
  end

  describe "anti-entropy HWM safety" do
    test "uses the remote advertised cursor rather than local inbound HWM" do
      peers = TestCluster.start_peers(2)
      [{_, node_a}, {_, node_b}] = peers
      ekv_name = unique_name(:anti_entropy_direction)
      on_exit(fn -> TestCluster.stop_peers(peers) end)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)

      start_cluster(peers, ekv_name, anti_entropy_interval: false)

      assert :ok == TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "zzz/1", "v1"])
      assert :ok == TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "yyy/1", "v2"])
      assert :ok == TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "xxx/1", "v3"])

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "zzz/1"]) == "v1" and
          TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "yyy/1"]) == "v2" and
          TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "xxx/1"]) == "v3"
      end)

      a_max = TestCluster.max_seq(node_a, ekv_name)
      impossible_hwm = a_max + 100
      assert :ok = TestCluster.set_member_hwm(node_a, ekv_name, node_b, impossible_hwm)
      assert :ok = TestCluster.set_cached_remote_hwm(node_a, ekv_name, node_b, a_max)

      assert :ok = TestCluster.trace_shard_sends(node_a, ekv_name, self())
      assert :ok = TestCluster.trigger_anti_entropy(node_a, ekv_name)
      assert_no_sync_messages(400)
      assert :ok = TestCluster.untrace_shard_sends(node_a, ekv_name)
    end

    test "full sync lowers impossible HWM and refreshes sender cache so the next tick is quiet" do
      peers = TestCluster.start_peers(2)
      [{_, node_a}, {_, node_b}] = peers
      ekv_name = unique_name(:anti_entropy_hwm_reset)
      on_exit(fn -> TestCluster.stop_peers(peers) end)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)

      start_cluster(peers, ekv_name, anti_entropy_interval: false)

      write_many(node_a, ekv_name, "from_a", 4)

      TestCluster.assert_eventually(fn ->
        TestCluster.keys_count(node_b, ekv_name, "from_a/") == 4
      end)

      a_max = TestCluster.max_seq(node_a, ekv_name)
      impossible_hwm = a_max + 10

      assert :ok = TestCluster.set_member_hwm(node_b, ekv_name, node_a, impossible_hwm)
      assert :ok = TestCluster.set_cached_remote_hwm(node_a, ekv_name, node_b, impossible_hwm)

      assert :ok = TestCluster.trace_shard_sends(node_a, ekv_name, self())
      assert :ok = TestCluster.trigger_anti_entropy(node_a, ekv_name)

      first_sync = collect_sync_messages([], 500)
      assert first_sync != []

      TestCluster.assert_eventually(fn ->
        TestCluster.member_hwm(node_b, ekv_name, node_a) == a_max
      end)

      TestCluster.assert_eventually(fn ->
        state = TestCluster.replica_state(node_a, ekv_name)
        Map.get(state.remote_member_hwms, node_b) == a_max
      end)

      assert :ok = TestCluster.trigger_anti_entropy(node_a, ekv_name)
      assert_no_sync_messages()
      assert :ok = TestCluster.untrace_shard_sends(node_a, ekv_name)
    end

    test "empty full sync still lowers impossible HWM and settles" do
      peers = TestCluster.start_peers(2)
      [{_, node_a}, {_, node_b}] = peers
      ekv_name = unique_name(:anti_entropy_empty_full_sync)
      on_exit(fn -> TestCluster.stop_peers(peers) end)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)

      start_cluster(peers, ekv_name, anti_entropy_interval: false)

      impossible_hwm = 42

      assert :ok = TestCluster.set_member_hwm(node_b, ekv_name, node_a, impossible_hwm)
      assert :ok = TestCluster.set_cached_remote_hwm(node_a, ekv_name, node_b, impossible_hwm)

      assert TestCluster.max_seq(node_a, ekv_name) == 0
      assert :ok = TestCluster.trace_shard_sends(node_a, ekv_name, self())
      assert :ok = TestCluster.trigger_anti_entropy(node_a, ekv_name)

      assert [
               {_from, 0, [], 0, 0, _dest}
             ] = collect_sync_messages([], 500)

      TestCluster.assert_eventually(fn ->
        TestCluster.member_hwm(node_b, ekv_name, node_a) == 0
      end)

      TestCluster.assert_eventually(fn ->
        state = TestCluster.replica_state(node_a, ekv_name)
        Map.get(state.remote_member_hwms, node_b) == 0
      end)

      assert :ok = TestCluster.trigger_anti_entropy(node_a, ekv_name)
      assert_no_sync_messages()
      assert :ok = TestCluster.untrace_shard_sends(node_a, ekv_name)
    end

    test "anti-entropy falls back to full sync after real oplog truncation and then settles" do
      peers = TestCluster.start_peers(2)
      [{_, node_a}, {_, node_b}] = peers
      ekv_name = unique_name(:anti_entropy_truncation)
      on_exit(fn -> TestCluster.stop_peers(peers) end)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)

      start_cluster(
        peers,
        ekv_name,
        anti_entropy_interval: false,
        sync_chunk_size: 2,
        gc_interval: 100,
        tombstone_ttl: 700
      )

      write_many(node_a, ekv_name, "trunc", 5)

      TestCluster.assert_eventually(fn ->
        TestCluster.keys_count(node_b, ekv_name, "trunc/") == 5
      end)

      a_max = TestCluster.max_seq(node_a, ekv_name)
      assert :ok = TestCluster.set_member_hwm(node_a, ekv_name, node_b, a_max)
      trigger_gc(node_a, ekv_name, 0, 700)

      TestCluster.assert_eventually(fn ->
        min_seq = TestCluster.min_seq(node_a, ekv_name)
        is_integer(min_seq) and min_seq > 0
      end)

      assert :ok = TestCluster.set_cached_remote_hwm(node_a, ekv_name, node_b, 0)
      assert :ok = TestCluster.trace_shard_sends(node_a, ekv_name, self())
      assert :ok = TestCluster.trigger_anti_entropy(node_a, ekv_name)

      sync_messages = collect_sync_messages([], 750)

      assert Enum.map(sync_messages, fn {_from, _shard, _keys, len, _seq, _dest} -> len end) == [
               2,
               2,
               1
             ]

      assert Enum.map(sync_messages, fn {_from, _shard, _keys, _len, seq, _dest} -> seq end) == [
               0,
               0,
               a_max
             ]

      TestCluster.assert_eventually(fn ->
        TestCluster.member_hwm(node_b, ekv_name, node_a) == a_max
      end)

      TestCluster.assert_eventually(fn ->
        state = TestCluster.replica_state(node_a, ekv_name)
        Map.get(state.remote_member_hwms, node_b) == a_max
      end)

      assert :ok = TestCluster.trigger_anti_entropy(node_a, ekv_name)
      assert_no_sync_messages(400)
      assert :ok = TestCluster.untrace_shard_sends(node_a, ekv_name)
    end

    test "restart with reset local history heals once and later anti-entropy stays quiet" do
      peers = TestCluster.start_peers(2)
      [{_, node_a}, {_, node_b}] = peers
      ekv_name = unique_name(:anti_entropy_restart)
      on_exit(fn -> TestCluster.stop_peers(peers) end)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)

      start_cluster(peers, ekv_name, anti_entropy_interval: false)

      write_many(node_b, ekv_name, "restart", 4)

      TestCluster.assert_eventually(fn ->
        TestCluster.keys_count(node_a, ekv_name, "restart/") == 4
      end)

      assert :ok = TestCluster.stop_ekv(node_a, ekv_name, 10_000)
      TestCluster.rpc!(node_a, File, :rm_rf!, [data_dir(node_a, ekv_name)])

      start_member(node_a, ekv_name, 1, cluster_size: 2)

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_a, EKV, :await_quorum, [ekv_name, 5_000]) == :ok
      end)

      TestCluster.assert_eventually(fn ->
        TestCluster.keys_count(node_a, ekv_name, "restart/") == 4
      end)

      assert :ok = TestCluster.trace_shard_sends(node_b, ekv_name, self())
      assert :ok = TestCluster.trigger_anti_entropy(node_b, ekv_name)
      assert_no_sync_messages(400)
      assert :ok = TestCluster.untrace_shard_sends(node_b, ekv_name)
    end
  end

  describe "anti-entropy suppression / gating" do
    test "skips quarantined members" do
      peers = TestCluster.start_peers(2)
      [{_, node_a}, {_, node_b}] = peers
      ekv_name = unique_name(:anti_entropy_quarantine)
      on_exit(fn -> TestCluster.stop_peers(peers) end)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)

      start_cluster(
        peers,
        ekv_name,
        anti_entropy_interval: false,
        tombstone_ttl: 700,
        gc_interval: 100
      )

      TestCluster.disconnect_nodes(node_a, node_b)
      Process.sleep(1_200)
      TestCluster.reconnect_nodes(node_a, node_b)

      TestCluster.assert_eventually(fn ->
        state = TestCluster.replica_state(node_a, ekv_name)
        MapSet.member?(state.quarantined_members, node_b)
      end)

      assert :ok = TestCluster.trace_shard_sends(node_a, ekv_name, self())
      assert :ok = TestCluster.trigger_anti_entropy(node_a, ekv_name)
      assert_no_sync_messages(400)
      assert :ok = TestCluster.untrace_shard_sends(node_a, ekv_name)
    end

    test "skips proxy-mode shards" do
      peers = TestCluster.start_peers(2)
      [{_, node_a}, {_, node_b}] = peers
      ekv_name = unique_name(:anti_entropy_proxy)
      on_exit(fn -> TestCluster.stop_peers(peers) end)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)

      start_cluster(peers, ekv_name, anti_entropy_interval: false)

      assert :ok = TestCluster.set_handoff_node(node_a, ekv_name, node_b)
      assert :ok = TestCluster.trace_shard_sends(node_a, ekv_name, self())
      assert :ok = TestCluster.trigger_anti_entropy(node_a, ekv_name)
      assert_no_sync_messages()
      assert :ok = TestCluster.untrace_shard_sends(node_a, ekv_name)
    end

    test "does not start duplicate syncs while one is already in flight" do
      peers = TestCluster.start_peers(2)
      [{_, node_a}, {_, node_b}] = peers
      ekv_name = unique_name(:anti_entropy_inflight)
      on_exit(fn -> TestCluster.stop_peers(peers) end)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)

      start_cluster(peers, ekv_name, anti_entropy_interval: false, sync_chunk_size: 2)

      write_many(node_a, ekv_name, "delta", 5)

      TestCluster.assert_eventually(fn ->
        TestCluster.keys_count(node_b, ekv_name, "delta/") == 5
      end)

      a_max = TestCluster.max_seq(node_a, ekv_name)
      assert :ok = TestCluster.set_cached_remote_hwm(node_a, ekv_name, node_b, 0)

      assert :ok = TestCluster.trace_shard_sends(node_a, ekv_name, self())
      assert :ok = TestCluster.trigger_anti_entropy(node_a, ekv_name)
      assert :ok = TestCluster.trigger_anti_entropy(node_a, ekv_name)

      details = collect_sync_messages([], 500)

      assert Enum.map(details, fn {_from, _shard, _keys, len, _seq, _dest} -> len end) == [
               2,
               2,
               1
             ]

      assert Enum.map(details, fn {_from, _shard, _keys, _len, seq, _dest} -> seq end) == [
               0,
               0,
               a_max
             ]

      assert :ok = TestCluster.untrace_shard_sends(node_a, ekv_name)
    end
  end
end
