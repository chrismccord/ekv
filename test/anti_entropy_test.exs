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

    member_progress_retention_ttl =
      Keyword.get(opts, :member_progress_retention_ttl, tombstone_ttl)

    cluster_size = Keyword.fetch!(opts, :cluster_size)

    TestCluster.start_ekv(
      node,
      name: ekv_name,
      data_dir: data_dir(node, ekv_name),
      shards: shards,
      log: false,
      gc_interval: gc_interval,
      tombstone_ttl: tombstone_ttl,
      member_progress_retention_ttl: member_progress_retention_ttl,
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
      {:trace, _pid, :send, {:ekv_sync, from_node, shard, _mode, entries, progress}, destination} ->
        collect_sync_messages(
          [
            {from_node, shard, Enum.map(entries, &elem(&1, 0)), length(entries),
             progress_seq(from_node, progress), destination}
            | acc
          ],
          timeout
        )

      {:trace, _pid, :send, {:ekv, 1, :sync, {from_node, shard, _mode, entries, progress}, _meta},
       destination} ->
        collect_sync_messages(
          [
            {from_node, shard, Enum.map(entries, &elem(&1, 0)), length(entries),
             progress_seq(from_node, progress), destination}
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

  defp collect_sync_request_messages(acc, timeout) do
    receive do
      {:trace, _pid, :send, {:ekv_sync_request, _from_pid, shard, request}, destination} ->
        collect_sync_request_messages([{shard, request, destination} | acc], timeout)

      {:trace, _pid, :send, {:ekv, 1, :sync_request, {_from_pid, shard, request}, _meta},
       destination} ->
        collect_sync_request_messages([{shard, request, destination} | acc], timeout)

      {:trace, _pid, :send, _msg, _destination} ->
        collect_sync_request_messages(acc, timeout)
    after
      timeout -> Enum.reverse(acc)
    end
  end

  defp collect_trace_messages(acc, timeout) do
    receive do
      {:trace, _pid, :send, {:ekv_sync_request, _from_pid, shard, request}, destination} ->
        collect_trace_messages([{:request, shard, request, destination} | acc], timeout)

      {:trace, _pid, :send, {:ekv, 1, :sync_request, {_from_pid, shard, request}, _meta},
       destination} ->
        collect_trace_messages([{:request, shard, request, destination} | acc], timeout)

      {:trace, _pid, :send, {:ekv_sync, from_node, shard, mode, entries, progress}, destination} ->
        collect_trace_messages(
          [
            {:sync, from_node, shard, mode, Enum.map(entries, &elem(&1, 0)), progress,
             destination}
            | acc
          ],
          timeout
        )

      {:trace, _pid, :send, {:ekv, 1, :sync, {from_node, shard, mode, entries, progress}, _meta},
       destination} ->
        collect_trace_messages(
          [
            {:sync, from_node, shard, mode, Enum.map(entries, &elem(&1, 0)), progress,
             destination}
            | acc
          ],
          timeout
        )

      {:trace, _pid, :send, _msg, _destination} ->
        collect_trace_messages(acc, timeout)
    after
      timeout -> Enum.reverse(acc)
    end
  end

  defp assert_no_sync_messages(timeout \\ 250) do
    assert collect_sync_messages([], timeout) == []
  end

  defp progress_seq(from_node, progress) when is_map(progress),
    do: Map.get(progress, from_node, 0)

  defp progress_seq(_from_node, _progress), do: 0

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

      fresh_ts = max(elem(vsn1, 0) + 1_000, System.system_time(:nanosecond) + 1_000_000)

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

      fresh_ts = max(elem(vsn1, 0) + 1_000, System.system_time(:nanosecond) + 1_000_000)

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

    test "live LWW replication advances remote progress so anti-entropy stays quiet" do
      peers = TestCluster.start_peers(2)
      [{_, node_a}, {_, node_b}] = peers
      ekv_name = unique_name(:anti_entropy_live_lww)
      on_exit(fn -> TestCluster.stop_peers(peers) end)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)

      start_cluster(peers, ekv_name, anti_entropy_interval: false)

      write_many(node_a, ekv_name, "live_lww", 5)

      TestCluster.assert_eventually(fn ->
        TestCluster.keys_count(node_b, ekv_name, "live_lww/") == 5
      end)

      assert :ok = TestCluster.trace_shard_sends(node_a, ekv_name, self())
      assert :ok = TestCluster.trigger_anti_entropy(node_a, ekv_name)
      assert_no_sync_messages(400)
      assert :ok = TestCluster.untrace_shard_sends(node_a, ekv_name)
    end

    test "non-origin member settles remote-origin live entries once and then stays quiet" do
      peers = TestCluster.start_peers(2)
      [{_, node_a}, {_, node_b}] = peers
      ekv_name = unique_name(:anti_entropy_non_origin_lww)
      on_exit(fn -> TestCluster.stop_peers(peers) end)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)

      start_cluster(peers, ekv_name, anti_entropy_interval: false)

      write_many(node_a, ekv_name, "remote_origin", 5)

      TestCluster.assert_eventually(fn ->
        TestCluster.keys_count(node_b, ekv_name, "remote_origin/") == 5
      end)

      assert Enum.map(TestCluster.oplog_since(node_b, ekv_name, 0, 10), &elem(&1, 4)) ==
               List.duplicate(node_a, 5)

      assert Map.has_key?(TestCluster.replica_state(node_b, ekv_name).remote_shards, node_a)

      assert :ok = TestCluster.trace_shard_sends(node_b, ekv_name, self())
      assert :ok = TestCluster.trigger_anti_entropy(node_b, ekv_name)
      assert_no_sync_messages(500)

      assert :ok = TestCluster.trigger_anti_entropy(node_b, ekv_name)
      assert_no_sync_messages(400)
      assert :ok = TestCluster.untrace_shard_sends(node_b, ekv_name)
    end

    test "summary exchange refreshes CAS progress and anti-entropy stays quiet" do
      peers = TestCluster.start_peers(2)
      [{_, node_a}, {_, node_b}] = peers
      ekv_name = unique_name(:anti_entropy_live_cas)
      on_exit(fn -> TestCluster.stop_peers(peers) end)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)

      start_cluster(peers, ekv_name, anti_entropy_interval: false)

      assert {:ok, _vsn} =
               TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "live_cas/1", "v1", [if_vsn: nil]])

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "live_cas/1"]) == "v1"
      end)

      assert :ok = TestCluster.trace_shard_sends(node_a, ekv_name, self())
      assert :ok = TestCluster.trigger_anti_entropy(node_a, ekv_name)

      TestCluster.assert_eventually(fn ->
        state = TestCluster.replica_state(node_a, ekv_name)
        get_in(state.remote_member_progress, [node_b, node_a]) == state.local_origin_seq
      end)

      assert_no_sync_messages(400)
      assert :ok = TestCluster.untrace_shard_sends(node_a, ekv_name)
    end

    test "live LWW gap triggers immediate delta request and repair" do
      peers = TestCluster.start_peers(2)
      [{_, node_a}, {_, node_b}] = peers
      ekv_name = unique_name(:anti_entropy_live_gap_lww)
      key1 = "gap_lww/1"
      key2 = "gap_lww/2"
      key3 = "gap_lww/3"
      on_exit(fn -> TestCluster.stop_peers(peers) end)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)

      start_cluster(peers, ekv_name, anti_entropy_interval: false)

      assert :ok == TestCluster.rpc!(node_a, EKV, :put, [ekv_name, key1, "v1"])

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, EKV, :get, [ekv_name, key1]) == "v1"
      end)

      base_ts = System.system_time(:nanosecond) + 1_000_000

      assert :ok =
               TestCluster.inject_committed_entry(node_a, ekv_name, key2, "v2", base_ts,
                 origin: node_a,
                 origin_seq: 2
               )

      assert :ok =
               TestCluster.inject_committed_entry(node_a, ekv_name, key3, "v3", base_ts + 1_000,
                 origin: node_a,
                 origin_seq: 3
               )

      assert TestCluster.local_progress(node_b, ekv_name, node_a) == 1
      assert :ok = TestCluster.trace_shard_sends(node_b, ekv_name, self())

      shard_name = EKV.Replica.shard_name(ekv_name, 0)

      value_binary = :erlang.term_to_binary("v3")

      TestCluster.rpc!(node_b, :erlang, :send, [
        shard_name,
        {:ekv_put, key3, value_binary, base_ts + 1_000, node_a, 3, nil}
      ])

      requests = collect_sync_request_messages([], 500)

      assert Enum.any?(requests, fn {shard, request, _destination} ->
               shard == 0 and request == {:delta, node_a, 1}
             end)

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, EKV, :get, [ekv_name, key2]) == "v2" and
          TestCluster.rpc!(node_b, EKV, :get, [ekv_name, key3]) == "v3" and
          TestCluster.local_progress(node_b, ekv_name, node_a) == 3
      end)

      assert :ok = TestCluster.untrace_shard_sends(node_b, ekv_name)
    end

    test "live CAS gap triggers immediate delta request and repair" do
      peers = TestCluster.start_peers(2)
      [{_, node_a}, {_, node_b}] = peers
      ekv_name = unique_name(:anti_entropy_live_gap_cas)
      key = "gap_cas/1"
      on_exit(fn -> TestCluster.stop_peers(peers) end)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)

      start_cluster(peers, ekv_name, anti_entropy_interval: false)

      assert {:ok, vsn1} =
               TestCluster.rpc!(node_a, EKV, :put, [ekv_name, key, "v1", [if_vsn: nil]])

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, EKV, :get, [ekv_name, key]) == "v1"
      end)

      ts2 = elem(vsn1, 0) + 1_000
      ts3 = ts2 + 1_000

      assert :ok =
               TestCluster.inject_committed_entry(node_a, ekv_name, key, "v2", ts2,
                 origin: node_a,
                 origin_seq: 2
               )

      assert :ok =
               TestCluster.inject_committed_entry(node_a, ekv_name, key, "v3", ts3,
                 origin: node_a,
                 origin_seq: 3
               )

      assert TestCluster.local_progress(node_b, ekv_name, node_a) == 1
      assert :ok = TestCluster.trace_shard_sends(node_b, ekv_name, self())

      ballot_c = elem(vsn1, 0) + 10_000
      ballot_n = "gap-cas"

      assert :ok =
               TestCluster.inject_paxos_accept(node_b, ekv_name, key, "v3", ballot_c, ballot_n,
                 timestamp: ts3,
                 origin: Atom.to_string(node_a)
               )

      entry_tuple = {key, :erlang.term_to_binary("v3"), ts3, Atom.to_string(node_a), nil, nil}

      TestCluster.rpc!(node_b, :erlang, :send, [
        EKV.Replica.shard_name(ekv_name, 0),
        {:ekv_cas_committed, key, ballot_c, ballot_n, entry_tuple, 0, node_a, 3}
      ])

      requests = collect_sync_request_messages([], 500)

      assert Enum.any?(requests, fn {shard, request, _destination} ->
               shard == 0 and request == {:delta, node_a, 1}
             end)

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, EKV, :get, [ekv_name, key]) == "v3" and
          TestCluster.local_progress(node_b, ekv_name, node_a) == 3
      end)

      assert :ok = TestCluster.untrace_shard_sends(node_b, ekv_name)
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

      base_ts = System.system_time(:nanosecond) + 1_000_000
      fresh_ts0 = max(elem(vsn0, 0) + 1_000, base_ts)
      fresh_ts1 = max(elem(vsn1, 0) + 2_000, base_ts + 1_000)

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

    test "non-origin member relays dead-origin entries during delta repair" do
      peers = TestCluster.start_peers(3)
      [{_, node_a}, {_, node_b}, {_, node_c}] = peers
      ekv_name = unique_name(:anti_entropy_dead_origin_relay)
      key1 = "dead_origin/1"
      key2 = "dead_origin/2"
      on_exit(fn -> TestCluster.stop_peers(peers) end)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)

      start_cluster(peers, ekv_name, anti_entropy_interval: false)

      assert :ok == TestCluster.rpc!(node_a, EKV, :put, [ekv_name, key1, "v1"])

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, EKV, :get, [ekv_name, key1]) == "v1" and
          TestCluster.rpc!(node_c, EKV, :get, [ekv_name, key1]) == "v1"
      end)

      TestCluster.disconnect_nodes(node_a, node_c)
      assert :ok == TestCluster.rpc!(node_a, EKV, :put, [ekv_name, key2, "v2"])

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, EKV, :get, [ekv_name, key2]) == "v2"
      end)

      assert TestCluster.rpc!(node_c, EKV, :get, [ekv_name, key2]) == nil

      assert :ok = TestCluster.stop_ekv(node_a, ekv_name, 10_000)

      TestCluster.assert_eventually(fn ->
        state = TestCluster.replica_state(node_b, ekv_name)
        not Map.has_key?(state.remote_shards, node_a)
      end)

      assert :ok = TestCluster.trace_shard_sends(node_c, ekv_name, self())
      assert :ok = TestCluster.trigger_anti_entropy(node_b, ekv_name)

      assert Enum.any?(collect_sync_request_messages([], 2_000), fn {shard, request, _destination} ->
               shard == 0 and request == {:delta, node_a, 1}
             end)

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_c, EKV, :get, [ekv_name, key2]) == "v2"
      end)

      assert :ok = TestCluster.untrace_shard_sends(node_c, ekv_name)
    end

    test "missing third-origin handshake relays delta instead of requesting full" do
      peers = TestCluster.start_peers(3)
      [{_, node_a}, {_, node_b}, {_, node_c}] = peers
      ekv_name = unique_name(:anti_entropy_no_premature_full)
      on_exit(fn -> TestCluster.stop_peers(peers) end)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)

      start_cluster(peers, ekv_name, anti_entropy_interval: false)

      write_many(node_a, ekv_name, "startup_gap", 3)

      TestCluster.assert_eventually(fn ->
        TestCluster.keys_count(node_c, ekv_name, "startup_gap/") == 3
      end)

      assert :ok = TestCluster.force_local_progress(node_b, ekv_name, node_a, 0)
      assert :ok = TestCluster.drop_remote_shard(node_b, ekv_name, node_a)
      assert :ok = TestCluster.trace_shard_sends(node_b, ekv_name, self())
      assert :ok = TestCluster.trigger_anti_entropy(node_b, ekv_name)

      requests = collect_sync_request_messages([], 1_000)

      assert Enum.any?(requests, fn {shard, request, _destination} ->
               shard == 0 and request == {:delta, node_a, 0}
             end)

      refute Enum.any?(requests, fn {shard, request, _destination} ->
               shard == 0 and request == :full
             end)

      assert :ok = TestCluster.untrace_shard_sends(node_b, ekv_name)
    end

    test "recently unavailable third origin requests relayed delta immediately" do
      peers = TestCluster.start_peers(3)
      [{_, node_a}, {_, node_b}, {_, node_c}] = peers
      ekv_name = unique_name(:anti_entropy_recent_unavailable_origin)
      key1 = "recent_unavailable/1"
      key2 = "recent_unavailable/2"
      on_exit(fn -> TestCluster.stop_peers(peers) end)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)

      start_cluster(peers, ekv_name, anti_entropy_interval: false)

      assert :ok == TestCluster.rpc!(node_a, EKV, :put, [ekv_name, key1, "v1"])

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, EKV, :get, [ekv_name, key1]) == "v1" and
          TestCluster.rpc!(node_c, EKV, :get, [ekv_name, key1]) == "v1"
      end)

      TestCluster.disconnect_nodes(node_a, node_c)
      assert :ok == TestCluster.rpc!(node_a, EKV, :put, [ekv_name, key2, "v2"])

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, EKV, :get, [ekv_name, key2]) == "v2"
      end)

      assert TestCluster.rpc!(node_c, EKV, :get, [ekv_name, key2]) == nil

      assert :ok = TestCluster.stop_ekv(node_a, ekv_name, 10_000)

      TestCluster.assert_eventually(fn ->
        state = TestCluster.replica_state(node_b, ekv_name)
        not Map.has_key?(state.remote_shards, node_a)
      end)

      assert :ok = TestCluster.trace_shard_sends(node_c, ekv_name, self())
      assert :ok = TestCluster.trigger_anti_entropy(node_b, ekv_name)

      requests = collect_sync_request_messages([], 2_000)

      assert Enum.any?(requests, fn {shard, request, _destination} ->
               shard == 0 and request == {:delta, node_a, 1}
             end)

      refute Enum.any?(requests, fn {shard, request, _destination} ->
               shard == 0 and request == :full
             end)

      assert :ok = TestCluster.untrace_shard_sends(node_c, ekv_name)
    end

    test "relayed third-origin delta falls back to full when retained replay is gone" do
      peers = TestCluster.start_peers(3)
      [{_, node_a}, {_, node_b}, {_, node_c}] = peers
      ekv_name = unique_name(:anti_entropy_third_origin_retained_gap)
      on_exit(fn -> TestCluster.stop_peers(peers) end)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)

      start_cluster(
        peers,
        ekv_name,
        anti_entropy_interval: false,
        sync_chunk_size: 2
      )

      write_many(node_a, ekv_name, "relay_trunc", 5)

      TestCluster.assert_eventually(fn ->
        TestCluster.keys_count(node_b, ekv_name, "relay_trunc/") == 5 and
          TestCluster.keys_count(node_c, ekv_name, "relay_trunc/") == 5
      end)

      assert :ok = TestCluster.prune_origin_replay(node_b, ekv_name, node_a, 3)

      assert :ok = TestCluster.force_local_progress(node_c, ekv_name, node_a, 0)
      assert :ok = TestCluster.stop_ekv(node_a, ekv_name, 10_000)

      TestCluster.assert_eventually(fn ->
        state = TestCluster.replica_state(node_b, ekv_name)
        not Map.has_key?(state.remote_shards, node_a)
      end)

      assert :ok = TestCluster.trace_shard_sends(node_c, ekv_name, self())
      assert :ok = TestCluster.trace_shard_sends(node_b, ekv_name, self())
      assert :ok = TestCluster.trigger_anti_entropy(node_b, ekv_name)

      trace_messages = collect_trace_messages([], 2_000)

      assert Enum.any?(trace_messages, fn
               {:request, shard, request, _destination} ->
                 shard == 0 and request == {:delta, node_a, 0}

               _ ->
                 false
             end)

      assert Enum.any?(trace_messages, fn
               {:sync, from_node, shard, mode, _keys, _progress, _destination} ->
                 from_node == node_b and shard == 0 and mode == :full

               _ ->
                 false
             end)

      assert Enum.any?(trace_messages, fn
               {:sync, from_node, shard, mode, _keys, progress, _destination} ->
                 from_node == node_b and shard == 0 and mode == :full and is_map(progress)

               _ ->
                 false
             end)

      TestCluster.assert_eventually(fn ->
        TestCluster.keys_count(node_c, ekv_name, "relay_trunc/") == 5
      end)

      assert :ok = TestCluster.untrace_shard_sends(node_b, ekv_name)
      assert :ok = TestCluster.untrace_shard_sends(node_c, ekv_name)
    end

    test "dead-origin bootstrap prefers relayed delta over full sync" do
      peers = TestCluster.start_peers(4)
      [{_, node_a}, {_, node_b}, {_, node_c}, {_, node_d}] = peers
      ekv_name = unique_name(:anti_entropy_single_full_source)
      on_exit(fn -> TestCluster.stop_peers(peers) end)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)

      start_cluster(peers, ekv_name, anti_entropy_interval: false)

      write_many(node_c, ekv_name, "dead_source", 3)

      TestCluster.assert_eventually(fn ->
        Enum.all?([node_a, node_b, node_d], fn node ->
          TestCluster.keys_count(node, ekv_name, "dead_source/") == 3
        end)
      end)

      assert :ok = TestCluster.force_local_progress(node_b, ekv_name, node_c, 0)
      assert :ok = TestCluster.stop_ekv(node_c, ekv_name, 10_000)

      TestCluster.assert_eventually(fn ->
        state = TestCluster.replica_state(node_b, ekv_name)
        not Map.has_key?(state.remote_shards, node_c)
      end)

      assert :ok = TestCluster.trace_shard_sends(node_b, ekv_name, self())
      assert :ok = TestCluster.trigger_anti_entropy(node_b, ekv_name)

      requests = collect_sync_request_messages([], 1_000)

      assert Enum.any?(requests, fn {shard, request, _destination} ->
               shard == 0 and request == {:delta, node_c, 0}
             end)

      refute Enum.any?(requests, fn {shard, request, _destination} ->
               shard == 0 and request == :full
             end)

      assert :ok = TestCluster.untrace_shard_sends(node_b, ekv_name)
    end

    test "same-shard gaps from multiple origins trigger overlapping repairs" do
      peers = TestCluster.start_peers(3)
      [{_, node_a}, {_, node_b}, {_, node_c}] = peers
      ekv_name = unique_name(:anti_entropy_multi_origin_gap)
      a1 = "multi_origin/a1"
      a2 = "multi_origin/a2"
      a3 = "multi_origin/a3"
      b1 = "multi_origin/b1"
      b2 = "multi_origin/b2"
      b3 = "multi_origin/b3"
      on_exit(fn -> TestCluster.stop_peers(peers) end)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)

      start_cluster(peers, ekv_name, anti_entropy_interval: false)

      assert :ok == TestCluster.rpc!(node_a, EKV, :put, [ekv_name, a1, "a-v1"])
      assert :ok == TestCluster.rpc!(node_b, EKV, :put, [ekv_name, b1, "b-v1"])

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_c, EKV, :get, [ekv_name, a1]) == "a-v1" and
          TestCluster.rpc!(node_c, EKV, :get, [ekv_name, b1]) == "b-v1"
      end)

      base_ts = System.system_time(:nanosecond) + 1_000_000

      assert :ok =
               TestCluster.inject_committed_entry(node_a, ekv_name, a2, "a-v2", base_ts,
                 origin: node_a,
                 origin_seq: 2
               )

      assert :ok =
               TestCluster.inject_committed_entry(node_a, ekv_name, a3, "a-v3", base_ts + 1_000,
                 origin: node_a,
                 origin_seq: 3
               )

      assert :ok =
               TestCluster.inject_committed_entry(node_b, ekv_name, b2, "b-v2", base_ts + 2_000,
                 origin: node_b,
                 origin_seq: 2
               )

      assert :ok =
               TestCluster.inject_committed_entry(node_b, ekv_name, b3, "b-v3", base_ts + 3_000,
                 origin: node_b,
                 origin_seq: 3
               )

      assert TestCluster.local_progress(node_c, ekv_name, node_a) == 1
      assert TestCluster.local_progress(node_c, ekv_name, node_b) == 1
      assert :ok = TestCluster.trace_shard_sends(node_c, ekv_name, self())

      shard_name = EKV.Replica.shard_name(ekv_name, 0)

      TestCluster.rpc!(node_c, :erlang, :send, [
        shard_name,
        {:ekv_put, a3, :erlang.term_to_binary("a-v3"), base_ts + 1_000, node_a, 3, nil}
      ])

      TestCluster.rpc!(node_c, :erlang, :send, [
        shard_name,
        {:ekv_put, b3, :erlang.term_to_binary("b-v3"), base_ts + 3_000, node_b, 3, nil}
      ])

      requests = collect_sync_request_messages([], 500)

      assert Enum.any?(requests, fn {shard, request, _destination} ->
               shard == 0 and request == {:delta, node_a, 1}
             end)

      assert Enum.any?(requests, fn {shard, request, _destination} ->
               shard == 0 and request == {:delta, node_b, 1}
             end)

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_c, EKV, :get, [ekv_name, a2]) == "a-v2" and
          TestCluster.rpc!(node_c, EKV, :get, [ekv_name, a3]) == "a-v3" and
          TestCluster.rpc!(node_c, EKV, :get, [ekv_name, b2]) == "b-v2" and
          TestCluster.rpc!(node_c, EKV, :get, [ekv_name, b3]) == "b-v3" and
          TestCluster.local_progress(node_c, ekv_name, node_a) == 3 and
          TestCluster.local_progress(node_c, ekv_name, node_b) == 3
      end)

      assert :ok = TestCluster.untrace_shard_sends(node_c, ekv_name)
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
      assert :ok = TestCluster.set_peer_progress(node_a, ekv_name, node_b, node_a, impossible_hwm)
      assert :ok = TestCluster.set_cached_remote_progress(node_a, ekv_name, node_b, node_a, a_max)

      assert :ok = TestCluster.trace_shard_sends(node_a, ekv_name, self())
      assert :ok = TestCluster.trigger_anti_entropy(node_a, ekv_name)
      assert_no_sync_messages(400)
      assert :ok = TestCluster.untrace_shard_sends(node_a, ekv_name)
    end

    test "authoritative summary lowers impossible local progress and later refreshes sender cache" do
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

      assert :ok = TestCluster.set_local_progress(node_b, ekv_name, node_a, impossible_hwm)

      assert :ok =
               TestCluster.set_cached_remote_progress(
                 node_a,
                 ekv_name,
                 node_b,
                 node_a,
                 impossible_hwm
               )

      assert :ok = TestCluster.trace_shard_sends(node_a, ekv_name, self())
      assert :ok = TestCluster.trigger_anti_entropy(node_b, ekv_name)
      assert_no_sync_messages(400)

      TestCluster.assert_eventually(fn ->
        TestCluster.local_progress(node_b, ekv_name, node_a) == a_max
      end)

      assert :ok = TestCluster.trigger_anti_entropy(node_b, ekv_name)
      assert_no_sync_messages(400)

      TestCluster.assert_eventually(fn ->
        state = TestCluster.replica_state(node_a, ekv_name)
        get_in(state.remote_member_progress, [node_b, node_a]) == a_max
      end)

      assert :ok = TestCluster.untrace_shard_sends(node_a, ekv_name)
    end

    test "empty summary exchange still lowers impossible local progress and settles" do
      peers = TestCluster.start_peers(2)
      [{_, node_a}, {_, node_b}] = peers
      ekv_name = unique_name(:anti_entropy_empty_full_sync)
      on_exit(fn -> TestCluster.stop_peers(peers) end)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)

      start_cluster(peers, ekv_name, anti_entropy_interval: false)

      impossible_hwm = 42

      assert :ok = TestCluster.set_local_progress(node_b, ekv_name, node_a, impossible_hwm)

      assert :ok =
               TestCluster.set_cached_remote_progress(
                 node_a,
                 ekv_name,
                 node_b,
                 node_a,
                 impossible_hwm
               )

      assert TestCluster.max_seq(node_a, ekv_name) == 0
      assert :ok = TestCluster.trace_shard_sends(node_a, ekv_name, self())
      assert :ok = TestCluster.trigger_anti_entropy(node_b, ekv_name)
      assert_no_sync_messages(400)

      TestCluster.assert_eventually(fn ->
        TestCluster.local_progress(node_b, ekv_name, node_a) == 0
      end)

      assert :ok = TestCluster.trigger_anti_entropy(node_b, ekv_name)
      assert_no_sync_messages(400)

      TestCluster.assert_eventually(fn ->
        state = TestCluster.replica_state(node_a, ekv_name)
        get_in(state.remote_member_progress, [node_b, node_a]) == 0
      end)

      assert :ok = TestCluster.untrace_shard_sends(node_a, ekv_name)
    end

    test "stale low sender cache does not replay when the receiver is already caught up" do
      peers = TestCluster.start_peers(2)
      [{_, node_a}, {_, node_b}] = peers
      ekv_name = unique_name(:anti_entropy_stale_low_cache)
      on_exit(fn -> TestCluster.stop_peers(peers) end)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)

      start_cluster(peers, ekv_name, anti_entropy_interval: false)

      write_many(node_a, ekv_name, "stale_low", 3)

      TestCluster.assert_eventually(fn ->
        TestCluster.keys_count(node_b, ekv_name, "stale_low/") == 3
      end)

      a_max = TestCluster.max_seq(node_a, ekv_name)
      assert :ok = TestCluster.set_peer_progress(node_a, ekv_name, node_b, node_a, 0)
      assert :ok = TestCluster.set_cached_remote_progress(node_a, ekv_name, node_b, node_a, 0)

      assert :ok = TestCluster.trace_shard_sends(node_a, ekv_name, self())
      assert :ok = TestCluster.trigger_anti_entropy(node_a, ekv_name)
      assert_no_sync_messages(400)

      TestCluster.assert_eventually(fn ->
        state = TestCluster.replica_state(node_a, ekv_name)
        get_in(state.remote_member_progress, [node_b, node_a]) == a_max
      end)

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
      trigger_gc(node_a, ekv_name, 0, 700)

      TestCluster.assert_eventually(fn ->
        min_seq = TestCluster.min_seq(node_a, ekv_name)
        is_integer(min_seq) and min_seq > 0
      end)

      assert :ok = TestCluster.force_local_progress(node_b, ekv_name, node_a, 0)
      assert :ok = TestCluster.trace_shard_sends(node_a, ekv_name, self())
      assert :ok = TestCluster.trigger_anti_entropy(node_b, ekv_name)

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
        TestCluster.local_progress(node_b, ekv_name, node_a) == a_max
      end)

      TestCluster.assert_eventually(fn ->
        state = TestCluster.replica_state(node_a, ekv_name)
        get_in(state.remote_member_progress, [node_b, node_a]) == a_max
      end)

      assert :ok = TestCluster.trigger_anti_entropy(node_b, ekv_name)
      assert_no_sync_messages(400)
      assert :ok = TestCluster.untrace_shard_sends(node_a, ekv_name)
    end

    test "gc retains recently disconnected member progress so reconnect heals by delta" do
      peers = TestCluster.start_peers(2)
      [{_, node_a}, {_, node_b}] = peers
      ekv_name = unique_name(:anti_entropy_recent_disconnect_delta)
      on_exit(fn -> TestCluster.stop_peers(peers) end)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)

      start_cluster(
        peers,
        ekv_name,
        anti_entropy_interval: false,
        sync_chunk_size: 2,
        gc_interval: 100,
        tombstone_ttl: 10_000,
        member_progress_retention_ttl: 10_000
      )

      write_many(node_a, ekv_name, "retained_window", 5)

      TestCluster.assert_eventually(fn ->
        TestCluster.keys_count(node_b, ekv_name, "retained_window/") == 5
      end)

      assert :ok = TestCluster.set_peer_progress(node_a, ekv_name, node_b, node_a, 5)
      assert :ok = TestCluster.set_cached_remote_progress(node_a, ekv_name, node_b, node_a, 5)

      TestCluster.monitor_nodes_on(node_a, self())
      TestCluster.monitor_nodes_on(node_b, self())
      TestCluster.disconnect_nodes(node_a, node_b)
      assert_receive {:nodedown_on_remote, ^node_b}, 5_000
      assert_receive {:nodedown_on_remote, ^node_a}, 5_000

      for i <- 6..8 do
        assert :ok ==
                 TestCluster.rpc!(node_a, EKV, :put, [
                   ekv_name,
                   "retained_window/#{i}",
                   "v#{i}"
                 ])
      end

      trigger_gc(node_a, ekv_name, 0, 10_000)

      assert TestCluster.peer_progress(node_a, ekv_name, node_b, node_a) == 5

      assert :ok = TestCluster.trace_shard_sends(node_a, ekv_name, self())
      assert :ok = TestCluster.trace_shard_sends(node_b, ekv_name, self())
      TestCluster.reconnect_nodes(node_a, node_b)

      TestCluster.assert_eventually(fn ->
        TestCluster.keys_count(node_b, ekv_name, "retained_window/") == 8
      end)

      trace_messages = collect_trace_messages([], 2_000)

      assert Enum.any?(trace_messages, fn
               {:request, shard, request, _destination} ->
                 shard == 0 and request == {:delta, node_a, 5}

               _ ->
                 false
             end)

      assert Enum.any?(trace_messages, fn
               {:sync, from_node, shard, mode, _keys, _progress, _destination} ->
                 from_node == node_a and shard == 0 and mode == :delta

               _ ->
                 false
             end)

      refute Enum.any?(trace_messages, fn
               {:sync, from_node, shard, mode, _keys, _progress, _destination} ->
                 from_node == node_a and shard == 0 and mode == :full

               _ ->
                 false
             end)

      assert :ok = TestCluster.untrace_shard_sends(node_a, ekv_name)
      assert :ok = TestCluster.untrace_shard_sends(node_b, ekv_name)
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

      b_max = TestCluster.max_seq(node_b, ekv_name)

      TestCluster.assert_eventually(fn ->
        state = TestCluster.replica_state(node_b, ekv_name)
        get_in(state.remote_member_progress, [node_a, node_b]) == b_max
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

      assert :ok = TestCluster.force_local_progress(node_b, ekv_name, node_a, 0)

      assert :ok = TestCluster.trace_shard_sends(node_a, ekv_name, self())
      assert :ok = TestCluster.trigger_anti_entropy(node_b, ekv_name)
      assert :ok = TestCluster.trigger_anti_entropy(node_b, ekv_name)

      details = collect_sync_messages([], 500)

      assert Enum.map(details, fn {_from, _shard, _keys, len, _seq, _dest} -> len end) == [
               2,
               2,
               1
             ]

      assert :ok = TestCluster.untrace_shard_sends(node_a, ekv_name)
    end
  end
end
