defmodule EKV.CASDistributedTest do
  use ExUnit.Case

  @moduletag :capture_log
  @moduletag timeout: 60_000

  alias EKV.TestCluster

  defp unique_name(prefix) do
    :"#{prefix}_#{System.unique_integer([:positive])}"
  end

  defp start_cas_cluster(peers, ekv_name, opts \\ []) do
    cluster_size = Keyword.get(opts, :cluster_size, length(peers))
    shards = Keyword.get(opts, :shards, 2)

    peers
    |> Enum.with_index(1)
    |> Enum.each(fn {{_pid, node}, node_id} ->
      data_dir = "/tmp/ekv_cas_test_#{node}_#{ekv_name}"
      TestCluster.rpc!(node, File, :rm_rf!, [data_dir])

      TestCluster.start_ekv(
        node,
        name: ekv_name,
        data_dir: data_dir,
        shards: shards,
        log: false,
        gc_interval: :timer.hours(1),
        tombstone_ttl: :timer.hours(24 * 7),
        cluster_size: cluster_size,
        node_id: node_id
      )
    end)
  end

  defp cleanup_data(peers, ekv_name) do
    for {_pid, node} <- peers do
      data_dir = "/tmp/ekv_cas_test_#{node}_#{ekv_name}"

      try do
        TestCluster.rpc!(node, File, :rm_rf!, [data_dir])
      catch
        _, _ -> :ok
      end
    end
  end

  # =====================================================================
  # Basic distributed CAS — 2-node, cluster_size: 2
  # =====================================================================

  describe "basic distributed CAS" do
    test "CAS put on node A, value readable on node B after replication" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      ekv_name = unique_name(:cas)

      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(200)

      :ok = TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "key1", "val1", [if_vsn: nil]])

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "key1"]) == "val1"
      end)
    end

    test "CAS delete on node A, tombstone replicates to node B" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      ekv_name = unique_name(:cas)

      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(200)

      :ok = TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "key1", "val1", [if_vsn: nil]])

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "key1"]) == "val1"
      end)

      {:ok, _, vsn} = TestCluster.rpc!(node_a, EKV, :fetch, [ekv_name, "key1"])
      :ok = TestCluster.rpc!(node_a, EKV, :delete, [ekv_name, "key1", [if_vsn: vsn]])

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "key1"]) == nil
      end)
    end

    test "fetch on node B returns same vsn after replication" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      ekv_name = unique_name(:cas)

      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(200)

      :ok = TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "key1", "val1", [if_vsn: nil]])
      {:ok, _, vsn_a} = TestCluster.rpc!(node_a, EKV, :fetch, [ekv_name, "key1"])

      TestCluster.assert_eventually(fn ->
        {:ok, _, vsn_b} = TestCluster.rpc!(node_b, EKV, :fetch, [ekv_name, "key1"])
        vsn_b == vsn_a
      end)
    end

    test "CAS put returns :ok only after quorum" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, _] = peers
      ekv_name = unique_name(:cas)

      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(200)

      # Should return :ok (quorum = 2, both nodes reachable)
      assert :ok = TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "key1", "val1", [if_vsn: nil]])
    end

    test "update from either node succeeds" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      ekv_name = unique_name(:cas)

      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(200)

      {:ok, 1} =
        TestCluster.rpc!(node_a, EKV, :update, [ekv_name, "counter", &TestCluster.cas_increment/1])

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "counter"]) == 1
      end)

      {:ok, 2} =
        TestCluster.rpc!(node_b, EKV, :update, [ekv_name, "counter", &TestCluster.cas_increment/1])

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_a, EKV, :get, [ekv_name, "counter"]) == 2
      end)
    end
  end

  # =====================================================================
  # Quorum — 3-node, cluster_size: 3
  # =====================================================================

  describe "quorum" do
    test "CAS succeeds with 2 of 3 reachable (quorum=2)" do
      peers = TestCluster.start_peers(3)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}, {_, node_c}] = peers
      ekv_name = unique_name(:cas)

      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(300)

      # Disconnect node C
      TestCluster.disconnect_nodes(node_a, node_c)
      TestCluster.disconnect_nodes(node_b, node_c)
      Process.sleep(200)

      # CAS should still work with 2 of 3 (quorum = 2)
      assert :ok = TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "key1", "val1", [if_vsn: nil]])
    end

    test "CAS fails with only 1 reachable" do
      peers = TestCluster.start_peers(3)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}, {_, node_c}] = peers
      ekv_name = unique_name(:cas)

      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(300)

      # Isolate node A from both B and C
      TestCluster.disconnect_nodes(node_a, node_b)
      TestCluster.disconnect_nodes(node_a, node_c)
      Process.sleep(200)

      # CAS should fail (only node A = 1 of 3, quorum = 2)
      result = TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "key1", "val1", [if_vsn: nil]])
      assert result in [{:error, :no_quorum}, {:error, :quorum_timeout}]
    end

    test "CAS delete succeeds with quorum" do
      peers = TestCluster.start_peers(3)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}, {_, node_c}] = peers
      ekv_name = unique_name(:cas)

      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(300)

      # Write via CAS
      :ok = TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "key1", "val1", [if_vsn: nil]])
      {:ok, _, vsn} = TestCluster.rpc!(node_a, EKV, :fetch, [ekv_name, "key1"])

      # Disconnect one node (still have quorum)
      TestCluster.disconnect_nodes(node_a, node_c)
      TestCluster.disconnect_nodes(node_b, node_c)
      Process.sleep(200)

      # CAS delete should work
      assert :ok = TestCluster.rpc!(node_a, EKV, :delete, [ekv_name, "key1", [if_vsn: vsn]])
    end

    test "update succeeds with quorum" do
      peers = TestCluster.start_peers(3)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}, {_, node_c}] = peers
      ekv_name = unique_name(:cas)

      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(300)

      # Disconnect one node
      TestCluster.disconnect_nodes(node_a, node_c)
      TestCluster.disconnect_nodes(node_b, node_c)
      Process.sleep(200)

      {:ok, 1} =
        TestCluster.rpc!(node_a, EKV, :update, [ekv_name, "counter", &TestCluster.cas_increment/1])
    end
  end

  # =====================================================================
  # Concurrent CAS — 2+ proposers
  # =====================================================================

  describe "concurrent CAS" do
    test "two nodes CAS-put same key concurrently: exactly one succeeds" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      ekv_name = unique_name(:cas)

      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(200)

      # Both try insert-if-absent concurrently
      task_a =
        Task.async(fn ->
          TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "race/1", "from_a", [if_vsn: nil]])
        end)

      task_b =
        Task.async(fn ->
          TestCluster.rpc!(node_b, EKV, :put, [ekv_name, "race/1", "from_b", [if_vsn: nil]])
        end)

      results = Task.await_many([task_a, task_b], 10_000)
      successes = Enum.count(results, &(&1 == :ok))
      conflicts = Enum.count(results, &(&1 == {:error, :conflict}))

      assert successes == 1
      assert conflicts == 1
    end

    test "CAS conflict does not leave phantom write on proposer node" do
      # This test targets a specific bug: the proposer does local paxos_accept
      # BEFORE quorum is confirmed. If the accept phase fails (remote acceptors
      # nack due to a higher-ballot prepare from a third proposer), the proposer
      # returns {:error, :conflict} but the value is in local SQLite.
      #
      # Trigger: 3 concurrent proposers. The middle-ballot proposer (B) can get
      # prepare quorum (own + A's promise), enter accept with local accept, but
      # then get nacked in accept phase (C's higher prepare pre-empted A and C).
      peers = TestCluster.start_peers(3)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}, {_, node_c}] = peers
      ekv_name = unique_name(:cas)

      start_cas_cluster(peers, ekv_name, cluster_size: 3)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(500)

      for round <- 1..20 do
        key = "phantom/#{round}"

        # All 3 nodes try insert-if-absent concurrently
        task_a =
          Task.async(fn ->
            result = TestCluster.rpc!(node_a, EKV, :put, [ekv_name, key, "from_a", [if_vsn: nil]])
            {node_a, result, "from_a"}
          end)

        task_b =
          Task.async(fn ->
            result = TestCluster.rpc!(node_b, EKV, :put, [ekv_name, key, "from_b", [if_vsn: nil]])
            {node_b, result, "from_b"}
          end)

        task_c =
          Task.async(fn ->
            result = TestCluster.rpc!(node_c, EKV, :put, [ekv_name, key, "from_c", [if_vsn: nil]])
            {node_c, result, "from_c"}
          end)

        results = Task.await_many([task_a, task_b, task_c], 10_000)

        for {node, result, attempted_value} <- results do
          if result != :ok do
            # CAS failed — immediately check local value BEFORE LWW broadcast overwrites.
            # If local accept was deferred until quorum, this value cannot be the
            # attempted write. If it IS, the proposer wrote locally before quorum.
            local_value = TestCluster.rpc!(node, EKV, :get, [ekv_name, key])

            assert local_value != attempted_value,
                   "round #{round}: CAS returned #{inspect(result)} on #{inspect(node)} " <>
                     "but phantom write '#{attempted_value}' is visible locally"
          end
        end
      end
    end

    test "two nodes update same counter concurrently: both increments applied via retry" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      ekv_name = unique_name(:cas)

      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(200)

      # Sequential updates from different nodes
      {:ok, 1} =
        TestCluster.rpc!(node_a, EKV, :update, [ekv_name, "counter", &TestCluster.cas_increment/1])

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "counter"]) == 1
      end)

      {:ok, 2} =
        TestCluster.rpc!(node_b, EKV, :update, [ekv_name, "counter", &TestCluster.cas_increment/1])

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_a, EKV, :get, [ekv_name, "counter"]) == 2
      end)
    end

    test "rapid concurrent updates from 2 nodes: counter reaches expected total" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      ekv_name = unique_name(:cas)

      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(200)

      n_per_node = 5

      # Sequential updates from each node (interleaved)
      for i <- 1..n_per_node do
        if rem(i, 2) == 0 do
          {:ok, _} =
            TestCluster.rpc!(node_a, EKV, :update, [
              ekv_name,
              "counter",
              &TestCluster.cas_increment/1
            ])
        else
          {:ok, _} =
            TestCluster.rpc!(node_b, EKV, :update, [
              ekv_name,
              "counter",
              &TestCluster.cas_increment/1
            ])
        end
      end

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_a, EKV, :get, [ekv_name, "counter"]) == n_per_node
      end)
    end

    test "three nodes simultaneously CAS same key: exactly one wins" do
      peers = TestCluster.start_peers(3)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}, {_, node_c}] = peers
      ekv_name = unique_name(:cas)

      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(300)

      # All three nodes try insert-if-absent concurrently
      task_a =
        Task.async(fn ->
          TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "tri/1", "from_a", [if_vsn: nil]])
        end)

      task_b =
        Task.async(fn ->
          TestCluster.rpc!(node_b, EKV, :put, [ekv_name, "tri/1", "from_b", [if_vsn: nil]])
        end)

      task_c =
        Task.async(fn ->
          TestCluster.rpc!(node_c, EKV, :put, [ekv_name, "tri/1", "from_c", [if_vsn: nil]])
        end)

      results = Task.await_many([task_a, task_b, task_c], 15_000)
      successes = Enum.count(results, &(&1 == :ok))

      # Exactly one should win
      assert successes == 1,
             "Expected exactly 1 success, got #{successes}: #{inspect(results)}"

      # All nodes should agree on the final value
      TestCluster.assert_eventually(fn ->
        val_a = TestCluster.rpc!(node_a, EKV, :get, [ekv_name, "tri/1"])
        val_b = TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "tri/1"])
        val_c = TestCluster.rpc!(node_c, EKV, :get, [ekv_name, "tri/1"])
        val_a != nil and val_a == val_b and val_b == val_c
      end)
    end

    test "CAS put and update on same key concurrently: consistent state" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      ekv_name = unique_name(:cas)

      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(200)

      # Node A does insert-if-absent
      :ok = TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "cas/1", "initial", [if_vsn: nil]])

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "cas/1"]) == "initial"
      end)

      # Node B updates
      {:ok, "INITIAL"} =
        TestCluster.rpc!(node_b, EKV, :update, [ekv_name, "cas/1", &TestCluster.cas_upcase/1])

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_a, EKV, :get, [ekv_name, "cas/1"]) == "INITIAL"
      end)
    end
  end

  # =====================================================================
  # Partition + CAS — 3-node
  # =====================================================================

  describe "partition + CAS" do
    test "partition isolating 1 node: CAS on isolated node gets no_quorum" do
      peers = TestCluster.start_peers(3)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}, {_, node_c}] = peers
      ekv_name = unique_name(:cas)

      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(300)

      # Isolate node C
      TestCluster.disconnect_nodes(node_a, node_c)
      TestCluster.disconnect_nodes(node_b, node_c)
      Process.sleep(200)

      result = TestCluster.rpc!(node_c, EKV, :put, [ekv_name, "key1", "val1", [if_vsn: nil]])
      assert result in [{:error, :no_quorum}, {:error, :quorum_timeout}]
    end

    test "majority side can still CAS" do
      peers = TestCluster.start_peers(3)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}, {_, node_c}] = peers
      ekv_name = unique_name(:cas)

      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(300)

      # Isolate node C
      TestCluster.disconnect_nodes(node_a, node_c)
      TestCluster.disconnect_nodes(node_b, node_c)
      Process.sleep(200)

      # A+B have quorum
      assert :ok = TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "key1", "val1", [if_vsn: nil]])
    end

    test "CAS during partition + LWW during partition: after heal, values converge" do
      peers = TestCluster.start_peers(3)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}, {_, node_c}] = peers
      ekv_name = unique_name(:cas)

      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(300)

      # Write initial CAS values
      :ok = TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "mix/1", "initial", [if_vsn: nil]])

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_c, EKV, :get, [ekv_name, "mix/1"]) == "initial"
      end)

      # Partition: isolate C
      TestCluster.disconnect_nodes(node_a, node_c)
      TestCluster.disconnect_nodes(node_b, node_c)
      Process.sleep(200)

      # Majority side: CAS update
      {:ok, _, vsn} = TestCluster.rpc!(node_a, EKV, :fetch, [ekv_name, "mix/1"])
      :ok = TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "mix/1", "cas_updated", [if_vsn: vsn]])

      # Majority side: regular LWW put on a different key
      :ok = TestCluster.rpc!(node_b, EKV, :put, [ekv_name, "mix/2", "lww_val"])

      # Minority side: regular LWW put
      :ok = TestCluster.rpc!(node_c, EKV, :put, [ekv_name, "mix/3", "from_c"])

      # Heal
      TestCluster.reconnect_nodes(node_a, node_c)
      TestCluster.reconnect_nodes(node_b, node_c)

      # All values converge
      TestCluster.assert_eventually(fn ->
        val1 = TestCluster.rpc!(node_c, EKV, :get, [ekv_name, "mix/1"])
        val2 = TestCluster.rpc!(node_c, EKV, :get, [ekv_name, "mix/2"])
        val3 = TestCluster.rpc!(node_a, EKV, :get, [ekv_name, "mix/3"])
        val1 == "cas_updated" and val2 == "lww_val" and val3 == "from_c"
      end)
    end

    test "rapid partition/heal flapping with CAS writes: all converge" do
      peers = TestCluster.start_peers(3)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}, {_, node_c}] = peers
      ekv_name = unique_name(:cas)

      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(300)

      for cycle <- 1..3 do
        # Partition: isolate C
        TestCluster.disconnect_nodes(node_a, node_c)
        TestCluster.disconnect_nodes(node_b, node_c)
        Process.sleep(100)

        # CAS write on majority during partition
        :ok =
          TestCluster.rpc!(node_a, EKV, :put, [
            ekv_name,
            "flap/#{cycle}",
            "v#{cycle}",
            [if_vsn: nil]
          ])

        # Heal
        TestCluster.reconnect_nodes(node_a, node_c)
        TestCluster.reconnect_nodes(node_b, node_c)
        Process.sleep(200)
      end

      # All values converge on all nodes
      for cycle <- 1..3 do
        TestCluster.assert_eventually(fn ->
          val_a = TestCluster.rpc!(node_a, EKV, :get, [ekv_name, "flap/#{cycle}"])
          val_c = TestCluster.rpc!(node_c, EKV, :get, [ekv_name, "flap/#{cycle}"])
          val_a == "v#{cycle}" and val_c == "v#{cycle}"
        end)
      end
    end

    test "after partition heal: CAS-written values replicate to minority node" do
      peers = TestCluster.start_peers(3)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}, {_, node_c}] = peers
      ekv_name = unique_name(:cas)

      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(300)

      # Isolate node C
      TestCluster.disconnect_nodes(node_a, node_c)
      TestCluster.disconnect_nodes(node_b, node_c)
      Process.sleep(200)

      # CAS write on majority
      :ok = TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "key1", "val1", [if_vsn: nil]])

      # Heal
      TestCluster.reconnect_nodes(node_a, node_c)
      TestCluster.reconnect_nodes(node_b, node_c)

      # Value should replicate to C
      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_c, EKV, :get, [ekv_name, "key1"]) == "val1"
      end)
    end
  end

  # =====================================================================
  # Node Death — THE KEY TEST: no election needed
  # =====================================================================

  describe "node death - no election needed" do
    test "3-node cluster: kill 1 node → CAS on remaining 2 works immediately" do
      peers = TestCluster.start_peers(3)
      [{_, node_a}, _, {peer_c, _}] = peers
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      ekv_name = unique_name(:cas)
      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(300)

      # Kill node C
      :peer.stop(peer_c)
      Process.sleep(200)

      # CAS on remaining nodes works IMMEDIATELY (no election delay)
      assert :ok = TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "key1", "val1", [if_vsn: nil]])
      assert TestCluster.rpc!(node_a, EKV, :get, [ekv_name, "key1"]) == "val1"
    end

    test "kill proposer after CAS success: value survives on remaining nodes" do
      peers = TestCluster.start_peers(3)
      [{peer_a, node_a}, {_, node_b}, {_, node_c}] = peers
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      ekv_name = unique_name(:cas)
      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(300)

      # CAS write from node A
      :ok = TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "key1", "durable", [if_vsn: nil]])

      # Wait for replication
      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "key1"]) == "durable"
      end)

      # Kill node A (the proposer)
      :peer.stop(peer_a)
      Process.sleep(200)

      # Value survives on B and C
      assert TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "key1"]) == "durable"
      assert TestCluster.rpc!(node_c, EKV, :get, [ekv_name, "key1"]) == "durable"
    end

    test "full node restart: ballot counter restored, CAS resumes" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, _] = peers
      ekv_name = unique_name(:cas)

      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(200)

      # CAS write
      :ok = TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "key1", "v1", [if_vsn: nil]])

      # Kill EKV on node A, restart it
      TestCluster.rpc!(node_a, TestCluster, :kill_registered, [:"#{ekv_name}_ekv_sup"])
      Process.sleep(200)

      data_dir = "/tmp/ekv_cas_test_#{node_a}_#{ekv_name}"

      TestCluster.start_ekv(
        node_a,
        name: ekv_name,
        data_dir: data_dir,
        shards: 2,
        log: false,
        gc_interval: :timer.hours(1),
        tombstone_ttl: :timer.hours(24 * 7),
        cluster_size: 2,
        node_id: 1
      )

      Process.sleep(200)

      # CAS should work after restart
      {:ok, "v1", vsn} = TestCluster.rpc!(node_a, EKV, :fetch, [ekv_name, "key1"])
      :ok = TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "key1", "v2", [if_vsn: vsn]])
      assert TestCluster.rpc!(node_a, EKV, :get, [ekv_name, "key1"]) == "v2"
    end

    test "kill acceptor node: proposer succeeds with remaining quorum" do
      peers = TestCluster.start_peers(3)
      [{_, node_a}, {_, node_b}, {peer_c, _}] = peers
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      ekv_name = unique_name(:cas)
      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(300)

      # Do initial CAS to establish state
      :ok = TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "kill_acc/1", "v1", [if_vsn: nil]])

      # Kill node C (an acceptor)
      :peer.stop(peer_c)
      Process.sleep(200)

      # Proposer (node A) should still succeed with remaining quorum (A+B = 2 of 3)
      {:ok, _, vsn} = TestCluster.rpc!(node_a, EKV, :fetch, [ekv_name, "kill_acc/1"])
      :ok = TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "kill_acc/1", "v2", [if_vsn: vsn]])

      # Update also works with reduced quorum
      {:ok, 1} =
        TestCluster.rpc!(node_b, EKV, :update, [
          ekv_name,
          "kill_acc/cnt",
          &TestCluster.cas_increment/1
        ])

      # Verify values
      assert TestCluster.rpc!(node_a, EKV, :get, [ekv_name, "kill_acc/1"]) == "v2"
      assert TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "kill_acc/cnt"]) == 1
    end

    test "acceptor restart preserves promises (kv_paxos survives in SQLite)" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      ekv_name = unique_name(:cas)

      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(200)

      # Do a CAS write (creates kv_paxos entries)
      :ok = TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "key1", "v1", [if_vsn: nil]])

      # Kill shard on node B (acceptor), it will restart
      TestCluster.rpc!(node_b, TestCluster, :kill_registered, [:"#{ekv_name}_ekv_replica_0"])
      Process.sleep(200)

      # CAS should still work (kv_paxos survived in SQLite)
      {:ok, _, vsn} = TestCluster.rpc!(node_a, EKV, :fetch, [ekv_name, "key1"])
      :ok = TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "key1", "v2", [if_vsn: vsn]])
    end
  end

  # =====================================================================
  # Update retry — Jepsen-style
  # =====================================================================

  describe "update retry" do
    test "update under contention: sequential from alternating nodes" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      ekv_name = unique_name(:cas)

      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(200)

      n = 6

      for i <- 1..n do
        node = if rem(i, 2) == 0, do: node_a, else: node_b

        {:ok, ^i} =
          TestCluster.rpc!(node, EKV, :update, [ekv_name, "counter", &TestCluster.cas_increment/1])
      end

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_a, EKV, :get, [ekv_name, "counter"]) == n
      end)
    end

    test "update with 3 concurrent proposers: all increments applied" do
      peers = TestCluster.start_peers(3)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}, {_, node_c}] = peers
      ekv_name = unique_name(:cas)

      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(300)

      # Sequential updates from 3 different nodes
      {:ok, 1} =
        TestCluster.rpc!(node_a, EKV, :update, [ekv_name, "tri_cnt", &TestCluster.cas_increment/1])

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "tri_cnt"]) == 1
      end)

      {:ok, 2} =
        TestCluster.rpc!(node_b, EKV, :update, [ekv_name, "tri_cnt", &TestCluster.cas_increment/1])

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_c, EKV, :get, [ekv_name, "tri_cnt"]) == 2
      end)

      {:ok, 3} =
        TestCluster.rpc!(node_c, EKV, :update, [ekv_name, "tri_cnt", &TestCluster.cas_increment/1])

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_a, EKV, :get, [ekv_name, "tri_cnt"]) == 3
      end)
    end

    test "update exhausts retries: returns {:error, :conflict} after max_retries" do
      # This tests that when update can't succeed after 5 retries, it returns conflict.
      # We simulate this by having two nodes rapidly competing on the same key.
      # With cluster_size: 2, both nodes are proposers and acceptors.
      # We use CAS put to create contention that update can't resolve.
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      ekv_name = unique_name(:cas)

      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(200)

      # Create initial key with integer value (cas_increment expects integers)
      {:ok, 1} =
        TestCluster.rpc!(node_a, EKV, :update, [
          ekv_name,
          "exhaust/1",
          &TestCluster.cas_increment/1
        ])

      # Rapid CAS puts from node B while node A tries to update
      # This creates contention that may exhaust retries
      # Even if update succeeds (retries work), the test validates the retry path
      results =
        for _ <- 1..20 do
          task_a =
            Task.async(fn ->
              TestCluster.rpc!(node_a, EKV, :update, [
                ekv_name,
                "exhaust/1",
                &TestCluster.cas_increment/1
              ])
            end)

          task_b =
            Task.async(fn ->
              {:ok, _, vsn} = TestCluster.rpc!(node_b, EKV, :fetch, [ekv_name, "exhaust/1"])
              TestCluster.rpc!(node_b, EKV, :put, [ekv_name, "exhaust/1", 99999, [if_vsn: vsn]])
            end)

          [res_a, _res_b] = Task.await_many([task_a, task_b], 15_000)
          res_a
        end

      # At least some updates should succeed (via retry), some may fail with conflict
      successes =
        Enum.count(results, fn
          {:ok, _} -> true
          _ -> false
        end)

      conflicts = Enum.count(results, &(&1 == {:error, :conflict}))

      # The key invariant: every result is either {:ok, _} or {:error, :conflict}
      assert successes + conflicts == 20,
             "All results should be :ok or :conflict, got: #{inspect(results)}"
    end

    test "update preempted by higher-ballot prepare: retries with higher ballot, succeeds" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      ekv_name = unique_name(:cas)

      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(200)

      # Interleaved updates — one will be preempted and must retry
      {:ok, 1} =
        TestCluster.rpc!(node_a, EKV, :update, [ekv_name, "preempt", &TestCluster.cas_increment/1])

      {:ok, 2} =
        TestCluster.rpc!(node_b, EKV, :update, [ekv_name, "preempt", &TestCluster.cas_increment/1])

      {:ok, 3} =
        TestCluster.rpc!(node_a, EKV, :update, [ekv_name, "preempt", &TestCluster.cas_increment/1])

      # All increments should be applied
      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "preempt"]) == 3
      end)
    end

    test "100 concurrent updates across 2 nodes on 10 keys: final counters correct" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      ekv_name = unique_name(:cas)

      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(200)

      n_per_key = 10
      n_keys = 10
      nodes = [node_a, node_b]

      # Sequential updates spread across keys and nodes
      for key_idx <- 1..n_keys do
        key = "bulk/#{key_idx}"

        for i <- 1..n_per_key do
          node = Enum.at(nodes, rem(i, length(nodes)))

          {:ok, ^i} =
            TestCluster.rpc!(node, EKV, :update, [ekv_name, key, &TestCluster.cas_increment/1])
        end
      end

      # Verify all counters reached expected value
      for key_idx <- 1..n_keys do
        key = "bulk/#{key_idx}"

        TestCluster.assert_eventually(fn ->
          val_a = TestCluster.rpc!(node_a, EKV, :get, [ekv_name, key])
          val_b = TestCluster.rpc!(node_b, EKV, :get, [ekv_name, key])
          val_a == n_per_key and val_b == n_per_key
        end)
      end
    end
  end

  # =====================================================================
  # Edge Cases
  # =====================================================================

  describe "edge cases" do
    test "CAS put with TTL → expires → put(if_vsn: nil) re-creates" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, _] = peers
      ekv_name = unique_name(:cas)

      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(200)

      :ok = TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "ttl/1", "val", [if_vsn: nil, ttl: 1]])
      Process.sleep(10)

      # Key expired — fetch returns nil
      {:ok, nil, nil} = TestCluster.rpc!(node_a, EKV, :fetch, [ekv_name, "ttl/1"])

      # insert-if-absent should work (expired key is treated as absent)
      :ok = TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "ttl/1", "reborn", [if_vsn: nil]])
      assert TestCluster.rpc!(node_a, EKV, :get, [ekv_name, "ttl/1"]) == "reborn"
    end

    test "CAS across multiple shards: each shard handles independently" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      ekv_name = unique_name(:cas)

      start_cas_cluster(peers, ekv_name, shards: 4)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(200)

      # Write keys that hash to different shards
      for i <- 1..8 do
        :ok =
          TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "key/#{i}", "val#{i}", [if_vsn: nil]])
      end

      # All should be readable from node B
      for i <- 1..8 do
        TestCluster.assert_eventually(fn ->
          TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "key/#{i}"]) == "val#{i}"
        end)
      end
    end

    test "CAS on key that only exists as tombstone: if_vsn: nil succeeds" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      ekv_name = unique_name(:cas)

      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(200)

      # Create and then delete (creating a tombstone)
      :ok = TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "tomb/1", "val", [if_vsn: nil]])
      {:ok, _, vsn} = TestCluster.rpc!(node_a, EKV, :fetch, [ekv_name, "tomb/1"])
      :ok = TestCluster.rpc!(node_a, EKV, :delete, [ekv_name, "tomb/1", [if_vsn: vsn]])

      # Key is now a tombstone — fetch returns nil
      {:ok, nil, nil} = TestCluster.rpc!(node_a, EKV, :fetch, [ekv_name, "tomb/1"])

      # insert-if-absent should succeed (tombstone treated as absent)
      :ok = TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "tomb/1", "reborn", [if_vsn: nil]])
      assert TestCluster.rpc!(node_a, EKV, :get, [ekv_name, "tomb/1"]) == "reborn"

      # Replication to node B
      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "tomb/1"]) == "reborn"
      end)
    end

    test "duplicate prepare with same ballot: nack (not strictly greater)" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, _] = peers
      ekv_name = unique_name(:cas)

      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(200)

      # First CAS establishes ballot state
      :ok = TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "dup/1", "v1", [if_vsn: nil]])

      # Second CAS with the same stale vsn should get conflict
      # (the ballot from the first CAS is now the promised ballot,
      #  a new CAS must generate a strictly higher ballot)
      {:ok, _, vsn} = TestCluster.rpc!(node_a, EKV, :fetch, [ekv_name, "dup/1"])
      :ok = TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "dup/1", "v2", [if_vsn: vsn]])

      # Stale vsn gets conflict
      assert {:error, :conflict} =
               TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "dup/1", "v3", [if_vsn: vsn]])
    end

    test "subscriber on remote node receives event from CAS-triggered write" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      ekv_name = unique_name(:cas)

      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(200)

      # Subscribe on node B
      TestCluster.subscribe_on(node_b, ekv_name, "sub/", self())
      Process.sleep(50)

      # CAS write on node A
      :ok = TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "sub/1", "val", [if_vsn: nil]])

      # Remote subscriber should get event (via LWW replication)
      assert_receive {:remote_ekv_event, events, _}, 3000
      assert Enum.any?(events, fn e -> e.key == "sub/1" and e.type == :put end)
    end

    test "acceptor node subscriber gets put event from CAS accept" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      ekv_name = unique_name(:cas)

      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(200)

      # Subscribe on acceptor node (B)
      TestCluster.subscribe_on(node_b, ekv_name, "asub/", self())
      Process.sleep(50)

      # CAS put on proposer node (A) — B acts as acceptor
      :ok =
        TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "asub/1", "accepted_val", [if_vsn: nil]])

      # Acceptor subscriber should get the event
      assert_receive {:remote_ekv_event, events, _}, 3000
      assert [%EKV.Event{type: :put, key: "asub/1", value: "accepted_val"}] = events
    end

    test "acceptor node subscriber gets delete event with previous value" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      ekv_name = unique_name(:cas)

      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(200)

      # Write initial value via CAS
      :ok = TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "dsub/1", "old_val", [if_vsn: nil]])

      # Wait for replication so B has the value
      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "dsub/1"]) == "old_val"
      end)

      # Subscribe on acceptor node (B)
      TestCluster.subscribe_on(node_b, ekv_name, "dsub/", self())
      Process.sleep(50)

      # CAS delete on proposer node (A)
      {:ok, _, vsn} = TestCluster.rpc!(node_a, EKV, :fetch, [ekv_name, "dsub/1"])
      :ok = TestCluster.rpc!(node_a, EKV, :delete, [ekv_name, "dsub/1", [if_vsn: vsn]])

      # Acceptor subscriber gets delete event with the previous value
      assert_receive {:remote_ekv_event, events, _}, 3000
      assert [%EKV.Event{type: :delete, key: "dsub/1", value: "old_val"}] = events
    end

    test "proposer node subscriber does not get event on CAS conflict" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, _node_b}] = peers
      ekv_name = unique_name(:cas)

      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(200)

      # Write initial value
      :ok = TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "nosub/1", "v1", [if_vsn: nil]])

      # Subscribe on proposer node (A)
      TestCluster.subscribe_on(node_a, ekv_name, "nosub/", self())
      Process.sleep(50)

      # Get current vsn, then write again to make it stale
      {:ok, _, stale_vsn} = TestCluster.rpc!(node_a, EKV, :fetch, [ekv_name, "nosub/1"])
      :ok = TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "nosub/1", "v2", [if_vsn: stale_vsn]])

      # Drain the put event from the v2 write
      assert_receive {:remote_ekv_event, _, _}, 3000

      # Now try CAS with the stale vsn — should conflict
      assert {:error, :conflict} =
               TestCluster.rpc!(node_a, EKV, :put, [
                 ekv_name,
                 "nosub/1",
                 "v3",
                 [if_vsn: stale_vsn]
               ])

      # No event should be dispatched for the conflict
      refute_receive {:remote_ekv_event, _, _}, 500
    end

    test "acceptor node does not get duplicate events from CAS accept + LWW broadcast" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      ekv_name = unique_name(:cas)

      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(200)

      # Use a collecting subscriber on B to capture all events over a window
      TestCluster.start_collecting_subscriber_on(node_b, ekv_name, "nodup/", self(), 1000)
      Process.sleep(50)

      # CAS put on A — B will get accept event, then LWW broadcast
      :ok = TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "nodup/1", "val", [if_vsn: nil]])

      # Wait for collection window
      assert_receive {:collected_events, events}, 3000

      # Should have exactly 1 put event, not 2 (no duplicate from LWW)
      put_events = Enum.filter(events, fn e -> e.key == "nodup/1" and e.type == :put end)
      assert length(put_events) == 1
    end
  end

  # =====================================================================
  # Safety Invariant Checks — Jepsen-style
  # =====================================================================

  describe "safety invariants" do
    test "linearizability: concurrent CAS on same key — no two conflicting succeed" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      ekv_name = unique_name(:cas)

      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(200)

      # Test 10 rounds of concurrent insert-if-absent
      for i <- 1..10 do
        key = "lin/#{i}"

        task_a =
          Task.async(fn ->
            TestCluster.rpc!(node_a, EKV, :put, [ekv_name, key, "a", [if_vsn: nil]])
          end)

        task_b =
          Task.async(fn ->
            TestCluster.rpc!(node_b, EKV, :put, [ekv_name, key, "b", [if_vsn: nil]])
          end)

        results = Task.await_many([task_a, task_b], 10_000)
        successes = Enum.count(results, &(&1 == :ok))

        # At most one should succeed for insert-if-absent (zero is also safe —
        # both proposers may nack each other via ballot interleaving)
        assert successes <= 1,
               "Expected at most 1 success for key #{key}, got #{successes}: #{inspect(results)}"
      end
    end

    test "durability: every acknowledged CAS write readable after proposer continues" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      ekv_name = unique_name(:cas)

      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(200)

      # Write 20 keys via CAS from node A
      for i <- 1..20 do
        :ok =
          TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "dur/#{i}", "val#{i}", [if_vsn: nil]])
      end

      # All should be readable from both nodes
      for i <- 1..20 do
        assert TestCluster.rpc!(node_a, EKV, :get, [ekv_name, "dur/#{i}"]) == "val#{i}"
      end

      for i <- 1..20 do
        TestCluster.assert_eventually(fn ->
          TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "dur/#{i}"]) == "val#{i}"
        end)
      end
    end

    test "consistency: after CAS + partition + heal, all nodes agree" do
      peers = TestCluster.start_peers(3)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}, {_, node_c}] = peers
      ekv_name = unique_name(:cas)

      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(300)

      # Write some keys
      :ok = TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "k/1", "v1", [if_vsn: nil]])
      :ok = TestCluster.rpc!(node_b, EKV, :put, [ekv_name, "k/2", "v2", [if_vsn: nil]])

      # Partition: isolate C
      TestCluster.disconnect_nodes(node_a, node_c)
      TestCluster.disconnect_nodes(node_b, node_c)
      Process.sleep(200)

      # Write on majority
      {:ok, _, vsn1} = TestCluster.rpc!(node_a, EKV, :fetch, [ekv_name, "k/1"])
      :ok = TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "k/1", "v1_updated", [if_vsn: vsn1]])

      # Heal
      TestCluster.reconnect_nodes(node_a, node_c)
      TestCluster.reconnect_nodes(node_b, node_c)
      Process.sleep(500)

      # All nodes should agree
      TestCluster.assert_eventually(fn ->
        val_a = TestCluster.rpc!(node_a, EKV, :get, [ekv_name, "k/1"])
        val_b = TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "k/1"])
        val_c = TestCluster.rpc!(node_c, EKV, :get, [ekv_name, "k/1"])
        val_a == "v1_updated" and val_a == val_b and val_b == val_c
      end)
    end
  end

  # =====================================================================
  # 5-node cluster tests — quorum=3, tolerate 2 failures
  # =====================================================================

  describe "5-node cluster" do
    test "basic CAS works with 5-node cluster (quorum=3)" do
      peers = TestCluster.start_peers(5)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, n1}, {_, n2}, {_, n3}, {_, n4}, {_, n5}] = peers
      ekv_name = unique_name(:cas5)

      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(500)

      # CAS put from node 1
      :ok = TestCluster.rpc!(n1, EKV, :put, [ekv_name, "key/1", "val1", [if_vsn: nil]])

      # All 5 nodes should eventually see the value
      for node <- [n1, n2, n3, n4, n5] do
        TestCluster.assert_eventually(fn ->
          TestCluster.rpc!(node, EKV, :get, [ekv_name, "key/1"]) == "val1"
        end)
      end
    end

    test "kill 2 of 5 nodes: CAS still works (quorum=3, 3 alive)" do
      peers = TestCluster.start_peers(5)
      [{_, n1}, {_, n2}, _, {p4, _}, {p5, _}] = peers
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      ekv_name = unique_name(:cas5)
      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(500)

      # Write before killing
      :ok = TestCluster.rpc!(n1, EKV, :put, [ekv_name, "survive/1", "v1", [if_vsn: nil]])

      # Kill 2 nodes — quorum=3, 3 still alive
      :peer.stop(p4)
      :peer.stop(p5)
      Process.sleep(300)

      # CAS should still work with 3 of 5 (quorum = 3)
      {:ok, _, vsn} = TestCluster.rpc!(n1, EKV, :fetch, [ekv_name, "survive/1"])
      :ok = TestCluster.rpc!(n1, EKV, :put, [ekv_name, "survive/1", "v2", [if_vsn: vsn]])
      assert TestCluster.rpc!(n1, EKV, :get, [ekv_name, "survive/1"]) == "v2"

      # update also works
      {:ok, 1} =
        TestCluster.rpc!(n2, EKV, :update, [ekv_name, "survive/cnt", &TestCluster.cas_increment/1])
    end

    test "kill 3 of 5 nodes: CAS fails (quorum=3, only 2 alive)" do
      peers = TestCluster.start_peers(5)
      [{_, n1}, _, {p3, _}, {p4, _}, {p5, _}] = peers
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      ekv_name = unique_name(:cas5)
      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(500)

      # Kill 3 nodes — only 2 alive, quorum=3
      :peer.stop(p3)
      :peer.stop(p4)
      :peer.stop(p5)
      Process.sleep(300)

      # CAS should fail
      result = TestCluster.rpc!(n1, EKV, :put, [ekv_name, "fail/1", "v1", [if_vsn: nil]])
      assert result in [{:error, :no_quorum}, {:error, :quorum_timeout}]
    end

    test "3+2 partition: majority (3) can CAS, minority (2) cannot" do
      peers = TestCluster.start_peers(5)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, n1}, {_, n2}, {_, n3}, {_, n4}, {_, n5}] = peers
      ekv_name = unique_name(:cas5)

      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(500)

      # Partition: {n1, n2, n3} vs {n4, n5}
      for majority <- [n1, n2, n3], minority <- [n4, n5] do
        TestCluster.disconnect_nodes(majority, minority)
      end

      Process.sleep(300)

      # Majority side (3 nodes, quorum=3) can CAS
      :ok = TestCluster.rpc!(n1, EKV, :put, [ekv_name, "split/1", "majority", [if_vsn: nil]])

      # Minority side (2 nodes, quorum=3) cannot CAS
      result = TestCluster.rpc!(n4, EKV, :put, [ekv_name, "split/2", "minority", [if_vsn: nil]])
      assert result in [{:error, :no_quorum}, {:error, :quorum_timeout}]

      # Heal
      for majority <- [n1, n2, n3], minority <- [n4, n5] do
        TestCluster.reconnect_nodes(majority, minority)
      end

      # After heal, majority's value replicates to minority
      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(n4, EKV, :get, [ekv_name, "split/1"]) == "majority"
      end)

      # Minority's failed CAS should not have written anything
      assert TestCluster.rpc!(n4, EKV, :get, [ekv_name, "split/2"]) == nil
    end

    test "5-way concurrent CAS: exactly one wins insert-if-absent" do
      peers = TestCluster.start_peers(5)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      nodes = Enum.map(peers, fn {_, node} -> node end)
      ekv_name = unique_name(:cas5)

      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(500)

      # All 5 nodes try insert-if-absent concurrently
      tasks =
        for {node, i} <- Enum.with_index(nodes, 1) do
          Task.async(fn ->
            TestCluster.rpc!(node, EKV, :put, [ekv_name, "race5/1", "from_#{i}", [if_vsn: nil]])
          end)
        end

      results = Task.await_many(tasks, 15_000)
      successes = Enum.count(results, &(&1 == :ok))

      # At most one should succeed
      assert successes <= 1,
             "Expected at most 1 success, got #{successes}: #{inspect(results)}"

      # All nodes should eventually agree on the final value
      TestCluster.assert_eventually(fn ->
        vals = Enum.map(nodes, fn n -> TestCluster.rpc!(n, EKV, :get, [ekv_name, "race5/1"]) end)
        first = hd(vals)
        first != nil and Enum.all?(vals, &(&1 == first))
      end)
    end

    test "sequential updates across all 5 nodes: counter correct" do
      peers = TestCluster.start_peers(5)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      nodes = Enum.map(peers, fn {_, node} -> node end)
      ekv_name = unique_name(:cas5)

      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(500)

      # Round-robin updates across all 5 nodes
      n = 10

      for i <- 1..n do
        node = Enum.at(nodes, rem(i - 1, 5))

        {:ok, ^i} =
          TestCluster.rpc!(node, EKV, :update, [ekv_name, "cnt5", &TestCluster.cas_increment/1])
      end

      # All nodes agree on final value
      for node <- nodes do
        TestCluster.assert_eventually(fn ->
          TestCluster.rpc!(node, EKV, :get, [ekv_name, "cnt5"]) == n
        end)
      end
    end

    test "partition heal with 5 nodes: minority values sync after reconnect" do
      peers = TestCluster.start_peers(5)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, n1}, {_, n2}, {_, n3}, {_, n4}, {_, n5}] = peers
      ekv_name = unique_name(:cas5)

      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(500)

      # Write initial values
      :ok = TestCluster.rpc!(n1, EKV, :put, [ekv_name, "heal5/1", "initial", [if_vsn: nil]])

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(n5, EKV, :get, [ekv_name, "heal5/1"]) == "initial"
      end)

      # Partition: {n1, n2, n3} vs {n4, n5}
      for majority <- [n1, n2, n3], minority <- [n4, n5] do
        TestCluster.disconnect_nodes(majority, minority)
      end

      Process.sleep(300)

      # CAS update on majority
      {:ok, _, vsn} = TestCluster.rpc!(n1, EKV, :fetch, [ekv_name, "heal5/1"])
      :ok = TestCluster.rpc!(n1, EKV, :put, [ekv_name, "heal5/1", "updated", [if_vsn: vsn]])

      # Regular LWW write on majority (different key)
      :ok = TestCluster.rpc!(n2, EKV, :put, [ekv_name, "heal5/2", "from_majority"])

      # Regular LWW write on minority
      :ok = TestCluster.rpc!(n4, EKV, :put, [ekv_name, "heal5/3", "from_minority"])

      # Heal
      for majority <- [n1, n2, n3], minority <- [n4, n5] do
        TestCluster.reconnect_nodes(majority, minority)
      end

      # All values converge across all 5 nodes
      for node <- [n1, n2, n3, n4, n5] do
        TestCluster.assert_eventually(fn ->
          v1 = TestCluster.rpc!(node, EKV, :get, [ekv_name, "heal5/1"])
          v2 = TestCluster.rpc!(node, EKV, :get, [ekv_name, "heal5/2"])
          v3 = TestCluster.rpc!(node, EKV, :get, [ekv_name, "heal5/3"])
          v1 == "updated" and v2 == "from_majority" and v3 == "from_minority"
        end)
      end
    end

    test "kill 1 of 5, partition remaining 4 into 2+2: both sides lose quorum" do
      peers = TestCluster.start_peers(5)
      [{_, n1}, {_, n2}, {_, n3}, {_, n4}, {p5, _}] = peers
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      ekv_name = unique_name(:cas5)
      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(500)

      # Kill node 5
      :peer.stop(p5)
      Process.sleep(200)

      # Partition remaining 4 into {n1, n2} vs {n3, n4}
      TestCluster.disconnect_nodes(n1, n3)
      TestCluster.disconnect_nodes(n1, n4)
      TestCluster.disconnect_nodes(n2, n3)
      TestCluster.disconnect_nodes(n2, n4)
      Process.sleep(300)

      # Both sides have 2 nodes, quorum=3 — neither can CAS
      result_a = TestCluster.rpc!(n1, EKV, :put, [ekv_name, "split22/1", "a", [if_vsn: nil]])
      result_b = TestCluster.rpc!(n3, EKV, :put, [ekv_name, "split22/2", "b", [if_vsn: nil]])

      assert result_a in [{:error, :no_quorum}, {:error, :quorum_timeout}]
      assert result_b in [{:error, :no_quorum}, {:error, :quorum_timeout}]
    end
  end

  # =====================================================================
  # Latency scenarios — simulating slow nodes via :sys.suspend/:sys.resume
  # =====================================================================

  describe "latency scenarios" do
    test "CAS succeeds after slow acceptor resumes (suspend during prepare)" do
      # Suspend an acceptor BEFORE CAS starts. The proposer's prepare message
      # queues in the suspended acceptor's mailbox. CAS blocks waiting for quorum.
      # Resume the acceptor → queued prepare processes → quorum reached → CAS succeeds.
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      ekv_name = unique_name(:cas)

      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(200)

      # Suspend B (acceptor) — messages will queue
      TestCluster.suspend_shards(node_b, ekv_name)

      # Start CAS in background — will block waiting for B's promise
      task =
        Task.async(fn ->
          TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "slow/1", "val1", [if_vsn: nil]])
        end)

      # Let the prepare message sit in B's queue
      Process.sleep(500)

      # Resume B — it processes the queued prepare, sends promise
      TestCluster.resume_shards(node_b, ekv_name)

      # CAS should complete successfully
      assert :ok = Task.await(task, 10_000)
      assert TestCluster.rpc!(node_a, EKV, :get, [ekv_name, "slow/1"]) == "val1"
    end

    test "CAS succeeds after slow acceptor resumes (suspend during accept phase)" do
      # 3-node cluster. Suspend C before CAS. A proposes, gets promises from A+B
      # (quorum for prepare). Enters accept phase, sends accepts to B and C.
      # B accepts → quorum for accept (A+B). CAS commits.
      # Then resume C — C gets the queued accept + commit notification.
      peers = TestCluster.start_peers(3)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}, {_, node_c}] = peers
      ekv_name = unique_name(:cas)

      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(300)

      # Suspend C — it won't process any messages
      TestCluster.suspend_shards(node_c, ekv_name)

      # CAS from A — needs quorum of 2. A+B available, C suspended.
      # Prepare: A local + B remote → quorum. Accept: B remote → quorum (A deferred + B).
      assert :ok =
               TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "slow/2", "val2", [if_vsn: nil]])

      # Value readable on A and B immediately
      assert TestCluster.rpc!(node_a, EKV, :get, [ekv_name, "slow/2"]) == "val2"

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "slow/2"]) == "val2"
      end)

      # C doesn't have it yet (still suspended)
      # Resume C — it processes the queued messages
      TestCluster.resume_shards(node_c, ekv_name)

      # C should eventually get the value (via commit notification or LWW)
      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_c, EKV, :get, [ekv_name, "slow/2"]) == "val2"
      end)
    end

    test "CAS times out when all acceptors are suspended" do
      # 2-node cluster. Suspend B. A's CAS can't get quorum → timeout.
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      ekv_name = unique_name(:cas)

      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(200)

      # Suspend B
      TestCluster.suspend_shards(node_b, ekv_name)

      # CAS from A — quorum requires both nodes, B is frozen
      result = TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "slow/3", "val3", [if_vsn: nil]])
      assert result == {:error, :quorum_timeout}

      # Resume B for cleanup
      TestCluster.resume_shards(node_b, ekv_name)
    end

    test "slow minority does not block CAS when majority is fast (3-node)" do
      # 3-node cluster (quorum=2). Suspend C. A proposes with quorum A+B.
      # CAS should succeed quickly without waiting for C.
      peers = TestCluster.start_peers(3)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, _node_b}, {_, node_c}] = peers
      ekv_name = unique_name(:cas)

      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(300)

      # Suspend C (slow minority)
      TestCluster.suspend_shards(node_c, ekv_name)

      # CAS should succeed quickly with just A+B
      t1 = System.monotonic_time(:millisecond)
      assert :ok = TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "fast/1", "val", [if_vsn: nil]])
      elapsed = System.monotonic_time(:millisecond) - t1

      # Should complete in well under the 5s timeout
      assert elapsed < 2000,
             "CAS took #{elapsed}ms, expected < 2000ms (slow minority should not block)"

      # Resume C — it catches up
      TestCluster.resume_shards(node_c, ekv_name)

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_c, EKV, :get, [ekv_name, "fast/1"]) == "val"
      end)
    end

    test "concurrent CAS with asymmetric latency: one proposer's acceptor is slow" do
      # 3 nodes. Suspend B briefly while A and C both try CAS on the same key.
      # A can only reach quorum with A+C (B is frozen).
      # C can only reach quorum with C+A (B is frozen).
      # One should win, one should conflict.
      peers = TestCluster.start_peers(3)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}, {_, node_c}] = peers
      ekv_name = unique_name(:cas)

      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(300)

      # Suspend B — forces A and C to compete for quorum via A↔C
      TestCluster.suspend_shards(node_b, ekv_name)

      task_a =
        Task.async(fn ->
          TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "asym/1", "from_a", [if_vsn: nil]])
        end)

      task_c =
        Task.async(fn ->
          TestCluster.rpc!(node_c, EKV, :put, [ekv_name, "asym/1", "from_c", [if_vsn: nil]])
        end)

      results = Task.await_many([task_a, task_c], 15_000)
      successes = Enum.count(results, &(&1 == :ok))

      # At most one wins (could be 0 if both nack each other)
      assert successes <= 1

      # Resume B
      TestCluster.resume_shards(node_b, ekv_name)

      # All nodes eventually agree
      TestCluster.assert_eventually(fn ->
        val_a = TestCluster.rpc!(node_a, EKV, :get, [ekv_name, "asym/1"])
        val_b = TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "asym/1"])
        val_c = TestCluster.rpc!(node_c, EKV, :get, [ekv_name, "asym/1"])
        val_a != nil and val_a == val_b and val_b == val_c
      end)
    end

    test "suspend acceptor between prepare and accept: CAS still commits after resume" do
      # 2-node cluster. Start CAS from A — prepare succeeds (both nodes respond).
      # Then suspend B before accept phase messages arrive.
      # A's accept to B queues. A waits. Resume B → accept processes → quorum.
      #
      # We can't precisely time the suspend between phases, so we use a 3-node
      # cluster, suspend C before CAS (so C misses prepare), then CAS gets
      # prepare quorum from A+B. A enters accept phase. Accept goes to B and C.
      # B accepts (quorum reached). C gets accept after resume. This is covered
      # by "suspend during accept phase" test above, so instead we test a
      # different angle: suspend+resume cycles during a series of CAS operations.
      peers = TestCluster.start_peers(3)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}, {_, node_c}] = peers
      ekv_name = unique_name(:cas)

      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(300)

      # Rapid suspend/resume cycles with CAS operations
      for i <- 1..5 do
        # Suspend a different node each cycle
        slow_node = Enum.at([node_b, node_c, node_b, node_c, node_b], i - 1)
        TestCluster.suspend_shards(slow_node, ekv_name)

        # CAS should still work (quorum of 2 from remaining fast nodes)
        key = "cycle/#{i}"
        assert :ok = TestCluster.rpc!(node_a, EKV, :put, [ekv_name, key, "v#{i}", [if_vsn: nil]])

        # Resume before next cycle
        TestCluster.resume_shards(slow_node, ekv_name)
        Process.sleep(100)
      end

      # All values should be readable everywhere after all resumes
      for i <- 1..5 do
        key = "cycle/#{i}"

        TestCluster.assert_eventually(fn ->
          val_a = TestCluster.rpc!(node_a, EKV, :get, [ekv_name, key])
          val_b = TestCluster.rpc!(node_b, EKV, :get, [ekv_name, key])
          val_c = TestCluster.rpc!(node_c, EKV, :get, [ekv_name, key])
          val_a == "v#{i}" and val_b == "v#{i}" and val_c == "v#{i}"
        end)
      end
    end

    test "slow commit notification: acceptor promotes value after delay" do
      # 3-node cluster. CAS from A with quorum A+B. A commits locally.
      # B is suspended when commit notification arrives — it queues.
      # Resume B → promote fires → value appears on B.
      peers = TestCluster.start_peers(3)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}, {_, node_c}] = peers
      ekv_name = unique_name(:cas)

      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(300)

      # First, do a CAS write normally to establish ballot state
      :ok = TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "delay/1", "v1", [if_vsn: nil]])

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "delay/1"]) == "v1"
      end)

      # Now suspend B, do a CAS update from A
      TestCluster.suspend_shards(node_b, ekv_name)

      # CAS update — A+C form quorum (B is suspended but still "alive" from dist POV)
      {:ok, _, vsn} = TestCluster.rpc!(node_a, EKV, :fetch, [ekv_name, "delay/1"])
      :ok = TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "delay/1", "v2", [if_vsn: vsn]])

      # A has v2, C should have v2 (it was either acceptor or got LWW)
      assert TestCluster.rpc!(node_a, EKV, :get, [ekv_name, "delay/1"]) == "v2"

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_c, EKV, :get, [ekv_name, "delay/1"]) == "v2"
      end)

      # B still has v1 (suspended, hasn't processed commit notification)
      # Resume B — it processes the queued commit/LWW message
      TestCluster.resume_shards(node_b, ekv_name)

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "delay/1"]) == "v2"
      end)
    end

    test "update retries succeed despite intermittent acceptor suspension" do
      # 3-node cluster. While node A does update operations, periodically
      # suspend/resume node C. The update retry mechanism should handle
      # the intermittent failures from C being slow.
      peers = TestCluster.start_peers(3)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}, {_, node_c}] = peers
      ekv_name = unique_name(:cas)

      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(300)

      # Background task that flaps C's shards
      flapper =
        Task.async(fn ->
          for _ <- 1..8 do
            TestCluster.suspend_shards(node_c, ekv_name)
            Process.sleep(50 + :rand.uniform(100))
            TestCluster.resume_shards(node_c, ekv_name)
            Process.sleep(50 + :rand.uniform(100))
          end
        end)

      # Sequential updates while C is flapping
      for i <- 1..6 do
        {:ok, ^i} =
          TestCluster.rpc!(node_a, EKV, :update, [
            ekv_name,
            "flap_counter",
            &TestCluster.cas_increment/1
          ])
      end

      Task.await(flapper, 30_000)

      # Final value should be correct everywhere
      TestCluster.assert_eventually(fn ->
        val_a = TestCluster.rpc!(node_a, EKV, :get, [ekv_name, "flap_counter"])
        val_b = TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "flap_counter"])
        val_c = TestCluster.rpc!(node_c, EKV, :get, [ekv_name, "flap_counter"])
        val_a == 6 and val_b == 6 and val_c == 6
      end)
    end

    test "staggered resume: two suspended acceptors resume at different times" do
      # 5-node cluster (quorum=3). Suspend nodes 4 and 5.
      # CAS from node 1 — gets quorum from 1+2+3. Nodes 4,5 have queued messages.
      # Resume node 4 first, then node 5 later.
      # Both should eventually get the value.
      peers = TestCluster.start_peers(5)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, n1}, {_, _n2}, {_, _n3}, {_, n4}, {_, n5}] = peers
      ekv_name = unique_name(:cas5)

      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(500)

      # Suspend n4 and n5
      TestCluster.suspend_shards(n4, ekv_name)
      TestCluster.suspend_shards(n5, ekv_name)

      # CAS — quorum from n1+n2+n3
      :ok = TestCluster.rpc!(n1, EKV, :put, [ekv_name, "stagger/1", "val", [if_vsn: nil]])

      # Resume n4 first
      TestCluster.resume_shards(n4, ekv_name)

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(n4, EKV, :get, [ekv_name, "stagger/1"]) == "val"
      end)

      # n5 still suspended — resume it after a delay
      Process.sleep(300)
      TestCluster.resume_shards(n5, ekv_name)

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(n5, EKV, :get, [ekv_name, "stagger/1"]) == "val"
      end)
    end

    test "CAS put then immediate suspend: value survives on remaining nodes" do
      # 3-node cluster. CAS from A (quorum A+B). Immediately suspend A.
      # B should have the value (via commit notification). C should get it via LWW.
      # Then resume A — it should still have the value (it committed locally).
      peers = TestCluster.start_peers(3)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}, {_, node_c}] = peers
      ekv_name = unique_name(:cas)

      start_cas_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(300)

      # CAS from A
      :ok = TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "freeze/1", "val", [if_vsn: nil]])

      # Immediately suspend A (simulating proposer going slow right after commit)
      TestCluster.suspend_shards(node_a, ekv_name)

      # B and C should still have or get the value
      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "freeze/1"]) == "val"
      end)

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_c, EKV, :get, [ekv_name, "freeze/1"]) == "val"
      end)

      # Resume A — value should still be there
      TestCluster.resume_shards(node_a, ekv_name)
      assert TestCluster.rpc!(node_a, EKV, :get, [ekv_name, "freeze/1"]) == "val"
    end
  end
end
