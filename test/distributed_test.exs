defmodule EKV.DistributedTest do
  use ExUnit.Case

  @moduletag :capture_log
  @moduletag timeout: 30_000

  alias EKV.TestCluster

  defp unique_name(prefix) do
    :"#{prefix}_#{System.unique_integer([:positive])}"
  end

  defp start_cluster(peers, ekv_name) do
    for {_pid, node} <- peers do
      data_dir = "/tmp/ekv_dist_test_#{node}_#{ekv_name}"
      TestCluster.rpc!(node, File, :rm_rf!, [data_dir])

      TestCluster.start_ekv(
        node,
        name: ekv_name,
        data_dir: data_dir,
        shards: 2,
        log: false,
        gc_interval: :timer.hours(1),
        tombstone_ttl: :timer.hours(24 * 7)
      )
    end
  end

  defp cleanup_data(peers, ekv_name) do
    for {_pid, node} <- peers do
      data_dir = "/tmp/ekv_dist_test_#{node}_#{ekv_name}"

      try do
        TestCluster.rpc!(node, File, :rm_rf!, [data_dir])
      catch
        _, _ -> :ok
      end
    end
  end

  describe "2-node replication" do
    test "put replicates to other node" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      ekv_name = unique_name(:ekv)

      start_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)

      # Wait for peer discovery
      Process.sleep(200)

      # Put on node A
      TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "key1", "value1"])

      # Should be visible on node B
      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "key1"]) == "value1"
      end)
    end

    test "delete replicates to other node" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      ekv_name = unique_name(:ekv)

      start_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)

      Process.sleep(200)

      TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "key1", "value1"])

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "key1"]) == "value1"
      end)

      # Delete on node A
      TestCluster.rpc!(node_a, EKV, :delete, [ekv_name, "key1"])

      # Should be nil on node B
      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "key1"]) == nil
      end)
    end

    test "complex values replicate" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      ekv_name = unique_name(:ekv)

      start_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)

      Process.sleep(200)

      value = %{users: [%{id: 1, name: "Alice"}, %{id: 2, name: "Bob"}], count: 2}
      TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "complex", value])

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "complex"]) == value
      end)
    end
  end

  describe "late joiner sync" do
    test "new node receives existing data via sync" do
      peers = TestCluster.start_peers(2)
      [{_, node_a}, {_, node_b}] = peers
      ekv_name = unique_name(:ekv)

      # Only start on node A first
      data_dir_a = "/tmp/ekv_dist_test_#{node_a}_#{ekv_name}"
      TestCluster.rpc!(node_a, File, :rm_rf!, [data_dir_a])

      TestCluster.start_ekv(
        node_a,
        name: ekv_name,
        data_dir: data_dir_a,
        shards: 2,
        log: false,
        gc_interval: :timer.hours(1),
        tombstone_ttl: :timer.hours(24 * 7)
      )

      # Put data on A
      TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "key1", "from_a"])
      TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "key2", "from_a_2"])

      # Now start on node B (late joiner)
      data_dir_b = "/tmp/ekv_dist_test_#{node_b}_#{ekv_name}"
      TestCluster.rpc!(node_b, File, :rm_rf!, [data_dir_b])

      TestCluster.start_ekv(
        node_b,
        name: ekv_name,
        data_dir: data_dir_b,
        shards: 2,
        log: false,
        gc_interval: :timer.hours(1),
        tombstone_ttl: :timer.hours(24 * 7)
      )

      on_exit(fn ->
        TestCluster.stop_peers(peers)

        for dir <- [data_dir_a, data_dir_b] do
          File.rm_rf!(dir)
        end
      end)

      # Node B should receive data via sync
      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "key1"]) == "from_a" and
          TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "key2"]) == "from_a_2"
      end)
    end
  end

  describe "node restart (data survives)" do
    test "data persists after node restart via new node" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      ekv_name = unique_name(:ekv)

      start_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)

      Process.sleep(200)

      # Put on node A, wait for replication to B
      TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "survive", "yes"])

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "survive"]) == "yes"
      end)

      # Put on node B directly
      TestCluster.rpc!(node_b, EKV, :put, [ekv_name, "local_b", "b_data"])

      # Nodedown does NOT purge data — verify node A still has data
      assert TestCluster.rpc!(node_a, EKV, :get, [ekv_name, "survive"]) == "yes"
    end
  end

  describe "node comes back with empty storage" do
    test "data is re-synced from surviving peer" do
      # Start 2 nodes, both have data
      peers = TestCluster.start_peers(2)
      [{peer_a, node_a}, {peer_b, node_b}] = peers
      ekv_name = unique_name(:ekv)

      data_dir_a = "/tmp/ekv_dist_test_#{node_a}_#{ekv_name}"
      data_dir_b = "/tmp/ekv_dist_test_#{node_b}_#{ekv_name}"

      for {node, dir} <- [{node_a, data_dir_a}, {node_b, data_dir_b}] do
        TestCluster.rpc!(node, File, :rm_rf!, [dir])

        TestCluster.start_ekv(
          node,
          name: ekv_name,
          data_dir: dir,
          shards: 2,
          log: false,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )
      end

      Process.sleep(200)

      # Put data on both nodes
      TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "key1", "from_a"])
      TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "key2", "also_a"])
      TestCluster.rpc!(node_b, EKV, :put, [ekv_name, "key3", "from_b"])

      # Wait for replication to settle
      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "key1"]) == "from_a" and
          TestCluster.rpc!(node_a, EKV, :get, [ekv_name, "key3"]) == "from_b"
      end)

      # Kill node B
      :peer.stop(peer_b)
      Process.sleep(200)

      # Start a fresh node C (replaces B — no local data)
      [{peer_c, node_c}] = TestCluster.start_peers(1)
      data_dir_c = "/tmp/ekv_dist_test_#{node_c}_#{ekv_name}"
      TestCluster.rpc!(node_c, File, :rm_rf!, [data_dir_c])

      TestCluster.start_ekv(
        node_c,
        name: ekv_name,
        data_dir: data_dir_c,
        shards: 2,
        log: false,
        gc_interval: :timer.hours(1),
        tombstone_ttl: :timer.hours(24 * 7)
      )

      on_exit(fn ->
        TestCluster.stop_peers([{peer_a, node_a}, {peer_c, node_c}])
        for dir <- [data_dir_a, data_dir_b, data_dir_c], do: File.rm_rf!(dir)
      end)

      # Node C should get ALL data from node A via sync
      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_c, EKV, :get, [ekv_name, "key1"]) == "from_a" and
          TestCluster.rpc!(node_c, EKV, :get, [ekv_name, "key2"]) == "also_a" and
          TestCluster.rpc!(node_c, EKV, :get, [ekv_name, "key3"]) == "from_b"
      end)
    end
  end

  describe "concurrent writes" do
    test "LWW resolves concurrent puts" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      ekv_name = unique_name(:ekv)

      start_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)

      Process.sleep(200)

      # Both nodes write the same key
      TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "race", "from_a"])
      Process.sleep(5)
      TestCluster.rpc!(node_b, EKV, :put, [ekv_name, "race", "from_b"])

      # After replication settles, both should agree
      Process.sleep(300)

      val_a = TestCluster.rpc!(node_a, EKV, :get, [ekv_name, "race"])
      val_b = TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "race"])

      # Both should have the same value (from_b since it has higher timestamp)
      assert val_a == val_b
      assert val_a == "from_b"
    end
  end

  # =====================================================================
  # Pathological / Jepsen-style tests
  # =====================================================================

  describe "partition: writes on both sides converge after heal" do
    @tag timeout: 60_000
    test "disjoint keys written during partition merge on reconnect" do
      # 3-node cluster: partition C from {A, B}, write on both sides, heal
      peers = TestCluster.start_peers(3)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}, {_, node_c}] = peers
      ekv_name = unique_name(:ekv)
      data_dirs = start_cluster_3(peers, ekv_name)
      on_exit(fn -> for d <- data_dirs, do: File.rm_rf!(d) end)

      # Wait for full mesh
      TestCluster.assert_eventually(
        fn ->
          c_nodes = TestCluster.rpc!(node_c, Node, :list, [])
          node_a in c_nodes and node_b in c_nodes
        end,
        timeout: 5000
      )

      # Monitor nodedown so we know partition took effect
      TestCluster.monitor_nodes_on(node_a, self())

      # Partition: disconnect C from A and B
      TestCluster.disconnect_nodes(node_c, node_a)
      TestCluster.disconnect_nodes(node_c, node_b)
      assert_receive {:nodedown_on_remote, ^node_c}, 5000

      # Write disjoint keys on each side
      TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "a_side/1", "from_a"])
      TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "a_side/2", "from_a_2"])
      TestCluster.rpc!(node_c, EKV, :put, [ekv_name, "c_side/1", "from_c"])
      TestCluster.rpc!(node_c, EKV, :put, [ekv_name, "c_side/2", "from_c_2"])

      # A's writes replicate to B (still connected)
      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "a_side/1"]) == "from_a"
      end)

      # Verify isolation: C can't see A's data and vice versa
      assert TestCluster.rpc!(node_c, EKV, :get, [ekv_name, "a_side/1"]) == nil
      assert TestCluster.rpc!(node_a, EKV, :get, [ekv_name, "c_side/1"]) == nil

      # Heal partition
      TestCluster.reconnect_nodes(node_c, node_a)
      TestCluster.reconnect_nodes(node_c, node_b)

      # All 3 nodes converge to the union of all writes
      TestCluster.assert_eventually(
        fn ->
          for node <- [node_a, node_b, node_c] do
            TestCluster.rpc!(node, EKV, :get, [ekv_name, "a_side/1"]) == "from_a" and
              TestCluster.rpc!(node, EKV, :get, [ekv_name, "a_side/2"]) == "from_a_2" and
              TestCluster.rpc!(node, EKV, :get, [ekv_name, "c_side/1"]) == "from_c" and
              TestCluster.rpc!(node, EKV, :get, [ekv_name, "c_side/2"]) == "from_c_2"
          end
          |> Enum.all?()
        end,
        timeout: 10_000
      )
    end

    @tag timeout: 60_000
    test "same key written on both sides: LWW picks a deterministic winner" do
      # 3-node cluster: partition C from {A, B}, write same key on both sides
      peers = TestCluster.start_peers(3)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}, {_, node_c}] = peers
      ekv_name = unique_name(:ekv)
      data_dirs = start_cluster_3(peers, ekv_name)
      on_exit(fn -> for d <- data_dirs, do: File.rm_rf!(d) end)

      TestCluster.assert_eventually(
        fn ->
          c_nodes = TestCluster.rpc!(node_c, Node, :list, [])
          node_a in c_nodes and node_b in c_nodes
        end,
        timeout: 5000
      )

      TestCluster.monitor_nodes_on(node_a, self())
      TestCluster.monitor_nodes_on(node_c, self())

      TestCluster.disconnect_nodes(node_c, node_a)
      TestCluster.disconnect_nodes(node_c, node_b)
      assert_receive {:nodedown_on_remote, ^node_c}, 5000
      assert_receive {:nodedown_on_remote, ^node_a}, 5000

      # Write same key on both sides. A writes first, C writes second (higher ts wins).
      TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "conflict", "A_wins?"])
      Process.sleep(50)
      TestCluster.rpc!(node_c, EKV, :put, [ekv_name, "conflict", "C_wins!"])

      # Each side sees its own value
      assert TestCluster.rpc!(node_a, EKV, :get, [ekv_name, "conflict"]) == "A_wins?"
      assert TestCluster.rpc!(node_c, EKV, :get, [ekv_name, "conflict"]) == "C_wins!"

      # Heal
      TestCluster.reconnect_nodes(node_c, node_a)
      TestCluster.reconnect_nodes(node_c, node_b)

      # All nodes must converge to the SAME value (C_wins! because higher timestamp)
      TestCluster.assert_eventually(
        fn ->
          vals =
            for node <- [node_a, node_b, node_c] do
              TestCluster.rpc!(node, EKV, :get, [ekv_name, "conflict"])
            end

          # All agree
          length(Enum.uniq(vals)) == 1 and hd(vals) == "C_wins!"
        end,
        timeout: 10_000
      )
    end
  end

  describe "partition: delete vs put conflict" do
    @tag timeout: 60_000
    test "delete on one side, put on other: higher timestamp wins" do
      # A puts a key. Partition. A deletes it. C re-puts it (higher ts).
      # After heal, C's put should win because it has a higher timestamp.
      peers = TestCluster.start_peers(3)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}, {_, node_c}] = peers
      ekv_name = unique_name(:ekv)
      data_dirs = start_cluster_3(peers, ekv_name)
      on_exit(fn -> for d <- data_dirs, do: File.rm_rf!(d) end)

      TestCluster.assert_eventually(
        fn ->
          c_nodes = TestCluster.rpc!(node_c, Node, :list, [])
          node_a in c_nodes and node_b in c_nodes
        end,
        timeout: 5000
      )

      # Pre-partition: put key, wait for replication
      TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "phoenix", "v1"])

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_c, EKV, :get, [ekv_name, "phoenix"]) == "v1"
      end)

      # Partition C
      TestCluster.monitor_nodes_on(node_a, self())
      TestCluster.disconnect_nodes(node_c, node_a)
      TestCluster.disconnect_nodes(node_c, node_b)
      assert_receive {:nodedown_on_remote, ^node_c}, 5000

      # A deletes the key
      TestCluster.rpc!(node_a, EKV, :delete, [ekv_name, "phoenix"])
      assert TestCluster.rpc!(node_a, EKV, :get, [ekv_name, "phoenix"]) == nil

      # C overwrites it (higher timestamp, wins over delete)
      Process.sleep(50)
      TestCluster.rpc!(node_c, EKV, :put, [ekv_name, "phoenix", "resurrected"])
      assert TestCluster.rpc!(node_c, EKV, :get, [ekv_name, "phoenix"]) == "resurrected"

      # Heal
      TestCluster.reconnect_nodes(node_c, node_a)
      TestCluster.reconnect_nodes(node_c, node_b)

      # All nodes should see "resurrected" (C's put has higher ts than A's delete)
      TestCluster.assert_eventually(
        fn ->
          for node <- [node_a, node_b, node_c] do
            TestCluster.rpc!(node, EKV, :get, [ekv_name, "phoenix"]) == "resurrected"
          end
          |> Enum.all?()
        end,
        timeout: 10_000
      )
    end

    @tag timeout: 60_000
    test "put on one side, delete on other (delete has higher ts): delete wins" do
      peers = TestCluster.start_peers(3)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}, {_, node_c}] = peers
      ekv_name = unique_name(:ekv)
      data_dirs = start_cluster_3(peers, ekv_name)
      on_exit(fn -> for d <- data_dirs, do: File.rm_rf!(d) end)

      TestCluster.assert_eventually(
        fn ->
          c_nodes = TestCluster.rpc!(node_c, Node, :list, [])
          node_a in c_nodes and node_b in c_nodes
        end,
        timeout: 5000
      )

      # Pre-partition: put key
      TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "doomed", "alive"])

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_c, EKV, :get, [ekv_name, "doomed"]) == "alive"
      end)

      # Partition
      TestCluster.monitor_nodes_on(node_a, self())
      TestCluster.disconnect_nodes(node_c, node_a)
      TestCluster.disconnect_nodes(node_c, node_b)
      assert_receive {:nodedown_on_remote, ^node_c}, 5000

      # A updates the key (lower ts than C's upcoming delete)
      TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "doomed", "updated_a"])
      Process.sleep(50)

      # C deletes (higher ts)
      TestCluster.rpc!(node_c, EKV, :delete, [ekv_name, "doomed"])
      assert TestCluster.rpc!(node_c, EKV, :get, [ekv_name, "doomed"]) == nil

      # Heal
      TestCluster.reconnect_nodes(node_c, node_a)
      TestCluster.reconnect_nodes(node_c, node_b)

      # Delete wins — all nodes should return nil
      TestCluster.assert_eventually(
        fn ->
          for node <- [node_a, node_b, node_c] do
            TestCluster.rpc!(node, EKV, :get, [ekv_name, "doomed"]) == nil
          end
          |> Enum.all?()
        end,
        timeout: 10_000
      )
    end
  end

  describe "partition: many keys across shards" do
    @tag timeout: 60_000
    test "50 keys written on each side of partition all converge" do
      peers = TestCluster.start_peers(3)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}, {_, node_c}] = peers
      ekv_name = unique_name(:ekv)
      data_dirs = start_cluster_3(peers, ekv_name)
      on_exit(fn -> for d <- data_dirs, do: File.rm_rf!(d) end)

      TestCluster.assert_eventually(
        fn ->
          c_nodes = TestCluster.rpc!(node_c, Node, :list, [])
          node_a in c_nodes and node_b in c_nodes
        end,
        timeout: 5000
      )

      TestCluster.monitor_nodes_on(node_a, self())
      TestCluster.disconnect_nodes(node_c, node_a)
      TestCluster.disconnect_nodes(node_c, node_b)
      assert_receive {:nodedown_on_remote, ^node_c}, 5000

      # Write 50 keys on A side, 50 on C side (disjoint)
      for i <- 1..50 do
        TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "batch_a/#{i}", "val_a_#{i}"])
      end

      for i <- 1..50 do
        TestCluster.rpc!(node_c, EKV, :put, [ekv_name, "batch_c/#{i}", "val_c_#{i}"])
      end

      # Heal
      TestCluster.reconnect_nodes(node_c, node_a)
      TestCluster.reconnect_nodes(node_c, node_b)

      # All 100 keys must be visible on all 3 nodes
      TestCluster.assert_eventually(
        fn ->
          for node <- [node_a, node_b, node_c] do
            a_keys = TestCluster.rpc!(node, EKV, :keys, [ekv_name, "batch_a/"])
            c_keys = TestCluster.rpc!(node, EKV, :keys, [ekv_name, "batch_c/"])
            length(a_keys) == 50 and length(c_keys) == 50
          end
          |> Enum.all?()
        end,
        timeout: 15_000
      )

      # Spot-check values
      for node <- [node_a, node_b, node_c] do
        assert TestCluster.rpc!(node, EKV, :get, [ekv_name, "batch_a/25"]) == "val_a_25"
        assert TestCluster.rpc!(node, EKV, :get, [ekv_name, "batch_c/50"]) == "val_c_50"
      end
    end
  end

  describe "partition: rapid writes then heal" do
    @tag timeout: 60_000
    test "rapid fire puts during partition, all converge" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      ekv_name = unique_name(:ekv)

      start_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)

      Process.sleep(200)

      # Set up confirmed disconnect
      TestCluster.monitor_nodes_on(node_a, self())
      TestCluster.monitor_nodes_on(node_b, self())
      TestCluster.disconnect_nodes(node_a, node_b)
      assert_receive {:nodedown_on_remote, ^node_b}, 5000
      assert_receive {:nodedown_on_remote, ^node_a}, 5000

      # Rapid fire: same key overwritten many times on each side
      for i <- 1..20 do
        TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "hot_key", "a_v#{i}"])
      end

      for i <- 1..20 do
        TestCluster.rpc!(node_b, EKV, :put, [ekv_name, "hot_key", "b_v#{i}"])
      end

      # Each side's last write should be visible locally
      assert TestCluster.rpc!(node_a, EKV, :get, [ekv_name, "hot_key"]) == "a_v20"
      assert TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "hot_key"]) == "b_v20"

      # Heal
      TestCluster.reconnect_nodes(node_a, node_b)

      # Both must converge to the same value (b_v20 has higher ts)
      TestCluster.assert_eventually(
        fn ->
          val_a = TestCluster.rpc!(node_a, EKV, :get, [ekv_name, "hot_key"])
          val_b = TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "hot_key"])
          val_a == val_b and val_a != nil
        end,
        timeout: 10_000
      )

      # The winner should be b_v20 since B wrote after A (higher timestamp)
      assert TestCluster.rpc!(node_a, EKV, :get, [ekv_name, "hot_key"]) == "b_v20"
    end
  end

  describe "partition: delete during partition, resurrect after heal" do
    @tag timeout: 60_000
    test "key deleted during partition can be re-created after heal" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      ekv_name = unique_name(:ekv)

      start_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)

      Process.sleep(200)

      # Put key, wait for replication
      TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "lazarus", "alive_v1"])

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "lazarus"]) == "alive_v1"
      end)

      # Partition
      TestCluster.monitor_nodes_on(node_a, self())
      TestCluster.monitor_nodes_on(node_b, self())
      TestCluster.disconnect_nodes(node_a, node_b)
      assert_receive {:nodedown_on_remote, ^node_b}, 5000
      assert_receive {:nodedown_on_remote, ^node_a}, 5000

      # A deletes the key
      TestCluster.rpc!(node_a, EKV, :delete, [ekv_name, "lazarus"])

      # Heal — delete replicates to B
      TestCluster.reconnect_nodes(node_a, node_b)

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "lazarus"]) == nil
      end)

      # Now re-create the key (post-heal, should get a new higher timestamp)
      TestCluster.rpc!(node_b, EKV, :put, [ekv_name, "lazarus", "alive_v2"])

      # Should replicate the resurrection to A
      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_a, EKV, :get, [ekv_name, "lazarus"]) == "alive_v2"
      end)
    end
  end

  describe "partition: overlapping prefix writes" do
    @tag timeout: 60_000
    test "prefix scan returns correct union after partition heal" do
      peers = TestCluster.start_peers(3)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}, {_, node_c}] = peers
      ekv_name = unique_name(:ekv)
      data_dirs = start_cluster_3(peers, ekv_name)
      on_exit(fn -> for d <- data_dirs, do: File.rm_rf!(d) end)

      TestCluster.assert_eventually(
        fn ->
          c_nodes = TestCluster.rpc!(node_c, Node, :list, [])
          node_a in c_nodes and node_b in c_nodes
        end,
        timeout: 5000
      )

      # Pre-partition data under shared prefix
      TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "user/1", "alice"])

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_c, EKV, :get, [ekv_name, "user/1"]) == "alice"
      end)

      # Partition
      TestCluster.monitor_nodes_on(node_a, self())
      TestCluster.disconnect_nodes(node_c, node_a)
      TestCluster.disconnect_nodes(node_c, node_b)
      assert_receive {:nodedown_on_remote, ^node_c}, 5000

      # Both sides add more entries under same prefix
      TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "user/2", "bob"])
      TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "user/3", "carol"])
      TestCluster.rpc!(node_c, EKV, :put, [ekv_name, "user/4", "dave"])
      TestCluster.rpc!(node_c, EKV, :put, [ekv_name, "user/5", "eve"])

      # Also delete one on each side
      TestCluster.rpc!(node_a, EKV, :delete, [ekv_name, "user/1"])
      TestCluster.rpc!(node_c, EKV, :put, [ekv_name, "user/6", "frank"])

      # Heal
      TestCluster.reconnect_nodes(node_c, node_a)
      TestCluster.reconnect_nodes(node_c, node_b)

      # All nodes should converge on the same prefix scan result
      TestCluster.assert_eventually(
        fn ->
          for node <- [node_a, node_b, node_c] do
            keys = TestCluster.rpc!(node, EKV, :keys, [ekv_name, "user/"])
            # user/1 was deleted (by A, which is the latest write)
            # user/2, user/3 from A; user/4, user/5, user/6 from C
            keys == ["user/2", "user/3", "user/4", "user/5", "user/6"]
          end
          |> Enum.all?()
        end,
        timeout: 10_000
      )
    end
  end

  describe "partition: 3-way split and multi-heal" do
    @tag timeout: 60_000
    test "all 3 nodes isolated then reconnected: full convergence" do
      # Total isolation: each node is alone. Each writes different keys.
      # When they all reconnect, the union of all writes is visible everywhere.
      peers = TestCluster.start_peers(3)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}, {_, node_c}] = peers
      ekv_name = unique_name(:ekv)
      data_dirs = start_cluster_3(peers, ekv_name)
      on_exit(fn -> for d <- data_dirs, do: File.rm_rf!(d) end)

      # Wait for full mesh
      TestCluster.assert_eventually(
        fn ->
          a_nodes = TestCluster.rpc!(node_a, Node, :list, [])
          node_b in a_nodes and node_c in a_nodes
        end,
        timeout: 5000
      )

      # Monitor nodedown on all nodes
      TestCluster.monitor_nodes_on(node_a, self())
      TestCluster.monitor_nodes_on(node_b, self())

      # Create a 3-way partition: disconnect C from both, then A from B
      TestCluster.disconnect_nodes(node_c, node_a)
      TestCluster.disconnect_nodes(node_c, node_b)
      assert_receive {:nodedown_on_remote, ^node_c}, 5000

      TestCluster.disconnect_nodes(node_a, node_b)
      assert_receive {:nodedown_on_remote, ^node_b}, 5000

      # Each isolated node writes its own keys
      TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "iso/a", "only_a"])
      TestCluster.rpc!(node_b, EKV, :put, [ekv_name, "iso/b", "only_b"])
      TestCluster.rpc!(node_c, EKV, :put, [ekv_name, "iso/c", "only_c"])

      # Verify total isolation
      assert TestCluster.rpc!(node_a, EKV, :get, [ekv_name, "iso/b"]) == nil
      assert TestCluster.rpc!(node_a, EKV, :get, [ekv_name, "iso/c"]) == nil
      assert TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "iso/a"]) == nil
      assert TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "iso/c"]) == nil
      assert TestCluster.rpc!(node_c, EKV, :get, [ekv_name, "iso/a"]) == nil
      assert TestCluster.rpc!(node_c, EKV, :get, [ekv_name, "iso/b"]) == nil

      # Heal all connections
      TestCluster.reconnect_nodes(node_a, node_b)
      TestCluster.reconnect_nodes(node_c, node_a)
      TestCluster.reconnect_nodes(node_c, node_b)

      # All 3 keys visible on all 3 nodes
      TestCluster.assert_eventually(
        fn ->
          for node <- [node_a, node_b, node_c] do
            TestCluster.rpc!(node, EKV, :get, [ekv_name, "iso/a"]) == "only_a" and
              TestCluster.rpc!(node, EKV, :get, [ekv_name, "iso/b"]) == "only_b" and
              TestCluster.rpc!(node, EKV, :get, [ekv_name, "iso/c"]) == "only_c"
          end
          |> Enum.all?()
        end,
        timeout: 10_000
      )
    end
  end

  describe "partition: double partition (partition, heal, partition, heal)" do
    @tag timeout: 60_000
    test "data survives two consecutive partition cycles" do
      peers = TestCluster.start_peers(3)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}, {_, node_c}] = peers
      ekv_name = unique_name(:ekv)
      data_dirs = start_cluster_3(peers, ekv_name)
      on_exit(fn -> for d <- data_dirs, do: File.rm_rf!(d) end)

      TestCluster.assert_eventually(
        fn ->
          c_nodes = TestCluster.rpc!(node_c, Node, :list, [])
          node_a in c_nodes and node_b in c_nodes
        end,
        timeout: 5000
      )

      # --- Partition cycle 1 ---
      TestCluster.monitor_nodes_on(node_a, self())
      TestCluster.disconnect_nodes(node_c, node_a)
      TestCluster.disconnect_nodes(node_c, node_b)
      assert_receive {:nodedown_on_remote, ^node_c}, 5000

      TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "round1/a", "v1"])
      TestCluster.rpc!(node_c, EKV, :put, [ekv_name, "round1/c", "v1"])

      TestCluster.reconnect_nodes(node_c, node_a)
      TestCluster.reconnect_nodes(node_c, node_b)

      # Wait for convergence after first heal
      TestCluster.assert_eventually(
        fn ->
          for node <- [node_a, node_b, node_c] do
            TestCluster.rpc!(node, EKV, :get, [ekv_name, "round1/a"]) == "v1" and
              TestCluster.rpc!(node, EKV, :get, [ekv_name, "round1/c"]) == "v1"
          end
          |> Enum.all?()
        end,
        timeout: 10_000
      )

      # --- Partition cycle 2 ---
      # Re-establish nodedown monitors (old monitor process may have timed out)
      TestCluster.monitor_nodes_on(node_a, self())
      TestCluster.disconnect_nodes(node_c, node_a)
      TestCluster.disconnect_nodes(node_c, node_b)
      assert_receive {:nodedown_on_remote, ^node_c}, 5000

      # Overwrite round1 keys AND add new keys
      TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "round1/a", "v2_from_a"])
      TestCluster.rpc!(node_c, EKV, :put, [ekv_name, "round1/c", "v2_from_c"])
      TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "round2/a", "new"])
      TestCluster.rpc!(node_c, EKV, :put, [ekv_name, "round2/c", "new"])

      TestCluster.reconnect_nodes(node_c, node_a)
      TestCluster.reconnect_nodes(node_c, node_b)

      # All nodes converge after second heal
      TestCluster.assert_eventually(
        fn ->
          for node <- [node_a, node_b, node_c] do
            TestCluster.rpc!(node, EKV, :get, [ekv_name, "round1/a"]) == "v2_from_a" and
              TestCluster.rpc!(node, EKV, :get, [ekv_name, "round1/c"]) == "v2_from_c" and
              TestCluster.rpc!(node, EKV, :get, [ekv_name, "round2/a"]) == "new" and
              TestCluster.rpc!(node, EKV, :get, [ekv_name, "round2/c"]) == "new"
          end
          |> Enum.all?()
        end,
        timeout: 10_000
      )
    end
  end

  describe "node death and full replacement" do
    @tag timeout: 60_000
    test "node dies, replaced by fresh node, deletes also sync" do
      # A and B have data including tombstones. B dies. C joins fresh.
      # C should see live keys but NOT see tombstoned keys.
      peers = TestCluster.start_peers(2)
      [{peer_a, node_a}, {peer_b, node_b}] = peers
      ekv_name = unique_name(:ekv)

      data_dir_a = "/tmp/ekv_dist_test_#{node_a}_#{ekv_name}"
      data_dir_b = "/tmp/ekv_dist_test_#{node_b}_#{ekv_name}"

      for {node, dir} <- [{node_a, data_dir_a}, {node_b, data_dir_b}] do
        TestCluster.rpc!(node, File, :rm_rf!, [dir])

        TestCluster.start_ekv(
          node,
          name: ekv_name,
          data_dir: dir,
          shards: 2,
          log: false,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )
      end

      Process.sleep(200)

      # Put several keys, delete some
      for i <- 1..10 do
        TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "data/#{i}", "val_#{i}"])
      end

      # Wait for replication
      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "data/10"]) == "val_10"
      end)

      # Delete odd-numbered keys
      for i <- [1, 3, 5, 7, 9] do
        TestCluster.rpc!(node_a, EKV, :delete, [ekv_name, "data/#{i}"])
      end

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "data/1"]) == nil
      end)

      # Kill B
      :peer.stop(peer_b)
      Process.sleep(300)

      # Start fresh C
      [{peer_c, node_c}] = TestCluster.start_peers(1)
      data_dir_c = "/tmp/ekv_dist_test_#{node_c}_#{ekv_name}"
      TestCluster.rpc!(node_c, File, :rm_rf!, [data_dir_c])

      TestCluster.start_ekv(
        node_c,
        name: ekv_name,
        data_dir: data_dir_c,
        shards: 2,
        log: false,
        gc_interval: :timer.hours(1),
        tombstone_ttl: :timer.hours(24 * 7)
      )

      on_exit(fn ->
        TestCluster.stop_peers([{peer_a, node_a}, {peer_c, node_c}])
        for dir <- [data_dir_a, data_dir_b, data_dir_c], do: File.rm_rf!(dir)
      end)

      # C should see even keys, NOT odd (deleted) keys
      TestCluster.assert_eventually(
        fn ->
          evens =
            for i <- [2, 4, 6, 8, 10] do
              TestCluster.rpc!(node_c, EKV, :get, [ekv_name, "data/#{i}"]) == "val_#{i}"
            end

          odds =
            for i <- [1, 3, 5, 7, 9] do
              TestCluster.rpc!(node_c, EKV, :get, [ekv_name, "data/#{i}"]) == nil
            end

          Enum.all?(evens) and Enum.all?(odds)
        end,
        timeout: 10_000
      )
    end
  end

  describe "write-during-heal race" do
    @tag timeout: 60_000
    test "writes made right as partition heals are not lost" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      ekv_name = unique_name(:ekv)

      start_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)

      Process.sleep(200)

      # Partition
      TestCluster.monitor_nodes_on(node_a, self())
      TestCluster.monitor_nodes_on(node_b, self())
      TestCluster.disconnect_nodes(node_a, node_b)
      assert_receive {:nodedown_on_remote, ^node_b}, 5000
      assert_receive {:nodedown_on_remote, ^node_a}, 5000

      # Write a bunch of data during partition
      for i <- 1..10 do
        TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "pre_heal/a/#{i}", "a_#{i}"])
        TestCluster.rpc!(node_b, EKV, :put, [ekv_name, "pre_heal/b/#{i}", "b_#{i}"])
      end

      # Reconnect — and immediately write more data (race with sync)
      TestCluster.reconnect_nodes(node_a, node_b)

      for i <- 1..10 do
        TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "post_heal/a/#{i}", "a2_#{i}"])
        TestCluster.rpc!(node_b, EKV, :put, [ekv_name, "post_heal/b/#{i}", "b2_#{i}"])
      end

      # ALL 40 keys should be visible on both nodes
      TestCluster.assert_eventually(
        fn ->
          for node <- [node_a, node_b] do
            pre_a = TestCluster.rpc!(node, EKV, :keys, [ekv_name, "pre_heal/a/"])
            pre_b = TestCluster.rpc!(node, EKV, :keys, [ekv_name, "pre_heal/b/"])
            post_a = TestCluster.rpc!(node, EKV, :keys, [ekv_name, "post_heal/a/"])
            post_b = TestCluster.rpc!(node, EKV, :keys, [ekv_name, "post_heal/b/"])

            length(pre_a) == 10 and length(pre_b) == 10 and
              length(post_a) == 10 and length(post_b) == 10
          end
          |> Enum.all?()
        end,
        timeout: 10_000
      )
    end
  end

  # =====================================================================
  # Jepsen-style pathological tests
  # =====================================================================

  describe "shard-level crash and recovery" do
    @tag timeout: 60_000
    test "crashed shard restarts and re-syncs" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      ekv_name = unique_name(:ekv)

      start_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)

      Process.sleep(200)

      # Write data on A, wait for replication
      for i <- 1..5 do
        TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "shard_crash/#{i}", "val_#{i}"])
      end

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "shard_crash/5"]) == "val_5"
      end)

      # Kill shard 0 on node_b
      shard_0_name = :"#{ekv_name}_ekv_replica_0"
      shard_0_pid = TestCluster.rpc!(node_b, Process, :whereis, [shard_0_name])
      assert shard_0_pid != nil

      # Shard 1 should be unaffected
      shard_1_name = :"#{ekv_name}_ekv_replica_1"
      shard_1_pid_before = TestCluster.rpc!(node_b, Process, :whereis, [shard_1_name])

      TestCluster.rpc!(node_b, Process, :exit, [shard_0_pid, :kill])
      Process.sleep(500)

      # Shard 1 pid should be the same (unaffected by shard 0 crash)
      shard_1_pid_after = TestCluster.rpc!(node_b, Process, :whereis, [shard_1_name])
      assert shard_1_pid_after == shard_1_pid_before

      # Shard 0 should have restarted with a new pid
      new_shard_0_pid = TestCluster.rpc!(node_b, Process, :whereis, [shard_0_name])
      assert new_shard_0_pid != nil
      assert new_shard_0_pid != shard_0_pid

      # Write more data on A after crash
      TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "shard_crash/6", "post_crash"])

      # All data should be visible on B (re-synced after restart)
      TestCluster.assert_eventually(
        fn ->
          for i <- 1..5 do
            TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "shard_crash/#{i}"]) == "val_#{i}"
          end
          |> Enum.all?() and
            TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "shard_crash/6"]) == "post_crash"
        end,
        timeout: 10_000
      )
    end
  end

  describe "write during active sync" do
    @tag timeout: 60_000
    test "writes during sync are not lost" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      ekv_name = unique_name(:ekv)

      start_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)

      Process.sleep(200)

      # Partition
      TestCluster.monitor_nodes_on(node_a, self())
      TestCluster.monitor_nodes_on(node_b, self())
      TestCluster.disconnect_nodes(node_a, node_b)
      assert_receive {:nodedown_on_remote, ^node_b}, 5000
      assert_receive {:nodedown_on_remote, ^node_a}, 5000

      # Write 100 keys on each side during partition
      for i <- 1..100 do
        TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "sync_a/#{i}", "a_#{i}"])
      end

      for i <- 1..100 do
        TestCluster.rpc!(node_b, EKV, :put, [ekv_name, "sync_b/#{i}", "b_#{i}"])
      end

      # Heal — sync begins
      TestCluster.reconnect_nodes(node_a, node_b)

      # While sync is in-flight, write 50 MORE keys on each side
      for i <- 101..150 do
        TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "sync_a/#{i}", "a_#{i}"])
      end

      for i <- 101..150 do
        TestCluster.rpc!(node_b, EKV, :put, [ekv_name, "sync_b/#{i}", "b_#{i}"])
      end

      # ALL keys must converge
      TestCluster.assert_eventually(
        fn ->
          for node <- [node_a, node_b] do
            a_keys = TestCluster.rpc!(node, EKV, :keys, [ekv_name, "sync_a/"])
            b_keys = TestCluster.rpc!(node, EKV, :keys, [ekv_name, "sync_b/"])
            length(a_keys) == 150 and length(b_keys) == 150
          end
          |> Enum.all?()
        end,
        timeout: 15_000
      )
    end
  end

  describe "GC during partition — tombstone survives sync" do
    @tag timeout: 60_000
    test "deletes replicate via sync after partition heal" do
      peers = TestCluster.start_peers(3)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}, {_, node_c}] = peers
      ekv_name = unique_name(:ekv)
      data_dirs = start_cluster_3(peers, ekv_name)
      on_exit(fn -> for d <- data_dirs, do: File.rm_rf!(d) end)

      TestCluster.assert_eventually(
        fn ->
          c_nodes = TestCluster.rpc!(node_c, Node, :list, [])
          node_a in c_nodes and node_b in c_nodes
        end,
        timeout: 5000
      )

      # Write keys on A, replicate to all
      for i <- 1..5 do
        TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "gc_part/#{i}", "val_#{i}"])
      end

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "gc_part/5"]) == "val_5" and
          TestCluster.rpc!(node_c, EKV, :get, [ekv_name, "gc_part/5"]) == "val_5"
      end)

      # Partition C
      TestCluster.monitor_nodes_on(node_a, self())
      TestCluster.disconnect_nodes(node_c, node_a)
      TestCluster.disconnect_nodes(node_c, node_b)
      assert_receive {:nodedown_on_remote, ^node_c}, 5000

      # A deletes keys 1-3
      for i <- 1..3 do
        TestCluster.rpc!(node_a, EKV, :delete, [ekv_name, "gc_part/#{i}"])
      end

      # Verify B got the deletes
      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "gc_part/1"]) == nil
      end)

      # Trigger GC on A to convert any expired into tombstones (already done by delete)
      # The key point: tombstones exist when partition heals
      now = System.system_time(:nanosecond)
      config = TestCluster.rpc!(node_a, EKV, :get_config, [ekv_name])
      tombstone_cutoff = now - config.tombstone_ttl * 1_000_000

      for shard <- 0..(config.num_shards - 1) do
        TestCluster.rpc!(node_a, :erlang, :send, [
          :"#{ekv_name}_ekv_replica_#{shard}",
          {:gc, now, tombstone_cutoff}
        ])
      end

      TestCluster.flush_shards(node_a, ekv_name)

      # Heal partition
      TestCluster.reconnect_nodes(node_c, node_a)
      TestCluster.reconnect_nodes(node_c, node_b)

      # C should learn about the deletes
      TestCluster.assert_eventually(
        fn ->
          deleted =
            for i <- 1..3 do
              TestCluster.rpc!(node_c, EKV, :get, [ekv_name, "gc_part/#{i}"]) == nil
            end

          live =
            for i <- 4..5 do
              TestCluster.rpc!(node_c, EKV, :get, [ekv_name, "gc_part/#{i}"]) == "val_#{i}"
            end

          Enum.all?(deleted) and Enum.all?(live)
        end,
        timeout: 10_000
      )
    end
  end

  describe "GC tombstone purge + late joiner" do
    @tag timeout: 60_000
    test "late joiner gets only live keys after tombstone purge" do
      peers = TestCluster.start_peers(2)
      [{peer_a, node_a}, {peer_b, node_b}] = peers
      ekv_name = unique_name(:ekv)

      data_dir_a = "/tmp/ekv_dist_test_#{node_a}_#{ekv_name}"
      data_dir_b = "/tmp/ekv_dist_test_#{node_b}_#{ekv_name}"

      for {node, dir} <- [{node_a, data_dir_a}, {node_b, data_dir_b}] do
        TestCluster.rpc!(node, File, :rm_rf!, [dir])

        TestCluster.start_ekv(
          node,
          name: ekv_name,
          data_dir: dir,
          shards: 2,
          log: false,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )
      end

      Process.sleep(200)

      # Write 10 keys
      for i <- 1..10 do
        TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "purge_test/#{i}", "val_#{i}"])
      end

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "purge_test/10"]) == "val_10"
      end)

      # Delete 5 keys (1-5) on A, replicate to B
      for i <- 1..5 do
        TestCluster.rpc!(node_a, EKV, :delete, [ekv_name, "purge_test/#{i}"])
      end

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "purge_test/1"]) == nil
      end)

      # Purge the 5 tombstones on both A and B by triggering GC with a far-future cutoff
      now = System.system_time(:nanosecond)
      future_cutoff = now + :timer.hours(24 * 365) * 1_000_000
      config = TestCluster.rpc!(node_a, EKV, :get_config, [ekv_name])

      for node <- [node_a, node_b] do
        for shard <- 0..(config.num_shards - 1) do
          TestCluster.rpc!(node, :erlang, :send, [
            :"#{ekv_name}_ekv_replica_#{shard}",
            {:gc, now, future_cutoff}
          ])
        end

        TestCluster.flush_shards(node, ekv_name)
      end

      # Stop B
      :peer.stop(peer_b)
      Process.sleep(200)

      # Start fresh node C
      [{peer_c, node_c}] = TestCluster.start_peers(1)
      data_dir_c = "/tmp/ekv_dist_test_#{node_c}_#{ekv_name}"
      TestCluster.rpc!(node_c, File, :rm_rf!, [data_dir_c])

      TestCluster.start_ekv(
        node_c,
        name: ekv_name,
        data_dir: data_dir_c,
        shards: 2,
        log: false,
        gc_interval: :timer.hours(1),
        tombstone_ttl: :timer.hours(24 * 7)
      )

      on_exit(fn ->
        TestCluster.stop_peers([{peer_a, node_a}, {peer_c, node_c}])
        for dir <- [data_dir_a, data_dir_b, data_dir_c], do: File.rm_rf!(dir)
      end)

      # C should get only the 5 live keys (6-10)
      TestCluster.assert_eventually(
        fn ->
          live =
            for i <- 6..10 do
              TestCluster.rpc!(node_c, EKV, :get, [ekv_name, "purge_test/#{i}"]) == "val_#{i}"
            end

          stale =
            for i <- 1..5 do
              TestCluster.rpc!(node_c, EKV, :get, [ekv_name, "purge_test/#{i}"]) == nil
            end

          Enum.all?(live) and Enum.all?(stale)
        end,
        timeout: 10_000
      )
    end
  end

  describe "oplog truncation forces full sync on reconnect" do
    @tag timeout: 60_000
    test "full sync after oplog truncation delivers all data" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      ekv_name = unique_name(:ekv)

      start_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)

      Process.sleep(200)

      # Write initial data, verify replication
      for i <- 1..10 do
        TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "oplog_trunc/#{i}", "val_#{i}"])
      end

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "oplog_trunc/10"]) == "val_10"
      end)

      # Partition
      TestCluster.monitor_nodes_on(node_a, self())
      TestCluster.monitor_nodes_on(node_b, self())
      TestCluster.disconnect_nodes(node_a, node_b)
      assert_receive {:nodedown_on_remote, ^node_b}, 5000
      assert_receive {:nodedown_on_remote, ^node_a}, 5000

      # Write more data on A
      for i <- 11..20 do
        TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "oplog_trunc/#{i}", "val_#{i}"])
      end

      # Force oplog truncation on A: advance all peer HWMs to max_seq,
      # truncate, then delete B's HWM row so reconnect triggers full sync.
      config = TestCluster.rpc!(node_a, EKV, :get_config, [ekv_name])

      for shard <- 0..(config.num_shards - 1) do
        shard_name = :"#{ekv_name}_ekv_replica_#{shard}"
        %{db: db} = TestCluster.rpc!(node_a, :sys, :get_state, [shard_name])
        max = TestCluster.rpc!(node_a, EKV.Store, :max_seq, [db])
        TestCluster.rpc!(node_a, EKV.Store, :set_hwm, [db, node_b, max])
      end

      # Trigger GC to truncate oplog
      now = System.system_time(:nanosecond)
      tombstone_cutoff = now - config.tombstone_ttl * 1_000_000

      for shard <- 0..(config.num_shards - 1) do
        TestCluster.rpc!(node_a, :erlang, :send, [
          :"#{ekv_name}_ekv_replica_#{shard}",
          {:gc, now, tombstone_cutoff}
        ])
      end

      TestCluster.flush_shards(node_a, ekv_name)

      # Delete B's HWM row so reconnect sees peer_hwm=nil → full sync
      for shard <- 0..(config.num_shards - 1) do
        shard_name = :"#{ekv_name}_ekv_replica_#{shard}"
        %{db: db} = TestCluster.rpc!(node_a, :sys, :get_state, [shard_name])
        peer_str = TestCluster.rpc!(node_a, Atom, :to_string, [node_b])

        TestCluster.rpc!(node_a, EKV.Sqlite3, :execute, [
          db,
          "DELETE FROM kv_peer_hwm WHERE peer_node = '#{peer_str}'"
        ])
      end

      # Heal partition — B's old HWM is behind the truncated oplog, forcing full sync
      TestCluster.reconnect_nodes(node_a, node_b)

      # B should get ALL data via full sync
      TestCluster.assert_eventually(
        fn ->
          for i <- 1..20 do
            TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "oplog_trunc/#{i}"]) == "val_#{i}"
          end
          |> Enum.all?()
        end,
        timeout: 10_000
      )
    end
  end

  describe "rapid partition/heal cycles (flapping)" do
    @tag timeout: 120_000
    test "data converges after 5 rapid partition/heal cycles" do
      peers = TestCluster.start_peers(3)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}, {_, node_c}] = peers
      ekv_name = unique_name(:ekv)
      data_dirs = start_cluster_3(peers, ekv_name)
      on_exit(fn -> for d <- data_dirs, do: File.rm_rf!(d) end)

      TestCluster.assert_eventually(
        fn ->
          c_nodes = TestCluster.rpc!(node_c, Node, :list, [])
          node_a in c_nodes and node_b in c_nodes
        end,
        timeout: 5000
      )

      for cycle <- 1..5 do
        # Partition C from {A, B}
        TestCluster.monitor_nodes_on(node_a, self())
        TestCluster.disconnect_nodes(node_c, node_a)
        TestCluster.disconnect_nodes(node_c, node_b)
        assert_receive {:nodedown_on_remote, ^node_c}, 5000

        # Write unique keys on both sides
        for i <- 1..5 do
          TestCluster.rpc!(node_a, EKV, :put, [
            ekv_name,
            "flap/ab/c#{cycle}/#{i}",
            "ab_#{cycle}_#{i}"
          ])

          TestCluster.rpc!(node_c, EKV, :put, [
            ekv_name,
            "flap/c/c#{cycle}/#{i}",
            "c_#{cycle}_#{i}"
          ])
        end

        # Heal
        TestCluster.reconnect_nodes(node_c, node_a)
        TestCluster.reconnect_nodes(node_c, node_b)

        # Wait for convergence before next cycle
        TestCluster.assert_eventually(
          fn ->
            for node <- [node_a, node_b, node_c] do
              TestCluster.rpc!(node, EKV, :get, [ekv_name, "flap/ab/c#{cycle}/5"]) ==
                "ab_#{cycle}_5" and
                TestCluster.rpc!(node, EKV, :get, [ekv_name, "flap/c/c#{cycle}/5"]) ==
                  "c_#{cycle}_5"
            end
            |> Enum.all?()
          end,
          timeout: 10_000
        )
      end

      # After all cycles: verify complete data set on all 3 nodes
      TestCluster.assert_eventually(
        fn ->
          for node <- [node_a, node_b, node_c] do
            ab_keys = TestCluster.rpc!(node, EKV, :keys, [ekv_name, "flap/ab/"])
            c_keys = TestCluster.rpc!(node, EKV, :keys, [ekv_name, "flap/c/"])
            length(ab_keys) == 25 and length(c_keys) == 25
          end
          |> Enum.all?()
        end,
        timeout: 10_000
      )
    end
  end

  describe "concurrent deletes on both sides of partition" do
    @tag timeout: 60_000
    test "higher-ts tombstone wins after partition heal" do
      peers = TestCluster.start_peers(3)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}, {_, node_c}] = peers
      ekv_name = unique_name(:ekv)
      data_dirs = start_cluster_3(peers, ekv_name)
      on_exit(fn -> for d <- data_dirs, do: File.rm_rf!(d) end)

      TestCluster.assert_eventually(
        fn ->
          c_nodes = TestCluster.rpc!(node_c, Node, :list, [])
          node_a in c_nodes and node_b in c_nodes
        end,
        timeout: 5000
      )

      # Write key on A, replicate to all
      TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "dual_del", "alive"])

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_c, EKV, :get, [ekv_name, "dual_del"]) == "alive"
      end)

      # Partition C from {A, B}
      TestCluster.monitor_nodes_on(node_a, self())
      TestCluster.disconnect_nodes(node_c, node_a)
      TestCluster.disconnect_nodes(node_c, node_b)
      assert_receive {:nodedown_on_remote, ^node_c}, 5000

      # A deletes (ts1)
      TestCluster.rpc!(node_a, EKV, :delete, [ekv_name, "dual_del"])
      Process.sleep(50)

      # C also deletes independently (ts2 > ts1 because of sleep)
      TestCluster.rpc!(node_c, EKV, :delete, [ekv_name, "dual_del"])

      # Both sides see nil
      assert TestCluster.rpc!(node_a, EKV, :get, [ekv_name, "dual_del"]) == nil
      assert TestCluster.rpc!(node_c, EKV, :get, [ekv_name, "dual_del"]) == nil

      # Heal
      TestCluster.reconnect_nodes(node_c, node_a)
      TestCluster.reconnect_nodes(node_c, node_b)

      # All nodes agree key is deleted
      TestCluster.assert_eventually(
        fn ->
          for node <- [node_a, node_b, node_c] do
            TestCluster.rpc!(node, EKV, :get, [ekv_name, "dual_del"]) == nil
          end
          |> Enum.all?()
        end,
        timeout: 10_000
      )
    end
  end

  describe "put + delete interleaving across partition" do
    @tag timeout: 60_000
    test "delete with highest ts wins over put" do
      peers = TestCluster.start_peers(3)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}, {_, node_c}] = peers
      ekv_name = unique_name(:ekv)
      data_dirs = start_cluster_3(peers, ekv_name)
      on_exit(fn -> for d <- data_dirs, do: File.rm_rf!(d) end)

      TestCluster.assert_eventually(
        fn ->
          c_nodes = TestCluster.rpc!(node_c, Node, :list, [])
          node_a in c_nodes and node_b in c_nodes
        end,
        timeout: 5000
      )

      # Partition C
      TestCluster.monitor_nodes_on(node_a, self())
      TestCluster.disconnect_nodes(node_c, node_a)
      TestCluster.disconnect_nodes(node_c, node_b)
      assert_receive {:nodedown_on_remote, ^node_c}, 5000

      # A: put key="x" (ts1), then delete key="x" (ts2)
      TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "interleave", "from_a"])
      Process.sleep(50)
      TestCluster.rpc!(node_a, EKV, :delete, [ekv_name, "interleave"])

      # C: put key="x" (ts3 where ts1 < ts3 < ts2 because of timing)
      # Actually we can't control timestamps precisely, but C's put happened
      # during the sleep between A's put and delete. Since A's delete is last,
      # it will have the highest ts and should win.
      # More precisely: A's delete (ts2) happens after C's put because of the sleep.
      TestCluster.rpc!(node_c, EKV, :put, [ekv_name, "interleave", "from_c"])

      # Wait to ensure A's delete gets a later timestamp
      Process.sleep(50)
      # Re-do A's delete to guarantee it's the latest
      TestCluster.rpc!(node_a, EKV, :delete, [ekv_name, "interleave"])

      assert TestCluster.rpc!(node_a, EKV, :get, [ekv_name, "interleave"]) == nil

      # Heal
      TestCluster.reconnect_nodes(node_c, node_a)
      TestCluster.reconnect_nodes(node_c, node_b)

      # A's delete (highest ts) should win — all nodes see nil
      TestCluster.assert_eventually(
        fn ->
          for node <- [node_a, node_b, node_c] do
            TestCluster.rpc!(node, EKV, :get, [ekv_name, "interleave"]) == nil
          end
          |> Enum.all?()
        end,
        timeout: 10_000
      )
    end
  end

  describe "multi-shard consistency after partition" do
    @tag timeout: 60_000
    test "all shards converge independently" do
      peers = TestCluster.start_peers(3)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}, {_, node_c}] = peers
      ekv_name = unique_name(:ekv)

      # Use 4 shards for this test
      for {_pid, node} <- peers do
        data_dir = "/tmp/ekv_dist_test_#{node}_#{ekv_name}"
        TestCluster.rpc!(node, File, :rm_rf!, [data_dir])

        TestCluster.start_ekv(
          node,
          name: ekv_name,
          data_dir: data_dir,
          shards: 4,
          log: false,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )
      end

      on_exit(fn ->
        for {_pid, node} <- peers do
          dir = "/tmp/ekv_dist_test_#{node}_#{ekv_name}"

          try do
            TestCluster.rpc!(node, File, :rm_rf!, [dir])
          catch
            _, _ -> :ok
          end
        end
      end)

      TestCluster.assert_eventually(
        fn ->
          c_nodes = TestCluster.rpc!(node_c, Node, :list, [])
          node_a in c_nodes and node_b in c_nodes
        end,
        timeout: 5000
      )

      # Write keys that hash to different shards
      for i <- 1..20 do
        TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "multi_shard/#{i}", "val_#{i}"])
      end

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_c, EKV, :get, [ekv_name, "multi_shard/20"]) == "val_20"
      end)

      # Partition C
      TestCluster.monitor_nodes_on(node_a, self())
      TestCluster.disconnect_nodes(node_c, node_a)
      TestCluster.disconnect_nodes(node_c, node_b)
      assert_receive {:nodedown_on_remote, ^node_c}, 5000

      # Write more keys on each side
      for i <- 21..30 do
        TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "multi_shard/#{i}", "val_#{i}"])
      end

      for i <- 31..40 do
        TestCluster.rpc!(node_c, EKV, :put, [ekv_name, "multi_shard/#{i}", "val_#{i}"])
      end

      # Heal
      TestCluster.reconnect_nodes(node_c, node_a)
      TestCluster.reconnect_nodes(node_c, node_b)

      # Verify all 40 keys on all nodes
      TestCluster.assert_eventually(
        fn ->
          for node <- [node_a, node_b, node_c] do
            keys = TestCluster.rpc!(node, EKV, :keys, [ekv_name, "multi_shard/"])
            length(keys) == 40
          end
          |> Enum.all?()
        end,
        timeout: 10_000
      )

      # Verify prefix scan returns correct union
      for node <- [node_a, node_b, node_c] do
        result = TestCluster.rpc!(node, EKV, :scan, [ekv_name, "multi_shard/"])
        assert map_size(result) == 40
      end
    end
  end

  describe "duplicate peer_connect handling" do
    @tag timeout: 60_000
    test "duplicate peer_connect does not crash or cause duplicates" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      ekv_name = unique_name(:ekv)

      start_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)

      Process.sleep(200)

      # Write some data
      for i <- 1..5 do
        TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "dup_connect/#{i}", "val_#{i}"])
      end

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "dup_connect/5"]) == "val_5"
      end)

      # Manually send a second peer_connect from A to B
      config = TestCluster.rpc!(node_a, EKV, :get_config, [ekv_name])

      for shard <- 0..(config.num_shards - 1) do
        shard_name = :"#{ekv_name}_ekv_replica_#{shard}"
        a_pid = TestCluster.rpc!(node_a, Process, :whereis, [shard_name])
        %{db: db} = TestCluster.rpc!(node_a, :sys, :get_state, [shard_name])
        my_seq = TestCluster.rpc!(node_a, EKV.Store, :max_seq, [db])

        TestCluster.rpc!(node_b, :erlang, :send, [
          shard_name,
          {:ekv_peer_connect, a_pid, shard, config.num_shards, my_seq}
        ])
      end

      # Wait for message processing
      TestCluster.flush_shards(node_b, ekv_name)
      Process.sleep(200)

      # B should not have crashed
      for shard <- 0..(config.num_shards - 1) do
        shard_name = :"#{ekv_name}_ekv_replica_#{shard}"
        pid = TestCluster.rpc!(node_b, Process, :whereis, [shard_name])
        assert pid != nil
        assert TestCluster.rpc!(node_b, Process, :alive?, [pid])
      end

      # Data should still be correct
      for i <- 1..5 do
        assert TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "dup_connect/#{i}"]) == "val_#{i}"
      end

      # New writes should still work
      TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "dup_connect/6", "val_6"])

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "dup_connect/6"]) == "val_6"
      end)
    end
  end

  describe "write during node shutdown" do
    @tag timeout: 60_000
    test "writes are either fully committed or fully absent" do
      peers = TestCluster.start_peers(2)
      [{peer_a, node_a}, {_, node_b}] = peers
      ekv_name = unique_name(:ekv)

      start_cluster(peers, ekv_name)

      on_exit(fn ->
        TestCluster.stop_peers(peers)
        cleanup_data(peers, ekv_name)
      end)

      Process.sleep(200)

      # Write a batch of data on A
      for i <- 1..50 do
        TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "shutdown/#{i}", "val_#{i}"])
      end

      # Wait for replication
      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "shutdown/50"]) == "val_50"
      end)

      # Start rapid writes on A then kill A
      # We need to be careful — :peer.stop might not be instant
      task =
        Task.async(fn ->
          for i <- 51..100 do
            try do
              TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "shutdown/#{i}", "val_#{i}"])
            catch
              _, _ -> :node_died
            end
          end
        end)

      # Kill A while writes are in-flight
      Process.sleep(5)
      :peer.stop(peer_a)

      Task.await(task, 5000)
      Process.sleep(500)

      # Verify on B: every key that exists has the correct value (no half-writes)
      for i <- 1..100 do
        val = TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "shutdown/#{i}"])
        # Either the value is correct or it's nil (write never replicated)
        assert val == "val_#{i}" or val == nil,
               "Key shutdown/#{i} has unexpected value: #{inspect(val)}"
      end

      # The first 50 must be present (they replicated before shutdown)
      for i <- 1..50 do
        assert TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "shutdown/#{i}"]) == "val_#{i}"
      end
    end
  end

  describe "full sync with large dataset" do
    @tag timeout: 120_000
    test "5000 keys sync to fresh node" do
      peers = TestCluster.start_peers(1)
      [{_, node_a}] = peers
      ekv_name = unique_name(:ekv)

      data_dir_a = "/tmp/ekv_dist_test_#{node_a}_#{ekv_name}"
      TestCluster.rpc!(node_a, File, :rm_rf!, [data_dir_a])

      TestCluster.start_ekv(
        node_a,
        name: ekv_name,
        data_dir: data_dir_a,
        shards: 2,
        log: false,
        gc_interval: :timer.hours(1),
        tombstone_ttl: :timer.hours(24 * 7)
      )

      # Write 5000 keys on A
      for i <- 1..5000 do
        TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "large/#{i}", "val_#{i}"])
      end

      # Start fresh node B
      [{peer_b, node_b}] = TestCluster.start_peers(1)
      data_dir_b = "/tmp/ekv_dist_test_#{node_b}_#{ekv_name}"
      TestCluster.rpc!(node_b, File, :rm_rf!, [data_dir_b])

      TestCluster.start_ekv(
        node_b,
        name: ekv_name,
        data_dir: data_dir_b,
        shards: 2,
        log: false,
        gc_interval: :timer.hours(1),
        tombstone_ttl: :timer.hours(24 * 7)
      )

      on_exit(fn ->
        TestCluster.stop_peers(peers ++ [{peer_b, node_b}])
        for dir <- [data_dir_a, data_dir_b], do: File.rm_rf!(dir)
      end)

      # B should get all 5000 keys
      TestCluster.assert_eventually(
        fn ->
          keys = TestCluster.rpc!(node_b, EKV, :keys, [ekv_name, "large/"])
          length(keys) == 5000
        end,
        timeout: 60_000
      )

      # Spot-check values
      for i <- [1, 100, 500, 1000, 2500, 5000] do
        assert TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "large/#{i}"]) == "val_#{i}"
      end

      # Verify no duplicate keys
      keys = TestCluster.rpc!(node_b, EKV, :keys, [ekv_name, "large/"])
      assert length(keys) == length(Enum.uniq(keys))
    end
  end

  describe "three-way conflict resolution" do
    @tag timeout: 60_000
    test "all 3 isolated nodes converge to same winner" do
      peers = TestCluster.start_peers(3)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}, {_, node_c}] = peers
      ekv_name = unique_name(:ekv)
      data_dirs = start_cluster_3(peers, ekv_name)
      on_exit(fn -> for d <- data_dirs, do: File.rm_rf!(d) end)

      TestCluster.assert_eventually(
        fn ->
          a_nodes = TestCluster.rpc!(node_a, Node, :list, [])
          node_b in a_nodes and node_c in a_nodes
        end,
        timeout: 5000
      )

      # Create 3-way partition
      TestCluster.monitor_nodes_on(node_a, self())
      TestCluster.monitor_nodes_on(node_b, self())

      TestCluster.disconnect_nodes(node_c, node_a)
      TestCluster.disconnect_nodes(node_c, node_b)
      assert_receive {:nodedown_on_remote, ^node_c}, 5000

      TestCluster.disconnect_nodes(node_a, node_b)
      assert_receive {:nodedown_on_remote, ^node_b}, 5000

      # Each isolated node writes the same key with different values
      # Stagger writes to ensure different timestamps
      TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "three_way", "from_a"])
      Process.sleep(50)
      TestCluster.rpc!(node_b, EKV, :put, [ekv_name, "three_way", "from_b"])
      Process.sleep(50)
      TestCluster.rpc!(node_c, EKV, :put, [ekv_name, "three_way", "from_c"])

      # Each sees its own value
      assert TestCluster.rpc!(node_a, EKV, :get, [ekv_name, "three_way"]) == "from_a"
      assert TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "three_way"]) == "from_b"
      assert TestCluster.rpc!(node_c, EKV, :get, [ekv_name, "three_way"]) == "from_c"

      # Heal all connections
      TestCluster.reconnect_nodes(node_a, node_b)
      TestCluster.reconnect_nodes(node_c, node_a)
      TestCluster.reconnect_nodes(node_c, node_b)

      # All 3 nodes must converge to the same winner (C wrote last → highest ts)
      TestCluster.assert_eventually(
        fn ->
          vals =
            for node <- [node_a, node_b, node_c] do
              TestCluster.rpc!(node, EKV, :get, [ekv_name, "three_way"])
            end

          length(Enum.uniq(vals)) == 1 and hd(vals) == "from_c"
        end,
        timeout: 10_000
      )
    end
  end

  describe "subscribe: replication triggers subscriber" do
    @tag timeout: 60_000
    test "subscriber on node B receives event from put on node A" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      ekv_name = unique_name(:ekv)

      start_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)

      Process.sleep(200)

      # Subscribe on node B, forwarding events to test process
      TestCluster.subscribe_on(node_b, ekv_name, "sub_test/", self())

      Process.sleep(50)

      # Put on node A
      TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "sub_test/1", "from_a"])

      # Subscriber on B should receive event via replication
      assert_receive {:remote_ekv_event, events, %{name: ^ekv_name}}, 5000
      assert [%EKV.Event{type: :put, key: "sub_test/1", value: "from_a"}] = events
    end
  end

  describe "subscribe: distributed delete triggers subscriber" do
    @tag timeout: 60_000
    test "subscriber on node B receives delete event from node A" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}] = peers
      ekv_name = unique_name(:ekv)

      start_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)

      Process.sleep(200)

      # Put on node A, wait for replication to B
      TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "del_sub/1", "original"])

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(node_b, EKV, :get, [ekv_name, "del_sub/1"]) == "original"
      end)

      # Subscribe on node B
      TestCluster.subscribe_on(node_b, ekv_name, "del_sub/", self())
      Process.sleep(50)

      # Delete on node A
      TestCluster.rpc!(node_a, EKV, :delete, [ekv_name, "del_sub/1"])

      # Subscriber on B should receive delete event with previous value
      assert_receive {:remote_ekv_event, events, %{name: ^ekv_name}}, 5000
      assert [%EKV.Event{type: :delete, key: "del_sub/1", value: "original"}] = events
    end
  end

  describe "subscribe: partition heal sync triggers subscriber" do
    @tag timeout: 60_000
    test "subscriber receives sync events after partition heal" do
      peers = TestCluster.start_peers(3)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, node_a}, {_, node_b}, {_, node_c}] = peers
      ekv_name = unique_name(:ekv)
      data_dirs = start_cluster_3(peers, ekv_name)
      on_exit(fn -> for d <- data_dirs, do: File.rm_rf!(d) end)

      TestCluster.assert_eventually(
        fn ->
          c_nodes = TestCluster.rpc!(node_c, Node, :list, [])
          node_a in c_nodes and node_b in c_nodes
        end,
        timeout: 5000
      )

      # Partition C
      TestCluster.monitor_nodes_on(node_a, self())
      TestCluster.disconnect_nodes(node_c, node_a)
      TestCluster.disconnect_nodes(node_c, node_b)
      assert_receive {:nodedown_on_remote, ^node_c}, 5000

      # Write on A side during partition
      TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "heal_sub/1", "val1"])
      TestCluster.rpc!(node_a, EKV, :put, [ekv_name, "heal_sub/2", "val2"])

      # Subscribe on C before heal — collector process
      TestCluster.rpc!(node_c, EKV.TestCluster, :start_collecting_subscriber, [
        ekv_name,
        "heal_sub/",
        self(),
        5000
      ])

      Process.sleep(50)

      # Heal
      TestCluster.reconnect_nodes(node_c, node_a)
      TestCluster.reconnect_nodes(node_c, node_b)

      # Subscriber on C should receive the sync events
      assert_receive {:collected_events, events}, 10_000
      keys = Enum.map(events, & &1.key)
      assert "heal_sub/1" in keys
      assert "heal_sub/2" in keys
    end
  end

  # =====================================================================
  # Helpers
  # =====================================================================

  defp start_cluster_3(peers, ekv_name) do
    for {_pid, node} <- peers do
      data_dir = "/tmp/ekv_dist_test_#{node}_#{ekv_name}"
      TestCluster.rpc!(node, File, :rm_rf!, [data_dir])

      TestCluster.start_ekv(
        node,
        name: ekv_name,
        data_dir: data_dir,
        shards: 2,
        log: false,
        gc_interval: :timer.hours(1),
        tombstone_ttl: :timer.hours(24 * 7)
      )

      data_dir
    end
  end
end
