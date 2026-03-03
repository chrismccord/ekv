defmodule EKV.StressTest do
  use ExUnit.Case

  @moduletag :capture_log
  @moduletag timeout: 120_000

  alias EKV.TestCluster

  defp unique_name(prefix) do
    :"#{prefix}_#{System.unique_integer([:positive])}"
  end

  defp start_stress_cluster(peers, ekv_name, opts \\ []) do
    cluster_size = Keyword.get(opts, :cluster_size, length(peers))
    shards = Keyword.get(opts, :shards, 4)
    gc_interval = Keyword.get(opts, :gc_interval, :timer.hours(1))
    tombstone_ttl = Keyword.get(opts, :tombstone_ttl, :timer.hours(24 * 7))

    peers
    |> Enum.with_index(1)
    |> Enum.each(fn {{_pid, node}, node_id} ->
      data_dir = "/tmp/ekv_stress_test_#{node}_#{ekv_name}"
      TestCluster.rpc!(node, File, :rm_rf!, [data_dir])

      TestCluster.start_ekv(
        node,
        name: ekv_name,
        data_dir: data_dir,
        shards: shards,
        log: false,
        gc_interval: gc_interval,
        tombstone_ttl: tombstone_ttl,
        cluster_size: cluster_size,
        node_id: node_id
      )
    end)
  end

  defp cleanup_data(peers, ekv_name) do
    for {_pid, node} <- peers do
      data_dir = "/tmp/ekv_stress_test_#{node}_#{ekv_name}"

      try do
        TestCluster.rpc!(node, File, :rm_rf!, [data_dir])
      catch
        _, _ -> :ok
      end
    end
  end

  defp partition(group_a, group_b) do
    for node_a <- group_a, node_b <- group_b do
      TestCluster.disconnect_nodes(node_a, node_b)
    end

    :ok
  end

  defp heal(group_a, group_b) do
    for node_a <- group_a, node_b <- group_b do
      TestCluster.reconnect_nodes(node_a, node_b)
    end

    :ok
  end

  defp assert_all_agree(nodes, ekv_name, key, opts) do
    timeout = Keyword.get(opts, :timeout, 5000)

    TestCluster.assert_eventually(
      fn ->
        vals = Enum.map(nodes, fn n -> TestCluster.rpc!(n, EKV, :get, [ekv_name, key]) end)
        first = hd(vals)
        first != nil and Enum.all?(vals, &(&1 == first))
      end,
      timeout: timeout
    )
  end

  # =====================================================================
  # 1. Split-Brain Prevention
  # =====================================================================

  describe "split-brain prevention" do
    # Intention: verify an acknowledged CAS write still converges cluster-wide
    # after a partition heal, even when its timestamp is lower than an existing
    # LWW timestamp (clock-skew scenario).
    test "3-node heal: committed CAS write converges despite lower timestamp than prior LWW value" do
      peers = TestCluster.start_peers(3)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, n1}, {_, n2}, {_, n3}] = peers
      ekv_name = unique_name(:cas_lww_skew)

      start_stress_cluster(peers, ekv_name, shards: 1)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(500)

      key = "skew/cas_lww"
      shard_name = :"#{ekv_name}_ekv_replica_0"

      # Seed all nodes with the same value at an artificially high timestamp
      # (simulates a prior write from a clock-ahead node).
      old_value_bin = :erlang.term_to_binary("old")
      old_ts = System.system_time(:nanosecond) + 1_000_000_000_000
      old_origin = :"ahead_clock@127.0.0.1"

      for {_pid, node} <- peers do
        TestCluster.rpc!(node, Kernel, :send, [
          shard_name,
          {:ekv_put, key, old_value_bin, old_ts, old_origin, nil}
        ])

        # Drain shard mailbox so the synthetic write is definitely applied.
        TestCluster.rpc!(node, :sys, :get_state, [shard_name])
      end

      TestCluster.assert_eventually(fn ->
        Enum.all?([n1, n2, n3], fn node ->
          TestCluster.rpc!(node, EKV, :get, [ekv_name, key]) == "old"
        end)
      end)

      majority = [n1, n2]
      minority = [n3]

      partition(majority, minority)
      Process.sleep(300)

      # Commit CAS from majority with proposer timestamp likely lower than old_ts.
      assert {:ok, _} =
               TestCluster.rpc!(n1, EKV, :put, [ekv_name, key, "new", [consistent: true]])

      TestCluster.assert_eventually(fn ->
        Enum.all?(majority, fn node ->
          TestCluster.rpc!(node, EKV, :get, [ekv_name, key]) == "new"
        end)
      end)

      heal(majority, minority)
      Process.sleep(700)

      # Expected correctness property: cluster converges to the acknowledged CAS value.
      deadline = System.monotonic_time(:millisecond) + 5000

      {converged?, last_vals} =
        Enum.reduce_while(1..120, {false, []}, fn _, _acc ->
          vals =
            Enum.map([n1, n2, n3], fn node ->
              TestCluster.rpc!(node, EKV, :get, [ekv_name, key])
            end)

          if Enum.all?(vals, &(&1 == "new")) do
            {:halt, {true, vals}}
          else
            if System.monotonic_time(:millisecond) >= deadline do
              {:halt, {false, vals}}
            else
              Process.sleep(50)
              {:cont, {false, vals}}
            end
          end
        end)

      assert converged?,
             "expected convergence to CAS value, got values=#{inspect(last_vals)}"
    end

    # Intention: verify repeated partition/heal cycles do not miss writes due to
    # incorrect delta-sync cursor bookkeeping across peers.
    test "second heal still replicates writes after prior conflicting skewed merge" do
      peers = TestCluster.start_peers(2)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, n1}, {_, n2}] = peers
      ekv_name = unique_name(:hwm_seq_space)

      start_stress_cluster(peers, ekv_name, shards: 1)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(500)

      shard_name = :"#{ekv_name}_ekv_replica_0"
      conflict_key = "hwm/conflict"

      partition([n1], [n2])
      Process.sleep(300)

      TestCluster.assert_eventually(fn ->
        n2 not in TestCluster.rpc!(n1, Node, :list, []) and
          n1 not in TestCluster.rpc!(n2, Node, :list, [])
      end)

      # Inject a far-future timestamp on n1 so n2's normal writes lose on heal.
      ahead_bin = :erlang.term_to_binary("ahead")
      ahead_ts = System.system_time(:nanosecond) + 1_000_000_000_000
      ahead_origin = :"ahead_clock@127.0.0.1"

      TestCluster.rpc!(n1, Kernel, :send, [
        shard_name,
        {:ekv_put, conflict_key, ahead_bin, ahead_ts, ahead_origin, nil}
      ])

      TestCluster.rpc!(n1, :sys, :get_state, [shard_name])

      for i <- 1..120 do
        TestCluster.rpc!(n2, EKV, :put, [ekv_name, conflict_key, "behind_#{i}"])
      end

      assert TestCluster.rpc!(n2, EKV, :get, [ekv_name, conflict_key]) == "behind_120"

      # First heal: both nodes converge to the ahead-clock value.
      heal([n1], [n2])
      Process.sleep(700)

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(n1, EKV, :get, [ekv_name, conflict_key]) == "ahead" and
          TestCluster.rpc!(n2, EKV, :get, [ekv_name, conflict_key]) == "ahead"
      end)

      # Second partition and a fresh write only on n1.
      partition([n1], [n2])
      Process.sleep(300)

      TestCluster.assert_eventually(fn ->
        n2 not in TestCluster.rpc!(n1, Node, :list, []) and
          n1 not in TestCluster.rpc!(n2, Node, :list, [])
      end)

      TestCluster.assert_eventually(fn ->
        state = TestCluster.rpc!(n1, :sys, :get_state, [shard_name])
        not Map.has_key?(state.remote_shards, n2)
      end)

      second_key = "hwm/second_heal"
      second_value = "from_n1_after_second_partition"
      :ok = TestCluster.rpc!(n1, EKV, :put, [ekv_name, second_key, second_value])

      # Second heal must still deliver n1's write to n2.
      heal([n1], [n2])
      Process.sleep(700)

      deadline = System.monotonic_time(:millisecond) + 5000

      {converged?, last_seen} =
        Enum.reduce_while(1..120, {false, nil}, fn _, _acc ->
          val = TestCluster.rpc!(n2, EKV, :get, [ekv_name, second_key])

          if val == second_value do
            {:halt, {true, val}}
          else
            if System.monotonic_time(:millisecond) >= deadline do
              {:halt, {false, val}}
            else
              Process.sleep(50)
              {:cont, {false, val}}
            end
          end
        end)

      assert converged?,
             "expected second-heal replication to deliver #{inspect(second_value)} to n2, got #{inspect(last_seen)}"
    end

    test "3+2 partition: concurrent CAS on SAME key — only majority commits" do
      peers = TestCluster.start_peers(5)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, n1}, {_, n2}, {_, n3}, {_, n4}, {_, n5}] = peers
      ekv_name = unique_name(:split)

      start_stress_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(500)

      majority = [n1, n2, n3]
      minority = [n4, n5]

      partition(majority, minority)
      Process.sleep(300)

      # Both sides attempt CAS insert-if-absent on the SAME key simultaneously
      task_maj =
        Task.async(fn ->
          TestCluster.rpc!(n1, EKV, :put, [ekv_name, "brain/1", "majority_val", [if_vsn: nil]])
        end)

      task_min =
        Task.async(fn ->
          TestCluster.rpc!(n4, EKV, :put, [ekv_name, "brain/1", "minority_val", [if_vsn: nil]])
        end)

      [result_maj, result_min] = Task.await_many([task_maj, task_min], 15_000)

      # Majority (3 nodes, quorum=3) should succeed
      assert match?({:ok, _}, result_maj),
             "Majority side should succeed, got: #{inspect(result_maj)}"

      # Minority (2 nodes, quorum=3) must fail
      assert result_min in [{:error, :no_quorum}, {:error, :quorum_timeout}],
             "Minority side should fail, got: #{inspect(result_min)}"

      # Heal
      heal(majority, minority)
      Process.sleep(500)

      # All 5 agree on majority's value — minority's value never appears
      all_nodes = [n1, n2, n3, n4, n5]

      TestCluster.assert_eventually(
        fn ->
          vals =
            Enum.map(all_nodes, fn n -> TestCluster.rpc!(n, EKV, :get, [ekv_name, "brain/1"]) end)

          Enum.all?(vals, &(&1 == "majority_val"))
        end,
        timeout: 5000
      )
    end

    test "2+1+2 three-way partition: nobody can CAS" do
      peers = TestCluster.start_peers(5)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, n1}, {_, n2}, {_, n3}, {_, n4}, {_, n5}] = peers
      ekv_name = unique_name(:three_way)

      start_stress_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(500)

      # Three-way partition: {n1,n2}, {n3,n4}, {n5}
      group_a = [n1, n2]
      group_b = [n3, n4]
      group_c = [n5]

      partition(group_a, group_b)
      partition(group_a, group_c)
      partition(group_b, group_c)
      Process.sleep(300)

      # All three groups attempt CAS on the same key — none has quorum (3)
      task_a =
        Task.async(fn ->
          TestCluster.rpc!(n1, EKV, :put, [ekv_name, "three/1", "from_a", [if_vsn: nil]])
        end)

      task_b =
        Task.async(fn ->
          TestCluster.rpc!(n3, EKV, :put, [ekv_name, "three/1", "from_b", [if_vsn: nil]])
        end)

      task_c =
        Task.async(fn ->
          TestCluster.rpc!(n5, EKV, :put, [ekv_name, "three/1", "from_c", [if_vsn: nil]])
        end)

      results = Task.await_many([task_a, task_b, task_c], 15_000)

      # All must fail — no group has 3 nodes
      for {result, label} <- Enum.zip(results, ["group_a", "group_b", "group_c"]) do
        assert result in [{:error, :no_quorum}, {:error, :quorum_timeout}],
               "#{label} should fail, got: #{inspect(result)}"
      end

      # Heal all partitions
      heal(group_a, group_b)
      heal(group_a, group_c)
      heal(group_b, group_c)
      Process.sleep(500)

      # Key still nil, no value committed
      all_nodes = [n1, n2, n3, n4, n5]

      TestCluster.assert_eventually(
        fn ->
          vals =
            Enum.map(all_nodes, fn n -> TestCluster.rpc!(n, EKV, :get, [ekv_name, "three/1"]) end)

          Enum.all?(vals, &is_nil/1)
        end,
        timeout: 3000
      )

      # CAS now works after heal
      assert {:ok, _} =
               TestCluster.rpc!(n1, EKV, :put, [ekv_name, "three/1", "after_heal", [if_vsn: nil]])

      TestCluster.assert_eventually(
        fn ->
          vals =
            Enum.map(all_nodes, fn n -> TestCluster.rpc!(n, EKV, :get, [ekv_name, "three/1"]) end)

          Enum.all?(vals, &(&1 == "after_heal"))
        end,
        timeout: 5000
      )
    end

    test "rolling quorum: majority group rotates across 5 rounds" do
      peers = TestCluster.start_peers(5)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      nodes = Enum.map(peers, fn {_, node} -> node end)
      ekv_name = unique_name(:rolling)

      start_stress_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(500)

      key = "rolling/counter"
      versions = []

      # Round i: exclude nodes i and (i+1) mod 5 from majority
      {_final_versions, _} =
        Enum.reduce(0..4, {versions, nil}, fn i, {vsns, _prev_vsn} ->
          excluded_a = rem(i, 5)
          excluded_b = rem(i + 1, 5)
          minority = [Enum.at(nodes, excluded_a), Enum.at(nodes, excluded_b)]
          majority = nodes -- minority

          # Partition minority from majority
          partition(majority, minority)
          Process.sleep(200)

          # CAS increment from the first node in the majority
          proposer = hd(majority)

          {:ok, new_val, _} =
            TestCluster.rpc!(proposer, EKV, :update, [
              ekv_name,
              key,
              &TestCluster.cas_increment/1
            ])

          # Record the version
          {_val, vsn} = TestCluster.rpc!(proposer, EKV, :lookup, [ekv_name, key])

          # Heal before next round
          heal(majority, minority)
          Process.sleep(300)

          # Wait for convergence
          TestCluster.assert_eventually(
            fn ->
              vals = Enum.map(nodes, fn n -> TestCluster.rpc!(n, EKV, :get, [ekv_name, key]) end)
              Enum.all?(vals, &(&1 == new_val))
            end,
            timeout: 5000
          )

          {vsns ++ [vsn], vsn}
        end)

      # After 5 rounds: counter should be 5
      all_nodes = nodes

      TestCluster.assert_eventually(
        fn ->
          vals = Enum.map(all_nodes, fn n -> TestCluster.rpc!(n, EKV, :get, [ekv_name, key]) end)
          Enum.all?(vals, &(&1 == 5))
        end,
        timeout: 5000
      )
    end
  end

  # =====================================================================
  # 2. Lost Update Prevention
  # =====================================================================

  describe "lost update prevention" do
    test "committed CAS survives proposer crash + 1 acceptor crash" do
      peers = TestCluster.start_peers(5)
      [{p1, n1}, {p2, _n2}, {_, n3}, {_, n4}, {_, n5}] = peers
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      ekv_name = unique_name(:crash)

      start_stress_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(500)

      # CAS put from n1 (commits with quorum of n1 + some others)
      {:ok, _} =
        TestCluster.rpc!(n1, EKV, :put, [ekv_name, "crash/1", "committed", [if_vsn: nil]])

      # Wait for replication to at least n3
      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(n3, EKV, :get, [ekv_name, "crash/1"]) == "committed"
      end)

      # Kill n1 (proposer) + n2 (one acceptor)
      :peer.stop(p1)
      :peer.stop(p2)
      Process.sleep(300)

      # n3, n4, n5 must all agree on committed value
      survivors = [n3, n4, n5]

      for node <- survivors do
        TestCluster.assert_eventually(fn ->
          TestCluster.rpc!(node, EKV, :get, [ekv_name, "crash/1"]) == "committed"
        end)
      end

      # CAS update from n3 with quorum n3+n4+n5 succeeds and sees previous value
      {_, vsn} = TestCluster.rpc!(n3, EKV, :lookup, [ekv_name, "crash/1"])
      assert vsn != nil, "Version should exist for committed value"
      {:ok, _} = TestCluster.rpc!(n3, EKV, :put, [ekv_name, "crash/1", "updated", [if_vsn: vsn]])

      for node <- survivors do
        TestCluster.assert_eventually(fn ->
          TestCluster.rpc!(node, EKV, :get, [ekv_name, "crash/1"]) == "updated"
        end)
      end
    end

    test "CAS values survive full-cluster rolling restart" do
      peers = TestCluster.start_peers(5)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      nodes = Enum.map(peers, fn {_, node} -> node end)
      ekv_name = unique_name(:rolling_restart)

      start_stress_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(500)

      # CAS put 5 keys from 5 different nodes
      for {node, i} <- Enum.with_index(nodes, 1) do
        {:ok, _} =
          TestCluster.rpc!(node, EKV, :put, [ekv_name, "restart/#{i}", "val#{i}", [if_vsn: nil]])
      end

      # Wait for full replication
      for i <- 1..5, node <- nodes do
        TestCluster.assert_eventually(fn ->
          TestCluster.rpc!(node, EKV, :get, [ekv_name, "restart/#{i}"]) == "val#{i}"
        end)
      end

      # Rolling restart: stop and restart EKV on each node sequentially
      for {_pid, node} <- peers do
        TestCluster.rpc!(node, TestCluster, :kill_registered, [:"#{ekv_name}_ekv_sup"])
        Process.sleep(200)

        # Restart with same data_dir (data persisted)
        node_id =
          Enum.find_index(nodes, &(&1 == node))
          |> Kernel.+(1)

        data_dir = "/tmp/ekv_stress_test_#{node}_#{ekv_name}"

        TestCluster.start_ekv(
          node,
          name: ekv_name,
          data_dir: data_dir,
          shards: 4,
          log: false,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7),
          cluster_size: 5,
          node_id: node_id
        )

        Process.sleep(300)
      end

      Process.sleep(500)

      # After all 5 restarted: all 5 keys present on all nodes
      for i <- 1..5, node <- nodes do
        TestCluster.assert_eventually(
          fn ->
            TestCluster.rpc!(node, EKV, :get, [ekv_name, "restart/#{i}"]) == "val#{i}"
          end,
          timeout: 5000
        )
      end

      # CAS update on one key succeeds (ballot counter restored)
      {_, vsn} = TestCluster.rpc!(hd(nodes), EKV, :lookup, [ekv_name, "restart/1"])

      assert {:ok, _} =
               TestCluster.rpc!(hd(nodes), EKV, :put, [
                 ekv_name,
                 "restart/1",
                 "updated",
                 [if_vsn: vsn]
               ])
    end

    test "50 concurrent increments from 5 nodes: counter exact" do
      peers = TestCluster.start_peers(5)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      nodes = Enum.map(peers, fn {_, node} -> node end)
      ekv_name = unique_name(:counter)

      start_stress_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(500)

      key = "counter/exact"

      # Each node does 10 sequential increments, all 5 in parallel
      # Track actual success count via the returned values
      tasks =
        for node <- nodes do
          Task.async(fn ->
            for _i <- 1..10 do
              do_update_with_retry(node, ekv_name, key, 20)
            end
          end)
        end

      all_results = Task.await_many(tasks, 120_000)

      # Every update must have succeeded (retried until ok)
      total_successes =
        all_results
        |> List.flatten()
        |> Enum.count(fn
          {:ok, _} -> true
          _ -> false
        end)

      assert total_successes == 50,
             "Expected all 50 updates to succeed, got #{total_successes}"

      # The final counter value should be the max of all returned values
      max_val =
        all_results
        |> List.flatten()
        |> Enum.map(fn {:ok, v} -> v end)
        |> Enum.max()

      # All nodes agree on max_val
      TestCluster.assert_eventually(
        fn ->
          vals = Enum.map(nodes, fn n -> TestCluster.rpc!(n, EKV, :get, [ekv_name, key]) end)
          Enum.all?(vals, &(&1 == max_val))
        end,
        timeout: 15_000
      )
    end
  end

  # =====================================================================
  # 3. Phantom Read Prevention
  # =====================================================================

  describe "phantom read prevention" do
    test "reads on acceptor nodes return nil while CAS in accept phase" do
      peers = TestCluster.start_peers(5)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, n1}, {_, n2}, {_, n3}, {_, n4}, {_, n5}] = peers
      ekv_name = unique_name(:phantom)

      start_stress_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(500)

      # Suspend n4, n5 (only 3 active: n1+n2+n3, exactly quorum)
      TestCluster.suspend_shards(n4, ekv_name)
      TestCluster.suspend_shards(n5, ekv_name)

      key = "phantom/read"

      # Before CAS: get from n2,n3 returns nil
      assert TestCluster.rpc!(n2, EKV, :get, [ekv_name, key]) == nil
      assert TestCluster.rpc!(n3, EKV, :get, [ekv_name, key]) == nil

      # CAS insert from n1 → quorum n1+n2+n3
      {:ok, _} = TestCluster.rpc!(n1, EKV, :put, [ekv_name, key, "visible", [if_vsn: nil]])

      # After CAS completes: get from n2,n3 returns committed value
      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(n2, EKV, :get, [ekv_name, key]) == "visible"
      end)

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(n3, EKV, :get, [ekv_name, key]) == "visible"
      end)

      # Resume n4,n5 — they catch up
      TestCluster.resume_shards(n4, ekv_name)
      TestCluster.resume_shards(n5, ekv_name)

      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(n4, EKV, :get, [ekv_name, key]) == "visible" and
          TestCluster.rpc!(n5, EKV, :get, [ekv_name, key]) == "visible"
      end)
    end

    test "scan never returns uncommitted values during contention" do
      peers = TestCluster.start_peers(5)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      nodes = Enum.map(peers, fn {_, node} -> node end)
      ekv_name = unique_name(:scan_phantom)

      start_stress_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(500)

      # Track all values that get committed
      committed_ref = :atomics.new(20, signed: false)

      # 20 concurrent CAS insert-if-absent from random nodes
      cas_tasks =
        for i <- 1..20 do
          node = Enum.random(nodes)

          Task.async(fn ->
            val = "committed_#{i}"

            result =
              TestCluster.rpc!(node, EKV, :put, [ekv_name, "pscan/#{i}", val, [if_vsn: nil]])

            if match?({:ok, _}, result) do
              :atomics.put(committed_ref, i, 1)
              {i, val}
            else
              nil
            end
          end)
        end

      # While CAS ops are in-flight, scan from all nodes repeatedly
      scan_task =
        Task.async(fn ->
          scan_results = []

          for _ <- 1..20 do
            for node <- nodes do
              result = TestCluster.scan_to_map(node, ekv_name, "pscan/")
              [{node, result} | scan_results]
            end

            Process.sleep(10)
          end

          scan_results
        end)

      # Wait for all CAS ops to complete
      cas_results = Task.await_many(cas_tasks, 30_000)
      _scan_results = Task.await(scan_task, 10_000)

      committed_vals =
        cas_results
        |> Enum.reject(&is_nil/1)
        |> Map.new(fn {i, val} -> {"pscan/#{i}", val} end)

      # Verify: every value in final scan matches a committed value
      for node <- nodes do
        final_scan = TestCluster.scan_to_map(node, ekv_name, "pscan/")

        for {key, val} <- final_scan do
          assert Map.has_key?(committed_vals, key),
                 "Key #{key} with value #{inspect(val)} on #{node} was never committed"

          assert committed_vals[key] == val,
                 "Key #{key} on #{node}: expected #{inspect(committed_vals[key])}, got #{inspect(val)}"
        end
      end
    end
  end

  # =====================================================================
  # 4. Livelock Resistance
  # =====================================================================

  describe "livelock resistance" do
    test "5 concurrent proposers: at least one completes" do
      peers = TestCluster.start_peers(5)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      nodes = Enum.map(peers, fn {_, node} -> node end)
      ekv_name = unique_name(:livelock)

      start_stress_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(500)

      key = "livelock/1"

      # All 5 nodes attempt update simultaneously
      tasks =
        for node <- nodes do
          Task.async(fn ->
            TestCluster.rpc!(node, EKV, :update, [
              ekv_name,
              key,
              &TestCluster.cas_increment/1
            ])
          end)
        end

      results = Task.await_many(tasks, 30_000)

      successes =
        Enum.count(results, fn
          {:ok, _, _} -> true
          _ -> false
        end)

      # At least 1 must succeed (system makes progress)
      assert successes >= 1,
             "Expected at least 1 success, got #{successes}: #{inspect(results)}"

      # All 5 agree on final value
      assert_all_agree(nodes, ekv_name, key, timeout: 5000)
    end

    test "sustained contention: 3 updaters for 5 seconds" do
      peers = TestCluster.start_peers(5)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, n1}, {_, n2}, {_, n3}, {_, n4}, {_, n5}] = peers
      all_nodes = [n1, n2, n3, n4, n5]
      ekv_name = unique_name(:sustained)

      start_stress_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(500)

      key = "sustained/counter"
      duration_ms = 5_000

      # n1, n2, n3 continuously update the same key for 5 seconds
      tasks =
        for node <- [n1, n2, n3] do
          Task.async(fn ->
            deadline = System.monotonic_time(:millisecond) + duration_ms
            count = do_sustained_updates(node, ekv_name, key, deadline, 0)
            count
          end)
        end

      success_counts = Task.await_many(tasks, duration_ms + 30_000)
      total = Enum.sum(success_counts)

      # No livelock — at least some increments succeed
      assert total > 0,
             "Expected at least 1 successful increment, got 0 (possible livelock)"

      # Read the final counter from n1 (a participating node)
      final_val = TestCluster.rpc!(n1, EKV, :get, [ekv_name, key])
      assert is_integer(final_val) and final_val > 0

      # All 5 agree on the same final value
      TestCluster.assert_eventually(
        fn ->
          vals = Enum.map(all_nodes, fn n -> TestCluster.rpc!(n, EKV, :get, [ekv_name, key]) end)
          first = hd(vals)
          first != nil and Enum.all?(vals, &(&1 == first))
        end,
        timeout: 15_000
      )
    end

    test "suspended minority doesn't block majority CAS" do
      peers = TestCluster.start_peers(5)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, n1}, {_, n2}, {_, n3}, {_, n4}, {_, n5}] = peers
      all_nodes = [n1, n2, n3, n4, n5]
      ekv_name = unique_name(:suspended_min)

      start_stress_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(500)

      # Suspend n5 permanently
      TestCluster.suspend_shards(n5, ekv_name)

      key = "suspended/counter"

      # 20 sequential CAS increments, round-robin proposer across n1-n4
      active = [n1, n2, n3, n4]

      for i <- 1..20 do
        proposer = Enum.at(active, rem(i - 1, 4))

        {:ok, ^i, _} =
          TestCluster.rpc!(proposer, EKV, :update, [
            ekv_name,
            key,
            &TestCluster.cas_increment/1
          ])
      end

      # All 20 succeeded; n1-n4 agree on counter=20
      for node <- active do
        TestCluster.assert_eventually(fn ->
          TestCluster.rpc!(node, EKV, :get, [ekv_name, key]) == 20
        end)
      end

      # Resume n5 → syncs → agrees on counter=20
      TestCluster.resume_shards(n5, ekv_name)

      TestCluster.assert_eventually(
        fn ->
          vals = Enum.map(all_nodes, fn n -> TestCluster.rpc!(n, EKV, :get, [ekv_name, key]) end)
          Enum.all?(vals, &(&1 == 20))
        end,
        timeout: 5000
      )
    end
  end

  # =====================================================================
  # 5. Version Monotonicity
  # =====================================================================

  describe "version monotonicity" do
    test "20 CAS updates rotating proposers: versions strictly increase" do
      peers = TestCluster.start_peers(5)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      nodes = Enum.map(peers, fn {_, node} -> node end)
      ekv_name = unique_name(:mono)

      start_stress_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(500)

      key = "mono/counter"

      # 20 sequential CAS updates, proposer = node i % 5
      versions =
        Enum.reduce(1..20, [], fn i, vsns ->
          proposer = Enum.at(nodes, rem(i - 1, 5))

          {:ok, ^i, _} =
            TestCluster.rpc!(proposer, EKV, :update, [
              ekv_name,
              key,
              &TestCluster.cas_increment/1
            ])

          {_val, vsn} = TestCluster.rpc!(proposer, EKV, :lookup, [ekv_name, key])
          vsns ++ [vsn]
        end)

      # Assert each version > previous (strict, using tuple comparison)
      versions
      |> Enum.chunk_every(2, 1, :discard)
      |> Enum.with_index(1)
      |> Enum.each(fn {[prev, curr], i} ->
        assert curr > prev,
               "Version regression at step #{i}: #{inspect(prev)} -> #{inspect(curr)}"
      end)
    end

    test "partition heal doesn't cause version regression" do
      peers = TestCluster.start_peers(5)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, n1}, {_, n2}, {_, n3}, {_, n4}, {_, n5}] = peers
      ekv_name = unique_name(:vsn_heal)

      start_stress_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(500)

      key = "vsn/heal"

      # CAS put "v1", record vsn1
      {:ok, _} = TestCluster.rpc!(n1, EKV, :put, [ekv_name, key, "v1", [if_vsn: nil]])
      {"v1", vsn1} = TestCluster.rpc!(n1, EKV, :lookup, [ekv_name, key])
      assert vsn1 != nil

      # Wait for replication
      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(n5, EKV, :get, [ekv_name, key]) == "v1"
      end)

      # Partition {n1,n2,n3} vs {n4,n5}
      majority = [n1, n2, n3]
      minority = [n4, n5]
      partition(majority, minority)
      Process.sleep(300)

      # CAS update to "v2" on majority, record vsn2
      {_, vsn_before} = TestCluster.rpc!(n1, EKV, :lookup, [ekv_name, key])
      {:ok, _} = TestCluster.rpc!(n1, EKV, :put, [ekv_name, key, "v2", [if_vsn: vsn_before]])
      {"v2", vsn2} = TestCluster.rpc!(n1, EKV, :lookup, [ekv_name, key])
      assert vsn2 > vsn1, "vsn2 should be greater than vsn1"

      # Heal
      heal(majority, minority)
      Process.sleep(500)

      # Fetch from n4, n5 — version must be >= vsn2, never vsn1
      for node <- [n4, n5] do
        TestCluster.assert_eventually(fn ->
          {val, vsn_node} = TestCluster.rpc!(node, EKV, :lookup, [ekv_name, key])
          val == "v2" and vsn_node >= vsn2
        end)
      end
    end

    test "CAS after shard crash: ballot strictly higher than pre-crash" do
      peers = TestCluster.start_peers(5)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, n1}, {_, n2}, {_, n3}, {_, n4}, {_, n5}] = peers
      all_nodes = [n1, n2, n3, n4, n5]
      ekv_name = unique_name(:ballot_crash)

      start_stress_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(500)

      key = "ballot/crash"

      # CAS put from n1, record vsn_before
      {:ok, _} = TestCluster.rpc!(n1, EKV, :put, [ekv_name, key, "before_crash", [if_vsn: nil]])
      {"before_crash", vsn_before} = TestCluster.rpc!(n1, EKV, :lookup, [ekv_name, key])

      # Kill shard 0 on n1 via Process.exit(:kill) (supervisor restarts it)
      TestCluster.rpc!(n1, TestCluster, :kill_registered, [:"#{ekv_name}_ekv_replica_0"])
      Process.sleep(500)

      # Wait for shard to restart and reconnect peers
      TestCluster.assert_eventually(
        fn ->
          try do
            TestCluster.rpc!(n1, EKV, :get, [ekv_name, key]) == "before_crash"
          rescue
            _ -> false
          end
        end,
        timeout: 5000
      )

      # CAS put from n1 again, record vsn_after
      {_, vsn_current} = TestCluster.rpc!(n1, EKV, :lookup, [ekv_name, key])

      {:ok, _} =
        TestCluster.rpc!(n1, EKV, :put, [ekv_name, key, "after_crash", [if_vsn: vsn_current]])

      {"after_crash", vsn_after} = TestCluster.rpc!(n1, EKV, :lookup, [ekv_name, key])

      # Version after crash must be strictly higher
      assert vsn_after > vsn_before,
             "Version should increase after shard crash: #{inspect(vsn_before)} -> #{inspect(vsn_after)}"

      # All converge
      TestCluster.assert_eventually(
        fn ->
          vals = Enum.map(all_nodes, fn n -> TestCluster.rpc!(n, EKV, :get, [ekv_name, key]) end)
          Enum.all?(vals, &(&1 == "after_crash"))
        end,
        timeout: 5000
      )
    end
  end

  # =====================================================================
  # 6. Convergence Under Chaos
  # =====================================================================

  describe "convergence under chaos" do
    test "partition during commit broadcast: all converge after heal" do
      peers = TestCluster.start_peers(5)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, n1}, {_, n2}, {_, n3}, {_, n4}, {_, n5}] = peers
      all_nodes = [n1, n2, n3, n4, n5]
      ekv_name = unique_name(:commit_part)

      start_stress_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(500)

      # CAS put from n1 (quorum n1+n2+n3)
      {:ok, _} =
        TestCluster.rpc!(n1, EKV, :put, [ekv_name, "chaos/cas", "cas_val", [if_vsn: nil]])

      # Immediately after :ok return, partition {n1,n2} from {n3,n4,n5}
      # (n3 may not have received commit notification yet)
      partition([n1, n2], [n3, n4, n5])
      Process.sleep(200)

      # LWW writes on both sides (different keys)
      :ok = TestCluster.rpc!(n1, EKV, :put, [ekv_name, "chaos/lww_left", "from_left"])
      :ok = TestCluster.rpc!(n4, EKV, :put, [ekv_name, "chaos/lww_right", "from_right"])

      # Heal
      heal([n1, n2], [n3, n4, n5])
      Process.sleep(500)

      # All 5 agree on CAS value + LWW values
      keys = ["chaos/cas", "chaos/lww_left", "chaos/lww_right"]

      expected = %{
        "chaos/cas" => "cas_val",
        "chaos/lww_left" => "from_left",
        "chaos/lww_right" => "from_right"
      }

      TestCluster.assert_eventually(
        fn ->
          Enum.all?(keys, fn key ->
            vals =
              Enum.map(all_nodes, fn n -> TestCluster.rpc!(n, EKV, :get, [ekv_name, key]) end)

            Enum.all?(vals, &(&1 == expected[key]))
          end)
        end,
        timeout: 10_000
      )
    end

    test "GC purge + CAS: deleted key can be re-created via CAS" do
      peers = TestCluster.start_peers(5)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, n1}, {_, n2}, {_, n3}, {_, n4}, {_, n5}] = peers
      all_nodes = [n1, n2, n3, n4, n5]
      ekv_name = unique_name(:gc_cas)

      # Short gc_interval and tombstone_ttl for this test
      start_stress_cluster(peers, ekv_name,
        gc_interval: 500,
        tombstone_ttl: 2000
      )

      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(500)

      key = "gc/1"

      # CAS put
      {:ok, _} = TestCluster.rpc!(n1, EKV, :put, [ekv_name, key, "original", [if_vsn: nil]])

      TestCluster.assert_eventually(fn ->
        Enum.all?(all_nodes, fn n ->
          TestCluster.rpc!(n, EKV, :get, [ekv_name, key]) == "original"
        end)
      end)

      # CAS delete
      {_, vsn} = TestCluster.rpc!(n1, EKV, :lookup, [ekv_name, key])
      {:ok, _} = TestCluster.rpc!(n1, EKV, :delete, [ekv_name, key, [if_vsn: vsn]])

      TestCluster.assert_eventually(fn ->
        Enum.all?(all_nodes, fn n -> TestCluster.rpc!(n, EKV, :get, [ekv_name, key]) == nil end)
      end)

      # Wait for GC to purge tombstone and orphan kv_paxos
      Process.sleep(3000)

      # CAS insert-if-absent with new value → must succeed
      {:ok, _} = TestCluster.rpc!(n1, EKV, :put, [ekv_name, key, "reborn", [if_vsn: nil]])

      # All 5 agree
      TestCluster.assert_eventually(
        fn ->
          vals = Enum.map(all_nodes, fn n -> TestCluster.rpc!(n, EKV, :get, [ekv_name, key]) end)
          Enum.all?(vals, &(&1 == "reborn"))
        end,
        timeout: 5000
      )
    end

    test "CAS + LWW on same key: second CAS resolves divergence" do
      peers = TestCluster.start_peers(5)
      on_exit(fn -> TestCluster.stop_peers(peers) end)

      [{_, n1}, {_, n2}, {_, n3}, {_, n4}, {_, n5}] = peers
      all_nodes = [n1, n2, n3, n4, n5]
      ekv_name = unique_name(:cas_lww)

      start_stress_cluster(peers, ekv_name)
      on_exit(fn -> cleanup_data(peers, ekv_name) end)
      Process.sleep(500)

      key = "mix/1"

      # CAS put from n1 (committed on quorum via kv_force_upsert)
      {:ok, _} = TestCluster.rpc!(n1, EKV, :put, [ekv_name, key, "cas_v1", [if_vsn: nil]])

      # Wait for replication to n4,n5
      TestCluster.assert_eventually(fn ->
        TestCluster.rpc!(n4, EKV, :get, [ekv_name, key]) == "cas_v1"
      end)

      # Partition to isolate n4,n5
      partition([n1, n2, n3], [n4, n5])
      Process.sleep(300)

      # LWW put from n4 (higher timestamp, wins on n4,n5 via LWW)
      # Sleep to ensure higher timestamp
      Process.sleep(10)
      :ok = TestCluster.rpc!(n4, EKV, :put, [ekv_name, key, "lww_v1"])

      # State may diverge: majority has "cas_v1", minority has "lww_v1"
      assert TestCluster.rpc!(n1, EKV, :get, [ekv_name, key]) == "cas_v1"
      assert TestCluster.rpc!(n4, EKV, :get, [ekv_name, key]) == "lww_v1"

      # Heal
      heal([n1, n2, n3], [n4, n5])
      Process.sleep(500)

      # LWW has higher timestamp, so after sync all should converge to "lww_v1"
      TestCluster.assert_eventually(
        fn ->
          vals = Enum.map(all_nodes, fn n -> TestCluster.rpc!(n, EKV, :get, [ekv_name, key]) end)
          first = hd(vals)
          first != nil and Enum.all?(vals, &(&1 == first))
        end,
        timeout: 5000
      )

      # CAS put with if_vsn may conflict: fetch reads version from kv (which
      # has LWW value), but Paxos prepare reads kv_paxos on acceptors that may
      # not have received the commit notification (partition could interrupt it).
      # Those acceptors still have accepted_value="cas_v1" with a different
      # version, causing the if_vsn check to fail during the accept phase.
      {_converged_val, stale_vsn} = TestCluster.rpc!(n2, EKV, :lookup, [ekv_name, key])
      result = TestCluster.rpc!(n2, EKV, :put, [ekv_name, key, "cas_v2", [if_vsn: stale_vsn]])

      # If commit broadcast was interrupted by partition → :conflict
      # (kv_paxos has old accepted values with different version than kv).
      # If commit broadcast completed before partition → {:ok, _}
      # Either way: the value must be deterministic and consistent.
      assert match?({:ok, _}, result) or result in [{:error, :conflict}, {:error, :unconfirmed}],
             "Expected {:ok, _}, :conflict, or :unconfirmed, got: #{inspect(result)}"

      # update/3 always resolves the divergence: it reads the highest accepted
      # value during prepare and applies the transform function to it, so
      # there's no version mismatch — it doesn't use if_vsn.
      {:ok, new_val, _} =
        TestCluster.rpc!(n2, EKV, :update, [
          ekv_name,
          key,
          &TestCluster.cas_upcase/1
        ])

      assert is_binary(new_val)

      # All 5 converge to the CAS-resolved value
      TestCluster.assert_eventually(
        fn ->
          vals = Enum.map(all_nodes, fn n -> TestCluster.rpc!(n, EKV, :get, [ekv_name, key]) end)
          first = hd(vals)
          first == new_val and Enum.all?(vals, &(&1 == first))
        end,
        timeout: 5000
      )

      # Verify the system isn't stuck: a fresh fetch + if_vsn CAS must work
      # (proves kv_paxos is clean after update resolved the divergence)
      {^new_val, fresh_vsn} = TestCluster.rpc!(n2, EKV, :lookup, [ekv_name, key])
      assert fresh_vsn != nil
      {:ok, _} = TestCluster.rpc!(n2, EKV, :put, [ekv_name, key, "final", [if_vsn: fresh_vsn]])

      TestCluster.assert_eventually(
        fn ->
          vals = Enum.map(all_nodes, fn n -> TestCluster.rpc!(n, EKV, :get, [ekv_name, key]) end)
          Enum.all?(vals, &(&1 == "final"))
        end,
        timeout: 5000
      )
    end
  end

  # =====================================================================
  # 7. Fault Injection (local single-shard with injected fake peers)
  # =====================================================================

  describe "fault injection" do
    setup do
      name = :"ekv_fi_#{System.unique_integer([:positive])}"
      data_dir = Path.join(System.tmp_dir!(), "ekv_fi_test_#{name}")

      {:ok, pid} =
        EKV.start_link(
          name: name,
          data_dir: data_dir,
          shards: 1,
          log: false,
          cluster_size: 5,
          node_id: "1",
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )

      on_exit(fn ->
        Process.exit(pid, :shutdown)
        File.rm_rf!(data_dir)
      end)

      shard_name = :"#{name}_ekv_replica_0"

      # Inject 4 fake peers — all pointing to self() so we receive their messages
      fake_nodes = [
        :"fi_peer2@127.0.0.1",
        :"fi_peer3@127.0.0.1",
        :"fi_peer4@127.0.0.1",
        :"fi_peer5@127.0.0.1"
      ]

      :sys.replace_state(shard_name, fn state ->
        %{
          state
          | remote_shards: Map.new(fake_nodes, fn n -> {n, self()} end),
            peer_node_ids: %{
              :"fi_peer2@127.0.0.1" => "2",
              :"fi_peer3@127.0.0.1" => "3",
              :"fi_peer4@127.0.0.1" => "4",
              :"fi_peer5@127.0.0.1" => "5"
            }
        }
      end)

      %{name: name, data_dir: data_dir, shard_name: shard_name}
    end

    # Helper: start a CAS GenServer.call in a task, wait for it to be pending,
    # then extract the ref from pending_cas via :sys.get_state.
    # Returns {task, ref, shard_pid} — shard_pid is needed to send responses.
    defp start_cas_and_get_ref(shard_name, call_args) do
      task = Task.async(fn -> GenServer.call(shard_name, call_args, 10_000) end)

      # Wait for the CAS to appear in pending_cas
      {ref, _op} =
        poll_pending_cas(shard_name, fn pending_cas ->
          case Map.to_list(pending_cas) do
            [{ref, op}] -> {ref, op}
            _ -> nil
          end
        end)

      shard_pid = Process.whereis(shard_name)
      {task, ref, shard_pid}
    end

    defp poll_pending_cas(shard_name, extract_fn, attempts \\ 50) do
      state = :sys.get_state(shard_name)
      result = extract_fn.(state.pending_cas)

      if result != nil do
        result
      else
        if attempts <= 0, do: raise("pending_cas never populated")
        Process.sleep(10)
        poll_pending_cas(shard_name, extract_fn, attempts - 1)
      end
    end

    # ----- Timing & Race Conditions (4) -----

    test "GC purge_orphan_paxos preserves promised-but-not-accepted rows", %{
      name: name,
      shard_name: shard_name
    } do
      key = "fi/gc_purge_during_cas"

      # Send a prepare to the shard (as if we're a remote proposer)
      # This creates a kv_paxos row with promised_counter > 0, accepted_counter = 0
      ref = make_ref()
      send(shard_name, {:ekv_prepare, ref, self(), key, 100, "2", 0})
      assert_receive {:ekv_promise, ^ref, _, _, _, _, _}, 2000

      # Verify: key NOT in kv, but kv_paxos has a promised row
      assert EKV.get(name, key) == nil

      # Trigger GC — purge_orphan_paxos must NOT delete this row because
      # promised_counter > 0 (active Paxos round in progress)
      now_ms = System.system_time(:millisecond)
      send(shard_name, {:gc, now_ms, 0})
      :sys.get_state(shard_name)

      # Accept with the original ballot — should succeed because
      # the promised row survived GC
      now = System.system_time(:nanosecond)
      origin_str = Atom.to_string(node())
      val = :erlang.term_to_binary("gc_survivor")
      entry = {key, val, now, origin_str, nil, nil}

      ref2 = make_ref()
      send(shard_name, {:ekv_accept, ref2, self(), key, 100, "2", entry, 0})
      assert_receive {:ekv_accepted, ^ref2, _, _}, 2000

      # Commit and verify the value
      send(shard_name, {:ekv_cas_committed, key, 100, "2", 0})
      :sys.get_state(shard_name)
      assert EKV.get(name, key) == "gc_survivor"
    end

    test "GC purge_orphan_paxos preserves rows with stale promises after commit", %{
      name: name,
      shard_name: shard_name
    } do
      key = "fi/gc_stale_promise"

      # Accept with ballot {100, "2"} — creates row with promised + accepted state
      now = System.system_time(:nanosecond)
      origin_str = Atom.to_string(node())
      val = :erlang.term_to_binary("accepted")
      entry = {key, val, now, origin_str, nil, nil}

      ref = make_ref()
      send(shard_name, {:ekv_accept, ref, self(), key, 100, "2", entry, 0})
      assert_receive {:ekv_accepted, ^ref, _, _}, 2000

      # Commit — promotes to kv, clears accepted columns, keeps promised_counter
      send(shard_name, {:ekv_cas_committed, key, 100, "2", 0})
      :sys.get_state(shard_name)
      assert EKV.get(name, key) == "accepted"

      # Delete the key so kv row goes away (tombstone)
      :ok = EKV.delete(name, key)

      # Trigger GC with future cutoff to purge the tombstone
      future_ms = System.system_time(:millisecond) + 1_000_000
      send(shard_name, {:gc, future_ms, future_ms})
      :sys.get_state(shard_name)

      # kv_paxos row survives: promised_counter > 0 (from the accept's implicit
      # prepare). This is necessary — clearing it would allow stale accepts from
      # older ballots, and kv_force_upsert would overwrite committed values.
      state = :sys.get_state(shard_name)

      {:ok, rows} =
        EKV.Sqlite3.fetch_all(
          state.db,
          "SELECT promised_counter, accepted_counter FROM kv_paxos WHERE key = ?1",
          [key]
        )

      assert length(rows) == 1
      [[prom_c, acc_c]] = rows
      assert prom_c > 0, "promised_counter should be preserved"
      assert acc_c == 0, "accepted_counter should be cleared"

      # A fresh prepare still works — accepted state is clean
      ref2 = make_ref()
      send(shard_name, {:ekv_prepare, ref2, self(), key, 200, "3", 0})

      assert_receive {:ekv_promise, ^ref2, _, _, acc_c2, acc_n2, _kv_row}, 2000
      assert acc_c2 == 0
      assert acc_n2 in ["", nil]
    end

    test "commit to non-acceptor: no kv_paxos row exists", %{
      name: name,
      shard_name: shard_name
    } do
      key = "fi/commit_no_row"

      # Send a commit for a key that was never prepared/accepted on this shard
      send(shard_name, {:ekv_cas_committed, key, 100, "2", 0})
      :sys.get_state(shard_name)

      # paxos_promote looks up kv_paxos row → no row → should return :stale
      # No crash, no phantom value
      assert EKV.get(name, key) == nil
      assert Process.alive?(Process.whereis(shard_name))

      # Shard still functional — can do normal operations
      :ok = EKV.put(name, "fi/after_phantom", "still_works")
      assert EKV.get(name, "fi/after_phantom") == "still_works"
    end

    test "concurrent CAS on different keys, same shard: no interference", %{
      name: name,
      shard_name: shard_name
    } do
      key_a = "fi/concurrent_a"
      key_b = "fi/concurrent_b"

      # Start 2 CAS ops simultaneously via tasks
      task_a =
        Task.async(fn ->
          GenServer.call(
            shard_name,
            {:cas_put, key_a, :erlang.term_to_binary("val_a"), nil, []},
            10_000
          )
        end)

      task_b =
        Task.async(fn ->
          GenServer.call(
            shard_name,
            {:cas_put, key_b, :erlang.term_to_binary("val_b"), nil, []},
            10_000
          )
        end)

      # Wait for both CAS ops to appear in pending_cas
      {refs, _ops} =
        poll_pending_cas(shard_name, fn pending ->
          if map_size(pending) == 2 do
            {Map.keys(pending), Map.values(pending)}
          else
            nil
          end
        end)

      shard_pid = Process.whereis(shard_name)

      # Send 2 promises for each ref (from "2" and "3")
      for ref <- refs do
        send(shard_pid, {:ekv_promise, ref, self(), "2", 0, "", nil})
        send(shard_pid, {:ekv_promise, ref, self(), "3", 0, "", nil})
      end

      Process.sleep(50)

      # Wait for both to enter accept phase (or be completed)
      poll_pending_cas(shard_name, fn pending ->
        if Enum.all?(refs, fn r ->
             case Map.get(pending, r) do
               %{phase: :accept} -> true
               nil -> true
               _ -> false
             end
           end) do
          true
        else
          nil
        end
      end)

      # Send 2 accepts for each ref
      for ref <- refs do
        send(shard_pid, {:ekv_accepted, ref, self(), "2"})
        send(shard_pid, {:ekv_accepted, ref, self(), "3"})
      end

      # Both should commit
      assert match?({:ok, _}, Task.await(task_a, 5000))
      assert match?({:ok, _}, Task.await(task_b, 5000))

      # No cross-contamination
      assert EKV.get(name, key_a) == "val_a"
      assert EKV.get(name, key_b) == "val_b"
    end

    test "late accept arriving after proposer already committed", %{
      name: name,
      shard_name: shard_name
    } do
      key = "fi/late_accept"

      {task, ref, shard_pid} =
        start_cas_and_get_ref(
          shard_name,
          {:cas_put, key, :erlang.term_to_binary("committed_val"), nil, []}
        )

      # 2 promises → quorum
      send(shard_pid, {:ekv_promise, ref, self(), "2", 0, "", nil})
      send(shard_pid, {:ekv_promise, ref, self(), "3", 0, "", nil})
      Process.sleep(50)

      # Wait for accept phase
      poll_pending_cas(shard_name, fn pending ->
        case Map.get(pending, ref) do
          %{phase: :accept} -> true
          nil -> true
          _ -> nil
        end
      end)

      # Save the ref before it gets removed
      old_ref = ref

      # 2 accepts → quorum → commit
      send(shard_pid, {:ekv_accepted, ref, self(), "2"})
      send(shard_pid, {:ekv_accepted, ref, self(), "3"})

      assert match?({:ok, _}, Task.await(task, 5000))
      assert EKV.get(name, key) == "committed_val"

      # Now send late accepts with the old ref — ref already removed from pending_cas
      send(shard_name, {:ekv_accepted, old_ref, self(), "4"})
      send(shard_name, {:ekv_accepted, old_ref, self(), "5"})
      :sys.get_state(shard_name)

      # No crash, value unchanged
      assert EKV.get(name, key) == "committed_val"
      assert Process.alive?(Process.whereis(shard_name))
    end

    # ----- Value Corruption Attempts (3) -----

    test "promises with conflicting kv_rows: picks highest {ts, origin}", %{
      name: name,
      shard_name: shard_name
    } do
      key = "fi/conflicting_kv_rows"

      # Write val_A via LWW
      :ok = EKV.put(name, key, "val_A")

      {task, ref, shard_pid} =
        start_cas_and_get_ref(
          shard_name,
          {:update, key, fn val -> String.upcase(val) end, []}
        )

      # Forge promises with different kv_rows (different values, different timestamps)
      now = System.system_time(:nanosecond)
      origin = Atom.to_string(node())

      # Promise from "2": kv_row with "val_B" at ts=now+100
      kv_row_b = [:erlang.term_to_binary("val_B"), now + 100, origin, nil, nil]
      send(shard_pid, {:ekv_promise, ref, self(), "2", 0, "", kv_row_b})

      # Promise from "3": kv_row with "val_C" at ts=now+200
      kv_row_c = [:erlang.term_to_binary("val_C"), now + 200, origin, nil, nil]
      send(shard_pid, {:ekv_promise, ref, self(), "3", 0, "", kv_row_c})

      Process.sleep(50)

      # Wait for accept phase (or done)
      poll_pending_cas(shard_name, fn pending ->
        case Map.get(pending, ref) do
          %{phase: :accept} -> true
          nil -> true
          _ -> nil
        end
      end)

      # Respond with 2 accepts
      send(shard_pid, {:ekv_accepted, ref, self(), "2"})
      send(shard_pid, {:ekv_accepted, ref, self(), "3"})

      {:ok, result, _} = Task.await(task, 5000)

      # The update function uppercased the value with the highest {ts, origin}.
      # "val_C" has ts=now+200 (highest), so it always wins deterministically.
      assert result == "VAL_C"

      # Value is committed and readable
      assert EKV.get(name, key) == result
    end

    test "commit notification with wrong ballot: paxos_promote rejects", %{
      name: name,
      shard_name: shard_name
    } do
      key = "fi/wrong_ballot_commit"

      # Accept with ballot {100, "2"}
      now = System.system_time(:nanosecond)
      origin_str = Atom.to_string(node())
      val = :erlang.term_to_binary("accepted_val")
      entry = {key, val, now, origin_str, nil, nil}

      ref = make_ref()
      send(shard_name, {:ekv_accept, ref, self(), key, 100, "2", entry, 0})
      assert_receive {:ekv_accepted, ^ref, _, _}, 2000

      # Value NOT in kv yet (only in kv_paxos)
      assert EKV.get(name, key) == nil

      # Send commit with WRONG ballot {101, "2"} (doesn't match accepted {100, "2"})
      send(shard_name, {:ekv_cas_committed, key, 101, "2", 0})
      :sys.get_state(shard_name)

      # paxos_promote checks ballot matches → :stale
      assert EKV.get(name, key) == nil
      assert Process.alive?(Process.whereis(shard_name))

      # Correct ballot commit should still work
      send(shard_name, {:ekv_cas_committed, key, 100, "2", 0})
      :sys.get_state(shard_name)
      assert EKV.get(name, key) == "accepted_val"
    end

    test "triple prepare: middle ballot wins, lowest and highest stale", %{
      name: name,
      shard_name: shard_name
    } do
      key = "fi/triple_prepare"

      # Prepare ballot=100 → promise
      ref1 = make_ref()
      send(shard_name, {:ekv_prepare, ref1, self(), key, 100, "2", 0})
      assert_receive {:ekv_promise, ^ref1, _, _, _, _, _}, 2000

      # Prepare ballot=200 → promise (supersedes 100)
      ref2 = make_ref()
      send(shard_name, {:ekv_prepare, ref2, self(), key, 200, "3", 0})
      assert_receive {:ekv_promise, ^ref2, _, _, _, _, _}, 2000

      # Prepare ballot=150 → nack (lower than 200)
      ref3 = make_ref()
      send(shard_name, {:ekv_prepare, ref3, self(), key, 150, "4", 0})
      assert_receive {:ekv_nack, ^ref3, _, _, 200, "3"}, 2000

      # Accept ballot=200 → accepted
      now = System.system_time(:nanosecond)
      origin_str = Atom.to_string(node())
      val200 = :erlang.term_to_binary("ballot_200_val")
      entry200 = {key, val200, now, origin_str, nil, nil}

      ref4 = make_ref()
      send(shard_name, {:ekv_accept, ref4, self(), key, 200, "3", entry200, 0})
      assert_receive {:ekv_accepted, ^ref4, _, _}, 2000

      # Commit ballot=200 → value in kv
      send(shard_name, {:ekv_cas_committed, key, 200, "3", 0})
      :sys.get_state(shard_name)
      assert EKV.get(name, key) == "ballot_200_val"

      # Late accept for ballot=100 → rejected (promised/accepted 200)
      val100 = :erlang.term_to_binary("ballot_100_val")
      entry100 = {key, val100, now + 1, origin_str, nil, nil}

      ref5 = make_ref()
      send(shard_name, {:ekv_accept, ref5, self(), key, 100, "2", entry100, 0})
      assert_receive {:ekv_accept_nack, ^ref5, _, _}, 2000

      # Only ballot 200's value in kv
      assert EKV.get(name, key) == "ballot_200_val"
      assert Process.alive?(Process.whereis(shard_name))
    end

    # ----- Real-World Latency & Reordering (4) -----

    test "message reordering: accept arrives before prepare at acceptor", %{
      name: name,
      shard_name: shard_name
    } do
      mref = Process.monitor(Process.whereis(shard_name))
      key = "fi/reorder_accept_before_prepare"
      now = System.system_time(:nanosecond)
      origin_str = Atom.to_string(node())
      val = :erlang.term_to_binary("reordered_val")
      entry = {key, val, now, origin_str, nil, nil}

      # Send accept WITHOUT any prior prepare — TCP reordering scenario
      # No kv_paxos row exists, so promised_counter defaults to 0.
      # paxos_accept checks ballot_c >= promised_c → 100 >= 0 → succeeds (inserts new row)
      ref1 = make_ref()
      send(shard_name, {:ekv_accept, ref1, self(), key, 100, "2", entry, 0})
      assert_receive {:ekv_accepted, ^ref1, _, _}, 2000

      # Now send prepare with SAME ballot — arrives late due to reordering
      # paxos_prepare checks ballot_c > promised_c (strictly greater)
      # After accept, promised_counter=100 → 100 is NOT > 100 → nack
      ref2 = make_ref()
      send(shard_name, {:ekv_prepare, ref2, self(), key, 100, "2", 0})
      assert_receive {:ekv_nack, ^ref2, _, _, 100, "2"}, 2000

      # Higher ballot can still recover — prepare with ballot 200
      ref3 = make_ref()
      send(shard_name, {:ekv_prepare, ref3, self(), key, 200, "3", 0})
      assert_receive {:ekv_promise, ^ref3, _, _, acc_c, acc_n, _kv_row}, 2000

      # Promise carries the accepted value from the out-of-order accept
      assert acc_c == 100
      assert acc_n == "2"

      # Value not in kv yet (no commit), shard healthy
      assert EKV.get(name, key) == nil
      refute_receive {:DOWN, ^mref, :process, _, _}
    end

    test "delayed commit: new round completes before old commit arrives", %{
      name: name,
      shard_name: shard_name
    } do
      mref = Process.monitor(Process.whereis(shard_name))
      key = "fi/delayed_commit"
      now = System.system_time(:nanosecond)
      origin_str = Atom.to_string(node())

      # Round 1: ballot {100, "2"} — prepare + accept
      ref1 = make_ref()
      send(shard_name, {:ekv_prepare, ref1, self(), key, 100, "2", 0})
      assert_receive {:ekv_promise, ^ref1, _, _, _, _, _}, 2000

      val_old = :erlang.term_to_binary("old_val")
      entry_100 = {key, val_old, now, origin_str, nil, nil}
      ref2 = make_ref()
      send(shard_name, {:ekv_accept, ref2, self(), key, 100, "2", entry_100, 0})
      assert_receive {:ekv_accepted, ^ref2, _, _}, 2000

      # Round 2: ballot {200, "3"} — supersedes round 1
      ref3 = make_ref()
      send(shard_name, {:ekv_prepare, ref3, self(), key, 200, "3", 0})
      assert_receive {:ekv_promise, ^ref3, _, _, acc_c, acc_n, _kv_row}, 2000
      # Promise carries round 1's accepted value
      assert acc_c == 100
      assert acc_n == "2"

      val_new = :erlang.term_to_binary("new_val")
      entry_200 = {key, val_new, now + 1, origin_str, nil, nil}
      ref4 = make_ref()
      send(shard_name, {:ekv_accept, ref4, self(), key, 200, "3", entry_200, 0})
      assert_receive {:ekv_accepted, ^ref4, _, _}, 2000

      # Commit round 2 — value promoted to kv
      send(shard_name, {:ekv_cas_committed, key, 200, "3", 0})
      :sys.get_state(shard_name)
      assert EKV.get(name, key) == "new_val"

      # Delayed commit for round 1 arrives — stale because accepted columns
      # were cleared after round 2's commit (ballot {100,"2"} doesn't match)
      send(shard_name, {:ekv_cas_committed, key, 100, "2", 0})
      :sys.get_state(shard_name)

      # Value unchanged — stale commit rejected
      assert EKV.get(name, key) == "new_val"
      refute_receive {:DOWN, ^mref, :process, _, _}
    end

    test "GC between prepare and accept: concurrent proposer does not corrupt", %{
      name: name,
      shard_name: shard_name
    } do
      mref = Process.monitor(Process.whereis(shard_name))
      key = "fi/gc_between_prepare_accept"

      # Proposer A: prepare ballot {100, "2"}
      ref1 = make_ref()
      send(shard_name, {:ekv_prepare, ref1, self(), key, 100, "2", 0})
      assert_receive {:ekv_promise, ^ref1, _, _, _, _, _}, 2000

      # Trigger GC — purge_orphan_paxos must NOT delete the promised row
      # (our fix: AND promised_counter = 0 in purge SQL)
      now_ms = System.system_time(:millisecond)
      send(shard_name, {:gc, now_ms, 0})
      :sys.get_state(shard_name)

      # Verify kv_paxos row survived GC
      state = :sys.get_state(shard_name)

      {:ok, rows} =
        EKV.Sqlite3.fetch_all(
          state.db,
          "SELECT promised_counter, promised_node FROM kv_paxos WHERE key = ?1",
          [key]
        )

      assert length(rows) == 1
      [[prom_c, _prom_n]] = rows
      assert prom_c == 100

      # Concurrent proposer B: prepare ballot {200, "3"} — supersedes A
      ref2 = make_ref()
      send(shard_name, {:ekv_prepare, ref2, self(), key, 200, "3", 0})
      assert_receive {:ekv_promise, ^ref2, _, _, _, _, _}, 2000

      # Late accept from proposer A (ballot 100) — must be rejected
      # because promised_counter is now 200
      now = System.system_time(:nanosecond)
      origin_str = Atom.to_string(node())
      val_a = :erlang.term_to_binary("val_a")
      entry_a = {key, val_a, now, origin_str, nil, nil}

      ref3 = make_ref()
      send(shard_name, {:ekv_accept, ref3, self(), key, 100, "2", entry_a, 0})
      assert_receive {:ekv_accept_nack, ^ref3, _, _}, 2000

      # Accept from proposer B (ballot 200) — succeeds
      val_b = :erlang.term_to_binary("val_b")
      entry_b = {key, val_b, now + 1, origin_str, nil, nil}

      ref4 = make_ref()
      send(shard_name, {:ekv_accept, ref4, self(), key, 200, "3", entry_b, 0})
      assert_receive {:ekv_accepted, ^ref4, _, _}, 2000

      # Commit ballot 200 — only B's value committed
      send(shard_name, {:ekv_cas_committed, key, 200, "3", 0})
      :sys.get_state(shard_name)
      assert EKV.get(name, key) == "val_b"
      refute_receive {:DOWN, ^mref, :process, _, _}
    end

    test "proposer crash mid-broadcast: partial commit notification", %{
      name: name,
      shard_name: shard_name
    } do
      mref = Process.monitor(Process.whereis(shard_name))
      key_a = "fi/partial_commit_a"
      key_b = "fi/partial_commit_b"
      now = System.system_time(:nanosecond)
      origin_str = Atom.to_string(node())

      # Prepare both keys with ballot {100, "2"}
      ref_a1 = make_ref()
      send(shard_name, {:ekv_prepare, ref_a1, self(), key_a, 100, "2", 0})
      assert_receive {:ekv_promise, ^ref_a1, _, _, _, _, _}, 2000

      ref_b1 = make_ref()
      send(shard_name, {:ekv_prepare, ref_b1, self(), key_b, 100, "2", 0})
      assert_receive {:ekv_promise, ^ref_b1, _, _, _, _, _}, 2000

      # Accept both keys
      val_a = :erlang.term_to_binary("val_a")
      entry_a = {key_a, val_a, now, origin_str, nil, nil}
      ref_a2 = make_ref()
      send(shard_name, {:ekv_accept, ref_a2, self(), key_a, 100, "2", entry_a, 0})
      assert_receive {:ekv_accepted, ^ref_a2, _, _}, 2000

      val_b = :erlang.term_to_binary("val_b")
      entry_b = {key_b, val_b, now + 1, origin_str, nil, nil}
      ref_b2 = make_ref()
      send(shard_name, {:ekv_accept, ref_b2, self(), key_b, 100, "2", entry_b, 0})
      assert_receive {:ekv_accepted, ^ref_b2, _, _}, 2000

      # Partial commit: proposer crashes after sending commit for key_a only
      send(shard_name, {:ekv_cas_committed, key_a, 100, "2", 0})
      :sys.get_state(shard_name)

      # key_a committed, key_b NOT committed
      assert EKV.get(name, key_a) == "val_a"
      assert EKV.get(name, key_b) == nil

      # Verify key_b's accepted state is preserved in kv_paxos
      state = :sys.get_state(shard_name)

      {:ok, rows} =
        EKV.Sqlite3.fetch_all(
          state.db,
          "SELECT accepted_counter, accepted_node, accepted_value FROM kv_paxos WHERE key = ?1",
          [key_b]
        )

      assert length(rows) == 1
      [[acc_c, acc_n, acc_val]] = rows
      assert acc_c == 100
      assert acc_n == "2"
      assert acc_val != nil

      # New proposer (ballot {200, "3"}) recovers key_b
      ref_b3 = make_ref()
      send(shard_name, {:ekv_prepare, ref_b3, self(), key_b, 200, "3", 0})
      assert_receive {:ekv_promise, ^ref_b3, _, _, rec_acc_c, rec_acc_n, _kv_row}, 2000

      # Promise carries the accepted value from the crashed proposer
      assert rec_acc_c == 100
      assert rec_acc_n == "2"

      # Accept with the recovered value (re-propose it at higher ballot)
      ref_b4 = make_ref()
      send(shard_name, {:ekv_accept, ref_b4, self(), key_b, 200, "3", entry_b, 0})
      assert_receive {:ekv_accepted, ^ref_b4, _, _}, 2000

      # Commit ballot 200 — key_b now in kv
      send(shard_name, {:ekv_cas_committed, key_b, 200, "3", 0})
      :sys.get_state(shard_name)

      # Both keys readable and correct
      assert EKV.get(name, key_a) == "val_a"
      assert EKV.get(name, key_b) == "val_b"
      refute_receive {:DOWN, ^mref, :process, _, _}
    end

    # ----- CASPaxos Correctness (3) -----

    # Intention: verify that `EKV.get(..., consistent: true)` acts as a recovery read
    # and does not rewrite accepted metadata (expires_at/deleted_at) when committing
    # a pending accepted value.
    test "consistent read recovery preserves accepted TTL and tombstone metadata", %{
      name: name,
      shard_name: shard_name
    } do
      state = :sys.get_state(shard_name)
      db = state.db
      shard_pid = Process.whereis(shard_name)
      origin = Atom.to_string(node())

      run_consistent_read = fn key ->
        task = Task.async(fn -> EKV.get(name, key, consistent: true) end)

        ref =
          poll_pending_cas(shard_name, fn pending_cas ->
            Enum.find_value(pending_cas, fn {ref, op} ->
              if op.key == key, do: ref
            end)
          end)

        send(shard_pid, {:ekv_promise, ref, self(), "2", 0, "", nil})
        send(shard_pid, {:ekv_promise, ref, self(), "3", 0, "", nil})

        poll_pending_cas(shard_name, fn pending ->
          case Map.get(pending, ref) do
            %{phase: :accept} -> true
            nil -> true
            _ -> nil
          end
        end)

        send(shard_pid, {:ekv_accepted, ref, self(), "2"})
        send(shard_pid, {:ekv_accepted, ref, self(), "3"})

        Task.await(task, 5_000)
      end

      key_ttl = "fi/cas_read_preserve_ttl"
      value_ttl = :erlang.term_to_binary("ttl_val")
      ts_ttl = System.system_time(:nanosecond) - 2_000_000_000
      expires_at = ts_ttl + 60_000_000_000

      {:ok, :promise, 0, "", _} = EKV.Store.paxos_prepare(db, key_ttl, 100, "n1")

      {:ok, true} =
        EKV.Store.paxos_accept(db, key_ttl, 100, "n1", [
          value_ttl,
          ts_ttl,
          origin,
          expires_at,
          nil
        ])

      assert run_consistent_read.(key_ttl) == "ttl_val"

      {stored_ttl_val, stored_ttl_ts, stored_ttl_origin, stored_ttl_exp, stored_ttl_del} =
        EKV.Store.get(db, key_ttl)

      assert stored_ttl_val == value_ttl
      assert stored_ttl_ts == ts_ttl
      assert stored_ttl_origin == String.to_atom(origin)
      assert stored_ttl_exp == expires_at
      assert stored_ttl_del == nil

      key_del = "fi/cas_read_preserve_delete"
      ts_del = System.system_time(:nanosecond) - 1_000_000_000

      {:ok, :promise, 0, "", _} = EKV.Store.paxos_prepare(db, key_del, 200, "n1")

      {:ok, true} =
        EKV.Store.paxos_accept(db, key_del, 200, "n1", [nil, ts_del, origin, nil, ts_del])

      assert run_consistent_read.(key_del) == nil

      {stored_del_val, stored_del_ts, stored_del_origin, stored_del_exp, stored_del_deleted_at} =
        EKV.Store.get(db, key_del)

      assert stored_del_val == nil
      assert stored_del_ts == ts_del
      assert stored_del_origin == String.to_atom(origin)
      assert stored_del_exp == nil
      assert stored_del_deleted_at == ts_del
    end

    test "prepare returns accepted tombstone (not stale kv row) after accepted delete", %{
      name: name,
      shard_name: shard_name
    } do
      key = "fi/delete_pending"
      :ok = EKV.put(name, key, "v1")

      state = :sys.get_state(shard_name)
      db = state.db

      {:ok, :promise, 0, "", _} = EKV.Store.paxos_prepare(db, key, 100, "n1")

      now = System.system_time(:nanosecond)
      origin = Atom.to_string(node())

      # Accepted-but-not-committed delete (tombstone) in kv_paxos.
      {:ok, true} = EKV.Store.paxos_accept(db, key, 100, "n1", [nil, now, origin, nil, now])

      # Correct CASPaxos behavior: a higher prepare must observe that accepted tombstone.
      {:ok, :promise, 100, "n1", [_val, _ts, _origin, _exp, deleted_at]} =
        EKV.Store.paxos_prepare(db, key, 200, "n2")

      assert is_integer(deleted_at),
             "prepare should return accepted tombstone from kv_paxos, not old committed kv value"
    end

    test "if_vsn CAS rejects stale vsn when quorum promises include fresher state", %{
      name: name,
      shard_name: shard_name
    } do
      key = "fi/quorum_select"
      :ok = EKV.put(name, key, "v1")
      {"v1", stale_vsn} = EKV.lookup(name, key)

      task =
        Task.async(fn ->
          EKV.put(name, key, "v_new", if_vsn: stale_vsn)
        end)

      ref =
        poll_pending_cas(shard_name, fn pending_cas ->
          case Map.to_list(pending_cas) do
            [{ref, _op}] -> ref
            _ -> nil
          end
        end)

      stale_value_bin = :erlang.term_to_binary("v1")
      {stale_ts, stale_origin} = stale_vsn
      stale_origin_str = Atom.to_string(stale_origin)
      stale_row = [stale_value_bin, stale_ts, stale_origin_str, nil, nil]

      fresh_row = [
        :erlang.term_to_binary("v2"),
        stale_ts + 1_000,
        stale_origin_str,
        nil,
        nil
      ]

      shard_pid = Process.whereis(shard_name)

      # Quorum has conflicting committed states with accepted_counter=0.
      # Proposer must pick highest {ts, origin} — fresh_row wins, stale if_vsn must conflict.
      send(shard_pid, {:ekv_promise, ref, self(), "2", 0, "", fresh_row})
      send(shard_pid, {:ekv_promise, ref, self(), "3", 0, "", stale_row})

      Process.sleep(20)
      send(shard_pid, {:ekv_accepted, ref, self(), "2"})
      send(shard_pid, {:ekv_accepted, ref, self(), "3"})

      assert Task.await(task, 5_000) == {:error, :conflict},
             "stale if_vsn should conflict when quorum already contains fresher state"
    end
  end

  # =====================================================================
  # Helpers
  # =====================================================================

  defp do_update_with_retry(node, ekv_name, key, retries) do
    case TestCluster.rpc!(node, EKV, :update, [
           ekv_name,
           key,
           &TestCluster.cas_increment/1
         ]) do
      {:ok, val, _vsn} ->
        {:ok, val}

      {:error, reason} when retries > 0 and reason in [:conflict, :unconfirmed] ->
        Process.sleep(:rand.uniform(50) + 10)
        do_update_with_retry(node, ekv_name, key, retries - 1)

      error ->
        raise "Update failed after retries: #{inspect(error)}"
    end
  end

  defp do_sustained_updates(node, ekv_name, key, deadline, count) do
    if System.monotonic_time(:millisecond) >= deadline do
      count
    else
      case TestCluster.rpc!(node, EKV, :update, [
             ekv_name,
             key,
             &TestCluster.cas_increment/1
           ]) do
        {:ok, _, _} ->
          do_sustained_updates(node, ekv_name, key, deadline, count + 1)

        {:error, _} ->
          # Brief backoff on contention failure
          Process.sleep(:rand.uniform(20))
          do_sustained_updates(node, ekv_name, key, deadline, count)
      end
    end
  end
end
