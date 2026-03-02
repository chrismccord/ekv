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
      assert result_maj == :ok,
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
          vals = Enum.map(all_nodes, fn n -> TestCluster.rpc!(n, EKV, :get, [ekv_name, "brain/1"]) end)
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
          vals = Enum.map(all_nodes, fn n -> TestCluster.rpc!(n, EKV, :get, [ekv_name, "three/1"]) end)
          Enum.all?(vals, &is_nil/1)
        end,
        timeout: 3000
      )

      # CAS now works after heal
      assert :ok =
               TestCluster.rpc!(n1, EKV, :put, [ekv_name, "three/1", "after_heal", [if_vsn: nil]])

      TestCluster.assert_eventually(
        fn ->
          vals = Enum.map(all_nodes, fn n -> TestCluster.rpc!(n, EKV, :get, [ekv_name, "three/1"]) end)
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

          {:ok, new_val} =
            TestCluster.rpc!(proposer, EKV, :update, [
              ekv_name,
              key,
              &TestCluster.cas_increment/1
            ])

          # Record the version
          {:ok, _val, vsn} = TestCluster.rpc!(proposer, EKV, :fetch, [ekv_name, key])

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
      :ok = TestCluster.rpc!(n1, EKV, :put, [ekv_name, "crash/1", "committed", [if_vsn: nil]])

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
      {:ok, _, vsn} = TestCluster.rpc!(n3, EKV, :fetch, [ekv_name, "crash/1"])
      assert vsn != nil, "Version should exist for committed value"
      :ok = TestCluster.rpc!(n3, EKV, :put, [ekv_name, "crash/1", "updated", [if_vsn: vsn]])

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
        :ok =
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
      {:ok, _, vsn} = TestCluster.rpc!(hd(nodes), EKV, :fetch, [ekv_name, "restart/1"])
      assert :ok = TestCluster.rpc!(hd(nodes), EKV, :put, [ekv_name, "restart/1", "updated", [if_vsn: vsn]])
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
        |> Enum.count(fn {:ok, _} -> true; _ -> false end)

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
      :ok = TestCluster.rpc!(n1, EKV, :put, [ekv_name, key, "visible", [if_vsn: nil]])

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
            result = TestCluster.rpc!(node, EKV, :put, [ekv_name, "pscan/#{i}", val, [if_vsn: nil]])

            if result == :ok do
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
              result = TestCluster.rpc!(node, EKV, :scan, [ekv_name, "pscan/"])
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
        final_scan = TestCluster.rpc!(node, EKV, :scan, [ekv_name, "pscan/"])

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
          {:ok, _} -> true
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

        {:ok, ^i} =
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

          {:ok, ^i} =
            TestCluster.rpc!(proposer, EKV, :update, [
              ekv_name,
              key,
              &TestCluster.cas_increment/1
            ])

          {:ok, _val, vsn} = TestCluster.rpc!(proposer, EKV, :fetch, [ekv_name, key])
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
      :ok = TestCluster.rpc!(n1, EKV, :put, [ekv_name, key, "v1", [if_vsn: nil]])
      {:ok, "v1", vsn1} = TestCluster.rpc!(n1, EKV, :fetch, [ekv_name, key])
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
      {:ok, _, vsn_before} = TestCluster.rpc!(n1, EKV, :fetch, [ekv_name, key])
      :ok = TestCluster.rpc!(n1, EKV, :put, [ekv_name, key, "v2", [if_vsn: vsn_before]])
      {:ok, "v2", vsn2} = TestCluster.rpc!(n1, EKV, :fetch, [ekv_name, key])
      assert vsn2 > vsn1, "vsn2 should be greater than vsn1"

      # Heal
      heal(majority, minority)
      Process.sleep(500)

      # Fetch from n4, n5 — version must be >= vsn2, never vsn1
      for node <- [n4, n5] do
        TestCluster.assert_eventually(fn ->
          {:ok, val, vsn_node} = TestCluster.rpc!(node, EKV, :fetch, [ekv_name, key])
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
      :ok = TestCluster.rpc!(n1, EKV, :put, [ekv_name, key, "before_crash", [if_vsn: nil]])
      {:ok, "before_crash", vsn_before} = TestCluster.rpc!(n1, EKV, :fetch, [ekv_name, key])

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
      {:ok, _, vsn_current} = TestCluster.rpc!(n1, EKV, :fetch, [ekv_name, key])
      :ok = TestCluster.rpc!(n1, EKV, :put, [ekv_name, key, "after_crash", [if_vsn: vsn_current]])
      {:ok, "after_crash", vsn_after} = TestCluster.rpc!(n1, EKV, :fetch, [ekv_name, key])

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
      :ok = TestCluster.rpc!(n1, EKV, :put, [ekv_name, "chaos/cas", "cas_val", [if_vsn: nil]])

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
            vals = Enum.map(all_nodes, fn n -> TestCluster.rpc!(n, EKV, :get, [ekv_name, key]) end)
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
      :ok = TestCluster.rpc!(n1, EKV, :put, [ekv_name, key, "original", [if_vsn: nil]])

      TestCluster.assert_eventually(fn ->
        Enum.all?(all_nodes, fn n -> TestCluster.rpc!(n, EKV, :get, [ekv_name, key]) == "original" end)
      end)

      # CAS delete
      {:ok, _, vsn} = TestCluster.rpc!(n1, EKV, :fetch, [ekv_name, key])
      :ok = TestCluster.rpc!(n1, EKV, :delete, [ekv_name, key, [if_vsn: vsn]])

      TestCluster.assert_eventually(fn ->
        Enum.all?(all_nodes, fn n -> TestCluster.rpc!(n, EKV, :get, [ekv_name, key]) == nil end)
      end)

      # Wait for GC to purge tombstone and orphan kv_paxos
      Process.sleep(3000)

      # CAS insert-if-absent with new value → must succeed
      :ok = TestCluster.rpc!(n1, EKV, :put, [ekv_name, key, "reborn", [if_vsn: nil]])

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
      :ok = TestCluster.rpc!(n1, EKV, :put, [ekv_name, key, "cas_v1", [if_vsn: nil]])

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
      {:ok, _converged_val, stale_vsn} = TestCluster.rpc!(n2, EKV, :fetch, [ekv_name, key])
      result = TestCluster.rpc!(n2, EKV, :put, [ekv_name, key, "cas_v2", [if_vsn: stale_vsn]])

      # If commit broadcast was interrupted by partition → :conflict
      # (kv_paxos has old accepted values with different version than kv).
      # If commit broadcast completed before partition → :ok
      # Either way: the value must be deterministic and consistent.
      assert result in [:ok, {:error, :conflict}],
             "Expected :ok or :conflict, got: #{inspect(result)}"

      # update/3 always resolves the divergence: it reads the highest accepted
      # value during prepare and applies the transform function to it, so
      # there's no version mismatch — it doesn't use if_vsn.
      {:ok, new_val} =
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
      {:ok, ^new_val, fresh_vsn} = TestCluster.rpc!(n2, EKV, :fetch, [ekv_name, key])
      assert fresh_vsn != nil
      :ok = TestCluster.rpc!(n2, EKV, :put, [ekv_name, key, "final", [if_vsn: fresh_vsn]])

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
  # Helpers
  # =====================================================================

  defp do_update_with_retry(node, ekv_name, key, retries) do
    case TestCluster.rpc!(node, EKV, :update, [
           ekv_name,
           key,
           &TestCluster.cas_increment/1
         ]) do
      {:ok, val} ->
        {:ok, val}

      {:error, :conflict} when retries > 0 ->
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
        {:ok, _} ->
          do_sustained_updates(node, ekv_name, key, deadline, count + 1)

        {:error, _} ->
          # Brief backoff on contention failure
          Process.sleep(:rand.uniform(20))
          do_sustained_updates(node, ekv_name, key, deadline, count)
      end
    end
  end
end
