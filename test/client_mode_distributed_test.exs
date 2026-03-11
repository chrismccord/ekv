defmodule EKV.ClientModeDistributedTest do
  use ExUnit.Case

  @moduletag :capture_log
  @moduletag timeout: 60_000

  alias EKV.TestCluster

  defp unique_name(prefix) do
    :"#{prefix}_#{System.unique_integer([:positive])}"
  end

  defp member_data_dir(node, ekv_name) do
    "/tmp/ekv_client_mode_#{node}_#{ekv_name}"
  end

  defp cleanup_data(peers, ekv_name) do
    Enum.each(peers, fn {_pid, node} ->
      dir = member_data_dir(node, ekv_name)

      try do
        TestCluster.rpc!(node, File, :rm_rf!, [dir])
      catch
        _, _ -> :ok
      end
    end)
  end

  defp start_blue_green_cluster(old_node, n2, n3, ekv_name, shared_dir) do
    File.rm_rf!(shared_dir)

    TestCluster.start_ekv(
      old_node,
      name: ekv_name,
      data_dir: shared_dir,
      shards: 2,
      log: false,
      blue_green: true,
      region: "iad",
      gc_interval: :timer.hours(1),
      tombstone_ttl: :timer.hours(24 * 7),
      cluster_size: 3,
      node_id: "m1"
    )

    for {node, node_id, region} <- [{n2, "m2", "dfw"}, {n3, "m3", "ord"}] do
      data_dir = member_data_dir(node, ekv_name)
      TestCluster.rpc!(node, File, :rm_rf!, [data_dir])

      TestCluster.start_ekv(
        node,
        name: ekv_name,
        data_dir: data_dir,
        shards: 2,
        log: false,
        region: region,
        gc_interval: :timer.hours(1),
        tombstone_ttl: :timer.hours(24 * 7),
        cluster_size: 3,
        node_id: node_id
      )
    end
  end

  test "client wait_for_route blocks startup until a preferred member route is available" do
    peers = TestCluster.start_peers(2)
    on_exit(fn -> TestCluster.stop_peers(peers) end)

    [{_, member_node}, {_, client_node}] = peers
    ekv_name = unique_name(:client_wait_route)
    on_exit(fn -> cleanup_data(peers, ekv_name) end)

    client_task =
      Task.async(fn ->
        TestCluster.start_ekv(
          client_node,
          name: ekv_name,
          mode: :client,
          log: false,
          region: "ord",
          region_routing: ["iad"],
          wait_for_route: 2_000
        )
      end)

    Process.sleep(150)
    assert Task.yield(client_task, 0) == nil

    {:ok, _member_pid} =
      TestCluster.start_ekv(
        member_node,
        name: ekv_name,
        data_dir: member_data_dir(member_node, ekv_name),
        shards: 1,
        log: false,
        region: "iad",
        gc_interval: :timer.hours(1),
        tombstone_ttl: :timer.hours(24 * 7)
      )

    assert {:ok, _client_pid} = Task.await(client_task, 3_000)

    TestCluster.assert_eventually(fn ->
      TestCluster.rpc!(client_node, EKV, :info, [ekv_name]).current_backend == member_node
    end)
  end

  test "client wait_for_quorum blocks startup until the selected backend reaches quorum" do
    peers = TestCluster.start_peers(3)
    on_exit(fn -> TestCluster.stop_peers(peers) end)

    [{_, member_a}, {_, member_b}, {_, client_node}] = peers
    ekv_name = unique_name(:client_wait_quorum)
    on_exit(fn -> cleanup_data(peers, ekv_name) end)

    client_task =
      Task.async(fn ->
        TestCluster.start_ekv(
          client_node,
          name: ekv_name,
          mode: :client,
          log: false,
          region: "ord",
          region_routing: ["iad", "lhr"],
          wait_for_route: 3_000,
          wait_for_quorum: 3_000
        )
      end)

    {:ok, _member_a_pid} =
      TestCluster.start_ekv(
        member_a,
        name: ekv_name,
        data_dir: member_data_dir(member_a, ekv_name),
        shards: 1,
        log: false,
        region: "iad",
        cluster_size: 2,
        node_id: 1,
        gc_interval: :timer.hours(1),
        tombstone_ttl: :timer.hours(24 * 7)
      )

    Process.sleep(200)
    assert Task.yield(client_task, 0) == nil

    {:ok, _member_b_pid} =
      TestCluster.start_ekv(
        member_b,
        name: ekv_name,
        data_dir: member_data_dir(member_b, ekv_name),
        shards: 1,
        log: false,
        region: "lhr",
        cluster_size: 2,
        node_id: 2,
        gc_interval: :timer.hours(1),
        tombstone_ttl: :timer.hours(24 * 7)
      )

    assert {:ok, _client_pid} = Task.await(client_task, 5_000)

    assert {:ok, _vsn} =
             TestCluster.rpc!(client_node, EKV, :put, [ekv_name, "boot/1", "ready", [if_vsn: nil]])

    TestCluster.assert_eventually(fn ->
      TestCluster.rpc!(member_b, EKV, :get, [ekv_name, "boot/1", [consistent: true]]) == "ready"
    end)
  end

  test "client mode routes by preferred region, supports scan/keys paging, CAS, subscriptions, and failover" do
    peers = TestCluster.start_peers(3)
    on_exit(fn -> TestCluster.stop_peers(peers) end)

    [{_, member_a}, {_, member_b}, {_, client_node}] = peers
    ekv_name = unique_name(:client_mode)
    on_exit(fn -> cleanup_data(peers, ekv_name) end)

    for {node, node_id, region} <- [{member_a, 1, "iad"}, {member_b, 2, "lhr"}] do
      data_dir = member_data_dir(node, ekv_name)
      TestCluster.rpc!(node, File, :rm_rf!, [data_dir])

      {:ok, _pid} =
        TestCluster.start_ekv(
          node,
          name: ekv_name,
          data_dir: data_dir,
          shards: 2,
          log: false,
          region: region,
          cluster_size: 2,
          node_id: node_id,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )
    end

    {:ok, _client_pid} =
      TestCluster.start_ekv(
        client_node,
        name: ekv_name,
        mode: :client,
        log: false,
        region: "ord",
        region_routing: ["lhr", "iad"]
      )

    TestCluster.assert_eventually(
      fn ->
        TestCluster.rpc!(client_node, EKV, :info, [ekv_name]).current_backend == member_b
      end,
      timeout: 5_000
    )

    assert :ok = TestCluster.rpc!(client_node, EKV, :put, [ekv_name, "client/lww", "value"])

    TestCluster.assert_eventually(fn ->
      TestCluster.rpc!(member_a, EKV, :get, [ekv_name, "client/lww"]) == "value" and
        TestCluster.rpc!(member_b, EKV, :get, [ekv_name, "client/lww"]) == "value"
    end)

    assert {:ok, _vsn} =
             TestCluster.rpc!(client_node, EKV, :put, [
               ekv_name,
               "client/cas",
               "seed",
               [if_vsn: nil]
             ])

    assert {:ok, "SEED", _vsn} =
             TestCluster.rpc!(client_node, EKV, :update, [
               ekv_name,
               "client/cas",
               &TestCluster.cas_upcase/1
             ])

    TestCluster.assert_eventually(fn ->
      TestCluster.rpc!(member_a, EKV, :get, [ekv_name, "client/cas", [consistent: true]]) ==
        "SEED"
    end)

    for i <- 1..550 do
      assert :ok = TestCluster.rpc!(member_a, EKV, :put, [ekv_name, "page/#{i}", i])
    end

    TestCluster.assert_eventually(
      fn ->
        TestCluster.rpc!(client_node, TestCluster, :do_scan_count, [ekv_name, "page/"]) == 550 and
          TestCluster.rpc!(client_node, TestCluster, :do_keys_count, [ekv_name, "page/"]) == 550
      end,
      timeout: 10_000
    )

    TestCluster.subscribe_on(client_node, ekv_name, "notify/", self())

    TestCluster.assert_eventually(
      fn ->
        TestCluster.rpc!(member_a, EKV.Supervisor, :client_subscribers?, [ekv_name])
      end,
      timeout: 5_000
    )

    assert :ok = TestCluster.rpc!(member_a, EKV, :put, [ekv_name, "notify/1", "event"])

    assert_receive {:remote_ekv_event, [%EKV.Event{type: :put, key: "notify/1", value: "event"}],
                    %{name: ^ekv_name}},
                   5_000

    TestCluster.disconnect_nodes(client_node, member_b)

    TestCluster.assert_eventually(
      fn ->
        try do
          TestCluster.rpc!(client_node, EKV, :get, [ekv_name, "client/cas", [consistent: true]]) ==
            "SEED"
        rescue
          _ -> false
        catch
          :exit, _ -> false
        end
      end,
      timeout: 5_000
    )

    TestCluster.assert_eventually(
      fn ->
        TestCluster.rpc!(client_node, EKV, :info, [ekv_name]).current_backend == member_a
      end,
      timeout: 5_000
    )
  end

  test "terminal member stops advertising before shutdown barrier wait and new clients avoid it" do
    peers = TestCluster.start_peers(3)
    on_exit(fn -> TestCluster.stop_peers(peers) end)

    [{_, member_a}, {_, member_b}, {_, client_node}] = peers
    ekv_name = unique_name(:client_shutdown_advertise)
    on_exit(fn -> cleanup_data(peers, ekv_name) end)

    for {node, node_id, region} <- [{member_a, 1, "iad"}, {member_b, 2, "lhr"}] do
      data_dir = member_data_dir(node, ekv_name)
      TestCluster.rpc!(node, File, :rm_rf!, [data_dir])

      {:ok, _pid} =
        TestCluster.start_ekv(
          node,
          name: ekv_name,
          data_dir: data_dir,
          shards: 1,
          log: false,
          region: region,
          cluster_size: 2,
          node_id: node_id,
          shutdown_barrier: 3_000,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )
    end

    stop_task = Task.async(fn -> TestCluster.stop_ekv(member_b, ekv_name, 5_000) end)

    TestCluster.assert_eventually(
      fn ->
        TestCluster.rpc!(member_b, EKV.MemberPresence, :advertised?, [ekv_name]) == false
      end,
      timeout: 2_000
    )

    {:ok, _client_pid} =
      TestCluster.start_ekv(
        client_node,
        name: ekv_name,
        mode: :client,
        log: false,
        region: "ord",
        region_routing: ["lhr", "iad"],
        wait_for_route: 5_000
      )

    TestCluster.assert_eventually(
      fn ->
        TestCluster.rpc!(client_node, EKV, :info, [ekv_name]).current_backend == member_a
      end,
      timeout: 5_000
    )

    assert :ok = Task.await(stop_task, 6_000)
  end

  test "client handles backend replica shutdown without raising and fails over" do
    peers = TestCluster.start_peers(4)
    on_exit(fn -> TestCluster.stop_peers(peers) end)

    [{_, member_a}, {_, member_b}, {_, member_c}, {_, client_node}] = peers
    ekv_name = unique_name(:client_backend_shutdown)
    on_exit(fn -> cleanup_data(peers, ekv_name) end)

    for {node, node_id, region} <- [
          {member_a, 1, "iad"},
          {member_b, 2, "lhr"},
          {member_c, 3, "ord"}
        ] do
      data_dir = member_data_dir(node, ekv_name)
      TestCluster.rpc!(node, File, :rm_rf!, [data_dir])

      {:ok, _pid} =
        TestCluster.start_ekv(
          node,
          name: ekv_name,
          data_dir: data_dir,
          shards: 1,
          log: false,
          region: region,
          cluster_size: 3,
          node_id: node_id,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )
    end

    assert {:ok, _vsn} =
             TestCluster.rpc!(member_a, EKV, :put, [ekv_name, "seed", "v1", [if_vsn: nil]])

    {:ok, _client_pid} =
      TestCluster.start_ekv(
        client_node,
        name: ekv_name,
        mode: :client,
        log: false,
        region: "ord",
        region_routing: ["lhr", "iad", "ord"]
      )

    TestCluster.assert_eventually(
      fn ->
        TestCluster.rpc!(client_node, EKV, :info, [ekv_name]).current_backend == member_b
      end,
      timeout: 5_000
    )

    TestCluster.terminate_replica_shard(member_b, ekv_name, 0)

    assert {:error, :unavailable} =
             TestCluster.rpc!(client_node, EKV, :put, [ekv_name, "lww", "v1"])

    assert {:error, :unavailable} =
             TestCluster.rpc!(client_node, EKV, :put, [
               ekv_name,
               "seed2",
               "v2",
               [if_vsn: nil, resolve_unconfirmed: true]
             ])

    TestCluster.assert_eventually(
      fn ->
        TestCluster.rpc!(client_node, EKV, :info, [ekv_name]).current_backend in [
          member_a,
          member_c
        ]
      end,
      timeout: 5_000
    )

    TestCluster.assert_eventually(
      fn ->
        try do
          TestCluster.rpc!(client_node, EKV, :get, [ekv_name, "seed", [consistent: true]]) == "v1"
        rescue
          _ -> false
        catch
          :exit, _ -> false
        end
      end,
      timeout: 5_000
    )

    assert {:ok, _vsn} =
             TestCluster.rpc!(client_node, EKV, :put, [ekv_name, "seed2", "v2", [if_vsn: nil]])
  end

  test "blue-green overlap keeps existing client bound to outgoing node while new clients route to incoming node" do
    peers = TestCluster.start_peers(6)
    on_exit(fn -> TestCluster.stop_peers(peers) end)

    [{_, n1}, {_, n1b}, {_, n2}, {_, n3}, {_, client_old_node}, {_, client_new_node}] = peers
    ekv_name = unique_name(:client_blue_green)
    shared_dir = "/tmp/ekv_client_blue_green_shared_#{ekv_name}"

    on_exit(fn ->
      cleanup_data(peers, ekv_name)
      File.rm_rf!(shared_dir)
    end)

    start_blue_green_cluster(n1, n2, n3, ekv_name, shared_dir)

    TestCluster.assert_eventually(fn ->
      match?(
        {:ok, _},
        TestCluster.rpc!(n1, EKV, :put, [ekv_name, "bg/seed", "v1", [if_vsn: nil]])
      )
    end)

    TestCluster.assert_eventually(fn ->
      TestCluster.rpc!(n2, EKV, :get, [ekv_name, "bg/seed", [consistent: true]]) == "v1"
    end)

    {:ok, _old_client_pid} =
      TestCluster.start_ekv(
        client_old_node,
        name: ekv_name,
        mode: :client,
        log: false,
        region: "ewr",
        region_routing: ["iad", "dfw", "ord"]
      )

    TestCluster.assert_eventually(fn ->
      TestCluster.rpc!(client_old_node, EKV, :info, [ekv_name]).current_backend == n1
    end)

    {:ok, _pid} =
      TestCluster.start_ekv(
        n1b,
        name: ekv_name,
        data_dir: shared_dir,
        shards: 2,
        log: false,
        blue_green: true,
        region: "iad",
        gc_interval: :timer.hours(1),
        tombstone_ttl: :timer.hours(24 * 7),
        cluster_size: 3,
        node_id: "m1"
      )

    TestCluster.assert_eventually(fn ->
      TestCluster.rpc!(n1b, EKV, :await_quorum, [ekv_name, 0]) == :ok
    end)

    TestCluster.assert_eventually(fn ->
      TestCluster.rpc!(client_old_node, EKV, :info, [ekv_name]).current_backend == n1
    end)

    assert {:ok, _vsn} =
             TestCluster.rpc!(client_old_node, EKV, :put, [
               ekv_name,
               "bg/proxy",
               "through_old",
               [if_vsn: nil]
             ])

    TestCluster.assert_eventually(fn ->
      TestCluster.rpc!(n2, EKV, :get, [ekv_name, "bg/proxy", [consistent: true]]) == "through_old"
    end)

    region_group = EKV.MemberPresence.region_group(ekv_name, "iad")

    TestCluster.assert_eventually(fn ->
      TestCluster.rpc!(n1b, :pg, :get_members, [EKV.Supervisor.pg_scope(ekv_name), region_group])
      |> Enum.map(&node/1)
      |> Enum.uniq()
      |> Enum.sort()
      |> Kernel.==([n1b])
    end)

    {:ok, _new_client_pid} =
      TestCluster.start_ekv(
        client_new_node,
        name: ekv_name,
        mode: :client,
        log: false,
        region: "bos",
        region_routing: ["iad", "dfw", "ord"],
        wait_for_route: 5_000
      )

    TestCluster.assert_eventually(fn ->
      case TestCluster.rpc!(client_new_node, EKV, :info, [ekv_name]).current_backend do
        backend when backend in [n1b, n2, n3] -> true
        _ -> false
      end
    end)

    assert :ok =
             TestCluster.rpc!(client_new_node, EKV, :put, [ekv_name, "bg/new", "from_incoming"])

    TestCluster.assert_eventually(fn ->
      TestCluster.rpc!(client_old_node, EKV, :get, [ekv_name, "bg/new"]) == "from_incoming"
    end)
  end
end
