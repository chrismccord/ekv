defmodule EKV.ObserverModeDistributedTest do
  use ExUnit.Case

  @moduletag :capture_log
  @moduletag timeout: 60_000

  alias EKV.TestCluster

  defp unique_name(prefix) do
    :"#{prefix}_#{System.unique_integer([:positive])}"
  end

  defp data_dir(node, ekv_name) do
    "/tmp/ekv_observer_mode_#{node}_#{ekv_name}"
  end

  defp cleanup_data(peers, ekv_name) do
    Enum.each(peers, fn {_pid, node} ->
      dir = data_dir(node, ekv_name)

      try do
        TestCluster.rpc!(node, File, :rm_rf!, [dir])
      catch
        _, _ -> :ok
      end
    end)
  end

  test "observer serves local eventual ops, routes CAS to voters, and clients ignore observers as backends" do
    peers = TestCluster.start_peers(4)
    on_exit(fn -> TestCluster.stop_peers(peers) end)

    [{_, voter_a}, {_, voter_b}, {_, observer_node}, {_, client_node}] = peers
    ekv_name = unique_name(:observer_mode)
    on_exit(fn -> cleanup_data(peers, ekv_name) end)

    for {node, node_id, region} <- [{voter_a, "v1", "iad"}, {voter_b, "v2", "lhr"}] do
      TestCluster.rpc!(node, File, :rm_rf!, [data_dir(node, ekv_name)])

      {:ok, _pid} =
        TestCluster.start_ekv(
          node,
          name: ekv_name,
          data_dir: data_dir(node, ekv_name),
          shards: 1,
          log: false,
          region: region,
          cluster_size: 2,
          node_id: node_id,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )
    end

    {:ok, _observer_pid} =
      TestCluster.start_ekv(
        observer_node,
        name: ekv_name,
        mode: :observer,
        data_dir: data_dir(observer_node, ekv_name),
        shards: 1,
        log: false,
        region: "ord",
        region_routing: ["iad", "lhr"],
        cluster_size: 2,
        node_id: "obs1",
        gc_interval: :timer.hours(1),
        tombstone_ttl: :timer.hours(24 * 7)
      )

    {:ok, _client_pid} =
      TestCluster.start_ekv(
        client_node,
        name: ekv_name,
        mode: :client,
        log: false,
        region: "cdg",
        region_routing: ["ord", "iad", "lhr"]
      )

    TestCluster.assert_eventually(fn ->
      TestCluster.rpc!(observer_node, EKV, :info, [ekv_name]).current_backend in [
        voter_a,
        voter_b
      ]
    end)

    TestCluster.assert_eventually(fn ->
      TestCluster.rpc!(client_node, EKV, :info, [ekv_name]).current_backend in [voter_a, voter_b]
    end)

    assert :ok = TestCluster.rpc!(observer_node, EKV, :put, [ekv_name, "obs/lww", "local"])
    assert TestCluster.rpc!(observer_node, EKV, :get, [ekv_name, "obs/lww"]) == "local"

    assert {[{"obs/lww", "local", _vsn}], _next_cursor} =
             TestCluster.rpc!(observer_node, EKV, :__scan_page__, [ekv_name, "obs/", nil, 10])

    assert {[{"obs/lww", _vsn}], _next_cursor} =
             TestCluster.rpc!(observer_node, EKV, :__keys_page__, [ekv_name, "obs/", nil, 10])

    TestCluster.assert_eventually(fn ->
      TestCluster.rpc!(voter_a, EKV, :get, [ekv_name, "obs/lww"]) == "local" and
        TestCluster.rpc!(voter_b, EKV, :get, [ekv_name, "obs/lww"]) == "local"
    end)

    assert {:ok, _vsn} =
             TestCluster.rpc!(observer_node, EKV, :put, [ekv_name, "obs/cas", "c1", [if_vsn: nil]])

    assert TestCluster.rpc!(observer_node, EKV, :get, [ekv_name, "obs/cas"]) == "c1"

    TestCluster.assert_eventually(fn ->
      TestCluster.rpc!(voter_a, EKV, :get, [ekv_name, "obs/cas"]) == "c1" and
        TestCluster.rpc!(voter_b, EKV, :get, [ekv_name, "obs/cas"]) == "c1"
    end)

    backup_dir = data_dir(observer_node, :"#{ekv_name}_backup")
    TestCluster.rpc!(observer_node, File, :rm_rf!, [backup_dir])
    assert :ok = TestCluster.rpc!(observer_node, EKV, :backup, [ekv_name, backup_dir])
    assert TestCluster.rpc!(observer_node, File, :exists?, [Path.join(backup_dir, "shard_0.db")])
  end

  test "observer does not count toward quorum" do
    peers = TestCluster.start_peers(3)
    on_exit(fn -> TestCluster.stop_peers(peers) end)

    [{_, voter_a}, {_, voter_b}, {_, observer_node}] = peers
    ekv_name = unique_name(:observer_quorum)
    on_exit(fn -> cleanup_data(peers, ekv_name) end)

    for {node, node_id, region} <- [{voter_a, "v1", "iad"}, {voter_b, "v2", "lhr"}] do
      TestCluster.rpc!(node, File, :rm_rf!, [data_dir(node, ekv_name)])

      {:ok, _pid} =
        TestCluster.start_ekv(
          node,
          name: ekv_name,
          data_dir: data_dir(node, ekv_name),
          shards: 1,
          log: false,
          region: region,
          cluster_size: 2,
          node_id: node_id,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )
    end

    {:ok, _observer_pid} =
      TestCluster.start_ekv(
        observer_node,
        name: ekv_name,
        mode: :observer,
        data_dir: data_dir(observer_node, ekv_name),
        shards: 1,
        log: false,
        region: "ord",
        region_routing: ["iad", "lhr"],
        cluster_size: 2,
        node_id: "obs1",
        gc_interval: :timer.hours(1),
        tombstone_ttl: :timer.hours(24 * 7)
      )

    assert {:ok, _vsn} =
             TestCluster.rpc!(voter_a, EKV, :put, [ekv_name, "q/key", "v", [if_vsn: nil]])

    TestCluster.disconnect_nodes(voter_a, voter_b)

    TestCluster.assert_eventually(fn ->
      TestCluster.rpc!(voter_a, EKV, :put, [ekv_name, "q/key2", "v2", [if_vsn: nil]]) ==
        {:error, :no_quorum}
    end)
  end

  test "surviving observer still performs CAS after another observer exits" do
    peers = TestCluster.start_peers(4)
    on_exit(fn -> TestCluster.stop_peers(peers) end)

    [{_, voter_a}, {_, voter_b}, {_, observer_a}, {observer_peer, observer_b}] = peers
    ekv_name = unique_name(:observer_survives_observer_exit)
    on_exit(fn -> cleanup_data(peers, ekv_name) end)

    for {node, node_id, region} <- [{voter_a, "v1", "iad"}, {voter_b, "v2", "lhr"}] do
      TestCluster.rpc!(node, File, :rm_rf!, [data_dir(node, ekv_name)])

      {:ok, _pid} =
        TestCluster.start_ekv(
          node,
          name: ekv_name,
          data_dir: data_dir(node, ekv_name),
          shards: 1,
          log: false,
          region: region,
          cluster_size: 2,
          node_id: node_id,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )
    end

    for {node, node_id, region} <- [{observer_a, "obs1", "ord"}, {observer_b, "obs2", "fra"}] do
      {:ok, _observer_pid} =
        TestCluster.start_ekv(
          node,
          name: ekv_name,
          mode: :observer,
          data_dir: data_dir(node, ekv_name),
          shards: 1,
          log: false,
          region: region,
          region_routing: ["iad", "lhr"],
          cluster_size: 2,
          node_id: node_id,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )
    end

    TestCluster.assert_eventually(fn ->
      TestCluster.rpc!(observer_a, EKV, :info, [ekv_name]).current_backend in [voter_a, voter_b]
    end)

    assert {:ok, _vsn} =
             TestCluster.rpc!(observer_a, EKV, :put, [
               ekv_name,
               "obs/live/1",
               "before",
               [if_vsn: nil]
             ])

    TestCluster.stop_peers([{observer_peer, observer_b}])

    TestCluster.assert_eventually(fn ->
      case TestCluster.rpc!(observer_a, Node, :list, []) do
        nodes when is_list(nodes) -> observer_b not in nodes
      end
    end)

    assert {:ok, _vsn} =
             TestCluster.rpc!(observer_a, EKV, :put, [
               ekv_name,
               "obs/live/2",
               "after",
               [if_vsn: nil]
             ])
  end

  test "observers spread across same-region voters using stable hashed routing" do
    peers = TestCluster.start_peers(4)
    on_exit(fn -> TestCluster.stop_peers(peers) end)

    [{_, voter_a}, {_, voter_b}, {_, observer_a}, {_, observer_b}] = peers
    ekv_name = unique_name(:observer_spread)
    on_exit(fn -> cleanup_data(peers, ekv_name) end)

    for {node, node_id} <- [{voter_a, "v1"}, {voter_b, "v2"}] do
      TestCluster.rpc!(node, File, :rm_rf!, [data_dir(node, ekv_name)])

      {:ok, _pid} =
        TestCluster.start_ekv(
          node,
          name: ekv_name,
          data_dir: data_dir(node, ekv_name),
          shards: 1,
          log: false,
          region: "iad",
          cluster_size: 2,
          node_id: node_id,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )
    end

    candidates = Enum.sort_by([voter_a, voter_b], &Atom.to_string/1)

    choose_backend = fn observer_id ->
      seed = {ekv_name, observer_id}

      Enum.min_by(candidates, fn candidate ->
        {:erlang.phash2({seed, candidate}), Atom.to_string(candidate)}
      end)
    end

    {observer_a_id, expected_a, observer_b_id, expected_b} =
      1..20
      |> Stream.map(fn index -> "obs#{index}" end)
      |> Enum.find_value(fn left_id ->
        left_backend = choose_backend.(left_id)

        Enum.find_value(1..20, fn right_index ->
          right_id = "obs#{100 + right_index}"
          right_backend = choose_backend.(right_id)

          if left_backend != right_backend do
            {left_id, left_backend, right_id, right_backend}
          end
        end)
      end)

    assert observer_a_id
    assert expected_a != expected_b

    for {node, node_id} <- [{observer_a, observer_a_id}, {observer_b, observer_b_id}] do
      {:ok, _observer_pid} =
        TestCluster.start_ekv(
          node,
          name: ekv_name,
          mode: :observer,
          data_dir: data_dir(node, ekv_name),
          shards: 1,
          log: false,
          region: "ord",
          region_routing: ["iad"],
          cluster_size: 2,
          node_id: node_id,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )
    end

    TestCluster.assert_eventually(fn ->
      TestCluster.rpc!(observer_a, EKV, :info, [ekv_name]).current_backend == expected_a
    end)

    TestCluster.assert_eventually(fn ->
      TestCluster.rpc!(observer_b, EKV, :info, [ekv_name]).current_backend == expected_b
    end)
  end
end
