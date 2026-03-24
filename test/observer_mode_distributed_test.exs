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
end
