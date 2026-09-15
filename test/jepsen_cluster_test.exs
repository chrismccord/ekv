Code.require_file("../jepsen/lib/history.exs", __DIR__)
Code.require_file("../jepsen/lib/cluster.exs", __DIR__)

defmodule EKV.JepsenClusterTest do
  use ExUnit.Case
  alias EkvJepsen.{Cluster, History}

  @moduletag timeout: 120_000
  @tag :tmp_dir
  test "partition blocks implicit reconnects and VM crashes preserve member identity and data", %{
    tmp_dir: dir
  } do
    cluster = Cluster.start_link(3, Path.join(dir, "data"))
    {:ok, history} = History.start_link()

    try do
      [a, b, _] = nodes = Cluster.nodes(cluster)

      {:ok, vsn} =
        :erpc.call(a, EKV, :put, [:jepsen_kv, "crash-survivor", "value", [consistent: true]])

      old = Cluster.members(cluster)
      Cluster.partition(cluster, a)
      assert false == :erpc.call(a, Node, :connect, [b])
      assert false == :erpc.call(b, Node, :connect, [a])
      assert :ok == Cluster.assert_partition!(cluster, a)
      Cluster.heal(cluster)
      Cluster.run_faults(cluster, history, :partition_crash, 0)

      assert nodes == Cluster.nodes(cluster)
      current = Cluster.members(cluster)
      assert Enum.count(Enum.zip(old, current), fn {x, y} -> x.pid != y.pid end) == 2
      assert Enum.map(old, & &1.opts) == Enum.map(current, & &1.opts)

      for node <- nodes do
        assert "value" ==
                 :erpc.call(node, EKV, :get, [:jepsen_kv, "crash-survivor", [consistent: true]])

        assert {"value", ^vsn} = :erpc.call(node, EKV, :lookup, [:jepsen_kv, "crash-survivor"])
      end

      assert Enum.map(History.events(history), & &1.f) == [
               :fault_start,
               :fault_healing,
               :fault_end,
               :fault_start,
               :fault_healing,
               :fault_end
             ]
    after
      Cluster.stop(cluster)
      Agent.stop(history)
    end
  end
end
