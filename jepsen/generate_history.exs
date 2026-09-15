for file <- ~w(history workload cluster) do
  Code.require_file("lib/#{file}.exs", __DIR__)
end

defmodule EkvJepsen.HistoryGen do
  @moduledoc false
  alias EkvJepsen.{Cluster, History, Workload}

  def run(path, workers, ops, count, mode, profile, seed)
      when workers > 0 and ops > 0 and count >= 3 and profile in [:register, :lock] do
    unless mode in Cluster.modes(), do: raise("invalid fault mode")
    path = Path.expand(path)
    File.mkdir_p!(Path.dirname(path))
    {:ok, history} = History.start_link()
    {:ok, tasks} = Task.Supervisor.start_link()
    data_dir = path <> ".data-#{System.system_time(:microsecond)}"
    cluster = Cluster.start_link(count, data_dir)
    nodes = Cluster.nodes(cluster)
    IO.puts("peer nodes: #{inspect(nodes)}; retained data: #{data_dir}")

    try do
      History.log(history, %{
        process: :nemesis,
        type: :info,
        f: :config,
        value: %{format: 2, nodes: Enum.map(nodes, &to_string/1), profile: profile, mode: mode}
      })

      setup = Workload.context(nodes, history, :setup)
      # Fresh, uniquely allocated databases, not a swallowed unsupported delete.
      for node <- nodes, do: {:ok, nil} = Workload.read(setup, node)

      done = :atomics.new(1, [])
      if mode == :none, do: :atomics.put(done, 1, 1)
      workload = Workload.context(nodes, history, :workload)

      workers_tasks =
        for worker <- 0..(workers - 1) do
          n = div(ops, workers) + if(worker < rem(ops, workers), do: 1, else: 0)

          Task.Supervisor.async_nolink(tasks, fn ->
            Workload.run(workload, worker, n, profile, seed, done)
          end)
        end

      fault_task =
        Task.Supervisor.async_nolink(tasks, fn ->
          Cluster.run_faults(cluster, history, mode, seed)
          :atomics.put(done, 1, 1)
        end)

      :ok = Task.await(fault_task, 120_000)
      states = Enum.map(workers_tasks, &Task.await(&1, 120_000))
      Cluster.heal(cluster)
      recovery = Workload.context(nodes, history, :recovery)

      # Observe the last concurrent writes BEFORE a new write can mask data loss.
      for node <- nodes, do: {:ok, _} = Workload.read(recovery, node)
      if profile == :lock, do: Enum.each(states, &Workload.drain(recovery, &1))

      # Bounded fault-free progress is a requirement, not a linearizability
      # assumption. Run a full lifecycle and observe it through every member.
      case profile do
        :register ->
          for node <- nodes, do: {:ok, _} = Workload.write(recovery, node)

        :lock ->
          state = Workload.cas(recovery, Workload.state("recovery"), :acquire)
          false = state.pending
          state = Workload.cas(recovery, state, :renew)
          false = state.pending
          for node <- nodes, do: {:ok, _} = Workload.read(recovery, node)
          state = Workload.cas(recovery, state, :release)
          false = state.pending
      end

      for node <- nodes, do: {:ok, _} = Workload.read(recovery, node)
      History.log(history, %{process: :nemesis, type: :info, f: :recovery_complete, value: true})
    after
      # Preserve even partial histories on an error. Stop and join all children
      # before serializing so no fault can restart a member during teardown.
      Supervisor.stop(tasks)
      History.write!(history, path)
      Cluster.stop(cluster)
      Agent.stop(history)
    end

    IO.puts("history written: #{path}")
  end
end

defaults = ["jepsen/results/history.edn", "4", "200", "3", "none", "register", "1"]
args = System.argv()

if length(args) > length(defaults),
  do: raise("expected history workers ops nodes mode profile seed")

[path, workers, ops, nodes, mode, profile, seed] = args ++ Enum.drop(defaults, length(args))

EkvJepsen.HistoryGen.run(
  path,
  String.to_integer(workers),
  String.to_integer(ops),
  String.to_integer(nodes),
  String.to_existing_atom(mode),
  String.to_existing_atom(profile),
  String.to_integer(seed)
)
