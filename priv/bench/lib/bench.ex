defmodule Bench do
  def main(args) do
    case args do
      ["local"] ->
        Bench.Local.run()

      ["distributed"] ->
        Bench.Distributed.run()

      _ ->
        IO.puts("""
        Usage: Bench.main(["local" | "distributed"])

          local        — Run single-node benchmarks
          distributed  — Coordinator: connects to replicas, drives benchmarks
        """)
    end
  end
end
