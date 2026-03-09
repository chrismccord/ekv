defmodule EKV.LinearizabilityReproTest do
  use ExUnit.Case

  @moduletag :capture_log
  @moduletag timeout: 300_000

  @workers 6
  @ops 400
  @cluster_nodes 3
  @attempts 5

  # Intention: enforce linearizable behavior under concurrent read/write CAS load.
  # This is a regression test and should fail until the underlying bug is fixed.
  test "jepsen checker reports valid history under concurrent CAS load" do
    unless System.find_executable("lein") do
      flunk("lein executable not found; cannot run Jepsen reproduction test")
    end

    repo_root = Path.expand("..", __DIR__)
    jepsen_dir = Path.join(repo_root, "jepsen")

    results =
      Enum.map(1..@attempts, fn attempt ->
        history_path =
          Path.join(
            System.tmp_dir!(),
            "ekv_jepsen_repro_#{System.unique_integer([:positive])}_#{attempt}.edn"
          )

        args = [
          "run",
          history_path,
          Integer.to_string(@workers),
          Integer.to_string(@ops),
          Integer.to_string(@cluster_nodes)
        ]

        {output, status} = System.cmd("lein", args, cd: jepsen_dir, stderr_to_stdout: true)

        %{
          attempt: attempt,
          status: status,
          history_path: history_path,
          invalid?: String.contains?(output, "valid?:       false"),
          output_tail: output_tail(output, 120)
        }
      end)

    invalid = Enum.filter(results, & &1.invalid?)

    assert invalid == [],
           """
           expected all Jepsen runs to be linearizable (valid?: true), but found #{length(invalid)} invalid run(s).

           #{Enum.map_join(results, "\n\n", fn r ->
             "attempt=#{r.attempt} status=#{r.status} history=#{r.history_path}\n#{r.output_tail}"
           end)}
           """
  end

  defp output_tail(output, max_lines) do
    output
    |> String.split("\n")
    |> Enum.take(-max_lines)
    |> Enum.join("\n")
  end
end
