defmodule EKV.LinearizabilityReproTest do
  use ExUnit.Case

  @moduletag :capture_log
  @moduletag timeout: 300_000

  @workers 6
  @ops 400
  @cluster_nodes 3
  @attempts 5

  # Require a conclusive positive verdict from every run, not just the absence
  # of a negative verdict (the generator or checker may never have started).
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
          valid?:
            status == 0 and
              Regex.scan(~r/^EKV_JEPSEN_RESULT=.*$/m, output) == [["EKV_JEPSEN_RESULT=true"]],
          output_tail: output_tail(output, 120)
        }
      end)

    invalid = Enum.reject(results, & &1.valid?)

    assert invalid == [],
           """
           expected all Jepsen runs to succeed with a positive verdict, but found #{length(invalid)} non-passing run(s).

           #{Enum.map_join(results, "\n\n", fn r -> "attempt=#{r.attempt} status=#{r.status} history=#{r.history_path}\n#{r.output_tail}" end)}
           """
  end

  defp output_tail(output, max_lines) do
    output
    |> String.split("\n")
    |> Enum.take(-max_lines)
    |> Enum.join("\n")
  end
end
