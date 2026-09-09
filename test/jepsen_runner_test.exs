defmodule EKV.JepsenRunnerTest do
  use ExUnit.Case, async: true

  @jepsen Path.expand("../jepsen", __DIR__)

  @tag :tmp_dir
  test "all runner gates require one positive verdict and a zero exit", %{tmp_dir: dir} do
    for file <- ~w(run_scenario.sh run_preprod_gates.sh run_lock_matrix.sh result.sh) do
      File.cp!(Path.join(@jepsen, file), Path.join(dir, file))
    end

    bin = Path.join(dir, "bin")
    File.mkdir_p!(bin)
    lein = Path.join(bin, "lein")
    File.write!(lein, "#!/bin/sh\nprintf '%s\\n' \"$FAKE_OUTPUT\"\nexit \"$FAKE_EXIT\"\n")
    File.chmod!(lein, 0o755)

    cases = [
      {"EKV_JEPSEN_RESULT=true", 0, true},
      {"EKV_JEPSEN_RESULT=false", 0, false},
      {"EKV_JEPSEN_RESULT=unknown", 0, false},
      {"EKV_JEPSEN_RESULT=true", 42, false},
      {"startup failed", 42, false},
      {"  valid?:       true", 0, false},
      {"EKV_JEPSEN_RESULT=banana", 0, false},
      {"EKV_JEPSEN_RESULT=true\nEKV_JEPSEN_RESULT=true", 0, false}
    ]

    for {{output, status, passes?}, i} <- Enum.with_index(cases) do
      env = [
        {"PATH", bin <> ":" <> System.fetch_env!("PATH")},
        {"FAKE_OUTPUT", output},
        {"FAKE_EXIT", Integer.to_string(status)}
      ]

      for {script, args} <- [
            {"run_preprod_gates.sh", ["1", "1", "case-#{i}"]},
            {"run_lock_matrix.sh", ["1", "case-#{i}"]}
          ] do
        {log, exit} =
          System.cmd("bash", [script | args], cd: dir, env: env, stderr_to_stdout: true)

        assert exit == 0 == passes?, "#{script}: #{inspect({output, status})}\n#{log}"
      end

      {log, exit} =
        System.cmd(
          "elixir",
          [
            "-e",
            "ExUnit.start(); Code.require_file(#{inspect(Path.join(@jepsen, "linearizability_repro_test.exs"))})"
          ],
          env: env,
          stderr_to_stdout: true
        )

      assert exit == 0 == passes?, "reproduction test: #{inspect({output, status})}\n#{log}"
    end
  end
end
