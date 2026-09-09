# Keep the benchmark's reporting regressions in the normal root CI suite.
Code.require_file("../bench/lib/bench/helpers.ex", __DIR__)

defmodule EKV.BenchmarkReportingTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureIO

  alias Bench.Helpers

  test "only acknowledged read and write successes contribute to successful throughput" do
    summary =
      Helpers.summarize_results([
        {:ok, nil},
        {:ok, 42},
        {:ok, {1, "member"}},
        {:ok, 7, {2, "member"}},
        {:error, :conflict},
        {:error, :unconfirmed},
        {:error, :unconfirmed},
        {:error, :unavailable}
      ])

    assert summary == %{
             attempted: 8,
             ok: 4,
             errors: %{conflict: 1, unconfirmed: 2, unavailable: 1}
           }

    output =
      capture_io(fn -> Helpers.report_operation_throughput("mixed", summary, 2_000_000) end)

    assert output =~ "mixed (successful)\n    total   : 4 ops in 2,000 ms\n    ops/sec : 2"
    assert output =~ "mixed (attempted)\n    total   : 8 ops in 2,000 ms\n    ops/sec : 4"
    assert output =~ "errors[:conflict] : 1"
    assert output =~ "errors[:unconfirmed] : 2"
    assert output =~ "errors[:unavailable] : 1"
  end

  test "fast rejection never appears as successful throughput" do
    summary = Helpers.summarize_results(List.duplicate({:error, :no_quorum}, 100))

    output =
      capture_io(fn -> Helpers.report_operation_throughput("rejected", summary, 1_000) end)

    assert output =~ "rejected (successful)\n    total   : 0 ops in 1 ms\n    ops/sec : 0"
    assert output =~ "rejected (attempted)\n    total   : 100 ops in 1 ms\n    ops/sec : 100,000"
    assert output =~ "errors[:no_quorum] : 100"
  end

  test "empty results and a zero-duration measurement remain defined" do
    summary = Helpers.summarize_results([])
    assert summary == %{attempted: 0, ok: 0, errors: %{}}
    output = capture_io(fn -> Helpers.report_operation_throughput("empty", summary, 0) end)
    assert length(Regex.scan(~r/ops\/sec : 0/, output)) == 2
  end

  test "unexpected response shapes cannot silently count as success" do
    for result <- [:ok, nil, {:unexpected, 7}] do
      assert_raise RuntimeError, ~r/unexpected benchmark result/, fn ->
        Helpers.summarize_results([result])
      end
    end
  end

  test "counter validation accounts for ambiguity without counting it as acknowledged work" do
    summary =
      Helpers.summarize_results([
        {:ok, 1, {1, "a"}},
        {:ok, 2, {2, "a"}},
        {:error, :conflict},
        {:error, :unconfirmed},
        {:error, :unavailable}
      ])

    for value <- 2..4, do: assert(:ok == Helpers.validate_counter!(value, summary))

    for value <- [nil, 0, 1, 5, 2.0] do
      assert_raise RuntimeError, ~r/outside acknowledged\/ambiguous bounds/, fn ->
        Helpers.validate_counter!(value, summary)
      end
    end
  end

  test "an absent counter is valid only if no increments were acknowledged" do
    summary = Helpers.summarize_results([{:error, :conflict}])
    assert :ok == Helpers.validate_counter!(nil, summary)

    assert_raise RuntimeError, ~r/outside acknowledged\/ambiguous bounds/, fn ->
      Helpers.validate_counter!(1, summary)
    end
  end

  test "unknown errors cannot justify a counter bound" do
    summary = Helpers.summarize_results([{:error, :unexpected}])

    assert_raise RuntimeError, ~r/cannot validate counter/, fn ->
      Helpers.validate_counter!(nil, summary)
    end
  end
end
