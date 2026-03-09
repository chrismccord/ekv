defmodule BenchWeb.BenchRunner do
  @moduledoc false

  @stream_poll_ms 250

  def run_cas_stream(opts, notify_pid, run_ref) do
    {:ok, io} = StringIO.open("")

    task =
      Task.async(fn ->
        original_gl = :erlang.group_leader()
        Process.group_leader(self(), io)

        try do
          Bench.CAS.run(opts)
        after
          Process.group_leader(self(), original_gl)
        end
      end)

    {result, _sent_bytes} = await_with_stream(task, io, notify_pid, run_ref, 0)

    {_input, output} = StringIO.contents(io)
    StringIO.close(io)

    case result do
      :ok -> {:ok, output}
      {:exit, reason} -> {:error, "Benchmark failed: #{inspect(reason, pretty: true)}", output}
    end
  end

  defp await_with_stream(task, io, notify_pid, run_ref, sent_bytes) do
    sent_bytes = emit_new_output(io, notify_pid, run_ref, sent_bytes)

    case Task.yield(task, @stream_poll_ms) do
      {:ok, _value} ->
        {:ok, emit_new_output(io, notify_pid, run_ref, sent_bytes)}

      {:exit, reason} ->
        {{:exit, reason}, emit_new_output(io, notify_pid, run_ref, sent_bytes)}

      nil ->
        await_with_stream(task, io, notify_pid, run_ref, sent_bytes)
    end
  end

  defp emit_new_output(io, notify_pid, run_ref, sent_bytes) do
    {_input, output} = StringIO.contents(io)
    total_bytes = byte_size(output)

    if total_bytes > sent_bytes do
      chunk = binary_part(output, sent_bytes, total_bytes - sent_bytes)
      send(notify_pid, {:bench_progress, run_ref, chunk})
      total_bytes
    else
      sent_bytes
    end
  end
end
