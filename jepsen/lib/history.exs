defmodule EkvJepsen.History do
  @moduledoc false

  def start_link do
    Agent.start_link(fn -> {0, System.monotonic_time(:nanosecond), []} end)
  end

  def log(history, event) do
    Agent.update(history, fn {index, start, events} ->
      event =
        event
        |> Map.put(:index, index)
        |> Map.put(:time, System.monotonic_time(:nanosecond) - start)

      {index + 1, start, [event | events]}
    end)
  end

  def events(history), do: Agent.get(history, fn {_, _, events} -> Enum.reverse(events) end)

  def write!(history, path) do
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, "[\n" <> Enum.map_join(events(history), "\n", &edn/1) <> "\n]\n")
  end

  def edn(nil), do: "nil"
  def edn(true), do: "true"
  def edn(false), do: "false"
  def edn(value) when is_integer(value), do: Integer.to_string(value)
  def edn(value) when is_atom(value), do: ":" <> Atom.to_string(value)
  def edn(value) when is_tuple(value), do: value |> Tuple.to_list() |> edn()
  def edn(value) when is_list(value), do: "[" <> Enum.map_join(value, " ", &edn/1) <> "]"

  def edn(value) when is_map(value) do
    "{" <> Enum.map_join(value, ", ", fn {k, v} -> edn(k) <> " " <> edn(v) end) <> "}"
  end

  def edn(value) when is_binary(value) do
    escaped =
      value
      |> String.replace("\\", "\\\\")
      |> String.replace("\"", "\\\"")
      |> String.replace("\n", "\\n")
      |> String.replace("\r", "\\r")
      |> String.replace("\t", "\\t")

    "\"" <> escaped <> "\""
  end
end
