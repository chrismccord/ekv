defmodule Bench.MixProject do
  use Mix.Project

  def project do
    [
      app: :bench,
      version: "0.1.0",
      elixir: "~> 1.19",
      start_permanent: false,
      consolidate_protocols: true,
      deps: deps()
    ]
  end

  def application do
    [extra_applications: [:logger]]
  end

  defp deps do
    [{:ekv, path: "../../"}]
  end
end
