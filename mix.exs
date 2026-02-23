defmodule EKV.MixProject do
  use Mix.Project

  @version "0.1.2"

  def project do
    [
      app: :ekv,
      version: @version,
      elixir: "~> 1.19",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      compilers: [:elixir_make] ++ Mix.compilers(),
      make_targets: ["all"],
      make_clean: ["clean"],
      description: description(),
      package: package(),
      deps: deps()
    ] ++ precompiler_config()
  end

  defp precompiler_config do
    if System.get_env("EKV_BUILD") in ["1", "true"] or Mix.env() in [:dev, :test] do
      # Build NIF from source — skip cc_precompiler entirely
      []
    else
      [
        make_precompiler: {:nif, CCPrecompiler},
        make_precompiler_url:
          "https://github.com/chrismccord/ekv/releases/download/v#{@version}/@{artefact_filename}",
        make_precompiler_filename: "ekv_sqlite3_nif",
        make_precompiler_nif_versions: [versions: ["2.16", "2.17"]],
        make_precompiler_priv_paths: ["ekv_sqlite3_nif.*"],
        cc_precompiler: [cleanup: "clean"]
      ]
    end
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  def application do
    [
      extra_applications: [:logger],
      mod: {EKV.Application, []}
    ]
  end

  defp description do
    """
    Eventually consistent durable KV store for Elixir with zero runtime dependencies.
    Data survives node restarts, node death, and network partitions.
    Peer-to-peer replication across Erlang nodes with delta sync.
    """
  end

  defp package do
    [
      name: "ekv",
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/chrismccord/ekv"},
      files: [
        "lib",
        "c_src/ekv_sqlite3_nif.c",
        "c_src/sqlite3.c",
        "c_src/sqlite3.h",
        "Makefile",
        "mix.exs",
        "README.md",
        "LICENSE",
        "checksum-ekv.exs"
      ]
    ]
  end

  defp deps do
    [
      {:elixir_make, "~> 0.9", runtime: false},
      {:cc_precompiler, "~> 0.1", runtime: false},
      {:ex_doc, "~> 0.38", only: :docs}
    ]
  end
end
