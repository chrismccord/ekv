defmodule EKV.MixProject do
  use Mix.Project

  @version "0.4.7"
  # OTP 26+ supports NIF ABI 2.17. Build it once instead of having OTP
  # matrix jobs overwrite the same release assets.
  @nif_versions ["2.17"]
  @compilers %{
    {:unix, :linux} => %{
      "x86_64-linux-gnu" => "x86_64-linux-gnu-",
      "aarch64-linux-gnu" => "aarch64-linux-gnu-",
      "x86_64-linux-musl" => "x86_64-linux-musl-",
      "aarch64-linux-musl" => "aarch64-linux-musl-"
    },
    {:unix, :darwin} => %{
      "x86_64-apple-darwin" =>
        {"gcc", "g++", "<%= cc %> -arch x86_64", "<%= cxx %> -arch x86_64"},
      "aarch64-apple-darwin" => {"gcc", "g++", "<%= cc %> -arch arm64", "<%= cxx %> -arch arm64"}
    }
  }

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
      aliases: [
        "ekv.verify_checksums": [&verify_checksums!/1],
        "hex.build": [&verify_checksums!/1, "hex.build"],
        "hex.publish": [&verify_checksums!/1, "hex.publish"]
      ],
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
        make_precompiler_nif_versions: [versions: @nif_versions],
        make_precompiler_priv_paths: ["ekv_sqlite3_nif.*"],
        cc_precompiler: [cleanup: "clean", compilers: @compilers]
      ]
    end
  end

  defp verify_checksums!(_args) do
    unless File.regular?("checksum.exs") do
      Mix.raise("Missing checksum.exs. Generate release checksums before packaging EKV.")
    end

    {checksums, _} = Code.eval_file("checksum.exs")

    unless is_map(checksums) do
      Mix.raise("checksum.exs must contain a map of release artifact checksums.")
    end

    expected =
      for {_os, compilers} <- @compilers,
          target <- Map.keys(compilers),
          nif <- @nif_versions do
        "ekv-nif-#{nif}-#{target}-#{@version}.tar.gz"
      end

    missing = expected -- Map.keys(checksums)
    unexpected = Map.keys(checksums) -- expected

    invalid =
      for {file, checksum} <- checksums,
          not (is_binary(checksum) and Regex.match?(~r/\Asha256:[0-9a-f]{64}\z/, checksum)),
          do: file

    if missing != [] or unexpected != [] or invalid != [] do
      Mix.raise("""
      Invalid release checksums for EKV #{@version}.
      Missing: #{inspect(Enum.sort(missing))}
      Unexpected (possibly stale): #{inspect(Enum.sort(unexpected))}
      Invalid SHA-256: #{inspect(Enum.sort(invalid))}
      Run MIX_ENV=prod mix elixir_make.checksum --all after every precompile job succeeds.
      """)
    end

    Mix.shell().info("Verified checksums for all #{length(expected)} EKV #{@version} artifacts.")
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
    Direct member replication across Erlang nodes with delta sync.
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
        "LICENSE.md",
        "checksum.exs"
      ]
    ]
  end

  defp deps do
    [
      {:elixir_make, "~> 0.9", runtime: false},
      {:cc_precompiler, "~> 0.1", runtime: false},
      {:stream_data, "~> 1.2", only: :test},
      {:ex_doc, "~> 0.38", only: :docs}
    ]
  end
end
