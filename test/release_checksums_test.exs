defmodule EKV.ReleaseChecksumsTest do
  use ExUnit.Case, async: true

  @moduletag :tmp_dir
  @targets ~w(
    aarch64-apple-darwin x86_64-apple-darwin
    aarch64-linux-gnu x86_64-linux-gnu
    aarch64-linux-musl x86_64-linux-musl
  )

  setup %{tmp_dir: dir} do
    # Exercise the real Mix aliases without modifying the checkout's manifest,
    # compiling the NIF, or contacting Hex/GitHub.
    File.cp!("mix.exs", Path.join(dir, "mix.exs"))
    version = Mix.Project.config()[:version]

    checksums =
      Map.new(@targets, fn target ->
        {"ekv-nif-2.17-#{target}-#{version}.tar.gz", "sha256:" <> String.duplicate("a", 64)}
      end)

    %{checksums: checksums}
  end

  test "accepts the full current-version matrix", %{tmp_dir: dir, checksums: checksums} do
    write_checksums(dir, checksums)
    assert {output, 0} = run_mix(dir, "ekv.verify_checksums")
    assert output =~ "Verified checksums for all 6"
  end

  test "Hex tarball contains the verified manifest", %{tmp_dir: dir, checksums: checksums} do
    write_checksums(dir, checksums)

    # Minimal package fixtures: test packaging, not compilation or publication.
    for path <- [
          "lib/ekv.ex",
          "c_src/ekv_sqlite3_nif.c",
          "c_src/sqlite3.c",
          "c_src/sqlite3.h",
          "Makefile",
          "README.md",
          "LICENSE.md"
        ] do
      file = Path.join(dir, path)
      File.mkdir_p!(Path.dirname(file))
      File.write!(file, "packaging fixture\n")
    end

    assert {_output, 0} = run_mix(dir, "hex.build")
    tarball = Path.join(dir, "ekv-#{Mix.Project.config()[:version]}.tar")
    assert {:ok, outer} = :erl_tar.extract(String.to_charlist(tarball), [:memory])
    {~c"contents.tar.gz", contents} = List.keyfind(outer, ~c"contents.tar.gz", 0)
    assert {:ok, files} = :erl_tar.extract({:binary, contents}, [:compressed, :memory])
    assert {~c"checksum.exs", manifest} = List.keyfind(files, ~c"checksum.exs", 0)
    assert manifest == File.read!(Path.join(dir, "checksum.exs"))
  end

  test "rejects a missing manifest before building or publishing", %{tmp_dir: dir} do
    for task <- ["ekv.verify_checksums", "hex.build", "hex.publish"] do
      assert {output, status} = run_mix(dir, task)
      assert status != 0
      assert output =~ "Missing checksum.exs"
    end
  end

  test "rejects stale checksums even if the file exists", %{tmp_dir: dir, checksums: checksums} do
    stale =
      Map.new(checksums, fn {file, hash} ->
        {String.replace(file, Mix.Project.config()[:version], "0.0.0"), hash}
      end)

    write_checksums(dir, stale)

    for task <- ["ekv.verify_checksums", "hex.build", "hex.publish"] do
      assert {output, status} = run_mix(dir, task)
      assert status != 0
      assert output =~ "Unexpected (possibly stale)"
      assert output =~ "0.0.0.tar.gz"
    end
  end

  test "rejects empty and partial downloads", %{tmp_dir: dir, checksums: checksums} do
    for partial <- [%{}, Map.delete(checksums, Enum.at(Map.keys(checksums), 0))] do
      write_checksums(dir, partial)
      assert {output, status} = run_mix(dir, "ekv.verify_checksums")
      assert status != 0
      assert output =~ "Missing:"
      refute output =~ "Missing: []"
    end
  end

  test "rejects malformed hashes and non-map manifests", %{tmp_dir: dir, checksums: checksums} do
    key = Enum.at(Map.keys(checksums), 0)

    for invalid <- [Map.put(checksums, key, "sha256:bad"), Map.put(checksums, key, nil), []] do
      write_checksums(dir, invalid)
      assert {_output, status} = run_mix(dir, "ekv.verify_checksums")
      assert status != 0
    end
  end

  defp write_checksums(dir, checksums) do
    File.write!(Path.join(dir, "checksum.exs"), inspect(checksums))
  end

  defp run_mix(dir, task) do
    System.cmd("mix", [task],
      cd: dir,
      env: [{"MIX_ENV", "prod"}, {"ERL_FLAGS", "+S 2:2"}],
      stderr_to_stdout: true
    )
  end
end
