defmodule EKV.ScanTombstoneTest do
  use ExUnit.Case, async: true

  @moduletag :tmp_dir
  @moduletag :capture_log

  for mode <- [:member, :observer] do
    describe "#{mode} scans" do
      setup %{tmp_dir: tmp_dir} do
        start_store(tmp_dir, unquote(mode))
      end

      test "scan excludes tombstones newer than the scan clock", %{name: name} do
        assert Enum.map(EKV.scan(name, "scan/"), fn {key, value, _vsn} -> {key, value} end) ==
                 [{"scan/b", nil}, {"scan/c", "live"}, {"scan/f", "last"}]
      end

      test "keys excludes tombstones newer than the scan clock", %{name: name} do
        assert Enum.map(EKV.keys(name, "scan/"), fn {key, _vsn} -> key end) ==
                 ["scan/b", "scan/c", "scan/f"]
      end

      test "paged scans skip tombstones on first and subsequent pages", %{name: name} do
        assert Enum.map(pages(name, :__scan_page__), fn {key, value, _vsn} -> {key, value} end) ==
                 [{"scan/b", nil}, {"scan/c", "live"}, {"scan/f", "last"}]
      end

      test "paged keys skip tombstones on first and subsequent pages", %{name: name} do
        assert Enum.map(pages(name, :__keys_page__), fn {key, _vsn} -> key end) ==
                 ["scan/b", "scan/c", "scan/f"]
      end
    end
  end

  describe "store queries" do
    setup %{tmp_dir: tmp_dir} do
      start_store(tmp_dir, :member)
    end

    test "prefix scans exclude tombstones newer than the supplied clock", %{db: db, now: now} do
      assert Enum.map(EKV.Store.scan_prefix(db, "scan/", now), fn {key, value} ->
               {key, :erlang.binary_to_term(value)}
             end) == [{"scan/b", nil}, {"scan/c", "live"}, {"scan/f", "last"}]
    end

    test "prefix keys exclude tombstones newer than the supplied clock", %{db: db, now: now} do
      assert EKV.Store.scan_prefix_keys(db, "scan/", now) == ["scan/b", "scan/c", "scan/f"]
    end

    test "live counts exclude tombstones newer than the supplied clock", %{db: db, now: now} do
      assert EKV.Store.count_live(db, now) == 4
    end
  end

  defp start_store(tmp_dir, mode) do
    name = :"scan_tombstone_#{System.unique_integer([:positive])}"

    opts = [
      name: name,
      mode: mode,
      data_dir: tmp_dir,
      shards: 1,
      log: false,
      gc_interval: :timer.hours(1)
    ]

    opts = if mode == :observer, do: Keyword.put(opts, :region_routing, ["local"]), else: opts
    start_supervised!({EKV, opts})

    %{db: db, stmts: stmts} = :sys.get_state(EKV.Replica.shard_name(name, 0))
    now = System.system_time(:nanosecond)
    deleted_at = now + System.convert_time_unit(3_600, :second, :nanosecond)

    # Model a delete committed after a scan sampled its clock but before its
    # SQLite query reads the row. Clock skew can produce the same ordering.
    for {key, value, expires_at, tombstone} <- [
          {"scan/a", nil, nil, deleted_at},
          {"scan/b", :erlang.term_to_binary(nil), nil, nil},
          {"scan/c", :erlang.term_to_binary("live"), nil, nil},
          {"scan/d", nil, nil, deleted_at},
          {"scan/e", :erlang.term_to_binary("expired"), now - 1, nil},
          {"scan/f", :erlang.term_to_binary("last"), nil, nil},
          {"scan/g", nil, nil, deleted_at},
          {"unrelated/key", :erlang.term_to_binary("outside prefix"), nil, nil}
        ] do
      assert {:ok, true, _, _} =
               EKV.Store.write_entry(
                 db,
                 stmts.kv_upsert,
                 stmts.keyref_upsert,
                 stmts.oplog_insert,
                 key,
                 value,
                 tombstone || now,
                 "scan-test-origin",
                 expires_at,
                 tombstone
               )
    end

    assert EKV.lookup(name, "scan/a") == nil
    %{name: name, db: db, now: now}
  end

  defp pages(name, function, cursor \\ nil) do
    case apply(EKV, function, [name, "scan/", cursor, 1]) do
      {items, :done} -> items
      {items, next_cursor} -> items ++ pages(name, function, next_cursor)
    end
  end
end
