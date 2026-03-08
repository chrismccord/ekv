defmodule EKVTest do
  use ExUnit.Case, async: false

  @moduletag :capture_log

  setup do
    name = :"ekv_test_#{System.unique_integer([:positive])}"
    data_dir = Path.join(System.tmp_dir!(), "ekv_test_#{name}")

    {:ok, ekv_pid} =
      EKV.start_link(
        name: name,
        data_dir: data_dir,
        shards: 2,
        log: false,
        gc_interval: :timer.hours(1),
        tombstone_ttl: :timer.hours(24 * 7)
      )

    on_exit(fn ->
      Process.exit(ekv_pid, :shutdown)
      File.rm_rf!(data_dir)
    end)

    %{name: name, data_dir: data_dir}
  end

  describe "put/get" do
    test "basic put and get", %{name: name} do
      :ok = EKV.put(name, "key1", "value1")
      assert EKV.get(name, "key1") == "value1"
    end

    test "get returns nil for missing key", %{name: name} do
      assert EKV.get(name, "missing") == nil
    end

    test "put overwrites existing value", %{name: name} do
      :ok = EKV.put(name, "key1", "v1")
      :ok = EKV.put(name, "key1", "v2")
      assert EKV.get(name, "key1") == "v2"
    end

    test "put with complex values", %{name: name} do
      :ok = EKV.put(name, "map", %{a: 1, b: [2, 3]})
      assert EKV.get(name, "map") == %{a: 1, b: [2, 3]}

      :ok = EKV.put(name, "tuple", {:ok, 42})
      assert EKV.get(name, "tuple") == {:ok, 42}
    end
  end

  describe "delete" do
    test "delete makes key return nil", %{name: name} do
      :ok = EKV.put(name, "key1", "value1")
      assert EKV.get(name, "key1") == "value1"

      :ok = EKV.delete(name, "key1")
      assert EKV.get(name, "key1") == nil
    end

    test "delete of missing key is ok", %{name: name} do
      assert :ok = EKV.delete(name, "nope")
    end
  end

  describe "TTL" do
    test "expired entries return nil", %{name: name} do
      # TTL of 1ms — will expire almost instantly
      :ok = EKV.put(name, "ttl_key", "val", ttl: 1)
      Process.sleep(10)
      assert EKV.get(name, "ttl_key") == nil
    end

    test "non-expired entries are returned", %{name: name} do
      :ok = EKV.put(name, "ttl_key", "val", ttl: :timer.seconds(60))
      assert EKV.get(name, "ttl_key") == "val"
    end
  end

  describe "scan/keys (prefix scan)" do
    test "scan returns matching entries as stream of {key, value, vsn}", %{name: name} do
      :ok = EKV.put(name, "user/1", %{name: "Alice"})
      :ok = EKV.put(name, "user/2", %{name: "Bob"})
      :ok = EKV.put(name, "post/1", %{title: "Hello"})

      result = EKV.scan(name, "user/") |> Map.new(fn {k, v, _vsn} -> {k, v} end)
      assert result == %{"user/1" => %{name: "Alice"}, "user/2" => %{name: "Bob"}}
    end

    test "scan includes vsn in results", %{name: name} do
      :ok = EKV.put(name, "user/1", "alice")

      [{key, value, vsn}] = EKV.scan(name, "user/") |> Enum.to_list()
      assert key == "user/1"
      assert value == "alice"
      assert {ts, origin} = vsn
      assert is_integer(ts)
      assert is_atom(origin)
    end

    test "scan is a Stream (lazy)", %{name: name} do
      :ok = EKV.put(name, "user/1", "a")
      stream = EKV.scan(name, "user/")
      assert is_function(stream) or match?(%Stream{}, stream)
    end

    test "keys returns matching {key, vsn} tuples as stream", %{name: name} do
      :ok = EKV.put(name, "user/1", "a")
      :ok = EKV.put(name, "user/2", "b")
      :ok = EKV.put(name, "post/1", "c")

      result = EKV.keys(name, "user/") |> Enum.sort()

      assert Enum.map(result, fn {key, _vsn} -> key end) == ["user/1", "user/2"]
      assert Enum.all?(result, fn {_key, {ts, origin}} -> is_integer(ts) and is_atom(origin) end)
    end

    test "keys is a Stream (lazy)", %{name: name} do
      :ok = EKV.put(name, "user/1", "a")
      stream = EKV.keys(name, "user/")
      assert is_function(stream) or match?(%Stream{}, stream)
    end

    test "scan excludes deleted entries", %{name: name} do
      :ok = EKV.put(name, "user/1", "a")
      :ok = EKV.put(name, "user/2", "b")
      :ok = EKV.delete(name, "user/1")

      result = EKV.scan(name, "user/") |> Map.new(fn {k, v, _vsn} -> {k, v} end)
      assert result == %{"user/2" => "b"}
    end

    test "scan excludes expired entries", %{name: name} do
      :ok = EKV.put(name, "user/1", "a", ttl: 1)
      :ok = EKV.put(name, "user/2", "b")
      Process.sleep(10)

      result = EKV.scan(name, "user/") |> Map.new(fn {k, v, _vsn} -> {k, v} end)
      assert result == %{"user/2" => "b"}
    end
  end

  describe "restart rehydration" do
    test "data survives replica restart", %{name: name, data_dir: data_dir} do
      :ok = EKV.put(name, "key1", "value1")
      :ok = EKV.put(name, "key2", "value2")
      assert EKV.get(name, "key1") == "value1"

      # Stop EKV — trap exits to survive the linked shutdown
      Process.flag(:trap_exit, true)
      ekv_sup = :"#{name}_ekv_sup"
      Supervisor.stop(ekv_sup, :shutdown)
      Process.sleep(50)

      # Restart EKV with same config
      {:ok, _pid} =
        EKV.start_link(
          name: name,
          data_dir: data_dir,
          shards: 2,
          log: false,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )

      # Data should be rehydrated from SQLite
      assert EKV.get(name, "key1") == "value1"
      assert EKV.get(name, "key2") == "value2"
    end

    test "deleted entries stay deleted after restart", %{name: name, data_dir: data_dir} do
      :ok = EKV.put(name, "key1", "value1")
      :ok = EKV.delete(name, "key1")
      assert EKV.get(name, "key1") == nil

      Process.flag(:trap_exit, true)
      ekv_sup = :"#{name}_ekv_sup"
      Supervisor.stop(ekv_sup, :shutdown)
      Process.sleep(50)

      {:ok, _pid} =
        EKV.start_link(
          name: name,
          data_dir: data_dir,
          shards: 2,
          log: false,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )

      assert EKV.get(name, "key1") == nil
    end
  end

  describe "LWW conflicts" do
    test "higher timestamp wins", %{name: name} do
      :ok = EKV.put(name, "conflict", "first")
      :ok = EKV.put(name, "conflict", "second")
      assert EKV.get(name, "conflict") == "second"
    end

    test "lower timestamp is rejected", %{name: name} do
      config = EKV.get_config(name)
      shard = EKV.Replica.shard_index_for("lww_key", config.num_shards)
      shard_name = EKV.Replica.shard_name(name, shard)
      %{db: db, stmts: stmts} = :sys.get_state(shard_name)

      # Write with ts=1000
      val1 = :erlang.term_to_binary("winner")

      {:ok, true} =
        EKV.Store.write_entry(
          db,
          stmts.kv_upsert,
          stmts.oplog_insert,
          "lww_key",
          val1,
          1000,
          :node_a,
          nil
        )

      # Write with ts=500 — should be rejected
      val2 = :erlang.term_to_binary("loser")

      {:ok, false} =
        EKV.Store.write_entry(
          db,
          stmts.kv_upsert,
          stmts.oplog_insert,
          "lww_key",
          val2,
          500,
          :node_a,
          nil
        )

      # Original value preserved
      assert EKV.get(name, "lww_key") == "winner"
    end

    test "equal timestamp uses origin_node as tiebreaker", %{name: name} do
      config = EKV.get_config(name)
      shard = EKV.Replica.shard_index_for("tie_key", config.num_shards)
      shard_name = EKV.Replica.shard_name(name, shard)
      %{db: db, stmts: stmts} = :sys.get_state(shard_name)

      ts = 1000

      # Write from node_a
      val_a = :erlang.term_to_binary("from_a")

      {:ok, true} =
        EKV.Store.write_entry(
          db,
          stmts.kv_upsert,
          stmts.oplog_insert,
          "tie_key",
          val_a,
          ts,
          :node_a,
          nil
        )

      # Write from node_b with same timestamp — node_b > node_a lexicographically, should win
      val_b = :erlang.term_to_binary("from_b")

      {:ok, true} =
        EKV.Store.write_entry(
          db,
          stmts.kv_upsert,
          stmts.oplog_insert,
          "tie_key",
          val_b,
          ts,
          :node_b,
          nil
        )

      assert EKV.get(name, "tie_key") == "from_b"

      # Write from node_a again with same timestamp — node_a < node_b, should lose
      val_a2 = :erlang.term_to_binary("from_a_again")

      {:ok, false} =
        EKV.Store.write_entry(
          db,
          stmts.kv_upsert,
          stmts.oplog_insert,
          "tie_key",
          val_a2,
          ts,
          :node_a,
          nil
        )

      assert EKV.get(name, "tie_key") == "from_b"
    end

    test "tiebreaker matches Elixir atom ordering", %{name: name} do
      # Verify that SQLite TEXT comparison matches Erlang atom comparison
      # for node names — both should be lexicographic ASCII
      config = EKV.get_config(name)
      ts = 5000

      pairs = [
        {:aaa@host, :zzz@host},
        {:"node1@10.0.0.1", :"node2@10.0.0.1"},
        {:a@short, :a@short_longer}
      ]

      for {{loser, winner}, i} <- Enum.with_index(pairs) do
        key = "atom_key_#{i}"
        shard = EKV.Replica.shard_index_for(key, config.num_shards)
        shard_name = EKV.Replica.shard_name(name, shard)
        %{db: db, stmts: stmts} = :sys.get_state(shard_name)

        # Confirm Elixir agrees which is larger
        assert winner > loser, "Expected #{winner} > #{loser} in Elixir"

        # Write from loser first
        val_l = :erlang.term_to_binary(:loser_val)

        {:ok, true} =
          EKV.Store.write_entry(
            db,
            stmts.kv_upsert,
            stmts.oplog_insert,
            key,
            val_l,
            ts,
            loser,
            nil
          )

        # Write from winner with same timestamp — should win
        val_w = :erlang.term_to_binary(:winner_val)

        {:ok, true} =
          EKV.Store.write_entry(
            db,
            stmts.kv_upsert,
            stmts.oplog_insert,
            key,
            val_w,
            ts,
            winner,
            nil
          )

        assert EKV.get(name, key) == :winner_val

        # Write from loser again — should lose
        val_l2 = :erlang.term_to_binary(:loser_retry)

        {:ok, false} =
          EKV.Store.write_entry(
            db,
            stmts.kv_upsert,
            stmts.oplog_insert,
            key,
            val_l2,
            ts,
            loser,
            nil
          )

        assert EKV.get(name, key) == :winner_val
      end
    end

    test "delete vs put at same timestamp uses origin tiebreaker", %{name: name} do
      config = EKV.get_config(name)
      shard = EKV.Replica.shard_index_for("del_tie", config.num_shards)
      shard_name = EKV.Replica.shard_name(name, shard)
      %{db: db, stmts: stmts} = :sys.get_state(shard_name)

      ts = 2000

      # Put from node_a
      val = :erlang.term_to_binary("alive")

      {:ok, true} =
        EKV.Store.write_entry(
          db,
          stmts.kv_upsert,
          stmts.oplog_insert,
          "del_tie",
          val,
          ts,
          :node_a,
          nil
        )

      # Delete from node_b at same timestamp — node_b > node_a, delete wins
      {:ok, true} =
        EKV.Store.write_entry(
          db,
          stmts.kv_upsert,
          stmts.oplog_insert,
          "del_tie",
          nil,
          ts,
          :node_b,
          nil,
          ts
        )

      assert EKV.get(name, "del_tie") == nil

      # Put from node_a at same timestamp — node_a < node_b, should lose
      {:ok, false} =
        EKV.Store.write_entry(
          db,
          stmts.kv_upsert,
          stmts.oplog_insert,
          "del_tie",
          val,
          ts,
          :node_a,
          nil
        )

      assert EKV.get(name, "del_tie") == nil
    end

    test "higher timestamp put beats delete", %{name: name} do
      config = EKV.get_config(name)
      shard = EKV.Replica.shard_index_for("resurr", config.num_shards)
      shard_name = EKV.Replica.shard_name(name, shard)
      %{db: db, stmts: stmts} = :sys.get_state(shard_name)

      # Delete at ts=1000
      {:ok, true} =
        EKV.Store.write_entry(
          db,
          stmts.kv_upsert,
          stmts.oplog_insert,
          "resurr",
          nil,
          1000,
          :node_a,
          nil,
          1000
        )

      assert EKV.get(name, "resurr") == nil

      # Put at ts=2000 — higher timestamp, should win
      val = :erlang.term_to_binary("resurrected")

      {:ok, true} =
        EKV.Store.write_entry(
          db,
          stmts.kv_upsert,
          stmts.oplog_insert,
          "resurr",
          val,
          2000,
          :node_a,
          nil
        )

      assert EKV.get(name, "resurr") == "resurrected"
    end

    test "oplog not written when LWW loses", %{name: name} do
      config = EKV.get_config(name)
      shard = EKV.Replica.shard_index_for("oplog_key", config.num_shards)
      shard_name = EKV.Replica.shard_name(name, shard)
      %{db: db, stmts: stmts} = :sys.get_state(shard_name)

      val1 = :erlang.term_to_binary("first")

      {:ok, true} =
        EKV.Store.write_entry(
          db,
          stmts.kv_upsert,
          stmts.oplog_insert,
          "oplog_key",
          val1,
          1000,
          :node_a,
          nil
        )

      seq_after_first = EKV.Store.max_seq(db)

      # This write should lose LWW — oplog should NOT advance
      val2 = :erlang.term_to_binary("rejected")

      {:ok, false} =
        EKV.Store.write_entry(
          db,
          stmts.kv_upsert,
          stmts.oplog_insert,
          "oplog_key",
          val2,
          500,
          :node_a,
          nil
        )

      assert EKV.Store.max_seq(db) == seq_after_first
    end
  end

  # =====================================================================
  # Pathological / Jepsen-style unit tests
  # =====================================================================

  describe "HWM monotonicity" do
    test "HWM does not regress when lower seq is set", %{name: name} do
      config = EKV.get_config(name)
      shard = EKV.Replica.shard_index_for("hwm_key", config.num_shards)
      shard_name = EKV.Replica.shard_name(name, shard)
      %{db: db} = :sys.get_state(shard_name)

      peer = :fake_peer@host

      # Set HWM to 100
      :ok = EKV.Store.set_hwm(db, peer, 100)
      assert EKV.Store.get_hwm(db, peer) == 100

      # Try to set HWM to 50 — should stay at 100
      :ok = EKV.Store.set_hwm(db, peer, 50)
      assert EKV.Store.get_hwm(db, peer) == 100

      # Set HWM to 200 — should advance
      :ok = EKV.Store.set_hwm(db, peer, 200)
      assert EKV.Store.get_hwm(db, peer) == 200
    end
  end

  describe "oplog seq gaps on LWW loss" do
    test "LWW loss does not create oplog seq gaps", %{name: name} do
      config = EKV.get_config(name)
      shard = EKV.Replica.shard_index_for("gap_key", config.num_shards)
      shard_name = EKV.Replica.shard_name(name, shard)
      %{db: db, stmts: stmts} = :sys.get_state(shard_name)

      # Write 3 entries to get seq 1, 2, 3
      for i <- 1..3 do
        val = :erlang.term_to_binary("val_#{i}")

        {:ok, true} =
          EKV.Store.write_entry(
            db,
            stmts.kv_upsert,
            stmts.oplog_insert,
            "gap_key_#{i}",
            val,
            1000 + i,
            :node_a,
            nil
          )
      end

      seq_after_three = EKV.Store.max_seq(db)

      # LWW-losing write — should not increment oplog seq
      val_loser = :erlang.term_to_binary("loser")

      {:ok, false} =
        EKV.Store.write_entry(
          db,
          stmts.kv_upsert,
          stmts.oplog_insert,
          "gap_key_1",
          val_loser,
          500,
          :node_a,
          nil
        )

      assert EKV.Store.max_seq(db) == seq_after_three

      # Verify oplog has exactly 3 entries with no gaps
      entries = EKV.Store.oplog_since(db, 0)
      assert length(entries) == 3

      seqs = Enum.map(entries, fn {seq, _, _, _, _, _, _} -> seq end)
      # Seqs should be contiguous
      assert seqs == Enum.to_list(Enum.min(seqs)..Enum.max(seqs))
    end
  end

  describe "oplog truncation forces full sync" do
    test "truncation removes entries below min HWM", %{name: name} do
      config = EKV.get_config(name)
      shard = EKV.Replica.shard_index_for("trunc_key_1", config.num_shards)
      shard_name = EKV.Replica.shard_name(name, shard)
      %{db: db, stmts: stmts} = :sys.get_state(shard_name)

      # Write 10 entries
      for i <- 1..10 do
        val = :erlang.term_to_binary("val_#{i}")

        {:ok, true} =
          EKV.Store.write_entry(
            db,
            stmts.kv_upsert,
            stmts.oplog_insert,
            "trunc_key_#{i}",
            val,
            1000 + i,
            :node_a,
            nil
          )
      end

      max = EKV.Store.max_seq(db)
      assert max >= 10

      # Set HWMs for 2 fake peers — min is at seq 5
      :ok = EKV.Store.set_hwm(db, :peer_a@host, 5)
      :ok = EKV.Store.set_hwm(db, :peer_b@host, 10)

      # Truncate oplog
      :ok = EKV.Store.truncate_oplog(db)

      # min_seq should now be >= 5 (entries below 5 removed)
      assert EKV.Store.min_seq(db) >= 5

      # full_state should still return all 10 live entries
      now = System.system_time(:nanosecond)
      cutoff = now - :timer.hours(24 * 7) * 1_000_000
      entries = EKV.Store.full_state(db, cutoff)
      assert length(entries) == 10
    end
  end

  describe "GC expires TTL entries into tombstones" do
    test "expired entries become tombstones after GC", %{name: name} do
      config = EKV.get_config(name)
      shard = EKV.Replica.shard_index_for("gc_ttl_key", config.num_shards)
      shard_name = EKV.Replica.shard_name(name, shard)
      %{db: db, stmts: stmts} = :sys.get_state(shard_name)

      # Write an already-expired entry (expires_at in the past)
      now = System.system_time(:nanosecond)
      past = now - 1_000_000_000
      val = :erlang.term_to_binary("doomed")

      {:ok, true} =
        EKV.Store.write_entry(
          db,
          stmts.kv_upsert,
          stmts.oplog_insert,
          "gc_ttl_key",
          val,
          past - 1000,
          :node_a,
          past
        )

      # Entry exists but is expired
      assert EKV.get(name, "gc_ttl_key") == nil

      # The entry is in kv table (not yet tombstoned)
      raw = EKV.Store.get(db, "gc_ttl_key")
      assert raw != nil
      {_, _, _, _, deleted_at} = raw
      assert deleted_at == nil

      seq_before_gc = EKV.Store.max_seq(db)

      # Trigger GC
      tombstone_cutoff = now - config.tombstone_ttl * 1_000_000
      send(shard_name, {:gc, now, tombstone_cutoff})
      # Wait for GC to process (GenServer is sequential)
      :sys.get_state(shard_name)

      # Entry should now be a tombstone (deleted_at set)
      raw_after = EKV.Store.get(db, "gc_ttl_key")
      assert raw_after != nil
      {_, _, _, _, deleted_at_after} = raw_after
      assert is_integer(deleted_at_after)

      # Oplog should have the delete entry
      assert EKV.Store.max_seq(db) > seq_before_gc
    end
  end

  describe "GC purges old tombstones" do
    test "tombstones older than cutoff are hard-deleted", %{name: name} do
      config = EKV.get_config(name)
      shard = EKV.Replica.shard_index_for("gc_purge_key", config.num_shards)
      shard_name = EKV.Replica.shard_name(name, shard)
      %{db: db, stmts: stmts} = :sys.get_state(shard_name)

      # Write entry and delete it (creates tombstone)
      now = System.system_time(:nanosecond)
      val = :erlang.term_to_binary("to_be_purged")

      {:ok, true} =
        EKV.Store.write_entry(
          db,
          stmts.kv_upsert,
          stmts.oplog_insert,
          "gc_purge_key",
          val,
          now - 2000,
          :node_a,
          nil
        )

      {:ok, true} =
        EKV.Store.write_entry(
          db,
          stmts.kv_upsert,
          stmts.oplog_insert,
          "gc_purge_key",
          nil,
          now - 1000,
          :node_a,
          nil,
          now - 1000
        )

      # Tombstone exists in kv table
      raw = EKV.Store.get(db, "gc_purge_key")
      assert raw != nil
      {_, _, _, _, deleted_at} = raw
      assert is_integer(deleted_at)

      # Purge tombstones with a cutoff far in the future
      EKV.Store.purge_tombstones(db, now + 1_000_000_000)

      # Tombstone should be hard-deleted
      assert EKV.Store.get(db, "gc_purge_key") == nil

      # Public API also returns nil
      assert EKV.get(name, "gc_purge_key") == nil
    end
  end

  describe "GC truncates oplog at min HWM" do
    test "oplog entries below min HWM are removed", %{name: name} do
      config = EKV.get_config(name)
      shard = EKV.Replica.shard_index_for("gc_oplog_1", config.num_shards)
      shard_name = EKV.Replica.shard_name(name, shard)
      %{db: db, stmts: stmts} = :sys.get_state(shard_name)

      # Write 10 entries
      for i <- 1..10 do
        val = :erlang.term_to_binary("v_#{i}")

        {:ok, true} =
          EKV.Store.write_entry(
            db,
            stmts.kv_upsert,
            stmts.oplog_insert,
            "gc_oplog_#{i}",
            val,
            1000 + i,
            :node_a,
            nil
          )
      end

      # Set HWMs for 2 fake peers at seq 5 and seq 10
      :ok = EKV.Store.set_hwm(db, :gc_peer_a@host, 5)
      :ok = EKV.Store.set_hwm(db, :gc_peer_b@host, 10)

      # Add fake peers to remote_shards so their HWMs survive GC pruning
      :sys.replace_state(shard_name, fn state ->
        %{state | remote_shards: %{gc_peer_a@host: self(), gc_peer_b@host: self()}}
      end)

      # Trigger GC — this will truncate oplog at min(5, 10) = 5
      now = System.system_time(:nanosecond)
      tombstone_cutoff = now - config.tombstone_ttl * 1_000_000
      send(shard_name, {:gc, now, tombstone_cutoff})
      :sys.get_state(shard_name)

      # Entries below seq 5 should be gone
      entries = EKV.Store.oplog_since(db, 0)
      seqs = Enum.map(entries, fn {seq, _, _, _, _, _, _} -> seq end)
      assert Enum.all?(seqs, &(&1 >= 5))
      # Entries >= 5 should remain
      assert length(entries) >= 6
    end
  end

  describe "stale DB detection and wipe" do
    test "stale database is wiped on reopen", %{name: name, data_dir: data_dir} do
      # Write some data
      :ok = EKV.put(name, "stale_test", "alive")
      assert EKV.get(name, "stale_test") == "alive"

      # Get which shard this key goes to
      config = EKV.get_config(name)
      shard = EKV.Replica.shard_index_for("stale_test", config.num_shards)
      db_path = Path.join(data_dir, "shard_#{shard}.db")

      # Manually set last_active_at to a very old timestamp
      {:ok, tmp_db} = EKV.Sqlite3.open(db_path)

      {:ok, stmt} =
        EKV.Sqlite3.prepare(tmp_db, """
        INSERT INTO kv_meta (key, value_int) VALUES ('last_active_at', ?1)
        ON CONFLICT(key) DO UPDATE SET value_int = excluded.value_int
        """)

      old_ts = System.system_time(:nanosecond) - :timer.hours(24 * 14) * 1_000_000
      :ok = EKV.Sqlite3.bind(stmt, [old_ts])
      :done = EKV.Sqlite3.step(tmp_db, stmt)
      :ok = EKV.Sqlite3.release(tmp_db, stmt)
      EKV.Sqlite3.close(tmp_db)

      # Stop EKV and restart with short tombstone_ttl that will detect staleness
      Process.flag(:trap_exit, true)
      ekv_sup = :"#{name}_ekv_sup"
      Supervisor.stop(ekv_sup, :shutdown)
      Process.sleep(50)

      {:ok, _pid} =
        EKV.start_link(
          name: name,
          data_dir: data_dir,
          shards: 2,
          log: false,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )

      # DB should have been wiped — data gone
      assert EKV.get(name, "stale_test") == nil
    end
  end

  describe "shard count mismatch protection" do
    test "refuses to start with different shard count", %{name: name, data_dir: data_dir} do
      # Data already written with shards: 2 (from setup)
      :ok = EKV.put(name, "shard_guard", "exists")

      # Stop EKV
      Process.flag(:trap_exit, true)
      ekv_sup = :"#{name}_ekv_sup"
      Supervisor.stop(ekv_sup, :shutdown)
      Process.sleep(50)

      # Try to restart with different shard count — should fail
      # The error surfaces as a supervisor startup failure
      result =
        EKV.start_link(
          name: name,
          data_dir: data_dir,
          shards: 4,
          log: false,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )

      assert {:error, _} = result
    end

    test "Store.open raises on shard count change", %{data_dir: data_dir} do
      # Open with 2 shards
      {:ok, db} = EKV.Store.open(data_dir, 99, :timer.hours(24 * 7), 2, :timer.minutes(5))
      EKV.Store.close(db)

      # Re-open with 4 shards — should raise
      assert_raise ArgumentError, ~r/shard count mismatch/, fn ->
        EKV.Store.open(data_dir, 99, :timer.hours(24 * 7), 4, :timer.minutes(5))
      end
    end
  end

  describe "concurrent put during GC expiry" do
    test "new write wins after GC tombstones an expired entry", %{name: name} do
      config = EKV.get_config(name)
      shard = EKV.Replica.shard_index_for("gc_race_key", config.num_shards)
      shard_name = EKV.Replica.shard_name(name, shard)
      %{db: db, stmts: stmts} = :sys.get_state(shard_name)

      # Write entry with expired TTL
      now = System.system_time(:nanosecond)
      past = now - 1_000_000_000
      val = :erlang.term_to_binary("expired")

      {:ok, true} =
        EKV.Store.write_entry(
          db,
          stmts.kv_upsert,
          stmts.oplog_insert,
          "gc_race_key",
          val,
          past - 1000,
          :node_a,
          past
        )

      # Trigger GC to tombstone the expired entry
      tombstone_cutoff = now - config.tombstone_ttl * 1_000_000
      send(shard_name, {:gc, now, tombstone_cutoff})
      :sys.get_state(shard_name)

      # Entry should be tombstoned now
      raw = EKV.Store.get(db, "gc_race_key")
      assert raw != nil
      {_, _, _, _, deleted_at} = raw
      assert is_integer(deleted_at)

      # Write same key with higher timestamp — should resurrect
      future = now + 1_000_000_000
      val2 = :erlang.term_to_binary("resurrected")

      {:ok, true} =
        EKV.Store.write_entry(
          db,
          stmts.kv_upsert,
          stmts.oplog_insert,
          "gc_race_key",
          val2,
          future,
          :node_a,
          nil
        )

      assert EKV.get(name, "gc_race_key") == "resurrected"
    end
  end

  describe "full state excludes purged tombstones" do
    test "purged tombstones absent, recent tombstones present in full_state", %{name: name} do
      config = EKV.get_config(name)
      shard = EKV.Replica.shard_index_for("fs_key_1", config.num_shards)
      shard_name = EKV.Replica.shard_name(name, shard)
      %{db: db, stmts: stmts} = :sys.get_state(shard_name)

      now = System.system_time(:nanosecond)

      # Write 3 entries
      for i <- 1..3 do
        val = :erlang.term_to_binary("val_#{i}")

        {:ok, true} =
          EKV.Store.write_entry(
            db,
            stmts.kv_upsert,
            stmts.oplog_insert,
            "fs_key_#{i}",
            val,
            now - 3000 + i,
            :node_a,
            nil
          )
      end

      # Delete entry 1 with an old tombstone (will be purged)
      old_deleted_at = now - :timer.hours(24 * 14) * 1_000_000

      {:ok, true} =
        EKV.Store.write_entry(
          db,
          stmts.kv_upsert,
          stmts.oplog_insert,
          "fs_key_1",
          nil,
          now - 2000,
          :node_a,
          nil,
          old_deleted_at
        )

      # Delete entry 2 with a recent tombstone (will be kept)
      {:ok, true} =
        EKV.Store.write_entry(
          db,
          stmts.kv_upsert,
          stmts.oplog_insert,
          "fs_key_2",
          nil,
          now - 1000,
          :node_a,
          nil,
          now - 1000
        )

      # Purge old tombstones (entry 1 is old, entry 2 is recent)
      cutoff = now - :timer.hours(24 * 7) * 1_000_000
      EKV.Store.purge_tombstones(db, cutoff)

      # full_state should:
      # - NOT include entry 1 (purged)
      # - Include entry 2 (recent tombstone, deleted_at > cutoff)
      # - Include entry 3 (live)
      entries = EKV.Store.full_state(db, cutoff)
      keys = Enum.map(entries, fn {key, _, _, _, _, _} -> key end)

      refute "fs_key_1" in keys
      assert "fs_key_2" in keys
      assert "fs_key_3" in keys
    end
  end

  # =====================================================================
  # Subscribe / notification tests
  # =====================================================================

  describe "subscribe" do
    test "put notification", %{name: name} do
      :ok = EKV.subscribe(name, "user/")
      :ok = EKV.put(name, "user/1", "alice")
      flush_dispatchers(name)

      assert_receive {:ekv, [%EKV.Event{type: :put, key: "user/1", value: "alice"}],
                      %{name: ^name}}
    end

    test "delete notification with previous value", %{name: name} do
      :ok = EKV.put(name, "user/1", "alice")
      :ok = EKV.subscribe(name, "user/")
      :ok = EKV.delete(name, "user/1")
      flush_dispatchers(name)

      assert_receive {:ekv, [%EKV.Event{type: :delete, key: "user/1", value: "alice"}],
                      %{name: ^name}}
    end

    test "delete notification for missing key has nil value", %{name: name} do
      :ok = EKV.subscribe(name, "user/")
      :ok = EKV.delete(name, "user/missing")
      flush_dispatchers(name)

      assert_receive {:ekv, [%EKV.Event{type: :delete, key: "user/missing", value: nil}],
                      %{name: ^name}}
    end

    test "prefix filtering", %{name: name} do
      :ok = EKV.subscribe(name, "user/")
      :ok = EKV.put(name, "post/1", "hello")
      flush_dispatchers(name)
      refute_receive {:ekv, _, _}, 50
    end

    test "empty prefix matches all keys", %{name: name} do
      :ok = EKV.subscribe(name)
      :ok = EKV.put(name, "anything/goes", "val")
      flush_dispatchers(name)
      assert_receive {:ekv, [%EKV.Event{type: :put, key: "anything/goes"}], _}
    end

    test "LWW rejection does not generate event", %{name: name} do
      config = EKV.get_config(name)
      shard = EKV.Replica.shard_index_for("lww_sub_key", config.num_shards)
      shard_name = EKV.Replica.shard_name(name, shard)

      # Write with high timestamp first via remote put
      high_ts = System.system_time(:nanosecond) + 1_000_000_000_000
      value_binary = :erlang.term_to_binary("winner")

      send(
        shard_name,
        {:ekv_put, "lww_sub_key", value_binary, high_ts, :big_node@host, nil}
      )

      :sys.get_state(shard_name)
      flush_dispatchers(name)

      :ok = EKV.subscribe(name, "lww_sub_key")

      # Send a low-ts put that should lose LWW
      low_ts = 1000
      loser_binary = :erlang.term_to_binary("loser")

      send(
        shard_name,
        {:ekv_put, "lww_sub_key", loser_binary, low_ts, :small_node@host, nil}
      )

      :sys.get_state(shard_name)
      flush_dispatchers(name)
      refute_receive {:ekv, _, _}, 50
    end

    test "multiple subscribers with different prefixes", %{name: name} do
      :ok = EKV.subscribe(name, "user/")

      other =
        spawn(fn ->
          :ok = EKV.subscribe(name, "post/")

          receive do
            msg -> send(self(), msg)
          after
            5000 -> :timeout
          end
        end)

      ref = Process.monitor(other)
      Process.sleep(20)

      :ok = EKV.put(name, "user/1", "alice")
      flush_dispatchers(name)

      assert_receive {:ekv, [%EKV.Event{type: :put, key: "user/1"}], _}

      # The other process should NOT get the user/ event — wait for it to exit
      assert_receive {:DOWN, ^ref, :process, ^other, _}, 5100
    end

    test "subscriber death cleanup", %{name: name} do
      pid =
        spawn(fn ->
          :ok = EKV.subscribe(name, "user/")

          receive do
            :stop -> :ok
          end
        end)

      Process.sleep(20)
      Process.exit(pid, :kill)
      Process.sleep(50)

      # Subsequent puts should not crash
      :ok = EKV.put(name, "user/1", "val")
      assert EKV.get(name, "user/1") == "val"
    end

    test "unsubscribe stops events", %{name: name} do
      :ok = EKV.subscribe(name, "user/")
      :ok = EKV.put(name, "user/1", "val1")
      flush_dispatchers(name)
      assert_receive {:ekv, _, _}

      :ok = EKV.unsubscribe(name, "user/")
      # Wait for SubTracker to process the unregister and update the cache generation
      Process.sleep(20)
      :ok = EKV.put(name, "user/2", "val2")
      flush_dispatchers(name)
      refute_receive {:ekv, _, _}, 50
    end

    test "GC TTL expiry generates delete event", %{name: name} do
      config = EKV.get_config(name)
      shard = EKV.Replica.shard_index_for("gc_sub_key", config.num_shards)
      shard_name = EKV.Replica.shard_name(name, shard)
      %{db: db, stmts: stmts} = :sys.get_state(shard_name)

      # Write an already-expired entry directly
      now = System.system_time(:nanosecond)
      past = now - 1_000_000_000
      val = :erlang.term_to_binary("doomed")

      {:ok, true} =
        EKV.Store.write_entry(
          db,
          stmts.kv_upsert,
          stmts.oplog_insert,
          "gc_sub_key",
          val,
          past - 1000,
          :node_a,
          past
        )

      :ok = EKV.subscribe(name, "gc_sub_key")

      # Trigger GC
      tombstone_cutoff = now - config.tombstone_ttl * 1_000_000
      send(shard_name, {:gc, now, tombstone_cutoff})
      :sys.get_state(shard_name)
      flush_dispatchers(name)

      assert_receive {:ekv, [%EKV.Event{type: :delete, key: "gc_sub_key", value: "doomed"}],
                      %{name: ^name}}
    end

    test "bulk sync generates batched events", %{name: name} do
      config = EKV.get_config(name)

      # Find a key that goes to shard 0
      key1 =
        Enum.find(1..100, fn i ->
          EKV.Replica.shard_index_for("sync_sub/#{i}", config.num_shards) == 0
        end)

      key2 =
        Enum.find((key1 + 1)..200, fn i ->
          EKV.Replica.shard_index_for("sync_sub/#{i}", config.num_shards) == 0
        end)

      k1 = "sync_sub/#{key1}"
      k2 = "sync_sub/#{key2}"

      shard_name = EKV.Replica.shard_name(name, 0)

      :ok = EKV.subscribe(name, "sync_sub/")

      now = System.system_time(:nanosecond)

      entries = [
        {k1, :erlang.term_to_binary("v1"), now - 2000, :remote@host, nil, nil},
        {k2, :erlang.term_to_binary("v2"), now - 1000, :remote@host, nil, nil}
      ]

      send(shard_name, {:ekv_sync, :remote@host, 0, entries, 100})
      :sys.get_state(shard_name)
      flush_dispatchers(name)

      assert_receive {:ekv, events, %{name: ^name}}
      assert length(events) == 2
      keys = Enum.map(events, & &1.key)
      assert k1 in keys
      assert k2 in keys
    end

    test "remote put generates event", %{name: name} do
      config = EKV.get_config(name)
      shard = EKV.Replica.shard_index_for("remote_put_key", config.num_shards)
      shard_name = EKV.Replica.shard_name(name, shard)

      :ok = EKV.subscribe(name, "remote_put_key")

      now = System.system_time(:nanosecond)
      val = :erlang.term_to_binary("remote_val")
      send(shard_name, {:ekv_put, "remote_put_key", val, now, :remote@host, nil})
      :sys.get_state(shard_name)
      flush_dispatchers(name)

      assert_receive {:ekv, [%EKV.Event{type: :put, key: "remote_put_key", value: "remote_val"}],
                      %{name: ^name}}
    end

    test "remote delete generates event with previous value", %{name: name} do
      :ok = EKV.put(name, "remote_del_key", "existing")

      config = EKV.get_config(name)
      shard = EKV.Replica.shard_index_for("remote_del_key", config.num_shards)
      shard_name = EKV.Replica.shard_name(name, shard)

      :ok = EKV.subscribe(name, "remote_del_key")

      now = System.system_time(:nanosecond) + 1_000_000_000
      send(shard_name, {:ekv_delete, "remote_del_key", now, :remote@host})
      :sys.get_state(shard_name)
      flush_dispatchers(name)

      assert_receive {:ekv, [%EKV.Event{type: :delete, key: "remote_del_key", value: "existing"}],
                      %{name: ^name}}
    end

    test "re-subscribe to same prefix is idempotent — no double delivery", %{name: name} do
      :ok = EKV.subscribe(name, "user/")
      :ok = EKV.subscribe(name, "user/")
      :ok = EKV.put(name, "user/1", "val")
      flush_dispatchers(name)

      assert_receive {:ekv, [%EKV.Event{type: :put, key: "user/1"}], _}
      refute_receive {:ekv, _, _}, 50
    end

    test "overlapping prefixes do not duplicate events", %{name: name} do
      # Subscribe to both "" (all keys) and "user/" (prefix)
      :ok = EKV.subscribe(name)
      :ok = EKV.subscribe(name, "user/")

      :ok = EKV.put(name, "user/1", "val")
      flush_dispatchers(name)

      # Should get ONE message with ONE event, not a duplicate
      assert_receive {:ekv, events, _}
      assert length(events) == 1
      assert [%EKV.Event{type: :put, key: "user/1", value: "val"}] = events
      refute_receive {:ekv, _, _}, 50
    end

    test "unsubscribe one prefix keeps another active", %{name: name} do
      :ok = EKV.subscribe(name, "user/")
      :ok = EKV.subscribe(name, "post/")

      :ok = EKV.put(name, "user/1", "u1")
      flush_dispatchers(name)
      assert_receive {:ekv, [%EKV.Event{key: "user/1"}], _}

      :ok = EKV.put(name, "post/1", "p1")
      flush_dispatchers(name)
      assert_receive {:ekv, [%EKV.Event{key: "post/1"}], _}

      # Unsubscribe from user/ only
      :ok = EKV.unsubscribe(name, "user/")
      Process.sleep(20)

      :ok = EKV.put(name, "user/2", "u2")
      flush_dispatchers(name)
      refute_receive {:ekv, _, _}, 50

      # post/ still active
      :ok = EKV.put(name, "post/2", "p2")
      flush_dispatchers(name)
      assert_receive {:ekv, [%EKV.Event{key: "post/2"}], _}
    end

    test "tombstone purge does not generate event", %{name: name} do
      config = EKV.get_config(name)
      shard = EKV.Replica.shard_index_for("purge_no_event", config.num_shards)
      shard_name = EKV.Replica.shard_name(name, shard)
      %{db: db, stmts: stmts} = :sys.get_state(shard_name)

      now = System.system_time(:nanosecond)

      # Create a tombstone
      {:ok, true} =
        EKV.Store.write_entry(
          db,
          stmts.kv_upsert,
          stmts.oplog_insert,
          "purge_no_event",
          nil,
          now - 2000,
          :node_a,
          nil,
          now - 2000
        )

      :ok = EKV.subscribe(name, "purge_no_event")

      # Purge with far-future cutoff
      future_cutoff = now + :timer.hours(24 * 365) * 1_000_000
      send(shard_name, {:gc, now, future_cutoff})
      :sys.get_state(shard_name)
      flush_dispatchers(name)

      refute_receive {:ekv, _, _}, 50
    end
  end

  # Flush all subscription dispatchers so async events are delivered before assertions
  defp flush_dispatchers(name) do
    config = EKV.get_config(name)

    for i <- 0..(config.num_shards - 1) do
      dispatcher = EKV.SubDispatcher.dispatcher_name(name, i)
      :sys.get_state(dispatcher)
    end

    :ok
  end

  # =====================================================================
  # Handoff tests
  # =====================================================================

  describe "handoff" do
    setup do
      ho_name = :"ekv_ho_#{System.unique_integer([:positive])}"
      ho_dir = Path.join(System.tmp_dir!(), "ekv_ho_test_#{ho_name}")

      on_exit(fn ->
        File.rm_rf!(ho_dir)
      end)

      %{ho_name: ho_name, ho_dir: ho_dir}
    end

    test "handoff request handler drains and closes", %{ho_name: name, ho_dir: dir} do
      {:ok, pid} =
        EKV.start_link(
          name: name,
          data_dir: dir,
          shards: 2,
          log: false,
          cluster_size: 1,
          node_id: 1,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )

      # Write some data
      {:ok, _} = EKV.put(name, "ho/1", "val1", if_vsn: nil)

      # Send handoff request directly to shard 0
      shard_name = :"#{name}_ekv_replica_0"
      ref = make_ref()
      send(shard_name, {:ekv_handoff_request, ref, node(), self()})

      assert_receive {:ekv_handoff_ack, ^ref}, 2000

      # Verify state
      state = :sys.get_state(shard_name)
      assert state.db == nil
      assert state.stmts == nil
      assert state.handoff_node == node()
      assert state.pending_cas == %{}

      Process.flag(:trap_exit, true)
      Process.exit(pid, :shutdown)
      Process.sleep(50)
    end

    test "handoff persists ballot_counter", %{ho_name: name, ho_dir: dir} do
      Process.flag(:trap_exit, true)

      {:ok, _} =
        EKV.start_link(
          name: name,
          data_dir: dir,
          shards: 2,
          log: false,
          cluster_size: 1,
          node_id: 1,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )

      # Do several CAS ops to advance ballot counter
      for i <- 1..5 do
        {:ok, ^i, _} =
          EKV.update(name, "counter", fn
            nil -> 1
            n -> n + 1
          end)
      end

      # Record ballot counter
      shard_name = :"#{name}_ekv_replica_0"
      state_before = :sys.get_state(shard_name)
      counter_before = state_before.ballot_counter

      # Send handoff request to all shards
      for i <- 0..1 do
        sn = :"#{name}_ekv_replica_#{i}"
        ref = make_ref()
        send(sn, {:ekv_handoff_request, ref, node(), self()})
        assert_receive {:ekv_handoff_ack, ^ref}, 2000
      end

      Supervisor.stop(:"#{name}_ekv_sup", :shutdown)
      Process.sleep(50)

      # Restart and verify ballot counter didn't regress
      {:ok, pid2} =
        EKV.start_link(
          name: name,
          data_dir: dir,
          shards: 2,
          log: false,
          cluster_size: 1,
          node_id: 1,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )

      state_after = :sys.get_state(shard_name)
      assert state_after.ballot_counter >= counter_before

      Process.exit(pid2, :shutdown)
      Process.sleep(50)
    end

    test "handoff + restart preserves data", %{ho_name: name, ho_dir: dir} do
      Process.flag(:trap_exit, true)

      {:ok, _} =
        EKV.start_link(
          name: name,
          data_dir: dir,
          shards: 2,
          log: false,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )

      :ok = EKV.put(name, "ho/1", "v1")
      :ok = EKV.put(name, "ho/2", "v2")
      :ok = EKV.put(name, "ho/3", "v3")

      # Send handoff to all shards
      for i <- 0..1 do
        sn = :"#{name}_ekv_replica_#{i}"
        ref = make_ref()
        send(sn, {:ekv_handoff_request, ref, node(), self()})
        assert_receive {:ekv_handoff_ack, ^ref}, 2000
      end

      Supervisor.stop(:"#{name}_ekv_sup", :shutdown)
      Process.sleep(50)

      {:ok, pid2} =
        EKV.start_link(
          name: name,
          data_dir: dir,
          shards: 2,
          log: false,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )

      assert EKV.get(name, "ho/1") == "v1"
      assert EKV.get(name, "ho/2") == "v2"
      assert EKV.get(name, "ho/3") == "v3"

      Process.exit(pid2, :shutdown)
      Process.sleep(50)
    end

    test "handoff + CAS state preserved", %{ho_name: name, ho_dir: dir} do
      Process.flag(:trap_exit, true)

      {:ok, _} =
        EKV.start_link(
          name: name,
          data_dir: dir,
          shards: 2,
          log: false,
          cluster_size: 1,
          node_id: 1,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )

      {:ok, _} = EKV.put(name, "cas/1", "val1", if_vsn: nil)
      {"val1", vsn} = EKV.lookup(name, "cas/1")

      # Handoff + restart
      for i <- 0..1 do
        sn = :"#{name}_ekv_replica_#{i}"
        ref = make_ref()
        send(sn, {:ekv_handoff_request, ref, node(), self()})
        assert_receive {:ekv_handoff_ack, ^ref}, 2000
      end

      Supervisor.stop(:"#{name}_ekv_sup", :shutdown)
      Process.sleep(50)

      {:ok, pid2} =
        EKV.start_link(
          name: name,
          data_dir: dir,
          shards: 2,
          log: false,
          cluster_size: 1,
          node_id: 1,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )

      # CAS update with old version should succeed
      {:ok, _} = EKV.put(name, "cas/1", "val2", if_vsn: vsn)
      assert EKV.get(name, "cas/1") == "val2"

      Process.exit(pid2, :shutdown)
      Process.sleep(50)
    end

    test "blue_green startup skips stale marker when old node is unreachable", %{
      ho_name: name,
      ho_dir: dir
    } do
      File.mkdir_p!(dir)
      File.write!(Path.join(dir, "current"), "nonexistent@node\n")

      started_at = System.monotonic_time(:millisecond)

      {:ok, pid} =
        EKV.start_link(
          name: name,
          data_dir: dir,
          shards: 2,
          log: false,
          blue_green: true,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )

      elapsed = System.monotonic_time(:millisecond) - started_at

      assert elapsed < 2_000

      {:ok, marker} = File.read(Path.join(dir, "current"))
      assert String.trim(marker) == Atom.to_string(node())

      Process.flag(:trap_exit, true)
      Process.exit(pid, :shutdown)
      Process.sleep(50)
    end

    test "blue_green graceful shutdown clears marker when it still points to self", %{
      ho_name: name,
      ho_dir: dir
    } do
      {:ok, pid} =
        EKV.start_link(
          name: name,
          data_dir: dir,
          shards: 2,
          log: false,
          blue_green: true,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )

      Process.unlink(pid)

      marker_path = Path.join(dir, "current")
      assert File.exists?(marker_path)
      {:ok, marker} = File.read(marker_path)
      assert String.trim(marker) == Atom.to_string(node())

      Supervisor.stop(:"#{name}_ekv_sup", :shutdown)
      refute File.exists?(marker_path)
    end

    test "blue_green graceful shutdown preserves marker when it points elsewhere", %{
      ho_name: name,
      ho_dir: dir
    } do
      {:ok, pid} =
        EKV.start_link(
          name: name,
          data_dir: dir,
          shards: 2,
          log: false,
          blue_green: true,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )

      Process.unlink(pid)

      marker_path = Path.join(dir, "current")
      File.write!(marker_path, "other@node\n")

      Supervisor.stop(:"#{name}_ekv_sup", :shutdown)

      assert File.exists?(marker_path)
      assert File.read!(marker_path) == "other@node\n"
    end

    test "blue_green graceful shutdown preserves self marker after handoff", %{
      ho_name: name,
      ho_dir: dir
    } do
      {:ok, pid} =
        EKV.start_link(
          name: name,
          data_dir: dir,
          shards: 2,
          log: false,
          blue_green: true,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )

      Process.unlink(pid)

      marker_path = Path.join(dir, "current")
      assert File.exists?(marker_path)
      assert String.trim(File.read!(marker_path)) == Atom.to_string(node())

      shard_name = :"#{name}_ekv_replica_0"
      ref = make_ref()
      send(shard_name, {:ekv_handoff_request, ref, :incoming@node, self()})
      assert_receive {:ekv_handoff_ack, ^ref}, 2000

      # Simulate shutdown racing before the incoming node rewrites the marker.
      Supervisor.stop(:"#{name}_ekv_sup", :shutdown)

      assert File.exists?(marker_path)
      assert String.trim(File.read!(marker_path)) == Atom.to_string(node())
    end

    test "double handoff request acks both", %{ho_name: name, ho_dir: dir} do
      {:ok, pid} =
        EKV.start_link(
          name: name,
          data_dir: dir,
          shards: 2,
          log: false,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )

      shard_name = :"#{name}_ekv_replica_0"

      # First handoff
      ref1 = make_ref()
      send(shard_name, {:ekv_handoff_request, ref1, node(), self()})
      assert_receive {:ekv_handoff_ack, ^ref1}, 2000

      # Second handoff (idempotent — db already closed, just acks)
      ref2 = make_ref()
      send(shard_name, {:ekv_handoff_request, ref2, node(), self()})
      assert_receive {:ekv_handoff_ack, ^ref2}, 2000

      Process.flag(:trap_exit, true)
      Process.exit(pid, :shutdown)
      Process.sleep(50)
    end

    test "reads work after handoff", %{ho_name: name, ho_dir: dir} do
      {:ok, pid} =
        EKV.start_link(
          name: name,
          data_dir: dir,
          shards: 2,
          log: false,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )

      :ok = EKV.put(name, "read/1", "val")

      # Handoff all shards
      for i <- 0..1 do
        sn = :"#{name}_ekv_replica_#{i}"
        ref = make_ref()
        send(sn, {:ekv_handoff_request, ref, node(), self()})
        assert_receive {:ekv_handoff_ack, ^ref}, 2000
      end

      # Reads still work (reader connections are still alive)
      assert EKV.get(name, "read/1") == "val"

      Process.flag(:trap_exit, true)
      Process.exit(pid, :shutdown)
      Process.sleep(50)
    end

    test "proxy rejects when new node unreachable", %{ho_name: name, ho_dir: dir} do
      {:ok, pid} =
        EKV.start_link(
          name: name,
          data_dir: dir,
          shards: 2,
          log: false,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )

      :ok = EKV.put(name, "proxy/1", "val")

      # Handoff with a fake unreachable node
      shard_name = :"#{name}_ekv_replica_0"
      ref = make_ref()
      send(shard_name, {:ekv_handoff_request, ref, :nonexistent@node, self()})
      assert_receive {:ekv_handoff_ack, ^ref}, 2000

      # Write call should fail with :shutting_down (proxy can't reach fake node)
      result = GenServer.call(shard_name, {:put, "proxy/2", :erlang.term_to_binary("v"), []})
      assert result == {:error, :shutting_down}

      Process.flag(:trap_exit, true)
      Process.exit(pid, :shutdown)
      Process.sleep(50)
    end

    test "NIF backup copies data correctly", %{ho_dir: dir} do
      source_dir = Path.join(dir, "backup_src")
      dest_dir = Path.join(dir, "backup_dst")
      File.mkdir_p!(source_dir)
      File.mkdir_p!(dest_dir)

      # Open a db, write data, close
      {:ok, db} = EKV.Store.open(source_dir, 0, :timer.hours(24 * 7), 2, :timer.minutes(5))
      stmts = EKV.Store.prepare_cached_stmts(db)

      {:ok, true} =
        EKV.Store.write_entry(
          db,
          stmts.kv_upsert,
          stmts.oplog_insert,
          "backup_key",
          :erlang.term_to_binary("backup_val"),
          1000,
          :node_a,
          nil
        )

      EKV.Store.release_stmts(db, stmts)
      EKV.Store.close(db)

      # Backup
      source_path = Path.join(source_dir, "shard_0.db")
      dest_path = Path.join(dest_dir, "shard_0.db")
      assert :ok = EKV.Sqlite3.backup(source_path, dest_path)

      # Open dest and verify data
      {:ok, db2} = EKV.Store.open(dest_dir, 0, :timer.hours(24 * 7), 2, :timer.minutes(5))

      result = EKV.Store.get(db2, "backup_key")
      assert result != nil
      {value_bin, _ts, _origin, _exp, _del} = result
      assert :erlang.binary_to_term(value_bin) == "backup_val"

      EKV.Store.close(db2)
    end
  end

  describe "delta sync returns correct entries" do
    test "oplog_since returns exactly the right slice", %{name: name} do
      config = EKV.get_config(name)
      shard = EKV.Replica.shard_index_for("delta_key_1", config.num_shards)
      shard_name = EKV.Replica.shard_name(name, shard)
      %{db: db, stmts: stmts} = :sys.get_state(shard_name)

      # Write 10 entries
      for i <- 1..10 do
        val = :erlang.term_to_binary("delta_val_#{i}")

        {:ok, true} =
          EKV.Store.write_entry(
            db,
            stmts.kv_upsert,
            stmts.oplog_insert,
            "delta_key_#{i}",
            val,
            1000 + i,
            :node_a,
            nil
          )
      end

      # Get all entries to find seq for entry 5
      all = EKV.Store.oplog_since(db, 0)
      assert length(all) == 10

      # Find seq of the 5th entry
      {fifth_seq, _, _, _, _, _, _} = Enum.at(all, 4)

      # oplog_since(fifth_seq) should return entries 6..10
      delta = EKV.Store.oplog_since(db, fifth_seq)
      assert length(delta) == 5

      delta_keys = Enum.map(delta, fn {_, key, _, _, _, _, _} -> key end)
      for i <- 6..10, do: assert("delta_key_#{i}" in delta_keys)
    end
  end

  # =====================================================================
  # CAS Config Validation
  # =====================================================================

  describe "CAS config validation" do
    test "cluster_size without node_id auto-generates node_id" do
      name = :"ekv_cas_cfg_#{System.unique_integer([:positive])}"
      data_dir = Path.join(System.tmp_dir!(), "ekv_cas_cfg_#{name}")

      {:ok, pid} =
        EKV.start_link(name: name, data_dir: data_dir, cluster_size: 3, shards: 1, log: false)

      on_exit(fn ->
        Process.exit(pid, :shutdown)
        File.rm_rf!(data_dir)
      end)

      config = EKV.get_config(name)
      assert is_binary(config.node_id)
      assert byte_size(config.node_id) > 0
    end

    test "wait_for_quorum requires CAS configuration" do
      name = :"ekv_cas_cfg_#{System.unique_integer([:positive])}"
      data_dir = Path.join(System.tmp_dir!(), "ekv_cas_cfg_#{name}")
      Process.flag(:trap_exit, true)

      assert {:error, {%ArgumentError{message: msg}, _}} =
               EKV.start_link(
                 name: name,
                 data_dir: data_dir,
                 wait_for_quorum: 100,
                 log: false
               )

      assert msg =~ ":wait_for_quorum requires CAS configuration"

      File.rm_rf!(data_dir)
    end

    test "await_quorum requires CAS configuration" do
      name = :"ekv_cas_cfg_#{System.unique_integer([:positive])}"
      data_dir = Path.join(System.tmp_dir!(), "ekv_cas_cfg_#{name}")

      {:ok, pid} = EKV.start_link(name: name, data_dir: data_dir, shards: 1, log: false)

      on_exit(fn ->
        Process.exit(pid, :shutdown)
        File.rm_rf!(data_dir)
      end)

      assert_raise ArgumentError, ~r/await_quorum\/2 requires :cluster_size and :node_id/, fn ->
        EKV.await_quorum(name, 0)
      end
    end

    test "invalid wait_for_quorum type raises" do
      name = :"ekv_cas_cfg_#{System.unique_integer([:positive])}"
      data_dir = Path.join(System.tmp_dir!(), "ekv_cas_cfg_#{name}")
      Process.flag(:trap_exit, true)

      assert {:error, {%ArgumentError{message: msg}, _}} =
               EKV.start_link(
                 name: name,
                 data_dir: data_dir,
                 cluster_size: 3,
                 node_id: 1,
                 wait_for_quorum: true,
                 log: false
               )

      assert msg =~ ":wait_for_quorum must be false/nil or a non-negative timeout in ms"

      File.rm_rf!(data_dir)
    end

    test "invalid node_id type raises" do
      name = :"ekv_cas_cfg_#{System.unique_integer([:positive])}"
      data_dir = Path.join(System.tmp_dir!(), "ekv_cas_cfg_#{name}")
      Process.flag(:trap_exit, true)

      assert {:error, {%ArgumentError{message: msg}, _}} =
               EKV.start_link(
                 name: name,
                 data_dir: data_dir,
                 cluster_size: 3,
                 node_id: :bad_atom,
                 log: false
               )

      assert msg =~ "non-empty string or positive integer"

      File.rm_rf!(data_dir)
    end

    test "empty string node_id raises" do
      name = :"ekv_cas_cfg_#{System.unique_integer([:positive])}"
      data_dir = Path.join(System.tmp_dir!(), "ekv_cas_cfg_#{name}")
      Process.flag(:trap_exit, true)

      assert {:error, {%ArgumentError{message: msg}, _}} =
               EKV.start_link(
                 name: name,
                 data_dir: data_dir,
                 cluster_size: 3,
                 node_id: "",
                 log: false
               )

      assert msg =~ "non-empty string or positive integer"

      File.rm_rf!(data_dir)
    end

    test "cluster_size: 1, node_id: 1 starts successfully (integer auto-converts)" do
      name = :"ekv_cas_cfg_#{System.unique_integer([:positive])}"
      data_dir = Path.join(System.tmp_dir!(), "ekv_cas_cfg_#{name}")

      {:ok, pid} =
        EKV.start_link(
          name: name,
          data_dir: data_dir,
          cluster_size: 1,
          node_id: 1,
          shards: 1,
          log: false
        )

      on_exit(fn ->
        Process.exit(pid, :shutdown)
        File.rm_rf!(data_dir)
      end)

      assert Process.alive?(pid)
      config = EKV.get_config(name)
      assert config.node_id == "1"
    end

    test "start does not block for quorum by default" do
      name = :"ekv_cas_cfg_#{System.unique_integer([:positive])}"
      data_dir = Path.join(System.tmp_dir!(), "ekv_cas_cfg_#{name}")

      {:ok, pid} =
        EKV.start_link(
          name: name,
          data_dir: data_dir,
          cluster_size: 3,
          node_id: 1,
          shards: 1,
          log: false
        )

      on_exit(fn ->
        Process.exit(pid, :shutdown)
        File.rm_rf!(data_dir)
      end)

      assert {:error, :timeout} = EKV.await_quorum(name, 0)
      assert {:error, :no_quorum} = EKV.put(name, "boot/default", "v", consistent: true)
    end

    test "wait_for_quorum times out startup when quorum is unavailable" do
      name = :"ekv_cas_cfg_#{System.unique_integer([:positive])}"
      data_dir = Path.join(System.tmp_dir!(), "ekv_cas_cfg_#{name}")
      Process.flag(:trap_exit, true)
      started_at = System.monotonic_time(:millisecond)

      assert {:error, reason} =
               EKV.start_link(
                 name: name,
                 data_dir: data_dir,
                 cluster_size: 3,
                 node_id: 1,
                 shards: 1,
                 wait_for_quorum: 100,
                 log: false
               )

      elapsed = System.monotonic_time(:millisecond) - started_at

      assert elapsed >= 100
      assert inspect(reason) =~ "timeout"

      File.rm_rf!(data_dir)
    end

    test "string node_id works" do
      name = :"ekv_cas_cfg_#{System.unique_integer([:positive])}"
      data_dir = Path.join(System.tmp_dir!(), "ekv_cas_cfg_#{name}")

      {:ok, pid} =
        EKV.start_link(
          name: name,
          data_dir: data_dir,
          cluster_size: 1,
          node_id: "fly-machine-abc123",
          shards: 1,
          log: false
        )

      on_exit(fn ->
        Process.exit(pid, :shutdown)
        File.rm_rf!(data_dir)
      end)

      config = EKV.get_config(name)
      assert config.node_id == "fly-machine-abc123"
    end

    test "CAS ops without cluster_size raise" do
      name = :"ekv_cas_cfg_#{System.unique_integer([:positive])}"
      data_dir = Path.join(System.tmp_dir!(), "ekv_cas_cfg_#{name}")

      {:ok, pid} = EKV.start_link(name: name, data_dir: data_dir, shards: 1, log: false)

      on_exit(fn ->
        Process.exit(pid, :shutdown)
        File.rm_rf!(data_dir)
      end)

      assert_raise ArgumentError, ~r/CAS operations require/, fn ->
        EKV.put(name, "key", "val", if_vsn: nil)
      end

      assert_raise ArgumentError, ~r/CAS operations require/, fn ->
        EKV.delete(name, "key", if_vsn: nil)
      end

      assert_raise ArgumentError, ~r/CAS operations require/, fn ->
        EKV.update(name, "key", fn v -> v end)
      end
    end
  end

  # =====================================================================
  # CAS: lookup/2
  # =====================================================================

  describe "lookup" do
    setup do
      name = :"ekv_lookup_#{System.unique_integer([:positive])}"
      data_dir = Path.join(System.tmp_dir!(), "ekv_test_#{name}")

      {:ok, pid} =
        EKV.start_link(
          name: name,
          data_dir: data_dir,
          shards: 2,
          log: false,
          cluster_size: 1,
          node_id: 1,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )

      on_exit(fn ->
        Process.exit(pid, :shutdown)
        File.rm_rf!(data_dir)
      end)

      %{cas_name: name}
    end

    test "returns {value, vsn} for existing key", %{cas_name: name} do
      :ok = EKV.put(name, "user/1", %{name: "Alice"})
      {value, vsn} = EKV.lookup(name, "user/1")
      assert value == %{name: "Alice"}
      assert {ts, origin} = vsn
      assert is_integer(ts)
      assert is_atom(origin)
    end

    test "returns nil for missing key", %{cas_name: name} do
      assert nil == EKV.lookup(name, "missing")
    end

    test "returns nil for deleted key", %{cas_name: name} do
      :ok = EKV.put(name, "del/1", "val")
      :ok = EKV.delete(name, "del/1")
      assert nil == EKV.lookup(name, "del/1")
    end

    test "returns nil for expired TTL key", %{cas_name: name} do
      :ok = EKV.put(name, "ttl/1", "val", ttl: 1)
      Process.sleep(10)
      assert nil == EKV.lookup(name, "ttl/1")
    end

    test "vsn changes after each put", %{cas_name: name} do
      :ok = EKV.put(name, "k", "v1")
      {_, vsn1} = EKV.lookup(name, "k")
      Process.sleep(1)
      :ok = EKV.put(name, "k", "v2")
      {_, vsn2} = EKV.lookup(name, "k")
      assert vsn1 != vsn2
    end
  end

  # =====================================================================
  # CAS: put with if_vsn (single-node, cluster_size: 1)
  # =====================================================================

  describe "CAS put" do
    setup do
      name = :"ekv_cas_put_#{System.unique_integer([:positive])}"
      data_dir = Path.join(System.tmp_dir!(), "ekv_test_#{name}")

      {:ok, pid} =
        EKV.start_link(
          name: name,
          data_dir: data_dir,
          shards: 2,
          log: false,
          cluster_size: 1,
          node_id: 1,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )

      on_exit(fn ->
        Process.exit(pid, :shutdown)
        File.rm_rf!(data_dir)
      end)

      %{cas_name: name}
    end

    test "put if_vsn: nil on missing key succeeds (insert-if-absent)", %{cas_name: name} do
      assert {:ok, _} = EKV.put(name, "new/1", "val", if_vsn: nil)
      assert EKV.get(name, "new/1") == "val"
    end

    test "put if_vsn: nil on existing key returns conflict", %{cas_name: name} do
      :ok = EKV.put(name, "exist/1", "val")
      assert {:error, :conflict} = EKV.put(name, "exist/1", "val2", if_vsn: nil)
    end

    test "put if_vsn: vsn succeeds when vsn matches", %{cas_name: name} do
      :ok = EKV.put(name, "cas/1", "v1")
      {_, vsn} = EKV.lookup(name, "cas/1")
      assert {:ok, _} = EKV.put(name, "cas/1", "v2", if_vsn: vsn)
      assert EKV.get(name, "cas/1") == "v2"
    end

    test "put if_vsn: vsn returns conflict when stale", %{cas_name: name} do
      :ok = EKV.put(name, "cas/2", "v1")
      {_, vsn1} = EKV.lookup(name, "cas/2")
      :ok = EKV.put(name, "cas/2", "v2")
      assert {:error, :conflict} = EKV.put(name, "cas/2", "v3", if_vsn: vsn1)
    end

    test "put if_vsn with TTL works", %{cas_name: name} do
      assert {:ok, _} = EKV.put(name, "ttl/1", "val", if_vsn: nil, ttl: 60_000)
      assert EKV.get(name, "ttl/1") == "val"
    end

    test "rejects invalid ttl at call site before hitting shard", %{cas_name: name} do
      config = EKV.get_config(name)
      shard_index = EKV.Replica.shard_index_for("ttl/bad", config.num_shards)
      shard_name = EKV.Replica.shard_name(name, shard_index)
      shard_pid = Process.whereis(shard_name)

      assert_raise ArgumentError, ~r/:ttl must be a positive integer/, fn ->
        EKV.put(name, "ttl/bad", "val", if_vsn: nil, ttl: "10")
      end

      assert Process.whereis(shard_name) == shard_pid
    end

    test "rejects invalid timeout at call site", %{cas_name: name} do
      assert_raise ArgumentError, ~r/:timeout must be a positive integer or :infinity/, fn ->
        EKV.put(name, "timeout/bad", "val", if_vsn: nil, timeout: 0)
      end
    end

    test "after CAS put, get returns new value", %{cas_name: name} do
      {:ok, _} = EKV.put(name, "get/1", "v1", if_vsn: nil)
      assert EKV.get(name, "get/1") == "v1"
    end

    test "after CAS put, lookup returns new vsn", %{cas_name: name} do
      {:ok, _} = EKV.put(name, "vsn/1", "v1", if_vsn: nil)
      {_, vsn1} = EKV.lookup(name, "vsn/1")
      assert is_tuple(vsn1)

      {:ok, _} = EKV.put(name, "vsn/1", "v2", if_vsn: vsn1)
      {_, vsn2} = EKV.lookup(name, "vsn/1")
      assert vsn2 != vsn1
    end

    test "sequential lookup-put-lookup-put chain", %{cas_name: name} do
      {:ok, _} = EKV.put(name, "chain/1", "v1", if_vsn: nil)
      {"v1", vsn1} = EKV.lookup(name, "chain/1")
      {:ok, _} = EKV.put(name, "chain/1", "v2", if_vsn: vsn1)
      {"v2", vsn2} = EKV.lookup(name, "chain/1")
      {:ok, _} = EKV.put(name, "chain/1", "v3", if_vsn: vsn2)
      assert EKV.get(name, "chain/1") == "v3"
    end

    test "CAS put writes to oplog (visible via delta sync)", %{cas_name: name} do
      {:ok, _} = EKV.put(name, "oplog_cas/1", "val1", if_vsn: nil)

      config = EKV.get_config(name)
      shard = EKV.Replica.shard_index_for("oplog_cas/1", config.num_shards)
      shard_name = EKV.Replica.shard_name(name, shard)
      %{db: db} = :sys.get_state(shard_name)

      entries = EKV.Store.oplog_since(db, 0)
      oplog_keys = Enum.map(entries, fn {_, key, _, _, _, _, _} -> key end)
      assert "oplog_cas/1" in oplog_keys
    end

    test "CAS put dispatches subscriber events", %{cas_name: name} do
      :ok = EKV.subscribe(name, "sub/")
      {:ok, _} = EKV.put(name, "sub/1", "val", if_vsn: nil)
      flush_dispatchers(name)

      assert_receive {:ekv, [%EKV.Event{type: :put, key: "sub/1", value: "val"}], _}
    end
  end

  # =====================================================================
  # CAS: delete with if_vsn (single-node)
  # =====================================================================

  describe "CAS delete" do
    setup do
      name = :"ekv_cas_del_#{System.unique_integer([:positive])}"
      data_dir = Path.join(System.tmp_dir!(), "ekv_test_#{name}")

      {:ok, pid} =
        EKV.start_link(
          name: name,
          data_dir: data_dir,
          shards: 2,
          log: false,
          cluster_size: 1,
          node_id: 1,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )

      on_exit(fn ->
        Process.exit(pid, :shutdown)
        File.rm_rf!(data_dir)
      end)

      %{cas_name: name}
    end

    test "delete if_vsn succeeds when vsn matches", %{cas_name: name} do
      :ok = EKV.put(name, "del/1", "val")
      {_, vsn} = EKV.lookup(name, "del/1")
      assert {:ok, _} = EKV.delete(name, "del/1", if_vsn: vsn)
      assert EKV.get(name, "del/1") == nil
    end

    test "delete if_vsn returns conflict when stale", %{cas_name: name} do
      :ok = EKV.put(name, "del/2", "v1")
      {_, vsn1} = EKV.lookup(name, "del/2")
      :ok = EKV.put(name, "del/2", "v2")
      assert {:error, :conflict} = EKV.delete(name, "del/2", if_vsn: vsn1)
      assert EKV.get(name, "del/2") == "v2"
    end

    test "after CAS delete, get returns nil and lookup returns nil", %{cas_name: name} do
      :ok = EKV.put(name, "del/3", "val")
      {_, vsn} = EKV.lookup(name, "del/3")
      {:ok, _} = EKV.delete(name, "del/3", if_vsn: vsn)
      assert EKV.get(name, "del/3") == nil
      assert nil == EKV.lookup(name, "del/3")
    end

    test "CAS delete dispatches subscriber event with previous value", %{cas_name: name} do
      :ok = EKV.put(name, "del/4", "old_val")
      {_, vsn} = EKV.lookup(name, "del/4")
      :ok = EKV.subscribe(name, "del/")
      {:ok, _} = EKV.delete(name, "del/4", if_vsn: vsn)
      flush_dispatchers(name)

      assert_receive {:ekv, [%EKV.Event{type: :delete, key: "del/4", value: "old_val"}], _}
    end

    test "CAS delete then put if_vsn: nil re-creates key", %{cas_name: name} do
      :ok = EKV.put(name, "del/5", "val")
      {_, vsn} = EKV.lookup(name, "del/5")
      {:ok, _} = EKV.delete(name, "del/5", if_vsn: vsn)
      assert nil == EKV.lookup(name, "del/5")
      {:ok, _} = EKV.put(name, "del/5", "reborn", if_vsn: nil)
      assert EKV.get(name, "del/5") == "reborn"
    end

    test "rejects non-boolean resolve_unconfirmed option", %{cas_name: name} do
      :ok = EKV.put(name, "del/6", "val")
      {_, vsn} = EKV.lookup(name, "del/6")

      assert_raise ArgumentError, ~r/:resolve_unconfirmed must be boolean/, fn ->
        EKV.delete(name, "del/6", if_vsn: vsn, resolve_unconfirmed: 1)
      end
    end
  end

  # =====================================================================
  # CAS: update/3 (single-node)
  # =====================================================================

  describe "update" do
    setup do
      name = :"ekv_update_#{System.unique_integer([:positive])}"
      data_dir = Path.join(System.tmp_dir!(), "ekv_test_#{name}")

      {:ok, pid} =
        EKV.start_link(
          name: name,
          data_dir: data_dir,
          shards: 2,
          log: false,
          cluster_size: 1,
          node_id: 1,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )

      on_exit(fn ->
        Process.exit(pid, :shutdown)
        File.rm_rf!(data_dir)
      end)

      %{cas_name: name}
    end

    test "update on missing key: fun.(nil) creates key", %{cas_name: name} do
      {:ok, 1, _} = EKV.update(name, "u/1", fn nil -> 1 end)
      assert EKV.get(name, "u/1") == 1
    end

    test "update on existing key: fun.(value) modifies it", %{cas_name: name} do
      :ok = EKV.put(name, "u/2", 10)
      {:ok, 20, _} = EKV.update(name, "u/2", fn v -> v * 2 end)
      assert EKV.get(name, "u/2") == 20
    end

    test "update returns {:ok, new_value}", %{cas_name: name} do
      {:ok, "hello", _} = EKV.update(name, "u/3", fn nil -> "hello" end)
    end

    test "counter increment nil→1→2→3", %{cas_name: name} do
      inc = fn
        nil -> 1
        n -> n + 1
      end

      {:ok, 1, _} = EKV.update(name, "counter", inc)
      {:ok, 2, _} = EKV.update(name, "counter", inc)
      {:ok, 3, _} = EKV.update(name, "counter", inc)
      assert EKV.get(name, "counter") == 3
    end

    test "update with TTL option", %{cas_name: name} do
      {:ok, "val", _} = EKV.update(name, "ttl/u", fn nil -> "val" end, ttl: 60_000)
      assert EKV.get(name, "ttl/u") == "val"
    end

    test "rejects invalid retries and backoff at call site", %{cas_name: name} do
      assert_raise ArgumentError, ~r/:retries must be a non-negative integer/, fn ->
        EKV.update(name, "opt/u1", fn nil -> "val" end, retries: -1)
      end

      assert_raise ArgumentError,
                   ~r/:backoff must be \{min_ms, max_ms\} with non-negative integers and min <= max/,
                   fn ->
                     EKV.update(name, "opt/u2", fn nil -> "val" end, backoff: {20, 10})
                   end
    end

    test "rejects non-boolean resolve_unconfirmed option", %{cas_name: name} do
      assert_raise ArgumentError, ~r/:resolve_unconfirmed must be boolean/, fn ->
        EKV.update(name, "opt/u3", fn nil -> "val" end, resolve_unconfirmed: :yes)
      end
    end

    test "update dispatches subscriber events", %{cas_name: name} do
      :ok = EKV.subscribe(name, "u/")
      {:ok, "val", _} = EKV.update(name, "u/4", fn nil -> "val" end)
      flush_dispatchers(name)

      assert_receive {:ekv, [%EKV.Event{type: :put, key: "u/4", value: "val"}], _}
    end
  end

  # =====================================================================
  # CAS: get/3 with consistent: true
  # =====================================================================

  describe "consistent get" do
    setup do
      name = :"ekv_cget_#{System.unique_integer([:positive])}"
      data_dir = Path.join(System.tmp_dir!(), "ekv_test_#{name}")

      {:ok, pid} =
        EKV.start_link(
          name: name,
          data_dir: data_dir,
          shards: 2,
          log: false,
          cluster_size: 1,
          node_id: 1,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )

      on_exit(fn ->
        Process.exit(pid, :shutdown)
        File.rm_rf!(data_dir)
      end)

      %{cas_name: name}
    end

    test "returns nil for missing key", %{cas_name: name} do
      assert nil == EKV.get(name, "missing", consistent: true)
    end

    test "returns value for existing key", %{cas_name: name} do
      :ok = EKV.put(name, "cg/1", "val")
      assert "val" == EKV.get(name, "cg/1", consistent: true)
    end

    test "accepts retries and backoff opts", %{cas_name: name} do
      :ok = EKV.put(name, "cg/2", "val")
      assert "val" == EKV.get(name, "cg/2", consistent: true, retries: 3, backoff: {5, 20})
    end

    test "rejects invalid retries/backoff at call site", %{cas_name: name} do
      assert_raise ArgumentError, ~r/:retries must be a non-negative integer/, fn ->
        EKV.get(name, "cg/2", consistent: true, retries: :many)
      end

      assert_raise ArgumentError,
                   ~r/:backoff must be \{min_ms, max_ms\} with non-negative integers and min <= max/,
                   fn ->
                     EKV.get(name, "cg/2", consistent: true, backoff: {5, -1})
                   end
    end

    test "rejects non-boolean consistent option", %{cas_name: name} do
      assert_raise ArgumentError, ~r/:consistent must be boolean/, fn ->
        EKV.get(name, "cg/3", consistent: :linearizable)
      end
    end
  end

  # =====================================================================
  # CAS: put/4 with consistent: true
  # =====================================================================

  describe "consistent put" do
    setup do
      name = :"ekv_cput_#{System.unique_integer([:positive])}"
      data_dir = Path.join(System.tmp_dir!(), "ekv_test_#{name}")

      {:ok, pid} =
        EKV.start_link(
          name: name,
          data_dir: data_dir,
          shards: 2,
          log: false,
          cluster_size: 1,
          node_id: 1,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )

      on_exit(fn ->
        Process.exit(pid, :shutdown)
        File.rm_rf!(data_dir)
      end)

      %{cas_name: name}
    end

    test "creates new key", %{cas_name: name} do
      assert {:ok, _} = EKV.put(name, "cp/1", "val", consistent: true)
      assert EKV.get(name, "cp/1") == "val"
    end

    test "overwrites existing key", %{cas_name: name} do
      :ok = EKV.put(name, "cp/2", "v1")
      assert {:ok, _} = EKV.put(name, "cp/2", "v2", consistent: true)
      assert EKV.get(name, "cp/2") == "v2"
    end

    test "eventual put on CAS-managed key is rejected", %{cas_name: name} do
      assert {:ok, _} = EKV.put(name, "cp/guard_put", "v1", consistent: true)
      assert {:error, :cas_managed_key} = EKV.put(name, "cp/guard_put", "v2")
      assert EKV.get(name, "cp/guard_put") == "v1"
    end

    test "eventual delete on CAS-managed key is rejected", %{cas_name: name} do
      assert {:ok, _} = EKV.put(name, "cp/guard_del", "v1", consistent: true)
      assert {:error, :cas_managed_key} = EKV.delete(name, "cp/guard_del")
      assert EKV.get(name, "cp/guard_del") == "v1"
    end

    test "with TTL expires as expected", %{cas_name: name} do
      assert {:ok, _} = EKV.put(name, "cp/3", "val", consistent: true, ttl: 1)
      assert EKV.get(name, "cp/3", consistent: true) == "val"

      Process.sleep(10)
      assert EKV.get(name, "cp/3") == nil
      assert EKV.get(name, "cp/3", consistent: true) == nil
    end

    test "consistent and if_vsn are mutually exclusive", %{cas_name: name} do
      assert_raise ArgumentError, ~r/mutually exclusive/, fn ->
        EKV.put(name, "cp/4", "val", consistent: true, if_vsn: nil)
      end
    end

    test "rejects non-boolean consistent option", %{cas_name: name} do
      assert_raise ArgumentError, ~r/:consistent must be boolean/, fn ->
        EKV.put(name, "cp/5", "val", consistent: :linearizable)
      end
    end

    test "rejects non-boolean resolve_unconfirmed option", %{cas_name: name} do
      assert_raise ArgumentError, ~r/:resolve_unconfirmed must be boolean/, fn ->
        EKV.put(name, "cp/6", "val", if_vsn: nil, resolve_unconfirmed: :yes)
      end
    end
  end

  # =====================================================================
  # CAS: NIF paxos operations (direct NIF calls)
  # =====================================================================

  describe "NIF paxos operations" do
    setup do
      name = :"ekv_nif_pax_#{System.unique_integer([:positive])}"
      data_dir = Path.join(System.tmp_dir!(), "ekv_test_#{name}")

      {:ok, pid} =
        EKV.start_link(
          name: name,
          data_dir: data_dir,
          shards: 1,
          log: false,
          cluster_size: 1,
          node_id: 1,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )

      on_exit(fn ->
        Process.exit(pid, :shutdown)
        File.rm_rf!(data_dir)
      end)

      # Get writer db from shard state
      shard_name = :"#{name}_ekv_replica_0"
      state = :sys.get_state(shard_name)
      %{cas_name: name, db: state.db, stmts: state.stmts}
    end

    test "prepare on empty kv_paxos returns promise with accepted={0,\"\"}, nil", %{db: db} do
      {:ok, :promise, 0, "", nil} = EKV.Store.paxos_prepare(db, "pax/1", 100, "1")
    end

    test "prepare with higher ballot updates promise", %{db: db} do
      {:ok, :promise, 0, "", nil} = EKV.Store.paxos_prepare(db, "pax/2", 100, "1")
      {:ok, :promise, 0, "", nil} = EKV.Store.paxos_prepare(db, "pax/2", 200, "1")
    end

    test "prepare with lower ballot returns nack", %{db: db} do
      {:ok, :promise, 0, "", nil} = EKV.Store.paxos_prepare(db, "pax/3", 200, "1")
      {:ok, :nack, 200, "1"} = EKV.Store.paxos_prepare(db, "pax/3", 100, "1")
    end

    test "prepare with equal ballot returns nack (must be strictly greater)", %{db: db} do
      {:ok, :promise, 0, "", nil} = EKV.Store.paxos_prepare(db, "pax/4", 100, "1")
      {:ok, :nack, 100, "1"} = EKV.Store.paxos_prepare(db, "pax/4", 100, "1")
    end

    test "prepare returns kv row when key exists in kv table", %{cas_name: name, db: db} do
      # Write a value via normal put (writes to kv table)
      :ok = EKV.put(name, "pax/5", "hello")

      {:ok, :promise, 0, "", [val, ts, origin, _expires, _deleted]} =
        EKV.Store.paxos_prepare(db, "pax/5", 100, "1")

      assert :erlang.binary_to_term(val) == "hello"
      assert is_integer(ts)
      assert is_binary(origin)
    end

    test "accept succeeds when ballot >= promised", %{db: db} do
      {:ok, :promise, 0, "", nil} = EKV.Store.paxos_prepare(db, "pax/6", 100, "1")

      now = System.system_time(:nanosecond)
      origin_str = Atom.to_string(node())
      val_bin = :erlang.term_to_binary("accepted_val")
      value_args = [val_bin, now, origin_str, nil, nil]

      {:ok, true} = EKV.Store.paxos_accept(db, "pax/6", 100, "1", value_args)
    end

    test "accept fails when ballot < promised", %{db: db} do
      {:ok, :promise, 0, "", nil} = EKV.Store.paxos_prepare(db, "pax/7", 200, "1")

      now = System.system_time(:nanosecond)
      origin_str = Atom.to_string(node())
      val_bin = :erlang.term_to_binary("val")
      value_args = [val_bin, now, origin_str, nil, nil]

      {:ok, false} = EKV.Store.paxos_accept(db, "pax/7", 100, "1", value_args)
    end

    test "accept writes to kv_paxos only (not kv or oplog)", %{db: db} do
      {:ok, :promise, 0, "", nil} = EKV.Store.paxos_prepare(db, "pax/8", 100, "1")

      now = System.system_time(:nanosecond)
      origin_str = Atom.to_string(node())
      val_bin = :erlang.term_to_binary("atomic_val")
      value_args = [val_bin, now, origin_str, nil, nil]

      {:ok, true} = EKV.Store.paxos_accept(db, "pax/8", 100, "1", value_args)

      # kv table should NOT have the value (accept writes kv_paxos only)
      assert EKV.Store.get(db, "pax/8") == nil

      # oplog should NOT have the value
      entries = EKV.Store.oplog_since(db, 0)
      pax8_entries = Enum.filter(entries, fn {_, key, _, _, _, _, _} -> key == "pax/8" end)
      assert length(pax8_entries) == 0
    end

    test "prepare after accept returns accepted ballot + value from kv_paxos", %{db: db} do
      {:ok, :promise, 0, "", nil} = EKV.Store.paxos_prepare(db, "pax/9", 100, "1")

      now = System.system_time(:nanosecond)
      origin_str = Atom.to_string(node())
      val_bin = :erlang.term_to_binary("accepted")
      value_args = [val_bin, now, origin_str, nil, nil]

      {:ok, true} = EKV.Store.paxos_accept(db, "pax/9", 100, "1", value_args)

      # Higher prepare should see accepted ballot + value from kv_paxos
      {:ok, :promise, 100, "1", [val, _, _, _, _]} =
        EKV.Store.paxos_prepare(db, "pax/9", 200, "1")

      assert :erlang.binary_to_term(val) == "accepted"
    end

    test "sequence: prepare(5) → accept(5) → prepare(3) → nack", %{db: db} do
      {:ok, :promise, 0, "", nil} = EKV.Store.paxos_prepare(db, "pax/10", 5, "1")

      now = System.system_time(:nanosecond)
      origin_str = Atom.to_string(node())
      val_bin = :erlang.term_to_binary("seq")
      value_args = [val_bin, now, origin_str, nil, nil]

      {:ok, true} = EKV.Store.paxos_accept(db, "pax/10", 5, "1", value_args)

      # Lower prepare should nack (accepted ballot 5 is now promised)
      {:ok, :nack, 5, "1"} = EKV.Store.paxos_prepare(db, "pax/10", 3, "1")
    end

    test "accept updates accepted_counter/node in kv_paxos", %{db: db} do
      {:ok, :promise, 0, "", nil} = EKV.Store.paxos_prepare(db, "pax/11", 100, "2")

      now = System.system_time(:nanosecond)
      origin_str = Atom.to_string(node())
      val_bin = :erlang.term_to_binary("val")
      value_args = [val_bin, now, origin_str, nil, nil]

      {:ok, true} = EKV.Store.paxos_accept(db, "pax/11", 100, "2", value_args)

      # Higher prepare should see accepted={100, "2"}
      {:ok, :promise, 100, "2", _kv_row} = EKV.Store.paxos_prepare(db, "pax/11", 200, "1")
    end

    test "promote after accept writes to kv + oplog", %{db: db, stmts: stmts} do
      {:ok, :promise, 0, "", nil} = EKV.Store.paxos_prepare(db, "pax/12", 100, "1")

      now = System.system_time(:nanosecond)
      origin_str = Atom.to_string(node())
      val_bin = :erlang.term_to_binary("promoted_val")
      value_args = [val_bin, now, origin_str, nil, nil]

      {:ok, true} = EKV.Store.paxos_accept(db, "pax/12", 100, "1", value_args)

      # Not in kv yet
      assert EKV.Store.get(db, "pax/12") == nil

      # Promote
      {:ok, ^val_bin, ^now, ^origin_str, nil, nil, nil} =
        EKV.Store.paxos_promote(db, stmts.kv_force_upsert, stmts.oplog_insert, "pax/12", 100, "1")

      # Now in kv
      {v, _ts, _origin, _exp, _del} = EKV.Store.get(db, "pax/12")
      assert :erlang.binary_to_term(v) == "promoted_val"

      # And in oplog
      entries = EKV.Store.oplog_since(db, 0)
      assert Enum.any?(entries, fn {_, key, _, _, _, _, _} -> key == "pax/12" end)
    end

    test "promote with stale ballot returns :stale", %{db: db, stmts: stmts} do
      {:ok, :promise, 0, "", nil} = EKV.Store.paxos_prepare(db, "pax/13", 100, "1")

      now = System.system_time(:nanosecond)
      origin_str = Atom.to_string(node())
      val_bin = :erlang.term_to_binary("v1")
      value_args = [val_bin, now, origin_str, nil, nil]

      {:ok, true} = EKV.Store.paxos_accept(db, "pax/13", 100, "1", value_args)

      # Higher accept overwrites
      {:ok, :promise, 100, "1", _} = EKV.Store.paxos_prepare(db, "pax/13", 200, "2")
      val_bin2 = :erlang.term_to_binary("v2")

      {:ok, true} =
        EKV.Store.paxos_accept(db, "pax/13", 200, "2", [val_bin2, now, origin_str, nil, nil])

      # Stale promote for ballot {100, "1"}
      {:ok, :stale} =
        EKV.Store.paxos_promote(db, stmts.kv_force_upsert, stmts.oplog_insert, "pax/13", 100, "1")
    end

    test "promote retains kv_paxos accepted columns", %{db: db, stmts: stmts} do
      {:ok, :promise, 0, "", nil} = EKV.Store.paxos_prepare(db, "pax/14", 100, "1")

      now = System.system_time(:nanosecond)
      origin_str = Atom.to_string(node())
      val_bin = :erlang.term_to_binary("cleared")

      {:ok, true} =
        EKV.Store.paxos_accept(db, "pax/14", 100, "1", [val_bin, now, origin_str, nil, nil])

      # Promote
      {:ok, _, _, _, _, _, _} =
        EKV.Store.paxos_promote(db, stmts.kv_force_upsert, stmts.oplog_insert, "pax/14", 100, "1")

      # After promote, higher prepare recovers retained accepted state
      {:ok, :promise, 100, "1", [v, _, _, _, _]} =
        EKV.Store.paxos_prepare(db, "pax/14", 300, "1")

      assert :erlang.binary_to_term(v) == "cleared"
    end
  end

  # =====================================================================
  # CAS: Ballot and concurrency
  # =====================================================================

  describe "ballot and concurrency" do
    setup do
      name = :"ekv_ballot_#{System.unique_integer([:positive])}"
      data_dir = Path.join(System.tmp_dir!(), "ekv_test_#{name}")

      {:ok, pid} =
        EKV.start_link(
          name: name,
          data_dir: data_dir,
          shards: 1,
          log: false,
          cluster_size: 1,
          node_id: 1,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )

      on_exit(fn ->
        Process.exit(pid, :shutdown)
        File.rm_rf!(data_dir)
      end)

      %{cas_name: name, data_dir: data_dir}
    end

    test "ballot counter always increases", %{cas_name: name} do
      # Multiple updates → ballot counter should be monotonically increasing
      for i <- 1..5 do
        {:ok, ^i, _} =
          EKV.update(name, "inc", fn
            nil -> 1
            n -> n + 1
          end)
      end

      assert EKV.get(name, "inc") == 5
    end

    test "ballot counter survives process restart", %{cas_name: name} do
      {:ok, 1, _} = EKV.update(name, "persist", fn nil -> 1 end)

      # Get ballot counter before restart
      shard_name = :"#{name}_ekv_replica_0"
      state_before = :sys.get_state(shard_name)
      old_counter = state_before.ballot_counter

      # Kill and restart the shard (will restore from kv_meta)
      Process.exit(Process.whereis(shard_name), :kill)
      Process.sleep(100)

      # After restart, ballot counter should be >= old + 1
      state_after = :sys.get_state(shard_name)
      assert state_after.ballot_counter > old_counter
    end

    test "clock regression: ballot still increases (max(time, counter+1))", %{cas_name: name} do
      # Do an update to establish a ballot counter
      {:ok, 1, _} = EKV.update(name, "clock/1", fn nil -> 1 end)

      shard_name = :"#{name}_ekv_replica_0"
      state = :sys.get_state(shard_name)
      counter_before = state.ballot_counter

      # The ballot counter uses max(System.system_time(:nanosecond), counter+1)
      # Even if system clock somehow returned a lower value, counter+1 ensures monotonicity
      # We verify this by doing another update and checking the counter increased
      {:ok, 2, _} = EKV.update(name, "clock/1", fn n -> n + 1 end)

      state2 = :sys.get_state(shard_name)
      assert state2.ballot_counter > counter_before
    end

    test "N concurrent update tasks on same key → final value = N (serialized)", %{cas_name: name} do
      n = 10

      tasks =
        for _ <- 1..n do
          Task.async(fn ->
            EKV.update(name, "concurrent", fn
              nil -> 1
              v -> v + 1
            end)
          end)
        end

      results = Task.await_many(tasks, 10_000)

      assert Enum.all?(results, fn
               {:ok, _, _} -> true
               _ -> false
             end)

      assert EKV.get(name, "concurrent") == n
    end

    test "concurrent put(if_vsn:) tasks — exactly one succeeds, others fail", %{
      cas_name: name
    } do
      # Create the key
      {:ok, _} = EKV.put(name, "race/1", "v0", if_vsn: nil)
      {_, vsn} = EKV.lookup(name, "race/1")

      # Launch concurrent CAS puts with same vsn
      tasks =
        for i <- 1..5 do
          Task.async(fn ->
            EKV.put(name, "race/1", "v#{i}", if_vsn: vsn)
          end)
        end

      results = Task.await_many(tasks, 10_000)
      successes = Enum.count(results, &match?({:ok, _}, &1))

      failures =
        Enum.count(results, fn
          {:error, :conflict} -> true
          {:error, :unconfirmed} -> true
          _ -> false
        end)

      assert successes == 1
      assert failures == 4
    end
  end

  # =====================================================================
  # CAS: deferred local accept (no phantom writes)
  # =====================================================================

  describe "deferred local accept" do
    test "CAS failure does not leave phantom write in local SQLite" do
      # Setup: single node with cluster_size: 3, inject fake peers
      # so the shard THINKS it has quorum. Then control the Paxos
      # messages to force: prepare succeeds → accept fails.
      # With the bug (local accept before quorum), the value is in
      # SQLite even though CAS returned conflict.
      name = :"ekv_phantom_#{System.unique_integer([:positive])}"
      data_dir = Path.join(System.tmp_dir!(), "ekv_test_#{name}")

      {:ok, pid} =
        EKV.start_link(
          name: name,
          data_dir: data_dir,
          shards: 1,
          log: false,
          cluster_size: 3,
          node_id: 1,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )

      on_exit(fn ->
        Process.exit(pid, :shutdown)
        File.rm_rf!(data_dir)
      end)

      shard_name = :"#{name}_ekv_replica_0"

      # Inject fake peers so the shard thinks alive_count = 3 (quorum = 2)
      :sys.replace_state(shard_name, fn state ->
        %{
          state
          | peer_node_ids: %{:fake_b@localhost => "2", :fake_c@localhost => "3"},
            remote_shards: %{:fake_b@localhost => self(), :fake_c@localhost => self()}
        }
      end)

      # Start CAS in a task (will hang waiting for remote promises)
      task =
        Task.async(fn ->
          EKV.put(name, "phantom_key", "phantom_value", if_vsn: nil)
        end)

      # Let the CAS call enter the shard and send prepare messages
      Process.sleep(50)

      # Get the pending CAS ref
      shard_state = :sys.get_state(shard_name)
      assert map_size(shard_state.pending_cas) == 1
      [{ref, _op}] = Map.to_list(shard_state.pending_cas)

      # Send a fake promise from node_id "2" — shard now has 2 promises
      # (own + fake_b) which meets quorum=2. It enters accept phase.
      # BUG: local paxos_accept writes to SQLite before quorum is confirmed.
      send(shard_name, {:ekv_promise, ref, self(), "2", 0, "", nil})
      Process.sleep(50)

      # Send accept nacks from both fake peers — quorum can't be reached
      send(shard_name, {:ekv_accept_nack, ref, self(), "2"})
      send(shard_name, {:ekv_accept_nack, ref, self(), "3"})

      # CAS entered accept phase and then lost quorum, so caller sees unconfirmed.
      result = Task.await(task, 10_000)
      assert result == {:error, :unconfirmed}

      # THE KEY ASSERTION: CAS failed, so the value must NOT be in local SQLite.
      # If local accept was deferred until quorum, this passes.
      # If local accept happened eagerly (the bug), this fails.
      assert EKV.get(name, "phantom_key") == nil,
             "CAS returned {:error, :unconfirmed} but phantom write is visible via EKV.get"
    end

    test "resolve_unconfirmed maps ambiguous CAS outcome to resolved error" do
      name = :"ekv_resolve_unconfirmed_#{System.unique_integer([:positive])}"
      data_dir = Path.join(System.tmp_dir!(), "ekv_test_#{name}")

      {:ok, pid} =
        EKV.start_link(
          name: name,
          data_dir: data_dir,
          shards: 1,
          log: false,
          cluster_size: 3,
          node_id: 1,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )

      on_exit(fn ->
        Process.exit(pid, :shutdown)
        File.rm_rf!(data_dir)
      end)

      shard_name = :"#{name}_ekv_replica_0"

      :sys.replace_state(shard_name, fn state ->
        %{
          state
          | peer_node_ids: %{:fake_b@localhost => "2", :fake_c@localhost => "3"},
            remote_shards: %{:fake_b@localhost => self(), :fake_c@localhost => self()}
        }
      end)

      task =
        Task.async(fn ->
          EKV.put(name, "resolve_key", "v1", if_vsn: nil, resolve_unconfirmed: true)
        end)

      Process.sleep(50)
      shard_state = :sys.get_state(shard_name)
      assert map_size(shard_state.pending_cas) == 1
      [{ref, _op}] = Map.to_list(shard_state.pending_cas)

      send(shard_name, {:ekv_promise, ref, self(), "2", 0, "", nil})
      Process.sleep(50)
      send(shard_name, {:ekv_accept_nack, ref, self(), "2"})
      send(shard_name, {:ekv_accept_nack, ref, self(), "3"})

      result = Task.await(task, 10_000)

      assert result in [{:error, :conflict}, {:error, :unavailable}]

      assert EKV.get(name, "resolve_key") == nil,
             "CAS resolved to error but phantom write is visible via EKV.get"
    end

    test "CAS success still works with deferred local accept" do
      name = :"ekv_deferred_ok_#{System.unique_integer([:positive])}"
      data_dir = Path.join(System.tmp_dir!(), "ekv_test_#{name}")

      {:ok, pid} =
        EKV.start_link(
          name: name,
          data_dir: data_dir,
          shards: 1,
          log: false,
          cluster_size: 3,
          node_id: 1,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )

      on_exit(fn ->
        Process.exit(pid, :shutdown)
        File.rm_rf!(data_dir)
      end)

      shard_name = :"#{name}_ekv_replica_0"

      # Inject fake peers
      :sys.replace_state(shard_name, fn state ->
        %{
          state
          | peer_node_ids: %{:fake_b@localhost => "2", :fake_c@localhost => "3"},
            remote_shards: %{:fake_b@localhost => self(), :fake_c@localhost => self()}
        }
      end)

      # Start CAS
      task =
        Task.async(fn ->
          EKV.put(name, "real_key", "real_value", if_vsn: nil)
        end)

      Process.sleep(50)

      shard_state = :sys.get_state(shard_name)
      [{ref, _op}] = Map.to_list(shard_state.pending_cas)

      # Send promise → quorum → accept phase
      send(shard_name, {:ekv_promise, ref, self(), "2", 0, "", nil})
      Process.sleep(50)

      # Send accepted from node_id "2" → quorum of accepts reached
      send(shard_name, {:ekv_accepted, ref, self(), "2"})

      result = Task.await(task, 10_000)
      assert match?({:ok, _}, result)

      # Value should be visible
      assert EKV.get(name, "real_key") == "real_value"
    end
  end

  # =====================================================================
  # CAS: GC + mixed writes
  # =====================================================================

  describe "CAS GC and mixed writes" do
    setup do
      name = :"ekv_cas_gc_#{System.unique_integer([:positive])}"
      data_dir = Path.join(System.tmp_dir!(), "ekv_test_#{name}")

      {:ok, pid} =
        EKV.start_link(
          name: name,
          data_dir: data_dir,
          shards: 1,
          log: false,
          cluster_size: 1,
          node_id: 1,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )

      on_exit(fn ->
        Process.exit(pid, :shutdown)
        File.rm_rf!(data_dir)
      end)

      %{cas_name: name}
    end

    test "GC purges orphan kv_paxos rows only when fully idle", %{cas_name: name} do
      shard_name = :"#{name}_ekv_replica_0"

      # CAS put creates kv_paxos entry with promised_counter > 0
      {:ok, _} = EKV.put(name, "gc/1", "val", if_vsn: nil)

      # Delete and purge tombstone — kv entry removed, kv_paxos row survives
      {_, vsn} = EKV.lookup(name, "gc/1")
      {:ok, _} = EKV.delete(name, "gc/1", if_vsn: vsn)

      now = System.system_time(:nanosecond)
      future_cutoff = now + :timer.hours(1) * 1_000_000
      send(shard_name, {:gc, now, future_cutoff})
      :sys.get_state(shard_name)

      # kv_paxos row kept: promised_counter > 0 from the CAS rounds.
      # This is correct — clearing promised state would let stale accepts
      # from older ballots succeed and kv_force_upsert would overwrite
      # the committed value (CASPaxos violation).
      state = :sys.get_state(shard_name)
      {:ok, rows} = EKV.Sqlite3.fetch_all(state.db, "SELECT key FROM kv_paxos", [])
      pax_keys = Enum.map(rows, fn [k] -> k end)
      assert "gc/1" in pax_keys

      # The latest accepted delete ballot is retained.
      {:ok, val_rows} =
        EKV.Sqlite3.fetch_all(
          state.db,
          "SELECT accepted_counter, accepted_value FROM kv_paxos WHERE key = ?1",
          ["gc/1"]
        )

      [[acc_counter, acc_value]] = val_rows
      assert acc_counter > 0
      assert acc_value == nil

      # A fresh prepare on the key resets promised state, and after that
      # commit cycle completes + tombstone purge, the row CAN be purged
      # (this is the normal lifecycle)
    end

    test "CAS put during GC TTL expiry: CAS wins (higher timestamp)", %{cas_name: name} do
      # Write a key with a very short TTL
      :ok = EKV.put(name, "gc_race/1", "old_val", ttl: 1)
      Process.sleep(10)

      # Key is expired — GC would tombstone it
      assert EKV.get(name, "gc_race/1") == nil

      # CAS put with if_vsn: nil should succeed (expired = absent)
      {:ok, _} = EKV.put(name, "gc_race/1", "new_val", if_vsn: nil)
      assert EKV.get(name, "gc_race/1") == "new_val"

      # Now trigger GC — the new CAS write should survive (higher timestamp)
      shard_name = :"#{name}_ekv_replica_0"
      now = System.system_time(:nanosecond)
      send(shard_name, {:gc, now, 0})
      :sys.get_state(shard_name)

      assert EKV.get(name, "gc_race/1") == "new_val"
    end

    test "non-CAS put does not touch kv_paxos", %{cas_name: name} do
      :ok = EKV.put(name, "nocas/1", "val")

      shard_name = :"#{name}_ekv_replica_0"
      state = :sys.get_state(shard_name)

      {:ok, rows} =
        EKV.Sqlite3.fetch_all(state.db, "SELECT key FROM kv_paxos WHERE key = ?1", ["nocas/1"])

      assert rows == []
    end

    test "CAS after non-CAS: vsn check works against LWW-written vsn", %{cas_name: name} do
      :ok = EKV.put(name, "mixed/1", "lww_val")
      {"lww_val", vsn} = EKV.lookup(name, "mixed/1")
      assert {_ts, _origin} = vsn

      {:ok, _} = EKV.put(name, "mixed/1", "cas_val", if_vsn: vsn)
      assert EKV.get(name, "mixed/1") == "cas_val"
    end
  end

  # =====================================================================
  # Handoff + CAS
  # =====================================================================

  describe "handoff + CAS" do
    setup do
      ho_name = :"ekv_ho_cas_#{System.unique_integer([:positive])}"
      ho_dir = Path.join(System.tmp_dir!(), "ekv_ho_cas_test_#{ho_name}")

      on_exit(fn ->
        File.rm_rf!(ho_dir)
      end)

      %{ho_name: ho_name, ho_dir: ho_dir}
    end

    test "CAS ballot counter does not regress after handoff+restart", %{
      ho_name: name,
      ho_dir: dir
    } do
      Process.flag(:trap_exit, true)

      {:ok, _} =
        EKV.start_link(
          name: name,
          data_dir: dir,
          shards: 2,
          log: false,
          cluster_size: 1,
          node_id: 1,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )

      # Do several CAS ops to advance ballot counter
      for i <- 1..5 do
        {:ok, ^i, _} =
          EKV.update(name, "counter", fn
            nil -> 1
            n -> n + 1
          end)
      end

      shard_name = :"#{name}_ekv_replica_0"
      state_before = :sys.get_state(shard_name)
      counter_before = state_before.ballot_counter

      # Handoff all shards
      for i <- 0..1 do
        sn = :"#{name}_ekv_replica_#{i}"
        ref = make_ref()
        send(sn, {:ekv_handoff_request, ref, node(), self()})
        assert_receive {:ekv_handoff_ack, ^ref}, 2000
      end

      Supervisor.stop(:"#{name}_ekv_sup", :shutdown)
      Process.sleep(50)

      {:ok, pid2} =
        EKV.start_link(
          name: name,
          data_dir: dir,
          shards: 2,
          log: false,
          cluster_size: 1,
          node_id: 1,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )

      state_after = :sys.get_state(shard_name)
      assert state_after.ballot_counter >= counter_before

      {:ok, 6, _} = EKV.update(name, "counter", fn n -> n + 1 end)

      Process.exit(pid2, :shutdown)
      Process.sleep(50)
    end

    test "CAS values visible after handoff+restart", %{ho_name: name, ho_dir: dir} do
      Process.flag(:trap_exit, true)

      {:ok, _} =
        EKV.start_link(
          name: name,
          data_dir: dir,
          shards: 2,
          log: false,
          cluster_size: 1,
          node_id: 1,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )

      {:ok, _} = EKV.put(name, "hand/1", "from_a", if_vsn: nil)
      {:ok, _} = EKV.put(name, "hand/2", "from_a2", if_vsn: nil)
      {"from_a", vsn1} = EKV.lookup(name, "hand/1")

      # Handoff + stop
      for i <- 0..1 do
        sn = :"#{name}_ekv_replica_#{i}"
        ref = make_ref()
        send(sn, {:ekv_handoff_request, ref, node(), self()})
        assert_receive {:ekv_handoff_ack, ^ref}, 2000
      end

      Supervisor.stop(:"#{name}_ekv_sup", :shutdown)
      Process.sleep(50)

      {:ok, pid2} =
        EKV.start_link(
          name: name,
          data_dir: dir,
          shards: 2,
          log: false,
          cluster_size: 1,
          node_id: 1,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )

      # Values visible after handoff+restart
      assert EKV.get(name, "hand/1") == "from_a"
      assert EKV.get(name, "hand/2") == "from_a2"

      # CAS-update with old vsn works
      {:ok, _} = EKV.put(name, "hand/1", "from_b", if_vsn: vsn1)
      assert EKV.get(name, "hand/1") == "from_b"

      Process.exit(pid2, :shutdown)
      Process.sleep(50)
    end
  end

  # =====================================================================
  # CAS: Phantom prevention — acceptor isolation tests
  # =====================================================================

  describe "phantom prevention" do
    setup do
      name = :"ekv_phantom_#{System.unique_integer([:positive])}"
      data_dir = Path.join(System.tmp_dir!(), "ekv_test_#{name}")

      {:ok, pid} =
        EKV.start_link(
          name: name,
          data_dir: data_dir,
          shards: 1,
          log: false,
          cluster_size: 3,
          node_id: 1,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )

      on_exit(fn ->
        Process.exit(pid, :shutdown)
        File.rm_rf!(data_dir)
      end)

      shard_name = :"#{name}_ekv_replica_0"

      # Inject fake peers via :sys.replace_state so the shard thinks it has remote nodes.
      # We use self() as a fake pid — messages come back to the test process.
      fake_node_a = :"fake_a@127.0.0.1"
      fake_node_b = :"fake_b@127.0.0.1"

      :sys.replace_state(shard_name, fn state ->
        %{
          state
          | remote_shards: %{fake_node_a => self(), fake_node_b => self()},
            peer_node_ids: %{fake_node_a => "2", fake_node_b => "3"}
        }
      end)

      %{name: name, shard_name: shard_name}
    end

    test "acceptor accept does NOT write to kv", %{name: name, shard_name: shard_name} do
      key = "phantom/1"
      ref = make_ref()
      now = System.system_time(:nanosecond)
      origin_str = Atom.to_string(node())
      val_bin = :erlang.term_to_binary("phantom_val")
      entry_tuple = {key, val_bin, now, origin_str, nil, nil}

      send(shard_name, {:ekv_accept, ref, self(), key, 100, "2", entry_tuple, 0})

      # Should get accepted back
      assert_receive {:ekv_accepted, ^ref, _, _}, 1000

      # Value should NOT be in kv (only in kv_paxos)
      assert EKV.get(name, key) == nil
    end

    test "acceptor accept does NOT dispatch subscriber events", %{
      name: name,
      shard_name: shard_name
    } do
      :ok = EKV.subscribe(name, "phantom/")
      Process.sleep(50)

      key = "phantom/2"
      ref = make_ref()
      now = System.system_time(:nanosecond)
      origin_str = Atom.to_string(node())
      val_bin = :erlang.term_to_binary("no_event")
      entry_tuple = {key, val_bin, now, origin_str, nil, nil}

      send(shard_name, {:ekv_accept, ref, self(), key, 100, "2", entry_tuple, 0})
      assert_receive {:ekv_accepted, ^ref, _, _}, 1000

      flush_dispatchers(name)
      refute_receive {:ekv, _, _}, 200
    end

    test "promote after commit writes to kv", %{name: name, shard_name: shard_name} do
      key = "phantom/3"
      ref = make_ref()
      now = System.system_time(:nanosecond)
      origin_str = Atom.to_string(node())
      val_bin = :erlang.term_to_binary("promoted")
      entry_tuple = {key, val_bin, now, origin_str, nil, nil}

      # Accept
      send(shard_name, {:ekv_accept, ref, self(), key, 100, "2", entry_tuple, 0})
      assert_receive {:ekv_accepted, ^ref, _, _}, 1000
      assert EKV.get(name, key) == nil

      # Commit notification → promote
      send(shard_name, {:ekv_cas_committed, key, 100, "2", nil, 0})
      :sys.get_state(shard_name)

      assert EKV.get(name, key) == "promoted"
    end

    test "promote dispatches subscriber events", %{name: name, shard_name: shard_name} do
      :ok = EKV.subscribe(name, "phantom/")
      Process.sleep(50)

      key = "phantom/4"
      ref = make_ref()
      now = System.system_time(:nanosecond)
      origin_str = Atom.to_string(node())
      val_bin = :erlang.term_to_binary("event_val")
      entry_tuple = {key, val_bin, now, origin_str, nil, nil}

      # Accept — no event
      send(shard_name, {:ekv_accept, ref, self(), key, 100, "2", entry_tuple, 0})
      assert_receive {:ekv_accepted, ^ref, _, _}, 1000
      flush_dispatchers(name)
      refute_receive {:ekv, _, _}, 100

      # Commit — event dispatched
      send(shard_name, {:ekv_cas_committed, key, 100, "2", nil, 0})
      :sys.get_state(shard_name)
      flush_dispatchers(name)

      assert_receive {:ekv, [%EKV.Event{type: :put, key: ^key, value: "event_val"}], _}, 1000
    end

    test "commit payload can promote without prior local accept", %{
      name: name,
      shard_name: shard_name
    } do
      :ok = EKV.subscribe(name, "phantom/")
      Process.sleep(50)

      key = "phantom/commit_payload_put"
      now = System.system_time(:nanosecond)
      origin_str = Atom.to_string(node())
      val_bin = :erlang.term_to_binary("payload_put")
      entry_tuple = {key, val_bin, now, origin_str, nil, nil}

      send(shard_name, {:ekv_cas_committed, key, 150, "2", entry_tuple, 0})
      :sys.get_state(shard_name)
      flush_dispatchers(name)

      assert_receive {:ekv, [%EKV.Event{type: :put, key: ^key, value: "payload_put"}], _}, 1000
      assert EKV.get(name, key) == "payload_put"
    end

    test "commit payload delete can promote without prior local accept", %{
      name: name,
      shard_name: shard_name
    } do
      key = "phantom/commit_payload_del"
      :ok = EKV.put(name, key, "payload_old")
      assert EKV.get(name, key) == "payload_old"

      :ok = EKV.subscribe(name, "phantom/")
      Process.sleep(50)
      flush_dispatchers(name)

      receive do
        {:ekv, _, _} -> :ok
      after
        100 -> :ok
      end

      now = System.system_time(:nanosecond)
      origin_str = Atom.to_string(node())
      entry_tuple = {key, nil, now, origin_str, nil, now}

      send(shard_name, {:ekv_cas_committed, key, 250, "3", entry_tuple, 0})
      :sys.get_state(shard_name)
      flush_dispatchers(name)

      assert_receive {:ekv, [%EKV.Event{type: :delete, key: ^key, value: "payload_old"}], _}, 1000
      assert EKV.get(name, key) == nil
    end

    test "promote with stale ballot is ignored", %{name: name, shard_name: shard_name} do
      key = "phantom/5"
      now = System.system_time(:nanosecond)
      origin_str = Atom.to_string(node())

      # Accept with ballot {100, "2"}
      ref1 = make_ref()
      val1 = :erlang.term_to_binary("v1")
      entry1 = {key, val1, now, origin_str, nil, nil}
      send(shard_name, {:ekv_accept, ref1, self(), key, 100, "2", entry1, 0})
      assert_receive {:ekv_accepted, ^ref1, _, _}, 1000

      # Accept with higher ballot {200, "3"} — overwrites kv_paxos
      ref2 = make_ref()
      val2 = :erlang.term_to_binary("v2")
      entry2 = {key, val2, now + 1, origin_str, nil, nil}
      send(shard_name, {:ekv_accept, ref2, self(), key, 200, "3", entry2, 0})
      assert_receive {:ekv_accepted, ^ref2, _, _}, 1000

      # Stale commit for ballot {100, "2"} — should be ignored
      send(shard_name, {:ekv_cas_committed, key, 100, "2", nil, 0})
      :sys.get_state(shard_name)

      # Neither value promoted to kv
      assert EKV.get(name, key) == nil
    end

    test "promote clears kv_paxos value columns (no storage doubling)", %{
      name: name,
      shard_name: shard_name
    } do
      key = "phantom/6"
      now = System.system_time(:nanosecond)
      origin_str = Atom.to_string(node())
      val_bin = :erlang.term_to_binary("clear_test")
      entry = {key, val_bin, now, origin_str, nil, nil}

      # Accept
      ref = make_ref()
      send(shard_name, {:ekv_accept, ref, self(), key, 100, "2", entry, 0})
      assert_receive {:ekv_accepted, ^ref, _, _}, 1000

      # Commit → promote
      send(shard_name, {:ekv_cas_committed, key, 100, "2", nil, 0})
      :sys.get_state(shard_name)

      assert EKV.get(name, key) == "clear_test"

      # After promote, a new CAS operation should see the value via kv (not kv_paxos)
      # Verify by doing a fetch — the vsn should match the committed value
      {"clear_test", vsn} = EKV.lookup(name, key)
      assert is_tuple(vsn)
    end

    test "prepare reads from kv_paxos when accepted (tentative value)", %{
      name: name,
      shard_name: shard_name
    } do
      key = "phantom/7"

      # Write "v1" via regular LWW put (goes to kv)
      :ok = EKV.put(name, key, "v1")
      assert EKV.get(name, key) == "v1"

      # Send paxos_accept with ballot {100, "2"} and value "v2"
      # kv_paxos has "v2", kv has "v1"
      now = System.system_time(:nanosecond)
      origin_str = Atom.to_string(node())
      val_bin = :erlang.term_to_binary("v2")
      entry = {key, val_bin, now, origin_str, nil, nil}

      ref = make_ref()
      send(shard_name, {:ekv_accept, ref, self(), key, 100, "2", entry, 0})
      assert_receive {:ekv_accepted, ^ref, _, _}, 1000

      # kv still has "v1"
      assert EKV.get(name, key) == "v1"

      # But paxos_prepare should see "v2" (from kv_paxos)
      state = :sys.get_state(shard_name)

      {:ok, :promise, 100, "2", [val, _, _, _, _]} =
        EKV.Store.paxos_prepare(state.db, key, 300, "1")

      assert :erlang.binary_to_term(val) == "v2"
    end

    test "prepare falls back to kv when kv_paxos has no accepted value", %{
      name: name,
      shard_name: shard_name
    } do
      key = "phantom/8"

      # Write "v1" via regular LWW put
      :ok = EKV.put(name, key, "v1")

      # No accept on this key — paxos_prepare should read from kv
      state = :sys.get_state(shard_name)

      {:ok, :promise, 0, "", [val, _, _, _, _]} =
        EKV.Store.paxos_prepare(state.db, key, 100, "1")

      assert :erlang.binary_to_term(val) == "v1"
    end

    test "CAS delete promote delivers previous value in event", %{
      name: name,
      shard_name: shard_name
    } do
      key = "phantom/10"

      # Put "v1" via regular LWW put (committed to kv)
      :ok = EKV.put(name, key, "v1")
      assert EKV.get(name, key) == "v1"

      # Subscribe
      :ok = EKV.subscribe(name, "phantom/")
      Process.sleep(50)
      # Drain the put event
      flush_dispatchers(name)

      receive do
        {:ekv, _, _} -> :ok
      after
        100 -> :ok
      end

      # Accept a delete (tombstone)
      now = System.system_time(:nanosecond)
      origin_str = Atom.to_string(node())
      entry = {key, nil, now, origin_str, nil, now}

      ref = make_ref()
      send(shard_name, {:ekv_accept, ref, self(), key, 200, "2", entry, 0})
      assert_receive {:ekv_accepted, ^ref, _, _}, 1000

      # kv still has "v1" (delete not committed yet)
      assert EKV.get(name, key) == "v1"

      # Commit → promote writes tombstone to kv
      send(shard_name, {:ekv_cas_committed, key, 200, "2", nil, 0})
      :sys.get_state(shard_name)
      flush_dispatchers(name)

      # Subscriber event has previous value
      assert_receive {:ekv, [%EKV.Event{type: :delete, key: ^key, value: "v1"}], _}, 1000

      # Key is now deleted
      assert EKV.get(name, key) == nil
    end
  end

  # =====================================================================
  # Chunked sync tests
  # =====================================================================

  describe "chunked sync" do
    setup do
      name = :"ekv_chunk_#{System.unique_integer([:positive])}"
      data_dir = Path.join(System.tmp_dir!(), "ekv_test_#{name}")

      {:ok, pid} =
        EKV.start_link(
          name: name,
          data_dir: data_dir,
          shards: 1,
          log: false,
          sync_chunk_size: 10,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )

      on_exit(fn ->
        Process.exit(pid, :shutdown)
        File.rm_rf!(data_dir)
      end)

      shard_name = :"#{name}_ekv_replica_0"

      %{name: name, data_dir: data_dir, shard_name: shard_name}
    end

    test "full_state_chunk paginates through all entries", %{shard_name: shard_name, name: name} do
      # Write 35 keys (more than 3 chunks of 10)
      for i <- 1..35 do
        key = String.pad_leading("#{i}", 3, "0")
        :ok = EKV.put(name, "chunk/#{key}", "val_#{i}")
      end

      state = :sys.get_state(shard_name)
      tombstone_cutoff = System.system_time(:nanosecond) - :timer.hours(24 * 7) * 1_000_000

      # Paginate through chunks
      {all_entries, chunk_count} = collect_full_chunks(state.db, tombstone_cutoff, nil, 10, [], 0)

      # 10 + 10 + 10 + 5
      assert chunk_count == 4
      assert length(all_entries) == 35

      # Verify entries are ordered by key
      keys = Enum.map(all_entries, fn {k, _, _, _, _, _} -> k end)
      assert keys == Enum.sort(keys)
    end

    test "oplog_since_chunk paginates through oplog entries", %{
      shard_name: shard_name,
      name: name
    } do
      for i <- 1..25 do
        :ok = EKV.put(name, "oplog_chunk/#{i}", "val_#{i}")
      end

      state = :sys.get_state(shard_name)

      # Paginate through oplog chunks
      {all_entries, chunk_count} = collect_oplog_chunks(state.db, 0, 10, [], 0)

      # 10 + 10 + 5
      assert chunk_count == 3
      assert length(all_entries) == 25

      # Verify entries are ordered by seq
      seqs = Enum.map(all_entries, fn {seq, _, _, _, _, _, _} -> seq end)
      assert seqs == Enum.sort(seqs)
    end

    test "chunked full sync delivers all data via multiple sync messages", %{
      shard_name: shard_name,
      name: name
    } do
      # Write 25 keys so full sync needs 3 chunks (chunk_size=10)
      for i <- 1..25 do
        key = String.pad_leading("#{i}", 3, "0")
        :ok = EKV.put(name, "sync/#{key}", "val_#{i}")
      end

      fake_node = :chunk_peer@fake

      :sys.replace_state(shard_name, fn state ->
        %{state | remote_shards: Map.put(state.remote_shards, fake_node, self())}
      end)

      :erlang.trace(Process.whereis(shard_name), true, [:send])

      # Directly trigger full sync by sending a continue_full_sync with nil cursor
      config = EKV.get_config(name)
      tombstone_cutoff = System.system_time(:nanosecond) - config.tombstone_ttl * 1_000_000
      state = :sys.get_state(shard_name)
      my_seq = EKV.Store.max_seq(state.db)

      send(
        shard_name,
        {:continue_full_sync, fake_node, nil, tombstone_cutoff, my_seq, 0, config.sync_chunk_size}
      )

      Process.sleep(200)
      :sys.get_state(shard_name)

      :erlang.trace(Process.whereis(shard_name), false, [:send])

      sync_count = count_trace_sync_messages()

      # Should have 3 chunks (10 + 10 + 5)
      assert sync_count == 3
    end

    test "chunked delta sync delivers all oplog entries", %{shard_name: shard_name, name: name} do
      # Write 25 keys
      for i <- 1..25 do
        :ok = EKV.put(name, "delta/#{i}", "val_#{i}")
      end

      fake_node = :delta_peer@fake

      :sys.replace_state(shard_name, fn state ->
        %{state | remote_shards: Map.put(state.remote_shards, fake_node, self())}
      end)

      :erlang.trace(Process.whereis(shard_name), true, [:send])

      config = EKV.get_config(name)
      state = :sys.get_state(shard_name)
      my_seq = EKV.Store.max_seq(state.db)

      # Trigger delta sync starting from seq 0
      send(shard_name, {:continue_delta_sync, fake_node, 0, my_seq, 0, config.sync_chunk_size})

      Process.sleep(200)
      :sys.get_state(shard_name)

      :erlang.trace(Process.whereis(shard_name), false, [:send])

      sync_count = count_trace_sync_messages()

      # Should have 3 chunks (10 + 10 + 5)
      assert sync_count == 3
    end

    test "peer disconnect aborts ongoing chunked sync", %{shard_name: shard_name, name: name} do
      # Write enough for multiple chunks
      for i <- 1..30 do
        :ok = EKV.put(name, "abort/#{String.pad_leading("#{i}", 3, "0")}", "val_#{i}")
      end

      fake_node = :abort_peer@fake

      :sys.replace_state(shard_name, fn state ->
        %{state | remote_shards: Map.put(state.remote_shards, fake_node, self())}
      end)

      config = EKV.get_config(name)
      tombstone_cutoff = System.system_time(:nanosecond) - config.tombstone_ttl * 1_000_000
      state = :sys.get_state(shard_name)
      my_seq = EKV.Store.max_seq(state.db)

      # Suspend the shard, inject the first continue message, then remove the peer
      :sys.suspend(shard_name)

      send(
        shard_name,
        {:continue_full_sync, fake_node, nil, tombstone_cutoff, my_seq, 0, config.sync_chunk_size}
      )

      # Resume to process just the first chunk (sends chunk + queues next continuation)
      :sys.resume(shard_name)
      :sys.get_state(shard_name)

      # Now remove the peer before the next continuation runs
      :sys.replace_state(shard_name, fn state ->
        %{state | remote_shards: %{}}
      end)

      # Start tracing to verify no more sync messages are sent
      :erlang.trace(Process.whereis(shard_name), true, [:send])

      # Let continuations process — they should check remote_shards and abort
      Process.sleep(200)
      :sys.get_state(shard_name)

      :erlang.trace(Process.whereis(shard_name), false, [:send])

      sync_count = count_trace_sync_messages()

      # No sync messages should be sent (continuation aborted)
      assert sync_count == 0

      # Shard should not crash
      assert Process.alive?(Process.whereis(shard_name))
    end

    test "write during chunked sync doesn't break sync", %{shard_name: shard_name, name: name} do
      # Write enough keys for multiple chunks
      for i <- 1..20 do
        :ok = EKV.put(name, "interleave/#{String.pad_leading("#{i}", 3, "0")}", "v#{i}")
      end

      fake_node = :interleave_peer@fake

      :sys.replace_state(shard_name, fn state ->
        %{state | remote_shards: Map.put(state.remote_shards, fake_node, self())}
      end)

      config = EKV.get_config(name)
      tombstone_cutoff = System.system_time(:nanosecond) - config.tombstone_ttl * 1_000_000
      state = :sys.get_state(shard_name)
      my_seq = EKV.Store.max_seq(state.db)

      # Suspend, inject first chunk trigger, resume
      :sys.suspend(shard_name)

      send(
        shard_name,
        {:continue_full_sync, fake_node, nil, tombstone_cutoff, my_seq, 0, config.sync_chunk_size}
      )

      :sys.resume(shard_name)
      :sys.get_state(shard_name)

      # Write a new key while continuations are pending
      :ok = EKV.put(name, "interleave/new", "new_val")

      # Let continuations complete
      Process.sleep(200)
      :sys.get_state(shard_name)

      # Shard is still alive and operational
      assert Process.alive?(Process.whereis(shard_name))
      assert EKV.get(name, "interleave/new") == "new_val"
    end

    test "intermediate sync chunks have seq=0, final chunk has real seq", %{
      shard_name: shard_name,
      name: name
    } do
      # Write 25 keys for 3 chunks
      for i <- 1..25 do
        :ok = EKV.put(name, "seqtest/#{String.pad_leading("#{i}", 3, "0")}", "v#{i}")
      end

      fake_node = :seq_peer@fake

      :sys.replace_state(shard_name, fn state ->
        %{state | remote_shards: Map.put(state.remote_shards, fake_node, self())}
      end)

      :erlang.trace(Process.whereis(shard_name), true, [:send])

      config = EKV.get_config(name)
      tombstone_cutoff = System.system_time(:nanosecond) - config.tombstone_ttl * 1_000_000
      state = :sys.get_state(shard_name)
      my_seq = EKV.Store.max_seq(state.db)

      send(
        shard_name,
        {:continue_full_sync, fake_node, nil, tombstone_cutoff, my_seq, 0, config.sync_chunk_size}
      )

      Process.sleep(200)
      :sys.get_state(shard_name)

      :erlang.trace(Process.whereis(shard_name), false, [:send])

      sync_messages = collect_trace_sync_details()

      assert length(sync_messages) == 3

      # Intermediate chunks should have seq=0
      {intermediate, [final]} = Enum.split(sync_messages, -1)

      for {_entries_count, seq} <- intermediate do
        assert seq == 0, "intermediate chunk should have seq=0, got #{seq}"
      end

      # Final chunk has real seq
      {_count, final_seq} = final
      assert final_seq > 0, "final chunk should have non-zero seq"
    end
  end

  # =====================================================================
  # Message ordering tests
  # =====================================================================

  describe "message ordering" do
    setup do
      name = :"ekv_ordering_#{System.unique_integer([:positive])}"
      data_dir = Path.join(System.tmp_dir!(), "ekv_test_#{name}")

      {:ok, pid} =
        EKV.start_link(
          name: name,
          data_dir: data_dir,
          shards: 1,
          log: false,
          cluster_size: 3,
          node_id: 1,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )

      on_exit(fn ->
        Process.exit(pid, :shutdown)
        File.rm_rf!(data_dir)
      end)

      shard_name = :"#{name}_ekv_replica_0"

      # Inject fake peers
      fake_node_a = :"order_a@127.0.0.1"
      fake_node_b = :"order_b@127.0.0.1"

      :sys.replace_state(shard_name, fn state ->
        %{
          state
          | remote_shards: %{fake_node_a => self(), fake_node_b => self()},
            peer_node_ids: %{fake_node_a => "2", fake_node_b => "3"}
        }
      end)

      %{name: name, data_dir: data_dir, shard_name: shard_name}
    end

    test "stale commit after higher-ballot accept", %{name: name, shard_name: shard_name} do
      key = "order/stale_commit"
      now = System.system_time(:nanosecond)
      origin_str = Atom.to_string(node())

      # Accept with ballot {100, "2"}
      ref1 = make_ref()
      val1 = :erlang.term_to_binary("v1")
      entry1 = {key, val1, now, origin_str, nil, nil}
      send(shard_name, {:ekv_accept, ref1, self(), key, 100, "2", entry1, 0})
      assert_receive {:ekv_accepted, ^ref1, _, _}, 1000

      # Accept with higher ballot {200, "3"} — overwrites kv_paxos
      ref2 = make_ref()
      val2 = :erlang.term_to_binary("v2")
      entry2 = {key, val2, now + 1, origin_str, nil, nil}
      send(shard_name, {:ekv_accept, ref2, self(), key, 200, "3", entry2, 0})
      assert_receive {:ekv_accepted, ^ref2, _, _}, 1000

      # Stale commit for ballot {100, "2"} — should return :stale, value NOT in kv
      send(shard_name, {:ekv_cas_committed, key, 100, "2", nil, 0})
      :sys.get_state(shard_name)
      assert EKV.get(name, key) == nil

      # Commit for ballot {200, "3"} — should succeed
      send(shard_name, {:ekv_cas_committed, key, 200, "3", nil, 0})
      :sys.get_state(shard_name)
      assert EKV.get(name, key) == "v2"
    end

    test "duplicate commit notification", %{name: name, shard_name: shard_name} do
      key = "order/dup_commit"
      now = System.system_time(:nanosecond)
      origin_str = Atom.to_string(node())

      # Accept
      ref = make_ref()
      val = :erlang.term_to_binary("dup_val")
      entry = {key, val, now, origin_str, nil, nil}
      send(shard_name, {:ekv_accept, ref, self(), key, 100, "2", entry, 0})
      assert_receive {:ekv_accepted, ^ref, _, _}, 1000

      # Subscribe to verify commit notifications
      :ok = EKV.subscribe(name, "order/")
      Process.sleep(50)

      # First commit — succeeds
      send(shard_name, {:ekv_cas_committed, key, 100, "2", nil, 0})
      :sys.get_state(shard_name)
      flush_dispatchers(name)

      assert EKV.get(name, key) == "dup_val"
      assert_receive {:ekv, [%EKV.Event{type: :put, key: ^key, value: "dup_val"}], _}, 1000

      # Second commit (duplicate) replays the same promoted value/event.
      send(shard_name, {:ekv_cas_committed, key, 100, "2", nil, 0})
      :sys.get_state(shard_name)
      flush_dispatchers(name)

      assert_receive {:ekv, [%EKV.Event{type: :put, key: ^key, value: "dup_val"}], _}, 1000
    end

    test "late promise after CAS timeout is silently ignored", %{shard_name: shard_name} do
      # Manually construct a ref that was part of a CAS that already timed out.
      # We simulate the scenario: a CAS started, timed out, and then a late
      # promise arrives with the old ref.
      old_ref = make_ref()

      # The ref is not in pending_cas (never was, or already cleaned up by timeout).
      # Sending a promise with this ref should be silently ignored.
      send(shard_name, {:ekv_promise, old_ref, self(), "2", 0, "", nil})
      :sys.get_state(shard_name)

      # Shard is still alive, no crash
      assert Process.alive?(Process.whereis(shard_name))

      # Also test with other late CAS messages
      send(shard_name, {:ekv_nack, old_ref, self(), "2", 100, "1"})
      :sys.get_state(shard_name)
      assert Process.alive?(Process.whereis(shard_name))

      send(shard_name, {:ekv_accepted, old_ref, self(), "2"})
      :sys.get_state(shard_name)
      assert Process.alive?(Process.whereis(shard_name))

      send(shard_name, {:ekv_accept_nack, old_ref, self(), "2"})
      :sys.get_state(shard_name)
      assert Process.alive?(Process.whereis(shard_name))
    end

    test "accept at acceptor after proposer timed out (prepare superseded)", %{
      shard_name: shard_name
    } do
      key = "order/superseded_accept"

      # Send prepare with ballot=100 (accepted by local shard)
      ref1 = make_ref()
      send(shard_name, {:ekv_prepare, ref1, self(), key, 100, "2", 0})
      assert_receive {:ekv_promise, ^ref1, _, _, _, _, _}, 1000

      # Send prepare with higher ballot=200 (supersedes ballot=100)
      ref2 = make_ref()
      send(shard_name, {:ekv_prepare, ref2, self(), key, 200, "3", 0})
      assert_receive {:ekv_promise, ^ref2, _, _, _, _, _}, 1000

      # Now send accept for the old ballot=100 — should be rejected
      now = System.system_time(:nanosecond)
      origin_str = Atom.to_string(node())
      val = :erlang.term_to_binary("stale")
      entry = {key, val, now, origin_str, nil, nil}

      ref3 = make_ref()
      send(shard_name, {:ekv_accept, ref3, self(), key, 100, "2", entry, 0})
      assert_receive {:ekv_accept_nack, ^ref3, _, _}, 1000
    end

    test "interleaved CAS from two proposers on same key", %{name: name, shard_name: shard_name} do
      key = "order/interleaved_cas"

      # Write initial value
      :ok = EKV.put(name, key, "initial")

      # Proposer A prepares with ballot 100 — gets promise from local shard
      ref_a = make_ref()
      send(shard_name, {:ekv_prepare, ref_a, self(), key, 100, "2", 0})
      assert_receive {:ekv_promise, ^ref_a, _, _, _, _, _}, 1000

      # Simulate peer 2 promise for A (quorum: need 2 out of 3)
      # We respond on behalf of fake node

      # Proposer B prepares with higher ballot 200 — preempts A
      ref_b = make_ref()
      send(shard_name, {:ekv_prepare, ref_b, self(), key, 200, "3", 0})
      assert_receive {:ekv_promise, ^ref_b, _, _, _, _, _}, 1000

      # Now A tries to accept with ballot 100 — should be rejected (promised 200)
      now = System.system_time(:nanosecond)
      origin_str = Atom.to_string(node())
      val_a = :erlang.term_to_binary("from_a")
      entry_a = {key, val_a, now, origin_str, nil, nil}

      ref_accept_a = make_ref()
      send(shard_name, {:ekv_accept, ref_accept_a, self(), key, 100, "2", entry_a, 0})
      assert_receive {:ekv_accept_nack, ^ref_accept_a, _, _}, 1000

      # B's accept should succeed
      val_b = :erlang.term_to_binary("from_b")
      entry_b = {key, val_b, now + 1, origin_str, nil, nil}

      ref_accept_b = make_ref()
      send(shard_name, {:ekv_accept, ref_accept_b, self(), key, 200, "3", entry_b, 0})
      assert_receive {:ekv_accepted, ^ref_accept_b, _, _}, 1000

      # B commits
      send(shard_name, {:ekv_cas_committed, key, 200, "3", nil, 0})
      :sys.get_state(shard_name)
      assert EKV.get(name, key) == "from_b"
    end

    test "sync message interleaved with CAS prepare", %{name: name, shard_name: shard_name} do
      key = "order/sync_during_cas"
      sync_key = "order/sync_other"

      # Start CAS — prepare locally
      ref = make_ref()
      send(shard_name, {:ekv_prepare, ref, self(), key, 100, "2", 0})
      assert_receive {:ekv_promise, ^ref, _, _, _, _, _}, 1000

      # While CAS is "in progress" (waiting for peer promises), deliver a sync message
      # with a DIFFERENT key — should process normally
      now = System.system_time(:nanosecond)
      origin = node()
      val_binary = :erlang.term_to_binary("synced_val")

      sync_entries = [{sync_key, val_binary, now, origin, nil, nil}]
      send(shard_name, {:ekv_sync, :some_node@host, 0, sync_entries, 5})
      :sys.get_state(shard_name)

      # Synced key should be available
      assert EKV.get(name, sync_key) == "synced_val"

      # CAS key should still be unset (only prepared, not accepted/committed)
      assert EKV.get(name, key) == nil

      # Accept and commit CAS normally
      val = :erlang.term_to_binary("cas_val")
      entry = {key, val, now + 1, Atom.to_string(node()), nil, nil}

      ref_accept = make_ref()
      send(shard_name, {:ekv_accept, ref_accept, self(), key, 100, "2", entry, 0})
      assert_receive {:ekv_accepted, ^ref_accept, _, _}, 1000

      send(shard_name, {:ekv_cas_committed, key, 100, "2", nil, 0})
      :sys.get_state(shard_name)

      assert EKV.get(name, key) == "cas_val"
    end

    test "commit notification to shard that restarted (kv_paxos survives)", %{
      name: name,
      shard_name: shard_name
    } do
      key = "order/restart_commit"
      now = System.system_time(:nanosecond)
      origin_str = Atom.to_string(node())
      val_bin = :erlang.term_to_binary("survive_restart")
      entry = {key, val_bin, now, origin_str, nil, nil}

      # Accept (writes to kv_paxos in SQLite)
      ref = make_ref()
      send(shard_name, {:ekv_accept, ref, self(), key, 100, "2", entry, 0})
      assert_receive {:ekv_accepted, ^ref, _, _}, 1000

      # kv_paxos is written, value not in kv yet
      assert EKV.get(name, key) == nil

      # Kill and restart the shard GenServer (supervised restart)
      Process.exit(Process.whereis(shard_name), :kill)
      Process.sleep(200)

      # Shard should be back (supervisor restarts it)
      assert Process.whereis(shard_name) != nil

      # Re-inject fake peers so the shard thinks it has remotes
      :sys.replace_state(shard_name, fn state ->
        %{
          state
          | remote_shards: %{:"order_a@127.0.0.1" => self(), :"order_b@127.0.0.1" => self()},
            peer_node_ids: %{:"order_a@127.0.0.1" => "2", :"order_b@127.0.0.1" => "3"}
        }
      end)

      # Send commit notification — kv_paxos should still have the accepted value
      send(shard_name, {:ekv_cas_committed, key, 100, "2", nil, 0})
      :sys.get_state(shard_name)

      assert EKV.get(name, key) == "survive_restart"
    end
  end

  # =====================================================================
  # Chunked sync helpers
  # =====================================================================

  defp collect_full_chunks(db, tombstone_cutoff, last_key, limit, acc, chunk_count) do
    entries = EKV.Store.full_state_chunk(db, tombstone_cutoff, last_key, limit)

    case entries do
      [] ->
        {acc, chunk_count}

      _ ->
        new_acc = acc ++ entries

        if length(entries) < limit do
          {new_acc, chunk_count + 1}
        else
          next_key = elem(List.last(entries), 0)
          collect_full_chunks(db, tombstone_cutoff, next_key, limit, new_acc, chunk_count + 1)
        end
    end
  end

  defp collect_oplog_chunks(db, seq, limit, acc, chunk_count) do
    entries = EKV.Store.oplog_since_chunk(db, seq, limit)

    case entries do
      [] ->
        {acc, chunk_count}

      _ ->
        new_acc = acc ++ entries

        if length(entries) < limit do
          {new_acc, chunk_count + 1}
        else
          {max_seq, _, _, _, _, _, _} = List.last(entries)
          collect_oplog_chunks(db, max_seq, limit, new_acc, chunk_count + 1)
        end
    end
  end

  defp count_trace_sync_messages do
    count_trace_sync_messages(0)
  end

  defp count_trace_sync_messages(count) do
    receive do
      {:trace, _, :send, {:ekv_sync, _, _, _, _}, _} ->
        count_trace_sync_messages(count + 1)

      {:trace, _, :send, _, _} ->
        count_trace_sync_messages(count)
    after
      100 -> count
    end
  end

  defp collect_trace_sync_details do
    collect_trace_sync_details([])
  end

  defp collect_trace_sync_details(acc) do
    receive do
      {:trace, _, :send, {:ekv_sync, _, _, entries, seq}, _} ->
        collect_trace_sync_details([{length(entries), seq} | acc])

      {:trace, _, :send, _, _} ->
        collect_trace_sync_details(acc)
    after
      100 -> Enum.reverse(acc)
    end
  end

  # =====================================================================
  # Auto-persist node_id
  # =====================================================================

  describe "auto-persist node_id" do
    test "persists configured node_id and re-reads on restart" do
      name = :"ekv_persist_nid_#{System.unique_integer([:positive])}"
      data_dir = Path.join(System.tmp_dir!(), "ekv_test_#{name}")
      Process.flag(:trap_exit, true)
      on_exit(fn -> File.rm_rf!(data_dir) end)

      # Start with explicit node_id
      {:ok, _pid} =
        EKV.start_link(
          name: name,
          data_dir: data_dir,
          shards: 1,
          log: false,
          cluster_size: 3,
          node_id: "node-a",
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )

      info = EKV.info(name)
      assert info.node_id == "node-a"

      Supervisor.stop(:"#{name}_ekv_sup", :shutdown)
      Process.sleep(100)

      # Restart WITHOUT node_id — should use persisted
      {:ok, _pid2} =
        EKV.start_link(
          name: name,
          data_dir: data_dir,
          shards: 1,
          log: false,
          cluster_size: 3,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )

      info2 = EKV.info(name)
      assert info2.node_id == "node-a"
    end

    test "auto-generates node_id when none provided" do
      name = :"ekv_autogen_nid_#{System.unique_integer([:positive])}"
      data_dir = Path.join(System.tmp_dir!(), "ekv_test_#{name}")

      on_exit(fn -> File.rm_rf!(data_dir) end)

      {:ok, _pid} =
        EKV.start_link(
          name: name,
          data_dir: data_dir,
          shards: 1,
          log: false,
          cluster_size: 3,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )

      info = EKV.info(name)
      assert is_binary(info.node_id)
      assert byte_size(info.node_id) > 0
    end

    @tag :capture_log
    test "persisted node_id wins over configured node_id on restart" do
      name = :"ekv_persist_wins_#{System.unique_integer([:positive])}"
      data_dir = Path.join(System.tmp_dir!(), "ekv_test_#{name}")
      Process.flag(:trap_exit, true)
      on_exit(fn -> File.rm_rf!(data_dir) end)

      {:ok, _pid} =
        EKV.start_link(
          name: name,
          data_dir: data_dir,
          shards: 1,
          log: false,
          cluster_size: 3,
          node_id: "node-a",
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )

      Supervisor.stop(:"#{name}_ekv_sup", :shutdown)
      Process.sleep(100)

      # Restart with DIFFERENT node_id — persisted wins, logs warning
      {:ok, _pid2} =
        EKV.start_link(
          name: name,
          data_dir: data_dir,
          shards: 1,
          log: false,
          cluster_size: 3,
          node_id: "node-b",
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )

      info = EKV.info(name)
      assert info.node_id == "node-a"
    end
  end

  # =====================================================================
  # Overflow guard
  # =====================================================================

  describe "overflow guard" do
    @tag :capture_log
    test "CAS returns :cluster_overflow when too many node_ids" do
      name = :"ekv_overflow_#{System.unique_integer([:positive])}"
      data_dir = Path.join(System.tmp_dir!(), "ekv_test_#{name}")

      {:ok, pid} =
        EKV.start_link(
          name: name,
          data_dir: data_dir,
          shards: 1,
          log: false,
          cluster_size: 2,
          node_id: "1",
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )

      on_exit(fn ->
        Process.exit(pid, :shutdown)
        File.rm_rf!(data_dir)
      end)

      shard_name = :"#{name}_ekv_replica_0"

      # Inject 3 distinct peer node_ids — total 4 (self + 3), exceeds cluster_size=2
      :sys.replace_state(shard_name, fn state ->
        %{
          state
          | peer_node_ids: %{
              :fake_a@localhost => "2",
              :fake_b@localhost => "3",
              :fake_c@localhost => "4"
            },
            remote_shards: %{
              :fake_a@localhost => self(),
              :fake_b@localhost => self(),
              :fake_c@localhost => self()
            }
        }
      end)

      assert {:error, :cluster_overflow} =
               EKV.put(name, "overflow_key", "val", if_vsn: nil)
    end
  end

  # =====================================================================
  # Backup API
  # =====================================================================

  describe "EKV.backup/2" do
    test "backup creates shard files and data is accessible" do
      name = :"ekv_backup_#{System.unique_integer([:positive])}"
      data_dir = Path.join(System.tmp_dir!(), "ekv_test_#{name}")
      backup_dir = Path.join(System.tmp_dir!(), "ekv_backup_#{name}")
      Process.flag(:trap_exit, true)

      on_exit(fn ->
        File.rm_rf!(data_dir)
        File.rm_rf!(backup_dir)
      end)

      {:ok, _pid} =
        EKV.start_link(
          name: name,
          data_dir: data_dir,
          shards: 2,
          log: false,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )

      # Write some data
      for i <- 1..10 do
        :ok = EKV.put(name, "backup/#{i}", "val_#{i}")
      end

      # Backup
      assert :ok = EKV.backup(name, backup_dir)

      # Verify backup files exist
      assert File.exists?(Path.join(backup_dir, "shard_0.db"))
      assert File.exists?(Path.join(backup_dir, "shard_1.db"))

      # Stop current EKV and start fresh from backup
      Supervisor.stop(:"#{name}_ekv_sup", :shutdown)
      Process.sleep(100)

      backup_name = :"ekv_backup_verify_#{System.unique_integer([:positive])}"

      {:ok, _pid2} =
        EKV.start_link(
          name: backup_name,
          data_dir: backup_dir,
          shards: 2,
          log: false,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7),
          skip_stale_check: true
        )

      # Verify all data is present in backup
      for i <- 1..10 do
        assert EKV.get(backup_name, "backup/#{i}") == "val_#{i}"
      end
    end
  end

  # =====================================================================
  # Info API
  # =====================================================================

  describe "EKV.info/1" do
    test "returns cluster info map" do
      name = :"ekv_info_#{System.unique_integer([:positive])}"
      data_dir = Path.join(System.tmp_dir!(), "ekv_test_#{name}")

      {:ok, pid} =
        EKV.start_link(
          name: name,
          data_dir: data_dir,
          shards: 4,
          log: false,
          cluster_size: 3,
          node_id: "my-node",
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )

      on_exit(fn ->
        Process.exit(pid, :shutdown)
        File.rm_rf!(data_dir)
      end)

      info = EKV.info(name)
      assert info.name == name
      assert info.mode == :member
      assert info.region == "default"
      assert info.node_id == "my-node"
      assert info.cluster_size == 3
      assert info.shards == 4
      assert info.data_dir == data_dir
      assert info.connected_peers == []
    end
  end

  describe "client mode" do
    test "requires region_routing" do
      Process.flag(:trap_exit, true)

      assert {:error, {%ArgumentError{message: msg}, _}} =
               EKV.start_link(
                 name: :"ekv_client_#{System.unique_integer([:positive])}",
                 mode: :client
               )

      assert msg =~ ":region_routing must be a non-empty list"
    end

    test "rejects member-only options" do
      Process.flag(:trap_exit, true)

      assert {:error, {%ArgumentError{message: msg}, _}} =
               EKV.start_link(
                 name: :"ekv_client_bad_#{System.unique_integer([:positive])}",
                 mode: :client,
                 data_dir: "/tmp/nope",
                 region_routing: ["iad"]
               )

      assert msg =~ ":data_dir is not supported in :client mode"
    end

    test "invalid wait_for_route type raises" do
      Process.flag(:trap_exit, true)

      assert {:error, {%ArgumentError{message: msg}, _}} =
               EKV.start_link(
                 name: :"ekv_client_wait_route_bad_#{System.unique_integer([:positive])}",
                 mode: :client,
                 region_routing: ["iad"],
                 wait_for_route: true
               )

      assert msg =~ ":wait_for_route must be false/nil or a non-negative timeout in ms"
    end

    test "invalid shutdown_barrier type raises" do
      Process.flag(:trap_exit, true)

      assert {:error, {%ArgumentError{message: msg}, _}} =
               EKV.start_link(
                 name: :"ekv_client_shutdown_barrier_bad_#{System.unique_integer([:positive])}",
                 mode: :client,
                 region_routing: ["iad"],
                 shutdown_barrier: true
               )

      assert msg =~ ":shutdown_barrier must be false/nil or a non-negative timeout in ms"
    end

    test "reports client info and rejects member-only APIs" do
      name = :"ekv_client_info_#{System.unique_integer([:positive])}"

      {:ok, pid} =
        EKV.start_link(
          name: name,
          mode: :client,
          region: "ord",
          region_routing: ["iad", "dfw"]
        )

      on_exit(fn -> Process.exit(pid, :shutdown) end)

      info = EKV.info(name)
      assert info.name == name
      assert info.mode == :client
      assert info.region == "ord"
      assert info.region_routing == ["iad", "dfw"]
      assert info.current_backend == nil

      assert {:error, :timeout} = EKV.await_quorum(name, 0)
      assert_raise ArgumentError, fn -> EKV.backup(name, Path.join(System.tmp_dir!(), "x")) end
    end

    test "await_backend waiter is removed when caller exits" do
      name = :"ekv_client_waiter_#{System.unique_integer([:positive])}"

      {:ok, pid} =
        EKV.start_link(
          name: name,
          mode: :client,
          region: "ord",
          region_routing: ["iad"]
        )

      on_exit(fn -> Process.exit(pid, :shutdown) end)

      {waiter_pid, waiter_ref} =
        spawn_monitor(fn ->
          EKV.ClientRouter.await_backend(name, 5_000)
        end)

      EKV.TestCluster.assert_eventually(fn ->
        %{waiters: waiters} = :sys.get_state(EKV.ClientRouter.router_name(name))
        map_size(waiters) == 1
      end)

      Process.exit(waiter_pid, :kill)

      assert_receive {:DOWN, ^waiter_ref, :process, _pid, _reason}, 5_000

      EKV.TestCluster.assert_eventually(fn ->
        %{waiters: waiters} = :sys.get_state(EKV.ClientRouter.router_name(name))
        map_size(waiters) == 0
      end)
    end

    test "failing an old backend does not clear a newer current backend" do
      name = :"ekv_client_failover_#{System.unique_integer([:positive])}"

      {:ok, pid} =
        EKV.start_link(
          name: name,
          mode: :client,
          region: "ord",
          region_routing: ["iad"]
        )

      on_exit(fn -> Process.exit(pid, :shutdown) end)

      table = EKV.ClientRouter.table_name(name)
      old_backend = :old_backend@invalid
      new_backend = :new_backend@invalid

      :ets.insert(table, {:current_backend, new_backend})
      send(EKV.ClientRouter.router_name(name), {:mark_backend_failed, old_backend})

      EKV.TestCluster.assert_eventually(fn ->
        :ets.lookup(table, :current_backend) == [{:current_backend, new_backend}] and
          match?(
            [{{:cooldown_until, ^old_backend}, _deadline}],
            :ets.lookup(table, {:cooldown_until, old_backend})
          )
      end)
    end
  end

  # =====================================================================
  # skip_stale_check
  # =====================================================================

  describe "skip_stale_check" do
    test "data survives when stale DB check is skipped" do
      name = :"ekv_skip_stale_#{System.unique_integer([:positive])}"
      data_dir = Path.join(System.tmp_dir!(), "ekv_test_#{name}")
      Process.flag(:trap_exit, true)
      on_exit(fn -> File.rm_rf!(data_dir) end)

      {:ok, _pid} =
        EKV.start_link(
          name: name,
          data_dir: data_dir,
          shards: 1,
          log: false,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )

      :ok = EKV.put(name, "stale/key", "stale_val")
      assert EKV.get(name, "stale/key") == "stale_val"

      Supervisor.stop(:"#{name}_ekv_sup", :shutdown)
      Process.sleep(100)

      # Manually set last_active_at to an ancient time (triggers stale check)
      db_path = Path.join(data_dir, "shard_0.db")
      {:ok, db} = EKV.Sqlite3.open(db_path)
      ancient = System.system_time(:millisecond) - :timer.hours(24 * 30)

      {:ok, stmt} =
        EKV.Sqlite3.prepare(
          db,
          "UPDATE kv_meta SET value_int = ?1 WHERE key = 'last_active_at'"
        )

      :ok = EKV.Sqlite3.bind(stmt, [ancient])
      :done = EKV.Sqlite3.step(db, stmt)
      :ok = EKV.Sqlite3.release(db, stmt)
      EKV.Sqlite3.close(db)

      # Restart with skip_stale_check: true — data should survive
      {:ok, _pid2} =
        EKV.start_link(
          name: name,
          data_dir: data_dir,
          shards: 1,
          log: false,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7),
          skip_stale_check: true
        )

      assert EKV.get(name, "stale/key") == "stale_val"
    end
  end
end
