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
    test "scan returns matching entries as a map", %{name: name} do
      :ok = EKV.put(name, "user/1", %{name: "Alice"})
      :ok = EKV.put(name, "user/2", %{name: "Bob"})
      :ok = EKV.put(name, "post/1", %{title: "Hello"})

      result = EKV.scan(name, "user/")
      assert result == %{"user/1" => %{name: "Alice"}, "user/2" => %{name: "Bob"}}
    end

    test "keys returns matching keys only", %{name: name} do
      :ok = EKV.put(name, "user/1", "a")
      :ok = EKV.put(name, "user/2", "b")
      :ok = EKV.put(name, "post/1", "c")

      result = EKV.keys(name, "user/")
      assert result == ["user/1", "user/2"]
    end

    test "scan excludes deleted entries", %{name: name} do
      :ok = EKV.put(name, "user/1", "a")
      :ok = EKV.put(name, "user/2", "b")
      :ok = EKV.delete(name, "user/1")

      result = EKV.scan(name, "user/")
      assert result == %{"user/2" => "b"}
    end

    test "scan excludes expired entries", %{name: name} do
      :ok = EKV.put(name, "user/1", "a", ttl: 1)
      :ok = EKV.put(name, "user/2", "b")
      Process.sleep(10)

      result = EKV.scan(name, "user/")
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
        INSERT INTO kv_meta (key, value) VALUES ('last_active_at', ?1)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
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
end
