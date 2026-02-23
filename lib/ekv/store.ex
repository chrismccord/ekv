defmodule EKV.Store do
  @moduledoc false

  _archdoc = ~S"""
  Pure function module for SQLite operations. Called inside Replica GenServer.

  Each shard has its own SQLite database file at `#{data_dir}/shard_#{i}.db`.
  Uses WAL mode for concurrent reads.

  ## Tables

  - `kv` — current state of all keys: key -> (value, timestamp, origin_node, expires_at, deleted_at)
  - `kv_oplog` — append-only log of all mutations, used for delta sync
  - `kv_peer_hwm` — per-peer high-water marks for oplog sync
  """

  @get_sql """
  SELECT value, timestamp, origin_node, expires_at, deleted_at
  FROM kv WHERE key = ?1
  """

  # SQL for the 2 hot cached statements
  @kv_upsert_sql """
  INSERT INTO kv (key, value, timestamp, origin_node, expires_at, deleted_at)
  VALUES (?1, ?2, ?3, ?4, ?5, ?6)
  ON CONFLICT(key) DO UPDATE SET
    value = excluded.value, timestamp = excluded.timestamp,
    origin_node = excluded.origin_node, expires_at = excluded.expires_at,
    deleted_at = excluded.deleted_at
  WHERE excluded.timestamp > kv.timestamp
    OR (excluded.timestamp = kv.timestamp AND excluded.origin_node > kv.origin_node)
  """

  @oplog_insert_sql """
  INSERT INTO kv_oplog (key, value, timestamp, origin_node, expires_at, is_delete)
  VALUES (?1, ?2, ?3, ?4, ?5, ?6)
  """

  def open(data_dir, shard_index, tombstone_ttl, num_shards) do
    File.mkdir_p!(data_dir)
    path = Path.join(data_dir, "shard_#{shard_index}.db")

    # Check for stale db before opening. If the db exists and its last_active_at
    # is older than tombstone_ttl, peers will have GC'd tombstones for entries
    # deleted while we were away. Wipe and let full sync rebuild from scratch.
    if stale_db?(path, tombstone_ttl) do
      File.rm(path)
      File.rm(path <> "-wal")
      File.rm(path <> "-shm")
    end

    {:ok, db} = EKV.Sqlite3.open(path)

    # PRAGMAs
    :ok = EKV.Sqlite3.execute(db, "PRAGMA journal_mode=WAL")
    :ok = EKV.Sqlite3.execute(db, "PRAGMA synchronous=NORMAL")
    :ok = EKV.Sqlite3.execute(db, "PRAGMA busy_timeout=5000")
    :ok = EKV.Sqlite3.execute(db, "PRAGMA mmap_size=268435456")

    # Schema
    :ok =
      EKV.Sqlite3.execute(db, """
      CREATE TABLE IF NOT EXISTS kv (
        key TEXT NOT NULL PRIMARY KEY,
        value BLOB,
        timestamp INTEGER NOT NULL,
        origin_node TEXT NOT NULL,
        expires_at INTEGER,
        deleted_at INTEGER
      )
      """)

    :ok =
      EKV.Sqlite3.execute(db, """
      CREATE TABLE IF NOT EXISTS kv_oplog (
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        key TEXT NOT NULL,
        value BLOB,
        timestamp INTEGER NOT NULL,
        origin_node TEXT NOT NULL,
        expires_at INTEGER,
        is_delete INTEGER NOT NULL DEFAULT 0
      )
      """)

    :ok =
      EKV.Sqlite3.execute(db, """
      CREATE TABLE IF NOT EXISTS kv_peer_hwm (
        peer_node TEXT NOT NULL PRIMARY KEY,
        last_seq INTEGER NOT NULL
      )
      """)

    :ok =
      EKV.Sqlite3.execute(db, """
      CREATE TABLE IF NOT EXISTS kv_meta (
        key TEXT NOT NULL PRIMARY KEY,
        value INTEGER NOT NULL
      )
      """)

    :ok =
      EKV.Sqlite3.execute(
        db,
        "CREATE INDEX IF NOT EXISTS idx_kv_deleted ON kv(deleted_at) WHERE deleted_at IS NOT NULL"
      )

    :ok =
      EKV.Sqlite3.execute(
        db,
        "CREATE INDEX IF NOT EXISTS idx_kv_expires ON kv(expires_at) WHERE expires_at IS NOT NULL"
      )

    # Validate shard count — must never change after first open
    validate_num_shards(db, num_shards, data_dir, shard_index)

    # Mark as active
    touch_last_active(db)

    {:ok, db}
  end

  def open_reader(path) do
    {:ok, db} = EKV.Sqlite3.open(path)
    :ok = EKV.Sqlite3.execute(db, "PRAGMA journal_mode=WAL")
    :ok = EKV.Sqlite3.execute(db, "PRAGMA synchronous=NORMAL")
    :ok = EKV.Sqlite3.execute(db, "PRAGMA busy_timeout=5000")
    :ok = EKV.Sqlite3.execute(db, "PRAGMA mmap_size=268435456")
    {:ok, db}
  end

  def close(db) do
    EKV.Sqlite3.close(db)
  end

  # =====================================================================
  # Cached Statements
  # =====================================================================

  @doc """
  Prepare the 2 hot statements on the writer connection
  """
  def prepare_cached_stmts(db) do
    {:ok, kv_stmt} = EKV.Sqlite3.prepare(db, @kv_upsert_sql)
    {:ok, oplog_stmt} = EKV.Sqlite3.prepare(db, @oplog_insert_sql)
    %{kv_upsert: kv_stmt, oplog_insert: oplog_stmt}
  end

  @doc """
  Prepare a cached read statement on a reader connection
  """
  def prepare_read_stmt(db) do
    {:ok, stmt} = EKV.Sqlite3.prepare(db, @get_sql)
    stmt
  end

  @doc """
  Release cached statements before closing a connection
  """
  def release_stmts(db, stmts) when is_map(stmts) do
    for {_k, stmt} <- stmts, do: EKV.Sqlite3.release(db, stmt)
    :ok
  end

  @doc """
  Combined write: LWW check + kv upsert + oplog insert in a single NIF call.

  Returns {:ok, true} if the write was applied (LWW won or new key),
  or {:ok, false} if the write was skipped (LWW lost).
  """
  def write_entry(
        db,
        kv_stmt,
        oplog_stmt,
        key,
        value_binary,
        timestamp,
        origin_node,
        expires_at,
        deleted_at \\ nil
      ) do
    is_delete = if deleted_at, do: 1, else: 0
    origin_str = Atom.to_string(origin_node)

    kv_args = [key, value_binary, timestamp, origin_str, expires_at, deleted_at]
    oplog_args = [key, value_binary, timestamp, origin_str, expires_at, is_delete]

    EKV.Sqlite3.write_entry(db, kv_stmt, oplog_stmt, kv_args, oplog_args)
  end

  # =====================================================================
  # KV CRUD
  # =====================================================================

  def get(db, key) do
    {:ok, stmt} =
      EKV.Sqlite3.prepare(db, """
      SELECT value, timestamp, origin_node, expires_at, deleted_at
      FROM kv WHERE key = ?1
      """)

    :ok = EKV.Sqlite3.bind(stmt, [key])

    result =
      case EKV.Sqlite3.step(db, stmt) do
        {:row, [value, timestamp, origin_node, expires_at, deleted_at]} ->
          {value, timestamp, String.to_atom(origin_node), expires_at, deleted_at}

        :done ->
          nil
      end

    :ok = EKV.Sqlite3.release(db, stmt)
    result
  end

  @doc """
  Single-bounce read using a cached prepared statement
  """
  def get_cached(db, get_stmt, key) do
    case EKV.Sqlite3.read_entry(db, get_stmt, [key]) do
      {:ok, nil} ->
        nil

      {:ok, [value, timestamp, origin_node, expires_at, deleted_at]} ->
        {value, timestamp, String.to_atom(origin_node), expires_at, deleted_at}
    end
  end

  @scan_prefix_sql """
  SELECT key, value FROM kv
  WHERE key >= ?1 AND key < ?2
    AND (deleted_at IS NULL OR deleted_at > ?3)
    AND (expires_at IS NULL OR expires_at > ?3)
  ORDER BY key
  """

  @scan_prefix_keys_sql """
  SELECT key FROM kv
  WHERE key >= ?1 AND key < ?2
    AND (deleted_at IS NULL OR deleted_at > ?3)
    AND (expires_at IS NULL OR expires_at > ?3)
  ORDER BY key
  """

  @doc """
  Scan keys matching prefix that are live (not deleted, not expired)
  """
  def scan_prefix(db, prefix, now) do
    prefix_end = next_binary_prefix(prefix)
    {:ok, rows} = EKV.Sqlite3.fetch_all(db, @scan_prefix_sql, [prefix, prefix_end, now])
    Enum.map(rows, fn [key, value] -> {key, value} end)
  end

  @doc """
  Scan keys only matching prefix that are live
  """
  def scan_prefix_keys(db, prefix, now) do
    prefix_end = next_binary_prefix(prefix)
    {:ok, rows} = EKV.Sqlite3.fetch_all(db, @scan_prefix_keys_sql, [prefix, prefix_end, now])
    Enum.map(rows, fn [key] -> key end)
  end

  @doc """
  Count live entries
  """
  def count_live(db, now) do
    {:ok, stmt} =
      EKV.Sqlite3.prepare(db, """
      SELECT COUNT(*) FROM kv
      WHERE (deleted_at IS NULL OR deleted_at > ?1)
        AND (expires_at IS NULL OR expires_at > ?1)
      """)

    :ok = EKV.Sqlite3.bind(stmt, [now])
    {:row, [count]} = EKV.Sqlite3.step(db, stmt)
    :ok = EKV.Sqlite3.release(db, stmt)
    count
  end

  def next_binary_prefix(prefix) do
    size = byte_size(prefix) - 1
    <<head::binary-size(size), last_byte>> = prefix
    <<head::binary, last_byte + 1>>
  end

  # =====================================================================
  # oplog
  # =====================================================================

  @oplog_since_sql """
  SELECT seq, key, value, timestamp, origin_node, expires_at, is_delete
  FROM kv_oplog WHERE seq > ?1 ORDER BY seq
  """

  def oplog_since(db, seq) do
    {:ok, rows} = EKV.Sqlite3.fetch_all(db, @oplog_since_sql, [seq])

    Enum.map(rows, fn [seq, key, value, timestamp, origin_node, expires_at, is_delete] ->
      {seq, key, value, timestamp, String.to_atom(origin_node), expires_at, is_delete == 1}
    end)
  end

  def max_seq(db) do
    {:ok, stmt} = EKV.Sqlite3.prepare(db, "SELECT MAX(seq) FROM kv_oplog")

    result =
      case EKV.Sqlite3.step(db, stmt) do
        {:row, [nil]} -> 0
        {:row, [seq]} -> seq
        :done -> 0
      end

    :ok = EKV.Sqlite3.release(db, stmt)
    result
  end

  def min_seq(db) do
    {:ok, stmt} = EKV.Sqlite3.prepare(db, "SELECT MIN(seq) FROM kv_oplog")

    result =
      case EKV.Sqlite3.step(db, stmt) do
        {:row, [nil]} -> 0
        {:row, [seq]} -> seq
        :done -> 0
      end

    :ok = EKV.Sqlite3.release(db, stmt)
    result
  end

  # =====================================================================
  # Peer HWM
  # =====================================================================

  def get_hwm(db, peer_node) do
    {:ok, stmt} =
      EKV.Sqlite3.prepare(db, "SELECT last_seq FROM kv_peer_hwm WHERE peer_node = ?1")

    :ok = EKV.Sqlite3.bind(stmt, [Atom.to_string(peer_node)])

    result =
      case EKV.Sqlite3.step(db, stmt) do
        {:row, [seq]} -> seq
        :done -> nil
      end

    :ok = EKV.Sqlite3.release(db, stmt)
    result
  end

  def set_hwm(db, peer_node, seq) do
    {:ok, stmt} =
      EKV.Sqlite3.prepare(db, """
      INSERT INTO kv_peer_hwm (peer_node, last_seq) VALUES (?1, ?2)
      ON CONFLICT(peer_node) DO UPDATE SET last_seq = MAX(kv_peer_hwm.last_seq, excluded.last_seq)
      """)

    :ok = EKV.Sqlite3.bind(stmt, [Atom.to_string(peer_node), seq])
    :done = EKV.Sqlite3.step(db, stmt)
    :ok = EKV.Sqlite3.release(db, stmt)
    :ok
  end

  # =====================================================================
  # GC ops
  # =====================================================================

  @find_expired_sql """
  SELECT key, value, timestamp, origin_node, expires_at
  FROM kv WHERE expires_at IS NOT NULL AND expires_at < ?1 AND deleted_at IS NULL
  """

  @doc """
  Find entries with expired TTL that haven't been tombstoned yet
  """
  def find_expired(db, now) do
    {:ok, rows} = EKV.Sqlite3.fetch_all(db, @find_expired_sql, [now])

    Enum.map(rows, fn [key, value, timestamp, origin_node, expires_at] ->
      {key, value, timestamp, String.to_atom(origin_node), expires_at}
    end)
  end

  @doc """
  Hard-delete tombstones older than cutoff
  """
  def purge_tombstones(db, cutoff) do
    {:ok, stmt} =
      EKV.Sqlite3.prepare(db, """
      DELETE FROM kv WHERE deleted_at IS NOT NULL AND deleted_at < ?1
      """)

    :ok = EKV.Sqlite3.bind(stmt, [cutoff])
    :done = EKV.Sqlite3.step(db, stmt)
    :ok = EKV.Sqlite3.release(db, stmt)
    :ok
  end

  @doc """
  Remove HWM entries for peers not currently connected.
  Prevents dead peers from anchoring the oplog forever.
  """
  def prune_peer_hwms(db, connected_peers) do
    connected_set = MapSet.new(connected_peers, &Atom.to_string/1)
    {:ok, rows} = EKV.Sqlite3.fetch_all(db, "SELECT peer_node FROM kv_peer_hwm", [])

    for [peer_node] <- rows, not MapSet.member?(connected_set, peer_node) do
      {:ok, stmt} =
        EKV.Sqlite3.prepare(db, "DELETE FROM kv_peer_hwm WHERE peer_node = ?1")

      :ok = EKV.Sqlite3.bind(stmt, [peer_node])
      :done = EKV.Sqlite3.step(db, stmt)
      :ok = EKV.Sqlite3.release(db, stmt)
    end

    :ok
  end

  @doc """
  Truncate oplog entries below the min of all peer HWMs
  """
  def truncate_oplog(db) do
    {:ok, stmt} =
      EKV.Sqlite3.prepare(db, "SELECT MIN(last_seq) FROM kv_peer_hwm")

    min_hwm =
      case EKV.Sqlite3.step(db, stmt) do
        {:row, [nil]} -> nil
        {:row, [seq]} -> seq
        :done -> nil
      end

    :ok = EKV.Sqlite3.release(db, stmt)

    if min_hwm do
      {:ok, del_stmt} =
        EKV.Sqlite3.prepare(db, "DELETE FROM kv_oplog WHERE seq < ?1")

      :ok = EKV.Sqlite3.bind(del_stmt, [min_hwm])
      :done = EKV.Sqlite3.step(db, del_stmt)
      :ok = EKV.Sqlite3.release(db, del_stmt)
    end

    :ok
  end

  # =====================================================================
  # Liveliness
  # =====================================================================

  @full_state_sql """
  SELECT key, value, timestamp, origin_node, expires_at, deleted_at
  FROM kv WHERE deleted_at IS NULL OR deleted_at > ?1
  """

  @doc """
  Get all live entries from SQLite for full sync (excludes old tombstones)
  """
  def full_state(db, tombstone_cutoff) do
    {:ok, rows} = EKV.Sqlite3.fetch_all(db, @full_state_sql, [tombstone_cutoff])

    Enum.map(rows, fn [key, value, timestamp, origin_node, expires_at, deleted_at] ->
      {key, value, timestamp, String.to_atom(origin_node), expires_at, deleted_at}
    end)
  end

  def touch_last_active(db) do
    now = System.system_time(:nanosecond)

    {:ok, stmt} =
      EKV.Sqlite3.prepare(db, """
      INSERT INTO kv_meta (key, value) VALUES ('last_active_at', ?1)
      ON CONFLICT(key) DO UPDATE SET value = excluded.value
      """)

    :ok = EKV.Sqlite3.bind(stmt, [now])
    :done = EKV.Sqlite3.step(db, stmt)
    :ok = EKV.Sqlite3.release(db, stmt)
    :ok
  end

  defp stale_db?(path, tombstone_ttl) do
    if File.exists?(path) do
      case read_last_active(path) do
        nil ->
          # No meta table or no row — legacy db, treat as stale
          true

        last_active_at ->
          now = System.system_time(:nanosecond)
          age_ms = div(now - last_active_at, 1_000_000)
          age_ms > tombstone_ttl
      end
    else
      false
    end
  end

  defp read_last_active(path) do
    case EKV.Sqlite3.open(path) do
      {:ok, db} ->
        result =
          try do
            case EKV.Sqlite3.prepare(db, "SELECT value FROM kv_meta WHERE key = 'last_active_at'") do
              {:ok, stmt} ->
                val =
                  case EKV.Sqlite3.step(db, stmt) do
                    {:row, [ts]} -> ts
                    :done -> nil
                  end

                EKV.Sqlite3.release(db, stmt)
                val

              {:error, _} ->
                # Table doesn't exist
                nil
            end
          after
            EKV.Sqlite3.close(db)
          end

        result

      {:error, _} ->
        nil
    end
  end

  # =====================================================================
  # Shard count validation
  # =====================================================================

  defp validate_num_shards(db, num_shards, data_dir, shard_index) do
    {:ok, stmt} =
      EKV.Sqlite3.prepare(db, "SELECT value FROM kv_meta WHERE key = 'num_shards'")

    stored =
      case EKV.Sqlite3.step(db, stmt) do
        {:row, [n]} -> n
        :done -> nil
      end

    :ok = EKV.Sqlite3.release(db, stmt)

    case stored do
      nil ->
        # First open — persist the shard count
        {:ok, ins} =
          EKV.Sqlite3.prepare(db, """
          INSERT INTO kv_meta (key, value) VALUES ('num_shards', ?1)
          """)

        :ok = EKV.Sqlite3.bind(ins, [num_shards])
        :done = EKV.Sqlite3.step(db, ins)
        :ok = EKV.Sqlite3.release(db, ins)

      ^num_shards ->
        :ok

      other ->
        raise ArgumentError,
              "EKV shard count mismatch for #{data_dir}/shard_#{shard_index}.db: " <>
                "database was created with shards=#{other}, but started with shards=#{num_shards}. " <>
                "Changing shard count is not supported — data is physically partitioned by phash2(key, num_shards)."
    end
  end
end
