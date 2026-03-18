defmodule EKV.Store do
  @moduledoc false

  _archdoc = ~S"""
  Pure function module for SQLite operations. Called inside Replica GenServer.

  Each shard has its own SQLite database file at `#{data_dir}/shard_#{i}.db`.
  Uses WAL mode for concurrent reads. Store is responsible for:

  - current committed KV state
  - CAS accept/promote state (`kv_paxos`)
  - anti-entropy replay/progress (`kv_oplog`, `kv_origin_progress`, `kv_member_progress`)
  - startup schema compatibility via `kv_meta.schema_version`
  - startup stale-db checks (`allow_stale_startup` override)
  - local TTL-expiry bookkeeping via `expired_at`

  Simple mental model:

  - `kv` is the current dataset
  - `kv_oplog` is recent per-origin history for delta repair
  - `kv_origin_progress` is this shard's contiguous applied cursor per origin
  - `kv_member_progress` is each peer's cursor per origin; GC uses it to
    decide how much replay history must be kept

  ## Tables

  - `kv` — current committed state of all keys:
    `(value, timestamp, origin_node, origin_seq, expires_at, deleted_at, expired_at)`
    `expired_at` is local-only bookkeeping so GC emits `:expired` once; it is
    not a replicated tombstone marker.
  - `kv_oplog` — authoritative replay log keyed by `(origin_node, origin_seq)`.
    Already-expired rows are filtered out when delta sync is built.
  - `kv_origin_progress` — highest contiguous locally-applied replay progress
    per origin. Local-origin writes/promotes can advance this directly because
    the shard allocates self `origin_seq` in-order inside the same transaction.
  - `kv_member_progress` — per-member, per-origin progress for anti-entropy
    summaries, sync settlement, and replay retention/truncation. GC keeps
    recently disconnected members' rows for `member_progress_retention_ttl`
    so moderate partitions can still heal by delta.
  - `kv_meta` — shard metadata such as `schema_version`, `num_shards`,
    `last_active_at`, persisted `node_id`, member-node identity mappings, and
    long-partition down-since markers.
  - `kv_paxos` — durable CASPaxos acceptor state per key.

  Write/promote primitives still cross the Elixir/NIF boundary once. The
  extra replay bookkeeping now happens inside that same SQLite transaction:
  allocate the next local `origin_seq` when needed, append the replay row,
  and update local contiguous progress before commit.
  """

  @get_sql """
  SELECT value, timestamp, origin_node, expires_at, deleted_at
  FROM kv WHERE key = ?1
  """

  # SQL for the 2 hot cached statements
  @kv_upsert_sql """
  INSERT INTO kv (key, value, timestamp, origin_node, origin_seq, expires_at, deleted_at)
  VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
  ON CONFLICT(key) DO UPDATE SET
    value = excluded.value, timestamp = excluded.timestamp,
    origin_node = excluded.origin_node, origin_seq = excluded.origin_seq,
    expires_at = excluded.expires_at, deleted_at = excluded.deleted_at,
    expired_at = NULL
  WHERE excluded.timestamp > kv.timestamp
    OR (excluded.timestamp = kv.timestamp AND excluded.origin_node > kv.origin_node)
  """

  # Unconditional upsert — no LWW WHERE clause. Used by paxos_accept where
  # Paxos ballots determine ordering, not timestamps.
  @kv_force_upsert_sql """
  INSERT INTO kv (key, value, timestamp, origin_node, origin_seq, expires_at, deleted_at)
  VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
  ON CONFLICT(key) DO UPDATE SET
    value = excluded.value, timestamp = excluded.timestamp,
    origin_node = excluded.origin_node, origin_seq = excluded.origin_seq,
    expires_at = excluded.expires_at, deleted_at = excluded.deleted_at,
    expired_at = NULL
  """

  @oplog_insert_sql """
  INSERT INTO kv_oplog (key, value, timestamp, origin_node, origin_seq, expires_at, is_delete)
  VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
  ON CONFLICT(origin_node, origin_seq) DO NOTHING
  """

  @schema_version 1

  def open(data_dir, shard_index, tombstone_ttl, num_shards, gc_interval, opts \\ []) do
    allow_stale_startup = Keyword.get(opts, :allow_stale_startup, false)
    File.mkdir_p!(data_dir)
    path = Path.join(data_dir, "shard_#{shard_index}.db")

    # Check for stale db before opening. If the db exists and its last_active_at
    # is older than the safe threshold, other members will have GC'd tombstones for entries
    # deleted while we were away. Fail startup by default so operators can
    # explicitly choose whether to wipe/rebuild from members or trust the old
    # on-disk data.
    #
    # Safety margin: last_active_at can lag real shutdown by up to gc_interval
    # (worst case: node crashes right before a GC tick). We subtract gc_interval
    # from tombstone_ttl so that a node that was truly unreachable for tombstone_ttl
    # is always detected as stale, even in the worst case.
    stale_threshold = tombstone_ttl - gc_interval

    with :ok <- maybe_reject_stale_db(path, stale_threshold, allow_stale_startup) do
      do_open(path, num_shards, data_dir, shard_index)
    end
  end

  defp do_open(path, num_shards, data_dir, shard_index) do
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
        origin_seq INTEGER NOT NULL DEFAULT 0,
        expires_at INTEGER,
        deleted_at INTEGER,
        expired_at INTEGER
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
        origin_seq INTEGER NOT NULL DEFAULT 0,
        expires_at INTEGER,
        is_delete INTEGER NOT NULL DEFAULT 0
      )
      """)

    :ok =
      EKV.Sqlite3.execute(db, """
      CREATE UNIQUE INDEX IF NOT EXISTS idx_kv_oplog_origin_seq
      ON kv_oplog(origin_node, origin_seq)
      """)

    :ok =
      EKV.Sqlite3.execute(db, """
      CREATE TABLE IF NOT EXISTS kv_origin_progress (
        origin_node TEXT NOT NULL PRIMARY KEY,
        last_seq INTEGER NOT NULL
      )
      """)

    :ok =
      EKV.Sqlite3.execute(db, """
      CREATE TABLE IF NOT EXISTS kv_member_progress (
        member_node TEXT NOT NULL,
        origin_node TEXT NOT NULL,
        last_seq INTEGER NOT NULL,
        PRIMARY KEY (member_node, origin_node)
      )
      """)

    :ok =
      EKV.Sqlite3.execute(db, """
      CREATE TABLE IF NOT EXISTS kv_member_hwm (
        member_node TEXT NOT NULL PRIMARY KEY,
        last_seq INTEGER NOT NULL
      )
      """)

    :ok =
      EKV.Sqlite3.execute(db, """
      CREATE TABLE IF NOT EXISTS kv_meta (
        key TEXT NOT NULL PRIMARY KEY,
        value_int INTEGER,
        value_text TEXT
      )
      """)

    # Migration for existing databases with old kv_meta schema (single `value` column)
    case EKV.Sqlite3.execute(db, "ALTER TABLE kv_meta ADD COLUMN value_int INTEGER") do
      :ok -> :ok
      {:error, _} -> :ok
    end

    case EKV.Sqlite3.execute(db, "ALTER TABLE kv_meta ADD COLUMN value_text TEXT") do
      :ok -> :ok
      {:error, _} -> :ok
    end

    :ok =
      EKV.Sqlite3.execute(db, """
      CREATE TABLE IF NOT EXISTS kv_paxos (
        key TEXT NOT NULL PRIMARY KEY,
        promised_counter INTEGER NOT NULL DEFAULT 0,
        promised_node TEXT NOT NULL DEFAULT '',
        accepted_counter INTEGER NOT NULL DEFAULT 0,
        accepted_node TEXT NOT NULL DEFAULT '',
        accepted_value BLOB,
        accepted_timestamp INTEGER,
        accepted_origin TEXT,
        accepted_expires_at INTEGER,
        accepted_deleted_at INTEGER
      )
      """)

    # Migration for existing dev databases — add value columns if missing
    for {col, type} <- [
          {"accepted_value", "BLOB"},
          {"accepted_timestamp", "INTEGER"},
          {"accepted_origin", "TEXT"},
          {"accepted_expires_at", "INTEGER"},
          {"accepted_deleted_at", "INTEGER"}
        ] do
      case EKV.Sqlite3.execute(db, "ALTER TABLE kv_paxos ADD COLUMN #{col} #{type}") do
        :ok -> :ok
        {:error, _} -> :ok
      end
    end

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

    case EKV.Sqlite3.execute(db, "ALTER TABLE kv ADD COLUMN expired_at INTEGER") do
      :ok -> :ok
      {:error, _} -> :ok
    end

    case EKV.Sqlite3.execute(
           db,
           "ALTER TABLE kv ADD COLUMN origin_seq INTEGER NOT NULL DEFAULT 0"
         ) do
      :ok -> :ok
      {:error, _} -> :ok
    end

    case EKV.Sqlite3.execute(
           db,
           "ALTER TABLE kv_oplog ADD COLUMN origin_seq INTEGER NOT NULL DEFAULT 0"
         ) do
      :ok -> :ok
      {:error, _} -> :ok
    end

    :ok =
      EKV.Sqlite3.execute(
        db,
        "CREATE INDEX IF NOT EXISTS idx_kv_expired_marker ON kv(expired_at) WHERE expired_at IS NOT NULL"
      )

    # Validate/startup guards — must never silently open incompatible data.
    validate_schema_version(db, data_dir, shard_index)
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

  def backup_shard(source_dir, dest_dir, shard_index) do
    source = Path.join(source_dir, "shard_#{shard_index}.db")
    dest = Path.join(dest_dir, "shard_#{shard_index}.db")
    EKV.Sqlite3.backup(source, dest)
  end

  # =====================================================================
  # Cached Statements
  # =====================================================================

  @doc """
  Prepare the 2 hot statements on the writer connection
  """
  def prepare_cached_stmts(db) do
    {:ok, kv_stmt} = EKV.Sqlite3.prepare(db, @kv_upsert_sql)
    {:ok, kv_force_stmt} = EKV.Sqlite3.prepare(db, @kv_force_upsert_sql)
    {:ok, oplog_stmt} = EKV.Sqlite3.prepare(db, @oplog_insert_sql)
    %{kv_upsert: kv_stmt, kv_force_upsert: kv_force_stmt, oplog_insert: oplog_stmt}
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

  Returns:
    - `{:ok, true, origin_seq, local_progress_seq}` when the write was applied
    - `{:ok, false, origin_seq, local_progress_seq}` when the kv row was not updated but
      the replay row/progress state was still processed
    - `{:ok, false}` only for local-origin LWW loss where no replay row was retained
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
        deleted_at \\ nil,
        origin_seq \\ nil
      ) do
    is_delete = if deleted_at, do: 1, else: 0
    origin_str = Atom.to_string(origin_node)
    local_origin = origin_node == node()

    kv_args = [key, value_binary, timestamp, origin_str, origin_seq, expires_at, deleted_at]
    oplog_args = [key, value_binary, timestamp, origin_str, origin_seq, expires_at, is_delete]

    EKV.Sqlite3.write_entry(db, kv_stmt, oplog_stmt, kv_args, oplog_args, local_origin)
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

  def max_timestamp(db) do
    {:ok, stmt} = EKV.Sqlite3.prepare(db, "SELECT MAX(timestamp) FROM kv")

    result =
      case EKV.Sqlite3.step(db, stmt) do
        {:row, [nil]} -> nil
        {:row, [ts]} -> ts
        :done -> nil
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
  SELECT seq, key, value, timestamp, origin_node, origin_seq, expires_at, is_delete
  FROM kv_oplog WHERE seq > ?1 ORDER BY seq
  """

  def oplog_since(db, seq) do
    {:ok, rows} = EKV.Sqlite3.fetch_all(db, @oplog_since_sql, [seq])

    Enum.map(rows, fn [seq, key, value, timestamp, origin_node, origin_seq, expires_at, is_delete] ->
      {seq, key, value, timestamp, String.to_atom(origin_node), origin_seq, expires_at,
       is_delete == 1}
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

  def max_origin_seq(db, origin_node) do
    {:ok, stmt} =
      EKV.Sqlite3.prepare(
        db,
        "SELECT MAX(origin_seq) FROM kv_oplog WHERE origin_node = ?1"
      )

    :ok = EKV.Sqlite3.bind(stmt, [Atom.to_string(origin_node)])

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
  # Anti-Entropy Progress
  # =====================================================================

  def local_progress_summary(db) do
    {:ok, rows} =
      EKV.Sqlite3.fetch_all(
        db,
        "SELECT origin_node, last_seq FROM kv_origin_progress ORDER BY origin_node",
        []
      )

    Map.new(rows, fn [origin_node, seq] -> {String.to_atom(origin_node), seq} end)
  end

  def merge_local_progress_summary(_db, progress_map) when progress_map == %{}, do: :ok

  def merge_local_progress_summary(db, progress_map) when is_map(progress_map) do
    EKV.Sqlite3.merge_local_progress_summary(db, encode_progress_entries(progress_map))
  end

  def replace_local_progress_summary(db, progress_map) when is_map(progress_map) do
    EKV.Sqlite3.replace_local_progress_summary(db, encode_progress_entries(progress_map))
  end

  def merge_local_progress(db, origin_node, seq) do
    {:ok, stmt} =
      EKV.Sqlite3.prepare(
        db,
        """
        INSERT INTO kv_origin_progress (origin_node, last_seq) VALUES (?1, ?2)
        ON CONFLICT(origin_node) DO UPDATE SET last_seq = MAX(last_seq, excluded.last_seq)
        """
      )

    :ok = EKV.Sqlite3.bind(stmt, [Atom.to_string(origin_node), seq])
    :done = EKV.Sqlite3.step(db, stmt)
    :ok = EKV.Sqlite3.release(db, stmt)
    :ok
  end

  def get_peer_progress(db, member_node) do
    {:ok, rows} =
      EKV.Sqlite3.fetch_all(
        db,
        "SELECT origin_node, last_seq FROM kv_member_progress WHERE member_node = ?1 ORDER BY origin_node",
        [Atom.to_string(member_node)]
      )

    Map.new(rows, fn [origin_node, seq] -> {String.to_atom(origin_node), seq} end)
  end

  def replace_peer_progress(db, member_node, progress_map) when is_map(progress_map) do
    EKV.Sqlite3.replace_peer_progress(
      db,
      Atom.to_string(member_node),
      encode_progress_entries(progress_map)
    )
  end

  def update_peer_progress(db, member_node, origin_node, seq) do
    {:ok, stmt} =
      EKV.Sqlite3.prepare(
        db,
        """
        INSERT INTO kv_member_progress (member_node, origin_node, last_seq)
        VALUES (?1, ?2, ?3)
        ON CONFLICT(member_node, origin_node) DO UPDATE SET last_seq = MAX(last_seq, excluded.last_seq)
        """
      )

    :ok = EKV.Sqlite3.bind(stmt, [Atom.to_string(member_node), Atom.to_string(origin_node), seq])
    :done = EKV.Sqlite3.step(db, stmt)
    :ok = EKV.Sqlite3.release(db, stmt)
    :ok
  end

  def prune_member_progress(db, connected_members) do
    connected_set = MapSet.new(connected_members, &Atom.to_string/1)

    {:ok, rows} =
      EKV.Sqlite3.fetch_all(db, "SELECT DISTINCT member_node FROM kv_member_progress", [])

    for [member_node] <- rows, not MapSet.member?(connected_set, member_node) do
      {:ok, stmt} =
        EKV.Sqlite3.prepare(db, "DELETE FROM kv_member_progress WHERE member_node = ?1")

      :ok = EKV.Sqlite3.bind(stmt, [member_node])
      :done = EKV.Sqlite3.step(db, stmt)
      :ok = EKV.Sqlite3.release(db, stmt)
    end

    :ok
  end

  def member_progress_members(db) do
    {:ok, rows} =
      EKV.Sqlite3.fetch_all(db, "SELECT DISTINCT member_node FROM kv_member_progress", [])

    Enum.map(rows, fn [member_node] -> String.to_atom(member_node) end)
  end

  def replay_origin_bounds(db) do
    {:ok, rows} =
      EKV.Sqlite3.fetch_all(
        db,
        """
        SELECT origin_node, MIN(origin_seq), MAX(origin_seq)
        FROM kv_oplog
        GROUP BY origin_node
        """,
        []
      )

    Map.new(rows, fn [origin_node, min_seq, max_seq] ->
      {String.to_atom(origin_node), {min_seq, max_seq}}
    end)
  end

  @replay_since_origin_chunk_sql """
  SELECT key, value, timestamp, origin_node, origin_seq, expires_at, is_delete
  FROM kv_oplog
  WHERE origin_node = ?1
    AND origin_seq > ?2
    AND (is_delete = 1 OR expires_at IS NULL OR expires_at > ?3)
  ORDER BY origin_seq LIMIT ?4
  """

  def replay_since_origin_chunk(db, origin_node, origin_seq, limit) do
    now = System.system_time(:nanosecond)

    {:ok, rows} =
      EKV.Sqlite3.fetch_all(db, @replay_since_origin_chunk_sql, [
        Atom.to_string(origin_node),
        origin_seq,
        now,
        limit
      ])

    Enum.map(rows, fn [key, value, timestamp, origin_node, replay_seq, expires_at, is_delete] ->
      {key, value, timestamp, String.to_atom(origin_node), replay_seq, expires_at, is_delete == 1}
    end)
  end

  defp encode_progress_entries(progress_map) do
    Enum.map(progress_map, fn {origin_node, seq} ->
      {Atom.to_string(origin_node), seq}
    end)
  end

  # =====================================================================
  # Legacy flat HWM helpers
  # =====================================================================

  def get_hwm(db, member_node) do
    {:ok, stmt} =
      EKV.Sqlite3.prepare(db, "SELECT last_seq FROM kv_member_hwm WHERE member_node = ?1")

    :ok = EKV.Sqlite3.bind(stmt, [Atom.to_string(member_node)])

    result =
      case EKV.Sqlite3.step(db, stmt) do
        {:row, [seq]} -> seq
        :done -> nil
      end

    :ok = EKV.Sqlite3.release(db, stmt)
    result
  end

  def set_hwm(db, member_node, seq) do
    {:ok, stmt} =
      EKV.Sqlite3.prepare(db, """
      INSERT INTO kv_member_hwm (member_node, last_seq) VALUES (?1, ?2)
      ON CONFLICT(member_node) DO UPDATE SET last_seq = excluded.last_seq
      """)

    :ok = EKV.Sqlite3.bind(stmt, [Atom.to_string(member_node), seq])
    :done = EKV.Sqlite3.step(db, stmt)
    :ok = EKV.Sqlite3.release(db, stmt)
    :ok
  end

  # =====================================================================
  # GC ops
  # =====================================================================

  @find_expired_sql """
  SELECT key, value, timestamp, origin_node, expires_at
  FROM kv
  WHERE expires_at IS NOT NULL
    AND expires_at < ?1
    AND deleted_at IS NULL
    AND expired_at IS NULL
  """

  @doc """
  Find entries with expired TTL that have not yet emitted a local expiry event.
  """
  def find_expired(db, now) do
    {:ok, rows} = EKV.Sqlite3.fetch_all(db, @find_expired_sql, [now])

    Enum.map(rows, fn [key, value, timestamp, origin_node, expires_at] ->
      {key, value, timestamp, String.to_atom(origin_node), expires_at}
    end)
  end

  @doc """
  Mark an expired row as locally observed so GC emits `:expired` once.
  """
  def mark_expired(db, key, now) do
    {:ok, stmt} =
      EKV.Sqlite3.prepare(db, """
      UPDATE kv
      SET expired_at = ?2
      WHERE key = ?1
        AND expires_at IS NOT NULL
        AND expires_at < ?2
        AND deleted_at IS NULL
        AND expired_at IS NULL
      """)

    :ok = EKV.Sqlite3.bind(stmt, [key, now])
    :done = EKV.Sqlite3.step(db, stmt)
    :ok = EKV.Sqlite3.release(db, stmt)
    {:ok, [[changes]]} = EKV.Sqlite3.fetch_all(db, "SELECT changes()", [])
    {:ok, changes > 0}
  end

  @doc """
  Hard-delete CAS-managed TTL-expired rows older than the retention cutoff.
  """
  def purge_expired(db, cutoff) do
    {:ok, stmt} =
      EKV.Sqlite3.prepare(db, """
      DELETE FROM kv
      WHERE expires_at IS NOT NULL
        AND expires_at < ?1
        AND deleted_at IS NULL
        AND key IN (SELECT key FROM kv_paxos)
      """)

    :ok = EKV.Sqlite3.bind(stmt, [cutoff])
    :done = EKV.Sqlite3.step(db, stmt)
    :ok = EKV.Sqlite3.release(db, stmt)
    :ok
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
  Remove HWM entries for members not currently connected.
  Prevents dead members from anchoring the oplog forever.
  """
  def prune_member_hwms(db, connected_members) do
    connected_set = MapSet.new(connected_members, &Atom.to_string/1)
    {:ok, rows} = EKV.Sqlite3.fetch_all(db, "SELECT member_node FROM kv_member_hwm", [])

    for [member_node] <- rows, not MapSet.member?(connected_set, member_node) do
      {:ok, stmt} =
        EKV.Sqlite3.prepare(db, "DELETE FROM kv_member_hwm WHERE member_node = ?1")

      :ok = EKV.Sqlite3.bind(stmt, [member_node])
      :done = EKV.Sqlite3.step(db, stmt)
      :ok = EKV.Sqlite3.release(db, stmt)
    end

    :ok
  end

  @doc """
  Truncate replay-log entries below the minimum retained peer progress per origin.
  """
  def truncate_oplog(db) do
    {:ok, rows} =
      EKV.Sqlite3.fetch_all(
        db,
        """
        SELECT origin_node, MIN(last_seq)
        FROM kv_member_progress
        GROUP BY origin_node
        """,
        []
      )

    Enum.each(rows, fn [origin_node, min_seq] ->
      {:ok, del_stmt} =
        EKV.Sqlite3.prepare(
          db,
          "DELETE FROM kv_oplog WHERE origin_node = ?1 AND origin_seq < ?2"
        )

      :ok = EKV.Sqlite3.bind(del_stmt, [origin_node, min_seq])
      :done = EKV.Sqlite3.step(db, del_stmt)
      :ok = EKV.Sqlite3.release(db, del_stmt)
    end)

    :ok
  end

  # =====================================================================
  # Liveliness
  # =====================================================================

  @full_state_sql """
  SELECT key, value, timestamp, origin_node, origin_seq, expires_at, deleted_at
  FROM kv
  WHERE (deleted_at IS NULL AND (expires_at IS NULL OR expires_at > ?2))
     OR deleted_at > ?1
  """

  @doc """
  Get all live entries from SQLite for full sync (excludes old tombstones)
  """
  def full_state(db, tombstone_cutoff) do
    now = System.system_time(:nanosecond)
    {:ok, rows} = EKV.Sqlite3.fetch_all(db, @full_state_sql, [tombstone_cutoff, now])

    Enum.map(rows, fn [key, value, timestamp, origin_node, origin_seq, expires_at, deleted_at] ->
      {key, value, timestamp, String.to_atom(origin_node), origin_seq, expires_at, deleted_at}
    end)
  end

  # =====================================================================
  # Chunked query functions (for cursor-based sync pagination)
  # =====================================================================

  @full_state_first_chunk_sql """
  SELECT key, value, timestamp, origin_node, origin_seq, expires_at, deleted_at
  FROM kv
  WHERE (deleted_at IS NULL AND (expires_at IS NULL OR expires_at > ?2))
     OR deleted_at > ?1
  ORDER BY key LIMIT ?3
  """

  @full_state_chunk_sql """
  SELECT key, value, timestamp, origin_node, origin_seq, expires_at, deleted_at
  FROM kv
  WHERE (((deleted_at IS NULL AND (expires_at IS NULL OR expires_at > ?3))
     OR deleted_at > ?1))
    AND key > ?2
  ORDER BY key LIMIT ?4
  """

  @doc """
  Get a chunk of live entries for full sync, ordered by key with cursor pagination.
  Pass `nil` as `last_key` for the first chunk.
  """
  def full_state_chunk(db, tombstone_cutoff, nil, limit) do
    now = System.system_time(:nanosecond)

    {:ok, rows} =
      EKV.Sqlite3.fetch_all(db, @full_state_first_chunk_sql, [tombstone_cutoff, now, limit])

    map_full_state_rows(rows)
  end

  def full_state_chunk(db, tombstone_cutoff, last_key, limit) do
    now = System.system_time(:nanosecond)

    {:ok, rows} =
      EKV.Sqlite3.fetch_all(db, @full_state_chunk_sql, [tombstone_cutoff, last_key, now, limit])

    map_full_state_rows(rows)
  end

  defp map_full_state_rows(rows) do
    Enum.map(rows, fn [key, value, timestamp, origin_node, origin_seq, expires_at, deleted_at] ->
      {key, value, timestamp, String.to_atom(origin_node), origin_seq, expires_at, deleted_at}
    end)
  end

  @oplog_since_chunk_sql """
  SELECT seq, key, value, timestamp, origin_node, origin_seq, expires_at, is_delete
  FROM kv_oplog
  WHERE seq > ?1
    AND (is_delete = 1 OR expires_at IS NULL OR expires_at > ?2)
  ORDER BY seq LIMIT ?3
  """

  @doc """
  Get a chunk of oplog entries since `seq`, ordered by seq with cursor pagination.
  """
  def oplog_since_chunk(db, seq, limit) do
    now = System.system_time(:nanosecond)
    {:ok, rows} = EKV.Sqlite3.fetch_all(db, @oplog_since_chunk_sql, [seq, now, limit])

    Enum.map(rows, fn [seq, key, value, timestamp, origin_node, origin_seq, expires_at, is_delete] ->
      {seq, key, value, timestamp, String.to_atom(origin_node), origin_seq, expires_at,
       is_delete == 1}
    end)
  end

  def touch_last_active(db) do
    now = System.system_time(:nanosecond)

    {:ok, stmt} =
      EKV.Sqlite3.prepare(db, """
      INSERT INTO kv_meta (key, value_int) VALUES ('last_active_at', ?1)
      ON CONFLICT(key) DO UPDATE SET value_int = excluded.value_int
      """)

    :ok = EKV.Sqlite3.bind(stmt, [now])
    :done = EKV.Sqlite3.step(db, stmt)
    :ok = EKV.Sqlite3.release(db, stmt)
    :ok
  end

  defp maybe_reject_stale_db(_path, _stale_threshold_ms, true), do: :ok

  defp maybe_reject_stale_db(path, stale_threshold_ms, false) do
    case stale_db_info(path, stale_threshold_ms) do
      nil -> :ok
      info -> {:error, {:stale_db, info}}
    end
  end

  defp stale_db_info(path, stale_threshold_ms) do
    if File.exists?(path) do
      case read_last_active(path) do
        nil ->
          # No meta table or no row — stale/unknown db, treat as stale.
          %{
            path: path,
            threshold_ms: stale_threshold_ms,
            age_ms: nil,
            reason: :missing_last_active_at
          }

        last_active_at ->
          now = System.system_time(:nanosecond)
          age_ms = div(now - last_active_at, 1_000_000)

          if age_ms > stale_threshold_ms do
            %{
              path: path,
              threshold_ms: stale_threshold_ms,
              age_ms: age_ms,
              reason: :stale_last_active_at
            }
          else
            nil
          end
      end
    else
      nil
    end
  end

  defp read_last_active(path) do
    case EKV.Sqlite3.open(path) do
      {:ok, db} ->
        result =
          try do
            case EKV.Sqlite3.prepare(
                   db,
                   "SELECT value_int FROM kv_meta WHERE key = 'last_active_at'"
                 ) do
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

  # =====================================================================
  # Metadata
  # =====================================================================

  def get_meta(db, key) do
    get_meta_int(db, key)
  end

  def set_meta(db, key, value) do
    set_meta_int(db, key, value)
  end

  def get_meta_int(db, key) do
    {:ok, stmt} = EKV.Sqlite3.prepare(db, "SELECT value_int FROM kv_meta WHERE key = ?1")
    :ok = EKV.Sqlite3.bind(stmt, [key])

    result =
      case EKV.Sqlite3.step(db, stmt) do
        {:row, [val]} -> val
        :done -> nil
      end

    :ok = EKV.Sqlite3.release(db, stmt)
    result
  end

  def set_meta_int(db, key, value) do
    {:ok, stmt} =
      EKV.Sqlite3.prepare(db, """
      INSERT INTO kv_meta (key, value_int) VALUES (?1, ?2)
      ON CONFLICT(key) DO UPDATE SET value_int = excluded.value_int
      """)

    :ok = EKV.Sqlite3.bind(stmt, [key, value])
    :done = EKV.Sqlite3.step(db, stmt)
    :ok = EKV.Sqlite3.release(db, stmt)
    :ok
  end

  def set_meta_int_if_absent(db, key, value) do
    case get_meta_int(db, key) do
      nil ->
        set_meta_int(db, key, value)
        :inserted

      _existing ->
        :exists
    end
  end

  def get_meta_text(db, key) do
    {:ok, stmt} = EKV.Sqlite3.prepare(db, "SELECT value_text FROM kv_meta WHERE key = ?1")
    :ok = EKV.Sqlite3.bind(stmt, [key])

    result =
      case EKV.Sqlite3.step(db, stmt) do
        {:row, [val]} -> val
        :done -> nil
      end

    :ok = EKV.Sqlite3.release(db, stmt)
    result
  end

  def set_meta_text(db, key, value) do
    {:ok, stmt} =
      EKV.Sqlite3.prepare(db, """
      INSERT INTO kv_meta (key, value_text) VALUES (?1, ?2)
      ON CONFLICT(key) DO UPDATE SET value_text = excluded.value_text
      """)

    :ok = EKV.Sqlite3.bind(stmt, [key, value])
    :done = EKV.Sqlite3.step(db, stmt)
    :ok = EKV.Sqlite3.release(db, stmt)
    :ok
  end

  def delete_meta_key(db, key) do
    {:ok, stmt} = EKV.Sqlite3.prepare(db, "DELETE FROM kv_meta WHERE key = ?1")
    :ok = EKV.Sqlite3.bind(stmt, [key])
    :done = EKV.Sqlite3.step(db, stmt)
    :ok = EKV.Sqlite3.release(db, stmt)
    :ok
  end

  def member_down_marker_get(db, key), do: get_meta_int(db, key)
  def member_down_marker_put(db, key, down_since_ms), do: set_meta_int(db, key, down_since_ms)

  def member_down_marker_set_if_absent(db, key, down_since_ms),
    do: set_meta_int_if_absent(db, key, down_since_ms)

  def member_down_marker_clear(db, key), do: delete_meta_key(db, key)

  def member_node_identity_get(db, member_node) when is_atom(member_node) do
    get_meta_text(db, "member_node_id:" <> Atom.to_string(member_node))
  end

  def member_node_identity_put(db, member_node, node_id)
      when is_atom(member_node) and is_binary(node_id) and byte_size(node_id) > 0 do
    set_meta_text(db, "member_node_id:" <> Atom.to_string(member_node), node_id)
  end

  def prune_member_down_name_markers(db, stale_before_ms, max_entries) do
    {:ok, rows} =
      EKV.Sqlite3.fetch_all(
        db,
        "SELECT key, value_int FROM kv_meta WHERE key LIKE 'member_down_at:name:%' ORDER BY value_int DESC",
        []
      )

    keys_to_delete =
      rows
      |> Enum.with_index()
      |> Enum.reduce([], fn {[key, down_since], idx}, acc ->
        stale? = not is_integer(down_since) or down_since < stale_before_ms
        over_cap? = idx >= max_entries
        if stale? or over_cap?, do: [key | acc], else: acc
      end)

    Enum.each(keys_to_delete, &delete_meta_key(db, &1))
    length(keys_to_delete)
  end

  def read_node_id(data_dir) do
    path = Path.join(data_dir, "shard_0.db")

    if File.exists?(path) do
      case EKV.Sqlite3.open(path) do
        {:ok, db} ->
          result =
            try do
              get_meta_text(db, "node_id")
            rescue
              _ -> nil
            end

          EKV.Sqlite3.close(db)
          result

        _ ->
          nil
      end
    end
  end

  def persist_node_id(db, node_id) do
    case get_meta_text(db, "node_id") do
      nil -> set_meta_text(db, "node_id", node_id)
      _ -> :ok
    end
  end

  # =====================================================================
  # Paxos
  # =====================================================================

  def paxos_prepare(db, key, ballot_counter, ballot_node) do
    EKV.Sqlite3.paxos_prepare(db, key, ballot_counter, ballot_node)
  end

  def paxos_accept(db, key, ballot_c, ballot_n, value_args) do
    EKV.Sqlite3.paxos_accept(db, key, ballot_c, ballot_n, value_args)
  end

  def paxos_promote(db, kv_force_stmt, oplog_stmt, key, ballot_c, ballot_n, origin_seq \\ nil) do
    EKV.Sqlite3.paxos_promote(db, kv_force_stmt, oplog_stmt, key, ballot_c, ballot_n, origin_seq)
  end

  def cas_managed_key?(db, key) do
    case EKV.Sqlite3.fetch_all(db, "SELECT 1 FROM kv_paxos WHERE key = ?1 LIMIT 1", [key]) do
      {:ok, []} -> false
      {:ok, _rows} -> true
      _ -> false
    end
  end

  @clear_paxos_accepted_sql """
  UPDATE kv_paxos SET accepted_counter = 0, accepted_node = '',
    accepted_value = NULL, accepted_timestamp = NULL, accepted_origin = NULL,
    accepted_expires_at = NULL, accepted_deleted_at = NULL
  WHERE key = ?1
  """

  @doc """
  Clear accepted value columns in kv_paxos after a commit.
  Called by the proposer after write_entry succeeds, so future prepares
  read from kv (committed state) rather than stale accepted values.

  Note: promised_counter is intentionally preserved — clearing it would allow
  stale accepts from older ballots to succeed, and kv_force_upsert would then
  unconditionally overwrite the committed value (CASPaxos violation).
  """
  def clear_paxos_accepted(db, key) do
    {:ok, stmt} = EKV.Sqlite3.prepare(db, @clear_paxos_accepted_sql)
    :ok = EKV.Sqlite3.bind(stmt, [key])
    :done = EKV.Sqlite3.step(db, stmt)
    :ok = EKV.Sqlite3.release(db, stmt)
    :ok
  end

  @doc """
  Remove kv_paxos rows for keys that no longer exist in kv.
  Called during GC to prevent unbounded growth.
  """
  def purge_orphan_paxos(db) do
    :ok =
      EKV.Sqlite3.execute(
        db,
        "DELETE FROM kv_paxos WHERE key NOT IN (SELECT key FROM kv) AND accepted_counter = 0 AND promised_counter = 0"
      )
  end

  # =====================================================================
  # Shard count validation
  # =====================================================================

  defp validate_schema_version(db, data_dir, shard_index) do
    case get_meta_int(db, "schema_version") do
      nil ->
        if initialized_db_without_schema_version?(db) do
          raise ArgumentError,
                "EKV schema_version mismatch for #{data_dir}/shard_#{shard_index}.db: " <>
                  "database has initialized state but no schema_version marker. " <>
                  "Start with a fresh data dir or migrate it before booting this build."
        end

        set_meta_int(db, "schema_version", @schema_version)

      @schema_version ->
        :ok

      other ->
        raise ArgumentError,
              "EKV schema_version mismatch for #{data_dir}/shard_#{shard_index}.db: " <>
                "database schema_version=#{other}, but this build expects schema_version=#{@schema_version}. " <>
                "Start with a fresh data dir or migrate it before booting this build."
    end
  end

  defp initialized_db_without_schema_version?(db) do
    Enum.any?(
      [
        "kv_meta",
        "kv",
        "kv_oplog",
        "kv_origin_progress",
        "kv_member_progress",
        "kv_member_hwm",
        "kv_paxos"
      ],
      &table_has_rows?(db, &1)
    )
  end

  defp table_has_rows?(db, table) do
    case EKV.Sqlite3.fetch_all(db, "SELECT 1 FROM #{table} LIMIT 1", []) do
      {:ok, []} -> false
      {:ok, [_ | _]} -> true
      _ -> false
    end
  end

  defp validate_num_shards(db, num_shards, data_dir, shard_index) do
    {:ok, stmt} =
      EKV.Sqlite3.prepare(db, "SELECT value_int FROM kv_meta WHERE key = 'num_shards'")

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
          INSERT INTO kv_meta (key, value_int) VALUES ('num_shards', ?1)
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
