defmodule EKV.Replica do
  @moduledoc false

  _archdoc = ~S"""
  EKV — Eventually Consistent Durable KV Store
  =============================================

  EKV is a sharded, replicated key-value store where data outlives the node
  that created it. EKV entries survive node restarts, node death, and network
  partitions. Data is only removed by explicit delete or TTL expiry.

  Peer discovery is fully self-contained: EKV uses :net_kernel.monitor_nodes/1
  and Node.list/0 directly. It has no external topology dependency.


  ## Supervision Tree

      EKV.Supervisor (rest_for_one)
      ├── EKV.Replica.Supervisor (one_for_one)
      │   ├── EKV.Replica 0     shard GenServer (writes + replication + SQLite)
      │   ├── EKV.Replica 1
      │   └── ...               N shards (default 8)
      └── EKV.GC                periodic timer, sends :gc to each shard

  rest_for_one means: single Replica crash → only that shard restarts.
  GC is downstream of Replicas.


  ## Storage: SQLite Only

  Each shard has a single SQLite database (WAL mode, synchronous=NORMAL):

      ┌──────────────────────────────────────────────────────────────┐
      │ SQLite (WAL mode, synchronous=NORMAL)                        │
      │ File: #{data_dir}/shard_#{i}.db                              │
      │                                                              │
      │ Tables:                                                      │
      │   kv          — current state, PK (key)                      │
      │   kv_oplog    — append-only mutation log, AUTOINCREMENT seq  │
      │   kv_peer_hwm — per-peer high-water marks for delta sync     │
      │   kv_meta     — liveness tracking                            │
      │                                                              │
      │ - Single source of truth. Survives process/node crashes.     │
      │ - kv + oplog writes are atomic (BEGIN IMMEDIATE / COMMIT).   │
      └──────────────────────────────────────────────────────────────┘

  Each shard opens System.schedulers_online() read connections, stored as a
  tuple in persistent_term keyed by {EKV, name, :readers, shard_index}.
  Reads pick a connection by rem(scheduler_id - 1, num_readers) — zero
  contention, no pool, no GenServer hop. WAL mode ensures readers don't
  block the writer.

  Values are stored as :erlang.term_to_binary/1 blobs. Encoding happens
  in the public EKV module; Replica and Store only see binaries.


  ## Sharding

  Shard assignment: :erlang.phash2(key, num_shards)

  Each shard is a completely independent GenServer with its own SQLite db
  file. Shards on different nodes with the same index are counterparts —
  they replicate to each other and sync on connect.

  Prefix scans (list/keys) cannot be routed to a single shard because
  the prefix doesn't determine the hash. They fan out to all shards.


  ## Write Path

      Client                  Replica (shard i)             Peers
        │                          │                          │
        │ GenServer.call           │                          │
        │  {:put, key,             │                          │
        │   value_binary, opts}    │                          │
        │─────────────────────────>│                          │
        │                          │                          │
        │                    LWW check vs SQLite              │
        │                    (skip if existing ts is higher)  │
        │                          │                          │
        │                    SQLite put_with_oplog (atomic)   │
        │                          │                          │
        │                          │ {:ekv_put, key,          │
        │                          │  value_binary, ts,       │
        │                          │  origin_node, expires_at}│
        │                          │─────────────────────────>│
        │                          │  (to counterpart shard   │
        │                          │   on each known peer)    │
        │                          │                          │
        │<─────────────────────────│                          │
        │         :ok              │                          │

  Delete is identical but sets deleted_at = now and value = nil. The
  broadcast message is {:ekv_delete, key, ts, origin_node}.

  Broadcasts go to Map.keys(state.remote_shards) — the set of nodes
  with a confirmed live counterpart shard for this shard index.


  ## Read Path

  Reads bypass the GenServer entirely:

      Client             SQLite (per-scheduler read connection)
        │                 │
        │  Store.get      │
        │────────────────>│   via read_conn(name, shard)
        │<────────────────│
        │                 │
        │  check deleted_at, expires_at
        │  binary_to_term if live
        │
        │  return value | nil

  No serialization, no message passing. Per-scheduler read connections
  stored in persistent_term ensure zero contention.


  ## Conflict Resolution: Last-Writer-Wins (LWW)

  Every entry carries a nanosecond timestamp and origin_node atom.

      lww_wins?(incoming_ts, incoming_origin, existing_ts, existing_origin)
        incoming_ts > existing_ts
        OR (incoming_ts == existing_ts AND incoming_origin > existing_origin)

  This function is used in ALL write paths:
    - Local put/delete (timestamp is always "now", so almost always wins)
    - Remote replication receive (ekv_put / ekv_delete)
    - Bulk sync (ekv_sync entries)
    - GC TTL expiry (converting expired entry to tombstone)

  The tiebreaker (origin_node atom comparison) is deterministic across all
  nodes, preventing mutual-overwrite on equal timestamps.

  A delete is just an entry with deleted_at set. Same LWW applies — a put
  with a higher timestamp beats a delete, and vice versa.


  ## Peer Discovery and Tracking

  EKV manages its own peer mesh independently:

      init/1:
        :net_kernel.monitor_nodes(true)
        for node <- Node.list(), send ekv_peer_connect

      nodeup:
        send ekv_peer_connect to the new node's counterpart shard

      nodedown:
        remove from remote_shards map. Does not purge any data.

      DOWN (monitored remote shard pid):
        remove from remote_shards map

  remote_shards :: %{node() => pid()} tracks confirmed live counterpart
  shard processes. A node enters this map only after a successful
  peer_connect / peer_connect_ack handshake where its pid is monitored.


  ## Peer Sync Protocol

  When two nodes discover each other (init, nodeup), they exchange a
  handshake per shard. The handshake determines whether to send a delta
  (oplog slice) or a full state snapshot.

      Node A (shard i)                        Node B (shard i)
        │                                         │
        │  {:ekv_peer_connect,                    │
        │   pid_a, i, num_shards, seq_a}          │
        │───────────────────────────────────────> │
        │                                         │
        │                          validate num_shards match
        │                          monitor pid_a
        │                          add A to remote_shards
        │                                         │
        │  {:ekv_peer_connect_ack,                │
        │   pid_b, i, num_shards, seq_b}          │
        │ <───────────────────────────────────────│
        │                                         │
        │                          send_sync_data(A, seq_a):
        │                            look up hwm for A
        │                            if hwm exists & oplog not truncated:
        │                              delta = oplog entries since hwm
        │                            else:
        │                              full = all live kv + recent tombstones
        │                                         │
        │  {:ekv_sync, node_b, i, entries, seq_b} │
        │ <───────────────────────────────────────│
        │                                         │
        │  (A does the same for B)                │
        │───────────────────────────────────────> │
        │  {:ekv_sync, node_a, i, entries, seq_a} │
        │                                         │

  Both sides send data. This is symmetric — each side sends what the
  other is missing based on HWMs.


  ## Delta Sync vs Full Sync

      ┌─────────────────────────────────────────────────────────────┐
      │ Delta Sync                                                  │
      │ Condition: we have a HWM for this peer AND our oplog still  │
      │            contains entries back to that HWM                │
      │                                                             │
      │ Query: SELECT * FROM kv_oplog WHERE seq > peer_hwm          │
      │                                                             │
      │ Sends only mutations since the last sync. Efficient for     │
      │ brief disconnects where the oplog hasn't been truncated.    │
      └─────────────────────────────────────────────────────────────┘

      ┌─────────────────────────────────────────────────────────────┐
      │ Full Sync                                                   │
      │ Condition: no HWM for this peer, OR oplog truncated past    │
      │            the peer's HWM (min_seq > peer_hwm)              │
      │                                                             │
      │ Query: SELECT * FROM kv WHERE deleted_at IS NULL            │
      │        OR deleted_at > tombstone_cutoff                     │
      │                                                             │
      │ Sends all live entries + recent tombstones (so the peer     │
      │ learns about deletes that happened while it was away).      │
      │ Used for first contact and after long partitions.           │
      └─────────────────────────────────────────────────────────────┘

  After receiving sync data, the receiver applies each entry through
  merge_remote_entry (LWW check), then records the sender's advertised
  max_seq as the new HWM for that peer.


  ## High-Water Marks (HWM)

  Each shard's SQLite db has a kv_peer_hwm table:

      peer_node TEXT PRIMARY KEY  →  last_seq INTEGER

  This records: "the last oplog seq I know peer X has seen". When peer X
  reconnects, I query oplog entries with seq > hwm[X] and send them.

  HWMs are updated in two places:
    1. In send_sync_data: after deciding what to send, record their
       advertised seq as our HWM for them.
    2. In ekv_sync handler: after applying received data, record the
       sender's seq from the sync message.


  ## Recovery Scenarios

  ### Scenario 1: Clean restart (same node, same data dir)

      Node crashes / restarts
        │
        Replica.init:
          Store.open  →  SQLite db still on disk
          open read connections
          monitor_nodes + send ekv_peer_connect
        │
        Peer responds with ekv_peer_connect_ack
          delta sync catches up missed mutations
        │
        Fully operational

  Data survives because SQLite is durable. The oplog enables efficient
  delta sync for the mutations missed while the node was down.

  ### Scenario 2: Fresh node (empty data dir, replacing a dead node)

      New node joins cluster
        │
        Replica.init:
          Store.open  →  creates fresh empty SQLite db
          open read connections
          monitor_nodes + send ekv_peer_connect
        │
        Peers have no HWM for this new node
          → full sync: send all live entries + recent tombstones
        │
        New node applies all entries via merge_remote_entry
        Records HWMs for all peers
        │
        Fully caught up

  ### Scenario 3: Network partition (2 groups can't talk)

      Before:   A ←→ B ←→ C     (fully meshed)
      Partition: {A, B} | {C}    (C isolated)

      During partition:
        - A and B replicate to each other normally
        - C writes to local SQLite only (no peers in remote_shards)
        - No data is lost on either side
        - nodedown fires, C removed from A/B's remote_shards

      Heal:
        - nodeup fires on both sides
        - ekv_peer_connect / ekv_peer_connect_ack exchanged
        - send_sync_data: if HWMs still valid → delta sync
                          if oplog truncated  → full sync
        - Both sides send their missed mutations
        - LWW resolves any conflicts deterministically:
            * Disjoint keys: union of both sides
            * Same key both sides: higher timestamp wins
            * Put vs delete: whichever has higher timestamp wins


  ## TTL (Time-To-Live)

      EKV.put(name, key, val, ttl: 30_000)
        → expires_at = System.system_time(:nanosecond) + ttl * 1_000_000

  expires_at is absolute nanoseconds, stored in SQLite and included
  in all replication messages.

  Read path: EKV.get checks expires_at lazily — returns nil if past.

  GC converts expired entries into tombstones:
    1. Find entries where expires_at < now AND deleted_at IS NULL
    2. Set deleted_at = now, append to oplog
    3. Broadcast {:ekv_delete, ...} so peers tombstone it too


  ## Garbage Collection (EKV.GC)

  Periodic timer sends {:gc, now, tombstone_cutoff} to each shard.
  The shard handles GC inside its own process (serialized with writes).

  Each tick, three phases:

      Phase 1: Expire TTL entries
        expired entries → set deleted_at, write oplog, broadcast delete
        (converts live-but-expired entries into proper tombstones)

      Phase 2: Purge old tombstones
        deleted_at < now - tombstone_ttl → hard delete from SQLite kv
        (tombstone_ttl default: 7 days — keeps tombstones long enough for
         partitioned nodes to learn about deletes when they reconnect)

      Phase 3: Prune stale peer HWMs
        Remove kv_peer_hwm rows for peers not currently connected.
        Prevents dead/decommissioned peers from anchoring the oplog
        forever. Disconnected peers get full sync on reconnect.

      Phase 4: Truncate oplog
        DELETE FROM kv_oplog WHERE seq < MIN(all peer HWMs)
        (keeps oplog bounded; entries below the slowest connected
         peer's HWM are no longer needed for delta sync)


  ## Message Reference

  Steady-state replication (per-operation, fire-and-forget):
    {:ekv_put, key, value_binary, timestamp, origin_node, expires_at}
    {:ekv_delete, key, timestamp, origin_node}

  Peer handshake (on nodeup / init):
    {:ekv_peer_connect, pid, shard_index, num_shards, my_max_seq}
    {:ekv_peer_connect_ack, pid, shard_index, num_shards, my_max_seq}

  Bulk sync (after handshake):
    {:ekv_sync, from_node, shard_index, entries, sender_max_seq}
      entries: [{key, value_binary, timestamp, origin_node,
                 expires_at, deleted_at}]

  GC (from EKV.GC timer):
    {:gc, now_nanoseconds, tombstone_cutoff_nanoseconds}

  All messages are sent to the counterpart shard by registered name:
    send({:"#{name}_ekv_replica_#{shard}", target_node}, message)

  There is no gossip or re-broadcast. Replication is direct: the node
  that performs a write sends to all known peers exactly once.
  """

  use GenServer

  require Logger

  alias EKV.Store

  defstruct [
    :name,
    :shard_index,
    :num_shards,
    :db,
    :data_dir,
    :stmts,
    readers: [],
    remote_shards: %{},
    # CAS fields
    node_id: nil,
    cluster_size: nil,
    ballot_counter: 0,
    peer_node_ids: %{},
    pending_cas: %{}
  ]

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    shard_index = Keyword.fetch!(opts, :shard_index)
    GenServer.start_link(__MODULE__, opts, name: shard_name(name, shard_index))
  end

  def shard_name(name, shard_index), do: :"#{name}_ekv_replica_#{shard_index}"

  def shard_index_for(key, num_shards) do
    :erlang.phash2(key, num_shards)
  end

  # =====================================================================
  # GenServer callbacks
  # =====================================================================

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    name = Keyword.fetch!(opts, :name)
    shard_index = Keyword.fetch!(opts, :shard_index)
    num_shards = Keyword.fetch!(opts, :num_shards)
    data_dir = Keyword.fetch!(opts, :data_dir)

    config = EKV.get_config(name)

    {:ok, db} =
      Store.open(data_dir, shard_index, config.tombstone_ttl, num_shards, config.gc_interval)

    # Open per-scheduler read connections
    db_path = Path.join(data_dir, "shard_#{shard_index}.db")
    num_readers = System.schedulers_online()

    readers =
      for _ <- 1..num_readers do
        {:ok, rdb} = Store.open_reader(db_path)
        get_stmt = Store.prepare_read_stmt(rdb)
        {rdb, get_stmt}
      end

    readers_tuple = List.to_tuple(readers)
    :persistent_term.put({EKV, name, :readers, shard_index}, readers_tuple)

    # Prepare cached statements on writer connection
    stmts = Store.prepare_cached_stmts(db)

    # CAS ballot counter — restore from persisted value
    ballot_counter =
      if config.cluster_size do
        persisted = Store.get_meta(db, "ballot_counter") || 0
        max(System.system_time(:nanosecond), persisted + 1)
      else
        0
      end

    state = %__MODULE__{
      name: name,
      shard_index: shard_index,
      num_shards: num_shards,
      db: db,
      data_dir: data_dir,
      stmts: stmts,
      readers: readers,
      node_id: config.node_id,
      cluster_size: config.cluster_size,
      ballot_counter: ballot_counter
    }

    :net_kernel.monitor_nodes(true)

    log_once(state, fn -> "#{log_prefix(state)} started (shards=#{num_shards})" end)

    # Discover peers on all known nodes
    registered_name = shard_name(name, shard_index)
    my_seq = Store.max_seq(db)

    for remote_node <- Node.list() do
      send(
        {registered_name, remote_node},
        {:ekv_peer_connect, self(), shard_index, num_shards, my_seq, config.node_id}
      )
    end

    {:ok, state}
  end

  @impl true
  def terminate(_reason, state) do
    for {rdb, get_stmt} <- state.readers do
      EKV.Sqlite3.release(rdb, get_stmt)
      Store.close(rdb)
    end

    try do
      :persistent_term.erase({EKV, state.name, :readers, state.shard_index})
    rescue
      ArgumentError -> :ok
    end

    # Persist ballot counter for CAS
    if state.cluster_size && state.db do
      Store.set_meta(state.db, "ballot_counter", state.ballot_counter)
    end

    # Release cached statements before closing connections
    if state.stmts, do: Store.release_stmts(state.db, state.stmts)

    if state.db, do: Store.close(state.db)
    :ok
  end

  # =====================================================================
  # Write calls
  # =====================================================================

  @impl true
  def handle_call({:put, key, value_binary, opts}, _from, state) do
    %{db: db, stmts: stmts} = state
    now = System.system_time(:nanosecond)
    origin_node = node()

    ttl = Keyword.get(opts, :ttl)
    expires_at = if ttl, do: now + ttl * 1_000_000

    {:ok, applied} =
      Store.write_entry(
        db,
        stmts.kv_upsert,
        stmts.oplog_insert,
        key,
        value_binary,
        now,
        origin_node,
        expires_at,
        nil
      )

    if applied do
      broadcast_to_peers(state, {:ekv_put, key, value_binary, now, origin_node, expires_at})

      dispatch_events(state, [
        %EKV.Event{type: :put, key: key, value: :erlang.binary_to_term(value_binary)}
      ])
    end

    {:reply, :ok, state}
  end

  def handle_call({:delete, key}, _from, state) do
    %{db: db, stmts: stmts} = state
    now = System.system_time(:nanosecond)
    origin_node = node()

    prev_value = if has_subscribers?(state), do: read_previous_value(state, key)

    {:ok, applied} =
      Store.write_entry(
        db,
        stmts.kv_upsert,
        stmts.oplog_insert,
        key,
        nil,
        now,
        origin_node,
        nil,
        now
      )

    if applied do
      broadcast_to_peers(state, {:ekv_delete, key, now, origin_node})
      dispatch_events(state, [%EKV.Event{type: :delete, key: key, value: prev_value}])
    end

    {:reply, :ok, state}
  end

  # =====================================================================
  # CAS write calls
  # =====================================================================

  def handle_call({:cas_put, key, value_binary, expected_vsn, opts}, from, state) do
    operation = {:cas_put, expected_vsn, value_binary, opts}
    start_cas(key, operation, from, state)
  end

  def handle_call({:cas_delete, key, expected_vsn}, from, state) do
    operation = {:cas_delete, expected_vsn}
    start_cas(key, operation, from, state)
  end

  def handle_call({:update, key, fun, opts}, from, state) do
    operation = {:update, fun, opts, 5}
    start_cas(key, operation, from, state)
  end

  # =====================================================================
  # Replication receive
  # =====================================================================

  @impl true
  def handle_info({:ekv_put, key, value_binary, timestamp, origin_node, expires_at}, state) do
    {:ok, applied} =
      merge_remote_entry(state, key, value_binary, timestamp, origin_node, expires_at, nil)

    if applied do
      dispatch_events(state, [
        %EKV.Event{type: :put, key: key, value: :erlang.binary_to_term(value_binary)}
      ])
    end

    {:noreply, state}
  end

  def handle_info({:ekv_delete, key, timestamp, origin_node}, state) do
    prev_value = if has_subscribers?(state), do: read_previous_value(state, key)

    {:ok, applied} =
      merge_remote_entry(state, key, nil, timestamp, origin_node, nil, timestamp)

    if applied do
      dispatch_events(state, [%EKV.Event{type: :delete, key: key, value: prev_value}])
    end

    {:noreply, state}
  end

  # =====================================================================
  # Peer sync protocol
  # =====================================================================

  # Handle both old (5-tuple) and new (6-tuple with node_id) peer_connect
  def handle_info({:ekv_peer_connect, remote_pid, remote_shard, remote_num_shards, remote_seq}, state) do
    do_peer_connect(remote_pid, remote_shard, remote_num_shards, remote_seq, nil, state)
  end

  def handle_info({:ekv_peer_connect, remote_pid, remote_shard, remote_num_shards, remote_seq, remote_node_id}, state) do
    do_peer_connect(remote_pid, remote_shard, remote_num_shards, remote_seq, remote_node_id, state)
  end

  # Handle both old (5-tuple) and new (6-tuple with node_id) peer_connect_ack
  def handle_info({:ekv_peer_connect_ack, remote_pid, remote_shard, remote_num_shards, remote_seq}, state) do
    do_peer_connect_ack(remote_pid, remote_shard, remote_num_shards, remote_seq, nil, state)
  end

  def handle_info({:ekv_peer_connect_ack, remote_pid, remote_shard, remote_num_shards, remote_seq, remote_node_id}, state) do
    do_peer_connect_ack(remote_pid, remote_shard, remote_num_shards, remote_seq, remote_node_id, state)
  end

  def handle_info({:ekv_sync, from_node, _shard, entries, their_seq}, state) do
    %{shard_index: shard, db: db, num_shards: num_shards} = state

    log_verbose(state, fn ->
      "#{log_prefix_shard(state)} ekv_sync from #{from_node} (#{length(entries)} entries)"
    end)

    has_subs = has_subscribers?(state)

    sync_events =
      Enum.reduce(entries, [], fn {key, value_binary, timestamp, origin_node, expires_at,
                                   deleted_at},
                                  acc ->
        if shard_index_for(key, num_shards) == shard do
          prev_value = if deleted_at && has_subs, do: read_previous_value(state, key)

          {:ok, applied} =
            if deleted_at do
              merge_remote_entry(state, key, nil, timestamp, origin_node, nil, deleted_at)
            else
              merge_remote_entry(
                state,
                key,
                value_binary,
                timestamp,
                origin_node,
                expires_at,
                nil
              )
            end

          if applied do
            event =
              if deleted_at,
                do: %EKV.Event{type: :delete, key: key, value: prev_value},
                else: %EKV.Event{
                  type: :put,
                  key: key,
                  value: :erlang.binary_to_term(value_binary)
                }

            [event | acc]
          else
            acc
          end
        else
          acc
        end
      end)

    dispatch_events(state, Enum.reverse(sync_events))

    # Update HWM for the sender
    Store.set_hwm(db, from_node, their_seq)

    {:noreply, state}
  end

  # =====================================================================
  # Node up/down
  # =====================================================================

  def handle_info({:nodeup, remote_node}, state) do
    %{shard_index: shard, name: name, db: db} = state
    my_seq = Store.max_seq(db)

    send(
      {shard_name(name, shard), remote_node},
      {:ekv_peer_connect, self(), shard, state.num_shards, my_seq, state.node_id}
    )

    {:noreply, state}
  end

  def handle_info({:nodedown, dead_node}, state) do
    log_once(state, fn -> "#{log_prefix(state)} nodedown #{dead_node} (data preserved)" end)

    state = %{state |
      remote_shards: Map.delete(state.remote_shards, dead_node),
      peer_node_ids: Map.delete(state.peer_node_ids, dead_node)
    }

    # Check if any pending CAS ops lost quorum
    state = fail_pending_cas_if_no_quorum(state)

    {:noreply, state}
  end

  # =====================================================================
  # Process DOWN (remote shard died)
  # =====================================================================

  def handle_info({:DOWN, _mref, :process, pid, _reason}, state) do
    remote_node = node(pid)

    if Map.get(state.remote_shards, remote_node) == pid do
      log_verbose(state, fn ->
        "#{log_prefix_shard(state)} remote_shard_down #{remote_node} (data preserved)"
      end)

      state = %{state |
        remote_shards: Map.delete(state.remote_shards, remote_node),
        peer_node_ids: Map.delete(state.peer_node_ids, remote_node)
      }

      state = fail_pending_cas_if_no_quorum(state)
      {:noreply, state}
    else
      {:noreply, state}
    end
  end

  # =====================================================================
  # CAS Acceptor handlers (remote proposer sends to us)
  # =====================================================================

  def handle_info({:ekv_prepare, ref, proposer_pid, key, ballot_c, ballot_n, _shard}, state) do
    %{db: db} = state

    case Store.paxos_prepare(db, key, ballot_c, ballot_n) do
      {:ok, :promise, acc_c, acc_n, kv_row} ->
        send(proposer_pid, {:ekv_promise, ref, self(), state.node_id, acc_c, acc_n, kv_row})

      {:ok, :nack, prom_c, prom_n} ->
        send(proposer_pid, {:ekv_nack, ref, self(), state.node_id, prom_c, prom_n})
    end

    {:noreply, state}
  end

  def handle_info({:ekv_accept, ref, proposer_pid, key, ballot_c, ballot_n, entry_tuple, _shard}, state) do
    %{db: db, stmts: stmts} = state

    {_key, value_binary, timestamp, origin_node_str, expires_at, deleted_at} = entry_tuple
    is_delete = if deleted_at, do: 1, else: 0
    kv_args = [key, value_binary, timestamp, origin_node_str, expires_at, deleted_at]
    oplog_args = [key, value_binary, timestamp, origin_node_str, expires_at, is_delete]

    case Store.paxos_accept(db, stmts.kv_upsert, stmts.oplog_insert, key, ballot_c, ballot_n, kv_args, oplog_args) do
      {:ok, true} ->
        send(proposer_pid, {:ekv_accepted, ref, self(), state.node_id})

        # Dispatch subscriber events for accepted writes
        if deleted_at do
          prev_value = if has_subscribers?(state), do: read_previous_value(state, key)
          dispatch_events(state, [%EKV.Event{type: :delete, key: key, value: prev_value}])
        else
          dispatch_events(state, [
            %EKV.Event{type: :put, key: key, value: :erlang.binary_to_term(value_binary)}
          ])
        end

      {:ok, false} ->
        send(proposer_pid, {:ekv_accept_nack, ref, self(), state.node_id})
    end

    {:noreply, state}
  end

  # =====================================================================
  # CAS Proposer response handlers (responses from acceptors)
  # =====================================================================

  def handle_info({:ekv_promise, ref, _pid, remote_node_id, acc_c, acc_n, kv_row}, state) do
    case Map.get(state.pending_cas, ref) do
      nil ->
        {:noreply, state}

      %{phase: :prepare} = op ->
        if MapSet.member?(op.responded, remote_node_id) do
          {:noreply, state}
        else
          op = %{op |
            promises: [{remote_node_id, acc_c, acc_n, kv_row} | op.promises],
            responded: MapSet.put(op.responded, remote_node_id)
          }

          if length(op.promises) >= op.quorum do
            state = enter_accept_phase(ref, op, state)
            {:noreply, state}
          else
            {:noreply, %{state | pending_cas: Map.put(state.pending_cas, ref, op)}}
          end
        end

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:ekv_nack, ref, _pid, remote_node_id, _prom_c, _prom_n}, state) do
    case Map.get(state.pending_cas, ref) do
      nil ->
        {:noreply, state}

      %{phase: :prepare} = op ->
        if MapSet.member?(op.responded, remote_node_id) do
          {:noreply, state}
        else
          op = %{op |
            nacks: op.nacks + 1,
            responded: MapSet.put(op.responded, remote_node_id)
          }

          max_possible_promises = count_alive_node_ids(state) - op.nacks

          if max_possible_promises < op.quorum do
            # Can't reach quorum — fail or retry
            state = handle_cas_failure(ref, op, state)
            {:noreply, state}
          else
            {:noreply, %{state | pending_cas: Map.put(state.pending_cas, ref, op)}}
          end
        end

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:ekv_accepted, ref, _pid, remote_node_id}, state) do
    case Map.get(state.pending_cas, ref) do
      nil ->
        {:noreply, state}

      %{phase: :accept} = op ->
        if MapSet.member?(op.responded, remote_node_id) do
          {:noreply, state}
        else
          accepts = MapSet.put(op.accepts, remote_node_id)
          responded = MapSet.put(op.responded, remote_node_id)
          op = %{op | accepts: accepts, responded: responded}

          if MapSet.size(accepts) >= op.quorum do
            # Quorum reached — success!
            cancel_timer(op.timer)
            GenServer.reply(op.from, op.reply_value)

            # Broadcast to all peers for LWW replication
            if op.broadcast_msg, do: broadcast_to_peers(state, op.broadcast_msg)

            {:noreply, %{state | pending_cas: Map.delete(state.pending_cas, ref)}}
          else
            {:noreply, %{state | pending_cas: Map.put(state.pending_cas, ref, op)}}
          end
        end

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:ekv_accept_nack, ref, _pid, remote_node_id}, state) do
    case Map.get(state.pending_cas, ref) do
      nil ->
        {:noreply, state}

      %{phase: :accept} = op ->
        if MapSet.member?(op.responded, remote_node_id) do
          {:noreply, state}
        else
          op = %{op |
            accept_nacks: op.accept_nacks + 1,
            responded: MapSet.put(op.responded, remote_node_id)
          }

          max_possible = count_alive_node_ids(state) - op.accept_nacks

          if max_possible < op.quorum do
            state = handle_cas_failure(ref, op, state)
            {:noreply, state}
          else
            {:noreply, %{state | pending_cas: Map.put(state.pending_cas, ref, op)}}
          end
        end

      _ ->
        {:noreply, state}
    end
  end

  # CAS timeout
  def handle_info({:cas_timeout, ref}, state) do
    case Map.pop(state.pending_cas, ref) do
      {nil, _} ->
        {:noreply, state}

      {op, pending_cas} ->
        GenServer.reply(op.from, {:error, :quorum_timeout})
        {:noreply, %{state | pending_cas: pending_cas}}
    end
  end

  # CAS retry (for update only)
  def handle_info({:cas_retry, ref, key, operation}, state) do
    # Re-check if we still have the pending op (might have been cleaned up)
    case Map.pop(state.pending_cas, ref) do
      {nil, _} ->
        {:noreply, state}

      {old_op, pending_cas} ->
        state = %{state | pending_cas: pending_cas}
        # Start a new CAS round
        {:noreply, state} = start_cas_internal(key, operation, old_op.from, state)
        {:noreply, state}
    end
  end

  # =====================================================================
  # GC
  # =====================================================================

  def handle_info({:gc, now, tombstone_cutoff}, state) do
    %{db: db, stmts: stmts} = state

    # 1. Expire TTL entries → tombstones → broadcast deletes
    expired = Store.find_expired(db, now)

    gc_events =
      Enum.reduce(expired, [], fn {key, value_binary, _timestamp, _origin_node, _expires_at},
                                  acc ->
        origin = node()

        {:ok, applied} =
          Store.write_entry(
            db,
            stmts.kv_upsert,
            stmts.oplog_insert,
            key,
            nil,
            now,
            origin,
            nil,
            now
          )

        if applied do
          broadcast_to_peers(state, {:ekv_delete, key, now, origin})
          prev_value = if value_binary, do: :erlang.binary_to_term(value_binary)
          [%EKV.Event{type: :delete, key: key, value: prev_value} | acc]
        else
          acc
        end
      end)

    dispatch_events(state, Enum.reverse(gc_events))

    # 2. Purge old tombstones from SQLite (no notification — already notified on delete)
    Store.purge_tombstones(db, tombstone_cutoff)

    # 2b. Purge orphan kv_paxos rows (keys that were tombstone-purged)
    if state.cluster_size, do: Store.purge_orphan_paxos(db)

    # 3. Prune HWMs for disconnected peers (prevents unbounded oplog growth)
    Store.prune_peer_hwms(db, Map.keys(state.remote_shards))

    # 4. Truncate oplog
    Store.truncate_oplog(db)

    # 5. Bump liveness timestamp
    Store.touch_last_active(db)

    {:noreply, state}
  end

  def handle_info(_msg, state) do
    {:noreply, state}
  end

  # =====================================================================
  # Internal helpers
  # =====================================================================

  defp do_peer_connect(remote_pid, remote_shard, remote_num_shards, remote_seq, remote_node_id, state)
       when remote_shard == state.shard_index do
    if remote_num_shards != state.num_shards do
      Logger.error(
        "#{log_prefix(state)} rejecting peer_connect from #{node(remote_pid)}: " <>
          "shard count mismatch (local=#{state.num_shards}, remote=#{remote_num_shards})"
      )

      {:noreply, state}
    else
      %{db: db} = state
      remote_node = node(remote_pid)
      my_seq = Store.max_seq(db)

      state = track_remote_shard(state, remote_node, remote_pid)
      state = track_peer_node_id(state, remote_node, remote_node_id)

      send_to_peer(
        state,
        remote_node,
        {:ekv_peer_connect_ack, self(), state.shard_index, state.num_shards, my_seq, state.node_id}
      )

      log_once(state, fn -> "#{log_prefix(state)} ekv_peer_connect from #{remote_node}" end)
      send_sync_data(state, remote_node, remote_seq)

      {:noreply, state}
    end
  end

  defp do_peer_connect(_remote_pid, _remote_shard, _remote_num_shards, _remote_seq, _remote_node_id, state) do
    {:noreply, state}
  end

  defp do_peer_connect_ack(remote_pid, remote_shard, remote_num_shards, remote_seq, remote_node_id, state)
       when remote_shard == state.shard_index do
    if remote_num_shards != state.num_shards do
      Logger.error(
        "#{log_prefix(state)} rejecting peer_connect_ack from #{node(remote_pid)}: " <>
          "shard count mismatch (local=#{state.num_shards}, remote=#{remote_num_shards})"
      )

      {:noreply, state}
    else
      remote_node = node(remote_pid)

      state = track_remote_shard(state, remote_node, remote_pid)
      state = track_peer_node_id(state, remote_node, remote_node_id)

      log_once(state, fn ->
        "#{log_prefix(state)} ekv_peer_connect_ack from #{remote_node}"
      end)

      send_sync_data(state, remote_node, remote_seq)

      {:noreply, state}
    end
  end

  defp do_peer_connect_ack(_remote_pid, _remote_shard, _remote_num_shards, _remote_seq, _remote_node_id, state) do
    {:noreply, state}
  end

  defp merge_remote_entry(
         state,
         key,
         value_binary,
         timestamp,
         origin_node,
         expires_at,
         deleted_at
       ) do
    %{db: db, stmts: stmts} = state

    Store.write_entry(
      db,
      stmts.kv_upsert,
      stmts.oplog_insert,
      key,
      value_binary,
      timestamp,
      origin_node,
      expires_at,
      deleted_at
    )
  end

  defp send_sync_data(state, remote_node, remote_seq) do
    %{db: db} = state
    config = EKV.get_config(state.name)
    tombstone_cutoff = System.system_time(:nanosecond) - config.tombstone_ttl * 1_000_000

    # Check if we have a HWM for this peer and can do delta sync
    peer_hwm = Store.get_hwm(db, remote_node)
    my_min_seq = Store.min_seq(db)

    {entries, their_seq_to_record} =
      cond do
        # Peer told us their seq, so after sync we'll record it as their HWM
        # Can we send a delta?
        peer_hwm && peer_hwm >= my_min_seq ->
          # Delta: send oplog entries since the HWM we have for them
          oplog_entries = Store.oplog_since(db, peer_hwm)

          entries =
            Enum.map(oplog_entries, fn {_seq, key, value, timestamp, origin_node, expires_at,
                                        is_delete} ->
              deleted_at = if is_delete, do: timestamp, else: nil
              {key, value, timestamp, origin_node, expires_at, deleted_at}
            end)

          {entries, remote_seq}

        true ->
          # Full sync
          entries = Store.full_state(db, tombstone_cutoff)
          {entries, remote_seq}
      end

    if entries != [] do
      my_seq = Store.max_seq(db)

      send_to_peer(
        state,
        remote_node,
        {:ekv_sync, node(), state.shard_index, entries, my_seq}
      )
    end

    # Record their advertised seq as our HWM for them
    if their_seq_to_record > 0 do
      Store.set_hwm(db, remote_node, their_seq_to_record)
    end
  end

  defp send_to_peer(state, target_node, message) do
    shard_name = shard_name(state.name, state.shard_index)
    send({shard_name, target_node}, message)
  end

  # Track a remote shard pid in remote_shards. Handles three cases:
  # 1. New node: monitor and add
  # 2. Same pid: no-op
  # 3. Different pid (shard restarted): demonitor old, monitor new, update
  defp track_remote_shard(state, remote_node, remote_pid) do
    case Map.get(state.remote_shards, remote_node) do
      nil ->
        Process.monitor(remote_pid)
        %{state | remote_shards: Map.put(state.remote_shards, remote_node, remote_pid)}

      ^remote_pid ->
        state

      _old_pid ->
        Process.monitor(remote_pid)
        %{state | remote_shards: Map.put(state.remote_shards, remote_node, remote_pid)}
    end
  end

  defp broadcast_to_peers(state, message) do
    shard_name = shard_name(state.name, state.shard_index)

    for target_node <- Map.keys(state.remote_shards) do
      send({shard_name, target_node}, message)
    end
  end

  # =====================================================================
  # CAS helpers
  # =====================================================================

  defp start_cas(key, operation, from, state) do
    {:noreply, state} = start_cas_internal(key, operation, from, state)
    {:noreply, state}
  end

  defp start_cas_internal(key, operation, from, state) do
    %{db: db, cluster_size: cluster_size, node_id: node_id} = state
    quorum = div(cluster_size, 2) + 1

    # Check quorum achievable
    alive_count = count_alive_node_ids(state)

    if alive_count < quorum do
      GenServer.reply(from, {:error, :no_quorum})
      {:noreply, state}
    else
      # Generate ballot
      {ballot_c, ballot_n, state} = next_ballot(state)
      ref = make_ref()

      # Local prepare (this node is always an acceptor)
      local_result = Store.paxos_prepare(db, key, ballot_c, ballot_n)

      {local_promise, local_nack} =
        case local_result do
          {:ok, :promise, acc_c, acc_n, kv_row} ->
            {[{node_id, acc_c, acc_n, kv_row}], 0}

          {:ok, :nack, _prom_c, _prom_n} ->
            {[], 1}
        end

      # Send prepare to all peers
      for {remote_node, _pid} <- state.remote_shards do
        send_to_peer(state, remote_node,
          {:ekv_prepare, ref, self(), key, ballot_c, ballot_n, state.shard_index})
      end

      # Start timeout timer
      timer = Process.send_after(self(), {:cas_timeout, ref}, 5_000)

      op = %{
        ref: ref,
        from: from,
        key: key,
        ballot: {ballot_c, ballot_n},
        phase: :prepare,
        operation: operation,
        promises: local_promise,
        nacks: local_nack,
        accepts: MapSet.new(),
        accept_nacks: 0,
        responded: MapSet.new([node_id]),
        quorum: quorum,
        timer: timer,
        reply_value: nil,
        broadcast_msg: nil
      }

      # Check if local promise already gave us quorum (cluster_size: 1)
      if length(op.promises) >= quorum do
        state = %{state | pending_cas: Map.put(state.pending_cas, ref, op)}
        state = enter_accept_phase(ref, op, state)
        {:noreply, state}
      else if local_nack > 0 and alive_count - local_nack < quorum do
        # Can't reach quorum
        cancel_timer(timer)
        state = handle_cas_failure(ref, op, %{state | pending_cas: Map.put(state.pending_cas, ref, op)})
        {:noreply, state}
      else
        {:noreply, %{state | pending_cas: Map.put(state.pending_cas, ref, op)}}
      end
      end
    end
  end

  defp next_ballot(state) do
    counter = max(System.system_time(:nanosecond), state.ballot_counter + 1)
    {counter, state.node_id, %{state | ballot_counter: counter}}
  end

  defp enter_accept_phase(ref, op, state) do
    cancel_timer(op.timer)

    # Find highest accepted ballot from promises
    {_best_node_id, best_acc_c, best_acc_n, best_kv_row} =
      Enum.max_by(op.promises, fn {_nid, acc_c, acc_n, _row} -> {acc_c, acc_n} end)

    # The value with the highest accepted ballot is the current state
    # If all accepted ballots are {0, 0}, no value was ever accepted
    {current_value, current_vsn} =
      if best_acc_c == 0 and best_acc_n == 0 do
        # Use any non-nil kv_row from promises (there might be LWW-written data)
        best_kv_row_any = Enum.find_value(op.promises, fn {_, _, _, row} -> row end)
        decode_kv_row(best_kv_row_any)
      else
        decode_kv_row(best_kv_row)
      end

    # Apply operation
    case apply_operation(op.operation, op.key, current_value, current_vsn) do
      {:ok, _new_value_binary, new_entry_tuple, reply_value, broadcast_msg, events} ->
        %{db: db, stmts: stmts} = state
        {key, value_binary, timestamp, origin_str, expires_at, deleted_at} = new_entry_tuple
        is_delete = if deleted_at, do: 1, else: 0
        kv_args = [key, value_binary, timestamp, origin_str, expires_at, deleted_at]
        oplog_args = [key, value_binary, timestamp, origin_str, expires_at, is_delete]

        {ballot_c, ballot_n} = op.ballot

        # Local accept
        case Store.paxos_accept(db, stmts.kv_upsert, stmts.oplog_insert, op.key, ballot_c, ballot_n, kv_args, oplog_args) do
          {:ok, true} ->
            # Dispatch local events
            dispatch_events(state, events)

            # Send accept to all peers
            for {remote_node, _pid} <- state.remote_shards do
              send_to_peer(state, remote_node,
                {:ekv_accept, ref, self(), op.key, ballot_c, ballot_n, new_entry_tuple, state.shard_index})
            end

            timer = Process.send_after(self(), {:cas_timeout, ref}, 5_000)

            op = %{op |
              phase: :accept,
              accepts: MapSet.new([state.node_id]),
              accept_nacks: 0,
              responded: MapSet.new([state.node_id]),
              timer: timer,
              reply_value: reply_value,
              broadcast_msg: broadcast_msg
            }

            # Check if local accept already gave us quorum (cluster_size: 1)
            if MapSet.size(op.accepts) >= op.quorum do
              cancel_timer(timer)
              GenServer.reply(op.from, reply_value)
              if broadcast_msg, do: broadcast_to_peers(state, broadcast_msg)
              %{state | pending_cas: Map.delete(state.pending_cas, ref)}
            else
              %{state | pending_cas: Map.put(state.pending_cas, ref, op)}
            end

          {:ok, false} ->
            # LWW lost — should not happen since CAS uses fresh timestamps
            # but handle gracefully
            handle_cas_failure(ref, op, state)
        end

      {:error, :conflict} ->
        GenServer.reply(op.from, {:error, :conflict})
        %{state | pending_cas: Map.delete(state.pending_cas, ref)}
    end
  end

  defp apply_operation(operation, key, current_value, current_vsn) do
    case operation do
      {:cas_put, expected_vsn, value_binary, opts} ->
        if current_vsn == expected_vsn do
          now = System.system_time(:nanosecond)
          origin = node()
          origin_str = Atom.to_string(origin)
          ttl = Keyword.get(opts, :ttl)
          expires_at = if ttl, do: now + ttl * 1_000_000

          entry_tuple = {key, value_binary, now, origin_str, expires_at, nil}
          broadcast_msg = {:ekv_put, key, value_binary, now, origin, expires_at}
          events = [%EKV.Event{type: :put, key: key, value: :erlang.binary_to_term(value_binary)}]
          {:ok, value_binary, entry_tuple, :ok, broadcast_msg, events}
        else
          {:error, :conflict}
        end

      {:cas_delete, expected_vsn} ->
        if current_vsn == expected_vsn do
          now = System.system_time(:nanosecond)
          origin = node()
          origin_str = Atom.to_string(origin)

          entry_tuple = {key, nil, now, origin_str, nil, now}
          broadcast_msg = {:ekv_delete, key, now, origin}
          events = [%EKV.Event{type: :delete, key: key, value: current_value}]
          {:ok, nil, entry_tuple, :ok, broadcast_msg, events}
        else
          {:error, :conflict}
        end

      {:update, fun, opts, _retries} ->
        new_value = fun.(current_value)
        new_value_binary = :erlang.term_to_binary(new_value)
        now = System.system_time(:nanosecond)
        origin = node()
        origin_str = Atom.to_string(origin)
        ttl = Keyword.get(opts, :ttl)
        expires_at = if ttl, do: now + ttl * 1_000_000

        entry_tuple = {key, new_value_binary, now, origin_str, expires_at, nil}
        broadcast_msg = {:ekv_put, key, new_value_binary, now, origin, expires_at}
        events = [%EKV.Event{type: :put, key: key, value: new_value}]
        {:ok, new_value_binary, entry_tuple, {:ok, new_value}, broadcast_msg, events}
    end
  end

  defp decode_kv_row(nil), do: {nil, nil}

  defp decode_kv_row([value_binary, timestamp, origin_node_str, expires_at, deleted_at]) do
    now = System.system_time(:nanosecond)

    cond do
      # Deleted entry → treat as absent
      is_integer(deleted_at) ->
        {nil, nil}

      # Expired entry → treat as absent
      is_integer(expires_at) and expires_at <= now ->
        {nil, nil}

      # Live entry
      true ->
        origin = if is_binary(origin_node_str), do: String.to_atom(origin_node_str), else: origin_node_str
        value = if value_binary, do: :erlang.binary_to_term(value_binary)
        {value, {timestamp, origin}}
    end
  end

  defp handle_cas_failure(ref, op, state) do
    cancel_timer(op.timer)

    case op.operation do
      {:update, fun, opts, retries} when retries > 0 ->
        # Retry with new ballot after random backoff
        new_op = %{op | operation: {:update, fun, opts, retries - 1}}
        state = %{state | pending_cas: Map.put(state.pending_cas, ref, new_op)}
        delay = :rand.uniform(50) + 10
        Process.send_after(self(), {:cas_retry, ref, op.key, {:update, fun, opts, retries - 1}}, delay)
        state

      _ ->
        GenServer.reply(op.from, {:error, :conflict})
        %{state | pending_cas: Map.delete(state.pending_cas, ref)}
    end
  end

  defp count_alive_node_ids(state) do
    if state.cluster_size do
      # Our own node_id + distinct peer node_ids
      peer_ids = state.peer_node_ids
        |> Map.values()
        |> Enum.reject(&is_nil/1)
        |> MapSet.new()

      MapSet.size(MapSet.put(peer_ids, state.node_id))
    else
      1
    end
  end

  defp fail_pending_cas_if_no_quorum(state) do
    if state.cluster_size == nil or map_size(state.pending_cas) == 0 do
      state
    else
      alive_count = count_alive_node_ids(state)

      {to_fail, to_keep} =
        Enum.split_with(state.pending_cas, fn {_ref, op} ->
          alive_count < op.quorum
        end)

      for {_ref, op} <- to_fail do
        cancel_timer(op.timer)
        GenServer.reply(op.from, {:error, :no_quorum})
      end

      %{state | pending_cas: Map.new(to_keep)}
    end
  end

  defp track_peer_node_id(state, _remote_node, nil), do: state

  defp track_peer_node_id(state, remote_node, remote_node_id) do
    %{state | peer_node_ids: Map.put(state.peer_node_ids, remote_node, remote_node_id)}
  end

  defp cancel_timer(nil), do: :ok
  defp cancel_timer(ref), do: Process.cancel_timer(ref)

  # =====================================================================
  # Subscriber dispatch helpers
  # =====================================================================

  defp has_subscribers?(state) do
    config = EKV.get_config(state.name)
    :atomics.get(config.sub_count, 1) > 0
  end

  defp dispatch_events(_state, []), do: :ok

  defp dispatch_events(state, events) do
    send(EKV.SubDispatcher.dispatcher_name(state.name, state.shard_index), {:dispatch, events})
    :ok
  end

  defp read_conn(state) do
    readers = :persistent_term.get({EKV, state.name, :readers, state.shard_index})
    sid = :erlang.system_info(:scheduler_id)
    elem(readers, rem(sid - 1, tuple_size(readers)))
  end

  defp read_previous_value(state, key) do
    {db, get_stmt} = read_conn(state)

    case Store.get_cached(db, get_stmt, key) do
      nil ->
        nil

      {_value_binary, _ts, _origin, _expires_at, deleted_at} when is_integer(deleted_at) ->
        nil

      {value_binary, _ts, _origin, _expires_at, _deleted_at} ->
        :erlang.binary_to_term(value_binary)
    end
  end

  # =====================================================================
  # Logging helpers
  # =====================================================================

  defp log(state, message_fn) when is_function(message_fn, 0) do
    case EKV.get_config(state.name) do
      %{log: false} -> :ok
      _ -> Logger.info(message_fn)
    end
  end

  defp log_verbose(state, message_fn) when is_function(message_fn, 0) do
    case EKV.get_config(state.name) do
      %{log: :verbose} -> Logger.info(message_fn)
      _ -> :ok
    end
  end

  defp log_once(state, message_fn) do
    if state.shard_index == 0, do: log(state, message_fn)
  end

  defp log_prefix(state) do
    "[EKV #{inspect(state.name)}]"
  end

  defp log_prefix_shard(state) do
    "[EKV #{inspect(state.name)}/#{state.shard_index}]"
  end
end
