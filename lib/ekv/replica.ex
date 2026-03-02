defmodule EKV.Replica do
  @moduledoc false

  _archdoc = ~S"""
  EKV — Eventually Consistent Durable KV Store
  =============================================

  EKV is a sharded, replicated key-value store where data outlives the node
  that created it. EKV entries survive node restarts, node death, and network
  partitions. Data is only removed by explicit delete or TTL expiry.

  The default consistency model is Last-Writer-Wins (LWW) — every node can
  read and write independently, and conflicts are resolved by timestamp. For
  keys that need stronger guarantees, an opt-in Compare-And-Swap (CAS) mode
  provides linearizable read-modify-write via CASPaxos consensus.

  Peer discovery is fully self-contained: EKV uses :net_kernel.monitor_nodes/1
  and Node.list/0 directly. It has no external topology dependency. Zero
  runtime deps. SQLite is vendored as a C NIF (c_src/sqlite3.c amalgamation).


  ## Supervision Tree

      EKV.Supervisor (rest_for_one)
      ├── EKV.SubTracker         atomics subscriber count + process monitors
      ├── Registry               keys: :duplicate, listeners: [sub_tracker]
      ├── EKV.SubDispatcher.Supervisor (one_for_one)
      │   ├── EKV.SubDispatcher 0   async event fan-out per shard
      │   ├── EKV.SubDispatcher 1
      │   └── ...
      ├── EKV.Replica.Supervisor (one_for_one)
      │   ├── EKV.Replica 0     shard GenServer (writes + replication + SQLite)
      │   ├── EKV.Replica 1
      │   └── ...               N shards (default 8)
      └── EKV.GC                periodic timer, sends :gc to each shard

  rest_for_one: SubTracker crash restarts everything. Registry crash
  restarts Dispatchers + Replicas. Single Replica crash → only that shard
  restarts. GC is downstream of Replicas.


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
      │   kv_meta     — liveness + ballot counter + down markers     │
      │   kv_paxos    — CAS consensus state per key (opt-in)         │
      │                                                              │
      │ Indexes:                                                     │
      │   idx_kv_deleted  — partial on deleted_at (WHERE NOT NULL)   │
      │   idx_kv_expires  — partial on expires_at (WHERE NOT NULL)   │
      │                                                              │
      │ - Single source of truth. Survives process/node crashes.     │
      │ - kv + oplog writes are atomic (BEGIN IMMEDIATE / COMMIT).   │
      └──────────────────────────────────────────────────────────────┘

  Connections per shard:
    - 1 writer connection (owned by the Replica GenServer)
    - System.schedulers_online() reader connections

  Reader connections are stored as a tuple of {db, get_stmt} in
  persistent_term keyed by {EKV, name, :readers, shard_index}. Reads
  pick a connection by rem(scheduler_id - 1, num_readers) — zero
  contention, no pool, no GenServer hop. WAL mode ensures readers
  don't block the writer.

  Values are stored as :erlang.term_to_binary/1 blobs. Encoding happens
  in the public EKV module; Replica and Store only see binaries.


  ## Custom NIF and Dirty IO Bounces

  EKV vendors sqlite3.c (amalgamation) and uses a custom NIF with no
  runtime deps. The main latency bottleneck is the dirty IO scheduler
  bounce (~1μs per NIF call), not SQL parsing.

  Combined NIF functions reduce bounces per operation:

      ┌──────────────────────────────────────────────────────────────┐
      │ NIF function          │ Bounces │ Operations                 │
      ├───────────────────────┼─────────┼────────────────────────────│
      │ write_entry           │    1    │ BEGIN + kv upsert (LWW) +  │
      │                       │         │ check changes + oplog +    │
      │                       │         │ COMMIT (or ROLLBACK)       │
      │ read_entry            │    1    │ reset + bind + step on     │
      │                       │         │ cached prepared stmt       │
      │ fetch_all             │    1    │ prepare + bind + step all  │
      │                       │         │ rows + finalize            │
      │ paxos_prepare         │    1    │ BEGIN + read/insert/update │
      │                       │         │ kv_paxos + fallback read   │
      │                       │         │ from kv + COMMIT           │
      │ paxos_accept          │    1    │ BEGIN + ballot check +     │
      │                       │         │ upsert kv_paxos + COMMIT   │
      │ paxos_promote         │    1    │ BEGIN + read kv_paxos +    │
      │                       │         │ ballot check + kv upsert + │
      │                       │         │ oplog insert + clear       │
      │                       │         │ kv_paxos + COMMIT          │
      └──────────────────────────────────────────────────────────────┘

  Without combined NIFs, a simple put would be 5 dirty bounces
  (begin, upsert, check, oplog, commit). write_entry does it in 1.

  Cached prepared statements:
    - Writer: kv_upsert (LWW), kv_force_upsert (no LWW), oplog_insert
    - Readers: get statement (per reader connection)

  All IO-touching NIFs use ERL_NIF_DIRTY_JOB_IO_BOUND. The bind NIF
  runs on a normal scheduler (no IO).


  ## Sharding

  Shard assignment: :erlang.phash2(key, num_shards)

  Each shard is a completely independent GenServer with its own SQLite db
  file. Shards on different nodes with the same index are counterparts —
  they replicate to each other and sync on connect.

  Prefix scans (scan/keys) cannot be routed to a single shard because
  the prefix doesn't determine the hash. They fan out to all shards.

  The shard count is immutable — persisted in kv_meta on first open.
  Changing it raises ArgumentError. Peer connections from nodes with
  mismatched shard counts are rejected (logged error, no crash).


  ## Write Path (LWW)

      Client                  Replica (shard i)             Peers
        │                          │                          │
        │ GenServer.call           │                          │
        │  {:put, key,             │                          │
        │   value_binary, opts}    │                          │
        │─────────────────────────>│                          │
        │                          │                          │
        │                    write_entry NIF (1 dirty bounce): │
        │                      BEGIN IMMEDIATE                │
        │                      kv upsert (LWW WHERE clause)  │
        │                      sqlite3_changes() == 0?        │
        │                        → ROLLBACK (LWW lost)        │
        │                        → oplog INSERT + COMMIT      │
        │                          │                          │
        │                          │ {:ekv_put, key,          │
        │                          │  value_binary, ts,       │
        │                          │  origin_node, expires_at}│
        │                          │─────────────────────────>│
        │                          │  (fire-and-forget to     │
        │                          │   each known peer)       │
        │                          │                          │
        │<─────────────────────────│                          │
        │         :ok              │                          │

  Delete is identical but sets deleted_at = now and value = nil.
  Broadcast message: {:ekv_delete, key, ts, origin_node}.

  On the receiving side, merge_remote_entry calls the same write_entry
  NIF with the remote's timestamp — LWW decides whether to apply.


  ## Read Path

  Reads bypass the GenServer entirely:

      Client             SQLite (per-scheduler read connection)
        │                 │
        │ read_entry NIF  │
        │────────────────>│   via read_conn(name, shard)
        │<────────────────│   1 dirty bounce: reset+bind+step
        │                 │
        │  check deleted_at, expires_at
        │  binary_to_term if live
        │
        │  return value | nil

  No serialization, no message passing. The read_entry NIF reuses a
  cached prepared statement — reset+bind+step in a single dirty bounce.


  ## Conflict Resolution: Last-Writer-Wins (LWW)

  Every entry carries a nanosecond timestamp and origin_node atom.

  LWW is pushed into SQL via ON CONFLICT ... WHERE:

      INSERT INTO kv (...) VALUES (...)
      ON CONFLICT(key) DO UPDATE SET ...
      WHERE excluded.timestamp > kv.timestamp
        OR (excluded.timestamp = kv.timestamp
            AND excluded.origin_node > kv.origin_node)

  After the upsert, sqlite3_changes() == 0 means LWW lost — the
  transaction is rolled back and no oplog entry is written.

  Equivalent logic:

      lww_wins?(incoming_ts, incoming_origin, existing_ts, existing_origin)
        incoming_ts > existing_ts
        OR (incoming_ts == existing_ts AND incoming_origin > existing_origin)

  Used in ALL write paths:
    - Local put/delete (timestamp is always "now", so almost always wins)
    - Remote replication receive (ekv_put / ekv_delete)
    - Bulk sync (ekv_sync entries)
    - GC TTL expiry (converting expired entry to tombstone)

  The tiebreaker (origin_node atom comparison) is deterministic across
  all nodes. Atom ordering matches SQLite TEXT ordering for node names
  (both lexicographic ASCII).

  A delete is just an entry with deleted_at set. Same LWW applies — a
  put with a higher timestamp beats a delete, and vice versa.


  ## Compare-And-Swap (CAS) via CASPaxos

  EKV is eventually consistent by default. For keys that need atomic
  read-modify-write, CAS provides per-key linearizability via a
  simplified CASPaxos protocol. CAS is opt-in: requires cluster_size
  and node_id config. Both LWW and CAS writes coexist — different keys
  can use different consistency models in the same EKV instance.

  ### Why CASPaxos?

  Classic Paxos replicates a log. CASPaxos is simpler — it's a
  single-decree consensus protocol for values, not logs. Each key is
  an independent consensus instance. The protocol is:

      1. Prepare (read phase): learn the current value + get promises
      2. Accept (write phase): propose a new value to acceptors
      3. Commit: write to kv + oplog, broadcast to all peers

  No log replication, no leader election, no view changes. Any node
  can be a proposer for any key at any time.

  ### Ballot Numbers

  Each CAS operation gets a unique ballot {counter, node_id}. Counters
  are monotonically increasing per-shard (max(system_time_ns, prev+1))
  and persisted in kv_meta to survive restarts. The {counter, node_id}
  tuple is compared lexicographically — higher counter wins, ties broken
  by node_id.

  ### SQLite Table: kv_paxos

      key TEXT PRIMARY KEY
      promised_counter INTEGER    — highest ballot promised
      promised_node INTEGER
      accepted_counter INTEGER    — highest ballot accepted
      accepted_node INTEGER
      accepted_value BLOB         — tentative value (not yet committed)
      accepted_timestamp INTEGER
      accepted_origin TEXT
      accepted_expires_at INTEGER
      accepted_deleted_at INTEGER

  kv_paxos is separate from kv. Values only move to kv after commit
  (via paxos_promote). This prevents phantom reads — a CAS value is
  invisible to get/scan until consensus is reached.

  ### CAS Write Path (3-phase)

      Proposer (shard i)          Acceptor 1            Acceptor 2
        │                           │                      │
        │ ── Phase 1: PREPARE ──    │                      │
        │                           │                      │
        │ local paxos_prepare NIF   │                      │
        │ (kv_paxos: promise +      │                      │
        │  return accepted value)   │                      │
        │                           │                      │
        │ {:ekv_prepare, ref, pid,  │                      │
        │  key, ballot_c, ballot_n} │                      │
        │──────────────────────────>│                      │
        │──────────────────────────────────────────────────>│
        │                           │                      │
        │ {:ekv_promise, ref, ...   │                      │
        │  acc_c, acc_n, kv_row}    │                      │
        │<──────────────────────────│                      │
        │<──────────────────────────────────────────────────│
        │                           │                      │
        │  quorum promises reached  │                      │
        │  pick highest accepted    │                      │
        │  value → apply operation  │                      │
        │  (compare-and-swap check) │                      │
        │                           │                      │
        │ ── Phase 2: ACCEPT ───    │                      │
        │                           │                      │
        │ {:ekv_accept, ref, pid,   │                      │
        │  key, ballot, entry}      │                      │
        │──────────────────────────>│                      │
        │──────────────────────────────────────────────────>│
        │                           │                      │
        │  acceptors write to       │                      │
        │  kv_paxos ONLY (not kv)   │                      │
        │                           │                      │
        │ {:ekv_accepted, ref, ...} │                      │
        │<──────────────────────────│                      │
        │<──────────────────────────────────────────────────│
        │                           │                      │
        │  quorum accepts reached   │                      │
        │                           │                      │
        │ ── Phase 3: COMMIT ───    │                      │
        │                           │                      │
        │  local: paxos_accept +    │                      │
        │    write_entry (kv+oplog) │                      │
        │    clear kv_paxos accepted│                      │
        │                           │                      │
        │ {:ekv_cas_committed, ...} │  (to acceptors)      │
        │──────────────────────────>│                      │
        │──────────────────────────────────────────────────>│
        │                           │                      │
        │ {:ekv_put, ...}           │  (LWW to non-        │
        │──────────────────────────>│   acceptors)         │
        │                           │                      │
        │  acceptors: paxos_promote │                      │
        │  (kv_paxos → kv + oplog)  │                      │

  The proposer defers its own local accept until after remote quorum.
  This prevents phantom reads — if the proposer crashes before quorum,
  no value was written to kv locally.

  ### Phantom Prevention

  Key invariant: accepted values in kv_paxos are INVISIBLE to reads.

    - paxos_accept writes to kv_paxos only (not kv, not oplog)
    - No subscriber events on accept
    - get/scan/keys read from kv table only
    - Value appears in kv only after paxos_promote (commit phase)

  If the proposer crashes between accept and commit:
    - kv_paxos has the tentative value
    - kv does NOT have it → no phantom read
    - Next CAS on same key: paxos_prepare reads from kv_paxos, recovers
      the tentative value, and either re-proposes or overwrites it

  ### Commit Dissemination

  After commit, the proposer sends two types of messages:

    - {:ekv_cas_committed, key, ballot_c, ballot_n, shard}
      → sent to nodes that participated as acceptors (have kv_paxos state)
      → receiver calls paxos_promote: reads from kv_paxos, writes to
        kv + oplog, clears kv_paxos accepted columns

    - {:ekv_put, key, value, ts, origin, expires_at}
      → sent to nodes that were NOT acceptors (no kv_paxos state)
      → receiver applies via normal LWW merge (idempotent)

  This split avoids sending the value twice to acceptors (they already
  have it in kv_paxos) while ensuring non-acceptors still get the data.

  ### paxos_promote (commit on acceptor side)

  Single NIF dirty bounce:
    1. BEGIN IMMEDIATE
    2. Read kv_paxos for the key
    3. Verify ballot matches (if stale → return {:ok, :stale})
    4. Read previous value from kv (for subscriber events)
    5. Force-upsert to kv (no LWW — Paxos ballots determine ordering)
    6. Insert to oplog
    7. Clear kv_paxos accepted columns
    8. COMMIT

  Clearing accepted columns means future paxos_prepare reads fall back
  to kv (committed state). Also allows GC to purge the kv_paxos row
  when the kv entry itself is tombstone-purged.

  ### Quorum and Failure Handling

  Quorum: floor(cluster_size / 2) + 1

  Before starting CAS, the proposer checks that enough node_ids are
  reachable. count_alive_node_ids tracks distinct node_ids (not Erlang
  nodes — multiple Erlang nodes may share a node_id in blue-green).

  Failure modes:
    - Nack in prepare: ballot too low. If can't reach quorum → fail.
    - Nack in accept: ballot superseded. If can't reach quorum → fail.
    - Timeout (5s): no response from enough peers → {:error, :quorum_timeout}
    - Node death during CAS: fail_pending_cas_if_no_quorum checks all
      pending ops and fails those that can no longer reach quorum.

  For EKV.update (read-modify-write), failures auto-retry up to 5 times
  with random backoff (10-60ms). Each retry generates a new ballot.

  ### CAS + LWW Interaction

  CAS and LWW share the same kv/oplog storage and replication paths, but
  ordering models are different (ballot order for CAS vs timestamp order for
  LWW). The supported model is key-level ownership:

    - Different keys may use different modes in the same EKV instance.
    - A key managed via CAS should continue to use CAS write APIs.
      Reads may still choose eventual or consistent paths based on needs.
    - Mixed-mode writes on the same key (eventual + CAS) are unsupported.
    - Sync sends entries from kv (committed state). kv_paxos tentative
      values are not included in sync — they only exist locally until
      committed.

  CAS operations are serialized per-shard through the GenServer. Two
  concurrent CAS operations on the same key from different nodes will
  compete via ballot ordering — the higher ballot wins. update() with
  auto-retry handles this transparently.


  ## Peer Discovery and Tracking

  EKV manages its own peer mesh independently:

      init/1:
        :net_kernel.monitor_nodes(true)
        for node <- Node.list(), send ekv_peer_connect

      nodeup:
        attempt ekv_peer_connect (gated by long-partition quarantine)

      nodedown:
        remove from remote_shards + peer_node_ids
        persist peer down marker (node_id key if known, fallback name key)
        fail any pending CAS ops that lost quorum

      DOWN (monitored remote shard pid):
        remove from remote_shards + peer_node_ids
        persist peer down marker (node_id key if known, fallback name key)
        fail any pending CAS ops that lost quorum

  remote_shards :: %{node() => pid()} tracks confirmed live counterpart
  shard processes. A node enters this map only after a successful
  peer_connect / peer_connect_ack handshake where its pid is monitored.

  peer_node_ids :: %{node() => string()} maps Erlang nodes to their
  configured node_id. Used for CAS quorum counting (distinct node_ids,
  not Erlang nodes, determine quorum).

  Long-partition tracking is persisted in kv_meta:
    - peer_down_at:id:<node_id>    (preferred, stable identity)
    - peer_down_at:name:<node>     (fallback when node_id unknown)

  On reconnect handshake, if downtime > tombstone_ttl and policy is
  :quarantine, replication is blocked for that peer. Down markers are
  cleared only after a successful non-quarantined reconnect.


  ## Peer Sync Protocol

  When two nodes discover each other (init, nodeup), they exchange a
  handshake per shard. The handshake determines whether to send a delta
  (oplog slice) or a full state snapshot.

      Node A (shard i)                        Node B (shard i)
        │                                         │
        │  {:ekv_peer_connect,                    │
        │   pid_a, i, num_shards, hwm_a, nid_a}  │
        │────────────────────────────────────────>│
        │                                         │
        │                          validate num_shards match
        │                          monitor pid_a
        │                          add A to remote_shards
        │                          track A's node_id
        │                                         │
        │  {:ekv_peer_connect_ack,                │
        │   pid_b, i, num_shards, hwm_b, nid_b}  │
        │<────────────────────────────────────────│
        │                                         │
        │                          send_sync_data(A, hwm_a):
        │                            if hwm_a exists & oplog not truncated:
        │                              delta sync (chunked)
        │                            else:
        │                              full sync (chunked)
        │                                         │
        │  {:ekv_sync, node_b, i, chunk, seq}     │
        │<────────────────────────────────────────│
        │  {:ekv_sync, node_b, i, chunk, seq}     │
        │<────────────────────────────────────────│
        │  ...                                    │
        │                                         │
        │  (A does the same for B)                │
        │────────────────────────────────────────>│
        │  {:ekv_sync, node_a, i, chunk, seq}     │
        │                                         │

  Both sides send data. This is symmetric — each side sends what the
  other is missing based on HWMs.


  ## Delta Sync vs Full Sync

      ┌─────────────────────────────────────────────────────────────┐
      │ Delta Sync                                                  │
      │ Condition: remote_hwm is within [local_min_seq, local_max_seq] │
      │                                                             │
      │ Query: SELECT * FROM kv_oplog WHERE seq > peer_hwm          │
      │        ORDER BY seq LIMIT chunk_size                        │
      │                                                             │
      │ Sends only mutations since the last sync. Efficient for     │
      │ brief disconnects where the oplog hasn't been truncated.    │
      └─────────────────────────────────────────────────────────────┘

      ┌─────────────────────────────────────────────────────────────┐
      │ Full Sync                                                   │
      │ Condition: no HWM for this peer, OR oplog truncated past    │
      │            the peer's HWM (min_seq > peer_hwm), OR          │
      │            peer_hwm > local_max_seq                         │
      │                                                             │
      │ Query: SELECT * FROM kv WHERE (deleted_at IS NULL           │
      │          OR deleted_at > cutoff) AND key > cursor            │
      │        ORDER BY key LIMIT chunk_size                        │
      │                                                             │
      │ Sends all live entries + recent tombstones (so the peer     │
      │ learns about deletes that happened while it was away).      │
      │ Used for first contact and after long partitions.           │
      └─────────────────────────────────────────────────────────────┘

  After receiving sync data, the receiver applies each entry through
  merge_remote_entry (LWW check), then records the sender's advertised
  max_seq as the new inbound HWM for that peer.


  ## Chunked Sync

  Both full and delta sync use cursor-based pagination to avoid loading
  the entire dataset into memory. Default chunk size: 500 entries
  (configurable via :sync_chunk_size).

  The sender sends one chunk, then yields to other messages via
  send(self(), {:continue_full_sync, ...}) before sending the next.
  This prevents the shard GenServer from blocking — CAS messages,
  regular writes, and other sync operations can interleave between
  chunks.

      Sender (shard i)
        │
        │ send_full_chunk(cursor=nil)
        │   query chunk 1 (500 entries)
        │   send {:ekv_sync, entries, seq=0}  ──> peer
        │   send(self(), {:continue_full_sync, cursor="last_key"})
        │   return {:noreply, state}
        │
        │ ... process other messages ...
        │
        │ handle_info(:continue_full_sync)
        │   check peer still in remote_shards (abort if gone)
        │   query chunk 2 (500 entries)
        │   send {:ekv_sync, entries, seq=0}  ──> peer
        │   send(self(), {:continue_full_sync, cursor="last_key"})
        │
        │ ... process other messages ...
        │
        │ handle_info(:continue_full_sync)
        │   query chunk 3 (< 500 entries = final)
        │   send {:ekv_sync, entries, seq=my_seq}  ──> peer
        │

  HWM safety: intermediate chunks send seq=0 in the sync message.
  Only the final chunk carries the real my_seq. The receiver updates
  inbound HWM from that final seq; MAX(current, new) makes seq=0
  harmless and prevents regression on out-of-order arrival.

  Continuation handlers check remote_shards before each chunk. If the
  peer disconnected mid-sync, the continuation silently stops.

  Safety under concurrent activity:
    - Write between chunks: LWW is idempotent. Duplicates resolved.
    - CAS between chunks: independent tables (kv_paxos vs kv).
    - GC between chunks: cursor-based, skips purged entries.
    - Second sync triggered: LWW + MAX(hwm) make double-sends safe.


  ## High-Water Marks (HWM)

  Each shard's SQLite db has a kv_peer_hwm table:

      peer_node TEXT PRIMARY KEY  →  last_seq INTEGER

  This records: "the last oplog seq from peer X that I have applied."
  During handshake, each side advertises this inbound HWM so the sender
  can delta-sync from the receiver's cursor in sender sequence space.

  HWMs are updated in one place:
    1. In ekv_sync handler: after applying received data, record the
       sender's seq from the sync message.

  HWM monotonicity: set_hwm uses MAX(current, new) in SQL so HWMs
  never regress, even if messages arrive out of order.


  ## Recovery Scenarios

  ### Scenario 1: Clean restart (same node, same data dir)

      Node crashes / restarts
        │
        Replica.init:
          Store.open  →  SQLite db still on disk
          open read connections
          restore ballot_counter from kv_meta
          monitor_nodes + send ekv_peer_connect
        │
        Peer responds with ekv_peer_connect_ack
          delta sync catches up missed mutations
        │
        Fully operational

  Data survives because SQLite is durable. The oplog enables efficient
  delta sync for the mutations missed while the node was down.

  CAS state (kv_paxos) also survives restart. A commit notification
  received after restart will successfully promote values that were
  accepted before the crash.

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
            (chunked, ~500 entries per message)
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
        - CAS operations on C fail with {:error, :no_quorum}
          (cannot reach majority)
        - CAS operations on A/B succeed if A+B form a majority

      Heal:
        - nodeup fires on both sides
        - ekv_peer_connect / ekv_peer_connect_ack exchanged
        - Reconnect gate checks persisted down marker age:
            * age <= tombstone_ttl: proceed to sync
            * age > tombstone_ttl: quarantine peer pair (no sync)
        - If sync proceeds:
            * send_sync_data: if HWMs still valid → delta sync (chunked)
                             if oplog truncated  → full sync (chunked)
            * Both sides send their missed mutations
            * LWW resolves any conflicts deterministically:
                - Disjoint keys: union of both sides
                - Same key both sides: higher timestamp wins
                - Put vs delete: whichever has higher timestamp wins
            * CAS-written keys: already committed to kv via normal sync.
              kv_paxos state is local only — sync uses kv (committed values).


  ## Subscription System

  Subscribers receive {:ekv, [%EKV.Event{}], %{name: name}} messages
  for keys matching a prefix. Fan-out is async — moved off the shard
  write path into per-shard SubDispatcher processes.

  On write, the shard does at most:
    1. atomics read of sub_count (1 cell)
    2. send({dispatcher, {:dispatch, events}}) if sub_count > 0

  SubDispatcher does prefix decomposition lookup via Registry:
    key "a/b/c" → lookup prefixes "", "a/", "a/b/", "a/b/c"
    O(slash_count) ETS hash lookups, not O(N) subscriber scan.

  Subscription matching is at "/" boundaries only:
    - "foo/" matches "foo/bar", "foo/baz/qux"
    - "foo" matches exactly "foo" (no trailing slash = exact key)
    - "" matches all keys

  Events are dispatched for:
    - Local put/delete (LWW writes)
    - Remote put/delete (replication receives)
    - Sync entries (bulk, with batched events)
    - CAS commit (paxos_promote, with previous value for deletes)
    - GC TTL expiry (delete events with previous value)

  Events are NOT dispatched for:
    - CAS accept (kv_paxos write only — no phantom events)
    - Tombstone purge (already notified on original delete)
    - LWW-rejected writes (no state change)


  ## TTL (Time-To-Live)

      EKV.put(name, key, val, ttl: 30_000)
        → expires_at = System.system_time(:nanosecond) + ttl * 1_000_000

  expires_at is absolute nanoseconds, stored in SQLite and included
  in all replication messages. CAS operations also support :ttl.

  Read path: EKV.get checks expires_at lazily — returns nil if past.

  GC converts expired entries into tombstones:
    1. Find entries where expires_at < now AND deleted_at IS NULL
    2. Set deleted_at = now, append to oplog (via write_entry NIF)
    3. Broadcast {:ekv_delete, ...} so peers tombstone it too


  ## Garbage Collection (EKV.GC)

  Periodic timer sends {:gc, now, tombstone_cutoff} to each shard.
  The shard handles GC inside its own process (serialized with writes).

  Each tick, six phases:

      Phase 1: Expire TTL entries
        expired entries → set deleted_at, write oplog, broadcast delete
        (converts live-but-expired entries into proper tombstones)

      Phase 2: Purge old tombstones
        deleted_at < now - tombstone_ttl → hard delete from SQLite kv
        (tombstone_ttl default: 7 days — keeps tombstones long enough
         for partitioned nodes to learn about deletes on reconnect)

      Phase 2b: Purge orphan kv_paxos rows (if CAS enabled)
        DELETE FROM kv_paxos WHERE key NOT IN (SELECT key FROM kv)
          AND accepted_counter = 0
        (cleans up paxos state for tombstone-purged keys, only if
         no in-flight accept is pending)

      Phase 3: Prune stale peer HWMs
        Remove kv_peer_hwm rows for peers not currently connected.
        Prevents dead/decommissioned peers from anchoring the oplog
        forever. Disconnected peers get full sync on reconnect.

      Phase 4: Truncate oplog
        DELETE FROM kv_oplog WHERE seq < MIN(all peer HWMs)
        (keeps oplog bounded; entries below the slowest connected
         peer's HWM are no longer needed for delta sync)

      Phase 5: Bump liveness
        touch_last_active updates kv_meta.last_active_at.
        Used by stale DB detection on restart.

      Phase 6: Prune fallback name-based down markers
        Delete old / over-cap kv_meta keys:
          peer_down_at:name:*
        (bounds marker growth under node-name churn; primary id-based
         markers remain until successful reconnect or operator action)


  ## Stale DB Protection

  On Store.open, if an existing db file's last_active_at is older than
  tombstone_ttl - gc_interval, the db is wiped (deleted and recreated).
  This prevents zombie resurrection: peers will have GC'd tombstones for
  entries deleted while the node was away, so a stale db would never learn
  about them. After wipe, full sync from peers rebuilds from scratch.


  ## GenServer State

      %EKV.Replica{
        name:           atom,           # EKV instance name
        shard_index:    integer,        # 0..num_shards-1
        num_shards:     integer,
        db:             reference,      # SQLite writer connection (NIF resource)
        data_dir:       string,
        stmts:          %{              # cached prepared statements
          kv_upsert:       reference,   #   LWW upsert
          kv_force_upsert: reference,   #   unconditional upsert (CAS commit)
          oplog_insert:    reference    #   oplog append
        },
        readers:        [{db, stmt}],   # per-scheduler read connections
        tombstone_ttl:  integer,        # ms
        partition_ttl_policy: :quarantine | :ignore,
        remote_shards:  %{node => pid}, # confirmed live peer shards
        peer_down_at:   %{marker_key => down_since_ms}, # cached kv_meta markers
        quarantined_peers: MapSet.t(node()),            # blocked peers
        # CAS fields (nil if CAS not configured):
        node_id:        string | nil,   # this node's CAS identity
        cluster_size:   integer | nil,  # total CAS participants
        ballot_counter: integer,        # monotonic, persisted in kv_meta
        peer_node_ids:  %{node => string},  # Erlang node → CAS node_id
        pending_cas:    %{ref => op}    # in-flight CAS operations
      }


  ## Message Reference

  Steady-state LWW replication (per-operation, fire-and-forget):
    {:ekv_put, key, value_binary, timestamp, origin_node, expires_at}
    {:ekv_delete, key, timestamp, origin_node}

  Peer handshake (on nodeup / init):
    {:ekv_peer_connect, pid, shard_index, num_shards, my_hwm_for_remote, node_id}
    {:ekv_peer_connect_ack, pid, shard_index, num_shards, my_hwm_for_remote, node_id}

  Chunked sync (after handshake, multiple messages per sync):
    {:ekv_sync, from_node, shard_index, entries, sender_seq}
      entries: [{key, value_binary, timestamp, origin_node,
                 expires_at, deleted_at}]
      sender_seq: 0 for intermediate chunks, real seq for final chunk

  Sync continuations (self-messages for chunking):
    {:continue_full_sync, node, last_key, cutoff, my_seq, chunk_size}
    {:continue_delta_sync, node, last_seq, my_seq, chunk_size}

  CAS proposer → acceptors:
    {:ekv_prepare, ref, proposer_pid, key, ballot_c, ballot_n, shard}
    {:ekv_accept, ref, proposer_pid, key, ballot_c, ballot_n, entry_tuple, shard}
    {:ekv_cas_committed, key, ballot_c, ballot_n, shard}

  CAS acceptors → proposer:
    {:ekv_promise, ref, pid, node_id, acc_c, acc_n, kv_row}
    {:ekv_nack, ref, pid, node_id, promised_c, promised_n}
    {:ekv_accepted, ref, pid, node_id}
    {:ekv_accept_nack, ref, pid, node_id}

  CAS internal (self-messages):
    {:cas_timeout, ref}
    {:cas_retry, ref, key, operation}

  GC (from EKV.GC timer):
    {:gc, now_nanoseconds, tombstone_cutoff_nanoseconds}

  Subscriber dispatch (shard → SubDispatcher):
    {:dispatch, [%EKV.Event{}]}

  All peer messages are sent to the counterpart shard by registered name:
    send({:"#{name}_ekv_replica_#{shard}", target_node}, message)

  There is no gossip or re-broadcast. Replication is direct: the node
  that performs a write sends to all known peers exactly once.
  """

  use GenServer

  require Logger

  alias EKV.Store

  @peer_down_id_prefix "peer_down_at:id:"
  @peer_down_name_prefix "peer_down_at:name:"
  @peer_down_name_min_retention_ms :timer.hours(24 * 30)
  @peer_down_name_max_entries 4096

  defstruct [
    :name,
    :shard_index,
    :num_shards,
    :db,
    :data_dir,
    :stmts,
    :tombstone_ttl,
    partition_ttl_policy: :quarantine,
    readers: [],
    remote_shards: %{},
    # Marker key -> down_since_ms (wall clock cache for kv_meta)
    peer_down_at: %{},
    quarantined_peers: MapSet.new(),
    # CAS fields
    node_id: nil,
    cluster_size: nil,
    ballot_counter: 0,
    peer_node_ids: %{},
    pending_cas: %{},
    handoff_node: nil
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
      Store.open(data_dir, shard_index, config.tombstone_ttl, num_shards, config.gc_interval,
        skip_stale_check: config[:skip_stale_check] || false
      )

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

    # Persist node_id to volume (idempotent — all shards store it)
    if config.node_id, do: Store.persist_node_id(db, config.node_id)

    state = %__MODULE__{
      name: name,
      shard_index: shard_index,
      num_shards: num_shards,
      db: db,
      data_dir: data_dir,
      stmts: stmts,
      readers: readers,
      tombstone_ttl: config.tombstone_ttl,
      partition_ttl_policy: config.partition_ttl_policy,
      node_id: config.node_id,
      cluster_size: config.cluster_size,
      ballot_counter: ballot_counter
    }

    :net_kernel.monitor_nodes(true)

    log_once(state, fn -> "#{log_prefix(state)} started (shards=#{num_shards})" end)

    # Discover peers on all known nodes
    registered_name = shard_name(name, shard_index)

    for remote_node <- Node.list() do
      remote_hwm = Store.get_hwm(db, remote_node) || 0

      send(
        {registered_name, remote_node},
        {:ekv_peer_connect, self(), shard_index, num_shards, remote_hwm, config.node_id}
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
  # Blue-green handoff proxy
  # =====================================================================

  @impl true
  def handle_call(request, _from, %{handoff_node: handoff_node} = state)
      when handoff_node != nil do
    shard_name = shard_name(state.name, state.shard_index)

    try do
      result = GenServer.call({shard_name, handoff_node}, request, 5_000)
      {:reply, result, state}
    catch
      :exit, _ ->
        {:reply, {:error, :shutting_down}, state}
    end
  end

  # =====================================================================
  # Write calls
  # =====================================================================

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
    {:noreply, start_cas(state, key, operation, from)}
  end

  def handle_call({:cas_delete, key, expected_vsn}, from, state) do
    operation = {:cas_delete, expected_vsn}
    {:noreply, start_cas(state, key, operation, from)}
  end

  def handle_call({:update, key, fun, opts}, from, state) do
    retries = Keyword.get(opts, :retries, 5)
    operation = {:update, fun, opts, retries}
    {:noreply, start_cas(state, key, operation, from)}
  end

  def handle_call({:cas_read, key, opts}, from, state) do
    retries = Keyword.get(opts, :retries, 5)
    backoff = Keyword.get(opts, :backoff, {10, 60})
    operation = {:cas_read, [backoff: backoff], retries}
    {:noreply, start_cas(state, key, operation, from)}
  end

  # =====================================================================
  # Blue-green handoff
  # =====================================================================

  @impl true
  def handle_info({:ekv_handoff_request, ref, new_node, caller_pid}, state) do
    # 1. Drain pending CAS ops
    for {_ref, op} <- state.pending_cas do
      cancel_timer(op.timer)
      GenServer.reply(op.from, {:error, :shutting_down})
    end

    # 2. Persist ballot counter
    if state.cluster_size && state.db do
      Store.set_meta(state.db, "ballot_counter", state.ballot_counter)
    end

    # 3. WAL checkpoint — flush to main db
    if state.db, do: EKV.Sqlite3.execute(state.db, "PRAGMA wal_checkpoint(TRUNCATE)")

    # 4. Release stmts + close writer
    if state.stmts, do: Store.release_stmts(state.db, state.stmts)
    if state.db, do: Store.close(state.db)

    # 5. Ack
    send(caller_pid, {:ekv_handoff_ack, ref})

    log(state, fn ->
      "#{log_prefix_shard(state)} handoff to #{new_node} complete, proxy mode"
    end)

    # 6. Enter proxy mode (readers stay alive until terminate)
    {:noreply, %{state | db: nil, stmts: nil, pending_cas: %{}, handoff_node: new_node}}
  end

  # In handoff mode: drop all messages (replication, GC, nodeup/down, CAS)
  # Readers stay alive for old VM reads. Peers discover new node via nodeup.
  def handle_info(_msg, %{handoff_node: handoff_node} = state)
      when handoff_node != nil do
    {:noreply, state}
  end

  # =====================================================================
  # Replication receive
  # =====================================================================

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

  def handle_info(
        {:ekv_peer_connect, remote_pid, remote_shard, remote_num_shards, remote_hwm,
         remote_node_id},
        state
      ) do
    {:noreply,
     do_peer_connect(
       state,
       remote_pid,
       remote_shard,
       remote_num_shards,
       remote_hwm,
       remote_node_id
     )}
  end

  def handle_info(
        {:ekv_peer_connect_ack, remote_pid, remote_shard, remote_num_shards, remote_hwm,
         remote_node_id},
        state
      ) do
    {:noreply,
     do_peer_connect_ack(
       state,
       remote_pid,
       remote_shard,
       remote_num_shards,
       remote_hwm,
       remote_node_id
     )}
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
    case maybe_allow_peer_reconnect(state, remote_node) do
      {:quarantine, state} ->
        {:noreply, state}

      {:ok, state} ->
        %{shard_index: shard, name: name, db: db} = state
        remote_hwm = Store.get_hwm(db, remote_node) || 0

        send(
          {shard_name(name, shard), remote_node},
          {:ekv_peer_connect, self(), shard, state.num_shards, remote_hwm, state.node_id}
        )

        {:noreply, state}
    end
  end

  def handle_info({:nodedown, dead_node}, state) do
    log_once(state, fn -> "#{log_prefix(state)} nodedown #{dead_node} (data preserved)" end)

    dead_node_id = Map.get(state.peer_node_ids, dead_node)

    state = %{
      state
      | remote_shards: Map.delete(state.remote_shards, dead_node),
        peer_node_ids: Map.delete(state.peer_node_ids, dead_node)
    }

    state = mark_peer_down(state, dead_node, dead_node_id)

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

      remote_node_id = Map.get(state.peer_node_ids, remote_node)

      state = %{
        state
        | remote_shards: Map.delete(state.remote_shards, remote_node),
          peer_node_ids: Map.delete(state.peer_node_ids, remote_node)
      }

      state = mark_peer_down(state, remote_node, remote_node_id)
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

  def handle_info(
        {:ekv_accept, ref, proposer_pid, key, ballot_c, ballot_n, entry_tuple, _shard},
        state
      ) do
    %{db: db} = state

    {_key, value_binary, timestamp, origin_node_str, expires_at, deleted_at} = entry_tuple
    value_args = [value_binary, timestamp, origin_node_str, expires_at, deleted_at]

    # Write to kv_paxos only — no kv write, no oplog, no events.
    # The proposer will send {:ekv_cas_committed} after quorum, which triggers promote.
    case Store.paxos_accept(db, key, ballot_c, ballot_n, value_args) do
      {:ok, true} ->
        send(proposer_pid, {:ekv_accepted, ref, self(), state.node_id})

      {:ok, false} ->
        send(proposer_pid, {:ekv_accept_nack, ref, self(), state.node_id})
    end

    {:noreply, state}
  end

  # CAS commit notification — acceptor promotes kv_paxos → kv + oplog
  def handle_info({:ekv_cas_committed, key, ballot_c, ballot_n, _shard}, state) do
    %{db: db, stmts: stmts} = state

    case Store.paxos_promote(
           db,
           stmts.kv_force_upsert,
           stmts.oplog_insert,
           key,
           ballot_c,
           ballot_n
         ) do
      {:ok, value_binary, _ts, _origin, _expires, deleted_at, prev_value_binary} ->
        if deleted_at != nil and deleted_at != nil do
          prev =
            if prev_value_binary != nil and prev_value_binary != nil,
              do: :erlang.binary_to_term(prev_value_binary)

          dispatch_events(state, [%EKV.Event{type: :delete, key: key, value: prev}])
        else
          dispatch_events(state, [
            %EKV.Event{type: :put, key: key, value: :erlang.binary_to_term(value_binary)}
          ])
        end

      {:ok, :stale} ->
        :ok
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
          op = %{
            op
            | promises: [{remote_node_id, acc_c, acc_n, kv_row} | op.promises],
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
          op = %{op | nacks: op.nacks + 1, responded: MapSet.put(op.responded, remote_node_id)}

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

          if MapSet.size(accepts) + 1 >= op.quorum do
            # Remote quorum reached — do local accept and commit
            {:noreply, commit_cas(ref, op, state)}
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
          op = %{
            op
            | accept_nacks: op.accept_nacks + 1,
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
        {:noreply, start_cas(state, key, operation, old_op.from)}
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

    # 6. Bounded cleanup for fallback name-based down markers.
    prune_stale_peer_down_name_markers(state)

    {:noreply, state}
  end

  # =====================================================================
  # Chunked sync continuations
  # =====================================================================

  # Backward-compat continuation shape (includes obsolete remote_hwm field).
  # Keep handling this shape so tests/older internal senders still work.
  def handle_info(
        {:continue_full_sync, remote_node, last_key, tombstone_cutoff, my_seq, _remote_hwm,
         chunk_size},
        state
      ) do
    handle_info(
      {:continue_full_sync, remote_node, last_key, tombstone_cutoff, my_seq, chunk_size},
      state
    )
  end

  def handle_info(
        {:continue_full_sync, remote_node, last_key, tombstone_cutoff, my_seq, chunk_size},
        state
      ) do
    if Map.has_key?(state.remote_shards, remote_node) do
      send_full_chunk(
        state,
        remote_node,
        last_key,
        tombstone_cutoff,
        my_seq,
        chunk_size
      )
    end

    {:noreply, state}
  end

  # Backward-compat continuation shape (includes obsolete remote_hwm field).
  def handle_info(
        {:continue_delta_sync, remote_node, last_seq, my_seq, _remote_hwm, chunk_size},
        state
      ) do
    handle_info({:continue_delta_sync, remote_node, last_seq, my_seq, chunk_size}, state)
  end

  def handle_info(
        {:continue_delta_sync, remote_node, last_seq, my_seq, chunk_size},
        state
      ) do
    if Map.has_key?(state.remote_shards, remote_node) do
      send_delta_chunk(state, remote_node, last_seq, my_seq, chunk_size)
    end

    {:noreply, state}
  end

  def handle_info(_msg, state) do
    {:noreply, state}
  end

  # =====================================================================
  # Internal helpers
  # =====================================================================

  defp do_peer_connect(
         state,
         remote_pid,
         remote_shard,
         remote_num_shards,
         remote_hwm,
         remote_node_id
       )
       when remote_shard == state.shard_index do
    if remote_num_shards != state.num_shards do
      Logger.error(
        "#{log_prefix(state)} rejecting peer_connect from #{node(remote_pid)}: " <>
          "shard count mismatch (local=#{state.num_shards}, remote=#{remote_num_shards})"
      )

      state
    else
      %{db: db} = state
      remote_node = node(remote_pid)

      case maybe_allow_peer_reconnect(state, remote_node, remote_node_id) do
        {:quarantine, state} ->
          state

        {:ok, state} ->
          my_hwm_for_remote = Store.get_hwm(db, remote_node) || 0

          state = track_remote_shard(state, remote_node, remote_pid)
          state = track_peer_node_id(state, remote_node, remote_node_id)

          if state.cluster_size do
            alive = count_alive_node_ids(state)

            if alive > state.cluster_size do
              Logger.error(
                "#{log_prefix(state)} cluster overflow: #{alive} distinct node_ids, " <>
                  "cluster_size=#{state.cluster_size}. New peer #{remote_node} " <>
                  "has node_id=#{inspect(remote_node_id)}"
              )
            end
          end

          send_to_peer(
            state,
            remote_node,
            {:ekv_peer_connect_ack, self(), state.shard_index, state.num_shards,
             my_hwm_for_remote, state.node_id}
          )

          log_once(state, fn -> "#{log_prefix(state)} ekv_peer_connect from #{remote_node}" end)
          send_sync_data(state, remote_node, remote_hwm)

          state
      end
    end
  end

  defp do_peer_connect(
         state,
         _remote_pid,
         _remote_shard,
         _remote_num_shards,
         _remote_hwm,
         _remote_node_id
       ) do
    state
  end

  defp do_peer_connect_ack(
         state,
         remote_pid,
         remote_shard,
         remote_num_shards,
         remote_hwm,
         remote_node_id
       )
       when remote_shard == state.shard_index do
    if remote_num_shards != state.num_shards do
      Logger.error(
        "#{log_prefix(state)} rejecting peer_connect_ack from #{node(remote_pid)}: " <>
          "shard count mismatch (local=#{state.num_shards}, remote=#{remote_num_shards})"
      )

      state
    else
      remote_node = node(remote_pid)

      case maybe_allow_peer_reconnect(state, remote_node, remote_node_id) do
        {:quarantine, state} ->
          state

        {:ok, state} ->
          state = track_remote_shard(state, remote_node, remote_pid)
          state = track_peer_node_id(state, remote_node, remote_node_id)

          if state.cluster_size do
            alive = count_alive_node_ids(state)

            if alive > state.cluster_size do
              Logger.error(
                "#{log_prefix(state)} cluster overflow: #{alive} distinct node_ids, " <>
                  "cluster_size=#{state.cluster_size}. New peer #{remote_node} " <>
                  "has node_id=#{inspect(remote_node_id)}"
              )
            end
          end

          log_once(state, fn ->
            "#{log_prefix(state)} ekv_peer_connect_ack from #{remote_node}"
          end)

          send_sync_data(state, remote_node, remote_hwm)

          state
      end
    end
  end

  defp do_peer_connect_ack(
         state,
         _remote_pid,
         _remote_shard,
         _remote_num_shards,
         _remote_hwm,
         _remote_node_id
       ) do
    state
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

  defp send_sync_data(state, remote_node, remote_hwm) do
    %{db: db} = state
    config = EKV.get_config(state.name)
    tombstone_cutoff = System.system_time(:nanosecond) - config.tombstone_ttl * 1_000_000
    chunk_size = config.sync_chunk_size

    # remote_hwm is what the remote side reports having already applied from us.
    # Use it as the delta cursor into our local oplog sequence space.
    my_min_seq = Store.min_seq(db)
    my_seq = Store.max_seq(db)

    cond do
      is_integer(remote_hwm) and remote_hwm >= my_min_seq and remote_hwm <= my_seq ->
        send_delta_chunk(state, remote_node, remote_hwm, my_seq, chunk_size)

      is_integer(remote_hwm) and remote_hwm > my_seq ->
        log(state, fn ->
          "#{log_prefix_shard(state)} remote_hwm #{remote_hwm} > local_max_seq #{my_seq} " <>
            "for #{remote_node}; forcing full sync"
        end)

        send_full_chunk(state, remote_node, nil, tombstone_cutoff, my_seq, chunk_size)

      true ->
        send_full_chunk(state, remote_node, nil, tombstone_cutoff, my_seq, chunk_size)
    end
  end

  defp send_full_chunk(
         state,
         remote_node,
         last_key,
         tombstone_cutoff,
         my_seq,
         chunk_size
       ) do
    fetched = Store.full_state_chunk(state.db, tombstone_cutoff, last_key, chunk_size + 1)

    case fetched do
      [] ->
        :ok

      _ ->
        has_more? = length(fetched) > chunk_size
        entries = if has_more?, do: Enum.take(fetched, chunk_size), else: fetched
        final? = not has_more?
        seq_to_send = if final?, do: my_seq, else: 0

        send_to_peer(
          state,
          remote_node,
          {:ekv_sync, node(), state.shard_index, entries, seq_to_send}
        )

        if final? do
          :ok
        else
          next_key = elem(List.last(entries), 0)

          send(
            self(),
            {:continue_full_sync, remote_node, next_key, tombstone_cutoff, my_seq, chunk_size}
          )
        end
    end
  end

  defp send_delta_chunk(state, remote_node, last_seq, my_seq, chunk_size) do
    fetched = Store.oplog_since_chunk(state.db, last_seq, chunk_size + 1)

    case fetched do
      [] ->
        :ok

      _ ->
        has_more? = length(fetched) > chunk_size
        oplog_entries = if has_more?, do: Enum.take(fetched, chunk_size), else: fetched

        entries =
          Enum.map(oplog_entries, fn {_seq, key, value, timestamp, origin_node, expires_at,
                                      is_delete} ->
            deleted_at = if is_delete, do: timestamp, else: nil
            {key, value, timestamp, origin_node, expires_at, deleted_at}
          end)

        final? = not has_more?
        seq_to_send = if final?, do: my_seq, else: 0

        send_to_peer(
          state,
          remote_node,
          {:ekv_sync, node(), state.shard_index, entries, seq_to_send}
        )

        if final? do
          :ok
        else
          {max_chunk_seq, _, _, _, _, _, _} = List.last(oplog_entries)

          send(
            self(),
            {:continue_delta_sync, remote_node, max_chunk_seq, my_seq, chunk_size}
          )
        end
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

  # Send commit notification to acceptors (they promote kv_paxos → kv),
  # send LWW broadcast to non-acceptors (they don't have kv_paxos state).
  defp broadcast_commit_and_lww(state, key, ballot_c, ballot_n, lww_msg, accepted_node_ids) do
    shard_name = shard_name(state.name, state.shard_index)

    for {target_node, _pid} <- state.remote_shards do
      if MapSet.member?(accepted_node_ids, Map.get(state.peer_node_ids, target_node)) do
        send(
          {shard_name, target_node},
          {:ekv_cas_committed, key, ballot_c, ballot_n, state.shard_index}
        )
      else
        send({shard_name, target_node}, lww_msg)
      end
    end
  end

  # =====================================================================
  # CAS helpers
  # =====================================================================

  defp start_cas(state, key, operation, from) do
    %{db: db, cluster_size: cluster_size, node_id: node_id} = state
    quorum = div(cluster_size, 2) + 1

    # Check quorum achievable
    alive_count = count_alive_node_ids(state)

    cond do
      alive_count > cluster_size ->
        all_ids =
          [state.node_id | Map.values(state.peer_node_ids) |> Enum.reject(&is_nil/1)]
          |> Enum.uniq()

        Logger.error(
          "#{log_prefix(state)} cluster overflow: #{alive_count} distinct node_ids " <>
            "but cluster_size=#{cluster_size}. IDs: #{inspect(all_ids)}"
        )

        GenServer.reply(from, {:error, :cluster_overflow})
        state

      alive_count < quorum ->
        log(state, fn ->
          "#{log_prefix(state)} CAS no_quorum: #{alive_count}/#{cluster_size} " <>
            "node_ids reachable, need #{quorum}"
        end)

        GenServer.reply(from, {:error, :no_quorum})
        state

      true ->
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
          send_to_peer(
            state,
            remote_node,
            {:ekv_prepare, ref, self(), key, ballot_c, ballot_n, state.shard_index}
          )
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
          broadcast_msg: nil,
          entry_tuple: nil,
          events: []
        }

        # Check if local promise already gave us quorum (cluster_size: 1)
        cond do
          length(op.promises) >= quorum ->
            state = %{state | pending_cas: Map.put(state.pending_cas, ref, op)}
            enter_accept_phase(ref, op, state)

          local_nack > 0 and alive_count - local_nack < quorum ->
            # Can't reach quorum
            cancel_timer(timer)

            handle_cas_failure(ref, op, %{
              state
              | pending_cas: Map.put(state.pending_cas, ref, op)
            })

          true ->
            %{state | pending_cas: Map.put(state.pending_cas, ref, op)}
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

    # The value with the highest accepted ballot is the current state.
    # If all accepted ballots are {0, 0}, no value was ever accepted —
    # pick the kv_row with the highest {timestamp, origin} to match LWW
    # ordering. This ensures deterministic selection regardless of message
    # arrival order.
    {current_value, current_vsn} =
      if best_acc_c == 0 and best_acc_n in ["", nil] do
        best_kv_row_any =
          op.promises
          |> Enum.map(fn {_, _, _, row} -> row end)
          |> Enum.reject(&is_nil/1)
          |> Enum.max_by(fn [_val, ts, origin | _] -> {ts, origin} end, fn -> nil end)

        decode_kv_row(best_kv_row_any)
      else
        decode_kv_row(best_kv_row)
      end

    # Read-only CAS: if operation is :cas_read and no pending accepted value,
    # reply immediately without accept/commit phases.
    case op.operation do
      {:cas_read, _, _} when best_acc_c == 0 ->
        cancel_timer(op.timer)
        GenServer.reply(op.from, {:ok, current_value})
        %{state | pending_cas: Map.delete(state.pending_cas, ref)}

      _ ->
        # Apply operation. For :cas_read recovery (best_acc_c > 0), pass
        # the raw kv_row so metadata (expires_at, deleted_at) is preserved.
        apply_result =
          case op.operation do
            {:cas_read, _, _} ->
              apply_cas_read_recovery(op.key, best_kv_row, current_value)

            _ ->
              apply_operation(op.operation, op.key, current_value, current_vsn)
          end

        case apply_result do
          {:ok, _new_value_binary, new_entry_tuple, reply_value, broadcast_msg, events} ->
            # Send accept to all peers (do NOT do local accept yet — deferred until quorum)
            for {remote_node, _pid} <- state.remote_shards do
              send_to_peer(
                state,
                remote_node,
                {:ekv_accept, ref, self(), op.key, elem(op.ballot, 0), elem(op.ballot, 1),
                 new_entry_tuple, state.shard_index}
              )
            end

            timer = Process.send_after(self(), {:cas_timeout, ref}, 5_000)

            op = %{
              op
              | phase: :accept,
                accepts: MapSet.new(),
                accept_nacks: 0,
                responded: MapSet.new(),
                timer: timer,
                reply_value: reply_value,
                broadcast_msg: broadcast_msg,
                entry_tuple: new_entry_tuple,
                events: events
            }

            # For cluster_size: 1 (no peers), commit immediately
            if MapSet.size(op.accepts) + 1 >= op.quorum do
              commit_cas(ref, op, state)
            else
              %{state | pending_cas: Map.put(state.pending_cas, ref, op)}
            end

          {:error, :conflict} ->
            GenServer.reply(op.from, {:error, :conflict})
            %{state | pending_cas: Map.delete(state.pending_cas, ref)}
        end
    end
  end

  # Commit CAS: do local paxos_accept (kv_paxos), then write to kv + oplog.
  # Called only when remote accepts + self >= quorum.
  defp commit_cas(ref, op, state) do
    %{db: db, stmts: stmts} = state
    {key, value_binary, timestamp, origin_str, expires_at, deleted_at} = op.entry_tuple
    {ballot_c, ballot_n} = op.ballot
    value_args = [value_binary, timestamp, origin_str, expires_at, deleted_at]

    case Store.paxos_accept(db, key, ballot_c, ballot_n, value_args) do
      {:ok, true} ->
        # Write committed value to kv + oplog
        is_delete = if deleted_at, do: 1, else: 0
        kv_args = [key, value_binary, timestamp, origin_str, expires_at, deleted_at]
        oplog_args = [key, value_binary, timestamp, origin_str, expires_at, is_delete]

        {:ok, true} =
          EKV.Sqlite3.write_entry(
            db,
            stmts.kv_force_upsert,
            stmts.oplog_insert,
            kv_args,
            oplog_args
          )

        # Clear kv_paxos accepted columns so future prepares read from kv
        # and GC can purge the row when the kv entry is tombstone-purged
        Store.clear_paxos_accepted(db, key)

        cancel_timer(op.timer)
        dispatch_events(state, op.events)
        GenServer.reply(op.from, op.reply_value)

        if op.broadcast_msg do
          broadcast_commit_and_lww(state, key, ballot_c, ballot_n, op.broadcast_msg, op.accepts)
        end

        %{state | pending_cas: Map.delete(state.pending_cas, ref)}

      {:ok, false} ->
        # Local accept rejected (pre-empted between send and commit)
        handle_cas_failure(ref, op, state)
    end
  end

  defp apply_operation(operation, key, current_value, current_vsn) do
    case operation do
      {:cas_put, expected_vsn, value_binary, opts} ->
        if current_vsn == expected_vsn do
          now = monotonic_cas_ts(current_vsn)
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
          now = monotonic_cas_ts(current_vsn)
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
        now = monotonic_cas_ts(current_vsn)
        origin = node()
        origin_str = Atom.to_string(origin)
        ttl = Keyword.get(opts, :ttl)
        expires_at = if ttl, do: now + ttl * 1_000_000

        entry_tuple = {key, new_value_binary, now, origin_str, expires_at, nil}
        broadcast_msg = {:ekv_put, key, new_value_binary, now, origin, expires_at}
        events = [%EKV.Event{type: :put, key: key, value: new_value}]
        {:ok, new_value_binary, entry_tuple, {:ok, new_value}, broadcast_msg, events}

      {:cas_read, _opts, _retries} ->
        # Unreachable: cas_read recovery is handled via apply_cas_read_recovery
        # in enter_accept_phase. This clause exists only for completeness.
        {:error, :conflict}
    end
  end

  # Ensure CAS commit timestamps are strictly greater than the current value's
  # timestamp. This prevents LWW merge from overwriting the CAS-committed value
  # with a prior high-timestamp value after partition heal.
  defp monotonic_cas_ts(nil), do: System.system_time(:nanosecond)

  defp monotonic_cas_ts({current_ts, _origin}),
    do: max(System.system_time(:nanosecond), current_ts + 1)

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
        origin =
          if is_binary(origin_node_str),
            do: String.to_atom(origin_node_str),
            else: origin_node_str

        value = if value_binary, do: :erlang.binary_to_term(value_binary)
        {value, {timestamp, origin}}
    end
  end

  # Build entry_tuple for cas_read recovery directly from the raw accepted
  # kv_row columns, preserving expires_at and deleted_at exactly as accepted.
  defp apply_cas_read_recovery(
         key,
         [value_binary, ts, origin_str, expires_at, deleted_at],
         current_value
       ) do
    origin = if is_binary(origin_str), do: String.to_atom(origin_str), else: origin_str

    if is_integer(deleted_at) do
      # Tombstone — re-propose as delete with original metadata
      entry_tuple = {key, nil, ts, to_string(origin), nil, deleted_at}
      broadcast_msg = {:ekv_delete, key, ts, origin}
      {:ok, nil, entry_tuple, {:ok, nil}, broadcast_msg, []}
    else
      # Live value — re-propose with original expires_at
      entry_tuple = {key, value_binary, ts, to_string(origin), expires_at, nil}
      broadcast_msg = {:ekv_put, key, value_binary, ts, origin, expires_at}
      {:ok, value_binary, entry_tuple, {:ok, current_value}, broadcast_msg, []}
    end
  end

  defp handle_cas_failure(ref, op, state) do
    cancel_timer(op.timer)

    case op.operation do
      {:update, fun, opts, retries} when retries > 0 ->
        # Retry with new ballot after random backoff
        new_op = %{op | operation: {:update, fun, opts, retries - 1}}
        state = %{state | pending_cas: Map.put(state.pending_cas, ref, new_op)}
        {min_ms, max_ms} = Keyword.get(opts, :backoff, {10, 60})
        delay = Enum.random(min_ms..max_ms)

        Process.send_after(
          self(),
          {:cas_retry, ref, op.key, {:update, fun, opts, retries - 1}},
          delay
        )

        state

      {:cas_read, opts, retries} when retries > 0 ->
        new_op = %{op | operation: {:cas_read, opts, retries - 1}}
        state = %{state | pending_cas: Map.put(state.pending_cas, ref, new_op)}
        {min_ms, max_ms} = Keyword.get(opts, :backoff, {10, 60})
        delay = Enum.random(min_ms..max_ms)

        Process.send_after(
          self(),
          {:cas_retry, ref, op.key, {:cas_read, opts, retries - 1}},
          delay
        )

        state

      _ ->
        GenServer.reply(op.from, {:error, :conflict})
        %{state | pending_cas: Map.delete(state.pending_cas, ref)}
    end
  end

  defp count_alive_node_ids(state) do
    if state.cluster_size do
      # Our own node_id + distinct peer node_ids
      peer_ids =
        state.peer_node_ids
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

  defp mark_peer_down(state, remote_node, nil) do
    remember_peer_down_marker(state, peer_down_name_key(remote_node))
  end

  defp mark_peer_down(state, remote_node, remote_node_id) do
    if node_id_connected?(state, remote_node_id) do
      # Blue/green overlap: same cluster member identity is still connected.
      clear_peer_down_marker(state, peer_down_name_key(remote_node))
    else
      state
      |> clear_peer_down_marker(peer_down_name_key(remote_node))
      |> remember_peer_down_marker(peer_down_id_key(remote_node_id))
    end
  end

  defp maybe_allow_peer_reconnect(state, remote_node, remote_node_id \\ nil)

  defp maybe_allow_peer_reconnect(
         %{partition_ttl_policy: :ignore} = state,
         remote_node,
         remote_node_id
       ) do
    state = clear_peer_down_markers(state, remote_node, remote_node_id)

    state = %{
      state
      | quarantined_peers: MapSet.delete(state.quarantined_peers, remote_node)
    }

    {:ok, state}
  end

  defp maybe_allow_peer_reconnect(state, remote_node, remote_node_id) do
    {state, down_since_ms} = resolve_peer_down_since(state, remote_node, remote_node_id)

    age_ms =
      if is_integer(down_since_ms), do: max(0, System.system_time(:millisecond) - down_since_ms)

    cond do
      is_nil(down_since_ms) ->
        state = %{state | quarantined_peers: MapSet.delete(state.quarantined_peers, remote_node)}
        {:ok, state}

      is_integer(down_since_ms) and age_ms > state.tombstone_ttl ->
        state = %{
          state
          | quarantined_peers: MapSet.put(state.quarantined_peers, remote_node),
            remote_shards: Map.delete(state.remote_shards, remote_node),
            peer_node_ids: Map.delete(state.peer_node_ids, remote_node)
        }

        log_once(state, fn ->
          "#{log_prefix(state)} quarantining #{remote_node}: reconnect downtime exceeded " <>
            "tombstone_ttl (#{state.tombstone_ttl}ms). " <>
            "Replication is blocked until operator rebuilds one side."
        end)

        {:quarantine, state}

      true ->
        state = clear_peer_down_markers(state, remote_node, remote_node_id)

        state = %{
          state
          | quarantined_peers: MapSet.delete(state.quarantined_peers, remote_node)
        }

        {:ok, state}
    end
  end

  defp resolve_peer_down_since(state, remote_node, nil) do
    read_peer_down_marker(state, peer_down_name_key(remote_node))
  end

  defp resolve_peer_down_since(state, remote_node, remote_node_id) do
    id_key = peer_down_id_key(remote_node_id)
    name_key = peer_down_name_key(remote_node)

    {state, id_down_since} = read_peer_down_marker(state, id_key)
    {state, name_down_since} = read_peer_down_marker(state, name_key)

    if is_integer(name_down_since) do
      merged_down_since =
        if is_integer(id_down_since),
          do: min(id_down_since, name_down_since),
          else: name_down_since

      state =
        if id_down_since == merged_down_since,
          do: state,
          else: put_peer_down_marker(state, id_key, merged_down_since)

      state = clear_peer_down_marker(state, name_key)
      {state, merged_down_since}
    else
      {state, id_down_since}
    end
  end

  defp remember_peer_down_marker(state, marker_key) do
    {state, existing_down_since} = read_peer_down_marker(state, marker_key)

    if is_integer(existing_down_since) do
      state
    else
      put_peer_down_marker(state, marker_key, System.system_time(:millisecond))
    end
  end

  defp read_peer_down_marker(state, marker_key) do
    case Map.fetch(state.peer_down_at, marker_key) do
      {:ok, down_since} ->
        {state, down_since}

      :error ->
        down_since = Store.peer_down_marker_get(state.db, marker_key)

        state =
          if is_integer(down_since),
            do: %{state | peer_down_at: Map.put(state.peer_down_at, marker_key, down_since)},
            else: state

        {state, down_since}
    end
  end

  defp put_peer_down_marker(state, marker_key, down_since) do
    Store.peer_down_marker_put(state.db, marker_key, down_since)
    %{state | peer_down_at: Map.put(state.peer_down_at, marker_key, down_since)}
  end

  defp clear_peer_down_marker(state, marker_key) do
    Store.peer_down_marker_clear(state.db, marker_key)
    %{state | peer_down_at: Map.delete(state.peer_down_at, marker_key)}
  end

  defp clear_peer_down_markers(state, remote_node, remote_node_id) do
    remote_node
    |> peer_down_marker_keys(remote_node_id)
    |> Enum.uniq()
    |> Enum.reduce(state, fn marker_key, acc ->
      clear_peer_down_marker(acc, marker_key)
    end)
  end

  defp peer_down_marker_keys(remote_node, nil), do: [peer_down_name_key(remote_node)]

  defp peer_down_marker_keys(remote_node, remote_node_id) do
    [peer_down_id_key(remote_node_id), peer_down_name_key(remote_node)]
  end

  defp peer_down_id_key(remote_node_id), do: @peer_down_id_prefix <> to_string(remote_node_id)
  defp peer_down_name_key(remote_node), do: @peer_down_name_prefix <> Atom.to_string(remote_node)

  defp node_id_connected?(state, remote_node_id) do
    Enum.any?(state.peer_node_ids, fn {peer_node, peer_node_id} ->
      peer_node_id == remote_node_id and Map.has_key?(state.remote_shards, peer_node)
    end)
  end

  defp prune_stale_peer_down_name_markers(state) do
    retention_ms = max(@peer_down_name_min_retention_ms, state.tombstone_ttl * 4)
    stale_before_ms = System.system_time(:millisecond) - retention_ms

    Store.prune_peer_down_name_markers(
      state.db,
      stale_before_ms,
      @peer_down_name_max_entries
    )
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
