defmodule EKV.Replica do
  @moduledoc false

  _archdoc = ~S"""
  EKV — Eventually Consistent Durable KV Store with Compare-And-Swap via CASPaxos
  ===============================================================================

  EKV is a sharded, replicated key-value store where data outlives the node
  that created it. EKV entries survive node restarts, node death, and network
  partitions. Data is only removed by explicit delete or TTL expiry.

  The default consistency model is Last-Write-Wins (LWW) — every node can
  read and write independently, and conflicts are resolved by timestamp. For
  keys that need stronger guarantees, an opt-in Compare-And-Swap (CAS) mode
  provides linearizable read-modify-write via CASPaxos consensus.

  Member-to-member replica discovery is shard-local, but not purely raw
  `Node.list/0` anymore: shards still use `:net_kernel.monitor_nodes/1` and
  Erlang node connectivity for direct handshakes, while `EKV.MemberPresence`
  provides the current logical-member view used by routing, origin
  recognition, and reconnect retry to peers missing from `remote_shards`.
  Client routing is separate — client EKV instances discover ready members
  through scoped `:pg` groups published by `EKV.MemberPresence`. Each EKV
  instance owns its own scoped `:pg` mesh, isolating routing, subscriptions,
  and shutdown coordination from other EKV instances and unrelated
  default-scope `:pg` traffic. Zero runtime deps. SQLite is vendored as a C
  NIF (c_src/sqlite3.c amalgamation).


  ## Supervision Tree

      Member mode:
        EKV.Supervisor (rest_for_one)
        ├── :pg scope               instance-local routing/subscription mesh
        ├── EKV.BlueGreenMarker?    marker cleanup / handoff bookkeeping
        ├── EKV.SubTracker          atomics subscriber count + process monitors
        ├── Registry                keys: :duplicate, listeners: [sub_tracker]
        ├── EKV.SubDispatcher.Supervisor (one_for_one)
        │   ├── EKV.SubDispatcher 0   async event fan-out per shard
        │   ├── EKV.SubDispatcher 1
        │   └── ...
        ├── EKV.MemberPresence    publishes ready member in :pg region groups
        │                         and node_id groups
        ├── EKV.Replica.Supervisor (one_for_one)
        │   ├── EKV.Replica 0     shard GenServer (writes + replication + SQLite)
        │   ├── EKV.Replica 1
        │   └── ...               N shards (default 8)
        ├── EKV.QuorumGate?       optional startup barrier for CAS quorum
        ├── EKV.GC                periodic timer, sends :gc to each shard
        └── EKV.ShutdownBarrier?  optional graceful shutdown barrier

      Client mode:
        EKV.Supervisor (rest_for_one)
        ├── :pg scope                 instance-local routing/subscription mesh
        ├── EKV.ClientRouter         region-ordered backend selection
        ├── EKV.RouteGate?           optional startup barrier for route selection
        ├── EKV.QuorumGate?          optional startup barrier via selected member
        ├── EKV.ClientSubscriptions  local subscribe/unsubscribe bookkeeping
        └── EKV.ShutdownBarrier?     optional graceful shutdown barrier

  rest_for_one: SubTracker crash restarts everything. Registry crash restarts
  Dispatchers + MemberPresence + Replicas. Single Replica crash → only that
  shard restarts. MemberPresence now starts before Replicas so startup-time
  origin recognition and reconnect retry can see current logical members
  before anti-entropy begins. QuorumGate, GC, and ShutdownBarrier are
  downstream of Replicas.


  ## Graceful Shutdown Barrier

  `shutdown_barrier: timeout_ms` is an opt-in coordinated shutdown aid.
  `EKV.ShutdownBarrier` is the last child in the tree, so it receives shutdown
  first and can block while the rest of EKV is still serving traffic.

  Coordination is via the EKV instance's scoped `:pg` mesh:

    - `{:ekv_shutdown_live, name}` / `{:ekv_shutdown_terminal, name}`
      track all EKV instances for this logical store
    - `{:ekv_shutdown_live_member, name, node_id}` /
      `{:ekv_shutdown_terminal_member, name, node_id}`
      track logical voting members only

  Members count quorum by logical `node_id`, not Erlang node name, so
  blue-green overlap still counts as one voter. A member waits only when
  coordinated shutdown is in progress or exiting would risk dropping the live
  voting set below quorum while other members are still non-terminal. Clients
  participate in terminal coordination but never count toward quorum.

  A blue-green outgoing member that has already entered proxy mode skips the
  barrier wait: it has already handed responsibility to its replacement.


  ## Storage: SQLite Only

  Each shard has a single SQLite database (WAL mode, synchronous=NORMAL):

      ┌──────────────────────────────────────────────────────────────┐
      │ SQLite (WAL mode, synchronous=NORMAL)                        │
      │ File: #{data_dir}/shard_#{i}.db                              │
      │                                                              │
      │ Tables:                                                      │
      │   kv          — current state, PK (key)                      │
      │   kv_keyrefs  — deduplicated replay-key dictionary           │
      │   kv_oplog    — authoritative replay log keyed by            │
      │                 (origin_node, origin_seq) using key_id refs  │
      │   kv_origin_progress — local applied replay progress         │
      │   kv_member_progress — per-member, per-origin peer progress  │
      │   kv_meta     — liveness + ballot counter + down markers     │
      │   kv_paxos    — CAS consensus state per key (opt-in)         │
      │                                                              │
      │ Indexes:                                                     │
      │   idx_kv_deleted  — partial on deleted_at (WHERE NOT NULL)   │
      │   idx_kv_expires  — partial on expires_at (WHERE NOT NULL)   │
      │                                                              │
      │ - Single source of truth. Survives process/node crashes.     │
      │ - normal writes commit kv + replay history atomically.       │
      │ - full sync rebuilds kv only; it does not seed kv_oplog.     │
      └──────────────────────────────────────────────────────────────┘

  Connections per shard:
    - 1 writer connection (owned by the Replica GenServer)
    - `System.schedulers_online()` reader connections

  Reader connections are stored as a tuple of `{db, get_stmt}` in
  persistent_term keyed by `{EKV, name, :readers, shard_index}`. Reads
  pick a connection by `rem(scheduler_id - 1, num_readers)` — zero
  contention, no pool, no GenServer hop. WAL mode ensures readers
  don't block the writer.

  Values are stored as `:erlang.term_to_binary/1` blobs. Encoding happens
  in the public EKV module; Replica and Store only see binaries.

  ### Anti-entropy/storage model:

  - `kv` - current shard state
  - `kv_oplog` - per-origin write history used for delta repair
  - `kv_keyrefs` - replay-key dictionary so oplog rows do not repeat full keys;
    SQLite triggers maintain `oplog_refs`, and GC prunes orphan keyrefs only
    after replay is gone and the key no longer exists in `kv`
  - `kv_origin_progress` - how far this shard has contiguously applied each origin
    stream
  - `kv_member_progress` - how far each peer has consumed each origin stream;
    this drives both repair decisions and oplog truncation
  - persisted origin/member identity uses stable `node_id`; transient Erlang
    node names are transport-only and do not define replay streams

  Delta sync sends retained `kv_oplog` rows for the missing origin range.
  Full sync copies current `kv` state only when retained history is not
  available or a quarantined/explicit rebuild path requires it. Receivers do
  not append those snapshot rows back into `kv_oplog`.


  ## Custom NIF and Dirty IO Bounces

  EKV vendors sqlite3.c (amalgamation) and uses a custom NIF with no
  runtime deps. The main latency bottleneck is the dirty IO scheduler
  bounce (~1μs per NIF call), not SQL parsing.

  Combined NIF functions reduce bounces per operation:

      ┌──────────────────────────────────────────────────────────────┐
      │ NIF function          │ Bounces │ Operations                 │
      ├───────────────────────┼─────────┼────────────────────────────│
      │ write_entry           │    1    │ BEGIN + local origin seq   │
      │                       │         │ alloc (if needed) + kv     │
      │                       │         │ upsert (LWW) + check       │
      │                       │         │ changes + keyref + oplog + │
      │                       │         │ progress update + commit    │
      │                       │         │ update + COMMIT/ROLLBACK   │
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
      │                       │         │ ballot check + local       │
      │                       │         │ origin seq alloc (if       │
      │                       │         │ needed) + kv upsert +      │
      │                       │         │ keyref ensure + oplog      │
      │                       │         │ insert +                   │
      │                       │         │ progress update + commit   │
      │                       │         │ update + clear kv_paxos +  │
      │                       │         │ COMMIT                     │
      └──────────────────────────────────────────────────────────────┘

  Without combined NIFs, a simple put would be 5 dirty bounces
  (begin, upsert, check, oplog, commit). write_entry does it in 1.

  Cached prepared statements:
    - Writer: kv_upsert (LWW), kv_force_upsert (no LWW), keyref_upsert,
      oplog_insert
    - Readers: get statement (per reader connection)
    - NIF-internal writer helpers: local origin seq/progress statements are
      cached on the connection as well, so the extra replay bookkeeping stays
      inside the same single dirty NIF hop without per-write prepare/finalize
      churn.

  All IO-touching NIFs use ERL_NIF_DIRTY_JOB_IO_BOUND. The bind NIF
  runs on a normal scheduler (no IO).


  ## Sharding

  Shard assignment: `:erlang.phash2(key, num_shards)`

  Each shard is a completely independent GenServer with its own SQLite db
  file. Shards on different nodes with the same index are counterparts —
  they replicate to each other and sync on connect.

  Prefix scans (scan/keys) cannot be routed to a single shard because
  the prefix doesn't determine the hash. They fan out to all shards.

  Startup metadata is persisted in `kv_meta` on first open:
  - `schema_version` gates shard-db compatibility with this build
  - `num_shards` makes shard count immutable after first open

  Changing shard count raises `ArgumentError`. Peer connections from nodes with
  mismatched shard counts are rejected (logged error, no crash).


  ## Write Path (LWW)

      Client                  Replica (shard i)              Peers
        │                          │                           │
        │ send local request       │                           │
        │  {:put | :delete, ...}   │                           │
        │─────────────────────────>│                           │
        │                          │                           │
        │                    opportunistically drain adjacent  │
        │                    local LWW writes                  │
        │                    write_local_entries_batch NIF:    │
        │                      BEGIN IMMEDIATE                 │
        │                      per entry: kv upsert (LWW)      │
        │                      applied rows allocate           │
        │                      contiguous local origin_seq     │
        │                      keyref + oplog insert           │
        │                      persist final local_origin_seq  │
        │                      COMMIT                          │
        │                          │                           │
        │                          │ {:ekv_replication_batch,  │
        │                          │  from_node, shard,        │
        │                          │  origin_node, entries}    │
        │                          │──────────────────────────>│
        │                          │  (one bounded live        │
        │                          │   replication turn per    │
        │                          │   destination shard)      │
        │<─────────────────────────│                           │
        │         :ok              │                           │

  Delete is identical but sets `deleted_at = now` and `value = nil`. Local
  writes still allocate origin seqs one entry at a time; only the outbound
  live replication fanout is buffered into `:replication_batch` messages.

  Local non-CAS ingress is no longer a normal `GenServer.call/3` fast path.
  Replica shards still run as GenServers, but local non-CAS and local CAS API
  calls now arrive as plain messages with caller-managed timeout/monitor
  semantics. This lets the shard selectively receive local work between
  replicated live LWW messages without trying to reorder `'$gen_call'`
  traffic inside OTP internals.

  ### Mailbox fairness and selective receive

  The fairness goal is deliberately narrow:

    - local API traffic (both local LWW and local CAS) should not be buried
      forever behind a burst of replicated live LWW
    - CAS protocol/control traffic (`prepare`, `promise`, `accept`,
      `accepted`, retries, timeouts, handoff, member sync, node up/down) must
      stay responsive
    - but neither control traffic nor local traffic should be allowed to
      "drain forever" and starve the rest of the mailbox

  EKV therefore uses two different mailbox policies:

    1. Local ingest collection
    2. Post-replication turn-taking

  #### 1. Local ingest collection is FIFO across local request classes

  Local API requests arrive as:

      {@local_request_tag, reply_dest, ref, request}

  `reply_dest` is a generic reply destination, not necessarily a pid:

    - in the hot local API path it is a process alias created by the caller
    - in a few tests/manual cases it may still be a plain pid

  When the shard begins a local LWW write turn, it opportunistically collects
  adjacent local `put` / `delete` requests into one batch. The collector uses
  one broad local-request receive clause, then branches on `request` in normal
  Elixir code. That is intentional:

    - BEAM receive scans the mailbox from oldest to newest
    - clause order does not reorder two messages that are both matchable in
      the same receive
    - actual reordering happens only when an older message does not match any
      active clause and a newer one does

  So, for local ingest, EKV does not selectively match local CAS before local
  LWW. It takes the oldest local request, and if that request is batchable LWW
  it may extend the batch with immediately-adjacent later `put` / `delete`
  requests that still fit the batch budget.

  Important invariant:

    - once a local LWW batch has started collecting, a later local request
      that is not batchable into that batch does not execute inline
    - instead, the current batch commits first, and the later request is
      deferred until after the batch finishes

  This prevents:

    - later local `put` / `delete` overtaking earlier local LWW
    - later local CAS observing pre-batch state by jumping ahead of an already
      open local LWW batch

  In other words, local ingest fairness is FIFO-first, not
  latency-first-for-CAS.

  #### 2. Post-replication turn-taking prioritizes control, then one local turn

  Live replicated LWW (`:replication_batch`) still applies eagerly on receipt.
  After one such replicated turn completes, the
  shard does not recursively drain all "priority" traffic. Instead it performs
  a bounded two-stage turn:

    a. process up to N control/CAS-protocol messages immediately
    b. process at most one broad local request turn
    c. yield back to the normal mailbox loop

  `N` is intentionally bounded (today tied to the local batch budget) so that:

    - handoff, member sync, CAS proposer/acceptor traffic, node monitoring,
      retries, and timeouts get prompt service
    - but a hot stream of control/CAS protocol messages cannot monopolize the
      shard forever

  Then EKV takes at most one local request turn via the same broad
  `{@local_request_tag, reply_dest, ref, request}` match described above.
  This gives local API traffic a lane between replicated LWW turns, but still
  avoids recursively draining the entire local mailbox.

  So the policy is:

    - replicated live LWW yields frequently
    - control/CAS protocol traffic gets bounded priority
    - local API traffic gets one FIFO turn
    - then the shard yields back to the mailbox

  #### Why this split exists

  Local CAS is latency-sensitive, but ingest-side CAS is not the critical
  latency path. The critical path for CAS is receiver-side protocol handling:

    - prepare / promise / nack
    - accept / accepted / accept_nack
    - CAS retry / timeout / quorum bookkeeping

  That receiver-side protocol traffic is part of the bounded control turn
  above, so it stays responsive during replicated LWW bursts.

  Local ingest CAS, on the other hand, should not jump ahead of an older local
  LWW request just because both happen to be waiting in the same mailbox. LWW
  callers also expect prompt service, and allowing local CAS to selectively
  skip older local LWW would make local request ordering surprising.

  #### What this does not promise

  This fairness scheme is not a hard real-time scheduler and does not create
  isolated queues. It does not guarantee:

    - local API traffic always outruns replicated traffic
    - CAS protocol traffic outruns everything forever
    - equal service shares across all message classes

  It is a pragmatic mailbox policy:

    - prevent replicated LWW from monopolizing the shard
    - keep control/CAS protocol responsive
    - preserve FIFO semantics for local API ingress
    - avoid starvation caused by recursively draining one message class
      indefinitely

  On the receiving side, delta replay uses the same write_entry NIF with the
  remote's timestamp and `origin_seq`, so replay rows and local contiguous
  progress advance in the same SQLite transaction. Full-sync receive uses a
  kv-only snapshot apply path instead: it rebuilds current state and settles
  progress only from the terminal advertised summary. For self-origin writes,
  the shard allocates `origin_seq` durably in the write/promote transaction
  and can advance self progress directly to the allocated seq without scanning
  for gaps; remote-origin writes still use contiguous-prefix advancement. Live
  writes therefore piggyback the sender's current head via
  `(origin_node, origin_seq)`, and receivers can detect gaps immediately
  without any extra live progress-ack message.

  For live replicated LWW traffic, the shard still applies each inbound
  replication-batch turn eagerly, then runs the bounded
  fairness turn above. This is the mailbox-level mechanism that keeps
  replicated write bursts from monopolizing the shard while still preserving
  FIFO local ingest semantics.

  Outbound member sends are also split into two reliability classes:

    - best-effort replication / repair coordination:
      `:replication_batch`, `:member_connect`,
      `:member_connect_ack`, `:summary_probe`, `:summary_reply`,
      `:sync_request`, and `:progress_ack`
    - must-send protocol / bulk transfer:
      CAS traffic and `:sync` chunks

  The default transport uses `send_nosuspend` for best-effort classes so a slow
  peer does not suspend the shard in `dsend_continue_trap`. If such a send
  cannot be enqueued without suspending the shard, it is dropped and later
  anti-entropy/discovery rounds are expected to catch the peer up. Must-send
  classes use normal blocking distribution sends by default.

  Custom data-plane transports implement `EKV.Transport` and receive the same
  reliability classification in send opts. They are expected to provide a
  volatile, bounded, ordered lane for shard-to-shard messages. EKV initializes
  the adapter per shard process so transports can pin any per-caller lane
  selection to that shard. EKV does not start or supervise external transports.

  Large replicated value payloads may be compressed on the wire only.
  The message shape stays the same, but the value field may be tagged as
  `{:ekv_wire_compressed, compressed_binary}` for:

    - replication-batch entry values
    - `{:ekv_accept, ...}`
    - full-payload `{:ekv_cas_committed, ...}`

  Receivers inflate the value before normal processing. Values remain
  uncompressed in SQLite and on the read path.


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

  Every entry carries a nanosecond timestamp and persisted `origin_node`
  string. In current member mode this is the stable logical `node_id`.
  For successive local writes on the same shard, EKV does not reuse a
  timestamp, so a key will not see the same `{timestamp, origin_node}` from
  that shard twice.

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
    - GC TTL expiry bookkeeping / local `:expired` notification

  The tiebreaker (lexicographic `origin_node` string comparison) is
  deterministic across all nodes and matches SQLite TEXT ordering.

  A delete is just an entry with deleted_at set. Same LWW applies — a
  put with a higher timestamp beats a delete, and vice versa.


  ## Compare-And-Swap (CAS) via CASPaxos

  EKV is eventually consistent by default. For keys that need atomic
  read-modify-write, CAS provides per-key linearizability via a CASPaxos-based
  protocol. CAS is opt-in: requires `cluster_size` and `node_id` config. Both LWW
  and CAS writes coexist — different keys can use different consistency models
  in the same EKV instance.

  ### Why CASPaxos?

  [CASPaxos Paper](https://arxiv.org/pdf/1802.07000)

  Classic Paxos replicates a log. CASPaxos is simpler — it's a
  single-decree consensus protocol for values, not logs. Each key is
  an independent consensus instance. The protocol is:

      1. Prepare (read phase): learn the current value + get promises
      2. Accept (write phase): propose a new value to acceptors
      3. Commit: write to kv + oplog, broadcast to all members

  No log replication, no leader election, no view changes. Any node
  can be a proposer for any key at any time.

  ### Ballot Numbers

  Each CAS operation gets a unique ballot `{counter, node_id}`. Counters
  are monotonically increasing per-shard `(max(system_time_ns, prev + 1))`
  and persisted in kv_meta to survive restarts. Ballots are ordered by
  `(counter, node_id)`: `counter` compares numerically, and equal counters
  are broken by lexicographic comparison of the normalized string `node_id`.

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
        │─────────────────────────────────────────────────>│
        │                           │                      │
        │ {:ekv_promise, ref, ...   │                      │
        │  acc_c, acc_n, kv_row}    │                      │
        │<──────────────────────────│                      │
        │<─────────────────────────────────────────────────│
        │                           │                      │
        │  quorum promises reached  │                      │
        │  pick highest accepted    │                      │
        │  value → apply operation  │                      │
        │  (compare-and-swap check) │                      │
        │                           │                      │
        │ ── Phase 2: ACCEPT ───    │                      │
        │                           │                      │
        │ local paxos_accept        │                      │
        │ (durable self-accept)     │                      │
        │                           │                      │
        │ {:ekv_accept, ref, pid,   │                      │
        │  key, ballot, entry}      │                      │
        │──────────────────────────>│                      │
        │─────────────────────────────────────────────────>│
        │                           │                      │
        │  acceptors write to       │                      │
        │  kv_paxos ONLY (not kv)   │                      │
        │                           │                      │
        │ {:ekv_accepted, ref, ...} │                      │
        │<──────────────────────────│                      │
        │<─────────────────────────────────────────────────│
        │                           │                      │
        │  quorum accepts reached   │                      │
        │                           │                      │
        │ ── Phase 3: COMMIT ───    │                      │
        │                           │                      │
        │  local: paxos_promote     │                      │
        │   (kv_paxos -> kv+oplog)  │                      │
        │                           │                      │
        │ {:ekv_cas_committed, ...} │  (to all members)    │
        │──────────────────────────>│                      │
        │─────────────────────────────────────────────────>│
        │                           │                      │
        │  receivers: paxos_promote │                      │
        │  or paxos_accept+promote  │                      │

  The proposer records local accept before sending remote accepts, then
  commits only after accepted quorum is reached. Accepted values remain
  invisible to reads until promote writes them to kv.

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

  After commit, the proposer sends:

    - `{:ekv_cas_committed, key, ballot_c, ballot_n, entry_tuple | nil, shard}`
      → sent to all members
      → members that already accepted may receive `nil` payload and promote from
        local `kv_paxos`
      → members that may have missed accept receive full `entry_tuple`
      → when a full payload is present, its value field may be wire-compressed
      → receiver first tries paxos_promote
      → if stale/missing accepted state and a full payload is present, receiver
        can paxos_accept(entry_tuple) then paxos_promote (ballot-guarded)

  ### paxos_promote (commit on acceptor side)

  Single NIF dirty bounce:
    1. BEGIN IMMEDIATE
    2. Read kv_paxos for the key
    3. Verify ballot matches (if stale → return `{:ok, :stale}`)
    4. Read previous value from kv (for subscriber events)
    5. Force-upsert to kv (no LWW — Paxos ballots determine ordering)
    6. Insert to oplog
    7. COMMIT

  Accepted state is retained so future paxos_prepare can recover the
  latest chosen CAS value directly from kv_paxos.

  ### Quorum and Failure Handling

  Quorum: `floor(cluster_size / 2) + 1`

  Before starting CAS, the proposer checks that enough node_ids are
  reachable. `alive_node_id_count/1` tracks distinct node ids (not Erlang
  nodes — multiple Erlang nodes may share a node_id in blue-green).

  Failure modes:
    - Nack in prepare: ballot too low. If can't reach quorum → fail.
    - Nack in accept: ballot superseded. If can't reach quorum → fail.
    - Timeout (call deadline): no response from enough members before the
      CAS call's `:timeout` budget expires → `{:error, :quorum_timeout}`
    - Node death during CAS: `fail_pending_cas_if_no_quorum/1` checks all
      pending ops and fails those that can no longer reach quorum.

  Caller-visible CAS write outcomes (put if_vsn/delete if_vsn/update):

    - `:ok`
      CAS reached commit and value was applied to kv + oplog.

    - `{:error, :conflict}`
      The write was rejected before a deciding accept phase.
      Typical paths:
        - prepare phase loses quorum (nacks/no quorum) and retries are exhausted
        - compare-and-swap predicate fails while applying operation
          (for example stale if_vsn) before accept messages are sent

    - `{:error, :unconfirmed}`
      The write reached accept phase, so some acceptors may have accepted
      it, but the proposer could not confirm final outcome.
      Typical paths:
        - accept phase loses quorum after accept messages were sent
        - local accepted ballot is superseded before commit can promote

  In code, this mapping is phase-based: failures in accept phase for write ops are
  returned as `:unconfirmed`. Other CAS write failures are returned as `:conflict`.

  Operational rule: on `:unconfirmed`, issue a barrier read
  (`EKV.get(name, key, consistent: true)`) to resolve committed state before
  taking follow-up actions.

  Optional API behavior: CAS write calls can pass `resolve_unconfirmed: true`.
  In that mode, when a write fails in accept phase, EKV performs one internal
  barrier read and maps the result to current-state outcomes:
    - returns the original `{:ok, ...}` if resolved VSN matches the attempted write
    - returns `{:error, :conflict}` if current VSN differs
    - returns `{:error, :unavailable}` if the resolution read cannot complete

  For EKV.update (read-modify-write), retries happen only for definite
  conflicts. Ambiguous accept outcomes return `:unconfirmed` by default,
  or the resolved current-state mapping above when
  `resolve_unconfirmed: true` is set.

  ### CAS + LWW Interaction

  CAS and LWW share the same kv/oplog storage and replication paths, but
  ordering models are different (ballot order for CAS vs timestamp order for
  LWW). The supported model is key-level ownership:

    - Different keys may use different modes in the same EKV instance.
    - A key may start in LWW mode and later transition to CAS mode
      (`LWW -> CAS` is supported).
    - A key managed via CAS should continue to use CAS write APIs
      (`CAS -> LWW` writes are not supported).
      Reads may still choose eventual or consistent paths based on needs.
    - Eventual writes (`put/delete` without CAS options) to CAS-managed keys
      are rejected with `{:error, :cas_managed_key}`.
    - Important limitation: `LWW -> CAS` is an operational migration, not a
      partition-safe fenced mode switch. A stale or partitioned node that has
      not yet learned CAS ownership for a key can still accept an eventual
      LWW write for that key during cutover. On heal, that stale LWW write may
      still win by normal `{timestamp, origin}` ordering if it is newer than
      the state the CAS quorum saw. This is a mixed-mode migration edge case,
      not a steady-state CAS race.
    - Sync sends entries from kv (committed state). kv_paxos tentative
      values are not included in sync — they only exist locally until
      committed.

  CAS operations are serialized per-shard through the GenServer. Two
  concurrent CAS operations on the same key from different nodes will
  compete via ballot ordering — the higher ballot wins. `update()` with
  auto-retry handles this transparently.


  ## Member Discovery and Tracking

  Replica shards manage their own member mesh independently:

      `init/1`:
        :net_kernel.monitor_nodes(true)
        for node <- Node.list(), send {:ekv_member_connect, ...}

      nodeup:
        attempt :ekv_member_connect (gated by long-partition quarantine)

      nodedown:
        remove from remote_shards + member_node_ids
        persist member down marker (node_id key if known, fallback name key)
        fail any pending CAS ops that lost quorum

      DOWN (monitored remote shard pid):
        remove from remote_shards + member_node_ids
        persist member down marker (node_id key if known, fallback name key)
        fail any pending CAS ops that lost quorum

  remote_shards :: %{node() => pid()} tracks confirmed live counterpart
  shard processes. A node enters this map only after a successful
  member_connect / member_connect_ack handshake where its pid is monitored.

  member_node_ids :: %{node() => string()} maps Erlang nodes to their
  configured node_id. Used for CAS quorum counting (distinct node_ids,
  not Erlang nodes, determine quorum).

  Long-partition tracking is persisted in kv_meta:
    - member_down_at:id:<node_id>    (preferred, stable identity)
    - member_down_at:name:<node>     (fallback when node_id unknown)

  On reconnect handshake, if downtime > tombstone_ttl and policy is
  `:quarantine`, replication is blocked for that member. Down markers are
  cleared only after a successful non-quarantined reconnect.
  Anti-entropy also retries member_connect to current MemberPresence members
  missing from remote_shards, so transient false down-markers should normally
  self-heal before they age into quarantine. Current presence does not bypass
  overdue quarantine.


  ## Member Sync Protocol

  When two member nodes discover each other (init, nodeup), matching shard
  processes exchange a handshake. Sync remains strictly shard-local: one
  handshake only negotiates one shard pair.

  Member-to-member traffic is wrapped in a fixed wire envelope:

      {:ekv, 1, kind, payload, meta}

  The handshake advertises replay progress as a map of origin streams:

      %{origin_node => last_seq_applied}

  This is the core anti-entropy cursor model:
    - progress is per shard, per origin
    - the receiver decides whether it is behind
    - delta replay is built from authoritative per-origin oplog history
    - full sync remains the fallback when replay history is missing or outside
      the retained window

      Node A (shard i)                                         Node B (shard i)
        │                                                             │
        │ {:ekv, 1, :member_connect,                                  │
        │  {pid_a, i, num_shards, progress_a,                         │
        │   node_id_a}, %{features: ...}}                             │
        │────────────────────────────────────────────────────────────>│
        │                                                             │
        │                        validate shard counts match          │
        │                        monitor pid_a                        │
        │                        add A to remote_shards               │
        │                        persist A's advertised progress map  │
        │                                                             │
        │ {:ekv, 1, :member_connect_ack,                              │
        │  {pid_b, i, num_shards, progress_b,                         │
        │   node_id_b}, %{features: ...}}                             │
        │<────────────────────────────────────────────────────────────│
        │                                                             │
        │         after handshake, each side compares:                │
        │           remote origin heads vs local contiguous progress  │
        │         if local side is behind:                            │
        │           request delta from that live origin               │
        │         if local side is behind on quarantined dead-origin  │
        │         state:                                              │
        │           request full sync from a live peer immediately    │
        │         if local side is behind on a known member origin    │
        │         that is merely down/disconnected:                   │
        │           request relayed delta from any live peer that     │
        │           advertises retained history for that origin       │
        │                                                             │
        │ {:ekv, 1, :summary_probe,                                   │
        │  {pid_a, i, progress_a}, %{}}                               │
        │────────────────────────────────────────────────────────────>│
        │ {:ekv, 1, :summary_reply,                                   │
        │  {pid_b, i, progress_b}, %{}}                               │
        │<────────────────────────────────────────────────────────────│
        │ {:ekv, 1, :sync_request,                                    │
        │  {pid_b, i, {:delta, node_a, from_seq}}, %{}}               │
        │<────────────────────────────────────────────────────────────│
        │ {:ekv, 1, :sync,                                            │
        │  {node_a, i, mode, entries, progress},                      │
        │  %{}}                                                       │
        │────────────────────────────────────────────────────────────>│

  Healthy connected members periodically exchange summary probes. The steady
  state anti-entropy tick is therefore lightweight control-plane traffic:
  it exchanges current per-origin heads and lets actually-behind receivers
  request repair explicitly.
  Each shard keeps at most one summary probe in flight per peer and at most
  one full-sync source active at a time, so cold-start/bootstrap repair does
  not fan out into duplicate full snapshots from every eligible peer.

  Upgraded peers may also exchange live LWW replication batches:

      {:ekv, 1, :replication_batch,
       {from_node, shard, origin_node, entries}, %{}}

  This is only for hot live replication fanout. Local writes still commit one
  at a time. Batches are bounded by time/count/bytes per destination shard,
  same-origin by construction, and applied on the receiver in one dirty IO NIF
  hop and one SQLite transaction. The batch NIF preserves per-entry applied
  flags in input order, then Elixir dispatches ordered events from those
  results.


  ## Delta Sync vs Full Sync

      ┌────────────────────────────────────────────────────────────────┐
      │ Delta Sync                                                     │
      │ Condition: requester is behind a live origin stream and the    │
      │            requested range is still inside retained replay     │
      │            history                                             │
      │                                                                │
      │ Query: SELECT * FROM kv_oplog                                  │
      │        WHERE origin_node = requested_origin                    │
      │          AND origin_seq > from_seq                             │
      │        ORDER BY origin_seq LIMIT chunk_size                    │
      │                                                                │
      │ Delta replay is origin-ordered, but any live peer can relay    │
      │ retained oplog rows for that origin. Direct origin delta       │
      │ remains preferred when the origin is connected. If a known     │
      │ member origin is down/disconnected, peers immediately try      │
      │ relayed delta instead of resending the shard. Quarantine is    │
      │ immediate. Mere handshake lag is not enough to trigger full    │
      │ fallback.                                                      │
      └────────────────────────────────────────────────────────────────┘

  Live replication batching is a separate path from sync:
    - no requester-side `:sync_request`
    - no terminal sync progress settlement
    - receiver still must advance local contiguous origin progress correctly
      after the batch and preserve ordered subscriber semantics

      ┌────────────────────────────────────────────────────────────────┐
      │ Full Sync                                                      │
      │ Condition: requester is behind retained replay history,        │
      │            relayed delta cannot serve the requested range,     │
      │            or quarantined/synthetic dead-origin state must     │
      │            be repaired                                         │
      │                                                                │
      │ Query: SELECT * FROM kv WHERE (deleted_at IS NULL              │
      │          OR deleted_at > cutoff) AND key > cursor              │
      │        ORDER BY key LIMIT chunk_size                           │
      │                                                                │
      │ Sends the full current shard state plus a terminal progress    │
      │ summary so the requester can advance local applied progress    │
      │ without replaying superseded history.                          │
      │ summary. The receiver treats that final progress summary as    │
      │ authoritative replacement for its local progress table.        │
      └────────────────────────────────────────────────────────────────┘

  After receiving sync data, the receiver applies each entry through
  merge_remote_entry (LWW check), then updates progress:
    - delta sync merges the final advertised progress map
    - full sync replaces the local progress map with the final advertised map
  The receiver then acks the same final progress map back to the sender.


  ## Chunked Sync

  Both full and delta sync use cursor-based pagination to avoid loading
  the entire dataset into memory. Default chunk size is 500 entries
  (configurable via `:sync_chunk_size`).

  The sender sends one chunk, then yields to other messages via
  `send(self(), {:continue_full_sync, ...})` before sending the next.
  This prevents the shard GenServer from blocking — CAS messages,
  regular writes, and other sync operations can interleave between
  chunks.

      Sender (shard i)
        │
        │ send_full_chunk(cursor=nil)
        │   query chunk 1 (500 entries)
        │   send {:ekv, 1, :sync, {..., progress=nil}, %{}} ──> member
        │   send(self(), {:continue_full_sync, cursor="last_key"})
        │   return {:noreply, state}
        │
        │ ... process other messages ...
        │
        │ handle_info(:continue_full_sync)
        │   check member still in remote_shards (abort if gone)
        │   query chunk 2 (500 entries)
        │   send {:ekv, 1, :sync, {..., progress=nil}, %{}} ──> member
        │   send(self(), {:continue_full_sync, cursor="last_key"})
        │
        │ ... process other messages ...
        │
        │ handle_info(:continue_full_sync)
        │   query chunk 3 (< 500 entries = final)
        │   send {:ekv, 1, :sync, {..., progress=summary}, %{}} ──> member
        │

  Progress safety: intermediate chunks carry `progress=nil`. Only the final
  chunk carries a progress map. Empty final chunks are still meaningful:
    - empty delta can advance a retained origin cursor
    - empty full sync can authoritatively replace impossible remote progress

  Continuation handlers check remote_shards before each chunk. If the
  member disconnected mid-sync, the continuation silently stops.

  Safety under concurrent activity:
    - Write between chunks: LWW is idempotent. Duplicates resolved.
    - CAS between chunks: shard mailbox serialization prevents true
      same-shard races. Tentative CAS state lives in kv_paxos, while sync
      applies committed kv state. Once a ballot is accepted, later prepares
      and commits consult kv_paxos rather than stale kv rows.
    - GC between chunks: cursor-based, skips purged entries.
    - Second sync triggered: LWW makes duplicate replay safe.

  ## Replay Progress

  Each shard tracks two progress views:

    - `kv_origin_progress`
      "How far have I locally applied each origin stream?"

    - `kv_member_progress`
      "How far has peer member X applied each origin stream, based on the
      latest progress it advertised or acked?"

  Progress is exact, not monotonic-by-max semantics. If a peer
  authoritatively reports a lower progress value after full sync or restart,
  the stored cursor must be allowed to move lower. Keeping an impossible
  higher cursor causes repeated forced full syncs.


  ## Recovery Scenarios

  ### Scenario 1: Clean restart (same node, same data dir)

      Node crashes / restarts
        │
        Replica.init:
          Store.open  →  SQLite db still on disk
          open read connections
          restore ballot_counter from kv_meta
          monitor_nodes + send ekv_member_connect
        │
        Member responds with ekv_member_connect_ack
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
          monitor_nodes + send ekv_member_connect
        │
        Members have no usable progress for this new node
          → full sync: send all live entries + recent tombstones
            (expired rows skipped)
            (chunked, ~500 entries per message)
        │
        New node applies all entries via merge_remote_entry
        Replaces local progress from the sender's final advertised summary
        │
        Fully caught up

  ### Scenario 3: Network partition (2 groups can't talk)

      Before:   A ←→ B ←→ C     (fully meshed)
      Partition: {A, B} | {C}    (C isolated)

      During partition:
        - A and B replicate to each other normally
        - C writes to local SQLite only (no members in remote_shards)
        - No data is lost on either side
        - nodedown fires, C removed from A/B's remote_shards
        - CAS operations on C fail with {:error, :no_quorum}
          (cannot reach majority)
        - CAS operations on A/B succeed if A+B form a majority

      Heal:
        - nodeup fires on both sides
        - ekv_member_connect / ekv_member_connect_ack exchanged
        - Reconnect gate checks persisted down marker age:
            * age <= tombstone_ttl: proceed to sync
            * age > tombstone_ttl: quarantine member pair (no sync)
        - If sync proceeds:
            * summary exchange tells each side whether it is behind
            * behind side requests delta from the live origin when possible
            * quarantined/unrecoverable origins can trigger immediate full
            * ordinary disconnected third origins try relayed delta
              immediately and fall back to full only if replay is unavailable
            * summary-probe and sync in-flight suppression is bounded:
                - each shard suppresses duplicate probes/repairs per peer
                - stale in-flight markers expire after a short timeout
                - dropped sync requests therefore cannot freeze peer-progress
                  refresh forever
            * Both sides repair their own missed mutations independently
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
    - GC TTL expiry (`:expired` events with previous value)

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

  GC handles expiry differently by key mode:
    1. LWW keys: write a tombstone, append to oplog, broadcast delete, emit `:expired`
    2. CAS-managed keys: mark expired_at locally so `:expired` is emitted once,
       do not broadcast a delete, and hard-delete long-expired rows later


  ## Garbage Collection (EKV.GC)

  Periodic timer sends {:gc, now, tombstone_cutoff} to each shard.
  The shard handles GC inside its own process (serialized with writes).

  Each tick, six phases:

      Phase 1: Observe TTL expiry
        LWW keys → tombstone + oplog + replicated delete + `:expired`
        CAS-managed keys → mark expired_at locally, emit `:expired`
        (reads/CAS already treat expired rows as absent; no replicated
         delete for CAS-managed expiry)

      Phase 2: Purge old tombstones
        deleted_at < now - tombstone_ttl → hard delete from SQLite kv
        (tombstone_ttl default: 7 days — keeps tombstones long enough
         for partitioned nodes to learn about deletes on reconnect)

      Phase 2b: Purge long-expired CAS rows
        expires_at < now - tombstone_ttl → hard delete from SQLite kv
        (keeps expired rows around long enough for local `:expired`
         notification without turning CAS TTL expiry into a replicated delete)

      Phase 2c: Purge orphan kv_paxos rows (if CAS enabled)
        DELETE FROM kv_paxos WHERE key NOT IN (SELECT key FROM kv)
          AND accepted_counter = 0
        (cleans up paxos state for tombstone-purged keys, only if
         no in-flight accept is pending)

      Phase 3: Prune stale member progress
        Remove kv_member_progress rows for members that are neither
        currently connected nor still inside member_progress_retention_ttl
        since their last disconnect marker.
        Prevents dead/decommissioned members from anchoring replay retention
        forever while still letting short/moderate partitions reconnect by
        delta instead of forcing a later full sync. The default replay window
        is intentionally bounded (`min(tombstone_ttl, 6 hours)`) so a dead
        member does not hold oplog history for the full tombstone/quarantine
        horizon unless operators opt in.

      Phase 4: Truncate replay log
        For each origin stream, delete kv_oplog rows older than the minimum
        retained progress across currently connected members plus recently
        disconnected members still inside member_progress_retention_ttl, then
        prune orphan `kv_keyrefs`.
        (keeps replay history bounded without conflating different origins)

      Phase 5: Bump liveness
        touch_last_active updates kv_meta.last_active_at.
        Used by stale DB detection on restart.

      Phase 6: Prune fallback name-based down markers
        Delete old / over-cap kv_meta keys:
          member_down_at:name:*
        (bounds marker growth under node-name churn; primary id-based
         markers remain until successful reconnect or operator action)


  ## Stale DB Protection

  On Store.open, if an existing db file's last_active_at is older than
  tombstone_ttl - gc_interval, startup is refused by default. This prevents
  zombie resurrection: other members will have GC'd tombstones for entries deleted
  while the node was away, so a stale db would never learn about them.
  Operators must then either wipe that node's data dir so it rebuilds from
  members, or explicitly allow stale startup when intentionally trusting the
  old on-disk cluster state.


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
          keyref_upsert:   reference,   #   ensure replay-key dictionary entry
          oplog_insert:    reference    #   oplog append
        },
        readers:        [{db, stmt}],   # per-scheduler read connections
        tombstone_ttl:  integer,        # ms
        partition_ttl_policy: :quarantine | :ignore,
        remote_shards:  %{node => pid}, # confirmed live member shards
        member_down_at:   %{marker_key => down_since_ms}, # cached kv_meta markers
        quarantined_members: MapSet.t(node()),         # blocked members
        # CAS fields (nil if CAS not configured):
        node_id:        string | nil,   # this node's CAS identity
        cluster_size:   integer | nil,  # total CAS participants
        lww_ts_counter: integer,        # monotonic local LWW timestamp floor
        local_origin_seq: integer,      # this node's local origin replay cursor
        local_progress: %{origin => seq}, # local contiguous applied progress
        ballot_counter: integer,        # monotonic, persisted in kv_meta
        member_node_ids:  %{node => string},  # Erlang node → CAS node_id
        remote_member_progress: %{node => %{origin => seq}},
        summary_probe_inflight: %{node => monotonic_ms}, # peers with a summary probe in flight; stale entries expire
        sync_inflight:  %{node => monotonic_ms}, # remote members currently servicing our repair requests; stale entries expire
        delta_origin_inflight: %{origin => {node, monotonic_ms}}, # one delta repair per missing origin at a time
        delta_sync_storm_started_at_ms: integer | nil,
        delta_sync_storm_count: non_neg_integer,
        delta_sync_storm_entries: non_neg_integer,
        delta_sync_storm_peers: MapSet.t(node),
        delta_sync_storm_logged?: boolean,
        full_sync_inflight: node() | nil, # single full-bootstrap source for this shard
        pending_cas:    %{ref => op},   # in-flight CAS operations
        quorum_waiters: %{ref => waiter} # pending await_quorum callers
      }


  ## Message Reference

  Member-to-member wire traffic uses:

    {:ekv, 1, kind, payload, meta}

  Required fields live in `payload`. Optional/extensible fields live in
  `meta`. `origin_seq` is required in the v1 replication contract, so it is
  part of payload for `:replication_batch` entries and `:cas_committed`.

  Wire protocol v1 kinds:
    :replication_batch   {from_node, shard_index, origin_node, entries}
      entries: [{key, value_binary, timestamp, origin_seq, expires_at, deleted_at}]
    :member_connect      {pid, shard_index, num_shards, progress_summary, node_id}
    :member_connect_ack  {pid, shard_index, num_shards, progress_summary, node_id}
    :summary_probe       {pid, shard_index, progress_summary}
    :summary_reply       {pid, shard_index, progress_summary}
    :sync_request        {pid, shard_index, :full | {:delta, origin_node, from_seq}}
    :sync                {from_node, shard_index, mode, entries, progress}
      entries: [{key, value_binary, timestamp, origin_node, origin_seq, expires_at, deleted_at}]
      progress: nil for intermediate chunks, final progress map for terminal chunk
    :progress_ack        {pid, shard_index, mode, progress_summary}
    :prepare             {ref, proposer_pid, key, ballot_c, ballot_n, shard}
    :accept              {ref, proposer_pid, key, ballot_c, ballot_n, entry_tuple, shard}
    :cas_committed       {key, ballot_c, ballot_n, entry_tuple, shard, origin_node, origin_seq}
    :promise             {ref, pid, node_id, acc_c, acc_n, kv_row}
    :nack                {ref, pid, node_id, promised_c, promised_n}
    :accepted            {ref, pid, node_id}
    :accept_nack         {ref, pid, node_id}

  Current `meta` usage:
    - handshake messages advertise
      `%{features: %{live_progress: true, wire_compression: true}}`
      where `live_progress` means the peer supports progress summaries and
      sync-settlement progress exchange in the v1 contract; it is not a
      promise of per-write live progress-ack traffic
    - `:sync_request` may include `%{explicit_full_reason: ...}` for requester-
      driven full syncs while keeping the payload shape backward compatible
    - replication/control messages keep `meta` empty unless an optional feature requires it

  Internally, Replica still uses the raw tuples below after decoding the wire
  envelope. The local live-replication queue also uses raw `{:ekv_put, ...}`
  / `{:ekv_delete, ...}` entry tuples internally before they are flushed into
  `{:ekv_replication_batch, ...}`:
    {:ekv_replication_batch, ...}
    {:ekv_member_connect, ...}
    {:ekv_member_connect_ack, ...}
    {:ekv_sync, ...}
    {:ekv_summary_probe, ...}
    {:ekv_summary_reply, ...}
    {:ekv_sync_request, ...}
    {:ekv_prepare, ...}
    {:ekv_accept, ...}
    {:ekv_cas_committed, ...}
    {:ekv_promise, ...}
    {:ekv_nack, ...}
    {:ekv_accepted, ...}
    {:ekv_accept_nack, ...}

  Sync continuations (self-messages for chunking):
    {:continue_full_sync, node, last_key, cutoff, my_seq, chunk_size}
    {:continue_delta_sync, node, last_seq, my_seq, chunk_size}

  CAS internal (self-messages):
    {:cas_timeout, ref}
    {:cas_retry, ref, key, operation}
    {:await_quorum_timeout, ref}

  GC (from EKV.GC timer):
    {:gc, now_nanoseconds, tombstone_cutoff_nanoseconds}

  Subscriber dispatch (shard → SubDispatcher):
    {:dispatch, [%EKV.Event{}]}

  All member messages are sent to the counterpart shard by registered name:
    send({:"#{name}_ekv_replica_#{shard}", target_node}, message)

  There is no gossip or re-broadcast. Replication is direct: the node
  that performs a write sends to all known members exactly once.
  """

  use GenServer

  require Logger

  alias EKV.Replica
  alias EKV.Store

  @member_down_id_prefix "member_down_at:id:"
  @member_down_name_prefix "member_down_at:name:"
  @member_down_name_min_retention_ms :timer.hours(24 * 30)
  @member_down_name_max_entries 4096
  @member_seen_hint_ttl_ms :timer.minutes(15)
  @member_seen_refresh_window_ms :timer.minutes(1)
  @member_seen_max_entries 8192
  @unknown_member_origin_startup_grace_ms :timer.minutes(5)
  @wire_protocol_version 1
  @wire_compressed_tag :ekv_wire_compressed
  @wire_feature_live_progress :live_progress
  @wire_feature_compression :wire_compression
  @wire_feature_observer :observer
  @local_request_tag :ekv_local_request
  @local_reply_tag :ekv_local_reply
  @summary_probe_timeout_ms :timer.minutes(2)
  @sync_inflight_timeout_ms :timer.minutes(2)
  defstruct [
    :name,
    :shard_index,
    :num_shards,
    :db,
    :data_dir,
    :stmts,
    :transport,
    :tombstone_ttl,
    partition_ttl_policy: :quarantine,
    readers: [],
    remote_shards: %{},
    # Marker key -> down_since_ms (wall clock cache for kv_meta)
    member_down_at: %{},
    # node_id -> last persisted seen_at_ms
    member_seen_at: %{},
    quarantined_members: MapSet.new(),
    # CAS fields
    node_id: nil,
    cluster_size: nil,
    cas_voter?: true,
    lww_ts_counter: 0,
    local_max_seq: 0,
    local_origin_seq: 0,
    local_progress: %{},
    ballot_counter: 0,
    wire_compression_threshold: nil,
    member_node_ids: %{},
    remote_member_progress: %{},
    remote_member_hwms: %{},
    remote_features: %{},
    summary_probe_inflight: %{},
    started_at_ms: nil,
    local_write_batch_max_entries: 32,
    local_write_batch_max_bytes: 256 * 1024,
    replication_batch_flush_ms: 3,
    replication_batch_max_entries: 64,
    replication_batch_max_bytes: 256 * 1024,
    replication_batches: %{},
    sync_inflight: %{},
    delta_origin_inflight: %{},
    delta_sync_storm_started_at_ms: nil,
    delta_sync_storm_count: 0,
    delta_sync_storm_entries: 0,
    delta_sync_storm_peers: MapSet.new(),
    delta_sync_storm_logged?: false,
    full_sync_inflight: nil,
    pending_cas: %{},
    quorum_waiters: %{},
    handoff_node: nil
  ]

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    shard_index = Keyword.fetch!(opts, :shard_index)
    GenServer.start_link(__MODULE__, opts, name: shard_name(name, shard_index))
  end

  def local_request(shard_name, request, timeout) when is_atom(shard_name) do
    case GenServer.whereis(shard_name) do
      pid when is_pid(pid) ->
        do_local_request(pid, shard_name, request, timeout)

      nil ->
        exit({:noproc, {GenServer, :call, [shard_name, request, timeout]}})
    end
  end

  def shard_name(name, shard_index), do: :"#{name}_ekv_replica_#{shard_index}"

  def shard_index_for(key, num_shards) do
    :erlang.phash2(key, num_shards)
  end

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    name = Keyword.fetch!(opts, :name)
    shard_index = Keyword.fetch!(opts, :shard_index)
    num_shards = Keyword.fetch!(opts, :num_shards)
    data_dir = Keyword.fetch!(opts, :data_dir)

    config = EKV.Supervisor.get_config(name)

    transport_config = Map.get(config, :transport, EKV.Transport.default_config())

    case EKV.Transport.init(transport_config) do
      {:ok, transport} ->
        case Store.open(
               data_dir,
               shard_index,
               config.tombstone_ttl,
               num_shards,
               config.gc_interval, allow_stale_startup: config[:allow_stale_startup] || false) do
          {:ok, db} ->
            init_with_open_db(db, name, shard_index, num_shards, data_dir, config, transport)

          {:error, {:stale_db, info} = reason} ->
            maybe_log_stale_db_failure(config, name, shard_index, info)
            {:stop, reason}
        end

      {:error, reason} ->
        {:stop, {:transport_init_failed, reason}}
    end
  end

  defp init_with_open_db(db, name, shard_index, num_shards, data_dir, config, transport) do
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

    # Local eventual-write counter — seed from the current shard watermark so
    # future same-node writes never reuse an existing committed timestamp.
    lww_ts_counter = max(System.system_time(:nanosecond), Store.max_timestamp(db) || 0)
    local_max_seq = Store.max_seq(db)
    local_origin_id = config.node_id || Atom.to_string(node())
    local_origin_seq = Store.max_origin_seq(db, local_origin_id)

    local_progress =
      Store.local_progress_summary(db) |> Map.put(local_origin_id, local_origin_seq)

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

    state = %Replica{
      name: name,
      shard_index: shard_index,
      num_shards: num_shards,
      db: db,
      data_dir: data_dir,
      stmts: stmts,
      transport: transport,
      readers: readers,
      tombstone_ttl: config.tombstone_ttl,
      partition_ttl_policy: config.partition_ttl_policy,
      node_id: config.node_id,
      cluster_size: config.cluster_size,
      cas_voter?: Map.get(config, :cas_voter, true),
      wire_compression_threshold: config[:wire_compression_threshold],
      local_write_batch_max_entries: config[:local_write_batch_max_entries] || 32,
      local_write_batch_max_bytes: config[:local_write_batch_max_bytes] || 256 * 1024,
      replication_batch_flush_ms: config[:replication_batch_flush_ms] || 3,
      replication_batch_max_entries: config[:replication_batch_max_entries] || 64,
      replication_batch_max_bytes: config[:replication_batch_max_bytes] || 256 * 1024,
      lww_ts_counter: lww_ts_counter,
      local_max_seq: local_max_seq,
      local_origin_seq: local_origin_seq,
      local_progress: local_progress,
      ballot_counter: ballot_counter,
      started_at_ms: System.system_time(:millisecond)
    }

    :net_kernel.monitor_nodes(true)

    log_once(state, fn -> "#{log_prefix(state)} started (shards=#{num_shards})" end)

    # Discover members on all known nodes
    for remote_node <- Node.list() do
      send_to_member(state, remote_node, member_connect_message(state))
    end

    schedule_anti_entropy_tick(state)

    {:ok, state}
  end

  defp maybe_log_stale_db_failure(config, name, shard_index, info) do
    if config.log do
      detail =
        case info.age_ms do
          nil -> "missing last_active_at metadata"
          age_ms -> "age=#{age_ms}ms exceeds threshold=#{info.threshold_ms}ms"
        end

      require Logger

      Logger.error(
        "[EKV #{name}] shard #{shard_index} refusing stale database startup at #{info.path}: " <>
          "#{detail}. Wipe the data dir to rebuild from members, or set " <>
          "`allow_stale_startup: true` to trust the on-disk data."
      )
    end
  end

  @impl true
  def terminate(_reason, %Replica{} = state) do
    state = flush_all_replication_batches(state)
    drain_pending_local_requests_with_reply({:error, :shutting_down})

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
  def handle_call(request, _from, %Replica{handoff_node: handoff_node} = state)
      when handoff_node != nil do
    shard_name = shard_name(state.name, state.shard_index)

    try do
      result = GenServer.call({shard_name, handoff_node}, request, 5_000)
      cb_reply(result, state)
    catch
      :exit, _ ->
        cb_reply({:error, :shutting_down}, state)
    end
  end

  # =====================================================================
  # Write calls
  # =====================================================================

  def handle_call({:put, key, value_binary, opts}, _from, %Replica{} = state) do
    {reply, state} = handle_single_put_request(state, key, value_binary, opts)
    cb_reply(reply, state)
  end

  def handle_call({:delete, key}, _from, %Replica{} = state) do
    {reply, state} = handle_single_delete_request(state, key)
    cb_reply(reply, state)
  end

  # =====================================================================
  # CAS write calls
  # =====================================================================

  def handle_call({:cas_put, key, value_binary, expected_vsn, opts}, from, %Replica{} = state) do
    operation = {:cas_put, expected_vsn, value_binary, opts}
    cb_noreply(start_cas(state, key, operation, {:genserver, from}, cas_deadline_from_opts(opts)))
  end

  def handle_call(
        {:observer_cas_put, key, value_binary, expected_vsn, opts},
        from,
        %Replica{} = state
      ) do
    operation = {:cas_put, expected_vsn, value_binary, opts}

    cb_noreply(
      start_cas(
        state,
        key,
        operation,
        {:genserver, from},
        cas_deadline_from_opts(opts),
        :observer_write
      )
    )
  end

  def handle_call({:cas_delete, key, expected_vsn, opts}, from, %Replica{} = state) do
    operation = {:cas_delete, expected_vsn, opts}
    cb_noreply(start_cas(state, key, operation, {:genserver, from}, cas_deadline_from_opts(opts)))
  end

  def handle_call({:observer_cas_delete, key, expected_vsn, opts}, from, %Replica{} = state) do
    operation = {:cas_delete, expected_vsn, opts}

    cb_noreply(
      start_cas(
        state,
        key,
        operation,
        {:genserver, from},
        cas_deadline_from_opts(opts),
        :observer_write
      )
    )
  end

  def handle_call({:update, key, fun, opts}, from, %Replica{} = state) do
    retries = Keyword.get(opts, :retries, 5)
    operation = {:update, fun, opts, retries}
    cb_noreply(start_cas(state, key, operation, {:genserver, from}, cas_deadline_from_opts(opts)))
  end

  def handle_call({:observer_update, key, fun, opts}, from, %Replica{} = state) do
    retries = Keyword.get(opts, :retries, 5)
    operation = {:update, fun, opts, retries}

    cb_noreply(
      start_cas(
        state,
        key,
        operation,
        {:genserver, from},
        cas_deadline_from_opts(opts),
        :observer_write
      )
    )
  end

  def handle_call({:cas_read, key, opts}, from, %Replica{} = state) do
    retries = Keyword.get(opts, :retries, 5)
    operation = {:cas_read, opts, retries}
    cb_noreply(start_cas(state, key, operation, {:genserver, from}, cas_deadline_from_opts(opts)))
  end

  def handle_call({:observer_cas_read, key, opts}, from, %Replica{} = state) do
    retries = Keyword.get(opts, :retries, 5)
    operation = {:cas_read, opts, retries}

    cb_noreply(
      start_cas(
        state,
        key,
        operation,
        {:genserver, from},
        cas_deadline_from_opts(opts),
        :observer_read
      )
    )
  end

  def handle_call(
        {:apply_observer_commit, key, ballot_c, ballot_n, entry_tuple, origin_node, origin_seq},
        _from,
        %Replica{} = state
      ) do
    {reply, state} =
      handle_apply_observer_commit_request(
        state,
        key,
        ballot_c,
        ballot_n,
        entry_tuple,
        origin_node,
        origin_seq
      )

    cb_reply(reply, state)
  end

  def handle_call({:await_quorum, timeout_ms}, from, %Replica{} = state) do
    case handle_await_quorum_request(state, {:genserver, from}, timeout_ms) do
      {:reply, reply, state} -> cb_reply(reply, state)
      {:noreply, state} -> cb_noreply(state)
    end
  end

  defp handle_single_put_request(%Replica{} = state, key, value_binary, opts) do
    %{db: db, stmts: stmts} = state
    {now, state} = next_lww_ts(state)
    origin_node = local_origin_id(state)
    ttl = Keyword.get(opts, :ttl)
    expires_at = if ttl, do: now + ttl * 1_000_000

    case Store.write_entry(
           db,
           stmts.kv_upsert,
           stmts.keyref_upsert,
           stmts.oplog_insert,
           key,
           value_binary,
           now,
           origin_node,
           expires_at,
           nil,
           nil,
           true,
           true
         ) do
      {:ok, true, origin_seq, local_progress_seq} ->
        state =
          state
          |> set_local_origin_seq(origin_seq)
          |> merge_local_progress_seq(origin_node, local_progress_seq)
          |> replicate_live_to_members(
            {:ekv_put, key, value_binary, now, origin_node, origin_seq, expires_at}
          )

        dispatch_events(state, [
          %EKV.Event{type: :put, key: key, value: :erlang.binary_to_term(value_binary)}
        ])

        {:ok, state}

      {:ok, false, _origin_seq, local_progress_seq} ->
        {:ok, merge_local_progress_seq(state, origin_node, local_progress_seq)}

      {:ok, false} ->
        {:ok, state}

      {:error, :cas_managed_key} ->
        {{:error, :cas_managed_key}, state}
    end
    |> normalize_local_write_result()
  end

  defp handle_single_delete_request(%Replica{} = state, key) do
    %{db: db, stmts: stmts} = state
    {now, %Replica{} = state} = next_lww_ts(state)
    origin_node = local_origin_id(state)
    prev_value = if has_subscribers?(state), do: read_previous_value(state, key)

    case Store.write_entry(
           db,
           stmts.kv_upsert,
           stmts.keyref_upsert,
           stmts.oplog_insert,
           key,
           nil,
           now,
           origin_node,
           nil,
           now,
           nil,
           true,
           true
         ) do
      {:ok, true, origin_seq, local_progress_seq} ->
        state =
          state
          |> set_local_origin_seq(origin_seq)
          |> merge_local_progress_seq(origin_node, local_progress_seq)
          |> replicate_live_to_members({:ekv_delete, key, now, origin_node, origin_seq})

        dispatch_events(state, [%EKV.Event{type: :delete, key: key, value: prev_value}])
        {:ok, state}

      {:ok, false, _origin_seq, local_progress_seq} ->
        {:ok, merge_local_progress_seq(state, origin_node, local_progress_seq)}

      {:ok, false} ->
        {:ok, state}

      {:error, :cas_managed_key} ->
        {{:error, :cas_managed_key}, state}
    end
    |> normalize_local_write_result()
  end

  defp normalize_local_write_result({reply, %Replica{} = state})
       when reply in [:ok, {:error, :cas_managed_key}],
       do: {reply, state}

  defp normalize_local_write_result({:ok, %Replica{} = state}), do: {:ok, state}

  defp handle_apply_observer_commit_request(
         %Replica{} = state,
         key,
         ballot_c,
         ballot_n,
         entry_tuple,
         origin_node,
         origin_seq
       ) do
    origin_node = normalize_origin_node(origin_node)
    gap? = origin_gap?(state, origin_node, origin_seq)
    {state, applied?} = apply_cas_commit(state, key, ballot_c, ballot_n, entry_tuple, origin_seq)
    {:ok, maybe_request_origin_gap_repair(state, origin_node, origin_seq, gap? and applied?)}
  end

  defp handle_await_quorum_request(%Replica{} = state, from, timeout_ms) do
    case quorum_status(state) do
      :ok ->
        {:reply, :ok, state}

      {:error, :cluster_overflow} = error ->
        {:reply, error, state}

      {:error, :cas_not_configured} = error ->
        {:reply, error, state}

      {:error, :no_quorum} ->
        if timeout_ms == 0 do
          {:reply, {:error, :timeout}, state}
        else
          ref = make_ref()
          timer = Process.send_after(self(), {:await_quorum_timeout, ref}, timeout_ms)
          waiter = %{from: from, timer: timer}
          {:noreply, %{state | quorum_waiters: Map.put(state.quorum_waiters, ref, waiter)}}
        end
    end
  end

  # =====================================================================
  # Blue-green handoff
  # =====================================================================

  @impl true
  def handle_info({:ekv_handoff_request, ref, new_node, caller_pid}, %Replica{} = state) do
    handoff_started_ms = System.monotonic_time(:millisecond)

    mailbox_len =
      case Process.info(self(), :message_queue_len) do
        {:message_queue_len, len} when is_integer(len) -> len
        _ -> :unknown
      end

    log(state, fn ->
      "#{log_prefix_shard(state)} handoff requested by #{new_node} " <>
        "mailbox_len=#{mailbox_len} pending_cas=#{map_size(state.pending_cas)}"
    end)

    EKV.BlueGreenMarker.mark_handoff_performed(state.name)
    EKV.MemberPresence.leave(state.name)

    # 1. Drain pending CAS ops
    for {_ref, op} <- state.pending_cas do
      cancel_timer(op.timer)
      reply_cas_error(op, {:error, :shutting_down})
    end

    %Replica{} = state = fail_quorum_waiters(state, {:error, :shutting_down})
    %Replica{} = state = drain_pending_local_requests(state)
    %Replica{} = state = flush_all_replication_batches(state)

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
      elapsed_ms = System.monotonic_time(:millisecond) - handoff_started_ms

      "#{log_prefix_shard(state)} handoff to #{new_node} complete, " <>
        "elapsed_ms=#{elapsed_ms}, proxy mode"
    end)

    # 6. Enter proxy mode (readers stay alive until terminate)
    cb_noreply(%{
      state
      | db: nil,
        stmts: nil,
        pending_cas: %{},
        quorum_waiters: %{},
        handoff_node: new_node
    })
  end

  def handle_info({@local_request_tag, caller_pid, ref, request}, %Replica{} = state) do
    cb_noreply(handle_local_request_message(state, caller_pid, ref, request))
  end

  # In handoff mode: drop all messages (replication, GC, nodeup/down, CAS).
  # Readers stay alive for old VM reads. Replica members rediscover the incoming
  # node via nodeup; new clients bind to the incoming member via MemberPresence.
  def handle_info(_msg, %Replica{handoff_node: handoff_node} = state)
      when handoff_node != nil do
    cb_noreply(state)
  end

  # =====================================================================
  # Replication receive
  # =====================================================================

  def handle_info({:ekv, @wire_protocol_version, kind, payload, meta}, %Replica{} = state) do
    case decode_wire_message(kind, payload, meta) do
      {:ok, message} ->
        handle_wire_message(state, message)

      :ignore ->
        cb_noreply(state)
    end
  end

  def handle_info({:ekv, version, kind, _payload, _meta}, %Replica{} = state)
      when is_integer(version) do
    log_verbose(state, fn ->
      "#{log_prefix_shard(state)} ignoring unsupported wire version #{version} kind=#{inspect(kind)}"
    end)

    cb_noreply(state)
  end

  def handle_info(
        {:ekv_replication_batch, from_node, remote_shard, origin_node, entries},
        %Replica{} = state
      )
      when remote_shard == state.shard_index do
    origin_node = normalize_origin_node(origin_node)
    entries = wire_decompress_replication_batch_entries(entries)
    state = apply_replication_batch(state, from_node, origin_node, entries)
    cb_noreply(take_priority_turn(state))
  end

  def handle_info({:flush_replication_batch, remote_node}, %Replica{} = state) do
    cb_noreply(flush_replication_batch(state, remote_node))
  end

  # =====================================================================
  # Member sync protocol
  # =====================================================================

  def handle_info(
        {:ekv_member_connect, remote_pid, remote_shard, remote_num_shards, remote_progress,
         remote_node_id},
        %Replica{} = state
      ) do
    cb_noreply(
      do_member_connect(
        state,
        remote_pid,
        remote_shard,
        remote_num_shards,
        remote_progress,
        remote_node_id,
        MapSet.new()
      )
    )
  end

  def handle_info(
        {:ekv_member_connect, remote_pid, remote_shard, remote_num_shards, remote_progress,
         remote_node_id, remote_features},
        %Replica{} = state
      ) do
    cb_noreply(
      do_member_connect(
        state,
        remote_pid,
        remote_shard,
        remote_num_shards,
        remote_progress,
        remote_node_id,
        remote_features
      )
    )
  end

  def handle_info(
        {:ekv_member_connect_ack, remote_pid, remote_shard, remote_num_shards, remote_progress,
         remote_node_id},
        %Replica{} = state
      ) do
    cb_noreply(
      do_member_connect_ack(
        state,
        remote_pid,
        remote_shard,
        remote_num_shards,
        remote_progress,
        remote_node_id,
        MapSet.new()
      )
    )
  end

  def handle_info(
        {:ekv_member_connect_ack, remote_pid, remote_shard, remote_num_shards, remote_progress,
         remote_node_id, remote_features},
        %Replica{} = state
      ) do
    cb_noreply(
      do_member_connect_ack(
        state,
        remote_pid,
        remote_shard,
        remote_num_shards,
        remote_progress,
        remote_node_id,
        remote_features
      )
    )
  end

  def handle_info({:ekv_sync_request, remote_pid, remote_shard, request}, %Replica{} = state)
      when remote_shard == state.shard_index do
    remote_node = node(remote_pid)

    cond do
      state.handoff_node != nil ->
        cb_noreply(state)

      not Map.has_key?(state.remote_shards, remote_node) ->
        cb_noreply(state)

      MapSet.member?(state.quarantined_members, remote_node) ->
        cb_noreply(state)

      true ->
        cb_noreply(serve_sync_request(state, remote_node, request))
    end
  end

  def handle_info({:ekv_sync, from_node, _shard, mode, entries, progress}, %Replica{} = state) do
    %{shard_index: shard, db: db, num_shards: num_shards} = state
    state = touch_sync_inflight(state, from_node)

    log_verbose(state, fn ->
      "#{log_prefix_shard(state)} ekv_sync from #{from_node} (#{length(entries)} entries)"
    end)

    has_subs = has_subscribers?(state)

    {state, sync_events} =
      Enum.reduce(entries, {state, []}, fn {key, value_binary, timestamp, origin_node, origin_seq,
                                            expires_at, deleted_at},
                                           {state, acc} ->
        origin_node = normalize_origin_node(origin_node)

        if shard_index_for(key, num_shards) == shard do
          prev_value = if deleted_at && has_subs, do: read_previous_value(state, key)

          {applied, state} =
            apply_sync_entry(
              state,
              mode,
              key,
              value_binary,
              timestamp,
              origin_node,
              origin_seq,
              expires_at,
              deleted_at
            )

          if applied do
            event =
              if deleted_at,
                do: %EKV.Event{type: :delete, key: key, value: prev_value},
                else: %EKV.Event{
                  type: :put,
                  key: key,
                  value: :erlang.binary_to_term(value_binary)
                }

            {state, [event | acc]}
          else
            {state, acc}
          end
        else
          {state, acc}
        end
      end)

    dispatch_events(state, Enum.reverse(sync_events))

    progress = normalize_progress_summary(progress)

    {state, replied?} =
      if progress != %{} do
        :ok = Store.merge_local_progress_summary(db, progress)
        state = replace_local_progress_summary(state, progress)
        ack_progress = progress_ack_summary(state, mode, progress)

        send_to_member(
          state,
          from_node,
          {:ekv_progress_ack, self(), state.shard_index, mode, ack_progress}
        )

        {state, true}
      else
        {state, false}
      end

    state =
      if replied? do
        state = clear_sync_inflight(state, from_node)

        if mode == :full do
          maybe_request_repairs(state)
        else
          maybe_request_repair(
            state,
            from_node,
            Map.get(state.remote_member_progress, from_node, %{})
          )
        end
      else
        state
      end

    cb_noreply(state)
  end

  def handle_info(
        {:ekv_progress_ack, remote_pid, remote_shard, mode, progress},
        %Replica{} = state
      )
      when remote_shard == state.shard_index do
    remote_node = node(remote_pid)
    progress = normalize_progress_summary(progress)

    state =
      case mode do
        :full -> replace_remote_member_progress(state, remote_node, progress)
        :delta -> merge_remote_member_progress(state, remote_node, progress)
      end

    cb_noreply(state)
  end

  # =====================================================================
  # Node up/down
  # =====================================================================

  def handle_info({:nodeup, remote_node}, %Replica{} = state) do
    case maybe_allow_member_reconnect(state, remote_node) do
      {:quarantine, %Replica{} = state} ->
        cb_noreply(state)

      {:ok, %Replica{} = state} ->
        send_to_member(state, remote_node, member_connect_message(state))

        cb_noreply(state)
    end
  end

  def handle_info({:nodedown, dead_node}, %Replica{} = state) do
    log_once(state, fn -> "#{log_prefix(state)} nodedown #{dead_node} (data preserved)" end)

    dead_node_id = Map.get(state.member_node_ids, dead_node)

    state = %{
      state
      | remote_shards: Map.delete(state.remote_shards, dead_node),
        member_node_ids: Map.delete(state.member_node_ids, dead_node),
        remote_member_progress: Map.delete(state.remote_member_progress, dead_node),
        remote_member_hwms: Map.delete(state.remote_member_hwms, dead_node),
        remote_features: Map.delete(state.remote_features, dead_node),
        summary_probe_inflight: Map.delete(state.summary_probe_inflight, dead_node)
    }

    state = clear_sync_inflight(state, dead_node)
    state = drop_replication_batch(state, dead_node)

    state = mark_member_down(state, dead_node, dead_node_id)
    # Check if any pending CAS ops lost quorum
    new_state =
      state
      |> fail_pending_cas_if_no_quorum()
      |> maybe_reply_to_quorum_waiters()

    cb_noreply(new_state)
  end

  def handle_info({:ekv_transport_down, remote_node, reason}, %Replica{} = state) do
    log_once(state, fn ->
      "#{log_prefix(state)} transport_down #{remote_node}: #{inspect(reason)}"
    end)

    handle_info({:nodedown, remote_node}, state)
  end

  # =====================================================================
  # Process DOWN (remote shard died)
  # =====================================================================

  def handle_info({:DOWN, _mref, :process, pid, _reason}, %Replica{} = state) do
    remote_node = node(pid)

    if Map.get(state.remote_shards, remote_node) == pid do
      log_verbose(state, fn ->
        "#{log_prefix_shard(state)} remote_shard_down #{remote_node} (data preserved)"
      end)

      remote_node_id = Map.get(state.member_node_ids, remote_node)

      state = %{
        state
        | remote_shards: Map.delete(state.remote_shards, remote_node),
          member_node_ids: Map.delete(state.member_node_ids, remote_node),
          remote_member_progress: Map.delete(state.remote_member_progress, remote_node),
          remote_member_hwms: Map.delete(state.remote_member_hwms, remote_node),
          remote_features: Map.delete(state.remote_features, remote_node),
          summary_probe_inflight: Map.delete(state.summary_probe_inflight, remote_node)
      }

      state = clear_sync_inflight(state, remote_node)
      state = drop_replication_batch(state, remote_node)

      new_state =
        state
        |> mark_member_down(remote_node, remote_node_id)
        |> fail_pending_cas_if_no_quorum()
        |> maybe_reply_to_quorum_waiters()

      cb_noreply(new_state)
    else
      cb_noreply(state)
    end
  end

  def handle_info({:await_quorum_timeout, ref}, %Replica{} = state) do
    case Map.pop(state.quorum_waiters, ref) do
      {nil, _waiters} ->
        cb_noreply(state)

      {%{from: from}, waiters} ->
        reply_local_request(from, {:error, :timeout})
        cb_noreply(%{state | quorum_waiters: waiters})
    end
  end

  # =====================================================================
  # CAS Acceptor handlers (remote proposer sends to us)
  # =====================================================================

  def handle_info(
        {:ekv_prepare, ref, proposer_pid, key, ballot_c, ballot_n, _shard},
        %Replica{} = state
      ) do
    if not local_cas_voter?(state) do
      send(
        proposer_pid,
        wire_encode_message(
          state,
          node(proposer_pid),
          {:ekv_nack, ref, self(), state.node_id, 0, ""}
        )
      )

      cb_noreply(state)
    else
      %{db: db} = state

      case Store.paxos_prepare(db, key, ballot_c, ballot_n) do
        {:ok, :promise, acc_c, acc_n, kv_row} ->
          send(
            proposer_pid,
            wire_encode_message(
              state,
              node(proposer_pid),
              {:ekv_promise, ref, self(), state.node_id, acc_c, acc_n, kv_row}
            )
          )

        {:ok, :nack, prom_c, prom_n} ->
          send(
            proposer_pid,
            wire_encode_message(
              state,
              node(proposer_pid),
              {:ekv_nack, ref, self(), state.node_id, prom_c, prom_n}
            )
          )
      end

      cb_noreply(state)
    end
  end

  def handle_info(
        {:ekv_accept, ref, proposer_pid, key, ballot_c, ballot_n, entry_tuple, _shard},
        %Replica{} = state
      ) do
    if not local_cas_voter?(state) do
      send(
        proposer_pid,
        wire_encode_message(
          state,
          node(proposer_pid),
          {:ekv_accept_nack, ref, self(), state.node_id}
        )
      )

      cb_noreply(state)
    else
      %{db: db} = state
      entry_tuple = wire_decompress_entry_tuple(entry_tuple)

      {_key, value_binary, timestamp, origin_node_str, expires_at, deleted_at} = entry_tuple
      value_args = [value_binary, timestamp, origin_node_str, expires_at, deleted_at]

      # Write to kv_paxos only — no kv write, no oplog, no events.
      # The proposer will send {:ekv_cas_committed, ..., entry_tuple, ...} after quorum.
      case Store.paxos_accept(db, key, ballot_c, ballot_n, value_args) do
        {:ok, true} ->
          send(
            proposer_pid,
            wire_encode_message(
              state,
              node(proposer_pid),
              {:ekv_accepted, ref, self(), state.node_id}
            )
          )

        {:ok, false} ->
          send(
            proposer_pid,
            wire_encode_message(
              state,
              node(proposer_pid),
              {:ekv_accept_nack, ref, self(), state.node_id}
            )
          )
      end

      cb_noreply(state)
    end
  end

  # CAS commit notification carries committed entry tuple.
  def handle_info(
        {:ekv_cas_committed, key, ballot_c, ballot_n, entry_tuple, _shard, origin_node,
         origin_seq},
        %Replica{} = state
      ) do
    origin_node = normalize_origin_node(origin_node)
    gap? = origin_gap?(state, origin_node, origin_seq)
    entry_tuple = wire_decompress_entry_tuple(entry_tuple)
    {state, applied?} = apply_cas_commit(state, key, ballot_c, ballot_n, entry_tuple, origin_seq)

    state = maybe_request_origin_gap_repair(state, origin_node, origin_seq, gap? and applied?)

    cb_noreply(state)
  end

  # =====================================================================
  # CAS Proposer response handlers (responses from acceptors)
  # =====================================================================

  def handle_info(
        {:ekv_promise, ref, _pid, remote_node_id, acc_c, acc_n, kv_row},
        %Replica{} = state
      ) do
    case Map.get(state.pending_cas, ref) do
      nil ->
        cb_noreply(state)

      %{phase: :prepare} = op ->
        if MapSet.member?(op.responded, remote_node_id) do
          cb_noreply(state)
        else
          op = %{
            op
            | promises: [{remote_node_id, acc_c, acc_n, kv_row} | op.promises],
              responded: MapSet.put(op.responded, remote_node_id)
          }

          if length(op.promises) >= op.quorum do
            cb_noreply(enter_accept_phase(state, ref, op))
          else
            cb_noreply(%{state | pending_cas: Map.put(state.pending_cas, ref, op)})
          end
        end

      _ ->
        cb_noreply(state)
    end
  end

  def handle_info({:ekv_nack, ref, _pid, remote_node_id, _prom_c, _prom_n}, %Replica{} = state) do
    case Map.get(state.pending_cas, ref) do
      nil ->
        cb_noreply(state)

      %{phase: :prepare} = op ->
        if MapSet.member?(op.responded, remote_node_id) do
          cb_noreply(state)
        else
          op = %{op | nacks: op.nacks + 1, responded: MapSet.put(op.responded, remote_node_id)}

          max_possible_promises = alive_node_id_count(state) - op.nacks

          if max_possible_promises < op.quorum do
            # Can't reach quorum — fail or retry
            cb_noreply(handle_cas_failure(state, ref, op))
          else
            cb_noreply(%{state | pending_cas: Map.put(state.pending_cas, ref, op)})
          end
        end

      _ ->
        cb_noreply(state)
    end
  end

  def handle_info({:ekv_accepted, ref, _pid, remote_node_id}, %Replica{} = state) do
    case Map.get(state.pending_cas, ref) do
      nil ->
        cb_noreply(state)

      %{phase: :accept} = op ->
        if MapSet.member?(op.responded, remote_node_id) do
          cb_noreply(state)
        else
          accepts = MapSet.put(op.accepts, remote_node_id)
          responded = MapSet.put(op.responded, remote_node_id)
          op = %{op | accepts: accepts, responded: responded}

          if MapSet.size(accepts) >= op.quorum do
            # Accept quorum reached — commit
            cb_noreply(commit_cas(state, ref, op))
          else
            cb_noreply(%{state | pending_cas: Map.put(state.pending_cas, ref, op)})
          end
        end

      _ ->
        cb_noreply(state)
    end
  end

  def handle_info({:ekv_accept_nack, ref, _pid, remote_node_id}, %Replica{} = state) do
    case Map.get(state.pending_cas, ref) do
      nil ->
        cb_noreply(state)

      %{phase: :accept} = op ->
        if MapSet.member?(op.responded, remote_node_id) do
          cb_noreply(state)
        else
          op = %{
            op
            | accept_nacks: op.accept_nacks + 1,
              responded: MapSet.put(op.responded, remote_node_id)
          }

          max_possible = alive_node_id_count(state) - op.accept_nacks

          if max_possible < op.quorum do
            cb_noreply(handle_cas_failure(state, ref, op))
          else
            cb_noreply(%{state | pending_cas: Map.put(state.pending_cas, ref, op)})
          end
        end

      _ ->
        cb_noreply(state)
    end
  end

  # CAS timeout
  def handle_info({:cas_timeout, ref}, %Replica{} = state) do
    case Map.pop(state.pending_cas, ref) do
      {nil, _} ->
        cb_noreply(state)

      {op, pending_cas} ->
        cb_noreply(reply_cas_timeout(%{state | pending_cas: pending_cas}, op))
    end
  end

  # CAS retry (for operations with retry budget)
  def handle_info({:cas_retry, ref, key, operation}, %Replica{} = state) do
    # Re-check if we still have the pending op (might have been cleaned up)
    case Map.pop(state.pending_cas, ref) do
      {nil, _} ->
        cb_noreply(state)

      {old_op, pending_cas} ->
        state = %{state | pending_cas: pending_cas}

        cb_noreply(
          start_cas(
            state,
            key,
            operation,
            old_op.from,
            old_op.deadline_ms,
            Map.get(old_op, :reply_mode, :normal)
          )
        )
    end
  end

  # =====================================================================
  # GC
  # =====================================================================

  def handle_info({:gc, now, tombstone_cutoff}, %Replica{} = state) do
    %{db: db} = state

    # 1. Observe TTL expiry and emit :expired.
    expired = Store.find_expired(db, now)

    {state, gc_events} =
      Enum.reduce(expired, {state, []}, fn {key, value_binary, _timestamp, _origin_node,
                                            _expires_at},
                                           {state, acc} ->
        origin = local_origin_id(state)

        case Store.write_entry(
               db,
               state.stmts.kv_upsert,
               state.stmts.keyref_upsert,
               state.stmts.oplog_insert,
               key,
               nil,
               now,
               origin,
               nil,
               now,
               nil,
               true,
               true
             ) do
          {:error, :cas_managed_key} ->
            {:ok, applied} = Store.mark_expired(db, key, now)

            if applied do
              prev_value = if value_binary, do: :erlang.binary_to_term(value_binary)
              {state, [%EKV.Event{type: :expired, key: key, value: prev_value} | acc]}
            else
              {state, acc}
            end

          {:ok, true, origin_seq, local_progress_seq} ->
            state =
              state
              |> set_local_origin_seq(origin_seq)
              |> merge_local_progress_seq(origin, local_progress_seq)
              |> replicate_live_to_members({:ekv_delete, key, now, origin, origin_seq})

            prev_value = if value_binary, do: :erlang.binary_to_term(value_binary)
            {state, [%EKV.Event{type: :expired, key: key, value: prev_value} | acc]}

          {:ok, false, _origin_seq, local_progress_seq} ->
            {merge_local_progress_seq(state, origin, local_progress_seq), acc}

          {:ok, false} ->
            {state, acc}
        end
      end)

    dispatch_events(state, Enum.reverse(gc_events))

    # 2. Purge old tombstones from SQLite (no notification — already notified on delete)
    Store.purge_tombstones(db, tombstone_cutoff)

    # 2b. Purge long-expired rows from SQLite (no notification — already notified on expiry)
    Store.purge_expired(db, tombstone_cutoff)

    # 2c. Purge orphan kv_paxos rows (keys that were hard-deleted)
    if state.cluster_size, do: Store.purge_orphan_paxos(db)

    # 3. Prune progress for members outside the replay-retention window
    {state, retained_members} = retained_member_nodes_for_replay_gc(state)
    Store.prune_member_progress(db, retained_members)

    # 4. Truncate oplog
    {truncate_us, truncate_stats} = :timer.tc(Store, :truncate_oplog, [db])
    maybe_log_oplog_truncate(state, truncate_stats, truncate_us)
    state = %{state | local_max_seq: Store.max_seq(db)}

    # 5. Bump liveness timestamp
    Store.touch_last_active(db)

    # 6. Bounded cleanup for fallback name-based down markers.
    prune_stale_member_down_name_markers(state)
    prune_stale_member_seen_markers(state)

    cb_noreply(state)
  end

  def handle_info(:anti_entropy_tick, %Replica{} = state) do
    state =
      if state.handoff_node do
        state
      else
        state
        |> expire_stale_inflight()
        |> trigger_missing_member_connects()
        |> trigger_summary_probe()
      end

    schedule_anti_entropy_tick(state)
    cb_noreply(state)
  end

  # =====================================================================
  # Chunked sync continuations
  # =====================================================================

  def handle_info(
        {:continue_full_sync, remote_node, last_key, tombstone_cutoff, progress_summary,
         chunk_size, reason},
        %Replica{} = state
      ) do
    if Map.has_key?(state.remote_shards, remote_node) do
      cb_noreply(
        send_full_chunk(
          state,
          remote_node,
          last_key,
          tombstone_cutoff,
          progress_summary,
          chunk_size,
          reason
        )
      )
    else
      cb_noreply(state)
    end
  end

  def handle_info(
        {:continue_delta_sync, remote_node, origin_node, last_seq, my_seq, chunk_size},
        %Replica{} = state
      ) do
    if Map.has_key?(state.remote_shards, remote_node) do
      cb_noreply(send_delta_chunk(state, remote_node, origin_node, last_seq, my_seq, chunk_size))
    else
      cb_noreply(state)
    end
  end

  def handle_info(_msg, %Replica{} = state) do
    cb_noreply(state)
  end

  @impl true
  def handle_continue(:flush_due_replication_batches, %Replica{} = state) do
    {:noreply, flush_due_replication_batches(state)}
  end

  # =====================================================================
  # Internal helpers
  # =====================================================================

  defp handle_wire_message(
         %Replica{} = state,
         {:ekv_summary_probe, remote_pid, remote_shard, remote_progress, remote_node_id}
       ) do
    handle_wire_summary_probe(state, remote_pid, remote_shard, remote_progress, remote_node_id)
  end

  defp handle_wire_message(
         %Replica{} = state,
         {:ekv_summary_reply, remote_pid, remote_shard, remote_progress, remote_node_id}
       ) do
    handle_wire_summary_reply(state, remote_pid, remote_shard, remote_progress, remote_node_id)
  end

  defp handle_wire_message(%Replica{} = state, message), do: handle_info(message, state)

  defp handle_wire_summary_probe(
         %Replica{} = state,
         remote_pid,
         remote_shard,
         remote_progress,
         remote_node_id
       )
       when remote_shard == state.shard_index do
    remote_node = node(remote_pid)
    remote_progress = normalize_progress_summary(remote_progress)

    case maybe_allow_member_reconnect(state, remote_node, remote_node_id) do
      {:quarantine, %Replica{} = state} ->
        cb_noreply(state)

      {:ok, %Replica{} = state} ->
        state =
          state
          |> clear_summary_probe_inflight(remote_node)
          |> track_remote_shard(remote_node, remote_pid)
          |> track_member_node_id(remote_node, remote_node_id)
          |> persist_member_node_identity(remote_node, remote_node_id)
          |> remember_member_origin_seen(remote_node_id)
          |> replace_remote_member_progress(remote_node, remote_progress)
          |> reconcile_authoritative_origin_head(remote_node, remote_progress)

        send_to_member(
          state,
          remote_node,
          {:ekv_summary_reply, self(), state.shard_index, local_progress_summary_for_wire(state),
           state.node_id}
        )

        cb_noreply(
          maybe_request_repair(state, remote_node, remote_progress, preserve_inflight?: true)
        )
    end
  end

  defp handle_wire_summary_probe(
         %Replica{} = state,
         _remote_pid,
         _remote_shard,
         _remote_progress,
         _remote_node_id
       ),
       do: cb_noreply(state)

  defp handle_wire_summary_reply(
         %Replica{} = state,
         remote_pid,
         remote_shard,
         remote_progress,
         remote_node_id
       )
       when remote_shard == state.shard_index do
    remote_node = node(remote_pid)
    remote_progress = normalize_progress_summary(remote_progress)

    case maybe_allow_member_reconnect(state, remote_node, remote_node_id) do
      {:quarantine, %Replica{} = state} ->
        cb_noreply(state)

      {:ok, %Replica{} = state} ->
        state =
          state
          |> clear_summary_probe_inflight(remote_node)
          |> track_remote_shard(remote_node, remote_pid)
          |> track_member_node_id(remote_node, remote_node_id)
          |> persist_member_node_identity(remote_node, remote_node_id)
          |> remember_member_origin_seen(remote_node_id)
          |> replace_remote_member_progress(remote_node, remote_progress)
          |> reconcile_authoritative_origin_head(remote_node, remote_progress)

        cb_noreply(
          maybe_request_repair(state, remote_node, remote_progress, preserve_inflight?: true)
        )
    end
  end

  defp handle_wire_summary_reply(
         %Replica{} = state,
         _remote_pid,
         _remote_shard,
         _remote_progress,
         _remote_node_id
       ),
       do: cb_noreply(state)

  defp do_member_connect(
         %Replica{} = state,
         remote_pid,
         remote_shard,
         remote_num_shards,
         remote_progress,
         remote_node_id,
         remote_features
       )
       when remote_shard == state.shard_index do
    if remote_num_shards != state.num_shards do
      Logger.error(
        "#{log_prefix(state)} rejecting member_connect from #{node(remote_pid)}: " <>
          "shard count mismatch (local=#{state.num_shards}, remote=#{remote_num_shards})"
      )

      state
    else
      remote_node = node(remote_pid)
      remote_progress = normalize_progress_summary(remote_progress)

      case maybe_allow_member_reconnect(state, remote_node, remote_node_id) do
        {:quarantine, %Replica{} = state} ->
          state

        {:ok, %Replica{} = state} ->
          my_progress = local_progress_summary_for_wire(state)

          state =
            state
            |> track_remote_shard(remote_node, remote_pid)
            |> track_member_node_id(remote_node, remote_node_id)
            |> persist_member_node_identity(remote_node, remote_node_id)
            |> remember_member_origin_seen(remote_node_id)
            |> replace_remote_member_progress(remote_node, remote_progress)
            |> reconcile_authoritative_origin_head(remote_node, remote_progress)
            |> track_remote_features(remote_node, remote_features)

          if state.cluster_size do
            alive = alive_node_id_count(state)

            if alive > state.cluster_size do
              Logger.error(
                "#{log_prefix(state)} cluster overflow: #{alive} distinct node_ids, " <>
                  "cluster_size=#{state.cluster_size}. New member #{remote_node} " <>
                  "has node_id=#{inspect(remote_node_id)}"
              )
            end
          end

          send_to_member(
            state,
            remote_node,
            member_connect_ack_message(state, my_progress)
          )

          log_once(state, fn -> "#{log_prefix(state)} ekv_member_connect from #{remote_node}" end)

          state =
            maybe_request_repair(
              state,
              remote_node,
              remote_progress,
              preserve_inflight?: true
            )

          maybe_reply_to_quorum_waiters(state)
      end
    end
  end

  defp do_member_connect(
         %Replica{} = state,
         _remote_pid,
         _remote_shard,
         _remote_num_shards,
         _remote_progress,
         _remote_node_id,
         _remote_features
       ) do
    state
  end

  defp do_member_connect_ack(
         %Replica{} = state,
         remote_pid,
         remote_shard,
         remote_num_shards,
         remote_progress,
         remote_node_id,
         remote_features
       )
       when remote_shard == state.shard_index do
    if remote_num_shards != state.num_shards do
      Logger.error(
        "#{log_prefix(state)} rejecting member_connect_ack from #{node(remote_pid)}: " <>
          "shard count mismatch (local=#{state.num_shards}, remote=#{remote_num_shards})"
      )

      state
    else
      remote_node = node(remote_pid)
      remote_progress = normalize_progress_summary(remote_progress)

      case maybe_allow_member_reconnect(state, remote_node, remote_node_id) do
        {:quarantine, %Replica{} = state} ->
          state

        {:ok, %Replica{} = state} ->
          state =
            state
            |> track_remote_shard(remote_node, remote_pid)
            |> track_member_node_id(remote_node, remote_node_id)
            |> persist_member_node_identity(remote_node, remote_node_id)
            |> remember_member_origin_seen(remote_node_id)
            |> replace_remote_member_progress(remote_node, remote_progress)
            |> reconcile_authoritative_origin_head(remote_node, remote_progress)
            |> track_remote_features(remote_node, remote_features)

          if state.cluster_size do
            alive = alive_node_id_count(state)

            if alive > state.cluster_size do
              Logger.error(
                "#{log_prefix(state)} cluster overflow: #{alive} distinct node_ids, " <>
                  "cluster_size=#{state.cluster_size}. New member #{remote_node} " <>
                  "has node_id=#{inspect(remote_node_id)}"
              )
            end
          end

          log_once(state, fn ->
            "#{log_prefix(state)} ekv_member_connect_ack from #{remote_node}"
          end)

          state =
            maybe_request_repair(
              state,
              remote_node,
              remote_progress,
              preserve_inflight?: true
            )

          maybe_reply_to_quorum_waiters(state)
      end
    end
  end

  defp do_member_connect_ack(
         %Replica{} = state,
         _remote_pid,
         _remote_shard,
         _remote_num_shards,
         _remote_progress,
         _remote_node_id,
         _remote_features
       ) do
    state
  end

  defp merge_remote_entry(
         %Replica{} = state,
         key,
         value_binary,
         timestamp,
         origin_node,
         origin_seq,
         expires_at,
         deleted_at
       ) do
    %{db: db, stmts: stmts} = state

    case Store.write_entry(
           db,
           stmts.kv_upsert,
           stmts.keyref_upsert,
           stmts.oplog_insert,
           key,
           value_binary,
           timestamp,
           origin_node,
           expires_at,
           deleted_at,
           origin_seq,
           false
         ) do
      {:ok, true, applied_origin_seq, local_progress_seq} ->
        {true,
         track_applied_origin_progress(state, origin_node, applied_origin_seq, local_progress_seq)}

      {:ok, false, applied_origin_seq, local_progress_seq} ->
        {false,
         track_applied_origin_progress(state, origin_node, applied_origin_seq, local_progress_seq)}

      {:ok, false} ->
        {false, state}
    end
  end

  defp apply_sync_entry(
         %Replica{} = state,
         :delta,
         key,
         value_binary,
         timestamp,
         origin_node,
         origin_seq,
         expires_at,
         deleted_at
       ) do
    merge_remote_entry(
      state,
      key,
      value_binary,
      timestamp,
      origin_node,
      origin_seq,
      expires_at,
      deleted_at
    )
  end

  defp apply_sync_entry(
         %Replica{} = state,
         :full,
         key,
         value_binary,
         timestamp,
         origin_node,
         origin_seq,
         expires_at,
         deleted_at
       ) do
    case Store.write_snapshot_entry(
           state.db,
           state.stmts.kv_upsert,
           key,
           value_binary,
           timestamp,
           origin_node,
           origin_seq,
           expires_at,
           deleted_at
         ) do
      {:ok, true} -> {true, state}
      {:ok, false} -> {false, state}
    end
  end

  defp apply_replication_batch(%Replica{} = state, from_node, origin_node, entries)
       when is_atom(from_node) and is_binary(origin_node) and is_list(entries) do
    case normalize_replication_batch_entries(entries) do
      {:ok, normalized_entries, first_origin_seq, last_origin_seq} ->
        gap? = origin_gap?(state, origin_node, first_origin_seq)
        initial_delete_values = replication_batch_initial_delete_values(state, normalized_entries)

        case Store.write_entries_batch(
               state.db,
               state.stmts.kv_upsert,
               state.stmts.keyref_upsert,
               state.stmts.oplog_insert,
               origin_node,
               normalized_entries
             ) do
          {:ok, applied_flags, _applied_origin_seq, local_progress_seq} ->
            state =
              state
              |> track_applied_origin_progress(origin_node, last_origin_seq, local_progress_seq)
              |> maybe_request_origin_gap_repair(origin_node, last_origin_seq, gap?)

            dispatch_events(
              state,
              replication_batch_events(normalized_entries, applied_flags, initial_delete_values)
            )

            state

          {:error, _reason} ->
            apply_replication_batch_fallback(state, origin_node, normalized_entries)
        end

      :error ->
        apply_replication_batch_fallback(state, origin_node, entries)
    end
  end

  defp apply_replication_batch(%Replica{} = state, _from_node, _origin_node, _entries), do: state

  defp normalize_replication_batch_entries(entries) when is_list(entries) do
    Enum.reduce_while(entries, {:ok, [], nil, nil}, fn
      {key, value_binary, timestamp, origin_seq, expires_at, deleted_at}, {:ok, acc, nil, nil}
      when is_binary(key) and is_integer(timestamp) and is_integer(origin_seq) and origin_seq >= 0 ->
        {:cont,
         {:ok, [{key, value_binary, timestamp, origin_seq, expires_at, deleted_at} | acc],
          origin_seq, origin_seq}}

      {key, value_binary, timestamp, origin_seq, expires_at, deleted_at},
      {:ok, acc, first_origin_seq, prev_origin_seq}
      when is_binary(key) and is_integer(timestamp) and is_integer(origin_seq) and
             origin_seq == prev_origin_seq + 1 ->
        {:cont,
         {:ok, [{key, value_binary, timestamp, origin_seq, expires_at, deleted_at} | acc],
          first_origin_seq, origin_seq}}

      _entry, _acc ->
        {:halt, :error}
    end)
    |> case do
      {:ok, [], nil, nil} ->
        :error

      {:ok, acc, first_origin_seq, last_origin_seq} ->
        {:ok, Enum.reverse(acc), first_origin_seq, last_origin_seq}

      :error ->
        :error
    end
  end

  defp normalize_replication_batch_entries(_entries), do: :error

  defp replication_batch_initial_delete_values(%Replica{} = state, entries) do
    if has_subscribers?(state) do
      entries
      |> Enum.reduce(MapSet.new(), fn
        {key, _value_binary, _timestamp, _origin_seq, _expires_at, deleted_at}, acc ->
          if is_integer(deleted_at), do: MapSet.put(acc, key), else: acc
      end)
      |> Map.new(fn key -> {key, read_previous_value(state, key)} end)
    else
      %{}
    end
  end

  defp replication_batch_events(entries, applied_flags, initial_delete_values) do
    {events, _shadow_values} =
      entries
      |> Enum.zip(applied_flags)
      |> Enum.reduce({[], initial_delete_values}, fn
        {{_key, _value_binary, _timestamp, _origin_seq, _expires_at, _deleted_at}, false},
        {acc_events, shadow_values} ->
          {acc_events, shadow_values}

        {{key, _value_binary, _timestamp, _origin_seq, _expires_at, deleted_at}, true},
        {acc_events, shadow_values}
        when is_integer(deleted_at) ->
          event = %EKV.Event{type: :delete, key: key, value: Map.get(shadow_values, key)}
          {[event | acc_events], Map.put(shadow_values, key, nil)}

        {{key, value_binary, _timestamp, _origin_seq, _expires_at, _deleted_at}, true},
        {acc_events, shadow_values} ->
          value = :erlang.binary_to_term(value_binary)
          event = %EKV.Event{type: :put, key: key, value: value}
          {[event | acc_events], Map.put(shadow_values, key, value)}
      end)

    Enum.reverse(events)
  end

  defp apply_replication_batch_fallback(%Replica{} = state, origin_node, entries) do
    {state, events} =
      Enum.reduce(entries, {state, []}, fn
        {key, value_binary, timestamp, origin_seq, expires_at, deleted_at},
        {acc_state, acc_events} ->
          gap? = origin_gap?(acc_state, origin_node, origin_seq)

          prev_value =
            if is_integer(deleted_at) and has_subscribers?(acc_state),
              do: read_previous_value(acc_state, key)

          {applied, acc_state} =
            merge_remote_entry(
              acc_state,
              key,
              value_binary,
              timestamp,
              origin_node,
              origin_seq,
              expires_at,
              deleted_at
            )

          acc_state = maybe_request_origin_gap_repair(acc_state, origin_node, origin_seq, gap?)

          acc_events =
            cond do
              not applied ->
                acc_events

              is_integer(deleted_at) ->
                [%EKV.Event{type: :delete, key: key, value: prev_value} | acc_events]

              true ->
                [
                  %EKV.Event{type: :put, key: key, value: :erlang.binary_to_term(value_binary)}
                  | acc_events
                ]
            end

          {acc_state, acc_events}
      end)

    dispatch_events(state, Enum.reverse(events))
    state
  end

  defp serve_sync_request(%Replica{} = state, remote_node, {:delta, origin_node, from_seq})
       when is_integer(from_seq) and from_seq >= 0 do
    %{db: db} = state
    origin_node = normalize_origin_node(origin_node)
    chunk_size = EKV.Supervisor.get_config(state.name).sync_chunk_size
    local_progress = local_progress_summary_for_wire(state)
    my_seq = Map.get(local_progress, origin_node, 0)
    replay_bounds = Map.get(Store.replay_origin_bounds(db), origin_node)

    cond do
      my_seq <= from_seq ->
        state = record_delta_sync_send(state, remote_node, 0)
        maybe_log_empty_terminal_delta_sync(state, remote_node, origin_node, from_seq, my_seq)

        send_to_member(
          state,
          remote_node,
          {:ekv_sync, node(), state.shard_index, :delta, [], %{origin_node => my_seq}}
        )

        state

      is_nil(replay_bounds) ->
        log(state, fn ->
          "#{log_prefix_shard(state)} #{remote_node} requested delta for #{origin_node} " <>
            "from_seq=#{from_seq} but retained replay bounds are unavailable; sending full sync"
        end)

        send_full_sync(state, remote_node, {:no_replay_bounds, origin_node, from_seq})

      from_seq < max(elem(replay_bounds, 0) - 1, 0) ->
        log(state, fn ->
          "#{log_prefix_shard(state)} #{remote_node} requested delta for #{origin_node} " <>
            "from_seq=#{from_seq} below retained min=#{elem(replay_bounds, 0)}; sending full sync"
        end)

        send_full_sync(
          state,
          remote_node,
          {:below_retained_min, origin_node, from_seq, elem(replay_bounds, 0)}
        )

      true ->
        send_delta_chunk(state, remote_node, origin_node, from_seq, my_seq, chunk_size)
    end
  end

  defp serve_sync_request(%Replica{} = state, remote_node, :full) do
    send_full_sync(state, remote_node, :explicit_request)
  end

  defp serve_sync_request(%Replica{} = state, remote_node, {:full, reason}) do
    send_full_sync(state, remote_node, {:explicit_request, reason})
  end

  defp serve_sync_request(%Replica{} = state, _remote_node, _request), do: state

  defp send_full_sync(%Replica{} = state, remote_node, reason) do
    config = EKV.Supervisor.get_config(state.name)
    tombstone_cutoff = System.system_time(:nanosecond) - config.tombstone_ttl * 1_000_000
    chunk_size = config.sync_chunk_size

    send_full_chunk(
      state,
      remote_node,
      nil,
      tombstone_cutoff,
      local_progress_summary_for_wire(state),
      chunk_size,
      reason
    )
  end

  defp send_full_chunk(
         %Replica{} = state,
         remote_node,
         last_key,
         tombstone_cutoff,
         progress_summary,
         chunk_size,
         reason
       ) do
    fetched = Store.full_state_chunk(state.db, tombstone_cutoff, last_key, chunk_size + 1)

    case fetched do
      [] ->
        log(state, fn ->
          "#{log_prefix_shard(state)} sending empty terminal full sync to #{remote_node} " <>
            "progress=#{map_size(progress_summary)} origins reason=#{format_full_sync_reason(reason)}"
        end)

        send_to_member(
          state,
          remote_node,
          {:ekv_sync, node(), state.shard_index, :full, [], progress_summary}
        )

        state

      _ ->
        has_more? = length(fetched) > chunk_size
        entries = if has_more?, do: Enum.take(fetched, chunk_size), else: fetched
        final? = not has_more?
        progress = if final?, do: progress_summary, else: nil

        log(state, fn ->
          "#{log_prefix_shard(state)} sending full sync to #{remote_node} " <>
            "entries=#{length(entries)} final=#{final?} reason=#{format_full_sync_reason(reason)}"
        end)

        send_to_member(
          state,
          remote_node,
          {:ekv_sync, node(), state.shard_index, :full, entries, progress}
        )

        if final? do
          state
        else
          next_key = elem(List.last(entries), 0)

          send(
            self(),
            {:continue_full_sync, remote_node, next_key, tombstone_cutoff, progress_summary,
             chunk_size, reason}
          )

          state
        end
    end
  end

  defp format_full_sync_reason(:explicit_request), do: "explicit_request"

  defp format_full_sync_reason(
         {:explicit_request, {:unknown_member_origin, origin_node, local_seq}}
       ) do
    "explicit_request subreason=unknown_member_origin origin=#{origin_node} local_seq=#{local_seq}"
  end

  defp format_full_sync_reason({:explicit_request, {:quarantined_origin, origin_node, local_seq}}) do
    "explicit_request subreason=quarantined_origin origin=#{origin_node} local_seq=#{local_seq}"
  end

  defp format_full_sync_reason({:explicit_request, reason}) do
    "explicit_request detail=#{inspect(reason)}"
  end

  defp format_full_sync_reason({:no_replay_bounds, origin_node, from_seq}) do
    "no_replay_bounds origin=#{origin_node} from_seq=#{from_seq}"
  end

  defp format_full_sync_reason({:below_retained_min, origin_node, from_seq, min_seq}) do
    "below_retained_min origin=#{origin_node} from_seq=#{from_seq} min_seq=#{min_seq}"
  end

  defp format_full_sync_reason(other), do: inspect(other)

  defp send_delta_chunk(
         %Replica{} = state,
         remote_node,
         origin_node,
         last_seq,
         my_seq,
         chunk_size
       ) do
    fetched = Store.replay_since_origin_chunk(state.db, origin_node, last_seq, chunk_size + 1)

    case fetched do
      [] ->
        if my_seq > last_seq do
          state = record_delta_sync_send(state, remote_node, 0)
          maybe_log_empty_terminal_delta_sync(state, remote_node, origin_node, last_seq, my_seq)

          send_to_member(
            state,
            remote_node,
            {:ekv_sync, node(), state.shard_index, :delta, [], %{origin_node => my_seq}}
          )
        end

        state

      _ ->
        has_more? = length(fetched) > chunk_size
        replay_entries = if has_more?, do: Enum.take(fetched, chunk_size), else: fetched

        entries =
          replay_entries
          |> Enum.map(fn {key, value, timestamp, replay_origin, origin_seq, expires_at, is_delete} ->
            deleted_at = if is_delete, do: timestamp, else: nil
            {key, value, timestamp, replay_origin, origin_seq, expires_at, deleted_at}
          end)

        final? = not has_more?
        progress = if final?, do: %{origin_node => my_seq}, else: nil

        cond do
          entries == [] and final? ->
            state = record_delta_sync_send(state, remote_node, 0)
            maybe_log_empty_terminal_delta_sync(state, remote_node, origin_node, last_seq, my_seq)

            send_to_member(
              state,
              remote_node,
              {:ekv_sync, node(), state.shard_index, :delta, [], %{origin_node => my_seq}}
            )

            state

          entries == [] ->
            max_chunk_seq = replay_chunk_max_seq(replay_entries)

            send(
              self(),
              {:continue_delta_sync, remote_node, origin_node, max_chunk_seq, my_seq, chunk_size}
            )

            state

          true ->
            entry_count = length(entries)
            state = record_delta_sync_send(state, remote_node, entry_count)

            maybe_log_delta_sync(
              state,
              remote_node,
              origin_node,
              last_seq,
              my_seq,
              entry_count,
              final?
            )

            send_to_member(
              state,
              remote_node,
              {:ekv_sync, node(), state.shard_index, :delta, entries, progress}
            )

            if final? do
              state
            else
              max_chunk_seq = replay_chunk_max_seq(replay_entries)

              send(
                self(),
                {:continue_delta_sync, remote_node, origin_node, max_chunk_seq, my_seq,
                 chunk_size}
              )

              state
            end
        end
    end
  end

  defp mark_sync_inflight(%Replica{} = state, remote_node, request) when request in [:full] do
    now_ms = System.monotonic_time(:millisecond)

    %{
      state
      | sync_inflight: Map.put(state.sync_inflight, remote_node, now_ms),
        full_sync_inflight: remote_node
    }
  end

  defp mark_sync_inflight(%Replica{} = state, remote_node, {:full, _reason}) do
    now_ms = System.monotonic_time(:millisecond)

    %{
      state
      | sync_inflight: Map.put(state.sync_inflight, remote_node, now_ms),
        full_sync_inflight: remote_node
    }
  end

  defp mark_sync_inflight(%Replica{} = state, remote_node, _request) do
    %{
      state
      | sync_inflight:
          Map.put(state.sync_inflight, remote_node, System.monotonic_time(:millisecond))
    }
  end

  defp maybe_log_empty_terminal_delta_sync(
         %Replica{} = state,
         remote_node,
         origin_node,
         from_seq,
         to_seq
       ) do
    if should_log_delta_sync?(state, 0, true) do
      log(state, fn ->
        "#{log_prefix_shard(state)} sending empty terminal delta sync to #{remote_node} " <>
          "from_seq=#{from_seq} origin=#{origin_node} to_seq=#{to_seq}"
      end)
    end

    state
  end

  defp maybe_log_delta_sync(
         %Replica{} = state,
         remote_node,
         origin_node,
         from_seq,
         to_seq,
         entry_count,
         final?
       ) do
    if should_log_delta_sync?(state, entry_count, final?) do
      log(state, fn ->
        "#{log_prefix_shard(state)} sending delta sync to #{remote_node} " <>
          "entries=#{entry_count} from_seq=#{from_seq} final=#{final?} " <>
          "origin=#{origin_node} to_seq=#{to_seq}"
      end)
    end

    state
  end

  defp should_log_delta_sync?(%Replica{} = state, entry_count, final?)
       when is_integer(entry_count) and is_boolean(final?) do
    case EKV.Supervisor.get_config(state.name) do
      %{log: false} ->
        false

      %{log: :verbose} ->
        true

      %{delta_sync_log_min_entries: min_entries} when final? ->
        entry_count >= min_entries

      _ ->
        true
    end
  end

  defp record_delta_sync_send(%Replica{} = state, remote_node, entry_count)
       when is_atom(remote_node) and is_integer(entry_count) and entry_count >= 0 do
    config = EKV.Supervisor.get_config(state.name)
    threshold = Map.get(config, :delta_sync_storm_threshold)
    window_ms = Map.get(config, :delta_sync_storm_window, :timer.minutes(1))

    if is_integer(threshold) and threshold > 0 do
      now_ms = System.monotonic_time(:millisecond)

      state =
        case state.delta_sync_storm_started_at_ms do
          nil ->
            %{state | delta_sync_storm_started_at_ms: now_ms}

          started_at_ms when now_ms - started_at_ms >= window_ms ->
            %{
              state
              | delta_sync_storm_started_at_ms: now_ms,
                delta_sync_storm_count: 0,
                delta_sync_storm_entries: 0,
                delta_sync_storm_peers: MapSet.new(),
                delta_sync_storm_logged?: false
            }

          _ ->
            state
        end

      state = %{
        state
        | delta_sync_storm_count: state.delta_sync_storm_count + 1,
          delta_sync_storm_entries: state.delta_sync_storm_entries + entry_count,
          delta_sync_storm_peers: MapSet.put(state.delta_sync_storm_peers, remote_node)
      }

      if not state.delta_sync_storm_logged? and state.delta_sync_storm_count >= threshold do
        log_warn(state, fn ->
          "#{log_prefix_shard(state)} delta_sync_storm count=#{state.delta_sync_storm_count} " <>
            "entries=#{state.delta_sync_storm_entries} peers=#{MapSet.size(state.delta_sync_storm_peers)} " <>
            "window_ms=#{window_ms}"
        end)

        %{state | delta_sync_storm_logged?: true}
      else
        state
      end
    else
      state
    end
  end

  defp record_delta_sync_send(%Replica{} = state, _remote_node, _entry_count), do: state

  defp clear_sync_inflight(%Replica{} = state, remote_node) do
    state = %{
      state
      | sync_inflight: Map.delete(state.sync_inflight, remote_node),
        delta_origin_inflight:
          state.delta_origin_inflight
          |> Enum.reject(fn {_origin_node, {source_node, _activity_at_ms}} ->
            source_node == remote_node
          end)
          |> Map.new()
    }

    if state.full_sync_inflight == remote_node do
      %{state | full_sync_inflight: nil}
    else
      state
    end
  end

  defp touch_sync_inflight(%Replica{} = state, remote_node) when is_atom(remote_node) do
    if Map.has_key?(state.sync_inflight, remote_node) do
      now_ms = System.monotonic_time(:millisecond)

      %{
        state
        | sync_inflight: Map.put(state.sync_inflight, remote_node, now_ms),
          delta_origin_inflight:
            state.delta_origin_inflight
            |> Enum.map(fn
              {origin_node, {^remote_node, _activity_at_ms}} ->
                {origin_node, {remote_node, now_ms}}

              other ->
                other
            end)
            |> Map.new()
      }
    else
      state
    end
  end

  defp touch_sync_inflight(%Replica{} = state, _remote_node), do: state

  defp request_sync(%Replica{} = state, remote_node, request) do
    state = expire_stale_sync_inflight(state, remote_node)
    state = expire_stale_delta_origin_inflight(state, remote_node, request)

    cond do
      state.handoff_node != nil ->
        state

      not Map.has_key?(state.remote_shards, remote_node) ->
        clear_sync_inflight(state, remote_node)

      MapSet.member?(state.quarantined_members, remote_node) ->
        clear_sync_inflight(state, remote_node)

      full_sync_request?(request) and state.full_sync_inflight == remote_node ->
        state

      full_sync_request?(request) and not is_nil(state.full_sync_inflight) ->
        state

      delta_origin_inflight?(state, remote_node, request) ->
        state

      Map.has_key?(state.sync_inflight, remote_node) ->
        state

      true ->
        maybe_log_full_sync_request(state, remote_node, request)

        send_to_member(
          state,
          remote_node,
          {:ekv_sync_request, self(), state.shard_index, request}
        )

        state
        |> mark_sync_inflight(remote_node, request)
        |> mark_delta_origin_inflight(remote_node, request)
    end
  end

  defp maybe_request_repair(%Replica{} = state, remote_node, remote_progress, opts \\ []) do
    {state, request} =
      sync_request_for_remote(state, remote_node, normalize_progress_summary(remote_progress))

    preserve_inflight? = Keyword.get(opts, :preserve_inflight?, false)

    case request do
      nil ->
        if preserve_inflight? and Map.has_key?(state.sync_inflight, remote_node) do
          state
        else
          clear_sync_inflight(state, remote_node)
        end

      request ->
        request_sync(state, remote_node, request)
    end
  end

  defp maybe_request_repairs(%Replica{} = state) do
    Enum.reduce(state.remote_member_progress, state, fn {remote_node, remote_progress}, acc ->
      maybe_request_repair(acc, remote_node, remote_progress)
    end)
  end

  defp delta_origin_inflight?(%Replica{} = state, remote_node, {:delta, origin_node, _from_seq})
       when is_atom(remote_node) and is_binary(origin_node) do
    relayed_delta_request?(state, remote_node, origin_node) and
      Map.has_key?(state.delta_origin_inflight, origin_node)
  end

  defp delta_origin_inflight?(%Replica{} = _state, _remote_node, _request), do: false

  defp mark_delta_origin_inflight(
         %Replica{} = state,
         remote_node,
         {:delta, origin_node, _from_seq}
       )
       when is_atom(remote_node) and is_binary(origin_node) do
    if relayed_delta_request?(state, remote_node, origin_node) do
      %{
        state
        | delta_origin_inflight:
            Map.put(
              state.delta_origin_inflight,
              origin_node,
              {remote_node, System.monotonic_time(:millisecond)}
            )
      }
    else
      state
    end
  end

  defp mark_delta_origin_inflight(%Replica{} = state, _remote_node, _request), do: state

  defp full_sync_request?(:full), do: true
  defp full_sync_request?({:full, _reason}), do: true
  defp full_sync_request?(_request), do: false

  defp maybe_log_full_sync_request(
         %Replica{} = state,
         remote_node,
         {:full, {:unknown_member_origin, origin_node, local_seq}}
       ) do
    diagnostics = unknown_member_origin_diagnostics(state, origin_node)

    log(state, fn ->
      "#{log_prefix_shard(state)} requesting full sync from #{remote_node} " <>
        "reason=unknown_member_origin origin=#{origin_node} local_seq=#{local_seq} " <>
        "presence_origin_known=#{diagnostics.presence_origin_known} " <>
        "known_member_nodes_count=#{diagnostics.known_member_nodes_count} " <>
        "known_member_node_matches=#{inspect(diagnostics.known_member_node_matches)} " <>
        "member_node_id_matches=#{inspect(diagnostics.member_node_id_matches)} " <>
        "remote_shard_matches=#{inspect(diagnostics.remote_shard_matches)} " <>
        "known_down_member=#{diagnostics.known_down_member}"
    end)
  end

  defp maybe_log_full_sync_request(
         %Replica{} = state,
         remote_node,
         {:full, {:quarantined_origin, origin_node, local_seq}}
       ) do
    quarantined_matches = quarantined_member_matches(state, origin_node)

    log(state, fn ->
      "#{log_prefix_shard(state)} requesting full sync from #{remote_node} " <>
        "reason=quarantined_origin origin=#{origin_node} local_seq=#{local_seq} " <>
        "quarantined_matches=#{inspect(quarantined_matches)} " <>
        "known_down_member=#{known_down_member?(state, origin_node)}"
    end)
  end

  defp maybe_log_full_sync_request(%Replica{} = _state, _remote_node, _request), do: :ok

  defp unknown_member_origin_diagnostics(%Replica{} = state, origin_node) do
    known_member_node_matches =
      state
      |> known_member_nodes()
      |> Enum.filter(&member_matches_origin?(state, &1, origin_node))
      |> Enum.map(&Atom.to_string/1)
      |> Enum.sort()

    member_node_id_matches =
      state.member_node_ids
      |> Enum.filter(fn {member_node, _member_node_id} ->
        member_matches_origin?(state, member_node, origin_node)
      end)
      |> Enum.map(fn {member_node, member_node_id} ->
        {Atom.to_string(member_node), member_node_id}
      end)
      |> Enum.sort()

    remote_shard_matches =
      state.remote_shards
      |> Map.keys()
      |> Enum.filter(&member_matches_origin?(state, &1, origin_node))
      |> Enum.map(&Atom.to_string/1)
      |> Enum.sort()

    %{
      presence_origin_known: EKV.MemberPresence.member_origin_known?(state.name, origin_node),
      known_member_nodes_count: MapSet.size(known_member_nodes(state)),
      known_member_node_matches: known_member_node_matches,
      member_node_id_matches: member_node_id_matches,
      remote_shard_matches: remote_shard_matches,
      known_down_member: known_down_member?(state, origin_node)
    }
  end

  defp quarantined_member_matches(%Replica{} = state, origin_node) do
    state.quarantined_members
    |> Enum.filter(fn remote_node ->
      remote_origin_id(state, remote_node) == origin_node or
        Atom.to_string(remote_node) == origin_node
    end)
    |> Enum.map(&Atom.to_string/1)
    |> Enum.sort()
  end

  defp relayed_delta_request?(%Replica{} = state, remote_node, origin_node)
       when is_atom(remote_node) and is_binary(origin_node) do
    remote_origin_id(state, remote_node) != origin_node
  end

  defp relayed_delta_request?(%Replica{} = _state, _remote_node, _origin_node), do: false

  defp mark_summary_probe_inflight(%Replica{} = state, remote_node) do
    %{
      state
      | summary_probe_inflight:
          Map.put(state.summary_probe_inflight, remote_node, System.monotonic_time(:millisecond))
    }
  end

  defp clear_summary_probe_inflight(%Replica{} = state, remote_node) do
    %{state | summary_probe_inflight: Map.delete(state.summary_probe_inflight, remote_node)}
  end

  defp sync_request_for_remote(%Replica{} = state, remote_node, remote_progress) do
    local_progress = local_progress_summary_for_wire(state)
    known_member_nodes = known_member_nodes(state)
    remote_origin_id = remote_origin_id(state, remote_node, remote_progress)
    remote_origin_seq = Map.get(remote_progress, remote_origin_id, 0)
    local_origin_seq = Map.get(local_progress, remote_origin_id, 0)

    cond do
      remote_origin_seq > local_origin_seq ->
        {state, {:delta, remote_origin_id, local_origin_seq}}

      true ->
        Enum.reduce_while(remote_progress, {state, nil}, fn {origin_node, remote_seq},
                                                            {acc, _request} ->
          local_seq = Map.get(local_progress, origin_node, 0)

          if origin_node != local_origin_id(acc) and origin_node != remote_origin_id and
               remote_seq > local_seq do
            {acc, request} =
              third_origin_sync_request(
                acc,
                known_member_nodes,
                remote_node,
                origin_node,
                local_seq
              )

            if is_nil(request) do
              {:cont, {acc, nil}}
            else
              {:halt, {acc, request}}
            end
          else
            {:cont, {acc, nil}}
          end
        end)
    end
  end

  defp third_origin_sync_request(
         %Replica{} = state,
         known_member_nodes,
         _remote_node,
         origin_node,
         local_seq
       )
       when is_binary(origin_node) do
    cond do
      known_down_member_quarantined?(state, origin_node) ->
        {state, {:full, {:quarantined_origin, origin_node, local_seq}}}

      known_member_origin?(state, known_member_nodes, origin_node) ->
        {state, {:delta, origin_node, local_seq}}

      recent_member_origin_hint?(state, origin_node) ->
        {state, nil}

      true ->
        {state, {:full, {:unknown_member_origin, origin_node, local_seq}}}
    end
  end

  defp third_origin_sync_request(
         %Replica{} = state,
         _known_member_nodes,
         _remote_node,
         _origin_node,
         _local_seq
       ),
       do: {state, nil}

  defp known_member_nodes(%Replica{} = state) do
    state.name
    |> EKV.MemberPresence.member_nodes()
    |> MapSet.new()
  rescue
    _ -> MapSet.new()
  end

  defp known_member_origin?(%Replica{} = state, known_member_nodes, origin_node) do
    Enum.any?(known_member_nodes, &member_matches_origin?(state, &1, origin_node)) or
      EKV.MemberPresence.member_origin_known?(state.name, origin_node) or
      origin_node == state.node_id or
      Enum.any?(Map.keys(state.member_node_ids), &member_matches_origin?(state, &1, origin_node)) or
      Enum.any?(Map.keys(state.remote_shards), &member_matches_origin?(state, &1, origin_node)) or
      known_down_member?(state, origin_node)
  end

  defp recent_member_origin_hint?(%Replica{} = state, origin_node)
       when is_binary(origin_node) and byte_size(origin_node) > 0 do
    now_ms = System.system_time(:millisecond)

    now_ms - (state.started_at_ms || now_ms) <= @unknown_member_origin_startup_grace_ms and
      member_origin_seen_recently?(state, origin_node, now_ms)
  end

  defp recent_member_origin_hint?(%Replica{} = _state, _origin_node), do: false

  defp member_origin_seen_recently?(%Replica{} = state, origin_node, now_ms) do
    case Map.fetch(state.member_seen_at, origin_node) do
      {:ok, seen_at_ms} when is_integer(seen_at_ms) ->
        now_ms - seen_at_ms <= @member_seen_hint_ttl_ms

      _ ->
        seen_at_ms = Store.member_seen_marker_get(state.db, origin_node)
        is_integer(seen_at_ms) and now_ms - seen_at_ms <= @member_seen_hint_ttl_ms
    end
  end

  defp member_matches_origin?(%Replica{} = state, member_node, origin_node)
       when is_atom(member_node) and is_binary(origin_node) do
    remote_member_id(state, member_node) == origin_node or
      Atom.to_string(member_node) == origin_node
  end

  defp member_matches_origin?(%Replica{} = _state, _member_node, _origin_node), do: false

  defp member_progress_retention_ttl(%Replica{} = state) do
    EKV.Supervisor.get_config(state.name)[:member_progress_retention_ttl] || 0
  end

  defp retained_member_nodes_for_replay_gc(%Replica{} = state) do
    connected_members = Enum.map(Map.keys(state.remote_shards), &remote_member_id(state, &1))
    connected_set = MapSet.new(connected_members)

    {state, retained_set} =
      Enum.reduce(Store.member_progress_members(state.db), {state, connected_set}, fn member_node,
                                                                                      {acc, kept} ->
        if MapSet.member?(kept, member_node) do
          {acc, kept}
        else
          {acc, retain?} = retain_member_progress_anchor?(acc, member_node)
          {acc, if(retain?, do: MapSet.put(kept, member_node), else: kept)}
        end
      end)

    {state, MapSet.to_list(retained_set)}
  end

  defp retain_member_progress_anchor?(%Replica{} = state, member_node)
       when is_atom(member_node) or is_binary(member_node) do
    retention_ttl = member_progress_retention_ttl(state)

    if retention_ttl == 0 do
      {state, false}
    else
      now_ms = System.system_time(:millisecond)
      member_node_key = normalize_origin_node(member_node)
      member_node_id = Store.member_node_identity_get(state.db, member_node_key)

      {%Replica{} = state, down_since_ms} =
        retained_member_down_marker_keys(member_node_key, member_node_id)
        |> Enum.reduce({state, nil}, fn marker_key, {acc, best} ->
          {%Replica{} = acc, down_since} = read_member_down_marker(acc, marker_key)

          merged =
            cond do
              is_integer(best) and is_integer(down_since) -> min(best, down_since)
              is_integer(best) -> best
              true -> down_since
            end

          {acc, merged}
        end)

      retain? =
        is_integer(down_since_ms) and max(0, now_ms - down_since_ms) <= retention_ttl

      {state, retain?}
    end
  end

  defp known_down_member?(%Replica{} = state, remote_node_or_id)
       when is_atom(remote_node_or_id) or is_binary(remote_node_or_id) do
    remote_node_or_id
    |> known_down_member_marker_keys(state)
    |> Enum.any?(fn marker_key ->
      case Map.fetch(state.member_down_at, marker_key) do
        {:ok, down_since} -> is_integer(down_since)
        :error -> is_integer(Store.member_down_marker_get(state.db, marker_key))
      end
    end)
  end

  defp known_down_member?(%Replica{} = _state, _remote_node_or_id), do: false

  defp known_down_member_quarantined?(%Replica{} = state, origin_node_id)
       when is_binary(origin_node_id) do
    Enum.any?(state.quarantined_members, fn remote_node ->
      remote_origin_id(state, remote_node) == origin_node_id or
        Atom.to_string(remote_node) == origin_node_id
    end)
  end

  defp known_down_member_quarantined?(%Replica{} = _state, _origin_node_id), do: false

  defp progress_ack_summary(%Replica{} = state, :full, _progress) do
    local_progress_summary_for_wire(state)
  end

  defp progress_ack_summary(%Replica{} = state, :delta, progress) do
    local_progress = local_progress_summary_for_wire(state)

    Map.new(progress, fn {origin_node, _seq} ->
      {origin_node, Map.get(local_progress, origin_node, 0)}
    end)
  end

  defp trigger_summary_probe(%Replica{} = state) do
    Enum.reduce(Map.keys(state.remote_shards), state, fn remote_node, acc ->
      if MapSet.member?(acc.quarantined_members, remote_node) or
           Map.has_key?(acc.sync_inflight, remote_node) or
           Map.has_key?(acc.summary_probe_inflight, remote_node) do
        acc
      else
        send_to_member(
          acc,
          remote_node,
          {:ekv_summary_probe, self(), acc.shard_index, local_progress_summary_for_wire(acc),
           acc.node_id}
        )

        mark_summary_probe_inflight(acc, remote_node)
      end
    end)
  end

  defp trigger_missing_member_connects(%Replica{} = state) do
    state.name
    |> EKV.MemberPresence.member_nodes()
    |> Enum.reduce(state, fn remote_node, acc ->
      cond do
        remote_node == node() ->
          acc

        Map.has_key?(acc.remote_shards, remote_node) ->
          acc

        true ->
          send_to_member(acc, remote_node, member_connect_message(acc))
          acc
      end
    end)
  rescue
    _ -> state
  end

  defp expire_stale_inflight(%Replica{} = state) do
    now_ms = System.monotonic_time(:millisecond)

    stale_summary =
      state.summary_probe_inflight
      |> Enum.filter(fn {_remote_node, sent_at_ms} ->
        is_integer(sent_at_ms) and now_ms - sent_at_ms > @summary_probe_timeout_ms
      end)
      |> Enum.map(&elem(&1, 0))

    stale_sync =
      state.sync_inflight
      |> Enum.filter(fn {_remote_node, activity_at_ms} ->
        is_integer(activity_at_ms) and now_ms - activity_at_ms > @sync_inflight_timeout_ms
      end)
      |> Enum.map(&elem(&1, 0))

    state =
      Enum.reduce(stale_summary, state, fn remote_node, acc ->
        sent_at_ms = Map.get(acc.summary_probe_inflight, remote_node)
        age_ms = if is_integer(sent_at_ms), do: max(0, now_ms - sent_at_ms), else: nil

        log(acc, fn ->
          "#{log_prefix_shard(acc)} expiring stale summary probe for #{remote_node}" <>
            format_inflight_age(age_ms)
        end)

        clear_summary_probe_inflight(acc, remote_node)
      end)

    Enum.reduce(stale_sync, state, fn remote_node, acc ->
      activity_at_ms = Map.get(acc.sync_inflight, remote_node)
      age_ms = if is_integer(activity_at_ms), do: max(0, now_ms - activity_at_ms), else: nil

      log(acc, fn ->
        "#{log_prefix_shard(acc)} expiring stale sync inflight for #{remote_node}" <>
          format_inflight_age(age_ms)
      end)

      clear_sync_inflight(acc, remote_node)
    end)
  end

  defp expire_stale_sync_inflight(%Replica{} = state, remote_node) do
    now_ms = System.monotonic_time(:millisecond)

    case Map.get(state.sync_inflight, remote_node) do
      activity_at_ms
      when is_integer(activity_at_ms) and now_ms - activity_at_ms > @sync_inflight_timeout_ms ->
        age_ms = max(0, now_ms - activity_at_ms)

        log(state, fn ->
          "#{log_prefix_shard(state)} expiring stale sync inflight for #{remote_node}" <>
            format_inflight_age(age_ms)
        end)

        clear_sync_inflight(state, remote_node)

      _ ->
        state
    end
  end

  defp expire_stale_delta_origin_inflight(
         %Replica{} = state,
         remote_node,
         {:delta, origin_node, _from_seq}
       )
       when is_atom(remote_node) and is_binary(origin_node) do
    if relayed_delta_request?(state, remote_node, origin_node) do
      now_ms = System.monotonic_time(:millisecond)

      case Map.get(state.delta_origin_inflight, origin_node) do
        {source_node, activity_at_ms}
        when is_atom(source_node) and is_integer(activity_at_ms) and
               now_ms - activity_at_ms > @sync_inflight_timeout_ms ->
          age_ms = max(0, now_ms - activity_at_ms)

          log(state, fn ->
            "#{log_prefix_shard(state)} expiring stale delta origin inflight for #{origin_node} via #{source_node}" <>
              format_inflight_age(age_ms)
          end)

          clear_sync_inflight(state, source_node)

        _ ->
          state
      end
    else
      state
    end
  end

  defp expire_stale_delta_origin_inflight(%Replica{} = state, _remote_node, _request), do: state

  defp format_inflight_age(age_ms) when is_integer(age_ms), do: " age_ms=#{age_ms}"
  defp format_inflight_age(_age_ms), do: ""

  defp schedule_anti_entropy_tick(%Replica{} = state) do
    case EKV.Supervisor.get_config(state.name)[:anti_entropy_interval] do
      interval when is_integer(interval) and interval > 0 ->
        Process.send_after(
          self(),
          :anti_entropy_tick,
          interval + anti_entropy_jitter_ms(interval)
        )

      _ ->
        :ok
    end
  end

  defp anti_entropy_jitter_ms(interval) when interval <= 1_000, do: :rand.uniform(100) - 1

  defp anti_entropy_jitter_ms(interval) do
    max_jitter = min(div(interval, 10), 5_000)
    :rand.uniform(max_jitter) - 1
  end

  defp replace_remote_member_progress(%Replica{} = state, remote_node, remote_progress)
       when is_map(remote_progress) do
    remote_progress = normalize_progress_summary(remote_progress)

    :ok =
      Store.replace_peer_progress(state.db, remote_member_id(state, remote_node), remote_progress)

    %{
      state
      | remote_member_progress:
          Map.put(state.remote_member_progress, remote_node, remote_progress)
    }
  end

  defp replace_remote_member_progress(%Replica{} = state, _remote_node, _remote_progress),
    do: state

  defp merge_remote_member_progress(%Replica{} = state, remote_node, remote_progress)
       when is_map(remote_progress) do
    remote_progress = normalize_progress_summary(remote_progress)

    merged =
      state.remote_member_progress
      |> Map.get(remote_node, %{})
      |> Map.merge(remote_progress, fn _origin, current, incoming -> max(current, incoming) end)

    Enum.each(remote_progress, fn {origin_node, seq} ->
      Store.update_peer_progress(state.db, remote_member_id(state, remote_node), origin_node, seq)
    end)

    %{state | remote_member_progress: Map.put(state.remote_member_progress, remote_node, merged)}
  end

  defp merge_remote_member_progress(%Replica{} = state, _remote_node, _remote_progress), do: state

  defp track_remote_features(%Replica{} = state, remote_node, remote_features) do
    %{state | remote_features: Map.put(state.remote_features, remote_node, remote_features)}
  end

  defp replay_chunk_max_seq(replay_entries) do
    replay_entries
    |> List.last()
    |> elem(4)
  end

  defp member_connect_message(%Replica{} = state) do
    {:ekv_member_connect, self(), state.shard_index, state.num_shards,
     local_progress_summary_for_wire(state), state.node_id}
  end

  defp member_connect_ack_message(%Replica{} = state, progress_summary) do
    {:ekv_member_connect_ack, self(), state.shard_index, state.num_shards, progress_summary,
     state.node_id}
  end

  defp local_progress_summary_for_wire(%Replica{} = state) do
    state.local_progress
    |> normalize_progress_summary()
    |> Map.put(local_origin_id(state), state.local_origin_seq)
  end

  defp local_origin_id(%Replica{} = state) do
    state.node_id || Atom.to_string(node())
  end

  defp remote_member_id(%Replica{} = state, remote_node) when is_atom(remote_node) do
    Map.get(state.member_node_ids, remote_node) ||
      Store.member_node_identity_get(state.db, remote_node) ||
      Atom.to_string(remote_node)
  end

  defp remote_origin_id(%Replica{} = state, remote_node, remote_progress \\ %{})
       when is_atom(remote_node) do
    remote_member_id = remote_member_id(state, remote_node)

    cond do
      is_binary(remote_member_id) and byte_size(remote_member_id) > 0 ->
        remote_member_id

      is_map(remote_progress) and Map.has_key?(remote_progress, Atom.to_string(remote_node)) ->
        Atom.to_string(remote_node)

      true ->
        Atom.to_string(remote_node)
    end
  end

  defp source_node_for_origin(%Replica{} = state, origin_id) when is_binary(origin_id) do
    Enum.find(Map.keys(state.remote_shards), &member_matches_origin?(state, &1, origin_id))
  end

  defp send_to_member(%Replica{} = state, target_node, message) do
    shard_name = shard_name(state.name, state.shard_index)
    encoded_message = wire_encode_message(state, target_node, message)
    destination = {shard_name, target_node}
    best_effort? = best_effort_wire_message?(encoded_message)

    case EKV.Transport.send(state.transport, destination, encoded_message,
           best_effort?: best_effort?,
           target_node: target_node,
           shard_index: state.shard_index
         ) do
      :ok ->
        :ok

      {:error, _reason} when best_effort? ->
        :ok

      {:error, reason} ->
        send(self(), {:ekv_transport_down, target_node, reason})
        {:error, reason}
    end
  end

  defp best_effort_wire_message?({:ekv, @wire_protocol_version, kind, _payload, _meta})
       when kind in [
              :replication_batch,
              :member_connect,
              :member_connect_ack,
              :summary_probe,
              :summary_reply,
              :sync_request,
              :progress_ack
            ],
       do: true

  defp best_effort_wire_message?(_message), do: false

  # Track a remote shard pid in remote_shards. Handles three cases:
  # 1. New node: monitor and add
  # 2. Same pid: no-op
  # 3. Different pid (shard restarted): demonitor old, monitor new, update
  defp track_remote_shard(%Replica{} = state, remote_node, remote_pid) do
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

  defp replicate_live_to_members(%Replica{} = state, message) do
    Enum.reduce(Map.keys(state.remote_shards), state, fn target_node, acc ->
      enqueue_replication_batch(acc, target_node, message)
    end)
  end

  defp enqueue_replication_batch(%Replica{} = state, target_node, message) do
    entry = replication_batch_entry(message)
    entry_bytes = replication_batch_entry_bytes(entry)
    now_ms = System.monotonic_time(:millisecond)

    batch =
      case Map.get(state.replication_batches, target_node) do
        nil ->
          flush_at_ms = replication_batch_deadline_ms(state, now_ms)

          %{
            entries: [entry],
            bytes: entry_bytes,
            flush_at_ms: flush_at_ms,
            timer_ref: schedule_replication_batch_flush(state, target_node, flush_at_ms)
          }

        %{entries: entries, bytes: bytes, flush_at_ms: flush_at_ms, timer_ref: timer_ref} ->
          %{
            entries: [entry | entries],
            bytes: bytes + entry_bytes,
            flush_at_ms: flush_at_ms,
            timer_ref:
              timer_ref || schedule_replication_batch_flush(state, target_node, flush_at_ms)
          }
      end

    state = %{state | replication_batches: Map.put(state.replication_batches, target_node, batch)}

    if length(batch.entries) >= state.replication_batch_max_entries or
         batch.bytes >= state.replication_batch_max_bytes do
      flush_replication_batch(state, target_node)
    else
      state
    end
  end

  defp replication_batch_entry({:ekv_put, key, value_binary, ts, _origin, origin_seq, exp}) do
    {key, value_binary, ts, origin_seq, exp, nil}
  end

  defp replication_batch_entry({:ekv_delete, key, ts, _origin, origin_seq}) do
    {key, nil, ts, origin_seq, nil, ts}
  end

  defp replication_batch_entry_bytes({key, value_binary, _ts, _origin_seq, _exp, _deleted_at}) do
    byte_size(key) + if(is_binary(value_binary), do: byte_size(value_binary), else: 0) + 64
  end

  defp replication_batch_deadline_ms(%Replica{} = state, now_ms) do
    now_ms + state.replication_batch_flush_ms
  end

  defp schedule_replication_batch_flush(%Replica{} = _state, target_node, flush_at_ms) do
    delay_ms = max(flush_at_ms - System.monotonic_time(:millisecond), 0)

    Process.send_after(
      self(),
      {:flush_replication_batch, target_node},
      delay_ms
    )
  end

  defp flush_replication_batch(%Replica{} = state, target_node) do
    case Map.pop(state.replication_batches, target_node) do
      {nil, _batches} ->
        state

      {%{entries: entries, timer_ref: timer_ref}, batches} ->
        cancel_timer(timer_ref)
        state = %{state | replication_batches: batches}

        cond do
          entries == [] ->
            state

          not Map.has_key?(state.remote_shards, target_node) ->
            state

          MapSet.member?(state.quarantined_members, target_node) ->
            state

          true ->
            send_to_member(
              state,
              target_node,
              {:ekv_replication_batch, node(), state.shard_index, local_origin_id(state),
               Enum.reverse(entries)}
            )

            state
        end
    end
  end

  defp flush_all_replication_batches(%Replica{} = state) do
    Enum.reduce(Map.keys(state.replication_batches), state, fn target_node, acc ->
      flush_replication_batch(acc, target_node)
    end)
  end

  defp flush_due_replication_batches(%Replica{} = state) do
    now_ms = System.monotonic_time(:millisecond)

    Enum.reduce(Map.keys(state.replication_batches), state, fn target_node, acc ->
      case Map.get(acc.replication_batches, target_node) do
        %{flush_at_ms: flush_at_ms} when is_integer(flush_at_ms) and flush_at_ms <= now_ms ->
          flush_replication_batch(acc, target_node)

        _ ->
          acc
      end
    end)
  end

  defp replication_batches_due?(%Replica{replication_batches: batches})
       when map_size(batches) == 0,
       do: false

  defp replication_batches_due?(%Replica{} = state) do
    now_ms = System.monotonic_time(:millisecond)

    Enum.any?(state.replication_batches, fn
      {_target_node, %{flush_at_ms: flush_at_ms}} when is_integer(flush_at_ms) ->
        flush_at_ms <= now_ms

      _ ->
        false
    end)
  end

  defp cb_noreply(%Replica{} = state) do
    if replication_batches_due?(state) do
      {:noreply, state, {:continue, :flush_due_replication_batches}}
    else
      {:noreply, state}
    end
  end

  defp cb_reply(reply, %Replica{} = state) do
    if replication_batches_due?(state) do
      {:reply, reply, state, {:continue, :flush_due_replication_batches}}
    else
      {:reply, reply, state}
    end
  end

  defp reply_local_request({:genserver, from}, reply) do
    GenServer.reply(from, reply)
  end

  defp reply_local_request({caller_pid, reply_tag} = from, reply)
       when is_pid(caller_pid) and is_reference(reply_tag) do
    GenServer.reply(from, reply)
  end

  defp reply_local_request({:send, reply_dest, ref}, reply) when is_reference(ref) do
    send(reply_dest, {@local_reply_tag, ref, reply})
    :ok
  end

  defp do_local_request(pid, shard_name, request, timeout) when is_pid(pid) do
    reply_dest = :erlang.alias()
    ref = make_ref()
    mref = Process.monitor(pid)
    send(pid, {@local_request_tag, reply_dest, ref, request})

    receive do
      {@local_reply_tag, ^ref, reply} ->
        Process.demonitor(mref, [:flush])
        :erlang.unalias(reply_dest)
        reply

      {:DOWN, ^mref, :process, ^pid, reason} ->
        :erlang.unalias(reply_dest)
        exit({reason, {GenServer, :call, [shard_name, request, timeout]}})
    after
      timeout ->
        Process.demonitor(mref, [:flush])
        :erlang.unalias(reply_dest)
        exit({:timeout, {GenServer, :call, [shard_name, request, timeout]}})
    end
  end

  defp handle_local_request_message(
         %Replica{handoff_node: handoff_node} = state,
         reply_dest,
         ref,
         request
       )
       when handoff_node != nil do
    shard_name = shard_name(state.name, state.shard_index)

    reply =
      try do
        GenServer.call({shard_name, handoff_node}, request, 5_000)
      catch
        :exit, _ -> {:error, :shutting_down}
      end

    reply_local_request({:send, reply_dest, ref}, reply)
    state
  end

  defp handle_local_request_message(%Replica{} = state, reply_dest, ref, request) do
    from = {:send, reply_dest, ref}

    case request do
      {:put, key, value_binary, opts} ->
        {state, first_item} = build_local_put_batch_item(state, from, key, value_binary, opts)

        {state, batch_items, deferred_request} =
          collect_local_write_batch(
            state,
            [first_item],
            local_request_message_bytes(request)
          )

        state
        |> apply_local_write_batch(batch_items)
        |> maybe_handle_deferred_local_request(deferred_request)

      {:delete, key} ->
        {state, first_item} = build_local_delete_batch_item(state, from, key)

        {state, batch_items, deferred_request} =
          collect_local_write_batch(
            state,
            [first_item],
            local_request_message_bytes(request)
          )

        state
        |> apply_local_write_batch(batch_items)
        |> maybe_handle_deferred_local_request(deferred_request)

      {:cas_put, key, value_binary, expected_vsn, opts} ->
        start_cas(
          state,
          key,
          {:cas_put, expected_vsn, value_binary, opts},
          from,
          cas_deadline_from_opts(opts)
        )

      {:observer_cas_put, key, value_binary, expected_vsn, opts} ->
        start_cas(
          state,
          key,
          {:cas_put, expected_vsn, value_binary, opts},
          from,
          cas_deadline_from_opts(opts),
          :observer_write
        )

      {:cas_delete, key, expected_vsn, opts} ->
        start_cas(
          state,
          key,
          {:cas_delete, expected_vsn, opts},
          from,
          cas_deadline_from_opts(opts)
        )

      {:observer_cas_delete, key, expected_vsn, opts} ->
        start_cas(
          state,
          key,
          {:cas_delete, expected_vsn, opts},
          from,
          cas_deadline_from_opts(opts),
          :observer_write
        )

      {:update, key, fun, opts} ->
        retries = Keyword.get(opts, :retries, 5)
        start_cas(state, key, {:update, fun, opts, retries}, from, cas_deadline_from_opts(opts))

      {:observer_update, key, fun, opts} ->
        retries = Keyword.get(opts, :retries, 5)

        start_cas(
          state,
          key,
          {:update, fun, opts, retries},
          from,
          cas_deadline_from_opts(opts),
          :observer_write
        )

      {:cas_read, key, opts} ->
        retries = Keyword.get(opts, :retries, 5)
        start_cas(state, key, {:cas_read, opts, retries}, from, cas_deadline_from_opts(opts))

      {:observer_cas_read, key, opts} ->
        retries = Keyword.get(opts, :retries, 5)

        start_cas(
          state,
          key,
          {:cas_read, opts, retries},
          from,
          cas_deadline_from_opts(opts),
          :observer_read
        )

      {:apply_observer_commit, key, ballot_c, ballot_n, entry_tuple, origin_node, origin_seq} ->
        {reply, state} =
          handle_apply_observer_commit_request(
            state,
            key,
            ballot_c,
            ballot_n,
            entry_tuple,
            origin_node,
            origin_seq
          )

        reply_local_request(from, reply)
        state

      {:await_quorum, timeout_ms} ->
        case handle_await_quorum_request(state, from, timeout_ms) do
          {:reply, reply, state} ->
            reply_local_request(from, reply)
            state

          {:noreply, state} ->
            state
        end
    end
  end

  defp collect_local_write_batch(%Replica{} = state, batch_items, batch_bytes)
       when is_list(batch_items) and is_integer(batch_bytes) do
    if state.handoff_node != nil do
      {state, batch_items, nil}
    else
      receive do
        {@local_request_tag, reply_dest, ref, request} when is_reference(ref) ->
          case maybe_extend_local_write_batch(
                 state,
                 batch_items,
                 batch_bytes,
                 {:send, reply_dest, ref},
                 request
               ) do
            {:batch, %Replica{} = next_state, next_batch_items, next_batch_bytes} ->
              collect_local_write_batch(next_state, next_batch_items, next_batch_bytes)

            {:defer, %Replica{} = next_state, deferred_request} ->
              {next_state, Enum.reverse(batch_items), deferred_request}
          end

        {:ekv_handoff_request, _ref, _new_node, _caller_pid} = msg ->
          state = process_inline_priority_message(state, msg)
          collect_local_write_batch(state, batch_items, batch_bytes)

        {:ekv, @wire_protocol_version, kind, _payload, _meta} = msg
        when kind != :replication_batch ->
          state = process_inline_priority_message(state, msg)
          collect_local_write_batch(state, batch_items, batch_bytes)

        {:ekv, version, _kind, _payload, _meta} = msg
        when is_integer(version) and version != @wire_protocol_version ->
          state = process_inline_priority_message(state, msg)
          collect_local_write_batch(state, batch_items, batch_bytes)

        {:nodeup, _remote_node} = msg ->
          state = process_inline_priority_message(state, msg)
          collect_local_write_batch(state, batch_items, batch_bytes)

        {:nodedown, _remote_node} = msg ->
          state = process_inline_priority_message(state, msg)
          collect_local_write_batch(state, batch_items, batch_bytes)

        {:ekv_transport_down, _remote_node, _reason} = msg ->
          state = process_inline_priority_message(state, msg)
          collect_local_write_batch(state, batch_items, batch_bytes)

        {:DOWN, _mref, :process, _pid, _reason} = msg ->
          state = process_inline_priority_message(state, msg)
          collect_local_write_batch(state, batch_items, batch_bytes)

        {:await_quorum_timeout, _ref} = msg ->
          state = process_inline_priority_message(state, msg)
          collect_local_write_batch(state, batch_items, batch_bytes)

        {:cas_timeout, _ref} = msg ->
          state = process_inline_priority_message(state, msg)
          collect_local_write_batch(state, batch_items, batch_bytes)

        {:cas_retry, _ref, _key, _operation} = msg ->
          state = process_inline_priority_message(state, msg)
          collect_local_write_batch(state, batch_items, batch_bytes)

        {:gc, _now, _cutoff} = msg ->
          state = process_inline_priority_message(state, msg)
          collect_local_write_batch(state, batch_items, batch_bytes)

        :anti_entropy_tick = msg ->
          state = process_inline_priority_message(state, msg)
          collect_local_write_batch(state, batch_items, batch_bytes)

        {:continue_full_sync, _remote_node, _last_key, _cutoff, _progress_summary, _chunk_size,
         _reason} = msg ->
          state = process_inline_priority_message(state, msg)
          collect_local_write_batch(state, batch_items, batch_bytes)

        {:continue_delta_sync, _remote_node, _origin_node, _last_seq, _my_seq, _chunk_size} =
            msg ->
          state = process_inline_priority_message(state, msg)
          collect_local_write_batch(state, batch_items, batch_bytes)

        {:flush_replication_batch, _remote_node} = msg ->
          state = process_inline_priority_message(state, msg)
          collect_local_write_batch(state, batch_items, batch_bytes)
      after
        0 ->
          {state, Enum.reverse(batch_items), nil}
      end
    end
  end

  defp maybe_extend_local_write_batch(
         %Replica{} = state,
         batch_items,
         batch_bytes,
         from,
         {:put, key, value_binary, opts} = request
       ) do
    request_bytes = local_request_message_bytes(request)

    if length(batch_items) < state.local_write_batch_max_entries and
         batch_bytes + request_bytes <= state.local_write_batch_max_bytes do
      {state, item} = build_local_put_batch_item(state, from, key, value_binary, opts)
      {:batch, state, [item | batch_items], batch_bytes + request_bytes}
    else
      {:defer, state, {elem(from, 1), elem(from, 2), request}}
    end
  end

  defp maybe_extend_local_write_batch(
         %Replica{} = state,
         batch_items,
         batch_bytes,
         from,
         {:delete, key} = request
       ) do
    request_bytes = local_request_message_bytes(request)

    if length(batch_items) < state.local_write_batch_max_entries and
         batch_bytes + request_bytes <= state.local_write_batch_max_bytes do
      {state, item} = build_local_delete_batch_item(state, from, key)
      {:batch, state, [item | batch_items], batch_bytes + request_bytes}
    else
      {:defer, state, {elem(from, 1), elem(from, 2), request}}
    end
  end

  defp maybe_extend_local_write_batch(
         %Replica{} = state,
         _batch_items,
         _batch_bytes,
         {:send, caller_pid, ref},
         request
       ) do
    {:defer, state, {caller_pid, ref, request}}
  end

  defp maybe_handle_deferred_local_request(%Replica{} = state, nil), do: state

  defp maybe_handle_deferred_local_request(
         %Replica{} = state,
         {reply_dest, ref, request}
       )
       when is_reference(ref) do
    handle_local_request_message(state, reply_dest, ref, request)
  end

  defp apply_local_write_batch(%Replica{} = state, batch_items) when is_list(batch_items) do
    if state.handoff_node != nil do
      proxy_local_write_batch(state, batch_items)
    else
      do_apply_local_write_batch(state, batch_items)
    end
  end

  defp do_apply_local_write_batch(%Replica{} = state, batch_items) when is_list(batch_items) do
    local_entries =
      Enum.map(batch_items, fn item ->
        {item.key, item.value_binary, item.timestamp, item.expires_at, item.deleted_at}
      end)

    initial_delete_values = local_batch_initial_delete_values(state, batch_items)
    origin_node = local_origin_id(state)

    case Store.write_local_entries_batch(
           state.db,
           state.stmts.kv_upsert,
           state.stmts.keyref_upsert,
           state.stmts.oplog_insert,
           origin_node,
           state.local_origin_seq,
           local_entries
         ) do
      {:ok, results, final_origin_seq} ->
        state =
          if is_integer(final_origin_seq) and final_origin_seq > state.local_origin_seq do
            set_local_origin_seq(state, final_origin_seq)
          else
            state
          end

        state =
          Enum.zip(batch_items, results)
          |> Enum.reduce(state, fn
            {%{type: :put} = item, {:applied, origin_seq}}, acc ->
              reply_local_request(item.from, :ok)

              replicate_live_to_members(
                acc,
                {:ekv_put, item.key, item.value_binary, item.timestamp, origin_node, origin_seq,
                 item.expires_at}
              )

            {%{type: :delete} = item, {:applied, origin_seq}}, acc ->
              reply_local_request(item.from, :ok)

              replicate_live_to_members(
                acc,
                {:ekv_delete, item.key, item.timestamp, origin_node, origin_seq}
              )

            {item, :ignored}, acc ->
              reply_local_request(item.from, :ok)
              acc

            {item, :cas_managed_key}, acc ->
              reply_local_request(item.from, {:error, :cas_managed_key})
              acc
          end)

        dispatch_events(state, local_batch_events(batch_items, results, initial_delete_values))
        state

      {:error, _reason} ->
        apply_local_write_batch_fallback(state, batch_items)
    end
  end

  defp proxy_local_write_batch(%Replica{handoff_node: handoff_node} = state, batch_items)
       when handoff_node != nil do
    shard_name = shard_name(state.name, state.shard_index)

    Enum.reduce(batch_items, state, fn item, acc ->
      reply =
        try do
          GenServer.call({shard_name, handoff_node}, item.request, 5_000)
        catch
          :exit, _ -> {:error, :shutting_down}
        end

      reply_local_request(item.from, reply)
      acc
    end)
  end

  defp apply_local_write_batch_fallback(%Replica{} = state, batch_items) do
    Enum.reduce(batch_items, state, fn item, acc ->
      {reply, acc} =
        case item.type do
          :put ->
            apply_single_local_batch_item(
              acc,
              item.key,
              item.value_binary,
              item.timestamp,
              item.expires_at,
              nil
            )

          :delete ->
            apply_single_local_batch_item(
              acc,
              item.key,
              nil,
              item.timestamp,
              nil,
              item.deleted_at
            )
        end

      reply_local_request(item.from, reply)
      acc
    end)
  end

  defp apply_single_local_batch_item(
         %Replica{} = state,
         key,
         value_binary,
         timestamp,
         expires_at,
         deleted_at
       ) do
    prev_value = if deleted_at && has_subscribers?(state), do: read_previous_value(state, key)
    origin_node = local_origin_id(state)

    case Store.write_entry(
           state.db,
           state.stmts.kv_upsert,
           state.stmts.keyref_upsert,
           state.stmts.oplog_insert,
           key,
           value_binary,
           timestamp,
           origin_node,
           expires_at,
           deleted_at,
           nil,
           true,
           true
         ) do
      {:ok, true, origin_seq, local_progress_seq} ->
        state =
          state
          |> set_local_origin_seq(origin_seq)
          |> merge_local_progress_seq(origin_node, local_progress_seq)

        state =
          if is_integer(deleted_at) do
            dispatch_events(state, [%EKV.Event{type: :delete, key: key, value: prev_value}])

            replicate_live_to_members(
              state,
              {:ekv_delete, key, timestamp, origin_node, origin_seq}
            )
          else
            dispatch_events(state, [
              %EKV.Event{type: :put, key: key, value: :erlang.binary_to_term(value_binary)}
            ])

            replicate_live_to_members(
              state,
              {:ekv_put, key, value_binary, timestamp, origin_node, origin_seq, expires_at}
            )
          end

        {:ok, state}

      {:ok, false, _origin_seq, local_progress_seq} ->
        {:ok, merge_local_progress_seq(state, origin_node, local_progress_seq)}

      {:ok, false} ->
        {:ok, state}

      {:error, :cas_managed_key} ->
        {{:error, :cas_managed_key}, state}
    end
    |> normalize_local_write_result()
  end

  defp local_batch_initial_delete_values(%Replica{} = state, batch_items) do
    if has_subscribers?(state) do
      batch_items
      |> Enum.reduce(MapSet.new(), fn
        %{type: :delete, key: key}, acc -> MapSet.put(acc, key)
        _item, acc -> acc
      end)
      |> Map.new(fn key -> {key, read_previous_value(state, key)} end)
    else
      %{}
    end
  end

  defp local_batch_events(batch_items, results, initial_delete_values) do
    {events, _shadow_values} =
      batch_items
      |> Enum.zip(results)
      |> Enum.reduce({[], initial_delete_values}, fn
        {%{type: :put}, :cas_managed_key}, {acc_events, shadow_values} ->
          {acc_events, shadow_values}

        {%{type: :put}, :ignored}, {acc_events, shadow_values} ->
          {acc_events, shadow_values}

        {%{type: :delete}, :cas_managed_key}, {acc_events, shadow_values} ->
          {acc_events, shadow_values}

        {%{type: :delete}, :ignored}, {acc_events, shadow_values} ->
          {acc_events, shadow_values}

        {%{type: :delete, key: key}, {:applied, _origin_seq}}, {acc_events, shadow_values} ->
          event = %EKV.Event{type: :delete, key: key, value: Map.get(shadow_values, key)}
          {[event | acc_events], Map.put(shadow_values, key, nil)}

        {%{type: :put, key: key, value_binary: value_binary}, {:applied, _origin_seq}},
        {acc_events, shadow_values} ->
          value = :erlang.binary_to_term(value_binary)
          event = %EKV.Event{type: :put, key: key, value: value}
          {[event | acc_events], Map.put(shadow_values, key, value)}
      end)

    Enum.reverse(events)
  end

  defp build_local_put_batch_item(%Replica{} = state, from, key, value_binary, opts) do
    {now, state} = next_lww_ts(state)
    ttl = Keyword.get(opts, :ttl)
    expires_at = if ttl, do: now + ttl * 1_000_000

    {state,
     %{
       type: :put,
       from: from,
       request: {:put, key, value_binary, opts},
       key: key,
       value_binary: value_binary,
       timestamp: now,
       expires_at: expires_at,
       deleted_at: nil
     }}
  end

  defp build_local_delete_batch_item(%Replica{} = state, from, key) do
    {now, state} = next_lww_ts(state)

    {state,
     %{
       type: :delete,
       from: from,
       request: {:delete, key},
       key: key,
       value_binary: nil,
       timestamp: now,
       expires_at: nil,
       deleted_at: now
     }}
  end

  defp local_request_message_bytes({:put, key, value_binary, _opts})
       when is_binary(key) and is_binary(value_binary),
       do: byte_size(key) + byte_size(value_binary) + 64

  defp local_request_message_bytes({:delete, key}) when is_binary(key), do: byte_size(key) + 64
  defp local_request_message_bytes(_request), do: 64

  defp process_inline_priority_message(%Replica{} = state, msg) do
    case handle_info(msg, state) do
      {:noreply, %Replica{} = next_state} ->
        next_state

      {:noreply, %Replica{} = next_state, {:continue, :flush_due_replication_batches}} ->
        flush_due_replication_batches(next_state)
    end
  end

  defp take_priority_turn(%Replica{} = state) do
    if state.handoff_node != nil do
      state
    else
      state
      |> take_priority_control_turn(max(1, state.local_write_batch_max_entries))
      |> take_one_local_request_turn()
    end
  end

  defp take_priority_control_turn(%Replica{} = state, remaining_control_budget)
       when remaining_control_budget > 0 do
    receive do
      {:ekv_handoff_request, _ref, _new_node, _caller_pid} = msg ->
        state = process_inline_priority_message(state, msg)
        take_priority_control_turn(state, remaining_control_budget - 1)

      {:ekv, @wire_protocol_version, kind, _payload, _meta} = msg
      when kind != :replication_batch ->
        state = process_inline_priority_message(state, msg)
        take_priority_control_turn(state, remaining_control_budget - 1)

      {:ekv, version, _kind, _payload, _meta} = msg
      when is_integer(version) and version != @wire_protocol_version ->
        state = process_inline_priority_message(state, msg)
        take_priority_control_turn(state, remaining_control_budget - 1)

      {:nodeup, _remote_node} = msg ->
        state = process_inline_priority_message(state, msg)
        take_priority_control_turn(state, remaining_control_budget - 1)

      {:nodedown, _remote_node} = msg ->
        state = process_inline_priority_message(state, msg)
        take_priority_control_turn(state, remaining_control_budget - 1)

      {:ekv_transport_down, _remote_node, _reason} = msg ->
        state = process_inline_priority_message(state, msg)
        take_priority_control_turn(state, remaining_control_budget - 1)

      {:DOWN, _mref, :process, _pid, _reason} = msg ->
        state = process_inline_priority_message(state, msg)
        take_priority_control_turn(state, remaining_control_budget - 1)

      {:await_quorum_timeout, _ref} = msg ->
        state = process_inline_priority_message(state, msg)
        take_priority_control_turn(state, remaining_control_budget - 1)

      {:cas_timeout, _ref} = msg ->
        state = process_inline_priority_message(state, msg)
        take_priority_control_turn(state, remaining_control_budget - 1)

      {:cas_retry, _ref, _key, _operation} = msg ->
        state = process_inline_priority_message(state, msg)
        take_priority_control_turn(state, remaining_control_budget - 1)

      {:gc, _now, _cutoff} = msg ->
        state = process_inline_priority_message(state, msg)
        take_priority_control_turn(state, remaining_control_budget - 1)

      :anti_entropy_tick = msg ->
        state = process_inline_priority_message(state, msg)
        take_priority_control_turn(state, remaining_control_budget - 1)

      {:continue_full_sync, _remote_node, _last_key, _cutoff, _progress_summary, _chunk_size,
       _reason} = msg ->
        state = process_inline_priority_message(state, msg)
        take_priority_control_turn(state, remaining_control_budget - 1)

      {:continue_delta_sync, _remote_node, _origin_node, _last_seq, _my_seq, _chunk_size} = msg ->
        state = process_inline_priority_message(state, msg)
        take_priority_control_turn(state, remaining_control_budget - 1)

      {:flush_replication_batch, _remote_node} = msg ->
        state = process_inline_priority_message(state, msg)
        take_priority_control_turn(state, remaining_control_budget - 1)
    after
      0 ->
        state
    end
  end

  defp take_priority_control_turn(%Replica{} = state, _remaining_control_budget), do: state

  defp take_one_local_request_turn(%Replica{} = state) do
    receive do
      {@local_request_tag, reply_dest, ref, request} when is_reference(ref) ->
        handle_local_request_message(state, reply_dest, ref, request)
    after
      0 ->
        state
    end
  end

  defp drain_pending_local_requests(%Replica{} = state) do
    receive do
      {@local_request_tag, reply_dest, ref, request} when is_reference(ref) ->
        state
        |> handle_local_request_message(reply_dest, ref, request)
        |> drain_pending_local_requests()
    after
      0 ->
        state
    end
  end

  defp drain_pending_local_requests_with_reply(reply) do
    receive do
      {@local_request_tag, reply_dest, ref, _request} when is_reference(ref) ->
        send(reply_dest, {@local_reply_tag, ref, reply})
        drain_pending_local_requests_with_reply(reply)
    after
      0 ->
        :ok
    end
  end

  defp drop_replication_batch(%Replica{} = state, remote_node) do
    case Map.pop(state.replication_batches, remote_node) do
      {nil, _} ->
        state

      {%{timer_ref: timer_ref}, batches} ->
        cancel_timer(timer_ref)
        %{state | replication_batches: batches}
    end
  end

  # Broadcast committed CAS entry to all members.
  #
  # Members that already acknowledged accept for a unique node_id only need the
  # ballot/key commit signal; they can promote from local kv_paxos state. Members
  # that may have missed accept (or whose node_id is ambiguous during blue-green
  # overlap) still receive the full entry_tuple so they can recover via
  # paxos_accept + paxos_promote on commit.
  defp broadcast_cas_commit(
         %Replica{} = state,
         %{key: key, ballot: {ballot_c, ballot_n}} = op,
         origin_seq
       ) do
    for {target_node, _pid} <- state.remote_shards do
      entry_tuple = commit_payload_for_member(state, target_node, op)

      send_to_member(
        state,
        target_node,
        {:ekv_cas_committed, key, ballot_c, ballot_n, entry_tuple, state.shard_index,
         local_origin_id(state), origin_seq}
      )
    end
  end

  defp commit_message(
         %Replica{} = state,
         %{key: key, ballot: {ballot_c, ballot_n}, entry_tuple: entry_tuple},
         origin_seq
       ) do
    {:ekv_cas_committed, key, ballot_c, ballot_n, entry_tuple, state.shard_index,
     local_origin_id(state), origin_seq}
  end

  defp wire_encode_message(
         %Replica{} = state,
         target_node,
         {:ekv_replication_batch, from_node, shard, origin, entries}
       ) do
    compress? = remote_supports_feature?(state, target_node, @wire_feature_compression)

    payload =
      {from_node, shard, origin,
       wire_compress_replication_batch_entries(state, entries, compress?)}

    {:ekv, @wire_protocol_version, :replication_batch, payload, %{}}
  end

  defp wire_encode_message(
         %Replica{} = state,
         target_node,
         {:ekv_accept, ref, proposer_pid, key, ballot_c, ballot_n, entry_tuple, shard}
       ) do
    compress? = remote_supports_feature?(state, target_node, @wire_feature_compression)

    payload =
      {ref, proposer_pid, key, ballot_c, ballot_n,
       wire_compress_entry_tuple(state, entry_tuple, compress?), shard}

    {:ekv, @wire_protocol_version, :accept, payload, %{}}
  end

  defp wire_encode_message(
         %Replica{} = state,
         target_node,
         {:ekv_cas_committed, key, ballot_c, ballot_n, entry_tuple, shard, origin_node,
          origin_seq}
       ) do
    compress? = remote_supports_feature?(state, target_node, @wire_feature_compression)

    payload =
      {key, ballot_c, ballot_n, wire_compress_entry_tuple(state, entry_tuple, compress?), shard,
       origin_node, origin_seq}

    {:ekv, @wire_protocol_version, :cas_committed, payload, %{}}
  end

  defp wire_encode_message(
         %Replica{} = state,
         _target_node,
         {:ekv_member_connect, pid, shard, num_shards, remote_progress, remote_node_id}
       ) do
    {:ekv, @wire_protocol_version, :member_connect,
     {pid, shard, num_shards, remote_progress, remote_node_id},
     %{features: wire_features_meta(state)}}
  end

  defp wire_encode_message(
         %Replica{} = state,
         _target_node,
         {:ekv_member_connect_ack, pid, shard, num_shards, remote_progress, remote_node_id}
       ) do
    {:ekv, @wire_protocol_version, :member_connect_ack,
     {pid, shard, num_shards, remote_progress, remote_node_id},
     %{features: wire_features_meta(state)}}
  end

  defp wire_encode_message(
         %Replica{} = _state,
         _target_node,
         {:ekv_sync, from_node, shard, mode, entries, progress}
       ) do
    {:ekv, @wire_protocol_version, :sync, {from_node, shard, mode, entries, progress}, %{}}
  end

  defp wire_encode_message(
         %Replica{} = state,
         _target_node,
         {:ekv_summary_probe, pid, shard, progress, remote_node_id}
       ) do
    {:ekv, @wire_protocol_version, :summary_probe, {pid, shard, progress},
     summary_wire_meta(state, remote_node_id)}
  end

  defp wire_encode_message(
         %Replica{} = state,
         _target_node,
         {:ekv_summary_reply, pid, shard, progress, remote_node_id}
       ) do
    {:ekv, @wire_protocol_version, :summary_reply, {pid, shard, progress},
     summary_wire_meta(state, remote_node_id)}
  end

  defp wire_encode_message(
         %Replica{} = _state,
         _target_node,
         {:ekv_summary_probe, pid, shard, progress}
       ) do
    {:ekv, @wire_protocol_version, :summary_probe, {pid, shard, progress}, %{}}
  end

  defp wire_encode_message(
         %Replica{} = _state,
         _target_node,
         {:ekv_summary_reply, pid, shard, progress}
       ) do
    {:ekv, @wire_protocol_version, :summary_reply, {pid, shard, progress}, %{}}
  end

  defp wire_encode_message(
         %Replica{} = _state,
         _target_node,
         {:ekv_sync_request, pid, shard, {:full, explicit_full_reason}}
       ) do
    {:ekv, @wire_protocol_version, :sync_request, {pid, shard, :full},
     %{explicit_full_reason: explicit_full_reason}}
  end

  defp wire_encode_message(
         %Replica{} = _state,
         _target_node,
         {:ekv_sync_request, pid, shard, request}
       ) do
    {:ekv, @wire_protocol_version, :sync_request, {pid, shard, request}, %{}}
  end

  defp wire_encode_message(
         %Replica{} = _state,
         _target_node,
         {:ekv_progress_ack, pid, shard, mode, progress}
       ) do
    {:ekv, @wire_protocol_version, :progress_ack, {pid, shard, mode, progress}, %{}}
  end

  defp wire_encode_message(
         %Replica{} = _state,
         _target_node,
         {:ekv_prepare, ref, proposer_pid, key, ballot_c, ballot_n, shard}
       ) do
    {:ekv, @wire_protocol_version, :prepare, {ref, proposer_pid, key, ballot_c, ballot_n, shard},
     %{}}
  end

  defp wire_encode_message(
         %Replica{} = _state,
         _target_node,
         {:ekv_promise, ref, pid, node_id, acc_c, acc_n, kv_row}
       ) do
    {:ekv, @wire_protocol_version, :promise, {ref, pid, node_id, acc_c, acc_n, kv_row}, %{}}
  end

  defp wire_encode_message(
         %Replica{} = _state,
         _target_node,
         {:ekv_nack, ref, pid, node_id, promised_c, promised_n}
       ) do
    {:ekv, @wire_protocol_version, :nack, {ref, pid, node_id, promised_c, promised_n}, %{}}
  end

  defp wire_encode_message(%Replica{} = _state, _target_node, {:ekv_accepted, ref, pid, node_id}) do
    {:ekv, @wire_protocol_version, :accepted, {ref, pid, node_id}, %{}}
  end

  defp wire_encode_message(
         %Replica{} = _state,
         _target_node,
         {:ekv_accept_nack, ref, pid, node_id}
       ) do
    {:ekv, @wire_protocol_version, :accept_nack, {ref, pid, node_id}, %{}}
  end

  defp wire_encode_message(%Replica{} = _state, _target_node, message), do: message

  defp decode_wire_message(:replication_batch, {from_node, shard, origin, entries}, _meta) do
    {:ok, {:ekv_replication_batch, from_node, shard, origin, entries}}
  end

  defp decode_wire_message(
         :member_connect,
         {pid, shard, num_shards, remote_progress, remote_node_id},
         meta
       ) do
    {:ok,
     {:ekv_member_connect, pid, shard, num_shards, remote_progress, remote_node_id,
      normalize_wire_features(meta)}}
  end

  defp decode_wire_message(
         :member_connect_ack,
         {pid, shard, num_shards, remote_progress, remote_node_id},
         meta
       ) do
    {:ok,
     {:ekv_member_connect_ack, pid, shard, num_shards, remote_progress, remote_node_id,
      normalize_wire_features(meta)}}
  end

  defp decode_wire_message(:sync, {from_node, shard, mode, entries, progress}, _meta) do
    {:ok, {:ekv_sync, from_node, shard, mode, entries, progress}}
  end

  defp decode_wire_message(:summary_probe, {pid, shard, progress}, meta) do
    {:ok, {:ekv_summary_probe, pid, shard, progress, wire_meta_node_id(meta)}}
  end

  defp decode_wire_message(:summary_reply, {pid, shard, progress}, meta) do
    {:ok, {:ekv_summary_reply, pid, shard, progress, wire_meta_node_id(meta)}}
  end

  defp decode_wire_message(:sync_request, {pid, shard, :full}, meta) do
    case wire_meta_explicit_full_reason(meta) do
      nil ->
        {:ok, {:ekv_sync_request, pid, shard, :full}}

      explicit_full_reason ->
        {:ok, {:ekv_sync_request, pid, shard, {:full, explicit_full_reason}}}
    end
  end

  defp decode_wire_message(:sync_request, {pid, shard, request}, _meta) do
    {:ok, {:ekv_sync_request, pid, shard, request}}
  end

  defp decode_wire_message(:progress_ack, {pid, shard, mode, progress}, _meta) do
    {:ok, {:ekv_progress_ack, pid, shard, mode, progress}}
  end

  defp decode_wire_message(:prepare, {ref, proposer_pid, key, ballot_c, ballot_n, shard}, _meta) do
    {:ok, {:ekv_prepare, ref, proposer_pid, key, ballot_c, ballot_n, shard}}
  end

  defp decode_wire_message(
         :accept,
         {ref, proposer_pid, key, ballot_c, ballot_n, entry_tuple, shard},
         _meta
       ) do
    {:ok, {:ekv_accept, ref, proposer_pid, key, ballot_c, ballot_n, entry_tuple, shard}}
  end

  defp decode_wire_message(
         :cas_committed,
         {key, ballot_c, ballot_n, entry_tuple, shard, origin_node, origin_seq},
         _meta
       ) do
    {:ok,
     {:ekv_cas_committed, key, ballot_c, ballot_n, entry_tuple, shard, origin_node, origin_seq}}
  end

  defp decode_wire_message(:promise, {ref, pid, node_id, acc_c, acc_n, kv_row}, _meta) do
    {:ok, {:ekv_promise, ref, pid, node_id, acc_c, acc_n, kv_row}}
  end

  defp decode_wire_message(:nack, {ref, pid, node_id, promised_c, promised_n}, _meta) do
    {:ok, {:ekv_nack, ref, pid, node_id, promised_c, promised_n}}
  end

  defp decode_wire_message(:accepted, {ref, pid, node_id}, _meta) do
    {:ok, {:ekv_accepted, ref, pid, node_id}}
  end

  defp decode_wire_message(:accept_nack, {ref, pid, node_id}, _meta) do
    {:ok, {:ekv_accept_nack, ref, pid, node_id}}
  end

  defp decode_wire_message(_kind, _payload, _meta), do: :ignore

  defp wire_compress_entry_tuple(%Replica{} = _state, nil, _compress?), do: nil

  defp wire_compress_entry_tuple(
         %Replica{} = state,
         {key, value_binary, timestamp, origin_node_str, expires_at, deleted_at},
         compress?
       ) do
    {key, maybe_wire_compress_value(state, value_binary, compress?), timestamp, origin_node_str,
     expires_at, deleted_at}
  end

  defp wire_compress_replication_batch_entries(%Replica{} = state, entries, compress?) do
    Enum.map(entries, fn
      {key, value_binary, timestamp, origin_seq, expires_at, deleted_at} ->
        {key, maybe_wire_compress_value(state, value_binary, compress?), timestamp, origin_seq,
         expires_at, deleted_at}
    end)
  end

  defp wire_decompress_replication_batch_entries(entries) when is_list(entries) do
    Enum.map(entries, fn
      {key, value_binary, timestamp, origin_seq, expires_at, deleted_at} ->
        {key, wire_decompress_value(value_binary), timestamp, origin_seq, expires_at, deleted_at}
    end)
  end

  defp maybe_wire_compress_value(%Replica{} = _state, nil, _compress?), do: nil

  defp maybe_wire_compress_value(%Replica{} = _state, value_binary, false),
    do: value_binary

  defp maybe_wire_compress_value(
         %Replica{wire_compression_threshold: threshold},
         value_binary,
         true
       )
       when threshold in [false, nil],
       do: value_binary

  defp maybe_wire_compress_value(
         %Replica{wire_compression_threshold: threshold},
         value_binary,
         true
       )
       when is_binary(value_binary) do
    if byte_size(value_binary) >= threshold do
      {@wire_compressed_tag, compress_binary(value_binary)}
    else
      value_binary
    end
  end

  defp remote_supports_feature?(%Replica{} = state, remote_node, feature) do
    case Map.get(state.remote_features, remote_node) do
      %MapSet{} = features -> MapSet.member?(features, feature)
      _ -> false
    end
  end

  defp wire_features_meta(%Replica{} = state) do
    %{
      @wire_feature_live_progress => true,
      @wire_feature_compression => true,
      @wire_feature_observer => not state.cas_voter?
    }
  end

  defp normalize_wire_features(%{features: features}) when is_map(features) do
    features
    |> Enum.filter(fn {_feature, enabled?} -> enabled? end)
    |> Enum.map(fn {feature, _enabled?} -> feature end)
    |> MapSet.new()
  end

  defp normalize_wire_features(_meta), do: MapSet.new()

  defp wire_meta_node_id(%{node_id: node_id}) when is_binary(node_id) and byte_size(node_id) > 0,
    do: node_id

  defp wire_meta_node_id(_meta), do: nil

  defp wire_meta_explicit_full_reason(%{explicit_full_reason: explicit_full_reason}),
    do: explicit_full_reason

  defp wire_meta_explicit_full_reason(_meta), do: nil

  defp summary_wire_meta(%Replica{} = _state, node_id)
       when is_binary(node_id) and byte_size(node_id) > 0 do
    %{node_id: node_id}
  end

  defp summary_wire_meta(%Replica{} = _state, _node_id), do: %{}

  defp normalize_progress_summary(progress) when is_map(progress) do
    Map.new(progress, fn
      {origin_node, seq} when is_binary(origin_node) and is_integer(seq) and seq >= 0 ->
        {origin_node, seq}

      {origin_node, seq} when is_binary(origin_node) and is_integer(seq) ->
        {origin_node, max(seq, 0)}

      {origin_node, seq} when is_atom(origin_node) and is_integer(seq) and seq >= 0 ->
        {Atom.to_string(origin_node), seq}

      {origin_node, seq} when is_atom(origin_node) and is_integer(seq) ->
        {Atom.to_string(origin_node), max(seq, 0)}
    end)
  end

  defp normalize_progress_summary(_progress), do: %{}

  defp normalize_origin_node(origin_node) when is_binary(origin_node), do: origin_node

  defp normalize_origin_node(origin_node) when is_atom(origin_node),
    do: Atom.to_string(origin_node)

  defp normalize_origin_node(origin_node) when is_integer(origin_node),
    do: Integer.to_string(origin_node)

  defp normalize_origin_node(origin_node), do: to_string(origin_node)

  # Runs on the receiver member. Raw and compressed value payloads are both accepted.
  defp wire_decompress_entry_tuple(nil), do: nil

  defp wire_decompress_entry_tuple(
         {key, value_binary, timestamp, origin_node_str, expires_at, deleted_at}
       ) do
    {key, wire_decompress_value(value_binary), timestamp, origin_node_str, expires_at, deleted_at}
  end

  defp wire_decompress_value({@wire_compressed_tag, compressed_binary})
       when is_binary(compressed_binary) do
    :zlib.uncompress(compressed_binary)
  end

  defp wire_decompress_value(value_binary), do: value_binary

  defp compress_binary(binary) when is_binary(binary) do
    z = :zlib.open()

    try do
      :ok = :zlib.deflateInit(z, 1)
      z |> :zlib.deflate(binary, :finish) |> IO.iodata_to_binary()
    after
      :zlib.close(z)
    end
  end

  # =====================================================================
  # CAS helpers
  # =====================================================================

  defp start_cas(%Replica{} = state, key, operation, from, deadline_ms, reply_mode \\ :normal) do
    %{db: db, cluster_size: cluster_size, node_id: node_id} = state
    quorum = div(cluster_size, 2) + 1

    # Check quorum achievable
    alive_count = alive_node_id_count(state)

    cond do
      alive_count > cluster_size ->
        all_ids =
          [state.node_id | Map.values(state.member_node_ids) |> Enum.reject(&is_nil/1)]
          |> Enum.uniq()

        Logger.error(
          "#{log_prefix(state)} cluster overflow: #{alive_count} distinct node_ids " <>
            "but cluster_size=#{cluster_size}. IDs: #{inspect(all_ids)}"
        )

        reply_cas_reply(from, reply_mode, {:error, :cluster_overflow})
        state

      alive_count < quorum ->
        log(state, fn ->
          "#{log_prefix(state)} CAS no_quorum: #{alive_count}/#{cluster_size} " <>
            "node_ids reachable, need #{quorum}"
        end)

        reply_cas_reply(from, reply_mode, {:error, :no_quorum})
        state

      cas_deadline_expired?(deadline_ms) ->
        reply_cas_reply(from, reply_mode, {:error, :quorum_timeout})
        state

      true ->
        timer = arm_cas_timeout(ref = make_ref(), deadline_ms)

        if timer == :expired do
          reply_cas_reply(from, reply_mode, {:error, :quorum_timeout})
          state
        else
          # Generate ballot
          {ballot_c, ballot_n, %Replica{} = state} = next_ballot(state)

          # Local prepare (this node is always an acceptor)
          local_result = Store.paxos_prepare(db, key, ballot_c, ballot_n)

          {local_promise, local_nack} =
            case local_result do
              {:ok, :promise, acc_c, acc_n, kv_row} ->
                {[{node_id, acc_c, acc_n, kv_row}], 0}

              {:ok, :nack, _prom_c, _prom_n} ->
                {[], 1}
            end

          # Send prepare to voter members only.
          for {remote_node, _pid} <- state.remote_shards, remote_cas_voter?(state, remote_node) do
            send_to_member(
              state,
              remote_node,
              {:ekv_prepare, ref, self(), key, ballot_c, ballot_n, state.shard_index}
            )
          end

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
            deadline_ms: deadline_ms,
            reply_mode: reply_mode,
            reply_value: nil,
            broadcast_msg: nil,
            entry_tuple: nil,
            events: []
          }

          # Check if local promise already gave us quorum (cluster_size: 1)
          cond do
            length(op.promises) >= quorum ->
              state = %{state | pending_cas: Map.put(state.pending_cas, ref, op)}
              enter_accept_phase(state, ref, op)

            local_nack > 0 and alive_count - local_nack < quorum ->
              # Can't reach quorum
              cancel_timer(timer)
              new_state = %{state | pending_cas: Map.put(state.pending_cas, ref, op)}
              handle_cas_failure(new_state, ref, op)

            true ->
              %{state | pending_cas: Map.put(state.pending_cas, ref, op)}
          end
        end
    end
  end

  defp next_ballot(%Replica{} = state) do
    counter = max(System.system_time(:nanosecond), state.ballot_counter + 1)
    {counter, state.node_id, %{state | ballot_counter: counter}}
  end

  defp enter_accept_phase(%Replica{} = state, ref, op) do
    cancel_timer(op.timer)

    if cas_deadline_expired?(op.deadline_ms) do
      handle_cas_timeout_now(state, ref, %{op | timer: nil})
    else
      # Find highest accepted ballot from promises
      {_best_node_id, best_acc_c, best_acc_n, best_kv_row} =
        Enum.max_by(op.promises, fn {_nid, acc_c, acc_n, _row} -> {acc_c, acc_n} end)

      # The value with the highest accepted ballot is the current state.
      # If all accepted ballots are {0, 0}, no value was ever accepted —
      # pick the kv_row with the highest {timestamp, origin} to match LWW
      # ordering. This ensures deterministic selection regardless of message
      # arrival order.
      selected_kv_row =
        if best_acc_c == 0 and best_acc_n in ["", nil] do
          op.promises
          |> Enum.map(fn {_, _, _, row} -> row end)
          |> Enum.reject(&is_nil/1)
          |> Enum.max_by(fn [_val, ts, origin | _] -> {ts, origin} end, fn -> nil end)
        else
          best_kv_row
        end

      if cas_read_returns_committed_absent?(
           state,
           op.operation,
           op.key,
           best_acc_c,
           best_acc_n,
           selected_kv_row
         ) do
        reply_cas_reply(op.from, op.reply_mode, {:ok, nil, nil})
        %{state | pending_cas: Map.delete(state.pending_cas, ref)}
      else
        {current_value, current_vsn} = decode_kv_row(selected_kv_row)

        # Apply operation. For :cas_read recovery, pass the raw kv_row so
        # metadata (expires_at, deleted_at) is preserved.
        apply_result =
          case op.operation do
            {:cas_read, _, _} ->
              apply_cas_read_recovery(state, op.key, selected_kv_row, current_value, current_vsn)

            _ ->
              apply_operation(state, op.operation, op.key, current_value, current_vsn)
          end

        case apply_result do
          {:ok, _new_value_binary, new_entry_tuple, reply_value, broadcast_msg, events} ->
            enter_accept_phase_with_entry(
              state,
              ref,
              op,
              new_entry_tuple,
              reply_value,
              broadcast_msg,
              events
            )

          {:error, :conflict} ->
            maybe_repair_conflict_visibility(
              state,
              ref,
              op,
              selected_kv_row,
              current_value,
              current_vsn
            )
        end
      end
    end
  end

  defp enter_accept_phase_with_entry(
         %Replica{} = state,
         ref,
         op,
         new_entry_tuple,
         reply_value,
         broadcast_msg,
         events
       ) do
    {_key, value_binary, ts, origin_str, expires_at, deleted_at} = new_entry_tuple
    value_args = [value_binary, ts, origin_str, expires_at, deleted_at]
    {ballot_c, ballot_n} = op.ballot

    # Local accept first. We only count this node toward quorum after the
    # accept is durably recorded. This avoids "assumed self-accept" races.
    case Store.paxos_accept(state.db, op.key, ballot_c, ballot_n, value_args) do
      {:ok, false} ->
        handle_cas_failure(state, ref, op)

      {:ok, true} ->
        # Send accept to voter members only.
        for {remote_node, _pid} <- state.remote_shards, remote_cas_voter?(state, remote_node) do
          send_to_member(
            state,
            remote_node,
            {:ekv_accept, ref, self(), op.key, ballot_c, ballot_n, new_entry_tuple,
             state.shard_index}
          )
        end

        op = %{
          op
          | phase: :accept,
            accepts: MapSet.new([state.node_id]),
            accept_nacks: 0,
            responded: MapSet.new([state.node_id]),
            timer: nil,
            reply_value: reply_value,
            broadcast_msg: broadcast_msg,
            entry_tuple: new_entry_tuple,
            events: events
        }

        case arm_cas_timeout(ref, op.deadline_ms) do
          :expired ->
            state = %{state | pending_cas: Map.put(state.pending_cas, ref, op)}
            handle_cas_timeout_now(state, ref, op)

          timer ->
            op = %{op | timer: timer}

            # For cluster_size: 1 (no members), or if local accept already meets quorum.
            if MapSet.size(op.accepts) >= op.quorum do
              commit_cas(state, ref, op)
            else
              %{state | pending_cas: Map.put(state.pending_cas, ref, op)}
            end
        end
    end
  end

  # Commit CAS: promote already-accepted local ballot into kv + oplog.
  # Called only when actual accepts (including local) reach quorum.
  defp commit_cas(%Replica{} = state, ref, op) do
    %{db: db, stmts: stmts} = state
    {key, _value_binary, _timestamp, _origin_str, _expires_at, _deleted_at} = op.entry_tuple
    {ballot_c, ballot_n} = op.ballot

    case Store.paxos_promote(
           db,
           stmts.kv_force_upsert,
           stmts.keyref_upsert,
           stmts.oplog_insert,
           key,
           ballot_c,
           ballot_n
         ) do
      {:ok, _value_binary, _ts, origin, _expires, _deleted_at, _prev_value_binary, origin_seq,
       local_progress_seq} ->
        origin = normalize_origin_node(origin)

        state =
          state
          |> set_local_origin_seq(origin_seq)
          |> merge_local_progress_seq(origin, local_progress_seq)

        cancel_timer(op.timer)
        dispatch_events(state, op.events)
        reply_cas_commit(op, state, origin_seq)
        broadcast_cas_commit(state, op, origin_seq)
        %{state | pending_cas: Map.delete(state.pending_cas, ref)}

      {:ok, :stale} ->
        # Local accepted ballot was superseded before commit.
        handle_cas_failure(state, ref, op)
    end
  end

  defp reply_cas_commit(%{reply_mode: :observer_write} = op, %Replica{} = state, origin_seq) do
    reply_cas_reply(op.from, op.reply_mode, op.reply_value, commit_message(state, op, origin_seq))
  end

  defp reply_cas_commit(%{reply_mode: :observer_read} = op, %Replica{} = state, origin_seq) do
    reply_cas_reply(op.from, op.reply_mode, op.reply_value, commit_message(state, op, origin_seq))
  end

  defp reply_cas_commit(op, _state, _origin_seq) do
    reply_cas_reply(op.from, op.reply_mode, op.reply_value)
  end

  defp apply_operation(%Replica{} = state, operation, key, current_value, current_vsn) do
    case operation do
      {:cas_put, expected_vsn, value_binary, opts} ->
        if current_vsn == expected_vsn do
          now = monotonic_cas_ts(current_vsn)
          origin = local_origin_id(state)
          origin_str = origin
          ttl = Keyword.get(opts, :ttl)
          expires_at = if ttl, do: now + ttl * 1_000_000

          entry_tuple = {key, value_binary, now, origin_str, expires_at, nil}
          broadcast_msg = {:ekv_put, key, value_binary, now, origin, expires_at}
          events = [%EKV.Event{type: :put, key: key, value: :erlang.binary_to_term(value_binary)}]
          reply_value = {:ok, {now, origin}}
          {:ok, value_binary, entry_tuple, reply_value, broadcast_msg, events}
        else
          {:error, :conflict}
        end

      {:cas_delete, expected_vsn, _opts} ->
        if current_vsn == expected_vsn do
          now = monotonic_cas_ts(current_vsn)
          origin = local_origin_id(state)
          origin_str = origin

          entry_tuple = {key, nil, now, origin_str, nil, now}
          broadcast_msg = {:ekv_delete, key, now, origin}
          events = [%EKV.Event{type: :delete, key: key, value: current_value}]
          {:ok, nil, entry_tuple, {:ok, {now, origin}}, broadcast_msg, events}
        else
          {:error, :conflict}
        end

      {:update, fun, opts, _retries} ->
        new_value = apply_update_callback(fun, current_value)
        new_value_binary = :erlang.term_to_binary(new_value)
        now = monotonic_cas_ts(current_vsn)
        origin = local_origin_id(state)
        origin_str = origin
        ttl = Keyword.get(opts, :ttl)
        expires_at = if ttl, do: now + ttl * 1_000_000

        entry_tuple = {key, new_value_binary, now, origin_str, expires_at, nil}
        broadcast_msg = {:ekv_put, key, new_value_binary, now, origin, expires_at}
        events = [%EKV.Event{type: :put, key: key, value: new_value}]
        reply_value = {:ok, new_value, {now, origin}}
        {:ok, new_value_binary, entry_tuple, reply_value, broadcast_msg, events}

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

  defp apply_update_callback(fun, current_value) when is_function(fun, 1), do: fun.(current_value)

  defp apply_update_callback({mod, fun, extra_args}, current_value)
       when is_atom(mod) and is_atom(fun) and is_list(extra_args) do
    apply(mod, fun, [current_value | extra_args])
  end

  # Ensure local eventual writes never reuse a timestamp on this shard. LWW
  # compares {timestamp, origin_node}, so same-origin timestamp reuse would make
  # later writes ambiguous or no-op under conflict resolution.
  defp next_lww_ts(%Replica{} = state) do
    now = max(System.system_time(:nanosecond), state.lww_ts_counter + 1)
    {now, %{state | lww_ts_counter: now}}
  end

  defp set_local_origin_seq(%Replica{} = state, seq) when is_integer(seq) and seq >= 0 do
    local_progress = normalize_progress_summary(state.local_progress)
    local_origin = local_origin_id(state)

    %{
      state
      | local_origin_seq: seq,
        local_progress:
          Map.put(
            local_progress,
            local_origin,
            max(seq, Map.get(local_progress, local_origin, 0))
          )
    }
  end

  defp merge_local_progress_seq(%Replica{} = state, origin_node, seq)
       when is_integer(seq) and seq >= 0 do
    origin_node = normalize_origin_node(origin_node)
    local_progress = normalize_progress_summary(state.local_progress)
    next_seq = max(Map.get(local_progress, origin_node, 0), seq)
    local_progress = Map.put(local_progress, origin_node, next_seq)
    state = %{state | local_progress: local_progress}

    if origin_node == local_origin_id(state) do
      %{state | local_origin_seq: max(state.local_origin_seq, next_seq)}
    else
      state
    end
  end

  defp merge_local_progress_seq(%Replica{} = state, _origin_node, _seq), do: state

  defp replace_local_progress_summary(%Replica{} = state, progress_summary)
       when is_map(progress_summary) do
    progress_summary = normalize_progress_summary(progress_summary)

    local_progress =
      state.local_progress
      |> normalize_progress_summary()
      |> Map.merge(progress_summary, fn _origin, current, incoming -> max(current, incoming) end)

    local_origin = local_origin_id(state)
    local_origin_seq = max(state.local_origin_seq, Map.get(local_progress, local_origin, 0))

    %{
      state
      | local_progress: Map.put(local_progress, local_origin, local_origin_seq),
        local_origin_seq: local_origin_seq
    }
  end

  defp reconcile_authoritative_origin_head(%Replica{} = state, remote_node, remote_progress)
       when is_atom(remote_node) and is_map(remote_progress) do
    local_progress = normalize_progress_summary(state.local_progress)
    remote_origin = remote_origin_id(state, remote_node, remote_progress)
    remote_head = Map.get(remote_progress, remote_origin)
    local_head = Map.get(local_progress, remote_origin, 0)

    corroborated_higher_head? =
      Enum.any?(state.remote_member_progress, fn
        {^remote_node, _member_progress} ->
          false

        {_member_node, member_progress} ->
          Map.get(normalize_progress_summary(member_progress), remote_origin, 0) >= local_head
      end)

    if is_integer(remote_head) and remote_head >= 0 and local_head > remote_head and
         not corroborated_higher_head? do
      local_progress = Map.put(local_progress, remote_origin, remote_head)
      :ok = Store.replace_local_progress_summary(state.db, local_progress)
      %{state | local_progress: local_progress}
    else
      state
    end
  end

  defp reconcile_authoritative_origin_head(%Replica{} = state, _remote_node, _remote_progress),
    do: state

  defp track_applied_origin_progress(
         %Replica{} = state,
         origin_node,
         origin_seq,
         local_progress_seq
       ) do
    state =
      if origin_node == local_origin_id(state) and is_integer(origin_seq) and origin_seq >= 0 do
        set_local_origin_seq(state, max(state.local_origin_seq, origin_seq))
      else
        state
      end

    merge_local_progress_seq(state, origin_node, local_progress_seq)
  end

  defp origin_gap?(%Replica{} = state, origin_node, origin_seq)
       when is_binary(origin_node) and is_integer(origin_seq) and origin_seq >= 0 do
    origin_seq > Map.get(state.local_progress, origin_node, 0) + 1
  end

  defp origin_gap?(%Replica{} = _state, _origin_node, _origin_seq), do: false

  defp maybe_request_origin_gap_repair(%Replica{} = state, origin_node, origin_seq, true) do
    from_seq = Map.get(state.local_progress, origin_node, 0)

    if origin_seq > from_seq + 1 do
      case source_node_for_origin(state, origin_node) do
        nil -> state
        remote_node -> request_sync(state, remote_node, {:delta, origin_node, from_seq})
      end
    else
      state
    end
  end

  defp maybe_request_origin_gap_repair(%Replica{} = state, _origin_node, _origin_seq, _gap?),
    do: state

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
        value = if value_binary, do: :erlang.binary_to_term(value_binary)
        {value, {timestamp, normalize_origin_node(origin_node_str)}}
    end
  end

  defp maybe_repair_conflict_visibility(
         %Replica{} = state,
         ref,
         op,
         selected_kv_row,
         current_value,
         current_vsn
       ) do
    if stale_local_kv_view?(state, op.key, selected_kv_row) do
      {:ok, _value_binary, entry_tuple, _reply_value, broadcast_msg, _events} =
        apply_cas_read_recovery(state, op.key, selected_kv_row, current_value, current_vsn)

      repair_op =
        op
        |> Map.put(:failure_reason_override, :conflict)
        |> Map.put(:reply_value, {:error, :conflict})

      enter_accept_phase_with_entry(
        state,
        ref,
        repair_op,
        entry_tuple,
        {:error, :conflict},
        broadcast_msg,
        []
      )
    else
      handle_cas_failure(state, ref, op)
    end
  end

  defp stale_local_kv_view?(%Replica{} = state, key, selected_kv_row) do
    local_kv_row = Store.get(state.db, key)
    normalize_kv_row(local_kv_row) != normalize_kv_row(selected_kv_row)
  end

  defp normalize_kv_row(nil), do: nil

  defp normalize_kv_row({value_binary, ts, origin, expires_at, deleted_at}) do
    [value_binary, ts, normalize_origin_node(origin), expires_at, deleted_at]
  end

  defp normalize_kv_row([value_binary, ts, origin, expires_at, deleted_at]) do
    [value_binary, ts, normalize_origin_node(origin), expires_at, deleted_at]
  end

  # Build entry_tuple for cas_read recovery directly from the raw accepted
  # kv_row columns, preserving expires_at and deleted_at exactly as accepted.
  defp apply_cas_read_recovery(%Replica{} = state, key, nil, _current_value, _current_vsn) do
    # Absent key: barrier read proposes a tombstone marker at a fresh timestamp.
    # This closes outstanding accept-state ambiguity for this key.
    now = System.system_time(:nanosecond)
    origin = local_origin_id(state)
    origin_str = origin
    entry_tuple = {key, nil, now, origin_str, nil, now}
    broadcast_msg = {:ekv_delete, key, now, origin}
    {:ok, nil, entry_tuple, {:ok, nil, nil}, broadcast_msg, []}
  end

  defp apply_cas_read_recovery(
         %Replica{} = _state,
         key,
         [value_binary, ts, origin_str, expires_at, deleted_at],
         current_value,
         current_vsn
       ) do
    origin = normalize_origin_node(origin_str)

    if is_integer(deleted_at) do
      # Tombstone — re-propose as delete with original metadata
      entry_tuple = {key, nil, ts, origin, nil, deleted_at}
      broadcast_msg = {:ekv_delete, key, ts, origin}
      {:ok, nil, entry_tuple, {:ok, nil, nil}, broadcast_msg, []}
    else
      # Live value — re-propose with original expires_at
      {final_ts, final_origin} =
        case current_vsn do
          {vsn_ts, vsn_origin} ->
            {vsn_ts, normalize_origin_node(vsn_origin)}

          _ ->
            {ts, origin}
        end

      entry_tuple = {key, value_binary, final_ts, final_origin, expires_at, nil}

      broadcast_msg = {:ekv_put, key, value_binary, final_ts, final_origin, expires_at}

      {:ok, value_binary, entry_tuple, {:ok, current_value, {final_ts, final_origin}},
       broadcast_msg, []}
    end
  end

  defp cas_read_returns_committed_absent?(
         %Replica{} = state,
         {:cas_read, _, _},
         key,
         best_acc_c,
         best_acc_n,
         selected_kv_row
       ) do
    clean_absent_cas_read?(best_acc_c, best_acc_n, selected_kv_row) or
      committed_absent_matches_selected?(state, key, selected_kv_row)
  end

  defp cas_read_returns_committed_absent?(
         %Replica{} = _state,
         _operation,
         _key,
         _best_acc_c,
         _best_acc_n,
         _selected_kv_row
       ),
       do: false

  defp clean_absent_cas_read?(best_acc_c, best_acc_n, selected_kv_row) do
    best_acc_c == 0 and best_acc_n in ["", nil] and is_nil(selected_kv_row)
  end

  defp committed_absent_matches_selected?(%Replica{} = state, key, selected_kv_row)
       when is_list(selected_kv_row) do
    logically_absent_kv_row?(selected_kv_row) and
      normalize_kv_row(Store.get(state.db, key)) == normalize_kv_row(selected_kv_row)
  end

  defp committed_absent_matches_selected?(%Replica{} = _state, _key, _selected_kv_row),
    do: false

  defp logically_absent_kv_row?([_value_binary, _ts, _origin, expires_at, deleted_at]) do
    now = System.system_time(:nanosecond)
    is_integer(deleted_at) or (is_integer(expires_at) and expires_at <= now)
  end

  defp handle_cas_failure(%Replica{} = state, ref, op) do
    cancel_timer(op.timer)

    case op.operation do
      {:update, fun, opts, retries} when retries > 0 ->
        case cas_failure_reason(op) do
          :conflict ->
            # Retry only for definite conflicts (no accept-phase ambiguity).
            # If accept may have happened, do not auto-retry the same logical op.
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

          :unconfirmed ->
            reply_unconfirmed_or_resolve(op, state)
            %{state | pending_cas: Map.delete(state.pending_cas, ref)}
        end

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
        reason = cas_failure_reason(op)

        if reason == :unconfirmed do
          reply_unconfirmed_or_resolve(op, state)
        else
          reply_cas_error(op, {:error, reason})
        end

        %{state | pending_cas: Map.delete(state.pending_cas, ref)}
    end
  end

  defp cas_failure_reason(op) do
    case op do
      %{failure_reason_override: override} when not is_nil(override) ->
        override

      %{phase: :accept, operation: operation} ->
        if writes_operation?(operation), do: :unconfirmed, else: :conflict

      _ ->
        :conflict
    end
  end

  defp writes_operation?({:cas_put, _, _, _}), do: true
  defp writes_operation?({:cas_delete, _, _}), do: true
  defp writes_operation?({:update, _, _, _}), do: true
  defp writes_operation?(_), do: false

  defp reply_cas_error(%{reply_mode: mode, from: from}, reply)
       when mode in [:observer_write, :observer_read] do
    reply_cas_reply(from, mode, reply)
  end

  defp reply_cas_error(%{reply_mode: mode, from: from}, reply),
    do: reply_cas_reply(from, mode, reply)

  defp reply_cas_error(%{from: from}, reply), do: reply_cas_reply(from, :normal, reply)

  defp reply_unconfirmed_or_resolve(op, %Replica{} = state) do
    case op.reply_mode do
      :observer_write ->
        reply =
          case op.reply_value do
            nil -> {:error, :unconfirmed}
            reply_value -> {:error, :unconfirmed, reply_value}
          end

        reply_cas_reply(op.from, :observer_write, reply)

      :observer_read ->
        reply_cas_reply(op.from, :observer_read, {:error, :unconfirmed})

      _ ->
        reply_local_request(op.from, {:error, :unconfirmed, op.reply_value})
    end

    state
  end

  defp cas_deadline_from_opts(opts) when is_list(opts) do
    case Keyword.get(opts, :timeout, 10_000) do
      :infinity ->
        :infinity

      timeout when is_integer(timeout) and timeout > 0 ->
        System.monotonic_time(:millisecond) + timeout
    end
  end

  defp cas_deadline_expired?(:infinity), do: false

  defp cas_deadline_expired?(deadline_ms) when is_integer(deadline_ms) do
    deadline_ms <= System.monotonic_time(:millisecond)
  end

  defp arm_cas_timeout(_ref, :infinity), do: nil

  defp arm_cas_timeout(ref, deadline_ms) when is_integer(deadline_ms) do
    remaining_ms = deadline_ms - System.monotonic_time(:millisecond)

    if remaining_ms > 0 do
      Process.send_after(self(), {:cas_timeout, ref}, remaining_ms)
    else
      :expired
    end
  end

  defp handle_cas_timeout_now(%Replica{} = state, ref, op) do
    cancel_timer(op.timer)
    reply_cas_timeout(%{state | pending_cas: Map.delete(state.pending_cas, ref)}, op)
  end

  defp reply_cas_timeout(%Replica{} = state, op) do
    if op.phase == :accept and writes_operation?(op.operation) do
      reply_unconfirmed_or_resolve(op, state)
    else
      reply_cas_error(op, {:error, :quorum_timeout})
      state
    end
  end

  defp apply_cas_commit(%Replica{} = state, key, ballot_c, ballot_n, entry_tuple, origin_seq) do
    %{db: db, stmts: stmts} = state

    case Store.paxos_promote(
           db,
           stmts.kv_force_upsert,
           stmts.keyref_upsert,
           stmts.oplog_insert,
           key,
           ballot_c,
           ballot_n,
           origin_seq
         ) do
      {:ok, value_binary, _ts, origin, _expires, deleted_at, prev_value_binary, promoted_seq,
       local_progress_seq} ->
        origin = normalize_origin_node(origin)
        state = track_applied_origin_progress(state, origin, promoted_seq, local_progress_seq)
        dispatch_promote_event(state, key, value_binary, deleted_at, prev_value_binary)
        {state, true}

      {:ok, :stale} ->
        # Node may have missed original accept; try to stage accepted state
        # from commit payload, then promote.
        if is_tuple(entry_tuple) and tuple_size(entry_tuple) == 6 do
          {_key, value_binary, ts, origin_str, expires_at, deleted_at} = entry_tuple
          value_args = [value_binary, ts, origin_str, expires_at, deleted_at]

          case Store.paxos_accept(db, key, ballot_c, ballot_n, value_args) do
            {:ok, true} ->
              case Store.paxos_promote(
                     db,
                     stmts.kv_force_upsert,
                     stmts.keyref_upsert,
                     stmts.oplog_insert,
                     key,
                     ballot_c,
                     ballot_n,
                     origin_seq
                   ) do
                {:ok, promoted_value, _ts, origin, _expires, promoted_deleted, prev_value_binary,
                 promoted_seq, local_progress_seq} ->
                  origin = normalize_origin_node(origin)

                  state =
                    track_applied_origin_progress(
                      state,
                      origin,
                      promoted_seq,
                      local_progress_seq
                    )

                  dispatch_promote_event(
                    state,
                    key,
                    promoted_value,
                    promoted_deleted,
                    prev_value_binary
                  )

                  {state, true}

                {:ok, :stale} ->
                  {state, false}
              end

            {:ok, false} ->
              {state, false}
          end
        else
          {state, false}
        end
    end
  end

  defp dispatch_promote_event(
         %Replica{} = state,
         key,
         value_binary,
         deleted_at,
         prev_value_binary
       ) do
    if deleted_at != nil do
      prev = if prev_value_binary != nil, do: :erlang.binary_to_term(prev_value_binary)
      dispatch_events(state, [%EKV.Event{type: :delete, key: key, value: prev}])
    else
      dispatch_events(state, [
        %EKV.Event{type: :put, key: key, value: :erlang.binary_to_term(value_binary)}
      ])
    end
  end

  defp commit_payload_for_member(%Replica{} = state, target_node, op) do
    case Map.get(state.member_node_ids, target_node) do
      nil ->
        op.entry_tuple

      remote_node_id ->
        if slim_commit_safe?(state, target_node, remote_node_id, op.accepts) do
          nil
        else
          op.entry_tuple
        end
    end
  end

  defp slim_commit_safe?(%Replica{} = state, _target_node, remote_node_id, _accepts)
       when remote_node_id == state.node_id,
       do: false

  defp slim_commit_safe?(%Replica{} = state, target_node, remote_node_id, accepts) do
    MapSet.member?(accepts, remote_node_id) and
      unique_live_remote_node_id?(state, target_node, remote_node_id)
  end

  defp unique_live_remote_node_id?(%Replica{} = state, target_node, remote_node_id) do
    not Enum.any?(state.member_node_ids, fn {remote_node, other_node_id} ->
      remote_node != target_node and
        other_node_id == remote_node_id and
        Map.has_key?(state.remote_shards, remote_node)
    end)
  end

  @doc false
  def remote_cas_voter?(%Replica{} = state, remote_node) when is_atom(remote_node) do
    Map.has_key?(state.remote_shards, remote_node) and
      not remote_supports_feature?(state, remote_node, @wire_feature_observer)
  end

  defp local_cas_voter?(%Replica{} = state), do: state.cas_voter?

  def alive_node_id_count(%Replica{} = state) do
    if is_integer(state.cluster_size) and local_cas_voter?(state) do
      # Our own node_id + distinct voter member node_ids.
      member_ids =
        state.member_node_ids
        |> Enum.filter(fn {remote_node, remote_node_id} ->
          is_binary(remote_node_id) and remote_cas_voter?(state, remote_node)
        end)
        |> Enum.map(fn {_remote_node, remote_node_id} -> remote_node_id end)
        |> MapSet.new()

      MapSet.size(MapSet.put(member_ids, state.node_id))
    else
      1
    end
  end

  def quorum_status(%Replica{cluster_size: nil}), do: {:error, :cas_not_configured}

  def quorum_status(%Replica{} = state) do
    quorum = div(state.cluster_size, 2) + 1
    alive_count = alive_node_id_count(state)

    cond do
      alive_count > state.cluster_size -> {:error, :cluster_overflow}
      alive_count < quorum -> {:error, :no_quorum}
      true -> :ok
    end
  end

  defp maybe_reply_to_quorum_waiters(%Replica{quorum_waiters: waiters} = state)
       when map_size(waiters) == 0,
       do: state

  defp maybe_reply_to_quorum_waiters(%Replica{} = state) do
    case quorum_status(state) do
      :ok ->
        reply_and_clear_quorum_waiters(state, :ok)

      {:error, :cluster_overflow} = error ->
        reply_and_clear_quorum_waiters(state, error)

      _ ->
        state
    end
  end

  defp fail_quorum_waiters(%Replica{quorum_waiters: waiters} = state, _reason)
       when map_size(waiters) == 0,
       do: state

  defp fail_quorum_waiters(%Replica{} = state, reason) do
    reply_and_clear_quorum_waiters(state, reason)
  end

  defp reply_and_clear_quorum_waiters(%Replica{} = state, reply) do
    Enum.each(state.quorum_waiters, fn {_ref, %{from: from, timer: timer}} ->
      cancel_timer(timer)
      reply_local_request(from, reply)
    end)

    %{state | quorum_waiters: %{}}
  end

  defp fail_pending_cas_if_no_quorum(%Replica{} = state) do
    if state.cluster_size == nil or map_size(state.pending_cas) == 0 do
      state
    else
      alive_count = alive_node_id_count(state)

      {to_fail, to_keep} =
        Enum.split_with(state.pending_cas, fn {_ref, op} ->
          alive_count < op.quorum
        end)

      for {_ref, op} <- to_fail do
        cancel_timer(op.timer)
        reply_cas_error(op, {:error, :no_quorum})
      end

      %{state | pending_cas: Map.new(to_keep)}
    end
  end

  defp track_member_node_id(%Replica{} = state, _remote_node, nil), do: state

  defp track_member_node_id(%Replica{} = state, remote_node, remote_node_id) do
    %{state | member_node_ids: Map.put(state.member_node_ids, remote_node, remote_node_id)}
  end

  defp remember_member_origin_seen(%Replica{} = state, nil), do: state

  defp remember_member_origin_seen(%Replica{} = state, remote_node_id)
       when is_binary(remote_node_id) and byte_size(remote_node_id) > 0 do
    now_ms = System.system_time(:millisecond)

    case Map.get(state.member_seen_at, remote_node_id) do
      nil ->
        Store.member_seen_marker_put(state.db, remote_node_id, now_ms)
        %{state | member_seen_at: Map.put(state.member_seen_at, remote_node_id, now_ms)}

      seen_at_ms
      when is_integer(seen_at_ms) and now_ms - seen_at_ms >= @member_seen_refresh_window_ms ->
        Store.member_seen_marker_put(state.db, remote_node_id, now_ms)
        %{state | member_seen_at: Map.put(state.member_seen_at, remote_node_id, now_ms)}

      _recent ->
        state
    end
  end

  defp persist_member_node_identity(%Replica{} = state, _remote_node, nil), do: state

  defp persist_member_node_identity(%Replica{} = state, remote_node, remote_node_id) do
    Store.member_node_identity_put(state.db, remote_node, remote_node_id)
    state
  end

  defp reply_cas_reply(from, mode, reply, commit_payload \\ nil)

  defp reply_cas_reply(from, mode, reply, commit_payload)
       when mode in [:observer_write, :observer_read] do
    reply_local_request(from, {:observer_result, reply, commit_payload})
  end

  defp reply_cas_reply(from, _mode, reply, _commit_payload) do
    reply_local_request(from, reply)
  end

  defp mark_member_down(%Replica{} = state, remote_node, nil) do
    remember_member_down_marker(state, member_down_name_key(remote_node))
  end

  defp mark_member_down(%Replica{} = state, remote_node, remote_node_id) do
    if node_id_connected?(state, remote_node_id) do
      # Blue/green overlap: same cluster member identity is still connected.
      clear_member_down_marker(state, member_down_name_key(remote_node))
    else
      state
      |> remember_member_down_marker(member_down_name_key(remote_node))
      |> remember_member_down_marker(member_down_id_key(remote_node_id))
    end
  end

  defp maybe_allow_member_reconnect(state, remote_node, remote_node_id \\ nil)

  defp maybe_allow_member_reconnect(
         %Replica{partition_ttl_policy: :ignore} = state,
         remote_node,
         remote_node_id
       ) do
    reconnecting? =
      MapSet.member?(state.quarantined_members, remote_node) or
        known_down_member?(state, remote_node) or
        (is_binary(remote_node_id) and known_down_member?(state, remote_node_id))

    %Replica{} = state = clear_member_down_markers(state, remote_node, remote_node_id)

    state = %{
      state
      | quarantined_members: MapSet.delete(state.quarantined_members, remote_node)
    }

    state =
      if reconnecting? do
        state
        |> clear_summary_probe_inflight(remote_node)
        |> clear_sync_inflight(remote_node)
      else
        clear_summary_probe_inflight(state, remote_node)
      end

    {:ok, state}
  end

  defp maybe_allow_member_reconnect(%Replica{} = state, remote_node, remote_node_id) do
    {%Replica{} = state, down_marker} =
      resolve_member_down_marker(state, remote_node, remote_node_id)

    down_since_ms = down_marker.down_since_ms

    age_ms =
      if is_integer(down_since_ms), do: max(0, System.system_time(:millisecond) - down_since_ms)

    cond do
      is_nil(down_since_ms) ->
        state = %{
          state
          | quarantined_members: MapSet.delete(state.quarantined_members, remote_node)
        }

        {:ok, clear_summary_probe_inflight(state, remote_node)}

      is_integer(down_since_ms) and
          allow_live_member_id_marker_reconnect?(state, remote_node, remote_node_id, down_marker) ->
        %Replica{} = state = clear_member_down_markers(state, remote_node, remote_node_id)

        state = %{
          state
          | quarantined_members: MapSet.delete(state.quarantined_members, remote_node)
        }

        state =
          state
          |> clear_summary_probe_inflight(remote_node)
          |> clear_sync_inflight(remote_node)

        {:ok, state}

      is_integer(down_since_ms) and age_ms > state.tombstone_ttl ->
        state = %{
          state
          | quarantined_members: MapSet.put(state.quarantined_members, remote_node),
            remote_shards: Map.delete(state.remote_shards, remote_node),
            member_node_ids: Map.delete(state.member_node_ids, remote_node),
            summary_probe_inflight: Map.delete(state.summary_probe_inflight, remote_node)
        }

        state =
          state
          |> clear_sync_inflight(remote_node)
          |> drop_replication_batch(remote_node)

        log_once(state, fn ->
          "#{log_prefix(state)} quarantining #{remote_node}: reconnect downtime exceeded " <>
            "tombstone_ttl (#{state.tombstone_ttl}ms). " <>
            "Replication is blocked until operator rebuilds one side."
        end)

        {:quarantine, state}

      true ->
        %Replica{} = state = clear_member_down_markers(state, remote_node, remote_node_id)

        state = %{
          state
          | quarantined_members: MapSet.delete(state.quarantined_members, remote_node)
        }

        state =
          state
          |> clear_summary_probe_inflight(remote_node)
          |> clear_sync_inflight(remote_node)

        {:ok, state}
    end
  end

  defp resolve_member_down_marker(%Replica{} = state, remote_node, nil) do
    {%Replica{} = state, name_down_since} =
      read_member_down_marker(state, member_down_name_key(remote_node))

    {state,
     %{down_since_ms: name_down_since, id_down_since: nil, name_down_since: name_down_since}}
  end

  defp resolve_member_down_marker(%Replica{} = state, remote_node, remote_node_id) do
    id_key = member_down_id_key(remote_node_id)
    name_key = member_down_name_key(remote_node)

    {%Replica{} = state, id_down_since} = read_member_down_marker(state, id_key)
    {%Replica{} = state, name_down_since} = read_member_down_marker(state, name_key)

    cond do
      is_integer(name_down_since) ->
        merged_down_since =
          if is_integer(id_down_since),
            do: min(id_down_since, name_down_since),
            else: name_down_since

        state =
          if id_down_since == merged_down_since,
            do: state,
            else: put_member_down_marker(state, id_key, merged_down_since)

        %Replica{} = state = clear_member_down_marker(state, name_key)

        {state,
         %{
           down_since_ms: merged_down_since,
           id_down_since: id_down_since,
           name_down_since: name_down_since
         }}

      true ->
        {state,
         %{
           down_since_ms: id_down_since,
           id_down_since: id_down_since,
           name_down_since: name_down_since
         }}
    end
  end

  defp allow_live_member_id_marker_reconnect?(
         %Replica{} = state,
         remote_node,
         remote_node_id,
         %{id_down_since: id_down_since, name_down_since: nil}
       )
       when is_atom(remote_node) and is_binary(remote_node_id) and is_integer(id_down_since) do
    remote_node in known_member_nodes(state) and
      EKV.MemberPresence.member_origin_known?(state.name, remote_node_id)
  rescue
    _ -> false
  end

  defp allow_live_member_id_marker_reconnect?(
         %Replica{} = _state,
         _remote_node,
         _remote_node_id,
         _down_marker
       ),
       do: false

  defp remember_member_down_marker(%Replica{} = state, marker_key) do
    {%Replica{} = state, existing_down_since} = read_member_down_marker(state, marker_key)

    if is_integer(existing_down_since) do
      state
    else
      put_member_down_marker(state, marker_key, System.system_time(:millisecond))
    end
  end

  defp read_member_down_marker(%Replica{} = state, marker_key) do
    case Map.fetch(state.member_down_at, marker_key) do
      {:ok, down_since} ->
        {state, down_since}

      :error ->
        down_since = Store.member_down_marker_get(state.db, marker_key)

        state =
          if is_integer(down_since),
            do: %{state | member_down_at: Map.put(state.member_down_at, marker_key, down_since)},
            else: state

        {state, down_since}
    end
  end

  defp put_member_down_marker(%Replica{} = state, marker_key, down_since) do
    Store.member_down_marker_put(state.db, marker_key, down_since)
    %{state | member_down_at: Map.put(state.member_down_at, marker_key, down_since)}
  end

  defp clear_member_down_marker(%Replica{} = state, marker_key) do
    Store.member_down_marker_clear(state.db, marker_key)
    %{state | member_down_at: Map.delete(state.member_down_at, marker_key)}
  end

  defp clear_member_down_markers(%Replica{} = state, remote_node, remote_node_id) do
    remote_node
    |> member_down_marker_keys(remote_node_id)
    |> Enum.uniq()
    |> Enum.reduce(state, fn marker_key, acc ->
      clear_member_down_marker(acc, marker_key)
    end)
  end

  defp member_down_marker_keys(remote_node, nil), do: [member_down_name_key(remote_node)]

  defp member_down_marker_keys(remote_node, remote_node_id) do
    [member_down_id_key(remote_node_id), member_down_name_key(remote_node)]
  end

  defp retained_member_down_marker_keys(member_node_key, nil) do
    [member_down_id_key(member_node_key), member_down_name_key(member_node_key)]
  end

  defp retained_member_down_marker_keys(member_node_key, member_node_id) do
    [member_down_id_key(member_node_id), member_down_name_key(member_node_key)]
  end

  defp known_down_member_marker_keys(remote_node_or_id, %Replica{} = _state)
       when is_binary(remote_node_or_id) do
    [member_down_id_key(remote_node_or_id), member_down_name_key(remote_node_or_id)]
  end

  defp known_down_member_marker_keys(remote_node_or_id, %Replica{} = _state)
       when is_atom(remote_node_or_id) do
    [member_down_name_key(remote_node_or_id)]
  end

  defp member_down_id_key(remote_node_id), do: @member_down_id_prefix <> to_string(remote_node_id)

  defp member_down_name_key(remote_node) when is_atom(remote_node),
    do: @member_down_name_prefix <> Atom.to_string(remote_node)

  defp member_down_name_key(remote_node) when is_binary(remote_node),
    do: @member_down_name_prefix <> remote_node

  defp node_id_connected?(%Replica{} = state, remote_node_id) do
    Enum.any?(state.member_node_ids, fn {member_node, member_node_id} ->
      member_node_id == remote_node_id and Map.has_key?(state.remote_shards, member_node)
    end)
  end

  defp prune_stale_member_down_name_markers(%Replica{} = state) do
    retention_ms = max(@member_down_name_min_retention_ms, state.tombstone_ttl * 4)
    stale_before_ms = System.system_time(:millisecond) - retention_ms

    Store.prune_member_down_name_markers(
      state.db,
      stale_before_ms,
      @member_down_name_max_entries
    )
  end

  defp prune_stale_member_seen_markers(%Replica{} = state) do
    stale_before_ms = System.system_time(:millisecond) - @member_seen_hint_ttl_ms

    Store.prune_member_seen_markers(
      state.db,
      stale_before_ms,
      @member_seen_max_entries
    )
  end

  defp cancel_timer(nil), do: :ok
  defp cancel_timer(ref), do: Process.cancel_timer(ref)

  # =====================================================================
  # Subscriber dispatch helpers
  # =====================================================================

  defp has_subscribers?(%Replica{} = state) do
    config = EKV.Supervisor.get_config(state.name)
    :atomics.get(config.sub_count, 1) > 0 or EKV.Supervisor.client_subscribers?(state.name)
  end

  defp dispatch_events(%Replica{} = _state, []), do: :ok

  defp dispatch_events(%Replica{} = state, events) do
    send(EKV.SubDispatcher.dispatcher_name(state.name, state.shard_index), {:dispatch, events})
    :ok
  end

  defp read_conn(%Replica{} = state) do
    readers = :persistent_term.get({EKV, state.name, :readers, state.shard_index})
    sid = :erlang.system_info(:scheduler_id)
    elem(readers, rem(sid - 1, tuple_size(readers)))
  end

  defp read_previous_value(%Replica{} = state, key) do
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

  defp log(%Replica{} = state, message_fn) when is_function(message_fn, 0) do
    case EKV.Supervisor.get_config(state.name) do
      %{log: false} -> :ok
      _ -> Logger.info(message_fn)
    end
  end

  defp log_warn(%Replica{} = state, message_fn) when is_function(message_fn, 0) do
    case EKV.Supervisor.get_config(state.name) do
      %{log: false} -> :ok
      _ -> Logger.warning(message_fn)
    end
  end

  defp log_verbose(%Replica{} = state, message_fn) when is_function(message_fn, 0) do
    case EKV.Supervisor.get_config(state.name) do
      %{log: :verbose} -> Logger.info(message_fn)
      _ -> :ok
    end
  end

  defp log_once(%Replica{} = state, message_fn) do
    if state.shard_index == 0, do: log(state, message_fn)
  end

  defp maybe_log_oplog_truncate(%Replica{} = state, truncate_stats, truncate_us) do
    duration_ms = System.convert_time_unit(truncate_us, :microsecond, :millisecond)
    deleted_rows = Map.get(truncate_stats, :deleted_rows, 0)
    retained_floors = Map.get(truncate_stats, :retained_floors, [])
    retention_lag = Map.get(truncate_stats, :retention_lag, [])

    if retention_lag != [] do
      log_warn(state, fn ->
        "#{log_prefix_shard(state)} oplog truncate retention lag " <>
          "duration_ms=#{duration_ms} deleted_rows=#{deleted_rows} " <>
          "retained_floors=#{inspect(retained_floors)} lag=#{inspect(retention_lag)}"
      end)
    end

    if deleted_rows >= 10_000 or duration_ms >= 1_000 do
      log(state, fn ->
        "#{log_prefix_shard(state)} oplog truncate " <>
          "duration_ms=#{duration_ms} deleted_rows=#{deleted_rows} " <>
          "retained_floors=#{inspect(retained_floors)}"
      end)
    end
  end

  defp log_prefix(%Replica{} = state) do
    "[EKV #{inspect(state.name)}]"
  end

  defp log_prefix_shard(%Replica{} = state) do
    "[EKV #{inspect(state.name)}/#{state.shard_index}]"
  end
end
