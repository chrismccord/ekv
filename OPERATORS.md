# EKV Operator Runbook

Scenario-based guide for operating EKV in production.

## Checking Cluster Health

```elixir
EKV.info(:my_kv)
# => %{
#   name: :my_kv,
#   mode: :member,
#   region: "iad",
#   node_id: "a1b2c3d4e5f6",
#   cluster_size: 3,
#   shards: 8,
#   data_dir: "/var/data/ekv",
#   connected_members: [
#     %{node: :app@host2, node_id: "f7e8d9c0b1a2"},
#     %{node: :app@host3, node_id: "1122334455aa"}
#   ]
# }
```

Verify:
- In steady state, `connected_members` length equals `cluster_size - 1`
- Each member has a non-nil `node_id`
- In steady state, member `node_id` values are distinct

During blue-green overlap, two Erlang nodes may temporarily share the same
logical `node_id`. In that case, dedupe by `node_id` and reason about logical
members rather than raw node count.

If a partition or maintenance event is active, `connected_members` may be lower
temporarily. Treat this as healthy only if it matches an expected outage.

## Startup Readiness

### wait_for_quorum

Use `wait_for_quorum: timeout_ms` when callers must be able to perform CAS
operations immediately after EKV starts.

Member mode:

```elixir
{EKV, name: :my_kv, data_dir: "/var/data/ekv",
 cluster_size: 3, wait_for_quorum: 30_000}
```

Client mode:

```elixir
{EKV, name: :my_kv, mode: :client,
 region_routing: ["iad", "dfw"], wait_for_quorum: 30_000}
```

- Member mode blocks startup until that EKV member can reach CAS quorum.
- Client mode blocks startup until the selected backend member reports quorum.
- This is only a startup gate. Quorum can still be lost later during runtime.
- In client mode, `wait_for_quorum` implicitly waits for route selection first.

Use it when downstream supervisors perform CAS writes during their own init.
Leave it disabled if the workload is LWW-only and startup should not wait for
quorum.

### wait_for_route (Client Mode)

Use `wait_for_route: timeout_ms` on clients when startup should wait for a
usable backend route before the app continues.

- The client picks the first reachable member in `region_routing` order.
- The chosen backend stays sticky until failure.
- `wait_for_route` is about routing readiness, not quorum.

### anti_entropy_interval

Members run periodic anti-entropy by default:

```elixir
{EKV, name: :my_kv, data_dir: "/var/data/ekv", anti_entropy_interval: 30_000}
```

- This re-runs the normal member handshake + delta/full sync path for already-connected members.
- It is meant to heal a member that missed a prior replication message without waiting for reconnect.
- In the steady state it should be cheap because members are already caught up and delta sync is usually empty.
- Set `false` only for debugging; the default is the safer production setting.

## Backups

### Taking a Backup

Safe to run while EKV is serving traffic (uses SQLite online backup API):

```elixir
:ok = EKV.backup(:my_kv, "/backups/ekv/2026-03-01")
```

The destination directory will contain one file per shard (`shard_0.db`,
`shard_1.db`, etc.). These are self-contained SQLite databases.

### Verifying a Backup

Start a temporary EKV instance pointing at the backup directory:

```elixir
{:ok, pid} = EKV.start_link(
  name: :backup_check,
  data_dir: "/backups/ekv/2026-03-01",
  shards: 8,                      # must match original
  allow_stale_startup: true,      # backup is old by definition
  gc_interval: :timer.hours(999)  # don't GC during verification
)

# Spot-check keys
EKV.get(:backup_check, "important/key")
EKV.keys(:backup_check, "users/") |> Enum.count()

# Clean up
Supervisor.stop(:backup_check_ekv_sup)
```

## Disaster Recovery

### Restoring a Recent Backup (< tombstone_ttl old)

If the backup is younger than `tombstone_ttl` (default 7 days):

1. Stop the node
2. Replace `data_dir` contents with backup files
3. Restart normally — delta sync from members catches up

### Restoring an Old Backup to the Entire Cluster

If the backup is older than `tombstone_ttl`, stale DB detection will fail
startup by default. Restore all nodes simultaneously with explicit stale
startup allowed:

1. Stop all nodes
2. Copy backup files to each node's `data_dir`
3. Restart all nodes with `allow_stale_startup: true`:
   ```elixir
   {EKV, name: :my_kv, data_dir: "/var/data/ekv", allow_stale_startup: true}
   ```
4. Verify data is present on all nodes
5. Remove `allow_stale_startup: true` and do a rolling restart

**Warning:** Entries deleted between the backup timestamp and now will
reappear ("zombie resurrection"). This is expected when restoring old backups.

## Node Identity (CAS Clusters)

### First Deploy

No special configuration needed. Each node auto-generates a unique `node_id`
on first boot and persists it to the data volume:

```elixir
# Minimal CAS config:
{EKV, name: :my_kv, data_dir: "/var/data/ekv", cluster_size: 3}
```

Check the assigned identity:

```elixir
EKV.info(:my_kv).node_id
# => "a1b2c3d4e5f67890"
```

### Passing an Explicit node_id

You can pass a string identifier (e.g., `FLY_MACHINE_ID`) on first boot:

```elixir
{EKV, name: :my_kv, data_dir: "/var/data/ekv",
 cluster_size: 3, node_id: System.get_env("FLY_MACHINE_ID")}
```

Once persisted to the volume, the configured value is ignored on subsequent
boots. If the configured value differs from the persisted one, EKV logs a
warning and uses the persisted identity.

### Machine Replacement (Same Volume)

Transparent. The new machine reads the persisted `node_id` from the volume
and resumes the old identity. No configuration change needed.

### Machine Replacement (New Volume)

The new machine auto-generates a fresh `node_id`. This counts as a new
cluster member. Ensure the old machine is fully disconnected first — if both
are reachable simultaneously, you may trigger a cluster overflow error
(see below).

## Scaling

### Accidental Scale-Up

If more distinct logical members are reachable than `cluster_size` allows, CAS
operations (`if_vsn`, `consistent: true`, `EKV.update/3`) return
`{:error, :cluster_overflow}`. Regular LWW operations (`put`, `get`, `delete`
without CAS options) continue working.

**Symptom:**
```elixir
EKV.put(:my_kv, "key", "val", if_vsn: nil)
# => {:error, :cluster_overflow}
```

**Diagnosis:**
```elixir
info = EKV.info(:my_kv)

logical_members =
  [info.node_id | Enum.map(info.connected_members, & &1.node_id)]
  |> Enum.uniq()
  |> length()

IO.puts("#{logical_members} logical members, cluster_size=#{info.cluster_size}")
```

**Fix:** Remove the extra machine(s). CAS resumes immediately.

### Intentional Scale-Up

1. Update `cluster_size` in config on **all existing nodes**
2. Rolling restart the existing nodes
3. Start the new node(s) with the same `cluster_size`

The new node auto-generates a `node_id` and joins the cluster. Full sync
from members populates its data.

### Intentional Scale-Down

1. Stop the node being removed
2. Wait for one GC cycle (`gc_interval`, default 5 min) — the removed node's
   HWM is pruned, allowing oplog truncation to proceed
3. Optionally update `cluster_size` on remaining nodes and rolling restart

## Client Mode

Client mode is for app nodes that need the EKV API but should **not** become
replicas or CAS voters.

```elixir
{EKV, name: :my_kv, mode: :client,
 region: "ord", region_routing: ["iad", "dfw", "lhr"]}
```

- Clients do not store data locally.
- Clients do not increase replication fan-out or quorum size.
- Reads and writes are forwarded to a selected member.
- Eventual reads in client mode are remote reads, not local SQLite reads.

Use member mode for the durable replica set. Use client mode for horizontally
scaled app nodes that should route into that replica set.

Startup guidance:

- LWW-only clients usually want `wait_for_route`
- CAS-heavy clients usually want `wait_for_route` and/or `wait_for_quorum`

If a client fails over to a different backend member, eventual reads may lose
session-style read-your-writes. Use `consistent: true` when fresh CAS-backed
reads are required.

## Network Partitions

### During a Partition

Both sides continue serving reads and writes. LWW resolves conflicts on heal.
CAS operations require a quorum (`floor(cluster_size / 2) + 1`) — the
minority side returns `{:error, :no_quorum}` for CAS but LWW still works.

### After Partition Heals

Nodes automatically reconnect and sync:

- **Short partition** (downtime <= `tombstone_ttl`): automatic sync.
  - Oplog intact: Delta sync — only mutations since the last known sequence
    are exchanged
  - Oplog truncated: Full sync — the entire live dataset is transferred

- **Very long live partition** (downtime > `tombstone_ttl`):
  - Default (`partition_ttl_policy: :quarantine`): EKV blocks replication for
    that member identity to prevent zombie resurrection. This is intentional.
    Operator action is required (rebuild one side, then reconnect).
  - `partition_ttl_policy: :ignore`: EKV allows reconnect/sync without
    quarantine. This trades safety for availability and can resurrect state
    that would otherwise have been protected by quarantine.

For automatic-sync cases, no operator action is needed. Monitor convergence:

```elixir
# On node A — write a canary
EKV.put(:my_kv, "canary/heal", System.system_time())

# On node B — check it appears
EKV.get(:my_kv, "canary/heal")  # should be non-nil within seconds
```

### Long Live-Partition Notes

Quarantine downtime tracking survives restarts. EKV persists down markers in
`kv_meta` keyed by `node_id` when known (fallback: node name), so a restart
does not clear long-partition history.

## Stale Database Protection

### What It Does

On startup, each shard checks how long ago it was last active. If the gap
exceeds `tombstone_ttl - gc_interval` (≈6 days 23 hours 55 minutes with
defaults), the database is considered stale and EKV **refuses startup** by
default.

This prevents a node that was offline for weeks from reintroducing entries
that other members already garbage-collected. It also avoids silent cluster-wide data
loss if an entire cold cluster is restarted after the TTL window.

### When a Stale Startup Is Rejected

Startup fails closed until an operator chooses one of these paths:

1. Single stale node rejoining fresh members:
   - wipe that node's EKV data dir
   - restart it
   - let it full-sync from healthy members

2. Full cold-cluster restart where the old on-disk data is intentional:
   - restart all nodes with `allow_stale_startup: true`
   - verify the recovered state
   - remove the override and do a rolling restart

**Symptoms:** On startup after a long offline period, logs will show stale-db
startup being refused with the detected age/threshold and recovery options.

### Allowing Stale Startup

Use `allow_stale_startup: true` only when the operator intentionally wants to
trust old on-disk state, typically during full-cluster disaster recovery. Do
not leave it enabled in normal production — it defeats zombie resurrection
protection.

## Garbage Collection Tuning

### tombstone_ttl (default: 7 days)

How long deleted entries are kept as tombstones before permanent purge.

**Increase if:**
- Nodes may be offline for extended periods (maintenance windows, etc.)
- You see zombie key resurrection after bringing nodes back online

**Decrease if:**
- Storage is constrained and you have many deletes
- All nodes are reliably connected and rarely go offline

### gc_interval (default: 5 minutes)

How often GC scans for expired TTLs, purgeable tombstones, and oplog
truncation opportunities.

**Increase if:**
- GC scans are measurably impacting write latency on loaded systems
- Storage growth rate is acceptable

**Decrease if:**
- TTL-expired entries need to disappear faster
- Oplog is growing too large between truncations

### Interaction

Stale detection threshold = `tombstone_ttl - gc_interval`. If `gc_interval`
is large relative to `tombstone_ttl`, the safety margin shrinks. Keep
`gc_interval` well below `tombstone_ttl` (at least 100x smaller).

## Blue-Green Deployments

### Setup

Enable with `blue_green: true`. Both VMs must share the same `data_dir` and
`node_id`. Each deploy must use a different Erlang node name:

```elixir
{EKV, name: :my_kv, data_dir: "/var/data/ekv", blue_green: true,
 cluster_size: 3, node_id: "m1"}
```

### How It Works

Both VMs use the **same** database files. On startup,
the new VM performs a synchronized handoff with the old VM:

1. New VM reads the `current` marker file to discover the old node name
2. Sends `{:ekv_handoff_request, ...}` to each shard on the old VM (in parallel)
3. Old VM drains pending CAS ops, persists ballot counter, checkpoints WAL,
   closes its writer connection, and sends an ack
4. New VM receives acks, opens the same database files, starts normally
5. Old VM enters proxy mode — forwards any remaining writes to the new VM

```
data_dir/
├── current           # marker: "node_name\n"
├── shard_0.db        # shared between old and new VMs (sequential access)
├── shard_1.db
└── ...
```

If the old VM is dead or the marker is stale, EKV skips handoff and opens the
files directly. SQLite's WAL recovery handles any incomplete writes.

### Requirements

- `data_dir` must be on a **shared filesystem** accessible to both VMs
  (e.g., a persistent volume on Fly.io)
- Both VMs must use the **same `node_id`** — from the cluster's perspective,
  it's one member that briefly restarted
- Each deploy must produce a **different `node()` name**

### Brief Unavailability

During handoff there is a brief window (~5-50ms) where the old VM's writer
is closed but the new VM hasn't opened yet. Writes during this window will
block until the new VM is ready (proxied via GenServer.call). Reads continue
uninterrupted on the old VM's reader connections.

### Rollback

If the new deploy is bad, start the old version again. The marker will point
to the new VM's node name, so a handoff is attempted. If the new VM is still
alive, it performs a clean handoff back. If it's already dead, the old VM
opens the files directly with WAL recovery.

## Graceful Shutdown Barrier

Use `shutdown_barrier: timeout_ms` when coordinated graceful shutdowns should
keep members serving for a short window while other members flush final writes.

```elixir
{EKV, name: :my_kv, data_dir: "/var/data/ekv",
 cluster_size: 3, shutdown_barrier: 15_000}
```

- Members stay up during graceful shutdown until other members also enter terminal
  state or the timeout expires.
- Clients participate in the coordination signal but do not count toward
  quorum.
- Blue-green outgoing members skip the wait after successful handoff/proxy.

Use this for rolling deploys or coordinated stops where other nodes may still
be finishing CAS writes. It does **not** help for crashes, `kill -9`, or bad
local supervisor ordering.

## Shard Count

### Choosing a Shard Count

Default is 8. Good for most workloads. More shards = more write parallelism
but more file descriptors and slightly more memory.

| Workload | Shards |
|----------|--------|
| Light (< 1k writes/sec) | 1–4 |
| Moderate | 8 (default) |
| Heavy (> 10k writes/sec) | 16–32 |

### Shard Count is Immutable

The shard count is persisted to `kv_meta` on first open. Changing `:shards`
after data exists raises `ArgumentError` at startup.

There is **no built-in resharding or automatic shard-count migration**.
Raw shard backups are tied to the original shard count, and member full sync
also requires matching shard counts.

If you need a different shard count and want to preserve data:

1. Stand up a fresh EKV instance or cluster with the desired `:shards`
2. Migrate data logically through the API or custom export/import tooling
3. Cut traffic over to the new instance

If you delete the data directory and restart with a new shard count, that is
effectively a fresh empty store unless some separate migration process
represents the data.

### Shard Count Mismatch Between Members

If a member connects with a different shard count, EKV logs an error and
rejects the connection. It does **not** crash. Fix the config and restart
the mismatched node.

## CAS Operations — Error Reference

| Error | Meaning | Action |
|-------|---------|--------|
| `{:error, :conflict}` | Version mismatch — value changed between `fetch` and `put` | Retry with `EKV.update/3` (auto-retries 5x) or re-fetch and retry manually |
| `{:error, :no_quorum}` | Not enough members reachable for consensus | Check connectivity; wait for partition to heal; verify `cluster_size` matches actual cluster |
| `{:error, :quorum_timeout}` | Quorum may exist, but consensus did not finish before the call timeout | Check cluster latency / load; increase timeout if needed; verify members are healthy |
| `{:error, :unconfirmed}` | CAS write entered accept phase but final outcome was ambiguous to the caller | Resolve with `EKV.get(name, key, consistent: true)` or use `resolve_unconfirmed: true` |
| `{:error, :cluster_overflow}` | More distinct node_ids reachable than `cluster_size` | Remove extra nodes or increase `cluster_size` on all nodes |
| `{:error, :cas_managed_key}` | Eventual `put`/`delete` was attempted on a CAS-managed key | Use CAS write APIs for that key; do not mix CAS -> LWW writes |
| `{:error, :unavailable}` | Client backend or ambiguity-resolution read was unavailable | Check member availability/routing; retry after route recovers |
| `{:error, :cas_not_configured}` | `cluster_size` not set | Add `cluster_size` to config |

## Monitoring Checklist

| Check | How | Healthy |
|-------|-----|---------|
| Cluster membership | `EKV.info(:my_kv).connected_members` | Length = `cluster_size - 1` |
| Node identity | `EKV.info(:my_kv).node_id` | Non-nil, stable across restarts |
| Data reachable | `EKV.get(:my_kv, "known_key")` | Returns expected value |
| CAS working | `EKV.update(:my_kv, "counter", &(&1 + 1))` | Returns `{:ok, _}` |
| Backup freshness | Periodic `EKV.backup/2` + timestamp | Backup age < `tombstone_ttl` |
