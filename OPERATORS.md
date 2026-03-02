# EKV Operator Runbook

Scenario-based guide for operating EKV in production.

## Checking Cluster Health

```elixir
EKV.info(:my_kv)
# => %{
#   name: :my_kv,
#   node_id: "a1b2c3d4e5f6",
#   cluster_size: 3,
#   shards: 8,
#   data_dir: "/var/data/ekv",
#   connected_peers: [
#     %{node: :app@host2, node_id: "f7e8d9c0b1a2"},
#     %{node: :app@host3, node_id: "1122334455aa"}
#   ]
# }
```

Verify:
- `connected_peers` length equals `cluster_size - 1`
- Each peer has a non-nil `node_id`
- `node_id` values are distinct

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
  skip_stale_check: true,         # backup is old by definition
  gc_interval: :timer.hours(999)  # don't GC during verification
)

# Spot-check keys
EKV.get(:backup_check, "important/key")
EKV.keys(:backup_check, "users/") |> length()

# Clean up
Supervisor.stop(:backup_check_ekv_sup)
```

## Disaster Recovery

### Restoring a Recent Backup (< tombstone_ttl old)

If the backup is younger than `tombstone_ttl` (default 7 days):

1. Stop the node
2. Replace `data_dir` contents with backup files
3. Restart normally — delta sync from peers catches up

### Restoring an Old Backup to the Entire Cluster

If the backup is older than `tombstone_ttl`, stale DB detection would wipe
it on startup. Restore all nodes simultaneously with the check disabled:

1. Stop all nodes
2. Copy backup files to each node's `data_dir`
3. Restart all nodes with `skip_stale_check: true`:
   ```elixir
   {EKV, name: :my_kv, data_dir: "/var/data/ekv", skip_stale_check: true}
   ```
4. Verify data is present on all nodes
5. Remove `skip_stale_check: true` and do a rolling restart

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

If more machines are running than `cluster_size` allows, CAS operations
(`:if_vsn`, `EKV.update/3`) return `{:error, :cluster_overflow}`. Regular
LWW operations (`put`, `get`, `delete` without `:if_vsn`) continue working.

**Symptom:**
```elixir
EKV.put(:my_kv, "key", "val", if_vsn: nil)
# => {:error, :cluster_overflow}
```

**Diagnosis:**
```elixir
info = EKV.info(:my_kv)
IO.puts("#{length(info.connected_peers) + 1} nodes, cluster_size=#{info.cluster_size}")
```

**Fix:** Remove the extra machine(s). CAS resumes immediately.

### Intentional Scale-Up

1. Update `cluster_size` in config on **all existing nodes**
2. Rolling restart the existing nodes
3. Start the new node(s) with the same `cluster_size`

The new node auto-generates a `node_id` and joins the cluster. Full sync
from peers populates its data.

### Intentional Scale-Down

1. Stop the node being removed
2. Wait for one GC cycle (`gc_interval`, default 5 min) — the removed node's
   HWM is pruned, allowing oplog truncation to proceed
3. Optionally update `cluster_size` on remaining nodes and rolling restart

## Network Partitions

### During a Partition

Both sides continue serving reads and writes. LWW resolves conflicts on heal.
CAS operations require a quorum (`floor(cluster_size / 2) + 1`) — the
minority side returns `{:error, :no_quorum}` for CAS but LWW still works.

### After Partition Heals

Nodes automatically reconnect and sync:

- **Short partition** (oplog intact): Delta sync — only mutations since
  the last known sequence are exchanged
- **Long partition** (oplog truncated): Full sync — the entire live dataset
  is transferred. This happens when `gc_interval` ticks have truncated the
  oplog past the peer's last known position

No operator action needed. Monitor convergence:

```elixir
# On node A — write a canary
EKV.put(:my_kv, "canary/heal", System.system_time())

# On node B — check it appears
EKV.get(:my_kv, "canary/heal")  # should be non-nil within seconds
```

## Stale Database Protection

### What It Does

On startup, each shard checks how long ago it was last active. If the gap
exceeds `tombstone_ttl - gc_interval` (≈6 days 23 hours 55 minutes with
defaults), the database is considered stale and is **deleted**.

This prevents a node that was offline for weeks from reintroducing entries
that peers already garbage-collected.

### When a Wipe Happens

A wiped shard rebuilds from scratch via full sync from peers. This is safe
and automatic — no data is lost cluster-wide (the stale node's data was, by
definition, outdated).

**Symptoms:** On startup after a long offline period, logs will show the
shard being wiped and a full sync starting. Initial reads on that node
return nil until sync completes.

### Disabling the Check

Use `skip_stale_check: true` only for disaster recovery (see above). Do not
leave it enabled in production — it defeats zombie resurrection protection.

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

Both VMs use the **same** database files (no slot directories). On startup,
the new VM performs a synchronized handoff with the old VM:

1. New VM reads the `current` marker file to discover the old node name
2. Sends `{:ekv_handoff_request}` to each shard on the old VM (in parallel)
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

If the old VM is dead, handoff requests time out after 5 seconds and the new
VM opens the files directly. SQLite's WAL recovery handles any incomplete
writes.

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
after data exists raises `ArgumentError` at startup. To change shard count:

1. Take a backup
2. Delete the data directory
3. Restart with new shard count (triggers full sync from peers)

### Shard Count Mismatch Between Peers

If a peer connects with a different shard count, EKV logs an error and
rejects the connection. It does **not** crash. Fix the config and restart
the mismatched node.

## CAS Operations — Error Reference

| Error | Meaning | Action |
|-------|---------|--------|
| `{:error, :conflict}` | Version mismatch — value changed between `fetch` and `put` | Retry with `EKV.update/3` (auto-retries 5x) or re-fetch and retry manually |
| `{:error, :no_quorum}` | Not enough peers reachable for consensus | Check connectivity; wait for partition to heal; verify `cluster_size` matches actual cluster |
| `{:error, :cluster_overflow}` | More distinct node_ids reachable than `cluster_size` | Remove extra nodes or increase `cluster_size` on all nodes |
| `{:error, :cas_not_configured}` | `cluster_size` not set | Add `cluster_size` to config |

## Monitoring Checklist

| Check | How | Healthy |
|-------|-----|---------|
| Cluster membership | `EKV.info(:my_kv).connected_peers` | Length = `cluster_size - 1` |
| Node identity | `EKV.info(:my_kv).node_id` | Non-nil, stable across restarts |
| Data reachable | `EKV.get(:my_kv, "known_key")` | Returns expected value |
| CAS working | `EKV.update(:my_kv, "counter", &(&1 + 1))` | Returns `{:ok, _}` |
| Backup freshness | Periodic `EKV.backup/2` + timestamp | Backup age < `tombstone_ttl` |
