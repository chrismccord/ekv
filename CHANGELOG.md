## 0.4.6 (2026-09-23)
- Prevent anti-entropy storms after member churn by trying a retained relayed
  delta for disconnected, retired, unknown, and quarantined third-party origins
  before falling back to a full snapshot.
- Coalesce duplicate full-sync requests into one active outbound snapshot
  stream per destination shard, preventing parallel rescans and duplicate chunk
  sends.
- Remove stale transient quarantine entries when a remote node disconnects while
  retaining the durable reconnect fence.

## 0.4.5 (2026-09-23)
- Fix precompiled NIF checksum packaging and reject stale or incomplete release
  manifests before building or publishing Hex packages.
- Add Linux x86-64 and ARM64 musl binaries for Alpine images, and build glibc
  binaries against Ubuntu 22.04 (glibc 2.35) for compatible Fly Machine images.
- Generate and verify the complete release checksum manifest after all
  precompiled binaries are built.

## 0.4.4 (2026-09-22)
- Update bundled SQLite from 3.47.2 to 3.53.4, including the upstream WAL-reset
  database corruption fix.

## 0.4.3 (2026-07-13)
- Background WAL checkpoints for improveds sustained write performance

## 0.4.2 (2026-07-12)
- Bound standalone oplog retention to fix single member oplog failing to gc

## 0.4.1 (2026-04-20)
- Fix a blue-green handoff race where a queued local write batch could reach proxy mode and
  crash on nil prepared statements instead of proxying or returning `{:error, :shutting_down}`
- Add an optional `EKV.Transport` data-plane adapter for member shard sends and routed
  client RPC, with Erlang distribution as the default transport
- Add `:sync_chunk_max_bytes` so delta/full sync chunks are bounded by both entry count
  and approximate uncompressed payload bytes

## 0.4.0 (2026-04-17)
- Remove legacy non matched message handlers

## 0.3.3 (2026-04-17)
- Use `send_nosuspend` for best-effort live replication and repair coordination traffic
  which prevents blocking of shards for an individual dist erl socket at its busy limit

## 0.3.2 (2026-04-16)
- Fix leaked late `:ekv_local_reply` messages after local request timeout

## 0.3.1 (2026-04-15)
- Optimize replicated message churn with turn-taking queue

## 0.3.0 (2026-03-19)
- Add `:observer` mode
- Fix barrier reads on non-existing keys

## 0.2.0 (2026-03-19)
- Add CAS support

## 0.1.6 (2026-02-26)
- Add blue_green support

## 0.1.5 (2026-02-24)
- Account for edge case in gc cleanup

## 0.1.4 (2026-02-23) 🚀
- Initial release!
