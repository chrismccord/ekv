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
