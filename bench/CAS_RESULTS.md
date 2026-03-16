# EKV CAS Benchmark Results

**Date:** 2026-03-16 10:25 EDT  
**Machine:** Darwin 23.6.0 (arm64)  
**Cluster:** 3 nodes (`replica1`, `replica2`, `replica3`) on localhost  
**Shards:** 8  
**Run:** `bash bench/run_cas.sh`

## Summary

| Workload | ops/sec | p50 | p99 |
|----------|---------|-----|-----|
| CAS insert-if-absent (lookup + put if_vsn: nil) | 2,195 | 421 us | 1,519 us |
| CAS conditional update (lookup + put if_vsn: vsn) | 2,481 | 377 us | 1,494 us |
| Consistent put (put consistent: true) | 2,350 | 345 us | 1,752 us |
| Eventual read (local SQLite) | 16,890 | 47 us | 162 us |
| Consistent read (quorum) | 2,663 | 343 us | 1,711 us |
| Update distinct keys (no contention) | 2,338 | 390 us | 1,597 us |
| Update same key (sequential) | 2,672 | 345 us | 1,589 us |
| Update with TTL | 2,654 | 353 us | 1,410 us |
| Peak parallel CAS throughput (2048 workers/node) | 7,553 | — | — |
| Hot-key 300 increments | 2,849 | — | — |
| Hot-key 1,500 increments | 2,655 | — | — |
| CAS replication (R1 -> visible R2) | 1,950 | 499 us | 672 us |
| Config store 90% read / 10% write | 2,675 | — | — |
| Config store 50% read / 50% write | 2,160 | — | — |
| Session lifecycle (single node) | 677 | 1,399 us | 3,455 us |
| Session lifecycle (cross-node) | 183 | 5,330 us | 9,403 us |
| LWW put (baseline) | 6,056 | 157 us | 247 us |
| CAS update (quorum) | 2,394 | 374 us | 1,721 us |

**Consistent read/eventual read p50 ratio: 7.3x**  
**CAS/LWW p50 ratio: 2.4x**

## Parallel CAS Throughput

| Workers per node | Total ops | ops/sec |
|------------------|-----------|---------|
| 64 | 12,288 | 6,855 |
| 128 | 12,288 | 7,385 |
| 256 | 12,288 | 7,622 |
| 512 | 24,576 | 7,467 |
| 1024 | 49,152 | 7,419 |
| 1536 | 73,728 | 7,429 |
| 2048 | 98,304 | 7,553 |

## Hot-Key Contention

| Workload | Successes | Unconfirmed | Final value | ops/sec |
|----------|-----------|-------------|-------------|---------|
| 300 increments across 3 request nodes | 275 | 25 | 283 | 2,849 |
| 1,500 increments across 3 request nodes | 1,432 | 68 | 1,456 | 2,655 |

## Large Payload Put Latency

| Mode | Compression | ops/sec | p50 | p99 | max |
|------|-------------|---------|-----|-----|-----|
| LWW put | disabled | 28 | 34,842 us | 68,212 us | 68,212 us |
| Consistent put | disabled | 17 | 49,846 us | 434,559 us | 434,559 us |
| LWW put | enabled | 21 | 47,439 us | 99,407 us | 99,407 us |
| Consistent put | enabled | 16 | 53,393 us | 300,750 us | 300,750 us |

Measured payload accounting in this run:

| Compression mode | Raw bytes | Wire bytes | Ratio |
|------------------|-----------|------------|-------|
| disabled | 4,001,126 | 76,957 | 52.0x |
| enabled | 4,001,126 | 76,957 | 52.0x |

## Notes

- All operations are measured from the coordinator node via `:erpc.call`, so latencies include the `erpc` round trip.
- The later synthetic scenarios still emitted startup full-sync chatter because the harness cold-starts scenarios aggressively; these numbers reflect that current harness behavior.
- The hot-key workloads showed `:unconfirmed` outcomes under contention, but no exhausted conflict retries in this run.
