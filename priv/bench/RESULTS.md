# EKV Benchmark Results

**Date:** 2026-02-21
**Machine:** Darwin 23.6.0, 8 schedulers
**Config:** 8 shards (default), log disabled, GC disabled

## Local Benchmarks

### 1. Get Throughput (ETS hot path, no GenServer)

| Mode | ops/sec | p50 | p99 | max |
|------|---------|-----|-----|-----|
| Sequential | 1,428,183 | 1 us | 2 us | 730 us |
| Parallel (16 workers) | 226,075 | — | — | — |

### 2. Put Throughput (GenServer call per write)

| Mode | ops/sec | p50 | p99 | max |
|------|---------|-----|-----|-----|
| Sequential | 18,564 | 39 us | 146 us | 13,585 us |
| Parallel (16 workers) | 25,650 | — | — | — |

### 3. Mixed Workload (80% reads, 20% writes)

| ops/sec |
|---------|
| 99,140 |

### 4. Delete Throughput

| ops/sec | p50 | p99 | max |
|---------|-----|-----|-----|
| 16,772 | 43 us | 104 us | 3,892 us |

### 5. Prefix Scan (fan-out to all 8 shards)

| Keys | Operation | ops/sec | p50 | p99 | max |
|------|-----------|---------|-----|-----|-----|
| 100 | list/2 | 25,213 | 38 us | 59 us | 78 us |
| 100 | keys/2 | 34,567 | 28 us | 51 us | 81 us |
| 1,000 | list/2 | 3,309 | 296 us | 350 us | 462 us |
| 1,000 | keys/2 | 5,290 | 186 us | 226 us | 282 us |
| 10,000 | list/2 | 322 | 3,069 us | 3,559 us | 4,367 us |
| 10,000 | keys/2 | 473 | 2,092 us | 2,405 us | 3,296 us |

### 6. TTL Put Throughput

| Mode | ops/sec | p50 | p99 | max |
|------|---------|-----|-----|-----|
| Without TTL | 17,868 | 39 us | 159 us | 14,905 us |
| With TTL | 14,875 | 49 us | 158 us | 5,747 us |

### 7. Value Size Scaling

| Size | Put ops/sec | Get ops/sec | Get p50 | Get p99 |
|------|-------------|-------------|---------|---------|
| 64 B | 13,841 | 1,346,982 | 1 us | 1 us |
| 1 KB | 12,267 | 957,762 | 1 us | 2 us |
| 10 KB | 4,157 | 495,712 | 2 us | 12 us |
| 100 KB | 580 | 49,236 | 16 us | 49 us |

### 8. Shard Scaling (parallel puts, 32 workers)

| Shards | ops/sec |
|--------|---------|
| 1 | 17,102 |
| 2 | 15,281 |
| 4 | 17,537 |
| 8 | 21,219 |

## Distributed Benchmarks

### 1. Replication Latency (put on replica1, visible on replica2)

| ops/sec | p50 | p99 | max |
|---------|-----|-----|-----|
| 19,515 | 44 us | 80 us | 5,234 us |

### 2. Bulk Sync (new peer catches up)

| Keys | Sync time | keys/sec |
|------|-----------|----------|
| 1,000 | 105 ms | 9,519 |
| 10,000 | 644 ms | 15,505 |

### 3. Concurrent Cross-Node Writes (5K per node)

| Total keys | ops/sec | Wall time |
|------------|---------|-----------|
| 10,000 | 5,920 | 1,688 ms |

### 4. Delete Replication

| Deletes | Wall time | deletes/sec |
|---------|-----------|-------------|
| 1,000 | 257 ms | 3,879 |
| 5,000 | 621 ms | 8,043 |

### 5. Network Partition & Heal

| Metric | Value |
|--------|-------|
| Isolation verified | yes (0 leaked keys) |
| Convergence after heal (2,000 keys) | 126 ms |

### 6. Value Size Replication Latency

| Size | ops/sec | p50 | p99 | max |
|------|---------|-----|-----|-----|
| 64 B | 14,703 | 44 us | 93 us | 5,461 us |
| 1 KB | 21,691 | 44 us | 77 us | 150 us |
| 10 KB | 4,626 | 41 us | 5,969 us | 11,866 us |
| 100 KB | 1,175 | 85 us | 23,802 us | 35,919 us |

### 7. Busy App Simulation (10 workers/node, 200 keys/worker, 5 churn rounds)

| Phase | ops/sec |
|-------|---------|
| Initial load + converge (4,000 keys) | 5,252 |
| Churn round 1 | 11,558 |
| Churn round 2 | 12,266 |
| Churn round 3 | 12,429 |
| Churn round 4 | 13,255 |
| Churn round 5 | 13,256 |
| Cross-node reads (10,000) | 97,298 |
| **Total wall time** | **2,462 ms** |
| Final consistency | replica1=4000, replica2=4000, match=true |

## Key Takeaways

- **Reads are fast:** ~1.4M ops/sec sequential gets (ETS hot path, sub-microsecond p50)
- **Writes are SQLite-bound:** ~18K ops/sec sequential puts, ~25K parallel (GenServer + WAL)
- **Replication is tight:** 44 us p50 replication latency across nodes
- **Partition heal is quick:** 126 ms to converge 2,000 keys after reconnect
- **Value size matters for writes:** 100KB values drop puts to 580 ops/sec but gets stay at 49K
- **Shard scaling helps parallel writes:** 8 shards = 21K ops/sec vs 17K for 1 shard (32 workers)
- **Bulk sync scales well:** 15.5K keys/sec for 10K key sync (better throughput at scale)
