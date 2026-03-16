# EKV Distributed Benchmark Results

**Date:** 2026-03-16 10:25 EDT  
**Machine:** Darwin 23.6.0 (arm64)  
**Cluster:** 2 nodes (`replica1`, `replica2`) on localhost  
**Shards:** 8  
**Run:** `bash bench/run_distributed.sh`

## Summary

| Workload | ops/sec | p50 | p99 |
|----------|---------|-----|-----|
| Replication latency (put on replica1 -> visible on replica2) | 4,740 | 200 us | 347 us |
| Concurrent cross-node writes | 13,598 | — | — |
| Cross-node reads in busy simulation | 66,973 | — | — |
| Subscribe replication event latency | 4,451 | 205 us | 385 us |

## 1. Replication Latency

| Workload | ops/sec | p50 | p99 | max |
|----------|---------|-----|-----|-----|
| 1,000 puts on replica1 -> visible on replica2 | 4,740 | 200 us | 347 us | 1,589 us |

## 2. Bulk Sync

| Keys | Sync time | Keys/sec |
|------|-----------|----------|
| 1,000 | 48 ms | 20,708 |
| 10,000 | 251 ms | 39,750 |

## 3. Concurrent Cross-Node Writes

| Workload | Total ops | ops/sec |
|----------|-----------|---------|
| 10,000 keys (5,000 per node), both converged | 10,000 | 13,598 |

## 4. Delete Replication

| Deletes | Delete + converge | Deletes/sec |
|---------|-------------------|-------------|
| 1,000 | 111 ms | 8,967 |
| 5,000 | 480 ms | 10,416 |

## 5. Network Partition & Heal

| Workload | Result |
|----------|--------|
| During partition | replica1 saw `0` of replica2's keys; replica2 saw `0` of replica1's keys |
| Heal + convergence | 65 ms for 2,000 keys |

## 6. Value Size Replication Latency

| Size | ops/sec | p50 | p99 | max |
|------|---------|-----|-----|-----|
| 64 B | 3,907 | 221 us | 648 us | 1,028 us |
| 1 KB | 4,371 | 216 us | 306 us | 1,654 us |
| 10 KB | 3,543 | 245 us | 1,680 us | 3,426 us |
| 100 KB | 917 | 755 us | 5,719 us | 7,819 us |

## 7. Busy App Simulation

Configuration:

- workers/node: `10`
- keys/worker: `200`
- churn rounds: `5`

| Phase | Total ops | ops/sec |
|-------|-----------|---------|
| initial load + converge | 4,000 | 12,988 |
| churn round 1 | 4,000 | 18,746 |
| churn round 2 | 4,000 | 19,221 |
| churn round 3 | 4,000 | 19,358 |
| churn round 4 | 4,000 | 19,188 |
| churn round 5 | 4,000 | 15,143 |
| cross-node reads | 10,000 | 66,973 |

Final state:

- total wall time: `1,558 ms`
- final keys: `replica1=4000`, `replica2=4000`, `match=true`

## 8. Subscribe Replication Event Latency

| Workload | ops/sec | p50 | p99 | max |
|----------|---------|-----|-----|-----|
| 1,000 puts on replica1 -> event signal back to coordinator | 4,451 | 205 us | 385 us | 1,585 us |

## 9. Subscribe Overhead on Distributed Writes

| Subscribers | Total ops | ops/sec |
|-------------|-----------|---------|
| 0 | 2,000 | 8,344 |
| 10 | 2,000 | 7,802 |
| 100 | 2,000 | 6,207 |

## Notes

- These are localhost distributed numbers, not geo or WAN numbers.
- Bulk sync and partition-heal timings are wall-clock convergence measurements, not just sender-side enqueue time.
