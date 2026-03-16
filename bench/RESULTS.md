# EKV Local Benchmark Results

**Date:** 2026-03-16 10:25 EDT  
**Machine:** Darwin 23.6.0 (arm64), 8 schedulers  
**Config:** 8 shards (default)

Command run:

```bash
bash bench/run_local.sh
```

## 1. Get Throughput (SQLite read path, no GenServer)

| Mode | ops/sec | p50 | p99 | max |
|------|---------|-----|-----|-----|
| Sequential | 276,967 | 3 us | 12 us | 396 us |
| Parallel (16 workers) | 150,833 | - | - | - |

## 2. Put Throughput (GenServer call per write)

| Mode | ops/sec | p50 | p99 | max |
|------|---------|-----|-----|-----|
| Sequential | 17,039 | 45 us | 108 us | 16,387 us |
| Parallel (16 workers) | 30,920 | - | - | - |

## 3. Mixed Workload (80% reads, 20% writes)

| ops/sec |
|---------|
| 85,801 |

## 4. Delete Throughput

| ops/sec | p50 | p99 | max |
|---------|-----|-----|-----|
| 16,540 | 48 us | 109 us | 4,774 us |

## 5. Prefix Scan (fan-out to all 8 shards)

| Keys | Operation | ops/sec | p50 | p99 | max |
|------|-----------|---------|-----|-----|-----|
| 100 | scan/2 | 6,308 | 153 us | 210 us | 311 us |
| 100 | keys/2 | 6,970 | 139 us | 201 us | 229 us |
| 1,000 | scan/2 | 1,329 | 695 us | 1,650 us | 14,461 us |
| 1,000 | keys/2 | 1,764 | 566 us | 633 us | 795 us |
| 10,000 | scan/2 | 166 | 5,946 us | 6,737 us | 39,412 us |
| 10,000 | keys/2 | 216 | 4,541 us | 4,754 us | 54,963 us |

## 6. TTL Put Throughput

| Mode | ops/sec | p50 | p99 | max |
|------|---------|-----|-----|-----|
| Without TTL | 17,399 | 45 us | 100 us | 5,921 us |
| With TTL | 15,366 | 49 us | 152 us | 14,316 us |

## 7. Value Size Scaling

| Size | Put ops/sec | Get ops/sec | Get p50 | Get p99 | Get max |
|------|-------------|-------------|---------|---------|---------|
| 64 B | 15,831 | 289,611 | 3 us | 7 us | 69 us |
| 1 KB | 14,547 | 246,214 | 4 us | 15 us | 67 us |
| 10 KB | 7,304 | 140,556 | 6 us | 17 us | 82 us |
| 100 KB | 1,581 | 34,931 | 27 us | 60 us | 155 us |

## 8. Shard Scaling (parallel puts, 32 workers)

| Shards | ops/sec |
|--------|---------|
| 1 | 18,756 |
| 2 | 27,416 |
| 4 | 29,943 |
| 8 | 31,747 |

## 9. Subscribe Overhead on Writes

| Subscribers | ops/sec | p50 | p99 | max |
|-------------|---------|-----|-----|-----|
| 0 | 17,486 | 45 us | 107 us | 1,508 us |
| 1 | 17,104 | 46 us | 106 us | 1,552 us |
| 10 | 15,593 | 53 us | 113 us | 1,566 us |
| 100 | 10,337 | 79 us | 214 us | 15,133 us |

## 10. Subscribe Fan-out (all subscribers match)

| Subscribers | ops/sec | p50 | p99 | max |
|-------------|---------|-----|-----|-----|
| 1 | 16,121 | 52 us | 102 us | 1,321 us |
| 10 | 14,177 | 60 us | 119 us | 1,502 us |
| 50 | 12,672 | 63 us | 207 us | 1,626 us |
| 200 | 8,143 | 106 us | 275 us | 1,720 us |

## 11. Subscribe at Scale (10,000 subscribers)

| Mode | ops/sec | p50 | p99 | max |
|------|---------|-----|-----|-----|
| Random keys (5,000 puts) | 13,352 | 58 us | 212 us | 1,517 us |
| Same key (5,000 puts) | 14,168 | 42 us | 456 us | 1,936 us |

## 12. Subscribe Event Latency (put call -> event received)

| ops/sec | p50 | p99 | max |
|---------|-----|-----|-----|
| 14,683 | 52 us | 208 us | 25,875 us |
