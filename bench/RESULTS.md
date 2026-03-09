# EKV Local Benchmark Results

**Date:** 2026-03-03 11:35 EST  
**Machine:** Darwin 23.6.0 (arm64), 8 schedulers  
**Config:** 8 shards (default)

Command run:

```bash
cd /Users/chris/oss/ekv/bench
./run_local.sh
```

## 1. Get Throughput (SQLite read path, no GenServer)

| Mode | ops/sec | p50 | p99 | max |
|------|---------|-----|-----|-----|
| Sequential | 204,180 | 3 us | 18 us | 1,281 us |
| Parallel (16 workers) | 138,689 | - | - | - |

## 2. Put Throughput (GenServer call per write)

| Mode | ops/sec | p50 | p99 | max |
|------|---------|-----|-----|-----|
| Sequential | 17,651 | 29 us | 137 us | 20,819 us |
| Parallel (16 workers) | 27,868 | - | - | - |

## 3. Mixed Workload (80% reads, 20% writes)

| ops/sec |
|---------|
| 95,107 |

## 4. Delete Throughput

| ops/sec | p50 | p99 | max |
|---------|-----|-----|-----|
| 17,259 | 32 us | 101 us | 13,669 us |

## 5. Prefix Scan (fan-out to all 8 shards)

| Keys | Operation | ops/sec | p50 | p99 | max |
|------|-----------|---------|-----|-----|-----|
| 100 | scan/2 | 5,079 | 159 us | 560 us | 8,955 us |
| 100 | keys/2 | 7,575 | 121 us | 410 us | 743 us |
| 1,000 | scan/2 | 1,371 | 691 us | 1,388 us | 2,097 us |
| 1,000 | keys/2 | 2,205 | 444 us | 674 us | 960 us |
| 10,000 | scan/2 | 165 | 5,801 us | 9,645 us | 31,290 us |
| 10,000 | keys/2 | 326 | 3,021 us | 3,672 us | 5,948 us |

## 6. TTL Put Throughput

| Mode | ops/sec | p50 | p99 | max |
|------|---------|-----|-----|-----|
| Without TTL | 18,975 | 29 us | 137 us | 105,711 us |
| With TTL | 17,333 | 34 us | 123 us | 11,538 us |

## 7. Value Size Scaling

| Size | Put ops/sec | Get ops/sec | Get p50 | Get p99 | Get max |
|------|-------------|-------------|---------|---------|---------|
| 64 B | 9,105 | 288,875 | 3 us | 8 us | 67 us |
| 1 KB | 4,302 | 245,272 | 4 us | 12 us | 65 us |
| 10 KB | 1,861 | 111,861 | 7 us | 65 us | 548 us |
| 100 KB | 264 | 2,141 | 32 us | 2,960 us | 31,128 us |

## 8. Shard Scaling (parallel puts, 32 workers)

| Shards | ops/sec |
|--------|---------|
| 1 | 17,111 |
| 2 | 19,669 |
| 4 | 16,930 |
| 8 | 15,150 |

## 9. Subscribe Overhead on Writes

| Subscribers | ops/sec | p50 | p99 | max |
|-------------|---------|-----|-----|-----|
| 0 | 9,267 | 33 us | 240 us | 119,385 us |
| 1 | 12,583 | 33 us | 168 us | 70,882 us |
| 10 | 9,225 | 41 us | 261 us | 73,810 us |
| 100 | 8,459 | 63 us | 263 us | 243,128 us |

## 10. Subscribe Fan-out (all subscribers match)

| Subscribers | ops/sec | p50 | p99 | max |
|-------------|---------|-----|-----|-----|
| 1 | 7,225 | 43 us | 1,243 us | 45,026 us |
| 10 | 9,496 | 52 us | 331 us | 27,115 us |
| 50 | 9,273 | 56 us | 387 us | 16,139 us |
| 200 | 6,803 | 113 us | 386 us | 38,451 us |

## 11. Subscribe at Scale (10,000 subscribers)

| Mode | ops/sec | p50 | p99 | max |
|------|---------|-----|-----|-----|
| Random keys (5,000 puts) | 10,477 | 43 us | 282 us | 9,893 us |
| Same key (5,000 puts) | 15,263 | 24 us | 433 us | 8,396 us |

## 12. Subscribe Event Latency (put call -> event received)

| ops/sec | p50 | p99 | max |
|---------|-----|-----|-----|
| 13,861 | 40 us | 249 us | 12,550 us |
