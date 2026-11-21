# Hetzner Cloud Mixed Go + Rust Mimir Cluster Stress Report (Pass 5)

**Date:** 2026-11-21
**Total Active Benchmark Duration:** 72.83 minutes (`4,369.6 s` across Stages 1A, 1B, 2, 3, 4A, 4B, 4C; total written samples: **877,824,384**)
**Images Tested:**
- **Rust (`mimir-rs`):** `ghcr.io/birdayz/mimir-rs:0bbd40e212861989813e3e00994a182dac095fd5` (`rustc 1.100.0`, `linux/amd64`, post-Waves 15–23 / 211 bug & parity fixes)
- **Upstream Go Mimir:** `docker.io/grafana/mimir:latest` (`version=3.2.6`, `revision=634964f9`, `go1.26.11`, `linux/amd64`)
- **Kafka Broker:** `docker.io/redpandadata/redpanda:latest`
- **Object Storage (S3):** `docker.io/bitnamilegacy/minio:latest`

---

## 1. Infrastructure and Topology (`cluster=mimir-stress`, `fsn1`)

Provisioned on Hetzner Cloud (`fsn1`, `ubuntu-24.04`) on isolated private network `mimir-stress-net` (`10.88.0.0/16`, subnet `10.88.0.0/24`).

| Hostname | Role / Zone | Server Type | vCPU / RAM / Kernel | Private IP | Software |
|---|---|---|---|---|---|
| `mimir-stress-ing-1` | Ingester 1 (`zone-a`) | `cpx32` | 4 vCPU (AMD EPYC-Genoa) / 7,745 MiB / `6.8.0-138-generic` | `10.88.0.11` | `mimir-rs:0bbd40e` |
| `mimir-stress-ing-2` | Ingester 2 (`zone-b`) | `cpx32` | 4 vCPU (AMD EPYC-Genoa) / 7,745 MiB / `6.8.0-138-generic` | `10.88.0.12` | `mimir-rs:0bbd40e` |
| `mimir-stress-ing-3` | Ingester 3 (`zone-c`) | `cpx32` | 4 vCPU (AMD EPYC-Genoa) / 7,745 MiB / `6.8.0-138-generic` | `10.88.0.13` | `grafana/mimir:3.2.6` |
| `mimir-stress-go-stack` | Go Stack + Redpanda + MinIO | `ccx43` | 16 dedicated vCPU (AMD EPYC-Milan) / 62,784 MiB / `6.8.0-138-generic` | `10.88.0.20` | Go Mimir (`distributor`, `query-frontend`, `query-scheduler`, `querier`, `store-gateway`, `compactor`), Redpanda (`:9092`), MinIO (`:9000`) |
| `mimir-stress-loadgen` | Loadgen + PromQL + Deep Scraper | `ccx43` | 16 dedicated vCPU (AMD EPYC-Milan) / 62,784 MiB / `6.8.0-147-generic` | `10.88.0.30` | Custom Go `stress-loadgen` + 5s cgroup/`smaps_rollup`/`/proc/<pid>/io`/`/metrics` scraper + `pprof`/`fgprof` collector |

All Mimir services communicate over `10.88.0.0/24` using `memberlist` TCP gossip (`7941..7947`), with `replication_factor: 3` and `zone_awareness_enabled: true`.

---

## 2. Key Deep-Telemetry Discoveries in Pass 5 (`0bbd40e`)

1. **Zero NVMe Page-Cache Thrashing on Rust vs `152 GiB` Physical Disk Reads & `2.20M` Major Page Faults on Go:**
   - Collecting `/proc/<pid>/io`, `/proc/<pid>/smaps_rollup`, and cgroup v2 `memory.stat` (`pgmajfault`) across all stages exposed a second-order consequence of Go's `3.1x–3.3x` higher `RssAnon` on 8 GB nodes:
     - **End of Stage 1B (35m Kafka soak + 1M-series unthrottled scans):** Rust `ing-1` and `ing-2` held `1,969.4 / 1,891.4 MB RssAnon`, leaving `> 5.5 GB` of host RAM for `chunks_head` mmap page cache -> **`0.0 MB` physical disk `read_bytes` and `0` major page faults (`pgmajfault = 0`)**. Go `ing-3` held `6,704.2 MB RssAnon` on the same `7,745 MiB` VM, starving the Linux page cache and forcing **`87,128.1 MB` (`85.1 GiB`) of physical NVMe reads and `1,259,493` major page faults**.
     - **End of Stage 2 (45m cumulative):** Go `ing-3` reached **`155,733.9 MB` (`152.1 GiB`) physical disk reads and `2,196,405` major page faults**, vs `4.9–10.7 GB` (compaction reads only) and `9k–73k` major faults on Rust `ing-1`/`ing-2`.
     - **End of Stage 4B (Classic Push 2.5M series):** Go `ing-3` thrashed **`106,635.2 MB` (`104.1 GiB`) of NVMe reads and `1,006,476` major page faults** before kernel `oom-kill` (`anon-rss: 7,555,032 kB`), while Rust `ing-1` and `ing-2` recorded **`2.4 MB` and `13.1 MB` physical disk reads (`29–130` major faults)** all the way through `3,500,000` series in Stage 4C with **0 OOMs and 0 restarts**.

2. **Server-Side `cortex.Ingester/QueryStream` Latency Is `2.7x` Faster in Steady State & `6.3x` Faster Under Scan Storms:**
   - Directly scraping `cortex_request_duration_seconds_{sum,count}{route="/cortex.Ingester/QueryStream"}` on each ingester isolates per-pod gRPC read execution from querier/frontend fanout:
     - **Stage 1A (`~81,500` `QueryStream`s/pod):** Rust `ing-1` **`68.15 ms`**, Rust `ing-2` **`60.28 ms`** vs Go `ing-3` **`172.38 ms`** (**`2.69x` faster**; `LabelValuesCardinality`: Rust **`2.51–2.98 ms`** vs Go **`27.95 ms` = `10.2x` faster**).
     - **Stage 1B delta (`~5,500` heavy `QueryStream`s/pod during unthrottled 1M `count_exact`):** Rust `ing-1` **`404.3 ms`**, Rust `ing-2` **`389.3 ms`** vs Go `ing-3` **`2,505.8 ms`** (**`6.32x` faster**).

3. **Pass 4 -> Pass 5 (`0bbd40e`) Improvements Across Waves 15–23:**
   - **Stage 1A:** Served **`40,566` PromQL queries (`0` errors)** vs `37,983` in Pass 4; `count_exact` p99 dropped **`4.26x`** (`48,698 ms -> 11,423 ms`, max `12,469 ms`); Rust avg `RssAnon` dropped from `1,567–1,606 MB` to **`1,476–1,526 MB`** (`3.14x` below Go's `4,710 MB`) and avg CPU dropped to **`2.09–2.42` cores** (vs Go `3.39` cores).
   - **Stage 1B:** Served **`2,311` PromQL queries (`269` full 1M-series `count_exact` scans, `mismatch = 0`)** vs `1,859` (`198` scans) in Pass 4 (**`+24.3%` higher read throughput**); `/api/v1/series` p95 improved **`4.6x`** (`8,808 -> 1,914 ms`) and `cardinality` p99 improved **`8.0x`** (`5,882 -> 732 ms`).
   - **Stage 2 Live Flush:** Served **`4,514` PromQL queries** vs `1,396` in Pass 4 (**`3.23x` more reads completed during compaction**), `ing-2` live flush completed in **`47.01 s`** (`HTTP 204`, `2,349.5 -> 2,592.9 -> 1,093.1 MB RssAnon`, vs `74.27 s` in Pass 4), `6` ULID blocks uploaded to S3 (`201` Go `compactor` runs, `0` failures; `6` blocks loaded by Go `store-gateway`, `0` load/bucket failures).
   - **Stage 3 Chaos:** `20s SIGKILL` of `ing-1` caused **`0` write errors**; `ing-1` replayed its 1.09M-series WAL to `/ready = 200` in **`62,023.5 ms` (`62.02 s`, `11.6 s` faster than Pass 4's `73.60 s`)** and hit `0`-lag (`offset = 533,620 / 533,628`) at **`62,038.7 ms`** (`15.2 ms` after `/ready`!). During `docker restart redpanda` (`1,503 ms`), transient write errors dropped **`2x`** (`64` vs `128` in Pass 4, `0` thereafter), and all 3 ingesters converged to exact lockstep `offset = 606,447`.
   - **Stage 4A/4B/4C Classic gRPC `Push`:** **`0` write errors across all three sub-stages (`270.8M` samples)** — including **`0` write errors at `2.5M` series in Stage 4B** (vs `251` in Pass 4) with `4.5x` lower write p50 (`10.22 ms` vs `46.48 ms`) and `3.7x` lower Stage 4C write p99 (`54.25 ms` vs `199.53 ms` at `3.5M` series / `300.0k samples/s`).

4. **Why End-to-End PromQL Latency Rose in Stage 3 & Stage 4A/4B (`store-gateway` 6-Block S3 Bottleneck):**
   - Because Go `ing-3` survived Stage 2 in Pass 5, all 3 ingesters uploaded 2 blocks each (`6` un-deduplicated 1.3M-series ULID blocks in MinIO `mimir-blocks/prod/`, vs `4` in Pass 4), `go-stack-push.yaml` sets `querier.query_store_after: 0s`, and `start-push-cluster.sh` stops `compactor` without clearing `mimir-blocks/prod/`. Server-side `store-gateway` metrics confirm every PromQL query fanned out to `/gatewaypb.StoreGateway/Series` across all 6 S3 blocks (`3,090 ms` mean + `47` `Unknown` / `33` `Canceled` on `store-gateway`), while Rust `ing-1`/`ing-2` sat at just `0.53–1.00` cores.

---

## 3. Stage-by-Stage Results (Pass 5)

### Stage 1A: 25 Minutes (`1,515 s`) Sustained Kafka `ingest_storage` (RF=3, Partition 0)

- **Writes:** `300,416,116` samples (`198,293.9/s` avg), **`0` write errors**, `p50 = 36.44 ms`, `p95 = 59.80 ms`, `p99 = 64.69 ms`.
- **PromQL Reads:** `40,566` OK, **`0` errors**, `count_checks = 101`, `last_count = 1,000,000`, **`count_mismatch = 0`**.

| Node (4 vCPU / 7.75 GiB) | Engine | Avg Cores | Max Cores | Avg `RssAnon` | Peak `RssAnon` | `QueryStream` Mean | `io_read` / `pgmajfault` | Restarts | Final Offset |
|---|---|---|---|---|---|---|---|---|---|
| `ing-1` (`10.88.0.11`) | Rust `mimir-rs:0bbd40e` | **2.42** | 3.24 | **1,525.9 MB** | **2,850.2 MB** | **68.15 ms** | **0.0 MB / 0** | 0 | 299,999 |
| `ing-2` (`10.88.0.12`) | Rust `mimir-rs:0bbd40e` | **2.09** | 3.07 | **1,475.9 MB** | **2,662.0 MB** | **60.28 ms** | **0.0 MB / 0** | 0 | 300,033 |
| `ing-3` (`10.88.0.13`) | Go `mimir 3.2.6` | **3.39** | 3.95 | **4,710.3 MB** | **6,555.9 MB** | **172.38 ms** | 23.4 MB / 351 | 0 | 300,011 |

| PromQL Endpoint (Stage 1A) | OK / Err | p50 (ms) | p95 (ms) | p99 (ms) | Max (ms) |
|---|---|---|---|---|---|
| `instant_rate` | 5,058 / 0 | 1,661.72 | 3,320.59 | 3,917.12 | 4,768.10 |
| `range_rate` | 5,100 / 0 | 1,764.49 | 3,431.77 | 4,039.53 | 4,999.56 |
| `count_exact` (15s periodic) | 101 / 0 | 4,839.84 | 10,213.77 | 11,422.80 | 12,469.11 |
| `regex_pod` | 5,027 / 0 | 1,569.95 | 3,138.65 | 3,589.09 | 4,485.53 |
| `/api/v1/labels` | 5,026 / 0 | 31.28 | 104.06 | 164.96 | 361.20 |
| `/api/v1/label/namespace/values` | 5,068 / 0 | 25.81 | 108.45 | 175.43 | 328.26 |
| `/api/v1/series` | 5,040 / 0 | 512.13 | 1,172.19 | 1,640.32 | 2,471.32 |
| `/api/v1/metadata` | 5,037 / 0 | 16.54 | 102.01 | 159.73 | 341.51 |
| `/api/v1/cardinality/label_values` | 5,109 / 0 | 28.80 | 146.66 | 234.27 | 625.87 |

#### Go `ing-3` Live CPU & Heap Profile Breakdown (Stage 1A, `1.3M` Series)
- **CPU (`15s`, `57.03s` = `380%` CPU):** `snappy.encodeBlock` (`9.2%`), Go `maps.ctrlGroup.matchH2` + `mapaccess1_fast64` (`22.1%`), `headIndexReader.ShardedPostings` (`14.1%` cum), `labels.Compare` (`6.2%`), Go GC/allocator `spanClass` + `tryDeferToSpanScan` + `memclrNoHeapPointers` (`15.4%`).
- **Heap (`inuse_space = 1,824.25 MB` vs `5,915.4 MB RssAnon` = `3.24x` RSS/live multiplier):** `grpc sizedBufferPool` (`205.3 MB`), `tsdb.newMemSeries` (`199.0 MB`), `NewXORChunk` (`165.0 MB`), `stringslite.Clone` (`158.5 MB`), `activeseries` (`159.2 MB`), postings `appendWithExponentialGrowth` (`135.7 MB`), `bytes.Clone` (`118.6 MB`).

---

### Stage 1B: 10 Minutes (`615.9 s`) Unthrottled 1M-Series `count_exact` Scan Storm

- **Writes:** `120,306,186` samples (`195,328.1/s`), **`0` write errors**, `p50 = 36.56 ms`, `p99 = 65.22 ms`.
- **PromQL Reads:** `2,311` OK / `8` client 60s timeouts (down from `25` in Pass 4), `269` `count_exact` checks, `last_count = 1,000,000`, **`count_mismatch = 0`**.

| Node | Engine | Avg Cores | Avg `RssAnon` | Peak `RssAnon` | Stage-1B `QueryStream` Mean | Cumulative `io_read` | `pgmajfault` | Restarts / OOMs |
|---|---|---|---|---|---|---|---|---|
| `ing-1` | Rust `0bbd40e` | **2.33** | **1,887.2 MB** | **3,049.9 MB** | **404.3 ms** | **0.0 MB** | **0** | **0 / 0** |
| `ing-2` | Rust `0bbd40e` | **1.88** | **1,864.1 MB** | **2,816.3 MB** | **389.3 ms** | **0.0 MB** | **0** | **0 / 0** |
| `ing-3` | Go `3.2.6` | **3.53** | **6,065.5 MB** | **6,596.8 MB** | **2,505.8 ms** | **87,128.1 MB (85.1 GiB)** | **1,259,493** | 0 / 0 (lagged ~4.7k offsets) |

---

### Stage 2: 10 Minutes (`643.8 s`) Live Head Compaction, S3 Block Shipping, & Go Compactor / Store-Gateway Interop

- **Writes:** `90,166,076` samples (`140,057.4/s`), **`0` write errors**, `p50 = 36.14 ms`, `p99 = 63.47 ms`.
- **PromQL Reads:** `4,514` OK (`3.23x` Pass 4's `1,396`), `count_checks = 17`, `last_count = 1,000,000`, **`count_mismatch = 0`**.
- **Live Flush Sawtooth:**
  - `ing-2` (Rust `0bbd40e`): `HTTP 204` in **`47,005.2 ms` (`47.01 s`)**, `2,349.5 -> 2,592.9 -> 1,093.1 MB RssAnon` (`+243.4 MB` sawtooth), `2` ULID blocks shipped, `0` restarts.
  - `ing-1` (Rust `0bbd40e`): compacted `2,076.1 -> 3,147.7 -> 1,144.4 MB RssAnon`, `2` ULID blocks shipped, caught up to `offset = 510,324`, `0` restarts (client HTTP poll timed out at `120s` while shipping finished cleanly).
  - `ing-3` (Go `3.2.6`): `4,048.7 -> 4,692.9 -> 4,271.5 MB RssAnon`, `155.7 GB` cumulative NVMe reads.
- **S3 / Go Compactor / Go Store-Gateway Verification:** `6` ULID blocks + `bucket-index.json.gz` in `mimir-blocks/prod/`; Go `compactor` `runs_completed_total = 201`, `runs_failed_total = 0`; Go `store-gateway` `blocks_loaded = 6`, `block_load_failures_total = 0`, `0` bucket operation failures; post-flush PromQL `count_exact == 1,000,000`.

---

### Stage 3: 10 Minutes (`628.6 s`) Distributed Chaos (`20s SIGKILL ing-1` + `Redpanda` Restart)

- **Chaos Event A (`20s SIGKILL` on Rust `ing-1`):** `0` write errors before and during the 20s outage; `ing-1` replayed its 1.09M-series WAL to `/ready = 200` in **`62,023.5 ms` (`62.02 s`)** and reached `0`-lag (`offset = 533,620` vs peer `533,628`) at **`62,038.7 ms`** (`15.2 ms` after `/ready`).
- **Chaos Event B (`docker restart redpanda`, `1,503.0 ms`):** Only `64` transient distributor write errors during the 1.5s broker restart (`0` errors from `T+20s` through `T+600s`). All 3 ingesters finished in exact lockstep at **`offset = 606,447`** (`1,096,070` series, `count_mismatch = 0`).

---

### Stage 4: 15 Minutes (`5m + 5m + 5m`) Classic gRPC `Push` Mode (RF=3, Snappy, `1M -> 2.5M -> 3.5M` Series)

- **`/ingester/prepare-shutdown` Parity (`ing-1`):** `POST = 204` (`metric = 1`), `GET = 200` (`set`), `DELETE = 204` (`metric = 0`, `/ready = 200`).

| Sub-Stage | Workload | Writes & Latency | `ing-1` (Rust `0bbd40e`) | `ing-2` (Rust `0bbd40e`) | `ing-3` (Go `3.2.6`) |
|---|---|---|---|---|---|
| **Stage 4A** (`328.3s`) | `1,000,000` series @ `300k/s` + `15 req/s` PromQL | `90,075,000` samp (`274.4k/s`), **`0` write err**, `p50=9.63ms`, `p99=136.48ms` | `1.00c` avg, `1,246.2 MB` avg / `1,708.5 MB` peak `RssAnon`, `0` restarts | `0.53c` avg, `1,400.5 MB` avg / `1,829.8 MB` peak `RssAnon`, `0` restarts | `1.28c` avg, `3,479.1 MB` avg / `4,677.3 MB` peak `RssAnon`, `0` restarts |
| **Stage 4B** (`335.9s`) | `2,500,000` series @ `300k/s` + `15 req/s` PromQL | `90,138,000` samp (`268.4k/s`), **`0` write err**, `p50=10.22ms`, `p99=207.30ms` | `1.56c` avg, `2,862.6 MB` avg / `3,982.9 MB` peak `RssAnon`, `0` restarts | `0.85c` avg, `2,396.2 MB` avg / `3,794.9 MB` peak `RssAnon`, `0` restarts | `2.02c` avg, `7,356.3 MB` peak `RssAnon`, **OOM-killed at `7.56 GB`** (`104.1 GiB` NVMe thrash) |
| **Stage 4C** (`302.1s`) | `3,500,000` series @ `300k/s` ceiling | `90,621,000` samp (**`300.0k/s`**), **`0` write err**, `p50=11.76ms`, **`p99=54.25ms`** | `0.77c` avg, `3,832.1 MB` avg / `4,282.8 MB` peak `RssAnon`, `0` restarts | `0.42c` avg, `3,443.2 MB` avg / `3,899.8 MB` peak `RssAnon`, `0` restarts | `1.21c` avg, `7,324.2 MB` peak `RssAnon`, OOM-killed |

---

## 4. Pass 1 -> Pass 2 -> Pass 3 -> Pass 4 -> Pass 5 (`0bbd40e`) Progression

| Stage | Pass 1 (`f1f680d`) | Pass 2 (`08370b3`) | Pass 3 (`73b262d`) | Pass 4 (`13b2226`) | **Pass 5 (`0bbd40e`)** | Go Mimir `3.2.6` (Pass 5) |
|---|---|---|---|---|---|---|
| **1A: Sustained Kafka** (25m, 1.3M series) | 120s only | 60s only | 0 err, 2.55c, 1.49 GB | 0 err, 37,983 q, 2.35–2.82c, 1.57–1.61 GB, `cnt_p99=48.7s` | **`0` err, `40,566` q, `2.09–2.42c`, `1.48–1.53 GB`, `cnt_p99=11.4s`, `QS=60–68ms`, `0` majfault** | `3.39c`, `4.71 GB` avg / `6.56 GB` peak, `QS=172ms` |
| **1B: Unthrottled 1M Scan** (10m) | 7.58 GB OOM | 120s OK | 0 OOMs, 2.25c, 1.80 GB | 0 OOMs, 1,859 q (198 cnt), 2.12–2.40c, 1.89–1.97 GB | **`0` OOMs, `2,311` q (`269` cnt, `+24%`), `1.88–2.33c`, `1.86–1.89 GB`, `QS=389–404ms`, `0` disk read** | `3.53c`, `6.07 GB` avg, `QS=2,506ms`, **`85.1 GiB` NVMe thrash (`1.26M` majfault)** |
| **2: Live Flush + S3** (10m) | `ing-2` OOM | 14–16s (small) | `ing-1` deadlock | 58.7s / 74.3s, 1,396 q, 4 blocks | **`47.0s` (`ing-2`), `4,514` q (`3.23x`), `6` S3 blocks, `201` compactor / `6` store-gw OK** | `4.85 GB` avg, **`152.1 GiB` NVMe thrash (`2.20M` majfault)** |
| **3: Chaos** (10m) | `ing-2` OOM | `/ready` 7.8s (small) | Redpanda wedge | `/ready` 73.6s, 128 broker err | **`0` kill err, `/ready` `62.02s`, `0`-lag `62.04s`, `64` broker err, lockstep `606,447`** | Survived (after Stage 2 near-OOM) |
| **4A: Push 1.0M** (5m) | 134.9k/s, 237 err | 267.4k/s, 75 err | 281.7k/s, 0 err | 285.8k/s, 0 err, 1.94–3.00c | **`274.4k/s`, `0` err, `p50=9.63ms`, `0.53–1.00c`, `1.25–1.40 GB`** | `1.28c`, `3.48 GB` avg / `4.68 GB` peak |
| **4B: Push 2.5M** (5m) | 161.9k/s, 939 err | 205.3k/s | 291.5k/s, 0 rst | 292.0k/s, 251 w_err, `p50=46.5ms` | **`268.4k/s`, `0` write err, `p50=10.22ms` (`4.5x` faster), `0.85–1.56c`, `2.40–2.86 GB`, `0` rst** | **`7.36 GB` peak, OOM-killed at `7.56 GB` (`104.1 GiB` NVMe thrash)** |
| **4C: Push 3.5M** (5m) | Go VM lockup | 0.65c, 4.63 GB | 300.0k/s, 0.53c | 300.0k/s, `p99=199.5ms`, 0.56–0.73c | **`300.0k/s`, `0` err, `p50=11.76ms`, `p99=54.25ms` (`3.7x` lower), `0.42–0.77c`, `3.44–3.83 GB`, `0` rst** | `7.32 GB` peak, OOM-killed |

---

## 5. Teardown

All temporary Hetzner Cloud resources carry label `cluster=mimir-stress` and were torn down automatically via `./scripts/hetzner-teardown.sh` (`0` servers, networks, firewalls, or SSH keys remaining).
