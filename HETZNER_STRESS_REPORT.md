# Hetzner Cloud Mixed Go + Rust Mimir Cluster Stress Test Report

**Date:** 2026-11-16
**Images Tested:**
- **Rust (`mimir-rs`)**: `ghcr.io/birdayz/mimir-rs:f1f680d` (`revision=f1f680d57d286e624a32af07bdcfebbc4e2f40bb`)
- **Upstream Go Mimir**: `docker.io/grafana/mimir:latest` (`version=3.2.5`, `revision=46f5839c`, `go1.26.10`)
- **Kafka Broker**: `docker.io/redpandadata/redpanda:latest`
- **Object Storage (S3)**: `docker.io/bitnamilegacy/minio:latest`

---

## 1. Infrastructure & Topology (`cluster=mimir-stress`, `fsn1`)

### Hardware Allocation Note (Hetzner EU Stockout Fallback)
At provisioning time, all Hetzner Cloud ARM64 (`cax11`, `cax21`, `cax31`, `cax41`) server types reported `available: false` (`resource_unavailable` across `fsn1`, `nbg1`, `hel1`). To preserve the exact **4 vCPU / 8 GB RAM** ingester constraint and **16-core** stack/loadgen envelope, we provisioned matching `x86_64` nodes in `fsn1` (`ubuntu-24.04`) on private network `mimir-stress-net` (`10.88.0.0/16`, subnet `10.88.0.0/24`):

| Hostname | Role | Zone / Node | Server Type | vCPU / RAM | Public IPv4 / IPv6 | Private IP | Software |
|---|---|---|---|---|---|---|---|
| `mimir-stress-ing-1` | Ingester 1 | `zone-a` | `cpx32` | 4 vCPU (AMD EPYC-Genoa) / 7.6 GiB | `188.245.63.154` / `2a01:4f8:c016:2b51::1` | `10.88.0.11` | `mimir-rs:f1f680d` |
| `mimir-stress-ing-2` | Ingester 2 | `zone-b` | `cpx32` | 4 vCPU (AMD EPYC-Genoa) / 7.6 GiB | `188.245.55.21` / `2a01:4f8:c016:56e1::1` | `10.88.0.12` | `mimir-rs:f1f680d` |
| `mimir-stress-ing-3` | Ingester 3 | `zone-c` | `cpx32` | 4 vCPU (AMD EPYC-Genoa) / 7.6 GiB | `188.245.58.216` / `2a01:4f8:c016:fb72::1` | `10.88.0.13` | `grafana/mimir:3.2.5` |
| `mimir-stress-go-stack` | Go Stack + Redpanda + MinIO | — | `ccx43` | 16 dedicated vCPU (AMD EPYC-Milan) / 61 GiB | `2a01:4f8:c016:762d::1` | `10.88.0.20` | Go Mimir (`distributor,query-frontend`, `query-scheduler`, `querier`, `store-gateway`, `compactor`), Redpanda (`:9092`), MinIO (`:9000`) |
| `mimir-stress-loadgen` | Load + PromQL Verifier | — | `ccx43` | 16 dedicated vCPU (AMD EPYC-Milan) / 61 GiB | `2a01:4f8:c016:436e::1` | `10.88.0.30` | Custom Go `stress-loadgen` (Snappy protobuf `WriteRequest` + 9-endpoint PromQL verifier + 5s `/metrics` scraper) |

All Mimir services communicate over `10.88.0.0/24` via live TCP `memberlist` gossip (`7941..7947`). Pre-existing Hetzner servers (`sommer2026`, `gh-runner-drain-0`, `gh-runner-fdb`), `ci-fleet`, and pre-existing firewalls/SSH keys were untouched.

---

## 2. Executive Summary of Findings

1. **Drop-In Mixed-Cluster Wire, Ring, Kafka, WAL, & S3 Interop Verified:**
   - **Memberlist + Partition Ring + Classic Ring:** Go `distributor`, `querier`, `store-gateway`, `compactor`, and `query-scheduler` cleanly converged with both `mimir-rs` ingesters (`ingester-zone-a-0`, `ingester-zone-b-0`) and the Go ingester (`ingester-zone-c-0`), all `ACTIVE` in `/ingester/ring` and sharing Partition `0` (`ACTIVE`) in `/ingester/partition-ring`.
   - **Kafka `ingest_storage` Parity:** Under 1,000,000 active 10-label series + metadata + 0.1% OOO + churn, all 3 ingesters consumed identical Kafka offsets (`offset = 61,563` at end of Stage 1, `memory_series = 1,011,400` on all 3).
   - **S3 Compaction & Store-Gateway Interop:** Go `compactor` opened, merged, and deduplicated Rust-uploaded + Go-uploaded TSDB blocks in MinIO (`mimir-blocks/prod/`) into level-3 compacted block `01M7JN1YNG1K57GCD4XEKQRB8J` (1,011,400 series, 62,468,400 samples, 695 MB) with **0 errors**, and Go `store-gateway` loaded all 6 blocks (`cortex_bucket_store_block_load_failures_total = 0`).
   - **Crash Recovery:** After `docker kill -s KILL ingester` on `ing-1` (Rust) at >1M active series under active `160k samples/s` load, `ing-1` replayed WAL to `/ready == 200` in **6.41s** and caught up to 0 Kafka lag in **9.97s**, with **0 distributor write errors** during the outage.

2. **Massive Head Memory Advantage for `mimir-rs` (`~2.2×` Less RAM at Scale):**
   - At rest / write-steady **1,011,400 active series** (post-Stage 1): Rust `ing-1` used **`1,817 MB` `RssAnon`** (`1.81 GB`) vs Go `ing-3`'s **`2,592 MB` `RssAnon`** (`2.59 GB`).
   - In Classic gRPC `Push` mode at **1,000,000 active series**: Rust `ing-1`/`ing-2` peaked at **`1,242–1,316 MB` `RssAnon`** vs Go `ing-3`'s **`2,878 MB` `RssAnon`** (**`2.25×` lower RAM**).
   - At **2,500,000 active series**: Rust `ing-1`/`ing-2` peaked at **`3,085–3,298 MB` `RssAnon`** vs Go `ing-3`'s **`6,093 MB` `RssAnon`** (`6.46 GB` `process_resident_memory_bytes`).
   - At **3,500,000 active series** (Stage 4C overdrive): Rust `ing-1`/`ing-2` held steady at **`3.43 GB` / `3.51 GB` `peak_RssAnon`** (`~4.2 GB` free RAM, `0` restarts, `<10%` CPU), while Go `ing-3` climbed to **`7,715 MB` (`7.72 GB` RSS)** and locked up the 7.6 GiB VM under OOM thrashing.

3. **Two Real Production Bottlenecks Uncovered in `mimir-rs:f1f680d`:**
   - **Finding A (`QueryStream` Full-Response Materialization OOM):** In `crates/mimir-ingester/src/query.rs` (`query_stream_inner`), `mimir-rs` builds the entire `Vec<QueryStreamResponse>` in memory before streaming (`chunk_series` + protobuf `QueryStreamSeriesChunks`). A single unsharded `count({__name__="container_cpu_usage_seconds_total"})` query across 1,000,000 series spikes RSS by ~1.3–1.5 GB; when >3 unsharded 1M-series scans overlap concurrently at `30 req/s`, Rust `ing-1`/`ing-2` hit `7.58 GB` `anon-rss` and OOM-killed (while Go `ing-3` streamed incrementally at `~3.5 GB`).
   - **Finding B (Classic gRPC `Push` CPU Saturation):** In Classic gRPC `Push` mode (`Stage 4A`/`4B`), `mimir-rs` saturated all **4.00 CPU cores** at ~`135k–200k samples/s` (causing `237` / `939` `context deadline exceeded` 2s distributor timeouts), whereas Go `ing-3` used only **1.01–1.09 cores** (`~27%` host CPU) for the same replicated `RF=3` write rate. In Kafka `ingest_storage` mode without unsharded 1M-series scans, throughput sustained **`200,000 samples/s` with `0` write errors** (`p99 = 55.22 ms`).

---

## 3. Stage-by-Stage Results

### Stage 1: Mixed Kafka `ingest_storage` Architecture (`RF=3`, Zone-Aware Partition 0)

- **Configuration:** Redpanda topic `mimir-ingest` (1 partition), `ingester-zone-a-0` (Rust), `ingester-zone-b-0` (Rust), `ingester-zone-c-0` (Go) all owning Partition `0` (`ACTIVE`).
- **Workload:** 1,000,000 active 10-label K8s series (`container_cpu_usage_seconds_total`), `200,000 samples/s` target, `0.1%` OOO (`-5m` within `30m` window), `200 series/s` churn (`k8s_pod_churn_metric`), `MetricMetadata`, plus `30 req/s` concurrent PromQL across 9 endpoints.

#### Initial Run Observation (Unsharded 1M-Series `count` at `~3.3 req/s`)
When `count({__name__="container_cpu_usage_seconds_total"})` was included in the 30 req/s round-robin (~3.3 concurrent 1M-series full-head `QueryStream` scans/s), **225.1s** run wrote **36,063,000 samples** (`0` write errors, `p50=29.41ms`, `p99=55.67ms`) and converged to `count == 1,000,000`, but both `ing-1` (`16:55:55`, `anon-rss: 7,580,644 kB`) and `ing-2` (`16:56:53`, `anon-rss: 7,579,656 kB`) hit Linux OOM-kill due to concurrent `QueryStream` buffer materialization. Go `ing-3` had `0` OOMs (`~3.84 GB` host used).

#### Sustained 120s Run (10s `count_exact` Verifier + `30 req/s` Mixed PromQL)
- **Writes:** **24,053,000 samples** (`200k/s` steady state, `172,262/s` incl. 19.6s template pre-build), **`24,053` OK, `0` errors**, `p50 = 30.30 ms`, `p95 = 53.96 ms`, `p99 = 55.22 ms`.
- **OOO & Churn:** `23,972` OOO samples, `7,600` churn series, `1,011,400` total active series on all 3 ingesters (`offset = 61,563` on `ing-1`, `ing-2`, `ing-3`).
- **PromQL Reads:** **`372` OK, `0` errors** across all 9 endpoints; `count_checks = 6`, `last_count = 1,000,000`, **`mismatch = 0`**.

| PromQL Endpoint | OK / Err | p50 (ms) | p95 (ms) | p99 (ms) |
|---|---|---|---|---|
| `instant_rate` (`sum(rate(...{namespace="ns-3"}[5m]))`) | 47 / 0 | 5,154.48 | 9,750.00 | 11,285.23 |
| `range_rate` (10m range, `30s` step) | 47 / 0 | 9,838.81 | 15,793.04 | 16,376.04 |
| `count_exact` (`count({__name__="container_cpu_usage_seconds_total"})`) | 6 / 0 | 22,893.32 | 27,559.03 | 27,559.03 |
| `regex_pod` (`count({pod=~"pod-12.*"})`) | 46 / 0 | 3,573.97 | 7,495.71 | 8,590.54 |
| `/api/v1/labels` | 45 / 0 | 50.28 | 635.99 | 809.75 |
| `/api/v1/label/namespace/values` | 44 / 0 | 205.37 | 714.69 | 1,488.26 |
| `/api/v1/series` | 45 / 0 | 149.13 | 997.51 | 1,900.32 |
| `/api/v1/metadata` | 46 / 0 | 106.16 | 618.39 | 1,299.62 |
| `/api/v1/cardinality/label_values` | 46 / 0 | 690.55 | 1,881.17 | 3,010.01 |

| Node (4 vCPU / 8 GB) | Engine | Avg CPU % (cg cores) | Max CPU % | Avg `RssAnon` | Peak `RssAnon` | Peak `VmRSS` | Restarts (during run) |
|---|---|---|---|---|---|---|---|
| `ing-1` (`10.88.0.11`) | Rust `mimir-rs` | 75.26% (3.09c) | 100.00% | 4,129.6 MB | 6,883.7 MB | 6,900.9 MB | 0 |
| `ing-2` (`10.88.0.12`) | Rust `mimir-rs` | 91.93% (3.82c) | 100.00% | 3,553.5 MB | 5,655.6 MB | 5,672.2 MB | 0 |
| `ing-3` (`10.88.0.13`) | Go `mimir 3.2.5` | 26.76% (1.08c) | 42.17% | 3,097.3 MB | 3,507.5 MB | 3,586.6 MB | 0 |

*(Idle post-Stage-1 snapshot at `1,011,400` series: `ing-1` Rust `RssAnon = 1,817 MB`, `ing-2` Rust `RssAnon = 2,251 MB`, `ing-3` Go `RssAnon = 2,592 MB`.)*

---

### Stage 2: Live Compaction + MinIO S3 Shipping + Go `store-gateway` & `compactor` Interop

- **Workload:** `150k samples/s` active writes (`13,540,000` samples, `0` write errors, `p50=32.17ms`, `p99=55.26ms`) + `20 req/s` PromQL (`181` OK, `3` errors during `ing-2` OOM, `count=1,000,000` `mismatch=0`).
- **Live Flush (`POST /ingester/flush?wait=true` under active load):**
  - `ing-3` (Go): **HTTP 204 in 19,412 ms** (`peak_RssAnon = 3,909.0 MB`, `peak_VmRSS = 4,614.2 MB`, `avg_cpu = 39.35%`).
  - `ing-1` (Rust): **HTTP 204 in 26,588 ms** (sawtooth `2,563 MB -> 5,835 MB -> 850 MB` post-compaction; later hit `7,564 MB` OOM at `17:05:36` under overlapping query load).
  - `ing-2` (Rust): **HTTP 000 at 29,309 ms** (OOM-killed at `17:04:27`, `anon-rss: 7,586,172 kB`, from concurrent head compaction + active 1M-series `QueryStream` queries; restarted cleanly).
- **MinIO S3 & Go `compactor` / `store-gateway` Verification:**
  - Uploaded Level-1 TSDB blocks in `mimir-blocks/prod/`:
    - `01M7JN0YGXB92G6TVTVCAYW8DE` (`1,011,400` series, `62,402,490` samples)
    - `01M7JN10KCND9D9RMN3FXQHTF9` (`1,011,400` series, `62,407,187` samples)
    - `01M7JN1F43DMADJ3FDX14YAWHE` (`59,395` series, `61,213` OOO samples)
    - `01M7JN1FRGDP5VE7CPDRV99XFZ` (`59,392` series, `61,210` OOO samples)
  - Go `compactor` compacted and deduplicated all 4 blocks into **Level-3 block `01M7JN1YNG1K57GCD4XEKQRB8J`** (`1,011,400` series, `62,468,400` samples, `695,110,255` bytes) in **`14.70 s`**, with `cortex_compactor_runs_completed_total = 29` and **`cortex_compactor_runs_failed_total = 0`**.
  - Go `store-gateway` (with `-blocks-storage.bucket-store.ignore-blocks-within=0s`) loaded **`cortex_bucket_store_blocks_loaded = 6`** (`8.82 MB` index/metadata cache), **`cortex_bucket_store_block_load_failures_total = 0`**, **`thanos_objstore_bucket_operation_failures_total = 0`**, and served PromQL `count({__name__="container_cpu_usage_seconds_total"}) = 1,000,000`.

---

### Stage 3: Hardcore Chaos (`SIGKILL` Crash + Redpanda Broker Restart Under Load)

- **Workload:** `160k samples/s` (`28,263,000` samples total, `avg 148,098/s`, `p50=37.81ms`, `p99=69.73ms`) + `25 req/s` PromQL (`294` OK, `17` errors across chaos transitions).
- **Chaos Event 1 (`T+20s..T+35s`): `docker kill -s KILL ingester` on `ing-1` (Rust `zone-a`)**:
  - **Write Availability:** **`0` distributor write errors** (`write_err` stayed `0` throughout the entire 15s `ing-1` outage; `ing-2` Rust + `ing-3` Go maintained `RF=2/3` quorum).
  - **WAL Replay & Kafka Catch-Up (`T+35s` `docker start ingester`):**
    - `/ready == 200` in **`6,409 ms` (6.41 s)** replaying `1,002,900` series.
    - **0-lag Kafka catch-up** (`ing-1` `offset=82,483` vs `ing-3` `offset=82,491`; `memory_series = 1,002,900` matching `ing-3`) in **`9,974 ms` (9.97 s)**.
- **Chaos Event 2 (`T+~50s`): `docker restart redpanda` (`834 ms` broker restart)**:
  - **79 transient write errors** during the sub-second broker outage (`48` in `45..50s`, `31` in `50..55s`), immediately returning to **`0` new write errors** (`write_err = 79` flat from `55s` to `180s`).
  - Both Rust (`ing-1`, `ing-2`) and Go (`ing-3`) Kafka readers reconnected automatically with **0 wedged readers**, advancing in lockstep to `offset = 102,663` (`1,005,800` series on `ing-1` & `ing-3`; `ing-2` had 1 early OOM at `t=15s` and held `1,011,400` series cleanly thereafter).

| Node | Engine | Avg CPU % | Max CPU % | Avg `RssAnon` | Peak `RssAnon` | Peak `VmRSS` |
|---|---|---|---|---|---|---|
| `ing-1` | Rust `mimir-rs` | 54.16% | 100.00% | 3,065.9 MB | 6,769.8 MB | 7,092.7 MB |
| `ing-2` | Rust `mimir-rs` | 67.30% | 100.00% | 4,296.5 MB | 6,484.6 MB | 6,501.0 MB |
| `ing-3` | Go `mimir 3.2.5` | 25.54% | 56.13% | 3,370.4 MB | 3,706.3 MB | 4,411.6 MB |

---

### Stage 4: Classic gRPC `Push` Mode (`ingest_storage.enabled: false`, `RF=3`) & Overdrive Ceiling

All nodes switched to `ingest_storage.enabled: false`, `push_grpc_method_enabled: true`, `replication_factor: 3`, `zone_awareness_enabled: true` (`cortex_distributor_ingest_storage_enabled = 0`), with clean TSDB directories.

#### Stage 4A: 1,000,000 Active Series @ 300k/s Target + 15 req/s PromQL (60s)
- **Throughput & Latency:** `8,719,500` samples (`avg 134,888/s`, peak `224,400/s`), `writeOK = 5,813`, `writeErr = 237` (2s Go distributor push timeouts to `ing-1`/`ing-2` under 4-core CPU saturation), `p50 = 653.67 ms`, `p99 = 1,926.87 ms`.
- **PromQL:** `398` OK, **`0` errors**, post-run `count({__name__="container_cpu_usage_seconds_total"}) = 1,000,000`.
- **Resource Comparison (1M series, Classic gRPC Push):**
  - `ing-1` (Rust): `avg_cg_cores = 4.00` (93.91% host CPU), **`avg_RssAnon = 1,055.4 MB`**, **`peak_RssAnon = 1,316.5 MB`** (`0` restarts)
  - `ing-2` (Rust): `avg_cg_cores = 3.98` (94.86% host CPU), **`avg_RssAnon = 1,021.2 MB`**, **`peak_RssAnon = 1,242.5 MB`** (`0` restarts)
  - `ing-3` (Go): `avg_cg_cores = 1.01` (27.13% host CPU), **`avg_RssAnon = 2,427.4 MB`**, **`peak_RssAnon = 2,877.9 MB`** (`0` restarts)

#### Stage 4B: Overdrive Ramp to 2,500,000 Active Series (180s)
- **Throughput & Latency:** `30,386,000` samples (`avg 161,943/s`, peak `288,000/s`), `writeOK = 15,193`, `writeErr = 939` (4-core push saturation timeouts on `ing-1`/`ing-2`), `Query = 388 OK / 0 errors`.
- **Resource Comparison (2.5M series):**
  - `ing-1` (Rust): `avg_cg_cores = 3.93` (96.99% CPU), **`peak_RssAnon = 3,085.4 MB`** (`3.10 GB` `VmRSS`), `0` restarts
  - `ing-2` (Rust): `avg_cg_cores = 3.92` (96.61% CPU), **`peak_RssAnon = 3,297.9 MB`** (`3.32 GB` `VmRSS`), `0` restarts
  - `ing-3` (Go): `avg_cg_cores = 1.09` (27.76% CPU), **`peak_RssAnon = 6,093.3 MB`** (`6.46 GB` `process_resident_memory_bytes`), `0` restarts

#### Stage 4C: Push-to-the-Wall Overdrive — 3,500,000 Active Series (Breaking Point on 8 GB Nodes)
- **Workload:** Ramped to `3,500,000` active 10-label series (`13,575` write batches = `27,150,000` samples, `0` write errors before `ing-3` stalled).
- **Exact Breaking Point:**
  - **`ing-1` (Rust, `3,500,000` series):** **`peak_RssAnon = 3,432.9 MB` (`3.43 GB`)**, `avg_cpu = 9.71%` (`0.36` cores post-ramp), `RestartCount = 0`, **healthy**.
  - **`ing-2` (Rust, `3,500,000` series):** **`peak_RssAnon = 3,506.5 MB` (`3.51 GB`)**, `avg_cpu = 9.50%` (`0.35` cores post-ramp), `RestartCount = 0`, **healthy**.
  - **`ing-3` (Go, `3,500,000` series):** Reached **`process_resident_memory_bytes = 7,715,028,992` (`7.72 GB` RSS)** out of `7.6 GiB` physical RAM, exhausting host memory and locking up SSH/kernel under OOM thrashing (`hcloud server reboot mimir-stress-ing-3` required; afterward TSDB was reset and all 3 ingesters returned to `/ready == 200` and `ACTIVE`).

---

## 4. Side-by-Side Scaling Summary (4 vCPU / 8 GB Ingester Nodes)

| Active Series (10 labels) | `mimir-rs` `peak_RssAnon` (`ing-1` / `ing-2`) | Go Mimir `peak_RssAnon` / RSS (`ing-3`) | Rust RAM Savings |
|---|---|---|---|
| **1,000,000** (Classic Push, 4A) | **1.24 – 1.32 GB** | 2.88 GB | **−55% (2.25× less RAM)** |
| **1,011,400** (Idle post-Kafka, S1) | **1.82 – 2.25 GB** | 2.59 GB | **−13% to −30%** |
| **2,500,000** (Classic Push, 4B) | **3.09 – 3.30 GB** | 6.09 GB (`6.46 GB` RSS) | **−49% (1.96× less RAM)** |
| **3,500,000** (Classic Push, 4C) | **3.43 – 3.51 GB** (healthy, 4+ GB free) | **7.72 GB RSS -> VM OOM lockup** | **−55% (2.25× less RAM, survives 3.5M+)** |

---

## 5. Pass 2 Results (`mimir-rs:08370b3` / `56099c3` — Post-Fix Verification)

After Pass 1, three performance/memory bottlenecks and one live-flush lock inversion were fixed:
1. **Removed unconditional `.send_compressed(CompressionEncoding::Gzip)`** on `IngesterService` & `HttpServer` (`crates/mimir-cmd/src/app.rs`), added transparent `grpc-encoding: snappy` frame decompression (`crates/mimir-ingester/src/grpc.rs`), and scaled `mimir-rpc` worker threads via `available_parallelism().clamp(4, 16)`.
2. **True Lazy-Chunk `QueryStream`** (`crates/mimir-tsdb/src/{head,querier}.rs`, `crates/mimir-ingester/src/query.rs`): Phase 1 computes exact `chunk_count` and streams 128-series `QueryStreamSeries` batches with zero chunk bytes cloned; Phase 2 loads one series at a time into bounded `mpsc::channel(4)` 256-series / 1 MiB batches.
3. **Prehashed Classic gRPC `Push`** (`crates/mimir-ingester/src/push.rs`, `crates/mimir-proto/src/zerocopy.rs`): offloads non-trivial payloads (`>= 2 KiB`) to `spawn_blocking` and encodes labels directly into `stringlabels` + `xxh64` to reuse `ingest_prehashed_shard_batch_inner`.
4. **Non-blocking `blocks_lock` on `mimir-rpc`** (`crates/mimir-tsdb/src/querier.rs`, `crates/mimir-ingester/src/query.rs`): added `try_select_adaptive` and offloaded label/search queries to `spawn_blocking` so `mimir-rpc` async reactor threads never stall behind `blocks.write()` during live head compaction.

### Pass 1 vs Pass 2 Comparison (`mimir-rs` `ing-1` / `ing-2` vs Go `ing-3`)

| Benchmark | Pass 1 (`mimir-rs:f1f680d`) | Pass 2 (`mimir-rs:56099c3` / `08370b3`) | Go Mimir `3.2.5` (`ing-3`) |
|---|---|---|---|
| **Stage 1 Unthrottled** (`30 req/s` incl. `count_exact` 1M scans @ `200k/s` writes) | **7.58 GB OOM-kill** (`ing-1` & `ing-2`) | **0 OOMs**, `peak_RssAnon` **1.27–1.28 GB** (**6.0x less RAM**), `count_exact` p50 **4.96 s** | `peak_RssAnon` **2.80 GB** (**2.2x more RAM** than Rust) |
| **Stage 1B Clean** (`30 req/s` + 10s `count_exact` @ `200k/s` writes) | `372` queries OK; `range_rate` p50 `9,839 ms`; `instant_rate` p50 `5,154 ms` | **`1,673` queries OK (`0` err)**; `range_rate` p50 **245 ms (`40.1x` faster)**; `instant_rate` p50 **233 ms (`22.1x` faster)**; `labels` p50 **3.22 ms** | `peak_RssAnon` **3.35 GB** vs Rust **1.47–1.48 GB** (**2.28x less RAM**) |
| **Stage 2 Live Flush + S3** (`150k/s` writes + `20 req/s` reads) | `ing-2` **OOM-killed** (`7.59 GB`), `ing-1` `26.6 s` | **0 OOMs**, `ing-1` **14.18 s** (`HTTP 204`), `ing-2` **15.68 s** (`HTTP 204`), `0` compactor/store-gw errors | `14.56 s` (`HTTP 204`), `peak_RssAnon` **3.23 GB** vs Rust **2.49–3.42 GB** |
| **Stage 3 Chaos** (`SIGKILL ing-1` + Redpanda restart @ `160k/s`) | `ing-2` early OOM; `ing-1` `peak_RssAnon = 6.77 GB` | **0 OOMs (`0` restarts)**; `ing-1` `/ready` in **7.76 s**, 0-lag in **11.88 s**; `0` write err during outage | `peak_RssAnon` **4.58 GB** vs Rust **3.03–3.48 GB** |
| **Stage 4A Classic Push** (`1M` series @ `300k/s` + `15 req/s` reads) | `avg 134.9k/s`, `4.00` cores (100% sat), `237` write timeouts | **`avg 267.4k/s` (`2.0x`)**, `0` errors post-ramp, **`3.07–3.09` cores** | `1.64` cores, `peak_RssAnon` **2.67 GB** vs Rust **1.36–1.67 GB** |
| **Stage 4B Classic Push** (`2.5M` series @ `300k/s` + `15 req/s` reads) | `avg 161.9k/s`, `939` write timeouts | **`avg 205.3k/s`** (`300k+/s` steady), `0` errors post-ramp | `peak_RssAnon` **7.31 GB** vs Rust **4.05–4.29 GB** |
| **Stage 4C Classic Push Overdrive** (`3.5M` series @ `300k/s` writes) | Go VM OOM lockup | **`0` write errors, `p50=11.51 ms`**, **`0.65–0.66` cores (16.4% CPU)**, `peak_RssAnon` **4.62–4.63 GB** | **`0.93` cores (`+41%` more CPU than Rust!)**, **`7.68 GB` RSS (`+3.05 GB` more RAM)** |

### Detailed Pass 2 Stage Metrics

#### Stage 1 (Unthrottled 1M-Series `count_exact` Killer Workload, 120s @ 200k/s, `mimir-rs:56099c3` & 60s @ 200k/s, `mimir-rs:08370b3`)
- **120s Unthrottled (`56099c3`):** `24,060,000` samples (`200k/s` steady), **`0` write errors** (`p50=31.26 ms`, `p99=59.08 ms`), `753` PromQL OK / `3` `429` (`query-scheduler` queue cap during 95 concurrent 1M-series `count_exact` scans), `count_checks=95`, `last_count=1,000,000`, `mismatch=0`.
  - `ing-1` (Rust): `0` restarts, `avg_cores=3.41 (85.3%)`, `avg_RssAnon=1,083.7 MB`, **`peak_RssAnon=1,269.1 MB`**
  - `ing-2` (Rust): `0` restarts, `avg_cores=3.33 (83.2%)`, `avg_RssAnon=1,088.4 MB`, **`peak_RssAnon=1,276.6 MB`**
  - `ing-3` (Go): `0` restarts, `avg_cores=2.24 (56.1%)`, `avg_RssAnon=2,374.6 MB`, **`peak_RssAnon=2,804.9 MB`**
- **60s Clean (`56099c3`, `-unthrottled-count=false`):** `12,061,000` samples (`199,963/s`), `0` write errors, **`1,673` PromQL OK, `0` errors** (`p50=55.59 ms`, `instant_rate p50=233.31 ms`, `range_rate p50=245.32 ms`, `count_exact p50=3,074.74 ms`, `labels p50=3.22 ms`, `label_values p50=3.97 ms`, `metadata p50=2.67 ms`).
- **60s Unthrottled (`08370b3`):** `12,061,000` samples (`200k/s`), `0` write errors (`p50=30.60 ms`, `p99=58.21 ms`), `435` PromQL OK (`56` `count_exact` scans, `mismatch=0`).

#### Stage 2 (Live Flush + MinIO S3 + Go Compactor & Store-Gateway, `mimir-rs:08370b3`)
- **Active Load:** `13,557,000` samples (`150k/s` steady, `0` write errors, `p50=32.77 ms`, `p99=56.78 ms`), `1,494` PromQL OK (`0` query errors, `count=1,000,000`, `mismatch=0`).
- **Live Flush (`POST /ingester/flush?wait=true` under load):**
  - `ing-1` (Rust): **`HTTP 204` in `14,183 ms`**, `0` restarts, `avg_RssAnon=1,465.0 MB`, `peak_RssAnon=3,419.0 MB`
  - `ing-2` (Rust): **`HTTP 204` in `15,683 ms`**, `0` restarts, `avg_RssAnon=1,368.7 MB`, `peak_RssAnon=2,489.0 MB`
  - `ing-3` (Go): **`HTTP 204` in `14,560 ms`**, `0` restarts, `avg_RssAnon=2,728.4 MB`, `peak_RssAnon=3,231.3 MB`
- **Compactor & Store-Gateway:** `cortex_compactor_runs_completed_total = 8`, `cortex_compactor_runs_failed_total = 0`, `cortex_bucket_store_block_load_failures_total = 0`, `thanos_objstore_bucket_operation_failures_total = 0`.

#### Stage 3 (Hardcore Chaos: `SIGKILL ing-1` + Redpanda Restart, `mimir-rs:08370b3`)
- `19,099,000` samples (`158,140/s` avg), **`0` write errors during `ing-1` 15s outage**, `64` transient write errors during `1,679 ms` Redpanda restart (`0` after recovery), `1,433` PromQL OK / `9` transient errors during `ing-1` kill, `count=1,000,000`, `mismatch=0`.
- `ing-1` WAL replay to `/ready == 200` in **7.76 s** (`7,756 ms`), 0-lag Kafka catch-up (`offset = 36,586`) in **11.88 s** (`11,876 ms`).
- **0 OOMs (`restarts=0`)** across all 3 nodes (`ing-1` `peak_RssAnon=3,483.2 MB`, `ing-2` `peak_RssAnon=3,029.7 MB`, `ing-3` `peak_RssAnon=4,575.3 MB`), all converging at `offset = 43,919` (`1,005,800` series).

#### Stage 4 (Classic gRPC `Push` Mode, `mimir-rs:08370b3`)
- **Stage 4A (`1M` series @ `300k/s` + `15 req/s` PromQL):** `16,224,000` samples (`avg 267,421/s`), `0` errors after first 10s cold-create burst (`writeErr=75` flat), `p50=143.34 ms`, `p99=971.13 ms`, `537` PromQL OK (`last_count=1,000,000`, `mismatch=0`). `ing-1`/`ing-2` `avg_cores=3.07–3.09`, `peak_RssAnon=1,355.7–1,666.1 MB` vs `ing-3` `2,673.8 MB`.
- **Stage 4B (`2.5M` series @ `300k/s` + `15 req/s` PromQL):** `43,676,000` samples (`avg 205,296/s`, `300k+/s` steady), `0` write errors after 45s cold-create burst, `p50=143.72 ms`. `ing-1`/`ing-2` `peak_RssAnon=4,052.2–4,289.9 MB` vs `ing-3` `7,305.7 MB`.
- **Stage 4C (`3.5M` series @ `300k/s` writes):** `48,934,000` samples (`24,467` OK, **`0` write errors**, `p50=11.51 ms`, `p95=16.88 ms`).
  - `ing-1` (Rust): **`avg_cores=0.66 (16.5%)`**, **`peak_RssAnon=4,615.4 MB`** (`0` restarts)
  - `ing-2` (Rust): **`avg_cores=0.65 (16.4%)`**, **`peak_RssAnon=4,629.6 MB`** (`0` restarts)
  - `ing-3` (Go): **`avg_cores=0.93 (23.2%)`**, **`VmRSS = 7,679.6 MB` (`7.68 GB` RSS)**

---

## 6. Teardown

All created Hetzner Cloud resources are labeled `cluster=mimir-stress`. To delete ONLY the stress cluster (`5` servers, `mimir-stress-fw`, `mimir-stress-net`, `mimir-stress-key`) without touching any pre-existing infrastructure:

```bash
./scripts/hetzner-teardown.sh
```
