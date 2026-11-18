# Hetzner Cloud Mixed Go + Rust Mimir Cluster Stress Report (Pass 4)

**Date:** 2026-11-18
**Total Active Benchmark Duration:** 71.53 minutes (`4,291.7 s` across Stages 1A, 1B, 2, 3, 4A, 4B, 4C; total written samples: **879,299,053**)
**Images Tested:**
- **Rust (`mimir-rs`):** `ghcr.io/birdayz/mimir-rs:13b2226abb57422c6241108bebaa8114d9aabd51` (`rustc 1.100.0`, `linux/amd64`)
- **Upstream Go Mimir:** `docker.io/grafana/mimir:latest` (`version=3.2.6`, `revision=634964f9`, `go1.26.11`, `linux/amd64`)
- **Kafka Broker:** `docker.io/redpandadata/redpanda:latest`
- **Object Storage (S3):** `docker.io/bitnamilegacy/minio:latest`

---

## 1. Infrastructure and Topology (`cluster=mimir-stress`, `fsn1`)

Provisioned on Hetzner Cloud (`fsn1`, `ubuntu-24.04`) on isolated private network `mimir-stress-net` (`10.88.0.0/16`, subnet `10.88.0.0/24`).

| Hostname | Role / Zone | Server Type | vCPU / RAM / Kernel | Private IP | Software |
|---|---|---|---|---|---|
| `mimir-stress-ing-1` | Ingester 1 (`zone-a`) | `cpx32` | 4 vCPU (AMD EPYC-Genoa) / 7,745 MiB / `6.8.0-138-generic` | `10.88.0.11` | `mimir-rs:13b2226` |
| `mimir-stress-ing-2` | Ingester 2 (`zone-b`) | `cpx32` | 4 vCPU (AMD EPYC-Genoa) / 7,745 MiB / `6.8.0-138-generic` | `10.88.0.12` | `mimir-rs:13b2226` |
| `mimir-stress-ing-3` | Ingester 3 (`zone-c`) | `cpx32` | 4 vCPU (AMD EPYC-Genoa) / 7,745 MiB / `6.8.0-138-generic` | `10.88.0.13` | `grafana/mimir:3.2.6` |
| `mimir-stress-go-stack` | Go Stack + Redpanda + MinIO | `ccx43` | 16 dedicated vCPU (AMD EPYC-Milan) / 62,784 MiB / `6.8.0-138-generic` | `10.88.0.20` | Go Mimir (`distributor`, `query-frontend`, `query-scheduler`, `querier`, `store-gateway`, `compactor`), Redpanda (`:9092`), MinIO (`:9000`) |
| `mimir-stress-loadgen` | Loadgen + PromQL + Scraper | `ccx43` | 16 dedicated vCPU (AMD EPYC-Milan) / 62,784 MiB / `6.8.0-138-generic` | `10.88.0.30` | Custom Go `stress-loadgen` + 5s cgroup/proc/metrics scraper |

All Mimir services communicate over `10.88.0.0/24` using `memberlist` TCP gossip (`7941..7947`), with `replication_factor: 3` and `zone_awareness_enabled: true`.

---

## 2. Summary of Pass 4 Findings

1. **Pass 3 Reliability Fixes Verified in `mimir-rs:13b2226`:**
   - **Stage 2 Live Flush (0 deadlocks):** Under active `150k samples/s` + `20 req/s` load, both Rust `ing-1` (**58.74 s**, `HTTP 204`, `2,077.3 -> 2,969.4 -> 1,138.3 MB RssAnon`) and `ing-2` (**74.27 s**, `HTTP 204`, `2,298.0 -> 4,700.5 -> 1,131.5 MB RssAnon`) completed compaction/flush and caught up in lockstep to `offset=510,680` with **0 restarts** and **0 deadlocks**. Go `ing-3` was OOM-killed during Stage 2 flush (`7.26 GB RssAnon`, `http_code=0`, `4` restarts).
   - **Stage 3 Redpanda Restart Recovery (0 unhandled `fetcher.rs` errors):** During `docker restart redpanda` (`1,419.3 ms`), both `ing-1` and `ing-2` logged retryable `fetcher.rs:1132` errors (`connection refused`, `FencedLeaderEpoch`, `NotLeaderOrFollower`), reconnected automatically within 5 s, and advanced in lockstep to `offset=607,084` with **0 unhandled `fetcher.rs:363` errors** and **0 unexpected restarts**.
   - **`/ingester/prepare-shutdown` Parity in Classic Push Mode:** `POST /ingester/prepare-shutdown` returned `204` (`cortex_ingester_prepare_shutdown_requested = 1`), `GET` returned `200` (`body=set`), and `DELETE /ingester/prepare-shutdown` returned `204` (`cortex_ingester_prepare_shutdown_requested = 0`, `/ready = 200`).

2. **Zero OOM Kills and Zero Unplanned Restarts on Rust vs 35+ Kernel OOM Kills on Go:**
   - **Stage 1A (25m Kafka `ingest_storage`, 1M series + 300k churn at `200k/s` + `30 req/s`):** Coprime OOO modulo (`503`) produced `count_exact == 1,000,000` from `t=20s` through `t=1515s` (`99` checks, `count_mismatch = 0`). Rust `ing-1`/`ing-2` averaged **2.35-2.82 cores** and **1.57-1.61 GB `RssAnon`** (`3.15-3.16 GB` peak) with **0 write errors** (`300.35M` samples) and **0 query errors** (`37,983` queries), vs Go `ing-3` at **3.32 cores** and **4.87 GB avg / 6.85 GB peak `RssAnon`** (**3.07x** higher average RAM).
   - **Stage 1B (10m unthrottled 1M-series `count_exact` scan):** Both Rust ingesters completed all 10 minutes with **0 restarts / 0 OOMs** (**2.12-2.40 avg cores**, **1.89-1.97 GB avg / 2.64-2.75 GB peak `RssAnon`**, `count_mismatch = 0`), whereas Go `ing-3` averaged **6.14 GB `RssAnon`** (`7.27 GB` peak) and was **OOM-killed twice** (`7.54-7.55 GB anon-rss`).
   - **Stage 4A/4B/4C (Classic gRPC `Push`, RF=3, Snappy, 1M -> 2.5M -> 3.5M series at `300k/s`):**
     - **4A (1.0M series, 5m):** `285.8k/s`, **0 write errors**, **0 query errors** (`4,522` queries, `count_mismatch=0`). Rust `ing-1`/`ing-2` used **1.12-1.18 GB avg / 1.51-1.58 GB peak `RssAnon`** vs Go `ing-3` at **3.87 GB avg / 5.09 GB peak `RssAnon`** (**3.36x** less average RAM).
     - **4B (2.5M series, 5m):** `292.0k/s`, Rust held **2.41-2.73 GB avg / 3.87-4.05 GB peak `RssAnon`** with **0 restarts**, while Go `ing-3` peaked at **7.18 GB `RssAnon`** and entered a WAL-replay OOM loop (`7.49 GB anon-rss`).
     - **4C (3.5M series, 5m ceiling test):** Both Rust nodes sustained **300.0k samples/s** (`299,986.5/s`, **0 write errors**, `p50 = 11.91 ms`) with **0 restarts** at **0.56-0.73 avg cores (16% CPU)** and **3.43-3.47 GB avg / 3.81-3.85 GB peak `RssAnon`**, while Go `ing-3` used **1.51 avg cores** and was OOM-killed (`7.49 GB anon-rss`).

---

## 3. Stage-by-Stage Results

### Stage 1A: 25 Minutes (1,500 s) Sustained Kafka `ingest_storage` (RF=3, Partition 0)

- **Workload:** `1,000,000` active 10-label series (`container_cpu_usage_seconds_total`), `200,000 samples/s`, `0.2%` OOO (`-5m` within `30m` window, coprime modulo `503`), `200 series/s` churn (`k8s_pod_churn_metric`), `MetricMetadata`, `30 req/s` mixed PromQL across 9 endpoints plus periodic `15s` `count_exact`.
- **Writes:** `300,346,046` samples (`198,247.2 samples/s` avg), `300,046` batches OK, **`0` write errors**, `596,000` OOO samples, `300,046` churn series, `p50 = 37.00 ms`, `p95 = 60.38 ms`, `p99 = 65.24 ms`.
- **PromQL Reads:** `37,983` OK, `0` errors, `count_checks = 99`, `last_count = 1,000,000`, `count_mismatch = 0` from `t=20s` onward.
- **CPU Profile Captured:** 15-second `/debug/pprof/profile?seconds=15` captured mid-Stage-1A (`t~710s`) from `ing-1` (`Duration: 15s, Total samples = 29.32s (195.44%)`).

| Node (4 vCPU / 8 GB) | Engine | Avg Cores | Max Cores | Avg `RssAnon` | Peak `RssAnon` | Peak `VmRSS` | Restarts | Final Offset | Final `memory_series` |
|---|---|---|---|---|---|---|---|---|---|
| `ing-1` (`10.88.0.11`) | Rust `mimir-rs:13b2226` | **2.35** | 3.30 | **1,606.4 MB** | **3,160.8 MB** | 4,030.5 MB | 0 | 299,984 | 1,299,996 |
| `ing-2` (`10.88.0.12`) | Rust `mimir-rs:13b2226` | **2.82** | 3.94 | **1,566.7 MB** | **3,149.7 MB** | 3,930.9 MB | 0 | 299,996 | 1,300,005 |
| `ing-3` (`10.88.0.13`) | Go `mimir 3.2.6` | **3.32** | 3.97 | **4,867.2 MB** | **6,846.8 MB** | 7,223.6 MB | 0 | 296,113 | 1,297,754 |

| PromQL Endpoint (Stage 1A) | OK / Err | p50 (ms) | p95 (ms) | p99 (ms) | Max (ms) |
|---|---|---|---|---|---|
| `instant_rate` | 4,727 / 0 | 1,701.85 | 3,562.63 | 4,470.14 | 33,968.84 |
| `range_rate` | 4,750 / 0 | 1,797.15 | 3,678.85 | 4,626.29 | 43,789.74 |
| `count_exact` (15s periodic) | 99 / 0 | 4,964.04 | 12,332.89 | 48,698.05 | 48,698.05 |
| `regex_pod` | 4,704 / 0 | 1,616.07 | 3,416.97 | 4,051.96 | 30,263.35 |
| `/api/v1/labels` | 4,773 / 0 | 34.09 | 110.53 | 160.74 | 1,069.71 |
| `/api/v1/label/namespace/values` | 4,771 / 0 | 30.78 | 107.45 | 167.40 | 2,891.79 |
| `/api/v1/series` | 4,730 / 0 | 587.44 | 1,154.29 | 1,589.59 | 8,751.28 |
| `/api/v1/metadata` | 4,703 / 0 | 19.65 | 96.78 | 151.97 | 501.71 |
| `/api/v1/cardinality/label_values` | 4,726 / 0 | 32.36 | 153.32 | 264.74 | 575.15 |

---

### Stage 1B: 10 Minutes (600 s) Unthrottled 1M-Series `count_exact` Scan Workload

- **Workload:** `200,000 samples/s` writes plus `30 req/s` PromQL with `count({__name__="container_cpu_usage_seconds_total"})` directly in the 9-endpoint round-robin, stacked immediately onto Stage 1A's 25-minute `1.3M`-series head.
- **Writes:** `120,746,626` samples (`197,130.1/s` avg), `120,626` batches OK, **`0` write errors**, `p50 = 36.97 ms`, `p95 = 60.66 ms`, `p99 = 64.69 ms`.
- **PromQL Reads:** `1,859` OK / `25` errors (60s client timeouts during Go `ing-3` OOMs), `count_checks = 198`, `last_count = 1,000,000`, `count_mismatch = 0`.

| Node | Engine | Avg Cores | Max Cores | Avg `RssAnon` | Peak `RssAnon` | Peak `VmRSS` | Restarts / OOMs | End State |
|---|---|---|---|---|---|---|---|---|
| `ing-1` | Rust `13b2226` | **2.12** | 3.95 | **1,972.7 MB** | **2,636.3 MB** | 5,008.7 MB | **0 / 0** | Healthy, `offset=420,031`, `1,300,046` series |
| `ing-2` | Rust `13b2226` | **2.40** | 3.98 | **1,890.9 MB** | **2,745.6 MB** | 4,963.1 MB | **0 / 0** | Healthy, `offset=420,031`, `1,300,046` series |
| `ing-3` | Go `3.2.6` | **3.07** | 3.97 | **6,139.7 MB** | **7,273.3 MB** | 7,282.8 MB | **2 OOMs (7.54-7.55 GB, 15:39:17 & 15:42:55 UTC)** | Lagging at `offset=393,149` |

| PromQL Endpoint (Stage 1B) | OK / Err | p50 (ms) | p95 (ms) | p99 (ms) |
|---|---|---|---|---|
| `instant_rate` | 205 / 6 | 10,925.07 | 30,249.81 | 48,242.35 |
| `range_rate` | 196 / 3 | 10,394.41 | 36,730.11 | 42,639.66 |
| `count_exact` | 198 / 14 | 15,608.96 | 34,420.28 | 45,128.08 |
| `regex_pod` | 217 / 2 | 9,986.82 | 20,899.01 | 33,391.95 |
| `/api/v1/labels` | 212 / 0 | 79.42 | 933.71 | 5,477.09 |
| `/api/v1/label/namespace/values` | 227 / 0 | 70.05 | 935.97 | 3,327.15 |
| `/api/v1/series` | 203 / 0 | 809.70 | 8,808.25 | 15,975.25 |
| `/api/v1/metadata` | 195 / 0 | 95.91 | 961.76 | 2,317.48 |
| `/api/v1/cardinality/label_values` | 206 / 0 | 75.76 | 1,074.75 | 5,881.93 |

---

### Stage 2: 10 Minutes (600 s) Live Head Compaction, MinIO S3 Shipping, and Go Compactor/Store-Gateway Interop

- **Workload:** `150,000 samples/s` writes (`90,107,017` samples, `90,017` batches OK, **`0` write errors**, `p50 = 36.78 ms`, `p99 = 63.40 ms`) plus `20 req/s` PromQL (`1,396` OK / `19` timeouts during Go `ing-3` OOMs, `count_checks = 9`, `last_count = 1,000,000`, `count_mismatch = 0`).
- **Live Flush (`POST /ingester/flush?wait=true` under active load, 200ms `RssAnon` sawtooth polling):**
  - `ing-1` (Rust `13b2226`): `HTTP 204` in **58,741.0 ms (58.74 s)**, sawtooth `2,077.3 -> 2,969.4 -> 1,138.3 MB RssAnon`, **0 restarts, 0 deadlocks**, `avg_cores = 1.14`, `avg_RssAnon = 1,700.2 MB`, `peak_RssAnon = 2,713.9 MB`, caught up to `offset = 510,670` (`1,080,911` series).
  - `ing-2` (Rust `13b2226`): `HTTP 204` in **74,271.9 ms (74.27 s)**, sawtooth `2,298.0 -> 4,700.5 -> 1,131.5 MB RssAnon`, **0 restarts, 0 deadlocks**, `avg_cores = 1.05`, `avg_RssAnon = 1,703.8 MB`, `peak_RssAnon = 4,657.2 MB`, caught up to `offset = 510,680` (`1,067,132` series).
  - `ing-3` (Go `3.2.6`): OOM-killed during live flush (`http_code = 0` at `155,061.7 ms`, sawtooth `7,164.8 -> 7,256.3 -> 59.1 MB RssAnon`, `delta_restarts = 4`, `oom_kills = 2`, stuck in WAL-replay/catch-up OOM loop at `offset = 424,377`).
- **MinIO S3, Go `compactor`, and Go `store-gateway` Interop:**
  - **4 compacted ULID blocks** and `bucket-index.json.gz` verified in MinIO `mimir-blocks/prod/` (`01M7QN9YP0Z8QT3FZDZBMFPNRR`, `01M7QNAQVBJ6FPFC8XJNV3S6NS`, `01M7QNCT03SG6TVDJCD263CC2S`, `01M7QNE8JYFW7559K3PPNGFSNF`).
  - **Go `compactor`:** `cortex_compactor_runs_completed_total = 200`, `cortex_compactor_runs_failed_total = 0`.
  - **Go `store-gateway`:** `cortex_bucket_store_blocks_loaded = 4` (`95,164` bytes cached), `cortex_bucket_store_block_load_failures_total = 0`, `0` bucket operation failures across `attributes`, `delete`, `exists`, `get`, `get_range`, `iter`, and `upload`.
  - Post-Stage-2 PromQL `count_exact` across S3 + head returned `1,000,000`.

*(Before Stage 3, `ing-3`'s `/data/tsdb/*` and Kafka consumer group `ingester-zone-c-0` were reset so Go `ing-3` could exit its Stage 2 WAL-replay OOM loop. Rust `ing-1` and `ing-2` kept running continuously from Stage 1A through Stage 3.)*

---

### Stage 3: 10 Minutes (600 s) Distributed Chaos (`160,000 samples/s` Writes + `25 req/s` PromQL)

- **Workload:** `96,460,364` samples (`156,845.7/s` avg, `96,364` OK, `128` transient write errors during the 1.4 s Redpanda restart, `p50 = 36.76 ms`, `p99 = 64.68 ms`) plus `3,800` PromQL OK / `2` errors, `count_checks = 33`, `last_count = 1,000,000`, `count_mismatch = 0`.
- **Chaos Event A (`T+60s..T+80s`, `docker kill -s KILL ingester` on `ing-1`):**
  - **Write Availability:** `0` distributor write errors before kill and throughout the 20 s `ing-1` outage.
  - **WAL Replay and Kafka Catch-Up (`T+80s` `docker start ingester`):** `ing-1` replayed its 1.09M-series WAL (`547 MB -> 1,943.5 MB -> 1,148.3 MB RssAnon`), reached `/ready == 200` in **73,598.9 ms (73.60 s)**, and reached 0-lag parity with `ing-2` by `t=150.0s` (`<= 70 s` post-start: `ing-1 offset = 534,688` == `ing-2 offset = 534,688`, `1,090,017` series).
- **Chaos Event B (`T+240s`, `docker restart redpanda` mid-load, `1,419.3 ms`):**
  - **Transient Write Impact:** `write_err` rose `0 -> 64 -> 128` during the broker restart and remained flat at `128` (`0` new write errors from `T+255s` to `T+600s`).
  - **Fetcher Recovery:** Both Rust `ing-1` and `ing-2` logged retryable `fetcher.rs:1132` errors (`connection refused`, `FencedLeaderEpoch`, `NotLeaderOrFollower`), reconnected automatically within 5 s (`t=240s: 548,961 -> t=250s: 550,588/550,598`), and consumed in lockstep to `offset = 607,084` (`ing-1 = 607,084`, `ing-2 = 607,084`) with `0` unhandled `fetcher.rs:363` errors.
  - **Go `ing-3` Under Stage 3 Load:** Go `ing-3` hit repeated OOM kills under the 1M-series `160k/s + 25 req/s` workload (`delta_restarts = 19`, `oom_kills = 10`).

---

### Stage 4: 15 Minutes (900 s: 5m + 5m + 5m) Classic gRPC `Push` Mode (RF=3, Snappy) and 8 GB Ceiling

Cluster switched to `ingest_storage.enabled: false`, `push_grpc_method_enabled: true`, `replication_factor: 3`, `zone_awareness_enabled: true`, `ingester_client.grpc_client_config.grpc_compression: snappy`, with clean `/data/tsdb` directories (`cortex_distributor_ingest_storage_enabled = 0`).

- **`/ingester/prepare-shutdown` Classic Push Parity Check (`ing-1`):**
  - `POST /ingester/prepare-shutdown` -> `HTTP 204`, `cortex_ingester_prepare_shutdown_requested = 1`
  - `GET /ingester/prepare-shutdown` -> `HTTP 200` (`body=set`)
  - `DELETE /ingester/prepare-shutdown` -> `HTTP 204`, `cortex_ingester_prepare_shutdown_requested = 0`, `/ready = 200`

| Sub-Stage | Workload | Throughput and Latency | PromQL | `ing-1` (Rust `13b2226`) | `ing-2` (Rust `13b2226`) | `ing-3` (Go `3.2.6`) |
|---|---|---|---|---|---|---|
| **Stage 4A** (315.0s = 5m) | 1,000,000 series at 300k/s + 15 req/s PromQL | 90,039,000 samples (285.8k/s), 0 write err, p50=15.91ms, p99=288.46ms | 4,522 OK / 0 err, count=1,000,000, mismatch=0 | 3.00c avg, 1,184.5 MB avg / 1,584.8 MB peak `RssAnon`, 0 restarts | 1.94c avg, 1,121.2 MB avg / 1,511.1 MB peak `RssAnon`, 0 restarts | 3.49c avg, 3,866.9 MB avg / 5,089.7 MB peak `RssAnon`, 0 restarts |
| **Stage 4B** (310.6s = 5m) | 2,500,000 series at 300k/s + 15 req/s PromQL | 90,679,000 samples (292.0k/s), 251 write err (during Go OOM), p50=46.48ms, p99=567.36ms | 890 OK / 1 err, count=2,500,000, mismatch=1 (early transient 2,498,000 at t=15s during Go OOM) | 2.86c avg, 2,729.8 MB avg / 4,047.8 MB peak `RssAnon`, 0 restarts | 2.87c avg, 2,408.9 MB avg / 3,874.2 MB peak `RssAnon`, 0 restarts | 2.44c avg, 4,134.8 MB avg / 7,175.1 MB peak `RssAnon`, OOM-killed at 7.49 GB (looped rest of 4B) |
| **Stage 4C** (303.1s = 5m) | 3,500,000 series at 300k/s ceiling | 90,921,000 samples (300.0k/s), 0 write err, p50=11.91ms, p99=199.53ms | (write ceiling test) | 0.73c avg, 3,468.3 MB avg / 3,847.4 MB peak `RssAnon`, 0 restarts | 0.56c avg, 3,428.9 MB avg / 3,813.0 MB peak `RssAnon`, 0 restarts | 1.51c avg, 5,068.8 MB avg / 7,281.2 MB peak `RssAnon`, OOM-killed at 7.49 GB |

#### Kernel OOM-Kill Summary (`dmesg -T`)
- **Rust `ing-1` and `ing-2` (`13b2226`):** **0 OOM kills**, **0 unplanned restarts** across all 71.53 minutes (879.3M samples).
- **Go `ing-3` (`3.2.6`):** **35+ kernel OOM kills** recorded across Stage 1B (`15:39:17`, `15:42:55`, `7.54-7.55 GB anon-rss`), Stage 2 (`15:47:04..15:59:17`, `7.47-7.53 GB anon-rss`), Stage 3 (`16:00:28..16:13:43`, `7.49-7.52 GB anon-rss`), and Stage 4B/4C (`16:21:48`, `16:25:56`, `7.49 GB anon-rss`).

---

## 4. Pass 1 -> Pass 2 -> Pass 3 -> Pass 4 (`13b2226`) Progression

| Benchmark | Pass 1 (`f1f680d`) | Pass 2 (`56099c3` / `08370b3`) | Pass 3 (`73b262d`, >1h suite) | Pass 4 (`13b2226`, 71.5m suite) | Go Mimir `3.2.6` (`ing-3`, Pass 4) |
|---|---|---|---|---|---|
| **Stage 1A Sustained Kafka** (25m, 1M+300k churn at `200k/s` + `30 req/s`) | (120s only) | (60s only) | 0 err, 2.54-2.56c, 1.46-1.51 GB avg `RssAnon` (`cnt=998k`) | **0 write/query err**, **`cnt=1,000,000` (`mismatch=0`)**, **2.35-2.82c**, **1.57-1.61 GB avg `RssAnon`** | **3.32c**, **4.87 GB avg / 6.85 GB peak `RssAnon`** (3.07x RAM) |
| **Stage 1B Unthrottled 1M `count_exact`** (10m at `200k/s` + `30 req/s`) | 7.58 GB OOM-kill | 0 OOMs (120s), peak 1.27-1.28 GB | 0 OOMs (600s), 2.22-2.27c, 1.77-1.82 GB avg | **0 OOMs (600s)**, **2.12-2.40c**, **1.89-1.97 GB avg / 2.64-2.75 GB peak `RssAnon`**, `mismatch=0` | **3.07c**, **6.14 GB avg / 7.27 GB peak**, **2 OOM kills at 7.55 GB** |
| **Stage 2 Live Flush + S3** (10m at `150k/s` + `20 req/s`) | `ing-2` OOM, `ing-1` 26.6s | 0 OOMs, 14.18s / 15.68s | `ing-2` 94.98s OK; `ing-1` post-catch-up deadlock | **Both `HTTP 204` (58.74s and 74.27s), 0 deadlocks**, **1.70 GB avg `RssAnon`**, `200` compactor / `4` store-gw OK | **OOM-killed during flush** (`http_code=0`, `7.26 GB` peak) |
| **Stage 3 Distributed Chaos** (10m at `160k/s` + `25 req/s`) | `ing-2` early OOM | 0 OOMs, `/ready` 7.76s, 0-lag 11.88s | 0 err on `ing-1` kill; Redpanda restart wedged `fetcher.rs:363` | **0 err on 20s `SIGKILL ing-1`**, `/ready` **73.60s**, 0-lag **<=70s**; **Redpanda restart clean (`0` unhandled `fetcher.rs` errs, lockstep `offset=607,084`)** | **10 OOM kills / 19 restarts** |
| **Stage 4A Classic Push 1M** (5m at `300k/s` + `15 req/s`) | 134.9k/s, 4.00c, 237 err | 267.4k/s, 75 err, 3.07-3.09c | 281.7k/s, 0 err, 0.95-1.36c, 1.18-1.19 GB avg (`DELETE 405`) | **285.8k/s**, **0 write/query err**, **1.12-1.18 GB avg / 1.51-1.58 GB peak**, `prepare-shutdown` 204/200/204 | **3.49c**, **3.87 GB avg / 5.09 GB peak `RssAnon`** |
| **Stage 4B Classic Push 2.5M** (5m at `300k/s` + `15 req/s`) | 161.9k/s, 939 err | 205.3k/s, peak 4.05-4.29 GB | 291.5k/s, 0 Rust restarts, 2.60-2.62 GB avg | **292.0k/s**, **0 Rust restarts**, **2.41-2.73 GB avg / 3.87-4.05 GB peak `RssAnon`**, `cnt=2.5M` | **7.18 GB peak**, **OOM-killed at 7.49 GB** |
| **Stage 4C Classic Push 3.5M** (5m at `300k/s` ceiling) | Go VM OOM lockup | 0.65-0.66c, peak 4.62-4.63 GB | 300.0k/s, 0.52-0.54c, 3.42 GB avg / 3.99 GB peak | **300.0k/s (`0` write err)**, `p50=11.91ms`, **0.56-0.73c**, **3.43-3.47 GB avg / 3.81-3.85 GB peak `RssAnon`**, **0 restarts** | **1.51c**, **7.28 GB peak**, **OOM-killed at 7.49 GB** |

---

## 5. Teardown

All temporary Hetzner Cloud resources created for the benchmark carry label `cluster=mimir-stress` and are deleted via:

```bash
./scripts/hetzner-teardown.sh
```
