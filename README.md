# mimir-rs

`mimir-rs` is a Rust implementation of [Grafana Mimir](https://github.com/grafana/mimir) (reference commit `06de3ebd`) targeting `-target=ingester` and `-target=memberlist-kv`. It implements the same external wire, disk, configuration, and observability contracts as the Go binary.

A `mimir-rs` ingester runs inside a live Go Mimir cluster without changes to distributors, queriers, query-frontends, store-gateways, compactors, or Helm manifests. It supports both classic gRPC `Push` and Kafka `ingest_storage` ingestion, preserves Go CLI flags, strict YAML parsing, `memberlist`/`etcd`/`consul` ring gossip, WAL/WBL/TSDB/ULID disk layouts, S3/GCS/Azure object layouts, HTTP and gRPC status codes, and Prometheus metrics.

## Design

Go Mimir spends a large share of its CPU and resident memory in runtime overhead and per-series object graphs:

- **GC headroom:** With `GOGC=100`, OS `RssAnon` sits near 2x live heap. In a 2,000,000-series `prod_bench` heap profile (`/debug/pprof/heap -inuse_space`), Go held `1,513.85 MB` live heap against `2,936.1 MiB` `RssAnon`, rising to `5,690.0 MiB` under steady read/write load.
- **Per-series pointer graphs:** Each active series allocates Go `string`/`slice` headers (`stringslite.Clone`), `chunkenc.NewXORChunk` buffers, `tsdb.newMemSeries` structs, and active-series map entries that the garbage collector must trace.
- **Per-RPC allocations:** Decoding `cortexpb.WriteRequest` allocates `TimeSeries` and `LabelPair` structs on every push batch, alongside per-request Snappy/Zstd compression buffers.

`mimir-rs` replaces these layouts with fixed-width tables, arena slices, and unboxed encoders:

- **Symbol interning and compact series state:** Per-tenant label tables intern names and values to `u32` IDs, and canonical label sets are stored as contiguous `stringlabels` byte slices. `SeriesEntry` fits in `176 B` with inline `mmapped_chunks` metadata and a 3-bit active chunk encoding tag. Out-of-order float samples default to `16 B` `PlainFloatOooSample` slots and only widen when non-zero ST timestamps or mixed encodings appear.
- **Two-level paged tables with OS page reclamation:** `PagedById` (series lookup by ID) and lock-free `NanosSlab` (active-series timestamps) index up to `256 * 256 * 4096` slots via two pointer indirections. Empty pages are returned to the kernel via `madvise(MADV_DONTNEED)`.
- **Single-pass zero-copy wire decoders:** `decode_classic_stream` and `decode_rw2_stream` validate UTF-8, resolve `V2CommonSymbols`, encode `stringlabels` directly into reusable per-worker arenas, and compute `xxhash64` in one pass without intermediate `LabelPair` structs.
- **Direct-to-`BytesMut` gRPC encoders:** `QueryStream` (`QueryStreamBatchBuilder`) and `MetricsForLabelMatchers` (`encode_metric_stringlabels`) serialize protobuf wire frames straight from `stringlabels` and raw chunk bytes into `BytesMut`, avoiding `prost` message trees and `Bytes::slice` refcount operations.
- **Lockless WAL pre-compression and `jemalloc`:** WAL records are Snappy/Zstd-compressed on background worker threads before taking the WAL writer lock (`WlogWriter::log_precompressed`). Linux builds link `tikv-jemallocator` (`background_thread:true,dirty_decay_ms:250,muzzy_decay_ms:0,narenas:8`) to return freed pages promptly (see [docs/allocator-benchmark.md](docs/allocator-benchmark.md) for the ABBA comparison against `glibc` and stock `jemalloc`).

## Compatibility and Current Scope

### Implemented Surface

- **CLI, YAML, and runtime configuration:** Single-dash (`-target=ingester`, `-config.file=...`) and double-dash (`--target=ingester`) flags, `-version`, `-modules`, `-help`, `-help-all`, `-print.config`. Strict `yaml.v3` `KnownFields(true)` validation with matching field/type error messages, `common:` inheritance, `${VAR}` and `${VAR:default}` expansion, file and HTTP(S) `runtime_config` reload (`yaml` and `map` loaders), and per-tenant `Overrides`.
- **KV stores and rings:** `memberlist` (SWIM TCP/UDP gossip, cluster label framing, TLS, `dns+`/`dnssrv+`/`dnssrvnoa+` seed discovery), `etcd` (HTTP/JSON gateway with mTLS and `WaitForReady` backoff), `consul`, `multi` (primary/secondary mirroring and runtime cutover), and `inmemory`. Supports both classic `Ring` (`Lifecycler`) and `PartitionRing`, including tokenless `num_tokens: 0` pure-Kafka deployments without `memberlist` or port `7946`.
- **gRPC and HTTP APIs:** All 14 `cortex.Ingester` RPCs (`Push`, `QueryStream`, `QueryExemplars`, `LabelValues`, `LabelNames`, `UserStats`, `AllUserStats`, `MetricsForLabelMatchers`, `MetricsMetadata`, `LabelNamesAndValues`, `LabelValuesCardinality`, `ActiveSeries`, `SearchLabelNames`, `SearchLabelValues`), `httpgrpc.HTTP`, and `grpc.health.v1.Health`. Supports `snappy`, `s2`, `gzip`, `zstd`, and `identity` gRPC compression, HTTP/gRPC TLS/mTLS with per-handshake cert reload, `x-scope-orgid` multitenancy, `ErrorDetails` error payloads, and HTTP endpoints (`/ready`, `/metrics`, `/config`, `/runtime_config`, `/services`, `/memberlist`, `/ingester/ring`, `/ingester/partition-ring`, `/ingester/flush`, `/ingester/prepare-shutdown`, `/ingester/prepare-partition-downscale`, `/ingester/prepare-instance-ring-downscale`, `/ingester/unregister-on-shutdown`, `/ingester/shutdown`, `/ingester/tenants`, `/ingester/tsdb/:tenant`, `/debug/pprof`).
- **Kafka `ingest_storage`:** Pure-Rust Kafka client supporting record versions v0, v1, and v2 RW2 (`V2CommonSymbols` offset 64), Zstd/Snappy/LZ4/Gzip batch compression, `ConcurrentFetchers` (`fetch_concurrency_max > 0`) and rack-aware follower fetching (`fetch_concurrency_max = 0`), `ListOffsets` `-1`/`-2`, offset catalogue and local `offset` file persistence, strong read consistency checks, and TLS/mTLS plus SASL (`PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, `OAUTHBEARER`, `AWS_MSK_IAM`).
- **TSDB and disk layout:** Per-tenant heads with float samples (`Xor`, `Xor2` with ST timestamps), integer and float native histograms (including NHCB), out-of-order chunks and WBL, exemplars, metadata, tombstones, torn-tail WAL/WBL/`chunks_head` startup repair, mmap `chunks_head`, and ULID block compaction. Bidirectional `SIGKILL` handoff between Go Mimir and `mimir-rs` on the same PVC works without data conversion.
- **Object storage:** S3 (SigV4 header and streaming `aws-chunked` signing, SSE-S3, SSE-KMS, per-tenant SSE overrides, region auto-detection), GCS (service account JWT, metadata server, OAuth2 refresh), Azure Blob, and local filesystem, with 1-hour IAM/OAuth token caching and refresh on expiry or HTTP 401.
- **Observability:** W3C `traceparent` and Jaeger trace propagation, `logfmt` and `json` structured logs, and `251 / 251` non-`go_*` Prometheus metric family parity (text and delimited protobuf). The 33 Go-runtime `go_*` metrics do not exist in a Rust binary.

### Not Implemented Yet

- **Other Mimir targets:** `distributor`, `querier`, `query-frontend`, `query-scheduler`, `store-gateway`, `compactor`, `ruler`, `alertmanager`, and `all`. Passing any unimplemented module in `-target` fails startup with `target module "<name>" is not yet implemented in mimir-rs; supported targets: ingester, memberlist-kv`.
- **Object storage / secret backends:** `swift` (OpenStack Swift) and `vault` (`-vault.enabled`).
- **Explicitly rejected flags:** `-memberlist.propagation-delay-tracker.enabled=true`, `memory_snapshot_on_shutdown: true`, and local leveled compaction (`> 1` `block_ranges_period`) fail config validation at startup.

## Benchmarks

### Multi-Node Hetzner Cloud Suite (71.5 Minutes, 879.3M Samples)

Compared `ghcr.io/birdayz/mimir-rs:13b2226` against `grafana/mimir:3.2.6` inside a mixed `RF=3`, zone-aware cluster on Hetzner Cloud (`fsn1`, Ubuntu 24.04, Linux `6.8.0-138-generic`, private network `10.88.0.0/24`, `memberlist` gossip):

- **Ingesters (`3x cpx32`, 4 vCPU AMD EPYC-Genoa, 7,745 MiB RAM each):** `ing-1` (`zone-a`, `mimir-rs:13b2226`), `ing-2` (`zone-b`, `mimir-rs:13b2226`), `ing-3` (`zone-c`, `grafana/mimir:3.2.6`).
- **Control plane, broker, storage, and loadgen (`2x ccx43`, 16 dedicated vCPU AMD EPYC-Milan, 62,784 MiB RAM each):** one host running Go Mimir (`distributor`, `query-frontend`, `query-scheduler`, `querier`, `store-gateway`, `compactor`), Redpanda, and MinIO; one host running the Go load generator and 5 s scraper.

| Stage | Workload | Throughput and Correctness | Rust `ing-1` / `ing-2` | Go `ing-3` |
|---|---|---|---|---|
| **1A: Sustained Kafka** (25m) | 1M series + 300k churn, 200k samples/s, 0.2% OOO, 30 req/s PromQL | 300.35M samples, 0 write err, 37,983 PromQL OK / 0 err, `count_exact=1,000,000` (`mismatch=0`) | 2.35-2.82 avg cores, 1.57-1.61 GB avg / 3.15-3.16 GB peak `RssAnon`, 0 restarts | 3.32 avg cores, 4.87 GB avg / 6.85 GB peak `RssAnon`, 0 restarts |
| **1B: Unthrottled `count_exact`** (10m) | 200k samples/s + 30 req/s PromQL with 1M-series `count` in round-robin | 120.75M samples, 0 write err, 1,859 PromQL OK / 25 err (during Go OOMs), `mismatch=0` | 2.12-2.40 avg cores, 1.89-1.97 GB avg / 2.64-2.75 GB peak `RssAnon`, 0 OOMs | 3.07 avg cores, 6.14 GB avg / 7.27 GB peak `RssAnon`, 2 OOM kills |
| **2: Live Flush + S3** (10m) | 150k samples/s + 20 req/s PromQL + `POST /ingester/flush?wait=true` | 90.11M samples, 0 write err, 4 S3 blocks, 200 Go compactor runs (0 fail), 4 Go store-gw blocks loaded | HTTP 204 in 58.74 s and 74.27 s, 1.70 GB avg `RssAnon`, 0 deadlocks / 0 restarts | OOM-killed during flush (`http_code=0`, 7.26 GB peak), 4 restarts |
| **3: Chaos** (10m) | 160k samples/s + 25 req/s PromQL, 20s `SIGKILL ing-1`, `1.4s` Redpanda restart | 96.46M samples, 0 write err during `ing-1` kill, 128 transient write err during broker restart | `/ready` in 73.60 s, 0-lag in <=70 s, lockstep recovery to `offset=607,084`, 0 restarts post-start | 10 OOM kills, 19 restarts |
| **4A: Classic Push 1M** (5m) | Classic gRPC `Push` (Snappy), 1.0M series, 300k/s + 15 req/s PromQL | 90.04M samples (285.8k/s), 0 write err, 4,522 PromQL OK / 0 err, `mismatch=0` | 1.94-3.00 avg cores, 1.12-1.18 GB avg / 1.51-1.58 GB peak `RssAnon`, 0 restarts | 3.49 avg cores, 3.87 GB avg / 5.09 GB peak `RssAnon`, 0 restarts |
| **4B: Classic Push 2.5M** (5m) | Classic gRPC `Push` (Snappy), 2.5M series, 300k/s + 15 req/s PromQL | 90.68M samples (292.0k/s), 251 write err (during Go OOM), `count=2,500,000` | 2.86-2.87 avg cores, 2.41-2.73 GB avg / 3.87-4.05 GB peak `RssAnon`, 0 restarts | 2.44 avg cores, 4.13 GB avg / 7.18 GB peak `RssAnon`, OOM loop (7.49 GB) |
| **4C: Classic Push 3.5M** (5m) | Classic gRPC `Push` (Snappy), 3.5M series, 300k/s write ceiling | 90.92M samples (300.0k/s), 0 write err, p50=11.91 ms, p99=199.53 ms | 0.56-0.73 avg cores, 3.43-3.47 GB avg / 3.81-3.85 GB peak `RssAnon`, 0 restarts | 1.51 avg cores, 5.07 GB avg / 7.28 GB peak `RssAnon`, OOM-killed (7.49 GB) |

Across the full 71.53-minute run (879,299,053 samples), Rust `ing-1` and `ing-2` recorded 0 OOM kills and 0 unplanned restarts, while Go `ing-3` hit 35+ kernel OOM kills in `dmesg`. See [HETZNER_STRESS_REPORT.md](HETZNER_STRESS_REPORT.md) for per-endpoint PromQL latencies, sawtooth measurements, and the Pass 1 through Pass 4 progression.

### Single-Host Isolated Benchmarks

Measured sequentially on an AMD Ryzen 9 3900X (12C / 24T, 62 GiB RAM, Linux) comparing `mimir-rs` against `target/go-mimir` (`3.2.6`) at commit `13b2226`:

#### Kafka `ingest_storage` Simulation (500k Series, 4 Historical Blocks, Bimodal Reads, Compaction, SIGKILL)

```bash
just prod-bench --series 500000 --steady-secs 35
```

| Phase / Metric | Go Mimir | Rust `mimir-rs` | Ratio (Go / Rust) |
|---|---:|---:|---:|
| Phase 1 catch-up (600k samples) | 3.82 s (8.18 s CPU) | 3.34 s (7.10 s CPU) | 1.14x wall / 1.15x CPU |
| Phase 1 `RssAnon` | 888.4 MiB | 592.1 MiB | 1.50x less RAM |
| Phase 2 steady CPU (reads ON) | 5.26 us/sample | 1.93 us/sample | 2.73x less CPU (-63.4%) |
| Phase 2 `RssAnon` / `VmHWM` | 2,081.0 / 2,186.3 MiB | 684.7 / 831.9 MiB | 3.04x `RssAnon` / 2.63x `VmHWM` |
| `QueryStream` small / heavy (28k) p99 | 0.98 ms / 674.09 ms | 0.69 ms / 357.84 ms | 1.42x / 1.88x lower |
| `LabelNamesAndValues` / `LabelValuesCardinality` p50 | 472.07 ms / 145.95 ms | 118.01 ms / 3.11 ms | 4.00x / 46.93x faster |
| Kafka consume delay p99 | 0.874 s | 0.203 s | 4.31x lower |
| Phase 3 live head compaction wall / peak `RssAnon` | 10.49 s / 2,439.4 MiB | 6.22 s / 825.8 MiB | 1.69x faster / 2.95x less RAM |
| Phase 4 `SIGKILL` `/ready` / 0-lag catch-up | 4.68 s / 7.66 s | 3.40 s / 4.13 s | 1.38x / 1.86x faster |

#### Classic gRPC `Push` + Concurrent Reads (302k Series, 5 Tenants, 20 Rounds = 6.04M Samples, `c=16`)

```bash
just bench-vs-go --series 300000 --concurrency 16 --compression snappy
just bench-vs-go --series 300000 --concurrency 16 --compression s2
```

| Metric | Go (Snappy) | Rust (Snappy) | Go (S2) | Rust (S2) |
|---|---:|---:|---:|---:|
| Ingest wall / throughput | 4.883 s (1.237M/s) | 3.833 s (1.576M/s) | 4.887 s (1.236M/s) | 3.733 s (1.618M/s) |
| Ingest CPU | 31.960 s (5.291 CPU-s/1M) | 14.640 s (2.424 CPU-s/1M) | 31.650 s (5.240 CPU-s/1M) | 14.470 s (2.396 CPU-s/1M) |
| `Push` latency p50 / p99 | 8.25 / 88.90 ms | 4.53 / 106.34 ms | 8.31 / 91.87 ms | 4.55 / 97.87 ms |
| Query wall (200 RPCs) | 0.702 s | 0.290 s | 0.647 s | 0.316 s |
| `QueryStream` p99 | 50.98 ms | 25.91 ms | 66.38 ms | 27.35 ms |
| `LabelNamesAndValues` p99 | 194.41 ms | 47.49 ms | 207.20 ms | 51.52 ms |
| `LabelValuesCardinality` p99 | 89.82 ms | 13.54 ms | 82.40 ms | 18.51 ms |
| Total CPU (ingest + queries) | 39.770 s | 17.310 s | 39.420 s | 17.410 s |
| Final `VmRSS` / `VmHWM` | 1,401.2 / 1,401.2 MiB | 411.5 / 422.8 MiB | 1,354.8 / 1,354.8 MiB | 435.1 / 448.7 MiB |

## Deployment and Usage

### Container Images

Multi-architecture (`linux/amd64` and `linux/arm64`) distroless images (`gcr.io/distroless/cc-debian13`, entrypoint `/bin/mimir`, port `8080`, compatible with both root and Kubernetes `10001:10001` `podSecurityContext`) are built reproducibly (`SOURCE_DATE_EPOCH=0`, `rewrite-timestamp=true`) and published on pushes to `main`:

```bash
docker pull ghcr.io/birdayz/mimir-rs:main
# or pin a specific commit SHA:
docker pull ghcr.io/birdayz/mimir-rs:<git-sha>
```

### Helm (`mimir-distributed`) Ingester Override

To run `mimir-rs` ingesters alongside upstream Go Mimir components in `mimir-distributed`, override only `ingester.image`:

```yaml
ingester:
  image:
    repository: ghcr.io/birdayz/mimir-rs
    tag: main
```

Or via `helm upgrade --install`:

```bash
helm upgrade --install mimir grafana/mimir-distributed -n mimir \
  --set ingester.image.repository=ghcr.io/birdayz/mimir-rs \
  --set ingester.image.tag=main
```

To roll back to Go Mimir, point `ingester.image` back to `grafana/mimir` on the same PVCs. Existing `wal/`, `wbl/`, `chunks_head/`, ULID blocks, and `tokens_file_path` state are directly readable by Go Mimir without migration.

### Standalone CLI

```bash
mimir -target=ingester -config.file=/etc/mimir/mimir.yaml
mimir -target=memberlist-kv -config.file=/etc/mimir/mimir.yaml
mimir -print.config -config.file=/etc/mimir/mimir.yaml
mimir -version
mimir -modules
```

## Workspace Layout

| Crate | Purpose |
|---|---|
| `crates/mimir-proto` | `cortexpb`, `ingesterpb` (14 RPCs), `ring`, `partition_ring`, `kv`, `httpgrpc`, zero-copy classic/RW2 decoders, `QueryStream`/`MetricsForLabelMatchers` encoders, FNV-1 sharding, Snappy/S2 gRPC codecs |
| `crates/mimir-common` | Tenant IDs, `stringlabels` `xxhash64`, `Service` FSM, `ScopedWorkerPool`, logging, `ErrorDetails`, limiters, `netutil`/`sockaddr`, TLS config, shutdown marker, activity tracker |
| `crates/mimir-config` | Go `-flag`/`--flag` parser, strict `yaml.v3` loader and emitter, `${VAR}` expansion, `common:` inheritance, HTTP/file runtime config, per-tenant `Overrides` |
| `crates/mimir-tsdb` | `u32` symbol table, `LabelMatcher` and Go regexp syntax errors, XOR/XOR2/histogram/NHCB/OOO chunks, WAL/WBL, `chunks_head` mmap, striped `Head` with `PagedById`, postings, postings cache, exemplars, ULID blocks |
| `crates/mimir-ring` | `inmemory`/`consul`/`etcd`/`multi`/`memberlist` KV stores, SWIM gossip + DNS provider, classic `Ring` + `Lifecycler`, `PartitionRing` |
| `crates/mimir-storage` | S3/GCS/Azure/filesystem `BucketClient`, `Shipper`, pure-Rust Kafka `ingest_storage` v0-v2 client with TLS/SASL and rack-aware fetchers |
| `crates/mimir-ingester` | `UserTSDB`, `NanosSlab` active series, owned series, limiters, reactive prioritizer, cost attribution, all 14 gRPC RPCs, HTTP admin routes, metrics |
| `crates/mimir-cmd` | `mimir` binary (`-target=ingester`, `-target=memberlist-kv`), HTTP/gRPC server, `httpgrpc`, `/ready`, `/metrics`, `/config`, `/debug/pprof`, `tikv-jemallocator` |
| `crates/mimir-conformance` | Go-oracle differential tests, twin-process suites, Docker/Testcontainers suites, `bench_vs_go`, `prod_bench`, `soak_vs_go` |

## Building and Verification

### Prerequisites

- Rust stable toolchain (Workspace Edition 2021).
- For Go-oracle, conformance, and head-to-head benchmarks: Go toolchain (matching `.ref/mimir/go.mod`) and upstream Go Mimir cloned at `.ref/mimir`:

```bash
git clone --depth 1 https://github.com/grafana/mimir.git .ref/mimir
git -C .ref/mimir fetch --depth 1 origin 06de3ebd2b12aa22861b96e6863b93a162c31e3c
git -C .ref/mimir checkout 06de3ebd2b12aa22861b96e6863b93a162c31e3c
```

- For container and Helm e2e suites: `docker`, `just`, `k3d`, `helm`, `kubectl`, `jq`.

### Commands

```bash
just build-release        # builds target/release/mimir
just test                 # runs workspace tests (with .ref/mimir present)
just qa                   # builds go-oracle + go-mimir, cargo check, clippy, parity, tests
just conformance          # runs Go-vs-Rust differential suites
just parity               # verifies ported upstream Go test coverage
just docker-build         # builds local container image
just docker-build-repro   # builds reproducible OCI tarball (target/mimir-rs-oci.tar)
```

### Verification Layers

- **Upstream test parity:** 575 / 575 ported upstream Go tests (`./scripts/verify-test-parity.sh`, `0` missing, `#[ignore]` disallowed).
- **Go Package Oracle (`tests/go-oracle`):** Verifies bit-for-bit parity against compiled Go packages for hashes, ring/partition-ring token tables, protobuf and Kafka RW2 wire bytes, config parsing/YAML output, regex error syntax, and bidirectional TSDB WAL/ULID block compatibility.
- **Twin-process conformance:** Runs `target/go-mimir` and `target/release/mimir` side-by-side across all 14 `cortex.Ingester` RPCs, `httpgrpc`, `ErrorDetails`, `/config` YAML rendering, `251 / 251` non-`go_*` `/metrics` parity, and `SIGKILL` WAL/WBL/`chunks_head` repair and handoff.
- **Testcontainers suites:** Exercises real Redpanda (TLS/mTLS, SASL, rack-aware follower fetch, broker outage recovery), Silo S3 (KMS, SSE-S3, per-tenant SSE), Azurite (Azure Blob), etcd (mTLS), Consul, and pure-Kafka `num_tokens: 0` clusters without `memberlist`.
- **3-zone `k3d` Helm e2e (`scripts/k3d-helm-e2e.sh`):** Deploys the unmodified upstream `mimir-distributed` chart across 3 zone-labelled agent nodes in `mixed` (2 Rust + 1 Go) and `rust` (3 Rust) modes for both `memberlist` and `etcd` KV configurations, testing readiness, ring state, Prometheus remote-write/query path, and zone rolling restarts.
- **Miri on `unsafe` primitives:** CI runs `cargo +nightly miri test` over `thread_pool`, `labels_hash`, `zerocopy`, `snappy`, `s2`, `query_stream_enc`, `bstream`, `paged_by_id`, `hash_sorted_pairs`, `labels`, `symbols`, `postings`, `postings_cache`, `ooo`, and `nanos_slab`.
- **10-decoder fuzzing (`w3_codec_and_disk_fuzz_conformance.rs`):** Seeded property and corrupt-byte mutation fuzzing across classic/RW2 protobuf, `QueryStream` encoding, Snappy/S2 framed gRPC, Kafka RecordBatch v2, WAL/WBL, `chunks_head`, block chunks, TSDB `IndexReader`, chunk iterators, and `memberlist` wire/Snappy envelopes.
- **24-hour containerized soak (`just soak` / `soak_vs_go`):** Runs Go and Rust ingesters side-by-side in `gcr.io/distroless/cc-debian13` against Redpanda and Silo S3 under continuous writes, OOO, churn, all 13 read RPCs, and periodic response-parity checks.

## License

Licensed under [AGPL-3.0-only](LICENSE), matching upstream Grafana Mimir.
