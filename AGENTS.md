# AGENTS.md — `mimir-rs`

## Mission

`mimir-rs` is a **100% drop-in compatible, mechanical 1:1 Rust port of Grafana Mimir** (starting with `-target=ingester`, expanding to the remaining targets in this workspace), engineered to dramatically cut **CPU and memory consumption** at production scale without sacrificing a single flag, wire byte, metric, or behavior.

**Drop-in compatibility is NON-NEGOTIABLE.** A `mimir-rs` binary must be able to replace a Go `mimir` pod inside a live mixed Go + Rust Mimir cluster (classic gRPC push or Kafka `ingest_storage`) with identical configuration, CLI invocation, ring gossip, storage layout, gRPC/HTTP APIs, error semantics, and Prometheus metrics.

---

## Source of Truth

- Upstream Go reference lives at **`.ref/mimir`** (gitignored shallow clone of `github.com/grafana/mimir` `main`).
- **Never guess, invent, simplify, or "improve" externally observable behavior.** Before implementing any struct, RPC, flag, error, hash, codec, or state transition, read the exact Go source and `*_test.go` in `.ref/mimir` (and its `dskit` / `prometheus` dependencies via `go list` / module cache when needed).
- Every Rust module must carry a top-of-file comment noting the upstream Go file(s) it ports, e.g.:
  `//! Ported from github.com/grafana/mimir/pkg/ingester/owned_series.go`
- No "Rust-idiomatic" renaming of YAML keys, CLI flags, metric names, label names, HTTP routes, gRPC paths, KV keys, or log fields. Keep upstream names verbatim on every external boundary.

---

## Non-Negotiable Compatibility Surface (`-target=ingester` first, shared foundations reusable by all targets)

1. **CLI & Config (`cmd/mimir`, `pkg/mimir`)**
   - Go `flag` single-dash (`-target=ingester`, `-config.file=...`, `-config.expand-env`) and double-dash (`--target=ingester`) parsing.
   - Exact YAML schema matching `mimir.Config` (`KnownFields(true)` strict unknown-field rejection), `common:` block inheritance (`InheritCommonFlagValues`), `${VAR}` / `${VAR:default}` env expansion, `-version`, `-modules`, `-help`, `-help-all`.
   - Runtime config file reload (`-runtime-config.file`, `-runtime-config.reload-period`) and per-tenant overrides (`pkg/util/validation`).

2. **Cluster Membership, KV & Rings (`dskit/kv`, `dskit/ring`, `pkg/ingester`)**
   - KV backends: `memberlist` (TCP/UDP gossip wire protocol + Snappy/protobuf KV envelopes), `consul`, `etcd`, `multi`, `inmemory`.
   - Classic hash ring (`ring` key, `ring.Desc` protobuf, token generation, replication factor, zone awareness, lifecycler state machine: `PENDING`, `JOINING`, `ACTIVE`, `LEAVING`, `UNHEALTHY`, read-only state).
   - Partition ring (`ingester-partitions` key, `ring.PartitionRingDesc`, `PartitionTokenTable`, compartments, partition lifecycler & watchers).
   - Exact series & token hashing (`xxhash64` label fingerprinting, token Murmur/FNV/modulo math matching Go `dskit/ring` & Prometheus `model/labels` bit-for-bit).

3. **gRPC & HTTP Wire Protocols (`pkg/ingester/client`, `pkg/mimirpb`, `dskit/httpgrpc`)**
   - Full `cortex.Ingester` gRPC service: `Push`, `QueryStream`, `QueryExemplars`, `LabelValues`, `LabelNames`, `UserStats`, `AllUserStats`, `MetricsForLabelMatchers`, `MetricsMetadata`, `LabelNamesAndValues`, `LabelValuesCardinality`, `ActiveSeries`, `SearchLabelNames`, `SearchLabelValues`.
   - `httpgrpc.HTTP` gRPC service, `grpc.health.v1.Health`, gRPC keepalive/max-msg-size/compression (gzip & snappy), TLS/mTLS.
   - Multi-tenancy metadata header (`x-scope-orgid`, multi-tenant validation rules).
   - Exact error propagation: `google.rpc.Status`, `cortexpb.ErrorDetails` (`ErrorCause`, `Soft`, `RejectedSamples`), `httpgrpc` 4xx/5xx status codes, circuit-breaker & rate/series/inflight limit errors.
   - All HTTP admin/lifecycle endpoints: `/ready`, `/metrics`, `/ingester/ring`, `/ingester/partition-ring`, `/ingester/flush`, `/ingester/prepare_shutdown`, `/ingester/shutdown`, `/ingester/tenants`, `/config`, `/runtime_config`, `/services`.

4. **Kafka Ingest Storage (`pkg/storage/ingest`)**
   - Record versions v0, v1, and v2 (`Version` 4-byte big-endian header, `V2CommonSymbols` 64-entry reserved offset table, RW2 symbolised `WriteRequest` framing, Snappy/zstd/lz4/gzip Kafka compression).
   - Partition reader, multi-cluster reader, partition offset client/reader/watcher (`ListOffsets` `-1` / `-2` semantics), offset catalogue, local `offset` file persistence, strong read consistency offset checks.

5. **TSDB, WAL & Block Shipping (`prometheus/tsdb`, `pkg/storage/tsdb`, `pkg/ingester`)**
   - Per-tenant TSDB head: float samples, integer & float native histograms (including NHCB), exemplars, created timestamps, metadata store, active-series & owned-series tracking, label churn & cardinality counters.
   - Out-of-order (OOO) head chunks & WBL, on-disk WAL segments & checkpoints, mmap head chunks (`chunks_head`) byte-compatible with Prometheus TSDB so pod restarts across Go <-> Rust work cleanly.
   - Compaction to standard Prometheus/Thanos ULID blocks (`meta.json` with `thanos` metadata, `index`, `chunks/*`, `tombstones`) and upload via S3, GCS, Azure Blob, and local filesystem (`pkg/storage/bucket`).

6. **Observability**
   - Exact Prometheus metric names (`cortex_*`, `mimir_*`, `prometheus_tsdb_*`), label names, types, and histogram buckets; `cortex_config_hash`, `mimir_build_info`, `cortex_build_info`.
   - `logfmt` and `json` structured logging (`level`, `ts`, `caller`, `msg`, `org_id`, rate-limited logger); OpenTelemetry/Jaeger trace propagation.

---

## Performance Principles (Core Goal: Massive CPU & RAM Savings)

Compatibility defines **what** bytes cross the wire and disk; performance defines **how** we execute internally:

1. **Eliminate Per-Sample & Per-Label Heap Churn**
   - Never allocate `Vec<(String, String)>` per series on `Push` or Kafka batch decode.
   - Decode protobuf payloads (`cortexpb.WriteRequest`, RW2 `TimeSeriesRW2`, `QueryStreamResponse`) zero-copy from pooled `bytes::Bytes` / reusable per-worker scratch arenas directly into interned symbol IDs (`u32`) and striped TSDB series refs.
2. **Compact In-Memory TSDB Layout**
   - Intern label names and values per tenant in dense arena/symbol tables (`u32` IDs) instead of heap-allocated Go string headers + GC pointers.
   - Store head chunks (XOR float, varbit/float histogram, OOO) in pooled page buffers / mmap-backed segments without per-chunk heap object graphs.
   - Shard per-tenant series lookup tables (`stripe_size` power-of-two shards with `xxhash64` + `hashbrown` raw entry API) to avoid double hashing, cloning keys on hit, and cross-core lock contention.
3. **Zero-Allocation Streaming Reads**
   - `QueryStream`, `LabelNamesAndValues`, `ActiveSeries`, and `Search*` must stream batches using reusable encoding buffers without materializing full tenant result sets in RAM.
4. **Concurrency & Allocator Discipline**
   - Separate Tokio runtime/pools where appropriate: non-blocking network I/O (`tonic`/HTTP/Kafka/memberlist) vs bounded worker pools for CPU-heavy TSDB compaction/WAL replay.
   - Use lock-free atomics (`AtomicU64`/`AtomicI64` with explicit ordering) or sharded counters on hot metrics, limiters, and circuit breakers.
   - Use `tikv-jemallocator` on Linux builds to prevent glibc allocator fragmentation across tenant lifecycles, and expose heap/CPU profiling endpoints (`/debug/pprof/...` compatible where practical).
5. **Measure Against Go Mimir Continuously**
   - Every major subsystem (`mimirpb` decode, ring lookup, TSDB append, XOR/histogram chunk encode, `QueryStream`, Kafka v2 reader) gets Criterion microbenchmarks **plus** head-to-head Go vs Rust integration benchmarks measuring CPU-seconds/million-samples, p99 latency, and RSS bytes/million-active-series.

---

## Engineering, Conformance & Subagent Rules

### Differential Conformance Against Upstream Go Mimir (Mandatory)

Unit tests translated by hand are necessary but **never sufficient** — ported tests can accidentally encode the same misunderstanding as ported code. Every subsystem must be verified against the **real upstream Go code in `.ref/mimir`** (which has a complete offline `vendor/` tree) via our 3-tier conformance suite:

1. **Tier 1 — Go Package Oracle (`tests/go-oracle` -> `target/go-oracle`):**
   - A small Go CLI compiled with `-mod=vendor` directly importing `.ref/mimir/pkg/...`, `dskit/...`, and `prometheus/tsdb/...`.
   - Used by Rust `cargo test` from Wave 1 onward to assert **bit-for-bit parity** on:
     - FNV-1 sharding (`ShardByUser`, `ShardByMetricName`, `ShardByAllLabels`), `labels.Labels.Hash()` (`xxhash64`), ring & partition-ring token tables.
     - Protobuf & Kafka v0/v1/v2 RW2 wire bytes (`WriteRequest`, `QueryStreamResponse`, `ErrorDetails`, `ring.Desc`, `PartitionRingDesc`, `memberlist` KV envelopes).
     - Config YAML loading, env expansion, `common:` inheritance, CLI flag overrides, and validation errors.
     - Cross-engine TSDB persistence: Rust writes chunks/WAL/WBL/checkpoints/`chunks_head`/ULID blocks and Go `prometheus/tsdb` opens, replays, validates, and queries them; Go writes them and Rust opens, replays, and queries them.
2. **Tier 2 — Twin-Process Black-Box Differential Harness (`crates/mimir-conformance` + `target/go-mimir`):**
   - Compiles the unmodified Go `mimir` binary (`go build -mod=vendor ./cmd/mimir`) alongside Rust `mimir`.
   - Boots Go `-target=ingester` and Rust `-target=ingester` side-by-side with identical YAML + CLI flags, replays identical write/error/limit/OOO/histogram/exemplar/metadata workloads and all 13 `cortex.Ingester` read RPCs against both, and diffs gRPC responses, gRPC/`httpgrpc`/`ErrorDetails` error statuses, HTTP admin endpoints, and scraped `/metrics` (`cortex_ingester_*`, `prometheus_tsdb_*`).
3. **Tier 3 — Mixed Go + Rust Cluster & Kafka Harness:**
   - Runs mixed clusters (Go Distributor + Go Querier + mixed Go & Rust Ingesters over live `memberlist` gossip, plus `franz-go/pkg/kfake` + Go `ingest.Pusher` for Kafka `ingest_storage`) verifying quorum replication, rolling WAL restarts, ring join/leave, and block shipping readable by Go Mimir.

### General Rules

- **No silent stubs or fake implementations.** Never return `Ok(())` or dummy empty responses for unimplemented behavior to make something "look green". If a target or optional backend is not yet wired in the current milestone, return an explicit error and track it in `PORT_STATUS.md`.
- **Port upstream tests alongside code.** When porting `pkg/foo/bar.go`, port the test cases from `.ref/mimir/pkg/foo/bar_test.go` in the same task, plus add differential Go-vs-Rust tests for wire/disk formats and hashes.
- **Verification before completion:** Every task must pass:
  - `cargo check --workspace --all-targets`
  - `cargo test --workspace`
  - `cargo clippy --workspace --all-targets -- -D warnings`
- **Minimal dependencies, maximum control on hot paths:** Prefer battle-tested crates (`tokio`, `tonic`/`prost`, `hyper`/`axum`, `rdkafka` or pure-Rust Kafka protocol where needed for exact `ListOffsets`/headers control, `object_store`, `prometheus` / custom zero-overhead registry, `xxhash-rust`, `snap`, `zstd`, `lz4_flex`, `flate2`, `memmap2`, `parking_lot`, `dashmap`/`hashbrown`), and hand-roll hot wire codecs (`WriteRequest`, TSDB WAL/chunks/index, memberlist KV framing) wherever generic derive macros add allocations or break Go/gogo wire quirks.
