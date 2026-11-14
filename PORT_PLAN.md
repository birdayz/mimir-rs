# `mimir-rs` Architecture & Mechanical Port Plan

Reference upstream commit: `grafana/mimir` `main` @ `06de3ebd` (`.ref/mimir`, including full vendored `dskit`, `prometheus`, `objstore`, `memberlist`, and `franz-go` under `.ref/mimir/vendor`).

## 1. Crate Graph & Ownership Boundaries

All crates live under `crates/` in a single Cargo workspace producing the drop-in `mimir` binary:

1. **`crates/mimir-proto`**
   - Upstream: `pkg/mimirpb/*`, `pkg/ingester/client/ingester.proto`, `dskit/ring/ring.proto`, `dskit/ring/partition_ring_desc.proto`, `dskit/kv/memberlist/kv.proto`, `dskit/httpgrpc/httpgrpc.proto`, `dskit/grpcutil/error_details.proto`, `google.rpc.Status`.
   - Responsibilities:
     - Exact wire-compatible Protobuf message types (`prost` + custom zero-copy arena decoders/encoders for `cortexpb.WriteRequest`, RW2 `TimeSeriesRW2` + `FastSymbolsTable` / `CommonSymbols` offset 64, `QueryStreamResponse`, `TimeSeriesChunk`).
     - Exact `fnv32` (`HashNew32`/`HashAdd32`: **FNV-1** `h = h.wrapping_mul(16777619) ^ b`, NOT FNV-1a!) & `ShardByUser` / `ShardByMetricName` / `ShardByAllLabels` (`pkg/mimirpb/fnv.go`, `series_sharding.go`).
     - gRPC `tonic` service definitions + Snappy (`"snappy"`, `dskit/grpcencoding/snappy`) and Gzip compression codecs.

2. **`crates/mimir-common`**
   - Upstream: `dskit/services`, `dskit/tenant`, `dskit/backoff`, `dskit/limiter`, `dskit/concurrency`, `pkg/util/log`, `pkg/util/globalerror`, `pkg/util/validation` (tenant IDs, label/sample validation), `pkg/util/activitytracker`, `pkg/util/shutdownmarker`, `pkg/util/workerpool`, `pkg/util/reactivelimiter`, `prometheus/model/labels` hashing.
   - Responsibilities:
     - Multi-tenant context (`x-scope-orgid` single & `|`-federated parsing, strict tenant ID character/length validation).
     - Exact Prometheus `labels.Labels.Hash()` (`xxhash64` over `name \xff value \xff` with fallback seed when length >= threshold — verified bit-for-bit against `.ref/mimir/vendor/github.com/prometheus/prometheus/model/labels/`).
     - `Service` lifecycle state machine (`New` -> `Starting` -> `Running` -> `Stopping` -> `Terminated` / `Failed`) + `ServiceManager`.
     - Structured `logfmt` & `json` logger with token-bucket rate limiting, `globalerror` / `httpgrpc` / `cortexpb.ErrorDetails` error wrapping, `shutdownmarker` & `activitytracker` file persistence.

3. **`crates/mimir-config`**
   - Upstream: `cmd/mimir/main.go`, `pkg/mimir/mimir.go`, `pkg/ingester/ingester.go` (`Config`), `pkg/storage/tsdb/config.go`, `pkg/storage/bucket/config.go`, `pkg/storage/ingest/config.go`, `pkg/util/validation/limits.go`, `pkg/util/runtimeconfig`.
   - Responsibilities:
     - Go `flag.FlagSet` compatible CLI parser supporting `-flag=val`, `-flag val`, `--flag=val`, `--flag val`, `-config.file`, `-config.expand-env`, `-version`, `-modules`, `-help`, `-help-all`.
     - Strict YAML parser rejecting unknown fields (`KnownFields(true)`), `${VAR}` / `${VAR:default}` expansion, `common:` block inheritance (`InheritCommonFlagValues`), Go duration (`1h30m`, `500ms`) and humanize size parsers.
     - Complete `Config` & `LimitsConfig` defaults, `Validate()`, runtime-config YAML hot reload (`RuntimeConfigManager`), and per-tenant `Overrides`.

4. **`crates/mimir-tsdb`**
   - Upstream: `vendor/github.com/prometheus/prometheus/tsdb/*`, `pkg/storage/tsdb/*`.
   - Responsibilities:
     - Per-tenant `u32` symbol arena interner + compact sorted `LabelSet` + `LabelMatcher` (`=`, `!=`, `=~`, `!~` full-string anchored regex + fast prefix/set literal optimizations).
     - Bit-exact Prometheus chunk encodings (`chunkenc`): `EncXOR` (`0x01`), `EncHistogram` (`0x02`), `EncFloatHistogram` (`0x03`), plus out-of-order (`OOO`) chunk buffer & merge/cut into XOR/Histogram chunks.
     - Bit-exact Prometheus WAL & WBL (`tsdb/wlog`): 32KiB page framing, CRC32 Castagnoli (`0x82f63b78`), Snappy compression record flags, all record types (`Series`, `Samples`, `Tombstones`, `Exemplars`, `HistogramSamples`, `FloatHistogramSamples`, `Metadata`), segment rotation, checkpointing (`checkpoint.XXXXXXXX`), and parallel crash replay.
     - Bit-exact `chunks_head` mmap head chunk files (`HeadChunkFile` header magic `0x0130BC91`, version, series ref, mint/maxt, encoding, CRC32C).
     - Striped `Head` (`stripe_size` shards, `hashbrown` raw entry + `xxhash64`, postings inverted index, active/in-memory series tracking, circular exemplar buffer, metric metadata store, OOO window enforcement).
     - Head `ChunkQuerier` / `Querier`, ULID block compaction (`index` v2 binary format magic `0xBAAAD700`, `chunks/000001` magic `0x85BD40DD`, `tombstones`, `meta.json` + Thanos `thanos` block metadata extension).

5. **`crates/mimir-ring`**
   - Upstream: `dskit/kv/*` (`inmemory`, `consul`, `etcd`, `multi`, `memberlist`), `dskit/ring/*`.
   - Responsibilities:
     - `KVClient` trait (`CAS`, `Get`, `WatchKey`, `WatchPrefix`, `List`, `Delete`) + codecs.
     - `memberlist` KV gossip compatible with `hashicorp/memberlist` + `dskit/kv/memberlist` (`kv.proto` `KeyValuePair`/`KeyValueStore`, TCP push-pull state sync + UDP/TCP compound/ping/ack/gossip packets, Snappy stream framing).
     - Classic `Ring` (`ring.Desc` CAS merge, token lookup, replication set with zone-awareness & unhealthy/HeartbeatTimeout checks, `BasicLifecycler` / `Lifecycler`, `/ingester/ring` HTML & JSON status/forget handler).
     - `PartitionRing` (`PartitionRingDesc`, `PartitionTokenTable` with `MaxDerivedTokenPartitions`, `PartitionLifecycler`, `PartitionRingWatcher`, `PartitionInstanceRing`, `/ingester/partition-ring`).

6. **`crates/mimir-storage`**
   - Upstream: `pkg/storage/bucket/*`, `pkg/ingester/shipper.go`, `pkg/storage/ingest/*`.
   - Responsibilities:
     - Object storage `BucketClient` (`filesystem`, `s3`, `gcs`, `azure`) with tenant prefixing + `Shipper` (`thanos.shipper.json` meta tracking, block upload, `Sync()`).
     - Kafka `ingest_storage` client: record versions v0, v1, v2 (`Version` 4-byte BE header, `V2CommonSymbols` 64-entry offset table, RW2 `WriteRequest` payload), partition `Reader`, `MultiClusterReader`, `PartitionOffsetClient` / `Reader` / `Watcher` (`ListOffsets` `-1` latest / `-2` earliest), `OffsetCatalogue`, local `offset` file persistence.

7. **`crates/mimir-ingester`**
   - Upstream: `pkg/ingester/*`.
   - Responsibilities:
     - `Ingester` service + per-tenant `UserTSDB` lifecycle (lazy open, parallel WAL replay, idle close, compartments).
     - `Push` pipeline: inflight & byte limiters, circuit breaker (latency & consecutive failure modes), reactive limiter, instance limits, sample/label/histogram/exemplar/metadata validation, ring ownership check (`owned_series.go`), active-series (`activeseries/*`) & label-churn (`label_churn.go`) tracking, metric counter, TSDB append.
     - Kafka `ingest_storage` partition consumer integration + strong read consistency (`WaitForOffset` / `QueryRequest` offset headers).
     - All 13 `cortex.Ingester` gRPC RPCs (`Push`, `QueryStream`, `QueryExemplars`, `LabelValues`, `LabelNames`, `UserStats`, `AllUserStats`, `MetricsForLabelMatchers`, `MetricsMetadata`, `LabelNamesAndValues`, `LabelValuesCardinality`, `ActiveSeries`, `SearchLabelNames`, `SearchLabelValues`).
     - Compaction & block shipping loops (periodic head compaction, early compaction, idle TSDB compaction with jitter, `/ingester/flush`, `/ingester/prepare_shutdown`, `/ingester/shutdown`, `/ingester/tenants`).
     - Complete `cortex_ingester_*` & `prometheus_tsdb_*` metrics registration.

8. **`crates/mimir-cmd`**
   - Upstream: `cmd/mimir/main.go`, `pkg/mimir/mimir.go`, `pkg/mimir/modules.go`, `pkg/api/*`, `dskit/server/*`.
   - Responsibilities:
     - `mimir` binary entrypoint with `tikv-jemallocator` on Linux.
     - Module DAG manager (`-target=ingester`, `-modules`, future targets).
     - Dual HTTP (`axum`/`hyper`) + gRPC (`tonic` + `httpgrpc.HTTP` + `grpc.health.v1.Health`) server, `/ready`, `/metrics`, `/services`, `/config`, `/runtime_config`, graceful SIGTERM/SIGINT lifecycle.

9. **`crates/mimir-conformance` + `tests/go-oracle`**
   - Differential conformance & benchmark harness against `.ref/mimir` (`go build -mod=vendor`):
     - **Tier 1 (`tests/go-oracle` -> `target/go-oracle`):** Go binary importing `.ref/mimir/pkg/*`, `dskit/*`, and `prometheus/tsdb/*` directly; invoked from Rust tests starting in Wave 1 to verify bit-exact hashes, protobuf/RW2/Kafka v2 codecs, config parsing, and bidirectional Go<->Rust TSDB WAL/checkpoint/head-chunk/ULID-block compatibility.
     - **Tier 2 (`crates/mimir-conformance` Twin-Process Oracle):** Boots unmodified Go `mimir -target=ingester` (`target/go-mimir`) and Rust `mimir -target=ingester` side-by-side with identical config/flags, mirrors write/limit/error workloads + all 13 `cortex.Ingester` gRPC read RPCs + HTTP lifecycle routes, and diffs responses, `ErrorDetails`, and `/metrics`.
     - **Tier 3 (Mixed Cluster & CPU/RSS Benchmarks):** Mixed Go+Rust `memberlist` ring cluster (`RF=3` with Go Distributor & Querier), in-process `kfake` Kafka `ingest_storage` Go-Pusher -> Rust-Ingester test, and head-to-head CPU-seconds + RSS memory benchmarks.

---

## 2. Execution Waves

- **Wave 0:** Workspace scaffold (`Cargo.toml` + 8 crates compiling cleanly).
- **Wave 1 (Parallel Foundation Crates):**
  - `W1-A`: `mimir-proto` (all protos, RW2 + `V2CommonSymbols`, FNV-1 sharding, Snappy/Gzip gRPC codecs, zero-copy `WriteRequest` & `QueryStreamResponse` codecs).
  - `W1-B`: `mimir-common` (tenant, exact `xxhash64` label hash, `Service` state machine, logger, `globalerror`/`httpgrpc`/`ErrorDetails`, limiters, shutdownmarker/activitytracker).
  - `W1-C`: `mimir-config` (Go single/double-dash flag parser, strict YAML + env expansion + `common` inheritance, full ingester/server/storage/ring/limits configs, runtime-config & `Overrides`).
  - `W1-D`: `mimir-tsdb` Part 1 (symbol interner, `LabelSet`, `LabelMatcher`, XOR/Histogram/FloatHistogram/OOO chunk codecs, WAL/WBL + checkpoints + `chunks_head` mmap format).
- **Wave 2 (Parallel Core Engines):**
  - `W2-A`: `mimir-tsdb` Part 2 (striped `Head`, postings, exemplars, metadata, `ChunkQuerier`, ULID block compaction `index` v2 + `chunks` + `meta.json` Thanos ext).
  - `W2-B`: `mimir-ring` (KV stores incl. `memberlist` TCP/UDP gossip + CAS merge, Classic `Ring` + `Lifecycler`, `PartitionRing` + `PartitionTokenTable` + `PartitionLifecycler`, HTTP ring status/forget).
  - `W2-C`: `mimir-storage` (`BucketClient` fs/s3/gcs/azure + `Shipper`, Kafka `ingest_storage` v0/v1/v2 reader/writer + offset watcher/catalogue/file).
- **Wave 3 (Ingester & Binary Assembly):**
  - `W3-A`: `mimir-ingester` (full `pkg/ingester` RPCs, `UserTSDB`, `Push`, Kafka consumer integration, active/owned series, circuit breaker, limiters, HTTP admin routes, all `cortex_ingester_*` metrics).
  - `W3-B`: `mimir-cmd` (`mimir` binary, module manager, HTTP+gRPC server, `httpgrpc`, `/ready`, `/metrics`, `/config`, `/services`, jemalloc).
- **Wave 4 (Differential Go-vs-Rust QA & Performance Benchmarks):**
  - Automated interop tests against compiled Go `.ref/mimir` (WAL/block cross-read, gRPC Push+QueryStream interop, Memberlist mixed-cluster gossip, Kafka v2 ingest_storage interop).
  - Head-to-head CPU & RSS benchmark harness (`mimir` Go vs `mimir` Rust under sustained multi-tenant high-cardinality ingest + query load).
