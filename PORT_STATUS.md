# `mimir-rs` Port Status

Upstream reference: `grafana/mimir` `main` @ `06de3ebd` (`.ref/mimir`)

| Wave | Task | Crate / Scope | Status | Notes |
|------|------|---------------|--------|-------|
| W0 | Workspace & Go Oracle Bootstrap | `Cargo.toml`, `crates/*`, `tests/go-oracle` | Done | Workspace + `target/go-mimir` & `target/go-oracle` verified |
| W1-A | Wire Protocols & Zero-Copy Codecs | `crates/mimir-proto` | Done | `cortexpb`, `ingesterpb` (14 RPCs), `ring`, `partition_ring`, `kv`, `httpgrpc`, RW2 + `V2CommonSymbols`, FNV-1, Snappy gRPC |
| W1-B | Shared Primitives & Services | `crates/mimir-common` | Done | Tenant IDs, `stringlabels` `xxhash64`, `Service` FSM, logger, `globalerror`/`ErrorDetails`, limiters, `shutdownmarker`, `activitytracker` |
| W1-C | CLI Flags, YAML Config & Overrides | `crates/mimir-config` | Done | Go `-flag`/`--flag` parser, strict YAML + env expansion + `common:` inheritance, runtime config & per-tenant `Overrides` |
| W1-D | TSDB Encodings, Chunks & WAL | `crates/mimir-tsdb` (part 1) | Done | `u32` symbol table, `LabelMatcher`, XOR/Histogram/FloatHistogram/OOO chunks, WAL/WBL types 1..14, `chunks_head` mmap |
| W2-A | TSDB Head, Postings & Block Compaction | `crates/mimir-tsdb` (part 2) | Done | Striped `Head`, postings, exemplars, metadata, `ChunkQuerier`, ULID block writer/reader verified vs Go `tsdb` |
| W2-B | KV, Memberlist Gossip & Hash/Partition Rings | `crates/mimir-ring` | Done | `inmemory`/`consul`/`etcd`/`multi`/`memberlist` KV, Classic `Ring` + `Lifecycler`, `PartitionRing`, live gossip verified vs `go-mimir` |
| W2-C | Bucket Shipper & Kafka Ingest Storage | `crates/mimir-storage` | Done | `BucketClient`, `Shipper` (`mimir.shipper.json`), pure-Rust Kafka v0-v2 client verified vs `kfake` |
| W3-A | Ingester Core & All 14 gRPC RPCs | `crates/mimir-ingester` | Done | `UserTSDB`, zero-copy `Push`, Kafka consumer loop, circuit breaker, limiters, active/owned series, all 14 `cortex.Ingester` RPCs, HTTP admin, metrics |
| W3-B | `mimir` Binary & Module Manager | `crates/mimir-cmd` | Done | `-target=ingester`, HTTP+gRPC server (`TCP_NODELAY`), `httpgrpc`, `/ready`, `/metrics`, `/services`, `/config`, `tikv-jemallocator` |
| W4 | Differential Go-vs-Rust QA & Perf Bench | `crates/mimir-conformance`, `examples/bench_vs_go.rs` | Done | Twin-process classic Push (14 RPCs + `ErrorDetails` + HTTP + ULID cross-verify), mixed Go+Rust memberlist ring, `kfake` Kafka `ingest_storage` parity; 100k series x 20 rounds benchmark: 1.67x throughput, 36.7% less ingest CPU, 37.2% less RSS / 41.3% less post-ingest RSS |

## Verified Wave 4 Conformance & Head-to-Head Benchmark Results

### Differential Conformance (`crates/mimir-conformance/tests/w4_twin_process_conformance.rs`)
- `twin_process_classic_push_all_14_rpcs_and_errors_parity`: Twin-process `go-mimir` vs `target/release/mimir` parity across `Push` (floats, OOO floats, `created_timestamp`, int/float/NHCB histograms, exemplars, metadata), all 13 read RPCs, gRPC/`ErrorDetails` error parity (unsorted labels, duplicate timestamp, sample too old, per-user series limit), HTTP endpoints (`/ready`, `/metrics`, `/api/v1/status/buildinfo`, `/ingester/ring`, `/ingester/tenants`, `/httpgrpc.HTTP/Handle`), `/ingester/flush?wait=true`, and bidirectional Go <-> Rust TSDB ULID block cross-verification.
- `mixed_cluster_go_and_rust_ingesters_memberlist_ring`: Live mixed Go + Rust cluster over `memberlist` (`replication_factor: 2`), ring convergence to 2 `ACTIVE` ingesters, push/query parity, and clean Rust `/ingester/shutdown` ring exit.
- `twin_process_kafka_ingest_storage_parity`: Twin-process `go-mimir` vs `target/release/mimir` consuming v2 RW2 records from `SpawnedKfakeBroker` (`ingest_storage.enabled: true`) with strong read consistency (`__consistency_level__: strong`, `__consistency_offsets__`) and 100% matching `QueryStream`, `LabelNames`, `LabelValues`, and `QueryExemplars`.

### Head-to-Head Benchmark (`just bench-vs-go`: 100k float + 2k histogram series, 5 tenants, 20 rounds = 2.04M samples, concurrency=8, + 200 read RPCs)
| Metric | Go `go-mimir` | Rust `mimir-rs` | Improvement (Go vs Rust) |
|--------|---------------|-----------------|--------------------------|
| Ingest wall / throughput | 3.117 s (654,511 samples/s) | 1.870 s (1,091,051 samples/s) | **1.67x** faster |
| Push latency (p50 / p99) | 7.73 ms / 63.40 ms | 4.34 ms / 51.30 ms | **1.24x** p99 |
| CPU after ingest | 10.700 s (5.245 CPU-s/1M) | 6.770 s (3.319 CPU-s/1M) | **1.58x** (**36.7% less CPU**) |
| RSS after ingest | 524.05 MiB | 307.84 MiB | **1.70x** (**41.3% less RSS**) |
| QueryStream latency (p50 / p99) | 6.73 ms / 16.32 ms | 5.67 ms / 11.12 ms | **1.47x** p99 |
| Total CPU (ingest + queries) | 11.880 s | 7.890 s | **1.51x** (**33.6% less CPU**) |
| Final RSS / Peak HWM | 610.23 MiB | 383.05 MiB | **1.59x** (**37.2% less RAM**) |
