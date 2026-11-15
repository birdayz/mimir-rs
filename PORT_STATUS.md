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
| W4 | Differential Go-vs-Rust QA & Perf Bench | `crates/mimir-conformance`, `examples/bench_vs_go.rs` | Done | Twin-process classic Push (14 RPCs + `ErrorDetails` + HTTP + ULID cross-verify), mixed Go+Rust memberlist ring, `kfake` Kafka `ingest_storage` parity; 100k series x 20/80 rounds benchmarks: up to **2.91x** `QueryStream` p99, **40.4%–51.6% less RSS**, **20.0%–26.7% less total CPU** |
| P1-A / P1-B | Production RAM Diet, Postings Cache & Sharded Kafka `PusherConsumer` | `crates/mimir-tsdb`, `crates/mimir-storage`, `crates/mimir-ingester` | Done | Compact `SeriesEntry`/`SeriesState`/`ActiveSeries`, `XorChunk` 32B initial alloc, `u32` `MemPostings`, lock-free-for-readers metadata fast path, `PostingsForMatchersCache`, parallel WAL & block replay, `PusherConsumer` + `ParallelStoragePusher` + pipelined Kafka fetch |
| P2 | Production Kafka `ingest_storage` Benchmark (`2M` Series) & Grind | `examples/prod_bench.rs`, `just prod-bench`, `crates/mimir-tsdb`, `crates/mimir-storage`, `crates/mimir-ingester`, `crates/mimir-cmd` | Done | `2,000,000`-series 4-phase Kafka v2 Zstd + 4 local ULID blocks + bimodal reads + live compaction + `SIGKILL` restart benchmark: **59.7% less heap RAM (`2.48x`)**, **33.3% less steady CPU (`1.50x`)**, **1.73x** heavy `QueryStream` p99, **2.30x** `LabelNamesAndValues`, **2.07x** `LabelValuesCardinality`, **1.96x** compaction peak `RssAnon` reduction |

## Verified Wave 4 Conformance & Head-to-Head Benchmark Results

### Differential Conformance (`crates/mimir-conformance/tests/w4_twin_process_conformance.rs`)
- `twin_process_classic_push_all_14_rpcs_and_errors_parity`: Twin-process `go-mimir` vs `target/release/mimir` parity across `Push` (floats, OOO floats, `created_timestamp`, int/float/NHCB histograms, exemplars, metadata), all 13 read RPCs, gRPC/`ErrorDetails` error parity (unsorted labels, duplicate timestamp, sample too old, per-user series limit), HTTP endpoints (`/ready`, `/metrics`, `/api/v1/status/buildinfo`, `/ingester/ring`, `/ingester/tenants`, `/httpgrpc.HTTP/Handle`), `/ingester/flush?wait=true`, and bidirectional Go <-> Rust TSDB ULID block cross-verification.
- `mixed_cluster_go_and_rust_ingesters_memberlist_ring`: Live mixed Go + Rust cluster over `memberlist` (`replication_factor: 2`), ring convergence to 2 `ACTIVE` ingesters, push/query parity, and clean Rust `/ingester/shutdown` ring exit.
- `twin_process_kafka_ingest_storage_parity`: Twin-process `go-mimir` vs `target/release/mimir` consuming v2 RW2 records from `SpawnedKfakeBroker` (`ingest_storage.enabled: true`) with strong read consistency (`__consistency_level__: strong`, `__consistency_offsets__`) and 100% matching `QueryStream`, `LabelNames`, `LabelValues`, and `QueryExemplars`.

### Head-to-Head Benchmark (`just bench-vs-go`: 100k float + 2k histogram series, 5 tenants, 20 rounds = 2.04M samples, concurrency=8, + 200 read RPCs)
| Metric | Go `go-mimir` | Rust `mimir-rs` | Improvement (Go vs Rust) |
|--------|---------------|-----------------|--------------------------|
| Ingest wall / throughput | 1.854 s (1,100,144 samples/s) | 1.741 s (1,171,549 samples/s) | **1.06x** faster |
| Push latency (p50 / p99) | 4.91 ms / 41.08 ms | 4.10 ms / 45.22 ms | **1.20x** p50 / 0.91x p99 |
| CPU after ingest | 8.780 s (4.304 CPU-s/1M) | 6.300 s (3.088 CPU-s/1M) | **1.39x** (**28.2% less CPU**) |
| RSS after ingest | 536.29 MiB | 301.50 MiB | **1.78x** (**43.8% less RSS**) |
| QueryStream latency (p50 / p99) | 3.49 ms / 17.76 ms | 4.14 ms / 6.10 ms | **2.91x** p99 |
| Total CPU (ingest + queries) | 9.800 s | 7.180 s | **1.36x** (**26.7% less CPU**) |
| Final RSS / Peak HWM | 583.89 MiB | 347.77 MiB | **1.68x** (**40.4% less RAM**) |

### Head-to-Head Benchmark (`just bench-vs-go --rounds 80`: 100k float + 2k histogram series, 5 tenants, 80 rounds = 8.16M samples, concurrency=8, + 200 read RPCs)
| Metric | Go `go-mimir` | Rust `mimir-rs` | Improvement (Go vs Rust) |
|--------|---------------|-----------------|--------------------------|
| Ingest wall / throughput | 6.233 s (1,309,236 samples/s) | 6.105 s (1,336,549 samples/s) | **1.02x** faster |
| Push latency (p50 / p99) | 4.75 ms / 27.25 ms | 4.38 ms / 27.60 ms | **1.08x** p50 / 0.99x p99 |
| CPU after ingest | 31.710 s (3.886 CPU-s/1M) | 25.430 s (3.116 CPU-s/1M) | **1.25x** (**19.8% less CPU**) |
| RSS after ingest | 726.20 MiB | 334.57 MiB | **2.17x** (**53.9% less RSS**) |
| Query wall (200x RPCs) | 0.167 s | 0.158 s | **1.06x** faster |
| QueryStream latency (p50 / p99) | 4.12 ms / 10.96 ms | 3.95 ms / 5.91 ms | **1.04x** p50 / **1.85x** p99 |
| Total CPU (ingest + queries) | 32.820 s | 26.250 s | **1.25x** (**20.0% less CPU**) |
| Final RSS / Peak HWM | 754.72 MiB | 364.96 MiB | **2.07x** (**51.6% less RAM**) |

### Production Kafka `ingest_storage` Benchmark (`just prod-bench --series 2000000 --steady-secs 30`)
Workload: `2,000,000` active series (10 K8s labels, `prod` 99.5% + `preprod` 0.5%, `1,600` metric families), Kafka v2 RW2 (`82.4 B/sample` uncompressed, Zstd `10.32x` wire compression, `500` samples/record, `95` metadata/record), `4` historical 2h ULID blocks on disk, steady `327k samples/s` + `0.07%` OOO + `1%/h` churn + bimodal concurrent reads (`6.35/s` `QueryStream`, `LabelNamesAndValues`, `LabelValuesCardinality`, `UserStats`), live head compaction, and `SIGKILL` cold restart with 5s Kafka backlog (`1.635M` samples).

| Phase / Metric | Go `go-mimir` | Rust `mimir-rs` | Improvement (Go vs Rust) |
|----------------|---------------|-----------------|--------------------------|
| Phase 1 Catch-up (`2.4M` samples, `1.2` chunks/series) | 13.37 s (179,446 samples/s) | 12.84 s (186,877 samples/s) | **1.04x** faster |
| Phase 1 `RssAnon` (`B/series`) | 2,908.5 MiB (`1,525 B/series`) | 1,850.2 MiB (`970 B/series`) | **1.57x** (**36.4% less heap**) |
| Phase 2 Steady CPU (`µs / ingested sample`, reads ON) | 6.63 µs/sample | 4.42 µs/sample | **1.50x** (**33.3% less CPU**) |
| Phase 2 Steady `RssAnon` (`B/series`) | 5,334.7 MiB (`2,663 B/series`) | 2,152.1 MiB (`1,079 B/series`) | **2.48x** (**59.7% less heap RAM**) |
| `QueryStream` small (`~7 series`) p50 / p99 | 0.58 ms / 3.02 ms | 0.55 ms / 15.78 ms | **1.05x** p50 |
| `QueryStream` heavy (`~28k series`) p50 / p99 | 641.34 ms / 899.13 ms | 514.18 ms / 521.12 ms | **1.25x** p50 / **1.73x** p99 |
| `LabelNamesAndValues` p50 | 1,767.28 ms | 768.54 ms | **2.30x** faster |
| `LabelValuesCardinality` p50 | 773.06 ms | 374.25 ms | **2.07x** faster |
| `UserStats` p50 | 0.48 ms | 0.42 ms | **1.14x** faster |
| Phase 3 Head Compaction wall | 37.88 s | 32.34 s | **1.17x** faster |
| Phase 3 Peak `RssAnon` (sawtooth) | 6,609.3 MiB (+1,274.7 MiB) | 3,365.7 MiB (+1,215.6 MiB) | **1.96x** (**49.1% less peak RAM**) |
| Phase 4 `SIGKILL` `/ready` / `0-lag` catch-up | 15.17 s / 34.51 s | 15.89 s / 32.36 s | **1.07x** faster catch-up |
| Phase 4 Post-restart `RssAnon` | 5,696.8 MiB | 3,311.1 MiB | **1.72x** (**41.9% less heap**) |

#### Go `go-mimir` Heap Decomposition (`/debug/pprof/heap -inuse_space` after Phase 1 preload, 2M series)
- **Go live heap (`inuse_space`)**: `1,415.42 MB` vs OS `RssAnon = 2,908.5 MiB` (**1,493 MiB `GOGC=100` GC headroom**, ~2.05x live heap; grows to `5,334.7 MiB` during steady state!).
- **Top Go `inuse_space` allocators**: `stringslite.Clone` (`260.57 MB`, `18.41%`), `chunkenc.NewXORChunk` (`225.02 MB`, `15.90%`), `tsdb.newMemSeries` (`200.04 MB`, `14.13%`), `activeseries.(*seriesStripe).findAndUpdateOrCreateEntryForSeries` (`139.89 MB`, `9.88%`), `zstd.(*sequenceDecs).executeSeq` (`115.68 MB`, `8.17%`), `tsdb/index.appendWithExponentialGrowth` (`111.08 MB`, `7.85%`), `mimirpb.newTimeSeries` (`80.54 MB`, `5.69%`), `tsdb.(*memSeries).cutNewHeadChunk` (`73.00 MB`, `5.16%`).
