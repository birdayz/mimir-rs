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
| P2 | Production Kafka `ingest_storage` Benchmark (`2M` Series) & Grind | `examples/prod_bench.rs`, `just prod-bench`, `crates/mimir-tsdb`, `crates/mimir-storage`, `crates/mimir-ingester`, `crates/mimir-cmd` | Done | `2,000,000`-series 4-phase Kafka v2 Zstd + 4 local ULID blocks + bimodal reads + live compaction + `SIGKILL` restart benchmark |
| P3 | Streaming Compaction, 64-Stripe Postings, Parallel WAL Decode & Isolated RPC Runtime | `crates/mimir-tsdb`, `crates/mimir-storage`, `crates/mimir-ingester`, `crates/mimir-cmd` | Done | Streaming Head compaction + zero-copy `DeletedSeries` (`+124.6 MiB` sawtooth, **2.92x** less peak RAM), 64-stripe `MemPostings` + parallel WAL decompression/decode (`13.91 s` `SIGKILL /ready`, **1.08x** faster than Go), inline `<= 256`-series `QueryStream` on dedicated RPC runtime (**0.46 ms p50 / 1.01 ms p99**), **35.0% less steady CPU (`1.54x`)**, **59.7% less steady heap (`2.48x`)** |
| P4 | CPU Obliteration: O(1) Chunk Appenders, Zero-Alloc Sharded Kafka Pipeline, `pending_commits` Fast Path & O(1) Block Symbol Decode | `crates/mimir-tsdb`, `crates/mimir-ingester`, `crates/mimir-conformance` | Done | Cached `XorAppendState` / `Xor2AppendState` & histogram appender state (`O(1)` appends), zero-alloc single-hash `IngesterShardedBatchConsumer` (2,048-sample shard commits), `pending_commits == 0` `HeadAppender` fast path + pooled scratch + `FastU64BuildHasher` in `ActiveSeries`/`MetricCounter`, once-at-open UTF-8 validation + direct `Labels` encode in `IndexReader`: **37.0% less pure-write CPU (`3.15 vs 5.00 µs/sample`)**, **43.1% less steady CPU with reads (`3.75 vs 6.60 µs/sample`)**, **2.88x** `QueryStream` heavy p99 (`277.39 vs 800.07 ms`) |
| P5 | Persistent `ScopedWorkerPool`, Parallel Zstd Kafka Batch Decode, `Head::open` WAL Parallelism & `/debug/pprof/profile` Hot-Path Polish | `crates/mimir-common`, `crates/mimir-proto`, `crates/mimir-tsdb`, `crates/mimir-storage`, `crates/mimir-ingester`, `crates/mimir-cmd` | Done | Persistent `ScopedWorkerPool` (zero `pthread_create`/`munmap`), parallel multi-batch Zstd RecordBatch decode + thread-local `zstd::bulk::Decompressor`, `Head::open` `wal_replay_concurrency == 0` -> `available_parallelism()`, `/debug/pprof/profile` (`pprof` `SIGPROF`), 4096-stripe `ActiveSeries` without `cmpxchg`, fast `<255` `Labels::eq_sorted_pairs`, 1-byte varint fast path in `zerocopy`: **42.7% less pure-write CPU (`2.87 vs 5.01 µs/sample`)**, **51.0% less steady CPU with reads (`3.54 vs 7.24 µs/sample`, `2.04x`)**, **54.1% less steady `RssAnon` (`2.18x`)**, **3.23x** `QueryStream` heavy p99 (`271.71 vs 876.88 ms`) |
| P6 | Lock-Free `NanosSlab`, Software Prefetching, Single-Pass RW2 UTF-8, Single-Lock `HeadAppender` & Parallel WAL `Series` Replay | `crates/mimir-proto`, `crates/mimir-tsdb`, `crates/mimir-ingester` | Done | Lock-free 65536-slot `NanosSlab` + `SeriesEntry::active_cache` (0 `RwLock` / 0 `HashMap` on active float updates), `_mm_prefetch`/`prfm` pipelined `Head::resolve_prehashed_batch` & `HeadAppender::commit`, pre-validated RW2 `&str` label iterator (`labels_str()`), single-lock `HeadAppender` (`in_order_max_time` + atomic `pending_commits`) + `take_entry` zero extra `Arc` churn, parallel WAL `Series` hash/insert/`MemPostings` replay: **48.0% less pure-write CPU (`2.19 vs 4.22 µs/sample`, `1.92x`)**, **55.7% less steady CPU with reads (`2.86 vs 6.47 µs/sample`, `2.26x`)**, **53.2% less steady `RssAnon` (`2.14x`)**, **4.33x** `QueryStream` heavy p99 (`289.78 vs 1,254.84 ms`), **1.16x** faster `SIGKILL /ready` (`12.09 vs 14.03 s`) |

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
Workload: `2,000,000` active series (10 K8s labels, `prod` 99.5% + `preprod` 0.5%, `1,600` metric families), Kafka v2 RW2 (`82.4 B/sample` uncompressed, Zstd `10.32x` wire compression, `500` samples/record, `95` metadata/record), `4` historical 2h ULID blocks on disk, steady `327k samples/s` + `0.07%` OOO + `1%/h` churn + bimodal concurrent reads (`QueryStream`, `LabelNamesAndValues`, `LabelValuesCardinality`, `UserStats`), live head compaction, and `SIGKILL` cold restart with 5s Kafka backlog (`1.635M` samples).

#### Pure Write Path (`--series 2000000 --steady-secs 30 --no-reads`)
| Phase / Metric | Go `go-mimir` | Rust `mimir-rs` | Improvement (Go vs Rust) |
|----------------|---------------|-----------------|--------------------------|
| Phase 1 Catch-up (`2.4M` samples, `1.2` chunks/series) | 11.48 s (209,090 samples/s, 29.36 s CPU) | 10.76 s (223,113 samples/s, 22.53 s CPU) | **1.07x** faster (**1.30x** less CPU) |
| Phase 2 Steady Write CPU (`µs / ingested sample`) | 4.22 µs/sample | 2.19 µs/sample | **1.92x** (**48.0% less CPU**) |
| Phase 2 Steady `RssAnon` (`B/series`) | 5,019.5 MiB (`2,536 B/series`) | 2,393.9 MiB (`1,210 B/series`) | **2.10x** (**52.3% less heap RAM**) |
| Phase 2 Kafka record proc p50 / p99 | 12.46 ms / 48.15 ms | 2.59 ms / 18.94 ms | **4.81x** p50 / **2.54x** p99 |
| Phase 3 Head Compaction wall | 31.48 s | 22.27 s | **1.41x** faster |
| Phase 3 Peak `RssAnon` (sawtooth) | 5,595.5 MiB (+576.0 MiB) | 3,165.2 MiB (+771.3 MiB) | **1.77x** less peak RAM |
| Phase 4 `SIGKILL` `/ready` / `0-lag` catch-up | 11.90 s / 26.17 s | 11.17 s / 20.38 s | **1.07x** `/ready` / **1.28x** `0-lag` catch-up |

#### Full Production Workload (`--series 2000000 --steady-secs 30`, concurrent bimodal reads ON)
| Phase / Metric | Go `go-mimir` | Rust `mimir-rs` | Improvement (Go vs Rust) |
|----------------|---------------|-----------------|--------------------------|
| Phase 1 Catch-up (`2.4M` samples, `1.2` chunks/series) | 14.19 s (169,177 samples/s, 35.96 s CPU) | 13.17 s (182,213 samples/s, 25.65 s CPU) | **1.08x** faster (**1.40x** less CPU) |
| Phase 1 `RssAnon` (`B/series`) | 2,936.1 MiB (`1,539 B/series`) | 2,179.7 MiB (`1,143 B/series`) | **1.35x** (**25.8% less heap**) |
| Phase 2 Steady CPU (`µs / ingested sample`, reads ON) | 6.47 µs/sample | 2.86 µs/sample | **2.26x** (**55.7% less CPU**) |
| Phase 2 Steady `RssAnon` (`B/series`) | 5,431.3 MiB (`2,712 B/series`) | 2,539.9 MiB (`1,274 B/series`) | **2.14x** (**53.2% less heap RAM**) |
| `QueryStream` small (`~7 series`) p50 / p99 | 0.53 ms / 1.95 ms | 0.41 ms / 1.36 ms | **1.29x** p50 / **1.44x** p99 |
| `QueryStream` heavy (`~28k series`) p50 / p99 | 641.60 ms / 1,254.84 ms | 241.94 ms / 289.78 ms | **2.65x** p50 / **4.33x** p99 |
| `LabelNamesAndValues` p50 | 1,768.26 ms | 590.31 ms | **3.00x** faster |
| `LabelValuesCardinality` p50 | 742.23 ms | 152.95 ms | **4.85x** faster |
| `UserStats` p50 | 0.51 ms | 0.35 ms | **1.46x** faster |
| Phase 3 Head Compaction wall | 34.22 s | 25.42 s | **1.35x** faster |
| Phase 3 Peak `RssAnon` (sawtooth) | 6,545.3 MiB (+1,114.0 MiB) | 2,686.1 MiB (+145.9 MiB) | **2.44x** (**59.0% less peak RAM**, **7.6x smaller sawtooth**) |
| Phase 4 `SIGKILL` `/ready` / `0-lag` catch-up | 14.03 s / 34.79 s | 12.09 s / 24.33 s | **1.16x** `/ready` / **1.43x** catch-up |
| Phase 4 Post-restart `RssAnon` | 5,815.3 MiB | 2,803.1 MiB | **2.07x** (**51.8% less heap**) |

#### Go `go-mimir` Heap Decomposition (`/debug/pprof/heap -inuse_space` after Phase 1 preload, 2M series)
- **Go live heap (`inuse_space`)**: `1,513.85 MB` vs OS `RssAnon = 2,936.1 MiB` (**1,422.3 MiB `GOGC=100` GC headroom**, ~1.94x live heap; grows to `5,431.3 MiB` in steady state!).
- **Top Go `inuse_space` allocators**: `stringslite.Clone` (`309.08 MB`, `20.42%`), `chunkenc.NewXORChunk` (`234.02 MB`, `15.46%`), `tsdb.newMemSeries` (`215.54 MB`, `14.24%`), `activeseries.(*seriesStripe).findAndUpdateOrCreateEntryForSeries` (`148.53 MB`, `9.81%`), `tsdb/index.appendWithExponentialGrowth` (`129.05 MB`, `8.52%`), `zstd.(*sequenceDecs).executeSeq` (`120.04 MB`, `7.93%`), `mimirpb.newTimeSeries` (`84.54 MB`, `5.58%`), `tsdb.(*memSeries).cutNewHeadChunk` (`74.00 MB`, `4.89%`).
