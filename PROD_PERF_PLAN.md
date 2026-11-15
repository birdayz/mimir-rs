# Production-Profile Performance & Realism Plan (`PROD_PERF_PLAN.md`)

Calibrated against a live Mimir 3.2.0 per-ingester production profile:

## 1. Production Workload Reference (Per Ingester)

- **Write path**: **Kafka `ingest_storage` ONLY** (`0` `/cortex.Ingester/Push` gRPC calls; 1 partition per ingester per zone, 6 partitions total).
  - **Rate**: `327k samples/s` flat (`653 Kafka records/s`, `~500 samples/record`).
  - **Wire & Record shape**: `28.6 MB/s` uncompressed (`~87 B/sample`), `7.3 MB/s` wire (`3.9x` compression) -> **Record `Version = 2`** (`V2CommonSymbols` offset-64 + RW2 shared symbol table + Zstd/Snappy batch compression).
  - **Batching**: `74 records/fetch`, `126 series/flush`, `2.3 shards/push`.
  - **Metadata**: `61.7k entries/s` ingested, `1.6k` distinct in memory.
  - **Out-of-order**: `221 samples/s` (`0.07%`), OOO window enabled.
  - **Exemplars / Native Histograms**: `0`.
  - **Consume latency**: record processing `p50 = 95 ms`, `p99 = 259 ms`; consume delay `p50 = 0.37 s`, `p99 = 4 s`.
- **Series & Storage**:
  - `13.9M` in-memory series (`13.55M` active) in 1 dominant tenant (`~41 s` scrape interval, `41–53 new series/s` = `~1%/h` churn).
  - `17M` head chunks (`~1.2 chunks/series`), `686 new chunks/s`.
  - `28` local ULID blocks on disk (`33.6 GiB`).
- **Lifecycle, CPU & Memory (Go Mimir 3.2.0 Baseline)**:
  - **RSS**: `43.6 GiB` (`3.37 KB RSS / series`), `7.4 GiB` 2h compaction sawtooth (`working set = 49 GiB`, `31.5 GiB` page cache from mmapped blocks).
  - **Head compaction**: `76 s` avg (`8.5 min` worst), `10.6 s` Head GC.
  - **Startup**: `210 s` TSDB open + Kafka catch-up.
  - **CPU**: `3.5 cores` (`10.7 µs CPU / ingested sample` including reads & compaction).
- **Read path (`~643k samples/s` read = ~2x write rate, `42%` head postings cache hit rate)**:
  - `QueryStream`: `6.35 req/s`, `p50 = 3.9 ms`, `p99 = 482 ms` (series/query: `p50 = 7`, `mean = 661`, `p99 = 28k`; samples/query: `mean = 99k`, `p99 = 2.3M`).
  - `LabelNamesAndValues`: `0.07 req/s` (`p50 = 25 ms`, `p99 = 4.3 s`, `351 KB` responses).
  - `LabelValuesCardinality`: `0.08 req/s` (`p50 = 46 ms`, `p99 = 4.6 s`).
  - `UserStats`: `0.08 req/s` (`p50 = 2.5 ms`, `p99 = 5 ms`).

---

## 2. Engine Optimizations (Zero Regression on `just qa` / `575/575` Tests)

### Wave P1-A (`crates/mimir-tsdb` — Per-Series RAM Diet, Metadata Read-Lock, Postings Cache & Parallel WAL Replay)
1. **`SeriesEntry` & `SeriesState` Compaction (`head.rs`)**:
   - Remove redundant `symbol_labels: SymbolLabels` from `SeriesEntry` (saves ~196 B/series = **2.7 GiB @ 13.9M series**; resolve symbols from `entry.labels` on creation/deletion/cold label-name scans).
   - Box `Histogram` & `FloatHistogram` inside `enum LastSample` (`176 B -> 16 B`, saves **2.2 GiB @ 13.9M float series**).
   - Intern `SeriesMetadata` `unit` and `help` as `SymbolId` (`Copy` 12-byte struct instead of `2 × String`, saves **~2.0 GiB @ 13.9M series**).
   - Store `mmapped_chunks` in `SmallVec<[MmappedChunk; 2]>` (inline for the `1.2 chunks/series` prod average, avoiding 13.9M heap `Vec` allocations) and keep single active `MemChunk` compact.
2. **`MemPostings` `u32` Series Refs (`postings.rs`)**:
   - Store `Vec<u32>` internally for head series refs `<= u32::MAX` (halves postings memory across ~180M entries, saving **~1.1 GiB**).
3. **Lock-Free / Read-Lock Metadata Fast Path (`metadata.rs`)**:
   - Under `RwLock::read()`, check if `(metric_name: &str, metric_type, help: &str, unit: &str)` already exists and update an `AtomicU64` last-seen epoch-second timestamp without allocating `String`s or taking a write lock (`61.7k/s -> 0 allocs, 0 write locks`).
4. **Head & Block `PostingsForMatchersCache` (`postings_cache.rs`)**:
   - Port Prometheus `PostingsForMatchersCache` (`TTL = 10s`, bounded items & bytes, per-metric-name version invalidation on series creation/deletion + global invalidation on delete/truncate, `prometheus_tsdb_postings_for_matchers_cache_*` metrics) so the 42% prod cache hit rate skips regex scans and postings intersections completely while remaining 100% strongly consistent with immediate Push-then-Query tests.
5. **Parallel WAL & Block Startup Replay (`head.rs`, `db.rs`)**:
   - Parallelize WAL segment replay across `wal_replay_concurrency` sharded workers (`ref_id % workers`) with pipelined 32KiB page Snappy decompression, plus concurrent `Block::open` across local ULID blocks, directly targeting Go's `210 s` startup window.

### Wave P1-B (`crates/mimir-ingester` — Sharded Kafka `PusherConsumer`, Compact `ActiveSeries` & Bimodal `QueryStream`)
1. **Sharded Kafka `ingest_storage` Write Pipeline (`ingest_storage.rs`)**:
   - Wire `PusherConsumer` + `ParallelStoragePusher` (`BatchingQueue` + `xxhash64` series sharding across workers, matching Go's `74 records/fetch -> 126 series/flush -> 2.3 shards/push`) into `start_ingest_reader` when `push_concurrency > 1` (and enable `ConcurrentFetchers` when `fetch_concurrency_max > 0`).
   - Add fast zero-String-copy `BorrowedWriteRequest` / pooled RW2 metadata & series batching so sharded pushers sustain multi-hundred-thousand samples/s alongside WAL fsyncs and compactions.
2. **`ActiveSeries` Per-Series Compaction (`active_series.rs`)**:
   - Inline `nanos: AtomicI64` inside `active_series::SeriesEntry` (eliminating 13.9M `Arc<AtomicI64>` heap allocations = **~500 MiB saved**), shrink `num_native_histogram_buckets` to `i32`, and store `Bytes` instead of duplicate `Vec<u8>` in `DeletedSeries`.
3. **Bimodal `QueryStream` Fast Path (`grpc.rs`, `query.rs`)**:
   - Inline 2-message stream (`tokio_stream::iter`) when matching series `<= 128` and chunks fit in 1 batch (`p50 = 7 series` prod queries skip `tokio::spawn` + `mpsc::channel`), while keeping bounded incremental channel streaming for large (`p99 = 28k series`) queries; prune non-overlapping local blocks strictly by `[min_time, max_time]` before index lookup.

---

## 3. Realistic Production Benchmark & Go Heap Profile (`Wave P2`)

- **`crates/mimir-conformance/examples/prod_bench.rs` (`just prod-bench`)**:
  - **Kafka `ingest_storage` v2 ONLY** (`push_grpc_method_enabled: false`), supporting both `--brokers` / `--capture` (real Redpanda topic replay) and calibrated synthetic v2 batches (`~500 samples/rec`, `~87 B/sample` uncompressed, Zstd `~3.9x` wire compression, `61.7k/s` metadata over `1.6k` distinct, `0.07%` OOO, `1%/h` churn, `0` histograms/exemplars, pre-populated local ULID blocks).
  - **Separate Catch-up vs Paced `327k samples/s` Modes** + **Concurrent Prod Read Workload** (`6.35/s` bimodal `QueryStream` reporting 7-series p50 and 28k-series p99 separately, plus `LabelNamesAndValues`, `LabelValuesCardinality`, `UserStats`).
  - **Lifecycle Phases**: Live head compaction (recording compaction duration + peak `RssAnon` / `VmRSS` sawtooth) + **`SIGKILL` -> `/ready` + Kafka catch-up** restart timer.
  - **Memory Decomposition**: Reports `RssAnon` (pure heap RSS, excluding mmapped block page cache!), `RssFile`, `VmRSS`, `VmHWM`, AND captures Go `/debug/pprof/heap` (`inuse_space`) at multi-million-series scale to separate Go live data from `GOGC` headroom.
  - **Graviton (`r6g`) Cross-Build**: `just build-release-arm64` (`aarch64-unknown-linux-gnu`) for full 14M-series / 6h runs on 128 GiB ARM64 nodes.
