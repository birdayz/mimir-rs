# Allocator Benchmark Report — `main` (`tikv-jemallocator` tuned) vs Stock `jemalloc` vs `glibc` (`std::alloc::System`) vs `glibc + malloc_trim(0)`

- **Commit benchmarked**: `d62e71d` (PR #42 / Wave 23, `--release` profile, identical codegen across all 4 binaries; only `#[global_allocator]` / `_rjem_malloc_conf` / purge hook in `crates/mimir-cmd/src/main.rs` changed).
- **Methodology**: Interleaved **ABBA** paired runs on an idle local Linux host so thermal / page-cache / frequency-scaling drift cancels out across means.

## Variants Tested

| Variant | Global Allocator | Configuration / Purge Hook | Stripped Binary | Unstripped Binary |
|---|---|---|---:|---:|
| **`jemalloc-main` (`main`)** | `tikv_jemallocator::Jemalloc` | `background_thread:true,dirty_decay_ms:250,muzzy_decay_ms:0,narenas:8` + `arena.4096.purge` post-WAL-replay | **32.66 MiB** (`33,440 KiB`) | 267.52 MiB |
| **`jemalloc-stock`** | `tikv_jemallocator::Jemalloc` | Stock `jemalloc` defaults (`dirty_decay_ms:10000,muzzy_decay_ms:10000,narenas:4*cores`) + `arena.4096.purge` | **32.66 MiB** (`33,440 KiB`) | 267.52 MiB |
| **`glibc-system`** | `std::alloc::System` | Vanilla Linux glibc `ptmalloc2`, no purge hook | **32.23 MiB** (`33,001 KiB`) | 266.35 MiB |
| **`glibc-malloc-trim`** | `std::alloc::System` | glibc `ptmalloc2` + `libc::malloc_trim(0)` post-WAL-replay | **32.23 MiB** (`33,002 KiB`) | 266.36 MiB |

Binary size cost of `tikv-jemallocator`: **`+439 KiB` (`+1.3%`)** stripped.

---

## Suite B — Production Mimir 3.2.0 Kafka `ingest_storage` v2 + Bimodal Reads + Live Compaction + `SIGKILL` Cold Restart (`prod_bench`)

**Workload**: `500,000` active series (`prod` 99.5% + `preprod` 0.5%, 10 K8s labels, metadata, OOO, churn) + `4` local 2h ULID blocks, Kafka v2 RW2 Zstd batches, `327,000` samples/s steady-state with concurrent bimodal reads (`QueryStream` small ~7s / med ~700s / heavy ~28,000s + `LabelNamesAndValues` + `LabelValuesCardinality` + `UserStats`), followed by live `POST /ingester/flush?wait=true` Head compaction and `SIGKILL` cold WAL replay + 5s Kafka catch-up (ABBA paired runs).

| Metric | `jemalloc-main` (`main`) | `glibc-system` | `glibc-malloc-trim` | `jemalloc-stock` | `glibc` vs `main` Delta |
|---|---:|---:|---:|---:|---|
| **Phase 1 Catch-up Throughput** | **177,595 samp/s** | 166,216 samp/s | 76,206 samp/s | 198,165 samp/s | `glibc` **-6.4% slower** |
| **Phase 1 `RssAnon` (500k series)** | 661.2 MiB | 634.1 MiB | 632.3 MiB | 911.5 MiB | `glibc` -4.1% |
| **Phase 2 Steady `RssAnon` (under bimodal 28k-series reads)** | **607.6 MiB** (`1,229 B/s`) | **871.7 MiB** (`1,761 B/s`) | **903.7 MiB** (`1,825 B/s`) | **955.2 MiB** (`1,929 B/s`) | `glibc` **+264.1 MiB (+43.5% RAM!)** |
| **Phase 2 Steady CPU / sample** | 2.70 µs/samp | 2.55 µs/samp | 2.80 µs/samp | 2.74 µs/samp | ~parity (within noise) |
| **Phase 3 Live Compaction Wall** | **6.92 s** | 7.92 s | 6.99 s | 6.16 s | `glibc` **+14.5% slower** |
| **Phase 3 Compaction Sawtooth Delta** | **+186.7 MiB** | **+335.8 MiB** | +258.5 MiB | +228.5 MiB | `glibc` **1.80× larger spike** |
| **Phase 3 Peak `RssAnon` / Peak `VmRSS`** | **797.2 / 893.9 MiB** | **1,207.4 / 1,279.3 MiB** | 1,162.2 / 1,231.6 MiB | 1,180.1 / 1,246.9 MiB | `glibc` **+410.2 MiB (+51.5% peak RAM!)** |
| **Phase 4 `SIGKILL` Cold `/ready`** | **3.53 s** | 4.49 s | 5.45 s | 2.51 s | `glibc` **+27.2% slower** |
| **Phase 4 `SIGKILL` 0-Lag Catch-up** | **4.90 s** | **8.87 s** | **8.78 s** | 4.92 s | `glibc` **1.81× slower catch-up!** |
| **Phase 4 Post-Restart `RssAnon`** | **883.5 MiB** | **2,053.6 MiB** | **1,718.6 MiB** | **1,420.3 MiB** | `glibc` **+1,170.1 MiB (2.32× RAM!)** |

---

## Suite A — Classic gRPC `Push` + Read RPCs (`bench_vs_go`, ABBA Paired Runs, `4.4M` Samples + `300` Read RPCs)

### A1: `snappy` Compression (ABBA Mean of 2 Runs)

| Metric | `jemalloc-main` (`main`) | `glibc-system` | `jemalloc-stock` | `jemalloc-main` Advantage vs `glibc` |
|---|---:|---:|---:|---|
| **Ingest Throughput** | **1,866,721 samp/s** | 1,682,773 samp/s | 1,848,343 samp/s | **+10.9% faster** |
| **Ingest CPU-s (`CPU-s / 1M`)** | **9.975 s (`2.267`)** | 10.980 s (`2.496`) | 9.705 s (`2.206`) | **-9.2% less ingest CPU** |
| **Total CPU-s (Ingest + Queries)** | **11.055 s** | 12.275 s | 10.730 s | **-9.9% less total CPU** |
| **`Push` Latency (p50 / p99)** | **3.40 / 62.72 ms** | 3.91 / 67.45 ms | 3.33 / 63.20 ms | **-13.0% p50 / -7.0% p99** |
| **Query Wall (300 RPCs)** | **0.154 s** | 0.182 s | 0.144 s | **18.2% faster** |
| **`QueryStream` (p50 / p99)** | 2.96 / **5.79 ms** | 2.92 / 6.17 ms | 2.78 / 4.92 ms | **-6.2% p99** |
| **`LabelNamesAndValues` (p50 / p99)** | **6.89 / 10.61 ms** | 10.66 / 15.03 ms | 6.51 / 8.50 ms | **1.55× faster p50 / 1.42× faster p99** |
| **`LabelValuesCardinality` (p50 / p99)** | 1.53 / **4.92 ms** | 1.47 / 6.10 ms | 1.34 / 4.51 ms | **-19.3% p99** |
| **Fresh-Process `VmRSS` (no churn yet)** | 361.09 MiB | 315.82 MiB | **704.03 MiB** | `glibc` -45 MiB on un-compacted fresh proc; `main` **-48.7% vs stock jemalloc!** |

### A2: `s2` Compression (ABBA Mean of 2 Runs)

| Metric | `jemalloc-main` (`main`) | `glibc-system` | `jemalloc-main` Advantage vs `glibc` |
|---|---:|---:|---|
| **Ingest Throughput** | **1,852,600 samp/s** | 1,736,172 samp/s | **+6.7% faster** |
| **Ingest CPU-s (`CPU-s / 1M`)** | **10.185 s (`2.315`)** | 10.825 s (`2.461`) | **-5.9% less ingest CPU** |
| **Total CPU-s (Ingest + Queries)** | **11.230 s** | 12.045 s | **-6.8% less total CPU** |
| **`Push` Latency (p50 / p99)** | **3.30 / 66.76 ms** | 3.49 / 72.11 ms | **-7.4% p99** |
| **`QueryStream` (p50 / p99)** | 2.74 / **5.61 ms** | 2.70 / 7.04 ms | **-20.3% p99** |
| **`LabelNamesAndValues` (p50 / p99)** | **6.54 / 8.96 ms** | 9.30 / 13.43 ms | **1.42× faster p50 / 1.50× faster p99** |
| **`VmRSS` After Queries** | 326.71 MiB | 309.56 MiB | `glibc` -17 MiB on un-compacted fresh proc |

---

## Suite C — 4-Wave Ephemeral Tenant Churn & Compaction Heap Retention (`alloc_churn_bench`, ABBA Mean of 2 Runs)

| Wave / Phase | `jemalloc-main` (`main`) `RssAnon` | `glibc-system` `RssAnon` | `jemalloc-stock` `RssAnon` | Behavior When 150k Series Are Freed |
|---|---:|---:|---:|---|
| **Boot Idle (0 tenants)** | 7.38 MiB | 4.46 MiB | 10.75 MiB | — |
| **Wave 1 Active -> Post-Flush (0 series)** | 267.5 -> **219.1 MiB** (**-48.4 MiB**) | 251.8 -> **286.4 MiB** (**+34.6 MiB!**) | 321.3 -> 363.0 MiB (+41.7 MiB) | `jemalloc-main` **returns 48 MiB**; `glibc` **grows +35 MiB** (+67 MiB higher than `main`!) |
| **Wave 2 Active -> Post-Flush (0 series)** | 403.6 -> **355.7 MiB** (**-47.9 MiB**) | 401.4 -> **431.7 MiB** (**+30.3 MiB!**) | 484.6 -> 474.3 MiB (-10.3 MiB) | `glibc` retains **+76.0 MiB** more `RssAnon` than `main` |
| **Wave 3 Active -> Post-Flush (0 series)** | 542.6 -> **494.2 MiB** (**-48.4 MiB**) | 528.7 -> **566.1 MiB** (**+37.4 MiB!**) | 619.8 -> 610.4 MiB (-9.4 MiB) | `glibc` retains **+71.9 MiB** more `RssAnon` than `main` |
| **Wave 4 Active -> Post-Flush (0 series)** | 671.4 -> **624.9 MiB** (**-46.5 MiB**) | 662.7 -> **698.8 MiB** (**+36.1 MiB!**) | 752.7 -> 751.5 MiB (-1.2 MiB) | `glibc` retains **+73.9 MiB** more `RssAnon` (+4.1% more CPU: `21.25s` vs `20.41s`) |

---

## Verdict

**Keep `main`s exact `tikv-jemallocator` setup (`background_thread:true,dirty_decay_ms:250,muzzy_decay_ms:0,narenas:8` + `arena.4096.purge`).**
