# mimir-rs

A 1:1 drop-in compatible Rust port of [Grafana Mimir](https://github.com/grafana/mimir) (upstream reference `@ 06de3ebd`), starting with `-target=ingester`.

`mimir-rs` matches upstream Grafana Mimir's CLI flags, strict YAML configuration, `memberlist`/Consul/etcd/inmemory KV and hash/partition rings, gRPC & HTTP wire protocols (all 14 `cortex.Ingester` RPCs, `httpgrpc.HTTP`, `ErrorDetails`), Kafka `ingest_storage` v0–v2 RW2 records, TSDB WAL/WBL/head/ULID block formats, and Prometheus metrics—while cutting CPU usage and memory footprint through zero-copy decoding, `u32` symbol interning, and `tikv-jemallocator` on Linux.

## Crate Layout

| Crate | Purpose |
|-------|---------|
| `crates/mimir-proto` | `cortexpb`, `ingesterpb` (14 RPCs), `ring`, `partition_ring`, `kv`, `httpgrpc`, RW2 + `V2CommonSymbols`, FNV-1 sharding, Snappy gRPC |
| `crates/mimir-common` | Tenant IDs, `stringlabels` `xxhash64`, `Service` FSM, logging, `ErrorDetails`, limiters, shutdown marker, activity tracker |
| `crates/mimir-config` | Go `-flag`/`--flag` parser, strict YAML + `${VAR}` env expansion + `common:` inheritance, runtime config & per-tenant `Overrides` |
| `crates/mimir-tsdb` | `u32` symbol table, `LabelMatcher`, XOR/histogram/OOO chunks, WAL/WBL, `chunks_head` mmap, striped `Head`, postings, exemplars, ULID blocks |
| `crates/mimir-ring` | `inmemory`/`consul`/`etcd`/`multi`/`memberlist` KV, classic `Ring` + `Lifecycler`, `PartitionRing`, live gossip compatibility |
| `crates/mimir-storage` | Object-store `BucketClient`, `Shipper` (`mimir.shipper.json`), pure-Rust Kafka `ingest_storage` v0–v2 client |
| `crates/mimir-ingester` | `UserTSDB`, zero-copy `Push`, Kafka consumer loop, circuit breaker, limiters, active/owned series, all 14 gRPC RPCs, HTTP admin, metrics |
| `crates/mimir-cmd` | `mimir` binary (`-target=ingester`), HTTP+gRPC server, `httpgrpc`, `/ready`, `/metrics`, `/services`, `/config` |
| `crates/mimir-conformance` | 3-tier differential Go-vs-Rust conformance suite and `bench_vs_go` harness |

## Verified Compatibility & Test Parity

- **Upstream test parity:** **575 / 575** upstream Go tests ported (`./scripts/verify-test-parity.sh`, `0` missing, `#[ignore]` forbidden).
- **3-tier differential conformance against upstream Go Mimir:**
  1. **Tier 1 — Go Package Oracle (`tests/go-oracle` -> `target/go-oracle`):** Bit-for-bit parity checks on hashes, ring token tables, protobuf/Kafka RW2 wire bytes, config parsing, and bidirectional Go ↔ Rust TSDB WAL/ULID block compatibility.
  2. **Tier 2 — Twin-Process Black-Box Harness (`crates/mimir-conformance`):** Runs unmodified `go-mimir` and Rust `mimir` side-by-side across classic `Push` (floats, OOO floats, `created_timestamp`, int/float/NHCB histograms, exemplars, metadata), all 13 read RPCs, `ErrorDetails`, HTTP admin routes, `/ingester/flush`, and ULID block cross-verification.
  3. **Tier 3 — Mixed Go + Rust Cluster & Kafka Harness:** Live mixed Go + Rust ingester cluster over `memberlist` gossip (`replication_factor: 2`) and twin-process Kafka `ingest_storage` v2 RW2 parity with strong read consistency.

## Head-to-Head Benchmarks (`go-mimir` vs `mimir-rs`)

Measured via `just bench-vs-go` (`100k` float + `2k` histogram series, 5 tenants, concurrency `8`, + `200` read RPCs):

### 20 rounds (2.04M samples)

| Metric | Go `go-mimir` | Rust `mimir-rs` | Improvement (Go vs Rust) |
|--------|---------------|-----------------|--------------------------|
| Ingest wall / throughput | 1.854 s (1,100,144 samples/s) | 1.741 s (1,171,549 samples/s) | **1.06x** faster |
| Push latency (p50 / p99) | 4.91 ms / 41.08 ms | 4.10 ms / 45.22 ms | **1.20x** p50 / 0.91x p99 |
| CPU after ingest | 8.780 s (4.304 CPU-s/1M) | 6.300 s (3.088 CPU-s/1M) | **1.39x** (**28.2% less CPU**) |
| RSS after ingest | 536.29 MiB | 301.50 MiB | **1.78x** (**43.8% less RSS**) |
| QueryStream latency (p50 / p99) | 3.49 ms / 17.76 ms | 4.14 ms / 6.10 ms | **2.91x** p99 |
| Total CPU (ingest + queries) | 9.800 s | 7.180 s | **1.36x** (**26.7% less CPU**) |
| Final RSS / Peak HWM | 583.89 MiB | 347.77 MiB | **1.68x** (**40.4% less RAM**) |

### 80 rounds (8.16M samples)

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

`2,000,000` active series (10 K8s labels, `prod` 99.5% + `preprod` 0.5%, `1,600` metric families), Kafka v2 RW2 + Zstd (`82.4 B/sample` raw, `8.0 B/sample` wire), `4` historical 2h ULID blocks on disk, `327k samples/s` steady state + `0.07%` OOO + `1%/h` churn + bimodal concurrent reads, live head compaction, and `SIGKILL` cold restart:

| Phase / Metric | Go `go-mimir` | Rust `mimir-rs` | Improvement (Go vs Rust) |
|----------------|---------------|-----------------|--------------------------|
| Phase 1 Catch-up (`2.4M` samples) | 13.37 s (179,446 samples/s) | 12.84 s (186,877 samples/s) | **1.04x** faster |
| Phase 1 `RssAnon` | 2,908.5 MiB (1,525 B/series) | 1,850.2 MiB (970 B/series) | **1.57x** (**36.4% less heap**) |
| Phase 2 Steady CPU (reads ON) | 6.63 µs/sample | 4.42 µs/sample | **1.50x** (**33.3% less CPU**) |
| Phase 2 Steady `RssAnon` | 5,334.7 MiB (2,663 B/series) | 2,152.1 MiB (1,079 B/series) | **2.48x** (**59.7% less heap RAM**) |
| `QueryStream` heavy (`~28k series`) p50 / p99 | 641.34 ms / 899.13 ms | 514.18 ms / 521.12 ms | **1.25x** p50 / **1.73x** p99 |
| `LabelNamesAndValues` p50 | 1,767.28 ms | 768.54 ms | **2.30x** faster |
| `LabelValuesCardinality` p50 | 773.06 ms | 374.25 ms | **2.07x** faster |
| Phase 3 Head Compaction wall | 37.88 s | 32.34 s | **1.17x** faster |
| Phase 3 Peak `RssAnon` | 6,609.3 MiB | 3,365.7 MiB | **1.96x** (**49.1% less peak RAM**) |
| Phase 4 `SIGKILL` `/ready` / `0-lag` catch-up | 15.17 s / 34.51 s | 15.89 s / 32.36 s | **1.07x** faster catch-up |

Go `/debug/pprof/heap -inuse_space` after Phase 1 preload (`2M` series): `1,415.42 MB` live heap vs `2,908.5 MiB` OS `RssAnon` (`~1,493 MiB` `GOGC=100` headroom; climbs to `5,334.7 MiB` in steady state).

## Quickstart

Build the release binary (`target/release/mimir`) and run workspace tests:

```bash
just build-release
just test
```

### Go Oracle Conformance & Benchmarks

Go oracle conformance, test-parity verification, and Go-vs-Rust benchmarks require the upstream Go reference repository cloned into `.ref/mimir` (gitignored, with Go toolchain installed):

```bash
git clone --depth 1 https://github.com/grafana/mimir.git .ref/mimir
# Or check out the exact reference commit (06de3ebd2b12aa22861b96e6863b93a162c31e3c):
git -C .ref/mimir fetch --depth 1 origin 06de3ebd2b12aa22861b96e6863b93a162c31e3c
git -C .ref/mimir checkout 06de3ebd2b12aa22861b96e6863b93a162c31e3c
```

Then run the full QA suite or head-to-head benchmark:

```bash
just qa            # builds go-oracle + go-mimir, checks, clippy, test parity (575/575), workspace tests
just bench-vs-go   # builds release + go binaries and runs the head-to-head Go vs Rust benchmark
```

## License

Licensed under [AGPL-3.0-only](LICENSE), matching upstream Grafana Mimir.
