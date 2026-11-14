# `mimir-rs` Port Status

Upstream reference: `grafana/mimir` `main` @ `06de3ebd` (`.ref/mimir`)

| Wave | Task | Crate / Scope | Status | Notes |
|------|------|---------------|--------|-------|
| W0 | Workspace & Go Oracle Bootstrap | `Cargo.toml`, `crates/*`, `tests/go-oracle` | Done | Workspace + `target/go-mimir` & `target/go-oracle` verified |
| W1-A | Wire Protocols & Zero-Copy Codecs | `crates/mimir-proto` | Pending | `cortexpb`, `ingesterpb`, `ring`, `partition_ring`, `kv`, `httpgrpc`, RW2 + `V2CommonSymbols`, FNV-1 sharding, Snappy/Gzip gRPC |
| W1-B | Shared Primitives & Services | `crates/mimir-common` | Pending | Tenant IDs, `xxhash64` label hash, `Service` FSM, logger, `globalerror`/`ErrorDetails`, limiters, `shutdownmarker`, `activitytracker` |
| W1-C | CLI Flags, YAML Config & Overrides | `crates/mimir-config` | Pending | Go `-flag`/`--flag` parser, strict YAML + env expansion + `common:` inheritance, runtime config & per-tenant `Overrides` |
| W1-D | TSDB Encodings, Chunks & WAL | `crates/mimir-tsdb` (part 1) | Pending | `u32` symbol table, `LabelMatcher`, XOR/Histogram/FloatHistogram/OOO chunks, WAL/WBL 32KiB Snappy/CRC32C, `chunks_head` mmap |
| W2-A | TSDB Head, Postings & Block Compaction | `crates/mimir-tsdb` (part 2) | Pending | Striped `Head`, postings, exemplars, metadata, `ChunkQuerier`, ULID block writer (`index` v2, `chunks`, `tombstones`, `meta.json`) |
| W2-B | KV, Memberlist Gossip & Hash/Partition Rings | `crates/mimir-ring` | Pending | `inmemory`/`consul`/`etcd`/`multi`/`memberlist` KV, Classic `Ring` + `Lifecycler`, `PartitionRing` + `PartitionTokenTable` |
| W2-C | Bucket Shipper & Kafka Ingest Storage | `crates/mimir-storage` | Pending | `BucketClient` (`filesystem`/`s3`/`gcs`/`azure`), `Shipper`, Kafka v0-v2 reader/writer, `ListOffsets` watcher & offset file |
| W3-A | Ingester Core & All 13 gRPC RPCs | `crates/mimir-ingester` | Pending | `UserTSDB`, `Push`, Kafka consumer loop, circuit breaker, limiters, active/owned series, all `cortex.Ingester` RPCs, HTTP admin, metrics |
| W3-B | `mimir` Binary & Module Manager | `crates/mimir-cmd` | Pending | `-target=ingester`, HTTP+gRPC server, `httpgrpc`, `/ready`, `/metrics`, `/services`, `/config`, `tikv-jemallocator` |
| W4 | Differential Go-vs-Rust QA & Perf Bench | `tests/`, `benches/` | Pending | Cross-read WAL/blocks, gRPC/Memberlist/Kafka Go<->Rust interop, CPU & RSS head-to-head benchmark |
