//! Prometheus TSDB engine: symbol interner, chunk encodings, WAL/WBL, mmap head chunks, postings, and ULID compaction.
//!
//! Ported from:
//! - github.com/prometheus/prometheus/tsdb/*
//! - github.com/prometheus/prometheus/tsdb/chunkenc/*
//! - github.com/prometheus/prometheus/tsdb/wlog/*
//! - github.com/prometheus/prometheus/tsdb/chunks/*
//! - github.com/prometheus/prometheus/tsdb/index/*
//! - github.com/grafana/mimir/pkg/storage/tsdb/*
