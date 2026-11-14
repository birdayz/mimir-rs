//! Prometheus TSDB engine: symbol interner, labels & matchers, chunk encodings, WAL/WBL,
//! mmap head chunks, postings, and ULID compaction.
//!
//! Ported from:
//! - github.com/prometheus/prometheus/model/labels/*
//! - github.com/prometheus/prometheus/tsdb/*
//! - github.com/prometheus/prometheus/tsdb/chunkenc/*
//! - github.com/prometheus/prometheus/tsdb/encoding/*
//! - github.com/prometheus/prometheus/tsdb/record/*
//! - github.com/prometheus/prometheus/tsdb/wlog/*
//! - github.com/prometheus/prometheus/tsdb/chunks/*
//! - github.com/grafana/mimir/pkg/storage/tsdb/*

pub mod chunkenc;
pub mod encoding;
pub mod head_chunks;
pub mod labels;
pub mod matcher;
pub mod record;
pub mod symbols;
pub mod wlog;
