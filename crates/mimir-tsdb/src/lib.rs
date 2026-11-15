//! Prometheus TSDB engine: symbol interner, labels & matchers, chunk encodings, WAL/WBL,
//! mmap head chunks, postings, exemplars, metadata, search, querier, and ULID compaction.
//!
//! Ported from:
//! - github.com/prometheus/prometheus/model/labels/*
//! - github.com/prometheus/prometheus/tsdb/*
//! - github.com/prometheus/prometheus/tsdb/chunkenc/*
//! - github.com/prometheus/prometheus/tsdb/encoding/*
//! - github.com/prometheus/prometheus/tsdb/record/*
//! - github.com/prometheus/prometheus/tsdb/wlog/*
//! - github.com/prometheus/prometheus/tsdb/chunks/*
//! - github.com/prometheus/prometheus/tsdb/index/*
//! - github.com/prometheus/prometheus/tsdb/tombstones/*
//! - github.com/grafana/mimir/pkg/storage/tsdb/*
//! - github.com/grafana/mimir/pkg/streaminglabelvalues/*
//! - github.com/grafana/mimir/pkg/ingester/user_metrics_metadata.go

pub mod block;
pub mod chunkenc;
pub mod chunks_block;
pub mod compact;
pub mod db;
pub mod encoding;
pub mod exemplar;
pub mod head;
pub mod head_chunks;
pub mod index;
pub mod labels;
pub mod matcher;
pub mod metadata;
pub mod postings;
pub mod postings_cache;
pub mod querier;
pub mod record;
pub mod search;
pub mod symbols;
pub mod tombstones;
pub mod wlog;
