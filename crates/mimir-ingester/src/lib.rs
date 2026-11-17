//! Mimir ingester service, UserTSDB lifecycle, Push/QueryStream/metadata RPCs, Kafka consumer, and HTTP admin routes.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/ingester/*
//! - github.com/grafana/mimir/pkg/ingester/activeseries/*
//! - github.com/grafana/mimir/pkg/ingester/lookupplan/*

pub mod active_groups;
pub mod active_series;
pub mod activity;
pub mod circuit_breaker;
pub mod compaction;
pub mod costattribution;
pub mod errors;
pub mod expfmt;
pub mod grpc;
pub mod http;
pub mod ingest_storage;
pub mod ingester;
pub mod ingester_ring_lifecycler;
pub mod label_churn;
pub mod limiter;
pub mod lookupplan;
pub mod metric_counter;
pub mod metrics;
pub mod mirror_querier;
pub mod owned_series;
pub mod prioritizer;
pub mod push;
pub mod query;
pub mod reactive_limiter;
pub mod user_tsdb;
pub mod utilization;
pub mod workerpool;

pub use errors::IngesterError;
pub use grpc::{IngesterService, IngesterZeroCopyServer};
pub use http::ingester_router;
pub use ingester::Ingester;
pub use ingester_ring_lifecycler::{IngesterLifecycler, TokenlessLifecycler};
pub use user_tsdb::UserTsdb;
