//! KV backends (memberlist, consul, etcd, multi, inmemory), classic hash ring, and partition ring.
//!
//! Ported from:
//! - github.com/grafana/dskit/kv/*
//! - github.com/grafana/dskit/ring/*
//! - github.com/hashicorp/memberlist/*

pub mod kv;
pub mod lifecycler;
pub mod netutil;
pub mod partition_ring;
pub mod prng;
pub mod ring;
pub mod token_gen;

// Re-export runtime dependencies for downstream conformance tests.
pub use mimir_proto::prost;
pub use reqwest;
pub use tokio;
