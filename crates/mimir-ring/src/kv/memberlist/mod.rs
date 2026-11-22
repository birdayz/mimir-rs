//! Memberlist gossip KV store, TCP transport, wire framing, and Mergeable trait.
//!
//! Ported from:
//! - github.com/grafana/dskit/kv/memberlist/*
//! - github.com/hashicorp/memberlist/*

pub mod client;
pub mod gofmt;
pub mod gometrics;
pub mod http_status;
pub mod kv_init_service;
pub mod lzw;
pub mod mergeable;
mod metrics;
pub mod node_meta;
pub mod propagation_tracker;
pub mod queue;
mod stream;
pub mod suspicion;
pub mod tcp_transport;
pub mod wire;
pub mod zone_aware;

pub use client::{KvOptions, MemberlistKv, MemberlistKvClient, NodeState, RingBroadcast, ValueDesc};
pub use mergeable::Mergeable;
pub use node_meta::{encode_node_metadata, EncodedNodeMetadata, NodeRole};
pub use propagation_tracker::{
    propagation_delay_tracker_codec, PROPAGATION_DELAY_TRACKER_CODEC_ID,
    PROPAGATION_DELAY_TRACKER_KEY,
};
pub use tcp_transport::TcpTransport;
