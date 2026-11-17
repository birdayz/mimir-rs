//! Memberlist gossip KV store, TCP transport, wire framing, and Mergeable trait.
//!
//! Ported from:
//! - github.com/grafana/dskit/kv/memberlist/*
//! - github.com/hashicorp/memberlist/*

pub mod client;
pub mod gometrics;
pub mod lzw;
pub mod mergeable;
mod metrics;
pub mod node_meta;
pub mod queue;
mod stream;
pub mod suspicion;
pub mod tcp_transport;
pub mod wire;
mod zone_aware;

pub use client::{KvOptions, MemberlistKv, MemberlistKvClient, NodeState};
pub use mergeable::Mergeable;
pub use node_meta::{encode_node_metadata, EncodedNodeMetadata, NodeRole};
pub use tcp_transport::TcpTransport;
