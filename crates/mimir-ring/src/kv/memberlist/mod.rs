//! Memberlist gossip KV store, TCP transport, wire framing, and Mergeable trait.
//!
//! Ported from:
//! - github.com/grafana/dskit/kv/memberlist/*
//! - github.com/hashicorp/memberlist/*

pub mod client;
pub mod lzw;
pub mod mergeable;
pub mod node_meta;
pub mod tcp_transport;
pub mod wire;

pub use client::{MemberlistKv, MemberlistKvClient, NodeState};
pub use mergeable::Mergeable;
pub use node_meta::{encode_node_metadata, EncodedNodeMetadata, NodeRole};
pub use tcp_transport::TcpTransport;
