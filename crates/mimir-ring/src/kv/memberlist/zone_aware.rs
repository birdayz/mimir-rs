//! Zone-aware node selection for memberlist gossip and push/pull.
//!
//! Ported from github.com/grafana/dskit/kv/memberlist/node_zone_aware_routing.go
//! (`zoneAwareNodeSelectionDelegate`) and github.com/hashicorp/memberlist/node_selection_delegate.go.

use std::sync::atomic::{AtomicBool, Ordering};

use prometheus::{Counter, Opts, Registry};
use rand::Rng;
use tracing::warn;

use super::client::NodeState;
use super::node_meta::{EncodedNodeMetadata, NodeRole};
use super::wire::NodeStateType;

pub struct ZoneAwareNodeSelection {
    local_role: NodeRole,
    local_zone: String,
    joined: AtomicBool,
    select_nodes_calls: Counter,
    select_nodes_calls_skipped: Counter,
}

impl ZoneAwareNodeSelection {
    pub fn new(local_role: NodeRole, local_zone: &str, reg: Option<&Registry>) -> Self {
        let calls = Counter::with_opts(Opts::new(
            "memberlist_client_zone_aware_routing_select_nodes_total",
            "Total number of times memberlist attempted to select node candidates for gossiping (tracked only when when zone-aware routing is enabled).",
        ))
        .expect("valid metric");
        let skipped = Counter::with_opts(Opts::new(
            "memberlist_client_zone_aware_routing_select_nodes_skipped_total",
            "Total number of times memberlist zone-aware routing was skipped because the local zone is unknown or a zone has no alive bridges.",
        ))
        .expect("valid metric");
        if let Some(r) = reg {
            let _ = r.register(Box::new(calls.clone()));
            let _ = r.register(Box::new(skipped.clone()));
        }
        Self {
            local_role,
            local_zone: local_zone.to_string(),
            joined: AtomicBool::new(false),
            select_nodes_calls: calls,
            select_nodes_calls_skipped: skipped,
        }
    }

    pub fn mark_joined(&self) {
        self.joined.store(true, Ordering::Release);
    }

    fn skip(&self, n: usize) -> (Vec<usize>, Option<usize>) {
        if self.joined.load(Ordering::Acquire) {
            self.select_nodes_calls_skipped.inc();
        }
        ((0..n).collect(), None)
    }

    /// Go `SelectNodes`: returns indices of selected nodes and an optional preferred node index.
    pub fn select_nodes(&self, nodes: &[&NodeState]) -> (Vec<usize>, Option<usize>) {
        self.select_nodes_calls.inc();
        if self.local_zone.is_empty() {
            return self.skip(nodes.len());
        }
        let mut zones_with_members: Vec<&str> = Vec::new();
        let mut zones_with_alive_bridges: Vec<&str> = Vec::new();
        let mut selected = Vec::with_capacity(nodes.len());
        let mut preferred = None;
        let mut preferred_count = 0u32;
        let mut rng = rand::thread_rng();
        for (i, node) in nodes.iter().enumerate() {
            let meta = EncodedNodeMetadata(node.meta.as_deref().unwrap_or(&[]));
            let remote_zone = meta.zone();
            let remote_role = meta.role();
            if !remote_zone.is_empty() {
                if remote_role == NodeRole::Bridge {
                    if node.state == NodeStateType::Alive && !zones_with_alive_bridges.contains(&remote_zone) {
                        zones_with_alive_bridges.push(remote_zone);
                        zones_with_alive_bridges.sort();
                    }
                } else if !zones_with_members.contains(&remote_zone) {
                    zones_with_members.push(remote_zone);
                    zones_with_members.sort();
                }
            }
            let (is_selected, is_preferred) = self.select_node(remote_zone, remote_role);
            if is_selected {
                selected.push(i);
                if is_preferred {
                    preferred_count += 1;
                    if rng.gen_range(0..preferred_count) == 0 {
                        preferred = Some(i);
                    }
                }
            }
        }
        for zone in &zones_with_members {
            if !zones_with_alive_bridges.contains(zone) {
                warn!(zone, "memberlist zone-aware routing is skipped because a zone has no alive bridge");
                return self.skip(nodes.len());
            }
        }
        (selected, preferred)
    }

    fn select_node(&self, remote_zone: &str, remote_role: NodeRole) -> (bool, bool) {
        if remote_zone.is_empty() {
            return (true, false);
        }
        match self.local_role {
            NodeRole::Member => (remote_zone == self.local_zone, false),
            NodeRole::Bridge => {
                if remote_zone == self.local_zone {
                    (true, false)
                } else if remote_role == NodeRole::Bridge {
                    (true, true)
                } else {
                    (false, false)
                }
            }
        }
    }
}
