//! Compact binary node metadata for memberlist zone-aware routing.
//!
//! Ported from:
//! - github.com/grafana/dskit/kv/memberlist/node_meta.go
//! - github.com/grafana/dskit/kv/memberlist/node_zone_aware_routing.go

use std::fmt;

pub const MAX_ZONE_NAME_LENGTH: usize = 16;
const MIN_METADATA_SIZE_VERSION_1: usize = 3;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum NodeRole {
    Member = 1,
    Bridge = 2,
}

impl NodeRole {
    pub fn parse(s: &str) -> Result<Self, String> {
        match s {
            "member" => Ok(Self::Member),
            "bridge" => Ok(Self::Bridge),
            _ => Err(format!(
                "invalid zone-aware routing role: {s} (valid values: member, bridge)"
            )),
        }
    }
}

impl fmt::Display for NodeRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NodeRole::Member => f.write_str("member"),
            NodeRole::Bridge => f.write_str("bridge"),
        }
    }
}

/// Encodes node metadata into Version 1 compact format:
/// `[version=1: u8][role: u8][zone_len: u8][zone bytes]`.
pub fn encode_node_metadata(role: NodeRole, zone: &str) -> Result<Vec<u8>, String> {
    let zone_bytes = zone.as_bytes();
    if zone_bytes.len() > MAX_ZONE_NAME_LENGTH {
        return Err(format!(
            "zone name too long: {} bytes (max {MAX_ZONE_NAME_LENGTH})",
            zone_bytes.len()
        ));
    }
    let mut buf = Vec::with_capacity(3 + zone_bytes.len());
    buf.push(1u8);
    buf.push(role as u8);
    buf.push(zone_bytes.len() as u8);
    buf.extend_from_slice(zone_bytes);
    Ok(buf)
}

/// Zero-allocation reader over encoded node metadata bytes.
pub struct EncodedNodeMetadata<'a>(pub &'a [u8]);

impl<'a> EncodedNodeMetadata<'a> {
    pub fn role(&self) -> NodeRole {
        if self.0.len() < MIN_METADATA_SIZE_VERSION_1 || self.0[0] != 1 {
            return NodeRole::Member;
        }
        match self.0[1] {
            2 => NodeRole::Bridge,
            _ => NodeRole::Member,
        }
    }

    pub fn zone(&self) -> &'a str {
        if self.0.len() < MIN_METADATA_SIZE_VERSION_1 || self.0[0] != 1 {
            return "";
        }
        let zone_len = self.0[2] as usize;
        if zone_len == 0 || self.0.len() < 3 + zone_len {
            return "";
        }
        std::str::from_utf8(&self.0[3..3 + zone_len]).unwrap_or("")
    }
}
