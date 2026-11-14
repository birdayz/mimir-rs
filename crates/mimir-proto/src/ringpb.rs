//! Ported from github.com/grafana/dskit/ring/ring.proto and partition_ring_desc.proto

use std::collections::HashMap;

#[derive(Clone, PartialEq, prost::Message)]
pub struct Desc {
    #[prost(map = "string, message", tag = "1")]
    pub ingesters: HashMap<String, InstanceDesc>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct InstanceDesc {
    #[prost(string, tag = "1")]
    pub addr: String,
    #[prost(int64, tag = "2")]
    pub timestamp: i64,
    #[prost(enumeration = "InstanceState", tag = "3")]
    pub state: i32,
    #[prost(uint32, repeated, tag = "6")]
    pub tokens: Vec<u32>,
    #[prost(string, tag = "7")]
    pub zone: String,
    #[prost(int64, tag = "8")]
    pub registered_timestamp: i64,
    #[prost(string, tag = "9")]
    pub id: String,
    #[prost(int64, tag = "10")]
    pub read_only_updated_timestamp: i64,
    #[prost(bool, tag = "11")]
    pub read_only: bool,
    #[prost(map = "uint64, uint64", tag = "12")]
    pub versions: HashMap<u64, u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, prost::Enumeration)]
#[repr(i32)]
pub enum InstanceState {
    Active = 0,
    Leaving = 1,
    Pending = 2,
    Joining = 3,
    Left = 4,
}

impl InstanceState {
    pub fn as_str_name(&self) -> &'static str {
        match self {
            InstanceState::Active => "ACTIVE",
            InstanceState::Leaving => "LEAVING",
            InstanceState::Pending => "PENDING",
            InstanceState::Joining => "JOINING",
            InstanceState::Left => "LEFT",
        }
    }

    pub fn from_str_name(value: &str) -> Option<Self> {
        match value {
            "ACTIVE" => Some(Self::Active),
            "LEAVING" => Some(Self::Leaving),
            "PENDING" => Some(Self::Pending),
            "JOINING" => Some(Self::Joining),
            "LEFT" => Some(Self::Left),
            _ => None,
        }
    }
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct PartitionRingDesc {
    #[prost(map = "int32, message", tag = "1")]
    pub partitions: HashMap<i32, PartitionDesc>,
    #[prost(map = "string, message", tag = "2")]
    pub owners: HashMap<String, OwnerDesc>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct PartitionDesc {
    #[prost(uint32, repeated, tag = "1")]
    pub tokens: Vec<u32>,
    #[prost(enumeration = "PartitionState", tag = "2")]
    pub state: i32,
    #[prost(int64, tag = "3")]
    pub state_timestamp: i64,
    #[prost(int32, tag = "4")]
    pub id: i32,
    #[prost(bool, tag = "5")]
    pub state_change_locked: bool,
    #[prost(int64, tag = "6")]
    pub state_change_locked_timestamp: i64,
    #[prost(enumeration = "PartitionTokenScheme", tag = "7")]
    pub token_scheme: i32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, prost::Enumeration)]
#[repr(i32)]
pub enum PartitionTokenScheme {
    PartitionTokensStored = 0,
    PartitionTokensSmt512 = 1,
}

impl PartitionTokenScheme {
    pub fn as_str_name(&self) -> &'static str {
        match self {
            PartitionTokenScheme::PartitionTokensStored => "PartitionTokensStored",
            PartitionTokenScheme::PartitionTokensSmt512 => "PartitionTokensSmt512",
        }
    }

    pub fn from_str_name(value: &str) -> Option<Self> {
        match value {
            "PartitionTokensStored" => Some(Self::PartitionTokensStored),
            "PartitionTokensSmt512" => Some(Self::PartitionTokensSmt512),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, prost::Enumeration)]
#[repr(i32)]
pub enum PartitionState {
    PartitionUnknown = 0,
    PartitionPending = 1,
    PartitionActive = 2,
    PartitionInactive = 3,
    PartitionDeleted = 4,
}

impl PartitionState {
    pub fn as_str_name(&self) -> &'static str {
        match self {
            PartitionState::PartitionUnknown => "PartitionUnknown",
            PartitionState::PartitionPending => "PartitionPending",
            PartitionState::PartitionActive => "PartitionActive",
            PartitionState::PartitionInactive => "PartitionInactive",
            PartitionState::PartitionDeleted => "PartitionDeleted",
        }
    }

    pub fn from_str_name(value: &str) -> Option<Self> {
        match value {
            "PartitionUnknown" => Some(Self::PartitionUnknown),
            "PartitionPending" => Some(Self::PartitionPending),
            "PartitionActive" => Some(Self::PartitionActive),
            "PartitionInactive" => Some(Self::PartitionInactive),
            "PartitionDeleted" => Some(Self::PartitionDeleted),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, prost::Message)]
pub struct OwnerDesc {
    #[prost(int32, tag = "1")]
    pub owned_partition: i32,
    #[prost(enumeration = "OwnerState", tag = "2")]
    pub state: i32,
    #[prost(int64, tag = "3")]
    pub updated_timestamp: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, prost::Enumeration)]
#[repr(i32)]
pub enum OwnerState {
    OwnerUnknown = 0,
    OwnerActive = 1,
    OwnerDeleted = 2,
}

impl OwnerState {
    pub fn as_str_name(&self) -> &'static str {
        match self {
            OwnerState::OwnerUnknown => "OwnerUnknown",
            OwnerState::OwnerActive => "OwnerActive",
            OwnerState::OwnerDeleted => "OwnerDeleted",
        }
    }

    pub fn from_str_name(value: &str) -> Option<Self> {
        match value {
            "OwnerUnknown" => Some(Self::OwnerUnknown),
            "OwnerActive" => Some(Self::OwnerActive),
            "OwnerDeleted" => Some(Self::OwnerDeleted),
            _ => None,
        }
    }
}
