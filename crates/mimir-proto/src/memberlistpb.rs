//! Ported from github.com/grafana/dskit/kv/memberlist/kv.proto and propagation_tracker.proto

use std::collections::HashMap;

use bytes::Bytes;

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct KeyValueStore {
    #[prost(message, repeated, tag = "1")]
    pub pairs: Vec<KeyValuePair>,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct KeyValuePair {
    #[prost(string, tag = "1")]
    pub key: String,
    #[prost(bytes = "bytes", tag = "2")]
    pub value: Bytes,
    #[prost(string, tag = "3")]
    pub codec: String,
    #[prost(bool, tag = "4")]
    pub deleted: bool,
    #[prost(int64, tag = "5")]
    pub update_time_millis: i64,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct PropagationDelayTrackerDesc {
    #[prost(map = "uint64, message", tag = "1")]
    pub beacons: HashMap<u64, BeaconDesc>,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct BeaconDesc {
    #[prost(int64, tag = "1")]
    pub published_at: i64,
    #[prost(int64, tag = "2")]
    pub deleted_at: i64,
    #[prost(string, tag = "3")]
    pub published_by: String,
}
