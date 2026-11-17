//! HA-tracker replica descriptor stored in the KV store (`replicaDesc` codec).
//!
//! Go Mimir registers `distributor.GetReplicaDescCodec()` in the memberlist KV
//! of every target, so ingesters store and relay HA-tracker entries over gossip.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/distributor/ha_tracker.proto (`ReplicaDesc`)
//! - github.com/grafana/mimir/pkg/distributor/ha_tracker.go (`ReplicaDesc.Merge`,
//!   `mergeWithTime`, `MergeContent`, `RemoveTombstones`, `Clone`,
//!   `GetReplicaDescCodec`)
//! - github.com/grafana/mimir/pkg/distributor/ha_tracker.pb.go (`ReplicaDesc.String`)

use std::any::Any;
use std::fmt;
use std::sync::Arc;
use std::time::SystemTime;

use super::memberlist::gofmt::{go_sharp_v_str, GoJson};
use super::memberlist::Mergeable;
use super::{Codec, ProtoSnappyCodec};

/// `distributor.ReplicaDesc` (proto3, package `distributor`).
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct ReplicaDesc {
    #[prost(string, tag = "1")]
    pub replica: String,
    /// Unix timestamp in milliseconds when we have last received request from this replica.
    #[prost(int64, tag = "2")]
    pub received_at: i64,
    /// Unix timestamp in milliseconds when this entry was marked for deletion.
    #[prost(int64, tag = "3")]
    pub deleted_at: i64,
    /// This is the most recent timestamp when this replica was elected as the leader.
    #[prost(int64, tag = "4")]
    pub elected_at: i64,
    /// This is incremented every time a new replica is elected as the leader.
    #[prost(int64, tag = "5")]
    pub elected_changes: i64,
}

/// Go `GetReplicaDescCodec()`: `codec.NewProtoCodec("replicaDesc", ProtoReplicaDescFactory)`.
pub fn replica_desc_codec() -> Arc<dyn Codec> {
    Arc::new(
        ProtoSnappyCodec::<ReplicaDesc>::new("replicaDesc").with_mergeable(
            |d| Box::new(d),
            |m| m.as_any().downcast_ref::<ReplicaDesc>(),
        ),
    )
}

impl ReplicaDesc {
    /// Go `mergeWithTime`. The decision is made based on the `ReceivedAt`
    /// timestamp if the Replica name is the same and on `ElectedAt` if the
    /// Replica name is different. Returns the change (a copy of the new
    /// state) or `None` if nothing changed.
    pub fn merge_with(&mut self, other: Option<&ReplicaDesc>) -> Option<ReplicaDesc> {
        let other = other?;

        let mut changed = false;
        if other.replica == self.replica {
            // Keeping the one with the most recent receivedAt timestamp
            if other.received_at > self.received_at
                || (self.received_at == other.received_at
                    && self.deleted_at == 0
                    && other.deleted_at != 0)
            {
                *self = other.clone();
                changed = true;
            }
        } else {
            // keep the most recent ElectedAt to reach consistency
            if other.elected_at > self.elected_at
                || (other.elected_at == self.elected_at && other.received_at > self.received_at)
            {
                *self = other.clone();
                changed = true;
            }
        }

        if !changed {
            return None;
        }
        Some(self.clone())
    }
}

/// Go gogo-generated `(*ReplicaDesc).String()`.
impl fmt::Display for ReplicaDesc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "&ReplicaDesc{{Replica:{},ReceivedAt:{},DeletedAt:{},ElectedAt:{},ElectedChanges:{},}}",
            self.replica, self.received_at, self.deleted_at, self.elected_at, self.elected_changes
        )
    }
}

impl Mergeable for ReplicaDesc {
    fn merge(
        &mut self,
        other: &mut dyn Mergeable,
        _local_cas: bool,
    ) -> Result<Option<Box<dyn Mergeable>>, String> {
        let other = other
            .as_any()
            .downcast_ref::<ReplicaDesc>()
            .ok_or_else(|| {
                "expected *distributor.ReplicaDesc, got a different Mergeable type".to_string()
            })?;
        Ok(self
            .merge_with(Some(other))
            .map(|c| Box::new(c) as Box<dyn Mergeable>))
    }

    /// Given that ReplicaDesc can have only one instance at a time, it returns
    /// the ReplicaDesc it contains (no subset invalidation).
    fn merge_content(&self) -> Vec<String> {
        if self.replica.is_empty() {
            Vec::new()
        } else {
            vec![self.to_string()]
        }
    }

    /// No-op: replica deletions are handled outside of memberlist.
    fn remove_tombstones(&mut self, _limit: Option<SystemTime>) -> (usize, usize) {
        (0, 0)
    }

    fn clone_mergeable(&self) -> Box<dyn Mergeable> {
        Box::new(self.clone())
    }

    /// Go `encoding/json` of `distributor.ReplicaDesc` (all fields `omitempty`).
    fn to_go_json(&self) -> GoJson {
        let mut f: Vec<(String, GoJson)> = Vec::new();
        if !self.replica.is_empty() {
            f.push(("replica".into(), GoJson::str(self.replica.as_str())));
        }
        for (name, v) in [
            ("received_at", self.received_at),
            ("deleted_at", self.deleted_at),
            ("elected_at", self.elected_at),
            ("elected_changes", self.elected_changes),
        ] {
            if v != 0 {
                f.push((name.into(), GoJson::int(v)));
            }
        }
        GoJson::Object(f)
    }

    /// Go `(*ReplicaDesc).GoString` (ha_tracker.pb.go).
    fn go_string(&self) -> String {
        format!(
            "&distributor.ReplicaDesc{{Replica: {},\nReceivedAt: {},\nDeletedAt: {},\nElectedAt: {},\nElectedChanges: {},\n}}",
            go_sharp_v_str(&self.replica),
            self.received_at,
            self.deleted_at,
            self.elected_at,
            self.elected_changes
        )
    }

    fn as_any(&self) -> &(dyn Any + Send + Sync) {
        self
    }

    fn as_any_mut(&mut self) -> &mut (dyn Any + Send + Sync) {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any + Send + Sync> {
        self
    }
}
