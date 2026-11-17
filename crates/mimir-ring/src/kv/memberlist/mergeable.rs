//! Mergeable trait for gossip-replicated KV values.
//!
//! Ported from github.com/grafana/dskit/kv/memberlist/mergeable.go

use std::any::Any;
use std::time::SystemTime;

use super::gofmt::GoJson;

/// Values stored in [`super::MemberlistKv`] must implement `Mergeable` so state from
/// concurrent CAS updates and gossip/push-pull syncs converges deterministically.
pub trait Mergeable: Send + Sync {
    /// Merges `other` into `self` in place.
    ///
    /// Returns `Some(change)` representing the delta to broadcast to peers, or `None`
    /// if the merge resulted in no change.
    ///
    /// When `local_cas` is `true`, missing entries in `other` compared to `self` are
    /// converted into tombstones (`LEFT` / `Deleted`).
    fn merge(
        &mut self,
        other: &mut dyn Mergeable,
        local_cas: bool,
    ) -> Result<Option<Box<dyn Mergeable>>, String>;

    /// Describes the keys/IDs present in this mergeable change for broadcast invalidation.
    fn merge_content(&self) -> Vec<String>;

    /// Removes tombstones older than `limit` (or all tombstones if `limit` is `None`).
    /// Returns `(remaining_tombstones, removed_tombstones)`.
    fn remove_tombstones(&mut self, limit: Option<SystemTime>) -> (usize, usize);

    /// Deep-clones this value as a boxed [`Mergeable`].
    fn clone_mergeable(&self) -> Box<dyn Mergeable>;

    /// Go `encoding/json` encoding of the gogo-generated value (`json:"..."` tags), as used by
    /// dskit's `/memberlist` `formatValue` (`format=json` / `json-pretty`).
    fn to_go_json(&self) -> GoJson;

    /// Go `fmt.Sprintf("%#v", value)` of the pointer value (gogo `GoString()`), as used by dskit's
    /// `/memberlist` `formatValue` for any other format.
    fn go_string(&self) -> String;

    fn as_any(&self) -> &(dyn Any + Send + Sync);
    fn as_any_mut(&mut self) -> &mut (dyn Any + Send + Sync);
    fn into_any(self: Box<Self>) -> Box<dyn Any + Send + Sync>;
}
