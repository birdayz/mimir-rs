//! Partition ring for ingest-storage partitioning.
//!
//! Ported from:
//! - github.com/grafana/dskit/ring/partition_ring_model.go
//! - github.com/grafana/dskit/ring/partition_ring.go
//! - github.com/grafana/dskit/ring/partition_ring_watcher.go
//! - github.com/grafana/dskit/ring/partition_instance_ring.go
//! - github.com/grafana/dskit/ring/partition_instance_lifecycler.go
//! - github.com/grafana/dskit/ring/partition_ring_http.go

pub mod http;
pub mod instance_ring;
pub mod lifecycler;
pub mod model;
pub mod ring;
pub mod watcher;
pub mod watchers;

pub use instance_ring::{InstanceRingReader, PartitionInstanceRing, PartitionRingReader};
pub use lifecycler::{
    change_partition_state_in_desc, is_partition_state_change_allowed,
    multi_partition_owner_instance_id, PartitionInstanceLifecycler,
    PartitionInstanceLifecyclerConfig, PartitionRingEditor,
};
pub use model::{
    clean_partition_state_name, clean_token_scheme_name, partition_ring_desc_codec,
    PartitionDescExt, PartitionRingDescExt, PartitionRingError,
};
pub use ring::{resolve_partition_tokens, ActivePartitionBatchRing, PartitionRing, PartitionRingOptions};
pub use watcher::{PartitionRingWatcher, PartitionRingWatcherDelegate};
pub use watchers::PartitionRingWatchers;
