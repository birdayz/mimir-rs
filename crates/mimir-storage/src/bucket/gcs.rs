//! Google Cloud Storage backend for the blocks-storage bucket client.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/storage/bucket/gcs/bucket_client.go
//! - github.com/grafana/mimir/pkg/storage/bucket/gcs/config.go
//! - github.com/thanos-io/objstore/providers/gcs/gcs.go

use std::sync::Arc;

use mimir_config::GCSConfig;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::ObjectStore;

use super::BucketError;

/// Builds the GCS object store for `cfg` (Go `gcs.NewBucketClient`).
pub fn new_store(cfg: &GCSConfig) -> Result<Arc<dyn ObjectStore>, BucketError> {
    let mut builder = GoogleCloudStorageBuilder::from_env().with_bucket_name(&cfg.bucket_name);
    if !cfg.service_account.is_empty() {
        builder = builder.with_service_account_key(&cfg.service_account);
    }
    Ok(Arc::new(builder.build()?))
}
