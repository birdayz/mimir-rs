//! Azure Blob Storage backend for the blocks-storage bucket client.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/storage/bucket/azure/bucket_client.go
//! - github.com/grafana/mimir/pkg/storage/bucket/azure/config.go
//! - github.com/thanos-io/objstore/providers/azure/azure.go

use std::sync::Arc;

use mimir_config::AzureConfig;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::ObjectStore;

use super::BucketError;

/// Builds the Azure object store for `cfg` (Go `azure.NewBucketClient`).
pub fn new_store(cfg: &AzureConfig) -> Result<Arc<dyn ObjectStore>, BucketError> {
    let az = cfg;
    let mut builder = MicrosoftAzureBuilder::from_env();
    if !az.container_name.is_empty() {
        builder = builder.with_container_name(&az.container_name);
    }
    if !az.account_name.is_empty() {
        builder = builder.with_account(&az.account_name);
    }
    if !az.account_key.is_empty() {
        builder = builder.with_access_key(&az.account_key);
    }
    if !az.user_assigned_id.is_empty() {
        builder = builder.with_client_id(&az.user_assigned_id);
    }
    if !az.connection_string.is_empty() {
        for part in az.connection_string.split(';') {
            if let Some((k, v)) = part.split_once('=') {
                match k.trim() {
                    "AccountName" => {
                        builder = builder.with_account(v.trim());
                    }
                    "AccountKey" => {
                        builder = builder.with_access_key(v.trim());
                    }
                    "BlobEndpoint" => {
                        builder = builder.with_endpoint(v.trim().to_string());
                    }
                    _ => {}
                }
            }
        }
    }
    Ok(Arc::new(builder.build()?))
}
