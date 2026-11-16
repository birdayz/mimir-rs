//! Object storage bucket client abstraction wrapping `object_store` with prefix/tenant scoping.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/storage/bucket/client.go
//! - github.com/grafana/mimir/pkg/storage/bucket/bucket_util.go
//! - github.com/grafana/mimir/pkg/storage/bucket/prefixed_bucket_client.go
//! - github.com/grafana/mimir/pkg/storage/bucket/user_bucket_client.go
//! - github.com/grafana/mimir/pkg/storage/bucket/filesystem/bucket_client.go
//! - github.com/grafana/mimir/pkg/storage/bucket/s3/bucket_client.go
//! - github.com/grafana/mimir/pkg/storage/bucket/gcs/bucket_client.go
//! - github.com/grafana/mimir/pkg/storage/bucket/azure/bucket_client.go
//! - github.com/grafana/mimir/pkg/storage/tsdb/tenant_deletion_mark.go

use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::AsyncReadExt;

use bytes::Bytes;
use futures::TryStreamExt;
use mimir_config::{BucketConfig, S3BucketLookupType};
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{GetOptions, GetRange, ObjectStore, PutPayload};
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub mod azure;
pub mod gcs;

pub const DIR_DELIM: &str = "/";
pub const MIMIR_INTERNALS_PREFIX: &str = "__mimir_cluster";
pub const TENANT_DELETION_MARK_PATH: &str = "markers/tenant-deletion-mark.json";

#[derive(Debug, Error)]
pub enum BucketError {
    #[error("unsupported storage backend: {0}")]
    UnsupportedBackend(String),
    #[error("invalid storage prefix: {0}")]
    InvalidPrefix(String),
    #[error("object not found: {0}")]
    NotFound(String),
    #[error("invalid object path {0}: {1}")]
    InvalidPath(String, String),
    #[error("object store error: {0}")]
    Store(#[from] object_store::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("configuration error: {0}")]
    Config(String),
}

impl BucketError {
    pub fn is_not_found(&self) -> bool {
        matches!(
            self,
            BucketError::NotFound(_) | BucketError::Store(object_store::Error::NotFound { .. })
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectAttributes {
    pub size: u64,
    pub last_modified_unix_secs: i64,
}

/// Async object-storage client supporting Filesystem, S3, GCS, Azure, and InMemory backends,
/// with composable prefix/tenant scoping (`with_prefix` / `for_user`).
#[derive(Clone)]
pub struct BucketClient {
    store: Arc<dyn ObjectStore>,
    prefix: Option<String>,
    name: String,
    provider: String,
}

impl std::fmt::Debug for BucketClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BucketClient")
            .field("prefix", &self.prefix)
            .field("name", &self.name)
            .field("provider", &self.provider)
            .finish()
    }
}

impl BucketClient {
    /// Creates an in-memory bucket client (for tests and ephemeral workloads).
    pub fn new_in_memory(name: impl Into<String>) -> Self {
        Self {
            store: Arc::new(InMemory::new()),
            prefix: None,
            name: name.into(),
            provider: "INMEMORY".to_string(),
        }
    }

    /// Creates a local filesystem bucket client rooted at `dir`.
    pub fn new_filesystem(dir: impl AsRef<Path>) -> Result<Self, BucketError> {
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir)?;
        let fs = LocalFileSystem::new_with_prefix(dir)?;
        Ok(Self {
            store: Arc::new(fs),
            prefix: None,
            name: dir.to_string_lossy().into_owned(),
            provider: "FILESYSTEM".to_string(),
        })
    }

    /// Wraps an arbitrary `Arc<dyn ObjectStore>` (useful for decorators/fault injection in tests).
    pub fn from_object_store(
        store: Arc<dyn ObjectStore>,
        name: impl Into<String>,
        provider: impl Into<String>,
    ) -> Self {
        Self {
            store,
            prefix: None,
            name: name.into(),
            provider: provider.into(),
        }
    }

    /// Builds a `BucketClient` from Mimir's `BucketConfig`.
    pub fn from_config(cfg: &BucketConfig) -> Result<Self, BucketError> {
        cfg.validate()
            .map_err(|e| BucketError::Config(e.to_string()))?;

        let b = &cfg.backend_cfg;
        let client = match b.backend.as_str() {
            "filesystem" => {
                if b.filesystem.dir.is_empty() {
                    return Err(BucketError::Config(
                        "filesystem directory must not be empty".to_string(),
                    ));
                }
                Self::new_filesystem(&b.filesystem.dir)?
            }
            "s3" => {
                let s3 = &b.s3;
                let mut builder = AmazonS3Builder::from_env().with_bucket_name(&s3.bucket_name);
                if !s3.region.is_empty() {
                    builder = builder.with_region(&s3.region);
                }
                if !s3.endpoint.is_empty() {
                    let endpoint = if s3.endpoint.starts_with("http://")
                        || s3.endpoint.starts_with("https://")
                    {
                        s3.endpoint.clone()
                    } else if s3.insecure {
                        format!("http://{}", s3.endpoint)
                    } else {
                        format!("https://{}", s3.endpoint)
                    };
                    builder = builder.with_endpoint(endpoint);
                }
                if s3.insecure {
                    builder = builder.with_allow_http(true);
                }
                if !s3.access_key_id.is_empty() {
                    builder = builder.with_access_key_id(&s3.access_key_id);
                }
                if !s3.secret_access_key.is_empty() {
                    builder = builder.with_secret_access_key(&s3.secret_access_key);
                }
                if !s3.session_token.is_empty() {
                    builder = builder.with_token(&s3.session_token);
                }
                match s3.bucket_lookup_type {
                    S3BucketLookupType::VirtualHost => {
                        builder = builder.with_virtual_hosted_style_request(true);
                    }
                    S3BucketLookupType::Path => {
                        builder = builder.with_virtual_hosted_style_request(false);
                    }
                    S3BucketLookupType::Auto => {}
                }
                let store = builder.build()?;
                Self {
                    store: Arc::new(store),
                    prefix: None,
                    name: s3.bucket_name.clone(),
                    provider: "S3".to_string(),
                }
            }
            "gcs" => Self {
                store: gcs::new_store(&b.gcs)?,
                prefix: None,
                name: b.gcs.bucket_name.clone(),
                provider: "GCS".to_string(),
            },
            "azure" => Self {
                store: azure::new_store(&b.azure)?,
                prefix: None,
                name: b.azure.container_name.clone(),
                provider: "AZURE".to_string(),
            },
            other => {
                return Err(BucketError::UnsupportedBackend(other.to_string()));
            }
        };

        if cfg.storage_prefix.is_empty() {
            Ok(client)
        } else {
            Ok(client.with_prefix(&cfg.storage_prefix))
        }
    }

    /// Returns a view of this bucket scoped under `<prefix>/` (matching `PrefixedBucketClient`).
    pub fn with_prefix(&self, prefix: impl AsRef<str>) -> Self {
        let p = prefix.as_ref().trim_matches('/');
        if p.is_empty() {
            return self.clone();
        }
        let combined = match &self.prefix {
            Some(existing) if !existing.is_empty() => format!("{existing}/{p}"),
            _ => p.to_string(),
        };
        Self {
            store: Arc::clone(&self.store),
            prefix: Some(combined),
            name: self.name.clone(),
            provider: self.provider.clone(),
        }
    }

    /// Returns a tenant-scoped bucket client under `<user_id>/` (matching `NewUserBucketClient`).
    pub fn for_user(&self, user_id: &str) -> Self {
        if user_id.is_empty() {
            self.clone()
        } else {
            self.with_prefix(user_id)
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn provider(&self) -> &str {
        &self.provider
    }

    pub fn prefix(&self) -> Option<&str> {
        self.prefix.as_deref()
    }

    pub fn inner_store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    fn full_name(&self, name: &str) -> String {
        let clean = name.trim_start_matches('/');
        match &self.prefix {
            Some(p) if !p.is_empty() => {
                if clean.is_empty() {
                    p.clone()
                } else {
                    format!("{p}/{clean}")
                }
            }
            _ => clean.to_string(),
        }
    }

    fn object_path(&self, name: &str) -> Result<ObjectPath, BucketError> {
        let full = self.full_name(name);
        ObjectPath::parse(&full).map_err(|e| BucketError::InvalidPath(full, e.to_string()))
    }

    fn strip_prefix(&self, full: &str) -> String {
        match &self.prefix {
            Some(p) if !p.is_empty() => {
                let p_slash = format!("{p}/");
                full.strip_prefix(&p_slash).unwrap_or(full).to_string()
            }
            _ => full.to_string(),
        }
    }

    /// Uploads `data` as object `name`.
    pub async fn upload(&self, name: &str, data: impl Into<Bytes>) -> Result<(), BucketError> {
        let path = self.object_path(name)?;
        self.store
            .put(&path, PutPayload::from_bytes(data.into()))
            .await?;
        Ok(())
    }

    /// Uploads a local file at `src_path` as object `name`, streaming files > 16 MiB via multipart upload.
    pub async fn upload_file(
        &self,
        name: &str,
        src_path: impl AsRef<Path>,
    ) -> Result<(), BucketError> {
        const MULTIPART_THRESHOLD: u64 = 16 * 1024 * 1024;
        const CHUNK_SIZE: usize = 8 * 1024 * 1024;

        let src_path = src_path.as_ref();
        let meta = tokio::fs::metadata(src_path).await?;
        if meta.len() <= MULTIPART_THRESHOLD {
            let data = tokio::fs::read(src_path).await?;
            return self.upload(name, Bytes::from(data)).await;
        }

        let path = self.object_path(name)?;
        let mut file = tokio::fs::File::open(src_path).await?;
        let mut upload = self.store.put_multipart(&path).await?;
        let mut buf = vec![0u8; CHUNK_SIZE];
        loop {
            let mut filled = 0usize;
            while filled < buf.len() {
                match file.read(&mut buf[filled..]).await {
                    Ok(0) => break,
                    Ok(n) => filled += n,
                    Err(e) => {
                        let _ = upload.abort().await;
                        return Err(BucketError::Io(e));
                    }
                }
            }
            if filled == 0 {
                break;
            }
            let chunk = Bytes::copy_from_slice(&buf[..filled]);
            if let Err(e) = upload.put_part(PutPayload::from_bytes(chunk)).await {
                let _ = upload.abort().await;
                return Err(BucketError::Store(e));
            }
        }
        upload.complete().await?;
        Ok(())
    }

    /// Downloads the entire object `name`.
    pub async fn download(&self, name: &str) -> Result<Bytes, BucketError> {
        let path = self.object_path(name)?;
        match self.store.get(&path).await {
            Ok(res) => Ok(res.bytes().await?),
            Err(object_store::Error::NotFound { .. }) => {
                Err(BucketError::NotFound(name.to_string()))
            }
            Err(e) => Err(BucketError::Store(e)),
        }
    }

    /// Downloads `length` bytes starting at `offset` from object `name`.
    pub async fn get_range(
        &self,
        name: &str,
        offset: u64,
        length: u64,
    ) -> Result<Bytes, BucketError> {
        let path = self.object_path(name)?;
        let opts = GetOptions {
            range: Some(GetRange::Bounded((offset as usize)..((offset + length) as usize))),
            ..Default::default()
        };
        match self.store.get_opts(&path, opts).await {
            Ok(res) => Ok(res.bytes().await?),
            Err(object_store::Error::NotFound { .. }) => {
                Err(BucketError::NotFound(name.to_string()))
            }
            Err(e) => Err(BucketError::Store(e)),
        }
    }

    /// Returns `true` if object `name` exists in the bucket.
    pub async fn exists(&self, name: &str) -> Result<bool, BucketError> {
        let path = self.object_path(name)?;
        match self.store.head(&path).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(BucketError::Store(e)),
        }
    }

    /// Deletes object `name`. Succeeds if the object does not exist (matching objstore semantics
    /// when deleting missing objects or cleaning up).
    pub async fn delete(&self, name: &str) -> Result<(), BucketError> {
        let path = self.object_path(name)?;
        match self.store.delete(&path).await {
            Ok(()) => Ok(()),
            Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(BucketError::Store(e)),
        }
    }

    /// Deletes object `name`, returning `BucketError::NotFound` if the object does not exist
    /// (matching Go `objstore.Bucket.Delete` for strict deletion checks such as `DeleteNoCompactMarker`).
    pub async fn delete_strict(&self, name: &str) -> Result<(), BucketError> {
        let path = self.object_path(name)?;
        match self.store.delete(&path).await {
            Ok(()) => Ok(()),
            Err(object_store::Error::NotFound { .. }) => {
                Err(BucketError::NotFound(name.to_string()))
            }
            Err(e) => Err(BucketError::Store(e)),
        }
    }

    /// Returns object metadata attributes (size and last modified unix seconds).
    pub async fn attributes(&self, name: &str) -> Result<ObjectAttributes, BucketError> {
        let path = self.object_path(name)?;
        match self.store.head(&path).await {
            Ok(meta) => Ok(ObjectAttributes {
                size: meta.size as u64,
                last_modified_unix_secs: meta.last_modified.timestamp(),
            }),
            Err(object_store::Error::NotFound { .. }) => {
                Err(BucketError::NotFound(name.to_string()))
            }
            Err(e) => Err(BucketError::Store(e)),
        }
    }

    /// Non-recursively lists immediate entries under `dir` (matching Thanos `objstore.Bucket.Iter`).
    /// Subdirectories are returned with a trailing `/` and configured prefixes are stripped.
    pub async fn iter_dir(&self, dir: &str) -> Result<Vec<String>, BucketError> {
        let full = self.full_name(dir);
        let prefix = if full.is_empty() {
            None
        } else {
            Some(
                ObjectPath::parse(&full)
                    .map_err(|e| BucketError::InvalidPath(full.clone(), e.to_string()))?,
            )
        };

        let list_res = match self.store.list_with_delimiter(prefix.as_ref()).await {
            Ok(res) => res,
            Err(object_store::Error::NotFound { .. }) => return Ok(Vec::new()),
            Err(e) => return Err(BucketError::Store(e)),
        };

        let mut entries =
            Vec::with_capacity(list_res.common_prefixes.len() + list_res.objects.len());
        for cp in list_res.common_prefixes {
            let stripped = self.strip_prefix(cp.as_ref());
            if stripped.is_empty() {
                continue;
            }
            entries.push(format!("{stripped}{DIR_DELIM}"));
        }
        for obj in list_res.objects {
            let stripped = self.strip_prefix(obj.location.as_ref());
            if stripped.is_empty() {
                continue;
            }
            entries.push(stripped);
        }
        entries.sort();
        Ok(entries)
    }

    /// Recursively lists all object keys under `prefix` with configured prefixes stripped.
    pub async fn list_all(&self, prefix: &str) -> Result<Vec<String>, BucketError> {
        let full = self.full_name(prefix);
        let path = if full.is_empty() {
            None
        } else {
            Some(
                ObjectPath::parse(&full)
                    .map_err(|e| BucketError::InvalidPath(full.clone(), e.to_string()))?,
            )
        };

        let mut stream = self.store.list(path.as_ref());
        let mut keys = Vec::new();
        while let Some(meta) = stream.try_next().await? {
            keys.push(self.strip_prefix(meta.location.as_ref()));
        }
        keys.sort();
        Ok(keys)
    }

    /// Recursively deletes all objects under `prefix` and returns the count of deleted objects.
    /// Ported from `DeletePrefix` in `pkg/storage/bucket/bucket_util.go`.
    pub async fn delete_prefix(&self, prefix: &str) -> Result<usize, BucketError> {
        let keys = self.list_all(prefix).await?;
        let mut deleted = 0usize;
        for key in keys {
            self.delete(&key).await?;
            deleted += 1;
        }
        Ok(deleted)
    }
}

/// Ported from `TenantDeletionMark` in `pkg/storage/tsdb/tenant_deletion_mark.go`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TenantDeletionMark {
    pub deletion_time: i64,
    #[serde(default, skip_serializing_if = "is_zero_i64")]
    pub finished_time: i64,
}

fn is_zero_i64(v: &i64) -> bool {
    *v == 0
}

impl TenantDeletionMark {
    pub fn now() -> Self {
        let secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        Self {
            deletion_time: secs,
            finished_time: 0,
        }
    }
}

pub async fn tenant_deletion_mark_exists(
    bucket: &BucketClient,
    user_id: &str,
) -> Result<bool, BucketError> {
    let marker = format!("{user_id}/{TENANT_DELETION_MARK_PATH}");
    bucket.exists(&marker).await
}

pub async fn write_tenant_deletion_mark(
    bucket: &BucketClient,
    user_id: &str,
    mark: &TenantDeletionMark,
) -> Result<(), BucketError> {
    let user_bucket = bucket.for_user(user_id);
    let data = serde_json::to_vec(mark)?;
    user_bucket
        .upload(TENANT_DELETION_MARK_PATH, Bytes::from(data))
        .await
}

pub async fn read_tenant_deletion_mark(
    bucket: &BucketClient,
    user_id: &str,
) -> Result<Option<TenantDeletionMark>, BucketError> {
    let marker = format!("{user_id}/{TENANT_DELETION_MARK_PATH}");
    match bucket.download(&marker).await {
        Ok(data) => Ok(Some(serde_json::from_slice(&data)?)),
        Err(e) if e.is_not_found() => Ok(None),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_in_memory_and_filesystem_crud_and_prefix() {
        for bkt in [
            BucketClient::new_in_memory("test"),
            BucketClient::new_filesystem(tempfile::tempdir().unwrap().path()).unwrap(),
        ] {
            let scoped = bkt.with_prefix("cluster-prefix").for_user("tenant-a");

            assert!(!scoped.exists("a/b.txt").await.unwrap());
            scoped.upload("a/b.txt", Bytes::from_static(b"hello world")).await.unwrap();
            scoped.upload("a/c.txt", Bytes::from_static(b"second")).await.unwrap();
            scoped.upload("top.txt", Bytes::from_static(b"top")).await.unwrap();

            assert!(scoped.exists("a/b.txt").await.unwrap());
            assert_eq!(scoped.download("a/b.txt").await.unwrap().as_ref(), b"hello world");
            assert_eq!(scoped.get_range("a/b.txt", 6, 5).await.unwrap().as_ref(), b"world");

            let attrs = scoped.attributes("a/b.txt").await.unwrap();
            assert_eq!(attrs.size, 11);

            let root_entries = scoped.iter_dir("").await.unwrap();
            assert_eq!(root_entries, vec!["a/".to_string(), "top.txt".to_string()]);

            let sub_entries = scoped.iter_dir("a").await.unwrap();
            assert_eq!(sub_entries, vec!["a/b.txt".to_string(), "a/c.txt".to_string()]);

            // Verify underlying unprefixed path matches <storage_prefix>/<user_id>/...
            assert!(bkt.exists("cluster-prefix/tenant-a/a/b.txt").await.unwrap());

            let deleted = scoped.delete_prefix("a").await.unwrap();
            assert_eq!(deleted, 2);
            assert!(!scoped.exists("a/b.txt").await.unwrap());
            assert!(scoped.exists("top.txt").await.unwrap());
        }
    }

    #[tokio::test]
    async fn test_tenant_deletion_mark() {
        let bkt = BucketClient::new_in_memory("test");
        assert!(!tenant_deletion_mark_exists(&bkt, "user-1").await.unwrap());
        assert!(read_tenant_deletion_mark(&bkt, "user-1").await.unwrap().is_none());

        let mark = TenantDeletionMark {
            deletion_time: 1_700_000_000,
            finished_time: 0,
        };
        write_tenant_deletion_mark(&bkt, "user-1", &mark).await.unwrap();
        assert!(tenant_deletion_mark_exists(&bkt, "user-1").await.unwrap());
        let read_back = read_tenant_deletion_mark(&bkt, "user-1").await.unwrap().unwrap();
        assert_eq!(read_back, mark);
    }

    #[tokio::test]
    async fn test_upload_file_streams_multipart_above_16mib() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("large.bin");
        let size = 17 * 1024 * 1024 + 123;
        let payload: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
        tokio::fs::write(&file_path, &payload).await.unwrap();

        for bkt in [
            BucketClient::new_in_memory("test"),
            BucketClient::new_filesystem(tempfile::tempdir().unwrap().path()).unwrap(),
        ] {
            bkt.upload_file("blocks/large.bin", &file_path).await.unwrap();
            let got = bkt.download("blocks/large.bin").await.unwrap();
            assert_eq!(got.len(), payload.len());
            assert_eq!(got.as_ref(), payload.as_slice());
        }
    }
}
