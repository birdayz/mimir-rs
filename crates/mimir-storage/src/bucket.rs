//! Object storage bucket client: S3 (thanos objstore/minio-go port), GCS/Azure/filesystem
//! (`object_store`), prefix/tenant scoping, per-tenant S3 SSE and objstore operation metrics.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/storage/bucket/client.go
//! - github.com/grafana/mimir/pkg/storage/bucket/bucket_util.go
//! - github.com/grafana/mimir/pkg/storage/bucket/prefixed_bucket_client.go
//! - github.com/grafana/mimir/pkg/storage/bucket/user_bucket_client.go
//! - github.com/grafana/mimir/pkg/storage/bucket/sse_bucket_client.go
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
use futures::stream::FuturesUnordered;
use futures::{StreamExt, TryStreamExt};
use mimir_config::BucketConfig;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{GetOptions, GetRange, ObjectStore, PutPayload};
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub mod azure;
pub mod exthttp;
pub mod gcs;
pub mod metrics;
pub mod s3;
pub mod s3utils;

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
    #[error("{0}")]
    S3(#[from] s3::S3BucketError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("configuration error: {0}")]
    Config(String),
    #[error("{0}")]
    Metrics(#[from] prometheus::Error),
    /// Mimir `SSEBucketClient.getCustomS3SSEConfig` failure.
    #[error("unable to customise S3 SSE config for tenant {user}: {reason}")]
    TenantSse { user: String, reason: String },
}

impl BucketError {
    pub fn is_not_found(&self) -> bool {
        match self {
            BucketError::NotFound(_) | BucketError::Store(object_store::Error::NotFound { .. }) => {
                true
            }
            BucketError::S3(e) => e.is_obj_not_found(),
            _ => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectAttributes {
    pub size: u64,
    pub last_modified_unix_secs: i64,
}

/// Per-tenant configuration consumed by the bucket client (Mimir
/// `bucket.TenantConfigProvider`, implemented by the limits overrides).
pub trait TenantConfigProvider: Send + Sync {
    /// `S3SSEType(userID)`.
    fn s3_sse_type(&self, user_id: &str) -> String;
    /// `S3SSEKMSKeyID(userID)`.
    fn s3_sse_kms_key_id(&self, user_id: &str) -> String;
    /// `S3SSEKMSEncryptionContext(userID)`.
    fn s3_sse_kms_encryption_context(&self, user_id: &str) -> String;
}

/// The ingester passes its limits overrides (`i.limits`) as the tenant config provider.
impl TenantConfigProvider for mimir_config::Overrides {
    fn s3_sse_type(&self, user_id: &str) -> String {
        self.for_tenant(user_id).s3_sse_type.clone()
    }
    fn s3_sse_kms_key_id(&self, user_id: &str) -> String {
        self.for_tenant(user_id).s3_sse_kms_key_id.clone()
    }
    fn s3_sse_kms_encryption_context(&self, user_id: &str) -> String {
        self.for_tenant(user_id).s3_sse_kms_encryption_context.clone()
    }
}

#[derive(Clone)]
struct TenantSse {
    user_id: String,
    provider: Arc<dyn TenantConfigProvider>,
}

#[derive(Clone)]
enum Backend {
    Store(Arc<dyn ObjectStore>),
    S3(Arc<s3::S3Bucket>),
}

/// Async object-storage client supporting Filesystem, S3, GCS, Azure, and InMemory backends,
/// with composable prefix/tenant scoping (`with_prefix` / `for_user`), the objstore operation
/// metrics (`thanos_objstore_bucket_*`) and Mimir's per-tenant S3 SSE injection.
#[derive(Clone)]
pub struct BucketClient {
    backend: Backend,
    prefix: Option<String>,
    name: String,
    provider: String,
    metrics: Option<Arc<metrics::BucketMetrics>>,
    tenant_sse: Option<TenantSse>,
    expected_not_found: bool,
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
    fn from_backend(backend: Backend, name: String, provider: &str) -> Self {
        Self {
            backend,
            prefix: None,
            name,
            provider: provider.to_string(),
            metrics: None,
            tenant_sse: None,
            expected_not_found: false,
        }
    }

    /// Creates an in-memory bucket client (for tests and ephemeral workloads).
    pub fn new_in_memory(name: impl Into<String>) -> Self {
        Self::from_backend(Backend::Store(Arc::new(InMemory::new())), name.into(), "INMEMORY")
    }

    /// Creates a local filesystem bucket client rooted at `dir`.
    pub fn new_filesystem(dir: impl AsRef<Path>) -> Result<Self, BucketError> {
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir)?;
        let fs = LocalFileSystem::new_with_prefix(dir)?;
        let abs = std::path::absolute(dir).unwrap_or_else(|_| dir.to_path_buf());
        // thanos filesystem `Name()`: "fs: <absolute root dir>".
        Ok(Self::from_backend(
            Backend::Store(Arc::new(fs)),
            format!("fs: {}", abs.to_string_lossy()),
            "FILESYSTEM",
        ))
    }

    /// Wraps an arbitrary `Arc<dyn ObjectStore>` (useful for decorators/fault injection in tests).
    pub fn from_object_store(
        store: Arc<dyn ObjectStore>,
        name: impl Into<String>,
        provider: impl Into<String>,
    ) -> Self {
        Self::from_backend(Backend::Store(store), name.into(), &provider.into())
    }

    /// Builds a `BucketClient` from Mimir's `BucketConfig` without metrics (component
    /// `ingester`).
    pub fn from_config(cfg: &BucketConfig) -> Result<Self, BucketError> {
        Self::new_client(cfg, "ingester", None)
    }

    /// Mimir `bucket.NewClient(ctx, cfg, component, logger, reg)`: backend client, storage
    /// prefix, then `thanos_objstore_bucket_*` metrics labelled with `component` (when a
    /// registry is given).
    pub fn new_client(
        cfg: &BucketConfig,
        component: &str,
        registry: Option<&prometheus::Registry>,
    ) -> Result<Self, BucketError> {
        cfg.validate()
            .map_err(|e| BucketError::Config(e.to_string()))?;

        let b = &cfg.backend_cfg;
        let mut client = match b.backend.as_str() {
            "filesystem" => {
                if b.filesystem.dir.is_empty() {
                    return Err(BucketError::Config(
                        "filesystem directory must not be empty".to_string(),
                    ));
                }
                Self::new_filesystem(&b.filesystem.dir)?
            }
            "s3" => {
                let bucket = s3::S3Bucket::new(&b.s3, component).map_err(BucketError::Config)?;
                Self::from_backend(
                    Backend::S3(Arc::new(bucket)),
                    b.s3.bucket_name.clone(),
                    "S3",
                )
            }
            "gcs" => Self::from_backend(
                Backend::Store(gcs::new_store(&b.gcs)?),
                b.gcs.bucket_name.clone(),
                "GCS",
            ),
            "azure" => Self::from_backend(
                Backend::Store(azure::new_store(&b.azure)?),
                b.azure.container_name.clone(),
                "AZURE",
            ),
            other => {
                return Err(BucketError::UnsupportedBackend(other.to_string()));
            }
        };

        if !cfg.storage_prefix.is_empty() {
            client = client.with_prefix(&cfg.storage_prefix);
        }
        if let Some(reg) = registry {
            client.metrics = Some(Arc::new(metrics::BucketMetrics::register(
                reg,
                component,
                &client.name,
            )?));
        }
        Ok(client)
    }

    /// Returns a view of this bucket that treats `BucketError::NotFound` as an expected error
    /// (matching Thanos `WithExpectedErrs(IsObjNotFoundErr)`), so `thanos_objstore_bucket_operation_failures_total`
    /// is not incremented when probing for optional objects.
    pub fn with_expected_not_found(&self) -> Self {
        Self {
            expected_not_found: true,
            ..self.clone()
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
            prefix: Some(combined),
            ..self.clone()
        }
    }

    /// Returns a tenant-scoped bucket client under `<user_id>/` (matching `NewUserBucketClient`
    /// without a tenant config provider).
    pub fn for_user(&self, user_id: &str) -> Self {
        if user_id.is_empty() {
            self.clone()
        } else {
            self.with_prefix(user_id)
        }
    }

    /// Mimir `NewUserBucketClient(userID, bucket, cfgProvider)`: the tenant prefix plus the
    /// `SSEBucketClient`, which injects the tenant's S3 SSE override (`s3_sse_type`,
    /// `s3_sse_kms_key_id`, `s3_sse_kms_encryption_context`) into every upload.
    pub fn for_user_with_config(
        &self,
        user_id: &str,
        provider: Arc<dyn TenantConfigProvider>,
    ) -> Self {
        let mut c = self.for_user(user_id);
        c.tenant_sse = Some(TenantSse {
            user_id: user_id.to_string(),
            provider,
        });
        c
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

    /// Mimir `SSEBucketClient.getCustomS3SSEConfig`.
    fn tenant_sse_override(&self) -> Result<Option<s3::ServerSideEncryption>, BucketError> {
        let Some(t) = &self.tenant_sse else {
            return Ok(None);
        };
        let sse_type = t.provider.s3_sse_type(&t.user_id);
        if sse_type.is_empty() {
            return Ok(None);
        }
        s3::build_minio_sse(
            &sse_type,
            &t.provider.s3_sse_kms_key_id(&t.user_id),
            &t.provider.s3_sse_kms_encryption_context(&t.user_id),
        )
        .map_err(|reason| BucketError::TenantSse {
            user: t.user_id.clone(),
            reason,
        })
    }

    fn timer(&self, op: &'static str) -> Option<metrics::OpTimer<'_>> {
        self.metrics.as_deref().map(|m| m.start(op))
    }

    async fn upload_source(&self, name: &str, source: s3::UploadSource) -> Result<(), BucketError> {
        let timer = self.timer(metrics::OP_UPLOAD);
        let size = match &source {
            s3::UploadSource::Bytes(b) => b.len() as u64,
            s3::UploadSource::File { len, .. } => *len,
        };
        let result = async {
            let sse = self.tenant_sse_override()?;
            match &self.backend {
                Backend::S3(b) => {
                    b.upload(&self.full_name(name), source, sse).await?;
                    Ok(())
                }
                Backend::Store(store) => {
                    let path = self.object_path(name)?;
                    store_upload(store, &path, source).await
                }
            }
        }
        .await;
        if let Some(t) = timer {
            // minio-go reads S3 upload bodies through io.ReaderAt, which bypasses the
            // timingReader byte counter; the other providers read through io.Reader.
            let transferred = match (&self.backend, &result) {
                (Backend::S3(_), _) => 0,
                (_, Ok(())) => size,
                (_, Err(_)) => 0,
            };
            t.finish_upload(result.is_ok(), transferred);
        }
        result
    }

    /// Uploads `data` as object `name`.
    pub async fn upload(&self, name: &str, data: impl Into<Bytes>) -> Result<(), BucketError> {
        self.upload_source(name, s3::UploadSource::Bytes(data.into()))
            .await
    }

    /// Uploads a local file at `src_path` as object `name` (objstore `UploadFile`).
    pub async fn upload_file(
        &self,
        name: &str,
        src_path: impl AsRef<Path>,
    ) -> Result<(), BucketError> {
        let src_path = src_path.as_ref();
        let meta = tokio::fs::metadata(src_path).await?;
        self.upload_source(
            name,
            s3::UploadSource::File {
                path: src_path.to_path_buf(),
                len: meta.len(),
            },
        )
        .await
    }

    async fn read_op(
        &self,
        op: &'static str,
        name: &str,
        range: Option<(u64, u64)>,
    ) -> Result<Bytes, BucketError> {
        let timer = self.timer(op);
        let result: Result<Bytes, BucketError> = match &self.backend {
            Backend::S3(b) => {
                let full = self.full_name(name);
                let r = match range {
                    None => b.get(&full).await,
                    Some((off, len)) => b.get_range(&full, off as i64, len as i64).await,
                };
                r.map_err(|e| {
                    if e.is_obj_not_found() {
                        BucketError::NotFound(name.to_string())
                    } else {
                        BucketError::S3(e)
                    }
                })
            }
            Backend::Store(store) => async {
                let path = self.object_path(name)?;
                let res = match range {
                    None => store.get(&path).await,
                    Some((off, len)) => {
                        let opts = GetOptions {
                            range: Some(GetRange::Bounded(
                                (off as usize)..((off + len) as usize),
                            )),
                            ..Default::default()
                        };
                        store.get_opts(&path, opts).await
                    }
                };
                match res {
                    Ok(res) => Ok(res.bytes().await?),
                    Err(object_store::Error::NotFound { .. }) => {
                        Err(BucketError::NotFound(name.to_string()))
                    }
                    Err(e) => Err(BucketError::Store(e)),
                }
            }
            .await,
        };
        if let Some(t) = timer {
            let expected = self.expected_not_found && matches!(result, Err(BucketError::NotFound(_)));
            t.finish_read(result.as_ref().map(|b| b.len() as u64).map_err(|_| ()), expected);
        }
        result
    }

    /// Downloads the entire object `name`.
    pub async fn download(&self, name: &str) -> Result<Bytes, BucketError> {
        self.read_op(metrics::OP_GET, name, None).await
    }

    /// Downloads `length` bytes starting at `offset` from object `name`.
    pub async fn get_range(
        &self,
        name: &str,
        offset: u64,
        length: u64,
    ) -> Result<Bytes, BucketError> {
        self.read_op(metrics::OP_GET_RANGE, name, Some((offset, length)))
            .await
    }

    /// Returns `true` if object `name` exists in the bucket.
    pub async fn exists(&self, name: &str) -> Result<bool, BucketError> {
        let timer = self.timer(metrics::OP_EXISTS);
        let result = match &self.backend {
            Backend::S3(b) => b.exists(&self.full_name(name)).await.map_err(BucketError::S3),
            Backend::Store(store) => async {
                let path = self.object_path(name)?;
                match store.head(&path).await {
                    Ok(_) => Ok(true),
                    Err(object_store::Error::NotFound { .. }) => Ok(false),
                    Err(e) => Err(BucketError::Store(e)),
                }
            }
            .await,
        };
        if let Some(t) = timer {
            t.finish_simple(result.is_ok());
        }
        result
    }

    async fn delete_op(&self, name: &str, strict: bool) -> Result<(), BucketError> {
        let timer = self.timer(metrics::OP_DELETE);
        let result = match &self.backend {
            // S3 DeleteObject succeeds for missing objects (minio-go RemoveObject).
            Backend::S3(b) => b.delete(&self.full_name(name)).await.map_err(BucketError::S3),
            Backend::Store(store) => async {
                let path = self.object_path(name)?;
                match store.delete(&path).await {
                    Ok(()) => Ok(()),
                    Err(object_store::Error::NotFound { .. }) if strict => {
                        Err(BucketError::NotFound(name.to_string()))
                    }
                    Err(object_store::Error::NotFound { .. }) => Ok(()),
                    Err(e) => Err(BucketError::Store(e)),
                }
            }
            .await,
        };
        if let Some(t) = timer {
            t.finish_simple(result.is_ok());
        }
        result
    }

    /// Deletes object `name`. Succeeds if the object does not exist (matching objstore semantics
    /// when deleting missing objects or cleaning up).
    pub async fn delete(&self, name: &str) -> Result<(), BucketError> {
        self.delete_op(name, false).await
    }

    /// Deletes object `name`, returning `BucketError::NotFound` if the object does not exist
    /// (matching Go `objstore.Bucket.Delete` for strict deletion checks such as `DeleteNoCompactMarker`).
    pub async fn delete_strict(&self, name: &str) -> Result<(), BucketError> {
        self.delete_op(name, true).await
    }

    /// Returns object metadata attributes (size and last modified unix seconds).
    pub async fn attributes(&self, name: &str) -> Result<ObjectAttributes, BucketError> {
        let timer = self.timer(metrics::OP_ATTRIBUTES);
        let result = match &self.backend {
            Backend::S3(b) => b
                .attributes(&self.full_name(name))
                .await
                .map(|a| ObjectAttributes {
                    size: a.size.max(0) as u64,
                    last_modified_unix_secs: a.last_modified.timestamp(),
                })
                .map_err(|e| {
                    if e.is_obj_not_found() {
                        BucketError::NotFound(name.to_string())
                    } else {
                        BucketError::S3(e)
                    }
                }),
            Backend::Store(store) => async {
                let path = self.object_path(name)?;
                match store.head(&path).await {
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
            .await,
        };
        if let Some(t) = timer {
            t.finish_simple(result.is_ok());
        }
        result
    }

    async fn iter_op(&self, dir: &str, recursive: bool) -> Result<Vec<String>, BucketError> {
        let timer = self.timer(metrics::OP_ITER);
        let result = self.iter_inner(dir, recursive).await;
        if let Some(t) = timer {
            t.finish_iter(result.is_ok());
        }
        result
    }

    async fn iter_inner(&self, dir: &str, recursive: bool) -> Result<Vec<String>, BucketError> {
        let full = self.full_name(dir);
        if let Backend::S3(b) = &self.backend {
            let entries = b.iter(&full, recursive).await?;
            return Ok(entries
                .into_iter()
                .map(|e| self.strip_prefix(&e))
                .filter(|e| !e.is_empty())
                .collect());
        }
        let Backend::Store(store) = &self.backend else {
            unreachable!()
        };
        let prefix = if full.is_empty() {
            None
        } else {
            Some(
                ObjectPath::parse(&full)
                    .map_err(|e| BucketError::InvalidPath(full.clone(), e.to_string()))?,
            )
        };
        if recursive {
            let mut stream = store.list(prefix.as_ref());
            let mut keys = Vec::new();
            loop {
                match stream.try_next().await {
                    Ok(Some(meta)) => keys.push(self.strip_prefix(meta.location.as_ref())),
                    Ok(None) => break,
                    Err(object_store::Error::NotFound { .. }) => break,
                    Err(e) => return Err(BucketError::Store(e)),
                }
            }
            keys.sort();
            return Ok(keys);
        }
        let list_res = match store.list_with_delimiter(prefix.as_ref()).await {
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

    /// Non-recursively lists immediate entries under `dir` (matching Thanos `objstore.Bucket.Iter`).
    /// Subdirectories are returned with a trailing `/` and configured prefixes are stripped.
    pub async fn iter_dir(&self, dir: &str) -> Result<Vec<String>, BucketError> {
        self.iter_op(dir, false).await
    }

    /// Recursively lists all object keys under `prefix` with configured prefixes stripped
    /// (objstore `Iter` with `WithRecursiveIter()`).
    pub async fn list_all(&self, prefix: &str) -> Result<Vec<String>, BucketError> {
        self.iter_op(prefix, true).await
    }

    /// Recursively deletes all objects under `prefix` and returns the count of deleted objects.
    /// Ported from `DeletePrefix` in `pkg/storage/bucket/bucket_util.go` (one non-recursive
    /// `Iter` per directory level, one `Delete` per object).
    pub fn delete_prefix<'a>(
        &'a self,
        prefix: &'a str,
    ) -> futures::future::BoxFuture<'a, Result<usize, BucketError>> {
        Box::pin(async move {
            let mut deleted = 0usize;
            for name in self.iter_dir(prefix).await? {
                if name.ends_with(DIR_DELIM) {
                    deleted += self.delete_prefix(&name).await?;
                    continue;
                }
                self.delete(&name).await?;
                deleted += 1;
            }
            Ok(deleted)
        })
    }
}

struct UploadAbortGuard(Option<Box<dyn object_store::MultipartUpload>>);

impl Drop for UploadAbortGuard {
    fn drop(&mut self) {
        if let Some(mut upload) = self.0.take() {
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    let _ = upload.abort().await;
                });
            }
        }
    }
}

/// `object_store` upload of a byte buffer or a file (payloads above 16 MiB are streamed with a
/// multipart upload using dynamically sized parts and 4-way concurrency).
async fn store_upload(
    store: &Arc<dyn ObjectStore>,
    path: &ObjectPath,
    source: s3::UploadSource,
) -> Result<(), BucketError> {
    const MULTIPART_THRESHOLD: u64 = 16 * 1024 * 1024;
    const BASE_CHUNK_SIZE: usize = 8 * 1024 * 1024;
    const MAX_PARTS: u64 = 10_000;
    const PART_CONCURRENCY: usize = 4;

    let len = match &source {
        s3::UploadSource::Bytes(data) => data.len() as u64,
        s3::UploadSource::File { len, .. } => *len,
    };
    if len <= MULTIPART_THRESHOLD {
        let data = match source {
            s3::UploadSource::Bytes(data) => data,
            s3::UploadSource::File { path: src_path, .. } => {
                Bytes::from(tokio::fs::read(&src_path).await?)
            }
        };
        store.put(path, PutPayload::from_bytes(data)).await?;
        return Ok(());
    }

    let chunk_size = BASE_CHUNK_SIZE.max(len.div_ceil(MAX_PARTS) as usize);
    let upload = store.put_multipart(path).await?;
    let mut guard = UploadAbortGuard(Some(upload));
    let mut in_flight: FuturesUnordered<object_store::UploadPart> = FuturesUnordered::new();

    match source {
        s3::UploadSource::Bytes(data) => {
            let mut off = 0usize;
            while off < data.len() {
                let end = (off + chunk_size).min(data.len());
                let chunk = data.slice(off..end);
                off = end;
                let Some(upload_ref) = guard.0.as_mut() else {
                    return Err(BucketError::Config("multipart upload missing".into()));
                };
                let fut = upload_ref.put_part(PutPayload::from_bytes(chunk));
                in_flight.push(fut);
                if in_flight.len() >= PART_CONCURRENCY {
                    if let Some(Err(e)) = in_flight.next().await {
                        if let Some(mut u) = guard.0.take() {
                            let _ = u.abort().await;
                        }
                        return Err(BucketError::Store(e));
                    }
                }
            }
        }
        s3::UploadSource::File { path: src_path, .. } => {
            let mut file = match tokio::fs::File::open(&src_path).await {
                Ok(f) => f,
                Err(e) => {
                    if let Some(mut u) = guard.0.take() {
                        let _ = u.abort().await;
                    }
                    return Err(BucketError::Io(e));
                }
            };
            let mut buf = vec![0u8; chunk_size];
            loop {
                let mut filled = 0usize;
                while filled < buf.len() {
                    match file.read(&mut buf[filled..]).await {
                        Ok(0) => break,
                        Ok(n) => filled += n,
                        Err(e) => {
                            if let Some(mut u) = guard.0.take() {
                                let _ = u.abort().await;
                            }
                            return Err(BucketError::Io(e));
                        }
                    }
                }
                if filled == 0 {
                    break;
                }
                let chunk = Bytes::copy_from_slice(&buf[..filled]);
                let Some(upload_ref) = guard.0.as_mut() else {
                    return Err(BucketError::Config("multipart upload missing".into()));
                };
                let fut = upload_ref.put_part(PutPayload::from_bytes(chunk));
                in_flight.push(fut);
                if in_flight.len() >= PART_CONCURRENCY {
                    if let Some(Err(e)) = in_flight.next().await {
                        if let Some(mut u) = guard.0.take() {
                            let _ = u.abort().await;
                        }
                        return Err(BucketError::Store(e));
                    }
                }
            }
        }
    }

    while let Some(res) = in_flight.next().await {
        if let Err(e) = res {
            if let Some(mut u) = guard.0.take() {
                let _ = u.abort().await;
            }
            return Err(BucketError::Store(e));
        }
    }

    let Some(upload_ref) = guard.0.as_mut() else {
        return Err(BucketError::Config("multipart upload missing".into()));
    };
    let res = upload_ref.complete().await;
    match res {
        Ok(_) => {
            guard.0 = None;
            Ok(())
        }
        Err(e) => {
            if let Some(mut u) = guard.0.take() {
                let _ = u.abort().await;
            }
            Err(BucketError::Store(e))
        }
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

    #[tokio::test]
    async fn test_with_expected_not_found_does_not_increment_failures() {
        let dir = tempfile::tempdir().unwrap();
        let reg = prometheus::Registry::new();
        let mut cfg = BucketConfig::default();
        cfg.backend_cfg.backend = "filesystem".to_string();
        cfg.backend_cfg.filesystem.dir = dir.path().to_string_lossy().into_owned();
        let bkt = BucketClient::new_client(&cfg, "usage-stats", Some(&reg)).unwrap();

        assert!(matches!(
            bkt.with_expected_not_found().download("missing.json").await,
            Err(BucketError::NotFound(_))
        ));

        let mfs = reg.gather();
        let failures: f64 = mfs
            .iter()
            .filter(|mf| mf.get_name() == "thanos_objstore_bucket_operation_failures_total")
            .flat_map(|mf| mf.get_metric())
            .map(|m| m.get_counter().get_value())
            .sum();
        assert_eq!(failures, 0.0);
    }
}
