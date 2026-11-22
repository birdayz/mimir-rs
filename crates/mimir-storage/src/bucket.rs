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

use std::path::{Path, PathBuf};
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
pub const READ_COMPARTMENT_ID_PLACEHOLDER: &str = "<read-compartment-id>";

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
    /// Thanos `objstore.ErrUploadOptionInvalid`.
    #[error("upload option is invalid: {0}")]
    UploadOptionInvalid(String),
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

    /// Matching Thanos `objstore.Bucket.IsAccessDeniedErr`:
    /// - S3: `AccessDenied`
    /// - GCS: HTTP 403 / `PermissionDenied`
    /// - Azure: `AuthorizationPermissionMismatch` or `InsufficientAccountPermissions`
    /// - Filesystem / InMemory: `false`
    pub fn is_access_denied(&self) -> bool {
        match self {
            BucketError::S3(e) => e.is_access_denied(),
            BucketError::Store(object_store::Error::PermissionDenied { .. }) => true,
            _ => false,
        }
    }

    /// Matching Thanos `objstore.Bucket.IsConditionNotMetErr`.
    pub fn is_condition_not_met(&self) -> bool {
        match self {
            BucketError::S3(e) => e.is_condition_not_met(),
            BucketError::Store(
                object_store::Error::Precondition { .. } | object_store::Error::AlreadyExists { .. },
            ) => true,
            _ => false,
        }
    }
}

/// Options for [`BucketClient::iter_with_attributes`] (Thanos `objstore.IterOption`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IterOptions {
    pub recursive: bool,
    pub updated_at: bool,
}

impl IterOptions {
    pub fn recursive() -> Self {
        Self {
            recursive: true,
            updated_at: false,
        }
    }

    pub fn updated_at() -> Self {
        Self {
            recursive: false,
            updated_at: true,
        }
    }

    pub fn with_recursive(mut self, recursive: bool) -> Self {
        self.recursive = recursive;
        self
    }

    pub fn with_updated_at(mut self, updated_at: bool) -> Self {
        self.updated_at = updated_at;
        self
    }
}

/// Entry yielded by [`BucketClient::iter_with_attributes`] (Thanos `objstore.IterObjectAttributes`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IterObjectAttributes {
    pub name: String,
    pub last_modified: Option<SystemTime>,
}

/// Options for [`BucketClient::download_dir`] (Thanos `objstore.DownloadOption`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DownloadDirOptions {
    pub concurrency: usize,
    pub ignored_paths: Vec<String>,
}

impl Default for DownloadDirOptions {
    fn default() -> Self {
        Self {
            concurrency: 1,
            ignored_paths: Vec::new(),
        }
    }
}

/// Options for [`BucketClient::upload_dir`] (Thanos `objstore.UploadOption`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadDirOptions {
    pub concurrency: usize,
}

impl Default for UploadDirOptions {
    fn default() -> Self {
        Self { concurrency: 1 }
    }
}

/// Version token for conditional uploads (Thanos `objstore.ObjectVersion`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectVersion {
    Generation(String),
    ETag(String),
}

/// Conditional upload constraint (Thanos `objstore.ObjectUploadOption`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UploadCondition {
    IfNotExists,
    IfMatch(ObjectVersion),
    IfNotMatch(ObjectVersion),
}

/// Options for [`BucketClient::upload_with_options`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ObjectUploadOptions {
    pub content_type: Option<String>,
    pub condition: Option<UploadCondition>,
}

/// Ported from `github.com/grafana/mimir/pkg/storage/bucket/client.go::ReadCompartmentConfig`.
/// Replaces `<read-compartment-id>` in the active backend's bucket/container/directory name
/// without mutating the input config.
pub fn read_compartment_config(cfg: &BucketConfig, read_compartment_id: i32) -> BucketConfig {
    let mut out = cfg.clone();
    let id_str = read_compartment_id.to_string();
    match out.backend_cfg.backend.as_str() {
        "s3" => {
            out.backend_cfg.s3.bucket_name = out
                .backend_cfg
                .s3
                .bucket_name
                .replace(READ_COMPARTMENT_ID_PLACEHOLDER, &id_str);
        }
        "gcs" => {
            out.backend_cfg.gcs.bucket_name = out
                .backend_cfg
                .gcs
                .bucket_name
                .replace(READ_COMPARTMENT_ID_PLACEHOLDER, &id_str);
        }
        "azure" => {
            out.backend_cfg.azure.container_name = out
                .backend_cfg
                .azure
                .container_name
                .replace(READ_COMPARTMENT_ID_PLACEHOLDER, &id_str);
        }
        "swift" => {
            out.backend_cfg.swift.container_name = out
                .backend_cfg
                .swift
                .container_name
                .replace(READ_COMPARTMENT_ID_PLACEHOLDER, &id_str);
        }
        "filesystem" => {
            out.backend_cfg.filesystem.dir = out
                .backend_cfg
                .filesystem
                .dir
                .replace(READ_COMPARTMENT_ID_PLACEHOLDER, &id_str);
        }
        _ => {}
    }
    out
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

#[derive(Debug)]
struct LazyFilesystem {
    root: PathBuf,
    store: parking_lot::Mutex<Option<Arc<LocalFileSystem>>>,
}

impl LazyFilesystem {
    fn new(root: PathBuf) -> Self {
        Self {
            root,
            store: parking_lot::Mutex::new(None),
        }
    }

    async fn get_existing(&self) -> Result<Option<Arc<dyn ObjectStore>>, BucketError> {
        if let Some(store) = self.store.lock().as_ref().cloned() {
            return Ok(Some(store as Arc<dyn ObjectStore>));
        }
        match tokio::fs::metadata(&self.root).await {
            Ok(_) => {
                let fs = LocalFileSystem::new_with_prefix(&self.root)?
                    .with_automatic_cleanup(true);
                let arc = Arc::new(fs);
                let mut guard = self.store.lock();
                let stored = guard.get_or_insert_with(|| Arc::clone(&arc));
                Ok(Some(Arc::clone(stored) as Arc<dyn ObjectStore>))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(BucketError::Io(e)),
        }
    }

    async fn get_or_create(&self) -> Result<Arc<dyn ObjectStore>, BucketError> {
        tokio::fs::create_dir_all(&self.root).await?;
        if let Some(store) = self.store.lock().as_ref().cloned() {
            return Ok(store as Arc<dyn ObjectStore>);
        }
        let fs = LocalFileSystem::new_with_prefix(&self.root)?
            .with_automatic_cleanup(true);
        let arc = Arc::new(fs);
        let mut guard = self.store.lock();
        let stored = guard.get_or_insert_with(|| Arc::clone(&arc));
        Ok(Arc::clone(stored) as Arc<dyn ObjectStore>)
    }
}

#[derive(Clone)]
enum Backend {
    Store(Arc<dyn ObjectStore>),
    Filesystem(Arc<LazyFilesystem>),
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
    fs_root: Option<PathBuf>,
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
            fs_root: None,
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
    /// Matches Thanos `filesystem.NewBucket`: resolves `dir` to an absolute path
    /// without eagerly creating directories on disk until an upload/write occurs.
    pub fn new_filesystem(dir: impl AsRef<Path>) -> Result<Self, BucketError> {
        let dir = dir.as_ref();
        let abs = std::path::absolute(dir).unwrap_or_else(|_| dir.to_path_buf());
        // thanos filesystem `Name()`: "fs: <absolute root dir>".
        let mut client = Self::from_backend(
            Backend::Filesystem(Arc::new(LazyFilesystem::new(abs.clone()))),
            format!("fs: {}", abs.to_string_lossy()),
            "FILESYSTEM",
        );
        client.fs_root = Some(abs);
        Ok(client)
    }

    /// Wraps an arbitrary `Arc<dyn ObjectStore>` (useful for decorators/fault injection in tests).
    pub fn from_object_store(
        store: Arc<dyn ObjectStore>,
        name: impl Into<String>,
        provider: impl Into<String>,
    ) -> Self {
        Self::from_backend(Backend::Store(store), name.into(), &provider.into())
    }

    /// Wraps an [`s3::S3Bucket`] into a [`BucketClient`].
    pub fn from_s3(bucket: s3::S3Bucket) -> Self {
        let name = bucket.name().to_string();
        Self::from_backend(Backend::S3(Arc::new(bucket)), name, "S3")
    }

    /// Attaches [`metrics::BucketMetrics`] registered on `registry` for `component`.
    pub fn with_metrics(
        mut self,
        registry: &prometheus::Registry,
        component: &str,
    ) -> Result<Self, BucketError> {
        self.metrics = Some(Arc::new(metrics::BucketMetrics::register(
            registry,
            component,
            &self.name,
        )?));
        Ok(self)
    }

    /// Shares the registered [`metrics::BucketMetrics`] from `other` (used when swapping the
    /// underlying bucket while preserving metrics series).
    pub fn with_shared_metrics_from(mut self, other: &BucketClient) -> Self {
        self.metrics = other.metrics.clone();
        self
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
        if !cfg.storage_prefix.is_empty()
            && !cfg.storage_prefix.bytes().all(|b| b.is_ascii_alphanumeric())
        {
            return Err(BucketError::InvalidPrefix(cfg.storage_prefix.clone()));
        }

        let b = &cfg.backend_cfg;
        if !matches!(b.backend.as_str(), "filesystem" | "s3" | "gcs" | "azure") {
            return Err(BucketError::UnsupportedBackend(b.backend.clone()));
        }
        cfg.validate()
            .map_err(|e| BucketError::Config(e.to_string()))?;
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
            "azure" => {
                let timeout = if component == "sanity-check" {
                    Some(std::time::Duration::from_secs(10))
                } else {
                    Some(azure::DEFAULT_CONTAINER_CHECK_TIMEOUT)
                };
                Self::from_backend(
                    Backend::Store(azure::new_store_with_timeout(&b.azure, timeout)?),
                    b.azure.container_name.clone(),
                    "AZURE",
                )
            }
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
        // Validate per-tenant SSE before starting OpTimer, matching Go SSEBucketClient wrapping
        // InstrumentedBucket from the outside.
        let sse = self.tenant_sse_override()?;
        let timer = self.timer(metrics::OP_UPLOAD);
        let size = match &source {
            s3::UploadSource::Bytes(b) => b.len() as u64,
            s3::UploadSource::File { len, .. } => *len,
        };
        let result = async {
            if name.trim_start_matches('/').is_empty() {
                return Err(BucketError::InvalidPath(
                    name.to_string(),
                    "object name cannot be empty".to_string(),
                ));
            }
            match &self.backend {
                Backend::S3(b) => {
                    b.upload(&self.full_name(name), source, sse).await?;
                    Ok(())
                }
                Backend::Store(store) => {
                    let path = self.object_path(name)?;
                    store_upload(store, &path, source).await
                }
                Backend::Filesystem(fs) => {
                    let path = self.object_path(name)?;
                    let store = fs.get_or_create().await?;
                    store_upload(&store, &path, source)
                        .await
                        .map_err(|e| match e {
                            BucketError::Store(se) => map_fs_store_error(se),
                            other => other,
                        })
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

    /// Uploads `data` as object `name` with Thanos/Mimir upload options
    /// (`ContentType`, `IfNotExists`, `IfMatch`, `IfNotMatch`).
    pub async fn upload_with_options(
        &self,
        name: &str,
        data: impl Into<Bytes>,
        opts: ObjectUploadOptions,
    ) -> Result<(), BucketError> {
        let data = data.into();
        let len = data.len() as u64;
        let sse = self.tenant_sse_override()?;
        if name.trim_start_matches('/').is_empty() {
            let timer = self.timer(metrics::OP_UPLOAD);
            let err = BucketError::InvalidPath(
                name.to_string(),
                "object name cannot be empty".to_string(),
            );
            if let Some(t) = timer {
                t.finish_upload(false, 0);
            }
            return Err(err);
        }
        // Delegate directly to self.upload() on backends that don't support custom options so
        // OP_UPLOAD metrics are recorded once (not double-counted).
        match &self.backend {
            Backend::Filesystem(_) => {
                if opts.condition.is_some() {
                    let timer = self.timer(metrics::OP_UPLOAD);
                    if let Some(t) = timer {
                        t.finish_upload(false, 0);
                    }
                    return Err(BucketError::UploadOptionInvalid(
                        "conditional upload is not supported on filesystem".into(),
                    ));
                }
                return self.upload(name, data).await;
            }
            Backend::S3(b) => {
                let timer = self.timer(metrics::OP_UPLOAD);
                let result: Result<(), BucketError> = async {
                    let (if_match, if_none_match) = match opts.condition {
                        None => (None, None),
                        Some(UploadCondition::IfNotExists) => (None, Some("*".to_string())),
                        Some(UploadCondition::IfMatch(ObjectVersion::ETag(e))) => (Some(e), None),
                        Some(UploadCondition::IfNotMatch(ObjectVersion::ETag(e))) => (None, Some(e)),
                        Some(UploadCondition::IfMatch(ObjectVersion::Generation(_)))
                        | Some(UploadCondition::IfNotMatch(ObjectVersion::Generation(_))) => {
                            return Err(BucketError::UploadOptionInvalid(
                                "S3 conditional uploads require ETag object versions".into(),
                            ));
                        }
                    };
                    b.upload_with_options(
                        &self.full_name(name),
                        s3::UploadSource::Bytes(data),
                        sse,
                        opts.content_type,
                        if_match,
                        if_none_match,
                    )
                    .await?;
                    Ok(())
                }
                .await;
                if let Some(t) = timer {
                    t.finish_upload(result.is_ok(), 0);
                }
                result
            }
            Backend::Store(store) => {
                let timer = self.timer(metrics::OP_UPLOAD);
                let result: Result<(), BucketError> = async {
                    if let Some(cond) = &opts.condition {
                        if self.provider == "GCS" {
                            let ver = match cond {
                                UploadCondition::IfNotExists => None,
                                UploadCondition::IfMatch(v) | UploadCondition::IfNotMatch(v) => Some(v),
                            };
                            if let Some(v) = ver {
                                match v {
                                    ObjectVersion::Generation(s) => {
                                        if s.parse::<i64>().is_err() {
                                            return Err(BucketError::UploadOptionInvalid(format!(
                                                "GCS conditional upload generation {s:?} is invalid"
                                            )));
                                        }
                                    }
                                    ObjectVersion::ETag(_) => {
                                        return Err(BucketError::UploadOptionInvalid(
                                            "GCS conditional uploads require generation object versions".into(),
                                        ));
                                    }
                                }
                            }
                        }
                    }
                    let path = self.object_path(name)?;
                    let mut put_opts = object_store::PutOptions::default();
                    if let Some(ct) = opts.content_type {
                        put_opts
                            .attributes
                            .insert(object_store::Attribute::ContentType, ct.into());
                    }
                    if let Some(cond) = opts.condition {
                        match cond {
                            UploadCondition::IfNotExists => {
                                put_opts.mode = object_store::PutMode::Create;
                            }
                            UploadCondition::IfMatch(ver) => {
                                let (e_tag, version) = match ver {
                                    ObjectVersion::ETag(e) => (Some(e), None),
                                    ObjectVersion::Generation(g) => (None, Some(g)),
                                };
                                put_opts.mode = object_store::PutMode::Update(
                                    object_store::UpdateVersion { e_tag, version },
                                );
                            }
                            UploadCondition::IfNotMatch(ver) => match ver {
                                ObjectVersion::Generation(g) if self.provider == "GCS" => {
                                    let gen_val = g.parse::<i64>().map_err(|_| {
                                        BucketError::UploadOptionInvalid(format!(
                                            "GCS conditional upload generation {g:?} is invalid"
                                        ))
                                    })?;
                                    put_opts
                                        .tags
                                        .push(gcs::IF_GEN_NOT_MATCH_TAG, &gen_val.to_string());
                                }
                                _ => {
                                    return Err(BucketError::UploadOptionInvalid(
                                        "IfNotMatch is not supported for this backend".into(),
                                    ));
                                }
                            },
                        }
                    }
                    store
                        .put_opts(&path, PutPayload::from_bytes(data), put_opts)
                        .await?;
                    Ok(())
                }
                .await;
                if let Some(t) = timer {
                    let transferred = if result.is_ok() { len } else { 0 };
                    t.finish_upload(result.is_ok(), transferred);
                }
                result
            }
        }
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
        range: Option<(u64, Option<u64>)>,
    ) -> Result<Bytes, BucketError> {
        let timer = self.timer(op);
        let result: Result<Bytes, BucketError> = if name.trim_start_matches('/').is_empty() {
            Err(BucketError::InvalidPath(
                name.to_string(),
                "object name cannot be empty".to_string(),
            ))
        } else {
            match &self.backend {
                Backend::S3(b) => {
                    let full = self.full_name(name);
                    let r = match range {
                        None => b.get(&full).await,
                        Some((off, Some(len))) => {
                            let off_i64 = i64::try_from(off).map_err(|_| {
                                BucketError::InvalidPath(
                                    name.to_string(),
                                    "range offset overflow".into(),
                                )
                            })?;
                            let len_i64 = i64::try_from(len).map_err(|_| {
                                BucketError::InvalidPath(
                                    name.to_string(),
                                    "range length overflow".into(),
                                )
                            })?;
                            let _ = off_i64.checked_add(len_i64).ok_or_else(|| {
                                BucketError::InvalidPath(
                                    name.to_string(),
                                    "range offset + length overflow".into(),
                                )
                            })?;
                            b.get_range(&full, off_i64, len_i64).await
                        }
                        Some((off, None)) => {
                            let off_i64 = i64::try_from(off).map_err(|_| {
                                BucketError::InvalidPath(
                                    name.to_string(),
                                    "range offset overflow".into(),
                                )
                            })?;
                            b.get_range(&full, off_i64, -1).await
                        }
                    };
                    r.map_err(|e| {
                        if e.is_obj_not_found() {
                            BucketError::NotFound(name.to_string())
                        } else {
                            BucketError::S3(e)
                        }
                    })
                }
                Backend::Store(store) => {
                    let path = self.object_path(name)?;
                    store_read_op(store, &path, name, range).await
                }
                Backend::Filesystem(fs) => {
                    let path = self.object_path(name)?;
                    match fs.get_existing().await {
                        Ok(Some(store)) => fs_read_op(&store, &path, name, range).await,
                        Ok(None) => Err(BucketError::NotFound(name.to_string())),
                        Err(e) => Err(e),
                    }
                }
            }
        };
        if let Some(t) = timer {
            let expected =
                self.expected_not_found && result.as_ref().is_err_and(BucketError::is_not_found);
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
        self.read_op(metrics::OP_GET_RANGE, name, Some((offset, Some(length))))
            .await
    }

    /// Downloads bytes starting at `offset` to the end of object `name` (Thanos `GetRange(..., off, -1)`).
    pub async fn get_range_from(&self, name: &str, offset: u64) -> Result<Bytes, BucketError> {
        self.read_op(metrics::OP_GET_RANGE, name, Some((offset, None)))
            .await
    }

    /// Returns `true` if object `name` exists in the bucket.
    pub async fn exists(&self, name: &str) -> Result<bool, BucketError> {
        let timer = self.timer(metrics::OP_EXISTS);
        let result = if name.trim_start_matches('/').is_empty() {
            Err(BucketError::InvalidPath(
                name.to_string(),
                "object name cannot be empty".to_string(),
            ))
        } else {
            match &self.backend {
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
            Backend::Filesystem(fs) => async {
                let path = self.object_path(name)?;
                let Some(store) = fs.get_existing().await? else {
                    return Ok(false);
                };
                match store.head(&path).await {
                    Ok(_) => Ok(true),
                    Err(object_store::Error::NotFound { .. }) => Ok(false),
                    Err(e) => Err(map_fs_store_error(e)),
                }
            }
            .await,
            }
        };
        if let Some(t) = timer {
            let expected =
                self.expected_not_found && result.as_ref().is_err_and(BucketError::is_not_found);
            t.finish_simple(result.is_ok(), expected);
        }
        result
    }

    async fn delete_op(&self, name: &str, strict: bool) -> Result<(), BucketError> {
        let timer = self.timer(metrics::OP_DELETE);
        let result = if name.trim_start_matches('/').is_empty() {
            Err(BucketError::InvalidPath(
                name.to_string(),
                "object name cannot be empty".to_string(),
            ))
        } else {
            match &self.backend {
            // S3 DeleteObject succeeds for missing objects (minio-go RemoveObject).
            Backend::S3(b) => b.delete(&self.full_name(name)).await.map_err(BucketError::S3),
            // GCS, Azure, and InMemory Delete return NotFound for missing objects (matching Go Thanos).
            Backend::Store(store) => async {
                let path = self.object_path(name)?;
                match store.delete(&path).await {
                    Ok(()) => Ok(()),
                    Err(object_store::Error::NotFound { .. }) => {
                        Err(BucketError::NotFound(name.to_string()))
                    }
                    Err(e) => Err(BucketError::Store(e)),
                }
            }
            .await,
            // Filesystem Delete succeeds for missing files (os.RemoveAll), while delete_strict()
            // preserves explicit missing-object checks (e.g. w16_apc_09, w19_mod_01).
            Backend::Filesystem(fs) => async {
                let path = self.object_path(name)?;
                let Some(store) = fs.get_existing().await? else {
                    return if strict {
                        Err(BucketError::NotFound(name.to_string()))
                    } else {
                        Ok(())
                    };
                };
                match store.delete(&path).await {
                    Ok(()) => Ok(()),
                    Err(object_store::Error::NotFound { .. }) if strict => {
                        Err(BucketError::NotFound(name.to_string()))
                    }
                    Err(object_store::Error::NotFound { .. }) => Ok(()),
                    Err(e) => Err(map_fs_store_error(e)),
                }
            }
            .await,
            }
        };
        if let Some(t) = timer {
            let expected =
                self.expected_not_found && result.as_ref().is_err_and(BucketError::is_not_found);
            t.finish_simple(result.is_ok(), expected);
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
        let result = if name.trim_start_matches('/').is_empty() {
            Err(BucketError::InvalidPath(
                name.to_string(),
                "object name cannot be empty".to_string(),
            ))
        } else {
            match &self.backend {
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
            Backend::Filesystem(fs) => async {
                let path = self.object_path(name)?;
                let Some(store) = fs.get_existing().await? else {
                    return Err(BucketError::NotFound(name.to_string()));
                };
                match store.head(&path).await {
                    Ok(meta) => Ok(ObjectAttributes {
                        size: meta.size as u64,
                        last_modified_unix_secs: meta.last_modified.timestamp(),
                    }),
                    Err(object_store::Error::NotFound { .. }) => {
                        Err(BucketError::NotFound(name.to_string()))
                    }
                    Err(e) => Err(map_fs_store_error(e)),
                }
            }
            .await,
            }
        };
        if let Some(t) = timer {
            let expected =
                self.expected_not_found && result.as_ref().is_err_and(BucketError::is_not_found);
            t.finish_simple(result.is_ok(), expected);
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
        let prefix = if full.is_empty() {
            None
        } else {
            Some(
                ObjectPath::parse(&full)
                    .map_err(|e| BucketError::InvalidPath(full.clone(), e.to_string()))?,
            )
        };
        let is_fs = matches!(&self.backend, Backend::Filesystem(_));
        let owned_fs_store: Arc<dyn ObjectStore>;
        let store: &Arc<dyn ObjectStore> = match &self.backend {
            Backend::Store(store) => store,
            Backend::Filesystem(fs) => {
                let Some(s) = fs.get_existing().await? else {
                    return Ok(Vec::new());
                };
                owned_fs_store = s;
                &owned_fs_store
            }
            Backend::S3(_) => unreachable!(),
        };
        if recursive {
            let mut stream = store.list(prefix.as_ref());
            let mut keys = Vec::new();
            loop {
                match stream.try_next().await {
                    Ok(Some(meta)) => keys.push(self.strip_prefix(meta.location.as_ref())),
                    Ok(None) => break,
                    Err(object_store::Error::NotFound { .. }) if is_fs => break,
                    Err(e) => {
                        return Err(if is_fs {
                            map_fs_store_error(e)
                        } else {
                            BucketError::Store(e)
                        });
                    }
                }
            }
            keys.sort();
            return Ok(keys);
        }
        let list_res = match store.list_with_delimiter(prefix.as_ref()).await {
            Ok(res) => res,
            Err(object_store::Error::NotFound { .. }) if is_fs => return Ok(Vec::new()),
            Err(e) => {
                return Err(if is_fs {
                    map_fs_store_error(e)
                } else {
                    BucketError::Store(e)
                });
            }
        };

        let mut entries =
            Vec::with_capacity(list_res.common_prefixes.len() + list_res.objects.len());
        for cp in list_res.common_prefixes {
            if let Some(root) = &self.fs_root {
                let mut dir_path = root.clone();
                for part in cp.parts() {
                    dir_path.push(part.as_ref());
                }
                if is_dir_empty(&dir_path)? {
                    continue;
                }
            }
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

    /// Lists entries under `dir` with optional recursion and `last_modified` attributes
    /// (Thanos `objstore.Bucket.IterWithAttributes`).
    pub async fn iter_with_attributes(
        &self,
        dir: &str,
        opts: IterOptions,
    ) -> Result<Vec<IterObjectAttributes>, BucketError> {
        let timer = self.timer(metrics::OP_ITER);
        let result = self.iter_with_attributes_inner(dir, opts).await;
        if let Some(t) = timer {
            t.finish_iter(result.is_ok());
        }
        result
    }

    async fn iter_with_attributes_inner(
        &self,
        dir: &str,
        opts: IterOptions,
    ) -> Result<Vec<IterObjectAttributes>, BucketError> {
        let full = self.full_name(dir);
        if let Backend::S3(b) = &self.backend {
            let entries = b.iter_with_attributes(&full, opts.recursive).await?;
            return Ok(entries
                .into_iter()
                .filter_map(|e| {
                    let name = self.strip_prefix(&e.name);
                    if name.is_empty() {
                        return None;
                    }
                    let last_modified = if opts.updated_at {
                        e.last_modified.map(SystemTime::from)
                    } else {
                        None
                    };
                    Some(IterObjectAttributes {
                        name,
                        last_modified,
                    })
                })
                .collect());
        }
        let prefix = if full.is_empty() {
            None
        } else {
            Some(
                ObjectPath::parse(&full)
                    .map_err(|e| BucketError::InvalidPath(full.clone(), e.to_string()))?,
            )
        };
        let is_fs = matches!(&self.backend, Backend::Filesystem(_));
        let owned_fs_store: Arc<dyn ObjectStore>;
        let store: &Arc<dyn ObjectStore> = match &self.backend {
            Backend::Store(store) => store,
            Backend::Filesystem(fs) => {
                let Some(s) = fs.get_existing().await? else {
                    return Ok(Vec::new());
                };
                owned_fs_store = s;
                &owned_fs_store
            }
            Backend::S3(_) => unreachable!(),
        };
        if opts.recursive {
            let mut stream = store.list(prefix.as_ref());
            let mut out = Vec::new();
            loop {
                match stream.try_next().await {
                    Ok(Some(meta)) => {
                        let name = self.strip_prefix(meta.location.as_ref());
                        if name.is_empty() {
                            continue;
                        }
                        let last_modified = if opts.updated_at {
                            if let Some(root) = &self.fs_root {
                                let mut p = root.clone();
                                for part in meta.location.parts() {
                                    p.push(part.as_ref());
                                }
                                std::fs::metadata(&p)?.modified().ok()
                            } else {
                                Some(SystemTime::from(meta.last_modified))
                            }
                        } else {
                            None
                        };
                        out.push(IterObjectAttributes {
                            name,
                            last_modified,
                        });
                    }
                    Ok(None) => break,
                    Err(object_store::Error::NotFound { .. }) if is_fs => break,
                    Err(e) => {
                        return Err(if is_fs {
                            map_fs_store_error(e)
                        } else {
                            BucketError::Store(e)
                        });
                    }
                }
            }
            out.sort_by(|a, b| a.name.cmp(&b.name));
            return Ok(out);
        }
        let list_res = match store.list_with_delimiter(prefix.as_ref()).await {
            Ok(res) => res,
            Err(object_store::Error::NotFound { .. }) if is_fs => return Ok(Vec::new()),
            Err(e) => {
                return Err(if is_fs {
                    map_fs_store_error(e)
                } else {
                    BucketError::Store(e)
                });
            }
        };
        let mut out = Vec::with_capacity(list_res.common_prefixes.len() + list_res.objects.len());
        for cp in list_res.common_prefixes {
            if let Some(root) = &self.fs_root {
                let mut dir_path = root.clone();
                for part in cp.parts() {
                    dir_path.push(part.as_ref());
                }
                if is_dir_empty(&dir_path)? {
                    continue;
                }
                let stripped = self.strip_prefix(cp.as_ref());
                if stripped.is_empty() {
                    continue;
                }
                let last_modified = if opts.updated_at {
                    std::fs::metadata(&dir_path)?.modified().ok()
                } else {
                    None
                };
                out.push(IterObjectAttributes {
                    name: format!("{stripped}{DIR_DELIM}"),
                    last_modified,
                });
            } else {
                let stripped = self.strip_prefix(cp.as_ref());
                if stripped.is_empty() {
                    continue;
                }
                let last_modified = if opts.updated_at && self.provider == "INMEMORY" {
                    // Thanos InMemBucket sets directory LastModified from a contained object.
                    let mut stream = store.list(Some(&cp));
                    match stream.try_next().await? {
                        Some(meta) => Some(SystemTime::from(meta.last_modified)),
                        None => None,
                    }
                } else {
                    None
                };
                out.push(IterObjectAttributes {
                    name: format!("{stripped}{DIR_DELIM}"),
                    last_modified,
                });
            }
        }
        for obj in list_res.objects {
            let stripped = self.strip_prefix(obj.location.as_ref());
            if stripped.is_empty() {
                continue;
            }
            let last_modified = if opts.updated_at {
                if let Some(root) = &self.fs_root {
                    let mut p = root.clone();
                    for part in obj.location.parts() {
                        p.push(part.as_ref());
                    }
                    std::fs::metadata(&p)?.modified().ok()
                } else {
                    Some(SystemTime::from(obj.last_modified))
                }
            } else {
                None
            };
            out.push(IterObjectAttributes {
                name: stripped,
                last_modified,
            });
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    /// Ported from `github.com/thanos-io/objstore/objstore.go::DownloadDir`.
    /// Downloads all objects under `src` into `dst`. On failure, cleans up `dst`.
    pub async fn download_dir(
        &self,
        original_src: &str,
        src: &str,
        dst: &Path,
        opts: DownloadDirOptions,
    ) -> Result<(), BucketError> {
        let res = self.download_dir_inner(original_src, src, dst, &opts).await;
        if res.is_err() {
            let _ = tokio::fs::remove_dir_all(dst).await;
        }
        res
    }

    fn download_dir_inner<'a>(
        &'a self,
        original_src: &'a str,
        src: &'a str,
        dst: &'a Path,
        opts: &'a DownloadDirOptions,
    ) -> futures::future::BoxFuture<'a, Result<(), BucketError>> {
        Box::pin(async move {
            tokio::fs::create_dir_all(dst).await?;
            let concurrency = opts.concurrency.max(1);
            let entries = self.iter_dir(src).await?;
            let orig_prefix = format!("{}/", original_src.trim_end_matches('/'));
            let mut futs = FuturesUnordered::new();
            for name in entries {
                let base = name
                    .trim_end_matches('/')
                    .rsplit('/')
                    .next()
                    .unwrap_or("")
                    .to_string();
                if base.is_empty()
                    || base == "."
                    || base == ".."
                    || base.contains('/')
                    || base.contains('\\')
                {
                    return Err(BucketError::InvalidPath(
                        name,
                        "unsafe basename in download_dir".into(),
                    ));
                }
                let child_dst = dst.join(&base);
                if name.ends_with(DIR_DELIM) {
                    futs.push(Box::pin(async move {
                        self.download_dir_inner(original_src, &name, &child_dst, opts)
                            .await
                    }) as futures::future::BoxFuture<'_, Result<(), BucketError>>);
                } else {
                    let rel = name.strip_prefix(&orig_prefix).unwrap_or(&name).to_string();
                    if opts.ignored_paths.iter().any(|p| p == &rel) {
                        continue;
                    }
                    futs.push(Box::pin(async move {
                        self.stream_download_to_file(&name, &child_dst).await
                    }));
                }
                if futs.len() >= concurrency {
                    if let Some(Err(e)) = futs.next().await {
                        return Err(e);
                    }
                }
            }
            while let Some(res) = futs.next().await {
                res?;
            }
            Ok(())
        })
    }

    async fn stream_download_to_file(&self, name: &str, dst: &Path) -> Result<(), BucketError> {
        use std::sync::atomic::{AtomicU64, Ordering};
        use tokio::io::AsyncWriteExt as _;
        static DL_TMP_SEQ: AtomicU64 = AtomicU64::new(0);

        let timer = self.timer(metrics::OP_GET);
        let parent = dst.parent().filter(|p| !p.as_os_str().is_empty()).unwrap_or_else(|| Path::new("."));
        let file_name = dst.file_name().and_then(|n| n.to_str()).unwrap_or("obj");
        let pid = std::process::id();
        let seq = DL_TMP_SEQ.fetch_add(1, Ordering::Relaxed);
        let tmp_path = parent.join(format!(".{file_name}.{pid}.{seq}.tmp"));

        let mut total_bytes = 0u64;
        let result: Result<(), BucketError> = async {
            if name.trim_start_matches('/').is_empty() {
                return Err(BucketError::InvalidPath(
                    name.to_string(),
                    "object name cannot be empty".to_string(),
                ));
            }
            let file = tokio::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&tmp_path)
                .await?;
            let mut writer = tokio::io::BufWriter::new(file);

            match &self.backend {
                Backend::S3(b) => {
                    let mut stream = b.get_stream(&self.full_name(name)).await.map_err(|e| {
                        if e.is_obj_not_found() {
                            BucketError::NotFound(name.to_string())
                        } else {
                            BucketError::S3(e)
                        }
                    })?;
                    while let Some(chunk) = stream.next().await {
                        let bytes = chunk.map_err(|e| {
                            if e.is_obj_not_found() {
                                BucketError::NotFound(name.to_string())
                            } else {
                                BucketError::S3(e)
                            }
                        })?;
                        total_bytes = total_bytes.saturating_add(bytes.len() as u64);
                        writer.write_all(&bytes).await?;
                    }
                }
                Backend::Store(store) => {
                    let path = self.object_path(name)?;
                    let res = match store.get(&path).await {
                        Ok(r) => r,
                        Err(object_store::Error::NotFound { .. }) => {
                            return Err(BucketError::NotFound(name.to_string()));
                        }
                        Err(e) => return Err(BucketError::Store(e)),
                    };
                    let mut stream = res.into_stream();
                    while let Some(chunk) = stream.next().await {
                        let bytes = chunk?;
                        total_bytes = total_bytes.saturating_add(bytes.len() as u64);
                        writer.write_all(&bytes).await?;
                    }
                }
                Backend::Filesystem(fs) => {
                    let path = self.object_path(name)?;
                    let Some(store) = fs.get_existing().await? else {
                        return Err(BucketError::NotFound(name.to_string()));
                    };
                    let res = match store.get(&path).await {
                        Ok(r) => r,
                        Err(object_store::Error::NotFound { .. }) => {
                            return Err(BucketError::NotFound(name.to_string()));
                        }
                        Err(e) => return Err(map_fs_store_error(e)),
                    };
                    let mut stream = res.into_stream();
                    while let Some(chunk) = stream.next().await {
                        let bytes = chunk.map_err(map_fs_store_error)?;
                        total_bytes = total_bytes.saturating_add(bytes.len() as u64);
                        writer.write_all(&bytes).await?;
                    }
                }
            }
            writer.flush().await?;
            drop(writer);
            tokio::fs::rename(&tmp_path, dst).await?;
            Ok(())
        }
        .await;

        if result.is_err() {
            let _ = tokio::fs::remove_file(&tmp_path).await;
        }
        if let Some(t) = timer {
            let expected =
                self.expected_not_found && result.as_ref().is_err_and(BucketError::is_not_found);
            t.finish_read(result.as_ref().map(|()| total_bytes).map_err(|_| ()), expected);
        }
        result
    }

    /// Ported from `github.com/thanos-io/objstore/objstore.go::UploadDir`.
    /// Uploads all files in `src_dir` recursively into `dst_dir`.
    pub async fn upload_dir(
        &self,
        src_dir: &Path,
        dst_dir: &str,
        opts: UploadDirOptions,
    ) -> Result<(), BucketError> {
        let meta = tokio::fs::metadata(src_dir).await?;
        if !meta.is_dir() {
            return Err(BucketError::InvalidPath(
                src_dir.to_string_lossy().into_owned(),
                "is not a directory".to_string(),
            ));
        }
        let mut files = Vec::new();
        let mut stack = vec![src_dir.to_path_buf()];
        while let Some(dir) = stack.pop() {
            let mut rd = tokio::fs::read_dir(&dir).await?;
            while let Some(entry) = rd.next_entry().await? {
                let path = entry.path();
                let ft = entry.file_type().await?;
                if ft.is_dir() {
                    stack.push(path);
                } else if ft.is_file() {
                    files.push(path);
                }
            }
        }
        let dst_clean = dst_dir.trim_matches('/');
        let concurrency = opts.concurrency.max(1);
        let mut futs = FuturesUnordered::new();
        for src_path in files {
            let rel = src_path
                .strip_prefix(src_dir)
                .map_err(|e| BucketError::Config(e.to_string()))?;
            let rel_slash = rel
                .components()
                .map(|c| c.as_os_str().to_string_lossy().into_owned())
                .collect::<Vec<_>>()
                .join("/");
            let dst_key = if dst_clean.is_empty() {
                rel_slash
            } else {
                format!("{dst_clean}/{rel_slash}")
            };
            futs.push(async move { self.upload_file(&dst_key, &src_path).await });
            if futs.len() >= concurrency {
                if let Some(Err(e)) = futs.next().await {
                    return Err(e);
                }
            }
        }
        while let Some(res) = futs.next().await {
            res?;
        }
        Ok(())
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

fn is_dir_empty(path: &Path) -> std::io::Result<bool> {
    match std::fs::read_dir(path) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Err(e) => Err(e),
        Ok(mut rd) => match rd.next() {
            None => Ok(true),
            Some(Ok(_)) => Ok(false),
            Some(Err(e)) => Err(e),
        },
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

fn map_fs_store_error(e: object_store::Error) -> BucketError {
    match e {
        object_store::Error::PermissionDenied { source, .. } => {
            BucketError::Io(std::io::Error::new(std::io::ErrorKind::PermissionDenied, source))
        }
        other => BucketError::Store(other),
    }
}

async fn fs_read_op(
    store: &Arc<dyn ObjectStore>,
    path: &ObjectPath,
    name: &str,
    range: Option<(u64, Option<u64>)>,
) -> Result<Bytes, BucketError> {
    match store_read_op(store, path, name, range).await {
        Ok(b) => Ok(b),
        Err(BucketError::Store(e @ object_store::Error::Generic { .. })) => {
            if let Some((off, len_opt)) = range {
                if len_opt != Some(0) {
                    match store.head(path).await {
                        Ok(meta) if off >= meta.size as u64 => return Ok(Bytes::new()),
                        Err(object_store::Error::NotFound { .. }) => {
                            return Err(BucketError::NotFound(name.to_string()));
                        }
                        _ => {}
                    }
                }
            }
            Err(map_fs_store_error(e))
        }
        Err(BucketError::Store(e)) => Err(map_fs_store_error(e)),
        Err(e) => Err(e),
    }
}

async fn store_read_op(
    store: &Arc<dyn ObjectStore>,
    path: &ObjectPath,
    name: &str,
    range: Option<(u64, Option<u64>)>,
) -> Result<Bytes, BucketError> {
    match range {
        None => match store.get(path).await {
            Ok(res) => Ok(res.bytes().await?),
            Err(object_store::Error::NotFound { .. }) => {
                Err(BucketError::NotFound(name.to_string()))
            }
            Err(e) => Err(BucketError::Store(e)),
        },
        Some((_, Some(0))) => match store.head(path).await {
            Ok(_) => Ok(Bytes::new()),
            Err(object_store::Error::NotFound { .. }) => {
                Err(BucketError::NotFound(name.to_string()))
            }
            Err(e) => Err(BucketError::Store(e)),
        },
        Some((off, Some(len))) => {
            let start = usize::try_from(off).map_err(|_| {
                BucketError::InvalidPath(name.to_string(), "range offset overflow".into())
            })?;
            let end = off
                .checked_add(len)
                .and_then(|e| usize::try_from(e).ok())
                .ok_or_else(|| {
                    BucketError::InvalidPath(name.to_string(), "range end overflow".into())
                })?;
            let opts = GetOptions {
                range: Some(GetRange::Bounded(start..end)),
                ..Default::default()
            };
            match store.get_opts(path, opts).await {
                Ok(res) => Ok(res.bytes().await?),
                Err(object_store::Error::NotFound { .. }) => {
                    Err(BucketError::NotFound(name.to_string()))
                }
                Err(e) => Err(BucketError::Store(e)),
            }
        }
        Some((off, None)) => {
            let opts = GetOptions {
                range: Some(GetRange::Offset(off as usize)),
                ..Default::default()
            };
            match store.get_opts(path, opts).await {
                Ok(res) => Ok(res.bytes().await?),
                Err(object_store::Error::NotFound { .. }) => {
                    Err(BucketError::NotFound(name.to_string()))
                }
                Err(e) => Err(BucketError::Store(e)),
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

    #[tokio::test]
    async fn w24_b07_invalid_tenant_sse_does_not_increment_bucket_metrics() {
        struct BadSseProvider;
        impl TenantConfigProvider for BadSseProvider {
            fn s3_sse_type(&self, _user_id: &str) -> String {
                "INVALID-SSE-TYPE".to_string()
            }
            fn s3_sse_kms_key_id(&self, _user_id: &str) -> String {
                String::new()
            }
            fn s3_sse_kms_encryption_context(&self, _user_id: &str) -> String {
                String::new()
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let reg = prometheus::Registry::new();
        let mut cfg = BucketConfig::default();
        cfg.backend_cfg.backend = "filesystem".to_string();
        cfg.backend_cfg.filesystem.dir = dir.path().to_string_lossy().into_owned();
        let bkt = BucketClient::new_client(&cfg, "blocks", Some(&reg))
            .unwrap()
            .for_user_with_config("tenant-bad", Arc::new(BadSseProvider));

        let err = bkt
            .upload("01ABCDEF/meta.json", Bytes::from_static(b"{}"))
            .await
            .unwrap_err();
        assert!(matches!(err, BucketError::TenantSse { .. }), "got {err:?}");

        let mfs = reg.gather();
        let sum_counters = |name: &str| -> f64 {
            mfs.iter()
                .filter(|mf| mf.get_name() == name)
                .flat_map(|mf| mf.get_metric())
                .map(|m| m.get_counter().get_value())
                .sum()
        };
        assert_eq!(sum_counters("thanos_objstore_bucket_operations_total"), 0.0);
        assert_eq!(
            sum_counters("thanos_objstore_bucket_operation_failures_total"),
            0.0
        );
    }

    #[tokio::test]
    async fn d2_03_delete_provider_semantics_match_thanos() {
        // Filesystem: delete() succeeds on missing objects without incrementing ops_failures.
        let dir = tempfile::tempdir().unwrap();
        let reg_fs = prometheus::Registry::new();
        let mut cfg_fs = BucketConfig::default();
        cfg_fs.backend_cfg.backend = "filesystem".to_string();
        cfg_fs.backend_cfg.filesystem.dir = dir.path().to_string_lossy().into_owned();
        let fs_bkt = BucketClient::new_client(&cfg_fs, "ingester", Some(&reg_fs)).unwrap();

        fs_bkt.delete("missing.bin").await.unwrap();
        let fs_failures: f64 = reg_fs
            .gather()
            .iter()
            .filter(|mf| mf.get_name() == "thanos_objstore_bucket_operation_failures_total")
            .flat_map(|mf| mf.get_metric())
            .map(|m| m.get_counter().get_value())
            .sum();
        assert_eq!(fs_failures, 0.0);

        // Backend::Store (GCS / Azure): delete() returns NotFound and increments ops_failures
        // unless with_expected_not_found() is used.
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let srv = tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(async move {
                    loop {
                        let mut buf = Vec::new();
                        let mut tmp = [0u8; 4096];
                        let hdr_end = loop {
                            let Ok(n) = sock.read(&mut tmp).await else {
                                return;
                            };
                            if n == 0 {
                                return;
                            }
                            buf.extend_from_slice(&tmp[..n]);
                            if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                                break pos;
                            }
                        };
                        let head = String::from_utf8_lossy(&buf[..hdr_end]);
                        let resp = if head.contains("restype=container") {
                            "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"
                        } else {
                            "HTTP/1.1 404 Not Found\r\nx-ms-error-code: BlobNotFound\r\nContent-Length: 0\r\n\r\n"
                        };
                        if sock.write_all(resp.as_bytes()).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });

        let gcs_sa = format!(
            r#"{{"type":"service_account","private_key":"unused","private_key_id":"x","client_email":"x@y","gcs_base_url":"http://{addr}","disable_oauth":true}}"#
        );
        let mut gcs_cfg = BucketConfig::default();
        gcs_cfg.backend_cfg.backend = "gcs".to_string();
        gcs_cfg.backend_cfg.gcs.bucket_name = "bkt".to_string();
        gcs_cfg.backend_cfg.gcs.service_account = gcs_sa;

        let mut az_cfg = BucketConfig::default();
        az_cfg.backend_cfg.backend = "azure".to_string();
        az_cfg.backend_cfg.azure.account_name = "devstoreaccount1".to_string();
        az_cfg.backend_cfg.azure.container_name = "blocks".to_string();
        az_cfg.backend_cfg.azure.connection_string = format!(
            "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://{addr}/devstoreaccount1;"
        );

        for cfg in [gcs_cfg, az_cfg] {
            let reg_store = prometheus::Registry::new();
            let reg_c = reg_store.clone();
            let bkt = tokio::task::spawn_blocking(move || {
                BucketClient::new_client(&cfg, "ingester", Some(&reg_c))
            })
            .await
            .unwrap()
            .unwrap();

            let err = bkt.delete("missing.bin").await.unwrap_err();
            assert!(err.is_not_found(), "expected NotFound, got {err:?}");
            let delete_failures = || -> f64 {
                reg_store
                    .gather()
                    .iter()
                    .filter(|mf| mf.get_name() == "thanos_objstore_bucket_operation_failures_total")
                    .flat_map(|mf| mf.get_metric())
                    .filter(|m| {
                        m.get_label().iter().any(|lp| {
                            lp.get_name() == "operation" && lp.get_value() == metrics::OP_DELETE
                        })
                    })
                    .map(|m| m.get_counter().get_value())
                    .sum()
            };
            assert_eq!(delete_failures(), 1.0);

            let err2 = bkt
                .with_expected_not_found()
                .delete("missing.bin")
                .await
                .unwrap_err();
            assert!(err2.is_not_found());
            assert_eq!(delete_failures(), 1.0);
        }
        srv.abort();
    }

    #[test]
    fn d2_07_bucket_error_is_access_denied() {
        assert!(BucketError::Store(object_store::Error::PermissionDenied {
            path: "a".into(),
            source: "forbidden".into(),
        })
        .is_access_denied());
        assert!(!BucketError::NotFound("a".into()).is_access_denied());
        assert!(!BucketError::Io(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "eacces"
        ))
        .is_access_denied());
    }

    #[tokio::test]
    async fn d2_10_filesystem_get_range_at_or_past_eof_returns_empty() {
        let dir = tempfile::tempdir().unwrap();
        let bkt = BucketClient::new_filesystem(dir.path()).unwrap();

        bkt.upload("empty.bin", Bytes::new()).await.unwrap();
        bkt.upload("hello.bin", Bytes::from_static(b"hello"))
            .await
            .unwrap();

        assert_eq!(bkt.get_range("empty.bin", 0, 10).await.unwrap(), Bytes::new());
        assert_eq!(bkt.get_range("empty.bin", 5, 10).await.unwrap(), Bytes::new());
        assert_eq!(bkt.get_range("hello.bin", 5, 10).await.unwrap(), Bytes::new());
        assert_eq!(bkt.get_range("hello.bin", 100, 10).await.unwrap(), Bytes::new());
        assert_eq!(
            bkt.get_range("hello.bin", 2, 10).await.unwrap(),
            Bytes::from_static(b"llo")
        );
        assert!(matches!(
            bkt.get_range("missing.bin", 0, 10).await,
            Err(BucketError::NotFound(_))
        ));
    }
}
