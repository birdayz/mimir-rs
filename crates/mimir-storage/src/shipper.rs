//! TSDB block shipper porting `pkg/ingester/shipper.go` and `pkg/storage/tsdb/block/{block,meta}.go`.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/ingester/shipper.go
//! - github.com/grafana/mimir/pkg/storage/tsdb/block/block.go
//! - github.com/grafana/mimir/pkg/storage/tsdb/block/meta.go

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use prometheus::{Counter, Gauge, Opts, Registry};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;
use tracing::{debug, error, info, warn};
use ulid::Ulid;

use crate::bucket::{BucketClient, BucketError};

pub const SHIPPER_META_FILENAME: &str = "mimir.shipper.json";
pub const SHIPPER_META_VERSION_1: i32 = 1;

pub const META_FILENAME: &str = "meta.json";
pub const INDEX_FILENAME: &str = "index";
pub const SPARSE_INDEX_HEADER_FILENAME: &str = "sparse-index-header";
pub const CHUNKS_DIRNAME: &str = "chunks";

pub const TSDB_VERSION_1: i32 = 1;
pub const THANOS_VERSION_1: i32 = 1;

pub const COMPACTION_HINT_FROM_OUT_OF_ORDER: &str = "from-out-of-order";
pub const OUT_OF_ORDER_EXTERNAL_LABEL: &str = "__out_of_order__";
pub const OUT_OF_ORDER_EXTERNAL_LABEL_VALUE: &str = "true";

#[derive(Debug, Error)]
pub enum ShipperError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("bucket error: {0}")]
    Bucket(#[from] BucketError),
    #[error("unexpected meta file version {0}")]
    UnexpectedMetaVersion(i32),
    #[error("unexpected shipper meta file version {0}")]
    UnexpectedShipperVersion(i32),
    #[error("not a valid block directory: {0}")]
    InvalidBlockDir(String),
    #[error("failed to upload {file_type}: {message}")]
    Upload { file_type: &'static str, message: String },
    #[error("failed to sync {failed} blocks (shipped {shipped})")]
    PartialSync { shipped: usize, failed: usize },
}

/// Ported from `block.SourceType` in `pkg/storage/tsdb/block/meta.go`.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SourceType {
    #[default]
    #[serde(rename = "")]
    Empty,
    Receive,
    Compactor,
    #[serde(rename = "compactor.repair")]
    CompactorRepair,
    #[serde(rename = "bucket.repair")]
    BucketRepair,
    BlockBuilder,
    SplitBlocks,
    Test,
}

impl SourceType {
    pub fn as_str(&self) -> &'static str {
        match self {
            SourceType::Empty => "",
            SourceType::Receive => "receive",
            SourceType::Compactor => "compactor",
            SourceType::CompactorRepair => "compactor.repair",
            SourceType::BucketRepair => "bucket.repair",
            SourceType::BlockBuilder => "block-builder",
            SourceType::SplitBlocks => "split-blocks",
            SourceType::Test => "test",
        }
    }
}

/// Prometheus `model.Time` (milliseconds since epoch, serialized as seconds float in JSON).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct ModelTime(pub i64);

impl ModelTime {
    pub fn now() -> Self {
        let ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        Self(ms)
    }

    pub fn from_millis(ms: i64) -> Self {
        Self(ms)
    }

    pub fn as_millis(self) -> i64 {
        self.0
    }
}

impl Serialize for ModelTime {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let secs = (self.0 as f64) / 1000.0;
        serializer.serialize_f64(secs)
    }
}

impl<'de> Deserialize<'de> for ModelTime {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let v = serde_json::Value::deserialize(deserializer)?;
        let s = match &v {
            serde_json::Value::Number(n) => n.to_string(),
            serde_json::Value::String(s) => s.clone(),
            _ => return Err(serde::de::Error::custom("expected number for model.Time")),
        };
        parse_model_time_str(&s).map(ModelTime).map_err(serde::de::Error::custom)
    }
}

fn parse_model_time_str(s: &str) -> Result<i64, String> {
    if let Some((base, frac)) = s.split_once('.') {
        let v: i64 = base.parse().map_err(|e| format!("bad time base: {e}"))?;
        let mut frac_ms: i64 = if frac.len() > 3 {
            frac[..3].parse().map_err(|e| format!("bad time frac: {e}"))?
        } else {
            frac.parse().map_err(|e| format!("bad time frac: {e}"))?
        };
        match frac.len() {
            1 => frac_ms *= 100,
            2 => frac_ms *= 10,
            _ => {}
        }
        if base.starts_with('-') {
            frac_ms = -frac_ms;
        }
        Ok(v * 1000 + frac_ms)
    } else {
        let v: i64 = s.parse().map_err(|e| format!("bad time: {e}"))?;
        Ok(v * 1000)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct BlockStats {
    #[serde(rename = "numSamples", default, skip_serializing_if = "is_zero_u64")]
    pub num_samples: u64,
    #[serde(rename = "numFloatSamples", default, skip_serializing_if = "is_zero_u64")]
    pub num_float_samples: u64,
    #[serde(rename = "numHistogramSamples", default, skip_serializing_if = "is_zero_u64")]
    pub num_histogram_samples: u64,
    #[serde(rename = "numSeries", default, skip_serializing_if = "is_zero_u64")]
    pub num_series: u64,
    #[serde(rename = "numChunks", default, skip_serializing_if = "is_zero_u64")]
    pub num_chunks: u64,
    #[serde(rename = "numTombstones", default, skip_serializing_if = "is_zero_u64")]
    pub num_tombstones: u64,
}

mod ulid_serde {
    use serde::{Deserialize, Deserializer, Serializer};
    use ulid::Ulid;

    pub fn serialize<S: Serializer>(u: &Ulid, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&u.to_string())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Ulid, D::Error> {
        let str_val = String::deserialize(d)?;
        Ulid::from_string(&str_val).map_err(serde::de::Error::custom)
    }
}

fn deserialize_null_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Default + Deserialize<'de>,
{
    Ok(Option::<T>::deserialize(deserializer)?.unwrap_or_default())
}

mod ulid_vec_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use ulid::Ulid;

    pub fn serialize<S: Serializer>(v: &[Ulid], s: S) -> Result<S::Ok, S::Error> {
        let strs: Vec<String> = v.iter().map(ToString::to_string).collect();
        strs.serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<Ulid>, D::Error> {
        let strs = Option::<Vec<String>>::deserialize(d)?.unwrap_or_default();
        strs.into_iter()
            .map(|x| Ulid::from_string(&x).map_err(serde::de::Error::custom))
            .collect()
    }
}

mod ulid_map_serde {
    use std::collections::BTreeMap;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use ulid::Ulid;
    use super::ModelTime;

    pub fn serialize<S: Serializer>(
        m: &BTreeMap<Ulid, ModelTime>,
        s: S,
    ) -> Result<S::Ok, S::Error> {
        let str_map: BTreeMap<String, ModelTime> =
            m.iter().map(|(k, v)| (k.to_string(), *v)).collect();
        str_map.serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        d: D,
    ) -> Result<BTreeMap<Ulid, ModelTime>, D::Error> {
        let str_map = Option::<BTreeMap<String, ModelTime>>::deserialize(d)?.unwrap_or_default();
        str_map
            .into_iter()
            .map(|(k, v)| {
                Ulid::from_string(&k)
                    .map(|u| (u, v))
                    .map_err(serde::de::Error::custom)
            })
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockDesc {
    #[serde(with = "ulid_serde")]
    pub ulid: Ulid,
    #[serde(rename = "minTime")]
    pub min_time: i64,
    #[serde(rename = "maxTime")]
    pub max_time: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct BlockMetaCompaction {
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub level: i32,
    #[serde(default, skip_serializing_if = "Vec::is_empty", with = "ulid_vec_serde")]
    pub sources: Vec<Ulid>,
    #[serde(default, deserialize_with = "deserialize_null_default", skip_serializing_if = "is_false")]
    pub deletable: bool,
    #[serde(default, deserialize_with = "deserialize_null_default", skip_serializing_if = "Vec::is_empty")]
    pub parents: Vec<BlockDesc>,
    #[serde(default, deserialize_with = "deserialize_null_default", skip_serializing_if = "is_false")]
    pub failed: bool,
    #[serde(default, deserialize_with = "deserialize_null_default", skip_serializing_if = "Vec::is_empty")]
    pub hints: Vec<String>,
}

impl BlockMetaCompaction {
    pub fn set_out_of_order(&mut self) {
        if !self.from_out_of_order() {
            self.hints.push(COMPACTION_HINT_FROM_OUT_OF_ORDER.to_string());
            self.hints.sort();
        }
    }

    pub fn from_out_of_order(&self) -> bool {
        self.hints.iter().any(|h| h == COMPACTION_HINT_FROM_OUT_OF_ORDER)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ThanosDownsample {
    pub resolution: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockFile {
    pub rel_path: String,
    #[serde(default, skip_serializing_if = "is_zero_i64")]
    pub size_bytes: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ThanosMeta {
    #[serde(default, deserialize_with = "deserialize_null_default", skip_serializing_if = "is_zero_i32")]
    pub version: i32,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub labels: BTreeMap<String, String>,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub downsample: ThanosDownsample,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub source: SourceType,
    #[serde(default, deserialize_with = "deserialize_null_default", skip_serializing_if = "Vec::is_empty")]
    pub segment_files: Vec<String>,
    #[serde(default, deserialize_with = "deserialize_null_default", skip_serializing_if = "Vec::is_empty")]
    pub files: Vec<BlockFile>,
}

/// Ported from `block.Meta` in `pkg/storage/tsdb/block/meta.go`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockMeta {
    #[serde(with = "ulid_serde")]
    pub ulid: Ulid,
    #[serde(rename = "minTime")]
    pub min_time: i64,
    #[serde(rename = "maxTime")]
    pub max_time: i64,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub stats: BlockStats,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub compaction: BlockMetaCompaction,
    pub version: i32,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub out_of_order: bool,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub thanos: ThanosMeta,
}

impl BlockMeta {
    pub fn read_from_dir(dir: impl AsRef<Path>) -> Result<Self, ShipperError> {
        let bytes = fs::read(dir.as_ref().join(META_FILENAME))?;
        let meta: Self = serde_json::from_slice(&bytes)?;
        if meta.version != TSDB_VERSION_1 {
            return Err(ShipperError::UnexpectedMetaVersion(meta.version));
        }
        let thanos_ver = if meta.thanos.version == 0 {
            THANOS_VERSION_1
        } else {
            meta.thanos.version
        };
        if thanos_ver != THANOS_VERSION_1 {
            return Err(ShipperError::UnexpectedMetaVersion(thanos_ver));
        }
        Ok(meta)
    }

    pub fn write_to_dir(&self, dir: impl AsRef<Path>) -> Result<(), ShipperError> {
        let path = dir.as_ref().join(META_FILENAME);
        let mut encoded = serde_json::to_vec_pretty(self)?;
        encoded.push(b'\n');
        atomic_write_file(&path, &encoded)?;
        Ok(())
    }

    pub fn encode_json_bytes(&self) -> Result<Vec<u8>, ShipperError> {
        let mut encoded = serde_json::to_vec_pretty(self)?;
        encoded.push(b'\n');
        Ok(encoded)
    }

    /// Returns true if the block was created from out-of-order samples
    /// (`Meta.IsOutOfOrder` in `pkg/storage/tsdb/block/meta.go`).
    pub fn is_out_of_order(&self) -> bool {
        if self.compaction.from_out_of_order() {
            return true;
        }
        self.thanos
            .labels
            .get(OUT_OF_ORDER_EXTERNAL_LABEL)
            .map(|v| v == OUT_OF_ORDER_EXTERNAL_LABEL_VALUE)
            .unwrap_or(false)
    }

    /// Calculates the total size in bytes of all files listed in `thanos.files`
    /// (`Meta.BlockBytes` in `pkg/storage/tsdb/block/meta.go`).
    pub fn block_bytes(&self) -> i64 {
        self.thanos
            .files
            .iter()
            .filter(|f| f.size_bytes > 0)
            .map(|f| f.size_bytes)
            .sum()
    }
}

/// Ported from `shipperMeta` in `pkg/ingester/shipper.go`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShipperMeta {
    pub version: i32,
    #[serde(default, with = "ulid_map_serde")]
    pub shipped: BTreeMap<Ulid, ModelTime>,
}

/// Metrics for the block shipper matching `pkg/ingester/shipper.go`.
#[derive(Clone)]
pub struct ShipperMetrics {
    pub uploads: Counter,
    pub upload_failures: Counter,
    pub last_successful_upload_time: Gauge,
}

impl ShipperMetrics {
    pub fn new(registry: Option<&Registry>) -> Self {
        let uploads = Counter::with_opts(Opts::new(
            "cortex_ingester_shipper_uploads_total",
            "Total number of uploaded TSDB blocks",
        ))
        .expect("valid metric");
        let upload_failures = Counter::with_opts(Opts::new(
            "cortex_ingester_shipper_upload_failures_total",
            "Total number of TSDB block upload failures",
        ))
        .expect("valid metric");
        let last_successful_upload_time = Gauge::with_opts(Opts::new(
            "cortex_ingester_shipper_last_successful_upload_timestamp_seconds",
            "Unix timestamp (in seconds) of the last successful TSDB block uploaded to the object storage.",
        ))
        .expect("valid metric");

        if let Some(reg) = registry {
            let _ = reg.register(Box::new(uploads.clone()));
            let _ = reg.register(Box::new(upload_failures.clone()));
            let _ = reg.register(Box::new(last_successful_upload_time.clone()));
        }

        Self {
            uploads,
            upload_failures,
            last_successful_upload_time,
        }
    }
}

/// Ported from `shipper` in `pkg/ingester/shipper.go`.
pub struct Shipper {
    pub user_id: String,
    pub dir: PathBuf,
    pub bucket: BucketClient,
    pub source: SourceType,
    pub out_of_order_external_label_enabled: bool,
    /// Go `cfgProvider.OutOfOrderBlocksExternalLabelEnabled(userID)`: consulted at every
    /// upload so runtime-config changes apply; falls back to the static flag when unset.
    pub out_of_order_external_label_provider: Option<Arc<dyn Fn() -> bool + Send + Sync>>,
    pub metrics: ShipperMetrics,
}

impl Shipper {
    /// Creates a new `Shipper`. Note: `bucket` should already be user-scoped if multi-tenant
    /// prefixing is desired (matching `newShipper` in `pkg/ingester/shipper.go`); see also
    /// [`Shipper::new_for_user`].
    pub fn new(
        user_id: impl Into<String>,
        dir: impl AsRef<Path>,
        bucket: BucketClient,
        source: SourceType,
        out_of_order_external_label_enabled: bool,
        metrics: ShipperMetrics,
    ) -> Self {
        Self {
            user_id: user_id.into(),
            dir: dir.as_ref().to_path_buf(),
            bucket,
            source,
            out_of_order_external_label_enabled,
            out_of_order_external_label_provider: None,
            metrics,
        }
    }

    /// Reads the per-tenant `out_of_order_blocks_external_label_enabled` limit at each
    /// upload (runtime overrides), like Go's shipper `cfgProvider`.
    pub fn with_out_of_order_external_label_provider(
        mut self,
        provider: Arc<dyn Fn() -> bool + Send + Sync>,
    ) -> Self {
        self.out_of_order_external_label_provider = Some(provider);
        self
    }

    /// Creates a `Shipper` whose bucket client is scoped to `<user_id>/...`.
    pub fn new_for_user(
        user_id: impl Into<String>,
        dir: impl AsRef<Path>,
        root_bucket: &BucketClient,
        source: SourceType,
        out_of_order_external_label_enabled: bool,
        metrics: ShipperMetrics,
    ) -> Self {
        let user_id = user_id.into();
        let scoped = root_bucket.for_user(&user_id);
        Self::new(
            user_id,
            dir,
            scoped,
            source,
            out_of_order_external_label_enabled,
            metrics,
        )
    }

    /// Like [`Shipper::new_for_user`], with the bucket built by Mimir
    /// `bucket.NewUserBucketClient(userID, bucket, cfgProvider)` (tenant prefix + per-tenant
    /// S3 SSE overrides).
    pub fn new_for_user_with_config(
        user_id: impl Into<String>,
        dir: impl AsRef<Path>,
        root_bucket: &BucketClient,
        cfg_provider: std::sync::Arc<dyn crate::bucket::TenantConfigProvider>,
        source: SourceType,
        out_of_order_external_label_enabled: bool,
        metrics: ShipperMetrics,
    ) -> Self {
        let user_id = user_id.into();
        let scoped = root_bucket.for_user_with_config(&user_id, cfg_provider);
        Self::new(
            user_id,
            dir,
            scoped,
            source,
            out_of_order_external_label_enabled,
            metrics,
        )
    }

    /// Performs a single synchronization, uploading all unshipped level-1 non-empty local blocks
    /// to object storage (chunks/* + index first, meta.json LAST), and persisting `mimir.shipper.json`.
    pub async fn sync(&self) -> Result<usize, ShipperError> {
        let meta_exists = self.dir.join(SHIPPER_META_FILENAME).exists();
        let (shipped_blocks, meta_read_ok) = match read_shipped_blocks(&self.dir) {
            Ok(m) => (m, true),
            Err(err) => {
                warn!(err = %err, dir = %self.dir.display(), "reading shipper meta file failed, will override it");
                (HashMap::new(), false)
            }
        };

        let mut meta = ShipperMeta {
            version: SHIPPER_META_VERSION_1,
            shipped: BTreeMap::new(),
        };
        let mut shipped = 0usize;
        let mut upload_errs = 0usize;

        let metas = block_metas_from_oldest(&self.dir)?;
        for mut m in metas {
            if let Some(&shipped_time) = shipped_blocks.get(&m.ulid) {
                meta.shipped.insert(m.ulid, shipped_time);
                continue;
            }

            if m.stats.num_samples == 0 {
                debug!(block = %m.ulid, "ignoring empty block");
                continue;
            }

            if m.compaction.level > 1 {
                continue;
            }

            let meta_key = format!("{}/{}", m.ulid, META_FILENAME);
            if self.bucket.exists(&meta_key).await? {
                meta.shipped.insert(m.ulid, ModelTime::now());
                shipped += 1;
                continue;
            }

            info!(block = %m.ulid, "uploading new block to long-term storage");
            if let Err(err) = self.upload_block(&mut m).await {
                error!(block = %m.ulid, err = %err, "uploading new block to long-term storage failed");
                upload_errs += 1;
                continue;
            }
            info!(block = %m.ulid, "finished uploading new block to long-term storage");

            meta.shipped.insert(m.ulid, ModelTime::now());
            shipped += 1;
            self.metrics.uploads.inc();
            let now_secs = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs_f64();
            self.metrics.last_successful_upload_time.set(now_secs);
        }

        let meta_unchanged = meta_exists
            && meta_read_ok
            && shipped == 0
            && meta.shipped.len() == shipped_blocks.len()
            && meta
                .shipped
                .iter()
                .all(|(k, v)| shipped_blocks.get(k) == Some(v));
        if !meta_unchanged {
            if let Err(err) = write_shipper_meta_file(&self.dir, &meta) {
                warn!(err = %err, dir = %self.dir.display(), "updating shipper meta file failed");
            }
        }

        if upload_errs > 0 {
            self.metrics.upload_failures.inc_by(upload_errs as f64);
            return Err(ShipperError::PartialSync {
                shipped,
                failed: upload_errs,
            });
        }

        Ok(shipped)
    }

    async fn upload_block(&self, meta: &mut BlockMeta) -> Result<(), ShipperError> {
        let block_dir = self.dir.join(meta.ulid.to_string());

        meta.thanos.source = self.source.clone();
        meta.thanos.downsample.resolution = 0;
        meta.thanos.segment_files = get_segment_files(&block_dir);

        let ooo_label_enabled = match self.out_of_order_external_label_provider.as_ref() {
            Some(provider) => provider(),
            None => self.out_of_order_external_label_enabled,
        };
        if meta.compaction.from_out_of_order() && ooo_label_enabled {
            meta.thanos.labels.insert(
                OUT_OF_ORDER_EXTERNAL_LABEL.to_string(),
                OUT_OF_ORDER_EXTERNAL_LABEL_VALUE.to_string(),
            );
        }

        upload_block_to_bucket(&self.bucket, &block_dir, meta).await
    }
}

/// Uploads a TSDB block to `bucket` (`chunks/*`, `index`, optional `sparse-index-header` first,
/// then `meta.json` LAST for atomicity), matching `block.Upload` in `pkg/storage/tsdb/block/block.go`.
pub async fn upload_block_to_bucket(
    bucket: &BucketClient,
    block_dir: &Path,
    meta: &mut BlockMeta,
) -> Result<(), ShipperError> {
    let dir_meta = fs::metadata(block_dir)?;
    if !dir_meta.is_dir() {
        return Err(ShipperError::InvalidBlockDir(
            block_dir.display().to_string(),
        ));
    }

    let dir_name = block_dir
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or_else(|| ShipperError::InvalidBlockDir(block_dir.display().to_string()))?;
    let id = Ulid::from_string(dir_name)
        .map_err(|_| ShipperError::InvalidBlockDir(block_dir.display().to_string()))?;

    meta.thanos.files = gather_file_stats(block_dir)?;
    let meta_bytes = meta.encode_json_bytes()?;

    // 1. Upload chunks/* sequentially in sorted order to bound upload memory.
    let chunks_dir = block_dir.join(CHUNKS_DIRNAME);
    if chunks_dir.exists() {
        let mut chunk_entries = Vec::new();
        for entry in fs::read_dir(&chunks_dir)? {
            let entry = entry?;
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let fname = entry.file_name().to_string_lossy().into_owned();
            chunk_entries.push((fname, path));
        }
        chunk_entries.sort_by(|a, b| a.0.cmp(&b.0));

        for (fname, path) in chunk_entries {
            let obj_key = format!("{id}/{CHUNKS_DIRNAME}/{fname}");
            if let Err(e) = bucket.upload_file(&obj_key, &path).await {
                return Err(cleanup_failed_upload(bucket, id, "chunks", e).await);
            }
        }
    }

    // 2. Upload index
    {
        let index_path = block_dir.join(INDEX_FILENAME);
        let obj_key = format!("{id}/{INDEX_FILENAME}");
        if let Err(e) = bucket.upload_file(&obj_key, &index_path).await {
            return Err(cleanup_failed_upload(bucket, id, "index", e).await);
        }
    }

    // 3. Optional sparse-index-header
    let has_sparse = meta
        .thanos
        .files
        .iter()
        .any(|f| f.rel_path == SPARSE_INDEX_HEADER_FILENAME);
    if has_sparse {
        let sparse_path = block_dir.join(SPARSE_INDEX_HEADER_FILENAME);
        let obj_key = format!("{id}/{SPARSE_INDEX_HEADER_FILENAME}");
        if let Err(e) = bucket.upload_file(&obj_key, &sparse_path).await {
            return Err(cleanup_failed_upload(bucket, id, "sparse_index_header", e).await);
        }
    }

    // 4. Upload meta.json LAST for atomicity!
    // Do NOT clean up on error here (matching block.go).
    let meta_key = format!("{id}/{META_FILENAME}");
    bucket
        .upload(&meta_key, Bytes::from(meta_bytes))
        .await
        .map_err(|e| ShipperError::Upload {
            file_type: "meta",
            message: e.to_string(),
        })?;

    Ok(())
}

/// Lists valid ULID block directories in `dir` and returns their `BlockMeta` sorted by `min_time` ascending.
pub fn block_metas_from_oldest(dir: &Path) -> Result<Vec<BlockMeta>, ShipperError> {
    let mut metas = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name_str) = name.to_str() else {
            continue;
        };
        if Ulid::from_string(name_str).is_err() {
            continue;
        }
        let block_dir = entry.path();
        if !block_dir.is_dir() {
            continue;
        }
        let m = BlockMeta::read_from_dir(&block_dir)?;
        metas.push(m);
    }
    metas.sort_by_key(|m| m.min_time);
    Ok(metas)
}

/// Returns sorted filenames inside `<block_dir>/chunks`, or empty vec on error.
pub fn get_segment_files(block_dir: &Path) -> Vec<String> {
    let Ok(entries) = fs::read_dir(block_dir.join(CHUNKS_DIRNAME)) else {
        return Vec::new();
    };
    let mut files = Vec::new();
    for entry in entries.flatten() {
        if let Some(name) = entry.file_name().to_str() {
            files.push(name.to_string());
        }
    }
    files.sort();
    files
}

/// Gathers `BlockFile` stats for `chunks/*`, `index`, optional `sparse-index-header`, and `meta.json`.
pub fn gather_file_stats(block_dir: &Path) -> Result<Vec<BlockFile>, ShipperError> {
    let mut res = Vec::new();
    let chunks_dir = block_dir.join(CHUNKS_DIRNAME);
    for entry in fs::read_dir(&chunks_dir)? {
        let entry = entry?;
        let meta = entry.metadata()?;
        if let Some(name) = entry.file_name().to_str() {
            res.push(BlockFile {
                rel_path: format!("{CHUNKS_DIRNAME}/{name}"),
                size_bytes: meta.len() as i64,
            });
        }
    }

    let index_meta = fs::metadata(block_dir.join(INDEX_FILENAME))?;
    res.push(BlockFile {
        rel_path: INDEX_FILENAME.to_string(),
        size_bytes: index_meta.len() as i64,
    });

    let sparse_path = block_dir.join(SPARSE_INDEX_HEADER_FILENAME);
    if let Ok(sparse_meta) = fs::metadata(&sparse_path) {
        res.push(BlockFile {
            rel_path: SPARSE_INDEX_HEADER_FILENAME.to_string(),
            size_bytes: sparse_meta.len() as i64,
        });
    }

    // meta.json entry omits size_bytes (matching Thanos GatherFileStats).
    let _ = fs::metadata(block_dir.join(META_FILENAME))?;
    res.push(BlockFile {
        rel_path: META_FILENAME.to_string(),
        size_bytes: 0,
    });

    res.sort_by(|a, b| a.rel_path.cmp(&b.rel_path));
    Ok(res)
}

pub fn read_shipped_blocks(dir: &Path) -> Result<HashMap<Ulid, ModelTime>, ShipperError> {
    let path = dir.join(SHIPPER_META_FILENAME);
    let data = match fs::read(&path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(HashMap::new()),
        Err(e) => return Err(ShipperError::Io(e)),
    };
    let meta: ShipperMeta = serde_json::from_slice(&data)?;
    if meta.version != SHIPPER_META_VERSION_1 {
        return Err(ShipperError::UnexpectedShipperVersion(meta.version));
    }
    Ok(meta.shipped.into_iter().collect())
}

pub fn write_shipper_meta_file(dir: &Path, meta: &ShipperMeta) -> Result<(), ShipperError> {
    let path = dir.join(SHIPPER_META_FILENAME);
    let mut bytes = serde_json::to_vec_pretty(meta)?;
    bytes.push(b'\n');
    atomic_write_file(&path, &bytes)?;
    Ok(())
}

pub const DELETION_MARK_FILENAME: &str = "deletion-mark.json";
pub const NO_COMPACT_MARK_FILENAME: &str = "no-compact-mark.json";
pub const DELETION_MARK_VERSION_1: i32 = 1;
pub const NO_COMPACT_MARK_VERSION_1: i32 = 1;

/// Ported from `block.DeletionMark` in `pkg/storage/tsdb/block/markers.go`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeletionMark {
    #[serde(with = "ulid_serde")]
    pub id: Ulid,
    pub version: i32,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub details: String,
    pub deletion_time: i64,
}

/// Ported from `block.NoCompactReason` in `pkg/storage/tsdb/block/markers.go`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum NoCompactReason {
    Manual,
    #[serde(rename = "block-index-out-of-order-chunk")]
    OutOfOrderChunks,
    Critical,
    PostingsOffsetTableTooLarge,
    #[serde(rename = "index-exceeds-64gib")]
    IndexExceeds64GiB,
    SymbolTableTooLarge,
    Preemptive,
}

/// Ported from `block.NoCompactMark` in `pkg/storage/tsdb/block/markers.go`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NoCompactMark {
    #[serde(with = "ulid_serde")]
    pub id: Ulid,
    pub version: i32,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub details: String,
    pub no_compact_time: i64,
    pub reason: NoCompactReason,
}

/// Ported from `block.IsBlockDir` in `pkg/storage/tsdb/block/block.go`.
pub fn is_block_dir(path: &str) -> (Ulid, bool) {
    let Some(base) = Path::new(path).file_name().and_then(|s| s.to_str()) else {
        return (Ulid::nil(), false);
    };
    match Ulid::from_string(base) {
        Ok(id) => (id, true),
        Err(_) => (Ulid::nil(), false),
    }
}

async fn cleanup_failed_upload(
    bucket: &BucketClient,
    id: Ulid,
    file_type: &'static str,
    orig_err: impl std::fmt::Display,
) -> ShipperError {
    match delete_block(bucket, id).await {
        Ok(()) => ShipperError::Upload {
            file_type,
            message: orig_err.to_string(),
        },
        Err(clean_err) => ShipperError::Upload {
            file_type,
            message: format!(
                "failed to clean block after upload issue. Partial block in system. Err: {clean_err}: {orig_err}"
            ),
        },
    }
}

/// Ported from `block.Upload` in `pkg/storage/tsdb/block/block.go`.
/// If `meta` is `None`, reads `meta.json` from `block_dir`.
/// Local `meta.json` must still exist on disk even when `meta` is supplied.
pub async fn upload_block(
    bucket: &BucketClient,
    block_dir: &Path,
    meta: Option<BlockMeta>,
) -> Result<BlockMeta, ShipperError> {
    let dir_meta = fs::metadata(block_dir)?;
    if !dir_meta.is_dir() {
        return Err(ShipperError::InvalidBlockDir(
            block_dir.display().to_string(),
        ));
    }
    let dir_name = block_dir
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or_else(|| ShipperError::InvalidBlockDir(block_dir.display().to_string()))?;
    let _id = Ulid::from_string(dir_name)
        .map_err(|_| ShipperError::InvalidBlockDir(block_dir.display().to_string()))?;

    let mut meta = match meta {
        Some(m) => m,
        None => BlockMeta::read_from_dir(block_dir)?,
    };
    upload_block_to_bucket(bucket, block_dir, &mut meta).await?;
    Ok(meta)
}

/// Ported from `block.DownloadMeta` in `pkg/storage/tsdb/block/block.go`.
pub async fn download_meta(bucket: &BucketClient, id: Ulid) -> Result<BlockMeta, ShipperError> {
    let key = format!("{id}/{META_FILENAME}");
    let bytes = bucket.download(&key).await?;
    let meta: BlockMeta = serde_json::from_slice(&bytes)?;
    Ok(meta)
}

/// Ported from `block.Delete` in `pkg/storage/tsdb/block/block.go`.
/// Deletes `meta.json` first, then all other block files, and `deletion-mark.json` last.
pub async fn delete_block(bucket: &BucketClient, id: Ulid) -> Result<(), BucketError> {
    let meta_file = format!("{id}/{META_FILENAME}");
    let deletion_mark_file = format!("{id}/{DELETION_MARK_FILENAME}");

    if bucket.exists(&meta_file).await? {
        bucket.delete(&meta_file).await?;
    }

    let keys = bucket.list_all(&id.to_string()).await?;
    for key in keys {
        if key == meta_file || key == deletion_mark_file {
            continue;
        }
        bucket.delete(&key).await?;
    }

    if bucket.exists(&deletion_mark_file).await? {
        bucket.delete(&deletion_mark_file).await?;
    }

    Ok(())
}

/// Ported from `block.MarkForDeletion` in `pkg/storage/tsdb/block/block.go`.
pub async fn mark_for_deletion(
    bucket: &BucketClient,
    id: Ulid,
    details: &str,
    marked_for_deletion: Option<&Counter>,
) -> Result<(), ShipperError> {
    let marker_file = format!("{id}/{DELETION_MARK_FILENAME}");
    if bucket.exists(&marker_file).await? {
        warn!(block = %id, file = %marker_file, "requested to mark for deletion, but file already exists");
        return Ok(());
    }
    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    let mark = DeletionMark {
        id,
        version: DELETION_MARK_VERSION_1,
        details: details.to_string(),
        deletion_time: now_secs,
    };
    let encoded = serde_json::to_vec(&mark)?;
    bucket.upload(&marker_file, Bytes::from(encoded)).await?;
    if let Some(c) = marked_for_deletion {
        c.inc();
    }
    Ok(())
}

/// Ported from `block.MarkForNoCompact` in `pkg/storage/tsdb/block/block.go`.
pub async fn mark_for_no_compact(
    bucket: &BucketClient,
    id: Ulid,
    reason: NoCompactReason,
    details: &str,
    marked_for_no_compact: &[&Counter],
) -> Result<(), ShipperError> {
    let marker_file = format!("{id}/{NO_COMPACT_MARK_FILENAME}");
    if bucket.exists(&marker_file).await? {
        warn!(block = %id, file = %marker_file, "requested to mark for no compaction, but file already exists");
        return Ok(());
    }
    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    let mark = NoCompactMark {
        id,
        version: NO_COMPACT_MARK_VERSION_1,
        details: details.to_string(),
        no_compact_time: now_secs,
        reason,
    };
    let encoded = serde_json::to_vec(&mark)?;
    bucket.upload(&marker_file, Bytes::from(encoded)).await?;
    for c in marked_for_no_compact {
        c.inc();
    }
    Ok(())
}

/// Ported from `block.DeleteNoCompactMarker` in `pkg/storage/tsdb/block/block.go`.
pub async fn delete_no_compact_marker(
    bucket: &BucketClient,
    id: Ulid,
) -> Result<(), BucketError> {
    let marker_file = format!("{id}/{NO_COMPACT_MARK_FILENAME}");
    if !bucket.exists(&marker_file).await? {
        return Err(BucketError::NotFound(marker_file));
    }
    bucket.delete_strict(&marker_file).await
}

pub(crate) fn atomic_write_file(target: &Path, data: &[u8]) -> std::io::Result<()> {
    let parent = match target.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    };
    fs::create_dir_all(parent)?;
    let tmp = target.with_extension(format!(
        "{}.tmp",
        target
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("file")
    ));
    let write_res = (|| -> std::io::Result<()> {
        let mut f = fs::File::create(&tmp)?;
        f.write_all(data)?;
        f.sync_all()?;
        drop(f);
        fs::rename(&tmp, target)?;
        Ok(())
    })();
    if let Err(e) = write_res {
        let _ = fs::remove_file(&tmp);
        return Err(e);
    }
    #[cfg(unix)]
    {
        fs::File::open(parent)?.sync_all()?;
    }
    Ok(())
}

fn is_zero_u64(v: &u64) -> bool {
    *v == 0
}

fn is_zero_i64(v: &i64) -> bool {
    *v == 0
}

fn is_zero_i32(v: &i32) -> bool {
    *v == 0
}

fn is_false(v: &bool) -> bool {
    !*v
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    fn create_test_block(blocks_dir: &Path, id: Ulid, meta: &BlockMeta) {
        let block_dir = blocks_dir.join(id.to_string());
        fs::create_dir_all(block_dir.join(CHUNKS_DIRNAME)).unwrap();
        meta.write_to_dir(&block_dir).unwrap();
        fs::write(block_dir.join(INDEX_FILENAME), b"index-bytes").unwrap();
        fs::write(block_dir.join(CHUNKS_DIRNAME).join("000001"), b"chunk-bytes").unwrap();
    }

    fn make_meta(id: Ulid, min_time: i64, max_time: i64, num_samples: u64) -> BlockMeta {
        BlockMeta {
            ulid: id,
            min_time,
            max_time,
            stats: BlockStats {
                num_samples,
                ..Default::default()
            },
            compaction: BlockMetaCompaction {
                level: 1,
                ..Default::default()
            },
            version: TSDB_VERSION_1,
            out_of_order: false,
            thanos: ThanosMeta::default(),
        }
    }

    #[tokio::test]
    async fn test_shipper_sync_and_idempotency() {
        let blocks_dir = tempfile::tempdir().unwrap();
        let bkt_dir = tempfile::tempdir().unwrap();
        let bkt = BucketClient::new_filesystem(bkt_dir.path()).unwrap();

        let shipper = Shipper::new(
            "tenant-a",
            blocks_dir.path(),
            bkt.for_user("tenant-a"),
            SourceType::Receive,
            true,
            ShipperMetrics::new(None),
        );

        assert!(read_shipped_blocks(blocks_dir.path()).unwrap().is_empty());

        let id1 = Ulid::from_parts(1000, 1);
        let mut m1 = make_meta(id1, 1000, 2000, 100);
        m1.thanos.labels.insert("a".to_string(), "b".to_string());
        create_test_block(blocks_dir.path(), id1, &m1);

        let uploaded = shipper.sync().await.unwrap();
        assert_eq!(uploaded, 1);

        // Second sync uploads 0 blocks.
        let uploaded2 = shipper.sync().await.unwrap();
        assert_eq!(uploaded2, 0);

        // Empty block (0 samples) is skipped.
        let id2 = Ulid::from_parts(2000, 2);
        let m2 = make_meta(id2, 2000, 3000, 0);
        create_test_block(blocks_dir.path(), id2, &m2);
        assert_eq!(shipper.sync().await.unwrap(), 0);

        // Level > 1 block is skipped.
        let id3 = Ulid::from_parts(3000, 3);
        let mut m3 = make_meta(id3, 3000, 4000, 50);
        m3.compaction.level = 2;
        create_test_block(blocks_dir.path(), id3, &m3);
        assert_eq!(shipper.sync().await.unwrap(), 0);

        // OOO block gets __out_of_order__ label injected in remote meta.json without mutating local meta.json.
        let id4 = Ulid::from_parts(4000, 4);
        let mut m4 = make_meta(id4, 4000, 5000, 42);
        m4.compaction.set_out_of_order();
        create_test_block(blocks_dir.path(), id4, &m4);
        assert_eq!(shipper.sync().await.unwrap(), 1);

        let remote_bytes = bkt
            .download(&format!("tenant-a/{id4}/{META_FILENAME}"))
            .await
            .unwrap();
        let remote_meta: BlockMeta = serde_json::from_slice(&remote_bytes).unwrap();
        assert_eq!(remote_meta.thanos.source, SourceType::Receive);
        assert_eq!(remote_meta.thanos.downsample.resolution, 0);
        assert_eq!(
            remote_meta.thanos.labels.get(OUT_OF_ORDER_EXTERNAL_LABEL),
            Some(&OUT_OF_ORDER_EXTERNAL_LABEL_VALUE.to_string())
        );
        assert_eq!(remote_meta.thanos.segment_files, vec!["000001".to_string()]);
        assert!(!remote_meta.thanos.files.is_empty());

        // Local meta.json was NOT mutated.
        let local_meta = BlockMeta::read_from_dir(blocks_dir.path().join(id4.to_string())).unwrap();
        assert!(local_meta.thanos.labels.is_empty());
    }

    /// Go shipper reads `OutOfOrderBlocksExternalLabelEnabled(userID)` from the runtime
    /// config provider at every upload, so a runtime override change applies without
    /// reopening the TSDB.
    #[tokio::test]
    async fn test_shipper_ooo_external_label_follows_runtime_provider() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let blocks_dir = tempfile::tempdir().unwrap();
        let bkt_dir = tempfile::tempdir().unwrap();
        let bkt = BucketClient::new_filesystem(bkt_dir.path()).unwrap();
        let enabled = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&enabled);
        let shipper = Shipper::new(
            "tenant-a",
            blocks_dir.path(),
            bkt.for_user("tenant-a"),
            SourceType::Receive,
            true, // static flag at creation; the provider wins.
            ShipperMetrics::new(None),
        )
        .with_out_of_order_external_label_provider(Arc::new(move || flag.load(Ordering::SeqCst)));

        let remote_label = |id: Ulid| {
            let bkt = bkt.clone();
            async move {
                let raw = bkt
                    .download(&format!("tenant-a/{id}/{META_FILENAME}"))
                    .await
                    .unwrap();
                let m: BlockMeta = serde_json::from_slice(&raw).unwrap();
                m.thanos.labels.get(OUT_OF_ORDER_EXTERNAL_LABEL).cloned()
            }
        };

        let id1 = Ulid::from_parts(1000, 1);
        let mut m1 = make_meta(id1, 1000, 2000, 10);
        m1.compaction.set_out_of_order();
        create_test_block(blocks_dir.path(), id1, &m1);
        assert_eq!(shipper.sync().await.unwrap(), 1);
        assert_eq!(remote_label(id1).await, None);

        enabled.store(true, Ordering::SeqCst);
        let id2 = Ulid::from_parts(2000, 2);
        let mut m2 = make_meta(id2, 2000, 3000, 10);
        m2.compaction.set_out_of_order();
        create_test_block(blocks_dir.path(), id2, &m2);
        assert_eq!(shipper.sync().await.unwrap(), 1);
        assert_eq!(
            remote_label(id2).await,
            Some(OUT_OF_ORDER_EXTERNAL_LABEL_VALUE.to_string())
        );
    }

    #[tokio::test]
    async fn test_shipper_upload_order_and_recovery() {
        use object_store::{
            GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
            PutMultipartOpts, PutOptions, PutPayload, PutResult,
        };
        use futures::stream::BoxStream;
        use object_store::path::Path as ObjPath;
        use parking_lot::Mutex;

        #[derive(Debug)]
        struct RecordingStore {
            inner: object_store::memory::InMemory,
            put_order: Mutex<Vec<String>>,
            fail_meta_count: AtomicUsize,
        }

        impl std::fmt::Display for RecordingStore {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "RecordingStore")
            }
        }

        impl ObjectStore for RecordingStore {
            fn put_opts<'life0, 'life1, 'async_trait>(
                &'life0 self,
                location: &'life1 ObjPath,
                payload: PutPayload,
                opts: PutOptions,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = object_store::Result<PutResult>> + Send + 'async_trait>>
            where
                'life0: 'async_trait,
                'life1: 'async_trait,
                Self: 'async_trait,
            {
                let loc = location.to_string();
                let location = location.clone();
                Box::pin(async move {
                    self.put_order.lock().push(loc.clone());
                    let res = self.inner.put_opts(&location, payload, opts).await?;
                    if loc.ends_with("/meta.json")
                        && self.fail_meta_count.fetch_sub(1, Ordering::SeqCst) > 0
                    {
                        return Err(object_store::Error::Generic {
                            store: "RecordingStore",
                            source: "simulated deceiving error".into(),
                        });
                    }
                    Ok(res)
                })
            }
            fn put_multipart_opts<'life0, 'life1, 'async_trait>(
                &'life0 self,
                location: &'life1 ObjPath,
                opts: PutMultipartOpts,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = object_store::Result<Box<dyn MultipartUpload>>> + Send + 'async_trait>>
            where
                'life0: 'async_trait,
                'life1: 'async_trait,
                Self: 'async_trait,
            {
                let location = location.clone();
                Box::pin(async move { self.inner.put_multipart_opts(&location, opts).await })
            }
            fn get_opts<'life0, 'life1, 'async_trait>(
                &'life0 self,
                location: &'life1 ObjPath,
                options: GetOptions,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = object_store::Result<GetResult>> + Send + 'async_trait>>
            where
                'life0: 'async_trait,
                'life1: 'async_trait,
                Self: 'async_trait,
            {
                let location = location.clone();
                Box::pin(async move { self.inner.get_opts(&location, options).await })
            }
            fn delete<'life0, 'life1, 'async_trait>(
                &'life0 self,
                location: &'life1 ObjPath,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = object_store::Result<()>> + Send + 'async_trait>>
            where
                'life0: 'async_trait,
                'life1: 'async_trait,
                Self: 'async_trait,
            {
                let location = location.clone();
                Box::pin(async move { self.inner.delete(&location).await })
            }
            fn list(&self, prefix: Option<&ObjPath>) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
                self.inner.list(prefix)
            }
            fn list_with_delimiter<'life0, 'life1, 'async_trait>(
                &'life0 self,
                prefix: Option<&'life1 ObjPath>,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = object_store::Result<ListResult>> + Send + 'async_trait>>
            where
                'life0: 'async_trait,
                'life1: 'async_trait,
                Self: 'async_trait,
            {
                let prefix = prefix.cloned();
                Box::pin(async move { self.inner.list_with_delimiter(prefix.as_ref()).await })
            }
            fn copy<'life0, 'life1, 'life2, 'async_trait>(
                &'life0 self,
                from: &'life1 ObjPath,
                to: &'life2 ObjPath,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = object_store::Result<()>> + Send + 'async_trait>>
            where
                'life0: 'async_trait,
                'life1: 'async_trait,
                'life2: 'async_trait,
                Self: 'async_trait,
            {
                let from = from.clone();
                let to = to.clone();
                Box::pin(async move { self.inner.copy(&from, &to).await })
            }
            fn copy_if_not_exists<'life0, 'life1, 'life2, 'async_trait>(
                &'life0 self,
                from: &'life1 ObjPath,
                to: &'life2 ObjPath,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = object_store::Result<()>> + Send + 'async_trait>>
            where
                'life0: 'async_trait,
                'life1: 'async_trait,
                'life2: 'async_trait,
                Self: 'async_trait,
            {
                let from = from.clone();
                let to = to.clone();
                Box::pin(async move { self.inner.copy_if_not_exists(&from, &to).await })
            }
        }

        let store = Arc::new(RecordingStore {
            inner: object_store::memory::InMemory::new(),
            put_order: Mutex::new(Vec::new()),
            fail_meta_count: AtomicUsize::new(1),
        });
        let bkt = BucketClient::from_object_store(store.clone(), "rec", "REC");
        let blocks_dir = tempfile::tempdir().unwrap();
        let id = Ulid::from_parts(5000, 5);
        create_test_block(blocks_dir.path(), id, &make_meta(id, 5000, 6000, 10));

        let shipper = Shipper::new(
            "t1",
            blocks_dir.path(),
            bkt,
            SourceType::Receive,
            false,
            ShipperMetrics::new(None),
        );

        let err = shipper.sync().await.unwrap_err();
        assert!(matches!(err, ShipperError::PartialSync { shipped: 0, failed: 1 }));

        // Verify meta.json was uploaded LAST.
        let order = store.put_order.lock().clone();
        assert_eq!(order.len(), 3);
        assert!(order.last().unwrap().ends_with("/meta.json"));

        // Second sync sees meta.json exists and reports 1 shipped without re-uploading.
        let shipped = shipper.sync().await.unwrap();
        assert_eq!(shipped, 1);
        assert_eq!(store.put_order.lock().len(), 3);
    }

    #[test]
    fn atomic_write_file_cleans_up_tmp_on_rename_error() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("kafka-offset.json");
        // Make target a non-empty directory so rename(&tmp, &target) fails with EISDIR.
        fs::create_dir_all(target.join("blocker")).unwrap();

        let err = atomic_write_file(&target, b"{}").unwrap_err();
        assert!(err.raw_os_error().is_some());
        assert!(
            !dir.path().join("kafka-offset.json.tmp").exists(),
            "orphan .tmp file must be removed on error"
        );
    }

    #[test]
    fn block_and_shipper_meta_accept_go_null_maps_and_slices() {
        let id = Ulid::from_parts(1000, 1);
        let block_json = format!(
            r#"{{
                "ulid": "{id}",
                "minTime": 1000,
                "maxTime": 2000,
                "stats": null,
                "compaction": {{
                    "level": 1,
                    "sources": null,
                    "parents": null,
                    "hints": null
                }},
                "version": 1,
                "thanos": {{
                    "version": 1,
                    "labels": null,
                    "source": "receive",
                    "segment_files": null,
                    "files": null
                }}
            }}"#
        );
        let dir = tempfile::tempdir().unwrap();
        let bdir = dir.path().join(id.to_string());
        fs::create_dir_all(&bdir).unwrap();
        fs::write(bdir.join(META_FILENAME), block_json).unwrap();
        let meta = BlockMeta::read_from_dir(&bdir).unwrap();
        assert!(meta.thanos.labels.is_empty());
        assert!(meta.thanos.segment_files.is_empty());
        assert!(meta.thanos.files.is_empty());
        assert!(meta.compaction.sources.is_empty());
        assert!(meta.compaction.parents.is_empty());
        assert!(meta.compaction.hints.is_empty());

        let shipper_json = r#"{"version": 1, "shipped": null}"#;
        let sm: ShipperMeta = serde_json::from_str(shipper_json).unwrap();
        assert!(sm.shipped.is_empty());
    }

    #[tokio::test]
    async fn upload_failure_cleanup_deletes_meta_json_first() {
        use futures::stream::BoxStream;
        use object_store::path::Path as ObjPath;
        use object_store::{
            GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
            PutMultipartOpts, PutOptions, PutPayload, PutResult,
        };
        use parking_lot::Mutex;

        #[derive(Debug)]
        struct DeleteOrderStore {
            inner: object_store::memory::InMemory,
            delete_order: Mutex<Vec<String>>,
        }
        impl std::fmt::Display for DeleteOrderStore {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "DeleteOrderStore")
            }
        }
        impl ObjectStore for DeleteOrderStore {
            fn put_opts<'life0, 'life1, 'async_trait>(
                &'life0 self,
                location: &'life1 ObjPath,
                payload: PutPayload,
                opts: PutOptions,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = object_store::Result<PutResult>> + Send + 'async_trait>>
            where 'life0: 'async_trait, 'life1: 'async_trait, Self: 'async_trait {
                let loc = location.to_string();
                let location = location.clone();
                Box::pin(async move {
                    if loc.ends_with("/index") {
                        return Err(object_store::Error::Generic {
                            store: "DeleteOrderStore",
                            source: "simulated index upload failure".into(),
                        });
                    }
                    self.inner.put_opts(&location, payload, opts).await
                })
            }
            fn put_multipart_opts<'life0, 'life1, 'async_trait>(
                &'life0 self,
                location: &'life1 ObjPath,
                opts: PutMultipartOpts,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = object_store::Result<Box<dyn MultipartUpload>>> + Send + 'async_trait>>
            where 'life0: 'async_trait, 'life1: 'async_trait, Self: 'async_trait {
                let location = location.clone();
                Box::pin(async move { self.inner.put_multipart_opts(&location, opts).await })
            }
            fn get_opts<'life0, 'life1, 'async_trait>(
                &'life0 self,
                location: &'life1 ObjPath,
                options: GetOptions,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = object_store::Result<GetResult>> + Send + 'async_trait>>
            where 'life0: 'async_trait, 'life1: 'async_trait, Self: 'async_trait {
                let location = location.clone();
                Box::pin(async move { self.inner.get_opts(&location, options).await })
            }
            fn delete<'life0, 'life1, 'async_trait>(
                &'life0 self,
                location: &'life1 ObjPath,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = object_store::Result<()>> + Send + 'async_trait>>
            where 'life0: 'async_trait, 'life1: 'async_trait, Self: 'async_trait {
                let loc = location.to_string();
                let location = location.clone();
                Box::pin(async move {
                    self.delete_order.lock().push(loc);
                    self.inner.delete(&location).await
                })
            }
            fn list(&self, prefix: Option<&ObjPath>) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
                self.inner.list(prefix)
            }
            fn list_with_delimiter<'life0, 'life1, 'async_trait>(
                &'life0 self,
                prefix: Option<&'life1 ObjPath>,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = object_store::Result<ListResult>> + Send + 'async_trait>>
            where 'life0: 'async_trait, 'life1: 'async_trait, Self: 'async_trait {
                let prefix = prefix.cloned();
                Box::pin(async move { self.inner.list_with_delimiter(prefix.as_ref()).await })
            }
            fn copy<'life0, 'life1, 'life2, 'async_trait>(
                &'life0 self,
                from: &'life1 ObjPath,
                to: &'life2 ObjPath,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = object_store::Result<()>> + Send + 'async_trait>>
            where 'life0: 'async_trait, 'life1: 'async_trait, 'life2: 'async_trait, Self: 'async_trait {
                let from = from.clone();
                let to = to.clone();
                Box::pin(async move { self.inner.copy(&from, &to).await })
            }
            fn copy_if_not_exists<'life0, 'life1, 'life2, 'async_trait>(
                &'life0 self,
                from: &'life1 ObjPath,
                to: &'life2 ObjPath,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = object_store::Result<()>> + Send + 'async_trait>>
            where 'life0: 'async_trait, 'life1: 'async_trait, 'life2: 'async_trait, Self: 'async_trait {
                let from = from.clone();
                let to = to.clone();
                Box::pin(async move { self.inner.copy_if_not_exists(&from, &to).await })
            }
        }

        let store = Arc::new(DeleteOrderStore {
            inner: object_store::memory::InMemory::new(),
            delete_order: Mutex::new(Vec::new()),
        });
        let bkt = BucketClient::from_object_store(store.clone(), "del", "DEL");
        let id = Ulid::from_parts(7000, 7);
        // Pre-seed an old meta.json to verify cleanup deletes meta.json FIRST (before chunks).
        bkt.upload(&format!("{id}/{META_FILENAME}"), Bytes::from_static(b"{}"))
            .await
            .unwrap();

        let blocks_dir = tempfile::tempdir().unwrap();
        let mut meta = make_meta(id, 7000, 8000, 10);
        create_test_block(blocks_dir.path(), id, &meta);
        let err = upload_block_to_bucket(&bkt, &blocks_dir.path().join(id.to_string()), &mut meta)
            .await
            .unwrap_err();
        assert!(matches!(err, ShipperError::Upload { file_type: "index", .. }));
        let dels = store.delete_order.lock().clone();
        assert!(!dels.is_empty());
        assert!(
            dels[0].ends_with(&format!("{id}/{META_FILENAME}")),
            "meta.json must be deleted first, got {dels:?}"
        );
    }
}
