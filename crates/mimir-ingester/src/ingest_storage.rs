//! Kafka ingest_storage integration for the ingester.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/ingester/ingester.go (ingest_storage init)
//! - github.com/grafana/mimir/pkg/storage/ingest/util.go (IngesterPartitionID)

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use mimir_common::thread_pool::ScopedWorkerPool;
use mimir_config::KafkaConfig;
use mimir_proto::cortexpb::{self, ErrorCause};
use mimir_proto::ringpb::PartitionRingDesc;
use mimir_proto::zerocopy::{
    decode_rw2_stream, BorrowedMetadata, DecodeOptions, Rw2ResolvedScratch, WriteRequestScratch,
};
use mimir_ring::partition_ring::PartitionRingWatcherDelegate;
use mimir_storage::ingest::version::{deserialize_record_content, parse_record_version};
use mimir_storage::ingest::{
    write_compartment_configs, ErrorSampler as StorageErrorSampler, FetchedRecord, LogPolicy,
    MultiClusterPartitionReader, PartitionOffsets, PreCommitNotifier, PushErrorHandler,
    PusherConsumerMetrics, PusherError, ReaderError, RecordBatchConsumer,
    SingleClusterPartitionReader, READ_COMPARTMENT_ID_PLACEHOLDER,
};
use mimir_tsdb::labels::{append_stringlabel_pair, Labels};
use parking_lot::Mutex;
use smallvec::SmallVec;
use tracing::{error, info};
use xxhash_rust::xxh64::xxh64;

use crate::errors::IngesterError;
use crate::ingester::Ingester;
use crate::push::{
    convert_borrowed_exemplar, convert_borrowed_histogram, fast_utf8_str, ConvertedHistogram,
    PrehashedSeriesItem,
};

/// Parses the numeric partition ID suffix from `ingester_id` (e.g. `"ingester-zone-a-2"` -> `2`).
pub fn ingester_partition_id_from_instance_id(ingester_id: &str) -> Result<i32, String> {
    let Some(pos) = ingester_id.rfind('-') else {
        return Err(format!(
            "ingester ID {ingester_id} doesn't match regular expression \"-([0-9]+)$\""
        ));
    };
    let suffix = &ingester_id[pos + 1..];
    if suffix.is_empty() || !suffix.bytes().all(|b| b.is_ascii_digit()) {
        return Err(format!(
            "ingester ID {ingester_id} doesn't match regular expression \"-([0-9]+)$\""
        ));
    }
    suffix
        .parse::<i32>()
        .map_err(|e| format!("no ingester sequence number in ingester ID {ingester_id}: {e}"))
}

pub enum IngestReaderHandle {
    Single(Arc<SingleClusterPartitionReader>),
    Multi(Arc<MultiClusterPartitionReader>),
}

impl IngestReaderHandle {
    pub fn last_seen_offsets(&self) -> PartitionOffsets {
        match self {
            Self::Single(r) => r.last_seen_offsets(),
            Self::Multi(r) => r.last_seen_offsets(),
        }
    }

    pub async fn wait_read_consistency_until_offsets(
        &self,
        offsets: &PartitionOffsets,
        timeout: Duration,
    ) -> Result<(), ReaderError> {
        match self {
            Self::Single(r) => {
                r.wait_read_consistency_until_offsets(offsets, timeout)
                    .await
            }
            Self::Multi(r) => {
                r.wait_read_consistency_until_offsets(offsets, timeout)
                    .await
            }
        }
    }

    pub async fn wait_read_consistency_until_last_produced_offset(
        &self,
        timeout: Duration,
    ) -> Result<(), ReaderError> {
        match self {
            Self::Single(r) => {
                r.wait_read_consistency_until_last_produced_offset(timeout)
                    .await
            }
            Self::Multi(r) => {
                r.wait_read_consistency_until_last_produced_offset(timeout)
                    .await
            }
        }
    }

    pub fn enforce_read_max_delay(&self, max_delay: Duration) -> Result<(), ReaderError> {
        match self {
            Self::Single(r) => r.enforce_read_max_delay(max_delay),
            Self::Multi(r) => r.enforce_read_max_delay(max_delay),
        }
    }

    pub async fn start(&self) -> Result<(), ReaderError> {
        match self {
            Self::Single(r) => r.start().await,
            Self::Multi(r) => r.start().await,
        }
    }

    pub async fn stop(&self) {
        match self {
            Self::Single(r) => r.stop().await,
            Self::Multi(r) => r.stop().await,
        }
    }
}

/// Adapts [`Ingester`] into [`PreCommitNotifier`].
pub struct IngesterStorageAdapter(pub Arc<Ingester>);

fn ingester_err_to_pusher_err(err: IngesterError) -> PusherError {
    let cause = err.error_cause();
    let grpc_code = match cause {
        ErrorCause::BadData => "InvalidArgument",
        ErrorCause::TenantLimit => "FailedPrecondition",
        ErrorCause::ServiceUnavailable
        | ErrorCause::InstanceLimit
        | ErrorCause::CircuitBreakerOpen
        | ErrorCause::RequestRateLimited => "Unavailable",
        ErrorCause::MethodNotAllowed => "Unimplemented",
        _ => "Internal",
    };
    let log_policy = if err.is_do_not_log() {
        Some(LogPolicy::DoNotLog)
    } else if err.is_optional_logging() {
        Some(LogPolicy::Sampled {
            should_log: err.should_log_error(),
            reason: err.sampling_reason().unwrap_or_default(),
        })
    } else {
        None
    };
    PusherError {
        message: err.to_string(),
        cause: Some(cause),
        grpc_code: Some(grpc_code.to_string()),
        log_policy,
    }
}

impl PreCommitNotifier for IngesterStorageAdapter {
    fn notify_pre_commit(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>> {
        // The fsyncs are blocking: run them on the blocking pool, not on the reader task.
        let ingester = Arc::clone(&self.0);
        Box::pin(async move {
            tokio::task::spawn_blocking(move || ingester.notify_pre_commit())
                .await
                .map_err(|e| format!("fsync task failed: {e}"))?
                .map_err(|e| e.to_string())
        })
    }
}

#[derive(Clone, Copy)]
struct ShardSeriesItem {
    worker_idx: u16,
    rec_seq: u32,
    record_offset: i64,
    tenant_idx: u32,
    source: i32,
    skip_label_validation: bool,
    labels_sorted_unique: bool,
    bad_utf8: bool,
    prom_hash: u64,
    label_bytes_start: u32,
    label_bytes_len: u32,
    samples_start: u32,
    samples_len: u32,
    created_timestamp: i64,
    histograms_start: u32,
    histograms_len: u32,
    exemplars_start: u32,
    exemplars_len: u32,
    min_ts: i64,
}

/// Metric metadata entry stashed during Phase A; strings live in `MetadataStash::bytes`.
#[derive(Clone, Copy)]
struct ShardMetadataItem {
    worker_idx: u16,
    rec_seq: u32,
    tenant_idx: u32,
    source: cortexpb::write_request::SourceEnum,
    record_offset: i64,
    metric_type: i32,
    trace_id_idx: u32,
    /// `[name_start, help_start, unit_start, unit_end)` offsets into `MetadataStash::bytes`.
    bounds: [u32; 4],
}

struct WorkerArena {
    wr_scratch: WriteRequestScratch,
    rw2_resolved: Rw2ResolvedScratch,
    label_bytes_arena: Vec<u8>,
    samples_arena: Vec<cortexpb::Sample>,
    histograms_arena: Vec<ConvertedHistogram>,
    exemplars_arena: Vec<Result<(i64, f64, Labels, String), crate::errors::IngesterError>>,
    shard_buckets: Vec<Vec<ShardSeriesItem>>,
    metadata: MetadataStash,
    trace_ids: Vec<String>,
    /// `(rec_seq, record_offset, tenant_idx)` for non-empty records whose tenant key is empty/invalid.
    tenant_key_errors: Vec<(u32, i64, u32)>,
    total_series: usize,
}

/// Per-worker metric metadata copied out of the records during Phase A, bucketed by shard.
struct MetadataStash {
    bytes: Vec<u8>,
    buckets: Vec<Vec<ShardMetadataItem>>,
    total: usize,
}

impl MetadataStash {
    fn new(num_shards: usize) -> Self {
        Self {
            bytes: Vec::new(),
            buckets: (0..num_shards).map(|_| Vec::new()).collect(),
            total: 0,
        }
    }

    fn reset_for(&mut self, num_shards: usize) {
        self.clear();
        if self.buckets.len() < num_shards {
            self.buckets.resize_with(num_shards, Vec::new);
        }
    }

    fn clear(&mut self) {
        self.bytes.clear();
        for b in &mut self.buckets {
            b.clear();
        }
        self.total = 0;
    }

    /// Copies the metadata of one record, distributing entries round-robin across `spread`
    /// consecutive shards (the tenant's Go `idealShardsFor`) like Go
    /// `parallelStorageShards.PushToStorageAndReleaseRequest`.
    fn stash<'m>(
        &mut self,
        num_shards: usize,
        spread: usize,
        item: ShardMetadataItem,
        first_series_shard: Option<usize>,
        metadata: impl Iterator<Item = BorrowedMetadata<'m>>,
    ) {
        let num_shards = num_shards.clamp(1, self.buckets.len().max(1));
        let spread = spread.clamp(1, num_shards);
        // Go starts from a random shard to avoid hotspots. Start from the shard holding the
        // record's first series so metadata of small tenants (one ideal shard in Go) flushes
        // with its series as one request, else from the record offset as a well-distributed
        // stand-in.
        let start = first_series_shard
            .unwrap_or((item.record_offset as u64 % num_shards as u64) as usize);
        for (i, m) in metadata.enumerate() {
            let shard = (start + i % spread) % num_shards;
            let name_start = self.bytes.len() as u32;
            self.bytes.extend_from_slice(m.metric_family_name.as_bytes());
            let help_start = self.bytes.len() as u32;
            self.bytes.extend_from_slice(m.help.as_bytes());
            let unit_start = self.bytes.len() as u32;
            self.bytes.extend_from_slice(m.unit.as_bytes());
            let unit_end = self.bytes.len() as u32;
            if let Some(bucket) = self.buckets.get_mut(shard) {
                bucket.push(ShardMetadataItem {
                    metric_type: m.metric_type,
                    bounds: [name_start, help_start, unit_start, unit_end],
                    ..item
                });
                self.total += 1;
            }
        }
    }

    #[inline]
    fn view(&self, item: &ShardMetadataItem) -> BorrowedMetadata<'_> {
        let [n, h, u, end] = item.bounds.map(|b| b as usize);
        let s = |a: usize, b: usize| {
            self.bytes
                .get(a..b)
                .and_then(|raw| std::str::from_utf8(raw).ok())
                .unwrap_or_default()
        };
        BorrowedMetadata {
            metric_type: item.metric_type,
            metric_family_name: s(n, h),
            help: s(h, u),
            unit: s(u, end),
        }
    }
}

impl WorkerArena {
    fn new(num_shards: usize) -> Self {
        Self {
            wr_scratch: WriteRequestScratch::new(),
            rw2_resolved: Rw2ResolvedScratch::new(),
            label_bytes_arena: Vec::with_capacity(65_536),
            samples_arena: Vec::with_capacity(512),
            histograms_arena: Vec::new(),
            exemplars_arena: Vec::new(),
            shard_buckets: (0..num_shards).map(|_| Vec::with_capacity(128)).collect(),
            metadata: MetadataStash::new(num_shards),
            trace_ids: Vec::new(),
            tenant_key_errors: Vec::new(),
            total_series: 0,
        }
    }

    fn reset_for(&mut self, num_shards: usize) {
        self.wr_scratch.clear();
        self.label_bytes_arena.clear();
        self.samples_arena.clear();
        self.histograms_arena.clear();
        self.exemplars_arena.clear();
        if self.shard_buckets.len() < num_shards {
            self.shard_buckets
                .resize_with(num_shards, || Vec::with_capacity(128));
        }
        for b in &mut self.shard_buckets {
            b.clear();
        }
        self.metadata.reset_for(num_shards);
        self.trace_ids.clear();
        self.tenant_key_errors.clear();
        self.total_series = 0;
    }

    #[inline]
    fn label_bytes_slice(&self, start: u32, len: u32) -> &[u8] {
        let s = start as usize;
        let e = s + len as usize;
        &self.label_bytes_arena[s..e]
    }
}

struct ArenaPoolGuard<'a> {
    pool: &'a Mutex<Vec<WorkerArena>>,
    arenas: Vec<WorkerArena>,
}

impl Drop for ArenaPoolGuard<'_> {
    fn drop(&mut self) {
        let mut arenas = std::mem::take(&mut self.arenas);
        for a in &mut arenas {
            a.label_bytes_arena.clear();
            a.samples_arena.clear();
            a.histograms_arena.clear();
            a.exemplars_arena.clear();
            a.wr_scratch.clear();
            for b in &mut a.shard_buckets {
                b.clear();
                if b.capacity() > 2048 {
                    b.shrink_to(512);
                }
            }
            a.metadata.clear();
            if a.metadata.bytes.capacity() > 256 * 1024 {
                a.metadata.bytes = Vec::new();
            }
            // Prevent retaining outlier allocations after massive spikes.
            if a.label_bytes_arena.capacity() > 512 * 1024 {
                a.label_bytes_arena = Vec::with_capacity(65_536);
            }
            if a.samples_arena.capacity() > 16_384 {
                a.samples_arena = Vec::with_capacity(512);
            }
        }
        let mut pool = self.pool.lock();
        for a in arenas {
            if pool.len() < 16 {
                pool.push(a);
            }
        }
    }
}

struct MutSlicePtr<T>(*mut T);

impl<T> MutSlicePtr<T> {
    /// SAFETY: caller must ensure `idx` is in-bounds and no two threads concurrently
    /// call `get(idx)` with the same `idx`.
    #[inline]
    #[allow(clippy::mut_from_ref)]
    unsafe fn get(&self, idx: usize) -> &mut T {
        &mut *self.0.add(idx)
    }
}

// SAFETY: callers only access disjoint indices `idx` across worker jobs.
unsafe impl<T: Send> Send for MutSlicePtr<T> {}
unsafe impl<T: Send> Sync for MutSlicePtr<T> {}

/// Zero-allocation, pre-hashed, two-phase scoped-parallel Kafka `ingest_storage` consumer.
///
/// Phase A: parallel decode (`BorrowedWriteRequest`) + metadata + single `xxh64` hash +
/// power-of-two `StripeSeries`-aligned shard bucketing.
/// Phase B: parallel shard ingestion directly into `Ingester::ingest_prehashed_shard_batch`
/// in batches of up to `ingestion_concurrency_batch_size` series per flush.
pub struct IngesterShardedBatchConsumer {
    ingester: Arc<Ingester>,
    kafka_cfg: KafkaConfig,
    metrics: PusherConsumerMetrics,
    processing_time_timeseries: OnceLock<prometheus::Histogram>,
    processing_time_metadata: OnceLock<prometheus::Histogram>,
    processing_time_timeseries_and_metadata: OnceLock<prometheus::Histogram>,
    error_handler: Arc<PushErrorHandler>,
    arena_pool: Mutex<Vec<WorkerArena>>,
    worker_pool: ScopedWorkerPool,
}

impl IngesterShardedBatchConsumer {
    pub fn new(
        ingester: Arc<Ingester>,
        kafka_cfg: KafkaConfig,
        metrics: PusherConsumerMetrics,
    ) -> Self {
        let sampler = if ingester.cfg.error_sample_rate > 0 {
            Some(Arc::new(StorageErrorSampler::new(
                ingester.cfg.error_sample_rate,
            )))
        } else {
            None
        };
        let error_handler = Arc::new(PushErrorHandler::new(
            metrics.storage_pusher_metrics.clone(),
            sampler,
            None,
        ));
        let hw_threads = std::thread::available_parallelism().map_or(4, |n| n.get());
        let max_shards = if kafka_cfg.ingestion_concurrency_max <= 0 {
            1
        } else {
            let max_s = kafka_cfg.ingestion_concurrency_max as usize;
            (1usize << (usize::BITS - 1 - max_s.leading_zeros())).clamp(1, 16)
        };
        let pool_threads = hw_threads.min(8).max(max_shards).saturating_sub(1);
        let worker_pool = ScopedWorkerPool::new("ingest-batch", pool_threads);
        Self {
            ingester,
            kafka_cfg,
            metrics,
            processing_time_timeseries: OnceLock::new(),
            processing_time_metadata: OnceLock::new(),
            processing_time_timeseries_and_metadata: OnceLock::new(),
            error_handler,
            arena_pool: Mutex::new(Vec::new()),
            worker_pool,
        }
    }

    fn take_arenas(&self, count: usize, num_shards: usize) -> ArenaPoolGuard<'_> {
        let mut arenas = Vec::with_capacity(count);
        {
            let mut pool = self.arena_pool.lock();
            while arenas.len() < count {
                if let Some(mut a) = pool.pop() {
                    a.reset_for(num_shards);
                    arenas.push(a);
                } else {
                    break;
                }
            }
        }
        while arenas.len() < count {
            arenas.push(WorkerArena::new(num_shards));
        }
        ArenaPoolGuard {
            pool: &self.arena_pool,
            arenas,
        }
    }

    pub fn consume_records_sync(&self, records: &[FetchedRecord]) -> Result<(), String> {
        if records.is_empty() {
            return Ok(());
        }

        // Map each record to a deduplicated tenant index + accumulate bytes per tenant for metrics.
        let mut tenants: SmallVec<[String; 4]> = SmallVec::new();
        let mut tenant_bytes: SmallVec<[usize; 4]> = SmallVec::new();
        let mut rec_tenant_idx: SmallVec<[u32; 64]> = SmallVec::with_capacity(records.len());
        let mut fallback_map: Option<HashMap<String, u32>> = None;
        for rec in records {
            let tid_cow = String::from_utf8_lossy(&rec.key);
            let tid_str = tid_cow.as_ref();
            let tid_idx = if let Some(ref mut map) = fallback_map {
                if let Some(&idx) = map.get(tid_str) {
                    tenant_bytes[idx as usize] += rec.value.len();
                    idx
                } else {
                    let idx = tenants.len() as u32;
                    tenants.push(tid_str.to_owned());
                    tenant_bytes.push(rec.value.len());
                    map.insert(tid_str.to_owned(), idx);
                    idx
                }
            } else if let Some(pos) = tenants.iter().position(|t| t == tid_str) {
                tenant_bytes[pos] += rec.value.len();
                pos as u32
            } else if tenants.len() < 8 {
                let idx = tenants.len() as u32;
                tenants.push(tid_str.to_owned());
                tenant_bytes.push(rec.value.len());
                idx
            } else {
                let mut map = HashMap::with_capacity(16);
                for (i, t) in tenants.iter().enumerate() {
                    map.insert(t.clone(), i as u32);
                }
                let idx = tenants.len() as u32;
                tenants.push(tid_str.to_owned());
                tenant_bytes.push(rec.value.len());
                map.insert(tid_str.to_owned(), idx);
                fallback_map = Some(map);
                idx
            };
            rec_tenant_idx.push(tid_idx);
        }

        // Resolve every distinct record key once to its canonical tenant ID like Go's
        // `tenant.TenantID(ctx)` (dskit/tenant/resolver.go): strip `:key=value` metadata
        // (`123456:product=k6` -> `123456`), accept `a|a`, and apply dskit's strict
        // validation (prevents path traversal).
        let mut tenant_ids: SmallVec<[String; 4]> = SmallVec::with_capacity(tenants.len());
        let mut tenant_invalid_errors: SmallVec<[Option<String>; 4]> =
            SmallVec::with_capacity(tenants.len());
        for tid in &tenants {
            if tid.is_empty() {
                tenant_ids.push(String::new());
                tenant_invalid_errors.push(None);
                continue;
            }
            match mimir_common::tenant::parse_single_tenant_id(tid) {
                Ok(id) => {
                    tenant_ids.push(id);
                    tenant_invalid_errors.push(None);
                }
                Err(e) => {
                    tenant_ids.push(String::new());
                    tenant_invalid_errors.push(Some(e.to_string()));
                }
            }
        }

        let is_sequential = self.kafka_cfg.ingestion_concurrency_max <= 0;
        let max_batch_series = if is_sequential {
            usize::MAX
        } else {
            self.kafka_cfg.ingestion_concurrency_batch_size.max(1) as usize
        };

        let num_shards = if is_sequential {
            1
        } else {
            let max_s = self.kafka_cfg.ingestion_concurrency_max as usize;
            let p2 = 1usize << (usize::BITS - 1 - max_s.leading_zeros());
            p2.clamp(1, 16)
        };
        let shard_mask = num_shards - 1;

        if self.kafka_cfg.ingestion_concurrency_max > 0 {
            let bytes_per_sample = self
                .kafka_cfg
                .ingestion_concurrency_estimated_bytes_per_sample
                .max(1) as usize;
            let mut est_total = 0usize;
            for &b in &tenant_bytes {
                est_total += b / bytes_per_sample;
            }
            if est_total > 0 {
                self.metrics
                    .storage_pusher_metrics
                    .estimated_timeseries
                    .inc_by(est_total as f64);
            }
        }

        // Go `parallelStoragePusher.idealShardsFor`: each tenant's series (and metadata) are
        // spread over at most that many shards, so a small tenant's batch is pushed as one
        // request like Go. Distinct tenants start at distinct shards to keep Phase B parallel.
        let tenant_shard_spread: SmallVec<[usize; 4]> = tenant_bytes
            .iter()
            .map(|&bytes| {
                let k = &self.kafka_cfg;
                if k.ingestion_concurrency_max <= 0 {
                    return 1;
                }
                let expected = bytes as i64
                    / k.ingestion_concurrency_estimated_bytes_per_sample.max(1);
                let ideal = expected
                    / k.ingestion_concurrency_batch_size.max(1)
                    / k.ingestion_concurrency_target_flushes_per_shard.max(1);
                ideal.min(k.ingestion_concurrency_max).max(1) as usize
            })
            .collect();
        let shard_for = |tenant_idx: u32, prom_hash: u64| -> usize {
            let spread = tenant_shard_spread
                .get(tenant_idx as usize)
                .copied()
                .unwrap_or(1);
            if spread >= num_shards {
                (prom_hash as usize) & shard_mask
            } else {
                (tenant_idx as usize + (prom_hash % spread as u64) as usize) & shard_mask
            }
        };

        let hw_threads = std::thread::available_parallelism().map_or(4, |n| n.get());
        let num_workers = if records.len() <= 1 {
            1
        } else {
            records.len().min(hw_threads).min(8)
        };

        let mut pool_guard = self.take_arenas(num_workers, num_shards);
        let batch_started = Instant::now();

        // Phase A: parallel decode + metadata + single xxh64 hash + shard bucketing.
        let decode_worker = |worker_idx: usize,
                             rec_offset_start: usize,
                             slice: &[FetchedRecord],
                             tids: &[u32],
                             arena: &mut WorkerArena| {
            let WorkerArena {
                wr_scratch,
                rw2_resolved,
                label_bytes_arena,
                samples_arena,
                histograms_arena,
                exemplars_arena,
                shard_buckets,
                metadata,
                trace_ids,
                tenant_key_errors,
                total_series,
            } = arena;
            let propagators = mimir_common::tracing::global_tracing()
                .map(|t| t.propagators.clone())
                .unwrap_or_default();
            let topic_name = &self.kafka_cfg.topic;
            let mut tmp_ex: Vec<Result<(i64, f64, Labels, String), crate::errors::IngesterError>> =
                Vec::new();
            let mut bucket_bases = [0usize; 16];
            for (local_i, rec) in slice.iter().enumerate() {
                let rec_seq = (rec_offset_start + local_i) as u32;
                let tenant_idx = tids[local_i];
                let bad_tenant_key = tenants[tenant_idx as usize].is_empty()
                    || tenant_invalid_errors[tenant_idx as usize].is_some();
                if bad_tenant_key {
                    let version = parse_record_version(
                        rec.headers.iter().map(|(k, v)| (k.as_str(), v.as_ref())),
                    );
                    if version == 2 {
                        let label_bytes_base = label_bytes_arena.len();
                        let samples_base = samples_arena.len();
                        let mut rec_metadata: Vec<BorrowedMetadata<'_>> = Vec::new();
                        let mut rec_series_count = 0usize;
                        let stream_res = decode_rw2_stream(
                            &rec.value,
                            DecodeOptions::kafka_v2(),
                            wr_scratch,
                            rw2_resolved,
                            label_bytes_arena,
                            samples_arena,
                            &mut rec_metadata,
                            |_| {
                                rec_series_count += 1;
                            },
                        );
                        label_bytes_arena.truncate(label_bytes_base);
                        samples_arena.truncate(samples_base);
                        match stream_res {
                            Ok(_) => {
                                if rec_series_count > 0 || !rec_metadata.is_empty() {
                                    tenant_key_errors.push((rec_seq, rec.offset, tenant_idx));
                                }
                            }
                            Err(e) => {
                                let err_msg = format!("parsing ingest consumer write request: {e}");
                                error!(err = %err_msg, "failed to parse write request; skipping");
                            }
                        }
                        drop(rec_metadata);
                        wr_scratch.clear();
                    } else {
                        match deserialize_record_content(&rec.value, version, wr_scratch) {
                            Ok(borrowed) => {
                                if borrowed.timeseries_len() > 0 || borrowed.metadata_len() > 0 {
                                    tenant_key_errors.push((rec_seq, rec.offset, tenant_idx));
                                }
                            }
                            Err(e) => {
                                let err_msg = format!("parsing ingest consumer write request: {e}");
                                error!(err = %err_msg, "failed to parse write request; skipping");
                            }
                        }
                        wr_scratch.clear();
                    }
                    continue;
                }

                let rec_trace_idx = if rec.headers.is_empty() {
                    u32::MAX
                } else {
                    let extracted = mimir_common::tracing::extract_from_headers(
                        rec.headers.iter().filter_map(|(k, v)| {
                            std::str::from_utf8(v).ok().map(|vs| (k.as_str(), vs))
                        }),
                        &propagators,
                    );
                    let (span_ctx, parent_span_id) =
                        mimir_common::tracing::start_server_span_context(&extracted, false);
                    if span_ctx.is_sampled() {
                        if let Some(rt) = mimir_common::tracing::global_tracing() {
                            if rt.exporter.is_some() {
                                let now_ns = mimir_common::tracing::now_unix_nanos();
                                mimir_common::tracing::export_span(
                                    mimir_common::tracing::ExportedSpan {
                                        service_name: rt.service_name.clone(),
                                        trace_id: span_ctx.trace_id,
                                        span_id: span_ctx.span_id,
                                        parent_span_id,
                                        name: format!("{topic_name} receive"),
                                        kind: 5,
                                        start_time_unix_nano: now_ns,
                                        end_time_unix_nano: now_ns,
                                        attributes: vec![
                                            mimir_common::tracing::SpanAttribute {
                                                key: "messaging.system".to_string(),
                                                value: mimir_common::tracing::SpanAttrValue::String(
                                                    "kafka".to_string(),
                                                ),
                                            },
                                            mimir_common::tracing::SpanAttribute {
                                                key: "messaging.destination.name".to_string(),
                                                value: mimir_common::tracing::SpanAttrValue::String(
                                                    topic_name.clone(),
                                                ),
                                            },
                                        ],
                                        status_error: false,
                                    },
                                );
                            }
                        }
                        let idx = trace_ids.len() as u32;
                        trace_ids.push(span_ctx.trace_id_hex());
                        idx
                    } else {
                        u32::MAX
                    }
                };

                let version = parse_record_version(
                    rec.headers.iter().map(|(k, v)| (k.as_str(), v.as_ref())),
                );

                if version == 2 {
                    let label_bytes_base = label_bytes_arena.len();
                    let samples_base = samples_arena.len();
                    let hist_base = histograms_arena.len();
                    let ex_base = exemplars_arena.len();
                    let total_series_base = *total_series;
                    for s_idx in 0..num_shards {
                        bucket_bases[s_idx] = shard_buckets[s_idx].len();
                    }

                    let mut rec_metadata: Vec<BorrowedMetadata<'_>> = Vec::new();
                    let mut rec_first_shard: Option<usize> = None;
                    let stream_res = decode_rw2_stream(
                        &rec.value,
                        DecodeOptions::kafka_v2(),
                        wr_scratch,
                        rw2_resolved,
                        label_bytes_arena,
                        samples_arena,
                        &mut rec_metadata,
                        |ts| {
                            let label_bytes_start = ts.label_bytes_range.start as u32;
                            let label_bytes_len =
                                (ts.label_bytes_range.end - ts.label_bytes_range.start) as u32;
                            let prom_hash = if !ts.bad_utf8
                                && ts.labels_sorted_unique
                                && label_bytes_len > 0
                            {
                                xxh64(ts.encoded_labels, 0)
                            } else {
                                0
                            };

                            let mut min_ts = ts.min_sample_ts;
                            let hist_start = histograms_arena.len() as u32;
                            let mut hist_len = 0u32;
                            if ts.histograms_len() > 0 {
                                for h in ts.histograms() {
                                    let conv = convert_borrowed_histogram(&h);
                                    min_ts = min_ts.min(conv.0);
                                    histograms_arena.push(conv);
                                    hist_len += 1;
                                }
                            }

                            let ex_start = exemplars_arena.len() as u32;
                            let mut ex_len = 0u32;
                            if ts.exemplars_len() > 0 {
                                tmp_ex.clear();
                                for ex in ts.exemplars() {
                                    let res = convert_borrowed_exemplar(&ex);
                                    if let Ok(ref conv) = res {
                                        min_ts = min_ts.min(conv.0);
                                    }
                                    tmp_ex.push(res);
                                    ex_len += 1;
                                }
                                exemplars_arena.append(&mut tmp_ex);
                            }

                            let shard = shard_for(tenant_idx, prom_hash);
                            rec_first_shard.get_or_insert(shard);
                            shard_buckets[shard].push(ShardSeriesItem {
                                worker_idx: worker_idx as u16,
                                rec_seq,
                                record_offset: rec.offset,
                                tenant_idx,
                                source: ts.source,
                                skip_label_validation: ts.skip_label_validation,
                                labels_sorted_unique: ts.labels_sorted_unique,
                                bad_utf8: ts.bad_utf8,
                                prom_hash,
                                label_bytes_start,
                                label_bytes_len,
                                samples_start: ts.samples_range.start as u32,
                                samples_len: (ts.samples_range.end - ts.samples_range.start)
                                    as u32,
                                created_timestamp: ts.created_timestamp,
                                histograms_start: hist_start,
                                histograms_len: hist_len,
                                exemplars_start: ex_start,
                                exemplars_len: ex_len,
                                min_ts,
                            });
                            *total_series += 1;
                        },
                    );

                    match stream_res {
                        Ok(hdr) => {
                            if hdr.header_changed_after_series {
                                for s_idx in 0..num_shards {
                                    for item in &mut shard_buckets[s_idx][bucket_bases[s_idx]..] {
                                        item.source = hdr.source;
                                        item.skip_label_validation = hdr.skip_label_validation;
                                    }
                                }
                            }
                            if !rec_metadata.is_empty() {
                                let norm_source =
                                    cortexpb::write_request::SourceEnum::try_from(hdr.source)
                                        .unwrap_or(cortexpb::write_request::SourceEnum::Api);
                                metadata.stash(
                                    num_shards,
                                    tenant_shard_spread[tenant_idx as usize],
                                    ShardMetadataItem {
                                        worker_idx: worker_idx as u16,
                                        rec_seq,
                                        tenant_idx,
                                        source: norm_source,
                                        record_offset: rec.offset,
                                        metric_type: 0,
                                        trace_id_idx: rec_trace_idx,
                                        bounds: [0; 4],
                                    },
                                    rec_first_shard,
                                    rec_metadata.iter().copied(),
                                );
                            }
                        }
                        Err(e) => {
                            let err_msg = format!("parsing ingest consumer write request: {e}");
                            error!(err = %err_msg, "failed to parse write request; skipping");
                            label_bytes_arena.truncate(label_bytes_base);
                            samples_arena.truncate(samples_base);
                            histograms_arena.truncate(hist_base);
                            exemplars_arena.truncate(ex_base);
                            for s_idx in 0..num_shards {
                                shard_buckets[s_idx].truncate(bucket_bases[s_idx]);
                            }
                            *total_series = total_series_base;
                        }
                    }
                    drop(rec_metadata);
                    wr_scratch.clear();
                    let _ = rec_offset_start;
                    continue;
                }

                let borrowed =
                    match deserialize_record_content(&rec.value, version, wr_scratch) {
                        Ok(b) => b,
                        Err(e) => {
                            let err_msg = format!("parsing ingest consumer write request: {e}");
                            error!(err = %err_msg, "failed to parse write request; skipping");
                            wr_scratch.clear();
                            continue;
                        }
                    };

                let mut rec_first_shard: Option<usize> = None;
                let source = borrowed.source;
                let skip_label_validation = borrowed.skip_label_validation;
                let ts_count = borrowed.timeseries_len();

                for ts in borrowed.timeseries() {
                    let label_bytes_start = label_bytes_arena.len();
                    let mut labels_sorted_unique = true;
                    let mut bad_utf8 = false;
                    let mut prev_name: Option<&str> = None;

                    if let Some(ts_labels_str) = ts.labels_str() {
                        label_bytes_arena.reserve(ts_labels_str.len() * 24);
                        for (k, v) in ts_labels_str {
                            if let Some(pn) = prev_name {
                                if pn >= k {
                                    labels_sorted_unique = false;
                                }
                            }
                            prev_name = Some(k);
                            if !v.is_empty() {
                                append_stringlabel_pair(
                                    label_bytes_arena,
                                    k.as_bytes(),
                                    v.as_bytes(),
                                );
                            }
                        }
                    } else {
                        let ts_labels = ts.labels();
                        label_bytes_arena.reserve(ts_labels.len() * 24);
                        for lp in ts_labels {
                            let Ok(k) = fast_utf8_str(lp.name) else {
                                bad_utf8 = true;
                                break;
                            };
                            let Ok(v) = fast_utf8_str(lp.value) else {
                                bad_utf8 = true;
                                break;
                            };
                            if let Some(pn) = prev_name {
                                if pn >= k {
                                    labels_sorted_unique = false;
                                }
                            }
                            prev_name = Some(k);
                            if !v.is_empty() {
                                append_stringlabel_pair(
                                    label_bytes_arena,
                                    k.as_bytes(),
                                    v.as_bytes(),
                                );
                            }
                        }
                    }

                    if bad_utf8 {
                        label_bytes_arena.truncate(label_bytes_start);
                    }
                    let label_bytes_end = label_bytes_arena.len();
                    let label_bytes_len = (label_bytes_end - label_bytes_start) as u32;
                    let prom_hash = if !bad_utf8 && labels_sorted_unique && label_bytes_len > 0 {
                        xxh64(&label_bytes_arena[label_bytes_start..label_bytes_end], 0)
                    } else {
                        0
                    };

                    let samples = ts.samples();
                    let samples_start = samples_arena.len() as u32;
                    let samples_len = samples.len() as u32;
                    let mut min_ts = i64::MAX;
                    for s in samples {
                        min_ts = min_ts.min(s.timestamp_ms);
                    }
                    samples_arena.extend_from_slice(samples);

                    let hist_start = histograms_arena.len() as u32;
                    let mut hist_len = 0u32;
                    for h in ts.histograms() {
                        let conv = convert_borrowed_histogram(&h);
                        min_ts = min_ts.min(conv.0);
                        histograms_arena.push(conv);
                        hist_len += 1;
                    }

                    let ex_start = exemplars_arena.len() as u32;
                    let mut ex_len = 0u32;
                    if ts.exemplars().len() > 0 {
                        tmp_ex.clear();
                        for ex in ts.exemplars() {
                            let res = convert_borrowed_exemplar(&ex);
                            if let Ok(ref conv) = res {
                                min_ts = min_ts.min(conv.0);
                            }
                            tmp_ex.push(res);
                            ex_len += 1;
                        }
                        exemplars_arena.append(&mut tmp_ex);
                    }

                    let shard = shard_for(tenant_idx, prom_hash);
                    rec_first_shard.get_or_insert(shard);
                    shard_buckets[shard].push(ShardSeriesItem {
                        worker_idx: worker_idx as u16,
                        rec_seq,
                        record_offset: rec.offset,
                        tenant_idx,
                        source,
                        skip_label_validation,
                        labels_sorted_unique,
                        bad_utf8,
                        prom_hash,
                        label_bytes_start: label_bytes_start as u32,
                        label_bytes_len,
                        samples_start,
                        samples_len,
                        created_timestamp: ts.created_timestamp(),
                        histograms_start: hist_start,
                        histograms_len: hist_len,
                        exemplars_start: ex_start,
                        exemplars_len: ex_len,
                        min_ts,
                    });
                    *total_series += 1;
                }

                if borrowed.metadata_len() > 0 {
                    let norm_source = cortexpb::write_request::SourceEnum::try_from(source)
                        .unwrap_or(cortexpb::write_request::SourceEnum::Api);
                    metadata.stash(
                        num_shards,
                        tenant_shard_spread[tenant_idx as usize],
                        ShardMetadataItem {
                            worker_idx: worker_idx as u16,
                            rec_seq,
                            tenant_idx,
                            source: norm_source,
                            record_offset: rec.offset,
                            metric_type: 0,
                            trace_id_idx: rec_trace_idx,
                            bounds: [0; 4],
                        },
                        rec_first_shard,
                        borrowed.metadata(),
                    );
                }

                let _ = rec_offset_start;
                let _ = ts_count;
                wr_scratch.clear();
            }
        };

        if num_workers == 1 {
            decode_worker(0, 0, records, &rec_tenant_idx, &mut pool_guard.arenas[0]);
        } else {
            let chunk_size = records.len().div_ceil(num_workers);
            let arenas_ptr = MutSlicePtr(pool_guard.arenas.as_mut_ptr());
            self.worker_pool.scoped_execute(num_workers, |w_idx| {
                let start = (w_idx * chunk_size).min(records.len());
                let end = (start + chunk_size).min(records.len());
                // SAFETY: `w_idx < num_workers == pool_guard.arenas.len()`, and each `w_idx`
                // executes at most once so `&mut arenas[w_idx]` is exclusive.
                let arena = unsafe { arenas_ptr.get(w_idx) };
                decode_worker(
                    w_idx,
                    start,
                    &records[start..end],
                    &rec_tenant_idx[start..end],
                    arena,
                );
            });
        }

        let mut total_series = 0usize;
        let mut total_metadata = 0usize;
        let mut first_tenant_key_err: Option<(u32, i64, u32)> = None;
        for a in &pool_guard.arenas {
            total_series += a.total_series;
            total_metadata += a.metadata.total;
            for &item in &a.tenant_key_errors {
                if first_tenant_key_err.is_none_or(|prev| item.0 < prev.0) {
                    first_tenant_key_err = Some(item);
                }
            }
        }

        if let Some((_, offset, tid_idx)) = first_tenant_key_err {
            let tid = &tenants[tid_idx as usize];
            let msg = if tid.is_empty() {
                "no org id"
            } else {
                tenant_invalid_errors[tid_idx as usize]
                    .as_deref()
                    .unwrap_or("invalid tenant id")
            };
            let err = PusherError {
                message: msg.to_string(),
                cause: None,
                grpc_code: Some("Internal".to_string()),
                log_policy: None,
            };
            let m = &self.metrics.storage_pusher_metrics;
            if !is_sequential {
                m.batch_age.observe(batch_started.elapsed().as_secs_f64());
                m.batching_queue_metrics.flush_total.inc();
                m.batching_queue_metrics.flush_errors_total.inc();
            }
            let _ = self.error_handler.is_server_error(tid, Some(&err));
            return Err(format!(
                "consuming record at offset {offset} for tenant {tid}: {err}"
            ));
        }

        if total_series == 0 && total_metadata == 0 {
            return Ok(());
        }

        // Phase B: parallel shard ingestion across `num_shards` threads.
        let arenas_ref: &[WorkerArena] = &pool_guard.arenas;
        let tenants_ref: &[String] = &tenants;
        let tenant_ids_ref: &[String] = &tenant_ids;

        let run_shard = |shard_idx: usize| -> Result<(), String> {
            let mut items_for_shard: Vec<ShardSeriesItem> = Vec::new();
            let mut metas_for_shard: Vec<ShardMetadataItem> = Vec::new();
            for arena in arenas_ref {
                let b = &arena.shard_buckets[shard_idx];
                if !b.is_empty() {
                    items_for_shard.extend_from_slice(b);
                }
                if let Some(mb) = arena.metadata.buckets.get(shard_idx) {
                    metas_for_shard.extend_from_slice(mb);
                }
            }
            if items_for_shard.is_empty() && metas_for_shard.is_empty() {
                return Ok(());
            }

            let norm_source_i32 = |raw: i32| -> i32 {
                cortexpb::write_request::SourceEnum::try_from(raw)
                    .map(|s| s as i32)
                    .unwrap_or(cortexpb::write_request::SourceEnum::Api as i32)
            };
            if !is_sequential {
                items_for_shard.sort_by_key(|it| (it.tenant_idx, norm_source_i32(it.source)));
                metas_for_shard.sort_by_key(|it| (it.tenant_idx, it.source as i32));
            }

            // One flush == one Go `Pusher.PushToStorageAndReleaseRequest` call: the series
            // batch plus (on the tenant's first flush in this shard) its metadata, all under a
            // single `startPushRequest` and `pushErrorHandler.IsServerError` accounting.
            let flush_batch = |tenant_idx: u32,
                                   source_raw: i32,
                                   min_ts: i64,
                                   last_off: i64,
                                   batch_vec: &mut Vec<PrehashedSeriesItem<'_>>,
                                   metas: &[ShardMetadataItem]|
             -> Result<(), String> {
                if batch_vec.is_empty() && metas.is_empty() {
                    return Ok(());
                }
                let tenant_key = &tenants_ref[tenant_idx as usize];
                let tenant_id = &tenant_ids_ref[tenant_idx as usize];
                let norm_source = cortexpb::write_request::SourceEnum::try_from(source_raw)
                    .unwrap_or(cortexpb::write_request::SourceEnum::Api);
                let m = &self.metrics.storage_pusher_metrics;
                let processing_hist = match (batch_vec.is_empty(), metas.is_empty()) {
                    (false, false) => self
                        .processing_time_timeseries_and_metadata
                        .get_or_init(|| {
                            m.processing_time
                                .with_label_values(&["timeseries_and_metadata"])
                        }),
                    (false, true) => self
                        .processing_time_timeseries
                        .get_or_init(|| m.processing_time.with_label_values(&["timeseries"])),
                    _ => self
                        .processing_time_metadata
                        .get_or_init(|| m.processing_time.with_label_values(&["metadata"])),
                };
                let meta_views: SmallVec<[BorrowedMetadata<'_>; 8]> = metas
                    .iter()
                    .map(|it| arenas_ref[it.worker_idx as usize].metadata.view(it))
                    .collect();

                if !is_sequential {
                    m.batch_age.observe(batch_started.elapsed().as_secs_f64());
                    m.batching_queue_metrics.flush_total.inc();
                }
                m.time_series_per_flush.observe(batch_vec.len() as f64);

                let sampled_trace_id = metas.iter().find_map(|it| {
                    (it.trace_id_idx != u32::MAX)
                        .then(|| {
                            arenas_ref
                                .get(it.worker_idx as usize)
                                .and_then(|a| a.trace_ids.get(it.trace_id_idx as usize))
                                .map(String::as_str)
                        })
                        .flatten()
                });
                let proc_t0 = Instant::now();
                let res = self
                    .ingester
                    .ingest_prehashed_shard_batch_with_trace(
                        tenant_id,
                        norm_source,
                        min_ts,
                        batch_vec,
                        &meta_views,
                        sampled_trace_id,
                    )
                    .map_err(ingester_err_to_pusher_err);
                processing_hist.observe(proc_t0.elapsed().as_secs_f64());
                batch_vec.clear();

                match res {
                    Ok(()) => {
                        self.error_handler.is_server_error(tenant_key, None);
                        Ok(())
                    }
                    Err(err) => {
                        if self.error_handler.is_server_error(tenant_key, Some(&err)) {
                            if !is_sequential {
                                m.batching_queue_metrics.flush_errors_total.inc();
                            }
                            Err(format!(
                                "consuming record at offset {last_off} for tenant {tenant_key}: {err}"
                            ))
                        } else {
                            Ok(())
                        }
                    }
                }
            };

            let meta_key = |it: &ShardMetadataItem| -> (u32, i32) {
                if is_sequential {
                    (it.rec_seq, 0)
                } else {
                    (it.tenant_idx, it.source as i32)
                }
            };
            // End (exclusive) of the run of metadata items for `key` starting at `pos`.
            let meta_run_end = |pos: usize, key: (u32, i32)| -> usize {
                metas_for_shard[pos..]
                    .iter()
                    .position(|it| meta_key(it) != key)
                    .map_or(metas_for_shard.len(), |n| pos + n)
            };
            // Metadata-only flushes for keys (sorted) before `upto` without series here.
            let flush_metadata_only_before =
                |upto: (u32, i32), pos: &mut usize| -> Result<(), String> {
                    while let Some(first) = metas_for_shard.get(*pos) {
                        let key = meta_key(first);
                        if key >= upto {
                            break;
                        }
                        let end = meta_run_end(*pos, key);
                        let last_off = metas_for_shard[end - 1].record_offset;
                        flush_batch(
                            first.tenant_idx,
                            first.source as i32,
                            i64::MAX,
                            last_off,
                            &mut Vec::new(),
                            &metas_for_shard[*pos..end],
                        )?;
                        *pos = end;
                    }
                    Ok(())
                };
            let mut meta_pos = 0usize;

            let Some(first_item) = items_for_shard.first() else {
                return flush_metadata_only_before((u32::MAX, i32::MAX), &mut meta_pos);
            };
            let mut batch: Vec<PrehashedSeriesItem<'_>> =
                Vec::with_capacity(items_for_shard.len().min(max_batch_series));
            let mut batch_min_ts = i64::MAX;
            let mut cur_tenant_idx = first_item.tenant_idx;
            let mut cur_rec_seq = first_item.rec_seq;
            let mut cur_source = first_item.source;
            let mut last_offset = first_item.record_offset;

            for item in &items_for_shard {
                let norm_item_src = norm_source_i32(item.source);
                let norm_cur_src = norm_source_i32(cur_source);

                let flush_needed = if is_sequential {
                    item.rec_seq != cur_rec_seq
                } else {
                    item.tenant_idx != cur_tenant_idx
                        || norm_item_src != norm_cur_src
                        || (!batch.is_empty() && batch.len() >= max_batch_series)
                };

                if flush_needed {
                    let cur_key = if is_sequential {
                        (cur_rec_seq, 0)
                    } else {
                        (cur_tenant_idx, norm_cur_src)
                    };
                    flush_metadata_only_before(cur_key, &mut meta_pos)?;
                    let meta_end = meta_run_end(meta_pos, cur_key);
                    flush_batch(
                        cur_tenant_idx,
                        norm_cur_src,
                        batch_min_ts,
                        last_offset,
                        &mut batch,
                        &metas_for_shard[meta_pos..meta_end],
                    )?;
                    meta_pos = meta_end;
                    batch_min_ts = i64::MAX;
                    cur_tenant_idx = item.tenant_idx;
                    cur_rec_seq = item.rec_seq;
                    cur_source = item.source;
                }

                let wa = &arenas_ref[item.worker_idx as usize];
                let encoded_labels =
                    wa.label_bytes_slice(item.label_bytes_start, item.label_bytes_len);
                let s_start = item.samples_start as usize;
                let s_end = s_start + item.samples_len as usize;
                let samples = &wa.samples_arena[s_start..s_end];

                let histograms = if item.histograms_len == 0 {
                    &[][..]
                } else {
                    let h_start = item.histograms_start as usize;
                    &wa.histograms_arena[h_start..h_start + item.histograms_len as usize]
                };
                let exemplars = if item.exemplars_len == 0 {
                    &[][..]
                } else {
                    let e_start = item.exemplars_start as usize;
                    &wa.exemplars_arena[e_start..e_start + item.exemplars_len as usize]
                };

                batch.push(PrehashedSeriesItem {
                    encoded_labels,
                    samples,
                    created_timestamp: item.created_timestamp,
                    prom_hash: item.prom_hash,
                    labels_sorted_unique: item.labels_sorted_unique,
                    bad_utf8: item.bad_utf8,
                    histograms,
                    exemplars,
                });
                batch_min_ts = batch_min_ts.min(item.min_ts);
                last_offset = item.record_offset;
            }

            let norm_cur_src = norm_source_i32(cur_source);
            let cur_key = if is_sequential {
                (cur_rec_seq, 0)
            } else {
                (cur_tenant_idx, norm_cur_src)
            };
            flush_metadata_only_before(cur_key, &mut meta_pos)?;
            let meta_end = meta_run_end(meta_pos, cur_key);
            flush_batch(
                cur_tenant_idx,
                norm_cur_src,
                batch_min_ts,
                last_offset,
                &mut batch,
                &metas_for_shard[meta_pos..meta_end],
            )?;
            meta_pos = meta_end;
            flush_metadata_only_before((u32::MAX, i32::MAX), &mut meta_pos)
        };

        let mut first_err: Option<String> = None;
        if num_shards == 1 || total_series < 64 {
            for s in 0..num_shards {
                if let Err(e) = run_shard(s) {
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                    break;
                }
            }
        } else {
            let shard_errs: SmallVec<[Mutex<Option<String>>; 16]> =
                (0..num_shards).map(|_| Mutex::new(None)).collect();
            self.worker_pool.scoped_execute(num_shards, |s| {
                if let Err(e) = run_shard(s) {
                    *shard_errs[s].lock() = Some(e);
                }
            });
            for slot in shard_errs {
                if let Some(e) = slot.into_inner() {
                    first_err = Some(e);
                    break;
                }
            }
        }

        if !is_sequential {
            let mut pushers: HashSet<(u32, i32)> = HashSet::new();
            for a in arenas_ref {
                for b in &a.shard_buckets {
                    for it in b {
                        let norm_src = cortexpb::write_request::SourceEnum::try_from(it.source)
                            .map(|s| s as i32)
                            .unwrap_or(cortexpb::write_request::SourceEnum::Api as i32);
                        pushers.insert((it.tenant_idx, norm_src));
                    }
                }
                for b in &a.metadata.buckets {
                    for it in b {
                        pushers.insert((it.tenant_idx, it.source as i32));
                    }
                }
            }
            let active_shards: usize = pushers
                .iter()
                .map(|(t, _)| tenant_shard_spread.get(*t as usize).copied().unwrap_or(1))
                .sum();
            self.metrics
                .storage_pusher_metrics
                .shards_per_push
                .observe(active_shards as f64);
            self.metrics
                .storage_pusher_metrics
                .pushers_per_push
                .observe(pushers.len() as f64);
        }

        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

struct ShardedConsumerHandle(Arc<IngesterShardedBatchConsumer>);

impl RecordBatchConsumer for ShardedConsumerHandle {
    fn consume(
        &self,
        records: Vec<FetchedRecord>,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>> {
        let this = Arc::clone(&self.0);
        Box::pin(async move {
            let start = Instant::now();
            let t = Arc::clone(&this);
            let res = tokio::task::spawn_blocking(move || t.consume_records_sync(&records))
                .await
                .map_err(|e| format!("ingest consumer task join error: {e}"))?;
            this.metrics
                .processing_time_seconds
                .observe(start.elapsed().as_secs_f64());
            res
        })
    }
}

pub async fn start_ingest_reader(ingester: Arc<Ingester>) -> Result<Arc<IngestReaderHandle>, ReaderError> {
    let tsdb_dir = PathBuf::from(&ingester.blocks_cfg.tsdb.dir);
    let adapter = Arc::new(IngesterStorageAdapter(Arc::clone(&ingester)));
    let notifier: Arc<dyn PreCommitNotifier> = adapter;

    // Go `NewPusherConsumer(pusher, kafkaCfg, NewPusherConsumerMetrics(reg), ...)`.
    let consumer_factory = |reg: Option<&prometheus::Registry>| -> Arc<dyn RecordBatchConsumer> {
        Arc::new(ShardedConsumerHandle(Arc::new(IngesterShardedBatchConsumer::new(
            Arc::clone(&ingester),
            ingester.ingest_cfg.kafka.clone(),
            PusherConsumerMetrics::new(reg),
        ))))
    };
    let max_replay_period = Duration::from_nanos(
        ingester
            .blocks_cfg
            .tsdb
            .retention_period
            .as_nanos()
            .max(0) as u64,
    );

    let compartments = ingester.compartments_config();
    let reader = if compartments.enabled {
        let rc_id = ingester.cfg.read_compartment_id.max(0) as usize;
        let topic = ingester
            .ingest_cfg
            .kafka
            .topic
            .replace(READ_COMPARTMENT_ID_PLACEHOLDER, &rc_id.to_string());
        let num_wc = compartments.write.num_compartments.max(1) as usize;
        let cluster_configs =
            write_compartment_configs(&ingester.ingest_cfg.kafka, num_wc, &topic);
        let offset_paths = (0..cluster_configs.len())
            .map(|w| Some(tsdb_dir.join(format!("kafka-offset-wc-{w}.json"))))
            .collect();
        let multi = MultiClusterPartitionReader::new_with_notifier(
            &cluster_configs,
            &ingester.ingest_cfg.ordered_consumption,
            ingester.ingest_partition_id,
            &ingester.cfg.ring.instance_id,
            offset_paths,
            &consumer_factory,
            notifier,
            max_replay_period,
            ingester.registry.as_ref(),
        )?;
        Arc::new(IngestReaderHandle::Multi(Arc::new(multi)))
    } else {
        let offset_file_path = tsdb_dir.join("kafka-offset.json");
        let mut single = SingleClusterPartitionReader::new_with_notifier(
            ingester.ingest_cfg.kafka.clone(),
            ingester.ingest_partition_id,
            &ingester.cfg.ring.instance_id,
            Some(offset_file_path),
            consumer_factory(ingester.registry.as_ref()),
            notifier,
            ingester.registry.as_ref(),
        )?;
        if !max_replay_period.is_zero() {
            single = single.with_max_replay_period(max_replay_period);
        }
        Arc::new(IngestReaderHandle::Single(Arc::new(single)))
    };

    // Store handle before awaiting startup catchup so stop() and push readiness checks see it.
    *ingester.ingest_reader.write() = Some(Arc::clone(&reader));
    if let Err(e) = reader.start().await {
        reader.stop().await;
        *ingester.ingest_reader.write() = None;
        return Err(e);
    }

    info!(
        partition = ingester.ingest_partition_id,
        topic = %ingester.ingest_cfg.kafka.topic,
        compartments_enabled = compartments.enabled,
        "ingest storage partition reader started"
    );
    Ok(reader)
}

/// Delegate resetting the read reactive limiter when this ingester's partition transitions to ACTIVE.
pub struct IngesterPartitionRingDelegate {
    ingester: Arc<Ingester>,
}

impl IngesterPartitionRingDelegate {
    pub fn new(ingester: Arc<Ingester>) -> Arc<Self> {
        Arc::new(Self { ingester })
    }
}

impl PartitionRingWatcherDelegate for IngesterPartitionRingDelegate {
    fn on_partition_ring_changed(&self, old: &PartitionRingDesc, new: &PartitionRingDesc) {
        let pid = self.ingester.ingest_partition_id;
        if let (Some(op), Some(np)) = (old.partitions.get(&pid), new.partitions.get(&pid)) {
            if op.state != np.state
                && np.state == mimir_proto::ringpb::PartitionState::PartitionActive as i32
            {
                if let Some(ref rl) = self.ingester.reactive_limiter.read {
                    rl.reset();
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mimir_common::services::State;

    #[test]
    fn error_mapping_distinguishes_client_and_server_errors() {
        let bad_data = IngesterError::sample_out_of_order(100, "up{job=\"a\"}".into());
        let bad_pusher = ingester_err_to_pusher_err(bad_data);
        assert!(bad_pusher.is_client_error());

        let tenant_limit = IngesterError::per_user_series_limit_reached(100);
        let limit_pusher = ingester_err_to_pusher_err(tenant_limit);
        assert!(limit_pusher.is_client_error());

        let tsdb_unavail = IngesterError::tsdb_closing();
        assert!(!ingester_err_to_pusher_err(tsdb_unavail).is_client_error());

        let unavail = IngesterError::Unavailable {
            state: State::Starting,
        };
        assert!(!ingester_err_to_pusher_err(unavail).is_client_error());

        let inst_limit = IngesterError::max_in_memory_series_reached();
        let inst_pusher = ingester_err_to_pusher_err(inst_limit);
        assert!(!inst_pusher.is_client_error());
        assert_eq!(inst_pusher.log_policy, Some(LogPolicy::DoNotLog));
    }

    #[test]
    fn batch_consumer_rejects_path_traversal_tenant_and_starting_bypasses_max_series() {
        let tmp = tempfile::tempdir().unwrap();
        let mut cfg = mimir_config::IngesterConfig::default();
        cfg.ring.instance_id = "test-0".to_string();
        cfg.instance_limits.max_series = 1;
        let mut blocks_cfg = mimir_config::BlocksStorageConfig::default();
        blocks_cfg.tsdb.dir = tmp.path().to_string_lossy().into_owned();
        let ingest_cfg = mimir_config::IngestStorageConfig::default();
        let overrides = mimir_config::Overrides::new(mimir_config::LimitsConfig::default());

        let ing = Ingester::new(
            cfg,
            blocks_cfg,
            ingest_cfg.clone(),
            overrides,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();

        // While Starting, get_instance_limits() returns None, so max_series=1 is bypassed.
        ing.set_state_for_test(State::Starting);
        assert!(ing.get_instance_limits().is_none());
        let db = ing.get_or_create_tsdb("tenant-a").unwrap();
        {
            let mut app = db.db.appender();
            app.append_float(None, &[("__name__", "m1")], 1000, 1.0).unwrap();
            app.append_float(None, &[("__name__", "m2")], 1000, 2.0).unwrap();
            app.commit().unwrap();
        }
        assert_eq!(db.db.head().num_series(), 2);

        // Once Running, max_series=1 is enforced.
        ing.set_state_for_test(State::Running);
        assert_eq!(ing.get_instance_limits().unwrap().max_series, 1);
        {
            let mut app = db.db.appender();
            assert!(app.append_float(None, &[("__name__", "m3")], 1000, 3.0).is_err());
        }

        // H6: path-traversal tenant ID in Kafka record key is rejected and never creates TSDB dir.
        let reg = prometheus::Registry::new();
        let pusher_metrics = PusherConsumerMetrics::new(Some(&reg));
        let consumer =
            IngesterShardedBatchConsumer::new(Arc::clone(&ing), ingest_cfg.kafka, pusher_metrics);
        use mimir_proto::prost::Message as _;
        let wr = cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![cortexpb::LabelPair::from_static("__name__", "m_probe")],
                samples: vec![cortexpb::Sample {
                    value: 1.0,
                    timestamp_ms: 1000,
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let rec = FetchedRecord {
            key: bytes::Bytes::from_static(b"../mimir_traversal_probe"),
            value: bytes::Bytes::from(wr.encode_to_vec()),
            headers: vec![],
            offset: 1,
            timestamp_ms: 0,
        };
        assert!(consumer.consume_records_sync(&[rec]).is_err());
        assert!(ing.get_tsdb("../mimir_traversal_probe").is_none());
        assert!(!tmp.path().join("../mimir_traversal_probe").exists());
        assert_eq!(
            consumer.metrics.storage_pusher_metrics.server_err_requests.get(),
            1.0
        );
        assert_eq!(
            consumer.metrics.storage_pusher_metrics.client_err_requests.get(),
            0.0
        );
    }
}

#[cfg(test)]
mod consumer_tests {
    use super::*;
    use mimir_common::services::State;
    use mimir_storage::ingest::{RecordSerializer, RecordVersion};

    fn running_ingester(dir: &std::path::Path) -> Arc<Ingester> {
        let mut cfg = mimir_config::IngesterConfig::default();
        cfg.ring.instance_id = "test-0".to_string();
        let mut blocks_cfg = mimir_config::BlocksStorageConfig::default();
        blocks_cfg.tsdb.dir = dir.to_string_lossy().into_owned();
        let overrides = mimir_config::Overrides::new(mimir_config::LimitsConfig::default());
        let ing = Ingester::new(
            cfg,
            blocks_cfg,
            mimir_config::IngestStorageConfig::default(),
            overrides,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        ing.set_state_for_test(State::Running);
        ing
    }

    fn consumer(ing: &Arc<Ingester>, reg: &prometheus::Registry) -> IngesterShardedBatchConsumer {
        IngesterShardedBatchConsumer::new(
            Arc::clone(ing),
            mimir_config::IngestStorageConfig::default().kafka,
            PusherConsumerMetrics::new(Some(reg)),
        )
    }

    fn now_ms() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    }

    fn series(name: &str) -> cortexpb::TimeSeries {
        cortexpb::TimeSeries {
            labels: vec![cortexpb::LabelPair {
                name: "__name__".into(),
                value: bytes::Bytes::copy_from_slice(name.as_bytes()),
            }],
            samples: vec![cortexpb::Sample {
                timestamp_ms: now_ms(),
                value: 1.0,
            }],
            ..Default::default()
        }
    }

    fn metadata(name: &str) -> cortexpb::MetricMetadata {
        cortexpb::MetricMetadata {
            r#type: cortexpb::metric_metadata::MetricType::Counter as i32,
            metric_family_name: name.into(),
            help: "help".into(),
            unit: String::new(),
        }
    }

    fn records(
        version: RecordVersion,
        key: &str,
        req: &cortexpb::WriteRequest,
        first_offset: i64,
    ) -> Vec<FetchedRecord> {
        let (recs, _) = RecordSerializer::new(version)
            .to_records("t", 0, key, req, 16 << 20)
            .unwrap();
        recs.into_iter()
            .enumerate()
            .map(|(i, r)| FetchedRecord {
                key: r.key,
                value: r.value,
                headers: r.headers,
                offset: first_offset + i as i64,
                timestamp_ms: 0,
            })
            .collect()
    }

    fn processing_count(reg: &prometheus::Registry, contents: &str) -> u64 {
        reg.gather()
            .iter()
            .find(|mf| mf.get_name() == "cortex_ingest_storage_reader_pusher_processing_time_seconds")
            .and_then(|mf| {
                mf.get_metric()
                    .iter()
                    .find(|m| m.get_label().iter().any(|l| l.get_value() == contents))
                    .map(|m| m.get_histogram().get_sample_count())
            })
            .unwrap_or(0)
    }

    /// W2-02: dskit `tenant.TenantID` strips `:key=value` metadata from the Kafka record key.
    #[test]
    fn record_key_with_tenant_metadata_is_normalized() {
        for version in [RecordVersion::V1, RecordVersion::V2] {
            let tmp = tempfile::tempdir().unwrap();
            let ing = running_ingester(tmp.path());
            let reg = prometheus::Registry::new();
            let c = consumer(&ing, &reg);
            let req = cortexpb::WriteRequest {
                timeseries: vec![series("m1")],
                ..Default::default()
            };
            let mut recs = records(version, "123456:product=k6", &req, 1);
            recs.extend(records(version, "a|a:k=v", &req, 2));
            c.consume_records_sync(&recs).unwrap();

            assert_eq!(ing.get_tsdb("123456").unwrap().db.head().num_series(), 1);
            assert!(ing.get_tsdb("123456:product=k6").is_none());
            assert_eq!(ing.get_tsdb("a").unwrap().db.head().num_series(), 1);

            let bad_recs = records(version, "x|y", &req, 3);
            assert!(
                c.consume_records_sync(&bad_recs).is_err(),
                "x|y (ErrTooManyOrgIDs) must fail Consume as a server error (WLL-02)"
            );
            assert!(ing.get_tsdb("x").is_none() && ing.get_tsdb("x|y").is_none());
            let m = &c.metrics.storage_pusher_metrics;
            assert_eq!(m.server_err_requests.get(), 1.0);
            assert_eq!(m.client_err_requests.get(), 0.0);
            assert_eq!(m.total_requests.get(), 3.0);
        }
    }

    /// W2-11: metadata goes through the pusher accounting like Go `requestContents`.
    #[test]
    fn metadata_records_are_accounted_as_pusher_requests() {
        for version in [RecordVersion::V1, RecordVersion::V2] {
            let tmp = tempfile::tempdir().unwrap();
            let ing = running_ingester(tmp.path());
            let reg = prometheus::Registry::new();
            let c = consumer(&ing, &reg);

            // Go decodes RW2 metadata as attached to a (sample-less) series, so a v2
            // metadata-only record still reaches the pusher as `timeseries_and_metadata`.
            let v2 = version == RecordVersion::V2;
            let meta_entries = vec![metadata("m_meta"), metadata("m_meta_2")];
            let num_meta = meta_entries.len();
            let meta_only = cortexpb::WriteRequest {
                metadata: meta_entries,
                ..Default::default()
            };
            c.consume_records_sync(&records(version, "tenant-a", &meta_only, 1))
                .unwrap();
            let m = &c.metrics.storage_pusher_metrics;
            // All entries in one request (Go: a single ideal shard for a small tenant).
            assert_eq!(m.total_requests.get(), 1.0);
            let meta_only_contents = if v2 { "timeseries_and_metadata" } else { "metadata" };
            assert_eq!(processing_count(&reg, meta_only_contents), 1);
            if !v2 {
                assert!(ing.get_tsdb("tenant-a").is_none(), "metadata-only must not create a TSDB");
            }
            let stored = ing
                .get_user_metadata("tenant-a")
                .unwrap()
                .query(&[], "", -1, -1);
            assert_eq!(stored.len(), num_meta);
            assert!(stored.iter().any(|m| m.metric_family_name == "m_meta"));

            let mixed = cortexpb::WriteRequest {
                timeseries: vec![series("m1")],
                metadata: vec![metadata("m1")],
                ..Default::default()
            };
            c.consume_records_sync(&records(version, "tenant-a", &mixed, 2))
                .unwrap();
            assert_eq!(m.total_requests.get(), 2.0);
            assert_eq!(processing_count(&reg, "timeseries_and_metadata"), 1 + u64::from(v2));
            assert_eq!(processing_count(&reg, "timeseries"), 0);
            assert_eq!(
                ing.get_user_metadata("tenant-a")
                    .unwrap()
                    .query(&[], "", -1, -1)
                    .len(),
                num_meta + 1
            );
            assert_eq!(ing.get_tsdb("tenant-a").unwrap().db.head().num_series(), 1);
        }
    }

    /// W2-11: metadata-only records pass through `startPushRequest` (rejected when not running).
    #[test]
    fn metadata_only_record_goes_through_start_push_request() {
        let tmp = tempfile::tempdir().unwrap();
        let ing = running_ingester(tmp.path());
        ing.set_state_for_test(State::Stopping);
        let reg = prometheus::Registry::new();
        let c = consumer(&ing, &reg);
        let meta_only = cortexpb::WriteRequest {
            metadata: vec![metadata("m_meta")],
            ..Default::default()
        };
        let err = c
            .consume_records_sync(&records(RecordVersion::V1, "tenant-a", &meta_only, 7))
            .unwrap_err();
        assert!(err.contains("consuming record at offset 7 for tenant tenant-a"), "{err}");
        assert_eq!(c.metrics.storage_pusher_metrics.server_err_requests.get(), 1.0);
        assert!(ing.get_user_metadata("tenant-a").is_none());
    }
}
