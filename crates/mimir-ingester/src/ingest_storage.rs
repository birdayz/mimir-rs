//! Kafka ingest_storage integration for the ingester.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/ingester/ingester.go (ingest_storage init)
//! - github.com/grafana/mimir/pkg/storage/ingest/util.go (IngesterPartitionID)

use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
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
                if offsets.num_kafka_clusters() != 1 {
                    return Err(ReaderError::ClusterCountMismatch {
                        expected: 1,
                        got: offsets.num_kafka_clusters(),
                    });
                }
                r.wait_for_offset(offsets.for_kafka_cluster(0), timeout)
                    .await?;
                Ok(())
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
        | ErrorCause::CircuitBreakerOpen => "Unavailable",
        ErrorCause::MethodNotAllowed => "Unimplemented",
        _ => "Internal",
    };
    let log_policy = if err.is_do_not_log() {
        Some(LogPolicy::DoNotLog)
    } else if err.is_optional_logging() {
        Some(LogPolicy::Sampled {
            should_log: err.should_log_error(),
            reason: String::new(),
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
        Box::pin(async move { self.0.notify_pre_commit().map_err(|e| e.to_string()) })
    }
}

const SHARD_COMMIT_MAX_SAMPLES: usize = 2048;
const SHARD_COMMIT_MAX_SERIES: usize = 2048;

#[derive(Clone, Copy)]
struct ShardSeriesItem {
    worker_idx: u16,
    record_offset: i64,
    tenant_idx: u16,
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
    histograms_len: u16,
    exemplars_start: u32,
    exemplars_len: u16,
    min_ts: i64,
}

struct WorkerArena {
    wr_scratch: WriteRequestScratch,
    rw2_resolved: Rw2ResolvedScratch,
    label_bytes_arena: Vec<u8>,
    samples_arena: Vec<cortexpb::Sample>,
    histograms_arena: Vec<ConvertedHistogram>,
    exemplars_arena: Vec<(i64, f64, Labels, String)>,
    shard_buckets: Vec<Vec<ShardSeriesItem>>,
    empty_tenant_errors: usize,
    total_series: usize,
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
            empty_tenant_errors: 0,
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
        self.empty_tenant_errors = 0;
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
            }
            // Prevent retaining outlier allocations after massive spikes.
            if a.label_bytes_arena.capacity() > 4 * 1024 * 1024 {
                a.label_bytes_arena = Vec::with_capacity(65_536);
            }
            if a.samples_arena.capacity() > 65_536 {
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
/// in batches of up to 2,048 samples per `HeadAppender::commit`.
pub struct IngesterShardedBatchConsumer {
    ingester: Arc<Ingester>,
    kafka_cfg: KafkaConfig,
    metrics: PusherConsumerMetrics,
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

    fn consume_records_sync(&self, records: &[FetchedRecord]) -> Result<(), String> {
        if records.is_empty() {
            return Ok(());
        }

        // Map each record to a deduplicated tenant index + accumulate bytes per tenant for metrics.
        let mut tenants: SmallVec<[String; 4]> = SmallVec::new();
        let mut tenant_bytes: SmallVec<[usize; 4]> = SmallVec::new();
        let mut tenant_lookup: HashMap<&str, u16> = HashMap::new();
        let mut rec_tenant_idx: SmallVec<[u16; 64]> = SmallVec::with_capacity(records.len());

        for rec in records {
            let tid_cow = String::from_utf8_lossy(&rec.key);
            let tid_idx = if let Some(&idx) = tenant_lookup.get(tid_cow.as_ref()) {
                tenant_bytes[idx as usize] += rec.value.len();
                idx
            } else {
                let idx = tenants.len() as u16;
                tenants.push(tid_cow.into_owned());
                tenant_bytes.push(rec.value.len());
                // Safety/simplicity: borrow from owned string? Rebuild small map or linear scan for <= 4 tenants.
                idx
            };
            if tenants.len() > tenant_lookup.len() && tenants.len() > 4 {
                tenant_lookup.clear();
                for (i, t) in tenants.iter().enumerate() {
                    // Use unsafe lifetime extension? Avoid: linear scan up to 8 tenants is faster than HashMap!
                    let _ = (i, t);
                }
            }
            rec_tenant_idx.push(tid_idx);
        }
        // Actually replace linear scan cleanly without unused map:
        let _ = tenant_lookup;
        tenants.clear();
        tenant_bytes.clear();
        rec_tenant_idx.clear();
        let mut fallback_map: Option<HashMap<String, u16>> = None;
        for rec in records {
            let tid_cow = String::from_utf8_lossy(&rec.key);
            let tid_str = tid_cow.as_ref();
            let tid_idx = if let Some(ref mut map) = fallback_map {
                if let Some(&idx) = map.get(tid_str) {
                    tenant_bytes[idx as usize] += rec.value.len();
                    idx
                } else {
                    let idx = tenants.len() as u16;
                    tenants.push(tid_str.to_owned());
                    tenant_bytes.push(rec.value.len());
                    map.insert(tid_str.to_owned(), idx);
                    idx
                }
            } else if let Some(pos) = tenants.iter().position(|t| t == tid_str) {
                tenant_bytes[pos] += rec.value.len();
                pos as u16
            } else if tenants.len() < 8 {
                let idx = tenants.len() as u16;
                tenants.push(tid_str.to_owned());
                tenant_bytes.push(rec.value.len());
                idx
            } else {
                let mut map = HashMap::with_capacity(16);
                for (i, t) in tenants.iter().enumerate() {
                    map.insert(t.clone(), i as u16);
                }
                let idx = tenants.len() as u16;
                tenants.push(tid_str.to_owned());
                tenant_bytes.push(rec.value.len());
                map.insert(tid_str.to_owned(), idx);
                fallback_map = Some(map);
                idx
            };
            rec_tenant_idx.push(tid_idx);
        }

        let num_shards = if self.kafka_cfg.ingestion_concurrency_max <= 0 {
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
                             tids: &[u16],
                             arena: &mut WorkerArena| {
            let WorkerArena {
                wr_scratch,
                rw2_resolved,
                label_bytes_arena,
                samples_arena,
                histograms_arena,
                exemplars_arena,
                shard_buckets,
                empty_tenant_errors,
                total_series,
            } = arena;
            let mut tmp_ex: Vec<(i64, f64, Labels, String)> = Vec::new();
            let mut rec_metadata: Vec<BorrowedMetadata<'_>> = Vec::with_capacity(128);
            let mut bucket_bases = [0usize; 16];
            for (local_i, rec) in slice.iter().enumerate() {
                let tenant_idx = tids[local_i];
                let tenant_id = &tenants[tenant_idx as usize];
                if tenant_id.is_empty() {
                    *empty_tenant_errors += 1;
                    continue;
                }

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
                            let prom_hash = if ts.labels_sorted_unique && label_bytes_len > 0 {
                                xxh64(ts.encoded_labels, 0)
                            } else {
                                0
                            };

                            let mut min_ts = ts.min_sample_ts;
                            let hist_start = histograms_arena.len() as u32;
                            let mut hist_len = 0u16;
                            if ts.histograms_len() > 0 {
                                for h in ts.histograms() {
                                    let conv = convert_borrowed_histogram(&h);
                                    min_ts = min_ts.min(conv.0);
                                    histograms_arena.push(conv);
                                    hist_len += 1;
                                }
                            }

                            let ex_start = exemplars_arena.len() as u32;
                            let mut ex_len = 0u16;
                            let mut bad_utf8 = false;
                            if ts.exemplars_len() > 0 {
                                tmp_ex.clear();
                                for ex in ts.exemplars() {
                                    if let Ok(conv) = convert_borrowed_exemplar(&ex) {
                                        tmp_ex.push(conv);
                                        ex_len += 1;
                                    } else {
                                        bad_utf8 = true;
                                        break;
                                    }
                                }
                                if !bad_utf8 {
                                    exemplars_arena.append(&mut tmp_ex);
                                }
                            }

                            let shard = (prom_hash as usize) & shard_mask;
                            shard_buckets[shard].push(ShardSeriesItem {
                                worker_idx: worker_idx as u16,
                                record_offset: rec.offset,
                                tenant_idx,
                                source: ts.source,
                                skip_label_validation: ts.skip_label_validation,
                                labels_sorted_unique: ts.labels_sorted_unique,
                                bad_utf8,
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
                                let meta_count = self
                                    .ingester
                                    .push_metadata_items(tenant_id, &rec_metadata);
                                if meta_count > 0 {
                                    self.ingester.ingestion_rate.add(meta_count as i64);
                                }
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
                    rec_metadata.clear();
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

                if borrowed.metadata_len() > 0 {
                    let meta_count = self.ingester.push_metadata_borrowed(tenant_id, &borrowed);
                    if meta_count > 0 {
                        self.ingester.ingestion_rate.add(meta_count as i64);
                    }
                }

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
                    let mut hist_len = 0u16;
                    for h in ts.histograms() {
                        let conv = convert_borrowed_histogram(&h);
                        min_ts = min_ts.min(conv.0);
                        histograms_arena.push(conv);
                        hist_len += 1;
                    }

                    let ex_start = exemplars_arena.len() as u32;
                    let mut ex_len = 0u16;
                    if ts.exemplars().len() > 0 {
                        tmp_ex.clear();
                        for ex in ts.exemplars() {
                            if let Ok(conv) = convert_borrowed_exemplar(&ex) {
                                tmp_ex.push(conv);
                                ex_len += 1;
                            } else {
                                bad_utf8 = true;
                                break;
                            }
                        }
                        if !bad_utf8 {
                            exemplars_arena.append(&mut tmp_ex);
                        }
                    }

                    let shard = (prom_hash as usize) & shard_mask;
                    shard_buckets[shard].push(ShardSeriesItem {
                        worker_idx: worker_idx as u16,
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
        for a in &pool_guard.arenas {
            total_series += a.total_series;
            for _ in 0..a.empty_tenant_errors {
                let err = PusherError::ingester(ErrorCause::BadData, "InvalidArgument", "no org id");
                let _ = self.error_handler.is_server_error("", Some(&err));
            }
        }

        if total_series == 0 {
            return Ok(());
        }

        // Phase B: parallel shard ingestion across `num_shards` threads.
        let arenas_ref: &[WorkerArena] = &pool_guard.arenas;
        let tenants_ref: &[String] = &tenants;

        let run_shard = |shard_idx: usize| -> Result<(), String> {
            let mut items_for_shard: Vec<ShardSeriesItem> = Vec::new();
            for arena in arenas_ref {
                let b = &arena.shard_buckets[shard_idx];
                if !b.is_empty() {
                    items_for_shard.extend_from_slice(b);
                }
            }
            if items_for_shard.is_empty() {
                return Ok(());
            }

            if tenants_ref.len() > 1 {
                items_for_shard.sort_by_key(|it| it.tenant_idx);
            }

            let mut batch: Vec<PrehashedSeriesItem<'_>> =
                Vec::with_capacity(items_for_shard.len().min(SHARD_COMMIT_MAX_SERIES));
            let mut batch_samples = 0usize;
            let mut batch_min_ts = i64::MAX;
            let mut cur_tenant_idx = items_for_shard[0].tenant_idx;
            let mut cur_source = items_for_shard[0].source;
            let mut last_offset = items_for_shard[0].record_offset;

            let flush_batch = |tenant_idx: u16,
                                   source_raw: i32,
                                   min_ts: i64,
                                   last_off: i64,
                                   batch_vec: &mut Vec<PrehashedSeriesItem<'_>>|
             -> Result<(), String> {
                if batch_vec.is_empty() {
                    return Ok(());
                }
                let tenant_id = &tenants_ref[tenant_idx as usize];
                let norm_source = cortexpb::write_request::SourceEnum::try_from(source_raw)
                    .unwrap_or(cortexpb::write_request::SourceEnum::Api);

                let m = &self.metrics.storage_pusher_metrics;
                m.batch_age.observe(batch_started.elapsed().as_secs_f64());
                m.batching_queue_metrics.flush_total.inc();
                m.time_series_per_flush.observe(batch_vec.len() as f64);

                let proc_t0 = Instant::now();
                let res = self
                    .ingester
                    .ingest_prehashed_shard_batch(tenant_id, norm_source, min_ts, batch_vec)
                    .map_err(ingester_err_to_pusher_err);
                m.processing_time
                    .with_label_values(&["timeseries"])
                    .observe(proc_t0.elapsed().as_secs_f64());
                batch_vec.clear();

                match res {
                    Ok(()) => {
                        self.error_handler.is_server_error(tenant_id, None);
                        Ok(())
                    }
                    Err(err) => {
                        if self.error_handler.is_server_error(tenant_id, Some(&err)) {
                            m.batching_queue_metrics.flush_errors_total.inc();
                            Err(format!(
                                "consuming record at offset {last_off} for tenant {tenant_id}: {err}"
                            ))
                        } else {
                            Ok(())
                        }
                    }
                }
            };

            for item in &items_for_shard {
                let norm_item_src = cortexpb::write_request::SourceEnum::try_from(item.source)
                    .map(|s| s as i32)
                    .unwrap_or(cortexpb::write_request::SourceEnum::Api as i32);
                let norm_cur_src = cortexpb::write_request::SourceEnum::try_from(cur_source)
                    .map(|s| s as i32)
                    .unwrap_or(cortexpb::write_request::SourceEnum::Api as i32);

                if item.tenant_idx != cur_tenant_idx
                    || norm_item_src != norm_cur_src
                    || (!batch.is_empty()
                        && (batch_samples + (item.samples_len as usize) > SHARD_COMMIT_MAX_SAMPLES
                            || batch.len() >= SHARD_COMMIT_MAX_SERIES))
                {
                    flush_batch(
                        cur_tenant_idx,
                        norm_cur_src,
                        batch_min_ts,
                        last_offset,
                        &mut batch,
                    )?;
                    batch_samples = 0;
                    batch_min_ts = i64::MAX;
                    cur_tenant_idx = item.tenant_idx;
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
                    skip_label_validation: item.skip_label_validation,
                    histograms,
                    exemplars,
                });
                batch_samples += item.samples_len as usize;
                batch_min_ts = batch_min_ts.min(item.min_ts);
                last_offset = item.record_offset;
            }

            let norm_cur_src = cortexpb::write_request::SourceEnum::try_from(cur_source)
                .map(|s| s as i32)
                .unwrap_or(cortexpb::write_request::SourceEnum::Api as i32);
            flush_batch(
                cur_tenant_idx,
                norm_cur_src,
                batch_min_ts,
                last_offset,
                &mut batch,
            )?;

            Ok(())
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

        self.metrics
            .storage_pusher_metrics
            .shards_per_push
            .observe(num_shards as f64);
        self.metrics
            .storage_pusher_metrics
            .pushers_per_push
            .observe(tenants.len() as f64);

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
            let res = match tokio::runtime::Handle::current().runtime_flavor() {
                tokio::runtime::RuntimeFlavor::MultiThread => {
                    tokio::task::block_in_place(|| this.consume_records_sync(&records))
                }
                tokio::runtime::RuntimeFlavor::CurrentThread => {
                    let t = Arc::clone(&this);
                    tokio::task::spawn_blocking(move || t.consume_records_sync(&records))
                        .await
                        .map_err(|e| format!("ingest consumer task join error: {e}"))?
                }
                _ => {
                    let t = Arc::clone(&this);
                    tokio::task::spawn_blocking(move || t.consume_records_sync(&records))
                        .await
                        .map_err(|e| format!("ingest consumer task join error: {e}"))?
                }
            };
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

    let pusher_metrics = PusherConsumerMetrics::new(ingester.registry.as_ref());
    let sharded_consumer = Arc::new(IngesterShardedBatchConsumer::new(
        Arc::clone(&ingester),
        ingester.ingest_cfg.kafka.clone(),
        pusher_metrics,
    ));
    let consumer: Arc<dyn RecordBatchConsumer> = Arc::new(ShardedConsumerHandle(sharded_consumer));
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
            consumer,
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
            consumer,
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
}
