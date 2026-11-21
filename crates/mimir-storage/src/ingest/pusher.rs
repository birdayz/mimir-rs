//! Kafka ingest_storage record consumer & two-tiered parallel storage pusher.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/storage/ingest/pusher.go
//! - github.com/grafana/mimir/pkg/storage/ingest/pusher_metrics.go

use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use mimir_config::KafkaConfig;
use mimir_proto::cortexpb::{self, is_client_error_cause, ErrorCause};
use mimir_proto::symbols::V2_COMMON_SYMBOLS_LIST;
use mimir_proto::zerocopy::{BorrowedWriteRequest, WriteRequestScratch};
use parking_lot::Mutex as SyncMutex;
use mimir_common::native_histogram::{
    HistogramOpts as NativeHistogramOpts, HistogramVec, SingleHistogram, DEF_BUCKETS,
};
use prometheus::{Counter, CounterVec, Opts, Registry};
use thiserror::Error;
use tokio::sync::{mpsc, watch, Mutex};
use tokio::task::JoinHandle;
use tracing::{error, warn};

use super::kafka_client::FetchedRecord;
use super::reader::RecordBatchConsumer;
use super::version::{deserialize_record_content, parse_record_version};

/// Standard xxHash64 (seed 0), matching `github.com/cespare/xxhash/v2.Sum64`.
fn xxh64_seed0(data: &[u8]) -> u64 {
    const P1: u64 = 0x9e3779b185ebca87;
    const P2: u64 = 0xc2b2ae3d27d4eb4f;
    const P3: u64 = 0x165667b19e3779f9;
    const P4: u64 = 0x85ebca77c2b2ae63;
    const P5: u64 = 0x27d4eb2f165667c5;

    let round = |acc: u64, input: u64| -> u64 {
        acc.wrapping_add(input.wrapping_mul(P2))
            .rotate_left(31)
            .wrapping_mul(P1)
    };
    let merge_round = |mut acc: u64, val: u64| -> u64 {
        acc ^= round(0, val);
        acc.wrapping_mul(P1).wrapping_add(P4)
    };
    let read64 = |b: &[u8]| u64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]);
    let read32 = |b: &[u8]| u32::from_le_bytes([b[0], b[1], b[2], b[3]]) as u64;

    let len = data.len();
    let mut i = 0usize;
    let mut h: u64 = if len >= 32 {
        let mut v1 = P1.wrapping_add(P2);
        let mut v2 = P2;
        let mut v3 = 0u64;
        let mut v4 = 0u64.wrapping_sub(P1);
        while i + 32 <= len {
            v1 = round(v1, read64(&data[i..]));
            v2 = round(v2, read64(&data[i + 8..]));
            v3 = round(v3, read64(&data[i + 16..]));
            v4 = round(v4, read64(&data[i + 24..]));
            i += 32;
        }
        let mut h = v1
            .rotate_left(1)
            .wrapping_add(v2.rotate_left(7))
            .wrapping_add(v3.rotate_left(12))
            .wrapping_add(v4.rotate_left(18));
        h = merge_round(h, v1);
        h = merge_round(h, v2);
        h = merge_round(h, v3);
        merge_round(h, v4)
    } else {
        P5
    };
    h = h.wrapping_add(len as u64);

    while i + 8 <= len {
        h ^= round(0, read64(&data[i..]));
        h = h.rotate_left(27).wrapping_mul(P1).wrapping_add(P4);
        i += 8;
    }
    if i + 4 <= len {
        h ^= read32(&data[i..]).wrapping_mul(P1);
        h = h.rotate_left(23).wrapping_mul(P2).wrapping_add(P3);
        i += 4;
    }
    while i < len {
        h ^= (data[i] as u64).wrapping_mul(P5);
        h = h.rotate_left(11).wrapping_mul(P1);
        i += 1;
    }
    h ^= h >> 33;
    h = h.wrapping_mul(P2);
    h ^= h >> 29;
    h = h.wrapping_mul(P3);
    h ^ (h >> 32)
}

static COMMON_SYMBOLS_BY_PTR: OnceLock<Vec<(usize, &'static [u8])>> = OnceLock::new();

fn common_symbols_table() -> &'static [(usize, &'static [u8])] {
    COMMON_SYMBOLS_BY_PTR.get_or_init(|| {
        let mut items: Vec<(usize, &'static [u8])> = V2_COMMON_SYMBOLS_LIST
            .iter()
            .map(|&s| {
                let b = s.as_bytes();
                (b.as_ptr() as usize, b)
            })
            .collect();
        items.sort_unstable_by_key(|&(p, _)| p);
        items
    })
}

thread_local! {
    static WR_SCRATCH: RefCell<WriteRequestScratch> = RefCell::new(WriteRequestScratch::new());
}

fn take_wr_scratch() -> WriteRequestScratch {
    match WR_SCRATCH.with(|c| c.try_borrow_mut().ok().map(|mut s| std::mem::take(&mut *s))) {
        Some(s) => s,
        None => WriteRequestScratch::new(),
    }
}

fn return_wr_scratch(mut scratch: WriteRequestScratch) {
    scratch.clear();
    WR_SCRATCH.with(|c| {
        if let Ok(mut slot) = c.try_borrow_mut() {
            *slot = scratch;
        }
    });
}

#[inline]
fn slice_to_bytes(payload: &Bytes, s: &[u8]) -> Bytes {
    if s.is_empty() {
        return Bytes::new();
    }
    let base = payload.as_ptr() as usize;
    let ptr = s.as_ptr() as usize;
    if ptr >= base && ptr + s.len() <= base + payload.len() {
        return payload.slice_ref(s);
    }
    let table = common_symbols_table();
    if let Ok(idx) = table.binary_search_by_key(&ptr, |&(p, _)| p) {
        let sym = table[idx].1;
        if s.len() <= sym.len() {
            return Bytes::from_static(&sym[..s.len()]);
        }
    }
    Bytes::copy_from_slice(s)
}

fn borrowed_write_request_to_owned_with_backing(
    borrowed: &BorrowedWriteRequest<'_, '_>,
    payload: &Bytes,
) -> cortexpb::WriteRequest {
    let mut timeseries = Vec::with_capacity(borrowed.timeseries_len());
    for ts in borrowed.timeseries() {
        let mut labels = Vec::with_capacity(ts.labels().len());
        for l in ts.labels() {
            labels.push(cortexpb::LabelPair {
                name: slice_to_bytes(payload, l.name),
                value: slice_to_bytes(payload, l.value),
            });
        }
        let samples = ts.samples().to_vec();
        let exemplars = if ts.exemplars().len() == 0 {
            Vec::new()
        } else {
            ts.exemplars()
                .map(|e| cortexpb::Exemplar {
                    labels: e
                        .labels()
                        .map(|l| cortexpb::LabelPair {
                            name: slice_to_bytes(payload, l.name),
                            value: slice_to_bytes(payload, l.value),
                        })
                        .collect(),
                    value: e.value(),
                    timestamp_ms: e.timestamp_ms(),
                })
                .collect()
        };
        let histograms = if ts.histograms().len() == 0 {
            Vec::new()
        } else {
            ts.histograms().map(|h| h.to_owned_histogram()).collect()
        };
        timeseries.push(cortexpb::TimeSeries {
            labels,
            samples,
            exemplars,
            histograms,
            created_timestamp: ts.created_timestamp(),
        });
    }

    let metadata = if borrowed.metadata_len() == 0 {
        Vec::new()
    } else {
        borrowed
            .metadata()
            .map(|m| cortexpb::MetricMetadata {
                r#type: m.metric_type,
                metric_family_name: m.metric_family_name.to_owned(),
                help: m.help.to_owned(),
                unit: m.unit.to_owned(),
            })
            .collect()
    };

    cortexpb::WriteRequest {
        timeseries,
        source: borrowed.source,
        metadata,
        symbols_rw2: Vec::new(),
        timeseries_rw2: Vec::new(),
        skip_label_validation: borrowed.skip_label_validation,
        skip_label_count_validation: borrowed.skip_label_count_validation,
    }
}

// ---------------------------------------------------------------------------
// PreCommitNotifier & Pusher Traits
// ---------------------------------------------------------------------------

pub trait PreCommitNotifier: Send + Sync + 'static {
    fn notify_pre_commit(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct NoOpPreCommitNotifier;

impl PreCommitNotifier for NoOpPreCommitNotifier {
    fn notify_pre_commit(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
}

/// Optional logging policy matching `dskit/middleware.OptionalLogging` and `util/log.Sampler`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogPolicy {
    Always,
    DoNotLog,
    Sampled {
        should_log: bool,
        reason: String,
    },
}

/// Rate-limited error sampler matching `pkg/util/log.Sampler`.
#[derive(Debug)]
pub struct ErrorSampler {
    sample_rate: i64,
    count: AtomicU64,
}

impl ErrorSampler {
    pub fn new(sample_rate: i64) -> Self {
        Self {
            sample_rate,
            count: AtomicU64::new(0),
        }
    }

    pub fn should_log(&self) -> (bool, String) {
        if self.sample_rate <= 0 {
            return (true, String::new());
        }
        let n = self.count.fetch_add(1, Ordering::Relaxed);
        let rate = self.sample_rate as u64;
        if n.is_multiple_of(rate) {
            (true, format!("sampled 1/{}", self.sample_rate))
        } else {
            (false, String::new())
        }
    }

    pub fn wrap_error(&self, mut err: PusherError) -> PusherError {
        let (should_log, reason) = self.should_log();
        err.log_policy = Some(LogPolicy::Sampled { should_log, reason });
        err
    }
}

#[derive(Debug, Clone, Error)]
pub struct PusherError {
    pub message: String,
    pub cause: Option<ErrorCause>,
    pub grpc_code: Option<String>,
    pub log_policy: Option<LogPolicy>,
}

impl fmt::Display for PusherError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(ref code) = self.grpc_code {
            write!(f, "rpc error: code = {code} desc = {}", self.message)
        } else {
            f.write_str(&self.message)
        }
    }
}

impl PusherError {
    pub fn server(msg: impl Into<String>) -> Self {
        Self {
            message: msg.into(),
            cause: None,
            grpc_code: None,
            log_policy: None,
        }
    }

    pub fn ingester(cause: ErrorCause, grpc_code: impl Into<String>, msg: impl Into<String>) -> Self {
        Self {
            message: msg.into(),
            cause: Some(cause),
            grpc_code: Some(grpc_code.into()),
            log_policy: None,
        }
    }

    pub fn with_do_not_log(mut self) -> Self {
        self.log_policy = Some(LogPolicy::DoNotLog);
        self
    }

    pub fn is_client_error(&self) -> bool {
        self.cause.is_some_and(is_client_error_cause)
    }
}

pub trait Pusher: PreCommitNotifier + Send + Sync + 'static {
    fn push_to_storage_and_release_request(
        &self,
        tenant_id: &str,
        request: cortexpb::WriteRequest,
    ) -> Pin<Box<dyn Future<Output = Result<(), PusherError>> + Send + '_>>;
}

// ---------------------------------------------------------------------------
// Metrics (ported from pusher_metrics.go)
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct BatchingQueueMetrics {
    pub flush_total: Counter,
    pub flush_errors_total: Counter,
}

impl BatchingQueueMetrics {
    pub fn new(reg: Option<&Registry>) -> Self {
        let flush_total = Counter::with_opts(Opts::new(
            "cortex_ingest_storage_reader_batching_queue_flush_total",
            "Number of times a batch of samples is flushed to the storage.",
        ))
        .expect("valid metric");
        let flush_errors_total = Counter::with_opts(Opts::new(
            "cortex_ingest_storage_reader_batching_queue_flush_errors_total",
            "Number of errors encountered while flushing a batch of samples to the storage.",
        ))
        .expect("valid metric");

        if let Some(r) = reg {
            let _ = r.register(Box::new(flush_total.clone()));
            let _ = r.register(Box::new(flush_errors_total.clone()));
        }

        Self {
            flush_total,
            flush_errors_total,
        }
    }
}

#[derive(Clone)]
pub struct StoragePusherMetrics {
    pub batch_age: SingleHistogram,
    pub processing_time: HistogramVec,
    pub time_series_per_flush: SingleHistogram,
    pub shards_per_push: SingleHistogram,
    pub pushers_per_push: SingleHistogram,
    pub batching_queue_metrics: BatchingQueueMetrics,
    pub client_err_requests: Counter,
    pub server_err_requests: Counter,
    pub total_requests: Counter,
    pub estimated_timeseries: Counter,
}

impl StoragePusherMetrics {
    pub fn new(reg: Option<&Registry>) -> Self {
        let err_requests_counter = CounterVec::new(
            Opts::new(
                "cortex_ingest_storage_reader_requests_failed_total",
                "Number of write requests which caused errors while processing. Client errors are errors such as tenant limits and samples out of bounds. Server errors indicate internal recoverable errors.",
            ),
            &["cause"],
        )
        .expect("valid metric");

        let batch_age = SingleHistogram::new(
            NativeHistogramOpts::new(
                "cortex_ingest_storage_reader_pusher_batch_age_seconds",
                "Age of the batch of samples that are being ingested by an ingestion shard. This is the time since adding the first sample to the batch. Higher values indicates that the batching queue is not processing fast enough or that the batches are not filling up fast enough.",
            )
            .native(1.1, 0, Duration::ZERO),
        )
        .expect("valid metric");

        let processing_time = HistogramVec::new(
            NativeHistogramOpts::new(
                "cortex_ingest_storage_reader_pusher_processing_time_seconds",
                "Time to ingest a batch of samples for timeseries or metadata by an ingestion shard. The 'batch_contents' label indicates the contents of the batch.",
            )
            .native(1.1, 0, Duration::ZERO),
            &["content"],
        )
        .expect("valid metric");

        let time_series_per_flush = SingleHistogram::new(
            NativeHistogramOpts::new(
                "cortex_ingest_storage_reader_pusher_timeseries_per_flush",
                "Number of time series pushed in each batch to an ingestion shard. A lower number than -ingest-storage.kafka.ingestion-concurrency-batch-size indicates that shards are not filling up and may not be parallelizing ingestion as efficiently.",
            )
            .native(1.1, 0, Duration::ZERO),
        )
        .expect("valid metric");

        let shards_per_push = SingleHistogram::new(
            NativeHistogramOpts::new(
                "cortex_ingest_storage_reader_shards_per_push",
                "Number of shards that are pushed to in each batch. There are one or more shards for each unique tenant and Source tuple.",
            )
            .native(1.1, 0, Duration::ZERO),
        )
        .expect("valid metric");

        let pushers_per_push = SingleHistogram::new(
            NativeHistogramOpts::new(
                "cortex_ingest_storage_reader_pushers_per_push",
                "Number of pushers that are pushed to in each batch. There is one pusher for each unique tenant and Source tuple.",
            )
            .native(1.1, 0, Duration::ZERO),
        )
        .expect("valid metric");

        let client_err_requests = err_requests_counter.with_label_values(&["client"]);
        let server_err_requests = err_requests_counter.with_label_values(&["server"]);

        let total_requests = Counter::with_opts(Opts::new(
            "cortex_ingest_storage_reader_requests_total",
            "Number of attempted write requests after batching records from Kafka.",
        ))
        .expect("valid metric");

        let estimated_timeseries = Counter::with_opts(Opts::new(
            "cortex_ingest_storage_reader_pusher_estimated_timeseries_total",
            "The estimated number of time series expected to be pushed to each shard. This is based on the decompressed size of records and is used to determine how many shards to use for each tenant for each batch. If the estimation is good, then it should match histogram_sum(cortex_ingest_storage_reader_pusher_timeseries_per_flush).",
        ))
        .expect("valid metric");

        if let Some(r) = reg {
            let _ = r.register(Box::new(err_requests_counter));
            let _ = r.register(Box::new(batch_age.clone()));
            let _ = r.register(Box::new(processing_time.clone()));
            let _ = r.register(Box::new(time_series_per_flush.clone()));
            let _ = r.register(Box::new(shards_per_push.clone()));
            let _ = r.register(Box::new(pushers_per_push.clone()));
            let _ = r.register(Box::new(total_requests.clone()));
            let _ = r.register(Box::new(estimated_timeseries.clone()));
        }

        Self {
            batch_age,
            processing_time,
            time_series_per_flush,
            shards_per_push,
            pushers_per_push,
            batching_queue_metrics: BatchingQueueMetrics::new(reg),
            client_err_requests,
            server_err_requests,
            total_requests,
            estimated_timeseries,
        }
    }
}

#[derive(Clone)]
pub struct PusherConsumerMetrics {
    pub processing_time_seconds: SingleHistogram,
    pub storage_pusher_metrics: StoragePusherMetrics,
}

impl PusherConsumerMetrics {
    pub fn new(reg: Option<&Registry>) -> Self {
        let processing_time_seconds = SingleHistogram::new(
            NativeHistogramOpts::new(
                "cortex_ingest_storage_reader_records_processing_time_seconds",
                "Time taken to process a batch of fetched records. Fetched records are effectively a set of WriteRequests read from Kafka. This is the latency of a single attempt and does not include retries.",
            )
            .buckets(DEF_BUCKETS.to_vec())
            .native(1.1, 100, Duration::from_secs(3600)),
        )
        .expect("valid metric");

        if let Some(r) = reg {
            let _ = r.register(Box::new(processing_time_seconds.clone()));
        }

        Self {
            processing_time_seconds,
            storage_pusher_metrics: StoragePusherMetrics::new(reg),
        }
    }
}

// ---------------------------------------------------------------------------
// PushErrorHandler (ported from pusher.go)
// ---------------------------------------------------------------------------

pub struct PushErrorHandler {
    metrics: StoragePusherMetrics,
    client_err_sampler: Option<Arc<ErrorSampler>>,
    log_sink: Option<Arc<SyncMutex<Vec<String>>>>,
}

impl PushErrorHandler {
    pub fn new(
        metrics: StoragePusherMetrics,
        client_err_sampler: Option<Arc<ErrorSampler>>,
        log_sink: Option<Arc<SyncMutex<Vec<String>>>>,
    ) -> Self {
        Self {
            metrics,
            client_err_sampler,
            log_sink,
        }
    }

    pub fn is_server_error(
        &self,
        tenant_id: &str,
        trace_id: Option<&str>,
        err: Option<&PusherError>,
    ) -> bool {
        self.metrics.total_requests.inc();
        let Some(err) = err else {
            return false;
        };

        if !err.is_client_error() {
            self.metrics.server_err_requests.inc();
            // Upstream Go (pusher.go:557-560) only records on spanLog and leaves retry/error logging to reader.go.
            return true;
        }

        self.metrics.client_err_requests.inc();
        let (keep, reason) = self.should_log_client_error(err);
        if keep {
            let err_str = if reason.is_empty() {
                err.to_string()
            } else {
                format!("{err} ({reason})")
            };
            if let Some(tid) = trace_id.filter(|s| !s.is_empty()) {
                warn!(
                    user = tenant_id,
                    trace_id = tid,
                    insight = true,
                    err = %err_str,
                    "detected a client error while ingesting write request (the request may have been partially ingested)"
                );
                if let Some(ref sink) = self.log_sink {
                    sink.lock().push(format!(
                        "user={tenant_id} trace_id={tid} level=warn msg=\"detected a client error while ingesting write request (the request may have been partially ingested)\" insight=true err=\"{err_str}\""
                    ));
                }
            } else {
                warn!(
                    user = tenant_id,
                    insight = true,
                    err = %err_str,
                    "detected a client error while ingesting write request (the request may have been partially ingested)"
                );
                if let Some(ref sink) = self.log_sink {
                    sink.lock().push(format!(
                        "user={tenant_id} level=warn msg=\"detected a client error while ingesting write request (the request may have been partially ingested)\" insight=true err=\"{err_str}\""
                    ));
                }
            }
        }
        false
    }

    pub fn should_log_client_error(&self, err: &PusherError) -> (bool, String) {
        match err.log_policy {
            Some(LogPolicy::DoNotLog) => (false, String::new()),
            Some(LogPolicy::Sampled {
                should_log,
                ref reason,
            }) => (should_log, reason.clone()),
            Some(LogPolicy::Always) => (true, String::new()),
            None => {
                if let Some(ref sampler) = self.client_err_sampler {
                    sampler.should_log()
                } else {
                    (true, String::new())
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// PusherConsumer & Storage Pushers
// ---------------------------------------------------------------------------

struct ParsedRecord {
    tenant_id: String,
    offset: i64,
    write_request: Result<cortexpb::WriteRequest, String>,
}

pub struct PusherConsumer {
    pusher: Arc<dyn Pusher>,
    kafka_cfg: KafkaConfig,
    metrics: PusherConsumerMetrics,
    fallback_client_error_sample_rate: i64,
    log_sink: Option<Arc<SyncMutex<Vec<String>>>>,
}

impl RecordBatchConsumer for PusherConsumer {
    fn consume(
        &self,
        records: Vec<FetchedRecord>,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>> {
        Box::pin(self.consume(records))
    }
}

impl PusherConsumer {
    pub fn new(
        pusher: Arc<dyn Pusher>,
        kafka_cfg: KafkaConfig,
        metrics: PusherConsumerMetrics,
    ) -> Self {
        Self {
            pusher,
            kafka_cfg,
            metrics,
            fallback_client_error_sample_rate: 0,
            log_sink: None,
        }
    }

    pub fn with_fallback_client_error_sample_rate(mut self, rate: i64) -> Self {
        self.fallback_client_error_sample_rate = rate;
        self
    }

    pub fn with_log_sink(mut self, sink: Arc<SyncMutex<Vec<String>>>) -> Self {
        self.log_sink = Some(sink);
        self
    }

    pub async fn consume(&self, records: Vec<FetchedRecord>) -> Result<(), String> {
        let start = Instant::now();
        let res = self.consume_inner(records).await;
        self.metrics
            .processing_time_seconds
            .observe(start.elapsed().as_secs_f64());
        res
    }

    async fn consume_inner(&self, records: Vec<FetchedRecord>) -> Result<(), String> {
        if records.is_empty() {
            return Ok(());
        }

        let bytes_per_tenant: HashMap<String, usize> = if self.kafka_cfg.ingestion_concurrency_max > 0 {
            let mut map: HashMap<String, usize> = HashMap::new();
            for rec in &records {
                if let Ok(tenant) = std::str::from_utf8(&rec.key) {
                    if let Some(entry) = map.get_mut(tenant) {
                        *entry += rec.value.len();
                    } else {
                        map.insert(tenant.to_owned(), rec.value.len());
                    }
                } else {
                    let tenant = String::from_utf8_lossy(&rec.key).into_owned();
                    *map.entry(tenant).or_insert(0) += rec.value.len();
                }
            }
            map
        } else {
            HashMap::new()
        };

        let sampler = if self.fallback_client_error_sample_rate > 0 {
            Some(Arc::new(ErrorSampler::new(
                self.fallback_client_error_sample_rate,
            )))
        } else {
            None
        };
        let error_handler = Arc::new(PushErrorHandler::new(
            self.metrics.storage_pusher_metrics.clone(),
            sampler,
            self.log_sink.clone(),
        ));

        let mut writer = if self.kafka_cfg.ingestion_concurrency_max <= 0 {
            StorageWriter::Sequential(SequentialStoragePusher::new(
                self.metrics.storage_pusher_metrics.clone(),
                Arc::clone(&self.pusher),
                error_handler,
            ))
        } else {
            StorageWriter::Parallel(ParallelStoragePusher::new(
                self.metrics.storage_pusher_metrics.clone(),
                Arc::clone(&self.pusher),
                bytes_per_tenant,
                error_handler,
                self.kafka_cfg.ingestion_concurrency_max as usize,
                self.kafka_cfg.ingestion_concurrency_batch_size.max(1) as usize,
                self.kafka_cfg.ingestion_concurrency_queue_capacity.max(1) as usize,
                self.kafka_cfg
                    .ingestion_concurrency_estimated_bytes_per_sample
                    .max(1) as usize,
                self.kafka_cfg
                    .ingestion_concurrency_target_flushes_per_shard
                    .max(1) as usize,
            ))
        };

        let mut return_err: Option<String> = None;

        if records.len() < 16 {
            // Fast inline path for small steady-state batches: avoid tokio::spawn / spawn_blocking / mpsc hops.
            let mut scratch = take_wr_scratch();
            for rec in records {
                let tenant_id_cow = String::from_utf8_lossy(&rec.key);
                let version = parse_record_version(
                    rec.headers.iter().map(|(k, v)| (k.as_str(), v.as_ref())),
                );
                let parsed = match deserialize_record_content(&rec.value, version, &mut scratch) {
                    Ok(borrowed) => {
                        Ok(borrowed_write_request_to_owned_with_backing(&borrowed, &rec.value))
                    }
                    Err(e) => Err(format!("parsing ingest consumer write request: {e}")),
                };
                scratch.clear();

                let wr = match parsed {
                    Ok(wr) => wr,
                    Err(err_msg) => {
                        error!(err = %err_msg, "failed to parse write request; skipping");
                        if let Some(ref sink) = self.log_sink {
                            sink.lock().push(format!(
                                "level=error msg=\"failed to parse write request; skipping\" err=\"{err_msg}\""
                            ));
                        }
                        continue;
                    }
                };

                if let Err(err) = writer
                    .push_to_storage_and_release_request(&tenant_id_cow, wr)
                    .await
                {
                    return_err = Some(format!(
                        "consuming record at offset {} for tenant {}: {err}",
                        rec.offset, tenant_id_cow
                    ));
                    break;
                }
            }
            return_wr_scratch(scratch);
        } else {
            let (cancel_tx, cancel_rx) = watch::channel(false);
            let (tx, mut rx) = mpsc::channel::<ParsedRecord>(256);

            let workers = (records.len() / 8).clamp(2, 8);
            let chunk_size = records.len().div_ceil(workers);
            let mut chunks: Vec<Vec<FetchedRecord>> = Vec::with_capacity(workers);
            let mut iter = records.into_iter();
            loop {
                let c: Vec<FetchedRecord> = iter.by_ref().take(chunk_size).collect();
                if c.is_empty() {
                    break;
                }
                chunks.push(c);
            }
            let unmarshal_handle: JoinHandle<()> = tokio::spawn(async move {
                let mut dec_handles = Vec::with_capacity(chunks.len());
                for chunk in chunks {
                    let cancel = cancel_rx.clone();
                    dec_handles.push(tokio::task::spawn_blocking(move || {
                        let mut scratch = take_wr_scratch();
                        let mut out = Vec::with_capacity(chunk.len());
                        for rec in chunk {
                            if *cancel.borrow() {
                                break;
                            }
                            let tenant_id = String::from_utf8_lossy(&rec.key).into_owned();
                            let version = parse_record_version(
                                rec.headers.iter().map(|(k, v)| (k.as_str(), v.as_ref())),
                            );
                            let parsed = match deserialize_record_content(
                                &rec.value,
                                version,
                                &mut scratch,
                            ) {
                                Ok(borrowed) => Ok(borrowed_write_request_to_owned_with_backing(
                                    &borrowed,
                                    &rec.value,
                                )),
                                Err(e) => Err(format!("parsing ingest consumer write request: {e}")),
                            };
                            scratch.clear();
                            out.push(ParsedRecord {
                                tenant_id,
                                offset: rec.offset,
                                write_request: parsed,
                            });
                        }
                        return_wr_scratch(scratch);
                        out
                    }));
                }
                for h in dec_handles {
                    if let Ok(items) = h.await {
                        for item in items {
                            if *cancel_rx.borrow() || tx.send(item).await.is_err() {
                                return;
                            }
                        }
                    }
                }
            });

            while let Some(item) = rx.recv().await {
                let wr = match item.write_request {
                    Ok(wr) => wr,
                    Err(err_msg) => {
                        error!(err = %err_msg, "failed to parse write request; skipping");
                        if let Some(ref sink) = self.log_sink {
                            sink.lock().push(format!(
                                "level=error msg=\"failed to parse write request; skipping\" err=\"{err_msg}\""
                            ));
                        }
                        continue;
                    }
                };

                if let Err(err) = writer
                    .push_to_storage_and_release_request(&item.tenant_id, wr)
                    .await
                {
                    let _ = cancel_tx.send(true);
                    return_err = Some(format!(
                        "consuming record at offset {} for tenant {}: {err}",
                        item.offset, item.tenant_id
                    ));
                    break;
                }
            }

            let _ = cancel_tx.send(true);
            drop(rx);
            let _ = unmarshal_handle.await;
        }

        let close_errs = writer.close().await;
        if return_err.is_none() && !close_errs.is_empty() {
            return_err = Some(format_multi_errors(&close_errs));
        }

        match return_err {
            None => Ok(()),
            Some(e) => Err(e),
        }
    }
}

fn format_multi_errors(errs: &[PusherError]) -> String {
    if errs.len() == 1 {
        return errs[0].to_string();
    }
    let joined: Vec<String> = errs.iter().map(ToString::to_string).collect();
    format!("{} errors: {}", errs.len(), joined.join("; "))
}

enum StorageWriter {
    Sequential(SequentialStoragePusher),
    Parallel(ParallelStoragePusher),
}

impl StorageWriter {
    async fn push_to_storage_and_release_request(
        &mut self,
        tenant_id: &str,
        req: cortexpb::WriteRequest,
    ) -> Result<(), PusherError> {
        match self {
            Self::Sequential(s) => s.push_to_storage_and_release_request(tenant_id, req).await,
            Self::Parallel(p) => p.push_to_storage_and_release_request(tenant_id, req).await,
        }
    }

    async fn close(&mut self) -> Vec<PusherError> {
        match self {
            Self::Sequential(_) => Vec::new(),
            Self::Parallel(p) => p.close().await,
        }
    }
}

pub struct SequentialStoragePusher {
    metrics: StoragePusherMetrics,
    error_handler: Arc<PushErrorHandler>,
    pusher: Arc<dyn Pusher>,
}

impl SequentialStoragePusher {
    pub fn new(
        metrics: StoragePusherMetrics,
        pusher: Arc<dyn Pusher>,
        error_handler: Arc<PushErrorHandler>,
    ) -> Self {
        Self {
            metrics,
            error_handler,
            pusher,
        }
    }

    pub async fn push_to_storage_and_release_request(
        &self,
        tenant_id: &str,
        wr: cortexpb::WriteRequest,
    ) -> Result<(), PusherError> {
        self.metrics
            .time_series_per_flush
            .observe(wr.timeseries.len() as f64);
        let contents = request_contents(&wr);
        let start = Instant::now();
        let res = self
            .pusher
            .push_to_storage_and_release_request(tenant_id, wr)
            .await;
        self.metrics
            .processing_time
            .with_label_values(&[contents])
            .observe(start.elapsed().as_secs_f64());

        if let Err(err) = res {
            if self.error_handler.is_server_error(tenant_id, None, Some(&err)) {
                return Err(err);
            }
        } else {
            self.error_handler.is_server_error(tenant_id, None, None);
        }
        Ok(())
    }
}

pub struct ParallelStoragePusher {
    metrics: StoragePusherMetrics,
    pushers: HashMap<String, Vec<(i32, ParallelStorageShards)>>,
    upstream_pusher: Arc<dyn Pusher>,
    error_handler: Arc<PushErrorHandler>,
    max_shards: usize,
    batch_size: usize,
    bytes_per_tenant: HashMap<String, usize>,
    queue_capacity: usize,
    bytes_per_sample: usize,
    target_flushes: usize,
    num_active_shards: usize,
}

impl ParallelStoragePusher {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        metrics: StoragePusherMetrics,
        pusher: Arc<dyn Pusher>,
        bytes_per_tenant: HashMap<String, usize>,
        error_handler: Arc<PushErrorHandler>,
        max_shards: usize,
        batch_size: usize,
        queue_capacity: usize,
        bytes_per_sample: usize,
        target_flushes: usize,
    ) -> Self {
        Self {
            metrics,
            pushers: HashMap::new(),
            upstream_pusher: pusher,
            error_handler,
            max_shards: max_shards.max(1),
            batch_size: batch_size.max(1),
            bytes_per_tenant,
            queue_capacity: queue_capacity.max(1),
            bytes_per_sample: bytes_per_sample.max(1),
            target_flushes: target_flushes.max(1),
            num_active_shards: 0,
        }
    }

    pub fn ideal_shards_for(&mut self, user_id: &str) -> usize {
        let bytes = self.bytes_per_tenant.get(user_id).copied().unwrap_or(0);
        let expected_timeseries = bytes / self.bytes_per_sample;
        self.metrics
            .estimated_timeseries
            .inc_by(expected_timeseries as f64);

        let ideal_shards = expected_timeseries / self.batch_size / self.target_flushes;
        let r = ideal_shards.min(self.max_shards).max(1);
        self.num_active_shards += r;
        r
    }

    fn shards_for_mut(
        &mut self,
        user_id: &str,
        source: i32,
    ) -> &mut ParallelStorageShards {
        let norm_source = cortexpb::write_request::SourceEnum::try_from(source)
            .map(|s| s as i32)
            .unwrap_or(cortexpb::write_request::SourceEnum::Api as i32);
        let has_entry = self
            .pushers
            .get(user_id)
            .is_some_and(|list| list.iter().any(|(s, _)| *s == norm_source));
        if !has_entry {
            let ideal = self.ideal_shards_for(user_id);
            let shards = ParallelStorageShards::new(
                self.metrics.clone(),
                Arc::clone(&self.error_handler),
                ideal,
                self.batch_size,
                self.queue_capacity,
                Arc::clone(&self.upstream_pusher),
            );
            if let Some(list) = self.pushers.get_mut(user_id) {
                list.push((norm_source, shards));
            } else {
                self.pushers
                    .insert(user_id.to_owned(), vec![(norm_source, shards)]);
            }
        }
        let list = self.pushers.get_mut(user_id).unwrap();
        &mut list.iter_mut().find(|(s, _)| *s == norm_source).unwrap().1
    }

    pub async fn push_to_storage_and_release_request(
        &mut self,
        tenant_id: &str,
        wr: cortexpb::WriteRequest,
    ) -> Result<(), PusherError> {
        let source = wr.source;
        let shards = self.shards_for_mut(tenant_id, source);
        shards.push_to_storage_and_release_request(tenant_id, wr).await
    }

    pub async fn close(&mut self) -> Vec<PusherError> {
        let mut errs = Vec::new();
        let mut num_pushers = 0usize;
        for (_, list) in self.pushers.drain() {
            num_pushers += list.len();
            for (_, shards) in list {
                errs.extend(shards.close().await);
            }
        }
        self.metrics
            .shards_per_push
            .observe(self.num_active_shards as f64);
        self.metrics.pushers_per_push.observe(num_pushers as f64);
        errs
    }
}

/// Computes xxhash64 over `LabelPair`s (`name \xff value \xff ...`), matching `LabelAdaptersHash` in `pusher.go`.
pub fn label_adapters_hash(buf: &mut Vec<u8>, labels: &[cortexpb::LabelPair]) -> u64 {
    const SEP: u8 = 0xff;
    buf.clear();
    for l in labels {
        buf.extend_from_slice(&l.name);
        buf.push(SEP);
        buf.extend_from_slice(&l.value);
        buf.push(SEP);
    }
    xxh64_seed0(buf)
}

pub struct ParallelStorageShards {
    num_shards: usize,
    shards: Vec<BatchingQueue>,
    workers: Vec<JoinHandle<()>>,
    rr_seed: AtomicU64,
    hash_buf: Vec<u8>,
}

impl ParallelStorageShards {
    pub fn new(
        metrics: StoragePusherMetrics,
        error_handler: Arc<PushErrorHandler>,
        num_shards: usize,
        batch_size: usize,
        capacity: usize,
        pusher: Arc<dyn Pusher>,
    ) -> Self {
        let num_shards = num_shards.max(1);
        let mut shards = Vec::with_capacity(num_shards);
        let mut workers = Vec::with_capacity(num_shards);

        for _ in 0..num_shards {
            let (queue, mut rx) = BatchingQueue::new(
                capacity,
                batch_size,
                metrics.batching_queue_metrics.clone(),
            );
            let done_handle = queue.done_handle();
            let err_store = queue.errors_handle();
            let m = metrics.clone();
            let eh = Arc::clone(&error_handler);
            let p = Arc::clone(&pusher);

            let handle = tokio::spawn(async move {
                while let Some(wr) = rx.recv().await {
                    m.batch_age.observe(wr.started_at.elapsed().as_secs_f64());
                    m.time_series_per_flush
                        .observe(wr.write_request.timeseries.len() as f64);
                    let contents = request_contents(&wr.write_request);
                    let start = Instant::now();
                    let res = p
                        .push_to_storage_and_release_request(&wr.tenant_id, wr.write_request)
                        .await;
                    m.processing_time
                        .with_label_values(&[contents])
                        .observe(start.elapsed().as_secs_f64());

                    match res {
                        Ok(()) => {
                            eh.is_server_error(&wr.tenant_id, None, None);
                        }
                        Err(err) => {
                            if eh.is_server_error(&wr.tenant_id, None, Some(&err)) {
                                err_store.lock().push(err);
                            }
                        }
                    }
                }
                done_handle.done();
            });
            shards.push(queue);
            workers.push(handle);
        }

        Self {
            num_shards,
            shards,
            workers,
            rr_seed: AtomicU64::new(0x517cc1b727220a95),
            hash_buf: if num_shards > 1 {
                Vec::with_capacity(1024)
            } else {
                Vec::new()
            },
        }
    }

    pub async fn push_to_storage_and_release_request(
        &mut self,
        tenant_id: &str,
        request: cortexpb::WriteRequest,
    ) -> Result<(), PusherError> {
        let source = request.source;

        for ts in request.timeseries {
            let shard = if self.num_shards > 1 {
                (label_adapters_hash(&mut self.hash_buf, &ts.labels) as usize) % self.num_shards
            } else {
                0
            };
            self.shards[shard]
                .add_to_batch(tenant_id, source, ts)
                .await
                .map_err(|e| {
                    PusherError::server(format!(
                        "encountered a non-client error when ingesting; this error was for a previous write request for the same tenant: {e}"
                    ))
                })?;
        }

        let mut md_shard = if self.num_shards > 1 {
            (self.rr_seed.fetch_add(1, Ordering::Relaxed) as usize) % self.num_shards
        } else {
            0
        };
        for md in request.metadata {
            self.shards[md_shard]
                .add_metadata_to_batch(tenant_id, source, md)
                .await
                .map_err(|e| {
                    PusherError::server(format!(
                        "encountered a non-client error when ingesting; this error was for a previous write request for the same tenant: {e}"
                    ))
                })?;
            if self.num_shards > 1 {
                md_shard = (md_shard + 1) % self.num_shards;
            }
        }

        Ok(())
    }

    pub async fn close(mut self) -> Vec<PusherError> {
        let mut errs = Vec::new();
        for shard in &mut self.shards {
            errs.extend(shard.close().await);
        }
        for w in self.workers {
            let _ = w.await;
        }
        errs
    }
}

pub fn request_contents(request: &cortexpb::WriteRequest) -> &'static str {
    match (!request.timeseries.is_empty(), !request.metadata.is_empty()) {
        (true, true) => "timeseries_and_metadata",
        (true, false) => "timeseries",
        (false, true) => "metadata",
        (false, false) => "empty",
    }
}

// ---------------------------------------------------------------------------
// BatchingQueue (ported from batchingQueue in pusher.go)
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct FlushableWriteRequest {
    pub started_at: Instant,
    pub started_set: bool,
    pub tenant_id: String,
    pub write_request: cortexpb::WriteRequest,
}

impl Default for FlushableWriteRequest {
    fn default() -> Self {
        Self {
            started_at: Instant::now(),
            started_set: false,
            tenant_id: String::new(),
            write_request: cortexpb::WriteRequest::default(),
        }
    }
}

#[derive(Clone)]
pub struct BatchingQueueDoneHandle {
    done_tx: tokio::sync::watch::Sender<bool>,
}

impl BatchingQueueDoneHandle {
    pub fn done(&self) {
        let _ = self.done_tx.send(true);
    }
}

pub struct BatchingQueue {
    metrics: BatchingQueueMetrics,
    tx: Option<mpsc::Sender<FlushableWriteRequest>>,
    errs: Arc<SyncMutex<Vec<PusherError>>>,
    done_tx: tokio::sync::watch::Sender<bool>,
    done_rx: tokio::sync::watch::Receiver<bool>,
    current_batch: FlushableWriteRequest,
    batch_size: usize,
}

impl BatchingQueue {
    pub fn new(
        capacity: usize,
        batch_size: usize,
        metrics: BatchingQueueMetrics,
    ) -> (Self, mpsc::Receiver<FlushableWriteRequest>) {
        let (tx, rx) = mpsc::channel(capacity.max(1));
        let (done_tx, done_rx) = tokio::sync::watch::channel(false);
        let batch_size = batch_size.max(1);
        let mut current_batch = FlushableWriteRequest::default();
        current_batch.write_request.timeseries.reserve(batch_size);
        let queue = Self {
            metrics,
            tx: Some(tx),
            errs: Arc::new(SyncMutex::new(Vec::new())),
            done_tx,
            done_rx,
            current_batch,
            batch_size,
        };
        (queue, rx)
    }

    pub fn done_handle(&self) -> BatchingQueueDoneHandle {
        BatchingQueueDoneHandle {
            done_tx: self.done_tx.clone(),
        }
    }

    pub fn errors_handle(&self) -> Arc<SyncMutex<Vec<PusherError>>> {
        Arc::clone(&self.errs)
    }

    pub fn current_batch_len(&self) -> usize {
        self.current_batch.write_request.timeseries.len()
            + self.current_batch.write_request.metadata.len()
    }

    pub fn pending_errors_len(&self) -> usize {
        self.errs.lock().len()
    }

    pub async fn add_to_batch(
        &mut self,
        tenant_id: &str,
        source: i32,
        ts: cortexpb::TimeSeries,
    ) -> Result<(), String> {
        if !self.current_batch.started_set {
            self.current_batch.started_at = Instant::now();
            self.current_batch.started_set = true;
        }
        if self.current_batch.tenant_id != tenant_id {
            self.current_batch.tenant_id.clear();
            self.current_batch.tenant_id.push_str(tenant_id);
        }
        self.current_batch.write_request.source = source;
        self.current_batch.write_request.timeseries.push(ts);
        self.push_if_full().await
    }

    pub async fn add_metadata_to_batch(
        &mut self,
        tenant_id: &str,
        source: i32,
        md: cortexpb::MetricMetadata,
    ) -> Result<(), String> {
        if !self.current_batch.started_set {
            self.current_batch.started_at = Instant::now();
            self.current_batch.started_set = true;
        }
        if self.current_batch.tenant_id != tenant_id {
            self.current_batch.tenant_id.clear();
            self.current_batch.tenant_id.push_str(tenant_id);
        }
        self.current_batch.write_request.source = source;
        self.current_batch.write_request.metadata.push(md);
        self.push_if_full().await
    }

    pub fn report_error(&self, err: Option<PusherError>) {
        if let Some(e) = err {
            self.errs.lock().push(e);
        }
    }

    pub fn done(&self) {
        let _ = self.done_tx.send(true);
    }

    async fn push_if_full(&mut self) -> Result<(), String> {
        if self.current_batch_len() >= self.batch_size {
            self.push().await
        } else {
            Ok(())
        }
    }

    async fn push(&mut self) -> Result<(), String> {
        let errs = self.collect_errors();
        self.metrics.flush_errors_total.inc_by(errs.len() as f64);
        self.metrics.flush_total.inc();

        let batch = std::mem::take(&mut self.current_batch);
        self.current_batch
            .write_request
            .timeseries
            .reserve(self.batch_size);
        if let Some(ref tx) = self.tx {
            let _ = tx.send(batch).await;
        }

        if errs.is_empty() {
            Ok(())
        } else {
            Err(format_multi_errors(&errs))
        }
    }

    fn collect_errors(&self) -> Vec<PusherError> {
        let mut guard = self.errs.lock();
        std::mem::take(&mut *guard)
    }

    /// Closes the queue, flushing any non-empty current batch, waiting for consumer `done()`,
    /// and returning any reported errors.
    pub async fn close(&mut self) -> Vec<PusherError> {
        let mut errs = Vec::new();
        if self.current_batch_len() > 0 {
            let collected = self.collect_errors();
            self.metrics
                .flush_errors_total
                .inc_by(collected.len() as f64);
            self.metrics.flush_total.inc();
            errs.extend(collected);

            let batch = std::mem::take(&mut self.current_batch);
            if let Some(ref tx) = self.tx {
                let _ = tx.send(batch).await;
            }
        }
        self.tx.take();
        let _ = self.done_rx.wait_for(|&done| done).await;
        errs.extend(self.collect_errors());
        errs
    }

    /// Closes only the sender side without awaiting `done()`, for unit tests inspecting channel contents directly.
    pub async fn close_without_waiting(&mut self) -> Result<(), String> {
        let mut errs = Vec::new();
        if self.current_batch_len() > 0 {
            let collected = self.collect_errors();
            self.metrics
                .flush_errors_total
                .inc_by(collected.len() as f64);
            self.metrics.flush_total.inc();
            errs.extend(collected);

            let batch = std::mem::take(&mut self.current_batch);
            if let Some(ref tx) = self.tx {
                let _ = tx.send(batch).await;
            }
        }
        self.tx.take();
        errs.extend(self.collect_errors());
        if errs.is_empty() {
            Ok(())
        } else {
            Err(format_multi_errors(&errs))
        }
    }
}

// Keep unused Mutex import from generating warnings if only SyncMutex used
#[allow(dead_code)]
fn _use_tokio_mutex() -> Option<Mutex<()>> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::version::{RecordSerializer, RecordVersion};

    #[test]
    fn rw2_zero_copy_backing_shares_payload_and_static_symbols() {
        let req = cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair::from_strs("__name__", "up"),
                    cortexpb::LabelPair::from_strs("custom_lbl", "custom_val"),
                    cortexpb::LabelPair::from_strs("job", "node-exporter"),
                ],
                samples: vec![cortexpb::Sample {
                    value: 1.0,
                    timestamp_ms: 1000,
                }],
                ..Default::default()
            }],
            metadata: vec![cortexpb::MetricMetadata {
                r#type: cortexpb::metric_metadata::MetricType::Gauge as i32,
                metric_family_name: "up".into(),
                help: "Target up".into(),
                unit: String::new(),
            }],
            source: cortexpb::write_request::SourceEnum::Api as i32,
            ..Default::default()
        };

        let (records, _) = RecordSerializer::new(RecordVersion::V2)
            .to_records("topic", 0, "tenant-1", &req, 1024 * 1024)
            .unwrap();
        assert_eq!(records.len(), 1);
        let payload = records[0].value.clone();
        let mut scratch = WriteRequestScratch::new();
        let borrowed = deserialize_record_content(&payload, 2, &mut scratch).unwrap();
        let owned = borrowed_write_request_to_owned_with_backing(&borrowed, &payload);

        assert_eq!(owned.timeseries.len(), 1);
        assert_eq!(owned.timeseries[0].labels.len(), 3);
        assert_eq!(owned.timeseries[0].labels[0].name.as_ref(), b"__name__");
        assert_eq!(owned.timeseries[0].labels[0].value.as_ref(), b"up");
        assert_eq!(owned.timeseries[0].labels[1].name.as_ref(), b"custom_lbl");
        assert_eq!(owned.timeseries[0].labels[1].value.as_ref(), b"custom_val");
        assert_eq!(owned.timeseries[0].labels[2].name.as_ref(), b"job");
        assert_eq!(owned.timeseries[0].labels[2].value.as_ref(), b"node-exporter");
        assert_eq!(owned.metadata.len(), 1);
        assert_eq!(owned.metadata[0].metric_family_name, "up");
        assert_eq!(owned.metadata[0].help, "Target up");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn d1_1_batching_queue_close_stress_500_iterations_no_lost_wakeup() {
        let reg = Registry::new();
        let bq_metrics = BatchingQueueMetrics::new(Some(&reg));
        for iter in 0..500usize {
            let (mut queue, mut rx) = BatchingQueue::new(4, 1, bq_metrics.clone());
            let done_handle = queue.done_handle();
            let worker = tokio::spawn(async move {
                while rx.recv().await.is_some() {}
                done_handle.done();
            });
            let errs = tokio::time::timeout(Duration::from_secs(5), queue.close())
                .await
                .unwrap_or_else(|_| panic!("BatchingQueue::close deadlocked at iteration {iter}"));
            assert!(errs.is_empty());
            worker.await.unwrap();
        }
    }
}
