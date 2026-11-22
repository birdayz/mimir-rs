//! Kafka partition readers (single-cluster & multi-cluster) with zero-copy decoding,
//! offset tracking, partition committer, startup lag catchup, and strong read consistency waiters.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/storage/ingest/reader.go
//! - github.com/grafana/mimir/pkg/storage/ingest/multi_cluster_reader.go

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures::future::{join_all, try_join_all};
use mimir_config::{KafkaConfig, OrderedConsumptionConfig};
use parking_lot::Mutex as SyncMutex;
use mimir_common::native_histogram::{
    HistogramOpts as NativeHistogramOpts, HistogramVec as NativeHistogramVec, SingleHistogram,
    DEF_BUCKETS,
};
use prometheus::{Counter, CounterVec, GaugeVec, Histogram, HistogramOpts, Opts, PullingGauge, Registry};
use thiserror::Error;
use tokio::sync::{oneshot, watch, Mutex};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use mimir_proto::zerocopy::{BorrowedWriteRequest, WriteRequestScratch};

use mimir_common::backoff::{Backoff, BackoffConfig};

use super::client_consumer::ClientPartitionConsumer;
use super::fetcher::{
    ConcurrentFetchers, FetchResult, FetcherMetrics, OffsetRange, RangeErrorPolicy,
};
use super::heap_merger::{HeapMerger, HeapMergerMetrics};
use super::kprom::KpromMetrics;
use super::labeled_registry::register_labeled_child;
use super::kafka_client::{
    FetchedRecord, KafkaClient, KafkaClientError, KAFKA_OFFSET_END, KAFKA_OFFSET_START,
};
use super::offset_catalogue::PartitionOffsets;
use super::offset_file::OffsetFile;
use super::partition_offset_client::PartitionOffsetClient;
use super::partition_offset_reader::{GenericOffsetReader, SingleClusterPartitionOffsetReader};
use super::partition_offset_watcher::{OffsetWatcherError, PartitionOffsetWatcher};
use super::pusher::{NoOpPreCommitNotifier, PreCommitNotifier};
use super::util::create_topics;
use super::version::{deserialize_record_content, parse_record_version, RecordCodecError};

pub const READER_METRICS_PREFIX: &str = "cortex_ingest_storage_reader";
pub const ERR_OFFSET_FILE_WRITE: &str = "failed to write offset to file";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ReaderState {
    New = 0,
    Starting = 1,
    Running = 2,
    Stopping = 3,
    Terminated = 4,
    Failed = 5,
}

impl std::fmt::Display for ReaderState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::New => "New",
            Self::Starting => "Starting",
            Self::Running => "Running",
            Self::Stopping => "Stopping",
            Self::Terminated => "Terminated",
            Self::Failed => "Failed",
        };
        f.write_str(s)
    }
}

#[derive(Debug, Error)]
pub enum ReaderError {
    #[error("kafka client error: {0}")]
    Kafka(#[from] KafkaClientError),
    #[error("offset watcher error: {0}")]
    Watcher(#[from] OffsetWatcherError),
    #[error("offset reader error: {0}")]
    OffsetReader(String),
    #[error("partition reader is lagging behind more than the allowed max delay")]
    MaxDelayExceeded,
    #[error("partition reader service is not running (state: {0})")]
    NotRunning(ReaderState),
    #[error("context deadline exceeded")]
    ContextDeadlineExceeded,
    /// `errors.Wrap(context.DeadlineExceeded, "wait strong read consistency timeout exceeded")`.
    #[error("wait strong read consistency timeout exceeded: context deadline exceeded")]
    WaitStrongReadConsistencyTimeoutExceeded,
    #[error("invalid configuration: {0}")]
    Config(String),
    #[error("the multi-cluster partition reader consumes from {expected} Kafka clusters but was given read consistency offsets for {got}")]
    ClusterCountMismatch { expected: usize, got: usize },
    /// `errors.Wrapf(err, "write compartment %d", kafkaClusterID)`.
    #[error("write compartment {id}: {source}")]
    WriteCompartment { id: usize, source: Box<ReaderError> },
    /// dskit `multierror.MultiError` of per write compartment errors.
    #[error("{}", format_multi_error(.0))]
    Multi(Vec<ReaderError>),
    #[error("a single-cluster partition reader was given read consistency offsets for {0} Kafka clusters")]
    InvalidClusterOffsetCount(usize),
    #[error("startup canceled")]
    Canceled,
    /// `errWaitTargetLagDeadlineExceeded`.
    #[error("target lag deadline exceeded")]
    WaitTargetLagDeadlineExceeded,
    #[error("startup error: {0}")]
    Startup(String),
}

/// Zero-copy decoded view over a fetched Kafka record.
pub struct DecodedKafkaRecord<'a, 's> {
    pub offset: i64,
    pub timestamp_ms: i64,
    pub tenant: &'a str,
    pub version: i32,
    pub write_request: BorrowedWriteRequest<'a, 's>,
}

/// Zero-copy decodes a `FetchedRecord` into `scratch` without per-series heap allocations.
pub fn decode_fetched_record<'a, 's>(
    rec: &'a FetchedRecord,
    scratch: &'s mut WriteRequestScratch,
) -> Result<DecodedKafkaRecord<'a, 's>, RecordCodecError> {
    let version = parse_record_version(rec.headers.iter().map(|(k, v)| (k.as_str(), v.as_ref())));
    let tenant = std::str::from_utf8(&rec.key).unwrap_or("");
    let write_request = deserialize_record_content(&rec.value, version, scratch)?;
    Ok(DecodedKafkaRecord {
        offset: rec.offset,
        timestamp_ms: rec.timestamp_ms,
        tenant,
        version,
        write_request,
    })
}

/// Async consumer interface for batches of `FetchedRecord`s.
pub trait RecordBatchConsumer: Send + Sync + 'static {
    fn consume(
        &self,
        records: Vec<FetchedRecord>,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>>;
}

/// Synchronous zero-copy record handler invoked per decoded record with a reusable `WriteRequestScratch`.
pub trait ZeroCopyRecordHandler: Send + Sync + 'static {
    fn on_batch_start(&self, _batch_len: usize) -> Result<(), String> {
        Ok(())
    }
    fn on_record(&self, record: DecodedKafkaRecord<'_, '_>) -> Result<(), String>;
    fn on_batch_end(&self) -> Result<(), String> {
        Ok(())
    }
}

/// Adapts a [`ZeroCopyRecordHandler`] into a [`RecordBatchConsumer`] using a reusable
/// per-consumer `WriteRequestScratch`.
pub struct ZeroCopyBatchConsumer<H: ZeroCopyRecordHandler> {
    handler: H,
    scratch: SyncMutex<WriteRequestScratch>,
}

impl<H: ZeroCopyRecordHandler> ZeroCopyBatchConsumer<H> {
    pub fn new(handler: H) -> Self {
        Self {
            handler,
            scratch: SyncMutex::new(WriteRequestScratch::new()),
        }
    }
}

impl<H: ZeroCopyRecordHandler> RecordBatchConsumer for ZeroCopyBatchConsumer<H> {
    fn consume(
        &self,
        records: Vec<FetchedRecord>,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>> {
        Box::pin(async move {
            let mut scratch = self.scratch.lock();
            self.handler.on_batch_start(records.len())?;
            for rec in &records {
                let decoded = decode_fetched_record(rec, &mut scratch)
                    .map_err(|e| format!("decode record offset {}: {e}", rec.offset))?;
                self.handler.on_record(decoded)?;
            }
            self.handler.on_batch_end()?;
            Ok(())
        })
    }
}

// ---------------------------------------------------------------------------
// StrongReadConsistencyMetrics
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct StrongReadConsistencyMetrics {
    pub requests: CounterVec,
    pub failures: CounterVec,
    pub latency: NativeHistogramVec,
}

impl StrongReadConsistencyMetrics {
    pub fn new(reg: Option<&Registry>, component: &str, topics: &[&str]) -> Self {
        let mut const_labels = std::collections::HashMap::new();
        const_labels.insert("component".to_string(), component.to_string());

        let mut req_opts = Opts::new(
            "cortex_ingest_storage_strong_consistency_requests_total",
            "Total number of requests for which strong consistency has been requested. The metric distinguishes between requests with an offset specified and requests requesting to enforce strong consistency up until the last produced offset.",
        );
        req_opts.const_labels = const_labels.clone();
        let requests = CounterVec::new(req_opts, &["with_offset", "topic"]).expect("valid metric");

        let mut fail_opts = Opts::new(
            "cortex_ingest_storage_strong_consistency_failures_total",
            "Total number of failures while waiting for strong consistency to be enforced.",
        );
        fail_opts.const_labels = const_labels.clone();
        let failures = CounterVec::new(fail_opts, &["topic"]).expect("valid metric");

        let latency = NativeHistogramVec::new(
            NativeHistogramOpts::new(
                "cortex_ingest_storage_strong_consistency_wait_duration_seconds",
                "How long a request spent waiting for strong consistency to be guaranteed.",
            )
            .const_labels(const_labels)
            .buckets(DEF_BUCKETS.to_vec())
            .native(1.1, 100, Duration::from_secs(3600)),
            &["topic"],
        )
        .expect("valid metric");

        for topic in topics {
            for val in ["true", "false"] {
                let _ = requests.with_label_values(&[val, topic]);
            }
            let _ = failures.with_label_values(&[topic]);
            let _ = latency.with_label_values(&[topic]);
        }

        if let Some(r) = reg {
            let _ = r.register(Box::new(requests.clone()));
            let _ = r.register(Box::new(failures.clone()));
            let _ = r.register(Box::new(latency.clone()));
        }

        Self {
            requests,
            failures,
            latency,
        }
    }
}

/// The fetcher currently used by a reader (`ReaderMetricsSource`), read by the pulled gauges.
type FetcherSlot = Arc<parking_lot::RwLock<Option<Arc<ReaderFetcher>>>>;

/// `prometheus.ExponentialBuckets(start, factor, count)`.
pub(crate) fn exponential_buckets(start: f64, factor: f64, count: usize) -> Vec<f64> {
    prometheus::exponential_buckets(start, factor, count).expect("valid buckets")
}

#[derive(Clone)]
pub struct ReaderMetrics {
    pub fetches_total: Counter,
    pub fetches_errors: Counter,
    pub records_per_fetch: Histogram,
    pub consume_latency: SingleHistogram,
    /// `fetchMaxBytes`, `fetchWaitDuration` and `fetchedDiscardedRecordBytes`
    /// (`ConcurrentFetchers` only).
    pub fetcher: FetcherMetrics,
    pub estimated_bytes_per_record: SingleHistogram,
    pub last_consumed_offset: GaugeVec,
    pub missed_records: Counter,
    pub receive_delay: NativeHistogramVec,
    pub receive_and_consume_delay: NativeHistogramVec,
    pub strong_consistency: StrongReadConsistencyMetrics,
    source: FetcherSlot,
}

impl ReaderMetrics {
    pub fn new(registry: Option<&Registry>) -> Self {
        Self::new_for_topic(registry, "")
    }

    /// `NewReaderMetrics` (`reader.go`).
    pub fn new_for_topic(registry: Option<&Registry>, topic: &str) -> Self {
        let source: FetcherSlot = Arc::new(parking_lot::RwLock::new(None));
        let fetches_total = Counter::with_opts(Opts::new(
            "cortex_ingest_storage_reader_fetches_total",
            "Total number of Kafka fetches received by the consumer.",
        ))
        .expect("valid metric");
        let fetches_errors = Counter::with_opts(Opts::new(
            "cortex_ingest_storage_reader_fetch_errors_total",
            "The number of fetch errors encountered by the consumer.",
        ))
        .expect("valid metric");
        let records_per_fetch = Histogram::with_opts(
            HistogramOpts::new(
                "cortex_ingest_storage_reader_records_per_fetch",
                "The number of records received by the consumer in a single fetch operation.",
            )
            .buckets(exponential_buckets(1.0, 2.0, 15)),
        )
        .expect("valid metric");
        let consume_latency = SingleHistogram::new(
            NativeHistogramOpts::new(
                "cortex_ingest_storage_reader_records_batch_process_duration_seconds",
                "How long a consumer spent processing a batch of records from Kafka. This includes retries on server errors.",
            )
            .native(1.1, 0, Duration::ZERO),
        )
        .expect("valid metric");
        let estimated_bytes_per_record = SingleHistogram::new(
            NativeHistogramOpts::new(
                "cortex_ingest_storage_reader_estimated_bytes_per_record",
                "Observations with the current size estimation of records fetched from Kafka. Sampled at 10Hz.",
            )
            .native(1.1, 0, Duration::ZERO),
        )
        .expect("valid metric");
        let last_consumed_offset = GaugeVec::new(
            Opts::new(
                "cortex_ingest_storage_reader_last_consumed_offset",
                "The last offset successfully consumed by the partition reader. Set to -1 if not offset has been consumed yet.",
            ),
            &["partition"],
        )
        .expect("valid metric");
        let missed_records = Counter::with_opts(Opts::new(
            "cortex_ingest_storage_reader_missed_records_total",
            "The number of offsets that were never consumed by the reader because they weren't fetched.",
        ))
        .expect("valid metric");
        // Buckets between 125ms and 9h (plus a native histogram upstream).
        let receive_delay = NativeHistogramVec::new(
            NativeHistogramOpts::new(
                "cortex_ingest_storage_reader_receive_delay_seconds",
                "Delay between producing a record and receiving it in the consumer.",
            )
            .buckets(exponential_buckets(0.125, 2.0, 18))
            .native(1.2, 100, Duration::from_secs(3600))
            .zero_threshold(2.0_f64.powi(-10)),
            &["phase"],
        )
        .expect("valid metric");
        let receive_and_consume_delay = NativeHistogramVec::new(
            NativeHistogramOpts::new(
                "cortex_ingest_storage_reader_receive_and_consume_delay_seconds",
                "Delay between producing a record and consuming it in the consumer.",
            )
            .buckets(exponential_buckets(0.125, 2.0, 18))
            .native(1.2, 100, Duration::from_secs(3600))
            .zero_threshold(2.0_f64.powi(-10)),
            &["phase"],
        )
        .expect("valid metric");
        for phase in [PHASE_STARTING, PHASE_RUNNING] {
            let _ = receive_delay.with_label_values(&[phase]);
            let _ = receive_and_consume_delay.with_label_values(&[phase]);
        }

        let src = Arc::clone(&source);
        let buffered_fetched_records = PullingGauge::new(
            "cortex_ingest_storage_reader_buffered_fetched_records",
            "The number of records fetched from Kafka by both concurrent fetchers and the Kafka client but not yet processed.",
            Box::new(move || src.read().as_ref().map_or(0, |f| f.buffered_records()) as f64),
        )
        .expect("valid metric");

        let src = Arc::clone(&source);
        let buffered_fetched_bytes = PullingGauge::new(
            "cortex_ingest_storage_reader_buffered_fetched_bytes",
            "The number of bytes fetched or requested from Kafka by both concurrent fetchers and the Kafka client but not yet processed. The value depends on -ingest-storage.kafka.use-compressed-bytes-as-fetch-max-bytes.",
            Box::new(move || src.read().as_ref().map_or(0, |f| f.buffered_bytes()) as f64),
        )
        .expect("valid metric");

        let topics_arr = if topic.is_empty() { vec![] } else { vec![topic] };
        let strong_consistency =
            StrongReadConsistencyMetrics::new(registry, "partition-reader", &topics_arr);
        let fetcher = FetcherMetrics::new(registry);

        if let Some(reg) = registry {
            let _ = reg.register(Box::new(buffered_fetched_records));
            let _ = reg.register(Box::new(buffered_fetched_bytes));
            let _ = reg.register(Box::new(estimated_bytes_per_record.clone()));
            let _ = reg.register(Box::new(receive_delay.clone()));
            let _ = reg.register(Box::new(receive_and_consume_delay.clone()));
            let _ = reg.register(Box::new(last_consumed_offset.clone()));
            let _ = reg.register(Box::new(records_per_fetch.clone()));
            let _ = reg.register(Box::new(fetches_errors.clone()));
            let _ = reg.register(Box::new(fetches_total.clone()));
            let _ = reg.register(Box::new(consume_latency.clone()));
            let _ = reg.register(Box::new(missed_records.clone()));
        }

        Self {
            fetches_total,
            fetches_errors,
            records_per_fetch,
            consume_latency,
            fetcher,
            estimated_bytes_per_record,
            last_consumed_offset,
            missed_records,
            receive_delay,
            receive_and_consume_delay,
            strong_consistency,
            source,
        }
    }

    /// The `ingest-storage-partition-reader-metrics` timer service: samples the estimated bytes
    /// per record every 100ms until `stop_rx` flips.
    fn spawn_sampler(&self, mut stop_rx: watch::Receiver<bool>) -> JoinHandle<()> {
        let hist = self.estimated_bytes_per_record.clone();
        let source = Arc::clone(&self.source);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_millis(100));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            ticker.tick().await;
            loop {
                tokio::select! {
                    res = stop_rx.changed() => {
                        if res.is_err() || *stop_rx.borrow() {
                            return;
                        }
                    }
                    _ = ticker.tick() => {
                        let v = source.read().as_ref().map_or(0, |f| f.estimated_bytes_per_record());
                        hist.observe(v as f64);
                    }
                }
            }
        })
    }
}

pub const PHASE_STARTING: &str = "starting";
pub const PHASE_RUNNING: &str = "running";

/// The fetcher of a partition reader: `ConcurrentFetchers` (`fetch_concurrency_max > 0`) or the
/// Kafka client consuming the partition (`fetch_concurrency_max = 0`).
pub enum ReaderFetcher {
    Concurrent(ConcurrentFetchers),
    Client(ClientPartitionConsumer),
}

impl ReaderFetcher {
    async fn start(&self) {
        match self {
            Self::Concurrent(f) => f.start().await,
            Self::Client(c) => c.start().await,
        }
    }

    async fn stop(&self) {
        match self {
            Self::Concurrent(f) => f.stop().await,
            Self::Client(c) => c.stop().await,
        }
    }

    /// `PollFetches(ctx)`: zero fetches if `cancel` completes first.
    async fn poll_fetches<F: Future>(&self, cancel: F) -> Vec<FetchResult> {
        let res = match self {
            Self::Concurrent(f) => f.poll_fetches_until(cancel).await,
            Self::Client(c) => c.poll_fetches_until(cancel).await,
        };
        res.into_iter().collect()
    }

    pub fn buffered_records(&self) -> i64 {
        match self {
            Self::Concurrent(f) => f.buffered_records(),
            Self::Client(c) => c.buffered_records(),
        }
    }

    pub fn buffered_bytes(&self) -> i64 {
        match self {
            Self::Concurrent(f) => f.buffered_bytes(),
            Self::Client(c) => c.buffered_bytes(),
        }
    }

    pub fn estimated_bytes_per_record(&self) -> i64 {
        match self {
            Self::Concurrent(f) => f.estimated_bytes_per_record(),
            Self::Client(_) => 0,
        }
    }
}

// ---------------------------------------------------------------------------
// AdmClient & PartitionCommitter (ported from reader.go)
// ---------------------------------------------------------------------------

pub trait AdmClient: Send + Sync + 'static {
    fn commit_offsets(
        &self,
        group: &str,
        topic: &str,
        partition_id: i32,
        offset: i64,
    ) -> Pin<Box<dyn Future<Output = Result<i64, String>> + Send + '_>>;
}

pub struct KafkaAdmClient {
    client: Arc<KafkaClient>,
}

impl KafkaAdmClient {
    pub fn new(client: Arc<KafkaClient>) -> Self {
        Self { client }
    }
}

impl AdmClient for KafkaAdmClient {
    fn commit_offsets(
        &self,
        group: &str,
        topic: &str,
        partition_id: i32,
        offset: i64,
    ) -> Pin<Box<dyn Future<Output = Result<i64, String>> + Send + '_>> {
        let group = group.to_string();
        let topic = topic.to_string();
        Box::pin(async move {
            self.client
                .commit_offset(&group, &topic, partition_id, offset)
                .await
                .map(|()| offset)
                .map_err(|e| e.to_string())
        })
    }
}

struct LabeledPullingGauge {
    desc: prometheus::core::Desc,
    label_name: String,
    label_value: String,
    cb: Box<dyn Fn() -> f64 + Send + Sync>,
}

impl LabeledPullingGauge {
    fn new(
        name: &str,
        help: &str,
        label_name: &str,
        label_value: String,
        cb: Box<dyn Fn() -> f64 + Send + Sync>,
    ) -> Self {
        let mut const_labels = std::collections::HashMap::new();
        const_labels.insert(label_name.to_string(), label_value.clone());
        let desc = prometheus::core::Desc::new(
            name.to_string(),
            help.to_string(),
            Vec::new(),
            const_labels,
        )
        .expect("valid desc");
        Self {
            desc,
            label_name: label_name.to_string(),
            label_value,
            cb,
        }
    }
}

impl prometheus::core::Collector for LabeledPullingGauge {
    fn desc(&self) -> Vec<&prometheus::core::Desc> {
        vec![&self.desc]
    }

    fn collect(&self) -> Vec<prometheus::proto::MetricFamily> {
        let mut lp = prometheus::proto::LabelPair::new();
        lp.set_name(self.label_name.clone());
        lp.set_value(self.label_value.clone());

        let mut g = prometheus::proto::Gauge::new();
        g.set_value((self.cb)());

        let mut m = prometheus::proto::Metric::new();
        m.set_label(vec![lp].into());
        m.set_gauge(g);

        let mut mf = prometheus::proto::MetricFamily::new();
        mf.set_name(self.desc.fq_name.clone());
        mf.set_help(self.desc.help.clone());
        mf.set_field_type(prometheus::proto::MetricType::GAUGE);
        mf.set_metric(vec![m].into());
        vec![mf]
    }
}

/// Manages periodic and shutdown Kafka group + file offset commits.
/// Ported from `partitionCommitter` in `pkg/storage/ingest/reader.go`.
pub struct PartitionCommitter {
    kafka_cfg: KafkaConfig,
    partition_id: i32,
    consumer_group: String,
    to_commit: Arc<AtomicI64>,
    last_committed_offset: Arc<AtomicI64>,
    adm_client: Arc<dyn AdmClient>,
    notifier: SyncMutex<Arc<dyn PreCommitNotifier>>,
    offset_file: Option<Arc<OffsetFile>>,
    commit_requests_total: Counter,
    commit_failures_total: Counter,
    commit_requests_latency: SingleHistogram,
    stop_tx: watch::Sender<bool>,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl PartitionCommitter {
    pub fn new(
        kafka_cfg: KafkaConfig,
        adm_client: Arc<dyn AdmClient>,
        partition_id: i32,
        consumer_group: impl Into<String>,
        notifier: Arc<dyn PreCommitNotifier>,
        offset_file: Option<Arc<OffsetFile>>,
        registry: Option<&Registry>,
    ) -> Self {
        let part_label = partition_id.to_string();
        let mut const_labels = std::collections::HashMap::new();
        const_labels.insert("partition".to_string(), part_label.clone());

        let mut req_opts = Opts::new(
            "cortex_ingest_storage_reader_offset_commit_requests_total",
            "Total number of requests issued to commit the last consumed offset (includes both successful and failed requests).",
        );
        req_opts.const_labels = const_labels.clone();
        let commit_requests_total = Counter::with_opts(req_opts).expect("valid metric");

        let mut fail_opts = Opts::new(
            "cortex_ingest_storage_reader_offset_commit_failures_total",
            "Total number of failed requests to commit the last consumed offset.",
        );
        fail_opts.const_labels = const_labels.clone();
        let commit_failures_total = Counter::with_opts(fail_opts).expect("valid metric");

        let commit_requests_latency = SingleHistogram::new(
            NativeHistogramOpts::new(
                "cortex_ingest_storage_reader_offset_commit_request_duration_seconds",
                "The duration of requests to commit the last consumed offset.",
            )
            .const_labels(const_labels.clone())
            .buckets(DEF_BUCKETS.to_vec())
            .native(1.1, 100, Duration::from_secs(3600)),
        )
        .expect("valid metric");

        let last_committed_offset = Arc::new(AtomicI64::new(-1));
        let lco_clone = Arc::clone(&last_committed_offset);
        let last_committed_gauge = LabeledPullingGauge::new(
            "cortex_ingest_storage_reader_last_committed_offset",
            "The last consumed offset successfully committed by the partition reader. Set to -1 if not offset has been committed yet.",
            "partition",
            part_label,
            Box::new(move || lco_clone.load(Ordering::SeqCst) as f64),
        );

        if let Some(reg) = registry {
            let _ = reg.register(Box::new(commit_requests_total.clone()));
            let _ = reg.register(Box::new(commit_failures_total.clone()));
            let _ = reg.register(Box::new(commit_requests_latency.clone()));
            let _ = reg.register(Box::new(last_committed_gauge));
        }

        let (stop_tx, _) = watch::channel(false);

        Self {
            kafka_cfg,
            partition_id,
            consumer_group: consumer_group.into(),
            to_commit: Arc::new(AtomicI64::new(-1)),
            last_committed_offset,
            adm_client,
            notifier: SyncMutex::new(notifier),
            offset_file,
            commit_requests_total,
            commit_failures_total,
            commit_requests_latency,
            stop_tx,
            task: Mutex::new(None),
        }
    }

    pub fn set_pre_commit_notifier(&self, notifier: Arc<dyn PreCommitNotifier>) {
        *self.notifier.lock() = notifier;
    }

    pub fn enqueue_offset(&self, offset: i64) {
        self.to_commit.store(offset, Ordering::SeqCst);
    }

    pub fn last_committed_offset(&self) -> i64 {
        self.last_committed_offset.load(Ordering::SeqCst)
    }

    pub fn commit_failures_count(&self) -> f64 {
        self.commit_failures_total.get()
    }

    pub fn commit_requests_count(&self) -> f64 {
        self.commit_requests_total.get()
    }

    pub async fn start(self: &Arc<Self>) {
        let mut guard = self.task.lock().await;
        if guard.is_some() {
            return;
        }
        let interval_dur = Duration::from_nanos(
            self.kafka_cfg
                .consumer_group_offset_commit_interval
                .as_nanos()
                .max(0) as u64,
        )
        .max(Duration::from_millis(10));
        let mut stop_rx = self.stop_tx.subscribe();
        let this = Arc::clone(self);

        let mut previous_offset = self.to_commit.load(Ordering::SeqCst);
        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval_dur);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            ticker.tick().await;

            loop {
                tokio::select! {
                    res = stop_rx.changed() => {
                        if res.is_err() || *stop_rx.borrow() {
                            break;
                        }
                    }
                    _ = ticker.tick() => {
                        let curr_offset = this.to_commit.load(Ordering::SeqCst);
                        if curr_offset == previous_offset || curr_offset < 0 {
                            continue;
                        }
                        if this.commit(curr_offset).await.is_ok() {
                            previous_offset = curr_offset;
                        }
                    }
                }
            }
        });
        *guard = Some(handle);
    }

    pub async fn stop(&self) {
        let _ = self.stop_tx.send(true);
        if let Some(h) = self.task.lock().await.take() {
            let _ = h.await;
        }
        let offset = self.to_commit.load(Ordering::SeqCst);
        if offset >= 0 {
            let _ = self.commit(offset).await;
        }
    }

    pub async fn commit(&self, offset: i64) -> Result<(), String> {
        let start = Instant::now();
        self.commit_requests_total.inc();

        let notifier = Arc::clone(&self.notifier.lock());
        if let Err(e) = notifier.notify_pre_commit().await {
            warn!(
                partition = self.partition_id,
                offset,
                err = %e,
                "pre-commit notification failed, continuing with commit"
            );
        }

        let kafka_res = if self.consumer_group.is_empty() {
            Ok(offset)
        } else {
            self.adm_client
                .commit_offsets(
                    &self.consumer_group,
                    &self.kafka_cfg.topic,
                    self.partition_id,
                    offset,
                )
                .await
        };

        let file_res = match self.offset_file {
            Some(ref f) => f
                .write(offset)
                .map_err(|e| format!("{ERR_OFFSET_FILE_WRITE}: {e}")),
            None => Ok(()),
        };

        let mut errs = Vec::new();
        let committed_at = match kafka_res {
            Ok(at) => at,
            Err(e) => {
                errs.push(e);
                offset
            }
        };
        if let Err(e) = file_res {
            errs.push(e);
        }

        self.commit_requests_latency
            .observe(start.elapsed().as_secs_f64());

        if !errs.is_empty() {
            // dskit `multierror.MultiError.Error()`: "<n> errors: " prefix only when n > 1.
            let err_msg = if errs.len() > 1 {
                format!("{} errors: {}", errs.len(), errs.join("; "))
            } else {
                errs.join("; ")
            };
            error!(partition = self.partition_id, offset, err = %err_msg, "failed to commit last consumed offset");
            self.commit_failures_total.inc();
            return Err(err_msg);
        }

        self.last_committed_offset
            .store(committed_at, Ordering::SeqCst);
        debug!(partition = self.partition_id, offset = committed_at, "last consumed offset committed to Kafka and file");
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// SingleClusterPartitionReader
// ---------------------------------------------------------------------------

/// Consumes a single Kafka partition, tracks consumed offsets, persists offset file & group commits,
/// and notifies strong-consistency read waiters.
pub struct SingleClusterPartitionReader {
    kafka_cfg: KafkaConfig,
    partition_id: i32,
    consumer_group: String,
    client: Arc<KafkaClient>,
    consumer: Arc<dyn RecordBatchConsumer>,
    notifier: Arc<dyn PreCommitNotifier>,
    offset_file: Option<Arc<OffsetFile>>,
    offset_watcher: Arc<PartitionOffsetWatcher>,
    offset_reader: Arc<SingleClusterPartitionOffsetReader>,
    committer: Arc<PartitionCommitter>,
    /// The offsets client shared by the offset reader and the concurrent fetchers.
    offset_client: PartitionOffsetClient,
    metrics: ReaderMetrics,
    /// Set once started: the fetcher records are polled from.
    fetcher: Arc<Mutex<Option<Arc<ReaderFetcher>>>>,
    /// Partition start offset reader refreshed every 10s (`startOffsetReader`).
    start_offset_reader: Arc<Mutex<Option<Arc<GenericOffsetReader<i64>>>>>,
    sampler: Arc<Mutex<Option<JoinHandle<()>>>>,
    last_seen_offset: Arc<AtomicI64>,
    highest_consumed_ts_before_end_ms: Arc<AtomicI64>,
    max_replay_period: Duration,
    concurrent_fetchers_min_bytes_max_wait_time: Duration,
    state: Arc<AtomicU8>,
    log_sink: Option<Arc<SyncMutex<Vec<String>>>>,
    stop_tx: watch::Sender<bool>,
    failure_tx: watch::Sender<Option<String>>,
    _failure_rx: watch::Receiver<Option<String>>,
    task: Mutex<Option<JoinHandle<()>>>,
    start_task: Mutex<Option<JoinHandle<Result<(), ReaderError>>>>,
}

impl SingleClusterPartitionReader {
    pub fn new(
        kafka_cfg: KafkaConfig,
        partition_id: i32,
        instance_id: &str,
        offset_file_path: Option<PathBuf>,
        consumer: Arc<dyn RecordBatchConsumer>,
        registry: Option<&Registry>,
    ) -> Result<Self, ReaderError> {
        Self::new_with_notifier(
            kafka_cfg,
            partition_id,
            instance_id,
            offset_file_path,
            consumer,
            Arc::new(NoOpPreCommitNotifier),
            registry,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_notifier(
        kafka_cfg: KafkaConfig,
        partition_id: i32,
        instance_id: &str,
        offset_file_path: Option<PathBuf>,
        consumer: Arc<dyn RecordBatchConsumer>,
        notifier: Arc<dyn PreCommitNotifier>,
        registry: Option<&Registry>,
    ) -> Result<Self, ReaderError> {
        let client = Arc::new(KafkaClient::from_config(&kafka_cfg)?);
        Self::with_client_and_notifier(
            kafka_cfg,
            partition_id,
            instance_id,
            offset_file_path,
            client,
            consumer,
            notifier,
            registry,
        )
    }

    pub fn new_strict(
        kafka_cfg: KafkaConfig,
        partition_id: i32,
        instance_id: &str,
        offset_file_path: &str,
        consumer: Arc<dyn RecordBatchConsumer>,
        registry: Option<&Registry>,
    ) -> Result<Self, ReaderError> {
        if offset_file_path.is_empty() {
            return Err(ReaderError::Config(
                "offset file path must be specified".to_string(),
            ));
        }
        Self::new(
            kafka_cfg,
            partition_id,
            instance_id,
            Some(PathBuf::from(offset_file_path)),
            consumer,
            registry,
        )
    }

    pub fn with_client(
        kafka_cfg: KafkaConfig,
        partition_id: i32,
        instance_id: &str,
        offset_file_path: Option<PathBuf>,
        client: Arc<KafkaClient>,
        consumer: Arc<dyn RecordBatchConsumer>,
        registry: Option<&Registry>,
    ) -> Result<Self, ReaderError> {
        Self::with_client_and_notifier(
            kafka_cfg,
            partition_id,
            instance_id,
            offset_file_path,
            client,
            consumer,
            Arc::new(NoOpPreCommitNotifier),
            registry,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn with_client_and_notifier(
        kafka_cfg: KafkaConfig,
        partition_id: i32,
        instance_id: &str,
        offset_file_path: Option<PathBuf>,
        client: Arc<KafkaClient>,
        consumer: Arc<dyn RecordBatchConsumer>,
        notifier: Arc<dyn PreCommitNotifier>,
        registry: Option<&Registry>,
    ) -> Result<Self, ReaderError> {
        let consumer_group = super::get_consumer_group(&kafka_cfg, instance_id, partition_id);
        let offset_file = offset_file_path.map(|p| Arc::new(OffsetFile::new(p, partition_id)));
        let offset_watcher = Arc::new(PartitionOffsetWatcher::new());
        // kprom hooks of the reader client (`NewKafkaReaderClientMetrics`).
        client.set_kprom(KpromMetrics::new(READER_METRICS_PREFIX, "partition-reader", registry));
        let offset_client = PartitionOffsetClient::new(Arc::clone(&client), registry);
        let offset_reader = Arc::new(SingleClusterPartitionOffsetReader::new(
            offset_client.clone(),
            kafka_cfg.topic.clone(),
            partition_id,
            Duration::from_nanos(
                kafka_cfg.last_produced_offset_poll_interval.as_nanos().max(0) as u64,
            ),
        ));

        let adm: Arc<dyn AdmClient> = Arc::new(KafkaAdmClient::new(Arc::clone(&client)));
        let committer = Arc::new(PartitionCommitter::new(
            kafka_cfg.clone(),
            adm,
            partition_id,
            consumer_group.clone(),
            Arc::clone(&notifier),
            offset_file.clone(),
            registry,
        ));

        let metrics = ReaderMetrics::new_for_topic(registry, &kafka_cfg.topic);
        metrics
            .last_consumed_offset
            .with_label_values(&[&partition_id.to_string()])
            .set(-1.0);

        let fetch_max_wait =
            Duration::from_nanos(kafka_cfg.fetch_max_wait.as_nanos().max(0) as u64);

        let (stop_tx, _) = watch::channel(false);
        let (failure_tx, failure_rx) = watch::channel(None);

        Ok(Self {
            kafka_cfg,
            partition_id,
            consumer_group,
            client,
            consumer,
            notifier,
            offset_file,
            offset_watcher,
            offset_reader,
            committer,
            offset_client,
            metrics,
            fetcher: Arc::new(Mutex::new(None)),
            start_offset_reader: Arc::new(Mutex::new(None)),
            sampler: Arc::new(Mutex::new(None)),
            last_seen_offset: Arc::new(AtomicI64::new(-1)),
            highest_consumed_ts_before_end_ms: Arc::new(AtomicI64::new(UNSET_TS_MS)),
            max_replay_period: Duration::ZERO,
            concurrent_fetchers_min_bytes_max_wait_time: fetch_max_wait,
            state: Arc::new(AtomicU8::new(ReaderState::New as u8)),
            log_sink: None,
            stop_tx,
            failure_tx,
            _failure_rx: failure_rx,
            task: Mutex::new(None),
            start_task: Mutex::new(None),
        })
    }

    pub fn with_max_replay_period(mut self, period: Duration) -> Self {
        self.max_replay_period = period;
        self
    }

    pub fn with_pre_commit_notifier(mut self, notifier: Arc<dyn PreCommitNotifier>) -> Self {
        self.committer.set_pre_commit_notifier(Arc::clone(&notifier));
        self.notifier = notifier;
        self
    }

    pub fn with_log_sink(mut self, sink: Arc<SyncMutex<Vec<String>>>) -> Self {
        self.log_sink = Some(sink);
        self
    }

    pub fn concurrent_fetchers_min_bytes_max_wait_time(&self) -> Duration {
        self.concurrent_fetchers_min_bytes_max_wait_time
    }

    #[doc(hidden)]
    pub fn set_state_for_test(&self, state: ReaderState) {
        self.state.store(state as u8, Ordering::SeqCst);
    }

    pub fn state(&self) -> ReaderState {
        match self.state.load(Ordering::SeqCst) {
            0 => ReaderState::New,
            1 => ReaderState::Starting,
            2 => ReaderState::Running,
            3 => ReaderState::Stopping,
            4 => ReaderState::Terminated,
            _ => ReaderState::Failed,
        }
    }

    pub fn consumer_group(&self) -> &str {
        &self.consumer_group
    }

    pub fn kafka_client(&self) -> &Arc<KafkaClient> {
        &self.client
    }

    pub fn offset_watcher(&self) -> &Arc<PartitionOffsetWatcher> {
        &self.offset_watcher
    }

    pub fn committer(&self) -> &Arc<PartitionCommitter> {
        &self.committer
    }

    pub fn last_consumed_offset(&self) -> i64 {
        self.offset_watcher.last_consumed_offset()
    }

    pub fn last_seen_offset(&self) -> i64 {
        self.last_seen_offset.load(Ordering::SeqCst)
    }

    pub fn last_committed_offset(&self) -> i64 {
        self.committer.last_committed_offset()
    }

    pub fn last_seen_offsets(&self) -> PartitionOffsets {
        PartitionOffsets::new_single_cluster(self.last_seen_offset())
    }

    /// `BufferedRecords`: records fetched but not yet consumed (0 before starting).
    pub fn buffered_records(&self) -> i64 {
        self.metrics.source.read().as_ref().map_or(0, |f| f.buffered_records())
    }

    /// `BufferedBytes`: bytes fetched or requested but not yet consumed (0 before starting).
    pub fn buffered_bytes(&self) -> i64 {
        self.metrics.source.read().as_ref().map_or(0, |f| f.buffered_bytes())
    }

    /// `EstimatedBytesPerRecord` (0 unless concurrent fetching is used).
    pub fn estimated_bytes_per_record(&self) -> i64 {
        self.metrics
            .source
            .read()
            .as_ref()
            .map_or(0, |f| f.estimated_bytes_per_record())
    }

    pub fn highest_consumed_timestamp_before_partition_end_ms(&self) -> i64 {
        self.highest_consumed_ts_before_end_ms
            .load(Ordering::SeqCst)
    }

    pub fn log_fetch_errors(&self, errors: &[(&str, i32, &str, bool)]) {
        let mut non_canceled = 0usize;
        let mut msgs = Vec::new();
        for &(topic, partition, err_msg, is_canceled) in errors {
            if is_canceled {
                continue;
            }
            non_canceled += 1;
            msgs.push(format!("topic {topic:?}, partition {partition}: {err_msg}"));
        }
        if non_canceled == 0 {
            return;
        }
        self.metrics.fetches_errors.inc_by(non_canceled as f64);
        error!(partition = self.partition_id, err = %msgs.join("; "), "encountered error while fetching");
    }

    pub async fn fetch_last_committed_offset(&self) -> Result<Option<i64>, ReaderError> {
        Ok(self
            .client
            .fetch_committed_offset(&self.consumer_group, &self.kafka_cfg.topic, self.partition_id)
            .await?)
    }

    pub async fn fetch_partition_start_offset(&self) -> Result<i64, ReaderError> {
        Ok(self
            .client
            .fetch_partition_start_offset(&self.kafka_cfg.topic, self.partition_id)
            .await?)
    }

    pub async fn fetch_first_offset_after_time(
        &self,
        ts_ms: i64,
    ) -> Result<Option<(i64, bool)>, ReaderError> {
        match self
            .client
            .list_offset_with_timestamp(&self.kafka_cfg.topic, self.partition_id, ts_ms)
            .await
        {
            Ok(listed) => {
                if listed.offset < 0 {
                    // Broker returned -1 (no matching record); resolve end offset
                    let end = self
                        .client
                        .list_offset(&self.kafka_cfg.topic, self.partition_id, KAFKA_OFFSET_END)
                        .await?;
                    Ok(Some((end, false)))
                } else {
                    Ok(Some((listed.offset, listed.timestamp >= 0)))
                }
            }
            Err(KafkaClientError::Broker(kafka_protocol::error::ResponseError::UnknownTopicOrPartition)) => {
                Ok(None)
            }
            Err(e) => Err(ReaderError::Kafka(e)),
        }
    }

    pub async fn fetch_record_timestamp_at_offset(&self, offset: i64) -> Result<i64, ReaderError> {
        let res = self
            .client
            .fetch(
                &self.kafka_cfg.topic,
                self.partition_id,
                offset,
                Duration::from_millis(500),
                1,
                1024 * 1024,
            )
            .await?;
        let rec = res
            .records
            .into_iter()
            .find(|r| r.offset == offset)
            .ok_or_else(|| {
                ReaderError::OffsetReader(format!(
                    "expected one record at offset {offset} for max replay period verification"
                ))
            })?;
        Ok(rec.timestamp_ms)
    }

    async fn resolve_offset_after_no_timestamp_match(
        &self,
        cutoff_ms: i64,
        resolved_end_offset: i64,
    ) -> Result<i64, ReaderError> {
        let partition_start = self.fetch_partition_start_offset().await?;
        if partition_start > resolved_end_offset {
            return Err(ReaderError::OffsetReader(format!(
                "partition start offset {partition_start} is after timestamp lookup result {resolved_end_offset}"
            )));
        }
        if partition_start == resolved_end_offset {
            return Ok(resolved_end_offset);
        }

        let tail_ts = self
            .fetch_record_timestamp_at_offset(resolved_end_offset - 1)
            .await?;
        if tail_ts < cutoff_ms {
            return Ok(resolved_end_offset);
        }

        let Some((retry_offset, matched)) = self.fetch_first_offset_after_time(cutoff_ms).await?
        else {
            return Err(ReaderError::OffsetReader(
                "partition offset is unavailable when retrying timestamp lookup".to_string(),
            ));
        };
        if matched {
            return Ok(retry_offset);
        }

        let partition_start = self.fetch_partition_start_offset().await?;
        if partition_start > retry_offset {
            return Err(ReaderError::OffsetReader(format!(
                "partition start offset {partition_start} is after repeated timestamp lookup result {retry_offset}"
            )));
        }
        if partition_start == retry_offset {
            return Ok(retry_offset);
        }

        warn!(
            resolved_offset = retry_offset,
            partition_start,
            consumer_group = %self.consumer_group,
            "max replay period timestamp lookup repeatedly matched no records despite a recent partition tail; clamping start offset to partition start to avoid skipping records"
        );
        Ok(partition_start)
    }

    /// Resolves `(start_offset, last_consumed_offset)` for startup.
    pub async fn get_start_offset(&self) -> Result<(i64, i64), ReaderError> {
        let topic = &self.kafka_cfg.topic;
        match self.kafka_cfg.consume_from_position_at_startup.as_str() {
            "start" => Ok((KAFKA_OFFSET_START, -1)),
            "end" => Ok((KAFKA_OFFSET_END, -1)),
            "timestamp" => {
                let ts = self.kafka_cfg.consume_from_timestamp_at_startup;
                if let Some((offset, _matched)) = self.fetch_first_offset_after_time(ts).await? {
                    return Ok((offset, offset - 1));
                }
                Ok((KAFKA_OFFSET_START, -1))
            }
            _ => {
                if self.kafka_cfg.consumer_group_offset_commit_file_enforced {
                    if self.max_replay_period.is_zero() {
                        return Err(ReaderError::Config(
                            "max replay period must be positive when file offset enforcement is enabled"
                                .to_string(),
                        ));
                    }
                    if let Some(ref f) = self.offset_file {
                        if let Some(file_offset) = f.read() {
                            match self
                                .client
                                .fetch_partition_start_offset(topic, self.partition_id)
                                .await
                            {
                                Ok(part_start) => {
                                    if file_offset >= part_start {
                                        return Ok((file_offset + 1, file_offset));
                                    }
                                    warn!(
                                        file_offset,
                                        part_start,
                                        "file-stored offset no longer exists for partition, resolving from max replay period or partition start"
                                    );
                                }
                                Err(KafkaClientError::Broker(
                                    kafka_protocol::error::ResponseError::UnknownTopicOrPartition,
                                )) => {
                                    warn!(
                                        file_offset,
                                        part_start = 0,
                                        "file-stored offset no longer exists for partition, resolving from max replay period or partition start"
                                    );
                                }
                                Err(e) => return Err(e.into()),
                            }
                        }
                    }

                    let now_ms = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as i64;
                    let cutoff_ms = now_ms - self.max_replay_period.as_millis() as i64;
                    if let Some((mut offset, matched)) =
                        self.fetch_first_offset_after_time(cutoff_ms).await?
                    {
                        if !matched {
                            offset = self
                                .resolve_offset_after_no_timestamp_match(cutoff_ms, offset)
                                .await?;
                        }
                        return Ok((offset, offset - 1));
                    }
                    Ok((KAFKA_OFFSET_START, -1))
                } else {
                    if let Some(committed) = self.fetch_last_committed_offset().await? {
                        return Ok((committed + 1, committed));
                    }
                    Ok((KAFKA_OFFSET_START, -1))
                }
            }
        }
    }

    /// Starts the partition reader (`SingleClusterPartitionReader.start`): resolves the start
    /// offset, starts the dependencies and the fetcher, consumes until the max consumer lag at
    /// startup is honored, then runs the fetch/consume loop in the background (`run`).
    pub async fn start(&self) -> Result<(), ReaderError> {
        let mut guard = self.task.lock().await;
        if guard.is_some() {
            return Ok(());
        }
        self.state
            .store(ReaderState::Starting as u8, Ordering::SeqCst);

        let fetch_loop = match self.start_inner().await {
            Ok(l) => l,
            Err(e) => {
                self.state.store(ReaderState::Failed as u8, Ordering::SeqCst);
                // Stop dependencies if the start fails.
                self.stop_dependencies().await;
                return Err(e);
            }
        };

        self.state
            .store(ReaderState::Running as u8, Ordering::SeqCst);
        let cancel = Cancel::new(self.stop_tx.subscribe(), None);
        let ctx = RuntimeFailureCtx {
            partition: self.partition_id,
            state: Arc::clone(&self.state),
            client: Arc::clone(&self.client),
            offset_watcher: Arc::clone(&self.offset_watcher),
            committer: Arc::clone(&self.committer),
            offset_reader: Arc::clone(&self.offset_reader),
            start_offset_reader: Arc::clone(&self.start_offset_reader),
            sampler: Arc::clone(&self.sampler),
            fetcher: Arc::clone(&self.fetcher),
            fallback_fetcher: Arc::clone(&fetch_loop.fetcher),
            stop_tx: self.stop_tx.clone(),
            failure_tx: self.failure_tx.clone(),
        };
        *guard = Some(tokio::spawn(async move {
            if let Err(e) = fetch_loop.run(cancel).await {
                // Fail the whole service in case of a non-recoverable error.
                error!(partition = ctx.partition, err = %e, "partition reader failed");
                ctx.fail_and_stop(e.to_string()).await;
            }
        }));
        info!(partition = self.partition_id, "started partition reader");
        Ok(())
    }

    async fn start_inner(&self) -> Result<Arc<FetchLoop>, ReaderError> {
        let startup_stop_rx = self.stop_tx.subscribe();
        let canceled = |rx: &watch::Receiver<bool>| rx.has_changed().is_err() || *rx.borrow();

        if self.kafka_cfg.auto_create_topic_enabled {
            let topic_ref = [self.kafka_cfg.topic.as_str()];
            let mut stop_rx = startup_stop_rx.clone();
            tokio::select! {
                biased;
                res = stop_rx.wait_for(|v| *v) => {
                    let _ = res;
                    return Err(ReaderError::Canceled);
                }
                r = create_topics(&self.kafka_cfg, &topic_ref) => {
                    r.map_err(ReaderError::Startup)?;
                }
            }
        }

        // getStartOffset(): resolving the start offset is retried with a dskit backoff
        // (min 100ms, max 2s, 10 retries); configuration errors are returned immediately.
        let (start_offset, last_consumed) = {
            let mut stop_rx = startup_stop_rx.clone();
            let mut retry = Backoff::new(BackoffConfig {
                min_backoff: Duration::from_millis(100),
                max_backoff: Duration::from_secs(2),
                max_retries: 10,
            });
            let mut last_err = None;
            let mut resolved = None;
            while retry.ongoing() {
                if canceled(&stop_rx) {
                    return Err(ReaderError::Canceled);
                }
                let mut call_stop_rx = startup_stop_rx.clone();
                let offset_res = tokio::select! {
                    biased;
                    res = call_stop_rx.wait_for(|v| *v) => {
                        let _ = res;
                        return Err(ReaderError::Canceled);
                    }
                    r = self.get_start_offset() => r,
                };
                match offset_res {
                    Ok(v) => {
                        resolved = Some(v);
                        break;
                    }
                    Err(e @ ReaderError::Config(_)) => return Err(e),
                    Err(e) => {
                        warn!(partition = self.partition_id, err = %e, "failed to fetch offset");
                        last_err = Some(e);
                    }
                }
                tokio::select! {
                    biased;
                    res = stop_rx.wait_for(|v| *v) => {
                        let _ = res;
                        return Err(ReaderError::Canceled);
                    }
                    _ = retry.wait() => {}
                }
            }
            match resolved {
                Some(v) => v,
                None => return Err(last_err.unwrap_or(ReaderError::Canceled)),
            }
        };

        // lastConsumedOffset could be a special negative offset (partition start / end).
        self.last_seen_offset.store(last_consumed, Ordering::SeqCst);
        // Initialise the last consumed offset only with an actual offset from the consumer group.
        if last_consumed >= 0 {
            self.offset_watcher.notify(last_consumed);
        }

        // It's ok to have the start offset slightly outdated: it is only needed if we fall
        // behind, or start, and the log gets truncated from beneath us.
        const START_OFFSET_READER_REFRESH_DURATION: Duration = Duration::from_secs(10);
        let oc = self.offset_client.clone();
        let topic = self.kafka_cfg.topic.clone();
        let partition_id = self.partition_id;
        let start_offset_reader = Arc::new(GenericOffsetReader::new(
            Arc::new(move || {
                let oc = oc.clone();
                let topic = topic.clone();
                Box::pin(async move {
                    oc.fetch_partition_start_offset(&topic, partition_id)
                        .await
                        .map_err(|e| e.to_string())
                })
            }),
            START_OFFSET_READER_REFRESH_DURATION,
        ));
        *self.start_offset_reader.lock().await = Some(Arc::clone(&start_offset_reader));
        // The offset readers fetch their first value synchronously while starting; don't let
        // that delay a stop requested meanwhile.
        {
            let mut stop_rx = startup_stop_rx.clone();
            tokio::select! {
                _ = stop_rx.wait_for(|v| *v) => return Err(ReaderError::Canceled),
                _ = async {
                    self.committer.start().await;
                    self.offset_reader.start().await;
                    start_offset_reader.start().await;
                } => {}
            }
        }
        *self.sampler.lock().await = Some(self.metrics.spawn_sampler(self.stop_tx.subscribe()));

        let fetcher = if self.kafka_cfg.fetch_concurrency_max > 0 {
            if !self.kafka_cfg.client_rack.is_empty() {
                // ConcurrentFetchers builds its own Fetch requests without a rack and always
                // reads from the partition leader.
                let msg = "the configured Kafka client rack has no effect because concurrent fetching is enabled: records are always fetched from the partition leader, regardless of the configured rack";
                warn!(
                    client_rack = %self.kafka_cfg.client_rack,
                    fetch_concurrency_max = self.kafka_cfg.fetch_concurrency_max,
                    "{msg}"
                );
                if let Some(ref sink) = self.log_sink {
                    sink.lock().push(msg.to_string());
                }
            }

            let mut stop_rx = startup_stop_rx.clone();
            let created = tokio::select! {
                _ = stop_rx.wait_for(|v| *v) => return Err(ReaderError::Canceled),
                f = ConcurrentFetchers::new(
                    Arc::clone(&self.client),
                    self.kafka_cfg.topic.clone(),
                    self.partition_id,
                    start_offset,
                    self.kafka_cfg.fetch_concurrency_max as usize,
                    self.kafka_cfg.max_buffered_bytes.clamp(0, i32::MAX as i64) as i32,
                    self.kafka_cfg.use_compressed_bytes_as_fetch_max_bytes,
                    self.concurrent_fetchers_min_bytes_max_wait_time,
                    &self.offset_client,
                    RangeErrorPolicy::ResumeFromStart,
                    Arc::clone(&start_offset_reader),
                    DEFAULT_FETCH_BACKOFF_CONFIG,
                    self.metrics.fetcher.clone(),
                ) => f,
            };
            let f = created.map_err(|e| {
                ReaderError::Startup(format!("creating concurrent fetchers during startup: {e}"))
            })?;
            ReaderFetcher::Concurrent(f)
        } else {
            // The Kafka client consumes the partition (it sends the client rack and honours
            // the broker's preferred read replica).
            ReaderFetcher::Client(
                ClientPartitionConsumer::new(
                    Arc::clone(&self.client),
                    self.kafka_cfg.topic.clone(),
                    self.partition_id,
                    start_offset,
                    go_duration(self.kafka_cfg.fetch_max_wait.as_nanos()),
                )
                .with_fetch_wait_duration(self.metrics.fetcher.fetch_wait_duration.clone()),
            )
        };
        let fetcher = Arc::new(fetcher);
        fetcher.start().await;
        *self.fetcher.lock().await = Some(Arc::clone(&fetcher));
        *self.metrics.source.write() = Some(Arc::clone(&fetcher));

        let fetch_loop = Arc::new(FetchLoop {
            partition_id: self.partition_id,
            topic: self.kafka_cfg.topic.clone(),
            fetcher,
            consumer: Arc::clone(&self.consumer),
            committer: Arc::clone(&self.committer),
            offset_watcher: Arc::clone(&self.offset_watcher),
            metrics: self.metrics.clone(),
            last_seen_offset: Arc::clone(&self.last_seen_offset),
            highest_ts: Arc::clone(&self.highest_consumed_ts_before_end_ms),
        });

        // Enforce the max consumer lag (if enabled).
        let target_lag = go_duration(self.kafka_cfg.target_consumer_lag_at_startup.as_nanos());
        let max_lag = go_duration(self.kafka_cfg.max_consumer_lag_at_startup.as_nanos());
        if !target_lag.is_zero() && !max_lag.is_zero() {
            if start_offset != KAFKA_OFFSET_END {
                self.process_next_fetches_until_target_or_max_lag_honored(
                    &fetch_loop,
                    target_lag,
                    max_lag,
                )
                .await?;
            } else {
                info!(partition = self.partition_id, "partition reader is skipping to consume partition until max consumer lag is honored because it's going to consume the partition from the end");
            }
        }

        Ok(fetch_loop)
    }

    /// `processNextFetchesUntilTargetOrMaxLagHonored`: consumes until at least the max lag is
    /// honored, best-effort reaching the target lag.
    async fn process_next_fetches_until_target_or_max_lag_honored(
        &self,
        fetch_loop: &FetchLoop,
        target_lag: Duration,
        max_lag: Duration,
    ) -> Result<(), ReaderError> {
        info!(partition = self.partition_id, ?target_lag, ?max_lag, "partition reader is starting to consume partition until target and max consumer lag is honored");

        let stop_rx = self.stop_tx.subscribe();
        let mut curr_lag = Duration::ZERO;
        for attempt in 0..3 {
            let (desired, deadline) = match attempt {
                // First process fetches until at least the max lag is honored.
                0 => (max_lag, None),
                // Then try to reach the (lower) target lag within 2x max lag (best-effort).
                1 => (target_lag, Some(tokio::time::Instant::now() + 2 * max_lag)),
                // Then guarantee the max lag is still honored.
                _ => (max_lag, None),
            };
            let mut cancel = Cancel::new(stop_rx.clone(), deadline);
            match self
                .process_next_fetches_until_lag_honored(fetch_loop, desired, &mut cancel)
                .await
            {
                Ok(l) => curr_lag = l,
                Err(ReaderError::WaitTargetLagDeadlineExceeded) => continue,
                Err(e) => return Err(e),
            }
            if curr_lag <= target_lag {
                let msg = "partition reader consumed partition and current lag is lower than configured target consumer lag";
                info!(
                    partition = self.partition_id,
                    last_consumed_offset = self.offset_watcher.last_consumed_offset(),
                    current_lag = ?curr_lag,
                    "{msg}"
                );
                if let Some(ref sink) = self.log_sink {
                    sink.lock().push(msg.to_string());
                }
                return Ok(());
            }
        }

        let msg = "partition reader consumed partition and current lag is lower than configured max consumer lag but higher than target consumer lag";
        warn!(
            partition = self.partition_id,
            last_consumed_offset = self.offset_watcher.last_consumed_offset(),
            current_lag = ?curr_lag,
            "{msg}"
        );
        if let Some(ref sink) = self.log_sink {
            sink.lock().push(msg.to_string());
        }
        Ok(())
    }

    /// `processNextFetchesUntilLagHonored`.
    async fn process_next_fetches_until_lag_honored(
        &self,
        fetch_loop: &FetchLoop,
        max_lag: Duration,
        cancel: &mut Cancel,
    ) -> Result<Duration, ReaderError> {
        // Retry forever (unless canceled / deadline exceeded).
        let mut boff = Backoff::new(BackoffConfig {
            min_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(1),
            max_retries: 0,
        });
        let mut curr_lag = Duration::ZERO;
        loop {
            if let Some(cause) = cancel.cause() {
                return Err(cause);
            }
            // Direct request to Kafka for the partition start offset.
            let partition_start = tokio::select! {
                biased;
                _ = cancel.clone().done() => {
                    return Err(cancel.cause().unwrap_or(ReaderError::Canceled));
                }
                res = self.offset_reader.fetch_partition_start_offset() => match res {
                    Ok(v) => v,
                    Err(e) => {
                        warn!(partition = self.partition_id, err = %e, "partition reader failed to fetch partition start offset");
                        cancel.sleep(boff.next_delay()).await;
                        continue;
                    }
                },
            };
            // Direct request for the last produced offset (no WaitNextFetchLastProducedOffset,
            // to not introduce further latency).
            let requested_at = Instant::now();
            let last_produced = tokio::select! {
                biased;
                _ = cancel.clone().done() => {
                    return Err(cancel.cause().unwrap_or(ReaderError::Canceled));
                }
                res = self.offset_reader.fetch_last_produced_offset() => match res {
                    Ok(v) => v,
                    Err(e) => {
                        warn!(partition = self.partition_id, err = %e, "partition reader failed to fetch last produced offset");
                        cancel.sleep(boff.next_delay()).await;
                        continue;
                    }
                },
            };

            // Ensure there are some records to consume (e.g. all records deleted by retention).
            if partition_start > last_produced {
                info!(partition = self.partition_id, partition_start_offset = partition_start, last_produced_offset = last_produced, "partition reader found no records to consume because partition is empty");
                return Ok(Duration::ZERO);
            }

            info!(
                partition = self.partition_id,
                current_lag = ?curr_lag,
                partition_start_offset = partition_start,
                last_produced_offset = last_produced,
                "partition reader is consuming records to honor target and max consumer lag"
            );

            // Continue reading until we reached the desired offset.
            while self.offset_watcher.last_consumed_offset() < last_produced {
                if let Some(cause) = cancel.cause() {
                    return Err(cause);
                }
                fetch_loop
                    .process_next_fetches(PHASE_STARTING, cancel)
                    .await?;
            }
            if let Some(cause) = cancel.cause() {
                return Err(cause);
            }

            // If it took less than the max desired lag to replay the partition then we can
            // stop here, otherwise we'll have to redo it.
            curr_lag = requested_at.elapsed();
            if curr_lag <= max_lag {
                return Ok(curr_lag);
            }
        }
    }

    pub async fn start_async(self: &Arc<Self>) {
        drop(self.start_async_with_result().await);
    }

    pub async fn start_async_with_result(
        self: &Arc<Self>,
    ) -> oneshot::Receiver<Result<(), ReaderError>> {
        let (tx, rx) = oneshot::channel();
        let mut guard = self.start_task.lock().await;
        if guard.is_some() {
            let _ = tx.send(Ok(()));
            return rx;
        }
        self.state
            .store(ReaderState::Starting as u8, Ordering::SeqCst);
        let this = Arc::clone(self);
        *guard = Some(tokio::spawn(async move {
            let res = this.start().await;
            match res {
                Ok(()) => {
                    let _ = tx.send(Ok(()));
                    Ok(())
                }
                Err(e) => {
                    let msg = e.to_string();
                    let _ = tx.send(Err(e));
                    Err(ReaderError::Startup(msg))
                }
            }
        }));
        rx
    }

    pub fn failure_error(&self) -> Option<String> {
        self.failure_tx.borrow().clone()
    }

    pub fn subscribe_failure(&self) -> watch::Receiver<Option<String>> {
        self.failure_tx.subscribe()
    }

    pub async fn stop(&self) {
        let was_failed = self.state() == ReaderState::Failed;
        if !was_failed {
            self.state
                .store(ReaderState::Stopping as u8, Ordering::SeqCst);
        }
        let _ = self.stop_tx.send(true);
        self.offset_watcher.stop();
        if let Some(sh) = self.start_task.lock().await.take() {
            let _ = sh.await;
        }
        if let Some(handle) = self.task.lock().await.take() {
            let _ = handle.await;
        }
        self.stop_dependencies().await;
        if !was_failed && self.state() != ReaderState::Failed {
            self.state
                .store(ReaderState::Terminated as u8, Ordering::SeqCst);
        }
    }

    #[cfg(test)]
    async fn simulate_runtime_failure_for_test(&self, err_msg: String) {
        let fallback_fetcher = self.fetcher.lock().await.clone();
        if let Some(fallback_fetcher) = fallback_fetcher {
            RuntimeFailureCtx {
                partition: self.partition_id,
                state: Arc::clone(&self.state),
                client: Arc::clone(&self.client),
                offset_watcher: Arc::clone(&self.offset_watcher),
                committer: Arc::clone(&self.committer),
                offset_reader: Arc::clone(&self.offset_reader),
                start_offset_reader: Arc::clone(&self.start_offset_reader),
                sampler: Arc::clone(&self.sampler),
                fetcher: Arc::clone(&self.fetcher),
                fallback_fetcher,
                stop_tx: self.stop_tx.clone(),
                failure_tx: self.failure_tx.clone(),
            }
            .fail_and_stop(err_msg)
            .await;
        }
    }

    /// `stopDependencies`: stops the committer (final commit), the offset readers, the metrics
    /// sampler, then the fetcher (buffered records are dropped).
    async fn stop_dependencies(&self) {
        let _ = self.stop_tx.send(true);
        self.committer.stop().await;
        self.offset_reader.stop().await;
        let start_offset_reader = self.start_offset_reader.lock().await.take();
        if let Some(r) = start_offset_reader {
            r.stop().await;
        }
        let sampler = self.sampler.lock().await.take();
        if let Some(h) = sampler {
            let _ = h.await;
        }
        let fetcher = self.fetcher.lock().await.take();
        if let Some(f) = fetcher {
            f.stop().await;
        }
        self.client.close().await;
    }

    /// Waits until `target_offset` has been consumed by this reader, or `timeout` expires.
    pub async fn wait_for_offset(
        &self,
        target_offset: i64,
        timeout: Duration,
    ) -> Result<(), OffsetWatcherError> {
        self.offset_watcher.wait_for_offset(target_offset, timeout).await
    }

    /// Waits until all data up until the offset of Kafka cluster 0 has been consumed.
    pub async fn wait_read_consistency_until_offsets(
        &self,
        offsets: &PartitionOffsets,
        timeout: Duration,
    ) -> Result<(), ReaderError> {
        if offsets.num_kafka_clusters() != 1 {
            return Err(ReaderError::InvalidClusterOffsetCount(
                offsets.num_kafka_clusters(),
            ));
        }
        let target = offsets.for_kafka_cluster(0);
        self.wait_read_consistency(true, timeout, || async move { Ok(target) })
            .await
    }

    /// Waits until all records produced up to the next polled last-produced-offset have been consumed.
    pub async fn wait_read_consistency_until_last_produced_offset(
        &self,
        timeout: Duration,
    ) -> Result<(), ReaderError> {
        self.wait_read_consistency(false, timeout, || async move {
            self.offset_reader
                .wait_next_fetch_last_produced_offset()
                .await
                .map_err(ReaderError::OffsetReader)
        })
        .await
    }

    async fn wait_read_consistency<F, Fut>(
        &self,
        with_offset: bool,
        caller_timeout: Duration,
        get_offset: F,
    ) -> Result<(), ReaderError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<i64, ReaderError>>,
    {
        let topic = self.kafka_cfg.topic.as_str();
        struct StrongConsistencyGuard<'a> {
            metrics: &'a ReaderMetrics,
            topic: &'a str,
            start: Instant,
            finished: bool,
            failed: bool,
        }

        impl Drop for StrongConsistencyGuard<'_> {
            fn drop(&mut self) {
                // Go `ObserveStrongReadConsistency`: skip latency & failure tracking on
                // cancellation (including dropped sibling futures in MultiClusterPartitionReader).
                if !self.finished {
                    return;
                }
                self.metrics
                    .strong_consistency
                    .latency
                    .with_label_values(&[self.topic])
                    .observe(self.start.elapsed().as_secs_f64());
                if self.failed {
                    self.metrics
                        .strong_consistency
                        .failures
                        .with_label_values(&[self.topic])
                        .inc();
                }
            }
        }

        self.metrics
            .strong_consistency
            .requests
            .with_label_values(&[if with_offset { "true" } else { "false" }, topic])
            .inc();
        let start = Instant::now();
        let mut guard = StrongConsistencyGuard {
            metrics: &self.metrics,
            topic,
            start,
            finished: false,
            failed: false,
        };

        let configured_timeout = Duration::from_nanos(
            self.kafka_cfg
                .wait_strong_read_consistency_timeout
                .as_nanos()
                .max(0) as u64,
        );
        let (effective_timeout, is_configured) = if !configured_timeout.is_zero()
            // Go `context.WithTimeoutCause(ctx, WaitStrongReadConsistencyTimeout, ...)`: the
            // configured timeout's cause wins unless the caller's deadline is strictly earlier.
            && (caller_timeout.is_zero() || configured_timeout <= caller_timeout)
        {
            (configured_timeout, true)
        } else {
            (caller_timeout, false)
        };

        let res = async {
            let cur_state = self.state();
            if cur_state != ReaderState::Running {
                return Err(ReaderError::NotRunning(cur_state));
            }
            let target = if effective_timeout.is_zero() {
                get_offset().await?
            } else {
                tokio::time::timeout(effective_timeout, get_offset())
                    .await
                    .map_err(|_| {
                        if is_configured {
                            ReaderError::WaitStrongReadConsistencyTimeoutExceeded
                        } else {
                            ReaderError::ContextDeadlineExceeded
                        }
                    })??
            };

            let rem = if effective_timeout.is_zero() {
                Duration::ZERO
            } else {
                effective_timeout.saturating_sub(start.elapsed())
            };
            if !effective_timeout.is_zero() && rem.is_zero() {
                return Err(if is_configured {
                    ReaderError::WaitStrongReadConsistencyTimeoutExceeded
                } else {
                    ReaderError::ContextDeadlineExceeded
                });
            }

            self.offset_watcher
                .wait_for_offset(target, rem)
                .await
                .map_err(|e| match e {
                    OffsetWatcherError::Timeout(_) if is_configured => {
                        ReaderError::WaitStrongReadConsistencyTimeoutExceeded
                    }
                    OffsetWatcherError::Timeout(_) => ReaderError::ContextDeadlineExceeded,
                    other => ReaderError::Watcher(other),
                })
        }
        .await;

        guard.failed = res.is_err();
        guard.finished = true;
        drop(guard);
        res
    }

    pub fn enforce_read_max_delay(&self, max_delay: Duration) -> Result<(), ReaderError> {
        if max_delay.is_zero() {
            return Ok(());
        }
        let ts_ms = self.highest_consumed_ts_before_end_ms.load(Ordering::SeqCst);
        if ts_ms == UNSET_TS_MS {
            return Ok(());
        }
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        if (now_ms as i128 - ts_ms as i128) > max_delay.as_millis() as i128 {
            return Err(ReaderError::MaxDelayExceeded);
        }
        Ok(())
    }
}


const UNSET_TS_MS: i64 = i64::MIN;

/// Computes `now.Sub(ts).Seconds()` matching Go `time.Time.Sub`, clamping to `[i64::MIN, i64::MAX]` ns.
#[inline]
fn elapsed_secs_from_ms(now_ms: i64, ts_ms: i64) -> f64 {
    let ns = ((now_ms as i128 - ts_ms as i128) * 1_000_000)
        .clamp(i64::MIN as i128, i64::MAX as i128);
    (ns as f64) / 1e9
}

/// dskit `backoff.Config` used by `ConcurrentFetchers` for Fetch errors
/// (`defaultFetchBackoffConfig` in `config.go`: retry forever).
pub const DEFAULT_FETCH_BACKOFF_CONFIG: BackoffConfig = BackoffConfig {
    min_backoff: Duration::from_millis(250),
    max_backoff: Duration::from_secs(2),
    max_retries: 0,
};

fn go_duration(nanos: i64) -> Duration {
    Duration::from_nanos(nanos.max(0) as u64)
}

/// The reader context: canceled when the reader stops, or past `deadline`
/// (`context.WithTimeoutCause(ctx, 2*maxLag, errWaitTargetLagDeadlineExceeded)`).
#[derive(Clone)]
struct Cancel {
    stop_rx: watch::Receiver<bool>,
    deadline: Option<tokio::time::Instant>,
}

impl Cancel {
    fn new(stop_rx: watch::Receiver<bool>, deadline: Option<tokio::time::Instant>) -> Self {
        Self { stop_rx, deadline }
    }

    /// `context.Cause(ctx)`, or `None` if not canceled.
    fn cause(&self) -> Option<ReaderError> {
        if *self.stop_rx.borrow() {
            return Some(ReaderError::Canceled);
        }
        if self
            .deadline
            .is_some_and(|d| tokio::time::Instant::now() >= d)
        {
            return Some(ReaderError::WaitTargetLagDeadlineExceeded);
        }
        None
    }

    /// Completes once canceled.
    async fn done(mut self) {
        let deadline = self.deadline;
        let stop = async move {
            let _ = self.stop_rx.wait_for(|v| *v).await;
        };
        match deadline {
            Some(d) => {
                tokio::select! {
                    _ = stop => {}
                    _ = tokio::time::sleep_until(d) => {}
                }
            }
            None => stop.await,
        }
    }

    /// Sleeps for `d` unless canceled first.
    async fn sleep(&self, d: Duration) {
        tokio::select! {
            _ = self.clone().done() => {}
            _ = tokio::time::sleep(d) => {}
        }
    }
}

/// The fetch / consume loop shared by the startup catch-up and the running phase.
struct RuntimeFailureCtx {
    partition: i32,
    state: Arc<AtomicU8>,
    client: Arc<KafkaClient>,
    offset_watcher: Arc<PartitionOffsetWatcher>,
    committer: Arc<PartitionCommitter>,
    offset_reader: Arc<SingleClusterPartitionOffsetReader>,
    start_offset_reader: Arc<Mutex<Option<Arc<GenericOffsetReader<i64>>>>>,
    sampler: Arc<Mutex<Option<JoinHandle<()>>>>,
    fetcher: Arc<Mutex<Option<Arc<ReaderFetcher>>>>,
    fallback_fetcher: Arc<ReaderFetcher>,
    stop_tx: watch::Sender<bool>,
    failure_tx: watch::Sender<Option<String>>,
}

impl RuntimeFailureCtx {
    async fn fail_and_stop(self, err_msg: String) {
        self.state.store(ReaderState::Failed as u8, Ordering::SeqCst);
        self.offset_watcher.stop();
        let _ = self.stop_tx.send(true);
        self.committer.stop().await;
        self.offset_reader.stop().await;
        let start_offset_reader = self.start_offset_reader.lock().await.take();
        if let Some(r) = start_offset_reader {
            r.stop().await;
        }
        let sampler = self.sampler.lock().await.take();
        if let Some(h) = sampler {
            let _ = h.await;
        }
        let fetcher = self.fetcher.lock().await.take();
        if let Some(f) = fetcher {
            f.stop().await;
        } else {
            self.fallback_fetcher.stop().await;
        }
        self.client.close().await;
        let _ = self.failure_tx.send(Some(err_msg));
    }
}

struct FetchLoop {
    partition_id: i32,
    topic: String,
    fetcher: Arc<ReaderFetcher>,
    consumer: Arc<dyn RecordBatchConsumer>,
    committer: Arc<PartitionCommitter>,
    offset_watcher: Arc<PartitionOffsetWatcher>,
    metrics: ReaderMetrics,
    last_seen_offset: Arc<AtomicI64>,
    highest_ts: Arc<AtomicI64>,
}

impl FetchLoop {
    /// `SingleClusterPartitionReader.run`.
    async fn run(&self, cancel: Cancel) -> Result<(), ReaderError> {
        let mut cancel = cancel;
        while cancel.cause().is_none() {
            match self.process_next_fetches(PHASE_RUNNING, &mut cancel).await {
                Ok(()) | Err(ReaderError::Canceled) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// `processNextFetches`.
    async fn process_next_fetches(
        &self,
        phase: &str,
        cancel: &mut Cancel,
    ) -> Result<(), ReaderError> {
        let fetches = self.fetcher.poll_fetches(cancel.clone().done()).await;
        self.record_fetches_metrics(&fetches, phase);
        self.log_fetch_errors(&fetches);
        // filterOutErrFetches
        let fetches: Vec<FetchResult> = fetches.into_iter().filter(|f| f.err.is_none()).collect();

        // Instrument only after all the pre-processing of records. This is the only place
        // that updates the last seen offset.
        let last_seen = self.last_seen_offset.load(Ordering::SeqCst);
        let all: Vec<&FetchedRecord> = fetches.iter().flat_map(|f| f.records.iter()).collect();
        instrument_gaps(
            &find_gaps_in_record_refs(&all, last_seen),
            &self.metrics.missed_records,
        );
        // `lastOffset(fetches)` is 0 when there is no record.
        let last_offset = all.last().map_or(0, |r| r.offset);
        self.last_seen_offset
            .store(last_seen.max(last_offset), Ordering::SeqCst);

        self.update_highest_consumed_timestamp_before_consumption(&fetches);
        let num_records = all.len();
        drop(all);
        self.consume_fetches(&fetches, cancel)
            .await
            .map_err(|e| match e {
                ReaderError::Canceled | ReaderError::WaitTargetLagDeadlineExceeded => e,
                other => ReaderError::Startup(format!("consume {num_records} records: {other}")),
            })?;
        self.enqueue_commit(&fetches);
        self.notify_last_consumed_offset(&fetches);
        self.update_highest_consumed_timestamp_after_consumption(&fetches, phase);
        Ok(())
    }

    /// `recordFetchesMetrics`.
    fn record_fetches_metrics(&self, fetches: &[FetchResult], phase: &str) {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        let observer = self.metrics.receive_delay.with_label_values(&[phase]);
        let mut num_records = 0usize;
        for r in fetches.iter().flat_map(|f| f.records.iter()) {
            num_records += 1;
            observer.observe(elapsed_secs_from_ms(now_ms, r.timestamp_ms));
        }
        self.metrics.fetches_total.inc_by(fetches.len() as f64);
        self.metrics.records_per_fetch.observe(num_records as f64);
    }

    /// `logFetchErrors`.
    fn log_fetch_errors(&self, fetches: &[FetchResult]) {
        let errs: Vec<String> = fetches
            .iter()
            .filter_map(|f| f.err.as_ref().map(|e| (f.partition, e)))
            .map(|(p, e)| format!("topic {:?}, partition {p}: {e}", self.topic))
            .collect();
        if errs.is_empty() {
            return;
        }
        self.metrics.fetches_errors.inc_by(errs.len() as f64);
        error!(partition = self.partition_id, err = %errs.join("; "), "encountered error while fetching");
    }

    /// `updateHighestConsumedTimestampBeforeConsumption`.
    fn update_highest_consumed_timestamp_before_consumption(&self, fetches: &[FetchResult]) {
        if self.highest_ts.load(Ordering::SeqCst) != UNSET_TS_MS {
            return;
        }
        for f in fetches {
            if f.partition != self.partition_id {
                continue;
            }
            if let Some(first) = f.records.first() {
                self.highest_ts.store(first.timestamp_ms, Ordering::SeqCst);
            }
        }
    }

    /// `updateHighestConsumedTimestampAfterConsumption`.
    fn update_highest_consumed_timestamp_after_consumption(&self, fetches: &[FetchResult], phase: &str) {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        let observer = self
            .metrics
            .receive_and_consume_delay
            .with_label_values(&[phase]);
        let mut highest = UNSET_TS_MS;
        for f in fetches {
            if f.partition != self.partition_id || f.records.is_empty() {
                continue;
            }
            for r in &f.records {
                observer.observe(elapsed_secs_from_ms(now_ms, r.timestamp_ms));
                highest = highest.max(r.timestamp_ms);
            }
            let last = f.records.last().expect("non-empty");
            if last.offset + 1 >= f.high_watermark {
                self.highest_ts.store(UNSET_TS_MS, Ordering::SeqCst);
            } else {
                self.highest_ts.store(highest, Ordering::SeqCst);
            }
        }
    }

    /// `consumeFetches`: retries forever (250ms..2s backoff) until canceled. An in-flight
    /// consumption is never interrupted.
    async fn consume_fetches(
        &self,
        fetches: &[FetchResult],
        cancel: &Cancel,
    ) -> Result<(), ReaderError> {
        let num: usize = fetches.iter().map(|f| f.records.len()).sum();
        if num == 0 {
            return Ok(());
        }
        let collect = || -> Vec<FetchedRecord> {
            fetches
                .iter()
                .flat_map(|f| f.records.iter().cloned())
                .collect()
        };
        let mut min_offset = i64::MAX;
        let mut max_offset = 0i64;
        for r in fetches.iter().flat_map(|f| f.records.iter()) {
            min_offset = min_offset.min(r.offset);
            max_offset = max_offset.max(r.offset);
        }

        let consume_start = Instant::now();
        let mut boff = Backoff::new(BackoffConfig {
            min_backoff: Duration::from_millis(250),
            max_backoff: Duration::from_secs(2),
            max_retries: 0,
        });
        let res = loop {
            // The records are rebuilt from the fetches for every attempt (the consumer takes
            // ownership); an in-flight attempt is never interrupted by cancellation.
            match self.consumer.consume(collect()).await {
                Ok(()) => break Ok(()),
                Err(err) => {
                    error!(
                        partition = self.partition_id,
                        err = %err,
                        record_min_offset = min_offset,
                        record_max_offset = max_offset,
                        num_retries = boff.num_retries(),
                        "encountered error while ingesting data from Kafka; should retry"
                    );
                    let delay = boff.next_delay();
                    if let Some(cause) = cancel.cause() {
                        break Err(cause);
                    }
                    cancel.sleep(delay).await;
                    if let Some(cause) = cancel.cause() {
                        break Err(cause);
                    }
                }
            }
        };
        self.metrics
            .consume_latency
            .observe(consume_start.elapsed().as_secs_f64());
        res
    }

    /// `enqueueCommit`.
    fn enqueue_commit(&self, fetches: &[FetchResult]) {
        let mut last = None;
        for f in fetches {
            if f.records.is_empty() {
                continue;
            }
            if f.partition != self.partition_id {
                error!(partition = f.partition, expected_partition = self.partition_id, "asked to commit wrong partition");
                continue;
            }
            last = f.records.last().map(|r| r.offset);
        }
        if let Some(o) = last {
            self.committer.enqueue_offset(o);
        }
    }

    /// `notifyLastConsumedOffset`.
    fn notify_last_consumed_offset(&self, fetches: &[FetchResult]) {
        for f in fetches {
            if f.partition != self.partition_id {
                continue;
            }
            let Some(rec) = f.records.last() else {
                continue;
            };
            self.offset_watcher.notify(rec.offset);
            self.metrics
                .last_consumed_offset
                .with_label_values(&[&self.partition_id.to_string()])
                .set(rec.offset as f64);
        }
    }
}

/// `findGapsInRecords` over the records of all fetches.
fn find_gaps_in_record_refs(records: &[&FetchedRecord], mut last_seen_offset: i64) -> Vec<OffsetRange> {
    let mut gaps = Vec::new();
    if last_seen_offset < 0 {
        if let Some(first) = records.first() {
            // No offset seen yet: assume the first record is the first available offset.
            last_seen_offset = first.offset;
        }
    }
    for r in records {
        if r.offset > last_seen_offset + 1 {
            gaps.push(OffsetRange {
                start: last_seen_offset + 1,
                end: r.offset,
            });
        }
        last_seen_offset = r.offset;
    }
    gaps
}

/// `instrumentGaps`.
pub fn instrument_gaps(gaps: &[OffsetRange], missed_records: &Counter) {
    for gap in gaps {
        error!(
            records_offset_gap_start_inclusive = gap.start,
            records_offset_gap_end_exclusive = gap.end,
            "there is a gap in consumed offsets; it is likely that there was data loss; see runbook for MimirIngesterMissedRecordsFromKafka"
        );
        missed_records.inc_by(gap.num_offsets() as f64);
    }
}

impl ReaderError {
    /// Go `errors.Is(err, context.DeadlineExceeded)` (gRPC status `DeadlineExceeded`).
    pub fn is_deadline_exceeded(&self) -> bool {
        match self {
            Self::ContextDeadlineExceeded
            | Self::WaitStrongReadConsistencyTimeoutExceeded
            | Self::WaitTargetLagDeadlineExceeded
            | Self::Watcher(OffsetWatcherError::Timeout(_)) => true,
            Self::WriteCompartment { source, .. } => source.is_deadline_exceeded(),
            Self::Multi(errs) => errs.iter().any(Self::is_deadline_exceeded),
            _ => false,
        }
    }
}

/// dskit `nonNilMultiError.Error()`.
fn format_multi_error(errs: &[ReaderError]) -> String {
    let mut out = String::new();
    if errs.len() > 1 {
        out.push_str(&format!("{} errors: ", errs.len()));
    }
    for (i, e) in errs.iter().enumerate() {
        if i != 0 {
            out.push_str("; ");
        }
        out.push_str(&e.to_string());
    }
    out
}

fn wrap_write_compartment(id: usize, res: Result<(), ReaderError>) -> Result<(), ReaderError> {
    res.map_err(|e| ReaderError::WriteCompartment {
        id,
        source: Box::new(e),
    })
}

/// Builds the downstream consumer of a partition reader, registering its metrics (Go
/// `NewPusherConsumerMetrics(reg)`) into the given registry.
pub type RecordBatchConsumerFactory<'a> =
    dyn Fn(&KafkaConfig, Option<&Registry>) -> Arc<dyn RecordBatchConsumer> + 'a;

/// Multi-cluster partition reader consuming the same partition ID across multiple Kafka write
/// compartments, optionally merging records in timestamp order via [`HeapMerger`].
pub struct MultiClusterPartitionReader {
    readers: Vec<Arc<SingleClusterPartitionReader>>,
    merger: Option<Arc<HeapMerger>>,
    state: Arc<AtomicU8>,
    failure_error: Arc<SyncMutex<Option<String>>>,
    stop_tx: watch::Sender<bool>,
    watcher_task: Mutex<Option<JoinHandle<()>>>,
}

impl MultiClusterPartitionReader {
    pub fn new(
        cluster_configs: &[KafkaConfig],
        ordered_cfg: &OrderedConsumptionConfig,
        partition_id: i32,
        instance_id: &str,
        offset_file_paths: Vec<Option<PathBuf>>,
        consumer: Arc<dyn RecordBatchConsumer>,
        registry: Option<&Registry>,
    ) -> Result<Self, ReaderError> {
        Self::new_with_notifier(
            cluster_configs,
            ordered_cfg,
            partition_id,
            instance_id,
            offset_file_paths,
            &|_, _| Arc::clone(&consumer),
            Arc::new(NoOpPreCommitNotifier),
            Duration::ZERO,
            registry,
        )
    }

    /// `consumer_factory` builds the downstream consumer: once on `registry` when records are
    /// merged across clusters (ordered consumption), otherwise once per Kafka cluster on that
    /// cluster's `write_compartment="<id>"`-labelled registerer, like Go.
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_notifier(
        cluster_configs: &[KafkaConfig],
        ordered_cfg: &OrderedConsumptionConfig,
        partition_id: i32,
        instance_id: &str,
        offset_file_paths: Vec<Option<PathBuf>>,
        consumer_factory: &RecordBatchConsumerFactory<'_>,
        notifier: Arc<dyn PreCommitNotifier>,
        max_replay_period: Duration,
        registry: Option<&Registry>,
    ) -> Result<Self, ReaderError> {
        if cluster_configs.is_empty() {
            return Err(ReaderError::Config(
                "at least one Kafka cluster must be configured".to_string(),
            ));
        }
        if offset_file_paths.len() != cluster_configs.len() {
            return Err(ReaderError::Config(
                "offset_file_paths length must match cluster_configs length".to_string(),
            ));
        }

        let merger = if ordered_cfg.enabled && cluster_configs.len() > 1 {
            // The downstream push path is shared across all clusters, so its consumer metrics
            // are registered once on the base registerer rather than per cluster.
            let downstream = consumer_factory(&cluster_configs[0], registry);
            let cb = Arc::new(move |recs: Vec<FetchedRecord>| {
                let d = Arc::clone(&downstream);
                Box::pin(async move { d.consume(recs).await })
                    as Pin<Box<dyn Future<Output = Result<(), String>> + Send>>
            });
            Some(HeapMerger::new(
                ordered_cfg,
                cb,
                HeapMergerMetrics::new(registry),
            ))
        } else {
            None
        };

        let mut readers = Vec::with_capacity(cluster_configs.len());
        let mut merger_opt = merger;
        let shared_merger = merger_opt.take().map(Arc::new);

        let build_res: Result<(), ReaderError> = (|| {
            for (cluster_id, (cfg, off_path)) in cluster_configs
                .iter()
                .zip(offset_file_paths)
                .enumerate()
            {
                // Go: `prometheus.WrapRegistererWith({write_compartment: <id>}, reg)`.
                let cluster_registry = registry.map(|_| Registry::new());
                let cluster_consumer: Arc<dyn RecordBatchConsumer> =
                    if let Some(ref m) = shared_merger {
                        struct MergerSubmitter {
                            merger: Arc<HeapMerger>,
                            cluster_id: usize,
                        }
                        impl RecordBatchConsumer for MergerSubmitter {
                            fn consume(
                                &self,
                                records: Vec<FetchedRecord>,
                            ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>>
                            {
                                Box::pin(async move {
                                    self.merger.submit_and_wait(self.cluster_id, records).await
                                })
                            }
                        }
                        Arc::new(MergerSubmitter {
                            merger: Arc::clone(m),
                            cluster_id,
                        })
                    } else {
                        consumer_factory(cfg, cluster_registry.as_ref())
                    };

                let cluster_instance = format!("{instance_id}-wc-{cluster_id}");
                let mut r = SingleClusterPartitionReader::new_with_notifier(
                    cfg.clone(),
                    partition_id,
                    &cluster_instance,
                    off_path,
                    cluster_consumer,
                    Arc::clone(&notifier),
                    cluster_registry.as_ref(),
                )?;
                if !max_replay_period.is_zero() {
                    r = r.with_max_replay_period(max_replay_period);
                }
                if let (Some(parent), Some(child)) = (registry, cluster_registry) {
                    let id = cluster_id.to_string();
                    register_labeled_child(parent, child, &[("write_compartment", id.as_str())])
                        .map_err(|e| {
                            ReaderError::Config(format!(
                                "creating partition reader for write compartment {cluster_id}: {e}"
                            ))
                        })?;
                }
                readers.push(Arc::new(r));
            }
            Ok(())
        })();

        if let Err(e) = build_res {
            if let Some(ref m) = shared_merger {
                m.stop_async();
            }
            return Err(e);
        }

        let (stop_tx, _) = watch::channel(false);
        Ok(Self {
            readers,
            merger: shared_merger,
            state: Arc::new(AtomicU8::new(ReaderState::New as u8)),
            failure_error: Arc::new(SyncMutex::new(None)),
            stop_tx,
            watcher_task: Mutex::new(None),
        })
    }

    pub fn state(&self) -> ReaderState {
        match self.state.load(Ordering::SeqCst) {
            0 => ReaderState::New,
            1 => ReaderState::Starting,
            2 => ReaderState::Running,
            3 => ReaderState::Stopping,
            4 => ReaderState::Terminated,
            _ => ReaderState::Failed,
        }
    }

    pub fn failure_error(&self) -> Option<String> {
        self.failure_error.lock().clone()
    }

    pub async fn start(&self) -> Result<(), ReaderError> {
        self.state
            .store(ReaderState::Starting as u8, Ordering::SeqCst);
        if let Some(ref m) = self.merger {
            m.ensure_started();
        }
        let rxs: Vec<(usize, watch::Receiver<Option<String>>)> = self
            .readers
            .iter()
            .enumerate()
            .map(|(cid, r)| (cid, r.subscribe_failure()))
            .collect();
        let merger_rx: Option<watch::Receiver<Option<String>>> =
            self.merger.as_ref().map(|m| m.subscribe_failure());
        let mut pending = Vec::with_capacity(self.readers.len());
        for r in &self.readers {
            pending.push(r.start_async_with_result().await);
        }
        let mut first_err: Option<ReaderError> = None;
        while !pending.is_empty() {
            let (res, _idx, remaining) = futures::future::select_all(pending).await;
            pending = remaining;
            match res {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    first_err = Some(e);
                    break;
                }
                Err(_) => {
                    first_err = Some(ReaderError::Startup(
                        "partition reader start task panicked".to_string(),
                    ));
                    break;
                }
            }
        }
        if first_err.is_none() {
            if let Some(ref mrx) = merger_rx {
                if let Some(err) = mrx.borrow().clone() {
                    first_err = Some(ReaderError::Startup(format!(
                        "a multi-cluster partition reader subservice failed (heap merger): {err}"
                    )));
                }
            }
        }
        if let Some(e) = first_err {
            self.state
                .store(ReaderState::Failed as u8, Ordering::SeqCst);
            *self.failure_error.lock() = Some(e.to_string());
            self.stop().await;
            return Err(e);
        }

        self.state
            .store(ReaderState::Running as u8, Ordering::SeqCst);

        let handle = self.spawn_watcher_task(rxs, merger_rx);
        *self.watcher_task.lock().await = Some(handle);
        Ok(())
    }

    fn spawn_watcher_task(
        &self,
        mut rxs: Vec<(usize, watch::Receiver<Option<String>>)>,
        mut merger_rx: Option<watch::Receiver<Option<String>>>,
    ) -> JoinHandle<()> {
        let readers = self.readers.clone();
        let merger = self.merger.clone();
        let state = Arc::clone(&self.state);
        let failure_error = Arc::clone(&self.failure_error);
        let mut stop_rx = self.stop_tx.subscribe();
        tokio::spawn(async move {
            loop {
                if *stop_rx.borrow() {
                    return;
                }
                for (cid, rx) in &rxs {
                    let err_opt = rx.borrow().clone();
                    if let Some(err) = err_opt {
                        let msg = format!(
                            "a multi-cluster partition reader subservice failed (write compartment {cid}): {err}"
                        );
                        error!(write_compartment = cid, err = %err, "multi-cluster partition reader subservice failed");
                        *failure_error.lock() = Some(msg);
                        state.store(ReaderState::Failed as u8, Ordering::SeqCst);
                        join_all(readers.iter().map(|r| r.stop())).await;
                        if let Some(ref m) = merger {
                            m.shutdown_and_drain().await;
                        }
                        return;
                    }
                }
                if let Some(ref mrx) = merger_rx {
                    let err_opt = mrx.borrow().clone();
                    if let Some(err) = err_opt {
                        let msg = format!(
                            "a multi-cluster partition reader subservice failed (heap merger): {err}"
                        );
                        error!(err = %err, "multi-cluster partition reader subservice failed");
                        *failure_error.lock() = Some(msg);
                        state.store(ReaderState::Failed as u8, Ordering::SeqCst);
                        join_all(readers.iter().map(|r| r.stop())).await;
                        if let Some(ref m) = merger {
                            m.shutdown_and_drain().await;
                        }
                        return;
                    }
                }

                let wait_futs = rxs.iter_mut().map(|(_, rx)| Box::pin(rx.changed()));
                let merger_changed = async {
                    match merger_rx.as_mut() {
                        Some(rx) => rx.changed().await,
                        None => std::future::pending().await,
                    }
                };
                tokio::select! {
                    res = stop_rx.changed() => {
                        if res.is_err() || *stop_rx.borrow() {
                            return;
                        }
                    }
                    _ = futures::future::select_all(wait_futs) => {}
                    res = merger_changed => {
                        if res.is_err() {
                            let msg = "a multi-cluster partition reader subservice failed (heap merger): channel closed".to_string();
                            error!("multi-cluster partition reader heap merger failure channel closed unexpectedly");
                            *failure_error.lock() = Some(msg);
                            state.store(ReaderState::Failed as u8, Ordering::SeqCst);
                            join_all(readers.iter().map(|r| r.stop())).await;
                            if let Some(ref m) = merger {
                                m.shutdown_and_drain().await;
                            }
                            return;
                        }
                    }
                }
            }
        })
    }

    pub async fn stop(&self) {
        let was_failed = self.state() == ReaderState::Failed;
        if !was_failed {
            self.state
                .store(ReaderState::Stopping as u8, Ordering::SeqCst);
        }
        let _ = self.stop_tx.send(true);
        if let Some(h) = self.watcher_task.lock().await.take() {
            let _ = h.await;
        }
        let stop_futs = self.readers.iter().map(|r| r.stop());
        join_all(stop_futs).await;
        if let Some(ref m) = self.merger {
            m.shutdown_and_drain().await;
        }
        if !was_failed && self.state() != ReaderState::Failed {
            self.state
                .store(ReaderState::Terminated as u8, Ordering::SeqCst);
        }
    }

    pub fn last_seen_offsets(&self) -> PartitionOffsets {
        PartitionOffsets::new_multi_cluster(
            self.readers.iter().map(|r| r.last_seen_offset()).collect(),
        )
    }

    pub async fn wait_read_consistency_until_offsets(
        &self,
        offsets: &PartitionOffsets,
        timeout: Duration,
    ) -> Result<(), ReaderError> {
        if offsets.num_kafka_clusters() != self.readers.len() {
            return Err(ReaderError::ClusterCountMismatch {
                expected: self.readers.len(),
                got: offsets.num_kafka_clusters(),
            });
        }
        // Each per-cluster offset goes through that cluster's reader as-is (Running check,
        // timeout error and strong consistency metrics); the first failure drops the others.
        let futs = self.readers.iter().enumerate().map(|(cid, r)| {
            let target = PartitionOffsets::new_single_cluster(offsets.for_kafka_cluster(cid));
            async move {
                wrap_write_compartment(
                    cid,
                    r.wait_read_consistency_until_offsets(&target, timeout).await,
                )
            }
        });
        try_join_all(futs).await?;
        Ok(())
    }

    pub async fn wait_read_consistency_until_last_produced_offset(
        &self,
        timeout: Duration,
    ) -> Result<(), ReaderError> {
        let futs = self.readers.iter().enumerate().map(|(cid, r)| async move {
            wrap_write_compartment(
                cid,
                r.wait_read_consistency_until_last_produced_offset(timeout).await,
            )
        });
        try_join_all(futs).await?;
        Ok(())
    }

    /// The ingester is caught up only when all Kafka clusters are.
    pub fn enforce_read_max_delay(&self, max_delay: Duration) -> Result<(), ReaderError> {
        let errs: Vec<ReaderError> = self
            .readers
            .iter()
            .enumerate()
            .filter_map(|(cid, r)| {
                wrap_write_compartment(cid, r.enforce_read_max_delay(max_delay)).err()
            })
            .collect();
        if errs.is_empty() {
            Ok(())
        } else {
            Err(ReaderError::Multi(errs))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mimir_config::types::StringSliceCSV;

    #[test]
    fn consumer_group_matches_upstream_single_and_multi_cluster() {
        struct NoOpConsumer;
        impl RecordBatchConsumer for NoOpConsumer {
            fn consume(
                &self,
                _records: Vec<FetchedRecord>,
            ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>> {
                Box::pin(async { Ok(()) })
            }
        }

        let cfg = KafkaConfig {
            address: StringSliceCSV(vec!["127.0.0.1:9092".to_string()]),
            topic: "ingest".to_string(),
            ..Default::default()
        };

        // Empty consumer_group -> instance_id verbatim (no -{partition} suffix)
        let r = SingleClusterPartitionReader::new(
            cfg.clone(),
            7,
            "ingester-zone-a-7",
            None,
            Arc::new(NoOpConsumer),
            None,
        )
        .unwrap();
        assert_eq!(r.consumer_group(), "ingester-zone-a-7");

        // Configured consumer_group with <partition> placeholder
        let mut cfg_tpl = cfg.clone();
        cfg_tpl.consumer_group = "my-group-<partition>".to_string();
        let r_tpl = SingleClusterPartitionReader::new(
            cfg_tpl,
            7,
            "ingester-zone-a-7",
            None,
            Arc::new(NoOpConsumer),
            None,
        )
        .unwrap();
        assert_eq!(r_tpl.consumer_group(), "my-group-7");

        // MultiClusterPartitionReader with empty consumer_group -> {instance_id}-wc-{idx}
        let multi = MultiClusterPartitionReader::new(
            &[cfg.clone(), cfg],
            &OrderedConsumptionConfig::default(),
            3,
            "ingester-3",
            vec![None, None],
            Arc::new(NoOpConsumer),
            None,
        )
        .unwrap();
        assert_eq!(multi.readers[0].consumer_group(), "ingester-3-wc-0");
        assert_eq!(multi.readers[1].consumer_group(), "ingester-3-wc-1");
    }

    #[tokio::test]
    async fn partition_committer_gates_commit_on_pre_commit_success_and_retries() {
        struct ToggleNotifier {
            fail: std::sync::atomic::AtomicBool,
            calls: std::sync::atomic::AtomicUsize,
        }
        impl PreCommitNotifier for ToggleNotifier {
            fn notify_pre_commit(
                &self,
            ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>> {
                Box::pin(async move {
                    self.calls.fetch_add(1, Ordering::SeqCst);
                    if self.fail.load(Ordering::SeqCst) {
                        Err("ENOSPC: No space left on device".to_string())
                    } else {
                        Ok(())
                    }
                })
            }
        }

        struct RecordingAdm {
            committed: SyncMutex<Vec<i64>>,
        }
        impl AdmClient for RecordingAdm {
            fn commit_offsets(
                &self,
                _group: &str,
                _topic: &str,
                _partition_id: i32,
                offset: i64,
            ) -> Pin<Box<dyn Future<Output = Result<i64, String>> + Send + '_>> {
                Box::pin(async move {
                    self.committed.lock().push(offset);
                    Ok(offset)
                })
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let off_path = dir.path().join("kafka-offset.json");
        let off_file = Arc::new(OffsetFile::new(off_path.clone(), 0));
        let notifier = Arc::new(ToggleNotifier {
            fail: std::sync::atomic::AtomicBool::new(true),
            calls: std::sync::atomic::AtomicUsize::new(0),
        });
        let adm = Arc::new(RecordingAdm {
            committed: SyncMutex::new(Vec::new()),
        });

        let mut cfg = KafkaConfig {
            address: StringSliceCSV(vec!["127.0.0.1:9092".to_string()]),
            topic: "ingest".to_string(),
            ..Default::default()
        };
        cfg.consumer_group_offset_commit_interval = mimir_config::GoDuration::from_millis(20);

        let reg = Registry::new();
        let committer = Arc::new(PartitionCommitter::new(
            cfg,
            Arc::clone(&adm) as Arc<dyn AdmClient>,
            0,
            "test-group",
            Arc::clone(&notifier) as Arc<dyn PreCommitNotifier>,
            Some(Arc::clone(&off_file)),
            Some(&reg),
        ));

        // Go 1:1 parity: pre-commit notification failure warns and continues with commit
        // without incrementing commit_failures_total.
        committer.commit(42).await.unwrap();
        assert_eq!(committer.last_committed_offset(), 42);
        assert_eq!(*adm.committed.lock(), vec![42]);
        assert_eq!(off_file.read().unwrap(), 42);
        assert_eq!(committer.commit_failures_count(), 0.0);

        // Periodic committer loop also warns & commits when notify_pre_commit fails.
        committer.start().await;
        committer.enqueue_offset(43);
        let dl = Instant::now() + Duration::from_secs(3);
        while committer.last_committed_offset() != 43 {
            assert!(Instant::now() < dl, "timed out waiting for periodic commit");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(*adm.committed.lock(), vec![42, 43]);
        assert_eq!(off_file.read().unwrap(), 43);
        assert_eq!(committer.commit_failures_count(), 0.0);

        // stop() always commits when to_commit >= 0 even if offset == last_committed_offset.
        notifier.fail.store(false, Ordering::SeqCst);
        let calls_before_stop = notifier.calls.load(Ordering::SeqCst);
        committer.stop().await;
        assert!(notifier.calls.load(Ordering::SeqCst) > calls_before_stop);
        assert_eq!(*adm.committed.lock(), vec![42, 43, 43]);
    }

    #[tokio::test]
    async fn wait_read_consistency_drop_guard_skips_failure_and_latency_on_cancel() {
        struct NoOpConsumer;
        impl RecordBatchConsumer for NoOpConsumer {
            fn consume(
                &self,
                _records: Vec<FetchedRecord>,
            ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>> {
                Box::pin(async { Ok(()) })
            }
        }

        let reg = Registry::new();
        let cfg = KafkaConfig {
            address: StringSliceCSV(vec!["127.0.0.1:9092".to_string()]),
            topic: "ingest-sc".to_string(),
            ..Default::default()
        };
        let reader = SingleClusterPartitionReader::new(
            cfg,
            0,
            "ing-0",
            None,
            Arc::new(NoOpConsumer),
            Some(&reg),
        )
        .unwrap();
        // Force Running state so wait_read_consistency reaches offset_watcher.wait_for_offset().
        reader
            .state
            .store(ReaderState::Running as u8, Ordering::SeqCst);

        let offs = PartitionOffsets::new_single_cluster(10);
        // Simulate client cancellation (RST_STREAM) dropping the future mid-await.
        let timed = tokio::time::timeout(
            Duration::from_millis(20),
            reader.wait_read_consistency_until_offsets(&offs, Duration::from_secs(30)),
        )
        .await;
        assert!(timed.is_err());

        let mfs = reg.gather();
        let reqs: f64 = mfs
            .iter()
            .find(|m| m.get_name() == "cortex_ingest_storage_strong_consistency_requests_total")
            .unwrap()
            .get_metric()
            .iter()
            .map(|m| m.get_counter().get_value())
            .sum();
        let fails: f64 = mfs
            .iter()
            .find(|m| m.get_name() == "cortex_ingest_storage_strong_consistency_failures_total")
            .map(|mf| mf.get_metric().iter().map(|m| m.get_counter().get_value()).sum())
            .unwrap_or(0.0);
        let lat_count: u64 = mfs
            .iter()
            .find(|m| m.get_name() == "cortex_ingest_storage_strong_consistency_wait_duration_seconds")
            .map(|mf| mf.get_metric().iter().map(|m| m.get_histogram().get_sample_count()).sum())
            .unwrap_or(0);
        assert_eq!(reqs, 1.0);
        assert_eq!(fails, 0.0, "canceled wait must not record failure");
        assert_eq!(lat_count, 0, "canceled wait must not observe latency");
    }

    #[tokio::test]
    async fn multi_cluster_wait_read_consistency_does_not_blame_healthy_sibling_and_formats_caller_deadline() {
        struct NoOpConsumer;
        impl RecordBatchConsumer for NoOpConsumer {
            fn consume(
                &self,
                _records: Vec<FetchedRecord>,
            ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>> {
                Box::pin(async { Ok(()) })
            }
        }

        let reg = Registry::new();
        let cfg0 = KafkaConfig {
            address: StringSliceCSV(vec!["127.0.0.1:9092".to_string()]),
            topic: "ingest-wc0".to_string(),
            ..Default::default()
        };
        let cfg1 = KafkaConfig {
            address: StringSliceCSV(vec!["127.0.0.1:9092".to_string()]),
            topic: "ingest-wc1".to_string(),
            ..Default::default()
        };
        let multi = MultiClusterPartitionReader::new(
            &[cfg0, cfg1],
            &OrderedConsumptionConfig::default(),
            0,
            "ing-0",
            vec![None, None],
            Arc::new(NoOpConsumer),
            Some(&reg),
        )
        .unwrap();

        // Cluster 0 is Running (healthy sibling), cluster 1 is New (NotRunning).
        multi.readers[0]
            .state
            .store(ReaderState::Running as u8, Ordering::SeqCst);

        let err = multi
            .wait_read_consistency_until_offsets(
                &PartitionOffsets::new_multi_cluster(vec![100, 100]),
                Duration::from_secs(5),
            )
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "write compartment 1: partition reader service is not running (state: New)"
        );

        let mfs = reg.gather();
        let fail_mf = mfs
            .iter()
            .find(|m| m.get_name() == "cortex_ingest_storage_strong_consistency_failures_total")
            .unwrap();
        let fail_for = |wc: &str| -> f64 {
            fail_mf
                .get_metric()
                .iter()
                .filter(|m| {
                    m.get_label()
                        .iter()
                        .any(|lp| lp.get_name() == "write_compartment" && lp.get_value() == wc)
                })
                .map(|m| m.get_counter().get_value())
                .sum()
        };
        assert_eq!(fail_for("1"), 1.0, "failing compartment 1 must record failure");
        assert_eq!(fail_for("0"), 0.0, "healthy sibling compartment 0 must not record failure");

        // Now mark cluster 1 Running too and test caller timeout error formatting.
        multi.readers[1]
            .state
            .store(ReaderState::Running as u8, Ordering::SeqCst);
        let err = multi
            .wait_read_consistency_until_offsets(
                &PartitionOffsets::new_multi_cluster(vec![100, 100]),
                Duration::from_millis(15),
            )
            .await
            .unwrap_err();
        assert!(err.is_deadline_exceeded());
        let msg = err.to_string();
        assert!(
            msg == "write compartment 0: context deadline exceeded"
                || msg == "write compartment 1: context deadline exceeded",
            "unexpected multi-cluster deadline error: {msg}"
        );

        // Verify InvalidClusterOffsetCount exact Go message.
        let single_err = multi.readers[0]
            .wait_read_consistency_until_offsets(
                &PartitionOffsets::new_multi_cluster(vec![1, 2]),
                Duration::from_millis(10),
            )
            .await
            .unwrap_err();
        assert_eq!(
            single_err.to_string(),
            "a single-cluster partition reader was given read consistency offsets for 2 Kafka clusters"
        );
    }

    #[tokio::test]
    async fn multi_cluster_reader_propagates_runtime_subservice_failure() {
        struct NoOpConsumer;
        impl RecordBatchConsumer for NoOpConsumer {
            fn consume(
                &self,
                _records: Vec<FetchedRecord>,
            ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>> {
                Box::pin(async { Ok(()) })
            }
        }

        let cfg = KafkaConfig {
            address: StringSliceCSV(vec!["127.0.0.1:9092".to_string()]),
            topic: "ingest-rt".to_string(),
            auto_create_topic_enabled: false,
            consume_from_position_at_startup: "start".to_string(),
            ..Default::default()
        };
        let multi = MultiClusterPartitionReader::new(
            &[cfg.clone(), cfg],
            &OrderedConsumptionConfig {
                enabled: true,
                ..Default::default()
            },
            0,
            "ing-0",
            vec![None, None],
            Arc::new(NoOpConsumer),
            None,
        )
        .unwrap();

        // Simulate running state + watcher monitoring both readers.
        multi.readers[0]
            .state
            .store(ReaderState::Running as u8, Ordering::SeqCst);
        multi.readers[1]
            .state
            .store(ReaderState::Running as u8, Ordering::SeqCst);
        multi
            .state
            .store(ReaderState::Running as u8, Ordering::SeqCst);

        let readers = multi.readers.clone();
        let merger = multi.merger.clone();
        let state = Arc::clone(&multi.state);
        let failure_error = Arc::clone(&multi.failure_error);
        let mut stop_rx = multi.stop_tx.subscribe();
        let handle = tokio::spawn(async move {
            let mut rxs: Vec<(usize, watch::Receiver<Option<String>>)> = readers
                .iter()
                .enumerate()
                .map(|(cid, r)| (cid, r.subscribe_failure()))
                .collect();
            loop {
                if *stop_rx.borrow() {
                    return;
                }
                for (cid, rx) in &rxs {
                    let err_opt = rx.borrow().clone();
                    if let Some(err) = err_opt {
                        *failure_error.lock() = Some(format!(
                            "a multi-cluster partition reader subservice failed (write compartment {cid}): {err}"
                        ));
                        state.store(ReaderState::Failed as u8, Ordering::SeqCst);
                        join_all(readers.iter().map(|r| r.stop())).await;
                        if let Some(ref m) = merger {
                            m.shutdown_and_drain().await;
                        }
                        return;
                    }
                }
                let wait_futs = rxs.iter_mut().map(|(_, rx)| Box::pin(rx.changed()));
                tokio::select! {
                    res = stop_rx.changed() => { if res.is_err() || *stop_rx.borrow() { return; } }
                    _ = futures::future::select_all(wait_futs) => {}
                }
            }
        });
        *multi.watcher_task.lock().await = Some(handle);

        // Simulate runtime failure in compartment 1.
        multi.readers[1]
            .state
            .store(ReaderState::Failed as u8, Ordering::SeqCst);
        let _ = multi.readers[1]
            .failure_tx
            .send(Some("simulated fatal fetch error".to_string()));

        let dl = Instant::now() + Duration::from_secs(3);
        while multi.state() != ReaderState::Failed {
            assert!(Instant::now() < dl, "timed out waiting for Failed");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(multi
            .failure_error()
            .unwrap()
            .contains("write compartment 1"));
        assert_eq!(multi.readers[0].state(), ReaderState::Terminated);
        assert_eq!(multi.readers[1].state(), ReaderState::Failed);
        multi.stop().await;
        assert_eq!(multi.state(), ReaderState::Failed);
    }

    #[tokio::test]
    async fn w22_deep_05_multi_cluster_reader_watches_heap_merger_failure() {
        struct PanickingConsumer;
        impl RecordBatchConsumer for PanickingConsumer {
            fn consume(
                &self,
                _records: Vec<FetchedRecord>,
            ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>> {
                Box::pin(async {
                    panic!("simulated heap merger downstream panic");
                })
            }
        }

        let cfg = KafkaConfig {
            address: StringSliceCSV(vec!["127.0.0.1:9092".to_string()]),
            topic: "ingest-rt".to_string(),
            auto_create_topic_enabled: false,
            consume_from_position_at_startup: "start".to_string(),
            ..Default::default()
        };
        let multi = MultiClusterPartitionReader::new(
            &[cfg.clone(), cfg],
            &OrderedConsumptionConfig {
                enabled: true,
                max_batch_records: 1,
                max_batch_wait: mimir_config::GoDuration::from_millis(10),
                ..Default::default()
            },
            0,
            "ing-0",
            vec![None, None],
            Arc::new(PanickingConsumer),
            None,
        )
        .unwrap();

        let merger = multi.merger.as_ref().expect("merger enabled");
        merger.ensure_started();
        multi.readers[0]
            .state
            .store(ReaderState::Running as u8, Ordering::SeqCst);
        multi.readers[1]
            .state
            .store(ReaderState::Running as u8, Ordering::SeqCst);
        multi
            .state
            .store(ReaderState::Running as u8, Ordering::SeqCst);

        let rxs: Vec<(usize, watch::Receiver<Option<String>>)> = multi
            .readers
            .iter()
            .enumerate()
            .map(|(cid, r)| (cid, r.subscribe_failure()))
            .collect();
        let mrx = multi.merger.as_ref().map(|m| m.subscribe_failure());
        *multi.watcher_task.lock().await = Some(multi.spawn_watcher_task(rxs, mrx));

        // Submit a record into HeapMerger to trigger the panic in its background task.
        let _ = merger
            .submit_and_wait(
                0,
                vec![FetchedRecord {
                    offset: 0,
                    timestamp_ms: 100,
                    key: bytes::Bytes::from_static(b"t1"),
                    value: bytes::Bytes::new(),
                    headers: Vec::new(),
                }],
            )
            .await;

        let dl = Instant::now() + Duration::from_secs(3);
        while multi.state() != ReaderState::Failed {
            assert!(Instant::now() < dl, "timed out waiting for Failed");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let err_msg = multi.failure_error().unwrap();
        assert!(
            err_msg.contains("heap merger"),
            "unexpected failure_error: {err_msg}"
        );
        assert_eq!(multi.readers[0].state(), ReaderState::Terminated);
        assert_eq!(multi.readers[1].state(), ReaderState::Terminated);
        multi.stop().await;
        assert_eq!(multi.state(), ReaderState::Failed);
    }

    #[tokio::test]
    async fn w22_deep_06_lag_catchup_honors_deadline_and_stop_on_stalled_broker() {
        use bytes::{BufMut, Bytes, BytesMut};
        use kafka_protocol::messages::api_versions_response::{
            ApiVersion, ApiVersionsResponse,
        };
        use kafka_protocol::messages::list_offsets_response::{
            ListOffsetsPartitionResponse, ListOffsetsResponse, ListOffsetsTopicResponse,
        };
        use kafka_protocol::messages::metadata_response::{
            MetadataResponse, MetadataResponseBroker, MetadataResponsePartition,
            MetadataResponseTopic,
        };
        use kafka_protocol::messages::{
            ApiKey, ApiVersionsRequest, BrokerId, ListOffsetsRequest, MetadataRequest,
            RequestHeader, ResponseHeader, TopicName,
        };
        use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};
        use std::sync::atomic::AtomicUsize;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        struct NoOpConsumer;
        impl RecordBatchConsumer for NoOpConsumer {
            fn consume(
                &self,
                _records: Vec<FetchedRecord>,
            ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>> {
                Box::pin(async { Ok(()) })
            }
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let list_offsets_calls = Arc::new(AtomicUsize::new(0));
        let lo_calls_srv = Arc::clone(&list_offsets_calls);

        let srv = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let lo_calls = Arc::clone(&lo_calls_srv);
                tokio::spawn(async move {
                    let (mut rd, mut wr) = stream.into_split();
                    let mut len_buf = [0u8; 4];
                    while rd.read_exact(&mut len_buf).await.is_ok() {
                        let sz = u32::from_be_bytes(len_buf) as usize;
                        let mut payload = vec![0u8; sz];
                        if rd.read_exact(&mut payload).await.is_err() {
                            break;
                        }
                        let mut buf = Bytes::from(payload);
                        let key_i16 = i16::from_be_bytes([buf[0], buf[1]]);
                        let ver = i16::from_be_bytes([buf[2], buf[3]]);
                        let Ok(api_key) = ApiKey::try_from(key_i16) else {
                            break;
                        };
                        let hdr_ver = match api_key {
                            ApiKey::ApiVersions => ApiVersionsRequest::header_version(ver),
                            ApiKey::Metadata => MetadataRequest::header_version(ver),
                            ApiKey::ListOffsets => ListOffsetsRequest::header_version(ver),
                            _ => break,
                        };
                        let Ok(req_hdr) = RequestHeader::decode(&mut buf, hdr_ver) else {
                            break;
                        };
                        let mut out = BytesMut::new();
                        out.put_u32(0);
                        let resp_hdr_ver = api_key.response_header_version(ver);
                        let mut resp_hdr = ResponseHeader::default();
                        resp_hdr.correlation_id = req_hdr.correlation_id;
                        resp_hdr.encode(&mut out, resp_hdr_ver).unwrap();

                        match api_key {
                            ApiKey::ApiVersions => {
                                let _ = ApiVersionsRequest::decode(&mut buf, ver);
                                let mut resp = ApiVersionsResponse::default();
                                for (k, mn, mx) in [
                                    (ApiKey::ApiVersions as i16, 0, 3),
                                    (ApiKey::Metadata as i16, 1, 12),
                                    (ApiKey::ListOffsets as i16, 1, 7),
                                ] {
                                    let mut av = ApiVersion::default();
                                    av.api_key = k;
                                    av.min_version = mn;
                                    av.max_version = mx;
                                    resp.api_keys.push(av);
                                }
                                resp.encode(&mut out, ver).unwrap();
                            }
                            ApiKey::Metadata => {
                                let _ = MetadataRequest::decode(&mut buf, ver);
                                let mut resp = MetadataResponse::default();
                                let mut b = MetadataResponseBroker::default();
                                b.node_id = BrokerId(0);
                                b.host = StrBytes::from_static_str("127.0.0.1");
                                b.port = addr.port() as i32;
                                resp.brokers.push(b);
                                resp.controller_id = BrokerId(0);
                                let mut t = MetadataResponseTopic::default();
                                t.name = Some(TopicName(StrBytes::from_static_str("ingest-lag")));
                                let mut p = MetadataResponsePartition::default();
                                p.partition_index = 0;
                                p.leader_id = BrokerId(0);
                                t.partitions.push(p);
                                resp.topics.push(t);
                                resp.encode(&mut out, ver).unwrap();
                            }
                            ApiKey::ListOffsets => {
                                let call_n = lo_calls.fetch_add(1, Ordering::SeqCst) + 1;
                                // Calls 1-2 (attempt 0): start=0, end=0 (takes >1ns, <=60ms).
                                // Call 3 (attempt 1): stall for 10s so deadline must abort it.
                                // Calls 4-5 (attempt 2): start=1, end=0 (empty -> finishes cleanly).
                                if call_n == 3 || call_n >= 6 {
                                    tokio::time::sleep(Duration::from_secs(10)).await;
                                    return;
                                }
                                let mut resp = ListOffsetsResponse::default();
                                let mut topic = ListOffsetsTopicResponse::default();
                                topic.name = TopicName(StrBytes::from_static_str("ingest-lag"));
                                let mut part = ListOffsetsPartitionResponse::default();
                                part.partition_index = 0;
                                part.offset = match call_n {
                                    1 | 2 => 0,
                                    4 => 1,
                                    _ => 0,
                                };
                                topic.partitions.push(part);
                                resp.topics.push(topic);
                                resp.encode(&mut out, ver).unwrap();
                            }
                            _ => {}
                        }
                        let frame_len = (out.len() - 4) as u32;
                        out[0..4].copy_from_slice(&frame_len.to_be_bytes());
                        if wr.write_all(&out).await.is_err() {
                            break;
                        }
                    }
                });
            }
        });

        let cfg = KafkaConfig {
            address: StringSliceCSV(vec![addr.to_string()]),
            topic: "ingest-lag".to_string(),
            auto_create_topic_enabled: false,
            ..Default::default()
        };
        let client = Arc::new(KafkaClient::from_config(&cfg).unwrap());
        let reader = SingleClusterPartitionReader::with_client(
            cfg,
            0,
            "ing-0",
            None,
            client,
            Arc::new(NoOpConsumer),
            None,
        )
        .unwrap();
        reader.offset_watcher.notify(0);

        let fetcher = Arc::new(ReaderFetcher::Client(ClientPartitionConsumer::new(
            Arc::clone(&reader.client),
            "ingest-lag".to_string(),
            0,
            0,
            Duration::from_millis(50),
        )));
        let fetch_loop = FetchLoop {
            partition_id: 0,
            topic: "ingest-lag".to_string(),
            fetcher,
            consumer: Arc::clone(&reader.consumer),
            committer: Arc::clone(&reader.committer),
            offset_watcher: Arc::clone(&reader.offset_watcher),
            metrics: reader.metrics.clone(),
            last_seen_offset: Arc::clone(&reader.last_seen_offset),
            highest_ts: Arc::clone(&reader.highest_consumed_ts_before_end_ms),
        };

        // Attempt 0 finishes immediately (> 1ns target_lag, <= 60ms max_lag);
        // Attempt 1 stalls on ListOffsets and MUST abort at 2 * max_lag = 120ms;
        // Attempt 2 completes cleanly — total elapsed must be << 10s broker stall.
        let started = Instant::now();
        reader
            .process_next_fetches_until_target_or_max_lag_honored(
                &fetch_loop,
                Duration::from_nanos(1),
                Duration::from_millis(60),
            )
            .await
            .unwrap();
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_secs(2),
            "attempt 1 ignored 2*max_lag deadline during stalled ListOffsets: {elapsed:?}"
        );

        // stop() must also abort stalled ListOffsets immediately with Canceled.
        let stop_rx = reader.stop_tx.subscribe();
        let mut cancel = Cancel::new(stop_rx, None);
        let stop_tx = reader.stop_tx.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            let _ = stop_tx.send(true);
        });
        let err = tokio::time::timeout(
            Duration::from_secs(2),
            reader.process_next_fetches_until_lag_honored(
                &fetch_loop,
                Duration::from_secs(5),
                &mut cancel,
            ),
        )
        .await
        .expect("stop() must abort stalled ListOffsets")
        .unwrap_err();
        assert!(matches!(err, ReaderError::Canceled), "got {err:?}");

        srv.abort();
    }

    #[tokio::test]
    async fn w24_b05_with_pre_commit_notifier_preserves_registered_committer_metrics() {
        use std::sync::atomic::AtomicUsize;

        struct NoOpConsumer;
        impl RecordBatchConsumer for NoOpConsumer {
            fn consume(
                &self,
                _records: Vec<FetchedRecord>,
            ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>> {
                Box::pin(async { Ok(()) })
            }
        }

        struct CountingNotifier(AtomicUsize);
        impl PreCommitNotifier for CountingNotifier {
            fn notify_pre_commit(&self) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>> {
                self.0.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { Ok(()) })
            }
        }

        let reg = Registry::new();
        let cfg = KafkaConfig {
            address: mimir_config::StringSliceCSV(vec!["127.0.0.1:9092".to_string()]),
            topic: "w24-b05".to_string(),
            // Empty instance_id -> consumer_group is empty, so commit() skips broker RPC.
            ..Default::default()
        };
        let client = Arc::new(KafkaClient::from_config(&cfg).unwrap());
        let notifier = Arc::new(CountingNotifier(AtomicUsize::new(0)));
        let reader = SingleClusterPartitionReader::with_client(
            cfg,
            3,
            "",
            None,
            client,
            Arc::new(NoOpConsumer),
            Some(&reg),
        )
        .unwrap()
        .with_pre_commit_notifier(Arc::clone(&notifier) as Arc<dyn PreCommitNotifier>);

        reader.committer().commit(42).await.unwrap();
        assert_eq!(notifier.0.load(Ordering::SeqCst), 1);
        assert_eq!(reader.committer().last_committed_offset(), 42);

        let mfs = reg.gather();
        let find_metric = |name: &str| {
            mfs.iter()
                .find(|mf| mf.get_name() == name)
                .unwrap_or_else(|| panic!("missing metric {name}"))
                .get_metric()[0]
                .clone()
        };
        assert_eq!(
            find_metric("cortex_ingest_storage_reader_offset_commit_requests_total")
                .get_counter()
                .get_value(),
            1.0
        );
        assert_eq!(
            find_metric("cortex_ingest_storage_reader_offset_commit_failures_total")
                .get_counter()
                .get_value(),
            0.0
        );
        assert_eq!(
            find_metric("cortex_ingest_storage_reader_offset_commit_request_duration_seconds")
                .get_histogram()
                .get_sample_count(),
            1
        );
        assert_eq!(
            find_metric("cortex_ingest_storage_reader_last_committed_offset")
                .get_gauge()
                .get_value(),
            42.0
        );
    }

    #[tokio::test]
    async fn d1_7_runtime_failure_stops_fetcher_and_start_offset_reader() {
        use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};
        use kafka_protocol::messages::list_offsets_response::{
            ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
        };
        use kafka_protocol::messages::{
            ApiVersionsRequest, ApiVersionsResponse, FetchRequest, FetchResponse,
            ListOffsetsRequest, ListOffsetsResponse, MetadataRequest, MetadataResponse,
            RequestHeader, ResponseHeader,
        };
        use kafka_protocol::messages::api_versions_response::ApiVersion;
        use kafka_protocol::messages::metadata_response::{
            MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
        };
        use std::sync::atomic::AtomicUsize;
        use kafka_protocol::messages::{ApiKey, BrokerId, TopicName};
        use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use bytes::BufMut;
        use tokio::net::TcpListener;

        struct FailOnRecordConsumer;
        impl RecordBatchConsumer for FailOnRecordConsumer {
            fn consume(
                &self,
                records: Vec<FetchedRecord>,
            ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>> {
                Box::pin(async move {
                    if !records.is_empty() {
                        Err("non-recoverable consumer failure".to_string())
                    } else {
                        Ok(())
                    }
                })
            }
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let fetch_calls = Arc::new(AtomicUsize::new(0));
        let fetch_calls_srv = Arc::clone(&fetch_calls);

        let srv = tokio::spawn(async move {
            loop {
                let Ok((sock, _)) = listener.accept().await else {
                    break;
                };
                let fc = Arc::clone(&fetch_calls_srv);
                tokio::spawn(async move {
                    let (mut rd, mut wr) = sock.into_split();
                    loop {
                        let mut len_buf = [0u8; 4];
                        if rd.read_exact(&mut len_buf).await.is_err() {
                            break;
                        }
                        let len = u32::from_be_bytes(len_buf) as usize;
                        let mut payload = vec![0u8; len];
                        if rd.read_exact(&mut payload).await.is_err() {
                            break;
                        }
                        let mut buf = bytes::Bytes::from(payload);
                        let req_hdr_ver = if i16::from_be_bytes([buf[0], buf[1]]) == ApiKey::ApiVersions as i16 {
                            let ver = i16::from_be_bytes([buf[2], buf[3]]);
                            ApiVersionsRequest::header_version(ver)
                        } else {
                            let key = ApiKey::try_from(i16::from_be_bytes([buf[0], buf[1]])).unwrap();
                            let ver = i16::from_be_bytes([buf[2], buf[3]]);
                            match key {
                                ApiKey::Metadata => MetadataRequest::header_version(ver),
                                ApiKey::ListOffsets => ListOffsetsRequest::header_version(ver),
                                ApiKey::Fetch => FetchRequest::header_version(ver),
                                _ => 1,
                            }
                        };
                        let req_hdr = RequestHeader::decode(&mut buf, req_hdr_ver).unwrap();
                        let api_key = ApiKey::try_from(req_hdr.request_api_key).unwrap();
                        let ver = req_hdr.request_api_version;

                        let mut out = bytes::BytesMut::new();
                        out.put_u32(0);
                        let resp_hdr_ver = if api_key == ApiKey::ApiVersions {
                            0
                        } else {
                            api_key.response_header_version(ver)
                        };
                        let mut resp_hdr = ResponseHeader::default();
                        resp_hdr.correlation_id = req_hdr.correlation_id;
                        resp_hdr.encode(&mut out, resp_hdr_ver).unwrap();

                        match api_key {
                            ApiKey::ApiVersions => {
                                let _ = ApiVersionsRequest::decode(&mut buf, ver);
                                let mut resp = ApiVersionsResponse::default();
                                for (k, mn, mx) in [
                                    (ApiKey::ApiVersions as i16, 0, 3),
                                    (ApiKey::Metadata as i16, 1, 12),
                                    (ApiKey::ListOffsets as i16, 1, 7),
                                    (ApiKey::Fetch as i16, 4, 12),
                                ] {
                                    let mut av = ApiVersion::default();
                                    av.api_key = k;
                                    av.min_version = mn;
                                    av.max_version = mx;
                                    resp.api_keys.push(av);
                                }
                                resp.encode(&mut out, ver).unwrap();
                            }
                            ApiKey::Metadata => {
                                let _ = MetadataRequest::decode(&mut buf, ver);
                                let mut resp = MetadataResponse::default();
                                let mut b = MetadataResponseBroker::default();
                                b.node_id = BrokerId(0);
                                b.host = StrBytes::from_static_str("127.0.0.1");
                                b.port = addr.port() as i32;
                                resp.brokers.push(b);
                                resp.controller_id = BrokerId(0);
                                let mut t = MetadataResponseTopic::default();
                                t.name = Some(TopicName(StrBytes::from_static_str("fail-clean")));
                                let mut p = MetadataResponsePartition::default();
                                p.partition_index = 0;
                                p.leader_id = BrokerId(0);
                                t.partitions.push(p);
                                resp.topics.push(t);
                                resp.encode(&mut out, ver).unwrap();
                            }
                            ApiKey::ListOffsets => {
                                let _ = ListOffsetsRequest::decode(&mut buf, ver);
                                let mut resp = ListOffsetsResponse::default();
                                let mut topic = ListOffsetsTopicResponse::default();
                                topic.name = TopicName(StrBytes::from_static_str("fail-clean"));
                                let mut part = ListOffsetsPartitionResponse::default();
                                part.partition_index = 0;
                                part.offset = 0;
                                topic.partitions.push(part);
                                resp.topics.push(topic);
                                resp.encode(&mut out, ver).unwrap();
                            }
                            ApiKey::Fetch => {
                                let _ = FetchRequest::decode(&mut buf, ver);
                                fc.fetch_add(1, Ordering::SeqCst);
                                let mut resp = FetchResponse::default();
                                let mut ft = FetchableTopicResponse::default();
                                ft.topic = TopicName(StrBytes::from_static_str("fail-clean"));
                                let mut pd = PartitionData::default();
                                pd.partition_index = 0;
                                pd.error_code = kafka_protocol::error::ResponseError::TopicAuthorizationFailed.code();
                                ft.partitions.push(pd);
                                resp.responses.push(ft);
                                resp.encode(&mut out, ver).unwrap();
                            }
                            _ => {}
                        }
                        let frame_len = (out.len() - 4) as u32;
                        out[0..4].copy_from_slice(&frame_len.to_be_bytes());
                        if wr.write_all(&out).await.is_err() {
                            break;
                        }
                    }
                });
            }
        });

        let mut cfg = KafkaConfig {
            address: StringSliceCSV(vec![addr.to_string()]),
            topic: "fail-clean".to_string(),
            auto_create_topic_enabled: false,
            fetch_concurrency_max: 0,
            ..Default::default()
        };
        cfg.target_consumer_lag_at_startup = mimir_config::GoDuration::ZERO;
        cfg.max_consumer_lag_at_startup = mimir_config::GoDuration::ZERO;

        let client = Arc::new(KafkaClient::from_config(&cfg).unwrap());
        let reader = SingleClusterPartitionReader::with_client(
            cfg,
            0,
            "",
            None,
            client,
            Arc::new(FailOnRecordConsumer),
            None,
        )
        .unwrap();

        let mut failure_rx = reader.subscribe_failure();
        reader.start().await.unwrap();
        assert_eq!(reader.state(), ReaderState::Running);
        assert!(reader.fetcher.lock().await.is_some());
        assert!(reader.start_offset_reader.lock().await.is_some());

        // Wait until fetcher has issued at least one Fetch while Running.
        let wait_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while fetch_calls.load(Ordering::SeqCst) == 0 {
            assert!(tokio::time::Instant::now() < wait_deadline, "fetcher never issued Fetch");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        // Trigger the exact runtime failure path used when fetch_loop.run() returns Err(e).
        reader
            .simulate_runtime_failure_for_test("simulated runtime error".to_string())
            .await;

        tokio::time::timeout(Duration::from_secs(2), failure_rx.wait_for(|v| v.is_some()))
            .await
            .expect("failure_tx must be notified")
            .unwrap();

        assert_eq!(reader.state(), ReaderState::Failed);
        assert!(
            reader.fetcher.lock().await.is_none(),
            "fetcher must be stopped and cleared on runtime failure"
        );
        assert!(
            reader.start_offset_reader.lock().await.is_none(),
            "start_offset_reader must be stopped and cleared on runtime failure"
        );
        assert!(
            reader.sampler.lock().await.is_none(),
            "sampler must be stopped and cleared on runtime failure"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
        let calls_at_fail = fetch_calls.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            fetch_calls.load(Ordering::SeqCst),
            calls_at_fail,
            "fetcher must not keep issuing Fetch requests after runtime failure"
        );

        reader.stop().await;
        srv.abort();
    }

    #[tokio::test]
    async fn w35_stor01_stop_interrupts_unreachable_get_start_offset_and_create_topics_and_closes_client() {
        use std::sync::atomic::AtomicUsize;
        use tokio::io::AsyncReadExt;
        use tokio::net::TcpListener;

        struct NoOpConsumer;
        impl RecordBatchConsumer for NoOpConsumer {
            fn consume(
                &self,
                _records: Vec<FetchedRecord>,
            ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>> {
                Box::pin(async { Ok(()) })
            }
        }

        // Case A: blocked inside get_start_offset() with last_produced_offset_retry_timeout = 0
        // (infinite retry) against a blackhole broker that accepts TCP but never responds.
        let listener_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr_a = listener_a.local_addr().unwrap();
        let accepted_a = Arc::new(AtomicUsize::new(0));
        let eof_a = Arc::new(AtomicUsize::new(0));
        let accepted_a_srv = Arc::clone(&accepted_a);
        let eof_a_srv = Arc::clone(&eof_a);
        let srv_a = tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener_a.accept().await else {
                    break;
                };
                accepted_a_srv.fetch_add(1, Ordering::SeqCst);
                let eof_cnt = Arc::clone(&eof_a_srv);
                tokio::spawn(async move {
                    let mut buf = [0u8; 256];
                    loop {
                        match sock.read(&mut buf).await {
                            Ok(0) | Err(_) => {
                                eof_cnt.fetch_add(1, Ordering::SeqCst);
                                break;
                            }
                            Ok(_) => {}
                        }
                    }
                });
            }
        });

        let mut cfg_a = KafkaConfig {
            address: StringSliceCSV(vec![addr_a.to_string()]),
            topic: "unreachable-a".to_string(),
            auto_create_topic_enabled: false,
            fetch_concurrency_max: 0,
            ..Default::default()
        };
        cfg_a.dial_timeout = mimir_config::GoDuration::from_secs(30);
        cfg_a.last_produced_offset_retry_timeout = mimir_config::GoDuration::ZERO;
        let client_a = Arc::new(KafkaClient::from_config(&cfg_a).unwrap());
        let reader_a = Arc::new(
            SingleClusterPartitionReader::with_client(
                cfg_a,
                0,
                "inst-a",
                None,
                Arc::clone(&client_a),
                Arc::new(NoOpConsumer),
                None,
            )
            .unwrap(),
        );

        let rx_a = reader_a.start_async_with_result().await;
        // Wait until get_start_offset() has dialed the blackhole broker and is waiting for response.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while accepted_a.load(Ordering::SeqCst) == 0 {
            assert!(tokio::time::Instant::now() < deadline, "reader never dialed broker");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        let t0 = Instant::now();
        reader_a.stop().await;
        let elapsed = t0.elapsed();
        assert!(
            elapsed < Duration::from_secs(2),
            "stop() must interrupt blocked get_start_offset() in <2s, took {elapsed:?}"
        );
        let start_res = rx_a.await.unwrap();
        assert!(
            matches!(start_res, Err(ReaderError::Canceled)),
            "expected ReaderError::Canceled, got {start_res:?}"
        );
        assert!(client_a.is_closed(), "KafkaClient must be marked closed");
        assert_eq!(
            client_a.active_conn_count().await,
            0,
            "KafkaClient pooled connections must be cleared"
        );
        let eof_deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        while eof_a.load(Ordering::SeqCst) < accepted_a.load(Ordering::SeqCst) {
            assert!(
                tokio::time::Instant::now() < eof_deadline,
                "broker sockets did not close after stop()"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        srv_a.abort();

        // Case B: blocked inside create_topics() when auto_create_topic_enabled = true.
        let listener_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr_b = listener_b.local_addr().unwrap();
        let accepted_b = Arc::new(AtomicUsize::new(0));
        let eof_b = Arc::new(AtomicUsize::new(0));
        let accepted_b_srv = Arc::clone(&accepted_b);
        let eof_b_srv = Arc::clone(&eof_b);
        let srv_b = tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener_b.accept().await else {
                    break;
                };
                accepted_b_srv.fetch_add(1, Ordering::SeqCst);
                let eof_cnt = Arc::clone(&eof_b_srv);
                tokio::spawn(async move {
                    let mut buf = [0u8; 256];
                    loop {
                        match sock.read(&mut buf).await {
                            Ok(0) | Err(_) => {
                                eof_cnt.fetch_add(1, Ordering::SeqCst);
                                break;
                            }
                            Ok(_) => {}
                        }
                    }
                });
            }
        });

        let mut cfg_b = KafkaConfig {
            address: StringSliceCSV(vec![addr_b.to_string()]),
            topic: "unreachable-b".to_string(),
            auto_create_topic_enabled: true,
            fetch_concurrency_max: 0,
            ..Default::default()
        };
        cfg_b.dial_timeout = mimir_config::GoDuration::from_secs(30);
        cfg_b.last_produced_offset_retry_timeout = mimir_config::GoDuration::from_secs(30);
        let client_b = Arc::new(KafkaClient::from_config(&cfg_b).unwrap());
        let reader_b = Arc::new(
            SingleClusterPartitionReader::with_client(
                cfg_b,
                0,
                "inst-b",
                None,
                Arc::clone(&client_b),
                Arc::new(NoOpConsumer),
                None,
            )
            .unwrap(),
        );

        let rx_b = reader_b.start_async_with_result().await;
        let deadline_b = tokio::time::Instant::now() + Duration::from_secs(2);
        while accepted_b.load(Ordering::SeqCst) == 0 {
            assert!(tokio::time::Instant::now() < deadline_b, "create_topics never dialed broker");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        let t1 = Instant::now();
        reader_b.stop().await;
        let elapsed_b = t1.elapsed();
        assert!(
            elapsed_b < Duration::from_secs(2),
            "stop() must interrupt blocked create_topics() in <2s, took {elapsed_b:?}"
        );
        let start_res_b = rx_b.await.unwrap();
        assert!(
            matches!(start_res_b, Err(ReaderError::Canceled)),
            "expected ReaderError::Canceled, got {start_res_b:?}"
        );
        assert!(client_b.is_closed(), "KafkaClient must be marked closed");
        assert_eq!(client_b.active_conn_count().await, 0);
        let eof_deadline_b = tokio::time::Instant::now() + Duration::from_secs(1);
        while eof_b.load(Ordering::SeqCst) < accepted_b.load(Ordering::SeqCst) {
            assert!(
                tokio::time::Instant::now() < eof_deadline_b,
                "create_topics broker sockets did not close after stop()"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        srv_b.abort();
    }

    #[tokio::test]
    async fn w35_stor02_stop_dependencies_and_fail_and_stop_close_client_with_live_reader_arc() {
        use bytes::BufMut;
        use kafka_protocol::messages::api_versions_response::ApiVersion;
        use kafka_protocol::messages::metadata_response::{
            MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
        };
        use kafka_protocol::messages::{
            ApiKey, ApiVersionsRequest, ApiVersionsResponse, BrokerId, FetchRequest, FetchResponse,
            ListOffsetsRequest, ListOffsetsResponse, MetadataRequest, MetadataResponse,
            RequestHeader, ResponseHeader, TopicName,
        };
        use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};
        use kafka_protocol::messages::list_offsets_response::{
            ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
        };
        use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};
        use std::sync::atomic::AtomicUsize;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        struct NoOpConsumer;
        impl RecordBatchConsumer for NoOpConsumer {
            fn consume(
                &self,
                _records: Vec<FetchedRecord>,
            ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>> {
                Box::pin(async { Ok(()) })
            }
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let active_conns = Arc::new(AtomicUsize::new(0));
        let active_conns_srv = Arc::clone(&active_conns);

        let srv = tokio::spawn(async move {
            loop {
                let Ok((sock, _)) = listener.accept().await else {
                    break;
                };
                let conns = Arc::clone(&active_conns_srv);
                conns.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(async move {
                    struct DecOnDrop(Arc<AtomicUsize>);
                    impl Drop for DecOnDrop {
                        fn drop(&mut self) {
                            self.0.fetch_sub(1, Ordering::SeqCst);
                        }
                    }
                    let _guard = DecOnDrop(conns);
                    let (mut rd, mut wr) = sock.into_split();
                    loop {
                        let mut len_buf = [0u8; 4];
                        if rd.read_exact(&mut len_buf).await.is_err() {
                            break;
                        }
                        let len = u32::from_be_bytes(len_buf) as usize;
                        let mut payload = vec![0u8; len];
                        if rd.read_exact(&mut payload).await.is_err() {
                            break;
                        }
                        let mut buf = bytes::Bytes::from(payload);
                        let req_hdr_ver = if i16::from_be_bytes([buf[0], buf[1]]) == ApiKey::ApiVersions as i16 {
                            let ver = i16::from_be_bytes([buf[2], buf[3]]);
                            ApiVersionsRequest::header_version(ver)
                        } else {
                            let key = ApiKey::try_from(i16::from_be_bytes([buf[0], buf[1]])).unwrap();
                            let ver = i16::from_be_bytes([buf[2], buf[3]]);
                            match key {
                                ApiKey::Metadata => MetadataRequest::header_version(ver),
                                ApiKey::ListOffsets => ListOffsetsRequest::header_version(ver),
                                ApiKey::Fetch => FetchRequest::header_version(ver),
                                _ => 1,
                            }
                        };
                        let req_hdr = RequestHeader::decode(&mut buf, req_hdr_ver).unwrap();
                        let api_key = ApiKey::try_from(req_hdr.request_api_key).unwrap();
                        let ver = req_hdr.request_api_version;

                        let mut out = bytes::BytesMut::new();
                        out.put_u32(0);
                        let resp_hdr_ver = if api_key == ApiKey::ApiVersions {
                            0
                        } else {
                            api_key.response_header_version(ver)
                        };
                        let mut resp_hdr = ResponseHeader::default();
                        resp_hdr.correlation_id = req_hdr.correlation_id;
                        resp_hdr.encode(&mut out, resp_hdr_ver).unwrap();

                        match api_key {
                            ApiKey::ApiVersions => {
                                let _ = ApiVersionsRequest::decode(&mut buf, ver);
                                let mut resp = ApiVersionsResponse::default();
                                for (k, mn, mx) in [
                                    (ApiKey::ApiVersions as i16, 0, 3),
                                    (ApiKey::Metadata as i16, 1, 12),
                                    (ApiKey::ListOffsets as i16, 1, 7),
                                    (ApiKey::Fetch as i16, 4, 12),
                                ] {
                                    let mut av = ApiVersion::default();
                                    av.api_key = k;
                                    av.min_version = mn;
                                    av.max_version = mx;
                                    resp.api_keys.push(av);
                                }
                                resp.encode(&mut out, ver).unwrap();
                            }
                            ApiKey::Metadata => {
                                let _ = MetadataRequest::decode(&mut buf, ver);
                                let mut resp = MetadataResponse::default();
                                let mut b = MetadataResponseBroker::default();
                                b.node_id = BrokerId(0);
                                b.host = StrBytes::from_static_str("127.0.0.1");
                                b.port = addr.port() as i32;
                                resp.brokers.push(b);
                                resp.controller_id = BrokerId(0);
                                let mut t = MetadataResponseTopic::default();
                                t.name = Some(TopicName(StrBytes::from_static_str("stor02")));
                                let mut p = MetadataResponsePartition::default();
                                p.partition_index = 0;
                                p.leader_id = BrokerId(0);
                                t.partitions.push(p);
                                resp.topics.push(t);
                                resp.encode(&mut out, ver).unwrap();
                            }
                            ApiKey::ListOffsets => {
                                let _ = ListOffsetsRequest::decode(&mut buf, ver);
                                let mut resp = ListOffsetsResponse::default();
                                let mut topic = ListOffsetsTopicResponse::default();
                                topic.name = TopicName(StrBytes::from_static_str("stor02"));
                                let mut part = ListOffsetsPartitionResponse::default();
                                part.partition_index = 0;
                                part.offset = 0;
                                topic.partitions.push(part);
                                resp.topics.push(topic);
                                resp.encode(&mut out, ver).unwrap();
                            }
                            ApiKey::Fetch => {
                                let _ = FetchRequest::decode(&mut buf, ver);
                                let mut resp = FetchResponse::default();
                                let mut ft = FetchableTopicResponse::default();
                                ft.topic = TopicName(StrBytes::from_static_str("stor02"));
                                let mut pd = PartitionData::default();
                                pd.partition_index = 0;
                                ft.partitions.push(pd);
                                resp.responses.push(ft);
                                resp.encode(&mut out, ver).unwrap();
                            }
                            _ => {}
                        }
                        let frame_len = (out.len() - 4) as u32;
                        out[0..4].copy_from_slice(&frame_len.to_be_bytes());
                        if wr.write_all(&out).await.is_err() {
                            break;
                        }
                    }
                });
            }
        });

        let mut cfg = KafkaConfig {
            address: StringSliceCSV(vec![addr.to_string()]),
            topic: "stor02".to_string(),
            auto_create_topic_enabled: false,
            fetch_concurrency_max: 0,
            ..Default::default()
        };
        cfg.target_consumer_lag_at_startup = mimir_config::GoDuration::ZERO;
        cfg.max_consumer_lag_at_startup = mimir_config::GoDuration::ZERO;

        // 1. stop_dependencies() must close KafkaClient even while an extra Arc<SingleClusterPartitionReader> is alive.
        let client1 = Arc::new(KafkaClient::from_config(&cfg).unwrap());
        let reader1 = Arc::new(
            SingleClusterPartitionReader::with_client(
                cfg.clone(),
                0,
                "",
                None,
                Arc::clone(&client1),
                Arc::new(NoOpConsumer),
                None,
            )
            .unwrap(),
        );
        let reader1_keepalive = Arc::clone(&reader1);
        reader1.start().await.unwrap();
        assert!(!client1.is_closed());
        assert!(client1.active_conn_count().await >= 1);
        assert!(active_conns.load(Ordering::SeqCst) >= 1);

        let _ = reader1.stop_tx.send(true);
        if let Some(h) = reader1.task.lock().await.take() {
            let _ = h.await;
        }
        reader1.stop_dependencies().await;
        assert!(
            client1.is_closed(),
            "stop_dependencies() must close KafkaClient even with live Arc<SingleClusterPartitionReader>"
        );
        assert_eq!(client1.active_conn_count().await, 0);
        let deadline1 = tokio::time::Instant::now() + Duration::from_secs(1);
        while active_conns.load(Ordering::SeqCst) > 0 {
            assert!(tokio::time::Instant::now() < deadline1, "TCP connections leaked after stop_dependencies()");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        drop(reader1_keepalive);

        // 2. RuntimeFailureCtx::fail_and_stop() must close KafkaClient even while an extra Arc<SingleClusterPartitionReader> is alive.
        let client2 = Arc::new(KafkaClient::from_config(&cfg).unwrap());
        let reader2 = Arc::new(
            SingleClusterPartitionReader::with_client(
                cfg,
                0,
                "",
                None,
                Arc::clone(&client2),
                Arc::new(NoOpConsumer),
                None,
            )
            .unwrap(),
        );
        let reader2_keepalive = Arc::clone(&reader2);
        let mut failure_rx = reader2.subscribe_failure();
        reader2.start().await.unwrap();
        assert!(!client2.is_closed());
        assert!(client2.active_conn_count().await >= 1);
        assert!(active_conns.load(Ordering::SeqCst) >= 1);

        reader2
            .simulate_runtime_failure_for_test("simulated stor02 failure".to_string())
            .await;
        tokio::time::timeout(Duration::from_secs(2), failure_rx.wait_for(|v| v.is_some()))
            .await
            .expect("failure_tx must be notified")
            .unwrap();
        assert!(
            client2.is_closed(),
            "fail_and_stop() must close KafkaClient even with live Arc<SingleClusterPartitionReader>"
        );
        assert_eq!(client2.active_conn_count().await, 0);
        let deadline2 = tokio::time::Instant::now() + Duration::from_secs(1);
        while active_conns.load(Ordering::SeqCst) > 0 {
            assert!(tokio::time::Instant::now() < deadline2, "TCP connections leaked after fail_and_stop()");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        drop(reader2_keepalive);
        srv.abort();
    }
}
