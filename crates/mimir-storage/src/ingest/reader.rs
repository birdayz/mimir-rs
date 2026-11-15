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

use futures::future::try_join_all;
use mimir_config::{KafkaConfig, OrderedConsumptionConfig};
use parking_lot::Mutex as SyncMutex;
use prometheus::{Counter, CounterVec, GaugeVec, Histogram, HistogramOpts, HistogramVec, Opts, PullingGauge, Registry};
use thiserror::Error;
use tokio::sync::{watch, Mutex};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use mimir_proto::zerocopy::{BorrowedWriteRequest, WriteRequestScratch};

use super::heap_merger::{HeapMerger, HeapMergerMetrics};
use super::kafka_client::{
    FetchedRecord, KafkaClient, KafkaClientError, KAFKA_OFFSET_END, KAFKA_OFFSET_START,
};
use super::offset_catalogue::PartitionOffsets;
use super::offset_file::OffsetFile;
use super::partition_offset_client::PartitionOffsetClient;
use super::partition_offset_reader::SingleClusterPartitionOffsetReader;
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
    #[error("partition reader service is not running")]
    NotRunning,
    #[error("wait strong read consistency timeout exceeded")]
    WaitStrongReadConsistencyTimeoutExceeded,
    #[error("invalid configuration: {0}")]
    Config(String),
    #[error("cluster offset mismatch: expected {expected} clusters, got {got}")]
    ClusterCountMismatch { expected: usize, got: usize },
    #[error("single-cluster partition reader was given read consistency offsets for {0} Kafka clusters")]
    InvalidClusterOffsetCount(usize),
    #[error("startup canceled")]
    Canceled,
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
    pub latency: HistogramVec,
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

        let mut lat_opts = HistogramOpts::new(
            "cortex_ingest_storage_strong_consistency_wait_duration_seconds",
            "How long a request spent waiting for strong consistency to be guaranteed.",
        );
        lat_opts.common_opts.const_labels = const_labels;
        let latency = HistogramVec::new(lat_opts, &["topic"]).expect("valid metric");

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

#[derive(Clone)]
pub struct ReaderMetrics {
    pub fetches_total: Counter,
    pub fetches_errors: Counter,
    pub records_per_fetch: Histogram,
    pub consume_latency: Histogram,
    pub fetch_wait_duration: Histogram,
    pub last_consumed_offset: GaugeVec,
    pub missed_records: Counter,
    pub receive_delay: HistogramVec,
    pub receive_and_consume_delay: HistogramVec,
    pub buffered_fetch_records_client: GaugeVec,
    pub strong_consistency: StrongReadConsistencyMetrics,
    buffered_records_cb: Arc<AtomicI64>,
    buffered_bytes_cb: Arc<AtomicI64>,
}

impl ReaderMetrics {
    pub fn new(registry: Option<&Registry>) -> Self {
        Self::new_for_topic(registry, "")
    }

    pub fn new_for_topic(registry: Option<&Registry>, topic: &str) -> Self {
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
        let records_per_fetch = Histogram::with_opts(HistogramOpts::new(
            "cortex_ingest_storage_reader_records_per_fetch",
            "The number of records received by the consumer in a single fetch operation.",
        ))
        .expect("valid metric");
        let consume_latency = Histogram::with_opts(HistogramOpts::new(
            "cortex_ingest_storage_reader_records_batch_process_duration_seconds",
            "How long a consumer spent processing a batch of records from Kafka.",
        ))
        .expect("valid metric");
        let fetch_wait_duration = Histogram::with_opts(HistogramOpts::new(
            "cortex_ingest_storage_reader_records_batch_wait_duration_seconds",
            "How long a consumer spent waiting for a batch of records from the Kafka client.",
        ))
        .expect("valid metric");
        let last_consumed_offset = GaugeVec::new(
            Opts::new(
                "cortex_ingest_storage_reader_last_consumed_offset",
                "The last offset successfully consumed by the partition reader. Set to -1 if no offset has been consumed yet.",
            ),
            &["partition"],
        )
        .expect("valid metric");
        let missed_records = Counter::with_opts(Opts::new(
            "cortex_ingest_storage_reader_missed_records_total",
            "The number of offsets that were never consumed by the reader because they weren't fetched.",
        ))
        .expect("valid metric");
        let receive_delay = HistogramVec::new(
            HistogramOpts::new(
                "cortex_ingest_storage_reader_receive_delay_seconds",
                "Delay between producing a record and receiving it in the consumer.",
            ),
            &["phase"],
        )
        .expect("valid metric");
        let receive_and_consume_delay = HistogramVec::new(
            HistogramOpts::new(
                "cortex_ingest_storage_reader_receive_and_consume_delay_seconds",
                "Delay between producing a record and consuming it in the consumer.",
            ),
            &["phase"],
        )
        .expect("valid metric");
        let buffered_fetch_records_client = GaugeVec::new(
            Opts::new(
                "cortex_ingest_storage_reader_buffered_fetch_records_total",
                "Total number of records buffered within the client ready to be consumed",
            ),
            &["component"],
        )
        .expect("valid metric");
        buffered_fetch_records_client
            .with_label_values(&["partition-reader"])
            .set(0.0);

        let buffered_records_cb = Arc::new(AtomicI64::new(0));
        let buffered_bytes_cb = Arc::new(AtomicI64::new(0));

        let br_clone = Arc::clone(&buffered_records_cb);
        let buffered_fetched_records = PullingGauge::new(
            "cortex_ingest_storage_reader_buffered_fetched_records",
            "The number of records fetched from Kafka by both concurrent fetchers and the Kafka client but not yet processed.",
            Box::new(move || br_clone.load(Ordering::Relaxed) as f64),
        )
        .expect("valid metric");

        let bb_clone = Arc::clone(&buffered_bytes_cb);
        let buffered_fetched_bytes = PullingGauge::new(
            "cortex_ingest_storage_reader_buffered_fetched_bytes",
            "The number of bytes fetched or requested from Kafka by both concurrent fetchers and the Kafka client but not yet processed.",
            Box::new(move || bb_clone.load(Ordering::Relaxed) as f64),
        )
        .expect("valid metric");

        let topics_arr = if topic.is_empty() { vec![] } else { vec![topic] };
        let strong_consistency =
            StrongReadConsistencyMetrics::new(registry, "partition-reader", &topics_arr);

        if let Some(reg) = registry {
            let _ = reg.register(Box::new(fetches_total.clone()));
            let _ = reg.register(Box::new(fetches_errors.clone()));
            let _ = reg.register(Box::new(records_per_fetch.clone()));
            let _ = reg.register(Box::new(consume_latency.clone()));
            let _ = reg.register(Box::new(fetch_wait_duration.clone()));
            let _ = reg.register(Box::new(last_consumed_offset.clone()));
            let _ = reg.register(Box::new(missed_records.clone()));
            let _ = reg.register(Box::new(receive_delay.clone()));
            let _ = reg.register(Box::new(receive_and_consume_delay.clone()));
            let _ = reg.register(Box::new(buffered_fetch_records_client.clone()));
            let _ = reg.register(Box::new(buffered_fetched_records));
            let _ = reg.register(Box::new(buffered_fetched_bytes));
        }

        Self {
            fetches_total,
            fetches_errors,
            records_per_fetch,
            consume_latency,
            fetch_wait_duration,
            last_consumed_offset,
            missed_records,
            receive_delay,
            receive_and_consume_delay,
            buffered_fetch_records_client,
            strong_consistency,
            buffered_records_cb,
            buffered_bytes_cb,
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
    notifier: Arc<dyn PreCommitNotifier>,
    offset_file: Option<Arc<OffsetFile>>,
    commit_requests_total: Counter,
    commit_failures_total: Counter,
    commit_requests_latency: Histogram,
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

        let mut lat_opts = HistogramOpts::new(
            "cortex_ingest_storage_reader_offset_commit_request_duration_seconds",
            "The duration of requests to commit the last consumed offset.",
        );
        lat_opts.common_opts.const_labels = const_labels.clone();
        let commit_requests_latency = Histogram::with_opts(lat_opts).expect("valid metric");

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
            notifier,
            offset_file,
            commit_requests_total,
            commit_failures_total,
            commit_requests_latency,
            stop_tx,
            task: Mutex::new(None),
        }
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
                    _ = stop_rx.changed() => {
                        if *stop_rx.borrow() {
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
        if offset >= 0 && offset != self.last_committed_offset.load(Ordering::SeqCst) {
            let _ = self.commit(offset).await;
        }
    }

    pub async fn commit(&self, offset: i64) -> Result<(), String> {
        let start = Instant::now();
        self.commit_requests_total.inc();

        if let Err(e) = self.notifier.notify_pre_commit().await {
            warn!(partition = self.partition_id, offset, err = %e, "pre-commit notification failed, continuing with commit");
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
            let err_msg = errs.join("; ");
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
    metrics: ReaderMetrics,
    last_seen_offset: Arc<AtomicI64>,
    highest_consumed_ts_before_end_ms: Arc<AtomicI64>,
    max_replay_period: Duration,
    concurrent_fetchers_min_bytes_max_wait_time: Duration,
    state: Arc<AtomicU8>,
    log_sink: Option<Arc<SyncMutex<Vec<String>>>>,
    stop_tx: watch::Sender<bool>,
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
        let consumer_group = if kafka_cfg.consumer_group.is_empty() {
            format!("{instance_id}-{partition_id}")
        } else {
            format!("{}-{instance_id}-{partition_id}", kafka_cfg.consumer_group)
        };
        let offset_file = offset_file_path.map(|p| Arc::new(OffsetFile::new(p, partition_id)));
        let offset_watcher = Arc::new(PartitionOffsetWatcher::new());
        let offset_client = PartitionOffsetClient::new(Arc::clone(&client), registry);
        let offset_reader = Arc::new(SingleClusterPartitionOffsetReader::new(
            offset_client,
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
            metrics,
            last_seen_offset: Arc::new(AtomicI64::new(-1)),
            highest_consumed_ts_before_end_ms: Arc::new(AtomicI64::new(0)),
            max_replay_period: Duration::ZERO,
            concurrent_fetchers_min_bytes_max_wait_time: fetch_max_wait,
            state: Arc::new(AtomicU8::new(ReaderState::New as u8)),
            log_sink: None,
            stop_tx,
            task: Mutex::new(None),
            start_task: Mutex::new(None),
        })
    }

    pub fn with_max_replay_period(mut self, period: Duration) -> Self {
        self.max_replay_period = period;
        self
    }

    pub fn with_pre_commit_notifier(mut self, notifier: Arc<dyn PreCommitNotifier>) -> Self {
        let adm: Arc<dyn AdmClient> = Arc::new(KafkaAdmClient::new(Arc::clone(&self.client)));
        // Rebuild committer without re-registering duplicate metrics if registry wasn't passed
        self.committer = Arc::new(PartitionCommitter::new(
            self.kafka_cfg.clone(),
            adm,
            self.partition_id,
            self.consumer_group.clone(),
            Arc::clone(&notifier),
            self.offset_file.clone(),
            None,
        ));
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

    pub fn buffered_records(&self) -> i64 {
        self.metrics.buffered_records_cb.load(Ordering::Relaxed)
    }

    pub fn buffered_bytes(&self) -> i64 {
        self.metrics.buffered_bytes_cb.load(Ordering::Relaxed)
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
                if let Some((offset, matched)) = self.fetch_first_offset_after_time(ts).await? {
                    if matched {
                        return Ok((offset, offset - 1));
                    }
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
                            let part_start = self
                                .client
                                .fetch_partition_start_offset(topic, self.partition_id)
                                .await?;
                            if file_offset >= part_start {
                                return Ok((file_offset + 1, file_offset));
                            }
                            warn!(
                                file_offset,
                                part_start,
                                "file-stored offset no longer exists for partition, resolving from max replay period or partition start"
                            );
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

    async fn resolve_concrete_fetch_offset(&self, start_offset: i64) -> Result<i64, ReaderError> {
        let topic = &self.kafka_cfg.topic;
        if start_offset == KAFKA_OFFSET_START {
            Ok(self
                .client
                .fetch_partition_start_offset(topic, self.partition_id)
                .await?)
        } else if start_offset == KAFKA_OFFSET_END {
            let last = self
                .client
                .fetch_partition_last_produced_offset(topic, self.partition_id)
                .await?;
            Ok((last + 1).max(0))
        } else {
            Ok(start_offset.max(0))
        }
    }

    /// Starts the partition reader background fetch/consume loop and offset poller.
    pub async fn start(&self) -> Result<(), ReaderError> {
        let mut guard = self.task.lock().await;
        if guard.is_some() {
            return Ok(());
        }
        self.state
            .store(ReaderState::Starting as u8, Ordering::SeqCst);

        if self.kafka_cfg.auto_create_topic_enabled {
            let topic_ref = [self.kafka_cfg.topic.as_str()];
            if let Err(e) = create_topics(&self.kafka_cfg, &topic_ref).await {
                self.state.store(ReaderState::Failed as u8, Ordering::SeqCst);
                return Err(ReaderError::Startup(e));
            }
        }

        if self.kafka_cfg.fetch_concurrency_max > 0 && !self.kafka_cfg.client_rack.is_empty() {
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

        let mut startup_stop_rx = self.stop_tx.subscribe();
        let (start_offset, last_consumed, concrete_start) = {
            let mut attempt = 0u32;
            loop {
                if *startup_stop_rx.borrow() {
                    self.state.store(ReaderState::Failed as u8, Ordering::SeqCst);
                    return Err(ReaderError::Canceled);
                }
                let res = async {
                    let (so, lc) = self.get_start_offset().await?;
                    let cs = self.resolve_concrete_fetch_offset(so).await?;
                    Ok::<_, ReaderError>((so, lc, cs))
                }
                .await;
                match res {
                    Ok(v) => break v,
                    Err(e) => {
                        if matches!(e, ReaderError::Config(_)) || attempt >= 10 {
                            self.state.store(ReaderState::Failed as u8, Ordering::SeqCst);
                            return Err(e);
                        }
                        attempt += 1;
                        warn!(partition = self.partition_id, error = %e, "failed to fetch offset; retrying");
                        tokio::select! {
                            _ = startup_stop_rx.changed() => {
                                if *startup_stop_rx.borrow() {
                                    self.state.store(ReaderState::Failed as u8, Ordering::SeqCst);
                                    return Err(ReaderError::Canceled);
                                }
                            }
                            _ = tokio::time::sleep(Duration::from_millis(50)) => {}
                        }
                    }
                }
            }
        };

        self.last_seen_offset.store(last_consumed, Ordering::SeqCst);
        if last_consumed >= 0 {
            self.offset_watcher.notify(last_consumed);
            self.metrics
                .last_consumed_offset
                .with_label_values(&[&self.partition_id.to_string()])
                .set(last_consumed as f64);
        }

        self.offset_reader.start().await;
        self.committer.start().await;

        let mut next_offset = concrete_start;

        // Enforce startup consumer lag if configured and not starting from end.
        let target_lag = Duration::from_nanos(
            self.kafka_cfg.target_consumer_lag_at_startup.as_nanos().max(0) as u64,
        );
        let max_lag = Duration::from_nanos(
            self.kafka_cfg.max_consumer_lag_at_startup.as_nanos().max(0) as u64,
        );
        if !target_lag.is_zero() && !max_lag.is_zero() && start_offset != KAFKA_OFFSET_END {
            match self
                .catchup_startup_lag(&mut next_offset, target_lag, max_lag)
                .await
            {
                Ok(()) => {}
                Err(e) => {
                    self.state.store(ReaderState::Failed as u8, Ordering::SeqCst);
                    self.offset_reader.stop().await;
                    self.committer.stop().await;
                    return Err(e);
                }
            }
        }

        let client = Arc::clone(&self.client);
        let consumer = Arc::clone(&self.consumer);
        let offset_watcher = Arc::clone(&self.offset_watcher);
        let committer = Arc::clone(&self.committer);
        let metrics = self.metrics.clone();
        let last_seen_offset = Arc::clone(&self.last_seen_offset);
        let highest_ts_ref = Arc::clone(&self.highest_consumed_ts_before_end_ms);
        let topic = self.kafka_cfg.topic.clone();
        let partition_id = self.partition_id;
        let fetch_max_wait = Duration::from_nanos(
            self.kafka_cfg.fetch_max_wait.as_nanos().max(0) as u64,
        )
        .min(Duration::from_millis(250))
        .max(Duration::from_millis(20));
        let max_partition_bytes = Self::partition_max_bytes(&self.kafka_cfg);
        let mut stop_rx = self.stop_tx.subscribe();

        let handle = tokio::spawn(async move {
            loop {
                if *stop_rx.borrow() {
                    break;
                }

                let fetch_start = Instant::now();
                let fetch_res = tokio::select! {
                    _ = stop_rx.changed() => {
                        if *stop_rx.borrow() {
                            break;
                        }
                        continue;
                    }
                    res = client.fetch(&topic, partition_id, next_offset, fetch_max_wait, 1, max_partition_bytes) => res,
                };
                metrics
                    .fetch_wait_duration
                    .observe(fetch_start.elapsed().as_secs_f64());

                match fetch_res {
                    Ok(batch) => {
                        metrics.fetches_total.inc();
                        metrics
                            .records_per_fetch
                            .observe(batch.records.len() as f64);

                        if !batch.records.is_empty() {
                            let batch_last_offset = batch.records.last().unwrap().offset;
                            let first_ts = batch.records.first().unwrap().timestamp_ms;
                            let highest_ts =
                                batch.records.iter().map(|r| r.timestamp_ms).max().unwrap_or(0);
                            last_seen_offset.fetch_max(batch_last_offset, Ordering::SeqCst);

                            let now_ms = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_millis() as i64;
                            for r in &batch.records {
                                let delay = ((now_ms - r.timestamp_ms).max(0) as f64) / 1000.0;
                                metrics
                                    .receive_delay
                                    .with_label_values(&["running"])
                                    .observe(delay);
                            }

                            if highest_ts_ref.load(Ordering::SeqCst) == 0 {
                                highest_ts_ref.store(first_ts, Ordering::SeqCst);
                            }

                            metrics
                                .buffered_records_cb
                                .store(batch.records.len() as i64, Ordering::Relaxed);
                            let total_b: i64 =
                                batch.records.iter().map(|r| r.value.len() as i64).sum();
                            metrics
                                .buffered_bytes_cb
                                .store(total_b, Ordering::Relaxed);

                            let consume_start = Instant::now();
                            loop {
                                match consumer.consume(batch.records.clone()).await {
                                    Ok(()) => break,
                                    Err(err) => {
                                        error!(
                                            partition = partition_id,
                                            error = %err,
                                            "error consuming Kafka batch; retrying"
                                        );
                                        if *stop_rx.borrow() {
                                            break;
                                        }
                                        tokio::time::sleep(Duration::from_millis(50)).await;
                                    }
                                }
                            }
                            metrics
                                .buffered_records_cb
                                .store(0, Ordering::Relaxed);
                            metrics.buffered_bytes_cb.store(0, Ordering::Relaxed);
                            metrics
                                .consume_latency
                                .observe(consume_start.elapsed().as_secs_f64());

                            if batch_last_offset + 1 >= batch.high_watermark {
                                highest_ts_ref.store(0, Ordering::SeqCst);
                            } else {
                                highest_ts_ref.store(highest_ts, Ordering::SeqCst);
                            }

                            committer.enqueue_offset(batch_last_offset);
                            offset_watcher.notify(batch_last_offset);
                            metrics
                                .last_consumed_offset
                                .with_label_values(&[&partition_id.to_string()])
                                .set(batch_last_offset as f64);

                            next_offset = batch_last_offset + 1;
                        }
                    }
                    Err(err) => {
                        metrics.fetches_errors.inc();
                        debug!(partition = partition_id, error = %err, "kafka fetch error");
                        tokio::select! {
                            _ = stop_rx.changed() => {
                                if *stop_rx.borrow() {
                                    break;
                                }
                            }
                            _ = tokio::time::sleep(Duration::from_millis(50)) => {}
                        }
                    }
                }
            }
        });

        *guard = Some(handle);
        self.state
            .store(ReaderState::Running as u8, Ordering::SeqCst);
        info!(partition = self.partition_id, start_offset, "started partition reader");
        Ok(())
    }

    fn partition_max_bytes(cfg: &KafkaConfig) -> i32 {
        let mut max_b = if cfg.max_buffered_bytes > 0 {
            cfg.max_buffered_bytes as i32
        } else {
            50 * 1024 * 1024
        };
        if cfg.fetch_concurrency_max > 1 {
            max_b = (max_b / cfg.fetch_concurrency_max as i32).max(1024 * 1024);
        }
        max_b
    }

    async fn catchup_startup_lag(
        &self,
        next_offset: &mut i64,
        target_lag: Duration,
        max_lag: Duration,
    ) -> Result<(), ReaderError> {
        let mut stop_rx = self.stop_tx.subscribe();
        let max_partition_bytes = Self::partition_max_bytes(&self.kafka_cfg);

        // Attempt 1: honor max_lag; Attempt 2: best-effort target_lag within 2*max_lag; Attempt 3: ensure max_lag
        let mut curr_lag = self
            .process_fetches_until_lag(
                next_offset,
                max_lag,
                None,
                max_partition_bytes,
                &mut stop_rx,
            )
            .await?;

        if curr_lag > target_lag {
            match self
                .process_fetches_until_lag(
                    next_offset,
                    target_lag,
                    Some(2 * max_lag),
                    max_partition_bytes,
                    &mut stop_rx,
                )
                .await
            {
                Ok(l) => curr_lag = l,
                Err(ReaderError::WaitStrongReadConsistencyTimeoutExceeded) => {
                    curr_lag = self
                        .process_fetches_until_lag(
                            next_offset,
                            max_lag,
                            None,
                            max_partition_bytes,
                            &mut stop_rx,
                        )
                        .await?;
                }
                Err(e) => return Err(e),
            }
        }

        if curr_lag <= target_lag {
            let msg = "partition reader consumed partition and current lag is lower than configured target consumer lag";
            info!(partition = self.partition_id, "{msg}");
            if let Some(ref sink) = self.log_sink {
                sink.lock().push(msg.to_string());
            }
        } else {
            let msg = "partition reader consumed partition and current lag is lower than configured max consumer lag but higher than target consumer lag";
            warn!(partition = self.partition_id, "{msg}");
            if let Some(ref sink) = self.log_sink {
                sink.lock().push(msg.to_string());
            }
        }
        Ok(())
    }

    async fn process_fetches_until_lag(
        &self,
        next_offset: &mut i64,
        desired_lag: Duration,
        deadline_dur: Option<Duration>,
        max_partition_bytes: i32,
        stop_rx: &mut watch::Receiver<bool>,
    ) -> Result<Duration, ReaderError> {
        let deadline = deadline_dur.map(|d| Instant::now() + d);
        loop {
            if *stop_rx.borrow() {
                return Err(ReaderError::Canceled);
            }
            if let Some(dl) = deadline {
                if Instant::now() >= dl {
                    return Err(ReaderError::WaitStrongReadConsistencyTimeoutExceeded);
                }
            }

            let part_start = match self.offset_reader.fetch_partition_start_offset().await {
                Ok(v) => v,
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
            };
            let req_at = Instant::now();
            let last_produced = match self.offset_reader.fetch_last_produced_offset().await {
                Ok(v) => v,
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
            };

            if part_start > last_produced {
                return Ok(Duration::ZERO);
            }

            while self.offset_watcher.last_consumed_offset() < last_produced {
                if *stop_rx.borrow() {
                    return Err(ReaderError::Canceled);
                }
                if let Some(dl) = deadline {
                    if Instant::now() >= dl {
                        return Err(ReaderError::WaitStrongReadConsistencyTimeoutExceeded);
                    }
                }
                let fetch_res = tokio::select! {
                    _ = stop_rx.changed() => {
                        if *stop_rx.borrow() {
                            return Err(ReaderError::Canceled);
                        }
                        continue;
                    }
                    r = self.client.fetch(
                        &self.kafka_cfg.topic,
                        self.partition_id,
                        *next_offset,
                        Duration::from_millis(100),
                        1,
                        max_partition_bytes,
                    ) => r,
                };
                match fetch_res {
                    Ok(batch) => {
                        self.metrics.fetches_total.inc();
                        self.metrics
                            .records_per_fetch
                            .observe(batch.records.len() as f64);
                        if !batch.records.is_empty() {
                            let last_off = batch.records.last().unwrap().offset;
                            let first_ts = batch.records.first().unwrap().timestamp_ms;
                            let high_ts = batch
                                .records
                                .iter()
                                .map(|r| r.timestamp_ms)
                                .max()
                                .unwrap_or(0);
                            self.last_seen_offset
                                .fetch_max(last_off, Ordering::SeqCst);
                            if self.highest_consumed_ts_before_end_ms.load(Ordering::SeqCst) == 0 {
                                self.highest_consumed_ts_before_end_ms
                                    .store(first_ts, Ordering::SeqCst);
                            }

                            while let Err(e) = self.consumer.consume(batch.records.clone()).await {
                                error!(partition = self.partition_id, error = %e, "error consuming startup batch; retrying");
                                if *stop_rx.borrow() {
                                    return Err(ReaderError::Canceled);
                                }
                                tokio::time::sleep(Duration::from_millis(50)).await;
                            }

                            if last_off + 1 >= batch.high_watermark {
                                self.highest_consumed_ts_before_end_ms
                                    .store(0, Ordering::SeqCst);
                            } else {
                                self.highest_consumed_ts_before_end_ms
                                    .store(high_ts, Ordering::SeqCst);
                            }

                            self.committer.enqueue_offset(last_off);
                            self.offset_watcher.notify(last_off);
                            self.metrics
                                .last_consumed_offset
                                .with_label_values(&[&self.partition_id.to_string()])
                                .set(last_off as f64);
                            *next_offset = last_off + 1;
                        }
                    }
                    Err(_) => {
                        self.metrics.fetches_errors.inc();
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
            }

            let curr_lag = req_at.elapsed();
            if curr_lag <= desired_lag {
                return Ok(curr_lag);
            }
        }
    }

    pub async fn start_async(self: &Arc<Self>) {
        let mut guard = self.start_task.lock().await;
        if guard.is_some() {
            return;
        }
        self.state
            .store(ReaderState::Starting as u8, Ordering::SeqCst);
        let this = Arc::clone(self);
        *guard = Some(tokio::spawn(async move { this.start().await }));
    }

    pub async fn stop(&self) {
        self.state
            .store(ReaderState::Stopping as u8, Ordering::SeqCst);
        let _ = self.stop_tx.send(true);
        self.offset_watcher.stop();
        if let Some(sh) = self.start_task.lock().await.take() {
            let _ = sh.await;
        }
        self.offset_reader.stop().await;
        if let Some(handle) = self.task.lock().await.take() {
            let _ = handle.await;
        }
        self.committer.stop().await;
        if self.state() != ReaderState::Failed {
            self.state
                .store(ReaderState::Terminated as u8, Ordering::SeqCst);
        }
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
        self.metrics
            .strong_consistency
            .requests
            .with_label_values(&[if with_offset { "true" } else { "false" }, topic])
            .inc();
        let start = Instant::now();

        let configured_timeout = Duration::from_nanos(
            self.kafka_cfg
                .wait_strong_read_consistency_timeout
                .as_nanos()
                .max(0) as u64,
        );
        let (effective_timeout, is_configured) = if !configured_timeout.is_zero()
            && (caller_timeout.is_zero() || configured_timeout < caller_timeout)
        {
            (configured_timeout, true)
        } else {
            (caller_timeout, false)
        };

        let res = async {
            if self.state() != ReaderState::Running {
                return Err(ReaderError::NotRunning);
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
                            ReaderError::Watcher(OffsetWatcherError::Timeout(effective_timeout))
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
                    ReaderError::Watcher(OffsetWatcherError::Timeout(effective_timeout))
                });
            }

            self.offset_watcher
                .wait_for_offset(target, rem)
                .await
                .map_err(|e| match e {
                    OffsetWatcherError::Timeout(_) if is_configured => {
                        ReaderError::WaitStrongReadConsistencyTimeoutExceeded
                    }
                    other => ReaderError::Watcher(other),
                })
        }
        .await;

        self.metrics
            .strong_consistency
            .latency
            .with_label_values(&[topic])
            .observe(start.elapsed().as_secs_f64());

        if res.is_err() {
            self.metrics
                .strong_consistency
                .failures
                .with_label_values(&[topic])
                .inc();
        }
        res
    }

    pub fn enforce_read_max_delay(&self, max_delay: Duration) -> Result<(), ReaderError> {
        if max_delay.is_zero() {
            return Ok(());
        }
        let ts_ms = self.highest_consumed_ts_before_end_ms.load(Ordering::SeqCst);
        if ts_ms <= 0 {
            return Ok(());
        }
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        if now_ms.saturating_sub(ts_ms) > max_delay.as_millis() as i64 {
            return Err(ReaderError::MaxDelayExceeded);
        }
        Ok(())
    }
}

/// Multi-cluster partition reader consuming the same partition ID across multiple Kafka write
/// compartments, optionally merging records in timestamp order via [`HeapMerger`].
pub struct MultiClusterPartitionReader {
    readers: Vec<Arc<SingleClusterPartitionReader>>,
    merger: Option<Mutex<Option<HeapMerger>>>,
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
            consumer,
            Arc::new(NoOpPreCommitNotifier),
            Duration::ZERO,
            registry,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_notifier(
        cluster_configs: &[KafkaConfig],
        ordered_cfg: &OrderedConsumptionConfig,
        partition_id: i32,
        instance_id: &str,
        offset_file_paths: Vec<Option<PathBuf>>,
        consumer: Arc<dyn RecordBatchConsumer>,
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
            let downstream = Arc::clone(&consumer);
            let cb = Arc::new(move |recs: Vec<FetchedRecord>| {
                let d = Arc::clone(&downstream);
                Box::pin(async move { d.consume(recs).await })
                    as Pin<Box<dyn Future<Output = Result<(), String>> + Send>>
            });
            Some(HeapMerger::start(
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

        for (cluster_id, (cfg, off_path)) in cluster_configs
            .iter()
            .zip(offset_file_paths)
            .enumerate()
        {
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
                    Arc::clone(&consumer)
                };

            let cluster_instance = format!("{instance_id}-wc-{cluster_id}");
            let mut r = SingleClusterPartitionReader::new_with_notifier(
                cfg.clone(),
                partition_id,
                &cluster_instance,
                off_path,
                cluster_consumer,
                Arc::clone(&notifier),
                registry,
            )?;
            if !max_replay_period.is_zero() {
                r = r.with_max_replay_period(max_replay_period);
            }
            readers.push(Arc::new(r));
        }

        let merger_holder = shared_merger
            .and_then(|arc| Arc::try_unwrap(arc).ok())
            .map(|m| Mutex::new(Some(m)));

        Ok(Self {
            readers,
            merger: merger_holder,
        })
    }

    pub async fn start(&self) -> Result<(), ReaderError> {
        let futs = self.readers.iter().map(|r| r.start());
        try_join_all(futs).await?;
        Ok(())
    }

    pub async fn stop(&self) {
        for r in &self.readers {
            r.stop().await;
        }
        if let Some(ref m_lock) = self.merger {
            if let Some(m) = m_lock.lock().await.take() {
                m.stop().await;
            }
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
        let futs = self.readers.iter().enumerate().map(|(cid, r)| {
            let target = offsets.for_kafka_cluster(cid);
            async move { r.wait_for_offset(target, timeout).await }
        });
        try_join_all(futs).await?;
        Ok(())
    }

    pub async fn wait_read_consistency_until_last_produced_offset(
        &self,
        timeout: Duration,
    ) -> Result<(), ReaderError> {
        let futs = self
            .readers
            .iter()
            .map(|r| r.wait_read_consistency_until_last_produced_offset(timeout));
        try_join_all(futs).await?;
        Ok(())
    }

    pub fn enforce_read_max_delay(&self, max_delay: Duration) -> Result<(), ReaderError> {
        for r in &self.readers {
            r.enforce_read_max_delay(max_delay)?;
        }
        Ok(())
    }
}
