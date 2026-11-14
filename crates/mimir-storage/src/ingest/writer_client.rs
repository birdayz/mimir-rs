//! Kafka producer client, buffer-limit admission, extended transport metrics, and retry/linger batching.
//!
//! Ported from github.com/grafana/mimir/pkg/storage/ingest/writer_client.go

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use kafka_protocol::error::ResponseError;
use mimir_config::KafkaConfig;
use parking_lot::Mutex as SyncMutex;
use prometheus::core::{Collector, Desc};
use prometheus::proto::{Metric as ProtoMetric, MetricFamily, MetricType, Quantile, Summary as ProtoSummary};
use prometheus::{Counter, CounterVec, Gauge, Histogram, HistogramOpts, Opts, Registry};
use thiserror::Error;
use tokio::sync::{watch, Mutex, Notify, Semaphore};

use super::compression::KafkaCompression;
use super::kafka_client::{KafkaClientError, ProduceRecord};
use super::util::{IngestKafkaClient, KafkaClientExtendedMetrics};

pub const DEFAULT_PRODUCER_LINGER: Duration = Duration::from_millis(50);
pub const DEFAULT_METADATA_REFRESH_INTERVAL: Duration = Duration::from_secs(10);
pub const WRITER_METRICS_PREFIX: &str = "cortex_ingest_storage_writer_";
const PRODUCER_BATCH_MAX_BYTES: usize = 16_000_000;

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum ProducerError {
    #[error("skipped producing Kafka records because context is already done: {0}")]
    CancelledBeforeProducing(String),
    #[error("waiting for Kafka records to be produced and acknowledged: {0}")]
    CancelledWhileWaiting(String),
    #[error("context deadline exceeded")]
    DeadlineExceeded,
    #[error("context canceled")]
    Canceled,
    #[error("Kafka record Timestamp must not be set by the caller; it is reserved for the Kafka client to track produce time")]
    TimestampSet,
    #[error("maximum buffered bytes reached")]
    MaxBuffered,
    #[error("record delivery timeout exceeded")]
    RecordTimeout,
    #[error("message too large")]
    MessageTooLarge,
    #[error("kafka error: {0}")]
    Other(String),
}

impl ProducerError {
    pub fn reason(&self) -> &'static str {
        match self {
            Self::RecordTimeout | Self::DeadlineExceeded => "timeout",
            Self::MaxBuffered => "buffer-full",
            Self::MessageTooLarge => "record-too-large",
            Self::Canceled => "canceled",
            Self::CancelledWhileWaiting(msg) | Self::CancelledBeforeProducing(msg) => {
                if msg.contains("deadline exceeded") {
                    "timeout"
                } else if msg.contains("canceled") {
                    "canceled"
                } else {
                    "other"
                }
            }
            _ => "other",
        }
    }

    pub fn is_timeout(&self) -> bool {
        matches!(self, Self::RecordTimeout | Self::DeadlineExceeded)
            || matches!(self, Self::CancelledBeforeProducing(m) | Self::CancelledWhileWaiting(m) if m.contains("deadline exceeded"))
    }

    pub fn is_canceled(&self) -> bool {
        matches!(self, Self::Canceled)
            || matches!(self, Self::CancelledBeforeProducing(m) | Self::CancelledWhileWaiting(m) if m.contains("canceled"))
    }
}

/// Cancellation and deadline context for `KafkaProducer::produce_sync`.
#[derive(Clone, Default)]
pub struct ProduceContext {
    pub deadline: Option<Instant>,
    cancel_rx: Option<watch::Receiver<Option<String>>>,
}

pub struct ProduceCancelSource {
    tx: watch::Sender<Option<String>>,
}

impl ProduceCancelSource {
    pub fn new() -> (Self, ProduceContext) {
        let (tx, rx) = watch::channel(None);
        (
            Self { tx },
            ProduceContext {
                deadline: None,
                cancel_rx: Some(rx),
            },
        )
    }

    pub fn with_timeout(dur: Duration) -> (Self, ProduceContext) {
        let (tx, rx) = watch::channel(None);
        (
            Self { tx },
            ProduceContext {
                deadline: Some(Instant::now() + dur),
                cancel_rx: Some(rx),
            },
        )
    }

    pub fn with_deadline(deadline: Instant) -> (Self, ProduceContext) {
        let (tx, rx) = watch::channel(None);
        (
            Self { tx },
            ProduceContext {
                deadline: Some(deadline),
                cancel_rx: Some(rx),
            },
        )
    }

    pub fn cancel(&self) {
        let _ = self.tx.send(Some("context canceled".to_string()));
    }

    pub fn cancel_with_cause(&self, cause: impl Into<String>) {
        let _ = self.tx.send(Some(cause.into()));
    }
}

impl ProduceContext {
    pub fn background() -> Self {
        Self::default()
    }

    pub fn with_deadline(mut self, deadline: Instant) -> Self {
        self.deadline = Some(deadline);
        self
    }

    pub fn with_timeout(mut self, dur: Duration) -> Self {
        self.deadline = Some(Instant::now() + dur);
        self
    }

    pub fn err(&self) -> Option<String> {
        if let Some(ref rx) = self.cancel_rx {
            if let Some(ref cause) = *rx.borrow() {
                return Some(cause.clone());
            }
        }
        if let Some(dl) = self.deadline {
            if Instant::now() >= dl {
                return Some("context deadline exceeded".to_string());
            }
        }
        None
    }

    pub async fn done(&self) -> String {
        if let Some(e) = self.err() {
            return e;
        }
        let mut rx = self.cancel_rx.clone();
        let deadline = self.deadline;

        let cancel_fut = async {
            if let Some(ref mut r) = rx {
                loop {
                    if let Some(ref c) = *r.borrow() {
                        return c.clone();
                    }
                    if r.changed().await.is_err() {
                        return "context canceled".to_string();
                    }
                }
            } else {
                std::future::pending::<String>().await
            }
        };

        let deadline_fut = async {
            if let Some(dl) = deadline {
                tokio::time::sleep_until(tokio::time::Instant::from_std(dl)).await;
                "context deadline exceeded".to_string()
            } else {
                std::future::pending::<String>().await
            }
        };

        tokio::select! {
            c = cancel_fut => c,
            d = deadline_fut => d,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct WriterRecord {
    pub topic: String,
    pub partition: i32,
    pub key: Bytes,
    pub value: Bytes,
    pub headers: Vec<(String, Bytes)>,
    pub timestamp_ms: Option<i64>,
    pub offset: i64,
}

#[derive(Debug, Clone)]
pub struct ProduceResult {
    pub record: WriterRecord,
    pub err: Option<ProducerError>,
}

pub fn first_produce_err(results: &[ProduceResult]) -> Option<&ProducerError> {
    results.iter().find_map(|r| r.err.as_ref())
}

#[derive(Debug, Clone, Default)]
pub struct KafkaWriterClientOptions {
    pub disable_default_topic: bool,
    pub disable_linger: bool,
}

/// Translates configured producer compression codec name into `KafkaCompression`.
pub fn kafka_producer_compression_from_config(name: &str) -> Option<KafkaCompression> {
    match name {
        "none" => Some(KafkaCompression::None),
        "gzip" => Some(KafkaCompression::Gzip),
        "snappy" => Some(KafkaCompression::Snappy),
        "lz4" => Some(KafkaCompression::Lz4),
        "zstd" => Some(KafkaCompression::Zstd),
        _ => None,
    }
}

/// Minimum surface `KafkaProducer` needs from the underlying client (matching `KafkaProducerClient` in Go).
pub trait KafkaProducerClient: Send + Sync + 'static {
    fn produce(
        &self,
        record: WriterRecord,
        on_done: Box<dyn FnOnce(WriterRecord, Result<i64, ProducerError>) + Send>,
    );
    fn buffered_produce_bytes(&self) -> i64;
    fn buffered_produce_records(&self) -> i64;
    fn close(&self);
}

struct QueuedItem {
    record: WriterRecord,
    enqueued_at: Instant,
    on_done: Box<dyn FnOnce(WriterRecord, Result<i64, ProducerError>) + Send>,
}

/// Async Kafka writer client with batching, linger, max in-flight requests, delivery timeouts,
/// SASL authentication, and extended Kafka client metrics.
pub struct KafkaWriterClient {
    inner: Arc<IngestKafkaClient>,
    default_topic: Option<String>,
    compression: KafkaCompression,
    linger: Duration,
    max_inflight: usize,
    write_timeout: Duration,
    write_timeout_overhead: Duration,
    buffered_bytes: Arc<AtomicI64>,
    buffered_records: Arc<AtomicI64>,
    queue: Arc<SyncMutex<VecDeque<QueuedItem>>>,
    notify: Arc<Notify>,
    closed: Arc<AtomicBool>,
    extended_metrics: Option<KafkaClientExtendedMetrics>,
}

impl KafkaWriterClient {
    pub fn new(
        cfg: &KafkaConfig,
        max_inflight_produce_requests: usize,
        registry: Option<&Registry>,
        opts: KafkaWriterClientOptions,
    ) -> Result<Arc<Self>, KafkaClientError> {
        Self::new_with_prefix(cfg, max_inflight_produce_requests, "", registry, opts)
    }

    pub fn new_with_prefix(
        cfg: &KafkaConfig,
        max_inflight_produce_requests: usize,
        metrics_prefix: &str,
        registry: Option<&Registry>,
        opts: KafkaWriterClientOptions,
    ) -> Result<Arc<Self>, KafkaClientError> {
        let extended_metrics = if registry.is_some() {
            Some(KafkaClientExtendedMetrics::new(metrics_prefix, registry, None))
        } else {
            None
        };
        let inner = Arc::new(IngestKafkaClient::from_config(cfg, extended_metrics.clone())?);
        let compression =
            kafka_producer_compression_from_config(&cfg.producer_compression)
                .unwrap_or(KafkaCompression::Snappy);
        let linger = if opts.disable_linger {
            Duration::ZERO
        } else {
            DEFAULT_PRODUCER_LINGER
        };
        let write_timeout =
            Duration::from_nanos(cfg.write_timeout.as_nanos().max(0) as u64);
        let write_timeout_overhead =
            Duration::from_nanos(cfg.write_timeout_overhead.as_nanos().max(0) as u64);
        let default_topic = if opts.disable_default_topic || cfg.topic.is_empty() {
            None
        } else {
            Some(cfg.topic.clone())
        };

        let client = Arc::new(Self {
            inner,
            default_topic,
            compression,
            linger,
            max_inflight: max_inflight_produce_requests.max(1),
            write_timeout: if write_timeout.is_zero() {
                Duration::from_secs(10)
            } else {
                write_timeout
            },
            write_timeout_overhead,
            buffered_bytes: Arc::new(AtomicI64::new(0)),
            buffered_records: Arc::new(AtomicI64::new(0)),
            queue: Arc::new(SyncMutex::new(VecDeque::new())),
            notify: Arc::new(Notify::new()),
            closed: Arc::new(AtomicBool::new(false)),
            extended_metrics,
        });

        let c_bg = Arc::clone(&client);
        tokio::spawn(async move {
            c_bg.dispatch_loop().await;
        });

        Ok(client)
    }

    pub fn request_timeout_overhead(&self) -> Duration {
        self.write_timeout_overhead
    }

    pub fn inner(&self) -> &Arc<IngestKafkaClient> {
        &self.inner
    }

    pub async fn ping(&self) -> Result<(), KafkaClientError> {
        self.inner.ping().await
    }

    pub async fn produce_sync(&self, mut record: WriterRecord) -> Result<i64, ProducerError> {
        if record.topic.is_empty() {
            if let Some(ref t) = self.default_topic {
                record.topic = t.clone();
            }
        }
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.produce(
            record,
            Box::new(move |_rec, res| {
                let _ = tx.send(res);
            }),
        );
        rx.await
            .unwrap_or_else(|_| Err(ProducerError::Other("producer closed".to_string())))
    }

    fn record_buf_bytes(r: &WriterRecord) -> i64 {
        (r.key.len() + r.value.len()) as i64
    }

    async fn dispatch_loop(self: Arc<Self>) {
        let inflight_sem = Arc::new(Semaphore::new(self.max_inflight));
        loop {
            if self.closed.load(Ordering::SeqCst) {
                let drained: Vec<QueuedItem> = self.queue.lock().drain(..).collect();
                for item in drained {
                    let bytes = Self::record_buf_bytes(&item.record);
                    self.buffered_bytes.fetch_sub(bytes, Ordering::SeqCst);
                    self.buffered_records.fetch_sub(1, Ordering::SeqCst);
                    (item.on_done)(
                        item.record,
                        Err(ProducerError::Other("client closed".to_string())),
                    );
                }
                break;
            }

            let has_items = !self.queue.lock().is_empty();
            if !has_items {
                tokio::select! {
                    _ = self.notify.notified() => {},
                    _ = tokio::time::sleep(Duration::from_millis(50)) => {},
                }
                continue;
            }

            let Ok(permit) = Arc::clone(&inflight_sem).acquire_owned().await else {
                break;
            };

            if !self.linger.is_zero() {
                tokio::time::sleep(self.linger).await;
            }

            // Drain all queued items and group by (topic, partition)
            let batch_items: Vec<QueuedItem> = self.queue.lock().drain(..).collect();
            if batch_items.is_empty() {
                drop(permit);
                continue;
            }

            let mut groups: HashMap<(String, i32), Vec<QueuedItem>> = HashMap::new();
            for mut item in batch_items {
                if item.record.topic.is_empty() {
                    if let Some(ref t) = self.default_topic {
                        item.record.topic = t.clone();
                    }
                }
                groups
                    .entry((item.record.topic.clone(), item.record.partition))
                    .or_default()
                    .push(item);
            }

            let mut first = true;
            let mut extra_permits = Vec::new();
            for _ in 1..groups.len() {
                if let Ok(p) = Arc::clone(&inflight_sem).acquire_owned().await {
                    extra_permits.push(p);
                }
            }
            let mut permit_opt = Some(permit);

            for ((topic, partition), items) in groups {
                let p = if first {
                    first = false;
                    permit_opt.take()
                } else {
                    extra_permits.pop()
                };
                let this = Arc::clone(&self);
                tokio::spawn(async move {
                    let _permit = p;
                    this.send_partition_batch(topic, partition, items).await;
                });
            }
        }
    }

    async fn send_partition_batch(
        &self,
        topic: String,
        partition: i32,
        items: Vec<QueuedItem>,
    ) {
        let earliest_enqueue = items
            .iter()
            .map(|i| i.enqueued_at)
            .min()
            .unwrap_or_else(Instant::now);

        let mut valid_items = Vec::with_capacity(items.len());
        for item in items {
            let rec_bytes = item.record.key.len() + item.record.value.len();
            if rec_bytes > PRODUCER_BATCH_MAX_BYTES {
                let bytes = Self::record_buf_bytes(&item.record);
                self.buffered_bytes.fetch_sub(bytes, Ordering::SeqCst);
                self.buffered_records.fetch_sub(1, Ordering::SeqCst);
                self.update_buffered_gauges();
                (item.on_done)(item.record, Err(ProducerError::MessageTooLarge));
            } else {
                valid_items.push(item);
            }
        }
        let mut items = valid_items;
        if items.is_empty() {
            return;
        }

        let mut last_err;
        loop {
            let elapsed = earliest_enqueue.elapsed();
            if elapsed >= self.write_timeout {
                last_err = Some(ProducerError::RecordTimeout);
                break;
            }

            let records: Vec<ProduceRecord> = items
                .iter()
                .map(|i| ProduceRecord {
                    key: Some(i.record.key.clone()),
                    value: Some(i.record.value.clone()),
                    headers: i.record.headers.clone(),
                    timestamp_ms: i.record.timestamp_ms,
                })
                .collect();

            let remaining_wait = (self.write_timeout + self.write_timeout_overhead)
                .saturating_sub(elapsed)
                .max(Duration::from_millis(50));

            let produce_res = tokio::time::timeout(
                remaining_wait,
                self.inner
                    .produce(&topic, partition, self.compression, records),
            )
            .await;

            match produce_res {
                Ok(Ok(base_offset)) => {
                    for (idx, mut item) in items.into_iter().enumerate() {
                        let bytes = Self::record_buf_bytes(&item.record);
                        self.buffered_bytes.fetch_sub(bytes, Ordering::SeqCst);
                        self.buffered_records.fetch_sub(1, Ordering::SeqCst);
                        self.update_buffered_gauges();
                        item.record.offset = base_offset + idx as i64;
                        (item.on_done)(item.record, Ok(base_offset + idx as i64));
                    }
                    return;
                }
                Ok(Err(KafkaClientError::Broker(ResponseError::MessageTooLarge))) => {
                    last_err = Some(ProducerError::MessageTooLarge);
                    break;
                }
                Ok(Err(KafkaClientError::Broker(ResponseError::UnknownTopicOrPartition))) => {
                    // Non-retriable unknown partition if already refreshed once
                    last_err = Some(ProducerError::Other("unknown topic or partition".to_string()));
                    if earliest_enqueue.elapsed() >= Duration::from_millis(200) {
                        break;
                    }
                }
                Ok(Err(e)) => {
                    last_err = Some(ProducerError::Other(e.to_string()));
                }
                Err(_) => {
                    last_err = Some(ProducerError::RecordTimeout);
                    // Fail all currently buffered records in the queue (matching franz-go cascading timeout behavior).
                    let drained: Vec<QueuedItem> = self.queue.lock().drain(..).collect();
                    for item in drained {
                        let bytes = Self::record_buf_bytes(&item.record);
                        self.buffered_bytes.fetch_sub(bytes, Ordering::SeqCst);
                        self.buffered_records.fetch_sub(1, Ordering::SeqCst);
                        self.update_buffered_gauges();
                        (item.on_done)(item.record, Err(ProducerError::RecordTimeout));
                    }
                    break;
                }
            }

            if self.closed.load(Ordering::SeqCst)
                || earliest_enqueue.elapsed() >= self.write_timeout
            {
                if earliest_enqueue.elapsed() >= self.write_timeout {
                    last_err = Some(ProducerError::RecordTimeout);
                }
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        let err = last_err.unwrap_or(ProducerError::RecordTimeout);
        for item in items.drain(..) {
            let bytes = Self::record_buf_bytes(&item.record);
            self.buffered_bytes.fetch_sub(bytes, Ordering::SeqCst);
            self.buffered_records.fetch_sub(1, Ordering::SeqCst);
            self.update_buffered_gauges();
            (item.on_done)(item.record, Err(err.clone()));
        }
    }

    fn update_buffered_gauges(&self) {
        if let Some(ref m) = self.extended_metrics {
            m.buffered_produce_bytes
                .set(self.buffered_bytes.load(Ordering::SeqCst) as f64);
            m.buffered_produce_records_total
                .set(self.buffered_records.load(Ordering::SeqCst) as f64);
        }
    }
}

impl KafkaProducerClient for KafkaWriterClient {
    fn produce(
        &self,
        mut record: WriterRecord,
        on_done: Box<dyn FnOnce(WriterRecord, Result<i64, ProducerError>) + Send>,
    ) {
        if record.topic.is_empty() {
            if let Some(ref t) = self.default_topic {
                record.topic = t.clone();
            }
        }
        let bytes = Self::record_buf_bytes(&record);
        self.buffered_bytes.fetch_add(bytes, Ordering::SeqCst);
        self.buffered_records.fetch_add(1, Ordering::SeqCst);
        self.update_buffered_gauges();
        self.queue.lock().push_back(QueuedItem {
            record,
            enqueued_at: Instant::now(),
            on_done,
        });
        self.notify.notify_one();
    }

    fn buffered_produce_bytes(&self) -> i64 {
        self.buffered_bytes.load(Ordering::SeqCst)
    }

    fn buffered_produce_records(&self) -> i64 {
        self.buffered_records.load(Ordering::SeqCst)
    }

    fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }
}

/// Sliding-window Prometheus Summary collector for `buffered_produce_bytes_distribution`.
#[derive(Clone)]
pub struct SlidingWindowSummary {
    desc: Desc,
    max_age: Duration,
    objectives: Vec<f64>,
    state: Arc<SyncMutex<SummaryState>>,
}

struct SummaryState {
    samples: VecDeque<(Instant, f64)>,
    count: u64,
    sum: f64,
}

impl SlidingWindowSummary {
    pub fn new(name: &str, help: &str, objectives: Vec<f64>, max_age: Duration) -> Self {
        let desc = Desc::new(
            name.to_string(),
            help.to_string(),
            Vec::new(),
            HashMap::new(),
        )
        .expect("valid summary desc");
        Self {
            desc,
            max_age,
            objectives,
            state: Arc::new(SyncMutex::new(SummaryState {
                samples: VecDeque::new(),
                count: 0,
                sum: 0.0,
            })),
        }
    }

    pub fn observe(&self, v: f64) {
        let now = Instant::now();
        let mut guard = self.state.lock();
        guard.samples.push_back((now, v));
        guard.count += 1;
        guard.sum += v;
        while let Some(&(ts, _)) = guard.samples.front() {
            if now.duration_since(ts) > self.max_age {
                guard.samples.pop_front();
            } else {
                break;
            }
        }
    }
}

impl Collector for SlidingWindowSummary {
    fn desc(&self) -> Vec<&Desc> {
        vec![&self.desc]
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let now = Instant::now();
        let mut guard = self.state.lock();
        while let Some(&(ts, _)) = guard.samples.front() {
            if now.duration_since(ts) > self.max_age {
                guard.samples.pop_front();
            } else {
                break;
            }
        }
        let mut vals: Vec<f64> = guard.samples.iter().map(|&(_, v)| v).collect();
        vals.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let mut quantiles = Vec::with_capacity(self.objectives.len());
        for &q in &self.objectives {
            let val = if vals.is_empty() {
                f64::NAN
            } else {
                let idx = ((q * (vals.len() - 1) as f64).round() as usize).min(vals.len() - 1);
                vals[idx]
            };
            let mut pq = Quantile::new();
            pq.set_quantile(q);
            pq.set_value(val);
            quantiles.push(pq);
        }

        let mut summary = ProtoSummary::new();
        summary.set_sample_count(guard.count);
        summary.set_sample_sum(guard.sum);
        summary.set_quantile(quantiles.into());

        let mut metric = ProtoMetric::new();
        metric.set_summary(summary);

        let mut mf = MetricFamily::new();
        mf.set_name(self.desc.fq_name.clone());
        mf.set_help(self.desc.help.clone());
        mf.set_field_type(MetricType::SUMMARY);
        mf.set_metric(vec![metric].into());
        vec![mf]
    }
}

/// High-level KafkaProducer wrapping `KafkaProducerClient` with buffer-limit enforcement,
/// context circuit-breaking, timestamp validation, and metrics.
///
/// Ported from `KafkaProducer` in `pkg/storage/ingest/writer_client.go`.
pub struct KafkaProducer {
    client: Arc<dyn KafkaProducerClient>,
    closed: Arc<AtomicBool>,
    buffered_bytes: Arc<AtomicI64>,
    max_buffered_bytes: i64,

    buffered_produce_bytes: SlidingWindowSummary,
    buffered_produce_bytes_limit: Gauge,
    produce_records_enqueued_total: Counter,
    produce_records_failed_total: CounterVec,
    produce_records_enqueue_duration: Histogram,
    produce_remaining_deadline: Histogram,
}

impl KafkaProducer {
    pub fn new(
        client: Arc<dyn KafkaProducerClient>,
        max_buffered_bytes: i64,
        registry: Option<&Registry>,
    ) -> Arc<Self> {
        Self::new_with_prefix(client, max_buffered_bytes, "", registry)
    }

    pub fn new_with_prefix(
        client: Arc<dyn KafkaProducerClient>,
        max_buffered_bytes: i64,
        prefix: &str,
        registry: Option<&Registry>,
    ) -> Arc<Self> {
        let buffered_produce_bytes = SlidingWindowSummary::new(
            &format!("{prefix}buffered_produce_bytes_distribution"),
            "The buffered produce records in bytes. Quantile buckets keep track of buffered records size over the last 60s.",
            vec![0.5, 0.99, 1.0],
            Duration::from_secs(60),
        );
        let buffered_produce_bytes_limit = Gauge::with_opts(Opts::new(
            format!("{prefix}buffered_produce_bytes_limit"),
            "The bytes limit on buffered produce records. Produce requests fail once this limit is reached.",
        ))
        .expect("valid metric");
        let produce_records_enqueued_total = Counter::with_opts(Opts::new(
            format!("{prefix}produce_records_enqueued_total"),
            "Total number of Kafka records enqueued to be sent to the Kafka backend (includes records that fail to be successfully sent to the Kafka backend).",
        ))
        .expect("valid metric");
        let produce_records_failed_total = CounterVec::new(
            Opts::new(
                format!("{prefix}produce_records_failed_total"),
                "Total number of Kafka records that failed to be sent to the Kafka backend.",
            ),
            &["reason"],
        )
        .expect("valid metric");
        let produce_records_enqueue_duration = Histogram::with_opts(HistogramOpts::new(
            format!("{prefix}produce_records_enqueue_duration_seconds"),
            "How long it takes to enqueue produced Kafka records in the client, appending them to the batches that will sent to the Kafka backend (in seconds).",
        ))
        .expect("valid metric");
        let produce_remaining_deadline = Histogram::with_opts(HistogramOpts::new(
            format!("{prefix}produce_remaining_deadline_seconds"),
            "The remaining deadline (in seconds) when records are requested to be produced.",
        ))
        .expect("valid metric");

        if let Some(reg) = registry {
            let _ = reg.register(Box::new(buffered_produce_bytes.clone()));
            let _ = reg.register(Box::new(buffered_produce_bytes_limit.clone()));
            let _ = reg.register(Box::new(produce_records_enqueued_total.clone()));
            let _ = reg.register(Box::new(produce_records_failed_total.clone()));
            let _ = reg.register(Box::new(produce_records_enqueue_duration.clone()));
            let _ = reg.register(Box::new(produce_remaining_deadline.clone()));
        }

        buffered_produce_bytes_limit.set(max_buffered_bytes as f64);

        let producer = Arc::new(Self {
            client,
            closed: Arc::new(AtomicBool::new(false)),
            buffered_bytes: Arc::new(AtomicI64::new(0)),
            max_buffered_bytes,
            buffered_produce_bytes,
            buffered_produce_bytes_limit,
            produce_records_enqueued_total,
            produce_records_failed_total,
            produce_records_enqueue_duration,
            produce_remaining_deadline,
        });

        let p_bg = Arc::clone(&producer);
        tokio::spawn(async move {
            p_bg.update_metrics_loop().await;
        });

        producer
    }

    pub fn from_config(
        cfg: &KafkaConfig,
        max_inflight: usize,
        registry: Option<&Registry>,
        opts: KafkaWriterClientOptions,
    ) -> Result<Arc<Self>, KafkaClientError> {
        Self::from_config_with_prefix(cfg, max_inflight, "", registry, opts)
    }

    pub fn from_config_with_prefix(
        cfg: &KafkaConfig,
        max_inflight: usize,
        metrics_prefix: &str,
        registry: Option<&Registry>,
        opts: KafkaWriterClientOptions,
    ) -> Result<Arc<Self>, KafkaClientError> {
        let client = KafkaWriterClient::new_with_prefix(
            cfg,
            max_inflight,
            metrics_prefix,
            registry,
            opts,
        )?;
        Ok(Self::new_with_prefix(
            client,
            cfg.producer_max_buffered_bytes,
            metrics_prefix,
            registry,
        ))
    }

    pub fn client(&self) -> &Arc<dyn KafkaProducerClient> {
        &self.client
    }

    pub fn buffered_bytes_counter(&self) -> i64 {
        self.buffered_bytes.load(Ordering::SeqCst)
    }

    pub fn buffered_produce_bytes_limit(&self) -> f64 {
        self.buffered_produce_bytes_limit.get()
    }

    pub fn buffered_produce_records(&self) -> i64 {
        self.client.buffered_produce_records()
    }

    pub fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        self.client.close();
    }

    async fn update_metrics_loop(self: Arc<Self>) {
        let mut ticker = tokio::time::interval(Duration::from_millis(25));
        loop {
            ticker.tick().await;
            if self.closed.load(Ordering::SeqCst) {
                break;
            }
            self.buffered_produce_bytes
                .observe(self.client.buffered_produce_bytes() as f64);
        }
    }

    /// Produces `records` synchronously to Kafka and returns one `ProduceResult` per record.
    pub async fn produce_sync(
        &self,
        ctx: &ProduceContext,
        records: Vec<WriterRecord>,
    ) -> Vec<ProduceResult> {
        if let Some(dl) = ctx.deadline {
            let rem = dl.saturating_duration_since(Instant::now()).as_secs_f64();
            self.produce_remaining_deadline.observe(rem);
        }

        // Circuit-break if context is already done.
        if let Some(cause) = ctx.err() {
            let count = records.len() as f64;
            self.produce_records_enqueued_total.inc_by(count);
            self.produce_records_failed_total
                .with_label_values(&["cancelled-before-producing"])
                .inc_by(count);
            let err = ProducerError::CancelledBeforeProducing(cause);
            return records
                .into_iter()
                .map(|r| ProduceResult {
                    record: r,
                    err: Some(err.clone()),
                })
                .collect();
        }

        // Reject if any record has Timestamp set.
        if records.iter().any(|r| r.timestamp_ms.is_some()) {
            let count = records.len() as f64;
            self.produce_records_enqueued_total.inc_by(count);
            self.produce_records_failed_total
                .with_label_values(&["record-timestamp-set"])
                .inc_by(count);
            return records
                .into_iter()
                .map(|r| ProduceResult {
                    record: r,
                    err: Some(ProducerError::TimestampSet),
                })
                .collect();
        }

        self.produce_records_enqueued_total
            .inc_by(records.len() as f64);

        // All-or-nothing max buffered bytes admission.
        if self.max_buffered_bytes > 0 {
            let batch_bytes: i64 = records.iter().map(|r| r.value.len() as i64).sum();
            let prev = self.buffered_bytes.fetch_add(batch_bytes, Ordering::SeqCst);
            if prev + batch_bytes > self.max_buffered_bytes {
                self.buffered_bytes.fetch_sub(batch_bytes, Ordering::SeqCst);
                self.produce_records_failed_total
                    .with_label_values(&[ProducerError::MaxBuffered.reason()])
                    .inc_by(records.len() as f64);
                return records
                    .into_iter()
                    .map(|r| ProduceResult {
                        record: r,
                        err: Some(ProducerError::MaxBuffered),
                    })
                    .collect();
            }
        }

        if records.is_empty() {
            return Vec::new();
        }

        let partitions: Vec<i32> = records.iter().map(|r| r.partition).collect();
        let remaining = Arc::new(AtomicUsize::new(records.len()));
        let results = Arc::new(Mutex::new(Vec::with_capacity(records.len())));
        let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
        let done_tx = Arc::new(SyncMutex::new(Some(done_tx)));

        let max_buffered = self.max_buffered_bytes;
        let buffered_bytes_ctr = Arc::clone(&self.buffered_bytes);
        let failed_total = self.produce_records_failed_total.clone();

        let enqueue_start = Instant::now();
        for record in records {
            let rem = Arc::clone(&remaining);
            let res_vec = Arc::clone(&results);
            let dtx = Arc::clone(&done_tx);
            let bctr = Arc::clone(&buffered_bytes_ctr);
            let ftotal = failed_total.clone();

            self.client.produce(
                record,
                Box::new(move |rec, res| {
                    if max_buffered > 0 {
                        bctr.fetch_sub(rec.value.len() as i64, Ordering::SeqCst);
                    }
                    let err_opt = match res {
                        Ok(_) => None,
                        Err(e) => {
                            ftotal.with_label_values(&[e.reason()]).inc();
                            Some(e)
                        }
                    };
                    let done_now = rem.fetch_sub(1, Ordering::SeqCst) == 1;
                    if let Ok(mut guard) = res_vec.try_lock() {
                        guard.push(ProduceResult {
                            record: rec,
                            err: err_opt,
                        });
                    } else {
                        let rv = Arc::clone(&res_vec);
                        tokio::spawn(async move {
                            rv.lock().await.push(ProduceResult {
                                record: rec,
                                err: err_opt,
                            });
                            if done_now {
                                if let Some(tx) = dtx.lock().take() {
                                    let _ = tx.send(());
                                }
                            }
                        });
                        return;
                    }
                    if done_now {
                        if let Some(tx) = dtx.lock().take() {
                            let _ = tx.send(());
                        }
                    }
                }),
            );
        }
        self.produce_records_enqueue_duration
            .observe(enqueue_start.elapsed().as_secs_f64());

        tokio::select! {
            _ = done_rx => {
                let mut guard = results.lock().await;
                std::mem::take(&mut *guard)
            }
            cause = ctx.done() => {
                let err = ProducerError::CancelledWhileWaiting(cause);
                partitions
                    .into_iter()
                    .map(|partition| ProduceResult {
                        record: WriterRecord {
                            partition,
                            ..Default::default()
                        },
                        err: Some(err.clone()),
                    })
                    .collect()
            }
        }
    }
}


