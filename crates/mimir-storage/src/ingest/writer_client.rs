//! Kafka producer client, buffer-limit admission, extended transport metrics, and retry/linger batching.
//!
//! Ported from github.com/grafana/mimir/pkg/storage/ingest/writer_client.go

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use bytes::Bytes;
use kafka_protocol::error::ResponseError;
use mimir_config::KafkaConfig;
use parking_lot::Mutex as SyncMutex;
use prometheus::core::{Collector, Desc};
use prometheus::proto::{Metric as ProtoMetric, MetricFamily, MetricType, Quantile, Summary as ProtoSummary};
use mimir_common::native_histogram::{HistogramOpts, SingleHistogram, DEF_BUCKETS};
use prometheus::{Counter, CounterVec, Gauge, Opts, Registry};
use thiserror::Error;
use tokio::sync::{watch, Notify, Semaphore};

use super::compression::KafkaCompression;
use super::kafka_client::{KafkaClientError, ProduceRecord};
use super::util::{IngestKafkaClient, KafkaClientExtendedMetrics, SampledOnlyPropagator};

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
    #[error("message too large (uncompressed_bytes={uncompressed_bytes}{})", compressed_bytes.map(|c| format!(" compressed_bytes={c}")).unwrap_or_default())]
    MessageTooLargeDetail {
        uncompressed_bytes: usize,
        compressed_bytes: Option<usize>,
    },
    #[error("client closed")]
    Closed,
    #[error("kafka error: {0}")]
    Other(String),
}

impl ProducerError {
    pub fn reason(&self) -> &'static str {
        match self {
            Self::RecordTimeout | Self::DeadlineExceeded => "timeout",
            Self::MaxBuffered => "buffer-full",
            Self::MessageTooLarge | Self::MessageTooLargeDetail { .. } => "record-too-large",
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

    pub fn is_message_too_large(&self) -> bool {
        matches!(self, Self::MessageTooLarge | Self::MessageTooLargeDetail { .. })
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

#[derive(Default)]
struct PartitionQueue {
    batches: VecDeque<Vec<QueuedItem>>,
    worker_active: bool,
}

/// Async Kafka writer client with batching, linger, max in-flight requests, delivery timeouts,
/// SASL authentication, and extended Kafka client metrics.
pub struct KafkaWriterClient {
    inner: Arc<IngestKafkaClient>,
    default_topic: Option<String>,
    compression: KafkaCompression,
    linger: Duration,
    write_timeout: Duration,
    batch_max_bytes: AtomicUsize,
    buffered_bytes: Arc<AtomicI64>,
    buffered_records: Arc<AtomicI64>,
    queue: Arc<SyncMutex<VecDeque<QueuedItem>>>,
    partition_queues: Arc<SyncMutex<HashMap<(String, i32), PartitionQueue>>>,
    inflight_sem: Arc<Semaphore>,
    notify: Arc<Notify>,
    closed: Arc<AtomicBool>,
    close_tx: watch::Sender<bool>,
    extended_metrics: Option<KafkaClientExtendedMetrics>,
    #[cfg(test)]
    test_tracing: SyncMutex<Option<mimir_common::tracing::TracingRuntime>>,
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
        let inner = Arc::new(IngestKafkaClient::from_config_for_producer(
            cfg,
            extended_metrics.clone(),
        )?);
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
        let default_topic = if opts.disable_default_topic || cfg.topic.is_empty() {
            None
        } else {
            Some(cfg.topic.clone())
        };

        let max_inflight = max_inflight_produce_requests.max(1);
        let (close_tx, close_rx) = watch::channel(false);
        let client = Arc::new(Self {
            inner,
            default_topic,
            compression,
            linger,
            write_timeout: if write_timeout.is_zero() {
                Duration::from_secs(10)
            } else {
                write_timeout
            },
            batch_max_bytes: AtomicUsize::new(PRODUCER_BATCH_MAX_BYTES),
            buffered_bytes: Arc::new(AtomicI64::new(0)),
            buffered_records: Arc::new(AtomicI64::new(0)),
            queue: Arc::new(SyncMutex::new(VecDeque::new())),
            partition_queues: Arc::new(SyncMutex::new(HashMap::new())),
            inflight_sem: Arc::new(Semaphore::new(max_inflight)),
            notify: Arc::new(Notify::new()),
            closed: Arc::new(AtomicBool::new(false)),
            close_tx,
            extended_metrics,
            #[cfg(test)]
            test_tracing: SyncMutex::new(None),
        });

        let c_bg = Arc::downgrade(&client);
        let inflight_sem = Arc::clone(&client.inflight_sem);
        let notify = Arc::clone(&client.notify);
        let closed = Arc::clone(&client.closed);
        tokio::spawn(async move {
            Self::dispatch_loop(c_bg, inflight_sem, notify, closed, close_rx).await;
        });

        Ok(client)
    }

    pub fn request_timeout_overhead(&self) -> Duration {
        self.inner.request_timeout()
    }

    pub fn inner(&self) -> &Arc<IngestKafkaClient> {
        &self.inner
    }

    pub fn set_batch_max_bytes(&self, bytes: usize) {
        self.batch_max_bytes.store(bytes, Ordering::Relaxed);
    }

    #[cfg(test)]
    fn set_test_tracing(&self, rt: Option<mimir_common::tracing::TracingRuntime>) {
        *self.test_tracing.lock() = rt;
    }

    fn with_active_tracing<R>(
        &self,
        f: impl FnOnce(Option<&mimir_common::tracing::TracingRuntime>) -> R,
    ) -> R {
        #[cfg(test)]
        if let Some(ref rt) = *self.test_tracing.lock() {
            return f(Some(rt));
        }
        f(mimir_common::tracing::global_tracing())
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

    fn fail_items_with_err(&self, items: Vec<QueuedItem>, err: ProducerError) {
        for mut item in items {
            let bytes = Self::record_buf_bytes(&item.record);
            self.buffered_bytes.fetch_sub(bytes, Ordering::SeqCst);
            self.buffered_records.fetch_sub(1, Ordering::SeqCst);
            self.update_buffered_gauges();
            item.record.offset = -1;
            (item.on_done)(item.record, Err(err.clone()));
        }
    }

    fn drain_closed(&self) {
        let mut drained: Vec<QueuedItem> = {
            let mut q = self.queue.lock();
            self.closed.store(true, Ordering::SeqCst);
            q.drain(..).collect()
        };
        {
            let mut pqs = self.partition_queues.lock();
            for pq in pqs.values_mut() {
                for items in pq.batches.drain(..) {
                    drained.extend(items);
                }
            }
        }
        let _ = self.close_tx.send(true);
        self.inflight_sem.close();
        self.notify.notify_waiters();
        self.fail_items_with_err(drained, ProducerError::Closed);
    }

    async fn dispatch_loop(
        weak: Weak<Self>,
        inflight_sem: Arc<Semaphore>,
        notify: Arc<Notify>,
        closed: Arc<AtomicBool>,
        mut close_rx: watch::Receiver<bool>,
    ) {
        loop {
            if closed.load(Ordering::SeqCst) || *close_rx.borrow_and_update() {
                if let Some(this) = weak.upgrade() {
                    this.drain_closed();
                }
                break;
            }

            let (has_items, linger) = {
                let Some(this) = weak.upgrade() else {
                    break;
                };
                let has_items = !this.queue.lock().is_empty();
                (has_items, this.linger)
            };

            if !has_items {
                tokio::select! {
                    _ = notify.notified() => {},
                    _ = close_rx.changed() => {},
                    _ = tokio::time::sleep(Duration::from_millis(250)) => {},
                }
                continue;
            }

            // Acquire at most one permit before lingering/draining so subsequent items stay
            // buffered in self.queue while all max_inflight slots are busy. Additional
            // permits for multi-partition batches are acquired INSIDE each spawned task,
            // never on dispatch_loop's own stack.
            let Ok(first_permit) = Arc::clone(&inflight_sem).acquire_owned().await else {
                if let Some(this) = weak.upgrade() {
                    this.drain_closed();
                }
                break;
            };

            if !linger.is_zero() {
                tokio::select! {
                    _ = tokio::time::sleep(linger) => {},
                    _ = close_rx.changed() => {},
                }
                if closed.load(Ordering::SeqCst) || *close_rx.borrow_and_update() {
                    drop(first_permit);
                    if let Some(this) = weak.upgrade() {
                        this.drain_closed();
                    }
                    break;
                }
            }

            // Drain all queued items and group by (topic, partition)
            let groups = {
                let Some(this) = weak.upgrade() else {
                    drop(first_permit);
                    break;
                };
                if closed.load(Ordering::SeqCst) || *close_rx.borrow_and_update() {
                    drop(first_permit);
                    this.drain_closed();
                    break;
                }
                let batch_items: Vec<QueuedItem> = this.queue.lock().drain(..).collect();
                if batch_items.is_empty() {
                    drop(first_permit);
                    continue;
                }

                let mut groups: HashMap<(String, i32), Vec<QueuedItem>> = HashMap::new();
                for mut item in batch_items {
                    if item.record.topic.is_empty() {
                        if let Some(ref t) = this.default_topic {
                            item.record.topic = t.clone();
                        }
                    }
                    groups
                        .entry((item.record.topic.clone(), item.record.partition))
                        .or_default()
                        .push(item);
                }
                groups
            };

            if closed.load(Ordering::SeqCst) || *close_rx.borrow_and_update() {
                drop(first_permit);
                if let Some(this) = weak.upgrade() {
                    for (_, items) in groups {
                        this.fail_items_with_err(items, ProducerError::Closed);
                    }
                    this.drain_closed();
                } else {
                    for (_, items) in groups {
                        for item in items {
                            (item.on_done)(item.record, Err(ProducerError::Closed));
                        }
                    }
                }
                break;
            }

            let Some(this_strong) = weak.upgrade() else {
                drop(first_permit);
                for (_, items) in groups {
                    for item in items {
                        (item.on_done)(item.record, Err(ProducerError::Closed));
                    }
                }
                break;
            };

            let mut first_permit_opt = Some(first_permit);
            for ((topic, partition), items) in groups {
                let should_spawn = {
                    let mut pqs = this_strong.partition_queues.lock();
                    let pq = pqs.entry((topic.clone(), partition)).or_default();
                    pq.batches.push_back(items);
                    if pq.worker_active {
                        false
                    } else {
                        pq.worker_active = true;
                        true
                    }
                };
                if should_spawn {
                    // Only hand the pre-acquired permit to a newly spawned idle partition worker
                    // (never park permits inside pq.batches behind an active worker!).
                    let initial_permit = first_permit_opt.take();
                    let sem = Arc::clone(&inflight_sem);
                    let weak_this = Arc::downgrade(&this_strong);
                    tokio::spawn(async move {
                        Self::run_partition_worker(weak_this, sem, topic, partition, initial_permit)
                            .await;
                    });
                }
            }
            // If every group already had an active worker, release first_permit immediately.
            drop(first_permit_opt);
        }
    }

    async fn run_partition_worker(
        weak_this: Weak<Self>,
        sem: Arc<Semaphore>,
        topic: String,
        partition: i32,
        mut initial_permit: Option<tokio::sync::OwnedSemaphorePermit>,
    ) {
        loop {
            let Some(this) = weak_this.upgrade() else {
                return;
            };
            let next = {
                let mut pqs = this.partition_queues.lock();
                let key = (topic.clone(), partition);
                match pqs.get_mut(&key) {
                    Some(pq) => match pq.batches.pop_front() {
                        Some(batch) => Some(batch),
                        None => {
                            pq.worker_active = false;
                            pqs.remove(&key);
                            None
                        }
                    },
                    None => None,
                }
            };
            let Some(items) = next else {
                return;
            };
            if this.closed.load(Ordering::SeqCst) {
                drop(initial_permit.take());
                this.fail_items_with_err(items, ProducerError::Closed);
                continue;
            }
            let _permit = match initial_permit.take() {
                Some(p) => p,
                None => {
                    let earliest_enqueue = items
                        .iter()
                        .map(|i| i.enqueued_at)
                        .min()
                        .unwrap_or_else(Instant::now);
                    let deadline = earliest_enqueue + this.write_timeout;
                    tokio::select! {
                        res = sem.clone().acquire_owned() => match res {
                            Ok(p) => p,
                            Err(_) => {
                                this.fail_items_with_err(items, ProducerError::Closed);
                                continue;
                            }
                        },
                        _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                            this.fail_items_with_err(items, ProducerError::RecordTimeout);
                            continue;
                        }
                    }
                }
            };
            if this.closed.load(Ordering::SeqCst) {
                this.fail_items_with_err(items, ProducerError::Closed);
            } else {
                this.send_partition_batch(topic.clone(), partition, items).await;
            }
        }
    }

    const RECORD_BATCH_V2_OVERHEAD: usize = 61;

    fn varint_len(mut v: u64) -> usize {
        let mut len = 1;
        while v >= 0x80 {
            v >>= 7;
            len += 1;
        }
        len
    }

    fn zigzag_varint_len(v: i64) -> usize {
        let z = ((v << 1) ^ (v >> 63)) as u64;
        Self::varint_len(z)
    }

    fn v2_record_wire_bytes(record: &WriterRecord, offset_delta: i32) -> usize {
        let key_len = record.key.len();
        let val_len = record.value.len();
        let mut headers_bytes = Self::zigzag_varint_len(record.headers.len() as i64);
        for (hk, hv) in &record.headers {
            headers_bytes += Self::zigzag_varint_len(hk.len() as i64)
                + hk.len()
                + Self::zigzag_varint_len(hv.len() as i64)
                + hv.len();
        }
        let body_len = 1 // attributes
            + 5 // conservative timestampDelta varlong
            + Self::zigzag_varint_len(offset_delta as i64)
            + Self::zigzag_varint_len(key_len as i64)
            + key_len
            + Self::zigzag_varint_len(val_len as i64)
            + val_len
            + headers_bytes;
        Self::zigzag_varint_len(body_len as i64) + body_len
    }

    async fn send_partition_batch(
        &self,
        topic: String,
        partition: i32,
        items: Vec<QueuedItem>,
    ) {
        let max_batch_bytes = self.batch_max_bytes.load(Ordering::Relaxed);
        let mut valid_items = Vec::with_capacity(items.len());
        for mut item in items {
            let rec_bytes = item.record.key.len() + item.record.value.len();
            if rec_bytes > max_batch_bytes {
                let bytes = Self::record_buf_bytes(&item.record);
                self.buffered_bytes.fetch_sub(bytes, Ordering::SeqCst);
                self.buffered_records.fetch_sub(1, Ordering::SeqCst);
                self.update_buffered_gauges();
                item.record.offset = -1;
                (item.on_done)(
                    item.record,
                    Err(ProducerError::MessageTooLargeDetail {
                        uncompressed_bytes: rec_bytes,
                        compressed_bytes: None,
                    }),
                );
            } else {
                valid_items.push(item);
            }
        }
        if valid_items.is_empty() {
            return;
        }

        // Split into sub-batches that fit within PRODUCER_BATCH_MAX_BYTES (matching kgo ProducerBatchMaxBytes).
        let mut sub_batches: Vec<Vec<QueuedItem>> = Vec::new();
        let mut curr_batch: Vec<QueuedItem> = Vec::new();
        let mut curr_bytes: usize = Self::RECORD_BATCH_V2_OVERHEAD;
        for item in valid_items {
            let rec_wire = Self::v2_record_wire_bytes(&item.record, curr_batch.len() as i32);
            if !curr_batch.is_empty() && curr_bytes + rec_wire > max_batch_bytes {
                sub_batches.push(curr_batch);
                curr_batch = Vec::new();
                curr_bytes = Self::RECORD_BATCH_V2_OVERHEAD
                    + Self::v2_record_wire_bytes(&item.record, 0);
                curr_batch.push(item);
            } else {
                curr_bytes += rec_wire;
                curr_batch.push(item);
            }
        }
        if !curr_batch.is_empty() {
            sub_batches.push(curr_batch);
        }

        for sub_batch in sub_batches {
            self.send_single_partition_batch(&topic, partition, sub_batch)
                .await;
        }
    }

    async fn send_single_partition_batch(
        &self,
        topic: &str,
        partition: i32,
        mut items: Vec<QueuedItem>,
    ) {
        let earliest_enqueue = items
            .iter()
            .map(|i| i.enqueued_at)
            .min()
            .unwrap_or_else(Instant::now);

        let mut last_err;
        let mut retry_backoff = Duration::from_millis(25);
        loop {
            let elapsed = earliest_enqueue.elapsed();
            if elapsed >= self.write_timeout {
                last_err = Some(ProducerError::RecordTimeout);
                self.drain_partition_with_err(topic, partition, ProducerError::RecordTimeout);
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

            let remaining_wait = self.write_timeout.saturating_sub(elapsed);

            let produce_res = tokio::time::timeout(
                remaining_wait,
                self.inner
                    .produce(topic, partition, self.compression, records),
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
                    let uncomp: usize = items.iter().map(|i| i.record.key.len() + i.record.value.len()).sum();
                    last_err = Some(ProducerError::MessageTooLargeDetail {
                        uncompressed_bytes: uncomp,
                        compressed_bytes: Some(uncomp),
                    });
                    break;
                }
                Ok(Err(KafkaClientError::Broker(ResponseError::UnknownTopicOrPartition))) => {
                    // During live Kafka partition expansion, broker metadata propagation can take
                    // longer than 200ms; refresh metadata and retry up to min(write_timeout, 2s).
                    last_err = Some(ProducerError::Other("unknown topic or partition".to_string()));
                    let unknown_limit = self.write_timeout.min(Duration::from_secs(2));
                    if earliest_enqueue.elapsed() >= unknown_limit {
                        break;
                    }
                    let _ = self.inner.refresh_metadata(&[topic]).await;
                    if earliest_enqueue.elapsed() >= unknown_limit {
                        break;
                    }
                    retry_backoff = (retry_backoff * 2).min(Duration::from_millis(250));
                }
                Ok(Err(e)) => {
                    let retriable = match &e {
                        KafkaClientError::Broker(resp_err) => {
                            super::kgo_retry::is_retriable_response_error(*resp_err)
                        }
                        KafkaClientError::Io(_)
                        | KafkaClientError::RequestTimeout(_)
                        | KafkaClientError::ConnectTimeout { .. }
                        | KafkaClientError::ChosenBrokerDead
                        | KafkaClientError::ConnectionClosed
                        | KafkaClientError::UnknownController(_)
                        | KafkaClientError::UnknownBroker
                        | KafkaClientError::UnexpectedResponse(_) => true,
                        KafkaClientError::Dial(d) => matches!(
                            d.kind,
                            super::kgo_retry::DialErrorKind::TimedOut
                                | super::kgo_retry::DialErrorKind::DnsTemporary
                        ),
                        KafkaClientError::Sasl(_)
                        | KafkaClientError::Config(_)
                        | KafkaClientError::FirstReadEof(_)
                        | KafkaClientError::NoSeedBrokers
                        | KafkaClientError::Protocol(_) => false,
                    };
                    last_err = Some(ProducerError::Other(e.to_string()));
                    if !retriable {
                        break;
                    }
                    retry_backoff = Duration::from_millis(25);
                }
                Err(_) => {
                    last_err = Some(ProducerError::RecordTimeout);
                    self.drain_partition_with_err(topic, partition, ProducerError::RecordTimeout);
                    break;
                }
            }

            if self.closed.load(Ordering::SeqCst)
                || earliest_enqueue.elapsed() >= self.write_timeout
            {
                if earliest_enqueue.elapsed() >= self.write_timeout
                    && !matches!(last_err, Some(ProducerError::Other(ref m)) if m == "unknown topic or partition")
                {
                    last_err = Some(ProducerError::RecordTimeout);
                    self.drain_partition_with_err(topic, partition, ProducerError::RecordTimeout);
                }
                break;
            }
            let remaining = self.write_timeout.saturating_sub(earliest_enqueue.elapsed());
            if remaining.is_zero() {
                if !matches!(last_err, Some(ProducerError::Other(ref m)) if m == "unknown topic or partition") {
                    last_err = Some(ProducerError::RecordTimeout);
                    self.drain_partition_with_err(topic, partition, ProducerError::RecordTimeout);
                }
                break;
            }
            tokio::time::sleep(retry_backoff.min(remaining)).await;
        }

        let err = last_err.unwrap_or(ProducerError::RecordTimeout);
        for mut item in items.drain(..) {
            let bytes = Self::record_buf_bytes(&item.record);
            self.buffered_bytes.fetch_sub(bytes, Ordering::SeqCst);
            self.buffered_records.fetch_sub(1, Ordering::SeqCst);
            self.update_buffered_gauges();
            item.record.offset = -1;
            (item.on_done)(item.record, Err(err.clone()));
        }
    }

    /// Drains and fails all queued buffered records belonging to `(topic, partition)`,
    /// matching franz-go per-partition `recBuf` timeout draining.
    fn drain_partition_with_err(&self, topic: &str, partition: i32, err: ProducerError) {
        let mut same_partition_items = Vec::new();
        {
            let mut q = self.queue.lock();
            let mut remaining = VecDeque::with_capacity(q.len());
            for item in q.drain(..) {
                if item.record.topic == topic && item.record.partition == partition {
                    same_partition_items.push(item);
                } else {
                    remaining.push_back(item);
                }
            }
            *q = remaining;
        }
        {
            let mut pqs = self.partition_queues.lock();
            if let Some(pq) = pqs.get_mut(&(topic.to_string(), partition)) {
                for items in pq.batches.drain(..) {
                    same_partition_items.extend(items);
                }
            }
        }
        self.fail_items_with_err(same_partition_items, err);
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
        if record.topic.is_empty() {
            record.offset = -1;
            (on_done)(record, Err(ProducerError::Other("missing topic".to_string())));
            return;
        }
        if record.partition < 0 {
            record.offset = -1;
            (on_done)(
                record,
                Err(ProducerError::Other("invalid record partitioning choice".to_string())),
            );
            return;
        }

        // Go `newSampledOnlyTracer()`: on enqueue, if the active trace context is sampled,
        // propagate W3C `traceparent` / `tracestate` unconditionally in O(1); when global
        // tracing is enabled, mint a child producer span context and export on completion if
        // an exporter is configured (no unconditional SpanRecorder retention).
        let active_tc = mimir_common::tracing::current_trace_context();
        let on_done: Box<dyn FnOnce(WriterRecord, Result<i64, ProducerError>) + Send> =
            if let Some(tc) = active_tc.filter(mimir_common::tracing::TraceContext::is_sampled) {
                record.headers.retain(|(k, _)| {
                    !k.eq_ignore_ascii_case("traceparent") && !k.eq_ignore_ascii_case("tracestate")
                });
                let (prod_ctx, parent_span_id, export_target) = self.with_active_tracing(|rt| {
                    let (prod_ctx, parent_span_id, tracing_enabled) =
                        mimir_common::tracing::start_kafka_sampled_span_context_with_runtime(
                            rt, &tc,
                        );
                    let export_target = if tracing_enabled {
                        rt.filter(|r| r.exporter.is_some()).cloned()
                    } else {
                        None
                    };
                    (prod_ctx, parent_span_id, export_target)
                });
                SampledOnlyPropagator.inject_trace_context(Some(&prod_ctx), &mut record.headers);
                if let Some(rt) = export_target {
                    let start_ns = mimir_common::tracing::now_unix_nanos();
                    Box::new(move |rec, res| {
                        mimir_common::tracing::export_span_with_runtime(
                            &rt,
                            mimir_common::tracing::ExportedSpan {
                                service_name: rt.service_name.clone(),
                                trace_id: prod_ctx.trace_id,
                                span_id: prod_ctx.span_id,
                                parent_span_id,
                                name: format!("{} publish", rec.topic),
                                kind: 4, // OTLP SPAN_KIND_PRODUCER
                                start_time_unix_nano: start_ns,
                                end_time_unix_nano: mimir_common::tracing::now_unix_nanos(),
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
                                            rec.topic.clone(),
                                        ),
                                    },
                                    mimir_common::tracing::SpanAttribute {
                                        key: "messaging.kafka.destination.partition".to_string(),
                                        value: mimir_common::tracing::SpanAttrValue::Int(
                                            rec.partition as i64,
                                        ),
                                    },
                                ],
                                status_error: res.is_err(),
                            },
                        );
                        on_done(rec, res);
                    })
                } else {
                    on_done
                }
            } else {
                on_done
            };

        let mut q = self.queue.lock();
        if self.closed.load(Ordering::SeqCst) {
            drop(q);
            (on_done)(record, Err(ProducerError::Closed));
            return;
        }
        let bytes = Self::record_buf_bytes(&record);
        self.buffered_bytes.fetch_add(bytes, Ordering::SeqCst);
        self.buffered_records.fetch_add(1, Ordering::SeqCst);
        self.update_buffered_gauges();
        q.push_back(QueuedItem {
            record,
            enqueued_at: Instant::now(),
            on_done,
        });
        drop(q);
        self.notify.notify_one();
    }

    fn buffered_produce_bytes(&self) -> i64 {
        self.buffered_bytes.load(Ordering::SeqCst)
    }

    fn buffered_produce_records(&self) -> i64 {
        self.buffered_records.load(Ordering::SeqCst)
    }

    fn close(&self) {
        self.drain_closed();
    }
}

impl Drop for KafkaWriterClient {
    fn drop(&mut self) {
        self.drain_closed();
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
    stop_notify: Arc<Notify>,
    buffered_bytes: Arc<AtomicI64>,
    max_buffered_bytes: i64,

    buffered_produce_bytes: SlidingWindowSummary,
    buffered_produce_bytes_limit: Gauge,
    produce_records_enqueued_total: Counter,
    produce_records_failed_total: CounterVec,
    produce_records_enqueue_duration: SingleHistogram,
    produce_remaining_deadline: SingleHistogram,
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
        let produce_records_enqueue_duration = SingleHistogram::new(
            HistogramOpts::new(
                format!("{prefix}produce_records_enqueue_duration_seconds"),
                "How long it takes to enqueue produced Kafka records in the client, appending them to the batches that will sent to the Kafka backend (in seconds).",
            )
            .buckets(DEF_BUCKETS.to_vec())
            .native(1.1, 100, Duration::from_secs(3600)),
        )
        .expect("valid metric");
        let produce_remaining_deadline = SingleHistogram::new(
            HistogramOpts::new(
                format!("{prefix}produce_remaining_deadline_seconds"),
                "The remaining deadline (in seconds) when records are requested to be produced.",
            )
            .buckets(DEF_BUCKETS.to_vec())
            .native(1.1, 100, Duration::from_secs(3600)),
        )
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
            stop_notify: Arc::new(Notify::new()),
            buffered_bytes: Arc::new(AtomicI64::new(0)),
            max_buffered_bytes,
            buffered_produce_bytes,
            buffered_produce_bytes_limit,
            produce_records_enqueued_total,
            produce_records_failed_total,
            produce_records_enqueue_duration,
            produce_remaining_deadline,
        });

        let p_bg = Arc::downgrade(&producer);
        let closed = Arc::clone(&producer.closed);
        let stop_notify = Arc::clone(&producer.stop_notify);
        tokio::spawn(async move {
            Self::update_metrics_loop(p_bg, closed, stop_notify).await;
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
        if cfg.backend.eq_ignore_ascii_case("warpstream") {
            return Err(KafkaClientError::Config(
                "kafka backend \"warpstream\" is not supported by mimir-rs".to_string(),
            ));
        }
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
        self.stop_notify.notify_waiters();
        self.client.close();
    }

    async fn update_metrics_loop(
        weak: Weak<Self>,
        closed: Arc<AtomicBool>,
        stop_notify: Arc<Notify>,
    ) {
        let mut ticker = tokio::time::interval(Duration::from_millis(250));
        loop {
            tokio::select! {
                _ = stop_notify.notified() => {
                    if closed.load(Ordering::SeqCst) || weak.strong_count() == 0 {
                        break;
                    }
                }
                _ = ticker.tick() => {
                    if closed.load(Ordering::SeqCst) {
                        break;
                    }
                    let Some(this) = weak.upgrade() else {
                        break;
                    };
                    this.buffered_produce_bytes
                        .observe(this.client.buffered_produce_bytes() as f64);
                }
            }
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
        let results = Arc::new(SyncMutex::new(Vec::with_capacity(records.len())));
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
                    {
                        let mut guard = res_vec.lock();
                        guard.push(ProduceResult {
                            record: rec,
                            err: err_opt,
                        });
                    }
                    if rem.fetch_sub(1, Ordering::AcqRel) == 1 {
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
                let mut guard = results.lock();
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

impl Drop for KafkaProducer {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::SeqCst);
        self.stop_notify.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, BytesMut};
    use kafka_protocol::messages::api_versions_response::{ApiVersion, ApiVersionsResponse};
    use kafka_protocol::messages::metadata_response::{
        MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
    };
    use kafka_protocol::messages::produce_response::{
        PartitionProduceResponse, TopicProduceResponse,
    };
    use kafka_protocol::messages::{
        ApiKey, ApiVersionsRequest, BrokerId, MetadataRequest, MetadataResponse, ProduceRequest,
        ProduceResponse, RequestHeader, ResponseHeader, TopicName,
    };
    use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};
    use mimir_config::types::StringSliceCSV;
    use mimir_config::GoDuration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn writer_client_retries_unknown_topic_or_partition_past_200ms_during_expansion() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let expanded = Arc::new(AtomicBool::new(false));
        let exp_c = Arc::clone(&expanded);
        let produce_attempts = Arc::new(AtomicUsize::new(0));
        let pa_c = Arc::clone(&produce_attempts);

        let srv = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let exp = Arc::clone(&exp_c);
                let pa = Arc::clone(&pa_c);
                let port = addr.port() as i32;
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
                            ApiKey::Produce => ProduceRequest::header_version(ver),
                            _ => break,
                        };
                        let Ok(req_hdr) = RequestHeader::decode(&mut buf, hdr_ver) else {
                            break;
                        };
                        let mut out = BytesMut::new();
                        out.put_u32(0);
                        let mut resp_hdr = ResponseHeader::default();
                        resp_hdr.correlation_id = req_hdr.correlation_id;
                        resp_hdr
                            .encode(&mut out, api_key.response_header_version(ver))
                            .unwrap();

                        match api_key {
                            ApiKey::ApiVersions => {
                                let _ = ApiVersionsRequest::decode(&mut buf, ver);
                                let mut resp = ApiVersionsResponse::default();
                                for (k, mn, mx) in [
                                    (ApiKey::ApiVersions as i16, 0, 3),
                                    (ApiKey::Metadata as i16, 1, 12),
                                    (ApiKey::Produce as i16, 3, 9),
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
                                b.port = port;
                                resp.brokers.push(b);
                                resp.controller_id = BrokerId(0);

                                let mut mt = MetadataResponseTopic::default();
                                mt.name = Some(TopicName(StrBytes::from_static_str("ingest")));
                                let count = if exp.load(Ordering::SeqCst) { 2 } else { 1 };
                                for pid in 0..count {
                                    let mut mp = MetadataResponsePartition::default();
                                    mp.partition_index = pid;
                                    mp.leader_id = BrokerId(0);
                                    mt.partitions.push(mp);
                                }
                                resp.topics.push(mt);
                                resp.encode(&mut out, ver).unwrap();
                            }
                            ApiKey::Produce => {
                                let req = ProduceRequest::decode(&mut buf, ver).unwrap();
                                let mut resp = ProduceResponse::default();
                                for rt in &req.topic_data {
                                    let mut tr = TopicProduceResponse::default();
                                    tr.name = rt.name.clone();
                                    for rp in &rt.partition_data {
                                        let mut pr = PartitionProduceResponse::default();
                                        pr.index = rp.index;
                                        if rp.index == 1 {
                                            pa.fetch_add(1, Ordering::SeqCst);
                                            if exp.load(Ordering::SeqCst) {
                                                pr.error_code = 0;
                                                pr.base_offset = 77;
                                            } else {
                                                pr.error_code =
                                                    ResponseError::UnknownTopicOrPartition.code();
                                            }
                                        }
                                        tr.partition_responses.push(pr);
                                    }
                                    resp.responses.push(tr);
                                }
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
            topic: "ingest".to_string(),
            write_timeout: GoDuration::from_secs(3),
            ..Default::default()
        };
        cfg.dial_timeout = GoDuration::from_secs(2);

        let client = KafkaWriterClient::new(
            &cfg,
            1,
            None,
            KafkaWriterClientOptions {
                disable_default_topic: false,
                disable_linger: true,
            },
        )
        .unwrap();

        // Simulate partition 1 propagating after 350ms (past the old 200ms cutoff).
        let exp_flip = Arc::clone(&expanded);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(350)).await;
            exp_flip.store(true, Ordering::SeqCst);
        });

        let res = client
            .produce_sync(WriterRecord {
                topic: "ingest".to_string(),
                partition: 1,
                key: Bytes::from_static(b"k"),
                value: Bytes::from_static(b"v"),
                ..Default::default()
            })
            .await
            .expect("produce to newly expanded partition must succeed past 200ms");

        assert_eq!(res, 77);
        assert!(produce_attempts.load(Ordering::SeqCst) >= 2);

        client.close();
        srv.abort();
    }

    async fn spawn_multi_partition_mock_kafka(
        num_partitions: i32,
        stall_partition_0_forever: Arc<AtomicBool>,
        produced_partitions: Arc<SyncMutex<Vec<i32>>>,
    ) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let srv = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let stall_p0 = Arc::clone(&stall_partition_0_forever);
                let produced = Arc::clone(&produced_partitions);
                let port = addr.port() as i32;
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
                            ApiKey::Produce => ProduceRequest::header_version(ver),
                            _ => break,
                        };
                        let Ok(req_hdr) = RequestHeader::decode(&mut buf, hdr_ver) else {
                            break;
                        };
                        let mut out = BytesMut::new();
                        out.put_u32(0);
                        let mut resp_hdr = ResponseHeader::default();
                        resp_hdr.correlation_id = req_hdr.correlation_id;
                        resp_hdr
                            .encode(&mut out, api_key.response_header_version(ver))
                            .unwrap();

                        match api_key {
                            ApiKey::ApiVersions => {
                                let _ = ApiVersionsRequest::decode(&mut buf, ver);
                                let mut resp = ApiVersionsResponse::default();
                                for (k, mn, mx) in [
                                    (ApiKey::ApiVersions as i16, 0, 3),
                                    (ApiKey::Metadata as i16, 1, 12),
                                    (ApiKey::Produce as i16, 3, 9),
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
                                b.port = port;
                                resp.brokers.push(b);
                                resp.controller_id = BrokerId(0);

                                let mut mt = MetadataResponseTopic::default();
                                mt.name = Some(TopicName(StrBytes::from_static_str("ingest")));
                                for pid in 0..num_partitions {
                                    let mut mp = MetadataResponsePartition::default();
                                    mp.partition_index = pid;
                                    mp.leader_id = BrokerId(0);
                                    mt.partitions.push(mp);
                                }
                                resp.topics.push(mt);
                                resp.encode(&mut out, ver).unwrap();
                            }
                            ApiKey::Produce => {
                                let req = ProduceRequest::decode(&mut buf, ver).unwrap();
                                let mut should_stall = false;
                                let mut resp = ProduceResponse::default();
                                for rt in &req.topic_data {
                                    let mut tr = TopicProduceResponse::default();
                                    tr.name = rt.name.clone();
                                    for rp in &rt.partition_data {
                                        if rp.index == 0 && stall_p0.load(Ordering::SeqCst) {
                                            should_stall = true;
                                        }
                                        produced.lock().push(rp.index);
                                        let mut pr = PartitionProduceResponse::default();
                                        pr.index = rp.index;
                                        pr.error_code = 0;
                                        pr.base_offset = 100 + rp.index as i64;
                                        tr.partition_responses.push(pr);
                                    }
                                    resp.responses.push(tr);
                                }
                                if should_stall {
                                    // Stall only this partition's Produce response without blocking
                                    // subsequent requests for healthy partitions on the same socket.
                                    continue;
                                }
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
        (addr, srv)
    }

    #[tokio::test]
    async fn w23_csc_01_dispatch_loop_does_not_deadlock_when_batch_spans_more_partitions_than_max_inflight() {
        let stall = Arc::new(AtomicBool::new(false));
        let produced = Arc::new(SyncMutex::new(Vec::new()));
        let (addr, srv) = spawn_multi_partition_mock_kafka(8, stall, Arc::clone(&produced)).await;

        let mut cfg = KafkaConfig {
            address: StringSliceCSV(vec![addr.to_string()]),
            topic: "ingest".to_string(),
            write_timeout: GoDuration::from_secs(3),
            ..Default::default()
        };
        cfg.dial_timeout = GoDuration::from_secs(2);

        // max_inflight = 2 while a single 50ms linger batch spans 8 partitions.
        let client = KafkaWriterClient::new(
            &cfg,
            2,
            None,
            KafkaWriterClientOptions {
                disable_default_topic: false,
                disable_linger: false,
            },
        )
        .unwrap();

        let mut futs = Vec::new();
        for p in 0..8 {
            let c = Arc::clone(&client);
            futs.push(tokio::spawn(async move {
                c.produce_sync(WriterRecord {
                    topic: "ingest".to_string(),
                    partition: p,
                    key: Bytes::from(format!("k-{p}")),
                    value: Bytes::from(format!("v-{p}")),
                    ..Default::default()
                })
                .await
                .map(|off| (p, off))
            }));
        }

        let results = tokio::time::timeout(Duration::from_secs(3), futures::future::join_all(futs))
            .await
            .expect("8-partition linger batch with max_inflight=2 must not deadlock");
        for r in results {
            let (p, off) = r.unwrap().expect("produce must succeed");
            assert_eq!(off, 100 + p as i64);
        }
        assert_eq!(client.buffered_produce_bytes(), 0);
        assert_eq!(client.buffered_produce_records(), 0);

        client.close();
        srv.abort();
    }

    #[tokio::test]
    async fn w23_csc_01_single_partition_timeout_only_drains_timed_out_partition() {
        let stall_p0 = Arc::new(AtomicBool::new(true));
        let produced = Arc::new(SyncMutex::new(Vec::new()));
        let (addr, srv) =
            spawn_multi_partition_mock_kafka(2, Arc::clone(&stall_p0), Arc::clone(&produced)).await;

        let mut cfg = KafkaConfig {
            address: StringSliceCSV(vec![addr.to_string()]),
            topic: "ingest".to_string(),
            write_timeout: GoDuration::from_millis(200),
            write_timeout_overhead: GoDuration::from_millis(40),
            ..Default::default()
        };
        cfg.dial_timeout = GoDuration::from_secs(2);

        let client = KafkaWriterClient::new(
            &cfg,
            2,
            None,
            KafkaWriterClientOptions {
                disable_default_topic: false,
                disable_linger: false,
            },
        )
        .unwrap();

        // Batch 1: partition 0 stalls and will hit tokio::time::timeout at ~200ms after linger (50ms).
        let (tx0_a, rx0_a) = tokio::sync::oneshot::channel();
        client.produce(
            WriterRecord {
                topic: "ingest".to_string(),
                partition: 0,
                key: Bytes::from_static(b"k0a"),
                value: Bytes::from_static(b"v0a"),
                ..Default::default()
            },
            Box::new(move |_rec, res| {
                let _ = tx0_a.send(res);
            }),
        );

        // Wait until batch 1 is in-flight on partition 0 (past the 50ms linger, before its 200ms timeout).
        while !produced.lock().contains(&0) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        // Sleep until just before the first batch times out (~155ms elapsed vs ~200ms timeout),
        // then enqueue a second p0 record AND a healthy p1 record so both sit in self.queue
        // during the 50ms linger while p0 times out.
        tokio::time::sleep(Duration::from_millis(105)).await;

        let (tx0_b, rx0_b) = tokio::sync::oneshot::channel();
        client.produce(
            WriterRecord {
                topic: "ingest".to_string(),
                partition: 0,
                key: Bytes::from_static(b"k0b"),
                value: Bytes::from_static(b"v0b"),
                ..Default::default()
            },
            Box::new(move |_rec, res| {
                let _ = tx0_b.send(res);
            }),
        );

        let (tx1, rx1) = tokio::sync::oneshot::channel();
        client.produce(
            WriterRecord {
                topic: "ingest".to_string(),
                partition: 1,
                key: Bytes::from_static(b"k1"),
                value: Bytes::from_static(b"v1"),
                ..Default::default()
            },
            Box::new(move |_rec, res| {
                let _ = tx1.send(res);
            }),
        );

        assert_eq!(rx0_a.await.unwrap(), Err(ProducerError::RecordTimeout));
        assert_eq!(rx0_b.await.unwrap(), Err(ProducerError::RecordTimeout));
        assert_eq!(rx1.await.unwrap(), Ok(101));
        assert_eq!(client.buffered_produce_bytes(), 0);

        client.close();
        srv.abort();
    }

    #[tokio::test]
    async fn w23_csc_01_drop_and_close_release_queued_items_and_buffered_bytes() {
        let stall = Arc::new(AtomicBool::new(true));
        let produced = Arc::new(SyncMutex::new(Vec::new()));
        let (addr, srv) = spawn_multi_partition_mock_kafka(1, stall, produced).await;

        let mut cfg = KafkaConfig {
            address: StringSliceCSV(vec![addr.to_string()]),
            topic: "ingest".to_string(),
            write_timeout: GoDuration::from_secs(5),
            ..Default::default()
        };
        cfg.dial_timeout = GoDuration::from_secs(2);

        // Case A: close() while items are queued in linger releases KafkaProducer.buffered_bytes.
        let producer = KafkaProducer::from_config_with_prefix(
            &cfg,
            2,
            "",
            None,
            KafkaWriterClientOptions {
                disable_default_topic: false,
                disable_linger: false,
            },
        )
        .unwrap();

        let p_c = Arc::clone(&producer);
        let h = tokio::spawn(async move {
            p_c.produce_sync(
                &ProduceContext::background(),
                vec![WriterRecord {
                    topic: "ingest".to_string(),
                    partition: 0,
                    key: Bytes::from_static(b"k"),
                    value: Bytes::from_static(b"payload-1234"),
                    ..Default::default()
                }],
            )
            .await
        });

        while producer.buffered_bytes_counter() == 0 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        producer.close();

        let res = tokio::time::timeout(Duration::from_secs(1), h)
            .await
            .expect("produce_sync must complete promptly on close")
            .unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].err, Some(ProducerError::Closed));
        assert_eq!(producer.buffered_bytes_counter(), 0);

        // Case B: dropping KafkaWriterClient while queued items exist invokes callbacks and clears bytes.
        let client = KafkaWriterClient::new(
            &cfg,
            2,
            None,
            KafkaWriterClientOptions {
                disable_default_topic: false,
                disable_linger: false,
            },
        )
        .unwrap();
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let sim_buffered = Arc::new(AtomicI64::new(12));
        let sim_ctr = Arc::clone(&sim_buffered);
        client.produce(
            WriterRecord {
                topic: "ingest".to_string(),
                partition: 0,
                key: Bytes::from_static(b"k2"),
                value: Bytes::from_static(b"payload-5678"),
                ..Default::default()
            },
            Box::new(move |rec, res| {
                sim_ctr.fetch_sub(rec.value.len() as i64, Ordering::SeqCst);
                let _ = done_tx.send(res);
            }),
        );
        assert_eq!(client.buffered_produce_records(), 1);
        drop(client);
        let res2 = tokio::time::timeout(Duration::from_secs(1), done_rx)
            .await
            .expect("drop must resolve queued callbacks without hanging")
            .unwrap();
        assert_eq!(res2, Err(ProducerError::Closed));
        assert_eq!(sim_buffered.load(Ordering::SeqCst), 0);

        srv.abort();
    }

    #[test]
    fn w23_csc_12a_rejects_warpstream_backend() {
        let cfg = KafkaConfig {
            backend: "warpstream".to_string(),
            address: StringSliceCSV(vec!["127.0.0.1:9092".to_string()]),
            topic: "ingest".to_string(),
            ..Default::default()
        };
        let err = match KafkaProducer::from_config_with_prefix(
            &cfg,
            1,
            "",
            None,
            KafkaWriterClientOptions::default(),
        ) {
            Ok(_) => panic!("expected warpstream backend to be rejected"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("warpstream"), "unexpected error: {err}");
    }

    #[tokio::test]
    async fn d1_4b_retry_loop_timeout_bounds_by_write_timeout_and_drains_same_partition() {
        // Server returns retriable RequestTimedOut on Produce for partition 0 and succeeds on p1.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let port = addr.port() as i32;
        let p0_attempts = Arc::new(AtomicUsize::new(0));
        let p0_attempts_srv = Arc::clone(&p0_attempts);

        let srv = tokio::spawn(async move {
            loop {
                let Ok((sock, _)) = listener.accept().await else {
                    break;
                };
                let p0_c = Arc::clone(&p0_attempts_srv);
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
                        let mut buf = Bytes::from(payload);
                        let key_i16 = i16::from_be_bytes([buf[0], buf[1]]);
                        let ver = i16::from_be_bytes([buf[2], buf[3]]);
                        let api_key = ApiKey::try_from(key_i16).unwrap();
                        let req_hdr = RequestHeader::decode(&mut buf, api_key.request_header_version(ver)).unwrap();
                        let mut out = BytesMut::new();
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
                                let mut r = ApiVersionsResponse::default();
                                for (k, mn, mx) in [
                                    (ApiKey::ApiVersions as i16, 0, 3),
                                    (ApiKey::Metadata as i16, 0, 12),
                                    (ApiKey::Produce as i16, 3, 9),
                                ] {
                                    let mut av = ApiVersion::default();
                                    av.api_key = k;
                                    av.min_version = mn;
                                    av.max_version = mx;
                                    r.api_keys.push(av);
                                }
                                r.encode(&mut out, ver).unwrap();
                            }
                            ApiKey::Metadata => {
                                let mut b = MetadataResponseBroker::default();
                                b.node_id = BrokerId(0);
                                b.host = StrBytes::from_static_str("127.0.0.1");
                                b.port = port;
                                let mut t = MetadataResponseTopic::default();
                                t.name = Some(TopicName(StrBytes::from_static_str("ingest")));
                                for pid in 0..2 {
                                    let mut p = MetadataResponsePartition::default();
                                    p.partition_index = pid;
                                    p.leader_id = BrokerId(0);
                                    t.partitions.push(p);
                                }
                                let mut r = MetadataResponse::default();
                                r.brokers = vec![b];
                                r.topics = vec![t];
                                r.encode(&mut out, ver).unwrap();
                            }
                            ApiKey::Produce => {
                                let req = ProduceRequest::decode(&mut buf, ver).unwrap();
                                let mut resp = ProduceResponse::default();
                                for rt in &req.topic_data {
                                    let mut tr = TopicProduceResponse::default();
                                    tr.name = rt.name.clone();
                                    for rp in &rt.partition_data {
                                        let mut pr = PartitionProduceResponse::default();
                                        pr.index = rp.index;
                                        if rp.index == 0 {
                                            p0_c.fetch_add(1, Ordering::SeqCst);
                                            pr.error_code = ResponseError::RequestTimedOut.code();
                                        } else {
                                            pr.error_code = 0;
                                            pr.base_offset = 100;
                                        }
                                        tr.partition_responses.push(pr);
                                    }
                                    resp.responses.push(tr);
                                }
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

        // Large write_timeout_overhead (5s) must NOT extend RecordDeliveryTimeout past write_timeout (150ms)!
        let mut cfg = KafkaConfig {
            address: StringSliceCSV(vec![addr.to_string()]),
            topic: "ingest".to_string(),
            write_timeout: GoDuration::from_millis(150),
            write_timeout_overhead: GoDuration::from_secs(5),
            ..Default::default()
        };
        cfg.dial_timeout = GoDuration::from_secs(2);

        let client = KafkaWriterClient::new(
            &cfg,
            2,
            None,
            KafkaWriterClientOptions {
                disable_default_topic: false,
                disable_linger: false,
            },
        )
        .unwrap();

        let started = Instant::now();
        let (tx0_a, rx0_a) = tokio::sync::oneshot::channel();
        client.produce(
            WriterRecord {
                topic: "ingest".to_string(),
                partition: 0,
                key: Bytes::from_static(b"k0a"),
                value: Bytes::from_static(b"v0a"),
                ..Default::default()
            },
            Box::new(move |_r, res| {
                let _ = tx0_a.send(res);
            }),
        );

        // Wait for at least one retriable Produce attempt on p0 (after 50ms linger), then wait
        // until ~110ms total so p0_b remains in self.queue (linger 50ms -> 160ms) when p0_a hits
        // write_timeout at 150ms.
        while p0_attempts.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        tokio::time::sleep(Duration::from_millis(60)).await;
        // Enqueue a second p0 record and a healthy p1 record while p0 is retrying.
        let (tx0_b, rx0_b) = tokio::sync::oneshot::channel();
        let p0_b_enqueued = Instant::now();
        client.produce(
            WriterRecord {
                topic: "ingest".to_string(),
                partition: 0,
                key: Bytes::from_static(b"k0b"),
                value: Bytes::from_static(b"v0b"),
                ..Default::default()
            },
            Box::new(move |_r, res| {
                let _ = tx0_b.send((res, p0_b_enqueued.elapsed()));
            }),
        );
        let (tx1, rx1) = tokio::sync::oneshot::channel();
        client.produce(
            WriterRecord {
                topic: "ingest".to_string(),
                partition: 1,
                key: Bytes::from_static(b"k1"),
                value: Bytes::from_static(b"v1"),
                ..Default::default()
            },
            Box::new(move |_r, res| {
                let _ = tx1.send(res);
            }),
        );

        assert_eq!(rx0_a.await.unwrap(), Err(ProducerError::RecordTimeout));
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_secs(1),
            "RecordDeliveryTimeout must NOT include 5s write_timeout_overhead, took {elapsed:?}"
        );
        let (res0_b, p0_b_elapsed) = rx0_b.await.unwrap();
        assert_eq!(res0_b, Err(ProducerError::RecordTimeout));
        assert!(
            p0_b_elapsed < Duration::from_millis(140),
            "queued p0_b must be drained immediately on retry-loop timeout, took {p0_b_elapsed:?}"
        );
        assert_eq!(rx1.await.unwrap(), Ok(100));
        assert_eq!(client.buffered_produce_bytes(), 0);
        assert_eq!(client.buffered_produce_records(), 0);

        client.close();
        srv.abort();
    }

    #[tokio::test]
    async fn writer_client_injects_traceparent_only_when_sampled() {
        use kafka_protocol::records::RecordBatchDecoder;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        type CapturedHeaders = Arc<SyncMutex<Vec<Vec<(String, Vec<u8>)>>>>;
        let captured_headers: CapturedHeaders = Arc::new(SyncMutex::new(Vec::new()));
        let ch_srv = Arc::clone(&captured_headers);

        let srv = tokio::spawn(async move {
            loop {
                let Ok((sock, _)) = listener.accept().await else {
                    break;
                };
                let ch = Arc::clone(&ch_srv);
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
                        let mut buf = Bytes::from(payload);
                        let key_i16 = i16::from_be_bytes([buf[0], buf[1]]);
                        let ver = i16::from_be_bytes([buf[2], buf[3]]);
                        let api_key = ApiKey::try_from(key_i16).unwrap();
                        let req_hdr =
                            RequestHeader::decode(&mut buf, api_key.request_header_version(ver))
                                .unwrap();
                        let mut out = BytesMut::new();
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
                                let mut r = ApiVersionsResponse::default();
                                for (k, mn, mx) in [
                                    (ApiKey::ApiVersions as i16, 0, 3),
                                    (ApiKey::Metadata as i16, 0, 12),
                                    (ApiKey::Produce as i16, 3, 9),
                                ] {
                                    let mut av = ApiVersion::default();
                                    av.api_key = k;
                                    av.min_version = mn;
                                    av.max_version = mx;
                                    r.api_keys.push(av);
                                }
                                r.encode(&mut out, ver).unwrap();
                            }
                            ApiKey::Metadata => {
                                let mut b = MetadataResponseBroker::default();
                                b.node_id = BrokerId(0);
                                b.host = StrBytes::from_static_str("127.0.0.1");
                                b.port = addr.port() as i32;
                                let mut t = MetadataResponseTopic::default();
                                t.name = Some(TopicName(StrBytes::from_static_str("ingest-tr")));
                                let mut p = MetadataResponsePartition::default();
                                p.partition_index = 0;
                                p.leader_id = BrokerId(0);
                                t.partitions.push(p);
                                let mut r = MetadataResponse::default();
                                r.brokers = vec![b];
                                r.topics = vec![t];
                                r.encode(&mut out, ver).unwrap();
                            }
                            ApiKey::Produce => {
                                let req = ProduceRequest::decode(&mut buf, ver).unwrap();
                                let mut resp = ProduceResponse::default();
                                for rt in &req.topic_data {
                                    let mut tr = TopicProduceResponse::default();
                                    tr.name = rt.name.clone();
                                    for rp in &rt.partition_data {
                                        if let Some(ref recs_bytes) = rp.records {
                                            let mut raw = recs_bytes.clone();
                                            let decoded = RecordBatchDecoder::decode_with_custom_compression::<
                                                _,
                                                fn(&mut Bytes, kafka_protocol::records::Compression) -> anyhow::Result<Bytes>,
                                            >(&mut raw, None)
                                            .unwrap();
                                            for rec in decoded {
                                                let hdrs: Vec<(String, Vec<u8>)> = rec
                                                    .headers
                                                    .into_iter()
                                                    .map(|(k, v)| {
                                                        (
                                                            k.to_string(),
                                                            v.map(|b| b.to_vec()).unwrap_or_default(),
                                                        )
                                                    })
                                                    .collect();
                                                ch.lock().push(hdrs);
                                            }
                                        }
                                        let mut pr = PartitionProduceResponse::default();
                                        pr.index = rp.index;
                                        pr.error_code = 0;
                                        pr.base_offset = 1;
                                        tr.partition_responses.push(pr);
                                    }
                                    resp.responses.push(tr);
                                }
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
            topic: "ingest-tr".to_string(),
            producer_compression: "none".to_string(),
            write_timeout: GoDuration::from_secs(2),
            ..Default::default()
        };
        cfg.dial_timeout = GoDuration::from_secs(2);
        let client = KafkaWriterClient::new(
            &cfg,
            4,
            None,
            KafkaWriterClientOptions {
                disable_default_topic: false,
                disable_linger: true,
            },
        )
        .unwrap();

        let sampled_tc = mimir_common::tracing::TraceContext {
            trace_id: [0x11; 16],
            span_id: [0x22; 8],
            sampled: true,
            trace_flags: 0x01,
            remote: false,
            jaeger_debug_id: None,
            tracestate: Some("rojo=00f067aa0ba902b7".to_string()),
        };
        let unsampled_tc = mimir_common::tracing::TraceContext {
            trace_id: [0x33; 16],
            span_id: [0x44; 8],
            sampled: false,
            trace_flags: 0x00,
            remote: false,
            jaeger_debug_id: None,
            tracestate: Some("rojo=unsampled".to_string()),
        };

        // 1. Sampled trace context injects traceparent & tracestate and records publish span.
        mimir_common::tracing::with_trace_context(sampled_tc.clone(), async {
            client
                .produce_sync(WriterRecord {
                    topic: "ingest-tr".to_string(),
                    partition: 0,
                    key: Bytes::from_static(b"t1"),
                    value: Bytes::from_static(b"v1"),
                    ..Default::default()
                })
                .await
                .unwrap();
        })
        .await;

        // 2. Unsampled trace context omits traceparent & tracestate and records no span.
        mimir_common::tracing::with_trace_context(unsampled_tc, async {
            client
                .produce_sync(WriterRecord {
                    topic: "ingest-tr".to_string(),
                    partition: 0,
                    key: Bytes::from_static(b"t2"),
                    value: Bytes::from_static(b"v2"),
                    ..Default::default()
                })
                .await
                .unwrap();
        })
        .await;

        // 3. No trace context omits traceparent & tracestate.
        client
            .produce_sync(WriterRecord {
                topic: "ingest-tr".to_string(),
                partition: 0,
                key: Bytes::from_static(b"t3"),
                value: Bytes::from_static(b"v3"),
                ..Default::default()
            })
            .await
            .unwrap();

        {
            let hdrs = captured_headers.lock();
            assert_eq!(hdrs.len(), 3);
        let tp = hdrs[0]
            .iter()
            .find(|(k, _)| k == "traceparent")
            .map(|(_, v)| String::from_utf8_lossy(v).to_string());
        assert_eq!(tp, Some(sampled_tc.to_traceparent()));
        let ts = hdrs[0]
            .iter()
            .find(|(k, _)| k == "tracestate")
            .map(|(_, v)| String::from_utf8_lossy(v).to_string());
        assert_eq!(ts.as_deref(), Some("rojo=00f067aa0ba902b7"));

        assert!(
            !hdrs[1]
                .iter()
                .any(|(k, _)| k == "traceparent" || k == "tracestate"),
            "unsampled trace must not inject headers, got {:?}",
            hdrs[1]
        );
        assert!(
            !hdrs[2]
                .iter()
                .any(|(k, _)| k == "traceparent" || k == "tracestate"),
            "absent trace must not inject headers, got {:?}",
            hdrs[2]
        );
        }

        // Now enable a tracing runtime (even with AlwaysOff root sampler) and verify a sampled
        // parent still mints a child producer span (same trace_id, fresh span_id) and exports
        // "<topic> publish" with kind=4 and parent_span_id=Some(sampled_tc.span_id).
        let exported_spans: Arc<SyncMutex<Vec<mimir_common::tracing::ExportedSpan>>> =
            Arc::new(SyncMutex::new(Vec::new()));
        let exp_cap = Arc::clone(&exported_spans);
        client.set_test_tracing(Some(mimir_common::tracing::TracingRuntime {
            enabled: true,
            service_name: "mimir-test".to_string(),
            propagators: mimir_common::tracing::PropagatorSet::default(),
            sampler: mimir_common::tracing::SamplerKind::AlwaysOff,
            resource_attributes: Vec::new(),
            exporter: Some(Arc::new(move |s| {
                exp_cap.lock().push(s);
            })),
        }));
        mimir_common::tracing::with_trace_context(sampled_tc.clone(), async {
            client
                .produce_sync(WriterRecord {
                    topic: "ingest-tr".to_string(),
                    partition: 0,
                    key: Bytes::from_static(b"t4"),
                    value: Bytes::from_static(b"v4"),
                    ..Default::default()
                })
                .await
                .unwrap();
        })
        .await;
        client.set_test_tracing(None);

        let hdrs = captured_headers.lock();
        assert_eq!(hdrs.len(), 4);
        let child_tp = hdrs[3]
            .iter()
            .find(|(k, _)| k == "traceparent")
            .map(|(_, v)| String::from_utf8_lossy(v).to_string())
            .expect("child traceparent must be injected");
        let parsed_child = mimir_common::tracing::parse_traceparent(&child_tp).unwrap();
        assert!(parsed_child.is_sampled());
        assert_eq!(parsed_child.trace_id, sampled_tc.trace_id);
        assert_ne!(parsed_child.span_id, sampled_tc.span_id);
        drop(hdrs);

        let spans = exported_spans.lock();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].name, "ingest-tr publish");
        assert_eq!(spans[0].kind, 4);
        assert_eq!(spans[0].trace_id, sampled_tc.trace_id);
        assert_eq!(spans[0].span_id, parsed_child.span_id);
        assert_eq!(spans[0].parent_span_id, Some(sampled_tc.span_id));
        assert!(!spans[0].status_error);

        client.close();
        srv.abort();
    }
}


