//! Kafka partition readers (single-cluster & multi-cluster) with zero-copy decoding,
//! offset tracking, and strong read consistency waiters.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/storage/ingest/reader.go
//! - github.com/grafana/mimir/pkg/storage/ingest/multi_cluster_reader.go

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::future::try_join_all;
use mimir_config::{KafkaConfig, OrderedConsumptionConfig};
use parking_lot::Mutex as SyncMutex;
use prometheus::{Counter, GaugeVec, Histogram, HistogramOpts, Opts, Registry};
use thiserror::Error;
use tokio::sync::{watch, Mutex};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use mimir_proto::zerocopy::{BorrowedWriteRequest, WriteRequestScratch};

use super::heap_merger::{HeapMerger, HeapMergerMetrics};
use super::kafka_client::{FetchedRecord, KafkaClient, KafkaClientError};
use super::offset_catalogue::PartitionOffsets;
use super::offset_file::OffsetFile;
use super::partition_offset_client::PartitionOffsetClient;
use super::partition_offset_reader::SingleClusterPartitionOffsetReader;
use super::partition_offset_watcher::{OffsetWatcherError, PartitionOffsetWatcher};
use super::version::{deserialize_record_content, parse_record_version, RecordCodecError};

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
    #[error("partition reader is not running")]
    NotRunning,
    #[error("invalid configuration: {0}")]
    Config(String),
    #[error("cluster offset mismatch: expected {expected} clusters, got {got}")]
    ClusterCountMismatch { expected: usize, got: usize },
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

#[derive(Clone)]
pub struct ReaderMetrics {
    pub fetches_total: Counter,
    pub fetches_errors: Counter,
    pub records_per_fetch: Histogram,
    pub consume_latency: Histogram,
    pub last_consumed_offset: GaugeVec,
}

impl ReaderMetrics {
    pub fn new(registry: Option<&Registry>) -> Self {
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
        let last_consumed_offset = GaugeVec::new(
            Opts::new(
                "cortex_ingest_storage_reader_last_consumed_offset",
                "The last offset successfully consumed by the partition reader. Set to -1 if no offset has been consumed yet.",
            ),
            &["partition"],
        )
        .expect("valid metric");

        if let Some(reg) = registry {
            let _ = reg.register(Box::new(fetches_total.clone()));
            let _ = reg.register(Box::new(fetches_errors.clone()));
            let _ = reg.register(Box::new(records_per_fetch.clone()));
            let _ = reg.register(Box::new(consume_latency.clone()));
            let _ = reg.register(Box::new(last_consumed_offset.clone()));
        }

        Self {
            fetches_total,
            fetches_errors,
            records_per_fetch,
            consume_latency,
            last_consumed_offset,
        }
    }
}

/// Consumes a single Kafka partition, tracks consumed offsets, persists offset file & group commits,
/// and notifies strong-consistency read waiters.
pub struct SingleClusterPartitionReader {
    kafka_cfg: KafkaConfig,
    partition_id: i32,
    consumer_group: String,
    client: Arc<KafkaClient>,
    consumer: Arc<dyn RecordBatchConsumer>,
    offset_file: Option<Arc<OffsetFile>>,
    offset_watcher: Arc<PartitionOffsetWatcher>,
    offset_reader: Arc<SingleClusterPartitionOffsetReader>,
    metrics: ReaderMetrics,
    last_seen_offset: Arc<AtomicI64>,
    last_committed_offset: Arc<AtomicI64>,
    highest_consumed_ts_before_end_ms: Arc<AtomicI64>,
    stop_tx: watch::Sender<bool>,
    task: Mutex<Option<JoinHandle<()>>>,
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
        let client = Arc::new(KafkaClient::from_config(&kafka_cfg)?);
        Self::with_client(
            kafka_cfg,
            partition_id,
            instance_id,
            offset_file_path,
            client,
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

        let metrics = ReaderMetrics::new(registry);
        metrics
            .last_consumed_offset
            .with_label_values(&[&partition_id.to_string()])
            .set(-1.0);

        let (stop_tx, _) = watch::channel(false);

        Ok(Self {
            kafka_cfg,
            partition_id,
            consumer_group,
            client,
            consumer,
            offset_file,
            offset_watcher,
            offset_reader,
            metrics,
            last_seen_offset: Arc::new(AtomicI64::new(-1)),
            last_committed_offset: Arc::new(AtomicI64::new(-1)),
            highest_consumed_ts_before_end_ms: Arc::new(AtomicI64::new(0)),
            stop_tx,
            task: Mutex::new(None),
        })
    }

    pub fn kafka_client(&self) -> &Arc<KafkaClient> {
        &self.client
    }

    pub fn offset_watcher(&self) -> &Arc<PartitionOffsetWatcher> {
        &self.offset_watcher
    }

    pub fn last_consumed_offset(&self) -> i64 {
        self.offset_watcher.last_consumed_offset()
    }

    pub fn last_seen_offset(&self) -> i64 {
        self.last_seen_offset.load(Ordering::SeqCst)
    }

    pub fn last_committed_offset(&self) -> i64 {
        self.last_committed_offset.load(Ordering::SeqCst)
    }

    pub fn last_seen_offsets(&self) -> PartitionOffsets {
        PartitionOffsets::new_single_cluster(self.last_seen_offset())
    }

    async fn resolve_start_offset(&self) -> Result<(i64, i64), ReaderError> {
        let topic = &self.kafka_cfg.topic;
        match self.kafka_cfg.consume_from_position_at_startup.as_str() {
            "start" => {
                let start = self
                    .client
                    .fetch_partition_start_offset(topic, self.partition_id)
                    .await?;
                Ok((start, -1))
            }
            "end" => {
                let last = self
                    .client
                    .fetch_partition_last_produced_offset(topic, self.partition_id)
                    .await?;
                Ok((last + 1, -1))
            }
            "timestamp" => {
                let ts = self.kafka_cfg.consume_from_timestamp_at_startup;
                let listed = self
                    .client
                    .list_offset_with_timestamp(topic, self.partition_id, ts)
                    .await?;
                let start = if listed.offset >= 0 {
                    listed.offset
                } else {
                    self.client
                        .fetch_partition_start_offset(topic, self.partition_id)
                        .await?
                };
                Ok((start, start - 1))
            }
            _ => {
                // "last-offset"
                if self.kafka_cfg.consumer_group_offset_commit_file_enforced {
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
                                "file-stored offset older than partition start; resuming from partition start"
                            );
                        }
                    }
                } else if let Ok(Some(committed)) = self
                    .client
                    .fetch_committed_offset(&self.consumer_group, topic, self.partition_id)
                    .await
                {
                    return Ok((committed + 1, committed));
                }

                let start = self
                    .client
                    .fetch_partition_start_offset(topic, self.partition_id)
                    .await?;
                Ok((start, -1))
            }
        }
    }

    /// Starts the partition reader background fetch/consume loop and offset poller.
    pub async fn start(&self) -> Result<(), ReaderError> {
        let mut guard = self.task.lock().await;
        if guard.is_some() {
            return Ok(());
        }

        if self.kafka_cfg.auto_create_topic_enabled {
            let parts = self.kafka_cfg.auto_create_topic_default_partitions as i32;
            self.client
                .create_topics(&[&self.kafka_cfg.topic], parts, -1)
                .await?;
        }

        let (start_offset, last_consumed) = self.resolve_start_offset().await?;
        self.last_seen_offset.store(last_consumed, Ordering::SeqCst);
        if last_consumed >= 0 {
            self.offset_watcher.notify(last_consumed);
            self.metrics
                .last_consumed_offset
                .with_label_values(&[&self.partition_id.to_string()])
                .set(last_consumed as f64);
        }

        self.offset_reader.start().await;

        let client = Arc::clone(&self.client);
        let consumer = Arc::clone(&self.consumer);
        let offset_file = self.offset_file.clone();
        let offset_watcher = Arc::clone(&self.offset_watcher);
        let metrics = self.metrics.clone();
        let last_seen_offset = Arc::clone(&self.last_seen_offset);
        let last_committed_offset = Arc::clone(&self.last_committed_offset);
        let highest_ts_ref = Arc::clone(&self.highest_consumed_ts_before_end_ms);
        let topic = self.kafka_cfg.topic.clone();
        let partition_id = self.partition_id;
        let consumer_group = self.consumer_group.clone();
        let fetch_max_wait = Duration::from_nanos(self.kafka_cfg.fetch_max_wait.as_nanos().max(0) as u64)
            .min(Duration::from_millis(250))
            .max(Duration::from_millis(20));
        let commit_interval = Duration::from_nanos(
            self.kafka_cfg
                .consumer_group_offset_commit_interval
                .as_nanos()
                .max(0) as u64,
        )
        .max(Duration::from_millis(50));
        let mut stop_rx = self.stop_tx.subscribe();

        let handle = tokio::spawn(async move {
            let mut next_offset = start_offset.max(0);
            let mut last_commit_at = tokio::time::Instant::now();
            let mut pending_commit: Option<i64> = None;

            loop {
                if *stop_rx.borrow() {
                    break;
                }

                let fetch_res = tokio::select! {
                    _ = stop_rx.changed() => {
                        if *stop_rx.borrow() {
                            break;
                        }
                        continue;
                    }
                    res = client.fetch(&topic, partition_id, next_offset, fetch_max_wait, 1, 50 * 1024 * 1024) => res,
                };

                match fetch_res {
                    Ok(batch) => {
                        metrics.fetches_total.inc();
                        metrics
                            .records_per_fetch
                            .observe(batch.records.len() as f64);

                        if !batch.records.is_empty() {
                            let batch_last_offset = batch.records.last().unwrap().offset;
                            let highest_ts =
                                batch.records.iter().map(|r| r.timestamp_ms).max().unwrap_or(0);
                            last_seen_offset.fetch_max(batch_last_offset, Ordering::SeqCst);

                            let consume_start = std::time::Instant::now();
                            loop {
                                if *stop_rx.borrow() {
                                    break;
                                }
                                match consumer.consume(batch.records.clone()).await {
                                    Ok(()) => break,
                                    Err(err) => {
                                        error!(
                                            partition = partition_id,
                                            error = %err,
                                            "error consuming Kafka batch; retrying"
                                        );
                                        tokio::time::sleep(Duration::from_millis(200)).await;
                                    }
                                }
                            }
                            metrics
                                .consume_latency
                                .observe(consume_start.elapsed().as_secs_f64());

                            if batch_last_offset + 1 >= batch.high_watermark {
                                highest_ts_ref.store(0, Ordering::SeqCst);
                            } else {
                                highest_ts_ref.store(highest_ts, Ordering::SeqCst);
                            }

                            offset_watcher.notify(batch_last_offset);
                            metrics
                                .last_consumed_offset
                                .with_label_values(&[&partition_id.to_string()])
                                .set(batch_last_offset as f64);

                            next_offset = batch_last_offset + 1;
                            pending_commit = Some(batch_last_offset);
                        }
                    }
                    Err(err) => {
                        metrics.fetches_errors.inc();
                        debug!(partition = partition_id, error = %err, "kafka fetch error");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }

                if let Some(off) = pending_commit {
                    if last_commit_at.elapsed() >= commit_interval {
                        Self::persist_offset(
                            &client,
                            offset_file.as_deref(),
                            &consumer_group,
                            topic.as_str(),
                            partition_id,
                            off,
                            &last_committed_offset,
                        )
                        .await;
                        pending_commit = None;
                        last_commit_at = tokio::time::Instant::now();
                    }
                }
            }

            if let Some(off) = pending_commit {
                Self::persist_offset(
                    &client,
                    offset_file.as_deref(),
                    &consumer_group,
                    topic.as_str(),
                    partition_id,
                    off,
                    &last_committed_offset,
                )
                .await;
            }
        });

        *guard = Some(handle);
        info!(partition = self.partition_id, start_offset, "started partition reader");
        Ok(())
    }

    async fn persist_offset(
        client: &KafkaClient,
        offset_file: Option<&OffsetFile>,
        consumer_group: &str,
        topic: &str,
        partition_id: i32,
        offset: i64,
        last_committed: &AtomicI64,
    ) {
        if let Some(f) = offset_file {
            if let Err(e) = f.write(offset) {
                warn!(partition = partition_id, error = %e, "failed to write offset file");
            }
        }
        if !consumer_group.is_empty() {
            if let Err(e) = client
                .commit_offset(consumer_group, topic, partition_id, offset)
                .await
            {
                debug!(partition = partition_id, error = %e, "failed to commit offset to Kafka");
            } else {
                last_committed.store(offset, Ordering::SeqCst);
            }
        } else {
            last_committed.store(offset, Ordering::SeqCst);
        }
    }

    pub async fn stop(&self) {
        let _ = self.stop_tx.send(true);
        self.offset_watcher.stop();
        self.offset_reader.stop().await;
        if let Some(handle) = self.task.lock().await.take() {
            let _ = handle.await;
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

    /// Waits until all records produced up to the next polled last-produced-offset have been consumed.
    pub async fn wait_read_consistency_until_last_produced_offset(
        &self,
        timeout: Duration,
    ) -> Result<(), ReaderError> {
        let target = self
            .offset_reader
            .wait_next_fetch_last_produced_offset()
            .await
            .map_err(ReaderError::OffsetReader)?;
        self.offset_watcher.wait_for_offset(target, timeout).await?;
        Ok(())
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
        // If merger is active, wrap via Arc<HeapMerger> so each cluster reader submits into it.
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
            let r = Arc::new(SingleClusterPartitionReader::new(
                cfg.clone(),
                partition_id,
                &cluster_instance,
                off_path,
                cluster_consumer,
                registry,
            )?);
            readers.push(r);
        }

        // Note: if shared_merger was created, we retain a handle to stop it on shutdown if needed.
        let merger_holder = shared_merger.and_then(|arc| Arc::try_unwrap(arc).ok()).map(|m| Mutex::new(Some(m)));

        Ok(Self {
            readers,
            merger: merger_holder,
        })
    }

    pub async fn start(&self) -> Result<(), ReaderError> {
        for r in &self.readers {
            r.start().await?;
        }
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
