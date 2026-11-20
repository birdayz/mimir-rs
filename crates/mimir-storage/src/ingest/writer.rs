//! Kafka ingest_storage producer/writer client.
//!
//! Ported from github.com/grafana/mimir/pkg/storage/ingest/writer.go

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use mimir_common::errors::DISTRIBUTOR_MAX_WRITE_REQUEST_DATA_ITEM_SIZE;
use mimir_common::native_histogram::{
    HistogramOpts as NativeHistogramOpts, HistogramVec as NativeHistogramVec, SingleHistogram,
    DEF_BUCKETS,
};
use mimir_config::KafkaConfig;
use mimir_proto::cortexpb;
use mimir_proto::prost::Message;
use parking_lot::RwLock;
use prometheus::{Counter, Histogram, HistogramOpts, Opts, Registry};
use thiserror::Error;

use super::compression::KafkaCompression;
use super::kafka_client::{KafkaClient, KafkaClientError, ProduceRecord};
use super::util::create_topics;
use super::version::{
    RecordCodecError, RecordSerializer, RecordVersion, SerializedKafkaRecord,
    MAX_PRODUCER_RECORD_DATA_BYTES_LIMIT,
};
use super::writer_client::{
    KafkaProducer, KafkaWriterClientOptions, ProduceContext, ProduceResult, ProducerError,
    WriterRecord, WRITER_METRICS_PREFIX,
};

pub const DEFAULT_MAX_INFLIGHT_PRODUCE_REQUESTS: usize = 20;

pub fn err_write_request_data_item_too_large_msg() -> String {
    DISTRIBUTOR_MAX_WRITE_REQUEST_DATA_ITEM_SIZE.message(&format!(
        "the write request contains a timeseries or metadata item which is larger that the maximum allowed size of {MAX_PRODUCER_RECORD_DATA_BYTES_LIMIT} bytes"
    ))
}

#[derive(Debug, Error)]
pub enum WriterError {
    #[error("kafka client error: {0}")]
    Kafka(#[from] KafkaClientError),
    #[error("record codec error: {0}")]
    Codec(#[from] RecordCodecError),
    #[error("compression config error: {0}")]
    Compression(#[from] anyhow::Error),
    #[error("{}", err_write_request_data_item_too_large_msg())]
    WriteRequestDataItemTooLarge,
    #[error("the Kafka writer client is not in the running state")]
    NotRunning,
    #[error("failed to write to partitions {partitions:?}: {source}")]
    FailedPartitions {
        partitions: Vec<i32>,
        source: ProducerError,
    },
    #[error("auto-create topics error: {0}")]
    AutoCreateTopics(String),
}

#[derive(Clone)]
pub struct WriterMetrics {
    pub latency: NativeHistogramVec,
    pub sent_bytes_total: Counter,
    pub input_bytes_total: Counter,
    pub records_per_request: Histogram,
    pub serialize_duration: SingleHistogram,
}

impl WriterMetrics {
    pub fn new(registry: Option<&Registry>) -> Self {
        let latency = NativeHistogramVec::new(
            NativeHistogramOpts::new(
                "cortex_ingest_storage_writer_latency_seconds",
                "Latency to write an incoming request to Kafka partitions.",
            )
            .buckets(DEF_BUCKETS.to_vec())
            .native(1.1, 100, Duration::from_secs(3600)),
            &["outcome"],
        )
        .expect("valid metric");
        let sent_bytes_total = Counter::with_opts(Opts::new(
            "cortex_ingest_storage_writer_sent_bytes_total",
            "Total number of bytes produced to the Kafka backend.",
        ))
        .expect("valid metric");
        let input_bytes_total = Counter::with_opts(Opts::new(
            "cortex_ingest_storage_writer_input_bytes_total",
            "Total number of bytes in write requests before conversion to the Kafka record format.",
        ))
        .expect("valid metric");
        let mut rpr_opts = HistogramOpts::new(
            "cortex_ingest_storage_writer_records_per_write_request",
            "The number of records a single per-partition write request has been split into.",
        );
        rpr_opts.buckets = prometheus::exponential_buckets(1.0, 2.0, 8).expect("valid buckets");
        let records_per_request = Histogram::with_opts(rpr_opts).expect("valid metric");
        let serialize_duration = SingleHistogram::new(
            NativeHistogramOpts::new(
                "cortex_ingest_storage_writer_serialize_duration_seconds",
                "Time spent serializing an incoming request to Kafka records.",
            )
            .buckets(DEF_BUCKETS.to_vec())
            .native(1.1, 100, Duration::from_secs(3600)),
        )
        .expect("valid metric");

        if let Some(reg) = registry {
            let _ = reg.register(Box::new(latency.clone()));
            let _ = reg.register(Box::new(sent_bytes_total.clone()));
            let _ = reg.register(Box::new(input_bytes_total.clone()));
            let _ = reg.register(Box::new(records_per_request.clone()));
            let _ = reg.register(Box::new(serialize_duration.clone()));
        }

        Self {
            latency,
            sent_bytes_total,
            input_bytes_total,
            records_per_request,
            serialize_duration,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PartitionWriteRequest<'a> {
    pub partition_id: i32,
    pub write_request: &'a cortexpb::WriteRequest,
}

/// Produces `cortexpb::WriteRequest`s to Kafka using record format v0, v1, or v2 and the
/// configured Kafka batch compression codec.
pub struct IngestWriter {
    client: Arc<KafkaClient>,
    serializer: RecordSerializer,
    compression: KafkaCompression,
    max_record_size_bytes: usize,
    metrics: WriterMetrics,
}

impl IngestWriter {
    pub fn from_config(
        cfg: &KafkaConfig,
        registry: Option<&Registry>,
    ) -> Result<Self, WriterError> {
        let client = Arc::new(KafkaClient::from_config(cfg)?);
        let version = RecordVersion::from_i64(cfg.producer_record_version)?;
        let compression: KafkaCompression = cfg.producer_compression.parse()?;
        let max_record_size_bytes = if cfg.producer_max_record_size_bytes <= 0 {
            MAX_PRODUCER_RECORD_DATA_BYTES_LIMIT
        } else {
            cfg.producer_max_record_size_bytes as usize
        };
        Ok(Self::with_client(
            client,
            version,
            compression,
            max_record_size_bytes,
            registry,
        ))
    }

    pub fn with_client(
        client: Arc<KafkaClient>,
        version: RecordVersion,
        compression: KafkaCompression,
        max_record_size_bytes: usize,
        registry: Option<&Registry>,
    ) -> Self {
        Self {
            client,
            serializer: RecordSerializer::new(version),
            compression,
            max_record_size_bytes,
            metrics: WriterMetrics::new(registry),
        }
    }

    pub fn kafka_client(&self) -> &Arc<KafkaClient> {
        &self.client
    }

    /// Serializes and produces `req` synchronously to `(topic, partition_id)`, returning the
    /// assigned base offset of the produced batch.
    pub async fn write_sync(
        &self,
        topic: &str,
        partition_id: i32,
        user_id: &str,
        req: &cortexpb::WriteRequest,
    ) -> Result<i64, WriterError> {
        let res = self
            .multi_write_sync(
                topic,
                user_id,
                &[PartitionWriteRequest {
                    partition_id,
                    write_request: req,
                }],
            )
            .await?;
        Ok(res.into_iter().next().map(|(_, off)| off).unwrap_or(-1))
    }

    /// Serializes and produces multiple partition requests, returning `(partition_id, base_offset)` pairs.
    pub async fn multi_write_sync(
        &self,
        topic: &str,
        user_id: &str,
        partition_requests: &[PartitionWriteRequest<'_>],
    ) -> Result<Vec<(i32, i64)>, WriterError> {
        let start = Instant::now();
        let mut by_partition: HashMap<i32, Vec<ProduceRecord>> = HashMap::new();
        let mut input_bytes = 0usize;
        let mut sent_bytes = 0usize;

        for pr in partition_requests {
            let (recs, orig_size) = self.serializer.to_records(
                topic,
                pr.partition_id,
                user_id,
                pr.write_request,
                self.max_record_size_bytes,
            )?;
            if recs.is_empty() {
                continue;
            }
            self.metrics.records_per_request.observe(recs.len() as f64);
            input_bytes += orig_size;
            let entry = by_partition.entry(pr.partition_id).or_default();
            for r in recs {
                sent_bytes += r.value.len();
                entry.push(ProduceRecord {
                    key: Some(r.key),
                    value: Some(r.value),
                    headers: r.headers,
                    timestamp_ms: None,
                });
            }
        }
        self.metrics
            .serialize_duration
            .observe(start.elapsed().as_secs_f64());

        if by_partition.is_empty() {
            return Ok(Vec::new());
        }

        let mut offsets = Vec::with_capacity(by_partition.len());
        for (pid, recs) in by_partition {
            match self
                .client
                .produce(topic, pid, self.compression, recs)
                .await
            {
                Ok(base_offset) => offsets.push((pid, base_offset)),
                Err(e) => {
                    self.metrics
                        .latency
                        .with_label_values(&["failure"])
                        .observe(start.elapsed().as_secs_f64());
                    return Err(WriterError::Kafka(e));
                }
            }
        }

        self.metrics
            .latency
            .with_label_values(&["success"])
            .observe(start.elapsed().as_secs_f64());
        self.metrics.sent_bytes_total.inc_by(sent_bytes as f64);
        self.metrics.input_bytes_total.inc_by(input_bytes as f64);

        Ok(offsets)
    }
}

// ---------------------------------------------------------------------------
// Full 1:1 Writer Service (ported from writer.go)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct WriterOptions {
    pub auto_create_topics: Option<Vec<String>>,
    pub disable_linger: bool,
    pub max_inflight_produce_requests: Option<usize>,
}

/// Full 1:1 port of `ingest.Writer` in `pkg/storage/ingest/writer.go`, backed by `KafkaProducer`.
pub struct Writer {
    kafka_cfg: KafkaConfig,
    registry: Option<Registry>,
    auto_create_topics: Vec<String>,
    disable_linger: bool,
    max_inflight_override: Option<usize>,
    serializer: RecordSerializer,
    max_record_size_bytes: usize,
    client: RwLock<Option<Arc<KafkaProducer>>>,
    metrics: WriterMetrics,
}

impl Writer {
    pub fn new(
        kafka_cfg: KafkaConfig,
        registry: Option<&Registry>,
        opts: WriterOptions,
    ) -> Result<Self, WriterError> {
        let version = RecordVersion::from_i64(kafka_cfg.producer_record_version)?;
        let max_record_size_bytes = if kafka_cfg.producer_max_record_size_bytes <= 0 {
            MAX_PRODUCER_RECORD_DATA_BYTES_LIMIT
        } else {
            kafka_cfg.producer_max_record_size_bytes as usize
        };
        let auto_create_topics = opts
            .auto_create_topics
            .unwrap_or_else(|| vec![kafka_cfg.topic.clone()]);
        let metrics = WriterMetrics::new(registry);

        Ok(Self {
            kafka_cfg,
            registry: registry.cloned(),
            auto_create_topics,
            disable_linger: opts.disable_linger,
            max_inflight_override: opts.max_inflight_produce_requests,
            serializer: RecordSerializer::new(version),
            max_record_size_bytes,
            client: RwLock::new(None),
            metrics,
        })
    }

    pub async fn start(&self) -> Result<(), WriterError> {
        if self.kafka_cfg.auto_create_topic_enabled {
            let topic_refs: Vec<&str> = self.auto_create_topics.iter().map(String::as_str).collect();
            create_topics(&self.kafka_cfg, &topic_refs)
                .await
                .map_err(WriterError::AutoCreateTopics)?;
        }

        let max_inflight = self
            .max_inflight_override
            .unwrap_or(DEFAULT_MAX_INFLIGHT_PRODUCE_REQUESTS)
            .max(1);
        let producer = KafkaProducer::from_config_with_prefix(
            &self.kafka_cfg,
            max_inflight,
            WRITER_METRICS_PREFIX,
            self.registry.as_ref(),
            KafkaWriterClientOptions {
                disable_default_topic: true,
                disable_linger: self.disable_linger,
            },
        )?;
        *self.client.write() = Some(producer);
        Ok(())
    }

    pub fn stop(&self) {
        if let Some(c) = self.client.write().take() {
            c.close();
        }
    }

    pub fn client(&self) -> Option<Arc<KafkaProducer>> {
        self.client.read().clone()
    }

    pub fn metrics(&self) -> &WriterMetrics {
        &self.metrics
    }

    pub async fn write_sync(
        &self,
        ctx: &ProduceContext,
        topic: &str,
        partition_id: i32,
        user_id: &str,
        req: &cortexpb::WriteRequest,
    ) -> Result<(), WriterError> {
        self.multi_write_sync(
            ctx,
            topic,
            user_id,
            &[PartitionWriteRequest {
                partition_id,
                write_request: req,
            }],
        )
        .await
    }

    pub async fn multi_write_sync(
        &self,
        ctx: &ProduceContext,
        topic: &str,
        user_id: &str,
        partition_requests: &[PartitionWriteRequest<'_>],
    ) -> Result<(), WriterError> {
        let Some(client) = self.client.read().clone() else {
            return Err(WriterError::NotRunning);
        };

        let start_time = Instant::now();
        let mut all_records = Vec::new();
        let mut request_size_bytes = 0usize;

        for pr in partition_requests {
            if is_write_request_empty(pr.write_request) {
                continue;
            }
            let (recs, req_size_bytes) = self.serializer.to_records(
                topic,
                pr.partition_id,
                user_id,
                pr.write_request,
                self.max_record_size_bytes,
            )?;
            self.metrics.records_per_request.observe(recs.len() as f64);
            for r in recs {
                all_records.push(WriterRecord {
                    topic: r.topic,
                    partition: r.partition,
                    key: r.key,
                    value: r.value,
                    headers: r.headers,
                    timestamp_ms: None,
                    offset: 0,
                });
            }
            request_size_bytes += req_size_bytes;
        }
        self.metrics
            .serialize_duration
            .observe(start_time.elapsed().as_secs_f64());

        if all_records.is_empty() {
            return Ok(());
        }

        let total_records = all_records.len();
        let results = client.produce_sync(ctx, all_records).await;

        let (succ_count, succ_bytes) = successful_produce_records_stats(&results);
        if succ_count == total_records {
            self.metrics
                .latency
                .with_label_values(&["success"])
                .observe(start_time.elapsed().as_secs_f64());
            self.metrics.sent_bytes_total.inc_by(succ_bytes as f64);
            self.metrics
                .input_bytes_total
                .inc_by(request_size_bytes as f64);
        } else {
            self.metrics
                .latency
                .with_label_values(&["failure"])
                .observe(start_time.elapsed().as_secs_f64());
        }

        produce_results_err(&results)
    }
}

pub fn is_write_request_empty(req: &cortexpb::WriteRequest) -> bool {
    req.timeseries.is_empty() && req.metadata.is_empty() && req.timeseries_rw2.is_empty()
}

pub fn successful_produce_records_stats(results: &[ProduceResult]) -> (usize, usize) {
    let mut count = 0usize;
    let mut size_bytes = 0usize;
    for res in results {
        if res.err.is_none() {
            count += 1;
            size_bytes += res.record.value.len();
        }
    }
    (count, size_bytes)
}

/// Inspects `results` and returns an appropriate `WriterError`.
/// Ported 1:1 from `produceResultsErr` in `pkg/storage/ingest/writer.go`.
pub fn produce_results_err(results: &[ProduceResult]) -> Result<(), WriterError> {
    let mut first_err: Option<&ProducerError> = None;
    let mut failed_partitions: Vec<i32> = Vec::new();

    for res in results {
        let Some(ref err) = res.err else {
            continue;
        };
        if matches!(err, ProducerError::MessageTooLarge) {
            return Err(WriterError::WriteRequestDataItemTooLarge);
        }
        if first_err.is_none() {
            first_err = Some(err);
        }
        if !failed_partitions.contains(&res.record.partition) {
            failed_partitions.push(res.record.partition);
        }
    }

    match first_err {
        None => Ok(()),
        Some(err) => Err(WriterError::FailedPartitions {
            partitions: failed_partitions,
            source: err.clone(),
        }),
    }
}

/// Marshals a `cortexpb::WriteRequest` into one or more `SerializedKafkaRecord`s using `splitter`.
/// Ported 1:1 from `marshalWriteRequestToRecords` in `pkg/storage/ingest/writer.go`.
pub fn marshal_write_request_to_records<F>(
    topic: &str,
    partition_id: i32,
    tenant_id: &str,
    req: &cortexpb::WriteRequest,
    req_size: usize,
    max_size: usize,
    splitter: F,
) -> Result<Vec<SerializedKafkaRecord>, RecordCodecError>
where
    F: FnOnce(&cortexpb::WriteRequest, usize, usize) -> Vec<cortexpb::WriteRequest>,
{
    let key = Bytes::copy_from_slice(tenant_id.as_bytes());
    if max_size == 0 || req_size <= max_size {
        let mut buf = Vec::with_capacity(req_size);
        req.encode(&mut buf)?;
        return Ok(vec![SerializedKafkaRecord {
            topic: topic.to_owned(),
            partition: partition_id,
            key,
            value: Bytes::from(buf),
            headers: Vec::new(),
        }]);
    }

    let split_reqs = splitter(req, req_size, max_size);
    let mut records = Vec::with_capacity(split_reqs.len());
    for part in split_reqs {
        let mut buf = Vec::with_capacity(part.encoded_len());
        part.encode(&mut buf)?;
        records.push(SerializedKafkaRecord {
            topic: topic.to_owned(),
            partition: partition_id,
            key: key.clone(),
            value: Bytes::from(buf),
            headers: Vec::new(),
        });
    }
    Ok(records)
}
