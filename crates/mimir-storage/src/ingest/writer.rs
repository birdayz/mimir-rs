//! Kafka ingest_storage producer/writer client.
//!
//! Ported from github.com/grafana/mimir/pkg/storage/ingest/writer.go

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use mimir_config::KafkaConfig;
use mimir_proto::cortexpb;
use prometheus::{Counter, Histogram, HistogramOpts, HistogramVec, Opts, Registry};
use thiserror::Error;

use super::compression::KafkaCompression;
use super::kafka_client::{KafkaClient, KafkaClientError, ProduceRecord};
use super::version::{
    RecordCodecError, RecordSerializer, RecordVersion, MAX_PRODUCER_RECORD_DATA_BYTES_LIMIT,
};

#[derive(Debug, Error)]
pub enum WriterError {
    #[error("kafka client error: {0}")]
    Kafka(#[from] KafkaClientError),
    #[error("record codec error: {0}")]
    Codec(#[from] RecordCodecError),
    #[error("compression config error: {0}")]
    Compression(#[from] anyhow::Error),
}

#[derive(Clone)]
pub struct WriterMetrics {
    pub latency: HistogramVec,
    pub sent_bytes_total: Counter,
    pub input_bytes_total: Counter,
    pub records_per_request: Histogram,
    pub serialize_duration: Histogram,
}

impl WriterMetrics {
    pub fn new(registry: Option<&Registry>) -> Self {
        let latency = HistogramVec::new(
            HistogramOpts::new(
                "cortex_ingest_storage_writer_latency_seconds",
                "Latency to write an incoming request to Kafka partitions.",
            ),
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
        let records_per_request = Histogram::with_opts(HistogramOpts::new(
            "cortex_ingest_storage_writer_records_per_write_request",
            "The number of records a single per-partition write request has been split into.",
        ))
        .expect("valid metric");
        let serialize_duration = Histogram::with_opts(HistogramOpts::new(
            "cortex_ingest_storage_writer_serialize_duration_seconds",
            "Time spent serializing an incoming request to Kafka records.",
        ))
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
