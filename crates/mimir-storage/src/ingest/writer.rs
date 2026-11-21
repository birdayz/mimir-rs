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

use kafka_protocol::error::ResponseError;

use super::compression::KafkaCompression;
use super::kafka_client::{KafkaClient, KafkaClientError, ProduceRecord};
use super::util::{create_topics, IngestKafkaClient, SampledOnlyPropagator};
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
    write_timeout: Duration,
    metrics: WriterMetrics,
}

fn ingest_writer_retriable_err(err: &KafkaClientError) -> bool {
    match err {
        KafkaClientError::Broker(resp) => super::kgo_retry::is_retriable_response_error(*resp),
        KafkaClientError::Io(_)
        | KafkaClientError::RequestTimeout(_)
        | KafkaClientError::ConnectTimeout { .. }
        | KafkaClientError::ChosenBrokerDead
        | KafkaClientError::UnknownController(_)
        | KafkaClientError::UnknownBroker => true,
        _ => false,
    }
}

impl IngestWriter {
    pub fn from_config(
        cfg: &KafkaConfig,
        registry: Option<&Registry>,
    ) -> Result<Self, WriterError> {
        let client = Arc::new(
            IngestKafkaClient::from_config_for_producer(cfg, None)?.into_inner(),
        );
        let version = RecordVersion::from_i64(cfg.producer_record_version)?;
        let compression: KafkaCompression = if cfg.producer_compression.trim().is_empty() {
            KafkaCompression::Snappy
        } else {
            cfg.producer_compression.parse()?
        };
        let max_record_size_bytes = if cfg.producer_max_record_size_bytes <= 0 {
            MAX_PRODUCER_RECORD_DATA_BYTES_LIMIT
        } else {
            cfg.producer_max_record_size_bytes as usize
        };
        let raw_wt = Duration::from_nanos(cfg.write_timeout.as_nanos().max(0) as u64);
        let write_timeout = if raw_wt.is_zero() {
            Duration::from_secs(10)
        } else {
            raw_wt
        };
        let mut w = Self::with_client(
            client,
            version,
            compression,
            max_record_size_bytes,
            registry,
        );
        w.write_timeout = write_timeout;
        Ok(w)
    }

    pub fn with_client(
        client: Arc<KafkaClient>,
        version: RecordVersion,
        compression: KafkaCompression,
        max_record_size_bytes: usize,
        registry: Option<&Registry>,
    ) -> Self {
        let pt = client.produce_timeout();
        let write_timeout = if pt.is_zero() {
            Duration::from_secs(10)
        } else {
            pt
        };
        Self {
            client,
            serializer: RecordSerializer::new(version),
            compression,
            max_record_size_bytes,
            write_timeout,
            metrics: WriterMetrics::new(registry),
        }
    }

    pub fn compression(&self) -> KafkaCompression {
        self.compression
    }

    pub fn write_timeout(&self) -> Duration {
        self.write_timeout
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
        type PartitionSpanEntries =
            Vec<(mimir_common::tracing::TraceContext, Option<[u8; 8]>, u64)>;
        let mut partition_spans: HashMap<i32, PartitionSpanEntries> = HashMap::new();
        let mut input_bytes = 0usize;
        let mut sent_bytes = 0usize;
        let active_tc = mimir_common::tracing::current_trace_context()
            .filter(mimir_common::tracing::TraceContext::is_sampled);
        let rt = mimir_common::tracing::global_tracing();

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
            let start_ns = if active_tc.is_some()
                && rt.is_some_and(|r| r.enabled && r.exporter.is_some())
            {
                mimir_common::tracing::now_unix_nanos()
            } else {
                0
            };
            for mut r in recs {
                if let Some(ref tc) = active_tc {
                    r.headers.retain(|(k, _)| {
                        !k.eq_ignore_ascii_case("traceparent")
                            && !k.eq_ignore_ascii_case("tracestate")
                    });
                    let (prod_ctx, parent_span_id, tracing_enabled) =
                        mimir_common::tracing::start_kafka_sampled_span_context_with_runtime(
                            rt, tc,
                        );
                    SampledOnlyPropagator.inject_trace_context(Some(&prod_ctx), &mut r.headers);
                    if tracing_enabled && rt.is_some_and(|r| r.exporter.is_some()) {
                        partition_spans
                            .entry(pr.partition_id)
                            .or_default()
                            .push((prod_ctx, parent_span_id, start_ns));
                    }
                }
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

        let produce_futs = by_partition.into_iter().map(|(pid, recs)| {
            let client = Arc::clone(&self.client);
            let compression = self.compression;
            let write_timeout = self.write_timeout;
            let spans = partition_spans.remove(&pid).unwrap_or_default();
            async move {
                let mut retry_backoff = Duration::from_millis(25);
                let mut last_err: Option<KafkaClientError> = None;
                let outcome: Result<(i32, i64), (i32, WriterError)> = loop {
                    let elapsed = start.elapsed();
                    if elapsed >= write_timeout {
                        break Err((
                            pid,
                            WriterError::Kafka(
                                last_err
                                    .unwrap_or(KafkaClientError::RequestTimeout(write_timeout)),
                            ),
                        ));
                    }
                    let remaining = write_timeout.saturating_sub(elapsed);
                    let res = tokio::time::timeout(
                        remaining,
                        client.produce(topic, pid, compression, recs.clone()),
                    )
                    .await;
                    match res {
                        Ok(Ok(base_offset)) => break Ok((pid, base_offset)),
                        Ok(Err(KafkaClientError::Broker(ResponseError::MessageTooLarge))) => {
                            break Err((pid, WriterError::WriteRequestDataItemTooLarge));
                        }
                        Ok(Err(KafkaClientError::Broker(
                            ResponseError::UnknownTopicOrPartition,
                        ))) => {
                            last_err = Some(KafkaClientError::Broker(
                                ResponseError::UnknownTopicOrPartition,
                            ));
                            let unknown_limit = write_timeout.min(Duration::from_secs(2));
                            if start.elapsed() >= unknown_limit {
                                break Err((
                                    pid,
                                    WriterError::Kafka(last_err.unwrap_or(
                                        KafkaClientError::RequestTimeout(write_timeout),
                                    )),
                                ));
                            }
                            let meta_remaining = unknown_limit.saturating_sub(start.elapsed());
                            let _ = tokio::time::timeout(
                                meta_remaining,
                                client.refresh_metadata(&[topic]),
                            )
                            .await;
                            if start.elapsed() >= unknown_limit {
                                break Err((
                                    pid,
                                    WriterError::Kafka(last_err.unwrap_or(
                                        KafkaClientError::RequestTimeout(write_timeout),
                                    )),
                                ));
                            }
                            retry_backoff = (retry_backoff * 2).min(Duration::from_millis(250));
                        }
                        Ok(Err(e)) if ingest_writer_retriable_err(&e) => {
                            last_err = Some(e);
                            retry_backoff = Duration::from_millis(25);
                        }
                        Ok(Err(e)) => break Err((pid, WriterError::Kafka(e))),
                        Err(_) => {
                            break Err((
                                pid,
                                WriterError::Kafka(last_err.unwrap_or(
                                    KafkaClientError::RequestTimeout(write_timeout),
                                )),
                            ));
                        }
                    }

                    let remaining = write_timeout.saturating_sub(start.elapsed());
                    if remaining.is_zero() {
                        break Err((
                            pid,
                            WriterError::Kafka(
                                last_err
                                    .unwrap_or(KafkaClientError::RequestTimeout(write_timeout)),
                            ),
                        ));
                    }
                    tokio::time::sleep(retry_backoff.min(remaining)).await;
                };

                if !spans.is_empty() {
                    if let Some(rt) = mimir_common::tracing::global_tracing() {
                        if rt.exporter.is_some() {
                            let end_ns = mimir_common::tracing::now_unix_nanos();
                            let is_err = outcome.is_err();
                            for (prod_ctx, parent_span_id, start_ns) in spans {
                                mimir_common::tracing::export_span_with_runtime(
                                    rt,
                                    mimir_common::tracing::ExportedSpan {
                                        service_name: rt.service_name.clone(),
                                        trace_id: prod_ctx.trace_id,
                                        span_id: prod_ctx.span_id,
                                        parent_span_id,
                                        name: format!("{topic} publish"),
                                        kind: 4, // OTLP SPAN_KIND_PRODUCER
                                        start_time_unix_nano: start_ns,
                                        end_time_unix_nano: end_ns,
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
                                                    topic.to_string(),
                                                ),
                                            },
                                            mimir_common::tracing::SpanAttribute {
                                                key: "messaging.kafka.destination.partition"
                                                    .to_string(),
                                                value: mimir_common::tracing::SpanAttrValue::Int(
                                                    pid as i64,
                                                ),
                                            },
                                        ],
                                        status_error: is_err,
                                    },
                                );
                            }
                        }
                    }
                }
                outcome
            }
        });

        let results = futures::future::join_all(produce_futs).await;
        let mut offsets = Vec::with_capacity(results.len());
        let mut failed_partitions = Vec::new();
        let mut first_err: Option<ProducerError> = None;
        let mut has_too_large = false;
        for r in results {
            match r {
                Ok(pair) => offsets.push(pair),
                Err((pid, WriterError::WriteRequestDataItemTooLarge)) => {
                    has_too_large = true;
                    if !failed_partitions.contains(&pid) {
                        failed_partitions.push(pid);
                    }
                }
                Err((pid, e)) => {
                    if first_err.is_none() {
                        first_err = Some(match e {
                            WriterError::Kafka(KafkaClientError::RequestTimeout(_)) => {
                                ProducerError::RecordTimeout
                            }
                            WriterError::Kafka(ke) => ProducerError::Other(ke.to_string()),
                            other => ProducerError::Other(other.to_string()),
                        });
                    }
                    if !failed_partitions.contains(&pid) {
                        failed_partitions.push(pid);
                    }
                }
            }
        }
        if has_too_large {
            self.metrics
                .latency
                .with_label_values(&["failure"])
                .observe(start.elapsed().as_secs_f64());
            return Err(WriterError::WriteRequestDataItemTooLarge);
        }
        if let Some(source) = first_err {
            self.metrics
                .latency
                .with_label_values(&["failure"])
                .observe(start.elapsed().as_secs_f64());
            return Err(WriterError::FailedPartitions {
                partitions: failed_partitions,
                source,
            });
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
        ApiKey, BrokerId, MetadataResponse, ProduceRequest, ProduceResponse, RequestHeader,
        ResponseHeader, TopicName,
    };
    use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
    use mimir_config::types::StringSliceCSV;
    use mimir_config::GoDuration;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[test]
    fn ingest_writer_from_config_defaults_compression_to_snappy_and_honors_overhead() {
        let cfg = KafkaConfig {
            address: StringSliceCSV(vec!["127.0.0.1:9092".to_string()]),
            topic: "ingest".to_string(),
            write_timeout: GoDuration::from_millis(250),
            write_timeout_overhead: GoDuration::from_secs(3),
            ..Default::default()
        };
        let w = IngestWriter::from_config(&cfg, None).unwrap();
        assert_eq!(w.compression(), KafkaCompression::Snappy);
        assert_eq!(w.write_timeout(), Duration::from_millis(250));
        assert_eq!(w.kafka_client().request_timeout(), Duration::from_secs(3));

        let mut none_cfg = cfg.clone();
        none_cfg.producer_compression = "none".to_string();
        let w_none = IngestWriter::from_config(&none_cfg, None).unwrap();
        assert_eq!(w_none.compression(), KafkaCompression::None);
    }

    #[tokio::test]
    async fn ingest_writer_retries_and_maps_message_too_large_and_writes_concurrently() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let p0_attempts = Arc::new(AtomicUsize::new(0));
        let p0_srv = Arc::clone(&p0_attempts);

        let srv = tokio::spawn(async move {
            loop {
                let Ok((sock, _)) = listener.accept().await else {
                    break;
                };
                let p0_c = Arc::clone(&p0_srv);
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
                                t.name = Some(TopicName(StrBytes::from_static_str("ingest-w")));
                                for pid in 0..3 {
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
                                            let n = p0_c.fetch_add(1, Ordering::SeqCst);
                                            if n == 0 {
                                                pr.error_code =
                                                    ResponseError::RequestTimedOut.code();
                                            } else {
                                                pr.error_code = 0;
                                                pr.base_offset = 10;
                                            }
                                        } else if rp.index == 1 {
                                            pr.error_code = 0;
                                            pr.base_offset = 20;
                                        } else {
                                            pr.error_code =
                                                ResponseError::MessageTooLarge.code();
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
            topic: "ingest-w".to_string(),
            write_timeout: GoDuration::from_secs(2),
            write_timeout_overhead: GoDuration::from_millis(500),
            ..Default::default()
        };
        cfg.dial_timeout = GoDuration::from_secs(2);
        let writer = IngestWriter::from_config(&cfg, None).unwrap();

        let sample_req = cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![cortexpb::LabelPair::from_static("__name__", "m")],
                samples: vec![cortexpb::Sample {
                    value: 1.0,
                    timestamp_ms: 1000,
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut res = writer
            .multi_write_sync(
                "ingest-w",
                "user-1",
                &[
                    PartitionWriteRequest {
                        partition_id: 0,
                        write_request: &sample_req,
                    },
                    PartitionWriteRequest {
                        partition_id: 1,
                        write_request: &sample_req,
                    },
                ],
            )
            .await
            .unwrap();
        res.sort_by_key(|(pid, _)| *pid);
        assert_eq!(res, vec![(0, 10), (1, 20)]);
        assert_eq!(p0_attempts.load(Ordering::SeqCst), 2);

        let err = writer
            .write_sync("ingest-w", 2, "user-1", &sample_req)
            .await
            .unwrap_err();
        assert!(
            matches!(err, WriterError::WriteRequestDataItemTooLarge),
            "expected WriteRequestDataItemTooLarge, got {err:?}"
        );

        srv.abort();
    }
}
