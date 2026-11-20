//! Kafka partition offset lookup client and Prometheus metrics.
//!
//! Ported from github.com/grafana/mimir/pkg/storage/ingest/partition_offset_client.go

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use mimir_common::native_histogram::{HistogramOpts, HistogramVec, DEF_BUCKETS};
use mimir_config::KafkaConfig;
use prometheus::{CounterVec, Opts, Registry};

use super::kafka_client::{KafkaClient, KafkaClientError, KAFKA_OFFSET_END};

#[derive(Clone)]
pub struct PartitionOffsetClientMetrics {
    pub last_produced_offset_requests_total: CounterVec,
    pub last_produced_offset_failures_total: CounterVec,
    pub last_produced_offset_latency: HistogramVec,
    pub partition_start_offset_requests_total: CounterVec,
    pub partition_start_offset_failures_total: CounterVec,
    pub partition_start_offset_latency: HistogramVec,
}

impl PartitionOffsetClientMetrics {
    pub fn new(registry: Option<&Registry>) -> Self {
        let last_produced_offset_requests_total = CounterVec::new(
            Opts::new(
                "cortex_ingest_storage_reader_last_produced_offset_requests_total",
                "Total number of requests issued to get the last produced offset.",
            ),
            &["topic", "partition"],
        )
        .expect("valid metric");
        let last_produced_offset_failures_total = CounterVec::new(
            Opts::new(
                "cortex_ingest_storage_reader_last_produced_offset_failures_total",
                "Total number of failed requests to get the last produced offset.",
            ),
            &["topic", "partition"],
        )
        .expect("valid metric");
        let last_produced_offset_latency = HistogramVec::new(
            HistogramOpts::new(
                "cortex_ingest_storage_reader_last_produced_offset_request_duration_seconds",
                "The duration of requests to fetch the last produced offset of a given partition.",
            )
            .buckets(DEF_BUCKETS.to_vec())
            .native(1.1, 100, Duration::from_secs(3600)),
            &["topic", "partition"],
        )
        .expect("valid metric");
        let partition_start_offset_requests_total = CounterVec::new(
            Opts::new(
                "cortex_ingest_storage_reader_partition_start_offset_requests_total",
                "Total number of requests issued to get the partition start offset.",
            ),
            &["topic", "partition"],
        )
        .expect("valid metric");
        let partition_start_offset_failures_total = CounterVec::new(
            Opts::new(
                "cortex_ingest_storage_reader_partition_start_offset_failures_total",
                "Total number of failed requests to get the partition start offset.",
            ),
            &["topic", "partition"],
        )
        .expect("valid metric");
        let partition_start_offset_latency = HistogramVec::new(
            HistogramOpts::new(
                "cortex_ingest_storage_reader_partition_start_offset_request_duration_seconds",
                "The duration of requests to fetch the start offset of a given partition.",
            )
            .buckets(DEF_BUCKETS.to_vec())
            .native(1.1, 100, Duration::from_secs(3600)),
            &["topic", "partition"],
        )
        .expect("valid metric");

        if let Some(reg) = registry {
            let _ = reg.register(Box::new(last_produced_offset_requests_total.clone()));
            let _ = reg.register(Box::new(last_produced_offset_failures_total.clone()));
            let _ = reg.register(Box::new(last_produced_offset_latency.clone()));
            let _ = reg.register(Box::new(partition_start_offset_requests_total.clone()));
            let _ = reg.register(Box::new(partition_start_offset_failures_total.clone()));
            let _ = reg.register(Box::new(partition_start_offset_latency.clone()));
        }

        Self {
            last_produced_offset_requests_total,
            last_produced_offset_failures_total,
            last_produced_offset_latency,
            partition_start_offset_requests_total,
            partition_start_offset_failures_total,
            partition_start_offset_latency,
        }
    }
}

/// Topic-agnostic client to read partition start & last produced offsets.
#[derive(Clone)]
pub struct PartitionOffsetClient {
    client: Arc<KafkaClient>,
    metrics: PartitionOffsetClientMetrics,
    closed: Arc<AtomicBool>,
}

impl PartitionOffsetClient {
    pub fn new(client: Arc<KafkaClient>, registry: Option<&Registry>) -> Self {
        Self {
            client,
            metrics: PartitionOffsetClientMetrics::new(registry),
            closed: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn from_config(cfg: &KafkaConfig, registry: Option<&Registry>) -> Result<Self, KafkaClientError> {
        let client = Arc::new(KafkaClient::from_config(cfg)?);
        Ok(Self::with_config(client, cfg, registry))
    }

    pub fn with_config(
        client: Arc<KafkaClient>,
        _cfg: &KafkaConfig,
        registry: Option<&Registry>,
    ) -> Self {
        Self {
            client,
            metrics: PartitionOffsetClientMetrics::new(registry),
            closed: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    pub fn kafka_client(&self) -> &Arc<KafkaClient> {
        &self.client
    }

    /// Fetches the last produced offset for `(topic, partition_id)`, or `-1` if empty.
    pub async fn fetch_partition_last_produced_offset(
        &self,
        topic: &str,
        partition_id: i32,
    ) -> Result<i64, KafkaClientError> {
        if self.is_closed() {
            return Err(KafkaClientError::ConnectionClosed);
        }
        let part_label = partition_id.to_string();
        let reqs = self
            .metrics
            .last_produced_offset_requests_total
            .with_label_values(&[topic, &part_label]);
        let fails = self
            .metrics
            .last_produced_offset_failures_total
            .with_label_values(&[topic, &part_label]);
        let lat = self
            .metrics
            .last_produced_offset_latency
            .with_label_values(&[topic, &part_label]);

        reqs.inc();
        let start = Instant::now();
        let res = self
            .client
            .fetch_partition_last_produced_offset(topic, partition_id)
            .await;
        lat.observe(start.elapsed().as_secs_f64());
        if res.is_err() {
            fails.inc();
        }
        res
    }

    /// Fetches the start offset (`ListOffsets -2`) for `(topic, partition_id)`.
    pub async fn fetch_partition_start_offset(
        &self,
        topic: &str,
        partition_id: i32,
    ) -> Result<i64, KafkaClientError> {
        if self.is_closed() {
            return Err(KafkaClientError::ConnectionClosed);
        }
        let part_label = partition_id.to_string();
        let reqs = self
            .metrics
            .partition_start_offset_requests_total
            .with_label_values(&[topic, &part_label]);
        let fails = self
            .metrics
            .partition_start_offset_failures_total
            .with_label_values(&[topic, &part_label]);
        let lat = self
            .metrics
            .partition_start_offset_latency
            .with_label_values(&[topic, &part_label]);

        reqs.inc();
        let start = Instant::now();
        let res = self
            .client
            .fetch_partition_start_offset(topic, partition_id)
            .await;
        lat.observe(start.elapsed().as_secs_f64());
        if res.is_err() {
            fails.inc();
        }
        res
    }

    /// Fetches last produced offsets for all requested `(topic, partition_ids)`, sharded by
    /// each partition's leader broker.
    pub async fn fetch_partitions_last_produced_offsets(
        &self,
        partitions_by_topic: &HashMap<String, Vec<i32>>,
    ) -> Result<HashMap<String, HashMap<i32, i64>>, KafkaClientError> {
        if self.is_closed() {
            return Err(KafkaClientError::ConnectionClosed);
        }
        let num_partitions: usize = partitions_by_topic.values().map(|v| v.len()).sum();
        if num_partitions == 0 {
            return Ok(HashMap::new());
        }

        let topic_label = if partitions_by_topic.len() == 1 {
            partitions_by_topic
                .keys()
                .next()
                .map(String::as_str)
                .unwrap_or("mixed")
        } else {
            "mixed"
        };

        let reqs = self
            .metrics
            .last_produced_offset_requests_total
            .with_label_values(&[topic_label, "mixed"]);
        let fails = self
            .metrics
            .last_produced_offset_failures_total
            .with_label_values(&[topic_label, "mixed"]);
        let lat = self
            .metrics
            .last_produced_offset_latency
            .with_label_values(&[topic_label, "mixed"]);

        reqs.inc();
        let start = Instant::now();

        let res = self
            .client
            .list_offsets_sharded(partitions_by_topic, KAFKA_OFFSET_END)
            .await
            .map(|mut map| {
                for pmap in map.values_mut() {
                    for off in pmap.values_mut() {
                        *off -= 1;
                    }
                }
                map
            });

        lat.observe(start.elapsed().as_secs_f64());
        if res.is_err() {
            fails.inc();
        }
        res
    }

    /// Lists sorted partition IDs for `topic`.
    pub async fn list_topic_partition_ids(
        &self,
        topic: &str,
    ) -> Result<Vec<i32>, KafkaClientError> {
        if self.is_closed() {
            return Err(KafkaClientError::ConnectionClosed);
        }
        self.client.list_topic_partition_ids(topic).await
    }
}
