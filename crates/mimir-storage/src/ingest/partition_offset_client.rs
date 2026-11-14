//! Kafka partition offset lookup client and Prometheus metrics.
//!
//! Ported from github.com/grafana/mimir/pkg/storage/ingest/partition_offset_client.go

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use prometheus::{CounterVec, HistogramOpts, HistogramVec, Opts, Registry};

use super::kafka_client::{KafkaClient, KafkaClientError};

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
            ),
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
            ),
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
}

impl PartitionOffsetClient {
    pub fn new(client: Arc<KafkaClient>, registry: Option<&Registry>) -> Self {
        Self {
            client,
            metrics: PartitionOffsetClientMetrics::new(registry),
        }
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

    /// Fetches last produced offsets for all requested `(topic, partition_ids)`.
    pub async fn fetch_partitions_last_produced_offsets(
        &self,
        partitions_by_topic: &HashMap<String, Vec<i32>>,
    ) -> Result<HashMap<String, HashMap<i32, i64>>, KafkaClientError> {
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

        let mut out: HashMap<String, HashMap<i32, i64>> =
            HashMap::with_capacity(partitions_by_topic.len());
        for (topic, pids) in partitions_by_topic {
            let mut pmap = HashMap::with_capacity(pids.len());
            for &pid in pids {
                match self
                    .client
                    .fetch_partition_last_produced_offset(topic, pid)
                    .await
                {
                    Ok(off) => {
                        pmap.insert(pid, off);
                    }
                    Err(e) => {
                        lat.observe(start.elapsed().as_secs_f64());
                        fails.inc();
                        return Err(e);
                    }
                }
            }
            out.insert(topic.clone(), pmap);
        }

        lat.observe(start.elapsed().as_secs_f64());
        Ok(out)
    }

    /// Lists sorted partition IDs for `topic`.
    pub async fn list_topic_partition_ids(
        &self,
        topic: &str,
    ) -> Result<Vec<i32>, KafkaClientError> {
        self.client.list_topic_partition_ids(topic).await
    }
}
