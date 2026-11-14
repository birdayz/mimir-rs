//! Kafka reader client factory and metrics configuration.
//!
//! Ported from github.com/grafana/mimir/pkg/storage/ingest/reader_client.go

use std::sync::Arc;
use std::time::Duration;

use mimir_config::KafkaConfig;
use prometheus::Registry;

use super::kafka_client::KafkaClientError;
use super::util::{IngestKafkaClient, KafkaClientExtendedMetrics};

pub const READER_FETCH_MIN_BYTES: i32 = 1;
pub const READER_FETCH_MAX_BYTES: i32 = 100_000_000;
pub const READER_FETCH_MAX_PARTITION_BYTES: i32 = 50_000_000;
pub const READER_BROKER_MAX_READ_BYTES: i32 = 1_000_000_000;

/// Kafka client configured for partition reading, supporting SASL authentication and metrics.
pub struct KafkaReaderClient {
    client: Arc<IngestKafkaClient>,
    pub fetch_min_bytes: i32,
    pub fetch_max_bytes: i32,
    pub fetch_max_wait: Duration,
    pub fetch_max_partition_bytes: i32,
    pub broker_max_read_bytes: i32,
}

impl KafkaReaderClient {
    pub fn new(
        cfg: &KafkaConfig,
        metrics: Option<KafkaClientExtendedMetrics>,
    ) -> Result<Self, KafkaClientError> {
        let client = Arc::new(IngestKafkaClient::from_config(cfg, metrics)?);
        let fetch_max_wait =
            Duration::from_nanos(cfg.fetch_max_wait.as_nanos().max(0) as u64);
        Ok(Self {
            client,
            fetch_min_bytes: READER_FETCH_MIN_BYTES,
            fetch_max_bytes: READER_FETCH_MAX_BYTES,
            fetch_max_wait,
            fetch_max_partition_bytes: READER_FETCH_MAX_PARTITION_BYTES,
            broker_max_read_bytes: READER_BROKER_MAX_READ_BYTES,
        })
    }

    pub fn inner(&self) -> &Arc<IngestKafkaClient> {
        &self.client
    }

    pub async fn ping(&self) -> Result<(), KafkaClientError> {
        self.client.ping().await
    }

    pub async fn close(&self) {
        self.client.close().await;
    }
}

pub fn new_kafka_reader_client(
    cfg: &KafkaConfig,
    metrics: Option<KafkaClientExtendedMetrics>,
) -> Result<KafkaReaderClient, KafkaClientError> {
    KafkaReaderClient::new(cfg, metrics)
}

pub fn new_kafka_reader_client_metrics(
    prefix: &str,
    component: &str,
    reg: Option<&Registry>,
) -> KafkaClientExtendedMetrics {
    KafkaClientExtendedMetrics::new(prefix, reg, Some(component))
}
