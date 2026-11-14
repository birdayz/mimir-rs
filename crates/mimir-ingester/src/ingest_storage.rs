//! Kafka ingest_storage integration for the ingester.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/ingester/ingester.go (ingest_storage init)
//! - github.com/grafana/mimir/pkg/storage/ingest/util.go (IngesterPartitionID)

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use mimir_proto::ringpb::PartitionRingDesc;
use mimir_ring::partition_ring::PartitionRingWatcherDelegate;
use mimir_storage::ingest::{
    DecodedKafkaRecord, MultiClusterPartitionReader, PartitionOffsets, ReaderError,
    RecordBatchConsumer, SingleClusterPartitionReader, ZeroCopyBatchConsumer,
    ZeroCopyRecordHandler,
};
use tracing::{error, info};

use crate::ingester::Ingester;

/// Parses the numeric partition ID suffix from `ingester_id` (e.g. `"ingester-zone-a-2"` -> `2`).
pub fn ingester_partition_id_from_instance_id(ingester_id: &str) -> Result<i32, String> {
    let Some(pos) = ingester_id.rfind('-') else {
        return Err(format!(
            "ingester ID {ingester_id} doesn't match regular expression \"-([0-9]+)$\""
        ));
    };
    let suffix = &ingester_id[pos + 1..];
    if suffix.is_empty() || !suffix.bytes().all(|b| b.is_ascii_digit()) {
        return Err(format!(
            "ingester ID {ingester_id} doesn't match regular expression \"-([0-9]+)$\""
        ));
    }
    suffix
        .parse::<i32>()
        .map_err(|e| format!("no ingester sequence number in ingester ID {ingester_id}: {e}"))
}

pub enum IngestReaderHandle {
    Single(Arc<SingleClusterPartitionReader>),
    Multi(Arc<MultiClusterPartitionReader>),
}

impl IngestReaderHandle {
    pub fn last_seen_offsets(&self) -> PartitionOffsets {
        match self {
            Self::Single(r) => r.last_seen_offsets(),
            Self::Multi(r) => r.last_seen_offsets(),
        }
    }

    pub async fn wait_read_consistency_until_offsets(
        &self,
        offsets: &PartitionOffsets,
        timeout: Duration,
    ) -> Result<(), ReaderError> {
        match self {
            Self::Single(r) => {
                if offsets.num_kafka_clusters() != 1 {
                    return Err(ReaderError::ClusterCountMismatch {
                        expected: 1,
                        got: offsets.num_kafka_clusters(),
                    });
                }
                r.wait_for_offset(offsets.for_kafka_cluster(0), timeout)
                    .await?;
                Ok(())
            }
            Self::Multi(r) => {
                r.wait_read_consistency_until_offsets(offsets, timeout)
                    .await
            }
        }
    }

    pub async fn wait_read_consistency_until_last_produced_offset(
        &self,
        timeout: Duration,
    ) -> Result<(), ReaderError> {
        match self {
            Self::Single(r) => {
                r.wait_read_consistency_until_last_produced_offset(timeout)
                    .await
            }
            Self::Multi(r) => {
                r.wait_read_consistency_until_last_produced_offset(timeout)
                    .await
            }
        }
    }

    pub fn enforce_read_max_delay(&self, max_delay: Duration) -> Result<(), ReaderError> {
        match self {
            Self::Single(r) => r.enforce_read_max_delay(max_delay),
            Self::Multi(r) => r.enforce_read_max_delay(max_delay),
        }
    }

    pub async fn stop(&self) {
        match self {
            Self::Single(r) => r.stop().await,
            Self::Multi(r) => r.stop().await,
        }
    }
}

struct IngesterZeroCopyHandler {
    ingester: Arc<Ingester>,
}

impl ZeroCopyRecordHandler for IngesterZeroCopyHandler {
    fn on_record(&self, record: DecodedKafkaRecord<'_, '_>) -> Result<(), String> {
        if record.tenant.is_empty() {
            return Ok(());
        }
        match self
            .ingester
            .push_borrowed_to_storage(record.tenant, &record.write_request, 0)
        {
            Ok(()) => Ok(()),
            Err(err) => {
                // Soft/client data errors (4xx / BadData / TenantLimit) are already recorded
                // and must NOT block Kafka partition consumption; only fatal/internal/TSDB
                // unavailability errors retry.
                if err.is_soft()
                    || matches!(
                        err.error_cause(),
                        mimir_proto::cortexpb::ErrorCause::BadData
                            | mimir_proto::cortexpb::ErrorCause::TenantLimit
                    )
                {
                    Ok(())
                } else {
                    error!(
                        tenant = record.tenant,
                        offset = record.offset,
                        error = %err,
                        "fatal error pushing Kafka record to ingester"
                    );
                    Err(err.to_string())
                }
            }
        }
    }
}

pub async fn start_ingest_reader(ingester: Arc<Ingester>) -> Result<Arc<IngestReaderHandle>, ReaderError> {
    let offset_file_path =
        PathBuf::from(&ingester.blocks_cfg.tsdb.dir).join("kafka-offset.json");
    let consumer: Arc<dyn RecordBatchConsumer> =
        Arc::new(ZeroCopyBatchConsumer::new(IngesterZeroCopyHandler {
            ingester: Arc::clone(&ingester),
        }));

    let reader = Arc::new(SingleClusterPartitionReader::new(
        ingester.ingest_cfg.kafka.clone(),
        ingester.ingest_partition_id,
        &ingester.cfg.ring.instance_id,
        Some(offset_file_path),
        consumer,
        ingester.registry.as_ref(),
    )?);

    reader.start().await?;
    info!(
        partition = ingester.ingest_partition_id,
        topic = %ingester.ingest_cfg.kafka.topic,
        "ingest storage partition reader started"
    );
    Ok(Arc::new(IngestReaderHandle::Single(reader)))
}

/// Delegate resetting the read reactive limiter when this ingester's partition transitions to ACTIVE.
pub struct IngesterPartitionRingDelegate {
    ingester: Arc<Ingester>,
}

impl IngesterPartitionRingDelegate {
    pub fn new(ingester: Arc<Ingester>) -> Arc<Self> {
        Arc::new(Self { ingester })
    }
}

impl PartitionRingWatcherDelegate for IngesterPartitionRingDelegate {
    fn on_partition_ring_changed(&self, old: &PartitionRingDesc, new: &PartitionRingDesc) {
        let pid = self.ingester.ingest_partition_id;
        if let (Some(op), Some(np)) = (old.partitions.get(&pid), new.partitions.get(&pid)) {
            if op.state != np.state
                && np.state == mimir_proto::ringpb::PartitionState::PartitionActive as i32
            {
                if let Some(ref rl) = self.ingester.reactive_limiter.read {
                    rl.reset();
                }
            }
        }
    }
}
