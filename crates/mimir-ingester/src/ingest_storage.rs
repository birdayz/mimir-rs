//! Kafka ingest_storage integration for the ingester.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/ingester/ingester.go (ingest_storage init)
//! - github.com/grafana/mimir/pkg/storage/ingest/util.go (IngesterPartitionID)

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use mimir_proto::cortexpb::ErrorCause;
use mimir_proto::ringpb::PartitionRingDesc;
use mimir_ring::partition_ring::PartitionRingWatcherDelegate;
use mimir_storage::ingest::{
    write_compartment_configs, LogPolicy, MultiClusterPartitionReader, PartitionOffsets,
    PreCommitNotifier, Pusher, PusherConsumer, PusherConsumerMetrics, PusherError, ReaderError,
    RecordBatchConsumer, SingleClusterPartitionReader, READ_COMPARTMENT_ID_PLACEHOLDER,
};
use tracing::info;

use crate::errors::IngesterError;
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

    pub async fn start(&self) -> Result<(), ReaderError> {
        match self {
            Self::Single(r) => r.start().await,
            Self::Multi(r) => r.start().await,
        }
    }

    pub async fn stop(&self) {
        match self {
            Self::Single(r) => r.stop().await,
            Self::Multi(r) => r.stop().await,
        }
    }
}

/// Adapts [`Ingester`] into [`mimir_storage::ingest::Pusher`] + [`PreCommitNotifier`].
pub struct IngesterStorageAdapter(pub Arc<Ingester>);

fn ingester_err_to_pusher_err(err: IngesterError) -> PusherError {
    let cause = err.error_cause();
    let grpc_code = match cause {
        ErrorCause::BadData => "InvalidArgument",
        ErrorCause::TenantLimit => "FailedPrecondition",
        ErrorCause::ServiceUnavailable
        | ErrorCause::InstanceLimit
        | ErrorCause::CircuitBreakerOpen => "Unavailable",
        ErrorCause::MethodNotAllowed => "Unimplemented",
        _ => "Internal",
    };
    let log_policy = if err.is_do_not_log() {
        Some(LogPolicy::DoNotLog)
    } else if err.is_optional_logging() {
        Some(LogPolicy::Sampled {
            should_log: err.should_log_error(),
            reason: String::new(),
        })
    } else {
        None
    };
    PusherError {
        message: err.to_string(),
        cause: Some(cause),
        grpc_code: Some(grpc_code.to_string()),
        log_policy,
    }
}

impl PreCommitNotifier for IngesterStorageAdapter {
    fn notify_pre_commit(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>> {
        Box::pin(async move { self.0.notify_pre_commit().map_err(|e| e.to_string()) })
    }
}

impl Pusher for IngesterStorageAdapter {
    fn push_to_storage_and_release_request(
        &self,
        tenant_id: &str,
        request: mimir_proto::cortexpb::WriteRequest,
    ) -> Pin<Box<dyn Future<Output = Result<(), PusherError>> + Send + '_>> {
        let tenant_id = tenant_id.to_owned();
        Box::pin(async move {
            if tenant_id.is_empty() {
                return Err(PusherError::ingester(
                    ErrorCause::BadData,
                    "InvalidArgument",
                    "no org id",
                ));
            }
            self.0
                .push_to_storage(&tenant_id, &request)
                .map_err(ingester_err_to_pusher_err)
        })
    }
}

pub async fn start_ingest_reader(ingester: Arc<Ingester>) -> Result<Arc<IngestReaderHandle>, ReaderError> {
    let tsdb_dir = PathBuf::from(&ingester.blocks_cfg.tsdb.dir);
    let adapter = Arc::new(IngesterStorageAdapter(Arc::clone(&ingester)));
    let pusher: Arc<dyn Pusher> = Arc::clone(&adapter) as Arc<dyn Pusher>;
    let notifier: Arc<dyn PreCommitNotifier> = adapter;

    let pusher_metrics = PusherConsumerMetrics::new(ingester.registry.as_ref());
    let mut pusher_consumer = PusherConsumer::new(
        pusher,
        ingester.ingest_cfg.kafka.clone(),
        pusher_metrics,
    );
    if ingester.cfg.error_sample_rate > 0 {
        pusher_consumer =
            pusher_consumer.with_fallback_client_error_sample_rate(ingester.cfg.error_sample_rate);
    }
    let consumer: Arc<dyn RecordBatchConsumer> = Arc::new(pusher_consumer);
    let max_replay_period = Duration::from_nanos(
        ingester
            .blocks_cfg
            .tsdb
            .retention_period
            .as_nanos()
            .max(0) as u64,
    );

    let compartments = ingester.compartments_config();
    let reader = if compartments.enabled {
        let rc_id = ingester.cfg.read_compartment_id.max(0) as usize;
        let topic = ingester
            .ingest_cfg
            .kafka
            .topic
            .replace(READ_COMPARTMENT_ID_PLACEHOLDER, &rc_id.to_string());
        let num_wc = compartments.write.num_compartments.max(1) as usize;
        let cluster_configs =
            write_compartment_configs(&ingester.ingest_cfg.kafka, num_wc, &topic);
        let offset_paths = (0..cluster_configs.len())
            .map(|w| Some(tsdb_dir.join(format!("kafka-offset-wc-{w}.json"))))
            .collect();
        let multi = MultiClusterPartitionReader::new_with_notifier(
            &cluster_configs,
            &ingester.ingest_cfg.ordered_consumption,
            ingester.ingest_partition_id,
            &ingester.cfg.ring.instance_id,
            offset_paths,
            consumer,
            notifier,
            max_replay_period,
            ingester.registry.as_ref(),
        )?;
        Arc::new(IngestReaderHandle::Multi(Arc::new(multi)))
    } else {
        let offset_file_path = tsdb_dir.join("kafka-offset.json");
        let mut single = SingleClusterPartitionReader::new_with_notifier(
            ingester.ingest_cfg.kafka.clone(),
            ingester.ingest_partition_id,
            &ingester.cfg.ring.instance_id,
            Some(offset_file_path),
            consumer,
            notifier,
            ingester.registry.as_ref(),
        )?;
        if !max_replay_period.is_zero() {
            single = single.with_max_replay_period(max_replay_period);
        }
        Arc::new(IngestReaderHandle::Single(Arc::new(single)))
    };

    // Store handle before awaiting startup catchup so stop() and push readiness checks see it.
    *ingester.ingest_reader.write() = Some(Arc::clone(&reader));
    if let Err(e) = reader.start().await {
        reader.stop().await;
        *ingester.ingest_reader.write() = None;
        return Err(e);
    }

    info!(
        partition = ingester.ingest_partition_id,
        topic = %ingester.ingest_cfg.kafka.topic,
        compartments_enabled = compartments.enabled,
        "ingest storage partition reader started"
    );
    Ok(reader)
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

#[cfg(test)]
mod tests {
    use super::*;
    use mimir_common::services::State;

    #[test]
    fn error_mapping_distinguishes_client_and_server_errors() {
        let bad_data = IngesterError::sample_out_of_order(100, "up{job=\"a\"}".into());
        let bad_pusher = ingester_err_to_pusher_err(bad_data);
        assert!(bad_pusher.is_client_error());

        let tenant_limit = IngesterError::per_user_series_limit_reached(100);
        let limit_pusher = ingester_err_to_pusher_err(tenant_limit);
        assert!(limit_pusher.is_client_error());

        let tsdb_unavail = IngesterError::tsdb_closing();
        assert!(!ingester_err_to_pusher_err(tsdb_unavail).is_client_error());

        let unavail = IngesterError::Unavailable {
            state: State::Starting,
        };
        assert!(!ingester_err_to_pusher_err(unavail).is_client_error());

        let inst_limit = IngesterError::max_in_memory_series_reached();
        let inst_pusher = ingester_err_to_pusher_err(inst_limit);
        assert!(!inst_pusher.is_client_error());
        assert_eq!(inst_pusher.log_policy, Some(LogPolicy::DoNotLog));
    }
}
