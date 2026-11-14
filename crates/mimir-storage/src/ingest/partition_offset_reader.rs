//! Periodic Kafka offset pollers (single-partition, single-cluster topic, and multi-cluster).
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/storage/ingest/partition_offset_reader.go
//! - github.com/grafana/mimir/pkg/storage/ingest/partition_offset_reader_single_cluster.go
//! - github.com/grafana/mimir/pkg/storage/ingest/partition_offset_reader_multi_cluster.go

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures::future::try_join_all;
use parking_lot::RwLock;
use tokio::sync::{watch, Mutex};
use tokio::task::JoinHandle;
use tracing::warn;

use super::kafka_client::KafkaClientError;
use super::offset_catalogue::{merge_topics_partitions_offsets, TopicsPartitionsOffsets};
use super::partition_offset_client::PartitionOffsetClient;

pub type FetchOffsetsFn<O> =
    Arc<dyn Fn() -> Pin<Box<dyn Future<Output = Result<O, String>> + Send>> + Send + Sync>;

type NextWaiterTx<O> = Arc<Mutex<watch::Sender<Option<(u64, Result<O, String>)>>>>;

/// Generic periodic offset poller supporting `wait_next_fetch_last_produced_offset` and `cached_offset`.
pub struct GenericOffsetReader<O: Clone + Send + Sync + 'static> {
    fetch_fn: FetchOffsetsFn<O>,
    poll_interval: Duration,
    last_result: Arc<RwLock<Option<Result<O, String>>>>,
    next_waiter_tx: NextWaiterTx<O>,
    seq: Arc<std::sync::atomic::AtomicU64>,
    stop_tx: watch::Sender<bool>,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl<O: Clone + Send + Sync + 'static> GenericOffsetReader<O> {
    pub fn new(fetch_fn: FetchOffsetsFn<O>, poll_interval: Duration) -> Self {
        let (next_waiter_tx, _) = watch::channel(None);
        let (stop_tx, _) = watch::channel(false);
        Self {
            fetch_fn,
            poll_interval: poll_interval.max(Duration::from_millis(10)),
            last_result: Arc::new(RwLock::new(None)),
            next_waiter_tx: Arc::new(Mutex::new(next_waiter_tx)),
            seq: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            stop_tx,
            task: Mutex::new(None),
        }
    }

    pub async fn start(&self) {
        let mut guard = self.task.lock().await;
        if guard.is_some() {
            return;
        }
        // Perform initial poll synchronously at startup to populate cached_offset.
        self.poll_once().await;

        let fetch_fn = Arc::clone(&self.fetch_fn);
        let poll_interval = self.poll_interval;
        let last_result = Arc::clone(&self.last_result);
        let next_waiter_tx = Arc::clone(&self.next_waiter_tx);
        let seq = Arc::clone(&self.seq);
        let mut stop_rx = self.stop_tx.subscribe();

        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(poll_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            interval.tick().await; // skip immediate first tick since we already polled above
            loop {
                tokio::select! {
                    _ = stop_rx.changed() => {
                        if *stop_rx.borrow() {
                            break;
                        }
                    }
                    _ = interval.tick() => {
                        let res = (fetch_fn)().await;
                        if let Err(ref e) = res {
                            warn!(error = %e, "failed to fetch the last produced offset");
                        }
                        *last_result.write() = Some(res.clone());
                        let next_seq = seq.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                        let tx = next_waiter_tx.lock().await;
                        let _ = tx.send(Some((next_seq, res)));
                    }
                }
            }
        });
        *guard = Some(handle);
    }

    async fn poll_once(&self) {
        let res = (self.fetch_fn)().await;
        if let Err(ref e) = res {
            warn!(error = %e, "failed to fetch the last produced offset");
        }
        *self.last_result.write() = Some(res.clone());
        let next_seq = self.seq.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
        let tx = self.next_waiter_tx.lock().await;
        let _ = tx.send(Some((next_seq, res)));
    }

    pub async fn stop(&self) {
        let _ = self.stop_tx.send(true);
        if let Some(handle) = self.task.lock().await.take() {
            let _ = handle.await;
        }
    }

    /// Waits for the *next* poll cycle to complete and returns its result.
    pub async fn wait_next_fetch_last_produced_offset(&self) -> Result<O, String> {
        let target_seq = self.seq.load(std::sync::atomic::Ordering::SeqCst) + 1;
        let mut rx = self.next_waiter_tx.lock().await.subscribe();
        let mut stop_rx = self.stop_tx.subscribe();

        loop {
            if let Some((s, ref res)) = *rx.borrow() {
                if s >= target_seq {
                    return res.clone();
                }
            }
            if *stop_rx.borrow() {
                return Err("partition offset reader is stopped".to_string());
            }
            tokio::select! {
                changed = rx.changed() => {
                    if changed.is_err() {
                        return Err("partition offset reader is stopped".to_string());
                    }
                }
                _ = stop_rx.changed() => {
                    if *stop_rx.borrow() {
                        return Err("partition offset reader is stopped".to_string());
                    }
                }
            }
        }
    }

    /// Returns the most recently cached poll result, if any.
    pub fn cached_offset(&self) -> Option<Result<O, String>> {
        self.last_result.read().clone()
    }
}

/// Single-partition offset reader wrapping `PartitionOffsetClient`.
pub struct SingleClusterPartitionOffsetReader {
    client: PartitionOffsetClient,
    topic: String,
    partition_id: i32,
    generic: GenericOffsetReader<i64>,
}

impl SingleClusterPartitionOffsetReader {
    pub fn new(
        client: PartitionOffsetClient,
        topic: impl Into<String>,
        partition_id: i32,
        poll_interval: Duration,
    ) -> Self {
        let topic = topic.into();
        let c = client.clone();
        let t = topic.clone();
        let fetch_fn: FetchOffsetsFn<i64> = Arc::new(move || {
            let c = c.clone();
            let t = t.clone();
            Box::pin(async move {
                c.fetch_partition_last_produced_offset(&t, partition_id)
                    .await
                    .map_err(|e| e.to_string())
            })
        });
        Self {
            client,
            topic,
            partition_id,
            generic: GenericOffsetReader::new(fetch_fn, poll_interval),
        }
    }

    pub async fn start(&self) {
        self.generic.start().await;
    }

    pub async fn stop(&self) {
        self.generic.stop().await;
    }

    pub async fn fetch_last_produced_offset(&self) -> Result<i64, KafkaClientError> {
        self.client
            .fetch_partition_last_produced_offset(&self.topic, self.partition_id)
            .await
    }

    pub async fn fetch_partition_start_offset(&self) -> Result<i64, KafkaClientError> {
        self.client
            .fetch_partition_start_offset(&self.topic, self.partition_id)
            .await
    }

    pub async fn wait_next_fetch_last_produced_offset(&self) -> Result<i64, String> {
        self.generic.wait_next_fetch_last_produced_offset().await
    }

    pub fn cached_offset(&self) -> Option<Result<i64, String>> {
        self.generic.cached_offset()
    }
}

/// Single-cluster topic offsets reader (all partitions of one topic).
pub struct SingleClusterTopicOffsetsReader {
    topic: String,
    generic: GenericOffsetReader<HashMap<i32, i64>>,
}

impl SingleClusterTopicOffsetsReader {
    pub fn new(client: PartitionOffsetClient, topic: impl Into<String>, poll_interval: Duration) -> Self {
        let topic = topic.into();
        let t = topic.clone();
        let c = client;
        let fetch_fn: FetchOffsetsFn<HashMap<i32, i64>> = Arc::new(move || {
            let c = c.clone();
            let t = t.clone();
            Box::pin(async move {
                let pids = c
                    .list_topic_partition_ids(&t)
                    .await
                    .map_err(|e| format!("listing partitions of topic {t:?}: {e}"))?;
                let mut req = HashMap::new();
                req.insert(t.clone(), pids);
                let mut res = c
                    .fetch_partitions_last_produced_offsets(&req)
                    .await
                    .map_err(|e| e.to_string())?;
                Ok(res.remove(&t).unwrap_or_default())
            })
        });
        Self {
            topic,
            generic: GenericOffsetReader::new(fetch_fn, poll_interval),
        }
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub async fn start(&self) {
        self.generic.start().await;
    }

    pub async fn stop(&self) {
        self.generic.stop().await;
    }

    pub async fn wait_next_fetch_last_produced_offset(
        &self,
    ) -> Result<HashMap<i32, i64>, String> {
        self.generic.wait_next_fetch_last_produced_offset().await
    }
}

/// Multi-cluster topic offsets reader (one `PartitionOffsetClient` per write compartment).
pub struct MultiClusterOffsetsReader {
    topics: Vec<String>,
    generic: GenericOffsetReader<TopicsPartitionsOffsets>,
}

impl MultiClusterOffsetsReader {
    pub fn new(
        clients: Vec<PartitionOffsetClient>,
        topics: Vec<String>,
        poll_interval: Duration,
    ) -> Result<Self, String> {
        if clients.is_empty() {
            return Err("at least one Kafka cluster must be configured".to_string());
        }
        if topics.is_empty() {
            return Err("at least one topic must be configured".to_string());
        }

        let topics_cloned = topics.clone();
        let fetch_fn: FetchOffsetsFn<TopicsPartitionsOffsets> = Arc::new(move || {
            let clients = clients.clone();
            let topics = topics_cloned.clone();
            Box::pin(async move {
                let mut partitions_by_topic = HashMap::with_capacity(topics.len());
                for t in &topics {
                    let pids = clients[0]
                        .list_topic_partition_ids(t)
                        .await
                        .map_err(|e| format!("listing partitions of topic {t:?}: {e}"))?;
                    partitions_by_topic.insert(t.clone(), pids);
                }

                let futs = clients.iter().enumerate().map(|(cid, c)| {
                    let req = partitions_by_topic.clone();
                    async move {
                        c.fetch_partitions_last_produced_offsets(&req)
                            .await
                            .map_err(|e| format!("write compartment {cid}: {e}"))
                    }
                });
                let per_cluster = try_join_all(futs).await?;
                Ok(merge_topics_partitions_offsets(&per_cluster))
            })
        });

        Ok(Self {
            topics,
            generic: GenericOffsetReader::new(fetch_fn, poll_interval),
        })
    }

    pub fn topics(&self) -> &[String] {
        &self.topics
    }

    pub async fn start(&self) {
        self.generic.start().await;
    }

    pub async fn stop(&self) {
        self.generic.stop().await;
    }

    pub async fn wait_next_fetch_last_produced_offset(
        &self,
    ) -> Result<TopicsPartitionsOffsets, String> {
        self.generic.wait_next_fetch_last_produced_offset().await
    }
}
