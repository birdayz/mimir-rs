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

pub type GetPartitionIDsFn =
    Arc<dyn Fn() -> Pin<Box<dyn Future<Output = Result<Vec<i32>, String>> + Send>> + Send + Sync>;

const STOPPED_ERR: &str = "partition offset reader is stopped";

struct OffsetPollPromise<O> {
    once: std::sync::OnceLock<Result<O, String>>,
    tx: watch::Sender<Option<Result<O, String>>>,
}

impl<O: Clone + Send + Sync + 'static> OffsetPollPromise<O> {
    fn new() -> Arc<Self> {
        let (tx, _) = watch::channel(None);
        Arc::new(Self {
            once: std::sync::OnceLock::new(),
            tx,
        })
    }

    fn notify(&self, res: Result<O, String>) {
        if self.once.set(res.clone()).is_ok() {
            let _ = self.tx.send(Some(res));
        }
    }

    async fn wait(&self) -> Result<O, String> {
        if let Some(res) = self.once.get() {
            return res.clone();
        }
        let mut rx = self.tx.subscribe();
        loop {
            if let Some(res) = self.once.get() {
                return res.clone();
            }
            if let Some(ref res) = *rx.borrow_and_update() {
                return res.clone();
            }
            if rx.changed().await.is_err() {
                return Err(STOPPED_ERR.to_string());
            }
        }
    }
}

async fn wait_stopped(stop_rx: &mut watch::Receiver<bool>) {
    loop {
        if *stop_rx.borrow_and_update() {
            return;
        }
        if stop_rx.changed().await.is_err() {
            return;
        }
    }
}

/// Generic periodic offset poller supporting `wait_next_fetch_last_produced_offset` and `cached_offset`.
pub struct GenericOffsetReader<O: Clone + Send + Sync + 'static> {
    fetch_fn: FetchOffsetsFn<O>,
    poll_interval: Duration,
    last_result: Arc<RwLock<Option<Result<O, String>>>>,
    next_result_promise: Arc<parking_lot::Mutex<Arc<OffsetPollPromise<O>>>>,
    stop_tx: watch::Sender<bool>,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl<O: Clone + Send + Sync + 'static> GenericOffsetReader<O> {
    pub fn new(fetch_fn: FetchOffsetsFn<O>, poll_interval: Duration) -> Self {
        let (stop_tx, _) = watch::channel(false);
        Self {
            fetch_fn,
            poll_interval: poll_interval.max(Duration::from_millis(10)),
            last_result: Arc::new(RwLock::new(None)),
            next_result_promise: Arc::new(parking_lot::Mutex::new(OffsetPollPromise::new())),
            stop_tx,
            task: Mutex::new(None),
        }
    }

    async fn poll_and_notify(
        fetch_fn: &FetchOffsetsFn<O>,
        last_result: &Arc<RwLock<Option<Result<O, String>>>>,
        next_result_promise: &Arc<parking_lot::Mutex<Arc<OffsetPollPromise<O>>>>,
        stop_rx: &mut watch::Receiver<bool>,
    ) -> bool {
        let promise = {
            let mut guard = next_result_promise.lock();
            if *stop_rx.borrow() {
                guard.notify(Err(STOPPED_ERR.to_string()));
                return false;
            }
            std::mem::replace(&mut *guard, OffsetPollPromise::new())
        };

        let res = tokio::select! {
            biased;
            _ = wait_stopped(stop_rx) => {
                promise.notify(Err(STOPPED_ERR.to_string()));
                return false;
            }
            r = (fetch_fn)() => r,
        };

        if let Err(ref e) = res {
            warn!(err = %e, "failed to fetch the last produced offset");
        }
        *last_result.write() = Some(res.clone());
        promise.notify(res);
        true
    }

    pub async fn start(&self) {
        let mut guard = self.task.lock().await;
        if guard.is_some() {
            return;
        }
        let mut stop_rx = self.stop_tx.subscribe();
        if *stop_rx.borrow() {
            return;
        }
        // Perform initial poll synchronously at startup to populate cached_offset.
        if !Self::poll_and_notify(
            &self.fetch_fn,
            &self.last_result,
            &self.next_result_promise,
            &mut stop_rx,
        )
        .await
        {
            return;
        }

        let fetch_fn = Arc::clone(&self.fetch_fn);
        let poll_interval = self.poll_interval;
        let last_result = Arc::clone(&self.last_result);
        let next_result_promise = Arc::clone(&self.next_result_promise);

        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(poll_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            interval.tick().await; // skip immediate first tick since we already polled above
            loop {
                tokio::select! {
                    biased;
                    _ = wait_stopped(&mut stop_rx) => {
                        break;
                    }
                    _ = interval.tick() => {
                        if !Self::poll_and_notify(
                            &fetch_fn,
                            &last_result,
                            &next_result_promise,
                            &mut stop_rx,
                        )
                        .await
                        {
                            break;
                        }
                    }
                }
            }
        });
        *guard = Some(handle);
    }

    pub async fn stop(&self) {
        let _ = self.stop_tx.send(true);
        self.next_result_promise
            .lock()
            .notify(Err(STOPPED_ERR.to_string()));
        if let Some(handle) = self.task.lock().await.take() {
            let _ = handle.await;
        }
    }

    /// Waits for the *next* poll cycle to be issued and complete, and returns its result.
    pub async fn wait_next_fetch_last_produced_offset(&self) -> Result<O, String> {
        let mut stop_rx = self.stop_tx.subscribe();
        if *stop_rx.borrow() {
            return Err(STOPPED_ERR.to_string());
        }
        let promise = Arc::clone(&*self.next_result_promise.lock());
        tokio::select! {
            biased;
            _ = wait_stopped(&mut stop_rx) => Err(STOPPED_ERR.to_string()),
            res = promise.wait() => res,
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
        let c = client.clone();
        let get_pids: GetPartitionIDsFn = Arc::new(move || {
            let c = c.clone();
            let t = t.clone();
            Box::pin(async move {
                c.list_topic_partition_ids(&t)
                    .await
                    .map_err(|e| e.to_string())
            })
        });
        Self::with_partition_ids(client, topic, get_pids, poll_interval)
    }

    pub fn with_partition_ids(
        client: PartitionOffsetClient,
        topic: impl Into<String>,
        get_partition_ids: GetPartitionIDsFn,
        poll_interval: Duration,
    ) -> Self {
        let topic = topic.into();
        let t = topic.clone();
        let c = client;
        let fetch_fn: FetchOffsetsFn<HashMap<i32, i64>> = Arc::new(move || {
            let c = c.clone();
            let t = t.clone();
            let get_pids = Arc::clone(&get_partition_ids);
            Box::pin(async move {
                let pids = (get_pids)()
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
    clients: Vec<PartitionOffsetClient>,
    close_on_stop: bool,
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
        let c0 = clients[0].clone();
        let get_pids: Vec<GetPartitionIDsFn> = topics
            .iter()
            .map(|t| {
                let c = c0.clone();
                let t = t.clone();
                let f: GetPartitionIDsFn = Arc::new(move || {
                    let c = c.clone();
                    let t = t.clone();
                    Box::pin(async move {
                        c.list_topic_partition_ids(&t)
                            .await
                            .map_err(|e| e.to_string())
                    })
                });
                f
            })
            .collect();
        Self::with_partition_ids(clients, topics, get_pids, poll_interval, false)
    }

    pub fn with_partition_ids(
        clients: Vec<PartitionOffsetClient>,
        topics: Vec<String>,
        get_partition_ids: Vec<GetPartitionIDsFn>,
        poll_interval: Duration,
        close_on_stop: bool,
    ) -> Result<Self, String> {
        if clients.is_empty() {
            return Err("at least one Kafka cluster must be configured".to_string());
        }
        if topics.is_empty() {
            return Err("at least one topic must be configured".to_string());
        }
        if topics.len() != get_partition_ids.len() {
            return Err(format!(
                "the number of topics ({}) and partition ID functions ({}) must match",
                topics.len(),
                get_partition_ids.len()
            ));
        }

        let topics_cloned = topics.clone();
        let clients_cloned = clients.clone();
        let fetch_fn: FetchOffsetsFn<TopicsPartitionsOffsets> = Arc::new(move || {
            let clients = clients_cloned.clone();
            let topics = topics_cloned.clone();
            let get_pids = get_partition_ids.clone();
            Box::pin(async move {
                let mut partitions_by_topic = HashMap::with_capacity(topics.len());
                for (t, f) in topics.iter().zip(get_pids.iter()) {
                    let pids = (f)()
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
            clients,
            close_on_stop,
            generic: GenericOffsetReader::new(fetch_fn, poll_interval),
        })
    }

    pub fn clients(&self) -> &[PartitionOffsetClient] {
        &self.clients
    }

    pub fn topics(&self) -> &[String] {
        &self.topics
    }

    pub async fn start(&self) {
        self.generic.start().await;
    }

    pub async fn stop(&self) {
        self.generic.stop().await;
        if self.close_on_stop {
            for c in &self.clients {
                c.close();
            }
        }
    }

    pub async fn wait_next_fetch_last_produced_offset(
        &self,
    ) -> Result<TopicsPartitionsOffsets, String> {
        self.generic.wait_next_fetch_last_produced_offset().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Notify;

    #[tokio::test]
    async fn w23_csc_09_per_poll_promise_isolates_ok_from_subsequent_err() {
        let poll_num = Arc::new(AtomicUsize::new(0));
        let poll1_done = Arc::new(Notify::new());
        let poll2_done = Arc::new(Notify::new());

        let pn = Arc::clone(&poll_num);
        let p1d = Arc::clone(&poll1_done);
        let p2d = Arc::clone(&poll2_done);

        let fetch_fn: FetchOffsetsFn<i64> = Arc::new(move || {
            let n = pn.fetch_add(1, Ordering::SeqCst);
            let p1d = Arc::clone(&p1d);
            let p2d = Arc::clone(&p2d);
            Box::pin(async move {
                match n {
                    0 => Ok(10),
                    1 => {
                        p1d.notify_waiters();
                        Ok(42)
                    }
                    _ => {
                        p2d.notify_waiters();
                        Err("transient broker error".to_string())
                    }
                }
            })
        });

        let reader = Arc::new(GenericOffsetReader::new(
            fetch_fn,
            Duration::from_millis(20),
        ));
        reader.start().await;
        assert_eq!(reader.cached_offset(), Some(Ok(10)));

        // Subscribe to poll 1's promise before poll 1 runs.
        let promise_1 = Arc::clone(&*reader.next_result_promise.lock());

        // Wait for poll 1 (Ok(42)) AND poll 2 (Err("transient broker error")) to BOTH complete
        // before awaiting promise_1.
        poll1_done.notified().await;
        poll2_done.notified().await;
        while !matches!(reader.cached_offset(), Some(Err(_))) {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }

        // Poll 1's promise must still yield Ok(42), never overwritten by poll 2's Err.
        assert_eq!(promise_1.wait().await, Ok(42));
        reader.stop().await;
    }

    #[tokio::test]
    async fn w23_csc_09_stop_cancels_hanging_fetch_immediately() {
        let poll_num = Arc::new(AtomicUsize::new(0));
        let entered_poll_1 = Arc::new(Notify::new());

        let pn = Arc::clone(&poll_num);
        let ep1 = Arc::clone(&entered_poll_1);
        let fetch_fn: FetchOffsetsFn<i64> = Arc::new(move || {
            let n = pn.fetch_add(1, Ordering::SeqCst);
            let ep1 = Arc::clone(&ep1);
            Box::pin(async move {
                if n == 0 {
                    Ok(7)
                } else {
                    ep1.notify_waiters();
                    std::future::pending::<Result<i64, String>>().await
                }
            })
        });

        let reader = Arc::new(GenericOffsetReader::new(
            fetch_fn,
            Duration::from_millis(10),
        ));
        reader.start().await;
        entered_poll_1.notified().await;

        let r_wait = Arc::clone(&reader);
        let waiter = tokio::spawn(async move { r_wait.wait_next_fetch_last_produced_offset().await });
        tokio::task::yield_now().await;

        tokio::time::timeout(Duration::from_millis(50), reader.stop())
            .await
            .expect("stop() must cancel hanging fetch_fn in <50ms");
        assert!(waiter.await.unwrap().is_err());
    }
}
