//! The partition reader's "Kafka client" fetch path, used when concurrent fetching is disabled
//! (`-ingest-storage.kafka.fetch-concurrency-max=0`): the reader consumes the partition through
//! the client (`AddConsumePartitions` + `PollFetches`) instead of `ConcurrentFetchers`.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/storage/ingest/reader.go (`start`: the `FetchConcurrencyMax == 0`
//!   branch, `SingleClusterPartitionReader` as `fetcher`)
//! - github.com/grafana/mimir/pkg/storage/ingest/reader_client.go (`NewKafkaReaderClient`:
//!   `FetchMinBytes(1)`, `FetchMaxBytes(100MB)`, `FetchMaxWait(cfg.FetchMaxWait)`,
//!   `FetchMaxPartitionBytes(50MB)`)
//! - github.com/grafana/mimir/pkg/storage/ingest/util.go (`commonKafkaClientOptions`: `kgo.Rack`,
//!   10s metadata refresh)
//! - github.com/twmb/franz-go/pkg/kgo/source.go (one partition cursor: `createReq` with the rack,
//!   `handleReqResp` preferred read replica handling, `cursorOffsetPreferred.move`,
//!   `RecheckPreferredReplicaInterval`, OFFSET_OUT_OF_RANGE handling incl. KIP-392 cases,
//!   retriable error stripping, `loopFetch` backoff, a single buffered fetch per source)
//! - github.com/twmb/franz-go/pkg/kgo/topics_and_partitions.go (`migrateCursorTo` on a
//!   leader / leader epoch change)
//!
//! This is the only fetch path that sends the client rack: a rack-aware broker (KIP-392:
//! Kafka `RackAwareReplicaSelector`, Redpanda `enable_rack_awareness`) then answers with a
//! `preferred_read_replica` and no records, and the cursor moves to that replica.
//!
//! Not ported: fetch sessions (KIP-227; every request is a full fetch, `session_epoch = -1`) and
//! KIP-320 leader epoch validation after a leader change (the cursor resumes at its offset).

use std::future::Future;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use kafka_protocol::error::{ParseResponseErrorCode, ResponseError};
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::{BrokerId, FetchRequest, TopicName};
use kafka_protocol::protocol::StrBytes;
use tokio::sync::{mpsc, watch, Mutex};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use super::fetcher::{kafka_fetch_error_from_client, FetchResult, KafkaFetchError, PartitionMetadataRefresher};
use super::kafka_client::{KafkaClient, KAFKA_OFFSET_END, KAFKA_OFFSET_START};
use super::kgo_retry::{is_retriable_response_error, retry_backoff, DEFAULT_REQUEST_TIMEOUT_OVERHEAD};

/// `NewKafkaReaderClient`: `kgo.FetchMaxBytes(fetchMaxBytes)`.
pub const CLIENT_FETCH_MAX_BYTES: i32 = 100_000_000;
/// `NewKafkaReaderClient`: `kgo.FetchMaxPartitionBytes(50_000_000)`.
pub const CLIENT_FETCH_MAX_PARTITION_BYTES: i32 = 50_000_000;
/// kgo default `RecheckPreferredReplicaInterval`.
pub const RECHECK_PREFERRED_REPLICA_INTERVAL: Duration = Duration::from_secs(30 * 60);
/// Highest Fetch version used: v11 introduced `rack_id` / `preferred_read_replica`, v12
/// `current_leader` (KIP-951). Topic IDs (v13) are not needed here.
const CLIENT_FETCH_MAX_VERSION: i16 = 12;

/// The consuming cursor of the single partition (kgo `cursor`).
struct Cursor {
    offset: i64,
    /// The broker fetched from (`cursor.source`): the leader or a preferred replica.
    source: i32,
    /// The leader and leader epoch the cursor was last migrated to.
    leader: i32,
    leader_epoch: i32,
    /// When the cursor moved to a preferred replica (`moveAt`).
    move_at: Option<Instant>,
    /// Timestamp of the last consumed record (`lastConsumedTime`), if any.
    last_consumed_ms: Option<i64>,
}

/// Fetches one partition through the Kafka client like kgo's consumer does.
pub struct ClientPartitionConsumer {
    client: Arc<KafkaClient>,
    refresher: Arc<PartitionMetadataRefresher>,
    topic: String,
    partition_id: i32,
    rack: String,
    fetch_max_wait: Duration,
    start_offset: i64,
    buffered_records: Arc<AtomicI64>,
    buffered_bytes: Arc<AtomicI64>,
    rx: Mutex<mpsc::Receiver<FetchResult>>,
    tx: Mutex<Option<mpsc::Sender<FetchResult>>>,
    stop_tx: watch::Sender<bool>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl ClientPartitionConsumer {
    /// `AddConsumePartitions({topic: {partition: kgo.NewOffset().At(start_offset)}})`.
    /// `start_offset` may be `KAFKA_OFFSET_START` / `KAFKA_OFFSET_END`.
    pub fn new(
        client: Arc<KafkaClient>,
        topic: impl Into<String>,
        partition_id: i32,
        start_offset: i64,
        fetch_max_wait: Duration,
    ) -> Self {
        let topic = topic.into();
        // One buffered fetch, like a kgo source: the next fetch is only issued once the
        // previous one has been polled.
        let (tx, rx) = mpsc::channel(1);
        let (stop_tx, _) = watch::channel(false);
        Self {
            refresher: PartitionMetadataRefresher::new(Arc::clone(&client), topic.clone()),
            rack: client.client_rack().to_string(),
            client,
            topic,
            partition_id,
            fetch_max_wait,
            start_offset,
            buffered_records: Arc::new(AtomicI64::new(0)),
            buffered_bytes: Arc::new(AtomicI64::new(0)),
            rx: Mutex::new(rx),
            tx: Mutex::new(Some(tx)),
            stop_tx,
            tasks: Mutex::new(Vec::new()),
        }
    }

    /// `client.BufferedFetchRecords()`.
    pub fn buffered_records(&self) -> i64 {
        self.buffered_records.load(Ordering::SeqCst)
    }

    /// `client.BufferedFetchBytes()`.
    pub fn buffered_bytes(&self) -> i64 {
        self.buffered_bytes.load(Ordering::SeqCst)
    }

    pub async fn start(&self) {
        let Some(tx) = self.tx.lock().await.take() else {
            return;
        };
        let mut tasks = self.tasks.lock().await;
        tasks.push(self.refresher.spawn_periodic(self.stop_tx.subscribe()));
        let worker = Worker {
            client: Arc::clone(&self.client),
            refresher: Arc::clone(&self.refresher),
            topic: self.topic.clone(),
            partition_id: self.partition_id,
            rack: self.rack.clone(),
            fetch_max_wait: self.fetch_max_wait,
            buffered_records: Arc::clone(&self.buffered_records),
            buffered_bytes: Arc::clone(&self.buffered_bytes),
            stop_rx: self.stop_tx.subscribe(),
        };
        let start_offset = self.start_offset;
        tasks.push(tokio::spawn(async move { worker.run(start_offset, tx).await }));
    }

    /// Stops fetching; buffered records are dropped (the client is closed).
    pub async fn stop(&self) {
        let _ = self.stop_tx.send(true);
        let tasks: Vec<_> = self.tasks.lock().await.drain(..).collect();
        for t in tasks {
            let _ = t.await;
        }
        self.set_buffered(0, 0);
    }

    fn set_buffered(&self, records: i64, bytes: i64) {
        self.buffered_records.store(records, Ordering::SeqCst);
        self.buffered_bytes.store(bytes, Ordering::SeqCst);
        if let Some(k) = self.client.kprom() {
            k.set_buffered_fetch(records, bytes);
        }
    }

    /// `client.PollFetches(ctx)`: blocks until a fetch with records or an error is buffered.
    /// Returns `None` once `cancel` completes first.
    pub async fn poll_fetches_until<F: Future>(&self, cancel: F) -> Option<FetchResult> {
        let mut rx = self.rx.lock().await;
        let res = tokio::select! {
            biased;
            res = rx.recv() => res,
            _ = cancel => None,
        };
        if res.is_some() {
            self.set_buffered(0, 0);
        }
        res
    }
}

struct Worker {
    client: Arc<KafkaClient>,
    refresher: Arc<PartitionMetadataRefresher>,
    topic: String,
    partition_id: i32,
    rack: String,
    fetch_max_wait: Duration,
    buffered_records: Arc<AtomicI64>,
    buffered_bytes: Arc<AtomicI64>,
    stop_rx: watch::Receiver<bool>,
}

enum Step {
    /// Fetch again right away.
    Continue,
    /// Back off (`source.loopFetch` backoff: kgo retry backoff of the consecutive failures).
    Backoff,
    /// Buffer this fetch for `PollFetches`.
    Buffer(FetchResult),
}

impl Worker {
    fn stopped(&self) -> bool {
        self.stop_rx.has_changed().is_err() || *self.stop_rx.borrow()
    }

    /// Sleeps `d` unless stopped first; returns `false` if stopped.
    async fn sleep(&mut self, d: Duration) -> bool {
        tokio::select! {
            res = self.stop_rx.changed() => res.is_ok() && !self.stopped(),
            _ = tokio::time::sleep(d) => true,
        }
    }

    /// Resolves the special start offsets like kgo's offset listing (retried until stopped).
    async fn resolve_start(&mut self, start_offset: i64) -> Option<i64> {
        let mut fails = 0u32;
        loop {
            if self.stopped() {
                return None;
            }
            let res = match start_offset {
                KAFKA_OFFSET_START => self
                    .client
                    .fetch_partition_start_offset(&self.topic, self.partition_id)
                    .await,
                KAFKA_OFFSET_END => self
                    .client
                    .list_offset(&self.topic, self.partition_id, KAFKA_OFFSET_END)
                    .await,
                o => return Some(o.max(0)),
            };
            match res {
                Ok(o) => return Some(o),
                Err(e) => {
                    fails += 1;
                    warn!(partition = self.partition_id, err = %e, "unable to list offsets to start consuming from; retrying");
                    if !self.sleep(retry_backoff(fails)).await {
                        return None;
                    }
                }
            }
        }
    }

    async fn run(mut self, start_offset: i64, tx: mpsc::Sender<FetchResult>) {
        let Some(offset) = self.resolve_start(start_offset).await else {
            return;
        };
        let mut cursor = Cursor {
            offset,
            source: -1,
            leader: -1,
            leader_epoch: -1,
            move_at: None,
            last_consumed_ms: None,
        };
        let mut consecutive_failures = 0u32;

        loop {
            // A source only fetches again once its buffered fetch has been drained.
            let permit = tokio::select! {
                res = self.stop_rx.changed() => {
                    if res.is_err() || self.stopped() { return; }
                    continue;
                }
                p = tx.reserve() => match p {
                    Ok(p) => p,
                    Err(_) => return,
                },
            };
            if self.stopped() {
                return;
            }

            let mut stop_rx = self.stop_rx.clone();
            let step = tokio::select! {
                _ = stop_rx.wait_for(|v| *v) => return,
                step = self.fetch_once(&mut cursor) => step,
            };
            match step {
                Step::Continue => {
                    consecutive_failures = 0;
                }
                Step::Backoff => {
                    drop(permit);
                    consecutive_failures += 1;
                    if !self.sleep(retry_backoff(consecutive_failures)).await {
                        return;
                    }
                }
                Step::Buffer(fetch) => {
                    consecutive_failures = 0;
                    let records = fetch.records.len() as i64;
                    // kgo `Record.userSize`: key + value + header keys and values.
                    let bytes: i64 = fetch
                        .records
                        .iter()
                        .map(|r| {
                            (r.key.len()
                                + r.value.len()
                                + r.headers.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>())
                                as i64
                        })
                        .sum();
                    self.buffered_records.store(records, Ordering::SeqCst);
                    self.buffered_bytes.store(bytes, Ordering::SeqCst);
                    if let Some(k) = self.client.kprom() {
                        k.set_buffered_fetch(records, bytes);
                    }
                    permit.send(fetch);
                }
            }
        }
    }

    /// Moves the cursor to the current leader if the leader or its epoch changed
    /// (`migrateCursorTo`), or back from a preferred replica after
    /// `RecheckPreferredReplicaInterval`. Returns `false` if the leader is unknown.
    fn update_source(&mut self, cursor: &mut Cursor) -> bool {
        let (leader, epoch) = self.client.cached_partition_leader(&self.topic, self.partition_id);
        if leader < 0 {
            return false;
        }
        if leader != cursor.leader || epoch != cursor.leader_epoch {
            if cursor.leader >= 0 {
                info!(
                    topic = %self.topic,
                    partition = self.partition_id,
                    new_leader = leader,
                    new_leader_epoch = epoch,
                    old_leader = cursor.leader,
                    old_leader_epoch = cursor.leader_epoch,
                    "moving consuming partition to the new leader"
                );
            }
            cursor.leader = leader;
            cursor.leader_epoch = epoch;
            cursor.source = leader;
            cursor.move_at = None;
        }
        if cursor.source != cursor.leader
            && cursor
                .move_at
                .is_some_and(|t| t.elapsed() > RECHECK_PREFERRED_REPLICA_INTERVAL)
        {
            info!(
                from_broker = cursor.source,
                partition = self.partition_id,
                "redirecting follower fetchers back to their leader to re-check if a new follower should be chosen"
            );
            cursor.source = cursor.leader;
            cursor.move_at = None;
        }
        true
    }

    /// `cursorOffsetPreferred.move`: moves to `replica` if it is a known broker, otherwise
    /// forces a metadata update and keeps fetching from the current source.
    /// Returns whether the cursor moved.
    fn move_to(&self, cursor: &mut Cursor, replica: i32, recheck_leader: bool) -> bool {
        if !self.client.is_known_broker(replica) {
            return false;
        }
        cursor.source = replica;
        cursor.move_at = if recheck_leader || replica == cursor.leader {
            None
        } else {
            Some(Instant::now())
        };
        true
    }

    async fn fetch_once(&mut self, cursor: &mut Cursor) -> Step {
        if !self.update_source(cursor) {
            // No leader known yet: load metadata (kgo keeps the partition unassigned to a
            // source until metadata knows its leader).
            if let Err(e) = self.client.refresh_metadata(&[self.topic.as_str()]).await {
                debug!(topic = %self.topic, err = %e, "metadata load failed");
            }
            if !self.update_source(cursor) {
                return Step::Backoff;
            }
        }

        let mut fp = FetchPartition::default();
        fp.partition = self.partition_id;
        fp.current_leader_epoch = cursor.leader_epoch;
        fp.fetch_offset = cursor.offset;
        fp.last_fetched_epoch = -1;
        fp.log_start_offset = -1;
        fp.partition_max_bytes = CLIENT_FETCH_MAX_PARTITION_BYTES;

        let mut ft = FetchTopic::default();
        ft.topic = TopicName(StrBytes::from_string(self.topic.clone()));
        ft.partitions = vec![fp];

        let mut req = FetchRequest::default();
        req.replica_id = BrokerId(-1);
        req.max_wait_ms = self.fetch_max_wait.as_millis().min(i32::MAX as u128) as i32;
        req.min_bytes = 1;
        req.max_bytes = CLIENT_FETCH_MAX_BYTES;
        req.isolation_level = 0;
        req.session_id = 0;
        req.session_epoch = -1;
        req.topics = vec![ft];
        if !self.rack.is_empty() {
            req.rack_id = StrBytes::from_string(self.rack.clone());
        }

        let timeout = self.fetch_max_wait + DEFAULT_REQUEST_TIMEOUT_OVERHEAD;
        let source = cursor.source;
        let (resp, version) = match self
            .client
            .fetch_from_broker(source, &req, 4, CLIENT_FETCH_MAX_VERSION, timeout)
            .await
        {
            Ok(v) => v,
            Err(e) => {
                // Request-level failure: back off, opportunistically reload metadata. A replica
                // that is gone sends us back to the leader.
                debug!(broker = source, partition = self.partition_id, err = %e, "fetch request failed; backing off");
                self.refresher.trigger();
                if source != cursor.leader {
                    cursor.source = cursor.leader;
                    cursor.move_at = None;
                }
                return Step::Backoff;
            }
        };

        if let Some(e) = resp.error_code.err() {
            warn!(broker = source, err = ?e, "fetch response has unexpected top-level error, resetting session and backing off");
            return Step::Backoff;
        }

        let Some(rp) = resp
            .responses
            .iter()
            .filter(|t| t.topic.0.as_str() == self.topic)
            .flat_map(|t| t.partitions.iter())
            .find(|p| p.partition_index == self.partition_id)
        else {
            // The broker did not reply about our partition.
            return Step::Backoff;
        };

        // If we are fetching from the replica already, Kafka replies with a -1 preferred read
        // replica. If Kafka replies with a preferred replica, it sends no records.
        let preferred = rp.preferred_read_replica.0;
        if version >= 11 && preferred >= 0 {
            if preferred != cursor.source {
                info!(
                    from_broker = source,
                    topic = %self.topic,
                    partition = self.partition_id,
                    preferred_replica = preferred,
                    offset = cursor.offset,
                    "fetch partitions returned preferred replicas"
                );
                if self.move_to(cursor, preferred, false) {
                    return Step::Continue;
                }
                // A broker not yet known from metadata: update it now, keep fetching from the
                // current source until the preferred replica can be honored.
                if let Err(e) = self.client.refresh_metadata(&[self.topic.as_str()]).await {
                    debug!(topic = %self.topic, err = %e, "metadata refresh failed");
                }
                if self.move_to(cursor, preferred, false) {
                    return Step::Continue;
                }
            }
            return Step::Backoff;
        }

        if let Some(err) = rp.error_code.err() {
            return self.handle_partition_error(cursor, err, rp).await;
        }

        let raw = rp.records.clone().unwrap_or_default();
        let (records, next_offset) = if raw.is_empty() {
            (Vec::new(), cursor.offset)
        } else {
            match self.client.decode_fetched_partition(raw, cursor.offset) {
                Ok((records, next, _)) => (records, next),
                Err(e) => {
                    return Step::Buffer(FetchResult::error(
                        self.partition_id,
                        Some(KafkaFetchError::Other(format!("decode batch: {e}"))),
                    ));
                }
            }
        };
        // Keep offset advancements even without records (e.g. only control records).
        cursor.offset = next_offset.max(cursor.offset);
        if records.is_empty() {
            return Step::Continue;
        }
        cursor.last_consumed_ms = records.last().map(|r| r.timestamp_ms);
        Step::Buffer(FetchResult {
            partition: self.partition_id,
            records,
            high_watermark: rp.high_watermark,
            log_start_offset: rp.log_start_offset,
            fetched_bytes: 0,
            err: None,
        })
    }

    async fn handle_partition_error(
        &mut self,
        cursor: &mut Cursor,
        err: ResponseError,
        rp: &kafka_protocol::messages::fetch_response::PartitionData,
    ) -> Step {
        let source = cursor.source;
        if err == ResponseError::OffsetOutOfRange {
            if source == cursor.leader {
                // Non KIP-392 case: reset the offset.
                return match self.reset_offset(cursor).await {
                    Ok(()) => Step::Continue,
                    Err(()) => Step::Backoff,
                };
            }
            if cursor.offset < rp.log_start_offset {
                // KIP-392 case 3: go back to the leader, which has the offset or replies
                // OFFSET_OUT_OF_RANGE itself.
                let leader = cursor.leader;
                self.move_to(cursor, leader, true);
                return Step::Continue;
            }
            // KIP-392 case 4: the follower has not caught up yet; retry it.
            return Step::Backoff;
        }

        if is_retriable_response_error(err) {
            // Stripped from the fetch (not returned to the user): update metadata and back off.
            // KIP-951: a NOT_LEADER / FENCED reply may carry the current leader.
            let cl = &rp.current_leader;
            if matches!(
                err,
                ResponseError::NotLeaderOrFollower | ResponseError::FencedLeaderEpoch
            ) && cl.leader_id.0 >= 0
                && cl.leader_epoch >= cursor.leader_epoch
                && self.client.is_known_broker(cl.leader_id.0)
            {
                cursor.source = cl.leader_id.0;
                cursor.move_at = None;
            } else if source != cursor.leader {
                cursor.source = cursor.leader;
                cursor.move_at = None;
            }
            if err == ResponseError::UnknownTopicOrPartition {
                self.refresher.trigger();
            } else if let Err(e) = self.client.refresh_metadata(&[self.topic.as_str()]).await {
                debug!(topic = %self.topic, err = %e, "metadata refresh failed");
            }
            return Step::Backoff;
        }

        // Any other error is returned to the user with the fetch.
        Step::Buffer(FetchResult::error(
            self.partition_id,
            Some(kafka_fetch_error_from_client(
                super::kafka_client::KafkaClientError::Broker(err),
            )),
        ))
    }

    /// OFFSET_OUT_OF_RANGE from the leader: if records were consumed, resume from the last
    /// consumed timestamp, bounded within the log and never ahead of where we were; otherwise
    /// apply the reset offset (`ConsumeResetOffset` default: partition start).
    async fn reset_offset(&mut self, cursor: &mut Cursor) -> Result<(), ()> {
        let prior = cursor.offset;
        let res = match cursor.last_consumed_ms {
            Some(ts) => {
                let start_fut = self
                    .client
                    .fetch_partition_start_offset(&self.topic, self.partition_id);
                let end_fut = self.client.list_offset(
                    &self.topic,
                    self.partition_id,
                    KAFKA_OFFSET_END,
                );
                let by_time_fut =
                    self.client
                        .list_offset_with_timestamp(&self.topic, self.partition_id, ts);
                match tokio::try_join!(start_fut, end_fut, by_time_fut) {
                    Ok((start, end, by_time)) => {
                        let mut offset = if by_time.offset >= 0 && by_time.offset < prior {
                            by_time.offset
                        } else {
                            prior
                        };
                        offset = offset.max(start).min(end);
                        Ok(offset)
                    }
                    Err(e) => Err(e),
                }
            }
            None => {
                self.client
                    .fetch_partition_start_offset(&self.topic, self.partition_id)
                    .await
            }
        };
        match res {
            Ok(new_offset) => {
                if cursor.last_consumed_ms.is_some() {
                    warn!(
                        broker = cursor.source,
                        topic = %self.topic,
                        partition = self.partition_id,
                        prior_offset = prior,
                        "received OFFSET_OUT_OF_RANGE, resetting to the nearest offset; either you were consuming too slowly and the broker has deleted the segment you were in the middle of consuming, or the broker has lost data and has not yet transferred leadership"
                    );
                } else {
                    info!(
                        broker = cursor.source,
                        topic = %self.topic,
                        partition = self.partition_id,
                        prior_offset = prior,
                        "received OFFSET_OUT_OF_RANGE on the first fetch, resetting to the configured ConsumeResetOffset"
                    );
                }
                cursor.offset = new_offset;
                Ok(())
            }
            Err(e) => {
                debug!(partition = self.partition_id, err = %e, "unable to list offsets after OFFSET_OUT_OF_RANGE");
                Err(())
            }
        }
    }
}
