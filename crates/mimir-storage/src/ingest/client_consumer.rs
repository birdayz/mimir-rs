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
use kafka_protocol::messages::fetch_response::NodeEndpoint;
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::{BrokerId, FetchRequest, TopicName};
use kafka_protocol::protocol::StrBytes;
use tokio::sync::{mpsc, watch, Mutex};
use tokio::task::JoinHandle;
use mimir_common::native_histogram::SingleHistogram;
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
/// `current_leader`, v13 topic IDs, and v16 KIP-951 `node_endpoints`.
const CLIENT_FETCH_MAX_VERSION: i16 = 16;

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
    fetch_wait_duration: Option<SingleHistogram>,
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
            fetch_wait_duration: None,
            stop_tx,
            tasks: Mutex::new(Vec::new()),
        }
    }

    /// Attaches `cortex_ingest_storage_reader_records_batch_wait_duration_seconds` so
    /// `poll_fetches_until` observes fetch wait duration like Go `SingleClusterPartitionReader.PollFetches`.
    pub fn with_fetch_wait_duration(mut self, hist: SingleHistogram) -> Self {
        self.fetch_wait_duration = Some(hist);
        self
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
            unknown_id_fails: 0,
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
        let wait_start = Instant::now();
        let mut rx = self.rx.lock().await;
        let res = tokio::select! {
            biased;
            res = rx.recv() => res,
            _ = cancel => None,
        };
        if let Some(ref hist) = self.fetch_wait_duration {
            hist.observe(wait_start.elapsed().as_secs_f64());
        }
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
    unknown_id_fails: u32,
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

        let topic_id = self.client.cached_topic_id(&self.topic);
        let mut ft = FetchTopic::default();
        ft.topic = TopicName(StrBytes::from_string(self.topic.clone()));
        ft.topic_id = uuid::Uuid::from_bytes(topic_id);
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

        // Topic IDs are only usable with v13+; without a known topic ID stay on v12.
        let max_version = if topic_id == [0u8; 16] {
            12
        } else {
            CLIENT_FETCH_MAX_VERSION
        };
        let timeout = self.fetch_max_wait + DEFAULT_REQUEST_TIMEOUT_OVERHEAD;
        let source = cursor.source;
        let (resp, version) = match self
            .client
            .fetch_from_broker(source, &req, 4, max_version, timeout)
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
            .filter(|t| {
                if version >= 13 {
                    *t.topic_id.as_bytes() == topic_id
                } else {
                    t.topic.0.as_str() == self.topic
                }
            })
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
            if err != ResponseError::UnknownTopicId {
                self.unknown_id_fails = 0;
            }
            return self
                .handle_partition_error(cursor, err, rp, &resp.node_endpoints)
                .await;
        }
        self.unknown_id_fails = 0;

        let raw = rp.records.clone().unwrap_or_default();
        let raw_non_empty = !raw.is_empty();
        let prior_offset = cursor.offset;
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
            if raw_non_empty && cursor.offset <= prior_offset {
                // franz-go source.go:1314-1316 (errFetchNoProgress):
                // Broker returned batch bytes whose last offsets are all below our requested
                // offset; back off instead of hot-spinning at 100% CPU.
                return Step::Backoff;
            }
            return Step::Continue;
        }
        cursor.last_consumed_ms = records.last().map(|r| r.timestamp_ms);
        Step::Buffer(FetchResult {
            partition: self.partition_id,
            records,
            high_watermark: rp.high_watermark,
            log_start_offset: rp.log_start_offset,
            next_fetch_offset: cursor.offset,
            fetched_bytes: 0,
            err: None,
        })
    }

    async fn handle_partition_error(
        &mut self,
        cursor: &mut Cursor,
        err: ResponseError,
        rp: &kafka_protocol::messages::fetch_response::PartitionData,
        node_endpoints: &[NodeEndpoint],
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

        if err == ResponseError::UnknownTopicId {
            // franz-go source.go:1343-1363: strip UnknownTopicID for the first 5 consecutive
            // occurrences (allowing newly-created topics to propagate), then surface it.
            self.unknown_id_fails += 1;
            if self.unknown_id_fails > 5 {
                self.unknown_id_fails -= 1;
                return Step::Buffer(FetchResult::error(
                    self.partition_id,
                    Some(kafka_fetch_error_from_client(
                        super::kafka_client::KafkaClientError::Broker(err),
                    )),
                ));
            }
            if source != cursor.leader {
                cursor.source = cursor.leader;
                cursor.move_at = None;
            }
            if let Err(e) = self.client.refresh_metadata(&[self.topic.as_str()]).await {
                debug!(topic = %self.topic, err = %e, "metadata refresh failed");
            }
            return Step::Backoff;
        }

        if is_retriable_response_error(err) {
            // Stripped from the fetch (not returned to the user): update metadata and back off.
            // KIP-951 (topics_and_partitions.go:814-844): NOT_LEADER_OR_FOLLOWER may carry a
            // strictly newer CurrentLeader (+ NodeEndpoints in Fetch v16).
            let cl = &rp.current_leader;
            if err == ResponseError::NotLeaderOrFollower
                && cl.leader_id.0 >= 0
                && cl.leader_epoch >= 0
                && cl.leader_epoch > cursor.leader_epoch
                && self.client.apply_kip951_leader(
                    &self.topic,
                    self.partition_id,
                    cl.leader_id.0,
                    cl.leader_epoch,
                    node_endpoints,
                )
            {
                cursor.leader = cl.leader_id.0;
                cursor.leader_epoch = cl.leader_epoch;
                cursor.source = cl.leader_id.0;
                cursor.move_at = None;
                return Step::Backoff;
            }
            if source != cursor.leader {
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::{BufMut, Bytes, BytesMut};
    use kafka_protocol::indexmap::IndexMap;
    use kafka_protocol::messages::api_versions_response::ApiVersion;
    use kafka_protocol::messages::fetch_response::{
        FetchableTopicResponse, LeaderIdAndEpoch, NodeEndpoint, PartitionData,
    };
    use kafka_protocol::messages::metadata_response::{
        MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
    };
    use kafka_protocol::messages::{
        ApiKey, ApiVersionsRequest, ApiVersionsResponse, BrokerId, FetchRequest, FetchResponse,
        MetadataRequest, MetadataResponse, RequestHeader, ResponseHeader, TopicName,
    };
    use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};
    use kafka_protocol::records::{
        Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
        NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE,
    };
    use mimir_config::{KafkaConfig, StringSliceCSV};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;
    use crate::ingest::fetcher::FetcherMetrics;

    fn encode_msg<R: Encodable>(hv: i16, ver: i16, corr: i32, r: &R) -> Bytes {
        let mut hdr = ResponseHeader::default();
        hdr.correlation_id = corr;
        let mut out = BytesMut::new();
        out.put_u32(0);
        hdr.encode(&mut out, hv).unwrap();
        r.encode(&mut out, ver).unwrap();
        let len = (out.len() - 4) as u32;
        out[0..4].copy_from_slice(&len.to_be_bytes());
        out.freeze()
    }

    fn encode_batch_range(from: i64, to: i64) -> Bytes {
        let recs: Vec<Record> = (from..to)
            .map(|o| Record {
                transactional: false,
                control: false,
                partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
                producer_id: NO_PRODUCER_ID,
                producer_epoch: NO_PRODUCER_EPOCH,
                timestamp_type: TimestampType::Creation,
                offset: o,
                sequence: NO_SEQUENCE,
                timestamp: 1_700_000_000_000 + o,
                key: Some(Bytes::from_static(b"k")),
                value: Some(Bytes::from(format!("v-{o}"))),
                headers: IndexMap::new(),
            })
            .collect();
        let mut buf = BytesMut::new();
        RecordBatchEncoder::encode_with_custom_compression(
            &mut buf,
            recs.iter(),
            &RecordEncodeOptions {
                version: 2,
                compression: Compression::None,
            },
            Some(crate::ingest::compression::compress_record_batch),
        )
        .unwrap();
        buf.freeze()
    }

    #[tokio::test]
    async fn w24_b02_non_advancing_batch_backs_off_instead_of_hot_spinning() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let fetch_count = Arc::new(AtomicUsize::new(0));
        let fc = Arc::clone(&fetch_count);

        let srv = tokio::spawn(async move {
            loop {
                let Ok((mut s, _)) = listener.accept().await else {
                    break;
                };
                let fc = Arc::clone(&fc);
                tokio::spawn(async move {
                    loop {
                        let mut len = [0u8; 4];
                        if s.read_exact(&mut len).await.is_err() {
                            return;
                        }
                        let mut buf = vec![0u8; u32::from_be_bytes(len) as usize];
                        if s.read_exact(&mut buf).await.is_err() {
                            return;
                        }
                        let mut buf = Bytes::from(buf);
                        let key = ApiKey::try_from(i16::from_be_bytes([buf[0], buf[1]])).unwrap();
                        let ver = i16::from_be_bytes([buf[2], buf[3]]);
                        let hv = match key {
                            ApiKey::ApiVersions => ApiVersionsRequest::header_version(ver),
                            ApiKey::Metadata => MetadataRequest::header_version(ver),
                            ApiKey::Fetch => FetchRequest::header_version(ver),
                            _ => return,
                        };
                        let hdr = RequestHeader::decode(&mut buf, hv).unwrap();
                        let out = match key {
                            ApiKey::ApiVersions => {
                                let _ = ApiVersionsRequest::decode(&mut buf, ver);
                                let mut r = ApiVersionsResponse::default();
                                for (k, max) in [
                                    (ApiKey::ApiVersions, 3),
                                    (ApiKey::Metadata, 12),
                                    (ApiKey::Fetch, 12),
                                ] {
                                    let mut av = ApiVersion::default();
                                    av.api_key = k as i16;
                                    av.min_version = 0;
                                    av.max_version = max;
                                    r.api_keys.push(av);
                                }
                                encode_msg(0, ver, hdr.correlation_id, &r)
                            }
                            ApiKey::Metadata => {
                                let _ = MetadataRequest::decode(&mut buf, ver);
                                let mut b = MetadataResponseBroker::default();
                                b.node_id = BrokerId(0);
                                b.host = StrBytes::from_static_str("127.0.0.1");
                                b.port = addr.port() as i32;
                                let mut p = MetadataResponsePartition::default();
                                p.partition_index = 0;
                                p.leader_id = BrokerId(0);
                                p.leader_epoch = 1;
                                let mut t = MetadataResponseTopic::default();
                                t.name = Some(TopicName(StrBytes::from_static_str("no-progress")));
                                t.partitions = vec![p];
                                let mut r = MetadataResponse::default();
                                r.brokers = vec![b];
                                r.controller_id = BrokerId(0);
                                r.topics = vec![t];
                                encode_msg(MetadataResponse::header_version(ver), ver, hdr.correlation_id, &r)
                            }
                            ApiKey::Fetch => {
                                let _ = FetchRequest::decode(&mut buf, ver).unwrap();
                                let n = fc.fetch_add(1, Ordering::SeqCst);
                                let mut pd = PartitionData::default();
                                pd.partition_index = 0;
                                pd.high_watermark = 11;
                                pd.log_start_offset = 0;
                                // First 2 fetches return a non-empty batch strictly below offset 10 (no progress).
                                if n < 2 {
                                    pd.records = Some(encode_batch_range(0, 5));
                                } else {
                                    pd.records = Some(encode_batch_range(10, 11));
                                }
                                let mut ft = FetchableTopicResponse::default();
                                ft.topic = TopicName(StrBytes::from_static_str("no-progress"));
                                ft.partitions = vec![pd];
                                let mut r = FetchResponse::default();
                                r.responses = vec![ft];
                                encode_msg(FetchResponse::header_version(ver), ver, hdr.correlation_id, &r)
                            }
                            _ => unreachable!(),
                        };
                        if s.write_all(&out).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });

        let cfg = KafkaConfig {
            address: StringSliceCSV(vec![addr.to_string()]),
            topic: "no-progress".to_string(),
            ..Default::default()
        };
        let client = Arc::new(KafkaClient::from_config(&cfg).unwrap());
        let reg = prometheus::Registry::new();
        let fetcher_metrics = FetcherMetrics::new(Some(&reg));
        let consumer = ClientPartitionConsumer::new(
            Arc::clone(&client),
            "no-progress",
            0,
            10,
            Duration::from_millis(50),
        )
        .with_fetch_wait_duration(fetcher_metrics.fetch_wait_duration.clone());
        consumer.start().await;

        // During the first 200ms, the 1st no-progress fetch must back off (>= 250ms retry_backoff)
        // instead of hot-spinning dozens/hundreds of requests.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let early_fetches = fetch_count.load(Ordering::SeqCst);
        assert!(
            early_fetches <= 2,
            "expected backoff after non-advancing batch, saw {early_fetches} fetches in 200ms"
        );

        let f = consumer
            .poll_fetches_until(tokio::time::sleep(Duration::from_secs(5)))
            .await
            .expect("expected offset 10 after backoff");
        assert!(f.err.is_none());
        assert_eq!(f.records.len(), 1);
        assert_eq!(f.records[0].offset, 10);

        // W24-B-04: fetch_wait_duration observed on poll_fetches_until.
        let mfs = reg.gather();
        let wait_hist = mfs
            .iter()
            .find(|mf| mf.get_name() == "cortex_ingest_storage_reader_records_batch_wait_duration_seconds")
            .expect("fetch_wait_duration metric must be registered");
        assert!(
            wait_hist.get_metric()[0].get_histogram().get_sample_count() >= 1,
            "fetch_wait_duration must be observed on ClientPartitionConsumer poll"
        );

        consumer.stop().await;
        srv.abort();
    }

    #[tokio::test]
    async fn w24_b02_kip951_current_leader_requires_strictly_greater_epoch_and_uses_node_endpoints() {
        let l0 = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr0 = l0.local_addr().unwrap();
        let l1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr1 = l1.local_addr().unwrap();

        let topic_id = [9u8; 16];
        let b0_meta_calls = Arc::new(AtomicUsize::new(0));
        let b0_fetch_calls = Arc::new(AtomicUsize::new(0));
        let b1_fetch_epochs = Arc::new(parking_lot::Mutex::new(Vec::<i32>::new()));

        let b0_mc = Arc::clone(&b0_meta_calls);
        let b0_fc = Arc::clone(&b0_fetch_calls);
        let srv0 = tokio::spawn(async move {
            loop {
                let Ok((mut s, _)) = l0.accept().await else {
                    break;
                };
                let b0_mc = Arc::clone(&b0_mc);
                let b0_fc = Arc::clone(&b0_fc);
                tokio::spawn(async move {
                    loop {
                        let mut len = [0u8; 4];
                        if s.read_exact(&mut len).await.is_err() {
                            return;
                        }
                        let mut buf = vec![0u8; u32::from_be_bytes(len) as usize];
                        if s.read_exact(&mut buf).await.is_err() {
                            return;
                        }
                        let mut buf = Bytes::from(buf);
                        let key = ApiKey::try_from(i16::from_be_bytes([buf[0], buf[1]])).unwrap();
                        let ver = i16::from_be_bytes([buf[2], buf[3]]);
                        let hv = match key {
                            ApiKey::ApiVersions => ApiVersionsRequest::header_version(ver),
                            ApiKey::Metadata => MetadataRequest::header_version(ver),
                            ApiKey::Fetch => FetchRequest::header_version(ver),
                            _ => return,
                        };
                        let hdr = RequestHeader::decode(&mut buf, hv).unwrap();
                        let out = match key {
                            ApiKey::ApiVersions => {
                                let _ = ApiVersionsRequest::decode(&mut buf, ver);
                                let mut r = ApiVersionsResponse::default();
                                for (k, max) in [
                                    (ApiKey::ApiVersions, 3),
                                    (ApiKey::Metadata, 12),
                                    (ApiKey::Fetch, 16),
                                ] {
                                    let mut av = ApiVersion::default();
                                    av.api_key = k as i16;
                                    av.min_version = 0;
                                    av.max_version = max;
                                    r.api_keys.push(av);
                                }
                                encode_msg(0, ver, hdr.correlation_id, &r)
                            }
                            ApiKey::Metadata => {
                                let _ = MetadataRequest::decode(&mut buf, ver);
                                b0_mc.fetch_add(1, Ordering::SeqCst);
                                // Stale metadata: still claims broker 0 is leader at epoch 5, and does NOT list broker 1!
                                let mut b0 = MetadataResponseBroker::default();
                                b0.node_id = BrokerId(0);
                                b0.host = StrBytes::from_static_str("127.0.0.1");
                                b0.port = addr0.port() as i32;
                                let mut p = MetadataResponsePartition::default();
                                p.partition_index = 0;
                                p.leader_id = BrokerId(0);
                                p.leader_epoch = 5;
                                let mut t = MetadataResponseTopic::default();
                                t.name = Some(TopicName(StrBytes::from_static_str("kip951")));
                                t.topic_id = uuid::Uuid::from_bytes(topic_id);
                                t.partitions = vec![p];
                                let mut r = MetadataResponse::default();
                                r.brokers = vec![b0];
                                r.controller_id = BrokerId(0);
                                r.topics = vec![t];
                                encode_msg(MetadataResponse::header_version(ver), ver, hdr.correlation_id, &r)
                            }
                            ApiKey::Fetch => {
                                let _ = FetchRequest::decode(&mut buf, ver).unwrap();
                                let n = b0_fc.fetch_add(1, Ordering::SeqCst);
                                let mut pd = PartitionData::default();
                                pd.partition_index = 0;
                                pd.error_code = ResponseError::NotLeaderOrFollower.code();
                                let mut ep1 = NodeEndpoint::default();
                                ep1.node_id = BrokerId(1);
                                ep1.host = StrBytes::from_static_str("127.0.0.1");
                                ep1.port = addr1.port() as i32;
                                let mut cl = LeaderIdAndEpoch::default();
                                cl.leader_id = BrokerId(1);
                                // Fetch 0: equal epoch 5 -> MUST be ignored (triggers metadata refresh).
                                // Fetch 1: strictly greater epoch 6 -> MUST move to broker 1 via NodeEndpoints without metadata refresh!
                                cl.leader_epoch = if n == 0 { 5 } else { 6 };
                                pd.current_leader = cl;
                                let mut ft = FetchableTopicResponse::default();
                                ft.topic_id = uuid::Uuid::from_bytes(topic_id);
                                ft.partitions = vec![pd];
                                let mut r = FetchResponse::default();
                                r.responses = vec![ft];
                                r.node_endpoints = vec![ep1];
                                encode_msg(FetchResponse::header_version(ver), ver, hdr.correlation_id, &r)
                            }
                            _ => unreachable!(),
                        };
                        if s.write_all(&out).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });

        let b1_ep = Arc::clone(&b1_fetch_epochs);
        let srv1 = tokio::spawn(async move {
            loop {
                let Ok((mut s, _)) = l1.accept().await else {
                    break;
                };
                let b1_ep = Arc::clone(&b1_ep);
                tokio::spawn(async move {
                    loop {
                        let mut len = [0u8; 4];
                        if s.read_exact(&mut len).await.is_err() {
                            return;
                        }
                        let mut buf = vec![0u8; u32::from_be_bytes(len) as usize];
                        if s.read_exact(&mut buf).await.is_err() {
                            return;
                        }
                        let mut buf = Bytes::from(buf);
                        let key = ApiKey::try_from(i16::from_be_bytes([buf[0], buf[1]])).unwrap();
                        let ver = i16::from_be_bytes([buf[2], buf[3]]);
                        let hv = match key {
                            ApiKey::ApiVersions => ApiVersionsRequest::header_version(ver),
                            ApiKey::Fetch => FetchRequest::header_version(ver),
                            _ => return,
                        };
                        let hdr = RequestHeader::decode(&mut buf, hv).unwrap();
                        let out = match key {
                            ApiKey::ApiVersions => {
                                let _ = ApiVersionsRequest::decode(&mut buf, ver);
                                let mut r = ApiVersionsResponse::default();
                                for (k, max) in [(ApiKey::ApiVersions, 3), (ApiKey::Fetch, 16)] {
                                    let mut av = ApiVersion::default();
                                    av.api_key = k as i16;
                                    av.min_version = 0;
                                    av.max_version = max;
                                    r.api_keys.push(av);
                                }
                                encode_msg(0, ver, hdr.correlation_id, &r)
                            }
                            ApiKey::Fetch => {
                                let req = FetchRequest::decode(&mut buf, ver).unwrap();
                                let epoch = req.topics[0].partitions[0].current_leader_epoch;
                                b1_ep.lock().push(epoch);
                                let mut pd = PartitionData::default();
                                pd.partition_index = 0;
                                pd.high_watermark = 1;
                                pd.log_start_offset = 0;
                                pd.records = Some(encode_batch_range(0, 1));
                                let mut ft = FetchableTopicResponse::default();
                                ft.topic_id = uuid::Uuid::from_bytes(topic_id);
                                ft.partitions = vec![pd];
                                let mut r = FetchResponse::default();
                                r.responses = vec![ft];
                                encode_msg(FetchResponse::header_version(ver), ver, hdr.correlation_id, &r)
                            }
                            _ => unreachable!(),
                        };
                        if s.write_all(&out).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });

        let cfg = KafkaConfig {
            address: StringSliceCSV(vec![addr0.to_string()]),
            topic: "kip951".to_string(),
            ..Default::default()
        };
        let client = Arc::new(KafkaClient::from_config(&cfg).unwrap());
        let consumer = ClientPartitionConsumer::new(
            Arc::clone(&client),
            "kip951",
            0,
            0,
            Duration::from_millis(50),
        );
        consumer.start().await;

        let f = consumer
            .poll_fetches_until(tokio::time::sleep(Duration::from_secs(5)))
            .await
            .expect("expected record from new leader via KIP-951");
        assert!(f.err.is_none());
        assert_eq!(f.records.len(), 1);
        assert_eq!(f.records[0].offset, 0);

        // Fetch 0 (epoch 5 == cursor.leader_epoch 5) triggered metadata refresh (2 total: initial + after fetch 0);
        // Fetch 1 (epoch 6 > 5) moved directly via KIP-951 NodeEndpoints WITHOUT a 3rd metadata refresh!
        assert_eq!(b0_fetch_calls.load(Ordering::SeqCst), 2);
        assert_eq!(b0_meta_calls.load(Ordering::SeqCst), 2);
        assert_eq!(*b1_fetch_epochs.lock(), vec![6]);
        assert_eq!(client.cached_partition_leader("kip951", 0), (1, 6));
        assert!(client.is_known_broker(1));

        consumer.stop().await;
        srv0.abort();
        srv1.abort();
    }

    #[tokio::test]
    async fn d1_8_unknown_topic_id_surfaces_after_five_consecutive_failures_and_resets() {
        let fetch_calls = Arc::new(AtomicUsize::new(0));
        let fetch_calls_srv = Arc::clone(&fetch_calls);
        let return_ok_on_call = Arc::new(AtomicUsize::new(usize::MAX));
        let return_ok_srv = Arc::clone(&return_ok_on_call);
        let topic_id = [7u8; 16];

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let port = addr.port() as i32;
        let srv = tokio::spawn(async move {
            loop {
                let Ok((mut s, _)) = listener.accept().await else {
                    break;
                };
                let fc = Arc::clone(&fetch_calls_srv);
                let ok_at = Arc::clone(&return_ok_srv);
                tokio::spawn(async move {
                    loop {
                        let mut len = [0u8; 4];
                        if s.read_exact(&mut len).await.is_err() {
                            return;
                        }
                        let mut buf = vec![0u8; u32::from_be_bytes(len) as usize];
                        if s.read_exact(&mut buf).await.is_err() {
                            return;
                        }
                        let mut buf = Bytes::from(buf);
                        let key = ApiKey::try_from(i16::from_be_bytes([buf[0], buf[1]])).unwrap();
                        let ver = i16::from_be_bytes([buf[2], buf[3]]);
                        let hv = match key {
                            ApiKey::ApiVersions => ApiVersionsRequest::header_version(ver),
                            ApiKey::Metadata => MetadataRequest::header_version(ver),
                            ApiKey::Fetch => FetchRequest::header_version(ver),
                            _ => return,
                        };
                        let hdr = RequestHeader::decode(&mut buf, hv).unwrap();
                        let out = match key {
                            ApiKey::ApiVersions => {
                                let mut r = ApiVersionsResponse::default();
                                for (k, min, max) in [
                                    (ApiKey::ApiVersions as i16, 0, 3),
                                    (ApiKey::Metadata as i16, 0, 12),
                                    (ApiKey::Fetch as i16, 4, 15),
                                ] {
                                    let mut av = ApiVersion::default();
                                    av.api_key = k;
                                    av.min_version = min;
                                    av.max_version = max;
                                    r.api_keys.push(av);
                                }
                                encode_msg(0, ver, hdr.correlation_id, &r)
                            }
                            ApiKey::Metadata => {
                                let mut b = MetadataResponseBroker::default();
                                b.node_id = BrokerId(0);
                                b.host = StrBytes::from_static_str("127.0.0.1");
                                b.port = port;
                                let mut p = MetadataResponsePartition::default();
                                p.partition_index = 0;
                                p.leader_id = BrokerId(0);
                                let mut t = MetadataResponseTopic::default();
                                t.name = Some(TopicName(StrBytes::from_static_str("utid")));
                                t.topic_id = uuid::Uuid::from_bytes(topic_id);
                                t.partitions = vec![p];
                                let mut r = MetadataResponse::default();
                                r.brokers = vec![b];
                                r.topics = vec![t];
                                encode_msg(MetadataResponse::header_version(ver), ver, hdr.correlation_id, &r)
                            }
                            ApiKey::Fetch => {
                                let call_idx = fc.fetch_add(1, Ordering::SeqCst) + 1;
                                let mut pd = PartitionData::default();
                                pd.partition_index = 0;
                                if call_idx == ok_at.load(Ordering::SeqCst) {
                                    pd.high_watermark = 1;
                                    pd.log_start_offset = 0;
                                    pd.records = Some(encode_batch_range(0, 1));
                                } else {
                                    pd.error_code = ResponseError::UnknownTopicId.code();
                                }
                                let mut ft = FetchableTopicResponse::default();
                                ft.topic_id = uuid::Uuid::from_bytes(topic_id);
                                ft.partitions = vec![pd];
                                let mut r = FetchResponse::default();
                                r.responses = vec![ft];
                                encode_msg(FetchResponse::header_version(ver), ver, hdr.correlation_id, &r)
                            }
                            _ => unreachable!(),
                        };
                        if s.write_all(&out).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });

        // Case 1: 5 consecutive UnknownTopicId responses are stripped, then the 6th surfaces!
        let cfg = KafkaConfig {
            address: StringSliceCSV(vec![addr.to_string()]),
            topic: "utid".to_string(),
            ..Default::default()
        };
        let client = Arc::new(KafkaClient::from_config(&cfg).unwrap());
        let consumer = ClientPartitionConsumer::new(
            Arc::clone(&client),
            "utid",
            0,
            0,
            Duration::from_millis(20),
        );
        consumer.start().await;

        let f = consumer
            .poll_fetches_until(tokio::time::sleep(Duration::from_secs(15)))
            .await
            .expect("expected surfaced UnknownTopicId after >5 consecutive failures");
        assert!(f.err.is_some(), "expected fetch error on 6th UnknownTopicId");
        assert!(
            fetch_calls.load(Ordering::SeqCst) >= 6,
            "expected at least 6 fetch calls (5 stripped + 1 surfaced), got {}",
            fetch_calls.load(Ordering::SeqCst)
        );
        consumer.stop().await;

        // Case 2: 3 UnknownTopicId failures followed by a healthy response resets the counter,
        // so it takes another >5 consecutive failures before surfacing.
        fetch_calls.store(0, Ordering::SeqCst);
        return_ok_on_call.store(4, Ordering::SeqCst);
        let consumer2 = ClientPartitionConsumer::new(
            Arc::clone(&client),
            "utid",
            0,
            0,
            Duration::from_millis(20),
        );
        consumer2.start().await;
        let ok_fetch = consumer2
            .poll_fetches_until(tokio::time::sleep(Duration::from_secs(15)))
            .await
            .expect("expected healthy fetch at call 4");
        assert!(ok_fetch.err.is_none());
        assert_eq!(ok_fetch.records.len(), 1);
        assert_eq!(fetch_calls.load(Ordering::SeqCst), 4);

        let err_fetch = consumer2
            .poll_fetches_until(tokio::time::sleep(Duration::from_secs(15)))
            .await
            .expect("expected UnknownTopicId after reset + >5 failures");
        assert!(err_fetch.err.is_some());
        // 4 calls up to success + 6 more UnknownTopicId calls = 10 calls total.
        assert!(
            fetch_calls.load(Ordering::SeqCst) >= 10,
            "counter must reset after non-UnknownTopicId response (calls={})",
            fetch_calls.load(Ordering::SeqCst)
        );

        consumer2.stop().await;
        srv.abort();
    }
}
