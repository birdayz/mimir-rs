#[path = "upstream_storage_part2_helpers.rs"]
mod helpers;

use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use helpers::{
    create_test_kafka_config, get_counter_value, get_gauge_value, FakeKafkaCluster, HookAction,
};
use kafka_protocol::error::ResponseError;
use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};
use kafka_protocol::messages::list_offsets_response::{
    ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
};
use kafka_protocol::messages::offset_commit_response::{
    OffsetCommitResponsePartition, OffsetCommitResponseTopic,
};
use kafka_protocol::messages::offset_fetch_response::{
    OffsetFetchResponsePartition, OffsetFetchResponseTopic,
};
use kafka_protocol::messages::{
    FetchResponse, ListOffsetsResponse, OffsetCommitResponse, OffsetFetchResponse, TopicName,
};
use kafka_protocol::protocol::StrBytes;
use mimir_config::GoDuration;
use mimir_storage::ingest::{
    AdmClient, FetchedRecord, KafkaAdmClient, KafkaClient, NoOpPreCommitNotifier, OffsetFile,
    PartitionCommitter, PartitionOffsets, PreCommitNotifier, ReaderError, ReaderState,
    RecordBatchConsumer, SingleClusterPartitionReader, ERR_OFFSET_FILE_WRITE, KAFKA_OFFSET_END,
    KAFKA_OFFSET_START,
};
use parking_lot::Mutex as SyncMutex;
use prometheus::Registry;
use tokio::sync::{mpsc, Mutex, Notify};

struct TestConsumer {
    tx: mpsc::Sender<FetchedRecord>,
    rx: Mutex<mpsc::Receiver<FetchedRecord>>,
}

impl TestConsumer {
    fn new(capacity: usize) -> Arc<Self> {
        let (tx, rx) = mpsc::channel(capacity.max(1));
        Arc::new(Self {
            tx,
            rx: Mutex::new(rx),
        })
    }

    async fn wait_records(
        &self,
        num_records: usize,
        wait_timeout: Duration,
        drain_period: Duration,
    ) -> Result<Vec<FetchedRecord>, String> {
        let mut out = Vec::new();
        let mut rx = self.rx.lock().await;
        let deadline = tokio::time::Instant::now() + wait_timeout;
        let mut draining = false;
        let mut drain_deadline = deadline;

        loop {
            let target_dl = if draining { drain_deadline } else { deadline };
            let now = tokio::time::Instant::now();
            if now >= target_dl {
                if out.len() != num_records {
                    return Err(format!(
                        "waiting for records: received {}, expected {}",
                        out.len(),
                        num_records
                    ));
                }
                return Ok(out);
            }
            match tokio::time::timeout(target_dl - now, rx.recv()).await {
                Ok(Some(rec)) => {
                    out.push(rec);
                    if out.len() == num_records {
                        if drain_period.is_zero() {
                            return Ok(out);
                        }
                        draining = true;
                        drain_deadline = tokio::time::Instant::now() + drain_period;
                    }
                }
                Ok(None) => {
                    return Err("channel closed".to_string());
                }
                Err(_) => {
                    if out.len() != num_records {
                        return Err(format!(
                            "waiting for records: received {}, expected {}",
                            out.len(),
                            num_records
                        ));
                    }
                    return Ok(out);
                }
            }
        }
    }
}

impl RecordBatchConsumer for TestConsumer {
    fn consume(
        &self,
        records: Vec<FetchedRecord>,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>> {
        Box::pin(async move {
            for r in records {
                self.tx
                    .send(r)
                    .await
                    .map_err(|e| format!("send error: {e}"))?;
            }
            Ok(())
        })
    }
}

struct FnConsumer<F>(F);

impl<F, Fut> RecordBatchConsumer for FnConsumer<F>
where
    F: Fn(Vec<FetchedRecord>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), String>> + Send + 'static,
{
    fn consume(
        &self,
        records: Vec<FetchedRecord>,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>> {
        Box::pin((self.0)(records))
    }
}

fn produce_record_to_cluster(
    cluster: &FakeKafkaCluster,
    topic: &str,
    partition: i32,
    val: &[u8],
    version: Option<u32>,
) -> i64 {
    let headers = match version {
        None | Some(0) => Vec::new(),
        Some(v) => vec![(
            "Version".to_string(),
            Some(Bytes::copy_from_slice(&v.to_be_bytes())),
        )],
    };
    cluster.produce_direct(
        topic,
        partition,
        None,
        Some(Bytes::copy_from_slice(val)),
        headers,
        None,
    )
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

// Upstream: pkg/storage/ingest/reader_test.go::TestKafkaStartOffset
#[test]
fn test_kafka_start_offset() {
    assert_eq!(KAFKA_OFFSET_START, -2);
}

// Upstream: pkg/storage/ingest/reader_test.go::TestKafkaEndOffset
#[test]
fn test_kafka_end_offset() {
    assert_eq!(KAFKA_OFFSET_END, -1);
}

// Upstream: pkg/storage/ingest/reader_test.go::TestSingleClusterPartitionReader
#[tokio::test]
async fn test_single_cluster_partition_reader() {
    const TOPIC: &str = "test";
    const PARTITION_ID: i32 = 1;

    let cluster = FakeKafkaCluster::start(PARTITION_ID + 1, TOPIC).await;
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
    cfg.consume_from_position_at_startup = "start".to_string();
    cfg.target_consumer_lag_at_startup = GoDuration::ZERO;
    cfg.max_consumer_lag_at_startup = GoDuration::ZERO;

    let consumer = TestConsumer::new(8);
    let reg = Registry::new();
    let reader = SingleClusterPartitionReader::new(
        cfg,
        PARTITION_ID,
        "test-group",
        Some(dir.path().join("offset.json")),
        Arc::clone(&consumer) as Arc<dyn RecordBatchConsumer>,
        Some(&reg),
    )
    .unwrap();
    reader.start().await.unwrap();

    produce_record_to_cluster(&cluster, TOPIC, PARTITION_ID, b"record 1", Some(1));
    produce_record_to_cluster(&cluster, TOPIC, PARTITION_ID, b"record 2", Some(1));
    produce_record_to_cluster(&cluster, TOPIC, PARTITION_ID, b"record 3", Some(0));
    produce_record_to_cluster(&cluster, TOPIC, PARTITION_ID, b"record 4", Some(2));

    let records = consumer
        .wait_records(4, Duration::from_secs(5), Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(records.len(), 4);
    assert_eq!(records[0].value.as_ref(), b"record 1");
    assert_eq!(
        mimir_storage::ingest::parse_record_version(
            records[0]
                .headers
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_ref()))
        ),
        1
    );
    assert_eq!(records[1].value.as_ref(), b"record 2");
    assert_eq!(
        mimir_storage::ingest::parse_record_version(
            records[1]
                .headers
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_ref()))
        ),
        1
    );
    assert_eq!(records[2].value.as_ref(), b"record 3");
    assert_eq!(
        mimir_storage::ingest::parse_record_version(
            records[2]
                .headers
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_ref()))
        ),
        0
    );
    assert_eq!(records[3].value.as_ref(), b"record 4");
    assert_eq!(
        mimir_storage::ingest::parse_record_version(
            records[3]
                .headers
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_ref()))
        ),
        2
    );

    reader.stop().await;
    cluster.close();
}

// Upstream: pkg/storage/ingest/reader_test.go::TestSingleClusterPartitionReader_RequiresOffsetFilePath
#[test]
fn test_single_cluster_partition_reader_requires_offset_file_path() {
    let cfg = create_test_kafka_config("127.0.0.1:9092", "test");
    let consumer = TestConsumer::new(1);
    let res = SingleClusterPartitionReader::new_strict(cfg, 1, "test-group", "", consumer, None);
    let err = res.err().expect("expected error when offset file path empty");
    assert!(
        err.to_string().contains("offset file path must be specified"),
        "{err}"
    );
}

// Upstream: pkg/storage/ingest/reader_test.go::TestSingleClusterPartitionReader_ShouldHonorConfiguredFetchMaxWait
#[test]
fn test_single_cluster_partition_reader_should_honor_configured_fetch_max_wait() {
    let mut cfg = create_test_kafka_config("127.0.0.1:9092", "test");
    cfg.fetch_max_wait = GoDuration::from_secs(12);
    let dir = tempfile::tempdir().unwrap();
    let consumer = TestConsumer::new(1);
    let reader = SingleClusterPartitionReader::new(
        cfg,
        1,
        "test-group",
        Some(dir.path().join("offset.json")),
        consumer,
        None,
    )
    .unwrap();
    assert_eq!(
        reader.concurrent_fetchers_min_bytes_max_wait_time(),
        Duration::from_secs(12)
    );
}

// Upstream: pkg/storage/ingest/reader_test.go::TestSingleClusterPartitionReader_logFetchErrors
#[test]
fn test_single_cluster_partition_reader_log_fetch_errors() {
    let cfg = create_test_kafka_config("127.0.0.1:9092", "test");
    let dir = tempfile::tempdir().unwrap();
    let reg = Registry::new();
    let reader = SingleClusterPartitionReader::new(
        cfg,
        1,
        "test-group",
        Some(dir.path().join("offset.json")),
        TestConsumer::new(1),
        Some(&reg),
    )
    .unwrap();

    reader.log_fetch_errors(&[
        ("test", 1, "context canceled", true),
        ("test", 1, "wrapped: context canceled", true),
        ("test", 1, "real error", false),
    ]);

    assert_eq!(
        get_counter_value(&reg, "cortex_ingest_storage_reader_fetch_errors_total", &[]),
        1.0
    );
}

// Upstream: pkg/storage/ingest/reader_test.go::TestSingleClusterPartitionReader_ConsumerError
#[tokio::test]
async fn test_single_cluster_partition_reader_consumer_error() {
    const TOPIC: &str = "test";
    const PARTITION_ID: i32 = 1;

    for fetch_conc in [0, 2] {
        let cluster = FakeKafkaCluster::start(PARTITION_ID + 1, TOPIC).await;
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        cfg.fetch_concurrency_max = fetch_conc;
        cfg.target_consumer_lag_at_startup = GoDuration::ZERO;
        cfg.max_consumer_lag_at_startup = GoDuration::ZERO;

        let invocations = Arc::new(AtomicI64::new(0));
        let return_errors = Arc::new(AtomicBool::new(true));
        let tracking = TestConsumer::new(4);

        let inv_c = Arc::clone(&invocations);
        let ret_c = Arc::clone(&return_errors);
        let trk_c = Arc::clone(&tracking);
        let consumer = Arc::new(FnConsumer(move |recs: Vec<FetchedRecord>| {
            let inv_c = Arc::clone(&inv_c);
            let ret_c = Arc::clone(&ret_c);
            let trk_c = Arc::clone(&trk_c);
            async move {
                inv_c.fetch_add(1, Ordering::SeqCst);
                if !ret_c.load(Ordering::SeqCst) {
                    return trk_c.consume(recs).await;
                }
                assert_eq!(recs[0].value.as_ref(), b"1");
                Err("consumer error".to_string())
            }
        }));

        let reader = SingleClusterPartitionReader::new(
            cfg,
            PARTITION_ID,
            "test-group",
            Some(dir.path().join("offset.json")),
            consumer,
            None,
        )
        .unwrap();
        reader.start().await.unwrap();

        produce_record_to_cluster(&cluster, TOPIC, PARTITION_ID, b"1", Some(1));
        produce_record_to_cluster(&cluster, TOPIC, PARTITION_ID, b"2", Some(1));

        let dl = Instant::now() + Duration::from_secs(5);
        while invocations.load(Ordering::SeqCst) <= 1 {
            assert!(Instant::now() < dl);
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        return_errors.store(false, Ordering::SeqCst);

        let records = tracking
            .wait_records(2, Duration::from_secs(2), Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(records[0].value.as_ref(), b"1");
        assert_eq!(records[1].value.as_ref(), b"2");

        reader.stop().await;
        cluster.close();
    }
}

// Upstream: pkg/storage/ingest/reader_test.go::TestSingleClusterPartitionReader_ConsumerStopping
#[tokio::test]
async fn test_single_cluster_partition_reader_consumer_stopping() {
    const TOPIC: &str = "test";
    const PARTITION_ID: i32 = 1;

    for fetch_conc in [0, 2] {
        let cluster = FakeKafkaCluster::start(PARTITION_ID + 1, TOPIC).await;
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        cfg.fetch_concurrency_max = fetch_conc;
        cfg.target_consumer_lag_at_startup = GoDuration::ZERO;
        cfg.max_consumer_lag_at_startup = GoDuration::ZERO;

        let in_flight = Arc::new(Notify::new());
        let in_flight_flag = Arc::new(AtomicBool::new(false));
        let finished_ok = Arc::new(AtomicBool::new(false));

        let if_n = Arc::clone(&in_flight);
        let if_f = Arc::clone(&in_flight_flag);
        let fin_c = Arc::clone(&finished_ok);
        let consumer = Arc::new(FnConsumer(move |recs: Vec<FetchedRecord>| {
            let if_n = Arc::clone(&if_n);
            let if_f = Arc::clone(&if_f);
            let fin_c = Arc::clone(&fin_c);
            async move {
                assert_eq!(recs.len(), 1);
                assert_eq!(recs[0].value.as_ref(), b"1");
                if_f.store(true, Ordering::SeqCst);
                if_n.notify_waiters();
                tokio::time::sleep(Duration::from_millis(250)).await;
                fin_c.store(true, Ordering::SeqCst);
                Ok(())
            }
        }));

        let reader = Arc::new(
            SingleClusterPartitionReader::new(
                cfg,
                PARTITION_ID,
                "test-group",
                Some(dir.path().join("offset.json")),
                consumer,
                None,
            )
            .unwrap(),
        );
        reader.start().await.unwrap();

        produce_record_to_cluster(&cluster, TOPIC, PARTITION_ID, b"1", Some(1));

        while !in_flight_flag.load(Ordering::SeqCst) {
            let n = in_flight.notified();
            if in_flight_flag.load(Ordering::SeqCst) {
                break;
            }
            n.await;
        }

        // Stop while consumer is in-flight; must wait for in-flight consumption to succeed.
        reader.stop().await;
        assert!(finished_ok.load(Ordering::SeqCst));
        cluster.close();
    }
}

// Upstream: pkg/storage/ingest/reader_test.go::TestSingleClusterPartitionReader_WaitReadConsistencyUntilLastProducedOffset_And_WaitReadConsistencyUntilOffsets
#[tokio::test]
async fn test_single_cluster_partition_reader_wait_read_consistency() {
    const TOPIC: &str = "test";
    const PARTITION_ID: i32 = 0;

    // 1. "should return after all produced records have been consumed"
    for with_offset in [false, true] {
        let cluster = FakeKafkaCluster::start(PARTITION_ID + 1, TOPIC).await;
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        cfg.last_produced_offset_poll_interval = GoDuration::from_millis(30);
        cfg.target_consumer_lag_at_startup = GoDuration::ZERO;
        cfg.max_consumer_lag_at_startup = GoDuration::ZERO;

        let consumed = Arc::new(AtomicI64::new(0));
        let cons_c = Arc::clone(&consumed);
        let consumer = Arc::new(FnConsumer(move |recs: Vec<FetchedRecord>| {
            let cons_c = Arc::clone(&cons_c);
            async move {
                for r in recs {
                    let next = cons_c.load(Ordering::SeqCst) + 1;
                    if next == 2 {
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                    let cur = cons_c.fetch_add(1, Ordering::SeqCst) + 1;
                    assert_eq!(r.value.as_ref(), format!("record-{cur}").as_bytes());
                }
                Ok(())
            }
        }));

        let reg = Registry::new();
        let reader = SingleClusterPartitionReader::new(
            cfg,
            PARTITION_ID,
            "test-group",
            Some(dir.path().join("offset.json")),
            consumer,
            Some(&reg),
        )
        .unwrap();
        reader.start().await.unwrap();

        produce_record_to_cluster(&cluster, TOPIC, PARTITION_ID, b"record-1", Some(1));
        let last_off =
            produce_record_to_cluster(&cluster, TOPIC, PARTITION_ID, b"record-2", Some(1));

        if with_offset {
            reader
                .wait_read_consistency_until_offsets(
                    &PartitionOffsets::new_single_cluster(last_off),
                    Duration::from_secs(5),
                )
                .await
                .unwrap();
        } else {
            reader
                .wait_read_consistency_until_last_produced_offset(Duration::from_secs(5))
                .await
                .unwrap();
        }
        assert_eq!(consumed.load(Ordering::SeqCst), 2);
        assert_eq!(
            get_counter_value(
                &reg,
                "cortex_ingest_storage_strong_consistency_requests_total",
                &[
                    ("component", "partition-reader"),
                    ("topic", TOPIC),
                    ("with_offset", if with_offset { "true" } else { "false" })
                ]
            ),
            1.0
        );
        assert_eq!(
            get_counter_value(
                &reg,
                "cortex_ingest_storage_strong_consistency_failures_total",
                &[("component", "partition-reader"), ("topic", TOPIC)]
            ),
            0.0
        );

        reader.stop().await;
        cluster.close();
    }

    // 2. "should block until the request context deadline is exceeded if produced records are not consumed"
    for with_offset in [false, true] {
        let cluster = FakeKafkaCluster::start(PARTITION_ID + 1, TOPIC).await;
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        cfg.last_produced_offset_poll_interval = GoDuration::from_millis(30);
        cfg.wait_strong_read_consistency_timeout = GoDuration::ZERO;
        cfg.target_consumer_lag_at_startup = GoDuration::ZERO;
        cfg.max_consumer_lag_at_startup = GoDuration::ZERO;

        let block = Arc::new(AtomicBool::new(true));
        let block_c = Arc::clone(&block);
        let tracking = TestConsumer::new(4);
        let trk_c = Arc::clone(&tracking);
        let consumer = Arc::new(FnConsumer(move |recs: Vec<FetchedRecord>| {
            let block_c = Arc::clone(&block_c);
            let trk_c = Arc::clone(&trk_c);
            async move {
                while block_c.load(Ordering::SeqCst) {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                trk_c.consume(recs).await
            }
        }));

        let reg = Registry::new();
        let reader = SingleClusterPartitionReader::new(
            cfg,
            PARTITION_ID,
            "test-group",
            Some(dir.path().join("offset.json")),
            consumer,
            Some(&reg),
        )
        .unwrap();
        reader.start().await.unwrap();

        let last_off =
            produce_record_to_cluster(&cluster, TOPIC, PARTITION_ID, b"record-1", Some(1));
        let err = if with_offset {
            reader
                .wait_read_consistency_until_offsets(
                    &PartitionOffsets::new_single_cluster(last_off),
                    Duration::from_millis(150),
                )
                .await
                .unwrap_err()
        } else {
            reader
                .wait_read_consistency_until_last_produced_offset(Duration::from_millis(150))
                .await
                .unwrap_err()
        };
        assert!(matches!(err, ReaderError::Watcher(_)));

        block.store(false, Ordering::SeqCst);
        let recs = tracking
            .wait_records(1, Duration::from_secs(2), Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(recs[0].value.as_ref(), b"record-1");
        assert_eq!(
            get_counter_value(
                &reg,
                "cortex_ingest_storage_strong_consistency_failures_total",
                &[("component", "partition-reader"), ("topic", TOPIC)]
            ),
            1.0
        );

        reader
            .wait_read_consistency_until_last_produced_offset(Duration::from_secs(2))
            .await
            .unwrap();

        reader.stop().await;
        cluster.close();
    }

    // 3. "should block until the configured wait timeout is exceeded if produced records are not consumed"
    for with_offset in [false, true] {
        let cluster = FakeKafkaCluster::start(PARTITION_ID + 1, TOPIC).await;
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        cfg.last_produced_offset_poll_interval = GoDuration::from_millis(30);
        cfg.wait_strong_read_consistency_timeout = GoDuration::from_millis(150);
        cfg.target_consumer_lag_at_startup = GoDuration::ZERO;
        cfg.max_consumer_lag_at_startup = GoDuration::ZERO;

        let block = Arc::new(AtomicBool::new(true));
        let block_c = Arc::clone(&block);
        let consumer = Arc::new(FnConsumer(move |_| {
            let block_c = Arc::clone(&block_c);
            async move {
                while block_c.load(Ordering::SeqCst) {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Ok(())
            }
        }));

        let reg = Registry::new();
        let reader = SingleClusterPartitionReader::new(
            cfg,
            PARTITION_ID,
            "test-group",
            Some(dir.path().join("offset.json")),
            consumer,
            Some(&reg),
        )
        .unwrap();
        reader.start().await.unwrap();

        let last_off =
            produce_record_to_cluster(&cluster, TOPIC, PARTITION_ID, b"record-1", Some(1));
        let err = if with_offset {
            reader
                .wait_read_consistency_until_offsets(
                    &PartitionOffsets::new_single_cluster(last_off),
                    Duration::from_secs(5),
                )
                .await
                .unwrap_err()
        } else {
            reader
                .wait_read_consistency_until_last_produced_offset(Duration::from_secs(5))
                .await
                .unwrap_err()
        };
        assert!(matches!(
            err,
            ReaderError::WaitStrongReadConsistencyTimeoutExceeded
        ));
        block.store(false, Ordering::SeqCst);
        assert_eq!(
            get_counter_value(
                &reg,
                "cortex_ingest_storage_strong_consistency_failures_total",
                &[("component", "partition-reader"), ("topic", TOPIC)]
            ),
            1.0
        );

        reader.stop().await;
        cluster.close();
    }

    // 4. "should return if no records have been produced yet"
    for with_offset in [false, true] {
        let cluster = FakeKafkaCluster::start(PARTITION_ID + 1, TOPIC).await;
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        cfg.last_produced_offset_poll_interval = GoDuration::from_millis(30);
        cfg.target_consumer_lag_at_startup = GoDuration::ZERO;
        cfg.max_consumer_lag_at_startup = GoDuration::ZERO;

        let reg = Registry::new();
        let reader = SingleClusterPartitionReader::new(
            cfg,
            PARTITION_ID,
            "test-group",
            Some(dir.path().join("offset.json")),
            TestConsumer::new(1),
            Some(&reg),
        )
        .unwrap();
        reader.start().await.unwrap();

        if with_offset {
            reader
                .wait_read_consistency_until_offsets(
                    &PartitionOffsets::new_single_cluster(-1),
                    Duration::from_secs(1),
                )
                .await
                .unwrap();
        } else {
            reader
                .wait_read_consistency_until_last_produced_offset(Duration::from_secs(1))
                .await
                .unwrap();
        }
        assert_eq!(
            get_counter_value(
                &reg,
                "cortex_ingest_storage_strong_consistency_failures_total",
                &[("component", "partition-reader"), ("topic", TOPIC)]
            ),
            0.0
        );

        reader.stop().await;
        cluster.close();
    }

    // 5. "should return an error if the SingleClusterPartitionReader is not running"
    for with_offset in [false, true] {
        let cluster = FakeKafkaCluster::start(PARTITION_ID + 1, TOPIC).await;
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        cfg.target_consumer_lag_at_startup = GoDuration::ZERO;
        cfg.max_consumer_lag_at_startup = GoDuration::ZERO;

        let reg = Registry::new();
        let reader = SingleClusterPartitionReader::new(
            cfg,
            PARTITION_ID,
            "test-group",
            Some(dir.path().join("offset.json")),
            TestConsumer::new(1),
            Some(&reg),
        )
        .unwrap();
        reader.start().await.unwrap();
        reader.stop().await;

        let err = if with_offset {
            reader
                .wait_read_consistency_until_offsets(
                    &PartitionOffsets::new_single_cluster(-1),
                    Duration::from_secs(1),
                )
                .await
                .unwrap_err()
        } else {
            reader
                .wait_read_consistency_until_last_produced_offset(Duration::from_secs(1))
                .await
                .unwrap_err()
        };
        assert!(matches!(err, ReaderError::NotRunning));
        assert_eq!(
            get_counter_value(
                &reg,
                "cortex_ingest_storage_strong_consistency_failures_total",
                &[("component", "partition-reader"), ("topic", TOPIC)]
            ),
            1.0
        );
        cluster.close();
    }

    // 6. "should reject offsets for more than one Kafka cluster"
    {
        let cluster = FakeKafkaCluster::start(PARTITION_ID + 1, TOPIC).await;
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        cfg.target_consumer_lag_at_startup = GoDuration::ZERO;
        cfg.max_consumer_lag_at_startup = GoDuration::ZERO;

        let reader = SingleClusterPartitionReader::new(
            cfg,
            PARTITION_ID,
            "test-group",
            Some(dir.path().join("offset.json")),
            TestConsumer::new(1),
            None,
        )
        .unwrap();
        reader.start().await.unwrap();

        let err = reader
            .wait_read_consistency_until_offsets(
                &PartitionOffsets::new_multi_cluster(vec![1, 2]),
                Duration::from_secs(1),
            )
            .await
            .unwrap_err();
        assert!(err
            .to_string()
            .contains("single-cluster partition reader was given read consistency offsets for 2 Kafka clusters"));

        reader.stop().await;
        cluster.close();
    }
}

// Upstream: pkg/storage/ingest/reader_test.go::TestSingleClusterPartitionReader_EnforceReadMaxDelay
#[tokio::test]
async fn test_single_cluster_partition_reader_enforce_read_max_delay() {
    const TOPIC: &str = "test";
    const PARTITION_ID: i32 = 0;

    // "should succeed if no record has been consumed yet"
    {
        let cluster = FakeKafkaCluster::start(PARTITION_ID + 1, TOPIC).await;
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        cfg.target_consumer_lag_at_startup = GoDuration::ZERO;
        cfg.max_consumer_lag_at_startup = GoDuration::ZERO;
        let reader = SingleClusterPartitionReader::new(
            cfg,
            PARTITION_ID,
            "test-group",
            Some(dir.path().join("offset.json")),
            Arc::new(FnConsumer(|_| async { Ok(()) })),
            None,
        )
        .unwrap();
        reader.start().await.unwrap();
        assert_eq!(reader.highest_consumed_timestamp_before_partition_end_ms(), 0);
        reader.enforce_read_max_delay(Duration::from_secs(60)).unwrap();
        reader.stop().await;
        cluster.close();
    }

    // "should succeed if the partition has been consumed until the end"
    for age_ms in [0i64, 5 * 60 * 1000] {
        let cluster = FakeKafkaCluster::start(PARTITION_ID + 1, TOPIC).await;
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        cfg.target_consumer_lag_at_startup = GoDuration::ZERO;
        cfg.max_consumer_lag_at_startup = GoDuration::ZERO;

        let consumed = Arc::new(SyncMutex::new(Vec::<String>::new()));
        let cons_c = Arc::clone(&consumed);
        let reader = SingleClusterPartitionReader::new(
            cfg,
            PARTITION_ID,
            "test-group",
            Some(dir.path().join("offset.json")),
            Arc::new(FnConsumer(move |recs: Vec<FetchedRecord>| {
                let cons_c = Arc::clone(&cons_c);
                async move {
                    let mut g = cons_c.lock();
                    for r in recs {
                        g.push(String::from_utf8_lossy(&r.value).into_owned());
                    }
                    Ok(())
                }
            })),
            None,
        )
        .unwrap();
        reader.start().await.unwrap();

        let ts = now_ms() - age_ms;
        cluster.produce_direct(
            TOPIC,
            PARTITION_ID,
            None,
            Some(Bytes::from_static(b"record-1")),
            vec![],
            Some(ts),
        );
        cluster.produce_direct(
            TOPIC,
            PARTITION_ID,
            None,
            Some(Bytes::from_static(b"record-2")),
            vec![],
            Some(ts),
        );

        let dl = Instant::now() + Duration::from_secs(5);
        while consumed.lock().len() < 2 {
            assert!(Instant::now() < dl);
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(reader.highest_consumed_timestamp_before_partition_end_ms(), 0);
        reader.enforce_read_max_delay(Duration::from_secs(60)).unwrap();

        reader.stop().await;
        cluster.close();
    }

    // "should fail if the partition has not been consumed until the end and the current consumption delay is above the max delay"
    {
        let cluster = FakeKafkaCluster::start(PARTITION_ID + 1, TOPIC).await;
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        cfg.max_buffered_bytes = 2 * 1024 * 1024;
        cfg.fetch_concurrency_max = 2;
        cfg.target_consumer_lag_at_startup = GoDuration::ZERO;
        cfg.max_consumer_lag_at_startup = GoDuration::ZERO;

        let first = Arc::new(AtomicBool::new(true));
        let first_c = Arc::clone(&first);
        let reader = SingleClusterPartitionReader::new(
            cfg,
            PARTITION_ID,
            "test-group",
            Some(dir.path().join("offset.json")),
            Arc::new(FnConsumer(move |_| {
                let first_c = Arc::clone(&first_c);
                async move {
                    if first_c.swap(false, Ordering::SeqCst) {
                        Ok(())
                    } else {
                        Err("mocked error to stop consuming records".to_string())
                    }
                }
            })),
            None,
        )
        .unwrap();
        reader.start().await.unwrap();

        let payload = Bytes::from(vec![0xabu8; 512 * 1024]);
        let old_ts = now_ms() - (5 * 60 * 1000);
        for _ in 0..8 {
            cluster.produce_direct(TOPIC, PARTITION_ID, None, Some(payload.clone()), vec![], Some(old_ts));
        }

        let dl = Instant::now() + Duration::from_secs(5);
        while reader.highest_consumed_timestamp_before_partition_end_ms() == 0 {
            assert!(Instant::now() < dl);
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(matches!(
            reader.enforce_read_max_delay(Duration::from_secs(60)),
            Err(ReaderError::MaxDelayExceeded)
        ));

        reader.stop().await;
        cluster.close();
    }
}

// Upstream: pkg/storage/ingest/reader_test.go::TestSingleClusterPartitionReader_ConsumeAtStartup
#[tokio::test]
async fn test_single_cluster_partition_reader_consume_at_startup() {
    const TOPIC: &str = "test";
    const PARTITION_ID: i32 = 1;

    // 1. "should immediately switch to Running state if partition is empty"
    for conc in [0, 2] {
        let cluster = FakeKafkaCluster::start(PARTITION_ID + 1, TOPIC).await;
        let dir = tempfile::tempdir().unwrap();
        let reg = Registry::new();
        let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        cfg.fetch_concurrency_max = conc;
        cfg.target_consumer_lag_at_startup = GoDuration::from_secs(1);
        cfg.max_consumer_lag_at_startup = GoDuration::from_secs(1);

        let reader = SingleClusterPartitionReader::new(
            cfg,
            PARTITION_ID,
            "test-group",
            Some(dir.path().join("offset.json")),
            Arc::new(FnConsumer(|_| async { Ok(()) })),
            Some(&reg),
        )
        .unwrap();
        reader.start().await.unwrap();
        assert_eq!(reader.state(), ReaderState::Running);
        reader.stop().await;
        assert_eq!(
            get_gauge_value(
                &reg,
                "cortex_ingest_storage_reader_last_consumed_offset",
                &[("partition", "1")]
            ),
            Some(-1.0)
        );
        cluster.close();
    }

    // 2. "should immediately switch to Running state if configured target / max lag is 0"
    for conc in [0, 2] {
        let cluster = FakeKafkaCluster::start(PARTITION_ID + 1, TOPIC).await;
        *cluster.hooks.fetch.lock() = Some(Arc::new(|_| {
            Box::pin(async { HookAction::Respond(FetchResponse::default()) })
        }));
        produce_record_to_cluster(&cluster, TOPIC, PARTITION_ID, b"record-1", Some(1));
        produce_record_to_cluster(&cluster, TOPIC, PARTITION_ID, b"record-2", Some(1));

        let dir = tempfile::tempdir().unwrap();
        let reg = Registry::new();
        let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        cfg.fetch_concurrency_max = conc;
        cfg.target_consumer_lag_at_startup = GoDuration::ZERO;
        cfg.max_consumer_lag_at_startup = GoDuration::ZERO;

        let reader = SingleClusterPartitionReader::new(
            cfg,
            PARTITION_ID,
            "test-group",
            Some(dir.path().join("offset.json")),
            Arc::new(FnConsumer(|_| async { Ok(()) })),
            Some(&reg),
        )
        .unwrap();
        reader.start().await.unwrap();
        assert_eq!(reader.state(), ReaderState::Running);
        reader.stop().await;
        assert_eq!(
            get_gauge_value(
                &reg,
                "cortex_ingest_storage_reader_last_consumed_offset",
                &[("partition", "1")]
            ),
            Some(-1.0)
        );
        cluster.close();
    }

    // 3. "should consume partition from start if last committed offset is missing and wait until target lag is honored"
    for conc in [0, 2] {
        let cluster = FakeKafkaCluster::start(PARTITION_ID + 1, TOPIC).await;
        let fetch_reqs = Arc::new(AtomicI64::new(0));
        let should_fail = Arc::new(AtomicBool::new(true));
        let fr_c = Arc::clone(&fetch_reqs);
        let sf_c = Arc::clone(&should_fail);
        *cluster.hooks.fetch.lock() = Some(Arc::new(move |_| {
            let fr_c = Arc::clone(&fr_c);
            let sf_c = Arc::clone(&sf_c);
            Box::pin(async move {
                fr_c.fetch_add(1, Ordering::SeqCst);
                if sf_c.load(Ordering::SeqCst) {
                    HookAction::Respond(FetchResponse::default())
                } else {
                    HookAction::PassThrough
                }
            })
        }));

        produce_record_to_cluster(&cluster, TOPIC, PARTITION_ID, b"record-1", Some(1));
        produce_record_to_cluster(&cluster, TOPIC, PARTITION_ID, b"record-2", Some(1));

        let dir = tempfile::tempdir().unwrap();
        let reg = Registry::new();
        let logs = Arc::new(SyncMutex::new(Vec::<String>::new()));
        let consumed = Arc::new(AtomicI64::new(0));
        let cons_c = Arc::clone(&consumed);
        let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        cfg.fetch_concurrency_max = conc;
        cfg.target_consumer_lag_at_startup = GoDuration::from_secs(1);
        cfg.max_consumer_lag_at_startup = GoDuration::from_secs(2);

        let reader = Arc::new(
            SingleClusterPartitionReader::new(
                cfg,
                PARTITION_ID,
                "test-group",
                Some(dir.path().join("offset.json")),
                Arc::new(FnConsumer(move |recs: Vec<FetchedRecord>| {
                    let cons_c = Arc::clone(&cons_c);
                    async move {
                        cons_c.fetch_add(recs.len() as i64, Ordering::SeqCst);
                        Ok(())
                    }
                })),
                Some(&reg),
            )
            .unwrap()
            .with_log_sink(Arc::clone(&logs)),
        );

        reader.start_async().await;
        let dl = Instant::now() + Duration::from_secs(5);
        while fetch_reqs.load(Ordering::SeqCst) <= 2 {
            assert!(Instant::now() < dl);
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(reader.state(), ReaderState::Starting);
        assert_eq!(consumed.load(Ordering::SeqCst), 0);

        should_fail.store(false, Ordering::SeqCst);
        let dl = Instant::now() + Duration::from_secs(5);
        while reader.state() != ReaderState::Running {
            assert!(Instant::now() < dl);
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(logs
            .lock()
            .join("\n")
            .contains("current lag is lower than configured target consumer lag"));
        assert_eq!(consumed.load(Ordering::SeqCst), 2);
        assert_eq!(
            get_gauge_value(
                &reg,
                "cortex_ingest_storage_reader_last_consumed_offset",
                &[("partition", "1")]
            ),
            Some(1.0)
        );
        reader.stop().await;
        cluster.close();
    }

    // 4. "should consume partition from start ... and retry if a failure occurs when fetching last produced offset"
    {
        let cluster = FakeKafkaCluster::start(PARTITION_ID + 1, TOPIC).await;
        let lo_reqs = Arc::new(AtomicI64::new(0));
        let lo_fail = Arc::new(AtomicBool::new(true));
        let lr_c = Arc::clone(&lo_reqs);
        let lf_c = Arc::clone(&lo_fail);
        *cluster.hooks.list_offsets.lock() = Some(Arc::new(move |req| {
            let lr_c = Arc::clone(&lr_c);
            let lf_c = Arc::clone(&lf_c);
            Box::pin(async move {
                lr_c.fetch_add(1, Ordering::SeqCst);
                if lf_c.load(Ordering::SeqCst) && req.topics[0].partitions[0].timestamp == -1 {
                    let mut resp = ListOffsetsResponse::default();
                    let mut lt = ListOffsetsTopicResponse::default();
                    lt.name = TopicName(StrBytes::from_static_str(TOPIC));
                    let mut lp = ListOffsetsPartitionResponse::default();
                    lp.partition_index = PARTITION_ID;
                    lp.error_code = ResponseError::NotLeaderOrFollower.code();
                    lt.partitions = vec![lp];
                    resp.topics = vec![lt];
                    HookAction::Respond(resp)
                } else {
                    HookAction::PassThrough
                }
            })
        }));

        produce_record_to_cluster(&cluster, TOPIC, PARTITION_ID, b"record-1", Some(1));
        produce_record_to_cluster(&cluster, TOPIC, PARTITION_ID, b"record-2", Some(1));

        let dir = tempfile::tempdir().unwrap();
        let reg = Registry::new();
        let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        cfg.target_consumer_lag_at_startup = GoDuration::from_secs(1);
        cfg.max_consumer_lag_at_startup = GoDuration::from_secs(2);

        let reader = Arc::new(
            SingleClusterPartitionReader::new(
                cfg,
                PARTITION_ID,
                "test-group",
                Some(dir.path().join("offset.json")),
                Arc::new(FnConsumer(|_| async { Ok(()) })),
                Some(&reg),
            )
            .unwrap(),
        );
        reader.start_async().await;

        let dl = Instant::now() + Duration::from_secs(5);
        while lo_reqs.load(Ordering::SeqCst) <= 2 {
            assert!(Instant::now() < dl);
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(reader.state(), ReaderState::Starting);

        lo_fail.store(false, Ordering::SeqCst);
        let dl = Instant::now() + Duration::from_secs(5);
        while reader.state() != ReaderState::Running {
            assert!(Instant::now() < dl);
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(
            get_gauge_value(
                &reg,
                "cortex_ingest_storage_reader_last_consumed_offset",
                &[("partition", "1")]
            ),
            Some(1.0)
        );
        reader.stop().await;
        cluster.close();
    }

    // 5. "should consume partition from end if position=end, and skip honoring target max lag"
    for file_enforced in [false, true] {
        let cluster = FakeKafkaCluster::start(PARTITION_ID + 1, TOPIC).await;
        let fetch_reqs = Arc::new(AtomicI64::new(0));
        let fetch_fail = Arc::new(AtomicBool::new(true));
        let fr_c = Arc::clone(&fetch_reqs);
        let ff_c = Arc::clone(&fetch_fail);
        *cluster.hooks.fetch.lock() = Some(Arc::new(move |_| {
            let fr_c = Arc::clone(&fr_c);
            let ff_c = Arc::clone(&ff_c);
            Box::pin(async move {
                fr_c.fetch_add(1, Ordering::SeqCst);
                if ff_c.load(Ordering::SeqCst) {
                    HookAction::Respond(FetchResponse::default())
                } else {
                    HookAction::PassThrough
                }
            })
        }));

        produce_record_to_cluster(&cluster, TOPIC, PARTITION_ID, b"record-1", Some(1));
        produce_record_to_cluster(&cluster, TOPIC, PARTITION_ID, b"record-2", Some(1));

        let dir = tempfile::tempdir().unwrap();
        let reg = Registry::new();
        let consumed = Arc::new(SyncMutex::new(Vec::<String>::new()));
        let cons_c = Arc::clone(&consumed);
        let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        cfg.consumer_group_offset_commit_file_enforced = file_enforced;
        cfg.consume_from_position_at_startup = "end".to_string();
        cfg.target_consumer_lag_at_startup = GoDuration::from_secs(1);
        cfg.max_consumer_lag_at_startup = GoDuration::from_secs(1);

        let reader = SingleClusterPartitionReader::new(
            cfg,
            PARTITION_ID,
            "test-group",
            Some(dir.path().join("offset.json")),
            Arc::new(FnConsumer(move |recs: Vec<FetchedRecord>| {
                let cons_c = Arc::clone(&cons_c);
                async move {
                    let mut g = cons_c.lock();
                    for r in recs {
                        g.push(String::from_utf8_lossy(&r.value).into_owned());
                    }
                    Ok(())
                }
            })),
            Some(&reg),
        )
        .unwrap();
        reader.start().await.unwrap();
        assert_eq!(reader.state(), ReaderState::Running);

        fetch_fail.store(false, Ordering::SeqCst);
        let dl = Instant::now() + Duration::from_secs(5);
        while fetch_reqs.load(Ordering::SeqCst) == 0 {
            assert!(Instant::now() < dl);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        produce_record_to_cluster(&cluster, TOPIC, PARTITION_ID, b"record-3", Some(1));

        let dl = Instant::now() + Duration::from_secs(5);
        while consumed.lock().is_empty() {
            assert!(Instant::now() < dl);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(*consumed.lock(), vec!["record-3".to_string()]);
        assert_eq!(
            get_gauge_value(
                &reg,
                "cortex_ingest_storage_reader_last_consumed_offset",
                &[("partition", "1")]
            ),
            Some(2.0)
        );
        reader.stop().await;
        cluster.close();
    }

    // 6. "should consume partition from start if position=start, and wait until target lag is honored" (2 runs)
    {
        let cluster = FakeKafkaCluster::start(PARTITION_ID + 1, TOPIC).await;
        produce_record_to_cluster(&cluster, TOPIC, PARTITION_ID, b"record-1", Some(1));
        produce_record_to_cluster(&cluster, TOPIC, PARTITION_ID, b"record-2", Some(1));

        for _run in 0..2 {
            let dir = tempfile::tempdir().unwrap();
            let reg = Registry::new();
            let consumed = Arc::new(SyncMutex::new(Vec::<String>::new()));
            let cons_c = Arc::clone(&consumed);
            let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
            cfg.consume_from_position_at_startup = "start".to_string();
            cfg.target_consumer_lag_at_startup = GoDuration::from_secs(1);
            cfg.max_consumer_lag_at_startup = GoDuration::from_secs(2);

            let reader = SingleClusterPartitionReader::new(
                cfg,
                PARTITION_ID,
                "test-group",
                Some(dir.path().join("offset.json")),
                Arc::new(FnConsumer(move |recs: Vec<FetchedRecord>| {
                    let cons_c = Arc::clone(&cons_c);
                    async move {
                        let mut g = cons_c.lock();
                        for r in recs {
                            g.push(String::from_utf8_lossy(&r.value).into_owned());
                        }
                        Ok(())
                    }
                })),
                Some(&reg),
            )
            .unwrap();
            reader.start().await.unwrap();
            assert_eq!(
                *consumed.lock(),
                vec!["record-1".to_string(), "record-2".to_string()]
            );
            assert_eq!(
                get_gauge_value(
                    &reg,
                    "cortex_ingest_storage_reader_last_consumed_offset",
                    &[("partition", "1")]
                ),
                Some(1.0)
            );
            reader.stop().await;
        }
        cluster.close();
    }

    // 7. "should consume partition from start if position=start ... and then consume some records after lag is honored"
    {
        let cluster = FakeKafkaCluster::start(PARTITION_ID + 1, TOPIC).await;
        produce_record_to_cluster(&cluster, TOPIC, PARTITION_ID, b"record-1", Some(1));
        produce_record_to_cluster(&cluster, TOPIC, PARTITION_ID, b"record-2", Some(1));

        let dir = tempfile::tempdir().unwrap();
        let reg = Registry::new();
        let consumed = Arc::new(SyncMutex::new(Vec::<String>::new()));
        let cons_c = Arc::clone(&consumed);
        let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        cfg.consume_from_position_at_startup = "start".to_string();
        cfg.target_consumer_lag_at_startup = GoDuration::from_secs(1);
        cfg.max_consumer_lag_at_startup = GoDuration::from_secs(2);

        let reader = SingleClusterPartitionReader::new(
            cfg,
            PARTITION_ID,
            "test-group",
            Some(dir.path().join("offset.json")),
            Arc::new(FnConsumer(move |recs: Vec<FetchedRecord>| {
                let cons_c = Arc::clone(&cons_c);
                async move {
                    let mut g = cons_c.lock();
                    for r in recs {
                        g.push(String::from_utf8_lossy(&r.value).into_owned());
                    }
                    Ok(())
                }
            })),
            Some(&reg),
        )
        .unwrap();
        reader.start().await.unwrap();
        assert_eq!(
            *consumed.lock(),
            vec!["record-1".to_string(), "record-2".to_string()]
        );

        produce_record_to_cluster(&cluster, TOPIC, PARTITION_ID, b"record-3", Some(1));
        produce_record_to_cluster(&cluster, TOPIC, PARTITION_ID, b"record-4", Some(1));
        let dl = Instant::now() + Duration::from_secs(5);
        while consumed.lock().len() < 4 {
            assert!(Instant::now() < dl);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            *consumed.lock(),
            vec!["record-1", "record-2", "record-3", "record-4"]
        );
        assert_eq!(
            get_gauge_value(
                &reg,
                "cortex_ingest_storage_reader_last_consumed_offset",
                &[("partition", "1")]
            ),
            Some(3.0)
        );
        reader.stop().await;
        cluster.close();
    }

    // 8. "should consume partition from the timestamp if position=timestamp, and wait until target lag is honored"
    {
        let cluster = FakeKafkaCluster::start(PARTITION_ID + 1, TOPIC).await;
        let t0 = now_ms() - 5_000;
        cluster.produce_direct(
            TOPIC,
            PARTITION_ID,
            None,
            Some(Bytes::from_static(b"record-1")),
            vec![],
            Some(t0),
        );
        cluster.produce_direct(
            TOPIC,
            PARTITION_ID,
            None,
            Some(Bytes::from_static(b"record-2")),
            vec![],
            Some(t0),
        );
        let consume_from_ts = t0 + 2_000;
        cluster.produce_direct(
            TOPIC,
            PARTITION_ID,
            None,
            Some(Bytes::from_static(b"record-3")),
            vec![],
            Some(t0 + 3_000),
        );
        cluster.produce_direct(
            TOPIC,
            PARTITION_ID,
            None,
            Some(Bytes::from_static(b"record-4")),
            vec![],
            Some(t0 + 4_000),
        );

        let dir = tempfile::tempdir().unwrap();
        let reg = Registry::new();
        let consumed = Arc::new(SyncMutex::new(Vec::<String>::new()));
        let cons_c = Arc::clone(&consumed);
        let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        cfg.consume_from_position_at_startup = "timestamp".to_string();
        cfg.consume_from_timestamp_at_startup = consume_from_ts;
        cfg.target_consumer_lag_at_startup = GoDuration::from_secs(1);
        cfg.max_consumer_lag_at_startup = GoDuration::from_secs(2);

        let reader = SingleClusterPartitionReader::new(
            cfg,
            PARTITION_ID,
            "test-group",
            Some(dir.path().join("offset.json")),
            Arc::new(FnConsumer(move |recs: Vec<FetchedRecord>| {
                let cons_c = Arc::clone(&cons_c);
                async move {
                    let mut g = cons_c.lock();
                    for r in recs {
                        g.push(String::from_utf8_lossy(&r.value).into_owned());
                    }
                    Ok(())
                }
            })),
            Some(&reg),
        )
        .unwrap();
        reader.start().await.unwrap();
        assert_eq!(*consumed.lock(), vec!["record-3", "record-4"]);
        assert_eq!(
            get_gauge_value(
                &reg,
                "cortex_ingest_storage_reader_last_consumed_offset",
                &[("partition", "1")]
            ),
            Some(3.0)
        );
        reader.stop().await;
        cluster.close();
    }

    // 9. "should consume partition from last committed offset if position=last-offset, and wait until target lag is honored"
    {
        let cluster = FakeKafkaCluster::start(PARTITION_ID + 1, TOPIC).await;
        let dir = tempfile::tempdir().unwrap();
        let off_path = dir.path().join("offset.json");
        for run in 1..=2 {
            produce_record_to_cluster(
                &cluster,
                TOPIC,
                PARTITION_ID,
                format!("record-{run}").as_bytes(),
                Some(1),
            );
            let reg = Registry::new();
            let consumed = Arc::new(SyncMutex::new(Vec::<String>::new()));
            let cons_c = Arc::clone(&consumed);
            let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
            cfg.consume_from_position_at_startup = "last-offset".to_string();
            cfg.target_consumer_lag_at_startup = GoDuration::from_secs(1);
            cfg.max_consumer_lag_at_startup = GoDuration::from_secs(2);

            let reader = SingleClusterPartitionReader::new(
                cfg,
                PARTITION_ID,
                "test-group",
                Some(off_path.clone()),
                Arc::new(FnConsumer(move |recs: Vec<FetchedRecord>| {
                    let cons_c = Arc::clone(&cons_c);
                    async move {
                        let mut g = cons_c.lock();
                        for r in recs {
                            g.push(String::from_utf8_lossy(&r.value).into_owned());
                        }
                        Ok(())
                    }
                })),
                Some(&reg),
            )
            .unwrap();
            reader.start().await.unwrap();
            assert_eq!(*consumed.lock(), vec![format!("record-{run}")]);
            reader.stop().await;
        }
        cluster.close();
    }

    // 10. "should consume partition from last committed offset ... and wait until max lag is honored if can't honor target lag"
    {
        let cluster = FakeKafkaCluster::start(PARTITION_ID + 1, TOPIC).await;
        let target_lag = Duration::from_millis(80);
        let max_lag = Duration::from_millis(400);

        *cluster.hooks.list_offsets.lock() = Some(Arc::new(move |req| {
            Box::pin(async move {
                if !req.topics.is_empty()
                    && !req.topics[0].partitions.is_empty()
                    && req.topics[0].partitions[0].timestamp == -1
                {
                    tokio::time::sleep(Duration::from_millis(120)).await;
                }
                HookAction::PassThrough
            })
        }));

        produce_record_to_cluster(&cluster, TOPIC, PARTITION_ID, b"record-1", Some(1));
        let stop_prod = Arc::new(AtomicBool::new(false));
        let sp_c = Arc::clone(&stop_prod);
        let cl_c = Arc::clone(&cluster);
        let prod_h = tokio::spawn(async move {
            let mut i = 2;
            while !sp_c.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(40)).await;
                produce_record_to_cluster(
                    &cl_c,
                    TOPIC,
                    PARTITION_ID,
                    format!("record-{i}").as_bytes(),
                    Some(1),
                );
                i += 1;
            }
        });

        let dir = tempfile::tempdir().unwrap();
        let logs = Arc::new(SyncMutex::new(Vec::<String>::new()));
        let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        cfg.consume_from_position_at_startup = "last-offset".to_string();
        cfg.target_consumer_lag_at_startup = GoDuration::from_nanos(target_lag.as_nanos() as i64);
        cfg.max_consumer_lag_at_startup = GoDuration::from_nanos(max_lag.as_nanos() as i64);

        let reader = SingleClusterPartitionReader::new(
            cfg,
            PARTITION_ID,
            "test-group",
            Some(dir.path().join("offset.json")),
            Arc::new(FnConsumer(|_| async { Ok(()) })),
            None,
        )
        .unwrap()
        .with_log_sink(Arc::clone(&logs));

        reader.start().await.unwrap();
        assert_eq!(reader.state(), ReaderState::Running);
        assert!(logs
            .lock()
            .join("\n")
            .contains("lower than configured max consumer lag but higher than target consumer lag"));

        stop_prod.store(true, Ordering::Relaxed);
        let _ = prod_h.await;
        reader.stop().await;
        cluster.close();
    }

    // 11. "should not wait indefinitely if context is cancelled while fetching last produced offset"
    {
        let cluster = FakeKafkaCluster::start(PARTITION_ID + 1, TOPIC).await;
        let lo_reqs = Arc::new(AtomicI64::new(0));
        let lr_c = Arc::clone(&lo_reqs);
        *cluster.hooks.list_offsets.lock() = Some(Arc::new(move |_| {
            let lr_c = Arc::clone(&lr_c);
            Box::pin(async move {
                lr_c.fetch_add(1, Ordering::SeqCst);
                let mut resp = ListOffsetsResponse::default();
                let mut lt = ListOffsetsTopicResponse::default();
                lt.name = TopicName(StrBytes::from_static_str(TOPIC));
                let mut lp = ListOffsetsPartitionResponse::default();
                lp.partition_index = PARTITION_ID;
                lp.error_code = ResponseError::NotLeaderOrFollower.code();
                lt.partitions = vec![lp];
                resp.topics = vec![lt];
                HookAction::Respond(resp)
            })
        }));

        let dir = tempfile::tempdir().unwrap();
        let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        cfg.target_consumer_lag_at_startup = GoDuration::from_secs(1);
        cfg.max_consumer_lag_at_startup = GoDuration::from_secs(1);

        let reader = Arc::new(
            SingleClusterPartitionReader::new(
                cfg,
                PARTITION_ID,
                "test-group",
                Some(dir.path().join("offset.json")),
                Arc::new(FnConsumer(|_| async { Ok(()) })),
                None,
            )
            .unwrap(),
        );
        reader.start_async().await;
        let dl = Instant::now() + Duration::from_secs(5);
        while lo_reqs.load(Ordering::SeqCst) < 2 {
            assert!(Instant::now() < dl);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(reader.state(), ReaderState::Starting);
        reader.stop().await;
        assert_eq!(reader.state(), ReaderState::Failed);
        cluster.close();
    }

    // 12. "should not wait indefinitely if context is cancelled while fetching records"
    {
        let cluster = FakeKafkaCluster::start(PARTITION_ID + 1, TOPIC).await;
        produce_record_to_cluster(&cluster, TOPIC, PARTITION_ID, b"record-1", Some(1));
        let fetch_reqs = Arc::new(AtomicI64::new(0));
        let fr_c = Arc::clone(&fetch_reqs);
        *cluster.hooks.fetch.lock() = Some(Arc::new(move |_| {
            let fr_c = Arc::clone(&fr_c);
            Box::pin(async move {
                fr_c.fetch_add(1, Ordering::SeqCst);
                HookAction::Respond(FetchResponse::default())
            })
        }));

        let dir = tempfile::tempdir().unwrap();
        let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        cfg.target_consumer_lag_at_startup = GoDuration::from_secs(1);
        cfg.max_consumer_lag_at_startup = GoDuration::from_secs(1);

        let reader = Arc::new(
            SingleClusterPartitionReader::new(
                cfg,
                PARTITION_ID,
                "test-group",
                Some(dir.path().join("offset.json")),
                Arc::new(FnConsumer(|_| async { Ok(()) })),
                None,
            )
            .unwrap(),
        );
        reader.start_async().await;
        let dl = Instant::now() + Duration::from_secs(5);
        while fetch_reqs.load(Ordering::SeqCst) < 1 {
            assert!(Instant::now() < dl);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(reader.state(), ReaderState::Starting);
        reader.stop().await;
        assert_eq!(reader.state(), ReaderState::Failed);
        cluster.close();
    }

    // 13. "should not wait indefinitely if there are no records to consume from Kafka but partition start offset is > 0"
    for pos in ["start", "end", "timestamp", "last-offset"] {
        let cluster = FakeKafkaCluster::start(PARTITION_ID + 1, TOPIC).await;
        produce_record_to_cluster(&cluster, TOPIC, PARTITION_ID, b"record-1", Some(1));
        produce_record_to_cluster(&cluster, TOPIC, PARTITION_ID, b"record-2", Some(1));
        let end_off = cluster.get_end_offset(TOPIC, PARTITION_ID);
        cluster.advance_partition_start(TOPIC, PARTITION_ID, end_off);

        let dir = tempfile::tempdir().unwrap();
        let reg = Registry::new();
        let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        cfg.consume_from_position_at_startup = pos.to_string();
        cfg.consume_from_timestamp_at_startup = if pos == "timestamp" { now_ms() } else { 0 };
        cfg.target_consumer_lag_at_startup = GoDuration::from_secs(1);
        cfg.max_consumer_lag_at_startup = GoDuration::from_secs(1);

        let reader = SingleClusterPartitionReader::new(
            cfg,
            PARTITION_ID,
            "test-group",
            Some(dir.path().join("offset.json")),
            Arc::new(FnConsumer(|_| async { Ok(()) })),
            Some(&reg),
        )
        .unwrap();
        reader.start().await.unwrap();
        assert_eq!(reader.state(), ReaderState::Running);
        assert_eq!(
            get_gauge_value(
                &reg,
                "cortex_ingest_storage_reader_last_consumed_offset",
                &[("partition", "1")]
            ),
            Some(-1.0)
        );
        reader.stop().await;
        cluster.close();
    }
}

// Upstream: pkg/storage/ingest/reader_test.go::TestSingleClusterPartitionReader_ShouldNotBufferRecordsInTheKafkaClientWhenDone
//
// Upstream relies on synctest to have the reader fetch record-3 and record-4 separately; here
// record-4 is produced once the consumer is blocked on record-3.
#[tokio::test]
async fn test_single_cluster_partition_reader_should_not_buffer_records_in_the_kafka_client_when_done()
{
    const TOPIC: &str = "test";
    const PARTITION_ID: i32 = 1;
    const MAX_BUFFERED_BYTES: i64 = 2_000_000;

    // (fetch concurrency, expected buffered records, expected buffered bytes, expected
    // records buffered in the client)
    let variants: [(i64, f64, f64, f64); 3] = [
        // without concurrency: kgo buffers the fetch (userSize = 8 value + 11 header bytes).
        (0, 1.0, 19.0, 1.0),
        // only one fetcher is active (no fetch over the HWM); it fetches 1MB.
        (2, 1.0, 1_000_000.0, 0.0),
        // one fetcher fetching 500KB.
        (4, 1.0, 500_000.0, 0.0),
    ];

    for file_enforced in [false, true] {
        for (conc, exp_records, exp_bytes, exp_client_records) in variants {
            let cluster = FakeKafkaCluster::start(PARTITION_ID + 1, TOPIC).await;
            produce_record_to_cluster(&cluster, TOPIC, PARTITION_ID, b"record-1", Some(1));
            produce_record_to_cluster(&cluster, TOPIC, PARTITION_ID, b"record-2", Some(1));

            let blocked = Arc::new(AtomicBool::new(false));
            let waiting = Arc::new(AtomicBool::new(false));
            let consumed = Arc::new(SyncMutex::new(Vec::<String>::new()));
            let blk_c = Arc::clone(&blocked);
            let wait_c = Arc::clone(&waiting);
            let cons_c = Arc::clone(&consumed);

            let dir = tempfile::tempdir().unwrap();
            let reg = Registry::new();
            let logs = Arc::new(SyncMutex::new(Vec::<String>::new()));
            let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
            cfg.consume_from_position_at_startup = "start".to_string();
            cfg.target_consumer_lag_at_startup = GoDuration::from_secs(1);
            cfg.max_consumer_lag_at_startup = GoDuration::from_secs(2);
            cfg.max_buffered_bytes = MAX_BUFFERED_BYTES;
            cfg.fetch_concurrency_max = conc;
            cfg.consumer_group_offset_commit_file_enforced = file_enforced;

            let mut reader = SingleClusterPartitionReader::new(
                cfg,
                PARTITION_ID,
                "test-group",
                Some(dir.path().join("offset.json")),
                Arc::new(FnConsumer(move |recs: Vec<FetchedRecord>| {
                    let blk_c = Arc::clone(&blk_c);
                    let wait_c = Arc::clone(&wait_c);
                    let cons_c = Arc::clone(&cons_c);
                    async move {
                        while blk_c.load(Ordering::SeqCst) {
                            wait_c.store(true, Ordering::SeqCst);
                            tokio::time::sleep(Duration::from_millis(20)).await;
                        }
                        let mut g = cons_c.lock();
                        for r in recs {
                            g.push(String::from_utf8_lossy(&r.value).into_owned());
                        }
                        Ok(())
                    }
                })),
                Some(&reg),
            )
            .unwrap()
            .with_log_sink(Arc::clone(&logs));
            if file_enforced {
                reader = reader.with_max_replay_period(Duration::from_secs(3600));
            }
            reader.start().await.unwrap();
            assert!(logs.lock().iter().any(|l| l
                == "partition reader consumed partition and current lag is lower than configured target consumer lag"));
            assert_eq!(*consumed.lock(), vec!["record-1", "record-2"]);

            // Give the client some time to eventually read and buffer records (not expected).
            tokio::time::sleep(Duration::from_millis(300)).await;
            let gauges = |reg: &Registry| {
                (
                    get_gauge_value(reg, "cortex_ingest_storage_reader_last_consumed_offset", &[("partition", "1")]),
                    get_gauge_value(reg, "cortex_ingest_storage_reader_buffered_fetch_records_total", &[("component", "partition-reader")]),
                    get_gauge_value(reg, "cortex_ingest_storage_reader_buffered_fetched_records", &[]),
                    get_gauge_value(reg, "cortex_ingest_storage_reader_buffered_fetched_bytes", &[]),
                )
            };
            let (last, client_recs, recs, _) = gauges(&reg);
            assert_eq!(last, Some(1.0));
            assert_eq!(client_recs, Some(0.0));
            assert_eq!(recs, Some(0.0));

            // Make the consumer block on the next consumption and produce more records.
            blocked.store(true, Ordering::SeqCst);
            produce_record_to_cluster(&cluster, TOPIC, PARTITION_ID, b"record-3", Some(1));
            let dl = Instant::now() + Duration::from_secs(5);
            while !waiting.load(Ordering::SeqCst) {
                assert!(Instant::now() < dl, "consumer never received record-3 (conc {conc})");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            produce_record_to_cluster(&cluster, TOPIC, PARTITION_ID, b"record-4", Some(1));

            // We expect to have some records buffered.
            let dl = Instant::now() + Duration::from_secs(5);
            loop {
                let (last, client_recs, recs, bytes) = gauges(&reg);
                if last == Some(1.0)
                    && client_recs == Some(exp_client_records)
                    && recs == Some(exp_records)
                    && bytes == Some(exp_bytes)
                {
                    break;
                }
                assert!(
                    Instant::now() < dl,
                    "conc {conc}: got last={last:?} client={client_recs:?} records={recs:?} bytes={bytes:?}"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            assert_eq!(reader.buffered_records(), exp_records as i64);

            blocked.store(false, Ordering::SeqCst);
            let dl = Instant::now() + Duration::from_secs(5);
            while consumed.lock().len() < 4 {
                assert!(Instant::now() < dl);
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            assert_eq!(
                *consumed.lock(),
                vec!["record-1", "record-2", "record-3", "record-4"]
            );
            tokio::time::sleep(Duration::from_millis(300)).await;
            let (last, client_recs, recs, _) = gauges(&reg);
            assert_eq!(last, Some(3.0));
            assert_eq!(client_recs, Some(0.0));
            assert_eq!(recs, Some(0.0));

            reader.stop().await;
            cluster.close();
        }
    }
}

// Upstream: pkg/storage/ingest/reader_test.go::TestSingleClusterPartitionReader_ShouldNotPanicIfBufferedRecordsIsCalledBeforeStarting
#[test]
fn test_single_cluster_partition_reader_should_not_panic_if_buffered_records_is_called_before_starting()
{
    let cfg = create_test_kafka_config("127.0.0.1:9092", "test");
    let dir = tempfile::tempdir().unwrap();
    let reader = SingleClusterPartitionReader::new(
        cfg,
        1,
        "test-group",
        Some(dir.path().join("offset.json")),
        TestConsumer::new(1),
        None,
    )
    .unwrap();
    assert_eq!(reader.buffered_records(), 0);
}

// Upstream: pkg/storage/ingest/reader_test.go::TestSingleClusterPartitionReader_ShouldNotMissRecordsIfFetchRequestContainPartialFailuresWithConcurrentFetcherIsUsed
#[tokio::test]
async fn test_single_cluster_partition_reader_should_not_miss_records_on_partial_failures() {
    const TOPIC: &str = "test-topic";
    const PARTITION_ID: i32 = 1;
    const TOTAL_RECORDS: usize = 60;

    let cluster = FakeKafkaCluster::start(PARTITION_ID + 1, TOPIC).await;
    for i in 0..TOTAL_RECORDS {
        produce_record_to_cluster(
            &cluster,
            TOPIC,
            PARTITION_ID,
            format!("record-{i:05}").as_bytes(),
            Some(1),
        );
    }

    let req_num = Arc::new(AtomicI64::new(0));
    let cl_c = Arc::clone(&cluster);
    let rn_c = Arc::clone(&req_num);
    *cluster.hooks.fetch.lock() = Some(Arc::new(move |req| {
        let cl_c = Arc::clone(&cl_c);
        let rn_c = Arc::clone(&rn_c);
        Box::pin(async move {
            let n = rn_c.fetch_add(1, Ordering::Relaxed);
            let off = req.topics[0].partitions[0].fetch_offset;
            if n % 4 == 1 {
                let mut resp = FetchResponse::default();
                resp.error_code = ResponseError::UnknownServerError.code();
                let mut ft = FetchableTopicResponse::default();
                ft.topic = TopicName(StrBytes::from_static_str(TOPIC));
                let mut pd = PartitionData::default();
                pd.partition_index = PARTITION_ID;
                pd.error_code = ResponseError::UnknownServerError.code();
                ft.partitions = vec![pd];
                resp.responses = vec![ft];
                return HookAction::Respond(resp);
            }
            if let Some(resp) = cl_c.fetch_direct_encoded(TOPIC, PARTITION_ID, off) {
                return HookAction::Respond(resp);
            }
            HookAction::PassThrough
        })
    }));

    let dir = tempfile::tempdir().unwrap();
    let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
    cfg.consume_from_position_at_startup = "start".to_string();
    cfg.fetch_concurrency_max = 5;
    cfg.target_consumer_lag_at_startup = GoDuration::from_secs(1);
    cfg.max_consumer_lag_at_startup = GoDuration::from_secs(2);

    let seen_ids = Arc::new(SyncMutex::new(HashSet::<usize>::new()));
    let si_c = Arc::clone(&seen_ids);
    let reader = SingleClusterPartitionReader::new(
        cfg,
        PARTITION_ID,
        "test-group",
        Some(dir.path().join("offset.json")),
        Arc::new(FnConsumer(move |recs: Vec<FetchedRecord>| {
            let si_c = Arc::clone(&si_c);
            async move {
                let mut g = si_c.lock();
                for r in recs {
                    let s = std::str::from_utf8(&r.value).unwrap();
                    let id: usize = s[7..12].parse().unwrap();
                    g.insert(id);
                }
                Ok(())
            }
        })),
        None,
    )
    .unwrap();

    reader.start().await.unwrap();
    let dl = Instant::now() + Duration::from_secs(10);
    while seen_ids.lock().len() < TOTAL_RECORDS {
        assert!(Instant::now() < dl);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    for i in 0..TOTAL_RECORDS {
        assert!(seen_ids.lock().contains(&i), "missing record {i}");
    }

    reader.stop().await;
    cluster.close();
}

// Upstream: pkg/storage/ingest/reader_test.go::TestSingleClusterPartitionReader_ShouldNotMissRecordsIfKafkaReturnsAFetchBothWithAnErrorAndSomeRecords
#[tokio::test]
async fn test_single_cluster_partition_reader_should_not_miss_records_if_fetch_has_error_and_records()
{
    const TOPIC: &str = "test";
    const PARTITION_ID: i32 = 1;
    const TOTAL_RECORDS: usize = 20;

    let error_offsets_variants: Vec<Vec<i64>> = vec![
        vec![0],
        vec![(TOTAL_RECORDS / 2) as i64],
        vec![(TOTAL_RECORDS - 1) as i64],
        vec![1, (TOTAL_RECORDS / 2) as i64, (TOTAL_RECORDS - 1) as i64],
    ];

    for error_offsets in error_offsets_variants {
        for fetch_conc in [0, 2] {
            let cluster = FakeKafkaCluster::start(PARTITION_ID + 1, TOPIC).await;
            for i in 0..TOTAL_RECORDS {
                produce_record_to_cluster(
                    &cluster,
                    TOPIC,
                    PARTITION_ID,
                    format!("record-{i:05}").as_bytes(),
                    Some(1),
                );
            }

            let err_set: HashSet<i64> = error_offsets.iter().copied().collect();
            let injected = Arc::new(SyncMutex::new(HashSet::<i64>::new()));
            let cl_c = Arc::clone(&cluster);
            let inj_c = Arc::clone(&injected);
            *cluster.hooks.fetch.lock() = Some(Arc::new(move |req| {
                let cl_c = Arc::clone(&cl_c);
                let inj_c = Arc::clone(&inj_c);
                let err_set = err_set.clone();
                Box::pin(async move {
                    let off = req.topics[0].partitions[0].fetch_offset;
                    if let Some(mut resp) = cl_c.fetch_direct_encoded(TOPIC, PARTITION_ID, off) {
                        if err_set.contains(&off) && inj_c.lock().insert(off) {
                            resp.responses[0].partitions[0].error_code =
                                ResponseError::UnknownServerError.code();
                        }
                        return HookAction::Respond(resp);
                    }
                    HookAction::PassThrough
                })
            }));

            let dir = tempfile::tempdir().unwrap();
            let reg = Registry::new();
            let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
            cfg.consume_from_position_at_startup = "start".to_string();
            cfg.fetch_concurrency_max = fetch_conc;
            cfg.target_consumer_lag_at_startup = GoDuration::from_secs(1);
            cfg.max_consumer_lag_at_startup = GoDuration::from_secs(2);

            let total_consumed = Arc::new(AtomicI64::new(0));
            let unique_ids = Arc::new(SyncMutex::new(HashSet::<usize>::new()));
            let tc_c = Arc::clone(&total_consumed);
            let ui_c = Arc::clone(&unique_ids);

            let reader = SingleClusterPartitionReader::new(
                cfg,
                PARTITION_ID,
                "test-group",
                Some(dir.path().join("offset.json")),
                Arc::new(FnConsumer(move |recs: Vec<FetchedRecord>| {
                    let tc_c = Arc::clone(&tc_c);
                    let ui_c = Arc::clone(&ui_c);
                    async move {
                        let mut g = ui_c.lock();
                        for r in recs {
                            tc_c.fetch_add(1, Ordering::SeqCst);
                            let s = std::str::from_utf8(&r.value).unwrap();
                            let id: usize = s[7..12].parse().unwrap();
                            g.insert(id);
                        }
                        Ok(())
                    }
                })),
                Some(&reg),
            )
            .unwrap();

            reader.start().await.unwrap();
            let dl = Instant::now() + Duration::from_secs(10);
            while unique_ids.lock().len() < TOTAL_RECORDS {
                assert!(Instant::now() < dl);
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            assert_eq!(total_consumed.load(Ordering::SeqCst), TOTAL_RECORDS as i64);
            assert_eq!(injected.lock().len(), error_offsets.len());
            assert_eq!(
                get_gauge_value(
                    &reg,
                    "cortex_ingest_storage_reader_last_consumed_offset",
                    &[("partition", "1")]
                ),
                Some((TOTAL_RECORDS - 1) as f64)
            );

            reader.stop().await;
            cluster.close();
        }
    }
}

// Upstream: pkg/storage/ingest/reader_test.go::TestSingleClusterPartitionReader_fetchLastCommittedOffset
#[tokio::test]
async fn test_single_cluster_partition_reader_fetch_last_committed_offset() {
    const TOPIC: &str = "test";
    const PARTITION_ID: i32 = 1;

    // 1. "should return 'not exists' if Kafka returns GroupIDNotFound"
    {
        let cluster = FakeKafkaCluster::start_with_opts(
            PARTITION_ID + 1,
            TOPIC,
            helpers::ClusterSasl::None,
            false,
        )
        .await;
        let dir = tempfile::tempdir().unwrap();
        let cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        let reader = SingleClusterPartitionReader::new(
            cfg,
            PARTITION_ID,
            "test-group",
            Some(dir.path().join("offset.json")),
            TestConsumer::new(1),
            None,
        )
        .unwrap();

        *cluster.hooks.offset_fetch.lock() = Some(Arc::new(|_| {
            Box::pin(async {
                let mut resp = OffsetFetchResponse::default();
                resp.error_code = ResponseError::GroupIdNotFound.code();
                HookAction::Respond(resp)
            })
        }));

        assert_eq!(reader.fetch_last_committed_offset().await.unwrap(), None);
        cluster.close();
    }

    // 2. "should return 'not exists' if Kafka returns no offsets for the requested partition"
    {
        let cluster = FakeKafkaCluster::start_with_opts(
            PARTITION_ID + 1,
            TOPIC,
            helpers::ClusterSasl::None,
            false,
        )
        .await;
        let dir = tempfile::tempdir().unwrap();
        let cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        let reader = SingleClusterPartitionReader::new(
            cfg,
            PARTITION_ID,
            "test-group",
            Some(dir.path().join("offset.json")),
            TestConsumer::new(1),
            None,
        )
        .unwrap();

        *cluster.hooks.offset_fetch.lock() = Some(Arc::new(|_| {
            Box::pin(async {
                let mut resp = OffsetFetchResponse::default();
                let mut t = OffsetFetchResponseTopic::default();
                t.name = TopicName(StrBytes::from_static_str(TOPIC));
                let mut p = OffsetFetchResponsePartition::default();
                p.partition_index = PARTITION_ID + 1;
                p.committed_offset = 456;
                t.partitions = vec![p];
                resp.topics = vec![t];
                HookAction::Respond(resp)
            })
        }));

        assert_eq!(reader.fetch_last_committed_offset().await.unwrap(), None);
        cluster.close();
    }

    // 3. "should return the committed to Kafka offset"
    {
        let cluster = FakeKafkaCluster::start_with_opts(
            PARTITION_ID + 1,
            TOPIC,
            helpers::ClusterSasl::None,
            false,
        )
        .await;
        let dir = tempfile::tempdir().unwrap();
        let cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        let reader = SingleClusterPartitionReader::new(
            cfg,
            PARTITION_ID,
            "test-group",
            Some(dir.path().join("offset.json")),
            TestConsumer::new(1),
            None,
        )
        .unwrap();

        *cluster.hooks.offset_fetch.lock() = Some(Arc::new(|_| {
            Box::pin(async {
                let mut resp = OffsetFetchResponse::default();
                let mut t = OffsetFetchResponseTopic::default();
                t.name = TopicName(StrBytes::from_static_str(TOPIC));
                let mut p1 = OffsetFetchResponsePartition::default();
                p1.partition_index = PARTITION_ID;
                p1.committed_offset = 123;
                let mut p2 = OffsetFetchResponsePartition::default();
                p2.partition_index = PARTITION_ID + 1;
                p2.committed_offset = 456;
                t.partitions = vec![p1, p2];
                resp.topics = vec![t];
                HookAction::Respond(resp)
            })
        }));

        assert_eq!(
            reader.fetch_last_committed_offset().await.unwrap(),
            Some(123)
        );
        cluster.close();
    }
}

// Upstream: pkg/storage/ingest/reader_test.go::TestSingleClusterPartitionReader_getStartOffset_RetentionPeriodFallback
#[tokio::test]
async fn test_single_cluster_partition_reader_get_start_offset_retention_period_fallback() {
    const TOPIC: &str = "test-topic";
    const PARTITION_ID: i32 = 1;

    // 1. "uses retention period to calculate timestamp-based offset when file is missing"
    {
        let cluster = FakeKafkaCluster::start(PARTITION_ID + 1, TOPIC).await;
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        cfg.consume_from_position_at_startup = "last-offset".to_string();
        cfg.consumer_group_offset_commit_file_enforced = true;
        let reader = SingleClusterPartitionReader::new(
            cfg,
            PARTITION_ID,
            "test-group",
            Some(dir.path().join("offset.json")),
            TestConsumer::new(1),
            None,
        )
        .unwrap()
        .with_max_replay_period(Duration::from_secs(3600));

        let (start_off, last_cons) = reader.get_start_offset().await.unwrap();
        assert_eq!(start_off, 0);
        assert_eq!(last_cons, -1);
        cluster.close();
    }

    // 2. "keeps partition end when all records predate the retention period"
    {
        let cluster = FakeKafkaCluster::start(PARTITION_ID + 1, TOPIC).await;
        let old_ts = now_ms() - 2 * 3600 * 1000;
        for i in 0..5 {
            cluster.produce_direct(
                TOPIC,
                PARTITION_ID,
                None,
                Some(Bytes::from(format!("record-{i}"))),
                vec![],
                Some(old_ts),
            );
        }
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        cfg.consume_from_position_at_startup = "last-offset".to_string();
        cfg.consumer_group_offset_commit_file_enforced = true;
        let reader = SingleClusterPartitionReader::new(
            cfg,
            PARTITION_ID,
            "test-group",
            Some(dir.path().join("offset.json")),
            TestConsumer::new(1),
            None,
        )
        .unwrap()
        .with_max_replay_period(Duration::from_secs(3600));

        let (start_off, last_cons) = reader.get_start_offset().await.unwrap();
        assert_eq!(start_off, 5);
        assert_eq!(last_cons, 4);
        cluster.close();
    }

    // 3. "clamps to partition start when timestamp lookup matches no record and resolves ahead of available records"
    {
        let cluster = FakeKafkaCluster::start(PARTITION_ID + 1, TOPIC).await;
        for i in 0..5 {
            produce_record_to_cluster(
                &cluster,
                TOPIC,
                PARTITION_ID,
                format!("record-{i}").as_bytes(),
                Some(1),
            );
        }
        *cluster.hooks.list_offsets.lock() = Some(Arc::new(|req| {
            Box::pin(async move {
                if req.topics[0].partitions[0].timestamp <= 0 {
                    return HookAction::PassThrough;
                }
                let mut resp = ListOffsetsResponse::default();
                let mut lt = ListOffsetsTopicResponse::default();
                lt.name = TopicName(StrBytes::from_static_str(TOPIC));
                let mut lp = ListOffsetsPartitionResponse::default();
                lp.partition_index = PARTITION_ID;
                lp.offset = -1;
                lp.timestamp = -1;
                lt.partitions = vec![lp];
                resp.topics = vec![lt];
                HookAction::Respond(resp)
            })
        }));

        let dir = tempfile::tempdir().unwrap();
        let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        cfg.consume_from_position_at_startup = "last-offset".to_string();
        cfg.consumer_group_offset_commit_file_enforced = true;
        let reader = SingleClusterPartitionReader::new(
            cfg,
            PARTITION_ID,
            "test-group",
            Some(dir.path().join("offset.json")),
            TestConsumer::new(1),
            None,
        )
        .unwrap()
        .with_max_replay_period(Duration::from_secs(3600));

        let (start_off, last_cons) = reader.get_start_offset().await.unwrap();
        assert_eq!(start_off, 0);
        assert_eq!(last_cons, -1);
        cluster.close();
    }

    // 4. "uses a recovered offset when retrying an inconsistent timestamp lookup"
    {
        let cluster = FakeKafkaCluster::start(PARTITION_ID + 1, TOPIC).await;
        for i in 0..5 {
            produce_record_to_cluster(
                &cluster,
                TOPIC,
                PARTITION_ID,
                format!("record-{i}").as_bytes(),
                Some(1),
            );
        }
        let ts_lookups = Arc::new(AtomicI32::new(0));
        let tl_c = Arc::clone(&ts_lookups);
        *cluster.hooks.list_offsets.lock() = Some(Arc::new(move |req| {
            let tl_c = Arc::clone(&tl_c);
            Box::pin(async move {
                if req.topics[0].partitions[0].timestamp <= 0
                    || tl_c.fetch_add(1, Ordering::SeqCst) >= 1
                {
                    return HookAction::PassThrough;
                }
                let mut resp = ListOffsetsResponse::default();
                let mut lt = ListOffsetsTopicResponse::default();
                lt.name = TopicName(StrBytes::from_static_str(TOPIC));
                let mut lp = ListOffsetsPartitionResponse::default();
                lp.partition_index = PARTITION_ID;
                lp.offset = -1;
                lp.timestamp = -1;
                lt.partitions = vec![lp];
                resp.topics = vec![lt];
                HookAction::Respond(resp)
            })
        }));

        let dir = tempfile::tempdir().unwrap();
        let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        cfg.consume_from_position_at_startup = "last-offset".to_string();
        cfg.consumer_group_offset_commit_file_enforced = true;
        let reader = SingleClusterPartitionReader::new(
            cfg,
            PARTITION_ID,
            "test-group",
            Some(dir.path().join("offset.json")),
            TestConsumer::new(1),
            None,
        )
        .unwrap()
        .with_max_replay_period(Duration::from_secs(3600));

        let (start_off, last_cons) = reader.get_start_offset().await.unwrap();
        assert_eq!(start_off, 0);
        assert_eq!(last_cons, -1);
        assert_eq!(ts_lookups.load(Ordering::SeqCst), 2);
        cluster.close();
    }

    // 5. "tail probe returns fetch errors"
    {
        let cluster = FakeKafkaCluster::start(PARTITION_ID + 1, TOPIC).await;
        *cluster.hooks.fetch.lock() = Some(Arc::new(|_| {
            Box::pin(async {
                let mut resp = FetchResponse::default();
                let mut ft = FetchableTopicResponse::default();
                ft.topic = TopicName(StrBytes::from_static_str(TOPIC));
                let mut pd = PartitionData::default();
                pd.partition_index = PARTITION_ID;
                pd.error_code = ResponseError::TopicAuthorizationFailed.code();
                ft.partitions = vec![pd];
                resp.responses = vec![ft];
                HookAction::Respond(resp)
            })
        }));

        let dir = tempfile::tempdir().unwrap();
        let cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        let reader = SingleClusterPartitionReader::new(
            cfg,
            PARTITION_ID,
            "test-group",
            Some(dir.path().join("offset.json")),
            TestConsumer::new(1),
            None,
        )
        .unwrap();
        assert!(reader.fetch_record_timestamp_at_offset(0).await.is_err());
        cluster.close();
    }

    // 6. "does not clamp when timestamp lookup matches a record ahead of partition start"
    {
        let cluster = FakeKafkaCluster::start(PARTITION_ID + 1, TOPIC).await;
        for i in 0..5 {
            produce_record_to_cluster(
                &cluster,
                TOPIC,
                PARTITION_ID,
                format!("record-{i}").as_bytes(),
                Some(1),
            );
        }
        let matched_ts = now_ms() - 30 * 60 * 1000;
        *cluster.hooks.list_offsets.lock() = Some(Arc::new(move |req| {
            Box::pin(async move {
                if req.topics[0].partitions[0].timestamp <= 0 {
                    return HookAction::PassThrough;
                }
                let mut resp = ListOffsetsResponse::default();
                let mut lt = ListOffsetsTopicResponse::default();
                lt.name = TopicName(StrBytes::from_static_str(TOPIC));
                let mut lp = ListOffsetsPartitionResponse::default();
                lp.partition_index = PARTITION_ID;
                lp.offset = 3;
                lp.timestamp = matched_ts;
                lt.partitions = vec![lp];
                resp.topics = vec![lt];
                HookAction::Respond(resp)
            })
        }));

        let dir = tempfile::tempdir().unwrap();
        let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        cfg.consume_from_position_at_startup = "last-offset".to_string();
        cfg.consumer_group_offset_commit_file_enforced = true;
        let reader = SingleClusterPartitionReader::new(
            cfg,
            PARTITION_ID,
            "test-group",
            Some(dir.path().join("offset.json")),
            TestConsumer::new(1),
            None,
        )
        .unwrap()
        .with_max_replay_period(Duration::from_secs(3600));

        let (start_off, last_cons) = reader.get_start_offset().await.unwrap();
        assert_eq!(start_off, 3);
        assert_eq!(last_cons, 2);
        cluster.close();
    }

    // 7. "errors when retention period is zero"
    {
        let cluster = FakeKafkaCluster::start(PARTITION_ID + 1, TOPIC).await;
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        cfg.consume_from_position_at_startup = "last-offset".to_string();
        cfg.consumer_group_offset_commit_file_enforced = true;
        let reader = SingleClusterPartitionReader::new(
            cfg,
            PARTITION_ID,
            "test-group",
            Some(dir.path().join("offset.json")),
            TestConsumer::new(1),
            None,
        )
        .unwrap();
        assert!(reader.get_start_offset().await.is_err());
        cluster.close();
    }

    // 8. "file offset takes precedence over retention period"
    {
        let cluster = FakeKafkaCluster::start(PARTITION_ID + 1, TOPIC).await;
        let dir = tempfile::tempdir().unwrap();
        let off_path = dir.path().join("offset");
        OffsetFile::new(&off_path, PARTITION_ID).write(42).unwrap();

        let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        cfg.consume_from_position_at_startup = "last-offset".to_string();
        cfg.consumer_group_offset_commit_file_enforced = true;
        let reader = SingleClusterPartitionReader::new(
            cfg,
            PARTITION_ID,
            "test-group",
            Some(off_path),
            TestConsumer::new(1),
            None,
        )
        .unwrap()
        .with_max_replay_period(Duration::from_secs(3600));

        let (start_off, last_cons) = reader.get_start_offset().await.unwrap();
        assert_eq!(start_off, 43);
        assert_eq!(last_cons, 42);
        cluster.close();
    }

    // 9. "ignores stale file offset when file offset is behind partition start"
    {
        let cluster = FakeKafkaCluster::start(PARTITION_ID + 1, TOPIC).await;
        produce_record_to_cluster(&cluster, TOPIC, PARTITION_ID, b"record-1", Some(1));
        produce_record_to_cluster(&cluster, TOPIC, PARTITION_ID, b"record-2", Some(1));
        let end_off = cluster.get_end_offset(TOPIC, PARTITION_ID);
        cluster.advance_partition_start(TOPIC, PARTITION_ID, end_off);

        let dir = tempfile::tempdir().unwrap();
        let off_path = dir.path().join("offset");
        OffsetFile::new(&off_path, PARTITION_ID).write(0).unwrap();

        let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        cfg.consume_from_position_at_startup = "last-offset".to_string();
        cfg.consumer_group_offset_commit_file_enforced = true;
        let reader = SingleClusterPartitionReader::new(
            cfg,
            PARTITION_ID,
            "test-group",
            Some(off_path),
            TestConsumer::new(1),
            None,
        )
        .unwrap()
        .with_max_replay_period(Duration::from_secs(3600));

        let (start_off, last_cons) = reader.get_start_offset().await.unwrap();
        assert!(start_off >= 2);
        assert!(last_cons == -1 || last_cons == start_off - 1);
        cluster.close();
    }

    // 10. "returns expected values for each ConsumeFromPositionAtStartup mode"
    {
        let cluster = FakeKafkaCluster::start(PARTITION_ID + 1, TOPIC).await;
        let dir = tempfile::tempdir().unwrap();

        for (pos, ts, exp_start, exp_last) in [
            ("start", 0, KAFKA_OFFSET_START, -1),
            ("end", 0, KAFKA_OFFSET_END, -1),
            ("last-offset", 0, KAFKA_OFFSET_START, -1),
        ] {
            let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
            cfg.consume_from_position_at_startup = pos.to_string();
            cfg.consume_from_timestamp_at_startup = ts;
            let reader = SingleClusterPartitionReader::new(
                cfg,
                PARTITION_ID,
                "test-group",
                Some(dir.path().join("offset.json")),
                TestConsumer::new(1),
                None,
            )
            .unwrap();
            let (s, l) = reader.get_start_offset().await.unwrap();
            assert_eq!(s, exp_start);
            assert_eq!(l, exp_last);
        }
        cluster.close();
    }
}

struct TestPreCommitNotifier {
    count: AtomicI32,
    err: Option<String>,
    on_notify: Option<Box<dyn Fn() + Send + Sync>>,
}

impl PreCommitNotifier for TestPreCommitNotifier {
    fn notify_pre_commit(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>> {
        Box::pin(async move {
            self.count.fetch_add(1, Ordering::SeqCst);
            if let Some(ref cb) = self.on_notify {
                cb();
            }
            match self.err {
                Some(ref e) => Err(e.clone()),
                None => Ok(()),
            }
        })
    }
}

struct MockAdmClient {
    on_commit: Option<Box<dyn Fn() + Send + Sync>>,
}

impl AdmClient for MockAdmClient {
    fn commit_offsets(
        &self,
        _group: &str,
        _topic: &str,
        _partition_id: i32,
        offset: i64,
    ) -> Pin<Box<dyn Future<Output = Result<i64, String>> + Send + '_>> {
        Box::pin(async move {
            if let Some(ref cb) = self.on_commit {
                cb();
            }
            Ok(offset)
        })
    }
}

// Upstream: pkg/storage/ingest/reader_test.go::TestPartitionCommitter
#[tokio::test]
async fn test_partition_committer() {
    const TOPIC: &str = "test-topic";
    const GROUP: &str = "test-group";
    const PARTITION_ID: i32 = 1;

    let cluster = FakeKafkaCluster::start_with_opts(
        PARTITION_ID + 1,
        TOPIC,
        helpers::ClusterSasl::None,
        false,
    )
    .await;

    let commit_reqs = Arc::new(AtomicI64::new(0));
    let should_fail = Arc::new(AtomicBool::new(true));
    let cr_c = Arc::clone(&commit_reqs);
    let sf_c = Arc::clone(&should_fail);
    *cluster.hooks.offset_commit.lock() = Some(Arc::new(move |_| {
        let cr_c = Arc::clone(&cr_c);
        let sf_c = Arc::clone(&sf_c);
        Box::pin(async move {
            cr_c.fetch_add(1, Ordering::SeqCst);
            let mut resp = OffsetCommitResponse::default();
            let mut t = OffsetCommitResponseTopic::default();
            t.name = TopicName(StrBytes::from_static_str(TOPIC));
            let mut p = OffsetCommitResponsePartition::default();
            p.partition_index = PARTITION_ID;
            if sf_c.load(Ordering::SeqCst) {
                p.error_code = ResponseError::InvalidCommitOffsetSize.code();
            }
            t.partitions = vec![p];
            resp.topics = vec![t];
            HookAction::Respond(resp)
        })
    }));

    let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
    cfg.consumer_group_offset_commit_interval = GoDuration::from_millis(40);
    let client = Arc::new(KafkaClient::from_config(&cfg).unwrap());
    let adm: Arc<dyn AdmClient> = Arc::new(KafkaAdmClient::new(client));
    let dir = tempfile::tempdir().unwrap();
    let off_file = Arc::new(OffsetFile::new(dir.path().join("offset.json"), PARTITION_ID));
    let reg = Registry::new();

    let committer = Arc::new(PartitionCommitter::new(
        cfg.clone(),
        adm,
        PARTITION_ID,
        GROUP,
        Arc::new(NoOpPreCommitNotifier),
        Some(off_file),
        Some(&reg),
    ));
    committer.start().await;
    committer.enqueue_offset(123);

    let dl = Instant::now() + Duration::from_secs(5);
    while committer.commit_failures_count() < 1.0 {
        assert!(Instant::now() < dl);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    should_fail.store(false, Ordering::SeqCst);
    let dl = Instant::now() + Duration::from_secs(5);
    while committer.last_committed_offset() != 123 {
        assert!(Instant::now() < dl);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert_eq!(
        get_gauge_value(
            &reg,
            "cortex_ingest_storage_reader_last_committed_offset",
            &[("partition", "1")]
        ),
        Some(123.0)
    );
    assert_eq!(committer.commit_failures_count(), 1.0);
    assert!(committer.commit_requests_count() >= 2.0);

    // No further commits when no new offset enqueued
    let expected_reqs = commit_reqs.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(commit_reqs.load(Ordering::SeqCst), expected_reqs);

    committer.stop().await;
    cluster.close();
}

// Upstream: pkg/storage/ingest/reader_test.go::TestPartitionCommitter_commit
#[tokio::test]
async fn test_partition_committer_commit() {
    const TOPIC: &str = "test-topic";
    const GROUP: &str = "test-group";
    const PARTITION_ID: i32 = 1;

    // 1. "should track metrics on successful commit"
    {
        let cluster = FakeKafkaCluster::start(PARTITION_ID + 1, TOPIC).await;
        let cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        let client = Arc::new(KafkaClient::from_config(&cfg).unwrap());
        let adm: Arc<dyn AdmClient> = Arc::new(KafkaAdmClient::new(client));
        let dir = tempfile::tempdir().unwrap();
        let off_file = Arc::new(OffsetFile::new(dir.path().join("offset.json"), PARTITION_ID));
        let reg = Registry::new();

        let committer = PartitionCommitter::new(
            cfg,
            adm,
            PARTITION_ID,
            GROUP,
            Arc::new(NoOpPreCommitNotifier),
            Some(off_file),
            Some(&reg),
        );
        committer.commit(123).await.unwrap();

        assert_eq!(
            get_gauge_value(
                &reg,
                "cortex_ingest_storage_reader_last_committed_offset",
                &[("partition", "1")]
            ),
            Some(123.0)
        );
        assert_eq!(committer.commit_failures_count(), 0.0);
        assert_eq!(committer.commit_requests_count(), 1.0);
        cluster.close();
    }

    // 2. "should track metrics on failed commit"
    {
        let cluster = FakeKafkaCluster::start_with_opts(
            PARTITION_ID + 1,
            TOPIC,
            helpers::ClusterSasl::None,
            false,
        )
        .await;
        let cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        let client = Arc::new(KafkaClient::from_config(&cfg).unwrap());
        let adm: Arc<dyn AdmClient> = Arc::new(KafkaAdmClient::new(client));
        let dir = tempfile::tempdir().unwrap();
        let off_file = Arc::new(OffsetFile::new(dir.path().join("offset.json"), PARTITION_ID));
        let reg = Registry::new();

        let committer = PartitionCommitter::new(
            cfg,
            adm,
            PARTITION_ID,
            GROUP,
            Arc::new(NoOpPreCommitNotifier),
            Some(off_file),
            Some(&reg),
        );
        assert!(committer.commit(123).await.is_err());

        assert_eq!(
            get_gauge_value(
                &reg,
                "cortex_ingest_storage_reader_last_committed_offset",
                &[("partition", "1")]
            ),
            Some(-1.0)
        );
        assert_eq!(committer.commit_failures_count(), 1.0);
        assert_eq!(committer.commit_requests_count(), 1.0);
        cluster.close();
    }

    // 3. "should increment commitFailuresTotal when Kafka commit succeeds but file write fails"
    {
        let cluster = FakeKafkaCluster::start(PARTITION_ID + 1, TOPIC).await;
        let dir = tempfile::tempdir().unwrap();
        let dir_as_file = dir.path().join("offset");
        std::fs::create_dir_all(&dir_as_file).unwrap();
        let off_file = Arc::new(OffsetFile::new(dir_as_file, PARTITION_ID));

        let cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        let client = Arc::new(KafkaClient::from_config(&cfg).unwrap());
        let adm: Arc<dyn AdmClient> = Arc::new(KafkaAdmClient::new(client));
        let reg = Registry::new();

        let committer = PartitionCommitter::new(
            cfg,
            adm,
            PARTITION_ID,
            GROUP,
            Arc::new(NoOpPreCommitNotifier),
            Some(off_file),
            Some(&reg),
        );
        let err = committer.commit(123).await.unwrap_err();
        assert!(err.contains(ERR_OFFSET_FILE_WRITE), "{err}");
        assert_eq!(
            get_gauge_value(
                &reg,
                "cortex_ingest_storage_reader_last_committed_offset",
                &[("partition", "1")]
            ),
            Some(-1.0)
        );
        assert_eq!(committer.commit_failures_count(), 1.0);
        assert_eq!(committer.commit_requests_count(), 1.0);
        cluster.close();
    }

    // 4. "should call pre-commit notifier before committing"
    {
        let cfg = create_test_kafka_config("127.0.0.1:9092", TOPIC);
        let committed = Arc::new(AtomicBool::new(false));
        let c1 = Arc::clone(&committed);
        let c2 = Arc::clone(&committed);
        let notifier = Arc::new(TestPreCommitNotifier {
            count: AtomicI32::new(0),
            err: None,
            on_notify: Some(Box::new(move || {
                assert!(!c1.load(Ordering::SeqCst), "Commit happened before notification");
            })),
        });
        let adm = Arc::new(MockAdmClient {
            on_commit: Some(Box::new(move || {
                c2.store(true, Ordering::SeqCst);
            })),
        });
        let dir = tempfile::tempdir().unwrap();
        let off_file = Arc::new(OffsetFile::new(dir.path().join("offset.json"), PARTITION_ID));
        let committer = PartitionCommitter::new(
            cfg,
            adm,
            PARTITION_ID,
            GROUP,
            notifier,
            Some(off_file),
            None,
        );
        committer.commit(123).await.unwrap();
        assert!(committed.load(Ordering::SeqCst));
    }

    // 5. "should proceed with commit even if notifier fails"
    {
        let cfg = create_test_kafka_config("127.0.0.1:9092", TOPIC);
        let committed = Arc::new(AtomicBool::new(false));
        let c1 = Arc::clone(&committed);
        let c2 = Arc::clone(&committed);
        let notifier = Arc::new(TestPreCommitNotifier {
            count: AtomicI32::new(0),
            err: Some("notification failed".to_string()),
            on_notify: Some(Box::new(move || {
                assert!(!c1.load(Ordering::SeqCst), "Commit happened before notification");
            })),
        });
        let adm = Arc::new(MockAdmClient {
            on_commit: Some(Box::new(move || {
                c2.store(true, Ordering::SeqCst);
            })),
        });
        let dir = tempfile::tempdir().unwrap();
        let off_path = dir.path().join("offset.json");
        let off_file = Arc::new(OffsetFile::new(off_path.clone(), PARTITION_ID));
        let committer = PartitionCommitter::new(
            cfg,
            adm,
            PARTITION_ID,
            GROUP,
            notifier,
            Some(Arc::clone(&off_file)),
            None,
        );
        committer.commit(123).await.unwrap();
        assert!(committed.load(Ordering::SeqCst));
        assert_eq!(committer.last_committed_offset(), 123);
        assert_eq!(off_file.read().unwrap(), 123);
    }
}

// Upstream: pkg/storage/ingest/reader_test.go::TestPartitionCommitter_LastCommittedOffset
#[tokio::test]
async fn test_partition_committer_last_committed_offset() {
    const TOPIC: &str = "test-topic";
    const GROUP: &str = "test-group";
    const PARTITION_ID: i32 = 1;

    // 1. -1 before any commit
    {
        let cfg = create_test_kafka_config("127.0.0.1:9092", TOPIC);
        let dir = tempfile::tempdir().unwrap();
        let off_file = Arc::new(OffsetFile::new(dir.path().join("offset.json"), PARTITION_ID));
        let committer = PartitionCommitter::new(
            cfg,
            Arc::new(MockAdmClient { on_commit: None }),
            PARTITION_ID,
            GROUP,
            Arc::new(NoOpPreCommitNotifier),
            Some(off_file),
            None,
        );
        assert_eq!(committer.last_committed_offset(), -1);
    }

    // 2. updates after successful commit
    {
        let cluster = FakeKafkaCluster::start(PARTITION_ID + 1, TOPIC).await;
        let cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        let client = Arc::new(KafkaClient::from_config(&cfg).unwrap());
        let adm: Arc<dyn AdmClient> = Arc::new(KafkaAdmClient::new(client));
        let dir = tempfile::tempdir().unwrap();
        let off_file = Arc::new(OffsetFile::new(dir.path().join("offset.json"), PARTITION_ID));
        let committer = PartitionCommitter::new(
            cfg,
            adm,
            PARTITION_ID,
            GROUP,
            Arc::new(NoOpPreCommitNotifier),
            Some(off_file),
            None,
        );
        committer.commit(123).await.unwrap();
        assert_eq!(committer.last_committed_offset(), 123);
        committer.commit(456).await.unwrap();
        assert_eq!(committer.last_committed_offset(), 456);
        cluster.close();
    }

    // 3. does not update on failed commit
    {
        let cluster = FakeKafkaCluster::start_with_opts(
            PARTITION_ID + 1,
            TOPIC,
            helpers::ClusterSasl::None,
            false,
        )
        .await;
        let cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        let client = Arc::new(KafkaClient::from_config(&cfg).unwrap());
        let adm: Arc<dyn AdmClient> = Arc::new(KafkaAdmClient::new(client));
        let dir = tempfile::tempdir().unwrap();
        let off_file = Arc::new(OffsetFile::new(dir.path().join("offset.json"), PARTITION_ID));
        let committer = PartitionCommitter::new(
            cfg,
            adm,
            PARTITION_ID,
            GROUP,
            Arc::new(NoOpPreCommitNotifier),
            Some(off_file),
            None,
        );
        assert!(committer.commit(123).await.is_err());
        assert_eq!(committer.last_committed_offset(), -1);
        cluster.close();
    }
}

// Upstream: pkg/storage/ingest/reader_test.go::TestSingleClusterPartitionReader_ShouldWarnIfClientRackIsSetWithConcurrentFetching
#[tokio::test]
async fn test_single_cluster_partition_reader_should_warn_if_client_rack_is_set_with_concurrent_fetching()
{
    const TOPIC: &str = "test";
    const PARTITION_ID: i32 = 1;
    const WARNING: &str =
        "the configured Kafka client rack has no effect because concurrent fetching is enabled";

    for (rack, conc, expect_warning) in [("", 2, false), ("zone-a", 2, true), ("zone-a", 0, false)]
    {
        let cluster = FakeKafkaCluster::start(PARTITION_ID + 1, TOPIC).await;
        let dir = tempfile::tempdir().unwrap();
        let logs = Arc::new(SyncMutex::new(Vec::<String>::new()));
        let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        cfg.client_rack = rack.to_string();
        cfg.fetch_concurrency_max = conc;
        cfg.target_consumer_lag_at_startup = GoDuration::ZERO;
        cfg.max_consumer_lag_at_startup = GoDuration::ZERO;

        let reader = SingleClusterPartitionReader::new(
            cfg,
            PARTITION_ID,
            "test-group",
            Some(dir.path().join("offset.json")),
            Arc::new(FnConsumer(|_| async { Ok(()) })),
            None,
        )
        .unwrap()
        .with_log_sink(Arc::clone(&logs));
        reader.start().await.unwrap();

        let logged = logs.lock().join("\n");
        if expect_warning {
            assert!(logged.contains(WARNING), "expected warning in {logged}");
        } else {
            assert!(!logged.contains(WARNING), "unexpected warning in {logged}");
        }

        reader.stop().await;
        cluster.close();
    }
}

// Upstream: pkg/storage/ingest/reader_test.go::TestSingleClusterPartitionReader_Commit
#[tokio::test]
async fn test_single_cluster_partition_reader_commit() {
    const TOPIC: &str = "test";
    const PARTITION_ID: i32 = 1;

    // 1. "resume at committed"
    {
        let cluster = FakeKafkaCluster::start(PARTITION_ID + 1, TOPIC).await;
        let dir = tempfile::tempdir().unwrap();
        let off_path = dir.path().join("offset.json");
        let consumer = TestConsumer::new(8);

        let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        cfg.consumer_group_offset_commit_interval = GoDuration::from_millis(50);
        cfg.target_consumer_lag_at_startup = GoDuration::ZERO;
        cfg.max_consumer_lag_at_startup = GoDuration::ZERO;

        let r1 = SingleClusterPartitionReader::new(
            cfg.clone(),
            PARTITION_ID,
            "test-group",
            Some(off_path.clone()),
            Arc::clone(&consumer) as Arc<dyn RecordBatchConsumer>,
            None,
        )
        .unwrap();
        r1.start().await.unwrap();

        produce_record_to_cluster(&cluster, TOPIC, PARTITION_ID, b"1", Some(1));
        produce_record_to_cluster(&cluster, TOPIC, PARTITION_ID, b"2", Some(1));
        produce_record_to_cluster(&cluster, TOPIC, PARTITION_ID, b"3", Some(1));

        let _ = consumer
            .wait_records(3, Duration::from_secs(2), Duration::from_millis(120))
            .await
            .unwrap();
        r1.stop().await;

        produce_record_to_cluster(&cluster, TOPIC, PARTITION_ID, b"4", Some(1));

        let r2 = SingleClusterPartitionReader::new(
            cfg,
            PARTITION_ID,
            "test-group",
            Some(off_path),
            Arc::clone(&consumer) as Arc<dyn RecordBatchConsumer>,
            None,
        )
        .unwrap();
        r2.start().await.unwrap();
        let recs = consumer
            .wait_records(1, Duration::from_secs(2), Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(recs[0].value.as_ref(), b"4");
        r2.stop().await;
        cluster.close();
    }

    // 2. "pre-commit notifier is called"
    {
        let cluster = FakeKafkaCluster::start(PARTITION_ID + 1, TOPIC).await;
        let dir = tempfile::tempdir().unwrap();
        let notifier = Arc::new(TestPreCommitNotifier {
            count: AtomicI32::new(0),
            err: None,
            on_notify: None,
        });
        let consumer = TestConsumer::new(4);
        let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        cfg.consumer_group_offset_commit_interval = GoDuration::from_millis(40);
        cfg.target_consumer_lag_at_startup = GoDuration::ZERO;
        cfg.max_consumer_lag_at_startup = GoDuration::ZERO;

        let client = Arc::new(KafkaClient::from_config(&cfg).unwrap());
        let reader = SingleClusterPartitionReader::with_client_and_notifier(
            cfg,
            PARTITION_ID,
            "test-group",
            Some(dir.path().join("offset.json")),
            client,
            Arc::clone(&consumer) as Arc<dyn RecordBatchConsumer>,
            Arc::clone(&notifier) as Arc<dyn PreCommitNotifier>,
            None,
        )
        .unwrap();
        reader.start().await.unwrap();

        produce_record_to_cluster(&cluster, TOPIC, PARTITION_ID, b"1", Some(1));
        let _ = consumer
            .wait_records(1, Duration::from_secs(2), Duration::from_millis(120))
            .await
            .unwrap();
        assert!(notifier.count.load(Ordering::SeqCst) > 0);
        reader.stop().await;
        cluster.close();
    }

    // 3. "commit at shutdown"
    {
        let cluster = FakeKafkaCluster::start(PARTITION_ID + 1, TOPIC).await;
        let dir = tempfile::tempdir().unwrap();
        let off_path = dir.path().join("offset.json");
        let consumer = TestConsumer::new(8);

        let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        cfg.consumer_group_offset_commit_interval = GoDuration::from_secs(15);
        cfg.target_consumer_lag_at_startup = GoDuration::ZERO;
        cfg.max_consumer_lag_at_startup = GoDuration::ZERO;

        let r1 = SingleClusterPartitionReader::new(
            cfg.clone(),
            PARTITION_ID,
            "test-group",
            Some(off_path.clone()),
            Arc::clone(&consumer) as Arc<dyn RecordBatchConsumer>,
            None,
        )
        .unwrap();
        r1.start().await.unwrap();

        produce_record_to_cluster(&cluster, TOPIC, PARTITION_ID, b"1", Some(1));
        produce_record_to_cluster(&cluster, TOPIC, PARTITION_ID, b"2", Some(1));
        produce_record_to_cluster(&cluster, TOPIC, PARTITION_ID, b"3", Some(1));
        let _ = consumer
            .wait_records(3, Duration::from_secs(2), Duration::ZERO)
            .await
            .unwrap();
        r1.stop().await;

        produce_record_to_cluster(&cluster, TOPIC, PARTITION_ID, b"4", Some(1));
        let r2 = SingleClusterPartitionReader::new(
            cfg,
            PARTITION_ID,
            "test-group",
            Some(off_path),
            Arc::clone(&consumer) as Arc<dyn RecordBatchConsumer>,
            None,
        )
        .unwrap();
        r2.start().await.unwrap();
        let recs = consumer
            .wait_records(1, Duration::from_secs(2), Duration::from_millis(200))
            .await
            .unwrap();
        assert_eq!(recs[0].value.as_ref(), b"4");
        r2.stop().await;
        cluster.close();
    }

    // 4. "commit at shutdown doesn't persist if we haven't consumed any records since startup"
    {
        let cluster = FakeKafkaCluster::start(PARTITION_ID + 1, TOPIC).await;
        let dir = tempfile::tempdir().unwrap();
        let off_path = dir.path().join("offset.json");
        let consumer = TestConsumer::new(8);

        let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        cfg.consumer_group_offset_commit_interval = GoDuration::from_secs(15);
        cfg.target_consumer_lag_at_startup = GoDuration::ZERO;
        cfg.max_consumer_lag_at_startup = GoDuration::ZERO;

        let r1 = SingleClusterPartitionReader::new(
            cfg.clone(),
            PARTITION_ID,
            "test-group",
            Some(off_path.clone()),
            Arc::clone(&consumer) as Arc<dyn RecordBatchConsumer>,
            None,
        )
        .unwrap();
        r1.start().await.unwrap();
        produce_record_to_cluster(&cluster, TOPIC, PARTITION_ID, b"1", Some(1));
        produce_record_to_cluster(&cluster, TOPIC, PARTITION_ID, b"2", Some(1));
        produce_record_to_cluster(&cluster, TOPIC, PARTITION_ID, b"3", Some(1));
        let _ = consumer
            .wait_records(3, Duration::from_secs(2), Duration::ZERO)
            .await
            .unwrap();
        r1.stop().await;

        let r2 = SingleClusterPartitionReader::new(
            cfg.clone(),
            PARTITION_ID,
            "test-group",
            Some(off_path.clone()),
            Arc::clone(&consumer) as Arc<dyn RecordBatchConsumer>,
            None,
        )
        .unwrap();
        r2.start().await.unwrap();
        let _ = consumer
            .wait_records(0, Duration::from_millis(150), Duration::ZERO)
            .await
            .unwrap();
        r2.stop().await;

        let r3 = SingleClusterPartitionReader::new(
            cfg,
            PARTITION_ID,
            "test-group",
            Some(off_path),
            Arc::clone(&consumer) as Arc<dyn RecordBatchConsumer>,
            None,
        )
        .unwrap();
        r3.start().await.unwrap();
        let _ = consumer
            .wait_records(0, Duration::from_millis(150), Duration::ZERO)
            .await
            .unwrap();
        r3.stop().await;
        cluster.close();
    }
}

// Upstream: pkg/storage/ingest/reader_test.go::TestSingleClusterPartitionReader_LastCommittedOffset
#[tokio::test]
async fn test_single_cluster_partition_reader_last_committed_offset() {
    const TOPIC: &str = "test";
    const PARTITION_ID: i32 = 1;

    // "should return -1 before any commit"
    {
        let cluster = FakeKafkaCluster::start(PARTITION_ID + 1, TOPIC).await;
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        cfg.consumer_group_offset_commit_interval = GoDuration::from_secs(15);
        cfg.target_consumer_lag_at_startup = GoDuration::ZERO;
        cfg.max_consumer_lag_at_startup = GoDuration::ZERO;

        let reader = SingleClusterPartitionReader::new(
            cfg,
            PARTITION_ID,
            "test-group",
            Some(dir.path().join("offset.json")),
            TestConsumer::new(1),
            None,
        )
        .unwrap();
        reader.start().await.unwrap();
        assert_eq!(reader.last_committed_offset(), -1);
        reader.stop().await;
        cluster.close();
    }

    // "should return last committed offset after commit"
    {
        let cluster = FakeKafkaCluster::start(PARTITION_ID + 1, TOPIC).await;
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        cfg.consumer_group_offset_commit_interval = GoDuration::from_millis(40);
        cfg.target_consumer_lag_at_startup = GoDuration::ZERO;
        cfg.max_consumer_lag_at_startup = GoDuration::ZERO;

        let consumer = TestConsumer::new(4);
        let reader = SingleClusterPartitionReader::new(
            cfg,
            PARTITION_ID,
            "test-group",
            Some(dir.path().join("offset.json")),
            Arc::clone(&consumer) as Arc<dyn RecordBatchConsumer>,
            None,
        )
        .unwrap();
        reader.start().await.unwrap();

        produce_record_to_cluster(&cluster, TOPIC, PARTITION_ID, b"1", Some(1));
        produce_record_to_cluster(&cluster, TOPIC, PARTITION_ID, b"2", Some(1));
        produce_record_to_cluster(&cluster, TOPIC, PARTITION_ID, b"3", Some(1));
        let _ = consumer
            .wait_records(3, Duration::from_secs(2), Duration::from_millis(120))
            .await
            .unwrap();

        let dl = Instant::now() + Duration::from_secs(5);
        while reader.last_committed_offset() < 2 {
            assert!(Instant::now() < dl);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(reader.last_committed_offset(), 2);

        reader.stop().await;
        cluster.close();
    }
}

fn histogram_buckets(reg: &Registry, name: &str, labels: &[(&str, &str)]) -> Option<(Vec<f64>, u64)> {
    for mf in reg.gather() {
        if mf.get_name() != name {
            continue;
        }
        for m in mf.get_metric() {
            let ok = labels.iter().all(|(k, v)| {
                m.get_label()
                    .iter()
                    .any(|l| l.get_name() == *k && l.get_value() == *v)
            });
            if ok {
                let h = m.get_histogram();
                return Some((
                    h.get_bucket().iter().map(|b| b.get_upper_bound()).collect(),
                    h.get_sample_count(),
                ));
            }
        }
    }
    None
}

/// KAFKA-3: `processNextFetches` runs `instrumentGaps(findGapsInRecords(fetches, lastSeenOffset))`,
/// so offsets that were never fetched are counted in `missed_records_total` (both fetch paths).
// Upstream: pkg/storage/ingest/reader.go (processNextFetches / instrumentGaps)
#[tokio::test]
async fn test_single_cluster_partition_reader_counts_missed_records() {
    const TOPIC: &str = "test";
    const PARTITION_ID: i32 = 1;

    for conc in [0i64, 2] {
        let cluster = FakeKafkaCluster::start(PARTITION_ID + 1, TOPIC).await;
        for i in 0..6 {
            produce_record_to_cluster(&cluster, TOPIC, PARTITION_ID, format!("record-{i}").as_bytes(), Some(1));
        }
        // The broker never returns offsets 2 and 3 (e.g. deleted by retention between fetches).
        *cluster.hooks.fetch.lock() = Some(Arc::new(move |req: kafka_protocol::messages::FetchRequest| {
            Box::pin(async move {
                let fp = &req.topics[0].partitions[0];
                if fp.fetch_offset < 4 {
                    // Offsets 0..1 come in a first response, then the broker jumps to 4.
                    let range = if fp.fetch_offset <= 1 { fp.fetch_offset..2 } else { 4..6 };
                    let mut resp = FetchResponse::default();
                    let mut ft = FetchableTopicResponse::default();
                    ft.topic = req.topics[0].topic.clone();
                    ft.topic_id = req.topics[0].topic_id;
                    let mut pd = PartitionData::default();
                    pd.partition_index = fp.partition;
                    pd.high_watermark = 6;
                    pd.log_start_offset = 0;
                    let recs: Vec<helpers::StoredRecord> = range
                        .map(|o| helpers::StoredRecord {
                            offset: o,
                            timestamp_ms: now_ms(),
                            key: None,
                            value: Some(Bytes::from(format!("record-{o}"))),
                            headers: vec![("Version".to_string(), Some(Bytes::copy_from_slice(&1u32.to_be_bytes())))],
                        })
                        .collect();
                    pd.records = Some(helpers::encode_stored_records(&recs));
                    ft.partitions = vec![pd];
                    resp.responses = vec![ft];
                    return HookAction::Respond(resp);
                }
                HookAction::PassThrough
            })
        }));

        let consumed = Arc::new(SyncMutex::new(Vec::<i64>::new()));
        let c = Arc::clone(&consumed);
        let dir = tempfile::tempdir().unwrap();
        let reg = Registry::new();
        let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        cfg.consume_from_position_at_startup = "start".to_string();
        cfg.fetch_concurrency_max = conc;
        // Small fetches so that offsets 0..1 come first.
        cfg.max_buffered_bytes = 2_000_000;
        let reader = SingleClusterPartitionReader::new(
            cfg,
            PARTITION_ID,
            "test-group",
            Some(dir.path().join("offset.json")),
            Arc::new(FnConsumer(move |recs: Vec<FetchedRecord>| {
                let c = Arc::clone(&c);
                async move {
                    c.lock().extend(recs.iter().map(|r| r.offset));
                    Ok(())
                }
            })),
            Some(&reg),
        )
        .unwrap();
        reader.start().await.unwrap();

        let dl = Instant::now() + Duration::from_secs(10);
        while !consumed.lock().contains(&5) {
            assert!(Instant::now() < dl, "conc {conc}: consumed {:?}", consumed.lock());
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let got = consumed.lock().clone();
        let missed = get_counter_value(&reg, "cortex_ingest_storage_reader_missed_records_total", &[]);
        assert_eq!(got, vec![0, 1, 4, 5], "conc {conc}");
        assert_eq!(missed, 2.0, "conc {conc}");
        reader.stop().await;
        cluster.close();
    }
}

/// KAFKA-4 / KAFKA-5: the startup catch-up observes the `phase="starting"` delays, the running
/// loop the `phase="running"` ones, `receive_and_consume_delay` is observed after consumption,
/// and the histogram buckets match `NewReaderMetrics`.
// Upstream: pkg/storage/ingest/reader.go (NewReaderMetrics, processNextFetches)
#[tokio::test]
async fn test_single_cluster_partition_reader_metrics_match_upstream() {
    const TOPIC: &str = "test";
    const PARTITION_ID: i32 = 1;

    for conc in [0i64, 2] {
        let cluster = FakeKafkaCluster::start(PARTITION_ID + 1, TOPIC).await;
        produce_record_to_cluster(&cluster, TOPIC, PARTITION_ID, b"record-0", Some(1));
        produce_record_to_cluster(&cluster, TOPIC, PARTITION_ID, b"record-1", Some(1));

        let dir = tempfile::tempdir().unwrap();
        let reg = Registry::new();
        let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        cfg.consume_from_position_at_startup = "start".to_string();
        cfg.fetch_concurrency_max = conc;
        cfg.target_consumer_lag_at_startup = GoDuration::from_secs(1);
        cfg.max_consumer_lag_at_startup = GoDuration::from_secs(2);
        let reader = SingleClusterPartitionReader::new(
            cfg,
            PARTITION_ID,
            "test-group",
            Some(dir.path().join("offset.json")),
            TestConsumer::new(16),
            Some(&reg),
        )
        .unwrap();

        // Both phases are exposed from the start (WithLabelValues in NewReaderMetrics).
        for name in [
            "cortex_ingest_storage_reader_receive_delay_seconds",
            "cortex_ingest_storage_reader_receive_and_consume_delay_seconds",
        ] {
            for phase in ["starting", "running"] {
                let (bounds, count) = histogram_buckets(&reg, name, &[("phase", phase)]).unwrap();
                assert_eq!(count, 0);
                let want: Vec<f64> = (0..18).map(|i| 0.125 * 2f64.powi(i)).collect();
                assert_eq!(bounds, want, "{name}");
            }
        }

        reader.start().await.unwrap();
        for name in [
            "cortex_ingest_storage_reader_receive_delay_seconds",
            "cortex_ingest_storage_reader_receive_and_consume_delay_seconds",
        ] {
            let (_, count) = histogram_buckets(&reg, name, &[("phase", "starting")]).unwrap();
            assert_eq!(count, 2, "{name} starting (conc {conc})");
        }

        produce_record_to_cluster(&cluster, TOPIC, PARTITION_ID, b"record-2", Some(1));
        let dl = Instant::now() + Duration::from_secs(10);
        loop {
            let (_, count) = histogram_buckets(
                &reg,
                "cortex_ingest_storage_reader_receive_and_consume_delay_seconds",
                &[("phase", "running")],
            )
            .unwrap();
            if count == 1 {
                break;
            }
            assert!(Instant::now() < dl, "conc {conc}: running phase never observed");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let (bounds, count) =
            histogram_buckets(&reg, "cortex_ingest_storage_reader_records_per_fetch", &[]).unwrap();
        let want: Vec<f64> = (0..15).map(|i| 2f64.powi(i)).collect();
        assert_eq!(bounds, want);
        assert!(count >= 2);
        // Native-only upstream histograms: no classic bucket besides +Inf.
        for name in [
            "cortex_ingest_storage_reader_records_batch_process_duration_seconds",
            "cortex_ingest_storage_reader_records_batch_wait_duration_seconds",
            "cortex_ingest_storage_reader_records_batch_fetch_max_bytes",
            "cortex_ingest_storage_reader_estimated_bytes_per_record",
        ] {
            let (bounds, _) = histogram_buckets(&reg, name, &[]).unwrap();
            assert!(bounds.is_empty(), "{name}: {bounds:?}");
        }
        // The concurrent fetchers observe their own series; the client path doesn't fetch
        // through them.
        let (_, max_bytes_count) =
            histogram_buckets(&reg, "cortex_ingest_storage_reader_records_batch_fetch_max_bytes", &[]).unwrap();
        assert_eq!(max_bytes_count > 0, conc > 0, "conc {conc}");
        // kprom client metrics.
        assert!(
            get_counter_value(&reg, "cortex_ingest_storage_reader_fetch_records_total", &[("component", "partition-reader")])
                >= 3.0,
            "conc {conc}"
        );
        assert!(
            get_counter_value(&reg, "cortex_ingest_storage_reader_connects_total", &[("component", "partition-reader")])
                >= 1.0,
            "conc {conc}"
        );

        reader.stop().await;
        cluster.close();
    }
}
