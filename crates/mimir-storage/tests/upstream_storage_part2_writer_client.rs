#[path = "upstream_storage_part2_helpers.rs"]
mod helpers;

use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use helpers::{
    create_test_kafka_config, get_counter_value, get_gauge_value, get_histogram_count_and_sum,
    get_summary_quantile, ClusterSasl, FakeKafkaCluster, HookAction,
};
use mimir_config::types::SASLMechanism;
use mimir_config::GoDuration;
use mimir_storage::ingest::{
    first_produce_err, KafkaProducer, KafkaProducerClient, KafkaWriterClient,
    KafkaWriterClientOptions, ProduceCancelSource, ProduceContext, ProducerError, WriterRecord,
    DEFAULT_MAX_INFLIGHT_PRODUCE_REQUESTS, WRITER_METRICS_PREFIX,
};
use prometheus::Registry;
use tokio::sync::Notify;

fn create_test_producer(
    cluster_addr: &str,
    topic: &str,
    reg: &Registry,
    mutate: impl FnOnce(&mut mimir_config::KafkaConfig),
) -> Arc<KafkaProducer> {
    let mut cfg = create_test_kafka_config(cluster_addr, topic);
    mutate(&mut cfg);
    KafkaProducer::from_config_with_prefix(
        &cfg,
        DEFAULT_MAX_INFLIGHT_PRODUCE_REQUESTS,
        WRITER_METRICS_PREFIX,
        Some(reg),
        KafkaWriterClientOptions {
            disable_default_topic: true,
            disable_linger: false,
        },
    )
    .unwrap()
}

// Upstream: pkg/storage/ingest/writer_client_test.go::TestNewKafkaWriterClient_ShouldSupportSASLPlainAuthentication
#[tokio::test]
async fn test_new_kafka_writer_client_should_support_sasl_plain_authentication() {
    const TOPIC: &str = "test";
    const USERNAME: &str = "mimir";
    const PASSWORD: &str = "supersecret";

    let cluster = FakeKafkaCluster::start_with_opts(
        1,
        TOPIC,
        ClusterSasl::Plain {
            username: USERNAME.to_string(),
            password: PASSWORD.to_string(),
        },
        true,
    )
    .await;

    // wrong auth fails
    {
        let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        cfg.sasl_mechanism = SASLMechanism::Plain;
        cfg.sasl_username = USERNAME.to_string();
        cfg.sasl_password = "wrong".to_string();
        let client = KafkaWriterClient::new(
            &cfg,
            1,
            None,
            KafkaWriterClientOptions::default(),
        )
        .unwrap();
        assert!(client.ping().await.is_err());
        client.close();
    }

    // good auth succeeds
    {
        let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        cfg.sasl_mechanism = SASLMechanism::Plain;
        cfg.sasl_username = USERNAME.to_string();
        cfg.sasl_password = PASSWORD.to_string();
        let client = KafkaWriterClient::new(
            &cfg,
            1,
            None,
            KafkaWriterClientOptions::default(),
        )
        .unwrap();
        client.ping().await.unwrap();
        client.close();
    }

    cluster.close();
}

// Upstream: pkg/storage/ingest/writer_client_test.go::TestNewKafkaWriterClient_ShouldConfigureRequestTimeoutOverhead
#[tokio::test]
async fn test_new_kafka_writer_client_should_configure_request_timeout_overhead() {
    let cluster = FakeKafkaCluster::start(1, "test").await;
    let mut cfg = create_test_kafka_config(&cluster.addr, "test");
    cfg.write_timeout_overhead = GoDuration::from_secs(1);

    let client = KafkaWriterClient::new(
        &cfg,
        1,
        None,
        KafkaWriterClientOptions::default(),
    )
    .unwrap();
    assert_eq!(client.request_timeout_overhead(), Duration::from_secs(1));
    client.close();
    cluster.close();
}

// Upstream: pkg/storage/ingest/writer_client_test.go::TestNewKafkaWriterClient_ShouldTrackExtendedKafkaClientMetrics
#[tokio::test]
async fn test_new_kafka_writer_client_should_track_extended_kafka_client_metrics() {
    let cluster = FakeKafkaCluster::start(1, "test").await;
    let cfg = create_test_kafka_config(&cluster.addr, "test");
    let reg = Registry::new();

    let client = KafkaWriterClient::new_with_prefix(
        &cfg,
        1,
        WRITER_METRICS_PREFIX,
        Some(&reg),
        KafkaWriterClientOptions::default(),
    )
    .unwrap();

    client
        .produce_sync(WriterRecord {
            key: Bytes::from_static(b"test"),
            value: Bytes::from_static(b"message 1"),
            ..Default::default()
        })
        .await
        .unwrap();

    for name in [
        "cortex_ingest_storage_writer_kafka_write_wait_seconds",
        "cortex_ingest_storage_writer_kafka_write_time_seconds",
        "cortex_ingest_storage_writer_kafka_read_wait_seconds",
        "cortex_ingest_storage_writer_kafka_read_time_seconds",
        "cortex_ingest_storage_writer_kafka_request_duration_e2e_seconds",
    ] {
        let (count, _) = get_histogram_count_and_sum(&reg, name, &[])
            .unwrap_or_else(|| panic!("missing histogram {name}"));
        assert!(count >= 1, "expected count >= 1 for {name}, got {count}");
    }

    client.close();
    cluster.close();
}

// Upstream: pkg/storage/ingest/writer_client_test.go::TestKafkaProducer_ShouldExposeBufferedBytesLimit
#[tokio::test]
async fn test_kafka_producer_should_expose_buffered_bytes_limit() {
    let cluster = FakeKafkaCluster::start(1, "test").await;
    let reg = Registry::new();
    let producer = create_test_producer(&cluster.addr, "test", &reg, |cfg| {
        cfg.producer_max_buffered_bytes = 1024 * 1024;
    });

    let val = get_gauge_value(
        &reg,
        "cortex_ingest_storage_writer_buffered_produce_bytes_limit",
        &[],
    )
    .unwrap();
    assert_eq!(val, 1_048_576.0);

    producer.close();
    cluster.close();
}

// Upstream: pkg/storage/ingest/writer_client_test.go::TestKafkaProducer_ProduceSync_ShouldTrackBufferedProduceBytes
#[tokio::test]
async fn test_kafka_producer_produce_sync_should_track_buffered_produce_bytes() {
    let cluster = FakeKafkaCluster::start(1, "test").await;
    let unblock = Arc::new(Notify::new());
    let unblocked = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let unblock_c = Arc::clone(&unblock);
    let unblocked_c = Arc::clone(&unblocked);
    *cluster.hooks.produce.lock() = Some(Arc::new(move |_| {
        let u = Arc::clone(&unblock_c);
        let flag = Arc::clone(&unblocked_c);
        Box::pin(async move {
            while !flag.load(Ordering::SeqCst) {
                u.notified().await;
            }
            HookAction::PassThrough
        })
    }));

    let reg = Registry::new();
    let producer = create_test_producer(&cluster.addr, "test", &reg, |_| {});
    let client = Arc::clone(producer.client());

    // Initially 0
    tokio::time::sleep(Duration::from_millis(60)).await;
    let q1 = get_summary_quantile(
        &reg,
        "cortex_ingest_storage_writer_buffered_produce_bytes_distribution",
        1.0,
    )
    .unwrap();
    assert!((q1 - 0.0).abs() < 0.001);

    let done1 = Arc::new(Notify::new());
    let d1 = Arc::clone(&done1);
    client.produce(
        WriterRecord {
            topic: "test".to_string(),
            key: Bytes::from_static(b"test"),
            value: Bytes::from_static(b"message 1"),
            ..Default::default()
        },
        Box::new(move |_, res| {
            res.unwrap();
            d1.notify_one();
        }),
    );

    let expected_record_size = client.buffered_produce_bytes();
    assert!(expected_record_size > 0);

    // Wait for summary update
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if let Some(v) = get_summary_quantile(
            &reg,
            "cortex_ingest_storage_writer_buffered_produce_bytes_distribution",
            1.0,
        ) {
            if (v - expected_record_size as f64).abs() < 0.001 {
                break;
            }
        }
        assert!(Instant::now() < deadline, "timed out waiting for 1st record size");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let done2 = Arc::new(Notify::new());
    let d2 = Arc::clone(&done2);
    client.produce(
        WriterRecord {
            topic: "test".to_string(),
            key: Bytes::from_static(b"test"),
            value: Bytes::from_static(b"message 1"),
            ..Default::default()
        },
        Box::new(move |_, res| {
            res.unwrap();
            d2.notify_one();
        }),
    );

    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if let Some(v) = get_summary_quantile(
            &reg,
            "cortex_ingest_storage_writer_buffered_produce_bytes_distribution",
            1.0,
        ) {
            if (v - (2 * expected_record_size) as f64).abs() < 0.001 {
                break;
            }
        }
        assert!(Instant::now() < deadline, "timed out waiting for 2nd record size");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    unblocked.store(true, Ordering::SeqCst);
    unblock.notify_waiters();
    done1.notified().await;
    done2.notified().await;

    producer.close();
    cluster.close();
}

// Upstream: pkg/storage/ingest/writer_client_test.go::TestKafkaProducer_ProduceSync_ShouldCircuitBreakIfContextIsDone
#[tokio::test]
async fn test_kafka_producer_produce_sync_should_circuit_break_if_context_is_done() {
    let cluster = FakeKafkaCluster::start(1, "test").await;
    let reg = Registry::new();
    let producer = create_test_producer(&cluster.addr, "test", &reg, |_| {});

    let expired_ctx =
        ProduceContext::background().with_deadline(Instant::now() - Duration::from_secs(1));
    let res = producer
        .produce_sync(
            &expired_ctx,
            vec![WriterRecord {
                topic: "test".to_string(),
                key: Bytes::from_static(b"test"),
                value: Bytes::from_static(b"message 1"),
                ..Default::default()
            }],
        )
        .await;

    let err = first_produce_err(&res).unwrap();
    assert!(err.is_timeout(), "expected deadline exceeded, got {err}");
    assert_eq!(producer.buffered_produce_records(), 0);

    assert_eq!(
        get_counter_value(
            &reg,
            "cortex_ingest_storage_writer_produce_records_enqueued_total",
            &[]
        ),
        1.0
    );
    assert_eq!(
        get_counter_value(
            &reg,
            "cortex_ingest_storage_writer_produce_records_failed_total",
            &[("reason", "cancelled-before-producing")]
        ),
        1.0
    );

    let (cnt, sum) = get_histogram_count_and_sum(
        &reg,
        "cortex_ingest_storage_writer_produce_remaining_deadline_seconds",
        &[],
    )
    .unwrap();
    assert_eq!(cnt, 1);
    assert_eq!(sum, 0.0);

    producer.close();
    cluster.close();
}

// Upstream: pkg/storage/ingest/writer_client_test.go::TestKafkaProducer_ProduceSync_ShouldRejectRecordsWithTimestampSet
#[tokio::test]
async fn test_kafka_producer_produce_sync_should_reject_records_with_timestamp_set() {
    let cluster = FakeKafkaCluster::start(1, "test").await;
    let reg = Registry::new();
    let producer = create_test_producer(&cluster.addr, "test", &reg, |_| {});

    let records = vec![
        WriterRecord {
            topic: "test".to_string(),
            key: Bytes::from_static(b"test"),
            value: Bytes::from_static(b"message 1"),
            ..Default::default()
        },
        WriterRecord {
            topic: "test".to_string(),
            key: Bytes::from_static(b"test"),
            value: Bytes::from_static(b"message 2"),
            timestamp_ms: Some(1_700_000_000_000),
            ..Default::default()
        },
    ];

    let res = producer
        .produce_sync(&ProduceContext::background(), records)
        .await;
    assert_eq!(res.len(), 2);
    for r in &res {
        let err = r.err.as_ref().expect("expected error");
        assert!(err.to_string().contains("Kafka record Timestamp must not be set"));
    }

    assert_eq!(producer.buffered_produce_records(), 0);
    assert_eq!(producer.buffered_bytes_counter(), 0);
    assert_eq!(
        get_counter_value(
            &reg,
            "cortex_ingest_storage_writer_produce_records_enqueued_total",
            &[]
        ),
        2.0
    );
    assert_eq!(
        get_counter_value(
            &reg,
            "cortex_ingest_storage_writer_produce_records_failed_total",
            &[("reason", "record-timestamp-set")]
        ),
        2.0
    );

    producer.close();
    cluster.close();
}

// Upstream: pkg/storage/ingest/writer_client_test.go::TestKafkaProducer_ProduceSync_ShouldRejectWholeBatchIfBufferIsFull
#[tokio::test]
async fn test_kafka_producer_produce_sync_should_reject_whole_batch_if_buffer_is_full() {
    const RECORD_VAL: &[u8] = b"1234567890";
    let cluster = FakeKafkaCluster::start(1, "test").await;
    let reg = Registry::new();
    let producer = create_test_producer(&cluster.addr, "test", &reg, |cfg| {
        cfg.producer_max_buffered_bytes = (2 * RECORD_VAL.len()) as i64;
    });

    let batch = vec![
        WriterRecord {
            topic: "test".to_string(),
            key: Bytes::from_static(b"test"),
            value: Bytes::from_static(RECORD_VAL),
            ..Default::default()
        },
        WriterRecord {
            topic: "test".to_string(),
            key: Bytes::from_static(b"test"),
            value: Bytes::from_static(RECORD_VAL),
            ..Default::default()
        },
        WriterRecord {
            topic: "test".to_string(),
            key: Bytes::from_static(b"test"),
            value: Bytes::from_static(RECORD_VAL),
            ..Default::default()
        },
    ];

    let res = producer
        .produce_sync(&ProduceContext::background(), batch)
        .await;
    assert_eq!(res.len(), 3);
    for r in &res {
        assert_eq!(r.err.as_ref(), Some(&ProducerError::MaxBuffered));
    }

    assert_eq!(producer.buffered_produce_records(), 0);
    assert_eq!(producer.buffered_bytes_counter(), 0);
    assert_eq!(
        get_counter_value(
            &reg,
            "cortex_ingest_storage_writer_produce_records_enqueued_total",
            &[]
        ),
        3.0
    );
    assert_eq!(
        get_counter_value(
            &reg,
            "cortex_ingest_storage_writer_produce_records_failed_total",
            &[("reason", "buffer-full")]
        ),
        3.0
    );

    // Subsequent batch that fits should succeed.
    let res2 = producer
        .produce_sync(
            &ProduceContext::background(),
            vec![WriterRecord {
                topic: "test".to_string(),
                key: Bytes::from_static(b"test"),
                value: Bytes::from_static(RECORD_VAL),
                ..Default::default()
            }],
        )
        .await;
    assert!(first_produce_err(&res2).is_none());

    producer.close();
    cluster.close();
}

// Upstream: pkg/storage/ingest/writer_client_test.go::TestKafkaProducer_ProduceSync_ShouldReturnOneResultPerRecordOnContextCancellation
#[tokio::test]
async fn test_kafka_producer_produce_sync_should_return_one_result_per_record_on_context_cancellation()
{
    // "context is already done before producing"
    {
        let cluster = FakeKafkaCluster::start(1, "test").await;
        let reg = Registry::new();
        let producer = create_test_producer(&cluster.addr, "test", &reg, |_| {});

        let (cancel_src, ctx) = ProduceCancelSource::new();
        cancel_src.cancel();

        let records = vec![
            WriterRecord {
                topic: "test".to_string(),
                key: Bytes::from_static(b"key-1"),
                value: Bytes::from_static(b"message 1"),
                ..Default::default()
            },
            WriterRecord {
                topic: "test".to_string(),
                key: Bytes::from_static(b"key-2"),
                value: Bytes::from_static(b"message 2"),
                ..Default::default()
            },
        ];
        let res = producer.produce_sync(&ctx, records).await;
        assert_eq!(res.len(), 2);
        assert_eq!(res[0].record.key.as_ref(), b"key-1");
        assert_eq!(res[1].record.key.as_ref(), b"key-2");
        assert!(res[0].err.as_ref().unwrap().is_canceled());
        assert!(res[1].err.as_ref().unwrap().is_canceled());

        producer.close();
        cluster.close();
    }

    // "context is already done before producing with a non-Canceled cause"
    {
        let cluster = FakeKafkaCluster::start(1, "test").await;
        let reg = Registry::new();
        let producer = create_test_producer(&cluster.addr, "test", &reg, |_| {});

        let (cancel_src, ctx) = ProduceCancelSource::new();
        cancel_src.cancel_with_cause("custom cancellation cause");

        let records = vec![
            WriterRecord {
                topic: "test".to_string(),
                key: Bytes::from_static(b"key-1"),
                value: Bytes::from_static(b"message 1"),
                ..Default::default()
            },
            WriterRecord {
                topic: "test".to_string(),
                key: Bytes::from_static(b"key-2"),
                value: Bytes::from_static(b"message 2"),
                ..Default::default()
            },
        ];
        let res = producer.produce_sync(&ctx, records).await;
        assert_eq!(res.len(), 2);
        for r in &res {
            assert!(r
                .err
                .as_ref()
                .unwrap()
                .to_string()
                .contains("custom cancellation cause"));
        }

        producer.close();
        cluster.close();
    }

    // "context is canceled while waiting for produce results"
    {
        let cluster = FakeKafkaCluster::start(2, "test").await;
        let unblock = Arc::new(Notify::new());
        let unblocked = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let unblock_c = Arc::clone(&unblock);
        let unblocked_c = Arc::clone(&unblocked);

        *cluster.hooks.produce.lock() = Some(Arc::new(move |_| {
            let u = Arc::clone(&unblock_c);
            let f = Arc::clone(&unblocked_c);
            Box::pin(async move {
                while !f.load(Ordering::SeqCst) {
                    u.notified().await;
                }
                HookAction::PassThrough
            })
        }));

        let reg = Registry::new();
        let producer = create_test_producer(&cluster.addr, "test", &reg, |_| {});
        let (cancel_src, ctx) = ProduceCancelSource::new();

        let records = vec![
            WriterRecord {
                topic: "test".to_string(),
                partition: 0,
                key: Bytes::from_static(b"key-1"),
                value: Bytes::from_static(b"message 1"),
                ..Default::default()
            },
            WriterRecord {
                topic: "test".to_string(),
                partition: 1,
                key: Bytes::from_static(b"key-2"),
                value: Bytes::from_static(b"message 2"),
                ..Default::default()
            },
        ];

        let p_clone = Arc::clone(&producer);
        let handle = tokio::spawn(async move { p_clone.produce_sync(&ctx, records).await });

        let deadline = Instant::now() + Duration::from_secs(2);
        while producer.buffered_produce_records() < 2 {
            assert!(Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        cancel_src.cancel();
        let res = tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("ProduceSync did not return after cancel")
            .unwrap();
        assert_eq!(res.len(), 2);
        assert_eq!(res[0].record.partition, 0);
        assert_eq!(res[1].record.partition, 1);
        assert!(res[0].err.as_ref().unwrap().is_canceled());
        assert!(res[1].err.as_ref().unwrap().is_canceled());

        unblocked.store(true, Ordering::SeqCst);
        unblock.notify_waiters();
        producer.close();
        cluster.close();
    }
}

// Upstream: pkg/storage/ingest/writer_client_test.go::TestKafkaProducer_ProduceSync_LatencyShouldBeDrivenByKafkaProduceLatency
#[tokio::test]
async fn test_kafka_producer_produce_sync_latency_should_be_driven_by_kafka_produce_latency() {
    const TOPIC: &str = "test";
    const NUM_PARTITIONS: i32 = 20;
    const PRODUCE_INTERVAL: Duration = Duration::from_millis(5);
    const TOTAL_RECORDS: usize = 120;
    const PRODUCE_LATENCY: Duration = Duration::from_millis(300);

    let cluster = FakeKafkaCluster::start(NUM_PARTITIONS, TOPIC).await;
    *cluster.hooks.produce.lock() = Some(Arc::new(|_| {
        Box::pin(async move {
            tokio::time::sleep(PRODUCE_LATENCY).await;
            HookAction::PassThrough
        })
    }));

    let reg = Registry::new();
    let producer = create_test_producer(&cluster.addr, TOPIC, &reg, |cfg| {
        cfg.write_timeout = GoDuration::from_secs(10);
    });

    let payload = Bytes::from(vec![0xabu8; 1024]);
    let latency_sum_ms = Arc::new(AtomicI32::new(0));
    let acked = Arc::new(AtomicI32::new(0));
    let done = Arc::new(Notify::new());

    for i in 0..TOTAL_RECORDS {
        let sent_at = Instant::now();
        let sum_c = Arc::clone(&latency_sum_ms);
        let ack_c = Arc::clone(&acked);
        let done_c = Arc::clone(&done);
        producer.client().produce(
            WriterRecord {
                topic: TOPIC.to_string(),
                partition: (i as i32) % NUM_PARTITIONS,
                key: Bytes::from_static(b"user-1"),
                value: payload.clone(),
                ..Default::default()
            },
            Box::new(move |_, res| {
                res.unwrap();
                sum_c.fetch_add(sent_at.elapsed().as_millis() as i32, Ordering::Relaxed);
                if ack_c.fetch_add(1, Ordering::SeqCst) + 1 >= TOTAL_RECORDS as i32 {
                    done_c.notify_one();
                }
            }),
        );
        tokio::time::sleep(PRODUCE_INTERVAL).await;
    }

    while acked.load(Ordering::SeqCst) < TOTAL_RECORDS as i32 {
        let notified = done.notified();
        if acked.load(Ordering::SeqCst) >= TOTAL_RECORDS as i32 {
            break;
        }
        tokio::time::timeout(Duration::from_secs(10), notified)
            .await
            .expect("timed out waiting for produce acks");
    }

    let avg_latency_s =
        (latency_sum_ms.load(Ordering::Relaxed) as f64) / (TOTAL_RECORDS as f64) / 1000.0;
    let diff = (avg_latency_s - PRODUCE_LATENCY.as_secs_f64()).abs();
    assert!(
        diff < 0.5,
        "expected avg produce latency ~{:.2}s, got {:.2}s",
        PRODUCE_LATENCY.as_secs_f64(),
        avg_latency_s
    );

    producer.close();
    cluster.close();
}
