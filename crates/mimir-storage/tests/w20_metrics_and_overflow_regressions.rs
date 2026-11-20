//! Regression tests for Wave 20 metrics parity & security/overflow fixes in `mimir-storage`:
//! - M20-01: `StoragePusherMetrics` native-only histograms (no bogus `DefBuckets`, native buckets)
//! - M20-02: Native-only & hybrid native histograms across `mimir-storage`
//! - M20-08: `KafkaClientExtendedMetrics` real `write_wait`/`write_time`/`read_wait`/`read_time` & exact Go HELP
//! - M20-10: `mimir_storage::ingest::PartitionRingWatcher` / `PartitionRingWatchers` gauges
//! - W20-SDW-08: `SingleClusterPartitionReader` `i128` delay clamping & `i64::MIN` unset sentinel
//! - W20-SDW-10: `request_json_from_socket` raw `&[u8]` HTTP chunked decoding on split multi-byte UTF-8

#[path = "upstream_storage_part2_helpers.rs"]
mod helpers;

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use helpers::{create_test_kafka_config, FakeKafkaCluster};
use mimir_config::{BucketConfig, GoDuration};
use mimir_proto::ringpb::PartitionState;
use mimir_storage::bucket::BucketClient;
use mimir_storage::ingest::pusher::NoOpPreCommitNotifier;
use mimir_storage::ingest::writer_client::{
    KafkaProducer, KafkaWriterClientOptions, ProduceContext, WriterRecord, WRITER_METRICS_PREFIX,
};
use mimir_storage::ingest::{
    add_partition_with_derived_tokens, add_partition_with_stored_tokens, request_json_from_socket,
    AdmClient, FetchedRecord, InMemoryPartitionKv, KafkaClient, KafkaClientExtendedMetrics,
    PartitionCommitter, PartitionOffsetClientMetrics, PartitionRingWatchers, PartitionTokenTable,
    PusherConsumerMetrics, ReaderError, ReaderMetrics, RecordBatchConsumer,
    SingleClusterPartitionReader, WriterMetrics,
    DEFAULT_MAX_INFLIGHT_PRODUCE_REQUESTS,
};
use prometheus::{Encoder, Registry, TextEncoder};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixListener;

fn unzigzag32(n: u32) -> i32 {
    ((n >> 1) as i32) ^ (-((n & 1) as i32))
}

fn find_metric<'a>(
    families: &'a [prometheus::proto::MetricFamily],
    name: &str,
    labels: &[(&str, &str)],
) -> &'a prometheus::proto::Metric {
    let mf = families
        .iter()
        .find(|f| f.get_name() == name)
        .unwrap_or_else(|| panic!("missing metric family {name}"));
    mf.get_metric()
        .iter()
        .find(|m| {
            labels.iter().all(|(k, v)| {
                m.get_label()
                    .iter()
                    .any(|lp| lp.get_name() == *k && lp.get_value() == *v)
            })
        })
        .unwrap_or_else(|| panic!("missing metric {name} with labels {labels:?}"))
}

fn assert_native_histogram(
    reg: &Registry,
    name: &str,
    labels: &[(&str, &str)],
    expected_schema: i32,
    expected_classic_count: Option<usize>,
    expected_zero_threshold: Option<f64>,
) {
    let families = reg.gather();
    let m = find_metric(&families, name, labels);
    let h = m.get_histogram();
    if let Some(classic_n) = expected_classic_count {
        assert_eq!(
            h.get_bucket().len(),
            classic_n,
            "{name}: classic buckets len mismatch"
        );
    }
    let uf = &h.unknown_fields;
    let schema_vals = uf.get(5).and_then(|v| v.varint.first().copied());
    let schema = unzigzag32(
        schema_vals.unwrap_or_else(|| panic!("{name}: missing native schema (field 5)")) as u32,
    );
    assert_eq!(schema, expected_schema, "{name}: native schema mismatch");

    let zt_bits = uf
        .get(6)
        .and_then(|v| v.fixed64.first().copied())
        .unwrap_or_else(|| panic!("{name}: missing zero_threshold (field 6)"));
    let zt = f64::from_bits(zt_bits);
    if let Some(want_zt) = expected_zero_threshold {
        assert!((zt - want_zt).abs() < 1e-12, "{name}: zero_threshold {zt} != {want_zt}");
    }

    let has_pos_span = uf.get(12).is_some_and(|v| !v.length_delimited.is_empty());
    let has_zero_count = uf
        .get(7)
        .and_then(|v| v.varint.first().copied())
        .unwrap_or(0)
        > 0;
    assert!(
        has_pos_span || has_zero_count,
        "{name}: expected positive_span or non-zero zero_count on observed native histogram"
    );
}

struct DummyAdm;
impl AdmClient for DummyAdm {
    fn commit_offsets(
        &self,
        _group: &str,
        _topic: &str,
        _partition_id: i32,
        offset: i64,
    ) -> Pin<Box<dyn Future<Output = Result<i64, String>> + Send + '_>> {
        Box::pin(async move { Ok(offset) })
    }
}

struct StopAfterFirstConsumer {
    first: AtomicBool,
}

impl RecordBatchConsumer for StopAfterFirstConsumer {
    fn consume<'a>(
        &'a self,
        _records: Vec<FetchedRecord>,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async move {
            if self.first.swap(false, Ordering::SeqCst) {
                Ok(())
            } else {
                Err("stop".to_string())
            }
        })
    }
}

#[tokio::test]
async fn m20_01_and_m20_02_storage_native_only_and_hybrid_histograms() {
    let cluster = FakeKafkaCluster::start(1, "t").await;
    let reg = Registry::new();

    // 1. StoragePusherMetrics & PusherConsumerMetrics
    let pusher_consumer = PusherConsumerMetrics::new(Some(&reg));
    let spm = &pusher_consumer.storage_pusher_metrics;
    spm.batch_age.observe(0.25);
    spm.time_series_per_flush.observe(1500.0);
    spm.shards_per_push.observe(4.0);
    spm.pushers_per_push.observe(2.0);
    spm.processing_time.with_label_values(&["timeseries"]).observe(0.04);
    pusher_consumer.processing_time_seconds.observe(0.12);

    assert_native_histogram(&reg, "cortex_ingest_storage_reader_pusher_batch_age_seconds", &[], 3, Some(0), None);
    assert_native_histogram(&reg, "cortex_ingest_storage_reader_pusher_timeseries_per_flush", &[], 3, Some(0), None);
    assert_native_histogram(&reg, "cortex_ingest_storage_reader_shards_per_push", &[], 3, Some(0), None);
    assert_native_histogram(&reg, "cortex_ingest_storage_reader_pushers_per_push", &[], 3, Some(0), None);
    assert_native_histogram(&reg, "cortex_ingest_storage_reader_records_processing_time_seconds", &[], 3, Some(11), None);

    // Classic text exposition for native-only StoragePusherMetrics must emit ONLY le="+Inf" (no DefBuckets).
    let mut text_buf = Vec::new();
    TextEncoder::new().encode(&reg.gather(), &mut text_buf).unwrap();
    let text = String::from_utf8(text_buf).unwrap();
    for name in [
        "cortex_ingest_storage_reader_pusher_batch_age_seconds",
        "cortex_ingest_storage_reader_pusher_processing_time_seconds",
        "cortex_ingest_storage_reader_pusher_timeseries_per_flush",
        "cortex_ingest_storage_reader_shards_per_push",
        "cortex_ingest_storage_reader_pushers_per_push",
    ] {
        assert!(
            !text.contains(&format!("{name}_bucket{{le=\"0.005\"")),
            "{name} must not emit bogus DefBuckets:\n{text}"
        );
        assert!(
            !text.contains(&format!("{name}_bucket{{content=\"timeseries\",le=\"0.005\"")),
            "{name} must not emit bogus DefBuckets:\n{text}"
        );
    }

    // 2. ReaderMetrics, FetcherMetrics, StrongReadConsistencyMetrics, PartitionCommitter
    let reader_m = ReaderMetrics::new_for_topic(Some(&reg), "ingest-topic");
    reader_m.consume_latency.observe(0.05);
    reader_m.estimated_bytes_per_record.observe(4096.0);
    reader_m.fetcher.fetch_max_bytes.observe(1_048_576.0);
    reader_m.fetcher.fetch_wait_duration.observe(0.02);
    reader_m.receive_delay.with_label_values(&["running"]).observe(0.5);
    reader_m.receive_and_consume_delay.with_label_values(&["running"]).observe(0.6);

    assert_native_histogram(&reg, "cortex_ingest_storage_reader_records_batch_process_duration_seconds", &[], 3, Some(0), None);
    assert_native_histogram(&reg, "cortex_ingest_storage_reader_estimated_bytes_per_record", &[], 3, Some(0), None);
    assert_native_histogram(&reg, "cortex_ingest_storage_reader_records_batch_fetch_max_bytes", &[], 3, Some(0), None);
    assert_native_histogram(&reg, "cortex_ingest_storage_reader_records_batch_wait_duration_seconds", &[], 3, Some(0), None);
    assert_native_histogram(
        &reg,
        "cortex_ingest_storage_reader_receive_delay_seconds",
        &[("phase", "running")],
        2,
        Some(18),
        Some(2.0_f64.powi(-10)),
    );
    assert_native_histogram(
        &reg,
        "cortex_ingest_storage_reader_receive_and_consume_delay_seconds",
        &[("phase", "running")],
        2,
        Some(18),
        Some(2.0_f64.powi(-10)),
    );

    reader_m.strong_consistency.latency.with_label_values(&["ingest-topic"]).observe(0.08);
    assert_native_histogram(
        &reg,
        "cortex_ingest_storage_strong_consistency_wait_duration_seconds",
        &[("component", "partition-reader"), ("topic", "ingest-topic")],
        3,
        Some(11),
        None,
    );

    let committer = PartitionCommitter::new(
        create_test_kafka_config(&cluster.addr, "t1"),
        Arc::new(DummyAdm),
        0,
        "grp",
        Arc::new(NoOpPreCommitNotifier),
        None,
        Some(&reg),
    );
    committer.commit(42).await.unwrap();
    assert_native_histogram(
        &reg,
        "cortex_ingest_storage_reader_offset_commit_request_duration_seconds",
        &[("partition", "0")],
        3,
        Some(11),
        None,
    );

    // 3. PartitionOffsetClientMetrics
    let poc = PartitionOffsetClientMetrics::new(Some(&reg));
    poc.last_produced_offset_latency.with_label_values(&["t1", "0"]).observe(0.02);
    poc.partition_start_offset_latency.with_label_values(&["t1", "0"]).observe(0.03);
    assert_native_histogram(
        &reg,
        "cortex_ingest_storage_reader_last_produced_offset_request_duration_seconds",
        &[("topic", "t1"), ("partition", "0")],
        3,
        Some(11),
        None,
    );
    assert_native_histogram(
        &reg,
        "cortex_ingest_storage_reader_partition_start_offset_request_duration_seconds",
        &[("topic", "t1"), ("partition", "0")],
        3,
        Some(11),
        None,
    );

    // 4. WriterMetrics & KafkaProducer
    let wm = WriterMetrics::new(Some(&reg));
    wm.latency.with_label_values(&["success"]).observe(0.1);
    wm.serialize_duration.observe(0.01);
    assert_native_histogram(
        &reg,
        "cortex_ingest_storage_writer_latency_seconds",
        &[("outcome", "success")],
        3,
        Some(11),
        None,
    );
    assert_native_histogram(&reg, "cortex_ingest_storage_writer_serialize_duration_seconds", &[], 3, Some(11), None);

    let kcfg = create_test_kafka_config(&cluster.addr, "t");
    let prod = KafkaProducer::from_config_with_prefix(
        &kcfg,
        DEFAULT_MAX_INFLIGHT_PRODUCE_REQUESTS,
        WRITER_METRICS_PREFIX,
        Some(&reg),
        KafkaWriterClientOptions {
            disable_default_topic: true,
            disable_linger: true,
        },
    )
    .unwrap();
    let ctx = ProduceContext::default().with_deadline(Instant::now() + Duration::from_secs(5));
    let _ = prod
        .produce_sync(
            &ctx,
            vec![WriterRecord {
                topic: "t".to_string(),
                partition: 0,
                key: Bytes::new(),
                value: Bytes::from_static(b"x"),
                headers: vec![],
                timestamp_ms: None,
                offset: 0,
            }],
        )
        .await;
    prod.close();
    assert_native_histogram(&reg, "cortex_ingest_storage_writer_produce_records_enqueue_duration_seconds", &[], 3, Some(11), None);
    assert_native_histogram(&reg, "cortex_ingest_storage_writer_produce_remaining_deadline_seconds", &[], 3, Some(11), None);

    // 5. BucketMetrics
    let dir = tempfile::tempdir().unwrap();
    let mut bcfg = BucketConfig::default();
    bcfg.backend_cfg.backend = "filesystem".to_string();
    bcfg.backend_cfg.filesystem.dir = dir.path().to_string_lossy().to_string();
    let bkt = BucketClient::new_client(&bcfg, "ingester", Some(&reg)).unwrap();
    bkt.upload("foo/bar", Bytes::from_static(b"hello-world")).await.unwrap();
    assert_native_histogram(
        &reg,
        "thanos_objstore_bucket_operation_transferred_bytes",
        &[("component", "ingester"), ("operation", "upload")],
        0,
        Some(16),
        None,
    );
    assert_native_histogram(
        &reg,
        "thanos_objstore_bucket_operation_duration_seconds",
        &[("component", "ingester"), ("operation", "upload")],
        3,
        Some(14),
        None,
    );

    cluster.close();
}

#[tokio::test]
async fn m20_08_kafka_client_extended_metrics_real_timings_and_exact_go_help() {
    let cluster = FakeKafkaCluster::start(1, "t").await;
    let reg = Registry::new();
    let ext = KafkaClientExtendedMetrics::new(
        "cortex_ingest_storage_reader_",
        Some(&reg),
        Some("partition-reader"),
    );
    let cfg = create_test_kafka_config(&cluster.addr, "t");
    let client = KafkaClient::from_config(&cfg)
        .unwrap()
        .with_extended_metrics(Some(ext));
    client.refresh_metadata(&["t"]).await.unwrap();

    let families = reg.gather();
    let expected_helps = [
        (
            "cortex_ingest_storage_reader_kafka_write_wait_seconds",
            "Time spent waiting to write to Kafka backend.",
        ),
        (
            "cortex_ingest_storage_reader_kafka_write_time_seconds",
            "Time spent writing to Kafka backend.",
        ),
        (
            "cortex_ingest_storage_reader_kafka_read_wait_seconds",
            "Time spent waiting to read from Kafka backend.",
        ),
        (
            "cortex_ingest_storage_reader_kafka_read_time_seconds",
            "Time spent reading from Kafka backend.",
        ),
        (
            "cortex_ingest_storage_reader_kafka_request_duration_e2e_seconds",
            "Time from the start of when a Kafka request is written to the end of when the response for that request was fully read from the Kafka backend.",
        ),
        (
            "cortex_ingest_storage_reader_kafka_request_throttled_seconds",
            "How long Kafka requests have been throttled by the Kafka client.",
        ),
    ];

    for (name, help) in expected_helps {
        let mf = families
            .iter()
            .find(|f| f.get_name() == name)
            .unwrap_or_else(|| panic!("missing {name}"));
        assert_eq!(mf.get_help(), help, "HELP mismatch for {name}");
    }

    for name in [
        "cortex_ingest_storage_reader_kafka_write_wait_seconds",
        "cortex_ingest_storage_reader_kafka_write_time_seconds",
        "cortex_ingest_storage_reader_kafka_read_wait_seconds",
        "cortex_ingest_storage_reader_kafka_read_time_seconds",
        "cortex_ingest_storage_reader_kafka_request_duration_e2e_seconds",
    ] {
        let m = find_metric(&families, name, &[("component", "partition-reader")]);
        let h = m.get_histogram();
        assert!(h.get_sample_count() >= 1, "{name}: count == 0");
        assert!(
            h.get_sample_sum() > 0.0,
            "{name}: sample_sum must be > 0.0 (got {})",
            h.get_sample_sum()
        );
        assert_native_histogram(&reg, name, &[("component", "partition-reader")], 3, Some(11), None);
    }

    cluster.close();
}

#[tokio::test]
async fn m20_10_storage_partition_ring_watcher_updates_all_three_gauges() {
    let reg = Registry::new();
    let kv = Arc::new(InMemoryPartitionKv::new());
    let table = Arc::new(PartitionTokenTable::new(10).unwrap());

    let watchers = PartitionRingWatchers::new(
        false,
        1,
        "ingester-partitions",
        "ingester-partitions",
        Arc::clone(&kv),
        Some(table),
        Some(&reg),
    )
    .unwrap();

    let f0 = reg.gather();
    assert_eq!(
        find_metric(&f0, "partition_ring_max_partition_id", &[("name", "ingester-partitions")])
            .get_gauge()
            .get_value(),
        -1.0
    );
    assert_eq!(
        find_metric(&f0, "partition_ring_max_derived_partition_id", &[("name", "ingester-partitions")])
            .get_gauge()
            .get_value(),
        -1.0
    );

    let mut desc = mimir_proto::ringpb::PartitionRingDesc::default();
    add_partition_with_stored_tokens(&mut desc, 2, PartitionState::PartitionActive, vec![100]);
    add_partition_with_derived_tokens(&mut desc, 5, PartitionState::PartitionPending);
    kv.put("ingester-partitions", desc.clone());

    watchers.start().await.unwrap();
    let f1 = reg.gather();
    assert_eq!(
        find_metric(&f1, "partition_ring_max_partition_id", &[("name", "ingester-partitions")])
            .get_gauge()
            .get_value(),
        5.0
    );
    assert_eq!(
        find_metric(&f1, "partition_ring_max_derived_partition_id", &[("name", "ingester-partitions")])
            .get_gauge()
            .get_value(),
        5.0
    );
    assert_eq!(
        find_metric(&f1, "partition_ring_partitions", &[("name", "ingester-partitions"), ("state", "Active")])
            .get_gauge()
            .get_value(),
        1.0
    );
    assert_eq!(
        find_metric(&f1, "partition_ring_partitions", &[("name", "ingester-partitions"), ("state", "Pending")])
            .get_gauge()
            .get_value(),
        1.0
    );
    assert_eq!(
        find_metric(&f1, "partition_ring_partitions", &[("name", "ingester-partitions"), ("state", "Inactive")])
            .get_gauge()
            .get_value(),
        0.0
    );

    add_partition_with_stored_tokens(&mut desc, 8, PartitionState::PartitionInactive, vec![200]);
    kv.put("ingester-partitions", desc);

    let dl = Instant::now() + Duration::from_secs(3);
    loop {
        let f2 = reg.gather();
        let max_id = find_metric(&f2, "partition_ring_max_partition_id", &[("name", "ingester-partitions")])
            .get_gauge()
            .get_value();
        if max_id == 8.0 {
            assert_eq!(
                find_metric(&f2, "partition_ring_max_derived_partition_id", &[("name", "ingester-partitions")])
                    .get_gauge()
                    .get_value(),
                5.0
            );
            assert_eq!(
                find_metric(&f2, "partition_ring_partitions", &[("name", "ingester-partitions"), ("state", "Inactive")])
                    .get_gauge()
                    .get_value(),
                1.0
            );
            break;
        }
        assert!(Instant::now() < dl, "watcher did not update gauges in time");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    watchers.stop().await;
}

#[tokio::test]
async fn w20_sdw_08_extreme_negative_and_epoch_timestamps_no_overflow_and_enforce_max_delay() {
    const TOPIC: &str = "t";
    const PARTITION_ID: i32 = 0;

    let cluster = FakeKafkaCluster::start(1, TOPIC).await;
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
    cfg.target_consumer_lag_at_startup = GoDuration::ZERO;
    cfg.max_consumer_lag_at_startup = GoDuration::ZERO;
    cfg.max_buffered_bytes = 2 * 1024 * 1024;
    cfg.fetch_concurrency_max = 1;

    let reg = Registry::new();
    let reader = SingleClusterPartitionReader::new(
        cfg,
        PARTITION_ID,
        "test-group",
        Some(dir.path().join("offset.json")),
        Arc::new(StopAfterFirstConsumer {
            first: AtomicBool::new(true),
        }),
        Some(&reg),
    )
    .unwrap();
    reader.start().await.unwrap();

    let payload = Bytes::from(vec![0xabu8; 512 * 1024]);
    cluster.produce_direct(TOPIC, PARTITION_ID, None, Some(payload.clone()), vec![], Some(i64::MIN));
    for _ in 0..6 {
        cluster.produce_direct(TOPIC, PARTITION_ID, None, Some(payload.clone()), vec![], Some(0));
    }

    let dl = Instant::now() + Duration::from_secs(5);
    while reader.highest_consumed_timestamp_before_partition_end_ms() == i64::MIN {
        assert!(Instant::now() < dl, "reader never recorded consumed timestamp");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    assert!(matches!(
        reader.enforce_read_max_delay(Duration::from_secs(60)),
        Err(ReaderError::MaxDelayExceeded)
    ));

    let families = reg.gather();
    let recv_delay = find_metric(
        &families,
        "cortex_ingest_storage_reader_receive_delay_seconds",
        &[("phase", "running")],
    );
    assert!(
        recv_delay.get_histogram().get_sample_sum() > 1e6,
        "expected positive clamped delay sum, got {}",
        recv_delay.get_histogram().get_sample_sum()
    );

    reader.stop().await;
    cluster.close();
}

#[tokio::test]
async fn w20_sdw_10_chunked_unix_socket_split_multibyte_utf8_no_panic() {
    let dir = tempfile::tempdir().unwrap();
    let sock_path = dir.path().join("creds.sock");
    let listener = UnixListener::bind(&sock_path).unwrap();

    let json = r#"{"token":"10€"}"#.as_bytes();
    let euro_pos = json.windows(3).position(|w| w == "€".as_bytes()).unwrap();
    let part1 = &json[..euro_pos + 2];
    let part2 = &json[euro_pos + 2..];

    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut req_buf = [0u8; 256];
        let _ = stream.read(&mut req_buf).await.unwrap();
        let mut resp = Vec::new();
        resp.extend_from_slice(
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nContent-Type: application/json\r\n\r\n",
        );
        resp.extend_from_slice(format!("{:x}\r\n", part1.len()).as_bytes());
        resp.extend_from_slice(part1);
        resp.extend_from_slice(b"\r\n");
        resp.extend_from_slice(format!("{:x}\r\n", part2.len()).as_bytes());
        resp.extend_from_slice(part2);
        resp.extend_from_slice(b"\r\n0\r\n\r\n");
        stream.write_all(&resp).await.unwrap();
        stream.shutdown().await.unwrap();
    });

    let res: BTreeMap<String, String> =
        request_json_from_socket(sock_path.to_str().unwrap(), Duration::from_secs(2))
            .await
            .unwrap();
    assert_eq!(res.get("token").map(String::as_str), Some("10€"));
    server.await.unwrap();
}
