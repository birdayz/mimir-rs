#[path = "upstream_storage_part2_helpers.rs"]
mod helpers;

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use helpers::{
    create_test_kafka_config, get_counter_value, get_histogram_count_and_sum, has_metric,
    mock_timeseries, FakeKafkaCluster, HookAction,
};
use kafka_protocol::messages::ProduceResponse;
use kafka_protocol::records::RecordBatchDecoder;
use mimir_config::GoDuration;
use mimir_proto::cortexpb;
use mimir_proto::prost::Message;
use mimir_proto::symbols::{v2_common_symbols, V2_RECORD_SYMBOL_OFFSET};
use mimir_proto::zerocopy::{from_write_request_to_rw2, WriteRequestScratch};
use mimir_storage::ingest::compression::decompress_record_batch;
use mimir_storage::ingest::{
    deserialize_record_content, marshal_write_request_to_records, parse_record_version,
    produce_results_err, split_write_request_by_max_marshal_size,
    split_write_request_by_max_marshal_size_rw2, IngestKafkaClient, KafkaClient, PartitionWriteRequest,
    ProduceContext, ProduceResult, ProducerError, RecordSerializer,
    RecordVersion, Writer, WriterError, WriterOptions, WriterRecord, KAFKA_OFFSET_START,
    MAX_PRODUCER_RECORD_DATA_BYTES_LIMIT, PRODUCER_BATCH_MAX_BYTES,
};
use parking_lot::Mutex as SyncMutex;
use prometheus::Registry;
use tokio::sync::Notify;

async fn create_test_writer(
    cfg: mimir_config::KafkaConfig,
    opts: WriterOptions,
) -> (Arc<Writer>, Registry) {
    let reg = Registry::new();
    let w = Arc::new(Writer::new(cfg, Some(&reg), opts).unwrap());
    w.start().await.unwrap();
    (w, reg)
}

fn produce_request_records_count(req: &kafka_protocol::messages::ProduceRequest) -> usize {
    let mut count = 0usize;
    for t in &req.topic_data {
        for p in &t.partition_data {
            if let Some(ref raw) = p.records {
                let mut b = raw.clone();
                if let Ok(recs) = RecordBatchDecoder::decode_with_custom_compression(
                    &mut b,
                    Some(decompress_record_batch),
                ) {
                    count += recs.len();
                }
            }
        }
    }
    count
}

// Upstream: pkg/storage/ingest/writer_test.go::TestWriter_WriteSync
#[tokio::test]
async fn test_writer_write_sync() {
    const TOPIC: &str = "test";
    const NUM_PARTITIONS: i32 = 10;
    const PARTITION_ID: i32 = 0;
    const TENANT_ID: &str = "user-1";

    let multi_series = vec![mock_timeseries("series_1"), mock_timeseries("series_2")];
    let series1 = vec![mock_timeseries("series_1")];
    let series2 = vec![mock_timeseries("series_2")];
    let series3 = vec![mock_timeseries("series_3")];

    // 1. "should block until data has been committed to storage (WriteRequest stored in a single record)"
    {
        let cluster = FakeKafkaCluster::start(NUM_PARTITIONS, TOPIC).await;
        let processed = Arc::new(AtomicBool::new(false));
        let proc_c = Arc::clone(&processed);
        *cluster.hooks.produce.lock() = Some(Arc::new(move |_| {
            let p = Arc::clone(&proc_c);
            Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(150)).await;
                p.store(true, Ordering::SeqCst);
                HookAction::PassThrough
            })
        }));

        let cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        let (writer, reg) = create_test_writer(cfg.clone(), WriterOptions::default()).await;

        let req = cortexpb::WriteRequest {
            timeseries: multi_series.clone(),
            source: cortexpb::write_request::SourceEnum::Api as i32,
            ..Default::default()
        };
        let input_size = req.encoded_len();
        writer
            .write_sync(&ProduceContext::background(), TOPIC, PARTITION_ID, TENANT_ID, &req)
            .await
            .unwrap();
        assert!(processed.load(Ordering::SeqCst));

        // Read back from Kafka
        let reader_client = KafkaClient::from_config(&cfg).unwrap();
        let fetched = reader_client
            .fetch(TOPIC, PARTITION_ID, 0, Duration::from_millis(200), 1, 10_000_000)
            .await
            .unwrap();
        assert_eq!(fetched.records.len(), 1);
        assert_eq!(fetched.records[0].key.as_ref(), TENANT_ID.as_bytes());

        let mut scratch = WriteRequestScratch::new();
        let v = parse_record_version(
            fetched.records[0]
                .headers
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_ref())),
        );
        let received = deserialize_record_content(&fetched.records[0].value, v, &mut scratch)
            .unwrap()
            .to_owned_write_request();
        assert_eq!(received.timeseries, multi_series);

        assert_eq!(
            get_counter_value(&reg, "cortex_ingest_storage_writer_input_bytes_total", &[]),
            input_size as f64
        );
        assert_eq!(
            get_counter_value(&reg, "cortex_ingest_storage_writer_sent_bytes_total", &[]),
            fetched.records[0].value.len() as f64
        );
        let (rpr_cnt, rpr_sum) = get_histogram_count_and_sum(
            &reg,
            "cortex_ingest_storage_writer_records_per_write_request",
            &[],
        )
        .unwrap();
        assert_eq!(rpr_cnt, 1);
        assert_eq!(rpr_sum, 1.0);
        assert_eq!(
            get_counter_value(
                &reg,
                "cortex_ingest_storage_writer_produce_records_enqueued_total",
                &[]
            ),
            1.0
        );
        let (ser_cnt, _) = get_histogram_count_and_sum(
            &reg,
            "cortex_ingest_storage_writer_serialize_duration_seconds",
            &[],
        )
        .unwrap();
        assert_eq!(ser_cnt, 1);

        writer.stop();
        cluster.close();
    }

    // 2. "should block until data has been committed to storage (WriteRequest stored in multiple records)"
    {
        let cluster = FakeKafkaCluster::start(NUM_PARTITIONS, TOPIC).await;
        let processed = Arc::new(AtomicBool::new(false));
        let proc_c = Arc::clone(&processed);
        *cluster.hooks.produce.lock() = Some(Arc::new(move |_| {
            let p = Arc::clone(&proc_c);
            Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(100)).await;
                p.store(true, Ordering::SeqCst);
                HookAction::PassThrough
            })
        }));

        let expected_req = cortexpb::WriteRequest {
            timeseries: multi_series.clone(),
            source: cortexpb::write_request::SourceEnum::Api as i32,
            ..Default::default()
        };
        let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        cfg.producer_record_version = 1;
        cfg.producer_max_record_size_bytes = ((expected_req.encoded_len() as f64) * 0.7) as i64;
        let (writer, reg) = create_test_writer(cfg.clone(), WriterOptions::default()).await;

        writer
            .write_sync(
                &ProduceContext::background(),
                TOPIC,
                PARTITION_ID,
                TENANT_ID,
                &expected_req,
            )
            .await
            .unwrap();
        assert!(processed.load(Ordering::SeqCst));

        let reader_client = KafkaClient::from_config(&cfg).unwrap();
        let fetched = reader_client
            .fetch(TOPIC, PARTITION_ID, 0, Duration::from_millis(200), 1, 10_000_000)
            .await
            .unwrap();
        assert_eq!(fetched.records.len(), 2);

        let mut scratch = WriteRequestScratch::new();
        let mut merged_ts = Vec::new();
        let mut sent_bytes = 0usize;
        for rec in &fetched.records {
            assert_eq!(rec.key.as_ref(), TENANT_ID.as_bytes());
            sent_bytes += rec.value.len();
            let v = parse_record_version(rec.headers.iter().map(|(k, v)| (k.as_str(), v.as_ref())));
            let dec = deserialize_record_content(&rec.value, v, &mut scratch)
                .unwrap()
                .to_owned_write_request();
            assert_eq!(dec.source, expected_req.source);
            merged_ts.extend(dec.timeseries);
        }
        assert_eq!(merged_ts, expected_req.timeseries);
        assert_eq!(
            get_counter_value(&reg, "cortex_ingest_storage_writer_sent_bytes_total", &[]),
            sent_bytes as f64
        );
        assert_eq!(
            get_counter_value(
                &reg,
                "cortex_ingest_storage_writer_produce_records_enqueued_total",
                &[]
            ),
            2.0
        );

        writer.stop();
        cluster.close();
    }

    // 3. "should write to the requested partition"
    {
        let cluster = FakeKafkaCluster::start(NUM_PARTITIONS, TOPIC).await;
        let cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        let (writer, reg) = create_test_writer(cfg.clone(), WriterOptions::default()).await;

        let req1 = cortexpb::WriteRequest {
            timeseries: series1.clone(),
            source: cortexpb::write_request::SourceEnum::Api as i32,
            ..Default::default()
        };
        let req2 = cortexpb::WriteRequest {
            timeseries: series2.clone(),
            source: cortexpb::write_request::SourceEnum::Api as i32,
            ..Default::default()
        };
        writer
            .write_sync(&ProduceContext::background(), TOPIC, 0, TENANT_ID, &req1)
            .await
            .unwrap();
        writer
            .write_sync(&ProduceContext::background(), TOPIC, 1, TENANT_ID, &req2)
            .await
            .unwrap();

        let reader_client = KafkaClient::from_config(&cfg).unwrap();
        let mut scratch = WriteRequestScratch::new();
        for (pid, exp) in [(0, &series1), (1, &series2)] {
            let fetched = reader_client
                .fetch(TOPIC, pid, 0, Duration::from_millis(200), 1, 10_000_000)
                .await
                .unwrap();
            assert_eq!(fetched.records.len(), 1);
            let v = parse_record_version(
                fetched.records[0]
                    .headers
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.as_ref())),
            );
            let dec = deserialize_record_content(&fetched.records[0].value, v, &mut scratch)
                .unwrap()
                .to_owned_write_request();
            assert_eq!(&dec.timeseries, exp);
        }
        assert_eq!(
            get_counter_value(
                &reg,
                "cortex_ingest_storage_writer_produce_records_enqueued_total",
                &[]
            ),
            2.0
        );

        writer.stop();
        cluster.close();
    }

    // 4. "should interrupt the WriteSync() on context cancelled but other concurrent requests should not fail"
    {
        let cluster = FakeKafkaCluster::start(NUM_PARTITIONS, TOPIC).await;
        let first_req = Arc::new(AtomicBool::new(true));
        let first_received = Arc::new(Notify::new());
        let first_received_flag = Arc::new(AtomicBool::new(false));
        let batches_len = Arc::new(SyncMutex::new(Vec::<usize>::new()));

        let fr = Arc::clone(&first_req);
        let frec = Arc::clone(&first_received);
        let fflag = Arc::clone(&first_received_flag);
        let bl = Arc::clone(&batches_len);
        *cluster.hooks.produce.lock() = Some(Arc::new(move |req| {
            let fr = Arc::clone(&fr);
            let frec = Arc::clone(&frec);
            let fflag = Arc::clone(&fflag);
            let bl = Arc::clone(&bl);
            Box::pin(async move {
                let n = produce_request_records_count(&req);
                bl.lock().push(n);
                if fr.swap(false, Ordering::SeqCst) {
                    fflag.store(true, Ordering::SeqCst);
                    frec.notify_waiters();
                    tokio::time::sleep(Duration::from_millis(400)).await;
                }
                HookAction::PassThrough
            })
        }));

        let cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        let (writer, _) = create_test_writer(
            cfg,
            WriterOptions {
                max_inflight_produce_requests: Some(1),
                ..Default::default()
            },
        )
        .await;

        let w1 = Arc::clone(&writer);
        let s1 = series1.clone();
        let h1 = tokio::spawn(async move {
            w1.write_sync(
                &ProduceContext::background(),
                TOPIC,
                PARTITION_ID,
                TENANT_ID,
                &cortexpb::WriteRequest {
                    timeseries: s1,
                    source: cortexpb::write_request::SourceEnum::Api as i32,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        });

        while !first_received_flag.load(Ordering::SeqCst) {
            let n = first_received.notified();
            if first_received_flag.load(Ordering::SeqCst) {
                break;
            }
            n.await;
        }

        let w2 = Arc::clone(&writer);
        let s2 = series2.clone();
        let h2 = tokio::spawn(async move {
            let ctx = ProduceContext::background().with_timeout(Duration::from_millis(10));
            let err = w2
                .write_sync(
                    &ctx,
                    TOPIC,
                    PARTITION_ID,
                    TENANT_ID,
                    &cortexpb::WriteRequest {
                        timeseries: s2,
                        source: cortexpb::write_request::SourceEnum::Api as i32,
                        ..Default::default()
                    },
                )
                .await
                .unwrap_err();
            assert!(err.to_string().contains("deadline exceeded"));
        });

        let w3 = Arc::clone(&writer);
        let s3 = series3.clone();
        let h3 = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            w3.write_sync(
                &ProduceContext::background(),
                TOPIC,
                PARTITION_ID,
                TENANT_ID,
                &cortexpb::WriteRequest {
                    timeseries: s3,
                    source: cortexpb::write_request::SourceEnum::Api as i32,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        });

        h1.await.unwrap();
        h2.await.unwrap();
        h3.await.unwrap();
        assert_eq!(*batches_len.lock(), vec![1, 2]);

        writer.stop();
        cluster.close();
    }

    // 5. "should batch multiple subsequent records together while sending the previous batches to Kafka once max in-flight Produce requests limit has been reached"
    {
        let cluster = FakeKafkaCluster::start(NUM_PARTITIONS, TOPIC).await;
        let first_req = Arc::new(AtomicBool::new(true));
        let first_received = Arc::new(Notify::new());
        let first_received_flag = Arc::new(AtomicBool::new(false));
        let batches_len = Arc::new(SyncMutex::new(Vec::<usize>::new()));

        let fr = Arc::clone(&first_req);
        let frec = Arc::clone(&first_received);
        let fflag = Arc::clone(&first_received_flag);
        let bl = Arc::clone(&batches_len);
        *cluster.hooks.produce.lock() = Some(Arc::new(move |req| {
            let fr = Arc::clone(&fr);
            let frec = Arc::clone(&frec);
            let fflag = Arc::clone(&fflag);
            let bl = Arc::clone(&bl);
            Box::pin(async move {
                if fr.swap(false, Ordering::SeqCst) {
                    fflag.store(true, Ordering::SeqCst);
                    frec.notify_waiters();
                    tokio::time::sleep(Duration::from_millis(350)).await;
                }
                let n = produce_request_records_count(&req);
                bl.lock().push(n);
                HookAction::PassThrough
            })
        }));

        let cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        let (writer, _) = create_test_writer(
            cfg,
            WriterOptions {
                max_inflight_produce_requests: Some(1),
                ..Default::default()
            },
        )
        .await;

        let w1 = Arc::clone(&writer);
        let s1 = series1.clone();
        let h1 = tokio::spawn(async move {
            w1.write_sync(
                &ProduceContext::background(),
                TOPIC,
                PARTITION_ID,
                TENANT_ID,
                &cortexpb::WriteRequest {
                    timeseries: s1,
                    source: cortexpb::write_request::SourceEnum::Api as i32,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        });

        while !first_received_flag.load(Ordering::SeqCst) {
            let n = first_received.notified();
            if first_received_flag.load(Ordering::SeqCst) {
                break;
            }
            n.await;
        }

        let w2 = Arc::clone(&writer);
        let s2 = series2.clone();
        let h2 = tokio::spawn(async move {
            w2.write_sync(
                &ProduceContext::background(),
                TOPIC,
                PARTITION_ID,
                TENANT_ID,
                &cortexpb::WriteRequest {
                    timeseries: s2,
                    source: cortexpb::write_request::SourceEnum::Api as i32,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        });

        let w3 = Arc::clone(&writer);
        let s3 = series3.clone();
        let h3 = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            w3.write_sync(
                &ProduceContext::background(),
                TOPIC,
                PARTITION_ID,
                TENANT_ID,
                &cortexpb::WriteRequest {
                    timeseries: s3,
                    source: cortexpb::write_request::SourceEnum::Api as i32,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        });

        h1.await.unwrap();
        h2.await.unwrap();
        h3.await.unwrap();
        assert_eq!(*batches_len.lock(), vec![1, 2]);

        writer.stop();
        cluster.close();
    }

    // 6. "should return error on non existing partition"
    {
        let cluster = FakeKafkaCluster::start(NUM_PARTITIONS, TOPIC).await;
        let cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        let (writer, reg) = create_test_writer(cfg, WriterOptions::default()).await;

        let err = writer
            .write_sync(
                &ProduceContext::background(),
                TOPIC,
                100,
                TENANT_ID,
                &cortexpb::WriteRequest {
                    timeseries: multi_series.clone(),
                    source: cortexpb::write_request::SourceEnum::Api as i32,
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
        let _ = err;

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
                &[("reason", "other")]
            ),
            1.0
        );

        writer.stop();
        cluster.close();
    }

    // 7. "should return an error and stop retrying sending a record once the write timeout expires"
    {
        let cluster = FakeKafkaCluster::start(NUM_PARTITIONS, TOPIC).await;
        let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        cfg.write_timeout = GoDuration::from_millis(250);
        cfg.write_timeout_overhead = GoDuration::from_millis(100);

        *cluster.hooks.produce.lock() = Some(Arc::new(|_req| {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(60)).await;
                HookAction::Respond(ProduceResponse::default()) // missing partition => retriable failure
            })
        }));

        let (writer, reg) = create_test_writer(cfg, WriterOptions::default()).await;
        let start = Instant::now();
        let err = writer
            .write_sync(
                &ProduceContext::background(),
                TOPIC,
                PARTITION_ID,
                TENANT_ID,
                &cortexpb::WriteRequest {
                    timeseries: series1.clone(),
                    source: cortexpb::write_request::SourceEnum::Api as i32,
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
        let elapsed = start.elapsed();
        assert!(matches!(
            err,
            WriterError::FailedPartitions {
                source: ProducerError::RecordTimeout,
                ..
            }
        ));
        assert!(elapsed < Duration::from_secs(2));
        assert!(elapsed >= Duration::from_millis(120));

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
                &[("reason", "timeout")]
            ),
            1.0
        );

        writer.stop();
        cluster.close();
    }

    // 8. "should fail all buffered records and close the connection on timeout while waiting for Produce response (franz-go client)"
    {
        let cluster = FakeKafkaCluster::start(NUM_PARTITIONS, TOPIC).await;
        let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        cfg.write_timeout = GoDuration::from_millis(200);
        cfg.write_timeout_overhead = GoDuration::from_millis(100);

        let first = Arc::new(AtomicBool::new(true));
        let first_rx = Arc::new(Notify::new());
        let first_rx_flag = Arc::new(AtomicBool::new(false));
        let f_c = Arc::clone(&first);
        let frx_c = Arc::clone(&first_rx);
        let fflag_c = Arc::clone(&first_rx_flag);
        *cluster.hooks.produce.lock() = Some(Arc::new(move |_| {
            let f = Arc::clone(&f_c);
            let n = Arc::clone(&frx_c);
            let fl = Arc::clone(&fflag_c);
            Box::pin(async move {
                if f.swap(false, Ordering::SeqCst) {
                    fl.store(true, Ordering::SeqCst);
                    n.notify_waiters();
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                HookAction::PassThrough
            })
        }));

        let (writer, _) = create_test_writer(
            cfg,
            WriterOptions {
                max_inflight_produce_requests: Some(1),
                ..Default::default()
            },
        )
        .await;

        let w1 = Arc::clone(&writer);
        let s1 = series1.clone();
        let h1 = tokio::spawn(async move {
            let err = w1
                .write_sync(
                    &ProduceContext::background(),
                    TOPIC,
                    PARTITION_ID,
                    TENANT_ID,
                    &cortexpb::WriteRequest {
                        timeseries: s1,
                        source: cortexpb::write_request::SourceEnum::Api as i32,
                        ..Default::default()
                    },
                )
                .await
                .unwrap_err();
            assert!(matches!(
                err,
                WriterError::FailedPartitions {
                    source: ProducerError::RecordTimeout,
                    ..
                }
            ));
        });

        while !first_rx_flag.load(Ordering::SeqCst) {
            let n = first_rx.notified();
            if first_rx_flag.load(Ordering::SeqCst) {
                break;
            }
            n.await;
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
        let err2 = writer
            .write_sync(
                &ProduceContext::background(),
                TOPIC,
                PARTITION_ID,
                TENANT_ID,
                &cortexpb::WriteRequest {
                    timeseries: series2.clone(),
                    source: cortexpb::write_request::SourceEnum::Api as i32,
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(
            err2,
            WriterError::FailedPartitions {
                source: ProducerError::RecordTimeout,
                ..
            }
        ));

        h1.await.unwrap();
        writer.stop();
        cluster.close();
    }

    // 9. "should return error if the WriteRequest contains a timeseries which is larger than the maximum allowed record data size"
    {
        let cluster = FakeKafkaCluster::start(NUM_PARTITIONS, TOPIC).await;
        let processed = Arc::new(AtomicBool::new(false));
        let proc_c = Arc::clone(&processed);
        *cluster.hooks.produce.lock() = Some(Arc::new(move |_| {
            let p = Arc::clone(&proc_c);
            Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(100)).await;
                p.store(true, Ordering::SeqCst);
                HookAction::PassThrough
            })
        }));

        let req = cortexpb::WriteRequest {
            timeseries: vec![
                mock_timeseries(&"x".repeat(PRODUCER_BATCH_MAX_BYTES)),
                mock_timeseries("series_1"),
            ],
            source: cortexpb::write_request::SourceEnum::Api as i32,
            ..Default::default()
        };

        let cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        let (writer, reg) = create_test_writer(cfg.clone(), WriterOptions::default()).await;

        let err = writer
            .write_sync(&ProduceContext::background(), TOPIC, PARTITION_ID, TENANT_ID, &req)
            .await
            .unwrap_err();
        assert!(matches!(err, WriterError::WriteRequestDataItemTooLarge));
        assert!(processed.load(Ordering::SeqCst));

        // Small series was still ingested
        let reader_client = KafkaClient::from_config(&cfg).unwrap();
        let fetched = reader_client
            .fetch(TOPIC, PARTITION_ID, 0, Duration::from_millis(200), 1, 10_000_000)
            .await
            .unwrap();
        assert_eq!(fetched.records.len(), 1);
        let mut scratch = WriteRequestScratch::new();
        let v = parse_record_version(
            fetched.records[0]
                .headers
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_ref())),
        );
        let dec = deserialize_record_content(&fetched.records[0].value, v, &mut scratch)
            .unwrap()
            .to_owned_write_request();
        assert_eq!(dec.timeseries, vec![mock_timeseries("series_1")]);

        // Metrics: input/sent bytes stay 0 on partial failure
        assert_eq!(
            get_counter_value(&reg, "cortex_ingest_storage_writer_input_bytes_total", &[]),
            0.0
        );
        assert_eq!(
            get_counter_value(&reg, "cortex_ingest_storage_writer_sent_bytes_total", &[]),
            0.0
        );
        let (rpr_cnt, rpr_sum) = get_histogram_count_and_sum(
            &reg,
            "cortex_ingest_storage_writer_records_per_write_request",
            &[],
        )
        .unwrap();
        assert_eq!(rpr_cnt, 1);
        assert_eq!(rpr_sum, 2.0);
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
                &[("reason", "record-too-large")]
            ),
            1.0
        );

        writer.stop();
        cluster.close();
    }

    // 10. "should not block the WriteSync() because Kafka buffer is full"
    {
        let cluster = FakeKafkaCluster::start(NUM_PARTITIONS, TOPIC).await;
        let write_req = cortexpb::WriteRequest {
            timeseries: series1.clone(),
            source: cortexpb::write_request::SourceEnum::Api as i32,
            ..Default::default()
        };
        let (recs, _) = RecordSerializer::new(RecordVersion::V2)
            .to_records(
                TOPIC,
                PARTITION_ID,
                TENANT_ID,
                &write_req,
                MAX_PRODUCER_RECORD_DATA_BYTES_LIMIT,
            )
            .unwrap();
        assert_eq!(recs.len(), 1);
        let est_size = recs[0].value.len();

        let unblock = Arc::new(Notify::new());
        let unblocked = Arc::new(AtomicBool::new(false));
        let records_received = Arc::new(AtomicI64::new(0));

        let u_c = Arc::clone(&unblock);
        let ub_c = Arc::clone(&unblocked);
        let rr_c = Arc::clone(&records_received);
        *cluster.hooks.produce.lock() = Some(Arc::new(move |req| {
            let u = Arc::clone(&u_c);
            let ub = Arc::clone(&ub_c);
            let rr = Arc::clone(&rr_c);
            Box::pin(async move {
                rr.fetch_add(produce_request_records_count(&req) as i64, Ordering::SeqCst);
                while !ub.load(Ordering::SeqCst) {
                    u.notified().await;
                }
                HookAction::PassThrough
            })
        }));

        let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        cfg.write_timeout = GoDuration::from_millis(300);
        cfg.write_timeout_overhead = GoDuration::from_millis(100);
        cfg.producer_max_buffered_bytes = ((est_size * 4) - 1) as i64;
        let (writer, reg) = create_test_writer(cfg, WriterOptions::default()).await;

        let errs = Arc::new(SyncMutex::new(Vec::<WriterError>::new()));
        let mut handles = Vec::new();
        for i in 0..10 {
            let w = Arc::clone(&writer);
            let r = write_req.clone();
            let errs_c = Arc::clone(&errs);
            handles.push(tokio::spawn(async move {
                if let Err(e) = w
                    .write_sync(&ProduceContext::background(), TOPIC, i, TENANT_ID, &r)
                    .await
                {
                    errs_c.lock().push(e);
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        let (timeout_cnt, buf_full_cnt) = {
            let guard = errs.lock();
            let timeout_cnt = guard
                .iter()
                .filter(|e| {
                    matches!(
                        e,
                        WriterError::FailedPartitions {
                            source: ProducerError::RecordTimeout,
                            ..
                        }
                    )
                })
                .count();
            let buf_full_cnt = guard
                .iter()
                .filter(|e| {
                    matches!(
                        e,
                        WriterError::FailedPartitions {
                            source: ProducerError::MaxBuffered,
                            ..
                        }
                    )
                })
                .count();
            (timeout_cnt, buf_full_cnt)
        };

        assert_eq!(timeout_cnt, 3);
        assert_eq!(buf_full_cnt, 7);
        assert_eq!(records_received.load(Ordering::SeqCst), 3);

        assert_eq!(
            get_counter_value(
                &reg,
                "cortex_ingest_storage_writer_produce_records_enqueued_total",
                &[]
            ),
            10.0
        );
        assert_eq!(
            get_counter_value(
                &reg,
                "cortex_ingest_storage_writer_produce_records_failed_total",
                &[("reason", "buffer-full")]
            ),
            7.0
        );
        assert_eq!(
            get_counter_value(
                &reg,
                "cortex_ingest_storage_writer_produce_records_failed_total",
                &[("reason", "timeout")]
            ),
            3.0
        );

        unblocked.store(true, Ordering::SeqCst);
        unblock.notify_waiters();

        for i in 0..3 {
            writer
                .write_sync(&ProduceContext::background(), TOPIC, i, TENANT_ID, &write_req)
                .await
                .unwrap();
        }
        assert_eq!(
            get_counter_value(
                &reg,
                "cortex_ingest_storage_writer_produce_records_enqueued_total",
                &[]
            ),
            13.0
        );

        writer.stop();
        cluster.close();
    }

    // 11. "should not panic if WriteSync() is called after the writer has been stopped"
    {
        let cluster = FakeKafkaCluster::start(NUM_PARTITIONS, TOPIC).await;
        let cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        let (writer, _) = create_test_writer(cfg, WriterOptions::default()).await;

        let req = cortexpb::WriteRequest {
            timeseries: multi_series,
            source: cortexpb::write_request::SourceEnum::Api as i32,
            ..Default::default()
        };
        writer
            .write_sync(&ProduceContext::background(), TOPIC, PARTITION_ID, TENANT_ID, &req)
            .await
            .unwrap();
        writer.stop();

        let err = writer
            .write_sync(&ProduceContext::background(), TOPIC, PARTITION_ID, TENANT_ID, &req)
            .await
            .unwrap_err();
        assert!(matches!(err, WriterError::NotRunning));
        cluster.close();
    }
}

// Upstream: pkg/storage/ingest/writer_test.go::TestWriter_MultiWriteSync
#[tokio::test]
async fn test_writer_multi_write_sync() {
    const TOPIC: &str = "test";
    const NUM_PARTITIONS: i32 = 10;
    const TENANT_ID: &str = "user-1";

    let series1 = vec![mock_timeseries("series_1")];
    let series2 = vec![mock_timeseries("series_2")];
    let series3 = vec![mock_timeseries("series_3")];

    // "should write records to multiple partitions"
    {
        let cluster = FakeKafkaCluster::start(NUM_PARTITIONS, TOPIC).await;
        let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        cfg.producer_record_version = 1;
        let (writer, reg) = create_test_writer(cfg.clone(), WriterOptions::default()).await;

        let req1 = cortexpb::WriteRequest {
            timeseries: series1.clone(),
            source: cortexpb::write_request::SourceEnum::Api as i32,
            ..Default::default()
        };
        let req2 = cortexpb::WriteRequest {
            timeseries: series2.clone(),
            source: cortexpb::write_request::SourceEnum::Api as i32,
            ..Default::default()
        };
        let req3 = cortexpb::WriteRequest {
            timeseries: series3.clone(),
            source: cortexpb::write_request::SourceEnum::Api as i32,
            ..Default::default()
        };
        let total_input = req1.encoded_len() + req2.encoded_len() + req3.encoded_len();

        let prs = vec![
            PartitionWriteRequest {
                partition_id: 0,
                write_request: &req1,
            },
            PartitionWriteRequest {
                partition_id: 1,
                write_request: &req2,
            },
            PartitionWriteRequest {
                partition_id: 2,
                write_request: &req3,
            },
        ];
        writer
            .multi_write_sync(&ProduceContext::background(), TOPIC, TENANT_ID, &prs)
            .await
            .unwrap();

        let reader_client = KafkaClient::from_config(&cfg).unwrap();
        let mut total_sent = 0usize;
        for pr in &prs {
            let fetched = reader_client
                .fetch(
                    TOPIC,
                    pr.partition_id,
                    0,
                    Duration::from_millis(200),
                    1,
                    10_000_000,
                )
                .await
                .unwrap();
            assert_eq!(fetched.records.len(), 1);
            total_sent += fetched.records[0].value.len();
            let decoded = cortexpb::WriteRequest::decode(fetched.records[0].value.as_ref()).unwrap();
            assert_eq!(decoded.timeseries, pr.write_request.timeseries);
        }

        assert_eq!(
            get_counter_value(&reg, "cortex_ingest_storage_writer_input_bytes_total", &[]),
            total_input as f64
        );
        assert_eq!(
            get_counter_value(&reg, "cortex_ingest_storage_writer_sent_bytes_total", &[]),
            total_sent as f64
        );
        let (rpr_cnt, rpr_sum) = get_histogram_count_and_sum(
            &reg,
            "cortex_ingest_storage_writer_records_per_write_request",
            &[],
        )
        .unwrap();
        assert_eq!(rpr_cnt, 3);
        assert_eq!(rpr_sum, 3.0);
        assert_eq!(
            get_counter_value(
                &reg,
                "cortex_ingest_storage_writer_produce_records_enqueued_total",
                &[]
            ),
            3.0
        );

        writer.stop();
        cluster.close();
    }

    // "should skip empty requests"
    {
        let cluster = FakeKafkaCluster::start(NUM_PARTITIONS, TOPIC).await;
        let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        cfg.producer_record_version = 1;
        let (writer, reg) = create_test_writer(cfg.clone(), WriterOptions::default()).await;

        let empty = cortexpb::WriteRequest::default();
        let non_empty = cortexpb::WriteRequest {
            timeseries: series1.clone(),
            source: cortexpb::write_request::SourceEnum::Api as i32,
            ..Default::default()
        };
        let input_size = non_empty.encoded_len();
        let prs = vec![
            PartitionWriteRequest {
                partition_id: 0,
                write_request: &empty,
            },
            PartitionWriteRequest {
                partition_id: 1,
                write_request: &non_empty,
            },
        ];
        writer
            .multi_write_sync(&ProduceContext::background(), TOPIC, TENANT_ID, &prs)
            .await
            .unwrap();

        let reader_client = KafkaClient::from_config(&cfg).unwrap();
        let fetched = reader_client
            .fetch(TOPIC, 1, 0, Duration::from_millis(200), 1, 10_000_000)
            .await
            .unwrap();
        assert_eq!(fetched.records.len(), 1);
        assert_eq!(
            get_counter_value(&reg, "cortex_ingest_storage_writer_input_bytes_total", &[]),
            input_size as f64
        );
        assert_eq!(
            get_counter_value(&reg, "cortex_ingest_storage_writer_sent_bytes_total", &[]),
            fetched.records[0].value.len() as f64
        );
        let (rpr_cnt, rpr_sum) = get_histogram_count_and_sum(
            &reg,
            "cortex_ingest_storage_writer_records_per_write_request",
            &[],
        )
        .unwrap();
        assert_eq!(rpr_cnt, 1);
        assert_eq!(rpr_sum, 1.0);

        writer.stop();
        cluster.close();
    }

    // "should return nil when all partition requests are empty"
    {
        let cluster = FakeKafkaCluster::start(NUM_PARTITIONS, TOPIC).await;
        let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        cfg.producer_record_version = 1;
        let (writer, _) = create_test_writer(cfg, WriterOptions::default()).await;

        let empty = cortexpb::WriteRequest::default();
        let prs = vec![
            PartitionWriteRequest {
                partition_id: 0,
                write_request: &empty,
            },
            PartitionWriteRequest {
                partition_id: 1,
                write_request: &empty,
            },
        ];
        writer
            .multi_write_sync(&ProduceContext::background(), TOPIC, TENANT_ID, &prs)
            .await
            .unwrap();

        writer.stop();
        cluster.close();
    }
}

// Upstream: pkg/storage/ingest/writer_test.go::TestWriter_WriteSync_HighConcurrencyOnKafkaClientBufferFull
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_writer_write_sync_high_concurrency_on_kafka_client_buffer_full() {
    const TOPIC: &str = "test";
    const NUM_WORKERS: usize = 30;
    const TENANT_ID: &str = "user-1";

    let cluster = FakeKafkaCluster::start(1, TOPIC).await;
    *cluster.hooks.produce.lock() = Some(Arc::new(|_| {
        Box::pin(async move {
            tokio::time::sleep(Duration::from_millis(2)).await;
            HookAction::PassThrough
        })
    }));

    let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
    cfg.producer_max_buffered_bytes = 2_000;
    cfg.write_timeout = GoDuration::from_secs(10);
    let (writer, _) = create_test_writer(cfg, WriterOptions::default()).await;

    let done = Arc::new(AtomicBool::new(false));
    let succ_count = Arc::new(AtomicI64::new(0));
    let fail_count = Arc::new(AtomicI64::new(0));

    let mut handles = Vec::new();
    for wid in 0..NUM_WORKERS {
        let w = Arc::clone(&writer);
        let d = Arc::clone(&done);
        let sc = Arc::clone(&succ_count);
        let fc = Arc::clone(&fail_count);
        handles.push(tokio::spawn(async move {
            let mut n = wid * 17;
            while !d.load(Ordering::Relaxed) {
                n = n.wrapping_mul(1103515245).wrapping_add(12345);
                let len = (n % 900) + 50;
                let req = cortexpb::WriteRequest {
                    timeseries: vec![mock_timeseries(&"x".repeat(len))],
                    source: cortexpb::write_request::SourceEnum::Api as i32,
                    ..Default::default()
                };
                match w
                    .write_sync(&ProduceContext::background(), TOPIC, 0, TENANT_ID, &req)
                    .await
                {
                    Ok(()) => {
                        sc.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(WriterError::FailedPartitions {
                        source: ProducerError::MaxBuffered,
                        ..
                    }) => {
                        fc.fetch_add(1, Ordering::Relaxed);
                        tokio::task::yield_now().await;
                    }
                    Err(other) => panic!("unexpected error: {other}"),
                }
            }
        }));
    }

    tokio::time::sleep(Duration::from_millis(800)).await;
    done.store(true, Ordering::Relaxed);
    for h in handles {
        h.await.unwrap();
    }

    assert!(succ_count.load(Ordering::Relaxed) > 0);
    assert!(fail_count.load(Ordering::Relaxed) > 0);
    assert_eq!(writer.client().unwrap().buffered_bytes_counter(), 0);

    writer.stop();
    cluster.close();
}

// Upstream: pkg/storage/ingest/writer_test.go::TestProduceResultsErr
#[test]
fn test_produce_results_err() {
    // Success
    {
        let results = vec![
            ProduceResult {
                record: WriterRecord {
                    partition: 0,
                    ..Default::default()
                },
                err: None,
            },
            ProduceResult {
                record: WriterRecord {
                    partition: 1,
                    ..Default::default()
                },
                err: None,
            },
        ];
        produce_results_err(&results).unwrap();
    }

    // MessageTooLarge
    {
        let results = vec![
            ProduceResult {
                record: WriterRecord {
                    partition: 0,
                    ..Default::default()
                },
                err: None,
            },
            ProduceResult {
                record: WriterRecord {
                    partition: 1,
                    ..Default::default()
                },
                err: Some(ProducerError::MessageTooLarge),
            },
        ];
        let err = produce_results_err(&results).unwrap_err();
        assert!(matches!(err, WriterError::WriteRequestDataItemTooLarge));
    }

    // First error with failed partition IDs
    {
        let results = vec![
            ProduceResult {
                record: WriterRecord {
                    partition: 0,
                    ..Default::default()
                },
                err: None,
            },
            ProduceResult {
                record: WriterRecord {
                    partition: 1,
                    ..Default::default()
                },
                err: Some(ProducerError::Other("test error".to_string())),
            },
            ProduceResult {
                record: WriterRecord {
                    partition: 2,
                    ..Default::default()
                },
                err: Some(ProducerError::Other("another error".to_string())),
            },
        ];
        let err = produce_results_err(&results).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("failed to write to partitions [1, 2]"), "{msg}");
        assert!(msg.contains("test error"), "{msg}");
    }

    // Deduplicate partition IDs
    {
        let results = vec![
            ProduceResult {
                record: WriterRecord {
                    partition: 1,
                    ..Default::default()
                },
                err: Some(ProducerError::Other("test error".to_string())),
            },
            ProduceResult {
                record: WriterRecord {
                    partition: 1,
                    ..Default::default()
                },
                err: Some(ProducerError::Other("test error".to_string())),
            },
        ];
        let err = produce_results_err(&results).unwrap_err();
        assert!(
            err.to_string().contains("failed to write to partitions [1]"),
            "{err}"
        );
    }
}

// Upstream: pkg/storage/ingest/writer_test.go::TestMarshalWriteRequestToRecords
#[test]
fn test_marshal_write_request_to_records() {
    let test_req = || cortexpb::WriteRequest {
        source: cortexpb::write_request::SourceEnum::Rule as i32,
        skip_label_validation: true,
        timeseries: vec![
            mock_timeseries("series_1"),
            mock_timeseries("series_2"),
            mock_timeseries("series_3"),
        ],
        metadata: vec![
            cortexpb::MetricMetadata {
                r#type: cortexpb::metric_metadata::MetricType::Counter as i32,
                metric_family_name: "series_1".to_string(),
                help: "This is the first test metric.".to_string(),
                unit: String::new(),
            },
            cortexpb::MetricMetadata {
                r#type: cortexpb::metric_metadata::MetricType::Counter as i32,
                metric_family_name: "series_2".to_string(),
                help: "This is the second test metric.".to_string(),
                unit: String::new(),
            },
            cortexpb::MetricMetadata {
                r#type: cortexpb::metric_metadata::MetricType::Counter as i32,
                metric_family_name: "series_3".to_string(),
                help: "This is the third test metric.".to_string(),
                unit: String::new(),
            },
        ],
        ..Default::default()
    };

    let test_req_v2 = || {
        from_write_request_to_rw2(&test_req(), V2_RECORD_SYMBOL_OFFSET, Some(v2_common_symbols()))
            .unwrap()
    };

    // 1 record RW1 < limit
    {
        let req = test_req();
        let records = marshal_write_request_to_records(
            "test",
            1,
            "user-1",
            &req,
            req.encoded_len(),
            req.encoded_len() * 2,
            split_write_request_by_max_marshal_size,
        )
        .unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].topic, "test");
        assert_eq!(records[0].partition, 1);
        let actual = cortexpb::WriteRequest::decode(records[0].value.as_ref()).unwrap();
        assert_eq!(actual, req);
    }

    // 1 record RW2 < limit
    {
        let req = test_req_v2();
        let records = marshal_write_request_to_records(
            "test",
            1,
            "user-1",
            &req,
            req.encoded_len(),
            req.encoded_len() * 2,
            |r, sz, max| {
                split_write_request_by_max_marshal_size_rw2(
                    r,
                    sz,
                    max,
                    V2_RECORD_SYMBOL_OFFSET,
                    v2_common_symbols(),
                )
            },
        )
        .unwrap();
        assert_eq!(records.len(), 1);
        let mut scratch = WriteRequestScratch::new();
        let actual = deserialize_record_content(&records[0].value, 2, &mut scratch)
            .unwrap()
            .to_owned_write_request();
        assert_eq!(actual.source, cortexpb::write_request::SourceEnum::Rule as i32);
        assert!(actual.skip_label_validation);
        assert_eq!(actual.timeseries, test_req().timeseries);
        assert_eq!(actual.metadata, test_req().metadata);
    }

    // multiple records RW1 > limit
    {
        const LIMIT: usize = 100;
        let req = test_req();
        let records = marshal_write_request_to_records(
            "test",
            1,
            "user-1",
            &req,
            req.encoded_len(),
            LIMIT,
            split_write_request_by_max_marshal_size,
        )
        .unwrap();
        assert_eq!(records.len(), 4);
        let mut partials = Vec::new();
        for rec in &records {
            assert_eq!(rec.topic, "test");
            assert_eq!(rec.partition, 1);
            assert_eq!(rec.key.as_ref(), b"user-1");
            assert!(rec.value.len() < LIMIT);
            partials.push(cortexpb::WriteRequest::decode(rec.value.as_ref()).unwrap());
        }
        assert_eq!(
            partials,
            vec![
                cortexpb::WriteRequest {
                    source: cortexpb::write_request::SourceEnum::Rule as i32,
                    skip_label_validation: true,
                    timeseries: vec![req.timeseries[0].clone(), req.timeseries[1].clone()],
                    ..Default::default()
                },
                cortexpb::WriteRequest {
                    source: cortexpb::write_request::SourceEnum::Rule as i32,
                    skip_label_validation: true,
                    timeseries: vec![req.timeseries[2].clone()],
                    ..Default::default()
                },
                cortexpb::WriteRequest {
                    source: cortexpb::write_request::SourceEnum::Rule as i32,
                    skip_label_validation: true,
                    metadata: vec![req.metadata[0].clone(), req.metadata[1].clone()],
                    ..Default::default()
                },
                cortexpb::WriteRequest {
                    source: cortexpb::write_request::SourceEnum::Rule as i32,
                    skip_label_validation: true,
                    metadata: vec![req.metadata[2].clone()],
                    ..Default::default()
                },
            ]
        );
    }

    // multiple records RW2 > limit
    {
        const LIMIT: usize = 100;
        let req_v2 = test_req_v2();
        let records = marshal_write_request_to_records(
            "test",
            1,
            "user-1",
            &req_v2,
            req_v2.encoded_len(),
            LIMIT,
            |r, sz, max| {
                split_write_request_by_max_marshal_size_rw2(
                    r,
                    sz,
                    max,
                    V2_RECORD_SYMBOL_OFFSET,
                    v2_common_symbols(),
                )
            },
        )
        .unwrap();
        assert_eq!(records.len(), 3);
        let mut scratch = WriteRequestScratch::new();
        let orig = test_req();
        for (i, rec) in records.iter().enumerate() {
            assert!(rec.value.len() <= LIMIT);
            let actual = deserialize_record_content(&rec.value, 2, &mut scratch)
                .unwrap()
                .to_owned_write_request();
            assert_eq!(actual.timeseries, vec![orig.timeseries[i].clone()]);
            assert_eq!(actual.metadata, vec![orig.metadata[i].clone()]);
        }
    }

    // multiple records larger than limit when limit = 1
    {
        const LIMIT: usize = 1;
        let req = test_req();
        let records = marshal_write_request_to_records(
            "test",
            1,
            "user-1",
            &req,
            req.encoded_len(),
            LIMIT,
            split_write_request_by_max_marshal_size,
        )
        .unwrap();
        assert_eq!(records.len(), 6);
        for rec in &records {
            assert!(rec.value.len() > LIMIT);
        }
    }
}

// Upstream: pkg/storage/ingest/writer_test.go::TestWriter_KafkaClientMetricsParity
#[tokio::test]
async fn test_writer_kafka_client_metrics_parity() {
    const TOPIC: &str = "test";
    let cluster = FakeKafkaCluster::start(1, TOPIC).await;
    let cfg = create_test_kafka_config(&cluster.addr, TOPIC);
    let (writer, reg) = create_test_writer(cfg, WriterOptions::default()).await;

    let req = cortexpb::WriteRequest {
        timeseries: vec![mock_timeseries("series_1")],
        source: cortexpb::write_request::SourceEnum::Api as i32,
        ..Default::default()
    };
    writer
        .write_sync(&ProduceContext::background(), TOPIC, 0, "user-1", &req)
        .await
        .unwrap();

    for name in [
        "cortex_ingest_storage_writer_connects_total",
        "cortex_ingest_storage_writer_read_bytes_total",
        "cortex_ingest_storage_writer_write_bytes_total",
        "cortex_ingest_storage_writer_produce_records_total",
        "cortex_ingest_storage_writer_produce_batches_total",
        "cortex_ingest_storage_writer_produce_bytes_total",
        "cortex_ingest_storage_writer_produce_compressed_bytes_total",
    ] {
        assert!(
            get_counter_value(&reg, name, &[]) > 0.0,
            "expected counter {name} > 0"
        );
    }

    for name in [
        "cortex_ingest_storage_writer_kafka_read_time_seconds",
        "cortex_ingest_storage_writer_kafka_read_wait_seconds",
        "cortex_ingest_storage_writer_kafka_write_time_seconds",
        "cortex_ingest_storage_writer_kafka_write_wait_seconds",
        "cortex_ingest_storage_writer_kafka_request_duration_e2e_seconds",
    ] {
        let (cnt, _) = get_histogram_count_and_sum(&reg, name, &[])
            .unwrap_or_else(|| panic!("missing histogram {name}"));
        assert!(cnt > 0, "expected sample count > 0 for {name}");
    }

    for name in [
        "cortex_ingest_storage_writer_buffered_produce_records_total",
        "cortex_ingest_storage_writer_buffered_produce_bytes",
        "cortex_ingest_storage_writer_buffered_fetch_records_total",
        "cortex_ingest_storage_writer_buffered_fetch_bytes",
        "cortex_ingest_storage_writer_kafka_request_throttled_seconds",
    ] {
        assert!(has_metric(&reg, name), "expected metric {name} to be registered");
    }

    let _ = KAFKA_OFFSET_START;
    writer.stop();
    cluster.close();
}

// Upstream: pkg/storage/ingest/writer_test.go::TestWriter_AutoCreateTopics
#[tokio::test]
async fn test_writer_auto_create_topics() {
    // "auto-creates the configured topic when no explicit set is provided"
    {
        let cluster = FakeKafkaCluster::start(1, "test").await;
        let mut cfg = create_test_kafka_config(&cluster.addr, "auto-created");
        cfg.auto_create_topic_enabled = true;
        cfg.auto_create_topic_default_partitions = 1;

        let (writer, _) = create_test_writer(cfg.clone(), WriterOptions::default()).await;
        let client = IngestKafkaClient::from_config(&cfg, None).unwrap();
        let topics = client.list_topics().await.unwrap();
        assert!(topics.contains(&"auto-created".to_string()));

        client.close().await;
        writer.stop();
        cluster.close();
    }

    // "auto-creates the explicit set of topics, not the configured topic"
    {
        let cluster = FakeKafkaCluster::start(1, "test").await;
        let mut cfg = create_test_kafka_config(&cluster.addr, "mimir-ingest-rc-<read-compartment-id>");
        cfg.auto_create_topic_enabled = true;
        cfg.auto_create_topic_default_partitions = 1;

        let opts = WriterOptions {
            auto_create_topics: Some(vec![
                "mimir-ingest-rc-0".to_string(),
                "mimir-ingest-rc-1".to_string(),
            ]),
            ..Default::default()
        };
        let (writer, _) = create_test_writer(cfg.clone(), opts).await;
        let client = IngestKafkaClient::from_config(&cfg, None).unwrap();
        let topics = client.list_topics().await.unwrap();
        assert!(topics.contains(&"mimir-ingest-rc-0".to_string()));
        assert!(topics.contains(&"mimir-ingest-rc-1".to_string()));
        assert!(!topics.contains(&"mimir-ingest-rc-<read-compartment-id>".to_string()));

        client.close().await;
        writer.stop();
        cluster.close();
    }
}
