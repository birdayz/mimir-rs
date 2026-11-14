//! Wave 2 Storage Conformance Tests:
//! - Live Kafka wire-protocol conformance against Go `franz-go/pkg/kfake` + Go `pkg/storage/ingest`
//! - Rust `KafkaClient` / `PartitionOffsetClient` / `SingleClusterPartitionReader` / `IngestWriter`
//! - `BucketClient` (filesystem & in-memory) + `Shipper::sync` atomicity and `mimir.shipper.json`

use std::sync::Arc;
use std::time::Duration;

use mimir_common::block_on_for_tests;
use mimir_config::{
    BucketConfig, GoDuration, KafkaConfig, OrderedConsumptionConfig, StringSliceCSV,
};
use mimir_conformance::oracle::{
    ExemplarJson, HistogramJson, KfakeBrokerRequest, KfakeRecordInput, LabelJson, MetadataJson,
    SampleJson, SpanJson, SpawnedKfakeBroker, TimeSeriesJson, WriteRequestJson,
};
use mimir_proto::bytes::Bytes;
use mimir_proto::cortexpb;
use mimir_storage::ingest::{
    read_offset_catalogue_from_file, DecodedKafkaRecord, IngestWriter, KafkaClient,
    KafkaCompression, MultiClusterOffsetsReader, MultiClusterPartitionReader, OffsetCatalogue,
    OffsetCatalogueMetrics, OffsetFile, PartitionOffsetClient, PartitionOffsets, RecordVersion,
    SingleClusterPartitionReader, SingleClusterTopicOffsetsReader, ZeroCopyBatchConsumer,
    ZeroCopyRecordHandler, MAX_PRODUCER_RECORD_DATA_BYTES_LIMIT,
};
use mimir_storage::{
    tenant_deletion_mark_exists, write_tenant_deletion_mark, BlockMeta, BlockMetaCompaction,
    BlockStats, BucketClient, Shipper, ShipperMeta, ShipperMetrics, SourceType, TenantDeletionMark,
    ThanosMeta, CHUNKS_DIRNAME, INDEX_FILENAME, META_FILENAME, OUT_OF_ORDER_EXTERNAL_LABEL,
    OUT_OF_ORDER_EXTERNAL_LABEL_VALUE, SHIPPER_META_FILENAME, SHIPPER_META_VERSION_1,
};
use mimir_storage::ulid::Ulid;

fn lbl(name: &str, value: &str) -> LabelJson {
    LabelJson {
        name: name.to_string(),
        value: value.to_string(),
    }
}

fn rich_write_request_json(metric: &str, tenant_tag: &str, ts_base: i64, val: f64) -> WriteRequestJson {
    WriteRequestJson {
        mode: String::new(),
        source: "API".to_string(),
        skip_label_validation: false,
        skip_label_count_validation: false,
        timeseries: vec![
            TimeSeriesJson {
                labels: vec![
                    lbl("__name__", metric),
                    lbl("cluster", "prod-us"),
                    lbl("job", tenant_tag),
                    lbl("le", "0.5"),
                ],
                samples: vec![SampleJson {
                    timestamp_ms: ts_base,
                    value: val,
                }],
                exemplars: vec![ExemplarJson {
                    labels: vec![lbl("trace_id", "0123456789abcdef")],
                    value: val + 0.25,
                    timestamp_ms: ts_base + 10,
                }],
                histograms: vec![HistogramJson {
                    timestamp_ms: ts_base + 20,
                    schema: 1,
                    zero_threshold: 0.001,
                    zero_count: 4,
                    count: 10,
                    sum: 12.5,
                    reset_hint: 1,
                    is_float: false,
                    positive_spans: vec![SpanJson {
                        offset: 0,
                        length: 2,
                    }],
                    positive_deltas: vec![3, -1],
                    negative_spans: vec![SpanJson {
                        offset: -1,
                        length: 1,
                    }],
                    negative_deltas: vec![1],
                    ..Default::default()
                }],
                created_timestamp: ts_base - 1000,
            },
            TimeSeriesJson {
                labels: vec![
                    lbl("__name__", &format!("{metric}_float_hist")),
                    lbl("job", tenant_tag),
                ],
                histograms: vec![HistogramJson {
                    timestamp_ms: ts_base + 30,
                    schema: 0,
                    zero_threshold: 0.01,
                    zero_count_float: 2.5,
                    count_float: 7.5,
                    sum: 9.25,
                    reset_hint: 2,
                    is_float: true,
                    positive_spans: vec![SpanJson {
                        offset: 1,
                        length: 2,
                    }],
                    positive_counts: vec![2.5, 2.5],
                    ..Default::default()
                }],
                ..Default::default()
            },
        ],
        metadata: vec![MetadataJson {
            metric_type: "COUNTER".to_string(),
            metric_family_name: metric.to_string(),
            help: format!("help for {metric}"),
            unit: "seconds".to_string(),
        }],
    }
}

#[derive(Debug, Clone)]
struct CapturedRecord {
    offset: i64,
    tenant: String,
    version: i32,
    write_request: cortexpb::WriteRequest,
}

struct CapturingHandler {
    records: Arc<std::sync::Mutex<Vec<CapturedRecord>>>,
}

impl ZeroCopyRecordHandler for CapturingHandler {
    fn on_record(&self, record: DecodedKafkaRecord<'_, '_>) -> Result<(), String> {
        let owned = record.write_request.to_owned_write_request();
        self.records.lock().unwrap().push(CapturedRecord {
            offset: record.offset,
            tenant: record.tenant.to_owned(),
            version: record.version,
            write_request: owned,
        });
        Ok(())
    }
}

fn make_kafka_cfg(broker_addr: &str, topic: &str) -> KafkaConfig {
    KafkaConfig {
        address: StringSliceCSV(vec![broker_addr.to_string()]),
        topic: topic.to_string(),
        client_id: "mimir-rs-conformance".to_string(),
        auto_create_topic_enabled: false,
        consume_from_position_at_startup: "start".to_string(),
        last_produced_offset_poll_interval: GoDuration::from_millis(50),
        consumer_group_offset_commit_interval: GoDuration::from_millis(50),
        fetch_max_wait: GoDuration::from_millis(100),
        ..Default::default()
    }
}

#[test]
fn test_kfake_live_kafka_v1_v2_all_compressions_and_partition_reader() {
    let p0_records = vec![
        KfakeRecordInput {
            partition: 0,
            tenant: "tenant-a".to_string(),
            version: 1,
            compression: Some("none".to_string()),
            write_request: rich_write_request_json("http_requests_total", "api-none", 1_700_000_000_000, 1.0),
        },
        KfakeRecordInput {
            partition: 0,
            tenant: "tenant-b".to_string(),
            version: 2,
            compression: Some("gzip".to_string()),
            write_request: rich_write_request_json("http_requests_total", "api-gzip", 1_700_000_001_000, 2.0),
        },
        KfakeRecordInput {
            partition: 0,
            tenant: "tenant-a".to_string(),
            version: 2,
            compression: Some("snappy".to_string()),
            write_request: rich_write_request_json("cortex_request_duration_seconds_bucket", "api-snappy", 1_700_000_002_000, 3.0),
        },
        KfakeRecordInput {
            partition: 0,
            tenant: "tenant-b".to_string(),
            version: 1,
            compression: Some("lz4".to_string()),
            write_request: rich_write_request_json("up", "api-lz4", 1_700_000_003_000, 4.0),
        },
        KfakeRecordInput {
            partition: 0,
            tenant: "tenant-a".to_string(),
            version: 2,
            compression: Some("zstd".to_string()),
            write_request: rich_write_request_json("storage_operation_duration_seconds_bucket", "api-zstd", 1_700_000_004_000, 5.0),
        },
    ];
    let p1_records = vec![KfakeRecordInput {
        partition: 1,
        tenant: "tenant-b".to_string(),
        version: 2,
        compression: Some("zstd".to_string()),
        write_request: rich_write_request_json("p1_metric", "p1-job", 1_700_000_005_000, 99.5),
    }];

    let mut all_inputs = p0_records.clone();
    all_inputs.extend(p1_records.clone());

    let broker = SpawnedKfakeBroker::spawn(&KfakeBrokerRequest {
        topic: "ingest-conf".to_string(),
        partitions: 2,
        compression: None,
        records: all_inputs,
    })
    .expect("spawn kfake broker");

    let broker_addr = broker.broker_addr().to_string();
    let topic = broker.topic().to_string();
    let expected_offsets = broker.offsets().to_vec();
    assert_eq!(expected_offsets.len(), 2);
    assert_eq!(expected_offsets[0].start_offset, 0);
    assert_eq!(expected_offsets[0].end_offset, 5);
    assert_eq!(expected_offsets[1].start_offset, 0);
    assert_eq!(expected_offsets[1].end_offset, 1);

    block_on_for_tests(async move {
        let cfg = make_kafka_cfg(&broker_addr, &topic);
        let client = Arc::new(KafkaClient::from_config(&cfg).unwrap());
        let offset_client = PartitionOffsetClient::new(Arc::clone(&client), None);

        // b) Verify ListOffsets (-2 start, -1 end - 1 = last produced) & partition IDs match kfake 100%!
        let pids = offset_client.list_topic_partition_ids(&topic).await.unwrap();
        assert_eq!(pids, vec![0, 1]);

        for exp in &expected_offsets {
            let start = offset_client
                .fetch_partition_start_offset(&topic, exp.partition)
                .await
                .unwrap();
            let last_prod = offset_client
                .fetch_partition_last_produced_offset(&topic, exp.partition)
                .await
                .unwrap();
            assert_eq!(start, exp.start_offset);
            assert_eq!(last_prod, exp.end_offset - 1);
        }

        // c) Run Rust SingleClusterPartitionReader for partition 0 & partition 1, wait_for_offset,
        //    and assert every decoded tenant, series, sample, histogram, exemplar, and metadata!
        let tmp = tempfile::tempdir().unwrap();
        let off_path_0 = tmp.path().join("partition-0.offset");
        let captured_0 = Arc::new(std::sync::Mutex::new(Vec::new()));
        let consumer_0 = Arc::new(ZeroCopyBatchConsumer::new(CapturingHandler {
            records: Arc::clone(&captured_0),
        }));
        let reader_0 = SingleClusterPartitionReader::new(
            cfg.clone(),
            0,
            "ingester-0",
            Some(off_path_0.clone()),
            consumer_0,
            None,
        )
        .unwrap();

        reader_0.start().await.unwrap();
        reader_0
            .wait_for_offset(expected_offsets[0].end_offset - 1, Duration::from_secs(10))
            .await
            .unwrap();
        reader_0
            .wait_read_consistency_until_last_produced_offset(Duration::from_secs(10))
            .await
            .unwrap();
        // Immediate wait on already-consumed offset must return immediately.
        reader_0
            .wait_for_offset(expected_offsets[0].end_offset - 1, Duration::from_millis(1))
            .await
            .unwrap();
        reader_0.stop().await;

        assert_eq!(reader_0.last_consumed_offset(), 4);
        assert_eq!(OffsetFile::new(&off_path_0, 0).read(), Some(4));

        let recs_0 = captured_0.lock().unwrap().clone();
        assert_eq!(recs_0.len(), p0_records.len());
        for (idx, (got, exp)) in recs_0.iter().zip(p0_records.iter()).enumerate() {
            assert_eq!(got.offset, idx as i64);
            assert_eq!(got.tenant, exp.tenant);
            assert_eq!(got.version, exp.version);
            assert_eq!(got.write_request.timeseries.len(), 2);
            assert_eq!(got.write_request.metadata.len(), 1);

            let ts0 = &got.write_request.timeseries[0];
            let exp_ts0 = &exp.write_request.timeseries[0];
            let got_labels: Vec<(&str, &str)> = ts0
                .labels
                .iter()
                .map(|l| (std::str::from_utf8(&l.name).unwrap(), std::str::from_utf8(&l.value).unwrap()))
                .collect();
            let exp_labels: Vec<(&str, &str)> = exp_ts0
                .labels
                .iter()
                .map(|l| (l.name.as_str(), l.value.as_str()))
                .collect();
            assert_eq!(got_labels, exp_labels);

            assert_eq!(ts0.samples.len(), 1);
            assert_eq!(ts0.samples[0].value, exp_ts0.samples[0].value);
            assert_eq!(ts0.samples[0].timestamp_ms, exp_ts0.samples[0].timestamp_ms);
            assert_eq!(ts0.created_timestamp, exp_ts0.created_timestamp);

            assert_eq!(ts0.exemplars.len(), 1);
            assert_eq!(ts0.exemplars[0].value, exp_ts0.exemplars[0].value);
            assert_eq!(ts0.exemplars[0].timestamp_ms, exp_ts0.exemplars[0].timestamp_ms);

            assert_eq!(ts0.histograms.len(), 1);
            assert_eq!(ts0.histograms[0].sum, exp_ts0.histograms[0].sum);
            assert_eq!(ts0.histograms[0].schema, exp_ts0.histograms[0].schema);
            assert_eq!(
                ts0.histograms[0].count,
                Some(cortexpb::histogram::Count::CountInt(10))
            );
            assert_eq!(
                ts0.histograms[0].zero_count,
                Some(cortexpb::histogram::ZeroCount::ZeroCountInt(4))
            );

            let ts1 = &got.write_request.timeseries[1];
            assert_eq!(ts1.histograms.len(), 1);
            assert_eq!(
                ts1.histograms[0].count,
                Some(cortexpb::histogram::Count::CountFloat(7.5))
            );
            assert_eq!(
                ts1.histograms[0].zero_count,
                Some(cortexpb::histogram::ZeroCount::ZeroCountFloat(2.5))
            );

            let m = &got.write_request.metadata[0];
            assert_eq!(m.metric_family_name, exp.write_request.metadata[0].metric_family_name);
            assert_eq!(m.help, exp.write_request.metadata[0].help);
            assert_eq!(m.unit, exp.write_request.metadata[0].unit);
            assert_eq!(m.r#type, cortexpb::metric_metadata::MetricType::Counter as i32);
        }

        // Verify partition 1 reader as well.
        let captured_1 = Arc::new(std::sync::Mutex::new(Vec::new()));
        let consumer_1 = Arc::new(ZeroCopyBatchConsumer::new(CapturingHandler {
            records: Arc::clone(&captured_1),
        }));
        let reader_1 = SingleClusterPartitionReader::new(
            cfg,
            1,
            "ingester-1",
            Some(tmp.path().join("partition-1.offset")),
            consumer_1,
            None,
        )
        .unwrap();
        reader_1.start().await.unwrap();
        reader_1
            .wait_for_offset(0, Duration::from_secs(10))
            .await
            .unwrap();
        reader_1.stop().await;
        let recs_1 = captured_1.lock().unwrap();
        assert_eq!(recs_1.len(), 1);
        assert_eq!(recs_1[0].tenant, "tenant-b");
        assert_eq!(recs_1[0].write_request.timeseries[0].samples[0].value, 99.5);
    });
}

#[test]
fn test_kfake_rust_producer_v2_roundtrip_all_compressions() {
    let broker = SpawnedKfakeBroker::spawn(&KfakeBrokerRequest {
        topic: "rust-produce-test".to_string(),
        partitions: 1,
        compression: None,
        records: Vec::new(),
    })
    .expect("spawn empty kfake broker");

    let broker_addr = broker.broker_addr().to_string();
    let topic = broker.topic().to_string();

    block_on_for_tests(async move {
        let cfg = make_kafka_cfg(&broker_addr, &topic);
        let client = Arc::new(KafkaClient::from_config(&cfg).unwrap());
        let offset_client = PartitionOffsetClient::new(Arc::clone(&client), None);

        // Empty partition: start_offset == 0, last_produced_offset == -1.
        assert_eq!(
            offset_client
                .fetch_partition_start_offset(&topic, 0)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            offset_client
                .fetch_partition_last_produced_offset(&topic, 0)
                .await
                .unwrap(),
            -1
        );

        let codecs = [
            KafkaCompression::None,
            KafkaCompression::Gzip,
            KafkaCompression::Snappy,
            KafkaCompression::Lz4,
            KafkaCompression::Zstd,
        ];

        let mut expected_reqs = Vec::new();
        for (i, codec) in codecs.iter().enumerate() {
            let writer = IngestWriter::with_client(
                Arc::clone(&client),
                RecordVersion::V2,
                *codec,
                MAX_PRODUCER_RECORD_DATA_BYTES_LIMIT,
                None,
            );
            let req = cortexpb::WriteRequest {
                timeseries: vec![cortexpb::TimeSeries {
                    labels: vec![
                        cortexpb::LabelPair::from_static("__name__", "cortex_request_duration_seconds_bucket"),
                        cortexpb::LabelPair::from_static("job", "rust-producer"),
                        cortexpb::LabelPair {
                            name: Bytes::from_static(b"codec"),
                            value: Bytes::from(codec.as_str().as_bytes().to_vec()),
                        },
                    ],
                    samples: vec![cortexpb::Sample {
                        value: (i as f64) * 10.5 + 1.0,
                        timestamp_ms: 1_700_001_000_000 + (i as i64) * 1000,
                    }],
                    exemplars: vec![cortexpb::Exemplar {
                        labels: vec![cortexpb::LabelPair::from_static("trace_id", "rust-trace")],
                        value: 0.42,
                        timestamp_ms: 1_700_001_000_050 + (i as i64) * 1000,
                    }],
                    histograms: vec![cortexpb::Histogram {
                        sum: 5.5,
                        schema: 1,
                        zero_threshold: 0.001,
                        positive_spans: vec![cortexpb::BucketSpan {
                            offset: 0,
                            length: 1,
                        }],
                        positive_deltas: vec![2],
                        reset_hint: cortexpb::histogram::ResetHint::Yes as i32,
                        timestamp: 1_700_001_000_100 + (i as i64) * 1000,
                        count: Some(cortexpb::histogram::Count::CountInt(3)),
                        zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountInt(1)),
                        ..Default::default()
                    }],
                    created_timestamp: 1_700_000_999_000,
                }],
                metadata: vec![cortexpb::MetricMetadata {
                    r#type: cortexpb::metric_metadata::MetricType::Histogram as i32,
                    metric_family_name: "cortex_request_duration_seconds_bucket".to_string(),
                    help: "request latency".to_string(),
                    unit: "seconds".to_string(),
                }],
                ..Default::default()
            };

            let base_offset = writer
                .write_sync(&topic, 0, "tenant-rust", &req)
                .await
                .unwrap();
            assert_eq!(base_offset, i as i64);
            assert_eq!(
                offset_client
                    .fetch_partition_last_produced_offset(&topic, 0)
                    .await
                    .unwrap(),
                i as i64
            );
            expected_reqs.push(req);
        }

        // Consume all 5 produced v2 records back via SingleClusterPartitionReader.
        let captured = Arc::new(std::sync::Mutex::new(Vec::new()));
        let consumer = Arc::new(ZeroCopyBatchConsumer::new(CapturingHandler {
            records: Arc::clone(&captured),
        }));
        let reader = SingleClusterPartitionReader::with_client(
            cfg,
            0,
            "rust-consumer",
            None,
            client,
            consumer,
            None,
        )
        .unwrap();
        reader.start().await.unwrap();
        reader
            .wait_for_offset((codecs.len() - 1) as i64, Duration::from_secs(10))
            .await
            .unwrap();
        reader.stop().await;

        let recs = captured.lock().unwrap();
        assert_eq!(recs.len(), expected_reqs.len());
        for (i, (got, exp)) in recs.iter().zip(expected_reqs.iter()).enumerate() {
            assert_eq!(got.offset, i as i64);
            assert_eq!(got.tenant, "tenant-rust");
            assert_eq!(got.version, 2);
            assert_eq!(&got.write_request, exp);
        }
    });
}

#[test]
fn test_bucket_client_and_shipper_sync_atomicity() {
    block_on_for_tests(async {
        for bkt in [
            BucketClient::new_in_memory("shipper-mem"),
            BucketClient::new_filesystem(tempfile::tempdir().unwrap().path()).unwrap(),
        ] {
            let blocks_dir = tempfile::tempdir().unwrap();

            let id1 = Ulid::from_parts(1_700_000_000_000, 1);
            let mut m1 = BlockMeta {
                ulid: id1,
                min_time: 1000,
                max_time: 2000,
                stats: BlockStats {
                    num_samples: 250,
                    ..Default::default()
                },
                compaction: BlockMetaCompaction {
                    level: 1,
                    ..Default::default()
                },
                version: 1,
                out_of_order: false,
                thanos: ThanosMeta::default(),
            };
            m1.compaction.set_out_of_order();

            let b1_dir = blocks_dir.path().join(id1.to_string());
            std::fs::create_dir_all(b1_dir.join(CHUNKS_DIRNAME)).unwrap();
            m1.write_to_dir(&b1_dir).unwrap();
            std::fs::write(b1_dir.join(INDEX_FILENAME), b"idx-payload").unwrap();
            std::fs::write(b1_dir.join(CHUNKS_DIRNAME).join("000001"), b"chunk-payload").unwrap();

            let shipper = Shipper::new_for_user(
                "tenant-ship",
                blocks_dir.path(),
                &bkt,
                SourceType::Receive,
                true,
                ShipperMetrics::new(None),
            );

            let uploaded = shipper.sync().await.unwrap();
            assert_eq!(uploaded, 1);

            // Second sync is idempotent and uploads 0 blocks.
            assert_eq!(shipper.sync().await.unwrap(), 0);

            // Verify mimir.shipper.json persisted with version=1 and id1.
            let shipper_json: ShipperMeta = serde_json::from_slice(
                &std::fs::read(blocks_dir.path().join(SHIPPER_META_FILENAME)).unwrap(),
            )
            .unwrap();
            assert_eq!(shipper_json.version, SHIPPER_META_VERSION_1);
            assert!(shipper_json.shipped.contains_key(&id1));

            // Verify remote block files and Thanos metadata injection.
            let remote_meta_key = format!("tenant-ship/{id1}/{META_FILENAME}");
            assert!(bkt.exists(&remote_meta_key).await.unwrap());
            assert_eq!(
                bkt.get_range(&format!("tenant-ship/{id1}/{INDEX_FILENAME}"), 4, 7)
                    .await
                    .unwrap()
                    .as_ref(),
                b"payload"
            );

            let remote_meta: BlockMeta =
                serde_json::from_slice(&bkt.download(&remote_meta_key).await.unwrap()).unwrap();
            assert_eq!(remote_meta.thanos.source, SourceType::Receive);
            assert_eq!(remote_meta.thanos.downsample.resolution, 0);
            assert_eq!(
                remote_meta.thanos.labels.get(OUT_OF_ORDER_EXTERNAL_LABEL),
                Some(&OUT_OF_ORDER_EXTERNAL_LABEL_VALUE.to_string())
            );
            assert_eq!(remote_meta.thanos.segment_files, vec!["000001".to_string()]);

            // Verify local meta.json was NOT mutated.
            let local_meta = BlockMeta::read_from_dir(&b1_dir).unwrap();
            assert!(local_meta.thanos.labels.is_empty());

            // Verify TenantDeletionMark helpers.
            assert!(!tenant_deletion_mark_exists(&bkt, "tenant-ship").await.unwrap());
            write_tenant_deletion_mark(&bkt, "tenant-ship", &TenantDeletionMark::now())
                .await
                .unwrap();
            assert!(tenant_deletion_mark_exists(&bkt, "tenant-ship").await.unwrap());
        }

        // Verify BucketClient::from_config for filesystem + storage_prefix.
        let fs_root = tempfile::tempdir().unwrap();
        let mut bcfg = BucketConfig::default();
        bcfg.backend_cfg.backend = "filesystem".to_string();
        bcfg.backend_cfg.filesystem.dir = fs_root.path().to_string_lossy().into_owned();
        bcfg.storage_prefix = "mimirdata".to_string();
        let cfg_bkt = BucketClient::from_config(&bcfg).unwrap();
        cfg_bkt
            .upload("t1/test.txt", Bytes::from_static(b"ok"))
            .await
            .unwrap();
        assert!(fs_root.path().join("mimirdata/t1/test.txt").exists());
    });
}

#[test]
fn test_kfake_multi_cluster_offset_readers_heap_merger_and_offset_catalogue() {
    let broker0 = SpawnedKfakeBroker::spawn(&KfakeBrokerRequest {
        topic: "ingest-mc".to_string(),
        partitions: 1,
        compression: Some("snappy".to_string()),
        records: vec![
            KfakeRecordInput {
                partition: 0,
                tenant: "tenant-a".to_string(),
                version: 2,
                compression: None,
                write_request: rich_write_request_json("mc_metric", "c0-r0", 1000, 1.0),
            },
            KfakeRecordInput {
                partition: 0,
                tenant: "tenant-a".to_string(),
                version: 2,
                compression: None,
                write_request: rich_write_request_json("mc_metric", "c0-r1", 2000, 2.0),
            },
        ],
    })
    .expect("spawn kfake cluster 0");

    let broker1 = SpawnedKfakeBroker::spawn(&KfakeBrokerRequest {
        topic: "ingest-mc".to_string(),
        partitions: 1,
        compression: Some("zstd".to_string()),
        records: vec![KfakeRecordInput {
            partition: 0,
            tenant: "tenant-b".to_string(),
            version: 2,
            compression: None,
            write_request: rich_write_request_json("mc_metric", "c1-r0", 3000, 3.0),
        }],
    })
    .expect("spawn kfake cluster 1");

    let cfg0 = make_kafka_cfg(broker0.broker_addr(), broker0.topic());
    let cfg1 = make_kafka_cfg(broker1.broker_addr(), broker1.topic());

    block_on_for_tests(async move {
        let c0 = Arc::new(KafkaClient::from_config(&cfg0).unwrap());
        let c1 = Arc::new(KafkaClient::from_config(&cfg1).unwrap());
        let oc0 = PartitionOffsetClient::new(c0, None);
        let oc1 = PartitionOffsetClient::new(c1, None);

        // SingleClusterTopicOffsetsReader
        let sc_reader = SingleClusterTopicOffsetsReader::new(
            oc0.clone(),
            "ingest-mc",
            Duration::from_millis(25),
        );
        sc_reader.start().await;
        let sc_offsets = sc_reader.wait_next_fetch_last_produced_offset().await.unwrap();
        assert_eq!(sc_offsets.get(&0), Some(&1));
        sc_reader.stop().await;

        // MultiClusterOffsetsReader
        let mc_offsets_reader = MultiClusterOffsetsReader::new(
            vec![oc0, oc1],
            vec!["ingest-mc".to_string()],
            Duration::from_millis(25),
        )
        .unwrap();
        mc_offsets_reader.start().await;
        let mc_offsets = mc_offsets_reader
            .wait_next_fetch_last_produced_offset()
            .await
            .unwrap();
        let part0 = &mc_offsets["ingest-mc"][&0];
        assert_eq!(part0.num_kafka_clusters(), 2);
        assert_eq!(part0.for_kafka_cluster(0), 1);
        assert_eq!(part0.for_kafka_cluster(1), 0);
        mc_offsets_reader.stop().await;

        // MultiClusterPartitionReader with HeapMerger enabled
        let captured = Arc::new(std::sync::Mutex::new(Vec::new()));
        let consumer = Arc::new(ZeroCopyBatchConsumer::new(CapturingHandler {
            records: Arc::clone(&captured),
        }));
        let tmp = tempfile::tempdir().unwrap();
        let ordered_cfg = OrderedConsumptionConfig {
            enabled: true,
            max_batch_records: 16,
            max_batch_wait: GoDuration::from_millis(30),
            ..Default::default()
        };

        let mc_reader = MultiClusterPartitionReader::new(
            &[cfg0, cfg1],
            &ordered_cfg,
            0,
            "ingester-mc",
            vec![
                Some(tmp.path().join("wc-0.offset")),
                Some(tmp.path().join("wc-1.offset")),
            ],
            consumer,
            None,
        )
        .unwrap();
        mc_reader.start().await.unwrap();
        mc_reader
            .wait_read_consistency_until_offsets(
                &PartitionOffsets::new_multi_cluster(vec![1, 0]),
                Duration::from_secs(10),
            )
            .await
            .unwrap();
        mc_reader
            .wait_read_consistency_until_last_produced_offset(Duration::from_secs(10))
            .await
            .unwrap();

        let hws = mc_reader.last_seen_offsets();
        assert_eq!(hws.for_kafka_cluster(0), 1);
        assert_eq!(hws.for_kafka_cluster(1), 0);
        mc_reader.stop().await;

        assert_eq!(captured.lock().unwrap().len(), 3);

        // OffsetCatalogue sync using the multi-cluster high watermarks
        let tsdb_dir = tempfile::tempdir().unwrap();
        let block_id = Ulid::from_parts(1_700_000_000_000, 7);
        std::fs::create_dir_all(tsdb_dir.path().join(block_id.to_string())).unwrap();

        let catalogue = OffsetCatalogue::new(
            tsdb_dir.path(),
            "tenant-a",
            0,
            OffsetCatalogueMetrics::new(None),
        );
        catalogue.sync(&hws).unwrap();
        let cat_data = read_offset_catalogue_from_file(tsdb_dir.path()).unwrap();
        assert_eq!(cat_data.version, 2);
        assert_eq!(cat_data.data[&block_id.to_string()][&0].offset, 1);
        assert_eq!(cat_data.data[&block_id.to_string()][&1].offset, 0);
    });
}
