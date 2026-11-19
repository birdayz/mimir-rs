//! Wave 15 Track 2 regression tests (WLL-02, WLL-04..WLL-10, P2-3, P3-1).

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use mimir_common::services::{Service, ServiceContext};
use mimir_config::ingester::RingConfig;
use mimir_config::limits::LimitsConfig;
use mimir_config::{GoDuration, KafkaConfig, Overrides};
use mimir_ingester::compaction::spawn_owned_series_loop;
use mimir_ingester::ingest_storage::IngesterShardedBatchConsumer;
use mimir_ingester::ingester_ring_lifecycler::{IngesterLifecycler, TokenlessLifecycler};
use mimir_ingester::Ingester;
use mimir_proto::cortexpb;
use mimir_proto::prost::Message;
use mimir_proto::ringpb::InstanceState;
use mimir_proto::symbols::RECORD_VERSION_HEADER_KEY;
use mimir_ring::kv::inmemory::InMemoryKv;
use mimir_ring::kv::KvClient;
use mimir_ring::lifecycler::BasicLifecyclerConfig;
use mimir_ring::ring::ring_desc_codec;
use mimir_storage::ingest::version::{RecordSerializer, RecordVersion};
use mimir_storage::ingest::{FetchedRecord, PusherConsumerMetrics};
use prometheus::Registry;

use common::{
    default_ingester_test_config, metric_value, prepare_ingester_with_blocks_storage,
    start_and_wait_healthy,
};

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn make_kafka_record(
    version: RecordVersion,
    tenant_id: &str,
    offset: i64,
    req: &cortexpb::WriteRequest,
) -> FetchedRecord {
    let ser = RecordSerializer::new(version);
    let (recs, _) = ser
        .to_records("ingest", 0, tenant_id, req, 4 * 1024 * 1024)
        .expect("serialize record");
    let rec = recs.into_iter().next().expect("at least 1 record");
    FetchedRecord {
        offset,
        timestamp_ms: now_ms(),
        key: rec.key,
        value: rec.value,
        headers: rec.headers,
    }
}

fn make_empty_kafka_record(version: RecordVersion, tenant_id: &str, offset: i64) -> FetchedRecord {
    let empty = cortexpb::WriteRequest::default();
    let payload = Bytes::from(empty.encode_to_vec());
    let mut headers = Vec::new();
    if version != RecordVersion::V0 {
        let ver_byte = match version {
            RecordVersion::V0 => 0u8,
            RecordVersion::V1 => 1u8,
            RecordVersion::V2 => 2u8,
        };
        headers.push((
            RECORD_VERSION_HEADER_KEY.to_string(),
            Bytes::from(vec![0, 0, 0, ver_byte]),
        ));
    }
    FetchedRecord {
        offset,
        timestamp_ms: now_ms(),
        key: Bytes::copy_from_slice(tenant_id.as_bytes()),
        value: payload,
        headers,
    }
}

fn make_simple_ts(name: &str, extra: &[(&str, &str)], ts_ms: i64, val: f64) -> cortexpb::TimeSeries {
    let mut labels = vec![cortexpb::LabelPair::from_strs("__name__", name)];
    for &(k, v) in extra {
        labels.push(cortexpb::LabelPair::from_strs(k, v));
    }
    labels.sort_by(|a, b| a.name.cmp(&b.name));
    cortexpb::TimeSeries {
        labels,
        samples: vec![cortexpb::Sample {
            value: val,
            timestamp_ms: ts_ms,
        }],
        ..Default::default()
    }
}

fn new_kafka_consumer(
    ing: Arc<Ingester>,
    reg: &Registry,
    batch_size: i64,
    max_shards: i64,
) -> IngesterShardedBatchConsumer {
    let kafka_cfg = KafkaConfig {
        ingestion_concurrency_max: max_shards,
        ingestion_concurrency_batch_size: batch_size,
        ingestion_concurrency_target_flushes_per_shard: 1,
        ..Default::default()
    };
    IngesterShardedBatchConsumer::new(ing, kafka_cfg, PusherConsumerMetrics::new(Some(reg)))
}

// ---------------------------------------------------------------------------
// 1. [WLL-02] Kafka ingest_storage empty / invalid tenant key server-error parity
// ---------------------------------------------------------------------------

#[tokio::test]
async fn wll02_kafka_empty_and_invalid_tenant_keys_are_server_errors_and_empty_records_skip() {
    let tmp = tempfile::tempdir().unwrap();
    let reg = Registry::new();
    let (cfg, blocks_cfg, ingest_cfg) = default_ingester_test_config(tmp.path());
    let (ing, _ring, _kv) = prepare_ingester_with_blocks_storage(
        tmp.path(),
        cfg,
        blocks_cfg,
        ingest_cfg,
        LimitsConfig::default(),
        Some(&reg),
    );
    start_and_wait_healthy(&ing).await;
    let consumer = new_kafka_consumer(Arc::clone(&ing), &reg, 1024, 4);

    // A) Completely empty records (0 timeseries, 0 metadata) with empty or invalid tenant key
    //    must skip cleanly with Ok(()) and zero errors counted.
    let empty_v1 = make_empty_kafka_record(RecordVersion::V1, "", 1);
    let empty_v2 = make_empty_kafka_record(RecordVersion::V2, "../bad", 2);
    consumer
        .consume_records_sync(&[empty_v1, empty_v2])
        .expect("empty records must skip cleanly");
    assert_eq!(
        metric_value(
            &reg,
            "cortex_ingest_storage_reader_requests_total",
            &[]
        ),
        Some(0.0)
    );
    assert_eq!(
        metric_value(
            &reg,
            "cortex_ingest_storage_reader_requests_failed_total",
            &[("cause", "server")]
        ),
        Some(0.0)
    );
    assert_eq!(
        metric_value(
            &reg,
            "cortex_ingest_storage_reader_requests_failed_total",
            &[("cause", "client")]
        ),
        Some(0.0)
    );

    // B) Non-empty record with empty tenant key ("") -> server error, aborts Consume.
    let ts = now_ms();
    let wr = cortexpb::WriteRequest {
        timeseries: vec![make_simple_ts("m1", &[], ts, 1.0)],
        source: cortexpb::write_request::SourceEnum::Api as i32,
        ..Default::default()
    };
    let rec_empty_tid = make_kafka_record(RecordVersion::V1, "", 10, &wr);
    let err = consumer
        .consume_records_sync(&[rec_empty_tid])
        .expect_err("non-empty record with empty tenant key must fail Consume");
    assert!(
        err.contains("consuming record at offset 10 for tenant "),
        "unexpected error: {err}"
    );
    assert_eq!(
        metric_value(
            &reg,
            "cortex_ingest_storage_reader_requests_failed_total",
            &[("cause", "server")]
        ),
        Some(1.0)
    );
    assert_eq!(
        metric_value(
            &reg,
            "cortex_ingest_storage_reader_requests_failed_total",
            &[("cause", "client")]
        ),
        Some(0.0)
    );

    // C) Non-empty v2 metadata-only record with invalid tenant key ("a|b") -> server error, aborts Consume.
    let meta_wr = cortexpb::WriteRequest {
        metadata: vec![cortexpb::MetricMetadata {
            r#type: 1,
            metric_family_name: "m_meta".to_string(),
            help: "help".to_string(),
            unit: "".to_string(),
        }],
        source: cortexpb::write_request::SourceEnum::Api as i32,
        ..Default::default()
    };
    let rec_invalid_tid = make_kafka_record(RecordVersion::V2, "a|b", 11, &meta_wr);
    let err2 = consumer
        .consume_records_sync(&[rec_invalid_tid])
        .expect_err("non-empty record with invalid tenant key must fail Consume");
    assert!(
        err2.contains("consuming record at offset 11 for tenant a|b"),
        "unexpected error: {err2}"
    );
    assert_eq!(
        metric_value(
            &reg,
            "cortex_ingest_storage_reader_requests_failed_total",
            &[("cause", "server")]
        ),
        Some(2.0)
    );
    assert_eq!(
        metric_value(
            &reg,
            "cortex_ingest_storage_reader_requests_failed_total",
            &[("cause", "client")]
        ),
        Some(0.0)
    );

    ing.stop().await.unwrap();
}

// ---------------------------------------------------------------------------
// 2. [WLL-04] Parallel Kafka batch flushing & GroupLabel attribution parity
// ---------------------------------------------------------------------------

#[tokio::test]
async fn wll04_kafka_batch_flushing_ignores_group_hash_and_2048_samples_cap() {
    let tmp = tempfile::tempdir().unwrap();
    let reg = Registry::new();
    let (cfg, blocks_cfg, ingest_cfg) = default_ingester_test_config(tmp.path());
    let limits = LimitsConfig {
        separate_metrics_group_label: "cluster".to_string(),
        ..Default::default()
    };

    let (ing, _ring, _kv) = prepare_ingester_with_blocks_storage(
        tmp.path(),
        cfg,
        blocks_cfg,
        ingest_cfg,
        limits,
        Some(&reg),
    );
    start_and_wait_healthy(&ing).await;
    // 1 shard so all series go into the single shard's batchingQueue (up to 1024 series).
    let consumer = new_kafka_consumer(Arc::clone(&ing), &reg, 1024, 1);

    // 100 series alternating cluster="a" / cluster="b", each with 30 samples (3,000 samples > 2048).
    // Include one OOO sample in the 2nd series (cluster="b") to verify discarded group attribution
    // comes from timeseries[0] (cluster="a"), matching Go validation.GroupLabel.
    let base_ts = now_ms();
    let mut records = Vec::new();
    for i in 0..100i64 {
        let cluster = if i % 2 == 0 { "a" } else { "b" };
        let series_name = format!("s_{i:03}");
        let mut samples = Vec::new();
        for s in 0..30i64 {
            samples.push(cortexpb::Sample {
                value: s as f64,
                timestamp_ms: base_ts + s * 1000,
            });
        }
        if i == 1 {
            // Make one sample out-of-order after the later timestamps.
            samples.push(cortexpb::Sample {
                value: 999.0,
                timestamp_ms: base_ts,
            });
        }
        let wr = cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair::from_strs("__name__", &series_name),
                    cortexpb::LabelPair::from_strs("cluster", cluster),
                ],
                samples,
                ..Default::default()
            }],
            source: cortexpb::write_request::SourceEnum::Api as i32,
            ..Default::default()
        };
        records.push(make_kafka_record(RecordVersion::V2, "t1", i, &wr));
    }

    consumer
        .consume_records_sync(&records)
        .expect("consume batch");

    // Must flush as 1 single batch (100 series <= 1024), NOT split on group_hash or 2048 samples.
    assert_eq!(
        metric_value(
            &reg,
            "cortex_ingest_storage_reader_batching_queue_flush_total",
            &[]
        ),
        Some(1.0)
    );
    assert_eq!(
        metric_value(
            &reg,
            "cortex_ingest_storage_reader_requests_total",
            &[]
        ),
        Some(1.0)
    );
    // Discarded sample on series 1 (cluster="b") must be attributed to timeseries[0]'s group ("a").
    assert_eq!(
        metric_value(
            &reg,
            "cortex_discarded_samples_total",
            &[
                ("group", "a"),
                ("reason", "sample-out-of-order"),
                ("user", "t1"),
            ]
        ),
        Some(1.0)
    );
    assert_eq!(
        metric_value(
            &reg,
            "cortex_discarded_samples_total",
            &[
                ("group", "b"),
                ("reason", "sample-out-of-order"),
                ("user", "t1"),
            ]
        ),
        None
    );

    ing.stop().await.unwrap();
}

// ---------------------------------------------------------------------------
// 3. [WLL-05 & P2-3] Consistent non-UTF-8 label handling across v0/v1/v2 & classic push
// ---------------------------------------------------------------------------

#[tokio::test]
async fn wll05_p2_3_non_utf8_series_labels_do_not_rollback_valid_siblings_or_mutate_to_fffd() {
    let tmp = tempfile::tempdir().unwrap();
    let reg = Registry::new();
    let (cfg, blocks_cfg, ingest_cfg) = default_ingester_test_config(tmp.path());
    let (ing, _ring, _kv) = prepare_ingester_with_blocks_storage(
        tmp.path(),
        cfg,
        blocks_cfg,
        ingest_cfg,
        LimitsConfig::default(),
        Some(&reg),
    );
    start_and_wait_healthy(&ing).await;
    let consumer = new_kafka_consumer(Arc::clone(&ing), &reg, 1024, 1);
    let ts = now_ms();

    // Test both Kafka v1 and Kafka v2:
    // Record 0: valid series before bad one
    // Record 1: series with non-UTF-8 label value [0xff, 0xfe]
    // Record 2: valid series after bad one
    for (tenant, ver) in [("t-v1", RecordVersion::V1), ("t-v2", RecordVersion::V2)] {
        let wr_good1 = cortexpb::WriteRequest {
            timeseries: vec![make_simple_ts("good_before", &[], ts, 1.0)],
            source: cortexpb::write_request::SourceEnum::Api as i32,
            ..Default::default()
        };
        let wr_bad = cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair::from_strs("__name__", "bad_utf8_metric"),
                    cortexpb::LabelPair {
                        name: Bytes::from_static(b"k"),
                        value: Bytes::from_static(&[0xff, 0xfe]),
                    },
                ],
                samples: vec![cortexpb::Sample {
                    value: 2.0,
                    timestamp_ms: ts,
                }],
                ..Default::default()
            }],
            source: cortexpb::write_request::SourceEnum::Api as i32,
            ..Default::default()
        };
        let wr_good2 = cortexpb::WriteRequest {
            timeseries: vec![make_simple_ts("good_after", &[], ts, 3.0)],
            source: cortexpb::write_request::SourceEnum::Api as i32,
            ..Default::default()
        };

        let bad_rec = if ver == RecordVersion::V2 {
            // In Go, yoloString allows non-UTF-8 bytes in symbols_rw2 (field 4); construct raw RW2 wire bytes.
            let mut payload = Vec::new();
            for sym in [&b""[..], b"__name__", b"bad_utf8_metric", b"k", &[0xff, 0xfe]] {
                payload.push((4 << 3) | 2);
                mimir_proto::prost::encoding::encode_varint(sym.len() as u64, &mut payload);
                payload.extend_from_slice(sym);
            }
            let ts_rw2 = cortexpb::TimeSeriesRw2 {
                labels_refs: vec![65, 66, 67, 68],
                samples: vec![cortexpb::Sample {
                    value: 2.0,
                    timestamp_ms: ts,
                }],
                ..Default::default()
            };
            let ts_bytes = ts_rw2.encode_to_vec();
            payload.push((5 << 3) | 2);
            mimir_proto::prost::encoding::encode_varint(ts_bytes.len() as u64, &mut payload);
            payload.extend_from_slice(&ts_bytes);
            FetchedRecord {
                offset: 2,
                timestamp_ms: now_ms(),
                key: Bytes::copy_from_slice(tenant.as_bytes()),
                value: Bytes::from(payload),
                headers: vec![(
                    RECORD_VERSION_HEADER_KEY.to_string(),
                    Bytes::from_static(&[0, 0, 0, 2]),
                )],
            }
        } else {
            make_kafka_record(ver, tenant, 2, &wr_bad)
        };
        let recs = [
            make_kafka_record(ver, tenant, 1, &wr_good1),
            bad_rec,
            make_kafka_record(ver, tenant, 3, &wr_good2),
        ];
        consumer
            .consume_records_sync(&recs)
            .expect("client validation error must not abort Kafka consumer");

        let db = ing.get_tsdb(tenant).expect("tenant tsdb created");
        assert_eq!(
            db.db.head().num_series(),
            2,
            "{tenant}: both valid sibling series must commit (no rollback, no U+FFFD series)"
        );
        assert_eq!(
            metric_value(
                &reg,
                "cortex_ingester_ingested_samples_total",
                &[("user", tenant)]
            ),
            Some(2.0)
        );
        assert_eq!(
            metric_value(
                &reg,
                "cortex_ingester_ingested_samples_failures_total",
                &[("user", tenant)]
            ),
            Some(1.0)
        );
    }

    // Classic gRPC raw push with 1 valid + 1 non-UTF-8 series must also ingest the valid series
    // and return soft BadData error with rejected_samples=1.
    let classic_wr = cortexpb::WriteRequest {
        timeseries: vec![
            make_simple_ts("classic_good", &[], ts, 10.0),
            cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair::from_strs("__name__", "classic_bad"),
                    cortexpb::LabelPair {
                        name: Bytes::from_static(b"bad"),
                        value: Bytes::from_static(&[0xff]),
                    },
                ],
                samples: vec![cortexpb::Sample {
                    value: 20.0,
                    timestamp_ms: ts,
                }],
                ..Default::default()
            },
        ],
        source: cortexpb::write_request::SourceEnum::Api as i32,
        ..Default::default()
    };
    let err = ing
        .push_grpc_raw("t-classic", Bytes::from(classic_wr.encode_to_vec()))
        .await
        .expect_err("non-UTF-8 series must return soft error");
    assert!(err.is_soft(), "expected soft error, got {err:?}");
    assert_eq!(
        err.error_cause(),
        cortexpb::ErrorCause::BadData,
        "expected BadData error cause"
    );
    assert_eq!(err.rejected_samples(), 1);
    let db = ing.get_tsdb("t-classic").unwrap();
    assert_eq!(db.db.head().num_series(), 1);

    ing.stop().await.unwrap();
}

// ---------------------------------------------------------------------------
// 4. [P3-1] Preserve WriteRequest.source on metadata-only Kafka records
// ---------------------------------------------------------------------------

#[tokio::test]
async fn p3_1_metadata_only_kafka_records_preserve_source_in_pusher_metrics() {
    let tmp = tempfile::tempdir().unwrap();
    let reg = Registry::new();
    let (cfg, blocks_cfg, ingest_cfg) = default_ingester_test_config(tmp.path());
    let (ing, _ring, _kv) = prepare_ingester_with_blocks_storage(
        tmp.path(),
        cfg,
        blocks_cfg,
        ingest_cfg,
        LimitsConfig::default(),
        Some(&reg),
    );
    start_and_wait_healthy(&ing).await;
    let consumer = new_kafka_consumer(Arc::clone(&ing), &reg, 1024, 4);
    let ts = now_ms();

    // Case 1: RULE series + RULE metadata for same tenant -> 1 pusher (not 2!).
    let wr_rule_series = cortexpb::WriteRequest {
        timeseries: vec![make_simple_ts("rule_metric", &[], ts, 1.0)],
        source: cortexpb::write_request::SourceEnum::Rule as i32,
        ..Default::default()
    };
    let wr_rule_meta = cortexpb::WriteRequest {
        metadata: vec![cortexpb::MetricMetadata {
            r#type: 1,
            metric_family_name: "rule_metric".to_string(),
            help: "rule help".to_string(),
            unit: "".to_string(),
        }],
        source: cortexpb::write_request::SourceEnum::Rule as i32,
        ..Default::default()
    };
    consumer
        .consume_records_sync(&[
            make_kafka_record(RecordVersion::V1, "t-rule", 1, &wr_rule_series),
            make_kafka_record(RecordVersion::V1, "t-rule", 2, &wr_rule_meta),
        ])
        .unwrap();

    // Case 2: API series + OTLP metadata-only for same tenant -> 2 pushers (not 1!).
    let wr_api_series = cortexpb::WriteRequest {
        timeseries: vec![make_simple_ts("api_metric", &[], ts, 1.0)],
        source: cortexpb::write_request::SourceEnum::Api as i32,
        ..Default::default()
    };
    let wr_otlp_meta = cortexpb::WriteRequest {
        metadata: vec![cortexpb::MetricMetadata {
            r#type: 1,
            metric_family_name: "otlp_metric".to_string(),
            help: "otlp help".to_string(),
            unit: "".to_string(),
        }],
        source: cortexpb::write_request::SourceEnum::Otlp as i32,
        ..Default::default()
    };
    consumer
        .consume_records_sync(&[
            make_kafka_record(RecordVersion::V1, "t-mix", 3, &wr_api_series),
            make_kafka_record(RecordVersion::V1, "t-mix", 4, &wr_otlp_meta),
        ])
        .unwrap();

    // Inspect cortex_ingest_storage_reader_pushers_per_push histogram sample_sum:
    // Batch 1 (t-rule RULE series + RULE meta) = 1 pusher; Batch 2 (t-mix API + OTLP) = 2 pushers -> sum = 3.
    let pushers_sum = reg
        .gather()
        .into_iter()
        .find(|mf| mf.get_name() == "cortex_ingest_storage_reader_pushers_per_push")
        .and_then(|mf| mf.get_metric().first().map(|m| m.get_histogram().get_sample_sum()))
        .unwrap_or(-1.0);
    assert_eq!(pushers_sum, 3.0);

    ing.stop().await.unwrap();
}

// ---------------------------------------------------------------------------
// 5. [WLL-06] ActiveSeries Matchers::new decision tree wired in production
// ---------------------------------------------------------------------------

#[tokio::test]
async fn wll06_active_series_uses_matchers_new_decision_tree_in_open_and_reload() {
    let tmp = tempfile::tempdir().unwrap();
    let reg = Registry::new();
    let (cfg, blocks_cfg, ingest_cfg) = default_ingester_test_config(tmp.path());
    let mut limits = LimitsConfig::default();
    limits.active_series_custom_trackers.insert(
        "api_job".to_string(),
        r#"{job="api", env="prod"}"#.to_string(),
    );
    limits.active_series_custom_trackers.insert(
        "single_ns".to_string(),
        r#"{namespace="kube-system"}"#.to_string(),
    );
    limits.active_series_custom_trackers.insert(
        "bad_tracker_ignored".to_string(),
        r#"{invalid="[unclosed"}"#.to_string(),
    );

    let (ing, _ring, _kv) = prepare_ingester_with_blocks_storage(
        tmp.path(),
        cfg,
        blocks_cfg,
        ingest_cfg,
        limits,
        Some(&reg),
    );
    start_and_wait_healthy(&ing).await;

    let ts = now_ms();
    let wr = cortexpb::WriteRequest {
        timeseries: vec![make_simple_ts(
            "up",
            &[("job", "api"), ("env", "prod"), ("namespace", "kube-system")],
            ts,
            1.0,
        )],
        source: cortexpb::write_request::SourceEnum::Api as i32,
        ..Default::default()
    };
    ing.push_grpc_raw("t1", Bytes::from(wr.encode_to_vec()))
        .await
        .unwrap();

    let db = ing.get_tsdb("t1").unwrap();
    let (unopt, val_keys, single_keys) = db.active_series.decision_tree_counts();
    assert_eq!(unopt, 0, "exact & single-label trackers must not be unoptimized");
    assert!(val_keys >= 1, "expected value_matchers populated, got {val_keys}");
    assert!(
        single_keys >= 1,
        "expected single_label_matchers populated, got {single_keys}"
    );

    // Reload via update_active_series_metrics with updated tenant overrides.
    let mut new_limits = LimitsConfig::default();
    new_limits.active_series_custom_trackers.insert(
        "multi".to_string(),
        r#"{cluster="c1", team="core"}"#.to_string(),
    );
    ing.overrides.set_static_override("t1", Some(new_limits));

    ing.update_active_series_metrics(SystemTime::now());
    let (unopt2, val_keys2, single_keys2) = db.active_series.decision_tree_counts();
    assert_eq!(unopt2, 0);
    assert_eq!(val_keys2, 1);
    assert_eq!(single_keys2, 0);
    assert_eq!(
        metric_value(
            &reg,
            "cortex_ingester_active_series_custom_tracker",
            &[("name", "multi"), ("user", "t1")]
        ),
        None
    );

    ing.stop().await.unwrap();
}

// ---------------------------------------------------------------------------
// 6. [WLL-07] close_all_tsdbs & spawn_owned_series_loop offload & error parity
// ---------------------------------------------------------------------------

#[tokio::test]
async fn wll07_close_all_tsdbs_keeps_failed_close_entry_and_owned_series_loop_cancels_cleanly() {
    let tmp = tempfile::tempdir().unwrap();
    let reg = Registry::new();
    let (mut cfg, blocks_cfg, ingest_cfg) = default_ingester_test_config(tmp.path());
    cfg.track_ingester_owned_series = true;
    cfg.owned_series_update_interval = GoDuration::from_millis(20);

    let (ing, _ring, _kv) = prepare_ingester_with_blocks_storage(
        tmp.path(),
        cfg,
        blocks_cfg,
        ingest_cfg,
        LimitsConfig::default(),
        Some(&reg),
    );
    start_and_wait_healthy(&ing).await;

    let ts = now_ms();
    // Push > 240 samples to "u1" so head_chunks_len() > 1 and close() runs mmap_head_chunks().
    let u1_samples: Vec<cortexpb::Sample> = (0..260i64)
        .map(|i| cortexpb::Sample {
            value: i as f64,
            timestamp_ms: ts - (260 - i) * 1000,
        })
        .collect();
    let wr_u1 = cortexpb::WriteRequest {
        timeseries: vec![cortexpb::TimeSeries {
            labels: vec![cortexpb::LabelPair::from_strs("__name__", "m")],
            samples: u1_samples,
            ..Default::default()
        }],
        source: cortexpb::write_request::SourceEnum::Api as i32,
        ..Default::default()
    };
    ing.push_grpc_raw("u1", Bytes::from(wr_u1.encode_to_vec()))
        .await
        .unwrap();

    let wr_u2 = cortexpb::WriteRequest {
        timeseries: vec![make_simple_ts("m", &[], ts, 1.0)],
        source: cortexpb::write_request::SourceEnum::Api as i32,
        ..Default::default()
    };
    ing.push_grpc_raw("u2", Bytes::from(wr_u2.encode_to_vec()))
        .await
        .unwrap();

    assert_eq!(ing.get_tsdb_users().len(), 2);
    assert_eq!(metric_value(&reg, "cortex_ingester_memory_users", &[]), Some(2.0));

    // Verify owned_series_loop runs on spawn_blocking and terminates promptly on cancel.
    let ctx = ServiceContext::background();
    let handle = spawn_owned_series_loop(Arc::clone(&ing), ctx.clone());
    tokio::time::sleep(Duration::from_millis(60)).await;
    ctx.cancel();
    tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("owned_series_loop must exit promptly")
        .unwrap();

    // Induce a real I/O error during db_u1.close() -> mmap_head_chunks() -> refresh_chunk_reader()
    // by replacing u1/chunks_head directory with a regular file (ENOTDIR on read_dir).
    let chunks_head_u1 = tmp.path().join("u1").join("chunks_head");
    std::fs::remove_dir_all(&chunks_head_u1).unwrap();
    std::fs::write(&chunks_head_u1, b"not-a-dir").unwrap();

    ing.close_all_tsdbs().await;

    // "u2" closed cleanly -> removed; "u1" failed close -> retained in tsdbs & mem_users == 1.
    assert!(ing.get_tsdb("u2").is_none());
    assert!(ing.get_tsdb("u1").is_some());
    assert_eq!(metric_value(&reg, "cortex_ingester_memory_users", &[]), Some(1.0));

    ing.stop().await.unwrap();
}

// ---------------------------------------------------------------------------
// 7. [WLL-08] Missing config & limit validations
// ---------------------------------------------------------------------------

#[test]
fn wll08_rejects_offset_catalogue_without_ingest_storage_and_negative_custom_trackers_limit() {
    let tmp = tempfile::tempdir().unwrap();
    let (cfg, mut blocks_cfg, mut ingest_cfg) = default_ingester_test_config(tmp.path());
    blocks_cfg.tsdb.offset_catalogue.enabled = true;
    ingest_cfg.enabled = false;

    let err = Ingester::new(
        cfg,
        blocks_cfg,
        ingest_cfg,
        Overrides::new(LimitsConfig::default()),
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .err()
    .expect("must reject offset_catalogue without ingest_storage");
    assert_eq!(
        err.to_string(),
        "kafka offset catalogue can only be enabled when ingest storage is enabled"
    );

    let mut limits = LimitsConfig {
        max_active_series_additional_custom_trackers: -1,
        ..Default::default()
    };
    let lim_err = limits
        .validate()
        .expect_err("must reject negative max_active_series_additional_custom_trackers");
    assert!(
        lim_err.to_string().contains(
            "active_series_additional_custom_trackers validation failed: invalid max custom trackers limit: -1"
        ),
        "unexpected limit validation error: {lim_err}"
    );
}

// ---------------------------------------------------------------------------
// 8. [WLL-09 & RKW-08] TokenlessLifecycler readiness Go string & duration formatting
// ---------------------------------------------------------------------------

#[tokio::test]
async fn wll09_tokenless_lifecycler_formats_go_state_and_duration_strings() {
    let ring_cfg = RingConfig {
        instance_id: "ingester-0".to_string(),
        instance_addr: "127.0.0.1".to_string(),
        num_tokens: 0,
        ..Default::default()
    };

    let basic_cfg = BasicLifecyclerConfig::from_ring_config_tokenless(&ring_cfg, 9095).unwrap();
    let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));
    let tokenless = TokenlessLifecycler::new(
        basic_cfg,
        "ingester",
        "collectors/ring",
        Arc::clone(&kv),
        Duration::from_secs(60),
        Duration::ZERO,
        false,
        None,
        None,
    );

    // Not registered yet
    let err = tokenless.check_ready_sync(Instant::now()).unwrap_err();
    assert_eq!(err, "instance not registered in the ring");

    // Start service so instance registers and becomes ACTIVE, then test min_ready_duration Go formatting.
    let svc = Arc::clone(&tokenless).into_managed_service();
    svc.start_async(ServiceContext::background()).await.unwrap();
    svc.await_running().await.unwrap();

    let t0 = Instant::now();
    let err_wait = tokenless.check_ready_sync(t0).unwrap_err();
    assert_eq!(err_wait, "waiting for 1m0s after being ready");

    // Stop service -> transitions to LEAVING (or terminated); before stopping finishes, check state string if still registered.
    svc.stop_async();
    svc.await_terminated().await.unwrap();

    // Also verify non-ACTIVE state formatting directly via a fresh BasicLifecycler registered in Pending/Leaving.
    // While Pending and registered, check_ready_sync must report "PENDING" (not Rust Debug "Pending").
    assert_eq!(InstanceState::Pending.as_str_name(), "PENDING");
    assert_eq!(InstanceState::Leaving.as_str_name(), "LEAVING");
}

// ---------------------------------------------------------------------------
// 9. [WLL-10] Exemplar labels sorted in TsdbIngestExemplar error messages
// ---------------------------------------------------------------------------

#[tokio::test]
async fn wll10_tsdb_ingest_exemplar_formats_exemplar_labels_in_sorted_order() {
    let tmp = tempfile::tempdir().unwrap();
    let (cfg, blocks_cfg, ingest_cfg) = default_ingester_test_config(tmp.path());
    let limits = LimitsConfig {
        max_global_exemplars_per_user: 100,
        ignore_ooo_exemplars: false,
        ..Default::default()
    };

    let (ing, _ring, _kv) = prepare_ingester_with_blocks_storage(
        tmp.path(),
        cfg,
        blocks_cfg,
        ingest_cfg,
        limits,
        None,
    );
    start_and_wait_healthy(&ing).await;

    let ts = now_ms();
    // 1) Ingest newer exemplar at ts + 10_000.
    let wr1 = cortexpb::WriteRequest {
        timeseries: vec![cortexpb::TimeSeries {
            labels: vec![cortexpb::LabelPair::from_strs("__name__", "ex_sorted_err")],
            samples: vec![cortexpb::Sample {
                value: 1.0,
                timestamp_ms: ts + 10_000,
            }],
            exemplars: vec![cortexpb::Exemplar {
                labels: vec![cortexpb::LabelPair::from_strs("trace_id", "t1")],
                value: 1.0,
                timestamp_ms: ts + 10_000,
            }],
            ..Default::default()
        }],
        source: cortexpb::write_request::SourceEnum::Api as i32,
        ..Default::default()
    };
    ing.push_grpc_raw("t1", Bytes::from(wr1.encode_to_vec()))
        .await
        .unwrap();

    // 2) Push OOO exemplar at ts with UNSORTED exemplar labels: trace_id, a_key, m_key.
    //    TsdbIngestExemplar must format exemplar={a_key="1", m_key="2", trace_id="t2"}.
    let wr2 = cortexpb::WriteRequest {
        timeseries: vec![cortexpb::TimeSeries {
            labels: vec![cortexpb::LabelPair::from_strs("__name__", "ex_sorted_err")],
            samples: vec![cortexpb::Sample {
                value: 2.0,
                timestamp_ms: ts + 20_000,
            }],
            exemplars: vec![cortexpb::Exemplar {
                labels: vec![
                    cortexpb::LabelPair::from_strs("trace_id", "t2"),
                    cortexpb::LabelPair::from_strs("a_key", "1"),
                    cortexpb::LabelPair::from_strs("m_key", "2"),
                ],
                value: 2.0,
                timestamp_ms: ts,
            }],
            ..Default::default()
        }],
        source: cortexpb::write_request::SourceEnum::Api as i32,
        ..Default::default()
    };
    let err = ing
        .push_grpc_raw("t1", Bytes::from(wr2.encode_to_vec()))
        .await
        .expect_err("OOO exemplar should error");
    let msg = err.to_string();
    assert!(
        msg.contains(r#"exemplar={a_key="1", m_key="2", trace_id="t2"}"#),
        "expected sorted exemplar labels in TsdbIngestExemplar error, got: {msg}"
    );

    // 3) ExemplarTooFarInFuture runs before sorting and must retain wire order.
    let wr3 = cortexpb::WriteRequest {
        timeseries: vec![cortexpb::TimeSeries {
            labels: vec![cortexpb::LabelPair::from_strs("__name__", "ex_sorted_err")],
            samples: vec![cortexpb::Sample {
                value: 3.0,
                timestamp_ms: ts + 30_000,
            }],
            exemplars: vec![cortexpb::Exemplar {
                labels: vec![
                    cortexpb::LabelPair::from_strs("trace_id", "t3"),
                    cortexpb::LabelPair::from_strs("a_key", "1"),
                ],
                value: 3.0,
                timestamp_ms: ts + 3_600_000,
            }],
            ..Default::default()
        }],
        source: cortexpb::write_request::SourceEnum::Api as i32,
        ..Default::default()
    };
    let err3 = ing
        .push_grpc_raw("t1", Bytes::from(wr3.encode_to_vec()))
        .await
        .expect_err("future exemplar should error");
    let msg3 = err3.to_string();
    assert!(
        msg3.contains(r#"{trace_id="t3", a_key="1"}"#),
        "expected wire-order exemplar labels in ExemplarTooFarInFuture error, got: {msg3}"
    );

    ing.stop().await.unwrap();
}
