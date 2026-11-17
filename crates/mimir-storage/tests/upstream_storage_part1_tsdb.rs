//! 1:1 upstream test ports for `pkg/storage/tsdb/` (7 files, 18 tests).

use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use futures::stream::BoxStream;
use mimir_config::{BlocksStorageConfig, FlagSet, GoDuration};
use mimir_storage::ulid::Ulid;
use mimir_storage::{
    all_users, delete_block, delete_no_compact_marker, download_meta, hash_block_id, is_block_dir,
    mark_for_deletion, mark_for_no_compact, tenant_deletion_mark_exists, upload_block, BlockMeta,
    BlockMetaCompaction, BlockStats, BucketClient, BucketError, DeletionMark, DurationList,
    NoCompactMark, NoCompactReason, ShipperError, SourceType, TSDBMetrics, ThanosMeta,
    UsersScanner, CHUNKS_DIRNAME, DELETION_MARK_FILENAME, DELETION_MARK_VERSION_1,
    INDEX_FILENAME, META_FILENAME, MIMIR_INTERNALS_PREFIX, NO_COMPACT_MARK_FILENAME,
    NO_COMPACT_MARK_VERSION_1, OUT_OF_ORDER_EXTERNAL_LABEL, OUT_OF_ORDER_EXTERNAL_LABEL_VALUE,
    TENANT_DELETION_MARK_PATH, TSDB_VERSION_1,
};
use object_store::memory::InMemory;
use object_store::path::Path as ObjPath;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOpts,
    PutOptions, PutPayload, PutResult,
};
use prometheus::core::Collector;
use prometheus::proto::{Metric, MetricFamily, MetricType, Quantile, Summary as ProtoSummary};
use prometheus::{Counter, CounterVec, Gauge, Histogram, HistogramOpts, Opts, Registry, TextEncoder};

// ===========================================================================
// pkg/storage/tsdb/config_test.go
// ===========================================================================

// Upstream: pkg/storage/tsdb/config_test.go::TestConfig_Validate
#[test]
fn test_config_validate() {
    type SetupFn = fn(&mut BlocksStorageConfig, &mut bool);
    struct Case {
        name: &'static str,
        setup: SetupFn,
        expected_err: Option<&'static str>,
    }

    let cases: Vec<Case> = vec![
        Case {
            name: "should pass on S3 backend",
            setup: |cfg, _| cfg.bucket.backend_cfg.backend = "s3".to_string(),
            expected_err: None,
        },
        Case {
            name: "should pass on GCS backend",
            setup: |cfg, _| cfg.bucket.backend_cfg.backend = "gcs".to_string(),
            expected_err: None,
        },
        Case {
            name: "should fail on unknown storage backend",
            setup: |cfg, _| cfg.bucket.backend_cfg.backend = "unknown".to_string(),
            expected_err: Some("unsupported storage backend"),
        },
        Case {
            name: "should fail on invalid ship concurrency",
            setup: |cfg, _| cfg.tsdb.ship_concurrency = 0,
            expected_err: Some("invalid TSDB ship concurrency"),
        },
        Case {
            name: "should pass on invalid ship concurrency but shipping is disabled",
            setup: |cfg, _| {
                cfg.tsdb.ship_concurrency = 0;
                cfg.tsdb.ship_interval = GoDuration(0);
            },
            expected_err: None,
        },
        Case {
            name: "should fail on invalid compaction interval",
            setup: |cfg, _| cfg.tsdb.head_compaction_interval = GoDuration(0),
            expected_err: Some("invalid TSDB compaction interval"),
        },
        Case {
            name: "should fail on too high compaction interval",
            setup: |cfg, _| cfg.tsdb.head_compaction_interval = GoDuration::from_mins(20),
            expected_err: Some("invalid TSDB compaction interval"),
        },
        Case {
            name: "should fail on invalid compaction concurrency",
            setup: |cfg, _| cfg.tsdb.head_compaction_concurrency = 0,
            expected_err: Some("invalid TSDB compaction concurrency"),
        },
        Case {
            name: "should pass on valid compaction concurrency",
            setup: |cfg, _| cfg.tsdb.head_compaction_concurrency = 10,
            expected_err: None,
        },
        Case {
            name: "should fail on negative stripe size",
            setup: |cfg, _| cfg.tsdb.stripe_size = -2,
            expected_err: Some("invalid TSDB stripe size"),
        },
        Case {
            name: "should fail on stripe size 0",
            setup: |cfg, _| cfg.tsdb.stripe_size = 0,
            expected_err: Some("invalid TSDB stripe size"),
        },
        Case {
            name: "should fail on stripe size 1",
            setup: |cfg, _| cfg.tsdb.stripe_size = 1,
            expected_err: Some("invalid TSDB stripe size"),
        },
        Case {
            name: "should pass on valid stripe size",
            setup: |cfg, _| cfg.tsdb.stripe_size = 1 << 14,
            expected_err: None,
        },
        Case {
            name: "should fail on empty block ranges",
            setup: |cfg, _| cfg.tsdb.block_ranges_period.clear(),
            expected_err: Some("empty block ranges for TSDB"),
        },
        Case {
            name: "should fail on invalid TSDB WAL segment size",
            setup: |cfg, _| cfg.tsdb.wal_segment_size_bytes = 0,
            expected_err: Some("invalid TSDB WAL segment size bytes"),
        },
        Case {
            name: "should fail on invalid store-gateway streaming batch size",
            setup: |cfg, _| cfg.bucket_store.streaming_series_batch_size = 0,
            expected_err: Some("invalid store-gateway streaming batch size"),
        },
        Case {
            name: "should fail if forced compaction is enabled but active series tracker is not",
            setup: |cfg, active_series| {
                cfg.tsdb.early_head_compaction_min_in_memory_series = 1_000_000;
                *active_series = false;
            },
            expected_err: Some("early compaction requires"),
        },
        Case {
            name: "should fail on invalid forced compaction min series reduction percentage",
            setup: |cfg, _| cfg.tsdb.early_head_compaction_min_estimated_series_reduction_percentage = 101,
            expected_err: Some("early compaction minimum series reduction percentage"),
        },
        Case {
            name: "should not fail on ignore deletion marks while querying delay < store-gateways delay",
            setup: |cfg, _| {
                cfg.bucket_store.ignore_deletion_mark_while_querying_delay = GoDuration::from_mins(120);
                cfg.bucket_store.ignore_deletion_mark_delay = GoDuration::from_mins(121);
            },
            expected_err: None,
        },
        Case {
            name: "should fail on ignore deletion marks while querying delay == store-gateways delay",
            setup: |cfg, _| {
                cfg.bucket_store.ignore_deletion_mark_while_querying_delay = GoDuration::from_hours(2);
                cfg.bucket_store.ignore_deletion_mark_delay = GoDuration::from_hours(2);
            },
            expected_err: Some("must be less than"),
        },
        Case {
            name: "should fail on ignore deletion marks while querying delay > store-gateways delay",
            setup: |cfg, _| {
                cfg.bucket_store.ignore_deletion_mark_while_querying_delay = GoDuration::from_mins(121);
                cfg.bucket_store.ignore_deletion_mark_delay = GoDuration::from_mins(120);
            },
            expected_err: Some("must be less than"),
        },
        Case {
            name: "should not fail on ignore deletion marks while querying delay > 3x sync interval",
            setup: |cfg, _| {
                cfg.bucket_store.sync_interval = GoDuration::from_mins(10);
                cfg.bucket_store.ignore_deletion_mark_while_querying_delay = GoDuration::from_mins(31);
            },
            expected_err: None,
        },
        Case {
            name: "should fail on ignore deletion marks while query delay == 3x sync interval",
            setup: |cfg, _| {
                cfg.bucket_store.sync_interval = GoDuration::from_mins(10);
                cfg.bucket_store.ignore_deletion_mark_while_querying_delay = GoDuration::from_mins(30);
            },
            expected_err: Some("must be greater than 3"),
        },
        Case {
            name: "should fail on ignore deletion marks while query delay < 3x sync interval",
            setup: |cfg, _| {
                cfg.bucket_store.sync_interval = GoDuration::from_mins(10);
                cfg.bucket_store.ignore_deletion_mark_while_querying_delay = GoDuration::from_mins(29);
            },
            expected_err: Some("must be greater than 3"),
        },
    ];

    for tc in cases {
        let mut cfg = BlocksStorageConfig::default();
        let mut active_series_enabled = true;
        (tc.setup)(&mut cfg, &mut active_series_enabled);
        let res = cfg.validate(active_series_enabled);
        match tc.expected_err {
            None => assert!(res.is_ok(), "{}: unexpected err: {:?}", tc.name, res),
            Some(sub) => {
                let err = res.expect_err(&format!("{}: expected error", tc.name));
                assert!(
                    err.to_string().contains(sub),
                    "{}: expected error containing {:?}, got {:?}",
                    tc.name,
                    sub,
                    err.to_string()
                );
            }
        }
    }
}

/// mimir-rs: TSDB settings Go accepts but mimir-rs does not implement yet are rejected at
/// config validation instead of being silently ignored (see PORT_STATUS.md).
#[test]
fn test_config_validate_rejects_unsupported_tsdb_settings() {
    type SetupFn = fn(&mut BlocksStorageConfig);
    let cases: [(&str, SetupFn, &str); 3] = [
        (
            "multiple block ranges",
            |cfg| {
                cfg.tsdb.block_ranges_period =
                    vec![GoDuration::from_hours(2), GoDuration::from_hours(12)]
            },
            "block-ranges-period",
        ),
        (
            "head chunks end time variance",
            |cfg| cfg.tsdb.head_chunks_end_time_variance = 0.5,
            "head-chunks-end-time-variance",
        ),
        (
            "memory snapshot on shutdown",
            |cfg| cfg.tsdb.memory_snapshot_on_shutdown = true,
            "memory-snapshot-on-shutdown",
        ),
    ];
    assert!(BlocksStorageConfig::default().validate(true).is_ok());
    for (name, setup, want) in cases {
        let mut cfg = BlocksStorageConfig::default();
        setup(&mut cfg);
        let err = cfg.validate(true).expect_err(name).to_string();
        assert!(err.contains("mimir-rs") && err.contains(want), "{name}: {err}");
    }
}

// Upstream: pkg/storage/tsdb/config_test.go::TestConfig_DurationList
#[test]
fn test_config_duration_list() {
    // 1. Default to 2h via flag registration.
    let mut cfg = BlocksStorageConfig::default();
    let mut fs1 = FlagSet::new();
    cfg.clone()
        .register_flags_into(&mut fs1, |c| c, |c| c);
    fs1.parse(&mut cfg, &[]).unwrap();
    assert_eq!(
        DurationList(cfg.tsdb.block_ranges_period.clone()).to_milliseconds(),
        vec![7_200_000]
    );

    // 2. Parse ranges correctly.
    let parsed = DurationList::parse_csv("2h,10h,50h").unwrap();
    assert_eq!(
        parsed.to_milliseconds(),
        vec![7_200_000, 36_000_000, 180_000_000]
    );

    // 3. Handle multiple flag parses without duplicating default.
    let mut cfg3 = BlocksStorageConfig::default();
    let mut fs_a = FlagSet::new();
    cfg3.clone()
        .register_flags_into(&mut fs_a, |c| c, |c| c);
    fs_a.parse(
        &mut cfg3,
        &["-blocks-storage.tsdb.block-ranges-period=2h,10h,50h".to_string()],
    )
    .unwrap();
    let mut fs_b = FlagSet::new();
    cfg3.clone()
        .register_flags_into(&mut fs_b, |c| c, |c| c);
    fs_b.parse(
        &mut cfg3,
        &["-blocks-storage.tsdb.block-ranges-period=2h".to_string()],
    )
    .unwrap();
    assert_eq!(
        DurationList(cfg3.tsdb.block_ranges_period).to_milliseconds(),
        vec![7_200_000]
    );
}

// ===========================================================================
// pkg/storage/tsdb/tenant_deletion_mark_test.go
// ===========================================================================

// Upstream: pkg/storage/tsdb/tenant_deletion_mark_test.go::TestTenantDeletionMarkExists
#[tokio::test]
async fn test_tenant_deletion_mark_exists() {
    const USERNAME: &str = "user";

    let cases: Vec<(&str, Vec<&str>, bool)> = vec![
        ("empty", vec![], false),
        (
            "mark doesn't exist",
            vec!["user/01EQK4QKFHVSZYVJ908Y7HH9E0/meta.json"],
            false,
        ),
        (
            "mark exists",
            vec![
                "user/01EQK4QKFHVSZYVJ908Y7HH9E0/meta.json",
                "user/markers/tenant-deletion-mark.json",
            ],
            true,
        ),
    ];

    assert_eq!(TENANT_DELETION_MARK_PATH, "markers/tenant-deletion-mark.json");

    for (name, objects, expected) in cases {
        let bkt = BucketClient::new_in_memory("test");
        for obj in objects {
            bkt.upload(obj, Bytes::from_static(b"data")).await.unwrap();
        }
        let exists = tenant_deletion_mark_exists(&bkt, USERNAME).await.unwrap();
        assert_eq!(exists, expected, "case: {name}");
    }
}

// ===========================================================================
// pkg/storage/tsdb/tsdb_metrics_test.go
// ===========================================================================

struct ConstSummaryCollector {
    name: String,
    help: String,
    count: u64,
    sum: f64,
    quantiles: Vec<(f64, f64)>,
    desc: prometheus::core::Desc,
}

impl ConstSummaryCollector {
    fn new(name: &str, help: &str, count: u64, sum: f64, quantiles: Vec<(f64, f64)>) -> Self {
        let desc = prometheus::core::Desc::new(
            name.to_string(),
            help.to_string(),
            Vec::new(),
            HashMap::new(),
        )
        .unwrap();
        Self {
            name: name.to_string(),
            help: help.to_string(),
            count,
            sum,
            quantiles,
            desc,
        }
    }
}

impl Collector for ConstSummaryCollector {
    fn desc(&self) -> Vec<&prometheus::core::Desc> {
        vec![&self.desc]
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let mut s = ProtoSummary::default();
        s.set_sample_count(self.count);
        s.set_sample_sum(self.sum);
        let qs: Vec<Quantile> = self
            .quantiles
            .iter()
            .map(|&(q, v)| {
                let mut pq = Quantile::default();
                pq.set_quantile(q);
                pq.set_value(v);
                pq
            })
            .collect();
        s.set_quantile(qs.into());
        let mut m = Metric::default();
        m.set_summary(s);
        let mut mf = MetricFamily::default();
        mf.set_name(self.name.clone());
        mf.set_help(self.help.clone());
        mf.set_field_type(MetricType::SUMMARY);
        mf.set_metric(vec![m].into());
        vec![mf]
    }
}

fn populate_tsdb_metrics(base: f64) -> Arc<Registry> {
    let r = Arc::new(Registry::new());

    let reg_gauge = |name: &str, help: &str, val: f64| {
        let g = Gauge::with_opts(Opts::new(name, help)).unwrap();
        g.set(val);
        r.register(Box::new(g)).unwrap();
    };
    let reg_counter = |name: &str, help: &str, val: f64| {
        let c = Counter::with_opts(Opts::new(name, help)).unwrap();
        c.inc_by(val);
        r.register(Box::new(c)).unwrap();
    };

    reg_gauge("prometheus_tsdb_head_series", "head series", 4.0 * base);
    reg_counter("prometheus_tsdb_head_series_created_total", "created", 5.0 * base);
    reg_counter("prometheus_tsdb_head_series_removed_total", "removed", 6.0 * base);
    reg_counter("prometheus_tsdb_compactions_total", "compactions", 7.0 * base);

    let dur = Histogram::with_opts(
        HistogramOpts::new("prometheus_tsdb_compaction_duration_seconds", "dur")
            .buckets(prometheus::exponential_buckets(1.0, 2.0, 10).unwrap()),
    )
    .unwrap();
    dur.observe(9.0);
    r.register(Box::new(dur)).unwrap();

    r.register(Box::new(ConstSummaryCollector::new(
        "prometheus_tsdb_wal_fsync_duration_seconds",
        "Duration of WAL fsync.",
        1,
        10.0,
        vec![(0.5, 10.0), (0.9, 10.0), (0.99, 10.0)],
    )))
    .unwrap();

    reg_counter("prometheus_tsdb_wal_page_flushes_total", "flushes", 11.0 * base);
    reg_counter("prometheus_tsdb_wal_completed_pages_total", "pages", 12.0 * base);
    reg_counter("prometheus_tsdb_wal_truncations_failed_total", "tfail", 13.0 * base);
    reg_counter("prometheus_tsdb_wal_truncations_total", "ttot", 14.0 * base);
    reg_counter("prometheus_tsdb_wal_writes_failed_total", "wfail", 15.0 * base);
    reg_counter("prometheus_tsdb_checkpoint_deletions_failed_total", "cdfail", 16.0 * base);
    reg_counter("prometheus_tsdb_checkpoint_deletions_total", "cdtot", 17.0 * base);
    reg_counter("prometheus_tsdb_checkpoint_creations_failed_total", "ccfail", 18.0 * base);
    reg_counter("prometheus_tsdb_checkpoint_creations_total", "cctot", 19.0 * base);
    reg_gauge("prometheus_tsdb_head_active_appenders", "appenders", 20.0 * base);
    reg_counter("prometheus_tsdb_head_series_not_found_total", "notfound", 21.0 * base);
    reg_gauge("prometheus_tsdb_head_chunks", "chunks", 22.0 * base);
    reg_counter("prometheus_tsdb_head_chunks_created_total", "ccreated", 23.0 * base);
    reg_counter("prometheus_tsdb_head_chunks_removed_total", "cremoved", 24.0 * base);

    r.register(Box::new(ConstSummaryCollector::new(
        "prometheus_tsdb_wal_truncate_duration_seconds",
        "Duration of WAL truncation.",
        1,
        25.0,
        Vec::new(),
    )))
    .unwrap();

    reg_counter("prometheus_tsdb_mmap_chunk_corruptions_total", "mcorr", 26.0 * base);

    let q_ops = CounterVec::new(
        Opts::new("prometheus_tsdb_chunk_write_queue_operations_total", "qops"),
        &["operation"],
    )
    .unwrap();
    q_ops.with_label_values(&["add"]).inc_by(50.0);
    q_ops.with_label_values(&["get"]).inc_by(10.0);
    q_ops.with_label_values(&["complete"]).inc_by(40.0);
    r.register(Box::new(q_ops)).unwrap();

    let ooo = Histogram::with_opts(
        HistogramOpts::new("prometheus_tsdb_sample_ooo_delta", "ooo")
            .buckets(vec![60.0 * 10.0, 60.0 * 60.0 * 24.0]),
    )
    .unwrap();
    ooo.observe(7.0 * base);
    r.register(Box::new(ooo)).unwrap();

    reg_counter("prometheus_tsdb_wal_corruptions_total", "wcorr", 27.0 * base);
    reg_counter("prometheus_tsdb_head_truncations_failed_total", "htfail", 28.0 * base);
    reg_counter("prometheus_tsdb_head_truncations_total", "httot", 29.0 * base);

    r.register(Box::new(ConstSummaryCollector::new(
        "prometheus_tsdb_head_gc_duration_seconds",
        "Runtime of garbage collection in the head block.",
        1,
        3.0,
        Vec::new(),
    )))
    .unwrap();

    reg_gauge("prometheus_tsdb_blocks_loaded", "loaded", 5.0);
    reg_counter("prometheus_tsdb_reloads_total", "reloads", 10.0);
    reg_counter("prometheus_tsdb_reloads_failures_total", "rfail", 7.0);
    reg_gauge("prometheus_tsdb_symbol_table_size_bytes", "sym", 1024.0 * base);
    reg_gauge("prometheus_tsdb_storage_blocks_bytes", "bytes", 4096.0 * base);
    reg_counter("prometheus_tsdb_time_retentions_total", "ret", 11.0);

    reg_counter("prometheus_tsdb_exemplar_exemplars_appended_total", "exapp", 100.0);
    reg_gauge("prometheus_tsdb_exemplar_exemplars_in_storage", "exstor", 10.0);
    reg_gauge("prometheus_tsdb_exemplar_series_with_exemplars_in_storage", "exser", 1.0);
    reg_gauge("prometheus_tsdb_exemplar_last_exemplars_timestamp_seconds", "exts", 1234.0);
    reg_counter("prometheus_tsdb_exemplar_out_of_order_exemplars_total", "exooo", 3.0);
    reg_counter("prometheus_tsdb_head_out_of_order_samples_appended_total", "oooapp", 3.0);
    reg_counter("prometheus_tsdb_mmap_chunks_total", "mmap", 30.0 * base);
    reg_gauge("prometheus_tsdb_head_chunks_max_mmapped", "maxmm", 31.0 * base);

    let wal_unk = CounterVec::new(
        Opts::new("prometheus_tsdb_wal_replay_unknown_refs_total", "walunk"),
        &["type"],
    )
    .unwrap();
    wal_unk.with_label_values(&["series"]).inc_by(base);
    wal_unk.with_label_values(&["samples"]).inc_by(base * 2.0);
    r.register(Box::new(wal_unk)).unwrap();

    let wbl_unk = CounterVec::new(
        Opts::new("prometheus_tsdb_wbl_replay_unknown_refs_total", "wblunk"),
        &["type"],
    )
    .unwrap();
    wbl_unk.with_label_values(&["exemplars"]).inc_by(base);
    r.register(Box::new(wbl_unk)).unwrap();

    r
}

// Upstream: pkg/storage/tsdb/tsdb_metrics_test.go::TestTSDBMetrics
#[test]
fn test_tsdb_metrics() {
    let main_reg = Registry::new();
    let tsdb_metrics = TSDBMetrics::new("cortex_ingester_", Some(&main_reg));

    tsdb_metrics.set_registry_for_tenant("user1", populate_tsdb_metrics(12345.0));
    tsdb_metrics.set_registry_for_tenant("user2", populate_tsdb_metrics(85787.0));
    tsdb_metrics.set_registry_for_tenant("user3", populate_tsdb_metrics(999.0));

    let text = TextEncoder::new().encode_to_string(&main_reg.gather()).unwrap();

    let expected_lines = [
        "cortex_ingester_tsdb_compactions_total 693917",
        r#"cortex_ingester_tsdb_compaction_duration_seconds_bucket{le="8"} 0"#,
        r#"cortex_ingester_tsdb_compaction_duration_seconds_bucket{le="16"} 3"#,
        "cortex_ingester_tsdb_compaction_duration_seconds_sum 27",
        "cortex_ingester_tsdb_compaction_duration_seconds_count 3",
        r#"cortex_ingester_tsdb_wal_fsync_duration_seconds{quantile="0.5"} 30"#,
        r#"cortex_ingester_tsdb_wal_fsync_duration_seconds{quantile="0.9"} 30"#,
        r#"cortex_ingester_tsdb_wal_fsync_duration_seconds{quantile="0.99"} 30"#,
        "cortex_ingester_tsdb_wal_fsync_duration_seconds_sum 30",
        "cortex_ingester_tsdb_wal_fsync_duration_seconds_count 3",
        "cortex_ingester_tsdb_wal_page_flushes_total 1090441",
        "cortex_ingester_tsdb_wal_completed_pages_total 1189572",
        "cortex_ingester_tsdb_wal_truncations_failed_total 1288703",
        "cortex_ingester_tsdb_wal_truncations_total 1387834",
        "cortex_ingester_tsdb_wal_corruptions_total 2676537",
        "cortex_ingester_tsdb_wal_writes_failed_total 1486965",
        "cortex_ingester_tsdb_head_truncations_failed_total 2775668",
        "cortex_ingester_tsdb_head_truncations_total 2874799",
        "cortex_ingester_tsdb_head_gc_duration_seconds_sum 9",
        "cortex_ingester_tsdb_head_gc_duration_seconds_count 3",
        "cortex_ingester_tsdb_checkpoint_deletions_failed_total 1586096",
        "cortex_ingester_tsdb_checkpoint_deletions_total 1685227",
        "cortex_ingester_tsdb_checkpoint_creations_failed_total 1784358",
        "cortex_ingester_tsdb_checkpoint_creations_total 1883489",
        "cortex_ingester_memory_series 396524",
        r#"cortex_ingester_memory_series_created_total{user="user1"} 61725"#,
        r#"cortex_ingester_memory_series_created_total{user="user2"} 428935"#,
        r#"cortex_ingester_memory_series_created_total{user="user3"} 4995"#,
        r#"cortex_ingester_memory_series_removed_total{user="user1"} 74070"#,
        r#"cortex_ingester_memory_series_removed_total{user="user2"} 514722"#,
        r#"cortex_ingester_memory_series_removed_total{user="user3"} 5994"#,
        "cortex_ingester_tsdb_head_active_appenders 1982620",
        "cortex_ingester_tsdb_head_series_not_found_total 2081751",
        "cortex_ingester_tsdb_head_chunks 2180882",
        r#"cortex_ingester_tsdb_head_chunks_created_total{user="user1"} 283935"#,
        r#"cortex_ingester_tsdb_head_chunks_created_total{user="user2"} 1973101"#,
        r#"cortex_ingester_tsdb_head_chunks_created_total{user="user3"} 22977"#,
        r#"cortex_ingester_tsdb_head_chunks_removed_total{user="user1"} 296280"#,
        r#"cortex_ingester_tsdb_head_chunks_removed_total{user="user2"} 2058888"#,
        r#"cortex_ingester_tsdb_head_chunks_removed_total{user="user3"} 23976"#,
        "cortex_ingester_tsdb_wal_truncate_duration_seconds_sum 75",
        "cortex_ingester_tsdb_wal_truncate_duration_seconds_count 3",
        "cortex_ingester_tsdb_mmap_chunk_corruptions_total 2577406",
        r#"cortex_ingester_tsdb_mmap_chunk_write_queue_operations_total{operation="add"} 150"#,
        r#"cortex_ingester_tsdb_mmap_chunk_write_queue_operations_total{operation="complete"} 120"#,
        r#"cortex_ingester_tsdb_mmap_chunk_write_queue_operations_total{operation="get"} 30"#,
        "cortex_ingester_tsdb_mmap_chunks_total 2973930",
        "cortex_ingester_tsdb_head_chunks_max_mmapped 2659397",
        "cortex_ingester_tsdb_blocks_loaded 15",
        "cortex_ingester_tsdb_reloads_total 30",
        r#"cortex_ingester_tsdb_sample_out_of_order_delta_seconds_bucket{le="600"} 0"#,
        r#"cortex_ingester_tsdb_sample_out_of_order_delta_seconds_bucket{le="86400"} 1"#,
        r#"cortex_ingester_tsdb_sample_out_of_order_delta_seconds_bucket{le="+Inf"} 3"#,
        "cortex_ingester_tsdb_sample_out_of_order_delta_seconds_sum 693917",
        "cortex_ingester_tsdb_sample_out_of_order_delta_seconds_count 3",
        "cortex_ingester_tsdb_reloads_failures_total 21",
        r#"cortex_ingester_tsdb_symbol_table_size_bytes{user="user1"} 12641280"#,
        r#"cortex_ingester_tsdb_symbol_table_size_bytes{user="user2"} 87845888"#,
        r#"cortex_ingester_tsdb_symbol_table_size_bytes{user="user3"} 1022976"#,
        r#"cortex_ingester_tsdb_storage_blocks_bytes{user="user1"} 50565120"#,
        r#"cortex_ingester_tsdb_storage_blocks_bytes{user="user2"} 351383552"#,
        r#"cortex_ingester_tsdb_storage_blocks_bytes{user="user3"} 4091904"#,
        "cortex_ingester_tsdb_time_retentions_total 33",
        r#"cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds{user="user1"} 1234"#,
        r#"cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds{user="user2"} 1234"#,
        r#"cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds{user="user3"} 1234"#,
        "cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total 9",
        r#"cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage{user="user1"} 1"#,
        r#"cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage{user="user2"} 1"#,
        r#"cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage{user="user3"} 1"#,
        r#"cortex_ingester_tsdb_exemplar_exemplars_appended_total{user="user1"} 100"#,
        r#"cortex_ingester_tsdb_exemplar_exemplars_appended_total{user="user2"} 100"#,
        r#"cortex_ingester_tsdb_exemplar_exemplars_appended_total{user="user3"} 100"#,
        r#"cortex_ingester_tsdb_out_of_order_samples_appended_total{user="user1"} 3"#,
        r#"cortex_ingester_tsdb_out_of_order_samples_appended_total{user="user2"} 3"#,
        r#"cortex_ingester_tsdb_out_of_order_samples_appended_total{user="user3"} 3"#,
        "cortex_ingester_tsdb_exemplar_exemplars_in_storage 30",
        r#"cortex_ingester_tsdb_wal_replay_unknown_refs_total{user="user1",type="series"} 12345"#,
        r#"cortex_ingester_tsdb_wal_replay_unknown_refs_total{user="user1",type="samples"} 24690"#,
        r#"cortex_ingester_tsdb_wal_replay_unknown_refs_total{user="user2",type="series"} 85787"#,
        r#"cortex_ingester_tsdb_wal_replay_unknown_refs_total{user="user2",type="samples"} 171574"#,
        r#"cortex_ingester_tsdb_wal_replay_unknown_refs_total{user="user3",type="series"} 999"#,
        r#"cortex_ingester_tsdb_wal_replay_unknown_refs_total{user="user3",type="samples"} 1998"#,
        r#"cortex_ingester_tsdb_wbl_replay_unknown_refs_total{user="user1",type="exemplars"} 12345"#,
        r#"cortex_ingester_tsdb_wbl_replay_unknown_refs_total{user="user2",type="exemplars"} 85787"#,
        r#"cortex_ingester_tsdb_wbl_replay_unknown_refs_total{user="user3",type="exemplars"} 999"#,
    ];

    for line in expected_lines {
        assert!(
            text.contains(line),
            "missing expected metric line {line:?} in:\n{text}"
        );
    }
}

// Upstream: pkg/storage/tsdb/tsdb_metrics_test.go::TestTSDBMetricsWithRemoval
#[test]
fn test_tsdb_metrics_with_removal() {
    let main_reg = Registry::new();
    let tsdb_metrics = TSDBMetrics::new("cortex_ingester_", Some(&main_reg));

    tsdb_metrics.set_registry_for_tenant("user1", populate_tsdb_metrics(12345.0));
    tsdb_metrics.set_registry_for_tenant("user2", populate_tsdb_metrics(85787.0));
    tsdb_metrics.set_registry_for_tenant("user3", populate_tsdb_metrics(999.0));
    tsdb_metrics.remove_registry_for_tenant("user3");

    let text = TextEncoder::new().encode_to_string(&main_reg.gather()).unwrap();

    let expected_lines = [
        // Archived counters/histograms/summaries retain user3's values:
        "cortex_ingester_tsdb_compactions_total 693917",
        "cortex_ingester_tsdb_compaction_duration_seconds_sum 27",
        "cortex_ingester_tsdb_compaction_duration_seconds_count 3",
        "cortex_ingester_tsdb_wal_fsync_duration_seconds_sum 30",
        "cortex_ingester_tsdb_wal_fsync_duration_seconds_count 3",
        "cortex_ingester_tsdb_wal_page_flushes_total 1090441",
        "cortex_ingester_tsdb_sample_out_of_order_delta_seconds_bucket{le=\"86400\"} 1",
        "cortex_ingester_tsdb_sample_out_of_order_delta_seconds_count 3",
        "cortex_ingester_tsdb_sample_out_of_order_delta_seconds_sum 693917",
        "cortex_ingester_tsdb_reloads_total 30",
        "cortex_ingester_tsdb_reloads_failures_total 21",
        "cortex_ingester_tsdb_time_retentions_total 33",
        "cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total 9",
        // Gauges exclude user3:
        "cortex_ingester_memory_series 392528",
        "cortex_ingester_tsdb_head_active_appenders 1962640",
        "cortex_ingester_tsdb_head_chunks 2158904",
        "cortex_ingester_tsdb_head_chunks_max_mmapped 2659397",
        "cortex_ingester_tsdb_blocks_loaded 10",
        "cortex_ingester_tsdb_exemplar_exemplars_in_storage 20",
        // Per-tenant metrics exclude user3:
        r#"cortex_ingester_memory_series_created_total{user="user1"} 61725"#,
        r#"cortex_ingester_memory_series_created_total{user="user2"} 428935"#,
    ];

    for line in expected_lines {
        assert!(
            text.contains(line),
            "missing expected metric line {line:?} in:\n{text}"
        );
    }

    // user3 must NOT appear in any per-tenant metric line.
    assert!(!text.contains(r#"user="user3""#), "user3 should be removed from per-tenant metrics:\n{text}");
}

// ===========================================================================
// pkg/storage/tsdb/users_scanner_test.go
// ===========================================================================

// Upstream: pkg/storage/tsdb/users_scanner_test.go::TestUsersScanner_ScanUsers_ShouldReturnedOwnedUsersOnly
#[tokio::test]
async fn test_users_scanner_scan_users_should_returned_owned_users_only() {
    let bkt = BucketClient::new_in_memory("test");
    for u in ["user-1", "user-2", "user-3", "user-4"] {
        bkt.upload(&format!("{u}/01EQK4QKFHVSZYVJ908Y7HH9E0/meta.json"), Bytes::from_static(b"{}"))
            .await
            .unwrap();
    }
    bkt.upload(
        &format!("user-3/{TENANT_DELETION_MARK_PATH}"),
        Bytes::from_static(b"{}"),
    )
    .await
    .unwrap();

    let scanner = UsersScanner::new(bkt, |uid| Ok(uid == "user-1" || uid == "user-3"));
    let (actual, deleted) = scanner.scan_users().await.unwrap();
    assert_eq!(actual, vec!["user-1".to_string()]);
    assert_eq!(deleted, vec!["user-3".to_string()]);
}

#[derive(Debug)]
struct FailingHeadStore {
    inner: InMemory,
    fail_head_path: String,
}

impl std::fmt::Display for FailingHeadStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FailingHeadStore")
    }
}

impl ObjectStore for FailingHeadStore {
    fn put_opts<'life0, 'life1, 'async_trait>(
        &'life0 self,
        location: &'life1 ObjPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = object_store::Result<PutResult>> + Send + 'async_trait>,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        let location = location.clone();
        Box::pin(async move { self.inner.put_opts(&location, payload, opts).await })
    }
    fn put_multipart_opts<'life0, 'life1, 'async_trait>(
        &'life0 self,
        location: &'life1 ObjPath,
        opts: PutMultipartOpts,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = object_store::Result<Box<dyn MultipartUpload>>>
                + Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        let location = location.clone();
        Box::pin(async move { self.inner.put_multipart_opts(&location, opts).await })
    }
    fn get_opts<'life0, 'life1, 'async_trait>(
        &'life0 self,
        location: &'life1 ObjPath,
        options: GetOptions,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = object_store::Result<GetResult>> + Send + 'async_trait>,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        let location = location.clone();
        Box::pin(async move { self.inner.get_opts(&location, options).await })
    }
    fn head<'life0, 'life1, 'async_trait>(
        &'life0 self,
        location: &'life1 ObjPath,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = object_store::Result<ObjectMeta>> + Send + 'async_trait>,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        let loc_str = location.to_string();
        let location = location.clone();
        Box::pin(async move {
            if loc_str == self.fail_head_path {
                return Err(object_store::Error::Generic {
                    store: "FailingHeadStore",
                    source: "fail".into(),
                });
            }
            self.inner.head(&location).await
        })
    }
    fn delete<'life0, 'life1, 'async_trait>(
        &'life0 self,
        location: &'life1 ObjPath,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = object_store::Result<()>> + Send + 'async_trait>,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        let location = location.clone();
        Box::pin(async move { self.inner.delete(&location).await })
    }
    fn list(&self, prefix: Option<&ObjPath>) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }
    fn list_with_delimiter<'life0, 'life1, 'async_trait>(
        &'life0 self,
        prefix: Option<&'life1 ObjPath>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = object_store::Result<ListResult>> + Send + 'async_trait>,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        let prefix = prefix.cloned();
        Box::pin(async move { self.inner.list_with_delimiter(prefix.as_ref()).await })
    }
    fn copy<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        from: &'life1 ObjPath,
        to: &'life2 ObjPath,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = object_store::Result<()>> + Send + 'async_trait>,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        let from = from.clone();
        let to = to.clone();
        Box::pin(async move { self.inner.copy(&from, &to).await })
    }
    fn copy_if_not_exists<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        from: &'life1 ObjPath,
        to: &'life2 ObjPath,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = object_store::Result<()>> + Send + 'async_trait>,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        let from = from.clone();
        let to = to.clone();
        Box::pin(async move { self.inner.copy_if_not_exists(&from, &to).await })
    }
}

// Upstream: pkg/storage/tsdb/users_scanner_test.go::TestUsersScanner_ScanUsers_ShouldReturnUsersForWhichOwnerCheckOrTenantDeletionCheckFailed
#[tokio::test]
async fn test_users_scanner_scan_users_should_return_users_for_which_owner_or_deletion_check_failed() {
    let store = Arc::new(FailingHeadStore {
        inner: InMemory::new(),
        fail_head_path: format!("user-2/{TENANT_DELETION_MARK_PATH}"),
    });
    let bkt = BucketClient::from_object_store(store, "test", "MEM");
    for u in ["user-1", "user-2"] {
        bkt.upload(&format!("{u}/01EQK4QKFHVSZYVJ908Y7HH9E0/meta.json"), Bytes::from_static(b"{}"))
            .await
            .unwrap();
    }

    let scanner = UsersScanner::new(bkt, |_| Err("failed to check if user is owned".to_string()));
    let (actual, deleted) = scanner.scan_users().await.unwrap();
    assert_eq!(actual, vec!["user-1".to_string(), "user-2".to_string()]);
    assert!(deleted.is_empty());
}

// Upstream: pkg/storage/tsdb/users_scanner_test.go::TestUsersScanner_ScanUsers_ShouldNotReturnPrefixedUsedByMimirInternals
#[tokio::test]
async fn test_users_scanner_scan_users_should_not_return_prefixed_used_by_mimir_internals() {
    let bkt = BucketClient::new_in_memory("test");
    bkt.upload("user-1/01EQK4QKFHVSZYVJ908Y7HH9E0/meta.json", Bytes::from_static(b"{}"))
        .await
        .unwrap();
    bkt.upload("user-2/01EQK4QKFHVSZYVJ908Y7HH9E0/meta.json", Bytes::from_static(b"{}"))
        .await
        .unwrap();
    bkt.upload(
        &format!("{MIMIR_INTERNALS_PREFIX}/bucket-index.json.gz"),
        Bytes::from_static(b"{}"),
    )
    .await
    .unwrap();

    let scanner = UsersScanner::new(bkt, all_users);
    let (actual, _) = scanner.scan_users().await.unwrap();
    assert_eq!(actual, vec!["user-1".to_string(), "user-2".to_string()]);
}

// ===========================================================================
// pkg/storage/tsdb/util_test.go
// ===========================================================================

// Upstream: pkg/storage/tsdb/util_test.go::TestHashBlockID
#[test]
fn test_hash_block_id() {
    let u10_a = Ulid::from_parts(10, 0);
    let u10_b = Ulid::from_parts(10, 0);
    let u20 = Ulid::from_parts(20, 0);
    let u_rand_a = Ulid::from_parts(10, 123456789);
    let u_rand_b = Ulid::from_parts(10, 987654321);

    assert_eq!(hash_block_id(u10_a), hash_block_id(u10_b));
    assert_ne!(hash_block_id(u10_a), hash_block_id(u20));
    assert_ne!(hash_block_id(u_rand_a), hash_block_id(u_rand_b));
}

// ===========================================================================
// pkg/storage/tsdb/block/meta_test.go
// ===========================================================================

// Upstream: pkg/storage/tsdb/block/meta_test.go::TestMeta_IsOutOfOrder
#[test]
fn test_meta_is_out_of_order() {
    // regular block
    let regular = BlockMeta {
        ulid: Ulid::from_parts(1, 0),
        min_time: 1000,
        max_time: 2000,
        stats: BlockStats::default(),
        compaction: BlockMetaCompaction::default(),
        version: TSDB_VERSION_1,
        out_of_order: false,
        thanos: ThanosMeta::default(),
    };
    assert!(!regular.is_out_of_order());

    // out of order from compaction hint
    let mut from_hint = regular.clone();
    from_hint.compaction.set_out_of_order();
    assert!(from_hint.is_out_of_order());

    // out of order from external label
    let mut from_label = regular.clone();
    from_label.thanos.version = 1;
    from_label.thanos.labels.insert(
        OUT_OF_ORDER_EXTERNAL_LABEL.to_string(),
        OUT_OF_ORDER_EXTERNAL_LABEL_VALUE.to_string(),
    );
    assert!(from_label.is_out_of_order());
}

// ===========================================================================
// pkg/storage/tsdb/block/block_test.go
// ===========================================================================

fn create_test_block_dir(parent: &Path, id: Ulid, ext_labels: &[(&str, &str)]) {
    let block_dir = parent.join(id.to_string());
    fs::create_dir_all(block_dir.join(CHUNKS_DIRNAME)).unwrap();
    let mut thanos = ThanosMeta {
        version: 1,
        source: SourceType::Test,
        ..Default::default()
    };
    for &(k, v) in ext_labels {
        thanos.labels.insert(k.to_string(), v.to_string());
    }
    let meta = BlockMeta {
        ulid: id,
        min_time: 0,
        max_time: 1000,
        stats: BlockStats {
            num_samples: 100,
            num_series: 5,
            num_chunks: 5,
            ..Default::default()
        },
        compaction: BlockMetaCompaction {
            level: 1,
            ..Default::default()
        },
        version: TSDB_VERSION_1,
        out_of_order: false,
        thanos,
    };
    meta.write_to_dir(&block_dir).unwrap();
    fs::write(block_dir.join(INDEX_FILENAME), b"test-index-data").unwrap();
    fs::write(block_dir.join(CHUNKS_DIRNAME).join("000001"), b"test-chunk-data").unwrap();
}

// Upstream: pkg/storage/tsdb/block/block_test.go::TestIsBlockDir
#[test]
fn test_is_block_dir() {
    let u1 = Ulid::from_parts(1, 0);
    let u2 = Ulid::from_parts(2, 0);
    let u3 = Ulid::from_parts(3, 0);
    let u4 = Ulid::from_parts(4, 0);

    let cases: Vec<(String, Ulid, bool)> = vec![
        (String::new(), Ulid::nil(), false),
        ("something".to_string(), Ulid::nil(), false),
        (u1.to_string(), u1, true),
        (format!("/{u2}"), u2, true),
        (format!("some/path/{u3}"), u3, true),
        (format!("{u4}/something"), Ulid::nil(), false),
    ];

    for (input, exp_id, exp_ok) in &cases {
        let (id, ok) = is_block_dir(input);
        assert_eq!(ok, *exp_ok, "input: {input}");
        assert_eq!(id, *exp_id, "input: {input}");
    }
}

// Upstream: pkg/storage/tsdb/block/block_test.go::TestDelete
#[tokio::test]
async fn test_delete() {
    let tmp = tempfile::tempdir().unwrap();
    let bkt = BucketClient::new_in_memory("test");

    {
        let b1 = Ulid::from_parts(100, 1);
        create_test_block_dir(tmp.path(), b1, &[("ext1", "val1")]);
        upload_block(&bkt, &tmp.path().join(b1.to_string()), None)
            .await
            .unwrap();
        assert_eq!(bkt.list_all("").await.unwrap().len(), 3);

        let marked = Counter::new("test_marked", "test").unwrap();
        mark_for_deletion(&bkt, b1, "", Some(&marked)).await.unwrap();
        assert_eq!(marked.get(), 1.0);

        delete_block(&bkt, b1).await.unwrap();
        assert_eq!(bkt.list_all("").await.unwrap().len(), 0);
    }

    {
        let b2 = Ulid::from_parts(200, 2);
        create_test_block_dir(tmp.path(), b2, &[("ext1", "val1")]);
        upload_block(&bkt, &tmp.path().join(b2.to_string()), None)
            .await
            .unwrap();
        assert_eq!(bkt.list_all("").await.unwrap().len(), 3);

        // Remove meta.json and check if delete_block can still clean up remaining files.
        bkt.delete(&format!("{b2}/{META_FILENAME}")).await.unwrap();
        delete_block(&bkt, b2).await.unwrap();
        assert_eq!(bkt.list_all("").await.unwrap().len(), 0);
    }
}

// Upstream: pkg/storage/tsdb/block/block_test.go::TestUpload
#[tokio::test]
async fn test_upload() {
    let tmp = tempfile::tempdir().unwrap();
    let bkt = BucketClient::new_in_memory("test");

    let b1 = Ulid::from_parts(100, 1);
    create_test_block_dir(tmp.path(), b1, &[("ext1", "val1")]);
    fs::create_dir_all(tmp.path().join("test").join(b1.to_string())).unwrap();

    // wrong dir
    let err = upload_block(&bkt, &tmp.path().join("not-existing"), None)
        .await
        .unwrap_err();
    assert!(matches!(err, ShipperError::Io(_)));

    // wrong existing dir (not a block)
    let err = upload_block(&bkt, &tmp.path().join("test"), None)
        .await
        .unwrap_err();
    assert!(matches!(err, ShipperError::InvalidBlockDir(_)));

    // empty block dir (missing meta.json)
    let err = upload_block(&bkt, &tmp.path().join("test").join(b1.to_string()), None)
        .await
        .unwrap_err();
    assert!(matches!(err, ShipperError::Io(_)));

    // missing chunks
    fs::copy(
        tmp.path().join(b1.to_string()).join(META_FILENAME),
        tmp.path().join("test").join(b1.to_string()).join(META_FILENAME),
    )
    .unwrap();
    let err = upload_block(&bkt, &tmp.path().join("test").join(b1.to_string()), None)
        .await
        .unwrap_err();
    assert!(matches!(err, ShipperError::Io(_)));

    // missing index file
    fs::create_dir_all(
        tmp.path()
            .join("test")
            .join(b1.to_string())
            .join(CHUNKS_DIRNAME),
    )
    .unwrap();
    fs::copy(
        tmp.path()
            .join(b1.to_string())
            .join(CHUNKS_DIRNAME)
            .join("000001"),
        tmp.path()
            .join("test")
            .join(b1.to_string())
            .join(CHUNKS_DIRNAME)
            .join("000001"),
    )
    .unwrap();
    let err = upload_block(&bkt, &tmp.path().join("test").join(b1.to_string()), None)
        .await
        .unwrap_err();
    assert!(matches!(err, ShipperError::Io(_)));

    // missing meta.json file
    fs::copy(
        tmp.path().join(b1.to_string()).join(INDEX_FILENAME),
        tmp.path()
            .join("test")
            .join(b1.to_string())
            .join(INDEX_FILENAME),
    )
    .unwrap();
    fs::remove_file(tmp.path().join("test").join(b1.to_string()).join(META_FILENAME)).unwrap();
    let err = upload_block(&bkt, &tmp.path().join("test").join(b1.to_string()), None)
        .await
        .unwrap_err();
    assert!(matches!(err, ShipperError::Io(_)));

    // missing meta.json file, but valid metadata supplied as argument
    let supplied = BlockMeta::read_from_dir(tmp.path().join(b1.to_string())).unwrap();
    let err = upload_block(
        &bkt,
        &tmp.path().join("test").join(b1.to_string()),
        Some(supplied),
    )
    .await
    .unwrap_err();
    assert!(matches!(err, ShipperError::Io(_)));

    // restore meta.json and test full block
    fs::copy(
        tmp.path().join(b1.to_string()).join(META_FILENAME),
        tmp.path().join("test").join(b1.to_string()).join(META_FILENAME),
    )
    .unwrap();
    upload_block(&bkt, &tmp.path().join("test").join(b1.to_string()), None)
        .await
        .unwrap();
    assert_eq!(bkt.list_all("").await.unwrap().len(), 3);
    let chunk_size = fs::metadata(
        tmp.path()
            .join("test")
            .join(b1.to_string())
            .join(CHUNKS_DIRNAME)
            .join("000001"),
    )
    .unwrap()
    .len() as i64;
    let index_size = fs::metadata(
        tmp.path()
            .join("test")
            .join(b1.to_string())
            .join(INDEX_FILENAME),
    )
    .unwrap()
    .len() as i64;
    let orig_meta =
        BlockMeta::read_from_dir(tmp.path().join("test").join(b1.to_string())).unwrap();
    let mut uploaded_meta = download_meta(&bkt, b1).await.unwrap();
    assert_eq!(uploaded_meta.thanos.files.len(), 3);
    assert_eq!(uploaded_meta.thanos.files[0].rel_path, "chunks/000001");
    assert_eq!(uploaded_meta.thanos.files[0].size_bytes, chunk_size);
    assert_eq!(uploaded_meta.thanos.files[1].rel_path, "index");
    assert_eq!(uploaded_meta.thanos.files[1].size_bytes, index_size);
    assert_eq!(uploaded_meta.thanos.files[2].rel_path, "meta.json");
    assert_eq!(uploaded_meta.thanos.files[2].size_bytes, 0);
    uploaded_meta.thanos.files.clear();
    assert_eq!(uploaded_meta, orig_meta);

    // upload is idempotent
    upload_block(&bkt, &tmp.path().join("test").join(b1.to_string()), None)
        .await
        .unwrap();
    assert_eq!(bkt.list_all("").await.unwrap().len(), 3);

    // upload with no external labels works just fine
    let b2 = Ulid::from_parts(200, 2);
    create_test_block_dir(tmp.path(), b2, &[]);
    upload_block(&bkt, &tmp.path().join(b2.to_string()), None)
        .await
        .unwrap();
    assert_eq!(bkt.list_all("").await.unwrap().len(), 6);
    let orig2 = BlockMeta::read_from_dir(tmp.path().join(b2.to_string())).unwrap();
    let mut up2 = download_meta(&bkt, b2).await.unwrap();
    up2.thanos.files.clear();
    assert_eq!(up2, orig2);

    // upload with supplied meta.json does not mutate local meta.json
    let b3 = Ulid::from_parts(300, 3);
    create_test_block_dir(tmp.path(), b3, &[]);
    let mut updated = BlockMeta::read_from_dir(tmp.path().join(b3.to_string())).unwrap();
    assert!(updated.thanos.labels.is_empty());
    assert_eq!(updated.thanos.source, SourceType::Test);
    updated
        .thanos
        .labels
        .insert("a".to_string(), "b".to_string());
    updated
        .thanos
        .labels
        .insert("c".to_string(), "d".to_string());
    updated.thanos.source = SourceType::Receive;
    upload_block(
        &bkt,
        &tmp.path().join(b3.to_string()),
        Some(updated.clone()),
    )
    .await
    .unwrap();
    let local3 = BlockMeta::read_from_dir(tmp.path().join(b3.to_string())).unwrap();
    assert!(local3.thanos.labels.is_empty());
    assert_eq!(local3.thanos.source, SourceType::Test);
    let bucket3 = download_meta(&bkt, b3).await.unwrap();
    assert_eq!(bucket3.thanos.labels, updated.thanos.labels);
    assert_eq!(bucket3.thanos.source, SourceType::Receive);
}

// Upstream: pkg/storage/tsdb/block/block_test.go::TestMarkForDeletion
#[tokio::test]
async fn test_mark_for_deletion() {
    let tmp = tempfile::tempdir().unwrap();

    // Case 1: block marked for deletion
    {
        let bkt = BucketClient::new_in_memory("test");
        let id = Ulid::from_parts(100, 1);
        create_test_block_dir(tmp.path(), id, &[("ext1", "val1")]);
        upload_block(&bkt, &tmp.path().join(id.to_string()), None)
            .await
            .unwrap();
        let c = Counter::new("c1", "c1").unwrap();
        mark_for_deletion(&bkt, id, "", Some(&c)).await.unwrap();
        assert_eq!(c.get(), 1.0);
    }

    // Case 2: block with deletion mark already, no metric increment
    {
        let bkt = BucketClient::new_in_memory("test");
        let id = Ulid::from_parts(200, 2);
        create_test_block_dir(tmp.path(), id, &[("ext1", "val1")]);
        let dm = serde_json::to_vec(&DeletionMark {
            id,
            version: DELETION_MARK_VERSION_1,
            details: String::new(),
            deletion_time: 1_700_000_000,
        })
        .unwrap();
        bkt.upload(&format!("{id}/{DELETION_MARK_FILENAME}"), Bytes::from(dm))
            .await
            .unwrap();
        upload_block(&bkt, &tmp.path().join(id.to_string()), None)
            .await
            .unwrap();
        let c = Counter::new("c2", "c2").unwrap();
        mark_for_deletion(&bkt, id, "", Some(&c)).await.unwrap();
        assert_eq!(c.get(), 0.0);
    }
}

// Upstream: pkg/storage/tsdb/block/block_test.go::TestMarkForNoCompact
#[tokio::test]
async fn test_mark_for_no_compact() {
    let tmp = tempfile::tempdir().unwrap();

    // Case 1: block marked
    {
        let bkt = BucketClient::new_in_memory("test");
        let id = Ulid::from_parts(100, 1);
        create_test_block_dir(tmp.path(), id, &[("ext1", "val1")]);
        upload_block(&bkt, &tmp.path().join(id.to_string()), None)
            .await
            .unwrap();
        let c = Counter::new("nc1", "nc1").unwrap();
        mark_for_no_compact(&bkt, id, NoCompactReason::Manual, "", &[&c])
            .await
            .unwrap();
        assert_eq!(c.get(), 1.0);
    }

    // Case 2: already marked, no metric increment
    {
        let bkt = BucketClient::new_in_memory("test");
        let id = Ulid::from_parts(200, 2);
        create_test_block_dir(tmp.path(), id, &[("ext1", "val1")]);
        let nm = serde_json::to_vec(&NoCompactMark {
            id,
            version: NO_COMPACT_MARK_VERSION_1,
            details: String::new(),
            no_compact_time: 1_700_000_000,
            reason: NoCompactReason::Manual,
        })
        .unwrap();
        bkt.upload(&format!("{id}/{NO_COMPACT_MARK_FILENAME}"), Bytes::from(nm))
            .await
            .unwrap();
        upload_block(&bkt, &tmp.path().join(id.to_string()), None)
            .await
            .unwrap();
        let c = Counter::new("nc2", "nc2").unwrap();
        mark_for_no_compact(&bkt, id, NoCompactReason::Manual, "", &[&c])
            .await
            .unwrap();
        assert_eq!(c.get(), 0.0);
    }
}

// Upstream: pkg/storage/tsdb/block/block_test.go::TestUnMarkForNoCompact
#[tokio::test]
async fn test_unmark_for_no_compact() {
    let tmp = tempfile::tempdir().unwrap();

    // Case 1: unmark existing block should succeed
    {
        let bkt = BucketClient::new_in_memory("test");
        let id = Ulid::from_parts(100, 1);
        create_test_block_dir(tmp.path(), id, &[("ext1", "val1")]);
        upload_block(&bkt, &tmp.path().join(id.to_string()), None)
            .await
            .unwrap();
        mark_for_no_compact(&bkt, id, NoCompactReason::Manual, "", &[])
            .await
            .unwrap();
        assert!(bkt
            .exists(&format!("{id}/{NO_COMPACT_MARK_FILENAME}"))
            .await
            .unwrap());
        delete_no_compact_marker(&bkt, id).await.unwrap();
        assert!(!bkt
            .exists(&format!("{id}/{NO_COMPACT_MARK_FILENAME}"))
            .await
            .unwrap());
    }

    // Case 2: unmark non-existing block should fail
    {
        let bkt = BucketClient::new_in_memory("test");
        let id = Ulid::from_parts(200, 2);
        let err = delete_no_compact_marker(&bkt, id).await.unwrap_err();
        assert!(matches!(err, BucketError::NotFound(_)));
    }
}

#[derive(Debug)]
struct SuffixErrStore {
    inner: InMemory,
    fail_suffix: String,
    write_before_fail: bool,
    fail_count: AtomicUsize,
}

impl std::fmt::Display for SuffixErrStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SuffixErrStore")
    }
}

impl ObjectStore for SuffixErrStore {
    fn put_opts<'life0, 'life1, 'async_trait>(
        &'life0 self,
        location: &'life1 ObjPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = object_store::Result<PutResult>> + Send + 'async_trait>,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        let loc_str = location.to_string();
        let location = location.clone();
        Box::pin(async move {
            if self.write_before_fail {
                let res = self.inner.put_opts(&location, payload, opts).await?;
                if loc_str.ends_with(&self.fail_suffix)
                    && self.fail_count.fetch_sub(1, Ordering::SeqCst) > 0
                {
                    return Err(object_store::Error::Generic {
                        store: "SuffixErrStore",
                        source: "failed".into(),
                    });
                }
                Ok(res)
            } else {
                if loc_str.ends_with(&self.fail_suffix)
                    && self.fail_count.fetch_sub(1, Ordering::SeqCst) > 0
                {
                    return Err(object_store::Error::Generic {
                        store: "SuffixErrStore",
                        source: "failed".into(),
                    });
                }
                self.inner.put_opts(&location, payload, opts).await
            }
        })
    }
    fn put_multipart_opts<'life0, 'life1, 'async_trait>(
        &'life0 self,
        location: &'life1 ObjPath,
        opts: PutMultipartOpts,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = object_store::Result<Box<dyn MultipartUpload>>>
                + Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        let location = location.clone();
        Box::pin(async move { self.inner.put_multipart_opts(&location, opts).await })
    }
    fn get_opts<'life0, 'life1, 'async_trait>(
        &'life0 self,
        location: &'life1 ObjPath,
        options: GetOptions,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = object_store::Result<GetResult>> + Send + 'async_trait>,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        let location = location.clone();
        Box::pin(async move { self.inner.get_opts(&location, options).await })
    }
    fn delete<'life0, 'life1, 'async_trait>(
        &'life0 self,
        location: &'life1 ObjPath,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = object_store::Result<()>> + Send + 'async_trait>,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        let location = location.clone();
        Box::pin(async move { self.inner.delete(&location).await })
    }
    fn list(&self, prefix: Option<&ObjPath>) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }
    fn list_with_delimiter<'life0, 'life1, 'async_trait>(
        &'life0 self,
        prefix: Option<&'life1 ObjPath>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = object_store::Result<ListResult>> + Send + 'async_trait>,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        let prefix = prefix.cloned();
        Box::pin(async move { self.inner.list_with_delimiter(prefix.as_ref()).await })
    }
    fn copy<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        from: &'life1 ObjPath,
        to: &'life2 ObjPath,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = object_store::Result<()>> + Send + 'async_trait>,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        let from = from.clone();
        let to = to.clone();
        Box::pin(async move { self.inner.copy(&from, &to).await })
    }
    fn copy_if_not_exists<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        from: &'life1 ObjPath,
        to: &'life2 ObjPath,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = object_store::Result<()>> + Send + 'async_trait>,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        let from = from.clone();
        let to = to.clone();
        Box::pin(async move { self.inner.copy_if_not_exists(&from, &to).await })
    }
}

// Upstream: pkg/storage/tsdb/block/block_test.go::TestUploadCleanup
#[tokio::test]
async fn test_upload_cleanup() {
    let tmp = tempfile::tempdir().unwrap();
    let b1 = Ulid::from_parts(100, 1);
    create_test_block_dir(tmp.path(), b1, &[("ext1", "val1")]);

    // If upload of index fails, partial block files are deleted.
    {
        let store = Arc::new(SuffixErrStore {
            inner: InMemory::new(),
            fail_suffix: "/index".to_string(),
            write_before_fail: true,
            fail_count: AtomicUsize::new(1),
        });
        let bkt = BucketClient::from_object_store(store, "test", "MEM");
        let err = upload_block(&bkt, &tmp.path().join(b1.to_string()), None)
            .await
            .unwrap_err();
        assert!(matches!(err, ShipperError::Upload { file_type: "index", .. }));
        assert_eq!(bkt.list_all("").await.unwrap().len(), 0);
    }

    // If upload of meta.json fails, nothing is cleaned up.
    {
        let store = Arc::new(SuffixErrStore {
            inner: InMemory::new(),
            fail_suffix: "/meta.json".to_string(),
            write_before_fail: true,
            fail_count: AtomicUsize::new(1),
        });
        let bkt = BucketClient::from_object_store(store, "test", "MEM");
        let err = upload_block(&bkt, &tmp.path().join(b1.to_string()), None)
            .await
            .unwrap_err();
        assert!(matches!(err, ShipperError::Upload { file_type: "meta", .. }));
        assert_eq!(bkt.list_all("").await.unwrap().len(), 3);
    }
}

// Upstream: pkg/storage/tsdb/block/block_test.go::TestUploadError
#[tokio::test]
async fn test_upload_error() {
    let tmp = tempfile::tempdir().unwrap();
    let block_id = Ulid::from_parts(100, 1);
    create_test_block_dir(tmp.path(), block_id, &[]);

    let store = Arc::new(SuffixErrStore {
        inner: InMemory::new(),
        fail_suffix: format!("{block_id}/{META_FILENAME}"),
        write_before_fail: false,
        fail_count: AtomicUsize::new(1),
    });
    let bkt = BucketClient::from_object_store(store, "test", "MEM");

    let err = upload_block(&bkt, &tmp.path().join(block_id.to_string()), None)
        .await
        .unwrap_err();
    assert!(matches!(err, ShipperError::Upload { file_type: "meta", .. }));
}
