//! Ported from github.com/grafana/mimir/pkg/ingester/ingester_test.go (lines 5376..8985).

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::{to_bytes, Body};
use axum::http::{Method, Request as HttpRequest, StatusCode};
use mimir_common::services::State;
use mimir_common::tenant::LOWER_ORG_ID_HEADER_NAME;
use mimir_config::{
    BlocksStorageConfig, GoDuration, IngestStorageConfig, IngesterConfig, LimitsConfig,
    ModelDuration, Overrides,
};
use mimir_ingester::ingester::{
    get_open_tsdbs_concurrency_config, UtilizationBasedLimiter, COMPACTION_IDLE_TIMEOUT_JITTER,
};
use mimir_ingester::query::QUERY_STREAM_BATCH_SIZE;
use mimir_ingester::user_tsdb::{BlockShipper, TsdbCloseCheckResult, TsdbState};
use mimir_ingester::{ingester_router, Ingester, IngesterError, IngesterService};
use mimir_proto::cortexpb;
use mimir_proto::ingesterpb;
use mimir_proto::ingesterpb::ingester_client::IngesterClient;
use mimir_ring::kv::inmemory::InMemoryKv;
use mimir_ring::kv::KvClient;
use mimir_ring::lifecycler::{Lifecycler, LifecyclerConfig};
use mimir_ring::ring::model::ring_desc_codec;
use mimir_ring::ring::replication_strategy::DefaultReplicationStrategy;
use mimir_ring::ring::{Ring, RingSettings};
use mimir_storage::{
    write_shipper_meta_file, write_tenant_deletion_mark, BucketClient, ModelTime, ShipperError,
    ShipperMeta, TenantDeletionMark, SHIPPER_META_VERSION_1,
};
use mimir_tsdb::chunkenc::{
    CounterResetHeader, CounterResetHint, EncodedChunk, Encoding, FloatHistogram, Histogram,
    HistogramSpan,
};
use mimir_tsdb::compact::{decode_chunk_samples, SampleValue};
use mimir_tsdb::db::{Tsdb, TsdbOptions};
use mimir_tsdb::labels::Labels;
use prometheus::proto::MetricType;
use prometheus::Registry;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::metadata::{MetadataMap, MetadataValue};
use tonic::{Code, Request};
use tower::ServiceExt;
use ulid::Ulid;

const USER_ID: &str = "1";
const METRIC_NAME_LABEL: &str = "__name__";
const SHARD_LABEL: &str = "__query_shard__";
const TENANT_PARAM: &str = "tenant";

fn default_ingester_test_config(tmp: &Path) -> (IngesterConfig, BlocksStorageConfig) {
    let mut cfg = IngesterConfig::default();
    cfg.active_series.active_series_metrics_enabled = true;
    cfg.ring.instance_id = "localhost".to_string();

    let mut blocks_cfg = BlocksStorageConfig::default();
    blocks_cfg.tsdb.dir = tmp.join("tsdb").to_string_lossy().into_owned();
    blocks_cfg.bucket.backend_cfg.backend = "filesystem".to_string();
    blocks_cfg.bucket.backend_cfg.filesystem.dir =
        tmp.join("bucket").to_string_lossy().into_owned();
    blocks_cfg.tsdb.head_compaction_interval_jitter_enabled = false;
    std::fs::create_dir_all(&blocks_cfg.tsdb.dir).unwrap();
    std::fs::create_dir_all(&blocks_cfg.bucket.backend_cfg.filesystem.dir).unwrap();
    (cfg, blocks_cfg)
}

fn default_limits_test_config() -> LimitsConfig {
    LimitsConfig {
        native_histograms_ingestion_enabled: true,
        ..Default::default()
    }
}

async fn prepare_ingester_with_blocks_storage(
    _tmp: &Path,
    cfg: IngesterConfig,
    blocks_cfg: BlocksStorageConfig,
    overrides: Option<Overrides>,
    registry: Option<&Registry>,
) -> Arc<Ingester> {
    let ov = overrides.unwrap_or_else(|| Overrides::new(default_limits_test_config()));
    let ing = Ingester::new(
        cfg,
        blocks_cfg,
        IngestStorageConfig::default(),
        ov,
        None,
        None,
        None,
        None,
        None,
        registry,
    )
    .expect("create ingester");
    ing.start().await.expect("start ingester");
    ing
}

fn md_with_org(user_id: &str) -> MetadataMap {
    let mut md = MetadataMap::new();
    md.insert(
        LOWER_ORG_ID_HEADER_NAME,
        MetadataValue::try_from(user_id).unwrap(),
    );
    md
}

fn mock_write_request(lbls: &Labels, value: f64, timestamp_ms: i64) -> cortexpb::WriteRequest {
    cortexpb::WriteRequest {
        timeseries: vec![cortexpb::TimeSeries {
            labels: lbls
                .iter()
                .map(|(k, v)| cortexpb::LabelPair::from_strs(k, v))
                .collect(),
            samples: vec![cortexpb::Sample {
                value,
                timestamp_ms,
            }],
            ..Default::default()
        }],
        source: cortexpb::write_request::SourceEnum::Api as i32,
        ..Default::default()
    }
}

fn write_request_single_series(
    lbls: &Labels,
    samples: Vec<cortexpb::Sample>,
) -> cortexpb::WriteRequest {
    cortexpb::WriteRequest {
        timeseries: vec![cortexpb::TimeSeries {
            labels: lbls
                .iter()
                .map(|(k, v)| cortexpb::LabelPair::from_strs(k, v))
                .collect(),
            samples,
            ..Default::default()
        }],
        source: cortexpb::write_request::SourceEnum::Api as i32,
        ..Default::default()
    }
}

fn generate_test_histogram_proto(ts: i64, idx: i64) -> cortexpb::Histogram {
    let spans = vec![
        cortexpb::BucketSpan {
            offset: 0,
            length: 2,
        },
        cortexpb::BucketSpan {
            offset: 1,
            length: 2,
        },
    ];
    let deltas = vec![idx + 1, 1, -1, 0];
    cortexpb::Histogram {
        count: Some(cortexpb::histogram::Count::CountInt(12 + (idx * 9) as u64)),
        zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountInt(
            2 + idx as u64,
        )),
        zero_threshold: 0.001,
        sum: 18.4 * (idx + 1) as f64,
        schema: 1,
        positive_spans: spans.clone(),
        positive_deltas: deltas.clone(),
        negative_spans: spans,
        negative_deltas: deltas,
        reset_hint: cortexpb::histogram::ResetHint::Unknown as i32,
        timestamp: ts,
        ..Default::default()
    }
}

fn generate_test_float_histogram_proto(ts: i64, idx: i64) -> cortexpb::Histogram {
    let spans = vec![
        cortexpb::BucketSpan {
            offset: 0,
            length: 2,
        },
        cortexpb::BucketSpan {
            offset: 1,
            length: 2,
        },
    ];
    let counts = vec![
        (idx + 1) as f64,
        (idx + 2) as f64,
        (idx + 1) as f64,
        (idx + 1) as f64,
    ];
    cortexpb::Histogram {
        count: Some(cortexpb::histogram::Count::CountFloat(
            12.0 + (idx * 9) as f64,
        )),
        zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountFloat(
            2.0 + idx as f64,
        )),
        zero_threshold: 0.001,
        sum: 18.4 * (idx + 1) as f64,
        schema: 1,
        positive_spans: spans.clone(),
        positive_counts: counts.clone(),
        negative_spans: spans,
        negative_counts: counts,
        reset_hint: cortexpb::histogram::ResetHint::Unknown as i32,
        timestamp: ts,
        ..Default::default()
    }
}

fn generate_test_histogram(idx: i64, hint: CounterResetHint) -> Histogram {
    let spans = vec![
        HistogramSpan {
            offset: 0,
            length: 2,
        },
        HistogramSpan {
            offset: 1,
            length: 2,
        },
    ];
    let buckets = vec![idx + 1, 1, -1, 0];
    Histogram {
        counter_reset_hint: hint,
        schema: 1,
        zero_threshold: 0.001,
        zero_count: 2 + idx as u64,
        count: 12 + (idx * 9) as u64,
        sum: 18.4 * (idx + 1) as f64,
        positive_spans: spans.clone(),
        negative_spans: spans,
        positive_buckets: buckets.clone(),
        negative_buckets: buckets,
        custom_values: Vec::new(),
    }
}

fn generate_test_float_histogram(idx: i64, hint: CounterResetHint) -> FloatHistogram {
    let spans = vec![
        HistogramSpan {
            offset: 0,
            length: 2,
        },
        HistogramSpan {
            offset: 1,
            length: 2,
        },
    ];
    let counts = vec![
        (idx + 1) as f64,
        (idx + 2) as f64,
        (idx + 1) as f64,
        (idx + 1) as f64,
    ];
    FloatHistogram {
        counter_reset_hint: hint,
        schema: 1,
        zero_threshold: 0.001,
        zero_count: 2.0 + idx as f64,
        count: 12.0 + (idx * 9) as f64,
        sum: 18.4 * (idx + 1) as f64,
        positive_spans: spans.clone(),
        negative_spans: spans,
        positive_buckets: counts.clone(),
        negative_buckets: counts,
        custom_values: Vec::new(),
    }
}

fn mock_histogram_write_request(
    lbls: &Labels,
    ts_ms: i64,
    hist_idx: i64,
    gen_float_hist: bool,
) -> cortexpb::WriteRequest {
    let h = if gen_float_hist {
        generate_test_float_histogram_proto(ts_ms, hist_idx)
    } else {
        generate_test_histogram_proto(ts_ms, hist_idx)
    };
    cortexpb::WriteRequest {
        timeseries: vec![cortexpb::TimeSeries {
            labels: lbls
                .iter()
                .map(|(k, v)| cortexpb::LabelPair::from_strs(k, v))
                .collect(),
            histograms: vec![h],
            ..Default::default()
        }],
        source: cortexpb::write_request::SourceEnum::Api as i32,
        ..Default::default()
    }
}

fn mk_matcher(ty: ingesterpb::MatchType, name: &str, value: &str) -> ingesterpb::LabelMatcher {
    ingesterpb::LabelMatcher {
        r#type: ty as i32,
        name: name.to_string(),
        value: value.to_string(),
    }
}

fn format_shard_id_label_value(shard_index: u64, shard_count: u64) -> String {
    format!("{}_of_{}", shard_index + 1, shard_count)
}

fn labels_from_pairs(pairs: &[cortexpb::LabelPair]) -> Labels {
    let p: Vec<(&str, &str)> = pairs
        .iter()
        .map(|lp| (lp.name_str().unwrap(), lp.value_str().unwrap()))
        .collect();
    Labels::from_pairs(p)
}

fn decode_wire_chunk(wire_enc: i32, data: &[u8]) -> EncodedChunk {
    let enc = match wire_enc {
        4 => Encoding::Xor,
        5 => Encoding::Histogram,
        6 => Encoding::FloatHistogram,
        other => panic!("unexpected wire encoding: {other}"),
    };
    EncodedChunk::from_bytes(enc, data.to_vec()).expect("decode chunk")
}

/// Decodes `QueryStreamResponse` sequence into `BTreeMap<Labels, Vec<(i64, f64)>>`.
fn streams_to_matrix(responses: &[ingesterpb::QueryStreamResponse]) -> BTreeMap<Labels, Vec<(i64, f64)>> {
    let mut series_labels = Vec::new();
    let mut out: BTreeMap<Labels, Vec<(i64, f64)>> = BTreeMap::new();

    for resp in responses {
        for s in &resp.streaming_series {
            series_labels.push(labels_from_pairs(&s.labels));
        }
        for sc in &resp.streaming_series_chunks {
            let lbls = series_labels[sc.series_index as usize].clone();
            let entry = out.entry(lbls).or_default();
            for c in &sc.chunks {
                let enc = decode_wire_chunk(c.encoding, &c.data);
                for sample in decode_chunk_samples(&enc).unwrap() {
                    if let SampleValue::Float(v) = sample.value {
                        entry.push((sample.t, v));
                    }
                }
            }
        }
    }
    out
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

async fn push_single_sample_with_metadata(ing: &Ingester) {
    let lbls = Labels::from_pairs([(METRIC_NAME_LABEL, "test")]);
    let mut req = mock_write_request(&lbls, 0.0, now_ms());
    req.metadata.push(cortexpb::MetricMetadata {
        r#type: cortexpb::metric_metadata::MetricType::Counter as i32,
        metric_family_name: "test".to_string(),
        help: "a help for metric".to_string(),
        unit: String::new(),
    });
    ing.push_grpc(USER_ID, &req).await.unwrap();
}

async fn push_single_sample_at_time(ing: &Ingester, ts: i64) {
    let lbls = Labels::from_pairs([(METRIC_NAME_LABEL, "test")]);
    let req = mock_write_request(&lbls, 0.0, ts);
    ing.push_grpc(USER_ID, &req).await.unwrap();
}

fn verify_compacted_head(ing: &Ingester, expected: bool) {
    let db = ing.get_tsdb(USER_ID).expect("user tsdb should exist");
    assert_eq!(expected, db.db.head().num_series() == 0);
}

fn generate_samples(sample_count: usize) -> Vec<cortexpb::Sample> {
    (0..sample_count)
        .map(|i| cortexpb::Sample {
            value: i as f64,
            timestamp_ms: i as i64,
        })
        .collect()
}

async fn start_grpc_server(
    ing: Arc<Ingester>,
) -> (
    IngesterClient<tonic::transport::Channel>,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let incoming = TcpListenerStream::new(listener);
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let svc = IngesterService::new(ing).into_server();

    let handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(svc)
            .serve_with_incoming_shutdown(incoming, async {
                let _ = rx.await;
            })
            .await
            .unwrap();
    });

    let endpoint = format!("http://{addr}");
    let client = IngesterClient::connect(endpoint)
        .await
        .unwrap()
        .max_decoding_message_size(64 * 1024 * 1024);
    (client, tx, handle)
}

fn gather_metric_value(reg: &Registry, name: &str, labels: &[(&str, &str)]) -> Option<f64> {
    for mf in reg.gather() {
        if mf.get_name() != name {
            continue;
        }
        for m in mf.get_metric() {
            let mut matched = true;
            for &(lk, lv) in labels {
                if !m
                    .get_label()
                    .iter()
                    .any(|lp| lp.get_name() == lk && lp.get_value() == lv)
                {
                    matched = false;
                    break;
                }
            }
            if matched {
                return match mf.get_field_type() {
                    MetricType::COUNTER => Some(m.get_counter().get_value()),
                    MetricType::GAUGE => Some(m.get_gauge().get_value()),
                    _ => None,
                };
            }
        }
    }
    None
}

fn gather_metric_count(reg: &Registry, name: &str) -> usize {
    for mf in reg.gather() {
        if mf.get_name() == name {
            return mf.get_metric().len();
        }
    }
    0
}

fn verify_shipper_last_successful_upload_time_metric(reg: &Registry, expected_secs: i64) {
    let v = gather_metric_value(
        reg,
        "cortex_ingester_shipper_last_successful_upload_timestamp_seconds",
        &[],
    )
    .unwrap_or(0.0);
    assert!(
        (v - expected_secs as f64).abs() <= 5.0,
        "expected upload timestamp near {expected_secs}, got {v}"
    );
}

async fn http_call(
    ing: &Arc<Ingester>,
    method: Method,
    uri: &str,
) -> (StatusCode, String) {
    let app = ingester_router(Arc::clone(ing));
    let req = HttpRequest::builder()
        .method(method)
        .uri(uri)
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let status = resp.status();
    let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    (status, String::from_utf8_lossy(&body).into_owned())
}

// ---------------------------------------------------------------------------
// Tests #9..#47 from .ref/mimir/pkg/ingester/ingester_test.go (lines 5376..8985)
// ---------------------------------------------------------------------------

// Upstream: pkg/ingester/ingester_test.go::Test_Ingester_LabelNames
#[tokio::test]
async fn test_ingester_label_names() {
    let tmp = tempfile::tempdir().unwrap();
    let (cfg, blocks_cfg) = default_ingester_test_config(tmp.path());
    let reg = Registry::new();
    let ing = prepare_ingester_with_blocks_storage(tmp.path(), cfg, blocks_cfg, None, Some(&reg)).await;

    let series = [
        (
            Labels::from_pairs([(METRIC_NAME_LABEL, "test_1"), ("status", "200"), ("route", "get_user")]),
            1.0,
            100000,
        ),
        (
            Labels::from_pairs([(METRIC_NAME_LABEL, "test_1"), ("status", "500"), ("route", "get_user")]),
            1.0,
            110000,
        ),
        (Labels::from_pairs([(METRIC_NAME_LABEL, "test_2")]), 2.0, 200000),
        (
            Labels::from_pairs([(METRIC_NAME_LABEL, "test_3"), ("status", "500")]),
            2.0,
            200000,
        ),
    ];

    for (lbls, val, ts) in &series {
        ing.push_grpc("test", &mock_write_request(lbls, *val, *ts))
            .await
            .unwrap();
    }

    let md = md_with_org("test");

    // without matchers
    {
        let res = ing
            .handle_label_names(
                &md,
                "test",
                ingesterpb::LabelNamesRequest {
                    end_timestamp_ms: i64::MAX,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let mut names = res.label_names;
        names.sort();
        assert_eq!(names, vec!["__name__", "route", "status"]);
    }

    // with matchers
    {
        let res = ing
            .handle_label_names(
                &md,
                "test",
                ingesterpb::LabelNamesRequest {
                    start_timestamp_ms: 0,
                    end_timestamp_ms: i64::MAX,
                    matchers: Some(ingesterpb::LabelMatchers {
                        matchers: vec![mk_matcher(
                            ingesterpb::MatchType::NotEqual,
                            "route",
                            "get_user",
                        )],
                    }),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let mut names = res.label_names;
        names.sort();
        assert_eq!(names, vec!["__name__", "status"]);
    }

    // without matchers, with limit = 2
    {
        let res = ing
            .handle_label_names(
                &md,
                "test",
                ingesterpb::LabelNamesRequest {
                    start_timestamp_ms: 0,
                    end_timestamp_ms: i64::MAX,
                    limit: 2,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(res.label_names, vec!["__name__", "route"]);
    }

    // without matchers, with limit set to 0
    {
        let res = ing
            .handle_label_names(
                &md,
                "test",
                ingesterpb::LabelNamesRequest {
                    start_timestamp_ms: 0,
                    end_timestamp_ms: i64::MAX,
                    limit: 0,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let mut names = res.label_names;
        names.sort();
        assert_eq!(names, vec!["__name__", "route", "status"]);
    }

    // without matchers, with limit set to same number of items returned
    {
        let res = ing
            .handle_label_names(
                &md,
                "test",
                ingesterpb::LabelNamesRequest {
                    start_timestamp_ms: 0,
                    end_timestamp_ms: i64::MAX,
                    limit: 3,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let mut names = res.label_names;
        names.sort();
        assert_eq!(names, vec!["__name__", "route", "status"]);
    }

    // without matchers, with limit set to a higher number than the items returned
    {
        let res = ing
            .handle_label_names(
                &md,
                "test",
                ingesterpb::LabelNamesRequest {
                    start_timestamp_ms: 0,
                    end_timestamp_ms: i64::MAX,
                    limit: 10,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let mut names = res.label_names;
        names.sort();
        assert_eq!(names, vec!["__name__", "route", "status"]);
    }

    ing.stop().await.unwrap();
}

// Upstream: pkg/ingester/ingester_test.go::Test_Ingester_LabelValues
#[tokio::test]
async fn test_ingester_label_values() {
    let tmp = tempfile::tempdir().unwrap();
    let (cfg, blocks_cfg) = default_ingester_test_config(tmp.path());
    let reg = Registry::new();
    let ing = prepare_ingester_with_blocks_storage(tmp.path(), cfg, blocks_cfg, None, Some(&reg)).await;

    let series = [
        (
            Labels::from_pairs([(METRIC_NAME_LABEL, "test_1"), ("status", "200"), ("route", "get_user")]),
            1.0,
            100000,
        ),
        (
            Labels::from_pairs([(METRIC_NAME_LABEL, "test_1"), ("status", "500"), ("route", "get_user")]),
            1.0,
            110000,
        ),
        (Labels::from_pairs([(METRIC_NAME_LABEL, "test_2")]), 2.0, 200000),
    ];

    for (lbls, val, ts) in &series {
        ing.push_grpc("test", &mock_write_request(lbls, *val, *ts))
            .await
            .unwrap();
    }

    let md = md_with_org("test");
    let expected = [
        ("__name__", vec!["test_1", "test_2"]),
        ("status", vec!["200", "500"]),
        ("route", vec!["get_user"]),
        ("unknown", vec![]),
    ];

    for (label_name, exp) in expected {
        let res = ing
            .handle_label_values(
                &md,
                "test",
                ingesterpb::LabelValuesRequest {
                    label_name: label_name.to_string(),
                    end_timestamp_ms: i64::MAX,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let mut vals = res.label_values;
        vals.sort();
        assert_eq!(vals, exp);
    }

    let expected_limits = [
        (0, vec!["test_1", "test_2"]),
        (1, vec!["test_1"]),
        (2, vec!["test_1", "test_2"]),
        (4, vec!["test_1", "test_2"]),
    ];
    for (limit, exp) in expected_limits {
        let res = ing
            .handle_label_values(
                &md,
                "test",
                ingesterpb::LabelValuesRequest {
                    label_name: "__name__".to_string(),
                    start_timestamp_ms: 0,
                    end_timestamp_ms: i64::MAX,
                    limit,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(res.label_values, exp);
    }

    ing.stop().await.unwrap();
}

// Upstream: pkg/ingester/ingester_test.go::Test_Ingester_Query
#[tokio::test]
async fn test_ingester_query() {
    let tmp = tempfile::tempdir().unwrap();
    let (cfg, blocks_cfg) = default_ingester_test_config(tmp.path());
    let ing = prepare_ingester_with_blocks_storage(tmp.path(), cfg, blocks_cfg, None, None).await;

    let series = [
        (
            Labels::from_pairs([(METRIC_NAME_LABEL, "test_1"), ("status", "200"), ("route", "get_user")]),
            100000i64,
            1.0f64,
        ),
        (
            Labels::from_pairs([(METRIC_NAME_LABEL, "test_1"), ("status", "500"), ("route", "get_user")]),
            110000i64,
            1.0f64,
        ),
        (
            Labels::from_pairs([(METRIC_NAME_LABEL, "test_2"), ("status", "500"), ("route", "get_user")]),
            200000i64,
            2.0f64,
        ),
    ];

    for (lbls, ts, val) in &series {
        ing.push_grpc("test", &mock_write_request(lbls, *val, *ts))
            .await
            .unwrap();
    }

    type QueryCase = (&'static str, i64, i64, Vec<ingesterpb::LabelMatcher>, Vec<(Labels, Vec<(i64, f64)>)>);
    let cases: Vec<QueryCase> = vec![
        (
            "should return an empty response if no metric matches",
            i64::MIN,
            i64::MAX,
            vec![mk_matcher(ingesterpb::MatchType::Equal, METRIC_NAME_LABEL, "unknown")],
            vec![],
        ),
        (
            "should filter series by == matcher",
            i64::MIN,
            i64::MAX,
            vec![mk_matcher(ingesterpb::MatchType::Equal, METRIC_NAME_LABEL, "test_1")],
            vec![
                (series[0].0.clone(), vec![(100000, 1.0)]),
                (series[1].0.clone(), vec![(110000, 1.0)]),
            ],
        ),
        (
            "should filter series by != matcher",
            i64::MIN,
            i64::MAX,
            vec![mk_matcher(ingesterpb::MatchType::NotEqual, METRIC_NAME_LABEL, "test_1")],
            vec![(series[2].0.clone(), vec![(200000, 2.0)])],
        ),
        (
            "should filter series by =~ matcher",
            i64::MIN,
            i64::MAX,
            vec![mk_matcher(ingesterpb::MatchType::RegexMatch, METRIC_NAME_LABEL, ".*_1")],
            vec![
                (series[0].0.clone(), vec![(100000, 1.0)]),
                (series[1].0.clone(), vec![(110000, 1.0)]),
            ],
        ),
        (
            "should filter series by !~ matcher",
            i64::MIN,
            i64::MAX,
            vec![mk_matcher(ingesterpb::MatchType::RegexNoMatch, METRIC_NAME_LABEL, ".*_1")],
            vec![(series[2].0.clone(), vec![(200000, 2.0)])],
        ),
        (
            "should filter series by multiple matchers",
            i64::MIN,
            i64::MAX,
            vec![
                mk_matcher(ingesterpb::MatchType::Equal, METRIC_NAME_LABEL, "test_1"),
                mk_matcher(ingesterpb::MatchType::RegexMatch, "status", "5.."),
            ],
            vec![(series[1].0.clone(), vec![(110000, 1.0)])],
        ),
        (
            "should filter series by matcher and time range",
            100000,
            100000,
            vec![mk_matcher(ingesterpb::MatchType::Equal, METRIC_NAME_LABEL, "test_1")],
            vec![(series[0].0.clone(), vec![(100000, 1.0)])],
        ),
    ];

    let md = md_with_org("test");
    for (name, from, to, matchers, expected) in cases {
        let req = ingesterpb::QueryRequest {
            start_timestamp_ms: from,
            end_timestamp_ms: to,
            matchers,
            streaming_chunks_batch_size: 64,
        };
        let resps = ing.handle_query_stream(&md, "test", req).await.unwrap();
        let actual = streams_to_matrix(&resps);
        let exp_map: BTreeMap<Labels, Vec<(i64, f64)>> = expected.into_iter().collect();
        assert_eq!(actual, exp_map, "case: {name}");
    }

    ing.stop().await.unwrap();
}

// Upstream: pkg/ingester/ingester_test.go::TestIngester_LabelNamesAndValues
#[tokio::test]
async fn test_ingester_label_names_and_values() {
    let tmp = tempfile::tempdir().unwrap();
    let (cfg, blocks_cfg) = default_ingester_test_config(tmp.path());
    let reg = Registry::new();
    let ing = prepare_ingester_with_blocks_storage(tmp.path(), cfg, blocks_cfg, None, Some(&reg)).await;

    let series = [
        (Labels::from_pairs([(METRIC_NAME_LABEL, "metric_0"), ("status", "500")]), 100000, 1.0),
        (Labels::from_pairs([(METRIC_NAME_LABEL, "metric_0"), ("status", "200")]), 110000, 1.0),
        (Labels::from_pairs([(METRIC_NAME_LABEL, "metric_1"), ("env", "prod")]), 200000, 2.0),
        (
            Labels::from_pairs([(METRIC_NAME_LABEL, "metric_1"), ("env", "prod"), ("status", "300")]),
            200000,
            3.0,
        ),
    ];
    for (lbls, ts, val) in &series {
        ing.push_grpc("test", &mock_write_request(lbls, *val, *ts))
            .await
            .unwrap();
    }

    let md = md_with_org("test");

    type LnvCase = (&'static str, Vec<ingesterpb::LabelMatcher>, BTreeMap<&'static str, Vec<&'static str>>);
    let cases: Vec<LnvCase> = vec![
        (
            "expected all label with values",
            vec![],
            BTreeMap::from([
                (METRIC_NAME_LABEL, vec!["metric_0", "metric_1"]),
                ("status", vec!["200", "300", "500"]),
                ("env", vec!["prod"]),
            ]),
        ),
        (
            "expected label values only from `metric_0`",
            vec![mk_matcher(ingesterpb::MatchType::Equal, "__name__", "metric_0")],
            BTreeMap::from([
                (METRIC_NAME_LABEL, vec!["metric_0"]),
                ("status", vec!["200", "500"]),
            ]),
        ),
    ];

    for (name, matchers, expected) in cases {
        let req = ingesterpb::LabelNamesAndValuesRequest {
            matchers,
            count_method: ingesterpb::CountMethod::InMemory as i32,
        };
        let resps = ing
            .handle_label_names_and_values(&md, "test", req)
            .await
            .unwrap();
        let mut actual: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for resp in resps {
            for mut item in resp.items {
                item.values.sort();
                actual.insert(item.label_name, item.values);
            }
        }
        let exp: BTreeMap<String, Vec<String>> = expected
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.into_iter().map(String::from).collect()))
            .collect();
        assert_eq!(actual, exp, "case: {name}");
    }

    ing.stop().await.unwrap();
}

// Upstream: pkg/ingester/ingester_test.go::TestIngester_LabelValuesCardinality
#[tokio::test]
async fn test_ingester_label_values_cardinality() {
    let tmp = tempfile::tempdir().unwrap();
    let (cfg, blocks_cfg) = default_ingester_test_config(tmp.path());
    let reg = Registry::new();
    let ing = prepare_ingester_with_blocks_storage(tmp.path(), cfg, blocks_cfg, None, Some(&reg)).await;

    let series = [
        (Labels::from_pairs([(METRIC_NAME_LABEL, "metric_0"), ("status", "500")]), 100000, 1.5),
        (Labels::from_pairs([(METRIC_NAME_LABEL, "metric_0"), ("status", "200")]), 110030, 1.5),
        (Labels::from_pairs([(METRIC_NAME_LABEL, "metric_1"), ("env", "prod")]), 100060, 1.5),
        (
            Labels::from_pairs([(METRIC_NAME_LABEL, "metric_1"), ("env", "prod"), ("status", "300")]),
            100090,
            1.5,
        ),
    ];
    for (lbls, ts, val) in &series {
        ing.push_grpc("test", &mock_write_request(lbls, *val, *ts))
            .await
            .unwrap();
    }

    let md = md_with_org("test");

    // 1. expected all label values cardinality
    {
        let resps = ing
            .handle_label_values_cardinality(
                &md,
                "test",
                ingesterpb::LabelValuesCardinalityRequest {
                    label_names: vec![
                        METRIC_NAME_LABEL.to_string(),
                        "env".to_string(),
                        "status".to_string(),
                    ],
                    matchers: vec![],
                    count_method: ingesterpb::CountMethod::InMemory as i32,
                },
            )
            .await
            .unwrap();
        assert_eq!(resps.len(), 1);
        let mut by_name: BTreeMap<String, HashMap<String, u64>> = BTreeMap::new();
        for item in &resps[0].items {
            by_name.insert(item.label_name.clone(), item.label_value_series.clone());
        }
        assert_eq!(
            by_name.get("status").unwrap(),
            &HashMap::from([
                ("200".to_string(), 1u64),
                ("300".to_string(), 1),
                ("500".to_string(), 1),
            ])
        );
        assert_eq!(
            by_name.get(METRIC_NAME_LABEL).unwrap(),
            &HashMap::from([("metric_0".to_string(), 2u64), ("metric_1".to_string(), 2)])
        );
        assert_eq!(
            by_name.get("env").unwrap(),
            &HashMap::from([("prod".to_string(), 2u64)])
        );
    }

    // 2. expected status values cardinality applying matchers
    {
        let resps = ing
            .handle_label_values_cardinality(
                &md,
                "test",
                ingesterpb::LabelValuesCardinalityRequest {
                    label_names: vec!["status".to_string()],
                    matchers: vec![mk_matcher(
                        ingesterpb::MatchType::Equal,
                        METRIC_NAME_LABEL,
                        "metric_1",
                    )],
                    count_method: ingesterpb::CountMethod::InMemory as i32,
                },
            )
            .await
            .unwrap();
        assert_eq!(resps.len(), 1);
        assert_eq!(resps[0].items.len(), 1);
        assert_eq!(resps[0].items[0].label_name, "status");
        assert_eq!(
            resps[0].items[0].label_value_series,
            HashMap::from([("300".to_string(), 1u64)])
        );
    }

    // 3. empty response is returned when no matchers match the requested labels
    {
        let resps = ing
            .handle_label_values_cardinality(
                &md,
                "test",
                ingesterpb::LabelValuesCardinalityRequest {
                    label_names: vec!["status".to_string()],
                    matchers: vec![mk_matcher(
                        ingesterpb::MatchType::Equal,
                        "job",
                        "store-gateway",
                    )],
                    count_method: ingesterpb::CountMethod::InMemory as i32,
                },
            )
            .await
            .unwrap();
        assert!(resps.is_empty());
    }

    ing.stop().await.unwrap();
}

// Upstream: pkg/ingester/ingester_test.go::TestIngester_QueryStream_QuerySharding
#[tokio::test]
async fn test_ingester_query_stream_query_sharding() {
    const NUM_SERIES: usize = 1000;
    const NUM_SHARDS: u64 = 16;

    let tmp = tempfile::tempdir().unwrap();
    let (cfg, blocks_cfg) = default_ingester_test_config(tmp.path());
    let ing = prepare_ingester_with_blocks_storage(tmp.path(), cfg, blocks_cfg, None, None).await;

    for series_id in 0..NUM_SERIES {
        let lbls = Labels::from_pairs([
            (METRIC_NAME_LABEL, "foo"),
            ("series_id", &series_id.to_string()),
        ]);
        ing.push_grpc(
            USER_ID,
            &mock_write_request(&lbls, series_id as f64, series_id as i64),
        )
        .await
        .unwrap();

        if series_id == NUM_SERIES / 2 {
            ing.flush().await;
        }
    }

    let md = md_with_org(USER_ID);
    let mut actual_series_ids = Vec::new();
    let mut seen_ids = HashSet::new();

    for shard_idx in 0..NUM_SHARDS {
        let req = ingesterpb::QueryRequest {
            start_timestamp_ms: i64::MIN,
            end_timestamp_ms: i64::MAX,
            matchers: vec![
                mk_matcher(ingesterpb::MatchType::Equal, METRIC_NAME_LABEL, "foo"),
                mk_matcher(
                    ingesterpb::MatchType::Equal,
                    SHARD_LABEL,
                    &format_shard_id_label_value(shard_idx, NUM_SHARDS),
                ),
            ],
            streaming_chunks_batch_size: 128,
        };
        let resps = ing.handle_query_stream(&md, USER_ID, req).await.unwrap();
        let matrix = streams_to_matrix(&resps);
        for (lbls, samples) in matrix {
            let sid: usize = lbls.get("series_id").parse().unwrap();
            assert!(
                seen_ids.insert(sid),
                "series {sid} was returned multiple times"
            );
            actual_series_ids.push(sid);
            assert_eq!(samples.len(), 1);
            assert_eq!(samples[0].0, sid as i64);
            assert_eq!(samples[0].1, sid as f64);
        }
    }

    assert_eq!(actual_series_ids.len(), NUM_SERIES);
    let expected: HashSet<usize> = (0..NUM_SERIES).collect();
    assert_eq!(seen_ids, expected);

    ing.stop().await.unwrap();
}

// Upstream: pkg/ingester/ingester_test.go::TestIngester_QueryStream_QueryShardingShouldGuaranteeSeriesShardingConsistencyOverTime
#[tokio::test]
async fn test_ingester_query_stream_query_sharding_should_guarantee_series_sharding_consistency_over_time() {
    const NUM_SERIES: usize = 100;
    const NUM_SHARDS: u64 = 2;

    let expected_by_shard: HashMap<&str, Vec<usize>> = HashMap::from([
        (
            "1_of_2",
            vec![
                0, 1, 10, 12, 16, 18, 2, 22, 23, 24, 26, 28, 29, 3, 30, 33, 34, 35, 36, 39, 40,
                41, 42, 43, 44, 47, 53, 54, 57, 58, 60, 61, 63, 66, 67, 68, 69, 7, 71, 75, 77, 80,
                81, 83, 84, 86, 87, 89, 9, 90, 91, 92, 94, 96, 98, 99,
            ],
        ),
        (
            "2_of_2",
            vec![
                11, 13, 14, 15, 17, 19, 20, 21, 25, 27, 31, 32, 37, 38, 4, 45, 46, 48, 49, 5, 50,
                51, 52, 55, 56, 59, 6, 62, 64, 65, 70, 72, 73, 74, 76, 78, 79, 8, 82, 85, 88, 93,
                95, 97,
            ],
        ),
    ]);

    let tmp = tempfile::tempdir().unwrap();
    let (cfg, blocks_cfg) = default_ingester_test_config(tmp.path());
    let ing = prepare_ingester_with_blocks_storage(tmp.path(), cfg, blocks_cfg, None, None).await;

    for series_id in 0..NUM_SERIES {
        let lbls = Labels::from_pairs([
            (METRIC_NAME_LABEL, "test"),
            ("series_id", &series_id.to_string()),
        ]);
        ing.push_grpc(
            USER_ID,
            &mock_write_request(&lbls, series_id as f64, series_id as i64),
        )
        .await
        .unwrap();
    }

    let md = md_with_org(USER_ID);
    for shard_id in 0..NUM_SHARDS {
        let shard_label = format_shard_id_label_value(shard_id, NUM_SHARDS);
        let req = ingesterpb::QueryRequest {
            start_timestamp_ms: i64::MIN,
            end_timestamp_ms: i64::MAX,
            matchers: vec![
                mk_matcher(ingesterpb::MatchType::RegexMatch, "series_id", ".+"),
                mk_matcher(ingesterpb::MatchType::Equal, SHARD_LABEL, &shard_label),
            ],
            streaming_chunks_batch_size: NUM_SERIES as u64,
        };
        let resps = ing.handle_query_stream(&md, USER_ID, req).await.unwrap();
        assert!(!resps.is_empty());

        let mut actual_ids = Vec::new();
        for resp in &resps {
            for s in &resp.streaming_series {
                let lbls = labels_from_pairs(&s.labels);
                actual_ids.push(lbls.get("series_id").parse::<usize>().unwrap());
            }
        }
        actual_ids.sort();
        let mut expected_ids = expected_by_shard[shard_label.as_str()].clone();
        expected_ids.sort();
        assert_eq!(actual_ids, expected_ids);
    }

    ing.stop().await.unwrap();
}

// Upstream: pkg/ingester/ingester_test.go::TestIngester_QueryStream_ShouldNotCreateTSDBIfDoesNotExists
#[tokio::test]
async fn test_ingester_query_stream_should_not_create_tsdb_if_does_not_exists() {
    for batch_size in [64u64, 0u64] {
        let tmp = tempfile::tempdir().unwrap();
        let (cfg, blocks_cfg) = default_ingester_test_config(tmp.path());
        let ing = prepare_ingester_with_blocks_storage(tmp.path(), cfg, blocks_cfg, None, None).await;

        let md = md_with_org("test");
        let resps = ing
            .handle_query_stream(
                &md,
                "test",
                ingesterpb::QueryRequest {
                    streaming_chunks_batch_size: batch_size,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(resps.is_empty());
        assert!(ing.get_tsdb("test").is_none());

        ing.stop().await.unwrap();
    }
}

// Upstream: pkg/ingester/ingester_test.go::TestIngester_LabelValues_ShouldNotCreateTSDBIfDoesNotExists
#[tokio::test]
async fn test_ingester_label_values_should_not_create_tsdb_if_does_not_exists() {
    let tmp = tempfile::tempdir().unwrap();
    let (cfg, blocks_cfg) = default_ingester_test_config(tmp.path());
    let ing = prepare_ingester_with_blocks_storage(tmp.path(), cfg, blocks_cfg, None, None).await;

    let md = md_with_org("test");
    let res = ing
        .handle_label_values(&md, "test", ingesterpb::LabelValuesRequest::default())
        .await
        .unwrap();
    assert!(res.label_values.is_empty());
    assert!(ing.get_tsdb("test").is_none());

    ing.stop().await.unwrap();
}

// Upstream: pkg/ingester/ingester_test.go::TestIngester_LabelNames_ShouldNotCreateTSDBIfDoesNotExists
#[tokio::test]
async fn test_ingester_label_names_should_not_create_tsdb_if_does_not_exists() {
    let tmp = tempfile::tempdir().unwrap();
    let (cfg, blocks_cfg) = default_ingester_test_config(tmp.path());
    let ing = prepare_ingester_with_blocks_storage(tmp.path(), cfg, blocks_cfg, None, None).await;

    let md = md_with_org("test");
    let res = ing
        .handle_label_names(
            &md,
            "test",
            ingesterpb::LabelNamesRequest {
                end_timestamp_ms: i64::MAX,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(res.label_names.is_empty());
    assert!(ing.get_tsdb("test").is_none());

    ing.stop().await.unwrap();
}

// Upstream: pkg/ingester/ingester_test.go::TestIngester_Push_ShouldNotCreateTSDBIngesterServiceIsNotInRunningState
#[tokio::test]
async fn test_ingester_push_should_not_create_tsdb_ingester_service_is_not_in_running_state() {
    let tmp = tempfile::tempdir().unwrap();
    let (mut cfg, blocks_cfg) = default_ingester_test_config(tmp.path());
    cfg.ring.final_sleep = GoDuration::from_secs(2);
    cfg.ring.heartbeat_period = GoDuration::from_millis(50);

    let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));
    let lc_cfg = LifecyclerConfig::from_ring_config(&cfg.ring, 9095).unwrap();
    let lc = Lifecycler::with_store(
        lc_cfg,
        Arc::clone(&kv),
        None,
        "ingester",
        "collectors/ingester",
        false,
        None,
    );
    let ring = Ring::with_store_and_strategy(
        RingSettings::from(&cfg.ring),
        "ingester",
        "collectors/ingester",
        kv,
        Arc::new(DefaultReplicationStrategy),
        None,
    )
    .unwrap();

    let ring_svc: Arc<dyn mimir_common::services::Service> = ring.clone().into_service();
    ring_svc
        .start_async(mimir_common::services::ServiceContext::background())
        .await
        .unwrap();
    ring_svc.await_running().await.unwrap();

    let ing = Ingester::new(
        cfg,
        blocks_cfg,
        IngestStorageConfig::default(),
        Overrides::new(default_limits_test_config()),
        Some(ring),
        Some(lc),
        None,
        None,
        None,
        None,
    )
    .unwrap();
    ing.start().await.unwrap();

    // Stop asynchronously so FinalSleep keeps ingester in Stopping state.
    let ing_stop = Arc::clone(&ing);
    let stop_handle = tokio::spawn(async move {
        ing_stop.stop().await.unwrap();
    });

    // Wait until ingester switches to Stopping state.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while ing.state() != State::Stopping && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(ing.state(), State::Stopping);

    let req = mock_write_request(&Labels::from_pairs([(METRIC_NAME_LABEL, "test")]), 0.0, 0);
    let err = ing.push_grpc("test", &req).await.unwrap_err();
    assert_eq!(
        err.to_string(),
        IngesterError::Unavailable {
            state: State::Stopping
        }
        .to_string()
    );
    assert!(ing.get_tsdb("test").is_none());

    stop_handle.await.unwrap();
    assert_eq!(ing.state(), State::Terminated);
}

// Upstream: pkg/ingester/ingester_test.go::Test_Ingester_MetricsForLabelMatchers
#[tokio::test]
async fn test_ingester_metrics_for_label_matchers() {
    let tmp = tempfile::tempdir().unwrap();
    let (cfg, blocks_cfg) = default_ingester_test_config(tmp.path());
    let reg = Registry::new();
    let ing = prepare_ingester_with_blocks_storage(tmp.path(), cfg, blocks_cfg, None, Some(&reg)).await;

    let fixtures = [
        (Labels::from_pairs([(METRIC_NAME_LABEL, "test_1"), ("status", "200")]), 1.0, 100000i64),
        (Labels::from_pairs([(METRIC_NAME_LABEL, "test_1"), ("status", "500")]), 1.0, 110000),
        (Labels::from_pairs([(METRIC_NAME_LABEL, "test_2")]), 2.0, 200000),
        // Colliding FastFingerprint in Prometheus Go
        (
            Labels::from_pairs([
                (METRIC_NAME_LABEL, "collision"),
                ("app", "l"),
                ("uniq0", "0"),
                ("uniq1", "1"),
            ]),
            1.0,
            300000,
        ),
        (
            Labels::from_pairs([
                (METRIC_NAME_LABEL, "collision"),
                ("app", "m"),
                ("uniq0", "1"),
                ("uniq1", "1"),
            ]),
            1.0,
            300000,
        ),
    ];

    for (lbls, val, ts) in &fixtures {
        ing.push_grpc("test", &mock_write_request(lbls, *val, *ts))
            .await
            .unwrap();
    }

    type MatchersCase = (
        &'static str,
        i64,
        i64,
        i64,
        Vec<ingesterpb::LabelMatchers>,
        Vec<Labels>,
    );
    let cases: Vec<MatchersCase> = vec![
        (
            "should return an empty response if no metric match",
            i64::MIN,
            i64::MAX,
            0,
            vec![ingesterpb::LabelMatchers {
                matchers: vec![mk_matcher(ingesterpb::MatchType::Equal, METRIC_NAME_LABEL, "unknown")],
            }],
            vec![],
        ),
        (
            "should filter metrics by single matcher",
            i64::MIN,
            i64::MAX,
            0,
            vec![ingesterpb::LabelMatchers {
                matchers: vec![mk_matcher(ingesterpb::MatchType::Equal, METRIC_NAME_LABEL, "test_1")],
            }],
            vec![fixtures[0].0.clone(), fixtures[1].0.clone()],
        ),
        (
            "should filter metrics by multiple matchers",
            i64::MIN,
            i64::MAX,
            0,
            vec![
                ingesterpb::LabelMatchers {
                    matchers: vec![mk_matcher(ingesterpb::MatchType::Equal, "status", "200")],
                },
                ingesterpb::LabelMatchers {
                    matchers: vec![mk_matcher(ingesterpb::MatchType::Equal, METRIC_NAME_LABEL, "test_2")],
                },
            ],
            vec![fixtures[0].0.clone(), fixtures[2].0.clone()],
        ),
        (
            "should filter metrics by time range to return nothing when queried for older time ranges",
            100,
            1000,
            0,
            vec![ingesterpb::LabelMatchers {
                matchers: vec![mk_matcher(ingesterpb::MatchType::Equal, METRIC_NAME_LABEL, "test_1")],
            }],
            vec![],
        ),
        (
            "should not return duplicated metrics on overlapping matchers",
            i64::MIN,
            i64::MAX,
            0,
            vec![
                ingesterpb::LabelMatchers {
                    matchers: vec![mk_matcher(ingesterpb::MatchType::Equal, METRIC_NAME_LABEL, "test_1")],
                },
                ingesterpb::LabelMatchers {
                    matchers: vec![mk_matcher(ingesterpb::MatchType::RegexMatch, METRIC_NAME_LABEL, "test.*")],
                },
            ],
            vec![fixtures[0].0.clone(), fixtures[1].0.clone(), fixtures[2].0.clone()],
        ),
        (
            "should return all matching metrics even if their FastFingerprint collide",
            i64::MIN,
            i64::MAX,
            0,
            vec![ingesterpb::LabelMatchers {
                matchers: vec![mk_matcher(ingesterpb::MatchType::Equal, METRIC_NAME_LABEL, "collision")],
            }],
            vec![fixtures[3].0.clone(), fixtures[4].0.clone()],
        ),
        (
            "should respect requested limit",
            i64::MIN,
            i64::MAX,
            1,
            vec![ingesterpb::LabelMatchers {
                matchers: vec![mk_matcher(ingesterpb::MatchType::Equal, METRIC_NAME_LABEL, "test_1")],
            }],
            vec![fixtures[0].0.clone()],
        ),
        (
            "should return all matching metrics when limit is higher than the result",
            i64::MIN,
            i64::MAX,
            5,
            vec![ingesterpb::LabelMatchers {
                matchers: vec![mk_matcher(ingesterpb::MatchType::Equal, METRIC_NAME_LABEL, "test_1")],
            }],
            vec![fixtures[0].0.clone(), fixtures[1].0.clone()],
        ),
    ];

    let md = md_with_org("test");
    for (name, from, to, limit, matchers_set, expected) in cases {
        let res = ing
            .handle_metrics_for_label_matchers(
                &md,
                "test",
                ingesterpb::MetricsForLabelMatchersRequest {
                    start_timestamp_ms: from,
                    end_timestamp_ms: to,
                    matchers_set,
                    limit,
                },
            )
            .await
            .unwrap();
        let actual: BTreeSet<Labels> = res
            .metric
            .iter()
            .map(|m| labels_from_pairs(&m.labels))
            .collect();
        let exp: BTreeSet<Labels> = expected.into_iter().collect();
        assert_eq!(actual, exp, "case: {name}");
    }

    ing.stop().await.unwrap();
}

// Upstream: pkg/ingester/ingester_test.go::Test_Ingester_MetricsForLabelMatchers_Deduplication
#[tokio::test]
async fn test_ingester_metrics_for_label_matchers_deduplication() {
    const NUM_SERIES: usize = 100_000;
    const BATCH_SIZE: usize = 1000;

    let tmp = tempfile::tempdir().unwrap();
    let (cfg, blocks_cfg) = default_ingester_test_config(tmp.path());
    let ing = prepare_ingester_with_blocks_storage(tmp.path(), cfg, blocks_cfg, None, None).await;

    let now = now_ms();
    for o in (0..NUM_SERIES).step_by(BATCH_SIZE) {
        let batch_len = BATCH_SIZE.min(NUM_SERIES - o);
        let mut ts_list = Vec::with_capacity(batch_len);
        for s in 0..batch_len {
            ts_list.push(cortexpb::TimeSeries {
                labels: vec![cortexpb::LabelPair::from_strs(
                    METRIC_NAME_LABEL,
                    format!("test_{}", o + s),
                )],
                samples: vec![cortexpb::Sample {
                    timestamp_ms: now,
                    value: 1.0,
                }],
                ..Default::default()
            });
        }
        let req = cortexpb::WriteRequest {
            timeseries: ts_list,
            source: cortexpb::write_request::SourceEnum::Api as i32,
            ..Default::default()
        };
        ing.push_grpc("test", &req).await.unwrap();
    }

    let md = md_with_org("test");
    let res = ing
        .handle_metrics_for_label_matchers(
            &md,
            "test",
            ingesterpb::MetricsForLabelMatchersRequest {
                start_timestamp_ms: now,
                end_timestamp_ms: now,
                matchers_set: vec![
                    ingesterpb::LabelMatchers {
                        matchers: vec![mk_matcher(
                            ingesterpb::MatchType::RegexMatch,
                            METRIC_NAME_LABEL,
                            "test.*",
                        )],
                    },
                    ingesterpb::LabelMatchers {
                        matchers: vec![mk_matcher(
                            ingesterpb::MatchType::RegexMatch,
                            METRIC_NAME_LABEL,
                            "test.*0",
                        )],
                    },
                ],
                limit: 0,
            },
        )
        .await
        .unwrap();
    assert_eq!(res.metric.len(), NUM_SERIES);

    ing.stop().await.unwrap();
}

struct FakeUtilizationLimiter {
    reason: String,
}

impl UtilizationBasedLimiter for FakeUtilizationLimiter {
    fn limiting_reason(&self) -> Option<String> {
        Some(self.reason.clone())
    }
}

// Upstream: pkg/ingester/ingester_test.go::TestIngester_QueryStream
#[tokio::test]
async fn test_ingester_query_stream() {
    const NUM_SERIES: usize = 1000;

    let tmp = tempfile::tempdir().unwrap();
    let (cfg, blocks_cfg) = default_ingester_test_config(tmp.path());
    let reg = Registry::new();
    let ing =
        prepare_ingester_with_blocks_storage(tmp.path(), cfg, blocks_cfg, None, Some(&reg)).await;

    for series_id in 0..NUM_SERIES {
        let sid_str = series_id.to_string();
        let float_lbls = Labels::from_pairs([
            (METRIC_NAME_LABEL, "foo"),
            ("series_id", &sid_str),
            ("type", "float"),
        ]);
        ing.push_grpc(
            USER_ID,
            &mock_write_request(&float_lbls, series_id as f64, series_id as i64),
        )
        .await
        .unwrap();

        let hist_lbls = Labels::from_pairs([
            (METRIC_NAME_LABEL, "foo"),
            ("series_id", &sid_str),
            ("type", "histogram"),
        ]);
        ing.push_grpc(
            USER_ID,
            &mock_histogram_write_request(&hist_lbls, series_id as i64, series_id as i64, false),
        )
        .await
        .unwrap();

        let fhist_lbls = Labels::from_pairs([
            (METRIC_NAME_LABEL, "foo"),
            ("series_id", &sid_str),
            ("type", "floathistogram"),
        ]);
        ing.push_grpc(
            USER_ID,
            &mock_histogram_write_request(&fhist_lbls, series_id as i64, series_id as i64, true),
        )
        .await
        .unwrap();

        if series_id == NUM_SERIES / 2 {
            ing.flush().await;
        }
    }

    let (mut client, shutdown_tx, srv_handle) = start_grpc_server(Arc::clone(&ing)).await;

    async fn run_grpc_query(
        c: &mut IngesterClient<tonic::transport::Channel>,
        req: ingesterpb::QueryRequest,
    ) -> (Vec<ingesterpb::QueryStreamSeries>, Vec<ingesterpb::QueryStreamSeriesChunks>) {
        let mut grpc_req = Request::new(req);
        grpc_req
            .metadata_mut()
            .insert(LOWER_ORG_ID_HEADER_NAME, USER_ID.parse().unwrap());
        let mut stream = c.query_stream(grpc_req).await.unwrap().into_inner();
        let mut series = Vec::new();
        let mut chunks = Vec::new();
        while let Some(resp) = stream.message().await.unwrap() {
            series.extend(resp.streaming_series);
            chunks.extend(resp.streaming_series_chunks);
        }
        (series, chunks)
    }

    for num_shards in [0u64, 16u64] {
        let mut actual_series = Vec::new();
        let mut actual_chunks = Vec::new();

        if num_shards > 0 {
            for shard_idx in 0..num_shards {
                let (s, ch) = run_grpc_query(
                    &mut client,
                    ingesterpb::QueryRequest {
                        start_timestamp_ms: i64::MIN,
                        end_timestamp_ms: i64::MAX,
                        matchers: vec![
                            mk_matcher(ingesterpb::MatchType::Equal, METRIC_NAME_LABEL, "foo"),
                            mk_matcher(
                                ingesterpb::MatchType::Equal,
                                SHARD_LABEL,
                                &format_shard_id_label_value(shard_idx, num_shards),
                            ),
                        ],
                        streaming_chunks_batch_size: 64,
                    },
                )
                .await;
                assert!(!s.is_empty());
                // Offset series_index by current total so we can index flat `actual_series`
                let base_idx = actual_series.len() as u64;
                actual_series.extend(s);
                for mut sc in ch {
                    sc.series_index += base_idx;
                    actual_chunks.push(sc);
                }
            }
        } else {
            let (s, ch) = run_grpc_query(
                &mut client,
                ingesterpb::QueryRequest {
                    start_timestamp_ms: i64::MIN,
                    end_timestamp_ms: i64::MAX,
                    matchers: vec![mk_matcher(
                        ingesterpb::MatchType::Equal,
                        METRIC_NAME_LABEL,
                        "foo",
                    )],
                    streaming_chunks_batch_size: 64,
                },
            )
            .await;
            assert!(!s.is_empty());
            actual_series.extend(s);
            actual_chunks.extend(ch);
        }

        assert_eq!(actual_series.len(), NUM_SERIES * 3);
        let mut seen: HashMap<String, HashSet<usize>> = HashMap::from([
            ("float".to_string(), HashSet::new()),
            ("histogram".to_string(), HashSet::new()),
            ("floathistogram".to_string(), HashSet::new()),
        ]);

        for (idx, s) in actual_series.iter().enumerate() {
            let lbls = labels_from_pairs(&s.labels);
            let type_lbl = lbls.get("type").to_string();
            let sid: usize = lbls.get("series_id").parse().unwrap();
            assert!(seen.get_mut(&type_lbl).unwrap().insert(sid));

            let sc = &actual_chunks[idx];
            assert_eq!(sc.chunks.len(), 1);
            let enc = decode_wire_chunk(sc.chunks[0].encoding, &sc.chunks[0].data);
            let decoded = decode_chunk_samples(&enc).unwrap();
            assert_eq!(decoded.len(), 1);
            assert_eq!(decoded[0].t, sid as i64);
            match (sc.chunks[0].encoding, &decoded[0].value) {
                (4, SampleValue::Float(v)) => assert_eq!(*v, sid as f64),
                (5, SampleValue::Histogram(h)) => {
                    assert_eq!(
                        **h,
                        generate_test_histogram(sid as i64, CounterResetHint::UnknownCounterReset)
                    );
                }
                (6, SampleValue::FloatHistogram(fh)) => {
                    assert_eq!(
                        **fh,
                        generate_test_float_histogram(
                            sid as i64,
                            CounterResetHint::UnknownCounterReset
                        )
                    );
                }
                _ => panic!("unexpected encoding/sample mismatch"),
            }
        }
    }

    // subtest: limited due to resource utilization
    {
        ing.set_utilization_based_limiter(Some(Arc::new(FakeUtilizationLimiter {
            reason: "cpu".to_string(),
        })));
        let err = ing
            .start_read_request(&MetadataMap::new())
            .await
            .err()
            .unwrap();
        let status = err.map_read_error_to_status();
        assert_eq!(status.code(), Code::ResourceExhausted);
        assert_eq!(
            status.message(),
            "ingester is currently too busy to process queries, try again later"
        );
        assert_eq!(
            gather_metric_value(
                &reg,
                "cortex_ingester_utilization_limited_read_requests_total",
                &[("reason", "cpu")]
            ),
            Some(1.0)
        );
        ing.set_utilization_based_limiter(None);
    }

    let _ = shutdown_tx.send(());
    srv_handle.await.unwrap();
    ing.stop().await.unwrap();
}

// Upstream: pkg/ingester/ingester_test.go::TestIngester_QueryStream_StreamingWithManySamples
#[tokio::test]
async fn test_ingester_query_stream_streaming_with_many_samples() {
    const SAMPLE_COUNT: usize = 1_000_000;

    let tmp = tempfile::tempdir().unwrap();
    let (cfg, blocks_cfg) = default_ingester_test_config(tmp.path());
    let ing = prepare_ingester_with_blocks_storage(tmp.path(), cfg, blocks_cfg, None, None).await;

    let samples = generate_samples(SAMPLE_COUNT);
    ing.push_grpc(
        USER_ID,
        &write_request_single_series(
            &Labels::from_pairs([(METRIC_NAME_LABEL, "foo"), ("l", "1")]),
            samples[0..100_000].to_vec(),
        ),
    )
    .await
    .unwrap();
    ing.push_grpc(
        USER_ID,
        &write_request_single_series(
            &Labels::from_pairs([(METRIC_NAME_LABEL, "foo"), ("l", "2")]),
            samples.clone(),
        ),
    )
    .await
    .unwrap();
    ing.push_grpc(
        USER_ID,
        &write_request_single_series(
            &Labels::from_pairs([(METRIC_NAME_LABEL, "foo"), ("l", "3")]),
            samples[0..500_000].to_vec(),
        ),
    )
    .await
    .unwrap();

    let (mut client, shutdown_tx, srv_handle) = start_grpc_server(Arc::clone(&ing)).await;

    let mut grpc_req = Request::new(ingesterpb::QueryRequest {
        start_timestamp_ms: 0,
        end_timestamp_ms: 1_000_001,
        streaming_chunks_batch_size: 64,
        matchers: vec![mk_matcher(
            ingesterpb::MatchType::Equal,
            METRIC_NAME_LABEL,
            "foo",
        )],
    });
    grpc_req
        .metadata_mut()
        .insert(LOWER_ORG_ID_HEADER_NAME, USER_ID.parse().unwrap());

    let mut stream = client.query_stream(grpc_req).await.unwrap().into_inner();
    let first = stream.message().await.unwrap().unwrap();
    assert!(first.is_end_of_series_stream);
    assert_eq!(first.streaming_series.len(), 3);
    assert_eq!(first.streaming_series[0].chunk_count, 834);
    assert_eq!(first.streaming_series[1].chunk_count, 8334);
    assert_eq!(first.streaming_series[2].chunk_count, 4167);
    assert_eq!(labels_from_pairs(&first.streaming_series[0].labels), Labels::from_pairs([(METRIC_NAME_LABEL, "foo"), ("l", "1")]));
    assert_eq!(labels_from_pairs(&first.streaming_series[1].labels), Labels::from_pairs([(METRIC_NAME_LABEL, "foo"), ("l", "2")]));
    assert_eq!(labels_from_pairs(&first.streaming_series[2].labels), Labels::from_pairs([(METRIC_NAME_LABEL, "foo"), ("l", "3")]));

    let mut recv_msgs = 0usize;
    let mut series_count = 0usize;
    let mut total_samples = 0usize;
    while let Some(resp) = stream.message().await.unwrap() {
        assert!(!resp.streaming_series_chunks.is_empty());
        recv_msgs += 1;
        series_count += resp.streaming_series_chunks.len();
        for sc in &resp.streaming_series_chunks {
            for c in &sc.chunks {
                let enc = decode_wire_chunk(c.encoding, &c.data);
                total_samples += enc.num_samples();
            }
        }
    }

    assert_eq!(recv_msgs, 3);
    assert_eq!(series_count, 3);
    assert_eq!(total_samples, 100_000 + 500_000 + 1_000_000);

    let _ = shutdown_tx.send(());
    srv_handle.await.unwrap();
    ing.stop().await.unwrap();
}

// Upstream: pkg/ingester/ingester_test.go::TestIngester_QueryStream_StreamingWithManySeries
#[tokio::test]
async fn test_ingester_query_stream_streaming_with_many_series() {
    let tmp = tempfile::tempdir().unwrap();
    let (cfg, blocks_cfg) = default_ingester_test_config(tmp.path());
    let ing = prepare_ingester_with_blocks_storage(tmp.path(), cfg, blocks_cfg, None, None).await;

    let small = generate_samples(10_000);
    let medium = generate_samples(600_000);
    let large = generate_samples(1_000_000);
    let mut expected_series = Vec::new();

    for idx in 0..201 {
        let samples = if idx % 80 == 0 {
            medium.clone()
        } else if idx == 105 {
            large.clone()
        } else {
            small.clone()
        };
        let l = Labels::from_pairs([(METRIC_NAME_LABEL, "foo"), ("l", &format!("{idx:3}"))]);
        expected_series.push(l.clone());
        ing.push_grpc(USER_ID, &write_request_single_series(&l, samples))
            .await
            .unwrap();
    }

    let (mut client, shutdown_tx, srv_handle) = start_grpc_server(Arc::clone(&ing)).await;
    let mut grpc_req = Request::new(ingesterpb::QueryRequest {
        start_timestamp_ms: 0,
        end_timestamp_ms: 1_000_001,
        streaming_chunks_batch_size: 64,
        matchers: vec![mk_matcher(
            ingesterpb::MatchType::Equal,
            METRIC_NAME_LABEL,
            "foo",
        )],
    });
    grpc_req
        .metadata_mut()
        .insert(LOWER_ORG_ID_HEADER_NAME, USER_ID.parse().unwrap());

    let mut stream = client.query_stream(grpc_req).await.unwrap().into_inner();
    let mut actual_series = Vec::new();
    let mut series_batch_count = 0usize;

    while let Some(resp) = stream.message().await.unwrap() {
        series_batch_count += 1;
        assert!(series_batch_count <= 2);
        match series_batch_count {
            1 => assert_eq!(resp.streaming_series.len(), QUERY_STREAM_BATCH_SIZE),
            2 => assert_eq!(resp.streaming_series.len(), 201 - QUERY_STREAM_BATCH_SIZE),
            _ => unreachable!(),
        }
        for s in &resp.streaming_series {
            actual_series.push(labels_from_pairs(&s.labels));
        }
        if resp.is_end_of_series_stream {
            break;
        }
    }
    assert_eq!(actual_series, expected_series);

    let expected_series_per_chunks_msg = vec![7usize, 64, 9, 7, 18, 1, 54, 7, 34];
    let mut actual_series_per_chunks_msg = Vec::new();
    let mut series_received = 0usize;

    while let Some(resp) = stream.message().await.unwrap() {
        actual_series_per_chunks_msg.push(resp.streaming_series_chunks.len());
        for sc in &resp.streaming_series_chunks {
            assert_eq!(sc.series_index as usize, series_received);
            let exp_samples = if series_received.is_multiple_of(80) {
                medium.len()
            } else if series_received == 105 {
                large.len()
            } else {
                small.len()
            };
            let actual_samples: usize = sc
                .chunks
                .iter()
                .map(|c| decode_wire_chunk(c.encoding, &c.data).num_samples())
                .sum();
            assert_eq!(actual_samples, exp_samples);
            series_received += 1;
        }
    }

    assert_eq!(series_received, expected_series.len());
    assert_eq!(actual_series_per_chunks_msg, expected_series_per_chunks_msg);

    let _ = shutdown_tx.send(());
    srv_handle.await.unwrap();
    ing.stop().await.unwrap();
}

// Upstream: pkg/ingester/ingester_test.go::TestIngester_QueryStream_CounterResets
#[tokio::test]
async fn test_ingester_query_stream_counter_resets() {
    let tmp = tempfile::tempdir().unwrap();
    let (mut cfg, mut blocks_cfg) = default_ingester_test_config(tmp.path());
    blocks_cfg.tsdb.head_compaction_interval = GoDuration::from_hours(1);
    blocks_cfg.tsdb.head_compaction_idle_timeout = GoDuration::from_secs(1);
    cfg.tsdb_config_update_period = GoDuration::from_secs(1);

    let mut limits = default_limits_test_config();
    limits.out_of_order_time_window = ModelDuration::from_mins(30);
    let overrides = Overrides::new(limits);

    let ing = prepare_ingester_with_blocks_storage(
        tmp.path(),
        cfg,
        blocks_cfg.clone(),
        Some(overrides),
        None,
    )
    .await;

    let hist_lbls = Labels::from_pairs([
        (METRIC_NAME_LABEL, "foo"),
        ("series_id", "0"),
        ("type", "histogram"),
    ]);
    for (ts, idx) in [(0i64, 4i64), (2, 6), (4, 8), (1, 2), (3, 3)] {
        ing.push_grpc(USER_ID, &mock_histogram_write_request(&hist_lbls, ts, idx, false))
            .await
            .unwrap();
    }

    let (mut client, shutdown_tx, srv_handle) = start_grpc_server(Arc::clone(&ing)).await;

    async fn run_counter_reset_query(
        c: &mut IngesterClient<tonic::transport::Channel>,
    ) -> (Vec<CounterResetHeader>, Vec<Vec<(i64, Histogram)>>) {
        let mut grpc_req = Request::new(ingesterpb::QueryRequest {
            start_timestamp_ms: 0,
            end_timestamp_ms: 5,
            matchers: vec![mk_matcher(
                ingesterpb::MatchType::Equal,
                METRIC_NAME_LABEL,
                "foo",
            )],
            streaming_chunks_batch_size: 0,
        });
        grpc_req
            .metadata_mut()
            .insert(LOWER_ORG_ID_HEADER_NAME, USER_ID.parse().unwrap());
        let mut stream = c.query_stream(grpc_req).await.unwrap().into_inner();
        let mut recv_msgs = 0usize;
        let mut chunks = Vec::new();
        while let Some(resp) = stream.message().await.unwrap() {
            for sc in resp.streaming_series_chunks {
                chunks.extend(sc.chunks);
            }
            recv_msgs += 1;
        }
        assert_eq!(recv_msgs, 2);
        chunks.sort_by_key(|c| c.start_timestamp_ms);

        let mut headers = Vec::new();
        let mut samples = Vec::new();
        for c in chunks {
            assert_eq!(c.encoding, 5);
            let EncodedChunk::Histogram(hc) = decode_wire_chunk(c.encoding, &c.data) else {
                panic!("expected HistogramChunk");
            };
            headers.push(hc.counter_reset_header());
            let mut it = hc.iter();
            let mut chunk_samples = Vec::new();
            while let Some((ts, h)) = it.next_histogram().unwrap() {
                chunk_samples.push((ts, h));
            }
            samples.push(chunk_samples);
        }
        (headers, samples)
    }

    // Before compaction
    let (act_headers, act_samples) = run_counter_reset_query(&mut client).await;
    assert_eq!(
        act_headers,
        vec![
            CounterResetHeader::UnknownCounterReset,
            CounterResetHeader::CounterReset,
            CounterResetHeader::CounterReset,
        ]
    );
    assert_eq!(
        act_samples,
        vec![
            vec![(
                0,
                generate_test_histogram(4, CounterResetHint::UnknownCounterReset)
            )],
            vec![
                (
                    1,
                    generate_test_histogram(2, CounterResetHint::UnknownCounterReset)
                ),
                (
                    2,
                    generate_test_histogram(6, CounterResetHint::NotCounterReset)
                ),
            ],
            vec![
                (
                    3,
                    generate_test_histogram(3, CounterResetHint::UnknownCounterReset)
                ),
                (
                    4,
                    generate_test_histogram(8, CounterResetHint::NotCounterReset)
                ),
            ],
        ]
    );

    let sleep_ns = (blocks_cfg.tsdb.head_compaction_idle_timeout.as_nanos() as f64
        * (1.0 + COMPACTION_IDLE_TIMEOUT_JITTER)) as u64;
    tokio::time::sleep(Duration::from_nanos(sleep_ns) + Duration::from_millis(20)).await;

    ing.compact_blocks(false, 0, None).await;
    verify_compacted_head(&ing, true);

    let (act_headers, act_samples) = run_counter_reset_query(&mut client).await;
    assert_eq!(
        act_headers,
        vec![
            CounterResetHeader::UnknownCounterReset,
            CounterResetHeader::UnknownCounterReset,
        ]
    );
    assert_eq!(
        act_samples,
        vec![
            vec![
                (
                    0,
                    generate_test_histogram(4, CounterResetHint::UnknownCounterReset)
                ),
                (
                    2,
                    generate_test_histogram(6, CounterResetHint::NotCounterReset)
                ),
                (
                    4,
                    generate_test_histogram(8, CounterResetHint::NotCounterReset)
                ),
            ],
            vec![
                (
                    1,
                    generate_test_histogram(2, CounterResetHint::UnknownCounterReset)
                ),
                (
                    3,
                    generate_test_histogram(3, CounterResetHint::NotCounterReset)
                ),
            ],
        ]
    );

    let _ = shutdown_tx.send(());
    srv_handle.await.unwrap();
    ing.stop().await.unwrap();
}

// Upstream: pkg/ingester/ingester_test.go::TestIngester_OpenExistingTSDBOnStartup
#[tokio::test]
async fn test_ingester_open_existing_tsdb_on_startup() {
    struct Case {
        name: &'static str,
        wal_replay_concurrency: i64,
        setup: fn(&Path),
        check: fn(&Ingester),
        expected_err: Option<&'static str>,
    }

    let cases = vec![
        Case {
            name: "should not load TSDB if the user directory is empty",
            wal_replay_concurrency: 10,
            setup: |dir| {
                std::fs::create_dir(dir.join("user0")).unwrap();
            },
            check: |i| {
                assert!(i.get_tsdb("user0").is_none());
            },
            expected_err: None,
        },
        Case {
            name: "should not load any TSDB if the root directory is empty",
            wal_replay_concurrency: 10,
            setup: |_| {},
            check: |i| {
                assert_eq!(i.tsdbs.len(), 0);
            },
            expected_err: None,
        },
        Case {
            name: "should not load any TSDB is the root directory is missing",
            wal_replay_concurrency: 10,
            setup: |dir| {
                std::fs::remove_dir(dir).unwrap();
            },
            check: |i| {
                assert_eq!(i.tsdbs.len(), 0);
            },
            expected_err: None,
        },
        Case {
            name: "should load TSDB for any non-empty user directory",
            wal_replay_concurrency: 10,
            setup: |dir| {
                for u in ["user0", "user1"] {
                    std::fs::create_dir_all(dir.join(u).join("dummy")).unwrap();
                }
                std::fs::create_dir(dir.join("user2")).unwrap();
            },
            check: |i| {
                assert_eq!(i.tsdbs.len(), 2);
                assert!(i.get_tsdb("user0").is_some());
                assert!(i.get_tsdb("user1").is_some());
                assert!(i.get_tsdb("user2").is_none());
            },
            expected_err: None,
        },
        Case {
            name: "should load all TSDBs on walReplayConcurrency < number of TSDBs",
            wal_replay_concurrency: 2,
            setup: |dir| {
                for u in ["user0", "user1", "user2", "user3", "user4"] {
                    std::fs::create_dir_all(dir.join(u).join("dummy")).unwrap();
                }
            },
            check: |i| {
                assert_eq!(i.tsdbs.len(), 5);
                for u in ["user0", "user1", "user2", "user3", "user4"] {
                    let db = i.get_tsdb(u).unwrap();
                    assert_eq!(db.db.head().options().wal_replay_concurrency, 2);
                }
            },
            expected_err: None,
        },
        Case {
            name: "should load all TSDBs on walReplayConcurrency > number of TSDBs",
            wal_replay_concurrency: 10,
            setup: |dir| {
                for u in ["user0", "user1", "user2", "user3", "user4"] {
                    std::fs::create_dir_all(dir.join(u).join("dummy")).unwrap();
                }
            },
            check: |i| {
                assert_eq!(i.tsdbs.len(), 5);
                for u in ["user0", "user1", "user2", "user3", "user4"] {
                    let db = i.get_tsdb(u).unwrap();
                    assert_eq!(db.db.head().options().wal_replay_concurrency, 10);
                }
            },
            expected_err: None,
        },
        Case {
            name: "should load all TSDBs on number of TSDBs > maxTSDBOpenWithoutConcurrency",
            wal_replay_concurrency: 2,
            setup: |dir| {
                for idx in 0..11 {
                    std::fs::create_dir_all(dir.join(format!("user{idx}")).join("dummy")).unwrap();
                }
            },
            check: |i| {
                assert_eq!(i.tsdbs.len(), 11);
                for idx in 0..11 {
                    let db = i.get_tsdb(&format!("user{idx}")).unwrap();
                    assert_eq!(db.db.head().options().wal_replay_concurrency, 1);
                }
            },
            expected_err: None,
        },
        Case {
            name: "should fail and rollback if an error occur while loading a TSDB on walReplayConcurrency > number of TSDBs",
            wal_replay_concurrency: 10,
            setup: |dir| {
                std::fs::create_dir_all(dir.join("user0").join("wal")).unwrap();
                std::fs::create_dir_all(dir.join("user0").join("chunks_head")).unwrap();
                std::fs::write(dir.join("user0").join("chunks_head").join("00000001"), []).unwrap();
                std::fs::write(dir.join("user0").join("chunks_head").join("00000002"), []).unwrap();
                std::fs::create_dir_all(dir.join("user1").join("dummy")).unwrap();
            },
            check: |i| {
                assert_eq!(i.tsdbs.len(), 0);
                assert!(i.get_tsdb("user0").is_none());
                assert!(i.get_tsdb("user1").is_none());
            },
            expected_err: Some("unable to open TSDB for user user0"),
        },
        Case {
            name: "should fail and rollback if an error occur while loading a TSDB on walReplayConcurrency < number of TSDBs",
            wal_replay_concurrency: 2,
            setup: |dir| {
                for u in ["user0", "user1", "user3", "user4"] {
                    std::fs::create_dir_all(dir.join(u).join("dummy")).unwrap();
                }
                std::fs::create_dir_all(dir.join("user2").join("wal")).unwrap();
                std::fs::create_dir_all(dir.join("user2").join("chunks_head")).unwrap();
                std::fs::write(dir.join("user2").join("chunks_head").join("00000001"), []).unwrap();
                std::fs::write(dir.join("user2").join("chunks_head").join("00000002"), []).unwrap();
            },
            check: |i| {
                assert!(i.get_tsdb("user0").is_none());
                assert!(i.get_tsdb("user1").is_none());
                assert!(i.get_tsdb("user2").is_none());
            },
            expected_err: Some("unable to open TSDB for user user2"),
        },
        Case {
            name: "should load all TSDBs and honor DeprecatedMaxTSDBOpeningConcurrencyOnStartup when walReplayConcurrency = 0",
            wal_replay_concurrency: 0,
            setup: |dir| {
                for u in ["user0", "user1", "user2", "user3", "user4"] {
                    std::fs::create_dir_all(dir.join(u).join("dummy")).unwrap();
                }
            },
            check: |i| {
                assert_eq!(i.tsdbs.len(), 5);
                for u in ["user0", "user1", "user2", "user3", "user4"] {
                    let db = i.get_tsdb(u).unwrap();
                    assert_ne!(db.db.head().options().wal_replay_concurrency, 0);
                }
            },
            expected_err: None,
        },
    ];

    // Also verify get_open_tsdbs_concurrency_config directly
    assert_eq!(get_open_tsdbs_concurrency_config(2, 5), (1, 2));
    assert_eq!(get_open_tsdbs_concurrency_config(10, 5), (1, 10));
    assert_eq!(get_open_tsdbs_concurrency_config(2, 11), (2, 1));
    assert_eq!(get_open_tsdbs_concurrency_config(0, 5), (10, 0));

    for case in cases {
        let tmp = tempfile::tempdir().unwrap();
        let tsdb_dir = tmp.path().join("tsdb");
        std::fs::create_dir_all(&tsdb_dir).unwrap();
        (case.setup)(&tsdb_dir);

        let (cfg, mut blocks_cfg) = default_ingester_test_config(tmp.path());
        blocks_cfg.tsdb.dir = tsdb_dir.to_string_lossy().into_owned();
        blocks_cfg.tsdb.wal_replay_concurrency = case.wal_replay_concurrency;

        let ing = Ingester::new(
            cfg,
            blocks_cfg,
            IngestStorageConfig::default(),
            Overrides::new(default_limits_test_config()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();

        let start_res = ing.start().await;
        match case.expected_err {
            None => start_res.expect(case.name),
            Some(exp) => {
                let err = start_res.expect_err(case.name);
                assert!(
                    err.to_string().contains(exp),
                    "case '{}': expected error containing '{exp}', got '{err}'",
                    case.name
                );
            }
        }
        (case.check)(&ing);
        let _ = ing.stop().await;
    }
}

struct MockUploader {
    calls: AtomicUsize,
    delay: Duration,
}

impl MockUploader {
    fn new(delay: Duration) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            delay,
        }
    }
}

impl BlockShipper for MockUploader {
    fn sync(&self) -> Pin<Box<dyn Future<Output = Result<usize, ShipperError>> + Send + '_>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::AcqRel);
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            Ok(0)
        })
    }
}

// Upstream: pkg/ingester/ingester_test.go::TestIngester_shipBlocks
#[tokio::test]
async fn test_ingester_ship_blocks() {
    let tmp = tempfile::tempdir().unwrap();
    let (cfg, mut blocks_cfg) = default_ingester_test_config(tmp.path());
    blocks_cfg.tsdb.ship_concurrency = 2;

    let ing = prepare_ingester_with_blocks_storage(tmp.path(), cfg, blocks_cfg, None, None).await;

    let mut mocks = Vec::new();
    for uid in ["user-1", "user-2", "user-3"] {
        let user_db = ing.get_or_create_tsdb(uid).unwrap();
        let m = Arc::new(MockUploader::new(Duration::ZERO));
        *user_db.shipper.write() = Some(Arc::clone(&m) as Arc<dyn BlockShipper>);
        mocks.push(m);
    }

    ing.ship_blocks(None).await;

    for m in &mocks {
        assert_eq!(m.calls.load(Ordering::Acquire), 1);
    }

    ing.stop().await.unwrap();
}

// Upstream: pkg/ingester/ingester_test.go::TestIngester_dontShipBlocksWhenTenantDeletionMarkerIsPresent
#[tokio::test]
async fn test_ingester_dont_ship_blocks_when_tenant_deletion_marker_is_present() {
    let tmp = tempfile::tempdir().unwrap();
    let (cfg, mut blocks_cfg) = default_ingester_test_config(tmp.path());
    blocks_cfg.tsdb.ship_concurrency = 2;

    let bucket = Arc::new(BucketClient::new_in_memory("test"));
    let ing = Ingester::new(
        cfg,
        blocks_cfg,
        IngestStorageConfig::default(),
        Overrides::new(default_limits_test_config()),
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .unwrap();
    ing.set_bucket(Some(Arc::clone(&bucket)));
    ing.start().await.unwrap();

    push_single_sample_with_metadata(&ing).await;
    assert_eq!(ing.series_count.load(Ordering::Relaxed), 1);
    ing.compact_blocks(true, i64::MAX, None).await;
    assert_eq!(ing.series_count.load(Ordering::Relaxed), 0);
    ing.ship_blocks(None).await;

    let num_objects = bucket.list_all("").await.unwrap().len();
    assert!(num_objects > 0);

    write_tenant_deletion_mark(&bucket, USER_ID, &TenantDeletionMark::now())
        .await
        .unwrap();
    let expected_objects = num_objects + 1;

    let db = ing.get_tsdb(USER_ID).unwrap();
    db.last_deletion_mark_check_sec.store(0, Ordering::Release);

    push_single_sample_with_metadata(&ing).await;
    assert_eq!(ing.series_count.load(Ordering::Relaxed), 1);
    ing.compact_blocks(true, i64::MAX, None).await;
    assert_eq!(ing.series_count.load(Ordering::Relaxed), 0);
    ing.ship_blocks(None).await;

    let num_objects_after = bucket.list_all("").await.unwrap().len();
    assert_eq!(num_objects_after, expected_objects);
    assert_eq!(
        ing.close_and_delete_user_tsdb_if_idle(USER_ID, Duration::from_secs(3600))
            .await,
        TsdbCloseCheckResult::TenantMarkedForDeletion
    );

    ing.stop().await.unwrap();
}

// Upstream: pkg/ingester/ingester_test.go::TestIngester_seriesCountIsCorrectAfterClosingTSDBForDeletedTenant
#[tokio::test]
async fn test_ingester_series_count_is_correct_after_closing_tsdb_for_deleted_tenant() {
    let tmp = tempfile::tempdir().unwrap();
    let (cfg, mut blocks_cfg) = default_ingester_test_config(tmp.path());
    blocks_cfg.tsdb.ship_concurrency = 2;

    let bucket = Arc::new(BucketClient::new_in_memory("test"));
    write_tenant_deletion_mark(&bucket, USER_ID, &TenantDeletionMark::now())
        .await
        .unwrap();

    let ing = Ingester::new(
        cfg,
        blocks_cfg,
        IngestStorageConfig::default(),
        Overrides::new(default_limits_test_config()),
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .unwrap();
    ing.set_bucket(Some(bucket));
    ing.start().await.unwrap();

    push_single_sample_with_metadata(&ing).await;
    assert_eq!(ing.series_count.load(Ordering::Relaxed), 1);

    ing.ship_blocks(None).await;

    let db = ing.get_tsdb(USER_ID).unwrap();
    assert!(db.deletion_mark_found.load(Ordering::Acquire));
    assert_eq!(db.db.head().num_series(), 1);
    assert_eq!(
        ing.close_and_delete_user_tsdb_if_idle(USER_ID, Duration::from_secs(3600))
            .await,
        TsdbCloseCheckResult::TenantMarkedForDeletion
    );
    assert_eq!(ing.series_count.load(Ordering::Relaxed), 0);

    ing.stop().await.unwrap();
}

// Upstream: pkg/ingester/ingester_test.go::TestIngester_closeAndDeleteUserTSDBIfIdle_shouldNotCloseTSDBIfShippingIsInProgress
#[tokio::test]
async fn test_ingester_close_and_delete_user_tsdb_if_idle_should_not_close_if_shipping_in_progress() {
    let tmp = tempfile::tempdir().unwrap();
    let (cfg, mut blocks_cfg) = default_ingester_test_config(tmp.path());
    blocks_cfg.tsdb.ship_concurrency = 2;
    blocks_cfg.tsdb.close_idle_tsdb_timeout = GoDuration::from_nanos(1);

    let ing = prepare_ingester_with_blocks_storage(tmp.path(), cfg, blocks_cfg, None, None).await;

    let db = ing.get_or_create_tsdb(USER_ID).unwrap();
    let mock_shipper = Arc::new(MockUploader::new(Duration::from_millis(600)));
    *db.shipper.write() = Some(mock_shipper);

    write_shipper_meta_file(
        db.db.dir(),
        &ShipperMeta {
            version: SHIPPER_META_VERSION_1,
            shipped: BTreeMap::new(),
        },
    )
    .unwrap();

    let ing_ship = Arc::clone(&ing);
    let ship_handle = tokio::spawn(async move {
        ing_ship.ship_blocks(None).await;
    });

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while db.state() != TsdbState::ActiveShipping && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(db.state(), TsdbState::ActiveShipping);

    assert_eq!(
        ing.close_and_delete_user_tsdb_if_idle(USER_ID, Duration::from_nanos(1))
            .await,
        TsdbCloseCheckResult::NotActive
    );

    ship_handle.await.unwrap();
    ing.stop().await.unwrap();
}

// Upstream: pkg/ingester/ingester_test.go::TestIngester_closingAndOpeningTsdbConcurrently
#[tokio::test]
async fn test_ingester_closing_and_opening_tsdb_concurrently() {
    let tmp = tempfile::tempdir().unwrap();
    let (cfg, mut blocks_cfg) = default_ingester_test_config(tmp.path());
    blocks_cfg.tsdb.close_idle_tsdb_timeout = GoDuration::ZERO;

    let ing = prepare_ingester_with_blocks_storage(tmp.path(), cfg, blocks_cfg, None, None).await;
    let _ = ing.get_or_create_tsdb(USER_ID).unwrap();

    let stop_flag = Arc::new(AtomicBool::new(false));
    let ing_open = Arc::clone(&ing);
    let flag_open = Arc::clone(&stop_flag);
    let open_task = tokio::spawn(async move {
        while !flag_open.load(Ordering::Acquire) {
            ing_open.get_or_create_tsdb(USER_ID).unwrap();
        }
    });

    for _ in 0..500 {
        let _ = ing
            .close_and_delete_user_tsdb_if_idle(USER_ID, Duration::ZERO)
            .await;
    }

    stop_flag.store(true, Ordering::Release);
    open_task.await.unwrap();
    ing.stop().await.unwrap();
}

// Upstream: pkg/ingester/ingester_test.go::TestIngester_idleCloseEmptyTSDB
#[tokio::test]
async fn test_ingester_idle_close_empty_tsdb() {
    let tmp = tempfile::tempdir().unwrap();
    let (cfg, mut blocks_cfg) = default_ingester_test_config(tmp.path());
    blocks_cfg.tsdb.ship_interval = GoDuration::from_mins(1);
    blocks_cfg.tsdb.head_compaction_interval = GoDuration::from_mins(1);
    blocks_cfg.tsdb.close_idle_tsdb_timeout = GoDuration::ZERO;

    let ing = prepare_ingester_with_blocks_storage(tmp.path(), cfg, blocks_cfg, None, None).await;

    let db = ing.get_or_create_tsdb(USER_ID).unwrap();
    assert_eq!(db.user_id, USER_ID);

    ing.compact_blocks(true, i64::MAX, None).await;
    ing.ship_blocks(None).await;

    assert_eq!(
        ing.close_and_delete_user_tsdb_if_idle(USER_ID, Duration::ZERO)
            .await,
        TsdbCloseCheckResult::IdleClosed
    );
    assert!(ing.get_tsdb(USER_ID).is_none());

    let db2 = ing.get_or_create_tsdb(USER_ID).unwrap();
    assert_eq!(db2.user_id, USER_ID);

    ing.stop().await.unwrap();
}

// Upstream: pkg/ingester/ingester_test.go::TestIngester_invalidSamplesDontChangeLastUpdateTime
#[tokio::test]
async fn test_ingester_invalid_samples_dont_change_last_update_time() {
    let tmp = tempfile::tempdir().unwrap();
    let (cfg, blocks_cfg) = default_ingester_test_config(tmp.path());
    let ing = prepare_ingester_with_blocks_storage(tmp.path(), cfg, blocks_cfg, None, None).await;

    let sample_ts = now_ms();
    let lbls = Labels::from_pairs([(METRIC_NAME_LABEL, "test")]);
    ing.push_grpc(USER_ID, &mock_write_request(&lbls, 0.0, sample_ts))
        .await
        .unwrap();

    let db = ing.get_tsdb(USER_ID).unwrap();
    let last_update = db.last_update();

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Duplicate sample with different value -> error
    let err = ing
        .push_grpc(USER_ID, &mock_write_request(&lbls, 1.0, sample_ts))
        .await;
    assert!(err.is_err());

    assert_eq!(db.last_update(), last_update);

    ing.stop().await.unwrap();
}

// Upstream: pkg/ingester/ingester_test.go::TestIngester_flushing
#[tokio::test]
async fn test_ingester_flushing() {
    // 1. should flush blocks on shutdown when enabled through the configuration
    {
        let tmp = tempfile::tempdir().unwrap();
        let (cfg, mut blocks_cfg) = default_ingester_test_config(tmp.path());
        blocks_cfg.tsdb.ship_concurrency = 1;
        blocks_cfg.tsdb.ship_interval = GoDuration::from_mins(1);
        blocks_cfg.tsdb.flush_blocks_on_shutdown = true;
        blocks_cfg.tsdb.keep_user_tsdb_open_on_shutdown = true;

        let reg = Registry::new();
        let ing =
            prepare_ingester_with_blocks_storage(tmp.path(), cfg, blocks_cfg, None, Some(&reg))
                .await;

        push_single_sample_with_metadata(&ing).await;
        assert_eq!(
            gather_metric_value(&reg, "cortex_ingester_shipper_uploads_total", &[]).unwrap(),
            0.0
        );
        assert_eq!(
            gather_metric_value(
                &reg,
                "cortex_ingester_shipper_last_successful_upload_timestamp_seconds",
                &[]
            )
            .unwrap(),
            0.0
        );

        ing.stop().await.unwrap();
        verify_compacted_head(&ing, true);
        assert_eq!(
            gather_metric_value(&reg, "cortex_ingester_shipper_uploads_total", &[]).unwrap(),
            1.0
        );
        verify_shipper_last_successful_upload_time_metric(&reg, now_ms() / 1000);
        ing.close_all_tsdbs().await;
    }

    // 2. should flush blocks on shutdown when enabled through the prepare shutdown API endpoint
    {
        let tmp = tempfile::tempdir().unwrap();
        let (mut cfg, mut blocks_cfg) = default_ingester_test_config(tmp.path());
        cfg.ring.unregister_on_shutdown = false;
        blocks_cfg.tsdb.ship_concurrency = 1;
        blocks_cfg.tsdb.ship_interval = GoDuration::from_mins(1);
        blocks_cfg.tsdb.flush_blocks_on_shutdown = false;
        blocks_cfg.tsdb.keep_user_tsdb_open_on_shutdown = true;

        let reg = Registry::new();
        let ing =
            prepare_ingester_with_blocks_storage(tmp.path(), cfg, blocks_cfg, None, Some(&reg))
                .await;

        push_single_sample_with_metadata(&ing).await;
        assert_eq!(
            gather_metric_value(&reg, "cortex_ingester_shipper_uploads_total", &[]).unwrap(),
            0.0
        );

        let (code, body) = http_call(&ing, Method::GET, "/ingester/prepare-shutdown").await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(body, "unset\n");

        let (code, _) = http_call(&ing, Method::POST, "/ingester/prepare-shutdown").await;
        assert_eq!(code, StatusCode::NO_CONTENT);
        assert_eq!(
            gather_metric_value(
                &reg,
                "cortex_ingester_prepare_shutdown_requested",
                &[]
            )
            .unwrap(),
            1.0
        );

        let (code, body) = http_call(&ing, Method::GET, "/ingester/prepare-shutdown").await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(body, "set\n");

        let (code, _) = http_call(&ing, Method::DELETE, "/ingester/prepare-shutdown").await;
        assert_eq!(code, StatusCode::NO_CONTENT);
        assert_eq!(
            gather_metric_value(
                &reg,
                "cortex_ingester_prepare_shutdown_requested",
                &[]
            )
            .unwrap(),
            0.0
        );

        let (code, _) = http_call(&ing, Method::POST, "/ingester/prepare-shutdown").await;
        assert_eq!(code, StatusCode::NO_CONTENT);

        ing.stop().await.unwrap();
        verify_compacted_head(&ing, true);
        assert_eq!(
            gather_metric_value(&reg, "cortex_ingester_shipper_uploads_total", &[]).unwrap(),
            1.0
        );
        verify_shipper_last_successful_upload_time_metric(&reg, now_ms() / 1000);

        let (code, _) = http_call(&ing, Method::POST, "/ingester/prepare-shutdown").await;
        assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE);

        ing.close_all_tsdbs().await;
    }

    // 3. should flush blocks when the shutdown API endpoint is called
    {
        let tmp = tempfile::tempdir().unwrap();
        let (mut cfg, mut blocks_cfg) = default_ingester_test_config(tmp.path());
        cfg.ring.unregister_on_shutdown = false;
        blocks_cfg.tsdb.ship_concurrency = 1;
        blocks_cfg.tsdb.ship_interval = GoDuration::from_mins(1);
        blocks_cfg.tsdb.flush_blocks_on_shutdown = false;
        blocks_cfg.tsdb.keep_user_tsdb_open_on_shutdown = true;

        let reg = Registry::new();
        let ing =
            prepare_ingester_with_blocks_storage(tmp.path(), cfg, blocks_cfg, None, Some(&reg))
                .await;

        push_single_sample_with_metadata(&ing).await;
        let (code, _) = http_call(&ing, Method::POST, "/ingester/shutdown").await;
        assert_eq!(code, StatusCode::NO_CONTENT);

        verify_compacted_head(&ing, true);
        assert_eq!(
            gather_metric_value(&reg, "cortex_ingester_shipper_uploads_total", &[]).unwrap(),
            1.0
        );
        verify_shipper_last_successful_upload_time_metric(&reg, now_ms() / 1000);
        ing.close_all_tsdbs().await;
    }

    // 4. should flush blocks for all tenants when the flush API endpoint is called without tenants list
    {
        let tmp = tempfile::tempdir().unwrap();
        let (cfg, mut blocks_cfg) = default_ingester_test_config(tmp.path());
        blocks_cfg.tsdb.ship_concurrency = 1;
        blocks_cfg.tsdb.ship_interval = GoDuration::from_mins(1);
        blocks_cfg.tsdb.flush_blocks_on_shutdown = false;

        let reg = Registry::new();
        let ing =
            prepare_ingester_with_blocks_storage(tmp.path(), cfg, blocks_cfg, None, Some(&reg))
                .await;

        push_single_sample_with_metadata(&ing).await;
        let (code, _) = http_call(&ing, Method::POST, "/ingester/flush?wait=true").await;
        assert_eq!(code, StatusCode::NO_CONTENT);

        verify_compacted_head(&ing, true);
        assert_eq!(
            gather_metric_value(&reg, "cortex_ingester_shipper_uploads_total", &[]).unwrap(),
            1.0
        );
        verify_shipper_last_successful_upload_time_metric(&reg, now_ms() / 1000);
        ing.stop().await.unwrap();
    }

    // 5. should flush blocks for requested tenants when the flush API endpoint is called with tenants list
    {
        let tmp = tempfile::tempdir().unwrap();
        let (cfg, mut blocks_cfg) = default_ingester_test_config(tmp.path());
        blocks_cfg.tsdb.ship_concurrency = 1;
        blocks_cfg.tsdb.ship_interval = GoDuration::from_mins(1);
        blocks_cfg.tsdb.flush_blocks_on_shutdown = false;

        let reg = Registry::new();
        let ing =
            prepare_ingester_with_blocks_storage(tmp.path(), cfg, blocks_cfg, None, Some(&reg))
                .await;

        push_single_sample_with_metadata(&ing).await;

        let uri1 = format!(
            "/ingester/flush?wait=true&{TENANT_PARAM}=unknown-user&{TENANT_PARAM}=another-unknown-user"
        );
        let (code, _) = http_call(&ing, Method::POST, &uri1).await;
        assert_eq!(code, StatusCode::NO_CONTENT);
        assert_eq!(
            gather_metric_value(&reg, "cortex_ingester_shipper_uploads_total", &[]).unwrap(),
            0.0
        );
        verify_compacted_head(&ing, false);

        let uri2 = format!(
            "/ingester/flush?wait=true&{TENANT_PARAM}=different-user&{TENANT_PARAM}={USER_ID}&{TENANT_PARAM}=yet-another-user"
        );
        let (code, _) = http_call(&ing, Method::POST, &uri2).await;
        assert_eq!(code, StatusCode::NO_CONTENT);
        verify_compacted_head(&ing, true);
        assert_eq!(
            gather_metric_value(&reg, "cortex_ingester_shipper_uploads_total", &[]).unwrap(),
            1.0
        );
        verify_shipper_last_successful_upload_time_metric(&reg, now_ms() / 1000);
        ing.stop().await.unwrap();
    }

    // 6. should flush blocks spanning multiple days with flush API endpoint is called
    {
        let tmp = tempfile::tempdir().unwrap();
        let (cfg, mut blocks_cfg) = default_ingester_test_config(tmp.path());
        blocks_cfg.tsdb.ship_concurrency = 1;
        blocks_cfg.tsdb.ship_interval = GoDuration::from_mins(1);
        blocks_cfg.tsdb.flush_blocks_on_shutdown = false;

        let reg = Registry::new();
        let ing =
            prepare_ingester_with_blocks_storage(tmp.path(), cfg, blocks_cfg, None, Some(&reg))
                .await;

        let hr = 3_600_000i64;
        push_single_sample_at_time(&ing, 23 * hr).await;
        push_single_sample_at_time(&ing, 24 * hr - 1).await;
        push_single_sample_at_time(&ing, 24 * hr + 1).await;
        push_single_sample_at_time(&ing, 25 * hr).await;
        push_single_sample_at_time(&ing, 50 * hr).await;

        let (code, _) = http_call(&ing, Method::POST, "/ingester/flush?wait=true").await;
        assert_eq!(code, StatusCode::NO_CONTENT);

        verify_compacted_head(&ing, true);
        assert_eq!(
            gather_metric_value(&reg, "cortex_ingester_shipper_uploads_total", &[]).unwrap(),
            3.0
        );
        verify_shipper_last_successful_upload_time_metric(&reg, now_ms() / 1000);

        let db = ing.get_tsdb(USER_ID).unwrap();
        let blocks = db.db.blocks();
        assert_eq!(blocks.len(), 3);
        assert_eq!(blocks[0].min_time(), 23 * hr);
        assert_eq!(blocks[0].max_time(), 24 * hr);
        assert_eq!(blocks[1].min_time(), 24 * hr + 1);
        assert_eq!(blocks[1].max_time(), 26 * hr);
        assert_eq!(blocks[2].max_time(), 50 * hr + 1);

        ing.stop().await.unwrap();
    }

    // 7. should not allow to flush blocks with flush API endpoint if ingester is not in Running state
    {
        let tmp = tempfile::tempdir().unwrap();
        let (cfg, mut blocks_cfg) = default_ingester_test_config(tmp.path());
        blocks_cfg.tsdb.ship_concurrency = 1;
        blocks_cfg.tsdb.ship_interval = GoDuration::from_mins(1);
        blocks_cfg.tsdb.flush_blocks_on_shutdown = false;

        let ing = prepare_ingester_with_blocks_storage(tmp.path(), cfg, blocks_cfg, None, None).await;
        ing.stop().await.unwrap();

        let (code, body) = http_call(&ing, Method::POST, "/ingester/flush?wait=true").await;
        assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            body,
            IngesterError::Unavailable {
                state: State::Terminated
            }
            .to_string()
        );
    }
}

// Upstream: pkg/ingester/ingester_test.go::Test_Ingester_UserStats
#[tokio::test]
async fn test_ingester_user_stats() {
    let tmp = tempfile::tempdir().unwrap();
    let (cfg, blocks_cfg) = default_ingester_test_config(tmp.path());
    let reg = Registry::new();
    let ing =
        prepare_ingester_with_blocks_storage(tmp.path(), cfg, blocks_cfg, None, Some(&reg)).await;

    let series = [
        (
            Labels::from_pairs([(METRIC_NAME_LABEL, "test_1"), ("status", "200"), ("route", "get_user")]),
            1.0,
            100000i64,
        ),
        (
            Labels::from_pairs([(METRIC_NAME_LABEL, "test_1"), ("status", "500"), ("route", "get_user")]),
            1.0,
            110000,
        ),
        (Labels::from_pairs([(METRIC_NAME_LABEL, "test_2")]), 2.0, 200000),
    ];

    for (lbls, val, ts) in &series {
        ing.push_grpc("test", &mock_write_request(lbls, *val, *ts))
            .await
            .unwrap();
    }

    for entry in ing.tsdbs.iter() {
        entry.value().ingested_api_samples.tick();
        entry.value().ingested_rule_samples.tick();
    }

    let md = md_with_org("test");
    let res = ing
        .handle_user_stats(&md, "test", ingesterpb::UserStatsRequest::default())
        .await
        .unwrap();
    assert!((res.api_ingestion_rate - 0.2).abs() < 0.0001);
    assert!((res.rule_ingestion_rate - 0.0).abs() < 0.0001);
    assert_eq!(res.num_series, 3);

    let res2 = ing
        .handle_user_stats(
            &md,
            "test",
            ingesterpb::UserStatsRequest {
                count_method: ingesterpb::CountMethod::InMemory as i32,
            },
        )
        .await
        .unwrap();
    assert!((res2.api_ingestion_rate - 0.2).abs() < 0.0001);
    assert!((res2.rule_ingestion_rate - 0.0).abs() < 0.0001);
    assert_eq!(res2.num_series, 3);

    ing.stop().await.unwrap();
}

// Upstream: pkg/ingester/ingester_test.go::Test_Ingester_AllUserStats
#[tokio::test]
async fn test_ingester_all_user_stats() {
    let tmp = tempfile::tempdir().unwrap();
    let (cfg, blocks_cfg) = default_ingester_test_config(tmp.path());
    let ing = prepare_ingester_with_blocks_storage(tmp.path(), cfg, blocks_cfg, None, None).await;

    let series = [
        (
            "user-1",
            Labels::from_pairs([
                (METRIC_NAME_LABEL, "test_1_1"),
                ("status", "200"),
                ("route", "get_user"),
            ]),
            1.0,
            100000i64,
        ),
        (
            "user-1",
            Labels::from_pairs([
                (METRIC_NAME_LABEL, "test_1_1"),
                ("status", "500"),
                ("route", "get_user"),
            ]),
            1.0,
            110000,
        ),
        (
            "user-1",
            Labels::from_pairs([(METRIC_NAME_LABEL, "test_1_2")]),
            2.0,
            200000,
        ),
        (
            "user-2",
            Labels::from_pairs([(METRIC_NAME_LABEL, "test_2_1")]),
            2.0,
            200000,
        ),
        (
            "user-2",
            Labels::from_pairs([(METRIC_NAME_LABEL, "test_2_2")]),
            2.0,
            200000,
        ),
    ];

    for (uid, lbls, val, ts) in &series {
        ing.push_grpc(uid, &mock_write_request(lbls, *val, *ts))
            .await
            .unwrap();
    }

    for entry in ing.tsdbs.iter() {
        entry.value().ingested_api_samples.tick();
        entry.value().ingested_rule_samples.tick();
    }

    let res = ing
        .handle_all_user_stats(&MetadataMap::new(), ingesterpb::UserStatsRequest::default())
        .await
        .unwrap();
    assert_eq!(res.stats.len(), 2);

    let by_user: HashMap<String, ingesterpb::UserStatsResponse> = res
        .stats
        .into_iter()
        .map(|s| (s.user_id, s.data.unwrap()))
        .collect();
    let u1 = &by_user["user-1"];
    assert!((u1.ingestion_rate - 0.2).abs() < 1e-9);
    assert_eq!(u1.num_series, 3);
    assert!((u1.api_ingestion_rate - 0.2).abs() < 1e-9);
    assert!((u1.rule_ingestion_rate - 0.0).abs() < 1e-9);

    let u2 = &by_user["user-2"];
    assert!((u2.ingestion_rate - 0.13333333333333333).abs() < 1e-9);
    assert_eq!(u2.num_series, 2);
    assert!((u2.api_ingestion_rate - 0.13333333333333333).abs() < 1e-9);
    assert!((u2.rule_ingestion_rate - 0.0).abs() < 1e-9);

    ing.stop().await.unwrap();
}

// Upstream: pkg/ingester/ingester_test.go::TestIngesterCompactIdleBlock
#[tokio::test]
async fn test_ingester_compact_idle_block() {
    let tmp = tempfile::tempdir().unwrap();
    let (cfg, mut blocks_cfg) = default_ingester_test_config(tmp.path());
    blocks_cfg.tsdb.ship_concurrency = 1;
    blocks_cfg.tsdb.head_compaction_interval = GoDuration::from_hours(1);
    blocks_cfg.tsdb.head_compaction_idle_timeout = GoDuration::from_secs(1);

    let reg = Registry::new();
    let ing = prepare_ingester_with_blocks_storage(
        tmp.path(),
        cfg,
        blocks_cfg.clone(),
        None,
        Some(&reg),
    )
    .await;

    push_single_sample_with_metadata(&ing).await;

    ing.compact_blocks(false, 0, None).await;
    verify_compacted_head(&ing, false);
    assert_eq!(
        gather_metric_value(
            &reg,
            "cortex_ingester_memory_series_created_total",
            &[("user", "1")]
        ),
        Some(1.0)
    );
    assert_eq!(
        gather_metric_value(
            &reg,
            "cortex_ingester_memory_series_removed_total",
            &[("user", "1")]
        ),
        Some(0.0)
    );
    assert_eq!(
        gather_metric_value(&reg, "cortex_ingester_memory_users", &[]),
        Some(1.0)
    );

    let sleep_ns = (blocks_cfg.tsdb.head_compaction_idle_timeout.as_nanos() as f64
        * (1.0 + COMPACTION_IDLE_TIMEOUT_JITTER)) as u64;
    tokio::time::sleep(Duration::from_nanos(sleep_ns) + Duration::from_millis(20)).await;

    ing.compact_blocks(false, 0, None).await;
    verify_compacted_head(&ing, true);
    assert_eq!(
        gather_metric_value(
            &reg,
            "cortex_ingester_memory_series_created_total",
            &[("user", "1")]
        ),
        Some(1.0)
    );
    assert_eq!(
        gather_metric_value(
            &reg,
            "cortex_ingester_memory_series_removed_total",
            &[("user", "1")]
        ),
        Some(1.0)
    );
    assert_eq!(
        gather_metric_value(&reg, "cortex_ingester_memory_users", &[]),
        Some(1.0)
    );

    push_single_sample_with_metadata(&ing).await;
    verify_compacted_head(&ing, false);
    assert_eq!(
        gather_metric_value(
            &reg,
            "cortex_ingester_memory_series_created_total",
            &[("user", "1")]
        ),
        Some(2.0)
    );
    assert_eq!(
        gather_metric_value(
            &reg,
            "cortex_ingester_memory_series_removed_total",
            &[("user", "1")]
        ),
        Some(1.0)
    );
    assert_eq!(
        gather_metric_value(&reg, "cortex_ingester_memory_users", &[]),
        Some(1.0)
    );

    ing.stop().await.unwrap();
}

// Upstream: pkg/ingester/ingester_test.go::TestIngesterCompactAndCloseIdleTSDB
#[tokio::test]
async fn test_ingester_compact_and_close_idle_tsdb() {
    let tmp = tempfile::tempdir().unwrap();
    let (cfg, mut blocks_cfg) = default_ingester_test_config(tmp.path());
    blocks_cfg.tsdb.ship_interval = GoDuration::from_secs(1);
    blocks_cfg.tsdb.ship_concurrency = 1;
    blocks_cfg.tsdb.head_compaction_idle_timeout = GoDuration::from_secs(1);
    blocks_cfg.tsdb.head_compaction_interval = GoDuration::from_millis(100);
    blocks_cfg.tsdb.close_idle_tsdb_timeout = GoDuration::from_secs(1);
    blocks_cfg.tsdb.close_idle_tsdb_interval = GoDuration::from_millis(100);

    let reg = Registry::new();
    let ing =
        prepare_ingester_with_blocks_storage(tmp.path(), cfg, blocks_cfg, None, Some(&reg)).await;

    push_single_sample_with_metadata(&ing).await;
    ing.update_active_series_metrics(SystemTime::now());
    assert_eq!(ing.series_count.load(Ordering::Relaxed), 1);

    assert_eq!(
        gather_metric_value(
            &reg,
            "cortex_ingester_memory_series_created_total",
            &[("user", "1")]
        ),
        Some(1.0)
    );
    assert_eq!(
        gather_metric_value(
            &reg,
            "cortex_ingester_memory_series_removed_total",
            &[("user", "1")]
        ),
        Some(0.0)
    );
    assert_eq!(
        gather_metric_value(&reg, "cortex_ingester_memory_users", &[]),
        Some(1.0)
    );
    assert_eq!(
        gather_metric_value(&reg, "cortex_ingester_active_series", &[("user", "1")]),
        Some(1.0)
    );
    assert_eq!(
        gather_metric_value(&reg, "cortex_ingester_memory_metadata", &[]),
        Some(1.0)
    );
    assert_eq!(
        gather_metric_value(
            &reg,
            "cortex_ingester_memory_metadata_created_total",
            &[("user", "1")]
        ),
        Some(1.0)
    );

    // Wait until TSDB has been closed and removed.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while !ing.tsdbs.is_empty() && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(ing.tsdbs.len(), 0);

    assert!(
        gather_metric_value(
            &reg,
            "cortex_ingester_idle_tsdb_checks_total",
            &[("result", "idle_closed")]
        )
        .unwrap_or(0.0)
            > 0.0
    );
    ing.update_active_series_metrics(SystemTime::now());
    assert_eq!(ing.series_count.load(Ordering::Relaxed), 0);

    // User disappeared from metrics
    assert_eq!(
        gather_metric_count(&reg, "cortex_ingester_memory_series_created_total"),
        0
    );
    assert_eq!(
        gather_metric_count(&reg, "cortex_ingester_memory_series_removed_total"),
        0
    );
    assert_eq!(
        gather_metric_value(&reg, "cortex_ingester_memory_users", &[]),
        Some(0.0)
    );
    assert_eq!(
        gather_metric_count(&reg, "cortex_ingester_active_series"),
        0
    );
    assert_eq!(
        gather_metric_value(&reg, "cortex_ingester_memory_metadata", &[]),
        Some(0.0)
    );

    // Pushing another sample recreates TSDB
    push_single_sample_with_metadata(&ing).await;
    ing.update_active_series_metrics(SystemTime::now());

    assert_eq!(
        gather_metric_value(
            &reg,
            "cortex_ingester_memory_series_created_total",
            &[("user", "1")]
        ),
        Some(1.0)
    );
    assert_eq!(
        gather_metric_value(
            &reg,
            "cortex_ingester_memory_series_removed_total",
            &[("user", "1")]
        ),
        Some(0.0)
    );
    assert_eq!(
        gather_metric_value(&reg, "cortex_ingester_memory_users", &[]),
        Some(1.0)
    );
    assert_eq!(
        gather_metric_value(&reg, "cortex_ingester_active_series", &[("user", "1")]),
        Some(1.0)
    );
    assert_eq!(
        gather_metric_value(&reg, "cortex_ingester_memory_metadata", &[]),
        Some(1.0)
    );
    assert_eq!(
        gather_metric_value(
            &reg,
            "cortex_ingester_memory_metadata_created_total",
            &[("user", "1")]
        ),
        Some(1.0)
    );

    ing.stop().await.unwrap();
}

// Upstream: pkg/ingester/ingester_test.go::TestIngesterCloseIdleTSDBWhenShippingDisabled
#[tokio::test]
async fn test_ingester_close_idle_tsdb_when_shipping_disabled() {
    let tmp = tempfile::tempdir().unwrap();
    let (cfg, mut blocks_cfg) = default_ingester_test_config(tmp.path());
    blocks_cfg.tsdb.head_compaction_idle_timeout = GoDuration::from_secs(1);
    blocks_cfg.tsdb.head_compaction_interval = GoDuration::from_millis(100);
    blocks_cfg.tsdb.close_idle_tsdb_timeout = GoDuration::from_secs(1);
    blocks_cfg.tsdb.close_idle_tsdb_interval = GoDuration::from_millis(100);
    blocks_cfg.tsdb.ship_interval = GoDuration::ZERO;
    blocks_cfg.tsdb.close_idle_tsdb_when_shipping_disabled = true;

    let reg = Registry::new();
    let ing =
        prepare_ingester_with_blocks_storage(tmp.path(), cfg, blocks_cfg, None, Some(&reg)).await;

    push_single_sample_with_metadata(&ing).await;
    assert_eq!(ing.series_count.load(Ordering::Relaxed), 1);

    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while !ing.tsdbs.is_empty() && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(ing.tsdbs.len(), 0);

    assert!(
        gather_metric_value(
            &reg,
            "cortex_ingester_idle_tsdb_checks_total",
            &[("result", "idle_closed")]
        )
        .unwrap_or(0.0)
            > 0.0
    );
    ing.update_active_series_metrics(SystemTime::now());
    assert_eq!(ing.series_count.load(Ordering::Relaxed), 0);

    ing.stop().await.unwrap();
}

// Upstream: pkg/ingester/ingester_test.go::TestHeadCompactionOnStartup
#[tokio::test]
async fn test_head_compaction_on_startup() {
    let tmp = tempfile::tempdir().unwrap();
    let tsdb_dir = tmp.path().join("tsdb");
    let user_dir = tsdb_dir.join(USER_ID);
    std::fs::create_dir_all(&user_dir).unwrap();

    let chunk_range = 2 * 3_600_000i64;
    {
        let mut opts = TsdbOptions::default();
        opts.head.chunk_range_ms = chunk_range;
        let db = Tsdb::open(&user_dir, opts).unwrap();

        let pairs = [("n", "v")];
        for i in 0..12i64 {
            let mut app = db.appender();
            let r = app
                .append_float(None, &pairs, i * chunk_range + 1, 9.99)
                .unwrap();
            let _ = app
                .append_float(Some(r), &pairs, (i + 1) * chunk_range, 9.99)
                .unwrap();
            app.commit().unwrap();
        }
        let head = db.head();
        assert!(head.max_time() - head.min_time() > 23 * 3_600_000);
        assert_eq!(db.blocks().len(), 0);
        db.close().unwrap();
    }

    let (cfg, mut blocks_cfg) = default_ingester_test_config(tmp.path());
    blocks_cfg.tsdb.dir = tsdb_dir.to_string_lossy().into_owned();
    blocks_cfg.tsdb.retention_period = GoDuration::from_hours(48);
    blocks_cfg.tsdb.head_compaction_idle_timeout = GoDuration::ZERO;

    let ing = prepare_ingester_with_blocks_storage(tmp.path(), cfg, blocks_cfg, None, None).await;
    let db = ing.get_tsdb(USER_ID).unwrap();
    let h = db.db.head();
    assert!(h.max_time() - h.min_time() <= 2 * 3_600_000);
    assert_eq!(db.db.blocks().len(), 11);

    ing.stop().await.unwrap();
}

// Upstream: pkg/ingester/ingester_test.go::TestIngester_CloseTSDBsOnShutdown
#[tokio::test]
async fn test_ingester_close_tsdbs_on_shutdown() {
    let tmp = tempfile::tempdir().unwrap();
    let (cfg, blocks_cfg) = default_ingester_test_config(tmp.path());
    let ing = prepare_ingester_with_blocks_storage(tmp.path(), cfg, blocks_cfg, None, None).await;

    push_single_sample_with_metadata(&ing).await;
    assert!(ing.get_tsdb(USER_ID).is_some());

    ing.stop().await.unwrap();
    assert!(ing.get_tsdb(USER_ID).is_none());
}

// Upstream: pkg/ingester/ingester_test.go::TestIngester_closeAllTSDB_waitsForInFlightAppends
#[tokio::test]
async fn test_ingester_close_all_tsdb_waits_for_in_flight_appends() {
    let tmp = tempfile::tempdir().unwrap();
    let (cfg, blocks_cfg) = default_ingester_test_config(tmp.path());
    let ing = prepare_ingester_with_blocks_storage(tmp.path(), cfg, blocks_cfg, None, None).await;

    push_single_sample_with_metadata(&ing).await;
    let db = ing.get_tsdb(USER_ID).unwrap();

    let guard = db.acquire_append_lock(0).unwrap();

    let ing_close = Arc::clone(&ing);
    let close_handle = tokio::spawn(async move {
        ing_close.close_all_tsdbs().await;
    });

    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    while db.state() != TsdbState::Closing && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(db.state(), TsdbState::Closing);

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!close_handle.is_finished(), "closeAllTSDB should wait for in-flight appends");

    drop(guard);
    close_handle.await.unwrap();
    assert!(ing.get_tsdb(USER_ID).is_none());

    ing.stop().await.unwrap();
}

// Upstream: pkg/ingester/ingester_test.go::TestIngesterNotDeleteUnshippedBlocks
#[tokio::test]
async fn test_ingester_not_delete_unshipped_blocks() {
    let chunk_range_ms = 2 * 3_600_000i64;
    let tmp = tempfile::tempdir().unwrap();
    let (cfg, mut blocks_cfg) = default_ingester_test_config(tmp.path());
    blocks_cfg.tsdb.block_ranges_period = vec![GoDuration::from_hours(2)];
    blocks_cfg.tsdb.retention_period = GoDuration::from_millis(1);

    let reg = Registry::new();
    let ing =
        prepare_ingester_with_blocks_storage(tmp.path(), cfg, blocks_cfg, None, Some(&reg)).await;

    assert_eq!(
        gather_metric_value(
            &reg,
            "cortex_ingester_oldest_unshipped_block_timestamp_seconds",
            &[]
        ),
        Some(0.0)
    );

    let lbls = Labels::from_pairs([(METRIC_NAME_LABEL, "test")]);
    for j in 0..5i64 {
        ing.push_grpc(USER_ID, &mock_write_request(&lbls, 0.0, j * chunk_range_ms))
            .await
            .unwrap();
    }

    let db = ing.get_tsdb(USER_ID).unwrap();
    db.compact().unwrap();

    let old_blocks = db.db.blocks();
    assert_eq!(old_blocks.len(), 3);
    assert_eq!(
        gather_metric_value(
            &reg,
            "cortex_ingester_oldest_unshipped_block_timestamp_seconds",
            &[]
        ),
        Some((old_blocks[0].ulid().timestamp_ms() / 1000) as f64)
    );

    // Mark second block shipped
    write_shipper_meta_file(
        db.db.dir(),
        &ShipperMeta {
            version: SHIPPER_META_VERSION_1,
            shipped: BTreeMap::from([(old_blocks[1].ulid(), ModelTime::now())]),
        },
    )
    .unwrap();
    db.update_cached_shipped_blocks().unwrap();

    tokio::time::sleep(Duration::from_millis(5)).await;
    ing.push_grpc(USER_ID, &mock_write_request(&lbls, 0.0, 5 * chunk_range_ms))
        .await
        .unwrap();
    db.compact().unwrap();

    let new_blocks = db.db.blocks();
    assert_eq!(new_blocks.len(), 3);
    assert_eq!(new_blocks[0].ulid(), old_blocks[0].ulid());
    assert_eq!(new_blocks[1].ulid(), old_blocks[2].ulid());
    assert_ne!(new_blocks[2].ulid(), old_blocks[1].ulid());
    assert_eq!(
        gather_metric_value(
            &reg,
            "cortex_ingester_oldest_unshipped_block_timestamp_seconds",
            &[]
        ),
        Some((new_blocks[0].ulid().timestamp_ms() / 1000) as f64)
    );

    // Ship 2 more blocks
    write_shipper_meta_file(
        db.db.dir(),
        &ShipperMeta {
            version: SHIPPER_META_VERSION_1,
            shipped: BTreeMap::from([
                (old_blocks[1].ulid(), ModelTime::now()),
                (new_blocks[0].ulid(), ModelTime::now()),
                (new_blocks[1].ulid(), ModelTime::now()),
            ]),
        },
    )
    .unwrap();
    db.update_cached_shipped_blocks().unwrap();

    tokio::time::sleep(Duration::from_millis(5)).await;
    ing.push_grpc(USER_ID, &mock_write_request(&lbls, 0.0, 6 * chunk_range_ms))
        .await
        .unwrap();
    db.compact().unwrap();

    let new_blocks2 = db.db.blocks();
    assert_eq!(new_blocks2.len(), 2);
    assert_eq!(new_blocks2[0].ulid(), new_blocks[2].ulid());
    let old_ids: HashSet<Ulid> = old_blocks.iter().map(|b| b.ulid()).collect();
    assert!(!old_ids.contains(&new_blocks2[1].ulid()));
    assert_eq!(
        gather_metric_value(
            &reg,
            "cortex_ingester_oldest_unshipped_block_timestamp_seconds",
            &[]
        ),
        Some((new_blocks2[0].ulid().timestamp_ms() / 1000) as f64)
    );

    ing.stop().await.unwrap();
}

// Upstream: pkg/ingester/ingester_test.go::TestIngesterNotDeleteShippedBlocksUntilRetentionExpires
#[tokio::test]
async fn test_ingester_not_delete_shipped_blocks_until_retention_expires() {
    let chunk_range_ms = 2 * 3_600_000i64;
    let tmp = tempfile::tempdir().unwrap();
    let (cfg, mut blocks_cfg) = default_ingester_test_config(tmp.path());
    blocks_cfg.tsdb.block_ranges_period = vec![GoDuration::from_hours(2)];
    blocks_cfg.tsdb.retention_period = GoDuration::from_hours(1);

    let reg = Registry::new();
    let ing =
        prepare_ingester_with_blocks_storage(tmp.path(), cfg, blocks_cfg, None, Some(&reg)).await;

    assert_eq!(
        gather_metric_value(
            &reg,
            "cortex_ingester_oldest_unshipped_block_timestamp_seconds",
            &[]
        ),
        Some(0.0)
    );

    let lbls = Labels::from_pairs([(METRIC_NAME_LABEL, "test")]);
    for j in 0..5i64 {
        ing.push_grpc(USER_ID, &mock_write_request(&lbls, 0.0, j * chunk_range_ms))
            .await
            .unwrap();
    }

    let db = ing.get_tsdb(USER_ID).unwrap();
    db.compact().unwrap();

    let old_blocks = db.db.blocks();
    assert_eq!(old_blocks.len(), 3);
    assert_eq!(
        gather_metric_value(
            &reg,
            "cortex_ingester_oldest_unshipped_block_timestamp_seconds",
            &[]
        ),
        Some((old_blocks[0].ulid().timestamp_ms() / 1000) as f64)
    );

    let now = now_ms();
    write_shipper_meta_file(
        db.db.dir(),
        &ShipperMeta {
            version: SHIPPER_META_VERSION_1,
            shipped: BTreeMap::from([
                (old_blocks[0].ulid(), ModelTime(now - 2 * 3_600_000)),
                (old_blocks[1].ulid(), ModelTime(now - 30 * 60_000)),
            ]),
        },
    )
    .unwrap();
    db.update_cached_shipped_blocks().unwrap();

    ing.push_grpc(USER_ID, &mock_write_request(&lbls, 0.0, 5 * chunk_range_ms))
        .await
        .unwrap();
    db.compact().unwrap();

    let new_blocks = db.db.blocks();
    assert_eq!(new_blocks.len(), 3);
    assert_eq!(new_blocks[0].ulid(), old_blocks[1].ulid());
    assert_eq!(new_blocks[1].ulid(), old_blocks[2].ulid());
    assert_eq!(
        gather_metric_value(
            &reg,
            "cortex_ingester_oldest_unshipped_block_timestamp_seconds",
            &[]
        ),
        Some((new_blocks[1].ulid().timestamp_ms() / 1000) as f64)
    );

    ing.stop().await.unwrap();
}

// Upstream: pkg/ingester/ingester_test.go::TestIngesterWithShippingDisabledDeletesBlocksOnlyAfterRetentionExpires
#[tokio::test]
async fn test_ingester_with_shipping_disabled_deletes_blocks_only_after_retention_expires() {
    let chunk_range_ms = 2 * 3_600_000i64;
    let tmp = tempfile::tempdir().unwrap();
    let (cfg, mut blocks_cfg) = default_ingester_test_config(tmp.path());
    blocks_cfg.tsdb.block_ranges_period = vec![GoDuration::from_hours(2)];
    blocks_cfg.tsdb.ship_interval = GoDuration::ZERO;
    blocks_cfg.tsdb.retention_period = GoDuration::from_secs(2);

    let reg = Registry::new();
    let ing =
        prepare_ingester_with_blocks_storage(tmp.path(), cfg, blocks_cfg, None, Some(&reg)).await;

    assert_eq!(
        gather_metric_value(&reg, "cortex_ingester_tsdb_compactions_total", &[]),
        Some(0.0)
    );

    let lbls = Labels::from_pairs([(METRIC_NAME_LABEL, "test")]);
    for j in 0..5i64 {
        ing.push_grpc(USER_ID, &mock_write_request(&lbls, 0.0, j * chunk_range_ms))
            .await
            .unwrap();
    }

    let db = ing.get_tsdb(USER_ID).unwrap();
    db.compact().unwrap();
    let old_blocks = db.db.blocks();
    assert_eq!(old_blocks.len(), 3);

    tokio::time::sleep(Duration::from_secs(2) + Duration::from_millis(50)).await;

    ing.push_grpc(USER_ID, &mock_write_request(&lbls, 0.0, 5 * chunk_range_ms))
        .await
        .unwrap();
    db.compact().unwrap();

    assert_eq!(
        gather_metric_value(&reg, "cortex_ingester_tsdb_compactions_total", &[]),
        Some(4.0)
    );

    let new_blocks = db.db.blocks();
    assert_eq!(new_blocks.len(), 1);
    let old_ids: HashSet<Ulid> = old_blocks.iter().map(|b| b.ulid()).collect();
    assert!(!old_ids.contains(&new_blocks[0].ulid()));

    ing.stop().await.unwrap();
}

// Upstream: pkg/ingester/ingester_test.go::TestIngesterPushErrorDuringForcedCompaction
#[tokio::test]
async fn test_ingester_push_error_during_forced_compaction() {
    let tmp = tempfile::tempdir().unwrap();
    let (cfg, blocks_cfg) = default_ingester_test_config(tmp.path());
    let ing = prepare_ingester_with_blocks_storage(tmp.path(), cfg, blocks_cfg, None, None).await;

    push_single_sample_with_metadata(&ing).await;

    let db = ing.get_tsdb(USER_ID).unwrap();
    let (ok, _) = db.change_state_to_forced_compaction(TsdbState::Active, i64::MAX);
    assert!(ok);

    let req = mock_write_request(&Labels::from_pairs([(METRIC_NAME_LABEL, "test")]), 0.0, now_ms());
    let err = ing.push_grpc(USER_ID, &req).await.unwrap_err();
    let status = err.map_push_error_to_status();
    assert_eq!(status.code(), Code::Internal);
    assert!(status
        .message()
        .contains("TSDB Head forced compaction in progress"));

    let (ok, _) = db.change_state(TsdbState::ForceCompacting, TsdbState::Active);
    assert!(ok);
    push_single_sample_with_metadata(&ing).await;

    ing.stop().await.unwrap();
}

// Upstream: pkg/ingester/ingester_test.go::TestIngesterNoFlushWithInFlightRequest
#[tokio::test]
async fn test_ingester_no_flush_with_in_flight_request() {
    let tmp = tempfile::tempdir().unwrap();
    let (cfg, blocks_cfg) = default_ingester_test_config(tmp.path());
    let reg = Registry::new();
    let ing =
        prepare_ingester_with_blocks_storage(tmp.path(), cfg, blocks_cfg, None, Some(&reg)).await;

    for _ in 0..5 {
        push_single_sample_with_metadata(&ing).await;
    }

    let db = ing.get_tsdb(USER_ID).unwrap();
    let lock_guard = db.acquire_append_lock(0).unwrap();

    let (code, _) = http_call(&ing, Method::POST, "/ingester/flush").await;
    assert_eq!(code, StatusCode::NO_CONTENT);

    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        gather_metric_value(&reg, "cortex_ingester_tsdb_compactions_total", &[]),
        Some(0.0)
    );

    drop(lock_guard);

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        if ing
            .get_tsdb(USER_ID)
            .is_some_and(|d| d.db.head().num_series() == 0)
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(ing.get_tsdb(USER_ID).unwrap().db.head().num_series(), 0);
    assert_eq!(
        gather_metric_value(&reg, "cortex_ingester_tsdb_compactions_total", &[]),
        Some(1.0)
    );

    ing.stop().await.unwrap();
}
