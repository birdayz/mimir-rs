//! Ported from github.com/grafana/mimir/pkg/ingester/ingester_test.go (TestIngester_Push, lines 1932..3749 & 4600..4747).

mod common;

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mimir_common::errors::{
    GlobalErrorId, EXEMPLAR_SERIES_MISSING, EXEMPLAR_TOO_FAR_IN_FUTURE, EXEMPLAR_TOO_FAR_IN_PAST,
    NATIVE_HISTOGRAM_COUNT_MISMATCH, NATIVE_HISTOGRAM_COUNT_NOT_BIG_ENOUGH,
    NATIVE_HISTOGRAM_CUSTOM_BUCKETS_INFINITE, NATIVE_HISTOGRAM_CUSTOM_BUCKETS_INVALID,
    NATIVE_HISTOGRAM_CUSTOM_BUCKETS_MISMATCH, NATIVE_HISTOGRAM_NEGATIVE_BUCKET_COUNT,
    NATIVE_HISTOGRAM_SPANS_BUCKETS_MISMATCH, NATIVE_HISTOGRAM_SPAN_NEGATIVE_OFFSET,
    SAMPLE_DUPLICATE_TIMESTAMP, SAMPLE_OUT_OF_ORDER, SAMPLE_TIMESTAMP_TOO_OLD,
    SAMPLE_TOO_FAR_IN_FUTURE,
};
use mimir_config::{LimitsConfig, ModelDuration};
use mimir_ingester::Ingester;
use mimir_proto::cortexpb;
use mimir_proto::grpcutilpb;
use mimir_proto::ingesterpb;
use mimir_proto::prost::Message;
use mimir_proto::zerocopy::{
    decode_write_request, from_write_request_to_rw2, DecodeOptions, WriteRequestScratch,
};
use mimir_tsdb::chunkenc::{
    CounterResetHint, EncodedChunk, Encoding, Histogram, HistogramSpan, CUSTOM_BUCKETS_SCHEMA,
};
use mimir_tsdb::compact::{decode_chunk_samples, SampleValue};
use mimir_tsdb::labels::Labels;
use prometheus::proto::MetricType as PromMetricType;
use prometheus::Registry;
use tonic::Code;

use crate::common::{
    default_ingester_test_config, prepare_ingester_with_blocks_storage, start_and_wait_healthy,
};

const USER_ID: &str = "test";
const METRIC_NAME_LABEL: &str = "__name__";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PushMode {
    Rw1,
    Rw2,
}

#[derive(Clone, Debug, PartialEq)]
struct ExpectedSeries {
    labels: Labels,
    samples: Vec<(i64, f64)>,
    histograms: Vec<(i64, Histogram)>,
}

#[derive(Clone, Debug)]
struct ExpectedExemplarSeries {
    labels: Vec<cortexpb::LabelPair>,
    exemplars: Vec<cortexpb::Exemplar>,
}

#[derive(Clone, Debug)]
struct ExpectedError {
    code: Code,
    cause: cortexpb::ErrorCause,
    soft: bool,
    rejected_samples: i64,
    err_id: Option<GlobalErrorId>,
    contains: Vec<String>,
}

struct PushTestCase {
    name: &'static str,
    reqs: Vec<cortexpb::WriteRequest>,
    expected_err: Option<ExpectedError>,
    expected_ingested: Vec<ExpectedSeries>,
    expected_metadata_ingested: Vec<cortexpb::MetricMetadata>,
    expected_exemplars_ingested: Vec<ExpectedExemplarSeries>,
    expected_metrics: String,
    additional_metrics: Vec<&'static str>,
    disable_active_series: bool,
    max_exemplars: i64,
    max_metadata_per_user: i64,
    max_metadata_per_metric: i64,
    native_histograms: bool,
    allow_ooo: bool,
    ignore_ooo_exemplars: bool,
    past_grace_period: Duration,
}

impl Default for PushTestCase {
    fn default() -> Self {
        Self {
            name: "",
            reqs: Vec::new(),
            expected_err: None,
            expected_ingested: Vec::new(),
            expected_metadata_ingested: Vec::new(),
            expected_exemplars_ingested: Vec::new(),
            expected_metrics: String::new(),
            additional_metrics: Vec::new(),
            disable_active_series: false,
            max_exemplars: 0,
            max_metadata_per_user: 0,
            max_metadata_per_metric: 0,
            native_histograms: false,
            allow_ooo: false,
            ignore_ooo_exemplars: false,
            past_grace_period: Duration::ZERO,
        }
    }
}

fn test_label_pairs() -> Vec<cortexpb::LabelPair> {
    vec![cortexpb::LabelPair::from_strs(METRIC_NAME_LABEL, "test")]
}

fn test_labels() -> Labels {
    Labels::from_pairs([(METRIC_NAME_LABEL, "test")])
}

fn named_labels(name: &str) -> Labels {
    Labels::from_pairs([(METRIC_NAME_LABEL, name)])
}

fn named_label_pairs(name: &str) -> Vec<cortexpb::LabelPair> {
    vec![cortexpb::LabelPair::from_strs(METRIC_NAME_LABEL, name)]
}

fn generate_test_histogram(i: i64) -> Histogram {
    Histogram {
        counter_reset_hint: CounterResetHint::UnknownCounterReset,
        schema: 1,
        zero_threshold: 0.001,
        zero_count: (2 + i) as u64,
        count: (12 + i * 9) as u64,
        sum: 18.4 * (i + 1) as f64,
        positive_spans: vec![
            HistogramSpan { offset: 0, length: 2 },
            HistogramSpan { offset: 1, length: 2 },
        ],
        negative_spans: vec![
            HistogramSpan { offset: 0, length: 2 },
            HistogramSpan { offset: 1, length: 2 },
        ],
        positive_buckets: vec![i + 1, 1, -1, 0],
        negative_buckets: vec![i + 1, 1, -1, 0],
        custom_values: Vec::new(),
    }
}

fn generate_test_custom_buckets_histogram(i: i64) -> Histogram {
    Histogram {
        counter_reset_hint: CounterResetHint::UnknownCounterReset,
        schema: CUSTOM_BUCKETS_SCHEMA,
        zero_threshold: 0.0,
        zero_count: 0,
        count: (5 + i * 4) as u64,
        sum: 18.4 * (i + 1) as f64,
        positive_spans: vec![
            HistogramSpan { offset: 0, length: 2 },
            HistogramSpan { offset: 1, length: 2 },
        ],
        negative_spans: Vec::new(),
        positive_buckets: vec![i + 1, 1, -1, 0],
        negative_buckets: Vec::new(),
        custom_values: vec![0.0, 1.0, 2.0, 3.0, 4.0],
    }
}

fn histogram_to_proto(ts: i64, h: &Histogram) -> cortexpb::Histogram {
    cortexpb::Histogram {
        count: Some(cortexpb::histogram::Count::CountInt(h.count)),
        zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountInt(h.zero_count)),
        zero_threshold: h.zero_threshold,
        sum: h.sum,
        schema: h.schema,
        positive_spans: h
            .positive_spans
            .iter()
            .map(|s| cortexpb::BucketSpan {
                offset: s.offset,
                length: s.length,
            })
            .collect(),
        negative_spans: h
            .negative_spans
            .iter()
            .map(|s| cortexpb::BucketSpan {
                offset: s.offset,
                length: s.length,
            })
            .collect(),
        positive_deltas: h.positive_buckets.clone(),
        positive_counts: Vec::new(),
        negative_deltas: h.negative_buckets.clone(),
        negative_counts: Vec::new(),
        custom_values: h.custom_values.clone(),
        reset_hint: h.counter_reset_hint as i32,
        timestamp: ts,
    }
}

fn write_req_samples(
    labels: Vec<cortexpb::LabelPair>,
    samples: Vec<cortexpb::Sample>,
) -> cortexpb::WriteRequest {
    cortexpb::WriteRequest {
        timeseries: vec![cortexpb::TimeSeries {
            labels,
            samples,
            ..Default::default()
        }],
        source: cortexpb::write_request::SourceEnum::Api as i32,
        ..Default::default()
    }
}

fn write_req_histogram(
    labels: Vec<cortexpb::LabelPair>,
    histograms: Vec<cortexpb::Histogram>,
    exemplars: Vec<cortexpb::Exemplar>,
) -> cortexpb::WriteRequest {
    cortexpb::WriteRequest {
        timeseries: vec![cortexpb::TimeSeries {
            labels,
            histograms,
            exemplars,
            ..Default::default()
        }],
        source: cortexpb::write_request::SourceEnum::Api as i32,
        ..Default::default()
    }
}

fn make_exemplar(trace_id: &str, ts_ms: i64, value: f64) -> cortexpb::Exemplar {
    cortexpb::Exemplar {
        labels: vec![cortexpb::LabelPair::from_strs("traceID", trace_id)],
        value,
        timestamp_ms: ts_ms,
    }
}

fn md_with_org(user_id: &str) -> tonic::metadata::MetadataMap {
    let mut md = tonic::metadata::MetadataMap::new();
    md.insert(
        mimir_common::tenant::LOWER_ORG_ID_HEADER_NAME,
        tonic::metadata::MetadataValue::try_from(user_id).unwrap(),
    );
    md
}

fn fmt_ts_secs(ts_ms: i64) -> String {
    let v = (ts_ms as f64) / 1000.0;
    format!("{v}")
}

fn histograms_equal_ignoring_hint(a: &Histogram, b: &Histogram) -> bool {
    a.prom_equals(b)
}

type SeriesSamples = (Vec<(i64, f64)>, Vec<(i64, Histogram)>);
type ParsedMetricsMap = BTreeMap<String, Vec<(BTreeMap<String, String>, f64)>>;

fn wire_to_tsdb_encoding(wire: i32) -> Encoding {
    match wire {
        4 => Encoding::Xor,
        5 => Encoding::Histogram,
        6 => Encoding::FloatHistogram,
        7 => Encoding::Xor2,
        8 => Encoding::HistogramSt,
        9 => Encoding::FloatHistogramSt,
        other => panic!("unexpected wire chunk encoding: {other}"),
    }
}

async fn query_ingested(ing: &Ingester, user_id: &str) -> Vec<ExpectedSeries> {
    let md = md_with_org(user_id);
    let responses = ing
        .handle_query_stream(
            &md,
            user_id,
            ingesterpb::QueryRequest {
                start_timestamp_ms: i64::MIN,
                end_timestamp_ms: i64::MAX,
                matchers: vec![ingesterpb::LabelMatcher {
                    r#type: ingesterpb::MatchType::RegexMatch as i32,
                    name: METRIC_NAME_LABEL.to_string(),
                    value: ".*".to_string(),
                }],
                ..Default::default()
            },
        )
        .await
        .expect("query_stream");

    let mut series_list: Vec<(Labels, SeriesSamples)> = Vec::new();
    for resp in responses {
        for s in resp.streaming_series {
            let lbls = Labels::from_pairs(
                s.labels
                    .iter()
                    .map(|lp| (lp.name_str().unwrap_or(""), lp.value_str().unwrap_or(""))),
            );
            series_list.push((lbls, (Vec::new(), Vec::new())));
        }
        for sc in resp.streaming_series_chunks {
            let entry = &mut series_list[sc.series_index as usize];
            for chunk in sc.chunks {
                let enc = wire_to_tsdb_encoding(chunk.encoding);
                let decoded =
                    EncodedChunk::from_bytes(enc, chunk.data.to_vec()).expect("decode chunk");
                let samples = decode_chunk_samples(&decoded).expect("decode samples");
                for s in samples {
                    match s.value {
                        SampleValue::Float(v) => entry.1 .0.push((s.t, v)),
                        SampleValue::Histogram(h) => entry.1 .1.push((s.t, h)),
                        SampleValue::FloatHistogram(_) => panic!("unexpected float histogram"),
                    }
                }
            }
        }
    }

    let mut by_labels: BTreeMap<Labels, SeriesSamples> = BTreeMap::new();
    for (lbls, (samples, histograms)) in series_list {
        let entry = by_labels.entry(lbls).or_default();
        entry.0.extend(samples);
        entry.1.extend(histograms);
    }

    by_labels
        .into_iter()
        .map(|(labels, (mut samples, mut histograms))| {
            samples.sort_by_key(|x| x.0);
            histograms.sort_by_key(|x| x.0);
            ExpectedSeries {
                labels,
                samples,
                histograms,
            }
        })
        .collect()
}

fn parse_expected_metrics(text: &str) -> ParsedMetricsMap {
    let mut out: ParsedMetricsMap = BTreeMap::new();
    for raw_line in text.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (metric_part, val_str) = if let Some(brace_idx) = line.find('{') {
            let end_brace = line.rfind('}').expect("closing brace");
            let name = line[..brace_idx].trim();
            let labels_str = &line[brace_idx + 1..end_brace];
            let val = line[end_brace + 1..].trim();
            let mut lbls = BTreeMap::new();
            if !labels_str.trim().is_empty() {
                for part in labels_str.split(',') {
                    let part = part.trim();
                    if part.is_empty() {
                        continue;
                    }
                    let (k, v) = part.split_once('=').expect("label k=v");
                    let v = v.trim().trim_matches('"');
                    lbls.insert(k.trim().to_string(), v.to_string());
                }
            }
            ((name.to_string(), lbls), val)
        } else {
            let mut parts = line.split_whitespace();
            let name = parts.next().expect("metric name").to_string();
            let val = parts.next().expect("metric value");
            ((name, BTreeMap::new()), val)
        };
        let value: f64 = val_str.parse().expect("parse metric value");
        out.entry(metric_part.0).or_default().push((metric_part.1, value));
    }
    out
}

fn gather_checked_metrics(reg: &Registry, checked_names: &[&str]) -> ParsedMetricsMap {
    let mut out: ParsedMetricsMap = BTreeMap::new();
    let checked_set: std::collections::HashSet<&str> = checked_names.iter().copied().collect();
    for mf in reg.gather() {
        let name = mf.get_name();
        if !checked_set.contains(name) {
            continue;
        }
        let mut series = Vec::new();
        for m in mf.get_metric() {
            let mut lbls = BTreeMap::new();
            for lp in m.get_label() {
                lbls.insert(lp.get_name().to_string(), lp.get_value().to_string());
            }
            let val = match mf.get_field_type() {
                PromMetricType::COUNTER => m.get_counter().get_value(),
                PromMetricType::GAUGE => m.get_gauge().get_value(),
                PromMetricType::UNTYPED => m.get_untyped().get_value(),
                _ => continue,
            };
            series.push((lbls, val));
        }
        if !series.is_empty() {
            series.sort_by(|a, b| a.0.cmp(&b.0));
            out.insert(name.to_string(), series);
        }
    }
    out
}

fn assert_metrics_match(reg: &Registry, expected_text: &str, checked_names: &[&str], label: &str) {
    let mut expected = parse_expected_metrics(expected_text);
    for vals in expected.values_mut() {
        vals.sort_by(|a, b| a.0.cmp(&b.0));
    }
    let actual = gather_checked_metrics(reg, checked_names);

    for name in checked_names {
        let exp_series = expected.get(*name);
        let act_series = actual.get(*name);
        match (exp_series, act_series) {
            (None, None) => {}
            (Some(e), Some(a)) => {
                assert_eq!(
                    e.len(),
                    a.len(),
                    "[{label}] metric {name} series count mismatch:\nexpected: {e:?}\nactual:   {a:?}"
                );
                for (es, as_) in e.iter().zip(a.iter()) {
                    assert_eq!(
                        es.0, as_.0,
                        "[{label}] metric {name} labels mismatch"
                    );
                    assert!(
                        (es.1 - as_.1).abs() <= 1e-9,
                        "[{label}] metric {name} value mismatch for {:?}: expected {}, got {}",
                        es.0,
                        es.1,
                        as_.1
                    );
                }
            }
            (e, a) => {
                panic!("[{label}] metric {name} presence mismatch:\nexpected: {e:?}\nactual:   {a:?}");
            }
        }
    }
}

async fn run_push_case(tc: &PushTestCase, mode: PushMode) {
    let metric_names: Vec<&'static str> = vec![
        "cortex_ingester_ingested_samples_total",
        "cortex_ingester_ingested_samples_failures_total",
        "cortex_ingester_memory_series",
        "cortex_ingester_memory_users",
        "cortex_ingester_memory_series_created_total",
        "cortex_ingester_memory_series_removed_total",
        "cortex_discarded_samples_total",
        "cortex_ingester_active_series",
        "cortex_ingester_active_native_histogram_series",
        "cortex_ingester_active_native_histogram_buckets",
        "cortex_ingester_tsdb_head_min_timestamp_seconds",
        "cortex_ingester_tsdb_head_max_timestamp_seconds",
        "cortex_ingester_series_with_floats_and_histograms_total",
    ];

    let tmp = tempfile::tempdir().unwrap();
    let registry = Registry::new();
    let (mut cfg, blocks_cfg, ingest_cfg) = default_ingester_test_config(tmp.path());
    cfg.ring.replication_factor = 1;
    cfg.active_series.active_series_metrics_enabled = !tc.disable_active_series;

    let mut limits = LimitsConfig {
        max_global_exemplars_per_user: tc.max_exemplars,
        max_global_metadata_per_user: tc.max_metadata_per_user,
        max_global_metadata_per_metric: tc.max_metadata_per_metric,
        native_histograms_ingestion_enabled: tc.native_histograms,
        ignore_ooo_exemplars: tc.ignore_ooo_exemplars,
        ..Default::default()
    };
    if !tc.past_grace_period.is_zero() {
        limits.past_grace_period = ModelDuration::from_nanos(tc.past_grace_period.as_nanos() as i64);
    }
    let ooo_time_window_secs = if tc.allow_ooo {
        limits.out_of_order_time_window = ModelDuration::from_nanos(3_600_000_000_000);
        3600i64
    } else {
        0i64
    };

    let (ing, _ring, _kv) = prepare_ingester_with_blocks_storage(
        tmp.path(),
        cfg,
        blocks_cfg,
        ingest_cfg,
        limits,
        Some(&registry),
    );
    start_and_wait_healthy(&ing).await;

    let case_label = format!("{} ({mode:?})", tc.name);

    for (idx, req) in tc.reqs.iter().enumerate() {
        let res = match mode {
            PushMode::Rw1 => ing.push_grpc(USER_ID, req).await,
            PushMode::Rw2 => {
                let rw2 = from_write_request_to_rw2(req, 0, None).expect("convert to rw2");
                let mut wire = Vec::with_capacity(rw2.encoded_len());
                rw2.encode(&mut wire).expect("encode rw2");
                let mut scratch = WriteRequestScratch::new();
                let borrowed = decode_write_request(&wire, DecodeOptions::rw2(), &mut scratch)
                    .expect("decode rw2");
                ing.push_borrowed_to_storage(USER_ID, &borrowed, wire.len() as i64)
            }
        };

        if idx + 1 < tc.reqs.len() {
            assert!(
                res.is_ok(),
                "[{case_label}] expected request {idx} to succeed, got: {res:?}"
            );
        } else if let Some(ref exp) = tc.expected_err {
            let err = res.expect_err(&format!("[{case_label}] expected final request to fail"));
            let status = err.map_push_error_to_status();
            assert_eq!(
                status.code(),
                exp.code,
                "[{case_label}] unexpected gRPC status code: {status:?}"
            );
            let details = grpcutilpb::extract_cortex_error_details(&status)
                .unwrap_or_else(|| panic!("[{case_label}] missing cortex ErrorDetails on status: {status:?}"));
            assert_eq!(
                details.cause, exp.cause as i32,
                "[{case_label}] unexpected ErrorCause"
            );
            assert_eq!(details.soft, exp.soft, "[{case_label}] unexpected soft flag");
            assert_eq!(
                details.rejected_samples, exp.rejected_samples,
                "[{case_label}] unexpected rejected_samples"
            );

            let msg = status.message();
            assert!(
                msg.starts_with(&format!("user={USER_ID}: ")),
                "[{case_label}] expected user prefix in error message, got: {msg}"
            );
            if let Some(ref id) = exp.err_id {
                assert!(
                    msg.contains(id.as_str()),
                    "[{case_label}] expected error ID {} in message, got: {msg}",
                    id.as_str()
                );
            }
            for sub in &exp.contains {
                assert!(
                    msg.contains(sub),
                    "[{case_label}] expected substring {sub:?} in error message, got: {msg}"
                );
            }
        } else {
            assert!(
                res.is_ok(),
                "[{case_label}] expected final request to succeed, got: {res:?}"
            );
        }
    }

    // 1. Read back samples via QueryStream
    let actual_ingested = query_ingested(&ing, USER_ID).await;
    assert_eq!(
        actual_ingested.len(),
        tc.expected_ingested.len(),
        "[{case_label}] ingested series count mismatch"
    );
    for (act, exp) in actual_ingested.iter().zip(tc.expected_ingested.iter()) {
        assert_eq!(act.labels, exp.labels, "[{case_label}] series labels mismatch");
        assert_eq!(act.samples, exp.samples, "[{case_label}] float samples mismatch");
        assert_eq!(
            act.histograms.len(),
            exp.histograms.len(),
            "[{case_label}] histogram count mismatch"
        );
        for ((at, ah), (et, eh)) in act.histograms.iter().zip(exp.histograms.iter()) {
            assert_eq!(at, et, "[{case_label}] histogram timestamp mismatch");
            assert!(
                histograms_equal_ignoring_hint(ah, eh),
                "[{case_label}] histogram mismatch at ts={at}:\nactual:   {ah:?}\nexpected: {eh:?}"
            );
        }
    }

    // 2. Read back exemplars via QueryExemplars
    let md = md_with_org(USER_ID);
    let ex_resp = ing
        .handle_query_exemplars(
            &md,
            USER_ID,
            ingesterpb::ExemplarQueryRequest {
                start_timestamp_ms: i64::MIN,
                end_timestamp_ms: i64::MAX,
                matchers: vec![ingesterpb::LabelMatchers {
                    matchers: vec![ingesterpb::LabelMatcher {
                        r#type: ingesterpb::MatchType::RegexMatch as i32,
                        name: METRIC_NAME_LABEL.to_string(),
                        value: ".*".to_string(),
                    }],
                }],
            },
        )
        .await
        .expect("query_exemplars");
    assert_eq!(
        ex_resp.timeseries.len(),
        tc.expected_exemplars_ingested.len(),
        "[{case_label}] exemplar series count mismatch"
    );
    for (act, exp) in ex_resp
        .timeseries
        .iter()
        .zip(tc.expected_exemplars_ingested.iter())
    {
        assert_eq!(act.labels, exp.labels, "[{case_label}] exemplar series labels mismatch");
        assert_eq!(
            act.exemplars, exp.exemplars,
            "[{case_label}] exemplars mismatch"
        );
    }

    // 3. Read back metadata via MetricsMetadata
    let meta_resp = ing
        .handle_metrics_metadata(
            &md,
            USER_ID,
            ingesterpb::MetricsMetadataRequest {
                limit: -1,
                limit_per_metric: -1,
                metric: String::new(),
                metric_names: Vec::new(),
            },
        )
        .await
        .expect("metrics_metadata");
    assert_eq!(
        meta_resp.metadata, tc.expected_metadata_ingested,
        "[{case_label}] metadata mismatch"
    );

    // 4. Update active series metrics
    if !tc.disable_active_series {
        ing.update_active_series_metrics(SystemTime::now());
    }

    // 5. Check Prometheus metrics
    let mut all_checked = metric_names;
    all_checked.extend_from_slice(&tc.additional_metrics);
    assert_metrics_match(&registry, &tc.expected_metrics, &all_checked, &case_label);

    // 6. Check anonymous usage stats
    let expected_tenants_count = if tc.expected_ingested.is_empty() { 0 } else { 1 };
    let expected_samples_count: i64 = tc
        .expected_ingested
        .iter()
        .map(|s| (s.samples.len() + s.histograms.len()) as i64)
        .sum();
    let expected_exemplars_count: i64 = tc
        .expected_exemplars_ingested
        .iter()
        .map(|s| s.exemplars.len() as i64)
        .sum();

    let stats = ing.update_usage_stats();
    if !tc.disable_active_series {
        assert_eq!(
            stats.active_series,
            tc.expected_ingested.len() as i64,
            "[{case_label}] usage_stats.active_series"
        );
    }
    assert_eq!(
        stats.memory_series,
        tc.expected_ingested.len() as i64,
        "[{case_label}] usage_stats.memory_series"
    );
    assert_eq!(
        stats.memory_users, expected_tenants_count,
        "[{case_label}] usage_stats.memory_users"
    );
    assert_eq!(
        stats.appended_samples, expected_samples_count,
        "[{case_label}] usage_stats.appended_samples"
    );
    assert_eq!(
        stats.appended_exemplars, expected_exemplars_count,
        "[{case_label}] usage_stats.appended_exemplars"
    );
    assert_eq!(
        stats.tenants_with_out_of_order_enabled == 1,
        tc.allow_ooo,
        "[{case_label}] usage_stats.tenants_with_out_of_order_enabled"
    );
    assert_eq!(
        stats.min_out_of_order_time_window_seconds, ooo_time_window_secs,
        "[{case_label}] usage_stats.min_out_of_order_time_window_seconds"
    );
    assert_eq!(
        stats.max_out_of_order_time_window_seconds, ooo_time_window_secs,
        "[{case_label}] usage_stats.max_out_of_order_time_window_seconds"
    );

    ing.stop().await.expect("stop ingester");
}

fn build_push_cases(now_ms: i64) -> Vec<PushTestCase> {
    let mut histogram_with_bucket_count_mismatch = generate_test_histogram(1);
    histogram_with_bucket_count_mismatch.count += 1;

    let mut histogram_with_count_not_big_enough = generate_test_histogram(1);
    histogram_with_count_not_big_enough.sum = f64::NAN;
    histogram_with_count_not_big_enough.count -= 1;

    let mut histogram_with_negative_bucket_count = generate_test_histogram(1);
    histogram_with_negative_bucket_count.negative_buckets[1] = -100;

    let mut histogram_with_span_negative_offset = generate_test_histogram(1);
    histogram_with_span_negative_offset.positive_spans[1].offset = -2;

    let mut histogram_with_spans_buckets_mismatch = generate_test_histogram(1);
    histogram_with_spans_buckets_mismatch.positive_spans[1].length += 1;

    let histogram_with_custom_buckets = generate_test_custom_buckets_histogram(1);

    let mut histogram_with_custom_buckets_mismatch = generate_test_custom_buckets_histogram(1);
    histogram_with_custom_buckets_mismatch.custom_values.truncate(1);

    let mut histogram_with_custom_buckets_invalid = generate_test_custom_buckets_histogram(1);
    histogram_with_custom_buckets_invalid.custom_values[0] = 100.0;
    histogram_with_custom_buckets_invalid.custom_values[1] = 50.0;

    let mut histogram_with_custom_buckets_infinite = generate_test_custom_buckets_histogram(1);
    let last_idx = histogram_with_custom_buckets_infinite.custom_values.len() - 1;
    histogram_with_custom_buckets_infinite.custom_values[last_idx] = f64::INFINITY;

    let now_s = fmt_ts_secs(now_ms);
    let now_plus_1_s = fmt_ts_secs(now_ms + 1);
    let two_hours_ms = 2 * 60 * 60 * 1000;
    let long_trace_id = "a".repeat(128);

    vec![
        // Case 1 (lines 1933..1972)
        PushTestCase {
            name: "should soft fail if histogram has a bucket count vs overall count mismatch",
            native_histograms: true,
            reqs: vec![write_req_histogram(
                test_label_pairs(),
                vec![histogram_to_proto(10, &histogram_with_bucket_count_mismatch)],
                Vec::new(),
            )],
            expected_err: Some(ExpectedError {
                code: Code::InvalidArgument,
                cause: cortexpb::ErrorCause::BadData,
                soft: true,
                rejected_samples: 1,
                err_id: Some(NATIVE_HISTOGRAM_COUNT_MISMATCH),
                contains: vec![
                    "21 observations found in buckets, but the Count field is 22".to_string(),
                    "timestamp=1970-01-01T00:00:00.01Z".to_string(),
                    "series=test".to_string(),
                ],
            }),
            expected_metrics: r#"
                cortex_ingester_ingested_samples_total{user="test"} 0
                cortex_discarded_samples_total{group="",reason="invalid-native-histogram",user="test"} 1
                cortex_ingester_ingested_samples_failures_total{user="test"} 1
                cortex_ingester_memory_users 1
                cortex_ingester_memory_series 0
                cortex_ingester_memory_series_created_total{user="test"} 0
                cortex_ingester_memory_series_removed_total{user="test"} 0
                cortex_ingester_tsdb_head_min_timestamp_seconds 0.01
                cortex_ingester_tsdb_head_max_timestamp_seconds 0.01
            "#
            .to_string(),
            ..Default::default()
        },
        // Case 2 (lines 1973..2013)
        PushTestCase {
            name: "should soft fail if histogram has a bucket count higher than overall count and sum NaN",
            native_histograms: true,
            reqs: vec![write_req_histogram(
                test_label_pairs(),
                vec![histogram_to_proto(10, &histogram_with_count_not_big_enough)],
                Vec::new(),
            )],
            expected_err: Some(ExpectedError {
                code: Code::InvalidArgument,
                cause: cortexpb::ErrorCause::BadData,
                soft: true,
                rejected_samples: 1,
                err_id: Some(NATIVE_HISTOGRAM_COUNT_NOT_BIG_ENOUGH),
                contains: vec![
                    "21 observations found in buckets, but the Count field is 20".to_string(),
                    "timestamp=1970-01-01T00:00:00.01Z".to_string(),
                    "series=test".to_string(),
                ],
            }),
            expected_metrics: r#"
                cortex_ingester_ingested_samples_total{user="test"} 0
                cortex_discarded_samples_total{group="",reason="invalid-native-histogram",user="test"} 1
                cortex_ingester_ingested_samples_failures_total{user="test"} 1
                cortex_ingester_memory_users 1
                cortex_ingester_memory_series 0
                cortex_ingester_memory_series_created_total{user="test"} 0
                cortex_ingester_memory_series_removed_total{user="test"} 0
                cortex_ingester_tsdb_head_min_timestamp_seconds 0.01
                cortex_ingester_tsdb_head_max_timestamp_seconds 0.01
            "#
            .to_string(),
            ..Default::default()
        },
        // Case 3 (lines 2014..2054)
        PushTestCase {
            name: "should soft fail if histogram has a negative span offset",
            native_histograms: true,
            reqs: vec![write_req_histogram(
                test_label_pairs(),
                vec![histogram_to_proto(10, &histogram_with_span_negative_offset)],
                Vec::new(),
            )],
            expected_err: Some(ExpectedError {
                code: Code::InvalidArgument,
                cause: cortexpb::ErrorCause::BadData,
                soft: true,
                rejected_samples: 1,
                err_id: Some(NATIVE_HISTOGRAM_SPAN_NEGATIVE_OFFSET),
                contains: vec![
                    "positive side: span number 2 with offset -2: histogram has a span whose offset is negative".to_string(),
                    "timestamp=1970-01-01T00:00:00.01Z".to_string(),
                    "series=test".to_string(),
                ],
            }),
            expected_metrics: r#"
                cortex_ingester_ingested_samples_total{user="test"} 0
                cortex_discarded_samples_total{group="",reason="invalid-native-histogram",user="test"} 1
                cortex_ingester_ingested_samples_failures_total{user="test"} 1
                cortex_ingester_memory_users 1
                cortex_ingester_memory_series 0
                cortex_ingester_memory_series_created_total{user="test"} 0
                cortex_ingester_memory_series_removed_total{user="test"} 0
                cortex_ingester_tsdb_head_min_timestamp_seconds 0.01
                cortex_ingester_tsdb_head_max_timestamp_seconds 0.01
            "#
            .to_string(),
            ..Default::default()
        },
        // Case 4 (lines 2055..2095)
        PushTestCase {
            name: "should soft fail if histogram has different number of buckets than encoded in spans",
            native_histograms: true,
            reqs: vec![write_req_histogram(
                test_label_pairs(),
                vec![histogram_to_proto(10, &histogram_with_spans_buckets_mismatch)],
                Vec::new(),
            )],
            expected_err: Some(ExpectedError {
                code: Code::InvalidArgument,
                cause: cortexpb::ErrorCause::BadData,
                soft: true,
                rejected_samples: 1,
                err_id: Some(NATIVE_HISTOGRAM_SPANS_BUCKETS_MISMATCH),
                contains: vec![
                    "positive side: spans need 5 buckets, have 4 buckets".to_string(),
                    "timestamp=1970-01-01T00:00:00.01Z".to_string(),
                    "series=test".to_string(),
                ],
            }),
            expected_metrics: r#"
                cortex_ingester_ingested_samples_total{user="test"} 0
                cortex_discarded_samples_total{group="",reason="invalid-native-histogram",user="test"} 1
                cortex_ingester_ingested_samples_failures_total{user="test"} 1
                cortex_ingester_memory_users 1
                cortex_ingester_memory_series 0
                cortex_ingester_memory_series_created_total{user="test"} 0
                cortex_ingester_memory_series_removed_total{user="test"} 0
                cortex_ingester_tsdb_head_min_timestamp_seconds 0.01
                cortex_ingester_tsdb_head_max_timestamp_seconds 0.01
            "#
            .to_string(),
            ..Default::default()
        },
        // Case 5 (lines 2096..2146)
        PushTestCase {
            name: "should succeed on valid histogram with custom buckets",
            native_histograms: true,
            reqs: vec![write_req_histogram(
                test_label_pairs(),
                vec![histogram_to_proto(10, &histogram_with_custom_buckets)],
                Vec::new(),
            )],
            expected_ingested: vec![ExpectedSeries {
                labels: test_labels(),
                samples: Vec::new(),
                histograms: vec![(10, generate_test_custom_buckets_histogram(1))],
            }],
            expected_metrics: r#"
                cortex_ingester_active_native_histogram_buckets{user="test"} 4
                cortex_ingester_active_native_histogram_series{user="test"} 1
                cortex_ingester_active_series{user="test"} 1
                cortex_ingester_ingested_samples_failures_total{user="test"} 0
                cortex_ingester_ingested_samples_total{user="test"} 1
                cortex_ingester_memory_series 1
                cortex_ingester_memory_series_created_total{user="test"} 1
                cortex_ingester_memory_series_removed_total{user="test"} 0
                cortex_ingester_memory_users 1
                cortex_ingester_tsdb_head_max_timestamp_seconds 0.01
                cortex_ingester_tsdb_head_min_timestamp_seconds 0.01
            "#
            .to_string(),
            ..Default::default()
        },
        // Case 6 (lines 2147..2187)
        PushTestCase {
            name: "should soft fail if native histogram with custom buckets has too few custom values",
            native_histograms: true,
            reqs: vec![write_req_histogram(
                test_label_pairs(),
                vec![histogram_to_proto(10, &histogram_with_custom_buckets_mismatch)],
                Vec::new(),
            )],
            expected_err: Some(ExpectedError {
                code: Code::InvalidArgument,
                cause: cortexpb::ErrorCause::BadData,
                soft: true,
                rejected_samples: 1,
                err_id: Some(NATIVE_HISTOGRAM_CUSTOM_BUCKETS_MISMATCH),
                contains: vec![
                    "custom buckets: only 1 custom bounds defined which is insufficient to cover total span length of 5".to_string(),
                    "timestamp=1970-01-01T00:00:00.01Z".to_string(),
                    "series=test".to_string(),
                ],
            }),
            expected_metrics: r#"
                cortex_ingester_ingested_samples_total{user="test"} 0
                cortex_discarded_samples_total{group="",reason="invalid-native-histogram",user="test"} 1
                cortex_ingester_ingested_samples_failures_total{user="test"} 1
                cortex_ingester_memory_users 1
                cortex_ingester_memory_series 0
                cortex_ingester_memory_series_created_total{user="test"} 0
                cortex_ingester_memory_series_removed_total{user="test"} 0
                cortex_ingester_tsdb_head_min_timestamp_seconds 0.01
                cortex_ingester_tsdb_head_max_timestamp_seconds 0.01
            "#
            .to_string(),
            ..Default::default()
        },
        // Case 7 (lines 2188..2228)
        PushTestCase {
            name: "should soft fail if native histogram with custom buckets has invalid custom values",
            native_histograms: true,
            reqs: vec![write_req_histogram(
                test_label_pairs(),
                vec![histogram_to_proto(10, &histogram_with_custom_buckets_invalid)],
                Vec::new(),
            )],
            expected_err: Some(ExpectedError {
                code: Code::InvalidArgument,
                cause: cortexpb::ErrorCause::BadData,
                soft: true,
                rejected_samples: 1,
                err_id: Some(NATIVE_HISTOGRAM_CUSTOM_BUCKETS_INVALID),
                contains: vec![
                    "custom buckets: previous bound is 100.000000 and current is 50.000000".to_string(),
                    "timestamp=1970-01-01T00:00:00.01Z".to_string(),
                    "series=test".to_string(),
                ],
            }),
            expected_metrics: r#"
                cortex_ingester_ingested_samples_total{user="test"} 0
                cortex_discarded_samples_total{group="",reason="invalid-native-histogram",user="test"} 1
                cortex_ingester_ingested_samples_failures_total{user="test"} 1
                cortex_ingester_memory_users 1
                cortex_ingester_memory_series 0
                cortex_ingester_memory_series_created_total{user="test"} 0
                cortex_ingester_memory_series_removed_total{user="test"} 0
                cortex_ingester_tsdb_head_min_timestamp_seconds 0.01
                cortex_ingester_tsdb_head_max_timestamp_seconds 0.01
            "#
            .to_string(),
            ..Default::default()
        },
        // Case 8 (lines 2229..2269)
        PushTestCase {
            name: "should soft fail if native histogram with custom buckets has explicit infinite boundary",
            native_histograms: true,
            reqs: vec![write_req_histogram(
                test_label_pairs(),
                vec![histogram_to_proto(10, &histogram_with_custom_buckets_infinite)],
                Vec::new(),
            )],
            expected_err: Some(ExpectedError {
                code: Code::InvalidArgument,
                cause: cortexpb::ErrorCause::BadData,
                soft: true,
                rejected_samples: 1,
                err_id: Some(NATIVE_HISTOGRAM_CUSTOM_BUCKETS_INFINITE),
                contains: vec![
                    "custom buckets: last +Inf bound must not be explicitly defined".to_string(),
                    "timestamp=1970-01-01T00:00:00.01Z".to_string(),
                    "series=test".to_string(),
                ],
            }),
            expected_metrics: r#"
                cortex_ingester_ingested_samples_total{user="test"} 0
                cortex_discarded_samples_total{group="",reason="invalid-native-histogram",user="test"} 1
                cortex_ingester_ingested_samples_failures_total{user="test"} 1
                cortex_ingester_memory_users 1
                cortex_ingester_memory_series 0
                cortex_ingester_memory_series_created_total{user="test"} 0
                cortex_ingester_memory_series_removed_total{user="test"} 0
                cortex_ingester_tsdb_head_min_timestamp_seconds 0.01
                cortex_ingester_tsdb_head_max_timestamp_seconds 0.01
            "#
            .to_string(),
            ..Default::default()
        },
        // Case 9 (lines 2270..2329)
        PushTestCase {
            name: "should soft fail if OOO native histograms are received and OOO is disabled",
            native_histograms: true,
            allow_ooo: false,
            reqs: vec![
                write_req_histogram(
                    test_label_pairs(),
                    vec![histogram_to_proto(10, &generate_test_histogram(1))],
                    Vec::new(),
                ),
                write_req_histogram(
                    test_label_pairs(),
                    vec![histogram_to_proto(-10, &generate_test_histogram(1))],
                    Vec::new(),
                ),
            ],
            expected_err: Some(ExpectedError {
                code: Code::InvalidArgument,
                cause: cortexpb::ErrorCause::BadData,
                soft: true,
                rejected_samples: 1,
                err_id: Some(SAMPLE_OUT_OF_ORDER),
                contains: vec![
                    "1969-12-31T23:59:59.99Z".to_string(),
                    "series test".to_string(),
                ],
            }),
            expected_ingested: vec![ExpectedSeries {
                labels: test_labels(),
                samples: Vec::new(),
                histograms: vec![(10, generate_test_histogram(1))],
            }],
            expected_metrics: r#"
                cortex_ingester_ingested_samples_total{user="test"} 1
                cortex_discarded_samples_total{group="",reason="sample-out-of-order",user="test"} 1
                cortex_ingester_ingested_samples_failures_total{user="test"} 1
                cortex_ingester_memory_users 1
                cortex_ingester_memory_series 1
                cortex_ingester_memory_series_created_total{user="test"} 1
                cortex_ingester_memory_series_removed_total{user="test"} 0
                cortex_ingester_tsdb_head_min_timestamp_seconds 0.01
                cortex_ingester_tsdb_head_max_timestamp_seconds 0.01
                cortex_ingester_active_native_histogram_buckets{user="test"} 8
                cortex_ingester_active_native_histogram_series{user="test"} 1
                cortex_ingester_active_series{user="test"} 1
            "#
            .to_string(),
            ..Default::default()
        },
        // Case 10 (lines 2330..2387)
        PushTestCase {
            name: "should not fail if OOO native histograms are received and OOO is enabled",
            native_histograms: true,
            allow_ooo: true,
            reqs: vec![
                write_req_histogram(
                    test_label_pairs(),
                    vec![histogram_to_proto(10, &generate_test_histogram(1))],
                    Vec::new(),
                ),
                write_req_histogram(
                    test_label_pairs(),
                    vec![histogram_to_proto(-10, &generate_test_histogram(1))],
                    Vec::new(),
                ),
            ],
            expected_ingested: vec![ExpectedSeries {
                labels: test_labels(),
                samples: Vec::new(),
                histograms: vec![
                    (-10, generate_test_histogram(1)),
                    (10, generate_test_histogram(1)),
                ],
            }],
            expected_metrics: r#"
                cortex_ingester_ingested_samples_total{user="test"} 2
                cortex_ingester_ingested_samples_failures_total{user="test"} 0
                cortex_ingester_memory_users 1
                cortex_ingester_memory_series 1
                cortex_ingester_memory_series_created_total{user="test"} 1
                cortex_ingester_memory_series_removed_total{user="test"} 0
                cortex_ingester_tsdb_head_min_timestamp_seconds 0.01
                cortex_ingester_tsdb_head_max_timestamp_seconds 0.01
                cortex_ingester_active_native_histogram_buckets{user="test"} 8
                cortex_ingester_active_native_histogram_series{user="test"} 1
                cortex_ingester_active_series{user="test"} 1
            "#
            .to_string(),
            ..Default::default()
        },
        // Case 11 (lines 2388..2428)
        PushTestCase {
            name: "should soft fail if histogram has a negative bucket count",
            native_histograms: true,
            reqs: vec![write_req_histogram(
                test_label_pairs(),
                vec![histogram_to_proto(10, &histogram_with_negative_bucket_count)],
                Vec::new(),
            )],
            expected_err: Some(ExpectedError {
                code: Code::InvalidArgument,
                cause: cortexpb::ErrorCause::BadData,
                soft: true,
                rejected_samples: 1,
                err_id: Some(NATIVE_HISTOGRAM_NEGATIVE_BUCKET_COUNT),
                contains: vec![
                    "negative side: bucket number 2 has observation count of -98".to_string(),
                    "timestamp=1970-01-01T00:00:00.01Z".to_string(),
                    "series=test".to_string(),
                ],
            }),
            expected_metrics: r#"
                cortex_ingester_ingested_samples_total{user="test"} 0
                cortex_discarded_samples_total{group="",reason="invalid-native-histogram",user="test"} 1
                cortex_ingester_ingested_samples_failures_total{user="test"} 1
                cortex_ingester_memory_users 1
                cortex_ingester_memory_series 0
                cortex_ingester_memory_series_created_total{user="test"} 0
                cortex_ingester_memory_series_removed_total{user="test"} 0
                cortex_ingester_tsdb_head_min_timestamp_seconds 0.01
                cortex_ingester_tsdb_head_max_timestamp_seconds 0.01
            "#
            .to_string(),
            ..Default::default()
        },
        // Case 12 (lines 2429..2572)
        PushTestCase {
            name: "should soft fail on existing histogram series if all exemplars are out of order",
            max_exemplars: 2,
            native_histograms: true,
            reqs: vec![
                write_req_histogram(
                    test_label_pairs(),
                    vec![histogram_to_proto(9, &generate_test_histogram(1))],
                    vec![
                        make_exemplar("123", 1000, 1000.0),
                        make_exemplar("456", 2000, 2000.0),
                    ],
                ),
                write_req_histogram(
                    test_label_pairs(),
                    vec![histogram_to_proto(10, &generate_test_histogram(2))],
                    vec![
                        make_exemplar("123", 1000, 1000.0),
                        make_exemplar("333", 1500, 1500.0),
                    ],
                ),
            ],
            expected_err: Some(ExpectedError {
                code: Code::InvalidArgument,
                cause: cortexpb::ErrorCause::BadData,
                soft: true,
                rejected_samples: 0,
                err_id: None,
                contains: vec![
                    "out of order exemplar".to_string(),
                    "timestamp=1970-01-01T00:00:01.5Z".to_string(),
                    "series=test".to_string(),
                    r#"exemplar={traceID="333"}"#.to_string(),
                ],
            }),
            expected_ingested: vec![ExpectedSeries {
                labels: test_labels(),
                samples: Vec::new(),
                histograms: vec![
                    (9, generate_test_histogram(1)),
                    (10, generate_test_histogram(2)),
                ],
            }],
            expected_exemplars_ingested: vec![ExpectedExemplarSeries {
                labels: test_label_pairs(),
                exemplars: vec![
                    make_exemplar("123", 1000, 1000.0),
                    make_exemplar("456", 2000, 2000.0),
                ],
            }],
            additional_metrics: vec![
                "cortex_ingester_tsdb_exemplar_exemplars_appended_total",
                "cortex_ingester_tsdb_exemplar_exemplars_in_storage",
                "cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage",
                "cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds",
                "cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total",
                "cortex_ingester_ingested_exemplars_total",
                "cortex_ingester_ingested_exemplars_failures_total",
            ],
            expected_metrics: r#"
                cortex_ingester_ingested_samples_total{user="test"} 2
                cortex_ingester_ingested_samples_failures_total{user="test"} 0
                cortex_ingester_memory_users 1
                cortex_ingester_memory_series 1
                cortex_ingester_memory_series_created_total{user="test"} 1
                cortex_ingester_memory_series_removed_total{user="test"} 0
                cortex_ingester_active_native_histogram_buckets{user="test"} 8
                cortex_ingester_active_native_histogram_series{user="test"} 1
                cortex_ingester_active_series{user="test"} 1
                cortex_ingester_tsdb_exemplar_exemplars_appended_total{user="test"} 2
                cortex_ingester_tsdb_exemplar_exemplars_in_storage 2
                cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage{user="test"} 1
                cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds{user="test"} 1
                cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total 0
                cortex_ingester_tsdb_head_min_timestamp_seconds 0.009
                cortex_ingester_tsdb_head_max_timestamp_seconds 0.01
                cortex_ingester_ingested_exemplars_total 2
                cortex_ingester_ingested_exemplars_failures_total 2
            "#
            .to_string(),
            ..Default::default()
        },
        // Case 13 (lines 2573..2714)
        PushTestCase {
            name: "should succeed on existing histogram series if all exemplars are out of order but OOO exemplars are ignored",
            max_exemplars: 2,
            native_histograms: true,
            ignore_ooo_exemplars: true,
            reqs: vec![
                write_req_histogram(
                    test_label_pairs(),
                    vec![histogram_to_proto(9, &generate_test_histogram(1))],
                    vec![
                        make_exemplar("123", 1000, 1000.0),
                        make_exemplar("456", 2000, 2000.0),
                    ],
                ),
                write_req_histogram(
                    test_label_pairs(),
                    vec![histogram_to_proto(10, &generate_test_histogram(2))],
                    vec![
                        make_exemplar("123", 1000, 1000.0),
                        make_exemplar("333", 1500, 1500.0),
                    ],
                ),
            ],
            expected_ingested: vec![ExpectedSeries {
                labels: test_labels(),
                samples: Vec::new(),
                histograms: vec![
                    (9, generate_test_histogram(1)),
                    (10, generate_test_histogram(2)),
                ],
            }],
            expected_exemplars_ingested: vec![ExpectedExemplarSeries {
                labels: test_label_pairs(),
                exemplars: vec![
                    make_exemplar("123", 1000, 1000.0),
                    make_exemplar("456", 2000, 2000.0),
                ],
            }],
            additional_metrics: vec![
                "cortex_ingester_tsdb_exemplar_exemplars_appended_total",
                "cortex_ingester_tsdb_exemplar_exemplars_in_storage",
                "cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage",
                "cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds",
                "cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total",
                "cortex_ingester_ingested_exemplars_total",
                "cortex_ingester_ingested_exemplars_failures_total",
            ],
            expected_metrics: r#"
                cortex_ingester_ingested_samples_total{user="test"} 2
                cortex_ingester_ingested_samples_failures_total{user="test"} 0
                cortex_ingester_memory_users 1
                cortex_ingester_memory_series 1
                cortex_ingester_memory_series_created_total{user="test"} 1
                cortex_ingester_memory_series_removed_total{user="test"} 0
                cortex_ingester_active_native_histogram_buckets{user="test"} 8
                cortex_ingester_active_native_histogram_series{user="test"} 1
                cortex_ingester_active_series{user="test"} 1
                cortex_ingester_tsdb_exemplar_exemplars_appended_total{user="test"} 2
                cortex_ingester_tsdb_exemplar_exemplars_in_storage 2
                cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage{user="test"} 1
                cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds{user="test"} 1
                cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total 0
                cortex_ingester_tsdb_head_min_timestamp_seconds 0.009
                cortex_ingester_tsdb_head_max_timestamp_seconds 0.01
                cortex_ingester_ingested_exemplars_total 2
                cortex_ingester_ingested_exemplars_failures_total 2
            "#
            .to_string(),
            ..Default::default()
        },
        // Case 14 (lines 2715..2761)
        PushTestCase {
            name: "successful push, active series disabled",
            disable_active_series: true,
            reqs: vec![
                write_req_samples(
                    test_label_pairs(),
                    vec![cortexpb::Sample {
                        value: 1.0,
                        timestamp_ms: 9,
                    }],
                ),
                write_req_samples(
                    test_label_pairs(),
                    vec![cortexpb::Sample {
                        value: 2.0,
                        timestamp_ms: 10,
                    }],
                ),
            ],
            expected_ingested: vec![ExpectedSeries {
                labels: test_labels(),
                samples: vec![(9, 1.0), (10, 2.0)],
                histograms: Vec::new(),
            }],
            expected_metrics: r#"
                cortex_ingester_ingested_samples_total{user="test"} 2
                cortex_ingester_ingested_samples_failures_total{user="test"} 0
                cortex_ingester_memory_users 1
                cortex_ingester_memory_series 1
                cortex_ingester_memory_series_created_total{user="test"} 1
                cortex_ingester_memory_series_removed_total{user="test"} 0
                cortex_ingester_tsdb_head_min_timestamp_seconds 0.009
                cortex_ingester_tsdb_head_max_timestamp_seconds 0.01
            "#
            .to_string(),
            ..Default::default()
        },
        // Case 15 (lines 2762..2815)
        PushTestCase {
            name: "should soft fail on sample out-of-order",
            reqs: vec![
                write_req_samples(
                    test_label_pairs(),
                    vec![cortexpb::Sample {
                        value: 2.0,
                        timestamp_ms: 10,
                    }],
                ),
                write_req_samples(
                    test_label_pairs(),
                    vec![cortexpb::Sample {
                        value: 1.0,
                        timestamp_ms: 9,
                    }],
                ),
            ],
            expected_err: Some(ExpectedError {
                code: Code::InvalidArgument,
                cause: cortexpb::ErrorCause::BadData,
                soft: true,
                rejected_samples: 1,
                err_id: Some(SAMPLE_OUT_OF_ORDER),
                contains: vec![
                    "1970-01-01T00:00:00.009Z".to_string(),
                    "series test".to_string(),
                ],
            }),
            expected_ingested: vec![ExpectedSeries {
                labels: test_labels(),
                samples: vec![(10, 2.0)],
                histograms: Vec::new(),
            }],
            expected_metrics: r#"
                cortex_ingester_ingested_samples_total{user="test"} 1
                cortex_ingester_ingested_samples_failures_total{user="test"} 1
                cortex_ingester_memory_users 1
                cortex_ingester_memory_series 1
                cortex_ingester_memory_series_created_total{user="test"} 1
                cortex_ingester_memory_series_removed_total{user="test"} 0
                cortex_discarded_samples_total{group="",reason="sample-out-of-order",user="test"} 1
                cortex_ingester_active_series{user="test"} 1
                cortex_ingester_tsdb_head_min_timestamp_seconds 0.01
                cortex_ingester_tsdb_head_max_timestamp_seconds 0.01
            "#
            .to_string(),
            ..Default::default()
        },
        // Case 16 (lines 2816..2874)
        PushTestCase {
            name: "should soft fail on all samples out of bound in a write request",
            reqs: vec![
                write_req_samples(
                    test_label_pairs(),
                    vec![cortexpb::Sample {
                        value: 2.0,
                        timestamp_ms: 1_575_043_969,
                    }],
                ),
                write_req_samples(
                    test_label_pairs(),
                    vec![
                        cortexpb::Sample {
                            value: 0.0,
                            timestamp_ms: 1_575_043_969 - (86400 * 1000),
                        },
                        cortexpb::Sample {
                            value: 1.0,
                            timestamp_ms: 1_575_043_969 - (86000 * 1000),
                        },
                    ],
                ),
            ],
            expected_err: Some(ExpectedError {
                code: Code::InvalidArgument,
                cause: cortexpb::ErrorCause::BadData,
                soft: true,
                rejected_samples: 2,
                err_id: Some(SAMPLE_TIMESTAMP_TOO_OLD),
                contains: vec![
                    "1970-01-18T05:30:43.969Z".to_string(),
                    "series test".to_string(),
                ],
            }),
            expected_ingested: vec![ExpectedSeries {
                labels: test_labels(),
                samples: vec![(1_575_043_969, 2.0)],
                histograms: Vec::new(),
            }],
            expected_metrics: r#"
                cortex_ingester_ingested_samples_total{user="test"} 1
                cortex_ingester_ingested_samples_failures_total{user="test"} 2
                cortex_ingester_memory_users 1
                cortex_ingester_memory_series 1
                cortex_ingester_memory_series_created_total{user="test"} 1
                cortex_ingester_memory_series_removed_total{user="test"} 0
                cortex_discarded_samples_total{group="",reason="sample-timestamp-too-old",user="test"} 2
                cortex_ingester_active_series{user="test"} 1
                cortex_ingester_tsdb_head_min_timestamp_seconds 1575043.969
                cortex_ingester_tsdb_head_max_timestamp_seconds 1575043.969
            "#
            .to_string(),
            ..Default::default()
        },
        // Case 17 (lines 2875..2957)
        PushTestCase {
            name: "should count series that have both float samples and histograms",
            native_histograms: true,
            reqs: vec![cortexpb::WriteRequest {
                timeseries: vec![
                    cortexpb::TimeSeries {
                        labels: named_label_pairs("test_1"),
                        samples: vec![cortexpb::Sample {
                            value: 1.0,
                            timestamp_ms: 1000,
                        }],
                        histograms: vec![histogram_to_proto(2000, &generate_test_histogram(1))],
                        ..Default::default()
                    },
                    cortexpb::TimeSeries {
                        labels: named_label_pairs("test_2"),
                        samples: vec![cortexpb::Sample {
                            value: 2.0,
                            timestamp_ms: 1000,
                        }],
                        ..Default::default()
                    },
                    cortexpb::TimeSeries {
                        labels: named_label_pairs("test_3"),
                        samples: vec![cortexpb::Sample {
                            value: 3.0,
                            timestamp_ms: 1000,
                        }],
                        histograms: vec![histogram_to_proto(2000, &generate_test_histogram(2))],
                        ..Default::default()
                    },
                ],
                source: cortexpb::write_request::SourceEnum::Api as i32,
                ..Default::default()
            }],
            expected_ingested: vec![
                ExpectedSeries {
                    labels: named_labels("test_1"),
                    samples: vec![(1000, 1.0)],
                    histograms: vec![(2000, generate_test_histogram(1))],
                },
                ExpectedSeries {
                    labels: named_labels("test_2"),
                    samples: vec![(1000, 2.0)],
                    histograms: Vec::new(),
                },
                ExpectedSeries {
                    labels: named_labels("test_3"),
                    samples: vec![(1000, 3.0)],
                    histograms: vec![(2000, generate_test_histogram(2))],
                },
            ],
            expected_metrics: r#"
                cortex_ingester_ingested_samples_total{user="test"} 5
                cortex_ingester_ingested_samples_failures_total{user="test"} 0
                cortex_ingester_memory_users 1
                cortex_ingester_memory_series 3
                cortex_ingester_memory_series_created_total{user="test"} 3
                cortex_ingester_memory_series_removed_total{user="test"} 0
                cortex_ingester_active_series{user="test"} 3
                cortex_ingester_active_native_histogram_series{user="test"} 2
                cortex_ingester_active_native_histogram_buckets{user="test"} 16
                cortex_ingester_series_with_floats_and_histograms_total{user="test"} 2
                cortex_ingester_tsdb_head_min_timestamp_seconds 1
                cortex_ingester_tsdb_head_max_timestamp_seconds 2
            "#
            .to_string(),
            ..Default::default()
        },
        // Case 18 (lines 2958..3020)
        PushTestCase {
            name: "should soft fail on all samples with histograms out of bound in a write request",
            native_histograms: true,
            reqs: vec![
                write_req_samples(
                    test_label_pairs(),
                    vec![cortexpb::Sample {
                        value: 2.0,
                        timestamp_ms: 1_575_043_969,
                    }],
                ),
                cortexpb::WriteRequest {
                    timeseries: vec![cortexpb::TimeSeries {
                        labels: test_label_pairs(),
                        samples: vec![
                            cortexpb::Sample {
                                value: 0.0,
                                timestamp_ms: 1_575_043_969 - (86400 * 1000),
                            },
                            cortexpb::Sample {
                                value: 1.0,
                                timestamp_ms: 1_575_043_969 - (86000 * 1000),
                            },
                        ],
                        histograms: vec![histogram_to_proto(
                            1_575_043_969 - (86800 * 1000),
                            &generate_test_histogram(0),
                        )],
                        ..Default::default()
                    }],
                    source: cortexpb::write_request::SourceEnum::Api as i32,
                    ..Default::default()
                },
            ],
            expected_err: Some(ExpectedError {
                code: Code::InvalidArgument,
                cause: cortexpb::ErrorCause::BadData,
                soft: true,
                rejected_samples: 3,
                err_id: Some(SAMPLE_TIMESTAMP_TOO_OLD),
                contains: vec![
                    "1970-01-18T05:24:03.969Z".to_string(),
                    "series test".to_string(),
                ],
            }),
            expected_ingested: vec![ExpectedSeries {
                labels: test_labels(),
                samples: vec![(1_575_043_969, 2.0)],
                histograms: Vec::new(),
            }],
            expected_metrics: r#"
                cortex_ingester_ingested_samples_total{user="test"} 1
                cortex_ingester_ingested_samples_failures_total{user="test"} 3
                cortex_ingester_memory_users 1
                cortex_ingester_memory_series 1
                cortex_ingester_memory_series_created_total{user="test"} 1
                cortex_ingester_memory_series_removed_total{user="test"} 0
                cortex_discarded_samples_total{group="",reason="sample-timestamp-too-old",user="test"} 3
                cortex_ingester_active_series{user="test"} 1
                cortex_ingester_tsdb_head_min_timestamp_seconds 1575043.969
                cortex_ingester_series_with_floats_and_histograms_total{user="test"} 1
                cortex_ingester_tsdb_head_max_timestamp_seconds 1575043.969
            "#
            .to_string(),
            ..Default::default()
        },
        // Case 19 (lines 3021..3080)
        PushTestCase {
            name: "should succeed if histograms are out of bound but samples are not and histograms are not accepted",
            native_histograms: false,
            reqs: vec![
                write_req_samples(
                    test_label_pairs(),
                    vec![cortexpb::Sample {
                        value: 2.0,
                        timestamp_ms: 1_575_043_969,
                    }],
                ),
                cortexpb::WriteRequest {
                    timeseries: vec![cortexpb::TimeSeries {
                        labels: test_label_pairs(),
                        samples: vec![cortexpb::Sample {
                            value: 0.0,
                            timestamp_ms: 1_575_043_969 + 1000,
                        }],
                        histograms: vec![histogram_to_proto(
                            1_575_043_969 - (86800 * 1000),
                            &generate_test_histogram(0),
                        )],
                        ..Default::default()
                    }],
                    source: cortexpb::write_request::SourceEnum::Api as i32,
                    ..Default::default()
                },
            ],
            expected_ingested: vec![ExpectedSeries {
                labels: test_labels(),
                samples: vec![(1_575_043_969, 2.0), (1_575_043_969 + 1000, 0.0)],
                histograms: Vec::new(),
            }],
            expected_metrics: r#"
                cortex_ingester_ingested_samples_total{user="test"} 2
                cortex_ingester_ingested_samples_failures_total{user="test"} 0
                cortex_ingester_memory_users 1
                cortex_ingester_memory_series 1
                cortex_ingester_memory_series_created_total{user="test"} 1
                cortex_ingester_memory_series_removed_total{user="test"} 0
                cortex_ingester_active_series{user="test"} 1
                cortex_ingester_tsdb_head_min_timestamp_seconds 1575043.969
                cortex_ingester_series_with_floats_and_histograms_total{user="test"} 1
                cortex_ingester_tsdb_head_max_timestamp_seconds 1575044.969
            "#
            .to_string(),
            ..Default::default()
        },
        // Case 20 (lines 3081..3141)
        PushTestCase {
            name: "should soft fail on some samples out of bound in a write request",
            reqs: vec![
                write_req_samples(
                    test_label_pairs(),
                    vec![cortexpb::Sample {
                        value: 2.0,
                        timestamp_ms: 1_575_043_969,
                    }],
                ),
                write_req_samples(
                    test_label_pairs(),
                    vec![
                        cortexpb::Sample {
                            value: 0.0,
                            timestamp_ms: 1_575_043_969 - (86400 * 1000),
                        },
                        cortexpb::Sample {
                            value: 1.0,
                            timestamp_ms: 1_575_043_969 - (86000 * 1000),
                        },
                        cortexpb::Sample {
                            value: 3.0,
                            timestamp_ms: 1_575_043_969 + 1,
                        },
                    ],
                ),
            ],
            expected_err: Some(ExpectedError {
                code: Code::InvalidArgument,
                cause: cortexpb::ErrorCause::BadData,
                soft: true,
                rejected_samples: 2,
                err_id: Some(SAMPLE_TIMESTAMP_TOO_OLD),
                contains: vec![
                    "1970-01-18T05:30:43.969Z".to_string(),
                    "series test".to_string(),
                ],
            }),
            expected_ingested: vec![ExpectedSeries {
                labels: test_labels(),
                samples: vec![(1_575_043_969, 2.0), (1_575_043_970, 3.0)],
                histograms: Vec::new(),
            }],
            expected_metrics: r#"
                cortex_ingester_ingested_samples_total{user="test"} 2
                cortex_ingester_ingested_samples_failures_total{user="test"} 2
                cortex_ingester_memory_users 1
                cortex_ingester_memory_series 1
                cortex_ingester_memory_series_created_total{user="test"} 1
                cortex_ingester_memory_series_removed_total{user="test"} 0
                cortex_discarded_samples_total{group="",reason="sample-timestamp-too-old",user="test"} 2
                cortex_ingester_active_series{user="test"} 1
                cortex_ingester_tsdb_head_min_timestamp_seconds 1575043.969
                cortex_ingester_tsdb_head_max_timestamp_seconds 1575043.970
            "#
            .to_string(),
            ..Default::default()
        },
        // Case 21 (lines 3142..3203)
        PushTestCase {
            name: "should soft fail on some samples with timestamp too far in future in a write request",
            reqs: vec![
                write_req_samples(
                    test_label_pairs(),
                    vec![cortexpb::Sample {
                        value: 1.0,
                        timestamp_ms: now_ms,
                    }],
                ),
                write_req_samples(
                    test_label_pairs(),
                    vec![
                        cortexpb::Sample {
                            value: 2.0,
                            timestamp_ms: now_ms + (86400 * 1000),
                        },
                        cortexpb::Sample {
                            value: 3.0,
                            timestamp_ms: now_ms + 1,
                        },
                    ],
                ),
            ],
            expected_err: Some(ExpectedError {
                code: Code::InvalidArgument,
                cause: cortexpb::ErrorCause::BadData,
                soft: true,
                rejected_samples: 1,
                err_id: Some(SAMPLE_TOO_FAR_IN_FUTURE),
                contains: vec!["too far in the future".to_string(), "series test".to_string()],
            }),
            expected_ingested: vec![ExpectedSeries {
                labels: test_labels(),
                samples: vec![(now_ms, 1.0), (now_ms + 1, 3.0)],
                histograms: Vec::new(),
            }],
            expected_metrics: format!(
                r#"
                cortex_ingester_ingested_samples_total{{user="test"}} 2
                cortex_ingester_ingested_samples_failures_total{{user="test"}} 1
                cortex_ingester_memory_users 1
                cortex_ingester_memory_series 1
                cortex_ingester_memory_series_created_total{{user="test"}} 1
                cortex_ingester_memory_series_removed_total{{user="test"}} 0
                cortex_discarded_samples_total{{group="",reason="sample-too-far-in-future",user="test"}} 1
                cortex_ingester_active_series{{user="test"}} 1
                cortex_ingester_tsdb_head_min_timestamp_seconds {now_s}
                cortex_ingester_tsdb_head_max_timestamp_seconds {now_plus_1_s}
                "#
            ),
            ..Default::default()
        },
        // Case 22 (lines 3204..3264)
        PushTestCase {
            name: "should soft fail on some histograms with timestamp too far in future in a write request",
            native_histograms: true,
            reqs: vec![write_req_histogram(
                test_label_pairs(),
                vec![
                    histogram_to_proto(now_ms, &generate_test_histogram(0)),
                    histogram_to_proto(now_ms + (86400 * 1000), &generate_test_histogram(1)),
                ],
                Vec::new(),
            )],
            expected_err: Some(ExpectedError {
                code: Code::InvalidArgument,
                cause: cortexpb::ErrorCause::BadData,
                soft: true,
                rejected_samples: 1,
                err_id: Some(SAMPLE_TOO_FAR_IN_FUTURE),
                contains: vec!["too far in the future".to_string(), "series test".to_string()],
            }),
            expected_ingested: vec![ExpectedSeries {
                labels: test_labels(),
                samples: Vec::new(),
                histograms: vec![(now_ms, generate_test_histogram(0))],
            }],
            expected_metrics: format!(
                r#"
                cortex_ingester_ingested_samples_total{{user="test"}} 1
                cortex_ingester_ingested_samples_failures_total{{user="test"}} 1
                cortex_ingester_memory_users 1
                cortex_ingester_memory_series 1
                cortex_ingester_memory_series_created_total{{user="test"}} 1
                cortex_ingester_memory_series_removed_total{{user="test"}} 0
                cortex_discarded_samples_total{{group="",reason="sample-too-far-in-future",user="test"}} 1
                cortex_ingester_active_series{{user="test"}} 1
                cortex_ingester_active_native_histogram_buckets{{user="test"}} 8
                cortex_ingester_active_native_histogram_series{{user="test"}} 1
                cortex_ingester_tsdb_head_min_timestamp_seconds {now_s}
                cortex_ingester_tsdb_head_max_timestamp_seconds {now_s}
                "#
            ),
            ..Default::default()
        },
        // Case 23 (lines 3265..3340)
        PushTestCase {
            name: "should soft fail on some exemplars with timestamp too far in future in a write request",
            max_exemplars: 1,
            reqs: vec![cortexpb::WriteRequest {
                timeseries: vec![cortexpb::TimeSeries {
                    labels: test_label_pairs(),
                    samples: vec![cortexpb::Sample {
                        value: 1.0,
                        timestamp_ms: now_ms,
                    }],
                    exemplars: vec![
                        make_exemplar("111", now_ms, 1.0),
                        make_exemplar("222", now_ms + (86400 * 1000), 2.0),
                    ],
                    ..Default::default()
                }],
                source: cortexpb::write_request::SourceEnum::Api as i32,
                ..Default::default()
            }],
            expected_err: Some(ExpectedError {
                code: Code::InvalidArgument,
                cause: cortexpb::ErrorCause::BadData,
                soft: true,
                rejected_samples: 0,
                err_id: Some(EXEMPLAR_TOO_FAR_IN_FUTURE),
                contains: vec![
                    "received an exemplar whose timestamp is too far in the future".to_string(),
                    r#"{traceID="222"}"#.to_string(),
                    "for series test".to_string(),
                ],
            }),
            expected_ingested: vec![ExpectedSeries {
                labels: test_labels(),
                samples: vec![(now_ms, 1.0)],
                histograms: Vec::new(),
            }],
            expected_exemplars_ingested: vec![ExpectedExemplarSeries {
                labels: test_label_pairs(),
                exemplars: vec![make_exemplar("111", now_ms, 1.0)],
            }],
            additional_metrics: vec![
                "cortex_ingester_ingested_exemplars_total",
                "cortex_ingester_ingested_exemplars_failures_total",
            ],
            expected_metrics: format!(
                r#"
                cortex_ingester_ingested_samples_total{{user="test"}} 1
                cortex_ingester_ingested_samples_failures_total{{user="test"}} 0
                cortex_ingester_memory_users 1
                cortex_ingester_memory_series 1
                cortex_ingester_memory_series_created_total{{user="test"}} 1
                cortex_ingester_memory_series_removed_total{{user="test"}} 0
                cortex_ingester_active_series{{user="test"}} 1
                cortex_ingester_ingested_exemplars_total 1
                cortex_ingester_ingested_exemplars_failures_total 1
                cortex_ingester_tsdb_head_min_timestamp_seconds {now_s}
                cortex_ingester_tsdb_head_max_timestamp_seconds {now_s}
                "#
            ),
            ..Default::default()
        },
        // Case 24 (lines 3341..3418)
        PushTestCase {
            name: "should soft fail on the first failing exemplar of a series in request order",
            max_exemplars: 1,
            reqs: vec![cortexpb::WriteRequest {
                timeseries: vec![cortexpb::TimeSeries {
                    labels: test_label_pairs(),
                    samples: vec![cortexpb::Sample {
                        value: 1.0,
                        timestamp_ms: now_ms,
                    }],
                    exemplars: vec![
                        make_exemplar("111", now_ms, 1.0),
                        make_exemplar(&long_trace_id, now_ms, 2.0),
                        make_exemplar("333", now_ms + (86400 * 1000), 3.0),
                    ],
                    ..Default::default()
                }],
                source: cortexpb::write_request::SourceEnum::Api as i32,
                ..Default::default()
            }],
            expected_err: Some(ExpectedError {
                code: Code::InvalidArgument,
                cause: cortexpb::ErrorCause::BadData,
                soft: true,
                rejected_samples: 0,
                err_id: None,
                contains: vec![
                    "label length for exemplar exceeds maximum of 128 UTF-8 characters".to_string(),
                    "series=test".to_string(),
                    format!(r#"exemplar={{traceID="{long_trace_id}"}}"#),
                ],
            }),
            expected_ingested: vec![ExpectedSeries {
                labels: test_labels(),
                samples: vec![(now_ms, 1.0)],
                histograms: Vec::new(),
            }],
            expected_exemplars_ingested: vec![ExpectedExemplarSeries {
                labels: test_label_pairs(),
                exemplars: vec![make_exemplar("111", now_ms, 1.0)],
            }],
            additional_metrics: vec![
                "cortex_ingester_ingested_exemplars_total",
                "cortex_ingester_ingested_exemplars_failures_total",
            ],
            expected_metrics: format!(
                r#"
                cortex_ingester_ingested_samples_total{{user="test"}} 1
                cortex_ingester_ingested_samples_failures_total{{user="test"}} 0
                cortex_ingester_memory_users 1
                cortex_ingester_memory_series 1
                cortex_ingester_memory_series_created_total{{user="test"}} 1
                cortex_ingester_memory_series_removed_total{{user="test"}} 0
                cortex_ingester_active_series{{user="test"}} 1
                cortex_ingester_ingested_exemplars_total 1
                cortex_ingester_ingested_exemplars_failures_total 2
                cortex_ingester_tsdb_head_min_timestamp_seconds {now_s}
                cortex_ingester_tsdb_head_max_timestamp_seconds {now_s}
                "#
            ),
            ..Default::default()
        },
        // Case 25 (lines 3419..3500)
        PushTestCase {
            name: "should match TSDB's exemplar errors to their exemplars when an earlier exemplar is too far in the past",
            max_exemplars: 1,
            past_grace_period: Duration::from_secs(3600),
            reqs: vec![cortexpb::WriteRequest {
                timeseries: vec![cortexpb::TimeSeries {
                    labels: test_label_pairs(),
                    samples: vec![cortexpb::Sample {
                        value: 1.0,
                        timestamp_ms: now_ms,
                    }],
                    exemplars: vec![
                        make_exemplar("111", now_ms - two_hours_ms, 1.0),
                        make_exemplar(&long_trace_id, now_ms, 2.0),
                        make_exemplar("333", now_ms, 3.0),
                    ],
                    ..Default::default()
                }],
                source: cortexpb::write_request::SourceEnum::Api as i32,
                ..Default::default()
            }],
            expected_err: Some(ExpectedError {
                code: Code::InvalidArgument,
                cause: cortexpb::ErrorCause::BadData,
                soft: true,
                rejected_samples: 0,
                err_id: Some(EXEMPLAR_TOO_FAR_IN_PAST),
                contains: vec![
                    "received an exemplar whose timestamp is too far in the past".to_string(),
                    r#"{traceID="111"}"#.to_string(),
                    "for series test".to_string(),
                ],
            }),
            expected_ingested: vec![ExpectedSeries {
                labels: test_labels(),
                samples: vec![(now_ms, 1.0)],
                histograms: Vec::new(),
            }],
            expected_exemplars_ingested: vec![ExpectedExemplarSeries {
                labels: test_label_pairs(),
                exemplars: vec![make_exemplar("333", now_ms, 3.0)],
            }],
            additional_metrics: vec![
                "cortex_ingester_ingested_exemplars_total",
                "cortex_ingester_ingested_exemplars_failures_total",
            ],
            expected_metrics: format!(
                r#"
                cortex_ingester_ingested_samples_total{{user="test"}} 1
                cortex_ingester_ingested_samples_failures_total{{user="test"}} 0
                cortex_ingester_memory_users 1
                cortex_ingester_memory_series 1
                cortex_ingester_memory_series_created_total{{user="test"}} 1
                cortex_ingester_memory_series_removed_total{{user="test"}} 0
                cortex_ingester_active_series{{user="test"}} 1
                cortex_ingester_ingested_exemplars_total 1
                cortex_ingester_ingested_exemplars_failures_total 2
                cortex_ingester_tsdb_head_min_timestamp_seconds {now_s}
                cortex_ingester_tsdb_head_max_timestamp_seconds {now_s}
                "#
            ),
            ..Default::default()
        },
        // Case 26 (lines 3501..3555)
        PushTestCase {
            name: "should soft fail on two different sample values at the same timestamp",
            reqs: vec![
                write_req_samples(
                    test_label_pairs(),
                    vec![cortexpb::Sample {
                        value: 2.0,
                        timestamp_ms: 1_575_043_969,
                    }],
                ),
                write_req_samples(
                    test_label_pairs(),
                    vec![cortexpb::Sample {
                        value: 1.0,
                        timestamp_ms: 1_575_043_969,
                    }],
                ),
            ],
            expected_err: Some(ExpectedError {
                code: Code::InvalidArgument,
                cause: cortexpb::ErrorCause::BadData,
                soft: true,
                rejected_samples: 1,
                err_id: Some(SAMPLE_DUPLICATE_TIMESTAMP),
                contains: vec![
                    "duplicate sample for timestamp 1575043969; overrides not allowed: existing 2, new value 1".to_string(),
                    "1970-01-19T05:30:43.969Z".to_string(),
                    "series test".to_string(),
                ],
            }),
            expected_ingested: vec![ExpectedSeries {
                labels: test_labels(),
                samples: vec![(1_575_043_969, 2.0)],
                histograms: Vec::new(),
            }],
            expected_metrics: r#"
                cortex_ingester_ingested_samples_total{user="test"} 1
                cortex_ingester_ingested_samples_failures_total{user="test"} 1
                cortex_ingester_memory_users 1
                cortex_ingester_memory_series 1
                cortex_ingester_memory_series_created_total{user="test"} 1
                cortex_ingester_memory_series_removed_total{user="test"} 0
                cortex_discarded_samples_total{group="",reason="new-value-for-timestamp",user="test"} 1
                cortex_ingester_active_series{user="test"} 1
                cortex_ingester_tsdb_head_min_timestamp_seconds 1575043.969
                cortex_ingester_tsdb_head_max_timestamp_seconds 1575043.969
            "#
            .to_string(),
            ..Default::default()
        },
        // Case 27 (lines 3556..3645)
        PushTestCase {
            name: "should soft fail on exemplar with unknown series",
            max_exemplars: 1,
            reqs: vec![cortexpb::WriteRequest {
                timeseries: vec![cortexpb::TimeSeries {
                    labels: test_label_pairs(),
                    exemplars: vec![make_exemplar("123", 1000, 1000.0)],
                    ..Default::default()
                }],
                source: cortexpb::write_request::SourceEnum::Api as i32,
                ..Default::default()
            }],
            expected_err: Some(ExpectedError {
                code: Code::InvalidArgument,
                cause: cortexpb::ErrorCause::BadData,
                soft: true,
                rejected_samples: 0,
                err_id: Some(EXEMPLAR_SERIES_MISSING),
                contains: vec![
                    "the exemplar has been rejected because the related series has not been ingested yet".to_string(),
                    r#"{traceID="123"}"#.to_string(),
                    "1970-01-01T00:00:01Z".to_string(),
                    "for series test".to_string(),
                ],
            }),
            additional_metrics: vec![
                "cortex_ingester_tsdb_exemplar_exemplars_appended_total",
                "cortex_ingester_tsdb_exemplar_exemplars_in_storage",
                "cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage",
                "cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds",
                "cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total",
                "cortex_ingester_ingested_exemplars_total",
                "cortex_ingester_ingested_exemplars_failures_total",
            ],
            expected_metrics: r#"
                cortex_ingester_ingested_samples_total{user="test"} 0
                cortex_ingester_ingested_samples_failures_total{user="test"} 0
                cortex_ingester_memory_users 1
                cortex_ingester_memory_series 0
                cortex_ingester_memory_series_created_total{user="test"} 0
                cortex_ingester_memory_series_removed_total{user="test"} 0
                cortex_ingester_tsdb_exemplar_exemplars_appended_total{user="test"} 0
                cortex_ingester_tsdb_exemplar_exemplars_in_storage 0
                cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage{user="test"} 0
                cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds{user="test"} 0
                cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total 0
                cortex_ingester_tsdb_head_min_timestamp_seconds 0
                cortex_ingester_tsdb_head_max_timestamp_seconds 0
                cortex_ingester_ingested_exemplars_total 0
                cortex_ingester_ingested_exemplars_failures_total 1
            "#
            .to_string(),
            ..Default::default()
        },
        // Case 28 (lines 3646..3749)
        PushTestCase {
            name: "should soft fail on exemplar with series later in the same write request",
            max_exemplars: 1,
            reqs: vec![cortexpb::WriteRequest {
                timeseries: vec![
                    cortexpb::TimeSeries {
                        labels: test_label_pairs(),
                        exemplars: vec![make_exemplar("123", 1000, 1000.0)],
                        ..Default::default()
                    },
                    cortexpb::TimeSeries {
                        labels: test_label_pairs(),
                        samples: vec![cortexpb::Sample {
                            value: 1.0,
                            timestamp_ms: 9,
                        }],
                        ..Default::default()
                    },
                ],
                source: cortexpb::write_request::SourceEnum::Api as i32,
                ..Default::default()
            }],
            expected_err: Some(ExpectedError {
                code: Code::InvalidArgument,
                cause: cortexpb::ErrorCause::BadData,
                soft: true,
                rejected_samples: 0,
                err_id: Some(EXEMPLAR_SERIES_MISSING),
                contains: vec![
                    "the exemplar has been rejected because the related series has not been ingested yet".to_string(),
                    r#"{traceID="123"}"#.to_string(),
                    "1970-01-01T00:00:01Z".to_string(),
                    "for series test".to_string(),
                ],
            }),
            expected_ingested: vec![ExpectedSeries {
                labels: test_labels(),
                samples: vec![(9, 1.0)],
                histograms: Vec::new(),
            }],
            additional_metrics: vec![
                "cortex_ingester_tsdb_exemplar_exemplars_appended_total",
                "cortex_ingester_tsdb_exemplar_exemplars_in_storage",
                "cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage",
                "cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds",
                "cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total",
                "cortex_ingester_ingested_exemplars_total",
                "cortex_ingester_ingested_exemplars_failures_total",
            ],
            expected_metrics: r#"
                cortex_ingester_active_series{user="test"} 1
                cortex_ingester_ingested_samples_total{user="test"} 1
                cortex_ingester_ingested_samples_failures_total{user="test"} 0
                cortex_ingester_memory_users 1
                cortex_ingester_memory_series 1
                cortex_ingester_memory_series_created_total{user="test"} 1
                cortex_ingester_memory_series_removed_total{user="test"} 0
                cortex_ingester_tsdb_exemplar_exemplars_appended_total{user="test"} 0
                cortex_ingester_tsdb_exemplar_exemplars_in_storage 0
                cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage{user="test"} 0
                cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds{user="test"} 0
                cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total 0
                cortex_ingester_tsdb_head_min_timestamp_seconds 0.009
                cortex_ingester_tsdb_head_max_timestamp_seconds 0.009
                cortex_ingester_ingested_exemplars_total 0
                cortex_ingester_ingested_exemplars_failures_total 1
            "#
            .to_string(),
            ..Default::default()
        },
    ]
}

// Upstream: pkg/ingester/ingester_test.go::TestIngester_Push
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_ingester_push_part1b() {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let cases = build_push_cases(now_ms);
    assert_eq!(cases.len(), 28);
    for tc in &cases {
        run_push_case(tc, PushMode::Rw1).await;
        run_push_case(tc, PushMode::Rw2).await;
    }
}
