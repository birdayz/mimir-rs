//! Ported from github.com/grafana/mimir/pkg/ingester/ingester_test.go (lines 3750..5375).

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mimir_common::block_on_for_tests;
use mimir_config::{
    BlocksStorageConfig, GoDuration, IngestStorageConfig, IngesterConfig, InstanceLimits,
    LimitsConfig, ModelDuration, Overrides,
};
use mimir_ingester::Ingester;
use mimir_proto::cortexpb;
use mimir_proto::ingesterpb;
use mimir_proto::prost::Message;
use mimir_proto::timeseries::PreallocWriteRequest;
use mimir_proto::zerocopy::{
    decode_write_request, from_write_request_to_rw2, DecodeMode, DecodeOptions,
    WriteRequestScratch,
};
use mimir_tsdb::chunkenc::{
    CounterResetHint, EncodedChunk, Encoding, FloatHistogram, Histogram, HistogramSpan,
};
use prometheus::proto::{MetricFamily, MetricType};
use prometheus::Registry;
use tonic::metadata::MetadataMap;

const METRIC_NAME_LABEL: &str = "__name__";

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn default_ingester_test_config(tsdb_dir: &Path) -> (IngesterConfig, BlocksStorageConfig) {
    let mut cfg = IngesterConfig::default();
    cfg.ring.instance_id = "localhost".to_string();
    cfg.ring.instance_addr = "127.0.0.1".to_string();
    cfg.ring.num_tokens = 128;
    cfg.ring.heartbeat_period = GoDuration::from_millis(50);
    cfg.ring.min_ready_duration = GoDuration::ZERO;
    cfg.ring.final_sleep = GoDuration::ZERO;
    cfg.ring.unregister_on_shutdown = true;
    cfg.ring.replication_factor = 1;
    cfg.active_series.active_series_metrics_enabled = true;
    cfg.active_series.active_series_metrics_update_period = GoDuration::from_secs(60);
    cfg.active_series.active_series_metrics_idle_timeout = GoDuration::from_secs(600);

    let mut blocks = BlocksStorageConfig::default();
    blocks.tsdb.dir = tsdb_dir.to_string_lossy().into_owned();
    blocks.tsdb.block_ranges_period = vec![GoDuration::from_hours(2)];
    blocks.tsdb.head_compaction_interval = GoDuration::from_hours(1);
    blocks.tsdb.head_compaction_idle_timeout = GoDuration::from_hours(1);
    blocks.tsdb.ship_interval = GoDuration::ZERO;
    blocks.tsdb.head_compaction_interval_jitter_enabled = false;
    (cfg, blocks)
}

fn default_limits_test_config() -> LimitsConfig {
    LimitsConfig {
        max_global_series_per_user: 0,
        max_global_series_per_metric: 0,
        max_global_metadata_per_user: 0,
        max_global_metadata_per_metric: 0,
        ..Default::default()
    }
}

async fn prepare_ingester_with_blocks_storage(
    cfg: IngesterConfig,
    blocks: BlocksStorageConfig,
    overrides: Overrides,
    registry: Option<&Registry>,
) -> Arc<Ingester> {
    let ing = Ingester::new(
        cfg,
        blocks,
        IngestStorageConfig::default(),
        overrides,
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

fn generate_test_histogram(i: i64) -> Histogram {
    Histogram {
        schema: 1,
        zero_threshold: 0.001,
        zero_count: (2 + i) as u64,
        count: (12 + i * 9) as u64,
        sum: 18.4 * (i + 1) as f64,
        positive_spans: vec![
            HistogramSpan {
                offset: 0,
                length: 2,
            },
            HistogramSpan {
                offset: 1,
                length: 2,
            },
        ],
        positive_buckets: vec![i + 1, 1, -1, 0],
        negative_spans: vec![
            HistogramSpan {
                offset: 0,
                length: 2,
            },
            HistogramSpan {
                offset: 1,
                length: 2,
            },
        ],
        negative_buckets: vec![i + 1, 1, -1, 0],
        ..Default::default()
    }
}

fn generate_test_gauge_histogram(i: i64) -> Histogram {
    let mut h = generate_test_histogram(i);
    h.counter_reset_hint = CounterResetHint::GaugeType;
    h
}

fn int_histogram_to_proto(ts: i64, h: &Histogram) -> cortexpb::Histogram {
    let spans = |ss: &[HistogramSpan]| -> Vec<cortexpb::BucketSpan> {
        ss.iter()
            .map(|s| cortexpb::BucketSpan {
                offset: s.offset,
                length: s.length,
            })
            .collect()
    };
    let reset_hint = match h.counter_reset_hint {
        CounterResetHint::UnknownCounterReset => cortexpb::histogram::ResetHint::Unknown as i32,
        CounterResetHint::CounterReset => cortexpb::histogram::ResetHint::Yes as i32,
        CounterResetHint::NotCounterReset => cortexpb::histogram::ResetHint::No as i32,
        CounterResetHint::GaugeType => cortexpb::histogram::ResetHint::Gauge as i32,
    };
    cortexpb::Histogram {
        count: Some(cortexpb::histogram::Count::CountInt(h.count)),
        zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountInt(h.zero_count)),
        zero_threshold: h.zero_threshold,
        sum: h.sum,
        schema: h.schema,
        positive_spans: spans(&h.positive_spans),
        positive_deltas: h.positive_buckets.clone(),
        negative_spans: spans(&h.negative_spans),
        negative_deltas: h.negative_buckets.clone(),
        reset_hint,
        timestamp: ts,
        ..Default::default()
    }
}

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

#[derive(Debug, Clone, PartialEq)]
struct DecodedSeries {
    labels: BTreeMap<String, String>,
    floats: Vec<(i64, f64)>,
    histograms: Vec<(i64, FloatHistogram)>,
}

fn compact_zero_buckets(mut fh: FloatHistogram) -> FloatHistogram {
    if fh.positive_buckets.iter().all(|&b| b == 0.0) {
        fh.positive_spans.clear();
        fh.positive_buckets.clear();
    }
    if fh.negative_buckets.iter().all(|&b| b == 0.0) {
        fh.negative_spans.clear();
        fh.negative_buckets.clear();
    }
    fh
}

fn decode_query_stream_responses(
    responses: &[ingesterpb::QueryStreamResponse],
) -> Vec<DecodedSeries> {
    let mut series_list: Vec<DecodedSeries> = Vec::new();
    for resp in responses {
        for s in &resp.streaming_series {
            let mut lbls = BTreeMap::new();
            for lp in &s.labels {
                lbls.insert(
                    String::from_utf8_lossy(&lp.name).into_owned(),
                    String::from_utf8_lossy(&lp.value).into_owned(),
                );
            }
            series_list.push(DecodedSeries {
                labels: lbls,
                floats: Vec::new(),
                histograms: Vec::new(),
            });
        }
        for sc in &resp.streaming_series_chunks {
            let entry = &mut series_list[sc.series_index as usize];
            for ch in &sc.chunks {
                let enc = wire_to_tsdb_encoding(ch.encoding);
                let chunk = EncodedChunk::from_bytes(enc, ch.data.to_vec()).expect("decode chunk");
                match chunk {
                    EncodedChunk::Xor(c) => {
                        let mut it = c.iter();
                        while let Some((t, v)) = it.next_sample().expect("xor next") {
                            entry.floats.push((t, v));
                        }
                    }
                    EncodedChunk::Xor2(c) => {
                        let mut it = c.iter();
                        while let Some((t, v)) = it.next_sample().expect("xor2 next") {
                            entry.floats.push((t, v));
                        }
                    }
                    EncodedChunk::Histogram(c) => {
                        let mut it = c.iter();
                        while let Some((t, h)) = it.next_histogram().expect("hist next") {
                            let mut fh = h.to_float();
                            fh.counter_reset_hint = CounterResetHint::UnknownCounterReset;
                            entry.histograms.push((t, compact_zero_buckets(fh)));
                        }
                    }
                    EncodedChunk::HistogramSt(c) => {
                        let mut it = c.iter();
                        while let Some((t, h)) = it.next_histogram().expect("hist st next") {
                            let mut fh = h.to_float();
                            fh.counter_reset_hint = CounterResetHint::UnknownCounterReset;
                            entry.histograms.push((t, compact_zero_buckets(fh)));
                        }
                    }
                    EncodedChunk::FloatHistogram(c) => {
                        let mut it = c.iter();
                        while let Some((t, mut fh)) = it.next_float_histogram().expect("fhist next")
                        {
                            fh.counter_reset_hint = CounterResetHint::UnknownCounterReset;
                            entry.histograms.push((t, compact_zero_buckets(fh)));
                        }
                    }
                    EncodedChunk::FloatHistogramSt(c) => {
                        let mut it = c.iter();
                        while let Some((t, mut fh)) =
                            it.next_float_histogram().expect("fhist st next")
                        {
                            fh.counter_reset_hint = CounterResetHint::UnknownCounterReset;
                            entry.histograms.push((t, compact_zero_buckets(fh)));
                        }
                    }
                }
            }
        }
    }
    for s in &mut series_list {
        s.floats.sort_by_key(|(t, _)| *t);
        s.histograms.sort_by_key(|(t, _)| *t);
    }
    series_list.sort_by(|a, b| a.labels.cmp(&b.labels));
    series_list
}

async fn query_all_series(ing: &Ingester, user_id: &str) -> Vec<DecodedSeries> {
    let req = ingesterpb::QueryRequest {
        start_timestamp_ms: i64::MIN,
        end_timestamp_ms: i64::MAX,
        matchers: vec![ingesterpb::LabelMatcher {
            r#type: ingesterpb::MatchType::RegexMatch as i32,
            name: METRIC_NAME_LABEL.to_string(),
            value: ".*".to_string(),
        }],
        streaming_chunks_batch_size: 0,
    };
    let responses = ing
        .handle_query_stream(&MetadataMap::new(), user_id, req)
        .await
        .expect("query_stream");
    decode_query_stream_responses(&responses)
}

fn find_family<'a>(families: &'a [MetricFamily], name: &str) -> Option<&'a MetricFamily> {
    families.iter().find(|mf| mf.get_name() == name)
}

fn metric_value_map(families: &[MetricFamily], name: &str) -> BTreeMap<Vec<(String, String)>, f64> {
    let mut out = BTreeMap::new();
    let Some(mf) = find_family(families, name) else {
        return out;
    };
    let ty = mf.get_field_type();
    for m in mf.get_metric() {
        let mut lbls: Vec<(String, String)> = m
            .get_label()
            .iter()
            .map(|lp| (lp.get_name().to_string(), lp.get_value().to_string()))
            .collect();
        lbls.sort();
        let val = match ty {
            MetricType::COUNTER => m.get_counter().get_value(),
            MetricType::GAUGE => m.get_gauge().get_value(),
            MetricType::UNTYPED => m.get_untyped().get_value(),
            _ => continue,
        };
        out.insert(lbls, val);
    }
    out
}

fn single_metric_value(families: &[MetricFamily], name: &str) -> Option<f64> {
    metric_value_map(families, name).get(&Vec::new()).copied()
}

fn zero_float_histogram_for_proto(h: &Histogram) -> FloatHistogram {
    FloatHistogram {
        counter_reset_hint: CounterResetHint::UnknownCounterReset,
        schema: h.schema,
        zero_threshold: h.zero_threshold,
        custom_values: h.custom_values.clone(),
        ..FloatHistogram::default()
    }
}

fn encode_rw2_bytes(rw1: &cortexpb::WriteRequest) -> Vec<u8> {
    let rw2 = from_write_request_to_rw2(rw1, 0, None).expect("convert to rw2");
    rw2.encode_to_vec()
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum WireMode {
    Rw1,
    Rw2Prealloc,
    Rw2Borrowed,
}

type ExpectedMetricEntries = Vec<(Vec<(&'static str, &'static str)>, f64)>;

#[derive(Clone)]
struct PushCase {
    name: &'static str,
    reqs: Vec<cortexpb::WriteRequest>,
    expected_ingested: Vec<DecodedSeries>,
    expected_metadata_ingested: Vec<cortexpb::MetricMetadata>,
    expected_metrics: Vec<(&'static str, ExpectedMetricEntries)>,
    absent_metrics: Vec<&'static str>,
    max_exemplars: i64,
    max_metadata_per_user: i64,
    max_metadata_per_metric: i64,
    native_histograms: bool,
    allow_ooo: bool,
}

fn metric_labels_map() -> BTreeMap<String, String> {
    BTreeMap::from([(METRIC_NAME_LABEL.to_string(), "test".to_string())])
}

fn metric_label_pairs() -> Vec<cortexpb::LabelPair> {
    vec![cortexpb::LabelPair::from_strs(METRIC_NAME_LABEL, "test")]
}

fn build_final_10_push_cases() -> Vec<PushCase> {
    let h1 = generate_test_histogram(1);
    let h2 = generate_test_histogram(2);
    let h3 = generate_test_histogram(3);
    let h4 = generate_test_histogram(4);
    let zero_fh = zero_float_histogram_for_proto(&h1);

    let mut fh1 = h1.to_float();
    fh1.counter_reset_hint = CounterResetHint::UnknownCounterReset;
    let mut fh2 = h2.to_float();
    fh2.counter_reset_hint = CounterResetHint::UnknownCounterReset;
    let mut fh3 = h3.to_float();
    fh3.counter_reset_hint = CounterResetHint::UnknownCounterReset;
    let mut fh4 = h4.to_float();
    fh4.counter_reset_hint = CounterResetHint::UnknownCounterReset;

    vec![
        // 1. "should succeed with a request containing only metadata"
        PushCase {
            name: "should succeed with a request containing only metadata",
            max_exemplars: 1,
            max_metadata_per_user: 0,
            max_metadata_per_metric: 0,
            native_histograms: false,
            allow_ooo: false,
            reqs: vec![cortexpb::WriteRequest {
                metadata: vec![cortexpb::MetricMetadata {
                    r#type: cortexpb::metric_metadata::MetricType::Counter as i32,
                    metric_family_name: "test_metric".to_string(),
                    help: "This is a test metric.".to_string(),
                    ..Default::default()
                }],
                source: cortexpb::write_request::SourceEnum::Api as i32,
                ..Default::default()
            }],
            expected_ingested: vec![],
            expected_metadata_ingested: vec![cortexpb::MetricMetadata {
                r#type: cortexpb::metric_metadata::MetricType::Counter as i32,
                metric_family_name: "test_metric".to_string(),
                help: "This is a test metric.".to_string(),
                ..Default::default()
            }],
            expected_metrics: vec![
                ("cortex_ingester_memory_series", vec![(vec![], 0.0)]),
                ("cortex_ingester_memory_users", vec![(vec![], 0.0)]),
                ("cortex_ingester_tsdb_head_active_appenders", vec![(vec![], 0.0)]),
                ("cortex_ingester_tsdb_head_min_timestamp_seconds", vec![(vec![], 0.0)]),
                ("cortex_ingester_tsdb_head_max_timestamp_seconds", vec![(vec![], 0.0)]),
            ],
            absent_metrics: vec![],
        },
        // 2. "should discard metadata when max metadata per user exceeded"
        PushCase {
            name: "should discard metadata when max metadata per user exceeded",
            max_exemplars: 0,
            max_metadata_per_user: 1,
            max_metadata_per_metric: 0,
            native_histograms: false,
            allow_ooo: false,
            reqs: vec![cortexpb::WriteRequest {
                metadata: vec![
                    cortexpb::MetricMetadata {
                        r#type: cortexpb::metric_metadata::MetricType::Counter as i32,
                        metric_family_name: "test_metric_1".to_string(),
                        help: "This is a test metric.".to_string(),
                        ..Default::default()
                    },
                    cortexpb::MetricMetadata {
                        r#type: cortexpb::metric_metadata::MetricType::Counter as i32,
                        metric_family_name: "test_metric_2".to_string(),
                        help: "This is a test metric.".to_string(),
                        ..Default::default()
                    },
                ],
                source: cortexpb::write_request::SourceEnum::Api as i32,
                ..Default::default()
            }],
            expected_ingested: vec![],
            expected_metadata_ingested: vec![cortexpb::MetricMetadata {
                r#type: cortexpb::metric_metadata::MetricType::Counter as i32,
                metric_family_name: "test_metric_1".to_string(),
                help: "This is a test metric.".to_string(),
                ..Default::default()
            }],
            expected_metrics: vec![
                (
                    "cortex_discarded_metadata_total",
                    vec![(vec![("reason", "per_user_metadata_limit"), ("user", "test")], 1.0)],
                ),
                ("cortex_ingester_ingested_metadata_failures_total", vec![(vec![], 1.0)]),
                ("cortex_ingester_ingested_metadata_total", vec![(vec![], 1.0)]),
                ("cortex_ingester_memory_metadata", vec![(vec![], 1.0)]),
                (
                    "cortex_ingester_memory_metadata_created_total",
                    vec![(vec![("user", "test")], 1.0)],
                ),
                ("cortex_ingester_memory_series", vec![(vec![], 0.0)]),
                ("cortex_ingester_memory_users", vec![(vec![], 0.0)]),
                ("cortex_ingester_tsdb_head_min_timestamp_seconds", vec![(vec![], 0.0)]),
                ("cortex_ingester_tsdb_head_max_timestamp_seconds", vec![(vec![], 0.0)]),
            ],
            absent_metrics: vec![],
        },
        // 3. "should discard metadata when max metadata per metric exceeded"
        PushCase {
            name: "should discard metadata when max metadata per metric exceeded",
            max_exemplars: 0,
            max_metadata_per_user: 0,
            max_metadata_per_metric: 1,
            native_histograms: false,
            allow_ooo: false,
            reqs: vec![cortexpb::WriteRequest {
                metadata: vec![
                    cortexpb::MetricMetadata {
                        r#type: cortexpb::metric_metadata::MetricType::Counter as i32,
                        metric_family_name: "test_metric_1".to_string(),
                        help: "This is a test metric.".to_string(),
                        ..Default::default()
                    },
                    cortexpb::MetricMetadata {
                        r#type: cortexpb::metric_metadata::MetricType::Counter as i32,
                        metric_family_name: "test_metric_1".to_string(),
                        help: "A test metric this is.".to_string(),
                        ..Default::default()
                    },
                ],
                source: cortexpb::write_request::SourceEnum::Api as i32,
                ..Default::default()
            }],
            expected_ingested: vec![],
            expected_metadata_ingested: vec![cortexpb::MetricMetadata {
                r#type: cortexpb::metric_metadata::MetricType::Counter as i32,
                metric_family_name: "test_metric_1".to_string(),
                help: "This is a test metric.".to_string(),
                ..Default::default()
            }],
            expected_metrics: vec![
                (
                    "cortex_discarded_metadata_total",
                    vec![(vec![("reason", "per_metric_metadata_limit"), ("user", "test")], 1.0)],
                ),
                ("cortex_ingester_ingested_metadata_failures_total", vec![(vec![], 1.0)]),
                ("cortex_ingester_ingested_metadata_total", vec![(vec![], 1.0)]),
                ("cortex_ingester_memory_metadata", vec![(vec![], 1.0)]),
                (
                    "cortex_ingester_memory_metadata_created_total",
                    vec![(vec![("user", "test")], 1.0)],
                ),
                ("cortex_ingester_memory_series", vec![(vec![], 0.0)]),
                ("cortex_ingester_memory_users", vec![(vec![], 0.0)]),
                ("cortex_ingester_tsdb_head_min_timestamp_seconds", vec![(vec![], 0.0)]),
                ("cortex_ingester_tsdb_head_max_timestamp_seconds", vec![(vec![], 0.0)]),
            ],
            absent_metrics: vec![],
        },
        // 4. "should ingest created timestamp with correct type"
        PushCase {
            name: "should ingest created timestamp with correct type",
            max_exemplars: 0,
            max_metadata_per_user: 0,
            max_metadata_per_metric: 0,
            native_histograms: true,
            allow_ooo: true,
            reqs: vec![
                cortexpb::WriteRequest {
                    timeseries: vec![cortexpb::TimeSeries {
                        labels: metric_label_pairs(),
                        samples: vec![cortexpb::Sample {
                            value: 1.0,
                            timestamp_ms: 600,
                        }],
                        created_timestamp: 500,
                        ..Default::default()
                    }],
                    source: cortexpb::write_request::SourceEnum::Api as i32,
                    ..Default::default()
                },
                cortexpb::WriteRequest {
                    timeseries: vec![cortexpb::TimeSeries {
                        labels: metric_label_pairs(),
                        histograms: vec![int_histogram_to_proto(800, &h1)],
                        created_timestamp: 700,
                        ..Default::default()
                    }],
                    source: cortexpb::write_request::SourceEnum::Api as i32,
                    ..Default::default()
                },
            ],
            expected_ingested: vec![DecodedSeries {
                labels: metric_labels_map(),
                floats: vec![(500, 0.0), (600, 1.0)],
                histograms: vec![(700, zero_fh.clone()), (800, fh1.clone())],
            }],
            expected_metadata_ingested: vec![],
            expected_metrics: vec![
                (
                    "cortex_ingester_active_native_histogram_buckets",
                    vec![(vec![("user", "test")], 8.0)],
                ),
                (
                    "cortex_ingester_active_native_histogram_series",
                    vec![(vec![("user", "test")], 1.0)],
                ),
                (
                    "cortex_ingester_active_series",
                    vec![(vec![("user", "test")], 1.0)],
                ),
                (
                    "cortex_ingester_ingested_samples_failures_total",
                    vec![(vec![("user", "test")], 0.0)],
                ),
                (
                    "cortex_ingester_ingested_samples_total",
                    vec![(vec![("user", "test")], 4.0)],
                ),
                ("cortex_ingester_memory_series", vec![(vec![], 1.0)]),
                (
                    "cortex_ingester_memory_series_created_total",
                    vec![(vec![("user", "test")], 1.0)],
                ),
                (
                    "cortex_ingester_memory_series_removed_total",
                    vec![(vec![("user", "test")], 0.0)],
                ),
                ("cortex_ingester_memory_users", vec![(vec![], 1.0)]),
                ("cortex_ingester_tsdb_head_max_timestamp_seconds", vec![(vec![], 0.8)]),
                ("cortex_ingester_tsdb_head_min_timestamp_seconds", vec![(vec![], 0.5)]),
                (
                    "cortex_ingester_tsdb_out_of_order_samples_appended_total",
                    vec![(vec![("user", "test")], 0.0)],
                ),
            ],
            absent_metrics: vec![],
        },
        // 5. "should ignore created timestamp in the future"
        PushCase {
            name: "should ignore created timestamp in the future",
            max_exemplars: 0,
            max_metadata_per_user: 0,
            max_metadata_per_metric: 0,
            native_histograms: true,
            allow_ooo: true,
            reqs: vec![
                cortexpb::WriteRequest {
                    timeseries: vec![cortexpb::TimeSeries {
                        labels: metric_label_pairs(),
                        samples: vec![cortexpb::Sample {
                            value: 1.0,
                            timestamp_ms: 600,
                        }],
                        created_timestamp: 1500,
                        ..Default::default()
                    }],
                    source: cortexpb::write_request::SourceEnum::Api as i32,
                    ..Default::default()
                },
                cortexpb::WriteRequest {
                    timeseries: vec![cortexpb::TimeSeries {
                        labels: metric_label_pairs(),
                        histograms: vec![int_histogram_to_proto(800, &h1)],
                        created_timestamp: 1500,
                        ..Default::default()
                    }],
                    source: cortexpb::write_request::SourceEnum::Api as i32,
                    ..Default::default()
                },
            ],
            expected_ingested: vec![DecodedSeries {
                labels: metric_labels_map(),
                floats: vec![(600, 1.0)],
                histograms: vec![(800, fh1.clone())],
            }],
            expected_metadata_ingested: vec![],
            expected_metrics: vec![
                (
                    "cortex_ingester_active_native_histogram_buckets",
                    vec![(vec![("user", "test")], 8.0)],
                ),
                (
                    "cortex_ingester_active_native_histogram_series",
                    vec![(vec![("user", "test")], 1.0)],
                ),
                (
                    "cortex_ingester_active_series",
                    vec![(vec![("user", "test")], 1.0)],
                ),
                (
                    "cortex_ingester_ingested_samples_failures_total",
                    vec![(vec![("user", "test")], 0.0)],
                ),
                (
                    "cortex_ingester_ingested_samples_total",
                    vec![(vec![("user", "test")], 2.0)],
                ),
                ("cortex_ingester_memory_series", vec![(vec![], 1.0)]),
                (
                    "cortex_ingester_memory_series_created_total",
                    vec![(vec![("user", "test")], 1.0)],
                ),
                (
                    "cortex_ingester_memory_series_removed_total",
                    vec![(vec![("user", "test")], 0.0)],
                ),
                ("cortex_ingester_memory_users", vec![(vec![], 1.0)]),
                ("cortex_ingester_tsdb_head_max_timestamp_seconds", vec![(vec![], 0.8)]),
                ("cortex_ingester_tsdb_head_min_timestamp_seconds", vec![(vec![], 0.6)]),
                (
                    "cortex_ingester_tsdb_out_of_order_samples_appended_total",
                    vec![(vec![("user", "test")], 0.0)],
                ),
            ],
            absent_metrics: vec![],
        },
        // 6. "should ignore created timestamp equal to sample timestamp"
        PushCase {
            name: "should ignore created timestamp equal to sample timestamp",
            max_exemplars: 0,
            max_metadata_per_user: 0,
            max_metadata_per_metric: 0,
            native_histograms: true,
            allow_ooo: true,
            reqs: vec![
                cortexpb::WriteRequest {
                    timeseries: vec![cortexpb::TimeSeries {
                        labels: metric_label_pairs(),
                        samples: vec![cortexpb::Sample {
                            value: 1.0,
                            timestamp_ms: 600,
                        }],
                        created_timestamp: 600,
                        ..Default::default()
                    }],
                    source: cortexpb::write_request::SourceEnum::Api as i32,
                    ..Default::default()
                },
                cortexpb::WriteRequest {
                    timeseries: vec![cortexpb::TimeSeries {
                        labels: metric_label_pairs(),
                        histograms: vec![int_histogram_to_proto(800, &h1)],
                        created_timestamp: 800,
                        ..Default::default()
                    }],
                    source: cortexpb::write_request::SourceEnum::Api as i32,
                    ..Default::default()
                },
            ],
            expected_ingested: vec![DecodedSeries {
                labels: metric_labels_map(),
                floats: vec![(600, 1.0)],
                histograms: vec![(800, fh1.clone())],
            }],
            expected_metadata_ingested: vec![],
            expected_metrics: vec![
                (
                    "cortex_ingester_active_native_histogram_buckets",
                    vec![(vec![("user", "test")], 8.0)],
                ),
                (
                    "cortex_ingester_active_native_histogram_series",
                    vec![(vec![("user", "test")], 1.0)],
                ),
                (
                    "cortex_ingester_active_series",
                    vec![(vec![("user", "test")], 1.0)],
                ),
                (
                    "cortex_ingester_ingested_samples_failures_total",
                    vec![(vec![("user", "test")], 0.0)],
                ),
                (
                    "cortex_ingester_ingested_samples_total",
                    vec![(vec![("user", "test")], 2.0)],
                ),
                ("cortex_ingester_memory_series", vec![(vec![], 1.0)]),
                (
                    "cortex_ingester_memory_series_created_total",
                    vec![(vec![("user", "test")], 1.0)],
                ),
                (
                    "cortex_ingester_memory_series_removed_total",
                    vec![(vec![("user", "test")], 0.0)],
                ),
                ("cortex_ingester_memory_users", vec![(vec![], 1.0)]),
                ("cortex_ingester_tsdb_head_max_timestamp_seconds", vec![(vec![], 0.8)]),
                ("cortex_ingester_tsdb_head_min_timestamp_seconds", vec![(vec![], 0.6)]),
                (
                    "cortex_ingester_tsdb_out_of_order_samples_appended_total",
                    vec![(vec![("user", "test")], 0.0)],
                ),
            ],
            absent_metrics: vec![],
        },
        // 7. "should ingest created timestamp with correct type among other samples and not fail duplicates"
        PushCase {
            name: "should ingest created timestamp with correct type among other samples and not fail duplicates",
            max_exemplars: 0,
            max_metadata_per_user: 0,
            max_metadata_per_metric: 0,
            native_histograms: true,
            allow_ooo: true,
            reqs: vec![
                cortexpb::WriteRequest {
                    timeseries: vec![cortexpb::TimeSeries {
                        labels: metric_label_pairs(),
                        histograms: vec![
                            int_histogram_to_proto(400, &h1),
                            int_histogram_to_proto(600, &h2),
                            int_histogram_to_proto(700, &h3),
                        ],
                        created_timestamp: 500,
                        ..Default::default()
                    }],
                    source: cortexpb::write_request::SourceEnum::Api as i32,
                    ..Default::default()
                },
                cortexpb::WriteRequest {
                    timeseries: vec![cortexpb::TimeSeries {
                        labels: metric_label_pairs(),
                        histograms: vec![int_histogram_to_proto(800, &h4)],
                        created_timestamp: 500,
                        ..Default::default()
                    }],
                    source: cortexpb::write_request::SourceEnum::Api as i32,
                    ..Default::default()
                },
                cortexpb::WriteRequest {
                    timeseries: vec![cortexpb::TimeSeries {
                        labels: metric_label_pairs(),
                        samples: vec![
                            cortexpb::Sample { value: 1.0, timestamp_ms: 1400 },
                            cortexpb::Sample { value: 2.0, timestamp_ms: 1600 },
                            cortexpb::Sample { value: 3.0, timestamp_ms: 1700 },
                        ],
                        created_timestamp: 1500,
                        ..Default::default()
                    }],
                    source: cortexpb::write_request::SourceEnum::Api as i32,
                    ..Default::default()
                },
                cortexpb::WriteRequest {
                    timeseries: vec![cortexpb::TimeSeries {
                        labels: metric_label_pairs(),
                        samples: vec![
                            cortexpb::Sample { value: 4.0, timestamp_ms: 1800 },
                        ],
                        created_timestamp: 1500,
                        ..Default::default()
                    }],
                    source: cortexpb::write_request::SourceEnum::Api as i32,
                    ..Default::default()
                },
            ],
            expected_ingested: vec![DecodedSeries {
                labels: metric_labels_map(),
                floats: vec![
                    (1400, 1.0),
                    (1500, 0.0),
                    (1600, 2.0),
                    (1700, 3.0),
                    (1800, 4.0),
                ],
                histograms: vec![
                    (400, fh1.clone()),
                    (500, zero_fh.clone()),
                    (600, fh2.clone()),
                    (700, fh3.clone()),
                    (800, fh4.clone()),
                ],
            }],
            expected_metadata_ingested: vec![],
            expected_metrics: vec![
                (
                    "cortex_ingester_active_series",
                    vec![(vec![("user", "test")], 1.0)],
                ),
                (
                    "cortex_ingester_ingested_samples_failures_total",
                    vec![(vec![("user", "test")], 0.0)],
                ),
                (
                    "cortex_ingester_ingested_samples_total",
                    vec![(vec![("user", "test")], 10.0)],
                ),
                ("cortex_ingester_memory_series", vec![(vec![], 1.0)]),
                (
                    "cortex_ingester_memory_series_created_total",
                    vec![(vec![("user", "test")], 1.0)],
                ),
                (
                    "cortex_ingester_memory_series_removed_total",
                    vec![(vec![("user", "test")], 0.0)],
                ),
                ("cortex_ingester_memory_users", vec![(vec![], 1.0)]),
                ("cortex_ingester_tsdb_head_max_timestamp_seconds", vec![(vec![], 1.8)]),
                ("cortex_ingester_tsdb_head_min_timestamp_seconds", vec![(vec![], 0.4)]),
                (
                    "cortex_ingester_tsdb_out_of_order_samples_appended_total",
                    vec![(vec![("user", "test")], 0.0)],
                ),
            ],
            absent_metrics: vec![],
        },
        // 8. "should ingest created timestamp with correct type among other samples and not fail duplicates if out-of-order is disabled"
        PushCase {
            name: "should ingest created timestamp with correct type among other samples and not fail duplicates if out-of-order is disabled",
            max_exemplars: 0,
            max_metadata_per_user: 0,
            max_metadata_per_metric: 0,
            native_histograms: true,
            allow_ooo: false,
            reqs: vec![
                cortexpb::WriteRequest {
                    timeseries: vec![cortexpb::TimeSeries {
                        labels: metric_label_pairs(),
                        histograms: vec![
                            int_histogram_to_proto(400, &h1),
                            int_histogram_to_proto(600, &h2),
                            int_histogram_to_proto(700, &h3),
                        ],
                        created_timestamp: 500,
                        ..Default::default()
                    }],
                    source: cortexpb::write_request::SourceEnum::Api as i32,
                    ..Default::default()
                },
                cortexpb::WriteRequest {
                    timeseries: vec![cortexpb::TimeSeries {
                        labels: metric_label_pairs(),
                        histograms: vec![int_histogram_to_proto(800, &h4)],
                        created_timestamp: 500,
                        ..Default::default()
                    }],
                    source: cortexpb::write_request::SourceEnum::Api as i32,
                    ..Default::default()
                },
                cortexpb::WriteRequest {
                    timeseries: vec![cortexpb::TimeSeries {
                        labels: metric_label_pairs(),
                        samples: vec![
                            cortexpb::Sample { value: 1.0, timestamp_ms: 1400 },
                            cortexpb::Sample { value: 2.0, timestamp_ms: 1600 },
                            cortexpb::Sample { value: 3.0, timestamp_ms: 1700 },
                        ],
                        created_timestamp: 1500,
                        ..Default::default()
                    }],
                    source: cortexpb::write_request::SourceEnum::Api as i32,
                    ..Default::default()
                },
                cortexpb::WriteRequest {
                    timeseries: vec![cortexpb::TimeSeries {
                        labels: metric_label_pairs(),
                        samples: vec![
                            cortexpb::Sample { value: 4.0, timestamp_ms: 1800 },
                        ],
                        created_timestamp: 1500,
                        ..Default::default()
                    }],
                    source: cortexpb::write_request::SourceEnum::Api as i32,
                    ..Default::default()
                },
            ],
            expected_ingested: vec![DecodedSeries {
                labels: metric_labels_map(),
                floats: vec![
                    (1400, 1.0),
                    (1500, 0.0),
                    (1600, 2.0),
                    (1700, 3.0),
                    (1800, 4.0),
                ],
                histograms: vec![
                    (400, fh1.clone()),
                    (500, zero_fh.clone()),
                    (600, fh2.clone()),
                    (700, fh3.clone()),
                    (800, fh4.clone()),
                ],
            }],
            expected_metadata_ingested: vec![],
            expected_metrics: vec![
                (
                    "cortex_ingester_active_series",
                    vec![(vec![("user", "test")], 1.0)],
                ),
                (
                    "cortex_ingester_ingested_samples_failures_total",
                    vec![(vec![("user", "test")], 0.0)],
                ),
                (
                    "cortex_ingester_ingested_samples_total",
                    vec![(vec![("user", "test")], 10.0)],
                ),
                ("cortex_ingester_memory_series", vec![(vec![], 1.0)]),
                (
                    "cortex_ingester_memory_series_created_total",
                    vec![(vec![("user", "test")], 1.0)],
                ),
                (
                    "cortex_ingester_memory_series_removed_total",
                    vec![(vec![("user", "test")], 0.0)],
                ),
                ("cortex_ingester_memory_users", vec![(vec![], 1.0)]),
                ("cortex_ingester_tsdb_head_max_timestamp_seconds", vec![(vec![], 1.8)]),
                ("cortex_ingester_tsdb_head_min_timestamp_seconds", vec![(vec![], 0.4)]),
                (
                    "cortex_ingester_tsdb_out_of_order_samples_appended_total",
                    vec![(vec![("user", "test")], 0.0)],
                ),
            ],
            absent_metrics: vec![],
        },
        // 9. "should ignore created timestamp if no samples"
        PushCase {
            name: "should ignore created timestamp if no samples",
            max_exemplars: 0,
            max_metadata_per_user: 0,
            max_metadata_per_metric: 0,
            native_histograms: true,
            allow_ooo: true,
            reqs: vec![
                cortexpb::WriteRequest {
                    timeseries: vec![cortexpb::TimeSeries {
                        labels: metric_label_pairs(),
                        samples: vec![cortexpb::Sample {
                            value: 1.0,
                            timestamp_ms: 400,
                        }],
                        ..Default::default()
                    }],
                    source: cortexpb::write_request::SourceEnum::Api as i32,
                    ..Default::default()
                },
                cortexpb::WriteRequest {
                    timeseries: vec![cortexpb::TimeSeries {
                        labels: metric_label_pairs(),
                        created_timestamp: 500,
                        ..Default::default()
                    }],
                    source: cortexpb::write_request::SourceEnum::Api as i32,
                    ..Default::default()
                },
            ],
            expected_ingested: vec![DecodedSeries {
                labels: metric_labels_map(),
                floats: vec![(400, 1.0)],
                histograms: vec![],
            }],
            expected_metadata_ingested: vec![],
            expected_metrics: vec![
                (
                    "cortex_ingester_active_series",
                    vec![(vec![("user", "test")], 1.0)],
                ),
                (
                    "cortex_ingester_ingested_samples_failures_total",
                    vec![(vec![("user", "test")], 0.0)],
                ),
                (
                    "cortex_ingester_ingested_samples_total",
                    vec![(vec![("user", "test")], 1.0)],
                ),
                ("cortex_ingester_memory_series", vec![(vec![], 1.0)]),
                (
                    "cortex_ingester_memory_series_created_total",
                    vec![(vec![("user", "test")], 1.0)],
                ),
                (
                    "cortex_ingester_memory_series_removed_total",
                    vec![(vec![("user", "test")], 0.0)],
                ),
                ("cortex_ingester_memory_users", vec![(vec![], 1.0)]),
                ("cortex_ingester_tsdb_head_max_timestamp_seconds", vec![(vec![], 0.4)]),
                ("cortex_ingester_tsdb_head_min_timestamp_seconds", vec![(vec![], 0.4)]),
                (
                    "cortex_ingester_tsdb_out_of_order_samples_appended_total",
                    vec![(vec![("user", "test")], 0.0)],
                ),
            ],
            absent_metrics: vec![],
        },
        // 10. "should succeed on created timestamp being duplicate sample"
        PushCase {
            name: "should succeed on created timestamp being duplicate sample",
            max_exemplars: 0,
            max_metadata_per_user: 0,
            max_metadata_per_metric: 0,
            native_histograms: true,
            allow_ooo: true,
            reqs: vec![
                cortexpb::WriteRequest {
                    timeseries: vec![cortexpb::TimeSeries {
                        labels: metric_label_pairs(),
                        samples: vec![cortexpb::Sample {
                            value: 1.0,
                            timestamp_ms: 400,
                        }],
                        ..Default::default()
                    }],
                    source: cortexpb::write_request::SourceEnum::Api as i32,
                    ..Default::default()
                },
                cortexpb::WriteRequest {
                    timeseries: vec![cortexpb::TimeSeries {
                        labels: metric_label_pairs(),
                        samples: vec![cortexpb::Sample {
                            value: 2.0,
                            timestamp_ms: 500,
                        }],
                        created_timestamp: 400,
                        ..Default::default()
                    }],
                    source: cortexpb::write_request::SourceEnum::Api as i32,
                    ..Default::default()
                },
            ],
            expected_ingested: vec![DecodedSeries {
                labels: metric_labels_map(),
                floats: vec![(400, 1.0), (500, 2.0)],
                histograms: vec![],
            }],
            expected_metadata_ingested: vec![],
            expected_metrics: vec![
                (
                    "cortex_ingester_active_series",
                    vec![(vec![("user", "test")], 1.0)],
                ),
                (
                    "cortex_ingester_ingested_samples_failures_total",
                    vec![(vec![("user", "test")], 0.0)],
                ),
                (
                    "cortex_ingester_ingested_samples_total",
                    vec![(vec![("user", "test")], 2.0)],
                ),
                ("cortex_ingester_memory_series", vec![(vec![], 1.0)]),
                (
                    "cortex_ingester_memory_series_created_total",
                    vec![(vec![("user", "test")], 1.0)],
                ),
                (
                    "cortex_ingester_memory_series_removed_total",
                    vec![(vec![("user", "test")], 0.0)],
                ),
                ("cortex_ingester_memory_users", vec![(vec![], 1.0)]),
                ("cortex_ingester_tsdb_head_max_timestamp_seconds", vec![(vec![], 0.5)]),
                ("cortex_ingester_tsdb_head_min_timestamp_seconds", vec![(vec![], 0.4)]),
                (
                    "cortex_ingester_tsdb_out_of_order_samples_appended_total",
                    vec![(vec![("user", "test")], 0.0)],
                ),
            ],
            absent_metrics: vec![],
        },
        // 11. "should succeed with histograms on created timestamp causing an error"
        PushCase {
            name: "should succeed with histograms on created timestamp causing an error",
            max_exemplars: 0,
            max_metadata_per_user: 0,
            max_metadata_per_metric: 0,
            native_histograms: true,
            allow_ooo: true,
            reqs: vec![
                cortexpb::WriteRequest {
                    timeseries: vec![cortexpb::TimeSeries {
                        labels: metric_label_pairs(),
                        histograms: vec![int_histogram_to_proto(400, &h1)],
                        ..Default::default()
                    }],
                    source: cortexpb::write_request::SourceEnum::Api as i32,
                    ..Default::default()
                },
                cortexpb::WriteRequest {
                    timeseries: vec![cortexpb::TimeSeries {
                        labels: metric_label_pairs(),
                        histograms: vec![int_histogram_to_proto(500, &h2)],
                        created_timestamp: 400,
                        ..Default::default()
                    }],
                    source: cortexpb::write_request::SourceEnum::Api as i32,
                    ..Default::default()
                },
            ],
            expected_ingested: vec![DecodedSeries {
                labels: metric_labels_map(),
                floats: vec![],
                histograms: vec![(400, fh1.clone()), (500, fh2.clone())],
            }],
            expected_metadata_ingested: vec![],
            expected_metrics: vec![
                (
                    "cortex_ingester_active_native_histogram_buckets",
                    vec![(vec![("user", "test")], 8.0)],
                ),
                (
                    "cortex_ingester_active_native_histogram_series",
                    vec![(vec![("user", "test")], 1.0)],
                ),
                (
                    "cortex_ingester_active_series",
                    vec![(vec![("user", "test")], 1.0)],
                ),
                (
                    "cortex_ingester_ingested_samples_failures_total",
                    vec![(vec![("user", "test")], 0.0)],
                ),
                (
                    "cortex_ingester_ingested_samples_total",
                    vec![(vec![("user", "test")], 2.0)],
                ),
                ("cortex_ingester_memory_series", vec![(vec![], 1.0)]),
                (
                    "cortex_ingester_memory_series_created_total",
                    vec![(vec![("user", "test")], 1.0)],
                ),
                (
                    "cortex_ingester_memory_series_removed_total",
                    vec![(vec![("user", "test")], 0.0)],
                ),
                ("cortex_ingester_memory_users", vec![(vec![], 1.0)]),
                ("cortex_ingester_tsdb_head_max_timestamp_seconds", vec![(vec![], 0.5)]),
                ("cortex_ingester_tsdb_head_min_timestamp_seconds", vec![(vec![], 0.4)]),
                (
                    "cortex_ingester_tsdb_out_of_order_samples_appended_total",
                    vec![(vec![("user", "test")], 0.0)],
                ),
            ],
            absent_metrics: vec![],
        },
    ]
}

async fn run_push_case(case: &PushCase, wire_mode: WireMode) {
    let user_id = "test";
    let tmp = tempfile::tempdir().unwrap();
    let reg = Registry::new();
    let (cfg, blocks) = default_ingester_test_config(tmp.path());

    let mut limits = default_limits_test_config();
    limits.max_global_exemplars_per_user = case.max_exemplars;
    limits.max_global_metadata_per_user = case.max_metadata_per_user;
    limits.max_global_metadata_per_metric = case.max_metadata_per_metric;
    limits.native_histograms_ingestion_enabled = case.native_histograms;
    let ooo_window_secs = if case.allow_ooo {
        limits.out_of_order_time_window = ModelDuration::from_secs(3600);
        3600i64
    } else {
        0i64
    };

    let ing =
        prepare_ingester_with_blocks_storage(cfg, blocks, Overrides::new(limits), Some(&reg)).await;

    for req in &case.reqs {
        match wire_mode {
            WireMode::Rw1 => {
                ing.push_grpc(user_id, req)
                    .await
                    .unwrap_or_else(|e| panic!("{}: RW1 push failed: {e}", case.name));
            }
            WireMode::Rw2Prealloc => {
                let bytes = encode_rw2_bytes(req);
                let mut wr = PreallocWriteRequest {
                    unmarshal_from_rw2: true,
                    skip_normalize_metadata_metric_name: true,
                    skip_deduplicate_metadata: true,
                    ..Default::default()
                };
                wr.unmarshal(&bytes).expect("rw2 unmarshal");
                ing.push_grpc(user_id, &wr.write_request)
                    .await
                    .unwrap_or_else(|e| panic!("{}: RW2 prealloc push failed: {e}", case.name));
            }
            WireMode::Rw2Borrowed => {
                let bytes = encode_rw2_bytes(req);
                let opts = DecodeOptions {
                    mode: DecodeMode::Rw2,
                    skip_unmarshaling_exemplars: false,
                    skip_normalize_metadata_metric_name: true,
                    skip_deduplicate_metadata: true,
                };
                let mut scratch = WriteRequestScratch::new();
                let borrowed = decode_write_request(&bytes, opts, &mut scratch).expect("rw2 decode");
                ing.push_borrowed_to_storage(user_id, &borrowed, bytes.len() as i64)
                    .unwrap_or_else(|e| panic!("{}: RW2 borrowed push failed: {e}", case.name));
            }
        }
    }

    let got_series = query_all_series(&ing, user_id).await;
    assert_eq!(
        got_series, case.expected_ingested,
        "case '{}' ({wire_mode:?}) ingested mismatch",
        case.name
    );

    let ex_res = ing
        .handle_query_exemplars(
            &MetadataMap::new(),
            user_id,
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
    assert!(ex_res.timeseries.is_empty(), "case '{}' ({wire_mode:?})", case.name);

    let meta_res = ing
        .handle_metrics_metadata(
            &MetadataMap::new(),
            user_id,
            ingesterpb::MetricsMetadataRequest {
                limit: -1,
                limit_per_metric: -1,
                ..Default::default()
            },
        )
        .await
        .expect("metrics_metadata");
    let mut got_meta = meta_res.metadata;
    let mut exp_meta = case.expected_metadata_ingested.clone();
    got_meta.sort_by_key(|a| (a.metric_family_name.clone(), a.help.clone()));
    exp_meta.sort_by_key(|a| (a.metric_family_name.clone(), a.help.clone()));
    assert_eq!(got_meta, exp_meta, "case '{}' ({wire_mode:?}) metadata mismatch", case.name);

    ing.update_active_series_metrics(SystemTime::now());
    let families = reg.gather();

    let is_metadata_only = case.expected_ingested.is_empty();
    for (metric_name, expected_entries) in &case.expected_metrics {
        // In RW2, a metadata-only TimeSeriesRW2 carries a TimeSeries entry with empty samples,
        // so TSDB is created when pushed via PreallocWriteRequest/BorrowedWriteRequest.
        if wire_mode != WireMode::Rw1 && is_metadata_only && *metric_name == "cortex_ingester_memory_users" {
            continue;
        }
        let actual = metric_value_map(&families, metric_name);
        let expected: BTreeMap<Vec<(String, String)>, f64> = expected_entries
            .iter()
            .map(|(lbls, val)| {
                let mut v: Vec<(String, String)> = lbls
                    .iter()
                    .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                    .collect();
                v.sort();
                (v, *val)
            })
            .collect();
        assert_eq!(
            actual, expected,
            "case '{}' ({wire_mode:?}) metric '{metric_name}' mismatch",
            case.name
        );
    }
    for abs_name in &case.absent_metrics {
        assert!(
            metric_value_map(&families, abs_name).is_empty(),
            "case '{}' ({wire_mode:?}) expected metric '{abs_name}' to be absent",
            case.name
        );
    }

    let stats = ing.update_usage_stats();
    assert_eq!(
        stats.active_series,
        case.expected_ingested.len() as i64,
        "case '{}' ({wire_mode:?}) active_series",
        case.name
    );
    assert_eq!(
        stats.memory_series,
        case.expected_ingested.len() as i64,
        "case '{}' ({wire_mode:?}) memory_series",
        case.name
    );
    let expected_tenants = if case.expected_ingested.is_empty() { 0 } else { 1 };
    assert_eq!(
        stats.memory_users, expected_tenants,
        "case '{}' ({wire_mode:?}) memory_users",
        case.name
    );
    assert_eq!(
        stats.tenants_with_out_of_order_enabled == 1,
        case.allow_ooo,
        "case '{}' ({wire_mode:?}) tenants_with_out_of_order_enabled",
        case.name
    );
    assert_eq!(
        stats.min_out_of_order_time_window_seconds, ooo_window_secs,
        "case '{}' ({wire_mode:?}) min_out_of_order_time_window_seconds",
        case.name
    );
    assert_eq!(
        stats.max_out_of_order_time_window_seconds, ooo_window_secs,
        "case '{}' ({wire_mode:?}) max_out_of_order_time_window_seconds",
        case.name
    );

    ing.stop().await.unwrap();
}

// Upstream: pkg/ingester/ingester_test.go::TestIngester_Push
#[test]
fn test_ingester_push_final_10_cases_rw1_and_rw2() {
    block_on_for_tests(async {
        let cases = build_final_10_push_cases();
        assert_eq!(cases.len(), 11);
        for case in &cases {
            for mode in [WireMode::Rw1, WireMode::Rw2Prealloc, WireMode::Rw2Borrowed] {
                run_push_case(case, mode).await;
            }
        }
    });
}

// Upstream: pkg/ingester/ingester_test.go::TestIngester_Push_ShouldCorrectlyTrackMetricsInMultiTenantScenario
#[test]
fn test_ingester_push_should_correctly_track_metrics_in_multi_tenant_scenario() {
    block_on_for_tests(async {
        let tmp = tempfile::tempdir().unwrap();
        let reg = Registry::new();
        let (cfg, blocks) = default_ingester_test_config(tmp.path());
        let ing = prepare_ingester_with_blocks_storage(
            cfg,
            blocks,
            Overrides::new(default_limits_test_config()),
            Some(&reg),
        )
        .await;

        let lbls_float = vec![cortexpb::LabelPair::from_strs(METRIC_NAME_LABEL, "test")];
        let lbls_hist = vec![cortexpb::LabelPair::from_strs(
            METRIC_NAME_LABEL,
            "test_histogram",
        )];

        for (ix, user_id) in ["test-1", "test-2"].into_iter().enumerate() {
            let reqs = vec![
                cortexpb::WriteRequest {
                    timeseries: vec![cortexpb::TimeSeries {
                        labels: lbls_float.clone(),
                        samples: vec![cortexpb::Sample {
                            value: 1.0,
                            timestamp_ms: (ix as i64) * 1000 + 9,
                        }],
                        ..Default::default()
                    }],
                    source: cortexpb::write_request::SourceEnum::Api as i32,
                    ..Default::default()
                },
                cortexpb::WriteRequest {
                    timeseries: vec![cortexpb::TimeSeries {
                        labels: lbls_float.clone(),
                        samples: vec![cortexpb::Sample {
                            value: 2.0,
                            timestamp_ms: (ix as i64) * 1000 + 10,
                        }],
                        ..Default::default()
                    }],
                    source: cortexpb::write_request::SourceEnum::Api as i32,
                    ..Default::default()
                },
                cortexpb::WriteRequest {
                    timeseries: vec![cortexpb::TimeSeries {
                        labels: lbls_hist.clone(),
                        histograms: vec![int_histogram_to_proto(7, &generate_test_histogram(1))],
                        ..Default::default()
                    }],
                    source: cortexpb::write_request::SourceEnum::Api as i32,
                    ..Default::default()
                },
                cortexpb::WriteRequest {
                    timeseries: vec![cortexpb::TimeSeries {
                        labels: lbls_hist.clone(),
                        histograms: vec![int_histogram_to_proto(
                            8,
                            &generate_test_gauge_histogram(2),
                        )],
                        ..Default::default()
                    }],
                    source: cortexpb::write_request::SourceEnum::Api as i32,
                    ..Default::default()
                },
            ];

            for req in reqs {
                ing.push_grpc(user_id, &req).await.unwrap();
            }
        }

        ing.update_active_series_metrics(SystemTime::now());
        let families = reg.gather();

        let expected_per_user = |v1: f64, v2: f64| -> BTreeMap<Vec<(String, String)>, f64> {
            BTreeMap::from([
                (vec![("user".into(), "test-1".into())], v1),
                (vec![("user".into(), "test-2".into())], v2),
            ])
        };

        assert_eq!(
            metric_value_map(&families, "cortex_ingester_ingested_samples_total"),
            expected_per_user(4.0, 4.0)
        );
        assert_eq!(
            metric_value_map(&families, "cortex_ingester_ingested_samples_failures_total"),
            expected_per_user(0.0, 0.0)
        );
        assert_eq!(
            single_metric_value(&families, "cortex_ingester_memory_users"),
            Some(2.0)
        );
        assert_eq!(
            single_metric_value(&families, "cortex_ingester_memory_series"),
            Some(4.0)
        );
        assert_eq!(
            metric_value_map(&families, "cortex_ingester_memory_series_created_total"),
            expected_per_user(2.0, 2.0)
        );
        assert_eq!(
            metric_value_map(&families, "cortex_ingester_memory_series_removed_total"),
            expected_per_user(0.0, 0.0)
        );
        assert_eq!(
            metric_value_map(&families, "cortex_ingester_active_series"),
            expected_per_user(2.0, 2.0)
        );
        assert_eq!(
            metric_value_map(&families, "cortex_ingester_active_native_histogram_series"),
            expected_per_user(1.0, 1.0)
        );
        assert_eq!(
            metric_value_map(&families, "cortex_ingester_active_native_histogram_buckets"),
            expected_per_user(8.0, 8.0)
        );
        assert_eq!(
            single_metric_value(&families, "cortex_ingester_tsdb_head_min_timestamp_seconds"),
            Some(0.007)
        );
        assert_eq!(
            single_metric_value(&families, "cortex_ingester_tsdb_head_max_timestamp_seconds"),
            Some(1.01)
        );

        ing.stop().await.unwrap();
    });
}

// Upstream: pkg/ingester/ingester_test.go::TestIngester_Push_DecreaseInactiveSeries
#[test]
fn test_ingester_push_decrease_inactive_series() {
    block_on_for_tests(async {
        let tmp = tempfile::tempdir().unwrap();
        let reg = Registry::new();
        let (mut cfg, blocks) = default_ingester_test_config(tmp.path());
        cfg.active_series.active_series_metrics_idle_timeout = GoDuration::from_millis(100);

        let ing = prepare_ingester_with_blocks_storage(
            cfg.clone(),
            blocks,
            Overrides::new(default_limits_test_config()),
            Some(&reg),
        )
        .await;

        let current_time = SystemTime::now();
        let lbls_float = vec![cortexpb::LabelPair::from_strs(METRIC_NAME_LABEL, "test")];
        let lbls_hist = vec![cortexpb::LabelPair::from_strs(
            METRIC_NAME_LABEL,
            "test_histogram",
        )];

        for user_id in ["test-1", "test-2"] {
            let reqs = vec![
                cortexpb::WriteRequest {
                    timeseries: vec![cortexpb::TimeSeries {
                        labels: lbls_float.clone(),
                        samples: vec![cortexpb::Sample {
                            value: 1.0,
                            timestamp_ms: 9,
                        }],
                        ..Default::default()
                    }],
                    source: cortexpb::write_request::SourceEnum::Api as i32,
                    ..Default::default()
                },
                cortexpb::WriteRequest {
                    timeseries: vec![cortexpb::TimeSeries {
                        labels: lbls_float.clone(),
                        samples: vec![cortexpb::Sample {
                            value: 2.0,
                            timestamp_ms: 10,
                        }],
                        ..Default::default()
                    }],
                    source: cortexpb::write_request::SourceEnum::Api as i32,
                    ..Default::default()
                },
                cortexpb::WriteRequest {
                    timeseries: vec![cortexpb::TimeSeries {
                        labels: lbls_hist.clone(),
                        histograms: vec![int_histogram_to_proto(7, &generate_test_histogram(1))],
                        ..Default::default()
                    }],
                    source: cortexpb::write_request::SourceEnum::Api as i32,
                    ..Default::default()
                },
                cortexpb::WriteRequest {
                    timeseries: vec![cortexpb::TimeSeries {
                        labels: lbls_hist.clone(),
                        histograms: vec![int_histogram_to_proto(
                            8,
                            &generate_test_gauge_histogram(2),
                        )],
                        ..Default::default()
                    }],
                    source: cortexpb::write_request::SourceEnum::Api as i32,
                    ..Default::default()
                },
            ];
            for req in reqs {
                ing.push_grpc(user_id, &req).await.unwrap();
            }
        }

        // Advance past idle timeout + 1s so active series are evicted.
        let future_time = current_time
            + Duration::from_nanos(
                cfg.active_series
                    .active_series_metrics_idle_timeout
                    .as_nanos()
                    .max(0) as u64,
            )
            + Duration::from_secs(1);
        ing.update_active_series_metrics(future_time);

        let families = reg.gather();
        assert_eq!(
            metric_value_map(&families, "cortex_ingester_memory_series_created_total"),
            BTreeMap::from([
                (vec![("user".into(), "test-1".into())], 2.0),
                (vec![("user".into(), "test-2".into())], 2.0),
            ])
        );
        assert_eq!(
            metric_value_map(&families, "cortex_ingester_memory_series_removed_total"),
            BTreeMap::from([
                (vec![("user".into(), "test-1".into())], 0.0),
                (vec![("user".into(), "test-2".into())], 0.0),
            ])
        );
        assert!(metric_value_map(&families, "cortex_ingester_active_series").is_empty());
        assert!(
            metric_value_map(&families, "cortex_ingester_active_native_histogram_series").is_empty()
        );
        assert!(
            metric_value_map(&families, "cortex_ingester_active_native_histogram_buckets")
                .is_empty()
        );

        ing.stop().await.unwrap();
    });
}

// Ported scenarios from BenchmarkIngesterPush / BenchmarkIngesterPushExemplars / Benchmark_Ingester_PushOnError
// (lines 4927..5375): WAL replay concurrency & concurrent error/limit scenarios.
#[test]
fn test_ingester_push_on_error_and_replay_concurrency_scenarios() {
    #[derive(Clone, Copy)]
    enum ErrorScenario {
        OutOfBounds,
        OutOfOrder,
        PerUserSeriesLimit,
        PerMetricSeriesLimit,
        IngestionRateLimit,
        MaxTenantsLimit,
        MaxSeriesLimit,
        MaxInflightRequestsLimit,
    }

    let replay_scenarios = [
        ("no_wal_replay_concurrency", 0i64, 50usize, 1usize),
        ("low_wal_replay_concurrency", 100i64, 50usize, 4usize),
        ("high_wal_replay_concurrency", 1000i64, 50usize, 8usize),
        ("very_high_wal_replay_concurrency", 2500i64, 20usize, 16usize),
    ];

    let error_cases = [
        (
            "out_of_bound_samples",
            ErrorScenario::OutOfBounds,
            false,
            "err-mimir-sample-timestamp-too-old",
        ),
        (
            "out_of_order_samples",
            ErrorScenario::OutOfOrder,
            false,
            "err-mimir-sample-out-of-order",
        ),
        (
            "per_user_series_limit",
            ErrorScenario::PerUserSeriesLimit,
            false,
            "per-user series limit",
        ),
        (
            "per_metric_series_limit",
            ErrorScenario::PerMetricSeriesLimit,
            false,
            "per-metric series limit",
        ),
        (
            "very_low_ingestion_rate_limit",
            ErrorScenario::IngestionRateLimit,
            true,
            "err-mimir-ingester-max-ingestion-rate",
        ),
        (
            "max_number_of_tenants_reached",
            ErrorScenario::MaxTenantsLimit,
            true,
            "err-mimir-ingester-max-tenants",
        ),
        (
            "max_number_of_series_reached",
            ErrorScenario::MaxSeriesLimit,
            true,
            "err-mimir-ingester-max-series",
        ),
        (
            "max_inflight_requests_reached",
            ErrorScenario::MaxInflightRequestsLimit,
            true,
            "err-mimir-ingester-max-inflight-push-requests",
        ),
    ];

    let instance_limit_variants = [false, true];
    let user_id = "test";
    let sample_ts = 100i64;
    let metric_name = "test";

    block_on_for_tests(async {
        for (scenario_name, wal_replay_conc, num_series_per_req, num_clients) in replay_scenarios {
            for (case_name, kind, requires_instance_limits, expected_substr) in error_cases {
                for use_instance_limits in instance_limit_variants {
                    if requires_instance_limits && !use_instance_limits {
                        continue;
                    }

                    let tmp = tempfile::tempdir().unwrap();
                    let reg = Registry::new();
                    let (mut cfg, mut blocks) = default_ingester_test_config(tmp.path());
                    blocks.tsdb.wal_replay_concurrency = wal_replay_conc;

                    let mut il = InstanceLimits {
                        max_ingestion_rate: 1e12,
                        max_tenants: 1,
                        max_series: 500,
                        max_inflight_push_requests: 2500,
                        ..Default::default()
                    };
                    let mut limits = default_limits_test_config();
                    match kind {
                        ErrorScenario::OutOfBounds | ErrorScenario::OutOfOrder => {}
                        ErrorScenario::PerUserSeriesLimit => {
                            limits.max_global_series_per_user = 1;
                        }
                        ErrorScenario::PerMetricSeriesLimit => {
                            limits.max_global_series_per_metric = 1;
                        }
                        ErrorScenario::IngestionRateLimit => {
                            il.max_ingestion_rate = 0.00001;
                        }
                        ErrorScenario::MaxTenantsLimit => {
                            il.max_tenants = 1;
                        }
                        ErrorScenario::MaxSeriesLimit => {
                            il.max_series = 1;
                        }
                        ErrorScenario::MaxInflightRequestsLimit => {
                            il.max_inflight_push_requests = 1;
                        }
                    }

                    if use_instance_limits {
                        cfg.instance_limits = il;
                    } else {
                        cfg.instance_limits = InstanceLimits::default();
                    }

                    let ing = prepare_ingester_with_blocks_storage(
                        cfg,
                        blocks,
                        Overrides::new(limits),
                        Some(&reg),
                    )
                    .await;

                    // Setup before concurrent error pushes.
                    match kind {
                        ErrorScenario::OutOfBounds => {
                            let req = cortexpb::WriteRequest {
                                timeseries: vec![cortexpb::TimeSeries {
                                    labels: vec![cortexpb::LabelPair::from_strs(
                                        METRIC_NAME_LABEL,
                                        metric_name,
                                    )],
                                    samples: vec![cortexpb::Sample {
                                        value: 1.0,
                                        timestamp_ms: now_ms(),
                                    }],
                                    ..Default::default()
                                }],
                                source: cortexpb::write_request::SourceEnum::Api as i32,
                                ..Default::default()
                            };
                            ing.push_grpc(user_id, &req).await.unwrap();
                        }
                        ErrorScenario::OutOfOrder => {
                            for i in 0..num_series_per_req {
                                let req = cortexpb::WriteRequest {
                                    timeseries: vec![cortexpb::TimeSeries {
                                        labels: vec![
                                            cortexpb::LabelPair::from_strs(
                                                METRIC_NAME_LABEL,
                                                metric_name,
                                            ),
                                            cortexpb::LabelPair::from_strs(
                                                "cardinality",
                                                i.to_string(),
                                            ),
                                        ],
                                        samples: vec![cortexpb::Sample {
                                            value: 1.0,
                                            timestamp_ms: sample_ts + 1,
                                        }],
                                        ..Default::default()
                                    }],
                                    source: cortexpb::write_request::SourceEnum::Api as i32,
                                    ..Default::default()
                                };
                                ing.push_grpc(user_id, &req).await.unwrap();
                            }
                        }
                        ErrorScenario::PerUserSeriesLimit => {
                            let req = cortexpb::WriteRequest {
                                timeseries: vec![cortexpb::TimeSeries {
                                    labels: vec![cortexpb::LabelPair::from_strs(
                                        METRIC_NAME_LABEL,
                                        "another",
                                    )],
                                    samples: vec![cortexpb::Sample {
                                        value: 1.0,
                                        timestamp_ms: sample_ts + 1,
                                    }],
                                    ..Default::default()
                                }],
                                source: cortexpb::write_request::SourceEnum::Api as i32,
                                ..Default::default()
                            };
                            ing.push_grpc(user_id, &req).await.unwrap();
                        }
                        ErrorScenario::PerMetricSeriesLimit => {
                            let req = cortexpb::WriteRequest {
                                timeseries: vec![cortexpb::TimeSeries {
                                    labels: vec![
                                        cortexpb::LabelPair::from_strs(
                                            METRIC_NAME_LABEL,
                                            metric_name,
                                        ),
                                        cortexpb::LabelPair::from_strs("cardinality", "another"),
                                    ],
                                    samples: vec![cortexpb::Sample {
                                        value: 1.0,
                                        timestamp_ms: sample_ts + 1,
                                    }],
                                    ..Default::default()
                                }],
                                source: cortexpb::write_request::SourceEnum::Api as i32,
                                ..Default::default()
                            };
                            ing.push_grpc(user_id, &req).await.unwrap();
                        }
                        ErrorScenario::IngestionRateLimit => {
                            let req = cortexpb::WriteRequest {
                                timeseries: vec![cortexpb::TimeSeries {
                                    labels: vec![cortexpb::LabelPair::from_strs(
                                        METRIC_NAME_LABEL,
                                        metric_name,
                                    )],
                                    samples: vec![cortexpb::Sample {
                                        value: 1.0,
                                        timestamp_ms: sample_ts + 1,
                                    }],
                                    ..Default::default()
                                }],
                                source: cortexpb::write_request::SourceEnum::Api as i32,
                                ..Default::default()
                            };
                            ing.push_grpc(user_id, &req).await.unwrap();
                            ing.ingestion_rate.tick();
                        }
                        ErrorScenario::MaxTenantsLimit => {
                            let req = cortexpb::WriteRequest {
                                timeseries: vec![cortexpb::TimeSeries {
                                    labels: vec![cortexpb::LabelPair::from_strs(
                                        METRIC_NAME_LABEL,
                                        metric_name,
                                    )],
                                    samples: vec![cortexpb::Sample {
                                        value: 1.0,
                                        timestamp_ms: sample_ts + 1,
                                    }],
                                    ..Default::default()
                                }],
                                source: cortexpb::write_request::SourceEnum::Api as i32,
                                ..Default::default()
                            };
                            ing.push_grpc("different_tenant", &req).await.unwrap();
                        }
                        ErrorScenario::MaxSeriesLimit => {
                            let req = cortexpb::WriteRequest {
                                timeseries: vec![cortexpb::TimeSeries {
                                    labels: vec![cortexpb::LabelPair::from_strs(
                                        METRIC_NAME_LABEL,
                                        metric_name,
                                    )],
                                    samples: vec![cortexpb::Sample {
                                        value: 1.0,
                                        timestamp_ms: sample_ts + 1,
                                    }],
                                    ..Default::default()
                                }],
                                source: cortexpb::write_request::SourceEnum::Api as i32,
                                ..Default::default()
                            };
                            ing.push_grpc(user_id, &req).await.unwrap();
                        }
                        ErrorScenario::MaxInflightRequestsLimit => {
                            ing.inflight_push_requests.fetch_add(1, Ordering::AcqRel);
                        }
                    }

                    let ts_batch: Vec<cortexpb::TimeSeries> = (0..num_series_per_req)
                        .map(|i| cortexpb::TimeSeries {
                            labels: vec![
                                cortexpb::LabelPair::from_strs(METRIC_NAME_LABEL, metric_name),
                                cortexpb::LabelPair::from_strs("cardinality", i.to_string()),
                            ],
                            samples: vec![cortexpb::Sample {
                                value: i as f64,
                                timestamp_ms: sample_ts,
                            }],
                            ..Default::default()
                        })
                        .collect();

                    let req = Arc::new(cortexpb::WriteRequest {
                        timeseries: ts_batch,
                        source: cortexpb::write_request::SourceEnum::Api as i32,
                        ..Default::default()
                    });

                    let ok_checks = Arc::new(AtomicUsize::new(0));
                    let mut handles = Vec::with_capacity(num_clients);
                    for _ in 0..num_clients {
                        let ing_c = Arc::clone(&ing);
                        let req_c = Arc::clone(&req);
                        let ok_c = Arc::clone(&ok_checks);
                        handles.push(tokio::spawn(async move {
                            let err = ing_c
                                .push_grpc(user_id, &req_c)
                                .await
                                .expect_err("expected push error");
                            let msg = err.to_string();
                            assert!(
                                msg.contains(expected_substr),
                                "scenario={scenario_name} case={case_name} limits={use_instance_limits}: expected '{expected_substr}', got '{msg}'"
                            );
                            ok_c.fetch_add(1, Ordering::Relaxed);
                        }));
                    }
                    for h in handles {
                        h.await.unwrap();
                    }
                    assert_eq!(ok_checks.load(Ordering::Relaxed), num_clients);

                    if matches!(kind, ErrorScenario::MaxInflightRequestsLimit) {
                        ing.inflight_push_requests.fetch_sub(1, Ordering::AcqRel);
                    }
                    ing.stop().await.unwrap();
                }
            }
        }
    });
}

// Ported from BenchmarkIngesterPushExemplars (lines 5033..5083).
#[test]
fn test_ingester_push_exemplars_multi_per_series() {
    block_on_for_tests(async {
        for per_series in [1usize, 4usize] {
            let tmp = tempfile::tempdir().unwrap();
            let reg = Registry::new();
            let (cfg, blocks) = default_ingester_test_config(tmp.path());
            let mut limits = default_limits_test_config();
            limits.max_global_exemplars_per_user = 1_000_000;

            let ing = prepare_ingester_with_blocks_storage(
                cfg,
                blocks,
                Overrides::new(limits),
                Some(&reg),
            )
            .await;

            let start_ts = now_ms();
            let series_count = 50usize;
            let mut timeseries = Vec::with_capacity(series_count);
            for s in 0..series_count {
                let mut exemplars = Vec::with_capacity(per_series);
                for e in 0..per_series {
                    exemplars.push(cortexpb::Exemplar {
                        labels: vec![cortexpb::LabelPair::from_strs(
                            "traceID",
                            format!("trace-{e}"),
                        )],
                        value: e as f64,
                        timestamp_ms: start_ts + e as i64,
                    });
                }
                timeseries.push(cortexpb::TimeSeries {
                    labels: vec![
                        cortexpb::LabelPair::from_strs(METRIC_NAME_LABEL, "test_exemplar_metric"),
                        cortexpb::LabelPair::from_strs("series", s.to_string()),
                    ],
                    samples: vec![cortexpb::Sample {
                        value: 1.0,
                        timestamp_ms: start_ts,
                    }],
                    exemplars,
                    ..Default::default()
                });
            }

            let req = cortexpb::WriteRequest {
                timeseries,
                source: cortexpb::write_request::SourceEnum::Api as i32,
                ..Default::default()
            };
            ing.push_grpc("test", &req).await.unwrap();

            let ex_res = ing
                .handle_query_exemplars(
                    &MetadataMap::new(),
                    "test",
                    ingesterpb::ExemplarQueryRequest {
                        start_timestamp_ms: i64::MIN,
                        end_timestamp_ms: i64::MAX,
                        matchers: vec![ingesterpb::LabelMatchers {
                            matchers: vec![ingesterpb::LabelMatcher {
                                r#type: ingesterpb::MatchType::Equal as i32,
                                name: METRIC_NAME_LABEL.to_string(),
                                value: "test_exemplar_metric".to_string(),
                            }],
                        }],
                    },
                )
                .await
                .unwrap();
            assert_eq!(ex_res.timeseries.len(), series_count);
            for ts in &ex_res.timeseries {
                assert_eq!(ts.exemplars.len(), per_series);
            }

            let families = reg.gather();
            assert_eq!(
                single_metric_value(&families, "cortex_ingester_ingested_exemplars_total"),
                Some((series_count * per_series) as f64)
            );
            assert_eq!(
                single_metric_value(&families, "cortex_ingester_ingested_exemplars_failures_total"),
                Some(0.0)
            );

            ing.stop().await.unwrap();
        }
    });
}

// Exercises WAL replay across ingester restart under all wal_replay_concurrency settings (0, 100, 1000, 2500).
#[test]
fn test_ingester_wal_replay_concurrency_restart() {
    block_on_for_tests(async {
        for wal_replay_conc in [0i64, 100, 1000, 2500] {
            let tmp = tempfile::tempdir().unwrap();
            let (cfg, mut blocks) = default_ingester_test_config(tmp.path());
            blocks.tsdb.wal_replay_concurrency = wal_replay_conc;

            let ing = prepare_ingester_with_blocks_storage(
                cfg.clone(),
                blocks.clone(),
                Overrides::new(default_limits_test_config()),
                None,
            )
            .await;

            let base_ts = now_ms();
            let req = cortexpb::WriteRequest {
                timeseries: (0..25)
                    .map(|i| cortexpb::TimeSeries {
                        labels: vec![
                            cortexpb::LabelPair::from_strs(METRIC_NAME_LABEL, "replay_metric"),
                            cortexpb::LabelPair::from_strs("id", i.to_string()),
                        ],
                        samples: vec![cortexpb::Sample {
                            value: i as f64,
                            timestamp_ms: base_ts + i,
                        }],
                        ..Default::default()
                    })
                    .collect(),
                source: cortexpb::write_request::SourceEnum::Api as i32,
                ..Default::default()
            };
            ing.push_grpc("tenant-replay", &req).await.unwrap();
            ing.stop().await.unwrap();

            let reg = Registry::new();
            let reopened = prepare_ingester_with_blocks_storage(
                cfg,
                blocks,
                Overrides::new(default_limits_test_config()),
                Some(&reg),
            )
            .await;
            let got = query_all_series(&reopened, "tenant-replay").await;
            assert_eq!(got.len(), 25, "wal_replay_concurrency={wal_replay_conc}");
            reopened.stop().await.unwrap();
        }
    });
}
