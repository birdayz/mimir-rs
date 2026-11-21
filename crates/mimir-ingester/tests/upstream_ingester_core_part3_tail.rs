//! Ported from github.com/grafana/mimir/pkg/ingester/ingester_test.go (lines 8986..13090)

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::{to_bytes, Body};
use axum::http::{Method, Request, StatusCode};
use mimir_common::block_on_for_tests;
use mimir_common::services::{Service, ServiceContext, State};
use mimir_common::tenant::LOWER_ORG_ID_HEADER_NAME;
use mimir_config::{
    BlocksStorageConfig, GoDuration, IngestStorageConfig, IngesterConfig, InstanceLimits,
    LimitsConfig, LimitsMap, ModelDuration, Overrides, RuntimeConfigManager, RuntimeConfigValues,
    TSDBConfig,
};
use mimir_proto::cortexpb;
use mimir_proto::ingesterpb;
use mimir_proto::ingesterpb::ingester_client::IngesterClient;
use mimir_proto::ingesterpb::ingester_server::IngesterServer;
use mimir_proto::prost::Message;
use mimir_proto::ringpb::{Desc as RingDesc, InstanceState};
use mimir_proto::symbols::FastSymbolsTable;
use mimir_proto::timeseries::PreallocWriteRequest;
use mimir_ring::kv::inmemory::InMemoryKv;
use mimir_ring::kv::{AsRegisterer, KvClient};
use mimir_ring::lifecycler::{Lifecycler, LifecyclerConfig};
use mimir_ring::ring::replication_strategy::DefaultReplicationStrategy;
use mimir_ring::ring::{ring_desc_codec, Ring, RingSettings};
use mimir_storage::bucket::BucketClient;
use mimir_storage::shipper::{OUT_OF_ORDER_EXTERNAL_LABEL, OUT_OF_ORDER_EXTERNAL_LABEL_VALUE};
use mimir_tsdb::block::BlockMeta;
use mimir_tsdb::chunkenc::{
    CounterResetHint, EncodedChunk, Encoding, FloatHistogram, Histogram, HistogramSpan,
};
use parking_lot::Mutex;
use prometheus::proto::{MetricFamily, MetricType};
use prometheus::Registry;
use tokio::net::TcpListener;
use tonic::metadata::MetadataMap;
use tonic::Code;
use tower::ServiceExt;

use mimir_ingester::ingester::{
    block_generation_calculator, get_ignore_series_limit_for_metric_names_map,
    get_open_tsdbs_concurrency_config, Ingester,
};
use mimir_ingester::{ingester_router, IngesterError, IngesterService, UserTsdb};

const METRIC_NAME_LABEL: &str = "__name__";
const JOB_LABEL: &str = "job";
const USER_ID: &str = "1";

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
    cfg.active_series.active_series_metrics_enabled = true;
    cfg.active_series.active_series_metrics_update_period = GoDuration::from_secs(60);
    cfg.active_series.active_series_metrics_idle_timeout = GoDuration::from_secs(600);

    let mut blocks = BlocksStorageConfig::default();
    blocks.tsdb.dir = tsdb_dir.to_string_lossy().into_owned();
    blocks.tsdb.block_ranges_period = vec![GoDuration::from_hours(2)];
    blocks.tsdb.head_compaction_interval = GoDuration::from_hours(1);
    blocks.tsdb.head_compaction_idle_timeout = GoDuration::from_hours(1);
    blocks.tsdb.ship_interval = GoDuration::ZERO;
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
    runtime_cfg: Option<Arc<RuntimeConfigManager>>,
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
        runtime_cfg,
        registry,
    )
    .expect("create ingester");
    ing.start().await.expect("start ingester");
    ing
}

async fn prepare_ingester_with_ring(
    cfg: IngesterConfig,
    blocks: BlocksStorageConfig,
    overrides: Overrides,
    kv: Arc<dyn KvClient>,
    registry: Option<&Registry>,
) -> (Arc<Ingester>, Arc<Ring>) {
    let ring = Ring::with_store_and_strategy(
        RingSettings::from(&cfg.ring),
        "ingester",
        "collectors/ring",
        kv.clone(),
        Arc::new(DefaultReplicationStrategy),
        registry.map(|r| r as &dyn AsRegisterer),
    )
    .expect("ring");
    let rsvc = Arc::clone(&ring).into_service();
    rsvc.start_async(ServiceContext::background())
        .await
        .expect("start ring");
    rsvc.await_running().await.expect("ring running");

    let lc_cfg = LifecyclerConfig::from_ring_config(&cfg.ring, 9095).expect("lifecycler cfg");
    let lifecycler = Lifecycler::with_store(
        lc_cfg,
        kv,
        None,
        "ingester",
        "collectors/ring",
        blocks.tsdb.flush_blocks_on_shutdown,
        registry.map(|r| r as &dyn AsRegisterer),
    );

    let ing = Ingester::new(
        cfg,
        blocks,
        IngestStorageConfig::default(),
        overrides,
        Some(Arc::clone(&ring)),
        Some(lifecycler),
        None,
        None,
        None,
        registry,
    )
    .expect("create ingester");
    ing.start().await.expect("start ingester");
    (ing, ring)
}

fn to_write_request(
    lbls: &[Vec<(&str, &str)>],
    samples: &[cortexpb::Sample],
    metadata: &[cortexpb::MetricMetadata],
    source: cortexpb::write_request::SourceEnum,
) -> cortexpb::WriteRequest {
    assert_eq!(lbls.len(), samples.len());
    let mut timeseries = Vec::with_capacity(lbls.len());
    for (ls, s) in lbls.iter().zip(samples.iter()) {
        timeseries.push(cortexpb::TimeSeries {
            labels: ls
                .iter()
                .map(|(k, v)| cortexpb::LabelPair::from_strs(*k, *v))
                .collect(),
            samples: vec![*s],
            ..Default::default()
        });
    }
    cortexpb::WriteRequest {
        timeseries,
        metadata: metadata.to_vec(),
        source: source as i32,
        ..Default::default()
    }
}

fn to_write_request_single_series(
    lbls: &[(&str, &str)],
    samples: Vec<cortexpb::Sample>,
    source: cortexpb::write_request::SourceEnum,
) -> cortexpb::WriteRequest {
    cortexpb::WriteRequest {
        timeseries: vec![cortexpb::TimeSeries {
            labels: lbls
                .iter()
                .map(|(k, v)| cortexpb::LabelPair::from_strs(*k, *v))
                .collect(),
            samples,
            ..Default::default()
        }],
        source: source as i32,
        ..Default::default()
    }
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

fn generate_test_float_histogram(i: i64) -> FloatHistogram {
    generate_test_histogram(i).to_float()
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
        reset_hint: cortexpb::histogram::ResetHint::Unknown as i32,
        timestamp: ts,
        ..Default::default()
    }
}

fn float_histogram_to_proto(ts: i64, fh: &FloatHistogram) -> cortexpb::Histogram {
    let spans = |ss: &[HistogramSpan]| -> Vec<cortexpb::BucketSpan> {
        ss.iter()
            .map(|s| cortexpb::BucketSpan {
                offset: s.offset,
                length: s.length,
            })
            .collect()
    };
    cortexpb::Histogram {
        count: Some(cortexpb::histogram::Count::CountFloat(fh.count)),
        zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountFloat(
            fh.zero_count,
        )),
        zero_threshold: fh.zero_threshold,
        sum: fh.sum,
        schema: fh.schema,
        positive_spans: spans(&fh.positive_spans),
        positive_counts: fh.positive_buckets.clone(),
        negative_spans: spans(&fh.negative_spans),
        negative_counts: fh.negative_buckets.clone(),
        reset_hint: cortexpb::histogram::ResetHint::Unknown as i32,
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

fn decode_query_stream_responses(responses: &[ingesterpb::QueryStreamResponse]) -> Vec<DecodedSeries> {
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
                            entry.histograms.push((t, fh));
                        }
                    }
                    EncodedChunk::HistogramSt(c) => {
                        let mut it = c.iter();
                        while let Some((t, h)) = it.next_histogram().expect("hist st next") {
                            let mut fh = h.to_float();
                            fh.counter_reset_hint = CounterResetHint::UnknownCounterReset;
                            entry.histograms.push((t, fh));
                        }
                    }
                    EncodedChunk::FloatHistogram(c) => {
                        let mut it = c.iter();
                        while let Some((t, mut fh)) = it.next_float_histogram().expect("fhist next") {
                            fh.counter_reset_hint = CounterResetHint::UnknownCounterReset;
                            entry.histograms.push((t, fh));
                        }
                    }
                    EncodedChunk::FloatHistogramSt(c) => {
                        let mut it = c.iter();
                        while let Some((t, mut fh)) =
                            it.next_float_histogram().expect("fhist st next")
                        {
                            fh.counter_reset_hint = CounterResetHint::UnknownCounterReset;
                            entry.histograms.push((t, fh));
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

async fn run_test_query_times(
    ing: &Ingester,
    user_id: &str,
    match_type: ingesterpb::MatchType,
    name: &str,
    value: &str,
    start_ms: i64,
    end_ms: i64,
) -> (Vec<DecodedSeries>, Vec<ingesterpb::QueryStreamResponse>) {
    let req = ingesterpb::QueryRequest {
        start_timestamp_ms: start_ms,
        end_timestamp_ms: end_ms,
        matchers: vec![ingesterpb::LabelMatcher {
            r#type: match_type as i32,
            name: name.to_string(),
            value: value.to_string(),
        }],
        streaming_chunks_batch_size: 0,
    };
    let resps = ing
        .handle_query_stream(&MetadataMap::new(), user_id, req)
        .await
        .expect("query stream");
    let decoded = decode_query_stream_responses(&resps);
    (decoded, resps)
}

async fn run_test_query(
    ing: &Ingester,
    user_id: &str,
    match_type: ingesterpb::MatchType,
    name: &str,
    value: &str,
) -> Vec<DecodedSeries> {
    run_test_query_times(ing, user_id, match_type, name, value, i64::MIN, i64::MAX)
        .await
        .0
}

async fn query_chunks_for_metric(
    ing: &Ingester,
    user_id: &str,
    metric_name: &str,
) -> Vec<ingesterpb::Chunk> {
    let req = ingesterpb::QueryRequest {
        start_timestamp_ms: i64::MIN,
        end_timestamp_ms: i64::MAX,
        matchers: vec![ingesterpb::LabelMatcher {
            r#type: ingesterpb::MatchType::Equal as i32,
            name: METRIC_NAME_LABEL.to_string(),
            value: metric_name.to_string(),
        }],
        streaming_chunks_batch_size: 64,
    };
    let resps = ing
        .handle_query_stream(&MetadataMap::new(), user_id, req)
        .await
        .expect("query stream");
    let mut chunks = Vec::new();
    for r in resps {
        for sc in r.streaming_series_chunks {
            chunks.extend(sc.chunks);
        }
    }
    chunks
}

fn find_family<'a>(families: &'a [MetricFamily], name: &str) -> Option<&'a MetricFamily> {
    families.iter().find(|mf| mf.get_name() == name)
}

fn metric_value_map(families: &[MetricFamily], name: &str) -> BTreeMap<Vec<(String, String)>, f64> {
    let mut out = BTreeMap::new();
    let Some(mf) = find_family(families, name) else {
        return out;
    };
    for m in mf.get_metric() {
        let mut lbls: Vec<(String, String)> = m
            .get_label()
            .iter()
            .map(|lp| (lp.get_name().to_string(), lp.get_value().to_string()))
            .collect();
        lbls.sort();
        let v = match mf.get_field_type() {
            MetricType::COUNTER => m.get_counter().get_value(),
            MetricType::GAUGE => m.get_gauge().get_value(),
            _ => 0.0,
        };
        out.insert(lbls, v);
    }
    out
}

fn get_single_gauge(families: &[MetricFamily], name: &str) -> f64 {
    let mf = find_family(families, name).expect("metric family missing");
    mf.get_metric()[0].get_gauge().get_value()
}

fn build_active_series_config(pairs: &[(&str, &str)]) -> LimitsMap<String> {
    pairs
        .iter()
        .map(|(name, matcher)| ((*name).to_string(), (*matcher).to_string()))
        .collect()
}

async fn push_test_metadata(ing: &Ingester, num_metadata: usize, metadata_per_metric: usize) -> Vec<String> {
    let user_ids = vec!["1".to_string(), "2".to_string(), "3".to_string()];
    for uid in &user_ids {
        let mut metadata = Vec::with_capacity(num_metadata * metadata_per_metric);
        for i in 0..num_metadata {
            let metric_name = format!("testmetric_{i}");
            for j in 0..metadata_per_metric {
                metadata.push(cortexpb::MetricMetadata {
                    metric_family_name: metric_name.clone(),
                    help: format!("a help for {j}"),
                    unit: String::new(),
                    r#type: cortexpb::metric_metadata::MetricType::Counter as i32,
                });
            }
        }
        let req = cortexpb::WriteRequest {
            metadata,
            source: cortexpb::write_request::SourceEnum::Api as i32,
            ..Default::default()
        };
        ing.push_grpc(uid, &req).await.expect("push metadata");
    }
    user_ids
}

async fn push_test_samples(
    ing: &Ingester,
    num_series: usize,
    samples_per_series: usize,
    offset: usize,
) -> Vec<String> {
    let user_ids = vec!["1".to_string(), "2".to_string(), "3".to_string()];
    for (u_idx, uid) in user_ids.iter().enumerate() {
        let mut timeseries = Vec::with_capacity(num_series);
        for i in 0..num_series {
            let mut samples = Vec::with_capacity(samples_per_series);
            for j in 0..samples_per_series {
                let val = (i + j + u_idx + offset) as i64;
                samples.push(cortexpb::Sample {
                    timestamp_ms: val,
                    value: val as f64,
                });
            }
            timeseries.push(cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair::from_strs(METRIC_NAME_LABEL, format!("testmetric_{i}")),
                    cortexpb::LabelPair::from_strs(JOB_LABEL, format!("testjob{}", i % 2)),
                ],
                samples,
                ..Default::default()
            });
        }
        let req = cortexpb::WriteRequest {
            timeseries,
            source: cortexpb::write_request::SourceEnum::Api as i32,
            ..Default::default()
        };
        ing.push_grpc(uid, &req).await.expect("push samples");
    }
    user_ids
}

/// Minimal `tracing::Subscriber` capturing formatted log lines so we can assert
/// exact sampled error log counts from `grpc.rs` without extra crates.
struct LogBuffer {
    lines: Mutex<Vec<String>>,
}

impl LogBuffer {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            lines: Mutex::new(Vec::new()),
        })
    }

    fn count(&self, needle: &str) -> usize {
        self.lines
            .lock()
            .iter()
            .filter(|line| line.contains(needle))
            .count()
    }
}

struct CaptureSubscriber {
    buf: Arc<LogBuffer>,
}

struct FieldVisitor(String);

impl tracing::field::Visit for FieldVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if !self.0.is_empty() {
            self.0.push(' ');
        }
        use std::fmt::Write;
        let _ = write!(&mut self.0, "{}={:?}", field.name(), value);
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if !self.0.is_empty() {
            self.0.push(' ');
        }
        use std::fmt::Write;
        let _ = write!(&mut self.0, "{}={}", field.name(), value);
    }
}

impl tracing::Subscriber for CaptureSubscriber {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        *metadata.level() <= tracing::Level::WARN
    }
    fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        let mut visitor = FieldVisitor(String::new());
        event.record(&mut visitor);
        self.buf.lines.lock().push(visitor.0);
    }
    fn enter(&self, _span: &tracing::span::Id) {}
    fn exit(&self, _span: &tracing::span::Id) {}
}

async fn start_grpc_server_and_client(
    ingester: Arc<Ingester>,
) -> (
    IngesterClient<tonic::transport::Channel>,
    tokio::task::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr: SocketAddr = listener.local_addr().expect("local addr");
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    let svc = IngesterService::new(ingester);
    let handle = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(IngesterServer::new(svc))
            .serve_with_incoming(incoming)
            .await;
    });
    let endpoint = format!("http://{addr}");
    let client = IngesterClient::connect(endpoint)
        .await
        .expect("connect grpc client");
    (client, handle)
}

fn grpc_req_for_user(
    user_id: &str,
    req: cortexpb::WriteRequest,
) -> tonic::Request<cortexpb::WriteRequest> {
    let mut r = tonic::Request::new(req);
    r.metadata_mut()
        .insert(LOWER_ORG_ID_HEADER_NAME, user_id.parse().unwrap());
    r
}

// ============================================================================
// Ported tests #48 through #81 (lines 8986..13090 of pkg/ingester/ingester_test.go)
// ============================================================================

// Upstream: pkg/ingester/ingester_test.go::TestIngester_PushInstanceLimits
#[test]
fn test_ingester_push_instance_limits() {
    struct Case {
        name: &'static str,
        limits: InstanceLimits,
        reqs: Vec<(&'static str, Vec<cortexpb::WriteRequest>)>,
        expected_err: Option<IngesterError>,
        expected_optional_logging: bool,
        expected_grpc_err: bool,
    }

    let cases = vec![
        Case {
            name: "should succeed creating one user and series",
            limits: InstanceLimits {
                max_series: 1,
                max_tenants: 1,
                ..Default::default()
            },
            reqs: vec![(
                "test",
                vec![to_write_request(
                    &[vec![(METRIC_NAME_LABEL, "test")]],
                    &[cortexpb::Sample {
                        value: 1.0,
                        timestamp_ms: 9,
                    }],
                    &[cortexpb::MetricMetadata {
                        metric_family_name: "metric_name_1".to_string(),
                        help: "a help for metric_name_1".to_string(),
                        unit: String::new(),
                        r#type: cortexpb::metric_metadata::MetricType::Counter as i32,
                    }],
                    cortexpb::write_request::SourceEnum::Api,
                )],
            )],
            expected_err: None,
            expected_optional_logging: false,
            expected_grpc_err: false,
        },
        Case {
            name: "should fail creating two series",
            limits: InstanceLimits {
                max_series: 1,
                max_tenants: 1,
                ..Default::default()
            },
            reqs: vec![(
                "test",
                vec![
                    to_write_request(
                        &[vec![(METRIC_NAME_LABEL, "test1")]],
                        &[cortexpb::Sample {
                            value: 1.0,
                            timestamp_ms: 9,
                        }],
                        &[],
                        cortexpb::write_request::SourceEnum::Api,
                    ),
                    to_write_request(
                        &[vec![(METRIC_NAME_LABEL, "test2")]],
                        &[cortexpb::Sample {
                            value: 1.0,
                            timestamp_ms: 10,
                        }],
                        &[],
                        cortexpb::write_request::SourceEnum::Api,
                    ),
                ],
            )],
            expected_err: Some(IngesterError::max_in_memory_series_reached()),
            expected_optional_logging: true,
            expected_grpc_err: true,
        },
        Case {
            name: "should fail creating two users",
            limits: InstanceLimits {
                max_series: 1,
                max_tenants: 1,
                ..Default::default()
            },
            reqs: vec![
                (
                    "user1",
                    vec![to_write_request(
                        &[vec![(METRIC_NAME_LABEL, "test1")]],
                        &[cortexpb::Sample {
                            value: 1.0,
                            timestamp_ms: 9,
                        }],
                        &[],
                        cortexpb::write_request::SourceEnum::Api,
                    )],
                ),
                (
                    "user2",
                    vec![to_write_request(
                        &[vec![(METRIC_NAME_LABEL, "test2")]],
                        &[cortexpb::Sample {
                            value: 1.0,
                            timestamp_ms: 10,
                        }],
                        &[],
                        cortexpb::write_request::SourceEnum::Api,
                    )],
                ),
            ],
            expected_err: Some(IngesterError::max_tenants_reached()),
            expected_optional_logging: true,
            expected_grpc_err: true,
        },
        Case {
            name: "should fail pushing samples in two requests due to rate limit",
            limits: InstanceLimits {
                max_series: 1,
                max_tenants: 1,
                max_ingestion_rate: 0.001,
                ..Default::default()
            },
            reqs: vec![(
                "user1",
                vec![
                    to_write_request(
                        &[vec![(METRIC_NAME_LABEL, "test1")]],
                        &[cortexpb::Sample {
                            value: 1.0,
                            timestamp_ms: 9,
                        }],
                        &[],
                        cortexpb::write_request::SourceEnum::Api,
                    ),
                    to_write_request(
                        &[vec![(METRIC_NAME_LABEL, "test1")]],
                        &[cortexpb::Sample {
                            value: 1.0,
                            timestamp_ms: 10,
                        }],
                        &[],
                        cortexpb::write_request::SourceEnum::Api,
                    ),
                ],
            )],
            expected_err: Some(IngesterError::max_ingestion_rate_reached()),
            expected_optional_logging: false,
            expected_grpc_err: false,
        },
    ];

    block_on_for_tests(async {
        for tc in cases {
            let tmp = tempfile::tempdir().unwrap();
            let reg = Registry::new();
            let (mut cfg, blocks) = default_ingester_test_config(tmp.path());
            cfg.instance_limits = tc.limits.clone();

            let ing = prepare_ingester_with_blocks_storage(
                cfg,
                blocks,
                Overrides::new(default_limits_test_config()),
                None,
                Some(&reg),
            )
            .await;

            let total_pushes: usize = tc.reqs.iter().map(|(_, rs)| rs.len()).sum();
            let mut push_idx = 0usize;
            for (uid, requests) in &tc.reqs {
                for req in requests {
                    push_idx += 1;
                    let res = ing.push_grpc(uid, req).await;
                    if push_idx < total_pushes {
                        assert!(res.is_ok(), "{}: push {push_idx} failed: {:?}", tc.name, res);
                    } else if let Some(ref exp_err) = tc.expected_err {
                        let err = res.expect_err(&format!("{}: expected final push error", tc.name));
                        assert!(
                            err.to_string().contains(&exp_err.to_string()),
                            "{}: expected error containing {:?}, got {:?}",
                            tc.name,
                            exp_err.to_string(),
                            err.to_string()
                        );
                        assert_eq!(
                            err.is_optional_logging(),
                            tc.expected_optional_logging,
                            "{}: optional logging mismatch",
                            tc.name
                        );
                        if tc.expected_grpc_err {
                            let status = err.map_push_error_to_status();
                            assert_eq!(status.code(), Code::Unavailable, "{}", tc.name);
                        }
                    } else {
                        assert!(res.is_ok(), "{}: expected no error, got {:?}", tc.name, res);
                    }

                    ing.ingestion_rate.tick();
                    let families = reg.gather();
                    let rate =
                        get_single_gauge(&families, "cortex_ingester_ingestion_rate_samples_per_second");
                    assert!(rate > 0.0, "{}: expected non-zero ingestion rate", tc.name);
                }
            }

            ing.stop().await.unwrap();
        }
    });
}

// Upstream: pkg/ingester/ingester_test.go::TestIngester_PushGrpcMethod_Disabled
#[test]
fn test_ingester_push_grpc_method_disabled() {
    block_on_for_tests(async {
        let tmp = tempfile::tempdir().unwrap();
        let (mut cfg, blocks) = default_ingester_test_config(tmp.path());
        cfg.push_grpc_method_enabled = false;
        let reg = Registry::new();

        let ing = prepare_ingester_with_blocks_storage(
            cfg,
            blocks,
            Overrides::new(default_limits_test_config()),
            None,
            Some(&reg),
        )
        .await;

        let req = to_write_request_single_series(
            &[(METRIC_NAME_LABEL, "foo"), ("l", "1")],
            vec![cortexpb::Sample {
                timestamp_ms: 1_000,
                value: 1.0,
            }],
            cortexpb::write_request::SourceEnum::Api,
        );
        let err = ing.push_grpc("test", &req).await.unwrap_err();
        assert_eq!(err, IngesterError::PushGrpcDisabled);
        ing.stop().await.unwrap();
    });
}

// Upstream: pkg/ingester/ingester_test.go::TestIngester_instanceLimitsMetrics
#[test]
fn test_ingester_instance_limits_metrics() {
    let tmp = tempfile::tempdir().unwrap();
    let reg = Registry::new();
    let (cfg, blocks) = default_ingester_test_config(tmp.path());

    let rt_cfg = Arc::new(
        RuntimeConfigManager::new(
            Default::default(),
            default_limits_test_config(),
            cfg.instance_limits.clone(),
        )
        .unwrap(),
    );
    let initial = InstanceLimits {
        max_ingestion_rate: 10.0,
        max_tenants: 20,
        max_series: 30,
        max_inflight_push_requests: 40,
        max_inflight_push_requests_bytes: 50,
        ..Default::default()
    };
    rt_cfg.set_values_for_tests(RuntimeConfigValues {
        ingester_limits: Some(initial.clone()),
        ..Default::default()
    });

    let _ing = Ingester::new(
        cfg,
        blocks,
        IngestStorageConfig::default(),
        rt_cfg.overrides(),
        None,
        None,
        None,
        None,
        Some(Arc::clone(&rt_cfg)),
        Some(&reg),
    )
    .unwrap();

    let families = reg.gather();
    let got = metric_value_map(&families, "cortex_ingester_instance_limits");
    assert_eq!(
        got,
        BTreeMap::from([
            (vec![("limit".into(), "max_ingestion_rate".into())], 10.0),
            (vec![("limit".into(), "max_series".into())], 30.0),
            (vec![("limit".into(), "max_tenants".into())], 20.0),
            (
                vec![("limit".into(), "max_inflight_push_requests".into())],
                40.0
            ),
            (
                vec![("limit".into(), "max_inflight_push_requests_bytes".into())],
                50.0
            ),
        ])
    );

    let mut updated = initial;
    updated.max_tenants = 1000;
    updated.max_series = 2000;
    rt_cfg.set_values_for_tests(RuntimeConfigValues {
        ingester_limits: Some(updated),
        ..Default::default()
    });

    let families = reg.gather();
    let got = metric_value_map(&families, "cortex_ingester_instance_limits");
    assert_eq!(
        got,
        BTreeMap::from([
            (vec![("limit".into(), "max_ingestion_rate".into())], 10.0),
            (vec![("limit".into(), "max_series".into())], 2000.0),
            (vec![("limit".into(), "max_tenants".into())], 1000.0),
            (
                vec![("limit".into(), "max_inflight_push_requests".into())],
                40.0
            ),
            (
                vec![("limit".into(), "max_inflight_push_requests_bytes".into())],
                50.0
            ),
        ])
    );
}

// Upstream: pkg/ingester/ingester_test.go::TestIngesterPurgeMetadata
#[test]
fn test_ingester_purge_metadata() {
    block_on_for_tests(async {
        let tmp = tempfile::tempdir().unwrap();
        let (mut cfg, blocks) = default_ingester_test_config(tmp.path());
        cfg.metadata_retain_period = GoDuration::from_millis(20);

        let ing = prepare_ingester_with_blocks_storage(
            cfg,
            blocks,
            Overrides::new(default_limits_test_config()),
            None,
            None,
        )
        .await;

        let user_ids = push_test_metadata(&ing, 10, 3).await;
        tokio::time::sleep(Duration::from_millis(40)).await;
        ing.purge_user_metrics_metadata();

        for uid in &user_ids {
            let resp = ing
                .handle_metrics_metadata(
                    &MetadataMap::new(),
                    uid,
                    ingesterpb::MetricsMetadataRequest {
                        limit: -1,
                        limit_per_metric: -1,
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            assert_eq!(resp.metadata.len(), 0);
        }
        ing.stop().await.unwrap();
    });
}

// Upstream: pkg/ingester/ingester_test.go::TestIngesterMetadataMetrics
#[test]
fn test_ingester_metadata_metrics() {
    block_on_for_tests(async {
        let tmp = tempfile::tempdir().unwrap();
        let reg = Registry::new();
        let (mut cfg, blocks) = default_ingester_test_config(tmp.path());
        cfg.metadata_retain_period = GoDuration::from_millis(20);

        let ing = prepare_ingester_with_blocks_storage(
            cfg,
            blocks,
            Overrides::new(default_limits_test_config()),
            None,
            Some(&reg),
        )
        .await;

        push_test_metadata(&ing, 10, 3).await;
        push_test_metadata(&ing, 10, 3).await;
        push_test_metadata(&ing, 10, 3).await;

        let families = reg.gather();
        assert_eq!(
            get_single_gauge(&families, "cortex_ingester_memory_metadata"),
            90.0
        );
        assert_eq!(
            metric_value_map(&families, "cortex_ingester_memory_metadata_created_total"),
            BTreeMap::from([
                (vec![("user".into(), "1".into())], 30.0),
                (vec![("user".into(), "2".into())], 30.0),
                (vec![("user".into(), "3".into())], 30.0),
            ])
        );

        tokio::time::sleep(Duration::from_millis(40)).await;
        ing.purge_user_metrics_metadata();

        let families = reg.gather();
        assert_eq!(
            get_single_gauge(&families, "cortex_ingester_memory_metadata"),
            0.0
        );
        assert_eq!(
            metric_value_map(&families, "cortex_ingester_memory_metadata_created_total"),
            BTreeMap::from([
                (vec![("user".into(), "1".into())], 30.0),
                (vec![("user".into(), "2".into())], 30.0),
                (vec![("user".into(), "3".into())], 30.0),
            ])
        );
        assert_eq!(
            metric_value_map(&families, "cortex_ingester_memory_metadata_removed_total"),
            BTreeMap::from([
                (vec![("user".into(), "1".into())], 30.0),
                (vec![("user".into(), "2".into())], 30.0),
                (vec![("user".into(), "3".into())], 30.0),
            ])
        );
        ing.stop().await.unwrap();
    });
}

// Upstream: pkg/ingester/ingester_test.go::TestIngesterNoRW2MetadataRefLeaks
#[test]
fn test_ingester_no_rw2_metadata_ref_leaks() {
    block_on_for_tests(async {
        let tmp = tempfile::tempdir().unwrap();
        let reg = Registry::new();
        let (mut cfg, blocks) = default_ingester_test_config(tmp.path());
        cfg.metadata_retain_period = GoDuration::from_secs(20);

        let ing = prepare_ingester_with_blocks_storage(
            cfg,
            blocks,
            Overrides::new(default_limits_test_config()),
            None,
            Some(&reg),
        )
        .await;

        let mut syms = FastSymbolsTable::new(16);
        let orig = cortexpb::WriteRequest {
            timeseries_rw2: vec![cortexpb::TimeSeriesRw2 {
                labels_refs: vec![
                    syms.symbolize("__name__"),
                    syms.symbolize("test_metric_total"),
                    syms.symbolize("job"),
                    syms.symbolize("test_job"),
                ],
                samples: vec![cortexpb::Sample {
                    value: 123.456,
                    timestamp_ms: 1234567890,
                }],
                exemplars: vec![cortexpb::ExemplarRw2 {
                    value: 123.456,
                    timestamp: 1234567890,
                    labels_refs: vec![
                        syms.symbolize("__name__"),
                        syms.symbolize("test_metric_total"),
                        syms.symbolize("traceID"),
                        syms.symbolize("1234567890abcdef"),
                    ],
                }],
                metadata: Some(cortexpb::MetadataRw2 {
                    r#type: cortexpb::metadata_rw2::MetricType::Counter as i32,
                    help_ref: syms.symbolize("test_metric_help"),
                    unit_ref: syms.symbolize("test_metric_unit"),
                }),
                ..Default::default()
            }],
            symbols_rw2: syms.symbols(),
            ..Default::default()
        };
        let buf = orig.encode_to_vec();

        let mut wr = PreallocWriteRequest {
            unmarshal_from_rw2: true,
            skip_normalize_metadata_metric_name: true,
            skip_deduplicate_metadata: true,
            ..Default::default()
        };
        wr.unmarshal(&buf).expect("unmarshal rw2");

        ing.push_grpc(USER_ID, &wr.write_request).await.unwrap();

        let meta = ing
            .handle_metrics_metadata(
                &MetadataMap::new(),
                USER_ID,
                ingesterpb::MetricsMetadataRequest {
                    metric: "test_metric_total".to_string(),
                    limit: 1,
                    limit_per_metric: 1,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(
            meta.metadata,
            vec![cortexpb::MetricMetadata {
                metric_family_name: "test_metric_total".to_string(),
                r#type: cortexpb::metric_metadata::MetricType::Counter as i32,
                help: "test_metric_help".to_string(),
                unit: "test_metric_unit".to_string(),
            }]
        );
        ing.stop().await.unwrap();
    });
}

// Upstream: pkg/ingester/ingester_test.go::TestIngesterSendsOnlySeriesWithData
#[test]
fn test_ingester_sends_only_series_with_data() {
    block_on_for_tests(async {
        let tmp = tempfile::tempdir().unwrap();
        let (cfg, blocks) = default_ingester_test_config(tmp.path());
        let ing = prepare_ingester_with_blocks_storage(
            cfg,
            blocks,
            Overrides::new(default_limits_test_config()),
            None,
            None,
        )
        .await;

        let user_ids = push_test_samples(&ing, 10, 1000, 0).await;
        let end = now_ms();
        let start = end - 15_000;
        for uid in &user_ids {
            let (_decoded, resps) = run_test_query_times(
                &ing,
                uid,
                ingesterpb::MatchType::RegexMatch,
                JOB_LABEL,
                ".+",
                start,
                end,
            )
            .await;
            assert_eq!(resps.len(), 1);
            assert!(resps[0].streaming_series.is_empty());
            assert!(resps[0].streaming_series_chunks.is_empty());
            assert!(resps[0].is_end_of_series_stream);
        }
        ing.stop().await.unwrap();
    });
}

// Upstream: pkg/ingester/ingester_test.go::TestIngester_Push_SeriesWithBlankLabel
#[test]
fn test_ingester_push_series_with_blank_label() {
    block_on_for_tests(async {
        let tmp = tempfile::tempdir().unwrap();
        let (cfg, blocks) = default_ingester_test_config(tmp.path());
        let ing = prepare_ingester_with_blocks_storage(
            cfg,
            blocks,
            Overrides::new(default_limits_test_config()),
            None,
            None,
        )
        .await;

        let req = to_write_request(
            &[vec![
                (METRIC_NAME_LABEL, "testmetric"),
                ("bar", ""),
                ("foo", ""),
            ]],
            &[cortexpb::Sample {
                timestamp_ms: 1,
                value: 0.0,
            }],
            &[],
            cortexpb::write_request::SourceEnum::Api,
        );
        ing.push_grpc(USER_ID, &req).await.unwrap();

        let res = run_test_query(
            &ing,
            USER_ID,
            ingesterpb::MatchType::Equal,
            METRIC_NAME_LABEL,
            "testmetric",
        )
        .await;
        assert_eq!(
            res,
            vec![DecodedSeries {
                labels: BTreeMap::from([(METRIC_NAME_LABEL.into(), "testmetric".into())]),
                floats: vec![(1, 0.0)],
                histograms: vec![],
            }]
        );
        ing.stop().await.unwrap();
    });
}

// Upstream: pkg/ingester/ingester_test.go::TestIngesterUserLimitExceeded
#[test]
fn test_ingester_user_limit_exceeded() {
    block_on_for_tests(async {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().to_path_buf();
        let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));

        let mut limits = default_limits_test_config();
        limits.max_global_series_per_user = 1;
        limits.max_global_metadata_per_user = 1;

        let new_ingester = |kv: Arc<dyn KvClient>| {
            let data_dir = data_dir.clone();
            let limits = limits.clone();
            async move {
                let (mut cfg, blocks) = default_ingester_test_config(&data_dir);
                cfg.ring.replication_factor = 1;
                let (ing, _ring) =
                    prepare_ingester_with_ring(cfg, blocks, Overrides::new(limits), kv, None).await;
                ing
            }
        };

        let mut ing = new_ingester(kv.clone()).await;

        let labels1 = vec![(METRIC_NAME_LABEL, "testmetric"), ("foo", "bar")];
        let sample1 = cortexpb::Sample {
            timestamp_ms: 0,
            value: 1.0,
        };
        let sample2 = cortexpb::Sample {
            timestamp_ms: 1,
            value: 2.0,
        };
        let labels3 = vec![(METRIC_NAME_LABEL, "testmetric"), ("foo", "biz")];
        let sample3 = cortexpb::Sample {
            timestamp_ms: 1,
            value: 3.0,
        };

        let metadata1 = cortexpb::MetricMetadata {
            metric_family_name: "testmetric".to_string(),
            help: "a help for testmetric".to_string(),
            unit: String::new(),
            r#type: cortexpb::metric_metadata::MetricType::Counter as i32,
        };
        let metadata2 = cortexpb::MetricMetadata {
            metric_family_name: "testmetric2".to_string(),
            help: "a help for testmetric2".to_string(),
            unit: String::new(),
            r#type: cortexpb::metric_metadata::MetricType::Counter as i32,
        };

        ing.push_grpc(
            USER_ID,
            &to_write_request(
                std::slice::from_ref(&labels1),
                &[sample1],
                std::slice::from_ref(&metadata1),
                cortexpb::write_request::SourceEnum::Api,
            ),
        )
        .await
        .unwrap();

        let test_limits = |ing: Arc<Ingester>| {
            let labels1 = labels1.clone();
            let labels3 = labels3.clone();
            let metadata1 = metadata1.clone();
            let metadata2 = metadata2.clone();
            async move {
                let err = ing
                    .push_grpc(
                        USER_ID,
                        &to_write_request(
                            &[labels1.clone(), labels3.clone()],
                            &[sample2, sample3],
                            &[],
                            cortexpb::write_request::SourceEnum::Api,
                        ),
                    )
                    .await
                    .unwrap_err();
                let status = err.map_push_error_to_status();
                assert_eq!(status.code(), Code::FailedPrecondition);
                let expected = IngesterError::per_user_series_limit_reached(
                    ing.limiter.max_series_per_user(USER_ID, 0),
                )
                .with_user(USER_ID, 1);
                assert_eq!(status.message(), expected.to_string());

                // Metadata is best effort.
                ing.push_grpc(
                    USER_ID,
                    &to_write_request(
                        &[],
                        &[],
                        &[metadata1.clone(), metadata2.clone()],
                        cortexpb::write_request::SourceEnum::Api,
                    ),
                )
                .await
                .unwrap();

                let res = run_test_query(
                    &ing,
                    USER_ID,
                    ingesterpb::MatchType::Equal,
                    METRIC_NAME_LABEL,
                    "testmetric",
                )
                .await;
                assert_eq!(
                    res,
                    vec![DecodedSeries {
                        labels: BTreeMap::from([
                            (METRIC_NAME_LABEL.into(), "testmetric".into()),
                            ("foo".into(), "bar".into()),
                        ]),
                        floats: vec![(0, 1.0), (1, 2.0)],
                        histograms: vec![],
                    }]
                );

                let meta = ing
                    .handle_metrics_metadata(
                        &MetadataMap::new(),
                        USER_ID,
                        ingesterpb::MetricsMetadataRequest {
                        limit: -1,
                        limit_per_metric: -1,
                        ..Default::default()
                    },
                    )
                    .await
                    .unwrap();
                assert_eq!(meta.metadata, vec![metadata1.clone()]);
            }
        };

        test_limits(ing.clone()).await;

        ing.stop().await.unwrap();
        ing = new_ingester(kv).await;
        test_limits(ing.clone()).await;
        ing.stop().await.unwrap();
    });
}

// Upstream: pkg/ingester/ingester_test.go::TestIngesterMetricLimitExceeded
#[test]
fn test_ingester_metric_limit_exceeded() {
    block_on_for_tests(async {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().to_path_buf();
        let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));

        let mut limits = default_limits_test_config();
        limits.max_global_series_per_metric = 1;
        limits.max_global_metadata_per_metric = 1;

        let new_ingester = |kv: Arc<dyn KvClient>| {
            let data_dir = data_dir.clone();
            let limits = limits.clone();
            async move {
                let (mut cfg, blocks) = default_ingester_test_config(&data_dir);
                cfg.ring.replication_factor = 1;
                let (ing, _ring) =
                    prepare_ingester_with_ring(cfg, blocks, Overrides::new(limits), kv, None).await;
                ing
            }
        };

        let mut ing = new_ingester(kv.clone()).await;

        let labels1 = vec![(METRIC_NAME_LABEL, "testmetric"), ("foo", "bar")];
        let sample1 = cortexpb::Sample {
            timestamp_ms: 0,
            value: 1.0,
        };
        let sample2 = cortexpb::Sample {
            timestamp_ms: 1,
            value: 2.0,
        };
        let labels3 = vec![(METRIC_NAME_LABEL, "testmetric"), ("foo", "biz")];
        let sample3 = cortexpb::Sample {
            timestamp_ms: 1,
            value: 3.0,
        };

        let metadata1 = cortexpb::MetricMetadata {
            metric_family_name: "testmetric".to_string(),
            help: "a help for testmetric".to_string(),
            unit: String::new(),
            r#type: cortexpb::metric_metadata::MetricType::Counter as i32,
        };
        let metadata2 = cortexpb::MetricMetadata {
            metric_family_name: "testmetric".to_string(),
            help: "a help for testmetric2".to_string(),
            unit: String::new(),
            r#type: cortexpb::metric_metadata::MetricType::Counter as i32,
        };

        ing.push_grpc(
            USER_ID,
            &to_write_request(
                std::slice::from_ref(&labels1),
                &[sample1],
                std::slice::from_ref(&metadata1),
                cortexpb::write_request::SourceEnum::Api,
            ),
        )
        .await
        .unwrap();

        let test_limits = |ing: Arc<Ingester>| {
            let labels1 = labels1.clone();
            let labels3 = labels3.clone();
            let metadata1 = metadata1.clone();
            let metadata2 = metadata2.clone();
            async move {
                let err = ing
                    .push_grpc(
                        USER_ID,
                        &to_write_request(
                            &[labels1.clone(), labels3.clone()],
                            &[sample2, sample3],
                            &[],
                            cortexpb::write_request::SourceEnum::Api,
                        ),
                    )
                    .await
                    .unwrap_err();
                let status = err.map_push_error_to_status();
                assert_eq!(status.code(), Code::FailedPrecondition);
                let expected = IngesterError::per_metric_series_limit_reached(
                    ing.limiter.max_series_per_metric(USER_ID),
                    "testmetric{foo=\"biz\"}".to_string(),
                )
                .with_user(USER_ID, 1);
                assert_eq!(status.message(), expected.to_string());

                ing.push_grpc(
                    USER_ID,
                    &to_write_request(
                        &[],
                        &[],
                        &[metadata1.clone(), metadata2.clone()],
                        cortexpb::write_request::SourceEnum::Api,
                    ),
                )
                .await
                .unwrap();

                let res = run_test_query(
                    &ing,
                    USER_ID,
                    ingesterpb::MatchType::Equal,
                    METRIC_NAME_LABEL,
                    "testmetric",
                )
                .await;
                assert_eq!(
                    res,
                    vec![DecodedSeries {
                        labels: BTreeMap::from([
                            (METRIC_NAME_LABEL.into(), "testmetric".into()),
                            ("foo".into(), "bar".into()),
                        ]),
                        floats: vec![(0, 1.0), (1, 2.0)],
                        histograms: vec![],
                    }]
                );

                let meta = ing
                    .handle_metrics_metadata(
                        &MetadataMap::new(),
                        USER_ID,
                        ingesterpb::MetricsMetadataRequest {
                        limit: -1,
                        limit_per_metric: -1,
                        ..Default::default()
                    },
                    )
                    .await
                    .unwrap();
                assert_eq!(meta.metadata, vec![metadata1.clone()]);
            }
        };

        test_limits(ing.clone()).await;

        ing.stop().await.unwrap();
        ing = new_ingester(kv).await;
        test_limits(ing.clone()).await;
        ing.stop().await.unwrap();
    });
}

// Upstream: pkg/ingester/ingester_test.go::TestIngesterActiveSeries
#[test]
fn test_ingester_active_series() {
    let labels_to_push = vec![
        vec![
            (METRIC_NAME_LABEL, "test_metric"),
            ("bool", "false"),
            ("team", "a"),
        ],
        vec![
            (METRIC_NAME_LABEL, "test_metric"),
            ("bool", "false"),
            ("team", "b"),
        ],
        vec![
            (METRIC_NAME_LABEL, "test_metric"),
            ("bool", "true"),
            ("team", "a"),
        ],
        vec![
            (METRIC_NAME_LABEL, "test_metric"),
            ("bool", "true"),
            ("team", "b"),
        ],
    ];
    let labels_to_push_otlp = vec![
        vec![
            (METRIC_NAME_LABEL, "test_metric_otlp"),
            ("bool", "false"),
            ("team", "a"),
        ],
        vec![
            (METRIC_NAME_LABEL, "test_metric_otlp"),
            ("bool", "false"),
            ("team", "b"),
        ],
        vec![
            (METRIC_NAME_LABEL, "test_metric_otlp"),
            ("bool", "true"),
            ("team", "a"),
        ],
        vec![
            (METRIC_NAME_LABEL, "test_metric_otlp"),
            ("bool", "true"),
            ("team", "b"),
        ],
    ];
    let labels_to_push_hist = vec![
        vec![
            (METRIC_NAME_LABEL, "test_histogram_metric"),
            ("bool", "false"),
            ("team", "a"),
        ],
        vec![
            (METRIC_NAME_LABEL, "test_histogram_metric"),
            ("bool", "false"),
            ("team", "b"),
        ],
        vec![
            (METRIC_NAME_LABEL, "test_histogram_metric"),
            ("bool", "true"),
            ("team", "a"),
        ],
        vec![
            (METRIC_NAME_LABEL, "test_histogram_metric"),
            ("bool", "true"),
            ("team", "b"),
        ],
    ];

    let req_float = |lbls: &[(&str, &str)], ts: i64, src: cortexpb::write_request::SourceEnum| {
        to_write_request_single_series(
            lbls,
            vec![cortexpb::Sample {
                timestamp_ms: ts,
                value: 1.0,
            }],
            src,
        )
    };
    let req_hist = |lbls: &[(&str, &str)], ts: i64| cortexpb::WriteRequest {
        timeseries: vec![cortexpb::TimeSeries {
            labels: lbls
                .iter()
                .map(|(k, v)| cortexpb::LabelPair::from_strs(*k, *v))
                .collect(),
            histograms: vec![float_histogram_to_proto(
                ts,
                &generate_test_float_histogram(1),
            )],
            ..Default::default()
        }],
        source: cortexpb::write_request::SourceEnum::Api as i32,
        ..Default::default()
    };

    let user1 = "test_user";
    let user2 = "other_test_user";
    let default_trackers = build_active_series_config(&[
        ("bool_is_true_flagbased", r#"{bool="true"}"#),
        ("bool_is_false_flagbased", r#"{bool="false"}"#),
    ]);
    let tenant_trackers =
        build_active_series_config(&[("team_a", r#"{team="a"}"#), ("team_b", r#"{team="b"}"#)]);

    let make_ingester = |disable_active_series: bool| {
        let default_trackers = default_trackers.clone();
        let tenant_trackers = tenant_trackers.clone();
        async move {
            let tmp = tempfile::tempdir().unwrap();
            let reg = Registry::new();
            let (mut cfg, blocks) = default_ingester_test_config(tmp.path());
            cfg.active_series.active_series_metrics_enabled = !disable_active_series;

            let mut limits = default_limits_test_config();
            limits.active_series_custom_trackers = default_trackers;
            limits.native_histograms_ingestion_enabled = true;

            let mut tenant_limits = limits.clone();
            tenant_limits.active_series_custom_trackers = tenant_trackers;

            let overrides = Overrides::with_static_map(
                limits,
                HashMap::from([(user1.to_string(), tenant_limits)]),
            );
            let ing =
                prepare_ingester_with_blocks_storage(cfg, blocks, overrides, None, Some(&reg))
                    .await;
            (tmp, reg, ing)
        }
    };

    let push_all = |ing: Arc<Ingester>| {
        let labels_to_push = labels_to_push.clone();
        let labels_to_push_otlp = labels_to_push_otlp.clone();
        let labels_to_push_hist = labels_to_push_hist.clone();
        async move {
            let ts = now_ms();
            for uid in [user1, user2] {
                for l in &labels_to_push {
                    ing.push_grpc(
                        uid,
                        &req_float(l, ts, cortexpb::write_request::SourceEnum::Api),
                    )
                    .await
                    .unwrap();
                }
                for l in &labels_to_push_otlp {
                    ing.push_grpc(
                        uid,
                        &req_float(l, ts, cortexpb::write_request::SourceEnum::Otlp),
                    )
                    .await
                    .unwrap();
                }
                for l in &labels_to_push_hist {
                    ing.push_grpc(uid, &req_hist(l, ts)).await.unwrap();
                }
            }
        }
    };

    block_on_for_tests(async {
        // Subtest 1: successful push, should count active series
        {
            let (_tmp, reg, ing) = make_ingester(false).await;
            push_all(ing.clone()).await;
            ing.update_active_series_metrics(SystemTime::now());
            let f = reg.gather();
            assert_eq!(
                metric_value_map(&f, "cortex_ingester_active_series"),
                BTreeMap::from([
                    (vec![("user".into(), user2.into())], 12.0),
                    (vec![("user".into(), user1.into())], 12.0),
                ])
            );
            assert_eq!(
                metric_value_map(&f, "cortex_ingester_active_otlp_series"),
                BTreeMap::from([
                    (vec![("user".into(), user2.into())], 4.0),
                    (vec![("user".into(), user1.into())], 4.0),
                ])
            );
            assert_eq!(
                metric_value_map(&f, "cortex_ingester_active_series_custom_tracker"),
                BTreeMap::from([
                    (
                        vec![("name".into(), "team_a".into()), ("user".into(), user1.into())],
                        6.0
                    ),
                    (
                        vec![("name".into(), "team_b".into()), ("user".into(), user1.into())],
                        6.0
                    ),
                    (
                        vec![
                            ("name".into(), "bool_is_true_flagbased".into()),
                            ("user".into(), user2.into()),
                        ],
                        6.0
                    ),
                    (
                        vec![
                            ("name".into(), "bool_is_false_flagbased".into()),
                            ("user".into(), user2.into()),
                        ],
                        6.0
                    ),
                ])
            );
            assert_eq!(
                metric_value_map(&f, "cortex_ingester_active_native_histogram_series"),
                BTreeMap::from([
                    (vec![("user".into(), user2.into())], 4.0),
                    (vec![("user".into(), user1.into())], 4.0),
                ])
            );
            assert_eq!(
                metric_value_map(
                    &f,
                    "cortex_ingester_active_native_histogram_series_custom_tracker"
                ),
                BTreeMap::from([
                    (
                        vec![("name".into(), "team_a".into()), ("user".into(), user1.into())],
                        2.0
                    ),
                    (
                        vec![("name".into(), "team_b".into()), ("user".into(), user1.into())],
                        2.0
                    ),
                    (
                        vec![
                            ("name".into(), "bool_is_true_flagbased".into()),
                            ("user".into(), user2.into()),
                        ],
                        2.0
                    ),
                    (
                        vec![
                            ("name".into(), "bool_is_false_flagbased".into()),
                            ("user".into(), user2.into()),
                        ],
                        2.0
                    ),
                ])
            );
            assert_eq!(
                metric_value_map(&f, "cortex_ingester_active_native_histogram_buckets"),
                BTreeMap::from([
                    (vec![("user".into(), user2.into())], 32.0),
                    (vec![("user".into(), user1.into())], 32.0),
                ])
            );
            assert_eq!(
                metric_value_map(
                    &f,
                    "cortex_ingester_active_native_histogram_buckets_custom_tracker"
                ),
                BTreeMap::from([
                    (
                        vec![("name".into(), "team_a".into()), ("user".into(), user1.into())],
                        16.0
                    ),
                    (
                        vec![("name".into(), "team_b".into()), ("user".into(), user1.into())],
                        16.0
                    ),
                    (
                        vec![
                            ("name".into(), "bool_is_true_flagbased".into()),
                            ("user".into(), user2.into()),
                        ],
                        16.0
                    ),
                    (
                        vec![
                            ("name".into(), "bool_is_false_flagbased".into()),
                            ("user".into(), user2.into()),
                        ],
                        16.0
                    ),
                ])
            );
            ing.stop().await.unwrap();
        }

        // Subtest 2: should cleanup metrics when tsdb closed
        {
            let (_tmp, reg, ing) = make_ingester(false).await;
            push_all(ing.clone()).await;
            ing.update_active_series_metrics(SystemTime::now());
            assert!(!metric_value_map(&reg.gather(), "cortex_ingester_active_series").is_empty());
            ing.close_all_tsdbs().await;
            let f = reg.gather();
            for metric in [
                "cortex_ingester_active_series",
                "cortex_ingester_active_otlp_series",
                "cortex_ingester_active_series_custom_tracker",
                "cortex_ingester_active_native_histogram_series",
                "cortex_ingester_active_native_histogram_series_custom_tracker",
                "cortex_ingester_active_native_histogram_buckets",
                "cortex_ingester_active_native_histogram_buckets_custom_tracker",
            ] {
                assert!(
                    metric_value_map(&f, metric).is_empty(),
                    "expected {metric} empty after close_all_tsdbs"
                );
            }
            ing.stop().await.unwrap();
        }

        // Subtest 3: should track custom matchers, removing when zero
        {
            let (_tmp, reg, ing) = make_ingester(false).await;
            let mut cur_time = SystemTime::now();
            push_all(ing.clone()).await;
            ing.update_active_series_metrics(cur_time);
            assert_eq!(
                metric_value_map(&reg.gather(), "cortex_ingester_active_series").len(),
                2
            );

            tokio::time::sleep(Duration::from_millis(5)).await;
            cur_time = SystemTime::now();
            let ts = now_ms();
            for l in &labels_to_push {
                ing.push_grpc(
                    user1,
                    &req_float(l, ts, cortexpb::write_request::SourceEnum::Api),
                )
                .await
                .unwrap();
            }
            for l in &labels_to_push_otlp {
                ing.push_grpc(
                    user1,
                    &req_float(l, ts, cortexpb::write_request::SourceEnum::Otlp),
                )
                .await
                .unwrap();
            }
            for l in &labels_to_push_hist {
                ing.push_grpc(user1, &req_hist(l, ts)).await.unwrap();
            }

            let idle_dur = Duration::from_nanos(
                ing.cfg.active_series.active_series_metrics_idle_timeout.0 as u64,
            );
            cur_time += idle_dur;
            ing.update_active_series_metrics(cur_time);

            let f = reg.gather();
            assert_eq!(
                metric_value_map(&f, "cortex_ingester_active_series"),
                BTreeMap::from([(vec![("user".into(), user1.into())], 12.0)])
            );
            assert_eq!(
                metric_value_map(&f, "cortex_ingester_active_otlp_series"),
                BTreeMap::from([(vec![("user".into(), user1.into())], 4.0)])
            );
            assert_eq!(
                metric_value_map(&f, "cortex_ingester_active_series_custom_tracker"),
                BTreeMap::from([
                    (
                        vec![("name".into(), "team_a".into()), ("user".into(), user1.into())],
                        6.0
                    ),
                    (
                        vec![("name".into(), "team_b".into()), ("user".into(), user1.into())],
                        6.0
                    ),
                ])
            );

            cur_time += idle_dur;
            ing.update_active_series_metrics(cur_time);
            let f = reg.gather();
            for metric in [
                "cortex_ingester_active_series",
                "cortex_ingester_active_otlp_series",
                "cortex_ingester_active_series_custom_tracker",
                "cortex_ingester_active_native_histogram_series",
                "cortex_ingester_active_native_histogram_series_custom_tracker",
                "cortex_ingester_active_native_histogram_buckets",
                "cortex_ingester_active_native_histogram_buckets_custom_tracker",
            ] {
                assert!(
                    metric_value_map(&f, metric).is_empty(),
                    "expected {metric} empty after idle expiry"
                );
            }
            ing.stop().await.unwrap();
        }

        // Subtest 4: successful push, active series disabled
        {
            let (_tmp, reg, ing) = make_ingester(true).await;
            push_all(ing.clone()).await;
            ing.update_active_series_metrics(SystemTime::now());
            let f = reg.gather();
            assert!(metric_value_map(&f, "cortex_ingester_active_series").is_empty());

            let resp = ing
                .handle_active_series(
                    &MetadataMap::new(),
                    user1,
                    ingesterpb::ActiveSeriesRequest {
                        matchers: vec![ingesterpb::LabelMatcher {
                            r#type: ingesterpb::MatchType::Equal as i32,
                            name: "team".into(),
                            value: "a".into(),
                        }],
                        r#type: ingesterpb::active_series_request::RequestType::Series as i32,
                    },
                )
                .await
                .unwrap();
            assert!(resp.is_empty());
            ing.stop().await.unwrap();
        }

        // Subtest 5: active series for cardinality API
        {
            let (_tmp, _reg, ing) = make_ingester(false).await;
            let ts = now_ms();
            for uid in [user1, user2] {
                for l in &labels_to_push {
                    ing.push_grpc(
                        uid,
                        &req_float(l, ts, cortexpb::write_request::SourceEnum::Api),
                    )
                    .await
                    .unwrap();
                }
                for l in &labels_to_push_hist {
                    ing.push_grpc(uid, &req_hist(l, ts)).await.unwrap();
                }
            }
            ing.update_active_series_metrics(SystemTime::now());

            let matchers = vec![
                ingesterpb::LabelMatcher {
                    r#type: ingesterpb::MatchType::Equal as i32,
                    name: "team".into(),
                    value: "a".into(),
                },
                ingesterpb::LabelMatcher {
                    r#type: ingesterpb::MatchType::Equal as i32,
                    name: "bool".into(),
                    value: "true".into(),
                },
            ];
            let batches = ing
                .handle_active_series(
                    &MetadataMap::new(),
                    user1,
                    ingesterpb::ActiveSeriesRequest {
                        matchers: matchers.clone(),
                        r#type: ingesterpb::active_series_request::RequestType::Series as i32,
                    },
                )
                .await
                .unwrap();
            let metrics: Vec<_> = batches.into_iter().flat_map(|b| b.metric).collect();
            assert_eq!(metrics.len(), 2);
            for m in &metrics {
                assert!(m
                    .labels
                    .iter()
                    .any(|lp| lp.name == "team" && lp.value == "a"));
            }

            // Sharded queries
            let shard1 = ing
                .handle_active_series(
                    &MetadataMap::new(),
                    user1,
                    ingesterpb::ActiveSeriesRequest {
                        matchers: vec![
                            ingesterpb::LabelMatcher {
                                r#type: ingesterpb::MatchType::Equal as i32,
                                name: METRIC_NAME_LABEL.into(),
                                value: "test_metric".into(),
                            },
                            ingesterpb::LabelMatcher {
                                r#type: ingesterpb::MatchType::Equal as i32,
                                name: "__query_shard__".into(),
                                value: "1_of_2".into(),
                            },
                        ],
                        r#type: ingesterpb::active_series_request::RequestType::Series as i32,
                    },
                )
                .await
                .unwrap();
            let shard1_count: usize = shard1.iter().map(|b| b.metric.len()).sum();

            let shard2 = ing
                .handle_active_series(
                    &MetadataMap::new(),
                    user1,
                    ingesterpb::ActiveSeriesRequest {
                        matchers: vec![
                            ingesterpb::LabelMatcher {
                                r#type: ingesterpb::MatchType::Equal as i32,
                                name: METRIC_NAME_LABEL.into(),
                                value: "test_metric".into(),
                            },
                            ingesterpb::LabelMatcher {
                                r#type: ingesterpb::MatchType::Equal as i32,
                                name: "__query_shard__".into(),
                                value: "2_of_2".into(),
                            },
                        ],
                        r#type: ingesterpb::active_series_request::RequestType::Series as i32,
                    },
                )
                .await
                .unwrap();
            let shard2_count: usize = shard2.iter().map(|b| b.metric.len()).sum();
            assert_eq!(shard1_count + shard2_count, 4);

            // Fast-forward past idle timeout
            let idle_dur = Duration::from_nanos(
                ing.cfg.active_series.active_series_metrics_idle_timeout.0 as u64,
            );
            ing.update_active_series_metrics(SystemTime::now() + idle_dur);
            let stale = ing
                .handle_active_series(
                    &MetadataMap::new(),
                    user1,
                    ingesterpb::ActiveSeriesRequest {
                        matchers,
                        r#type: ingesterpb::active_series_request::RequestType::Series as i32,
                    },
                )
                .await
                .unwrap();
            assert!(stale.is_empty());
            ing.stop().await.unwrap();
        }
    });
}

// Upstream: pkg/ingester/ingester_test.go::TestIngesterActiveSeriesConfigChanges
#[test]
fn test_ingester_active_series_config_changes() {
    let labels_to_push = vec![
        vec![
            (METRIC_NAME_LABEL, "test_metric"),
            ("bool", "false"),
            ("team", "a"),
        ],
        vec![
            (METRIC_NAME_LABEL, "test_metric"),
            ("bool", "false"),
            ("team", "b"),
        ],
        vec![
            (METRIC_NAME_LABEL, "test_metric"),
            ("bool", "true"),
            ("team", "a"),
        ],
        vec![
            (METRIC_NAME_LABEL, "test_metric"),
            ("bool", "true"),
            ("team", "b"),
        ],
    ];
    let labels_to_push_hist = vec![
        vec![
            (METRIC_NAME_LABEL, "test_histogram_metric"),
            ("bool", "false"),
            ("team", "a"),
        ],
        vec![
            (METRIC_NAME_LABEL, "test_histogram_metric"),
            ("bool", "false"),
            ("team", "b"),
        ],
        vec![
            (METRIC_NAME_LABEL, "test_histogram_metric"),
            ("bool", "true"),
            ("team", "a"),
        ],
        vec![
            (METRIC_NAME_LABEL, "test_histogram_metric"),
            ("bool", "true"),
            ("team", "b"),
        ],
    ];

    let req_float = |lbls: &[(&str, &str)], ts: i64| {
        to_write_request_single_series(
            lbls,
            vec![cortexpb::Sample {
                timestamp_ms: ts,
                value: 1.0,
            }],
            cortexpb::write_request::SourceEnum::Api,
        )
    };
    let req_hist = |lbls: &[(&str, &str)], ts: i64| cortexpb::WriteRequest {
        timeseries: vec![cortexpb::TimeSeries {
            labels: lbls
                .iter()
                .map(|(k, v)| cortexpb::LabelPair::from_strs(*k, *v))
                .collect(),
            histograms: vec![float_histogram_to_proto(
                ts,
                &generate_test_float_histogram(1),
            )],
            ..Default::default()
        }],
        source: cortexpb::write_request::SourceEnum::Api as i32,
        ..Default::default()
    };

    let user1 = "test_user";
    let user2 = "other_test_user";
    let default_cfg = build_active_series_config(&[
        ("bool_is_true_flagbased", r#"{bool="true"}"#),
        ("bool_is_false_flagbased", r#"{bool="false"}"#),
    ]);
    let tenant_cfg =
        build_active_series_config(&[("team_a", r#"{team="a"}"#), ("team_b", r#"{team="b"}"#)]);

    block_on_for_tests(async {
        // Subtest 1: override flag based config with runtime overwrite
        {
            let tmp = tempfile::tempdir().unwrap();
            let reg = Registry::new();
            let (mut cfg, blocks) = default_ingester_test_config(tmp.path());
            cfg.active_series.active_series_metrics_enabled = true;
            let mut limits = default_limits_test_config();
            limits.active_series_custom_trackers = default_cfg.clone();
            limits.native_histograms_ingestion_enabled = true;
            let overrides = Overrides::new(limits);
            let ing = prepare_ingester_with_blocks_storage(
                cfg,
                blocks,
                overrides.clone(),
                None,
                Some(&reg),
            )
            .await;

            let ts = now_ms();
            for uid in [user1, user2] {
                for l in &labels_to_push {
                    ing.push_grpc(uid, &req_float(l, ts)).await.unwrap();
                }
                for l in &labels_to_push_hist {
                    ing.push_grpc(uid, &req_hist(l, ts)).await.unwrap();
                }
            }
            ing.update_active_series_metrics(SystemTime::now());
            let f = reg.gather();
            assert_eq!(
                metric_value_map(&f, "cortex_ingester_active_series_custom_tracker"),
                BTreeMap::from([
                    (
                        vec![
                            ("name".into(), "bool_is_false_flagbased".into()),
                            ("user".into(), user2.into())
                        ],
                        4.0
                    ),
                    (
                        vec![
                            ("name".into(), "bool_is_true_flagbased".into()),
                            ("user".into(), user2.into())
                        ],
                        4.0
                    ),
                    (
                        vec![
                            ("name".into(), "bool_is_false_flagbased".into()),
                            ("user".into(), user1.into())
                        ],
                        4.0
                    ),
                    (
                        vec![
                            ("name".into(), "bool_is_true_flagbased".into()),
                            ("user".into(), user1.into())
                        ],
                        4.0
                    ),
                ])
            );

            let mut t_lim = default_limits_test_config();
            t_lim.active_series_custom_trackers = tenant_cfg.clone();
            t_lim.native_histograms_ingestion_enabled = true;
            overrides.set_static_override(user1, Some(t_lim));
            ing.update_active_series_metrics(SystemTime::now());
            let f = reg.gather();
            assert_eq!(
                metric_value_map(&f, "cortex_ingester_active_series_custom_tracker"),
                BTreeMap::from([
                    (
                        vec![
                            ("name".into(), "bool_is_false_flagbased".into()),
                            ("user".into(), user2.into())
                        ],
                        4.0
                    ),
                    (
                        vec![
                            ("name".into(), "bool_is_true_flagbased".into()),
                            ("user".into(), user2.into())
                        ],
                        4.0
                    ),
                    (
                        vec![("name".into(), "team_a".into()), ("user".into(), user1.into())],
                        4.0
                    ),
                    (
                        vec![("name".into(), "team_b".into()), ("user".into(), user1.into())],
                        4.0
                    ),
                ])
            );
            ing.stop().await.unwrap();
        }

        // Subtest 2: remove runtime overwrite and revert to flag based config
        {
            let tmp = tempfile::tempdir().unwrap();
            let reg = Registry::new();
            let (mut cfg, blocks) = default_ingester_test_config(tmp.path());
            cfg.active_series.active_series_metrics_enabled = true;
            let mut limits = default_limits_test_config();
            limits.active_series_custom_trackers = default_cfg.clone();
            limits.native_histograms_ingestion_enabled = true;
            let mut t_lim = limits.clone();
            t_lim.active_series_custom_trackers = tenant_cfg.clone();
            let overrides =
                Overrides::with_static_map(limits, HashMap::from([(user1.to_string(), t_lim)]));
            let ing = prepare_ingester_with_blocks_storage(
                cfg,
                blocks,
                overrides.clone(),
                None,
                Some(&reg),
            )
            .await;

            let ts = now_ms();
            for uid in [user1, user2] {
                for l in &labels_to_push {
                    ing.push_grpc(uid, &req_float(l, ts)).await.unwrap();
                }
                for l in &labels_to_push_hist {
                    ing.push_grpc(uid, &req_hist(l, ts)).await.unwrap();
                }
            }
            ing.update_active_series_metrics(SystemTime::now());
            assert_eq!(
                metric_value_map(&reg.gather(), "cortex_ingester_active_series_custom_tracker")
                    .get(&vec![
                        ("name".into(), "team_a".into()),
                        ("user".into(), user1.into())
                    ])
                    .copied(),
                Some(4.0)
            );

            overrides.clear_static_overrides();
            ing.update_active_series_metrics(SystemTime::now());
            let f = reg.gather();
            assert_eq!(
                metric_value_map(&f, "cortex_ingester_active_series_custom_tracker"),
                BTreeMap::from([
                    (
                        vec![
                            ("name".into(), "bool_is_false_flagbased".into()),
                            ("user".into(), user2.into())
                        ],
                        4.0
                    ),
                    (
                        vec![
                            ("name".into(), "bool_is_true_flagbased".into()),
                            ("user".into(), user2.into())
                        ],
                        4.0
                    ),
                    (
                        vec![
                            ("name".into(), "bool_is_false_flagbased".into()),
                            ("user".into(), user1.into())
                        ],
                        4.0
                    ),
                    (
                        vec![
                            ("name".into(), "bool_is_true_flagbased".into()),
                            ("user".into(), user1.into())
                        ],
                        4.0
                    ),
                ])
            );
            ing.stop().await.unwrap();
        }

        // Subtest 3: changing runtime override should result in new metrics
        {
            let tmp = tempfile::tempdir().unwrap();
            let reg = Registry::new();
            let (mut cfg, blocks) = default_ingester_test_config(tmp.path());
            cfg.active_series.active_series_metrics_enabled = true;
            let mut limits = default_limits_test_config();
            limits.active_series_custom_trackers = default_cfg.clone();
            limits.native_histograms_ingestion_enabled = true;
            let overrides = Overrides::new(limits);
            let ing = prepare_ingester_with_blocks_storage(
                cfg,
                blocks,
                overrides.clone(),
                None,
                Some(&reg),
            )
            .await;

            let ts = now_ms();
            for l in &labels_to_push {
                ing.push_grpc(user1, &req_float(l, ts)).await.unwrap();
            }
            for l in &labels_to_push_hist {
                ing.push_grpc(user1, &req_hist(l, ts)).await.unwrap();
            }
            ing.update_active_series_metrics(SystemTime::now());

            let mut t_lim = default_limits_test_config();
            t_lim.active_series_custom_trackers = build_active_series_config(&[
                ("team_a", r#"{team="a"}"#),
                ("team_b", r#"{team="b"}"#),
                ("team_c", r#"{team="b"}"#),
                ("team_d", r#"{team="b"}"#),
            ]);
            t_lim.native_histograms_ingestion_enabled = true;
            overrides.set_static_override(user1, Some(t_lim));
            ing.update_active_series_metrics(SystemTime::now());
            let f = reg.gather();
            assert_eq!(
                metric_value_map(&f, "cortex_ingester_active_series_custom_tracker"),
                BTreeMap::from([
                    (
                        vec![("name".into(), "team_a".into()), ("user".into(), user1.into())],
                        4.0
                    ),
                    (
                        vec![("name".into(), "team_b".into()), ("user".into(), user1.into())],
                        4.0
                    ),
                    (
                        vec![("name".into(), "team_c".into()), ("user".into(), user1.into())],
                        4.0
                    ),
                    (
                        vec![("name".into(), "team_d".into()), ("user".into(), user1.into())],
                        4.0
                    ),
                ])
            );
            ing.stop().await.unwrap();
        }

        // Subtest 4: should cleanup loading metric at close
        {
            let tmp = tempfile::tempdir().unwrap();
            let reg = Registry::new();
            let (mut cfg, blocks) = default_ingester_test_config(tmp.path());
            cfg.active_series.active_series_metrics_enabled = true;
            let mut limits = default_limits_test_config();
            limits.active_series_custom_trackers = default_cfg;
            limits.native_histograms_ingestion_enabled = true;
            let mut t_lim = limits.clone();
            t_lim.active_series_custom_trackers = tenant_cfg;
            let overrides =
                Overrides::with_static_map(limits, HashMap::from([(user1.to_string(), t_lim)]));
            let ing = prepare_ingester_with_blocks_storage(
                cfg,
                blocks,
                overrides.clone(),
                None,
                Some(&reg),
            )
            .await;

            let ts = now_ms();
            for uid in [user1, user2] {
                for l in &labels_to_push {
                    ing.push_grpc(uid, &req_float(l, ts)).await.unwrap();
                }
                for l in &labels_to_push_hist {
                    ing.push_grpc(uid, &req_hist(l, ts)).await.unwrap();
                }
            }
            ing.update_active_series_metrics(SystemTime::now());

            overrides.clear_static_overrides();
            overrides.set_defaults(default_limits_test_config());
            ing.update_active_series_metrics(SystemTime::now());
            let f = reg.gather();
            assert!(metric_value_map(&f, "cortex_ingester_active_series_custom_tracker").is_empty());
            assert_eq!(
                metric_value_map(&f, "cortex_ingester_active_series"),
                BTreeMap::from([
                    (vec![("user".into(), user2.into())], 8.0),
                    (vec![("user".into(), user1.into())], 8.0),
                ])
            );
            ing.close_all_tsdbs().await;
            let f = reg.gather();
            assert!(metric_value_map(&f, "cortex_ingester_active_series").is_empty());
            ing.stop().await.unwrap();
        }
    });
}

// Upstream: pkg/ingester/ingester_test.go::TestGetIgnoreSeriesLimitForMetricNamesMap
#[test]
fn test_get_ignore_series_limit_for_metric_names_map() {
    assert_eq!(get_ignore_series_limit_for_metric_names_map(""), None);
    assert_eq!(get_ignore_series_limit_for_metric_names_map(", ,,,"), None);
    assert_eq!(
        get_ignore_series_limit_for_metric_names_map("foo, bar, ,"),
        Some(["foo".to_string(), "bar".to_string()].into_iter().collect())
    );
}

enum SampleScenario {
    Float,
    IntHistogram,
    FloatHistogram,
}

impl SampleScenario {
    fn all() -> [Self; 3] {
        [Self::Float, Self::IntHistogram, Self::FloatHistogram]
    }

    fn make_write_request(&self, start_min: i64, end_min: i64, labels: &[(&str, &str)]) -> cortexpb::WriteRequest {
        let mut ts = cortexpb::TimeSeries {
            labels: labels
                .iter()
                .map(|(k, v)| cortexpb::LabelPair::from_strs(*k, *v))
                .collect(),
            ..Default::default()
        };
        for m in start_min..=end_min {
            let t = m * 60_000;
            match self {
                Self::Float => ts.samples.push(cortexpb::Sample {
                    timestamp_ms: t,
                    value: t as f64,
                }),
                Self::IntHistogram => ts
                    .histograms
                    .push(int_histogram_to_proto(t, &generate_test_histogram(m))),
                Self::FloatHistogram => ts
                    .histograms
                    .push(float_histogram_to_proto(t, &generate_test_float_histogram(m))),
            }
        }
        cortexpb::WriteRequest {
            timeseries: vec![ts],
            source: cortexpb::write_request::SourceEnum::Api as i32,
            ..Default::default()
        }
    }

    fn make_expected_series(&self, start_min: i64, end_min: i64, labels: BTreeMap<String, String>) -> DecodedSeries {
        let mut floats = Vec::new();
        let mut histograms = Vec::new();
        for m in start_min..=end_min {
            let t = m * 60_000;
            match self {
                Self::Float => floats.push((t, t as f64)),
                Self::IntHistogram | Self::FloatHistogram => {
                    histograms.push((t, generate_test_float_histogram(m)));
                }
            }
        }
        DecodedSeries {
            labels,
            floats,
            histograms,
        }
    }
}

// Upstream: pkg/ingester/ingester_test.go::Test_Ingester_OutOfOrder
#[test]
fn test_ingester_out_of_order() {
    block_on_for_tests(async {
        for scenario in SampleScenario::all() {
            let tmp = tempfile::tempdir().unwrap();
            let reg = Registry::new();
            let (mut cfg, blocks) = default_ingester_test_config(tmp.path());
            cfg.tsdb_config_update_period = GoDuration::from_millis(100);

            let mut limits = default_limits_test_config();
            limits.native_histograms_ingestion_enabled = true;
            let overrides = Overrides::new(limits);

            let ing = prepare_ingester_with_blocks_storage(
                cfg,
                blocks,
                overrides.clone(),
                None,
                Some(&reg),
            )
            .await;

            let user = "test";
            let lbls = [(METRIC_NAME_LABEL, "test_1"), ("status", "200")];
            let exp_lbls = BTreeMap::from([
                (METRIC_NAME_LABEL.into(), "test_1".into()),
                ("status".into(), "200".into()),
            ]);

            let set_ooo = |ooo_secs: u64| {
                let mut l = default_limits_test_config();
                l.native_histograms_ingestion_enabled = true;
                l.out_of_order_time_window = ModelDuration::from_secs(ooo_secs as i64);
                overrides.set_static_override(user, Some(l));
            };

            // Push first in-order sample at minute 100.
            ing.push_grpc(user, &scenario.make_write_request(100, 100, &lbls))
                .await
                .unwrap();
            let got = run_test_query(
                &ing,
                user,
                ingesterpb::MatchType::Equal,
                METRIC_NAME_LABEL,
                "test_1",
            )
            .await;
            assert_eq!(got, vec![scenario.make_expected_series(100, 100, exp_lbls.clone())]);

            // OOO disabled -> rejected.
            let err = ing
                .push_grpc(user, &scenario.make_write_request(90, 99, &lbls))
                .await
                .unwrap_err();
            assert!(
                err.to_string().contains(
                    "the sample has been rejected because another sample with a more recent timestamp has already been ingested and out-of-order samples are not allowed"
                ),
                "unexpected error: {err}"
            );

            let stats = ing.update_usage_stats();
            assert_eq!(stats.tenants_with_out_of_order_enabled, 0);
            assert_eq!(stats.min_out_of_order_time_window_seconds, 0);
            assert_eq!(stats.max_out_of_order_time_window_seconds, 0);

            let families = reg.gather();
            assert_eq!(
                metric_value_map(&families, "cortex_ingester_tsdb_out_of_order_samples_appended_total"),
                BTreeMap::from([(vec![("user".into(), user.into())], 0.0)])
            );
            let delta_mf =
                find_family(&families, "cortex_ingester_tsdb_sample_out_of_order_delta_seconds")
                    .unwrap();
            let delta_h = delta_mf.get_metric()[0].get_histogram();
            assert_eq!(delta_h.get_sample_count(), 10);
            assert!((delta_h.get_sample_sum() - 3300.0).abs() < 1e-6);

            // Enable 30m OOO window.
            set_ooo(30 * 60);
            tokio::time::sleep(Duration::from_millis(250)).await;

            ing.push_grpc(user, &scenario.make_write_request(90, 99, &lbls))
                .await
                .unwrap();
            let got = run_test_query(
                &ing,
                user,
                ingesterpb::MatchType::Equal,
                METRIC_NAME_LABEL,
                "test_1",
            )
            .await;
            assert_eq!(got, vec![scenario.make_expected_series(90, 100, exp_lbls.clone())]);

            let families = reg.gather();
            assert_eq!(
                metric_value_map(&families, "cortex_ingester_tsdb_out_of_order_samples_appended_total"),
                BTreeMap::from([(vec![("user".into(), user.into())], 10.0)])
            );
            let delta_h =
                find_family(&families, "cortex_ingester_tsdb_sample_out_of_order_delta_seconds")
                    .unwrap()
                    .get_metric()[0]
                    .get_histogram();
            assert_eq!(delta_h.get_sample_count(), 20);
            assert!((delta_h.get_sample_sum() - 6600.0).abs() < 1e-6);

            // 69m is beyond 30m window; 70..99 accepted (70..89 new, 90..99 dupes).
            let err = ing
                .push_grpc(user, &scenario.make_write_request(69, 99, &lbls))
                .await
                .unwrap_err();
            assert!(
                err.to_string().contains("beyond the out-of-order time window"),
                "unexpected error: {err}"
            );
            let got = run_test_query(
                &ing,
                user,
                ingesterpb::MatchType::Equal,
                METRIC_NAME_LABEL,
                "test_1",
            )
            .await;
            assert_eq!(got, vec![scenario.make_expected_series(70, 100, exp_lbls.clone())]);

            let families = reg.gather();
            assert_eq!(
                metric_value_map(&families, "cortex_ingester_tsdb_out_of_order_samples_appended_total"),
                BTreeMap::from([(vec![("user".into(), user.into())], 30.0)])
            );

            // 50..69 all beyond 30m window.
            let err = ing
                .push_grpc(user, &scenario.make_write_request(50, 69, &lbls))
                .await
                .unwrap_err();
            assert!(err.to_string().contains("beyond the out-of-order time window"));

            let stats = ing.update_usage_stats();
            assert_eq!(stats.tenants_with_out_of_order_enabled, 1);
            assert_eq!(stats.min_out_of_order_time_window_seconds, 30 * 60);
            assert_eq!(stats.max_out_of_order_time_window_seconds, 30 * 60);

            // Increase window to 60m.
            set_ooo(60 * 60);
            tokio::time::sleep(Duration::from_millis(250)).await;
            ing.push_grpc(user, &scenario.make_write_request(50, 69, &lbls))
                .await
                .unwrap();
            let got = run_test_query(
                &ing,
                user,
                ingesterpb::MatchType::Equal,
                METRIC_NAME_LABEL,
                "test_1",
            )
            .await;
            assert_eq!(got, vec![scenario.make_expected_series(50, 100, exp_lbls.clone())]);
            let families = reg.gather();
            assert_eq!(
                metric_value_map(&families, "cortex_ingester_tsdb_out_of_order_samples_appended_total"),
                BTreeMap::from([(vec![("user".into(), user.into())], 50.0)])
            );

            let stats = ing.update_usage_stats();
            assert_eq!(stats.tenants_with_out_of_order_enabled, 1);
            assert_eq!(stats.min_out_of_order_time_window_seconds, 60 * 60);
            assert_eq!(stats.max_out_of_order_time_window_seconds, 60 * 60);

            // Decrease back to 30m.
            set_ooo(30 * 60);
            tokio::time::sleep(Duration::from_millis(250)).await;
            let err = ing
                .push_grpc(user, &scenario.make_write_request(50, 69, &lbls))
                .await
                .unwrap_err();
            assert!(err.to_string().contains("beyond the out-of-order time window"));

            let stats = ing.update_usage_stats();
            assert_eq!(stats.tenants_with_out_of_order_enabled, 1);
            assert_eq!(stats.min_out_of_order_time_window_seconds, 30 * 60);
            assert_eq!(stats.max_out_of_order_time_window_seconds, 30 * 60);

            ing.stop().await.unwrap();
        }
    });
}

// Upstream: pkg/ingester/ingester_test.go::Test_Ingester_OutOfOrder_CompactHead
#[test]
fn test_ingester_out_of_order_compact_head() {
    block_on_for_tests(async {
        for scenario in SampleScenario::all() {
            let tmp = tempfile::tempdir().unwrap();
            let (mut cfg, mut blocks) = default_ingester_test_config(tmp.path());
            blocks.tsdb.head_compaction_interval = GoDuration::from_hours(1);
            blocks.tsdb.head_compaction_idle_timeout = GoDuration::from_millis(200);
            cfg.tsdb_config_update_period = GoDuration::from_secs(1);

            let mut tenant_limits = default_limits_test_config();
            tenant_limits.out_of_order_time_window = ModelDuration::from_secs(30 * 60);
            tenant_limits.native_histograms_ingestion_enabled = true;
            let overrides = Overrides::with_static_map(
                default_limits_test_config(),
                HashMap::from([(USER_ID.to_string(), tenant_limits)]),
            );

            let ing = prepare_ingester_with_blocks_storage(cfg, blocks, overrides, None, None).await;
            let lbls = [(METRIC_NAME_LABEL, "test_1"), ("status", "200")];
            let exp_lbls = BTreeMap::from([
                (METRIC_NAME_LABEL.into(), "test_1".into()),
                ("status".into(), "200".into()),
            ]);

            ing.push_grpc(USER_ID, &scenario.make_write_request(100, 100, &lbls))
                .await
                .unwrap();
            ing.push_grpc(USER_ID, &scenario.make_write_request(90, 99, &lbls))
                .await
                .unwrap();
            assert_eq!(
                run_test_query(
                    &ing,
                    USER_ID,
                    ingesterpb::MatchType::Equal,
                    METRIC_NAME_LABEL,
                    "test_1"
                )
                .await,
                vec![scenario.make_expected_series(90, 100, exp_lbls.clone())]
            );

            // Wait past idle timeout (+ 25% max jitter).
            tokio::time::sleep(Duration::from_millis(300)).await;
            ing.compact_blocks(false, 0, None).await;
            let db = ing.get_tsdb(USER_ID).unwrap();
            assert_eq!(db.db.head().num_series(), 0);
            assert!(!db.db.blocks().is_empty());

            ing.push_grpc(USER_ID, &scenario.make_write_request(110, 110, &lbls))
                .await
                .unwrap();
            ing.push_grpc(USER_ID, &scenario.make_write_request(101, 109, &lbls))
                .await
                .unwrap();
            assert_eq!(
                run_test_query(
                    &ing,
                    USER_ID,
                    ingesterpb::MatchType::Equal,
                    METRIC_NAME_LABEL,
                    "test_1"
                )
                .await,
                vec![scenario.make_expected_series(90, 110, exp_lbls)]
            );
            ing.compact_blocks(true, i64::MAX, None).await;
            assert_eq!(db.db.head().num_series(), 0);
            assert!(db.db.blocks().len() >= 2);
            ing.stop().await.unwrap();
        }
    });
}

// Upstream: pkg/ingester/ingester_test.go::Test_Ingester_OutOfOrder_CompactHead_StillActive
#[test]
fn test_ingester_out_of_order_compact_head_still_active() {
    block_on_for_tests(async {
        for scenario in SampleScenario::all() {
            let tmp = tempfile::tempdir().unwrap();
            let (mut cfg, blocks) = default_ingester_test_config(tmp.path());
            cfg.tsdb_config_update_period = GoDuration::from_secs(1);

            let mut tenant_limits = default_limits_test_config();
            tenant_limits.out_of_order_time_window = ModelDuration::from_hours(10);
            tenant_limits.native_histograms_ingestion_enabled = true;
            let overrides = Overrides::with_static_map(
                default_limits_test_config(),
                HashMap::from([(USER_ID.to_string(), tenant_limits)]),
            );

            let ing = prepare_ingester_with_blocks_storage(cfg, blocks, overrides, None, None).await;

            let push_one = |ts_min: i64, series_val: &str| {
                scenario.make_write_request(
                    ts_min,
                    ts_min,
                    &[(METRIC_NAME_LABEL, "test_1"), ("series", series_val)],
                )
            };

            ing.push_grpc(USER_ID, &push_one(1, "series")).await.unwrap();
            ing.push_grpc(USER_ID, &push_one(240, "series"))
                .await
                .unwrap();
            ing.push_grpc(USER_ID, &push_one(0, "ooo_series"))
                .await
                .unwrap();
            ing.push_grpc(USER_ID, &push_one(0, "ooo_series_not_pushed_again"))
                .await
                .unwrap();

            let db = ing.get_tsdb(USER_ID).unwrap();
            assert_eq!(db.db.head().num_series(), 3);
            assert_eq!(db.active_series.active().0, 3);

            ing.compact_blocks(false, 0, None).await;
            assert_eq!(db.db.head().num_series(), 1);
            assert_eq!(db.active_series.active().0, 3);

            ing.push_grpc(USER_ID, &push_one(480, "series"))
                .await
                .unwrap();
            ing.push_grpc(USER_ID, &push_one(240, "ooo_series"))
                .await
                .unwrap();

            ing.compact_blocks(false, i64::MAX, None).await;
            assert_eq!(db.db.head().num_series(), 1);
            assert_eq!(db.active_series.active().0, 3);
            ing.stop().await.unwrap();
        }
    });
}

// Upstream: pkg/ingester/ingester_test.go::Test_Ingester_ShipperLabelsOutOfOrderBlocksOnUpload
#[test]
fn test_ingester_shipper_labels_out_of_order_blocks_on_upload() {
    block_on_for_tests(async {
        for add_ooo_label in [true, false] {
            let tenant = "test";
            let tmp_dir = tempfile::tempdir().unwrap();
            let bucket_dir = tempfile::tempdir().unwrap();

            let (mut cfg, mut blocks) = default_ingester_test_config(tmp_dir.path());
            cfg.tsdb_config_update_period = GoDuration::from_secs(1);
            blocks.tsdb.ship_interval = GoDuration::from_hours(1);
            blocks.bucket.backend_cfg.backend = "filesystem".to_string();
            blocks.bucket.backend_cfg.filesystem.dir = bucket_dir.path().to_string_lossy().into_owned();

            let mut tenant_limits = default_limits_test_config();
            tenant_limits.out_of_order_time_window = ModelDuration::from_secs(30 * 60);
            tenant_limits.out_of_order_blocks_external_label_enabled = add_ooo_label;
            let overrides = Overrides::with_static_map(
                default_limits_test_config(),
                HashMap::from([(tenant.to_string(), tenant_limits)]),
            );

            let ing = prepare_ingester_with_blocks_storage(cfg, blocks, overrides, None, None).await;
            let lbls = [(METRIC_NAME_LABEL, "test_1"), ("status", "200")];
            ing.push_grpc(
                tenant,
                &SampleScenario::Float.make_write_request(100, 100, &lbls),
            )
            .await
            .unwrap();
            ing.push_grpc(
                tenant,
                &SampleScenario::Float.make_write_request(90, 99, &lbls),
            )
            .await
            .unwrap();

            ing.compact_blocks(true, i64::MAX, None).await;
            ing.ship_blocks(None).await;

            let user_tsdb = ing.get_tsdb(tenant).unwrap();
            let shipped = user_tsdb.cached_shipped_blocks();
            assert_eq!(shipped.len(), 2, "there should be two uploaded blocks");

            let bucket = BucketClient::new_filesystem(bucket_dir.path().join(tenant)).unwrap();
            let mut ooo_metas = Vec::new();
            let mut in_order_metas = Vec::new();
            for ulid in shipped.keys() {
                let meta_bytes = bucket
                    .download(&format!("{ulid}/meta.json"))
                    .await
                    .expect("download meta.json");
                let meta: BlockMeta = serde_json::from_slice(&meta_bytes).expect("parse meta.json");
                if meta.compaction.from_out_of_order() {
                    ooo_metas.push(meta);
                } else {
                    in_order_metas.push(meta);
                }
            }

            assert_eq!(ooo_metas.len(), 1);
            assert_eq!(in_order_metas.len(), 1);
            assert!(in_order_metas[0]
                .thanos
                .as_ref()
                .map(|t| t.labels.is_empty())
                .unwrap_or(true));
            if add_ooo_label {
                assert_eq!(
                    ooo_metas[0].thanos.as_ref().unwrap().labels,
                    BTreeMap::from([(
                        OUT_OF_ORDER_EXTERNAL_LABEL.to_string(),
                        OUT_OF_ORDER_EXTERNAL_LABEL_VALUE.to_string()
                    )])
                );
            } else {
                assert!(ooo_metas[0]
                    .thanos
                    .as_ref()
                    .map(|t| t.labels.is_empty())
                    .unwrap_or(true));
            }
            ing.stop().await.unwrap();
        }
    });
}

// Upstream: pkg/ingester/ingester_test.go::TestIngesterCanEnableIngestAndQueryNativeHistograms
#[test]
fn test_ingester_can_enable_ingest_and_query_native_histograms() {
    let expected_fh = generate_test_float_histogram(0);
    let cases = [
        (
            "integer histogram",
            int_histogram_to_proto(1, &generate_test_histogram(0)),
        ),
        (
            "float histogram",
            float_histogram_to_proto(1, &generate_test_float_histogram(0)),
        ),
    ];

    block_on_for_tests(async {
        for (name, mut hist_proto) in cases {
            let tmp = tempfile::tempdir().unwrap();
            let reg = Registry::new();
            let (mut cfg, blocks) = default_ingester_test_config(tmp.path());
            cfg.tsdb_config_update_period = GoDuration::from_millis(100);
            cfg.ring.replication_factor = 1;

            let mut limits = default_limits_test_config();
            limits.native_histograms_ingestion_enabled = false;
            let overrides = Overrides::new(limits);

            let ing = prepare_ingester_with_blocks_storage(
                cfg,
                blocks,
                overrides.clone(),
                None,
                Some(&reg),
            )
            .await;

            let labels1 = [(METRIC_NAME_LABEL, "testmetric"), ("foo", "bar")];
            let metadata1 = cortexpb::MetricMetadata {
                metric_family_name: "testmetric".to_string(),
                help: "a help for testmetric".to_string(),
                unit: String::new(),
                r#type: cortexpb::metric_metadata::MetricType::Counter as i32,
            };
            let req = cortexpb::WriteRequest {
                timeseries: vec![cortexpb::TimeSeries {
                    labels: labels1
                        .iter()
                        .map(|(k, v)| cortexpb::LabelPair::from_strs(*k, *v))
                        .collect(),
                    samples: vec![cortexpb::Sample {
                        timestamp_ms: 0,
                        value: 1.0,
                    }],
                    histograms: vec![hist_proto.clone()],
                    ..Default::default()
                }],
                metadata: vec![metadata1],
                source: cortexpb::write_request::SourceEnum::Api as i32,
                ..Default::default()
            };
            ing.push_grpc(USER_ID, &req).await.unwrap();

            let f = reg.gather();
            assert_eq!(
                metric_value_map(&f, "cortex_ingester_ingested_samples_total"),
                BTreeMap::from([(vec![("user".into(), USER_ID.into())], 1.0)]),
                "{name}"
            );
            assert_eq!(
                metric_value_map(&f, "cortex_ingester_ingested_samples_failures_total"),
                BTreeMap::from([(vec![("user".into(), USER_ID.into())], 0.0)]),
                "{name}"
            );

            let exp_lbls = BTreeMap::from([
                (METRIC_NAME_LABEL.into(), "testmetric".into()),
                ("foo".into(), "bar".into()),
            ]);
            assert_eq!(
                run_test_query(
                    &ing,
                    USER_ID,
                    ingesterpb::MatchType::Equal,
                    METRIC_NAME_LABEL,
                    "testmetric"
                )
                .await,
                vec![DecodedSeries {
                    labels: exp_lbls.clone(),
                    floats: vec![(0, 1.0)],
                    histograms: vec![],
                }],
                "{name}: expected no histogram before enabling"
            );

            let mut enabled_lim = default_limits_test_config();
            enabled_lim.native_histograms_ingestion_enabled = true;
            overrides.set_static_override(USER_ID, Some(enabled_lim));
            tokio::time::sleep(Duration::from_millis(200)).await;

            hist_proto.timestamp = 2;
            let metadata2 = cortexpb::MetricMetadata {
                metric_family_name: "testmetric".to_string(),
                help: "a help for testmetric".to_string(),
                unit: String::new(),
                r#type: cortexpb::metric_metadata::MetricType::Histogram as i32,
            };
            let req2 = cortexpb::WriteRequest {
                timeseries: vec![cortexpb::TimeSeries {
                    labels: labels1
                        .iter()
                        .map(|(k, v)| cortexpb::LabelPair::from_strs(*k, *v))
                        .collect(),
                    histograms: vec![hist_proto],
                    ..Default::default()
                }],
                metadata: vec![metadata2],
                source: cortexpb::write_request::SourceEnum::Api as i32,
                ..Default::default()
            };
            ing.push_grpc(USER_ID, &req2).await.unwrap();

            let f = reg.gather();
            assert_eq!(
                metric_value_map(&f, "cortex_ingester_ingested_samples_total"),
                BTreeMap::from([(vec![("user".into(), USER_ID.into())], 2.0)]),
                "{name}"
            );
            assert_eq!(
                metric_value_map(&f, "cortex_ingester_ingested_samples_failures_total"),
                BTreeMap::from([(vec![("user".into(), USER_ID.into())], 0.0)]),
                "{name}"
            );

            let expected_full = vec![DecodedSeries {
                labels: exp_lbls,
                floats: vec![(0, 1.0)],
                histograms: vec![(2, expected_fh.clone())],
            }];
            assert_eq!(
                run_test_query(
                    &ing,
                    USER_ID,
                    ingesterpb::MatchType::Equal,
                    METRIC_NAME_LABEL,
                    "testmetric"
                )
                .await,
                expected_full,
                "{name}: expected histogram after enabling"
            );

            let mut disabled_lim = default_limits_test_config();
            disabled_lim.native_histograms_ingestion_enabled = false;
            overrides.set_static_override(USER_ID, Some(disabled_lim));
            tokio::time::sleep(Duration::from_millis(200)).await;

            assert_eq!(
                run_test_query(
                    &ing,
                    USER_ID,
                    ingesterpb::MatchType::Equal,
                    METRIC_NAME_LABEL,
                    "testmetric"
                )
                .await,
                expected_full,
                "{name}: expected histogram preserved after disabling"
            );
            ing.stop().await.unwrap();
        }
    });
}

// Upstream: pkg/ingester/ingester_test.go::TestIngester_GetOpenTSDBsConcurrencyConfig
#[test]
fn test_ingester_get_open_tsdbs_concurrency_config() {
    let cases = [
        (0, 5, 10, 0),
        (3, 5, 1, 3),
        (3, 15, 3, 1),
    ];
    for (wal_replay_concurrency, tenant_count, exp_open, exp_wal) in cases {
        let tsdb_cfg = TSDBConfig {
            wal_replay_concurrency,
            ..Default::default()
        };
        let (open_c, wal_c) = get_open_tsdbs_concurrency_config(tsdb_cfg.wal_replay_concurrency, tenant_count);
        assert_eq!(open_c, exp_open);
        assert_eq!(wal_c, exp_wal);
    }
}

// Upstream: pkg/ingester/ingester_test.go::TestIngester_PushWithSampledErrors
#[test]
fn test_ingester_push_with_sampled_errors() {
    let error_sample_rate = 5;
    let now = now_ms();
    let users = ["user-1", "user-2"];
    let metric_lbls = vec![(METRIC_NAME_LABEL, "test")];

    struct Case {
        name: &'static str,
        reqs: Vec<cortexpb::WriteRequest>,
        expected_err_substr: &'static str,
        expected_reason: Option<&'static str>,
        expected_counts: Option<(f64, f64)>,
        expected_sampling: bool,
        max_exemplars: i64,
        native_histograms: bool,
    }

    let cases = vec![
        Case {
            name: "should soft fail on sample out-of-order",
            reqs: vec![
                to_write_request(
                    std::slice::from_ref(&metric_lbls),
                    &[cortexpb::Sample {
                        value: 2.0,
                        timestamp_ms: 10,
                    }],
                    &[],
                    cortexpb::write_request::SourceEnum::Api,
                ),
                to_write_request(
                    std::slice::from_ref(&metric_lbls),
                    &[cortexpb::Sample {
                        value: 1.0,
                        timestamp_ms: 9,
                    }],
                    &[],
                    cortexpb::write_request::SourceEnum::Api,
                ),
            ],
            expected_err_substr: "err-mimir-sample-out-of-order",
            expected_reason: Some("sample-out-of-order"),
            expected_counts: Some((4.0, 1.0)),
            expected_sampling: true,
            max_exemplars: 0,
            native_histograms: false,
        },
        Case {
            name: "should soft fail on all samples out of bound in a write request",
            reqs: vec![
                to_write_request(
                    std::slice::from_ref(&metric_lbls),
                    &[cortexpb::Sample {
                        value: 2.0,
                        timestamp_ms: 1575043969,
                    }],
                    &[],
                    cortexpb::write_request::SourceEnum::Api,
                ),
                to_write_request_single_series(
                    &metric_lbls,
                    vec![
                        cortexpb::Sample {
                            value: 0.0,
                            timestamp_ms: 1575043969 - (86400 * 1000),
                        },
                        cortexpb::Sample {
                            value: 1.0,
                            timestamp_ms: 1575043969 - (86000 * 1000),
                        },
                    ],
                    cortexpb::write_request::SourceEnum::Api,
                ),
            ],
            expected_err_substr: "err-mimir-sample-timestamp-too-old",
            expected_reason: Some("sample-timestamp-too-old"),
            expected_counts: Some((8.0, 2.0)),
            expected_sampling: true,
            max_exemplars: 0,
            native_histograms: false,
        },
        Case {
            name: "should soft fail on all histograms out of bound in a write request",
            reqs: vec![
                to_write_request(
                    std::slice::from_ref(&metric_lbls),
                    &[cortexpb::Sample {
                        value: 2.0,
                        timestamp_ms: 1575043969,
                    }],
                    &[],
                    cortexpb::write_request::SourceEnum::Api,
                ),
                cortexpb::WriteRequest {
                    timeseries: vec![cortexpb::TimeSeries {
                        labels: vec![cortexpb::LabelPair::from_strs(METRIC_NAME_LABEL, "test")],
                        histograms: vec![int_histogram_to_proto(
                            1575043969 - (86800 * 1000),
                            &generate_test_histogram(0),
                        )],
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            ],
            expected_err_substr: "err-mimir-sample-timestamp-too-old",
            expected_reason: Some("sample-timestamp-too-old"),
            expected_counts: Some((4.0, 1.0)),
            expected_sampling: true,
            max_exemplars: 0,
            native_histograms: true,
        },
        Case {
            name: "should soft fail on all samples and histograms out of bound in a write request",
            reqs: vec![
                to_write_request(
                    std::slice::from_ref(&metric_lbls),
                    &[cortexpb::Sample {
                        value: 2.0,
                        timestamp_ms: 1575043969,
                    }],
                    &[],
                    cortexpb::write_request::SourceEnum::Api,
                ),
                cortexpb::WriteRequest {
                    timeseries: vec![cortexpb::TimeSeries {
                        labels: vec![cortexpb::LabelPair::from_strs(METRIC_NAME_LABEL, "test")],
                        samples: vec![
                            cortexpb::Sample {
                                value: 0.0,
                                timestamp_ms: 1575043969 - (86400 * 1000),
                            },
                            cortexpb::Sample {
                                value: 1.0,
                                timestamp_ms: 1575043969 - (86000 * 1000),
                            },
                        ],
                        histograms: vec![int_histogram_to_proto(
                            1575043969 - (86800 * 1000),
                            &generate_test_histogram(0),
                        )],
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            ],
            expected_err_substr: "err-mimir-sample-timestamp-too-old",
            expected_reason: Some("sample-timestamp-too-old"),
            expected_counts: Some((12.0, 3.0)),
            expected_sampling: true,
            max_exemplars: 0,
            native_histograms: true,
        },
        Case {
            name: "should soft fail on some samples out of bound in a write request",
            reqs: vec![
                to_write_request(
                    std::slice::from_ref(&metric_lbls),
                    &[cortexpb::Sample {
                        value: 2.0,
                        timestamp_ms: 1575043969,
                    }],
                    &[],
                    cortexpb::write_request::SourceEnum::Api,
                ),
                to_write_request_single_series(
                    &metric_lbls,
                    vec![
                        cortexpb::Sample {
                            value: 0.0,
                            timestamp_ms: 1575043969 - (86400 * 1000),
                        },
                        cortexpb::Sample {
                            value: 1.0,
                            timestamp_ms: 1575043969 - (86000 * 1000),
                        },
                        cortexpb::Sample {
                            value: 3.0,
                            timestamp_ms: 1575043969 + 1,
                        },
                    ],
                    cortexpb::write_request::SourceEnum::Api,
                ),
            ],
            expected_err_substr: "err-mimir-sample-timestamp-too-old",
            expected_reason: Some("sample-timestamp-too-old"),
            expected_counts: Some((8.0, 2.0)),
            expected_sampling: true,
            max_exemplars: 0,
            native_histograms: false,
        },
        Case {
            name: "should soft fail on some samples with timestamp too far in future in a write request",
            reqs: vec![
                to_write_request(
                    std::slice::from_ref(&metric_lbls),
                    &[cortexpb::Sample {
                        value: 1.0,
                        timestamp_ms: now,
                    }],
                    &[],
                    cortexpb::write_request::SourceEnum::Api,
                ),
                to_write_request_single_series(
                    &metric_lbls,
                    vec![
                        cortexpb::Sample {
                            value: 2.0,
                            timestamp_ms: now + (86400 * 1000),
                        },
                        cortexpb::Sample {
                            value: 3.0,
                            timestamp_ms: now + 1,
                        },
                    ],
                    cortexpb::write_request::SourceEnum::Api,
                ),
            ],
            expected_err_substr: "err-mimir-too-far-in-future",
            expected_reason: Some("sample-too-far-in-future"),
            expected_counts: Some((4.0, 1.0)),
            expected_sampling: true,
            max_exemplars: 0,
            native_histograms: false,
        },
        Case {
            name: "should soft fail on some histograms with timestamp too far in future in a write request",
            reqs: vec![cortexpb::WriteRequest {
                timeseries: vec![cortexpb::TimeSeries {
                    labels: vec![cortexpb::LabelPair::from_strs(METRIC_NAME_LABEL, "test")],
                    histograms: vec![
                        int_histogram_to_proto(now, &generate_test_histogram(0)),
                        int_histogram_to_proto(
                            now + (86400 * 1000),
                            &generate_test_histogram(1),
                        ),
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            expected_err_substr: "err-mimir-too-far-in-future",
            expected_reason: Some("sample-too-far-in-future"),
            expected_counts: Some((4.0, 1.0)),
            expected_sampling: true,
            max_exemplars: 0,
            native_histograms: true,
        },
        Case {
            name: "should soft fail on some exemplars with timestamp too far in future in a write request",
            reqs: vec![cortexpb::WriteRequest {
                timeseries: vec![cortexpb::TimeSeries {
                    labels: vec![cortexpb::LabelPair::from_strs(METRIC_NAME_LABEL, "test")],
                    samples: vec![cortexpb::Sample {
                        value: 1.0,
                        timestamp_ms: now,
                    }],
                    exemplars: vec![
                        cortexpb::Exemplar {
                            labels: vec![cortexpb::LabelPair::from_strs("traceID", "111")],
                            value: 1.0,
                            timestamp_ms: now,
                        },
                        cortexpb::Exemplar {
                            labels: vec![cortexpb::LabelPair::from_strs("traceID", "222")],
                            value: 2.0,
                            timestamp_ms: now + (86400 * 1000),
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            expected_err_substr: "err-mimir-exemplar-too-far-in-future",
            expected_reason: None,
            expected_counts: None,
            expected_sampling: false,
            max_exemplars: 1,
            native_histograms: false,
        },
        Case {
            name: "should soft fail on two different sample values at the same timestamp",
            reqs: vec![
                to_write_request(
                    std::slice::from_ref(&metric_lbls),
                    &[cortexpb::Sample {
                        value: 2.0,
                        timestamp_ms: 1575043969,
                    }],
                    &[],
                    cortexpb::write_request::SourceEnum::Api,
                ),
                to_write_request(
                    std::slice::from_ref(&metric_lbls),
                    &[cortexpb::Sample {
                        value: 1.0,
                        timestamp_ms: 1575043969,
                    }],
                    &[],
                    cortexpb::write_request::SourceEnum::Api,
                ),
            ],
            expected_err_substr: "err-mimir-sample-duplicate-timestamp",
            expected_reason: Some("new-value-for-timestamp"),
            expected_counts: Some((4.0, 1.0)),
            expected_sampling: true,
            max_exemplars: 0,
            native_histograms: false,
        },
        Case {
            name: "should soft fail on exemplar with unknown series",
            reqs: vec![cortexpb::WriteRequest {
                timeseries: vec![cortexpb::TimeSeries {
                    labels: vec![cortexpb::LabelPair::from_strs(METRIC_NAME_LABEL, "test")],
                    exemplars: vec![cortexpb::Exemplar {
                        labels: vec![cortexpb::LabelPair::from_strs("traceID", "123")],
                        timestamp_ms: 1000,
                        value: 1000.0,
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            expected_err_substr: "err-mimir-exemplar-series-missing",
            expected_reason: None,
            expected_counts: None,
            expected_sampling: false,
            max_exemplars: 1,
            native_histograms: false,
        },
        Case {
            name: "should soft fail on series with unsorted labels",
            reqs: vec![cortexpb::WriteRequest {
                timeseries: vec![cortexpb::TimeSeries {
                    labels: vec![
                        cortexpb::LabelPair::from_strs("zzz", "last"),
                        cortexpb::LabelPair::from_strs(METRIC_NAME_LABEL, "test"),
                    ],
                    samples: vec![cortexpb::Sample {
                        value: 1.0,
                        timestamp_ms: 9,
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            expected_err_substr: "err-mimir-labels-not-sorted",
            expected_reason: Some("labels-not-sorted"),
            expected_counts: Some((4.0, 1.0)),
            expected_sampling: true,
            max_exemplars: 0,
            native_histograms: false,
        },
        Case {
            name: "should soft fail on series with duplicate labels",
            reqs: vec![cortexpb::WriteRequest {
                timeseries: vec![cortexpb::TimeSeries {
                    labels: vec![
                        cortexpb::LabelPair::from_strs(METRIC_NAME_LABEL, "foo"),
                        cortexpb::LabelPair::from_strs(METRIC_NAME_LABEL, "bar"),
                    ],
                    samples: vec![cortexpb::Sample {
                        value: 1.0,
                        timestamp_ms: 9,
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            expected_err_substr: "err-mimir-labels-not-sorted",
            expected_reason: Some("labels-not-sorted"),
            expected_counts: Some((4.0, 1.0)),
            expected_sampling: true,
            max_exemplars: 0,
            native_histograms: false,
        },
        Case {
            name: "should soft fail on series with unsorted labels with native histogram",
            reqs: vec![cortexpb::WriteRequest {
                timeseries: vec![cortexpb::TimeSeries {
                    labels: vec![
                        cortexpb::LabelPair::from_strs("zzz", "last"),
                        cortexpb::LabelPair::from_strs(METRIC_NAME_LABEL, "test"),
                    ],
                    histograms: vec![int_histogram_to_proto(9, &generate_test_histogram(1))],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            expected_err_substr: "err-mimir-labels-not-sorted",
            expected_reason: Some("labels-not-sorted"),
            expected_counts: Some((4.0, 1.0)),
            expected_sampling: true,
            max_exemplars: 0,
            native_histograms: true,
        },
        Case {
            name: "should soft fail on series with duplicate labels with native histogram",
            reqs: vec![cortexpb::WriteRequest {
                timeseries: vec![cortexpb::TimeSeries {
                    labels: vec![
                        cortexpb::LabelPair::from_strs(METRIC_NAME_LABEL, "foo"),
                        cortexpb::LabelPair::from_strs(METRIC_NAME_LABEL, "bar"),
                    ],
                    histograms: vec![int_histogram_to_proto(9, &generate_test_histogram(1))],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            expected_err_substr: "err-mimir-labels-not-sorted",
            expected_reason: Some("labels-not-sorted"),
            expected_counts: Some((4.0, 1.0)),
            expected_sampling: true,
            max_exemplars: 0,
            native_histograms: true,
        },
    ];

    for case in cases {
        let log_buf = LogBuffer::new();
        let sub = CaptureSubscriber {
            buf: Arc::clone(&log_buf),
        };
        tracing::subscriber::with_default(sub, || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                let tmp = tempfile::tempdir().unwrap();
                let reg = Registry::new();
                let (mut cfg, blocks) = default_ingester_test_config(tmp.path());
                cfg.ring.replication_factor = 1;
                cfg.error_sample_rate = error_sample_rate;

                let mut limits = default_limits_test_config();
                limits.max_global_exemplars_per_user = case.max_exemplars;
                limits.native_histograms_ingestion_enabled = case.native_histograms;

                let ing = prepare_ingester_with_blocks_storage(
                    cfg,
                    blocks,
                    Overrides::new(limits),
                    None,
                    Some(&reg),
                )
                .await;

                let (mut client, srv_handle) = start_grpc_server_and_client(ing.clone()).await;

                for (idx, req) in case.reqs.iter().enumerate() {
                    if idx + 1 < case.reqs.len() {
                        for uid in users {
                            client
                                .push(grpc_req_for_user(uid, req.clone()))
                                .await
                                .unwrap_or_else(|e| panic!("{}: unexpected err: {e}", case.name));
                        }
                    } else {
                        for _ in 0..(error_sample_rate - 1) {
                            let status = client
                                .push(grpc_req_for_user(users[0], req.clone()))
                                .await
                                .unwrap_err();
                            assert_eq!(status.code(), Code::InvalidArgument, "{}", case.name);
                            assert!(
                                status.message().contains(case.expected_err_substr),
                                "{}: got {}",
                                case.name,
                                status.message()
                            );
                        }
                        let status = client
                            .push(grpc_req_for_user(users[1], req.clone()))
                            .await
                            .unwrap_err();
                        assert_eq!(status.code(), Code::InvalidArgument, "{}", case.name);
                        assert!(
                            status.message().contains(case.expected_err_substr),
                            "{}: got {}",
                            case.name,
                            status.message()
                        );
                    }
                }

                if case.expected_sampling {
                    assert_eq!(
                        log_buf.count("user=user-1"),
                        1,
                        "{}: expected 1 sampled log for user-1, logs: {:?}",
                        case.name,
                        *log_buf.lines.lock()
                    );
                    assert_eq!(
                        log_buf.count("user=user-2"),
                        0,
                        "{}: expected 0 sampled logs for user-2",
                        case.name
                    );
                } else {
                    assert_eq!(
                        log_buf.count("user=user-1"),
                        (error_sample_rate - 1) as usize,
                        "{}: expected all logs for user-1",
                        case.name
                    );
                    assert_eq!(
                        log_buf.count("user=user-2"),
                        1,
                        "{}: expected 1 log for user-2",
                        case.name
                    );
                }

                if let (Some(reason), Some((c0, c1))) = (case.expected_reason, case.expected_counts) {
                    let vals = metric_value_map(&reg.gather(), "cortex_discarded_samples_total");
                    assert_eq!(
                        vals.get(&vec![
                            ("group".into(), "".into()),
                            ("reason".into(), reason.into()),
                            ("user".into(), users[0].into()),
                        ])
                        .copied()
                        .unwrap_or(0.0),
                        c0,
                        "{}: user-1 discarded metric",
                        case.name
                    );
                    assert_eq!(
                        vals.get(&vec![
                            ("group".into(), "".into()),
                            ("reason".into(), reason.into()),
                            ("user".into(), users[1].into()),
                        ])
                        .copied()
                        .unwrap_or(0.0),
                        c1,
                        "{}: user-2 discarded metric",
                        case.name
                    );
                }

                srv_handle.abort();
                ing.stop().await.unwrap();
            });
        });
    }
}

// Upstream: pkg/ingester/ingester_test.go::TestIngester_SampledUserLimitExceeded
#[test]
fn test_ingester_sampled_user_limit_exceeded() {
    let error_sample_rate = 5i64;
    let log_buf = LogBuffer::new();
    let sub = CaptureSubscriber {
        buf: Arc::clone(&log_buf),
    };
    tracing::subscriber::with_default(sub, || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let tmp = tempfile::tempdir().unwrap();
            let reg = Registry::new();
            let (mut cfg, blocks) = default_ingester_test_config(tmp.path());
            cfg.ring.replication_factor = 1;
            cfg.error_sample_rate = error_sample_rate;

            let mut limits = default_limits_test_config();
            limits.max_global_series_per_user = 1;

            let ing = prepare_ingester_with_blocks_storage(
                cfg,
                blocks,
                Overrides::new(limits),
                None,
                Some(&reg),
            )
            .await;

            let (mut client, srv_handle) = start_grpc_server_and_client(ing.clone()).await;

            let ts = 1575043969i64;
            let l1 = vec![(METRIC_NAME_LABEL, "test"), ("foo", "bar")];
            let l2 = vec![(METRIC_NAME_LABEL, "test"), ("foo", "biz")];
            let s1 = cortexpb::Sample {
                timestamp_ms: ts + 1,
                value: 1.0,
            };
            let s2 = cortexpb::Sample {
                timestamp_ms: ts + 2,
                value: 2.0,
            };
            let s3 = cortexpb::Sample {
                timestamp_ms: ts + 3,
                value: 3.0,
            };

            client
                .push(grpc_req_for_user(
                    USER_ID,
                    to_write_request(
                        std::slice::from_ref(&l1),
                        &[s1],
                        &[],
                        cortexpb::write_request::SourceEnum::Api,
                    ),
                ))
                .await
                .unwrap();

            for _ in 0..(2 * error_sample_rate) {
                let status = client
                    .push(grpc_req_for_user(
                        USER_ID,
                        to_write_request(
                            &[l1.clone(), l2.clone()],
                            &[s2, s3],
                            &[],
                            cortexpb::write_request::SourceEnum::Api,
                        ),
                    ))
                    .await
                    .unwrap_err();
                assert_eq!(status.code(), Code::FailedPrecondition);
                assert!(status.message().contains("per-user series limit of 1 exceeded"));
            }

            assert_eq!(log_buf.count("per-user series limit of 1 exceeded"), 2);

            let vals = metric_value_map(&reg.gather(), "cortex_discarded_samples_total");
            assert_eq!(
                vals.get(&vec![
                    ("group".into(), "".into()),
                    ("reason".into(), "per_user_series_limit".into()),
                    ("user".into(), USER_ID.into()),
                ])
                .copied(),
                Some(10.0)
            );

            srv_handle.abort();
            ing.stop().await.unwrap();
        });
    });
}

// Upstream: pkg/ingester/ingester_test.go::TestIngester_SampledMetricLimitExceeded
#[test]
fn test_ingester_sampled_metric_limit_exceeded() {
    let error_sample_rate = 5i64;
    let log_buf = LogBuffer::new();
    let sub = CaptureSubscriber {
        buf: Arc::clone(&log_buf),
    };
    tracing::subscriber::with_default(sub, || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let tmp = tempfile::tempdir().unwrap();
            let reg = Registry::new();
            let (mut cfg, blocks) = default_ingester_test_config(tmp.path());
            cfg.ring.replication_factor = 1;
            cfg.error_sample_rate = error_sample_rate;

            let mut limits = default_limits_test_config();
            limits.max_global_series_per_metric = 1;
            limits.max_global_metadata_per_metric = 1;

            let ing = prepare_ingester_with_blocks_storage(
                cfg,
                blocks,
                Overrides::new(limits),
                None,
                Some(&reg),
            )
            .await;

            let (mut client, srv_handle) = start_grpc_server_and_client(ing.clone()).await;

            let ts = 1575043969i64;
            let l1 = vec![(METRIC_NAME_LABEL, "test"), ("foo", "bar")];
            let l2 = vec![(METRIC_NAME_LABEL, "test"), ("foo", "biz")];
            let s1 = cortexpb::Sample {
                timestamp_ms: ts + 1,
                value: 1.0,
            };
            let s2 = cortexpb::Sample {
                timestamp_ms: ts + 2,
                value: 2.0,
            };
            let s3 = cortexpb::Sample {
                timestamp_ms: ts + 3,
                value: 3.0,
            };

            client
                .push(grpc_req_for_user(
                    USER_ID,
                    to_write_request(
                        std::slice::from_ref(&l1),
                        &[s1],
                        &[],
                        cortexpb::write_request::SourceEnum::Api,
                    ),
                ))
                .await
                .unwrap();

            for _ in 0..(2 * error_sample_rate) {
                let status = client
                    .push(grpc_req_for_user(
                        USER_ID,
                        to_write_request(
                            &[l1.clone(), l2.clone()],
                            &[s2, s3],
                            &[],
                            cortexpb::write_request::SourceEnum::Api,
                        ),
                    ))
                    .await
                    .unwrap_err();
                assert_eq!(status.code(), Code::FailedPrecondition);
                assert!(status.message().contains("per-metric series limit of 1 exceeded"));
            }

            assert_eq!(log_buf.count("per-metric series limit of 1 exceeded"), 2);

            let vals = metric_value_map(&reg.gather(), "cortex_discarded_samples_total");
            assert_eq!(
                vals.get(&vec![
                    ("group".into(), "".into()),
                    ("reason".into(), "per_metric_series_limit".into()),
                    ("user".into(), USER_ID.into()),
                ])
                .copied(),
                Some(10.0)
            );

            srv_handle.abort();
            ing.stop().await.unwrap();
        });
    });
}

// Upstream: pkg/ingester/ingester_test.go::TestIngester_lastUpdatedTimeIsNotInTheFuture
#[test]
fn test_ingester_last_updated_time_is_not_in_the_future() {
    block_on_for_tests(async {
        let tmp = tempfile::tempdir().unwrap();
        let (cfg, mut blocks) = default_ingester_test_config(tmp.path());
        blocks.tsdb.ship_interval = GoDuration::ZERO;
        blocks.tsdb.head_compaction_interval = GoDuration::from_secs(60);
        blocks.tsdb.close_idle_tsdb_timeout = GoDuration::ZERO;

        let mut limits = default_limits_test_config();
        limits.creation_grace_period = ModelDuration::from_hours(24 * 365 * 15);
        let overrides = Overrides::new(limits);

        let ing = prepare_ingester_with_blocks_storage(cfg, blocks, overrides, None, None).await;

        let db = ing.get_or_create_tsdb(USER_ID).unwrap();
        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();
        let last_secs = db
            .last_update()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();
        assert!((now_secs - last_secs).abs() < 5.0);

        // Push sample ~10 years in the future.
        let future_ts = now_ms() + (10 * 365 + 310) * 24 * 3600 * 1000;
        let lbls = vec![(METRIC_NAME_LABEL, "test_future")];
        ing.push_grpc(
            USER_ID,
            &to_write_request(
                &[lbls],
                &[cortexpb::Sample {
                    timestamp_ms: future_ts,
                    value: 1.0,
                }],
                &[],
                cortexpb::write_request::SourceEnum::Api,
            ),
        )
        .await
        .unwrap();

        ing.close_all_tsdbs().await;

        // Reopen existing TSDB and verify last_update is still clamped to now, while head.max_time is future_ts.
        let db = ing.get_or_create_tsdb(USER_ID).unwrap();
        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();
        let last_secs = db
            .last_update()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();
        assert!((now_secs - last_secs).abs() < 5.0);
        assert_eq!(db.db.head().max_time(), future_ts);

        ing.stop().await.unwrap();
    });
}

// Upstream: pkg/ingester/ingester_test.go::TestIngester_Starting
#[test]
fn test_ingester_starting() {
    #[derive(Clone, Copy)]
    enum FailureMode {
        ContextCanceled,
        StartupError,
    }

    struct Case {
        name: &'static str,
        mode: FailureMode,
        in_ring_leaving: bool,
        expected_ring_state: Option<InstanceState>,
    }

    let cases = [
        Case {
            name: "not in ring + context.Canceled -> lifecycler terminated and not in ring",
            mode: FailureMode::ContextCanceled,
            in_ring_leaving: false,
            expected_ring_state: None,
        },
        Case {
            name: "not in ring + error -> lifecycler terminated and not in ring",
            mode: FailureMode::StartupError,
            in_ring_leaving: false,
            expected_ring_state: None,
        },
        Case {
            name: "in ring LEAVING + context.Canceled -> lifecycler terminated and ring state LEAVING",
            mode: FailureMode::ContextCanceled,
            in_ring_leaving: true,
            expected_ring_state: Some(InstanceState::Leaving),
        },
        Case {
            name: "in ring LEAVING + error -> lifecycler terminated and ring state LEAVING",
            mode: FailureMode::StartupError,
            in_ring_leaving: true,
            expected_ring_state: Some(InstanceState::Leaving),
        },
    ];

    block_on_for_tests(async {
        for case in cases {
            let tmp = tempfile::tempdir().unwrap();
            let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));

            let (mut cfg, blocks) = default_ingester_test_config(tmp.path());
            cfg.ring.replication_factor = 1;
            cfg.ring.unregister_on_shutdown = !case.in_ring_leaving;

            // Start first ingester cleanly, then shut down so the ring state matches the test precondition.
            let (ing1, ring1) = prepare_ingester_with_ring(
                cfg.clone(),
                blocks.clone(),
                Overrides::new(default_limits_test_config()),
                kv.clone(),
                None,
            )
            .await;
            let _ = ring1;
            ing1.stop().await.expect("stop ing1");

            let get_ring_inst_state = || async {
                let val = kv.get("collectors/ring").await.ok().flatten()?;
                let desc = val.downcast::<RingDesc>().ok()?;
                desc.ingesters
                    .get(&cfg.ring.instance_id)
                    .map(|inst| InstanceState::try_from(inst.state).unwrap())
            };

            assert_eq!(get_ring_inst_state().await, case.expected_ring_state, "{}", case.name);

            // Prepare second ingester that fails during starting().
            let mut blocks2 = blocks.clone();
            if matches!(case.mode, FailureMode::StartupError) {
                // Point TSDB dir at a regular file so create_dir_all fails inside start_with_context.
                let bad_file = tmp.path().join("not_a_dir");
                std::fs::write(&bad_file, b"x").unwrap();
                blocks2.tsdb.dir = bad_file.to_string_lossy().into_owned();
            }

            let ring2 = Ring::with_store_and_strategy(
                RingSettings::from(&cfg.ring),
                "ingester",
                "collectors/ring",
                kv.clone(),
                Arc::new(DefaultReplicationStrategy),
                None,
            )
            .unwrap();
            let lc2 = Lifecycler::with_store(
                LifecyclerConfig::from_ring_config(&cfg.ring, 9095).unwrap(),
                kv.clone(),
                None,
                "ingester",
                "collectors/ring",
                blocks2.tsdb.flush_blocks_on_shutdown,
                None,
            );
            let ing2 = Ingester::new(
                cfg.clone(),
                blocks2,
                IngestStorageConfig::default(),
                Overrides::new(default_limits_test_config()),
                Some(ring2),
                Some(lc2),
                None,
                None,
                None,
                None,
            )
            .unwrap();

            let ctx = ServiceContext::background();
            if matches!(case.mode, FailureMode::ContextCanceled) {
                ctx.cancel();
            }
            let err = ing2.start_with_context(ctx).await.unwrap_err();
            let _ = err;

            // Lifecycler service (if started) must be terminated or not running; ring state preserved.
            let st = ing2.lifecycler_service_state();
            if st != State::New {
                assert_eq!(st, State::Terminated, "{}", case.name);
            }
            assert_eq!(get_ring_inst_state().await, case.expected_ring_state, "{}", case.name);
        }
    });
}

// Upstream: pkg/ingester/ingester_test.go::TestIngester_PrepareUnregisterHandler
#[test]
fn test_ingester_prepare_unregister_handler() {
    struct Case {
        name: &'static str,
        start_ingester: bool,
        method: Method,
        body: &'static str,
        prepare: Option<fn(&Ingester)>,
        expected_status: StatusCode,
        expected_body: &'static str,
        expected_unregister: bool,
    }

    let cases = [
        Case {
            name: "returns HTTP 503 if ingester is not running",
            start_ingester: false,
            method: Method::GET,
            body: "",
            prepare: None,
            expected_status: StatusCode::SERVICE_UNAVAILABLE,
            expected_body: "",
            expected_unregister: true,
        },
        Case {
            name: "returns HTTP 400 on PUT with request body that is not valid JSON",
            start_ingester: true,
            method: Method::PUT,
            body: "invalid json",
            prepare: None,
            expected_status: StatusCode::BAD_REQUEST,
            expected_body: "",
            expected_unregister: true,
        },
        Case {
            name: "returns HTTP 400 on PUT with request body that is valid JSON but has incorrect structure",
            start_ingester: true,
            method: Method::PUT,
            body: r#"{"ping": "pong"}"#,
            prepare: None,
            expected_status: StatusCode::BAD_REQUEST,
            expected_body: "",
            expected_unregister: true,
        },
        Case {
            name: "returns HTTP 200 and unregister status on PUT with valid request body",
            start_ingester: true,
            method: Method::PUT,
            body: r#"{"unregister": false}"#,
            prepare: None,
            expected_status: StatusCode::OK,
            expected_body: r#"{"unregister":false}"#,
            expected_unregister: false,
        },
        Case {
            name: "returns HTTP 200 with unregister status on GET request",
            start_ingester: true,
            method: Method::GET,
            body: "",
            prepare: None,
            expected_status: StatusCode::OK,
            expected_body: r#"{"unregister":true}"#,
            expected_unregister: true,
        },
        Case {
            name: "returns HTTP 200 with unregister status on DELETE request",
            start_ingester: true,
            method: Method::DELETE,
            body: "",
            prepare: Some(|i| {
                i.lifecycler
                    .as_ref()
                    .unwrap()
                    .set_unregister_on_shutdown(false);
            }),
            expected_status: StatusCode::OK,
            expected_body: r#"{"unregister":true}"#,
            expected_unregister: true,
        },
    ];

    block_on_for_tests(async {
        for tc in cases {
            let tmp = tempfile::tempdir().unwrap();
            let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));
            let (mut cfg, blocks) = default_ingester_test_config(tmp.path());
            cfg.ring.replication_factor = 1;

            let ing = if tc.start_ingester {
                let (ing, _ring) = prepare_ingester_with_ring(
                    cfg,
                    blocks,
                    Overrides::new(default_limits_test_config()),
                    kv,
                    None,
                )
                .await;
                ing
            } else {
                let lc = Lifecycler::with_store(
                    LifecyclerConfig::from_ring_config(&cfg.ring, 9095).unwrap(),
                    kv,
                    None,
                    "ingester",
                    "collectors/ring",
                    blocks.tsdb.flush_blocks_on_shutdown,
                    None,
                );
                Ingester::new(
                    cfg,
                    blocks,
                    IngestStorageConfig::default(),
                    Overrides::new(default_limits_test_config()),
                    None,
                    Some(lc),
                    None,
                    None,
                    None,
                    None,
                )
                .unwrap()
            };

            if let Some(prep) = tc.prepare {
                prep(&ing);
            }

            let router = ingester_router(ing.clone());
            let req = Request::builder()
                .method(tc.method)
                .uri("/ingester/unregister-on-shutdown")
                .body(Body::from(tc.body))
                .unwrap();
            let resp = router.oneshot(req).await.unwrap();
            assert_eq!(resp.status(), tc.expected_status, "{}", tc.name);
            let body_bytes = to_bytes(resp.into_body(), 1024 * 1024).await.unwrap();
            let body_str = String::from_utf8_lossy(&body_bytes);
            assert_eq!(body_str, tc.expected_body, "{}", tc.name);
            assert_eq!(
                ing.lifecycler
                    .as_ref()
                    .unwrap()
                    .should_unregister_on_shutdown(),
                tc.expected_unregister,
                "{}",
                tc.name
            );

            if tc.start_ingester {
                ing.stop().await.unwrap();
            }
        }
    });
}

// Upstream: pkg/ingester/ingester_test.go::TestIngester_NotifyPreCommit
#[test]
fn test_ingester_notify_pre_commit() {
    block_on_for_tests(async {
        let tmp = tempfile::tempdir().unwrap();
        let reg = Registry::new();
        let (mut cfg, mut blocks) = default_ingester_test_config(tmp.path());
        cfg.ring.replication_factor = 1;
        blocks.bucket.backend_cfg.backend = "s3".to_string();
        blocks.bucket.backend_cfg.s3.endpoint = "localhost".to_string();

        let ing = prepare_ingester_with_blocks_storage(
            cfg,
            blocks,
            Overrides::new(default_limits_test_config()),
            None,
            Some(&reg),
        )
        .await;

        let now = now_ms();
        for u in ["user1", "user2", "user3"] {
            ing.push_grpc(
                u,
                &to_write_request_single_series(
                    &[(METRIC_NAME_LABEL, "test_metric")],
                    vec![
                        cortexpb::Sample {
                            timestamp_ms: now,
                            value: 1.0,
                        },
                        cortexpb::Sample {
                            timestamp_ms: now + 1000,
                            value: 2.0,
                        },
                        cortexpb::Sample {
                            timestamp_ms: now + 2000,
                            value: 3.0,
                        },
                    ],
                    cortexpb::write_request::SourceEnum::Api,
                ),
            )
            .await
            .unwrap();
        }

        let get_fsync_count = || -> u64 {
            for mf in reg.gather() {
                if mf.get_name() == "cortex_ingester_tsdb_wal_fsync_duration_seconds" {
                    if let Some(m) = mf.get_metric().first() {
                        return m.get_summary().get_sample_count();
                    }
                }
            }
            0
        };

        let before = get_fsync_count();
        ing.notify_pre_commit().unwrap();
        let after = get_fsync_count();
        assert!(
            after - before >= 3,
            "expected at least 3 fsync observations, before={before} after={after}"
        );

        ing.stop().await.unwrap();
    });
}

// Upstream: pkg/ingester/ingester_test.go::TestBlockGenerationCalculator
#[test]
fn test_block_generation_calculator() {
    block_on_for_tests(async {
        let block_range = 2 * 3600 * 1000i64;
        let tmp = tempfile::tempdir().unwrap();
        let (cfg, blocks) = default_ingester_test_config(tmp.path());
        let ing = prepare_ingester_with_blocks_storage(
            cfg,
            blocks,
            Overrides::new(default_limits_test_config()),
            None,
            None,
        )
        .await;

        let head_min_time = 10 * 3600 * 1000i64;
        ing.push_grpc(
            USER_ID,
            &to_write_request(
                &[vec![(METRIC_NAME_LABEL, "test"), ("foo", "bar")]],
                &[cortexpb::Sample {
                    timestamp_ms: head_min_time,
                    value: 1.0,
                }],
                &[],
                cortexpb::write_request::SourceEnum::Api,
            ),
        )
        .await
        .unwrap();

        let user_db: Arc<UserTsdb> = ing.get_tsdb(USER_ID).unwrap();

        let cases = [
            ("head block", true, 0, "0"),
            ("persisted block generation 1", false, head_min_time - block_range, "1"),
            (
                "persisted block generation 3",
                false,
                head_min_time - 3 * block_range - 60_000,
                "3",
            ),
            (
                "persisted block with minTime equal to head",
                false,
                head_min_time,
                "1",
            ),
            (
                "persisted block with minTime after head",
                false,
                head_min_time + block_range,
                "1",
            ),
            (
                "persisted block with capped generation label",
                false,
                head_min_time - 110 * block_range,
                "100+",
            ),
        ];

        for (name, is_head, min_time, want_gen) in cases {
            assert_eq!(
                block_generation_calculator(&user_db, block_range, is_head, min_time),
                want_gen,
                "{name}"
            );
        }

        ing.stop().await.unwrap();
    });
}

// Upstream: pkg/ingester/ingester_test.go::TestBlockGenerationCalculator_EmptyHead
#[test]
fn test_block_generation_calculator_empty_head() {
    block_on_for_tests(async {
        let block_range = 2 * 3600 * 1000i64;
        let tmp = tempfile::tempdir().unwrap();
        let (cfg, blocks) = default_ingester_test_config(tmp.path());
        let ing = prepare_ingester_with_blocks_storage(
            cfg,
            blocks,
            Overrides::new(default_limits_test_config()),
            None,
            None,
        )
        .await;

        let user_db = ing.get_or_create_tsdb(USER_ID).unwrap();
        assert_eq!(user_db.db.head().min_time(), i64::MAX);

        assert_eq!(block_generation_calculator(&user_db, block_range, true, 0), "0");
        assert_eq!(
            block_generation_calculator(&user_db, block_range, false, 1000),
            "unknown"
        );

        ing.stop().await.unwrap();
    });
}

fn verify_chunk_sample(enc: Encoding, data: &[u8], expected_ts: i64, expected_val: f64) {
    let chunk = EncodedChunk::from_bytes(enc, data.to_vec()).expect("decode chunk");
    match chunk {
        EncodedChunk::Xor(c) => {
            let mut it = c.iter();
            assert_eq!(it.next_sample().unwrap(), Some((expected_ts, expected_val)));
            assert_eq!(it.next_sample().unwrap(), None);
        }
        EncodedChunk::Xor2(c) => {
            let mut it = c.iter();
            assert_eq!(it.next_sample().unwrap(), Some((expected_ts, expected_val)));
            assert_eq!(it.next_sample().unwrap(), None);
        }
        other => panic!("unexpected chunk variant: {:?}", other.encoding()),
    }
}

fn verify_histogram_chunk_sample(
    enc: Encoding,
    data: &[u8],
    expected_ts: i64,
    expected: &FloatHistogram,
) {
    let chunk = EncodedChunk::from_bytes(enc, data.to_vec()).expect("decode histogram chunk");
    match chunk {
        EncodedChunk::Histogram(c) => {
            let mut it = c.iter();
            let (ts, h) = it.next_histogram().unwrap().unwrap();
            assert_eq!(ts, expected_ts);
            assert_eq!(&h.to_float(), expected);
            assert_eq!(it.next_histogram().unwrap(), None);
        }
        EncodedChunk::HistogramSt(c) => {
            let mut it = c.iter();
            let (ts, h) = it.next_histogram().unwrap().unwrap();
            assert_eq!(ts, expected_ts);
            assert_eq!(&h.to_float(), expected);
            assert_eq!(it.at_st(), 0);
            assert_eq!(it.next_histogram().unwrap(), None);
        }
        EncodedChunk::FloatHistogram(c) => {
            let mut it = c.iter();
            let (ts, fh) = it.next_float_histogram().unwrap().unwrap();
            assert_eq!(ts, expected_ts);
            assert_eq!(&fh, expected);
            assert_eq!(it.next_float_histogram().unwrap(), None);
        }
        EncodedChunk::FloatHistogramSt(c) => {
            let mut it = c.iter();
            let (ts, fh) = it.next_float_histogram().unwrap().unwrap();
            assert_eq!(ts, expected_ts);
            assert_eq!(&fh, expected);
            assert_eq!(it.at_st(), 0);
            assert_eq!(it.next_float_histogram().unwrap(), None);
        }
        other => panic!("unexpected chunk variant: {:?}", other.encoding()),
    }
}

async fn run_xor2_encoding_test(xor2_enabled: bool) {
    let tmp = tempfile::tempdir().unwrap();
    let (cfg, blocks) = default_ingester_test_config(tmp.path());
    let mut limits = default_limits_test_config();
    if xor2_enabled {
        limits.float_chunk_encoding = "xor2".to_string();
    }

    let ing = prepare_ingester_with_blocks_storage(
        cfg,
        blocks,
        Overrides::new(limits),
        None,
        None,
    )
    .await;

    let ts = 1000i64;
    ing.push_grpc(
        "user1",
        &to_write_request(
            &[vec![(METRIC_NAME_LABEL, "testmetric_xor2")]],
            &[cortexpb::Sample {
                timestamp_ms: ts,
                value: 42.0,
            }],
            &[],
            cortexpb::write_request::SourceEnum::Api,
        ),
    )
    .await
    .unwrap();

    let chunks = query_chunks_for_metric(&ing, "user1", "testmetric_xor2").await;
    assert_eq!(chunks.len(), 1);
    let (expected_enc, expected_wire) = if xor2_enabled {
        (Encoding::Xor2, 7)
    } else {
        (Encoding::Xor, 4)
    };
    assert_eq!(chunks[0].encoding, expected_wire);
    verify_chunk_sample(expected_enc, &chunks[0].data, ts, 42.0);

    ing.stop().await.unwrap();
}

// Upstream: pkg/ingester/ingester_test.go::TestIngesterXOR2EncodingEnabled
#[test]
fn test_ingester_xor2_encoding_enabled() {
    block_on_for_tests(run_xor2_encoding_test(true));
}

// Upstream: pkg/ingester/ingester_test.go::TestIngesterXOR2EncodingDisabled
#[test]
fn test_ingester_xor2_encoding_disabled() {
    block_on_for_tests(run_xor2_encoding_test(false));
}

// Upstream: pkg/ingester/ingester_test.go::TestIngester_PushCreatedTimestampXOR2Encoding
#[test]
fn test_ingester_push_created_timestamp_xor2_encoding() {
    block_on_for_tests(async {
        let tmp = tempfile::tempdir().unwrap();
        let (cfg, blocks) = default_ingester_test_config(tmp.path());
        let mut limits = default_limits_test_config();
        limits.float_chunk_encoding = "xor2".to_string();

        let ing = prepare_ingester_with_blocks_storage(
            cfg,
            blocks,
            Overrides::new(limits),
            None,
            None,
        )
        .await;

        let metric_name = "testmetric_xor2_created_timestamp";
        let created_ts = 500i64;
        let ts = 1000i64;
        let mut req = to_write_request(
            &[vec![(METRIC_NAME_LABEL, metric_name)]],
            &[cortexpb::Sample {
                timestamp_ms: ts,
                value: 42.0,
            }],
            &[],
            cortexpb::write_request::SourceEnum::Api,
        );
        req.timeseries[0].created_timestamp = created_ts;
        ing.push_grpc("user1", &req).await.unwrap();

        let chunks = query_chunks_for_metric(&ing, "user1", metric_name).await;
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].encoding, 7);

        let chunk = EncodedChunk::from_bytes(Encoding::Xor2, chunks[0].data.to_vec()).unwrap();
        let EncodedChunk::Xor2(c) = chunk else {
            panic!("expected xor2");
        };
        let mut it = c.iter();
        for (exp_t, exp_v) in [(created_ts, 0.0), (ts, 42.0)] {
            assert_eq!(it.next_sample().unwrap(), Some((exp_t, exp_v)));
            assert_eq!(it.at_st(), 0, "sample at {exp_t} has a start timestamp");
        }
        assert_eq!(it.next_sample().unwrap(), None);

        ing.stop().await.unwrap();
    });
}

// Upstream: pkg/ingester/ingester_test.go::TestIngesterXOR2EncodingRuntimeToggle
#[test]
fn test_ingester_xor2_encoding_runtime_toggle() {
    let cases = [
        ("enabling XOR2", "xor", "xor2", 4i32, 7i32),
        ("clearing the limit falls back to XOR", "xor2", "", 7i32, 4i32),
    ];

    block_on_for_tests(async {
        for (name, initial_enc, updated_enc, exp_before, exp_after) in cases {
            let user_id = "user1";
            let tmp = tempfile::tempdir().unwrap();
            let (mut cfg, blocks) = default_ingester_test_config(tmp.path());
            cfg.tsdb_config_update_period = GoDuration::from_millis(100);

            let mut t_lim = default_limits_test_config();
            if !initial_enc.is_empty() {
                t_lim.float_chunk_encoding = initial_enc.to_string();
            }
            let overrides = Overrides::with_static_map(
                default_limits_test_config(),
                HashMap::from([(user_id.to_string(), t_lim)]),
            );

            let ing = prepare_ingester_with_blocks_storage(
                cfg,
                blocks,
                overrides.clone(),
                None,
                None,
            )
            .await;

            ing.push_grpc(
                user_id,
                &to_write_request(
                    &[vec![(METRIC_NAME_LABEL, "testmetric_xor2_before")]],
                    &[cortexpb::Sample {
                        timestamp_ms: 1000,
                        value: 1.0,
                    }],
                    &[],
                    cortexpb::write_request::SourceEnum::Api,
                ),
            )
            .await
            .unwrap();

            let chunks = query_chunks_for_metric(&ing, user_id, "testmetric_xor2_before").await;
            assert_eq!(chunks.len(), 1, "{name}");
            assert_eq!(chunks[0].encoding, exp_before, "{name}");

            let mut updated = default_limits_test_config();
            if !updated_enc.is_empty() {
                updated.float_chunk_encoding = updated_enc.to_string();
            }
            overrides.set_static_override(user_id, Some(updated));
            tokio::time::sleep(Duration::from_millis(250)).await;

            ing.push_grpc(
                user_id,
                &to_write_request(
                    &[vec![(METRIC_NAME_LABEL, "testmetric_xor2_after")]],
                    &[cortexpb::Sample {
                        timestamp_ms: 2000,
                        value: 2.0,
                    }],
                    &[],
                    cortexpb::write_request::SourceEnum::Api,
                ),
            )
            .await
            .unwrap();

            let chunks = query_chunks_for_metric(&ing, user_id, "testmetric_xor2_after").await;
            assert_eq!(chunks.len(), 1, "{name}");
            assert_eq!(chunks[0].encoding, exp_after, "{name}");

            ing.stop().await.unwrap();
        }
    });
}

// Upstream: pkg/ingester/ingester_test.go::TestIngesterHistogramSTEncoding
#[test]
fn test_ingester_histogram_st_encoding() {
    let cases = [
        (
            "histogram",
            Encoding::Histogram,
            5i32,
            Encoding::FloatHistogram,
            6i32,
        ),
        (
            "histogram_st",
            Encoding::HistogramSt,
            8i32,
            Encoding::FloatHistogramSt,
            9i32,
        ),
    ];

    block_on_for_tests(async {
        for (enc_name, exp_chunk_enc, exp_wire, exp_fchunk_enc, exp_fwire) in cases {
            let tmp = tempfile::tempdir().unwrap();
            let (cfg, blocks) = default_ingester_test_config(tmp.path());
            let mut limits = default_limits_test_config();
            limits.native_histograms_ingestion_enabled = true;
            limits.histogram_chunk_encoding = enc_name.to_string();

            let ing = prepare_ingester_with_blocks_storage(
                cfg,
                blocks,
                Overrides::new(limits),
                None,
                None,
            )
            .await;

            let ts = 1000i64;
            let h = generate_test_histogram(1);
            let fh = generate_test_float_histogram(1);

            let req = cortexpb::WriteRequest {
                timeseries: vec![
                    cortexpb::TimeSeries {
                        labels: vec![cortexpb::LabelPair::from_strs(
                            METRIC_NAME_LABEL,
                            "testmetric_histogram",
                        )],
                        histograms: vec![int_histogram_to_proto(ts, &h)],
                        ..Default::default()
                    },
                    cortexpb::TimeSeries {
                        labels: vec![cortexpb::LabelPair::from_strs(
                            METRIC_NAME_LABEL,
                            "testmetric_float_histogram",
                        )],
                        histograms: vec![float_histogram_to_proto(ts, &fh)],
                        ..Default::default()
                    },
                ],
                source: cortexpb::write_request::SourceEnum::Api as i32,
                ..Default::default()
            };
            ing.push_grpc("user1", &req).await.unwrap();

            let chunks = query_chunks_for_metric(&ing, "user1", "testmetric_histogram").await;
            assert_eq!(chunks.len(), 1, "{enc_name}");
            assert_eq!(chunks[0].encoding, exp_wire, "{enc_name}");
            verify_histogram_chunk_sample(exp_chunk_enc, &chunks[0].data, ts, &h.to_float());

            let chunks =
                query_chunks_for_metric(&ing, "user1", "testmetric_float_histogram").await;
            assert_eq!(chunks.len(), 1, "{enc_name}");
            assert_eq!(chunks[0].encoding, exp_fwire, "{enc_name}");
            verify_histogram_chunk_sample(exp_fchunk_enc, &chunks[0].data, ts, &fh);

            ing.stop().await.unwrap();
        }
    });
}

// Upstream: pkg/ingester/ingester_test.go::TestIngesterHistogramSTEncodingRuntimeToggle
#[test]
fn test_ingester_histogram_st_encoding_runtime_toggle() {
    let cases = [
        ("enabling HistogramST", "", "histogram_st", 5i32, 8i32),
        (
            "clearing the limit falls back to Histogram",
            "histogram_st",
            "",
            8i32,
            5i32,
        ),
    ];

    block_on_for_tests(async {
        for (name, initial_enc, updated_enc, exp_before, exp_after) in cases {
            let user_id = "user1";
            let tmp = tempfile::tempdir().unwrap();
            let (mut cfg, blocks) = default_ingester_test_config(tmp.path());
            cfg.tsdb_config_update_period = GoDuration::from_millis(100);

            let mut t_lim = default_limits_test_config();
            t_lim.native_histograms_ingestion_enabled = true;
            if !initial_enc.is_empty() {
                t_lim.histogram_chunk_encoding = initial_enc.to_string();
            }
            let overrides = Overrides::with_static_map(
                default_limits_test_config(),
                HashMap::from([(user_id.to_string(), t_lim)]),
            );

            let ing = prepare_ingester_with_blocks_storage(
                cfg.clone(),
                blocks.clone(),
                overrides.clone(),
                None,
                None,
            )
            .await;

            async fn push_hist(ing: &Ingester, user_id: &str, metric: &str, ts: i64) {
                let req = cortexpb::WriteRequest {
                    timeseries: vec![cortexpb::TimeSeries {
                        labels: vec![cortexpb::LabelPair::from_strs(METRIC_NAME_LABEL, metric)],
                        histograms: vec![int_histogram_to_proto(ts, &generate_test_histogram(1))],
                        ..Default::default()
                    }],
                    source: cortexpb::write_request::SourceEnum::Api as i32,
                    ..Default::default()
                };
                ing.push_grpc(user_id, &req).await.unwrap();
            }

            push_hist(&ing, user_id, "testmetric_histogram_before", 1000).await;
            let chunks =
                query_chunks_for_metric(&ing, user_id, "testmetric_histogram_before").await;
            assert_eq!(chunks.len(), 1, "{name}");
            assert_eq!(chunks[0].encoding, exp_before, "{name}");

            let mut updated = default_limits_test_config();
            updated.native_histograms_ingestion_enabled = true;
            if !updated_enc.is_empty() {
                updated.histogram_chunk_encoding = updated_enc.to_string();
            }
            overrides.set_static_override(user_id, Some(updated));
            tokio::time::sleep(Duration::from_millis(250)).await;

            // A new series still uses the encoding the open TSDB was seeded with.
            push_hist(&ing, user_id, "testmetric_histogram_after", 2000).await;
            let chunks =
                query_chunks_for_metric(&ing, user_id, "testmetric_histogram_after").await;
            assert_eq!(chunks.len(), 1, "{name}");
            assert_eq!(chunks[0].encoding, exp_before, "{name}");

            // Restarting the ingester reopens the tenant's TSDB, which picks up the updated setting.
            ing.stop().await.unwrap();
            let ing = prepare_ingester_with_blocks_storage(
                cfg,
                blocks,
                overrides,
                None,
                None,
            )
            .await;

            push_hist(&ing, user_id, "testmetric_histogram_after_reopen", 3000).await;
            let chunks =
                query_chunks_for_metric(&ing, user_id, "testmetric_histogram_after_reopen").await;
            assert_eq!(chunks.len(), 1, "{name}");
            assert_eq!(chunks[0].encoding, exp_after, "{name}");

            ing.stop().await.unwrap();
        }
    });
}

