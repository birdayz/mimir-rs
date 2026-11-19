//! Differential conformance for production soak metric parity between Go Mimir
//! (`target/go-mimir`) and Rust Mimir (`target/release/mimir`):
//!
//! (A) `cortex_ingester_queried_series` (stages `merged_blocks`, `single_block_index`,
//!     `single_block_filter`, classic text + native protobuf histogram),
//!     `cortex_ingester_queried_samples`, and `cortex_ingester_queried_blocks_total`
//!     across Head + 4 compacted ULID blocks (inline + lazy paths, sharding, empty, out-of-range).
//! (B) `cortex_ingester_tsdb_head_chunks` (and created/removed/mmap_chunks/max_mmapped)
//!     across active head chunks, chunks_head rollover, OOO chunks, compaction/GC, and WAL replay.
//! (C) Full `/metrics` metric family diff under Helm ingest-storage config (Kafka + S3 +
//!     memberlist + float/histogram/exemplar/metadata + flush/ship + queries).

#![allow(clippy::result_large_err)]

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::{Buf, Bytes};
use mimir_common::block_on_for_tests;
use mimir_conformance::free_test_port;
use mimir_conformance::oracle::{
    ExemplarJson, HistogramJson, KfakeBrokerRequest, KfakeRecordInput, LabelJson, MetadataJson,
    SampleJson, SpanJson, SpawnedKfakeBroker, TimeSeriesJson, WriteRequestJson,
};
use mimir_conformance::twin::{start_mimir, wait_ready, FakeS3, Impl};
use mimir_proto::cortexpb;
use mimir_proto::ingesterpb;
use mimir_proto::ingesterpb::ingester_client::IngesterClient;
use mimir_proto::prost;
use mimir_proto::prost::encoding::{decode_key, decode_varint, skip_field, DecodeContext, WireType};
use tokio_stream::StreamExt;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;

const IMPLS: [Impl; 2] = [Impl::Go, Impl::Rust];

// --- Minimal wire decoder for io.prometheus.client.MetricFamily (delimited protobuf) ---

#[derive(Clone, Debug, Default)]
struct PromBucketSpan {
    offset: i32,
    length: u32,
}

#[derive(Clone, Debug, Default)]
struct PromHistogram {
    sample_count: u64,
    sample_sum: f64,
    classic_buckets: usize,
    schema: Option<i32>,
    zero_threshold: Option<f64>,
    zero_count: Option<u64>,
    negative_span: Vec<PromBucketSpan>,
    negative_delta: Vec<i64>,
    positive_span: Vec<PromBucketSpan>,
    positive_delta: Vec<i64>,
}

#[derive(Clone, Debug, Default)]
struct PromMetric {
    labels: Vec<(String, String)>,
    histogram: Option<PromHistogram>,
}

#[derive(Clone, Debug, Default)]
struct PromMetricFamily {
    name: String,
    metrics: Vec<PromMetric>,
}

fn unzigzag32(v: u32) -> i32 {
    ((v >> 1) as i32) ^ (-((v & 1) as i32))
}

fn unzigzag64(v: u64) -> i64 {
    ((v >> 1) as i64) ^ (-((v & 1) as i64))
}

fn read_len_slice<'a>(buf: &mut &'a [u8]) -> &'a [u8] {
    let len = decode_varint(buf).unwrap() as usize;
    let s = &buf[..len];
    *buf = &buf[len..];
    s
}

fn decode_label_pair(mut buf: &[u8]) -> (String, String) {
    let (mut n, mut v) = (String::new(), String::new());
    while buf.has_remaining() {
        let (tag, wt) = decode_key(&mut buf).unwrap();
        match (tag, wt) {
            (1, WireType::LengthDelimited) => n = String::from_utf8_lossy(read_len_slice(&mut buf)).into_owned(),
            (2, WireType::LengthDelimited) => v = String::from_utf8_lossy(read_len_slice(&mut buf)).into_owned(),
            _ => skip_field(wt, tag, &mut buf, DecodeContext::default()).unwrap(),
        }
    }
    (n, v)
}

fn decode_span(mut buf: &[u8]) -> PromBucketSpan {
    let mut s = PromBucketSpan::default();
    while buf.has_remaining() {
        let (tag, wt) = decode_key(&mut buf).unwrap();
        match (tag, wt) {
            (1, WireType::Varint) => s.offset = unzigzag32(decode_varint(&mut buf).unwrap() as u32),
            (2, WireType::Varint) => s.length = decode_varint(&mut buf).unwrap() as u32,
            _ => skip_field(wt, tag, &mut buf, DecodeContext::default()).unwrap(),
        }
    }
    s
}

fn decode_histogram(mut buf: &[u8]) -> PromHistogram {
    let mut h = PromHistogram::default();
    while buf.has_remaining() {
        let (tag, wt) = decode_key(&mut buf).unwrap();
        match (tag, wt) {
            (1, WireType::Varint) => h.sample_count = decode_varint(&mut buf).unwrap(),
            (2, WireType::SixtyFourBit) => {
                h.sample_sum = f64::from_le_bytes(buf[..8].try_into().unwrap());
                buf.advance(8);
            }
            (3, WireType::LengthDelimited) => {
                let _ = read_len_slice(&mut buf);
                h.classic_buckets += 1;
            }
            (5, WireType::Varint) => h.schema = Some(unzigzag32(decode_varint(&mut buf).unwrap() as u32)),
            (6, WireType::SixtyFourBit) => {
                h.zero_threshold = Some(f64::from_le_bytes(buf[..8].try_into().unwrap()));
                buf.advance(8);
            }
            (7, WireType::Varint) => h.zero_count = Some(decode_varint(&mut buf).unwrap()),
            (9, WireType::LengthDelimited) => h.negative_span.push(decode_span(read_len_slice(&mut buf))),
            (10, WireType::Varint) => h.negative_delta.push(unzigzag64(decode_varint(&mut buf).unwrap())),
            (10, WireType::LengthDelimited) => {
                let mut p = read_len_slice(&mut buf);
                while p.has_remaining() {
                    h.negative_delta.push(unzigzag64(decode_varint(&mut p).unwrap()));
                }
            }
            (12, WireType::LengthDelimited) => h.positive_span.push(decode_span(read_len_slice(&mut buf))),
            (13, WireType::Varint) => h.positive_delta.push(unzigzag64(decode_varint(&mut buf).unwrap())),
            (13, WireType::LengthDelimited) => {
                let mut p = read_len_slice(&mut buf);
                while p.has_remaining() {
                    h.positive_delta.push(unzigzag64(decode_varint(&mut p).unwrap()));
                }
            }
            _ => skip_field(wt, tag, &mut buf, DecodeContext::default()).unwrap(),
        }
    }
    h
}

fn decode_metric(mut buf: &[u8]) -> PromMetric {
    let mut m = PromMetric::default();
    while buf.has_remaining() {
        let (tag, wt) = decode_key(&mut buf).unwrap();
        match (tag, wt) {
            (1, WireType::LengthDelimited) => m.labels.push(decode_label_pair(read_len_slice(&mut buf))),
            (7, WireType::LengthDelimited) => m.histogram = Some(decode_histogram(read_len_slice(&mut buf))),
            _ => skip_field(wt, tag, &mut buf, DecodeContext::default()).unwrap(),
        }
    }
    m
}

fn decode_metric_family(mut buf: &[u8]) -> PromMetricFamily {
    let mut mf = PromMetricFamily::default();
    while buf.has_remaining() {
        let (tag, wt) = decode_key(&mut buf).unwrap();
        match (tag, wt) {
            (1, WireType::LengthDelimited) => mf.name = String::from_utf8_lossy(read_len_slice(&mut buf)).into_owned(),
            (4, WireType::LengthDelimited) => mf.metrics.push(decode_metric(read_len_slice(&mut buf))),
            _ => skip_field(wt, tag, &mut buf, DecodeContext::default()).unwrap(),
        }
    }
    mf
}

fn decode_delimited_families(mut raw: &[u8]) -> Vec<PromMetricFamily> {
    let mut out = Vec::new();
    while raw.has_remaining() {
        let len = prost::decode_length_delimiter(&mut raw).expect("varint len");
        let msg = &raw[..len];
        out.push(decode_metric_family(msg));
        raw.advance(len);
    }
    out
}

async fn scrape_text(http: &reqwest::Client, port: u16) -> String {
    http.get(format!("http://127.0.0.1:{port}/metrics"))
        .header("Accept", "text/plain; version=0.0.4")
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap()
}

async fn scrape_proto(http: &reqwest::Client, port: u16) -> Vec<PromMetricFamily> {
    let bytes = http
        .get(format!("http://127.0.0.1:{port}/metrics"))
        .header(
            "Accept",
            "application/vnd.google.protobuf;proto=io.prometheus.client.MetricFamily;encoding=delimited",
        )
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    decode_delimited_families(&bytes)
}

fn text_families(text: &str) -> BTreeSet<String> {
    text.lines()
        .filter_map(|l| l.strip_prefix("# TYPE "))
        .filter_map(|rest| rest.split_whitespace().next())
        .map(ToString::to_string)
        .collect()
}

fn metric_value(exposition: &str, name: &str, labels: &[(&str, &str)]) -> Option<f64> {
    exposition
        .lines()
        .filter(|l| !l.starts_with('#'))
        .filter_map(|l| l.rsplit_once(' '))
        .find(|(series, _)| {
            let (n, rest) = series.split_once('{').unwrap_or((series, ""));
            n == name
                && labels
                    .iter()
                    .all(|(k, v)| rest.contains(&format!("{k}=\"{v}\"")))
        })
        .and_then(|(_, v)| v.parse().ok())
}

fn lp(name: &str, value: &str) -> cortexpb::LabelPair {
    cortexpb::LabelPair {
        name: Bytes::copy_from_slice(name.as_bytes()),
        value: Bytes::copy_from_slice(value.as_bytes()),
    }
}

fn with_org<T>(org: &str, msg: T) -> tonic::Request<T> {
    let mut req = tonic::Request::new(msg);
    req.metadata_mut()
        .insert("x-scope-orgid", MetadataValue::try_from(org).unwrap());
    req
}

fn matcher(t: ingesterpb::MatchType, name: &str, value: &str) -> ingesterpb::LabelMatcher {
    ingesterpb::LabelMatcher {
        r#type: t as i32,
        name: name.to_string(),
        value: value.to_string(),
    }
}

async fn connect_grpc(port: u16) -> IngesterClient<Channel> {
    let ch = tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{port}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    IngesterClient::new(ch)
}

async fn query_stream_counts(
    client: &mut IngesterClient<Channel>,
    org: &str,
    start_ms: i64,
    end_ms: i64,
    matchers: Vec<ingesterpb::LabelMatcher>,
) -> (usize, usize) {
    let mut stream = client
        .query_stream(with_org(
            org,
            ingesterpb::QueryRequest {
                start_timestamp_ms: start_ms,
                end_timestamp_ms: end_ms,
                matchers,
                streaming_chunks_batch_size: 128,
            },
        ))
        .await
        .unwrap()
        .into_inner();
    let mut series = 0usize;
    let mut chunks = 0usize;
    while let Some(resp) = stream.next().await {
        let resp = resp.unwrap();
        series += resp.streaming_series.len();
        for sc in &resp.streaming_series_chunks {
            chunks += sc.chunks.len();
        }
    }
    (series, chunks)
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn classic_yaml(http_port: u16, grpc_port: u16, dir: &Path, ooo_window: &str) -> String {
    format!(
        r#"target: ingester
multitenancy_enabled: true
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http_port}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc_port}
ingester:
  ring:
    instance_id: ing-1
    instance_addr: 127.0.0.1
    kvstore:
      store: inmemory
    replication_factor: 1
    min_ready_duration: 0s
    final_sleep: 0s
blocks_storage:
  backend: filesystem
  filesystem:
    dir: {dir}/bucket
  tsdb:
    dir: {dir}/tsdb
    retention_period: 48h
    head_compaction_interval: 15m
    ship_interval: 0s
limits:
  max_global_series_per_user: 0
  max_global_series_per_metric: 0
  max_global_exemplars_per_user: 100
  out_of_order_time_window: {ooo_window}
  native_histograms_ingestion_enabled: true
"#,
        dir = dir.display()
    )
}

fn proto_hist_summary(families: &[PromMetricFamily], stage: &str) -> String {
    let Some(mf) = families.iter().find(|f| f.name == "cortex_ingester_queried_series") else {
        return "<no family>".to_string();
    };
    let Some(m) = mf.metrics.iter().find(|m| {
        m.labels.iter().any(|(k, v)| k == "stage" && v == stage)
    }) else {
        return "<no stage>".to_string();
    };
    let h = m.histogram.as_ref().unwrap();
    format!(
        "cnt={} sum={} schema={:?} zero_cnt={:?} pos_spans={} pos_deltas={:?} classic_buckets={}",
        h.sample_count,
        h.sample_sum,
        h.schema,
        h.zero_count,
        h.positive_span.len(),
        h.positive_delta,
        h.classic_buckets,
    )
}

#[test]
fn twin_process_queried_series_head_plus_4_blocks_parity() {
    let tmp = tempfile::tempdir().unwrap();
    let http = reqwest::Client::new();
    let now = (now_ms() / 60_000) * 60_000;
    let h2 = 2 * 3_600_000i64;
    let t_b1 = now - 4 * h2;
    let t_b2 = now - 3 * h2;
    let t_b3 = now - 2 * h2;
    let t_b4 = now - h2;
    let t_head = now;

    block_on_for_tests(async {
        let mut side_summaries: Vec<(String, BTreeMap<String, String>)> = Vec::new();
        let mut side_protos: Vec<(String, BTreeMap<String, String>)> = Vec::new();

        for which in IMPLS {
            let hp = free_test_port();
            let gp = free_test_port();
            let dir = tmp.path().join(which.name());
            std::fs::create_dir_all(dir.join("bucket")).unwrap();
            let yaml = classic_yaml(hp, gp, &dir, "24h");
            let mut proc = start_mimir(which, "soak-a", &dir, &yaml, &[]);
            wait_ready(&http, &mut proc, hp, Duration::from_secs(15)).await;
            let mut client = connect_grpc(gp).await;

            // Create 4 compacted blocks + 1 active Head:
            // - 200 series "m_all" present in all 4 blocks AND Head (5 chunks each = 1000 refs -> triggers lazy!)
            // - 100 series "m_head_only" present ONLY in Head
            // - 50 series "m_blk1_only" present ONLY in block 1
            for (bi, ts) in [t_b1, t_b2, t_b3, t_b4].iter().enumerate() {
                let mut req = cortexpb::WriteRequest {
                    source: cortexpb::write_request::SourceEnum::Api as i32,
                    ..Default::default()
                };
                for i in 0..200 {
                    req.timeseries.push(cortexpb::TimeSeries {
                        labels: vec![lp("__name__", "m_all"), lp("i", &format!("s{i:03}"))],
                        samples: vec![
                            cortexpb::Sample { value: (bi * 10 + 1) as f64, timestamp_ms: *ts },
                            cortexpb::Sample { value: (bi * 10 + 2) as f64, timestamp_ms: *ts + 15_000 },
                        ],
                        ..Default::default()
                    });
                }
                if bi == 0 {
                    for i in 0..50 {
                        req.timeseries.push(cortexpb::TimeSeries {
                            labels: vec![lp("__name__", "m_blk1_only"), lp("i", &format!("b{i:03}"))],
                            samples: vec![cortexpb::Sample { value: 1.0, timestamp_ms: *ts }],
                            ..Default::default()
                        });
                    }
                }
                client.push(with_org("t-soak", req)).await.unwrap();
                let resp = http
                    .post(format!("http://127.0.0.1:{hp}/ingester/flush?tenant=t-soak&wait=true"))
                    .send()
                    .await
                    .unwrap();
                assert!(resp.status().is_success(), "flush status {}", resp.status());
            }
            // Now push to Head without flushing
            let mut req = cortexpb::WriteRequest {
                source: cortexpb::write_request::SourceEnum::Api as i32,
                ..Default::default()
            };
            for i in 0..200 {
                req.timeseries.push(cortexpb::TimeSeries {
                    labels: vec![lp("__name__", "m_all"), lp("i", &format!("s{i:03}"))],
                    samples: vec![
                        cortexpb::Sample { value: 99.0, timestamp_ms: t_head },
                        cortexpb::Sample { value: 100.0, timestamp_ms: t_head + 15_000 },
                    ],
                    ..Default::default()
                });
            }
            for i in 0..100 {
                req.timeseries.push(cortexpb::TimeSeries {
                    labels: vec![lp("__name__", "m_head_only"), lp("i", &format!("h{i:03}"))],
                    samples: vec![cortexpb::Sample { value: 7.0, timestamp_ms: t_head }],
                    ..Default::default()
                });
            }
            client.push(with_org("t-soak", req)).await.unwrap();

            let block_count = std::fs::read_dir(dir.join("tsdb/t-soak"))
                .unwrap()
                .filter_map(|e| e.ok())
                .filter(|e| e.file_name().to_str().is_some_and(|n| n.len() == 26))
                .count();
            assert_eq!(block_count, 4, "[{}] expected 4 blocks on disk", which.name());

            let queries: Vec<(&str, i64, i64, Vec<ingesterpb::LabelMatcher>)> = vec![
                ("q1_lazy_all_4blocks_plus_head", t_b1 - 1000, t_head + 60_000, vec![matcher(ingesterpb::MatchType::RegexMatch, "__name__", "m_all|m_head_only|m_blk1_only")]),
                ("q2_inline_small", t_b1 - 1000, t_head + 60_000, vec![matcher(ingesterpb::MatchType::Equal, "__name__", "m_blk1_only")]),
                ("q3_lazy_sharded_1of4", t_b1 - 1000, t_head + 60_000, vec![matcher(ingesterpb::MatchType::Equal, "__name__", "m_all"), matcher(ingesterpb::MatchType::Equal, "__query_shard__", "1_of_4")]),
                ("q4_empty_nonexistent", t_b1 - 1000, t_head + 60_000, vec![matcher(ingesterpb::MatchType::Equal, "__name__", "does_not_exist")]),
                ("q5_head_only_window", t_head - 60_000, t_head + 60_000, vec![matcher(ingesterpb::MatchType::Equal, "__name__", "m_all")]),
                ("q6_gap_between_blocks", t_b2 + 60_000, t_b3 - 60_000, vec![matcher(ingesterpb::MatchType::Equal, "__name__", "m_all")]),
            ];

            let mut map = BTreeMap::new();
            let mut pmap = BTreeMap::new();
            for (qname, qstart, qend, matchers) in queries {
                let t_before = scrape_text(&http, hp).await;
                let (s_cnt, c_cnt) = query_stream_counts(&mut client, "t-soak", qstart, qend, matchers).await;
                let t_after = scrape_text(&http, hp).await;
                let p_after = scrape_proto(&http, hp).await;

                let mut line = format!("ret_s={s_cnt} ret_c={c_cnt}");
                let mut pb_stages = String::new();
                for stage in ["merged_blocks", "single_block_index", "single_block_filter"] {
                    let dc = metric_value(&t_after, "cortex_ingester_queried_series_count", &[("stage", stage)]).unwrap_or(0.0)
                        - metric_value(&t_before, "cortex_ingester_queried_series_count", &[("stage", stage)]).unwrap_or(0.0);
                    let ds = metric_value(&t_after, "cortex_ingester_queried_series_sum", &[("stage", stage)]).unwrap_or(0.0)
                        - metric_value(&t_before, "cortex_ingester_queried_series_sum", &[("stage", stage)]).unwrap_or(0.0);
                    line.push_str(&format!(" | {stage}:c={dc}/s={ds}"));
                    pb_stages.push_str(&format!(" {stage}=({})", proto_hist_summary(&p_after, stage)));
                }
                let sc = metric_value(&t_after, "cortex_ingester_queried_samples_count", &[]).unwrap_or(0.0)
                    - metric_value(&t_before, "cortex_ingester_queried_samples_count", &[]).unwrap_or(0.0);
                let ss = metric_value(&t_after, "cortex_ingester_queried_samples_sum", &[]).unwrap_or(0.0)
                    - metric_value(&t_before, "cortex_ingester_queried_samples_sum", &[]).unwrap_or(0.0);
                line.push_str(&format!(" | samples:c={sc}/s={ss}"));

                let mut qb = BTreeMap::new();
                for l in t_after.lines().filter(|l| l.starts_with("cortex_ingester_queried_blocks_total{")) {
                    if let Some((k, v)) = l.rsplit_once(' ') {
                        let gen_val = k.split("generation=\"").nth(1).unwrap().split('"').next().unwrap();
                        let bv = metric_value(&t_before, "cortex_ingester_queried_blocks_total", &[("generation", gen_val)]).unwrap_or(0.0);
                        let av: f64 = v.parse().unwrap_or(0.0);
                        if av - bv > 0.0 {
                            qb.insert(k.to_string(), av - bv);
                        }
                    }
                }
                line.push_str(&format!(" | blocks={qb:?}"));
                map.insert(qname.to_string(), line);
                pmap.insert(qname.to_string(), pb_stages);
            }
            side_summaries.push((which.name().to_string(), map));
            side_protos.push((which.name().to_string(), pmap));
        }

        let go_map = &side_summaries[0].1;
        let rust_map = &side_summaries[1].1;
        assert_eq!(rust_map, go_map, "text queried_series/samples/blocks parity mismatch");
        assert_eq!(side_protos[1].1, side_protos[0].1, "protobuf native histogram queried_series parity mismatch");
        assert!(rust_map["q1_lazy_all_4blocks_plus_head"].contains("merged_blocks:c=1/s=350"));
        assert!(rust_map["q3_lazy_sharded_1of4"].contains("single_block_index:c=5/s=1000"));
    });
}

#[test]
fn twin_process_tsdb_head_chunks_lifecycle_parity() {
    let http = reqwest::Client::new();
    let now = (now_ms() / 60_000) * 60_000;
    let h2 = 2 * 3_600_000i64;

    block_on_for_tests(async {
        let mut side_phases: Vec<(String, BTreeMap<String, String>)> = Vec::new();
        for which in IMPLS {
            // Go's HeadCompactionIntervalJitterEnabled (yaml:"-", always true in binaries) picks
            // a uniform random first check in [0, 15m). Retry if that ~1/1800 roll lands mid-Phase-1..3.
            let phases = loop {
            let hp = free_test_port();
            let gp = free_test_port();
            let attempt_tmp = tempfile::tempdir().unwrap();
            let dir = attempt_tmp.path().join(which.name());
            std::fs::create_dir_all(dir.join("bucket")).unwrap();
            let yaml = classic_yaml(hp, gp, &dir, "24h");
            let mut proc = start_mimir(which, "soak-b", &dir, &yaml, &[]);
            wait_ready(&http, &mut proc, hp, Duration::from_secs(15)).await;
            let mut client = connect_grpc(gp).await;

            let snap = |t: &str| -> String {
                let g = |n: &str, lbl: &[(&str, &str)]| metric_value(t, n, lbl).unwrap_or(-1.0);
                format!(
                    "chunks={} created={} removed={} mmap_total={} max_mmapped={} mem_series={} blocks={}",
                    g("cortex_ingester_tsdb_head_chunks", &[]),
                    g("cortex_ingester_tsdb_head_chunks_created_total", &[("user", "t-chk")]),
                    g("cortex_ingester_tsdb_head_chunks_removed_total", &[("user", "t-chk")]),
                    g("cortex_ingester_tsdb_mmap_chunks_total", &[]),
                    g("cortex_ingester_tsdb_head_chunks_max_mmapped", &[]),
                    g("cortex_ingester_memory_series", &[]),
                    g("cortex_ingester_tsdb_blocks_loaded", &[]),
                )
            };

            let mut phases = BTreeMap::new();

            // Phase 1: 20 series x 5 samples in active head chunks (starting at now - 4*h2)
            let t0 = now - 4 * h2;
            let mut req = cortexpb::WriteRequest {
                source: cortexpb::write_request::SourceEnum::Api as i32,
                ..Default::default()
            };
            for i in 0..20 {
                req.timeseries.push(cortexpb::TimeSeries {
                    labels: vec![lp("__name__", "chk_m"), lp("i", &format!("{i}"))],
                    samples: (0..5)
                        .map(|k| cortexpb::Sample {
                            value: k as f64,
                            timestamp_ms: t0 + k * 1000,
                        })
                        .collect(),
                    ..Default::default()
                });
            }
            client.push(with_org("t-chk", req)).await.unwrap();
            phases.insert("1_initial_head".into(), snap(&scrape_text(&http, hp).await));

            // Phase 2: advance time past chunk_range (2h) + push 180 samples per series up to now-h2 to force chunk cuts + mmap rollover
            for batch in 1..=3 {
                let mut req = cortexpb::WriteRequest {
                    source: cortexpb::write_request::SourceEnum::Api as i32,
                    ..Default::default()
                };
                for i in 0..20 {
                    req.timeseries.push(cortexpb::TimeSeries {
                        labels: vec![lp("__name__", "chk_m"), lp("i", &format!("{i}"))],
                        samples: (0..60)
                            .map(|k| cortexpb::Sample {
                                value: (batch * 60 + k) as f64,
                                timestamp_ms: t0 + batch * h2 + k * 1000,
                            })
                            .collect(),
                        ..Default::default()
                    });
                }
                client.push(with_org("t-chk", req)).await.unwrap();
            }
            phases.insert("2_after_rollover".into(), snap(&scrape_text(&http, hp).await));

            // Phase 3: OOO samples (creating OOO head chunks + exceeding out_of_order_capacity_max=32)
            for k in 0..40 {
                let mut req = cortexpb::WriteRequest {
                    source: cortexpb::write_request::SourceEnum::Api as i32,
                    ..Default::default()
                };
                for i in 0..10 {
                    req.timeseries.push(cortexpb::TimeSeries {
                        labels: vec![lp("__name__", "chk_m"), lp("i", &format!("{i}"))],
                        samples: vec![cortexpb::Sample {
                            value: k as f64,
                            timestamp_ms: t0 + h2 - 300_000 + k * 1000,
                        }],
                        ..Default::default()
                    });
                }
                client.push(with_org("t-chk", req)).await.unwrap();
            }
            let snap3 = snap(&scrape_text(&http, hp).await);
            if !snap3.ends_with("blocks=0") {
                drop(client);
                proc.sigterm();
                proc.wait_exit(Duration::from_secs(10));
                continue;
            }
            phases.insert("3_after_ooo".into(), snap3);

            // Phase 4: flush/compact
            let resp = http
                .post(format!("http://127.0.0.1:{hp}/ingester/flush?tenant=t-chk&wait=true"))
                .send()
                .await
                .unwrap();
            assert!(resp.status().is_success(), "flush status {}", resp.status());
            phases.insert("4_after_flush".into(), snap(&scrape_text(&http, hp).await));

            // Phase 5: push new head + OOO, restart process, verify WAL/WBL/chunks_head replay
            let mut req = cortexpb::WriteRequest {
                source: cortexpb::write_request::SourceEnum::Api as i32,
                ..Default::default()
            };
            for i in 0..15 {
                req.timeseries.push(cortexpb::TimeSeries {
                    labels: vec![lp("__name__", "chk_post"), lp("i", &format!("{i}"))],
                    samples: vec![
                        cortexpb::Sample { value: 1.0, timestamp_ms: now },
                        cortexpb::Sample { value: 2.0, timestamp_ms: now - 60_000 },
                    ],
                    ..Default::default()
                });
            }
            client.push(with_org("t-chk", req)).await.unwrap();
            phases.insert("5_pre_restart".into(), snap(&scrape_text(&http, hp).await));

            drop(client);
            proc.sigterm();
            proc.wait_exit(Duration::from_secs(10));
            drop(proc);

            let hp2 = free_test_port();
            let gp2 = free_test_port();
            let yaml2 = classic_yaml(hp2, gp2, &dir, "24h");
            let mut proc2 = start_mimir(which, "soak-b-2", &dir, &yaml2, &[]);
            wait_ready(&http, &mut proc2, hp2, Duration::from_secs(15)).await;
            phases.insert("6_post_restart".into(), snap(&scrape_text(&http, hp2).await));
            let mut client2 = connect_grpc(gp2).await;
            let (qs_s, qs_c) = query_stream_counts(
                &mut client2,
                "t-chk",
                0,
                now + 60_000,
                vec![matcher(ingesterpb::MatchType::RegexMatch, "__name__", ".+")],
            )
            .await;
            phases.insert("7_post_restart_query".into(), format!("series={qs_s} chunks={qs_c}"));
            drop(client2);
            proc2.sigterm();
            proc2.wait_exit(Duration::from_secs(10));
            break phases;
            };
            side_phases.push((which.name().to_string(), phases));
        }

        assert_eq!(
            side_phases[1].1, side_phases[0].1,
            "tsdb_head_chunks lifecycle parity mismatch"
        );
    });
}

#[test]
fn twin_process_helm_ingest_storage_dashboard_metrics_parity() {
    let tmp = tempfile::tempdir().unwrap();
    let http = reqwest::Client::new();
    let now = (now_ms() / 1000) * 1000;

    block_on_for_tests(async {
        let mut fams_by_impl: Vec<(String, BTreeSet<String>, BTreeSet<String>)> = Vec::new();
        for which in IMPLS {
            let broker = SpawnedKfakeBroker::spawn(&KfakeBrokerRequest {
                topic: "mimir-ingest".to_string(),
                partitions: 1,
                compression: None,
                records: vec![KfakeRecordInput {
                    partition: 0,
                    tenant: "t-c".to_string(),
                    version: 2,
                    compression: None,
                    write_request: WriteRequestJson {
                        mode: String::new(),
                        source: "API".to_string(),
                        skip_label_validation: false,
                        skip_label_count_validation: false,
                        timeseries: vec![
                            TimeSeriesJson {
                                labels: vec![
                                    LabelJson { name: "__name__".into(), value: "http_reqs".into() },
                                    LabelJson { name: "job".into(), value: "api".into() },
                                ],
                                samples: vec![
                                    SampleJson { timestamp_ms: now, value: 42.0 },
                                    SampleJson { timestamp_ms: now - 60_000, value: 41.0 },
                                ],
                                exemplars: vec![ExemplarJson {
                                    labels: vec![LabelJson { name: "trace_id".into(), value: "abc".into() }],
                                    value: 1.5,
                                    timestamp_ms: now,
                                }],
                                ..Default::default()
                            },
                            TimeSeriesJson {
                                labels: vec![
                                    LabelJson { name: "__name__".into(), value: "http_dur".into() },
                                    LabelJson { name: "job".into(), value: "api".into() },
                                ],
                                histograms: vec![HistogramJson {
                                    timestamp_ms: now,
                                    schema: 0,
                                    zero_threshold: 0.001,
                                    zero_count: 1,
                                    count: 3,
                                    sum: 2.5,
                                    positive_spans: vec![SpanJson { offset: 0, length: 2 }],
                                    positive_deltas: vec![1, 0],
                                    ..Default::default()
                                }],
                                ..Default::default()
                            },
                        ],
                        metadata: vec![MetadataJson {
                            metric_type: "COUNTER".into(),
                            metric_family_name: "http_reqs".into(),
                            help: "requests".into(),
                            unit: String::new(),
                        }],
                    },
                }],
            })
            .unwrap();
            let s3_port = free_test_port();
            let _s3 = FakeS3::start(s3_port, None).await;
            let hp = free_test_port();
            let gp = free_test_port();
            let mp = free_test_port();
            let dir = tmp.path().join(which.name());
            std::fs::create_dir_all(&dir).unwrap();

            let yaml = format!(
                r#"target: ingester
multitenancy_enabled: true
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {hp}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {gp}
ingester:
  push_grpc_method_enabled: false
  active_series_metrics_enabled: true
  active_series_metrics_update_period: 100ms
  ring:
    instance_id: ing-zone-a-0
    instance_addr: 127.0.0.1
    instance_availability_zone: zone-a
    kvstore:
      store: memberlist
      prefix: partition-ingesters/
    replication_factor: 1
    num_tokens: 16
    min_ready_duration: 0s
    final_sleep: 0s
    zone_awareness_enabled: true
  partition_ring:
    kvstore:
      store: memberlist
    min_partition_owners_count: 1
    min_partition_owners_duration: 0s
memberlist:
  bind_addr: [127.0.0.1]
  bind_port: {mp}
  abort_if_cluster_join_fails: false
ingest_storage:
  enabled: true
  kafka:
    address: {kafka_addr}
    topic: mimir-ingest
    auto_create_topic_enabled: false
    consume_from_position_at_startup: start
    wait_strong_read_consistency_timeout: 2s
blocks_storage:
  backend: s3
  s3:
    endpoint: 127.0.0.1:{s3_port}
    bucket_name: mimir-tsdb
    access_key_id: test
    secret_access_key: test
    insecure: true
  tsdb:
    dir: {dir}/tsdb
    head_compaction_interval: 15m
    ship_interval: 1s
    retention_period: 24h
limits:
  max_global_series_per_user: 0
  max_global_series_per_metric: 0
  max_global_exemplars_per_user: 100
  out_of_order_time_window: 24h
  native_histograms_ingestion_enabled: true
"#,
                kafka_addr = broker.broker_addr(),
                dir = dir.display()
            );

            let mut proc = start_mimir(which, "diag-c", &dir, &yaml, &[]);
            wait_ready(&http, &mut proc, hp, Duration::from_secs(20)).await;

            // Wait for Kafka consumption + active series ticker, then run all read RPCs BEFORE flush
            let mut client = connect_grpc(gp).await;
            let deadline = Instant::now() + Duration::from_secs(10);
            loop {
                let (s, _) = query_stream_counts(
                    &mut client,
                    "t-c",
                    now - 120_000,
                    now + 60_000,
                    vec![matcher(ingesterpb::MatchType::RegexMatch, "__name__", ".+")],
                )
                .await;
                if s >= 2 || Instant::now() >= deadline {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            tokio::time::sleep(Duration::from_millis(300)).await;

            // Exercise all read RPCs while data is in Head
            let _ = client
                .query_exemplars(with_org(
                    "t-c",
                    ingesterpb::ExemplarQueryRequest {
                        start_timestamp_ms: 0,
                        end_timestamp_ms: now + 60_000,
                        matchers: vec![ingesterpb::LabelMatchers {
                            matchers: vec![matcher(ingesterpb::MatchType::RegexMatch, "__name__", ".+")],
                        }],
                    },
                ))
                .await
                .unwrap();
            let _ = client
                .label_names(with_org(
                    "t-c",
                    ingesterpb::LabelNamesRequest {
                        start_timestamp_ms: 0,
                        end_timestamp_ms: now + 60_000,
                        ..Default::default()
                    },
                ))
                .await
                .unwrap();
            let _ = client
                .metrics_for_label_matchers(with_org(
                    "t-c",
                    ingesterpb::MetricsForLabelMatchersRequest {
                        start_timestamp_ms: 0,
                        end_timestamp_ms: now + 60_000,
                        matchers_set: vec![ingesterpb::LabelMatchers {
                            matchers: vec![matcher(ingesterpb::MatchType::Equal, "job", "api")],
                        }],
                        limit: 0,
                    },
                ))
                .await
                .unwrap();
            let _ = client
                .metrics_metadata(with_org("t-c", ingesterpb::MetricsMetadataRequest::default()))
                .await
                .unwrap();
            let _ = client
                .user_stats(with_org("t-c", ingesterpb::UserStatsRequest::default()))
                .await
                .unwrap();
            let _ = client
                .all_user_stats(with_org("t-c", ingesterpb::UserStatsRequest::default()))
                .await
                .unwrap();
            let mut sln = client
                .search_label_names(with_org(
                    "t-c",
                    ingesterpb::SearchLabelNamesRequest {
                        start_timestamp_ms: 0,
                        end_timestamp_ms: now + 60_000,
                        ..Default::default()
                    },
                ))
                .await
                .unwrap()
                .into_inner();
            while sln.next().await.is_some() {}
            let mut slv = client
                .search_label_values(with_org(
                    "t-c",
                    ingesterpb::SearchLabelValuesRequest {
                        name: "job".into(),
                        start_timestamp_ms: 0,
                        end_timestamp_ms: now + 60_000,
                        ..Default::default()
                    },
                ))
                .await
                .unwrap()
                .into_inner();
            while slv.next().await.is_some() {}
            let mut acs = client
                .active_series(with_org(
                    "t-c",
                    ingesterpb::ActiveSeriesRequest::default(),
                ))
                .await
                .unwrap()
                .into_inner();
            while acs.next().await.is_some() {}

            // Capture pre-flush families (with active_series & head populated)
            let pre_text = scrape_text(&http, hp).await;
            let pre_fams = text_families(&pre_text);

            // Flush & ship block
            let _ = http
                .post(format!("http://127.0.0.1:{hp}/ingester/flush?tenant=t-c&wait=true"))
                .send()
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(2000)).await;

            // Run QueryStream + LabelValues + LabelNamesAndValues after flush
            let _ = query_stream_counts(
                &mut client,
                "t-c",
                0,
                now + 60_000,
                vec![matcher(ingesterpb::MatchType::RegexMatch, "__name__", ".+")],
            )
            .await;
            let _ = client
                .label_values(with_org(
                    "t-c",
                    ingesterpb::LabelValuesRequest {
                        label_name: "job".into(),
                        start_timestamp_ms: 0,
                        end_timestamp_ms: now + 60_000,
                        matchers: None,
                        limit: 0,
                    },
                ))
                .await
                .unwrap();
            let mut lnv = client
                .label_names_and_values(with_org(
                    "t-c",
                    ingesterpb::LabelNamesAndValuesRequest::default(),
                ))
                .await
                .unwrap()
                .into_inner();
            while lnv.next().await.is_some() {}

            let text = scrape_text(&http, hp).await;
            let proto = scrape_proto(&http, hp).await;
            let mut tf = text_families(&text);
            tf.extend(pre_fams);
            let pf: BTreeSet<String> = proto.into_iter().map(|f| f.name).filter(|n| !n.is_empty()).collect();
            fams_by_impl.push((which.name().to_string(), tf, pf));
        }

        let go_tf = &fams_by_impl[0].1;
        let rust_tf = &fams_by_impl[1].1;
        let go_pf = &fams_by_impl[0].2;
        let rust_pf = &fams_by_impl[1].2;

        const NON_INGESTER_GO_PACKAGE_GLOBALS: &[&str] = &[
            "cortex_distributor_ingester_clients",
            "deprecated_flags_inuse_total",
            "prometheus_remote_storage_exemplars_in_total",
            "prometheus_remote_storage_histograms_in_total",
            "prometheus_remote_storage_samples_in_total",
            "prometheus_remote_storage_string_interner_zero_reference_releases_total",
            "prometheus_template_text_expansion_failures_total",
            "prometheus_template_text_expansions_total",
        ];

        let missing_text: Vec<&String> = go_tf
            .difference(rust_tf)
            .filter(|n| !n.starts_with("go_") && !NON_INGESTER_GO_PACKAGE_GLOBALS.contains(&n.as_str()))
            .collect();
        let extra_text: Vec<&String> = rust_tf
            .difference(go_tf)
            .filter(|n| !n.starts_with("go_"))
            .collect();
        let missing_proto: Vec<&String> = go_pf
            .difference(rust_pf)
            .filter(|n| !n.starts_with("go_") && !NON_INGESTER_GO_PACKAGE_GLOBALS.contains(&n.as_str()))
            .collect();

        assert!(
            missing_text.is_empty(),
            "Rust missing Go metric families (text): {missing_text:?}"
        );
        assert!(
            extra_text.is_empty(),
            "Rust exposes extra metric families not in Go (text): {extra_text:?}"
        );
        assert!(
            missing_proto.is_empty(),
            "Rust missing Go metric families (proto): {missing_proto:?}"
        );

        // Verify key stock Mimir dashboard metric families are present in both text & protobuf.
        for required in [
            "cortex_ingester_active_series",
            "cortex_ingester_active_native_histogram_series",
            "cortex_ingester_active_native_histogram_buckets",
            "cortex_ingester_ingested_samples_total",
            "cortex_ingester_ingested_exemplars_total",
            "cortex_ingester_ingested_metadata_total",
            "cortex_ingester_memory_series",
            "cortex_ingester_memory_users",
            "cortex_ingester_memory_metadata",
            "cortex_ingester_queried_series",
            "cortex_ingester_queried_samples",
            "cortex_ingester_queried_exemplars",
            "cortex_ingester_queried_blocks_total",
            "cortex_ingester_tsdb_head_chunks",
            "cortex_ingester_tsdb_head_chunks_created_total",
            "cortex_ingester_tsdb_head_chunks_removed_total",
            "cortex_ingester_tsdb_compactions_total",
            "cortex_ingester_tsdb_compaction_duration_seconds",
            "cortex_ingester_tsdb_out_of_order_samples_appended_total",
            "cortex_ingester_shipper_uploads_total",
            "cortex_ingester_shipper_last_successful_upload_timestamp_seconds",
            "cortex_ingest_storage_reader_fetch_records_total",
            "cortex_ingest_storage_reader_records_per_fetch",
            "cortex_ingest_storage_reader_last_consumed_offset",
            "cortex_partition_ring_partitions",
            "cortex_ring_members",
            "memberlist_client_cluster_members_count",
            "thanos_objstore_bucket_operations_total",
        ] {
            assert!(rust_tf.contains(required), "Rust text missing dashboard metric {required}");
            assert!(rust_pf.contains(required), "Rust proto missing dashboard metric {required}");
        }
    });
}
