//! Wave 3 TSDB & Ingester twin-process differential conformance suite (`target/go-mimir` vs
//! `target/release/mimir`), verifying zero divergence across all 11 Wave 3 findings plus
//! bidirectional `SIGKILL` WAL/WBL/`chunks_head` handoff:
//!
//! - W3-01: `HeadAppender` preserves append order between floats and histograms within a single
//!   `WriteRequest` (`[Histogram@t, Float(StaleNaN)@t+1000]`, `[FloatHistogram@t, Float(StaleNaN)@t+1000]`,
//!   and `[Histogram@t, Float(42.0)@t+1000]`) under both `out_of_order_time_window: 0s` and `> 0`.
//! - W3-02: OOO `Float(StaleNaN)` on histogram / float-histogram series converts to a stale
//!   histogram in OOO head + WBL instead of being dropped.
//! - W3-03: `QueryStream` returns untrimmed `ooo_head_chunk` (`DisableTrimming = true`) on gap,
//!   partial-overlap, and exact-boundary ranges.
//! - W3-04: `LabelNames`, `LabelValues`, `SearchLabelNames`, and `SearchLabelValues` use in-order
//!   `[head.MinTime(), head.MaxTime()]` bounds for Head label discovery (never leaking Head labels
//!   into historical block ranges or pre-`Head.MinTime()` OOO windows).
//! - W3-05: `OooChunk::to_encoded_chunks` honours per-tenant `float_chunk_encoding: xor2` (`7`)
//!   and `histogram_chunk_encoding: histogram_st` (`8`/`9`) on both `QueryStream` and `chunks_head`
//!   OOO rollover files.
//! - W3-08: Unsorted-label series with 0 samples/histograms (exemplar-only) return `200 OK`;
//!   unsorted-label series with histograms when `native_histograms_ingestion_enabled: false`
//!   include `len(ts.Histograms)` in `ErrorDetails.rejected_samples`.
//! - W3-09: `LabelNamesAndValues(CountMethod=ACTIVE)` with matchers uses
//!   `activeseries.IsLabelValueActive` semantics (any active series in Head carrying `name=val`).
//! - W3-10: `/ingester/label_churn/:tenant` `503 Service Unavailable` responses carry Go's exact
//!   `NoRecord` and `CompactedTooRecently` `http.Error` bodies.
//! - W3-13: `ActiveSeries` invalid `__query_shard__` error carries Go's exact
//!   `"error listing active series: error removing shard matcher: ..."` prefix.
//! - W3-14: Unknown `CountMethod` error quotes the numeric value (`unknown count method "99"`).
//! - W3-15: Duplicate-timestamp float error formats `format_go_g` identically to Go `fmt "%g"`
//!   (`1e+06`, `1.5e-05`, `-0` for `-0.0`).
//! - Bidirectional `Go -> SIGKILL -> Rust` and `Rust -> SIGKILL -> Go` TSDB WAL/WBL/`chunks_head`
//!   crash-recovery handoff across both default and `xor2`/`histogram_st` encodings.

#![allow(clippy::result_large_err)]

use std::collections::BTreeMap;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use mimir_common::block_on_for_tests;
use mimir_conformance::free_test_port;
use mimir_conformance::twin::{http_get, start_mimir, wait_ready, Impl, Proc};
use mimir_proto::cortexpb;
use mimir_proto::grpcutilpb::extract_cortex_error_details;
use mimir_proto::ingesterpb;
use mimir_proto::ingesterpb::ingester_client::IngesterClient;
use mimir_proto::prost::Message as _;
use mimir_tsdb::chunkenc::{stale_nan, EncodedChunk, Encoding};
use mimir_tsdb::compact::SampleValue;
use mimir_tsdb::querier::chunk_to_samples;
use tokio_stream::StreamExt;
use tonic::codec::{Codec, DecodeBuf, EncodeBuf};
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::Status;

const IMPLS: [Impl; 2] = [Impl::Go, Impl::Rust];

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_millis() as i64
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
        .insert("x-scope-orgid", MetadataValue::try_from(org).expect("ascii org"));
    req
}

fn matcher(t: ingesterpb::MatchType, name: &str, value: &str) -> ingesterpb::LabelMatcher {
    ingesterpb::LabelMatcher {
        r#type: t as i32,
        name: name.to_string(),
        value: value.to_string(),
    }
}

fn eq(name: &str, value: &str) -> ingesterpb::LabelMatcher {
    matcher(ingesterpb::MatchType::Equal, name, value)
}

fn re(name: &str, value: &str) -> ingesterpb::LabelMatcher {
    matcher(ingesterpb::MatchType::RegexMatch, name, value)
}

fn span(offset: i32, length: u32) -> cortexpb::BucketSpan {
    cortexpb::BucketSpan { offset, length }
}

fn make_ih(ts: i64, d0: i64) -> cortexpb::Histogram {
    cortexpb::Histogram {
        count: Some(cortexpb::histogram::Count::CountInt(d0 as u64)),
        zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountInt(0)),
        sum: d0 as f64 * 1.5,
        schema: 0,
        positive_spans: vec![span(0, 1)],
        positive_deltas: vec![d0],
        timestamp: ts,
        ..Default::default()
    }
}

fn make_fh(ts: i64, c0: f64) -> cortexpb::Histogram {
    cortexpb::Histogram {
        count: Some(cortexpb::histogram::Count::CountFloat(c0)),
        zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountFloat(0.0)),
        sum: c0 * 1.5,
        schema: 0,
        positive_spans: vec![span(0, 1)],
        positive_counts: vec![c0],
        timestamp: ts,
        ..Default::default()
    }
}

fn make_rich_int_hist(
    ts: i64,
    reset_hint: i32,
    sum: f64,
    pos_delta: i64,
    neg_delta: i64,
) -> cortexpb::Histogram {
    let b0 = pos_delta;
    let b1 = pos_delta + 1;
    let total = (1 + b0 + b1 + neg_delta) as u64;
    cortexpb::Histogram {
        count: Some(cortexpb::histogram::Count::CountInt(total)),
        zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountInt(1)),
        zero_threshold: 0.001,
        sum,
        schema: 2,
        positive_spans: vec![span(-1, 2)],
        positive_deltas: vec![pos_delta, 1],
        negative_spans: vec![span(0, 1)],
        negative_deltas: vec![neg_delta],
        reset_hint,
        timestamp: ts,
        ..Default::default()
    }
}

fn make_rich_float_hist(
    ts: i64,
    reset_hint: i32,
    sum: f64,
    pos_cnt: f64,
    neg_cnt: f64,
) -> cortexpb::Histogram {
    let total = 0.5 + pos_cnt + 1.5 + neg_cnt;
    cortexpb::Histogram {
        count: Some(cortexpb::histogram::Count::CountFloat(total)),
        zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountFloat(0.5)),
        zero_threshold: 0.01,
        sum,
        schema: 1,
        positive_spans: vec![span(0, 2)],
        positive_counts: vec![pos_cnt, 1.5],
        negative_spans: vec![span(1, 1)],
        negative_counts: vec![neg_cnt],
        reset_hint,
        timestamp: ts,
        ..Default::default()
    }
}

fn make_nhcb_int(ts: i64, sum: f64, d0: i64, d1: i64) -> cortexpb::Histogram {
    let b0 = d0;
    let b1 = d0 + d1;
    let total = (b0 + b1) as u64;
    cortexpb::Histogram {
        count: Some(cortexpb::histogram::Count::CountInt(total)),
        zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountInt(0)),
        sum,
        schema: -53,
        positive_spans: vec![span(0, 2)],
        positive_deltas: vec![d0, d1],
        custom_values: vec![0.1, 0.5, 2.5],
        timestamp: ts,
        ..Default::default()
    }
}

fn make_nhcb_float(ts: i64, count: f64, sum: f64, c0: f64, c1: f64) -> cortexpb::Histogram {
    cortexpb::Histogram {
        count: Some(cortexpb::histogram::Count::CountFloat(count)),
        zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountFloat(0.0)),
        sum,
        schema: -53,
        positive_spans: vec![span(0, 2)],
        positive_counts: vec![c0, c1],
        custom_values: vec![1.0, 10.0, 100.0],
        timestamp: ts,
        ..Default::default()
    }
}

fn fmt_status<T>(res: &Result<T, Status>) -> String {
    match res {
        Ok(_) => "OK".to_string(),
        Err(st) => {
            let det = extract_cortex_error_details(st).map(|ed| {
                format!(
                    "cause={},soft={},rejected={}",
                    ed.cause, ed.soft, ed.rejected_samples
                )
            });
            format!(
                "code={:?}|msg={}|details={:?}",
                st.code(),
                st.message(),
                det
            )
        }
    }
}

fn fmt_read_err<T>(res: &Result<T, Status>) -> String {
    match res {
        Ok(_) => "OK".to_string(),
        Err(st) => format!(
            "code={:?}|msg={}|details_empty={}",
            st.code(),
            st.message(),
            st.details().is_empty()
        ),
    }
}

fn wire_enc_to_tsdb(enc: i32) -> Encoding {
    match enc {
        4 => Encoding::Xor,
        5 => Encoding::Histogram,
        6 => Encoding::FloatHistogram,
        7 => Encoding::Xor2,
        8 => Encoding::HistogramSt,
        9 => Encoding::FloatHistogramSt,
        other => panic!("unexpected wire chunk encoding {other}"),
    }
}

fn format_labels(pairs: &[cortexpb::LabelPair]) -> String {
    let mut parts = Vec::with_capacity(pairs.len());
    for p in pairs {
        let k = std::str::from_utf8(&p.name).expect("utf8 label name");
        let v = std::str::from_utf8(&p.value).expect("utf8 label val");
        parts.push(format!("{k}={v:?}"));
    }
    format!("{{{}}}", parts.join(", "))
}

async fn query_stream_summary(
    ing: &mut IngesterClient<Channel>,
    tenant: &str,
    start: i64,
    end: i64,
    matchers: Vec<ingesterpb::LabelMatcher>,
) -> String {
    let mut stream = ing
        .query_stream(with_org(
            tenant,
            ingesterpb::QueryRequest {
                start_timestamp_ms: start,
                end_timestamp_ms: end,
                matchers,
                ..Default::default()
            },
        ))
        .await
        .expect("QueryStream")
        .into_inner();

    let mut series_desc = Vec::new();
    let mut chunks_by_idx: BTreeMap<u64, Vec<ingesterpb::Chunk>> = BTreeMap::new();
    while let Some(msg) = stream.next().await {
        let msg = msg.expect("QueryStream frame");
        for s in msg.streaming_series {
            series_desc.push(format!("{}(cc={})", format_labels(&s.labels), s.chunk_count));
        }
        for sc in msg.streaming_series_chunks {
            chunks_by_idx
                .entry(sc.series_index)
                .or_default()
                .extend(sc.chunks);
        }
    }

    let mut out = Vec::new();
    for (idx, s_name) in series_desc.into_iter().enumerate() {
        let chks = chunks_by_idx.remove(&(idx as u64)).unwrap_or_default();
        let mut c_desc = Vec::new();
        for c in chks {
            let enc = wire_enc_to_tsdb(c.encoding);
            let parsed = EncodedChunk::from_bytes(enc, c.data.to_vec()).expect("decode chunk");
            let samples = chunk_to_samples(&parsed).expect("iter chunk");
            let sample_strs: Vec<String> = samples
                .into_iter()
                .map(|s| match s.value {
                    SampleValue::Float(v) => format!("F({}:{:016x})", s.t, v.to_bits()),
                    SampleValue::Histogram(h) => {
                        format!("IH({}:cnt={},sum={:016x})", s.t, h.count, h.sum.to_bits())
                    }
                    SampleValue::FloatHistogram(fh) => {
                        format!("FH({}:cnt={},sum={:016x})", s.t, fh.count, fh.sum.to_bits())
                    }
                })
                .collect();
            c_desc.push(format!(
                "[{}..{} enc={} samples={:?}]",
                c.start_timestamp_ms, c.end_timestamp_ms, c.encoding, sample_strs
            ));
        }
        out.push(format!("{s_name}->{c_desc:?}"));
    }
    out.join(" ; ")
}

/// Raw byte passthrough codec for sending explicit `-0.0` wire bytes (`0x8000_0000_0000_0000`)
/// without proto3 default-zero elision.
#[derive(Clone, Default)]
struct RawBytesCodec;

struct RawEncoder;
struct RawDecoder;

impl tonic::codec::Encoder for RawEncoder {
    type Item = Vec<u8>;
    type Error = Status;

    fn encode(&mut self, item: Self::Item, dst: &mut EncodeBuf<'_>) -> Result<(), Self::Error> {
        dst.put_slice(&item);
        Ok(())
    }
}

impl tonic::codec::Decoder for RawDecoder {
    type Item = Vec<u8>;
    type Error = Status;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        let rem = src.remaining();
        if rem == 0 {
            return Ok(None);
        }
        let mut out = vec![0u8; rem];
        src.copy_to_slice(&mut out);
        Ok(Some(out))
    }
}

impl Codec for RawBytesCodec {
    type Encode = Vec<u8>;
    type Decode = Vec<u8>;
    type Encoder = RawEncoder;
    type Decoder = RawDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        RawEncoder
    }

    fn decoder(&mut self) -> Self::Decoder {
        RawDecoder
    }
}

async fn push_raw_bytes(ch: Channel, org: &str, payload: Vec<u8>) -> Result<Vec<u8>, Status> {
    let mut grpc = tonic::client::Grpc::new(ch);
    grpc.ready().await.expect("grpc ready");
    let path = tonic::codegen::http::uri::PathAndQuery::from_static("/cortex.Ingester/Push");
    grpc.unary(with_org(org, payload), path, RawBytesCodec)
        .await
        .map(|r| r.into_inner())
}

/// Builds a valid protobuf `WriteRequest` containing a single `Sample { value: -0.0, timestamp_ms }`
/// with field 1 (`0x09`) explicitly encoded on the wire as `0x8000_0000_0000_0000`.
fn encode_explicit_neg_zero_write_request(metric: &str, ts: i64) -> Vec<u8> {
    let sentinel = f64::from_bits(0x3ff5_5555_5555_5555);
    let wr = cortexpb::WriteRequest {
        timeseries: vec![cortexpb::TimeSeries {
            labels: vec![lp("__name__", metric)],
            samples: vec![cortexpb::Sample {
                value: sentinel,
                timestamp_ms: ts,
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    wr.encode(&mut buf).expect("encode WriteRequest");
    let mut raw = buf.to_vec();
    let needle = sentinel.to_bits().to_le_bytes();
    let neg_zero = (-0.0_f64).to_bits().to_le_bytes();
    let pos = raw
        .windows(8)
        .position(|w| w == needle)
        .expect("locate sentinel double bytes");
    raw[pos..pos + 8].copy_from_slice(&neg_zero);
    raw
}

struct RunningIngester {
    proc: Proc,
    http_port: u16,
    ch: Channel,
    ing: IngesterClient<Channel>,
}

impl RunningIngester {
    async fn stop(mut self) {
        self.proc.sigterm();
        let _ = self.proc.wait_exit(Duration::from_secs(10));
    }

    fn kill9(&mut self) {
        self.proc.signal("KILL");
        let _ = self.proc.wait_exit(Duration::from_secs(5));
    }
}

async fn boot_wave3_ingester(
    which: Impl,
    name: &str,
    dir: &Path,
    http_client: &reqwest::Client,
) -> RunningIngester {
    let tsdb_dir = dir.join("tsdb");
    let blocks_dir = dir.join("blocks");
    std::fs::create_dir_all(&tsdb_dir).unwrap();
    std::fs::create_dir_all(&blocks_dir).unwrap();

    let rt_path = dir.join("runtime.yaml");
    let rt_yaml = r#"overrides:
  t-no-ooo:
    out_of_order_time_window: 0s
    native_histograms_ingestion_enabled: true
    max_global_exemplars_per_user: 100
  t-no-nh:
    native_histograms_ingestion_enabled: false
    max_global_exemplars_per_user: 100
  t-enc:
    float_chunk_encoding: xor2
    histogram_chunk_encoding: histogram_st
    native_histograms_ingestion_enabled: true
    out_of_order_time_window: 2h
    max_global_exemplars_per_user: 500
  t-rich:
    native_histograms_ingestion_enabled: true
    out_of_order_time_window: 2h
    max_global_exemplars_per_user: 500
"#;
    std::fs::write(&rt_path, rt_yaml).unwrap();

    let http_port = free_test_port();
    let grpc_port = free_test_port();
    let tokens_path = dir.join("tokens");
    let yaml = format!(
        r#"target: ingester
multitenancy_enabled: true
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http_port}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc_port}
  log_level: info
ingester:
  active_series_metrics_enabled: true
  active_series_metrics_idle_timeout: 500ms
  active_series_metrics_update_period: 150ms
  label_churn_enabled: true
  error_sample_rate: 0
  ring:
    instance_id: ingester-0
    instance_addr: 127.0.0.1
    kvstore:
      store: inmemory
    replication_factor: 1
    num_tokens: 16
    tokens_file_path: {}
    unregister_on_shutdown: false
    min_ready_duration: 0s
    final_sleep: 0s
blocks_storage:
  backend: filesystem
  filesystem:
    dir: {}
  tsdb:
    dir: {}
    ship_interval: 1h
    retention_period: 24h
    wal_compression_enabled: true
runtime_config:
  file: {}
  period: 200ms
limits:
  native_histograms_ingestion_enabled: true
  out_of_order_time_window: 2h
  max_global_exemplars_per_user: 200
  creation_grace_period: 24h
"#,
        tokens_path.display(),
        blocks_dir.display(),
        tsdb_dir.display(),
        rt_path.display(),
    );

    let mut proc = start_mimir(which, name, dir, &yaml, &[]);
    wait_ready(http_client, &mut proc, http_port, Duration::from_secs(25)).await;
    let ch = Channel::from_shared(format!("http://127.0.0.1:{grpc_port}"))
        .unwrap()
        .connect()
        .await
        .expect("connect ingester gRPC");
    let ing = IngesterClient::new(ch.clone());
    RunningIngester {
        proc,
        http_port,
        ch,
        ing,
    }
}

#[test]
fn wave3_push_read_rpcs_label_churn_and_error_parity() {
    block_on_for_tests(async {
        let tmp = tempfile::tempdir().unwrap();
        let http_client = reqwest::Client::new();

        let mut pair = Vec::new();
        for &which in &IMPLS {
            let d = tmp.path().join(which.name());
            pair.push(boot_wave3_ingester(which, "w3-live", &d, &http_client).await);
        }
        let [mut go, mut rust]: [RunningIngester; 2] = pair.try_into().ok().unwrap();

        let now = (now_ms() / 60_000) * 60_000;
        let min_q = now - 3_600_000;
        let max_q = now + 3_600_000;
        let stale = stale_nan();

        // =========================================================================
        // 1. W3-01: [Histogram@t, Float(StaleNaN)@t+1000], [FloatHistogram@t, StaleNaN@t+1000],
        //    and [Histogram@t, Float(42.0)@t+1000] in ONE WriteRequest!
        // =========================================================================
        for (tenant, suffix) in [("t-no-ooo", "no_ooo"), ("t-ooo", "with_ooo")] {
            let ih_metric = format!("w3_01_ih_{suffix}");
            let fh_metric = format!("w3_01_fh_{suffix}");
            let mix_metric = format!("w3_01_mix_{suffix}");
            let req = cortexpb::WriteRequest {
                timeseries: vec![
                    cortexpb::TimeSeries {
                        labels: vec![lp("__name__", &ih_metric)],
                        histograms: vec![make_ih(now, 7)],
                        ..Default::default()
                    },
                    cortexpb::TimeSeries {
                        labels: vec![lp("__name__", &ih_metric)],
                        samples: vec![cortexpb::Sample {
                            value: stale,
                            timestamp_ms: now + 1_000,
                        }],
                        ..Default::default()
                    },
                    cortexpb::TimeSeries {
                        labels: vec![lp("__name__", &fh_metric)],
                        histograms: vec![make_fh(now, 5.5)],
                        ..Default::default()
                    },
                    cortexpb::TimeSeries {
                        labels: vec![lp("__name__", &fh_metric)],
                        samples: vec![cortexpb::Sample {
                            value: stale,
                            timestamp_ms: now + 1_000,
                        }],
                        ..Default::default()
                    },
                    cortexpb::TimeSeries {
                        labels: vec![lp("__name__", &mix_metric)],
                        histograms: vec![make_ih(now, 4)],
                        ..Default::default()
                    },
                    cortexpb::TimeSeries {
                        labels: vec![lp("__name__", &mix_metric)],
                        samples: vec![cortexpb::Sample {
                            value: 42.0,
                            timestamp_ms: now + 1_000,
                        }],
                        ..Default::default()
                    },
                ],
                ..Default::default()
            };

            let g_push = go.ing.push(with_org(tenant, req.clone())).await;
            let r_push = rust.ing.push(with_org(tenant, req)).await;
            assert_eq!(fmt_status(&g_push), "OK");
            assert_eq!(fmt_status(&g_push), fmt_status(&r_push));

            for m in [&ih_metric, &fh_metric, &mix_metric] {
                let g_q = query_stream_summary(
                    &mut go.ing,
                    tenant,
                    min_q,
                    max_q,
                    vec![eq("__name__", m)],
                )
                .await;
                let r_q = query_stream_summary(
                    &mut rust.ing,
                    tenant,
                    min_q,
                    max_q,
                    vec![eq("__name__", m)],
                )
                .await;
                assert!(!g_q.is_empty(), "expected non-empty series for {m}");
                assert_eq!(g_q, r_q, "W3-01 mismatch on {m} ({tenant})");
            }
        }

        // =========================================================================
        // 2. W3-02: OOO Float(StaleNaN) on Histogram & FloatHistogram series
        // =========================================================================
        for side in [&mut go, &mut rust] {
            side.ing
                .push(with_org(
                    "t-ooo",
                    cortexpb::WriteRequest {
                        timeseries: vec![
                            cortexpb::TimeSeries {
                                labels: vec![lp("__name__", "w3_02_ih_ooo_stale")],
                                histograms: vec![make_ih(now + 2_000, 3)],
                                ..Default::default()
                            },
                            cortexpb::TimeSeries {
                                labels: vec![lp("__name__", "w3_02_fh_ooo_stale")],
                                histograms: vec![make_fh(now + 2_000, 3.5)],
                                ..Default::default()
                            },
                        ],
                        ..Default::default()
                    },
                ))
                .await
                .expect("W3-02 seed histograms");

            side.ing
                .push(with_org(
                    "t-ooo",
                    cortexpb::WriteRequest {
                        timeseries: vec![
                            cortexpb::TimeSeries {
                                labels: vec![lp("__name__", "w3_02_ih_ooo_stale")],
                                samples: vec![cortexpb::Sample {
                                    value: stale,
                                    timestamp_ms: now + 1_000,
                                }],
                                ..Default::default()
                            },
                            cortexpb::TimeSeries {
                                labels: vec![lp("__name__", "w3_02_fh_ooo_stale")],
                                samples: vec![cortexpb::Sample {
                                    value: stale,
                                    timestamp_ms: now + 1_000,
                                }],
                                ..Default::default()
                            },
                        ],
                        ..Default::default()
                    },
                ))
                .await
                .expect("W3-02 push OOO StaleNaN");
        }
        for m in ["w3_02_ih_ooo_stale", "w3_02_fh_ooo_stale"] {
            let g_q = query_stream_summary(
                &mut go.ing,
                "t-ooo",
                min_q,
                max_q,
                vec![eq("__name__", m)],
            )
            .await;
            let r_q = query_stream_summary(
                &mut rust.ing,
                "t-ooo",
                min_q,
                max_q,
                vec![eq("__name__", m)],
            )
            .await;
            assert!(g_q.contains("7ff0000000000002"), "expected stale sum in {g_q}");
            assert_eq!(g_q, r_q, "W3-02 mismatch on {m}");
        }

        // =========================================================================
        // 3. W3-03 & W3-04: Compacted block + Head + OOO-before-Head.MinTime()
        //    - W3-03: QueryStream untrimmed ooo_head_chunk across gap/partial/exact
        //    - W3-04: LabelNames / LabelValues / SearchLabelNames / SearchLabelValues
        //             on historical block range AND fresh-head pre-MinTime OOO range
        // =========================================================================
        let b0 = now - 4 * 3_600_000;
        let t_blk1 = b0 + 1_000;
        let t_blk2 = b0 + 2_000;
        let t_head_old1 = now - 30 * 60_000;
        let t_head_old2 = t_head_old1 + 1_000;
        let t_head_new = now - 5 * 60_000;
        let t_ooo1 = t_head_old1 - 10 * 60_000;
        let t_ooo2 = t_ooo1 + 2_000;

        for side in [&mut go, &mut rust] {
            // Phase 1: push historical block series and flush to create compacted ULID block.
            side.ing
                .push(with_org(
                    "t-blk-ooo",
                    cortexpb::WriteRequest {
                        timeseries: vec![cortexpb::TimeSeries {
                            labels: vec![
                                lp("__name__", "blk_metric"),
                                lp("blk_lbl", "v_blk"),
                                lp("shared", "s_blk"),
                            ],
                            samples: vec![
                                cortexpb::Sample {
                                    value: 10.0,
                                    timestamp_ms: t_blk1,
                                },
                                cortexpb::Sample {
                                    value: 20.0,
                                    timestamp_ms: t_blk2,
                                },
                            ],
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                ))
                .await
                .expect("push block series");
            let (code, _, _) = http_get(
                &http_client,
                side.http_port,
                "/ingester/flush?tenant=t-blk-ooo&wait=true",
                false,
            )
            .await
            .expect("flush tenant");
            assert_eq!(code, 204);

            // Phase 2: push in-order Head series.
            side.ing
                .push(with_org(
                    "t-blk-ooo",
                    cortexpb::WriteRequest {
                        timeseries: vec![
                            cortexpb::TimeSeries {
                                labels: vec![
                                    lp("__name__", "head_old_m"),
                                    lp("ho_lbl", "v_ho"),
                                    lp("shared", "s_ho"),
                                ],
                                samples: vec![
                                    cortexpb::Sample {
                                        value: 1.0,
                                        timestamp_ms: t_head_old1,
                                    },
                                    cortexpb::Sample {
                                        value: 2.0,
                                        timestamp_ms: t_head_old2,
                                    },
                                ],
                                ..Default::default()
                            },
                            cortexpb::TimeSeries {
                                labels: vec![
                                    lp("__name__", "ooo_m"),
                                    lp("ooo_lbl", "v_ooo"),
                                    lp("shared", "s_ooo"),
                                ],
                                samples: vec![cortexpb::Sample {
                                    value: 100.0,
                                    timestamp_ms: t_head_new,
                                }],
                                ..Default::default()
                            },
                        ],
                        ..Default::default()
                    },
                ))
                .await
                .expect("push head series");

            // Phase 3: push OOO samples at [t_ooo1, t_ooo2] strictly before Head.MinTime().
            side.ing
                .push(with_org(
                    "t-blk-ooo",
                    cortexpb::WriteRequest {
                        timeseries: vec![cortexpb::TimeSeries {
                            labels: vec![
                                lp("__name__", "ooo_m"),
                                lp("ooo_lbl", "v_ooo"),
                                lp("shared", "s_ooo"),
                            ],
                            samples: vec![
                                cortexpb::Sample {
                                    value: 50.0,
                                    timestamp_ms: t_ooo1,
                                },
                                cortexpb::Sample {
                                    value: 60.0,
                                    timestamp_ms: t_ooo2,
                                },
                            ],
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                ))
                .await
                .expect("push OOO samples");
        }

        // W3-03: QueryStream untrimmed ooo_head_chunk checks!
        for (label, s_ts, e_ts) in [
            ("ooo_gap", t_ooo1 + 500, t_ooo2 - 500),
            ("ooo_partial_overlap", t_ooo2 - 500, t_ooo2 + 500),
            ("ooo_exact_start", t_ooo1, t_ooo1),
            ("blk_only", t_blk1, t_blk2),
        ] {
            let g_qs = query_stream_summary(
                &mut go.ing,
                "t-blk-ooo",
                s_ts,
                e_ts,
                vec![re("__name__", ".+")],
            )
            .await;
            let r_qs = query_stream_summary(
                &mut rust.ing,
                "t-blk-ooo",
                s_ts,
                e_ts,
                vec![re("__name__", ".+")],
            )
            .await;
            assert!(!g_qs.is_empty(), "expected non-empty QueryStream on {label}");
            assert_eq!(g_qs, r_qs, "W3-03 QueryStream mismatch on {label}");
        }

        // W3-04 (a): Historical block-only range [t_blk1, t_blk2] after Head truncation + OOO!
        // W3-04 (b): Pre-Head.MinTime() OOO-only range [t_ooo1, t_ooo2]!
        for (label, s_ts, e_ts) in [
            ("block_only", t_blk1, t_blk2),
            ("ooo_before_head_mint", t_ooo1, t_ooo2),
            ("full_range", t_blk1, t_head_new),
        ] {
            let g_ln = go
                .ing
                .label_names(with_org(
                    "t-blk-ooo",
                    ingesterpb::LabelNamesRequest {
                        start_timestamp_ms: s_ts,
                        end_timestamp_ms: e_ts,
                        ..Default::default()
                    },
                ))
                .await
                .unwrap()
                .into_inner()
                .label_names;
            let r_ln = rust
                .ing
                .label_names(with_org(
                    "t-blk-ooo",
                    ingesterpb::LabelNamesRequest {
                        start_timestamp_ms: s_ts,
                        end_timestamp_ms: e_ts,
                        ..Default::default()
                    },
                ))
                .await
                .unwrap()
                .into_inner()
                .label_names;
            assert_eq!(g_ln, r_ln, "W3-04 LabelNames mismatch on {label}");

            let g_lv = go
                .ing
                .label_values(with_org(
                    "t-blk-ooo",
                    ingesterpb::LabelValuesRequest {
                        label_name: "shared".into(),
                        start_timestamp_ms: s_ts,
                        end_timestamp_ms: e_ts,
                        matchers: Some(ingesterpb::LabelMatchers {
                            matchers: vec![re("__name__", ".+")],
                        }),
                        ..Default::default()
                    },
                ))
                .await
                .unwrap()
                .into_inner()
                .label_values;
            let r_lv = rust
                .ing
                .label_values(with_org(
                    "t-blk-ooo",
                    ingesterpb::LabelValuesRequest {
                        label_name: "shared".into(),
                        start_timestamp_ms: s_ts,
                        end_timestamp_ms: e_ts,
                        matchers: Some(ingesterpb::LabelMatchers {
                            matchers: vec![re("__name__", ".+")],
                        }),
                        ..Default::default()
                    },
                ))
                .await
                .unwrap()
                .into_inner()
                .label_values;
            assert_eq!(g_lv, r_lv, "W3-04 LabelValues mismatch on {label}");

            let collect_search_names = |ing: &mut IngesterClient<Channel>| {
                let mut client = ing.clone();
                async move {
                    let mut st = client
                        .search_label_names(with_org(
                            "t-blk-ooo",
                            ingesterpb::SearchLabelNamesRequest {
                                start_timestamp_ms: s_ts,
                                end_timestamp_ms: e_ts,
                                ..Default::default()
                            },
                        ))
                        .await
                        .unwrap()
                        .into_inner();
                    let mut vals = Vec::new();
                    while let Some(b) = st.next().await {
                        for r in b.unwrap().results {
                            vals.push(r.value);
                        }
                    }
                    vals
                }
            };
            assert_eq!(
                collect_search_names(&mut go.ing).await,
                collect_search_names(&mut rust.ing).await,
                "W3-04 SearchLabelNames mismatch on {label}"
            );

            let collect_search_vals = |ing: &mut IngesterClient<Channel>| {
                let mut client = ing.clone();
                async move {
                    let mut st = client
                        .search_label_values(with_org(
                            "t-blk-ooo",
                            ingesterpb::SearchLabelValuesRequest {
                                name: "shared".into(),
                                start_timestamp_ms: s_ts,
                                end_timestamp_ms: e_ts,
                                ..Default::default()
                            },
                        ))
                        .await
                        .unwrap()
                        .into_inner();
                    let mut vals = Vec::new();
                    while let Some(b) = st.next().await {
                        for r in b.unwrap().results {
                            vals.push(r.value);
                        }
                    }
                    vals
                }
            };
            assert_eq!(
                collect_search_vals(&mut go.ing).await,
                collect_search_vals(&mut rust.ing).await,
                "W3-04 SearchLabelValues mismatch on {label}"
            );
        }

        // W3-04 (c): Fresh Head (no flush!) OOO-only window strictly before Head.MinTime()!
        for side in [&mut go, &mut rust] {
            side.ing
                .push(with_org(
                    "t-fresh-ooo",
                    cortexpb::WriteRequest {
                        timeseries: vec![cortexpb::TimeSeries {
                            labels: vec![lp("__name__", "m_fresh"), lp("k", "v")],
                            samples: vec![cortexpb::Sample {
                                value: 1.0,
                                timestamp_ms: now,
                            }],
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                ))
                .await
                .unwrap();
            side.ing
                .push(with_org(
                    "t-fresh-ooo",
                    cortexpb::WriteRequest {
                        timeseries: vec![cortexpb::TimeSeries {
                            labels: vec![lp("__name__", "m_fresh"), lp("k", "v")],
                            samples: vec![cortexpb::Sample {
                                value: 2.0,
                                timestamp_ms: now - 10 * 60_000,
                            }],
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                ))
                .await
                .unwrap();
        }
        let (fs, fe) = (now - 15 * 60_000, now - 5 * 60_000);
        let g_fresh_ln = go
            .ing
            .label_names(with_org(
                "t-fresh-ooo",
                ingesterpb::LabelNamesRequest {
                    start_timestamp_ms: fs,
                    end_timestamp_ms: fe,
                    ..Default::default()
                },
            ))
            .await
            .unwrap()
            .into_inner()
            .label_names;
        let r_fresh_ln = rust
            .ing
            .label_names(with_org(
                "t-fresh-ooo",
                ingesterpb::LabelNamesRequest {
                    start_timestamp_ms: fs,
                    end_timestamp_ms: fe,
                    ..Default::default()
                },
            ))
            .await
            .unwrap()
            .into_inner()
            .label_names;
        assert!(g_fresh_ln.is_empty());
        assert_eq!(g_fresh_ln, r_fresh_ln);

        // =========================================================================
        // 4. W3-05: OOO QueryStream wire chunk encoding (`xor2` = 7, `histogram_st` = 8 & 9)
        // =========================================================================
        for side in [&mut go, &mut rust] {
            side.ing
                .push(with_org(
                    "t-enc",
                    cortexpb::WriteRequest {
                        timeseries: vec![
                            cortexpb::TimeSeries {
                                labels: vec![lp("__name__", "w3_05_xor2")],
                                samples: vec![cortexpb::Sample {
                                    value: 1.0,
                                    timestamp_ms: now + 10_000,
                                }],
                                ..Default::default()
                            },
                            cortexpb::TimeSeries {
                                labels: vec![lp("__name__", "w3_05_ih_st")],
                                histograms: vec![make_ih(now + 10_000, 5)],
                                ..Default::default()
                            },
                            cortexpb::TimeSeries {
                                labels: vec![lp("__name__", "w3_05_fh_st")],
                                histograms: vec![make_fh(now + 10_000, 5.0)],
                                ..Default::default()
                            },
                        ],
                        ..Default::default()
                    },
                ))
                .await
                .unwrap();

            side.ing
                .push(with_org(
                    "t-enc",
                    cortexpb::WriteRequest {
                        timeseries: vec![
                            cortexpb::TimeSeries {
                                labels: vec![lp("__name__", "w3_05_xor2")],
                                samples: vec![cortexpb::Sample {
                                    value: 2.0,
                                    timestamp_ms: now + 5_000,
                                }],
                                ..Default::default()
                            },
                            cortexpb::TimeSeries {
                                labels: vec![lp("__name__", "w3_05_ih_st")],
                                histograms: vec![make_ih(now + 5_000, 3)],
                                ..Default::default()
                            },
                            cortexpb::TimeSeries {
                                labels: vec![lp("__name__", "w3_05_fh_st")],
                                histograms: vec![make_fh(now + 5_000, 3.0)],
                                ..Default::default()
                            },
                        ],
                        ..Default::default()
                    },
                ))
                .await
                .unwrap();
        }
        for (m, expected_enc) in [
            ("w3_05_xor2", "enc=7"),
            ("w3_05_ih_st", "enc=8"),
            ("w3_05_fh_st", "enc=9"),
        ] {
            let g_q = query_stream_summary(
                &mut go.ing,
                "t-enc",
                min_q,
                max_q,
                vec![eq("__name__", m)],
            )
            .await;
            let r_q = query_stream_summary(
                &mut rust.ing,
                "t-enc",
                min_q,
                max_q,
                vec![eq("__name__", m)],
            )
            .await;
            assert!(g_q.contains(expected_enc), "expected {expected_enc} in {g_q}");
            assert_eq!(g_q, r_q, "W3-05 encoding mismatch on {m}");
        }

        // =========================================================================
        // 5. W3-08: Unsorted labels on exemplar-only TimeSeries (200 OK) and with
        //    native_histograms_ingestion_enabled: false (rejected_samples = len(Histograms))
        // =========================================================================
        let ex_only_unsorted = cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![lp("z", "1"), lp("__name__", "unsort_ex_only")],
                exemplars: vec![cortexpb::Exemplar {
                    labels: vec![lp("t", "1")],
                    value: 1.0,
                    timestamp_ms: now,
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let g_ex_unsort = go
            .ing
            .push(with_org("t-no-nh", ex_only_unsorted.clone()))
            .await;
        let r_ex_unsort = rust.ing.push(with_org("t-no-nh", ex_only_unsorted)).await;
        assert_eq!(fmt_status(&g_ex_unsort), "OK");
        assert_eq!(fmt_status(&g_ex_unsort), fmt_status(&r_ex_unsort));

        let hist_disabled_unsorted = cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![lp("z", "1"), lp("__name__", "unsort_hist_disabled")],
                histograms: vec![make_ih(now, 2), make_ih(now + 1_000, 3)],
                ..Default::default()
            }],
            ..Default::default()
        };
        let g_hd_unsort = go
            .ing
            .push(with_org("t-no-nh", hist_disabled_unsorted.clone()))
            .await;
        let r_hd_unsort = rust
            .ing
            .push(with_org("t-no-nh", hist_disabled_unsorted))
            .await;
        assert!(fmt_status(&g_hd_unsort).contains("rejected=2"));
        assert_eq!(fmt_status(&g_hd_unsort), fmt_status(&r_hd_unsort));

        // =========================================================================
        // 6. W3-09: LabelNamesAndValues(ACTIVE) with matchers uses IsLabelValueActive
        // =========================================================================
        for side in [&mut go, &mut rust] {
            side.ing
                .push(with_org(
                    "t-lnv-act",
                    cortexpb::WriteRequest {
                        timeseries: vec![cortexpb::TimeSeries {
                            labels: vec![
                                lp("__name__", "m_inactive"),
                                lp("env", "prod"),
                                lp("only_inact", "x"),
                            ],
                            samples: vec![cortexpb::Sample {
                                value: 1.0,
                                timestamp_ms: now,
                            }],
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                ))
                .await
                .unwrap();
        }
        // Wait > 500ms idle_timeout + 150ms update_period so `m_inactive` becomes inactive in both.
        tokio::time::sleep(Duration::from_millis(900)).await;
        for side in [&mut go, &mut rust] {
            side.ing
                .push(with_org(
                    "t-lnv-act",
                    cortexpb::WriteRequest {
                        timeseries: vec![cortexpb::TimeSeries {
                            labels: vec![lp("__name__", "m_active"), lp("env", "prod")],
                            samples: vec![cortexpb::Sample {
                                value: 2.0,
                                timestamp_ms: now + 1_000,
                            }],
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                ))
                .await
                .unwrap();
        }
        let collect_lnv = |ing: &mut IngesterClient<Channel>| {
            let mut client = ing.clone();
            async move {
                let mut st = client
                    .label_names_and_values(with_org(
                        "t-lnv-act",
                        ingesterpb::LabelNamesAndValuesRequest {
                            matchers: vec![eq("__name__", "m_inactive")],
                            count_method: ingesterpb::CountMethod::Active as i32,
                        },
                    ))
                    .await
                    .unwrap()
                    .into_inner();
                let mut map: BTreeMap<String, Vec<String>> = BTreeMap::new();
                while let Some(msg) = st.next().await {
                    for it in msg.unwrap().items {
                        let v = map.entry(it.label_name).or_default();
                        v.extend(it.values);
                        v.sort();
                    }
                }
                map
            }
        };
        let g_lnv = collect_lnv(&mut go.ing).await;
        let r_lnv = collect_lnv(&mut rust.ing).await;
        assert_eq!(
            g_lnv,
            BTreeMap::from([("env".to_string(), vec!["prod".to_string()])])
        );
        assert_eq!(g_lnv, r_lnv, "W3-09 LabelNamesAndValues(ACTIVE) mismatch");

        // =========================================================================
        // 7. W3-10: /ingester/label_churn/:tenant 503 NoRecord & CompactedTooRecently bodies
        // =========================================================================
        // (a) NoRecord on `t-fresh-ooo` (lookback=1ms)
        let (g_nr_code, _, g_nr_body) = http_get(
            &http_client,
            go.http_port,
            "/ingester/label_churn/t-fresh-ooo?lookback=1ms",
            false,
        )
        .await
        .unwrap();
        let (r_nr_code, _, r_nr_body) = http_get(
            &http_client,
            rust.http_port,
            "/ingester/label_churn/t-fresh-ooo?lookback=1ms",
            false,
        )
        .await
        .unwrap();
        assert_eq!(g_nr_code, 503);
        assert_eq!((g_nr_code, &g_nr_body), (r_nr_code, &r_nr_body));

        // (b) CompactedTooRecently on `t-blk-ooo` (which flushed and removed series!)
        let (g_ct_code, _, g_ct_body) = http_get(
            &http_client,
            go.http_port,
            "/ingester/label_churn/t-blk-ooo?lookback=30m",
            false,
        )
        .await
        .unwrap();
        let (r_ct_code, _, r_ct_body) = http_get(
            &http_client,
            rust.http_port,
            "/ingester/label_churn/t-blk-ooo?lookback=30m",
            false,
        )
        .await
        .unwrap();
        assert_eq!(g_ct_code, 503);
        assert_eq!((g_ct_code, &g_ct_body), (r_ct_code, &r_ct_body));

        // =========================================================================
        // 8. W3-13 & W3-14: ActiveSeries invalid `__query_shard__` & unknown CountMethod
        // =========================================================================
        let collect_as_err = |ing: &mut IngesterClient<Channel>, tenant: &'static str| {
            let mut client = ing.clone();
            async move {
                let res = client
                    .active_series(with_org(
                        tenant,
                        ingesterpb::ActiveSeriesRequest {
                            matchers: vec![eq("__query_shard__", "bad")],
                            r#type: 0,
                        },
                    ))
                    .await;
                match res {
                    Err(s) => fmt_read_err::<()>(&Err(s)),
                    Ok(resp) => {
                        let mut st = resp.into_inner();
                        match st.next().await {
                            Some(Err(s)) => fmt_read_err::<()>(&Err(s)),
                            Some(Ok(_)) => "UNEXPECTED_ITEM".into(),
                            None => "EMPTY_OK".into(),
                        }
                    }
                }
            }
        };
        let g_as_err = collect_as_err(&mut go.ing, "t-blk-ooo").await;
        let r_as_err = collect_as_err(&mut rust.ing, "t-blk-ooo").await;
        assert!(g_as_err.contains("error listing active series: error removing shard matcher:"));
        assert_eq!(g_as_err, r_as_err, "W3-13 ActiveSeries shard error mismatch");

        // Non-existent tenant returns empty OK before shard parsing in Go (`active_series.go:57`).
        assert_eq!(
            collect_as_err(&mut go.ing, "nonexistent-tenant").await,
            collect_as_err(&mut rust.ing, "nonexistent-tenant").await,
        );

        // W3-14: Unknown CountMethod=99 across UserStats, AllUserStats, LNV, LVC
        let g_us_bad = go
            .ing
            .user_stats(with_org(
                "t-blk-ooo",
                ingesterpb::UserStatsRequest { count_method: 99 },
            ))
            .await;
        let r_us_bad = rust
            .ing
            .user_stats(with_org(
                "t-blk-ooo",
                ingesterpb::UserStatsRequest { count_method: 99 },
            ))
            .await;
        assert!(fmt_read_err(&g_us_bad).contains("unknown count method \"99\""));
        assert_eq!(fmt_read_err(&g_us_bad), fmt_read_err(&r_us_bad));

        let g_aus_bad = go
            .ing
            .all_user_stats(tonic::Request::new(ingesterpb::UserStatsRequest {
                count_method: 99,
            }))
            .await;
        let r_aus_bad = rust
            .ing
            .all_user_stats(tonic::Request::new(ingesterpb::UserStatsRequest {
                count_method: 99,
            }))
            .await;
        assert_eq!(fmt_read_err(&g_aus_bad), fmt_read_err(&r_aus_bad));

        for kind in ["lnv", "lvc"] {
            let recv_err = |ing: &mut IngesterClient<Channel>| {
                let mut client = ing.clone();
                async move {
                    if kind == "lnv" {
                        match client
                            .label_names_and_values(with_org(
                                "t-blk-ooo",
                                ingesterpb::LabelNamesAndValuesRequest {
                                    matchers: vec![],
                                    count_method: 99,
                                },
                            ))
                            .await
                        {
                            Err(s) => fmt_read_err::<()>(&Err(s)),
                            Ok(r) => match r.into_inner().next().await {
                                Some(Err(s)) => fmt_read_err::<()>(&Err(s)),
                                _ => "UNEXPECTED_OK".into(),
                            },
                        }
                    } else {
                        match client
                            .label_values_cardinality(with_org(
                                "t-blk-ooo",
                                ingesterpb::LabelValuesCardinalityRequest {
                                    label_names: vec!["shared".into()],
                                    matchers: vec![],
                                    count_method: 99,
                                },
                            ))
                            .await
                        {
                            Err(s) => fmt_read_err::<()>(&Err(s)),
                            Ok(r) => match r.into_inner().next().await {
                                Some(Err(s)) => fmt_read_err::<()>(&Err(s)),
                                _ => "UNEXPECTED_OK".into(),
                            },
                        }
                    }
                }
            };
            let g_e = recv_err(&mut go.ing).await;
            let r_e = recv_err(&mut rust.ing).await;
            assert!(g_e.contains("unknown count method \"99\""));
            assert_eq!(g_e, r_e, "W3-14 mismatch on {kind}");
        }

        // =========================================================================
        // 9. W3-15: Duplicate float error formatting (`1e+06`, `1.5e-05`, `-0` for `-0.0`)
        // =========================================================================
        for side in [&mut go, &mut rust] {
            side.ing
                .push(with_org(
                    "t-dup",
                    cortexpb::WriteRequest {
                        timeseries: vec![
                            cortexpb::TimeSeries {
                                labels: vec![lp("__name__", "m_sci")],
                                samples: vec![cortexpb::Sample {
                                    value: 1e6,
                                    timestamp_ms: now,
                                }],
                                ..Default::default()
                            },
                            cortexpb::TimeSeries {
                                labels: vec![lp("__name__", "m_negzero")],
                                samples: vec![cortexpb::Sample {
                                    value: 1.0,
                                    timestamp_ms: now,
                                }],
                                ..Default::default()
                            },
                        ],
                        ..Default::default()
                    },
                ))
                .await
                .unwrap();
        }
        let sci_dup = cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![lp("__name__", "m_sci")],
                samples: vec![cortexpb::Sample {
                    value: 1.5e-5,
                    timestamp_ms: now,
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let g_sci = go.ing.push(with_org("t-dup", sci_dup.clone())).await;
        let r_sci = rust.ing.push(with_org("t-dup", sci_dup)).await;
        assert!(fmt_status(&g_sci).contains("existing 1e+06, new value 1.5e-05"));
        assert_eq!(fmt_status(&g_sci), fmt_status(&r_sci));

        let raw_neg_zero = encode_explicit_neg_zero_write_request("m_negzero", now);
        let g_nz = push_raw_bytes(go.ch.clone(), "t-dup", raw_neg_zero.clone()).await;
        let r_nz = push_raw_bytes(rust.ch.clone(), "t-dup", raw_neg_zero).await;
        let g_nz_err = g_nz.expect_err("Go -0.0 duplicate error");
        let r_nz_err = r_nz.expect_err("Rust -0.0 duplicate error");
        assert!(
            g_nz_err.message().contains("existing 1, new value -0 "),
            "unexpected Go -0.0 msg: {}",
            g_nz_err.message()
        );
        assert_eq!(g_nz_err.code(), r_nz_err.code());
        assert_eq!(g_nz_err.message(), r_nz_err.message());

        go.stop().await;
        rust.stop().await;
    });
}

#[derive(Debug, PartialEq)]
struct TenantHandoffSnapshot {
    corruptions: String,
    series_samples: BTreeMap<String, Vec<String>>,
    chunk_encs: BTreeMap<String, Vec<i32>>,
    label_names: Vec<String>,
    label_values: Vec<String>,
    matchers_res: Vec<String>,
    lnv: BTreeMap<String, Vec<String>>,
    lvc: BTreeMap<String, BTreeMap<String, u64>>,
    exemplars: Vec<String>,
    metadata: Vec<(i32, String, String, String)>,
    user_stats_num_series: u64,
}

async fn populate_handoff_workload(s: &mut RunningIngester, base_ts: i64) {
    let ct = base_ts - 5_000;
    for tenant in ["t-rich", "t-enc"] {
        s.ing
            .push(with_org(
                tenant,
                cortexpb::WriteRequest {
                    metadata: vec![
                        cortexpb::MetricMetadata {
                            r#type: cortexpb::metric_metadata::MetricType::Counter as i32,
                            metric_family_name: "float_series_total".into(),
                            help: "float counter help".into(),
                            unit: "requests".into(),
                        },
                        cortexpb::MetricMetadata {
                            r#type: cortexpb::metric_metadata::MetricType::Histogram as i32,
                            metric_family_name: "req_dur_ih".into(),
                            help: "int hist help".into(),
                            unit: "seconds".into(),
                        },
                        cortexpb::MetricMetadata {
                            r#type: cortexpb::metric_metadata::MetricType::Gaugehistogram as i32,
                            metric_family_name: "req_dur_fh".into(),
                            help: "float hist help".into(),
                            unit: "bytes".into(),
                        },
                    ],
                    ..Default::default()
                },
            ))
            .await
            .expect("push metadata");

        // 150 in-order rounds across float, int-hist, float-hist, NHCB-int, NHCB-float + CT + UTF-8 exemplars
        // (forces in-order chunks_head rollover).
        for i in 0..150i64 {
            let ts = base_ts + i * 1_000;
            let v = match i {
                50 => -0.0,
                51 => f64::INFINITY,
                52 => f64::NEG_INFINITY,
                53 => f64::from_bits(0x7ff8_0000_0000_0001),
                54 => stale_nan(),
                _ => i as f64 * 1.25,
            };
            let hint = (i % 4) as i32;
            let exemplars = if i % 30 == 0 {
                vec![cortexpb::Exemplar {
                    labels: vec![lp("trace_id", &format!("tr-{i}")), lp("utf8_tag", "αβγ")],
                    value: i as f64,
                    timestamp_ms: ts,
                }]
            } else {
                Vec::new()
            };
            s.ing
                .push(with_org(
                    tenant,
                    cortexpb::WriteRequest {
                        timeseries: vec![
                            cortexpb::TimeSeries {
                                labels: vec![lp("__name__", "float_series_total"), lp("job", "api")],
                                samples: vec![cortexpb::Sample {
                                    value: v,
                                    timestamp_ms: ts,
                                }],
                                exemplars,
                                created_timestamp: ct,
                                ..Default::default()
                            },
                            cortexpb::TimeSeries {
                                labels: vec![lp("__name__", "req_dur_ih"), lp("job", "api")],
                                histograms: vec![make_rich_int_hist(
                                    ts,
                                    hint,
                                    i as f64 * 2.5,
                                    1 + i,
                                    1,
                                )],
                                created_timestamp: ct,
                                ..Default::default()
                            },
                            cortexpb::TimeSeries {
                                labels: vec![lp("__name__", "req_dur_fh"), lp("job", "api")],
                                histograms: vec![make_rich_float_hist(
                                    ts,
                                    hint,
                                    i as f64 * 1.5,
                                    1.0 + i as f64,
                                    0.5,
                                )],
                                created_timestamp: ct,
                                ..Default::default()
                            },
                            cortexpb::TimeSeries {
                                labels: vec![lp("__name__", "nhcb_ih"), lp("job", "api")],
                                histograms: vec![make_nhcb_int(ts, i as f64 * 0.7, 2 + i, -1)],
                                created_timestamp: ct,
                                ..Default::default()
                            },
                            cortexpb::TimeSeries {
                                labels: vec![lp("__name__", "nhcb_fh"), lp("job", "api")],
                                histograms: vec![make_nhcb_float(
                                    ts,
                                    4.5 + i as f64,
                                    i as f64 * 3.3,
                                    2.0 + i as f64,
                                    2.5,
                                )],
                                created_timestamp: ct,
                                ..Default::default()
                            },
                        ],
                        ..Default::default()
                    },
                ))
                .await
                .expect("push in-order round");
        }

        // 40 OOO rounds (> default ooo_cap_max=32 -> forces OOO chunk cut to chunks_head + WBL MmapMarkers + remaining in-mem OOO!).
        for j in 0..40i64 {
            let ooo_ts = base_ts - 300_000 + (39 - j) * 1_000;
            s.ing
                .push(with_org(
                    tenant,
                    cortexpb::WriteRequest {
                        timeseries: vec![
                            cortexpb::TimeSeries {
                                labels: vec![lp("__name__", "float_series_total"), lp("job", "api")],
                                samples: vec![cortexpb::Sample {
                                    value: 1_000.0 + j as f64,
                                    timestamp_ms: ooo_ts,
                                }],
                                ..Default::default()
                            },
                            cortexpb::TimeSeries {
                                labels: vec![lp("__name__", "req_dur_ih"), lp("job", "api")],
                                histograms: vec![make_rich_int_hist(
                                    ooo_ts,
                                    2, // NO
                                    j as f64 * 1.1,
                                    7 + j,
                                    1,
                                )],
                                ..Default::default()
                            },
                            cortexpb::TimeSeries {
                                labels: vec![lp("__name__", "req_dur_fh"), lp("job", "api")],
                                histograms: vec![make_rich_float_hist(
                                    ooo_ts,
                                    2, // NO
                                    j as f64 * 0.9,
                                    3.0 + j as f64,
                                    0.5,
                                )],
                                ..Default::default()
                            },
                            cortexpb::TimeSeries {
                                labels: vec![lp("__name__", "nhcb_ih"), lp("job", "api")],
                                histograms: vec![make_nhcb_int(ooo_ts, j as f64 * 0.3, 4 + j, -1)],
                                ..Default::default()
                            },
                        ],
                        ..Default::default()
                    },
                ))
                .await
                .expect("push OOO round");
        }
    }
}

async fn snapshot_handoff_tenant(
    http_client: &reqwest::Client,
    s: &mut RunningIngester,
    tenant: &str,
    min_ts: i64,
    max_ts: i64,
) -> TenantHandoffSnapshot {
    let tsdb_metrics_resp = http_client
        .get(format!("http://127.0.0.1:{}/ingester/tsdb_metrics", s.http_port))
        .header("X-Scope-OrgID", tenant)
        .send()
        .await
        .expect("GET /ingester/tsdb_metrics");
    assert_eq!(tsdb_metrics_resp.status().as_u16(), 200);
    let tsdb_metrics = tsdb_metrics_resp.text().await.unwrap();
    let mut corr_lines: Vec<String> = tsdb_metrics
        .lines()
        .filter(|l| {
            !l.starts_with('#') && (l.contains("corruption") || l.contains("unknown_series_refs"))
        })
        .map(str::to_string)
        .collect();
    corr_lines.sort();
    let corruptions = corr_lines.join(" | ");
    assert_eq!(
        corruptions,
        "prometheus_tsdb_mmap_chunk_corruptions_total 0 | prometheus_tsdb_wal_corruptions_total 0"
    );

    let mut qs = s
        .ing
        .query_stream(with_org(
            tenant,
            ingesterpb::QueryRequest {
                start_timestamp_ms: min_ts,
                end_timestamp_ms: max_ts,
                matchers: vec![re("__name__", ".+")],
                ..Default::default()
            },
        ))
        .await
        .expect("QueryStream")
        .into_inner();
    let mut series_labels = Vec::new();
    let mut chunks_by_idx: BTreeMap<u64, Vec<ingesterpb::Chunk>> = BTreeMap::new();
    while let Some(msg) = qs.next().await {
        let msg = msg.unwrap();
        for ss in msg.streaming_series {
            series_labels.push(format_labels(&ss.labels));
        }
        for sc in msg.streaming_series_chunks {
            chunks_by_idx
                .entry(sc.series_index)
                .or_default()
                .extend(sc.chunks);
        }
    }

    let mut series_samples = BTreeMap::new();
    let mut chunk_encs = BTreeMap::new();
    for (idx, s_name) in series_labels.into_iter().enumerate() {
        let chks = chunks_by_idx.remove(&(idx as u64)).unwrap_or_default();
        let mut encs = Vec::new();
        let mut recs: Vec<(i64, String)> = Vec::new();
        for c in chks {
            encs.push(c.encoding);
            let enc = wire_enc_to_tsdb(c.encoding);
            let parsed = EncodedChunk::from_bytes(enc, c.data.to_vec()).expect("decode chunk");
            for ds in chunk_to_samples(&parsed).expect("iter chunk") {
                let desc = match ds.value {
                    SampleValue::Float(v) => {
                        format!("F(st={},t={},bits={:016x})", ds.st, ds.t, v.to_bits())
                    }
                    SampleValue::Histogram(h) => format!(
                        "IH(st={},t={},sch={},rst={:?},cnt={},zc={},zt={},sum={:016x},ps={:?},pb={:?},ns={:?},nb={:?},cv={:?})",
                        ds.st,
                        ds.t,
                        h.schema,
                        h.counter_reset_hint,
                        h.count,
                        h.zero_count,
                        h.zero_threshold,
                        h.sum.to_bits(),
                        h.positive_spans,
                        h.positive_buckets,
                        h.negative_spans,
                        h.negative_buckets,
                        h.custom_values,
                    ),
                    SampleValue::FloatHistogram(fh) => format!(
                        "FH(st={},t={},sch={},rst={:?},cnt={},zc={},zt={},sum={:016x},ps={:?},pb={:?},ns={:?},nb={:?},cv={:?})",
                        ds.st,
                        ds.t,
                        fh.schema,
                        fh.counter_reset_hint,
                        fh.count,
                        fh.zero_count,
                        fh.zero_threshold,
                        fh.sum.to_bits(),
                        fh.positive_spans,
                        fh.positive_buckets,
                        fh.negative_spans,
                        fh.negative_buckets,
                        fh.custom_values,
                    ),
                };
                recs.push((ds.t, desc));
            }
        }
        recs.sort_by_key(|r| r.0);
        recs.dedup_by_key(|r| r.0);
        series_samples.insert(s_name.clone(), recs.into_iter().map(|(_, d)| d).collect());
        chunk_encs.insert(s_name, encs);
    }

    let label_names = s
        .ing
        .label_names(with_org(
            tenant,
            ingesterpb::LabelNamesRequest {
                start_timestamp_ms: min_ts,
                end_timestamp_ms: max_ts,
                ..Default::default()
            },
        ))
        .await
        .unwrap()
        .into_inner()
        .label_names;

    let label_values = s
        .ing
        .label_values(with_org(
            tenant,
            ingesterpb::LabelValuesRequest {
                label_name: "__name__".into(),
                start_timestamp_ms: min_ts,
                end_timestamp_ms: max_ts,
                ..Default::default()
            },
        ))
        .await
        .unwrap()
        .into_inner()
        .label_values;

    let mflm = s
        .ing
        .metrics_for_label_matchers(with_org(
            tenant,
            ingesterpb::MetricsForLabelMatchersRequest {
                start_timestamp_ms: min_ts,
                end_timestamp_ms: max_ts,
                matchers_set: vec![ingesterpb::LabelMatchers {
                    matchers: vec![eq("job", "api")],
                }],
                ..Default::default()
            },
        ))
        .await
        .unwrap()
        .into_inner();
    let matchers_res: Vec<String> = mflm.metric.iter().map(|m| format_labels(&m.labels)).collect();

    let mut lnv_st = s
        .ing
        .label_names_and_values(with_org(
            tenant,
            ingesterpb::LabelNamesAndValuesRequest::default(),
        ))
        .await
        .unwrap()
        .into_inner();
    let mut lnv: BTreeMap<String, Vec<String>> = BTreeMap::new();
    while let Some(m) = lnv_st.next().await {
        for it in m.unwrap().items {
            let v = lnv.entry(it.label_name).or_default();
            v.extend(it.values);
            v.sort();
        }
    }

    let mut lvc_st = s
        .ing
        .label_values_cardinality(with_org(
            tenant,
            ingesterpb::LabelValuesCardinalityRequest {
                label_names: vec!["__name__".into(), "job".into()],
                ..Default::default()
            },
        ))
        .await
        .unwrap()
        .into_inner();
    let mut lvc: BTreeMap<String, BTreeMap<String, u64>> = BTreeMap::new();
    while let Some(m) = lvc_st.next().await {
        for it in m.unwrap().items {
            lvc.entry(it.label_name)
                .or_default()
                .extend(it.label_value_series);
        }
    }

    let qe = s
        .ing
        .query_exemplars(with_org(
            tenant,
            ingesterpb::ExemplarQueryRequest {
                start_timestamp_ms: min_ts,
                end_timestamp_ms: max_ts,
                matchers: vec![ingesterpb::LabelMatchers {
                    matchers: vec![re("__name__", ".*")],
                }],
            },
        ))
        .await
        .unwrap()
        .into_inner();
    let mut exemplars = Vec::new();
    for ts in qe.timeseries {
        let s_lbl = format_labels(&ts.labels);
        for ex in ts.exemplars {
            exemplars.push(format!(
                "{s_lbl}@{}:{:016x}{}",
                ex.timestamp_ms,
                ex.value.to_bits(),
                format_labels(&ex.labels)
            ));
        }
    }

    let md_resp = s
        .ing
        .metrics_metadata(with_org(
            tenant,
            ingesterpb::MetricsMetadataRequest {
                limit: -1,
                limit_per_metric: -1,
                ..Default::default()
            },
        ))
        .await
        .unwrap()
        .into_inner();
    let mut metadata: Vec<(i32, String, String, String)> = md_resp
        .metadata
        .into_iter()
        .map(|m| (m.r#type, m.metric_family_name, m.help, m.unit))
        .collect();
    metadata.sort();

    let us = s
        .ing
        .user_stats(with_org(tenant, ingesterpb::UserStatsRequest::default()))
        .await
        .unwrap()
        .into_inner();

    TenantHandoffSnapshot {
        corruptions,
        series_samples,
        chunk_encs,
        label_names,
        label_values,
        matchers_res,
        lnv,
        lvc,
        exemplars,
        metadata,
        user_stats_num_series: us.num_series,
    }
}

fn copy_dir_recursive(src: &Path, dst: &Path) {
    let status = Command::new("cp")
        .args(["-a", &src.to_string_lossy(), &dst.to_string_lossy()])
        .status()
        .expect("cp -a");
    assert!(status.success(), "cp -a {} -> {} failed", src.display(), dst.display());
}

#[test]
fn wave3_bidirectional_sigkill_wal_wbl_chunks_head_handoff() {
    block_on_for_tests(async {
        let tmp = tempfile::tempdir().unwrap();
        let http_client = reqwest::Client::new();
        let base_ts = now_ms() - 30 * 60_000;
        let min_q = base_ts - 3_600_000;
        let max_q = base_ts + 3_600_000;

        for seed_impl in IMPLS {
            let seed_dir = tmp.path().join(format!("seed-{}", seed_impl.name()));
            let mut seed =
                boot_wave3_ingester(seed_impl, "seed", &seed_dir, &http_client).await;
            populate_handoff_workload(&mut seed, base_ts).await;
            seed.kill9();

            let go_copy = tmp
                .path()
                .join(format!("{}-to-go", seed_impl.name()));
            let rust_copy = tmp
                .path()
                .join(format!("{}-to-rust", seed_impl.name()));
            copy_dir_recursive(&seed_dir, &go_copy);
            copy_dir_recursive(&seed_dir, &rust_copy);

            let mut recovered_go =
                boot_wave3_ingester(Impl::Go, "rec-go", &go_copy, &http_client).await;
            let mut recovered_rust =
                boot_wave3_ingester(Impl::Rust, "rec-rust", &rust_copy, &http_client).await;

            for tenant in ["t-rich", "t-enc"] {
                let g_snap = snapshot_handoff_tenant(
                    &http_client,
                    &mut recovered_go,
                    tenant,
                    min_q,
                    max_q,
                )
                .await;
                let r_snap = snapshot_handoff_tenant(
                    &http_client,
                    &mut recovered_rust,
                    tenant,
                    min_q,
                    max_q,
                )
                .await;
                assert_eq!(g_snap.user_stats_num_series, 5);
                if tenant == "t-enc" {
                    // W3-05: Every float chunk on `t-enc` (both in-order and OOO rollover/WBL) MUST be Xor2 (`7`)!
                    assert!(
                        g_snap.chunk_encs["{__name__=\"float_series_total\", job=\"api\"}"]
                            .iter()
                            .all(|&e| e == 7),
                        "expected all Xor2(7) on Go: {:?}",
                        g_snap.chunk_encs
                    );
                }
                assert_eq!(
                    g_snap,
                    r_snap,
                    "Bidirectional SIGKILL handoff mismatch (seed={}, tenant={tenant})",
                    seed_impl.name()
                );
            }

            recovered_go.stop().await;
            recovered_rust.stop().await;
        }
    });
}
