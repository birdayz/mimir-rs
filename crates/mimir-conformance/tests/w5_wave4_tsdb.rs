//! Wave 4 TSDB twin-process differential conformance suite (`target/go-mimir` vs
//! `target/release/mimir`), verifying 100% upstream Go parity for all 4 Wave 4 TSDB fixes:
//!
//! - W4-01: Partial tombstones in on-disk ULID blocks (`<block>/tombstones`) and replayed Head WAL
//!   `Tombstones` records filter deleted samples (`in_bounds`), preserve non-deleted samples outside
//!   `[mint, maxt]` (`DisableTrimming = true`), preserve original chunk encoding (`Xor`, `Xor2`,
//!   `HistogramSt`), and omit chunks/series with 0 surviving samples.
//! - W4-02: Overlapping OOO + in-order Head chunks in `QueryStream` (`DisableTrimming = true`) merge
//!   without trimming to `[mint, maxt]` (gap queries inside overlapping chunks return the merged
//!   chunk instead of 0 series, and PromQL lookback samples are preserved).
//! - W4-04: Native histogram Head chunk rollover across all 4 histogram encodings (`Histogram`,
//!   `FloatHistogram`, `HistogramSt`, `FloatHistogramSt`) writes `CounterResetHeader::NotCounterReset`
//!   (`0x40`) on monotonic continuation and `CounterReset` (`0x80`) on count/bucket drop.
//! - W4-07: Runtime `float_chunk_encoding` (`xor2 -> xor` with `st == 0`) keeps appending to the
//!   active `Xor2` chunk until normal rollover (`CompatibleValues` in `head_append.go:2206`).

#![allow(clippy::result_large_err)]

use std::collections::BTreeMap;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use mimir_common::block_on_for_tests;
use mimir_conformance::free_test_port;
use mimir_conformance::twin::{http_get, start_mimir, wait_ready, Impl, Proc};
use mimir_proto::cortexpb;
use mimir_proto::ingesterpb;
use mimir_proto::ingesterpb::ingester_client::IngesterClient;
use mimir_tsdb::block::BlockMetaCompaction;
use mimir_tsdb::chunkenc::{CounterResetHint, EncodedChunk, Encoding, Histogram, HistogramSpan};
use mimir_tsdb::chunks_block::DEFAULT_CHUNK_SEGMENT_SIZE;
use mimir_tsdb::compact::{
    encode_samples_into_chunks_with_encodings, write_block_from_series, CompactionSeries,
    DecodedSample, SampleValue,
};
use mimir_tsdb::head::{Head, HeadOptions};
use mimir_tsdb::labels::Labels;
use mimir_tsdb::querier::chunk_to_samples;
use mimir_tsdb::record::{RecordEncoder, Tombstone};
use mimir_tsdb::tombstones::{write_tombstones_file, Interval, MemTombstones};
use mimir_tsdb::wlog::{CompressionType, WlogWriter};
use tokio_stream::StreamExt;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;

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

fn eq(name: &str, value: &str) -> ingesterpb::LabelMatcher {
    ingesterpb::LabelMatcher {
        r#type: ingesterpb::MatchType::Equal as i32,
        name: name.to_string(),
        value: value.to_string(),
    }
}

fn re(name: &str, value: &str) -> ingesterpb::LabelMatcher {
    ingesterpb::LabelMatcher {
        r#type: ingesterpb::MatchType::RegexMatch as i32,
        name: name.to_string(),
        value: value.to_string(),
    }
}

fn span(offset: i32, length: u32) -> cortexpb::BucketSpan {
    cortexpb::BucketSpan { offset, length }
}

fn make_int_h(ts: i64, reset_hint: i32, schema: i32, d0: i64, d1: i64, sum: f64) -> cortexpb::Histogram {
    let b0 = d0;
    let b1 = d0 + d1;
    let cnt = (1 + b0 + b1) as u64;
    cortexpb::Histogram {
        count: Some(cortexpb::histogram::Count::CountInt(cnt)),
        zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountInt(1)),
        zero_threshold: 0.001,
        sum,
        schema,
        positive_spans: vec![span(0, 2)],
        positive_deltas: vec![d0, d1],
        reset_hint,
        timestamp: ts,
        ..Default::default()
    }
}

fn make_float_h(
    ts: i64,
    reset_hint: i32,
    schema: i32,
    c0: f64,
    c1: f64,
    sum: f64,
) -> cortexpb::Histogram {
    let cnt = 0.5 + c0 + c1;
    cortexpb::Histogram {
        count: Some(cortexpb::histogram::Count::CountFloat(cnt)),
        zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountFloat(0.5)),
        zero_threshold: 0.001,
        sum,
        schema,
        positive_spans: vec![span(0, 2)],
        positive_counts: vec![c0, c1],
        reset_hint,
        timestamp: ts,
        ..Default::default()
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

fn crh_hex(parsed: &EncodedChunk) -> String {
    match parsed {
        EncodedChunk::Histogram(c) => format!("crh={:02x}", c.counter_reset_header() as u8),
        EncodedChunk::FloatHistogram(c) => format!("crh={:02x}", c.counter_reset_header() as u8),
        EncodedChunk::HistogramSt(c) => format!("crh={:02x}", c.counter_reset_header() as u8),
        EncodedChunk::FloatHistogramSt(c) => format!("crh={:02x}", c.counter_reset_header() as u8),
        _ => "crh=-".to_string(),
    }
}

async fn query_stream_detailed(
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

    if series_desc.is_empty() {
        return "(0 series)".to_string();
    }

    let mut out = Vec::new();
    for (idx, s_name) in series_desc.into_iter().enumerate() {
        let chks = chunks_by_idx.remove(&(idx as u64)).unwrap_or_default();
        let mut c_desc = Vec::new();
        for c in chks {
            let enc = wire_enc_to_tsdb(c.encoding);
            let parsed = EncodedChunk::from_bytes(enc, c.data.to_vec()).expect("decode chunk");
            let crh = crh_hex(&parsed);
            let samples = chunk_to_samples(&parsed).expect("iter chunk");
            let sample_strs: Vec<String> = samples
                .into_iter()
                .map(|s| match s.value {
                    SampleValue::Float(v) => format!("F(st={},t={},v={:016x})", s.st, s.t, v.to_bits()),
                    SampleValue::Histogram(h) => format!(
                        "IH(st={},t={},rst={:?},sch={},cnt={},sum={:016x},pb={:?})",
                        s.st,
                        s.t,
                        h.counter_reset_hint,
                        h.schema,
                        h.count,
                        h.sum.to_bits(),
                        h.positive_buckets
                    ),
                    SampleValue::FloatHistogram(fh) => format!(
                        "FH(st={},t={},rst={:?},sch={},cnt={},sum={:016x},pb={:?})",
                        s.st,
                        s.t,
                        fh.counter_reset_hint,
                        fh.schema,
                        fh.count,
                        fh.sum.to_bits(),
                        fh.positive_buckets
                    ),
                })
                .collect();
            c_desc.push(format!(
                "[enc={} {crh} {}..{} samples={sample_strs:?}]",
                c.encoding, c.start_timestamp_ms, c.end_timestamp_ms
            ));
        }
        out.push(format!("{s_name}->{c_desc:?}"));
    }
    out.sort();
    out.join(" ; ")
}

struct RunningIngester {
    proc: Proc,
    http_port: u16,
    ing: IngesterClient<Channel>,
}

impl RunningIngester {
    async fn stop(mut self) {
        self.proc.sigterm();
        let _ = self.proc.wait_exit(Duration::from_secs(10));
    }
}

async fn boot_wave4_ingester(
    which: Impl,
    name: &str,
    dir: &Path,
    rt_yaml: &str,
    http_client: &reqwest::Client,
) -> RunningIngester {
    let tsdb_dir = dir.join("tsdb");
    let blocks_dir = dir.join("blocks");
    std::fs::create_dir_all(&tsdb_dir).unwrap();
    std::fs::create_dir_all(&blocks_dir).unwrap();

    let rt_path = dir.join("runtime.yaml");
    std::fs::write(&rt_path, rt_yaml).unwrap();

    let http_port = free_test_port();
    let grpc_port = free_test_port();
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
  ring:
    instance_id: ingester-0
    instance_addr: 127.0.0.1
    kvstore:
      store: inmemory
    replication_factor: 1
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
  period: 150ms
limits:
  native_histograms_ingestion_enabled: true
  out_of_order_time_window: 2h
  creation_grace_period: 24h
"#,
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
    let ing = IngesterClient::new(ch);
    RunningIngester {
        proc,
        http_port,
        ing,
    }
}

fn copy_dir_recursive(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let ty = entry.file_type().unwrap();
        let target = dst.join(entry.file_name());
        if ty.is_dir() {
            copy_dir_recursive(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}

#[test]
fn w4_02_04_07_ooo_overlap_histogram_rollover_and_runtime_float_encoding_parity() {
    block_on_for_tests(async {
        let tmp = tempfile::tempdir().unwrap();
        let http_client = reqwest::Client::new();

        let initial_rt = r#"overrides:
  t-st:
    float_chunk_encoding: xor2
    histogram_chunk_encoding: histogram_st
    native_histograms_ingestion_enabled: true
    out_of_order_time_window: 2h
  t-rt:
    float_chunk_encoding: xor2
    out_of_order_time_window: 0s
"#;

        let mut pair = Vec::new();
        for &which in &IMPLS {
            let d = tmp.path().join(which.name());
            pair.push(boot_wave4_ingester(which, "w4-tsdb", &d, initial_rt, &http_client).await);
        }
        let [mut go, mut rust]: [RunningIngester; 2] = pair.try_into().ok().unwrap();

        let now = now_ms();
        const TWO_HOURS_MS: i64 = 2 * 3_600_000;
        let boundary = (now / TWO_HOURS_MS) * TWO_HOURS_MS;
        let t_before_boundary = boundary - 60_000;
        let t_after_boundary = boundary + 10_000;
        let t0 = now - 60_000;

        // =========================================================================
        // 1. W4-04: Native histogram 2h Head chunk rollover CounterResetHeader
        //    across all 4 encodings (Histogram, FloatHistogram, HistogramSt, FloatHistogramSt).
        // =========================================================================
        for ing in [&mut go.ing, &mut rust.ing] {
            for tenant in ["t-def", "t-st"] {
                for k in 0..6i64 {
                    let ts = t_before_boundary + k * 1_000;
                    ing.push(with_org(
                        tenant,
                        cortexpb::WriteRequest {
                            timeseries: vec![
                                cortexpb::TimeSeries {
                                    labels: vec![lp("__name__", "w4_rollover_ih_noreset")],
                                    histograms: vec![make_int_h(ts, 0, 2, 5 + k, 2, (10 + k) as f64)],
                                    ..Default::default()
                                },
                                cortexpb::TimeSeries {
                                    labels: vec![lp("__name__", "w4_rollover_ih_reset")],
                                    histograms: vec![make_int_h(ts, 0, 2, 10 + k, 2, (20 + k) as f64)],
                                    ..Default::default()
                                },
                                cortexpb::TimeSeries {
                                    labels: vec![lp("__name__", "w4_rollover_fh_noreset")],
                                    histograms: vec![make_float_h(
                                        ts,
                                        0,
                                        2,
                                        5.0 + k as f64,
                                        2.0,
                                        (10 + k) as f64,
                                    )],
                                    ..Default::default()
                                },
                                cortexpb::TimeSeries {
                                    labels: vec![lp("__name__", "w4_rollover_fh_reset")],
                                    histograms: vec![make_float_h(
                                        ts,
                                        0,
                                        2,
                                        10.0 + k as f64,
                                        2.0,
                                        (20 + k) as f64,
                                    )],
                                    ..Default::default()
                                },
                            ],
                            ..Default::default()
                        },
                    ))
                    .await
                    .expect("push pre-boundary histograms");
                }
                ing.push(with_org(
                    tenant,
                    cortexpb::WriteRequest {
                        timeseries: vec![
                            cortexpb::TimeSeries {
                                labels: vec![lp("__name__", "w4_rollover_ih_noreset")],
                                histograms: vec![make_int_h(t_after_boundary, 0, 2, 20, 3, 50.0)],
                                ..Default::default()
                            },
                            cortexpb::TimeSeries {
                                labels: vec![lp("__name__", "w4_rollover_ih_reset")],
                                histograms: vec![make_int_h(t_after_boundary, 0, 2, 1, 0, 2.0)],
                                ..Default::default()
                            },
                            cortexpb::TimeSeries {
                                labels: vec![lp("__name__", "w4_rollover_fh_noreset")],
                                histograms: vec![make_float_h(
                                    t_after_boundary,
                                    0,
                                    2,
                                    20.0,
                                    3.0,
                                    50.0,
                                )],
                                ..Default::default()
                            },
                            cortexpb::TimeSeries {
                                labels: vec![lp("__name__", "w4_rollover_fh_reset")],
                                histograms: vec![make_float_h(
                                    t_after_boundary,
                                    0,
                                    2,
                                    1.0,
                                    0.0,
                                    2.0,
                                )],
                                ..Default::default()
                            },
                        ],
                        ..Default::default()
                    },
                ))
                .await
                .expect("push post-boundary histograms");
            }
        }

        let q_min = boundary - 4 * 3_600_000;
        let q_max = now + 60_000;
        for tenant in ["t-def", "t-st"] {
            let g = query_stream_detailed(
                &mut go.ing,
                tenant,
                q_min,
                q_max,
                vec![re("__name__", "w4_rollover_.*")],
            )
            .await;
            let r = query_stream_detailed(
                &mut rust.ing,
                tenant,
                q_min,
                q_max,
                vec![re("__name__", "w4_rollover_.*")],
            )
            .await;
            assert_eq!(g, r, "W4-04 rollover CounterResetHeader mismatch for {tenant}");
            assert!(g.contains("crh=40"), "expected NotCounterReset crh=40 in {g}");
            assert!(g.contains("crh=80"), "expected CounterReset crh=80 in {g}");
        }

        // =========================================================================
        // 2. W4-02: Overlapping OOO + In-Order Head Chunks (gap & lookback windows)
        // =========================================================================
        for ing in [&mut go.ing, &mut rust.ing] {
            for tenant in ["t-def", "t-st"] {
                ing.push(with_org(
                    tenant,
                    cortexpb::WriteRequest {
                        timeseries: vec![
                            cortexpb::TimeSeries {
                                labels: vec![lp("__name__", "w4_ooo_gap")],
                                samples: vec![
                                    cortexpb::Sample {
                                        value: 1.0,
                                        timestamp_ms: t0,
                                    },
                                    cortexpb::Sample {
                                        value: 4.0,
                                        timestamp_ms: t0 + 20_000,
                                    },
                                ],
                                ..Default::default()
                            },
                            cortexpb::TimeSeries {
                                labels: vec![lp("__name__", "w4_ooo_hist")],
                                histograms: vec![
                                    make_int_h(t0, 0, 2, 2, 1, 5.0),
                                    make_int_h(t0 + 20_000, 0, 2, 8, 2, 20.0),
                                ],
                                ..Default::default()
                            },
                        ],
                        ..Default::default()
                    },
                ))
                .await
                .expect("push in-order for overlap");

                ing.push(with_org(
                    tenant,
                    cortexpb::WriteRequest {
                        timeseries: vec![
                            cortexpb::TimeSeries {
                                labels: vec![lp("__name__", "w4_ooo_gap")],
                                samples: vec![
                                    cortexpb::Sample {
                                        value: 2.0,
                                        timestamp_ms: t0 + 2_000,
                                    },
                                    cortexpb::Sample {
                                        value: 3.0,
                                        timestamp_ms: t0 + 18_000,
                                    },
                                ],
                                ..Default::default()
                            },
                            cortexpb::TimeSeries {
                                labels: vec![lp("__name__", "w4_ooo_hist")],
                                histograms: vec![make_int_h(t0 + 5_000, 0, 2, 4, 1, 10.0)],
                                ..Default::default()
                            },
                        ],
                        ..Default::default()
                    },
                ))
                .await
                .expect("push ooo for overlap");
            }
        }

        for tenant in ["t-def", "t-st"] {
            // Gap window [t0+8s, t0+12s] overlaps BOTH chunks, 0 samples inside [mint, maxt]!
            let g_gap = query_stream_detailed(
                &mut go.ing,
                tenant,
                t0 + 8_000,
                t0 + 12_000,
                vec![re("__name__", "w4_ooo_.*")],
            )
            .await;
            let r_gap = query_stream_detailed(
                &mut rust.ing,
                tenant,
                t0 + 8_000,
                t0 + 12_000,
                vec![re("__name__", "w4_ooo_.*")],
            )
            .await;
            assert_eq!(g_gap, r_gap, "W4-02 gap query mismatch for {tenant}");
            assert_ne!(g_gap, "(0 series)");

            // Partial overlap [t0+1.5s, t0+3s] must preserve surrounding lookback samples!
            let g_part = query_stream_detailed(
                &mut go.ing,
                tenant,
                t0 + 1_500,
                t0 + 3_000,
                vec![eq("__name__", "w4_ooo_gap")],
            )
            .await;
            let r_part = query_stream_detailed(
                &mut rust.ing,
                tenant,
                t0 + 1_500,
                t0 + 3_000,
                vec![eq("__name__", "w4_ooo_gap")],
            )
            .await;
            assert_eq!(g_part, r_part, "W4-02 partial overlap mismatch for {tenant}");
        }

        // =========================================================================
        // 3. W4-07: Runtime float_chunk_encoding xor2 -> xor keeps filling active Xor2 chunk
        // =========================================================================
        for ing in [&mut go.ing, &mut rust.ing] {
            ing.push(with_org(
                "t-rt",
                cortexpb::WriteRequest {
                    timeseries: vec![cortexpb::TimeSeries {
                        labels: vec![lp("__name__", "w4_rt_xor2_to_xor")],
                        samples: vec![cortexpb::Sample {
                            value: 0.0,
                            timestamp_ms: t0,
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            ))
            .await
            .expect("push initial xor2 sample");
        }

        for &which in &IMPLS {
            let rt_path = tmp.path().join(which.name()).join("runtime.yaml");
            std::fs::write(&rt_path, "overrides: {}\n").unwrap();
        }
        // Wait until both Go and Rust reload runtime_config (`overrides: {}`).
        for (label, port) in [("go", go.http_port), ("rust", rust.http_port)] {
            let mut reloaded = false;
            for _ in 0..40 {
                if let Ok((code, _hdrs, body)) =
                    http_get(&http_client, port, "/runtime_config?mode=diff", false).await
                {
                    if code == 200 && body.trim() == "{}" {
                        reloaded = true;
                        break;
                    }
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            assert!(reloaded, "{label} did not reload runtime_config within 4s");
        }

        for ing in [&mut go.ing, &mut rust.ing] {
            for k in 1..=245i64 {
                ing.push(with_org(
                    "t-rt",
                    cortexpb::WriteRequest {
                        timeseries: vec![cortexpb::TimeSeries {
                            labels: vec![lp("__name__", "w4_rt_xor2_to_xor")],
                            samples: vec![cortexpb::Sample {
                                value: k as f64,
                                timestamp_ms: t0 + k * 10,
                            }],
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                ))
                .await
                .expect("push post-reload sample");
            }
        }

        let g_rt = query_stream_detailed(
            &mut go.ing,
            "t-rt",
            t0 - 1_000,
            t0 + 10_000,
            vec![eq("__name__", "w4_rt_xor2_to_xor")],
        )
        .await;
        let r_rt = query_stream_detailed(
            &mut rust.ing,
            "t-rt",
            t0 - 1_000,
            t0 + 10_000,
            vec![eq("__name__", "w4_rt_xor2_to_xor")],
        )
        .await;
        assert_eq!(g_rt, r_rt, "W4-07 runtime xor2->xor mismatch");

        go.stop().await;
        rust.stop().await;
    });
}

#[test]
fn w4_01_partial_tombstones_ulid_block_and_head_wal_parity() {
    block_on_for_tests(async {
        let tmp = tempfile::tempdir().unwrap();
        let http_client = reqwest::Client::new();

        let now = now_ms();
        let t_blk = now - 3 * 3_600_000;
        let t_hd = now - 60_000;

        // Seed a shared TSDB tenant directory with:
        //  1) A ULID block containing Xor, Xor2, HistogramSt, and a fully-deleted series via 2 partial intervals.
        //  2) A Head WAL containing a float series + a partial WAL Tombstones record.
        let seed_tenant = tmp.path().join("seed_tomb");
        std::fs::create_dir_all(&seed_tenant).unwrap();

        let make_ih_local = |cnt: u64, d0: i64, d1: i64, sum: f64| Histogram {
            count: cnt,
            zero_count: 1,
            zero_threshold: 0.001,
            sum,
            schema: 2,
            positive_spans: vec![HistogramSpan {
                offset: 0,
                length: 2,
            }],
            positive_buckets: vec![d0, d1],
            negative_spans: Vec::new(),
            negative_buckets: Vec::new(),
            custom_values: Vec::new(),
            counter_reset_hint: CounterResetHint::NotCounterReset,
        };

        let chks_xor = encode_samples_into_chunks_with_encodings(
            &[
                DecodedSample { st: 0, t: t_blk + 1_000, value: SampleValue::Float(10.0) },
                DecodedSample { st: 0, t: t_blk + 2_000, value: SampleValue::Float(20.0) },
                DecodedSample { st: 0, t: t_blk + 3_000, value: SampleValue::Float(30.0) },
                DecodedSample { st: 0, t: t_blk + 4_000, value: SampleValue::Float(40.0) },
            ],
            120,
            Encoding::Xor,
            Encoding::Histogram,
            Encoding::FloatHistogram,
        )
        .unwrap();

        let chks_xor2 = encode_samples_into_chunks_with_encodings(
            &[
                DecodedSample { st: 0, t: t_blk + 1_000, value: SampleValue::Float(11.0) },
                DecodedSample { st: 0, t: t_blk + 2_000, value: SampleValue::Float(22.0) },
                DecodedSample { st: 0, t: t_blk + 4_000, value: SampleValue::Float(44.0) },
            ],
            120,
            Encoding::Xor2,
            Encoding::HistogramSt,
            Encoding::FloatHistogramSt,
        )
        .unwrap();

        let chks_hst = encode_samples_into_chunks_with_encodings(
            &[
                DecodedSample {
                    st: 0,
                    t: t_blk + 1_000,
                    value: SampleValue::Histogram(Box::new(Histogram {
                        counter_reset_hint: CounterResetHint::CounterReset,
                        ..make_ih_local(8, 5, 2, 10.0)
                    })),
                },
                DecodedSample {
                    st: 0,
                    t: t_blk + 2_000,
                    value: SampleValue::Histogram(Box::new(make_ih_local(10, 7, 2, 20.0))),
                },
                DecodedSample {
                    st: 0,
                    t: t_blk + 4_000,
                    value: SampleValue::Histogram(Box::new(make_ih_local(12, 9, 2, 30.0))),
                },
            ],
            120,
            Encoding::Xor2,
            Encoding::HistogramSt,
            Encoding::FloatHistogramSt,
        )
        .unwrap();

        let chks_gone = encode_samples_into_chunks_with_encodings(
            &[
                DecodedSample { st: 0, t: t_blk + 1_000, value: SampleValue::Float(1.0) },
                DecodedSample { st: 0, t: t_blk + 4_000, value: SampleValue::Float(4.0) },
            ],
            120,
            Encoding::Xor,
            Encoding::Histogram,
            Encoding::FloatHistogram,
        )
        .unwrap();

        let meta = write_block_from_series(
            &seed_tenant,
            t_blk,
            t_blk + 10_000,
            vec![
                CompactionSeries {
                    labels: Labels::from_pairs(vec![("__name__", "s_blk_gone")]),
                    chunks: chks_gone,
                    tombstones: Vec::new(),
                },
                CompactionSeries {
                    labels: Labels::from_pairs(vec![("__name__", "s_blk_hst")]),
                    chunks: chks_hst,
                    tombstones: Vec::new(),
                },
                CompactionSeries {
                    labels: Labels::from_pairs(vec![("__name__", "s_blk_xor")]),
                    chunks: chks_xor,
                    tombstones: Vec::new(),
                },
                CompactionSeries {
                    labels: Labels::from_pairs(vec![("__name__", "s_blk_xor2")]),
                    chunks: chks_xor2,
                    tombstones: Vec::new(),
                },
            ],
            BlockMetaCompaction::default(),
            false,
            DEFAULT_CHUNK_SEGMENT_SIZE,
        )
        .unwrap()
        .unwrap();

        let blk_path = seed_tenant.join(meta.ulid.to_string());
        let blk = mimir_tsdb::block::Block::open(&blk_path).unwrap();
        let ref_gone = blk
            .postings_for_matchers(&[mimir_tsdb::matcher::LabelMatcher::new(
                mimir_tsdb::matcher::MatchType::Equal,
                "__name__",
                "s_blk_gone",
            )
            .unwrap()])
            .unwrap()[0];
        let ref_hst = blk
            .postings_for_matchers(&[mimir_tsdb::matcher::LabelMatcher::new(
                mimir_tsdb::matcher::MatchType::Equal,
                "__name__",
                "s_blk_hst",
            )
            .unwrap()])
            .unwrap()[0];
        let ref_xor = blk
            .postings_for_matchers(&[mimir_tsdb::matcher::LabelMatcher::new(
                mimir_tsdb::matcher::MatchType::Equal,
                "__name__",
                "s_blk_xor",
            )
            .unwrap()])
            .unwrap()[0];
        let ref_xor2 = blk
            .postings_for_matchers(&[mimir_tsdb::matcher::LabelMatcher::new(
                mimir_tsdb::matcher::MatchType::Equal,
                "__name__",
                "s_blk_xor2",
            )
            .unwrap()])
            .unwrap()[0];
        drop(blk);

        let tombs = MemTombstones::new();
        tombs.add_interval(ref_xor, Interval::new(t_blk + 1_500, t_blk + 3_500));
        tombs.add_interval(ref_xor2, Interval::new(t_blk + 1_500, t_blk + 3_500));
        tombs.add_interval(ref_hst, Interval::new(t_blk + 1_500, t_blk + 2_500));
        tombs.add_interval(ref_gone, Interval::new(t_blk, t_blk + 5_000));
        write_tombstones_file(&blk_path, &tombs).unwrap();

        let hd_ref = {
            let head = Head::open(&seed_tenant, HeadOptions::default(), i64::MIN).unwrap();
            let r = {
                let mut app = head.appender();
                let mut r = None;
                for off in [1_000i64, 2_000, 3_000, 4_000] {
                    r = Some(
                        app.append_float(r, &[("__name__", "s_head_partial")], t_hd + off, off as f64)
                            .unwrap(),
                    );
                }
                app.commit().unwrap();
                r.unwrap()
            };
            head.close().unwrap();
            r
        };
        {
            let mut wal = WlogWriter::with_segment_size(
                seed_tenant.join("wal"),
                128 * 1024,
                CompressionType::None,
            )
            .unwrap();
            let rec = RecordEncoder::new(false).tombstones(
                &[Tombstone {
                    ref_id: hd_ref,
                    mint: t_hd + 1_500,
                    maxt: t_hd + 3_500,
                }],
                Vec::new(),
            );
            wal.log(&[&rec]).unwrap();
            wal.close().unwrap();
        }

        let mut pair = Vec::new();
        for &which in &IMPLS {
            let d = tmp.path().join(which.name());
            copy_dir_recursive(&seed_tenant, &d.join("tsdb").join("t-tomb"));
            pair.push(boot_wave4_ingester(which, "w4-tomb", &d, "overrides: {}\n", &http_client).await);
        }
        let [mut go, mut rust]: [RunningIngester; 2] = pair.try_into().ok().unwrap();

        // Query narrow window [t_blk+3.8s, t_blk+4.2s] -> DisableTrimming must keep t_blk+1s & t_blk+4s!
        for metric in ["s_blk_xor", "s_blk_xor2", "s_blk_hst", "s_blk_gone"] {
            let g = query_stream_detailed(
                &mut go.ing,
                "t-tomb",
                t_blk + 3_800,
                t_blk + 4_200,
                vec![eq("__name__", metric)],
            )
            .await;
            let r = query_stream_detailed(
                &mut rust.ing,
                "t-tomb",
                t_blk + 3_800,
                t_blk + 4_200,
                vec![eq("__name__", metric)],
            )
            .await;
            assert_eq!(g, r, "W4-01 ULID block partial tombstone mismatch for {metric}");
        }

        // Query narrow window [t_hd+3.8s, t_hd+4.2s] on replayed Head WAL!
        let g_hd = query_stream_detailed(
            &mut go.ing,
            "t-tomb",
            t_hd + 3_800,
            t_hd + 4_200,
            vec![eq("__name__", "s_head_partial")],
        )
        .await;
        let r_hd = query_stream_detailed(
            &mut rust.ing,
            "t-tomb",
            t_hd + 3_800,
            t_hd + 4_200,
            vec![eq("__name__", "s_head_partial")],
        )
        .await;
        assert_eq!(g_hd, r_hd, "W4-01 Head WAL partial tombstone mismatch");

        go.stop().await;
        rust.stop().await;
    });
}
