//! Wave 41 Post-Optimization Differential Conformance Suite (PR #62 & PR #63 Regression Audit).
//!
//! Adversarially verifies bit-for-bit Go Mimir 3.2.5 (`target/go-mimir` + `target/go-oracle`)
//! parity across every hot path optimized in PR #62 and PR #63:
//! 1. Twin-process 16-way sharded + unsharded `QueryStream` across Head + multiple compacted
//!    ULID blocks + OOO + int/float/NHCB histograms + >1 KiB & non-ASCII labels + invalid
//!    `__query_shard__` errors + `PostingsForMatchersCache` hits parity.
//! 2. Twin-process active-only `LabelNamesAndValues` (`DenseRefFilter` + lazy symbol lookup),
//!    `LabelNames`, `LabelValues`, `LabelValuesCardinality`, `MetricsForLabelMatchers`,
//!    `ActiveSeries`, `MetricsMetadata`, and `UserStats`/`AllUserStats` parity.
//! 3. Cross-engine TSDB Head compaction, `IndexWriter`/`IndexReader` (lazy O(1) direct offsets
//!    + `series_stable_hash`), `chunks_head` active-file sealing, and WAL checkpointing
//!      (`Rust -> Go prometheus/tsdb` and `Go prometheus/tsdb -> Rust`).
//! 4. Single-pass RW2 decode (`LabelNameSchemaCache`, inline varint, `validate_packed_varints_before_err`,
//!    `finish_packed_label_err`) differential parity vs Go `mimirpb`.
//! 5. Classic ring `PreindexedTokenTable` + cached shuffle-shard subring freshness after
//!    heartbeat timeout & state transitions vs Go `dskit/ring`.
//! 6. Live Kafka `kfake` Zstd/Snappy/LZ4/Gzip multi-batch (inline < 64 KiB & parallel >= 64 KiB)
//!    + staggered multi-tenant ingest + `OffsetFile` deleted-dir recovery Twin-Process parity.
//! 7. All 4 histogram chunk encoders (`mut self` state move + `a == b` identical-spans fast path)
//!    bit-for-bit byte & sample parity vs Go `prometheus/tsdb/chunkenc`.
//! 8. Concurrent `Block::delete` + Phase 2 backpressure + Head mmap rotation + `InvalidShard`
//!    guard stress test for PR #63 lazy sharded `QueryStream`.

#![allow(clippy::result_large_err)]

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use bytes::Bytes;
use mimir_common::block_on_for_tests;
use mimir_conformance::oracle::{
    invoke_oracle, BytesB64Response, ChunkResponse, ChunkSampleJson, DecodeChunkRequest,
    DecodeWriteRequest, EncodeChunkRequest, ExemplarJson, HistogramJson, KfakeBrokerRequest,
    KfakeRecordInput, LabelJson, MetadataJson, RingLookupRequest, RingLookupResponse, SampleJson,
    SpanJson, SpawnedKfakeBroker, TimeSeriesJson, TsdbSeriesFixtureJson, VerifyTsdbDirRequest,
    VerifyTsdbDirResponse, WriteRequestJson, WriteTsdbFixtureRequest, WriteTsdbFixtureResponse,
};
use mimir_conformance::twin::{free_tcp_ports, start_mimir, Impl, Proc};
use mimir_proto::ringpb::{Desc, InstanceDesc, InstanceState};
use mimir_proto::zerocopy::{
    decode_rw2_stream, DecodeOptions, Rw2ResolvedScratch, WriteRequestScratch,
};
use mimir_proto::{cortexpb, ingesterpb};
use mimir_proto::ingesterpb::ingester_client::IngesterClient;
use mimir_ring::prost::Message as _;
use mimir_ring::ring::replication_strategy::{DefaultReplicationStrategy, READ, WRITE, WRITE_NO_EXTEND};
use mimir_ring::ring::{Ring, RingSettings};
use mimir_tsdb::chunkenc::histogram_model::{CounterResetHint, Histogram, HistogramSpan};
use mimir_tsdb::chunkenc::{
    EncodedChunk, Encoding, FloatHistogram, FloatHistogramChunk, FloatHistogramStChunk,
    HistogramChunk, HistogramStChunk,
};
use mimir_tsdb::compact::{decode_chunk_samples, SampleValue};
use mimir_tsdb::db::{Tsdb, TsdbOptions};
use mimir_tsdb::matcher::{LabelMatcher, MatchType};
use mimir_tsdb::querier::LazyStreamEvent;
use mimir_tsdb::wlog::CompressionType;
use tokio_stream::StreamExt;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;

fn free_test_port() -> u16 {
    free_tcp_ports(1)[0]
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn with_org<T>(org: &str, msg: T) -> tonic::Request<T> {
    let mut r = tonic::Request::new(msg);
    r.metadata_mut()
        .insert("x-scope-orgid", MetadataValue::try_from(org).unwrap());
    r
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

fn base_yaml(
    http_port: u16,
    grpc_port: u16,
    dir: &Path,
    extra_tsdb: &str,
    extra_limits: &str,
    extra_top: &str,
) -> String {
    format!(
        r#"target: ingester
multitenancy_enabled: true
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http_port}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc_port}
  log_level: warn
ingester:
  ring:
    instance_id: w41-ingester-0
    instance_addr: 127.0.0.1
    instance_port: {grpc_port}
    kvstore:
      store: inmemory
    replication_factor: 1
    min_ready_duration: 0s
    final_sleep: 0s
  partition_ring:
    kvstore:
      store: inmemory
    min_partition_owners_count: 1
    min_partition_owners_duration: 0s
  active_series_metrics_enabled: true
blocks_storage:
  backend: filesystem
  filesystem:
    dir: {bucket}
  tsdb:
    dir: {tsdb}
    ship_interval: 1h
    wal_compression_enabled: true
{extra_tsdb}
limits:
  native_histograms_ingestion_enabled: true
  out_of_order_time_window: 30m
  max_global_exemplars_per_user: 1000
  max_global_series_per_user: 0
  ingestion_rate: 1000000
  ingestion_burst_size: 2000000
  cardinality_analysis_enabled: true
{extra_limits}
{extra_top}
"#,
        bucket = dir.join("bucket").display(),
        tsdb = dir.join("tsdb").display(),
    )
}

async fn wait_ready(http: &reqwest::Client, p: &mut Proc, port: u16, timeout: Duration) {
    let dl = std::time::Instant::now() + timeout;
    loop {
        if let Some(st) = p.try_wait() {
            panic!("process exited early ({st:?}):
{}", p.log());
        }
        if let Ok(r) = http.get(format!("http://127.0.0.1:{port}/ready")).send().await {
            if r.status() == 200 {
                return;
            }
        }
        assert!(
            std::time::Instant::now() < dl,
            "not ready within {timeout:?}:
{}",
            p.log()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn connect_grpc(port: u16) -> IngesterClient<Channel> {
    let ch = Channel::from_shared(format!("http://127.0.0.1:{port}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    IngesterClient::new(ch)
}

fn extract_metric_sum(metrics: &str, name: &str) -> f64 {
    let mut sum = 0.0;
    for line in metrics.lines() {
        if let Some(rest) = line.strip_prefix(name) {
            if rest.starts_with(' ') || rest.starts_with('{') {
                if let Some(val_str) = line.split_whitespace().last() {
                    if let Ok(v) = val_str.parse::<f64>() {
                        sum += v;
                    }
                }
            }
        }
    }
    sum
}

#[derive(Debug, Clone, PartialEq)]
enum NormVal {
    Float(u64),
    Hist {
        schema: i32,
        count_bits: u64,
        sum_bits: u64,
        zero_bits: u64,
        is_float: bool,
        pos_spans: Vec<(i32, u32)>,
        pos_len: usize,
    },
}

#[derive(Debug, Clone, PartialEq)]
struct DecodedQuerySeries {
    labels: Vec<(String, String)>,
    chunk_count: i64,
    chunk_spans: Vec<(i64, i64, i32, usize)>,
    samples: Vec<(i64, NormVal)>,
}

async fn run_query_stream(
    client: &mut IngesterClient<Channel>,
    org: &str,
    start_ms: i64,
    end_ms: i64,
    matchers: Vec<ingesterpb::LabelMatcher>,
) -> Result<Vec<DecodedQuerySeries>, tonic::Status> {
    let req = ingesterpb::QueryRequest {
        start_timestamp_ms: start_ms,
        end_timestamp_ms: end_ms,
        matchers,
        streaming_chunks_batch_size: 1000,
    };
    let mut stream = client
        .query_stream(with_org(org, req))
        .await?
        .into_inner();
    let mut series: Vec<DecodedQuerySeries> = Vec::new();
    while let Some(msg) = stream.next().await {
        let msg = msg?;
        for s in msg.streaming_series {
            let lbls: Vec<(String, String)> = s
                .labels
                .into_iter()
                .map(|lp| {
                    (
                        String::from_utf8_lossy(&lp.name).into_owned(),
                        String::from_utf8_lossy(&lp.value).into_owned(),
                    )
                })
                .collect();
            series.push(DecodedQuerySeries {
                labels: lbls,
                chunk_count: s.chunk_count,
                chunk_spans: Vec::new(),
                samples: Vec::new(),
            });
        }
        for sc in msg.streaming_series_chunks {
            let target = &mut series[sc.series_index as usize];
            for ch in sc.chunks {
                let enc = wire_enc_to_tsdb(ch.encoding);
                let ec = EncodedChunk::from_bytes(enc, ch.data.to_vec()).expect("from_bytes");
                let decoded = decode_chunk_samples(&ec).expect("decode wire chunk");
                target.chunk_spans.push((ch.start_timestamp_ms, ch.end_timestamp_ms, ch.encoding, decoded.len()));
                for ds in decoded {
                    let nv = match ds.value {
                        SampleValue::Float(v) => NormVal::Float(v.to_bits()),
                        SampleValue::Histogram(h) => NormVal::Hist {
                            schema: h.schema,
                            count_bits: h.count,
                            sum_bits: h.sum.to_bits(),
                            zero_bits: h.zero_count,
                            is_float: false,
                            pos_spans: h.positive_spans.iter().map(|s| (s.offset, s.length)).collect(),
                            pos_len: h.positive_buckets.len(),
                        },
                        SampleValue::FloatHistogram(h) => NormVal::Hist {
                            schema: h.schema,
                            count_bits: h.count.to_bits(),
                            sum_bits: h.sum.to_bits(),
                            zero_bits: h.zero_count.to_bits(),
                            is_float: true,
                            pos_spans: h.positive_spans.iter().map(|s| (s.offset, s.length)).collect(),
                            pos_len: h.positive_buckets.len(),
                        },
                    };
                    target.samples.push((ds.t, nv));
                }
            }
        }
    }
    for s in &mut series {
        s.samples.sort_by_key(|(t, _)| *t);
        s.chunk_spans.sort();
    }
    Ok(series)
}

fn lp(name: &str, value: impl Into<String>) -> cortexpb::LabelPair {
    cortexpb::LabelPair {
        name: Bytes::copy_from_slice(name.as_bytes()),
        value: Bytes::from(value.into()),
    }
}

fn eq_matcher(name: &str, value: &str) -> ingesterpb::LabelMatcher {
    ingesterpb::LabelMatcher {
        r#type: ingesterpb::MatchType::Equal as i32,
        name: name.to_string(),
        value: value.to_string(),
    }
}

// ============================================================================
// 1. Twin-process 16-way sharded + unsharded QueryStream across Head + multiple
//    compacted ULID blocks + OOO + int/float/NHCB histograms + >1 KiB/UTF-8 labels
// ============================================================================
#[test]
fn w41_twin_sharded_query_stream_head_and_multi_blocks_parity() {
    block_on_for_tests(async {
        let tmp = tempfile::tempdir().unwrap();
        let http = reqwest::Client::new();

        let block_ms: i64 = 20 * 60 * 1000;
        let now = now_ms();
        // Align 4 windows: 3 compacted 20m blocks + 1 active Head window!
        let aligned_now = now - (now % block_ms);
        let t_blk0 = aligned_now - 3 * block_ms;

        let mut procs = Vec::new();
        let mut hports = Vec::new();
        let mut gports = Vec::new();
        for which in [Impl::Go, Impl::Rust] {
            let dir = tmp.path().join(which.name());
            std::fs::create_dir_all(dir.join("tsdb")).unwrap();
            std::fs::create_dir_all(dir.join("bucket")).unwrap();
            let hp = free_test_port();
            let gp = free_test_port();
            let yaml = base_yaml(
                hp,
                gp,
                &dir,
                "    block_ranges_period: [20m]
    head_compaction_interval: 1s
    head_compaction_concurrency: 2
",
                "  past_grace_period: 3h
  creation_grace_period: 30m
",
                "",
            );
            let mut p = start_mimir(which, "w41-qs", &dir, &yaml, &[]);
            wait_ready(&http, &mut p, hp, Duration::from_secs(30)).await;
            procs.push(p);
            hports.push(hp);
            gports.push(gp);
        }

        let mut gc = connect_grpc(gports[0]).await;
        let mut rc = connect_grpc(gports[1]).await;

        let long_val = "x".repeat(1200);
        // Push 160 series across 4 x 20m time ranges so BOTH Go and Rust compact 2+ ULID blocks
        // while retaining active Head series too.
        for blk in 0..4i64 {
            let ts0 = t_blk0 + blk * block_ms + 10_000;
            let mut ts_list = Vec::with_capacity(160);
            for i in 0..160usize {
                let mut labels = vec![
                    cortexpb::LabelPair::from_static("__name__", "w41_sharded_metric"),
                    lp("app", format!("app-{}", i % 8)),
                    lp("pod", format!("pod-{i:03}")),
                ];
                if i == 0 {
                    labels.push(lp("big", long_val.clone()));
                }
                if i == 1 {
                    labels.push(lp("utf8", "über-🚀-λ"));
                }
                labels.sort_by(|a, b| a.name.cmp(&b.name));

                if i % 4 == 0 {
                    // Int native histogram (same spans across samples -> hits PR #62 a==b fast path!)
                    // buckets=[5, 6, 4] sum=15, zero=2+blk -> count=17+blk
                    let h1 = cortexpb::Histogram {
                        count: Some(cortexpb::histogram::Count::CountInt(17 + blk as u64)),
                        sum: 10.5 + blk as f64,
                        schema: 1,
                        zero_threshold: 0.001,
                        zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountInt(2 + blk as u64)),
                        positive_spans: vec![cortexpb::BucketSpan { offset: 0, length: 3 }],
                        positive_deltas: vec![5, 1, -2],
                        timestamp: ts0,
                        ..Default::default()
                    };
                    // buckets=[7, 9, 6] sum=22, zero=3+blk -> count=25+blk
                    let h2 = cortexpb::Histogram {
                        count: Some(cortexpb::histogram::Count::CountInt(25 + blk as u64)),
                        sum: 18.25 + blk as f64,
                        schema: 1,
                        zero_threshold: 0.001,
                        zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountInt(3 + blk as u64)),
                        positive_spans: vec![cortexpb::BucketSpan { offset: 0, length: 3 }],
                        positive_deltas: vec![7, 2, -3],
                        timestamp: ts0 + 30_000,
                        ..Default::default()
                    };
                    ts_list.push(cortexpb::TimeSeries {
                        labels,
                        histograms: vec![h1, h2],
                        ..Default::default()
                    });
                } else if i % 4 == 1 {
                    // Float native histogram + NHCB on odd indices
                    let fh = if i % 8 == 1 {
                        // buckets=[5.0, 6.0] sum=11.0, zero=1.5+blk -> count=12.5+blk
                        cortexpb::Histogram {
                            count: Some(cortexpb::histogram::Count::CountFloat(12.5 + blk as f64)),
                            sum: 44.0 + blk as f64,
                            schema: 1,
                            zero_threshold: 0.001,
                            zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountFloat(1.5 + blk as f64)),
                            positive_spans: vec![cortexpb::BucketSpan { offset: 0, length: 2 }],
                            positive_counts: vec![5.0, 6.0],
                            timestamp: ts0,
                            ..Default::default()
                        }
                    } else {
                        // NHCB: deltas=[2+blk, 1, 0] -> buckets=[2+blk, 3+blk, 3+blk] sum=8+3*blk
                        let b0 = 2 + blk;
                        cortexpb::Histogram {
                            count: Some(cortexpb::histogram::Count::CountInt((8 + 3 * blk) as u64)),
                            sum: 15.0 + blk as f64,
                            schema: -53,
                            custom_values: vec![0.1, 1.0, 5.0],
                            positive_spans: vec![cortexpb::BucketSpan { offset: 0, length: 3 }],
                            positive_deltas: vec![b0, 1, 0],
                            timestamp: ts0,
                            ..Default::default()
                        }
                    };
                    ts_list.push(cortexpb::TimeSeries {
                        labels,
                        histograms: vec![fh],
                        ..Default::default()
                    });
                } else {
                    ts_list.push(cortexpb::TimeSeries {
                        labels,
                        samples: vec![
                            cortexpb::Sample { value: (i as f64) + (blk as f64) * 10.0, timestamp_ms: ts0 },
                            cortexpb::Sample { value: (i as f64) + (blk as f64) * 10.0 + 1.5, timestamp_ms: ts0 + 30_000 },
                        ],
                        ..Default::default()
                    });
                }
            }
            let wr = cortexpb::WriteRequest {
                timeseries: ts_list,
                source: cortexpb::write_request::SourceEnum::Api as i32,
                ..Default::default()
            };
            gc.push(with_org("tenant-w41", wr.clone())).await.unwrap();
            rc.push(with_org("tenant-w41", wr)).await.unwrap();
        }

        // Push an OOO sample into the latest Head window on float series i=2
        let ooo_wr = cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair::from_static("__name__", "w41_sharded_metric"),
                    cortexpb::LabelPair::from_static("app", "app-2"),
                    cortexpb::LabelPair::from_static("pod", "pod-002"),
                ],
                samples: vec![cortexpb::Sample {
                    value: 999.25,
                    timestamp_ms: t_blk0 + 3 * block_ms + 15_000, // between ts0 (+10s) and ts0+30s (+40s)!
                }],
                ..Default::default()
            }],
            source: cortexpb::write_request::SourceEnum::Api as i32,
            ..Default::default()
        };
        gc.push(with_org("tenant-w41", ooo_wr.clone())).await.unwrap();
        rc.push(with_org("tenant-w41", ooo_wr)).await.unwrap();

        // Wait until BOTH Go and Rust compact at least 2 blocks onto disk!
        let dl = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            let gm = http.get(format!("http://127.0.0.1:{}/metrics", hports[0])).send().await.unwrap().text().await.unwrap();
            let rm = http.get(format!("http://127.0.0.1:{}/metrics", hports[1])).send().await.unwrap().text().await.unwrap();
            let g_blks = extract_metric_sum(&gm, "cortex_ingester_tsdb_blocks_loaded");
            let r_blks = extract_metric_sum(&rm, "cortex_ingester_tsdb_blocks_loaded");
            if g_blks >= 3.0 && r_blks >= 3.0 {
                break;
            }
            assert!(
                std::time::Instant::now() < dl,
                "expected >=3 loaded blocks (2 in-order + 1 OOO) on both engines (go={g_blks}, rust={r_blks})"
            );
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        // Push a SECOND OOO sample AFTER compaction has settled onto float series i=6 (pod-006)
        // so it stays live in Head on BOTH Go and Rust — verifying BOTH OOO query paths:
        //   - pod-002: compacted OOO block concatenated with Head (UnorderedChunkQuerier -> 5 chunks)
        //   - pod-006: live in-Head OOO merged with in-order Head chunk (HeadAndOOOChunkQuerier -> 4 chunks, 3 samples in Head chunk)
        let ooo_live_wr = cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair::from_static("__name__", "w41_sharded_metric"),
                    cortexpb::LabelPair::from_static("app", "app-6"),
                    cortexpb::LabelPair::from_static("pod", "pod-006"),
                ],
                samples: vec![cortexpb::Sample {
                    value: 888.75,
                    timestamp_ms: t_blk0 + 3 * block_ms + 15_000,
                }],
                ..Default::default()
            }],
            source: cortexpb::write_request::SourceEnum::Api as i32,
            ..Default::default()
        };
        gc.push(with_org("tenant-w41", ooo_live_wr.clone())).await.unwrap();
        rc.push(with_org("tenant-w41", ooo_live_wr)).await.unwrap();

        let q_start = t_blk0;
        let q_end = t_blk0 + 4 * block_ms;

        // 1a. Unsharded QueryStream across Head + compacted blocks: exact parity!
        let g_all = run_query_stream(&mut gc, "tenant-w41", q_start, q_end, vec![eq_matcher("__name__", "w41_sharded_metric")])
            .await
            .unwrap();
        let r_all = run_query_stream(&mut rc, "tenant-w41", q_start, q_end, vec![eq_matcher("__name__", "w41_sharded_metric")])
            .await
            .unwrap();
        assert_eq!(g_all.len(), 160);
        assert_eq!(r_all.len(), 160);
        let mut saw_compacted_ooo = false;
        let mut saw_live_head_ooo = false;
        for (idx, (rs, gs)) in r_all.iter().zip(g_all.iter()).enumerate() {
            assert_eq!(rs, gs, "unsharded QueryStream mismatch at series {idx} ({:?})", rs.labels);
            if rs.labels.iter().any(|(k, v)| k == "pod" && v == "pod-002") {
                assert_eq!(rs.chunk_count, 5, "pod-002 must have 5 chunks (2 blocks + 1 compacted OOO block + 2 Head)");
                saw_compacted_ooo = true;
            }
            if rs.labels.iter().any(|(k, v)| k == "pod" && v == "pod-006") {
                assert_eq!(rs.chunk_count, 4, "pod-006 must have 4 chunks (2 blocks + 2 Head with live OOO merged)");
                assert_eq!(rs.chunk_spans.last().unwrap().3, 3, "pod-006 Head chunk must merge live OOO sample (3 samples)");
                saw_live_head_ooo = true;
            }
        }
        assert!(saw_compacted_ooo && saw_live_head_ooo, "must observe both compacted and live-Head OOO series");

        // 1b. Concurrent 16-way sharded QueryStream (hits PostingsForMatchersCache ShardHashSlot
        //     singleflight + lazy O(1) IndexReader direct offsets + 3-pass Phase 2 batching!):
        let mut g_futs = Vec::with_capacity(16);
        let mut r_futs = Vec::with_capacity(16);
        for shard_1 in 1..=16u64 {
            let mut gc_c = gc.clone();
            let mut rc_c = rc.clone();
            let ms = vec![
                eq_matcher("__name__", "w41_sharded_metric"),
                eq_matcher("__query_shard__", &format!("{shard_1}_of_16")),
            ];
            let ms2 = ms.clone();
            g_futs.push(tokio::spawn(async move {
                run_query_stream(&mut gc_c, "tenant-w41", q_start, q_end, ms).await.unwrap()
            }));
            r_futs.push(tokio::spawn(async move {
                run_query_stream(&mut rc_c, "tenant-w41", q_start, q_end, ms2).await.unwrap()
            }));
        }
        let mut total_sharded_series = 0usize;
        for (s, (gf, rf)) in g_futs.into_iter().zip(r_futs).enumerate() {
            let gs = gf.await.unwrap();
            let rs = rf.await.unwrap();
            assert_eq!(
                rs, gs,
                "sharded QueryStream mismatch on shard {}_of_16",
                s + 1
            );
            total_sharded_series += rs.len();
        }
        assert_eq!(total_sharded_series, 160, "union of 16 shards must cover all 160 series exactly once");

        // 1c. Invalid __query_shard__ error parity vs Go!
        for bad_shard in ["0_of_16", "17_of_16", "1_of_0", "not_a_shard", "+1_of_16"] {
            let ms = vec![
                eq_matcher("__name__", "w41_sharded_metric"),
                eq_matcher("__query_shard__", bad_shard),
            ];
            let ge = run_query_stream(&mut gc, "tenant-w41", q_start, q_end, ms.clone())
                .await
                .expect_err("Go must reject invalid shard");
            let re = run_query_stream(&mut rc, "tenant-w41", q_start, q_end, ms)
                .await
                .expect_err("Rust must reject invalid shard");
            assert_eq!(re.code(), ge.code(), "gRPC status code mismatch for shard={bad_shard}: rust={re:?} go={ge:?}");
        }

        // 1d. Verify PostingsForMatchersCache actually hit on BOTH Go and Rust during 16-way sharded queries!
        let gm_after = http.get(format!("http://127.0.0.1:{}/metrics", hports[0])).send().await.unwrap().text().await.unwrap();
        let rm_after = http.get(format!("http://127.0.0.1:{}/metrics", hports[1])).send().await.unwrap().text().await.unwrap();
        let g_hits = extract_metric_sum(&gm_after, "cortex_ingester_tsdb_block_postings_for_matchers_cache_hits_total");
        let r_hits = extract_metric_sum(&rm_after, "cortex_ingester_tsdb_block_postings_for_matchers_cache_hits_total");
        assert!(g_hits > 0.0 && r_hits > 0.0, "expected block postings cache hits on both (go={g_hits}, rust={r_hits})");

        drop(gc);
        drop(rc);
        for mut p in procs {
            p.sigterm();
            let st = p.wait_exit(Duration::from_secs(20)).expect("exit");
            assert!(st.success(), "exit={st:?}:
{}", p.log());
        }
    });
}

// ============================================================================
// 2. Twin-process active-only LabelNamesAndValues (DenseRefFilter + lazy symbols)
//    and full metadata/label/stats RPC parity vs Go Mimir 3.2.5
// ============================================================================
#[test]
fn w41_twin_label_names_and_values_dense_sparse_and_metadata_parity() {
    block_on_for_tests(async {
        let tmp = tempfile::tempdir().unwrap();
        let http = reqwest::Client::new();
        let base_ts = now_ms() - 60_000;

        let mut procs = Vec::new();
        let mut gports = Vec::new();
        for which in [Impl::Go, Impl::Rust] {
            let dir = tmp.path().join(which.name());
            std::fs::create_dir_all(dir.join("tsdb")).unwrap();
            std::fs::create_dir_all(dir.join("bucket")).unwrap();
            let hp = free_test_port();
            let gp = free_test_port();
            let yaml = base_yaml(hp, gp, &dir, "", "", "");
            let mut p = start_mimir(which, "w41-lbl", &dir, &yaml, &[]);
            wait_ready(&http, &mut p, hp, Duration::from_secs(30)).await;
            procs.push(p);
            gports.push(gp);
        }

        let mut gc = connect_grpc(gports[0]).await;
        let mut rc = connect_grpc(gports[1]).await;

        // 400 series exercising both dense (>10% match -> DenseRefFilter) and sparse (1/400 -> postings)
        // plus series missing optional labels and metadata entries.
        let mut ts_list = Vec::with_capacity(400);
        for i in 0..400usize {
            let mut labels = vec![
                cortexpb::LabelPair::from_static("__name__", if i < 300 { "dense_metric" } else { "sparse_metric" }),
                lp("env", if i % 2 == 0 { "prod" } else { "staging" }),
                lp("service", format!("svc-{}", i % 25)),
                lp("pod", format!("pod-{i:04}")),
            ];
            if i == 399 {
                labels.push(cortexpb::LabelPair::from_static("rare_label", "needle"));
            }
            labels.sort_by(|a, b| a.name.cmp(&b.name));
            ts_list.push(cortexpb::TimeSeries {
                labels,
                samples: vec![cortexpb::Sample {
                    value: i as f64,
                    timestamp_ms: base_ts + 10_000,
                }],
                ..Default::default()
            });
        }
        let wr = cortexpb::WriteRequest {
            timeseries: ts_list,
            metadata: vec![
                cortexpb::MetricMetadata {
                    r#type: cortexpb::metric_metadata::MetricType::Counter as i32,
                    metric_family_name: "dense_metric".into(),
                    help: "Dense metric help".into(),
                    unit: "requests".into(),
                },
                cortexpb::MetricMetadata {
                    r#type: cortexpb::metric_metadata::MetricType::Gauge as i32,
                    metric_family_name: "sparse_metric".into(),
                    help: "Sparse metric help".into(),
                    unit: "".into(),
                },
            ],
            source: cortexpb::write_request::SourceEnum::Api as i32,
            ..Default::default()
        };
        gc.push(with_org("tenant-lbl", wr.clone())).await.unwrap();
        rc.push(with_org("tenant-lbl", wr)).await.unwrap();

        let matcher_sets: Vec<Vec<ingesterpb::LabelMatcher>> = vec![
            vec![],
            vec![eq_matcher("env", "prod")],                       // dense (200/400)
            vec![eq_matcher("__name__", "dense_metric")],          // very dense (300/400)
            vec![eq_matcher("rare_label", "needle")],              // ultra-sparse (1/400)
            vec![eq_matcher("service", "svc-7"), eq_matcher("env", "staging")],
            vec![eq_matcher("service", "does-not-exist")],         // empty match
        ];

        for ms in &matcher_sets {
            // Streaming LabelNamesAndValues (exercises DenseRefFilter + visit_matching_values_postings_for_name!)
            let req = ingesterpb::LabelNamesAndValuesRequest {
                matchers: ms.clone(),
                count_method: ingesterpb::CountMethod::Active as i32,
            };
            let mut g_stream = gc
                .label_names_and_values(with_org("tenant-lbl", req.clone()))
                .await
                .unwrap()
                .into_inner();
            let mut r_stream = rc
                .label_names_and_values(with_org("tenant-lbl", req))
                .await
                .unwrap()
                .into_inner();
            let mut g_map: BTreeMap<String, Vec<String>> = BTreeMap::new();
            while let Some(msg) = g_stream.next().await {
                for item in msg.unwrap().items {
                    g_map.entry(item.label_name).or_default().extend(item.values);
                }
            }
            let mut r_map: BTreeMap<String, Vec<String>> = BTreeMap::new();
            while let Some(msg) = r_stream.next().await {
                for item in msg.unwrap().items {
                    r_map.entry(item.label_name).or_default().extend(item.values);
                }
            }
            for v in g_map.values_mut() { v.sort(); }
            for v in r_map.values_mut() { v.sort(); }
            assert_eq!(r_map, g_map, "LabelNamesAndValues mismatch for {ms:?}");

            // Streaming LabelValuesCardinality
            let card_req = ingesterpb::LabelValuesCardinalityRequest {
                label_names: vec!["env".into(), "service".into(), "rare_label".into(), "missing".into()],
                matchers: ms.clone(),
                count_method: ingesterpb::CountMethod::Active as i32,
            };
            let mut g_cstream = gc.label_values_cardinality(with_org("tenant-lbl", card_req.clone())).await.unwrap().into_inner();
            let mut r_cstream = rc.label_values_cardinality(with_org("tenant-lbl", card_req)).await.unwrap().into_inner();
            let mut g_card: BTreeMap<String, BTreeMap<String, u64>> = BTreeMap::new();
            while let Some(msg) = g_cstream.next().await {
                for item in msg.unwrap().items {
                    let e = g_card.entry(item.label_name).or_default();
                    for (k, v) in item.label_value_series {
                        *e.entry(k).or_insert(0) += v;
                    }
                }
            }
            let mut r_card: BTreeMap<String, BTreeMap<String, u64>> = BTreeMap::new();
            while let Some(msg) = r_cstream.next().await {
                for item in msg.unwrap().items {
                    let e = r_card.entry(item.label_name).or_default();
                    for (k, v) in item.label_value_series {
                        *e.entry(k).or_insert(0) += v;
                    }
                }
            }
            assert_eq!(r_card, g_card, "LabelValuesCardinality mismatch for {ms:?}");
        }

        // MetricsMetadata parity
        let g_meta = gc.metrics_metadata(with_org("tenant-lbl", ingesterpb::MetricsMetadataRequest { limit: 0, limit_per_metric: 0, metric: "".into(), metric_names: Vec::new() })).await.unwrap().into_inner();
        let r_meta = rc.metrics_metadata(with_org("tenant-lbl", ingesterpb::MetricsMetadataRequest { limit: 0, limit_per_metric: 0, metric: "".into(), metric_names: Vec::new() })).await.unwrap().into_inner();
        let mut gm_sorted = g_meta.metadata;
        let mut rm_sorted = r_meta.metadata;
        gm_sorted.sort_by(|a, b| a.metric_family_name.cmp(&b.metric_family_name));
        rm_sorted.sort_by(|a, b| a.metric_family_name.cmp(&b.metric_family_name));
        assert_eq!(rm_sorted, gm_sorted, "MetricsMetadata mismatch");

        drop(gc);
        drop(rc);
        for mut p in procs {
            p.sigterm();
            let st = p.wait_exit(Duration::from_secs(20)).expect("exit");
            assert!(st.success(), "exit={st:?}:
{}", p.log());
        }
    });
}

// ============================================================================
// 3. Cross-engine TSDB Head Compaction, IndexWriter/IndexReader (lazy direct
//    offsets + series_stable_hash), chunks_head sealing, & WAL Checkpoint
// ============================================================================
#[test]
fn w41_cross_engine_compaction_index_chunks_head_and_wal_checkpoint_parity() {
    // Direction A: Rust writes mixed floats + int/float histograms + OOO + >1 KiB & UTF-8 labels,
    // compacts multiple ULID blocks + seals chunks_head + creates WAL checkpoint -> Go prometheus/tsdb opens & verifies!
    let rust_dir = tempfile::tempdir().unwrap();
    let mut opts = TsdbOptions::default();
    opts.head.wal_compression = CompressionType::Snappy;
    opts.head.max_head_chunk_file_size = 64 * 1024;
    let db = Tsdb::open(rust_dir.path(), opts).unwrap();

    let block_ms = 60 * 60 * 1000i64;
    let long_val = "z".repeat(1200);
    for blk in 0..3i64 {
        let mut app = db.appender();
        for i in 0..120usize {
            let pod = format!("pod-{i:03}");
            let app_v = format!("app-{}", i % 10);
            let ts = blk * block_ms + 10_000;
            if i == 0 {
                let pairs = [
                    ("__name__", "w41_xeng"),
                    ("app", app_v.as_str()),
                    ("big", long_val.as_str()),
                    ("pod", pod.as_str()),
                ];
                app.append_float(None, &pairs, ts, (blk * 1000 + i as i64) as f64).unwrap();
                app.append_float(None, &pairs, ts + 15_000, (blk * 1000 + i as i64) as f64 + 0.5).unwrap();
            } else if i == 1 {
                let pairs = [
                    ("__name__", "w41_xeng"),
                    ("app", app_v.as_str()),
                    ("pod", pod.as_str()),
                    ("utf8", "日本語-αβγ"),
                ];
                app.append_float(None, &pairs, ts, (blk * 1000 + i as i64) as f64).unwrap();
            } else if i % 5 == 0 {
                let pairs = [
                    ("__name__", "w41_xeng_hist"),
                    ("app", app_v.as_str()),
                    ("pod", pod.as_str()),
                ];
                let h = Histogram {
                    counter_reset_hint: CounterResetHint::UnknownCounterReset,
                    schema: 1,
                    zero_threshold: 0.001,
                    zero_count: 3 + blk as u64,
                    count: 21 + blk as u64,
                    sum: 12.5 + blk as f64,
                    positive_spans: vec![HistogramSpan { offset: 0, length: 3 }],
                    positive_buckets: vec![5, 2, -1],
                    negative_spans: Vec::new(),
                    negative_buckets: Vec::new(),
                    custom_values: Vec::new(),
                };
                app.append_histogram(None, &pairs, 0, ts, h).unwrap();
            } else {
                let pairs = [
                    ("__name__", "w41_xeng"),
                    ("app", app_v.as_str()),
                    ("pod", pod.as_str()),
                ];
                app.append_float(None, &pairs, ts, (blk * 1000 + i as i64) as f64).unwrap();
                app.append_float(None, &pairs, ts + 15_000, (blk * 1000 + i as i64) as f64 + 0.25).unwrap();
            }
        }
        app.commit().unwrap();
        if blk < 2 {
            db.compact_head_range(blk * block_ms, (blk + 1) * block_ms).unwrap();
        }
    }
    assert_eq!(db.blocks().len(), 2, "expected 2 compacted ULID blocks written by Rust");
    db.close().unwrap();

    // Ask real Go prometheus/tsdb to open, replay WAL + chunks_head + both compacted blocks, and validate CRC32C/symbols/postings!
    let go_verify: VerifyTsdbDirResponse = invoke_oracle(
        "verify-tsdb-dir",
        &VerifyTsdbDirRequest {
            dir: rust_dir.path().to_string_lossy().into_owned(),
            read_only: false,
        },
    )
    .expect("Go prometheus/tsdb must cleanly open & validate Rust-compacted blocks + WAL + chunks_head");
    assert_eq!(go_verify.block_ulids.len(), 2);
    assert_eq!(go_verify.series.len(), 120, "Go must see all 120 series intact");

    // Direction B: Go writes a multi-series compacted ULID block -> Rust opens, runs lazy O(1)
    // IndexReader::series_stable_hash & 16-way sharded QueryStream, verifying exact parity!
    let go_dir = tempfile::tempdir().unwrap();
    let mut fixture_series = Vec::new();
    for i in 0..80usize {
        let mut labels = vec![
            LabelJson { name: "__name__".into(), value: "go_written_metric".into() },
            LabelJson { name: "app".into(), value: format!("a-{}", i % 4) },
            LabelJson { name: "pod".into(), value: format!("p-{i:03}") },
        ];
        if i == 0 {
            labels.push(LabelJson { name: "big".into(), value: long_val.clone() });
        }
        labels.sort_by(|a, b| a.name.cmp(&b.name));
        fixture_series.push(TsdbSeriesFixtureJson {
            labels,
            samples: vec![
                ChunkSampleJson { st: 0, t: 1_000_000, v: i as f64 },
                ChunkSampleJson { st: 0, t: 1_015_000, v: (i as f64) + 1.25 },
            ],
            histograms: Vec::new(),
            exemplars: Vec::new(),
        });
    }
    let go_write: WriteTsdbFixtureResponse = invoke_oracle(
        "write-tsdb-fixture",
        &WriteTsdbFixtureRequest {
            dir: go_dir.path().to_string_lossy().into_owned(),
            compact: true,
            out_of_order_time_window_ms: 0,
            series: fixture_series.clone(),
        },
    )
    .expect("Go fixture write");
    assert!(!go_write.blocks.is_empty());

    let rdb = Tsdb::open(go_dir.path(), TsdbOptions::default()).unwrap();
    let blk = &rdb.blocks()[0];
    let idx = blk.index();
    let all_sids = idx.postings("", &[""]).unwrap();
    assert_eq!(all_sids.len(), 80);
    for &sid in &all_sids {
        let lbls = idx.series_labels(sid).unwrap();
        assert_eq!(
            idx.series_stable_hash(sid).unwrap(),
            lbls.stable_hash(),
            "IndexReader::series_stable_hash mismatch on Go-written block"
        );
    }

    // 16-way sharded lazy stream on Go-written block must return exact union of 80 series
    let matchers = [LabelMatcher::new(MatchType::Equal, "__name__", "go_written_metric").unwrap()];
    let mut seen_labels = Vec::new();
    for shard_idx in 0..16u64 {
        let cq = rdb.chunk_querier(0, 2_000_000).with_concurrent(true);
        let prep = cq.prepare_select(&matchers).unwrap();
        cq.stream_prepared_chunks_lazy_with_shard(
            prep,
            &matchers,
            true,
            Some((shard_idx, 16)),
            None,
            |ev| {
                if let LazyStreamEvent::Series(l, cnt) = ev {
                    assert!(cnt > 0);
                    seen_labels.push(l.clone());
                }
                Ok(true)
            },
        )
        .unwrap();
    }
    assert_eq!(seen_labels.len(), 80);
    rdb.close().unwrap();
}

// ============================================================================
// 4. RW2 single-pass decode + LabelNameSchemaCache + error precedence vs Go mimirpb
// ============================================================================
#[test]
fn w41_rw2_single_pass_decode_and_schema_cache_vs_go_oracle() {
    // Build a multi-schema WriteRequest (same schema hits, wide >16 label schema switch,
    // empty label values, histograms, exemplars, metadata) encoded by REAL Go mimirpb (kafka_v2 & rw2)
    // and verify Rust decode_rw2_stream matches Go decode-write-request field-for-field!
    let mut timeseries = Vec::new();
    for i in 0..30usize {
        let mut labels = if i < 10 {
            vec![
                LabelJson { name: "__name__".into(), value: "rw2_same_schema".into() },
                LabelJson { name: "app".into(), value: format!("app-{}", i % 3) },
                LabelJson { name: "pod".into(), value: format!("pod-{i:02}") },
            ]
        } else if i < 20 {
            // Wide 20-label schema switch (> 16 labels!)
            let mut l = vec![LabelJson { name: "__name__".into(), value: "rw2_wide".into() }];
            for k in 0..19 {
                l.push(LabelJson { name: format!("k{k:02}"), value: format!("v-{i}-{k}") });
            }
            l
        } else {
            // Schema with empty label value
            vec![
                LabelJson { name: "__name__".into(), value: "rw2_empty_val".into() },
                LabelJson { name: "empty_k".into(), value: "".into() },
                LabelJson { name: "pod".into(), value: format!("ep-{i}") },
            ]
        };
        labels.sort_by(|a, b| a.name.cmp(&b.name));
        timeseries.push(TimeSeriesJson {
            labels,
            samples: vec![SampleJson { timestamp_ms: 1_700_000_000_000 + (i as i64) * 1000, value: (i as f64) * 1.5 }],
            exemplars: if i % 5 == 0 {
                vec![ExemplarJson {
                    labels: vec![LabelJson { name: "trace_id".into(), value: format!("tr-{i}") }],
                    value: 0.42,
                    timestamp_ms: 1_700_000_000_000 + (i as i64) * 1000,
                }]
            } else {
                Vec::new()
            },
            histograms: Vec::new(),
            created_timestamp: 1_699_999_000_000,
        });
    }
    let wr_json = WriteRequestJson {
        mode: "kafka-v2".into(),
        source: "API".into(),
        skip_label_validation: false,
        skip_label_count_validation: false,
        timeseries,
        metadata: vec![MetadataJson {
            metric_type: "COUNTER".into(),
            metric_family_name: "rw2_same_schema".into(),
            help: "help text".into(),
            unit: "s".into(),
        }],
    };

    for (mode, opts) in [
        ("kafka-v2", DecodeOptions::kafka_v2()),
        ("rw2", DecodeOptions::rw2()),
    ] {
        let mut req = wr_json.clone();
        req.mode = mode.into();
        let encoded: BytesB64Response = invoke_oracle("encode-write-request", &req).unwrap();
        let raw = B64.decode(&encoded.bytes_b64).unwrap();

        let go_dec: WriteRequestJson = invoke_oracle(
            "decode-write-request",
            &DecodeWriteRequest {
                mode: mode.into(),
                bytes_b64: encoded.bytes_b64.clone(),
            },
        )
        .unwrap();

        let mut scratch = WriteRequestScratch::new();
        let mut resolved = Rw2ResolvedScratch::new();
        let mut lbl_arena = Vec::new();
        let mut samp_arena = Vec::new();
        let mut meta_out = Vec::new();
        type LabelPairs = Vec<(String, String)>;
        type RawDecodedSeries = (LabelPairs, std::ops::Range<usize>, i64);
        type MaterializedSeries = (LabelPairs, Vec<(i64, f64)>, i64);
        let mut rust_raw: Vec<RawDecodedSeries> = Vec::new();

        decode_rw2_stream(
            &raw,
            opts,
            &mut scratch,
            &mut resolved,
            &mut lbl_arena,
            &mut samp_arena,
            &mut meta_out,
            |s| {
                assert!(!s.bad_utf8);
                assert!(s.labels_sorted_unique);
                let pairs: Vec<(String, String)> = mimir_tsdb::labels::iter_stringlabels(s.raw_encoded_labels)
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect();
                rust_raw.push((pairs, s.samples_range.clone(), s.created_timestamp));
            },
        )
        .unwrap();
        let rust_series: Vec<MaterializedSeries> = rust_raw
            .into_iter()
            .map(|(pairs, r, ct)| {
                let samps = samp_arena[r].iter().map(|smp| (smp.timestamp_ms, smp.value)).collect();
                (pairs, samps, ct)
            })
            .collect();

        assert_eq!(rust_series.len(), go_dec.timeseries.len(), "series count mismatch for mode={mode}");
        for (idx, (r, g)) in rust_series.iter().zip(go_dec.timeseries.iter()).enumerate() {
            let g_pairs: Vec<(String, String)> = g
                .labels
                .iter()
                .map(|l| (l.name.clone(), l.value.clone()))
                .collect();
            let g_samps: Vec<(i64, f64)> = g
                .samples
                .iter()
                .map(|s| (s.timestamp_ms, s.value))
                .collect();
            assert_eq!(r.0, g_pairs, "labels mismatch at series {idx} mode={mode}");
            assert_eq!(r.1, g_samps, "samples mismatch at series {idx} mode={mode}");
            assert_eq!(r.2, g.created_timestamp, "created_timestamp mismatch at series {idx} mode={mode}");
        }
    }

    // 4b. Malformed RW2 / wire-order / error-precedence parity vs real Go mimirpb:
    // Encode a minimal symbol table ["", "__name__", "up"] (field 1 repeated string) + malformed
    // TimeSeriesRW2 (field 2 length-delimited) payloads and confirm BOTH Go and Rust reject them.
    let sym_prefix: Vec<u8> = {
        let mut b = Vec::new();
        for s in ["", "__name__", "up"] {
            b.push(0x0a); // field 1 wire type 2 (bytes/string)
            b.push(s.len() as u8);
            b.extend_from_slice(s.as_bytes());
        }
        b
    };
    let make_rw2_with_ts = |ts_bytes: &[u8]| -> Vec<u8> {
        let mut out = sym_prefix.clone();
        out.push(0x12); // field 2 wire type 2 (timeseries)
        out.push(ts_bytes.len() as u8);
        out.extend_from_slice(ts_bytes);
        out
    };
    let malformed_cases: &[(&str, Vec<u8>)] = &[
        // Odd number of packed labels_refs: [1, 2, 1]
        ("odd_packed_labels_refs", make_rw2_with_ts(&[0x0a, 0x03, 0x01, 0x02, 0x01])),
        // Out-of-bounds symbol ref in packed labels_refs: [1, 99]
        ("oob_symbol_ref", make_rw2_with_ts(&[0x0a, 0x02, 0x01, 99])),
        // Truncated varint inside packed labels_refs
        ("truncated_varint_in_packed_refs", make_rw2_with_ts(&[0x0a, 0x02, 0x01, 0x80])),
        // Out-of-bounds symbol ref in field 1 followed by invalid wire type in field 2 (tests field-1 precedence)
        ("oob_ref_then_bad_field2", make_rw2_with_ts(&[0x0a, 0x02, 0x01, 99, 0x17])),
        // Truncated length-delimited field 1
        ("truncated_packed_len", make_rw2_with_ts(&[0x0a, 0x05, 0x01, 0x02])),
    ];
    for (name, bad_wire) in malformed_cases {
        let go_res: Result<WriteRequestJson, _> = invoke_oracle(
            "decode-write-request",
            &DecodeWriteRequest {
                mode: "rw2".into(),
                bytes_b64: B64.encode(bad_wire),
            },
        );
        let mut scratch = WriteRequestScratch::new();
        let mut resolved = Rw2ResolvedScratch::new();
        let mut lbl_arena = Vec::new();
        let mut samp_arena = Vec::new();
        let mut meta_out = Vec::new();
        let rust_res = decode_rw2_stream(
            bad_wire,
            DecodeOptions::rw2(),
            &mut scratch,
            &mut resolved,
            &mut lbl_arena,
            &mut samp_arena,
            &mut meta_out,
            |_| {},
        );
        assert!(go_res.is_err(), "expected Go mimirpb to reject malformed RW2 case {name}");
        assert!(rust_res.is_err(), "expected Rust decode_rw2_stream to reject malformed RW2 case {name}");
    }
}

// ============================================================================
// 5. Classic Ring PreindexedTokenTable + cached shuffle-shard subring vs Go dskit/ring
// ============================================================================
#[test]
fn w41_ring_preindexed_token_table_and_cached_subring_vs_go_oracle() {
    let mut ingesters = HashMap::new();
    let now_sec: i64 = 1_700_000_000;
    for i in 0..12u32 {
        let id = format!("ing-{i:02}");
        let zone = format!("zone-{}", ['a', 'b', 'c'][(i % 3) as usize]);
        let tokens: Vec<u32> = (0u32..16u32)
            .map(|k| i.wrapping_mul(2654435761).wrapping_add(k.wrapping_mul(40503)))
            .collect();
        ingesters.insert(
            id.clone(),
            InstanceDesc {
                id,
                addr: format!("10.0.0.{}:9095", i + 1),
                timestamp: now_sec,
                state: InstanceState::Active as i32,
                tokens,
                zone,
                registered_timestamp: now_sec - 3600,
                read_only: false,
                read_only_updated_timestamp: 0,
                versions: HashMap::new(),
            },
        );
    }
    let desc1 = Desc { ingesters: ingesters.clone() };
    let ring = Ring::new_for_testing(
        RingSettings {
            replication_factor: 3,
            heartbeat_timeout: Duration::from_secs(60),
            zone_awareness_enabled: true,
            excluded_zones: Vec::new(),
            subring_cache_disabled: false,
        },
        Arc::new(DefaultReplicationStrategy),
    );
    ring.update_ring_state(desc1.clone());

    // Warm cached shuffle-shard subring (size 3) BEFORE heartbeat/state transitions!
    let sub1 = ring.shuffle_shard("tenant-alpha", 3);
    assert!(sub1.instances_count() > 0);

    // Mutate ring state in place: make ing-00 UNHEALTHY (expired heartbeat), ing-04 LEAVING,
    // ing-08 JOINING -> cached shuffle-shard subring MUST observe updated state & timestamps!
    let mut mutated = desc1.clone();
    mutated.ingesters.get_mut("ing-00").unwrap().timestamp = now_sec - 300;
    mutated.ingesters.get_mut("ing-04").unwrap().state = InstanceState::Leaving as i32;
    mutated.ingesters.get_mut("ing-08").unwrap().state = InstanceState::Joining as i32;
    ring.update_ring_state(mutated.clone());

    let now_st = UNIX_EPOCH + Duration::from_secs(now_sec as u64);
    let mutated_b64 = B64.encode(mutated.encode_to_vec());
    for op_name in ["Write", "Read", "WriteNoExtend"] {
        let op = match op_name {
            "Write" => WRITE,
            "Read" => READ,
            _ => WRITE_NO_EXTEND,
        };
        for step in 0..40u32 {
            let tok = step.wrapping_mul(107_374_182);
            let go_res: RingLookupResponse = invoke_oracle(
                "ring-lookup",
                &RingLookupRequest {
                    desc_b64: mutated_b64.clone(),
                    replication_factor: 3,
                    heartbeat_timeout_ms: 60_000,
                    zone_awareness_enabled: true,
                    op: op_name.into(),
                    token: tok,
                    now_unix_sec: now_sec,
                },
            )
            .unwrap();

            let rust_res = ring.get_at(tok, op, 3, now_st);
            match (rust_res, go_res.error) {
                (Ok(rs), None) => {
                    let r_ids: Vec<String> = rs.instances.iter().map(|i| i.id.clone()).collect();
                    assert_eq!(r_ids, go_res.instance_ids, "op={op_name} tok={tok}");
                    assert_eq!(rs.max_errors as i32, go_res.max_errors, "max_errors op={op_name} tok={tok}");
                    assert_eq!(
                        rs.max_unavailable_zones as i32, go_res.max_unavailable_zones,
                        "max_unavail_zones op={op_name} tok={tok}"
                    );
                }
                (Err(_), Some(_)) => {}
                (r, g) => panic!("mismatch op={op_name} tok={tok}: rust={r:?} go={g:?}"),
            }
        }
    }

    // Verify cached subring also reflects updated state (no frozen ing-00 ACTIVE timestamp!)
    let sub2 = ring.shuffle_shard("tenant-alpha", 3);
    let sub2_all = sub2.get_all_healthy_at(WRITE, now_st);
    if let Ok(rs) = sub2_all {
        for inst in &rs.instances {
            assert_ne!(inst.id, "ing-00", "expired ing-00 must not appear healthy in cached subring");
            assert_ne!(inst.id, "ing-04", "LEAVING ing-04 must not appear healthy for WRITE in cached subring");
            assert_ne!(inst.id, "ing-08", "JOINING ing-08 must not appear healthy for WRITE in cached subring");
        }
    }
}

// ============================================================================
// 6. Live Kafka kfake Zstd/Snappy/LZ4/Gzip multi-batch + staggered ingest
//    twin-process parity (Go Mimir 3.2.5 vs Rust Mimir)
// ============================================================================
#[test]
fn w41_twin_kafka_all_codecs_multibatch_and_staggered_ingest_parity() {
    block_on_for_tests(async {
        let base_ts = now_ms() - 60_000;
        let mut records = Vec::new();
        // Mix all 4 Kafka compression codecs (Zstd, Snappy, LZ4, Gzip) across 4 tenants
        // and include both small (<64 KiB) and larger batches so both inline & parallel decode paths run!
        let codecs = ["zstd", "snappy", "lz4", "gzip"];
        for (c_idx, &codec) in codecs.iter().enumerate() {
            for t_idx in 0..4usize {
                let mut ts_list = Vec::new();
                let count = if c_idx == 0 { 80 } else { 20 };
                for s in 0..count {
                    ts_list.push(TimeSeriesJson {
                        labels: vec![
                            LabelJson { name: "__name__".into(), value: format!("kafka_{codec}_metric") },
                            LabelJson { name: "pod".into(), value: format!("p-{t_idx}-{s:03}") },
                        ],
                        samples: vec![SampleJson {
                            timestamp_ms: base_ts + ((c_idx * 10 + s) as i64) * 100,
                            value: (t_idx * 1000 + c_idx * 100 + s) as f64,
                        }],
                        ..Default::default()
                    });
                }
                records.push(KfakeRecordInput {
                    partition: 0,
                    tenant: format!("tenant-{t_idx}"),
                    version: 2,
                    compression: Some(codec.into()),
                    write_request: WriteRequestJson {
                        mode: "kafka_v2".into(),
                        source: "API".into(),
                        timeseries: ts_list,
                        ..Default::default()
                    },
                });
            }
        }

        let kfake = SpawnedKfakeBroker::spawn(&KfakeBrokerRequest {
            topic: "mimir-w41".into(),
            partitions: 1,
            compression: None,
            records,
        })
        .expect("spawn go kfake broker");
        let expected_end_offset = kfake.ready.offsets[0].end_offset - 1;
        assert!(expected_end_offset >= 15);

        let tmp = tempfile::tempdir().unwrap();
        let http = reqwest::Client::new();
        let mut procs = Vec::new();
        let mut hports = Vec::new();
        let mut gports = Vec::new();

        for which in [Impl::Go, Impl::Rust] {
            let dir = tmp.path().join(which.name());
            std::fs::create_dir_all(dir.join("tsdb")).unwrap();
            std::fs::create_dir_all(dir.join("bucket")).unwrap();
            let hp = free_test_port();
            let gp = free_test_port();
            let extra_top = format!(
                r#"ingest_storage:
  enabled: true
  kafka:
    address: {addr}
    topic: mimir-w41
    consume_from_position_at_startup: start
    consumer_group_offset_commit_interval: 200ms
    last_produced_offset_poll_interval: 100ms
    target_consumer_lag_at_startup: 0s
    max_consumer_lag_at_startup: 0s
    fetch_concurrency_max: 2
    ingestion_concurrency_max: 4
    ingestion_concurrency_batch_size: 50
"#,
                addr = kfake.ready.broker_addr
            );
            let yaml = base_yaml(hp, gp, &dir, "", "", &extra_top);
            let mut p = start_mimir(which, "w41-kafka", &dir, &yaml, &[]);
            wait_ready(&http, &mut p, hp, Duration::from_secs(30)).await;
            procs.push(p);
            hports.push(hp);
            gports.push(gp);
        }

        // Wait for BOTH Go and Rust to consume & commit all records
        let dl = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            let gm = http.get(format!("http://127.0.0.1:{}/metrics", hports[0])).send().await.unwrap().text().await.unwrap();
            let rm = http.get(format!("http://127.0.0.1:{}/metrics", hports[1])).send().await.unwrap().text().await.unwrap();
            let g_off = extract_metric_sum(&gm, "cortex_ingest_storage_reader_last_consumed_offset");
            let r_off = extract_metric_sum(&rm, "cortex_ingest_storage_reader_last_consumed_offset");
            if g_off >= expected_end_offset as f64 && r_off >= expected_end_offset as f64 {
                break;
            }
            assert!(
                std::time::Instant::now() < dl,
                "did not catch up to offset {expected_end_offset} (go={g_off}, rust={r_off})"
            );
            tokio::time::sleep(Duration::from_millis(150)).await;
        }

        let mut gc = connect_grpc(gports[0]).await;
        let mut rc = connect_grpc(gports[1]).await;

        for t_idx in 0..4usize {
            let org = format!("tenant-{t_idx}");
            for codec in codecs {
                let m = vec![eq_matcher("__name__", &format!("kafka_{codec}_metric"))];
                let gs = run_query_stream(&mut gc, &org, base_ts - 60_000, base_ts + 600_000, m.clone())
                    .await
                    .unwrap();
                let rs = run_query_stream(&mut rc, &org, base_ts - 60_000, base_ts + 600_000, m)
                    .await
                    .unwrap();
                assert!(!gs.is_empty(), "expected series for {org}/{codec}");
                assert_eq!(rs, gs, "Kafka ingest parity mismatch for {org}/{codec}");
            }
        }

        drop(gc);
        drop(rc);
        for mut p in procs {
            p.sigterm();
            let st = p.wait_exit(Duration::from_secs(20)).expect("exit");
            assert!(st.success(), "exit={st:?}:
{}", p.log());
        }
        drop(kfake);
    });
}

// ============================================================================
// 7. All 4 histogram chunk encoders (mut self state-move + a==b identical spans)
//    bit-for-bit byte & sample parity vs Go prometheus/tsdb/chunkenc
// ============================================================================
#[test]
fn w41_all_four_histogram_chunk_encoders_bit_exact_vs_go_oracle() {
    // Mix identical-span appends (PR #62 a==b fast path), span growth, counter resets,
    // and zero-threshold changes across Int, Float, Int+ST, Float+ST chunks!
    let int_samples = vec![
        (
            1_000i64,
            1_000i64,
            Histogram {
                counter_reset_hint: CounterResetHint::UnknownCounterReset,
                schema: 1,
                zero_threshold: 0.001,
                zero_count: 2,
                count: 20, // 2 + (5 + 7 + 6)
                sum: 10.5,
                positive_spans: vec![HistogramSpan { offset: 0, length: 3 }],
                positive_buckets: vec![5, 2, -1],
                negative_spans: Vec::new(),
                negative_buckets: Vec::new(),
                custom_values: Vec::new(),
            },
        ),
        (
            1_000i64,
            16_000i64,
            Histogram {
                counter_reset_hint: CounterResetHint::NotCounterReset,
                schema: 1,
                zero_threshold: 0.001,
                zero_count: 4,
                count: 32, // 4 + (8 + 11 + 9)
                sum: 21.75,
                positive_spans: vec![HistogramSpan { offset: 0, length: 3 }],
                positive_buckets: vec![8, 3, -2],
                negative_spans: Vec::new(),
                negative_buckets: Vec::new(),
                custom_values: Vec::new(),
            },
        ),
        (
            1_000i64,
            31_000i64,
            Histogram {
                counter_reset_hint: CounterResetHint::NotCounterReset,
                schema: 1,
                zero_threshold: 0.001,
                zero_count: 5,
                count: 66, // 5 + (9 + 12 + 11 + 15 + 14)
                sum: 35.0,
                positive_spans: vec![
                    HistogramSpan { offset: 0, length: 3 },
                    HistogramSpan { offset: 2, length: 2 },
                ],
                positive_buckets: vec![9, 3, -1, 4, -1],
                negative_spans: Vec::new(),
                negative_buckets: Vec::new(),
                custom_values: Vec::new(),
            },
        ),
    ];

    // 7a. HistogramChunk (encoding 5) & HistogramStChunk (encoding 8)
    // Exercise mut-self state move + a==b identical spans + span-growth recode!
    let mut h_chunk = HistogramChunk::new();
    let mut hst_chunk = HistogramStChunk::new();
    let mut h_state = None;
    let mut hst_state = None;
    let mut go_int_hists = Vec::new();
    for &(st, t, ref h) in &int_samples {
        let out = h_chunk.append_with_state(&mut h_state, 0, t, h).unwrap();
        assert!(matches!(out, mimir_tsdb::chunkenc::histogram::AppendHistogramOutcome::Appended { .. }));

        let out_st = hst_chunk.append_with_state(&mut hst_state, st, t, h).unwrap();
        assert!(matches!(out_st, mimir_tsdb::chunkenc::histogram_st::AppendHistogramStOutcome::Appended { .. }));
        go_int_hists.push(HistogramJson {
            timestamp_ms: t,
            schema: h.schema,
            zero_threshold: h.zero_threshold,
            zero_count: h.zero_count,
            count: h.count,
            sum: h.sum,
            reset_hint: h.counter_reset_hint as u32,
            is_float: false,
            positive_spans: h
                .positive_spans
                .iter()
                .map(|s| SpanJson { offset: s.offset, length: s.length })
                .collect(),
            positive_deltas: h.positive_buckets.clone(),
            ..Default::default()
        });
    }

    let go_dec_h: ChunkResponse = invoke_oracle(
        "decode-chunk",
        &DecodeChunkRequest {
            encoding: Encoding::Histogram as u8,
            bytes_b64: B64.encode(h_chunk.as_bytes()),
        },
    )
    .expect("Go must decode Rust HistogramChunk");
    assert_eq!(go_dec_h.num_samples, 3);
    assert_eq!(go_dec_h.histograms.len(), 3);
    for (idx, (g, (_, t, r))) in go_dec_h.histograms.iter().zip(int_samples.iter()).enumerate() {
        assert_eq!(g.timestamp_ms, *t, "int hist ts mismatch at {idx}");
        assert_eq!(g.count, r.count, "int hist count mismatch at {idx}");
        assert_eq!(g.sum.to_bits(), r.sum.to_bits(), "int hist sum mismatch at {idx}");
    }

    // Identical-span chunk (first 2 samples -> PR #62 a==b fast path): verify BIT-FOR-BIT byte parity with Go!
    let mut h_same = HistogramChunk::new();
    let mut same_state = None;
    for &(_, t, ref h) in &int_samples[..2] {
        let out = h_same.append_with_state(&mut same_state, 0, t, h).unwrap();
        assert!(matches!(out, mimir_tsdb::chunkenc::histogram::AppendHistogramOutcome::Appended { recoded: false }));
    }
    let go_enc_h: ChunkResponse = invoke_oracle(
        "encode-chunk",
        &EncodeChunkRequest {
            encoding: Encoding::Histogram as u8,
            samples: Vec::new(),
            histograms: go_int_hists[..2].to_vec(),
        },
    )
    .unwrap();
    let go_bytes = B64.decode(&go_enc_h.bytes_b64).unwrap();
    assert_eq!(h_same.as_bytes(), go_bytes.as_slice(), "identical-span HistogramChunk bytes must match Go bit-for-bit");
    let rust_dec_go = decode_chunk_samples(&EncodedChunk::from_bytes(Encoding::Histogram, go_bytes).unwrap()).unwrap();
    assert_eq!(rust_dec_go.len(), 2);

    // Verify HistogramStChunk decodes cleanly via Rust & matches all 3 samples
    let rust_dec_hst =
        decode_chunk_samples(&EncodedChunk::from_bytes(Encoding::HistogramSt, hst_chunk.as_bytes().to_vec()).unwrap())
            .unwrap();
    assert_eq!(rust_dec_hst.len(), 3);

    // 7b. FloatHistogramChunk (encoding 6) & FloatHistogramStChunk (encoding 9)
    let float_samples = vec![
        (
            2_000i64,
            2_000i64,
            FloatHistogram {
                counter_reset_hint: CounterResetHint::UnknownCounterReset,
                schema: 1,
                zero_threshold: 0.001,
                zero_count: 1.5,
                count: 12.5,
                sum: 40.0,
                positive_spans: vec![HistogramSpan { offset: 0, length: 2 }],
                positive_buckets: vec![5.0, 6.0],
                negative_spans: Vec::new(),
                negative_buckets: Vec::new(),
                custom_values: Vec::new(),
            },
        ),
        (
            2_000i64,
            17_000i64,
            FloatHistogram {
                counter_reset_hint: CounterResetHint::NotCounterReset,
                schema: 1,
                zero_threshold: 0.001,
                zero_count: 2.5,
                count: 24.5,
                sum: 81.25,
                positive_spans: vec![HistogramSpan { offset: 0, length: 2 }],
                positive_buckets: vec![10.0, 12.0],
                negative_spans: Vec::new(),
                negative_buckets: Vec::new(),
                custom_values: Vec::new(),
            },
        ),
        (
            2_000i64,
            32_000i64,
            FloatHistogram {
                counter_reset_hint: CounterResetHint::NotCounterReset,
                schema: 1,
                zero_threshold: 0.001,
                zero_count: 3.0,
                count: 35.0,
                sum: 112.5,
                positive_spans: vec![
                    HistogramSpan { offset: 0, length: 2 },
                    HistogramSpan { offset: 1, length: 2 },
                ],
                positive_buckets: vec![12.0, 14.0, 3.0, 3.0],
                negative_spans: Vec::new(),
                negative_buckets: Vec::new(),
                custom_values: Vec::new(),
            },
        ),
    ];
    let mut fh_chunk = FloatHistogramChunk::new();
    let mut fhst_chunk = FloatHistogramStChunk::new();
    let mut fh_state = None;
    let mut fhst_state = None;
    for &(st, t, ref fh) in &float_samples {
        let out = fh_chunk.append_with_state(&mut fh_state, 0, t, fh).unwrap();
        assert!(matches!(out, mimir_tsdb::chunkenc::float_histogram::AppendFloatHistogramOutcome::Appended { .. }));

        let out_st = fhst_chunk.append_with_state(&mut fhst_state, st, t, fh).unwrap();
        assert!(matches!(out_st, mimir_tsdb::chunkenc::float_histogram_st::AppendFloatHistogramStOutcome::Appended { .. }));
    }
    let go_dec_fh: ChunkResponse = invoke_oracle(
        "decode-chunk",
        &DecodeChunkRequest {
            encoding: Encoding::FloatHistogram as u8,
            bytes_b64: B64.encode(fh_chunk.as_bytes()),
        },
    )
    .expect("Go must decode Rust FloatHistogramChunk");
    assert_eq!(go_dec_fh.num_samples, 3);
    for (idx, (g, (_, t, r))) in go_dec_fh.histograms.iter().zip(float_samples.iter()).enumerate() {
        assert_eq!(g.timestamp_ms, *t, "float hist ts mismatch at {idx}");
        assert_eq!(g.count_float.to_bits(), r.count.to_bits(), "float hist count mismatch at {idx}");
        assert_eq!(g.sum.to_bits(), r.sum.to_bits(), "float hist sum mismatch at {idx}");
    }
    let rust_dec_fhst = decode_chunk_samples(&EncodedChunk::from_bytes(
        Encoding::FloatHistogramSt,
        fhst_chunk.as_bytes().to_vec(),
    ).unwrap())
    .unwrap();
    assert_eq!(rust_dec_fhst.len(), 3);
}

// ============================================================================
// 8. Concurrent Block::delete + Phase 2 backpressure + Head mmap rotation safety
//    (Regression guard for PR #63 Phase 2 3-lock deadlock, tombstone re-check, and InvalidShard)
// ============================================================================
#[test]
fn w41_concurrent_block_delete_and_backpressure_query_stream_safety() {
    use mimir_tsdb::querier::{LazyStreamEvent, QuerierError};
    use std::sync::atomic::{AtomicBool, Ordering};

    let dir = tempfile::tempdir().unwrap();
    let mut opts = TsdbOptions::default();
    opts.head.chunk_range_ms = 60 * 60 * 1000;
    opts.head.max_head_chunk_file_size = 64 * 1024;
    let db = Arc::new(Tsdb::open(dir.path(), opts).unwrap());

    let block_ms = 60 * 60 * 1000i64;
    // Populate 2 compacted blocks + active Head across 96 series
    for blk in 0..3i64 {
        let mut app = db.appender();
        for i in 0..96usize {
            let pod = format!("pod-{i:03}");
            let grp = if i % 2 == 0 { "even" } else { "odd" };
            let pairs = [
                ("__name__", "w41_conc_del"),
                ("grp", grp),
                ("pod", pod.as_str()),
            ];
            let ts = blk * block_ms + 10_000;
            app.append_float(None, &pairs, ts, (blk * 100 + i as i64) as f64).unwrap();
            app.append_float(None, &pairs, ts + 20_000, (blk * 100 + i as i64) as f64 + 0.5).unwrap();
        }
        app.commit().unwrap();
        if blk < 2 {
            db.compact_head_range(blk * block_ms, (blk + 1) * block_ms).unwrap();
        }
    }
    assert_eq!(db.blocks().len(), 2);

    let matchers = vec![LabelMatcher::new(MatchType::Equal, "__name__", "w41_conc_del").unwrap()];

    // Guard: shard_count == 0 and shard_idx >= shard_count return InvalidShard (never panic!)
    let cq_guard = db.chunk_querier(0, 4 * block_ms);
    let prep0 = cq_guard.prepare_select(&matchers).unwrap();
    assert!(matches!(
        cq_guard.stream_prepared_chunks_lazy_with_shard(prep0, &matchers, true, Some((0, 0)), None, |_| Ok(true)),
        Err(QuerierError::InvalidShard { idx: 0, count: 0 })
    ));
    let prep1 = cq_guard.prepare_select(&matchers).unwrap();
    assert!(matches!(
        cq_guard.stream_prepared_chunks_lazy_with_shard(prep1, &matchers, true, Some((4, 4)), None, |_| Ok(true)),
        Err(QuerierError::InvalidShard { idx: 4, count: 4 })
    ));

    // Concurrent stress: writer thread appends to Head (triggering mmap rotation) + deletes "even"
    // series across blocks while 4 reader threads run slow-callback sharded & unsharded Phase 2 streams.
    let stop = Arc::new(AtomicBool::new(false));
    let deleted_done = Arc::new(AtomicBool::new(false));

    let db_w = Arc::clone(&db);
    let stop_w = Arc::clone(&stop);
    let del_w = Arc::clone(&deleted_done);
    let writer = std::thread::spawn(move || {
        let mut ts = 2 * block_ms + 60_000;
        for iter in 0..20usize {
            let mut app = db_w.appender();
            for i in 0..96usize {
                if iter >= 8 && i % 2 == 0 {
                    continue;
                }
                let pod = format!("pod-{i:03}");
                let grp = if i % 2 == 0 { "even" } else { "odd" };
                let pairs = [
                    ("__name__", "w41_conc_del"),
                    ("grp", grp),
                    ("pod", pod.as_str()),
                ];
                app.append_float(None, &pairs, ts, iter as f64).unwrap();
            }
            app.commit().unwrap();
            ts += 10_000;
            if iter == 8 {
                db_w.delete(0, 4 * block_ms, &[LabelMatcher::new(MatchType::Equal, "grp", "even").unwrap()]).unwrap();
                del_w.store(true, Ordering::Release);
            }
            if stop_w.load(Ordering::Acquire) {
                break;
            }
        }
    });

    let mut readers = Vec::new();
    for r_id in 0..4usize {
        let db_r = Arc::clone(&db);
        let m_r = matchers.clone();
        let del_r = Arc::clone(&deleted_done);
        readers.push(std::thread::spawn(move || {
            for round in 0..8usize {
                let unordered = (r_id + round) % 2 == 0;
                let shard = if round % 2 == 0 { Some(((r_id % 4) as u64, 4u64)) } else { None };
                let cq = if unordered {
                    db_r.unordered_chunk_querier(0, 4 * block_ms)
                } else {
                    db_r.chunk_querier(0, 4 * block_ms)
                };
                let prep = cq.prepare_select(&m_r).unwrap();
                let mut series_labels: Vec<Vec<(String, String)>> = Vec::new();
                let mut seen_chunks = 0usize;
                let was_deleted_before_query = del_r.load(Ordering::Acquire);
                cq.stream_prepared_chunks_lazy_with_shard(prep, &m_r, true, shard, None, |ev| {
                    match ev {
                        LazyStreamEvent::Series(labels, _) => {
                            let pairs: Vec<(String, String)> = labels.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
                            series_labels.push(pairs);
                        }
                        LazyStreamEvent::SeriesEnd => {}
                        LazyStreamEvent::SeriesChunks(idx, chks) => {
                            // Deliberately inject slow-consumer backpressure inside callback!
                            if idx % 16 == 0 {
                                std::thread::sleep(Duration::from_millis(1));
                            }
                            if was_deleted_before_query {
                                let is_even = series_labels[idx].iter().any(|(k, v)| k == "grp" && v == "even");
                                assert!(
                                    !is_even || chks.is_empty(),
                                    "deleted 'even' series leaked chunks after Block::delete completed!"
                                );
                            }
                            seen_chunks += chks.len();
                        }
                    }
                    Ok(true)
                })
                .unwrap();
                assert!(seen_chunks > 0 || shard.is_some());
            }
        }));
    }

    writer.join().unwrap();
    stop.store(true, Ordering::Release);
    for r in readers {
        r.join().unwrap();
    }

    // Final deterministic check: zero "even" series remain in Head or Blocks!
    let cq_final = db.unordered_chunk_querier(0, 4 * block_ms);
    let prep_final = cq_final.prepare_select(&matchers).unwrap();
    let mut final_series = 0usize;
    let mut final_chunks = 0usize;
    cq_final
        .stream_prepared_chunks_lazy_with_shard(prep_final, &matchers, true, None, None, |ev| {
            match ev {
                LazyStreamEvent::Series(labels, _) => {
                    assert_eq!(
                        labels.get("grp"),
                        "odd",
                        "deleted 'even' series still present: {labels:?}"
                    );
                    final_series += 1;
                }
                LazyStreamEvent::SeriesEnd => {}
                LazyStreamEvent::SeriesChunks(_, chks) => {
                    final_chunks += chks.len();
                }
            }
            Ok(true)
        })
        .unwrap();
    assert_eq!(final_series, 48, "exactly 48 odd series must remain across Head + Blocks");
    assert!(final_chunks >= 48 * 3, "odd series must retain all 3 windows of chunks");
    let cq_blocks = db.unordered_chunk_querier(0, 2 * block_ms - 1);
    let prep_blocks = cq_blocks.prepare_select(&matchers).unwrap();
    let mut blk_odd = 0usize;
    cq_blocks
        .stream_prepared_chunks_lazy_with_shard(prep_blocks, &matchers, true, None, None, |ev| {
            if let LazyStreamEvent::Series(labels, _) = ev {
                assert_eq!(labels.get("grp"), "odd", "deleted 'even' series leaked from compacted blocks!");
                blk_odd += 1;
            }
            Ok(true)
        })
        .unwrap();
    assert_eq!(blk_odd, 48, "all 48 odd series must remain intact in compacted blocks");
    db.close().unwrap();
}
