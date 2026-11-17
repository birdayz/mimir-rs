//! Wave 11 TSDB Correctness Differential Conformance & Regression Suite.
//!
//! Covers all findings from the Wave 11 TSDB audit (`P0-1..4`, `P1-1..7`, `P2-1..6`)
//! against both the Go package oracle (`target/go-oracle`) and twin-process Go vs Rust
//! ingesters (`target/go-mimir` vs `target/release/mimir`).

#![allow(clippy::result_large_err)]

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;
use std::sync::{Arc, Barrier};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use mimir_common::block_on_for_tests;
use mimir_conformance::free_test_port;
use mimir_conformance::oracle::{
    invoke_oracle, VerifyTsdbDirRequest, VerifyTsdbDirResponse,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize)]
struct RegexMatcherErrorsRequest {
    patterns: Vec<String>,
}

#[derive(Deserialize)]
struct RegexMatcherErrorsResponse {
    errors: Vec<String>,
}
use mimir_conformance::twin::{http_get, start_mimir, wait_ready, Impl, Proc};
use mimir_proto::cortexpb;
use mimir_proto::ingesterpb;
use mimir_proto::ingesterpb::ingester_client::IngesterClient;
use mimir_tsdb::block::{Block, BlockMetaCompaction};
use mimir_tsdb::chunkenc::{
    ChunkError, EncodedChunk, Encoding, FloatHistogram, FloatHistogramStChunk, Histogram,
    HistogramSpan, HistogramStChunk,
};
use mimir_tsdb::compact::{compact_blocks, write_block_from_series, CompactionSeries, SampleValue};
use mimir_tsdb::db::{Tsdb, TsdbOptions};
use mimir_tsdb::exemplar::{CircularExemplarStorage, Exemplar};
use mimir_tsdb::head::{Head, HeadOptions};
use mimir_tsdb::head_chunks::{HeadChunksError, HeadChunksReader, HeadChunksWriter};
use mimir_tsdb::labels::Labels;
use mimir_tsdb::matcher::{LabelMatcher, MatchType};
use mimir_tsdb::record::{RecordDecoder, RecordEncoder, RefExemplar, RefSample, RefSeries, Tombstone};
use mimir_tsdb::search::{apply_search_hints, FuzzAlg, SearchHints, SearchOrdering};
use mimir_tsdb::tombstones::{add_interval, write_tombstones_file, Interval, MemTombstones};
use mimir_tsdb::wlog::{CompressionType, WlogReader, WlogWriter};
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

async fn boot_w11_ingester(
    which: Impl,
    name: &str,
    dir: &Path,
    rt_yaml: &str,
    http_client: &reqwest::Client,
) -> RunningIngester {
    let tsdb_dir = dir.join("tsdb");
    let blocks_dir = dir.join("blocks");
    fs::create_dir_all(&tsdb_dir).unwrap();
    fs::create_dir_all(&blocks_dir).unwrap();

    let rt_path = dir.join("runtime.yaml");
    fs::write(&rt_path, rt_yaml).unwrap();

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
  tsdb_config_update_period: 100ms
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
  period: 100ms
limits:
  max_global_exemplars_per_user: 0
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
    RunningIngester {
        proc,
        http_port,
        ing: IngesterClient::new(ch),
    }
}

async fn query_stream_series_names(
    ing: &mut IngesterClient<Channel>,
    tenant: &str,
    start: i64,
    end: i64,
    matchers: Vec<ingesterpb::LabelMatcher>,
) -> Vec<String> {
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
    let mut out = Vec::new();
    while let Some(frame) = stream.next().await {
        let frame = frame.expect("QueryStream frame");
        for s in frame.streaming_series {
            let mut pairs = Vec::new();
            for p in s.labels {
                pairs.push(format!(
                    "{}={}",
                    String::from_utf8_lossy(&p.name),
                    String::from_utf8_lossy(&p.value)
                ));
            }
            pairs.sort();
            out.push(pairs.join(","));
        }
    }
    out.sort();
    out
}

/// Twin-process differential parity (`target/go-mimir` vs `target/release/mimir`) for:
/// - P0-3: Go RE2 regex dialect (`\<foo\>`, `[a&&b]`, `\Qa+b\E`, `\Qunclosed`, `\101`, `[\d-z]`, `[!--z]`, `[a[b]]`, `\d`, `\s`, `\w`, `a{01}`)
/// - P1-1/P1-2/P2-1: Runtime `max_global_exemplars_per_user: 0 -> 100` reload + duplicate exemplar no-op
/// - P1-6: Unicode `'İ'` & `'Σ'` case-insensitive `SearchLabelValues`
/// - P2-4: Empty label name `""` regex matcher (`{""=~"a*"}`)
#[test]
fn w11_twin_process_regex_exemplar_reload_and_unicode_search_parity() {
    block_on_for_tests(async {
        let tmp = tempfile::tempdir().unwrap();
        let http_client = reqwest::Client::new();
        let rt_v1 = "overrides: {}
";

        let mut pair = Vec::new();
        for &which in &IMPLS {
            let d = tmp.path().join(which.name());
            pair.push(boot_w11_ingester(which, "w11-tsdb", &d, rt_v1, &http_client).await);
        }
        let [mut go, mut rust]: [RunningIngester; 2] = pair.try_into().ok().unwrap();

        let base = now_ms() - 60_000;
        let tenant = "w11-tenant";

        let series_defs: Vec<Vec<(&str, &str)>> = vec![
            vec![("__name__", "rx_test"), ("k", "<foo>"), ("kind", "angle")],
            vec![("__name__", "rx_test"), ("k", "foo"), ("kind", "word")],
            vec![("__name__", "rx_test"), ("k", "&"), ("kind", "amp")],
            vec![("__name__", "rx_test"), ("k", "a"), ("kind", "char_a")],
            vec![("__name__", "rx_test"), ("k", "a+b"), ("kind", "quote")],
            vec![("__name__", "rx_test"), ("k", "A"), ("kind", "octal")],
            vec![("__name__", "rx_test"), ("k", "-"), ("kind", "dash")],
            vec![("__name__", "rx_test"), ("k", "z"), ("kind", "zeta")],
            vec![("__name__", "rx_test"), ("k", "+"), ("kind", "plus")],
            vec![("__name__", "rx_test"), ("k", "b]"), ("kind", "nested_close")],
            vec![("__name__", "rx_test"), ("k", "0"), ("kind", "ascii_digit")],
            vec![("__name__", "rx_test"), ("k", "\u{0660}"), ("kind", "arabic_digit")],
            vec![("__name__", "rx_test"), ("k", "\u{00a0}"), ("kind", "nbsp")],
            vec![("__name__", "rx_test"), ("k", "a{01}"), ("kind", "non_repeat_brace")],
            vec![("__name__", "rx_test"), ("k", "İSTANBUL"), ("kind", "tr_i")],
            vec![("__name__", "rx_test"), ("k", "ΟΣ"), ("kind", "gr_sigma")],
        ];

        let mut ts_list = Vec::with_capacity(series_defs.len());
        for (i, lbls) in series_defs.iter().enumerate() {
            ts_list.push(cortexpb::TimeSeries {
                labels: lbls.iter().map(|(k, v)| lp(k, v)).collect(),
                samples: vec![cortexpb::Sample {
                    timestamp_ms: base + 1000,
                    value: (i + 1) as f64,
                }],
                ..Default::default()
            });
        }
        let push_req = cortexpb::WriteRequest {
            timeseries: ts_list,
            ..Default::default()
        };
        go.ing
            .push(with_org(tenant, push_req.clone()))
            .await
            .expect("go push");
        rust.ing
            .push(with_org(tenant, push_req))
            .await
            .expect("rust push");

        // P0-3 regex dialect queries
        for pat in [
            r"\<foo\>",
            "[a&&b]",
            r"\Qa+b\E",
            r"\Qa+",
            r"\101",
            r"[\d-z]",
            "[!--z]",
            "[a[b]]",
            r"\d",
            r"\s",
            r"\w",
            "a{01}",
        ] {
            let matchers = vec![
                ingesterpb::LabelMatcher {
                    r#type: ingesterpb::MatchType::Equal as i32,
                    name: "__name__".to_string(),
                    value: "rx_test".to_string(),
                },
                ingesterpb::LabelMatcher {
                    r#type: ingesterpb::MatchType::RegexMatch as i32,
                    name: "k".to_string(),
                    value: pat.to_string(),
                },
            ];
            let g = query_stream_series_names(&mut go.ing, tenant, base, base + 5000, matchers.clone()).await;
            let r = query_stream_series_names(&mut rust.ing, tenant, base, base + 5000, matchers).await;
            assert_eq!(r, g, "regex pattern {pat:?} mismatch: rust={r:?} go={g:?}");
        }

        // P2-4: empty label name "" with regex matching ""
        {
            let matchers = vec![ingesterpb::LabelMatcher {
                r#type: ingesterpb::MatchType::RegexMatch as i32,
                name: String::new(),
                value: "a*".to_string(),
            }];
            let g = query_stream_series_names(&mut go.ing, tenant, base, base + 5000, matchers.clone()).await;
            let r = query_stream_series_names(&mut rust.ing, tenant, base, base + 5000, matchers).await;
            assert_eq!(r.len(), series_defs.len());
            assert_eq!(r, g, "empty label name regex mismatch");
        }

        // P1-6: SearchLabelValues with Unicode 'İ' and 'Σ' case_insensitive=true
        for term in ["istanbul", "οσ"] {
            let req = ingesterpb::SearchLabelValuesRequest {
                start_timestamp_ms: base,
                end_timestamp_ms: base + 5000,
                name: "k".to_string(),
                matchers: Vec::new(),
                filter: Some(ingesterpb::SearchFilter {
                    terms: vec![term.to_string()],
                    case_insensitive: true,
                    fuzz_alg: ingesterpb::search_filter::FuzzAlg::Substring as i32,
                    fuzz_threshold: 0,
                }),
                ordering: ingesterpb::SearchOrdering::OrderByValueAsc as i32,
                limit: 10,
            };
            let mut g_stream = go.ing.search_label_values(with_org(tenant, req.clone())).await.unwrap().into_inner();
            let mut r_stream = rust.ing.search_label_values(with_org(tenant, req)).await.unwrap().into_inner();
            let mut g_vals = Vec::new();
            while let Some(b) = g_stream.next().await {
                g_vals.extend(b.unwrap().results.into_iter().map(|x| x.value));
            }
            let mut r_vals = Vec::new();
            while let Some(b) = r_stream.next().await {
                r_vals.extend(b.unwrap().results.into_iter().map(|x| x.value));
            }
            assert!(!g_vals.is_empty(), "go should find match for {term}");
            assert_eq!(r_vals, g_vals, "search_label_values mismatch for {term}");
        }

        // P1-1 / P1-2 / P2-1: Runtime max_global_exemplars_per_user 0 -> 100
        let ex_push_0 = cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![lp("__name__", "ex_metric"), lp("job", "app")],
                samples: vec![cortexpb::Sample {
                    timestamp_ms: base + 2000,
                    value: 1.0,
                }],
                exemplars: vec![cortexpb::Exemplar {
                    labels: vec![lp("trace_id", "dropped-0")],
                    value: 1.0,
                    timestamp_ms: base + 2000,
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        go.ing.push(with_org(tenant, ex_push_0.clone())).await.unwrap();
        rust.ing.push(with_org(tenant, ex_push_0)).await.unwrap();

        let rt_v2 = format!("overrides:
  {tenant}:
    max_global_exemplars_per_user: 100
");
        for &which in &IMPLS {
            fs::write(tmp.path().join(which.name()).join("runtime.yaml"), &rt_v2).unwrap();
        }

        // Wait until both Go and Rust reload runtime_config and apply tsdb_config_update_period (100ms).
        for (label, port) in [("go", go.http_port), ("rust", rust.http_port)] {
            let mut reloaded = false;
            for _ in 0..40 {
                if let Ok((code, _hdrs, body)) =
                    http_get(&http_client, port, "/runtime_config?mode=diff", false).await
                {
                    if code == 200 && body.contains("max_global_exemplars_per_user") {
                        reloaded = true;
                        break;
                    }
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            assert!(reloaded, "{label} did not reload runtime_config");
        }
        tokio::time::sleep(Duration::from_millis(350)).await;

        let ts = base + 3000;
        let ex_req = cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![lp("__name__", "ex_metric"), lp("job", "app")],
                samples: vec![cortexpb::Sample {
                    timestamp_ms: ts,
                    value: 2.0,
                }],
                exemplars: vec![cortexpb::Exemplar {
                    labels: vec![lp("trace_id", "live-after-reload")],
                    value: 2.0,
                    timestamp_ms: ts,
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        go.ing.push(with_org(tenant, ex_req.clone())).await.unwrap();
        rust.ing.push(with_org(tenant, ex_req.clone())).await.unwrap();
        // Push duplicate exemplar too (must be a no-op in both).
        go.ing.push(with_org(tenant, ex_req.clone())).await.unwrap();
        rust.ing.push(with_org(tenant, ex_req)).await.unwrap();

        let q_ex = ingesterpb::ExemplarQueryRequest {
            start_timestamp_ms: base,
            end_timestamp_ms: base + 10_000,
            matchers: vec![ingesterpb::LabelMatchers {
                matchers: vec![ingesterpb::LabelMatcher {
                    r#type: ingesterpb::MatchType::Equal as i32,
                    name: "__name__".to_string(),
                    value: "ex_metric".to_string(),
                }],
            }],
        };
        let g_ex = go.ing.query_exemplars(with_org(tenant, q_ex.clone())).await.unwrap().into_inner();
        let r_ex = rust.ing.query_exemplars(with_org(tenant, q_ex)).await.unwrap().into_inner();
        let g_cnt: usize = g_ex.timeseries.iter().map(|s| s.exemplars.len()).sum();
        assert_eq!(g_cnt, 1, "expected 1 exemplar after 0 -> 100 reload");
        assert_eq!(r_ex.timeseries, g_ex.timeseries, "exemplar query mismatch");

        go.stop().await;
        rust.stop().await;
    });
}

#[test]
fn w11_p0_3_and_p2_3_regex_oracle_and_set_matches_parity() {
    let patterns = vec![
        r"\<foo\>",
        "[a&&b]",
        "[!--z]",
        "[a~~b]",
        "[a[b]]",
        r"\Qa*b\E",
        r"\Qa*b\E+",
        r"\Qab",
        r"\Q\E",
        r"\Q\E*",
        r"\0",
        r"\101",
        r"[\d-z]",
        r"\b{start}",
        "a{01}",
        "a{,2}",
        "x{1001",
        "(?i)abc",
        "(?i)k",
        r"\d+",
        r"\s+",
        r"\w+",
        "a|a|b",
        "foo)(bar",
        r"\1",
    ];
    let go_errs: RegexMatcherErrorsResponse = invoke_oracle(
        "regex-matcher-errors",
        &RegexMatcherErrorsRequest {
            patterns: patterns.clone().into_iter().map(str::to_string).collect(),
        },
    )
    .expect("go-oracle regex-matcher-errors");

    for (p, go_err) in patterns.iter().zip(go_errs.errors.iter()) {
        let rust_res = LabelMatcher::new(MatchType::RegexMatch, "x", *p);
        if go_err.is_empty() {
            assert!(rust_res.is_ok(), "pattern {p:?} valid in Go, failed in Rust: {rust_res:?}");
        } else {
            let r_err = rust_res.unwrap_err().to_string();
            assert_eq!(r_err, *go_err, "error text mismatch for {p:?}");
        }
    }

    assert!(LabelMatcher::new(MatchType::RegexMatch, "x", r"\<foo\>").unwrap().matches("<foo>"));
    assert!(!LabelMatcher::new(MatchType::RegexMatch, "x", r"\<foo\>").unwrap().matches("foo"));
    assert!(LabelMatcher::new(MatchType::RegexMatch, "x", "[a&&b]").unwrap().matches("&"));
    assert!(LabelMatcher::new(MatchType::RegexMatch, "x", "[!--z]").unwrap().matches("+"));
    assert!(!LabelMatcher::new(MatchType::RegexMatch, "x", "[!--z]").unwrap().matches("a"));
    assert!(LabelMatcher::new(MatchType::RegexMatch, "x", "[a~~b]").unwrap().matches("~"));
    assert!(LabelMatcher::new(MatchType::RegexMatch, "x", "[a[b]]").unwrap().matches("a]"));
    assert!(!LabelMatcher::new(MatchType::RegexMatch, "x", "[a[b]]").unwrap().matches("a"));
    assert!(LabelMatcher::new(MatchType::RegexMatch, "x", r"\Qa*b\E+").unwrap().matches("a*bb"));
    assert!(!LabelMatcher::new(MatchType::RegexMatch, "x", r"\Qa*b\E+").unwrap().matches("a*ba*b"));
    assert!(LabelMatcher::new(MatchType::RegexMatch, "x", r"\Qab").unwrap().matches("ab"));
    assert!(LabelMatcher::new(MatchType::RegexMatch, "x", r"\101").unwrap().matches("A"));
    assert!(LabelMatcher::new(MatchType::RegexMatch, "x", r"\0").unwrap().matches("\0"));
    assert!(LabelMatcher::new(MatchType::RegexMatch, "x", r"[\d-z]").unwrap().matches("-"));
    assert!(LabelMatcher::new(MatchType::RegexMatch, "x", r"[\d-z]").unwrap().matches("5"));
    assert!(LabelMatcher::new(MatchType::RegexMatch, "x", "a{01}").unwrap().matches("a{01}"));
    assert!(!LabelMatcher::new(MatchType::RegexMatch, "x", "a{01}").unwrap().matches("a"));
    assert!(!LabelMatcher::new(MatchType::RegexMatch, "x", r"\d").unwrap().matches("\u{0660}"));
    assert!(!LabelMatcher::new(MatchType::RegexMatch, "x", r"\s").unwrap().matches("\u{00a0}"));
    assert!(!LabelMatcher::new(MatchType::RegexMatch, "x", r"\w").unwrap().matches("é"));

    // P2-3: (?i) must NOT populate set_matches; deduplicated alternating literals at 255 vs 256
    let m_ci = LabelMatcher::new(MatchType::RegexMatch, "x", "(?i)abc").unwrap();
    assert_eq!(m_ci.set_matches(), None);
    assert!(m_ci.matches("ABC") && m_ci.matches("aBc"));

    let dup_255 = (0..255)
        .map(|i| format!("v{i}"))
        .chain((0..255).map(|i| format!("v{i}")))
        .collect::<Vec<_>>()
        .join("|");
    let m_255 = LabelMatcher::new(MatchType::RegexMatch, "x", &dup_255).unwrap();
    assert_eq!(m_255.set_matches().map(<[String]>::len), Some(255));

    let unique_256 = (0..256).map(|i| format!("v{i}")).collect::<Vec<_>>().join("|");
    let m_256 = LabelMatcher::new(MatchType::RegexMatch, "x", &unique_256).unwrap();
    assert_eq!(m_256.set_matches(), None);
}

/// P0-1: Concurrent OOO arrivals during OOO compaction must NEVER be wiped (`t <= ooo_maxt`)
/// AND already-compacted OOO chunks must NEVER be compacted twice (`t > ooo_maxt` or pending_commits).
#[test]
fn w11_p0_1_ooo_compaction_last_mmap_ref_concurrency_and_go_verify() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = TsdbOptions::default();
    opts.head.chunk_range_ms = 100_000;
    opts.head.ooo_time_window_ms = 1_000_000;
    opts.head.ooo_cap_max = 4;

    let db = Tsdb::open_with_reload_interval(dir.path(), opts, Duration::ZERO).unwrap();
    let p_s1 = [("__name__", "ooo_s1"), ("job", "a")];
    let p_s2 = [("__name__", "ooo_s2"), ("job", "b")];
    {
        let mut app = db.appender();
        app.append_float(None, &p_s1, 500_000, 1.0).unwrap();
        app.append_float(None, &p_s2, 500_000, 1.0).unwrap();
        app.append_float(None, &p_s1, 50_000, 10.0).unwrap();
        app.commit().unwrap();
    }

    let snap = db
        .head()
        .collect_ooo_compaction_candidates_with_watermark()
        .unwrap();
    assert_eq!(snap.candidates.len(), 1);
    assert_eq!(snap.max_time, 50_000);

    // Concurrent OOO arrivals AFTER snapshot: s2 at t = 40_000 (<= snap.max_time) AND s1 at t = 150_000 (> snap.max_time)
    // plus an unrelated in-order pending_commits > 0 on s1.
    {
        let mut app = db.appender();
        app.append_float(None, &p_s2, 40_000, 20.0).unwrap();
        app.append_float(None, &p_s1, 150_000, 30.0).unwrap();
        app.commit().unwrap();
    }
    let mut held_app = db.appender();
    held_app.append_float(None, &p_s1, 500_100, 2.0).unwrap();

    let mut comp = BlockMetaCompaction::default();
    comp.set_out_of_order();
    let _meta = write_block_from_series(
        dir.path(),
        0,
        100_000,
        snap.candidates
            .iter()
            .map(|(ref_id, labels)| {
                let mut chks = Vec::new();
                let mut tombs = Vec::new();
                db.head()
                    .load_series_ooo_chunks_into_up_to(
                        *ref_id,
                        snap.last_mmap_ref,
                        &mut chks,
                        &mut tombs,
                    )
                    .unwrap();
                CompactionSeries {
                    labels: labels.clone(),
                    chunks: chks,
                    tombstones: tombs,
                }
            })
            .collect(),
        comp,
        true,
        0,
    )
    .unwrap()
    .expect("OOO block written");
    db.reload_blocks().unwrap();

    db.head()
        .truncate_ooo(snap.last_wbl_file, snap.last_mmap_ref)
        .unwrap();
    held_app.commit().unwrap();

    let mut s1_ooo_chunks = Vec::new();
    let mut tombs = Vec::new();
    let s1_ref = db.head().get_ref_by_sorted_pairs(&p_s1).unwrap();
    db.head()
        .load_series_ooo_chunks_into(s1_ref, &mut s1_ooo_chunks, &mut tombs)
        .unwrap();
    assert_eq!(
        s1_ooo_chunks.len(),
        1,
        "only post-snapshot s1@150_000 should remain in Head OOO"
    );
    assert_eq!(s1_ooo_chunks[0].min_time, 150_000);

    let uids2 = db.compact_ooo_head().unwrap();
    assert_eq!(uids2.len(), 2);
    db.close().unwrap();

    let go_out: VerifyTsdbDirResponse = invoke_oracle(
        "verify-tsdb-dir",
        &VerifyTsdbDirRequest {
            dir: dir.path().to_string_lossy().to_string(),
            read_only: true,
        },
    )
    .expect("go-oracle verify-tsdb-dir");
    assert_eq!(go_out.series.len(), 2);
    let s1_samples: Vec<(i64, f64)> = go_out.series[0].samples.iter().map(|s| (s.t, s.v)).collect();
    let s2_samples: Vec<(i64, f64)> = go_out.series[1].samples.iter().map(|s| (s.t, s.v)).collect();
    assert_eq!(
        s1_samples,
        vec![(50_000, 10.0), (150_000, 30.0), (500_000, 1.0), (500_100, 2.0)]
    );
    assert_eq!(s2_samples, vec![(40_000, 20.0), (500_000, 1.0)]);
}

/// P0-2: `truncate_memory` must NOT log `Tombstone(MIN, MAX)` to WAL; `evict_series_refs_before`
/// synchronizes with concurrent `get_or_create` via `pending_tombstones` so post-compaction samples survive crash/replay.
#[test]
fn w11_p0_2_truncate_memory_no_wal_tombstone_and_evict_pending_tombstones_gate() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = TsdbOptions::default();
    opts.head.chunk_range_ms = 10_000;
    {
        let db = Tsdb::open_with_reload_interval(dir.path(), opts.clone(), Duration::ZERO).unwrap();
        let mut app = db.appender();
        app.append_float(None, &[("__name__", "s_recreate")], 1_000, 1.0)
            .unwrap();
        app.append_float(None, &[("__name__", "s_anchor")], 20_000, 2.0)
            .unwrap();
        app.commit().unwrap();

        db.compact_head_range(0, 10_000).unwrap();
        let mut app2 = db.appender();
        app2.append_float(None, &[("__name__", "s_recreate")], 20_500, 3.0)
            .unwrap();
        app2.commit().unwrap();
        db.close().unwrap();
    }

    let mut r = WlogReader::open_dir(dir.path().join("wal")).unwrap();
    let dec = RecordDecoder::new();
    while let Some(raw) = r.next_record().unwrap() {
        if let mimir_tsdb::record::DecodedRecord::Tombstones(stones) = dec.decode_record(raw).unwrap() {
            for s in stones {
                assert!(
                    !(s.mint == i64::MIN && s.maxt == i64::MAX),
                    "truncate_memory must not write Tombstone(MIN, MAX) to WAL"
                );
            }
        }
    }

    {
        let db = Tsdb::open_with_reload_interval(dir.path(), opts, Duration::ZERO).unwrap();
        let q = db.querier(i64::MIN, i64::MAX);
        let m = LabelMatcher::new(MatchType::Equal, "__name__", "s_recreate").unwrap();
        let res = q.select(true, &[m]).unwrap();
        assert_eq!(res.len(), 1);
        let pts: Vec<(i64, f64)> = res[0]
            .samples
            .iter()
            .map(|s| (s.t, match s.value { SampleValue::Float(v) => v, _ => unreachable!() }))
            .collect();
        assert_eq!(pts, vec![(1_000, 1.0), (20_500, 3.0)]);
        db.close().unwrap();
    }
    let go_out: VerifyTsdbDirResponse = invoke_oracle(
        "verify-tsdb-dir",
        &VerifyTsdbDirRequest {
            dir: dir.path().to_string_lossy().to_string(),
            read_only: true,
        },
    )
    .expect("go-oracle verify-tsdb-dir");
    assert_eq!(go_out.series[1].samples.len(), 2);

    for round in 0..8i64 {
        let dir2 = tempfile::tempdir().unwrap();
        let head = Arc::new(Head::open(dir2.path(), HeadOptions::default(), i64::MIN).unwrap());
        let p = [("__name__", "evict_race"), ("round", "1")];
        let r1 = {
            let mut app = head.appender();
            let id = app.append_float(None, &p, 1_000, 10.0).unwrap();
            app.commit().unwrap();
            id
        };
        let barrier = Arc::new(Barrier::new(2));
        let h1 = Arc::clone(&head);
        let b1 = Arc::clone(&barrier);
        let t_evict = std::thread::spawn(move || {
            b1.wait();
            h1.evict_series_refs_before(&[r1], 2_000).unwrap();
        });
        let h2 = Arc::clone(&head);
        let b2 = Arc::clone(&barrier);
        let t_create = std::thread::spawn(move || {
            b2.wait();
            let mut app = h2.appender();
            app.append_float(None, &p, 5_000 + round, 99.0).unwrap();
            app.commit().unwrap();
        });
        t_evict.join().unwrap();
        t_create.join().unwrap();
        head.close().unwrap();

        let reopened = Head::open(dir2.path(), HeadOptions::default(), i64::MIN).unwrap();
        let q = reopened
            .series_chunks(
                reopened
                    .get_ref_by_sorted_pairs(&p)
                    .expect("recreated series must survive WAL replay"),
                i64::MIN,
                i64::MAX,
            )
            .unwrap()
            .expect("chunks present");
        let has_new = q.1.iter().any(|c| c.max_time >= 5_000 + round);
        assert!(has_new, "post-eviction sample lost on WAL replay (round {round})");
        reopened.close().unwrap();
    }
}

/// P0-4: Duplicate `Series` record in WAL resets obsolete earlier-generation `head_chunks` and deduplicates mmapped chunks.
#[test]
fn w11_p0_4_duplicate_series_wal_replay_resets_obsolete_head_chunks() {
    for concurrency in [1usize, 4usize] {
        let dir = tempfile::tempdir().unwrap();
        let chunks_dir = dir.path().join("chunks_head");
        let wal_dir = dir.path().join("wal");
        fs::create_dir_all(&chunks_dir).unwrap();
        fs::create_dir_all(&wal_dir).unwrap();

        let labels = Labels::from_pairs([("__name__", "dup_series_metric"), ("job", "wal")]);
        let mut xc = mimir_tsdb::chunkenc::XorChunk::new();
        {
            let mut a = xc.appender().unwrap();
            a.append(0, 200, 2.0).unwrap();
            a.append(0, 300, 3.0).unwrap();
        }
        let mut cw = HeadChunksWriter::open(&chunks_dir).unwrap();
        cw.write_chunk(2, 200, 300, &EncodedChunk::Xor(xc), false)
            .unwrap();
        cw.close().unwrap();

        let enc = RecordEncoder::new(false);
        let mut w = WlogWriter::open(&wal_dir, CompressionType::None).unwrap();
        w.log(&[
            &enc.series(&[RefSeries { ref_id: 1, labels: labels.clone() }], Vec::new()),
            &enc.samples(
                &[
                    RefSample { ref_id: 1, st: 0, t: 100, v: 1.0 },
                    RefSample { ref_id: 1, st: 0, t: 150, v: 1.5 },
                ],
                Vec::new(),
            ),
            &enc.series(&[RefSeries { ref_id: 2, labels: labels.clone() }], Vec::new()),
            &enc.samples(&[RefSample { ref_id: 2, st: 0, t: 350, v: 3.5 }], Vec::new()),
        ])
        .unwrap();
        w.close().unwrap();

        let opts = HeadOptions {
            wal_replay_concurrency: concurrency,
            ..HeadOptions::default()
        };
        let head = Head::open(dir.path(), opts, i64::MIN).unwrap();
        assert_eq!(head.max_time(), 350);
        let ref_id = head
            .get_ref_by_sorted_pairs(&[("__name__", "dup_series_metric"), ("job", "wal")])
            .unwrap();
        let (_, chks, _) = head.series_chunks(ref_id, i64::MIN, i64::MAX).unwrap().unwrap();
        assert_eq!(chks.len(), 2, "chunks={chks:?}");
        assert_eq!((chks[0].min_time, chks[0].max_time), (200, 300));
        assert_eq!((chks[1].min_time, chks[1].max_time), (350, 350));
        head.close().unwrap();
    }
}

/// P1-2 & P2-1: `CircularExemplarStorage::resize` keeps lock held, preserves OOO exemplars across shrunk OOO window,
/// does not inflate `appended_count`, returns `Ok(())` on duplicates, and iterates in global ring-buffer order.
#[test]
fn w11_p1_2_and_p2_1_exemplar_resize_and_iteration_order() {
    let reg = prometheus::Registry::new();
    let store = CircularExemplarStorage::new_with_registry(4, 60_000, Some(&reg));
    let s1 = Labels::from_pairs([("__name__", "m1")]);
    let s2 = Labels::from_pairs([("__name__", "m2")]);
    let ex_l = |id: &str| Labels::from_pairs([("trace_id", id)]);

    store.add_exemplar(&s1, Exemplar::new(ex_l("a"), 1.0, 100_000)).unwrap();
    store.add_exemplar(&s2, Exemplar::new(ex_l("b"), 2.0, 100_500)).unwrap();
    store.add_exemplar(&s1, Exemplar::new(ex_l("c"), 3.0, 50_000)).unwrap();
    store.add_exemplar(&s2, Exemplar::new(ex_l("b"), 2.0, 100_500)).unwrap();
    assert_eq!(store.appended_count(), 3);

    let mut iter_order = Vec::new();
    store.iterate_exemplars(|sl, ex| iter_order.push((sl.clone(), ex.timestamp_ms)));
    assert_eq!(
        iter_order,
        vec![(s1.clone(), 100_000), (s2, 100_500), (s1, 50_000)]
    );

    store.set_ooo_time_window(0);
    let migrated = store.resize(8);
    assert_eq!(migrated, 3);
    assert_eq!(store.appended_count(), 3);
    assert_eq!(store.out_of_order_count(), 0);
    let all: &[LabelMatcher] = &[];
    let res = store.select(0, 200_000, &[all]);
    assert_eq!(res.len(), 2);
    assert_eq!(
        res[0].exemplars.iter().map(|e| e.timestamp_ms).collect::<Vec<_>>(),
        vec![50_000, 100_000]
    );
}

/// P1-3: Parallel WAL replay deletes series in order (no leaked post-delete exemplars or double-counted chunks_removed).
#[test]
fn w11_p1_3_parallel_wal_replay_ordered_deletes() {
    let mut counts = Vec::new();
    for concurrency in [1usize, 4usize] {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        fs::create_dir_all(&wal_dir).unwrap();
        let enc = RecordEncoder::new(false);
        let l = Labels::from_pairs([("__name__", "del_metric")]);
        let mut w = WlogWriter::open(&wal_dir, CompressionType::None).unwrap();
        w.log(&[
            &enc.series(&[RefSeries { ref_id: 1, labels: l }], Vec::new()),
            &enc.samples(&[RefSample { ref_id: 1, st: 0, t: 1_000, v: 1.0 }], Vec::new()),
            &enc.tombstones(&[Tombstone { ref_id: 1, mint: i64::MIN, maxt: i64::MAX }], Vec::new()),
            &enc.tombstones(&[Tombstone { ref_id: 1, mint: i64::MIN, maxt: i64::MAX }], Vec::new()),
            &enc.exemplars(
                &[RefExemplar {
                    ref_id: 1,
                    t: 1_000,
                    v: 1.0,
                    labels: Labels::from_pairs([("trace_id", "orphan")]),
                }],
                Vec::new(),
            ),
        ])
        .unwrap();
        w.close().unwrap();

        let reg = prometheus::Registry::new();
        let opts = HeadOptions {
            wal_replay_concurrency: concurrency,
            registry: Some(Arc::new(reg.clone())),
            ..HeadOptions::default()
        };
        let head = Head::open(dir.path(), opts, i64::MIN).unwrap();
        let all: &[LabelMatcher] = &[];
        assert!(
            head.exemplars().select(0, 10_000, &[all]).is_empty(),
            "post-delete exemplar must not be replayed (concurrency={concurrency})"
        );
        let mfs = reg.gather();
        let chunks_removed = mfs
            .iter()
            .find(|mf| mf.get_name() == "prometheus_tsdb_head_chunks_removed_total")
            .map(|mf| mf.get_metric()[0].get_counter().get_value())
            .unwrap_or(0.0);
        counts.push(chunks_removed);
        head.close().unwrap();
    }
    assert_eq!(counts[0], 1.0);
    assert_eq!(counts[1], 1.0);
}

/// P1-4: `zero_or_st` forces `st = 0` on live Head & OOO chunks when `enable_st_storage == false`.
#[test]
fn w11_p1_4_zero_or_st_matches_wal_replay() {
    let dir = tempfile::tempdir().unwrap();
    let opts = HeadOptions {
        enable_st_storage: false,
        float_chunk_encoding: Encoding::Xor2,
        enable_histogram_st_encoding: true,
        ooo_time_window_ms: 60_000,
        ..HeadOptions::default()
    };
    let p_f = [("__name__", "st_float")];
    let p_h = [("__name__", "st_hist")];
    let p_fh = [("__name__", "st_fhist")];
    let head = Head::open(dir.path(), opts, i64::MIN).unwrap();
    let mut app = head.appender();
    app.append_float_with_st(None, &p_f, 900, 10_000, 1.0).unwrap();
    app.append_histogram(
        None,
        &p_h,
        900,
        10_000,
        Histogram {
            count: 1,
            sum: 1.0,
            schema: 0,
            positive_spans: vec![HistogramSpan { offset: 0, length: 1 }],
            positive_buckets: vec![1],
            ..Histogram::default()
        },
    )
    .unwrap();
    app.append_float_histogram(
        None,
        &p_fh,
        900,
        10_000,
        FloatHistogram {
            count: 1.0,
            sum: 1.0,
            schema: 0,
            positive_spans: vec![HistogramSpan { offset: 0, length: 1 }],
            positive_buckets: vec![1.0],
            ..FloatHistogram::default()
        },
    )
    .unwrap();
    app.commit().unwrap();

    for p in [p_f, p_h, p_fh] {
        let r = head.get_ref_by_sorted_pairs(&p).unwrap();
        let (_, chks, _) = head.series_chunks(r, i64::MIN, i64::MAX).unwrap().unwrap();
        let samples = mimir_tsdb::compact::decode_chunk_samples(chks[0].chunk.as_ref().unwrap()).unwrap();
        assert_eq!(samples[0].st, 0, "st must be 0 when enable_st_storage=false");
    }
    head.close().unwrap();
}

/// P1-5, P1-6, P1-7, P2-2, P2-5, P2-6 direct invariants & Go-oracle block verification.
#[test]
fn w11_p1_5_6_7_and_p2_2_5_6_correctness() {
    // P1-5: tombstones::add_interval with i64::MIN
    let mut ivs = Vec::new();
    add_interval(&mut ivs, Interval::new(i64::MIN, 100));
    add_interval(&mut ivs, Interval::new(i64::MIN, 200));
    assert_eq!(ivs, vec![Interval::new(i64::MIN, 200)]);

    // P1-6: search.rs Go strings.ToLower parity ('İ' and 'Σ')
    let res = apply_search_hints(
        &["İSTANBUL".to_string(), "ΟΣ".to_string()],
        &SearchHints {
            terms: vec!["istanbul".to_string(), "οσ".to_string()],
            case_sensitive: false,
            fuzz_alg: FuzzAlg::Substring,
            fuzz_threshold: 0,
            order_by: SearchOrdering::ValueAsc,
            limit: 10,
        },
    )
    .unwrap();
    let vals: BTreeSet<String> = res.into_iter().map(|r| r.value).collect();
    assert_eq!(vals, BTreeSet::from(["İSTANBUL".to_string(), "ΟΣ".to_string()]));

    // P1-7: compact_blocks scopes tombstones strictly per input block
    let dir = tempfile::tempdir().unwrap();
    let l = Labels::from_pairs([("__name__", "per_block_tomb")]);
    let mut xc1 = mimir_tsdb::chunkenc::XorChunk::new();
    xc1.appender().unwrap().append(0, 1_000, 1.0).unwrap();
    let m1 = write_block_from_series(
        dir.path(),
        0,
        7_200_000,
        vec![CompactionSeries {
            labels: l.clone(),
            chunks: vec![mimir_tsdb::chunks_block::ChunkMeta {
                ref_id: 0,
                min_time: 1_000,
                max_time: 1_000,
                chunk: Some(EncodedChunk::Xor(xc1)),
            }],
            tombstones: Vec::new(),
        }],
        BlockMetaCompaction::default(),
        false,
        0,
    )
    .unwrap()
    .unwrap();
    {
        let b1_tmp = Block::open(dir.path().join(m1.ulid.to_string())).unwrap();
        let id1 = b1_tmp.index().postings("", &[""]).unwrap()[0];
        let mt = MemTombstones::new();
        mt.add_interval(id1, Interval::new(500, 1_500));
        write_tombstones_file(&dir.path().join(m1.ulid.to_string()), &mt).unwrap();
    }

    let mut xc2 = mimir_tsdb::chunkenc::XorChunk::new();
    xc2.appender().unwrap().append(0, 1_200, 2.0).unwrap();
    let m2 = write_block_from_series(
        dir.path(),
        0,
        7_200_000,
        vec![CompactionSeries {
            labels: l,
            chunks: vec![mimir_tsdb::chunks_block::ChunkMeta {
                ref_id: 0,
                min_time: 1_200,
                max_time: 1_200,
                chunk: Some(EncodedChunk::Xor(xc2)),
            }],
            tombstones: Vec::new(),
        }],
        BlockMetaCompaction::default(),
        true,
        0,
    )
    .unwrap()
    .unwrap();

    let b1 = Block::open(dir.path().join(m1.ulid.to_string())).unwrap();
    let b2 = Block::open(dir.path().join(m2.ulid.to_string())).unwrap();
    let merged_dir = tempfile::tempdir().unwrap();
    fs::create_dir_all(merged_dir.path().join("wal")).unwrap();
    let merged_meta = compact_blocks(merged_dir.path(), &[&b1, &b2])
        .unwrap()
        .expect("Block B sample t=1200 must survive Block A tombstone");
    assert_eq!(merged_meta.stats.num_samples, 1);

    let go_out: VerifyTsdbDirResponse = invoke_oracle(
        "verify-tsdb-dir",
        &VerifyTsdbDirRequest {
            dir: merged_dir.path().to_string_lossy().to_string(),
            read_only: true,
        },
    )
    .expect("go-oracle verify merged block");
    assert_eq!(go_out.series.len(), 1);
    assert_eq!(go_out.series[0].samples.len(), 1);
    assert_eq!(go_out.series[0].samples[0].t, 1_200);

    // P2-2: HeadChunksReader masks num_samples & 0x3FFF on HistogramSt/FloatHistogramSt and maps chunk() decode errors to Corruption
    let hc_dir = tempfile::tempdir().unwrap();
    let mut hst = HistogramStChunk::new();
    let mut app_h = hst.appender().unwrap();
    app_h
        .append_histogram(
            None,
            100,
            1_000,
            &Histogram {
                count: 5,
                sum: 5.0,
                schema: 0,
                positive_spans: vec![HistogramSpan { offset: 0, length: 1 }],
                positive_buckets: vec![5],
                ..Histogram::default()
            },
            true,
        )
        .unwrap();
    drop(app_h);
    let mut fhs = FloatHistogramStChunk::new();
    let mut app_fh = fhs.appender().unwrap();
    app_fh
        .append_float_histogram(
            None,
            100,
            2_000,
            &FloatHistogram {
                count: 5.0,
                sum: 5.0,
                schema: 0,
                positive_spans: vec![HistogramSpan { offset: 0, length: 1 }],
                positive_buckets: vec![5.0],
                ..FloatHistogram::default()
            },
            true,
        )
        .unwrap();
    drop(app_fh);
    {
        let mut cw = HeadChunksWriter::open(hc_dir.path()).unwrap();
        cw.write_chunk(1, 1_000, 1_000, &EncodedChunk::HistogramSt(hst), false)
            .unwrap();
        cw.write_chunk(2, 2_000, 2_000, &EncodedChunk::FloatHistogramSt(fhs), false)
            .unwrap();
        cw.close().unwrap();
    }
    let reader = HeadChunksReader::open(hc_dir.path()).unwrap();
    let recs = reader.collect_all_chunks().unwrap();
    assert_eq!(recs.len(), 2);
    assert_eq!(recs[0].num_samples, 1);
    assert_eq!(recs[1].num_samples, 1);

    assert!(matches!(
        EncodedChunk::from_bytes(Encoding::HistogramSt, vec![0x00]),
        Err(ChunkError::InvalidHeader { .. }) | Err(_)
    ));
    let bad_dir = tempfile::tempdir().unwrap();
    {
        let p = bad_dir.path().join("000001");
        let mut raw = Vec::new();
        raw.extend_from_slice(&0x0130_BC91u32.to_be_bytes());
        raw.push(1);
        raw.extend_from_slice(&[0u8; 3]);
        let start = raw.len();
        raw.extend_from_slice(&1u64.to_be_bytes());
        raw.extend_from_slice(&100i64.to_be_bytes());
        raw.extend_from_slice(&200i64.to_be_bytes());
        raw.push(0x7F);
        raw.push(10);
        raw.extend_from_slice(&[0u8; 10]);
        let crc = crc32c::crc32c(&raw[start..]);
        raw.extend_from_slice(&crc.to_be_bytes());
        fs::write(&p, &raw).unwrap();
    }
    let mut bad_reader = HeadChunksReader::open_no_repair(bad_dir.path()).unwrap();
    let recs = bad_reader.collect_all_chunks().unwrap();
    let err = bad_reader.chunk(recs[0].chunk_ref).unwrap_err();
    assert!(matches!(err, HeadChunksError::Corruption { .. }), "got {err:?}");
    bad_reader.delete_corrupted(&err).unwrap();
    assert!(!bad_dir.path().join("000001").exists());

    // P2-5: wal_expiries keeps deleted series across checkpoint when actual_mint >= mint
    let cp_dir = tempfile::tempdir().unwrap();
    let h_cp = Head::open(
        cp_dir.path(),
        HeadOptions {
            chunk_range_ms: 10_000,
            wal_segment_size: 32 * 1024,
            ..HeadOptions::default()
        },
        i64::MIN,
    )
    .unwrap();
    let p_del = [("__name__", "cp_deleted")];
    let p_keep = [("__name__", "cp_kept")];
    {
        let mut app = h_cp.appender();
        app.append_float(None, &p_del, 1_000, 1.0).unwrap();
        app.append_float(None, &p_keep, 20_000, 2.0).unwrap();
        app.commit().unwrap();
    }
    for seg in 0..4 {
        let mut app = h_cp.appender();
        let name = format!("pad_{seg}");
        for j in 0..500 {
            app.append_float(None, &[("__name__", &name), ("j", &j.to_string())], 25_000, 1.0)
                .unwrap();
        }
        app.commit().unwrap();
    }
    h_cp.truncate(10_000).unwrap();
    let mut found_cp_deleted = false;
    if let Some((cp_path, _)) = mimir_tsdb::wlog::last_checkpoint(&cp_dir.path().join("wal")).unwrap() {
        let mut r = WlogReader::open_dir(&cp_path).unwrap();
        let dec = RecordDecoder::new();
        while let Some(raw) = r.next_record().unwrap() {
            if let mimir_tsdb::record::DecodedRecord::Series(series) = dec.decode_record(raw).unwrap() {
                if series.iter().any(|s| s.labels.get("__name__") == "cp_deleted") {
                    found_cp_deleted = true;
                }
            }
        }
    }
    assert!(
        found_cp_deleted,
        "checkpoint must retain deleted series via wal_expiries = actual_mint"
    );
    h_cp.close().unwrap();

    // P2-6: FloatHistogram::prom_equals compares zero_threshold with == (-0.0 == +0.0)
    let fh_neg_zero = FloatHistogram {
        zero_threshold: -0.0,
        ..FloatHistogram::default()
    };
    let fh_pos_zero = FloatHistogram {
        zero_threshold: 0.0,
        ..FloatHistogram::default()
    };
    assert!(fh_neg_zero.prom_equals(&fh_pos_zero));
}
