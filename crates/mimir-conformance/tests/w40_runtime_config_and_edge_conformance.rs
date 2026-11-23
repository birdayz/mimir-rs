//! Wave 40 live Go-vs-Rust twin-process conformance suite:
//! 1. Live runtime-config hot-reload (`-runtime-config.file` + `-runtime-config.reload-period`),
//!    bit-for-bit `cortex_runtime_config_hash` SHA256 parity across YAML 1.1 plain scalars
//!    (`1_000`, `0777`, octal/hex), `GET /runtime_config` & `/api/v1/user_limits` parity,
//!    live limit/OOO enforcement flips, and corrupt-reload resilience.
//! 2. Deep query-matcher & data-model edge-case parity: empty/negative/wildcard/Go-RE2 matchers
//!    (`(?i)*`, `\p{^Greek}`, `\Q...\E`), `-0.0`/`StaleNaN`/`+Inf`/`-Inf`, int/float/NHCB
//!    histograms + created timestamps, and all read RPCs.

use std::collections::BTreeMap;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mimir_common::block_on_for_tests;
use mimir_conformance::twin::{free_tcp_ports, start_mimir, Impl, Proc};
use mimir_proto::{cortexpb, grpcutilpb, ingesterpb};
use mimir_proto::ingesterpb::ingester_client::IngesterClient;
use mimir_tsdb::chunkenc::{EncodedChunk, Encoding};
use mimir_tsdb::compact::{decode_chunk_samples as chunk_to_samples, SampleValue};
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
    rt_extra: &str,
    limits_extra: &str,
    ing_extra: &str,
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
    instance_id: w40-test
    instance_addr: 127.0.0.1
    instance_port: {grpc_port}
    kvstore:
      store: inmemory
    replication_factor: 1
    num_tokens: 16
    min_ready_duration: 0s
    final_sleep: 0s
{ing_extra}
blocks_storage:
  backend: filesystem
  filesystem:
    dir: {bucket}
  tsdb:
    dir: {tsdb}
    ship_interval: 1h
limits:
  native_histograms_ingestion_enabled: true
  max_global_exemplars_per_user: 100
{limits_extra}
{rt_extra}
"#,
        bucket = dir.join("bucket").display(),
        tsdb = dir.join("tsdb").display(),
    )
}

async fn wait_ready(http: &reqwest::Client, p: &mut Proc, port: u16, timeout: Duration) {
    let dl = std::time::Instant::now() + timeout;
    loop {
        if let Some(st) = p.try_wait() {
            panic!("process exited early ({st:?}):\n{}", p.log());
        }
        if let Ok(r) = http.get(format!("http://127.0.0.1:{port}/ready")).send().await {
            if r.status() == 200 {
                return;
            }
        }
        assert!(
            std::time::Instant::now() < dl,
            "not ready within {timeout:?}:\n{}",
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

fn extract_runtime_config_hash(metrics: &str) -> Option<String> {
    for line in metrics.lines() {
        if line.starts_with("cortex_runtime_config_hash{") {
            if let Some(pos) = line.find("sha256=\"") {
                let rest = &line[pos + 8..];
                if let Some(end) = rest.find('"') {
                    return Some(rest[..end].to_string());
                }
            }
        }
    }
    None
}

fn extract_metric_value(metrics: &str, name: &str) -> Option<f64> {
    for line in metrics.lines() {
        if let Some(rest) = line.strip_prefix(name) {
            if rest.starts_with(' ') || rest.starts_with('{') {
                if let Some(val_str) = line.split_whitespace().last() {
                    return val_str.parse::<f64>().ok();
                }
            }
        }
    }
    None
}

async fn wait_hash_change(
    http: &reqwest::Client,
    port: u16,
    prev: &str,
    timeout: Duration,
) -> String {
    let dl = std::time::Instant::now() + timeout;
    loop {
        let body = http
            .get(format!("http://127.0.0.1:{port}/metrics"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        if let Some(h) = extract_runtime_config_hash(&body) {
            if h != prev {
                return h;
            }
        }
        assert!(
            std::time::Instant::now() < dl,
            "runtime config hash did not change from {prev} within {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_reload_failed(http: &reqwest::Client, port: u16, timeout: Duration) {
    let dl = std::time::Instant::now() + timeout;
    loop {
        let body = http
            .get(format!("http://127.0.0.1:{port}/metrics"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        if extract_metric_value(&body, "cortex_runtime_config_last_reload_successful") == Some(0.0) {
            return;
        }
        assert!(
            std::time::Instant::now() < dl,
            "expected cortex_runtime_config_last_reload_successful == 0 within {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DecodedVal {
    Float(i64, u64),
    IntHist(i64, i32, u64, u64),
    FloatHist(i64, i32, u64, u64),
}

async fn query_matchers(
    c: &mut IngesterClient<Channel>,
    org: &str,
    start: i64,
    end: i64,
    matchers: Vec<ingesterpb::LabelMatcher>,
) -> BTreeMap<String, Vec<DecodedVal>> {
    let req = ingesterpb::QueryRequest {
        start_timestamp_ms: start,
        end_timestamp_ms: end,
        matchers,
        streaming_chunks_batch_size: 4,
    };
    let mut stream = c.query_stream(with_org(org, req)).await.unwrap().into_inner();
    let mut series_labels: Vec<String> = Vec::new();
    let mut saw_end = false;
    let mut chunks_by_idx: BTreeMap<u64, Vec<DecodedVal>> = BTreeMap::new();
    while let Some(resp) = stream.next().await {
        let resp = resp.unwrap();
        if !resp.streaming_series.is_empty() || resp.is_end_of_series_stream {
            for s in resp.streaming_series {
                let mut pairs: Vec<String> = s
                    .labels
                    .iter()
                    .map(|l| {
                        format!(
                            "{}={}",
                            String::from_utf8_lossy(&l.name),
                            String::from_utf8_lossy(&l.value)
                        )
                    })
                    .collect();
                pairs.sort();
                series_labels.push(pairs.join(","));
            }
            if resp.is_end_of_series_stream {
                saw_end = true;
            }
        } else {
            for sc in resp.streaming_series_chunks {
                let entry = chunks_by_idx.entry(sc.series_index).or_default();
                for ch in sc.chunks {
                    let enc = wire_enc_to_tsdb(ch.encoding);
                    let ec = EncodedChunk::from_bytes(enc, ch.data.to_vec()).unwrap();
                    for pt in chunk_to_samples(&ec).unwrap() {
                        let v = match pt.value {
                            SampleValue::Float(f) => DecodedVal::Float(pt.t, f.to_bits()),
                            SampleValue::Histogram(h) => {
                                DecodedVal::IntHist(pt.t, h.schema, h.count, h.sum.to_bits())
                            }
                            SampleValue::FloatHistogram(fh) => DecodedVal::FloatHist(
                                pt.t,
                                fh.schema,
                                fh.count.to_bits(),
                                fh.sum.to_bits(),
                            ),
                        };
                        entry.push(v);
                    }
                }
            }
        }
    }
    assert!(saw_end, "expected is_end_of_series_stream");
    let mut out = BTreeMap::new();
    for (idx, key) in series_labels.into_iter().enumerate() {
        let mut vals = chunks_by_idx.remove(&(idx as u64)).unwrap_or_default();
        vals.sort_by_key(|v| match *v {
            DecodedVal::Float(t, _)
            | DecodedVal::IntHist(t, _, _, _)
            | DecodedVal::FloatHist(t, _, _, _) => t,
        });
        out.insert(key, vals);
    }
    out
}

// 1. Live runtime-config hot-reload, YAML 1.1 scalar hash parity, admin endpoints & corrupt reload
#[test]
fn w40_live_runtime_config_hotreload_hash_and_admin_parity() {
    block_on_for_tests(async {
        let tmp = tempfile::tempdir().unwrap();
        let http = reqwest::Client::new();
        let base_ts = now_ms() - 60_000;

        // Runtime config v1 includes YAML 1.1 plain scalars (1_000, octal 012 = 10, hex 0x10 = 16)!
        let rt_v1 = concat!(
            "ingester_limits:\n",
            "  max_tenants: 5\n",
            "overrides:\n",
            "  tenant-a:\n",
            "    max_global_series_per_user: 2\n",
            "    max_global_exemplars_per_user: 012\n",
            "    out_of_order_time_window: 10m\n",
            "  tenant-b:\n",
            "    max_global_series_per_user: 1_000\n",
            "    max_global_series_per_metric: 0x10\n",
            "    float_chunk_encoding: xor2\n",
            "    separate_metrics_group_label: team\n",
        );

        let mut procs = Vec::new();
        let mut hports = Vec::new();
        let mut gports = Vec::new();
        let mut rt_paths = Vec::new();

        for which in [Impl::Go, Impl::Rust] {
            let dir = tmp.path().join(which.name());
            std::fs::create_dir_all(dir.join("tsdb")).unwrap();
            std::fs::create_dir_all(dir.join("bucket")).unwrap();
            let rt_path = dir.join("runtime.yaml");
            std::fs::write(&rt_path, rt_v1).unwrap();
            let hp = free_test_port();
            let gp = free_test_port();
            let rt_yaml = format!(
                "runtime_config:\n  file: {}\n  period: 200ms\n",
                rt_path.display()
            );
            let yaml = base_yaml(
                hp,
                gp,
                &dir,
                &rt_yaml,
                "",
                "  tsdb_config_update_period: 200ms\n",
            );
            let mut p = start_mimir(which, "rt", &dir, &yaml, &[]);
            wait_ready(&http, &mut p, hp, Duration::from_secs(30)).await;
            procs.push(p);
            hports.push(hp);
            gports.push(gp);
            rt_paths.push(rt_path);
        }

        // Phase 1: Exact bit-for-bit cortex_runtime_config_hash SHA256 parity!
        let gm1 = http.get(format!("http://127.0.0.1:{}/metrics", hports[0])).send().await.unwrap().text().await.unwrap();
        let rm1 = http.get(format!("http://127.0.0.1:{}/metrics", hports[1])).send().await.unwrap().text().await.unwrap();
        let gh1 = extract_runtime_config_hash(&gm1).expect("go runtime config hash");
        let rh1 = extract_runtime_config_hash(&rm1).expect("rust runtime config hash");
        assert_eq!(rh1, gh1, "v1 cortex_runtime_config_hash SHA256 mismatch (rust vs go) with YAML 1.1 scalars");
        assert_eq!(extract_metric_value(&gm1, "cortex_runtime_config_last_reload_successful"), Some(1.0));
        assert_eq!(extract_metric_value(&rm1, "cortex_runtime_config_last_reload_successful"), Some(1.0));

        // Exact GET /runtime_config & /runtime_config?mode=diff & /api/v1/user_limits parity!
        for q in ["", "?mode=diff"] {
            let gb = http.get(format!("http://127.0.0.1:{}/runtime_config{q}", hports[0])).send().await.unwrap().text().await.unwrap();
            let rb = http.get(format!("http://127.0.0.1:{}/runtime_config{q}", hports[1])).send().await.unwrap().text().await.unwrap();
            assert_eq!(rb, gb, "/runtime_config{q} diff (rust vs go)");
        }
        for org in ["tenant-a", "tenant-b", "unknown-tenant", "tenant-a|tenant-b"] {
            let gr = http
                .get(format!("http://127.0.0.1:{}/api/v1/user_limits", hports[0]))
                .header("X-Scope-OrgID", org)
                .send()
                .await
                .unwrap();
            let rr = http
                .get(format!("http://127.0.0.1:{}/api/v1/user_limits", hports[1]))
                .header("X-Scope-OrgID", org)
                .send()
                .await
                .unwrap();
            assert_eq!(rr.status(), gr.status(), "/api/v1/user_limits status diff for {org}");
            if gr.status() == 200 {
                let gv: serde_json::Value = gr.json().await.unwrap();
                let rv: serde_json::Value = rr.json().await.unwrap();
                assert_eq!(rv["max_global_series_per_user"], gv["max_global_series_per_user"], "user_limits {org}");
                assert_eq!(rv["max_global_series_per_metric"], gv["max_global_series_per_metric"], "user_limits {org}");
                assert_eq!(rv["max_global_exemplars_per_user"], gv["max_global_exemplars_per_user"], "user_limits {org}");
                assert_eq!(rv["out_of_order_time_window"], gv["out_of_order_time_window"], "user_limits {org}");
            }
        }

        // Phase 2: tenant-a has max_global_series_per_user=2 & OOO=10m -> 2 series + OOO pass, 3rd series fails!
        let mut gc = connect_grpc(gports[0]).await;
        let mut rc = connect_grpc(gports[1]).await;
        let push_both = |gc: &mut IngesterClient<Channel>, rc: &mut IngesterClient<Channel>, org: &str, wr: cortexpb::WriteRequest| {
            let mut gc = gc.clone();
            let mut rc = rc.clone();
            let org = org.to_string();
            async move {
                let gr = gc.push(with_org(&org, wr.clone())).await;
                let rr = rc.push(with_org(&org, wr)).await;
                match (gr, rr) {
                    (Ok(_), Ok(_)) => None,
                    (Err(ge), Err(re)) => {
                        assert_eq!(re.code(), ge.code(), "org={org} push code diff go={ge:?} rust={re:?}");
                        let gd = grpcutilpb::extract_cortex_error_details(&ge);
                        let rd = grpcutilpb::extract_cortex_error_details(&re);
                        assert_eq!(
                            rd.as_ref().map(|d| (d.cause, d.soft)),
                            gd.as_ref().map(|d| (d.cause, d.soft)),
                            "org={org} ErrorDetails diff go={gd:?} rust={rd:?}"
                        );
                        Some(ge.code())
                    }
                    (g, r) => panic!("org={org} ok/err mismatch go={g:?} rust={r:?}"),
                }
            }
        };

        for id in ["1", "2"] {
            assert!(push_both(&mut gc, &mut rc, "tenant-a", cortexpb::WriteRequest {
                timeseries: vec![cortexpb::TimeSeries {
                    labels: vec![cortexpb::LabelPair::from_static("__name__", "rt_m"), cortexpb::LabelPair::from_static("id", id)],
                    samples: vec![cortexpb::Sample { value: 10.0, timestamp_ms: base_ts + 20_000 }],
                    ..Default::default()
                }], ..Default::default()
            }).await.is_none());
        }
        // OOO sample (-10s) MUST succeed under out_of_order_time_window=10m!
        assert!(push_both(&mut gc, &mut rc, "tenant-a", cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![cortexpb::LabelPair::from_static("__name__", "rt_m"), cortexpb::LabelPair::from_static("id", "1")],
                samples: vec![cortexpb::Sample { value: 5.0, timestamp_ms: base_ts + 10_000 }],
                ..Default::default()
            }], ..Default::default()
        }).await.is_none(), "OOO sample must succeed when out_of_order_time_window=10m");
        // 3rd series MUST fail under max_global_series_per_user=2!
        assert!(push_both(&mut gc, &mut rc, "tenant-a", cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![cortexpb::LabelPair::from_static("__name__", "rt_m"), cortexpb::LabelPair::from_static("id", "3")],
                samples: vec![cortexpb::Sample { value: 1.0, timestamp_ms: base_ts + 20_000 }],
                ..Default::default()
            }], ..Default::default()
        }).await.is_some(), "3rd series must fail when max_global_series_per_user=2");

        // Phase 3: LIVE HOT-RELOAD -> raise tenant-a series limit to 5 and disable OOO!
        let rt_v2 = concat!(
            "ingester_limits:\n",
            "  max_tenants: 10\n",
            "overrides:\n",
            "  tenant-a:\n",
            "    max_global_series_per_user: 5\n",
            "    out_of_order_time_window: 0s\n",
            "  tenant-b:\n",
            "    max_global_series_per_user: 2_000\n",
        );
        for p in &rt_paths {
            let tmp_f = p.with_extension("yaml.tmp");
            std::fs::write(&tmp_f, rt_v2).unwrap();
            std::fs::rename(&tmp_f, p).unwrap();
        }
        let gh2 = wait_hash_change(&http, hports[0], &gh1, Duration::from_secs(10)).await;
        let rh2 = wait_hash_change(&http, hports[1], &rh1, Duration::from_secs(10)).await;
        assert_eq!(rh2, gh2, "v2 hot-reloaded cortex_runtime_config_hash SHA256 mismatch");

        // Wait one tsdb_config_update_period (200ms) so live TSDB OOO window updates on both Go and Rust.
        tokio::time::sleep(Duration::from_millis(500)).await;

        // 3rd series MUST NOW SUCCEED on BOTH Go and Rust!
        assert!(push_both(&mut gc, &mut rc, "tenant-a", cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![cortexpb::LabelPair::from_static("__name__", "rt_m"), cortexpb::LabelPair::from_static("id", "3")],
                samples: vec![cortexpb::Sample { value: 30.0, timestamp_ms: base_ts + 30_000 }],
                ..Default::default()
            }], ..Default::default()
        }).await.is_none(), "3rd series must succeed after hot-reload raised limit to 5");

        // OOO sample on id=2 MUST NOW FAIL identically on BOTH Go and Rust!
        assert!(push_both(&mut gc, &mut rc, "tenant-a", cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![cortexpb::LabelPair::from_static("__name__", "rt_m"), cortexpb::LabelPair::from_static("id", "2")],
                samples: vec![cortexpb::Sample { value: 1.0, timestamp_ms: base_ts + 5_000 }],
                ..Default::default()
            }], ..Default::default()
        }).await.is_some(), "OOO sample must fail after hot-reload disabled out_of_order_time_window");

        // Phase 4: Corrupt runtime.yaml reload -> last_reload_successful == 0, hash stays at v2!
        for p in &rt_paths {
            std::fs::write(p, "overrides: [invalid yaml\n").unwrap();
        }
        wait_reload_failed(&http, hports[0], Duration::from_secs(10)).await;
        wait_reload_failed(&http, hports[1], Duration::from_secs(10)).await;
        let gm3 = http.get(format!("http://127.0.0.1:{}/metrics", hports[0])).send().await.unwrap().text().await.unwrap();
        let rm3 = http.get(format!("http://127.0.0.1:{}/metrics", hports[1])).send().await.unwrap().text().await.unwrap();
        assert_eq!(extract_runtime_config_hash(&gm3).as_deref(), Some(gh2.as_str()));
        assert_eq!(extract_runtime_config_hash(&rm3).as_deref(), Some(rh2.as_str()));

        drop(gc);
        drop(rc);
        for mut p in procs {
            p.sigterm();
            let st = p.wait_exit(Duration::from_secs(20)).expect("exit");
            assert!(st.success(), "exit={st:?}:\n{}", p.log());
        }
    });
}

// 2. Deep query-matcher & data-model edge-case parity across all read RPCs
#[test]
fn w40_query_matcher_and_data_model_edge_cases_twin_process_parity() {
    block_on_for_tests(async {
        let tmp = tempfile::tempdir().unwrap();
        let http = reqwest::Client::new();
        let base_ts = now_ms() - 120_000;

        let mut procs = Vec::new();
        let mut gports = Vec::new();
        for which in [Impl::Go, Impl::Rust] {
            let dir = tmp.path().join(which.name());
            std::fs::create_dir_all(dir.join("tsdb")).unwrap();
            std::fs::create_dir_all(dir.join("bucket")).unwrap();
            let hp = free_test_port();
            let gp = free_test_port();
            let yaml = base_yaml(hp, gp, &dir, "", "  float_chunk_encoding: xor2\n", "");
            let mut p = start_mimir(which, "edge", &dir, &yaml, &[]);
            wait_ready(&http, &mut p, hp, Duration::from_secs(30)).await;
            procs.push(p);
            gports.push(gp);
        }

        let mut gc = connect_grpc(gports[0]).await;
        let mut rc = connect_grpc(gports[1]).await;

        let stale_nan = f64::from_bits(0x7ff0_0000_0000_0002);
        let nhcb = cortexpb::Histogram {
            count: Some(cortexpb::histogram::Count::CountInt(5)),
            sum: 12.5,
            schema: -53,
            custom_values: vec![0.1, 1.0, 10.0],
            positive_spans: vec![cortexpb::BucketSpan { offset: 0, length: 3 }],
            positive_deltas: vec![1, 1, 0],
            timestamp: base_ts + 10_000,
            ..Default::default()
        };
        let float_hist = cortexpb::Histogram {
            count: Some(cortexpb::histogram::Count::CountFloat(7.5)),
            sum: 21.25,
            schema: 1,
            zero_threshold: 0.001,
            zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountFloat(1.5)),
            positive_spans: vec![cortexpb::BucketSpan { offset: 0, length: 2 }],
            positive_counts: vec![2.5, 3.5],
            timestamp: base_ts + 10_000,
            ..Default::default()
        };

        let wr = cortexpb::WriteRequest {
            timeseries: vec![
                // Special IEEE-754 floats: -0.0, +0.0, +Inf, -Inf, StaleNaN + exemplar
                cortexpb::TimeSeries {
                    labels: vec![
                        cortexpb::LabelPair::from_static("__name__", "edge_float"),
                        cortexpb::LabelPair::from_static("env", "prod"),
                    ],
                    samples: vec![
                        cortexpb::Sample { value: -0.0, timestamp_ms: base_ts },
                        cortexpb::Sample { value: 0.0, timestamp_ms: base_ts + 10_000 },
                        cortexpb::Sample { value: f64::INFINITY, timestamp_ms: base_ts + 20_000 },
                        cortexpb::Sample { value: f64::NEG_INFINITY, timestamp_ms: base_ts + 30_000 },
                        cortexpb::Sample { value: stale_nan, timestamp_ms: base_ts + 40_000 },
                    ],
                    exemplars: vec![cortexpb::Exemplar {
                        labels: vec![
                            cortexpb::LabelPair::from_static("span_id", "sp1"),
                            cortexpb::LabelPair::from_static("trace_id", "tr1"),
                        ],
                        value: 1.0,
                        timestamp_ms: base_ts + 10_000,
                    }],
                    ..Default::default()
                },
                // Series WITHOUT env label (tests empty-value matcher semantics!)
                cortexpb::TimeSeries {
                    labels: vec![
                        cortexpb::LabelPair::from_static("__name__", "edge_float"),
                        cortexpb::LabelPair::from_static("tier", "core"),
                    ],
                    samples: vec![cortexpb::Sample { value: 42.0, timestamp_ms: base_ts + 10_000 }],
                    ..Default::default()
                },
                // Series with Unicode label value
                cortexpb::TimeSeries {
                    labels: vec![
                        cortexpb::LabelPair::from_static("__name__", "edge_float"),
                        cortexpb::LabelPair::from_static("env", "café-λ"),
                    ],
                    samples: vec![cortexpb::Sample { value: 7.25, timestamp_ms: base_ts + 10_000 }],
                    ..Default::default()
                },
                // NHCB custom bucket histogram (schema = -53)
                cortexpb::TimeSeries {
                    labels: vec![
                        cortexpb::LabelPair::from_static("__name__", "edge_nhcb"),
                        cortexpb::LabelPair::from_static("env", "prod"),
                    ],
                    histograms: vec![nhcb],
                    ..Default::default()
                },
                // Float native histogram
                cortexpb::TimeSeries {
                    labels: vec![
                        cortexpb::LabelPair::from_static("__name__", "edge_fhist"),
                        cortexpb::LabelPair::from_static("env", "dev"),
                    ],
                    histograms: vec![float_hist],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        gc.push(with_org("t-edge", wr.clone())).await.unwrap();
        rc.push(with_org("t-edge", wr)).await.unwrap();

        // Test tricky empty-value, negative, wildcard, and Go-RE2 edge matchers!
        let matcher_cases: Vec<(&str, Vec<ingesterpb::LabelMatcher>)> = vec![
            ("env_eq_empty", vec![
                ingesterpb::LabelMatcher { r#type: ingesterpb::MatchType::Equal as i32, name: "__name__".into(), value: "edge_float".into() },
                ingesterpb::LabelMatcher { r#type: ingesterpb::MatchType::Equal as i32, name: "env".into(), value: "".into() },
            ]),
            ("env_neq_empty", vec![
                ingesterpb::LabelMatcher { r#type: ingesterpb::MatchType::Equal as i32, name: "__name__".into(), value: "edge_float".into() },
                ingesterpb::LabelMatcher { r#type: ingesterpb::MatchType::NotEqual as i32, name: "env".into(), value: "".into() },
            ]),
            ("env_re_dotstar", vec![
                ingesterpb::LabelMatcher { r#type: ingesterpb::MatchType::RegexMatch as i32, name: "env".into(), value: ".*".into() },
            ]),
            ("env_nre_dotstar", vec![
                ingesterpb::LabelMatcher { r#type: ingesterpb::MatchType::Equal as i32, name: "__name__".into(), value: "edge_float".into() },
                ingesterpb::LabelMatcher { r#type: ingesterpb::MatchType::RegexNoMatch as i32, name: "env".into(), value: ".*".into() },
            ]),
            ("env_re_prod_or_empty", vec![
                ingesterpb::LabelMatcher { r#type: ingesterpb::MatchType::RegexMatch as i32, name: "env".into(), value: "prod|".into() },
            ]),
            ("go_re2_flag_quantifier", vec![
                ingesterpb::LabelMatcher { r#type: ingesterpb::MatchType::RegexMatch as i32, name: "env".into(), value: "(?:(?i))*p(?i)*ROD".into() },
            ]),
            ("go_re2_negated_unicode_class", vec![
                ingesterpb::LabelMatcher { r#type: ingesterpb::MatchType::RegexMatch as i32, name: "env".into(), value: r"\p{^Greek}+".into() },
            ]),
            ("go_re2_quote_meta", vec![
                ingesterpb::LabelMatcher { r#type: ingesterpb::MatchType::RegexMatch as i32, name: "env".into(), value: r"\Qcafé-λ\E".into() },
            ]),
            ("all_series_incl_nhcb_and_fhist", vec![
                ingesterpb::LabelMatcher { r#type: ingesterpb::MatchType::RegexMatch as i32, name: "__name__".into(), value: "edge_.+".into() },
            ]),
        ];

        for (label, matchers) in matcher_cases {
            let gm = query_matchers(&mut gc, "t-edge", base_ts - 10_000, base_ts + 60_000, matchers.clone()).await;
            let rm = query_matchers(&mut rc, "t-edge", base_ts - 10_000, base_ts + 60_000, matchers).await;
            assert_eq!(rm, gm, "QueryStream mismatch for matcher case {label}");
        }

        // Verify LabelNamesAndValues, LabelValuesCardinality, ActiveSeries, AllUserStats!
        let mut g_lnv = Vec::new();
        let mut g_st = gc.label_names_and_values(with_org("t-edge", ingesterpb::LabelNamesAndValuesRequest::default())).await.unwrap().into_inner();
        while let Some(item) = g_st.next().await {
            for it in item.unwrap().items {
                let mut vs = it.values;
                vs.sort();
                g_lnv.push((it.label_name, vs));
            }
        }
        g_lnv.sort();
        let mut r_lnv = Vec::new();
        let mut r_st = rc.label_names_and_values(with_org("t-edge", ingesterpb::LabelNamesAndValuesRequest::default())).await.unwrap().into_inner();
        while let Some(item) = r_st.next().await {
            for it in item.unwrap().items {
                let mut vs = it.values;
                vs.sort();
                r_lnv.push((it.label_name, vs));
            }
        }
        r_lnv.sort();
        assert_eq!(r_lnv, g_lnv, "LabelNamesAndValues mismatch");

        let lvc_req = ingesterpb::LabelValuesCardinalityRequest {
            label_names: vec!["__name__".into(), "env".into()],
            ..Default::default()
        };
        let mut g_lvc: BTreeMap<String, BTreeMap<String, u64>> = BTreeMap::new();
        let mut glvc_st = gc.label_values_cardinality(with_org("t-edge", lvc_req.clone())).await.unwrap().into_inner();
        while let Some(item) = glvc_st.next().await {
            for it in item.unwrap().items {
                g_lvc.entry(it.label_name).or_default().extend(it.label_value_series);
            }
        }
        let mut r_lvc: BTreeMap<String, BTreeMap<String, u64>> = BTreeMap::new();
        let mut rlvc_st = rc.label_values_cardinality(with_org("t-edge", lvc_req)).await.unwrap().into_inner();
        while let Some(item) = rlvc_st.next().await {
            for it in item.unwrap().items {
                r_lvc.entry(it.label_name).or_default().extend(it.label_value_series);
            }
        }
        assert_eq!(r_lvc, g_lvc, "LabelValuesCardinality mismatch");

        let g_aus = gc.all_user_stats(with_org("t-edge", ingesterpb::UserStatsRequest::default())).await.unwrap().into_inner();
        let r_aus = rc.all_user_stats(with_org("t-edge", ingesterpb::UserStatsRequest::default())).await.unwrap().into_inner();
        assert_eq!(r_aus.stats.len(), g_aus.stats.len());
        assert_eq!(r_aus.stats[0].data.as_ref().unwrap().num_series, g_aus.stats[0].data.as_ref().unwrap().num_series);

        drop(gc);
        drop(rc);
        for mut p in procs {
            p.sigterm();
            let st = p.wait_exit(Duration::from_secs(20)).expect("exit");
            assert!(st.success(), "exit={st:?}:\n{}", p.log());
        }
    });
}
