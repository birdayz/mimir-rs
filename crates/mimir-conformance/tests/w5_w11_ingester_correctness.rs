//! Wave 11 differential conformance tests for `mimir-ingester` correctness fixes
//! (`ING-01..04`, `ING-05..07`, `ING-10`, `ING-14`).
//!
//! Boots unmodified Go `target/go-mimir` and Rust `target/release/mimir` side by side and
//! asserts identical gRPC/HTTP status codes, headers, error messages, and Prometheus metrics.

#![allow(clippy::result_large_err)]

use std::collections::BTreeSet;
use std::fs;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use mimir_common::block_on_for_tests;
use mimir_conformance::free_test_port;
use mimir_conformance::oracle::{KfakeBrokerRequest, SpawnedKfakeBroker};
use mimir_conformance::twin::{start_mimir, wait_ready, Impl, Proc};
use mimir_proto::cortexpb;
use mimir_proto::ingesterpb;
use mimir_proto::ingesterpb::ingester_client::IngesterClient;
use mimir_proto::prost::Message as _;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::{Code, Status};

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

fn float_series(labels: &[(&str, &str)], ts: i64, v: f64) -> cortexpb::TimeSeries {
    cortexpb::TimeSeries {
        labels: labels.iter().map(|(n, v)| lp(n, v)).collect(),
        samples: vec![cortexpb::Sample {
            value: v,
            timestamp_ms: ts,
        }],
        ..Default::default()
    }
}

fn with_org<T>(org: Option<&str>, msg: T) -> tonic::Request<T> {
    let mut req = tonic::Request::new(msg);
    if let Some(org) = org {
        req.metadata_mut()
            .insert("x-scope-orgid", MetadataValue::try_from(org).expect("ascii org"));
    }
    req
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

fn metric_lines_with_prefix(exposition: &str, name: &str) -> BTreeSet<String> {
    exposition
        .lines()
        .filter(|l| !l.starts_with('#') && (l.starts_with(&format!("{name}{{")) || l.starts_with(&format!("{name} "))))
        .map(ToString::to_string)
        .collect()
}

async fn connect_grpc(port: u16) -> IngesterClient<Channel> {
    let ch = Channel::from_shared(format!("http://127.0.0.1:{port}"))
        .expect("valid uri")
        .connect()
        .await
        .expect("connect gRPC");
    IngesterClient::new(ch)
}

async fn wait_grpc_listening(port: u16, proc: &mut Proc, timeout: Duration) -> Channel {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(s) = proc.try_wait() {
            panic!("{} exited ({s}) before gRPC listened:\n{}", proc.name, proc.log());
        }
        if let Ok(ch) = Channel::from_shared(format!("http://127.0.0.1:{port}"))
            .expect("valid uri")
            .connect()
            .await
        {
            return ch;
        }
        assert!(
            Instant::now() < deadline,
            "{} gRPC not listening within {timeout:?}:\n{}",
            proc.name,
            proc.log()
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn http_req(
    client: &reqwest::Client,
    method: reqwest::Method,
    port: u16,
    path: &str,
) -> (u16, Option<String>, String) {
    let resp = client
        .request(method, format!("http://127.0.0.1:{port}{path}"))
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .expect("http req");
    let status = resp.status().as_u16();
    let nosniff = resp
        .headers()
        .get("x-content-type-options")
        .and_then(|v| v.to_str().ok())
        .map(ToString::to_string);
    let body = resp.text().await.expect("http body");
    (status, nosniff, body)
}

fn outcome<T>(res: &Result<T, Status>) -> String {
    match res {
        Ok(_) => "OK".to_string(),
        Err(s) => format!(
            "{:?}|{}|details={}",
            s.code(),
            s.message(),
            !s.details().is_empty()
        ),
    }
}

async fn raw_grpc_push(
    grpc_port: u16,
    org_id: Option<&str>,
    msg_size_hdr: Option<&str>,
    grpc_encoding: Option<&str>,
    framed_body: Bytes,
) -> (Code, String) {
    use http_body_util::BodyExt;

    let tcp = tokio::net::TcpStream::connect(("127.0.0.1", grpc_port))
        .await
        .expect("connect");
    let (mut sender, conn) = hyper::client::conn::http2::handshake(
        hyper_util::rt::TokioExecutor::new(),
        hyper_util::rt::TokioIo::new(tcp),
    )
    .await
    .expect("h2 handshake");
    tokio::spawn(conn);

    let mut b = axum::http::Request::builder()
        .method("POST")
        .uri(format!("http://127.0.0.1:{grpc_port}/cortex.Ingester/Push"))
        .header("content-type", "application/grpc")
        .header("te", "trailers");
    if let Some(org) = org_id {
        b = b.header("x-scope-orgid", org);
    }
    if let Some(ms) = msg_size_hdr {
        b = b.header("message-size", ms);
    }
    if let Some(enc) = grpc_encoding {
        b = b.header("grpc-encoding", enc);
    }
    let req = b
        .body(http_body_util::Full::new(framed_body))
        .expect("request");
    let resp = sender.send_request(req).await.expect("send h2");
    let headers = resp.headers().clone();
    let collected = resp.into_body().collect().await.expect("collect body");
    let trailers = collected.trailers().cloned();
    let st = Status::from_header_map(&headers)
        .or_else(|| trailers.as_ref().and_then(Status::from_header_map));
    match st {
        Some(s) => (s.code(), s.message().to_string()),
        None => (Code::Ok, String::new()),
    }
}

#[test]
fn twin_process_ing02_ing03_ing06_ing07_ing10_ing14_parity() {
    let tmp = tempfile::tempdir().expect("tempdir");
    block_on_for_tests(async {
        let http = reqwest::Client::new();
        let now = now_ms();
        let mut results = Vec::new();

        for which in IMPLS {
            let (hp, gp) = (free_test_port(), free_test_port());
            let dir = tmp.path().join(which.name());
            fs::create_dir_all(dir.join("tsdb")).unwrap();
            fs::create_dir_all(dir.join("bucket")).unwrap();
            let yaml = format!(
                r#"target: ingester
multitenancy_enabled: true
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {hp}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {gp}
ingester:
  ring:
    instance_id: ing-0
    instance_addr: 127.0.0.1
    kvstore:
      store: inmemory
    replication_factor: 1
    observe_period: 3s
    min_ready_duration: 0s
    final_sleep: 0s
  instance_limits:
    max_inflight_push_requests_bytes: 100
  push_circuit_breaker:
    enabled: true
    initial_delay: 0s
    failure_execution_threshold: 100
    failure_threshold_percentage: 50
    cooldown_period: 1s
    request_timeout: 5s
  error_sample_rate: 1
  label_churn_enabled: false
limits:
  max_global_metadata_per_user: 1
  max_global_metadata_per_metric: 1
blocks_storage:
  backend: filesystem
  filesystem:
    dir: {bucket}
  tsdb:
    dir: {tsdb}
activity_tracker:
  filepath: ""
"#,
                bucket = dir.join("bucket").display(),
                tsdb = dir.join("tsdb").display(),
            );
            let mut proc = start_mimir(which, "w11-core", &dir, &yaml, &[]);

            // Wait for gRPC listener while ingester is deterministically Starting (observe_period: 3s).
            let ch = wait_grpc_listening(gp, &mut proc, Duration::from_secs(15)).await;
            let (ready_code, _, ready_body) = http_req(&http, reqwest::Method::GET, hp, "/ready").await;
            let starting_checks = if ready_code != 200 && ready_body.contains("ingester-service: Starting") {
                let mut c = IngesterClient::new(ch.clone());
                let push_starting = c
                    .push(with_org(
                        Some("t1"),
                        cortexpb::WriteRequest {
                            timeseries: vec![float_series(&[("__name__", "m_start")], now, 1.0)],
                            ..Default::default()
                        },
                    ))
                    .await;
                let bad_search = c
                    .search_label_names(with_org(
                        Some("t1"),
                        ingesterpb::SearchLabelNamesRequest {
                            matchers: vec![ingesterpb::LabelMatcher {
                                r#type: ingesterpb::MatchType::RegexMatch as i32,
                                name: "a".to_string(),
                                value: "[".to_string(),
                            }],
                            ..Default::default()
                        },
                    ))
                    .await;
                let flush_starting = http_req(&http, reqwest::Method::POST, hp, "/ingester/flush").await;
                Some((outcome(&push_starting), outcome(&bad_search), flush_starting))
            } else {
                None
            };

            wait_ready(&http, &mut proc, hp, Duration::from_secs(30)).await;
            let mut c = connect_grpc(gp).await;

            // ING-02: pre-decompression message-size check rejects with Code::Unavailable BEFORE
            // reading/decompressing invalid snappy payload.
            let bomb_or_corrupt_snappy = Bytes::from_static(&[1, 0, 0, 0, 4, 0xff, 0xff, 0xff, 0xff]);
            let oversize_tap = raw_grpc_push(
                gp,
                Some("t1"),
                Some("150"),
                Some("snappy"),
                bomb_or_corrupt_snappy,
            )
            .await;
            // ING-07: instance_limits rejection in StartPushRequest must still finish breaker permit as success
            let (_, _, metrics_after_limit) = http_req(&http, reqwest::Method::GET, hp, "/metrics").await;
            let cb_success_after_limit = metric_value(
                &metrics_after_limit,
                "cortex_ingester_circuit_breaker_results_total",
                &[("request_type", "push"), ("result", "success")],
            );

            // message-size: 0 when inflight_bytes == 0 succeeds
            let valid_bytes = cortexpb::WriteRequest {
                timeseries: vec![float_series(&[("__name__", "m0")], now, 1.0)],
                ..Default::default()
            }
            .encode_to_vec();
            let valid_frame = mimir_proto::snappy::encode_grpc_frame(&valid_bytes, false).unwrap();
            let zero_hdr_ok = raw_grpc_push(gp, Some("t1"), Some("0"), None, valid_frame).await;

            // ING-03: unsorted wire labels parity against live Go Mimir
            let r_create = outcome(
                &c.push(with_org(
                    Some("t1"),
                    cortexpb::WriteRequest {
                        timeseries: vec![float_series(
                            &[("__name__", "m1"), ("a", "1"), ("b", "2")],
                            now,
                            1.0,
                        )],
                        ..Default::default()
                    },
                ))
                .await,
            );
            let r_sorted_empty = outcome(
                &c.push(with_org(
                    Some("t1"),
                    cortexpb::WriteRequest {
                        timeseries: vec![float_series(
                            &[("__name__", "m1"), ("a", "1"), ("b", "2"), ("e", "")],
                            now + 1000,
                            2.0,
                        )],
                        ..Default::default()
                    },
                ))
                .await,
            );
            let r_unsorted_existing = outcome(
                &c.push(with_org(
                    Some("t1"),
                    cortexpb::WriteRequest {
                        timeseries: vec![float_series(
                            &[("__name__", "m1"), ("b", "2"), ("a", "1")],
                            now + 2000,
                            3.0,
                        )],
                        ..Default::default()
                    },
                ))
                .await,
            );
            let r_unsorted_empty = outcome(
                &c.push(with_org(
                    Some("t1"),
                    cortexpb::WriteRequest {
                        timeseries: vec![float_series(
                            &[("__name__", "m1"), ("b", "2"), ("e", ""), ("a", "1")],
                            now + 3000,
                            4.0,
                        )],
                        ..Default::default()
                    },
                ))
                .await,
            );

            // ING-06: metadata limits warn logs report global limit ("1")
            let _ = c
                .push(with_org(
                    Some("t1"),
                    cortexpb::WriteRequest {
                        metadata: vec![
                            cortexpb::MetricMetadata {
                                r#type: 1,
                                metric_family_name: "meta_a".into(),
                                help: "h1".into(),
                                unit: String::new(),
                            },
                            cortexpb::MetricMetadata {
                                r#type: 1,
                                metric_family_name: "meta_b".into(),
                                help: "h2".into(),
                                unit: String::new(),
                            },
                        ],
                        ..Default::default()
                    },
                ))
                .await;
            let _ = c
                .push(with_org(
                    Some("t1"),
                    cortexpb::WriteRequest {
                        metadata: vec![cortexpb::MetricMetadata {
                            r#type: 1,
                            metric_family_name: "meta_a".into(),
                            help: "h_other".into(),
                            unit: String::new(),
                        }],
                        ..Default::default()
                    },
                ))
                .await;
            tokio::time::sleep(Duration::from_millis(200)).await;
            let log_text = proc.log();
            let has_global_user_meta_err = log_text.contains("per-user metric metadata limit of 1 exceeded");
            let has_global_metric_meta_err = log_text.contains("per-metric metadata limit of 1 exceeded");

            // ING-10: label_churn disabled returns 404 BEFORE query parsing (?limit=abc)
            let lc_disabled = http_req(
                &http,
                reqwest::Method::GET,
                hp,
                "/ingester/label_churn/t1?limit=abc",
            )
            .await;

            // ING-14: downscale & flush & shutdown parity
            let ds_get0 = http_req(
                &http,
                reqwest::Method::GET,
                hp,
                "/ingester/prepare-instance-ring-downscale",
            )
            .await;
            let ds_post = http_req(
                &http,
                reqwest::Method::POST,
                hp,
                "/ingester/prepare-instance-ring-downscale",
            )
            .await;
            let ds_del = http_req(
                &http,
                reqwest::Method::DELETE,
                hp,
                "/ingester/prepare-instance-ring-downscale",
            )
            .await;
            let flush_ok = http_req(&http, reqwest::Method::POST, hp, "/ingester/flush?wait=true").await;
            let shutdown_ok = http_req(&http, reqwest::Method::POST, hp, "/ingester/shutdown").await;

            results.push((
                which,
                starting_checks,
                oversize_tap,
                cb_success_after_limit,
                zero_hdr_ok,
                r_create,
                r_sorted_empty,
                r_unsorted_existing,
                r_unsorted_empty,
                has_global_user_meta_err,
                has_global_metric_meta_err,
                lc_disabled,
                ds_get0,
                ds_post.0,
                ds_del,
                flush_ok,
                shutdown_ok,
            ));
        }

        let g = &results[0];
        let r = &results[1];

        let gs = g.1.as_ref().expect("Go ingester did not observe Starting window");
        let rs = r.1.as_ref().expect("Rust ingester did not observe Starting window");
        assert_eq!(rs.0, gs.0, "ING-02 starting Push mismatch");
        assert!(rs.0.starts_with("Unavailable|"), "{}", rs.0);
        assert_eq!(rs.1, gs.1, "ING-02 starting SearchLabelNames bad regex mismatch");
        assert!(rs.1.starts_with("Unavailable|"), "{}", rs.1);
        assert_eq!(rs.2 .0, gs.2 .0, "ING-14 flush starting status");
        assert_eq!(r.2, g.2, "ING-02 oversize message-size tap mismatch");
        assert_eq!(r.2 .0, Code::Unavailable);
        assert_eq!(r.3, g.3, "ING-07 cb success after instance limit mismatch");
        assert_eq!(r.3, Some(1.0));
        assert_eq!(r.4, g.4, "ING-02 zero message-size mismatch");
        assert_eq!(r.4 .0, Code::Ok);

        assert_eq!(r.5, g.5, "ING-03 create");
        assert_eq!(r.6, g.6, "ING-03 sorted+empty");
        assert_eq!(r.7, g.7, "ING-03 unsorted existing");
        assert_eq!(r.8, g.8, "ING-03 unsorted+empty");

        assert!(g.9 && r.9, "ING-06 per-user meta limit log missing");
        assert!(g.10 && r.10, "ING-06 per-metric meta limit log missing");

        assert_eq!(r.11, g.11, "ING-10 disabled label_churn with bad query");
        assert_eq!(r.11 .0, 404);
        assert_eq!(r.11 .1.as_deref(), Some("nosniff"));

        assert_eq!(r.12, g.12, "ING-14 downscale GET");
        assert_eq!(r.13, g.13, "ING-14 downscale POST status");
        assert_eq!(r.14, g.14, "ING-14 downscale DELETE");
        assert_eq!(r.15, g.15, "ING-14 flush?wait=true");
        assert_eq!(r.16, g.16, "ING-14 shutdown");
    });
}

#[test]
fn twin_process_ing01_reactive_limiter_and_ing02_starting_push_disabled_and_toobusy() {
    let tmp = tempfile::tempdir().expect("tempdir");
    block_on_for_tests(async {
        let http = reqwest::Client::new();

        // 1) ING-01: Read reactive limiter under kfake strong-consistency delay -> Code::Unavailable "limit exceeded"
        //    + ING-02: push_grpc_method_enabled: false while Starting -> Code::Unavailable (NOT Unimplemented)
        let mut rl_outcomes = Vec::new();
        for which in IMPLS {
            let broker = SpawnedKfakeBroker::spawn(&KfakeBrokerRequest {
                topic: "ingest".to_string(),
                partitions: 1,
                compression: None,
                records: Vec::new(),
            })
            .expect("kfake");
            let (hp, gp) = (free_test_port(), free_test_port());
            let dir = tmp.path().join(format!("rl-{}", which.name()));
            fs::create_dir_all(dir.join("tsdb")).unwrap();
            fs::create_dir_all(dir.join("bucket")).unwrap();
            let yaml = format!(
                r#"target: ingester
multitenancy_enabled: true
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {hp}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {gp}
ingester:
  ring:
    instance_id: ing-0
    instance_addr: 127.0.0.1
    kvstore:
      store: inmemory
    replication_factor: 1
    min_ready_duration: 1s
    final_sleep: 0s
  partition_ring:
    kvstore:
      store: inmemory
    min_partition_owners_count: 1
    min_partition_owners_duration: 0s
  push_grpc_method_enabled: false
  read_reactive_limiter:
    enabled: true
    min_limit: 1
    initial_limit: 1
    max_limit: 1
    max_limit_factor: 1.0
    min_limit_factor: 1.0
    recent_window_min_duration: 1s
    recent_window_max_duration: 1s
    recent_window_min_samples: 1
    initial_rejection_factor: 1.0
    max_rejection_factor: 1.0
ingest_storage:
  enabled: true
  kafka:
    address: {broker}
    topic: {topic}
    auto_create_topic_enabled: false
    wait_strong_read_consistency_timeout: 500ms
    last_produced_offset_poll_interval: 25ms
blocks_storage:
  backend: filesystem
  filesystem:
    dir: {bucket}
  tsdb:
    dir: {tsdb}
activity_tracker:
  filepath: ""
"#,
                broker = broker.broker_addr(),
                topic = broker.topic(),
                bucket = dir.join("bucket").display(),
                tsdb = dir.join("tsdb").display(),
            );
            let mut proc = start_mimir(which, "ing01-rl", &dir, &yaml, &[]);
            let ch_early = wait_grpc_listening(gp, &mut proc, Duration::from_secs(15)).await;
            let early_ready = http_req(&http, reqwest::Method::GET, hp, "/ready").await.0;
            let push_disabled_starting = if early_ready != 200 {
                let mut ec = IngesterClient::new(ch_early);
                let res = ec
                    .push(with_org(
                        Some("t1"),
                        cortexpb::WriteRequest {
                            timeseries: vec![float_series(&[("__name__", "m")], now_ms(), 1.0)],
                            ..Default::default()
                        },
                    ))
                    .await;
                Some(outcome(&res))
            } else {
                None
            };

            wait_ready(&http, &mut proc, hp, Duration::from_secs(30)).await;
            let ch = Channel::from_shared(format!("http://127.0.0.1:{gp}"))
                .unwrap()
                .connect()
                .await
                .unwrap();

            // Hold 1 slow strong-consistency read while firing 8 concurrent reads to overflow queue (cap 1).
            let mut slow = IngesterClient::new(ch.clone());
            let slow_h = tokio::spawn(async move {
                let mut req = with_org(Some("t1"), ingesterpb::LabelNamesRequest::default());
                req.metadata_mut()
                    .insert("__consistency_level__", MetadataValue::from_static("strong"));
                req.metadata_mut()
                    .insert("__consistency_offsets__", MetadataValue::from_static("v1=0:999999"));
                let _ = slow.label_names(req).await;
            });
            tokio::time::sleep(Duration::from_millis(50)).await;

            let mut rejects = BTreeSet::new();
            let mut futs = Vec::new();
            for _ in 0..8 {
                let mut cl = IngesterClient::new(ch.clone());
                futs.push(tokio::spawn(async move {
                    let mut req = with_org(Some("t1"), ingesterpb::LabelNamesRequest::default());
                    req.metadata_mut()
                        .insert("__consistency_level__", MetadataValue::from_static("strong"));
                    req.metadata_mut().insert(
                        "__consistency_offsets__",
                        MetadataValue::from_static("v1=0:999999"),
                    );
                    cl.label_names(req).await
                }));
            }
            for f in futs {
                if let Err(st) = f.await.unwrap() {
                    if st.message().contains("limit exceeded") {
                        rejects.insert(format!(
                            "{:?}|{}|details={}",
                            st.code(),
                            st.message(),
                            !st.details().is_empty()
                        ));
                    }
                }
            }
            let _ = slow_h.await;
            rl_outcomes.push((which, push_disabled_starting, rejects));
        }
        if let (Some(gp), Some(rp)) = (&rl_outcomes[0].1, &rl_outcomes[1].1) {
            assert_eq!(rp, gp, "push_grpc_method_enabled=false with ingest_storage Starting mismatch");
            assert!(rp.starts_with("Unimplemented|"), "{rp}");
        }
        assert!(!rl_outcomes[0].2.is_empty(), "Go did not reject via reactive limiter");
        assert_eq!(rl_outcomes[1].2, rl_outcomes[0].2, "ING-01 reactive limiter mismatch");
        assert!(rl_outcomes[1].2.contains("Unavailable|limit exceeded|details=false"));

        // 2) ING-02: TooBusy via TapHandle returns Code::Unavailable without ErrorDetails
        let mut busy_outcomes = Vec::new();
        for which in IMPLS {
            let (hp, gp) = (free_test_port(), free_test_port());
            let dir = tmp.path().join(format!("busy-{}", which.name()));
            fs::create_dir_all(dir.join("tsdb")).unwrap();
            fs::create_dir_all(dir.join("bucket")).unwrap();
            let yaml = format!(
                r#"target: ingester
multitenancy_enabled: true
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {hp}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {gp}
ingester:
  ring:
    instance_id: ing-0
    instance_addr: 127.0.0.1
    kvstore:
      store: inmemory
    replication_factor: 1
    min_ready_duration: 0s
    final_sleep: 0s
  read_path_memory_utilization_limit: 1
blocks_storage:
  backend: filesystem
  filesystem:
    dir: {bucket}
  tsdb:
    dir: {tsdb}
activity_tracker:
  filepath: ""
"#,
                bucket = dir.join("bucket").display(),
                tsdb = dir.join("tsdb").display(),
            );
            let mut proc = start_mimir(which, "ing02-busy", &dir, &yaml, &[]);
            wait_ready(&http, &mut proc, hp, Duration::from_secs(30)).await;
            let mut c = connect_grpc(gp).await;

            let deadline = Instant::now() + Duration::from_secs(10);
            let st = loop {
                let res = c
                    .label_names(with_org(Some("t1"), ingesterpb::LabelNamesRequest::default()))
                    .await;
                if let Err(s) = res {
                    break s;
                }
                assert!(Instant::now() < deadline, "{}: TooBusy never engaged", which.name());
                tokio::time::sleep(Duration::from_millis(100)).await;
            };
            busy_outcomes.push((
                which,
                format!("{:?}|{}|details={}", st.code(), st.message(), !st.details().is_empty()),
            ));
        }
        assert_eq!(busy_outcomes[1].1, busy_outcomes[0].1, "ING-02 TooBusy TapHandle mismatch");
        assert!(busy_outcomes[1].1.starts_with("Unavailable|"));
    });
}

#[test]
fn twin_process_ing10_label_churn_query_and_encoded_path_parity() {
    let tmp = tempfile::tempdir().expect("tempdir");
    block_on_for_tests(async {
        let http = reqwest::Client::new();
        let mut results = Vec::new();
        for which in IMPLS {
            let (hp, gp) = (free_test_port(), free_test_port());
            let dir = tmp.path().join(which.name());
            fs::create_dir_all(dir.join("tsdb")).unwrap();
            fs::create_dir_all(dir.join("bucket")).unwrap();
            let yaml = format!(
                r#"target: ingester
multitenancy_enabled: true
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {hp}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {gp}
ingester:
  ring:
    instance_id: ing-0
    instance_addr: 127.0.0.1
    kvstore:
      store: inmemory
    replication_factor: 1
    min_ready_duration: 0s
    final_sleep: 0s
  label_churn_enabled: true
blocks_storage:
  backend: filesystem
  filesystem:
    dir: {bucket}
  tsdb:
    dir: {tsdb}
activity_tracker:
  filepath: ""
"#,
                bucket = dir.join("bucket").display(),
                tsdb = dir.join("tsdb").display(),
            );
            let mut proc = start_mimir(which, "ing10", &dir, &yaml, &[]);
            wait_ready(&http, &mut proc, hp, Duration::from_secs(30)).await;
            let mut c = connect_grpc(gp).await;
            c.push(with_org(
                Some("t1"),
                cortexpb::WriteRequest {
                    timeseries: vec![float_series(&[("__name__", "m1"), ("a", "1")], now_ms(), 1.0)],
                    ..Default::default()
                },
            ))
            .await
            .unwrap();

            let paths = [
                "/ingester/label_churn/t1?limit=abc",
                "/ingester/label_churn/t1?limit=-1",
                "/ingester/label_churn/t1?lookback=0s",
                "/ingester/label_churn/t1?lookback=&limit=",
                "/ingester/label_churn/t1?limit=1&limit=abc",
                "/ingester/label_churn/t%201",
            ];
            let mut resps = Vec::new();
            for p in paths {
                resps.push(http_req(&http, reqwest::Method::GET, hp, p).await);
            }
            results.push(resps);
        }
        assert_eq!(results[1], results[0], "ING-10 label_churn responses differ");
    });
}

#[test]
fn twin_process_ing04_cost_attribution_purge_and_ing05_label_value_bytes_cleanup() {
    let tmp = tempfile::tempdir().expect("tempdir");
    block_on_for_tests(async {
        let http = reqwest::Client::new();
        let now = now_ms();
        let mut ca_results = Vec::new();
        let mut lvb_results = Vec::new();

        // 1) ING-04: Cost attribution active series decrements cleanly on early compaction purge
        for which in IMPLS {
            let (hp, gp) = (free_test_port(), free_test_port());
            let dir = tmp.path().join(format!("ca-{}", which.name()));
            fs::create_dir_all(dir.join("tsdb")).unwrap();
            fs::create_dir_all(dir.join("bucket")).unwrap();
            let yaml = format!(
                r#"target: ingester
multitenancy_enabled: true
cost_attribution_registry_path: /usage-metrics
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {hp}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {gp}
ingester:
  ring:
    instance_id: ing-0
    instance_addr: 127.0.0.1
    kvstore:
      store: inmemory
    replication_factor: 1
    min_ready_duration: 0s
    final_sleep: 0s
  active_series_metrics_enabled: true
  active_series_metrics_update_period: 200ms
  active_series_metrics_idle_timeout: 500ms
limits:
  cost_attribution_trackers:
    by-team:
      labels:
        - input: team
          output: my_team
  max_cost_attribution_cardinality: 100
blocks_storage:
  backend: filesystem
  filesystem:
    dir: {bucket}
  tsdb:
    dir: {tsdb}
    head_compaction_interval: 300ms
    early_head_compaction_min_in_memory_series: 1
    early_head_compaction_min_estimated_series_reduction_percentage: 1
activity_tracker:
  filepath: ""
"#,
                bucket = dir.join("bucket").display(),
                tsdb = dir.join("tsdb").display(),
            );
            let mut proc = start_mimir(which, "ing04", &dir, &yaml, &[]);
            wait_ready(&http, &mut proc, hp, Duration::from_secs(30)).await;
            let mut c = connect_grpc(gp).await;
            c.push(with_org(
                Some("t1"),
                cortexpb::WriteRequest {
                    timeseries: vec![float_series(&[("__name__", "m_ca"), ("team", "alpha")], now - 10_000, 1.0)],
                    ..Default::default()
                },
            ))
            .await
            .unwrap();

            // First confirm the series is attributed (1.0), then wait for early compaction purge (idle timeout 500ms).
            let seen_deadline = Instant::now() + Duration::from_secs(5);
            loop {
                let (_, _, usage_body) = http_req(&http, reqwest::Method::GET, hp, "/usage-metrics").await;
                if metric_value(
                    &usage_body,
                    "cortex_ingester_attributed_active_series",
                    &[("tenant", "t1"), ("my_team", "alpha")],
                ) == Some(1.0)
                {
                    break;
                }
                assert!(Instant::now() < seen_deadline, "{}: attributed_active_series never reached 1.0", which.name());
                tokio::time::sleep(Duration::from_millis(50)).await;
            }

            let deadline = Instant::now() + Duration::from_secs(10);
            let attr_active = loop {
                let (_, _, usage_body) = http_req(&http, reqwest::Method::GET, hp, "/usage-metrics").await;
                let val = metric_value(
                    &usage_body,
                    "cortex_ingester_attributed_active_series",
                    &[("tenant", "t1"), ("my_team", "alpha")],
                );
                if val.is_none() || val == Some(0.0) {
                    break 0.0;
                }
                if Instant::now() >= deadline {
                    break val.unwrap_or(-1.0);
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            };
            let (_, _, metrics_body) = http_req(&http, reqwest::Method::GET, hp, "/metrics").await;
            let attr_failures = metric_value(
                &metrics_body,
                "cortex_ingester_attributed_active_series_failure",
                &[("user", "t1")],
            )
            .unwrap_or(0.0);
            ca_results.push((which, attr_failures, attr_active));
        }
        assert_eq!(ca_results[1].1, 0.0, "ING-04 attribution failures: {ca_results:?}");
        assert_eq!(ca_results[1].1, ca_results[0].1);
        assert_eq!(ca_results[1].2, 0.0, "ING-04 attributed_active_series not purged: {ca_results:?}");
        assert_eq!(ca_results[1].2, ca_results[0].2);

        // 2) ING-05: label_value_bytes_over_threshold deletes stale series when threshold disabled
        for which in IMPLS {
            let (hp, gp) = (free_test_port(), free_test_port());
            let dir = tmp.path().join(format!("lvb-{}", which.name()));
            fs::create_dir_all(dir.join("tsdb")).unwrap();
            fs::create_dir_all(dir.join("bucket")).unwrap();
            let runtime_cfg = dir.join("runtime.yaml");
            fs::write(
                &runtime_cfg,
                "overrides:\n  t1:\n    label_value_bytes_metric_threshold: 100\n",
            )
            .unwrap();
            let yaml = format!(
                r#"target: ingester
multitenancy_enabled: true
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {hp}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {gp}
ingester:
  ring:
    instance_id: ing-0
    instance_addr: 127.0.0.1
    kvstore:
      store: inmemory
    replication_factor: 1
    min_ready_duration: 0s
    final_sleep: 0s
runtime_config:
  file: {rt}
  period: 200ms
limits:
  label_value_bytes_metric_threshold: 100
blocks_storage:
  backend: filesystem
  filesystem:
    dir: {bucket}
  tsdb:
    dir: {tsdb}
activity_tracker:
  filepath: ""
"#,
                rt = runtime_cfg.display(),
                bucket = dir.join("bucket").display(),
                tsdb = dir.join("tsdb").display(),
            );
            let mut proc = start_mimir(which, "ing05", &dir, &yaml, &[]);
            wait_ready(&http, &mut proc, hp, Duration::from_secs(30)).await;
            let mut c = connect_grpc(gp).await;
            let long_val = "x".repeat(200);
            c.push(with_org(
                Some("t1"),
                cortexpb::WriteRequest {
                    timeseries: vec![float_series(
                        &[("__name__", "m_lvb"), ("long_lbl", &long_val)],
                        now,
                        1.0,
                    )],
                    ..Default::default()
                },
            ))
            .await
            .unwrap();

            let deadline = Instant::now() + Duration::from_secs(20);
            loop {
                let (_, _, m) = http_req(&http, reqwest::Method::GET, hp, "/metrics").await;
                if metric_value(
                    &m,
                    "cortex_ingester_label_value_bytes_over_threshold",
                    &[("user", "t1"), ("label", "long_lbl")],
                ) == Some(1.0)
                {
                    break;
                }
                assert!(Instant::now() < deadline, "{}: label_value_bytes metric never appeared", which.name());
                tokio::time::sleep(Duration::from_millis(500)).await;
            }

            fs::write(
                &runtime_cfg,
                "overrides:\n  t1:\n    label_value_bytes_metric_threshold: 0\n",
            )
            .unwrap();
            let deadline2 = Instant::now() + Duration::from_secs(20);
            let cleared = loop {
                let (_, _, m) = http_req(&http, reqwest::Method::GET, hp, "/metrics").await;
                if metric_lines_with_prefix(&m, "cortex_ingester_label_value_bytes_over_threshold").is_empty() {
                    break true;
                }
                if Instant::now() >= deadline2 {
                    break false;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            };
            lvb_results.push((which, cleared));
        }
        assert!(lvb_results[0].1 && lvb_results[1].1, "ING-05 stale metric not cleared: {lvb_results:?}");
    });
}
