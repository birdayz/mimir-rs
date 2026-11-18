//! Wave 12 differential conformance tests for Observability, Trace Propagation,
//! Admin HTTP UI/JSON, /debug/pprof + /debug/fgprof, -log.format=json, and usage-stats.
//!
//! Boots unmodified Go mimir (target/go-mimir) and Rust mimir (target/release/mimir)
//! side-by-side and verifies exact behavioral parity across W12-H2-01..14.

use std::fs;
use std::path::Path;
use std::time::Duration;

use mimir_conformance::free_test_port;
use mimir_conformance::oracle::{KfakeBrokerRequest, SpawnedKfakeBroker};
use mimir_conformance::twin::{binary, wait_ready, Impl, Proc};
use mimir_proto::httpgrpcpb::http_client::HttpClient;
use mimir_proto::httpgrpcpb::HttpRequest;
use mimir_proto::ingesterpb;
use mimir_proto::ingesterpb::ingester_client::IngesterClient;
use serde_json::Value;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;

const IMPLS: [Impl; 2] = [Impl::Go, Impl::Rust];

fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(15))
        .build()
        .unwrap()
}

fn tracing_cluster_yaml(
    dir: &Path,
    http_port: u16,
    grpc_port: u16,
    create_new_traces: bool,
    activity_path: &Path,
) -> String {
    let tsdb_dir = dir.join("tsdb");
    let bucket_dir = dir.join("bucket");
    fs::create_dir_all(&tsdb_dir).unwrap();
    fs::create_dir_all(&bucket_dir).unwrap();
    format!(
        r#"target: ingester
multitenancy_enabled: false
no_auth_tenant: tenant-default
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http_port}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc_port}
  log_level: debug
  log_format: json
  http_server_write_timeout: 2s
  create_new_traces: {create_new_traces}
  trace_request_headers: true
  cluster_validation:
    label: expected-cluster
    http:
      enabled: true
      soft_validation: true
    grpc:
      enabled: true
      soft_validation: true
activity_tracker:
  filepath: {activity}
ingester:
  ring:
    instance_id: ingester-0
    instance_addr: 127.0.0.1
    kvstore:
      store: inmemory
    replication_factor: 1
    min_ready_duration: 0s
    final_sleep: 0s
querier:
  timeout: 2s
blocks_storage:
  backend: filesystem
  filesystem:
    dir: {bucket}
  tsdb:
    dir: {tsdb}
usage_stats:
  enabled: false
"#,
        activity = activity_path.display(),
        bucket = bucket_dir.display(),
        tsdb = tsdb_dir.display(),
    )
}

fn parse_json_logs(raw: &str) -> Vec<Value> {
    raw.lines()
        .filter_map(|line| serde_json::from_str::<Value>(line.trim()).ok())
        .collect()
}

#[test]
fn w12_tracing_json_logs_pprof_fgprof_and_admin_parity() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let client = http_client();
    let dir = tempfile::tempdir().unwrap();

    for which in IMPLS {
        let bin = binary(which);
        let work = dir.path().join(which.name());
        fs::create_dir_all(&work).unwrap();
        let http_port = free_test_port();
        let grpc_port = free_test_port();
        let activity_path = work.join("activity.log");
        let cfg_path = work.join("mimir.yaml");
        fs::write(
            &cfg_path,
            tracing_cluster_yaml(&work, http_port, grpc_port, false, &activity_path),
        )
        .unwrap();

        let mut proc = Proc::spawn_with_env(
            &format!("tracing-{}", which.name()),
            &bin,
            &[format!("-config.file={}", cfg_path.display())],
            &work,
            &[("OTEL_TRACES_EXPORTER", "none")],
        );

        let sampled_tid = "4bf92f3577b34da6a3ce929d0e0e4736";
        let unsampled_tid = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let jaeger_short = "48475050943e8e05";
        let jaeger_padded = "000000000000000048475050943e8e05";
        let dbg_tid = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

        rt.block_on(async {
            wait_ready(&client, &mut proc, http_port, Duration::from_secs(20)).await;

            let _ = client
                .get(format!("http://127.0.0.1:{http_port}/nonexistent-sampled"))
                .header("traceparent", format!("00-{sampled_tid}-00f067aa0ba902b7-01"))
                .send()
                .await
                .unwrap();

            let _ = client
                .get(format!("http://127.0.0.1:{http_port}/nonexistent-unsampled"))
                .header("traceparent", format!("00-{unsampled_tid}-00f067aa0ba902b7-00"))
                .send()
                .await
                .unwrap();

            let _ = client
                .get(format!("http://127.0.0.1:{http_port}/nonexistent-jaeger"))
                .header("uber-trace-id", format!("{jaeger_short}:70e8b02d28e4337b:0:1"))
                .send()
                .await
                .unwrap();

            let _ = client
                .get(format!("http://127.0.0.1:{http_port}/nonexistent-debug-id"))
                .header("traceparent", format!("00-{dbg_tid}-00f067aa0ba902b7-00"))
                .header("Jaeger-Debug-Id", "force-sample-1")
                .send()
                .await
                .unwrap();

            let ch = Channel::from_shared(format!("http://127.0.0.1:{grpc_port}"))
                .unwrap()
                .connect()
                .await
                .unwrap();
            let mut ing_client = IngesterClient::new(ch.clone());
            let mut greq = tonic::Request::new(ingesterpb::LabelNamesRequest {
                start_timestamp_ms: 0,
                end_timestamp_ms: 1000,
                matchers: None,
                limit: 0,
            });
            greq.metadata_mut().insert(
                "traceparent",
                MetadataValue::try_from(format!("00-{sampled_tid}-00f067aa0ba902b7-01")).unwrap(),
            );
            let _ = ing_client.label_names(greq).await.unwrap();

            let mut hg_client = HttpClient::new(ch);
            let mut hreq = tonic::Request::new(HttpRequest {
                method: "GET".to_string(),
                url: "/ingester/ring".to_string(),
                headers: Vec::new(),
                body: Vec::new().into(),
            });
            hreq.metadata_mut().insert(
                "traceparent",
                MetadataValue::try_from(format!("00-{sampled_tid}-00f067aa0ba902b7-01")).unwrap(),
            );
            let hg_resp = hg_client.handle(hreq).await.unwrap().into_inner();
            assert_eq!(hg_resp.code, 200, "{}: httpgrpc /ingester/ring", which.name());

            // W12-H2-04: bogus /ring and /partition-ring must return 404.
            for bogus in ["/ring", "/partition-ring"] {
                let r = client
                    .get(format!("http://127.0.0.1:{http_port}{bogus}"))
                    .send()
                    .await
                    .unwrap();
                assert_eq!(r.status().as_u16(), 404, "{}: {bogus} must be 404", which.name());
            }

            // W12-H2-02: duplicate/malformed query params on /ingester/ring must return 200 OK.
            for q in ["?tokens=true&tokens=false", "?foo=%ZZ&tokens=true", "?a=1;b=2"] {
                let r = client
                    .get(format!("http://127.0.0.1:{http_port}/ingester/ring{q}"))
                    .send()
                    .await
                    .unwrap();
                assert_eq!(
                    r.status().as_u16(),
                    200,
                    "{}: /ingester/ring{q} must be 200",
                    which.name()
                );
            }

            // W12-H2-05: /ingester/ring JSON & HTML parity.
            let r_json = client
                .get(format!("http://127.0.0.1:{http_port}/ingester/ring"))
                .header("Accept", "application/json")
                .send()
                .await
                .unwrap();
            assert_eq!(r_json.status().as_u16(), 200);
            assert_eq!(
                r_json.headers().get("content-type").unwrap().to_str().unwrap(),
                "application/json"
            );
            let raw_ring_json = r_json.text().await.unwrap();
            assert!(
                raw_ring_json.ends_with('\n'),
                "{}: /ingester/ring JSON must end with newline",
                which.name()
            );
            let ring_v: Value = serde_json::from_str(&raw_ring_json).unwrap();
            assert!(ring_v["shards"].is_array());
            assert!(ring_v["now"].is_string());

            let r_html = client
                .get(format!("http://127.0.0.1:{http_port}/ingester/ring"))
                .send()
                .await
                .unwrap();
            assert_eq!(r_html.status().as_u16(), 200);
            assert_eq!(
                r_html.headers().get("content-type").unwrap().to_str().unwrap(),
                "text/html"
            );
            let ring_html = r_html.text().await.unwrap();
            assert!(ring_html.contains("name=\"csrf_token\""), "{}: csrf_token", which.name());
            assert!(ring_html.contains("100%"), "{}: ownership 100%", which.name());
            assert!(ring_html.contains("ago ("), "{}: durationSince ago", which.name());

            // W12-H2-03: /debug/pprof/* method gating, WriteTimeout extension, and /debug/fgprof.
            for method in [reqwest::Method::POST, reqwest::Method::PUT, reqwest::Method::DELETE] {
                for p in ["/debug/pprof/profile", "/debug/pprof/", "/debug/pprof/symbol"] {
                    let resp = client
                        .request(method.clone(), format!("http://127.0.0.1:{http_port}{p}"))
                        .send()
                        .await
                        .unwrap();
                    assert_eq!(
                        resp.status().as_u16(),
                        405,
                        "{}: {method} {p} expected 405",
                        which.name()
                    );
                    let allow = resp
                        .headers()
                        .get("allow")
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("");
                    assert!(allow.contains("GET") && allow.contains("HEAD"), "{}: allow={allow}", which.name());
                }
            }
            let cmdline_resp = client
                .get(format!("http://127.0.0.1:{http_port}/debug/pprof/cmdline"))
                .send()
                .await
                .unwrap();
            assert_eq!(cmdline_resp.status().as_u16(), 404);

            // seconds=3 > http_server_write_timeout=2s must SUCCEED (200 OK).
            let pprof_resp = client
                .get(format!("http://127.0.0.1:{http_port}/debug/pprof/profile?seconds=3"))
                .send()
                .await
                .unwrap();
            assert_eq!(
                pprof_resp.status().as_u16(),
                200,
                "{}: /debug/pprof/profile?seconds=2 must succeed past 1s WriteTimeout",
                which.name()
            );
            let gz_bytes = pprof_resp.bytes().await.unwrap();
            assert!(gz_bytes.starts_with(&[0x1f, 0x8b]), "{}: expected gzip pprof", which.name());

            let fg_bad = client
                .get(format!("http://127.0.0.1:{http_port}/debug/fgprof?seconds=abc"))
                .send()
                .await
                .unwrap();
            assert_eq!(fg_bad.status().as_u16(), 400);

            let fg_folded = client
                .get(format!(
                    "http://127.0.0.1:{http_port}/debug/fgprof?seconds=1&format=folded"
                ))
                .send()
                .await
                .unwrap();
            assert_eq!(fg_folded.status().as_u16(), 200);

            // W12-H2-12: case-insensitive /ingester/unregister-on-shutdown.
            let unreg = client
                .put(format!(
                    "http://127.0.0.1:{http_port}/ingester/unregister-on-shutdown"
                ))
                .body(r#"{"Unregister":false}"#)
                .send()
                .await
                .unwrap();
            assert_eq!(unreg.status().as_u16(), 200, "{}: case-insensitive PUT", which.name());
            let unreg_json: Value = unreg.json().await.unwrap();
            assert_eq!(unreg_json["unregister"], serde_json::json!(false));

            tokio::time::sleep(Duration::from_millis(150)).await;
        });
        proc.sigterm();
        let _ = proc.wait_exit(Duration::from_secs(5));

        let logs = parse_json_logs(&proc.log());

        let sampled_http_cv = logs.iter().find(|v| {
            v["path"] == "/nonexistent-sampled"
                && v["msg"] == "request with no cluster validation label"
        });
        let sampled_http_cv = sampled_http_cv
            .unwrap_or_else(|| panic!("{}: missing sampled HTTP cv log\n{}", which.name(), proc.log()));
        assert_eq!(sampled_http_cv["soft_validation"], serde_json::json!(true));
        assert_eq!(sampled_http_cv["trace_id"], serde_json::json!(sampled_tid));

        let unsampled_http_log = logs.iter().find(|v| {
            v["msg"]
                .as_str()
                .is_some_and(|m| m.contains("/nonexistent-unsampled (404)"))
        });
        let unsampled_http_log = unsampled_http_log
            .unwrap_or_else(|| panic!("{}: missing unsampled HTTP log\n{}", which.name(), proc.log()));
        assert_eq!(
            unsampled_http_log["trace_id_unsampled"],
            serde_json::json!(unsampled_tid)
        );

        let jaeger_http_log = logs.iter().find(|v| {
            v["msg"]
                .as_str()
                .is_some_and(|m| m.contains("/nonexistent-jaeger (404)"))
        });
        let jaeger_http_log = jaeger_http_log
            .unwrap_or_else(|| panic!("{}: missing jaeger HTTP log\n{}", which.name(), proc.log()));
        assert_eq!(jaeger_http_log["trace_id"], serde_json::json!(jaeger_padded));

        let dbg_http_log = logs.iter().find(|v| {
            v["msg"]
                .as_str()
                .is_some_and(|m| m.contains("/nonexistent-debug-id (404)"))
        });
        let dbg_http_log = dbg_http_log
            .unwrap_or_else(|| panic!("{}: missing Jaeger-Debug-Id HTTP log\n{}", which.name(), proc.log()));
        assert_eq!(dbg_http_log["trace_id"], serde_json::json!(dbg_tid));

        let grpc_cv = logs.iter().find(|v| {
            v["method"] == "/cortex.Ingester/LabelNames"
                && v["msg"] == "request with no cluster validation label"
        });
        let grpc_cv = grpc_cv
            .unwrap_or_else(|| panic!("{}: missing gRPC cv log\n{}", which.name(), proc.log()));
        assert_eq!(grpc_cv["soft_validation"], serde_json::json!(true));
        assert_eq!(grpc_cv["tenant"], serde_json::json!("tenant-default"));
        assert_eq!(grpc_cv["trace_id"], serde_json::json!(sampled_tid));
    }
}

#[test]
fn w12_create_new_traces_and_register_instrumentation_false_parity() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let client = http_client();
    let dir = tempfile::tempdir().unwrap();

    for which in IMPLS {
        let bin = binary(which);
        let work = dir.path().join(which.name());
        fs::create_dir_all(&work).unwrap();
        let http_port = free_test_port();
        let grpc_port = free_test_port();
        let cfg_path = work.join("mimir.yaml");
        let tsdb_dir = work.join("tsdb");
        let bucket_dir = work.join("bucket");
        fs::create_dir_all(&tsdb_dir).unwrap();
        fs::create_dir_all(&bucket_dir).unwrap();
        let yaml = format!(
            r#"target: ingester
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http_port}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc_port}
  log_level: debug
  log_format: json
  register_instrumentation: false
  create_new_traces: true
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
    dir: {bucket}
  tsdb:
    dir: {tsdb}
usage_stats:
  enabled: false
"#,
            bucket = bucket_dir.display(),
            tsdb = tsdb_dir.display(),
        );
        fs::write(&cfg_path, yaml).unwrap();

        let mut proc = Proc::spawn_with_env(
            &format!("noinstr-{}", which.name()),
            &bin,
            &[format!("-config.file={}", cfg_path.display())],
            &work,
            &[("OTEL_TRACES_EXPORTER", "none")],
        );

        rt.block_on(async {
            wait_ready(&client, &mut proc, http_port, Duration::from_secs(20)).await;

            for p in ["/metrics", "/debug/pprof/", "/debug/pprof/profile", "/debug/pprof/symbol"] {
                let r = client
                    .get(format!("http://127.0.0.1:{http_port}{p}"))
                    .send()
                    .await
                    .unwrap();
                assert_eq!(
                    r.status().as_u16(),
                    404,
                    "{}: {p} must be 404 when register_instrumentation=false",
                    which.name()
                );
            }

            let fg = client
                .get(format!("http://127.0.0.1:{http_port}/debug/fgprof?seconds=1&format=folded"))
                .send()
                .await
                .unwrap();
            assert_eq!(fg.status().as_u16(), 200, "{}: /debug/fgprof", which.name());

            let incoming_tid = "cccccccccccccccccccccccccccccccc";
            let _ = client
                .get(format!("http://127.0.0.1:{http_port}/new-trace-check"))
                .header("traceparent", format!("00-{incoming_tid}-00f067aa0ba902b7-01"))
                .send()
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(100)).await;
        });
        proc.sigterm();
        let _ = proc.wait_exit(Duration::from_secs(5));

        let logs = parse_json_logs(&proc.log());
        let log_line = logs
            .iter()
            .find(|v| {
                v["msg"]
                    .as_str()
                    .is_some_and(|m| m.contains("/new-trace-check (404)"))
            })
            .unwrap_or_else(|| panic!("{}: missing /new-trace-check log\n{}", which.name(), proc.log()));
        let logged_tid = log_line["trace_id"].as_str().unwrap_or("");
        assert!(!logged_tid.is_empty(), "{}: expected new trace_id", which.name());
        assert_ne!(
            logged_tid, "cccccccccccccccccccccccccccccccc",
            "{}: create_new_traces=true must not continue incoming traceparent",
            which.name()
        );
    }
}

#[test]
fn w12_partition_ring_ui_json_and_memberlist_sanity_check_parity() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let client = http_client();
    let dir = tempfile::tempdir().unwrap();

    let broker = SpawnedKfakeBroker::spawn(&KfakeBrokerRequest {
        topic: "ingest-topic".to_string(),
        partitions: 2,
        compression: None,
        records: Vec::new(),
    })
    .expect("spawn kfake");

    for which in IMPLS {
        let bin = binary(which);
        let work = dir.path().join(which.name());
        fs::create_dir_all(&work).unwrap();
        let http_port = free_test_port();
        let grpc_port = free_test_port();
        let tsdb_dir = work.join("tsdb");
        let bucket_dir = work.join("bucket");
        fs::create_dir_all(&tsdb_dir).unwrap();
        fs::create_dir_all(&bucket_dir).unwrap();
        let cfg_path = work.join("ingest.yaml");
        let yaml = format!(
            r#"target: ingester
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http_port}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc_port}
  log_level: info
  log_format: json
ingester:
  push_grpc_method_enabled: false
  ring:
    instance_id: ingester-0
    instance_addr: 127.0.0.1
    kvstore:
      store: inmemory
    num_tokens: 0
    replication_factor: 1
    min_ready_duration: 0s
    final_sleep: 0s
  partition_ring:
    kvstore:
      store: inmemory
ingest_storage:
  enabled: true
  kafka:
    address: {broker_addr}
    topic: ingest-topic
    auto_create_topic_default_partitions: 2
blocks_storage:
  backend: filesystem
  filesystem:
    dir: {bucket}
  tsdb:
    dir: {tsdb}
usage_stats:
  enabled: false
"#,
            broker_addr = broker.ready.broker_addr,
            bucket = bucket_dir.display(),
            tsdb = tsdb_dir.display(),
        );
        fs::write(&cfg_path, yaml).unwrap();

        let mut proc = Proc::spawn(
            &format!("part-{}", which.name()),
            &bin,
            &[format!("-config.file={}", cfg_path.display())],
            &work,
        );

        rt.block_on(async {
            wait_ready(&client, &mut proc, http_port, Duration::from_secs(30)).await;

            let dup = client
                .get(format!(
                    "http://127.0.0.1:{http_port}/ingester/partition-ring?tokens=true&tokens=false"
                ))
                .send()
                .await
                .unwrap();
            assert_eq!(dup.status().as_u16(), 200);

            let p_json = client
                .get(format!("http://127.0.0.1:{http_port}/ingester/partition-ring"))
                .header("Accept", "application/json")
                .send()
                .await
                .unwrap();
            assert_eq!(p_json.status().as_u16(), 200);
            let raw_p_json = p_json.text().await.unwrap();
            assert!(raw_p_json.ends_with('\n'), "{}: trailing newline", which.name());
            let pv: Value = serde_json::from_str(&raw_p_json).unwrap();
            assert!(pv["partitions"].is_array());

            let p_html_resp = client
                .get(format!("http://127.0.0.1:{http_port}/ingester/partition-ring"))
                .send()
                .await
                .unwrap();
            assert_eq!(p_html_resp.status().as_u16(), 200);
            assert_eq!(
                p_html_resp
                    .headers()
                    .get("content-type")
                    .unwrap()
                    .to_str()
                    .unwrap(),
                "text/html"
            );
            let p_html = p_html_resp.text().await.unwrap();
            assert!(
                p_html.contains("value=\"change_state_and_lock\""),
                "{}: missing change_state_and_lock button",
                which.name()
            );
            assert!(
                p_html.contains("& Lock</button>"),
                "{}: missing & Lock label",
                which.name()
            );

            let r_json = client
                .get(format!("http://127.0.0.1:{http_port}/ingester/ring"))
                .header("Accept", "application/json")
                .send()
                .await
                .unwrap()
                .text()
                .await
                .unwrap();
            let rv: Value = serde_json::from_str(&r_json).unwrap();
            assert_eq!(
                rv["shards"][0]["tokens"],
                Value::Null,
                "{}: tokenless tokens must be null",
                which.name()
            );
        });
    }
    drop(broker);

    for which in IMPLS {
        let bin = binary(which);
        let work = dir.path().join(format!("ml-{}", which.name()));
        fs::create_dir_all(&work).unwrap();
        let http_port = free_test_port();
        let grpc_port = free_test_port();
        let ml_port = free_test_port();
        let cfg_path = work.join("ml.yaml");
        let yaml = format!(
            r#"target: memberlist-kv
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http_port}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc_port}
  log_level: info
  log_format: json
memberlist:
  bind_addr: ["127.0.0.1"]
  bind_port: {ml_port}
"#,
        );
        fs::write(&cfg_path, yaml).unwrap();

        let mut proc = Proc::spawn(
            &format!("ml-{}", which.name()),
            &bin,
            &[format!("-config.file={}", cfg_path.display())],
            &work,
        );
        rt.block_on(async {
            wait_ready(&client, &mut proc, http_port, Duration::from_secs(20)).await;
        });
        proc.sigterm();
        let _ = proc.wait_exit(Duration::from_secs(5));
        let logs = parse_json_logs(&proc.log());
        for expected_msg in [
            "Checking directories read/write access",
            "Directories read/write access successfully checked",
            "Checking object storage config",
            "Object storage config successfully checked",
        ] {
            assert!(
                logs.iter().any(|l| l["msg"] == expected_msg),
                "{}: missing memberlist-kv sanity-check log {:?}\n{}",
                which.name(),
                expected_msg,
                proc.log()
            );
        }
    }
}

#[test]
fn w12_usage_stats_registers_all_six_ingester_gauges_at_startup() {
    let tmp = tempfile::tempdir().unwrap();
    let mut cfg = mimir_config::IngesterConfig::default();
    cfg.ring.instance_id = "usage-test-0".to_string();
    let mut blocks_cfg = mimir_config::BlocksStorageConfig::default();
    blocks_cfg.tsdb.dir = tmp.path().to_string_lossy().into_owned();
    let _ing = mimir_ingester::Ingester::new(
        cfg,
        blocks_cfg,
        mimir_config::IngestStorageConfig::default(),
        mimir_config::Overrides::new(mimir_config::LimitsConfig::default()),
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .unwrap();

    let seed = mimir_cmd::usagestats::new_cluster_seed();
    let report = mimir_cmd::usagestats::build_report(
        &seed,
        std::time::SystemTime::now(),
        mimir_cmd::usagestats::DEFAULT_REPORT_SEND_INTERVAL,
    );
    for key in [
        "ingester_inmemory_series",
        "ingester_active_series",
        "ingester_inmemory_tenants",
        "ingester_ooo_enabled_tenants",
        "ingester_ooo_min_window",
        "ingester_ooo_max_window",
        "cpu_usage",
    ] {
        assert!(
            report.metrics.contains_key(key),
            "expected usage-stats report.metrics to contain {key}, got keys: {:?}",
            report.metrics.keys().collect::<Vec<_>>()
        );
    }
}
