//! Wave 11 Config, CLI, Server & httpgrpc Twin-Process Differential Parity Tests.
//!
//! Runs `target/go-mimir` and `target/release/mimir` (or `MIMIR_CONFORMANCE_RUST_BIN`) side by
//! side and diffs their CLI, YAML, HTTP, and httpgrpc/gRPC responses.

use std::fs;
use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use bytes::Bytes;
use mimir_conformance::free_test_port;
use mimir_conformance::oracle::{go_mimir_bin, rust_mimir_release_bin};
use mimir_proto::httpgrpcpb::http_client::HttpClient;
use mimir_proto::httpgrpcpb::{Header as HttpGrpcHeader, HttpRequest};
use serde_yaml::Value;
use tempfile::tempdir;
use tonic::codec::CompressionEncoding;
use tonic::transport::Endpoint;

fn bins() -> (PathBuf, PathBuf) {
    let go = go_mimir_bin();
    let rust = std::env::var_os("MIMIR_CONFORMANCE_RUST_BIN")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(rust_mimir_release_bin);
    assert!(go.exists(), "missing go-mimir at {}", go.display());
    assert!(rust.exists(), "missing rust mimir at {}", rust.display());
    (go, rust)
}

struct ChildProc {
    child: Option<Child>,
    stderr: Arc<Mutex<Vec<u8>>>,
    reader: Option<JoinHandle<()>>,
}

impl ChildProc {
    fn spawn(bin: &Path, args: &[String], dir: &Path) -> Self {
        let mut child = Command::new(bin)
            .args(args)
            .current_dir(dir)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap_or_else(|e| panic!("spawn {}: {e}", bin.display()));
        let stderr = Arc::new(Mutex::new(Vec::new()));
        let reader = child.stderr.take().map(|mut s| {
            let buf = Arc::clone(&stderr);
            std::thread::spawn(move || {
                let mut tmp = [0u8; 4096];
                while let Ok(n) = s.read(&mut tmp) {
                    if n == 0 {
                        break;
                    }
                    if let Ok(mut b) = buf.lock() {
                        b.extend_from_slice(&tmp[..n]);
                    }
                }
            })
        });
        Self {
            child: Some(child),
            stderr,
            reader,
        }
    }

    fn stderr_str(&self) -> String {
        String::from_utf8_lossy(&self.stderr.lock().unwrap()).into_owned()
    }
}

impl Drop for ChildProc {
    fn drop(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
        if let Some(r) = self.reader.take() {
            let _ = r.join();
        }
    }
}

struct PairInstance {
    _tmp: tempfile::TempDir,
    go_http: u16,
    go_grpc: u16,
    rust_http: u16,
    rust_grpc: u16,
    _go_proc: ChildProc,
    _rust_proc: ChildProc,
}

async fn start_pair(extra_yaml: &str, extra_args: &[&str]) -> PairInstance {
    let (go_bin, rust_bin) = bins();
    let tmp = tempdir().unwrap();

    let spawn_one = |name: &str, bin: &Path| -> (u16, u16, ChildProc) {
        let dir = tmp.path().join(name);
        fs::create_dir_all(&dir).unwrap();
        let http_port = free_test_port();
        let grpc_port = free_test_port();
        let cfg_path = dir.join("mimir.yaml");
        let yaml = format!(
            "target: ingester\nserver:\n  http_listen_address: 127.0.0.1\n  http_listen_port: {http_port}\n  grpc_listen_address: 127.0.0.1\n  grpc_listen_port: {grpc_port}\ningester:\n  ring:\n    kvstore:\n      store: inmemory\n    instance_id: {name}-0\n    instance_addr: 127.0.0.1\n    min_ready_duration: 0s\n    replication_factor: 1\nblocks_storage:\n  backend: filesystem\n  filesystem:\n    dir: {blocks}\n  tsdb:\n    dir: {tsdb}\n    ship_interval: 0s\nactivity_tracker:\n  filepath: \"\"\n{extra_yaml}",
            blocks = dir.join("blocks").display(),
            tsdb = dir.join("tsdb").display(),
        );
        fs::write(&cfg_path, yaml).unwrap();
        let mut args = vec![format!("-config.file={}", cfg_path.display())];
        args.extend(extra_args.iter().map(|s| (*s).to_string()));
        let proc = ChildProc::spawn(bin, &args, &dir);
        (http_port, grpc_port, proc)
    };

    let (go_http, go_grpc, go_proc) = spawn_one("go", &go_bin);
    let (rust_http, rust_grpc, rust_proc) = spawn_one("rust", &rust_bin);

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    for (port, label, proc) in [(go_http, "go", &go_proc), (rust_http, "rust", &rust_proc)] {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if let Ok(r) = client.get(format!("http://127.0.0.1:{port}/ready")).send().await {
                if r.status().as_u16() == 200 {
                    break;
                }
            }
            assert!(
                Instant::now() < deadline,
                "{label} did not become ready; stderr:\n{}",
                proc.stderr_str()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    PairInstance {
        _tmp: tmp,
        go_http,
        go_grpc,
        rust_http,
        rust_grpc,
        _go_proc: go_proc,
        _rust_proc: rust_proc,
    }
}

#[tokio::test]
async fn p0_1_runtime_config_and_user_limits_return_404_when_unconfigured() {
    let p = start_pair("", &[]).await;
    let client = reqwest::Client::new();
    for path in ["/runtime_config", "/api/v1/user_limits"] {
        let gr = client
            .get(format!("http://127.0.0.1:{}{path}", p.go_http))
            .send()
            .await
            .unwrap();
        let rr = client
            .get(format!("http://127.0.0.1:{}{path}", p.rust_http))
            .send()
            .await
            .unwrap();
        assert_eq!(gr.status().as_u16(), 404, "go {path}");
        assert_eq!(rr.status().as_u16(), 404, "rust {path}");
        assert_eq!(rr.text().await.unwrap(), gr.text().await.unwrap(), "body {path}");
    }
}

#[test]
fn p0_2_p1_9_p1_11_p2_5_yaml_coercions_common_cli_hidden_sections_and_empty_target() {
    let (go_bin, rust_bin) = bins();
    let tmp = tempdir().unwrap();

    let cfg_path = tmp.path().join("coercion.yaml");
    fs::write(
        &cfg_path,
        r#"
target: ingester
multitenancy_enabled: yes
no_auth_tenant: 12345
blocks_storage:
  storage_prefix: 987
limits:
  max_global_series_per_user: 1e3
  ha_cluster_label: True
ruler_storage:
  backend: s3
"#,
    )
    .unwrap();

    let run_print = |bin: &Path, extra: &[&str]| {
        let mut args = vec![format!("-config.file={}", cfg_path.display()), "-print.config".to_string()];
        args.extend(extra.iter().map(|s| (*s).to_string()));
        Command::new(bin)
            .args(&args)
            .current_dir(tmp.path())
            .output()
            .unwrap()
    };

    let go_out = run_print(&go_bin, &["-common.storage.backend=filesystem", "-ruler.enable-api=True"]);
    let rust_out = run_print(&rust_bin, &["-common.storage.backend=filesystem", "-ruler.enable-api=True"]);
    assert!(go_out.status.success(), "go failed: {}", String::from_utf8_lossy(&go_out.stderr));
    assert!(rust_out.status.success(), "rust failed: {}", String::from_utf8_lossy(&rust_out.stderr));

    let go_yaml: Value = serde_yaml::from_slice(&go_out.stdout).unwrap();
    let rust_yaml: Value = serde_yaml::from_slice(&rust_out.stdout).unwrap();

    assert_eq!(rust_yaml["multitenancy_enabled"], go_yaml["multitenancy_enabled"]);
    assert_eq!(rust_yaml["no_auth_tenant"], Value::String("12345".into()));
    assert_eq!(rust_yaml["no_auth_tenant"], go_yaml["no_auth_tenant"]);
    assert_eq!(rust_yaml["blocks_storage"]["storage_prefix"], go_yaml["blocks_storage"]["storage_prefix"]);
    assert_eq!(rust_yaml["limits"]["max_global_series_per_user"], Value::Number(1000.into()));
    assert_eq!(rust_yaml["limits"]["max_global_series_per_user"], go_yaml["limits"]["max_global_series_per_user"]);
    assert_eq!(rust_yaml["limits"]["ha_cluster_label"], Value::String("True".into()));
    assert_eq!(rust_yaml["limits"]["ha_cluster_label"], go_yaml["limits"]["ha_cluster_label"]);
    assert_eq!(rust_yaml["ruler_storage"]["backend"], Value::String("filesystem".into()));
    assert_eq!(rust_yaml["ruler_storage"]["backend"], go_yaml["ruler_storage"]["backend"]);
    assert_eq!(rust_yaml["ruler"]["enable_api"], Value::Bool(true));
    assert_eq!(rust_yaml["ruler"]["enable_api"], go_yaml["ruler"]["enable_api"]);

    for sec in ["block_builder", "block_builder_scheduler", "usage_tracker"] {
        assert!(!rust_yaml[sec].is_null(), "missing {sec} in rust -print.config");
        assert_eq!(
            rust_yaml[sec].as_mapping().unwrap().keys().collect::<Vec<_>>(),
            go_yaml[sec].as_mapping().unwrap().keys().collect::<Vec<_>>(),
            "{sec} keys"
        );
    }
    assert_eq!(
        rust_yaml["block_builder_scheduler"],
        go_yaml["block_builder_scheduler"]
    );
    assert_eq!(
        rust_yaml["usage_tracker"]["snapshots_storage"]["filesystem"]["dir"],
        go_yaml["usage_tracker"]["snapshots_storage"]["filesystem"]["dir"]
    );

    for flag in ["-print.config", "-modules"] {
        let go_e = Command::new(&go_bin).args(["-target=", flag]).output().unwrap();
        let rs_e = Command::new(&rust_bin).args(["-target=", flag]).output().unwrap();
        assert!(!go_e.status.success(), "go -target= {flag}");
        assert!(!rs_e.status.success(), "rust -target= {flag}");
    }
}

#[tokio::test]
async fn p0_3_p1_1_p1_2_runtime_config_limitsmap_distributor_limits_and_otel_reset() {
    let tmp = tempdir().unwrap();
    let rt_path = tmp.path().join("runtime.yaml");
    fs::write(
        &rt_path,
        r#"
distributor_limits:
  max_ingestion_rate: 5000
overrides:
  12345:
    ha_cluster_label: 777
    max_global_series_per_user: 1e3
    accept_ha_samples: yes
    otel_translation_strategy: ""
    ruler_max_rules_per_rule_group_by_namespace: {}
    alertmanager_notification_rate_limit_per_integration:
      slack: 10
    ruler_alertmanager_client_config:
      oauth2:
        endpoint_params: {}
  otel_tenant:
    otel_translation_strategy: UnderscoreEscapingWithoutSuffixes
"#,
    )
    .unwrap();

    let extra_yaml = format!(
        r#"
runtime_config:
  file: {rt}
distributor:
  instance_limits:
    max_inflight_push_requests: 5000
limits:
  ruler_max_rules_per_rule_group_by_namespace:
    ns1: 50
  alertmanager_notification_rate_limit_per_integration:
    email: 5
    webhook: 9
  otel_metric_suffixes_enabled: true
  otel_translation_strategy: UnderscoreEscapingWithSuffixes
"#,
        rt = rt_path.display()
    );

    let p = start_pair(&extra_yaml, &[]).await;
    let client = reqwest::Client::new();

    for q in ["", "?mode=diff", "?mode=diff&mode=defaults"] {
        let g = client
            .get(format!("http://127.0.0.1:{}/runtime_config{q}", p.go_http))
            .send()
            .await
            .unwrap();
        let r = client
            .get(format!("http://127.0.0.1:{}/runtime_config{q}", p.rust_http))
            .send()
            .await
            .unwrap();
        assert_eq!(g.status().as_u16(), 200, "go /runtime_config{q}");
        assert_eq!(r.status().as_u16(), 200, "rust /runtime_config{q}");
        let g_txt = g.text().await.unwrap();
        let r_txt = r.text().await.unwrap();
        if r_txt != g_txt {
            let first = g_txt.lines().zip(r_txt.lines()).position(|(a, b)| a != b).unwrap_or(0);
            let ctx = |s: &str| s.lines().skip(first.saturating_sub(3)).take(10).collect::<Vec<_>>().join("\n");
            panic!("/runtime_config{q} differs at line {}:\n--- go ---\n{}\n--- rust ---\n{}", first + 1, ctx(&g_txt), ctx(&r_txt));
        }
    }

    let g_cfg = client
        .get(format!("http://127.0.0.1:{}/config?mode=diff&mode=defaults", p.go_http))
        .send()
        .await
        .unwrap();
    let r_cfg = client
        .get(format!("http://127.0.0.1:{}/config?mode=diff&mode=defaults", p.rust_http))
        .send()
        .await
        .unwrap();
    assert_eq!(g_cfg.status().as_u16(), 200);
    assert_eq!(r_cfg.status().as_u16(), 200);
    let r_cfg_full = client
        .get(format!("http://127.0.0.1:{}/config", p.rust_http))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let r_cfg_val: Value = serde_yaml::from_str(&r_cfg_full).unwrap();
    assert!(!r_cfg_val["block_builder"].is_null());
    assert!(!r_cfg_val["block_builder_scheduler"].is_null());
    assert!(!r_cfg_val["usage_tracker"].is_null());

    // Also verify `ruler_alertmanager_client_config.oauth2.endpoint_params: {}` replaces
    // global `endpoint_params` identically to Go (tested with a single tenant so Go's
    // shared-pointer clear bug doesn't leak into a second tenant).
    let rt_ep = tmp.path().join("runtime_ep.yaml");
    fs::write(
        &rt_ep,
        "overrides:\n  t_ep:\n    ruler_alertmanager_client_config:\n      oauth2:\n        endpoint_params: {}\n",
    )
    .unwrap();
    let p_ep = start_pair(
        &format!(
            "runtime_config:\n  file: {}\nlimits:\n  ruler_alertmanager_client_config:\n    oauth2:\n      endpoint_params:\n        global_key: global_val\n",
            rt_ep.display()
        ),
        &[],
    )
    .await;
    let g_ep = client
        .get(format!("http://127.0.0.1:{}/runtime_config", p_ep.go_http))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let r_ep = client
        .get(format!("http://127.0.0.1:{}/runtime_config", p_ep.rust_http))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert_eq!(r_ep, g_ep, "endpoint_params replacement parity");
}

#[tokio::test]
async fn p1_3_user_limits_federation_422_multi_header_and_noauth_metadata() {
    let tmp = tempdir().unwrap();
    let rt_path = tmp.path().join("runtime.yaml");
    fs::write(&rt_path, "overrides: {}\n").unwrap();

    let extra = format!("multitenancy_enabled: true\nruntime_config:\n  file: {}\n", rt_path.display());
    let p = start_pair(&extra, &[]).await;

    let client = reqwest::Client::new();
    for (hdrs, want_status) in [
        (vec![("X-Scope-OrgID", "orgA|orgB")], 422u16),
        (vec![("X-Scope-OrgID", "orgA"), ("X-Scope-OrgID", "orgB")], 200),
        (vec![("X-Scope-OrgID", "bad/tenant")], 401),
    ] {
        let call = |port: u16| {
            let mut req = client.get(format!("http://127.0.0.1:{port}/api/v1/user_limits"));
            for (k, v) in &hdrs {
                req = req.header(*k, *v);
            }
            async move {
                let r = req.send().await.unwrap();
                let st = r.status().as_u16();
                let body = r.text().await.unwrap();
                (st, body)
            }
        };
        let (gs, gb) = call(p.go_http).await;
        let (rs, rb) = call(p.rust_http).await;
        assert_eq!(gs, want_status, "go status hdrs={hdrs:?} body={gb:?}");
        assert_eq!(rs, gs, "rust vs go status hdrs={hdrs:?} body={rb:?} vs {gb:?}");
        if want_status != 200 {
            assert_eq!(rb, gb, "error body hdrs={hdrs:?}");
        }
    }

    let rt_noauth = tmp.path().join("rt_noauth.yaml");
    fs::write(
        &rt_noauth,
        "overrides:\n  anonymous:\n    max_global_series_per_user: 7777\n",
    )
    .unwrap();
    let p2 = start_pair(
        &format!(
            "multitenancy_enabled: false\nno_auth_tenant: \"anonymous:k=v\"\nruntime_config:\n  file: {}\n",
            rt_noauth.display()
        ),
        &[],
    ).await;
    let g2: serde_json::Value = client
        .get(format!("http://127.0.0.1:{}/api/v1/user_limits", p2.go_http))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let r2: serde_json::Value = client
        .get(format!("http://127.0.0.1:{}/api/v1/user_limits", p2.rust_http))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(g2["max_global_series_per_user"], 7777);
    let go_obj = g2.as_object().unwrap();
    let rs_obj = r2.as_object().unwrap();
    assert_eq!(rs_obj.keys().collect::<Vec<_>>(), go_obj.keys().collect::<Vec<_>>());
    for (k, gv) in go_obj {
        assert_eq!(rs_obj[k].as_f64(), gv.as_f64(), "user_limits field {k}");
    }
}

#[tokio::test]
async fn p1_6_p1_7_p1_8_p2_2_httpgrpc_urls_headers_compression_and_cost_attribution() {
    let p = start_pair("", &["-cost-attribution.registry-path=/usage-metrics"]).await;

    let client = reqwest::Client::builder()
        .no_gzip()
        .build()
        .unwrap();
    let mut ca_results = Vec::new();
    for port in [p.go_http, p.rust_http] {
        let post_resp = client
            .post(format!("http://127.0.0.1:{port}/usage-metrics"))
            .send()
            .await
            .unwrap();
        assert_eq!(post_resp.status().as_u16(), 405, "port={port}");

        let get_resp = client
            .get(format!("http://127.0.0.1:{port}/usage-metrics"))
            .header(
                "Accept",
                "application/openmetrics-text;version=1.0.0,text/plain;version=0.0.4;q=0.5",
            )
            .header("Accept-Encoding", "gzip")
            .send()
            .await
            .unwrap();
        assert_eq!(get_resp.status().as_u16(), 200, "port={port}");
        let ct = get_resp
            .headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        let ce = get_resp
            .headers()
            .get("content-encoding")
            .map(|v| v.to_str().unwrap().to_string());
        assert!(ct.starts_with("text/plain;"), "expected text/plain on cost attribution, got {ct}");
        ca_results.push((ct, ce));
    }
    assert_eq!(ca_results[1], ca_results[0], "cost attribution content-type/encoding parity");

    let go_ch = Endpoint::from_shared(format!("http://127.0.0.1:{}", p.go_grpc))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let rs_ch = Endpoint::from_shared(format!("http://127.0.0.1:{}", p.rust_grpc))
        .unwrap()
        .connect()
        .await
        .unwrap();

    let mut gc = HttpClient::new(go_ch.clone())
        .send_compressed(CompressionEncoding::Gzip)
        .accept_compressed(CompressionEncoding::Gzip);
    let mut rc = HttpClient::new(rs_ch.clone())
        .send_compressed(CompressionEncoding::Gzip)
        .accept_compressed(CompressionEncoding::Gzip);

    let req = || HttpRequest {
        method: "GET".to_string(),
        url: r#"/config?match[]={job="foo"}&mode=diff"#.to_string(),
        headers: vec![
            HttpGrpcHeader {
                key: "Accept".to_string(),
                values: vec!["text/plain".to_string()],
            },
            HttpGrpcHeader {
                key: "accept".to_string(),
                values: vec!["application/yaml".to_string()],
            },
        ],
        body: Bytes::new(),
    };

    let with_org = |r: HttpRequest| {
        let mut tr = tonic::Request::new(r);
        tr.metadata_mut().insert("x-scope-orgid", "tenant-1".parse().unwrap());
        tr
    };
    let g_resp = gc.handle(with_org(req())).await.unwrap().into_inner();
    let r_resp = rc.handle(with_org(req())).await.unwrap().into_inner();
    assert_eq!(g_resp.code, 200);
    assert_eq!(r_resp.code, 200);

    let bad_req = || HttpRequest {
        method: "GET".to_string(),
        url: "://bad".to_string(),
        headers: Vec::new(),
        body: Bytes::new(),
    };
    let g_err = gc.handle(with_org(bad_req())).await.unwrap_err();
    let r_err = rc.handle(with_org(bad_req())).await.unwrap_err();
    assert_eq!(g_err.code(), tonic::Code::Unknown);
    assert_eq!(r_err.code(), tonic::Code::Unknown);

    let rel_req = || HttpRequest {
        method: "GET".to_string(),
        url: "ready".to_string(),
        headers: Vec::new(),
        body: Bytes::new(),
    };
    let g_rel = gc.handle(with_org(rel_req())).await.unwrap().into_inner();
    let r_rel = rc.handle(with_org(rel_req())).await.unwrap().into_inner();
    assert_eq!(g_rel.code, 301);
    assert_eq!(r_rel.code, g_rel.code);
    assert_eq!(r_rel.headers, g_rel.headers);

    let mut raw_client = HttpClient::new(rs_ch)
        .send_compressed(CompressionEncoding::Zstd)
        .accept_compressed(CompressionEncoding::Zstd);
    let zstd_resp = raw_client
        .handle(with_org(HttpRequest {
            method: "GET".to_string(),
            url: "/ready".to_string(),
            headers: Vec::new(),
            body: Bytes::new(),
        }))
        .await
        .unwrap();
    assert_eq!(
        zstd_resp.metadata().get("grpc-encoding").map(|v| v.to_str().unwrap()),
        Some("zstd")
    );
    assert_eq!(zstd_resp.into_inner().code, 200);
}
