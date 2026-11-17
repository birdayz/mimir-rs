//! Wave 3 twin-process differential conformance for Consul KV, multitenant empty org ID,
//! partition-ring JSON local timezone formatting, and pre-instantiated gRPC message byte metrics:
//!
//! - W3-06: Consul KV URL path percent-encoding (`collectors#1/` prefix never truncated at `#`),
//!   reverse-proxy subpath, initial `WatchKey` omitting `index=0`, `?recurse=` on `List`,
//!   `CONSUL_*` environment variables (`CONSUL_HTTP_ADDR`, `CONSUL_HTTP_TOKEN`,
//!   `CONSUL_HTTP_TOKEN_FILE`, `CONSUL_HTTP_AUTH`, `CONSUL_NAMESPACE`, `CONSUL_PARTITION`,
//!   `CONSUL_HTTP_SSL` + `CONSUL_CACERT` + `CONSUL_CLIENT_CERT`/`CONSUL_CLIENT_KEY` mTLS), and
//!   startup rejection (`exit 1`) of unknown URL schemes (`ftp://`).
//! - W3-07: `cortex_consul_request_duration_seconds{kv_name,operation,status_code}` recorded across
//!   `Get`, `CAS`, `CAS loop`, `List`, and `Delete`.
//! - W3-11: Empty `x-scope-orgid: ""` header accepted on gRPC (`Push` + Read RPCs, safely bounded
//!   inside `tsdb.dir`) while rejected with `401 "no org id\n"` on HTTP and `httpgrpc`.
//! - W3-12: `GET /ingester/partition-ring` (`Accept: application/json`) serializes
//!   `state_timestamp` in the process's local timezone (`TZ=Asia/Tokyo` -> `+09:00`, `TZ=UTC` ->
//!   `Z`), matching Go `time.Unix(p.StateTimestamp, 0).MarshalJSON()`.
//! - W3-16: `cortex_response_message_bytes{method="gRPC",route=...}` is pre-instantiated at stream
//!   start (`TagRPC`) so `count=0` is exposed when an RPC is canceled before sending a frame.

use std::collections::{BTreeSet, HashMap};
use std::net::SocketAddr;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::Bytes;
use axum::extract::{Request, State};
use axum::http::{header::AUTHORIZATION, HeaderMap as AxumHeaders, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Router;
use base64::Engine as _;
use mimir_cmd::server::{RunningServers, Server as DskitServer};
use mimir_common::logging::{LogLevel, Logger};
use mimir_config::ingester::ConsulConfig;
use mimir_config::server::{ServerConfig, TLSConfig as ServerTlsConfig};
use mimir_conformance::free_test_port;
use mimir_conformance::oracle::{KfakeBrokerRequest, SpawnedKfakeBroker, workspace_root};
use mimir_conformance::twin::{
    binary, http_get, ingester_ring, start_mimir, start_mimir_with_env, wait_ready, Impl,
};
use mimir_proto::cortexpb;
use mimir_proto::httpgrpcpb::http_client::HttpClient as HttpGrpcClient;
use mimir_proto::httpgrpcpb::{Header as HttpGrpcHeader, HttpRequest as HttpGrpcRequest};
use mimir_proto::ingesterpb;
use mimir_proto::ingesterpb::ingester_client::IngesterClient;
use mimir_ring::kv::consul::ConsulKv;
use mimir_ring::kv::{registerer_with_kv_name, KvClient, Registerer, StringCodec};
use parking_lot::Mutex;
use prometheus::Registry;
use serde::Serialize;
use serde_json::Value;
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;

const IMPLS: [Impl; 2] = [Impl::Go, Impl::Rust];

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn candidate_is_go() -> bool {
    let bin = binary(Impl::Rust);
    let out = Command::new(&bin)
        .arg("-version")
        .output()
        .expect("run -version");
    String::from_utf8_lossy(&out.stdout).contains("go version:       go")
}

fn tls_fixture(name: &str) -> String {
    workspace_root()
        .join("crates/mimir-storage/tests/fixtures/kafka_tls")
        .join(name)
        .to_string_lossy()
        .into_owned()
}

fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap()
}

// ---------------------------------------------------------------------------
// In-process HTTP / HTTPS / mTLS Mock Consul KV Server (bound via free_test_port)
// ---------------------------------------------------------------------------

#[derive(Clone, Serialize)]
#[serde(rename_all = "PascalCase")]
struct ConsulPairJson {
    key: String,
    value: String,
    create_index: u64,
    modify_index: u64,
}

#[derive(Clone)]
struct ConsulEntry {
    key: String,
    value: Vec<u8>,
    create_index: u64,
    modify_index: u64,
}

struct MockConsulInner {
    index: u64,
    store: HashMap<String, ConsulEntry>,
    required_token: Option<String>,
    required_basic_auth: Option<String>,
    fail_next_cas: bool,
    recorded_reqs: Vec<String>,
}

#[derive(Clone)]
struct MockConsul {
    inner: Arc<Mutex<MockConsulInner>>,
    notify: Arc<Notify>,
}

impl MockConsul {
    fn new(required_token: Option<&str>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(MockConsulInner {
                index: 50,
                store: HashMap::new(),
                required_token: required_token.map(str::to_string),
                required_basic_auth: None,
                fail_next_cas: false,
                recorded_reqs: Vec::new(),
            })),
            notify: Arc::new(Notify::new()),
        }
    }

    fn set_fail_next_cas(&self, v: bool) {
        self.inner.lock().fail_next_cas = v;
    }

    fn set_required_basic_auth(&self, user_pass: &str) {
        let b64 = base64::engine::general_purpose::STANDARD.encode(user_pass);
        self.inner.lock().required_basic_auth = Some(format!("Basic {b64}"));
    }

    fn reset_index_backward(&self) {
        self.inner.lock().index = 1;
        self.notify.notify_waiters();
    }

    fn recorded_reqs(&self) -> Vec<String> {
        self.inner.lock().recorded_reqs.clone()
    }
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(v) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                out.push(v);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn parse_query(q: &str) -> HashMap<String, String> {
    let mut out = HashMap::new();
    if q.is_empty() {
        return out;
    }
    for part in q.split('&') {
        match part.split_once('=') {
            Some((k, v)) => {
                out.insert(percent_decode(k), percent_decode(v));
            }
            None => {
                out.insert(percent_decode(part), String::new());
            }
        }
    }
    out
}

async fn handle_consul(
    State(mc): State<MockConsul>,
    headers: AxumHeaders,
    req: Request,
) -> Response {
    let method = req.method().clone();
    let raw_path = req.uri().path().to_string();
    let raw_query = req.uri().query().unwrap_or("").to_string();
    let tok = headers
        .get("x-consul-token")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let auth = headers
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();

    {
        let mut g = mc.inner.lock();
        g.recorded_reqs
            .push(format!("{method} {raw_path}?{raw_query}"));
        if let Some(expected_tok) = &g.required_token {
            if &tok != expected_tok {
                return (StatusCode::FORBIDDEN, "Permission denied").into_response();
            }
        }
        if let Some(expected_auth) = &g.required_basic_auth {
            if &auth != expected_auth {
                return (StatusCode::UNAUTHORIZED, "Unauthorized").into_response();
            }
        }
    }

    let decoded_path = percent_decode(&raw_path);
    let Some(key) = decoded_path
        .strip_prefix("/v1/kv/")
        .or_else(|| decoded_path.strip_prefix("/consul-proxy/v1/kv/"))
    else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let key = key.to_string();
    let q = parse_query(&raw_query);

    match method {
        Method::GET => {
            let wait_idx = q
                .get("index")
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);
            if wait_idx > 0 {
                let deadline = tokio::time::Instant::now() + Duration::from_millis(200);
                loop {
                    let notified = mc.notify.notified();
                    let cur = mc.inner.lock().index;
                    if cur != wait_idx || tokio::time::Instant::now() >= deadline {
                        break;
                    }
                    if tokio::time::timeout_at(deadline, notified).await.is_err() {
                        break;
                    }
                }
            }

            if q.contains_key("recurse") {
                let (items, cur_idx) = {
                    let g = mc.inner.lock();
                    let mut list: Vec<ConsulPairJson> = g
                        .store
                        .values()
                        .filter(|e| e.key.starts_with(&key))
                        .map(|e| ConsulPairJson {
                            key: e.key.clone(),
                            value: base64::engine::general_purpose::STANDARD.encode(&e.value),
                            create_index: e.create_index,
                            modify_index: e.modify_index,
                        })
                        .collect();
                    list.sort_by(|a, b| a.key.cmp(&b.key));
                    (list, g.index)
                };
                let idx_hdr = cur_idx.to_string();
                if items.is_empty() {
                    return (StatusCode::NOT_FOUND, [("X-Consul-Index", idx_hdr)], "")
                        .into_response();
                }
                return (
                    StatusCode::OK,
                    [
                        ("X-Consul-Index", idx_hdr),
                        ("Content-Type", "application/json".to_string()),
                    ],
                    serde_json::to_string(&items).unwrap(),
                )
                    .into_response();
            }

            let (maybe_ent, cur_idx) = {
                let g = mc.inner.lock();
                (g.store.get(&key).cloned(), g.index)
            };
            let idx_hdr = cur_idx.to_string();
            match maybe_ent {
                None => (StatusCode::NOT_FOUND, [("X-Consul-Index", idx_hdr)], "").into_response(),
                Some(ent) => {
                    let body = serde_json::to_string(&[ConsulPairJson {
                        key: ent.key,
                        value: base64::engine::general_purpose::STANDARD.encode(&ent.value),
                        create_index: ent.create_index,
                        modify_index: ent.modify_index,
                    }])
                    .unwrap();
                    (
                        StatusCode::OK,
                        [
                            ("X-Consul-Index", idx_hdr),
                            ("Content-Type", "application/json".to_string()),
                        ],
                        body,
                    )
                        .into_response()
                }
            }
        }
        Method::PUT => {
            let body: Bytes = axum::body::to_bytes(req.into_body(), usize::MAX)
                .await
                .unwrap();
            let mut g = mc.inner.lock();
            if let Some(cas_str) = q.get("cas") {
                let cas_idx = cas_str.parse::<u64>().unwrap_or(0);
                if g.fail_next_cas {
                    g.fail_next_cas = false;
                    return (StatusCode::OK, "false").into_response();
                }
                let conflict = match g.store.get(&key) {
                    None => cas_idx != 0,
                    Some(ent) => ent.modify_index != cas_idx,
                };
                if conflict {
                    return (StatusCode::OK, "false").into_response();
                }
            }
            g.index += 1;
            let idx = g.index;
            match g.store.get_mut(&key) {
                Some(ent) => {
                    ent.value = body.to_vec();
                    ent.modify_index = idx;
                }
                None => {
                    g.store.insert(
                        key.clone(),
                        ConsulEntry {
                            key,
                            value: body.to_vec(),
                            create_index: idx,
                            modify_index: idx,
                        },
                    );
                }
            }
            drop(g);
            mc.notify.notify_waiters();
            (StatusCode::OK, "true").into_response()
        }
        Method::DELETE => {
            let mut g = mc.inner.lock();
            g.store.remove(&key);
            g.index += 1;
            drop(g);
            mc.notify.notify_waiters();
            (StatusCode::OK, "true").into_response()
        }
        _ => StatusCode::METHOD_NOT_ALLOWED.into_response(),
    }
}

async fn start_http_consul(mc: MockConsul) -> (u16, tokio::task::JoinHandle<()>) {
    let port = free_test_port();
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    let listener = TcpListener::bind(addr).await.unwrap();
    let app = Router::new().fallback(handle_consul).with_state(mc);
    let handle = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (port, handle)
}

struct NoopLogger;
impl Logger for NoopLogger {
    fn log(&self, _level: LogLevel, _fields: &[(&str, &str)]) {}
}

fn start_https_consul(
    mc: MockConsul,
    cert_path: &str,
    key_path: &str,
    client_ca_path: Option<&str>,
) -> (u16, RunningServers) {
    let tls_port = free_test_port();
    let dummy_grpc_port = free_test_port();
    let http_tls = match client_ca_path {
        Some(ca) => ServerTlsConfig {
            cert_file: cert_path.to_string(),
            key_file: key_path.to_string(),
            client_auth_type: "RequireAndVerifyClientCert".to_string(),
            client_ca_file: ca.to_string(),
            ..Default::default()
        },
        None => ServerTlsConfig {
            cert_file: cert_path.to_string(),
            key_file: key_path.to_string(),
            ..Default::default()
        },
    };
    let srv_cfg = ServerConfig {
        http_listen_address: "127.0.0.1".to_string(),
        http_listen_port: tls_port as i64,
        grpc_listen_address: "127.0.0.1".to_string(),
        grpc_listen_port: dummy_grpc_port as i64,
        http_tls_config: http_tls,
        ..Default::default()
    };
    let srv = DskitServer::new(&srv_cfg, &Registry::new(), Arc::new(NoopLogger)).unwrap();
    let app = Router::new().fallback(handle_consul).with_state(mc);
    let running = srv
        .start(app, tonic::service::Routes::default())
        .unwrap();
    (tls_port, running)
}

fn consul_ingester_yaml(
    dir: &Path,
    http_port: u16,
    grpc_port: u16,
    consul_host: &str,
    prefix: &str,
) -> String {
    let blocks = dir.join("blocks");
    let tsdb = dir.join("tsdb");
    std::fs::create_dir_all(&blocks).unwrap();
    std::fs::create_dir_all(&tsdb).unwrap();
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
    instance_id: ingester-0
    instance_addr: 127.0.0.1
    kvstore:
      store: consul
      prefix: "{prefix}"
      consul:
        host: "{consul_host}"
        consistent_reads: true
        cas_retry_delay: 25ms
        watch_rate_limit: 20
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
"#,
        blocks.display(),
        tsdb.display()
    )
}

fn scrape_consul_metric_ops(metrics_body: &str) -> HashMap<(String, String, String), u64> {
    let mut out = HashMap::new();
    for line in metrics_body.lines() {
        let Some(rest) = line.strip_prefix("cortex_consul_request_duration_seconds_count{") else {
            continue;
        };
        let Some((labels_str, val_str)) = rest.split_once("} ") else {
            continue;
        };
        let count: u64 = val_str.trim().parse().unwrap_or(0);
        let mut kv_name = String::new();
        let mut op = String::new();
        let mut code = String::new();
        for part in labels_str.split(',') {
            if let Some((k, v)) = part.split_once('=') {
                let v = v.trim_matches('"');
                match k {
                    "kv_name" => kv_name = v.to_string(),
                    "operation" => op = v.to_string(),
                    "status_code" => code = v.to_string(),
                    _ => {}
                }
            }
        }
        out.insert((kv_name, op, code), count);
    }
    out
}

// ---------------------------------------------------------------------------
// Test 1: W3-06 + W3-07 (Consul URL-special prefix, WatchKey/CAS/List/Delete,
// CONSUL_* env + TLS/mTLS, invalid scheme exit 1, and request duration metrics)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn w3_06_07_consul_url_encoding_env_tls_scheme_and_metrics_parity() {
    let tmp = tempfile::tempdir().unwrap();
    let client = http_client();

    // --- Part A: URL-special prefix 'collectors#1/' + reverse-proxy subpath + CONSUL_* env + CAS retry + WatchKey + clean unregister + W3-07 metrics ---
    let mut first_put_reqs = Vec::new();
    let mut first_watch_reqs = Vec::new();
    let mut put_counts_after_stop = Vec::new();
    let mut metric_keys_per_impl = Vec::new();

    for which in IMPLS {
        let mc = MockConsul::new(Some("env-secret-tok-77"));
        mc.set_fail_next_cas(true); // force 1 CAS retry so CAS count > CAS loop count!
        let (consul_port, consul_task) = start_http_consul(mc.clone()).await;

        let http_port = free_test_port();
        let grpc_port = free_test_port();
        let dir = tmp.path().join(format!("consul-special-{}", which.name()));
        let consul_host = format!("http://127.0.0.1:{consul_port}/consul-proxy");
        let yaml = consul_ingester_yaml(&dir, http_port, grpc_port, &consul_host, "collectors#1/");

        let mut proc = start_mimir_with_env(
            which,
            "consul-special",
            &dir,
            &yaml,
            &[],
            &[
                ("CONSUL_HTTP_TOKEN", "env-secret-tok-77"),
                ("CONSUL_NAMESPACE", "ns-prod"),
                ("CONSUL_PARTITION", "part-a"),
            ],
        );
        wait_ready(&client, &mut proc, http_port, Duration::from_secs(20)).await;

        let ring = ingester_ring(&client, http_port).await.unwrap();
        assert_eq!(
            ring.get("ingester-0").map(|(s, _)| s.as_str()),
            Some("ACTIVE"),
            "{}: expected ACTIVE in ring, got {ring:?}",
            which.name()
        );

        // Trigger backward Consul index reset (52 -> 1) and verify watcher recovers cleanly.
        mc.reset_index_backward();
        tokio::time::sleep(Duration::from_millis(350)).await;
        let (ready_code, _, _) = http_get(&client, http_port, "/ready", false).await.unwrap();
        assert_eq!(ready_code, 200);

        let (m_code, _, metrics_body) = http_get(&client, http_port, "/metrics", false)
            .await
            .unwrap();
        assert_eq!(m_code, 200);
        let ops = scrape_consul_metric_ops(&metrics_body);

        // Lifecycler did 2 CAS loops (PENDING/JOINING + ACTIVE) with 1 forced CAS conflict => 3 CAS, >= 3 Gets.
        assert_eq!(
            ops.get(&(
                "ingester-lifecycler".into(),
                "CAS loop".into(),
                "200".into()
            ))
            .copied(),
            Some(2),
            "{}: unexpected CAS loop count in {ops:?}",
            which.name()
        );
        assert_eq!(
            ops.get(&("ingester-lifecycler".into(), "CAS".into(), "200".into()))
                .copied(),
            Some(3),
            "{}: unexpected CAS count in {ops:?}",
            which.name()
        );
        assert!(
            ops.get(&("ingester-lifecycler".into(), "Get".into(), "200".into()))
                .copied()
                .unwrap_or(0)
                >= 3,
            "{}: expected lifecycler Get >= 3 in {ops:?}",
            which.name()
        );
        assert!(
            ops.get(&("ingester-ring".into(), "Get".into(), "200".into()))
                .copied()
                .unwrap_or(0)
                >= 2,
            "{}: expected ring watcher Get >= 2 in {ops:?}",
            which.name()
        );
        let key_set: BTreeSet<(String, String, String)> = ops.keys().cloned().collect();
        metric_keys_per_impl.push(key_set);

        // Clean SIGTERM unregisters the sole instance -> exercises Consul Delete!
        proc.sigterm();
        let exit = proc
            .wait_exit(Duration::from_secs(15))
            .unwrap_or_else(|| panic!("{} did not exit on SIGTERM", which.name()));
        assert!(
            exit.success(),
            "{}: expected clean exit 0 on SIGTERM, got {exit}\n{}",
            which.name(),
            proc.log()
        );

        let reqs = mc.recorded_reqs();
        let first_put = reqs
            .iter()
            .find(|r| r.starts_with("PUT "))
            .cloned()
            .expect("expected PUT request");
        let first_watch = reqs
            .iter()
            .find(|r| r.starts_with("GET ") && r.contains("wait="))
            .cloned()
            .expect("expected blocking WatchKey GET");
        let saw_subsequent_index = reqs
            .iter()
            .any(|r| r.starts_with("GET ") && r.contains("wait=") && r.contains("index="));
        assert!(
            saw_subsequent_index,
            "{}: expected subsequent WatchKey GET to include index=, got {reqs:?}",
            which.name()
        );
        let put_count = reqs.iter().filter(|r| r.starts_with("PUT ")).count();
        assert!(
            put_count >= 4,
            "{}: expected unregister CAS PUT on SIGTERM, got {put_count} PUTs in {reqs:?}",
            which.name()
        );

        first_put_reqs.push(first_put);
        first_watch_reqs.push(first_watch);
        put_counts_after_stop.push(put_count);
        consul_task.abort();
    }

    // Exact Go-vs-Rust wire URL & metric-key parity!
    assert_eq!(
        first_put_reqs[0],
        "PUT /consul-proxy/v1/kv/collectors%231/ring?cas=0&ns=ns-prod&partition=part-a"
    );
    assert_eq!(first_put_reqs[0], first_put_reqs[1]);
    assert_eq!(
        first_watch_reqs[0],
        "GET /consul-proxy/v1/kv/collectors%231/ring?consistent=&ns=ns-prod&partition=part-a&wait=10000ms"
    );
    assert_eq!(first_watch_reqs[0], first_watch_reqs[1]);
    assert_eq!(put_counts_after_stop[0], put_counts_after_stop[1]);
    assert_eq!(metric_keys_per_impl[0], metric_keys_per_impl[1]);

    // Also verify List (?recurse=) and Delete metrics via ConsulKv directly against mock Consul.
    {
        let mc_list = MockConsul::new(None);
        let (list_port, list_task) = start_http_consul(mc_list.clone()).await;
        let reg = Registry::new();
        let cortex = Registerer::new(&reg).wrap_with_prefix("cortex_");
        let kv_reg = registerer_with_kv_name(Some(&cortex), "test-kv");
        let kv = ConsulKv::with_registerer(
            ConsulConfig {
                host: format!("127.0.0.1:{list_port}"),
                ..Default::default()
            },
            Arc::new(StringCodec),
            kv_reg.as_ref(),
        )
        .unwrap();
        kv.put("collectors#1/a", &"1".to_string()).await.unwrap();
        let keys = kv.list("collectors#1/").await.unwrap();
        assert_eq!(keys, vec!["collectors#1/a".to_string()]);
        kv.delete("collectors#1/a").await.unwrap();
        assert!(mc_list
            .recorded_reqs()
            .iter()
            .any(|r| r == "GET /v1/kv/collectors%231/?recurse=&stale="));
        let encoder = prometheus::TextEncoder::new();
        let text = encoder.encode_to_string(&reg.gather()).unwrap();
        let ops = scrape_consul_metric_ops(&text);
        assert_eq!(
            ops.get(&("test-kv".into(), "List".into(), "200".into())),
            Some(&1)
        );
        assert_eq!(
            ops.get(&("test-kv".into(), "Delete".into(), "200".into())),
            Some(&1)
        );
        list_task.abort();
    }

    // --- Part B: Invalid URL scheme ('ftp://127.0.0.1:8500') fails startup with exit 1 on both ---
    for which in IMPLS {
        let dir = tmp.path().join(format!("consul-bad-scheme-{}", which.name()));
        let yaml = consul_ingester_yaml(
            &dir,
            free_test_port(),
            free_test_port(),
            "ftp://127.0.0.1:8500",
            "collectors/",
        );
        let mut proc = start_mimir(which, "bad-scheme", &dir, &yaml, &[]);
        let exit = proc
            .wait_exit(Duration::from_secs(5))
            .unwrap_or_else(|| panic!("{} stayed running with ftp:// consul host!", which.name()));
        assert_eq!(
            exit.code(),
            Some(1),
            "{}: expected exit 1 on ftp:// scheme, got {exit:?}\n{}",
            which.name(),
            proc.log()
        );
        assert!(
            proc.log().contains("Unknown protocol scheme: ftp"),
            "{}: expected 'Unknown protocol scheme: ftp' in log:\n{}",
            which.name(),
            proc.log()
        );
    }

    // --- Part C: HTTPS Consul (Go vs Rust with https:// + SSL_CERT_FILE) + mTLS subprocess via CONSUL_HTTP_SSL + CONSUL_CACERT + CONSUL_CLIENT_CERT/KEY ---
    let ca_crt = tls_fixture("ca.crt");
    let srv_crt = tls_fixture("server.crt");
    let srv_key = tls_fixture("server.key");
    let cli_crt = tls_fixture("client.crt");
    let cli_key = tls_fixture("client.key");

    for which in IMPLS {
        let mc_tls = MockConsul::new(None);
        let (tls_port, tls_srv) = start_https_consul(mc_tls, &srv_crt, &srv_key, None);
        let hp = free_test_port();
        let gp = free_test_port();
        let dir = tmp.path().join(format!("consul-https-{}", which.name()));
        let yaml = consul_ingester_yaml(
            &dir,
            hp,
            gp,
            &format!("https://127.0.0.1:{tls_port}"),
            "collectors/",
        );
        let mut proc = start_mimir_with_env(
            which,
            "consul-https",
            &dir,
            &yaml,
            &[],
            &[("SSL_CERT_FILE", &ca_crt)],
        );
        wait_ready(&client, &mut proc, hp, Duration::from_secs(20)).await;
        drop(proc);
        tls_srv.shutdown().await;
    }

    // Full mTLS + CONSUL_HTTP_ADDR + CONSUL_HTTP_SSL + CONSUL_CACERT + CONSUL_CLIENT_CERT/KEY +
    // CONSUL_HTTP_TOKEN_FILE + CONSUL_HTTP_AUTH on the Rust binary subprocess.
    // (Skipped only in Go-vs-Go harness validation mode because upstream Go dskit/kv/consul passes
    // a non-nil HttpClient + Scheme="http" to hashicorp/consul/api.NewClient, bypassing TLS env vars.)
    if !candidate_is_go() {
        let tok_file = tmp.path().join("mtls.token");
        std::fs::write(&tok_file, "  mtls-tok-42 \n").unwrap();

        let mc_mtls = MockConsul::new(Some("mtls-tok-42"));
        mc_mtls.set_required_basic_auth("ops:p4ss");
        let (mtls_port, mtls_srv) =
            start_https_consul(mc_mtls, &srv_crt, &srv_key, Some(&ca_crt));

        let hp = free_test_port();
        let gp = free_test_port();
        let dir = tmp.path().join("consul-mtls-rust");
        // Empty consul.host exercises CONSUL_HTTP_ADDR fallback!
        let yaml = consul_ingester_yaml(&dir, hp, gp, "", "collectors#mtls/");
        let addr_env = format!("127.0.0.1:{mtls_port}");
        let tok_file_str = tok_file.to_string_lossy().into_owned();
        let mut proc = start_mimir_with_env(
            Impl::Rust,
            "consul-mtls",
            &dir,
            &yaml,
            &[],
            &[
                ("CONSUL_HTTP_ADDR", &addr_env),
                ("CONSUL_HTTP_SSL", "true"),
                ("CONSUL_CACERT", &ca_crt),
                ("CONSUL_CLIENT_CERT", &cli_crt),
                ("CONSUL_CLIENT_KEY", &cli_key),
                ("CONSUL_HTTP_TOKEN_FILE", &tok_file_str),
                ("CONSUL_HTTP_AUTH", "ops:p4ss"),
            ],
        );
        wait_ready(&client, &mut proc, hp, Duration::from_secs(20)).await;
        let ring = ingester_ring(&client, hp).await.unwrap();
        assert_eq!(
            ring.get("ingester-0").map(|(s, _)| s.as_str()),
            Some("ACTIVE")
        );
        drop(proc);
        mtls_srv.shutdown().await;
    }
}

// ---------------------------------------------------------------------------
// Test 2: W3-11 (Empty x-scope-orgid: "" on gRPC vs HTTP & httpgrpc + UserTsdb path safety)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn w3_11_empty_org_id_grpc_http_and_httpgrpc_parity() {
    let tmp = tempfile::tempdir().unwrap();
    let http = http_client();
    let ts = now_ms();

    let mut procs = Vec::new();
    let mut http_ports = Vec::new();
    let mut grpc_ports = Vec::new();
    let mut tsdb_dirs = Vec::new();

    for which in IMPLS {
        let hp = free_test_port();
        let gp = free_test_port();
        let dir = tmp.path().join(format!("empty-org-{}", which.name()));
        let blocks = dir.join("blocks");
        let tsdb = dir.join("tsdb");
        std::fs::create_dir_all(&blocks).unwrap();
        std::fs::create_dir_all(&tsdb).unwrap();
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
    instance_id: ingester-0
    instance_addr: 127.0.0.1
    kvstore:
      store: inmemory
    replication_factor: 1
    min_ready_duration: 0s
blocks_storage:
  backend: filesystem
  filesystem:
    dir: {}
  tsdb:
    dir: {}
    ship_interval: 1h
"#,
            blocks.display(),
            tsdb.display()
        );
        let mut proc = start_mimir(which, "empty-org", &dir, &yaml, &[]);
        wait_ready(&http, &mut proc, hp, Duration::from_secs(20)).await;
        procs.push(proc);
        http_ports.push(hp);
        grpc_ports.push(gp);
        tsdb_dirs.push(tsdb);
    }

    let mut outcomes = Vec::new();
    for idx in 0..2 {
        let ch = Channel::from_shared(format!("http://127.0.0.1:{}", grpc_ports[idx]))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut ing = IngesterClient::new(ch.clone());
        let mut hg = HttpGrpcClient::new(ch);

        let with_org = |org: &str| {
            let mut md = tonic::metadata::MetadataMap::new();
            md.insert("x-scope-orgid", MetadataValue::try_from(org).unwrap());
            md
        };

        // 1. Read RPCs with empty org="" before Push
        let mut r_ln0 = tonic::Request::new(ingesterpb::LabelNamesRequest::default());
        *r_ln0.metadata_mut() = with_org("");
        let ln0 = ing.label_names(r_ln0).await.unwrap().into_inner();

        let mut r_aus = tonic::Request::new(ingesterpb::UserStatsRequest::default());
        *r_aus.metadata_mut() = with_org("");
        let _ = ing.all_user_stats(r_aus).await.unwrap();

        // 2. Push with empty org=""
        let mut r_push = tonic::Request::new(cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair {
                        name: b"__name__".to_vec().into(),
                        value: b"w3_empty_org_metric".to_vec().into(),
                    },
                    cortexpb::LabelPair {
                        name: b"job".to_vec().into(),
                        value: b"probe".to_vec().into(),
                    },
                ],
                samples: vec![cortexpb::Sample {
                    value: 42.5,
                    timestamp_ms: ts,
                }],
                ..Default::default()
            }],
            ..Default::default()
        });
        *r_push.metadata_mut() = with_org("");
        ing.push(r_push).await.unwrap();

        // Verify UserTsdb stayed inside tsdb_dir (wal + chunks_head created inside tsdb_dir).
        assert!(tsdb_dirs[idx].join("wal").is_dir());

        // 3. Read RPCs after Push with org="", org="|", and org=":k=v"
        let mut ln_after = Vec::new();
        for org_variant in ["", "|", ":k=v"] {
            let mut req = tonic::Request::new(ingesterpb::LabelNamesRequest {
                start_timestamp_ms: ts - 60_000,
                end_timestamp_ms: ts + 60_000,
                ..Default::default()
            });
            *req.metadata_mut() = with_org(org_variant);
            let res = ing.label_names(req).await.unwrap().into_inner();
            ln_after.push(res.label_names);
        }

        // 4. Repeated empty x-scope-orgid headers -> rejected with Unknown("no org id")
        let mut r_rep = tonic::Request::new(ingesterpb::LabelNamesRequest::default());
        r_rep
            .metadata_mut()
            .append("x-scope-orgid", MetadataValue::from_static(""));
        r_rep
            .metadata_mut()
            .append("x-scope-orgid", MetadataValue::from_static(""));
        let rep_err = ing.label_names(r_rep).await.unwrap_err();

        // 5. HTTP /ingester/tsdb_metrics with X-Scope-OrgID: "" -> 401 "no org id\n"
        let http_resp = http
            .get(format!(
                "http://127.0.0.1:{}/ingester/tsdb_metrics",
                http_ports[idx]
            ))
            .header("X-Scope-OrgID", "")
            .send()
            .await
            .unwrap();
        let http_status = http_resp.status().as_u16();
        let http_body = http_resp.text().await.unwrap();

        // 6. httpgrpc /ingester/tsdb_metrics with gRPC md x-scope-orgid="" and HTTP X-Scope-OrgID="" -> 401 "no org id\n"
        let mut hg_req = tonic::Request::new(HttpGrpcRequest {
            method: "GET".to_string(),
            url: "/ingester/tsdb_metrics".to_string(),
            headers: vec![HttpGrpcHeader {
                key: "X-Scope-OrgID".to_string(),
                values: vec![String::new()],
            }],
            body: Bytes::new(),
        });
        *hg_req.metadata_mut() = with_org("");
        let hg_err = hg.handle(hg_req).await.unwrap_err();

        // 7. GET /ingester/tenants (JSON) includes Tenant == "" with Series == 1
        let (t_code, _, t_body) = http_get(&http, http_ports[idx], "/ingester/tenants", true)
            .await
            .unwrap();
        assert_eq!(t_code, 200);
        let t_json: Value = serde_json::from_str(&t_body).unwrap();
        let tenants = t_json["Tenants"].as_array().unwrap();
        let empty_tenant_series = tenants
            .iter()
            .find(|t| t["Tenant"].as_str() == Some(""))
            .and_then(|t| t["Series"].as_i64());

        outcomes.push((
            ln0.label_names,
            ln_after,
            (rep_err.code(), rep_err.message().to_string()),
            (http_status, http_body),
            (hg_err.code(), hg_err.message().to_string()),
            empty_tenant_series,
        ));
    }

    assert_eq!(outcomes[0], outcomes[1]);
    assert_eq!(
        outcomes[0].1[0],
        vec!["__name__".to_string(), "job".to_string()]
    );
    assert_eq!(
        outcomes[0].2,
        (tonic::Code::Unknown, "no org id".to_string())
    );
    assert_eq!(outcomes[0].3, (401, "no org id\n".to_string()));
    assert_eq!(outcomes[0].5, Some(1));
    drop(procs);
}

// ---------------------------------------------------------------------------
// Test 3: W3-12 (/ingester/partition-ring JSON local timezone) &
//         W3-16 (pre-instantiated cortex_response_message_bytes on canceled gRPC)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn w3_12_16_partition_ring_local_tz_and_canceled_grpc_response_bytes_parity() {
    let tmp = tempfile::tempdir().unwrap();
    let http = http_client();

    let broker = SpawnedKfakeBroker::spawn_with_port(
        &KfakeBrokerRequest {
            topic: "ingest".to_string(),
            partitions: 1,
            compression: None,
            records: Vec::new(),
        },
        Some(free_test_port()),
    )
    .unwrap();
    let kafka_addr = broker.ready.broker_addr.clone();

    let mut procs = Vec::new();
    let mut http_ports = Vec::new();
    let mut grpc_ports = Vec::new();

    for which in IMPLS {
        let hp = free_test_port();
        let gp = free_test_port();
        let dir = tmp.path().join(format!("part-grpc-{}", which.name()));
        let blocks = dir.join("blocks");
        let tsdb = dir.join("tsdb");
        std::fs::create_dir_all(&blocks).unwrap();
        std::fs::create_dir_all(&tsdb).unwrap();
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
    instance_id: ingester-zone-a-0
    instance_addr: 127.0.0.1
    kvstore:
      store: inmemory
    replication_factor: 1
    min_ready_duration: 0s
  partition_ring:
    kvstore:
      store: inmemory
    min_partition_owners_count: 1
    min_partition_owners_duration: 0s
ingest_storage:
  enabled: true
  kafka:
    address: {kafka_addr}
    topic: ingest
    auto_create_topic_enabled: false
blocks_storage:
  backend: filesystem
  filesystem:
    dir: {}
  tsdb:
    dir: {}
    ship_interval: 1h
"#,
            blocks.display(),
            tsdb.display()
        );
        // Spawn both with TZ=Asia/Tokyo (+09:00) so W3-12 exercises a non-UTC local offset even on UTC CI runners!
        let mut proc = start_mimir_with_env(
            which,
            "part-grpc",
            &dir,
            &yaml,
            &[],
            &[("TZ", "Asia/Tokyo")],
        );
        wait_ready(&http, &mut proc, hp, Duration::from_secs(25)).await;
        procs.push(proc);
        http_ports.push(hp);
        grpc_ports.push(gp);
    }

    // --- Verify W3-12: GET /ingester/partition-ring (Accept: application/json) ---
    let mut part_keys = Vec::new();
    let mut tz_suffixes = Vec::new();
    for &hp in &http_ports {
        let (code, _, body) = http_get(&http, hp, "/ingester/partition-ring", true)
            .await
            .unwrap();
        assert_eq!(code, 200);
        let v: Value = serde_json::from_str(&body).unwrap();
        let p0 = v["partitions"][0].as_object().expect("partition 0 object");
        let keys: BTreeSet<String> = p0.keys().cloned().collect();
        let ts_str = p0["state_timestamp"].as_str().expect("state_timestamp str");
        assert!(
            ts_str.ends_with("+09:00"),
            "expected TZ=Asia/Tokyo (+09:00) in state_timestamp, got {ts_str:?}"
        );
        part_keys.push(keys);
        tz_suffixes.push(ts_str[ts_str.len() - 6..].to_string());
    }
    assert_eq!(part_keys[0], part_keys[1]);
    assert_eq!(tz_suffixes[0], tz_suffixes[1]);

    // --- Verify W3-16: Canceled gRPC call exposes cortex_response_message_bytes_count == 0 ---
    let mut canceled_metrics = Vec::new();
    for idx in 0..2 {
        let ch = Channel::from_shared(format!("http://127.0.0.1:{}", grpc_ports[idx]))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut ing = IngesterClient::new(ch);

        // Request strong read consistency for unreachable offset v1=0:999999 and cancel from the
        // client after 150ms (pure client RST_STREAM without sending a racing `grpc-timeout`
        // header, where grpc-go races its transport stream-reset timer against context deadline).
        let mut req = tonic::Request::new(ingesterpb::LabelNamesRequest::default());
        req.metadata_mut()
            .insert("x-scope-orgid", MetadataValue::from_static("t-cancel"));
        req.metadata_mut()
            .insert("__consistency_level__", MetadataValue::from_static("strong"));
        req.metadata_mut().insert(
            "__consistency_offsets__",
            MetadataValue::from_static("v1=0:999999"),
        );
        tokio::time::timeout(Duration::from_millis(150), ing.label_names(req))
            .await
            .expect_err("client timeout must cancel in-flight strong-consistency LabelNames");

        // Give the server a brief moment to finalize the canceled stream's RpcGuard.
        tokio::time::sleep(Duration::from_millis(100)).await;

        let (m_code, _, m_body) = http_get(&http, http_ports[idx], "/metrics", false)
            .await
            .unwrap();
        assert_eq!(m_code, 200);
        let interesting: Vec<String> = m_body
            .lines()
            .filter(|l| {
                !l.starts_with('#')
                    && l.contains("route=\"/cortex.Ingester/LabelNames\"")
                    && (l.starts_with("cortex_inflight_requests{")
                        || l.starts_with("cortex_request_duration_seconds_count{")
                        || l.starts_with("cortex_request_message_bytes_count{")
                        || l.starts_with("cortex_response_message_bytes_count{")
                        || l.starts_with("cortex_response_message_bytes_sum{"))
            })
            .map(str::to_string)
            .collect();
        canceled_metrics.push(interesting);
    }

    assert_eq!(canceled_metrics[0], canceled_metrics[1]);
    assert!(
        canceled_metrics[1].contains(
            &"cortex_response_message_bytes_count{method=\"gRPC\",route=\"/cortex.Ingester/LabelNames\"} 0"
                .to_string()
        ),
        "expected cortex_response_message_bytes_count == 0 in {:?}",
        canceled_metrics[1]
    );

    drop(procs);
    drop(broker);
}
