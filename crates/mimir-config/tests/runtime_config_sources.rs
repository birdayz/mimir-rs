//! Runtime config manager: HTTP(S) sources, per-source decoding/merging and the `map` loader.
//!
//! Ported from github.com/grafana/dskit/runtimeconfig/manager.go, provider.go, config_source.go
//! (upstream manager_test.go is not vendored; these cases mirror its HTTP-provider, failure
//! policy and merge tests) and github.com/grafana/mimir/pkg/mimir/runtime_config.go
//! (`-runtime-config.loader`). Expected error strings were checked against the Go binary.

use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use mimir_config::{
    GoDuration, InstanceLimits, LimitsConfig, RuntimeConfigConfig, RuntimeConfigManager,
    StringSliceCSV,
};
use mimir_config::runtime_config::RuntimeConfigHttpObserver;
use parking_lot::Mutex;

// ---------------------------------------------------------------------------------------------
// Minimal HTTP/1.1 test server.

#[derive(Clone, Debug)]
struct Response {
    status: u16,
    content_type: &'static str,
    body: Vec<u8>,
    delay: Duration,
}

impl Response {
    fn ok(body: &str) -> Self {
        Self {
            status: 200,
            content_type: "text/plain",
            body: body.as_bytes().to_vec(),
            delay: Duration::ZERO,
        }
    }
}

#[derive(Debug, Clone)]
struct Request {
    path: String,
    headers: Vec<(String, String)>,
}

impl Request {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

struct TestServer {
    addr: String,
    response: Arc<Mutex<Response>>,
    requests: Arc<Mutex<Vec<Request>>>,
    connections: Arc<AtomicUsize>,
}

impl TestServer {
    fn start(initial: Response) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = format!("http://{}", listener.local_addr().unwrap());
        let response = Arc::new(Mutex::new(initial));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let connections = Arc::new(AtomicUsize::new(0));
        let (r, q, c) = (response.clone(), requests.clone(), connections.clone());
        thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { return };
                c.fetch_add(1, Ordering::SeqCst);
                let (r, q) = (r.clone(), q.clone());
                thread::spawn(move || serve_conn(stream, r, q));
            }
        });
        Self {
            addr,
            response,
            requests,
            connections,
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.addr, path)
    }

    fn set(&self, resp: Response) {
        *self.response.lock() = resp;
    }
}

fn serve_conn(stream: TcpStream, resp: Arc<Mutex<Response>>, reqs: Arc<Mutex<Vec<Request>>>) {
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut stream = stream;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).unwrap_or(0) == 0 {
            return;
        }
        let path = line.split_whitespace().nth(1).unwrap_or("/").to_string();
        let mut headers = Vec::new();
        loop {
            let mut h = String::new();
            if reader.read_line(&mut h).unwrap_or(0) == 0 {
                return;
            }
            let h = h.trim_end();
            if h.is_empty() {
                break;
            }
            if let Some((k, v)) = h.split_once(':') {
                headers.push((k.trim().to_string(), v.trim().to_string()));
            }
        }
        let req = Request { path, headers };
        let close = req
            .header("connection")
            .is_some_and(|v| v.eq_ignore_ascii_case("close"));
        reqs.lock().push(req);
        let r = resp.lock().clone();
        thread::sleep(r.delay);
        let reason = match r.status {
            200 => "OK",
            511 => "Network Authentication Required",
            _ => "Error",
        };
        let head = format!(
            "HTTP/1.1 {} {reason}\r\nContent-Type: {}\r\nContent-Length: {}\r\n{}\r\n",
            r.status,
            r.content_type,
            r.body.len(),
            if close { "Connection: close\r\n" } else { "" }
        );
        if stream.write_all(head.as_bytes()).is_err() || stream.write_all(&r.body).is_err() {
            return;
        }
        if close {
            return;
        }
    }
}

// ---------------------------------------------------------------------------------------------

fn rt_cfg(files: &[&str]) -> RuntimeConfigConfig {
    RuntimeConfigConfig {
        file: StringSliceCSV(files.iter().map(|s| s.to_string()).collect()),
        period: GoDuration::from_secs(3600),
        ..Default::default()
    }
}

fn new_mgr(cfg: RuntimeConfigConfig) -> Result<Arc<RuntimeConfigManager>, String> {
    RuntimeConfigManager::new(cfg, LimitsConfig::default(), InstanceLimits::default())
        .map_err(|e| e.to_string())
}

fn series_limit(mgr: &RuntimeConfigManager, tenant: &str) -> Option<i64> {
    mgr.get_config()
        .overrides
        .get(tenant)
        .and_then(|l| l.as_ref().map(|l| l.max_global_series_per_user))
}

fn overrides(tenant: &str, v: i64) -> String {
    format!("overrides:\n  {tenant}:\n    max_global_series_per_user: {v}\n")
}

#[test]
fn http_source_is_loaded_and_reloaded() {
    let srv = TestServer::start(Response::ok(&overrides("t1", 7)));
    let mgr = new_mgr(rt_cfg(&[&srv.url("/rt.yaml")])).unwrap();
    assert_eq!(series_limit(&mgr, "t1"), Some(7));

    let req = srv.requests.lock()[0].clone();
    assert_eq!(req.path, "/rt.yaml");
    assert_eq!(req.header("user-agent"), Some("dskit-runtimeconfig"));
    // http_client_disable_keep_alives defaults to true.
    assert_eq!(req.header("connection"), Some("close"));
    assert_eq!(req.header("x-cluster"), None);

    srv.set(Response::ok(&overrides("t1", 9)));
    mgr.reload().unwrap();
    assert_eq!(series_limit(&mgr, "t1"), Some(9));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_source_can_be_loaded_from_async_context() {
    let srv = TestServer::start(Response::ok(&overrides("t1", 3)));
    let mgr = new_mgr(rt_cfg(&[&srv.url("/rt.yaml")])).unwrap();
    assert_eq!(series_limit(&mgr, "t1"), Some(3));
    drop(mgr);
}

#[test]
fn http_and_file_sources_merge_left_to_right() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("base.yaml");
    std::fs::write(&file, overrides("t1", 1) + "  t2:\n    max_global_series_per_user: 2\n")
        .unwrap();
    let srv = TestServer::start(Response::ok(&overrides("t2", 20)));
    let mgr = new_mgr(rt_cfg(&[file.to_str().unwrap(), &srv.url("/o.yaml")])).unwrap();
    assert_eq!(series_limit(&mgr, "t1"), Some(1));
    assert_eq!(series_limit(&mgr, "t2"), Some(20));
}

#[test]
fn http_non_2xx_fails_the_load() {
    let srv = TestServer::start(Response {
        status: 500,
        ..Response::ok("boom")
    });
    let url = srv.url("/rt.yaml");
    let err = new_mgr(rt_cfg(&[&url])).err().unwrap();
    assert!(
        err.contains(&format!("failed to load runtime config: read \"{url}\": HTTP 500 from {url}")),
        "{err}"
    );
}

#[test]
fn http_failure_policies() {
    let srv = TestServer::start(Response {
        status: 503,
        ..Response::ok("")
    });

    // optional-on-startup: tolerated at startup, fails a later reload.
    let url = srv.url("/a.yaml");
    let mgr = new_mgr(rt_cfg(&[&format!("{url};optional-on-startup")])).unwrap();
    assert!(mgr.get_config().overrides.is_empty());
    assert!(mgr.reload().is_err());

    // optional-keep-last-value-on-failure: keeps the last good value on failure.
    srv.set(Response::ok(&overrides("t1", 4)));
    let mgr = new_mgr(rt_cfg(&[&format!("{url};optional-keep-last-value-on-failure")])).unwrap();
    assert_eq!(series_limit(&mgr, "t1"), Some(4));
    srv.set(Response {
        status: 500,
        ..Response::ok("")
    });
    mgr.reload().unwrap();
    assert_eq!(series_limit(&mgr, "t1"), Some(4));
}

#[test]
fn http_client_timeout_is_honoured() {
    let srv = TestServer::start(Response {
        delay: Duration::from_secs(3),
        ..Response::ok(&overrides("t1", 1))
    });
    let mut cfg = rt_cfg(&[&srv.url("/slow.yaml")]);
    cfg.http_client_timeout = GoDuration::from_millis(200);
    let start = std::time::Instant::now();
    let err = new_mgr(cfg).err().unwrap();
    assert!(start.elapsed() < Duration::from_secs(2), "timeout not applied");
    assert!(err.contains("Client.Timeout exceeded"), "{err}");
}

#[test]
fn http_keep_alives_can_be_enabled() {
    let srv = TestServer::start(Response::ok(&overrides("t1", 1)));
    let mut cfg = rt_cfg(&[&srv.url("/rt.yaml")]);
    cfg.http_client_disable_keep_alives = false;
    let mgr = new_mgr(cfg).unwrap();
    for i in 2..5 {
        srv.set(Response::ok(&overrides("t1", i)));
        mgr.reload().unwrap();
        assert_eq!(series_limit(&mgr, "t1"), Some(i));
    }
    assert_eq!(srv.requests.lock().len(), 4);
    assert!(srv
        .requests
        .lock()
        .iter()
        .all(|r| r.header("connection").is_none()));
    assert_eq!(srv.connections.load(Ordering::SeqCst), 1, "connection was not reused");

    // Default (keep-alives disabled): one connection per request.
    let srv2 = TestServer::start(Response::ok(&overrides("t1", 1)));
    let mgr2 = new_mgr(rt_cfg(&[&srv2.url("/rt.yaml")])).unwrap();
    mgr2.reload().unwrap();
    mgr2.reload().unwrap();
    assert_eq!(srv2.connections.load(Ordering::SeqCst), 3);
}

#[derive(Default)]
struct RecordingObserver {
    requests: Mutex<Vec<(String, String)>>,
    rejections: Mutex<Vec<(String, String)>>,
}

impl RuntimeConfigHttpObserver for RecordingObserver {
    fn observe_request(&self, url: &str, status: &str, _elapsed: Duration) {
        self.requests.lock().push((url.to_string(), status.to_string()));
    }
    fn invalid_cluster_validation(&self, msg: &str, method: &str) {
        self.rejections.lock().push((msg.to_string(), method.to_string()));
    }
}

#[test]
fn http_cluster_validation_label() {
    let srv = TestServer::start(Response::ok(&overrides("t1", 1)));
    let url = srv.url("/rt.yaml?token=secret");
    let mut cfg = rt_cfg(&[&url]);
    cfg.http_client_cluster_validation.label = "cluster-a".to_string();
    let mgr = new_mgr(cfg).unwrap();
    assert_eq!(srv.requests.lock()[0].header("x-cluster"), Some("cluster-a"));

    let obs = Arc::new(RecordingObserver::default());
    mgr.set_http_observer(obs.clone());

    // Server rejects with dskit's cluster validation error.
    srv.set(Response {
        status: 511,
        content_type: "application/json",
        body: br#"{"cluster_validation_error_message":"rejected request with wrong cluster validation label \"cluster-a\" - it should be \"cluster-b\"","route":"runtime_config"}"#.to_vec(),
        delay: Duration::ZERO,
    });
    let err = mgr.reload().unwrap_err().to_string();
    assert!(
        err.contains(&format!(
            "read \"{url}\": Get \"{url}\": request rejected by the server: rejected request with wrong cluster validation label \"cluster-a\" - it should be \"cluster-b\""
        )),
        "{err}"
    );
    let rej = obs.rejections.lock().clone();
    assert_eq!(rej.len(), 1);
    assert_eq!(rej[0].1, "runtime_config");
    // Metrics label drops the query string.
    let reqs = obs.requests.lock().clone();
    assert_eq!(reqs, vec![(srv.url("/rt.yaml"), "error".to_string())]);

    // A 511 that is not JSON is passed through as a plain HTTP error.
    srv.set(Response {
        status: 511,
        ..Response::ok("nope")
    });
    let err = mgr.reload().unwrap_err().to_string();
    assert!(err.contains(&format!("HTTP 511 from {url}")), "{err}");
}

// ---------------------------------------------------------------------------------------------
// Per-source decoding and merging (dskit manager.go), verified against the Go binary.

fn write(dir: &tempfile::TempDir, name: &str, content: &[u8]) -> String {
    let p = dir.path().join(name);
    std::fs::write(&p, content).unwrap();
    p.to_str().unwrap().to_string()
}

#[test]
fn only_first_yaml_document_of_a_source_is_used() {
    let dir = tempfile::tempdir().unwrap();
    let f = write(
        &dir,
        "multi.yaml",
        format!("{}---\n{}", overrides("t1", 5), overrides("t2", 6)).as_bytes(),
    );
    let mgr = new_mgr(rt_cfg(&[&f])).unwrap();
    assert_eq!(series_limit(&mgr, "t1"), Some(5));
    assert_eq!(series_limit(&mgr, "t2"), None);
}

#[test]
fn numeric_tenant_ids_yaml_vs_map_loader() {
    let dir = tempfile::tempdir().unwrap();
    let f = write(&dir, "num.yaml", overrides("12345", 5).as_bytes());

    // yaml loader: the key round-trips to the string "12345".
    let mgr = new_mgr(rt_cfg(&[&f])).unwrap();
    assert_eq!(series_limit(&mgr, "12345"), Some(5));

    // map loader: mapstructure refuses the int key.
    let mut cfg = rt_cfg(&[&f]);
    cfg.loader = "map".to_string();
    let err = new_mgr(cfg).err().unwrap();
    assert!(
        err.ends_with(
            "failed to load runtime config: load file: decoding failed due to the following error(s):\n\n'overrides[<interface {} Value>]' expected type 'string', got unconvertible type 'int'"
        ),
        "{err}"
    );

    // Quoted tenant IDs work with both loaders.
    let q = write(&dir, "quoted.yaml", overrides("\"12345\"", 6).as_bytes());
    let mut cfg = rt_cfg(&[&q]);
    cfg.loader = "map".to_string();
    let mgr = new_mgr(cfg).unwrap();
    assert_eq!(series_limit(&mgr, "12345"), Some(6));
}

#[test]
fn numeric_tenant_maps_are_replaced_not_merged() {
    // yaml.v3 decodes a mapping with a non-string key into map[any]any, which mergeConfigMaps
    // does not recurse into.
    let dir = tempfile::tempdir().unwrap();
    let a = write(&dir, "a.yaml", overrides("1", 1).as_bytes());
    let b = write(&dir, "b.yaml", overrides("2", 2).as_bytes());
    let mgr = new_mgr(rt_cfg(&[&a, &b])).unwrap();
    assert_eq!(series_limit(&mgr, "1"), None);
    assert_eq!(series_limit(&mgr, "2"), Some(2));
}

#[test]
fn conflicting_types_fail_the_merge() {
    let dir = tempfile::tempdir().unwrap();
    let a = write(&dir, "a.yaml", overrides("t1", 5).as_bytes());
    let b = write(&dir, "conflict.yaml", b"overrides: foo\n");
    let err = new_mgr(rt_cfg(&[&a, &b])).err().unwrap();
    assert_eq!(
        err,
        format!(
            "error loading runtime config: failed to load runtime config: can't merge \"{b}\" on top of the previous providers: conflicting types for \".overrides\": map[string]interface {{}} != string"
        )
    );
}

#[test]
fn json_and_gzip_sources() {
    use std::io::Write as _;
    let dir = tempfile::tempdir().unwrap();
    let j = write(
        &dir,
        "o.json",
        br#"{"overrides": {"t1": {"max_global_series_per_user": 1e3}}}"#,
    );
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    enc.write_all(overrides("t2", 22).as_bytes()).unwrap();
    let gz = enc.finish().unwrap();
    let g = write(&dir, "o.yaml.gz", &gz);
    let mgr = new_mgr(rt_cfg(&[&j, &g])).unwrap();
    assert_eq!(series_limit(&mgr, "t1"), Some(1000));
    assert_eq!(series_limit(&mgr, "t2"), Some(22));

    let bad = write(&dir, "gz-without-ext.yaml", &gz);
    let err = new_mgr(rt_cfg(&[&bad])).err().unwrap();
    assert!(
        err.contains("file looks gzipped but doesn't have a .gz extension"),
        "{err}"
    );
}

#[test]
fn missing_file_error_matches_go() {
    let err = new_mgr(rt_cfg(&["/nonexistent/rt.yaml"])).err().unwrap();
    assert_eq!(
        err,
        "error loading runtime config: failed to load runtime config: read \"/nonexistent/rt.yaml\": open /nonexistent/rt.yaml: no such file or directory"
    );
}

#[test]
fn invalid_loader_is_rejected() {
    let mut cfg = rt_cfg(&["/x.yaml"]);
    cfg.loader = "json".to_string();
    let err = new_mgr(cfg).err().unwrap();
    assert!(
        err.contains("invalid runtime config loader \"json\": supported values are \"map\" and \"yaml\""),
        "{err}"
    );
}

#[test]
fn map_loader_hash_matches_dskit_formula() {
    let dir = tempfile::tempdir().unwrap();
    let body = overrides("t1", 5);
    let f = write(&dir, "a.yaml", body.as_bytes());
    let mut cfg = rt_cfg(&[&f]);
    cfg.loader = "map".to_string();
    let mgr = new_mgr(cfg).unwrap();
    assert_eq!(series_limit(&mgr, "t1"), Some(5));

    let mut buf = Vec::new();
    buf.extend_from_slice(&(f.len() as u64).to_be_bytes());
    buf.extend_from_slice(f.as_bytes());
    buf.extend_from_slice(&mimir_config::sha256(body.as_bytes()));
    assert_eq!(mgr.config_hash(), Some(mimir_config::sha256_hex(&buf)));
}

#[test]
fn listeners_receive_new_configs_and_unchanged_sources_skip_rebuild() {
    let dir = tempfile::tempdir().unwrap();
    let f = write(&dir, "a.yaml", overrides("t1", 1).as_bytes());
    let mgr = new_mgr(rt_cfg(&[&f])).unwrap();
    let mut rx = mgr.create_listener_channel(1);

    // Unchanged: no notification.
    mgr.reload().unwrap();
    assert!(rx.try_recv().is_err());

    std::fs::write(&f, format!("{}multi_kv_config:\n  primary: memberlist\n", overrides("t1", 2)))
        .unwrap();
    mgr.reload().unwrap();
    let got = rx.try_recv().unwrap();
    assert_eq!(got.multi_kv_config.primary, "memberlist");
    assert_eq!(got.overrides["t1"].as_ref().unwrap().max_global_series_per_user, 2);

    // Buffer full: further updates are dropped, not blocking.
    std::fs::write(&f, overrides("t1", 3)).unwrap();
    mgr.reload().unwrap();
    std::fs::write(&f, overrides("t1", 4)).unwrap();
    mgr.reload().unwrap();
    assert_eq!(rx.try_recv().unwrap().overrides["t1"].as_ref().unwrap().max_global_series_per_user, 3);
    assert!(rx.try_recv().is_err());

    // Stop closes listener channels.
    mgr.stop();
    assert!(matches!(
        rx.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Disconnected)
    ));
}

#[test]
fn http_server_unreachable() {
    // Bind and drop to get a port that refuses connections.
    let port = TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let url = format!("http://127.0.0.1:{port}/rt.yaml");
    let err = new_mgr(rt_cfg(&[&url])).err().unwrap();
    assert!(err.contains(&format!("read \"{url}\": Get \"{url}\":")), "{err}");
}
