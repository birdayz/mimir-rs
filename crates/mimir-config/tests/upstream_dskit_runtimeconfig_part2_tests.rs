//! Ported from github.com/grafana/dskit/runtimeconfig/manager_test.go (part 2: 24/50 tests).

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use mimir_config::{
    FlagSet, GenericRuntimeConfigManager, GenericRuntimeConfigOptions, GoDuration, HttpProvider,
    RuntimeConfigConfig, RuntimeConfigHttpObserver, RuntimeConfigLoadEvent, RuntimeConfigLoader,
    RuntimeConfigObserver, RuntimeConfigSourceStatus, StringSliceCSV,
};
use parking_lot::Mutex;
use serde::Deserialize;

#[derive(Clone, Debug)]
struct MockResp {
    status: u16,
    content_type: &'static str,
    body: Vec<u8>,
}

impl MockResp {
    fn ok(body: &str) -> Self {
        Self {
            status: 200,
            content_type: "text/plain",
            body: body.as_bytes().to_vec(),
        }
    }
}

#[derive(Debug, Clone)]
struct Req {
    path: String,
    headers: Vec<(String, String)>,
}

impl Req {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

type ReqHandler = Arc<Mutex<Box<dyn FnMut(&Req) -> MockResp + Send>>>;

struct MockSrv {
    addr: String,
    handler: ReqHandler,
    requests: Arc<Mutex<Vec<Req>>>,
    connections: Arc<AtomicUsize>,
    shutdown: Arc<AtomicBool>,
}

impl MockSrv {
    fn start(initial: MockResp) -> Self {
        Self::start_fn(move |_| initial.clone())
    }

    fn start_fn(f: impl FnMut(&Req) -> MockResp + Send + 'static) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = format!("http://{}", listener.local_addr().unwrap());
        let handler: ReqHandler =
            Arc::new(Mutex::new(Box::new(f)));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let connections = Arc::new(AtomicUsize::new(0));
        let shutdown = Arc::new(AtomicBool::new(false));
        let (h, q, c, sd) = (
            handler.clone(),
            requests.clone(),
            connections.clone(),
            shutdown.clone(),
        );
        thread::spawn(move || {
            while !sd.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let _ = stream.set_nonblocking(false);
                        c.fetch_add(1, Ordering::SeqCst);
                        let (h, q) = (h.clone(), q.clone());
                        thread::spawn(move || serve_conn(stream, h, q));
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(_) => break,
                }
            }
        });
        Self {
            addr,
            handler,
            requests,
            connections,
            shutdown,
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.addr, path)
    }

    fn set(&self, resp: MockResp) {
        *self.handler.lock() = Box::new(move |_| resp.clone());
    }
}

impl Drop for MockSrv {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }
}

fn serve_conn(
    stream: TcpStream,
    handler: ReqHandler,
    reqs: Arc<Mutex<Vec<Req>>>,
) {
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
        let req = Req { path, headers };
        let close = req
            .header("connection")
            .is_some_and(|v| v.eq_ignore_ascii_case("close"));
        reqs.lock().push(req.clone());
        let r = (handler.lock())(&req);
        let reason = match r.status {
            200 => "OK",
            500 => "Internal Server Error",
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

#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
struct ValueCfg {
    #[serde(default)]
    value: i64,
}

fn value_loader() -> RuntimeConfigLoader<ValueCfg> {
    Arc::new(|bytes| {
        if bytes.is_empty() {
            return Ok(ValueCfg::default());
        }
        serde_yaml::from_slice(bytes).map_err(|e| e.to_string())
    })
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
struct TwoKeys {
    #[serde(default)]
    from_file: i64,
    #[serde(default)]
    from_server: i64,
}

fn two_keys_loader() -> RuntimeConfigLoader<TwoKeys> {
    Arc::new(|bytes| {
        if bytes.is_empty() {
            return Ok(TwoKeys::default());
        }
        serde_yaml::from_slice(bytes).map_err(|e| e.to_string())
    })
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
struct TestLimits {
    #[serde(default)]
    limit1: i64,
    #[serde(default)]
    limit2: i64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
struct TestOverrides {
    #[serde(default)]
    overrides: BTreeMap<String, Option<TestLimits>>,
}

fn overrides_loader() -> RuntimeConfigLoader<TestOverrides> {
    Arc::new(|bytes| serde_yaml::from_slice(bytes).map_err(|e| e.to_string()))
}

#[derive(Default)]
struct Obs {
    last_success: Mutex<Option<bool>>,
    sources: Mutex<Vec<RuntimeConfigSourceStatus>>,
    http_reqs: Mutex<Vec<(String, String)>>,
    http_rejections: Mutex<Vec<(String, String)>>,
}

impl RuntimeConfigObserver for Obs {
    fn on_load(&self, event: &RuntimeConfigLoadEvent<'_>) {
        *self.last_success.lock() = Some(event.result.is_ok());
        *self.sources.lock() = event.sources.to_vec();
    }
}

impl RuntimeConfigHttpObserver for Obs {
    fn observe_request(&self, url_for_metrics: &str, status_code: &str, _elapsed: Duration) {
        self.http_reqs
            .lock()
            .push((url_for_metrics.to_string(), status_code.to_string()));
    }

    fn invalid_cluster_validation(&self, msg: &str, method: &str) {
        self.http_rejections
            .lock()
            .push((msg.to_string(), method.to_string()));
    }
}

fn cfg_with(files: Vec<String>, period: Duration) -> RuntimeConfigConfig {
    RuntimeConfigConfig {
        file: StringSliceCSV(files),
        period: GoDuration(period.as_nanos() as i64),
        ..Default::default()
    }
}

fn poll_until<T: PartialEq + std::fmt::Debug>(
    timeout: Duration,
    expected: T,
    mut f: impl FnMut() -> T,
) {
    let deadline = Instant::now() + timeout;
    loop {
        let got = f();
        if got == expected {
            return;
        }
        assert!(Instant::now() < deadline, "poll timed out; last={got:?}, want={expected:?}");
        thread::sleep(Duration::from_millis(15));
    }
}

// =============================================================================================
// Remaining 24 manager_test.go tests
// =============================================================================================

// Upstream: github.com/grafana/dskit/runtimeconfig/manager_test.go::TestManager_GetConfigNilBeforeStarting
#[test]
fn test_manager_get_config_nil_before_starting() {
    let dir = tempfile::tempdir().unwrap();
    let f = dir.path().join("val.yaml");
    std::fs::write(&f, "value: 1\n").unwrap();

    let mgr = GenericRuntimeConfigManager::new(GenericRuntimeConfigOptions {
        cfg: cfg_with(vec![f.to_string_lossy().into()], Duration::from_secs(1)),
        preprocessor: None,
        loader: Some(value_loader()),
        map_loader: None,
        observer: None,
        http_observer: None,
    })
    .unwrap();
    assert!(mgr.get_config().is_none());
}

// Upstream: github.com/grafana/dskit/runtimeconfig/manager_test.go::TestManager_URLPath
#[test]
fn test_manager_url_path() {
    let srv = MockSrv::start(MockResp::ok("value: 42\n"));
    let mgr = GenericRuntimeConfigManager::new(GenericRuntimeConfigOptions {
        cfg: cfg_with(vec![srv.url("/config.yaml")], Duration::from_secs(1)),
        preprocessor: None,
        loader: Some(value_loader()),
        map_loader: None,
        observer: None,
        http_observer: None,
    })
    .unwrap();
    mgr.start().unwrap();
    assert_eq!(mgr.get_config(), Some(ValueCfg { value: 42 }));
    mgr.stop();
}

// Upstream: github.com/grafana/dskit/runtimeconfig/manager_test.go::TestManager_URLPathFirstLoadFailure
#[test]
fn test_manager_url_path_first_load_failure() {
    let srv = MockSrv::start(MockResp {
        status: 500,
        ..MockResp::ok("")
    });
    let mgr = GenericRuntimeConfigManager::new(GenericRuntimeConfigOptions {
        cfg: cfg_with(vec![srv.url("/config.yaml")], Duration::from_secs(1)),
        preprocessor: None,
        loader: Some(value_loader()),
        map_loader: None,
        observer: None,
        http_observer: None,
    })
    .unwrap();
    let err = mgr.start().unwrap_err().to_string();
    assert!(err.contains("HTTP 500"), "{err}");
}

// Upstream: github.com/grafana/dskit/runtimeconfig/manager_test.go::TestManager_URLPathReloadFailure
#[test]
fn test_manager_url_path_reload_failure() {
    let srv = MockSrv::start(MockResp::ok("value: 42\n"));
    let obs = Arc::new(Obs::default());
    let mgr = GenericRuntimeConfigManager::new(GenericRuntimeConfigOptions {
        cfg: cfg_with(vec![srv.url("/config.yaml")], Duration::from_millis(50)),
        preprocessor: None,
        loader: Some(value_loader()),
        map_loader: None,
        observer: Some(obs.clone()),
        http_observer: None,
    })
    .unwrap();
    mgr.start().unwrap();
    mgr.start_reloader();
    assert_eq!(mgr.get_config(), Some(ValueCfg { value: 42 }));

    srv.set(MockResp {
        status: 500,
        ..MockResp::ok("")
    });
    poll_until(Duration::from_secs(5), Some(false), || *obs.last_success.lock());
    assert_eq!(mgr.get_config(), Some(ValueCfg { value: 42 }));

    srv.set(MockResp::ok("value: 99\n"));
    poll_until(Duration::from_secs(5), Some(ValueCfg { value: 99 }), || {
        mgr.get_config()
    });
    mgr.stop();
}

// Upstream: github.com/grafana/dskit/runtimeconfig/manager_test.go::TestManager_MixedPaths
#[test]
fn test_manager_mixed_paths() {
    let dir = tempfile::tempdir().unwrap();
    let f = dir.path().join("mixed.yaml");
    std::fs::write(&f, "overrides:\n  user1:\n    limit1: 100\n").unwrap();
    let srv = MockSrv::start(MockResp::ok("overrides:\n  user2:\n    limit2: 200\n"));

    let mgr = GenericRuntimeConfigManager::new(GenericRuntimeConfigOptions {
        cfg: cfg_with(
            vec![f.to_string_lossy().into(), srv.url("/config.yaml")],
            Duration::from_secs(1),
        ),
        preprocessor: None,
        loader: Some(overrides_loader()),
        map_loader: None,
        observer: None,
        http_observer: None,
    })
    .unwrap();
    mgr.start().unwrap();
    let conf = mgr.get_config().unwrap();
    assert_eq!(conf.overrides["user1"].as_ref().unwrap().limit1, 100);
    assert_eq!(conf.overrides["user2"].as_ref().unwrap().limit2, 200);
    mgr.stop();
}

// Upstream: github.com/grafana/dskit/runtimeconfig/manager_test.go::TestManager_URLPathMetrics
#[test]
fn test_manager_url_path_metrics() {
    let srv = MockSrv::start(MockResp::ok("value: 1\n"));
    let obs = Arc::new(Obs::default());
    let mgr = GenericRuntimeConfigManager::new(GenericRuntimeConfigOptions {
        cfg: cfg_with(vec![srv.url("/config.yaml")], Duration::from_secs(1)),
        preprocessor: None,
        loader: Some(value_loader()),
        map_loader: None,
        observer: None,
        http_observer: Some(obs.clone()),
    })
    .unwrap();
    mgr.start().unwrap();
    let reqs = obs.http_reqs.lock().clone();
    assert_eq!(reqs.len(), 1);
    assert_eq!(reqs[0].1, "200");
    assert!(reqs[0].0.contains(&srv.addr));
    mgr.stop();
}

// Upstream: github.com/grafana/dskit/runtimeconfig/manager_test.go::TestManager_URLPath_ClusterValidationLabel
#[test]
fn test_manager_url_path_cluster_validation_label() {
    let srv = MockSrv::start(MockResp::ok("value: 42\n"));
    let mut cfg = cfg_with(vec![srv.url("/config.yaml")], Duration::from_secs(1));
    cfg.http_client_cluster_validation.label = "my-cluster".into();

    let mgr = GenericRuntimeConfigManager::new(GenericRuntimeConfigOptions {
        cfg,
        preprocessor: None,
        loader: Some(value_loader()),
        map_loader: None,
        observer: None,
        http_observer: None,
    })
    .unwrap();
    mgr.start().unwrap();
    assert_eq!(mgr.get_config(), Some(ValueCfg { value: 42 }));
    assert_eq!(
        srv.requests.lock()[0].header("x-cluster"),
        Some("my-cluster")
    );
    mgr.stop();
}

// Upstream: github.com/grafana/dskit/runtimeconfig/manager_test.go::TestManager_URLPath_ClusterValidationLabel_RejectedByServer
#[test]
fn test_manager_url_path_cluster_validation_label_rejected_by_server() {
    let srv = MockSrv::start(MockResp {
        status: 511,
        content_type: "application/json",
        body: br#"{"cluster_validation_error_message":"wrong cluster","route":"GET"}"#.to_vec(),
    });
    let mut cfg = cfg_with(vec![srv.url("/config.yaml")], Duration::from_secs(1));
    cfg.http_client_cluster_validation.label = "client-cluster".into();

    let obs = Arc::new(Obs::default());
    let mgr = GenericRuntimeConfigManager::new(GenericRuntimeConfigOptions {
        cfg,
        preprocessor: None,
        loader: Some(value_loader()),
        map_loader: None,
        observer: None,
        http_observer: Some(obs.clone()),
    })
    .unwrap();
    let err = mgr.start().unwrap_err().to_string();
    assert!(
        err.contains("request rejected by the server: wrong cluster"),
        "{err}"
    );
    assert_eq!(obs.http_rejections.lock().len(), 1);
    assert_eq!(obs.http_rejections.lock()[0].1, "GET");
}

// Upstream: github.com/grafana/dskit/runtimeconfig/manager_test.go::TestManager_URLPath_ClusterValidationLabel_SharedRegistry
#[test]
fn test_manager_url_path_cluster_validation_label_shared_registry() {
    let srv = MockSrv::start(MockResp::ok("value: 1\n"));
    let shared_obs = Arc::new(Obs::default());

    for name in ["first", "second"] {
        let mut cfg = cfg_with(vec![srv.url(&format!("/{name}.yaml"))], Duration::from_secs(1));
        cfg.http_client_cluster_validation.label = "shared-cluster".into();
        let mgr = GenericRuntimeConfigManager::new(GenericRuntimeConfigOptions {
            cfg,
            preprocessor: None,
            loader: Some(value_loader()),
            map_loader: None,
            observer: Some(shared_obs.clone()),
            http_observer: Some(shared_obs.clone()),
        })
        .unwrap();
        mgr.start().unwrap();
        mgr.stop();
    }
    assert_eq!(shared_obs.http_reqs.lock().len(), 2);
}

// Upstream: github.com/grafana/dskit/runtimeconfig/manager_test.go::TestManager_URLPath_NoClusterValidationLabelByDefault
#[test]
fn test_manager_url_path_no_cluster_validation_label_by_default() {
    let srv = MockSrv::start(MockResp::ok("value: 7\n"));
    let mgr = GenericRuntimeConfigManager::new(GenericRuntimeConfigOptions {
        cfg: cfg_with(vec![srv.url("/config.yaml")], Duration::from_secs(1)),
        preprocessor: None,
        loader: Some(value_loader()),
        map_loader: None,
        observer: None,
        http_observer: None,
    })
    .unwrap();
    mgr.start().unwrap();
    assert_eq!(mgr.get_config(), Some(ValueCfg { value: 7 }));
    assert_eq!(srv.requests.lock()[0].header("x-cluster"), None);
    mgr.stop();
}

// Upstream: github.com/grafana/dskit/runtimeconfig/manager_test.go::TestConfig_RegisterFlagsWithPrefix
#[test]
fn test_config_register_flags_with_prefix() {
    for prefix in ["runtime-config.", "my-runtime-config.", ""] {
        let mut fs = FlagSet::<RuntimeConfigConfig>::new();
        let cfg = RuntimeConfigConfig::default();
        if prefix == "runtime-config." {
            cfg.register_flags_into(&mut fs, |c| c, |c| c);
        } else {
            cfg.register_flags_with_prefix_into(prefix, &mut fs, |c| c, |c| c);
        }

        let names: Vec<&str> = fs.flag_names().collect();
        for expected in [
            format!("{prefix}file"),
            format!("{prefix}reload-period"),
            format!("{prefix}http-client-timeout"),
            format!("{prefix}http-client-disable-keep-alives"),
            format!("{prefix}http-client-cluster-validation.label"),
        ] {
            assert!(
                names.contains(&expected.as_str()),
                "missing flag {expected:?} in {names:?}"
            );
        }
        assert_eq!(
            fs.get_value(&cfg, &format!("{prefix}http-client-disable-keep-alives"))
                .as_deref(),
            Some("true")
        );
        assert!(cfg.http_client_disable_keep_alives);
    }
}

// Upstream: github.com/grafana/dskit/runtimeconfig/manager_test.go::TestHTTPTransport_DisableKeepAlives
#[test]
fn test_http_transport_disable_keep_alives() {
    for disable in [true, false] {
        let p = HttpProvider::new(
            "http://127.0.0.1:1/c.yaml",
            "http://127.0.0.1:1/c.yaml",
            Duration::from_secs(5),
            disable,
            "",
            None,
        )
        .unwrap();
        assert_eq!(p.disable_keep_alives(), disable);
    }
}

// Upstream: github.com/grafana/dskit/runtimeconfig/manager_test.go::TestHTTPClient_DisableKeepAlives
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_http_client_disable_keep_alives() {
    for (disable_keep_alives, want_new_conns) in [(true, 5usize), (false, 1usize)] {
        let srv = MockSrv::start(MockResp::ok("value: 1\n"));
        let url = srv.url("/config.yaml");
        let p = HttpProvider::new(
            &url,
            &url,
            Duration::from_secs(5),
            disable_keep_alives,
            "",
            None,
        )
        .unwrap();

        for _ in 0..5 {
            p.read_async().await.unwrap();
            // Give reqwest/hyper's spawned HTTP/1.1 connection driver task a beat to return the
            // idle socket to the pool before the next sequential request on busy multi-core runners.
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(srv.connections.load(Ordering::SeqCst), want_new_conns);
    }
}

// Upstream: github.com/grafana/dskit/runtimeconfig/manager_test.go::TestManager_RequiredSourceFailsStartup
#[test]
fn test_manager_required_source_fails_startup() {
    let srv = MockSrv::start(MockResp {
        status: 500,
        ..MockResp::ok("")
    });
    let mgr = GenericRuntimeConfigManager::new(GenericRuntimeConfigOptions {
        cfg: cfg_with(vec![srv.url("/c.yaml")], Duration::from_millis(100)),
        preprocessor: None,
        loader: Some(two_keys_loader()),
        map_loader: None,
        observer: None,
        http_observer: None,
    })
    .unwrap();
    assert!(mgr.start().is_err());
}

// Upstream: github.com/grafana/dskit/runtimeconfig/manager_test.go::TestManager_MultipleSourceParameters
#[test]
fn test_manager_multiple_source_parameters() {
    let err = GenericRuntimeConfigManager::<TwoKeys>::new(GenericRuntimeConfigOptions {
        cfg: cfg_with(
            vec!["/etc/overrides.yaml;optional-on-startup;optional-keep-last-value-on-failure".into()],
            Duration::from_millis(100),
        ),
        preprocessor: None,
        loader: Some(two_keys_loader()),
        map_loader: None,
        observer: None,
        http_observer: None,
    })
    .err()
    .unwrap()
    .to_string();
    assert!(err.contains("more than one parameter"), "{err}");
}

// Upstream: github.com/grafana/dskit/runtimeconfig/manager_test.go::TestManager_OptionalSourceAloneDownAtStartup
#[test]
fn test_manager_optional_source_alone_down_at_startup() {
    let srv = MockSrv::start(MockResp {
        status: 500,
        ..MockResp::ok("")
    });
    let mgr = GenericRuntimeConfigManager::new(GenericRuntimeConfigOptions {
        cfg: cfg_with(
            vec![format!("{};optional-keep-last-value-on-failure", srv.url("/c.yaml"))],
            Duration::from_millis(50),
        ),
        preprocessor: None,
        loader: Some(two_keys_loader()),
        map_loader: None,
        observer: None,
        http_observer: None,
    })
    .unwrap();
    mgr.start().unwrap();
    mgr.start_reloader();
    assert_eq!(mgr.get_config(), Some(TwoKeys::default()));

    srv.set(MockResp::ok("from_server: 42\n"));
    poll_until(
        Duration::from_secs(5),
        Some(TwoKeys {
            from_file: 0,
            from_server: 42,
        }),
        || mgr.get_config(),
    );
    mgr.stop();
}

// Upstream: github.com/grafana/dskit/runtimeconfig/manager_test.go::TestManager_OptionalOnStartup
#[test]
fn test_manager_optional_on_startup() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("f.yaml");
    std::fs::write(&file, "from_file: 1\n").unwrap();
    let srv = MockSrv::start(MockResp {
        status: 500,
        ..MockResp::ok("")
    });

    let obs = Arc::new(Obs::default());
    let mgr = GenericRuntimeConfigManager::new(GenericRuntimeConfigOptions {
        cfg: cfg_with(
            vec![
                file.to_string_lossy().into(),
                format!("{};optional-on-startup", srv.url("/c.yaml")),
            ],
            Duration::from_millis(50),
        ),
        preprocessor: None,
        loader: Some(two_keys_loader()),
        map_loader: None,
        observer: Some(obs.clone()),
        http_observer: None,
    })
    .unwrap();
    mgr.start().unwrap();
    mgr.start_reloader();
    assert_eq!(
        mgr.get_config(),
        Some(TwoKeys {
            from_file: 1,
            from_server: 0
        })
    );
    assert_eq!(*obs.last_success.lock(), Some(true));
    assert!(obs.sources.lock()[0].error.is_none());
    assert!(obs.sources.lock()[1].error.is_some());

    srv.set(MockResp::ok("from_server: 42\n"));
    poll_until(
        Duration::from_secs(5),
        Some(TwoKeys {
            from_file: 1,
            from_server: 42,
        }),
        || mgr.get_config(),
    );

    srv.set(MockResp {
        status: 500,
        ..MockResp::ok("")
    });
    poll_until(Duration::from_secs(5), Some(false), || *obs.last_success.lock());
    assert_eq!(
        mgr.get_config(),
        Some(TwoKeys {
            from_file: 1,
            from_server: 42
        })
    );

    std::fs::write(&file, "from_file: 7\n").unwrap();
    thread::sleep(Duration::from_millis(200));
    assert_eq!(
        mgr.get_config(),
        Some(TwoKeys {
            from_file: 1,
            from_server: 42
        })
    );
    mgr.stop();
}

// Upstream: github.com/grafana/dskit/runtimeconfig/manager_test.go::TestManager_OptionalKeepLastValueOnFailure
#[test]
fn test_manager_optional_keep_last_value_on_failure() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("f.yaml");
    std::fs::write(&file, "from_file: 1\n").unwrap();
    let srv = MockSrv::start(MockResp {
        status: 500,
        ..MockResp::ok("")
    });

    let obs = Arc::new(Obs::default());
    let mgr = GenericRuntimeConfigManager::new(GenericRuntimeConfigOptions {
        cfg: cfg_with(
            vec![
                file.to_string_lossy().into(),
                format!("{};optional-keep-last-value-on-failure", srv.url("/c.yaml")),
            ],
            Duration::from_millis(50),
        ),
        preprocessor: None,
        loader: Some(two_keys_loader()),
        map_loader: None,
        observer: Some(obs.clone()),
        http_observer: None,
    })
    .unwrap();
    mgr.start().unwrap();
    mgr.start_reloader();
    assert_eq!(
        mgr.get_config(),
        Some(TwoKeys {
            from_file: 1,
            from_server: 0
        })
    );

    srv.set(MockResp::ok("from_server: 42\n"));
    poll_until(
        Duration::from_secs(5),
        Some(TwoKeys {
            from_file: 1,
            from_server: 42,
        }),
        || mgr.get_config(),
    );

    srv.set(MockResp {
        status: 500,
        ..MockResp::ok("")
    });
    poll_until(Duration::from_secs(5), true, || {
        obs.sources
            .lock()
            .get(1)
            .is_some_and(|s| s.error.is_some())
    });
    assert_eq!(*obs.last_success.lock(), Some(true));
    assert_eq!(
        mgr.get_config(),
        Some(TwoKeys {
            from_file: 1,
            from_server: 42
        })
    );

    std::fs::write(&file, "from_file: 7\n").unwrap();
    poll_until(
        Duration::from_secs(5),
        Some(TwoKeys {
            from_file: 7,
            from_server: 42,
        }),
        || mgr.get_config(),
    );
    mgr.stop();
}

// Upstream: github.com/grafana/dskit/runtimeconfig/manager_test.go::TestManager_OptionalKeepLastValueOnFailure_InvalidContentDoesNotPoisonLastValue
#[test]
fn test_manager_optional_keep_last_value_on_failure_invalid_content_does_not_poison_last_value() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("f.yaml");
    std::fs::write(&file, "from_file: 1\n").unwrap();
    let srv = MockSrv::start(MockResp::ok("from_server: 42\n"));

    let obs = Arc::new(Obs::default());
    let mgr = GenericRuntimeConfigManager::new(GenericRuntimeConfigOptions {
        cfg: cfg_with(
            vec![
                file.to_string_lossy().into(),
                format!("{};optional-keep-last-value-on-failure", srv.url("/c.yaml")),
            ],
            Duration::from_millis(50),
        ),
        preprocessor: None,
        loader: Some(two_keys_loader()),
        map_loader: None,
        observer: Some(obs.clone()),
        http_observer: None,
    })
    .unwrap();
    mgr.start().unwrap();
    mgr.start_reloader();
    assert_eq!(
        mgr.get_config(),
        Some(TwoKeys {
            from_file: 1,
            from_server: 42
        })
    );

    srv.set(MockResp::ok(":\n"));
    poll_until(Duration::from_secs(5), Some(false), || *obs.last_success.lock());
    assert_eq!(
        mgr.get_config(),
        Some(TwoKeys {
            from_file: 1,
            from_server: 42
        })
    );

    srv.set(MockResp {
        status: 500,
        ..MockResp::ok("")
    });
    std::fs::write(&file, "from_file: 7\n").unwrap();
    poll_until(
        Duration::from_secs(5),
        Some(TwoKeys {
            from_file: 7,
            from_server: 42,
        }),
        || mgr.get_config(),
    );
    mgr.stop();
}

// Upstream: github.com/grafana/dskit/runtimeconfig/manager_test.go::TestManager_LastValueKeptOnlyForReplayingSources
#[test]
fn test_manager_last_value_kept_only_for_replaying_sources() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("f.yaml");
    std::fs::write(&file, "from_file: 1\n").unwrap();
    let srv = MockSrv::start(MockResp::ok("from_server: 42\n"));

    let f_str = file.to_string_lossy().into_owned();
    let mgr = GenericRuntimeConfigManager::new(GenericRuntimeConfigOptions {
        cfg: cfg_with(
            vec![
                f_str.clone(),
                format!("{f_str};optional-on-startup"),
                format!("{};optional-keep-last-value-on-failure", srv.url("/c.yaml")),
            ],
            Duration::from_secs(1),
        ),
        preprocessor: None,
        loader: Some(two_keys_loader()),
        map_loader: None,
        observer: None,
        http_observer: None,
    })
    .unwrap();
    mgr.start().unwrap();
    let last_vals = mgr.source_last_values();
    assert_eq!(last_vals.len(), 3);
    assert!(last_vals[0].is_none());
    assert!(last_vals[1].is_none());
    assert_eq!(last_vals[2].as_deref(), Some(b"from_server: 42\n".as_slice()));
    mgr.stop();
}

// Upstream: github.com/grafana/dskit/runtimeconfig/manager_test.go::TestManager_SourceMetricIsPerSource
#[test]
fn test_manager_source_metric_is_per_source() {
    let srv = MockSrv::start_fn(|req| {
        if req.path.contains("tenant=b") {
            MockResp {
                status: 500,
                ..MockResp::ok("")
            }
        } else {
            MockResp::ok("from_file: 1\n")
        }
    });

    let obs = Arc::new(Obs::default());
    let mgr = GenericRuntimeConfigManager::new(GenericRuntimeConfigOptions {
        cfg: cfg_with(
            vec![
                srv.url("/config?tenant=a"),
                format!(
                    "{};optional-keep-last-value-on-failure",
                    srv.url("/config?tenant=b")
                ),
            ],
            Duration::from_secs(1),
        ),
        preprocessor: None,
        loader: Some(two_keys_loader()),
        map_loader: None,
        observer: Some(obs.clone()),
        http_observer: None,
    })
    .unwrap();
    mgr.start().unwrap();
    let sources = obs.sources.lock().clone();
    assert_eq!(sources[0].source, srv.url("/config#0"));
    assert!(sources[0].error.is_none());
    assert_eq!(sources[1].source, srv.url("/config#1"));
    assert!(sources[1].error.is_some());
    mgr.stop();
}

// Upstream: github.com/grafana/dskit/runtimeconfig/manager_test.go::TestManager_SourceMetricOmitsURLCredentials
#[test]
fn test_manager_source_metric_omits_url_credentials() {
    let srv = MockSrv::start(MockResp::ok("from_server: 42\n"));
    let with_creds = format!(
        "{}/config?token=s3cret#s3cret",
        srv.addr.replacen("http://", "http://user:s3cret@", 1)
    );
    let obs = Arc::new(Obs::default());
    let mgr = GenericRuntimeConfigManager::new(GenericRuntimeConfigOptions {
        cfg: cfg_with(vec![with_creds], Duration::from_secs(1)),
        preprocessor: None,
        loader: Some(two_keys_loader()),
        map_loader: None,
        observer: Some(obs.clone()),
        http_observer: Some(obs.clone()),
    })
    .unwrap();
    mgr.start().unwrap();

    let expected_id = srv.url("/config");
    assert_eq!(mgr.source_ids(), vec![expected_id.clone()]);
    assert_eq!(obs.sources.lock()[0].source, expected_id);
    assert!(!obs.sources.lock()[0].source.contains("s3cret"));
    assert_eq!(obs.http_reqs.lock()[0].0, expected_id);
    assert!(!obs.http_reqs.lock()[0].0.contains("s3cret"));
    mgr.stop();
}

// Upstream: github.com/grafana/dskit/runtimeconfig/manager_test.go::TestManager_SourceMetricIsNotStaleWhenALaterSourceFails
#[test]
fn test_manager_source_metric_is_not_stale_when_a_later_source_fails() {
    let optional = MockSrv::start(MockResp::ok("from_file: 1\n"));
    let required = MockSrv::start(MockResp::ok("from_server: 42\n"));

    let obs = Arc::new(Obs::default());
    let mgr = GenericRuntimeConfigManager::new(GenericRuntimeConfigOptions {
        cfg: cfg_with(
            vec![
                format!(
                    "{};optional-keep-last-value-on-failure",
                    optional.url("/c.yaml")
                ),
                required.url("/c.yaml"),
            ],
            Duration::from_millis(50),
        ),
        preprocessor: None,
        loader: Some(two_keys_loader()),
        map_loader: None,
        observer: Some(obs.clone()),
        http_observer: None,
    })
    .unwrap();
    mgr.start().unwrap();
    mgr.start_reloader();

    optional.set(MockResp {
        status: 500,
        ..MockResp::ok("")
    });
    poll_until(Duration::from_secs(5), (true, false), || {
        let s = obs.sources.lock();
        (
            s.first().is_some_and(|x| x.error.is_some()),
            s.get(1).is_some_and(|x| x.error.is_some()),
        )
    });

    optional.set(MockResp::ok("from_file: 1\n"));
    required.set(MockResp {
        status: 500,
        ..MockResp::ok("")
    });
    poll_until(Duration::from_secs(5), (false, true), || {
        let s = obs.sources.lock();
        (
            s.first().is_some_and(|x| x.error.is_some()),
            s.get(1).is_some_and(|x| x.error.is_some()),
        )
    });
    mgr.stop();
}

// Upstream: github.com/grafana/dskit/runtimeconfig/manager_test.go::TestManager_InvalidSourceRegistersNothing
#[test]
fn test_manager_invalid_source_registers_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("f.yaml");
    std::fs::write(&file, "from_file: 1\n").unwrap();
    let f_str = file.to_string_lossy().into_owned();

    for entry in [
        format!("{f_str};optional-on-startup;optional-keep-last-value-on-failure"),
        "http://config-server:not-a-port/overrides".to_string(),
    ] {
        let obs = Arc::new(Obs::default());
        let res = GenericRuntimeConfigManager::new(GenericRuntimeConfigOptions {
            cfg: cfg_with(vec![f_str.clone(), entry], Duration::from_secs(1)),
            preprocessor: None,
            loader: Some(two_keys_loader()),
            map_loader: None,
            observer: Some(obs.clone()),
            http_observer: Some(obs.clone()),
        });
        assert!(res.is_err());
        assert!(obs.last_success.lock().is_none());
        assert!(obs.sources.lock().is_empty());
        assert!(obs.http_reqs.lock().is_empty());
    }

    let mgr = GenericRuntimeConfigManager::new(GenericRuntimeConfigOptions {
        cfg: cfg_with(vec![f_str], Duration::from_secs(1)),
        preprocessor: None,
        loader: Some(two_keys_loader()),
        map_loader: None,
        observer: None,
        http_observer: None,
    })
    .unwrap();
    mgr.start().unwrap();
    assert_eq!(
        mgr.get_config(),
        Some(TwoKeys {
            from_file: 1,
            from_server: 0
        })
    );
    mgr.stop();
}
