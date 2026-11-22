//! Ported from github.com/grafana/dskit/runtimeconfig/*_test.go (part 1: 26/50 tests).

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use flate2::write::GzEncoder;
use flate2::Compression;
use mimir_config::{
    assign_config_source_ids, is_url, parse_config_source_entry, sha256_hex,
    GenericRuntimeConfigManager, GenericRuntimeConfigOptions, GoDuration, HttpProvider,
    HttpProviderError, ParsedConfigSource, RuntimeConfigConfig, RuntimeConfigHttpObserver,
    RuntimeConfigLoader,
    RuntimeConfigLoadEvent, RuntimeConfigObserver, RuntimeConfigSourceStatus, SourceParameter,
    StringSliceCSV,
};
use parking_lot::Mutex;
use serde::Deserialize;

// =============================================================================================
// Shared helpers & minimal HTTP/1.1 test server
// =============================================================================================

#[derive(Clone, Debug)]
struct MockHttpResponse {
    status: u16,
    content_type: &'static str,
    body: Vec<u8>,
    delay: Duration,
}

impl MockHttpResponse {
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
struct RecordedRequest {
    headers: Vec<(String, String)>,
}

impl RecordedRequest {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

struct MockHttpServer {
    addr: String,
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
    shutdown: Arc<AtomicBool>,
}

impl MockHttpServer {
    fn start(initial: MockHttpResponse) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = format!("http://{}", listener.local_addr().unwrap());
        let response = Arc::new(Mutex::new(initial));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let shutdown = Arc::new(AtomicBool::new(false));
        let (r, q, sd) = (response, requests.clone(), shutdown.clone());
        thread::spawn(move || {
            while !sd.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let _ = stream.set_nonblocking(false);
                        let (r, q) = (r.clone(), q.clone());
                        thread::spawn(move || serve_conn(stream, r, q));
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
            requests,
            shutdown,
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.addr, path)
    }
}

impl Drop for MockHttpServer {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }
}

fn serve_conn(
    stream: TcpStream,
    resp: Arc<Mutex<MockHttpResponse>>,
    reqs: Arc<Mutex<Vec<RecordedRequest>>>,
) {
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut stream = stream;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).unwrap_or(0) == 0 {
            return;
        }
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
        let req = RecordedRequest { headers };
        let close = req
            .header("connection")
            .is_some_and(|v| v.eq_ignore_ascii_case("close"));
        reqs.lock().push(req);
        let r = resp.lock().clone();
        if !r.delay.is_zero() {
            thread::sleep(r.delay);
        }
        let reason = match r.status {
            200 => "OK",
            403 => "Forbidden",
            500 => "Internal Server Error",
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

#[derive(Default)]
struct RecordingHttpObserver {
    requests: Mutex<Vec<(String, String)>>,
}

impl RuntimeConfigHttpObserver for RecordingHttpObserver {
    fn observe_request(&self, url_for_metrics: &str, status_code: &str, _elapsed: Duration) {
        self.requests
            .lock()
            .push((url_for_metrics.to_string(), status_code.to_string()));
    }

    fn invalid_cluster_validation(&self, _msg: &str, _method: &str) {}
}

#[derive(Default)]
struct RecordingObserver {
    last_success: Mutex<Option<bool>>,
    applied_hashes: Mutex<Vec<Option<String>>>,
    last_sources: Mutex<Vec<RuntimeConfigSourceStatus>>,
}

impl RuntimeConfigObserver for RecordingObserver {
    fn on_load(&self, event: &RuntimeConfigLoadEvent<'_>) {
        *self.last_success.lock() = Some(event.result.is_ok());
        self.applied_hashes
            .lock()
            .push(event.applied_hash.map(str::to_string));
        *self.last_sources.lock() = event.sources.to_vec();
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct TestLimits {
    #[serde(default)]
    limit1: i64,
    #[serde(default)]
    limit2: i64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct TestOverrides {
    #[serde(default)]
    overrides: BTreeMap<String, Option<TestLimits>>,
}

fn test_load_overrides(
    default_limits: Option<TestLimits>,
) -> RuntimeConfigLoader<TestOverrides> {
    Arc::new(move |bytes| {
        let val: serde_yaml::Value =
            serde_yaml::from_slice(bytes).map_err(|e| e.to_string())?;
        if val.is_null() {
            return Ok(TestOverrides::default());
        }
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct RawOverrides {
            #[serde(default)]
            overrides: BTreeMap<String, Option<serde_yaml::Value>>,
        }
        let raw: RawOverrides = serde_yaml::from_value(val).map_err(|e| e.to_string())?;
        let mut out = TestOverrides::default();
        for (k, v) in raw.overrides {
            let Some(v) = v else {
                out.overrides.insert(k, None);
                continue;
            };
            let mut lim = default_limits.clone().unwrap_or_default();
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Patch {
                limit1: Option<i64>,
                limit2: Option<i64>,
            }
            let patch: Patch = serde_yaml::from_value(v).map_err(|e| e.to_string())?;
            if let Some(l1) = patch.limit1 {
                lim.limit1 = l1;
            }
            if let Some(l2) = patch.limit2 {
                lim.limit2 = l2;
            }
            out.overrides.insert(k, Some(lim));
        }
        Ok(out)
    })
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

fn write_value_to_file(path: &std::path::Path, v: i64) {
    let tmp = path.with_extension("yaml.tmp");
    std::fs::write(&tmp, format!("value: {v}\n")).unwrap();
    std::fs::rename(&tmp, path).unwrap();
}

fn cfg_with(files: Vec<String>, period: Duration) -> RuntimeConfigConfig {
    RuntimeConfigConfig {
        file: StringSliceCSV(files),
        period: GoDuration(period.as_nanos() as i64),
        ..Default::default()
    }
}

// =============================================================================================
// config_source_test.go (4 tests)
// =============================================================================================

// Upstream: github.com/grafana/dskit/runtimeconfig/config_source_test.go::TestParseConfigSource
#[test]
fn test_parse_config_source() {
    let cases: &[(&str, &str, &[SourceParameter], &str)] = &[
        ("/etc/overrides.yaml", "/etc/overrides.yaml", &[], ""),
        ("http://config-server/overrides", "http://config-server/overrides", &[], ""),
        (
            "http://config-server/overrides;optional-on-startup",
            "http://config-server/overrides",
            &[SourceParameter::OptionalOnStartup],
            "",
        ),
        (
            "/etc/overrides.yaml;optional-keep-last-value-on-failure",
            "/etc/overrides.yaml",
            &[SourceParameter::OptionalKeepLastValueOnFailure],
            "",
        ),
        ("http://[::1]:8080/overrides", "http://[::1]:8080/overrides", &[], ""),
        (
            "http://[::1]:8080/overrides;optional-on-startup",
            "http://[::1]:8080/overrides",
            &[SourceParameter::OptionalOnStartup],
            "",
        ),
        (
            "http://config-server/overrides.yaml;jsessionid=ABC",
            "http://config-server/overrides.yaml;jsessionid=ABC",
            &[],
            "",
        ),
        (
            "http://config-server/overrides.yaml;jsessionid=ABC;optional-keep-last-value-on-failure",
            "http://config-server/overrides.yaml;jsessionid=ABC",
            &[SourceParameter::OptionalKeepLastValueOnFailure],
            "",
        ),
        ("/etc/overrides.yaml;optional", "/etc/overrides.yaml;optional", &[], ""),
        ("http://config-server/overrides;v2", "http://config-server/overrides;v2", &[], ""),
        ("/etc/overrides.yaml;café", "/etc/overrides.yaml;café", &[], ""),
        ("/etc/overrides.yaml;", "/etc/overrides.yaml;", &[], ""),
        (
            "/etc/overrides.yaml;optional-on-startup;optional-keep-last-value-on-failure",
            "",
            &[],
            "more than one parameter",
        ),
        (
            "/etc/overrides.yaml;optional-keep-last-value-on-failure;optional-on-startup",
            "",
            &[],
            "more than one parameter",
        ),
        (";optional-on-startup", "", &[], "has no path"),
        (
            ";optional-on-startup;optional-keep-last-value-on-failure",
            "",
            &[],
            "has no path",
        ),
    ];

    for &(entry, want_path, want_params, want_err) in cases {
        let res = parse_config_source_entry(entry);
        if !want_err.is_empty() {
            let err = res.unwrap_err().to_string();
            assert!(err.contains(want_err), "entry={entry:?} err={err:?}");
        } else {
            let cs = res.unwrap();
            assert_eq!(cs.path, want_path, "entry={entry:?}");
            assert_eq!(cs.parameters.as_slice(), want_params, "entry={entry:?}");
        }
    }
}

// Upstream: github.com/grafana/dskit/runtimeconfig/config_source_test.go::TestAssignSourceIDs
#[test]
fn test_assign_source_ids() {
    let cases: &[(&[&str], &[&str], &str)] = &[
        (&["/etc/overrides.yaml"], &["/etc/overrides.yaml"], ""),
        (
            &["http://config-server:8080/overrides.yaml"],
            &["http://config-server:8080/overrides.yaml"],
            "",
        ),
        (
            &["https://apikey:@config-server/overrides"],
            &["https://config-server/overrides"],
            "",
        ),
        (
            &["http://config-server/overrides?token=secret#secret"],
            &["http://config-server/overrides"],
            "",
        ),
        (
            &["http://config-server/overrides?"],
            &["http://config-server/overrides"],
            "",
        ),
        (
            &[
                "http://config-server/overrides?tenant=a",
                "http://config-server/overrides?tenant=b",
            ],
            &[
                "http://config-server/overrides#0",
                "http://config-server/overrides#1",
            ],
            "",
        ),
        (
            &[
                "http://config-server/a?x=1",
                "http://config-server/a?x=2",
                "/etc/overrides.yaml",
            ],
            &[
                "http://config-server/a#0",
                "http://config-server/a#1",
                "/etc/overrides.yaml",
            ],
            "",
        ),
        (
            &["/etc/overrides.yaml", "/etc/overrides.yaml"],
            &["/etc/overrides.yaml#0", "/etc/overrides.yaml#1"],
            "",
        ),
        (
            &["http://config-server:not-a-port/overrides"],
            &[],
            "parse runtime config URL",
        ),
        (
            &["/etc/o.yaml", "/etc/o.yaml", "/etc/o.yaml#0"],
            &[],
            r#"both report as "/etc/o.yaml#0""#,
        ),
    ];

    for &(paths, want_ids, want_err) in cases {
        let mut sources: Vec<ParsedConfigSource> = paths
            .iter()
            .map(|p| ParsedConfigSource {
                path: (*p).to_string(),
                parameters: Vec::new(),
                source_id: String::new(),
            })
            .collect();
        let res = assign_config_source_ids(&mut sources);
        if !want_err.is_empty() {
            let err = res.unwrap_err().to_string();
            assert!(err.contains(want_err), "paths={paths:?} err={err:?}");
        } else {
            res.unwrap();
            let ids: Vec<String> = sources.into_iter().map(|s| s.source_id).collect();
            let want: Vec<String> = want_ids.iter().map(|s| (*s).to_string()).collect();
            assert_eq!(ids, want);
        }
    }
}

// Upstream: github.com/grafana/dskit/runtimeconfig/config_source_test.go::TestSourceTolerates
#[test]
fn test_source_tolerates() {
    assert!(SourceParameter::OptionalOnStartup.tolerates_failure(true));
    assert!(!SourceParameter::OptionalOnStartup.tolerates_failure(false));
    assert!(!SourceParameter::OptionalOnStartup.keeps_last_value_on_failure());

    assert!(SourceParameter::OptionalKeepLastValueOnFailure.tolerates_failure(true));
    assert!(SourceParameter::OptionalKeepLastValueOnFailure.tolerates_failure(false));
    assert!(SourceParameter::OptionalKeepLastValueOnFailure.keeps_last_value_on_failure());
}

// Upstream: github.com/grafana/dskit/runtimeconfig/config_source_test.go::TestSourceParametersEmpty
#[test]
fn test_source_parameters_empty() {
    let cs = parse_config_source_entry("/etc/overrides.yaml").unwrap();
    assert!(cs.parameters.is_empty());
    assert!(!cs.parameters.iter().any(|p| p.tolerates_failure(true)));
    assert!(!cs.parameters.iter().any(|p| p.tolerates_failure(false)));
    assert!(!cs.parameters.iter().any(|p| p.keeps_last_value_on_failure()));
}

// =============================================================================================
// provider_test.go (6 tests)
// =============================================================================================

// Upstream: github.com/grafana/dskit/runtimeconfig/provider_test.go::TestIsURL
#[test]
fn test_is_url() {
    for (path, want) in [
        ("http://example.com/config.yaml", true),
        ("https://example.com/config.yaml", true),
        ("HTTP://EXAMPLE.COM/config.yaml", false),
        ("/etc/config.yaml", false),
        ("config.yaml", false),
        ("", false),
        ("ftp://example.com/config.yaml", false),
    ] {
        assert_eq!(is_url(path), want, "path={path:?}");
    }
}

// Upstream: github.com/grafana/dskit/runtimeconfig/provider_test.go::TestHTTPProvider_Success
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_http_provider_success() {
    let body = "overrides:\n  user1:\n    limit1: 100\n";
    let srv = MockHttpServer::start(MockHttpResponse::ok(body));
    let url = srv.url("/config.yaml");
    let obs = Arc::new(RecordingHttpObserver::default());
    let p = HttpProvider::new(&url, &url, Duration::from_secs(5), false, "", Some(obs.clone()))
        .unwrap();

    let data = p.read_async().await.unwrap();
    assert_eq!(String::from_utf8(data).unwrap(), body);
    assert_eq!(p.name(), url);
    assert_eq!(
        srv.requests.lock()[0].header("user-agent"),
        Some("dskit-runtimeconfig")
    );
    assert_eq!(obs.requests.lock().len(), 1);
    assert_eq!(obs.requests.lock()[0].1, "200");
}

// Upstream: github.com/grafana/dskit/runtimeconfig/provider_test.go::TestHTTPProvider_ServerError
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_http_provider_server_error() {
    let srv = MockHttpServer::start(MockHttpResponse {
        status: 500,
        ..MockHttpResponse::ok("")
    });
    let url = srv.url("/config.yaml");
    let obs = Arc::new(RecordingHttpObserver::default());
    let p = HttpProvider::new(&url, &url, Duration::from_secs(5), false, "", Some(obs.clone()))
        .unwrap();

    let err = p.read_async().await.unwrap_err();
    assert!(err.to_string().contains("HTTP 500"), "{err}");
    assert_eq!(err.status_code(), Some(500));
    assert_eq!(obs.requests.lock().len(), 1);
    assert_eq!(obs.requests.lock()[0].1, "500");
}

// Upstream: github.com/grafana/dskit/runtimeconfig/provider_test.go::TestHTTPProvider_NonOKStatus
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_http_provider_non_ok_status() {
    let srv = MockHttpServer::start(MockHttpResponse {
        status: 403,
        ..MockHttpResponse::ok("")
    });
    let url = srv.url("/config.yaml");
    let p = HttpProvider::new(&url, &url, Duration::from_secs(5), false, "", None).unwrap();

    let err = p.read_async().await.unwrap_err();
    assert!(err.to_string().contains("HTTP 403"), "{err}");
    assert_eq!(
        err,
        HttpProviderError::HttpStatus {
            status_code: 403,
            url,
        }
    );
}

// Upstream: github.com/grafana/dskit/runtimeconfig/provider_test.go::TestHTTPProvider_Timeout
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_http_provider_timeout() {
    let srv = MockHttpServer::start(MockHttpResponse {
        delay: Duration::from_secs(2),
        ..MockHttpResponse::ok("ok")
    });
    let url = srv.url("/config.yaml");
    let obs = Arc::new(RecordingHttpObserver::default());
    let p = HttpProvider::new(
        &url,
        &url,
        Duration::from_millis(50),
        true,
        "",
        Some(obs.clone()),
    )
    .unwrap();

    let err = p.read_async().await.unwrap_err();
    assert!(err.status_code().is_none(), "{err}");
    assert_eq!(obs.requests.lock().len(), 1);
    assert_eq!(obs.requests.lock()[0].1, "error");
}

// Upstream: github.com/grafana/dskit/runtimeconfig/provider_test.go::TestHTTPProvider_ContextCanceled
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_http_provider_context_canceled() {
    let srv = MockHttpServer::start(MockHttpResponse {
        delay: Duration::from_secs(2),
        ..MockHttpResponse::ok("ok")
    });
    let url = srv.url("/config.yaml");
    let p = HttpProvider::new(&url, &url, Duration::from_secs(5), true, "", None).unwrap();

    let res = tokio::select! {
        biased;
        _ = std::future::ready(()) => Err("canceled"),
        _ = p.read_async() => Ok(()),
    };
    assert!(res.is_err());
}

// =============================================================================================
// manager_test.go part 1 (16 tests)
// =============================================================================================

// Upstream: github.com/grafana/dskit/runtimeconfig/manager_test.go::TestNewOverridesManager
#[test]
fn test_new_overrides_manager() {
    let dir = tempfile::tempdir().unwrap();
    let f = dir.path().join("o.yaml");
    std::fs::write(&f, "overrides:\n  user1:\n    limit2: 150\n").unwrap();

    let mgr = GenericRuntimeConfigManager::new(GenericRuntimeConfigOptions {
        cfg: cfg_with(vec![f.to_string_lossy().into()], Duration::from_secs(1)),
        preprocessor: None,
        loader: Some(test_load_overrides(Some(TestLimits {
            limit1: 100,
            limit2: 0,
        }))),
        map_loader: None,
        observer: None,
        http_observer: None,
    })
    .unwrap();

    mgr.start().unwrap();
    mgr.stop();
    let conf = mgr.get_config().unwrap();
    assert_eq!(conf.overrides["user1"].as_ref().unwrap().limit2, 150);
    assert_eq!(conf.overrides["user1"].as_ref().unwrap().limit1, 100);
}

// Upstream: github.com/grafana/dskit/runtimeconfig/manager_test.go::TestManagerGzip
#[test]
fn test_manager_gzip() {
    let write_cfg = |dir: &tempfile::TempDir, name: &str, gzipped: bool| -> String {
        let p = dir.path().join(name);
        let content = b"overrides:\n  user1:\n    limit2: 150\n";
        if gzipped {
            let mut enc = GzEncoder::new(Vec::new(), Compression::default());
            enc.write_all(content).unwrap();
            std::fs::write(&p, enc.finish().unwrap()).unwrap();
        } else {
            std::fs::write(&p, content).unwrap();
        }
        p.to_string_lossy().into_owned()
    };

    let dir = tempfile::tempdir().unwrap();
    // gzipped with .gz extension should succeed
    {
        let file = write_cfg(&dir, "overrides.yaml.gz", true);
        let mgr = GenericRuntimeConfigManager::new(GenericRuntimeConfigOptions {
            cfg: cfg_with(vec![file], Duration::from_secs(1)),
            preprocessor: None,
            loader: Some(test_load_overrides(Some(TestLimits {
                limit1: 100,
                limit2: 0,
            }))),
            map_loader: None,
            observer: None,
            http_observer: None,
        })
        .unwrap();
        mgr.start().unwrap();
        let conf = mgr.get_config().unwrap();
        assert_eq!(conf.overrides["user1"].as_ref().unwrap().limit2, 150);
        mgr.stop();
    }

    // non-gzipped with .gz extension should fail
    {
        let file = write_cfg(&dir, "bad.yaml.gz", false);
        let mgr = GenericRuntimeConfigManager::new(GenericRuntimeConfigOptions {
            cfg: cfg_with(vec![file], Duration::from_secs(1)),
            preprocessor: None,
            loader: Some(test_load_overrides(None)),
            map_loader: None,
            observer: None,
            http_observer: None,
        })
        .unwrap();
        let err = mgr.start().unwrap_err().to_string();
        assert!(err.contains("gzip"), "{err}");
    }

    // gzipped without .gz extension should mention that in the error
    {
        let file = write_cfg(&dir, "overrides.yaml", true);
        let mgr = GenericRuntimeConfigManager::new(GenericRuntimeConfigOptions {
            cfg: cfg_with(vec![file], Duration::from_secs(1)),
            preprocessor: None,
            loader: Some(test_load_overrides(None)),
            map_loader: None,
            observer: None,
            http_observer: None,
        })
        .unwrap();
        let err = mgr.start().unwrap_err().to_string();
        assert!(
            err.contains("file looks gzipped but doesn't have a .gz extension"),
            "{err}"
        );
    }
}

// Upstream: github.com/grafana/dskit/runtimeconfig/manager_test.go::TestOverridesManagerMultipleFilesAppend
#[test]
fn test_overrides_manager_multiple_files_append() {
    let dir = tempfile::tempdir().unwrap();
    let contents = [
        "overrides:\n  user1:\n    limit1: 101\n",
        "overrides:\n  user1:\n    limit2: 102\n",
        "overrides:\n  user2:\n    limit1: 103\n",
        "overrides:\n  user2:\n    limit2: 104\n",
    ];
    let paths: Vec<String> = contents
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let p = dir.path().join(format!("f{i}.yaml"));
            std::fs::write(&p, c).unwrap();
            p.to_string_lossy().into_owned()
        })
        .collect();

    let mgr = GenericRuntimeConfigManager::new(GenericRuntimeConfigOptions {
        cfg: cfg_with(paths, Duration::from_secs(1)),
        preprocessor: None,
        loader: Some(test_load_overrides(None)),
        map_loader: None,
        observer: None,
        http_observer: None,
    })
    .unwrap();
    mgr.start().unwrap();
    mgr.stop();
    let conf = mgr.get_config().unwrap();
    assert_eq!(conf.overrides["user1"].as_ref().unwrap().limit1, 101);
    assert_eq!(conf.overrides["user1"].as_ref().unwrap().limit2, 102);
    assert_eq!(conf.overrides["user2"].as_ref().unwrap().limit1, 103);
    assert_eq!(conf.overrides["user2"].as_ref().unwrap().limit2, 104);
}

// Upstream: github.com/grafana/dskit/runtimeconfig/manager_test.go::TestOverridesManagerMultipleFilesWithOverrides
#[test]
fn test_overrides_manager_multiple_files_with_overrides() {
    let dir = tempfile::tempdir().unwrap();
    let f1 = dir.path().join("f1.yaml");
    let f2 = dir.path().join("f2.yaml");
    std::fs::write(&f1, "overrides:\n  user1:\n    limit1: 100\n").unwrap();
    std::fs::write(&f2, "overrides:\n  user1:\n    limit1: 1234\n").unwrap();

    let mgr = GenericRuntimeConfigManager::new(GenericRuntimeConfigOptions {
        cfg: cfg_with(
            vec![f1.to_string_lossy().into(), f2.to_string_lossy().into()],
            Duration::from_secs(1),
        ),
        preprocessor: None,
        loader: Some(test_load_overrides(None)),
        map_loader: None,
        observer: None,
        http_observer: None,
    })
    .unwrap();
    mgr.start().unwrap();
    mgr.stop();
    let conf = mgr.get_config().unwrap();
    assert_eq!(conf.overrides["user1"].as_ref().unwrap().limit1, 1234);
}

// Upstream: github.com/grafana/dskit/runtimeconfig/manager_test.go::TestOverridesManagerMapLoader
#[test]
fn test_overrides_manager_map_loader() {
    let dir = tempfile::tempdir().unwrap();
    let f1 = dir.path().join("f1.yaml");
    let f2 = dir.path().join("f2.yaml");
    std::fs::write(&f1, "overrides:\n  user1:\n    limit1: 101\n").unwrap();
    std::fs::write(&f2, "overrides:\n  user2:\n    limit2: 204\n").unwrap();

    let obs = Arc::new(RecordingObserver::default());
    let mgr = GenericRuntimeConfigManager::new(GenericRuntimeConfigOptions {
        cfg: cfg_with(
            vec![f1.to_string_lossy().into(), f2.to_string_lossy().into()],
            Duration::from_secs(1),
        ),
        preprocessor: None,
        loader: Some(Arc::new(|_| {
            Err("Loader should not be called when MapLoader is set".into())
        })),
        map_loader: Some(Arc::new(|m| {
            serde_yaml::from_value(serde_yaml::Value::Mapping(m.clone()))
                .map_err(|e| e.to_string())
        })),
        observer: Some(obs.clone()),
        http_observer: None,
    })
    .unwrap();
    mgr.start().unwrap();
    let conf: TestOverrides = mgr.get_config().unwrap();
    assert_eq!(conf.overrides["user1"].as_ref().unwrap().limit1, 101);
    assert_eq!(conf.overrides["user2"].as_ref().unwrap().limit2, 204);
    assert!(mgr.config_hash().is_some());
    mgr.stop();

    // Hash preserves provider order
    let w1 = dir.path().join("w1.yaml");
    let w2 = dir.path().join("w2.yaml");
    std::fs::write(&w1, "winner: first\n").unwrap();
    std::fs::write(&w2, "winner: second\n").unwrap();

    let load = |paths: Vec<String>| -> (String, String) {
        let m = GenericRuntimeConfigManager::new(GenericRuntimeConfigOptions {
            cfg: cfg_with(paths, Duration::from_secs(1)),
            preprocessor: None,
            loader: None,
            map_loader: Some(Arc::new(|map| {
                Ok(map
                    .get("winner")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string())
            })),
            observer: None,
            http_observer: None,
        })
        .unwrap();
        m.start().unwrap();
        m.reload().unwrap();
        (m.get_config().unwrap(), m.config_hash().unwrap())
    };
    let (fwd_cfg, fwd_hash) = load(vec![w1.to_string_lossy().into(), w2.to_string_lossy().into()]);
    let (rev_cfg, rev_hash) = load(vec![w2.to_string_lossy().into(), w1.to_string_lossy().into()]);
    assert_eq!(fwd_cfg, "second");
    assert_eq!(rev_cfg, "first");
    assert_ne!(fwd_hash, rev_hash);
}

// Upstream: github.com/grafana/dskit/runtimeconfig/manager_test.go::TestOverridesManagerMultipleIncompatibleFiles
#[test]
fn test_overrides_manager_multiple_incompatible_files() {
    let dir = tempfile::tempdir().unwrap();
    let f1 = dir.path().join("f1.yaml");
    let f2 = dir.path().join("f2.yaml");
    std::fs::write(&f1, "overrides:\n  \"123\":\n    limit1: 100\n").unwrap();
    std::fs::write(&f2, "overrides:\n  456:\n    limit1: 1234\n").unwrap();

    let mgr = GenericRuntimeConfigManager::new(GenericRuntimeConfigOptions {
        cfg: cfg_with(
            vec![f1.to_string_lossy().into(), f2.to_string_lossy().into()],
            Duration::from_secs(1),
        ),
        preprocessor: None,
        loader: Some(test_load_overrides(None)),
        map_loader: None,
        observer: None,
        http_observer: None,
    })
    .unwrap();
    let err = mgr.start().unwrap_err().to_string();
    assert!(
        err.contains(
            r#"conflicting types for ".overrides": map[string]interface {} != map[interface {}]interface {}"#
        ),
        "{err}"
    );
}

// Upstream: github.com/grafana/dskit/runtimeconfig/manager_test.go::TestOverridesManagerMultipleFilesWithEmptyFile
#[test]
fn test_overrides_manager_multiple_files_with_empty_file() {
    let dir = tempfile::tempdir().unwrap();
    let f1 = dir.path().join("f1.yaml");
    let f2 = dir.path().join("f2.yaml");
    std::fs::write(&f1, "overrides:\n  user1:\n    limit1: 100\n").unwrap();
    std::fs::write(&f2, "").unwrap();

    let mgr = GenericRuntimeConfigManager::new(GenericRuntimeConfigOptions {
        cfg: cfg_with(
            vec![f1.to_string_lossy().into(), f2.to_string_lossy().into()],
            Duration::from_secs(1),
        ),
        preprocessor: None,
        loader: Some(test_load_overrides(None)),
        map_loader: None,
        observer: None,
        http_observer: None,
    })
    .unwrap();
    mgr.start().unwrap();
    mgr.stop();
    let conf = mgr.get_config().unwrap();
    assert_eq!(conf.overrides["user1"].as_ref().unwrap().limit1, 100);
}

// Upstream: github.com/grafana/dskit/runtimeconfig/manager_test.go::TestOverridesManagerMultipleFilesWithNilValues
#[test]
fn test_overrides_manager_multiple_files_with_nil_values() {
    let dir = tempfile::tempdir().unwrap();
    let f1 = dir.path().join("f1.yaml");
    let f2 = dir.path().join("f2.yaml");
    std::fs::write(
        &f1,
        "overrides:\n  \"123\": null\n  \"456\":\n    limit1: 200\n  \"789\": null\n  \"other\": {}\n",
    )
    .unwrap();
    std::fs::write(
        &f2,
        "overrides:\n  \"123\":\n    limit1: 200\n  \"456\": null\n  \"789\": {}\n  \"other\": null\n",
    )
    .unwrap();

    let mgr = GenericRuntimeConfigManager::new(GenericRuntimeConfigOptions {
        cfg: cfg_with(
            vec![f1.to_string_lossy().into(), f2.to_string_lossy().into()],
            Duration::from_secs(1),
        ),
        preprocessor: None,
        loader: Some(test_load_overrides(Some(TestLimits {
            limit1: 100,
            limit2: 0,
        }))),
        map_loader: None,
        observer: None,
        http_observer: None,
    })
    .unwrap();
    mgr.start().unwrap();
    let conf = mgr.get_config().unwrap();
    assert_eq!(conf.overrides["123"].as_ref().unwrap().limit1, 200);
    assert_eq!(conf.overrides["456"].as_ref().unwrap().limit1, 200);
    assert_eq!(conf.overrides["789"].as_ref().unwrap().limit1, 100);
    assert_eq!(conf.overrides["other"].as_ref().unwrap().limit1, 100);
}

// Upstream: github.com/grafana/dskit/runtimeconfig/manager_test.go::TestOverridesManagerPreprocessor
#[test]
fn test_overrides_manager_preprocessor() {
    let dir = tempfile::tempdir().unwrap();
    let f1 = dir.path().join("f1.yaml");
    let f2 = dir.path().join("f2.yaml");
    std::fs::write(&f1, "overrides:\n  user1:\n    limit1: ${VALUE}\n").unwrap();
    std::fs::write(&f2, "").unwrap();

    let altered = b"overrides:\n  user1:\n    limit1: 200\n".to_vec();
    let mgr = GenericRuntimeConfigManager::new(GenericRuntimeConfigOptions {
        cfg: cfg_with(
            vec![f1.to_string_lossy().into(), f2.to_string_lossy().into()],
            Duration::from_secs(1),
        ),
        preprocessor: Some(Arc::new(move |_| Ok(altered.clone()))),
        loader: Some(test_load_overrides(None)),
        map_loader: None,
        observer: None,
        http_observer: None,
    })
    .unwrap();
    mgr.start().unwrap();
    mgr.stop();
    assert_eq!(
        mgr.get_config().unwrap().overrides["user1"]
            .as_ref()
            .unwrap()
            .limit1,
        200
    );
}

// Upstream: github.com/grafana/dskit/runtimeconfig/manager_test.go::TestOverridesManagerFailingPreprocessor
#[test]
fn test_overrides_manager_failing_preprocessor() {
    let dir = tempfile::tempdir().unwrap();
    let f1 = dir.path().join("f1.yaml");
    std::fs::write(&f1, "overrides:\n  user1:\n    limit1: ${VALUE}\n").unwrap();

    let mgr = GenericRuntimeConfigManager::new(GenericRuntimeConfigOptions {
        cfg: cfg_with(vec![f1.to_string_lossy().into()], Duration::from_secs(1)),
        preprocessor: Some(Arc::new(|_| Err("some preprocessor error".into()))),
        loader: Some(test_load_overrides(None)),
        map_loader: None,
        observer: None,
        http_observer: None,
    })
    .unwrap();
    let err = mgr.start().unwrap_err().to_string();
    assert!(err.contains("preprocess"), "{err}");
    assert!(err.contains("some preprocessor error"), "{err}");
}

// Upstream: github.com/grafana/dskit/runtimeconfig/manager_test.go::TestManager_ListenerWithDefaultLimits
#[test]
fn test_manager_listener_with_default_limits() {
    let dir = tempfile::tempdir().unwrap();
    let f = dir.path().join("o.yaml");
    let cfg1 = b"overrides:\n    user1:\n        limit2: 150\n";
    std::fs::write(&f, cfg1).unwrap();

    let obs = Arc::new(RecordingObserver::default());
    let mgr = GenericRuntimeConfigManager::new(GenericRuntimeConfigOptions {
        cfg: cfg_with(vec![f.to_string_lossy().into()], Duration::from_millis(50)),
        preprocessor: None,
        loader: Some(test_load_overrides(Some(TestLimits {
            limit1: 100,
            limit2: 0,
        }))),
        map_loader: None,
        observer: Some(obs.clone()),
        http_observer: None,
    })
    .unwrap();
    mgr.start().unwrap();
    assert_eq!(mgr.config_hash(), Some(sha256_hex(cfg1)));
    assert_eq!(*obs.last_success.lock(), Some(true));

    let (_id, mut rx) = mgr.create_listener_channel(1);
    mgr.start_reloader();

    let cfg2 = b"overrides:\n    user2:\n        limit2: 200\n";
    std::fs::write(&f, cfg2).unwrap();

    let new_val = rx.blocking_recv().expect("listener value");
    assert_eq!(new_val.overrides["user2"].as_ref().unwrap().limit2, 200);
    assert_eq!(new_val.overrides["user2"].as_ref().unwrap().limit1, 100);
    assert_eq!(mgr.config_hash(), Some(sha256_hex(cfg2)));

    mgr.stop();
    assert!(mgr.get_config().is_some());
}

// Upstream: github.com/grafana/dskit/runtimeconfig/manager_test.go::TestManager_ListenerChannel
#[test]
fn test_manager_listener_channel() {
    let dir = tempfile::tempdir().unwrap();
    let f = dir.path().join("val.yaml");
    write_value_to_file(&f, 555);

    let mgr = GenericRuntimeConfigManager::new(GenericRuntimeConfigOptions {
        cfg: cfg_with(vec![f.to_string_lossy().into()], Duration::from_millis(50)),
        preprocessor: None,
        loader: Some(value_loader()),
        map_loader: None,
        observer: None,
        http_observer: None,
    })
    .unwrap();

    let (id, mut rx) = mgr.create_listener_channel(1);
    mgr.start().unwrap();
    mgr.start_reloader();

    assert_eq!(rx.blocking_recv().unwrap(), ValueCfg { value: 555 });

    write_value_to_file(&f, 1111);
    assert_eq!(rx.blocking_recv().unwrap(), ValueCfg { value: 1111 });

    mgr.close_listener_channel(id);
    assert!(rx.blocking_recv().is_none());
    mgr.stop();
}

// Upstream: github.com/grafana/dskit/runtimeconfig/manager_test.go::TestManager_StopClosesListenerChannels
#[test]
fn test_manager_stop_closes_listener_channels() {
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
    mgr.start().unwrap();

    let (_id, mut rx) = mgr.create_listener_channel(0);
    mgr.stop();
    assert!(rx.blocking_recv().is_none());
}

// Upstream: github.com/grafana/dskit/runtimeconfig/manager_test.go::TestManager_ShouldFastFailOnInvalidConfigAtStartup
#[test]
fn test_manager_should_fast_fail_on_invalid_config_at_startup() {
    let dir = tempfile::tempdir().unwrap();
    let f = dir.path().join("invalid.yaml");
    std::fs::write(&f, "!invalid!").unwrap();

    let mgr = GenericRuntimeConfigManager::new(GenericRuntimeConfigOptions {
        cfg: cfg_with(vec![f.to_string_lossy().into()], Duration::from_secs(1)),
        preprocessor: None,
        loader: Some(test_load_overrides(None)),
        map_loader: None,
        observer: None,
        http_observer: None,
    })
    .unwrap();
    assert!(mgr.start().is_err());
}

// Upstream: github.com/grafana/dskit/runtimeconfig/manager_test.go::TestManager_ReloadMetricAfterBadConfigRecovery
#[test]
fn test_manager_reload_metric_after_bad_config_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let f = dir.path().join("valid.yaml");
    let valid_cfg = b"overrides:\n    user1:\n        limit2: 150\n";
    std::fs::write(&f, valid_cfg).unwrap();

    let obs = Arc::new(RecordingObserver::default());
    let mgr = GenericRuntimeConfigManager::new(GenericRuntimeConfigOptions {
        cfg: cfg_with(vec![f.to_string_lossy().into()], Duration::from_secs(60)),
        preprocessor: None,
        loader: Some(test_load_overrides(None)),
        map_loader: None,
        observer: Some(obs.clone()),
        http_observer: None,
    })
    .unwrap();
    mgr.start().unwrap();
    assert_eq!(*obs.last_success.lock(), Some(true));
    assert_eq!(mgr.config_hash(), Some(sha256_hex(valid_cfg)));

    std::fs::write(&f, b"invalid").unwrap();
    assert!(mgr.reload().is_err());
    assert_eq!(*obs.last_success.lock(), Some(false));
    assert_eq!(mgr.config_hash(), Some(sha256_hex(valid_cfg)));

    std::fs::write(&f, valid_cfg).unwrap();
    mgr.reload().unwrap();
    assert_eq!(*obs.last_success.lock(), Some(true));
    assert_eq!(mgr.config_hash(), Some(sha256_hex(valid_cfg)));
}

// Upstream: github.com/grafana/dskit/runtimeconfig/manager_test.go::TestManager_UnchangedFileDoesntTriggerReload
#[test]
fn test_manager_unchanged_file_doesnt_trigger_reload() {
    let dir = tempfile::tempdir().unwrap();
    let f = dir.path().join("val.yaml");
    std::fs::write(&f, "").unwrap();

    let load_counter = Arc::new(AtomicUsize::new(0));
    let lc = load_counter.clone();
    let mgr = GenericRuntimeConfigManager::new(GenericRuntimeConfigOptions {
        cfg: cfg_with(vec![f.to_string_lossy().into()], Duration::from_secs(60)),
        preprocessor: None,
        loader: Some(Arc::new(move |bytes| {
            lc.fetch_add(1, Ordering::SeqCst);
            value_loader()(bytes)
        })),
        map_loader: None,
        observer: None,
        http_observer: None,
    })
    .unwrap();

    let (_id, mut rx) = mgr.create_listener_channel(10);
    mgr.start().unwrap();
    mgr.reload().unwrap();
    assert_eq!(load_counter.load(Ordering::SeqCst), 1);

    for i in 0..3 {
        write_value_to_file(&f, i);
        mgr.reload().unwrap();
        mgr.reload().unwrap();
    }

    assert_eq!(load_counter.load(Ordering::SeqCst), 4);
    let mut received = 0;
    while rx.try_recv().is_ok() {
        received += 1;
    }
    assert_eq!(received, 4);
}
