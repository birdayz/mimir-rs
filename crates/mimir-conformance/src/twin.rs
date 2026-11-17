//! Shared twin-process harness: boot the unmodified Go `mimir` (`target/go-mimir`) and the
//! Rust `mimir` side by side with identical configuration and observe them from the outside
//! (HTTP, gRPC, exit status, logs, and a fake S3 endpoint).

use std::collections::BTreeMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use axum::body::Bytes;
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};

use crate::oracle::{ensure_go_oracle_built, go_mimir_bin, rust_mimir_release_bin, workspace_root};

/// Which implementation a process runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Impl {
    Go,
    Rust,
}

impl Impl {
    pub fn name(self) -> &'static str {
        match self {
            Impl::Go => "go",
            Impl::Rust => "rust",
        }
    }
}

/// Returns the binary for `which`, building it on first use: `target/go-mimir` via
/// `scripts/build-go-oracle.sh` if missing, and `target/release/mimir` always (incremental)
/// unless `MIMIR_CONFORMANCE_RUST_BIN` points at a prebuilt binary, so a stale Rust binary is
/// never exercised.
pub fn binary(which: Impl) -> PathBuf {
    static BUILT: OnceLock<Result<PathBuf, String>> = OnceLock::new();
    match which {
        Impl::Go => {
            ensure_go_oracle_built().expect("build target/go-mimir");
            go_mimir_bin()
        }
        Impl::Rust => BUILT
            .get_or_init(|| {
                if let Some(bin) = std::env::var_os("MIMIR_CONFORMANCE_RUST_BIN").filter(|v| !v.is_empty()) {
                    return Ok(PathBuf::from(bin));
                }
                let status = Command::new(std::env::var("CARGO").unwrap_or_else(|_| "cargo".into()))
                    .args(["build", "--release", "-p", "mimir-cmd"])
                    .current_dir(workspace_root())
                    .status()
                    .map_err(|e| format!("spawn cargo build --release -p mimir-cmd: {e}"))?;
                if !status.success() {
                    return Err(format!("cargo build --release -p mimir-cmd exited with {status}"));
                }
                Ok(rust_mimir_release_bin())
            })
            .clone()
            .unwrap_or_else(|e| panic!("{e}")),
    }
}

/// Hands out `n` distinct free TCP ports from [`crate::free_test_port`] (`15000..32000`,
/// below Linux's ephemeral range), so an outgoing connection of another process can't grab a
/// port between the check and the bind.
pub fn free_tcp_ports(n: usize) -> Vec<u16> {
    (0..n).map(|_| crate::free_test_port()).collect()
}

/// A child process with stdout+stderr captured; killed and reaped on drop.
pub struct Proc {
    pub name: String,
    child: Option<Child>,
    log: Arc<Mutex<Vec<u8>>>,
    readers: Vec<JoinHandle<()>>,
    exit: Option<ExitStatus>,
}

impl Proc {
    pub fn spawn(name: &str, bin: &Path, args: &[String], workdir: &Path) -> Self {
        Self::spawn_with_env(name, bin, args, workdir, &[])
    }

    pub fn spawn_with_env(
        name: &str,
        bin: &Path,
        args: &[String],
        workdir: &Path,
        env: &[(&str, &str)],
    ) -> Self {
        let mut cmd = Command::new(bin);
        cmd.args(args)
            .current_dir(workdir)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        for (k, v) in env {
            cmd.env(k, v);
        }
        let mut child = cmd
            .spawn()
            .unwrap_or_else(|e| panic!("spawn {name} ({}): {e}", bin.display()));
        let log = Arc::new(Mutex::new(Vec::new()));
        let mut readers = Vec::new();
        let pipes: Vec<Box<dyn Read + Send>> = vec![
            Box::new(child.stdout.take().expect("stdout")),
            Box::new(child.stderr.take().expect("stderr")),
        ];
        for mut pipe in pipes {
            let log = Arc::clone(&log);
            readers.push(std::thread::spawn(move || {
                let mut buf = [0u8; 8192];
                while let Ok(n) = pipe.read(&mut buf) {
                    if n == 0 {
                        break;
                    }
                    let mut g = log.lock().unwrap();
                    if g.len() < 4 * 1024 * 1024 {
                        g.extend_from_slice(&buf[..n]);
                    }
                }
            }));
        }
        Self {
            name: name.to_string(),
            child: Some(child),
            log,
            readers,
            exit: None,
        }
    }

    pub fn pid(&self) -> Option<u32> {
        self.child.as_ref().map(Child::id)
    }

    pub fn log(&self) -> String {
        String::from_utf8_lossy(&self.log.lock().unwrap()).into_owned()
    }

    pub fn try_wait(&mut self) -> Option<ExitStatus> {
        if self.exit.is_none() {
            if let Some(c) = self.child.as_mut() {
                self.exit = c.try_wait().ok().flatten();
            }
        }
        self.exit
    }

    pub fn signal(&self, sig: &str) {
        if let Some(pid) = self.pid() {
            let _ = Command::new("kill")
                .args([&format!("-{sig}"), &pid.to_string()])
                .status();
        }
    }

    pub fn sigterm(&self) {
        self.signal("TERM");
    }

    /// Waits for the process to exit; `None` if it is still running after `timeout`.
    pub fn wait_exit(&mut self, timeout: Duration) -> Option<ExitStatus> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(s) = self.try_wait() {
                for r in self.readers.drain(..) {
                    let _ = r.join();
                }
                return Some(s);
            }
            if Instant::now() >= deadline {
                return None;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

impl Drop for Proc {
    fn drop(&mut self) {
        if let Some(mut c) = self.child.take() {
            if c.try_wait().ok().flatten().is_none() {
                let _ = c.kill();
                let _ = c.wait();
            }
        }
        for r in self.readers.drain(..) {
            let _ = r.join();
        }
    }
}

/// Starts `bin -config.file=<dir>/mimir.yaml <extra...>` after writing `yaml` there.
pub fn start_mimir(which: Impl, name: &str, dir: &Path, yaml: &str, extra: &[String]) -> Proc {
    start_mimir_with_env(which, name, dir, yaml, extra, &[])
}

/// Like [`start_mimir`], with additional environment variables set on the child process.
pub fn start_mimir_with_env(
    which: Impl,
    name: &str,
    dir: &Path,
    yaml: &str,
    extra: &[String],
    env: &[(&str, &str)],
) -> Proc {
    std::fs::create_dir_all(dir).unwrap();
    let cfg = dir.join("mimir.yaml");
    std::fs::write(&cfg, yaml).unwrap();
    let mut args = vec![format!("-config.file={}", cfg.display())];
    args.extend(extra.iter().cloned());
    Proc::spawn_with_env(
        &format!("{}-{name}", which.name()),
        &binary(which),
        &args,
        dir,
        env,
    )
}

/// `GET http://127.0.0.1:{port}{path}` with a short timeout: `(status, headers, body)`.
pub async fn http_get(
    client: &reqwest::Client,
    port: u16,
    path: &str,
    accept_json: bool,
) -> Result<(u16, reqwest::header::HeaderMap, String), String> {
    let mut req = client
        .get(format!("http://127.0.0.1:{port}{path}"))
        .timeout(Duration::from_secs(2));
    if accept_json {
        req = req.header("Accept", "application/json");
    }
    let resp = req.send().await.map_err(|e| e.to_string())?;
    let status = resp.status().as_u16();
    let headers = resp.headers().clone();
    let body = resp.text().await.map_err(|e| e.to_string())?;
    Ok((status, headers, body))
}

/// Polls `/ready` until 200; panics with the process log on timeout or premature exit.
pub async fn wait_ready(client: &reqwest::Client, proc: &mut Proc, port: u16, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(s) = proc.try_wait() {
            panic!("{} exited ({s}) before becoming ready:\n{}", proc.name, proc.log());
        }
        let last = match http_get(client, port, "/ready", false).await {
            Ok((200, _, _)) => return,
            Ok((code, _, body)) => format!("{code} {body}"),
            Err(e) => e,
        };
        if Instant::now() >= deadline {
            panic!(
                "{} not ready within {timeout:?} (last: {last}):\n{}",
                proc.name,
                proc.log()
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// `GET /ingester/ring` (JSON): instance id -> (state, number of tokens).
pub async fn ingester_ring(
    client: &reqwest::Client,
    port: u16,
) -> Result<BTreeMap<String, (String, usize)>, String> {
    let (code, _, body) = http_get(client, port, "/ingester/ring", true).await?;
    if code != 200 {
        return Err(format!("{code} {body}"));
    }
    let v: serde_json::Value = serde_json::from_str(&body).map_err(|e| format!("{e}: {body}"))?;
    let mut out = BTreeMap::new();
    if let Some(shards) = v["shards"].as_array() {
        for s in shards {
            out.insert(
                s["id"].as_str().unwrap_or_default().to_string(),
                (
                    s["state"].as_str().unwrap_or_default().to_string(),
                    s["tokens"].as_array().map_or(0, Vec::len),
                ),
            );
        }
    }
    Ok(out)
}

/// `GET /services` (JSON): module -> state.
pub async fn services(client: &reqwest::Client, port: u16) -> Result<BTreeMap<String, String>, String> {
    let (code, _, body) = http_get(client, port, "/services", true).await?;
    if code != 200 {
        return Err(format!("{code} {body}"));
    }
    let v: serde_json::Value = serde_json::from_str(&body).map_err(|e| format!("{e}: {body}"))?;
    Ok(v["services"]
        .as_array()
        .map(|a| {
            a.iter()
                .map(|s| {
                    (
                        s["name"].as_str().unwrap_or_default().to_string(),
                        s["status"].as_str().unwrap_or_default().to_string(),
                    )
                })
                .collect()
        })
        .unwrap_or_default())
}

/// `grpc.health.v1.Health/Check` status (1 = SERVING, 2 = NOT_SERVING).
pub async fn grpc_health(port: u16) -> Result<i32, String> {
    use mimir_cmd::grpc::{HealthCheckRequest, HealthCheckResponse};
    let channel = tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{port}"))
        .map_err(|e| e.to_string())?
        .connect_timeout(Duration::from_secs(2))
        .timeout(Duration::from_secs(2))
        .connect()
        .await
        .map_err(|e| e.to_string())?;
    let mut grpc = tonic::client::Grpc::new(channel);
    grpc.ready().await.map_err(|e| e.to_string())?;
    let codec = tonic::codec::ProstCodec::<HealthCheckRequest, HealthCheckResponse>::default();
    let resp = grpc
        .unary(
            tonic::Request::new(HealthCheckRequest::default()),
            axum::http::uri::PathAndQuery::from_static("/grpc.health.v1.Health/Check"),
            codec,
        )
        .await
        .map_err(|e| e.to_string())?;
    Ok(resp.into_inner().status)
}

/// `(series name, labels)` of every exposed sample whose family belongs to the ring,
/// lifecycler or KV client instrumentation (values and the `le` label of histogram buckets are
/// not compared; `operation`/`status_code` only appear once requests happened).
pub fn ring_metric_series(exposition: &str) -> std::collections::BTreeSet<String> {
    let relevant = |name: &str| {
        (name.contains("ring_") && !name.contains("string_"))
            || name.contains("lifecycler")
            || name.contains("member_consul_heartbeats")
            || name.contains("kv_request_duration_seconds")
            || name.contains("shutdown_duration_seconds")
    };
    exposition
        .lines()
        .filter(|l| !l.starts_with('#') && !l.is_empty())
        .filter_map(|l| {
            let (series, _) = l.rsplit_once(' ')?;
            let (name, labels) = match series.split_once('{') {
                Some((n, rest)) => (n, rest.trim_end_matches('}')),
                None => (series, ""),
            };
            if !relevant(name) || name.starts_with("memberlist") {
                return None;
            }
            let mut kept: Vec<&str> = labels
                .split(',')
                .filter(|kv| !kv.is_empty() && !kv.starts_with("le=") && !kv.starts_with("operation=") && !kv.starts_with("status_code="))
                .collect();
            kept.sort_unstable();
            Some(format!("{name}{{{}}}", kept.join(",")))
        })
        .collect()
}

// ---------------------------------------------------------------------------------------------
// Fake S3 endpoint
// ---------------------------------------------------------------------------------------------

/// One request received by [`FakeS3`].
#[derive(Clone, Debug)]
pub struct S3Request {
    pub method: String,
    pub path: String,
    pub at: Instant,
}

type PutHook = Arc<dyn Fn(String) -> futures::future::BoxFuture<'static, ()> + Send + Sync>;

/// Minimal path-style S3 endpoint: every object is missing (`404 NoSuchKey`), listings are
/// empty and `PUT`s succeed (after an optional async hook, e.g. to observe the uploader while
/// the upload is in flight). Enough for Mimir's sanity check and block shipping.
pub struct FakeS3 {
    pub port: u16,
    requests: Arc<Mutex<Vec<S3Request>>>,
    _server: tokio::task::JoinHandle<()>,
}

impl FakeS3 {
    pub async fn start(port: u16, put_hook: Option<PutHook>) -> Self {
        let requests: Arc<Mutex<Vec<S3Request>>> = Arc::new(Mutex::new(Vec::new()));
        let reqs = Arc::clone(&requests);
        let app = axum::Router::new().fallback(move |method: Method, uri: Uri, _h: HeaderMap, _body: Bytes| {
            let reqs = Arc::clone(&reqs);
            let put_hook = put_hook.clone();
            async move {
                let path = uri.path().to_string();
                reqs.lock().unwrap().push(S3Request {
                    method: method.to_string(),
                    path: path.clone(),
                    at: Instant::now(),
                });
                fake_s3_response(method, &uri, &path, put_hook).await
            }
        });
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
            .await
            .expect("bind fake S3");
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Self {
            port,
            requests,
            _server: server,
        }
    }

    pub fn endpoint(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }

    pub fn requests(&self) -> Vec<S3Request> {
        self.requests.lock().unwrap().clone()
    }
}

async fn fake_s3_response(method: Method, uri: &Uri, path: &str, put_hook: Option<PutHook>) -> Response {
    let query = uri.query().unwrap_or_default();
    let xml = |code: StatusCode, body: String| {
        (code, [("Content-Type", "application/xml")], body).into_response()
    };
    if method == Method::GET && query.contains("location") {
        return xml(
            StatusCode::OK,
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><LocationConstraint xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">us-east-1</LocationConstraint>".to_string(),
        );
    }
    if method == Method::GET && (query.contains("list-type") || query.contains("prefix") || path.trim_matches('/').split('/').count() == 1) {
        let bucket = path.trim_matches('/').split('/').next().unwrap_or_default();
        return xml(
            StatusCode::OK,
            format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>{bucket}</Name><Prefix></Prefix><KeyCount>0</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated></ListBucketResult>"),
        );
    }
    match method {
        Method::PUT => {
            if let Some(hook) = put_hook {
                hook(path.to_string()).await;
            }
            (StatusCode::OK, [("ETag", "\"d41d8cd98f00b204e9800998ecf8427e\"")], "").into_response()
        }
        Method::HEAD => StatusCode::NOT_FOUND.into_response(),
        Method::DELETE => StatusCode::NO_CONTENT.into_response(),
        _ => xml(
            StatusCode::NOT_FOUND,
            format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message><Key>{path}</Key><RequestId>1</RequestId></Error>"),
        ),
    }
}
