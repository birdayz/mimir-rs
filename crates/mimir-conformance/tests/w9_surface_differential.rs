//! Wave 9 differential conformance of the externally observable server surface: the unmodified
//! Go `mimir` binary (`target/go-mimir`) and the candidate binary (`target/release/mimir`, or
//! `MIMIR_CONFORMANCE_RUST_BIN`) run side by side as live `-target=ingester` processes with
//! identical configuration, and their answers are diffed:
//!
//! - `/config` (and `?mode=diff`) secret masking (`flagext.Secret` → `********`)
//! - dual-stack listeners: the default (empty) listen address is reachable over `[::1]` and
//!   `127.0.0.1`, for HTTP and gRPC
//! - server TLS + mTLS (`RequireAndVerifyClientCert`) on HTTP and gRPC, and startup failure on a
//!   missing certificate
//! - dskit request instrumentation (`cortex_request_duration_seconds` route/method/status_code/ws
//!   labels and buckets), `process_*` families, `/metrics` content negotiation (text,
//!   OpenMetrics, delimited protobuf, gzip)
//! - `-target=all` (rejected by mimir-rs with an explicit error, never served ingester-only)
//! - `-help`, `-help-all`, `-version`
//! - gRPC status code, message and details of tenant (`x-scope-orgid`) and label matcher errors
//!   on all 14 `cortex.Ingester` RPCs
//!
//! Upstream behaviour: github.com/grafana/dskit/server/server.go, dskit/middleware/*,
//! github.com/grafana/mimir/pkg/api/*, cmd/mimir/main.go.
//!
//! Validate the harness itself with `MIMIR_CONFORMANCE_RUST_BIN=$PWD/target/go-mimir` (Go vs Go).

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use mimir_conformance::oracle::{ensure_go_oracle_built, go_mimir_bin, rust_mimir_release_bin, workspace_root};
use mimir_proto::httpgrpcpb::http_client::HttpClient;
use mimir_proto::httpgrpcpb::HttpRequest;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Identity};

// ---------------------------------------------------------------------------------------------
// Binaries & processes
// ---------------------------------------------------------------------------------------------

fn rust_bin_override() -> Option<PathBuf> {
    std::env::var_os("MIMIR_CONFORMANCE_RUST_BIN")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
}

/// Builds the Go binaries if missing and the candidate (incrementally, so a stale
/// `target/release/mimir` is never tested), once per test binary.
fn ensure_binaries() -> (PathBuf, PathBuf) {
    static BUILT: OnceLock<Result<(), String>> = OnceLock::new();
    let res = BUILT.get_or_init(|| {
        ensure_go_oracle_built().map_err(|e| format!("{e:#}"))?;
        if let Some(bin) = rust_bin_override() {
            eprintln!("using MIMIR_CONFORMANCE_RUST_BIN={}", bin.display());
            return Ok(());
        }
        let status = Command::new(std::env::var("CARGO").unwrap_or_else(|_| "cargo".into()))
            .args(["build", "--release", "-p", "mimir-cmd"])
            .current_dir(workspace_root())
            .status()
            .map_err(|e| format!("spawn cargo build: {e}"))?;
        if !status.success() {
            return Err(format!("cargo build --release -p mimir-cmd exited with {status}"));
        }
        Ok(())
    });
    if let Err(e) = res {
        panic!("building binaries: {e}");
    }
    (go_mimir_bin(), rust_bin_override().unwrap_or_else(rust_mimir_release_bin))
}

/// Whether the candidate is a Go Mimir (Go-vs-Go harness validation).
fn candidate_is_go(bin: &Path) -> bool {
    let out = Command::new(bin).arg("-version").output().expect("run -version");
    String::from_utf8_lossy(&out.stdout).contains("go version:       go")
}

/// Kills and reaps the child on drop; keeps its stderr for diagnostics.
struct Proc {
    child: Option<Child>,
    stderr: Arc<Mutex<Vec<u8>>>,
    reader: Option<JoinHandle<()>>,
}

impl Proc {
    fn spawn(name: &str, bin: &Path, args: &[String], dir: &Path) -> Self {
        let mut child = Command::new(bin)
            .args(args)
            .current_dir(dir)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap_or_else(|e| panic!("spawn {name} ({}): {e}", bin.display()));
        let stderr = Arc::new(Mutex::new(Vec::new()));
        let reader = child.stderr.take().map(|mut s| {
            let buf = Arc::clone(&stderr);
            std::thread::spawn(move || {
                let mut tmp = [0u8; 8192];
                while let Ok(n) = s.read(&mut tmp) {
                    if n == 0 {
                        break;
                    }
                    if let Ok(mut b) = buf.lock() {
                        if b.len() < 4 << 20 {
                            b.extend_from_slice(&tmp[..n]);
                        }
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

    fn stderr(&self) -> String {
        String::from_utf8_lossy(&self.stderr.lock().unwrap()).into_owned()
    }

    /// Waits for the process to exit on its own (startup failure).
    fn wait_exit(&mut self, timeout: Duration) -> Option<i32> {
        let deadline = Instant::now() + timeout;
        let child = self.child.as_mut()?;
        loop {
            if let Ok(Some(st)) = child.try_wait() {
                if let Some(r) = self.reader.take() {
                    let _ = r.join();
                }
                return st.code();
            }
            if Instant::now() > deadline {
                return None;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

impl Drop for Proc {
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

fn free_port() -> u16 {
    mimir_conformance::free_test_port()
}

// ---------------------------------------------------------------------------------------------
// HTTP via curl (IPv6 literals, TLS client certificates, raw headers)
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Default)]
struct CurlResp {
    /// 0 when no HTTP response was received.
    status: u16,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
}

fn curl(args: &[&str]) -> CurlResp {
    let dir = tempfile::tempdir().unwrap();
    let hdr = dir.path().join("h");
    let body = dir.path().join("b");
    let out = Command::new("curl")
        .args(["-s", "--max-time", "20", "-g", "-D"])
        .arg(&hdr)
        .arg("-o")
        .arg(&body)
        .args(["-w", "%{http_code}"])
        .args(args)
        .output()
        .expect("run curl");
    let status = String::from_utf8_lossy(&out.stdout).trim().parse().unwrap_or(0);
    let mut headers = BTreeMap::new();
    for line in fs::read_to_string(&hdr).unwrap_or_default().lines() {
        if let Some((k, v)) = line.split_once(':') {
            headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
        }
    }
    CurlResp {
        status,
        headers,
        body: fs::read(&body).unwrap_or_default(),
    }
}

fn wait_ready(name: &str, proc_: &mut Proc, url: &str, extra: &[&str]) {
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let mut args = extra.to_vec();
        args.push(url);
        if curl(&args).status == 200 {
            return;
        }
        if let Some(code) = proc_.wait_exit(Duration::ZERO) {
            panic!("{name} exited with {code} before becoming ready:\n{}", proc_.stderr());
        }
        assert!(Instant::now() < deadline, "{name} not ready at {url}:\n{}", proc_.stderr());
        std::thread::sleep(Duration::from_millis(200));
    }
}

// ---------------------------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------------------------

struct Instance {
    dir: PathBuf,
    http_port: u16,
    grpc_port: u16,
}

impl Instance {
    fn new(root: &Path, name: &str) -> Self {
        let dir = root.join(name);
        fs::create_dir_all(dir.join("tsdb")).unwrap();
        fs::create_dir_all(dir.join("bucket")).unwrap();
        Self {
            dir,
            http_port: free_port(),
            grpc_port: free_port(),
        }
    }

    /// Classic ingester on an in-memory ring; listen addresses left at their (empty) defaults.
    fn write_config(&self, server_extra: &str, top_extra: &str) -> PathBuf {
        let yaml = format!(
            r#"target: ingester
server:
  http_listen_port: {http}
  grpc_listen_port: {grpc}
{server_extra}multitenancy_enabled: true
ingester:
  ring:
    kvstore:
      store: inmemory
    instance_id: ingester-0
    instance_addr: 127.0.0.1
    min_ready_duration: 0s
    replication_factor: 1
blocks_storage:
  backend: filesystem
  filesystem:
    dir: {bucket}
  s3:
    secret_access_key: s3-secret-value
    access_key_id: AKIAEXAMPLE
  azure:
    account_key: azure-secret-value
  tsdb:
    dir: {tsdb}
ingest_storage:
  kafka:
    sasl_password: kafka-secret-value
activity_tracker:
  filepath: ""
{top_extra}"#,
            http = self.http_port,
            grpc = self.grpc_port,
            bucket = self.dir.join("bucket").display(),
            tsdb = self.dir.join("tsdb").display(),
        );
        let path = self.dir.join("mimir.yaml");
        fs::write(&path, yaml).unwrap();
        path
    }

    fn start(&self, name: &str, bin: &Path, cfg: &Path) -> Proc {
        Proc::spawn(name, bin, &[format!("-config.file={}", cfg.display())], &self.dir)
    }
}

/// Starts Go and the candidate with the same configuration and waits until both are ready.
fn start_pair(server_extra: &str, ready_args: &[&str], scheme: &str) -> (tempfile::TempDir, [(Instance, Proc); 2]) {
    let (go_bin, cand_bin) = ensure_binaries();
    let tmp = tempfile::tempdir().unwrap();
    let mut out = Vec::new();
    for (name, bin) in [("go", &go_bin), ("candidate", &cand_bin)] {
        let inst = Instance::new(tmp.path(), name);
        let cfg = inst.write_config(server_extra, "");
        let mut p = inst.start(name, bin, &cfg);
        let url = format!("{scheme}://localhost:{}/ready", inst.http_port);
        wait_ready(name, &mut p, &url, ready_args);
        out.push((inst, p));
    }
    let b = out.pop().unwrap();
    let a = out.pop().unwrap();
    (tmp, [a, b])
}

async fn grpc_ready(endpoint: String, tls: Option<ClientTlsConfig>) -> Result<i32, String> {
    let mut ep = Channel::from_shared(endpoint).map_err(|e| e.to_string())?.connect_timeout(Duration::from_secs(10));
    if let Some(tls) = tls {
        ep = ep.tls_config(tls).map_err(|e| e.to_string())?;
    }
    let ch = ep.connect().await.map_err(|e| format!("connect: {e:?}"))?;
    let mut req = tonic::Request::new(HttpRequest {
        method: "GET".into(),
        url: "/ready".into(),
        headers: vec![],
        body: Default::default(),
    });
    req.metadata_mut()
        .insert("x-scope-orgid", tonic::metadata::MetadataValue::from_static("w9"));
    let resp = HttpClient::new(ch)
        .handle(req)
        .await
        .map_err(|e| format!("rpc: {e}"))?;
    Ok(resp.into_inner().code)
}

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
}

// ---------------------------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------------------------

#[test]
fn w9_help_and_version_parity() {
    let (go_bin, cand_bin) = ensure_binaries();
    // Same argv[0] for both ("Usage of mimir:").
    let run = |bin: &Path, arg: &str| -> Output {
        use std::os::unix::process::CommandExt as _;
        Command::new(bin).arg0("mimir").arg(arg).output().unwrap()
    };
    for arg in ["-help", "-help-all"] {
        let g = run(&go_bin, arg);
        let c = run(&cand_bin, arg);
        assert_eq!(g.status.code(), c.status.code(), "{arg} exit code");
        assert!(g.stdout == c.stdout, "{arg} stdout differs");
        assert!(g.stderr == c.stderr, "{arg} stderr differs");
    }
    // -version: same layout; values (version, toolchain) legitimately differ.
    let shape = |o: &Output| -> Vec<String> {
        String::from_utf8_lossy(&o.stdout)
            .lines()
            .map(|l| match l.split_once(':') {
                Some((k, _)) if l.starts_with(' ') => format!("{k}:"),
                _ => l.split(" (").next().unwrap_or("").split(", version").next().unwrap_or("").to_string(),
            })
            .collect()
    };
    let g = run(&go_bin, "-version");
    let c = run(&cand_bin, "-version");
    assert_eq!(g.status.code(), c.status.code());
    assert_eq!(shape(&g), shape(&c), "-version layout");
}

#[test]
fn w9_target_all_is_rejected_explicitly() {
    let (go_bin, cand_bin) = ensure_binaries();
    if candidate_is_go(&cand_bin) {
        eprintln!("candidate is Go Mimir, which implements every target: nothing to check");
        return;
    }
    let _ = go_bin;
    let tmp = tempfile::tempdir().unwrap();
    for target in ["all", "ingester,distributor"] {
        let inst = Instance::new(tmp.path(), &format!("t-{}", target.replace(',', "-")));
        let cfg = inst.write_config("", "");
        let mut p = Proc::spawn(
            "candidate",
            &cand_bin,
            &[format!("-config.file={}", cfg.display()), format!("-target={target}")],
            &inst.dir,
        );
        let code = p.wait_exit(Duration::from_secs(20));
        let module = target.split(',').next_back().unwrap_or(target);
        assert_eq!(code, Some(1), "-target={target} must fail startup:\n{}", p.stderr());
        // Plain or logfmt-escaped (`err="target module \"all\" ..."`).
        let stderr = p.stderr().replace("\\\"", "\"");
        assert!(
            stderr.contains(&format!("target module \"{module}\" is not yet implemented in mimir-rs")),
            "-target={target}: {stderr}"
        );
        // It must never have served HTTP.
        assert_eq!(curl(&[&format!("http://127.0.0.1:{}/ready", inst.http_port)]).status, 0);
    }
}

#[test]
fn w9_config_secret_masking_parity() {
    let (_tmp, [(go, _gp), (cand, _cp)]) = start_pair("", &[], "http");
    let secret_lines = |port: u16, q: &str| -> Vec<String> {
        let r = curl(&[&format!("http://127.0.0.1:{port}/config{q}")]);
        assert_eq!(r.status, 200);
        let body = String::from_utf8_lossy(&r.body).into_owned();
        assert!(!body.contains("secret-value"), "secret leaked in /config{q}");
        // Only the configured (non-empty) secrets: how empty strings render is a YAML emitter
        // difference, not masking.
        body.lines()
            .filter(|l| {
                let k = l.trim_start().split(':').next().unwrap_or("");
                ["secret_access_key", "account_key", "sasl_password"].contains(&k) && l.contains("********")
            })
            .map(|l| l.trim().to_string())
            .collect()
    };
    for q in ["", "?mode=diff"] {
        let g = secret_lines(go.http_port, q);
        let c = secret_lines(cand.http_port, q);
        assert!(g.iter().any(|l| l.contains("'********'")), "go /config{q}: {g:?}");
        let gs: BTreeSet<_> = g.into_iter().collect();
        let cs: BTreeSet<_> = c.into_iter().collect();
        assert_eq!(gs, cs, "/config{q} secret lines");
    }
}

#[test]
fn w9_dual_stack_listener_parity() {
    let (_tmp, [(go, _gp), (cand, _cp)]) = start_pair("", &[], "http");
    for host in ["127.0.0.1", "[::1]"] {
        let g = curl(&[&format!("http://{host}:{}/ready", go.http_port)]).status;
        let c = curl(&[&format!("http://{host}:{}/ready", cand.http_port)]).status;
        assert_eq!(g, 200, "go over {host}");
        assert_eq!(c, g, "candidate HTTP over {host}");
        let rt = rt();
        let g = rt.block_on(grpc_ready(format!("http://{host}:{}", go.grpc_port), None));
        let c = rt.block_on(grpc_ready(format!("http://{host}:{}", cand.grpc_port), None));
        assert_eq!(g, Ok(200), "go gRPC over {host}");
        assert_eq!(c, g, "candidate gRPC over {host}");
    }
}

struct Pki {
    dir: tempfile::TempDir,
}

impl Pki {
    fn path(&self, f: &str) -> String {
        self.dir.path().join(f).display().to_string()
    }

    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let sh = |cmd: &str| {
            let st = Command::new("sh").arg("-c").arg(cmd).current_dir(dir.path()).output().unwrap();
            assert!(st.status.success(), "{cmd}: {}", String::from_utf8_lossy(&st.stderr));
        };
        sh("openssl req -x509 -newkey rsa:2048 -nodes -keyout ca.key -out ca.crt -days 2 -subj /CN=w9-ca");
        for (name, eku, san) in [
            ("server", "serverAuth", "subjectAltName=DNS:localhost,IP:127.0.0.1,IP:::1\n"),
            ("client", "clientAuth", ""),
        ] {
            sh(&format!(
                "openssl req -newkey rsa:2048 -nodes -keyout {name}.key -out {name}.csr -subj /CN=w9-{name}"
            ));
            fs::write(dir.path().join(format!("{name}.ext")), format!("{san}extendedKeyUsage={eku}\n")).unwrap();
            sh(&format!(
                "openssl x509 -req -in {name}.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out {name}.crt -days 2 -extfile {name}.ext"
            ));
        }
        Self { dir }
    }
}

#[test]
fn w9_server_tls_and_mtls_parity() {
    let pki = Pki::new();
    let tls = |prefix: &str| {
        format!(
            "  {prefix}_tls_config:\n    cert_file: {}\n    key_file: {}\n    client_auth_type: RequireAndVerifyClientCert\n    client_ca_file: {}\n",
            pki.path("server.crt"),
            pki.path("server.key"),
            pki.path("ca.crt")
        )
    };
    let server = format!("{}{}", tls("http"), tls("grpc"));
    let ca = pki.path("ca.crt");
    let cert = pki.path("client.crt");
    let key = pki.path("client.key");
    let mtls: Vec<&str> = vec!["--cacert", &ca, "--cert", &cert, "--key", &key];
    let (_tmp, [(go, _gp), (cand, _cp)]) = start_pair(&server, &mtls, "https");

    for (name, inst) in [("go", &go), ("candidate", &cand)] {
        let url = format!("https://localhost:{}/ready", inst.http_port);
        // With a client certificate: served (HTTP/1.1 and HTTP/2 via ALPN).
        let mut a = mtls.clone();
        a.push(&url);
        assert_eq!(curl(&a).status, 200, "{name} mTLS");
        let mut a = mtls.clone();
        a.extend(["--http2", url.as_str()]);
        assert_eq!(curl(&a).status, 200, "{name} mTLS over h2");
        // Without a client certificate: handshake rejected, no HTTP response.
        assert_eq!(curl(&["--cacert", &ca, &url]).status, 0, "{name} without client cert");
        // Plain HTTP on the TLS port: Go answers 400 "Client sent an HTTP request to an HTTPS
        // server."; anything but a served request is acceptable for the candidate.
        let plain = curl(&[&format!("http://localhost:{}/ready", inst.http_port)]).status;
        assert_ne!(plain, 200, "{name} plain HTTP on TLS port");
    }

    let ca_pem = fs::read(pki.path("ca.crt")).unwrap();
    let id = Identity::from_pem(fs::read(&cert).unwrap(), fs::read(&key).unwrap());
    let rt = rt();
    for (name, inst) in [("go", &go), ("candidate", &cand)] {
        let ep = format!("https://localhost:{}", inst.grpc_port);
        let with_id = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(&ca_pem))
            .identity(id.clone())
            .domain_name("localhost");
        assert_eq!(rt.block_on(grpc_ready(ep.clone(), Some(with_id))), Ok(200), "{name} gRPC mTLS");
        let without = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(&ca_pem))
            .domain_name("localhost");
        assert!(rt.block_on(grpc_ready(ep, Some(without))).is_err(), "{name} gRPC without client cert");
    }
}

#[test]
fn w9_missing_tls_certificate_fails_startup_like_go() {
    let (go_bin, cand_bin) = ensure_binaries();
    let tmp = tempfile::tempdir().unwrap();
    let server = "  http_tls_config:\n    cert_file: /nonexistent/w9.crt\n    key_file: /nonexistent/w9.key\n";
    let mut errs = Vec::new();
    for (name, bin) in [("go", &go_bin), ("candidate", &cand_bin)] {
        let inst = Instance::new(tmp.path(), name);
        let cfg = inst.write_config(server, "");
        let mut p = inst.start(name, bin, &cfg);
        let code = p.wait_exit(Duration::from_secs(30));
        assert_eq!(code, Some(1), "{name} must fail startup:\n{}", p.stderr());
        errs.push(p.stderr());
    }
    let want = "error generating http tls config: failed to read cert_file (/nonexistent/w9.crt): open /nonexistent/w9.crt: no such file or directory";
    for e in &errs {
        assert!(e.contains(want), "missing {want:?} in:\n{e}");
    }
}

/// `name{labels} value` samples of one family from text exposition.
fn samples(text: &str, family: &str) -> Vec<(BTreeMap<String, String>, String)> {
    let mut out = Vec::new();
    for line in text.lines() {
        let Some(rest) = line.strip_prefix(family) else { continue };
        let (labels, value) = match rest.strip_prefix('{') {
            Some(r) => match r.split_once("} ") {
                Some((l, v)) => (l, v),
                None => continue,
            },
            None => match rest.strip_prefix(' ') {
                Some(v) => ("", v),
                None => continue,
            },
        };
        let mut m = BTreeMap::new();
        for kv in labels.split("\",").filter(|s| !s.is_empty()) {
            if let Some((k, v)) = kv.split_once("=\"") {
                m.insert(k.to_string(), v.trim_end_matches('"').to_string());
            }
        }
        out.push((m, value.to_string()));
    }
    out
}

fn families(text: &str, prefix: &str) -> BTreeSet<String> {
    text.lines()
        .filter_map(|l| l.strip_prefix("# TYPE "))
        .filter_map(|l| l.split(' ').next())
        .filter(|n| n.starts_with(prefix))
        .map(str::to_string)
        .collect()
}

#[test]
fn w9_request_metrics_and_exposition_parity() {
    let (_tmp, [(go, _gp), (cand, _cp)]) = start_pair("", &[], "http");
    let rt = rt();
    // The same traffic against both.
    for inst in [&go, &cand] {
        let base = format!("http://127.0.0.1:{}", inst.http_port);
        for (method, path) in [
            ("GET", "/ready"),
            ("PUT", "/ready"),
            ("GET", "/config"),
            ("GET", "/config?mode=diff"),
            ("GET", "/api/v1/status/buildinfo"),
            ("GET", "/no/such/route"),
            ("PUT", "/ingester/ring"),
            ("GET", "/ingester/tenants"),
            ("GET", "/debug/pprof/cmdline"),
            ("GET", "/debug/pprof/symbol"),
        ] {
            curl(&["-X", method, &format!("{base}{path}")]);
        }
        assert_eq!(rt.block_on(grpc_ready(format!("http://127.0.0.1:{}", inst.grpc_port), None)), Ok(200));
    }

    let scrape = |port: u16, accept: Option<&str>, gzip: bool| {
        let url = format!("http://127.0.0.1:{port}/metrics");
        let mut args: Vec<String> = Vec::new();
        if let Some(a) = accept {
            args.extend(["-H".into(), format!("Accept: {a}")]);
        }
        if gzip {
            args.extend(["-H".into(), "Accept-Encoding: gzip".into()]);
        }
        args.push(url);
        let refs: Vec<&str> = args.iter().map(String::as_str).collect();
        curl(&refs)
    };

    let g = String::from_utf8_lossy(&scrape(go.http_port, None, false).body).into_owned();
    let c = String::from_utf8_lossy(&scrape(cand.http_port, None, false).body).into_owned();

    // cortex_request_duration_seconds: same series and counts for the traffic above (the /metrics
    // scrape itself is in flight while collected and therefore excluded; GET /ready was also
    // polled a process-dependent number of times during startup, so only its labels count).
    let label_sets = |text: &str| -> BTreeSet<String> {
        samples(text, "cortex_request_duration_seconds_count")
            .into_iter()
            .filter(|(l, _)| l.get("route").map(String::as_str) != Some("metrics"))
            .filter_map(|(l, v)| {
                let polled = l.get("route").map(String::as_str) == Some("ready")
                    && l.get("method").map(String::as_str) == Some("GET");
                if polled {
                    // Startup polls may also have seen 503 while modules were starting.
                    (l.get("status_code").map(String::as_str) == Some("200")).then(|| format!("{l:?}"))
                } else {
                    Some(format!("{l:?} {v}"))
                }
            })
            .collect()
    };
    let gl = label_sets(&g);
    assert!(gl.iter().any(|s| s.contains("\"other\"")), "go: {gl:#?}");
    assert!(gl.iter().any(|s| s.contains("/httpgrpc.HTTP/Handle")), "go: {gl:#?}");
    assert_eq!(gl, label_sets(&c), "cortex_request_duration_seconds_count series");

    let buckets = |text: &str| -> BTreeSet<String> {
        samples(text, "cortex_request_duration_seconds_bucket")
            .into_iter()
            .filter_map(|(l, _)| l.get("le").cloned())
            .collect()
    };
    assert_eq!(buckets(&g), buckets(&c), "cortex_request_duration_seconds buckets");

    for fam in [
        "cortex_request_message_bytes",
        "cortex_response_message_bytes",
        "cortex_inflight_requests",
        "cortex_tcp_connections",
        "cortex_tcp_connections_limit",
        "cortex_grpc_concurrent_streams_limit",
        "cortex_request_duration_seconds",
    ] {
        assert_eq!(families(&g, fam).contains(fam), families(&c, fam).contains(fam), "family {fam}");
        assert!(families(&g, fam).contains(fam), "go lacks {fam}");
    }
    let tcp_limit = |t: &str| samples(t, "cortex_tcp_connections_limit");
    assert_eq!(tcp_limit(&g), tcp_limit(&c));
    let streams_limit = |t: &str| samples(t, "cortex_grpc_concurrent_streams_limit");
    assert_eq!(streams_limit(&g), streams_limit(&c));

    // process_* collectors.
    assert_eq!(families(&g, "process_"), families(&c, "process_"), "process_* families");

    // Content negotiation and compression.
    for accept in [
        None,
        Some("application/openmetrics-text;version=1.0.0;q=0.75,text/plain;version=0.0.4;q=0.5,*/*;q=0.1"),
        Some("application/vnd.google.protobuf;proto=io.prometheus.client.MetricFamily;encoding=delimited;q=0.7,text/plain;version=0.0.4;q=0.3"),
    ] {
        for gzip in [false, true] {
            let gr = scrape(go.http_port, accept, gzip);
            let cr = scrape(cand.http_port, accept, gzip);
            assert_eq!(gr.status, 200);
            assert_eq!(cr.status, gr.status, "status for {accept:?}");
            for h in ["content-type", "content-encoding"] {
                assert_eq!(cr.headers.get(h), gr.headers.get(h), "{h} for Accept {accept:?} gzip={gzip}");
            }
        }
    }

    // Admin route compression (Mimir gziphandler) and Go's 404 body.
    for path in ["/config", "/api/v1/status/buildinfo"] {
        let gr = curl(&["-H", "Accept-Encoding: gzip", &format!("http://127.0.0.1:{}{path}", go.http_port)]);
        let cr = curl(&["-H", "Accept-Encoding: gzip", &format!("http://127.0.0.1:{}{path}", cand.http_port)]);
        for h in ["content-encoding", "vary", "content-type"] {
            assert_eq!(cr.headers.get(h), gr.headers.get(h), "{h} of {path}");
        }
    }
    let gr = curl(&[&format!("http://127.0.0.1:{}/no/such/route", go.http_port)]);
    let cr = curl(&[&format!("http://127.0.0.1:{}/no/such/route", cand.http_port)]);
    assert_eq!((cr.status, cr.body), (gr.status, gr.body), "404");
}

#[test]
fn w9_runtime_config_reload_metrics_parity() {
    let (go_bin, cand_bin) = ensure_binaries();
    let tmp = tempfile::tempdir().unwrap();
    let initial = "overrides:\n  tenant-1:\n    max_global_series_per_user: 1000\n    ingestion_rate: 1.5e4\n  \"123\":\n    max_global_series_per_user: 0x10\n  tenant-10:\n    active_series_custom_trackers:\n      foo: '{__name__=~\"foo.*\"}'\n";
    let mut procs = Vec::new();
    for (name, bin) in [("go", &go_bin), ("candidate", &cand_bin)] {
        let inst = Instance::new(tmp.path(), name);
        let rt_file = inst.dir.join("runtime.yaml");
        let json_file = inst.dir.join("runtime.json");
        fs::write(&rt_file, initial).unwrap();
        fs::write(&json_file, r#"{"overrides":{"tenant-2":{"max_global_series_per_user":5000000}}}"#).unwrap();
        let top = format!(
            "runtime_config:\n  period: 1s\n  file: {},{}\n",
            rt_file.display(),
            json_file.display()
        );
        let cfg = inst.write_config("", &top);
        let mut p = inst.start(name, bin, &cfg);
        wait_ready(name, &mut p, &format!("http://127.0.0.1:{}/ready", inst.http_port), &[]);
        procs.push((inst, p, rt_file));
    }
    let rc_metrics = |port: u16| -> BTreeSet<String> {
        let text = String::from_utf8_lossy(&curl(&[&format!("http://127.0.0.1:{port}/metrics")]).body).into_owned();
        let dir_free = |s: String| s.replace(&tmp.path().display().to_string(), "<tmp>").replace("/go/", "/<inst>/").replace("/candidate/", "/<inst>/");
        [
            "cortex_runtime_config_hash",
            "cortex_runtime_config_last_reload_successful",
            "cortex_runtime_config_source_last_reload_successful",
        ]
        .iter()
        .flat_map(|f| samples(&text, f).into_iter().map(move |(l, v)| dir_free(format!("{f} {l:?} {v}"))))
        .collect()
    };
    let g = rc_metrics(procs[0].0.http_port);
    assert!(g.iter().any(|s| s.starts_with("cortex_runtime_config_hash")), "{g:#?}");
    assert_eq!(rc_metrics(procs[1].0.http_port), g, "after initial load");

    // A broken reload keeps the old config and hash, flags the failure.
    for (_, _, rt_file) in &procs {
        fs::write(rt_file, "overrides:\n  tenant-1:\n    bogus_field: 1\n").unwrap();
    }
    std::thread::sleep(Duration::from_secs(4));
    let g2 = rc_metrics(procs[0].0.http_port);
    assert!(
        g2.iter().any(|s| s.starts_with("cortex_runtime_config_last_reload_successful") && s.ends_with(" 0")),
        "{g2:#?}"
    );
    assert_eq!(rc_metrics(procs[1].0.http_port), g2, "after failed reload");
    for (_, p, _) in &procs {
        assert!(p.stderr().contains("failed to load config"), "missing reload error log");
    }

    // A good reload applies and re-hashes.
    for (_, _, rt_file) in &procs {
        fs::write(rt_file, "overrides:\n  tenant-1:\n    max_global_series_per_user: 7\n").unwrap();
    }
    std::thread::sleep(Duration::from_secs(4));
    let g3 = rc_metrics(procs[0].0.http_port);
    assert_ne!(g3, g);
    assert_eq!(rc_metrics(procs[1].0.http_port), g3, "after good reload");
}

// ---------------------------------------------------------------------------------------------
// S24: gRPC status codes of tenant and matcher errors on every cortex.Ingester RPC
// ---------------------------------------------------------------------------------------------

mod s24 {
    use mimir_proto::cortexpb;
    use mimir_proto::ingesterpb::{self, ingester_client::IngesterClient};
    use tokio_stream::StreamExt;
    use tonic::metadata::MetadataValue;
    use tonic::transport::Channel;
    use tonic::{Code, Request, Status, Streaming};

    /// Final status: code, message and the raw `grpc-status-details-bin` payload.
    pub type Outcome = Result<(), (Code, String, Vec<u8>)>;

    pub const RPCS: &[&str] = &[
        "Push",
        "QueryStream",
        "QueryExemplars",
        "LabelValues",
        "LabelNames",
        "UserStats",
        "AllUserStats",
        "MetricsForLabelMatchers",
        "MetricsMetadata",
        "LabelNamesAndValues",
        "LabelValuesCardinality",
        "ActiveSeries",
        "SearchLabelNames",
        "SearchLabelValues",
    ];

    fn st(s: Status) -> (Code, String, Vec<u8>) {
        (s.code(), s.message().to_string(), s.details().to_vec())
    }

    async fn drain<T>(r: Result<tonic::Response<Streaming<T>>, Status>) -> Outcome {
        let mut s = r.map_err(st)?.into_inner();
        while let Some(item) = s.next().await {
            item.map_err(st)?;
        }
        Ok(())
    }

    fn req<T>(msg: T, org_ids: &[&str]) -> Request<T> {
        let mut r = Request::new(msg);
        for id in org_ids {
            r.metadata_mut()
                .append("x-scope-orgid", MetadataValue::try_from(*id).unwrap());
        }
        r
    }

    pub fn matcher(t: ingesterpb::MatchType, name: &str, value: &str) -> ingesterpb::LabelMatcher {
        ingesterpb::LabelMatcher {
            r#type: t as i32,
            name: name.to_string(),
            value: value.to_string(),
        }
    }

    pub fn sample_write_request(ts: i64) -> cortexpb::WriteRequest {
        cortexpb::WriteRequest {
            source: cortexpb::write_request::SourceEnum::Api as i32,
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair::from_static("__name__", "up"),
                    cortexpb::LabelPair::from_static("job", "w9"),
                ],
                samples: vec![cortexpb::Sample {
                    value: 1.0,
                    timestamp_ms: ts,
                }],
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    /// Calls `rpc` with the given `x-scope-orgid` values and matchers, returning the final status.
    pub async fn call(
        c: &mut IngesterClient<Channel>,
        rpc: &str,
        org_ids: &[&str],
        matchers: &[ingesterpb::LabelMatcher],
        now_ms: i64,
    ) -> Outcome {
        let (start, end) = (now_ms - 3_600_000, now_ms + 3_600_000);
        let m = matchers.to_vec();
        let ms = ingesterpb::LabelMatchers { matchers: m.clone() };
        match rpc {
            "Push" => c
                .push(req(sample_write_request(now_ms), org_ids))
                .await
                .map(|_| ())
                .map_err(st),
            "QueryStream" => drain(
                c.query_stream(req(
                    ingesterpb::QueryRequest {
                        start_timestamp_ms: start,
                        end_timestamp_ms: end,
                        matchers: m,
                        streaming_chunks_batch_size: 0,
                    },
                    org_ids,
                ))
                .await,
            )
            .await,
            "QueryExemplars" => c
                .query_exemplars(req(
                    ingesterpb::ExemplarQueryRequest {
                        start_timestamp_ms: start,
                        end_timestamp_ms: end,
                        matchers: vec![ms],
                    },
                    org_ids,
                ))
                .await
                .map(|_| ())
                .map_err(st),
            "LabelValues" => c
                .label_values(req(
                    ingesterpb::LabelValuesRequest {
                        label_name: "job".into(),
                        start_timestamp_ms: start,
                        end_timestamp_ms: end,
                        matchers: Some(ms),
                        limit: 0,
                    },
                    org_ids,
                ))
                .await
                .map(|_| ())
                .map_err(st),
            "LabelNames" => c
                .label_names(req(
                    ingesterpb::LabelNamesRequest {
                        start_timestamp_ms: start,
                        end_timestamp_ms: end,
                        matchers: Some(ms),
                        limit: 0,
                    },
                    org_ids,
                ))
                .await
                .map(|_| ())
                .map_err(st),
            "UserStats" => c
                .user_stats(req(ingesterpb::UserStatsRequest::default(), org_ids))
                .await
                .map(|_| ())
                .map_err(st),
            "AllUserStats" => c
                .all_user_stats(req(ingesterpb::UserStatsRequest::default(), org_ids))
                .await
                .map(|_| ())
                .map_err(st),
            "MetricsForLabelMatchers" => c
                .metrics_for_label_matchers(req(
                    ingesterpb::MetricsForLabelMatchersRequest {
                        start_timestamp_ms: start,
                        end_timestamp_ms: end,
                        matchers_set: vec![ms],
                        limit: 0,
                    },
                    org_ids,
                ))
                .await
                .map(|_| ())
                .map_err(st),
            "MetricsMetadata" => c
                .metrics_metadata(req(
                    ingesterpb::MetricsMetadataRequest {
                        limit: -1,
                        limit_per_metric: -1,
                        ..Default::default()
                    },
                    org_ids,
                ))
                .await
                .map(|_| ())
                .map_err(st),
            "LabelNamesAndValues" => drain(
                c.label_names_and_values(req(
                    ingesterpb::LabelNamesAndValuesRequest {
                        matchers: m,
                        count_method: 0,
                    },
                    org_ids,
                ))
                .await,
            )
            .await,
            "LabelValuesCardinality" => drain(
                c.label_values_cardinality(req(
                    ingesterpb::LabelValuesCardinalityRequest {
                        label_names: vec!["job".into()],
                        matchers: m,
                        count_method: 0,
                    },
                    org_ids,
                ))
                .await,
            )
            .await,
            "ActiveSeries" => drain(
                c.active_series(req(
                    ingesterpb::ActiveSeriesRequest {
                        matchers: m,
                        ..Default::default()
                    },
                    org_ids,
                ))
                .await,
            )
            .await,
            "SearchLabelNames" => drain(
                c.search_label_names(req(
                    ingesterpb::SearchLabelNamesRequest {
                        start_timestamp_ms: start,
                        end_timestamp_ms: end,
                        matchers: m,
                        ..Default::default()
                    },
                    org_ids,
                ))
                .await,
            )
            .await,
            "SearchLabelValues" => drain(
                c.search_label_values(req(
                    ingesterpb::SearchLabelValuesRequest {
                        start_timestamp_ms: start,
                        end_timestamp_ms: end,
                        name: "job".into(),
                        matchers: m,
                        ..Default::default()
                    },
                    org_ids,
                ))
                .await,
            )
            .await,
            other => panic!("unknown rpc {other}"),
        }
    }
}

/// S24: `x-scope-orgid` tenant errors (multi-tenant `a|b`, repeated header, invalid tenant ID)
/// and invalid regex matchers produce the same gRPC status code and message on every
/// `cortex.Ingester` RPC as Go (dskit `ServerUserHeaderInterceptor` + `tenant.TenantID`, Mimir
/// `mapReadErrorToErrorWithStatus`).
#[test]
fn w9_grpc_tenant_and_matcher_error_status_parity() {
    use mimir_proto::ingesterpb::{ingester_client::IngesterClient, MatchType};

    let (_tmp, [(go, _gp), (cand, _cp)]) = start_pair("", &[], "http");
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let bad_regex = [s24::matcher(MatchType::RegexMatch, "job", "(")];
    let bad_nregex = [s24::matcher(MatchType::RegexNoMatch, "job", "a(")];
    let good = [s24::matcher(MatchType::Equal, "job", "w9")];
    let bad_type = [mimir_proto::ingesterpb::LabelMatcher {
        r#type: 9,
        name: "job".into(),
        value: "w9".into(),
    }];
    // (label, org ids, matchers)
    let cases: Vec<(&str, Vec<&str>, &[mimir_proto::ingesterpb::LabelMatcher])> = vec![
        ("no header", vec![], &good),
        ("multi-tenant a|b", vec!["a|b"], &good),
        ("same tenant twice a|a", vec!["w9|w9"], &good),
        ("repeated header", vec!["w9", "w9"], &good),
        ("repeated header differing", vec!["a", "b"], &good),
        ("invalid tenant char", vec!["a/b"], &good),
        // Not compared: an empty `x-scope-orgid` value. Go accepts the empty tenant ID (reads
        // succeed, a push would put the tenant TSDB into the TSDB root directory); mimir-rs
        // rejects it as `no org id` (documented divergence in PORT_STATUS.md).
        ("unsafe tenant ..", vec![".."], &good),
        ("invalid regex, existing tenant", vec!["w9"], &bad_regex),
        ("invalid negative regex, existing tenant", vec!["w9"], &bad_nregex),
        ("invalid regex, unknown tenant", vec!["nobody"], &bad_regex),
        ("unknown matcher type", vec!["w9"], &bad_type),
    ];
    let rt = rt();
    let results = rt.block_on(async {
        let mut out: Vec<[s24::Outcome; 2]> = Vec::new();
        let mut clients = Vec::new();
        for inst in [&go, &cand] {
            let ch = Channel::from_shared(format!("http://127.0.0.1:{}", inst.grpc_port))
                .unwrap()
                .connect()
                .await
                .unwrap();
            let mut c = IngesterClient::new(ch);
            // Create the `w9` TSDB so matcher errors are hit with and without a tenant DB.
            s24::call(&mut c, "Push", &["w9"], &[], now_ms).await.unwrap();
            clients.push(c);
        }
        let mut labels = Vec::new();
        for (label, ids, matchers) in &cases {
            for rpc in s24::RPCS {
                let g = s24::call(&mut clients[0], rpc, ids, matchers, now_ms).await;
                let c = s24::call(&mut clients[1], rpc, ids, matchers, now_ms).await;
                labels.push(format!("{rpc} [{label}]"));
                out.push([g, c]);
            }
        }
        (labels, out)
    });
    let mut mismatches = Vec::new();
    for (label, [g, c]) in results.0.iter().zip(results.1.iter()) {
        eprintln!("{label}: go={g:?}");
        if g != c {
            mismatches.push(format!("{label}:\n    go:        {g:?}\n    candidate: {c:?}"));
        }
    }
    assert!(mismatches.is_empty(), "{} mismatches:\n{}", mismatches.len(), mismatches.join("\n"));
}

/// Item 6: ingester-level metrics every tenant TSDB updates are registered once at startup like
/// Go `newIngester` / `newIngesterMetrics`: the head / block PostingsForMatchers cache metrics
/// (`cortex_ingester_tsdb_{head,block}_postings_for_matchers_cache_*`, every `reason`), the
/// offset catalogue metrics and `cortex_ingester_inflight_push_requests_summary`. Compares HELP,
/// TYPE and the series (name + labels) of each family; counters and gauges must also match
/// (no queries are issued, so all are 0), summary quantile values are not compared.
#[test]
fn w9_ingester_shared_tsdb_metric_families_parity() {
    let (_tmp, [(go, _gp), (cand, _cp)]) = start_pair("", &[], "http");
    // Let the 250ms inflight sampler observe at least once in both processes.
    std::thread::sleep(Duration::from_millis(600));
    let scrape = |port: u16| {
        let r = curl(&[&format!("http://127.0.0.1:{port}/metrics")]);
        assert_eq!(r.status, 200);
        String::from_utf8(r.body).unwrap()
    };
    let (g, c) = (scrape(go.http_port), scrape(cand.http_port));
    let prefixes = [
        "cortex_ingester_tsdb_head_postings_for_matchers_cache_",
        "cortex_ingester_tsdb_block_postings_for_matchers_cache_",
        "cortex_ingester_offset_catalogue_",
        "cortex_ingester_inflight_push_requests_summary",
    ];
    for prefix in prefixes {
        let gf = families(&g, prefix);
        assert!(!gf.is_empty(), "go exposes no {prefix}* family");
        assert_eq!(families(&c, prefix), gf, "{prefix}* families");
        for fam in &gf {
            let meta = |text: &str| -> Vec<String> {
                text.lines()
                    .filter(|l| {
                        l.strip_prefix("# HELP ").or_else(|| l.strip_prefix("# TYPE "))
                            .is_some_and(|r| r.split(' ').next() == Some(fam.as_str()))
                    })
                    .map(str::to_string)
                    .collect()
            };
            assert_eq!(meta(&c), meta(&g), "{fam} HELP/TYPE");
            let series = |text: &str| -> BTreeSet<String> {
                text.lines()
                    .filter(|l| !l.starts_with('#'))
                    .filter_map(|l| l.rsplit_once(' ').map(|(s, _)| s.to_string()))
                    .filter(|s| s == fam || s.starts_with(&format!("{fam}{{")) || s.starts_with(&format!("{fam}_")))
                    .collect()
            };
            assert_eq!(series(&c), series(&g), "{fam} series");
            if !fam.ends_with("_summary") {
                let mut gs = samples(&g, fam);
                let mut cs = samples(&c, fam);
                gs.sort();
                cs.sort();
                assert_eq!(cs, gs, "{fam} values");
            }
        }
    }
}

/// S26: `/config`, `/config?mode=defaults`, `/config?mode=diff` and `-print.config` are
/// byte-identical to Go's yaml.v3 output (4-space indentation, `""`, struct field order, sorted
/// map keys in the diff), after replacing the per-instance values (directories, ports) and
/// dropping the hidden `block_builder`, `block_builder_scheduler` and `usage_tracker` blocks
/// mimir-rs does not model (documented gap in PORT_STATUS.md).
#[test]
fn w9_config_yaml_rendering_parity() {
    let (_tmp, [(go, _gp), (cand, _cp)]) = start_pair("", &[], "http");
    let normalize = |inst: &Instance, text: &str| -> String {
        let mut out = String::new();
        let mut skipping = false;
        for line in text.lines() {
            let top_level = !line.starts_with(' ');
            if top_level {
                skipping = ["block_builder:", "block_builder_scheduler:", "usage_tracker:"]
                    .contains(&line);
            }
            if skipping {
                continue;
            }
            out.push_str(line);
            out.push('\n');
        }
        out.replace(&inst.dir.display().to_string(), "<dir>")
            .replace(&format!(": {}\n", inst.http_port), ": <http-port>\n")
            .replace(&format!(": {}\n", inst.grpc_port), ": <grpc-port>\n")
    };
    let assert_same = |what: &str, g: &str, c: &str| {
        if g != c {
            let first = g
                .lines()
                .zip(c.lines())
                .position(|(a, b)| a != b)
                .unwrap_or(g.lines().count().min(c.lines().count()));
            let ctx = |s: &str| s.lines().skip(first.saturating_sub(3)).take(8).collect::<Vec<_>>().join("\n");
            panic!("{what} differs at line {}:\n--- go\n{}\n--- candidate\n{}", first + 1, ctx(g), ctx(c));
        }
    };
    for q in ["", "?mode=defaults", "?mode=diff"] {
        let get = |inst: &Instance| {
            let r = curl(&[&format!("http://127.0.0.1:{}/config{q}", inst.http_port)]);
            assert_eq!(r.status, 200, "/config{q}");
            normalize(inst, &String::from_utf8(r.body).unwrap())
        };
        assert_same(&format!("/config{q}"), &get(&go), &get(&cand));
    }

    // `-print.config` (Go `mimir.New`: `yaml.NewEncoder(os.Stdout).Encode(&cfg)`).
    let (go_bin, cand_bin) = ensure_binaries();
    let print = |inst: &Instance, bin: &Path| {
        let out = Command::new(bin)
            .arg(format!("-config.file={}", inst.dir.join("mimir.yaml").display()))
            .arg("-print.config")
            .current_dir(&inst.dir)
            .output()
            .expect("run -print.config");
        assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
        normalize(inst, &String::from_utf8(out.stdout).unwrap())
    };
    assert_same("-print.config", &print(&go, &go_bin), &print(&cand, &cand_bin));
}
