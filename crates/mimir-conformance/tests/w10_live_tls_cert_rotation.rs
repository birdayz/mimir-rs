//! Gate 1C + Gate 4B: Live atomic TLS certificate rotation across HTTP mTLS, gRPC mTLS,
//! Memberlist gossip TLS, and shared client TLS resolvers without restarting processes.
//!
//! Verifies:
//! 1. Root CA + Leaf Cert #1 (serial 01) + Leaf Cert #2 (serial 02) + Client mTLS Certs #1 & #2
//!    generated via `openssl` (zero extra Cargo dependencies).
//! 2. Two Rust `mimir -target=ingester` processes form a TLS-enabled Memberlist gossip ring
//!    (`-memberlist.tls-enabled=true`) and expose mTLS HTTP + gRPC servers
//!    (`-server.http-tls-*`, `-server.grpc-tls-*`) initially serving Cert #1.
//! 3. Torn Kubernetes `..data` swap simulation: `server.crt` is replaced with Cert #2 while
//!    `server.key` still holds Key #1. Live HTTPS, gRPC, and Memberlist handshakes continue to
//!    succeed using the cached last-known-good Cert #1 (serial 01) with zero dropped requests.
//! 4. Atomic swap completes (`server.key` -> Key #2, client mTLS -> Client #2). After >200ms
//!    throttle window (250ms sleep), fresh HTTPS, gRPC, and Memberlist connections present Cert #2
//!    (serial 02), mTLS succeeds with Client #2, and Memberlist TCP push/pull gossip continues
//!    with zero stream errors and without restarting either process.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};

use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use mimir_conformance::oracle::ensure_rust_mimir_release_built;
use mimir_proto::httpgrpcpb::http_client::HttpClient;
use mimir_proto::httpgrpcpb::HttpRequest;

use tonic::transport::{Certificate, Channel, ClientTlsConfig, Identity};

fn rust_bin_override() -> Option<PathBuf> {
    std::env::var_os("MIMIR_CONFORMANCE_RUST_BIN")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
}

fn ensure_rust_bin() -> PathBuf {
    if let Some(bin) = rust_bin_override() {
        return bin;
    }
    ensure_rust_mimir_release_built().expect("target/release/mimir must build")
}

struct Pki {
    dir: tempfile::TempDir,
    live_dir: PathBuf,
}

impl Pki {
    fn new() -> Result<Self> {
        let dir = tempfile::tempdir()?;
        let p = dir.path();
        let live_dir = p.join("live");
        fs::create_dir_all(&live_dir)?;

        let run_openssl = |args: &[&str]| -> Result<()> {
            let out = Command::new("openssl")
                .args(args)
                .current_dir(p)
                .output()
                .with_context(|| format!("spawn openssl {:?}", args))?;
            if !out.status.success() {
                bail!(
                    "openssl {:?} failed ({}): {}",
                    args,
                    out.status,
                    String::from_utf8_lossy(&out.stderr)
                );
            }
            Ok(())
        };

        // Root CA
        run_openssl(&[
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            "ca.key",
            "-out",
            "ca.crt",
            "-days",
            "2",
            "-subj",
            "/CN=w10-ca",
        ])?;

        // Server Leaf Certs #1 (serial 0x01) and #2 (serial 0x02)
        fs::write(
            p.join("server.ext"),
            "subjectAltName=DNS:localhost,IP:127.0.0.1,IP:::1
extendedKeyUsage=serverAuth,clientAuth
",
        )?;
        for (idx, serial) in [(1, "0x01"), (2, "0x02")] {
            let key = format!("server-{idx}.key");
            let csr = format!("server-{idx}.csr");
            let crt = format!("server-{idx}.crt");
            let subj = format!("/CN=w10-server-{idx}");
            run_openssl(&[
                "req", "-newkey", "rsa:2048", "-nodes", "-keyout", &key, "-out", &csr, "-subj",
                &subj,
            ])?;
            run_openssl(&[
                "x509",
                "-req",
                "-in",
                &csr,
                "-CA",
                "ca.crt",
                "-CAkey",
                "ca.key",
                "-set_serial",
                serial,
                "-out",
                &crt,
                "-days",
                "2",
                "-extfile",
                "server.ext",
            ])?;
        }

        // Client mTLS Certs #1 (serial 0x0b) and #2 (serial 0x0c)
        fs::write(p.join("client.ext"), "extendedKeyUsage=clientAuth
")?;
        for (idx, serial) in [(1, "0x0b"), (2, "0x0c")] {
            let key = format!("client-{idx}.key");
            let csr = format!("client-{idx}.csr");
            let crt = format!("client-{idx}.crt");
            let subj = format!("/CN=w10-client-{idx}");
            run_openssl(&[
                "req", "-newkey", "rsa:2048", "-nodes", "-keyout", &key, "-out", &csr, "-subj",
                &subj,
            ])?;
            run_openssl(&[
                "x509",
                "-req",
                "-in",
                &csr,
                "-CA",
                "ca.crt",
                "-CAkey",
                "ca.key",
                "-set_serial",
                serial,
                "-out",
                &crt,
                "-days",
                "2",
                "-extfile",
                "client.ext",
            ])?;
        }

        // Seed live files with Cert #1 and Client #1
        fs::copy(p.join("server-1.crt"), live_dir.join("server.crt"))?;
        fs::copy(p.join("server-1.key"), live_dir.join("server.key"))?;
        fs::copy(p.join("client-1.crt"), live_dir.join("client.crt"))?;
        fs::copy(p.join("client-1.key"), live_dir.join("client.key"))?;

        Ok(Self { dir, live_dir })
    }

    fn ca_crt(&self) -> PathBuf {
        self.dir.path().join("ca.crt")
    }

    fn live_server_crt(&self) -> PathBuf {
        self.live_dir.join("server.crt")
    }

    fn live_server_key(&self) -> PathBuf {
        self.live_dir.join("server.key")
    }

    fn live_client_crt(&self) -> PathBuf {
        self.live_dir.join("client.crt")
    }

    fn live_client_key(&self) -> PathBuf {
        self.live_dir.join("client.key")
    }

    fn staged_path(&self, name: &str) -> PathBuf {
        self.dir.path().join(name)
    }
}

fn atomic_replace(src: &Path, dst: &Path) -> Result<()> {
    let tmp = dst.with_extension("tmp");
    fs::copy(src, &tmp)?;
    fs::rename(&tmp, dst)?;
    Ok(())
}

struct NodeSpec<'a> {
    name: &'a str,
    http_port: u16,
    grpc_port: u16,
    ml_port: u16,
    join_ml_port: u16,
}

struct NodeProc {
    name: String,
    child: Option<Child>,
    log: PathBuf,
    http_port: u16,
    grpc_port: u16,
    ml_port: u16,
}

impl NodeProc {
    fn spawn(dir: &Path, bin: &Path, spec: NodeSpec<'_>, pki: &Pki) -> Result<Self> {
        let NodeSpec {
            name,
            http_port,
            grpc_port,
            ml_port,
            join_ml_port,
        } = spec;
        let ndir = dir.join(name);
        fs::create_dir_all(ndir.join("tsdb"))?;
        fs::create_dir_all(ndir.join("bucket"))?;

        let yaml = format!(
            r#"target: ingester
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http_port}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc_port}
multitenancy_enabled: true
memberlist:
  node_name: {name}
  randomize_node_name: false
  bind_addr: [127.0.0.1]
  bind_port: {ml_port}
  advertise_addr: 127.0.0.1
  advertise_port: {ml_port}
  gossip_interval: 100ms
  pull_push_interval: 400ms
  abort_if_cluster_join_fails: false
  join_members:
    - 127.0.0.1:{join_ml_port}
  tls_cert_path: {srv_crt}
  tls_key_path: {srv_key}
  tls_ca_path: {ca_crt}
ingester:
  ring:
    kvstore:
      store: memberlist
    instance_id: {name}
    instance_addr: 127.0.0.1
    num_tokens: 16
    min_ready_duration: 0s
    replication_factor: 1
blocks_storage:
  backend: filesystem
  filesystem:
    dir: {bucket}
  tsdb:
    dir: {tsdb}
    ship_interval: 0s
activity_tracker:
  filepath: ""
"#,
            srv_crt = pki.live_server_crt().display(),
            srv_key = pki.live_server_key().display(),
            ca_crt = pki.ca_crt().display(),
            bucket = ndir.join("bucket").display(),
            tsdb = ndir.join("tsdb").display(),
        );
        let cfg = ndir.join("mimir.yaml");
        fs::write(&cfg, yaml)?;
        let log = ndir.join("stderr.log");

        let child = Command::new(bin)
            .args([
                format!("-config.file={}", cfg.display()),
                "-memberlist.tls-enabled=true".to_string(),
                format!("-server.http-tls-cert-path={}", pki.live_server_crt().display()),
                format!("-server.http-tls-key-path={}", pki.live_server_key().display()),
                "-server.http-tls-client-auth=RequireAndVerifyClientCert".to_string(),
                format!("-server.http-tls-ca-path={}", pki.ca_crt().display()),
                format!("-server.grpc-tls-cert-path={}", pki.live_server_crt().display()),
                format!("-server.grpc-tls-key-path={}", pki.live_server_key().display()),
                "-server.grpc-tls-client-auth=RequireAndVerifyClientCert".to_string(),
                format!("-server.grpc-tls-ca-path={}", pki.ca_crt().display()),
            ])
            .current_dir(&ndir)
            .stdout(Stdio::null())
            .stderr(fs::File::create(&log)?)
            .spawn()
            .with_context(|| format!("spawn {name}"))?;

        Ok(Self {
            name: name.to_string(),
            child: Some(child),
            log,
            http_port,
            grpc_port,
            ml_port,
        })
    }

    fn assert_alive(&mut self) {
        if let Some(child) = self.child.as_mut() {
            if let Ok(Some(status)) = child.try_wait() {
                panic!(
                    "{} exited unexpectedly with {status}:
{}",
                    self.name,
                    self.log_tail()
                );
            }
        }
    }

    fn log_text(&self) -> String {
        fs::read_to_string(&self.log).unwrap_or_default()
    }

    fn log_tail(&self) -> String {
        let s = self.log_text();
        let lines: Vec<&str> = s.lines().collect();
        lines[lines.len().saturating_sub(40)..].join("
")
    }
}

impl Drop for NodeProc {
    fn drop(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

/// Connects via `openssl s_client` (forcing a brand-new TLS handshake), verifies the server
/// against `ca_crt`, optionally presents a client mTLS cert, and returns the uppercase hex
/// serial (e.g. `"01"` or `"02"`).
fn probe_peer_cert_serial(
    port: u16,
    ca_crt: &Path,
    client_cert: Option<(&Path, &Path)>,
    alpn: Option<&str>,
) -> Result<String> {
    let addr = format!("127.0.0.1:{port}");
    let mut args = vec![
        "s_client".to_string(),
        "-connect".to_string(),
        addr,
        "-servername".to_string(),
        "localhost".to_string(),
        "-CAfile".to_string(),
        ca_crt.display().to_string(),
        "-verify_return_error".to_string(),
    ];
    if let Some((cert, key)) = client_cert {
        args.push("-cert".to_string());
        args.push(cert.display().to_string());
        args.push("-key".to_string());
        args.push(key.display().to_string());
    }
    if let Some(proto) = alpn {
        args.push("-alpn".to_string());
        args.push(proto.to_string());
    }

    let out = Command::new("openssl")
        .args(&args)
        .stdin(Stdio::null())
        .output()
        .context("spawn openssl s_client")?;
    if !out.status.success() {
        bail!(
            "openssl s_client {:?} failed ({}):
stdout: {}
stderr: {}",
            args,
            out.status,
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
    }

    let mut x509 = Command::new("openssl")
        .args(["x509", "-noout", "-serial"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("spawn openssl x509")?;
    if let Some(mut stdin) = x509.stdin.take() {
        stdin.write_all(&out.stdout)?;
    }
    let parsed = x509.wait_with_output()?;
    if !parsed.status.success() {
        bail!(
            "openssl x509 failed: {}",
            String::from_utf8_lossy(&parsed.stderr)
        );
    }
    let text = String::from_utf8_lossy(&parsed.stdout);
    let serial = text
        .trim()
        .strip_prefix("serial=")
        .with_context(|| format!("unexpected serial output: {text:?}"))?
        .to_string();
    Ok(serial)
}

fn https_get_mtls(port: u16, path: &str, pki: &Pki) -> Result<(u16, String)> {
    let mut combined = fs::read(pki.live_client_crt())?;
    combined.push(b'\n');
    combined.extend_from_slice(&fs::read(pki.live_client_key())?);
    let identity = reqwest::Identity::from_pem(&combined)?;
    let ca = reqwest::Certificate::from_pem(&fs::read(pki.ca_crt())?)?;

    let client = reqwest::Client::builder()
        .use_rustls_tls()
        .add_root_certificate(ca)
        .identity(identity)
        .timeout(Duration::from_secs(10))
        .build()?;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    rt.block_on(async {
        let resp = client
            .get(format!("https://127.0.0.1:{port}{path}"))
            .send()
            .await?;
        let status = resp.status().as_u16();
        let body = resp.text().await?;
        Ok((status, body))
    })
}

fn grpc_ready_mtls(port: u16, pki: &Pki) -> Result<i32> {
    let ca_pem = fs::read(pki.ca_crt())?;
    let cert_pem = fs::read(pki.live_client_crt())?;
    let key_pem = fs::read(pki.live_client_key())?;
    let tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(ca_pem))
        .identity(Identity::from_pem(cert_pem, key_pem))
        .domain_name("localhost");

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    rt.block_on(async {
        let ch = Channel::from_shared(format!("https://127.0.0.1:{port}"))?
            .connect_timeout(Duration::from_secs(10))
            .tls_config(tls)?
            .connect()
            .await?;
        let mut req = tonic::Request::new(HttpRequest {
            method: "GET".into(),
            url: "/ready".into(),
            headers: vec![],
            body: Default::default(),
        });
        req.metadata_mut()
            .insert("x-scope-orgid", tonic::metadata::MetadataValue::from_static("w10"));
        let resp = HttpClient::new(ch).handle(req).await?;
        Ok(resp.into_inner().code)
    })
}

fn metric_value(metrics_body: &str, metric_name: &str) -> Option<f64> {
    metrics_body
        .lines()
        .find(|l| l.starts_with(&format!("{metric_name} ")))
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|v| v.parse::<f64>().ok())
}

fn wait_until(timeout: Duration, mut pred: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if pred() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(150));
    }
    pred()
}

#[test]
fn w10_live_tls_cert_rotation_across_http_grpc_and_memberlist() {
    let bin = ensure_rust_bin();
    let pki = Pki::new().expect("create test PKI");
    let workdir = tempfile::tempdir().unwrap();

    let (http1, grpc1, ml1) = (
        mimir_conformance::free_test_port(),
        mimir_conformance::free_test_port(),
        mimir_conformance::free_test_port(),
    );
    let (http2, grpc2, ml2) = (
        mimir_conformance::free_test_port(),
        mimir_conformance::free_test_port(),
        mimir_conformance::free_test_port(),
    );

    let mut node1 = NodeProc::spawn(
        workdir.path(),
        &bin,
        NodeSpec {
            name: "node-1",
            http_port: http1,
            grpc_port: grpc1,
            ml_port: ml1,
            join_ml_port: ml2,
        },
        &pki,
    )
    .expect("spawn node-1");
    let mut node2 = NodeProc::spawn(
        workdir.path(),
        &bin,
        NodeSpec {
            name: "node-2",
            http_port: http2,
            grpc_port: grpc2,
            ml_port: ml2,
            join_ml_port: ml1,
        },
        &pki,
    )
    .expect("spawn node-2");

    // Wait for both nodes to become /ready over HTTPS mTLS and converge to a 2-node ring.
    for node in [&mut node1, &mut node2] {
        let ready = wait_until(Duration::from_secs(60), || {
            node.assert_alive();
            matches!(https_get_mtls(node.http_port, "/ready", &pki), Ok((200, _)))
        });
        assert!(ready, "{} did not become ready:
{}", node.name, node.log_tail());
    }
    for node in [&mut node1, &mut node2] {
        let converged = wait_until(Duration::from_secs(60), || {
            node.assert_alive();
            let Ok((200, metrics)) = https_get_mtls(node.http_port, "/metrics", &pki) else {
                return false;
            };
            let members = metric_value(&metrics, "memberlist_client_cluster_members_count");
            let Ok((200, ring)) = https_get_mtls(node.http_port, "/ingester/ring", &pki) else {
                return false;
            };
            members == Some(2.0) && ring.contains("node-1") && ring.contains("node-2")
        });
        assert!(
            converged,
            "{} failed to converge 2-node TLS memberlist ring:
{}",
            node.name,
            node.log_tail()
        );
    }

    // -------------------------------------------------------------------------
    // Phase 1: Initial Cert #1 (serial 01) on HTTPS, gRPC, and Memberlist
    // -------------------------------------------------------------------------
    let client1 = (pki.live_client_crt(), pki.live_client_key());
    for node in [&node1, &node2] {
        assert_eq!(
            probe_peer_cert_serial(node.http_port, &pki.ca_crt(), Some((&client1.0, &client1.1)), None)
                .unwrap(),
            "01",
            "{} HTTPS initial cert",
            node.name
        );
        assert_eq!(
            probe_peer_cert_serial(node.grpc_port, &pki.ca_crt(), Some((&client1.0, &client1.1)), Some("h2"))
                .unwrap(),
            "01",
            "{} gRPC initial cert",
            node.name
        );
        assert_eq!(
            probe_peer_cert_serial(node.ml_port, &pki.ca_crt(), None, None).unwrap(),
            "01",
            "{} Memberlist initial cert",
            node.name
        );
        assert_eq!(grpc_ready_mtls(node.grpc_port, &pki).unwrap(), 200);
    }

    // -------------------------------------------------------------------------
    // Phase 2: Torn Kubernetes ..data swap (Cert #2 written while Key #1 remains)
    // -------------------------------------------------------------------------
    atomic_replace(&pki.staged_path("server-2.crt"), &pki.live_server_crt()).unwrap();
    // Wait past the 200ms metadata-check throttle so reloaders actively hit the torn pair.
    std::thread::sleep(Duration::from_millis(250));

    for _ in 0..3 {
        for node in [&mut node1, &mut node2] {
            node.assert_alive();
            let (st, _) = https_get_mtls(node.http_port, "/ready", &pki).unwrap();
            assert_eq!(st, 200, "{} HTTPS during torn swap", node.name);
            assert_eq!(
                grpc_ready_mtls(node.grpc_port, &pki).unwrap(),
                200,
                "{} gRPC during torn swap",
                node.name
            );
            assert_eq!(
                probe_peer_cert_serial(
                    node.http_port,
                    &pki.ca_crt(),
                    Some((&client1.0, &client1.1)),
                    None
                )
                .unwrap(),
                "01",
                "{} HTTPS must keep last-known-good Cert #1 during torn swap",
                node.name
            );
            assert_eq!(
                probe_peer_cert_serial(
                    node.grpc_port,
                    &pki.ca_crt(),
                    Some((&client1.0, &client1.1)),
                    Some("h2")
                )
                .unwrap(),
                "01",
                "{} gRPC must keep last-known-good Cert #1 during torn swap",
                node.name
            );
            assert_eq!(
                probe_peer_cert_serial(node.ml_port, &pki.ca_crt(), None, None).unwrap(),
                "01",
                "{} Memberlist must keep last-known-good Cert #1 during torn swap",
                node.name
            );
        }
    }

    // Snapshot memberlist TCP stream & error counters before completing the rotation.
    let m1_pre = https_get_mtls(node1.http_port, "/metrics", &pki).unwrap().1;
    let m2_pre = https_get_mtls(node2.http_port, "/metrics", &pki).unwrap().1;
    let pre_out_1 = metric_value(&m1_pre, "memberlist_tcp_transport_outgoing_streams_total").unwrap_or(0.0);
    let pre_in_2 = metric_value(&m2_pre, "memberlist_tcp_transport_incoming_streams_total").unwrap_or(0.0);
    let pre_err_1 = metric_value(&m1_pre, "memberlist_tcp_transport_outgoing_stream_errors_total").unwrap_or(0.0);
    let pre_err_2 = metric_value(&m2_pre, "memberlist_tcp_transport_outgoing_stream_errors_total").unwrap_or(0.0);

    // -------------------------------------------------------------------------
    // Phase 3: Complete atomic swap (Key #2 + Client #2) and verify live reload
    // -------------------------------------------------------------------------
    atomic_replace(&pki.staged_path("server-2.key"), &pki.live_server_key()).unwrap();
    atomic_replace(&pki.staged_path("client-2.crt"), &pki.live_client_crt()).unwrap();
    atomic_replace(&pki.staged_path("client-2.key"), &pki.live_client_key()).unwrap();
    std::thread::sleep(Duration::from_millis(250));

    let client2 = (pki.live_client_crt(), pki.live_client_key());
    for node in [&mut node1, &mut node2] {
        node.assert_alive();
        assert_eq!(
            probe_peer_cert_serial(node.http_port, &pki.ca_crt(), Some((&client2.0, &client2.1)), None)
                .unwrap(),
            "02",
            "{} HTTPS must serve rotated Cert #2",
            node.name
        );
        assert_eq!(
            probe_peer_cert_serial(
                node.grpc_port,
                &pki.ca_crt(),
                Some((&client2.0, &client2.1)),
                Some("h2")
            )
            .unwrap(),
            "02",
            "{} gRPC must serve rotated Cert #2",
            node.name
        );
        assert_eq!(
            probe_peer_cert_serial(node.ml_port, &pki.ca_crt(), None, None).unwrap(),
            "02",
            "{} Memberlist must serve rotated Cert #2",
            node.name
        );

        let (st, _) = https_get_mtls(node.http_port, "/ready", &pki).unwrap();
        assert_eq!(st, 200, "{} HTTPS mTLS with Client #2", node.name);
        assert_eq!(
            grpc_ready_mtls(node.grpc_port, &pki).unwrap(),
            200,
            "{} gRPC mTLS with Client #2",
            node.name
        );
    }

    // Trigger & observe live Memberlist TCP push/pull syncs after Cert #2 rotation!
    let push_pull_ok = wait_until(Duration::from_secs(15), || {
        node1.assert_alive();
        node2.assert_alive();
        let Ok((200, m1)) = https_get_mtls(node1.http_port, "/metrics", &pki) else {
            return false;
        };
        let Ok((200, m2)) = https_get_mtls(node2.http_port, "/metrics", &pki) else {
            return false;
        };
        let out1 = metric_value(&m1, "memberlist_tcp_transport_outgoing_streams_total").unwrap_or(0.0);
        let in2 = metric_value(&m2, "memberlist_tcp_transport_incoming_streams_total").unwrap_or(0.0);
        let err1 = metric_value(&m1, "memberlist_tcp_transport_outgoing_stream_errors_total").unwrap_or(0.0);
        let err2 = metric_value(&m2, "memberlist_tcp_transport_outgoing_stream_errors_total").unwrap_or(0.0);
        let cnt1 = metric_value(&m1, "memberlist_client_cluster_members_count");
        let cnt2 = metric_value(&m2, "memberlist_client_cluster_members_count");
        out1 > pre_out_1
            && in2 > pre_in_2
            && err1 == pre_err_1
            && err2 == pre_err_2
            && cnt1 == Some(2.0)
            && cnt2 == Some(2.0)
    });
    if !push_pull_ok {
        let m1 = https_get_mtls(node1.http_port, "/metrics", &pki).map(|r| r.1).unwrap_or_default();
        let m2 = https_get_mtls(node2.http_port, "/metrics", &pki).map(|r| r.1).unwrap_or_default();
        let ml1_lines: Vec<&str> = m1.lines().filter(|l| l.contains("memberlist") && !l.starts_with('#')).collect();
        let ml2_lines: Vec<&str> = m2.lines().filter(|l| l.contains("memberlist") && !l.starts_with('#')).collect();
        panic!(
            "memberlist TCP push/pull over rotated Cert #2 failed (pre_out_1={pre_out_1}, pre_in_2={pre_in_2}):\nnode1 metrics:\n{}\nnode2 metrics:\n{}\nnode1 logs:\n{}\nnode2 logs:\n{}",
            ml1_lines.join("\n"),
            ml2_lines.join("\n"),
            node1.log_tail(),
            node2.log_tail()
        );
    }

    // Verify torn swap logged warning + fallback and completed swap logged reload.
    for node in [&node1, &node2] {
        let logs = node.log_text();
        assert!(
            logs.contains("keeping last-known-good certificate"),
            "{} expected torn-swap fallback warning in logs:
{}",
            node.name,
            node.log_tail()
        );
        assert!(
            logs.contains("reloaded TLS certificate and key"),
            "{} expected reload info in logs:
{}",
            node.name,
            node.log_tail()
        );
    }
}

#[test]
fn w10_client_mtls_cert_live_rotation_and_torn_swap() {
    let pki = Pki::new().expect("create test PKI");
    let cfg = mimir_common::tls::ClientConfig {
        cert_path: pki.live_client_crt().display().to_string(),
        key_path: pki.live_client_key().display().to_string(),
        ca_path: pki.ca_crt().display().to_string(),
        ..Default::default()
    };
    let resolved = cfg.get_tls_config().expect("initial client TLS config");

    let cert_serial_from_der = |der: &[u8]| -> String {
        let mut child = Command::new("openssl")
            .args(["x509", "-inform", "DER", "-noout", "-serial"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        child.stdin.take().unwrap().write_all(der).unwrap();
        let out = child.wait_with_output().unwrap();
        assert!(out.status.success());
        String::from_utf8_lossy(&out.stdout)
            .trim()
            .strip_prefix("serial=")
            .unwrap()
            .to_string()
    };

    // Initial Client #1 (serial 0B)
    let ck1 = resolved
        .config
        .client_auth_cert_resolver
        .resolve(&[], &[])
        .unwrap();
    assert_eq!(cert_serial_from_der(ck1.cert[0].as_ref()), "0B");

    // Torn swap: overwrite client.crt with client-2.crt while client.key still holds client-1.key
    atomic_replace(&pki.staged_path("client-2.crt"), &pki.live_client_crt()).unwrap();
    std::thread::sleep(Duration::from_millis(250));
    let ck_torn = resolved
        .config
        .client_auth_cert_resolver
        .resolve(&[], &[])
        .unwrap();
    assert_eq!(cert_serial_from_der(ck_torn.cert[0].as_ref()), "0B");

    // Complete swap: client.key -> client-2.key
    atomic_replace(&pki.staged_path("client-2.key"), &pki.live_client_key()).unwrap();
    std::thread::sleep(Duration::from_millis(250));
    let ck2 = resolved
        .config
        .client_auth_cert_resolver
        .resolve(&[], &[])
        .unwrap();
    assert_eq!(cert_serial_from_der(ck2.cert[0].as_ref()), "0C");


}
