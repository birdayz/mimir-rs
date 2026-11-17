//! CFG-17 / F16 — etcd KV store over TLS (server TLS + client mTLS), Go vs Rust.
//!
//! An etcd v3.5 server (`quay.io/coreos/etcd`, testcontainers) serves its client port over TLS
//! and requires client certificates signed by a test CA (`--client-cert-auth`). The unmodified Go
//! `mimir` (`target/go-mimir`) and the candidate (`target/release/mimir`, or
//! `MIMIR_CONFORMANCE_RUST_BIN`) run on the host as `-target=ingester` with the ingester ring on
//! `kvstore.store: etcd` and identical `etcd.tls_*` settings, and every scenario compares the
//! externally observable outcome:
//!
//! - valid mTLS: both become ready and share ONE ring in etcd (each `/ingester/ring` lists the Go
//!   and the Rust ingester as ACTIVE), also with `tls_server_name` set to a name the server
//!   certificate does not have and with legacy `tls_min_version` / `tls_cipher_suites` values
//!   (Go's etcd client applies neither) and with `tls_insecure_skip_verify` instead of a CA;
//! - no client certificate / untrusted CA: neither becomes ready, both keep running (clientv3
//!   waits for a connection, `WaitForReady`);
//! - invalid certificate configuration: both exit with code 1 at startup with dskit's
//!   `unable to initialise TLS configuration for etcd` and the etcd `TLSInfo` cause.
//!
//! Upstream: github.com/grafana/dskit/kv/etcd/etcd.go (`GetTLS`, `New`),
//! go.etcd.io/etcd/client/pkg/v3/transport/listener.go (`TLSInfo.ClientConfig`),
//! go.etcd.io/etcd/client/v3 (`WaitForReady`, endpoint resolver `ServerName`).
//!
//! Requires Docker and `openssl`. `MIMIR_CONFORMANCE_SKIP_DOCKER=1` skips explicitly (prints
//! `SKIPPED`). `MIMIR_CONFORMANCE_RUST_BIN=$PWD/target/go-mimir` validates the harness Go-vs-Go.
//! Leftover containers carry the label `org.mimir-rs.conformance=w8-etcd-tls`.

use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use anyhow::{bail, ensure, Context, Result};
use testcontainers::core::{AccessMode, ContainerPort, Mount, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

use mimir_common::block_on_for_tests;
use mimir_conformance::free_test_port;
use mimir_conformance::oracle::{ensure_go_oracle_built, go_mimir_bin, rust_mimir_release_bin, workspace_root};

const ETCD_IMAGE: &str = "quay.io/coreos/etcd";
const ETCD_TAG: &str = "v3.5.17";
const CONTAINER_LABEL: (&str, &str) = ("org.mimir-rs.conformance", "w8-etcd-tls");

// ---------------------------------------------------------------------------------------------
// Preconditions & binaries
// ---------------------------------------------------------------------------------------------

fn skip_docker(test: &str) -> bool {
    if std::env::var("MIMIR_CONFORMANCE_SKIP_DOCKER").as_deref() == Ok("1") {
        eprintln!("SKIPPED {test}: MIMIR_CONFORMANCE_SKIP_DOCKER=1 (this test needs Docker)");
        return true;
    }
    false
}

fn rust_bin_override() -> Option<PathBuf> {
    std::env::var_os("MIMIR_CONFORMANCE_RUST_BIN")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
}

/// Builds the Go binaries if missing and the Rust release binary (incrementally), once.
fn ensure_fresh_binaries() -> (PathBuf, PathBuf) {
    static BUILT: OnceLock<std::result::Result<(), String>> = OnceLock::new();
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
        panic!("building conformance binaries failed: {e}");
    }
    let abs = |p: PathBuf| if p.is_absolute() { p } else { workspace_root().join(p) };
    (
        abs(go_mimir_bin()),
        abs(rust_bin_override().unwrap_or_else(rust_mimir_release_bin)),
    )
}

// ---------------------------------------------------------------------------------------------
// Test PKI (openssl)
// ---------------------------------------------------------------------------------------------

/// CA + etcd server certificate (`localhost` / `127.0.0.1`; serverAuth AND clientAuth, because
/// etcd's own HTTP gateway dials its gRPC server with it) + client certificate, plus an
/// unrelated CA.
struct Pki {
    dir: tempfile::TempDir,
}

impl Pki {
    fn new() -> Result<Self> {
        let dir = tempfile::tempdir()?;
        let sh = |cmd: &str| -> Result<()> {
            let out = Command::new("sh").arg("-c").arg(cmd).current_dir(dir.path()).output()?;
            ensure!(out.status.success(), "{cmd}: {}", String::from_utf8_lossy(&out.stderr));
            Ok(())
        };
        for ca in ["ca", "other-ca"] {
            sh(&format!(
                "openssl req -x509 -newkey rsa:2048 -nodes -keyout {ca}.key -out {ca}.crt -days 2 -subj /CN=w8-etcd-{ca}"
            ))?;
        }
        for (name, ext) in [
            (
                "server",
                "subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth,clientAuth\n",
            ),
            ("client", "extendedKeyUsage=clientAuth\n"),
        ] {
            sh(&format!(
                "openssl req -newkey rsa:2048 -nodes -keyout {name}.key -out {name}.csr -subj /CN=w8-etcd-{name}"
            ))?;
            fs::write(dir.path().join(format!("{name}.ext")), ext)?;
            sh(&format!(
                "openssl x509 -req -in {name}.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out {name}.crt -days 2 -extfile {name}.ext"
            ))?;
        }
        // The container's etcd user must be able to read the server key.
        sh("chmod 644 *.key *.crt")?;
        Ok(Self { dir })
    }

    fn path(&self, f: &str) -> String {
        self.dir.path().join(f).display().to_string()
    }
}

// ---------------------------------------------------------------------------------------------
// etcd
// ---------------------------------------------------------------------------------------------

struct Etcd {
    _container: ContainerAsync<GenericImage>,
    port: u16,
}

impl Etcd {
    async fn start(pki: &Pki) -> Result<Self> {
        let port = free_test_port();
        let container = GenericImage::new(ETCD_IMAGE, ETCD_TAG)
            .with_wait_for(WaitFor::message_on_either_std("ready to serve client requests"))
            .with_entrypoint("etcd")
            .with_cmd([
                "--name=w8",
                "--data-dir=/tmp/etcd",
                "--listen-client-urls=https://0.0.0.0:2379",
                "--advertise-client-urls=https://127.0.0.1:2379",
                "--cert-file=/certs/server.crt",
                "--key-file=/certs/server.key",
                "--client-cert-auth",
                "--trusted-ca-file=/certs/ca.crt",
            ])
            .with_mount(
                Mount::bind_mount(pki.dir.path().display().to_string(), "/certs")
                    .with_access_mode(AccessMode::ReadOnly),
            )
            .with_label(CONTAINER_LABEL.0, CONTAINER_LABEL.1)
            .with_mapped_port(port, ContainerPort::Tcp(2379))
            .with_startup_timeout(Duration::from_secs(120))
            .start()
            .await
            .context("starting etcd via testcontainers (is Docker running?)")?;
        Ok(Self {
            _container: container,
            port,
        })
    }
}

// ---------------------------------------------------------------------------------------------
// mimir processes
// ---------------------------------------------------------------------------------------------

/// Kills and reaps the child on drop; keeps its stderr for diagnostics.
struct Proc {
    name: String,
    child: Option<Child>,
    stderr: Arc<Mutex<Vec<u8>>>,
    reader: Option<JoinHandle<()>>,
    http_port: u16,
    _dir: tempfile::TempDir,
}

impl Proc {
    fn spawn(name: &str, bin: &Path, etcd_port: u16, instance_id: &str, etcd_tls_yaml: &str) -> Result<Self> {
        let dir = tempfile::tempdir()?;
        let http_port = free_test_port();
        let grpc_port = free_test_port();
        let yaml = format!(
            r#"target: ingester
server:
  http_listen_port: {http_port}
  grpc_listen_port: {grpc_port}
ingester:
  ring:
    kvstore:
      store: etcd
      etcd:
        endpoints: [127.0.0.1:{etcd_port}]
        dial_timeout: 2s
        tls_enabled: true
{etcd_tls_yaml}    instance_id: {instance_id}
    instance_addr: 127.0.0.1
    min_ready_duration: 0s
    replication_factor: 1
    heartbeat_period: 1s
blocks_storage:
  backend: filesystem
  filesystem:
    dir: {dir}/bucket
  tsdb:
    dir: {dir}/tsdb
activity_tracker:
  filepath: ""
"#,
            dir = dir.path().display()
        );
        let cfg = dir.path().join("mimir.yaml");
        fs::write(&cfg, yaml)?;
        let mut child = Command::new(bin)
            .arg(format!("-config.file={}", cfg.display()))
            .current_dir(dir.path())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .with_context(|| format!("spawn {name} ({})", bin.display()))?;
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
        Ok(Self {
            name: name.to_string(),
            child: Some(child),
            stderr,
            reader,
            http_port,
            _dir: dir,
        })
    }

    fn stderr(&self) -> String {
        let s = String::from_utf8_lossy(&self.stderr.lock().map(|b| b.clone()).unwrap_or_default()).into_owned();
        let lines: Vec<&str> = s.lines().collect();
        lines[lines.len().saturating_sub(15)..].join("\n")
    }

    fn exit_code(&mut self) -> Option<i32> {
        let st = self.child.as_mut()?.try_wait().ok()??;
        if let Some(r) = self.reader.take() {
            let _ = r.join();
        }
        Some(st.code().unwrap_or(-1))
    }

    async fn get(&self, path: &str) -> Result<(u16, String)> {
        let resp = reqwest::Client::new()
            .get(format!("http://127.0.0.1:{}{path}", self.http_port))
            .header("Accept", "application/json")
            .timeout(Duration::from_secs(5))
            .send()
            .await?;
        let status = resp.status().as_u16();
        Ok((status, resp.text().await?))
    }

    /// The externally observable outcome within `timeout`.
    async fn outcome(&mut self, timeout: Duration) -> Outcome {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(code) = self.exit_code() {
                return Outcome::Exited(code);
            }
            if matches!(self.get("/ready").await, Ok((200, _))) {
                return Outcome::Ready;
            }
            if Instant::now() > deadline {
                return Outcome::NotReady;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    /// Instance IDs and states in `/ingester/ring` (JSON).
    async fn ring(&self) -> Result<Vec<(String, String)>> {
        let (status, body) = self.get("/ingester/ring").await?;
        ensure!(status == 200, "{}: GET /ingester/ring: {status}: {body}", self.name);
        let v: serde_json::Value = serde_json::from_str(&body)?;
        let mut out: Vec<(String, String)> = v["shards"]
            .as_array()
            .map(|shards| {
                shards
                    .iter()
                    .map(|s| {
                        (
                            s["id"].as_str().unwrap_or_default().to_string(),
                            s["state"].as_str().unwrap_or_default().to_string(),
                        )
                    })
                    .collect()
            })
            .unwrap_or_default();
        out.sort();
        Ok(out)
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Outcome {
    Ready,
    NotReady,
    Exited(i32),
}

// ---------------------------------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------------------------------

/// Starts Go then the candidate (distinct instance IDs, one shared etcd ring) and returns both
/// processes with their outcomes.
async fn run_pair(
    bins: &(PathBuf, PathBuf),
    etcd: &Etcd,
    scenario: &str,
    tls_yaml: &str,
    timeout: Duration,
) -> Result<[(Proc, Outcome); 2]> {
    let mut go = Proc::spawn(&format!("go/{scenario}"), &bins.0, etcd.port, &format!("go-{scenario}"), tls_yaml)?;
    let go_outcome = go.outcome(timeout).await;
    let mut cand = Proc::spawn(
        &format!("candidate/{scenario}"),
        &bins.1,
        etcd.port,
        &format!("candidate-{scenario}"),
        tls_yaml,
    )?;
    let cand_outcome = cand.outcome(timeout).await;
    Ok([(go, go_outcome), (cand, cand_outcome)])
}

fn ok_yaml(pki: &Pki, extra: &str) -> String {
    format!(
        "        tls_cert_path: {}\n        tls_key_path: {}\n        tls_ca_path: {}\n{extra}",
        pki.path("client.crt"),
        pki.path("client.key"),
        pki.path("ca.crt"),
    )
}

async fn scenario_ready_and_shared_ring(bins: &(PathBuf, PathBuf), etcd: &Etcd, name: &str, tls_yaml: &str) -> Result<()> {
    let [(go, g), (cand, c)] = run_pair(bins, etcd, name, tls_yaml, Duration::from_secs(60)).await?;
    ensure!(g == Outcome::Ready, "go: {g:?}\n{}", go.stderr());
    ensure!(c == Outcome::Ready, "candidate: {c:?} (go: {g:?})\n{}", cand.stderr());
    let want = [
        (format!("candidate-{name}"), "ACTIVE".to_string()),
        (format!("go-{name}"), "ACTIVE".to_string()),
    ];
    // Both ingesters registered through etcd TLS; each side reads the other's ring entry.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let (gr, cr) = (go.ring().await?, cand.ring().await?);
        let has = |r: &[(String, String)]| want.iter().all(|w| r.contains(w));
        if has(&gr) && has(&cr) {
            return Ok(());
        }
        if Instant::now() > deadline {
            bail!("shared etcd ring not converged: go sees {gr:?}, candidate sees {cr:?}");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn scenario_never_ready(bins: &(PathBuf, PathBuf), etcd: &Etcd, name: &str, tls_yaml: &str) -> Result<()> {
    let [(go, g), (cand, c)] = run_pair(bins, etcd, name, tls_yaml, Duration::from_secs(15)).await?;
    ensure!(g == Outcome::NotReady, "go: {g:?}\n{}", go.stderr());
    ensure!(c == g, "candidate: {c:?}, go: {g:?}\n{}", cand.stderr());
    Ok(())
}

async fn scenario_startup_error(
    bins: &(PathBuf, PathBuf),
    etcd: &Etcd,
    name: &str,
    tls_yaml: &str,
    cause: &str,
) -> Result<()> {
    let [(go, g), (cand, c)] = run_pair(bins, etcd, name, tls_yaml, Duration::from_secs(30)).await?;
    ensure!(g == Outcome::Exited(1), "go: {g:?}\n{}", go.stderr());
    ensure!(c == g, "candidate: {c:?}, go: {g:?}\n{}", cand.stderr());
    for p in [&go, &cand] {
        let err = p.stderr();
        ensure!(
            err.contains("unable to initialise TLS configuration for etcd") && err.contains(cause),
            "{}: stderr lacks `unable to initialise TLS configuration for etcd` / {cause:?}:\n{err}",
            p.name
        );
    }
    Ok(())
}

#[test]
fn w8_kv_etcd_tls_go_rust_parity() {
    const NAME: &str = "w8_kv_etcd_tls_go_rust_parity";
    if skip_docker(NAME) {
        return;
    }
    let bins = ensure_fresh_binaries();
    block_on_for_tests(async {
        let pki = Pki::new().expect("generate test PKI (openssl)");
        let etcd = Etcd::start(&pki).await.expect("start etcd");
        let mut failures = Vec::new();
        let mut record = |name: &str, r: Result<()>| match r {
            Ok(()) => eprintln!("PASS {name}"),
            Err(e) => {
                eprintln!("FAIL {name}: {e:#}");
                failures.push(format!("{name}: {e:#}"));
            }
        };

        record("mtls", scenario_ready_and_shared_ring(&bins, &etcd, "mtls", &ok_yaml(&pki, "")).await);
        // Go's etcd client never applies these (see etcd_tls.rs): accepted, without effect.
        record(
            "mtls_ignored_server_name_min_version_cipher_suites",
            scenario_ready_and_shared_ring(
                &bins,
                &etcd,
                "ignored",
                &ok_yaml(
                    &pki,
                    "        tls_server_name: not-in-the-certificate.example\n        tls_min_version: VersionTLS10\n        tls_cipher_suites: TLS_RSA_WITH_RC4_128_SHA\n",
                ),
            )
            .await,
        );
        record(
            "mtls_insecure_skip_verify",
            scenario_ready_and_shared_ring(
                &bins,
                &etcd,
                "insecure",
                &format!(
                    "        tls_cert_path: {}\n        tls_key_path: {}\n        tls_insecure_skip_verify: true\n",
                    pki.path("client.crt"),
                    pki.path("client.key")
                ),
            )
            .await,
        );
        record(
            "no_client_certificate",
            scenario_never_ready(&bins, &etcd, "nocert", &format!("        tls_ca_path: {}\n", pki.path("ca.crt"))).await,
        );
        record(
            "untrusted_server_ca",
            scenario_never_ready(
                &bins,
                &etcd,
                "untrusted",
                &format!(
                    "        tls_cert_path: {}\n        tls_key_path: {}\n        tls_ca_path: {}\n",
                    pki.path("client.crt"),
                    pki.path("client.key"),
                    pki.path("other-ca.crt")
                ),
            )
            .await,
        );
        record(
            "certificate_without_key",
            scenario_startup_error(
                &bins,
                &etcd,
                "nokey",
                &format!("        tls_cert_path: {}\n", pki.path("client.crt")),
                &format!("KeyFile and CertFile must both be present[key: , cert: {}]", pki.path("client.crt")),
            )
            .await,
        );
        record(
            "missing_certificate_file",
            scenario_startup_error(
                &bins,
                &etcd,
                "missing",
                "        tls_cert_path: /nonexistent/client.crt\n        tls_key_path: /nonexistent/client.key\n",
                "open /nonexistent/client.crt: no such file or directory",
            )
            .await,
        );
        record(
            "missing_ca_file",
            scenario_startup_error(
                &bins,
                &etcd,
                "missingca",
                "        tls_ca_path: /nonexistent/ca.crt\n",
                "open /nonexistent/ca.crt: no such file or directory",
            )
            .await,
        );

        assert!(failures.is_empty(), "failures:\n{}", failures.join("\n\n"));
    });
}
