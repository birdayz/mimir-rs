//! P0 hotfix — ring instance address auto-detection inside a pod-like container.
//!
//! The mimir-distributed Helm chart never sets `ingester.ring.instance_addr`, so the address
//! registered in the ring must be auto-detected from the pod's network interfaces exactly
//! like Go Mimir does; registering `127.0.0.1` makes distributors/queriers dial themselves.
//!
//! The host-built Go `mimir` (`target/go-mimir`) and Rust `mimir` (`target/release/mimir`) are
//! bind-mounted into `gcr.io/distroless/cc-debian13` containers (so `eth0` exists, like a
//! Kubernetes pod, and there is no `ip` binary, like the official images) and booted as
//! `-target=ingester` with byte-identical configs WITHOUT `instance_addr`. Every assertion is
//! differential (Go defines the expected behavior) plus the absolute check that the registered
//! address is the container's `eth0` address.
//!
//! Upstream behavior under test:
//! - github.com/grafana/mimir/pkg/ingester/ingester_ring.go (`InstanceInterfaceNames` default,
//!   `ToLifecyclerConfig` `InfNames`, `ToTokenlessBasicLifecyclerConfig` `GetInstanceAddr`)
//! - github.com/grafana/dskit/ring/lifecycler.go (`NewLifecycler`: `GetInstanceAddr`,
//!   `net.JoinHostPort`)
//! - github.com/grafana/dskit/ring/util.go (`GetInstanceAddr`, `GetInstancePort`)
//! - github.com/grafana/dskit/netutil/netutil.go (`PrivateNetworkInterfacesWithFallback`,
//!   `GetFirstAddressOf`)
//! - github.com/grafana/dskit/kv/memberlist/tcp_transport.go (`FinalAdvertiseAddr`) +
//!   github.com/hashicorp/go-sockaddr (`GetPrivateIP`)
//!
//! Requires Docker. `MIMIR_CONFORMANCE_SKIP_DOCKER=1` skips explicitly (prints `SKIPPED`).
//! `MIMIR_CONFORMANCE_RUST_BIN=/path/to/mimir` tests a prebuilt binary instead of rebuilding
//! `target/release/mimir` (`MIMIR_CONFORMANCE_RUST_BIN=target/go-mimir` validates the harness
//! Go-vs-Go). Leftover containers carry the label `org.mimir-rs.conformance=w8-instance-addr`.

use std::net::IpAddr;
use std::path::PathBuf;
use std::process::Command;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use testcontainers::core::{AccessMode, Mount, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, CopyTargetOptions, GenericImage, ImageExt};

use mimir_common::block_on_for_tests;
use mimir_conformance::oracle::{go_mimir_bin, rust_mimir_release_bin, workspace_root};

const DISTROLESS_IMAGE: &str = "gcr.io/distroless/cc-debian13";
const DISTROLESS_TAG: &str =
    "latest@sha256:809d04afa1be033c441f76bb0051878c342fb76d0ea236bc6dc94b655fa0fbd6";
const CONTAINER_LABEL: (&str, &str) = ("org.mimir-rs.conformance", "w8-instance-addr");
const HTTP_PORT: u16 = 8080;
const GRPC_PORT: u16 = 9095;
const MEMBERLIST_PORT: u16 = 7946;

// ---------------------------------------------------------------------------------------------
// Preconditions
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

/// Always rebuilds the Go binaries (if missing) and the Rust release binary (incremental), so
/// these tests can never silently exercise a stale `target/release/mimir`.
fn ensure_fresh_binaries() {
    static BUILT: OnceLock<std::result::Result<(), String>> = OnceLock::new();
    let res = BUILT.get_or_init(|| {
        let root = workspace_root();
        let status = Command::new(root.join("scripts").join("build-go-oracle.sh"))
            .current_dir(&root)
            .status()
            .map_err(|e| format!("spawn scripts/build-go-oracle.sh: {e}"))?;
        if !status.success() {
            return Err(format!("scripts/build-go-oracle.sh exited with {status}"));
        }
        if let Some(bin) = rust_bin_override() {
            eprintln!("using MIMIR_CONFORMANCE_RUST_BIN={}", bin.display());
            return Ok(());
        }
        let status = Command::new(std::env::var("CARGO").unwrap_or_else(|_| "cargo".into()))
            .args(["build", "--release", "-p", "mimir-cmd"])
            .current_dir(&root)
            .status()
            .map_err(|e| format!("spawn cargo build --release -p mimir-cmd: {e}"))?;
        if !status.success() {
            return Err(format!(
                "cargo build --release -p mimir-cmd exited with {status}"
            ));
        }
        Ok(())
    });
    if let Err(e) = res {
        panic!("building conformance binaries failed: {e}");
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum Side {
    Go,
    Rust,
}

impl Side {
    fn label(self) -> &'static str {
        match self {
            Side::Go => "go",
            Side::Rust => "rust",
        }
    }

    fn bin(self) -> PathBuf {
        let p = match self {
            Side::Go => go_mimir_bin(),
            Side::Rust => rust_bin_override().unwrap_or_else(rust_mimir_release_bin),
        };
        let p = if p.is_absolute() { p } else { workspace_root().join(p) };
        p.canonicalize().unwrap_or(p)
    }
}

// ---------------------------------------------------------------------------------------------
// Container harness
// ---------------------------------------------------------------------------------------------

/// Minimal ingester config. `ring_extra` is spliced (pre-indented) under `ingester.ring`.
fn ingester_yaml(ring_extra: &str) -> String {
    format!(
        r#"target: ingester
server:
  http_listen_port: {HTTP_PORT}
  grpc_listen_port: {GRPC_PORT}
ingester:
  ring:
    instance_id: ingester-0
    min_ready_duration: 0s
    replication_factor: 1
{ring_extra}blocks_storage:
  backend: filesystem
  filesystem:
    dir: /data/bucket
  tsdb:
    dir: /data/tsdb
    ship_interval: 0s
activity_tracker:
  filepath: ""
"#
    )
}

struct Pod {
    side: Side,
    container: ContainerAsync<GenericImage>,
}

impl Pod {
    async fn start(side: Side, scenario: &str, yaml: &str) -> Result<Self> {
        let bin = side.bin();
        anyhow::ensure!(bin.exists(), "{} binary {} missing", side.label(), bin.display());
        let container = GenericImage::new(DISTROLESS_IMAGE, DISTROLESS_TAG)
            .with_wait_for(WaitFor::Nothing)
            .with_entrypoint("/mimir")
            .with_cmd(["-config.file=/etc/mimir/mimir.yaml"])
            .with_label(CONTAINER_LABEL.0, CONTAINER_LABEL.1)
            .with_label("org.mimir-rs.side", side.label())
            .with_label("org.mimir-rs.scenario", scenario)
            .with_mount(
                Mount::bind_mount(bin.to_string_lossy().into_owned(), "/mimir")
                    .with_access_mode(AccessMode::ReadOnly),
            )
            .with_copy_to(
                CopyTargetOptions::new("/etc/mimir/mimir.yaml").with_mode(0o644),
                yaml.as_bytes().to_vec(),
            )
            .start()
            .await
            .with_context(|| {
                format!("starting {} {scenario} container (is Docker running?)", side.label())
            })?;
        Ok(Self { side, container })
    }

    /// The container's `eth0` address on the default bridge network.
    async fn eth0_ip(&self) -> Result<IpAddr> {
        Ok(self.container.get_bridge_ip_address().await?)
    }

    async fn logs(&self) -> String {
        let out = self.container.stdout_to_vec().await.unwrap_or_default();
        let err = self.container.stderr_to_vec().await.unwrap_or_default();
        let mut s = String::from_utf8_lossy(&out).into_owned();
        s.push_str(&String::from_utf8_lossy(&err));
        let lines: Vec<&str> = s.lines().collect();
        lines[lines.len().saturating_sub(25)..].join("\n")
    }

    async fn get_json(&self, path: &str) -> Result<serde_json::Value> {
        let ip = self.eth0_ip().await?;
        let host = match ip {
            IpAddr::V4(v4) => v4.to_string(),
            IpAddr::V6(v6) => format!("[{v6}]"),
        };
        let resp = reqwest::Client::new()
            .get(format!("http://{host}:{HTTP_PORT}{path}"))
            .header("Accept", "application/json")
            .timeout(Duration::from_secs(5))
            .send()
            .await?;
        let status = resp.status();
        let body = resp.text().await?;
        if !status.is_success() {
            bail!("GET {path}: HTTP {status}: {body}");
        }
        serde_json::from_str(&body).with_context(|| format!("GET {path}: invalid JSON: {body}"))
    }

    /// Polls `GET /ingester/ring` (JSON) until `ingester-0` is registered and returns its
    /// `address`.
    async fn ring_address(&self) -> Result<String> {
        let deadline = Instant::now() + Duration::from_secs(90);
        let mut last = String::new();
        while Instant::now() < deadline {
            if let Ok(Some(code)) = self.container.exit_code().await {
                bail!(
                    "{} exited with code {code} before registering in the ring:\n{}",
                    self.side.label(),
                    self.logs().await
                );
            }
            match self.get_json("/ingester/ring").await {
                Ok(v) => {
                    let addr = v["shards"].as_array().and_then(|shards| {
                        shards
                            .iter()
                            .find(|s| s["id"] == "ingester-0")
                            .and_then(|s| s["address"].as_str())
                            .map(str::to_string)
                    });
                    if let Some(addr) = addr {
                        return Ok(addr);
                    }
                    last = v.to_string();
                }
                Err(e) => last = format!("{e:#}"),
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
        bail!(
            "{}: ingester-0 never appeared in /ingester/ring (last: {last}):\n{}",
            self.side.label(),
            self.logs().await
        )
    }

    /// The local memberlist node's advertised `ip:port`, from `GET /memberlist` (JSON).
    ///
    /// Both Go and Rust serialise dskit's `StatusPageData`; the single member of this one-node
    /// cluster is the local node (`SortedMembers[0].Addr` + `Port`).
    async fn memberlist_advertise(&self) -> Result<String> {
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut last = String::new();
        while Instant::now() < deadline {
            match self.get_json("/memberlist").await {
                Ok(v) => {
                    if let Some(m) = v["SortedMembers"].as_array().and_then(|m| m.first()) {
                        if let (Some(ip), Some(port)) = (m["Addr"].as_str(), m["Port"].as_u64()) {
                            let ip: IpAddr = ip.parse()?;
                            return Ok(std::net::SocketAddr::new(ip, port as u16).to_string());
                        }
                    }
                    last = v.to_string();
                }
                Err(e) => last = format!("{e:#}"),
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
        bail!("{}: no memberlist advertise address (last: {last})", self.side.label())
    }

    /// Waits for the process to exit and returns its exit code.
    async fn wait_exit(&self) -> Result<i64> {
        let deadline = Instant::now() + Duration::from_secs(90);
        while Instant::now() < deadline {
            if let Some(code) = self.container.exit_code().await? {
                return Ok(code);
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
        bail!(
            "{} did not exit within 90s:\n{}",
            self.side.label(),
            self.logs().await
        )
    }

    async fn remove(self) {
        let _ = self.container.rm().await;
    }
}

async fn start_twin(scenario: &str, yaml: &str) -> Result<(Pod, Pod)> {
    let (go, rust) = tokio::join!(
        Pod::start(Side::Go, scenario, yaml),
        Pod::start(Side::Rust, scenario, yaml)
    );
    match (go, rust) {
        (Ok(go), Ok(rust)) => Ok((go, rust)),
        (Ok(p), Err(e)) | (Err(e), Ok(p)) => {
            p.remove().await;
            Err(e)
        }
        (Err(e), Err(_)) => Err(e),
    }
}

/// Registered ring address of both sides; asserts it is `<eth0 ip>:9095` for each and that
/// Go and Rust agree in form.
async fn assert_ring_addr_is_eth0(go: &Pod, rust: &Pod, errs: &mut Vec<String>) -> Result<()> {
    let (go_addr, rust_addr) = tokio::join!(go.ring_address(), rust.ring_address());
    let (go_addr, rust_addr) = (go_addr?, rust_addr?);
    let go_ip = go.eth0_ip().await?;
    let rust_ip = rust.eth0_ip().await?;
    let go_expected = format!("{go_ip}:{GRPC_PORT}");
    let rust_expected = format!("{rust_ip}:{GRPC_PORT}");
    if go_addr != go_expected {
        bail!("harness: Go registered {go_addr:?}, expected eth0 address {go_expected:?}");
    }
    if rust_addr != rust_expected {
        errs.push(format!(
            "Rust registered {rust_addr:?} in the ring, expected eth0 address {rust_expected:?} \
             (Go registered {go_addr:?} = its eth0)"
        ));
    }
    // Identical in form: same address with the container-specific IP abstracted away.
    let go_form = go_addr.replace(&go_ip.to_string(), "<eth0>");
    let rust_form = rust_addr.replace(&rust_ip.to_string(), "<eth0>");
    if go_form != rust_form {
        errs.push(format!("ring address form differs: Go {go_form:?}, Rust {rust_form:?}"));
    }
    Ok(())
}

fn finish(scenario: &str, errs: Vec<String>) {
    assert!(
        errs.is_empty(),
        "{scenario}: Go/Rust divergence:\n - {}",
        errs.join("\n - ")
    );
}

// ---------------------------------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------------------------------

/// Helm-chart shape: no `instance_addr`, no `instance_interface_names` (default = private
/// interfaces, i.e. `[eth0]` in a pod), inmemory ring KV.
#[test]
fn w8_ring_addr_autodetected_inmemory_kv() {
    if skip_docker("w8_ring_addr_autodetected_inmemory_kv") {
        return;
    }
    ensure_fresh_binaries();
    block_on_for_tests(async {
        let yaml = ingester_yaml("    kvstore:\n      store: inmemory\n");
        let (go, rust) = start_twin("inmemory", &yaml).await.unwrap();
        let mut errs = Vec::new();
        let res = assert_ring_addr_is_eth0(&go, &rust, &mut errs).await;
        go.remove().await;
        rust.remove().await;
        res.unwrap();
        finish("inmemory", errs);
    });
}

/// Default (memberlist) ring KV with the default `0.0.0.0` bind address: the ring address and
/// the memberlist advertise address (go-sockaddr `GetPrivateIP`) must both be `eth0`.
#[test]
fn w8_ring_and_memberlist_addr_autodetected_memberlist_kv() {
    if skip_docker("w8_ring_and_memberlist_addr_autodetected_memberlist_kv") {
        return;
    }
    ensure_fresh_binaries();
    block_on_for_tests(async {
        let yaml = ingester_yaml("");
        let (go, rust) = start_twin("memberlist", &yaml).await.unwrap();
        let mut errs = Vec::new();
        let res = async {
            assert_ring_addr_is_eth0(&go, &rust, &mut errs).await?;
            let (go_ml, rust_ml) = tokio::join!(go.memberlist_advertise(), rust.memberlist_advertise());
            let (go_ml, rust_ml) = (go_ml?, rust_ml?);
            let go_expected = format!("{}:{MEMBERLIST_PORT}", go.eth0_ip().await?);
            let rust_expected = format!("{}:{MEMBERLIST_PORT}", rust.eth0_ip().await?);
            if go_ml != go_expected {
                bail!("harness: Go memberlist advertises {go_ml:?}, expected {go_expected:?}");
            }
            if rust_ml != rust_expected {
                errs.push(format!(
                    "Rust memberlist advertises {rust_ml:?}, expected {rust_expected:?} (Go: {go_ml:?})"
                ));
            }
            Ok(())
        }
        .await;
        go.remove().await;
        rust.remove().await;
        res.unwrap();
        finish("memberlist", errs);
    });
}

/// Explicit IPv6 `instance_addr`: registered as `net.JoinHostPort(addr, port)`.
#[test]
fn w8_ring_addr_explicit_ipv6_is_bracketed() {
    if skip_docker("w8_ring_addr_explicit_ipv6_is_bracketed") {
        return;
    }
    ensure_fresh_binaries();
    block_on_for_tests(async {
        let yaml = ingester_yaml(
            "    kvstore:\n      store: inmemory\n    instance_addr: \"fd00:1234::7\"\n",
        );
        let (go, rust) = start_twin("ipv6", &yaml).await.unwrap();
        let (go_addr, rust_addr) = tokio::join!(go.ring_address(), rust.ring_address());
        go.remove().await;
        rust.remove().await;
        let (go_addr, rust_addr) = (go_addr.unwrap(), rust_addr.unwrap());
        assert_eq!(go_addr, format!("[fd00:1234::7]:{GRPC_PORT}"), "harness: Go");
        assert_eq!(rust_addr, go_addr, "Rust ring address differs from Go");
    });
}

/// `instance_interface_names` naming a non-existent interface: no address can be found, so
/// both must fail to start (never fall back to 127.0.0.1) with the same exit code.
#[test]
fn w8_ring_addr_nonexistent_interface_fails_startup() {
    if skip_docker("w8_ring_addr_nonexistent_interface_fails_startup") {
        return;
    }
    ensure_fresh_binaries();
    block_on_for_tests(async {
        let yaml = ingester_yaml(
            "    kvstore:\n      store: inmemory\n    instance_interface_names: [nope0]\n",
        );
        let (go, rust) = start_twin("no-such-interface", &yaml).await.unwrap();
        let (go_code, rust_code) = tokio::join!(go.wait_exit(), rust.wait_exit());
        let (go_logs, rust_logs) = (go.logs().await, rust.logs().await);
        go.remove().await;
        rust.remove().await;
        let go_code = go_code.unwrap();
        let rust_code = rust_code.unwrap();
        const MSG: &str = "no useable address found for interfaces [nope0]";
        assert_ne!(go_code, 0, "harness: Go must fail to start:\n{go_logs}");
        assert!(go_logs.contains(MSG), "harness: Go error message:\n{go_logs}");
        assert_eq!(
            rust_code, go_code,
            "exit code differs (Go {go_code}, Rust {rust_code}).\nGo:\n{go_logs}\nRust:\n{rust_logs}"
        );
        assert!(rust_logs.contains(MSG), "Rust must report {MSG:?}:\n{rust_logs}");
    });
}
