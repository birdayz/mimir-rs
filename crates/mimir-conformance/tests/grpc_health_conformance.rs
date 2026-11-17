//! Differential conformance tests for gRPC health checking (`grpc.health.v1.Health/{Check,List,Watch}`)
//! using the real upstream Go `dskit/ring/client/pool.go` + `pkg/ingester/client` + `grpc_health_v1`
//! against both `target/go-mimir` and `target/release/mimir`.
//!
//! Tests:
//! - All 4 `ingester.client.grpc-compression` codecs (`""`, `"gzip"`, `"snappy"`, `"s2"`)
//! - `multitenancy_enabled: true` (`Check` exempt in `noGRPCAuthOn`; `List`/`Watch` require org id)
//! - `server.cluster_validation.grpc.enabled: true` (`Health/*` exempt in `ClusterUnaryServerInterceptor`,
//!   while `cortex.Ingester/*` and `httpgrpc.HTTP/Handle` enforce `x-cluster`)
//! - Real `distributor.NewPool` (`cleanUnhealthy` -> `healthCheck` loop) keeping the ingester
//!   client in the pool with zero failed healthcheck logs.

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

use mimir_conformance::oracle::invoke_oracle;
use mimir_conformance::twin::{binary, free_tcp_ports, grpc_health, http_get, Impl, Proc};
use serde::{Deserialize, Serialize};

#[derive(Serialize)]
struct ProbeInput {
    grpc_addr: String,
    compression: String,
    cluster_validation_label: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct RpcOutcome {
    code: String,
    message: String,
    #[serde(default)]
    status: i32,
    #[serde(default)]
    list: BTreeMap<String, i32>,
    #[serde(default)]
    extra: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct ProbeOutput {
    pool_count_after_checks: usize,
    pool_logs: Vec<String>,
    check_empty_svc_with_org: RpcOutcome,
    check_named_svc_with_org: RpcOutcome,
    check_no_org: RpcOutcome,
    list_with_org: RpcOutcome,
    list_no_org: RpcOutcome,
    watch_with_org: RpcOutcome,
    watch_no_org: RpcOutcome,
    label_names_with_org: RpcOutcome,
    httpgrpc_ready_with_org: RpcOutcome,
}

fn config_yaml(http_port: u16, grpc_port: u16, dir: &Path) -> String {
    std::fs::create_dir_all(dir).expect("mkdir");
    format!(
        r#"target: ingester
multitenancy_enabled: true
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http_port}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc_port}
  cluster_validation:
    label: prod-cluster
    grpc:
      enabled: true
      soft_validation: false
ingester:
  ring:
    instance_id: ingester-health-0
    instance_addr: 127.0.0.1
    kvstore:
      store: inmemory
    replication_factor: 1
    min_ready_duration: 0s
    final_sleep: 0s
blocks_storage:
  backend: filesystem
  filesystem:
    dir: {bucket}
  tsdb:
    dir: {tsdb}
    ship_interval: 10m
    head_compaction_interval: 10m
activity_tracker:
  filepath: ""
"#,
        bucket = dir.join("bucket").display(),
        tsdb = dir.join("tsdb").display(),
    )
}

#[test]
fn grpc_health_differential_with_go_ring_pool_and_all_compressions() {
    let go_bin = binary(Impl::Go);
    let rust_bin = binary(Impl::Rust);
    let tmp = tempfile::tempdir().expect("tempdir");

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let http = reqwest::Client::new();

    let mut procs = Vec::new();
    for (which, bin) in [(Impl::Go, &go_bin), (Impl::Rust, &rust_bin)] {
        let ports = free_tcp_ports(2);
        let (http_port, grpc_port) = (ports[0], ports[1]);
        let dir = tmp.path().join(which.name());
        let yaml = config_yaml(http_port, grpc_port, &dir);
        let cfg_path = dir.join("mimir.yaml");
        std::fs::write(&cfg_path, yaml).expect("write yaml");
        let args = vec![format!("-config.file={}", cfg_path.display())];
        let proc = Proc::spawn(which.name(), bin, &args, &dir);
        rt.block_on(async {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
            loop {
                if let Ok((200, _, _)) = http_get(&http, http_port, "/ready", true).await {
                    break;
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "{} never became /ready; log:\n{}",
                    which.name(),
                    proc.log()
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        });
        procs.push((which.name(), grpc_port, proc));
    }

    let cases: &[(&str, &str)] = &[
        ("", ""),
        ("", "prod-cluster"),
        ("", "wrong-cluster"),
        ("gzip", "prod-cluster"),
        ("gzip", ""),
        ("snappy", "prod-cluster"),
        ("snappy", "wrong-cluster"),
        ("s2", "prod-cluster"),
        ("s2", ""),
    ];

    for &(compression, cluster_label) in cases {
        let mut results: Vec<(&str, ProbeOutput)> = Vec::new();
        for (name, grpc_port, proc) in &procs {
            let out: ProbeOutput = invoke_oracle(
                "grpc-health-probe",
                &ProbeInput {
                    grpc_addr: format!("127.0.0.1:{grpc_port}"),
                    compression: compression.to_string(),
                    cluster_validation_label: cluster_label.to_string(),
                },
            )
            .unwrap_or_else(|e| {
                panic!(
                    "go-oracle grpc-health-probe failed for {name} (compression={compression:?}, cluster={cluster_label:?}): {e:#}\nlog:\n{}",
                    proc.log()
                )
            });
            results.push((name, out));
        }

        let (_, go) = &results[0];
        let (_, rust) = &results[1];
        assert_eq!(
            rust, go,
            "Go vs Rust mismatch for compression={compression:?}, cluster_label={cluster_label:?}"
        );

        // Explicit assertions so neither side can regress identically:
        assert_eq!(
            rust.pool_count_after_checks, 1,
            "ring pool evicted ingester client (compression={compression:?}, cluster={cluster_label:?}): logs={:?}",
            rust.pool_logs
        );
        assert!(
            rust.pool_logs.is_empty(),
            "unexpected ring pool logs: {:?}",
            rust.pool_logs
        );
        assert_eq!(rust.check_empty_svc_with_org.code, "OK");
        assert_eq!(rust.check_empty_svc_with_org.status, 1);
        assert_eq!(rust.check_named_svc_with_org.code, "OK");
        assert_eq!(rust.check_named_svc_with_org.status, 1);
        assert_eq!(rust.check_no_org.code, "OK");
        assert_eq!(rust.check_no_org.status, 1);

        assert_eq!(rust.list_with_org.code, "OK");
        assert_eq!(rust.list_with_org.list.get("server").copied(), Some(1));
        assert_eq!(rust.list_no_org.code, "Unknown");
        assert_eq!(rust.list_no_org.message, "no org id");

        assert_eq!(rust.watch_with_org.code, "Unimplemented");
        assert_eq!(rust.watch_with_org.message, "Watching is not supported");
        assert_eq!(rust.watch_no_org.code, "Unknown");
        assert_eq!(rust.watch_no_org.message, "no org id");

        if cluster_label == "prod-cluster" {
            assert_eq!(rust.label_names_with_org.code, "OK");
            assert_eq!(rust.httpgrpc_ready_with_org.code, "HTTP_200");
            assert_eq!(rust.httpgrpc_ready_with_org.message, "ready");
        } else if cluster_label.is_empty() {
            assert_eq!(rust.label_names_with_org.code, "FailedPrecondition");
            assert_eq!(rust.httpgrpc_ready_with_org.code, "FailedPrecondition");
        } else {
            // ClusterUnaryClientInterceptor translates FailedPrecondition+WRONG_CLUSTER_VALIDATION_LABEL
            // into Internal ("request rejected by the server: ...").
            assert_eq!(rust.label_names_with_org.code, "Internal");
            assert!(
                rust.label_names_with_org
                    .message
                    .contains("request rejected by the server:"),
                "unexpected message: {:?}",
                rust.label_names_with_org
            );
            assert_eq!(rust.httpgrpc_ready_with_org.code, "Internal");
        }
    }

    // Verify shutdown flips Health/Check to NOT_SERVING (2) in both Go and Rust.
    // (Use a process with shutdown_delay=5s so the gRPC server stays up after SIGTERM.)
    drop(procs);
    let tmp2 = tempfile::tempdir().expect("tempdir2");
    for (which, bin) in [(Impl::Go, &go_bin), (Impl::Rust, &rust_bin)] {
        let ports = free_tcp_ports(2);
        let (http_port, grpc_port) = (ports[0], ports[1]);
        let dir = tmp2.path().join(which.name());
        let yaml = format!(
            "shutdown_delay: 5s\n{}",
            config_yaml(http_port, grpc_port, &dir)
        );
        let cfg_path = dir.join("mimir.yaml");
        std::fs::write(&cfg_path, yaml).expect("write yaml");
        let args = vec![format!("-config.file={}", cfg_path.display())];
        let proc = Proc::spawn(which.name(), bin, &args, &dir);
        rt.block_on(async {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
            loop {
                if let Ok((200, _, _)) = http_get(&http, http_port, "/ready", true).await {
                    break;
                }
                assert!(tokio::time::Instant::now() < deadline);
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            assert_eq!(grpc_health(grpc_port).await.unwrap(), 1);
            proc.sigterm();
            let deadline = tokio::time::Instant::now() + Duration::from_secs(4);
            loop {
                if let Ok(2) = grpc_health(grpc_port).await {
                    break;
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "{} Health/Check did not flip to NOT_SERVING after SIGTERM; log:\n{}",
                    which.name(),
                    proc.log()
                );
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        });
    }
}
