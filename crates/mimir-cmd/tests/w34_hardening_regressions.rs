//! Wave 34 hardening regression tests for `mimir-cmd`.

use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::routing::get;
use axum::Router;
use bytes::Bytes;
use mimir_cmd::grpc::drive_bounded_grpc_decompress_for_test;
use mimir_cmd::server::grpc_server::{self, GrpcContext, GrpcServerConfig};
use mimir_cmd::server::http_server::{
    self, probe_request_inflight_guard_drop_for_test, HttpServerConfig,
};
use mimir_cmd::server::listener::{AcceptOptions, Acceptor};
use mimir_cmd::server::metrics::ServerMetrics;
use mimir_cmd::server::tls::{server_config, TlsOptions};
use mimir_cmd::server::Server;
use mimir_cmd::usagestats::{
    build_report, new_cluster_seed, Reporter, ReporterConfig, DEFAULT_REPORT_SEND_INTERVAL,
};
use mimir_common::logging::{LogLevel, Logger};
use mimir_config::{ServerConfig, TLSConfig};
use mimir_storage::bucket::BucketClient;
use prometheus::{Gauge, Registry};
use rustls::pki_types::{CertificateDer, ServerName};
use rustls::pki_types::pem::PemObject;
use rustls::{RootCertStore, SignatureScheme};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{watch, Notify};

struct NoopLogger;
impl Logger for NoopLogger {
    fn log(&self, _level: LogLevel, _fields: &[(&str, &str)]) {}
}

fn gen_ca_and_certs(dir: &Path, serial: u32) -> (PathBuf, PathBuf, PathBuf, PathBuf, PathBuf) {
    let run = |args: &[&str]| {
        let out = Command::new("openssl")
            .args(args)
            .current_dir(dir)
            .output()
            .expect("spawn openssl");
        assert!(
            out.status.success(),
            "openssl {args:?} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    };
    let ca_key = dir.join(format!("ca-{serial}.key"));
    let ca_crt = dir.join(format!("ca-{serial}.crt"));
    let srv_key = dir.join(format!("server-{serial}.key"));
    let srv_csr = dir.join(format!("server-{serial}.csr"));
    let srv_crt = dir.join(format!("server-{serial}.crt"));
    let cli_key = dir.join(format!("client-{serial}.key"));
    let cli_csr = dir.join(format!("client-{serial}.csr"));
    let cli_crt = dir.join(format!("client-{serial}.crt"));
    let ext = dir.join(format!("ext-{serial}.cnf"));
    std::fs::write(
        &ext,
        "subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth,clientAuth\n",
    )
    .unwrap();

    let ser_hex = format!("0x{serial:02x}");
    run(&[
        "req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:prime256v1",
        "-nodes", "-keyout", ca_key.to_str().unwrap(), "-out", ca_crt.to_str().unwrap(),
        "-days", "2", "-subj", &format!("/CN=w34-ca-{serial}"),
    ]);
    run(&[
        "req", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:prime256v1", "-nodes",
        "-keyout", srv_key.to_str().unwrap(), "-out", srv_csr.to_str().unwrap(),
        "-subj", "/CN=localhost",
    ]);
    run(&[
        "x509", "-req", "-in", srv_csr.to_str().unwrap(), "-CA", ca_crt.to_str().unwrap(),
        "-CAkey", ca_key.to_str().unwrap(), "-set_serial", &ser_hex,
        "-out", srv_crt.to_str().unwrap(), "-days", "2", "-extfile", ext.to_str().unwrap(),
    ]);
    run(&[
        "req", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:prime256v1", "-nodes",
        "-keyout", cli_key.to_str().unwrap(), "-out", cli_csr.to_str().unwrap(),
        "-subj", &format!("/CN=client-{serial}"),
    ]);
    run(&[
        "x509", "-req", "-in", cli_csr.to_str().unwrap(), "-CA", ca_crt.to_str().unwrap(),
        "-CAkey", ca_key.to_str().unwrap(), "-set_serial", &format!("0x{:02x}", serial + 100),
        "-out", cli_crt.to_str().unwrap(), "-days", "2", "-extfile", ext.to_str().unwrap(),
    ]);
    (ca_crt, srv_crt, srv_key, cli_crt, cli_key)
}

#[tokio::test]
async fn w34_http_and_grpc_servers_exit_cleanly_on_fatal_accept_error_and_drain_tasks() {
    // 1. HTTP accept loop breaks cleanly instead of spinning 100% CPU when accept() fails fatally (EINVAL).
    let std_http = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    std_http.set_nonblocking(true).unwrap();
    let http_fd = std_http.as_raw_fd();
    let http_listener = TcpListener::from_std(std_http).unwrap();
    let http_acceptor = Acceptor::new(
        http_listener,
        AcceptOptions {
            conn_limit: 0,
            connections_gauge: Gauge::new("test_http_conns", "h").unwrap(),
            proxy_protocol: false,
            proxy_header_timeout: Duration::from_secs(5),
        },
    );
    let (_shut_tx, shut_rx) = watch::channel(false);
    let http_cfg = HttpServerConfig {
        read_timeout: Duration::from_secs(5),
        read_header_timeout: Duration::from_secs(5),
        write_timeout: Duration::from_secs(5),
        idle_timeout: Duration::from_secs(5),
        tls: None,
        graceful_shutdown_timeout: Duration::from_secs(1),
        zero_response_logger: None,
    };
    let http_task = tokio::spawn(http_server::serve(
        http_acceptor,
        http_cfg,
        Router::new().route("/ok", get(|| async { "ok" })),
        shut_rx,
    ));
    tokio::time::sleep(Duration::from_millis(20)).await;
    // Shutdown the listening socket fd so Linux accept() returns EINVAL (fatal non-transient error).
    unsafe {
        libc::shutdown(http_fd, libc::SHUT_RDWR);
    }
    tokio::time::timeout(Duration::from_secs(2), http_task)
        .await
        .expect("http_server::serve must break on fatal accept error instead of spinning")
        .unwrap();

    // 2. gRPC accept loop also breaks cleanly on fatal accept error.
    let std_grpc = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    std_grpc.set_nonblocking(true).unwrap();
    let grpc_fd = std_grpc.as_raw_fd();
    let grpc_listener = TcpListener::from_std(std_grpc).unwrap();
    let grpc_acceptor = Acceptor::new(
        grpc_listener,
        AcceptOptions {
            conn_limit: 0,
            connections_gauge: Gauge::new("test_grpc_conns", "g").unwrap(),
            proxy_protocol: false,
            proxy_header_timeout: Duration::from_secs(5),
        },
    );
    let reg = Registry::new();
    let metrics = ServerMetrics::new(&reg, false, false).unwrap();
    let grpc_ctx = GrpcContext {
        metrics,
        logger: Arc::new(NoopLogger),
    };
    let grpc_cfg = GrpcServerConfig {
        max_concurrent_streams: 100,
        keepalive_time: None,
        keepalive_timeout: Duration::from_secs(20),
        max_connection_idle: None,
        max_connection_age: None,
        max_connection_age_grace: None,
        tls: None,
        stats_tracking: false,
        report_grpc_status: true,
        cluster_validation: None,
        multitenancy_enabled: false,
        no_auth_tenant: "anonymous".to_string(),
        tap_ingester: None,
    };
    let (_gshut_tx, gshut_rx) = watch::channel(false);
    let grpc_task = tokio::spawn(grpc_server::serve(
        grpc_acceptor,
        grpc_cfg,
        tonic::service::Routes::default(),
        grpc_ctx,
        gshut_rx,
    ));
    tokio::time::sleep(Duration::from_millis(20)).await;
    unsafe {
        libc::shutdown(grpc_fd, libc::SHUT_RDWR);
    }
    tokio::time::timeout(Duration::from_secs(2), grpc_task)
        .await
        .expect("grpc_server::serve must break on fatal accept error instead of spinning")
        .unwrap();

    // 3. Sustained burst of 40 HTTP requests exercises JoinSet try_join_next draining cleanly.
    let cfg = ServerConfig {
        http_listen_address: "127.0.0.1".to_string(),
        http_listen_port: 0,
        grpc_listen_address: "127.0.0.1".to_string(),
        grpc_listen_port: 0,
        ..Default::default()
    };
    let srv = Server::new(&cfg, &Registry::new(), Arc::new(NoopLogger)).unwrap();
    let addr = srv.http_addr().unwrap();
    let running = srv
        .start(
            Router::new().route("/ping", get(|| async { "pong" })),
            tonic::service::Routes::default(),
        )
        .unwrap();
    let client = reqwest::Client::new();
    for _ in 0..40 {
        let txt = client
            .get(format!("http://{addr}/ping"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert_eq!(txt, "pong");
    }
    tokio::time::timeout(Duration::from_secs(2), running.shutdown())
        .await
        .expect("server shutdown after burst must be prompt");
}

#[tokio::test]
async fn w34_http2_rst_stream_does_not_leak_inflight_requests() {
    // 1. Direct RAII guard drop probe: inflight increments to 1 and drops back to 0.
    let (during, after) = probe_request_inflight_guard_drop_for_test();
    assert_eq!(during, 1);
    assert_eq!(after, 0);

    // 2. End-to-end HTTP/2 TLS stream reset (RST_STREAM) mid-handler.
    let dir = TempDir::new().unwrap();
    let (ca_crt, srv_crt, srv_key, _cli_crt, _cli_key) = gen_ca_and_certs(dir.path(), 1);

    let cfg = ServerConfig {
        http_listen_address: "127.0.0.1".to_string(),
        http_listen_port: 0,
        grpc_listen_address: "127.0.0.1".to_string(),
        grpc_listen_port: 0,
        graceful_shutdown_timeout: mimir_config::GoDuration::from_secs(1),
        http_tls_config: TLSConfig {
            cert_file: srv_crt.to_string_lossy().into_owned(),
            key_file: srv_key.to_string_lossy().into_owned(),
            ..Default::default()
        },
        ..Default::default()
    };

    let entered = Arc::new(Notify::new());
    let entered_srv = Arc::clone(&entered);
    let srv = Server::new(&cfg, &Registry::new(), Arc::new(NoopLogger)).unwrap();
    let http_addr = srv.http_addr().unwrap();
    let router = Router::new()
        .route(
            "/slow",
            get(move || {
                let entered = Arc::clone(&entered_srv);
                async move {
                    entered.notify_one();
                    std::future::pending::<&'static str>().await
                }
            }),
        )
        .route("/ok", get(|| async { "ok" }));
    let running = srv.start(router, tonic::service::Routes::default()).unwrap();

    let mut roots = RootCertStore::empty();
    for c in CertificateDer::pem_file_iter(&ca_crt).unwrap().map(Result::unwrap) {
        roots.add(c).unwrap();
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut client_cfg = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    client_cfg.alpn_protocols = vec![b"h2".to_vec()];
    let connector = tokio_rustls::TlsConnector::from(Arc::new(client_cfg));
    let tcp = tokio::net::TcpStream::connect(http_addr).await.unwrap();
    let tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), tcp)
        .await
        .unwrap();

    let (mut send_req, conn) = h2::client::handshake(tls).await.unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });

    // Send GET /slow and wait until the server handler has started.
    let req = http::Request::builder()
        .method("GET")
        .uri("https://localhost/slow")
        .body(())
        .unwrap();
    let (resp_fut, mut send_stream) = send_req.send_request(req, true).unwrap();
    entered.notified().await;

    // Cancel the stream with RST_STREAM mid-request and drop the response future.
    send_stream.send_reset(h2::Reason::CANCEL);
    drop(resp_fut);

    // Send /ok on the same H2 connection and confirm it succeeds.
    let ok_req = http::Request::builder()
        .method("GET")
        .uri("https://localhost/ok")
        .body(())
        .unwrap();
    let (ok_fut, _) = send_req.send_request(ok_req, true).unwrap();
    let ok_resp = ok_fut.await.unwrap();
    assert_eq!(ok_resp.status(), http::StatusCode::OK);

    // Graceful shutdown completes promptly without hanging on leaked inflight count.
    drop(send_req);
    tokio::time::timeout(Duration::from_secs(2), running.shutdown())
        .await
        .expect("server shutdown after HTTP/2 RST_STREAM must not hang");
}

#[tokio::test]
async fn w34_tls_client_ca_reload_retains_last_good_on_zero_certs_and_clears_hints_after_rotation() {
    let dir = TempDir::new().unwrap();
    let (ca1_crt, srv_crt, srv_key, cli1_crt, cli1_key) = gen_ca_and_certs(dir.path(), 1);
    let (ca2_crt, _srv2_crt, _srv2_key, cli2_crt, cli2_key) = gen_ca_and_certs(dir.path(), 2);

    let active_ca = dir.path().join("active_client_ca.crt");
    std::fs::copy(&ca1_crt, &active_ca).unwrap();

    let cfg = TLSConfig {
        cert_file: srv_crt.to_string_lossy().into_owned(),
        key_file: srv_key.to_string_lossy().into_owned(),
        client_auth_type: "RequireAndVerifyClientCert".to_string(),
        client_ca_file: active_ca.to_string_lossy().into_owned(),
        ..Default::default()
    };
    let sc = server_config(&cfg, &TlsOptions::default(), &[b"http/1.1"]).unwrap();
    let acceptor = tokio_rustls::TlsAcceptor::from(sc);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let srv_task = tokio::spawn(async move {
        while let Ok((sock, _)) = listener.accept().await {
            let acc = acceptor.clone();
            tokio::spawn(async move {
                if let Ok(mut tls) = acc.accept(sock).await {
                    let _ = tls.write_all(b"ok").await;
                    let _ = tls.shutdown().await;
                }
            });
        }
    });

    let mut srv_roots = RootCertStore::empty();
    for c in CertificateDer::pem_file_iter(&ca1_crt).unwrap().map(Result::unwrap) {
        srv_roots.add(c).unwrap();
    }
    let srv_roots = Arc::new(srv_roots);
    let provider = Arc::new(rustls::crypto::ring::default_provider());

    let load_ck = |crt_path: &Path, key_path: &Path| -> Arc<rustls::sign::CertifiedKey> {
        Arc::new(
            mimir_common::tls::x509_key_pair(
                &std::fs::read(crt_path).unwrap(),
                &std::fs::read(key_path).unwrap(),
                &provider,
            )
            .unwrap(),
        )
    };
    let ck1 = load_ck(&cli1_crt, &cli1_key);
    let ck2 = load_ck(&cli2_crt, &cli2_key);

    // 1. Initial handshake with client1 (CA #1) succeeds.
    let cli1_cfg = rustls::ClientConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(Arc::clone(&srv_roots))
        .with_client_cert_resolver(Arc::new(StrictHintFilterResolver {
            ck: Arc::clone(&ck1),
            allow_non_empty_only_if_matches_ca_pem: Some(std::fs::read(&ca1_crt).unwrap()),
            seen_hints: Arc::new(parking_lot::Mutex::new(None)),
        }));
    let tcp1 = tokio::net::TcpStream::connect(addr).await.unwrap();
    let mut tls1 = tokio_rustls::TlsConnector::from(Arc::new(cli1_cfg))
        .connect(ServerName::try_from("localhost").unwrap(), tcp1)
        .await
        .unwrap();
    let mut b1 = [0u8; 2];
    tls1.read_exact(&mut b1).await.unwrap();
    assert_eq!(&b1, b"ok");

    // 2. Overwrite client_ca_file with 0 certs (comments/garbage only) and wait past 200ms cache.
    std::fs::write(&active_ca, "# empty rotation / 0 certs\n").unwrap();
    tokio::time::sleep(Duration::from_millis(250)).await;

    // Client #1 MUST STILL SUCCEED (last good verifier retained, NOT overwritten with reject-all!).
    let cli1_cfg2 = rustls::ClientConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(Arc::clone(&srv_roots))
        .with_client_cert_resolver(Arc::new(StrictHintFilterResolver {
            ck: Arc::clone(&ck1),
            allow_non_empty_only_if_matches_ca_pem: Some(std::fs::read(&ca1_crt).unwrap()),
            seen_hints: Arc::new(parking_lot::Mutex::new(None)),
        }));
    let tcp_zero = tokio::net::TcpStream::connect(addr).await.unwrap();
    let mut tls_zero = tokio_rustls::TlsConnector::from(Arc::new(cli1_cfg2))
        .connect(ServerName::try_from("localhost").unwrap(), tcp_zero)
        .await
        .unwrap();
    let mut b_zero = [0u8; 2];
    tls_zero.read_exact(&mut b_zero).await.unwrap();
    assert_eq!(&b_zero, b"ok");

    // 3. Rotate client_ca_file away from startup CA #1 to CA #2 and wait past 200ms cache.
    std::fs::copy(&ca2_crt, &active_ca).unwrap();
    tokio::time::sleep(Duration::from_millis(250)).await;

    // Client #2 uses a resolver that strictly filters by AcceptableCAs (root_hint_subjects):
    // if stale CA #1 hints were still returned, it would refuse to send ck2 and handshake would fail!
    let seen_hints = Arc::new(parking_lot::Mutex::new(None));
    let cli2_cfg = rustls::ClientConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(Arc::clone(&srv_roots))
        .with_client_cert_resolver(Arc::new(StrictHintFilterResolver {
            ck: Arc::clone(&ck2),
            allow_non_empty_only_if_matches_ca_pem: Some(std::fs::read(&ca2_crt).unwrap()),
            seen_hints: Arc::clone(&seen_hints),
        }));
    let tcp2 = tokio::net::TcpStream::connect(addr).await.unwrap();
    let mut tls2 = tokio_rustls::TlsConnector::from(Arc::new(cli2_cfg))
        .connect(ServerName::try_from("localhost").unwrap(), tcp2)
        .await
        .unwrap();
    let mut b2 = [0u8; 2];
    tls2.read_exact(&mut b2).await.unwrap();
    assert_eq!(&b2, b"ok");
    assert_eq!(
        seen_hints.lock().as_ref().unwrap().len(),
        0,
        "after CA rotation, root_hint_subjects() must return &[]"
    );

    // Client #1 is now rejected by CA #2.
    let cli1_rejected = rustls::ClientConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(Arc::clone(&srv_roots))
        .with_client_cert_resolver(Arc::new(StrictHintFilterResolver {
            ck: Arc::clone(&ck1),
            allow_non_empty_only_if_matches_ca_pem: None,
            seen_hints: Arc::new(parking_lot::Mutex::new(None)),
        }));
    let tcp_rej = tokio::net::TcpStream::connect(addr).await.unwrap();
    let mut tls_rej = tokio_rustls::TlsConnector::from(Arc::new(cli1_rejected))
        .connect(ServerName::try_from("localhost").unwrap(), tcp_rej)
        .await
        .unwrap();
    let mut b_rej = [0u8; 2];
    assert!(tls_rej.read_exact(&mut b_rej).await.is_err());

    srv_task.abort();
}

#[derive(Debug)]
struct StrictHintFilterResolver {
    ck: Arc<rustls::sign::CertifiedKey>,
    allow_non_empty_only_if_matches_ca_pem: Option<Vec<u8>>,
    seen_hints: Arc<parking_lot::Mutex<Option<Vec<Vec<u8>>>>>,
}

impl rustls::client::ResolvesClientCert for StrictHintFilterResolver {
    fn resolve(
        &self,
        root_hint_subjects: &[&[u8]],
        _sigschemes: &[SignatureScheme],
    ) -> Option<Arc<rustls::sign::CertifiedKey>> {
        *self.seen_hints.lock() = Some(root_hint_subjects.iter().map(|s| s.to_vec()).collect());
        if root_hint_subjects.is_empty() {
            return Some(Arc::clone(&self.ck));
        }
        if let Some(ref pem) = self.allow_non_empty_only_if_matches_ca_pem {
            let mut roots = RootCertStore::empty();
            for c in CertificateDer::pem_slice_iter(pem).flatten() {
                let _ = roots.add(c);
            }
            let allowed: Vec<Vec<u8>> = roots.subjects().into_iter().map(|d| d.as_ref().to_vec()).collect();
            if root_hint_subjects.iter().any(|h| allowed.iter().any(|a| a.as_slice() == *h)) {
                return Some(Arc::clone(&self.ck));
            }
            return None;
        }
        Some(Arc::clone(&self.ck))
    }

    fn has_certs(&self) -> bool {
        true
    }
}

#[tokio::test]
async fn w34_grpc_bounded_decompress_zstd_window_log_and_immediate_cardinality_violation() {
    // 1. zstd frame declaring windowLog=27 (128 MiB window > cap of 23 = 8 MiB) must be rejected.
    // Magic: 0x28 B5 2F FD, FHD: 0x00, Window_Descriptor: (27 - 10) << 3 = 0x88, empty last raw block: 0x01 0x00 0x00
    let zstd_128mib_window_frame = [0x28u8, 0xb5, 0x2f, 0xfd, 0x00, 0x88, 0x01, 0x00, 0x00];
    let mut wire = Vec::with_capacity(5 + zstd_128mib_window_frame.len());
    wire.push(1);
    wire.extend_from_slice(&(zstd_128mib_window_frame.len() as u32).to_be_bytes());
    wire.extend_from_slice(&zstd_128mib_window_frame);

    let err = drive_bounded_grpc_decompress_for_test(
        vec![Bytes::from(wire)],
        "zstd",
        4 * 1024 * 1024,
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Internal);
    assert!(
        err.message().contains("zstd decompression failed"),
        "expected zstd window_log_max rejection, got: {err}"
    );

    // 2. As soon as ANY non-empty bytes arrive with frames_seen >= 1 (even 1 byte, < 5-byte header),
    // BoundedGrpcDecompressBody immediately fails with cardinality violation.
    for algo in ["identity", "zstd"] {
        let mut frame1 = Vec::new();
        frame1.push(0); // uncompressed flag
        frame1.extend_from_slice(&1u32.to_be_bytes());
        frame1.push(b'x');

        let err2 = drive_bounded_grpc_decompress_for_test(
            vec![Bytes::from(frame1), Bytes::from_static(&[0x00])],
            algo,
            1024,
        )
        .await
        .unwrap_err();
        assert_eq!(err2.code(), tonic::Code::Internal);
        assert!(
            err2.message().contains("cardinality violation"),
            "algo={algo} expected immediate cardinality violation, got: {err2}"
        );
    }

    // Keep unused AtomicBool/Ordering clean if referenced
    let _ = AtomicBool::new(false).load(Ordering::Relaxed);
}

#[tokio::test]
async fn w34_usagestats_reporter_builds_and_reports_without_panic() {
    let bucket = BucketClient::new_in_memory("w34-test");
    let rcfg = ReporterConfig {
        server_url: "http://127.0.0.1:1/unreachable".to_string(),
        ..Default::default()
    };
    let reporter = Reporter::new(rcfg, &bucket, None);
    let seed = new_cluster_seed();
    let report = build_report(
        &seed,
        std::time::SystemTime::now(),
        DEFAULT_REPORT_SEND_INTERVAL,
    );
    assert!(reporter.send_report(&report).await.is_err());
}
