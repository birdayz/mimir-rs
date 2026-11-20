//! Wave 19 regression tests for lazy filesystem bucket dirs, S3 TLS SNI + idle pool disable,
//! and KafkaTransport::dial insecure_skip_verify non-DNS server_name tolerance.
//!
//! Covers:
//! - W19-MOD-01: BucketClient::new_filesystem does not eagerly create directories on disk
//!   until write/upload operations; read probes return NotFound / Ok(false) / Ok(vec![]).
//! - W19-RKN-07: S3 http.tls_server_name sends TLS ClientHello SNI and preserves Host header;
//!   negative max_idle_connections_per_host disables idle connection pooling.
//! - W19-RKN-09 (KafkaTransport side): KafkaTransport::dial succeeds with non-RFC-DNS
//!   tls_server_name / host when tls_insecure_skip_verify: true.

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use mimir_common::tls::ClientConfig as MimirTlsClientConfig;
use mimir_config::{BucketConfig, BucketHTTPConfig, S3Config};
use mimir_storage::bucket::s3::S3Bucket;
use mimir_storage::bucket::{BucketClient, BucketError};
use mimir_storage::ingest::KafkaTransport;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_rustls::LazyConfigAcceptor;

fn fixture(name: &str) -> String {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/kafka_tls")
        .join(name)
        .to_string_lossy()
        .into_owned()
}

#[tokio::test]
async fn w19_mod_01_filesystem_bucket_creates_dir_lazily_on_write_only() {
    let dir = TempDir::new().unwrap();
    let root = dir.path().join("nested").join("does_not_exist_yet");
    assert!(!root.exists());

    // Construction and read probes must NOT create root on disk.
    let bkt = BucketClient::new_filesystem(&root).unwrap();
    assert!(!root.exists(), "new_filesystem must not create directory eagerly");

    assert!(!bkt.exists("sanity-check-at-startup").await.unwrap());
    assert!(matches!(
        bkt.download("sanity-check-at-startup").await,
        Err(BucketError::NotFound(_))
    ));
    assert!(matches!(
        bkt.get_range("sanity-check-at-startup", 0, 10).await,
        Err(BucketError::NotFound(_))
    ));
    assert!(matches!(
        bkt.attributes("sanity-check-at-startup").await,
        Err(BucketError::NotFound(_))
    ));
    assert_eq!(bkt.iter_dir("").await.unwrap(), Vec::<String>::new());
    assert_eq!(bkt.list_all("").await.unwrap(), Vec::<String>::new());
    bkt.delete("sanity-check-at-startup").await.unwrap();
    assert!(matches!(
        bkt.delete_strict("sanity-check-at-startup").await,
        Err(BucketError::NotFound(_))
    ));
    assert!(!root.exists(), "read probes must not create directory on disk");

    // Non-existent top-level path (matching Thanos sanity_check_test.go) must also succeed on
    // construction + read probe without creating directories.
    let mut cfg = BucketConfig::default();
    cfg.backend_cfg.backend = "filesystem".to_string();
    cfg.backend_cfg.filesystem.dir = "/does/not/exists/mimir-w19-sanity".to_string();
    let sanity_bkt = BucketClient::new_client(&cfg, "sanity-check", None).unwrap();
    let err = sanity_bkt
        .download("sanity-check-at-startup")
        .await
        .unwrap_err();
    assert!(err.is_not_found(), "expected NotFound, got: {err:?}");

    // First write creates root lazily and persists the object.
    bkt.upload("tenant-a/obj.txt", Bytes::from_static(b"hello lazy fs"))
        .await
        .unwrap();
    assert!(root.exists(), "upload must create fs_root lazily");
    assert!(bkt.exists("tenant-a/obj.txt").await.unwrap());
    assert_eq!(
        bkt.download("tenant-a/obj.txt").await.unwrap().as_ref(),
        b"hello lazy fs"
    );
    assert_eq!(bkt.iter_dir("").await.unwrap(), vec!["tenant-a/"]);
}

fn load_server_config(cert_name: &str) -> Arc<rustls::ServerConfig> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let certs: Vec<CertificateDer<'static>> =
        CertificateDer::pem_file_iter(fixture(&format!("{cert_name}.crt")))
            .unwrap()
            .map(Result::unwrap)
            .collect();
    let key = PrivateKeyDer::from_pem_file(fixture(&format!("{cert_name}.key"))).unwrap();
    let cfg = rustls::ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .unwrap();
    Arc::new(cfg)
}

async fn start_sni_s3_server(
    expected_sni: &'static str,
) -> (
    String,
    Arc<AtomicUsize>,
    Arc<Mutex<Vec<Option<String>>>>,
    Arc<Mutex<Vec<String>>>,
    tokio::task::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let endpoint = format!("127.0.0.1:{}", addr.port());

    let tcp_accepts = Arc::new(AtomicUsize::new(0));
    let seen_snis = Arc::new(Mutex::new(Vec::new()));
    let seen_hosts = Arc::new(Mutex::new(Vec::new()));

    let tcp_accepts_srv = Arc::clone(&tcp_accepts);
    let seen_snis_srv = Arc::clone(&seen_snis);
    let seen_hosts_srv = Arc::clone(&seen_hosts);

    let sni_cfg = load_server_config("sni");
    let fallback_cfg = load_server_config("server");

    let handle = tokio::spawn(async move {
        loop {
            let Ok((sock, _)) = listener.accept().await else {
                break;
            };
            tcp_accepts_srv.fetch_add(1, Ordering::SeqCst);
            let snis = Arc::clone(&seen_snis_srv);
            let hosts = Arc::clone(&seen_hosts_srv);
            let sni_cfg = Arc::clone(&sni_cfg);
            let fallback_cfg = Arc::clone(&fallback_cfg);

            tokio::spawn(async move {
                let acceptor = LazyConfigAcceptor::new(rustls::server::Acceptor::default(), sock);
                let Ok(start) = acceptor.await else {
                    return;
                };
                let sni = start.client_hello().server_name().map(str::to_string);
                snis.lock().unwrap().push(sni.clone());
                let cfg = if sni.as_deref() == Some(expected_sni) {
                    sni_cfg
                } else {
                    fallback_cfg
                };
                let Ok(mut tls_stream) = start.into_stream(cfg).await else {
                    return;
                };

                let mut buf = Vec::new();
                loop {
                    buf.clear();
                    let mut tmp = [0u8; 4096];
                    let head_end = loop {
                        match tls_stream.read(&mut tmp).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => buf.extend_from_slice(&tmp[..n]),
                        }
                        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                            break pos;
                        }
                    };
                    let head = String::from_utf8_lossy(&buf[..head_end]).into_owned();
                    let req_line = head.lines().next().unwrap_or_default().to_string();
                    for l in head.lines().skip(1) {
                        if let Some((k, v)) = l.split_once(':') {
                            if k.trim().eq_ignore_ascii_case("host") {
                                hosts.lock().unwrap().push(v.trim().to_string());
                            }
                        }
                    }
                    let resp = if req_line.starts_with("HEAD ") {
                        "HTTP/1.1 200 OK\r\nContent-Length: 5\r\nLast-Modified: Wed, 21 Oct 2015 07:28:00 GMT\r\nETag: \"abc\"\r\n\r\n".to_string()
                    } else {
                        "HTTP/1.1 200 OK\r\nContent-Length: 5\r\nLast-Modified: Wed, 21 Oct 2015 07:28:00 GMT\r\nETag: \"abc\"\r\n\r\nhello".to_string()
                    };
                    if tls_stream.write_all(resp.as_bytes()).await.is_err() {
                        return;
                    }
                }
            });
        }
    });

    (endpoint, tcp_accepts, seen_snis, seen_hosts, handle)
}

#[tokio::test]
async fn w19_rkn_07_s3_sends_tls_sni_preserves_host_and_honors_negative_idle_pool() {
    let (endpoint, tcp_accepts, seen_snis, seen_hosts, srv) =
        start_sni_s3_server("kafka.mimir-rs.test").await;

    let mut s3_cfg = S3Config {
        endpoint: endpoint.clone(),
        bucket_name: "test-bkt".to_string(),
        region: "us-east-1".to_string(),
        access_key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
        secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
        insecure: false,
        http: BucketHTTPConfig {
            tls_ca_path: fixture("ca.crt"),
            tls_server_name: "kafka.mimir-rs.test".to_string(),
            max_idle_connections_per_host: -1,
            ..Default::default()
        },
        ..Default::default()
    };

    let bkt = S3Bucket::new(&s3_cfg, "ingester").unwrap();
    assert!(bkt.exists("key1").await.unwrap());
    assert_eq!(bkt.get("key1").await.unwrap().as_ref(), b"hello");

    assert_eq!(
        tcp_accepts.load(Ordering::SeqCst),
        2,
        "negative max_idle_connections_per_host must disable idle connection pooling"
    );
    let snis = seen_snis.lock().unwrap().clone();
    assert_eq!(
        snis,
        vec![
            Some("kafka.mimir-rs.test".to_string()),
            Some("kafka.mimir-rs.test".to_string()),
        ]
    );
    let hosts = seen_hosts.lock().unwrap().clone();
    assert_eq!(hosts, vec![endpoint.clone(), endpoint.clone()]);

    // Contrast with positive max_idle_connections_per_host: reuses 1 TCP connection.
    s3_cfg.http.max_idle_connections_per_host = 2;
    let bkt_pooled = S3Bucket::new(&s3_cfg, "ingester").unwrap();
    let before = tcp_accepts.load(Ordering::SeqCst);
    assert!(bkt_pooled.exists("key1").await.unwrap());
    assert_eq!(bkt_pooled.get("key1").await.unwrap().as_ref(), b"hello");
    assert_eq!(
        tcp_accepts.load(Ordering::SeqCst) - before,
        1,
        "positive max_idle_connections_per_host should reuse the pooled connection"
    );

    srv.abort();
}

#[tokio::test]
async fn w19_rkn_09_kafka_transport_dial_allows_non_dns_server_name_when_insecure_skip_verify() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let server_cfg = load_server_config("server");
    let srv = tokio::spawn(async move {
        let acceptor = tokio_rustls::TlsAcceptor::from(server_cfg);
        while let Ok((sock, _)) = listener.accept().await {
            let acc = acceptor.clone();
            tokio::spawn(async move {
                let _ = acc.accept(sock).await;
            });
        }
    });

    // Non-RFC-DNS tls_server_name must fail when insecure_skip_verify is false.
    let strict_cfg = MimirTlsClientConfig {
        server_name: "bad_host:9092".to_string(),
        insecure_skip_verify: false,
        ..Default::default()
    };
    assert!(strict_cfg.get_tls_config().is_err());

    // With insecure_skip_verify: true, non-RFC-DNS tls_server_name succeeds and dials cleanly.
    let insecure_cfg = MimirTlsClientConfig {
        server_name: "bad_host:9092".to_string(),
        insecure_skip_verify: true,
        ..Default::default()
    };
    let resolved = insecure_cfg.get_tls_config().unwrap();
    let transport = KafkaTransport::plaintext(Duration::from_secs(5)).with_tls(resolved);
    let stream = transport.dial(&addr).await.unwrap();
    drop(stream);

    srv.abort();
}
