//! OBSERV-1374: Kafka TLS + SASL on every connection site of the ingest storage clients.
//!
//! Covers github.com/grafana/mimir/pkg/storage/ingest/util.go (`commonKafkaClientOptions`:
//! `kafkaAuthOptions` + `kgo.DialTLSConfig`) and github.com/grafana/dskit/crypto/tls/tls.go
//! semantics. The in-process fake Kafka cluster (SASL-capable) sits behind a rustls TLS
//! terminating proxy; fixtures live in `tests/fixtures/kafka_tls` (see its README).

#[path = "upstream_storage_part2_helpers.rs"]
mod helpers;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use helpers::{create_test_kafka_config, ClusterSasl, FakeKafkaCluster};
use mimir_common::backoff::BackoffConfig;
use mimir_config::types::SASLMechanism;
use mimir_config::{GoDuration, KafkaConfig};
use mimir_storage::ingest::{
    new_kafka_reader_client, ConcurrentFetchers, FetcherMetrics, GenericOffsetReader, KafkaClient,
    KafkaClientError, KafkaCompression, PartitionOffsetClient, ProduceRecord, RangeErrorPolicy,
};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio::net::{TcpListener, TcpStream};

const TOPIC: &str = "observ-1374";
const USER: &str = "mimir";
const PASS: &str = "supersecret";

fn fixture(name: &str) -> String {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/kafka_tls")
        .join(name)
        .to_string_lossy()
        .into_owned()
}

#[derive(Clone, Copy)]
struct ProxyOpts {
    cert: &'static str,
    require_client_cert: bool,
    tls12_only: bool,
    tls12_suite: Option<rustls::SupportedCipherSuite>,
}

impl Default for ProxyOpts {
    fn default() -> Self {
        Self {
            cert: "server",
            require_client_cert: false,
            tls12_only: false,
            tls12_suite: None,
        }
    }
}

/// TLS-terminating proxy in front of `backend` (plain TCP). Returns the listen port.
async fn start_tls_proxy(backend: String, opts: ProxyOpts) -> u16 {
    let mut provider = rustls::crypto::ring::default_provider();
    if let Some(suite) = opts.tls12_suite {
        provider.cipher_suites = vec![suite];
    }
    let provider = Arc::new(provider);
    let certs: Vec<CertificateDer<'static>> =
        CertificateDer::pem_file_iter(fixture(&format!("{}.crt", opts.cert)))
            .unwrap()
            .map(Result::unwrap)
            .collect();
    let key = PrivateKeyDer::from_pem_file(fixture(&format!("{}.key", opts.cert))).unwrap();
    let versions: &[&rustls::SupportedProtocolVersion] = if opts.tls12_only {
        &[&rustls::version::TLS12]
    } else {
        &[&rustls::version::TLS13, &rustls::version::TLS12]
    };
    let builder = rustls::ServerConfig::builder_with_provider(Arc::clone(&provider))
        .with_protocol_versions(versions)
        .unwrap();
    let builder = if opts.require_client_cert {
        let mut roots = rustls::RootCertStore::empty();
        for c in CertificateDer::pem_file_iter(fixture("ca.crt")).unwrap() {
            roots.add(c.unwrap()).unwrap();
        }
        let verifier =
            rustls::server::WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider)
                .build()
                .unwrap();
        builder.with_client_cert_verifier(verifier)
    } else {
        builder.with_no_client_auth()
    };
    let config = Arc::new(builder.with_single_cert(certs, key).unwrap());
    let acceptor = tokio_rustls::TlsAcceptor::from(config);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        loop {
            let Ok((tcp, _)) = listener.accept().await else { return };
            let acceptor = acceptor.clone();
            let backend = backend.clone();
            tokio::spawn(async move {
                let Ok(mut tls) = acceptor.accept(tcp).await else { return };
                let Ok(mut upstream) = TcpStream::connect(&backend).await else { return };
                let _ = tokio::io::copy_bidirectional(&mut tls, &mut upstream).await;
            });
        }
    });
    port
}

/// Starts a SASL fake cluster behind a TLS proxy; metadata advertises the proxy port.
async fn tls_cluster(sasl: ClusterSasl, opts: ProxyOpts) -> (Arc<FakeKafkaCluster>, u16) {
    let cluster = FakeKafkaCluster::start_with_opts(1, TOPIC, sasl, true).await;
    let port = start_tls_proxy(cluster.addr.clone(), opts).await;
    cluster.set_advertised_port(port as i32);
    (cluster, port)
}

fn scram256() -> ClusterSasl {
    ClusterSasl::ScramSha256 {
        username: USER.into(),
        password: PASS.into(),
    }
}

fn tls_cfg(addr: &str, sasl: Option<SASLMechanism>) -> KafkaConfig {
    let mut cfg = create_test_kafka_config(addr, TOPIC);
    cfg.dial_timeout = GoDuration::from_secs(2);
    cfg.tls_enabled = true;
    cfg.tls_ca_path = fixture("ca.crt");
    if let Some(m) = sasl {
        cfg.sasl_mechanism = m;
        cfg.sasl_username = USER.into();
        cfg.sasl_password = PASS.into();
    }
    cfg
}

/// Pings through `IngestKafkaClient` (site: util.rs `SaslBrokerConn`).
async fn ping(cfg: &KafkaConfig) -> Result<(), KafkaClientError> {
    let client = new_kafka_reader_client(cfg, None)?;
    let res = client.ping().await;
    client.close().await;
    res
}

fn fast_backoff() -> BackoffConfig {
    BackoffConfig {
        min_backoff: Duration::from_millis(10),
        max_backoff: Duration::from_millis(10),
        max_retries: 0,
    }
}

/// Every connection site over TLS + SCRAM-SHA-256 (the OBSERV-1374 production setup).
#[tokio::test]
async fn tls_scram_sha256_all_connection_sites() {
    let (cluster, port) = tls_cluster(scram256(), ProxyOpts::default()).await;
    for host in ["127.0.0.1", "localhost"] {
        let cfg = tls_cfg(&format!("{host}:{port}"), Some(SASLMechanism::ScramSha256));

        // util.rs IngestKafkaClient.
        ping(&cfg).await.unwrap();

        // kafka_client.rs KafkaClient: metadata + produce + list offsets + fetch.
        let client = Arc::new(KafkaClient::from_config(&cfg).unwrap());
        assert!(client.transport().tls_enabled());
        let rec = ProduceRecord {
            key: None,
            value: Some(bytes::Bytes::from_static(b"hello")),
            headers: vec![],
            timestamp_ms: None,
        };
        client
            .produce(TOPIC, 0, KafkaCompression::None, vec![rec])
            .await
            .unwrap();
        let last = client
            .fetch_partition_last_produced_offset(TOPIC, 0)
            .await
            .unwrap();
        assert!(last >= 0);
        let fetched = client
            .fetch(TOPIC, 0, last, Duration::from_millis(10), 1, 1 << 20)
            .await
            .unwrap();
        assert_eq!(fetched.records.last().unwrap().value.as_ref(), b"hello");

        // partition_offset_client.rs BatchConn (batched ListOffsets to the seed brokers).
        let offsets = PartitionOffsetClient::with_config(Arc::clone(&client), &cfg, None);
        let mut req = HashMap::new();
        req.insert(TOPIC.to_string(), vec![0]);
        let got = offsets.fetch_partitions_last_produced_offsets(&req).await.unwrap();
        assert_eq!(got[TOPIC][&0], last);

        // fetcher.rs MuxConn (Metadata for the topic ID).
        let oc = offsets.clone();
        let start_reader = Arc::new(GenericOffsetReader::new(
            Arc::new(move || {
                let oc = oc.clone();
                Box::pin(async move {
                    oc.fetch_partition_start_offset(TOPIC, 0)
                        .await
                        .map_err(|e| e.to_string())
                })
            }),
            Duration::from_secs(1),
        ));
        let fetchers = ConcurrentFetchers::new(
            cfg.address.0.clone(),
            "observ-1374",
            TOPIC,
            0,
            0,
            1,
            0,
            true,
            Duration::from_millis(10),
            &offsets,
            RangeErrorPolicy::Abort,
            start_reader,
            fast_backoff(),
            FetcherMetrics::new(None),
        )
        .await;
        assert!(fetchers.is_ok(), "ConcurrentFetchers over TLS+SASL: {:?}", fetchers.err());
    }
    cluster.close();
}

/// SASL without TLS must also work on the sites that previously had no SASL at all.
#[tokio::test]
async fn sasl_without_tls_on_offset_and_fetcher_sites() {
    let cluster = FakeKafkaCluster::start_with_opts(
        1,
        TOPIC,
        ClusterSasl::ScramSha512 {
            username: USER.into(),
            password: PASS.into(),
        },
        true,
    )
    .await;
    let mut cfg = create_test_kafka_config(&cluster.addr, TOPIC);
    cfg.sasl_mechanism = SASLMechanism::ScramSha512;
    cfg.sasl_username = USER.into();
    cfg.sasl_password = PASS.into();
    let client = Arc::new(KafkaClient::from_config(&cfg).unwrap());
    let offsets = PartitionOffsetClient::with_config(client, &cfg, None);
    let mut req = HashMap::new();
    req.insert(TOPIC.to_string(), vec![0]);
    assert_eq!(
        offsets.fetch_partitions_last_produced_offsets(&req).await.unwrap()[TOPIC][&0],
        -1
    );

    cfg.sasl_password = "wrong".into();
    let client = Arc::new(KafkaClient::from_config(&cfg).unwrap());
    let offsets = PartitionOffsetClient::with_config(client, &cfg, None);
    let err = offsets
        .fetch_partitions_last_produced_offsets(&req)
        .await
        .unwrap_err();
    assert!(matches!(err, KafkaClientError::Sasl(_)), "{err}");
    cluster.close();
}

#[tokio::test]
async fn tls_sasl_mechanisms_and_wrong_credentials() {
    for (cluster_sasl, mech) in [
        (
            ClusterSasl::Plain {
                username: USER.into(),
                password: PASS.into(),
            },
            SASLMechanism::Plain,
        ),
        (scram256(), SASLMechanism::ScramSha256),
        (
            ClusterSasl::ScramSha512 {
                username: USER.into(),
                password: PASS.into(),
            },
            SASLMechanism::ScramSha512,
        ),
    ] {
        let (cluster, port) = tls_cluster(cluster_sasl, ProxyOpts::default()).await;
        let addr = format!("localhost:{port}");
        ping(&tls_cfg(&addr, Some(mech))).await.unwrap();

        let mut bad = tls_cfg(&addr, Some(mech));
        bad.sasl_password = "wrong".into();
        let err = ping(&bad).await.unwrap_err();
        assert!(matches!(err, KafkaClientError::Sasl(_)), "{err}");

        // SASL listener without credentials: no authentication, the broker rejects requests.
        assert!(ping(&tls_cfg(&addr, None)).await.is_err());
        cluster.close();
    }
}

#[tokio::test]
async fn tls_server_verification() {
    // Untrusted CA.
    let (cluster, port) = tls_cluster(ClusterSasl::None, ProxyOpts::default()).await;
    let addr = format!("127.0.0.1:{port}");
    let mut cfg = tls_cfg(&addr, None);
    cfg.tls_ca_path = fixture("other-ca.crt");
    let err = ping(&cfg).await.unwrap_err();
    assert!(err.to_string().contains("TLS handshake"), "{err}");

    // No CA configured: host root CAs, which do not contain the test CA.
    cfg.tls_ca_path = String::new();
    assert!(ping(&cfg).await.is_err());

    // tls_insecure_skip_verify accepts it.
    cfg.tls_insecure_skip_verify = true;
    ping(&cfg).await.unwrap();
    cluster.close();

    // Certificate valid only for kafka.mimir-rs.test: name mismatch unless overridden.
    let (cluster, port) = tls_cluster(
        ClusterSasl::None,
        ProxyOpts {
            cert: "sni",
            ..Default::default()
        },
    )
    .await;
    let mut cfg = tls_cfg(&format!("127.0.0.1:{port}"), None);
    let err = ping(&cfg).await.unwrap_err();
    assert!(err.to_string().contains("TLS handshake"), "{err}");
    cfg.tls_server_name = "kafka.mimir-rs.test".into();
    ping(&cfg).await.unwrap();
    cluster.close();
}

#[tokio::test]
async fn mtls_client_certificates() {
    let (cluster, port) = tls_cluster(
        scram256(),
        ProxyOpts {
            require_client_cert: true,
            ..Default::default()
        },
    )
    .await;
    let addr = format!("localhost:{port}");

    // Missing client certificate.
    assert!(ping(&tls_cfg(&addr, Some(SASLMechanism::ScramSha256))).await.is_err());

    for (cert, key) in [
        ("client.crt", "client.key"),
        ("client.crt", "client.pkcs1.key"),
        ("client-ec.crt", "client-ec.key"),
        ("client-ec.crt", "client-ec.sec1.key"),
    ] {
        let mut cfg = tls_cfg(&addr, Some(SASLMechanism::ScramSha256));
        cfg.tls_cert_path = fixture(cert);
        cfg.tls_key_path = fixture(key);
        ping(&cfg).await.unwrap_or_else(|e| panic!("{cert} + {key}: {e}"));
    }
    cluster.close();
}

#[tokio::test]
async fn tls_versions_and_cipher_suites() {
    use rustls::crypto::ring::cipher_suite as cs;
    let (cluster, port) = tls_cluster(
        ClusterSasl::None,
        ProxyOpts {
            tls12_only: true,
            tls12_suite: Some(cs::TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256),
            ..Default::default()
        },
    )
    .await;
    let addr = format!("localhost:{port}");
    ping(&tls_cfg(&addr, None)).await.unwrap();

    let mut cfg = tls_cfg(&addr, None);
    cfg.tls_min_version = "VersionTLS13".into();
    assert!(ping(&cfg).await.is_err());

    let mut cfg = tls_cfg(&addr, None);
    cfg.tls_cipher_suites = "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384".into();
    assert!(ping(&cfg).await.is_err());
    cfg.tls_cipher_suites = "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384, TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256".into();
    ping(&cfg).await.unwrap();
    cluster.close();
}

#[tokio::test]
async fn tls_plaintext_mismatch() {
    // TLS client to a plaintext listener.
    let cluster = FakeKafkaCluster::start(1, TOPIC).await;
    assert!(ping(&tls_cfg(&cluster.addr, None)).await.is_err());
    cluster.close();

    // Plaintext client to a TLS listener.
    let (cluster, port) = tls_cluster(ClusterSasl::None, ProxyOpts::default()).await;
    let cfg = create_test_kafka_config(&format!("127.0.0.1:{port}"), TOPIC);
    assert!(ping(&cfg).await.is_err());
    cluster.close();
}

#[test]
fn invalid_tls_config_is_a_construction_error() {
    let mut cfg = tls_cfg("localhost:9092", None);
    cfg.tls_cert_path = fixture("client.crt");
    let err = KafkaClient::from_config(&cfg).err().unwrap();
    assert_eq!(
        err.to_string(),
        "invalid Kafka TLS config: certificate given but no key configured"
    );
    let mut cfg = tls_cfg("localhost:9092", None);
    cfg.tls_min_version = "VersionTLS10".into();
    assert!(KafkaClient::from_config(&cfg).is_err());
}
