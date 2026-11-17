//! Tests of the memberlist TCP transport.
//!
//! Ported from github.com/grafana/dskit/kv/memberlist/tcp_transport_test.go, plus TLS tests
//! (dskit `TCPTransportConfig.TLSEnabled` / inline `crypto/tls.ClientConfig`) and a metric
//! name check against dskit `registerMetrics`.

use std::fmt::Write as _;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use mimir_config::duration::GoDuration;
use mimir_config::server::MemberlistConfig;
use mimir_ring::kv::memberlist::tcp_transport::TcpTransport;
use prometheus::Registry;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

// ---------------------------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------------------------

/// dskit `getLocalhostAddrs()` (IPv4 address of `localhost`).
fn localhost_addrs() -> Vec<String> {
    vec!["127.0.0.1".to_string()]
}

/// `TCPTransportConfig` after `flagext.DefaultValues` (dskit flag defaults, not Mimir's).
fn dskit_default_cfg() -> MemberlistConfig {
    MemberlistConfig {
        bind_addr: Vec::new(),
        bind_port: 7946,
        packet_dial_timeout: GoDuration::from_secs(2),
        packet_write_timeout: GoDuration::from_secs(5),
        max_concurrent_writes: 3,
        acquire_writer_timeout: GoDuration::from_millis(250),
        ..MemberlistConfig::default()
    }
}

fn fixture(name: &str) -> String {
    format!(
        "{}/../mimir-storage/tests/fixtures/kafka_tls/{name}",
        env!("CARGO_MANIFEST_DIR")
    )
}

/// TLS settings of a memberlist node: `server.crt` (SANs `localhost`, `127.0.0.1`) signed by
/// `ca.crt`, used both as the served certificate and as the client certificate (like dskit).
fn with_tls(mut cfg: MemberlistConfig) -> MemberlistConfig {
    cfg.tls_enabled = true;
    cfg.tls_cert_path = fixture("server.crt");
    cfg.tls_key_path = fixture("server.key");
    cfg.tls_ca_path = fixture("ca.crt");
    cfg
}

fn local_cfg() -> MemberlistConfig {
    let mut cfg = dskit_default_cfg();
    cfg.bind_addr = localhost_addrs();
    cfg.bind_port = 0;
    cfg
}

fn counter(reg: &Registry, name: &str) -> f64 {
    reg.gather()
        .iter()
        .find(|m| m.get_name() == name)
        .map(|m| m.get_metric()[0].get_counter().get_value())
        .unwrap_or(0.0)
}

async fn wait_for_counter(reg: &Registry, name: &str, at_least: f64) -> f64 {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let v = counter(reg, name);
        if v >= at_least || Instant::now() > deadline {
            return v;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Captures `tracing` events of the current thread (go-kit `log.NewLogfmtLogger` into a
/// `concurrency.SyncBuffer` in the Go tests). Tests using it run on a current-thread runtime.
#[derive(Clone, Default)]
struct Logs(Arc<Mutex<Vec<String>>>);

impl Logs {
    fn text(&self) -> String {
        self.0.lock().unwrap().join("\n")
    }
}

struct FieldVisitor(String);

impl tracing::field::Visit for FieldVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        let _ = write!(self.0, " {}={:?}", field.name(), value);
    }
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        let _ = write!(self.0, " {}={}", field.name(), value);
    }
}

struct CaptureSubscriber(Logs);

impl tracing::Subscriber for CaptureSubscriber {
    fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        let mut v = FieldVisitor(format!("level={}", event.metadata().level()));
        event.record(&mut v);
        self.0 .0.lock().unwrap().push(v.0);
    }
    fn enter(&self, _span: &tracing::span::Id) {}
    fn exit(&self, _span: &tracing::span::Id) {}
}

fn capture_logs() -> (Logs, tracing::subscriber::DefaultGuard) {
    let logs = Logs::default();
    let guard = tracing::subscriber::set_default(CaptureSubscriber(logs.clone()));
    (logs, guard)
}

// ---------------------------------------------------------------------------------------------
// Ported upstream tests
// ---------------------------------------------------------------------------------------------

// Upstream: github.com/grafana/dskit/kv/memberlist/tcp_transport_test.go::TestTCPTransport_WriteTo_ShouldNotLogAsWarningExpectedFailures
#[tokio::test]
async fn test_tcp_transport_write_to_should_not_log_as_warning_expected_failures() {
    struct Case {
        name: &'static str,
        transport_debug: bool,
        remote_addr: &'static str,
        expected_logs: &'static str,
        unexpected_logs: &'static str,
    }
    let cases = [
        Case {
            name: "should not log 'connection refused' by default",
            transport_debug: false,
            remote_addr: "127.0.0.1:12345",
            expected_logs: "",
            unexpected_logs: "connection refused",
        },
        Case {
            name: "should log 'connection refused' if debug log level is enabled",
            transport_debug: true,
            remote_addr: "127.0.0.1:12345",
            expected_logs: "connection refused",
            unexpected_logs: "",
        },
    ];

    for case in cases {
        let (logs, _guard) = capture_logs();

        let cfg = local_cfg();
        let (transport, _packets, _streams) = TcpTransport::new(&cfg, "", None).await.unwrap();
        transport.set_transport_debug(case.transport_debug);

        transport
            .write_to_with_result(b"test".to_vec(), case.remote_addr.to_string())
            .await
            .unwrap_or_else(|e| panic!("{}: {e}", case.name));

        transport.shutdown_and_wait().await;

        let text = logs.text();
        if !case.expected_logs.is_empty() {
            assert!(text.contains(case.expected_logs), "{}: logs: {text}", case.name);
        }
        if !case.unexpected_logs.is_empty() {
            assert!(!text.contains(case.unexpected_logs), "{}: logs: {text}", case.name);
        }
    }
}

// Upstream: github.com/grafana/dskit/kv/memberlist/tcp_transport_test.go::TestTCPTransportWriteToUnreachableAddr
//
// Go enables TLS on an existing transport (no TLS config -> default roots) and writes to a plain
// listener that never answers, so every TLS handshake runs into the 500ms dial timeout. Here the
// transport is created with TLS enabled (the Rust config is immutable after construction); the
// behaviour under test is the same: 50 concurrent writers all time out with
// "context deadline exceeded" in ~500ms without blocking each other.
#[tokio::test]
async fn test_tcp_transport_write_to_unreachable_addr() {
    let write_ct = 50;

    // Listen for TCP connections on a random port (never accepted / answered).
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listener_addr = listener.local_addr().unwrap().to_string();

    let (logs, _guard) = capture_logs();

    let mut cfg = with_tls(dskit_default_cfg());
    cfg.bind_addr = localhost_addrs();
    cfg.bind_port = 0;
    cfg.max_concurrent_writes = write_ct;
    cfg.packet_dial_timeout = GoDuration::from_millis(500);
    let (transport, _packets, _streams) = TcpTransport::new(&cfg, "", None).await.unwrap();

    let time_start = Instant::now();

    for _ in 0..write_ct {
        transport
            .write_to_with_result(b"test".to_vec(), listener_addr.clone())
            .await
            .unwrap();
    }

    transport.shutdown_and_wait().await;

    let got_error_ct = logs.text().matches("context deadline exceeded").count();
    assert_eq!(got_error_ct, write_ct as usize, "expected {write_ct} errors, got {got_error_ct}");
    let elapsed = time_start.elapsed();
    assert!(
        elapsed >= Duration::from_millis(500),
        "expected to take at least 500ms (timeout duration), took {elapsed:?}"
    );
    assert!(
        elapsed <= Duration::from_secs(2),
        "expected to take less than 2s (timeout + a good margin), writing to unreachable addresses should not block, took {elapsed:?}"
    );
    drop(listener);
}

// Upstream: github.com/grafana/dskit/kv/memberlist/tcp_transport_test.go::TestTCPTransportWriterAcquireTimeout
#[tokio::test]
async fn test_tcp_transport_writer_acquire_timeout() {
    // Listen for TCP connections on a random port.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listener_addr = listener.local_addr().unwrap().to_string();

    let (logs, _guard) = capture_logs();

    let mut cfg = local_cfg();
    cfg.max_concurrent_writes = 1;
    cfg.acquire_writer_timeout = GoDuration::from_millis(1); // very short timeout
    let (transport, _packets, _streams) = TcpTransport::new(&cfg, "", None).await.unwrap();

    let write_ct = 100;
    let mut handles = Vec::with_capacity(write_ct);
    for _ in 0..write_ct {
        let tr = transport.clone();
        let addr = listener_addr.clone();
        handles.push(tokio::spawn(async move {
            let _ = tr.write_to_with_result(b"test".to_vec(), addr).await;
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    transport.shutdown_and_wait().await;
    let got_error_ct = logs
        .text()
        .matches("WriteTo failed to acquire a writer. Dropping message")
        .count();
    assert!(
        got_error_ct < write_ct,
        "expected to have less errors ({got_error_ct}) than total writes ({write_ct}). Some writes should pass."
    );
    assert_ne!(got_error_ct, 0, "expected errors, got none");
    drop(listener);
}

// Upstream: github.com/grafana/dskit/kv/memberlist/tcp_transport_test.go::TestNonIPsAreRejected
#[tokio::test]
async fn test_non_ips_are_rejected() {
    let cfg = MemberlistConfig {
        bind_addr: vec!["localhost".to_string()],
        bind_port: 0,
        ..MemberlistConfig::default()
    };
    let err = TcpTransport::new(&cfg, "", None).await.err().expect("must fail");
    assert_eq!(err.to_string(), r#"could not parse bind addr "localhost" as IP address"#);
}

async fn sent_and_received_setup() -> (
    Registry,
    Arc<TcpTransport>,
    Arc<TcpTransport>,
    tokio::sync::mpsc::Receiver<mimir_ring::kv::memberlist::tcp_transport::ReceivedPacket>,
    tokio::sync::mpsc::Receiver<mimir_ring::kv::memberlist::tcp_transport::MemberlistConn>,
    String,
    Vec<u8>,
) {
    let reg = Registry::new();

    // Set up the receiver transport.
    let receiver_cfg = local_cfg();
    let (receiver, packets, streams) =
        TcpTransport::new(&receiver_cfg, "receiver", Some(&reg)).await.unwrap();

    // Get the advertised address.
    let (receiver_ip, receiver_port) = receiver
        .final_advertise_addr("", i64::from(receiver.auto_bind_port()))
        .unwrap();
    let receiver_addr = std::net::SocketAddr::new(receiver_ip, receiver_port).to_string();

    // Set up the sender transport.
    let sender_cfg = local_cfg();
    let (sender, _p, _s) = TcpTransport::new(&sender_cfg, "sender", Some(&reg)).await.unwrap();

    // Test data larger than the flush threshold to test periodic flushing.
    let test_data: Vec<u8> = (0..128 * 1024).map(|i| (i % 256) as u8).collect();

    (reg, sender, receiver, packets, streams, receiver_addr, test_data)
}

// Upstream: github.com/grafana/dskit/kv/memberlist/tcp_transport_test.go::TestTCPTransport_SentAndReceivedBytesMetrics
#[tokio::test]
async fn test_tcp_transport_sent_and_received_bytes_metrics() {
    // "WriteTo() packet mode"
    {
        let (reg, sender, receiver, mut packets, _streams, receiver_addr, test_data) =
            sent_and_received_setup().await;

        sender
            .write_to_with_result(test_data.clone(), receiver_addr)
            .await
            .unwrap();

        let pkt = tokio::time::timeout(Duration::from_secs(5), packets.recv())
            .await
            .expect("timeout waiting for packet")
            .unwrap();
        assert_eq!(pkt.buf, test_data);

        sender.shutdown_and_wait().await;
        receiver.shutdown_and_wait().await;

        assert!(
            counter(&reg, "sender_memberlist_tcp_transport_sent_bytes_total") >= test_data.len() as f64,
            "sender should have sent at least the test data size"
        );
        assert!(
            counter(&reg, "receiver_memberlist_tcp_transport_received_bytes_total")
                >= test_data.len() as f64,
            "receiver should have received at least the test data size"
        );
    }

    // "DialTimeout() stream mode"
    {
        let (reg, sender, receiver, _packets, mut streams, receiver_addr, test_data) =
            sent_and_received_setup().await;

        let mut conn = sender
            .dial_timeout(&receiver_addr, Duration::from_secs(5))
            .await
            .unwrap();

        let mut receiver_conn = tokio::time::timeout(Duration::from_secs(5), streams.recv())
            .await
            .expect("timeout waiting for stream connection")
            .unwrap();

        let writer = async {
            conn.write_all(&test_data).await.unwrap();
            conn.flush().await.unwrap();
        };
        let mut received = vec![0u8; test_data.len()];
        let reader = receiver_conn.read_exact(&mut received);
        let ((), read_res) = tokio::join!(writer, reader);
        read_res.unwrap();
        assert_eq!(received, test_data);

        drop(conn);
        drop(receiver_conn);

        sender.shutdown_and_wait().await;
        receiver.shutdown_and_wait().await;

        assert!(
            counter(&reg, "sender_memberlist_tcp_transport_sent_bytes_total") >= test_data.len() as f64,
            "sender should have sent at least the test data size"
        );
        assert!(
            counter(&reg, "receiver_memberlist_tcp_transport_received_bytes_total")
                >= test_data.len() as f64,
            "receiver should have received at least the test data size"
        );
    }
}

// ---------------------------------------------------------------------------------------------
// Metric names (dskit tcp_transport.go registerMetrics, MetricsNamespace "" as in Mimir)
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn test_tcp_transport_metric_names_match_dskit() {
    let reg = Registry::new();
    let (transport, _p, _s) = TcpTransport::new(&local_cfg(), "", Some(&reg)).await.unwrap();
    let mut got: Vec<(String, String, prometheus::proto::MetricType)> = reg
        .gather()
        .iter()
        .map(|m| (m.get_name().to_string(), m.get_help().to_string(), m.get_field_type()))
        .collect();
    got.sort_by(|a, b| a.0.cmp(&b.0));

    let bytes_help = "This metric only tracks broadcast packets, and does not include full state syncs or pings.";
    let all_help = "Total bytes sent by the transport. This metric tracks all data transferred, including broadcast packets, full state syncs, and pings.";
    let mut want: Vec<(String, String, prometheus::proto::MetricType)> = [
        ("memberlist_tcp_transport_incoming_streams_total", "Number of incoming memberlist streams".to_string()),
        ("memberlist_tcp_transport_outgoing_streams_total", "Number of outgoing streams".to_string()),
        ("memberlist_tcp_transport_outgoing_stream_errors_total", "Number of errors when opening memberlist stream to another node".to_string()),
        ("memberlist_tcp_transport_packets_received_total", "Number of received memberlist packets".to_string()),
        ("memberlist_tcp_transport_packets_received_bytes_total", format!("Total bytes received as packets. {bytes_help}")),
        ("memberlist_tcp_transport_packets_received_errors_total", "Number of errors when receiving memberlist packets".to_string()),
        ("memberlist_tcp_transport_packets_dropped_total", "Number of dropped memberlist packets. These packets were not sent due to timeout waiting for a writer.".to_string()),
        ("memberlist_tcp_transport_packets_sent_total", "Number of memberlist packets sent".to_string()),
        ("memberlist_tcp_transport_packets_sent_bytes_total", format!("Total bytes sent as packets. {bytes_help}")),
        ("memberlist_tcp_transport_packets_sent_errors_total", "Number of errors when sending memberlist packets".to_string()),
        ("memberlist_tcp_transport_unknown_connections_total", "Number of unknown TCP connections (not a packet or stream)".to_string()),
        ("memberlist_tcp_transport_sent_bytes_total", all_help.to_string()),
        ("memberlist_tcp_transport_received_bytes_total", all_help.to_string()),
    ]
    .into_iter()
    .map(|(n, h)| (n.to_string(), h, prometheus::proto::MetricType::COUNTER))
    .collect();
    want.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(got, want);
    transport.shutdown_and_wait().await;
}

// ---------------------------------------------------------------------------------------------
// TLS (dskit TCPTransportConfig.TLSEnabled + crypto/tls.ClientConfig)
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn test_tcp_transport_tls_round_trip() {
    let reg = Registry::new();
    let (receiver, mut packets, mut streams) =
        TcpTransport::new(&with_tls(local_cfg()), "receiver", Some(&reg)).await.unwrap();
    let (rip, rport) = receiver.final_advertise_addr("", 0).unwrap();
    let receiver_addr = std::net::SocketAddr::new(rip, rport).to_string();

    let (sender, _p, _s) =
        TcpTransport::new(&with_tls(local_cfg()), "sender", Some(&reg)).await.unwrap();
    let (sip, sport) = sender.final_advertise_addr("", 0).unwrap();
    assert!(sender.tls_enabled() && receiver.tls_enabled());

    // Packet.
    let payload: Vec<u8> = (0..100_000).map(|i| (i % 251) as u8).collect();
    sender
        .write_to_with_result(payload.clone(), receiver_addr.clone())
        .await
        .unwrap();
    let pkt = tokio::time::timeout(Duration::from_secs(5), packets.recv())
        .await
        .expect("timeout waiting for packet")
        .unwrap();
    assert_eq!(pkt.buf, payload);
    assert_eq!(pkt.from, std::net::SocketAddr::new(sip, sport).to_string());

    // Stream, both directions.
    let mut conn = sender
        .dial_timeout(&receiver_addr, Duration::from_secs(5))
        .await
        .unwrap();
    assert!(conn.is_tls());
    let mut rconn = tokio::time::timeout(Duration::from_secs(5), streams.recv())
        .await
        .expect("timeout waiting for stream")
        .unwrap();
    assert!(rconn.is_tls());

    conn.write_all(b"ping").await.unwrap();
    conn.flush().await.unwrap();
    let mut buf = [0u8; 4];
    rconn.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"ping");
    {
        let (_r, mut w) = rconn.split();
        w.write_all(b"pong").await.unwrap();
        w.flush().await.unwrap();
    }
    let (mut r, _w) = conn.split();
    r.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"pong");
    drop(conn);
    drop(rconn);

    sender.shutdown_and_wait().await;
    receiver.shutdown_and_wait().await;

    assert_eq!(counter(&reg, "sender_memberlist_tcp_transport_packets_sent_total"), 1.0);
    assert_eq!(counter(&reg, "sender_memberlist_tcp_transport_packets_sent_errors_total"), 0.0);
    assert_eq!(counter(&reg, "sender_memberlist_tcp_transport_outgoing_streams_total"), 1.0);
    assert_eq!(counter(&reg, "receiver_memberlist_tcp_transport_packets_received_total"), 1.0);
    assert_eq!(counter(&reg, "receiver_memberlist_tcp_transport_incoming_streams_total"), 1.0);
    // Application (plaintext) bytes, like dskit's meteredConn wrapping the tls.Conn.
    assert!(counter(&reg, "sender_memberlist_tcp_transport_sent_bytes_total") >= (payload.len() + 4 + 1) as f64);
    assert!(counter(&reg, "receiver_memberlist_tcp_transport_received_bytes_total") >= (payload.len() + 4 + 1) as f64);
}

#[tokio::test]
async fn test_tcp_transport_tls_client_against_plaintext_server_fails() {
    let reg = Registry::new();
    let (receiver, mut packets, mut streams) =
        TcpTransport::new(&local_cfg(), "receiver", Some(&reg)).await.unwrap();
    let receiver_addr = format!("127.0.0.1:{}", receiver.auto_bind_port());
    let (sender, _p, _s) =
        TcpTransport::new(&with_tls(local_cfg()), "sender", Some(&reg)).await.unwrap();

    sender
        .write_to_with_result(b"test".to_vec(), receiver_addr.clone())
        .await
        .unwrap();
    // The TLS ClientHello (0x16) is neither a packet nor a stream.
    assert_eq!(
        wait_for_counter(&reg, "receiver_memberlist_tcp_transport_unknown_connections_total", 1.0).await,
        1.0
    );
    assert_eq!(
        wait_for_counter(&reg, "sender_memberlist_tcp_transport_packets_sent_errors_total", 1.0).await,
        1.0
    );

    let err = sender
        .dial_timeout(&receiver_addr, Duration::from_secs(5))
        .await
        .unwrap_err();
    assert!(!err.to_string().is_empty());
    assert_eq!(counter(&reg, "sender_memberlist_tcp_transport_outgoing_stream_errors_total"), 1.0);

    sender.shutdown_and_wait().await;
    receiver.shutdown_and_wait().await;
    assert!(packets.try_recv().is_err());
    assert!(streams.try_recv().is_err());
    assert_eq!(counter(&reg, "receiver_memberlist_tcp_transport_packets_received_total"), 0.0);
}

#[tokio::test]
async fn test_tcp_transport_plaintext_client_against_tls_server_is_rejected() {
    let (logs, _guard) = capture_logs();
    let reg = Registry::new();
    let (receiver, mut packets, _streams) =
        TcpTransport::new(&with_tls(local_cfg()), "receiver", Some(&reg)).await.unwrap();
    let receiver_addr = format!("127.0.0.1:{}", receiver.auto_bind_port());
    let (sender, _p, _s) = TcpTransport::new(&local_cfg(), "sender", Some(&reg)).await.unwrap();

    sender
        .write_to_with_result(b"test".to_vec(), receiver_addr)
        .await
        .unwrap();
    sender.shutdown_and_wait().await;

    let deadline = Instant::now() + Duration::from_secs(5);
    while !logs.text().contains("failed to read message type") && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(logs.text().contains("failed to read message type"), "{}", logs.text());
    receiver.shutdown_and_wait().await;
    assert!(packets.try_recv().is_err());
    assert_eq!(counter(&reg, "receiver_memberlist_tcp_transport_packets_received_total"), 0.0);
}

#[tokio::test]
async fn test_tcp_transport_tls_server_verification() {
    let (receiver, mut packets, _streams) =
        TcpTransport::new(&with_tls(local_cfg()), "", None).await.unwrap();
    let receiver_addr = format!("127.0.0.1:{}", receiver.auto_bind_port());

    async fn send(cfg: MemberlistConfig, addr: &str) -> f64 {
        let reg = Registry::new();
        let (sender, _p, _s) = TcpTransport::new(&cfg, "", Some(&reg)).await.unwrap();
        sender
            .write_to_with_result(b"verification test".to_vec(), addr.to_string())
            .await
            .unwrap();
        sender.shutdown_and_wait().await;
        counter(&reg, "memberlist_tcp_transport_packets_sent_errors_total")
    }

    // Untrusted CA: handshake fails.
    let mut cfg = with_tls(local_cfg());
    cfg.tls_ca_path = fixture("other-ca.crt");
    assert_eq!(send(cfg.clone(), &receiver_addr).await, 1.0);
    // Same, but verification disabled.
    cfg.tls_insecure_skip_verify = true;
    assert_eq!(send(cfg, &receiver_addr).await, 0.0);
    // Server name override not matching the certificate.
    let mut cfg = with_tls(local_cfg());
    cfg.tls_server_name = "kafka.mimir-rs.test".to_string();
    assert_eq!(send(cfg, &receiver_addr).await, 1.0);
    // Server name override matching the certificate (DNS SAN), dialing by IP.
    let mut cfg = with_tls(local_cfg());
    cfg.tls_server_name = "localhost".to_string();
    assert_eq!(send(cfg, &receiver_addr).await, 0.0);
    // Dialing by host name: ServerName is derived from the address (DNS SAN `localhost`).
    let by_name = format!("localhost:{}", receiver.auto_bind_port());
    assert_eq!(send(with_tls(local_cfg()), &by_name).await, 0.0);

    // Exactly the three successful sends arrived.
    for _ in 0..3 {
        let pkt = tokio::time::timeout(Duration::from_secs(5), packets.recv())
            .await
            .expect("timeout waiting for packet")
            .unwrap();
        assert_eq!(pkt.buf, b"verification test");
    }
    receiver.shutdown_and_wait().await;
    assert!(packets.try_recv().is_err());
}

#[tokio::test]
async fn test_tcp_transport_tls_server_without_key_pair_fails_handshakes() {
    let reg = Registry::new();
    let mut rcfg = with_tls(local_cfg());
    rcfg.tls_cert_path.clear();
    rcfg.tls_key_path.clear();
    let (receiver, mut packets, _streams) = TcpTransport::new(&rcfg, "", None).await.unwrap();
    let receiver_addr = format!("127.0.0.1:{}", receiver.auto_bind_port());

    let (sender, _p, _s) = TcpTransport::new(&with_tls(local_cfg()), "", Some(&reg)).await.unwrap();
    sender
        .write_to_with_result(b"test".to_vec(), receiver_addr)
        .await
        .unwrap();
    sender.shutdown_and_wait().await;
    assert_eq!(counter(&reg, "memberlist_tcp_transport_packets_sent_errors_total"), 1.0);
    receiver.shutdown_and_wait().await;
    assert!(packets.try_recv().is_err());
}

#[tokio::test]
async fn test_tcp_transport_invalid_tls_config_fails_creation() {
    let missing = "/nonexistent/memberlist-tls/file.pem";
    let cases: Vec<(MemberlistConfig, String)> = vec![
        (
            {
                let mut c = with_tls(local_cfg());
                c.tls_cert_path = missing.to_string();
                c
            },
            format!(
                "unable to create TLS config: error loading client cert: {missing}: open {missing}: no such file or directory"
            ),
        ),
        (
            {
                let mut c = with_tls(local_cfg());
                c.tls_key_path = missing.to_string();
                c
            },
            format!(
                "unable to create TLS config: error loading client key: {missing}: open {missing}: no such file or directory"
            ),
        ),
        (
            {
                let mut c = with_tls(local_cfg());
                c.tls_ca_path = missing.to_string();
                c
            },
            format!(
                "unable to create TLS config: error loading ca cert: {missing}: open {missing}: no such file or directory"
            ),
        ),
        (
            {
                let mut c = with_tls(local_cfg());
                c.tls_key_path.clear();
                c
            },
            "unable to create TLS config: certificate given but no key configured".to_string(),
        ),
        (
            {
                let mut c = with_tls(local_cfg());
                c.tls_cert_path.clear();
                c
            },
            "unable to create TLS config: key given but no certificate configured".to_string(),
        ),
        (
            {
                let mut c = with_tls(local_cfg());
                c.tls_min_version = "VersionTLS99".to_string();
                c
            },
            "unable to create TLS config: unknown minimum TLS version: \"VersionTLS99\"".to_string(),
        ),
        (
            {
                // The TLS config is built before the bind addresses are parsed (as in Go).
                let mut c = with_tls(local_cfg());
                c.tls_cipher_suites = "FOO".to_string();
                c.bind_addr = vec!["localhost".to_string()];
                c
            },
            "unable to create TLS config: unsupported cipher suite: \"FOO\"".to_string(),
        ),
    ];
    for (cfg, want) in cases {
        let err = TcpTransport::new(&cfg, "", None).await.err().expect("must fail");
        assert_eq!(err.to_string(), want);
    }

    // TLS settings are ignored unless tls_enabled.
    let mut cfg = local_cfg();
    cfg.tls_cert_path = missing.to_string();
    cfg.tls_key_path = missing.to_string();
    let (t, _p, _s) = TcpTransport::new(&cfg, "", None).await.unwrap();
    t.shutdown_and_wait().await;
}

#[tokio::test]
async fn test_tcp_transport_listener_error_message() {
    let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = taken.local_addr().unwrap().port();
    let mut cfg = local_cfg();
    cfg.bind_port = i64::from(port);
    let err = TcpTransport::new(&cfg, "", None).await.err().expect("must fail");
    assert_eq!(
        err.to_string(),
        format!(
            "failed to start TCP listener on \"127.0.0.1\" port {port}: listen tcp 127.0.0.1:{port}: bind: address already in use"
        )
    );
    let err = TcpTransport::new(&with_tls(cfg), "", None).await.err().expect("must fail");
    assert!(err
        .to_string()
        .starts_with(&format!("failed to start TLS TCP listener on \"127.0.0.1\" port {port}: ")));
}

#[tokio::test]
async fn test_tcp_transport_write_after_shutdown_fails() {
    let (t, _p, _s) = TcpTransport::new(&local_cfg(), "", None).await.unwrap();
    t.shutdown_and_wait().await;
    let err = t
        .write_to_with_result(b"x".to_vec(), "127.0.0.1:1".to_string())
        .await
        .unwrap_err();
    assert_eq!(err.to_string(), "transport is shutting down");
}

#[tokio::test]
async fn test_tcp_transport_legacy_bind_rejects_tls() {
    let err = TcpTransport::bind(&with_tls(local_cfg()), "", None)
        .await
        .err().expect("must fail");
    assert!(err.to_string().contains("requires TcpTransport::new"));
}
