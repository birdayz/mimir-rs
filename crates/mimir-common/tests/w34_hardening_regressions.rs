use std::io::{Cursor, Read as _, Write as _};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use mimir_common::activitytracker::{ActivityTracker, ActivityTrackerConfig};
use mimir_common::labels_hash::{prom_labels_hash, LabelsBytes, MAX_STRINGLABELS_STRING_LEN};
use mimir_common::services::{
    basic_service, Listener, Service, ServiceContext, ServiceError, ServiceManager, State,
};
use mimir_common::thread_pool::ScopedWorkerPool;
use mimir_common::tls::ClientConfig;
use mimir_common::tracing::parse_traceparent;
use mimir_common::usagestats::{
    BoxFuture, PrometheusVersion, Report, Reporter, ReporterConfig, SeedBucket, SeedBucketError,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[test]
fn labels_hash_rejects_len_1_shl_24_instead_of_24bit_wrap_ub() {
    assert_eq!(MAX_STRINGLABELS_STRING_LEN, (1 << 24) - 1);
    let oversized = "x".repeat(1 << 24);
    let res_encode = catch_unwind(AssertUnwindSafe(|| {
        let _ = LabelsBytes::from_sorted_labels(&[("k", &oversized)]);
    }));
    assert!(
        res_encode.is_err(),
        "1 << 24 byte label value must panic rather than wrap 24-bit length to 0"
    );

    let res_hash = catch_unwind(AssertUnwindSafe(|| {
        let _ = prom_labels_hash(&[("k", &oversized)]);
    }));
    assert!(
        res_hash.is_err(),
        "prom_labels_hash must reject 1 << 24 byte label value"
    );

    let val_300 = "y".repeat(300);
    let lb = LabelsBytes::from_sorted_labels(&[("a", &val_300)]);
    let decoded = LabelsBytes::from_raw(lb.as_bytes().to_vec()).expect("valid utf-8 framing");
    assert_eq!(decoded.get("a"), Some(val_300.as_str()));
}

#[test]
fn traceparent_accepts_all_8bit_lowercase_hex_flags_per_w3c() {
    let tid = "4bf92f3577b34da6a3ce929d0e0e4736";
    let sid = "00f067aa0ba902b7";
    for flag in 0u8..=u8::MAX {
        let hdr = format!("00-{tid}-{sid}-{flag:02x}");
        let ctx = parse_traceparent(&hdr)
            .unwrap_or_else(|| panic!("W3C traceparent must accept flags {flag:02x}"));
        assert_eq!(ctx.is_sampled(), (flag & 0x01) != 0);
        assert_eq!(ctx.trace_flags, flag);
        assert_eq!(ctx.to_traceparent(), hdr);
    }
    assert!(parse_traceparent(&format!("00-{tid}-{sid}-FF")).is_none());
    assert!(parse_traceparent(&format!("00-{tid}-{sid}-0")).is_none());
}

struct DummySeedBucket;

impl SeedBucket for DummySeedBucket {
    fn get_object<'a>(&'a self, _name: &'a str) -> BoxFuture<'a, Result<Vec<u8>, SeedBucketError>> {
        Box::pin(async { Err(SeedBucketError::NotFound) })
    }
    fn put_object<'a>(&'a self, _name: &'a str, _data: Vec<u8>) -> BoxFuture<'a, Result<(), String>> {
        Box::pin(async { Ok(()) })
    }
}

fn dummy_report() -> Report {
    Report {
        cluster_id: "c1".to_string(),
        created_at: std::time::SystemTime::UNIX_EPOCH,
        interval: std::time::SystemTime::UNIX_EPOCH,
        interval_period: 3600.0,
        target: "ingester".to_string(),
        version: PrometheusVersion {
            version: "0.1.0".to_string(),
            ..Default::default()
        },
        os: "linux".to_string(),
        arch: "amd64".to_string(),
        edition: "oss".to_string(),
        mode: "classic".to_string(),
        metrics: Default::default(),
    }
}

const TEST_CERT_PEM: &str = "-----BEGIN CERTIFICATE-----
MIIBhTCCASugAwIBAgIQIRi6zePL6mKjOipn+dNuaTAKBggqhkjOPQQDAjASMRAw
DgYDVQQKEwdBY21lIENvMB4XDTE3MTAyMDE5NDMwNloXDTE4MTAyMDE5NDMwNlow
EjEQMA4GA1UEChMHQWNtZSBDbzBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABD0d
7VNhbWvZLWPuj/RtHFjvtJBEwOkhbN/BnnE8rnZR8+sbwnc/KhCk3FhnpHZnQz7B
5aETbbIgmuvewdjvSBSjYzBhMA4GA1UdDwEB/wQEAwICpDATBgNVHSUEDDAKBggr
BgEFBQcDATAPBgNVHRMBAf8EBTADAQH/MCkGA1UdEQQiMCCCDmxvY2FsaG9zdDo1
NDUzgg4xMjcuMC4wLjE6NTQ1MzAKBggqhkjOPQQDAgNIADBFAiEA2zpJEPQyz6/l
Wf86aX6PepsntZv2GYlA5UpabfT2EZICICpJ5h/iI+i341gBmLiAFQOyTDT+/wQc
6MF9+Yw1Yy0t
-----END CERTIFICATE-----";

const TEST_KEY_PEM: &str = "-----BEGIN EC PRIVATE KEY-----
MHcCAQEEIIrYSSNQFaA2Hwf1duRSxKtLYX5CB04fSeQ6tF1aY/PuoAoGCCqGSM49
AwEHoUQDQgAEPR3tU2Fta9ktY+6P9G0cWO+0kETA6SFs38GecTyudlHz6xvCdz8q
EKTcWGekdmdDPsHloRNtsiCa697B2O9IFA==
-----END EC PRIVATE KEY-----";

#[tokio::test]
async fn usagestats_supports_https_caps_body_to_4kib_and_applies_timeout_to_body() {
    let dir = tempfile::tempdir().unwrap();
    let cert_path = dir.path().join("cert.pem");
    let key_path = dir.path().join("key.pem");
    std::fs::write(&cert_path, TEST_CERT_PEM).unwrap();
    std::fs::write(&key_path, TEST_KEY_PEM).unwrap();

    let tls_pair = ClientConfig {
        cert_path: cert_path.to_string_lossy().into_owned(),
        key_path: key_path.to_string_lossy().into_owned(),
        insecure_skip_verify: true,
        ..Default::default()
    }
    .get_client_and_server_tls_config()
    .unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let srv_cfg = Arc::clone(&tls_pair.server);
    let tls_srv = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut conn = rustls::ServerConnection::new(srv_cfg).unwrap();
        let mut rbuf = [0u8; 4096];
        let mut wbuf = Vec::new();
        while conn.is_handshaking() {
            while conn.wants_write() {
                wbuf.clear();
                conn.write_tls(&mut wbuf).unwrap();
                sock.write_all(&wbuf).await.unwrap();
            }
            if conn.wants_read() {
                let n = sock.read(&mut rbuf).await.unwrap();
                conn.read_tls(&mut Cursor::new(&rbuf[..n])).unwrap();
                conn.process_new_packets().unwrap();
            }
        }
        while conn.wants_write() {
            wbuf.clear();
            conn.write_tls(&mut wbuf).unwrap();
            sock.write_all(&wbuf).await.unwrap();
        }
        let mut req_plain = Vec::new();
        while !req_plain.windows(4).any(|w| w == b"\r\n\r\n") {
            let n = sock.read(&mut rbuf).await.unwrap();
            if n == 0 {
                break;
            }
            conn.read_tls(&mut Cursor::new(&rbuf[..n])).unwrap();
            conn.process_new_packets().unwrap();
            let _ = conn.reader().read_to_end(&mut req_plain);
        }
        conn.writer()
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
            .unwrap();
        conn.send_close_notify();
        while conn.wants_write() {
            wbuf.clear();
            conn.write_tls(&mut wbuf).unwrap();
            sock.write_all(&wbuf).await.unwrap();
        }
    });

    let reporter = Reporter::new(
        ReporterConfig {
            server_url: format!("https://{addr}/mimir-usage-report"),
            ..Default::default()
        },
        Arc::new(DummySeedBucket),
        None,
    );
    reporter.set_tls_config(tls_pair.client.config);
    reporter
        .send_report(&dummy_report())
        .await
        .expect("HTTPS usage stats report must succeed");
    tls_srv.await.unwrap();

    let big_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let big_addr = big_listener.local_addr().unwrap();
    let big_srv = tokio::spawn(async move {
        let (mut sock, _) = big_listener.accept().await.unwrap();
        let mut buf = [0u8; 4096];
        let _ = sock.read(&mut buf).await;
        let payload = "Z".repeat(64 * 1024);
        let resp = format!(
            "HTTP/1.1 500 Internal Server Error\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            payload.len(),
            payload
        );
        let _ = sock.write_all(resp.as_bytes()).await;
    });
    let big_rep = Reporter::new(
        ReporterConfig {
            server_url: format!("http://{big_addr}/report"),
            ..Default::default()
        },
        Arc::new(DummySeedBucket),
        None,
    );
    let err = big_rep.send_report(&dummy_report()).await.unwrap_err();
    assert!(err.contains("received status code: 500"), "got {err}");
    big_srv.abort();

    let slow_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let slow_addr = slow_listener.local_addr().unwrap();
    let slow_srv = tokio::spawn(async move {
        let (mut sock, _) = slow_listener.accept().await.unwrap();
        let mut buf = [0u8; 4096];
        let _ = sock.read(&mut buf).await;
        let _ = sock
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\nhi")
            .await;
        tokio::time::sleep(Duration::from_secs(5)).await;
    });
    let slow_rep = Reporter::new(
        ReporterConfig {
            server_url: format!("http://{slow_addr}/report"),
            ..Default::default()
        },
        Arc::new(DummySeedBucket),
        None,
    );
    slow_rep.set_request_timeout(Duration::from_millis(80));
    let timeout_err = slow_rep.send_report(&dummy_report()).await.unwrap_err();
    assert!(
        timeout_err.contains("timeout"),
        "stalled body must time out, got: {timeout_err}"
    );
    slow_srv.abort();
}

struct RecordingListener {
    failed_count: AtomicUsize,
}

impl Listener for RecordingListener {
    fn starting(&self) {}
    fn running(&self) {}
    fn stopping(&self, _from: State) {}
    fn terminated(&self, _from: State) {}
    fn failed(&self, _from: State, _err: &ServiceError) {
        self.failed_count.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn managed_basic_service_panic_in_hooks_transitions_to_failed_without_hanging() {
    for phase in ["start", "running", "stopping"] {
        let svc = match phase {
            "start" => basic_service(
                Some(Box::new(|_| Box::pin(async { panic!("start boom") }))),
                None,
                None,
            ),
            "running" => basic_service(
                None,
                Some(Box::new(|_| Box::pin(async { panic!("running boom") }))),
                None,
            ),
            "stopping" => basic_service(
                None,
                Some(Box::new(|ctx| {
                    Box::pin(async move {
                        ctx.canceled().await;
                        Ok(())
                    })
                })),
                Some(Box::new(|_| Box::pin(async { panic!("stopping boom") }))),
            ),
            _ => unreachable!(),
        };

        let listener = Arc::new(RecordingListener {
            failed_count: AtomicUsize::new(0),
        });
        svc.add_listener(listener.clone());
        let mgr = ServiceManager::new(vec![svc.clone()]).unwrap();
        mgr.start_async(ServiceContext::background()).await.unwrap();
        let ctx = svc.service_context();

        if phase == "start" {
            let healthy_res = tokio::time::timeout(Duration::from_secs(2), mgr.await_healthy())
                .await
                .expect("await_healthy must not hang on start panic");
            assert!(healthy_res.is_err());
        } else if phase == "stopping" {
            tokio::time::timeout(Duration::from_secs(2), mgr.await_healthy())
                .await
                .unwrap()
                .unwrap();
            mgr.stop_async();
        }

        tokio::time::timeout(Duration::from_secs(2), mgr.await_stopped())
            .await
            .expect("ServiceManager must not hang on service hook panic")
            .expect("await_stopped completes cleanly once all services are Failed/Terminated");

        assert_eq!(svc.state(), State::Failed, "phase={phase}");
        if let Some(ref c) = ctx {
            assert!(c.is_canceled(), "service_ctx must be canceled phase={phase}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(listener.failed_count.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn thread_pool_stress_and_activity_tracker_double_delete_and_panic_safety() {
    let pool = ScopedWorkerPool::new("w34-stress", 4);
    for jobs in 1..=20 {
        let sum = AtomicUsize::new(0);
        pool.scoped_execute(jobs, |i| {
            sum.fetch_add(i + 1, Ordering::Relaxed);
        });
        assert_eq!(sum.load(Ordering::Relaxed), jobs * (jobs + 1) / 2);
    }

    let dir = tempfile::tempdir().unwrap();
    let fp = dir.path().join("activity");
    let tr = ActivityTracker::new(
        ActivityTrackerConfig {
            filepath: fp,
            max_entries: 2,
        },
        None,
    )
    .unwrap()
    .unwrap();

    let panic_res = catch_unwind(AssertUnwindSafe(|| {
        let _ = tr.insert(|| -> String { panic!("generator exploded") });
    }));
    assert!(panic_res.is_err());
    assert_eq!(tr.free_slots_count(), 2.0);

    let s0 = tr.insert_static("op-a");
    assert_eq!(tr.free_slots_count(), 1.0);
    tr.delete(s0);
    tr.delete(s0);
    assert_eq!(
        tr.free_slots_count(),
        2.0,
        "double delete must not inflate free slots past max_entries"
    );

    let a = tr.insert_static("first");
    let b = tr.insert_static("second");
    assert_ne!(a, b, "slots after double-delete must remain distinct");
    assert_eq!(tr.insert_static("third"), -1);
}
