//! Wave 17 regression tests for Track 1 (W17-LSH-01..03):
//! - W17-LSH-01: Cancel-safe `DynamicSemaphore::acquire` / `ReactiveLimiter` (no permit leaks on drop).
//! - W17-LSH-02: Handler-owned `PushTapPermit` across `spawn_blocking` when gRPC future is dropped.
//! - W17-LSH-03: `failsafe-go` `CircuitBreaker` 10-bucket `TimedStats`, `CountingStats` ring, and 2-decimal `Round`.

mod common;

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::http;
use bytes::{BufMut, Bytes, BytesMut};
use mimir_common::limiter::{DynamicSemaphore, ReactiveLimiter, ReactiveLimiterConfig};
use mimir_config::{CircuitBreakerConfig, GoDuration, InstanceLimits};
use mimir_ingester::circuit_breaker::{BreakerState, CircuitBreaker};
use mimir_ingester::IngesterService;
use mimir_proto::cortexpb;
use mimir_proto::prost::Message;
use mimir_tsdb::head_chunks::{WriteFaultAction, WriteFaultHook};
use prometheus::Registry;
use tempfile::TempDir;
use tower::ServiceExt;

use common::{
    default_ingester_test_config, default_limits_test_config, metric_value,
    prepare_ingester_with_blocks_storage, start_and_wait_healthy,
};

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("unix epoch")
        .as_millis() as i64
}

fn encode_grpc_frame(payload: &[u8]) -> Bytes {
    let mut buf = BytesMut::with_capacity(5 + payload.len());
    buf.put_u8(0); // uncompressed
    buf.put_u32(payload.len() as u32);
    buf.extend_from_slice(payload);
    buf.freeze()
}

fn large_write_request(num_series: usize, ts_ms: i64) -> cortexpb::WriteRequest {
    let mut timeseries = Vec::with_capacity(num_series);
    for i in 0..num_series {
        timeseries.push(cortexpb::TimeSeries {
            labels: vec![
                cortexpb::LabelPair {
                    name: Bytes::from_static(b"__name__"),
                    value: Bytes::from_static(b"w17_push_metric"),
                },
                cortexpb::LabelPair {
                    name: Bytes::from_static(b"instance"),
                    value: Bytes::from(format!("inst-{i:04}-padding-to-exceed-2048-bytes")),
                },
            ],
            samples: vec![cortexpb::Sample {
                value: i as f64,
                timestamp_ms: ts_ms,
            }],
            ..Default::default()
        });
    }
    cortexpb::WriteRequest {
        timeseries,
        source: cortexpb::write_request::SourceEnum::Api as i32,
        ..Default::default()
    }
}

struct SingleChunkBody(Option<Bytes>);

impl hyper::body::Body for SingleChunkBody {
    type Data = Bytes;
    type Error = tonic::Status;

    #[allow(clippy::result_large_err)]
    fn poll_frame(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        Poll::Ready(self.0.take().map(|b| Ok(hyper::body::Frame::data(b))))
    }
}

fn build_grpc_push_http_request(
    tenant: &str,
    frame: Bytes,
) -> http::Request<tonic::body::BoxBody> {
    let len_str = frame.len().to_string();
    let body = tonic::body::BoxBody::new(SingleChunkBody(Some(frame)));
    http::Request::builder()
        .method(http::Method::POST)
        .uri("/cortex.Ingester/Push")
        .header("content-type", "application/grpc")
        .header("x-scope-orgid", tenant)
        .header("message-size", len_str)
        .body(body)
        .expect("build http request")
}

// ---------------------------------------------------------------------------
// W17-LSH-01: Cancel-safe DynamicSemaphore & ReactiveLimiter
// ---------------------------------------------------------------------------

#[tokio::test]
async fn w17_lsh_01_cancelling_100_queued_acquire_permit_calls_leaks_zero_permits() {
    let rl = ReactiveLimiter::new(ReactiveLimiterConfig {
        min_limit: 1,
        initial_limit: 5,
        max_limit: 10,
        max_limit_factor: 1.0,
        min_limit_factor: 1.0,
        // High max_rejection_factor so all 100 queued callers enter semaphore.acquire().
        initial_rejection_factor: 25.0,
        max_rejection_factor: 25.0,
        recent_window_min_duration: Duration::from_millis(1),
        recent_window_max_duration: Duration::from_millis(1),
        recent_window_min_samples: 1,
        ..Default::default()
    })
    .expect("create reactive limiter");

    let mut active = Vec::with_capacity(5);
    for _ in 0..5 {
        active.push(rl.try_acquire_permit().expect("acquire initial 5"));
    }
    assert_eq!(rl.inflight(), 5);
    assert_eq!(rl.queued(), 0);

    // Spawn 100 queued acquire_permit() tasks behind the 5 active permits.
    let mut handles = Vec::with_capacity(100);
    for _ in 0..100 {
        let rl_c = Arc::clone(&rl);
        handles.push(tokio::spawn(async move {
            let p = rl_c.acquire_permit().await.expect("acquire");
            p.drop_permit();
        }));
    }

    // Wait until all 100 tasks are queued.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while rl.queued() < 100 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for 100 queued waiters, got {}",
            rl.queued()
        );
        tokio::task::yield_now().await;
    }

    // Abort all 100 queued tasks and await their JoinHandles so every future is dropped.
    for h in &handles {
        h.abort();
    }
    for h in handles {
        let _ = h.await;
    }

    // Queue must be completely drained with zero phantom waiters.
    assert_eq!(rl.queued(), 0, "cancelled waiters must not stay queued");
    assert_eq!(rl.inflight(), 5, "only the 5 active holders remain");

    // Release the 5 active permits.
    for p in active {
        p.drop_permit();
    }

    assert_eq!(rl.queued(), 0);
    assert_eq!(rl.inflight(), 0, "all 5 permits must be free after release");

    // Verify all 5 permits can be re-acquired immediately without leaking.
    let mut reacquired = Vec::with_capacity(5);
    for i in 0..5 {
        reacquired.push(
            rl.try_acquire_permit()
                .unwrap_or_else(|| panic!("permit {i} was leaked")),
        );
    }
    assert!(rl.try_acquire_permit().is_none());
    for p in reacquired {
        p.drop_permit();
    }
    assert_eq!(rl.inflight(), 0);
}

#[tokio::test]
async fn w17_lsh_01_semaphore_direct_wake_then_drop_passes_permit_to_next_waiter() {
    let sem = Arc::new(DynamicSemaphore::new(1));
    assert!(sem.try_acquire());

    let mut f1 = Box::pin(sem.acquire());
    let mut f2 = Box::pin(sem.acquire());
    let waker = std::task::Waker::noop();
    let mut cx = std::task::Context::from_waker(waker);

    assert!(f1.as_mut().poll(&mut cx).is_pending());
    assert!(f2.as_mut().poll(&mut cx).is_pending());
    assert_eq!(sem.waiters(), 2);

    // Release the active permit: wakes f1 and sets used = 1.
    sem.release();
    assert_eq!(sem.used(), 1);
    assert_eq!(sem.waiters(), 1);

    // Drop f1 BEFORE polling it again: f1's WaiterGuard must release its granted permit,
    // waking f2 and keeping used = 1 (not 2!).
    drop(f1);
    assert_eq!(sem.used(), 1);
    assert_eq!(sem.waiters(), 0);

    // f2 now completes cleanly.
    assert!(f2.as_mut().poll(&mut cx).is_ready());
    sem.release();
    assert_eq!(sem.used(), 0);
}

// ---------------------------------------------------------------------------
// W17-LSH-02: Handler-owned PushTapPermit across spawn_blocking
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn w17_lsh_02_dropped_grpc_push_holds_inflight_and_trips_breaker_when_blocking_finishes() {
    let dir = TempDir::new().unwrap();
    let reg = Registry::new();
    let (mut cfg, blocks_cfg, ingest_cfg) = default_ingester_test_config(dir.path());
    cfg.instance_limits = InstanceLimits {
        max_inflight_push_requests: 1,
        max_inflight_push_requests_bytes: 1024 * 1024,
        ..Default::default()
    };
    cfg.push_circuit_breaker = CircuitBreakerConfig {
        enabled: true,
        failure_threshold_percentage: 10,
        failure_execution_threshold: 3,
        thresholding_period: GoDuration::from_secs(60),
        cooldown_period: GoDuration::from_secs(30),
        initial_delay: GoDuration(0),
        request_timeout: GoDuration::from_millis(40),
    };
    cfg.push_reactive_limiter = mimir_config::ReactiveLimiterConfig {
        enabled: true,
        min_limit: 1,
        initial_limit: 5,
        max_limit: 10,
        max_limit_factor: 1.0,
        min_limit_factor: 1.0,
        recent_window_min_duration: GoDuration::from_millis(1),
        recent_window_max_duration: GoDuration::from_millis(1),
        recent_window_min_samples: 1,
        initial_rejection_factor: 1.0,
        max_rejection_factor: 1.0,
        ..Default::default()
    };

    let (ing, _, _) = prepare_ingester_with_blocks_storage(
        dir.path(),
        cfg,
        blocks_cfg,
        ingest_cfg,
        default_limits_test_config(),
        Some(&reg),
    );
    start_and_wait_healthy(&ing).await;

    let svc = IngesterService::new(Arc::clone(&ing)).into_server();

    // Seed tenant "t-w17" so UserTsdb & WAL exist (does not trip breaker: fast small push).
    let seed_wr = large_write_request(1, now_ms());
    ing.push_grpc("t-w17", &seed_wr).await.expect("seed push");

    // Reset breaker closed stats cleanly so only the 2 timed-out pushes count toward execution_threshold=2.
    let push_cb = ing.circuit_breaker.push.as_ref().unwrap();
    push_cb.force_open();
    push_cb.force_close();

    let base_ts = now_ms();
    let db = ing.get_tsdb("t-w17").expect("tenant tsdb");

    for attempt in 0..2 {
        // Prepare a >= 2048-byte payload with strictly advancing timestamps per attempt
        // so every attempt commits new samples to the WAL and takes the spawn_blocking path.
        let big_wr = large_write_request(80, base_ts + (attempt as i64 + 1) * 1000);
        let big_payload = big_wr.encode_to_vec();
        assert!(
            big_payload.len() >= 2048,
            "payload must exceed 2048 bytes (got {})",
            big_payload.len()
        );
        let big_frame = encode_grpc_frame(&big_payload);

        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let entered = Arc::new(AtomicBool::new(false));
        let gate_hook = Arc::clone(&gate);
        let entered_hook = Arc::clone(&entered);

        let hook: WriteFaultHook = Arc::new(move |_| {
            entered_hook.store(true, Ordering::Release);
            let (lock, cvar) = &*gate_hook;
            let mut release = lock.lock().expect("gate lock");
            while !*release {
                release = cvar.wait(release).expect("condvar wait");
            }
            WriteFaultAction::PassThrough
        });
        db.db.head().set_wal_write_fault_hook_for_test(Some(hook));

        let http_req = build_grpc_push_http_request("t-w17", big_frame.clone());
        let mut call_fut = Box::pin(svc.clone().oneshot(http_req));

        // Drive the gRPC future until the blocking WAL write enters the fault hook.
        tokio::select! {
            res = &mut call_fut => {
                let (lock, cvar) = &*gate;
                *lock.lock().expect("gate lock") = true;
                cvar.notify_all();
                let mut resp = res.expect("http resp");
                let status = resp.status();
                let headers = resp.headers().clone();
                let mut body = Vec::new();
                let mut trailers = None;
                let mut b = std::pin::Pin::new(resp.body_mut());
                while let Some(frame) = std::future::poll_fn(|cx| hyper::body::Body::poll_frame(b.as_mut(), cx)).await {
                    if let Ok(f) = frame {
                        if let Some(d) = f.data_ref() {
                            body.extend_from_slice(d);
                        }
                        if let Some(t) = f.trailers_ref() {
                            trailers = Some(t.clone());
                        }
                    }
                }
                panic!(
                    "attempt={attempt}: push completed unexpectedly while WAL gate was closed: status={status} headers={headers:?} trailers={trailers:?} num_series={}",
                    db.db.head().num_series()
                );
            }
            _ = async {
                while !entered.load(Ordering::Acquire) {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            } => {}
        }

        // Simulate grpc-timeout / client RST_STREAM by dropping the gRPC future mid-push!
        drop(call_fut);

        // W17-LSH-02 invariant 1: PushTapPermit is owned by the spawn_blocking task, so
        // inflight counters remain elevated until the blocking push actually finishes.
        assert_eq!(
            ing.inflight_push_requests.load(Ordering::Acquire),
            1,
            "attempt {attempt}: inflight_push_requests must stay 1 while spawn_blocking runs"
        );
        assert!(
            ing.inflight_push_requests_bytes.load(Ordering::Acquire) >= big_frame.len() as i64,
            "attempt {attempt}: inflight_push_requests_bytes must stay held while spawn_blocking runs"
        );

        // On attempt 0, verify a second concurrent request is rejected by max_inflight_push_requests=1
        // while task #1 is still running in spawn_blocking (records 1 non-deadline success on breaker).
        if attempt == 0 {
            let second_req = build_grpc_push_http_request("t-w17", big_frame.clone());
            let second_resp = svc.clone().oneshot(second_req).await.expect("http resp");
            let grpc_status = second_resp
                .headers()
                .get("grpc-status")
                .and_then(|v| v.to_str().ok());
            assert_eq!(
                grpc_status,
                Some("14"),
                "concurrent push must be rejected with Unavailable (14) by max_inflight_push_requests"
            );
        }

        // Wait past request_timeout (40ms), then unblock the WAL write.
        tokio::time::sleep(Duration::from_millis(55)).await;
        {
            let (lock, cvar) = &*gate;
            let mut release = lock.lock().expect("gate lock");
            *release = true;
            cvar.notify_all();
        }
        db.db.head().set_wal_write_fault_hook_for_test(None);

        // Wait for spawn_blocking to finish and release PushTapPermit.
        let wait_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while ing.inflight_push_requests.load(Ordering::Acquire) != 0 {
            assert!(
                tokio::time::Instant::now() < wait_deadline,
                "timed out waiting for spawn_blocking push to finish"
            );
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        assert_eq!(ing.inflight_push_requests_bytes.load(Ordering::Acquire), 0);
    }

    // W17-LSH-02 invariant 2: Push circuit breaker recorded both slow pushes as timeouts and opened!
    assert_eq!(
        metric_value(
            &reg,
            "cortex_ingester_circuit_breaker_request_timeouts_total",
            &[("request_type", "push")],
        ),
        Some(2.0)
    );
    assert_eq!(
        metric_value(
            &reg,
            "cortex_ingester_circuit_breaker_results_total",
            &[("request_type", "push"), ("result", "error")],
        ),
        Some(2.0)
    );
    assert_eq!(push_cb.state(), BreakerState::Open);

    // W17-LSH-02 invariant 3: Reactive limiter recorded the RTT samples instead of discarding them.
    let push_rl = ing.reactive_limiter.push.as_ref().unwrap();
    assert!(
        push_rl.max_inflight() >= 1,
        "reactive limiter must record RTT samples for completed blocking pushes"
    );

    let _ = ing.stop().await;
}

// ---------------------------------------------------------------------------
// W17-LSH-03: failsafe-go CircuitBreaker rounding & HalfOpen decision parity
// ---------------------------------------------------------------------------

#[test]
fn w17_lsh_03_circuit_breaker_2_decimal_rounding_and_half_open_100th_record() {
    // Closed state: 95/1000 = 0.095 rounds to 0.10 >= 0.10 -> Open.
    let cfg_closed = CircuitBreakerConfig {
        enabled: true,
        failure_threshold_percentage: 10,
        failure_execution_threshold: 1000,
        thresholding_period: GoDuration::from_secs(60),
        cooldown_period: GoDuration::from_secs(30),
        request_timeout: GoDuration::from_secs(10),
        ..Default::default()
    };
    let cb = CircuitBreaker::new(&cfg_closed, None, "push", false).unwrap();
    cb.activate();
    for _ in 0..95 {
        cb.try_acquire_permit()
            .unwrap()
            .unwrap()
            .finish(Duration::from_secs(1), true);
    }
    for _ in 0..904 {
        cb.try_acquire_permit()
            .unwrap()
            .unwrap()
            .finish(Duration::from_millis(1), false);
    }
    assert_eq!(cb.state(), BreakerState::Closed);
    cb.try_acquire_permit()
        .unwrap()
        .unwrap()
        .finish(Duration::from_millis(1), false);
    assert_eq!(
        cb.state(),
        BreakerState::Open,
        "95/1000 (0.095 -> 0.10) must open breaker at 1000th execution"
    );

    // HalfOpen: 10 timeouts + 90 successes with threshold=100, pct=10 -> must re-open on 100th record.
    let cfg_ho = CircuitBreakerConfig {
        enabled: true,
        failure_threshold_percentage: 10,
        failure_execution_threshold: 100,
        thresholding_period: GoDuration::from_secs(60),
        cooldown_period: GoDuration::from_secs(30),
        request_timeout: GoDuration::from_secs(10),
        ..Default::default()
    };
    let cb_ho = CircuitBreaker::new(&cfg_ho, None, "push", false).unwrap();
    cb_ho.activate();
    cb_ho.force_half_open();
    for _ in 0..10 {
        cb_ho
            .try_acquire_permit()
            .unwrap()
            .unwrap()
            .finish(Duration::from_secs(1), true);
    }
    for _ in 0..89 {
        cb_ho
            .try_acquire_permit()
            .unwrap()
            .unwrap()
            .finish(Duration::from_millis(1), false);
        assert_eq!(cb_ho.state(), BreakerState::HalfOpen);
    }
    cb_ho
        .try_acquire_permit()
        .unwrap()
        .unwrap()
        .finish(Duration::from_millis(1), false);
    assert_eq!(
        cb_ho.state(),
        BreakerState::Open,
        "10 failures + 90 successes in HalfOpen must re-open at exactly the 100th record"
    );
}
