//! Ported from:
//! - github.com/grafana/dskit/server/*_test.go (24 tests)
//! - github.com/prometheus/exporter-toolkit/web/*_test.go (15 tests)

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use axum::body::Body;
use axum::extract::Request as AxumRequest;
use axum::http::{header, Request as HttpRequest, StatusCode};
use axum::routing::get;
use axum::Router;
use bytes::Bytes;
use http_body_util::BodyExt as _;
use mimir_cmd::grpc::httpgrpc_error;
use mimir_cmd::grpc_call_check::{
    is_method_name_valid, DskitGrpcMethodLimiter, FinishFn, GrpcInflightLimitCheck,
    TestableTimer, TimeAfterFuncMock, UNPROCESSED_REQUEST_CHECK_TIMEOUT,
};
use mimir_cmd::http::{build_http_router, AppHttpState};
use mimir_cmd::modules::ModuleStates;
use mimir_cmd::server::http_server::ServedOverHttp;
use mimir_cmd::server::metrics::ServerMetrics;
use mimir_cmd::server::tls::{
    string_to_cipher_suites, string_to_curve_preferences, string_to_tls_version,
};
use mimir_cmd::server::web::{
    tls_next_protos, web_tls_is_enabled, WebAuthHandler, WebCache, WebConfig, WebFlagConfig,
    WebFlagError,
};
use mimir_cmd::server::Server;
use mimir_common::logging::{LogFormat, LogLevel, LogSink, Logger};
use mimir_config::server::TLSConfig;
use mimir_config::{GoDuration, LimitsConfig, MimirConfig, Overrides, ServerConfig};
use parking_lot::Mutex;
use prometheus::{Counter, Gauge, Registry};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tonic::metadata::MetadataMap;
use tonic::transport::Server as TonicServer;
use tonic::{Code, Status};
use tower::ServiceExt as _;

struct CaptureLogger {
    buf: Arc<Mutex<Vec<u8>>>,
    sink: LogSink,
}

impl CaptureLogger {
    fn new(level: LogLevel) -> Self {
        let buf = Arc::new(Mutex::new(Vec::new()));
        #[derive(Clone)]
        struct BufWriter(Arc<Mutex<Vec<u8>>>);
        impl io::Write for BufWriter {
            fn write(&mut self, data: &[u8]) -> io::Result<usize> {
                self.0.lock().extend_from_slice(data);
                Ok(data.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let sink = LogSink::new(LogFormat::Logfmt, level, Box::new(BufWriter(Arc::clone(&buf))));
        Self { buf, sink }
    }

    fn text(&self) -> String {
        String::from_utf8_lossy(&self.buf.lock()).into_owned()
    }


}

impl Logger for CaptureLogger {
    fn log(&self, level: LogLevel, kvs: &[(&str, &str)]) {
        self.sink.log(level, kvs);
    }
}

fn test_http_state(cfg: MimirConfig, reg: Registry) -> AppHttpState {
    let cfg = Arc::new(cfg);
    AppHttpState {
        config: Arc::clone(&cfg),
        default_config: cfg,
        registry: reg,
        shutdown_requested: Arc::new(AtomicBool::new(false)),
        ingester: None,
        runtime_config: None,
        overrides: Overrides::new(LimitsConfig::default()),
        memberlist_kv: None,
        modules: ModuleStates::new(),
        cost_attribution_registry: None,
    }
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
        "-days", "2", "-subj", &format!("/CN=test-ca-{serial}"),
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
        "-CAkey", ca_key.to_str().unwrap(), "-set_serial", &ser_hex,
        "-out", cli_crt.to_str().unwrap(), "-days", "2", "-extfile", ext.to_str().unwrap(),
    ]);
    (ca_crt, srv_crt, srv_key, cli_crt, cli_key)
}

fn et_testdata_dir() -> PathBuf {
    PathBuf::from("/home/birdy/go/pkg/mod/github.com/prometheus/exporter-toolkit@v0.20.0/web/testdata")
}

fn copy_dir_all(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let ty = entry.file_type().unwrap();
        if ty.is_file() {
            let target = dst.join(entry.file_name());
            std::fs::copy(entry.path(), &target).unwrap();
            let mut perms = std::fs::metadata(&target).unwrap().permissions();
            #[allow(clippy::permissions_set_readonly_false)]
            perms.set_readonly(false);
            let _ = std::fs::set_permissions(&target, perms);
        }
    }
}

// ============================================================================
// github.com/grafana/dskit/server/limits_test.go (6 tests)
// ============================================================================

struct MethodLimiter {
    all_inflight_limit: i64,
    all_inflight: AtomicI64,
    protected_method: String,
    protected_limit: i64,
    protected_inflight: AtomicI64,
}

impl DskitGrpcMethodLimiter for MethodLimiter {
    type CallCtx = String;

    fn rpc_call_starting(&self, method_name: &str, md: &MetadataMap) -> Result<String, Status> {
        if md.get("abort-request").and_then(|v| v.to_str().ok()) == Some("true") {
            return Err(Status::aborted("aborted"));
        }
        let v = self.all_inflight.fetch_add(1, Ordering::SeqCst) + 1;
        if self.all_inflight_limit > 0 && v > self.all_inflight_limit {
            self.all_inflight.fetch_sub(1, Ordering::SeqCst);
            return Err(Status::unavailable("too many requests"));
        }
        if method_name == self.protected_method && self.protected_limit > 0 {
            let pv = self.protected_inflight.fetch_add(1, Ordering::SeqCst) + 1;
            if pv > self.protected_limit {
                self.protected_inflight.fetch_sub(1, Ordering::SeqCst);
                self.all_inflight.fetch_sub(1, Ordering::SeqCst);
                return Err(Status::resource_exhausted(format!(
                    "too many requests to {}",
                    self.protected_method
                )));
            }
        }
        Ok(method_name.to_string())
    }

    fn rpc_call_processing(
        &self,
        _ctx: &String,
        _method_name: &str,
    ) -> Result<Option<FinishFn>, Status> {
        Ok(None)
    }

    fn rpc_call_finished(&self, ctx: &String) {
        self.all_inflight.fetch_sub(1, Ordering::SeqCst);
        if *ctx == self.protected_method && self.protected_limit > 0 {
            self.protected_inflight.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

type StartCb = Box<dyn Fn(&str, &MetadataMap) -> Result<(), Box<Status>> + Send + Sync>;
type ScheduledCb = Arc<Mutex<Option<Box<dyn FnOnce() + Send + Sync>>>>;

struct FuncMethodLimiter {
    on_start: StartCb,
    on_finish: Box<dyn Fn() + Send + Sync>,
}

impl DskitGrpcMethodLimiter for FuncMethodLimiter {
    type CallCtx = ();
    fn rpc_call_starting(&self, m: &str, md: &MetadataMap) -> Result<(), Status> {
        (self.on_start)(m, md).map_err(|b| *b)
    }
    fn rpc_call_processing(&self, _: &(), _: &str) -> Result<Option<FinishFn>, Status> {
        Ok(None)
    }
    fn rpc_call_finished(&self, _: &()) {
        (self.on_finish)();
    }
}

struct LimitSvc<L: DskitGrpcMethodLimiter> {
    check: Arc<GrpcInflightLimitCheck<L>>,
    unblock: Arc<tokio::sync::Notify>,
    unblocked: Arc<AtomicBool>,
    stream_msgs: usize,
}

impl<L: DskitGrpcMethodLimiter> Clone for LimitSvc<L> {
    fn clone(&self) -> Self {
        Self {
            check: Arc::clone(&self.check),
            unblock: Arc::clone(&self.unblock),
            unblocked: Arc::clone(&self.unblocked),
            stream_msgs: self.stream_msgs,
        }
    }
}

impl<L: DskitGrpcMethodLimiter> tonic::server::NamedService for LimitSvc<L> {
    const NAME: &'static str = "server.FakeServer";
}

impl<L: DskitGrpcMethodLimiter> tower::Service<http::Request<tonic::body::BoxBody>> for LimitSvc<L> {
    type Response = http::Response<tonic::body::BoxBody>;
    type Error = std::convert::Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;
    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, req: http::Request<tonic::body::BoxBody>) -> Self::Future {
        let check = Arc::clone(&self.check);
        let unblock = Arc::clone(&self.unblock);
        let unblocked = Arc::clone(&self.unblocked);
        let stream_msgs = self.stream_msgs;
        let path = req.uri().path().to_string();
        let md = MetadataMap::from_headers(req.headers().clone());
        Box::pin(async move {
            let state = match check.tap_handle(&path, &md, Arc::new(|| false)) {
                Ok(st) => st,
                Err(status) => return Ok(status.into_http()),
            };
            if !is_method_name_valid(&path) {
                return Ok(Status::unimplemented("malformed method name").into_http());
            }
            if let Some(ref st) = state {
                check.handle_rpc_in_header(st);
                if let Err(s) = check.rpc_call_processing(st, &path) {
                    check.handle_rpc_end(st);
                    return Ok(s.into_http());
                }
            }
            let resp = if path == "/server.FakeServer/StreamSleep" {
                let (tx, rx) = tokio::sync::mpsc::channel(8);
                let st_c = state.clone();
                let chk_c = Arc::clone(&check);
                tokio::spawn(async move {
                    for _ in 0..stream_msgs {
                        if tx.send(Ok(())).await.is_err() {
                            break;
                        }
                        if !unblocked.load(Ordering::Acquire) {
                            unblock.notified().await;
                        }
                    }
                    if let Some(ref st) = st_c {
                        chk_c.handle_rpc_end(st);
                    }
                });
                let mut grpc = tonic::server::Grpc::new(tonic::codec::ProstCodec::default());
                struct Svc(tokio::sync::mpsc::Receiver<Result<(), Status>>);
                impl tonic::server::ServerStreamingService<()> for Svc {
                    type Response = ();
                    type ResponseStream = tokio_stream::wrappers::ReceiverStream<Result<(), Status>>;
                    type Future = Pin<Box<dyn Future<Output = Result<tonic::Response<Self::ResponseStream>, Status>> + Send + 'static>>;
                    fn call(&mut self, _: tonic::Request<()>) -> Self::Future {
                        let (_, dummy_rx) = tokio::sync::mpsc::channel(1);
                        let rx = std::mem::replace(&mut self.0, dummy_rx);
                        Box::pin(async move {
                            Ok(tonic::Response::new(tokio_stream::wrappers::ReceiverStream::new(rx)))
                        })
                    }
                }
                grpc.server_streaming(Svc(rx), req).await
            } else {
                if !unblocked.load(Ordering::Acquire) {
                    unblock.notified().await;
                }
                if let Some(ref st) = state {
                    check.handle_rpc_end(st);
                }
                let mut grpc = tonic::server::Grpc::new(tonic::codec::ProstCodec::default());
                struct OkUnary;
                impl tonic::server::UnaryService<()> for OkUnary {
                    type Response = ();
                    type Future = Pin<Box<dyn Future<Output = Result<tonic::Response<()>, Status>> + Send + 'static>>;
                    fn call(&mut self, _: tonic::Request<()>) -> Self::Future {
                        Box::pin(async { Ok(tonic::Response::new(())) })
                    }
                }
                grpc.unary(OkUnary, req).await
            };
            Ok(resp)
        })
    }
}

async fn start_limit_server<L: DskitGrpcMethodLimiter>(
    svc: LimitSvc<L>,
) -> (tonic::transport::Channel, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        let _ = TonicServer::builder()
            .add_service(svc)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await;
    });
    let ch = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    (ch, handle)
}

#[allow(clippy::result_large_err)]
async fn invoke_unary(ch: &tonic::transport::Channel, path: &str, abort: bool) -> Result<(), Status> {
    let mut client = tonic::client::Grpc::new(ch.clone());
    client.ready().await.unwrap();
    let mut req = tonic::Request::new(());
    if abort {
        req.metadata_mut().insert("abort-request", "true".parse().unwrap());
    }
    let p: http::uri::PathAndQuery = path.parse().unwrap();
    let codec = tonic::codec::ProstCodec::<(), ()>::default();
    client.unary(req, p, codec).await.map(|_| ())
}

#[allow(clippy::result_large_err)]
async fn invoke_stream(ch: &tonic::transport::Channel, expected: usize, abort: bool) -> Result<(), Status> {
    let mut client = tonic::client::Grpc::new(ch.clone());
    client.ready().await.unwrap();
    let mut req = tonic::Request::new(());
    if abort {
        req.metadata_mut().insert("abort-request", "true".parse().unwrap());
    }
    let p: http::uri::PathAndQuery = "/server.FakeServer/StreamSleep".parse().unwrap();
    let codec = tonic::codec::ProstCodec::<(), ()>::default();
    let mut stream = client.server_streaming(req, p, codec).await?.into_inner();
    let mut count = 0;
    while stream.message().await?.is_some() {
        count += 1;
    }
    assert_eq!(count, expected);
    Ok(())
}

// Upstream: github.com/grafana/dskit/server/limits_test.go::TestGrpcLimitCheckMalformedMethodName
#[tokio::test]
async fn test_grpc_limit_check_malformed_method_name() {
    let ml = Arc::new(MethodLimiter {
        all_inflight_limit: 0,
        all_inflight: AtomicI64::new(0),
        protected_method: "bad_method_name".to_string(),
        protected_limit: 0,
        protected_inflight: AtomicI64::new(0),
    });
    let check = Arc::new(GrpcInflightLimitCheck::new(Arc::clone(&ml)));
    let mut svc = LimitSvc {
        check,
        unblock: Arc::new(tokio::sync::Notify::new()),
        unblocked: Arc::new(AtomicBool::new(false)),
        stream_msgs: 0,
    };
    let req = http::Request::builder()
        .uri("/bad_method_name")
        .body(tonic::body::empty_body())
        .unwrap();
    let resp = tower::Service::call(&mut svc, req).await.unwrap();
    let st = Status::from_header_map(resp.headers()).unwrap();
    assert_eq!(st.code(), Code::Unimplemented);
    assert!(st.message().contains("malformed method name"), "{}", st.message());
    assert_eq!(ml.all_inflight.load(Ordering::SeqCst), 0);
    assert_eq!(ml.protected_inflight.load(Ordering::SeqCst), 0);
}

// Upstream: github.com/grafana/dskit/server/limits_test.go::TestGrpcLimitCheckUnary
#[tokio::test]
async fn test_grpc_limit_check_unary() {
    for (all_limit, prot_limit) in [(5, 3), (5, 0)] {
        let ml = Arc::new(MethodLimiter {
            all_inflight_limit: all_limit,
            all_inflight: AtomicI64::new(0),
            protected_method: "/server.FakeServer/Succeed".to_string(),
            protected_limit: prot_limit,
            protected_inflight: AtomicI64::new(0),
        });
        let unblock = Arc::new(tokio::sync::Notify::new());
        let unblocked = Arc::new(AtomicBool::new(false));
        let check = Arc::new(GrpcInflightLimitCheck::new(Arc::clone(&ml)));
        let (ch, h) = start_limit_server(LimitSvc {
            check,
            unblock: Arc::clone(&unblock),
            unblocked: Arc::clone(&unblocked),
            stream_msgs: 5,
        })
        .await;

        let mut tasks = Vec::new();
        for _ in 0..prot_limit {
            let c = ch.clone();
            tasks.push(tokio::spawn(async move {
                invoke_unary(&c, "/server.FakeServer/Succeed", false)
                    .await
                    .unwrap();
            }));
        }
        if prot_limit > 0 {
            for _ in 0..40 {
                if ml.protected_inflight.load(Ordering::SeqCst) == prot_limit {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let err = invoke_unary(&ch, "/server.FakeServer/Succeed", false)
                .await
                .unwrap_err();
            assert_eq!(err.code(), Code::ResourceExhausted);
            assert_eq!(err.message(), "too many requests to /server.FakeServer/Succeed");
        }

        let abort_err = invoke_unary(&ch, "/server.FakeServer/Sleep", true)
            .await
            .unwrap_err();
        assert_eq!(abort_err.code(), Code::Aborted);

        let extra = all_limit - prot_limit;
        for _ in 0..extra {
            let c = ch.clone();
            tasks.push(tokio::spawn(async move {
                invoke_unary(&c, "/server.FakeServer/Sleep", false)
                    .await
                    .unwrap();
            }));
        }
        for _ in 0..40 {
            if ml.all_inflight.load(Ordering::SeqCst) == all_limit {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let err = invoke_unary(&ch, "/server.FakeServer/Sleep", false)
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::Unavailable);

        unblocked.store(true, Ordering::Release);
        unblock.notify_waiters();
        for t in tasks {
            t.await.unwrap();
        }
        assert_eq!(ml.all_inflight.load(Ordering::SeqCst), 0);
        assert_eq!(ml.protected_inflight.load(Ordering::SeqCst), 0);
        invoke_unary(&ch, "/server.FakeServer/Succeed", false)
            .await
            .unwrap();
        h.abort();
    }
}

// Upstream: github.com/grafana/dskit/server/limits_test.go::TestGrpcLimitCheckStreaming
#[tokio::test]
async fn test_grpc_limit_check_streaming() {
    let ml = Arc::new(MethodLimiter {
        all_inflight_limit: 5,
        all_inflight: AtomicI64::new(0),
        protected_method: "/server.FakeServer/StreamSleep".to_string(),
        protected_limit: 3,
        protected_inflight: AtomicI64::new(0),
    });
    let unblock = Arc::new(tokio::sync::Notify::new());
    let unblocked = Arc::new(AtomicBool::new(false));
    let check = Arc::new(GrpcInflightLimitCheck::new(Arc::clone(&ml)));
    let (ch, h) = start_limit_server(LimitSvc {
        check,
        unblock: Arc::clone(&unblock),
        unblocked: Arc::clone(&unblocked),
        stream_msgs: 5,
    })
    .await;

    let mut tasks = Vec::new();
    for _ in 0..3 {
        let c = ch.clone();
        tasks.push(tokio::spawn(async move {
            invoke_stream(&c, 5, false).await.unwrap();
        }));
    }
    for _ in 0..40 {
        if ml.protected_inflight.load(Ordering::SeqCst) == 3 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let err = invoke_stream(&ch, 5, false).await.unwrap_err();
    assert_eq!(err.code(), Code::ResourceExhausted);
    assert_eq!(err.message(), "too many requests to /server.FakeServer/StreamSleep");

    unblocked.store(true, Ordering::Release);
    unblock.notify_waiters();
    for t in tasks {
        t.await.unwrap();
    }
    assert_eq!(ml.all_inflight.load(Ordering::SeqCst), 0);
    assert_eq!(ml.protected_inflight.load(Ordering::SeqCst), 0);
    h.abort();
}

// Upstream: github.com/grafana/dskit/server/limits_test.go::TestGrpcLimitCheckWithContextDeadlineExpiredRightAfterTapHandleCheck
#[test]
fn test_grpc_limit_check_with_context_deadline_expired_right_after_tap_handle_check() {
    let finished_called = Arc::new(AtomicBool::new(false));
    let finished_c = Arc::clone(&finished_called);
    let ctx_done = Arc::new(AtomicBool::new(false));
    let ctx_done_c = Arc::clone(&ctx_done);
    let ml = Arc::new(FuncMethodLimiter {
        on_start: Box::new(move |_, _| {
            assert!(!ctx_done_c.load(Ordering::SeqCst));
            ctx_done_c.store(true, Ordering::SeqCst);
            Ok(())
        }),
        on_finish: Box::new(move || {
            finished_c.store(true, Ordering::SeqCst);
        }),
    });
    let scheduled: ScheduledCb = Arc::new(Mutex::new(None));
    let sched_c = Arc::clone(&scheduled);
    let mock: TimeAfterFuncMock = Arc::new(move |d, cb| {
        assert_eq!(d, UNPROCESSED_REQUEST_CHECK_TIMEOUT);
        *sched_c.lock() = Some(cb);
        TestableTimer::new(|| {
            panic!("Timer.Stop() should not be called when request aborted early");
        })
    });
    let check = GrpcInflightLimitCheck::new(ml).with_time_after_mock(mock);
    let ctx_done_flag = Arc::clone(&ctx_done);
    let _state = check
        .tap_handle(
            "/server.FakeServer/Succeed",
            &MetadataMap::new(),
            Arc::new(move || ctx_done_flag.load(Ordering::SeqCst)),
        )
        .unwrap()
        .unwrap();
    assert!(!finished_called.load(Ordering::SeqCst));
    let timer_fn = scheduled.lock().take().expect("timer scheduled");
    timer_fn();
    assert!(finished_called.load(Ordering::SeqCst));
}

// Upstream: github.com/grafana/dskit/server/limits_test.go::TestGrpcLimitCheckTimerFiresButRequestIsBeingProcessedCallsRPCCallFinishedOnlyOnce
#[test]
fn test_grpc_limit_check_timer_fires_but_request_is_being_processed_calls_rpc_call_finished_only_once() {
    let finished_count = Arc::new(AtomicI64::new(0));
    let fc = Arc::clone(&finished_count);
    let ctx_done = Arc::new(AtomicBool::new(false));
    let ctx_done_c = Arc::clone(&ctx_done);
    let ml = Arc::new(FuncMethodLimiter {
        on_start: Box::new(move |_, _| {
            ctx_done_c.store(true, Ordering::SeqCst);
            Ok(())
        }),
        on_finish: Box::new(move || {
            fc.fetch_add(1, Ordering::SeqCst);
        }),
    });
    let mock: TimeAfterFuncMock = Arc::new(|_, cb| {
        cb(); // Fires immediately!
        TestableTimer::new(|| true)
    });
    let check = GrpcInflightLimitCheck::new(ml).with_time_after_mock(mock);
    let ctx_done_flag = Arc::clone(&ctx_done);
    let state = check
        .tap_handle(
            "/server.FakeServer/Succeed",
            &MetadataMap::new(),
            Arc::new(move || ctx_done_flag.load(Ordering::SeqCst)),
        )
        .unwrap()
        .unwrap();
    check.handle_rpc_in_header(&state);
    check.handle_rpc_end(&state);
    assert_eq!(finished_count.load(Ordering::SeqCst), 1);
}

// Upstream: github.com/grafana/dskit/server/limits_test.go::TestGrpcLimitCheckSuccessfulRequestCancelsTheTimer
#[test]
fn test_grpc_limit_check_successful_request_cancels_the_timer() {
    let finished_called = Arc::new(AtomicBool::new(false));
    let fc = Arc::clone(&finished_called);
    let stop_called = Arc::new(AtomicBool::new(false));
    let sc = Arc::clone(&stop_called);
    let ml = Arc::new(FuncMethodLimiter {
        on_start: Box::new(|_, _| Ok(())),
        on_finish: Box::new(move || {
            fc.store(true, Ordering::SeqCst);
        }),
    });
    let mock: TimeAfterFuncMock = Arc::new(move |d, _| {
        assert_eq!(d, UNPROCESSED_REQUEST_CHECK_TIMEOUT);
        let sc2 = Arc::clone(&sc);
        TestableTimer::new(move || {
            sc2.store(true, Ordering::SeqCst);
            true
        })
    });
    let check = GrpcInflightLimitCheck::new(ml).with_time_after_mock(mock);
    let state = check
        .tap_handle("/server.FakeServer/Succeed", &MetadataMap::new(), Arc::new(|| false))
        .unwrap()
        .unwrap();
    check.handle_rpc_in_header(&state);
    check.handle_rpc_end(&state);
    assert!(stop_called.load(Ordering::SeqCst));
    assert!(finished_called.load(Ordering::SeqCst));
}

// ============================================================================
// github.com/grafana/dskit/server/metrics_cache_test.go (1 test)
// ============================================================================

// Upstream: github.com/grafana/dskit/server/metrics_cache_test.go::TestMetricsCacheDuration
#[tokio::test]
async fn test_metrics_cache_duration() {
    for dur in [GoDuration::ZERO, GoDuration::from_secs(15)] {
        let mut cfg = MimirConfig::default();
        cfg.server.metrics_cache_duration = dur;
        cfg.server.http_path_prefix = "/test".to_string();
        cfg.validate().unwrap();
        let reg = Registry::new();
        let g = Gauge::new("test_cache_value", "Value.").unwrap();
        reg.register(Box::new(g.clone())).unwrap();
        let router = build_http_router(test_http_state(cfg, reg));

        let scrape = |r: Router| async move {
            let resp = r
                .oneshot(HttpRequest::builder().uri("/test/metrics").body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
            String::from_utf8(resp.into_body().collect().await.unwrap().to_bytes().to_vec()).unwrap()
        };

        g.set(1.0);
        assert!(scrape(router.clone()).await.contains("test_cache_value 1
"));
        g.set(2.0);
        let second = scrape(router).await;
        if dur > GoDuration::ZERO {
            assert!(second.contains("test_cache_value 1
"), "{second}");
        } else {
            assert!(second.contains("test_cache_value 2
"), "{second}");
        }
    }

    let mut cfg = MimirConfig::default();
    cfg.server.metrics_cache_duration = GoDuration::from_secs(-1);
    let err = cfg.server.validate().unwrap_err().to_string();
    assert!(
        err.contains("server.metrics-cache-duration must not be negative"),
        "{err}"
    );
}

// ============================================================================
// github.com/grafana/dskit/server/tls_config_test.go (3 tests)
// ============================================================================

// Upstream: github.com/grafana/dskit/server/tls_config_test.go::Test_stringToCipherSuites
#[test]
fn test_string_to_cipher_suites() {
    assert!(string_to_cipher_suites("").unwrap().is_none());
    assert!(string_to_cipher_suites("not-a-cipher").is_err());
    assert!(string_to_cipher_suites("TLS_AES_256_GCM_SHA384").unwrap().is_some());
    assert!(string_to_cipher_suites("TLS_AES_256_GCM_SHA384,TLS_CHACHA20_POLY1305_SHA256")
        .unwrap()
        .is_some());
    assert!(string_to_cipher_suites("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256")
        .unwrap()
        .is_some());
}

// Upstream: github.com/grafana/dskit/server/tls_config_test.go::Test_stringToCurvePreferences
#[test]
fn test_string_to_curve_preferences() {
    assert!(string_to_curve_preferences("").unwrap().is_none());
    assert!(string_to_curve_preferences("not-a-curve").is_err());
    assert!(string_to_curve_preferences("CurveP256,").is_err());
    assert_eq!(string_to_curve_preferences("X25519").unwrap().unwrap().len(), 1);
    assert_eq!(
        string_to_curve_preferences("CurveP256,CurveP384")
            .unwrap()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        string_to_curve_preferences("CurveP256,CurveP384,CurveP521,X25519")
            .unwrap()
            .unwrap()
            .len(),
        3
    );
    assert!(string_to_curve_preferences("X25519MLKEM768").is_err());
    assert!(string_to_curve_preferences("SecP256r1MLKEM768").is_err());
    assert!(string_to_curve_preferences("SecP384r1MLKEM1024").is_err());
    assert_eq!(
        string_to_curve_preferences("X25519MLKEM768,X25519")
            .unwrap()
            .unwrap()
            .len(),
        1
    );
}

// Upstream: github.com/grafana/dskit/server/tls_config_test.go::Test_stringToTLSVersion
#[test]
fn test_string_to_tls_version() {
    assert_eq!(string_to_tls_version("").unwrap().len(), 2);
    assert!(string_to_tls_version("not-a-version").is_err());
    assert_eq!(string_to_tls_version("VersionTLS12").unwrap().len(), 2);
    assert_eq!(string_to_tls_version("VersionTLS13").unwrap().len(), 1);
}

// ============================================================================
// github.com/grafana/dskit/server/server_test.go (14 tests)
// ============================================================================

#[derive(Clone)]
struct FakeGrpcSvc;

impl tonic::server::NamedService for FakeGrpcSvc {
    const NAME: &'static str = "server.FakeServer";
}

impl tower::Service<http::Request<tonic::body::BoxBody>> for FakeGrpcSvc {
    type Response = http::Response<tonic::body::BoxBody>;
    type Error = std::convert::Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;
    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, req: http::Request<tonic::body::BoxBody>) -> Self::Future {
        let path = req.uri().path().to_string();
        let peer_ip = req
            .extensions()
            .get::<tonic::transport::server::TcpConnectInfo>()
            .and_then(|c| c.remote_addr())
            .map(|a| a.ip().to_string())
            .unwrap_or_default();
        Box::pin(async move {
            let mut grpc = tonic::server::Grpc::new(tonic::codec::ProstCodec::default());
            struct UnaryFn<F>(F);
            impl<F, Fut> tonic::server::UnaryService<()> for UnaryFn<F>
            where
                F: FnMut() -> Fut + Send + 'static,
                Fut: Future<Output = Result<tonic::Response<()>, Status>> + Send + 'static,
            {
                type Response = ();
                type Future = Pin<Box<dyn Future<Output = Result<tonic::Response<()>, Status>> + Send + 'static>>;
                fn call(&mut self, _: tonic::Request<()>) -> Self::Future {
                    Box::pin((self.0)())
                }
            }
            match path.as_str() {
                "/server.FakeServer/Succeed" => Ok(grpc.unary(UnaryFn(|| async { Ok(tonic::Response::new(())) }), req).await),
                "/server.FakeServer/FailWithError" => Ok(Status::unknown("test error").into_http()),
                "/server.FakeServer/FailWithHTTPError" => Ok(httpgrpc_error(402, "402").into_http()),
                "/server.FakeServer/Sleep" => {
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    Ok(grpc.unary(UnaryFn(|| async { Ok(tonic::Response::new(())) }), req).await)
                }
                "/server.FakeServer/ReturnProxyProtoCallerIP" => {
                    Ok(grpc.unary(UnaryFn(move || {
                        let ip = peer_ip.clone();
                        async move {
                            let mut r = tonic::Response::new(());
                            r.metadata_mut().insert("x-caller-ip", ip.parse().unwrap());
                            Ok(r)
                        }
                    }), req).await)
                }
                _ => Ok(Status::unimplemented("unknown").into_http()),
            }
        })
    }
}

fn auto_server_cfg() -> ServerConfig {
    ServerConfig {
        http_listen_address: "127.0.0.1".to_string(),
        http_listen_port: 0,
        grpc_listen_address: "127.0.0.1".to_string(),
        grpc_listen_port: 0,
        ..Default::default()
    }
}

// Upstream: github.com/grafana/dskit/server/server_test.go::TestDefaultAddresses
#[tokio::test]
async fn test_default_addresses() {
    let def = ServerConfig::default();
    assert_eq!(def.grpc_listen_port, 9095);
    assert_eq!(def.http_listen_port, 8080);

    let cfg = auto_server_cfg();
    let reg = Registry::new();
    let srv = Server::new(&cfg, &reg, Arc::new(CaptureLogger::new(LogLevel::Info))).unwrap();
    let http_addr = srv.http_addr().unwrap();
    let grpc_addr = srv.grpc_addr().unwrap();
    let router = Router::new().route("/test", get(|| async { StatusCode::NO_CONTENT }));
    let running = srv.start(router, tonic::service::Routes::new(mimir_cmd::grpc::HttpGrpcStatusFixService::new(FakeGrpcSvc))).unwrap();

    let ch = tonic::transport::Endpoint::from_shared(format!("http://{grpc_addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    invoke_unary(&ch, "/server.FakeServer/Succeed", false)
        .await
        .unwrap();
    let resp = reqwest::get(format!("http://{http_addr}/test")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    running.shutdown().await;
}

// Upstream: github.com/grafana/dskit/server/server_test.go::TestErrorInstrumentationMiddleware
#[tokio::test]
async fn test_error_instrumentation_middleware() {
    let cfg = auto_server_cfg();
    let reg = Registry::new();
    let srv = Server::new(&cfg, &reg, Arc::new(CaptureLogger::new(LogLevel::Error))).unwrap();
    let http_addr = srv.http_addr().unwrap();
    let grpc_addr = srv.grpc_addr().unwrap();
    let router = Router::new()
        .route("/succeed", get(|| async { StatusCode::OK }))
        .route("/error500", get(|| async { StatusCode::INTERNAL_SERVER_ERROR }));
    let running = srv.start(router, tonic::service::Routes::new(mimir_cmd::grpc::HttpGrpcStatusFixService::new(FakeGrpcSvc))).unwrap();

    let ch = tonic::transport::Endpoint::from_shared(format!("http://{grpc_addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    invoke_unary(&ch, "/server.FakeServer/Succeed", false).await.unwrap();
    let err = invoke_unary(&ch, "/server.FakeServer/FailWithError", false)
        .await
        .unwrap_err();
    assert_eq!(err.message(), "test error");
    let err = invoke_unary(&ch, "/server.FakeServer/FailWithHTTPError", false)
        .await
        .unwrap_err();
    assert_eq!(
        mimir_proto::grpcutilpb::extract_http_response(&err).unwrap().code,
        402
    );

    assert_eq!(
        reqwest::get(format!("http://{http_addr}/succeed")).await.unwrap().status(),
        StatusCode::OK
    );
    assert_eq!(
        reqwest::get(format!("http://{http_addr}/error500")).await.unwrap().status(),
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(
        reqwest::get(format!("http://{http_addr}/missing")).await.unwrap().status(),
        StatusCode::NOT_FOUND
    );
    drop(ch);
    running.shutdown().await;

    let mut statuses = std::collections::HashMap::new();
    for mf in reg.gather() {
        if mf.get_name() == "cortex_request_duration_seconds" {
            for m in mf.get_metric() {
                let mut route = String::new();
                let mut code = String::new();
                for l in m.get_label() {
                    if l.get_name() == "route" {
                        route = l.get_value().to_string();
                    } else if l.get_name() == "status_code" {
                        code = l.get_value().to_string();
                    }
                }
                if m.get_histogram().get_sample_count() > 0 {
                    statuses.insert(route, code);
                }
            }
        }
    }
    assert_eq!(statuses.get("/server.FakeServer/Succeed").map(String::as_str), Some("OK"));
    assert_eq!(statuses.get("/server.FakeServer/FailWithError").map(String::as_str), Some("Unknown"));
    assert_eq!(statuses.get("/server.FakeServer/FailWithHTTPError").map(String::as_str), Some("402"));
    assert_eq!(statuses.get("succeed").map(String::as_str), Some("200"));
    assert_eq!(statuses.get("error500").map(String::as_str), Some("500"));
    assert_eq!(statuses.get("other").map(String::as_str), Some("404"));
}

// Upstream: github.com/grafana/dskit/server/server_test.go::TestHTTPInstrumentationMetrics
#[tokio::test]
async fn test_http_instrumentation_metrics() {
    let cfg = auto_server_cfg();
    let reg = Registry::new();
    let srv = Server::new(&cfg, &reg, Arc::new(CaptureLogger::new(LogLevel::Error))).unwrap();
    let http_addr = srv.http_addr().unwrap();
    let router = Router::new()
        .route("/succeed", get(|| async { "OK" }))
        .route("/error500", get(|| async { StatusCode::INTERNAL_SERVER_ERROR }))
        .route("/post", axum::routing::post(|body: Bytes| async move { let _ = body; StatusCode::OK }));
    let running = srv.start(router, tonic::service::Routes::new(mimir_cmd::grpc::HttpGrpcStatusFixService::new(FakeGrpcSvc))).unwrap();

    let client = reqwest::Client::new();
    assert_eq!(
        client.get(format!("http://{http_addr}/succeed")).send().await.unwrap().text().await.unwrap(),
        "OK"
    );
    let _ = client.get(format!("http://{http_addr}/error500")).send().await.unwrap();
    let _ = client.post(format!("http://{http_addr}/post")).body("Body").send().await.unwrap();
    drop(client);
    running.shutdown().await;

    let mut found_req_bytes = false;
    let mut found_resp_bytes = false;
    for mf in reg.gather() {
        if mf.get_name() == "cortex_request_message_bytes" {
            for m in mf.get_metric() {
                if m.get_label().iter().any(|l| l.get_name() == "route" && l.get_value() == "post") {
                    assert_eq!(m.get_histogram().get_sample_sum(), 4.0);
                    found_req_bytes = true;
                }
            }
        }
        if mf.get_name() == "cortex_response_message_bytes" {
            for m in mf.get_metric() {
                if m.get_label().iter().any(|l| l.get_name() == "route" && l.get_value() == "succeed") {
                    assert_eq!(m.get_histogram().get_sample_sum(), 2.0);
                    found_resp_bytes = true;
                }
            }
        }
    }
    assert!(found_req_bytes && found_resp_bytes);
}

// Upstream: github.com/grafana/dskit/server/server_test.go::TestRunReturnsError
#[test]
fn test_run_returns_error() {
    for net in ["tcp", "tcp4"] {
        let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = occupied.local_addr().unwrap().port() as i64;
        let cfg = ServerConfig {
            http_listen_network: net.to_string(),
            http_listen_address: "127.0.0.1".to_string(),
            http_listen_port: port,
            grpc_listen_network: net.to_string(),
            grpc_listen_address: "127.0.0.1".to_string(),
            grpc_listen_port: 0,
            ..Default::default()
        };
        assert!(Server::new(&cfg, &Registry::new(), Arc::new(CaptureLogger::new(LogLevel::Info))).is_err());

        let cfg2 = ServerConfig {
            http_listen_network: net.to_string(),
            http_listen_address: "127.0.0.1".to_string(),
            http_listen_port: 0,
            grpc_listen_network: net.to_string(),
            grpc_listen_address: "127.0.0.1".to_string(),
            grpc_listen_port: port,
            ..Default::default()
        };
        assert!(Server::new(&cfg2, &Registry::new(), Arc::new(CaptureLogger::new(LogLevel::Info))).is_err());
    }
}

// Upstream: github.com/grafana/dskit/server/server_test.go::TestMiddlewareLogging
#[tokio::test]
async fn test_middleware_logging() {
    let logger = Arc::new(CaptureLogger::new(LogLevel::Info));
    let mut cfg = auto_server_cfg();
    cfg.log_level = mimir_config::LogLevel::Info;
    let reg = Registry::new();
    let srv = Server::new(&cfg, &reg, Arc::clone(&logger) as Arc<dyn Logger>).unwrap();
    let http_addr = srv.http_addr().unwrap();
    let router = Router::new().route("/error500", get(|| async { (StatusCode::INTERNAL_SERVER_ERROR, "boom") }));
    let running = srv.start(router, tonic::service::Routes::new(mimir_cmd::grpc::HttpGrpcStatusFixService::new(FakeGrpcSvc))).unwrap();
    let resp = reqwest::get(format!("http://{http_addr}/error500")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    running.shutdown().await;
    assert!(logger.text().contains("GET /error500 (500)"), "{}", logger.text());
}

// Upstream: github.com/grafana/dskit/server/server_test.go::TestTLSServer
#[tokio::test]
async fn test_tls_server() {
    let dir = TempDir::new().unwrap();
    let (ca_crt, srv_crt, srv_key, cli_crt, cli_key) = gen_ca_and_certs(dir.path(), 1);
    let mut cfg = auto_server_cfg();
    cfg.http_tls_config = TLSConfig {
        cert_file: srv_crt.to_string_lossy().into_owned(),
        key_file: srv_key.to_string_lossy().into_owned(),
        client_auth_type: "RequireAndVerifyClientCert".to_string(),
        client_ca_file: ca_crt.to_string_lossy().into_owned(),
        ..Default::default()
    };
    cfg.grpc_tls_config = TLSConfig {
        cert_file: srv_crt.to_string_lossy().into_owned(),
        key_file: srv_key.to_string_lossy().into_owned(),
        client_auth_type: "VerifyClientCertIfGiven".to_string(),
        client_ca_file: ca_crt.to_string_lossy().into_owned(),
        ..Default::default()
    };
    let srv = Server::new(&cfg, &Registry::new(), Arc::new(CaptureLogger::new(LogLevel::Info))).unwrap();
    let http_addr = srv.http_addr().unwrap();
    let grpc_addr = srv.grpc_addr().unwrap();
    let router = Router::new().route("/testhttps", get(|| async { "Hello World!" }));
    let running = srv.start(router, tonic::service::Routes::new(mimir_cmd::grpc::HttpGrpcStatusFixService::new(FakeGrpcSvc))).unwrap();

    let ca_pem = std::fs::read(&ca_crt).unwrap();
    let cli_pem = [std::fs::read(&cli_crt).unwrap(), std::fs::read(&cli_key).unwrap()].concat();
    let id = reqwest::Identity::from_pem(&cli_pem).unwrap();
    let cert = reqwest::Certificate::from_pem(&ca_pem).unwrap();
    let client = reqwest::Client::builder()
        .add_root_certificate(cert.clone())
        .identity(id)
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();
    let body = client
        .get(format!("https://localhost:{}/testhttps", http_addr.port()))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert_eq!(body, "Hello World!");

    let tls_cfg = tonic::transport::ClientTlsConfig::new()
        .ca_certificate(tonic::transport::Certificate::from_pem(ca_pem))
        .identity(tonic::transport::Identity::from_pem(
            std::fs::read(&cli_crt).unwrap(),
            std::fs::read(&cli_key).unwrap(),
        ))
        .domain_name("localhost");
    let ch = tonic::transport::Endpoint::from_shared(format!("https://127.0.0.1:{}", grpc_addr.port()))
        .unwrap()
        .tls_config(tls_cfg)
        .unwrap()
        .connect()
        .await
        .unwrap();
    invoke_unary(&ch, "/server.FakeServer/Succeed", false).await.unwrap();
    drop(ch);
    running.shutdown().await;
}

// Upstream: github.com/grafana/dskit/server/server_test.go::TestTLSServerWithInlineCerts
#[tokio::test]
async fn test_tls_server_with_inline_certs() {
    let dir = TempDir::new().unwrap();
    let (ca_crt, srv_crt, srv_key, cli_crt, cli_key) = gen_ca_and_certs(dir.path(), 2);
    let ca_text = std::fs::read_to_string(&ca_crt).unwrap();
    let srv_crt_text = std::fs::read_to_string(&srv_crt).unwrap();
    let srv_key_text = std::fs::read_to_string(&srv_key).unwrap();

    let mut cfg = auto_server_cfg();
    cfg.http_tls_config = TLSConfig {
        cert: srv_crt_text.clone(),
        key: srv_key_text.clone(),
        client_auth_type: "RequireAndVerifyClientCert".to_string(),
        client_ca: ca_text.clone(),
        ..Default::default()
    };
    cfg.grpc_tls_config = TLSConfig {
        cert: srv_crt_text,
        key: srv_key_text,
        client_auth_type: "VerifyClientCertIfGiven".to_string(),
        client_ca: ca_text.clone(),
        ..Default::default()
    };
    let srv = Server::new(&cfg, &Registry::new(), Arc::new(CaptureLogger::new(LogLevel::Info))).unwrap();
    let http_addr = srv.http_addr().unwrap();
    let grpc_addr = srv.grpc_addr().unwrap();
    let router = Router::new().route("/testhttps", get(|| async { "Hello World!" }));
    let running = srv.start(router, tonic::service::Routes::new(mimir_cmd::grpc::HttpGrpcStatusFixService::new(FakeGrpcSvc))).unwrap();

    let cli_pem = [std::fs::read(&cli_crt).unwrap(), std::fs::read(&cli_key).unwrap()].concat();
    let client = reqwest::Client::builder()
        .add_root_certificate(reqwest::Certificate::from_pem(ca_text.as_bytes()).unwrap())
        .identity(reqwest::Identity::from_pem(&cli_pem).unwrap())
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();
    let body = client
        .get(format!("https://localhost:{}/testhttps", http_addr.port()))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert_eq!(body, "Hello World!");

    let tls_cfg = tonic::transport::ClientTlsConfig::new()
        .ca_certificate(tonic::transport::Certificate::from_pem(ca_text.as_bytes()))
        .identity(tonic::transport::Identity::from_pem(
            std::fs::read(&cli_crt).unwrap(),
            std::fs::read(&cli_key).unwrap(),
        ))
        .domain_name("localhost");
    let ch = tonic::transport::Endpoint::from_shared(format!("https://127.0.0.1:{}", grpc_addr.port()))
        .unwrap()
        .tls_config(tls_cfg)
        .unwrap()
        .connect()
        .await
        .unwrap();
    invoke_unary(&ch, "/server.FakeServer/Succeed", false).await.unwrap();
    drop(ch);
    running.shutdown().await;
}

// Upstream: github.com/grafana/dskit/server/server_test.go::TestTLSServerWithCurvePreferences
#[tokio::test]
async fn test_tls_server_with_curve_preferences() {
    let dir = TempDir::new().unwrap();
    let (ca_crt, srv_crt, srv_key, cli_crt, cli_key) = gen_ca_and_certs(dir.path(), 3);
    let mut cfg = auto_server_cfg();
    cfg.tls_curve_preferences = "CurveP256,X25519".to_string();
    cfg.http_tls_config = TLSConfig {
        cert_file: srv_crt.to_string_lossy().into_owned(),
        key_file: srv_key.to_string_lossy().into_owned(),
        client_auth_type: "RequireAndVerifyClientCert".to_string(),
        client_ca_file: ca_crt.to_string_lossy().into_owned(),
        ..Default::default()
    };
    let srv = Server::new(&cfg, &Registry::new(), Arc::new(CaptureLogger::new(LogLevel::Info))).unwrap();
    let http_addr = srv.http_addr().unwrap();
    let running = srv
        .start(Router::new().route("/testhttps", get(|| async { "ok" })), tonic::service::Routes::new(FakeGrpcSvc))
        .unwrap();

    let mut provider = rustls::crypto::ring::default_provider();
    provider.kx_groups = vec![rustls::crypto::ring::kx_group::SECP256R1];
    let mut roots = rustls::RootCertStore::empty();
    for c in CertificateDer::pem_file_iter(&ca_crt).unwrap().map(Result::unwrap) {
        roots.add(c).unwrap();
    }
    let cli_certs: Vec<_> = CertificateDer::pem_file_iter(&cli_crt).unwrap().map(Result::unwrap).collect();
    let cli_key_der = PrivateKeyDer::from_pem_file(&cli_key).unwrap();
    let cli_cfg = rustls::ClientConfig::builder_with_provider(Arc::new(provider))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_client_auth_cert(cli_certs, cli_key_der)
        .unwrap();
    let connector = TlsConnector::from(Arc::new(cli_cfg));
    let tcp = TcpStream::connect(http_addr).await.unwrap();
    let mut tls = connector
        .connect(ServerName::try_from("localhost").unwrap(), tcp)
        .await
        .unwrap();
    tls.write_all(b"GET /testhttps HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut out = String::new();
    tls.read_to_string(&mut out).await.unwrap();
    assert!(out.contains("200 OK"), "{out}");
    running.shutdown().await;

    let mut bad_cfg = auto_server_cfg();
    bad_cfg.tls_curve_preferences = "not-a-curve".to_string();
    bad_cfg.http_tls_config = cfg.http_tls_config;
    assert!(Server::new(&bad_cfg, &Registry::new(), Arc::new(CaptureLogger::new(LogLevel::Info))).is_err());
}

// Upstream: github.com/grafana/dskit/server/server_test.go::TestLogSourceIPs
#[tokio::test]
async fn test_log_source_ips() {
    for use_proxy in [false, true] {
        let logger = Arc::new(CaptureLogger::new(LogLevel::Info));
        let mut cfg = auto_server_cfg();
        cfg.log_source_ips_enabled = true;
        cfg.proxy_protocol_enabled = use_proxy;
        let srv = Server::new(&cfg, &Registry::new(), Arc::clone(&logger) as Arc<dyn Logger>).unwrap();
        let http_addr = srv.http_addr().unwrap();
        let running = srv
            .start(
                Router::new().route("/error500", get(|| async { StatusCode::INTERNAL_SERVER_ERROR })),
                tonic::service::Routes::new(FakeGrpcSvc),
            )
            .unwrap();
        let mut s = TcpStream::connect(http_addr).await.unwrap();
        if use_proxy {
            s.write_all(b"PROXY TCP4 1.2.3.4 192.168.0.1 51234 80\r\n")
                .await
                .unwrap();
        }
        s.write_all(b"GET /error500 HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut resp = String::new();
        s.read_to_string(&mut resp).await.unwrap();
        assert!(resp.contains("500"), "{resp}");
        running.shutdown().await;
        let expected = if use_proxy { "sourceIPs=1.2.3.4" } else { "sourceIPs=127.0.0.1" };
        assert!(logger.text().contains(expected), "{}", logger.text());
    }
}

// Upstream: github.com/grafana/dskit/server/server_test.go::TestStopWithDisabledSignalHandling
#[tokio::test]
async fn test_stop_with_disabled_signal_handling() {
    let cfg = auto_server_cfg();
    let srv = Server::new(&cfg, &Registry::new(), Arc::new(CaptureLogger::new(LogLevel::Info))).unwrap();
    let running = srv.start(Router::new(), tonic::service::Routes::new(FakeGrpcSvc)).unwrap();
    let mut handle = running.shutdown_handle();
    handle.trigger_stop();
    handle.wait_stopped().await;
}

// Upstream: github.com/grafana/dskit/server/server_test.go::TestHttpOverProxyProtocol
#[tokio::test]
async fn test_http_over_proxy_protocol() {
    let mut cfg = auto_server_cfg();
    cfg.proxy_protocol_enabled = true;
    let srv = Server::new(&cfg, &Registry::new(), Arc::new(CaptureLogger::new(LogLevel::Info))).unwrap();
    let http_addr = srv.http_addr().unwrap();
    let router = Router::new().route(
        "/test-proxy-proto",
        get(|req: AxumRequest| async move {
            req.extensions()
                .get::<ServedOverHttp>()
                .map(|s| s.remote_addr.ip().to_string())
                .unwrap_or_default()
        }),
    );
    let running = srv.start(router, tonic::service::Routes::new(mimir_cmd::grpc::HttpGrpcStatusFixService::new(FakeGrpcSvc))).unwrap();

    // 1. Good PROXY header.
    let mut s = TcpStream::connect(http_addr).await.unwrap();
    s.write_all(b"PROXY TCP4 1.2.3.4 192.168.0.1 51234 80\r\nGET /test-proxy-proto HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut out = String::new();
    s.read_to_string(&mut out).await.unwrap();
    assert!(out.ends_with("1.2.3.4"), "{out}");

    // 2. Malformed PROXY header -> 400 Bad Request.
    let mut s = TcpStream::connect(http_addr).await.unwrap();
    s.write_all(b"badPROXY TCP4 1.2.3.4 192.168.0.1 51234 80\r\nGET /test-proxy-proto HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut out = String::new();
    s.read_to_string(&mut out).await.unwrap();
    assert!(out.contains("400 Bad Request"), "{out}");

    // 3. No PROXY header -> direct loopback IP.
    let body = reqwest::get(format!("http://{http_addr}/test-proxy-proto"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert_eq!(body, "127.0.0.1");

    running.shutdown().await;
}

// Upstream: github.com/grafana/dskit/server/server_test.go::TestGrpcOverProxyProtocol
#[tokio::test]
async fn test_grpc_over_proxy_protocol() {
    let mut cfg = auto_server_cfg();
    cfg.proxy_protocol_enabled = true;
    let srv = Server::new(&cfg, &Registry::new(), Arc::new(CaptureLogger::new(LogLevel::Info))).unwrap();
    let grpc_addr = srv.grpc_addr().unwrap();
    let running = srv.start(Router::new(), tonic::service::Routes::new(FakeGrpcSvc)).unwrap();

    let ch = tonic::transport::Endpoint::from_shared(format!("http://{grpc_addr}"))
        .unwrap()
        .connect_with_connector(tower::service_fn(move |_: tonic::transport::Uri| async move {
            let mut s = TcpStream::connect(grpc_addr).await?;
            s.write_all(b"PROXY TCP4 1.2.3.4 192.168.0.1 51234 80\r\n")
                .await?;
            Ok::<_, io::Error>(hyper_util::rt::TokioIo::new(s))
        }))
        .await
        .unwrap();
    let mut client = tonic::client::Grpc::new(ch);
    client.ready().await.unwrap();
    let resp = client
        .unary(
            tonic::Request::new(()),
            "/server.FakeServer/ReturnProxyProtoCallerIP".parse().unwrap(),
            tonic::codec::ProstCodec::<(), ()>::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.metadata().get("x-caller-ip").unwrap().to_str().unwrap(),
        "1.2.3.4"
    );
    drop(client);
    running.shutdown().await;
}

// Upstream: github.com/grafana/dskit/server/server_test.go::TestServer_OpenMetricsTextCreatedSamples
#[tokio::test]
async fn test_server_open_metrics_text_created_samples() {
    for (enabled, accept, want_created) in [
        (false, "application/openmetrics-text; version=1.0.0", false),
        (true, "application/openmetrics-text; version=1.0.0", true),
        (false, "text/plain; version=0.0.4", false),
        (true, "text/plain; version=0.0.4", false),
    ] {
        let mut cfg = MimirConfig::default();
        cfg.server.enable_open_metrics_text_created_samples = enabled;
        cfg.validate().unwrap();
        let reg = Registry::new();
        let c = Counter::new("test_requests_total", "Requests.").unwrap();
        reg.register(Box::new(c.clone())).unwrap();
        c.inc_by(7.0);
        let router = build_http_router(test_http_state(cfg, reg));
        let resp = router
            .oneshot(
                HttpRequest::builder()
                    .uri("/metrics")
                    .header(header::ACCEPT, accept)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = String::from_utf8(resp.into_body().collect().await.unwrap().to_bytes().to_vec()).unwrap();
        assert!(body.contains("test_requests_total 7"), "{body}");
        assert_eq!(body.contains("test_requests_created "), want_created, "{body}");
    }

    let mut cfg = MimirConfig::default();
    cfg.server.register_instrumentation = false;
    cfg.server.enable_open_metrics_text_created_samples = true;
    assert_eq!(
        cfg.validate().unwrap_err().to_string(),
        "error validating config: server.enable-open-metrics-text-created-samples can only be used if server.register-instrumentation is set to true"
    );
}

// Upstream: github.com/grafana/dskit/server/server_test.go::TestPprofCmdlineDisabled
#[tokio::test]
async fn test_pprof_cmdline_disabled() {
    let router = build_http_router(test_http_state(MimirConfig::default(), Registry::new()));
    let resp = router
        .clone()
        .oneshot(HttpRequest::builder().uri("/debug/pprof/cmdline").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    for path in ["/debug/pprof/", "/debug/pprof/symbol"] {
        let resp = router
            .clone()
            .oneshot(HttpRequest::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "path={path}");
    }
}

// ============================================================================
// github.com/prometheus/exporter-toolkit/web/*_test.go (15 tests)
// ============================================================================

// Upstream: github.com/prometheus/exporter-toolkit/web/cache_test.go::TestCacheSize
#[test]
fn test_cache_size() {
    let cache = WebCache::new();
    let mut expected_size = 0usize;
    for i in 0..200 {
        cache.set(format!("foo{i}"), true);
        expected_size += 1;
        if expected_size > 100 {
            expected_size = 90;
        }
        assert_eq!(cache.len(), expected_size, "iter {i}");
    }
}

// Upstream: github.com/prometheus/exporter-toolkit/web/flags_test.go::TestCheckFlags
#[test]
fn test_check_flags() {
    assert_eq!(WebFlagConfig::check_flags(None), Err(WebFlagError::MissingFlag));
    assert_eq!(
        WebFlagConfig::check_flags(Some(&WebFlagConfig {
            web_listen_addresses: Some(vec![":9100".into()]),
            web_systemd_socket: Some(false),
            web_config_file: None,
        })),
        Err(WebFlagError::MissingFlag)
    );
    assert!(WebFlagConfig::check_flags(Some(&WebFlagConfig {
        web_listen_addresses: Some(vec![":9100".into()]),
        web_systemd_socket: None,
        web_config_file: Some(String::new()),
    }))
    .is_ok());
    assert!(WebFlagConfig::check_flags(Some(&WebFlagConfig {
        web_listen_addresses: None,
        web_systemd_socket: Some(true),
        web_config_file: Some(String::new()),
    }))
    .is_ok());
    assert_eq!(
        WebFlagConfig::check_flags(Some(&WebFlagConfig {
            web_listen_addresses: None,
            web_systemd_socket: Some(false),
            web_config_file: Some(String::new()),
        })),
        Err(WebFlagError::NoListeners)
    );
    assert_eq!(
        WebFlagConfig::check_flags(Some(&WebFlagConfig {
            web_listen_addresses: Some(vec![]),
            web_systemd_socket: Some(false),
            web_config_file: Some(String::new()),
        })),
        Err(WebFlagError::NoListeners)
    );
    assert_eq!(
        WebFlagConfig::check_flags(Some(&WebFlagConfig {
            web_listen_addresses: None,
            web_systemd_socket: None,
            web_config_file: Some(String::new()),
        })),
        Err(WebFlagError::NoListeners)
    );
}

// Upstream: github.com/prometheus/exporter-toolkit/web/flags_test.go::TestListenAndServeNoListeners
#[test]
fn test_listen_and_serve_no_listeners() {
    let flags = WebFlagConfig {
        web_listen_addresses: None,
        web_systemd_socket: Some(false),
        web_config_file: Some(String::new()),
    };
    assert_eq!(
        WebFlagConfig::listen_and_serve_validate(Some(&flags)),
        Err(WebFlagError::NoListeners)
    );
}

fn run_web_auth(handler: &WebAuthHandler, basic: Option<(&str, &str)>) -> axum::response::Response {
    let mut headers = axum::http::HeaderMap::new();
    if let Some((u, p)) = basic {
        use std::fmt::Write as _;
        let raw = format!("{u}:{p}");
        const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let bytes = raw.as_bytes();
        let mut enc = String::new();
        for chunk in bytes.chunks(3) {
            let b0 = chunk[0];
            let b1 = chunk.get(1).copied().unwrap_or(0);
            let b2 = chunk.get(2).copied().unwrap_or(0);
            let _ = write!(
                enc,
                "{}{}{}{}",
                B64[(b0 >> 2) as usize] as char,
                B64[(((b0 & 0x03) << 4) | (b1 >> 4)) as usize] as char,
                if chunk.len() > 1 { B64[(((b1 & 0x0f) << 2) | (b2 >> 6)) as usize] as char } else { '=' },
                if chunk.len() > 2 { B64[(b2 & 0x3f) as usize] as char } else { '=' },
            );
        }
        headers.insert(header::AUTHORIZATION, format!("Basic {enc}").parse().unwrap());
    }
    handler.apply_headers_and_check_auth(&headers, axum::response::Response::new(Body::from("Hello World!")))
}

// Upstream: github.com/prometheus/exporter-toolkit/web/handler_test.go::TestBasicAuthCache
#[test]
fn test_basic_auth_cache() {
    let h = Arc::new(WebAuthHandler::new(et_testdata_dir().join("web_config_users_noTLS.good.yml")));
    assert_eq!(run_web_auth(&h, Some(("alice", "alice123"))).status(), StatusCode::OK);
    assert_eq!(run_web_auth(&h, Some(("alice", "alice1234"))).status(), StatusCode::UNAUTHORIZED);

    let mut threads = Vec::new();
    for _ in 0..8 {
        let hc = Arc::clone(&h);
        threads.push(std::thread::spawn(move || {
            for _ in 0..20 {
                assert_eq!(run_web_auth(&hc, Some(("alice", "alice123"))).status(), StatusCode::OK);
                assert_eq!(run_web_auth(&hc, Some(("alice", "alice1234"))).status(), StatusCode::UNAUTHORIZED);
            }
        }));
    }
    for t in threads {
        t.join().unwrap();
    }
}

// Upstream: github.com/prometheus/exporter-toolkit/web/handler_test.go::TestBasicAuthWithFakepassword
#[test]
fn test_basic_auth_with_fakepassword() {
    let h = WebAuthHandler::new(et_testdata_dir().join("web_config_users_noTLS.good.yml"));
    assert_eq!(run_web_auth(&h, Some(("fakeuser", "fakepassword"))).status(), StatusCode::UNAUTHORIZED);
    assert_eq!(run_web_auth(&h, Some(("fakeuser", "fakepassword"))).status(), StatusCode::UNAUTHORIZED);
}

// Upstream: github.com/prometheus/exporter-toolkit/web/handler_test.go::TestByPassBasicAuthVuln
#[test]
fn test_by_pass_basic_auth_vuln() {
    let h = WebAuthHandler::new(et_testdata_dir().join("web_config_users_noTLS.good.yml"));
    assert_eq!(
        run_web_auth(&h, Some(("alice$2y$12$1DpfPeqF9HzHJt.EWswy1exHluGfbhnn3yXhR7Xes6m3WJqFg0Wby", "fakepassword"))).status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        run_web_auth(&h, Some(("alice", "$2y$10$QOauhQNbBCuQDKes6eFzPeMqBSjb7Mr5DUmpZ/VcEd00UAV/LDeSifakepassword"))).status(),
        StatusCode::UNAUTHORIZED
    );
}

// Upstream: github.com/prometheus/exporter-toolkit/web/handler_test.go::TestHTTPHeaders
#[test]
fn test_http_headers() {
    let h = WebAuthHandler::new(et_testdata_dir().join("web_config_headers.good.yml"));
    let resp = run_web_auth(&h, None);
    assert_eq!(resp.status(), StatusCode::OK);
    for (k, v) in [
        ("Strict-Transport-Security", "max-age=31536000; includeSubDomains"),
        ("X-Frame-Options", "deny"),
        ("X-Content-Type-Options", "nosniff"),
        ("X-XSS-Protection", "1"),
    ] {
        assert_eq!(resp.headers().get(k).unwrap().to_str().unwrap(), v);
    }
}

// Upstream: github.com/prometheus/exporter-toolkit/web/tls_config_test.go::TestYAMLFiles
#[test]
fn test_yaml_files() {
    let td = et_testdata_dir();
    let cases = [
        ("somefile", Some("no such file")),
        ("web_config_empty.yml", None),
        ("web_config_junk.yml", Some("yaml")),
        ("web_config_junk_key.yml", Some("yaml")),
        ("web_config_noAuth_certPath_empty.bad.yml", Some("missing one of cert or cert_file")),
        ("web_config_noAuth_cert_empty.bad.yml", Some("missing one of cert or cert_file")),
        ("web_config_noAuth_keyPath_empty.bad.yml", Some("missing one of key or key_file")),
        ("web_config_noAuth_key_empty.bad.yml", Some("missing one of key or key_file")),
        ("web_config_noAuth_certPath_keyPath_empty.bad.yml", Some("missing one of cert or cert_file")),
        ("web_config_noAuth_certPath_invalid.bad.yml", Some("no such file")),
        ("web_config_noAuth_keyPath_invalid.bad.yml", Some("no such file")),
        ("web_config_noAuth_certPath_keyPath_invalid.bad.yml", Some("no such file")),
        ("web_config_noAuth.bad.yml", Some("client CA's have been configured without a Client Auth Policy")),
        ("web_config_auth_clientCAs_invalid.bad.yml", Some("no such file")),
        ("web_config_auth_user_list_invalid.bad.yml", Some("hashedSecret too short")),
        ("web_config_noAuth_inventedCiphers.bad.yml", Some("unknown cipher")),
        ("web_config_noAuth_inventedCurves.bad.yml", Some("unknown curve")),
        ("web_config_noAuth_wrongTLSVersion.bad.yml", Some("unknown TLS version")),
    ];
    for (file, exp) in cases {
        let res = WebConfig::validate(&td.join(file));
        match exp {
            None => assert!(res.is_ok(), "{file}: {:?}", res),
            Some(sub) => {
                let err = res.unwrap_err();
                assert!(err.to_lowercase().contains(&sub.to_lowercase()), "{file}: got {err:?}, expected {sub:?}");
            }
        }
    }
}

async fn start_web_server(
    sc: Arc<rustls::ServerConfig>,
    cfg_path: PathBuf,
) -> (SocketAddr, tokio::sync::watch::Sender<bool>, tokio::task::JoinHandle<()>) {
    let reg = Registry::new();
    let metrics = ServerMetrics::new(&reg, false, false).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let acceptor = mimir_cmd::server::listener::Acceptor::new(
        listener,
        mimir_cmd::server::listener::AcceptOptions {
            conn_limit: 0,
            connections_gauge: metrics.tcp_connections.with_label_values(&["http"]),
            proxy_protocol: false,
            proxy_header_timeout: Duration::ZERO,
        },
    );
    let auth = Arc::new(WebAuthHandler::new(cfg_path));
    let app = Router::new().route(
        "/",
        get(move |req: AxumRequest| {
            let auth = Arc::clone(&auth);
            async move {
                auth.apply_headers_and_check_auth(
                    req.headers(),
                    axum::response::Response::new(Body::from("Hello World!")),
                )
            }
        }),
    );
    let http_cfg = mimir_cmd::server::http_server::HttpServerConfig {
        read_timeout: Duration::from_secs(5),
        read_header_timeout: Duration::from_secs(5),
        write_timeout: Duration::from_secs(5),
        idle_timeout: Duration::from_secs(5),
        tls: Some(sc),
        graceful_shutdown_timeout: Duration::from_secs(1),
        zero_response_logger: None,
    };
    let handle = tokio::spawn(mimir_cmd::server::http_server::serve(acceptor, http_cfg, app, shutdown_rx));
    (addr, shutdown_tx, handle)
}

// Upstream: github.com/prometheus/exporter-toolkit/web/tls_config_test.go::TestServerBehaviour
#[tokio::test]
async fn test_server_behaviour() {
    let td = et_testdata_dir();
    let ca_pem = std::fs::read(td.join("tls-ca-chain.pem")).unwrap();
    let ca_cert = reqwest::Certificate::from_pem(&ca_pem).unwrap();

    // 1. Valid TLS config + TLS client.
    for file in [
        "web_config_noAuth.good.yml",
        "web_config_noAuth_tlsInline.good.yml",
        "web_config_noAuth_allCiphers.good.yml",
        "web_config_noAuth_someCiphers.good.yml",
        "web_config_noAuth_allCurves.good.yml",
        "web_config_noAuth_noHTTP2.good.yml",
    ] {
        let cfg = WebConfig::load(&td.join(file)).unwrap();
        let sc = cfg.build_tls_server_config().unwrap();
        let (addr, tx, h) = start_web_server(sc, td.join(file)).await;
        let client = reqwest::Client::builder()
            .add_root_certificate(ca_cert.clone())
            .build()
            .unwrap();
        let body = client
            .get(format!("https://localhost:{}/", addr.port()))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert_eq!(body, "Hello World!", "file={file}");
        let _ = tx.send(true);
        let _ = h.await;
    }

    // 2. Non-HTTP2 cipher with HTTP/2 enabled fails validation.
    let err = WebConfig::validate(&td.join("web_config_noAuth_noHTTP2Cipher.bad.yml")).unwrap_err();
    assert!(err.contains("TLSConfig.CipherSuites is missing an HTTP/2-required"), "{err}");

    // 3. client_allowed_sans good vs bad.
    let cli2_pem = [
        std::fs::read(td.join("client2_selfsigned.pem")).unwrap(),
        std::fs::read(td.join("client2_selfsigned.key")).unwrap(),
    ]
    .concat();
    let id2 = reqwest::Identity::from_pem(&cli2_pem).unwrap();
    for (file, should_ok) in [
        ("web_config_auth_client_san.good.yaml", true),
        ("web_config_auth_client_san.bad.yaml", false),
    ] {
        let cfg = WebConfig::load(&td.join(file)).unwrap();
        let sc = cfg.build_tls_server_config().unwrap();
        let (addr, tx, h) = start_web_server(sc, td.join(file)).await;
        let client = reqwest::Client::builder()
            .add_root_certificate(ca_cert.clone())
            .identity(id2.clone())
            .build()
            .unwrap();
        let res = client
            .get(format!("https://localhost:{}/", addr.port()))
            .send()
            .await;
        assert_eq!(res.is_ok(), should_ok, "file={file} res={res:?}");
        let _ = tx.send(true);
        let _ = h.await;
    }

    // 4. Rate limiter: nonblocking vs 1s burst.
    let nb = WebAuthHandler::new(td.join("web_config_rate_limiter_nonblocking.yaml"));
    for _ in 0..10 {
        assert_eq!(run_web_auth(&nb, None).status(), StatusCode::OK);
    }
    let rl = WebAuthHandler::new(td.join("web_config_rate_limiter_one_second.yaml"));
    let mut saw_429 = false;
    for _ in 0..10 {
        if run_web_auth(&rl, None).status() == StatusCode::TOO_MANY_REQUESTS {
            saw_429 = true;
            break;
        }
    }
    assert!(saw_429);
}

// Upstream: github.com/prometheus/exporter-toolkit/web/tls_config_test.go::TestConfigReloading
#[tokio::test]
async fn test_config_reloading() {
    let tmp = TempDir::new().unwrap();
    copy_dir_all(&et_testdata_dir(), tmp.path());
    let cert_path = tmp.path().join("server.crt");
    let key_path = tmp.path().join("server.key");
    let dir2 = TempDir::new().unwrap();
    let (ca1, srv_crt1, srv_key1, _, _) = gen_ca_and_certs(dir2.path(), 11);
    let (_ca2, srv_crt2, srv_key2, _, _) = gen_ca_and_certs(dir2.path(), 12);

    std::fs::copy(&srv_crt1, &cert_path).unwrap();
    std::fs::copy(&srv_key1, &key_path).unwrap();

    let cfg = WebConfig::load(&tmp.path().join("web_config_noAuth.good.yml")).unwrap();
    let sc = cfg.build_tls_server_config().unwrap();
    let (addr, tx, h) = start_web_server(sc, tmp.path().join("web_config_noAuth.good.yml")).await;

    let connect_and_get_leaf = |ca: &Path| {
        let ca_bytes = std::fs::read(ca).unwrap();
        async move {
            let mut roots = rustls::RootCertStore::empty();
            for c in CertificateDer::pem_slice_iter(&ca_bytes).flatten() {
                roots.add(c).unwrap();
            }
            let cli_cfg = rustls::ClientConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
            let connector = TlsConnector::from(Arc::new(cli_cfg));
            let tcp = TcpStream::connect(addr).await.unwrap();
            let tls = connector
                .connect(ServerName::try_from("localhost").unwrap(), tcp)
                .await;
            tls.map(|s| s.get_ref().1.peer_certificates().unwrap()[0].as_ref().to_vec())
        }
    };

    let leaf1 = connect_and_get_leaf(&ca1).await.unwrap();
    std::fs::copy(&srv_crt2, &cert_path).unwrap();
    std::fs::copy(&srv_key2, &key_path).unwrap();
    tokio::time::sleep(Duration::from_millis(250)).await;
    let leaf2 = connect_and_get_leaf(&_ca2).await.unwrap();
    assert_ne!(leaf1, leaf2);

    let _ = tx.send(true);
    let _ = h.await;
}

// Upstream: github.com/prometheus/exporter-toolkit/web/tls_config_test.go::TestUsers
#[tokio::test]
async fn test_users() {
    let td = et_testdata_dir();
    let ca_pem = std::fs::read(td.join("tls-ca-chain.pem")).unwrap();
    let ca_cert = reqwest::Certificate::from_pem(&ca_pem).unwrap();
    let cfg = WebConfig::load(&td.join("web_config_users.good.yml")).unwrap();
    let sc = cfg.build_tls_server_config().unwrap();
    let (addr, tx, h) = start_web_server(sc, td.join("web_config_users.good.yml")).await;
    let client = reqwest::Client::builder()
        .add_root_certificate(ca_cert)
        .build()
        .unwrap();
    let url = format!("https://localhost:{}/", addr.port());

    assert_eq!(client.get(&url).send().await.unwrap().status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        client.get(&url).basic_auth("dave", Some("dave123")).send().await.unwrap().status(),
        StatusCode::OK
    );
    assert_eq!(
        client.get(&url).basic_auth("carol", Some("carol123")).send().await.unwrap().status(),
        StatusCode::OK
    );
    assert_eq!(
        client.get(&url).basic_auth("dave", Some("bad")).send().await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        client.get(&url).basic_auth("nonexistent", Some("nonexistent")).send().await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );

    let _ = tx.send(true);
    let _ = h.await;
}

// Upstream: github.com/prometheus/exporter-toolkit/web/tls_config_test.go::TestTLSConfigIsEnabled
#[test]
fn test_tls_config_is_enabled() {
    assert!(!web_tls_is_enabled(&TLSConfig::default()));
    assert!(web_tls_is_enabled(&TLSConfig {
        cert_file: "server.crt".into(),
        key_file: "server.key".into(),
        ..Default::default()
    }));
    assert!(web_tls_is_enabled(&TLSConfig {
        cert: "cert".into(),
        key: "key".into(),
        ..Default::default()
    }));
    assert!(web_tls_is_enabled(&TLSConfig {
        client_ca_file: "client_ca.crt".into(),
        ..Default::default()
    }));
    assert!(web_tls_is_enabled(&TLSConfig {
        client_auth_type: "RequireAndVerifyClientCert".into(),
        ..Default::default()
    }));
}

// Upstream: github.com/prometheus/exporter-toolkit/web/tls_protocols_test.go::TestTLSNextProtosPreservesConfig
#[test]
fn test_tls_next_protos_preserves_config() {
    let original = ["acme-tls/1", "h2", "http/1.1"];
    let got = tls_next_protos(&original, false);
    assert_eq!(got, vec!["acme-tls/1", "http/1.1"]);
    assert_eq!(original, ["acme-tls/1", "h2", "http/1.1"]);
}

// Upstream: github.com/prometheus/exporter-toolkit/web/tls_protocols_test.go::TestServeTLSProtocols
#[tokio::test]
async fn test_serve_tls_protocols() {
    let td = et_testdata_dir();
    let ca_bytes = std::fs::read(td.join("tls-ca-chain.pem")).unwrap();
    for (http2, client_protos, want_alpn) in [
        (true, vec![b"h2".to_vec(), b"http/1.1".to_vec()], b"h2".as_slice()),
        (true, vec![b"http/1.1".to_vec()], b"http/1.1".as_slice()),
        (false, vec![b"h2".to_vec(), b"http/1.1".to_vec()], b"http/1.1".as_slice()),
    ] {
        let dir = TempDir::new().unwrap();
        let cfg_path = dir.path().join("web.yml");
        std::fs::write(
            &cfg_path,
            format!(
                "tls_server_config:
  cert_file: {:?}
  key_file: {:?}
http_server_config:
  http2: {http2}
",
                td.join("server.crt"),
                td.join("server.key")
            ),
        )
        .unwrap();
        let cfg = WebConfig::load(&cfg_path).unwrap();
        let sc = cfg.build_tls_server_config().unwrap();
        let (addr, tx, h) = start_web_server(sc, cfg_path).await;

        let mut roots = rustls::RootCertStore::empty();
        for c in CertificateDer::pem_slice_iter(&ca_bytes).flatten() {
            roots.add(c).unwrap();
        }
        let mut cli_cfg = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
        cli_cfg.alpn_protocols = client_protos;
        let connector = TlsConnector::from(Arc::new(cli_cfg));
        let tcp = TcpStream::connect(addr).await.unwrap();
        let tls = connector
            .connect(ServerName::try_from("localhost").unwrap(), tcp)
            .await
            .unwrap();
        assert_eq!(tls.get_ref().1.alpn_protocol(), Some(want_alpn));

        let _ = tx.send(true);
        let _ = h.await;
    }
}

// Upstream: github.com/prometheus/exporter-toolkit/web/tls_protocols_test.go::TestServeTLSProtocolsAfterReload
#[tokio::test]
async fn test_serve_tls_protocols_after_reload() {
    let dir = TempDir::new().unwrap();
    let (ca1, srv_crt1, srv_key1, cli_crt1, cli_key1) = gen_ca_and_certs(dir.path(), 21);
    let (ca2, srv_crt2, srv_key2, cli_crt2, cli_key2) = gen_ca_and_certs(dir.path(), 22);

    let live_crt = dir.path().join("server.crt");
    let live_key = dir.path().join("server.key");
    let live_ca = dir.path().join("client-ca.crt");
    std::fs::copy(&srv_crt1, &live_crt).unwrap();
    std::fs::copy(&srv_key1, &live_key).unwrap();
    std::fs::copy(&ca1, &live_ca).unwrap();

    let cfg_path = dir.path().join("web.yml");
    std::fs::write(
        &cfg_path,
        "tls_server_config:
  cert_file: server.crt
  key_file: server.key
  client_ca_file: client-ca.crt
  client_auth_type: RequireAndVerifyClientCert
",
    )
    .unwrap();
    let cfg = WebConfig::load(&cfg_path).unwrap();
    let sc = cfg.build_tls_server_config().unwrap();
    let (addr, tx, h) = start_web_server(sc, cfg_path).await;

    let roots_pem = [std::fs::read(&ca1).unwrap(), std::fs::read(&ca2).unwrap()].concat();
    let req_with = |crt: &Path, key: &Path| {
        let client = reqwest::Client::builder()
            .add_root_certificate(reqwest::Certificate::from_pem(&roots_pem).unwrap())
            .identity(
                reqwest::Identity::from_pem(
                    &[std::fs::read(crt).unwrap(), std::fs::read(key).unwrap()].concat(),
                )
                .unwrap(),
            )
            .build()
            .unwrap();
        async move {
            client
                .get(format!("https://localhost:{}/", addr.port()))
                .send()
                .await
        }
    };

    assert_eq!(req_with(&cli_crt1, &cli_key1).await.unwrap().status(), StatusCode::OK);

    std::fs::copy(&srv_crt2, &live_crt).unwrap();
    std::fs::copy(&srv_key2, &live_key).unwrap();
    std::fs::copy(&ca2, &live_ca).unwrap();
    tokio::time::sleep(Duration::from_millis(250)).await;

    assert_eq!(req_with(&cli_crt2, &cli_key2).await.unwrap().status(), StatusCode::OK);
    // Old client cert signed by CA1 must now be rejected after live CA reload.
    assert!(req_with(&cli_crt1, &cli_key1).await.is_err());

    let _ = tx.send(true);
    let _ = h.await;
}
