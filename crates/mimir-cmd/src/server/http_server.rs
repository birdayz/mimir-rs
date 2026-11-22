//! The HTTP server (dskit `Server.HTTPServer`, a Go `net/http.Server`).
//!
//! Ported from:
//! - github.com/grafana/dskit/server/server.go (`http.Server{ReadTimeout, ReadHeaderTimeout,
//!   WriteTimeout, IdleTimeout, TLSConfig}`, `Serve` / `ServeTLS`, `Shutdown` bounded by
//!   `-server.graceful-shutdown-timeout`)
//! - Go net/http server connection semantics:
//!   * plaintext connections speak HTTP/1.x only; TLS connections negotiate h2 / http/1.1 via ALPN
//!     (Go enables HTTP/2 automatically on TLS servers; its HTTP/2 server allows 250 concurrent
//!     streams);
//!   * `ReadHeaderTimeout` (falls back to `ReadTimeout` when 0) bounds reading request headers,
//!     starting at the first byte of the request (at connection start for the first request);
//!   * `IdleTimeout` (falls back to `ReadTimeout` when 0) bounds the wait for the next request on
//!     a keep-alive connection;
//!   * `ReadTimeout` bounds reading the whole request including the body;
//!   * `WriteTimeout` bounds writing the response, measured from the end of the request headers;
//!   * an expired deadline closes the connection.

use std::future::Future as _;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use axum::Router;
use mimir_common::logging::{LogLevel, Logger};
use hyper::body::Incoming;
use hyper_util::rt::{TokioExecutor, TokioIo};
use parking_lot::Mutex;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::watch;
use tokio::task::JoinSet;
use tower::ServiceExt;

use super::body::TrackedBody;
use super::listener::{finish_accept, is_fatal_accept_error, Acceptor, PrefixedIo};

/// Marker inserted into requests served by the HTTP server (as opposed to the httpgrpc bridge,
/// which dskit serves without the HTTP middleware chain).
#[derive(Clone, Copy, Debug)]
pub struct ServedOverHttp {
    pub remote_addr: SocketAddr,
}

/// Shared per-request write deadline handle mirroring Go's `http.ResponseController.SetWriteDeadline`.
#[derive(Clone)]
pub struct SharedWriteDeadline {
    deadline: Arc<Mutex<Option<Instant>>>,
    base_write_timeout: Duration,
}

impl SharedWriteDeadline {
    pub fn new(base_write_timeout: Duration) -> Self {
        let initial = (!base_write_timeout.is_zero())
            .then(|| Instant::now().checked_add(base_write_timeout))
            .flatten();
        Self {
            deadline: Arc::new(Mutex::new(initial)),
            base_write_timeout,
        }
    }

    pub fn extend_by(&self, extra: Duration) {
        if !self.base_write_timeout.is_zero() {
            // Saturate to a finite future deadline so oversized extensions never clear the write
            // deadline to `None`.
            const MAX_WRITE_DEADLINE_OFFSET: Duration = Duration::from_secs(86_400 * 365);
            let bounded = self
                .base_write_timeout
                .saturating_add(extra)
                .min(MAX_WRITE_DEADLINE_OFFSET);
            let mut g = self.deadline.lock();
            *g = Instant::now()
                .checked_add(bounded)
                .or_else(|| Instant::now().checked_add(self.base_write_timeout));
        }
    }

    pub fn disable(&self) {
        *self.deadline.lock() = None;
    }

    pub fn current(&self) -> Option<Instant> {
        *self.deadline.lock()
    }
}

#[derive(Clone)]
pub struct HttpServerConfig {
    pub read_timeout: Duration,
    pub read_header_timeout: Duration,
    pub write_timeout: Duration,
    pub idle_timeout: Duration,
    pub tls: Option<Arc<rustls::ServerConfig>>,
    pub graceful_shutdown_timeout: Duration,
    /// `-server.http-log-closed-connections-without-response-enabled` (dskit
    /// `middleware.NewZeroResponseListener`, logging at warn).
    pub zero_response_logger: Option<Arc<dyn Logger>>,
}

impl HttpServerConfig {
    fn header_timeout(&self) -> Duration {
        if self.read_header_timeout.is_zero() {
            self.read_timeout
        } else {
            self.read_header_timeout
        }
    }

    fn idle_timeout(&self) -> Duration {
        if self.idle_timeout.is_zero() {
            self.read_timeout
        } else {
            self.idle_timeout
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    /// Reading request headers since the given instant.
    Header,
    /// Waiting for the next request since the given instant.
    Idle,
    /// At least one request in flight.
    Active,
}

struct ConnState {
    inflight: AtomicUsize,
    phase: Mutex<(Phase, Instant)>,
    http2: bool,
}

impl ConnState {
    fn request_started(&self) -> Instant {
        self.inflight.fetch_add(1, Ordering::AcqRel);
        let mut p = self.phase.lock();
        let header_start = if p.0 == Phase::Header { p.1 } else { Instant::now() };
        *p = (Phase::Active, Instant::now());
        header_start
    }

    fn request_finished(&self) {
        if self.inflight.fetch_sub(1, Ordering::AcqRel) == 1 {
            *self.phase.lock() = (Phase::Idle, Instant::now());
        }
    }
}

struct RequestInflightGuard {
    state: Option<Arc<ConnState>>,
}

impl RequestInflightGuard {
    fn start(state: Arc<ConnState>) -> (Self, Instant) {
        let started = state.request_started();
        (Self { state: Some(state) }, started)
    }

    fn disarm(&mut self) {
        self.state = None;
    }
}

impl Drop for RequestInflightGuard {
    fn drop(&mut self) {
        if let Some(state) = self.state.take() {
            state.request_finished();
        }
    }
}

#[doc(hidden)]
pub fn probe_request_inflight_guard_drop_for_test() -> (usize, usize) {
    let state = Arc::new(ConnState {
        inflight: AtomicUsize::new(0),
        phase: Mutex::new((Phase::Header, Instant::now())),
        http2: true,
    });
    let (guard, _) = RequestInflightGuard::start(Arc::clone(&state));
    let during = state.inflight.load(Ordering::Acquire);
    drop(guard);
    let after = state.inflight.load(Ordering::Acquire);
    (during, after)
}

/// dskit `zeroResponseConn`: remembers the first bytes read from a connection until anything is
/// written to it, and logs them when the connection is closed after a read timeout without any
/// response.
struct ZeroResponse {
    logger: Arc<dyn Logger>,
    remote: SocketAddr,
    /// First `REQUEST_BUF_SIZE` bytes read; `None` once data was written to the connection.
    buf: Option<Vec<u8>>,
    last_read_timed_out: bool,
}

/// dskit `requestBufSize`.
const REQUEST_BUF_SIZE: usize = 512;

impl ZeroResponse {
    fn on_read(&mut self, data: &[u8]) {
        self.last_read_timed_out = false;
        if let Some(buf) = self.buf.as_mut() {
            let rem = REQUEST_BUF_SIZE.saturating_sub(buf.len()).min(data.len());
            buf.extend_from_slice(&data[..rem]);
        }
    }
}

impl Drop for ZeroResponse {
    fn drop(&mut self) {
        let Some(buf) = self.buf.take() else { return };
        if !self.last_read_timed_out {
            return;
        }
        static AUTH: std::sync::OnceLock<Option<regex::bytes::Regex>> = std::sync::OnceLock::new();
        let redacted = match AUTH
            .get_or_init(|| regex::bytes::Regex::new(r"((?i)\r\nauthorization:\s+)(\S+\s+)(\S+)").ok())
        {
            Some(re) => re.replace_all(&buf, &b"${1}${2}***"[..]).into_owned(),
            None => buf,
        };
        let read = go_quote(&redacted);
        let remote = self.remote.to_string();
        self.logger.log(
            LogLevel::Warn,
            &[
                ("msg", "read timeout, connection closed with no response"),
                ("read", &read),
                ("remote", &remote),
            ],
        );
    }
}

/// Go `strconv.IsPrint` for the categories that occur in request bytes: controls (Cc), format
/// characters (Cf) and every space except U+0020 are escaped.
fn go_is_print(c: char) -> bool {
    if c == ' ' {
        return true;
    }
    if c.is_control() || c.is_whitespace() {
        return false;
    }
    !matches!(
        c,
        '\u{ad}'
            | '\u{600}'..='\u{605}'
            | '\u{61c}'
            | '\u{6dd}'
            | '\u{70f}'
            | '\u{180e}'
            | '\u{200b}'..='\u{200f}'
            | '\u{202a}'..='\u{202e}'
            | '\u{2060}'..='\u{2064}'
            | '\u{2066}'..='\u{206f}'
            | '\u{feff}'
            | '\u{fff9}'..='\u{fffb}'
    )
}

/// Go `strconv.Quote` of a byte string (invalid UTF-8 bytes as `\xNN`).
fn go_quote(b: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(b.len() + 2);
    out.push('"');
    for chunk in b.utf8_chunks() {
        for c in chunk.valid().chars() {
            match c {
                '\x07' => out.push_str("\\a"),
                '\x08' => out.push_str("\\b"),
                '\x0c' => out.push_str("\\f"),
                '\n' => out.push_str("\\n"),
                '\r' => out.push_str("\\r"),
                '\t' => out.push_str("\\t"),
                '\x0b' => out.push_str("\\v"),
                '\\' => out.push_str("\\\\"),
                '"' => out.push_str("\\\""),
                c if (c as u32) < 0x20 || c == '\x7f' => {
                    let _ = write!(out, "\\x{:02x}", c as u32);
                }
                c if !go_is_print(c) => {
                    if (c as u32) < 0x10000 {
                        let _ = write!(out, "\\u{:04x}", c as u32);
                    } else {
                        let _ = write!(out, "\\U{:08x}", c as u32);
                    }
                }
                c => out.push(c),
            }
        }
        for byte in chunk.invalid() {
            let _ = write!(out, "\\x{byte:02x}");
        }
    }
    out.push('"');
    out
}

/// Connection IO enforcing the header-read and idle deadlines.
struct TimedIo<T> {
    inner: T,
    state: Arc<ConnState>,
    header_timeout: Duration,
    idle_timeout: Duration,
    sleep: Option<(Instant, Pin<Box<tokio::time::Sleep>>)>,
    zero_response: Option<ZeroResponse>,
}

impl<T> TimedIo<T> {
    fn timed_out(&mut self) -> Poll<std::io::Result<()>> {
        if let Some(z) = self.zero_response.as_mut() {
            z.last_read_timed_out = true;
        }
        Poll::Ready(Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "i/o timeout")))
    }

    fn wrote(&mut self, res: &Poll<std::io::Result<usize>>) {
        if let (Some(z), Poll::Ready(Ok(n))) = (self.zero_response.as_mut(), res) {
            if *n > 0 {
                z.buf = None;
            }
        }
    }
}

impl<T: AsyncRead + Unpin> AsyncRead for TimedIo<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = &mut *self;
        let before = buf.filled().len();
        match Pin::new(&mut this.inner).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                if let Some(z) = this.zero_response.as_mut() {
                    z.on_read(&buf.filled()[before..]);
                }
                if buf.filled().len() > before && !this.state.http2 {
                    let mut p = this.state.phase.lock();
                    if p.0 == Phase::Idle {
                        *p = (Phase::Header, Instant::now());
                    }
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => {
                if let Some(z) = this.zero_response.as_mut() {
                    z.last_read_timed_out = false;
                }
                Poll::Ready(Err(e))
            }
            Poll::Pending => {
                let (phase, since) = *this.state.phase.lock();
                let timeout = match phase {
                    Phase::Header => this.header_timeout,
                    Phase::Idle => this.idle_timeout,
                    Phase::Active => Duration::ZERO,
                };
                if timeout.is_zero() {
                    this.sleep = None;
                    return Poll::Pending;
                }
                let Some(deadline) = since.checked_add(timeout) else {
                    this.sleep = None;
                    return Poll::Pending;
                };
                match &mut this.sleep {
                    Some((d, s)) if *d == deadline => {
                        if s.as_mut().poll(cx).is_ready() {
                            return this.timed_out();
                        }
                    }
                    _ => {
                        let mut s = Box::pin(tokio::time::sleep_until(deadline.into()));
                        if s.as_mut().poll(cx).is_ready() {
                            return this.timed_out();
                        }
                        this.sleep = Some((deadline, s));
                    }
                }
                Poll::Pending
            }
        }
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for TimedIo<T> {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
        let res = Pin::new(&mut self.inner).poll_write(cx, buf);
        self.wrote(&res);
        res
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        let res = Pin::new(&mut self.inner).poll_write_vectored(cx, bufs);
        self.wrote(&res);
        res
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}

/// Serves `router` on `acceptor` until `shutdown` flips to true, then shuts down gracefully
/// within `graceful_shutdown_timeout` (Go `http.Server.Shutdown(ctx)`).
pub async fn serve(acceptor: Acceptor, cfg: HttpServerConfig, router: Router, mut shutdown: watch::Receiver<bool>) {
    let acceptor = Arc::new(acceptor);
    let mut conns: JoinSet<()> = JoinSet::new();
    let mut accept_backoff = Duration::from_millis(5);
    loop {
        while conns.try_join_next().is_some() {}
        tokio::select! {
            biased;
            _ = super::wait_shutdown(&mut shutdown) => break,
            res = acceptor.accept() => {
                let (stream, addr, guard) = match res {
                    Ok(v) => {
                        accept_backoff = Duration::from_millis(5);
                        v
                    }
                    Err(err) => {
                        tracing::error!(err = %err, "http server accept error");
                        if is_fatal_accept_error(&err) {
                            break;
                        }
                        tokio::select! {
                            _ = super::wait_shutdown(&mut shutdown) => break,
                            _ = tokio::time::sleep(accept_backoff) => {}
                        }
                        accept_backoff = (accept_backoff * 2).min(Duration::from_secs(1));
                        continue;
                    }
                };
                let acc = Arc::clone(&acceptor);
                let cfg = cfg.clone();
                let router = router.clone();
                let shutdown = shutdown.clone();
                conns.spawn(async move {
                    let Ok(accepted) = finish_accept(stream, addr, guard, acc.options()).await else { return };
                    let remote = accepted.remote_addr;
                    let _guard = accepted.guard;
                    let io = PrefixedIo::new(accepted.prefix, accepted.stream);
                    if let Some(tls) = cfg.tls.clone() {
                        let hs = tokio_rustls::TlsAcceptor::from(tls).accept(io);
                        let tls_stream = match tokio::time::timeout(handshake_timeout(&cfg), hs).await {
                            Ok(Ok(s)) => s,
                            _ => return,
                        };
                        let h2 = tls_stream.get_ref().1.alpn_protocol() == Some(b"h2");
                        serve_conn(tls_stream, h2, remote, cfg, router, shutdown).await;
                    } else {
                        serve_conn(io, false, remote, cfg, router, shutdown).await;
                    }
                });
            }
            Some(_) = conns.join_next(), if !conns.is_empty() => {}
        }
    }
    // Stop accepting (listener dropped with the acceptor once all tasks finish), let connections
    // finish their in-flight requests, and abort what is left after the graceful timeout.
    let drain = async { while conns.join_next().await.is_some() {} };
    if tokio::time::timeout(cfg.graceful_shutdown_timeout, drain).await.is_err() {
        conns.abort_all();
    }
}

async fn run_with_dynamic_deadline<F: std::future::Future>(
    fut: F,
    deadline: &SharedWriteDeadline,
) -> Result<F::Output, ()> {
    tokio::pin!(fut);
    loop {
        let Some(cur) = deadline.current() else {
            return Ok(fut.await);
        };
        tokio::select! {
            biased;
            res = &mut fut => return Ok(res),
            _ = tokio::time::sleep_until(cur.into()) => {
                // Re-check in case a handler (e.g. /debug/pprof/profile) extended the deadline.
                if let Some(latest) = deadline.current() {
                    if latest > cur {
                        continue;
                    }
                } else {
                    return Ok(fut.await);
                }
                return Err(());
            }
        }
    }
}

fn handshake_timeout(cfg: &HttpServerConfig) -> Duration {
    // Go applies the read deadline (header timeout) to the TLS handshake as well.
    let t = cfg.header_timeout();
    if t.is_zero() {
        Duration::from_secs(3600)
    } else {
        t
    }
}

async fn serve_conn<T>(
    io: T,
    h2: bool,
    remote: SocketAddr,
    cfg: HttpServerConfig,
    router: Router,
    mut shutdown: watch::Receiver<bool>,
) where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let state = Arc::new(ConnState {
        inflight: AtomicUsize::new(0),
        phase: Mutex::new((if h2 { Phase::Idle } else { Phase::Header }, Instant::now())),
        http2: h2,
    });
    let io = TokioIo::new(TimedIo {
        inner: io,
        state: Arc::clone(&state),
        header_timeout: cfg.header_timeout(),
        idle_timeout: cfg.idle_timeout(),
        sleep: None,
        zero_response: cfg.zero_response_logger.clone().map(|logger| ZeroResponse {
            logger,
            remote,
            buf: Some(Vec::with_capacity(REQUEST_BUF_SIZE)),
            last_read_timed_out: false,
        }),
    });
    let read_timeout = cfg.read_timeout;
    let write_timeout = cfg.write_timeout;
    let svc = hyper::service::service_fn(move |mut req: hyper::Request<Incoming>| {
        let state = Arc::clone(&state);
        let router = router.clone();
        async move {
            let (mut inflight_guard, header_start) =
                RequestInflightGuard::start(Arc::clone(&state));
            let read_deadline = (!read_timeout.is_zero())
                .then(|| header_start.checked_add(read_timeout))
                .flatten();
            let initial_write_deadline = (!write_timeout.is_zero())
                .then(|| Instant::now().checked_add(write_timeout))
                .flatten();
            let shared_write_deadline = SharedWriteDeadline {
                deadline: Arc::new(Mutex::new(initial_write_deadline)),
                base_write_timeout: write_timeout,
            };
            req.extensions_mut().insert(ServedOverHttp { remote_addr: remote });
            req.extensions_mut().insert(shared_write_deadline.clone());
            req.extensions_mut()
                .insert(axum::extract::ConnectInfo(remote));
            let req = req.map(|b| {
                axum::body::Body::new(TrackedBody::new(b).deadline(read_deadline, "http: request body read timeout"))
            });
            let fut = router.oneshot(req);
            let resp = match run_with_dynamic_deadline(fut, &shared_write_deadline).await {
                Ok(r) => r,
                Err(_) => {
                    return Err::<hyper::Response<axum::body::Body>, std::io::Error>(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "http: write timeout",
                    ));
                }
            };
            let mut resp: hyper::Response<axum::body::Body> = match resp {
                Ok(r) => r,
                Err(e) => match e {},
            };
            if resp
                .extensions()
                .get::<crate::server::http_middleware::MethodMismatch>()
                .is_some()
            {
                resp.headers_mut().remove(http::header::ALLOW);
            }
            let final_write_deadline = shared_write_deadline.current();
            let st = Arc::clone(&state);
            let resp = resp.map(|b| {
                axum::body::Body::new(
                    TrackedBody::new(b)
                        .deadline(final_write_deadline, "http: write timeout")
                        .on_done(Box::new(move |_| st.request_finished())),
                )
            });
            inflight_guard.disarm();
            Ok(resp)
        }
    });

    if h2 {
        let mut b = hyper::server::conn::http2::Builder::new(TokioExecutor::new());
        b.max_concurrent_streams(250);
        let conn = b.serve_connection(io, svc);
        tokio::pin!(conn);
        tokio::select! {
            _ = conn.as_mut() => {}
            _ = super::wait_shutdown(&mut shutdown) => {
                conn.as_mut().graceful_shutdown();
                let _ = conn.await;
            }
        }
    } else {
        let conn = hyper::server::conn::http1::Builder::new()
            .keep_alive(true)
            .serve_connection(io, svc)
            .with_upgrades();
        tokio::pin!(conn);
        tokio::select! {
            _ = conn.as_mut() => {}
            _ = super::wait_shutdown(&mut shutdown) => {
                conn.as_mut().graceful_shutdown();
                let _ = conn.await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn go_quote_matches_strconv_quote() {
        assert_eq!(go_quote(b"GET / HTTP/1.1\r\nHost: x\r\n"), r#""GET / HTTP/1.1\r\nHost: x\r\n""#);
        assert_eq!(go_quote(b"a\"b\\c\x00\x7f\xff"), r#""a\"b\\c\x00\x7f\xff""#);
        assert_eq!(go_quote("ü\u{200b}".as_bytes()), "\"ü\\u200b\"");
    }
}
