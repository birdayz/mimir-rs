//! Ported upstream tests from github.com/pires/go-proxyproto:
//! - protocol_test.go (first half: 31 tests)

use std::any::Any;
use std::collections::VecDeque;
use std::io::{self, Cursor, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
#[cfg(unix)]
use std::os::unix::net::{UnixListener, UnixStream};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread;
use std::time::{Duration, Instant, SystemTime};

use mimir_cmd::server::proxyproto::{
    default_read_header_timeout, set_default_read_header_timeout, set_read_header_timeout_nanos,
    with_buffer_size, with_policy, AcceptedConn, AddressFamilyAndProtocol, ConnPolicyOptions,
    Header, ListenerHeaderTimeout, Policy, ProtocolVersionAndCommand, ProxyAddr, ProxyBufReader,
    ProxyConn, ProxyError, ProxyListener, StreamConn,
};

static GLOBAL_RW: RwLock<()> = RwLock::new(());

const TEST_SOURCE_IPV4_ADDR: &str = "10.1.1.1";
const TEST_DEST_IPV4_ADDR: &str = "20.2.2.2";

fn test_tcp_v4_header() -> Header {
    Header {
        version: 2,
        command: ProtocolVersionAndCommand::PROXY,
        transport_protocol: AddressFamilyAndProtocol::TCPV4,
        source_addr: Some(ProxyAddr::tcp(TEST_SOURCE_IPV4_ADDR.parse().unwrap(), 1000)),
        destination_addr: Some(ProxyAddr::tcp(TEST_DEST_IPV4_ADDR.parse().unwrap(), 2000)),
        raw_tlvs: Vec::new(),
    }
}

#[derive(Default)]
struct HalfState {
    buf: VecDeque<u8>,
    closed: bool,
}

struct PipeInner {
    a_to_b: Mutex<HalfState>,
    b_to_a: Mutex<HalfState>,
    a_cv: Condvar,
    b_cv: Condvar,
}

struct PipeConn {
    inner: Arc<PipeInner>,
    is_a: bool,
    read_deadline: Option<SystemTime>,
    write_deadline: Option<SystemTime>,
    closed: bool,
}

fn net_pipe() -> (PipeConn, PipeConn) {
    let inner = Arc::new(PipeInner {
        a_to_b: Mutex::new(HalfState::default()),
        b_to_a: Mutex::new(HalfState::default()),
        a_cv: Condvar::new(),
        b_cv: Condvar::new(),
    });
    (
        PipeConn { inner: Arc::clone(&inner), is_a: true, read_deadline: None, write_deadline: None, closed: false },
        PipeConn { inner, is_a: false, read_deadline: None, write_deadline: None, closed: false },
    )
}

impl Read for PipeConn {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        let (mtx, cv) = if self.is_a { (&self.inner.b_to_a, &self.inner.b_cv) } else { (&self.inner.a_to_b, &self.inner.a_cv) };
        let mut guard = mtx.lock().unwrap();
        loop {
            if !guard.buf.is_empty() {
                let n = out.len().min(guard.buf.len());
                for b in out[..n].iter_mut() {
                    *b = guard.buf.pop_front().unwrap_or(0);
                }
                return Ok(n);
            }
            if guard.closed || self.closed {
                return Ok(0);
            }
            if let Some(dl) = self.read_deadline {
                let Ok(dur) = dl.duration_since(SystemTime::now()) else {
                    return Err(io::Error::new(io::ErrorKind::TimedOut, "i/o timeout"));
                };
                let (g, wait_res) = cv.wait_timeout(guard, dur).unwrap();
                guard = g;
                if wait_res.timed_out() && guard.buf.is_empty() && !guard.closed {
                    return Err(io::Error::new(io::ErrorKind::TimedOut, "i/o timeout"));
                }
            } else {
                guard = cv.wait(guard).unwrap();
            }
        }
    }
}

impl Write for PipeConn {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.closed { return Err(io::Error::new(io::ErrorKind::BrokenPipe, "closed pipe")); }
        let (mtx, cv) = if self.is_a { (&self.inner.a_to_b, &self.inner.a_cv) } else { (&self.inner.b_to_a, &self.inner.b_cv) };
        let mut guard = mtx.lock().unwrap();
        if guard.closed { return Err(io::Error::new(io::ErrorKind::BrokenPipe, "closed pipe")); }
        guard.buf.extend(buf);
        cv.notify_all();
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> { Ok(()) }
}

impl StreamConn for PipeConn {
    fn local_addr(&self) -> io::Result<ProxyAddr> { Ok(ProxyAddr::other("pipe", "pipe")) }
    fn remote_addr(&self) -> io::Result<ProxyAddr> { Ok(ProxyAddr::other("pipe", "pipe")) }
    fn close(&mut self) -> io::Result<()> {
        self.closed = true;
        { let mut g = self.inner.a_to_b.lock().unwrap(); g.closed = true; self.inner.a_cv.notify_all(); }
        { let mut g = self.inner.b_to_a.lock().unwrap(); g.closed = true; self.inner.b_cv.notify_all(); }
        Ok(())
    }
    fn set_deadline(&mut self, t: Option<SystemTime>) -> io::Result<()> {
        self.set_read_deadline(t)?;
        self.set_write_deadline(t)
    }
    fn set_read_deadline(&mut self, t: Option<SystemTime>) -> io::Result<()> {
        if self.closed { return Err(io::Error::new(io::ErrorKind::BrokenPipe, "closed")); }
        self.read_deadline = t;
        Ok(())
    }
    fn set_write_deadline(&mut self, t: Option<SystemTime>) -> io::Result<()> {
        if self.closed { return Err(io::Error::new(io::ErrorKind::BrokenPipe, "closed")); }
        self.write_deadline = t;
        Ok(())
    }
    fn as_any(&self) -> &dyn Any { self }
    fn as_any_mut(&mut self) -> &mut dyn Any { self }
}

struct DeadlineFailConn {
    r: Cursor<Vec<u8>>,
    set_calls: usize,
    fail_on_set: usize,
}
impl Read for DeadlineFailConn {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> { self.r.read(buf) }
}
impl Write for DeadlineFailConn {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> { Ok(buf.len()) }
    fn flush(&mut self) -> io::Result<()> { Ok(()) }
}
impl StreamConn for DeadlineFailConn {
    fn local_addr(&self) -> io::Result<ProxyAddr> { Ok(ProxyAddr::other("dummy", "local")) }
    fn remote_addr(&self) -> io::Result<ProxyAddr> { Ok(ProxyAddr::other("dummy", "remote")) }
    fn close(&mut self) -> io::Result<()> { Ok(()) }
    fn set_deadline(&mut self, _: Option<SystemTime>) -> io::Result<()> { Ok(()) }
    fn set_read_deadline(&mut self, _: Option<SystemTime>) -> io::Result<()> {
        self.set_calls += 1;
        if self.fail_on_set != 0 && self.set_calls == self.fail_on_set {
            return Err(io::Error::other("set read deadline failed"));
        }
        Ok(())
    }
    fn set_write_deadline(&mut self, _: Option<SystemTime>) -> io::Result<()> { Ok(()) }
    fn as_any(&self) -> &dyn Any { self }
    fn as_any_mut(&mut self) -> &mut dyn Any { self }
}

fn use_policy() -> mimir_cmd::server::proxyproto::ConnPolicyFunc {
    Arc::new(|_: &ConnPolicyOptions| Ok(Policy::Use))
}

fn spawn_tcp_client(addr: SocketAddr, header: Option<Header>, payload: Option<&'static [u8]>, expect_echo: Option<&'static [u8]>, close_after: bool, connect_only: bool) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let mut conn = TcpStream::connect(addr).unwrap();
        if connect_only {
            thread::sleep(Duration::from_millis(600));
            return;
        }
        if let Some(h) = header {
            h.write_to(&mut conn).unwrap();
        }
        if let Some(p) = payload {
            conn.write_all(p).unwrap();
        }
        if let Some(echo) = expect_echo {
            let mut recv = vec![0u8; echo.len()];
            conn.read_exact(&mut recv).unwrap();
            assert_eq!(recv.as_slice(), echo);
        }
        if close_after {
            drop(conn);
        } else {
            thread::sleep(Duration::from_millis(200));
        }
    })
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::TestPassthrough
#[test]
fn test_passthrough() {
    let _g = GLOBAL_RW.read().unwrap();
    let mut pl = ProxyListener::new(TcpListener::bind("127.0.0.1:0").unwrap());
    pl.conn_policy = Some(use_policy());
    let addr = pl.listener.local_addr().unwrap();
    let cli = spawn_tcp_client(addr, None, Some(b"ping"), Some(b"pong"), false, false);
    let mut conn = pl.accept().unwrap();
    let mut recv = [0u8; 4];
    conn.read_exact(&mut recv).unwrap();
    assert_eq!(&recv, b"ping");
    conn.write_all(b"pong").unwrap();
    cli.join().unwrap();
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::TestRequiredWithReadHeaderTimeout
#[test]
fn test_required_with_read_header_timeout() {
    let _g = GLOBAL_RW.read().unwrap();
    for ms in [80u64, 150, 250] {
        let start = Instant::now();
        let mut pl = ProxyListener::new(TcpListener::bind("127.0.0.1:0").unwrap());
        let timeout = Duration::from_millis(ms);
        pl.read_header_timeout = ListenerHeaderTimeout::Duration(timeout);
        pl.policy = Some(Arc::new(|_| Ok(Policy::Require)));
        let addr = pl.listener.local_addr().unwrap();
        let cli = spawn_tcp_client(addr, None, None, None, false, true);
        let mut conn = pl.accept().unwrap().into_proxy().unwrap();
        let err = conn.read_conn(&mut [0u8; 4]).unwrap_err();
        assert!(err.is_no_proxy_protocol(), "got {err:?}");
        assert!(start.elapsed() >= timeout, "elapsed {:?} < {:?}", start.elapsed(), timeout);
        cli.join().unwrap();
    }
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::TestUseWithReadHeaderTimeout
#[test]
fn test_use_with_read_header_timeout() {
    let _g = GLOBAL_RW.read().unwrap();
    for ms in [80u64, 150, 250] {
        let start = Instant::now();
        let mut pl = ProxyListener::new(TcpListener::bind("127.0.0.1:0").unwrap());
        let timeout = Duration::from_millis(ms);
        pl.read_header_timeout = ListenerHeaderTimeout::Duration(timeout);
        pl.policy = Some(Arc::new(|_| Ok(Policy::Use)));
        let addr = pl.listener.local_addr().unwrap();
        let cli = spawn_tcp_client(addr, None, None, None, false, true);
        let mut conn = pl.accept().unwrap().into_proxy().unwrap();
        conn.set_deadline(Some(SystemTime::now() + timeout * 2)).unwrap();
        let err = conn.read_conn(&mut [0u8; 4]).unwrap_err();
        assert!(err.is_timeout(), "expected timeout, got {err:?}");
        assert!(start.elapsed() >= timeout);
        cli.join().unwrap();
    }
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::TestNewConnSetReadHeaderTimeoutOption
#[test]
fn test_new_conn_set_read_header_timeout_option() {
    let _g = GLOBAL_RW.read().unwrap();
    let (conn, _peer) = net_pipe();
    let timeout = Duration::from_millis(150);
    let pc = ProxyConn::new(conn, vec![set_read_header_timeout_nanos(timeout.as_nanos() as i64)]);
    assert_eq!(pc.read_header_timeout(), timeout);
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::TestNewConnSetReadHeaderTimeoutIgnoresNegative
#[test]
fn test_new_conn_set_read_header_timeout_ignores_negative() {
    let _g = GLOBAL_RW.read().unwrap();
    let (conn, _peer) = net_pipe();
    let pc = ProxyConn::new(conn, vec![set_read_header_timeout_nanos(-1)]);
    assert_eq!(pc.read_header_timeout(), default_read_header_timeout());
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::TestNewConnAppliesDefaultReadHeaderTimeout
#[test]
fn test_new_conn_applies_default_read_header_timeout() {
    let _g = GLOBAL_RW.read().unwrap();
    let (conn, _peer) = net_pipe();
    let pc = ProxyConn::new(conn, vec![]);
    assert_eq!(pc.read_header_timeout(), default_read_header_timeout());
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::TestNewConnSetReadHeaderTimeoutZeroDisables
#[test]
fn test_new_conn_set_read_header_timeout_zero_disables() {
    let _g = GLOBAL_RW.read().unwrap();
    let (conn, _peer) = net_pipe();
    let pc = ProxyConn::new(conn, vec![set_read_header_timeout_nanos(0)]);
    assert_eq!(pc.read_header_timeout(), Duration::ZERO);
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::TestReadHeaderPreservesHeaderWhenDeadlineRestoreFails
#[test]
fn test_read_header_preserves_header_when_deadline_restore_fails() {
    let _g = GLOBAL_RW.read().unwrap();
    let data = b"PROXY TCP4 10.1.1.1 20.2.2.2 1000 2000\r\nping".to_vec();
    let inner = DeadlineFailConn { r: Cursor::new(data), set_calls: 0, fail_on_set: 2 };
    let mut pc = ProxyConn::new(inner, vec![]);
    let hdr = pc.proxy_header().unwrap().clone();
    assert_eq!(hdr.source_addr.unwrap().to_string(), "10.1.1.1:1000");
    assert_eq!(pc.raw().unwrap().set_calls, 2);
    let mut buf = [0u8; 4];
    let n = pc.read(&mut buf).unwrap();
    assert_eq!(&buf[..n], b"ping");
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::TestNewConnDefaultTimeoutBoundsHeaderDetection
#[test]
fn test_new_conn_default_timeout_bounds_header_detection() {
    let _g = GLOBAL_RW.write().unwrap();
    struct Reset(Duration);
    impl Drop for Reset { fn drop(&mut self) { set_default_read_header_timeout(self.0); } }
    let _r = Reset(default_read_header_timeout());
    set_default_read_header_timeout(Duration::from_millis(100));

    let (conn, _peer) = net_pipe();
    let mut pc = ProxyConn::new(conn, vec![]);
    let start = Instant::now();
    assert!(pc.proxy_header().is_none());
    assert!(start.elapsed() >= Duration::from_millis(50));
    assert!(start.elapsed() < Duration::from_secs(2));
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::TestNewConnRequirePolicyTimeoutUnblocksRead
#[test]
fn test_new_conn_require_policy_timeout_unblocks_read() {
    let _g = GLOBAL_RW.write().unwrap();
    struct Reset(Duration);
    impl Drop for Reset { fn drop(&mut self) { set_default_read_header_timeout(self.0); } }
    let _r = Reset(default_read_header_timeout());
    set_default_read_header_timeout(Duration::from_millis(100));

    let (conn, _peer) = net_pipe();
    let mut pc = ProxyConn::new(conn, vec![with_policy(Policy::Require)]);
    let start = Instant::now();
    let err = pc.read_conn(&mut [0u8; 1]).unwrap_err();
    assert!(err.is_no_proxy_protocol(), "got {err:?}");
    assert!(start.elapsed() >= Duration::from_millis(50));
    assert!(start.elapsed() < Duration::from_secs(2));
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::TestAcceptConcurrentDoesNotRaceOnReadHeaderTimeout
#[test]
fn test_accept_concurrent_does_not_race_on_read_header_timeout() {
    let _g = GLOBAL_RW.read().unwrap();
    let pl = Arc::new(ProxyListener::new(TcpListener::bind("127.0.0.1:0").unwrap()));
    let addr = pl.listener.local_addr().unwrap();
    let n = 8;
    let mut handles = Vec::with_capacity(n);
    for _ in 0..n {
        let pl_c = Arc::clone(&pl);
        handles.push(thread::spawn(move || pl_c.accept()));
    }
    for _ in 0..n {
        let c = TcpStream::connect(addr).unwrap();
        drop(c);
    }
    for h in handles {
        assert!(h.join().unwrap().is_ok());
    }
    assert_eq!(pl.read_header_timeout, ListenerHeaderTimeout::Default);
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::TestWithBufferSizePositive
#[test]
fn test_with_buffer_size_positive() {
    let _g = GLOBAL_RW.read().unwrap();
    let (conn, mut peer) = net_pipe();
    let mut pc = ProxyConn::new(conn, vec![with_policy(Policy::Use), with_buffer_size(4096)]);
    assert_eq!(pc.buffer_size(), Some(4096));
    let t = thread::spawn(move || { let _ = peer.write_all(b"x"); });
    let mut buf = [0u8; 1];
    pc.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"x");
    t.join().unwrap();
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::TestWithBufferSizeZeroOrNegative
#[test]
fn test_with_buffer_size_zero_or_negative() {
    let _g = GLOBAL_RW.read().unwrap();
    for len in [0isize, -1] {
        let (conn, mut peer) = net_pipe();
        let mut pc = ProxyConn::new(conn, vec![with_policy(Policy::Use), with_buffer_size(len)]);
        assert!(pc.buffer_size().is_none());
        let t = thread::spawn(move || { let _ = peer.write_all(b"y"); });
        let mut buf = [0u8; 1];
        pc.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"y");
        t.join().unwrap();
    }
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::TestListenerReadBufferSizeApplied
#[test]
fn test_listener_read_buffer_size_applied() {
    let _g = GLOBAL_RW.read().unwrap();
    let mut pl = ProxyListener::new(TcpListener::bind("127.0.0.1:0").unwrap());
    pl.read_buffer_size = 4096;
    let addr = pl.listener.local_addr().unwrap();
    let t = thread::spawn(move || { let _ = TcpStream::connect(addr); });
    let pc = pl.accept().unwrap().into_proxy().unwrap();
    assert_eq!(pc.buffer_size(), Some(4096));
    t.join().unwrap();
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::TestListenerReadBufferSizeZeroUsesDefault
#[test]
fn test_listener_read_buffer_size_zero_uses_default() {
    let _g = GLOBAL_RW.read().unwrap();
    let pl = ProxyListener::new(TcpListener::bind("127.0.0.1:0").unwrap());
    let addr = pl.listener.local_addr().unwrap();
    let t = thread::spawn(move || { let _ = TcpStream::connect(addr); });
    let pc = pl.accept().unwrap().into_proxy().unwrap();
    assert!(pc.buffer_size().is_none());
    t.join().unwrap();
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::TestReadHeaderTimeoutRespectsEarlierDeadline
#[test]
fn test_read_header_timeout_respects_earlier_deadline() {
    let _g = GLOBAL_RW.read().unwrap();
    let header_timeout = Duration::from_millis(250);
    let user_timeout = Duration::from_millis(60);
    let mut pl = ProxyListener::new(TcpListener::bind("127.0.0.1:0").unwrap());
    pl.read_header_timeout = ListenerHeaderTimeout::Duration(header_timeout);
    pl.policy = Some(Arc::new(|_| Ok(Policy::Require)));
    let addr = pl.listener.local_addr().unwrap();
    let cli = spawn_tcp_client(addr, None, None, None, false, true);
    let mut conn = pl.accept().unwrap().into_proxy().unwrap();
    conn.set_read_deadline(Some(SystemTime::now() + user_timeout)).unwrap();
    let start = Instant::now();
    let err = conn.read_conn(&mut [0u8; 1]).unwrap_err();
    let elapsed = start.elapsed();
    assert!(err.is_no_proxy_protocol(), "got {err:?}");
    assert!(elapsed <= user_timeout + Duration::from_millis(120), "elapsed {elapsed:?}");
    cli.join().unwrap();
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::TestDeadlineSettersAfterHeaderProcessed
#[test]
fn test_deadline_setters_after_header_processed() {
    let _g = GLOBAL_RW.read().unwrap();
    let (conn, mut peer) = net_pipe();
    let mut pc = ProxyConn::new(conn, vec![with_policy(Policy::Use)]);
    let t = thread::spawn(move || { let _ = peer.write_all(b"x"); });
    let mut buf = [0u8; 1];
    pc.read_exact(&mut buf).unwrap();
    let dl = Some(SystemTime::now() + Duration::from_secs(1));
    pc.set_deadline(dl).unwrap();
    pc.set_read_deadline(dl).unwrap();
    pc.set_write_deadline(dl).unwrap();
    t.join().unwrap();
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::TestReadHeaderTimeoutIsReset
#[test]
fn test_read_header_timeout_is_reset() {
    let _g = GLOBAL_RW.read().unwrap();
    let timeout = Duration::from_millis(150);
    let mut pl = ProxyListener::new(TcpListener::bind("127.0.0.1:0").unwrap());
    pl.read_header_timeout = ListenerHeaderTimeout::Duration(timeout);
    let addr = pl.listener.local_addr().unwrap();
    let header = test_tcp_v4_header();
    let h_c = header.clone();
    let cli = thread::spawn(move || {
        let mut c = TcpStream::connect(addr).unwrap();
        h_c.write_to(&mut c).unwrap();
        thread::sleep(timeout * 2);
        c.write_all(b"ping").unwrap();
        let mut recv = [0u8; 4];
        c.read_exact(&mut recv).unwrap();
        assert_eq!(&recv, b"pong");
    });
    let mut conn = pl.accept().unwrap().into_proxy().unwrap();
    conn.set_read_deadline(Some(SystemTime::now() + timeout * 4)).unwrap();
    conn.set_write_deadline(Some(SystemTime::now() + timeout * 4)).unwrap();
    let mut recv = [0u8; 4];
    conn.read_exact(&mut recv).unwrap();
    assert_eq!(&recv, b"ping");
    conn.write_all(b"pong").unwrap();
    let remote = conn.remote_addr_or_proxy().unwrap();
    assert_eq!(remote.to_string(), "10.1.1.1:1000");
    assert!(conn.proxy_header().unwrap().equals_to(Some(&header)));
    cli.join().unwrap();
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::TestAcceptDoesNotMutateListenerReadHeaderTimeout
#[test]
fn test_accept_does_not_mutate_listener_read_header_timeout() {
    let _g = GLOBAL_RW.read().unwrap();
    let pl = ProxyListener::new(TcpListener::bind("127.0.0.1:0").unwrap());
    let addr = pl.listener.local_addr().unwrap();
    let cli = thread::spawn(move || { let _ = TcpStream::connect(addr); });
    let _conn = pl.accept().unwrap();
    assert_eq!(pl.read_header_timeout, ListenerHeaderTimeout::Default);
    cli.join().unwrap();
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::TestReadHeaderTimeoutIsEmpty
#[test]
fn test_read_header_timeout_is_empty() {
    let _g = GLOBAL_RW.write().unwrap();
    struct Reset(Duration);
    impl Drop for Reset { fn drop(&mut self) { set_default_read_header_timeout(self.0); } }
    let _r = Reset(default_read_header_timeout());
    set_default_read_header_timeout(Duration::from_millis(120));

    let mut pl = ProxyListener::new(TcpListener::bind("127.0.0.1:0").unwrap());
    pl.conn_policy = Some(use_policy());
    let addr = pl.listener.local_addr().unwrap();
    let header = test_tcp_v4_header();
    let cli = thread::spawn(move || {
        let mut c = TcpStream::connect(addr).unwrap();
        thread::sleep(Duration::from_millis(200));
        header.write_to(&mut c).unwrap();
        c.write_all(b"ping").unwrap();
    });
    let mut conn = pl.accept().unwrap().into_proxy().unwrap();
    let mut recv = [0u8; 4];
    conn.read_exact(&mut recv).unwrap();
    let remote = conn.remote_addr_or_proxy().unwrap();
    assert_ne!(remote.to_string(), "10.1.1.1:1000");
    cli.join().unwrap();
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::TestReadHeaderTimeoutIsNegative
#[test]
fn test_read_header_timeout_is_negative() {
    let _g = GLOBAL_RW.read().unwrap();
    let mut pl = ProxyListener::new(TcpListener::bind("127.0.0.1:0").unwrap());
    pl.read_header_timeout = ListenerHeaderTimeout::Disabled;
    let addr = pl.listener.local_addr().unwrap();
    let header = test_tcp_v4_header();
    let cli = thread::spawn(move || {
        let mut c = TcpStream::connect(addr).unwrap();
        thread::sleep(Duration::from_millis(200));
        header.write_to(&mut c).unwrap();
        c.write_all(b"ping").unwrap();
    });
    let mut conn = pl.accept().unwrap().into_proxy().unwrap();
    let mut recv = [0u8; 4];
    conn.read_exact(&mut recv).unwrap();
    let remote = conn.remote_addr_or_proxy().unwrap();
    assert_eq!(remote.to_string(), "10.1.1.1:1000");
    cli.join().unwrap();
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::TestParse_ipv4
#[test]
fn test_parse_ipv4() {
    let _g = GLOBAL_RW.read().unwrap();
    let pl = ProxyListener::new(TcpListener::bind("127.0.0.1:0").unwrap());
    let addr = pl.listener.local_addr().unwrap();
    let header = test_tcp_v4_header();
    let cli = spawn_tcp_client(addr, Some(header.clone()), Some(b"ping"), Some(b"pong"), false, false);
    let mut conn = pl.accept().unwrap().into_proxy().unwrap();
    let mut recv = [0u8; 4];
    conn.read_exact(&mut recv).unwrap();
    assert_eq!(&recv, b"ping");
    conn.write_all(b"pong").unwrap();
    assert_eq!(conn.remote_addr_or_proxy().unwrap().to_string(), "10.1.1.1:1000");
    assert!(conn.proxy_header().unwrap().equals_to(Some(&header)));
    cli.join().unwrap();
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::TestParse_unixStream
#[cfg(unix)]
#[test]
fn test_parse_unix_stream() {
    let _g = GLOBAL_RW.read().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let sock_path = dir.path().join("proxy.sock");
    let ul = UnixListener::bind(&sock_path).unwrap();
    let pl = ProxyListener::new(ul);
    let header = Header {
        version: 2,
        command: ProtocolVersionAndCommand::PROXY,
        transport_protocol: AddressFamilyAndProtocol::UNIX_STREAM,
        source_addr: Some(ProxyAddr::unix("source.sock")),
        destination_addr: Some(ProxyAddr::unix("dest.sock")),
        raw_tlvs: Vec::new(),
    };
    let h_c = header.clone();
    let p_c = sock_path.clone();
    let cli = thread::spawn(move || {
        let mut c = UnixStream::connect(&p_c).unwrap();
        h_c.write_to(&mut c).unwrap();
        c.write_all(b"ping").unwrap();
        let mut recv = [0u8; 4];
        c.read_exact(&mut recv).unwrap();
        assert_eq!(&recv, b"pong");
    });
    let mut conn = pl.accept().unwrap().into_proxy().unwrap();
    let mut recv = [0u8; 4];
    conn.read_exact(&mut recv).unwrap();
    assert_eq!(&recv, b"ping");
    conn.write_all(b"pong").unwrap();
    assert_eq!(conn.remote_addr_or_proxy().unwrap().to_string(), "source.sock");
    assert!(conn.proxy_header().unwrap().equals_to(Some(&header)));
    cli.join().unwrap();
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::TestParse_unixDatagramHeaderOverStream
#[test]
fn test_parse_unix_datagram_header_over_stream() {
    let _g = GLOBAL_RW.read().unwrap();
    let (server, mut client) = net_pipe();
    let header = Header {
        version: 2,
        command: ProtocolVersionAndCommand::PROXY,
        transport_protocol: AddressFamilyAndProtocol::UNIX_DATAGRAM,
        source_addr: Some(ProxyAddr::unixgram("source.sock")),
        destination_addr: Some(ProxyAddr::unixgram("dest.sock")),
        raw_tlvs: Vec::new(),
    };
    let h_c = header.clone();
    let cli = thread::spawn(move || {
        h_c.write_to(&mut client).unwrap();
        client.write_all(b"ping").unwrap();
        let _ = client.close();
    });
    let mut conn = ProxyConn::new(server, vec![]);
    let mut recv = [0u8; 4];
    conn.read_exact(&mut recv).unwrap();
    assert_eq!(&recv, b"ping");
    assert_eq!(conn.remote_addr_or_proxy().unwrap().to_string(), "source.sock");
    assert_eq!(conn.local_addr_or_proxy().unwrap().to_string(), "dest.sock");
    assert!(conn.proxy_header().unwrap().equals_to(Some(&header)));
    cli.join().unwrap();
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::TestParse_ipv6
#[test]
fn test_parse_ipv6() {
    let _g = GLOBAL_RW.read().unwrap();
    let pl = ProxyListener::new(TcpListener::bind("127.0.0.1:0").unwrap());
    let addr = pl.listener.local_addr().unwrap();
    let header = Header {
        version: 2,
        command: ProtocolVersionAndCommand::PROXY,
        transport_protocol: AddressFamilyAndProtocol::TCPV6,
        source_addr: Some(ProxyAddr::tcp("ffff::ffff".parse().unwrap(), 1000)),
        destination_addr: Some(ProxyAddr::tcp("ffff::ffff".parse().unwrap(), 2000)),
        raw_tlvs: Vec::new(),
    };
    let cli = spawn_tcp_client(addr, Some(header.clone()), Some(b"ping"), Some(b"pong"), false, false);
    let mut conn = pl.accept().unwrap().into_proxy().unwrap();
    let mut recv = [0u8; 4];
    conn.read_exact(&mut recv).unwrap();
    assert_eq!(&recv, b"ping");
    conn.write_all(b"pong").unwrap();
    assert_eq!(conn.remote_addr_or_proxy().unwrap().to_string(), "[ffff::ffff]:1000");
    assert!(conn.proxy_header().unwrap().equals_to(Some(&header)));
    cli.join().unwrap();
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::TestAcceptReturnsErrorWhenPolicyFuncErrors
#[test]
fn test_accept_returns_error_when_policy_func_errors() {
    let _g = GLOBAL_RW.read().unwrap();
    let mut pl = ProxyListener::new(TcpListener::bind("127.0.0.1:0").unwrap());
    pl.policy = Some(Arc::new(|_| Err(ProxyError::Custom("failure".to_string()))));
    let addr = pl.listener.local_addr().unwrap();
    let cli = spawn_tcp_client(addr, None, None, None, true, true);
    let err = pl.accept().err().unwrap();
    assert_eq!(err.to_string(), "failure");
    cli.join().unwrap();
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::TestPanicIfPolicyAndConnPolicySet
#[test]
fn test_panic_if_policy_and_conn_policy_set() {
    let _g = GLOBAL_RW.read().unwrap();
    let mut pl = ProxyListener::new(TcpListener::bind("127.0.0.1:0").unwrap());
    pl.policy = Some(Arc::new(|_| Ok(Policy::Use)));
    pl.conn_policy = Some(use_policy());
    let addr = pl.listener.local_addr().unwrap();
    let cli = spawn_tcp_client(addr, None, None, None, true, true);
    let err = pl.accept().err().unwrap();
    assert!(matches!(err, ProxyError::BothPoliciesSet), "got {err:?}");
    cli.join().unwrap();
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::TestAcceptReturnsErrorWhenConnPolicyFuncErrors
#[test]
fn test_accept_returns_error_when_conn_policy_func_errors() {
    let _g = GLOBAL_RW.read().unwrap();
    let mut pl = ProxyListener::new(TcpListener::bind("127.0.0.1:0").unwrap());
    pl.conn_policy = Some(Arc::new(|_| Err(ProxyError::Custom("failure".to_string()))));
    let addr = pl.listener.local_addr().unwrap();
    let cli = spawn_tcp_client(addr, None, None, None, true, true);
    let err = pl.accept().err().unwrap();
    assert_eq!(err.to_string(), "failure");
    cli.join().unwrap();
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::TestReadingIsRefusedWhenProxyHeaderRequiredButMissing
#[test]
fn test_reading_is_refused_when_proxy_header_required_but_missing() {
    let _g = GLOBAL_RW.read().unwrap();
    let mut pl = ProxyListener::new(TcpListener::bind("127.0.0.1:0").unwrap());
    pl.policy = Some(Arc::new(|_| Ok(Policy::Require)));
    let addr = pl.listener.local_addr().unwrap();
    let cli = spawn_tcp_client(addr, None, Some(b"ping"), None, false, false);
    let mut conn = pl.accept().unwrap().into_proxy().unwrap();
    let err = conn.read_conn(&mut [0u8; 4]).unwrap_err();
    assert!(err.is_no_proxy_protocol(), "got {err:?}");
    cli.join().unwrap();
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::TestReadingIsRefusedWhenProxyHeaderPresentButNotAllowed
#[test]
fn test_reading_is_refused_when_proxy_header_present_but_not_allowed() {
    let _g = GLOBAL_RW.read().unwrap();
    let mut pl = ProxyListener::new(TcpListener::bind("127.0.0.1:0").unwrap());
    pl.policy = Some(Arc::new(|_| Ok(Policy::Reject)));
    let addr = pl.listener.local_addr().unwrap();
    let cli = spawn_tcp_client(addr, Some(test_tcp_v4_header()), None, None, false, false);
    let mut conn = pl.accept().unwrap().into_proxy().unwrap();
    let err = conn.read_conn(&mut [0u8; 4]).unwrap_err();
    assert!(matches!(err, ProxyError::SuperfluousProxyHeader), "got {err:?}");
    cli.join().unwrap();
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::TestIgnorePolicyIgnoresIpFromProxyHeader
#[test]
fn test_ignore_policy_ignores_ip_from_proxy_header() {
    let _g = GLOBAL_RW.read().unwrap();
    let mut pl = ProxyListener::new(TcpListener::bind("127.0.0.1:0").unwrap());
    pl.policy = Some(Arc::new(|_| Ok(Policy::Ignore)));
    let addr = pl.listener.local_addr().unwrap();
    let cli = spawn_tcp_client(addr, Some(test_tcp_v4_header()), Some(b"ping"), Some(b"pong"), false, false);
    let mut conn = pl.accept().unwrap().into_proxy().unwrap();
    let mut recv = [0u8; 4];
    conn.read_exact(&mut recv).unwrap();
    assert_eq!(&recv, b"ping");
    conn.write_all(b"pong").unwrap();
    let remote = conn.remote_addr_or_proxy().unwrap();
    assert!(remote.to_string().starts_with("127.0.0.1:"), "{remote}");
    assert!(conn.proxy_header().is_none());
    cli.join().unwrap();
}

#[test]
fn test_accepted_conn_smoke() {
    let (c, _p) = net_pipe();
    let _ac = AcceptedConn::Raw(c);
    let _br = ProxyBufReader::new(Cursor::new(Vec::<u8>::new()));
}
