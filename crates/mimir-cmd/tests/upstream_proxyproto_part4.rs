//! Ported upstream tests from github.com/pires/go-proxyproto:
//! - protocol_test.go (second half: 31 tests)

use std::any::Any;
use std::collections::VecDeque;
use std::io::{self, Cursor, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
#[cfg(unix)]
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread;
use std::time::{Duration, SystemTime};

use mimir_cmd::server::proxyproto::{
    default_policy, set_default_policy, trust_proxy_header_from, with_buffer_size, with_policy,
    AddressFamilyAndProtocol, ConnOption, Header, Policy,
    ProtocolVersionAndCommand, ProxyAddr, ProxyConn, ProxyError, ProxyListener, StreamConn,
    StreamListener,
};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};

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
            if guard.closed || self.closed { return Ok(0); }
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
    fn local_addr(&self) -> io::Result<ProxyAddr> { Ok(ProxyAddr::other("pipe", "pipe-local")) }
    fn remote_addr(&self) -> io::Result<ProxyAddr> { Ok(ProxyAddr::other("pipe", "pipe-remote")) }
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

struct ClosableListener<L> {
    inner: L,
    closed: Arc<AtomicBool>,
}
impl ClosableListener<TcpListener> {
    fn new_tcp(l: TcpListener) -> Self {
        l.set_nonblocking(true).unwrap();
        Self { inner: l, closed: Arc::new(AtomicBool::new(false)) }
    }
}
impl StreamListener for ClosableListener<TcpListener> {
    type Conn = TcpStream;
    fn accept_stream(&self) -> io::Result<TcpStream> {
        loop {
            if self.closed.load(Ordering::Acquire) {
                return Err(io::Error::new(io::ErrorKind::BrokenPipe, "listener closed"));
            }
            match self.inner.accept() {
                Ok((s, _)) => {
                    s.set_nonblocking(false)?;
                    return Ok(s);
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => thread::sleep(Duration::from_millis(5)),
                Err(e) => return Err(e),
            }
        }
    }
    fn close_listener(&self) -> io::Result<()> {
        self.closed.store(true, Ordering::Release);
        Ok(())
    }
    fn addr(&self) -> io::Result<ProxyAddr> {
        self.inner.local_addr().map(ProxyAddr::from)
    }
}
#[cfg(unix)]
impl ClosableListener<UnixListener> {
    fn new_unix(l: UnixListener) -> Self {
        l.set_nonblocking(true).unwrap();
        Self { inner: l, closed: Arc::new(AtomicBool::new(false)) }
    }
}
#[cfg(unix)]
impl StreamListener for ClosableListener<UnixListener> {
    type Conn = UnixStream;
    fn accept_stream(&self) -> io::Result<UnixStream> {
        loop {
            if self.closed.load(Ordering::Acquire) {
                return Err(io::Error::new(io::ErrorKind::BrokenPipe, "listener closed"));
            }
            match self.inner.accept() {
                Ok((s, _)) => {
                    s.set_nonblocking(false)?;
                    return Ok(s);
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => thread::sleep(Duration::from_millis(5)),
                Err(e) => return Err(e),
            }
        }
    }
    fn close_listener(&self) -> io::Result<()> {
        self.closed.store(true, Ordering::Release);
        Ok(())
    }
    fn addr(&self) -> io::Result<ProxyAddr> {
        Ok(ProxyAddr::unix("unix"))
    }
}

fn load_test_tls() -> (Arc<rustls::ServerConfig>, Arc<rustls::ClientConfig>) {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/testdata/exporter_toolkit_web");
    let cert_bytes = std::fs::read(dir.join("server.crt")).unwrap();
    let key_bytes = std::fs::read(dir.join("server.key")).unwrap();
    let ca_bytes = std::fs::read(dir.join("tls-ca-chain.pem")).unwrap();

    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(&cert_bytes).map(|c| c.unwrap()).collect();
    let key = PrivateKeyDer::from_pem_slice(&key_bytes).unwrap();
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let srv_cfg = rustls::ServerConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .unwrap();

    let mut roots = rustls::RootCertStore::empty();
    for ca in CertificateDer::pem_slice_iter(&ca_bytes) {
        roots.add(ca.unwrap()).unwrap();
    }
    let cli_cfg = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    (Arc::new(srv_cfg), Arc::new(cli_cfg))
}

struct TlsStreamConn<C: StreamConn> {
    tls: rustls::StreamOwned<rustls::ServerConnection, C>,
}
impl<C: StreamConn> Read for TlsStreamConn<C> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> { self.tls.read(buf) }
}
impl<C: StreamConn> Write for TlsStreamConn<C> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> { self.tls.write(buf) }
    fn flush(&mut self) -> io::Result<()> { self.tls.flush() }
}
impl<C: StreamConn + 'static> StreamConn for TlsStreamConn<C> {
    fn local_addr(&self) -> io::Result<ProxyAddr> { self.tls.sock.local_addr() }
    fn remote_addr(&self) -> io::Result<ProxyAddr> { self.tls.sock.remote_addr() }
    fn close(&mut self) -> io::Result<()> { self.tls.sock.close() }
    fn set_deadline(&mut self, t: Option<SystemTime>) -> io::Result<()> { self.tls.sock.set_deadline(t) }
    fn set_read_deadline(&mut self, t: Option<SystemTime>) -> io::Result<()> { self.tls.sock.set_read_deadline(t) }
    fn set_write_deadline(&mut self, t: Option<SystemTime>) -> io::Result<()> { self.tls.sock.set_write_deadline(t) }
    fn as_any(&self) -> &dyn Any { self }
    fn as_any_mut(&mut self) -> &mut dyn Any { self }
}

fn spawn_tcp_client(addr: SocketAddr, header: Option<Header>, payload: Option<&'static [u8]>, close_after: bool) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let mut conn = TcpStream::connect(addr).unwrap();
        if let Some(h) = header { h.write_to(&mut conn).unwrap(); }
        if let Some(p) = payload { conn.write_all(p).unwrap(); }
        if close_after { drop(conn); } else { thread::sleep(Duration::from_millis(200)); }
    })
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::Test_AllOptionsAreRecognized
#[test]
fn test_all_options_are_recognized() {
    let _g = GLOBAL_RW.read().unwrap();
    let (server, _client) = net_pipe();
    let r1 = Arc::new(AtomicBool::new(false));
    let r2 = Arc::new(AtomicBool::new(false));
    let r1c = Arc::clone(&r1);
    let r2c = Arc::clone(&r2);
    let opt1: ConnOption<PipeConn> = Box::new(move |_| r1c.store(true, Ordering::Release));
    let opt2: ConnOption<PipeConn> = Box::new(move |_| r2c.store(true, Ordering::Release));
    let _c = ProxyConn::new(server, vec![opt1, opt2]);
    assert!(r1.load(Ordering::Acquire));
    assert!(r2.load(Ordering::Acquire));
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::TestReadingIsRefusedOnErrorWhenAddrRequestedFirst
#[test]
fn test_reading_is_refused_on_error_when_addr_requested_first() {
    let _g = GLOBAL_RW.read().unwrap();
    for poke_remote in [true, false] {
        let mut pl = ProxyListener::new(TcpListener::bind("127.0.0.1:0").unwrap());
        pl.policy = Some(Arc::new(|_| Ok(Policy::Require)));
        let addr = pl.listener.local_addr().unwrap();
        let cli = spawn_tcp_client(addr, None, Some(b"ping"), false);
        let mut conn = pl.accept().unwrap().into_proxy().unwrap();
        if poke_remote {
            let _ = conn.remote_addr_or_proxy();
        } else {
            let _ = conn.local_addr_or_proxy();
        }
        let err = conn.read_conn(&mut [0u8; 4]).unwrap_err();
        assert!(err.is_no_proxy_protocol(), "got {err:?}");
        cli.join().unwrap();
    }
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::TestSkipProxyProtocolConnPolicy
#[test]
fn test_skip_proxy_protocol_conn_policy() {
    let _g = GLOBAL_RW.read().unwrap();
    let mut pl = ProxyListener::new(TcpListener::bind("127.0.0.1:0").unwrap());
    pl.conn_policy = Some(Arc::new(|_| Ok(Policy::Skip)));
    let addr = pl.listener.local_addr().unwrap();
    let cli = spawn_tcp_client(addr, None, Some(b"ping"), false);
    let mut tcp = pl.accept().unwrap().into_raw().unwrap();
    let _ = tcp.local_addr().unwrap();
    let mut recv = [0u8; 4];
    tcp.read_exact(&mut recv).unwrap();
    assert_eq!(&recv, b"ping");
    cli.join().unwrap();
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::TestSkipProxyProtocolPolicy
#[test]
fn test_skip_proxy_protocol_policy() {
    let _g = GLOBAL_RW.read().unwrap();
    let mut pl = ProxyListener::new(TcpListener::bind("127.0.0.1:0").unwrap());
    pl.policy = Some(Arc::new(|_| Ok(Policy::Skip)));
    let addr = pl.listener.local_addr().unwrap();
    let cli = spawn_tcp_client(addr, None, Some(b"ping"), false);
    let mut tcp = pl.accept().unwrap().into_raw().unwrap();
    let _ = tcp.local_addr().unwrap();
    let mut recv = [0u8; 4];
    tcp.read_exact(&mut recv).unwrap();
    assert_eq!(&recv, b"ping");
    cli.join().unwrap();
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::TestLocalCommandUsesUnderlyingAddrs
#[test]
fn test_local_command_uses_underlying_addrs() {
    let _g = GLOBAL_RW.read().unwrap();
    let pl = ProxyListener::new(TcpListener::bind("127.0.0.1:0").unwrap());
    let addr = pl.listener.local_addr().unwrap();
    let header = Header {
        version: 2,
        command: ProtocolVersionAndCommand::LOCAL,
        transport_protocol: AddressFamilyAndProtocol::UNSPEC,
        ..Default::default()
    };
    let cli = spawn_tcp_client(addr, Some(header), Some(b"ping"), true);
    let mut pc = pl.accept().unwrap().into_proxy().unwrap();
    assert_eq!(pc.local_addr_or_proxy().unwrap(), pc.local_addr().unwrap());
    assert_eq!(pc.remote_addr_or_proxy().unwrap(), pc.remote_addr().unwrap());
    cli.join().unwrap();
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::Test_ConnectionCasts
#[test]
fn test_connection_casts() {
    let _g = GLOBAL_RW.read().unwrap();
    let mut pl = ProxyListener::new(TcpListener::bind("127.0.0.1:0").unwrap());
    pl.policy = Some(Arc::new(|_| Ok(Policy::Require)));
    let addr = pl.listener.local_addr().unwrap();
    let cli = spawn_tcp_client(addr, None, Some(b"ping"), false);
    let pc = pl.accept().unwrap().into_proxy().unwrap();
    assert!(pc.tcp_conn().is_some());
    #[cfg(unix)]
    assert!(pc.unix_conn().is_none());
    assert!(pc.raw().is_some());
    cli.join().unwrap();
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::Test_ConnectionErrorsWhenHeaderValidationFails
#[test]
fn test_connection_errors_when_header_validation_fails() {
    let _g = GLOBAL_RW.read().unwrap();
    let mut pl = ProxyListener::new(TcpListener::bind("127.0.0.1:0").unwrap());
    pl.validate_header = Some(Arc::new(|_| Err(ProxyError::Custom("failed to validate".to_string()))));
    let addr = pl.listener.local_addr().unwrap();
    let cli = spawn_tcp_client(addr, Some(test_tcp_v4_header()), None, false);
    let mut pc = pl.accept().unwrap().into_proxy().unwrap();
    let err = pc.read_conn(&mut [0u8; 4]).unwrap_err();
    assert_eq!(err.to_string(), "failed to validate");
    cli.join().unwrap();
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::Test_ConnectionHandlesInvalidUpstreamError
#[test]
fn test_connection_handles_invalid_upstream_error() {
    let _g = GLOBAL_RW.read().unwrap();
    let raw_l = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = raw_l.local_addr().unwrap();
    let counter = Arc::new(AtomicI32::new(0));
    let counter_c = Arc::clone(&counter);
    let mut pl = ProxyListener::new(raw_l);
    pl.conn_policy = Some(Arc::new(move |_| {
        if counter_c.fetch_add(1, Ordering::AcqRel) == 0 {
            Err(ProxyError::InvalidUpstream("untrusted".to_string()))
        } else {
            Err(ProxyError::NoProxyProtocol)
        }
    }));
    let (tx, rx) = std::sync::mpsc::channel();
    let h = thread::spawn(move || { let _ = tx.send(pl.accept().err()); });

    let c1 = TcpStream::connect(addr).unwrap();
    drop(c1);
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while counter.load(Ordering::Acquire) < 1 && std::time::Instant::now() < deadline {
        thread::sleep(Duration::from_millis(5));
    }
    assert!(counter.load(Ordering::Acquire) >= 1);
    assert!(rx.recv_timeout(Duration::from_millis(120)).is_err());

    let _c2 = TcpStream::connect(addr).unwrap();
    let err = rx.recv_timeout(Duration::from_secs(2)).unwrap().unwrap();
    assert!(err.is_no_proxy_protocol(), "got {err:?}");
    h.join().unwrap();
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::Test_TLSServerHeaderInsideTLS
#[test]
fn test_tls_server_header_inside_tls() {
    let _g = GLOBAL_RW.read().unwrap();
    let (srv_cfg, cli_cfg) = load_test_tls();
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap();
    let header = test_tcp_v4_header();
    let h_c = header.clone();
    let cli = thread::spawn(move || {
        let tcp = TcpStream::connect(addr).unwrap();
        let cc = rustls::ClientConnection::new(cli_cfg, ServerName::try_from("localhost").unwrap()).unwrap();
        let mut tls = rustls::StreamOwned::new(cc, tcp);
        h_c.write_to(&mut tls).unwrap();
        tls.write_all(b"test").unwrap();
        tls.flush().unwrap();
        thread::sleep(Duration::from_millis(200));
    });
    let (tcp, _) = l.accept().unwrap();
    let sc = rustls::ServerConnection::new(srv_cfg).unwrap();
    let tls_conn = TlsStreamConn { tls: rustls::StreamOwned::new(sc, tcp) };
    let mut pc = ProxyConn::new(tls_conn, vec![with_policy(Policy::Require)]);
    let mut recv = [0u8; 4];
    pc.read_exact(&mut recv).unwrap();
    assert_eq!(&recv, b"test");
    assert_eq!(pc.remote_addr_or_proxy().unwrap().to_string(), "10.1.1.1:1000");
    cli.join().unwrap();
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::Test_TLSServerHeaderBeforeTLS
#[test]
fn test_tls_server_header_before_tls() {
    let _g = GLOBAL_RW.read().unwrap();
    let (srv_cfg, cli_cfg) = load_test_tls();
    let mut pl = ProxyListener::new(TcpListener::bind("127.0.0.1:0").unwrap());
    pl.policy = Some(Arc::new(|_| Ok(Policy::Require)));
    let addr = pl.listener.local_addr().unwrap();
    let header = test_tcp_v4_header();
    let cli = thread::spawn(move || {
        let mut raw = TcpStream::connect(addr).unwrap();
        header.write_to(&mut raw).unwrap();
        let cc = rustls::ClientConnection::new(cli_cfg, ServerName::try_from("localhost").unwrap()).unwrap();
        let mut tls = rustls::StreamOwned::new(cc, raw);
        tls.write_all(b"test").unwrap();
        tls.flush().unwrap();
        thread::sleep(Duration::from_millis(200));
    });
    let pc = pl.accept().unwrap().into_proxy().unwrap();
    let sc = rustls::ServerConnection::new(srv_cfg).unwrap();
    let mut tls = rustls::StreamOwned::new(sc, pc);
    let mut recv = [0u8; 4];
    tls.read_exact(&mut recv).unwrap();
    assert_eq!(&recv, b"test");
    assert_eq!(tls.sock.remote_addr_or_proxy().unwrap().to_string(), "10.1.1.1:1000");
    cli.join().unwrap();
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::Test_MisconfiguredTLSServerRespondsWithUnderlyingError
#[test]
fn test_misconfigured_tls_server_responds_with_underlying_error() {
    let _g = GLOBAL_RW.read().unwrap();
    let (srv_cfg, _) = load_test_tls();
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap();
    let header = test_tcp_v4_header();
    let cli = thread::spawn(move || {
        let mut raw = TcpStream::connect(addr).unwrap();
        header.write_to(&mut raw).unwrap();
        raw.write_all(b"GET /foo/bar HTTP/1.1").unwrap();
        let _ = raw.shutdown(Shutdown::Write);
    });
    let (tcp, _) = l.accept().unwrap();
    let sc = rustls::ServerConnection::new(srv_cfg).unwrap();
    let tls_conn = TlsStreamConn { tls: rustls::StreamOwned::new(sc, tcp) };
    let mut pc = ProxyConn::new(tls_conn, vec![with_policy(Policy::Require)]);
    let err = pc.read_conn(&mut [0u8; 1024]).unwrap_err();
    assert!(!err.is_no_proxy_protocol(), "expected TLS handshake error, got {err:?}");
    cli.join().unwrap();
}

struct TestConn {
    read_from_tag: Arc<Mutex<Option<String>>>,
    tag: String,
    reads: usize,
}
impl Read for TestConn {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.reads == 0 || buf.is_empty() { return Ok(0); }
        self.reads -= 1;
        buf[0] = 0;
        Ok(1)
    }
}
impl Write for TestConn {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> { Ok(buf.len()) }
    fn flush(&mut self) -> io::Result<()> { Ok(()) }
}
impl StreamConn for TestConn {
    fn local_addr(&self) -> io::Result<ProxyAddr> { Ok(ProxyAddr::other("dummy", "local")) }
    fn remote_addr(&self) -> io::Result<ProxyAddr> { Ok(ProxyAddr::other("dummy", "remote")) }
    fn close(&mut self) -> io::Result<()> { Ok(()) }
    fn set_deadline(&mut self, _: Option<SystemTime>) -> io::Result<()> { Ok(()) }
    fn set_read_deadline(&mut self, _: Option<SystemTime>) -> io::Result<()> { Ok(()) }
    fn set_write_deadline(&mut self, _: Option<SystemTime>) -> io::Result<()> { Ok(()) }
    fn read_from_reader(&mut self, r: &mut dyn Read) -> Option<io::Result<u64>> {
        // Identify inner source when wrapped via ProxyConn::write_to_stream.
        *self.read_from_tag.lock().unwrap() = Some(self.tag.clone());
        let mut sink = io::sink();
        Some(io::copy(r, &mut sink))
    }
    fn as_any(&self) -> &dyn Any { self }
    fn as_any_mut(&mut self) -> &mut dyn Any { self }
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::TestCopyToWrappedConnection
#[test]
fn test_copy_to_wrapped_connection() {
    let _g = GLOBAL_RW.read().unwrap();
    let tag = Arc::new(Mutex::new(None));
    let inner = TestConn { read_from_tag: Arc::clone(&tag), tag: "inner".to_string(), reads: 0 };
    let mut wrapped = ProxyConn::new(inner, vec![with_policy(Policy::Use)]);
    let mut dummy_src = TestConn { read_from_tag: Arc::new(Mutex::new(None)), tag: "src".to_string(), reads: 1 };
    wrapped.read_from(&mut dummy_src).unwrap();
    assert_eq!(*tag.lock().unwrap(), Some("inner".to_string()));
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::TestCopyFromWrappedConnection
#[test]
fn test_copy_from_wrapped_connection() {
    let _g = GLOBAL_RW.read().unwrap();
    let tag = Arc::new(Mutex::new(None));
    let inner = TestConn { read_from_tag: Arc::new(Mutex::new(None)), tag: "inner".to_string(), reads: 1 };
    let mut wrapped = ProxyConn::new(inner, vec![with_policy(Policy::Use)]);
    let mut dummy_dst = TestConn { read_from_tag: Arc::clone(&tag), tag: "dst".to_string(), reads: 0 };
    wrapped.write_to_stream(&mut dummy_dst).unwrap();
    assert_eq!(*tag.lock().unwrap(), Some("dst".to_string()));
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::TestCopyFromWrappedConnectionToWrappedConnection
#[test]
fn test_copy_from_wrapped_connection_to_wrapped_connection() {
    let _g = GLOBAL_RW.read().unwrap();
    let tag1 = Arc::new(Mutex::new(None));
    let inner1 = TestConn { read_from_tag: Arc::clone(&tag1), tag: "inner1".to_string(), reads: 1 };
    let mut wrapped1 = ProxyConn::new(inner1, vec![with_policy(Policy::Use)]);
    let inner2 = TestConn { read_from_tag: Arc::new(Mutex::new(None)), tag: "inner2".to_string(), reads: 1 };
    let mut wrapped2 = ProxyConn::new(inner2, vec![with_policy(Policy::Use)]);
    wrapped2.write_to_stream(&mut wrapped1).unwrap();
    assert_eq!(*tag1.lock().unwrap(), Some("inner1".to_string()));
}

#[derive(Default)]
struct DeadlineRecordConn {
    dl: Option<SystemTime>,
    rdl: Option<SystemTime>,
    wdl: Option<SystemTime>,
}
impl Read for DeadlineRecordConn { fn read(&mut self, _: &mut [u8]) -> io::Result<usize> { Ok(0) } }
impl Write for DeadlineRecordConn { fn write(&mut self, b: &[u8]) -> io::Result<usize> { Ok(b.len()) } fn flush(&mut self) -> io::Result<()> { Ok(()) } }
impl StreamConn for DeadlineRecordConn {
    fn local_addr(&self) -> io::Result<ProxyAddr> { Ok(ProxyAddr::other("d", "l")) }
    fn remote_addr(&self) -> io::Result<ProxyAddr> { Ok(ProxyAddr::other("d", "r")) }
    fn close(&mut self) -> io::Result<()> { Ok(()) }
    fn set_deadline(&mut self, t: Option<SystemTime>) -> io::Result<()> { self.dl = t; Ok(()) }
    fn set_read_deadline(&mut self, t: Option<SystemTime>) -> io::Result<()> { self.rdl = t; Ok(()) }
    fn set_write_deadline(&mut self, t: Option<SystemTime>) -> io::Result<()> { self.wdl = t; Ok(()) }
    fn as_any(&self) -> &dyn Any { self }
    fn as_any_mut(&mut self) -> &mut dyn Any { self }
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::TestDeadlineWrappersDelegate
#[test]
fn test_deadline_wrappers_delegate() {
    let _g = GLOBAL_RW.read().unwrap();
    let mut pc = ProxyConn::new(DeadlineRecordConn::default(), vec![]);
    let now = SystemTime::now();
    let d1 = Some(now + Duration::from_secs(2));
    let d2 = Some(now + Duration::from_secs(3));
    let d3 = Some(now + Duration::from_secs(4));
    pc.set_deadline(d1).unwrap();
    pc.set_read_deadline(d2).unwrap();
    pc.set_write_deadline(d3).unwrap();
    let inner = pc.raw().unwrap();
    assert_eq!(inner.dl, d1);
    assert_eq!(inner.rdl, d2);
    assert_eq!(inner.wdl, d3);
}

#[derive(Default)]
struct NoReadFromConn { written: Vec<u8> }
impl Read for NoReadFromConn { fn read(&mut self, _: &mut [u8]) -> io::Result<usize> { Ok(0) } }
impl Write for NoReadFromConn { fn write(&mut self, b: &[u8]) -> io::Result<usize> { self.written.extend_from_slice(b); Ok(b.len()) } fn flush(&mut self) -> io::Result<()> { Ok(()) } }
impl StreamConn for NoReadFromConn {
    fn local_addr(&self) -> io::Result<ProxyAddr> { Ok(ProxyAddr::other("d", "l")) }
    fn remote_addr(&self) -> io::Result<ProxyAddr> { Ok(ProxyAddr::other("d", "r")) }
    fn close(&mut self) -> io::Result<()> { Ok(()) }
    fn set_deadline(&mut self, _: Option<SystemTime>) -> io::Result<()> { Ok(()) }
    fn set_read_deadline(&mut self, _: Option<SystemTime>) -> io::Result<()> { Ok(()) }
    fn set_write_deadline(&mut self, _: Option<SystemTime>) -> io::Result<()> { Ok(()) }
    fn as_any(&self) -> &dyn Any { self }
    fn as_any_mut(&mut self) -> &mut dyn Any { self }
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::TestReadFromFallbackCopiesToConn
#[test]
fn test_read_from_fallback_copies_to_conn() {
    let _g = GLOBAL_RW.read().unwrap();
    let mut pc = ProxyConn::new(NoReadFromConn::default(), vec![with_policy(Policy::Use)]);
    let mut src = Cursor::new(b"payload".to_vec());
    pc.read_from(&mut src).unwrap();
    assert_eq!(pc.raw().unwrap().written, b"payload");
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::TestWriteToDrainsBufferedData
#[test]
fn test_write_to_drains_buffered_data() {
    let _g = GLOBAL_RW.read().unwrap();
    let pl = ProxyListener::new(TcpListener::bind("127.0.0.1:0").unwrap());
    let addr = pl.listener.local_addr().unwrap();
    let cli = spawn_tcp_client(addr, Some(test_tcp_v4_header()), Some(b"ping"), true);
    let mut pc = pl.accept().unwrap().into_proxy().unwrap();
    let mut out = Vec::new();
    pc.write_to(&mut out).unwrap();
    assert_eq!(out, b"ping");
    cli.join().unwrap();
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::TestWriteToWithEmptyBuffer
#[test]
fn test_write_to_with_empty_buffer() {
    let _g = GLOBAL_RW.read().unwrap();
    let pl = ProxyListener::new(TcpListener::bind("127.0.0.1:0").unwrap());
    let addr = pl.listener.local_addr().unwrap();
    let cli = spawn_tcp_client(addr, Some(test_tcp_v4_header()), Some(b""), true);
    let mut pc = pl.accept().unwrap().into_proxy().unwrap();
    let mut out = Vec::new();
    pc.write_to(&mut out).unwrap();
    assert!(out.is_empty());
    assert!(!pc.has_buf_reader());
    cli.join().unwrap();
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::TestWriteToWithEmptyBufferAndPendingError
#[test]
fn test_write_to_with_empty_buffer_and_pending_error() {
    let _g = GLOBAL_RW.read().unwrap();
    let pl = ProxyListener::new(TcpListener::bind("127.0.0.1:0").unwrap());
    let addr = pl.listener.local_addr().unwrap();
    let cli = spawn_tcp_client(addr, Some(test_tcp_v4_header()), Some(b""), true);
    let mut pc = pl.accept().unwrap().into_proxy().unwrap();
    let _ = pc.proxy_header();
    if let Some(br) = pc.buf_reader_mut() {
        br.set_pending_err(io::Error::other("read is intentionally broken"));
    }
    let mut out = Vec::new();
    let err = pc.write_to(&mut out).unwrap_err();
    assert!(err.to_string().contains("read is intentionally broken"), "{err}");
    assert!(out.is_empty());
    cli.join().unwrap();
}

struct ChunkedPipe {
    inner: PipeConn,
    max_read: usize,
    read_calls: usize,
    bytes_read: usize,
}
impl Read for ChunkedPipe {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let limit = buf.len().min(self.max_read);
        let n = self.inner.read(&mut buf[..limit])?;
        if n > 0 {
            self.read_calls += 1;
            self.bytes_read += n;
        }
        Ok(n)
    }
}
impl Write for ChunkedPipe {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> { self.inner.write(buf) }
    fn flush(&mut self) -> io::Result<()> { self.inner.flush() }
}
impl StreamConn for ChunkedPipe {
    fn local_addr(&self) -> io::Result<ProxyAddr> { self.inner.local_addr() }
    fn remote_addr(&self) -> io::Result<ProxyAddr> { self.inner.remote_addr() }
    fn close(&mut self) -> io::Result<()> { self.inner.close() }
    fn set_deadline(&mut self, t: Option<SystemTime>) -> io::Result<()> { self.inner.set_deadline(t) }
    fn set_read_deadline(&mut self, t: Option<SystemTime>) -> io::Result<()> { self.inner.set_read_deadline(t) }
    fn set_write_deadline(&mut self, t: Option<SystemTime>) -> io::Result<()> { self.inner.set_write_deadline(t) }
    fn as_any(&self) -> &dyn Any { self }
    fn as_any_mut(&mut self) -> &mut dyn Any { self }
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::TestConnReadHandlesChunkedPayload
#[test]
fn test_conn_read_handles_chunked_payload() {
    let _g = GLOBAL_RW.read().unwrap();
    let payload_size = 400;
    let hdr = b"PROXY TCP4 192.168.1.1 192.168.1.2 12345 443\r\n";
    let payload = vec![b'X'; payload_size];
    let full = [hdr.as_slice(), &payload].concat();
    let full_len = full.len();
    let (srv, mut cli) = net_pipe();
    let t = thread::spawn(move || {
        cli.write_all(&full).unwrap();
        let _ = cli.close();
    });
    let chunked = ChunkedPipe { inner: srv, max_read: 256, read_calls: 0, bytes_read: 0 };
    let mut conn = ProxyConn::new(chunked, vec![]);
    let mut read_payload = Vec::new();
    let mut buf = [0u8; 64];
    while read_payload.len() < payload_size {
        let n = conn.read(&mut buf).unwrap();
        if n == 0 { break; }
        read_payload.extend_from_slice(&buf[..n]);
    }
    assert_eq!(read_payload, payload);
    let raw = conn.raw().unwrap();
    assert!(raw.read_calls > 0);
    assert_eq!(raw.bytes_read, full_len);
    t.join().unwrap();
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::TestReadUsesConnWhenBufReaderNil
#[test]
fn test_read_uses_conn_when_buf_reader_nil() {
    let _g = GLOBAL_RW.read().unwrap();
    let (srv, mut cli) = net_pipe();
    let mut pc = ProxyConn::new(srv, vec![with_policy(Policy::Use)]);
    let (tx, rx) = std::sync::mpsc::channel();
    let t = thread::spawn(move || {
        cli.write_all(b"a").unwrap();
        rx.recv().unwrap();
        cli.write_all(b"b").unwrap();
        let _ = cli.close();
    });
    let mut buf = [0u8; 1];
    pc.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"a");
    assert!(!pc.has_buf_reader());
    tx.send(()).unwrap();
    pc.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"b");
    t.join().unwrap();
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::TestWriteToUsesConnWhenBufReaderNil
#[test]
fn test_write_to_uses_conn_when_buf_reader_nil() {
    let _g = GLOBAL_RW.read().unwrap();
    let (srv, mut cli) = net_pipe();
    let mut pc = ProxyConn::new(srv, vec![with_policy(Policy::Use)]);
    let (tx, rx) = std::sync::mpsc::channel();
    let t = thread::spawn(move || {
        cli.write_all(b"x").unwrap();
        rx.recv().unwrap();
        cli.write_all(b"payload").unwrap();
        let _ = cli.close();
    });
    let mut buf = [0u8; 1];
    pc.read_exact(&mut buf).unwrap();
    assert!(!pc.has_buf_reader());
    tx.send(()).unwrap();
    let mut out = Vec::new();
    pc.write_to(&mut out).unwrap();
    assert_eq!(out, b"payload");
    t.join().unwrap();
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::TestConnWritePathsSurfaceHeaderError
#[test]
fn test_conn_write_paths_surface_header_error() {
    let _g = GLOBAL_RW.read().unwrap();
    let new_failing = || {
        let (srv, mut cli) = net_pipe();
        thread::spawn(move || { let _ = cli.write_all(b"GET / HTTP/1.1\r\n"); thread::sleep(Duration::from_millis(200)); });
        ProxyConn::new(srv, vec![with_policy(Policy::Require)])
    };
    assert!(new_failing().write_conn(b"hello").unwrap_err().is_no_proxy_protocol());
    assert!(new_failing().read_from(&mut Cursor::new(b"hello")).unwrap_err().is_no_proxy_protocol());
    assert!(new_failing().write_to(&mut io::sink()).unwrap_err().is_no_proxy_protocol());
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::TestAcceptSurvivesPolicyAddrError
#[cfg(unix)]
#[test]
fn test_accept_survives_policy_addr_error() {
    let _g = GLOBAL_RW.read().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let sock = dir.path().join("s.sock");
    let ul = ClosableListener::new_unix(UnixListener::bind(&sock).unwrap());
    let mut pl = ProxyListener::new(ul);
    pl.conn_policy = Some(trust_proxy_header_from(vec!["10.0.0.1".parse().unwrap()]));
    let pl = Arc::new(pl);
    let pl_c = Arc::clone(&pl);
    let (tx, rx) = std::sync::mpsc::channel();
    let accept_t = thread::spawn(move || { let _ = tx.send(pl_c.accept().err()); });

    for _ in 0..2 {
        let mut c = UnixStream::connect(&sock).unwrap();
        c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut b = [0u8; 1];
        assert_eq!(c.read(&mut b).unwrap(), 0);
    }
    assert!(rx.try_recv().is_err());
    pl.close().unwrap();
    assert!(rx.recv_timeout(Duration::from_secs(2)).unwrap().is_some());
    accept_t.join().unwrap();
}

struct LimitedFailWriter { allowed: usize, written: Vec<u8> }
impl Write for LimitedFailWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.allowed == 0 { return Err(io::Error::other("writer failed")); }
        self.allowed -= 1;
        self.written.extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> { Ok(()) }
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::TestConnWriteToPropagatesWriterErrors
#[test]
fn test_conn_write_to_propagates_writer_errors() {
    let _g = GLOBAL_RW.read().unwrap();
    let (srv, mut cli) = net_pipe();
    let t1 = thread::spawn(move || { let _ = cli.write_all(b"PROXY TCP4 1.2.3.4 5.6.7.8 80 443\r\nPAYLOAD"); });
    let mut w = LimitedFailWriter { allowed: 0, written: Vec::new() };
    let err = ProxyConn::new(srv, vec![]).write_to(&mut w).unwrap_err();
    assert!(err.to_string().contains("writer failed"), "{err}");
    t1.join().unwrap();

    let (srv2, mut cli2) = net_pipe();
    let t2 = thread::spawn(move || {
        let _ = cli2.write_all(b"PROXY TCP4 1.2.3.4 5.6.7.8 80 443\r\nAB");
        thread::sleep(Duration::from_millis(20));
        let _ = cli2.write_all(b"CD");
        let _ = cli2.close();
    });
    let mut w2 = LimitedFailWriter { allowed: 1, written: Vec::new() };
    let err = ProxyConn::new(srv2, vec![]).write_to(&mut w2).unwrap_err();
    assert!(err.to_string().contains("writer failed"), "{err}");
    assert_eq!(w2.written, b"AB");
    t2.join().unwrap();
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::TestReadHeaderDeadlineSetError
#[test]
fn test_read_header_deadline_set_error() {
    let _g = GLOBAL_RW.read().unwrap();
    let (mut srv, mut peer) = net_pipe();
    let _ = peer.close();
    let _ = srv.close();
    let mut pc = ProxyConn::new(srv, vec![]);
    assert!(pc.read(&mut [0u8; 1]).is_err());
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::TestDefaultPolicyRequiresHeader
#[test]
fn test_default_policy_requires_header() {
    let _g = GLOBAL_RW.write().unwrap();
    struct Reset(Policy);
    impl Drop for Reset { fn drop(&mut self) { set_default_policy(self.0); } }

    let (srv, mut peer) = net_pipe();
    let t = thread::spawn(move || { let _ = peer.write_all(b"GET / HTTP/1.1\r\n"); thread::sleep(Duration::from_millis(100)); });
    assert!(ProxyConn::new(srv, vec![]).read_conn(&mut [0u8; 1]).unwrap_err().is_no_proxy_protocol());
    t.join().unwrap();

    let pl = ProxyListener::new(TcpListener::bind("127.0.0.1:0").unwrap());
    let addr = pl.listener.local_addr().unwrap();
    let t2 = spawn_tcp_client(addr, None, Some(b"GET / HTTP/1.1\r\n"), false);
    let mut conn = pl.accept().unwrap().into_proxy().unwrap();
    assert!(conn.read_conn(&mut [0u8; 1]).unwrap_err().is_no_proxy_protocol());
    t2.join().unwrap();

    let _r = Reset(default_policy());
    set_default_policy(Policy::Use);
    let (srv3, mut peer3) = net_pipe();
    let t3 = thread::spawn(move || { let _ = peer3.write_all(b"x"); });
    let mut buf = [0u8; 1];
    ProxyConn::new(srv3, vec![]).read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"x");
    t3.join().unwrap();
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::TestRejectPolicyAllowsHeaderlessConnection
#[test]
fn test_reject_policy_allows_headerless_connection() {
    let _g = GLOBAL_RW.read().unwrap();
    let (srv, mut peer) = net_pipe();
    let t = thread::spawn(move || { let _ = peer.write_all(b"raw"); });
    let mut pc = ProxyConn::new(srv, vec![with_policy(Policy::Reject)]);
    let mut buf = [0u8; 3];
    pc.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"raw");
    assert!(pc.proxy_header().is_none());
    t.join().unwrap();
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::TestNewConnSkipPolicyConsumesHeaderWithoutUsing
#[test]
fn test_new_conn_skip_policy_consumes_header_without_using() {
    let _g = GLOBAL_RW.read().unwrap();
    let (srv, mut peer) = net_pipe();
    let hdr = test_tcp_v4_header();
    let t = thread::spawn(move || {
        hdr.write_to(&mut peer).unwrap();
        peer.write_all(b"ping").unwrap();
    });
    let mut pc = ProxyConn::new(srv, vec![with_policy(Policy::Skip)]);
    let mut buf = [0u8; 4];
    pc.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"ping");
    assert!(pc.proxy_header().is_none());
    assert_eq!(pc.remote_addr_or_proxy().unwrap().to_string(), "pipe-remote");
    t.join().unwrap();
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::TestWithBufferSizeTooSmallForV1Header
#[test]
fn test_with_buffer_size_too_small_for_v1_header() {
    let _g = GLOBAL_RW.read().unwrap();
    let (srv, mut peer) = net_pipe();
    let t = thread::spawn(move || {
        let _ = peer.write_all(b"PROXY TCP4 1.2.3.4 5.6.7.8 80 443\r\nping");
    });
    let mut pc = ProxyConn::new(srv, vec![with_policy(Policy::Require), with_buffer_size(16)]);
    let err = pc.read_conn(&mut [0u8; 4]).unwrap_err();
    assert!(matches!(err, ProxyError::CantReadVersion1Header(_)), "got {err:?}");
    t.join().unwrap();
}

// Upstream: github.com/pires/go-proxyproto/protocol_test.go::TestTrustProxyHeaderFromListener
#[test]
fn test_trust_proxy_header_from_listener() {
    let _g = GLOBAL_RW.read().unwrap();
    {
        let mut pl = ProxyListener::new(TcpListener::bind("127.0.0.1:0").unwrap());
        pl.conn_policy = Some(trust_proxy_header_from(vec!["127.0.0.1".parse().unwrap()]));
        let addr = pl.listener.local_addr().unwrap();
        let cli = spawn_tcp_client(addr, None, Some(b"raw bytes, no header"), false);
        let mut conn = pl.accept().unwrap().into_proxy().unwrap();
        assert!(conn.read_conn(&mut [0u8; 1]).unwrap_err().is_no_proxy_protocol());
        cli.join().unwrap();
    }

    {
        let raw_l = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = raw_l.local_addr().unwrap();
        let mut pl = ProxyListener::new(ClosableListener::new_tcp(raw_l));
        pl.conn_policy = Some(trust_proxy_header_from(vec!["203.0.113.9".parse().unwrap()]));
        let pl = Arc::new(pl);
        let pl_c = Arc::clone(&pl);
        let (tx, rx) = std::sync::mpsc::channel();
        let accept_t = thread::spawn(move || { let _ = tx.send(pl_c.accept().err()); });

        let mut c = TcpStream::connect(addr).unwrap();
        c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut b = [0u8; 1];
        assert_eq!(c.read(&mut b).unwrap(), 0);
        drop(c);

        assert!(rx.try_recv().is_err());
        pl.close().unwrap();
        assert!(rx.recv_timeout(Duration::from_secs(2)).unwrap().is_some());
        accept_t.join().unwrap();
    }
}
