//! Ported upstream tests from github.com/pires/go-proxyproto:
//! - header_test.go (18)
//! - policy_test.go (15)
//! - udp_test.go (2)
//! - fuzz_test.go (6)

use std::any::Any;
use std::collections::VecDeque;
use std::io::{self, Cursor, Read, Write};
use std::net::IpAddr;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime};

use mimir_cmd::server::proxyproto::{
    conn_lax_whitelist_policy, conn_skip_proxy_header_for_cidr, conn_strict_whitelist_policy,
    ignore_proxy_header_not_on_interface, join_tlvs, lax_whitelist_policy, parse_udp_datagram,
    policy_from_ranges, read, read_header_timeout, read_timeout, skip_proxy_header_for_cidr,
    split_tlvs, strict_whitelist_policy, trust_proxy_header_from, trust_proxy_header_from_ranges,
    AddressFamilyAndProtocol, ConnPolicyOptions, Header, IpNet, PP2Type, Policy,
    ProtocolVersionAndCommand, ProxyAddr, ProxyBufReader, ProxyConn, ProxyError, StreamConn, Tlv,
    DEFAULT_READ_BUFFER_SIZE, LENGTH_UNIX, LENGTH_V4, SIGV2,
};

const TEST_SOURCE_IPV4_ADDR: &str = "10.1.1.1";
const TEST_DEST_IPV4_ADDR: &str = "20.2.2.2";
const TEST_LOCALHOST_IP4_ADDR: &str = "127.0.0.1";
const TEST_LOCALHOST_IP6_ADDR: &str = "::1";
const TEST_VALID_PORT: i32 = 65533;
const TEST_UNIX_SOCKET_NAME: &str = "socket";
const TEST_UNIX_SRC_NAME: &str = "src";
const TEST_UNIX_DST_NAME: &str = "dst";

const TEST_WHITELIST_IP1: &str = "10.0.0.2";
const TEST_WHITELIST_IP2: &str = "10.0.0.3";
const TEST_WHITELIST_IP3: &str = "10.0.0.4";
const TEST_WHITELIST_CIDR: &str = "10.0.0.0/30";
const TEST_MALFORMED_CIDR: &str = "20/80";
const TEST_MALFORMED_IP: &str = "855.222.233.11";
const TEST_WHITELIST_CIDR_V6: &str = "2001:db8::/64";

fn v4_addr() -> ProxyAddr {
    ProxyAddr::tcp(TEST_LOCALHOST_IP4_ADDR.parse().unwrap(), TEST_VALID_PORT)
}
fn v6_addr() -> ProxyAddr {
    ProxyAddr::tcp(TEST_LOCALHOST_IP6_ADDR.parse().unwrap(), TEST_VALID_PORT)
}
fn v4_udp_addr() -> ProxyAddr {
    ProxyAddr::udp(TEST_LOCALHOST_IP4_ADDR.parse().unwrap(), TEST_VALID_PORT)
}
fn unix_stream_addr() -> ProxyAddr {
    ProxyAddr::unix(TEST_UNIX_SOCKET_NAME)
}
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
fn v2_header(command: u8, transport: u8, payload: &[u8]) -> Vec<u8> {
    let mut b = Vec::with_capacity(16 + payload.len());
    b.extend_from_slice(SIGV2);
    b.push(command);
    b.push(transport);
    b.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    b.extend_from_slice(payload);
    b
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
                for (i, b) in out[..n].iter_mut().enumerate() {
                    *b = guard.buf.pop_front().unwrap_or(0);
                    let _ = i;
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
        if self.closed {
            return Err(io::Error::new(io::ErrorKind::BrokenPipe, "closed pipe"));
        }
        let (mtx, cv) = if self.is_a { (&self.inner.a_to_b, &self.inner.a_cv) } else { (&self.inner.b_to_a, &self.inner.b_cv) };
        let mut guard = mtx.lock().unwrap();
        if guard.closed {
            return Err(io::Error::new(io::ErrorKind::BrokenPipe, "closed pipe"));
        }
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
        {
            let mut g = self.inner.a_to_b.lock().unwrap();
            g.closed = true;
            self.inner.a_cv.notify_all();
        }
        {
            let mut g = self.inner.b_to_a.lock().unwrap();
            g.closed = true;
            self.inner.b_cv.notify_all();
        }
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

// --- header_test.go (18) ---

struct TimeoutReader;
impl Read for TimeoutReader {
    fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
        thread::sleep(Duration::from_millis(500));
        Ok(0)
    }
}

struct ErrorReader;
impl Read for ErrorReader {
    fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
        Err(io::Error::other("read is intentionally broken"))
    }
}

// Upstream: github.com/pires/go-proxyproto/header_test.go::TestReadTimeoutV1Invalid
#[test]
fn test_read_timeout_v1_invalid() {
    let r = ProxyBufReader::new(TimeoutReader);
    let (res, _) = read_timeout(r, Duration::from_millis(50));
    assert!(matches!(res.unwrap_err(), ProxyError::NoProxyProtocol));
}

// Upstream: github.com/pires/go-proxyproto/header_test.go::TestReadTimeoutPropagatesReadError
#[test]
fn test_read_timeout_propagates_read_error() {
    let r = ProxyBufReader::new(ErrorReader);
    let (res, _) = read_timeout(r, Duration::from_millis(50));
    let err = res.unwrap_err();
    assert!(err.to_string().contains("read is intentionally broken"), "{err}");
}

// Upstream: github.com/pires/go-proxyproto/header_test.go::TestReadHeaderTimeoutParsesHeaderAndPreservesPayload
#[test]
fn test_read_header_timeout_parses_header_and_preserves_payload() {
    let (mut client, server) = net_pipe();
    let payload = b"hello";
    let t = thread::spawn(move || {
        let _ = client.write_all(b"PROXY TCP4 127.0.0.1 127.0.0.2 12345 443\r\nhello");
        let _ = client.close();
    });
    let mut reader = ProxyBufReader::new(server);
    let h = read_header_timeout(&mut reader, Duration::from_secs(1)).unwrap();
    assert_eq!(h.version, 1);
    let mut got = [0u8; 5];
    let n = reader.read(&mut got).unwrap();
    assert_eq!(&got[..n], payload);
    t.join().unwrap();
}

// Upstream: github.com/pires/go-proxyproto/header_test.go::TestReadHeaderTimeoutCancelsStalledRead
#[test]
fn test_read_header_timeout_cancels_stalled_read() {
    let (_client, server) = net_pipe();
    let mut reader = ProxyBufReader::new(server);
    let start = Instant::now();
    let err = read_header_timeout(&mut reader, Duration::from_millis(50)).unwrap_err();
    assert!(matches!(err, ProxyError::NoProxyProtocol));
    assert!(start.elapsed() < Duration::from_secs(2));
}

// Upstream: github.com/pires/go-proxyproto/header_test.go::TestReadHeaderTimeoutNoHeaderPreservesData
#[test]
fn test_read_header_timeout_no_header_preserves_data() {
    let (mut client, server) = net_pipe();
    let want = b"GET / HTTP/1.1\r\n";
    let t = thread::spawn(move || {
        let _ = client.write_all(want);
        let _ = client.close();
    });
    let mut reader = ProxyBufReader::new(server);
    let err = read_header_timeout(&mut reader, Duration::from_secs(1)).unwrap_err();
    assert!(matches!(err, ProxyError::NoProxyProtocol));
    let mut got = vec![0u8; want.len()];
    let n = reader.read(&mut got).unwrap();
    assert_eq!(&got[..n], want);
    t.join().unwrap();
}

// Upstream: github.com/pires/go-proxyproto/header_test.go::TestReadHeaderTimeoutZeroTimeoutReadsWithoutDeadline
#[test]
fn test_read_header_timeout_zero_timeout_reads_without_deadline() {
    let (mut client, server) = net_pipe();
    let t = thread::spawn(move || {
        let _ = client.write_all(b"PROXY TCP4 10.1.1.1 20.2.2.2 1000 2000\r\n");
        let _ = client.close();
    });
    let mut reader = ProxyBufReader::new(server);
    let h = read_header_timeout(&mut reader, Duration::ZERO).unwrap();
    assert_eq!(h.source_addr.unwrap().to_string(), "10.1.1.1:1000");
    t.join().unwrap();
}

// Upstream: github.com/pires/go-proxyproto/header_test.go::TestReadHeaderTimeoutInitialDeadlineError
#[test]
fn test_read_header_timeout_initial_deadline_error() {
    let inner = DeadlineFailConn {
        r: Cursor::new(b"PROXY TCP4 10.1.1.1 20.2.2.2 1000 2000\r\n".to_vec()),
        set_calls: 0,
        fail_on_set: 1,
    };
    let mut reader = ProxyBufReader::new(inner);
    let err = read_header_timeout(&mut reader, Duration::from_secs(1)).unwrap_err();
    assert!(err.to_string().contains("set read deadline failed"));
    assert_eq!(reader.inner().set_calls, 1);
}

// Upstream: github.com/pires/go-proxyproto/header_test.go::TestReadHeaderTimeoutPropagatesParseError
#[test]
fn test_read_header_timeout_propagates_parse_error() {
    let (mut client, server) = net_pipe();
    let t = thread::spawn(move || {
        let _ = client.write_all(b"PROXY TCP4 10.1.1.1 20.2.2.2 notaport 2000\r\n");
        let _ = client.close();
    });
    let mut reader = ProxyBufReader::new(server);
    let err = read_header_timeout(&mut reader, Duration::from_secs(1)).unwrap_err();
    assert!(!err.is_no_proxy_protocol());
    t.join().unwrap();
}

// Upstream: github.com/pires/go-proxyproto/header_test.go::TestEqualsTo
#[test]
fn test_equals_to() {
    let src = ProxyAddr::tcp(TEST_SOURCE_IPV4_ADDR.parse().unwrap(), 1000);
    let dst = ProxyAddr::tcp(TEST_DEST_IPV4_ADDR.parse().unwrap(), 2000);
    let h1 = Header { version: 1, command: ProtocolVersionAndCommand::PROXY, transport_protocol: AddressFamilyAndProtocol::TCPV4, source_addr: Some(src.clone()), destination_addr: Some(dst.clone()), raw_tlvs: Vec::new() };
    let h2 = Header { version: 2, command: ProtocolVersionAndCommand::PROXY, transport_protocol: AddressFamilyAndProtocol::TCPV4, source_addr: Some(src.clone()), destination_addr: Some(dst.clone()), raw_tlvs: Vec::new() };
    assert!(!h1.equals_to(None));
    assert!(!h1.equals_to(Some(&h2)));
    assert!(h1.equals_to(Some(&h1.clone())));

    let p_hdr = Header { version: 2, command: ProtocolVersionAndCommand::PROXY, transport_protocol: AddressFamilyAndProtocol::TCPV4, source_addr: Some(v4_addr()), destination_addr: Some(v4_addr()), raw_tlvs: Vec::new() };
    let l_hdr = Header { version: 2, command: ProtocolVersionAndCommand::LOCAL, transport_protocol: AddressFamilyAndProtocol::TCPV4, source_addr: Some(v4_addr()), destination_addr: Some(v4_addr()), raw_tlvs: Vec::new() };
    let l_hdr6 = Header { version: 2, command: ProtocolVersionAndCommand::LOCAL, transport_protocol: AddressFamilyAndProtocol::TCPV4, source_addr: Some(v6_addr()), destination_addr: Some(v6_addr()), raw_tlvs: Vec::new() };
    assert!(!p_hdr.equals_to(Some(&l_hdr)));
    assert!(l_hdr.equals_to(Some(&l_hdr6)));
}

// Upstream: github.com/pires/go-proxyproto/header_test.go::TestEqualToDeprecatedWrapper
#[test]
fn test_equal_to_deprecated_wrapper() {
    let h = test_tcp_v4_header();
    assert!(h.equal_to(Some(&h)));
}

// Upstream: github.com/pires/go-proxyproto/header_test.go::TestGetters
#[test]
fn test_getters() {
    let src4 = ProxyAddr::tcp(TEST_SOURCE_IPV4_ADDR.parse().unwrap(), 1000);
    let dst4 = ProxyAddr::tcp(TEST_DEST_IPV4_ADDR.parse().unwrap(), 2000);
    let h_tcp = Header { version: 1, command: ProtocolVersionAndCommand::PROXY, transport_protocol: AddressFamilyAndProtocol::TCPV4, source_addr: Some(src4.clone()), destination_addr: Some(dst4.clone()), raw_tlvs: Vec::new() };
    assert_eq!(h_tcp.tcp_addrs(), Some((&src4, &dst4)));
    assert!(h_tcp.udp_addrs().is_none());
    assert!(h_tcp.unix_addrs().is_none());
    assert_eq!(h_tcp.ports(), Some((1000, 2000)));

    let usrc4 = ProxyAddr::udp(TEST_SOURCE_IPV4_ADDR.parse().unwrap(), 1000);
    let udst4 = ProxyAddr::udp(TEST_DEST_IPV4_ADDR.parse().unwrap(), 2000);
    let h_udp = Header { version: 2, command: ProtocolVersionAndCommand::PROXY, transport_protocol: AddressFamilyAndProtocol::UDPV4, source_addr: Some(usrc4.clone()), destination_addr: Some(udst4.clone()), raw_tlvs: Vec::new() };
    assert!(h_udp.tcp_addrs().is_none());
    assert_eq!(h_udp.udp_addrs(), Some((&usrc4, &udst4)));
    assert_eq!(h_udp.ports(), Some((1000, 2000)));

    let xsrc = ProxyAddr::unix(TEST_UNIX_SRC_NAME);
    let xdst = ProxyAddr::unix(TEST_UNIX_DST_NAME);
    let h_unix_s = Header { version: 2, command: ProtocolVersionAndCommand::PROXY, transport_protocol: AddressFamilyAndProtocol::UNIX_STREAM, source_addr: Some(xsrc.clone()), destination_addr: Some(xdst.clone()), raw_tlvs: Vec::new() };
    assert_eq!(h_unix_s.unix_addrs(), Some((&xsrc, &xdst)));
    assert!(h_unix_s.ips().is_none());
    assert!(h_unix_s.ports().is_none());

    let h_unix_d = Header { version: 2, command: ProtocolVersionAndCommand::PROXY, transport_protocol: AddressFamilyAndProtocol::UNIX_DATAGRAM, source_addr: Some(xsrc.clone()), destination_addr: Some(xdst.clone()), raw_tlvs: Vec::new() };
    assert_eq!(h_unix_d.unix_addrs(), Some((&xsrc, &xdst)));

    let h_unspec = Header { version: 1, command: ProtocolVersionAndCommand::PROXY, transport_protocol: AddressFamilyAndProtocol::UNSPEC, source_addr: None, destination_addr: None, raw_tlvs: Vec::new() };
    assert!(h_unspec.tcp_addrs().is_none());
    assert!(h_unspec.udp_addrs().is_none());
    assert!(h_unspec.unix_addrs().is_none());
    assert!(h_unspec.ips().is_none());
    assert!(h_unspec.ports().is_none());
}

// Upstream: github.com/pires/go-proxyproto/header_test.go::TestSetTLVs
#[test]
fn test_set_tlvs() {
    let mut h = test_tcp_v4_header();
    h.set_tlvs(&[Tlv { typ: PP2Type::AUTHORITY, value: b"example.org".to_vec() }]).unwrap();
    let mut big_val = vec![b'a'; 0xFFFF];
    big_val.extend_from_slice(b".example.org");
    assert!(h.set_tlvs(&[Tlv { typ: PP2Type::AUTHORITY, value: big_val }]).is_err());
}

// Upstream: github.com/pires/go-proxyproto/header_test.go::TestWriteTo
#[test]
fn test_write_to() {
    let mut buf = Vec::new();
    let valid = Header {
        version: 1,
        command: ProtocolVersionAndCommand::PROXY,
        transport_protocol: AddressFamilyAndProtocol::TCPV4,
        source_addr: Some(ProxyAddr::tcp(TEST_SOURCE_IPV4_ADDR.parse().unwrap(), 1000)),
        destination_addr: Some(ProxyAddr::tcp(TEST_DEST_IPV4_ADDR.parse().unwrap(), 2000)),
        raw_tlvs: Vec::new(),
    };
    valid.write_to(&mut buf).unwrap();

    let invalid = Header {
        version: 0,
        source_addr: valid.source_addr,
        destination_addr: valid.destination_addr,
        ..Default::default()
    };
    assert!(invalid.write_to(&mut buf).is_err());
}

// Upstream: github.com/pires/go-proxyproto/header_test.go::TestFormat
#[test]
fn test_format() {
    let valid = Header {
        version: 1,
        command: ProtocolVersionAndCommand::PROXY,
        transport_protocol: AddressFamilyAndProtocol::TCPV4,
        source_addr: Some(ProxyAddr::tcp(TEST_SOURCE_IPV4_ADDR.parse().unwrap(), 1000)),
        destination_addr: Some(ProxyAddr::tcp(TEST_DEST_IPV4_ADDR.parse().unwrap(), 2000)),
        raw_tlvs: Vec::new(),
    };
    valid.format().unwrap();
}

// Upstream: github.com/pires/go-proxyproto/header_test.go::TestFormatInvalid
#[test]
fn test_format_invalid() {
    let cases = [
        (Header { version: 3, command: ProtocolVersionAndCommand::PROXY, transport_protocol: AddressFamilyAndProtocol::TCPV4, source_addr: Some(v4_addr()), destination_addr: Some(v4_addr()), raw_tlvs: Vec::new() }, "unknown proxy protocol version"),
        (Header { version: 2, command: ProtocolVersionAndCommand::PROXY, transport_protocol: AddressFamilyAndProtocol::TCPV4, source_addr: Some(v4_udp_addr()), destination_addr: Some(v4_addr()), raw_tlvs: Vec::new() }, "invalid address"),
        (Header { version: 2, command: ProtocolVersionAndCommand::PROXY, transport_protocol: AddressFamilyAndProtocol::TCPV4, source_addr: Some(v4_addr()), destination_addr: Some(v6_addr()), raw_tlvs: Vec::new() }, "invalid address"),
        (Header { version: 2, command: ProtocolVersionAndCommand::PROXY, transport_protocol: AddressFamilyAndProtocol::UNIX_STREAM, source_addr: Some(v4_addr()), destination_addr: Some(unix_stream_addr()), raw_tlvs: Vec::new() }, "invalid address"),
        (Header { version: 1, command: ProtocolVersionAndCommand::PROXY, transport_protocol: AddressFamilyAndProtocol::TCPV4, source_addr: Some(v6_addr()), destination_addr: Some(v4_addr()), raw_tlvs: Vec::new() }, "invalid address"),
        (Header { version: 1, command: ProtocolVersionAndCommand::PROXY, transport_protocol: AddressFamilyAndProtocol::TCPV4, source_addr: Some(v4_udp_addr()), destination_addr: Some(v4_addr()), raw_tlvs: Vec::new() }, "invalid address"),
    ];
    for (h, substr) in cases {
        let err = h.format().unwrap_err();
        assert!(err.to_string().contains(substr), "got {err}");
    }
}

// Upstream: github.com/pires/go-proxyproto/header_test.go::TestHeaderProxyFromAddrs
#[test]
fn test_header_proxy_from_addrs() {
    let unspec = Header { version: 2, command: ProtocolVersionAndCommand::LOCAL, transport_protocol: AddressFamilyAndProtocol::UNSPEC, ..Default::default() };
    let src4 = ProxyAddr::tcp(TEST_SOURCE_IPV4_ADDR.parse().unwrap(), 1000);
    let dst4 = ProxyAddr::tcp(TEST_DEST_IPV4_ADDR.parse().unwrap(), 2000);
    let src6 = ProxyAddr::tcp("fde7::372".parse().unwrap(), 1000);
    let dst6 = ProxyAddr::tcp("fde7::1".parse().unwrap(), 2000);
    let usrc4 = ProxyAddr::udp(TEST_SOURCE_IPV4_ADDR.parse().unwrap(), 1000);
    let udst4 = ProxyAddr::udp(TEST_DEST_IPV4_ADDR.parse().unwrap(), 2000);
    let usrc6 = ProxyAddr::udp("fde7::372".parse().unwrap(), 1000);
    let udst6 = ProxyAddr::udp("fde7::1".parse().unwrap(), 2000);
    let unix_s = ProxyAddr::unix(TEST_UNIX_SRC_NAME);
    let unix_d = ProxyAddr::unix(TEST_UNIX_DST_NAME);
    let unixg_s = ProxyAddr::unixgram(TEST_UNIX_SRC_NAME);
    let unixg_d = ProxyAddr::unixgram(TEST_UNIX_DST_NAME);

    assert!(Header::proxy_from_addrs(0, &src4, &dst4).equals_to(Some(&Header { version: 2, command: ProtocolVersionAndCommand::PROXY, transport_protocol: AddressFamilyAndProtocol::TCPV4, source_addr: Some(src4.clone()), destination_addr: Some(dst4.clone()), raw_tlvs: Vec::new() })));
    assert!(Header::proxy_from_addrs(0, &src6, &dst6).equals_to(Some(&Header { version: 2, command: ProtocolVersionAndCommand::PROXY, transport_protocol: AddressFamilyAndProtocol::TCPV6, source_addr: Some(src6.clone()), destination_addr: Some(dst6.clone()), raw_tlvs: Vec::new() })));
    assert!(Header::proxy_from_addrs(0, &src4, &dst6).equals_to(Some(&Header { version: 2, command: ProtocolVersionAndCommand::PROXY, transport_protocol: AddressFamilyAndProtocol::TCPV6, source_addr: Some(src4.clone()), destination_addr: Some(dst6.clone()), raw_tlvs: Vec::new() })));
    assert!(Header::proxy_from_addrs(0, &usrc4, &udst4).equals_to(Some(&Header { version: 2, command: ProtocolVersionAndCommand::PROXY, transport_protocol: AddressFamilyAndProtocol::UDPV4, source_addr: Some(src4.clone()), destination_addr: Some(dst4.clone()), raw_tlvs: Vec::new() })));
    assert!(Header::proxy_from_addrs(0, &usrc6, &udst6).equals_to(Some(&Header { version: 2, command: ProtocolVersionAndCommand::PROXY, transport_protocol: AddressFamilyAndProtocol::UDPV6, source_addr: Some(src6), destination_addr: Some(dst6), raw_tlvs: Vec::new() })));
    assert!(Header::proxy_from_addrs(0, &unix_s, &unix_d).equals_to(Some(&Header { version: 2, command: ProtocolVersionAndCommand::PROXY, transport_protocol: AddressFamilyAndProtocol::UNIX_STREAM, source_addr: Some(unix_s.clone()), destination_addr: Some(unix_d), raw_tlvs: Vec::new() })));
    assert!(Header::proxy_from_addrs(0, &unixg_s, &unixg_d).equals_to(Some(&Header { version: 2, command: ProtocolVersionAndCommand::PROXY, transport_protocol: AddressFamilyAndProtocol::UNIX_DATAGRAM, source_addr: Some(unixg_s), destination_addr: Some(unixg_d.clone()), raw_tlvs: Vec::new() })));
    assert!(Header::proxy_from_addrs(1, &src4, &dst4).equals_to(Some(&Header { version: 1, command: ProtocolVersionAndCommand::PROXY, transport_protocol: AddressFamilyAndProtocol::TCPV4, source_addr: Some(src4.clone()), destination_addr: Some(dst4.clone()), raw_tlvs: Vec::new() })));
    assert!(Header::proxy_from_addrs(0, &ProxyAddr::Tcp { ip: None, port: 1000 }, &ProxyAddr::Tcp { ip: None, port: 2000 }).equals_to(Some(&unspec)));
    assert!(Header::proxy_from_addrs(0, &ProxyAddr::Udp { ip: None, port: 1000 }, &ProxyAddr::Udp { ip: None, port: 2000 }).equals_to(Some(&unspec)));
    assert!(Header::proxy_from_addrs(0, &src4, &udst4).equals_to(Some(&unspec)));
    assert!(Header::proxy_from_addrs(0, &usrc4, &dst4).equals_to(Some(&unspec)));
    assert!(Header::proxy_from_addrs(0, &unix_s, &dst4).equals_to(Some(&unspec)));
    assert!(Header::proxy_from_addrs(0, &unix_s, &unixg_d).equals_to(Some(&unspec)));
}

// Upstream: github.com/pires/go-proxyproto/header_test.go::TestEqualsToNilAddresses
#[test]
fn test_equals_to_nil_addresses() {
    let bare = Header { version: 2, command: ProtocolVersionAndCommand::PROXY, transport_protocol: AddressFamilyAndProtocol::TCPV4, ..Default::default() };
    let with_addrs = Header { version: 2, command: ProtocolVersionAndCommand::PROXY, transport_protocol: AddressFamilyAndProtocol::TCPV4, source_addr: Some(v4_addr()), destination_addr: Some(v4_addr()), raw_tlvs: Vec::new() };
    assert!(bare.equals_to(Some(&bare)));
    assert!(!bare.equals_to(Some(&with_addrs)));
    assert!(!with_addrs.equals_to(Some(&bare)));
}

struct PrefixErrReader {
    data: Vec<u8>,
    pos: usize,
}
impl Read for PrefixErrReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.pos >= self.data.len() {
            return Err(io::Error::other("boom"));
        }
        let n = buf.len().min(self.data.len() - self.pos);
        buf[..n].copy_from_slice(&self.data[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

// Upstream: github.com/pires/go-proxyproto/header_test.go::TestReadPropagatesPeekErrors
#[test]
fn test_read_propagates_peek_errors() {
    for prefix in [b"PR".as_slice(), &SIGV2[..6]] {
        let mut r = ProxyBufReader::new(PrefixErrReader { data: prefix.to_vec(), pos: 0 });
        let err = read(&mut r).unwrap_err();
        assert!(err.to_string().contains("boom"), "{err}");
    }
}

// --- policy_test.go (15) ---

fn failing_addr() -> ProxyAddr { ProxyAddr::other("failing", "failing") }
fn invalid_ip_addr() -> ProxyAddr { ProxyAddr::other("tcp", "999.999.999.999:1234") }
fn tcp_addr(s: &str) -> ProxyAddr { ProxyAddr::from(s.parse::<std::net::SocketAddr>().unwrap()) }

// Upstream: github.com/pires/go-proxyproto/policy_test.go::TestWhitelistPolicyReturnsErrorOnInvalidAddress
#[test]
fn test_whitelist_policy_returns_error_on_invalid_address() {
    let list = [TEST_WHITELIST_IP1, TEST_WHITELIST_IP2, TEST_WHITELIST_IP3, TEST_WHITELIST_CIDR];
    assert!(strict_whitelist_policy(&list).unwrap()(&failing_addr()).is_err());
    assert!(lax_whitelist_policy(&list).unwrap()(&failing_addr()).is_err());
}

// Upstream: github.com/pires/go-proxyproto/policy_test.go::TestWhitelistPolicyReturnsErrorOnInvalidIP
#[test]
fn test_whitelist_policy_returns_error_on_invalid_ip() {
    let opts = ConnPolicyOptions { upstream: invalid_ip_addr(), downstream: failing_addr() };
    assert!(conn_strict_whitelist_policy(&[TEST_WHITELIST_IP2]).unwrap()(&opts).is_err());
    assert!(conn_lax_whitelist_policy(&[TEST_WHITELIST_IP2]).unwrap()(&opts).is_err());
}

// Upstream: github.com/pires/go-proxyproto/policy_test.go::Test_CreateWhitelistPolicyWithInvalidCidrReturnsError
#[test]
fn test_create_whitelist_policy_with_invalid_cidr_returns_error() {
    assert!(strict_whitelist_policy(&[TEST_MALFORMED_CIDR]).is_err());
    assert!(lax_whitelist_policy(&[TEST_MALFORMED_CIDR]).is_err());
}

// Upstream: github.com/pires/go-proxyproto/policy_test.go::Test_CreateWhitelistPolicyWithInvalidIpAddressReturnsError
#[test]
fn test_create_whitelist_policy_with_invalid_ip_address_returns_error() {
    assert!(strict_whitelist_policy(&[TEST_MALFORMED_IP]).is_err());
    assert!(lax_whitelist_policy(&[TEST_MALFORMED_IP]).is_err());
}

// Upstream: github.com/pires/go-proxyproto/policy_test.go::TestSkipProxyHeaderForCIDR
#[test]
fn test_skip_proxy_header_for_cidr() {
    let cidr = IpNet::parse("192.0.2.1/24").unwrap();
    let f = skip_proxy_header_for_cidr(Some(cidr), Policy::Reject);
    assert_eq!(f(&tcp_addr("192.0.2.255:12345")).unwrap(), Policy::Skip);
    assert_eq!(f(&tcp_addr("8.8.8.8:12345")).unwrap(), Policy::Reject);
}

// Upstream: github.com/pires/go-proxyproto/policy_test.go::TestConnSkipProxyHeaderForCIDRReturnsErrorOnInvalidAddress
#[test]
fn test_conn_skip_proxy_header_for_cidr_returns_error_on_invalid_address() {
    let cidr = IpNet::parse("192.0.2.1/24").unwrap();
    let f = conn_skip_proxy_header_for_cidr(Some(cidr), Policy::Ignore);
    assert!(f(&ConnPolicyOptions { upstream: failing_addr(), downstream: failing_addr() }).is_err());
}

// Upstream: github.com/pires/go-proxyproto/policy_test.go::TestConnSkipProxyHeaderForCIDR
#[test]
fn test_conn_skip_proxy_header_for_cidr() {
    let cidr = IpNet::parse("192.0.2.1/24").unwrap();
    let f = conn_skip_proxy_header_for_cidr(Some(cidr), Policy::Reject);
    assert_eq!(f(&ConnPolicyOptions { upstream: tcp_addr("192.0.2.255:12345"), downstream: failing_addr() }).unwrap(), Policy::Skip);
    assert_eq!(f(&ConnPolicyOptions { upstream: tcp_addr("8.8.8.8:12345"), downstream: failing_addr() }).unwrap(), Policy::Reject);
}

// Upstream: github.com/pires/go-proxyproto/policy_test.go::TestConnWhitelistPolicies
#[test]
fn test_conn_whitelist_policies() {
    let cases = [
        (conn_strict_whitelist_policy(&[TEST_WHITELIST_IP2]).unwrap(), tcp_addr("10.0.0.3:45738"), tcp_addr("10.0.0.4:45738"), Policy::Use, Policy::Reject),
        (conn_lax_whitelist_policy(&[TEST_WHITELIST_IP2]).unwrap(), tcp_addr("10.0.0.3:45738"), tcp_addr("10.0.0.4:45738"), Policy::Use, Policy::Ignore),
        (conn_strict_whitelist_policy(&["2001:db8::5"]).unwrap(), tcp_addr("[2001:db8::5]:45738"), tcp_addr("[2001:db8::6]:45738"), Policy::Use, Policy::Reject),
        (conn_lax_whitelist_policy(&[TEST_WHITELIST_CIDR_V6]).unwrap(), tcp_addr("[2001:db8::17]:45738"), tcp_addr("[2001:db9::17]:45738"), Policy::Use, Policy::Ignore),
    ];
    for (p, allowed, denied, want_use, want_rej) in cases {
        assert_eq!(p(&ConnPolicyOptions { upstream: allowed, downstream: failing_addr() }).unwrap(), want_use);
        assert_eq!(p(&ConnPolicyOptions { upstream: denied, downstream: failing_addr() }).unwrap(), want_rej);
    }
}

// Upstream: github.com/pires/go-proxyproto/policy_test.go::TestTrustProxyHeaderFrom
#[test]
fn test_trust_proxy_header_from() {
    let up = tcp_addr("10.0.0.3:45738");
    let up6 = tcp_addr("[2001:db8::5]:45738");
    assert!(trust_proxy_header_from(vec!["192.0.2.1".parse().unwrap()])(&ConnPolicyOptions { upstream: up.clone(), downstream: failing_addr() }).unwrap_err().is_invalid_upstream());
    assert_eq!(trust_proxy_header_from(vec!["10.0.0.3".parse().unwrap()])(&ConnPolicyOptions { upstream: up.clone(), downstream: failing_addr() }).unwrap(), Policy::Require);
    assert_eq!(trust_proxy_header_from(vec!["192.0.2.1".parse().unwrap(), "10.0.0.3".parse().unwrap()])(&ConnPolicyOptions { upstream: up, downstream: failing_addr() }).unwrap(), Policy::Require);
    assert_eq!(trust_proxy_header_from(vec!["2001:db8::5".parse().unwrap()])(&ConnPolicyOptions { upstream: up6, downstream: failing_addr() }).unwrap(), Policy::Require);
    assert!(trust_proxy_header_from(vec!["10.0.0.3".parse().unwrap()])(&ConnPolicyOptions { upstream: failing_addr(), downstream: failing_addr() }).unwrap_err().is_invalid_upstream());
}

// Upstream: github.com/pires/go-proxyproto/policy_test.go::TestIgnoreProxyHeaderNotOnInterface
#[test]
fn test_ignore_proxy_header_not_on_interface() {
    let down = tcp_addr("10.0.0.3:45738");
    assert_eq!(ignore_proxy_header_not_on_interface("192.0.2.1".parse().unwrap())(&ConnPolicyOptions { upstream: failing_addr(), downstream: down.clone() }).unwrap(), Policy::Ignore);
    assert_eq!(ignore_proxy_header_not_on_interface("10.0.0.3".parse().unwrap())(&ConnPolicyOptions { upstream: failing_addr(), downstream: down }).unwrap(), Policy::Use);
    assert!(ignore_proxy_header_not_on_interface("10.0.0.3".parse().unwrap())(&ConnPolicyOptions { upstream: failing_addr(), downstream: failing_addr() }).is_err());
}

// Upstream: github.com/pires/go-proxyproto/policy_test.go::TestBuiltinPoliciesWrapAddrErrorsAsInvalidUpstream
#[test]
fn test_builtin_policies_wrap_addr_errors_as_invalid_upstream() {
    let unix_addr = ProxyAddr::unix("/tmp/app.sock");
    let opts = ConnPolicyOptions { upstream: unix_addr.clone(), downstream: unix_addr };
    let cidr = IpNet::parse("10.0.0.0/8").unwrap();
    let policies = [
        conn_skip_proxy_header_for_cidr(Some(cidr), Policy::Use),
        conn_lax_whitelist_policy(&[TEST_WHITELIST_IP1]).unwrap(),
        policy_from_ranges(&[TEST_WHITELIST_CIDR], Policy::Use, Policy::Ignore).unwrap(),
        trust_proxy_header_from(vec![TEST_WHITELIST_IP1.parse().unwrap()]),
        trust_proxy_header_from_ranges(&[TEST_WHITELIST_CIDR]).unwrap(),
        ignore_proxy_header_not_on_interface(TEST_WHITELIST_IP1.parse().unwrap()),
    ];
    for p in policies {
        let err = p(&opts).unwrap_err();
        assert!(err.is_invalid_upstream(), "expected InvalidUpstream, got {err:?}");
    }
}

// Upstream: github.com/pires/go-proxyproto/policy_test.go::TestWhiteListPolicyFuncs
#[test]
fn test_white_list_policy_funcs() {
    let list = [TEST_WHITELIST_IP2, TEST_WHITELIST_CIDR];
    let strict = strict_whitelist_policy(&list).unwrap();
    let lax = lax_whitelist_policy(&list).unwrap();
    assert_eq!(strict(&tcp_addr("10.0.0.3:45738")).unwrap(), Policy::Use);
    assert_eq!(strict(&tcp_addr("10.0.0.1:45738")).unwrap(), Policy::Use);
    assert_eq!(strict(&tcp_addr("10.0.0.5:45738")).unwrap(), Policy::Reject);
    assert_eq!(lax(&tcp_addr("10.0.0.3:45738")).unwrap(), Policy::Use);
    assert_eq!(lax(&tcp_addr("10.0.0.1:45738")).unwrap(), Policy::Use);
    assert_eq!(lax(&tcp_addr("10.0.0.5:45738")).unwrap(), Policy::Ignore);
}

// Upstream: github.com/pires/go-proxyproto/policy_test.go::TestMustWhiteListPolicyPanicsOnInvalidInput
#[test]
fn test_must_white_list_policy_panics_on_invalid_input() {
    let checks = [
        lax_whitelist_policy(&[TEST_MALFORMED_IP]).is_err(),
        lax_whitelist_policy(&[TEST_MALFORMED_CIDR]).is_err(),
        strict_whitelist_policy(&[TEST_MALFORMED_IP]).is_err(),
        strict_whitelist_policy(&[TEST_MALFORMED_CIDR]).is_err(),
        conn_lax_whitelist_policy(&[TEST_MALFORMED_IP]).is_err(),
        conn_strict_whitelist_policy(&[TEST_MALFORMED_CIDR]).is_err(),
        policy_from_ranges(&[TEST_MALFORMED_IP], Policy::Use, Policy::Ignore).is_err(),
        policy_from_ranges(&[TEST_MALFORMED_CIDR], Policy::Require, Policy::Ignore).is_err(),
    ];
    for is_err in checks {
        assert!(is_err);
    }
}

// Upstream: github.com/pires/go-proxyproto/policy_test.go::TestTrustProxyHeaderFromRanges
#[test]
fn test_trust_proxy_header_from_ranges() {
    let policy = trust_proxy_header_from_ranges(&["10.0.0.10", "192.0.2.0/24", TEST_WHITELIST_CIDR_V6]).unwrap();
    assert_eq!(policy(&ConnPolicyOptions { upstream: tcp_addr("10.0.0.10:45738"), downstream: failing_addr() }).unwrap(), Policy::Require);
    assert_eq!(policy(&ConnPolicyOptions { upstream: tcp_addr("192.0.2.200:45738"), downstream: failing_addr() }).unwrap(), Policy::Require);
    assert_eq!(policy(&ConnPolicyOptions { upstream: tcp_addr("[2001:db8::17]:45738"), downstream: failing_addr() }).unwrap(), Policy::Require);
    assert!(policy(&ConnPolicyOptions { upstream: tcp_addr("10.0.0.11:45738"), downstream: failing_addr() }).unwrap_err().is_invalid_upstream());
    assert!(policy(&ConnPolicyOptions { upstream: tcp_addr("[2001:db9::17]:45738"), downstream: failing_addr() }).unwrap_err().is_invalid_upstream());
    assert!(policy(&ConnPolicyOptions { upstream: failing_addr(), downstream: failing_addr() }).unwrap_err().is_invalid_upstream());
    assert!(trust_proxy_header_from_ranges(&[TEST_MALFORMED_IP]).is_err());
    assert!(trust_proxy_header_from_ranges(&[TEST_MALFORMED_CIDR]).is_err());
}

// Upstream: github.com/pires/go-proxyproto/policy_test.go::TestPolicyFromRanges
#[test]
fn test_policy_from_ranges() {
    let ranges = [TEST_WHITELIST_IP2, "192.0.2.0/24", TEST_WHITELIST_CIDR_V6];
    let p_lax = policy_from_ranges(&ranges, Policy::Use, Policy::Ignore).unwrap();
    assert_eq!(p_lax(&ConnPolicyOptions { upstream: tcp_addr("10.0.0.3:45738"), downstream: failing_addr() }).unwrap(), Policy::Use);
    assert_eq!(p_lax(&ConnPolicyOptions { upstream: tcp_addr("10.0.0.5:45738"), downstream: failing_addr() }).unwrap(), Policy::Ignore);
    let p_strict = policy_from_ranges(&ranges, Policy::Use, Policy::Reject).unwrap();
    assert_eq!(p_strict(&ConnPolicyOptions { upstream: tcp_addr("192.0.2.7:45738"), downstream: failing_addr() }).unwrap(), Policy::Use);
    assert_eq!(p_strict(&ConnPolicyOptions { upstream: tcp_addr("10.0.0.5:45738"), downstream: failing_addr() }).unwrap(), Policy::Reject);
    let p_mixed = policy_from_ranges(&ranges, Policy::Require, Policy::Ignore).unwrap();
    assert_eq!(p_mixed(&ConnPolicyOptions { upstream: tcp_addr("[2001:db8::17]:45738"), downstream: failing_addr() }).unwrap(), Policy::Require);
    assert_eq!(p_mixed(&ConnPolicyOptions { upstream: tcp_addr("10.0.0.5:45738"), downstream: failing_addr() }).unwrap(), Policy::Ignore);
    assert!(p_lax(&ConnPolicyOptions { upstream: failing_addr(), downstream: failing_addr() }).unwrap_err().is_invalid_upstream());
    assert!(policy_from_ranges(&[TEST_MALFORMED_IP], Policy::Use, Policy::Ignore).is_err());
    assert!(policy_from_ranges(&[TEST_MALFORMED_CIDR], Policy::Use, Policy::Ignore).is_err());
}

// --- udp_test.go (2) ---

// Upstream: github.com/pires/go-proxyproto/udp_test.go::TestParseUDPDatagram
#[test]
fn test_parse_udp_datagram() {
    let payload = b"proxied application datagram";

    let udp_header = Header {
        version: 2,
        command: ProtocolVersionAndCommand::PROXY,
        transport_protocol: AddressFamilyAndProtocol::UDPV4,
        source_addr: Some(ProxyAddr::udp("10.1.1.1".parse().unwrap(), 1000)),
        destination_addr: Some(ProxyAddr::udp("20.2.2.2".parse().unwrap(), 2000)),
        raw_tlvs: Vec::new(),
    };
    let dgram = udp_header.format_udp_datagram(payload).unwrap();
    let (h, rest) = parse_udp_datagram(&dgram).unwrap();
    assert!(h.equals_to(Some(&udp_header)));
    assert_eq!(rest, payload);

    let mut d2 = v2_header(ProtocolVersionAndCommand::LOCAL.0, AddressFamilyAndProtocol::UNSPEC.0, &[]);
    d2.extend_from_slice(payload);
    let (h, rest) = parse_udp_datagram(&d2).unwrap();
    assert!(h.command.is_local());
    assert_eq!(rest, payload);

    let mut d1 = b"PROXY TCP4 1.2.3.4 5.6.7.8 80 443\r\n".to_vec();
    d1.extend_from_slice(payload);
    let (h, rest) = parse_udp_datagram(&d1).unwrap();
    assert_eq!(h.version, 1);
    assert_eq!(rest, payload);

    let d_empty = v2_header(ProtocolVersionAndCommand::PROXY.0, AddressFamilyAndProtocol::UDPV4.0, &vec![0u8; usize::from(LENGTH_V4)]);
    let (_, rest) = parse_udp_datagram(&d_empty).unwrap();
    assert!(rest.is_empty());

    assert!(matches!(parse_udp_datagram(b"just application data").unwrap_err(), ProxyError::NoProxyProtocol));
    assert!(matches!(parse_udp_datagram(&[]).unwrap_err(), ProxyError::NoProxyProtocol));

    let trunc = [SIGV2.as_slice(), &[ProtocolVersionAndCommand::PROXY.0, AddressFamilyAndProtocol::UDPV4.0], &LENGTH_V4.to_be_bytes()].concat();
    assert!(parse_udp_datagram(&trunc).is_err());
}

// Upstream: github.com/pires/go-proxyproto/udp_test.go::TestFormatUDPDatagramError
#[test]
fn test_format_udp_datagram_error() {
    let h = Header { version: 9, ..Default::default() };
    assert!(matches!(h.format_udp_datagram(b"payload").unwrap_err(), ProxyError::UnknownProxyProtocolVersion));
}

// --- fuzz_test.go (6) ---

fn must_not_panic_addr(a: Option<&ProxyAddr>) {
    if let Some(addr) = a {
        let _ = addr.to_string();
        let _ = addr.network();
    }
}

// Upstream: github.com/pires/go-proxyproto/fuzz_test.go::TestV2UndefinedTransportRejected
#[test]
fn test_v2_undefined_transport_rejected() {
    let payload = vec![0u8; usize::from(LENGTH_UNIX)];
    for b in 0u8..=255u8 {
        if AddressFamilyAndProtocol(b).is_supported() {
            continue;
        }
        for cmd in [ProtocolVersionAndCommand::PROXY, ProtocolVersionAndCommand::LOCAL] {
            let raw = v2_header(cmd.0, b, &payload);
            let mut r = ProxyBufReader::new_size(Cursor::new(raw), DEFAULT_READ_BUFFER_SIZE);
            let err = read(&mut r).unwrap_err();
            assert!(matches!(err, ProxyError::UnsupportedAddressFamilyAndProtocol), "cmd={:#x} b={b:#x} err={err:?}", cmd.0);
        }
    }
}

// Upstream: github.com/pires/go-proxyproto/fuzz_test.go::TestParseV2LocalAddressBlock
#[test]
fn test_parse_v2_local_address_block() {
    let local_tlv = [0x01u8, 0x00, 0x01, 0xAA];
    let full_v4 = vec![0u8; usize::from(LENGTH_V4)];
    let mut short_block = local_tlv.to_vec();
    short_block.resize(usize::from(LENGTH_V4) - 1, 0);

    type LocalCase<'a> = (&'a str, u8, Vec<u8>, AddressFamilyAndProtocol, bool, Vec<u8>);
    let cases: Vec<LocalCase<'_>> = vec![
        ("UNSPEC, empty", AddressFamilyAndProtocol::UNSPEC.0, Vec::new(), AddressFamilyAndProtocol::UNSPEC, false, Vec::new()),
        ("UNSPEC, TLV preserved", AddressFamilyAndProtocol::UNSPEC.0, local_tlv.to_vec(), AddressFamilyAndProtocol::UNSPEC, false, local_tlv.to_vec()),
        ("TCPv4 full", AddressFamilyAndProtocol::TCPV4.0, full_v4.clone(), AddressFamilyAndProtocol::TCPV4, true, Vec::new()),
        ("UDPv4 full", AddressFamilyAndProtocol::UDPV4.0, full_v4.clone(), AddressFamilyAndProtocol::UDPV4, true, Vec::new()),
        ("TCPv4 full + TLV", AddressFamilyAndProtocol::TCPV4.0, [full_v4.as_slice(), &local_tlv].concat(), AddressFamilyAndProtocol::TCPV4, true, local_tlv.to_vec()),
        ("TCPv4 len 0 normalized", AddressFamilyAndProtocol::TCPV4.0, Vec::new(), AddressFamilyAndProtocol::UNSPEC, false, Vec::new()),
        ("TCPv4 short skipped", AddressFamilyAndProtocol::TCPV4.0, short_block, AddressFamilyAndProtocol::UNSPEC, false, Vec::new()),
    ];
    for (desc, transport, payload, want_tp, want_addrs, want_tlvs) in cases {
        let raw = v2_header(ProtocolVersionAndCommand::LOCAL.0, transport, &payload);
        let mut r = ProxyBufReader::new(Cursor::new(raw));
        let h = read(&mut r).expect(desc);
        assert!(h.command.is_local(), "{desc}");
        assert_eq!(h.transport_protocol, want_tp, "{desc}");
        assert_eq!(h.source_addr.is_some() && h.destination_addr.is_some(), want_addrs, "{desc}");
        assert_eq!(h.raw_tlvs, want_tlvs, "{desc}");
        must_not_panic_addr(h.source_addr.as_ref());
        must_not_panic_addr(h.destination_addr.as_ref());
        h.format().expect(desc);
    }
}

// Upstream: github.com/pires/go-proxyproto/fuzz_test.go::TestParseV2RoundTrip
#[test]
fn test_parse_v2_round_trip() {
    let tlv = [0x01u8, 0x00, 0x01, 0xAA];
    let v4_block = [1u8, 2, 3, 4, 5, 6, 7, 8, 0, 80, 1, 187];
    let mut unix_block = vec![0u8; usize::from(LENGTH_UNIX)];
    unix_block[..13].copy_from_slice(b"/tmp/src.sock");
    unix_block[108..121].copy_from_slice(b"/tmp/dst.sock");

    for (cmd, tp, block) in [
        (ProtocolVersionAndCommand::LOCAL, AddressFamilyAndProtocol::TCPV4, v4_block.as_slice()),
        (ProtocolVersionAndCommand::LOCAL, AddressFamilyAndProtocol::UNIX_STREAM, unix_block.as_slice()),
        (ProtocolVersionAndCommand::PROXY, AddressFamilyAndProtocol::UNIX_STREAM, unix_block.as_slice()),
    ] {
        let raw = v2_header(cmd.0, tp.0, &[block, &tlv].concat());
        let mut r = ProxyBufReader::new(Cursor::new(raw.clone()));
        let h = read(&mut r).unwrap();
        let got = h.format().unwrap();
        assert_eq!(got, raw);
    }
}

// Upstream: github.com/pires/go-proxyproto/fuzz_test.go::TestParsedHeaderNeverHasNilProxyAddr
#[test]
fn test_parsed_header_never_has_nil_proxy_addr() {
    let check = |raw: Vec<u8>| {
        let mut r = ProxyBufReader::new(Cursor::new(raw));
        let Ok(h) = read(&mut r) else { return; };
        if h.command.is_proxy() && h.transport_protocol != AddressFamilyAndProtocol::UNSPEC {
            assert!(h.source_addr.is_some() && h.destination_addr.is_some());
        }
        must_not_panic_addr(h.source_addr.as_ref());
        must_not_panic_addr(h.destination_addr.as_ref());
    };
    for b in 0u8..=255u8 {
        check(v2_header(ProtocolVersionAndCommand::PROXY.0, b, &vec![0u8; usize::from(LENGTH_UNIX)]));
        check(v2_header(ProtocolVersionAndCommand::LOCAL.0, b, &vec![0u8; usize::from(LENGTH_UNIX)]));
    }
}

// Upstream: github.com/pires/go-proxyproto/fuzz_test.go::TestRemoteAddrNoPanicOnCraftedTransport
#[test]
fn test_remote_addr_no_panic_on_crafted_transport() {
    let (conn, mut peer) = net_pipe();
    let raw = v2_header(ProtocolVersionAndCommand::PROXY.0, 0x13, &vec![0u8; usize::from(LENGTH_V4)]);
    let t = thread::spawn(move || {
        let _ = peer.write_all(&raw);
        let _ = peer.close();
    });
    let mut c = ProxyConn::new(conn, vec![]);
    let r = c.remote_addr_or_proxy().unwrap();
    assert!(!r.to_string().is_empty());
    let l = c.local_addr_or_proxy().unwrap();
    assert!(!l.to_string().is_empty());
    t.join().unwrap();
}

// Upstream: github.com/pires/go-proxyproto/fuzz_test.go::TestAddrAccessorsNilGuard
#[test]
fn test_addr_accessors_nil_guard() {
    let (conn, _peer) = net_pipe();
    let expected_remote = conn.remote_addr().unwrap();
    let expected_local = conn.local_addr().unwrap();
    let h = Header {
        version: 2,
        command: ProtocolVersionAndCommand::PROXY,
        transport_protocol: AddressFamilyAndProtocol::TCPV4,
        source_addr: None,
        destination_addr: None,
        raw_tlvs: Vec::new(),
    };
    let mut c = ProxyConn::from_pre_parsed(conn, Some(h));
    assert_eq!(c.remote_addr_or_proxy().unwrap(), expected_remote);
    assert_eq!(c.local_addr_or_proxy().unwrap(), expected_local);
}

// Extra sanity check for split_tlvs/join_tlvs & IpAddr compile check
#[test]
fn test_tlv_roundtrip_smoke() {
    let _ip: IpAddr = "127.0.0.1".parse().unwrap();
    let tlvs = vec![Tlv { typ: PP2Type::AUTHORITY, value: b"example.org".to_vec() }];
    let raw = join_tlvs(&tlvs).unwrap();
    assert_eq!(split_tlvs(&raw).unwrap(), tlvs);
}
