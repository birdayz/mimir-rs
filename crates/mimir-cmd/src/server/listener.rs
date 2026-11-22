//! TCP listeners for the HTTP and gRPC servers.
//!
//! Ported from:
//! - github.com/grafana/dskit/server/server.go (`net.Listen(network, JoinHostPort(addr, port))`,
//!   `middleware.CountingListener`, `netutil.LimitListener`, `newProxyProtocolListener`)
//! - Go `net` listen semantics: for network `tcp`, an empty host or a wildcard (`0.0.0.0`, `::`)
//!   listens dual-stack on `[::]` (IPv4-mapped addresses enabled); `tcp4` / `tcp6` restrict the
//!   family. Listen backlog is `/proc/sys/net/core/somaxconn` (Go `maxListenerBacklog`),
//!   `SO_REUSEADDR` is set, and accepted connections get `TCP_NODELAY` and Go's default
//!   keep-alive (15s idle, 15s interval, 9 probes).
//! - github.com/pires/go-proxyproto (PROXY v1/v2 header parsing with the `USE` policy dskit
//!   configures: connections without a header are accepted as-is).

use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use prometheus::Gauge;
use socket2::{Domain, Protocol, SockAddr, Socket, TcpKeepalive, Type};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Binds a listener with Go `net.Listen(network, net.JoinHostPort(address, port))` semantics.
pub fn listen(network: &str, address: &str, port: i64) -> Result<std::net::TcpListener> {
    let network = if network.is_empty() { "tcp" } else { network };
    let port = u16::try_from(port).with_context(|| format!("invalid listen port {port}"))?;
    let hostport = join_host_port(address, port);
    if !matches!(network, "tcp" | "tcp4" | "tcp6") {
        bail!("listen {network}: unknown network {network}");
    }

    let wildcard = address.is_empty()
        || address
            .parse::<IpAddr>()
            .map(|ip| ip.is_unspecified())
            .unwrap_or(false);

    // Go `favoriteAddrFamily`: listening on a wildcard address with network "tcp" uses an
    // AF_INET6 socket with IPV6_V6ONLY=0 (dual-stack), whatever the wildcard's family.
    let (addr, dual_stack) = if wildcard {
        match network {
            "tcp4" => (SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), port), false),
            "tcp6" => (SocketAddr::new(Ipv6Addr::UNSPECIFIED.into(), port), false),
            _ => (SocketAddr::new(Ipv6Addr::UNSPECIFIED.into(), port), true),
        }
    } else {
        let candidates: Vec<SocketAddr> = match address.parse::<IpAddr>() {
            Ok(ip) => vec![SocketAddr::new(ip, port)],
            Err(_) => (address, port)
                .to_socket_addrs()
                .with_context(|| format!("listen {network} {hostport}: lookup {address}"))?
                .collect(),
        };
        // Go's "tcp" resolution prefers IPv4 addresses for listening.
        let pick = |want_v4: Option<bool>| {
            candidates
                .iter()
                .find(|a| want_v4.map(|v4| a.is_ipv4() == v4).unwrap_or(true))
                .copied()
        };
        let a = match network {
            "tcp4" => pick(Some(true)),
            "tcp6" => pick(Some(false)),
            _ => pick(Some(true)).or_else(|| pick(None)),
        };
        let Some(a) = a else {
            bail!("listen {network} {hostport}: no suitable address found");
        };
        (a, false)
    };

    match bind_socket(addr, dual_stack, network == "tcp6") {
        Ok(l) => Ok(l),
        Err(e) if dual_stack && addr.is_ipv6() => {
            // No IPv6 support on this host: Go falls back to an IPv4 wildcard listener.
            let v4 = SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), port);
            bind_socket(v4, false, false)
                .with_context(|| format!("listen {network} {hostport}: {e}"))
        }
        Err(e) => Err(anyhow::anyhow!("listen {network} {hostport}: {e}")),
    }
}

fn join_host_port(host: &str, port: u16) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

fn somaxconn() -> i32 {
    std::fs::read_to_string("/proc/sys/net/core/somaxconn")
        .ok()
        .and_then(|s| s.trim().parse::<i32>().ok())
        .filter(|n| *n > 0)
        // Go caps at 1<<16-1 for kernels < 4.1; use the same cap (u16 backlog field).
        .map(|n| n.min(65535))
        .unwrap_or(128)
}

fn bind_socket(addr: SocketAddr, dual_stack: bool, v6_only: bool) -> io::Result<std::net::TcpListener> {
    let domain = if addr.is_ipv6() { Domain::IPV6 } else { Domain::IPV4 };
    let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
    if addr.is_ipv6() {
        socket.set_only_v6(v6_only || !dual_stack)?;
    }
    socket.set_reuse_address(true)?;
    socket.set_nonblocking(true)?;
    socket.bind(&SockAddr::from(addr))?;
    socket.listen(somaxconn())?;
    Ok(socket.into())
}

/// Options applied to every accepted connection.
#[derive(Clone)]
pub struct AcceptOptions {
    /// `-server.{http,grpc}-conn-limit` (0 = unlimited).
    pub conn_limit: usize,
    /// `cortex_tcp_connections{protocol=...}`.
    pub connections_gauge: Gauge,
    pub proxy_protocol: bool,
    /// go-proxyproto `ReadHeaderTimeout` (dskit passes `-server.http-read-header-timeout`; 0
    /// means go-proxyproto's default of 10s).
    pub proxy_header_timeout: Duration,
}

/// An accepted connection holding its connection-limit slot and connection count until dropped.
pub struct Accepted {
    pub stream: TcpStream,
    pub remote_addr: SocketAddr,
    pub local_addr: SocketAddr,
    /// Bytes already consumed while probing for a PROXY header that belong to the stream.
    pub prefix: Vec<u8>,
    pub guard: ConnGuard,
}

pub struct ConnGuard {
    _permit: Option<OwnedSemaphorePermit>,
    gauge: Gauge,
}

impl Drop for ConnGuard {
    fn drop(&mut self) {
        self.gauge.dec();
    }
}

/// Accept loop state (Go `LimitListener(CountingListener(listener))`).
pub struct Acceptor {
    listener: TcpListener,
    opts: AcceptOptions,
    limit: Option<Arc<Semaphore>>,
}

/// Minimal listener/conn traits mirroring Go `net.Listener` / `net.Conn` for [`CountingListener`].
pub trait NetConn: Send {
    fn close(&mut self) -> io::Result<()>;
}

pub trait NetListener: Send {
    type Conn: NetConn;
    fn accept_conn(&mut self) -> io::Result<Self::Conn>;
}

/// Ported from `github.com/grafana/dskit/middleware/counting_listener.go`.
pub struct CountingListener<L> {
    inner: L,
    gauge: Gauge,
}

impl<L> CountingListener<L> {
    pub fn new(inner: L, gauge: Gauge) -> Self {
        Self { inner, gauge }
    }

    pub fn accept(&mut self) -> io::Result<CountingConn<L::Conn>>
    where
        L: NetListener,
    {
        let conn = self.inner.accept_conn()?;
        self.gauge.inc();
        Ok(CountingConn {
            inner: conn,
            gauge: self.gauge.clone(),
            closed: std::sync::atomic::AtomicBool::new(false),
        })
    }
}

pub struct CountingConn<C> {
    inner: C,
    gauge: Gauge,
    closed: std::sync::atomic::AtomicBool,
}

impl<C: NetConn> CountingConn<C> {
    pub fn close(&mut self) -> io::Result<()> {
        let res = self.inner.close();
        if !self.closed.swap(true, std::sync::atomic::Ordering::AcqRel) {
            self.gauge.dec();
        }
        res
    }
}

impl Acceptor {
    pub fn new(listener: TcpListener, opts: AcceptOptions) -> Self {
        let limit = (opts.conn_limit > 0).then(|| Arc::new(Semaphore::new(opts.conn_limit)));
        Self {
            listener,
            opts,
            limit,
        }
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Waits for a connection slot (when limited), then accepts the next connection.
    pub async fn accept(&self) -> io::Result<(TcpStream, SocketAddr, ConnGuard)> {
        let permit = match &self.limit {
            Some(sem) => Some(
                Arc::clone(sem)
                    .acquire_owned()
                    .await
                    .map_err(|_| io::Error::other("connection limiter closed"))?,
            ),
            None => None,
        };
        loop {
            match self.listener.accept().await {
                Ok((stream, addr)) => {
                    let _ = stream.set_nodelay(true);
                    let sock = socket2::SockRef::from(&stream);
                    let _ = sock.set_tcp_keepalive(
                        &TcpKeepalive::new()
                            .with_time(Duration::from_secs(15))
                            .with_interval(Duration::from_secs(15))
                            .with_retries(9),
                    );
                    self.opts.connections_gauge.inc();
                    return Ok((
                        stream,
                        addr,
                        ConnGuard {
                            _permit: permit,
                            gauge: self.opts.connections_gauge.clone(),
                        },
                    ));
                }
                Err(e) if is_transient_accept_error(&e) => {
                    // Go's http.Server / grpc.Server back off and retry on temporary errors.
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
    }

    pub fn options(&self) -> &AcceptOptions {
        &self.opts
    }
}

fn is_transient_accept_error(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::Interrupted
            | io::ErrorKind::WouldBlock
    ) || matches!(e.raw_os_error(), Some(libc::EMFILE | libc::ENFILE | libc::ENOBUFS | libc::ENOMEM))
}

/// Completes per-connection setup that must not block the accept loop (PROXY header).
pub async fn finish_accept(
    stream: TcpStream,
    remote_addr: SocketAddr,
    guard: ConnGuard,
    opts: &AcceptOptions,
) -> io::Result<Accepted> {
    let local_addr = stream.local_addr()?;
    let mut accepted = Accepted {
        stream,
        remote_addr,
        local_addr,
        prefix: Vec::new(),
        guard,
    };
    if opts.proxy_protocol {
        let timeout = if opts.proxy_header_timeout.is_zero() {
            super::proxyproto::default_read_header_timeout()
        } else {
            opts.proxy_header_timeout
        };
        let mut reader = super::proxyproto::ProxyAsyncBufReader::new(accepted.stream);
        let res = super::proxyproto::read_header_timeout_async(&mut reader, timeout).await;
        let (stream, buffered) = reader.into_inner_and_buffered();
        accepted.stream = stream;
        accepted.prefix = buffered;
        match res {
            Ok(hdr) => {
                if !hdr.command.is_local() {
                    if let Some(src) = hdr.source_addr.and_then(|a| a.as_socket_addr()) {
                        accepted.remote_addr = src;
                    }
                }
            }
            Err(super::proxyproto::ProxyError::NoProxyProtocol) => {
                // dskit configures go-proxyproto with Policy::USE: connections without a
                // PROXY header (or header-detection timeout) are accepted as-is.
            }
            Err(e) => return Err(io::Error::from(e)),
        }
    }
    Ok(accepted)
}

#[cfg(test)]
async fn read_proxy_header(stream: &mut TcpStream) -> io::Result<(Vec<u8>, Option<SocketAddr>)> {
    let mut reader = super::proxyproto::ProxyAsyncBufReader::new(stream);
    let res = super::proxyproto::read_async(&mut reader).await;
    let (_, buffered) = reader.into_inner_and_buffered();
    match res {
        Ok(hdr) => {
            let src = if hdr.command.is_local() {
                None
            } else {
                hdr.source_addr.and_then(|a| a.as_socket_addr())
            };
            Ok((buffered, src))
        }
        Err(super::proxyproto::ProxyError::NoProxyProtocol) => Ok((buffered, None)),
        Err(e) => Err(io::Error::from(e)),
    }
}
/// A stream that first replays `prefix` (bytes consumed while probing for a PROXY header).
pub struct PrefixedIo<T> {
    prefix: Vec<u8>,
    pos: usize,
    inner: T,
}

impl<T> PrefixedIo<T> {
    pub fn new(prefix: Vec<u8>, inner: T) -> Self {
        Self {
            prefix,
            pos: 0,
            inner,
        }
    }
}

impl<T: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for PrefixedIo<T> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        if self.pos < self.prefix.len() {
            let n = (self.prefix.len() - self.pos).min(buf.remaining());
            let start = self.pos;
            buf.put_slice(&self.prefix[start..start + n]);
            self.pos += n;
            return std::task::Poll::Ready(Ok(()));
        }
        std::pin::Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<T: tokio::io::AsyncWrite + Unpin> tokio::io::AsyncWrite for PrefixedIo<T> {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        std::pin::Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
    }

    fn poll_write_vectored(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> std::task::Poll<io::Result<usize>> {
        std::pin::Pin::new(&mut self.inner).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;
    const V2_SIG: &[u8; 12] = crate::server::proxyproto::SIGV2;

    #[test]
    fn empty_address_listens_dual_stack() {
        let l = listen("tcp", "", 0).unwrap();
        let addr = l.local_addr().unwrap();
        assert!(addr.is_ipv6(), "{addr}");
        let port = addr.port();
        // Reachable over IPv4 and IPv6 loopback.
        std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
        std::net::TcpStream::connect(("::1", port)).unwrap();
    }

    #[test]
    fn ipv4_wildcard_with_tcp_is_dual_stack_like_go() {
        let l = listen("tcp", "0.0.0.0", 0).unwrap();
        let port = l.local_addr().unwrap().port();
        std::net::TcpStream::connect(("::1", port)).unwrap();
    }

    #[test]
    fn tcp4_is_ipv4_only() {
        let l = listen("tcp4", "", 0).unwrap();
        assert!(l.local_addr().unwrap().is_ipv4());
    }

    #[test]
    fn unknown_network_rejected() {
        assert!(listen("udp", "", 0).unwrap_err().to_string().contains("unknown network udp"));
    }

    #[tokio::test]
    async fn proxy_v1_and_passthrough() {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        let client = tokio::spawn(async move {
            let mut s = TcpStream::connect(addr).await.unwrap();
            s.write_all(b"PROXY TCP4 10.1.2.3 10.0.0.1 5555 80\r\nGET").await.unwrap();
            let mut s2 = TcpStream::connect(addr).await.unwrap();
            s2.write_all(b"GET / HTTP/1.1\r\n").await.unwrap();
            (s, s2)
        });
        let (mut a, _) = l.accept().await.unwrap();
        let (prefix, src) = read_proxy_header(&mut a).await.unwrap();
        assert_eq!(prefix, b"GET");
        assert_eq!(src.unwrap(), "10.1.2.3:5555".parse().unwrap());
        let (mut b, _) = l.accept().await.unwrap();
        let (prefix, src) = read_proxy_header(&mut b).await.unwrap();
        assert_eq!(prefix, b"GET / HTTP/1.1\r\n");
        assert!(src.is_none());
        let _ = client.await;
    }

    #[tokio::test]
    async fn proxy_v2() {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        tokio::spawn(async move {
            let mut s = TcpStream::connect(addr).await.unwrap();
            let mut h = V2_SIG.to_vec();
            h.extend_from_slice(&[0x21, 0x11, 0, 12, 192, 168, 1, 7, 10, 0, 0, 1, 0x1f, 0x90, 0, 80]);
            s.write_all(&h).await.unwrap();
            tokio::time::sleep(Duration::from_millis(200)).await;
        });
        let (mut a, _) = l.accept().await.unwrap();
        let (_, src) = read_proxy_header(&mut a).await.unwrap();
        assert_eq!(src.unwrap(), "192.168.1.7:8080".parse().unwrap());
    }

    #[tokio::test]
    async fn d2_08_proxy_v1_and_v2_reject_invalid_commands_families_and_truncated_payloads() {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();

        let bad_headers: Vec<Vec<u8>> = vec![
            // v1: TCP4 with IPv6 source
            b"PROXY TCP4 ::1 10.0.0.1 5555 80\r\n".to_vec(),
            // v1: TCP6 with IPv4 source
            b"PROXY TCP6 10.1.2.3 ::1 5555 80\r\n".to_vec(),
            // v1: TCP4 with invalid destination IP
            b"PROXY TCP4 10.1.2.3 notanip 5555 80\r\n".to_vec(),
            // v1: TCP4 with invalid destination port
            b"PROXY TCP4 10.1.2.3 10.0.0.1 5555 notaport\r\n".to_vec(),
            // v1: TCP4 with leading zero port (forbidden by spec / go-proxyproto)
            b"PROXY TCP4 10.1.2.3 10.0.0.1 5555 080\r\n".to_vec(),
            // v2: invalid command nibble (0x22)
            [V2_SIG.as_slice(), &[0x22, 0x11, 0, 12], &[0u8; 12]].concat(),
            // v2: undefined family/protocol byte (0x13)
            [V2_SIG.as_slice(), &[0x21, 0x13, 0, 12], &[0u8; 12]].concat(),
            // v2: undefined family/protocol byte even with LOCAL (0x20, 0xFF)
            [V2_SIG.as_slice(), &[0x20, 0xFF, 0, 0]].concat(),
            // v2: UNSPEC (0x00) with PROXY (0x21)
            [V2_SIG.as_slice(), &[0x21, 0x00, 0, 0]].concat(),
            // v2: truncated IPv4 payload (len=4 < 12) with PROXY
            [V2_SIG.as_slice(), &[0x21, 0x11, 0, 4], &[1, 2, 3, 4]].concat(),
            // v2: truncated IPv6 payload (len=16 < 36) with PROXY
            [V2_SIG.as_slice(), &[0x21, 0x21, 0, 16], &[0u8; 16]].concat(),
        ];

        for hdr in bad_headers {
            let hdr_c = hdr.clone();
            let cli = tokio::spawn(async move {
                let mut s = TcpStream::connect(addr).await.unwrap();
                s.write_all(&hdr_c).await.unwrap();
                s
            });
            let (mut srv, _) = l.accept().await.unwrap();
            let err = read_proxy_header(&mut srv)
                .await
                .expect_err(&format!("expected error for bad header: {hdr:?}"));
            assert_eq!(err.kind(), io::ErrorKind::InvalidData);
            let _ = cli.await;
        }

        // v2: LOCAL (0x20) + TCPv4 (0x11) + len=0 is valid per go-proxyproto v2.go:149 and yields None.
        let local_hdr = [V2_SIG.as_slice(), &[0x20, 0x11, 0, 0]].concat();
        let cli = tokio::spawn(async move {
            let mut s = TcpStream::connect(addr).await.unwrap();
            s.write_all(&local_hdr).await.unwrap();
            s
        });
        let (mut srv, _) = l.accept().await.unwrap();
        let (prefix, src) = read_proxy_header(&mut srv).await.unwrap();
        assert!(prefix.is_empty());
        assert!(src.is_none());
        let _ = cli.await;
    }
}
