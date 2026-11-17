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
use tokio::io::AsyncReadExt;
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
            Duration::from_secs(10)
        } else {
            opts.proxy_header_timeout
        };
        match tokio::time::timeout(timeout, read_proxy_header(&mut accepted.stream)).await {
            Ok(Ok((prefix, src))) => {
                accepted.prefix = prefix;
                if let Some(src) = src {
                    accepted.remote_addr = src;
                }
            }
            Ok(Err(e)) => return Err(e),
            Err(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "proxyproto: timeout reading PROXY protocol header",
                ))
            }
        }
    }
    Ok(accepted)
}

const V2_SIG: &[u8; 12] = b"\r\n\r\n\0\r\nQUIT\n";
const V1_SIG: &[u8; 6] = b"PROXY ";

/// Reads an optional PROXY v1/v2 header. Returns bytes read that are not part of a header
/// (to be replayed) and the source address announced by the header, if any.
async fn read_proxy_header(stream: &mut TcpStream) -> io::Result<(Vec<u8>, Option<SocketAddr>)> {
    let mut buf: Vec<u8> = Vec::with_capacity(256);
    // Read just enough to decide whether a header is present.
    loop {
        let is_v1_prefix = V1_SIG.starts_with(&buf[..buf.len().min(V1_SIG.len())]);
        let is_v2_prefix = V2_SIG.starts_with(&buf[..buf.len().min(V2_SIG.len())]);
        if !is_v1_prefix && !is_v2_prefix {
            return Ok((buf, None));
        }
        if buf.len() >= V1_SIG.len() && buf.starts_with(V1_SIG) {
            return read_v1(stream, buf).await;
        }
        if buf.len() >= V2_SIG.len() && buf.starts_with(V2_SIG) {
            return read_v2(stream, buf).await;
        }
        let mut b = [0u8; 1];
        let n = stream.read(&mut b).await?;
        if n == 0 {
            return Ok((buf, None));
        }
        buf.push(b[0]);
    }
}

fn proxy_err(msg: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, format!("proxyproto: {msg}"))
}

async fn read_v1(stream: &mut TcpStream, mut buf: Vec<u8>) -> io::Result<(Vec<u8>, Option<SocketAddr>)> {
    // The v1 header is at most 107 bytes including CRLF.
    while !buf.ends_with(b"\r\n") {
        if buf.len() >= 107 {
            return Err(proxy_err("version 1 header too long"));
        }
        let mut b = [0u8; 1];
        if stream.read(&mut b).await? == 0 {
            return Err(proxy_err("unexpected EOF in version 1 header"));
        }
        buf.push(b[0]);
    }
    let line = std::str::from_utf8(&buf[..buf.len() - 2]).map_err(|_| proxy_err("invalid header"))?;
    let parts: Vec<&str> = line.split(' ').collect();
    match parts.get(1).copied() {
        Some("UNKNOWN") => Ok((Vec::new(), None)),
        Some("TCP4") | Some("TCP6") if parts.len() == 6 => {
            let ip: IpAddr = parts[2].parse().map_err(|_| proxy_err("invalid source address"))?;
            let port: u16 = parts[4].parse().map_err(|_| proxy_err("invalid source port"))?;
            Ok((Vec::new(), Some(SocketAddr::new(ip, port))))
        }
        _ => Err(proxy_err("invalid version 1 header")),
    }
}

async fn read_v2(stream: &mut TcpStream, mut buf: Vec<u8>) -> io::Result<(Vec<u8>, Option<SocketAddr>)> {
    while buf.len() < 16 {
        let mut b = [0u8; 1];
        if stream.read(&mut b).await? == 0 {
            return Err(proxy_err("unexpected EOF in version 2 header"));
        }
        buf.push(b[0]);
    }
    let ver_cmd = buf[12];
    if ver_cmd >> 4 != 2 {
        return Err(proxy_err("invalid version 2 header"));
    }
    let fam = buf[13];
    let len = u16::from_be_bytes([buf[14], buf[15]]) as usize;
    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload).await?;
    // LOCAL command: keep the real connection address.
    if ver_cmd & 0x0f == 0 {
        return Ok((Vec::new(), None));
    }
    let src = match fam >> 4 {
        1 if payload.len() >= 12 => Some(SocketAddr::new(
            IpAddr::from([payload[0], payload[1], payload[2], payload[3]]),
            u16::from_be_bytes([payload[8], payload[9]]),
        )),
        2 if payload.len() >= 36 => {
            let mut a = [0u8; 16];
            a.copy_from_slice(&payload[..16]);
            Some(SocketAddr::new(
                IpAddr::from(a),
                u16::from_be_bytes([payload[32], payload[33]]),
            ))
        }
        _ => None,
    };
    Ok((Vec::new(), src))
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
        assert!(prefix.is_empty());
        assert_eq!(src.unwrap(), "10.1.2.3:5555".parse().unwrap());
        let (mut b, _) = l.accept().await.unwrap();
        let (prefix, src) = read_proxy_header(&mut b).await.unwrap();
        assert_eq!(prefix, b"G");
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
}
