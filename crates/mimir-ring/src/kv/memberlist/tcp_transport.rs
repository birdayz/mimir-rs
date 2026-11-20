//! TCP transport for memberlist gossip (packet + stream multiplexing over TCP, optionally TLS).
//!
//! Ported from github.com/grafana/dskit/kv/memberlist/tcp_transport.go
//! and github.com/grafana/dskit/kv/memberlist/tcp_transport_conn.go (`meteredConn`).
//!
//! TLS (`-memberlist.tls-enabled` + the inline dskit `crypto/tls.ClientConfig` fields) follows
//! dskit exactly: the single `GetTLSConfig()` result is used
//! - on the server side (`tls.Listen`): the configured `tls_cert_path`/`tls_key_path` key pair is
//!   served (no key pair: every handshake fails), client certificates are never requested;
//! - on the client side (`tls.DialWithDialer`): the server certificate is verified against
//!   `tls_ca_path` (or the host roots), the expected name is `tls_server_name` or else the host
//!   part of the dialed `host:port`, and `tls_insecure_skip_verify` disables verification. The
//!   dial timeout covers the TCP connect and the TLS handshake.
//!
//! The handshake of accepted connections happens in the per-connection task (Go performs it
//! lazily on the first `Read`), so a failing handshake is logged as `failed to read message type`.

use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime};

use md5::{Digest, Md5};
use mimir_common::tls::{parse_server_name, ClientConfig as TlsClientConfig};
use mimir_config::server::MemberlistConfig;
use parking_lot::{Mutex, RwLock};
use prometheus::{Counter, Opts, Registry};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tracing::{debug, error, warn};

use super::wire::MAX_PUSH_STATE_BYTES;

const ZERO_ZERO_ZERO_ZERO: &str = "0.0.0.0";
const COLON_COLON: &str = "::";

const PACKET_BYTE: u8 = 1;
const STREAM_BYTE: u8 = 2;

/// dskit `flushThreshold`: local byte counters are flushed to Prometheus after this many bytes.
const FLUSH_THRESHOLD: u64 = 64 * 1024;

/// Go `component` log field of the transport logger.
const COMPONENT: &str = "memberlist TCPTransport";

#[derive(Debug, Clone)]
pub struct ReceivedPacket {
    pub buf: Vec<u8>,
    pub from: String,
    pub timestamp: SystemTime,
}

#[derive(Clone)]
struct TransportMetrics {
    incoming_streams: Counter,
    outgoing_streams: Counter,
    outgoing_stream_errors: Counter,
    received_packets: Counter,
    received_packets_bytes: Counter,
    received_packets_errors: Counter,
    sent_packets: Counter,
    sent_packets_bytes: Counter,
    sent_packets_errors: Counter,
    dropped_packets: Counter,
    unknown_connections: Counter,
    sent_bytes: Counter,
    received_bytes: Counter,
}

impl TransportMetrics {
    /// Port of `TCPTransport.registerMetrics` (namespace = `MetricsNamespace`, subsystem
    /// `memberlist_tcp_transport`; Prometheus `BuildFQName` joins the non-empty parts with `_`).
    fn new(reg: Option<&Registry>, ns: &str) -> Self {
        let mk = |name: &str, help: &str| {
            let full = if ns.is_empty() {
                format!("memberlist_tcp_transport_{name}")
            } else {
                format!("{ns}_memberlist_tcp_transport_{name}")
            };
            let c = Counter::with_opts(Opts::new(full, help)).expect("valid counter");
            if let Some(r) = reg {
                let _ = r.register(Box::new(c.clone()));
            }
            c
        };
        Self {
            incoming_streams: mk("incoming_streams_total", "Number of incoming memberlist streams"),
            outgoing_streams: mk("outgoing_streams_total", "Number of outgoing streams"),
            outgoing_stream_errors: mk(
                "outgoing_stream_errors_total",
                "Number of errors when opening memberlist stream to another node",
            ),
            received_packets: mk("packets_received_total", "Number of received memberlist packets"),
            received_packets_bytes: mk(
                "packets_received_bytes_total",
                "Total bytes received as packets. This metric only tracks broadcast packets, and does not include full state syncs or pings.",
            ),
            received_packets_errors: mk(
                "packets_received_errors_total",
                "Number of errors when receiving memberlist packets",
            ),
            dropped_packets: mk(
                "packets_dropped_total",
                "Number of dropped memberlist packets. These packets were not sent due to timeout waiting for a writer.",
            ),
            sent_packets: mk("packets_sent_total", "Number of memberlist packets sent"),
            sent_packets_bytes: mk(
                "packets_sent_bytes_total",
                "Total bytes sent as packets. This metric only tracks broadcast packets, and does not include full state syncs or pings.",
            ),
            sent_packets_errors: mk(
                "packets_sent_errors_total",
                "Number of errors when sending memberlist packets",
            ),
            unknown_connections: mk(
                "unknown_connections_total",
                "Number of unknown TCP connections (not a packet or stream)",
            ),
            sent_bytes: mk(
                "sent_bytes_total",
                "Total bytes sent by the transport. This metric tracks all data transferred, including broadcast packets, full state syncs, and pings.",
            ),
            // Upstream reuses the "sent" help text for the received counter; kept verbatim.
            received_bytes: mk(
                "received_bytes_total",
                "Total bytes sent by the transport. This metric tracks all data transferred, including broadcast packets, full state syncs, and pings.",
            ),
        }
    }
}

/// Local byte counters of one connection (dskit `meteredConn`): flushed to the Prometheus
/// counters once either reaches [`FLUSH_THRESHOLD`] and when the connection is dropped (Go
/// `Close`).
struct ByteMeter {
    sent_counter: Counter,
    received_counter: Counter,
    sent_local: u64,
    received_local: u64,
}

impl ByteMeter {
    fn add_sent(&mut self, n: usize) {
        self.sent_local += n as u64;
        if self.sent_local >= FLUSH_THRESHOLD {
            self.flush();
        }
    }

    fn add_received(&mut self, n: usize) {
        self.received_local += n as u64;
        if self.received_local >= FLUSH_THRESHOLD {
            self.flush();
        }
    }

    fn flush(&mut self) {
        if self.sent_local > 0 {
            self.sent_counter.inc_by(self.sent_local as f64);
            self.sent_local = 0;
        }
        if self.received_local > 0 {
            self.received_counter.inc_by(self.received_local as f64);
            self.received_local = 0;
        }
    }
}

impl Drop for ByteMeter {
    fn drop(&mut self) {
        self.flush();
    }
}

enum ConnStream {
    Plain(TcpStream),
    ClientTls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
    ServerTls(Box<tokio_rustls::server::TlsStream<TcpStream>>),
}

/// A memberlist transport connection (Go `net.Conn` returned by `DialTimeout` / delivered on
/// `StreamCh`): plain TCP or TLS, metered into `memberlist_tcp_transport_{sent,received}_bytes_total`
/// (application bytes, i.e. above TLS, like dskit's `meteredConn` wrapping the `tls.Conn`).
pub struct MemberlistConn {
    stream: ConnStream,
    meter: ByteMeter,
}

impl MemberlistConn {
    fn new(stream: ConnStream, metrics: &TransportMetrics) -> Self {
        Self {
            stream,
            meter: ByteMeter {
                sent_counter: metrics.sent_bytes.clone(),
                received_counter: metrics.received_bytes.clone(),
                sent_local: 0,
                received_local: 0,
            },
        }
    }

    fn tcp(&self) -> &TcpStream {
        match &self.stream {
            ConnStream::Plain(s) => s,
            ConnStream::ClientTls(s) => s.get_ref().0,
            ConnStream::ServerTls(s) => s.get_ref().0,
        }
    }

    /// Whether the connection is TLS-encrypted.
    pub fn is_tls(&self) -> bool {
        !matches!(self.stream, ConnStream::Plain(_))
    }

    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.tcp().peer_addr()
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.tcp().local_addr()
    }

    /// Borrowing read/write split (like [`TcpStream::split`]).
    pub fn split(
        &mut self,
    ) -> (
        tokio::io::ReadHalf<&mut MemberlistConn>,
        tokio::io::WriteHalf<&mut MemberlistConn>,
    ) {
        tokio::io::split(self)
    }

    /// Unwraps a plaintext connection (byte counters are flushed; later traffic is not metered).
    /// `None` for TLS connections.
    pub fn into_tcp_stream(self) -> Option<TcpStream> {
        let MemberlistConn { stream, meter } = self;
        drop(meter);
        match stream {
            ConnStream::Plain(s) => Some(s),
            _ => None,
        }
    }
}

impl std::fmt::Debug for MemberlistConn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemberlistConn")
            .field("tls", &self.is_tls())
            .field("peer_addr", &self.peer_addr().ok())
            .finish_non_exhaustive()
    }
}

impl AsyncRead for MemberlistConn {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let before = buf.filled().len();
        let res = match &mut this.stream {
            ConnStream::Plain(s) => Pin::new(s).poll_read(cx, buf),
            ConnStream::ClientTls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
            ConnStream::ServerTls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
        };
        let n = buf.filled().len() - before;
        if n > 0 {
            this.meter.add_received(n);
        }
        res
    }
}

impl AsyncWrite for MemberlistConn {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let res = match &mut this.stream {
            ConnStream::Plain(s) => Pin::new(s).poll_write(cx, buf),
            ConnStream::ClientTls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
            ConnStream::ServerTls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
        };
        if let Poll::Ready(Ok(n)) = res {
            if n > 0 {
                this.meter.add_sent(n);
            }
        }
        res
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let res = match &mut this.stream {
            ConnStream::Plain(s) => Pin::new(s).poll_write_vectored(cx, bufs),
            ConnStream::ClientTls(s) => Pin::new(s.as_mut()).poll_write_vectored(cx, bufs),
            ConnStream::ServerTls(s) => Pin::new(s.as_mut()).poll_write_vectored(cx, bufs),
        };
        if let Poll::Ready(Ok(n)) = res {
            if n > 0 {
                this.meter.add_sent(n);
            }
        }
        res
    }

    fn is_write_vectored(&self) -> bool {
        match &self.stream {
            ConnStream::Plain(s) => s.is_write_vectored(),
            ConnStream::ClientTls(s) => s.is_write_vectored(),
            ConnStream::ServerTls(s) => s.is_write_vectored(),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.get_mut().stream {
            ConnStream::Plain(s) => Pin::new(s).poll_flush(cx),
            ConnStream::ClientTls(s) => Pin::new(s.as_mut()).poll_flush(cx),
            ConnStream::ServerTls(s) => Pin::new(s.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.get_mut().stream {
            ConnStream::Plain(s) => Pin::new(s).poll_shutdown(cx),
            ConnStream::ClientTls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
            ConnStream::ServerTls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}

/// The TLS side of `TCPTransport.tlsConfig`.
struct TransportTls {
    connector: TlsConnector,
    acceptor: TlsAcceptor,
    server_name: Option<ServerName<'static>>,
    insecure_skip_verify: bool,
}

/// The dskit `TCPTransportConfig.TLS` (`crypto/tls.ClientConfig`, inlined YAML keys) of `cfg`.
pub fn memberlist_tls_client_config(cfg: &MemberlistConfig) -> TlsClientConfig {
    TlsClientConfig {
        cert_path: cfg.tls_cert_path.clone(),
        key_path: cfg.tls_key_path.clone(),
        ca_path: cfg.tls_ca_path.clone(),
        server_name: cfg.tls_server_name.clone(),
        insecure_skip_verify: cfg.tls_insecure_skip_verify,
        cipher_suites: cfg.tls_cipher_suites.clone(),
        min_version: cfg.tls_min_version.clone(),
    }
}

impl TransportTls {
    fn new(cfg: &MemberlistConfig) -> Result<Self, mimir_common::tls::TlsConfigError> {
        let resolved = memberlist_tls_client_config(cfg).get_client_and_server_tls_config()?;
        Ok(Self {
            connector: TlsConnector::from(resolved.client.config),
            acceptor: TlsAcceptor::from(resolved.server),
            server_name: resolved.client.server_name,
            insecure_skip_verify: cfg.tls_insecure_skip_verify,
        })
    }

    /// Go `tls.DialWithDialer`: without `tls_server_name`, `ServerName` is the dialed address up
    /// to the last `:` (brackets of IPv6 literals are ignored by Go's SNI / verification).
    fn server_name_for(&self, addr: &str) -> io::Result<ServerName<'static>> {
        if let Some(name) = &self.server_name {
            return Ok(name.clone());
        }
        let host = addr.rfind(':').map_or(addr, |i| &addr[..i]);
        let host = host
            .strip_prefix('[')
            .and_then(|h| h.strip_suffix(']'))
            .unwrap_or(host);
        match parse_server_name(host) {
            Ok(name) => Ok(name),
            // Go accepts any name when verification is disabled; an IP name sends no SNI.
            Err(_) if self.insecure_skip_verify => {
                Ok(ServerName::IpAddress(IpAddr::V4(Ipv4Addr::UNSPECIFIED).into()))
            }
            Err(_) if host.is_empty() => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "tls: either ServerName or InsecureSkipVerify must be specified in the tls.Config",
            )),
            Err(err) => Err(io::Error::new(io::ErrorKind::InvalidInput, err.to_string())),
        }
    }
}

/// Go-style text of an OS error: lower-case `strerror` text without Rust's ` (os error N)`.
fn go_err_text(err: &io::Error) -> String {
    let mut msg = err.to_string();
    if let Some(idx) = msg.find(" (os error ") {
        msg.truncate(idx);
    }
    let mut chars = msg.chars();
    match chars.next() {
        Some(c) => c.to_lowercase().collect::<String>() + chars.as_str(),
        None => msg,
    }
}

/// Go `net.OpError` text of a failed dial: `dial tcp <addr>: connect: <errno text>`.
fn dial_error(addr: &str, err: io::Error) -> io::Error {
    let text = if err.raw_os_error().is_some() {
        format!("dial tcp {addr}: connect: {}", go_err_text(&err))
    } else {
        format!("dial tcp {addr}: {}", go_err_text(&err))
    };
    io::Error::new(err.kind(), text)
}

/// Go `net.ListenTCP("tcp", &net.TCPAddr{IP: ip, Port: port})` (and `tls.Listen("tcp", ...)`,
/// which resolves to the same call) on Linux:
/// - `favoriteAddrFamily`: a wildcard address (`0.0.0.0`, the default, or `::`) with network
///   `tcp` listens on an `AF_INET6` socket with `IPV6_V6ONLY=0`, i.e. dual-stack `[::]:port`
///   accepting both IPv4 (as IPv4-mapped) and IPv6 peers. Hosts without IPv6 support
///   (`supportsIPv4map() == false`) get a plain `AF_INET` wildcard socket instead.
/// - any other address binds exactly that address (IPv4-mapped IPv6 literals as IPv4).
/// - `SO_REUSEADDR` is set and the backlog is `/proc/sys/net/core/somaxconn`
///   (`maxListenerBacklog`).
fn go_listen_tcp(addr: SocketAddr) -> io::Result<TcpListener> {
    let listener = if addr.ip().is_unspecified() {
        let v6 = SocketAddr::new(Ipv6Addr::UNSPECIFIED.into(), addr.port());
        match bind_listen_socket(v6, false) {
            Ok(l) => l,
            // No IPv6 on this host: Go then listens on the address' own family.
            Err(e) if e.raw_os_error() == Some(libc::EAFNOSUPPORT) => {
                bind_listen_socket(addr, false)?
            }
            Err(e) => return Err(e),
        }
    } else {
        // Go treats IPv4-mapped IPv6 literals (`::ffff:a.b.c.d`) as IPv4 (`family()`), and
        // never sets IPV6_V6ONLY for a specific address.
        let addr = SocketAddr::new(addr.ip().to_canonical(), addr.port());
        bind_listen_socket(addr, false)?
    };
    TcpListener::from_std(listener)
}

fn bind_listen_socket(addr: SocketAddr, v6_only: bool) -> io::Result<std::net::TcpListener> {
    use socket2::{Domain, Protocol, SockAddr, Socket, Type};
    let domain = if addr.is_ipv6() { Domain::IPV6 } else { Domain::IPV4 };
    let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
    if addr.is_ipv6() {
        socket.set_only_v6(v6_only)?;
    }
    socket.set_reuse_address(true)?;
    socket.set_nonblocking(true)?;
    socket.bind(&SockAddr::from(addr))?;
    socket.listen(somaxconn())?;
    Ok(socket.into())
}

/// Go `maxListenerBacklog` on Linux.
fn somaxconn() -> i32 {
    std::fs::read_to_string("/proc/sys/net/core/somaxconn")
        .ok()
        .and_then(|s| s.trim().parse::<i32>().ok())
        .filter(|n| *n > 0)
        .map(|n| n.min(65535))
        .unwrap_or(128)
}

fn prefixed(prefix: &str, err: io::Error) -> io::Error {
    io::Error::new(err.kind(), format!("{prefix}: {err}"))
}

fn configure_tcp_socket(tcp: &TcpStream) {
    let _ = tcp.set_nodelay(true);
    let sock = socket2::SockRef::from(tcp);
    let _ = sock.set_tcp_keepalive(
        &socket2::TcpKeepalive::new()
            .with_time(Duration::from_secs(15))
            .with_interval(Duration::from_secs(15))
            .with_retries(9),
    );
}

/// Memberlist `Transport` over TCP: every packet / stream uses a new connection (dskit
/// `TCPTransport`).
pub struct TcpTransport {
    me: Weak<TcpTransport>,
    stream_timeout: Duration,
    packet_dial_timeout: Duration,
    packet_write_timeout: Duration,
    acquire_writer_timeout: Duration,
    max_concurrent_writes: u32,
    write_sem: Arc<Semaphore>,
    /// `cfg.BindAddrs` as configured (Go switches on the string value of `BindAddrs[0]`).
    bind_addrs: Vec<String>,
    tls: Option<TransportTls>,
    transport_debug: AtomicBool,
    local_addrs: Vec<SocketAddr>,
    advertise_addr: RwLock<String>,
    packet_tx: mpsc::Sender<ReceivedPacket>,
    stream_tx: mpsc::Sender<MemberlistConn>,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    listener_tasks: Mutex<Vec<JoinHandle<()>>>,
    metrics: TransportMetrics,
}

impl TcpTransport {
    /// Port of dskit `NewTCPTransport`: builds the TLS config (when `tls_enabled`), registers the
    /// metrics and starts one listener per `bind_addr`. Error messages are Go's
    /// (`unable to create TLS config: ...`, `could not parse bind addr "x" as IP address`,
    /// `failed to start [TLS ]TCP listener on "ip" port N: ...`); dskit's caller wraps them in
    /// `failed to create transport: ...`.
    pub async fn new(
        cfg: &MemberlistConfig,
        metrics_namespace: &str,
        reg: Option<&Registry>,
    ) -> Result<
        (
            Arc<Self>,
            mpsc::Receiver<ReceivedPacket>,
            mpsc::Receiver<MemberlistConn>,
        ),
        io::Error,
    > {
        let bind_addrs = if cfg.bind_addr.is_empty() {
            vec!["0.0.0.0".to_string()]
        } else {
            cfg.bind_addr.clone()
        };

        let concurrent_writes = cfg.max_concurrent_writes.clamp(1, i64::from(u32::MAX >> 4)) as u32;

        let tls = if cfg.tls_enabled {
            Some(TransportTls::new(cfg).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unable to create TLS config: {e}"),
                )
            })?)
        } else {
            None
        };

        let metrics = TransportMetrics::new(reg, metrics_namespace);

        let mut port = cfg.bind_port.max(0) as u16;
        let mut listeners = Vec::with_capacity(bind_addrs.len());
        let mut local_addrs = Vec::with_capacity(bind_addrs.len());

        for addr_str in &bind_addrs {
            let ip: IpAddr = addr_str.parse().map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("could not parse bind addr {addr_str:?} as IP address"),
                )
            })?;
            let sock = SocketAddr::new(ip, port);
            let listener = go_listen_tcp(sock).map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!(
                        "failed to start {}TCP listener on {addr_str:?} port {port}: listen tcp {sock}: bind: {}",
                        if tls.is_some() { "TLS " } else { "" },
                        go_err_text(&e)
                    ),
                )
            })?;
            let bound = listener.local_addr()?;
            if port == 0 {
                port = bound.port();
            }
            local_addrs.push(bound);
            listeners.push(listener);
        }

        let (packet_tx, packet_rx) = mpsc::channel(1024);
        let (stream_tx, stream_rx) = mpsc::channel(256);
        let (shutdown_tx, _) = tokio::sync::watch::channel(false);

        let transport = Arc::new_cyclic(|me| Self {
            me: me.clone(),
            stream_timeout: cfg.stream_timeout.as_std().unwrap_or(Duration::ZERO),
            packet_dial_timeout: cfg.packet_dial_timeout.as_std().unwrap_or(Duration::ZERO),
            packet_write_timeout: cfg.packet_write_timeout.as_std().unwrap_or(Duration::ZERO),
            acquire_writer_timeout: cfg.acquire_writer_timeout.as_std().unwrap_or(Duration::ZERO),
            max_concurrent_writes: concurrent_writes,
            write_sem: Arc::new(Semaphore::new(concurrent_writes as usize)),
            bind_addrs: bind_addrs.clone(),
            tls,
            transport_debug: AtomicBool::new(false),
            local_addrs,
            advertise_addr: RwLock::new(String::new()),
            packet_tx,
            stream_tx,
            shutdown_tx,
            listener_tasks: Mutex::new(Vec::new()),
            metrics,
        });

        let mut tasks = Vec::with_capacity(listeners.len());
        for listener in listeners {
            let tr = transport.clone();
            tasks.push(tokio::spawn(async move { tr.tcp_listen(listener).await }));
        }
        *transport.listener_tasks.lock() = tasks;

        Ok((transport, packet_rx, stream_rx))
    }

    /// Plaintext-only variant of [`TcpTransport::new`] that hands incoming streams over as raw
    /// [`TcpStream`]s. Kept for callers that predate TLS support; a TLS-enabled config is
    /// rejected with an explicit error because TLS streams cannot be represented as `TcpStream`.
    /// Incoming stream bytes read after the hand-over are not metered.
    pub async fn bind(
        cfg: &MemberlistConfig,
        metrics_namespace: &str,
        reg: Option<&Registry>,
    ) -> Result<
        (
            Arc<Self>,
            mpsc::Receiver<ReceivedPacket>,
            mpsc::Receiver<TcpStream>,
        ),
        io::Error,
    > {
        if cfg.tls_enabled {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "memberlist TLS (tls_enabled) requires TcpTransport::new: TcpTransport::bind only supports plaintext TcpStream connections",
            ));
        }
        let (transport, packet_rx, mut conn_rx) = Self::new(cfg, metrics_namespace, reg).await?;
        let (tx, rx) = mpsc::channel(256);
        tokio::spawn(async move {
            while let Some(conn) = conn_rx.recv().await {
                if let Some(tcp) = conn.into_tcp_stream() {
                    if tx.send(tcp).await.is_err() {
                        break;
                    }
                }
            }
        });
        Ok((transport, packet_rx, rx))
    }

    /// Go `TCPTransportConfig.TransportDebug` (`-memberlist.transport-debug`): log debug
    /// transport messages.
    pub fn set_transport_debug(&self, enabled: bool) {
        self.transport_debug.store(enabled, Ordering::Relaxed);
    }

    fn debug_enabled(&self) -> bool {
        self.transport_debug.load(Ordering::Relaxed)
    }

    /// Whether the transport uses TLS (`tls_enabled`).
    pub fn tls_enabled(&self) -> bool {
        self.tls.is_some()
    }

    fn is_shutting_down(&self) -> bool {
        *self.shutdown_tx.borrow()
    }

    /// Port of `tcpListen` (accept loop with 5ms..1s back-off on errors).
    async fn tcp_listen(self: Arc<Self>, listener: TcpListener) {
        const BASE_DELAY: Duration = Duration::from_millis(5);
        const MAX_DELAY: Duration = Duration::from_secs(1);

        let mut shutdown_rx = self.shutdown_tx.subscribe();
        if *shutdown_rx.borrow_and_update() {
            return;
        }
        let mut loop_delay = Duration::ZERO;
        loop {
            tokio::select! {
                biased;
                res = shutdown_rx.changed() => {
                    if res.is_err() || *shutdown_rx.borrow() {
                        break;
                    }
                }
                res = listener.accept() => {
                    match res {
                        Ok((stream, _remote)) => {
                            loop_delay = Duration::ZERO;
                            let tr = self.clone();
                            tokio::spawn(async move {
                                tr.handle_connection(stream).await;
                            });
                        }
                        Err(err) => {
                            if self.is_shutting_down() {
                                break;
                            }
                            loop_delay = if loop_delay.is_zero() {
                                BASE_DELAY
                            } else {
                                (loop_delay * 2).min(MAX_DELAY)
                            };
                            error!(component = COMPONENT, err = %err, "Error accepting TCP connection");
                            tokio::time::sleep(loop_delay).await;
                        }
                    }
                }
            }
        }
    }

    pub fn auto_bind_port(&self) -> u16 {
        self.local_addrs[0].port()
    }

    /// `FinalAdvertiseAddr`: given the user's configured values (which might be empty),
    /// returns the desired IP and port to advertise to the rest of the cluster.
    pub fn final_advertise_addr(
        &self,
        configured_addr: &str,
        configured_port: i64,
    ) -> Result<(IpAddr, u16), io::Error> {
        let other = |msg: String| io::Error::other(msg);
        let (ip, port) = if !configured_addr.is_empty() {
            // If they've supplied an address, use that.
            let ip: IpAddr = configured_addr.parse().map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("failed to parse advertise address {configured_addr:?}"),
                )
            })?;
            (ip, configured_port as u16)
        } else {
            let ip = match self.bind_addrs[0].as_str() {
                ZERO_ZERO_ZERO_ZERO => {
                    // Otherwise, if we're not bound to a specific IP, let's use a suitable
                    // private IP address.
                    let ip = mimir_common::sockaddr::get_private_ip()
                        .map_err(|e| other(format!("failed to get interface addresses: {e}")))?;
                    if ip.is_empty() {
                        return Err(other(
                            "no private IP address found, and explicit IP not provided".to_string(),
                        ));
                    }
                    ip.parse::<IpAddr>().map_err(|_| {
                        other(format!("failed to parse advertise address {ip:?}"))
                    })?
                }
                COLON_COLON => {
                    let inet6_ip = mimir_common::netutil::get_first_address_of(&[], true)
                        .map_err(|e| other(format!("failed to get private inet6 address: {e}")))?;
                    inet6_ip.parse::<IpAddr>().map_err(|_| {
                        other(format!("failed to parse inet6 advertise address {configured_addr:?}"))
                    })?
                }
                // Use the IP that we're bound to, based on the first TCP listener.
                _ => self.local_addrs[0].ip(),
            };
            // Use the port we are bound to.
            (ip, self.auto_bind_port())
        };
        debug!(msg = "FinalAdvertiseAddr", advertiseAddr = %ip, advertisePort = port);

        let formatted = SocketAddr::new(ip, port).to_string();
        *self.advertise_addr.write() = formatted;
        Ok((ip, port))
    }

    pub fn get_advertised_addr(&self) -> String {
        self.advertise_addr.read().clone()
    }

    async fn handle_connection(&self, tcp: TcpStream) {
        let remote = tcp.peer_addr().ok();
        if self.debug_enabled() {
            debug!(component = COMPONENT, addr = ?remote, "New connection");
        }
        // Go enables TCP_NODELAY and 15s TCP keepalive on every TCP connection.
        configure_tcp_socket(&tcp);

        let handshake_timeout = if !self.stream_timeout.is_zero() {
            self.stream_timeout
        } else {
            self.packet_write_timeout
        };
        let handshake_fut = async {
            let stream = match &self.tls {
                None => ConnStream::Plain(tcp),
                // Go handshakes lazily on the first Read (of the message type).
                Some(tls) => ConnStream::ServerTls(Box::new(tls.acceptor.accept(tcp).await?)),
            };
            let mut conn = MemberlistConn::new(stream, &self.metrics);
            let mut msg_type = [0u8; 1];
            conn.read_exact(&mut msg_type).await?;
            Ok::<_, io::Error>((conn, msg_type[0]))
        };
        let (mut conn, msg_type) = if handshake_timeout.is_zero() {
            match handshake_fut.await {
                Ok(v) => v,
                Err(err) => {
                    warn!(component = COMPONENT, err = %err, remote = ?remote, "failed to read message type");
                    return;
                }
            }
        } else {
            match tokio::time::timeout(handshake_timeout, handshake_fut).await {
                Ok(Ok(v)) => v,
                Ok(Err(err)) => {
                    warn!(component = COMPONENT, err = %err, remote = ?remote, "failed to read message type");
                    return;
                }
                Err(_) => {
                    warn!(component = COMPONENT, remote = ?remote, "failed to read message type: i/o timeout");
                    return;
                }
            }
        };

        match msg_type {
            STREAM_BYTE => {
                self.metrics.incoming_streams.inc();
                // Hand over this connection to memberlist.
                let _ = self.stream_tx.send(conn).await;
            }
            PACKET_BYTE => {
                self.metrics.received_packets.inc();
                let packet_timeout = if !self.packet_write_timeout.is_zero() {
                    self.packet_write_timeout
                } else {
                    self.stream_timeout
                };
                enum PacketReadErr {
                    AddrLen(io::Error),
                    Addr(io::Error),
                    Data(io::Error),
                }
                let read_pkt_fut = async {
                    let mut addr_len_buf = [0u8; 1];
                    conn.read_exact(&mut addr_len_buf)
                        .await
                        .map_err(PacketReadErr::AddrLen)?;
                    let mut addr_buf = vec![0u8; addr_len_buf[0] as usize];
                    conn.read_exact(&mut addr_buf)
                        .await
                        .map_err(PacketReadErr::Addr)?;
                    let from = String::from_utf8_lossy(&addr_buf).into_owned();
                    let mut buf = Vec::new();
                    (&mut conn)
                        .take((MAX_PUSH_STATE_BYTES + 1) as u64)
                        .read_to_end(&mut buf)
                        .await
                        .map_err(PacketReadErr::Data)?;
                    Ok::<_, PacketReadErr>((from, buf))
                };
                let (from, mut buf) = if packet_timeout.is_zero() {
                    match read_pkt_fut.await {
                        Ok(v) => v,
                        Err(PacketReadErr::AddrLen(err)) => {
                            self.metrics.received_packets_errors.inc();
                            warn!(component = COMPONENT, err = %err, remote = ?remote, "error while reading node address length from packet");
                            return;
                        }
                        Err(PacketReadErr::Addr(err)) => {
                            self.metrics.received_packets_errors.inc();
                            warn!(component = COMPONENT, err = %err, remote = ?remote, "error while reading node address from packet");
                            return;
                        }
                        Err(PacketReadErr::Data(err)) => {
                            self.metrics.received_packets_errors.inc();
                            warn!(component = COMPONENT, err = %err, remote = ?remote, "error while reading packet data");
                            return;
                        }
                    }
                } else {
                    match tokio::time::timeout(packet_timeout, read_pkt_fut).await {
                        Ok(Ok(v)) => v,
                        Ok(Err(PacketReadErr::AddrLen(err))) => {
                            self.metrics.received_packets_errors.inc();
                            warn!(component = COMPONENT, err = %err, remote = ?remote, "error while reading node address length from packet");
                            return;
                        }
                        Ok(Err(PacketReadErr::Addr(err))) => {
                            self.metrics.received_packets_errors.inc();
                            warn!(component = COMPONENT, err = %err, remote = ?remote, "error while reading node address from packet");
                            return;
                        }
                        Ok(Err(PacketReadErr::Data(err))) => {
                            self.metrics.received_packets_errors.inc();
                            warn!(component = COMPONENT, err = %err, remote = ?remote, "error while reading packet data");
                            return;
                        }
                        Err(_) => {
                            self.metrics.received_packets_errors.inc();
                            warn!(component = COMPONENT, remote = ?remote, "error while reading packet data: i/o timeout");
                            return;
                        }
                    }
                };
                if buf.len() > MAX_PUSH_STATE_BYTES {
                    self.metrics.received_packets_errors.inc();
                    warn!(
                        component = COMPONENT,
                        data_length = buf.len(),
                        max = MAX_PUSH_STATE_BYTES,
                        remote = ?remote,
                        "packet exceeds max size"
                    );
                    return;
                }
                if buf.len() < 16 {
                    self.metrics.received_packets_errors.inc();
                    warn!(component = COMPONENT, data_length = buf.len(), remote = ?remote, "not enough data received");
                    return;
                }

                let split = buf.len() - 16;
                let expected_digest = Md5::digest(&buf[..split]);
                let received_digest = &buf[split..];
                if received_digest != expected_digest.as_slice() {
                    self.metrics.received_packets_errors.inc();
                    warn!(
                        component = COMPONENT,
                        expected = %hex_lower(expected_digest.as_slice()),
                        received = %hex_lower(received_digest),
                        data_length = split,
                        remote = ?remote,
                        "packet digest mismatch"
                    );
                }
                if self.debug_enabled() {
                    debug!(component = COMPONENT, addr = %from, size = split, hash = %hex_lower(received_digest), "Received packet");
                }
                buf.truncate(split);
                // The whole packet has been read: close (and flush the byte counters) before
                // handing it over (Go closes right after the hand-over).
                drop(conn);

                self.metrics.received_packets_bytes.inc_by(buf.len() as f64);
                let _ = self
                    .packet_tx
                    .send(ReceivedPacket {
                        buf,
                        from,
                        timestamp: SystemTime::now(),
                    })
                    .await;
            }
            other => {
                self.metrics.unknown_connections.inc();
                error!(component = COMPONENT, msgType = other, remote = ?remote, "unknown message type");
            }
        }
    }

    /// Port of `getConnection`: plain `net.DialTimeout` or `tls.DialWithDialer` (the timeout
    /// covers connect + handshake), wrapped into a metered connection.
    async fn get_connection(&self, addr: &str, timeout: Duration) -> io::Result<MemberlistConn> {
        let deadline = (!timeout.is_zero()).then(|| Instant::now() + timeout);
        self.get_connection_until(addr, deadline).await
    }

    async fn get_connection_until(
        &self,
        addr: &str,
        deadline: Option<Instant>,
    ) -> io::Result<MemberlistConn> {

        let connect = TcpStream::connect(addr);
        let tcp = match deadline {
            None => connect.await,
            Some(d) => tokio::time::timeout_at(d, connect).await.map_err(|_| {
                io::Error::new(io::ErrorKind::TimedOut, format!("dial tcp {addr}: i/o timeout"))
            })?,
        }
        .map_err(|e| dial_error(addr, e))?;
        configure_tcp_socket(&tcp);

        let stream = match &self.tls {
            None => ConnStream::Plain(tcp),
            Some(tls) => {
                let name = tls.server_name_for(addr)?;
                let handshake = tls.connector.connect(name, tcp);
                let s = match deadline {
                    None => handshake.await,
                    Some(d) => tokio::time::timeout_at(d, handshake).await.map_err(|_| {
                        io::Error::new(io::ErrorKind::TimedOut, "context deadline exceeded")
                    })?,
                }?;
                ConnStream::ClientTls(Box::new(s))
            }
        };
        Ok(MemberlistConn::new(stream, &self.metrics))
    }

    /// Sends a memberlist packet over a short-lived TCP connection (Go `WriteTo`). Returns as
    /// soon as one of the `max_concurrent_writes` writers took the packet; the packet is dropped
    /// (`packets_dropped_total`) if no writer frees up within `acquire_writer_timeout`.
    pub async fn write_to(&self, payload: Vec<u8>, addr: String) {
        let _ = self.write_to_with_result(payload, addr).await;
    }

    /// Go `WriteTo` including its result: `Err("transport is shutting down")` after shutdown,
    /// otherwise the time the packet was handed over (or dropped).
    pub async fn write_to_with_result(
        &self,
        payload: Vec<u8>,
        addr: String,
    ) -> Result<SystemTime, io::Error> {
        if self.is_shutting_down() {
            return Err(io::Error::other("transport is shutting down"));
        }

        let permit = if self.acquire_writer_timeout.is_zero() {
            self.write_sem.clone().try_acquire_owned().ok()
        } else {
            tokio::time::timeout(
                self.acquire_writer_timeout,
                self.write_sem.clone().acquire_owned(),
            )
            .await
            .ok()
            .and_then(Result::ok)
        };

        let Some(permit) = permit else {
            // Dropped packets are not an issue, the memberlist protocol will retry later.
            debug!(
                component = COMPONENT,
                timeout = ?self.acquire_writer_timeout,
                addr,
                "WriteTo failed to acquire a writer. Dropping message"
            );
            self.metrics.dropped_packets.inc();
            return Ok(SystemTime::now());
        };

        let Some(this) = self.me.upgrade() else {
            return Err(io::Error::other("transport is shutting down"));
        };
        tokio::spawn(async move {
            this.write_worker(payload, addr).await;
            drop(permit);
        });
        Ok(SystemTime::now())
    }

    /// One iteration of Go `writeWorker`.
    async fn write_worker(&self, b: Vec<u8>, addr: String) {
        self.metrics.sent_packets.inc();
        self.metrics.sent_packets_bytes.inc_by(b.len() as f64);
        if let Err(err) = self.write_packet(&b, &addr).await {
            self.metrics.sent_packets_errors.inc();
            let msg = err.to_string();
            if msg.contains("connection refused") {
                // A common error during normal operations when a node shuts down (or crashes);
                // not a warning condition on the sender side.
                if self.debug_enabled() {
                    debug!(component = COMPONENT, addr, err = %msg, "WriteTo failed");
                }
            } else {
                warn!(component = COMPONENT, addr, err = %msg, "WriteTo failed");
            }
        }
    }

    /// Port of `writeTo`: connect, write `[1][len][our addr][payload][md5(payload)]`, close.
    async fn write_packet(&self, b: &[u8], addr: &str) -> io::Result<()> {
        let mut c = self.get_connection(addr, self.packet_dial_timeout).await?;

        // We need to send our address to the other side, otherwise other side can only see IP
        // and port from TCP header, which doesn't match our node address.
        let digest = Md5::digest(b);
        let our_addr = self.get_advertised_addr();
        if our_addr.len() > 255 {
            return Err(io::Error::other("local address too long"));
        }
        let mut frame = Vec::with_capacity(2 + our_addr.len() + b.len() + 16);
        frame.push(PACKET_BYTE);
        frame.push(our_addr.len() as u8);
        frame.extend_from_slice(our_addr.as_bytes());
        frame.extend_from_slice(b);
        frame.extend_from_slice(&digest);

        let write_fut = async {
            c.write_all(&frame)
                .await
                .map_err(|e| prefixed("sending data", e))?;
            c.shutdown().await.map_err(|e| prefixed("close", e))
        };
        if self.packet_write_timeout.is_zero() {
            write_fut.await?;
        } else {
            tokio::time::timeout(self.packet_write_timeout, write_fut)
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "sending data: i/o timeout"))??;
        }

        if self.debug_enabled() {
            debug!(component = COMPONENT, addr, size = b.len(), hash = %hex_lower(&digest), "WriteTo: packet sent");
        }
        Ok(())
    }

    /// Port of `DialTimeout`: opens a two-way memberlist stream connection to `addr` and sends
    /// the `[2]` stream header.
    pub async fn dial_timeout(&self, addr: &str, timeout: Duration) -> io::Result<MemberlistConn> {
        self.metrics.outgoing_streams.inc();
        let deadline = (!timeout.is_zero()).then(|| Instant::now() + timeout);
        let mut c = match self.get_connection_until(addr, deadline).await {
            Ok(c) => c,
            Err(err) => {
                self.metrics.outgoing_stream_errors.inc();
                return Err(err);
            }
        };
        let write_fut = async {
            c.write_all(&[STREAM_BYTE]).await?;
            c.flush().await
        };
        let res = match deadline {
            None => write_fut.await,
            Some(d) => tokio::time::timeout_at(d, write_fut)
                .await
                .unwrap_or_else(|_| Err(io::Error::new(io::ErrorKind::TimedOut, "i/o timeout"))),
        };
        if let Err(err) = res {
            self.metrics.outgoing_stream_errors.inc();
            return Err(err);
        }
        Ok(c)
    }

    /// Plaintext-only variant of [`TcpTransport::dial_timeout`] returning the raw [`TcpStream`]
    /// (for callers that predate TLS support; errors on a TLS transport).
    pub async fn dial_stream(&self, addr: &str, timeout: Duration) -> Result<TcpStream, io::Error> {
        if self.tls.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "memberlist TLS (tls_enabled) requires TcpTransport::dial_timeout: TLS streams cannot be returned as TcpStream",
            ));
        }
        let conn = self.dial_timeout(addr, timeout).await?;
        conn.into_tcp_stream()
            .ok_or_else(|| io::Error::other("unexpected TLS memberlist connection"))
    }

    /// Signals shutdown: new writes are rejected and the listeners stop accepting. Use
    /// [`TcpTransport::shutdown_and_wait`] to also wait like Go `Shutdown`.
    pub fn shutdown(&self) {
        self.shutdown_tx.send_replace(true);
    }

    /// Go `Shutdown`: stops accepting, then waits for all in-flight packet writes and for the
    /// listener tasks to finish.
    pub async fn shutdown_and_wait(&self) {
        self.shutdown();
        if let Ok(all) = self.write_sem.acquire_many(self.max_concurrent_writes).await {
            drop(all);
        }
        let tasks = std::mem::take(&mut *self.listener_tasks.lock());
        for t in tasks {
            let _ = t.await;
        }
    }
}

fn hex_lower(b: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(b.len() * 2);
    for byte in b {
        let _ = write!(s, "{byte:02x}");
    }
    s
}


#[cfg(test)]
mod tests {
    use super::*;

    fn host_has_ipv6_loopback() -> bool {
        std::net::TcpListener::bind("[::1]:0").is_ok()
    }

    async fn transport(bind_addr: &[&str]) -> Arc<TcpTransport> {
        let cfg = MemberlistConfig {
            bind_addr: bind_addr.iter().map(|s| s.to_string()).collect(),
            bind_port: 0,
            ..Default::default()
        };
        let (tr, _packets, _streams) = TcpTransport::new(&cfg, "test", None).await.unwrap();
        tr
    }

    /// Go `net.ListenTCP("tcp", 0.0.0.0:port)` on Linux listens dual-stack on `[::]` with
    /// IPV6_V6ONLY=0, so peers can reach the default (`0.0.0.0` / empty) bind address over IPv4
    /// and IPv6.
    #[tokio::test]
    async fn default_and_wildcard_bind_addr_listen_dual_stack_like_go() {
        if !host_has_ipv6_loopback() {
            eprintln!("SKIPPED: host has no IPv6 loopback");
            return;
        }
        for bind in [&[][..], &["0.0.0.0"][..], &["::"][..]] {
            let tr = transport(bind).await;
            let port = tr.auto_bind_port();
            assert!(tr.local_addrs[0].is_ipv6(), "{bind:?}: {}", tr.local_addrs[0]);
            TcpStream::connect(("127.0.0.1", port)).await.unwrap();
            TcpStream::connect(("::1", port)).await.unwrap();
            tr.shutdown_and_wait().await;
        }
    }

    /// A specific address binds exactly that address (no dual-stack).
    #[tokio::test]
    async fn specific_bind_addr_is_single_stack() {
        let tr = transport(&["127.0.0.1"]).await;
        let port = tr.auto_bind_port();
        assert_eq!(tr.local_addrs[0], SocketAddr::from(([127, 0, 0, 1], port)));
        TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        if host_has_ipv6_loopback() {
            assert!(TcpStream::connect(("::1", port)).await.is_err());
        }
        tr.shutdown_and_wait().await;

        // IPv4-mapped IPv6 literal: Go binds it as IPv4.
        let tr = transport(&["::ffff:127.0.0.1"]).await;
        assert!(tr.local_addrs[0].is_ipv4(), "{}", tr.local_addrs[0]);
        tr.shutdown_and_wait().await;
    }

    #[tokio::test]
    async fn bind_error_text_matches_go() {
        let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = taken.local_addr().unwrap().port();
        let cfg = MemberlistConfig {
            bind_addr: vec!["127.0.0.1".to_string()],
            bind_port: i64::from(port),
            ..Default::default()
        };
        let err = TcpTransport::new(&cfg, "test", None).await.err().unwrap();
        assert_eq!(
            err.to_string(),
            format!(
                "failed to start TCP listener on \"127.0.0.1\" port {port}: listen tcp 127.0.0.1:{port}: bind: address already in use"
            )
        );
    }

    #[tokio::test]
    async fn oversized_packet_is_rejected_without_unbounded_read() {
        let reg = Registry::new();
        let cfg = MemberlistConfig {
            bind_addr: vec!["127.0.0.1".to_string()],
            bind_port: 0,
            ..Default::default()
        };
        let (tr, mut packet_rx, _streams) = TcpTransport::new(&cfg, "test", Some(&reg)).await.unwrap();
        let port = tr.auto_bind_port();

        let mut client = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        client.write_all(&[PACKET_BYTE, 1, b'x']).await.unwrap();
        let chunk = vec![0xAAu8; 64 * 1024];
        let mut sent = 0usize;
        while sent <= MAX_PUSH_STATE_BYTES + 128 * 1024 {
            if client.write_all(&chunk).await.is_err() {
                break;
            }
            sent += chunk.len();
        }
        drop(client);

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(packet_rx.try_recv().is_err());
        assert!(tr.metrics.received_packets_errors.get() >= 1.0);
        tr.shutdown_and_wait().await;
    }
}
