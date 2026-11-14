//! TCP transport for memberlist gossip (packet + stream multiplexing over TCP).
//!
//! Ported from github.com/grafana/dskit/kv/memberlist/tcp_transport.go

use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use md5::{Digest, Md5};
use mimir_config::server::MemberlistConfig;
use parking_lot::RwLock;
use prometheus::{Counter, Opts, Registry};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Semaphore};
use tracing::{debug, error, warn};

const PACKET_BYTE: u8 = 1;
const STREAM_BYTE: u8 = 2;

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
}

impl TransportMetrics {
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
                "Total bytes received as packets",
            ),
            received_packets_errors: mk(
                "packets_received_errors_total",
                "Number of errors when receiving memberlist packets",
            ),
            sent_packets: mk("packets_sent_total", "Number of memberlist packets sent"),
            sent_packets_bytes: mk("packets_sent_bytes_total", "Total bytes sent as packets"),
            sent_packets_errors: mk(
                "packets_sent_errors_total",
                "Number of errors when sending memberlist packets",
            ),
            dropped_packets: mk(
                "packets_dropped_total",
                "Number of dropped memberlist packets",
            ),
            unknown_connections: mk(
                "unknown_connections_total",
                "Number of unknown TCP connections (not a packet or stream)",
            ),
        }
    }
}

pub struct TcpTransport {
    packet_dial_timeout: Duration,
    packet_write_timeout: Duration,
    acquire_writer_timeout: Duration,
    write_sem: Arc<Semaphore>,
    local_addrs: Vec<SocketAddr>,
    advertise_addr: RwLock<String>,
    packet_tx: mpsc::Sender<ReceivedPacket>,
    stream_tx: mpsc::Sender<TcpStream>,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    metrics: TransportMetrics,
}

impl TcpTransport {
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
        let bind_addrs = if cfg.bind_addr.is_empty() {
            vec!["0.0.0.0".to_string()]
        } else {
            cfg.bind_addr.clone()
        };

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
            let listener = TcpListener::bind(SocketAddr::new(ip, port)).await?;
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

        let concurrent_writes = cfg.max_concurrent_writes.max(1) as usize;
        let transport = Arc::new(Self {
            packet_dial_timeout: cfg.packet_dial_timeout.as_std().unwrap_or(Duration::ZERO),
            packet_write_timeout: cfg.packet_write_timeout.as_std().unwrap_or(Duration::ZERO),
            acquire_writer_timeout: cfg.acquire_writer_timeout.as_std().unwrap_or(Duration::ZERO),
            write_sem: Arc::new(Semaphore::new(concurrent_writes)),
            local_addrs,
            advertise_addr: RwLock::new(String::new()),
            packet_tx,
            stream_tx,
            shutdown_tx,
            metrics: TransportMetrics::new(reg, metrics_namespace),
        });

        for listener in listeners {
            let tr = transport.clone();
            let mut shutdown_rx = tr.shutdown_tx.subscribe();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        biased;
                        _ = shutdown_rx.changed() => {
                            if *shutdown_rx.borrow() {
                                break;
                            }
                        }
                        res = listener.accept() => {
                            match res {
                                Ok((stream, _remote)) => {
                                    let tr_conn = tr.clone();
                                    tokio::spawn(async move {
                                        tr_conn.handle_connection(stream).await;
                                    });
                                }
                                Err(err) => {
                                    if *shutdown_rx.borrow() {
                                        break;
                                    }
                                    error!(%err, "Error accepting memberlist TCP connection");
                                    tokio::time::sleep(Duration::from_millis(50)).await;
                                }
                            }
                        }
                    }
                }
            });
        }

        Ok((transport, packet_rx, stream_rx))
    }

    pub fn auto_bind_port(&self) -> u16 {
        self.local_addrs[0].port()
    }

    pub fn final_advertise_addr(
        &self,
        configured_addr: &str,
        configured_port: i64,
    ) -> Result<(IpAddr, u16), io::Error> {
        let (ip, port) = if !configured_addr.is_empty() {
            let ip: IpAddr = configured_addr.parse().map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("failed to parse advertise address {configured_addr:?}"),
                )
            })?;
            let p = if configured_port > 0 {
                configured_port as u16
            } else {
                self.auto_bind_port()
            };
            (ip, p)
        } else {
            let bound = self.local_addrs[0];
            let ip = if bound.ip().is_unspecified() {
                detect_local_ip(bound.is_ipv6()).unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST))
            } else {
                bound.ip()
            };
            (ip, self.auto_bind_port())
        };

        let formatted = SocketAddr::new(ip, port).to_string();
        *self.advertise_addr.write() = formatted;
        Ok((ip, port))
    }

    pub fn get_advertised_addr(&self) -> String {
        self.advertise_addr.read().clone()
    }

    async fn handle_connection(&self, mut conn: TcpStream) {
        let mut msg_type = [0u8; 1];
        if let Err(err) = conn.read_exact(&mut msg_type).await {
            warn!(%err, "failed to read memberlist message type");
            return;
        }

        match msg_type[0] {
            STREAM_BYTE => {
                self.metrics.incoming_streams.inc();
                let _ = self.stream_tx.send(conn).await;
            }
            PACKET_BYTE => {
                self.metrics.received_packets.inc();
                let mut addr_len_buf = [0u8; 1];
                if let Err(err) = conn.read_exact(&mut addr_len_buf).await {
                    self.metrics.received_packets_errors.inc();
                    warn!(%err, "error reading node address length from packet");
                    return;
                }
                let mut addr_buf = vec![0u8; addr_len_buf[0] as usize];
                if let Err(err) = conn.read_exact(&mut addr_buf).await {
                    self.metrics.received_packets_errors.inc();
                    warn!(%err, "error reading node address from packet");
                    return;
                }
                let from = String::from_utf8_lossy(&addr_buf).into_owned();

                let mut buf = Vec::new();
                if let Err(err) = conn.read_to_end(&mut buf).await {
                    self.metrics.received_packets_errors.inc();
                    warn!(%err, "error reading packet data");
                    return;
                }
                if buf.len() < 16 {
                    self.metrics.received_packets_errors.inc();
                    warn!(data_len = buf.len(), "not enough packet data received");
                    return;
                }

                let split = buf.len() - 16;
                let received_digest = &buf[split..];
                let expected_digest = Md5::digest(&buf[..split]);
                if received_digest != expected_digest.as_slice() {
                    self.metrics.received_packets_errors.inc();
                    warn!("memberlist packet digest mismatch");
                }
                buf.truncate(split);

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
                error!(msg_type = other, "unknown memberlist TCP connection type");
            }
        }
    }

    /// Sends a memberlist packet over a short-lived TCP connection.
    pub async fn write_to(&self, payload: Vec<u8>, addr: String) {
        if *self.shutdown_tx.borrow() {
            return;
        }

        let permit_res = if self.acquire_writer_timeout.is_zero() {
            Ok(self.write_sem.clone().acquire_owned().await)
        } else {
            tokio::time::timeout(
                self.acquire_writer_timeout,
                self.write_sem.clone().acquire_owned(),
            )
            .await
        };

        let _permit = match permit_res {
            Ok(Ok(p)) => p,
            _ => {
                self.metrics.dropped_packets.inc();
                debug!(addr, "WriteTo failed to acquire writer, dropping message");
                return;
            }
        };

        self.metrics.sent_packets.inc();
        self.metrics.sent_packets_bytes.inc_by(payload.len() as f64);

        if let Err(err) = self.write_to_inner(&payload, &addr).await {
            self.metrics.sent_packets_errors.inc();
            debug!(addr, %err, "WriteTo failed");
        }
    }

    async fn write_to_inner(&self, payload: &[u8], addr: &str) -> Result<(), io::Error> {
        let mut stream = if self.packet_dial_timeout.is_zero() {
            TcpStream::connect(addr).await?
        } else {
            tokio::time::timeout(self.packet_dial_timeout, TcpStream::connect(addr))
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "packet dial timeout"))??
        };

        let our_addr = self.get_advertised_addr();
        if our_addr.len() > 255 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "local address too long",
            ));
        }

        let digest = Md5::digest(payload);
        let mut frame = Vec::with_capacity(2 + our_addr.len() + payload.len() + 16);
        frame.push(PACKET_BYTE);
        frame.push(our_addr.len() as u8);
        frame.extend_from_slice(our_addr.as_bytes());
        frame.extend_from_slice(payload);
        frame.extend_from_slice(&digest);

        let write_fut = async {
            stream.write_all(&frame).await?;
            stream.shutdown().await?;
            Ok::<(), io::Error>(())
        };

        if self.packet_write_timeout.is_zero() {
            write_fut.await
        } else {
            tokio::time::timeout(self.packet_write_timeout, write_fut)
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "packet write timeout"))?
        }
    }

    /// Opens a two-way memberlist stream connection to `addr` and sends the `[2]` stream header.
    pub async fn dial_stream(&self, addr: &str, timeout: Duration) -> Result<TcpStream, io::Error> {
        self.metrics.outgoing_streams.inc();
        let res = async {
            let mut stream = if timeout.is_zero() {
                TcpStream::connect(addr).await?
            } else {
                tokio::time::timeout(timeout, TcpStream::connect(addr))
                    .await
                    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "stream dial timeout"))??
            };
            stream.write_all(&[STREAM_BYTE]).await?;
            Ok::<TcpStream, io::Error>(stream)
        }
        .await;

        if res.is_err() {
            self.metrics.outgoing_stream_errors.inc();
        }
        res
    }

    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }
}

fn detect_local_ip(prefer_v6: bool) -> Option<IpAddr> {
    let target = if prefer_v6 { "[2001:db8::1]:80" } else { "8.8.8.8:80" };
    if let Ok(sock) = std::net::UdpSocket::bind(if prefer_v6 { "[::]:0" } else { "0.0.0.0:0" }) {
        if sock.connect(target).is_ok() {
            if let Ok(local) = sock.local_addr() {
                return Some(local.ip());
            }
        }
    }
    None
}
