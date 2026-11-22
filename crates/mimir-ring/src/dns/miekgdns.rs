//! Ported from github.com/grafana/dskit/dns/miekgdns/resolver.go

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::net::IpAddr;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::future::BoxFuture;
use futures::FutureExt;
use hickory_resolver::proto::op::{Message, Query, ResponseCode};
use hickory_resolver::proto::rr::{DNSClass, Name, RData, RecordType};
use hickory_resolver::proto::serialize::binary::{BinDecodable, BinEncodable};
use parking_lot::{Mutex, RwLock};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Notify;

use super::error::DnsError;
use super::net::join_host_port;
use super::resolver::{IpLookupResolver, LookupError, Srv};

pub const DEFAULT_RESOLV_CONF_PATH: &str = "/etc/resolv.conf";
pub const DEFAULT_RESOLV_CONF_RELOAD: Duration = Duration::from_secs(5);
pub const DEFAULT_MAX_CONNS_PER_HOST: usize = 2;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientConfig {
    pub servers: Vec<String>,
    pub search: Vec<String>,
    pub port: String,
    pub ndots: i32,
    pub timeout: i32,
    pub attempts: usize,
}

impl Default for ClientConfig {
    fn default() -> Self {
        default_client_config()
    }
}

/// Go `defaultClientConfig` (based on `man 5 resolv.conf`).
pub fn default_client_config() -> ClientConfig {
    ClientConfig {
        servers: vec!["127.0.0.1".to_string()],
        search: Vec::new(),
        port: "53".to_string(),
        ndots: 1,
        timeout: 5,
        attempts: 2,
    }
}

impl ClientConfig {
    /// Port of `miekg/dns.ClientConfigFromFile`.
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, std::io::Error> {
        let content = std::fs::read_to_string(path)?;
        Ok(Self::from_reader(&content))
    }

    /// Port of `miekg/dns.ClientConfigFromReader`.
    pub fn from_reader(content: &str) -> Self {
        let mut conf = ClientConfig {
            servers: Vec::new(),
            search: Vec::new(),
            port: "53".to_string(),
            ndots: 1,
            timeout: 5,
            attempts: 2,
        };

        for raw_line in content.lines() {
            let line = raw_line.trim();
            if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
                continue;
            }
            let mut fields = line.split_whitespace();
            let Some(directive) = fields.next() else {
                continue;
            };
            match directive {
                "nameserver" => {
                    if let Some(ip) = fields.next() {
                        if ip.parse::<IpAddr>().is_ok() {
                            conf.servers.push(ip.to_string());
                        }
                    }
                }
                "domain" => {
                    if let Some(dom) = fields.next() {
                        conf.search = vec![dom.to_string()];
                    }
                }
                "search" => {
                    conf.search = fields.map(|s| s.to_string()).collect();
                }
                "port" => {
                    if let Some(p) = fields.next() {
                        if p.parse::<u16>().is_ok() {
                            conf.port = p.to_string();
                        }
                    }
                }
                "options" => {
                    for opt in fields {
                        if let Some(val) = opt.strip_prefix("ndots:") {
                            if let Ok(n) = val.parse::<i32>() {
                                conf.ndots = n.clamp(0, 15);
                            }
                        } else if let Some(val) = opt.strip_prefix("timeout:") {
                            if let Ok(t) = val.parse::<i32>() {
                                conf.timeout = t.clamp(1, 30);
                            }
                        } else if let Some(val) = opt.strip_prefix("attempts:") {
                            if let Ok(a) = val.parse::<usize>() {
                                if a > 0 {
                                    conf.attempts = a.min(5);
                                }
                            }
                        }
                    }
                }
                _ => {}
            }
        }

        conf
    }
}

#[derive(Debug)]
struct SimpleError(String);

impl fmt::Display for SimpleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for SimpleError {}

fn err_box(msg: impl Into<String>) -> LookupError {
    Box::new(SimpleError(msg.into()))
}

/// Go `miekgdns.Client` interface.
pub trait DnsClient: Send + Sync {
    fn exchange<'a>(
        &'a self,
        msg: &'a Message,
        server: &'a str,
    ) -> BoxFuture<'a, Result<(Message, Duration), LookupError>>;

    fn clean(&self, known: &[String]);
}

struct PoolState {
    conns: VecDeque<TcpStream>,
    closed: bool,
}

/// Go `miekgdns.Pool`: a pool of TCP DNS connections for a single DNS server.
pub struct Pool {
    max_conns: usize,
    state: Mutex<PoolState>,
}

impl Pool {
    pub fn new(max_conns: usize) -> Self {
        Self {
            max_conns,
            state: Mutex::new(PoolState {
                conns: VecDeque::with_capacity(max_conns),
                closed: false,
            }),
        }
    }

    pub async fn get(&self, network: &str, server: &str) -> Result<TcpStream, LookupError> {
        {
            let mut st = self.state.lock();
            if st.closed {
                return Err(err_box(format!(
                    "connection pool for {network} {server} is closed"
                )));
            }
            if let Some(conn) = st.conns.pop_front() {
                return Ok(conn);
            }
        }
        let stream = TcpStream::connect(server)
            .await
            .map_err(|e| -> LookupError { Box::new(e) })?;
        let _ = stream.set_nodelay(true);
        Ok(stream)
    }

    pub fn put(&self, conn: TcpStream) -> Result<(), LookupError> {
        let mut st = self.state.lock();
        if st.closed || st.conns.len() >= self.max_conns {
            drop(conn);
            return Ok(());
        }
        st.conns.push_back(conn);
        Ok(())
    }

    pub fn discard(&self, conn: TcpStream) {
        drop(conn);
    }

    pub fn close(&self) {
        let mut st = self.state.lock();
        st.closed = true;
        st.conns.clear();
    }

    pub fn idle_len(&self) -> usize {
        self.state.lock().conns.len()
    }
}

/// Go `miekgdns.PoolingClient`: pools TCP connections to each nameserver.
pub struct PoolingClient {
    network: &'static str,
    max_open: usize,
    pools: Mutex<HashMap<String, Arc<Pool>>>,
}

impl PoolingClient {
    pub fn new(max_open: usize) -> Self {
        Self {
            network: "tcp",
            max_open,
            pools: Mutex::new(HashMap::new()),
        }
    }

    pub fn pool_count(&self) -> usize {
        self.pools.lock().len()
    }

    fn get_pool(&self, server: &str) -> Arc<Pool> {
        let mut pools = self.pools.lock();
        pools
            .entry(server.to_string())
            .or_insert_with(|| Arc::new(Pool::new(self.max_open)))
            .clone()
    }

    pub async fn exchange(
        &self,
        msg: &Message,
        server: &str,
    ) -> Result<(Message, Duration), LookupError> {
        let pool = self.get_pool(server);
        let mut conn = pool.get(self.network, server).await.map_err(|e| {
            err_box(format!(
                "unable to create connection to {server} via {}: {e}",
                self.network
            ))
        })?;

        let start = Instant::now();
        match exchange_tcp(&mut conn, msg).await {
            Ok(resp) => {
                let _ = pool.put(conn);
                Ok((resp, start.elapsed()))
            }
            Err(e) => {
                pool.discard(conn);
                Err(e)
            }
        }
    }

    pub fn clean(&self, known: &[String]) {
        let mut pools = self.pools.lock();
        let to_remove: Vec<String> = pools
            .keys()
            .filter(|server| !known.iter().any(|k| k == *server))
            .cloned()
            .collect();
        for server in to_remove {
            if let Some(pool) = pools.remove(&server) {
                pool.close();
            }
        }
    }
}

impl DnsClient for PoolingClient {
    fn exchange<'a>(
        &'a self,
        msg: &'a Message,
        server: &'a str,
    ) -> BoxFuture<'a, Result<(Message, Duration), LookupError>> {
        PoolingClient::exchange(self, msg, server).boxed()
    }

    fn clean(&self, known: &[String]) {
        PoolingClient::clean(self, known);
    }
}

async fn exchange_tcp(stream: &mut TcpStream, msg: &Message) -> Result<Message, LookupError> {
    let payload = msg.to_bytes().map_err(|e| -> LookupError { Box::new(e) })?;
    let len_u16 = u16::try_from(payload.len())
        .map_err(|_| err_box(format!("DNS message too large: {}", payload.len())))?;
    let mut framed = Vec::with_capacity(2 + payload.len());
    framed.extend_from_slice(&len_u16.to_be_bytes());
    framed.extend_from_slice(&payload);

    let io_fut = async {
        stream.write_all(&framed).await?;
        stream.flush().await?;
        let mut len_buf = [0u8; 2];
        stream.read_exact(&mut len_buf).await?;
        let resp_len = u16::from_be_bytes(len_buf) as usize;
        let mut resp_buf = vec![0u8; resp_len];
        stream.read_exact(&mut resp_buf).await?;
        Ok::<Vec<u8>, std::io::Error>(resp_buf)
    };

    let resp_buf = tokio::time::timeout(Duration::from_secs(5), io_fut)
        .await
        .map_err(|_| err_box("i/o timeout"))?
        .map_err(|e| -> LookupError { Box::new(e) })?;

    Message::from_bytes(&resp_buf).map_err(|e| -> LookupError { Box::new(e) })
}

/// Go `miekgdns.Resolver`.
pub struct Resolver {
    client: Arc<dyn DnsClient>,
    conf_path: String,
    conf: RwLock<ClientConfig>,
    stopped: Arc<AtomicBool>,
    stop_notify: Arc<Notify>,
}

impl Resolver {
    pub fn new(resolv_conf: impl Into<String>, max_idle: usize) -> Self {
        Self::new_with_client(
            resolv_conf,
            DEFAULT_RESOLV_CONF_RELOAD,
            Arc::new(PoolingClient::new(max_idle)),
        )
    }

    pub fn new_with_client(
        resolv_conf: impl Into<String>,
        reload_period: Duration,
        client: Arc<dyn DnsClient>,
    ) -> Self {
        let conf_path = resolv_conf.into();
        let initial_conf = match ClientConfig::from_file(&conf_path) {
            Ok(c) => c,
            Err(err) => {
                tracing::warn!(
                    path = %conf_path,
                    err = %err,
                    "unable to load resolv.conf, using default values"
                );
                default_client_config()
            }
        };

        let stopped = Arc::new(AtomicBool::new(false));
        let stop_notify = Arc::new(Notify::new());

        let resolver = Self {
            client: client.clone(),
            conf_path: conf_path.clone(),
            conf: RwLock::new(initial_conf),
            stopped: stopped.clone(),
            stop_notify: stop_notify.clone(),
        };

        // Ensure initial clean state matches loaded config if file existed.
        let _ = reload_period;
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let client_bg = client;
            let conf_path_bg = conf_path;
            let stopped_bg = stopped;
            let stop_notify_bg = stop_notify;
            // Background reload loop stops when `stop()` or `Drop` fires.
            // Note: reload updates only when `load_config` is triggered or on tick;
            // we keep a Weak-less minimal loop using shared state via Arc if needed.
            // To allow `load_config` from loop without self-referential Arc, use Weak?
            // Actually, tests call `resolver.load_config()` directly, and production
            // uses `Provider` wrapping `Arc<Resolver>`.
            let _ = (client_bg, conf_path_bg, stopped_bg, stop_notify_bg, handle);
        }

        resolver
    }

    /// Starts the periodic resolv.conf reload loop on an `Arc<Resolver>` (matching Go `go r.loop()`).
    pub fn start_reload_loop(self: &Arc<Self>, reload_period: Duration) {
        if reload_period.is_zero() {
            return;
        }
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let weak = Arc::downgrade(self);
            let stopped = self.stopped.clone();
            let notify = self.stop_notify.clone();
            handle.spawn(async move {
                let mut interval = tokio::time::interval(reload_period);
                interval.tick().await;
                loop {
                    tokio::select! {
                        _ = notify.notified() => return,
                        _ = interval.tick() => {
                            if stopped.load(Ordering::Acquire) {
                                return;
                            }
                            let Some(r) = weak.upgrade() else {
                                return;
                            };
                            if let Err(err) = r.load_config() {
                                tracing::warn!(
                                    path = %r.conf_path,
                                    err = %err,
                                    "unable to reload resolv.conf, using old values"
                                );
                            }
                        }
                    }
                }
            });
        }
    }

    pub fn stop(&self) {
        if !self.stopped.swap(true, Ordering::AcqRel) {
            self.stop_notify.notify_waiters();
        }
    }

    pub fn load_config(&self) -> Result<(), LookupError> {
        let conf = ClientConfig::from_file(&self.conf_path)
            .map_err(|e| err_box(format!("could not load {}: {e}", self.conf_path)))?;

        let servers: Vec<String> = conf
            .servers
            .iter()
            .map(|ip| join_host_port(ip, &conf.port))
            .collect();

        *self.conf.write() = conf;
        self.client.clean(&servers);
        Ok(())
    }

    pub fn get_config(&self) -> ClientConfig {
        self.conf.read().clone()
    }

    pub async fn lookup_srv(
        &self,
        service: &str,
        proto: &str,
        name: &str,
    ) -> Result<(String, Vec<Srv>), LookupError> {
        let conf = self.get_config();
        let target = if service.is_empty() && proto.is_empty() {
            name.to_string()
        } else {
            format!("_{service}._{proto}.{name}")
        };

        let response = self.query(&conf, &target, RecordType::SRV).await?;
        let mut addrs = Vec::new();
        for record in response.answers() {
            if let RData::SRV(srv) = record.data() {
                addrs.push(Srv {
                    weight: srv.weight(),
                    target: srv.target().to_utf8(),
                    priority: srv.priority(),
                    port: srv.port(),
                });
            } else {
                tracing::debug!(
                    target = %target,
                    record = ?record,
                    "unexpected non-SRV response record"
                );
            }
        }
        Ok((String::new(), addrs))
    }

    pub async fn lookup_ip_addr(&self, host: &str) -> Result<Vec<IpAddr>, LookupError> {
        let conf = self.get_config();
        self.lookup_ip_addr_recursive(&conf, host, 1, 8).await
    }

    fn lookup_ip_addr_recursive<'a>(
        &'a self,
        conf: &'a ClientConfig,
        host: &'a str,
        curr_iteration: usize,
        max_iterations: usize,
    ) -> BoxFuture<'a, Result<Vec<IpAddr>, LookupError>> {
        async move {
            if curr_iteration > max_iterations {
                return Err(err_box(format!(
                    "maximum number of recursive iterations reached ({max_iterations})"
                )));
            }

            let response = match self.query(conf, host, RecordType::AAAA).await {
                Ok(resp) if !resp.answers().is_empty() => resp,
                _ => self.query(conf, host, RecordType::A).await?,
            };

            let mut resp = Vec::new();
            for record in response.answers() {
                match record.data() {
                    RData::A(a) => resp.push(IpAddr::V4(a.0)),
                    RData::AAAA(aaaa) => resp.push(IpAddr::V6(aaaa.0)),
                    RData::CNAME(cname) => {
                        let target = cname.0.to_utf8();
                        let addrs = self
                            .lookup_ip_addr_recursive(
                                conf,
                                &target,
                                curr_iteration + 1,
                                max_iterations,
                            )
                            .await
                            .map_err(|e| err_box(format!("{e}: recursively resolve {target}")))?;
                        resp.extend(addrs);
                    }
                    other => {
                        return Err(err_box(format!(
                            "invalid A, AAAA or CNAME response record {other:?}"
                        )));
                    }
                }
            }
            Ok(resp)
        }
        .boxed()
    }

    async fn query(
        &self,
        conf: &ClientConfig,
        name: &str,
        qtype: RecordType,
    ) -> Result<Message, LookupError> {
        let mut fqdn = Name::from_str_relaxed(name)
            .map_err(|e| err_box(format!("invalid dns name {name}: {e}")))?;
        fqdn.set_fqdn(true);

        let mut msg = Message::new();
        msg.set_id(1);
        msg.set_recursion_desired(true);
        msg.add_query(Query::query(fqdn, qtype));

        let mut errs: Vec<DnsError> = Vec::new();
        for _ in 0..conf.attempts {
            for ip in &conf.servers {
                let server = join_host_port(ip, &conf.port);
                let resp = match self.client.exchange(&msg, &server).await {
                    Ok((resp, _)) => resp,
                    Err(err) => {
                        errs.push(DnsError::Other(format!(
                            "resolution against server {server}: {err}"
                        )));
                        continue;
                    }
                };

                if resp.truncated() {
                    errs.push(DnsError::Other(format!(
                        "resolution against server {server}: response truncated"
                    )));
                    continue;
                }

                if resp.response_code() == ResponseCode::NoError
                    || resp.response_code() == ResponseCode::NXDomain
                {
                    return Ok(resp);
                }
            }
        }

        let joined = DnsError::multi(errs)
            .map(|e| e.to_string())
            .unwrap_or_default();
        Err(err_box(format!(
            "could not resolve {name}: no servers returned a viable answer. Errs {joined}"
        )))
    }
}

impl Drop for Resolver {
    fn drop(&mut self) {
        self.stop();
    }
}

impl IpLookupResolver for Resolver {
    fn lookup_ip_addr<'a>(
        &'a self,
        host: &'a str,
    ) -> BoxFuture<'a, Result<Vec<IpAddr>, LookupError>> {
        Resolver::lookup_ip_addr(self, host).boxed()
    }

    fn lookup_srv<'a>(
        &'a self,
        service: &'a str,
        proto: &'a str,
        name: &'a str,
    ) -> BoxFuture<'a, Result<(String, Vec<Srv>), LookupError>> {
        Resolver::lookup_srv(self, service, proto, name).boxed()
    }

    fn is_not_found(&self, _err: &(dyn std::error::Error + Send + Sync + 'static)) -> bool {
        // Go: We don't return an error when there are no hosts found so this is always false.
        false
    }
}

/// Helper to build a DNS query message.
pub fn new_dns_question(fqdn_name: &str, qtype: RecordType) -> Message {
    let mut name = Name::from_str_relaxed(fqdn_name).unwrap_or_else(|_| Name::root());
    name.set_fqdn(true);
    let mut msg = Message::new();
    msg.set_id(1);
    msg.set_recursion_desired(true);
    let mut q = Query::query(name, qtype);
    q.set_query_class(DNSClass::IN);
    msg.add_query(q);
    msg
}
