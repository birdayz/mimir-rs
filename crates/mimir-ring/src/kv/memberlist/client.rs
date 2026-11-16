//! Memberlist gossip-based KV store and client.
//!
//! Ported from:
//! - github.com/grafana/dskit/kv/memberlist/memberlist_client.go
//! - github.com/grafana/dskit/kv/memberlist/broadcast.go
//! - github.com/grafana/dskit/kv/memberlist/http_status_handler.go

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use mimir_common::backoff::{Backoff, BackoffConfig};
use mimir_common::services::ServiceContext;
use mimir_config::server::MemberlistConfig;
use mimir_proto::memberlistpb::KeyValuePair;
use mimir_proto::prost::Message as _;
use parking_lot::{Mutex, RwLock};
use prometheus::{Counter, Gauge, Opts, Registry};
use rand::seq::SliceRandom;
use rand::Rng;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, Notify};
use tracing::{debug, error, warn};

use super::mergeable::Mergeable;
use super::node_meta::{encode_node_metadata, NodeRole};
use super::tcp_transport::{ReceivedPacket, TcpTransport};
use super::wire::{
    add_label_header, compress_payload, decode_ack_resp, decode_alive, decode_compound_message,
    decode_dead, decode_indirect_ping, decode_nack_resp, decode_ping, decode_push_pull,
    decode_suspect, decompress_buffer, encode_ack_resp, encode_alive, encode_dead, encode_err_resp,
    encode_nack_resp, encode_ping, encode_push_pull, make_compound_messages,
    remove_label_header, unwrap_crc32, wrap_crc32, AckResp, Alive, CompressionType, Dead, ErrResp,
    MessageType, NackResp, NodeStateType, Ping, PushNodeState, PushPullHeader, Suspect,
    COMPOUND_HEADER_OVERHEAD, COMPOUND_OVERHEAD, MAX_PUSH_STATE_BYTES,
    PROTOCOL_VERSION_2_COMPATIBLE, PROTOCOL_VERSION_MAX, PROTOCOL_VERSION_MIN,
};
use crate::kv::{BoxFuture, CasFn, CasOutcome, Codec, KvClient, KvError};

const MAX_CAS_RETRIES: usize = 10;
const NO_CHANGE_DETECTED_RETRY_SLEEP: Duration = Duration::from_secs(1);
const LENGTH_PREFIX_SIZE: usize = 4;
const UDP_BUFFER_SIZE: usize = 10 * 1024 * 1024;
const PUSH_PULL_SCALE_THRESHOLD: usize = 32;

/// Snappy block encoding of empty byte slice `[]`.
const EMPTY_SNAPPY_DATA: &[u8] = &[0x00];

type MergeValueResult = (
    Option<Box<dyn Mergeable>>,
    usize,
    bool,
    Option<SystemTime>,
);

#[derive(Clone)]
struct RingBroadcast {
    key: String,
    content: HashSet<String>,
    version: usize,
    msg: Vec<u8>,
    transmits: usize,
}

impl RingBroadcast {
    fn invalidates(&self, other: &RingBroadcast) -> bool {
        if self.key != other.key || other.version >= self.version {
            return false;
        }
        other.content.iter().all(|c| self.content.contains(c))
    }
}

struct TransmitQueue {
    retransmit_mult: usize,
    items: Mutex<Vec<RingBroadcast>>,
}

impl TransmitQueue {
    fn new(retransmit_mult: usize) -> Self {
        Self {
            retransmit_mult,
            items: Mutex::new(Vec::new()),
        }
    }

    fn queue_broadcast(&self, b: RingBroadcast) {
        let mut items = self.items.lock();
        items.retain(|existing| !b.invalidates(existing));
        items.push(b);
    }

    fn num_queued(&self) -> usize {
        self.items.lock().len()
    }

    fn get_broadcasts(&self, overhead: usize, limit: usize, num_nodes: usize) -> Vec<Vec<u8>> {
        let mut items = self.items.lock();
        if items.is_empty() {
            return Vec::new();
        }
        let max_transmits = retransmit_limit(self.retransmit_mult, num_nodes).max(1);
        let mut bytes_used = 0usize;
        let mut out = Vec::new();
        let mut i = 0;

        while i < items.len() {
            let msg_len = items[i].msg.len() + overhead;
            if bytes_used + msg_len > limit && !out.is_empty() {
                i += 1;
                continue;
            }
            bytes_used += msg_len;
            out.push(items[i].msg.clone());
            items[i].transmits += 1;
            if items[i].transmits >= max_transmits {
                items.remove(i);
            } else {
                i += 1;
            }
            if bytes_used >= limit {
                break;
            }
        }
        out
    }
}

fn retransmit_limit(retransmit_mult: usize, n: usize) -> usize {
    let node_scale = ((n + 1) as f64).log10().ceil() as usize;
    retransmit_mult * node_scale.max(1)
}

#[derive(Clone, Debug)]
pub struct NodeState {
    pub name: String,
    pub addr: Vec<u8>,
    pub port: u16,
    pub meta: Option<Vec<u8>>,
    pub incarnation: u32,
    pub state: NodeStateType,
    pub state_change: SystemTime,
    pub vsn: Vec<u8>,
}

impl NodeState {
    pub fn address(&self) -> String {
        format_ip_port(&self.addr, self.port)
    }

    pub fn dead_or_left(&self) -> bool {
        matches!(self.state, NodeStateType::Dead | NodeStateType::Left)
    }
}

fn format_ip_port(addr: &[u8], port: u16) -> String {
    let ip: IpAddr = if addr.len() == 4 {
        let a: [u8; 4] = addr.try_into().expect("4 bytes");
        IpAddr::from(a)
    } else if addr.len() == 16 {
        let a: [u8; 16] = addr.try_into().expect("16 bytes");
        IpAddr::from(a)
    } else {
        return format!("127.0.0.1:{port}");
    };
    SocketAddr::new(ip, port).to_string()
}

fn ip_to_bytes(ip: IpAddr) -> Vec<u8> {
    match ip {
        IpAddr::V4(v4) => v4.octets().to_vec(),
        IpAddr::V6(v6) => v6.octets().to_vec(),
    }
}

struct ValueDesc {
    value: Box<dyn Mergeable>,
    version: usize,
    codec_id: String,
    deleted: bool,
    update_time: Option<SystemTime>,
}

impl Clone for ValueDesc {
    fn clone(&self) -> Self {
        Self {
            value: self.value.clone_mergeable(),
            version: self.version,
            codec_id: self.codec_id.clone(),
            deleted: self.deleted,
            update_time: self.update_time,
        }
    }
}

struct WatcherEntry {
    notify: Arc<Notify>,
}

struct PrefixWatcherEntry {
    id: usize,
    tx: mpsc::Sender<String>,
}

static NEXT_PREFIX_WATCHER_ID: AtomicUsize = AtomicUsize::new(1);

struct KeyWatcherGuard {
    kv: Arc<MemberlistKv>,
    key: String,
    notify: Arc<Notify>,
}

impl Drop for KeyWatcherGuard {
    fn drop(&mut self) {
        let mut watchers = self.kv.watchers.lock();
        if let Some(list) = watchers.get_mut(&self.key) {
            list.retain(|w| !Arc::ptr_eq(&w.notify, &self.notify));
            if list.is_empty() {
                watchers.remove(&self.key);
            }
        }
    }
}

struct PrefixWatcherGuard {
    kv: Arc<MemberlistKv>,
    prefix: String,
    id: usize,
}

impl Drop for PrefixWatcherGuard {
    fn drop(&mut self) {
        let mut pw = self.kv.prefix_watchers.lock();
        if let Some(list) = pw.get_mut(&self.prefix) {
            list.retain(|entry| entry.id != self.id);
            if list.is_empty() {
                pw.remove(&self.prefix);
            }
        }
    }
}

struct MemberlistMetrics {
    received_messages: Counter,
    invalid_messages: Counter,
    number_of_pulls: Counter,
    number_of_pushes: Counter,
    cas_attempts: Counter,
    cas_successes: Counter,
    cas_failures: Counter,
    members_count: Gauge,
}

impl MemberlistMetrics {
    fn new(reg: Option<&Registry>, ns: &str) -> Self {
        let mk_c = |name: &str, help: &str| {
            let full = if ns.is_empty() {
                format!("memberlist_client_{name}")
            } else {
                format!("{ns}_memberlist_client_{name}")
            };
            let c = Counter::with_opts(Opts::new(full, help)).expect("valid counter");
            if let Some(r) = reg {
                let _ = r.register(Box::new(c.clone()));
            }
            c
        };
        let mk_g = |name: &str, help: &str| {
            let full = if ns.is_empty() {
                format!("memberlist_client_{name}")
            } else {
                format!("{ns}_memberlist_client_{name}")
            };
            let g = Gauge::with_opts(Opts::new(full, help)).expect("valid gauge");
            if let Some(r) = reg {
                let _ = r.register(Box::new(g.clone()));
            }
            g
        };

        Self {
            received_messages: mk_c("messages_received_total", "Number of received messages"),
            invalid_messages: mk_c(
                "messages_received_invalid_total",
                "Number of invalid received messages",
            ),
            number_of_pulls: mk_c("state_pulls_total", "Number of state pulls"),
            number_of_pushes: mk_c("state_pushes_total", "Number of state pushes"),
            cas_attempts: mk_c("cas_attempts_total", "Number of CAS attempts"),
            cas_successes: mk_c("cas_successes_total", "Number of CAS successes"),
            cas_failures: mk_c("cas_failures_total", "Number of CAS failures"),
            members_count: mk_g("cluster_members_count", "Number of members in memberlist cluster"),
        }
    }
}

/// Memberlist gossip-based KV store matching `github.com/grafana/dskit/kv/memberlist.KV`.
pub struct MemberlistKv {
    cfg: MemberlistConfig,
    node_name: String,
    compression_algo: CompressionType,
    codecs: RwLock<HashMap<String, Arc<dyn Codec>>>,
    node_meta: Option<Vec<u8>>,

    transport: Arc<TcpTransport>,
    advertise_ip: IpAddr,
    advertise_port: u16,

    store: Mutex<HashMap<String, ValueDesc>>,
    watchers: Mutex<HashMap<String, Vec<WatcherEntry>>>,
    prefix_watchers: Mutex<HashMap<String, Vec<PrefixWatcherEntry>>>,

    nodes: RwLock<Vec<NodeState>>,
    incarnation: AtomicU32,
    sequence_num: AtomicU32,
    ack_handlers: Mutex<HashMap<u32, oneshot::Sender<AckResp>>>,

    local_broadcasts: Arc<TransmitQueue>,
    gossip_broadcasts: Arc<TransmitQueue>,
    gossip_notify: Arc<Notify>,

    running: AtomicBool,
    leaving: AtomicBool,
    shutdown_ctx: ServiceContext,
    probe_index: AtomicUsize,

    metrics: MemberlistMetrics,
}

impl MemberlistKv {
    /// Creates and binds a new [`MemberlistKv`] with the given config and codecs.
    /// Call [`MemberlistKv::start`] to start background loops and join seed peers.
    pub async fn bind(
        cfg: MemberlistConfig,
        codecs: Vec<Arc<dyn Codec>>,
        metrics_namespace: &str,
        reg: Option<&Registry>,
    ) -> Result<(Arc<Self>, mpsc::Receiver<ReceivedPacket>, mpsc::Receiver<TcpStream>), KvError>
    {
        let mut node_name = if cfg.node_name.is_empty() {
            default_hostname()
        } else {
            cfg.node_name.clone()
        };
        if cfg.randomize_node_name {
            let suffix: u32 = rand::thread_rng().gen();
            node_name = format!("{node_name}-{suffix:08x}");
        }

        let compression_algo = match cfg.compression_algorithm.as_str() {
            "snappy" => CompressionType::Snappy,
            _ => CompressionType::Lzw,
        };

        let node_meta = if cfg.zone_aware_routing.enabled {
            let role = NodeRole::parse(&cfg.zone_aware_routing.role)
                .map_err(KvError::InvalidConfig)?;
            Some(
                encode_node_metadata(role, &cfg.zone_aware_routing.instance_availability_zone)
                    .map_err(KvError::InvalidConfig)?,
            )
        } else {
            None
        };

        let (transport, packet_rx, stream_rx) =
            TcpTransport::bind(&cfg, metrics_namespace, reg).await?;
        let (advertise_ip, advertise_port) =
            transport.final_advertise_addr(&cfg.advertise_addr, cfg.advertise_port)?;

        let mut codec_map = HashMap::new();
        for c in codecs {
            codec_map.insert(c.codec_id().to_string(), c);
        }

        let retransmit_mult = cfg.retransmit_factor.max(1) as usize;
        let local_node = NodeState {
            name: node_name.clone(),
            addr: ip_to_bytes(advertise_ip),
            port: advertise_port,
            meta: node_meta.clone(),
            incarnation: 1,
            state: NodeStateType::Alive,
            state_change: SystemTime::now(),
            vsn: vec![
                PROTOCOL_VERSION_MIN,
                PROTOCOL_VERSION_MAX,
                PROTOCOL_VERSION_2_COMPATIBLE,
                0,
                0,
                0,
            ],
        };

        let kv = Arc::new(Self {
            cfg,
            node_name,
            compression_algo,
            codecs: RwLock::new(codec_map),
            node_meta,
            transport,
            advertise_ip,
            advertise_port,
            store: Mutex::new(HashMap::new()),
            watchers: Mutex::new(HashMap::new()),
            prefix_watchers: Mutex::new(HashMap::new()),
            nodes: RwLock::new(vec![local_node]),
            incarnation: AtomicU32::new(1),
            sequence_num: AtomicU32::new(0),
            ack_handlers: Mutex::new(HashMap::new()),
            local_broadcasts: Arc::new(TransmitQueue::new(retransmit_mult)),
            gossip_broadcasts: Arc::new(TransmitQueue::new(retransmit_mult)),
            gossip_notify: Arc::new(Notify::new()),
            running: AtomicBool::new(false),
            leaving: AtomicBool::new(false),
            shutdown_ctx: ServiceContext::background(),
            probe_index: AtomicUsize::new(0),
            metrics: MemberlistMetrics::new(reg, metrics_namespace),
        });
        kv.metrics.members_count.set(1.0);

        Ok((kv, packet_rx, stream_rx))
    }

    /// Starts the transport receiver tasks, gossip/push-pull/probe loops, and joins seed members.
    pub async fn start(
        cfg: MemberlistConfig,
        codecs: Vec<Arc<dyn Codec>>,
        metrics_namespace: &str,
        reg: Option<&Registry>,
    ) -> Result<Arc<Self>, KvError> {
        let (kv, packet_rx, stream_rx) =
            Self::bind(cfg, codecs, metrics_namespace, reg).await?;
        kv.spawn_tasks(packet_rx, stream_rx);
        kv.running.store(true, Ordering::Release);

        if !kv.cfg.join_members.0.is_empty() {
            let join_members = kv.cfg.join_members.0.clone();
            let res = kv
                .join_members_with_retries(
                    &join_members,
                    kv.cfg.max_join_retries.max(1) as usize,
                )
                .await;
            if let Err(err) = res {
                if kv.cfg.abort_if_cluster_join_fails {
                    kv.shutdown().await;
                    return Err(err);
                }
                warn!(%err, "failed to join memberlist cluster on startup");
            }
        }

        Ok(kv)
    }

    pub fn spawn_tasks(
        self: &Arc<Self>,
        mut packet_rx: mpsc::Receiver<ReceivedPacket>,
        mut stream_rx: mpsc::Receiver<TcpStream>,
    ) {
        // Packet receiver loop
        {
            let kv = self.clone();
            let cancel = self.shutdown_ctx.clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = cancel.canceled() => break,
                        pkt = packet_rx.recv() => {
                            let Some(pkt) = pkt else { break };
                            kv.ingest_packet(&pkt.buf, &pkt.from).await;
                        }
                    }
                }
            });
        }

        // Stream receiver loop
        {
            let kv = self.clone();
            let cancel = self.shutdown_ctx.clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = cancel.canceled() => break,
                        conn = stream_rx.recv() => {
                            let Some(conn) = conn else { break };
                            let kv_conn = kv.clone();
                            tokio::spawn(async move {
                                kv_conn.handle_stream_conn(conn).await;
                            });
                        }
                    }
                }
            });
        }

        // Gossip loop
        let gossip_interval: Duration = self.cfg.gossip_interval.as_std().unwrap_or(Duration::ZERO);
        if !gossip_interval.is_zero() && self.cfg.gossip_nodes > 0 {
            let kv = self.clone();
            let cancel = self.shutdown_ctx.clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = cancel.canceled() => break,
                        _ = kv.gossip_notify.notified() => {
                            kv.gossip().await;
                        }
                        _ = tokio::time::sleep(gossip_interval) => {
                            kv.gossip().await;
                        }
                    }
                }
            });
        }

        // Push/pull anti-entropy loop
        let push_pull_interval: Duration = self.cfg.pull_push_interval.as_std().unwrap_or(Duration::ZERO);
        if !push_pull_interval.is_zero() {
            let kv = self.clone();
            let cancel = self.shutdown_ctx.clone();
            tokio::spawn(async move {
                loop {
                    let n = kv.est_num_nodes();
                    let wait = push_pull_scale(push_pull_interval, n);
                    tokio::select! {
                        _ = cancel.canceled() => break,
                        _ = tokio::time::sleep(wait) => {
                            kv.push_pull().await;
                        }
                    }
                }
            });
        }

        // Probe loop
        {
            let kv = self.clone();
            let cancel = self.shutdown_ctx.clone();
            let probe_interval = Duration::from_secs(5);
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = cancel.canceled() => break,
                        _ = tokio::time::sleep(probe_interval) => {
                            kv.probe().await;
                        }
                    }
                }
            });
        }

        // Obsolete entries cleanup loop
        let obsolete_timeout: Duration = self.cfg.obsolete_entries_timeout.as_std().unwrap_or(Duration::ZERO);
        if !obsolete_timeout.is_zero() {
            let kv = self.clone();
            let cancel = self.shutdown_ctx.clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = cancel.canceled() => break,
                        _ = tokio::time::sleep(obsolete_timeout) => {
                            kv.cleanup_obsolete_entries();
                        }
                    }
                }
            });
        }
    }

    pub fn node_name(&self) -> &str {
        &self.node_name
    }

    pub fn node_meta(&self) -> Option<&[u8]> {
        self.node_meta.as_deref()
    }

    pub fn listening_port(&self) -> u16 {
        self.transport.auto_bind_port()
    }

    pub fn advertise_addr(&self) -> String {
        self.transport.get_advertised_addr()
    }

    pub fn get_codec(&self, codec_id: &str) -> Option<Arc<dyn Codec>> {
        self.codecs.read().get(codec_id).cloned()
    }

    pub fn register_codec(&self, codec: Arc<dyn Codec>) {
        self.codecs
            .write()
            .insert(codec.codec_id().to_string(), codec);
    }

    pub fn members(&self) -> Vec<NodeState> {
        self.nodes
            .read()
            .iter()
            .filter(|n| !n.dead_or_left())
            .cloned()
            .collect()
    }

    fn est_num_nodes(&self) -> usize {
        self.nodes
            .read()
            .iter()
            .filter(|n| !n.dead_or_left())
            .count()
            .max(1)
    }

    pub async fn join_members(&self, members: &[String]) -> Result<usize, KvError> {
        let resolved = self.resolve_members(members).await;
        if resolved.is_empty() {
            return Err(KvError::Other("found no nodes to join".to_string()));
        }
        let mut joined = 0usize;
        let mut last_err = None;
        for addr in resolved {
            match self.push_pull_node(&addr, true).await {
                Ok(()) => joined += 1,
                Err(e) => last_err = Some(e),
            }
        }
        if joined > 0 {
            Ok(joined)
        } else {
            Err(last_err.unwrap_or_else(|| KvError::Other("join failed".to_string())))
        }
    }

    async fn join_members_with_retries(
        &self,
        members: &[String],
        max_retries: usize,
    ) -> Result<usize, KvError> {
        let mut boff = Backoff::new(BackoffConfig {
            min_backoff: self.cfg.min_join_backoff.as_std().unwrap_or(Duration::ZERO),
            max_backoff: self.cfg.max_join_backoff.as_std().unwrap_or(Duration::ZERO),
            max_retries,
        });
        let mut last_err = None;
        while boff.ongoing() && !self.shutdown_ctx.is_canceled() {
            match self.join_members(members).await {
                Ok(n) if n > 0 => return Ok(n),
                Ok(_) => {}
                Err(e) => last_err = Some(e),
            }
            boff.wait().await;
        }
        Err(last_err.unwrap_or_else(|| KvError::Other("failed to join cluster".to_string())))
    }

    async fn resolve_members(&self, members: &[String]) -> Vec<String> {
        let default_port = self.cfg.bind_port.max(1) as u16;
        let mut out = Vec::new();
        for m in members {
            let trimmed = m.trim();
            if trimmed.is_empty() {
                continue;
            }
            // Strip optional nodeName/ prefix.
            let host_str = match trimmed.find('/') {
                Some(idx) => &trimmed[idx + 1..],
                None => trimmed,
            };
            let with_port = if host_str.contains(':') {
                host_str.to_string()
            } else {
                format!("{host_str}:{default_port}")
            };
            if let Ok(sa) = with_port.parse::<SocketAddr>() {
                out.push(sa.to_string());
            } else if let Ok(addrs) = tokio::net::lookup_host(&with_port).await {
                for sa in addrs {
                    out.push(sa.to_string());
                }
            }
        }
        out
    }

    pub async fn shutdown(&self) {
        self.leaving.store(true, Ordering::Release);
        self.running.store(false, Ordering::Release);

        // Wait briefly for locally generated broadcasts to flush.
        let timeout: Duration = self.cfg.broadcast_timeout_for_local_updates_on_shutdown.as_std().unwrap_or(Duration::ZERO);
        let deadline = tokio::time::Instant::now() + timeout;
        while self.local_broadcasts.num_queued() > 0 && self.est_num_nodes() > 1 {
            self.gossip().await;
            if !timeout.is_zero() && tokio::time::Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // Send Dead(from = self) to live peers so they mark us Left.
        let dead = Dead {
            incarnation: self.incarnation.load(Ordering::Acquire),
            node: self.node_name.clone(),
            from: self.node_name.clone(),
        };
        let dead_msg = encode_dead(&dead);
        let peers: Vec<NodeState> = self
            .nodes
            .read()
            .iter()
            .filter(|n| n.name != self.node_name && !n.dead_or_left())
            .cloned()
            .collect();
        for peer in peers {
            let _ = self.send_raw_packet(&peer.address(), Some(&peer), dead_msg.clone()).await;
        }

        self.shutdown_ctx.cancel();
        self.transport.shutdown();
    }

    // --- KV operations ---

    pub fn list_keys(&self, prefix: &str) -> Vec<String> {
        let store = self.store.lock();
        store
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect()
    }

    pub fn get_value(&self, key: &str) -> Option<(Box<dyn Any + Send + Sync>, usize)> {
        let mut desc = {
            let store = self.store.lock();
            store.get(key).cloned()?
        };
        desc.value.remove_tombstones(None);
        Some((desc.value.into_any(), desc.version))
    }

    pub fn get_mergeable(&self, key: &str) -> Option<(Box<dyn Mergeable>, usize)> {
        let mut desc = {
            let store = self.store.lock();
            store.get(key).cloned()?
        };
        desc.value.remove_tombstones(None);
        Some((desc.value, desc.version))
    }

    pub async fn delete_key(&self, key: &str) -> Result<(), KvError> {
        let existing = {
            let store = self.store.lock();
            store.get(key).cloned()
        };
        let Some(val) = existing else {
            return Ok(());
        };
        if val.deleted {
            return Ok(());
        }
        let codec = self
            .get_codec(&val.codec_id)
            .ok_or_else(|| KvError::Codec(format!("invalid codec: {}", val.codec_id)))?;
        let mut cloned = val.value.clone_mergeable();
        let (change, new_ver, deleted, updated) = self.merge_value_for_key(
            key,
            cloned.as_mut(),
            true,
            0,
            &val.codec_id,
            true,
            Some(SystemTime::now()),
        )?;
        if new_ver > 0 {
            self.notify_watchers(key);
            if let Some(ch) = change {
                self.broadcast_new_value(
                    key,
                    ch.as_ref(),
                    new_ver,
                    codec.as_ref(),
                    false,
                    deleted,
                    updated,
                );
            }
        }
        Ok(())
    }

    pub async fn cas_with_codec(
        &self,
        key: &str,
        codec: &dyn Codec,
        mut f: CasFn,
    ) -> Result<(), KvError> {
        let mut last_no_change = false;
        let mut last_err: Option<KvError> = None;

        for _ in 0..MAX_CAS_RETRIES {
            self.metrics.cas_attempts.inc();

            if last_no_change {
                tokio::time::sleep(NO_CHANGE_DETECTED_RETRY_SLEEP).await;
                last_no_change = false;
            }

            let (current_any, ver) = match self.get_value(key) {
                Some((v, ver)) => (Some(v), ver),
                None => (None, 0),
            };

            let outcome = match f(current_any.as_deref()) {
                Ok(o) => o,
                Err(cb_err) => {
                    last_err = Some(cb_err.err);
                    if !cb_err.retry {
                        break;
                    }
                    continue;
                }
            };

            let incoming_any = match outcome {
                CasOutcome::NoChange => return Ok(()),
                CasOutcome::Update(v) => v,
            };

            // Encode then decode via codec_mergeable to turn Box<dyn Any> into Box<dyn Mergeable>,
            // or downcast directly if it's already a Mergeable type we know? Wait: we can encode via
            // codec.encode(incoming_any) -> decode_mergeable! Even cleaner: try encoding & decoding,
            // OR let Codec encode and decode! Wait: encoding+decoding takes microseconds and works for ANY Codec!
            let encoded = codec.encode(incoming_any.as_ref())?;
            let mut incoming_mergeable = codec.decode_mergeable(&encoded)?;

            match self.merge_value_for_key(
                key,
                incoming_mergeable.as_mut(),
                false,
                ver,
                codec.codec_id(),
                false,
                Some(SystemTime::now()),
            ) {
                Ok((change, new_ver, deleted, updated)) => {
                    if new_ver == 0 {
                        last_no_change = true;
                        last_err = Some(KvError::Other("no change detected".to_string()));
                        continue;
                    }
                    self.metrics.cas_successes.inc();
                    self.notify_watchers(key);
                    if let Some(ch) = change {
                        self.broadcast_new_value(
                            key,
                            ch.as_ref(),
                            new_ver,
                            codec,
                            true,
                            deleted,
                            updated,
                        );
                    }
                    return Ok(());
                }
                Err(e) => {
                    last_err = Some(e);
                    continue;
                }
            }
        }

        self.metrics.cas_failures.inc();
        Err(last_err.unwrap_or_else(|| KvError::CasFailed {
            key: key.to_string(),
            reason: "failed to CAS".to_string(),
        }))
    }

    #[allow(clippy::too_many_arguments)]
    fn merge_value_for_key(
        &self,
        key: &str,
        incoming: &mut dyn Mergeable,
        _incoming_requires_clone: bool,
        cas_version: usize,
        codec_id: &str,
        deleted: bool,
        update_time: Option<SystemTime>,
    ) -> Result<MergeValueResult, KvError> {
        let mut store = self.store.lock();
        let curr_opt = store.get_mut(key);

        if curr_opt.is_none() && deleted {
            return Err(KvError::Other("cannot delete non-existent key".to_string()));
        }

        if let Some(curr) = curr_opt.as_ref() {
            if cas_version > 0 && curr.version != cas_version {
                return Err(KvError::CasFailed {
                    key: key.to_string(),
                    reason: "version mismatch".to_string(),
                });
            }
        }

        let (mut result, mut change) = if let Some(curr) = curr_opt {
            let mut cloned_curr = curr.value.clone_mergeable();
            let ch = cloned_curr
                .merge(incoming, cas_version > 0)
                .map_err(KvError::Other)?;
            (
                ValueDesc {
                    value: cloned_curr,
                    version: curr.version,
                    codec_id: curr.codec_id.clone(),
                    deleted: curr.deleted,
                    update_time: curr.update_time,
                },
                ch,
            )
        } else {
            let val = incoming.clone_mergeable();
            let ch = val.clone_mergeable();
            (
                ValueDesc {
                    value: val,
                    version: 0,
                    codec_id: codec_id.to_string(),
                    deleted: false,
                    update_time: None,
                },
                Some(ch),
            )
        };

        let mut new_updated = result.update_time;
        let mut new_deleted = result.deleted;
        if let Some(ut) = update_time {
            let is_newer = match new_updated {
                None => true,
                Some(prev) => ut > prev,
            };
            if is_newer && deleted {
                new_updated = Some(ut);
                new_deleted = deleted;
            }
        }

        let change_empty = change
            .as_ref()
            .map(|c| c.merge_content().is_empty())
            .unwrap_or(true);
        if change_empty && result.deleted == new_deleted {
            return Ok((None, 0, result.deleted, result.update_time));
        }

        let left_timeout: Duration = self.cfg.left_ingesters_timeout.as_std().unwrap_or(Duration::ZERO);
        if !left_timeout.is_zero() {
            let limit = SystemTime::now().checked_sub(left_timeout);
            result.value.remove_tombstones(limit);
            if let Some(ref mut ch) = change {
                ch.remove_tombstones(limit);
                if ch.merge_content().is_empty() {
                    return Ok((None, 0, result.deleted, result.update_time));
                }
            }
        }

        if change.is_none() && result.deleted != new_deleted {
            change = Some(result.value.clone_mergeable());
        }

        let new_version = result.version + 1;
        result.version = new_version;
        result.deleted = new_deleted;
        result.update_time = new_updated;
        result.codec_id = codec_id.to_string();

        store.insert(key.to_string(), result);

        Ok((change, new_version, new_deleted, new_updated))
    }

    fn merge_bytes_value_for_key(
        &self,
        key: &str,
        incoming_data: &[u8],
        codec: &dyn Codec,
        deleted: bool,
        update_time: Option<SystemTime>,
    ) -> Result<MergeValueResult, KvError> {
        let data = if incoming_data.is_empty() {
            EMPTY_SNAPPY_DATA
        } else {
            incoming_data
        };
        let mut decoded = codec.decode_mergeable(data)?;
        self.merge_value_for_key(
            key,
            decoded.as_mut(),
            false,
            0,
            codec.codec_id(),
            deleted,
            update_time,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn broadcast_new_value(
        &self,
        key: &str,
        change: &dyn Mergeable,
        version: usize,
        codec: &dyn Codec,
        locally_generated: bool,
        deleted: bool,
        update_time: Option<SystemTime>,
    ) {
        if locally_generated && !self.running.load(Ordering::Acquire) {
            // Allow broadcasting even during startup if transport is ready, unless leaving.
            if self.leaving.load(Ordering::Acquire) {
                warn!(key, "skipped broadcasting locally-generated update during shutdown");
                return;
            }
        }

        let data = match codec.encode_mergeable(change) {
            Ok(d) => d,
            Err(err) => {
                error!(key, version, %err, "failed to encode change");
                return;
            }
        };

        let update_time_millis = update_time
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        let kv_pair = KeyValuePair {
            key: key.to_string(),
            value: Bytes::from(data),
            codec: codec.codec_id().to_string(),
            deleted,
            update_time_millis,
        };
        let pair_data = kv_pair.encode_to_vec();
        let content: HashSet<String> = change.merge_content().into_iter().collect();

        let b = RingBroadcast {
            key: key.to_string(),
            content,
            version,
            msg: pair_data,
            transmits: 0,
        };

        if locally_generated {
            self.local_broadcasts.queue_broadcast(b);
        } else {
            self.gossip_broadcasts.queue_broadcast(b);
        }
        self.gossip_notify.notify_one();
    }

    fn notify_watchers(&self, key: &str) {
        {
            let watchers = self.watchers.lock();
            if let Some(list) = watchers.get(key) {
                for w in list {
                    w.notify.notify_one();
                }
            }
        }
        {
            let pwatchers = self.prefix_watchers.lock();
            for (prefix, list) in pwatchers.iter() {
                if key.starts_with(prefix) {
                    for pw in list {
                        let _ = pw.tx.try_send(key.to_string());
                    }
                }
            }
        }
    }

    fn cleanup_obsolete_entries(&self) {
        let timeout: Duration = self.cfg.obsolete_entries_timeout.as_std().unwrap_or(Duration::ZERO);
        if timeout.is_zero() {
            return;
        }
        let now = SystemTime::now();
        let mut store = self.store.lock();
        store.retain(|_, v| {
            if !v.deleted {
                return true;
            }
            match v.update_time {
                Some(ut) => now.duration_since(ut).unwrap_or(Duration::ZERO) <= timeout,
                None => true,
            }
        });
    }

    // --- Memberlist Delegate: LocalState, MergeRemoteState, NotifyMsg ---

    pub fn local_state(&self) -> Vec<u8> {
        self.metrics.number_of_pulls.inc();
        let store = self.store.lock();
        let mut buf = Vec::with_capacity(store.len() * 256);

        for (key, val) in store.iter() {
            let Some(codec) = self.get_codec(&val.codec_id) else {
                continue;
            };
            let Ok(encoded) = codec.encode_mergeable(val.value.as_ref()) else {
                continue;
            };
            let update_time_millis = val
                .update_time
                .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);

            let pair = KeyValuePair {
                key: key.clone(),
                value: Bytes::from(encoded),
                codec: val.codec_id.clone(),
                deleted: val.deleted,
                update_time_millis,
            };
            let ser = pair.encode_to_vec();
            buf.extend_from_slice(&(ser.len() as u32).to_be_bytes());
            buf.extend_from_slice(&ser);
        }

        buf
    }

    pub fn merge_remote_state(&self, mut data: &[u8]) {
        self.metrics.number_of_pushes.inc();
        while !data.is_empty() {
            if data.len() < LENGTH_PREFIX_SIZE {
                warn!("truncated KV pair length in push/pull state");
                break;
            }
            let pair_len = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
            data = &data[LENGTH_PREFIX_SIZE..];
            if data.len() < pair_len {
                warn!(
                    expected = pair_len,
                    remaining = data.len(),
                    "truncated KV pair in push/pull state"
                );
                break;
            }
            let pair_bytes = &data[..pair_len];
            data = &data[pair_len..];

            let Ok(kv_pair) = KeyValuePair::decode(pair_bytes) else {
                warn!("failed to decode KeyValuePair from push/pull state");
                continue;
            };
            let Some(codec) = self.get_codec(&kv_pair.codec) else {
                debug!(codec = %kv_pair.codec, key = %kv_pair.key, "unknown codec in remote state");
                continue;
            };
            let ut = if kv_pair.update_time_millis > 0 {
                Some(UNIX_EPOCH + Duration::from_millis(kv_pair.update_time_millis as u64))
            } else {
                None
            };
            match self.merge_bytes_value_for_key(
                &kv_pair.key,
                &kv_pair.value,
                codec.as_ref(),
                kv_pair.deleted,
                ut,
            ) {
                Ok((change, new_ver, deleted, updated)) => {
                    if new_ver > 0 {
                        self.notify_watchers(&kv_pair.key);
                        if let Some(ch) = change {
                            self.broadcast_new_value(
                                &kv_pair.key,
                                ch.as_ref(),
                                new_ver,
                                codec.as_ref(),
                                false,
                                deleted,
                                updated,
                            );
                        }
                    }
                }
                Err(err) => {
                    error!(key = %kv_pair.key, %err, "failed to merge remote KV pair");
                }
            }
        }
    }

    pub fn notify_msg(&self, msg: &[u8]) {
        self.metrics.received_messages.inc();
        let Ok(kv_pair) = KeyValuePair::decode(msg) else {
            self.metrics.invalid_messages.inc();
            return;
        };
        if kv_pair.key.is_empty() {
            self.metrics.invalid_messages.inc();
            return;
        }
        let Some(codec) = self.get_codec(&kv_pair.codec) else {
            self.metrics.invalid_messages.inc();
            return;
        };
        let ut = if kv_pair.update_time_millis > 0 {
            Some(UNIX_EPOCH + Duration::from_millis(kv_pair.update_time_millis as u64))
        } else {
            None
        };
        match self.merge_bytes_value_for_key(
            &kv_pair.key,
            &kv_pair.value,
            codec.as_ref(),
            kv_pair.deleted,
            ut,
        ) {
            Ok((change, new_ver, deleted, updated)) => {
                if new_ver > 0 {
                    self.notify_watchers(&kv_pair.key);
                    if let Some(ch) = change {
                        self.broadcast_new_value(
                            &kv_pair.key,
                            ch.as_ref(),
                            new_ver,
                            codec.as_ref(),
                            false,
                            deleted,
                            updated,
                        );
                    }
                }
            }
            Err(err) => {
                error!(key = %kv_pair.key, %err, "failed to process incoming userMsg");
            }
        }
    }

    fn get_broadcasts(&self, overhead: usize, limit: usize) -> Vec<Vec<u8>> {
        let num_nodes = self.est_num_nodes();
        let mut msgs = self
            .local_broadcasts
            .get_broadcasts(overhead, limit, num_nodes);
        let used: usize = msgs.iter().map(|m| overhead + m.len()).sum();
        if limit > used {
            msgs.extend(
                self.gossip_broadcasts
                    .get_broadcasts(overhead, limit - used, num_nodes),
            );
        }
        msgs
    }

    // --- Memberlist protocol handlers ---

    async fn ingest_packet(&self, raw: &[u8], from: &str) {
        let Ok((buf, label)) = remove_label_header(raw) else {
            return;
        };
        if !self.cfg.cluster_label_verification_disabled && label != self.cfg.cluster_label {
            return;
        }
        let Ok(buf) = unwrap_crc32(buf) else {
            return;
        };
        self.handle_command(buf, from).await;
    }

    fn handle_command<'a>(
        &'a self,
        buf: &'a [u8],
        from: &'a str,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            if buf.is_empty() {
                return;
            }
            let Ok(msg_type) = MessageType::try_from(buf[0]) else {
                return;
            };
            let body = &buf[1..];
            match msg_type {
                MessageType::Compound => {
                    if let Ok((_trunc, parts)) = decode_compound_message(body) {
                        for part in parts {
                            self.handle_command(part, from).await;
                        }
                    }
                }
                MessageType::Compress => {
                    if let Ok(cp) = super::wire::decode_compressed_payload(body) {
                        if let Ok(decomp) = decompress_buffer(cp.algo, &cp.buf) {
                            self.handle_command(&decomp, from).await;
                        }
                    }
                }
                MessageType::Ping => {
                    if let Ok(p) = decode_ping(body) {
                        if !p.node.is_empty() && p.node != self.node_name {
                            return;
                        }
                        let ack = encode_ack_resp(&AckResp {
                            seq_no: p.seq_no,
                            payload: None,
                        });
                        let reply_addr = if !p.source_addr.is_empty() && p.source_port > 0 {
                            format_ip_port(&p.source_addr, p.source_port)
                        } else {
                            from.to_string()
                        };
                        let _ = self.send_raw_packet(&reply_addr, None, ack).await;
                    }
                }
                MessageType::IndirectPing => {
                    if let Ok(ind) = decode_indirect_ping(body) {
                        let target_addr = format_ip_port(&ind.target, ind.port);
                        let local_seq = self.sequence_num.fetch_add(1, Ordering::Relaxed) + 1;
                        let ping = Ping {
                            seq_no: local_seq,
                            node: ind.node.clone(),
                            source_addr: ip_to_bytes(self.advertise_ip),
                            source_port: self.advertise_port,
                            source_node: self.node_name.clone(),
                        };
                        let (tx, rx) = oneshot::channel();
                        self.ack_handlers.lock().insert(local_seq, tx);
                        let _ = self
                            .send_raw_packet(&target_addr, None, encode_ping(&ping))
                            .await;

                        let reply_addr = if !ind.source_addr.is_empty() && ind.source_port > 0 {
                            format_ip_port(&ind.source_addr, ind.source_port)
                        } else {
                            from.to_string()
                        };
                        if tokio::time::timeout(Duration::from_secs(2), rx)
                            .await
                            .is_ok()
                        {
                            let ack = encode_ack_resp(&AckResp {
                                seq_no: ind.seq_no,
                                payload: None,
                            });
                            let _ = self.send_raw_packet(&reply_addr, None, ack).await;
                        } else {
                            self.ack_handlers.lock().remove(&local_seq);
                            if ind.nack {
                                let nack = encode_nack_resp(&NackResp { seq_no: ind.seq_no });
                                let _ = self.send_raw_packet(&reply_addr, None, nack).await;
                            }
                        }
                    }
                }
                MessageType::AckResp => {
                    if let Ok(ack) = decode_ack_resp(body) {
                        if let Some(tx) = self.ack_handlers.lock().remove(&ack.seq_no) {
                            let _ = tx.send(ack);
                        }
                    }
                }
                MessageType::NackResp => {
                    let _ = decode_nack_resp(body);
                }
                MessageType::Alive => {
                    if let Ok(alive) = decode_alive(body) {
                        self.handle_alive(&alive);
                    }
                }
                MessageType::Suspect => {
                    if let Ok(sus) = decode_suspect(body) {
                        self.handle_suspect(&sus);
                    }
                }
                MessageType::Dead => {
                    if let Ok(dead) = decode_dead(body) {
                        self.handle_dead(&dead);
                    }
                }
                MessageType::User => {
                    self.notify_msg(body);
                }
                _ => {}
            }
        })
    }

    async fn handle_stream_conn(&self, mut conn: TcpStream) {
        let timeout: Duration = self.cfg.stream_timeout.as_std().unwrap_or(Duration::ZERO);
        let fut = self.handle_stream_conn_inner(&mut conn);
        if !timeout.is_zero() {
            let _ = tokio::time::timeout(timeout, fut).await;
        } else {
            fut.await;
        }
    }

    async fn handle_stream_conn_inner(&self, conn: &mut TcpStream) {
        let (read_half, mut write_half) = conn.split();
        let mut reader = BufReader::new(read_half);

        let Ok((msg_type, stream_label, body_opt)) =
            read_stream_header(&mut reader).await
        else {
            return;
        };

        let effective_label = if self.cfg.cluster_label_verification_disabled {
            self.cfg.cluster_label.as_str()
        } else {
            stream_label.as_str()
        };
        if effective_label != self.cfg.cluster_label {
            return;
        }

        match msg_type {
            MessageType::PushPull => {
                let res = match body_opt {
                    Some(in_mem) => decode_push_pull(&in_mem)
                        .map(|(j, n, s)| (j, n, s.to_vec())),
                    None => read_push_pull_from_stream(&mut reader).await,
                };
                match res {
                    Ok((join, remote_nodes, user_state)) => {
                        let payload = self.build_push_pull_bytes(join);
                        if let Err(err) = self
                            .send_raw_stream(&mut write_half, payload, effective_label)
                            .await
                        {
                            warn!(%err, "failed to write push/pull local state");
                            return;
                        }
                        self.merge_remote_nodes(&remote_nodes);
                        if !user_state.is_empty() {
                            self.merge_remote_state(&user_state);
                        }
                    }
                    Err(err) => {
                        let err_msg = encode_err_resp(&ErrResp { error: err });
                        let _ = self
                            .send_raw_stream(&mut write_half, err_msg, effective_label)
                            .await;
                    }
                }
            }
            MessageType::User => {
                let res = match body_opt {
                    Some(in_mem) => super::wire::decode_user_msg_stream(&in_mem)
                        .map(|s| s.to_vec()),
                    None => read_user_msg_from_stream(&mut reader).await,
                };
                if let Ok(payload) = res {
                    self.notify_msg(&payload);
                }
            }
            MessageType::Ping => {
                let res = match body_opt {
                    Some(in_mem) => decode_ping(&in_mem),
                    None => read_msgpack_map_bytes(&mut reader)
                        .await
                        .and_then(|b| decode_ping(&b)),
                };
                if let Ok(p) = res {
                    if p.node.is_empty() || p.node == self.node_name {
                        let ack = encode_ack_resp(&AckResp {
                            seq_no: p.seq_no,
                            payload: None,
                        });
                        let _ = self
                            .send_raw_stream(&mut write_half, ack, effective_label)
                            .await;
                    }
                }
            }
            _ => {}
        }
    }

    fn build_push_pull_bytes(&self, join: bool) -> Vec<u8> {
        let local_nodes: Vec<PushNodeState> = self
            .nodes
            .read()
            .iter()
            .map(|n| PushNodeState {
                name: n.name.clone(),
                addr: n.addr.clone(),
                port: n.port,
                meta: n.meta.clone(),
                incarnation: n.incarnation,
                state: n.state,
                vsn: n.vsn.clone(),
            })
            .collect();
        let user_data = self.local_state();
        encode_push_pull(&local_nodes, &user_data, join)
    }

    async fn push_pull_node(&self, addr: &str, join: bool) -> Result<(), KvError> {
        let timeout: Duration = self.cfg.stream_timeout.as_std().unwrap_or(Duration::ZERO);
        let mut conn = self.transport.dial_stream(addr, timeout).await?;

        let payload = self.build_push_pull_bytes(join);
        let (mut read_half, mut write_half) = conn.split();
        self.send_raw_stream(&mut write_half, payload, &self.cfg.cluster_label)
            .await?;

        let mut reader = BufReader::new(&mut read_half);
        let read_fut = async {
            let (msg_type, _label, body_opt) =
                read_stream_header(&mut reader)
                    .await
                    .map_err(KvError::Other)?;
            match msg_type {
                MessageType::Err => {
                    let e = match body_opt {
                        Some(b) => super::wire::decode_err_resp(&b).map_err(KvError::Other)?,
                        None => read_msgpack_map_bytes(&mut reader)
                            .await
                            .and_then(|b| super::wire::decode_err_resp(&b))
                            .map_err(KvError::Other)?,
                    };
                    Err(KvError::Other(format!("remote error: {}", e.error)))
                }
                MessageType::PushPull => {
                    let (_j, remote_nodes, user_state) = match body_opt {
                        Some(b) => decode_push_pull(&b)
                            .map(|(j, n, s)| (j, n, s.to_vec()))
                            .map_err(KvError::Other)?,
                        None => read_push_pull_from_stream(&mut reader)
                            .await
                            .map_err(KvError::Other)?,
                    };
                    Ok((remote_nodes, user_state))
                }
                other => Err(KvError::Other(format!(
                    "unexpected msg_type {other:?} in push/pull response"
                ))),
            }
        };

        let (remote_nodes, user_state) = if !timeout.is_zero() {
            tokio::time::timeout(timeout, read_fut)
                .await
                .map_err(|_| KvError::Other("push/pull stream timeout".to_string()))??
        } else {
            read_fut.await?
        };

        self.merge_remote_nodes(&remote_nodes);
        if !user_state.is_empty() {
            self.merge_remote_state(&user_state);
        }
        Ok(())
    }

    async fn send_raw_stream<W: tokio::io::AsyncWrite + Unpin>(
        &self,
        writer: &mut W,
        mut send_buf: Vec<u8>,
        label: &str,
    ) -> Result<(), io::Error> {
        if self.cfg.compression_enabled {
            if let Ok(comp) = compress_payload(self.compression_algo, &send_buf) {
                if comp.len() < send_buf.len() {
                    send_buf = comp;
                }
            }
        }
        let framed = add_label_header(&send_buf, label)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        writer.write_all(&framed).await?;
        writer.flush().await?;
        Ok(())
    }

    async fn send_raw_packet(
        &self,
        addr: &str,
        node: Option<&NodeState>,
        mut msg: Vec<u8>,
    ) -> Result<(), KvError> {
        if self.cfg.compression_enabled {
            if let Ok(comp) = compress_payload(self.compression_algo, &msg) {
                if comp.len() < msg.len() {
                    msg = comp;
                }
            }
        }
        let pmax = node
            .and_then(|n| n.vsn.get(1).copied())
            .unwrap_or(PROTOCOL_VERSION_MAX);
        if pmax >= 5 {
            msg = wrap_crc32(&msg);
        }
        let framed = add_label_header(&msg, &self.cfg.cluster_label).map_err(KvError::Other)?;
        self.transport.write_to(framed, addr.to_string()).await;
        Ok(())
    }

    async fn gossip(&self) {
        let peers: Vec<NodeState> = {
            let nodes = self.nodes.read();
            let mut candidates: Vec<NodeState> = nodes
                .iter()
                .filter(|n| n.name != self.node_name && !n.dead_or_left())
                .cloned()
                .collect();
            let mut rng = rand::thread_rng();
            candidates.shuffle(&mut rng);
            candidates.truncate(self.cfg.gossip_nodes.max(1) as usize);
            candidates
        };

        if peers.is_empty() {
            return;
        }

        let bytes_avail = UDP_BUFFER_SIZE - COMPOUND_HEADER_OVERHEAD;
        for peer in peers {
            let msgs = self.get_broadcasts(COMPOUND_OVERHEAD, bytes_avail);
            if msgs.is_empty() {
                return;
            }
            // Wrap each KV broadcast in userMsg ([8][KeyValuePair bytes]).
            let user_msgs: Vec<Vec<u8>> = msgs
                .into_iter()
                .map(|m| {
                    let mut v = Vec::with_capacity(1 + m.len());
                    v.push(MessageType::User as u8);
                    v.extend_from_slice(&m);
                    v
                })
                .collect();

            if user_msgs.len() == 1 {
                let _ = self
                    .send_raw_packet(&peer.address(), Some(&peer), user_msgs[0].clone())
                    .await;
            } else {
                for compound in make_compound_messages(&user_msgs) {
                    let _ = self
                        .send_raw_packet(&peer.address(), Some(&peer), compound)
                        .await;
                }
            }
        }
    }

    async fn push_pull(&self) {
        let peer = {
            let nodes = self.nodes.read();
            let live: Vec<NodeState> = nodes
                .iter()
                .filter(|n| n.name != self.node_name && n.state == NodeStateType::Alive)
                .cloned()
                .collect();
            live.choose(&mut rand::thread_rng()).cloned()
        };
        if let Some(p) = peer {
            let _ = self.push_pull_node(&p.address(), false).await;
        }
    }

    async fn probe(&self) {
        let target = {
            let nodes = self.nodes.read();
            if nodes.is_empty() {
                return;
            }
            let len = nodes.len();
            let start = self.probe_index.fetch_add(1, Ordering::Relaxed) % len;
            let mut found = None;
            for offset in 0..len {
                let n = &nodes[(start + offset) % len];
                if n.name != self.node_name && !n.dead_or_left() {
                    found = Some(n.clone());
                    break;
                }
            }
            found
        };
        let Some(node) = target else {
            return;
        };
        let seq_no = self.sequence_num.fetch_add(1, Ordering::Relaxed) + 1;
        let ping = Ping {
            seq_no,
            node: node.name.clone(),
            source_addr: ip_to_bytes(self.advertise_ip),
            source_port: self.advertise_port,
            source_node: self.node_name.clone(),
        };
        let (tx, rx) = oneshot::channel();
        self.ack_handlers.lock().insert(seq_no, tx);
        let _ = self
            .send_raw_packet(&node.address(), Some(&node), encode_ping(&ping))
            .await;

        if tokio::time::timeout(Duration::from_secs(2), rx).await.is_err() {
            self.ack_handlers.lock().remove(&seq_no);
        }
    }

    fn handle_alive(&self, alive: &Alive) {
        if self.leaving.load(Ordering::Acquire) && alive.node == self.node_name {
            return;
        }
        let mut nodes = self.nodes.write();
        if let Some(existing) = nodes.iter_mut().find(|n| n.name == alive.node) {
            let is_local = existing.name == self.node_name;
            if (!is_local && alive.incarnation <= existing.incarnation)
                || (is_local && alive.incarnation < existing.incarnation)
            {
                return;
            }
            if is_local {
                let inc = self.incarnation.fetch_add(1, Ordering::AcqRel) + 1;
                existing.incarnation = inc.max(alive.incarnation + 1);
                return;
            }
            existing.incarnation = alive.incarnation;
            existing.addr = alive.addr.clone();
            existing.port = alive.port;
            existing.meta = alive.meta.clone();
            if !alive.vsn.is_empty() {
                existing.vsn = alive.vsn.clone();
            }
            if existing.state != NodeStateType::Alive {
                existing.state = NodeStateType::Alive;
                existing.state_change = SystemTime::now();
            }
        } else {
            nodes.push(NodeState {
                name: alive.node.clone(),
                addr: alive.addr.clone(),
                port: alive.port,
                meta: alive.meta.clone(),
                incarnation: alive.incarnation,
                state: NodeStateType::Alive,
                state_change: SystemTime::now(),
                vsn: alive.vsn.clone(),
            });
            self.metrics.members_count.set(
                nodes.iter().filter(|n| !n.dead_or_left()).count() as f64,
            );
        }
    }

    fn handle_suspect(&self, sus: &Suspect) {
        let mut nodes = self.nodes.write();
        let Some(existing) = nodes.iter_mut().find(|n| n.name == sus.node) else {
            return;
        };
        if sus.incarnation < existing.incarnation || existing.state != NodeStateType::Alive {
            return;
        }
        if existing.name == self.node_name {
            let new_inc = self.incarnation.fetch_add(1, Ordering::AcqRel) + 1;
            let inc = new_inc.max(sus.incarnation + 1);
            self.incarnation.store(inc, Ordering::Release);
            existing.incarnation = inc;
            let alive = Alive {
                incarnation: inc,
                node: existing.name.clone(),
                addr: existing.addr.clone(),
                port: existing.port,
                meta: existing.meta.clone(),
                vsn: existing.vsn.clone(),
            };
            let pkt = encode_alive(&alive);
            let peers: Vec<NodeState> = nodes
                .iter()
                .filter(|n| n.name != self.node_name && !n.dead_or_left())
                .cloned()
                .collect();
            drop(nodes);
            for p in peers {
                let tr = self.transport.clone();
                let addr = p.address();
                let bytes = pkt.clone();
                tokio::spawn(async move {
                    tr.write_to(bytes, addr).await;
                });
            }
            return;
        }
        existing.incarnation = sus.incarnation;
        existing.state = NodeStateType::Suspect;
        existing.state_change = SystemTime::now();
    }

    fn handle_dead(&self, dead: &Dead) {
        let mut nodes = self.nodes.write();
        let Some(existing) = nodes.iter_mut().find(|n| n.name == dead.node) else {
            return;
        };
        if dead.incarnation < existing.incarnation || existing.dead_or_left() {
            return;
        }
        if existing.name == self.node_name && !self.leaving.load(Ordering::Acquire) {
            let new_inc = self.incarnation.fetch_add(1, Ordering::AcqRel) + 1;
            existing.incarnation = new_inc.max(dead.incarnation + 1);
            return;
        }
        existing.incarnation = dead.incarnation;
        existing.state = if dead.node == dead.from {
            NodeStateType::Left
        } else {
            NodeStateType::Dead
        };
        existing.state_change = SystemTime::now();
        self.metrics.members_count.set(
            nodes.iter().filter(|n| !n.dead_or_left()).count() as f64,
        );
    }

    fn merge_remote_nodes(&self, remote_nodes: &[PushNodeState]) {
        for rn in remote_nodes {
            match rn.state {
                NodeStateType::Alive => {
                    self.handle_alive(&Alive {
                        incarnation: rn.incarnation,
                        node: rn.name.clone(),
                        addr: rn.addr.clone(),
                        port: rn.port,
                        meta: rn.meta.clone(),
                        vsn: rn.vsn.clone(),
                    });
                }
                NodeStateType::Left => {
                    self.handle_dead(&Dead {
                        incarnation: rn.incarnation,
                        node: rn.name.clone(),
                        from: rn.name.clone(),
                    });
                }
                NodeStateType::Dead | NodeStateType::Suspect => {
                    self.handle_suspect(&Suspect {
                        incarnation: rn.incarnation,
                        node: rn.name.clone(),
                        from: self.node_name.clone(),
                    });
                }
            }
        }
    }
}

fn push_pull_scale(interval: Duration, n: usize) -> Duration {
    if n <= PUSH_PULL_SCALE_THRESHOLD {
        return interval;
    }
    let mult = ((n as f64).log2() - (PUSH_PULL_SCALE_THRESHOLD as f64).log2()).ceil() + 1.0;
    interval.mul_f64(mult)
}

fn default_hostname() -> String {
    #[cfg(target_os = "linux")]
    if let Ok(s) = std::fs::read_to_string("/proc/sys/kernel/hostname") {
        let t = s.trim();
        if !t.is_empty() {
            return t.to_string();
        }
    }
    std::env::var("HOSTNAME").unwrap_or_else(|_| "localhost".to_string())
}

// --- Streaming MessagePack readers (never block waiting for EOF) ---

async fn read_stream_header<R: AsyncRead + Unpin + Send>(
    reader: &mut R,
) -> Result<(MessageType, String, Option<Vec<u8>>), String> {
    let mut b = [0u8; 1];
    reader
        .read_exact(&mut b)
        .await
        .map_err(|e| format!("read stream byte: {e}"))?;

    let mut label = String::new();
    if b[0] == MessageType::HasLabel as u8 {
        let mut sz = [0u8; 1];
        reader
            .read_exact(&mut sz)
            .await
            .map_err(|e| format!("read label size: {e}"))?;
        if sz[0] == 0 {
            return Err("empty label header".to_string());
        }
        let mut lbuf = vec![0u8; sz[0] as usize];
        reader
            .read_exact(&mut lbuf)
            .await
            .map_err(|e| format!("read label: {e}"))?;
        label = String::from_utf8(lbuf).map_err(|e| format!("label utf8: {e}"))?;
        reader
            .read_exact(&mut b)
            .await
            .map_err(|e| format!("read msg type after label: {e}"))?;
    }

    let msg_type = MessageType::try_from(b[0])
        .map_err(|v| format!("unsupported stream msgType {v}"))?;

    if msg_type == MessageType::Compress {
        let raw_cp = read_msgpack_map_bytes(reader).await?;
        let cp = super::wire::decode_compressed_payload(&raw_cp)?;
        let decomp = decompress_buffer(cp.algo, &cp.buf)?;
        if decomp.is_empty() {
            return Err("empty decompressed stream payload".to_string());
        }
        let inner_type = MessageType::try_from(decomp[0])
            .map_err(|v| format!("unsupported inner stream msgType {v}"))?;
        return Ok((inner_type, label, Some(decomp[1..].to_vec())));
    }

    Ok((msg_type, label, None))
}

async fn read_push_pull_from_stream<R: AsyncRead + Unpin + Send>(
    reader: &mut R,
) -> Result<(bool, Vec<PushNodeState>, Vec<u8>), String> {
    let hdr_bytes = read_msgpack_map_bytes(reader).await?;
    let hdr = parse_push_pull_header_only(&hdr_bytes)?;
    let mut nodes = Vec::with_capacity(hdr.nodes.min(4096));
    for _ in 0..hdr.nodes {
        let node_bytes = read_msgpack_map_bytes(reader).await?;
        let ns = parse_push_node_state_only(&node_bytes)?;
        nodes.push(ns);
    }
    if hdr.user_state_len > MAX_PUSH_STATE_BYTES {
        return Err(format!(
            "user state length {} exceeds limit",
            hdr.user_state_len
        ));
    }
    let mut user_buf = vec![0u8; hdr.user_state_len];
    if hdr.user_state_len > 0 {
        reader
            .read_exact(&mut user_buf)
            .await
            .map_err(|e| format!("read user state: {e}"))?;
    }
    Ok((hdr.join, nodes, user_buf))
}

async fn read_user_msg_from_stream<R: AsyncRead + Unpin + Send>(
    reader: &mut R,
) -> Result<Vec<u8>, String> {
    let hdr_bytes = read_msgpack_map_bytes(reader).await?;
    let len = parse_user_msg_len(&hdr_bytes)?;
    if len > MAX_PUSH_STATE_BYTES {
        return Err(format!("user msg len {len} exceeds limit"));
    }
    let mut out = vec![0u8; len];
    if len > 0 {
        reader
            .read_exact(&mut out)
            .await
            .map_err(|e| format!("read user msg payload: {e}"))?;
    }
    Ok(out)
}

fn parse_push_pull_header_only(bytes: &[u8]) -> Result<PushPullHeader, String> {
    // decode_push_pull with Nodes=0? No, if Nodes > 0 it expects node maps.
    // Let's write a tiny generic msgpack header helper in wire? Or reuse decode_push_pull by patching?
    // Even cleaner: let's add a helper right here using `read_mp_item_sync` or expose `decode_push_pull_header` in wire.rs!
    // Since we can call a small parser directly on `bytes`:
    let mut pos = 0usize;
    let map_len = read_sync_map_len(bytes, &mut pos)?;
    let mut hdr = PushPullHeader::default();
    for _ in 0..map_len {
        let key = read_sync_string(bytes, &mut pos)?;
        match key.as_str() {
            "Nodes" => hdr.nodes = read_sync_u64(bytes, &mut pos)? as usize,
            "UserStateLen" => hdr.user_state_len = read_sync_u64(bytes, &mut pos)? as usize,
            "Join" => hdr.join = read_sync_bool(bytes, &mut pos)?,
            _ => skip_sync_val(bytes, &mut pos)?,
        }
    }
    Ok(hdr)
}

fn parse_user_msg_len(bytes: &[u8]) -> Result<usize, String> {
    let mut pos = 0usize;
    let map_len = read_sync_map_len(bytes, &mut pos)?;
    let mut len = 0usize;
    for _ in 0..map_len {
        let key = read_sync_string(bytes, &mut pos)?;
        match key.as_str() {
            "UserMsgLen" => len = read_sync_u64(bytes, &mut pos)? as usize,
            _ => skip_sync_val(bytes, &mut pos)?,
        }
    }
    Ok(len)
}

fn parse_push_node_state_only(bytes: &[u8]) -> Result<PushNodeState, String> {
    let mut pos = 0usize;
    let map_len = read_sync_map_len(bytes, &mut pos)?;
    let mut ns = PushNodeState {
        name: String::new(),
        addr: Vec::new(),
        port: 0,
        meta: None,
        incarnation: 0,
        state: NodeStateType::Alive,
        vsn: Vec::new(),
    };
    for _ in 0..map_len {
        let key = read_sync_string(bytes, &mut pos)?;
        match key.as_str() {
            "Name" => ns.name = read_sync_string(bytes, &mut pos)?,
            "Addr" => ns.addr = read_sync_bytes(bytes, &mut pos)?,
            "Port" => ns.port = read_sync_u64(bytes, &mut pos)? as u16,
            "Meta" => ns.meta = read_sync_opt_bytes(bytes, &mut pos)?,
            "Incarnation" => ns.incarnation = read_sync_u64(bytes, &mut pos)? as u32,
            "State" => {
                let s = read_sync_u64(bytes, &mut pos)? as u8;
                ns.state = NodeStateType::try_from(s).map_err(|v| format!("invalid state {v}"))?;
            }
            "Vsn" => ns.vsn = read_sync_bytes(bytes, &mut pos)?,
            _ => skip_sync_val(bytes, &mut pos)?,
        }
    }
    Ok(ns)
}

fn read_sync_map_len(b: &[u8], pos: &mut usize) -> Result<usize, String> {
    let bd = *b.get(*pos).ok_or("eof")?;
    *pos += 1;
    match bd {
        0x80..=0x8f => Ok((bd & 0x0f) as usize),
        0xde => {
            let s = b.get(*pos..*pos + 2).ok_or("eof")?;
            *pos += 2;
            Ok(u16::from_be_bytes([s[0], s[1]]) as usize)
        }
        0xdf => {
            let s = b.get(*pos..*pos + 4).ok_or("eof")?;
            *pos += 4;
            Ok(u32::from_be_bytes([s[0], s[1], s[2], s[3]]) as usize)
        }
        _ => Err("expected map".to_string()),
    }
}

fn read_sync_u64(b: &[u8], pos: &mut usize) -> Result<u64, String> {
    let bd = *b.get(*pos).ok_or("eof")?;
    *pos += 1;
    match bd {
        0x00..=0x7f => Ok(bd as u64),
        0xcc => {
            let v = *b.get(*pos).ok_or("eof")?;
            *pos += 1;
            Ok(v as u64)
        }
        0xcd => {
            let s = b.get(*pos..*pos + 2).ok_or("eof")?;
            *pos += 2;
            Ok(u16::from_be_bytes([s[0], s[1]]) as u64)
        }
        0xce => {
            let s = b.get(*pos..*pos + 4).ok_or("eof")?;
            *pos += 4;
            Ok(u32::from_be_bytes([s[0], s[1], s[2], s[3]]) as u64)
        }
        0xcf => {
            let s = b.get(*pos..*pos + 8).ok_or("eof")?;
            *pos += 8;
            Ok(u64::from_be_bytes(s.try_into().expect("8")))
        }
        0xd0 => {
            let v = *b.get(*pos).ok_or("eof")? as i8;
            *pos += 1;
            u64::try_from(v).map_err(|_| "negative int".to_string())
        }
        0xd1 => {
            let s = b.get(*pos..*pos + 2).ok_or("eof")?;
            *pos += 2;
            let v = i16::from_be_bytes([s[0], s[1]]);
            u64::try_from(v).map_err(|_| "negative int".to_string())
        }
        0xd2 => {
            let s = b.get(*pos..*pos + 4).ok_or("eof")?;
            *pos += 4;
            let v = i32::from_be_bytes([s[0], s[1], s[2], s[3]]);
            u64::try_from(v).map_err(|_| "negative int".to_string())
        }
        0xd3 => {
            let s = b.get(*pos..*pos + 8).ok_or("eof")?;
            *pos += 8;
            let v = i64::from_be_bytes(s.try_into().expect("8"));
            u64::try_from(v).map_err(|_| "negative int".to_string())
        }
        other => Err(format!("expected uint, got 0x{other:02x}")),
    }
}

fn read_sync_bool(b: &[u8], pos: &mut usize) -> Result<bool, String> {
    let bd = *b.get(*pos).ok_or("eof")?;
    *pos += 1;
    match bd {
        0xc2 | 0x00 => Ok(false),
        0xc3 | 0x01 => Ok(true),
        _ => Err("expected bool".to_string()),
    }
}

fn read_sync_opt_bytes(b: &[u8], pos: &mut usize) -> Result<Option<Vec<u8>>, String> {
    let bd = *b.get(*pos).ok_or("eof")?;
    *pos += 1;
    let len = match bd {
        0xc0 => return Ok(None),
        0xa0..=0xbf => (bd & 0x1f) as usize,
        0xd9 | 0xc4 => {
            let l = *b.get(*pos).ok_or("eof")? as usize;
            *pos += 1;
            l
        }
        0xda | 0xc5 => {
            let s = b.get(*pos..*pos + 2).ok_or("eof")?;
            *pos += 2;
            u16::from_be_bytes([s[0], s[1]]) as usize
        }
        0xdb | 0xc6 => {
            let s = b.get(*pos..*pos + 4).ok_or("eof")?;
            *pos += 4;
            u32::from_be_bytes([s[0], s[1], s[2], s[3]]) as usize
        }
        _ => return Err("expected bytes".to_string()),
    };
    let s = b.get(*pos..*pos + len).ok_or("eof")?;
    *pos += len;
    Ok(Some(s.to_vec()))
}

fn read_sync_bytes(b: &[u8], pos: &mut usize) -> Result<Vec<u8>, String> {
    Ok(read_sync_opt_bytes(b, pos)?.unwrap_or_default())
}

fn read_sync_string(b: &[u8], pos: &mut usize) -> Result<String, String> {
    let bytes = read_sync_bytes(b, pos)?;
    String::from_utf8(bytes).map_err(|e| e.to_string())
}

fn skip_sync_val(b: &[u8], pos: &mut usize) -> Result<(), String> {
    let start = *pos;
    let bd = *b.get(start).ok_or("eof")?;
    *pos += 1;
    match bd {
        0x00..=0x7f | 0xc0..=0xc3 | 0xe0..=0xff => Ok(()),
        0xcc | 0xd0 => {
            *pos += 1;
            Ok(())
        }
        0xcd | 0xd1 => {
            *pos += 2;
            Ok(())
        }
        0xce | 0xd2 | 0xca => {
            *pos += 4;
            Ok(())
        }
        0xcf | 0xd3 | 0xcb => {
            *pos += 8;
            Ok(())
        }
        0xa0..=0xbf => {
            *pos += (bd & 0x1f) as usize;
            Ok(())
        }
        0xd9 | 0xc4 => {
            let n = *b.get(*pos).ok_or("eof")? as usize;
            *pos += 1 + n;
            Ok(())
        }
        0xda | 0xc5 => {
            let s = b.get(*pos..*pos + 2).ok_or("eof")?;
            *pos += 2 + u16::from_be_bytes([s[0], s[1]]) as usize;
            Ok(())
        }
        0xdb | 0xc6 => {
            let s = b.get(*pos..*pos + 4).ok_or("eof")?;
            *pos += 4 + u32::from_be_bytes([s[0], s[1], s[2], s[3]]) as usize;
            Ok(())
        }
        _ => Err("unsupported skip".to_string()),
    }
}

/// Reads a single complete self-delimiting MessagePack item from an async reader into a byte buffer.
async fn read_msgpack_map_bytes<R: AsyncRead + Unpin + Send>(reader: &mut R) -> Result<Vec<u8>, String> {
    let mut out = Vec::with_capacity(64);
    read_mp_item(reader, &mut out).await?;
    Ok(out)
}

fn read_mp_item<'a, R: AsyncRead + Unpin + Send>(
    reader: &'a mut R,
    out: &'a mut Vec<u8>,
) -> BoxFuture<'a, Result<(), String>> {
    Box::pin(async move {
        let mut bd_buf = [0u8; 1];
        reader
            .read_exact(&mut bd_buf)
            .await
            .map_err(|e| format!("msgpack read: {e}"))?;
        let bd = bd_buf[0];
        out.push(bd);

        match bd {
            0x00..=0x7f | 0xc0..=0xc3 | 0xe0..=0xff => Ok(()),
            0xcc | 0xd0 => read_append(reader, out, 1).await,
            0xcd | 0xd1 => read_append(reader, out, 2).await,
            0xce | 0xd2 | 0xca => read_append(reader, out, 4).await,
            0xcf | 0xd3 | 0xcb => read_append(reader, out, 8).await,
            0xa0..=0xbf => read_append(reader, out, (bd & 0x1f) as usize).await,
            0xd9 | 0xc4 => {
                let mut l = [0u8; 1];
                reader.read_exact(&mut l).await.map_err(|e| e.to_string())?;
                out.push(l[0]);
                read_append(reader, out, l[0] as usize).await
            }
            0xda | 0xc5 => {
                let mut l = [0u8; 2];
                reader.read_exact(&mut l).await.map_err(|e| e.to_string())?;
                out.extend_from_slice(&l);
                read_append(reader, out, u16::from_be_bytes(l) as usize).await
            }
            0xdb | 0xc6 => {
                let mut l = [0u8; 4];
                reader.read_exact(&mut l).await.map_err(|e| e.to_string())?;
                out.extend_from_slice(&l);
                let len = u32::from_be_bytes(l) as usize;
                if len > MAX_PUSH_STATE_BYTES {
                    return Err("msgpack bytes exceed max".to_string());
                }
                read_append(reader, out, len).await
            }
            0x90..=0x9f => {
                let n = (bd & 0x0f) as usize;
                for _ in 0..n {
                    read_mp_item(reader, out).await?;
                }
                Ok(())
            }
            0xdc => {
                let mut l = [0u8; 2];
                reader.read_exact(&mut l).await.map_err(|e| e.to_string())?;
                out.extend_from_slice(&l);
                let n = u16::from_be_bytes(l) as usize;
                for _ in 0..n {
                    read_mp_item(reader, out).await?;
                }
                Ok(())
            }
            0xdd => {
                let mut l = [0u8; 4];
                reader.read_exact(&mut l).await.map_err(|e| e.to_string())?;
                out.extend_from_slice(&l);
                let n = u32::from_be_bytes(l) as usize;
                for _ in 0..n {
                    read_mp_item(reader, out).await?;
                }
                Ok(())
            }
            0x80..=0x8f => {
                let n = (bd & 0x0f) as usize;
                for _ in 0..n {
                    read_mp_item(reader, out).await?;
                    read_mp_item(reader, out).await?;
                }
                Ok(())
            }
            0xde => {
                let mut l = [0u8; 2];
                reader.read_exact(&mut l).await.map_err(|e| e.to_string())?;
                out.extend_from_slice(&l);
                let n = u16::from_be_bytes(l) as usize;
                for _ in 0..n {
                    read_mp_item(reader, out).await?;
                    read_mp_item(reader, out).await?;
                }
                Ok(())
            }
            0xdf => {
                let mut l = [0u8; 4];
                reader.read_exact(&mut l).await.map_err(|e| e.to_string())?;
                out.extend_from_slice(&l);
                let n = u32::from_be_bytes(l) as usize;
                for _ in 0..n {
                    read_mp_item(reader, out).await?;
                    read_mp_item(reader, out).await?;
                }
                Ok(())
            }
            0xd4 => read_append(reader, out, 2).await,
            0xd5 => read_append(reader, out, 3).await,
            0xd6 => read_append(reader, out, 5).await,
            0xd7 => read_append(reader, out, 9).await,
            0xd8 => read_append(reader, out, 17).await,
            0xc7 => {
                let mut l = [0u8; 1];
                reader.read_exact(&mut l).await.map_err(|e| e.to_string())?;
                out.push(l[0]);
                read_append(reader, out, 1 + l[0] as usize).await
            }
            0xc8 => {
                let mut l = [0u8; 2];
                reader.read_exact(&mut l).await.map_err(|e| e.to_string())?;
                out.extend_from_slice(&l);
                read_append(reader, out, 1 + u16::from_be_bytes(l) as usize).await
            }
            0xc9 => {
                let mut l = [0u8; 4];
                reader.read_exact(&mut l).await.map_err(|e| e.to_string())?;
                out.extend_from_slice(&l);
                read_append(reader, out, 1 + u32::from_be_bytes(l) as usize).await
            }
        }
    })
}

async fn read_append<R: AsyncRead + Unpin + Send>(
    reader: &mut R,
    out: &mut Vec<u8>,
    len: usize,
) -> Result<(), String> {
    let start = out.len();
    out.resize(start + len, 0);
    reader
        .read_exact(&mut out[start..])
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

/// [`KvClient`] implementation backed by [`MemberlistKv`].
pub struct MemberlistKvClient {
    kv: Arc<MemberlistKv>,
    codec: Arc<dyn Codec>,
}

impl MemberlistKvClient {
    pub fn new(kv: Arc<MemberlistKv>, codec: Arc<dyn Codec>) -> Result<Self, KvError> {
        if kv.get_codec(codec.codec_id()).is_none() {
            return Err(KvError::InvalidConfig(format!(
                "codec not registered in KV: {}",
                codec.codec_id()
            )));
        }
        Ok(Self { kv, codec })
    }
}

impl KvClient for MemberlistKvClient {
    fn list<'a>(&'a self, prefix: &'a str) -> BoxFuture<'a, Result<Vec<String>, KvError>> {
        Box::pin(async move { Ok(self.kv.list_keys(prefix)) })
    }

    fn get<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<Box<dyn Any + Send + Sync>>, KvError>> {
        Box::pin(async move { Ok(self.kv.get_value(key).map(|(v, _)| v)) })
    }

    fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), KvError>> {
        Box::pin(async move { self.kv.delete_key(key).await })
    }

    fn cas<'a>(&'a self, key: &'a str, f: CasFn) -> BoxFuture<'a, Result<(), KvError>> {
        Box::pin(async move { self.kv.cas_with_codec(key, self.codec.as_ref(), f).await })
    }

    fn watch_key<'a>(
        &'a self,
        key: &'a str,
        cancel: ServiceContext,
        mut f: Box<dyn FnMut(Box<dyn Any + Send + Sync>) -> bool + Send>,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let notify = Arc::new(Notify::new());
            {
                let mut watchers = self.kv.watchers.lock();
                watchers
                    .entry(key.to_string())
                    .or_default()
                    .push(WatcherEntry {
                        notify: notify.clone(),
                    });
            }
            let _guard = KeyWatcherGuard {
                kv: Arc::clone(&self.kv),
                key: key.to_string(),
                notify: Arc::clone(&notify),
            };

            if let Some((val, _)) = self.kv.get_value(key) {
                if !f(val) {
                    return;
                }
            }

            loop {
                tokio::select! {
                    _ = cancel.canceled() => break,
                    _ = self.kv.shutdown_ctx.canceled() => break,
                    _ = notify.notified() => {
                        if let Some((val, _)) = self.kv.get_value(key) {
                            if !f(val) {
                                break;
                            }
                        }
                    }
                }
            }
        })
    }

    fn watch_prefix<'a>(
        &'a self,
        prefix: &'a str,
        cancel: ServiceContext,
        mut f: Box<dyn FnMut(&str, Box<dyn Any + Send + Sync>) -> bool + Send>,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let buf_size = self.kv.cfg.watch_prefix_buffer_size.max(1) as usize;
            let (tx, mut rx) = mpsc::channel(buf_size);
            let watcher_id = NEXT_PREFIX_WATCHER_ID.fetch_add(1, Ordering::Relaxed);
            {
                let mut pw = self.kv.prefix_watchers.lock();
                pw.entry(prefix.to_string())
                    .or_default()
                    .push(PrefixWatcherEntry { id: watcher_id, tx });
            }
            let _guard = PrefixWatcherGuard {
                kv: Arc::clone(&self.kv),
                prefix: prefix.to_string(),
                id: watcher_id,
            };

            loop {
                tokio::select! {
                    _ = cancel.canceled() => break,
                    _ = self.kv.shutdown_ctx.canceled() => break,
                    maybe_key = rx.recv() => {
                        let Some(key) = maybe_key else { break };
                        if let Some((val, _)) = self.kv.get_value(&key) {
                            if !f(&key, val) {
                                break;
                            }
                        }
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv::StringCodec;

    #[tokio::test]
    async fn test_dropped_watchers_are_unregistered() {
        let cfg = MemberlistConfig {
            bind_addr: vec!["127.0.0.1".to_string()],
            bind_port: 0,
            node_name: "watcher-test-node".to_string(),
            ..Default::default()
        };

        let codec: Arc<dyn Codec> = Arc::new(StringCodec);
        let kv = MemberlistKv::start(cfg, vec![Arc::clone(&codec)], "test", None)
            .await
            .unwrap();
        let client = Arc::new(MemberlistKvClient::new(Arc::clone(&kv), codec).unwrap());

        let ctx1 = ServiceContext::background();
        let ctx1_c = ctx1.clone();
        let c1 = Arc::clone(&client);
        let h1 = tokio::spawn(async move {
            c1.watch_key("k1", ctx1_c, Box::new(|_| true)).await;
        });

        let ctx2 = ServiceContext::background();
        let ctx2_c = ctx2.clone();
        let c2 = Arc::clone(&client);
        let h2 = tokio::spawn(async move {
            c2.watch_prefix("pref/", ctx2_c, Box::new(|_, _| true)).await;
        });

        // Wait for both watchers to register
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let wk = kv.watchers.lock().get("k1").map_or(0, Vec::len);
                let pw = kv.prefix_watchers.lock().get("pref/").map_or(0, Vec::len);
                if wk == 1 && pw == 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();

        // Cancel both watchers and verify both maps clean up completely
        ctx1.cancel();
        ctx2.cancel();
        h1.await.unwrap();
        h2.await.unwrap();

        assert!(kv.watchers.lock().is_empty());
        assert!(kv.prefix_watchers.lock().is_empty());

        kv.shutdown().await;
    }
}
