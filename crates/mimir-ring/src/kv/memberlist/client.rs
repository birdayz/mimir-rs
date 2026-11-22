//! Memberlist gossip-based KV store and client.
//!
//! Ported from:
//! - github.com/grafana/dskit/kv/memberlist/memberlist_client.go (KV service: starting/running/
//!   stopping, fast-join, join with retries in batches, DNS discovery, periodic rejoin, CAS,
//!   watchers with optional delayed notifications, per-key update workers, local-state cache,
//!   message history, obsolete entries cleanup)
//! - github.com/grafana/dskit/kv/memberlist/broadcast.go (`ringBroadcast`)
//! - github.com/hashicorp/memberlist (grafana fork) memberlist.go / net.go (`Join`, `resolveAddr`,
//!   packet/stream handling, handoff queues, compression metrics)
//!
//! The SWIM membership state machine lives in `state.rs`.

use std::any::Any;
use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use mimir_common::backoff::{Backoff, BackoffConfig};
use mimir_common::services::{
    basic_service, ManagedBasicService, Service, ServiceContext, ServiceError, State,
};
use mimir_config::server::MemberlistConfig;
use mimir_proto::memberlistpb::KeyValuePair;
use mimir_proto::prost::Message as _;
use parking_lot::{Mutex, RwLock};
use prometheus::{core::Collector, Registry};
use rand::seq::SliceRandom;
use rand::Rng;
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, Notify};
use tracing::{debug, error, info, warn};

use super::gometrics::GoMetrics;
use super::mergeable::Mergeable;
use super::metrics::KvMetrics;
use super::node_meta::{encode_node_metadata, NodeRole};
use super::queue::{Broadcast, TransmitLimitedQueue};
use super::stream::{
    default_hostname, read_msgpack_map_bytes, read_push_pull_from_stream,
    read_stream_with_first_byte, read_user_msg_from_stream, remove_label_header_from_stream,
    StreamReadError,
    StreamDecryptFn, StreamFirstByte,
};
use super::suspicion::Awareness;
use super::tcp_transport::{MemberlistConn, ReceivedPacket, TcpTransport};
use crate::kv::KeyDeleted;
use super::wire::{
    add_label_header, compress_payload, decode_ack_resp, decode_alive, decode_compound_message,
    decode_dead, decode_indirect_ping, decode_nack_resp, decode_ping, decode_push_pull,
    decode_suspect, decompress_buffer, encode_ack_resp, encode_err_resp, encode_push_pull,
    make_compound_messages, remove_label_header, unwrap_crc32, wrap_crc32, AckResp, Alive,
    CompressionType, ErrResp, MessageType, NodeStateType, PushNodeState, COMPOUND_HEADER_OVERHEAD,
    COMPOUND_OVERHEAD, LABEL_MAX_SIZE, PROTOCOL_VERSION_2_COMPATIBLE, PROTOCOL_VERSION_MAX,
    PROTOCOL_VERSION_MIN,
};
use super::zone_aware::ZoneAwareNodeSelection;
use crate::dns::{DnsProvider, Provider, GOLANG_RESOLVER_TYPE};
use crate::kv::{BoxFuture, CasFn, CasOutcome, Codec, KvClient, KvError};

#[path = "state.rs"]
mod state;
use state::{AckHandler, NodeTable, Timing};

/// dskit `maxCasRetries`.
const MAX_CAS_RETRIES: usize = 10;
/// dskit `noChangeDetectedRetrySleep`.
const NO_CHANGE_DETECTED_RETRY_SLEEP: Duration = Duration::from_secs(1);
const LENGTH_PREFIX_SIZE: usize = 4;
/// dskit sets `mlCfg.UDPBufferSize = 10 * 1024 * 1024`.
pub(crate) const UDP_BUFFER_SIZE: usize = 10 * 1024 * 1024;
/// memberlist `DefaultLANConfig().BindPort`: dskit never overrides the memberlist config's
/// BindPort (the TCP transport binds its own port), so default ports in `Join` addresses and
/// in alive/indirect-ping messages with port 0 are always 7946, exactly like Go.
const MEMBERLIST_DEFAULT_BIND_PORT: u16 = 7946;
/// memberlist `maxPushPullRequests`.
const MAX_PUSH_PULL_REQUESTS: usize = 128;
/// Default watch-prefix buffer (dskit `watchPrefixBufferSize`).
const ERR_FAILED_TO_JOIN_CLUSTER: &str = "failed to join memberlist cluster on startup";

/// Snappy block encoding of empty byte slice `[]`.
const EMPTY_SNAPPY_DATA: &[u8] = &[0x00];

type MergeValueResult = (Option<Box<dyn Mergeable>>, usize, bool, Option<SystemTime>);

/// dskit `ringBroadcast`.
pub struct RingBroadcast {
    pub key: String,
    pub content: Vec<String>,
    pub version: usize,
    pub msg: Vec<u8>,
    pub(crate) finished: Box<dyn Fn() + Send + Sync>,
}

impl RingBroadcast {
    pub fn new(key: String, content: Vec<String>, version: usize, msg: Vec<u8>) -> Self {
        Self {
            key,
            content,
            version,
            msg,
            finished: Box::new(|| {}),
        }
    }
}

impl Broadcast for RingBroadcast {
    fn invalidates(&self, old: &dyn Broadcast) -> bool {
        let Some(oldb) = old.as_any().downcast_ref::<RingBroadcast>() else {
            return false;
        };
        if self.key != oldb.key {
            return false;
        }
        // If the old broadcast contains something the new one doesn't, it's not invalidated.
        if !oldb.content.iter().all(|o| self.content.contains(o)) {
            return false;
        }
        self.version >= oldb.version
    }
    fn message(&self) -> &[u8] {
        &self.msg
    }
    fn finished(&self) {
        (self.finished)();
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Go `memberlist.NodeState` (with `Node` fields flattened; `vsn` = [PMin, PMax, PCur, DMin, DMax, DCur]).
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

pub(crate) fn format_ip_port(addr: &[u8], port: u16) -> String {
    // Go `net.IP.String()` prints IPv4-mapped IPv6 addresses (what `net.ParseIP` returns for an
    // IPv4 address, so what Go peers put on the wire) in dotted IPv4 form.
    let ip: IpAddr = if addr.len() == 4 {
        let a: [u8; 4] = addr.try_into().expect("4 bytes");
        IpAddr::from(a)
    } else if addr.len() == 16 {
        let a: [u8; 16] = addr.try_into().expect("16 bytes");
        IpAddr::from(a).to_canonical()
    } else {
        return format!("127.0.0.1:{port}");
    };
    SocketAddr::new(ip, port).to_string()
}

pub(crate) fn ip_to_bytes(ip: IpAddr) -> Vec<u8> {
    match ip {
        IpAddr::V4(v4) => v4.octets().to_vec(),
        IpAddr::V6(v6) => v6.octets().to_vec(),
    }
}

/// dskit `ValueDesc`: a stored value with its codec and local version.
pub struct ValueDesc {
    pub value: Box<dyn Mergeable>,
    /// Local only: used to keep track of what we're gossiping about.
    pub version: usize,
    /// ID of codec used to write this value.
    pub codec_id: String,
    /// Deleted is used to mark the value as deleted.
    pub deleted: bool,
    /// UpdateTime keeps track of the last time the value was updated (`None` = Go zero time).
    pub update_time: Option<SystemTime>,
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

/// dskit `Message`: entry of the sent/received message history (`/memberlist` status page).
#[derive(Clone, Debug)]
pub struct HistoryMessage {
    pub id: u64,
    pub time: SystemTime,
    pub size: usize,
    pub pair: KeyValuePair,
    pub version: usize,
    pub changes: Vec<String>,
}

#[derive(Default)]
struct MessageHistory {
    counter: u64,
    sent: VecDeque<HistoryMessage>,
    sent_size: usize,
    received: VecDeque<HistoryMessage>,
    received_size: usize,
}

/// dskit `addMessageToBuffer`.
fn add_message_to_buffer(
    msgs: &mut VecDeque<HistoryMessage>,
    size: &mut usize,
    limit: usize,
    msg: HistoryMessage,
) {
    *size += msg.size;
    msgs.push_back(msg);
    while !msgs.is_empty() && *size > limit {
        if let Some(m) = msgs.pop_front() {
            *size -= m.size;
        }
    }
}

/// dskit `valueUpdate`.
struct ValueUpdate {
    value: Bytes,
    codec: Arc<dyn Codec>,
    message_size: usize,
    deleted: bool,
    update_time: Option<SystemTime>,
}

/// memberlist `msgHandoff`.
struct Handoff {
    msg_type: MessageType,
    buf: Vec<u8>,
    from: String,
}

/// Options that dskit exposes only to tests / embedders (unexported `KVConfig` fields and the
/// injected `DNSProvider`).
#[derive(Clone)]
pub struct KvOptions {
    /// dskit `NewKV(..., dnsProvider, ...)`. `None` = Mimir's `dns.NewProvider(dns.GolangResolverType,
    /// 0, logger, cortex_-prefixed registerer with component="memberlist")`.
    pub dns_provider: Option<Arc<dyn DnsProvider>>,
    /// dskit `cfg.probeInterval` (tests only): probe interval, probe timeout = interval / 2.
    pub probe_interval: Option<Duration>,
    /// dskit `cfg.discoverMembersBackoff`.
    pub discover_members_backoff: BackoffConfig,
    /// Optional memberlist symmetric stream decryptor (when `MessageType::Encrypt` is used).
    pub stream_decryptor: Option<StreamDecryptFn>,
    /// Optional override for `maxCasRetries` (default 10).
    pub max_cas_retries: Option<usize>,
    /// Optional hook run at the start of `join_members_on_startup` (tests only).
    pub before_join_members_on_startup_hook: Option<Arc<dyn Fn(ServiceContext) -> BoxFuture<'static, ()> + Send + Sync>>,
}

impl Default for KvOptions {
    fn default() -> Self {
        Self {
            dns_provider: None,
            probe_interval: None,
            discover_members_backoff: BackoffConfig {
                min_backoff: Duration::from_millis(100),
                max_backoff: Duration::from_secs(5),
                max_retries: 10,
            },
            stream_decryptor: None,
            max_cas_retries: None,
            before_join_members_on_startup_hook: None,
        }
    }
}

/// Memberlist gossip-based KV store matching `github.com/grafana/dskit/kv/memberlist.KV`.
pub struct MemberlistKv {
    me: Weak<MemberlistKv>,
    cfg: MemberlistConfig,
    metrics_namespace: String,
    registry: Option<Registry>,
    node_name: String,
    compression_algo: CompressionType,
    codecs: RwLock<HashMap<String, Arc<dyn Codec>>>,
    node_meta: Option<Vec<u8>>,
    dns: Arc<dyn DnsProvider>,
    discover_backoff: BackoffConfig,
    timing: Timing,

    transport: OnceLock<Arc<TcpTransport>>,
    advertise: RwLock<(IpAddr, u16)>,

    // dskit KV state.
    store: Mutex<HashMap<String, ValueDesc>>,
    local_state_cache: Mutex<HashMap<String, (usize, Vec<u8>)>>,
    last_local_state_size: AtomicUsize,
    watchers: Mutex<HashMap<String, Vec<WatcherEntry>>>,
    prefix_watchers: Mutex<HashMap<String, Vec<PrefixWatcherEntry>>>,
    key_notifications: Mutex<HashSet<String>>,
    workers: Mutex<HashMap<String, mpsc::Sender<ValueUpdate>>>,
    history: Mutex<MessageHistory>,
    pub(crate) local_broadcasts: Arc<TransmitLimitedQueue>,
    pub(crate) gossip_broadcasts: Arc<TransmitLimitedQueue>,
    delegate_ready: AtomicBool,

    // memberlist state.
    nodes: RwLock<NodeTable>,
    num_nodes: AtomicU32,
    incarnation: AtomicU32,
    sequence_num: AtomicU32,
    ack_handlers: Mutex<HashMap<u32, AckHandler>>,
    awareness: Awareness,
    ml_broadcasts: TransmitLimitedQueue,
    probe_index: Mutex<usize>,
    leave: AtomicBool,
    leave_broadcast_tx: mpsc::Sender<()>,
    leave_broadcast_rx: tokio::sync::Mutex<mpsc::Receiver<()>>,
    handoff: Mutex<(VecDeque<Handoff>, VecDeque<Handoff>)>,
    handoff_notify: Notify,
    push_pull_req: AtomicUsize,
    zone_select: Option<ZoneAwareNodeSelection>,

    shutdown_ctx: ServiceContext,
    service: OnceLock<Arc<ManagedBasicService>>,
    metrics: KvMetrics,
    go_metrics: Arc<GoMetrics>,
    stream_decryptor: Option<StreamDecryptFn>,
    max_cas_retries: usize,
    before_join_hook: Option<Arc<dyn Fn(ServiceContext) -> BoxFuture<'static, ()> + Send + Sync>>,
    propagation_tracker: Mutex<Option<Arc<super::propagation_tracker::PropagationDelayTracker>>>,
}

/// Process-wide go-metrics sink (Go `metrics.NewGlobal` replaces the global sink).
fn global_go_metrics() -> Arc<GoMetrics> {
    static GLOBAL: OnceLock<Arc<GoMetrics>> = OnceLock::new();
    Arc::clone(GLOBAL.get_or_init(|| Arc::new(GoMetrics::new())))
}

impl MemberlistKv {
    /// dskit `NewKV`: creates the KV service (not started). Metrics are registered immediately.
    pub fn new(
        cfg: MemberlistConfig,
        codecs: Vec<Arc<dyn Codec>>,
        metrics_namespace: &str,
        reg: Option<&Registry>,
        opts: KvOptions,
    ) -> Result<Arc<Self>, KvError> {
        if cfg.watch_prefix_buffer_size <= 0 {
            return Err(KvError::InvalidConfig(
                "invalid WatchPrefixBufferSize: must be greater than 0".to_string(),
            ));
        }
        cfg.propagation_delay_tracker
            .validate()
            .map_err(|e| KvError::InvalidConfig(e.to_string()))?;

        let mut node_name = if cfg.node_name.is_empty() {
            default_hostname()
        } else {
            cfg.node_name.clone()
        };
        if cfg.randomize_node_name {
            node_name = format!("{node_name}-{}", generate_random_suffix());
        }

        let compression_algo = match cfg.compression_algorithm.as_str() {
            "snappy" => CompressionType::Snappy,
            _ => CompressionType::Lzw,
        };

        let (node_meta, zone_select, push_pull_nodes) = if cfg.zone_aware_routing.enabled {
            let role = NodeRole::parse(&cfg.zone_aware_routing.role).map_err(KvError::InvalidConfig)?;
            let meta = encode_node_metadata(role, &cfg.zone_aware_routing.instance_availability_zone)
                .map_err(|e| KvError::InvalidConfig(format!("failed to encode node metadata: {e}")))?;
            let sel = ZoneAwareNodeSelection::new(
                role,
                &cfg.zone_aware_routing.instance_availability_zone,
                reg,
            );
            // The bridge push/pulls with 2 nodes per interval (first a bridge, then a random node).
            let ppn = if role == NodeRole::Bridge { 2 } else { 1 };
            (Some(meta), Some(sel), ppn)
        } else {
            (None, None, 1)
        };

        let dns: Arc<dyn DnsProvider> = match opts.dns_provider {
            Some(p) => p,
            None => {
                let mut labels = HashMap::new();
                labels.insert("component".to_string(), "memberlist".to_string());
                Arc::new(
                    Provider::new(&GOLANG_RESOLVER_TYPE, 0, reg, "cortex_", labels)
                        .map_err(|e| KvError::Other(e.to_string()))?,
                )
            }
        };

        let d = |g: mimir_config::GoDuration| g.as_std().unwrap_or(Duration::ZERO);
        let (probe_interval, probe_timeout) = match opts.probe_interval {
            Some(p) if !p.is_zero() => (p, p / 2),
            _ => (Duration::from_secs(5), Duration::from_secs(2)),
        };
        let timing = Timing {
            probe_interval,
            probe_timeout,
            indirect_checks: 3,
            suspicion_mult: 4,
            suspicion_max_timeout_mult: 6,
            awareness_max_multiplier: 8,
            gossip_interval: d(cfg.gossip_interval),
            gossip_nodes: cfg.gossip_nodes.max(0) as usize,
            gossip_to_the_dead_time: d(cfg.gossip_to_dead_nodes_time),
            dead_node_reclaim_time: d(cfg.dead_node_reclaim_time),
            push_pull_interval: d(cfg.pull_push_interval),
            push_pull_nodes,
            retransmit_mult: cfg.retransmit_factor.max(0) as usize,
            tcp_timeout: d(cfg.stream_timeout),
            handoff_queue_depth: cfg.received_messages_queue_size.max(0) as usize,
            queue_check_interval: Duration::from_secs(30),
        };

        let go_metrics = global_go_metrics();
        let stream_decryptor = opts.stream_decryptor;
        let max_cas_retries = opts.max_cas_retries.unwrap_or(MAX_CAS_RETRIES);
        let before_join_hook = opts.before_join_members_on_startup_hook;
        let (leave_tx, leave_rx) = mpsc::channel(1);
        let retransmit_mult = timing.retransmit_mult;
        let awareness_max = timing.awareness_max_multiplier;

        let kv = Arc::new_cyclic(|me: &Weak<MemberlistKv>| {
            let members_fn = |w: Weak<MemberlistKv>| -> Box<dyn Fn() -> usize + Send + Sync> {
                Box::new(move || w.upgrade().map(|k| k.num_members()).unwrap_or(0))
            };
            let est_fn = {
                let w = me.clone();
                Box::new(move || w.upgrade().map(|k| k.est_num_nodes()).unwrap_or(0))
                    as Box<dyn Fn() -> usize + Send + Sync>
            };
            Self {
                me: me.clone(),
                metrics_namespace: metrics_namespace.to_string(),
                registry: reg.cloned(),
                node_name,
                compression_algo,
                codecs: RwLock::new({
                    let mut map: HashMap<String, Arc<dyn Codec>> = codecs
                        .into_iter()
                        .map(|c| (c.codec_id().to_string(), c))
                        .collect();
                    if cfg.propagation_delay_tracker.enabled {
                        let pt_codec = super::propagation_tracker::propagation_delay_tracker_codec();
                        map.insert(pt_codec.codec_id().to_string(), pt_codec);
                    }
                    map
                }),
                node_meta,
                dns,
                discover_backoff: opts.discover_members_backoff,
                timing,
                transport: OnceLock::new(),
                advertise: RwLock::new((IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0)),
                store: Mutex::new(HashMap::new()),
                local_state_cache: Mutex::new(HashMap::new()),
                last_local_state_size: AtomicUsize::new(0),
                watchers: Mutex::new(HashMap::new()),
                prefix_watchers: Mutex::new(HashMap::new()),
                key_notifications: Mutex::new(HashSet::new()),
                workers: Mutex::new(HashMap::new()),
                history: Mutex::new(MessageHistory::default()),
                local_broadcasts: Arc::new(TransmitLimitedQueue::new(retransmit_mult, members_fn(me.clone()))),
                gossip_broadcasts: Arc::new(TransmitLimitedQueue::new(retransmit_mult, members_fn(me.clone()))),
                delegate_ready: AtomicBool::new(false),
                nodes: RwLock::new(NodeTable::default()),
                num_nodes: AtomicU32::new(0),
                incarnation: AtomicU32::new(0),
                sequence_num: AtomicU32::new(0),
                ack_handlers: Mutex::new(HashMap::new()),
                awareness: Awareness::new(awareness_max, Arc::clone(&go_metrics)),
                ml_broadcasts: TransmitLimitedQueue::new(retransmit_mult, est_fn),
                probe_index: Mutex::new(0),
                leave: AtomicBool::new(false),
                leave_broadcast_tx: leave_tx,
                leave_broadcast_rx: tokio::sync::Mutex::new(leave_rx),
                handoff: Mutex::new((VecDeque::new(), VecDeque::new())),
                handoff_notify: Notify::new(),
                push_pull_req: AtomicUsize::new(0),
                zone_select,
                shutdown_ctx: ServiceContext::background(),
                service: OnceLock::new(),
                metrics: KvMetrics::new(metrics_namespace),
                go_metrics: Arc::clone(&go_metrics),
                stream_decryptor,
                max_cas_retries,
                before_join_hook,
                propagation_tracker: Mutex::new(None),
                cfg,
            }
        });

        if let Some(r) = reg {
            kv.metrics.register(r, metrics_namespace, Arc::downgrade(&kv));
            go_metrics.register(r);
        }

        let svc = {
            let w_start = Arc::downgrade(&kv);
            let w_run = Arc::downgrade(&kv);
            let w_stop = Arc::downgrade(&kv);
            basic_service(
                Some(Box::new(move |ctx| {
                    let w = w_start.clone();
                    Box::pin(async move {
                        let Some(kv) = w.upgrade() else {
                            return Err(ServiceError::Failure("memberlist KV dropped".to_string()));
                        };
                        kv.starting(ctx).await.map_err(ServiceError::Failure)
                    })
                })),
                Some(Box::new(move |ctx| {
                    let w = w_run.clone();
                    Box::pin(async move {
                        let Some(kv) = w.upgrade() else {
                            return Ok(());
                        };
                        kv.running(ctx).await.map_err(ServiceError::Failure)
                    })
                })),
                Some(Box::new(move |_failure| {
                    let w = w_stop.clone();
                    Box::pin(async move {
                        if let Some(kv) = w.upgrade() {
                            kv.stopping().await;
                        }
                        Ok(())
                    })
                })),
            )
            .with_name("memberlist_kv")
        };
        let _ = kv.service.set(svc);
        Ok(kv)
    }

    /// Creates the KV, starts it and waits until it is Running (fast-join done), like
    /// `services.StartAndAwaitRunning(ctx, NewKV(...))`.
    pub async fn start(
        cfg: MemberlistConfig,
        codecs: Vec<Arc<dyn Codec>>,
        metrics_namespace: &str,
        reg: Option<&Registry>,
    ) -> Result<Arc<Self>, KvError> {
        Self::start_with_options(cfg, codecs, metrics_namespace, reg, KvOptions::default()).await
    }

    pub async fn start_with_options(
        cfg: MemberlistConfig,
        codecs: Vec<Arc<dyn Codec>>,
        metrics_namespace: &str,
        reg: Option<&Registry>,
        opts: KvOptions,
    ) -> Result<Arc<Self>, KvError> {
        let kv = Self::new(cfg, codecs, metrics_namespace, reg, opts)?;
        let svc = kv.service();
        svc.start_async(ServiceContext::background())
            .await
            .map_err(|e| KvError::Other(e.to_string()))?;
        if let Err(e) = svc.await_running().await {
            let msg = svc
                .failure_case()
                .map(|f| f.to_string())
                .unwrap_or_else(|| e.to_string());
            return Err(KvError::Other(msg));
        }
        Ok(kv)
    }

    /// The `memberlist-kv` service (dskit `KV` is a `services.Service`).
    pub fn service(&self) -> Arc<ManagedBasicService> {
        Arc::clone(self.service.get().expect("service initialized in new()"))
    }

    pub fn state(&self) -> State {
        self.service.get().map(|s| s.state()).unwrap_or(State::New)
    }

    /// Stops the service (leave cluster) and waits for termination.
    pub async fn shutdown(&self) {
        let svc = self.service();
        if svc.state() == State::New {
            return;
        }
        svc.stop_async();
        let _ = svc.await_terminated().await;
    }

    fn transport(&self) -> &Arc<TcpTransport> {
        self.transport.get().expect("transport is created in starting()")
    }

    pub(crate) fn advertise(&self) -> (IpAddr, u16) {
        *self.advertise.read()
    }

    pub(crate) fn bind_port(&self) -> u16 {
        MEMBERLIST_DEFAULT_BIND_PORT
    }

    fn label_overhead(&self) -> usize {
        if self.cfg.cluster_label.is_empty() {
            0
        } else {
            2 + self.cfg.cluster_label.len()
        }
    }

    pub(crate) fn store_len(&self) -> usize {
        self.store.lock().len()
    }

    // --- service lifecycle ---------------------------------------------------------------------

    /// dskit `starting`: create transport + memberlist, then fast-join.
    async fn starting(self: Arc<Self>, ctx: ServiceContext) -> Result<(), String> {
        if self.cfg.cluster_label.len() > LABEL_MAX_SIZE {
            return Err(format!(
                "failed to create memberlist: could not use {:?} as a label: too long",
                self.cfg.cluster_label
            ));
        }
        info!(
            cluster_label = %self.cfg.cluster_label,
            node = %self.node_name,
            "Using memberlist cluster label and node name"
        );
        let reg = self.registry.clone();
        let (transport, packet_rx, stream_rx) =
            TcpTransport::new(&self.cfg, &self.metrics_namespace, reg.as_ref())
                .await
                .map_err(|e| format!("failed to create transport: {e}"))?;
        let (ip, port) = transport
            .final_advertise_addr(&self.cfg.advertise_addr, self.cfg.advertise_port)
            .map_err(|e| format!("failed to create memberlist: failed to get final advertise address: {e}"))?;
        let _ = self.transport.set(transport);
        *self.advertise.write() = (ip, port);

        self.spawn_listeners(packet_rx, stream_rx);
        self.set_alive();
        self.schedule();
        self.delegate_ready.store(true, Ordering::Release);

        if !self.cfg.join_members.0.is_empty() {
            if let Err(err) = self.fast_join_members_on_startup(&ctx).await {
                error!(%err, "failed to fast-join the memberlist cluster at startup");
                if self.cfg.abort_if_cluster_fast_join_fails {
                    return Err(format!("failed to fast-join the memberlist cluster at startup: {err}"));
                }
            }
        }
        Ok(())
    }

    /// dskit `running`: full join, optional periodic rejoin and obsolete entries cleanup.
    async fn running(self: Arc<Self>, ctx: ServiceContext) -> Result<(), String> {
        let notify_interval = self.cfg.notify_interval.as_std().unwrap_or(Duration::ZERO);
        if !notify_interval.is_zero() {
            let kv = Arc::clone(&self);
            let c = ctx.clone();
            tokio::spawn(async move {
                let mut t = tokio::time::interval(notify_interval);
                t.tick().await;
                loop {
                    tokio::select! {
                        _ = c.canceled() => return,
                        _ = t.tick() => kv.send_key_notifications(),
                    }
                }
            });
        }

        let ok = self.join_members_on_startup(&ctx).await;
        if let Some(sel) = &self.zone_select {
            sel.mark_joined();
        }
        if !ok && self.cfg.abort_if_cluster_join_fails {
            return Err(ERR_FAILED_TO_JOIN_CLUSTER.to_string());
        }

        if self.cfg.propagation_delay_tracker.enabled {
            let tracker = super::propagation_tracker::PropagationDelayTracker::new(
                self.me.clone(),
                self.cfg.propagation_delay_tracker.clone(),
                self.node_name.clone(),
                self.registry.as_ref(),
            );
            if let Err(err) = tracker.service().start_async(ctx.clone()).await {
                warn!(%err, "failed to start propagation delay tracker");
            } else {
                *self.propagation_tracker.lock() = Some(tracker);
            }
        }

        let rejoin_interval = self.cfg.rejoin_interval.as_std().unwrap_or(Duration::ZERO);
        let rejoin_nodes = self.rejoin_seed_nodes();
        let rejoin_enabled = !rejoin_interval.is_zero() && !rejoin_nodes.is_empty();
        // Random initial delay in [1ns, interval] to spread rejoins of simultaneously started processes.
        let mut next_rejoin = rejoin_enabled.then(|| {
            tokio::time::Instant::now()
                + Duration::from_nanos(
                    1 + rand::thread_rng().gen_range(0..rejoin_interval.as_nanos().max(1) as u64),
                )
        });
        let obsolete = self.cfg.obsolete_entries_timeout.as_std().unwrap_or(Duration::ZERO);
        let mut obsolete_tick = (!obsolete.is_zero()).then(|| {
            let mut t = tokio::time::interval_at(tokio::time::Instant::now() + obsolete, obsolete);
            t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            t
        });

        loop {
            let rejoin_sleep = async {
                match next_rejoin {
                    Some(at) => tokio::time::sleep_until(at).await,
                    None => std::future::pending::<()>().await,
                }
            };
            let obsolete_fut = async {
                match obsolete_tick.as_mut() {
                    Some(t) => {
                        t.tick().await;
                    }
                    None => std::future::pending::<()>().await,
                }
            };
            tokio::select! {
                _ = ctx.canceled() => return Ok(()),
                _ = rejoin_sleep => {
                    next_rejoin = Some(tokio::time::Instant::now() + rejoin_interval);
                    // Don't retry if resolution fails, we will try again next time.
                    match self.join_members_with_retries(&ctx, &rejoin_nodes, 1, "periodic_rejoin").await {
                        Ok(reached) => info!(phase = "periodic_rejoin", msg = "re-joined memberlist cluster", reached_nodes = reached),
                        Err(err) => warn!(phase = "periodic_rejoin", msg = "re-joining memberlist cluster failed", %err, next_try_in = ?rejoin_interval),
                    }
                }
                _ = obsolete_fut => self.cleanup_obsolete_entries(),
            }
        }
    }

    /// dskit stopping: flush local broadcasts, leave, shut down.
    async fn stopping(self: Arc<Self>) {
        let tracker = self.propagation_tracker.lock().take();
        if let Some(t) = tracker {
            let svc = t.service();
            svc.stop_async();
            if let Err(err) = svc.await_terminated().await {
                warn!(%err, "error stopping propagation delay tracker");
            }
        }
        info!("leaving memberlist cluster");
        if self.transport.get().is_some() {
            let timeout = self
                .cfg
                .broadcast_timeout_for_local_updates_on_shutdown
                .as_std()
                .unwrap_or(Duration::ZERO);
            let deadline = Instant::now() + timeout;
            let mut msgs = self.local_broadcasts.num_queued();
            let mut nodes = self.num_members();
            while msgs > 0 && nodes > 1 && (timeout.is_zero() || Instant::now() < deadline) {
                info!(count = msgs, nodes, "waiting for locally-generated broadcast messages to be sent out");
                tokio::time::sleep(Duration::from_millis(250)).await;
                msgs = self.local_broadcasts.num_queued();
                nodes = self.num_members();
            }
            if msgs > 0 {
                warn!(count = msgs, nodes, "locally-generated broadcast messages left the queue");
            }
            let leave_timeout = self.cfg.leave_timeout.as_std().unwrap_or(Duration::ZERO);
            if let Err(err) = self.leave(leave_timeout).await {
                error!(%err, "error when leaving memberlist cluster");
            }
        }
        self.shutdown_ctx.cancel();
        if let Some(t) = self.transport.get() {
            t.shutdown_and_wait().await;
        }
        for timer in self.nodes.read().timers.values() {
            timer.stop();
        }
    }

    // --- transport plumbing -------------------------------------------------------------------

    fn spawn_listeners(
        self: &Arc<Self>,
        mut packet_rx: mpsc::Receiver<ReceivedPacket>,
        mut stream_rx: mpsc::Receiver<MemberlistConn>,
    ) {
        // memberlist packetListen.
        {
            let kv = Arc::clone(self);
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
        // memberlist streamListen.
        {
            let kv = Arc::clone(self);
            let cancel = self.shutdown_ctx.clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = cancel.canceled() => break,
                        conn = stream_rx.recv() => {
                            let Some(conn) = conn else { break };
                            let kv_conn = Arc::clone(&kv);
                            tokio::spawn(async move { kv_conn.handle_conn(conn).await });
                        }
                    }
                }
            });
        }
        // memberlist packetHandler (handoff queues).
        {
            let kv = Arc::clone(self);
            let cancel = self.shutdown_ctx.clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = cancel.canceled() => break,
                        _ = kv.handoff_notify.notified() => {
                            while let Some(msg) = kv.get_next_message() {
                                kv.handle_handoff(msg);
                            }
                        }
                    }
                }
            });
        }
    }

    /// memberlist `setAlive`: register ourselves (bootstrap alive message).
    fn set_alive(&self) {
        let (addr, port) = self.advertise();
        let a = Alive {
            incarnation: self.incarnation.fetch_add(1, Ordering::AcqRel).wrapping_add(1),
            node: self.node_name.clone(),
            addr: ip_to_bytes(addr),
            port,
            meta: self.node_meta.clone(),
            vsn: vec![
                PROTOCOL_VERSION_MIN,
                PROTOCOL_VERSION_MAX,
                PROTOCOL_VERSION_2_COMPATIBLE,
                0,
                0,
                0,
            ],
        };
        self.alive_node(&a, None, true);
    }

    fn get_next_message(&self) -> Option<Handoff> {
        let mut q = self.handoff.lock();
        q.0.pop_back().or_else(|| q.1.pop_back())
    }

    fn handle_handoff(&self, msg: Handoff) {
        match msg.msg_type {
            MessageType::Suspect => match decode_suspect(&msg.buf) {
                Ok(s) => self.suspect_node(&s),
                Err(err) => error!(%err, from = %msg.from, "memberlist: Failed to decode suspect message"),
            },
            MessageType::Alive => match decode_alive(&msg.buf) {
                Ok(mut a) => {
                    if a.port == 0 {
                        a.port = self.bind_port();
                    }
                    self.alive_node(&a, None, false);
                }
                Err(err) => error!(%err, from = %msg.from, "memberlist: Failed to decode alive message"),
            },
            MessageType::Dead => match decode_dead(&msg.buf) {
                Ok(d) => self.dead_node(&d),
                Err(err) => error!(%err, from = %msg.from, "memberlist: Failed to decode dead message"),
            },
            MessageType::User => self.notify_msg(&msg.buf),
            other => error!(?other, "memberlist: Message type not supported (packet handler)"),
        }
    }

    /// memberlist `ingestPacket`.
    async fn ingest_packet(&self, raw: &[u8], from: &str) {
        let (buf, label) = match remove_label_header(raw) {
            Ok(v) => v,
            Err(err) => {
                error!(%err, from, "memberlist: failed to remove packet label");
                return;
            }
        };
        let label = if self.cfg.cluster_label_verification_disabled {
            self.cfg.cluster_label.as_str()
        } else {
            label
        };
        if label != self.cfg.cluster_label {
            error!(label, from, "memberlist: discarding packet with unacceptable label");
            return;
        }
        let buf = match unwrap_crc32(buf) {
            Ok(b) => b,
            Err(err) => {
                warn!(%err, from, "memberlist: Got invalid checksum for UDP packet");
                return;
            }
        };
        self.handle_command(buf, from, 0).await;
    }

    fn handle_command<'a>(&'a self, buf: &'a [u8], from: &'a str, depth: usize) -> BoxFuture<'a, ()> {
        const MAX_COMMAND_DEPTH: usize = 4;
        Box::pin(async move {
            if depth > MAX_COMMAND_DEPTH {
                warn!(depth, from, "memberlist: nested command exceeded max depth");
                return;
            }
            if buf.is_empty() {
                error!(from, "memberlist: missing message type byte");
                return;
            }
            let Ok(msg_type) = MessageType::try_from(buf[0]) else {
                error!(msg_type = buf[0], from, "memberlist: msg type not supported");
                return;
            };
            let body = &buf[1..];
            match msg_type {
                MessageType::Compound => match decode_compound_message(body) {
                    Ok((trunc, parts)) => {
                        if trunc > 0 {
                            warn!(trunc, from, "memberlist: Compound request had truncated messages");
                        }
                        for part in parts {
                            self.handle_command(part, from, depth + 1).await;
                        }
                    }
                    Err(err) => error!(%err, from, "memberlist: Failed to decode compound request"),
                },
                MessageType::Compress => self.handle_compressed(body, from, depth).await,
                MessageType::Ping => match decode_ping(body) {
                    Ok(p) => self.handle_ping(p, from).await,
                    Err(err) => error!(%err, from, "memberlist: Failed to decode ping request"),
                },
                MessageType::IndirectPing => match decode_indirect_ping(body) {
                    Ok(ind) => self.handle_indirect_ping(ind, from).await,
                    Err(err) => error!(%err, from, "memberlist: Failed to decode indirect ping request"),
                },
                MessageType::AckResp => match decode_ack_resp(body) {
                    Ok(ack) => self.invoke_ack_handler(&ack),
                    Err(err) => error!(%err, from, "memberlist: Failed to decode ack response"),
                },
                MessageType::NackResp => match decode_nack_resp(body) {
                    Ok(nack) => self.invoke_nack_handler(&nack),
                    Err(err) => error!(%err, from, "memberlist: Failed to decode nack response"),
                },
                MessageType::Suspect | MessageType::Alive | MessageType::Dead | MessageType::User => {
                    let dropped = {
                        let mut q = self.handoff.lock();
                        let queue = if msg_type == MessageType::Alive { &mut q.0 } else { &mut q.1 };
                        if queue.len() >= self.timing.handoff_queue_depth {
                            true
                        } else {
                            queue.push_back(Handoff {
                                msg_type,
                                buf: body.to_vec(),
                                from: from.to_string(),
                            });
                            false
                        }
                    };
                    if dropped {
                        warn!(?msg_type, from, "memberlist: handler queue full, dropping message");
                    } else {
                        self.handoff_notify.notify_one();
                    }
                }
                other => error!(?other, from, "memberlist: msg type not supported"),
            }
        })
    }

    fn compress_labels(&self) -> &'static str {
        match self.compression_algo {
            CompressionType::Snappy => "snappy",
            _ => "lzw",
        }
    }

    fn decompress_algo(algo: CompressionType) -> &'static str {
        match algo {
            CompressionType::Snappy => "snappy",
            CompressionType::Lzw => "lzw",
            #[allow(unreachable_patterns)]
            _ => "unknown",
        }
    }

    /// memberlist `handleCompressed`.
    async fn handle_compressed(&self, body: &[u8], from: &str, depth: usize) {
        match super::wire::decode_compressed_payload(body) {
            Ok(cp) => {
                let labels = [("algo", Self::decompress_algo(cp.algo))];
                self.go_metrics
                    .incr_counter(&["memberlist", "decompress", "attempts_total"], 1.0, &labels);
                match decompress_buffer(cp.algo, &cp.buf) {
                    Ok(decomp) => self.handle_command(&decomp, from, depth + 1).await,
                    Err(err) => {
                        self.go_metrics
                            .incr_counter(&["memberlist", "decompress", "errors_total"], 1.0, &labels);
                        error!(%err, from, "memberlist: Failed to decompress payload");
                    }
                }
            }
            Err(err) => {
                let labels = [("algo", "unknown")];
                self.go_metrics
                    .incr_counter(&["memberlist", "decompress", "attempts_total"], 1.0, &labels);
                self.go_metrics
                    .incr_counter(&["memberlist", "decompress", "errors_total"], 1.0, &labels);
                error!(%err, from, "memberlist: Failed to decompress payload");
            }
        }
    }

    /// memberlist `compressPayload` with go-metrics accounting; returns the bytes to send.
    fn maybe_compress(&self, msg: Vec<u8>) -> Vec<u8> {
        if !self.cfg.compression_enabled {
            return msg;
        }
        let algo = self.compress_labels();
        self.go_metrics
            .incr_counter(&["memberlist", "compress", "attempts_total"], 1.0, &[("algo", algo)]);
        match compress_payload(self.compression_algo, &msg) {
            Ok(comp) if comp.len() < msg.len() => comp,
            Ok(_) => {
                self.go_metrics.incr_counter(
                    &["memberlist", "compress", "skipped_total"],
                    1.0,
                    &[("algo", algo), ("reason", "size_worse_than_original")],
                );
                msg
            }
            Err(err) => {
                self.go_metrics
                    .incr_counter(&["memberlist", "compress", "errors_total"], 1.0, &[("algo", algo)]);
                warn!(%err, "memberlist: Failed to compress payload");
                msg
            }
        }
    }

    /// memberlist `sendMsg`: piggybacks pending broadcasts on the message.
    pub(crate) async fn send_msg(&self, addr: &str, msg: Vec<u8>) -> Result<(), KvError> {
        let bytes_avail = UDP_BUFFER_SIZE
            .saturating_sub(msg.len() + COMPOUND_HEADER_OVERHEAD + self.label_overhead());
        let extra = self.get_broadcasts(COMPOUND_OVERHEAD, bytes_avail);
        if extra.is_empty() {
            return self.raw_send_msg_packet(addr, None, msg).await;
        }
        let mut msgs = Vec::with_capacity(1 + extra.len());
        msgs.push(msg);
        msgs.extend(extra);
        for compound in make_compound_messages(&msgs) {
            self.raw_send_msg_packet(addr, None, compound).await?;
        }
        Ok(())
    }

    /// memberlist `rawSendMsgPacket`.
    pub(crate) async fn raw_send_msg_packet(
        &self,
        addr: &str,
        node: Option<&NodeState>,
        msg: Vec<u8>,
    ) -> Result<(), KvError> {
        let mut msg = self.maybe_compress(msg);
        let looked_up: Option<NodeState>;
        let node_ref = match node {
            Some(n) => Some(n),
            None => {
                if let Some((host, _port)) = crate::dns::split_host_port(addr) {
                    let table = self.nodes.read();
                    looked_up = table.map.get(host).cloned();
                    looked_up.as_ref()
                } else {
                    error!(addr, "memberlist: Failed to parse address");
                    return Err(KvError::Other(format!("Failed to parse address {addr:?}")));
                }
            }
        };
        if node_ref.is_some_and(|n| n.pmax() >= 5) {
            msg = wrap_crc32(&msg);
        }
        self.go_metrics
            .incr_counter(&["memberlist", "udp", "sent"], msg.len() as f64, &[]);
        let framed = add_label_header(&msg, &self.cfg.cluster_label).map_err(KvError::Other)?;
        self.transport().write_to(framed, addr.to_string()).await;
        Ok(())
    }

    /// memberlist `rawSendMsgStream` (never writes a label header; outbound stream dials
    /// prepend the label header once on connection setup).
    async fn send_raw_stream<W: tokio::io::AsyncWrite + Unpin>(
        &self,
        writer: &mut W,
        send_buf: Vec<u8>,
        _label: &str,
    ) -> Result<(), io::Error> {
        let send_buf = self.maybe_compress(send_buf);
        self.go_metrics
            .incr_counter(&["memberlist", "tcp", "sent"], send_buf.len() as f64, &[]);
        writer.write_all(&send_buf).await?;
        writer.flush().await?;
        Ok(())
    }

    /// memberlist `handleConn`.
    async fn handle_conn(&self, mut conn: MemberlistConn) {
        self.go_metrics.incr_counter(&["memberlist", "tcp", "accept"], 1.0, &[]);
        let timeout = self.timing.tcp_timeout;
        let fut = self.handle_conn_inner(&mut conn);
        if timeout.is_zero() {
            fut.await;
        } else {
            let _ = tokio::time::timeout(timeout, fut).await;
        }
    }

    async fn handle_conn_inner(&self, conn: &mut MemberlistConn) {
        let (read_half, mut write_half) = conn.split();
        let mut reader = BufReader::new(read_half);

        let (stream_label, first_byte) = match remove_label_header_from_stream(&mut reader).await {
            Ok(v) => v,
            Err(err) => {
                error!(%err, "memberlist: failed to receive and remove the stream label header");
                return;
            }
        };
        let label = if self.cfg.cluster_label_verification_disabled {
            self.cfg.cluster_label.clone()
        } else {
            stream_label
        };
        if label != self.cfg.cluster_label {
            error!(label, "memberlist: discarding stream with unacceptable label");
            return;
        }
        let first = match first_byte {
            StreamFirstByte::Eof => return,
            StreamFirstByte::Next => None,
            StreamFirstByte::Byte(b) => Some(b),
        };

        let (msg_type, body_opt) = match read_stream_with_first_byte(
            &mut reader,
            first,
            &label,
            Some(&self.go_metrics),
            self.stream_decryptor.as_ref(),
        )
        .await
        {
            Ok(v) => v,
            Err(StreamReadError::Eof) => return,
            Err(StreamReadError::Other(err)) => {
                error!(%err, "memberlist: failed to receive");
                let err_msg = encode_err_resp(&ErrResp { error: err });
                let _ = self.send_raw_stream(&mut write_half, err_msg, &label).await;
                return;
            }
        };

        match msg_type {
            MessageType::PushPull => {
                let n = self.push_pull_req.fetch_add(1, Ordering::AcqRel) + 1;
                struct Dec<'a>(&'a AtomicUsize);
                impl Drop for Dec<'_> {
                    fn drop(&mut self) {
                        self.0.fetch_sub(1, Ordering::AcqRel);
                    }
                }
                let _dec = Dec(&self.push_pull_req);
                if n >= MAX_PUSH_PULL_REQUESTS {
                    error!("memberlist: Too many pending push/pull requests");
                    return;
                }
                let res = match body_opt {
                    Some(in_mem) => decode_push_pull(&in_mem).map(|(j, n, s)| (j, n, s.to_vec())),
                    None => read_push_pull_from_stream(&mut reader).await,
                };
                match res {
                    Ok((join, remote_nodes, user_state)) => {
                        if let Err(err) = self.send_local_state(&mut write_half, join, &label).await {
                            error!(%err, "memberlist: Failed to push local state");
                            return;
                        }
                        if let Err(err) = self.merge_remote_state_full(join, &remote_nodes, &user_state) {
                            error!(%err, "memberlist: Failed push/pull merge");
                        }
                    }
                    Err(err) => {
                        error!(%err, "memberlist: Failed to read remote state");
                        let err_msg = encode_err_resp(&ErrResp { error: err });
                        let _ = self.send_raw_stream(&mut write_half, err_msg, &label).await;
                    }
                }
            }
            MessageType::User => {
                let res = match body_opt {
                    Some(in_mem) => super::wire::decode_user_msg_stream(&in_mem).map(|s| s.to_vec()),
                    None => read_user_msg_from_stream(&mut reader).await,
                };
                match res {
                    Ok(payload) => self.notify_msg(&payload),
                    Err(err) => error!(%err, "memberlist: Failed to receive user message"),
                }
            }
            MessageType::Ping => {
                let res = match body_opt {
                    Some(in_mem) => decode_ping(&in_mem),
                    None => read_msgpack_map_bytes(&mut reader).await.and_then(|b| decode_ping(&b)),
                };
                match res {
                    Ok(p) => {
                        if !p.node.is_empty() && p.node != self.node_name {
                            warn!(node = %p.node, "memberlist: Got ping for unexpected node");
                            return;
                        }
                        let ack = encode_ack_resp(&AckResp {
                            seq_no: p.seq_no,
                            payload: None,
                        });
                        if let Err(err) = self.send_raw_stream(&mut write_half, ack, &label).await {
                            error!(%err, "memberlist: Failed to send ack");
                        }
                    }
                    Err(err) => error!(%err, "memberlist: Failed to decode ping"),
                }
            }
            other => error!(?other, "memberlist: Received invalid msgType"),
        }
    }

    /// memberlist `sendLocalState`.
    async fn send_local_state<W: tokio::io::AsyncWrite + Unpin>(
        &self,
        writer: &mut W,
        join: bool,
        label: &str,
    ) -> Result<(), io::Error> {
        let local_nodes: Vec<PushNodeState> = self
            .nodes
            .read()
            .ordered()
            .into_iter()
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
        self.record_node_instances(&local_nodes);
        let user_data = self.local_state(join);
        let payload = encode_push_pull(&local_nodes, &user_data, join);
        if payload.len() >= 5 {
            let more = u32::from_be_bytes([payload[1], payload[2], payload[3], payload[4]]);
            self.go_metrics
                .set_gauge(&["memberlist", "size", "local"], more as f64, &[]);
        }
        self.send_raw_stream(writer, payload, label).await
    }

    /// memberlist `mergeRemoteState`.
    fn merge_remote_state_full(
        &self,
        join: bool,
        remote_nodes: &[PushNodeState],
        user_state: &[u8],
    ) -> Result<(), String> {
        self.verify_protocol(remote_nodes)?;
        self.merge_state(remote_nodes);
        if !user_state.is_empty() {
            self.merge_remote_state(user_state, join);
        }
        Ok(())
    }

    /// memberlist `pushPullNode` (+ `sendAndReceiveState`).
    pub(crate) async fn push_pull_node(&self, addr: &str, join: bool) -> Result<(), KvError> {
        let started = Instant::now();
        let timeout = self.timing.tcp_timeout;
        let mut conn = self.transport().dial_timeout(addr, timeout).await?;
        self.go_metrics.incr_counter(&["memberlist", "tcp", "connect"], 1.0, &[]);
        let label = self.cfg.cluster_label.clone();
        let (read_half, mut write_half) = conn.split();
        let write_fut = async {
            if !label.is_empty() {
                let label_hdr = add_label_header(&[], &label).map_err(KvError::Other)?;
                write_half.write_all(&label_hdr).await?;
            }
            self.send_local_state(&mut write_half, join, &label).await?;
            Ok::<(), KvError>(())
        };
        if timeout.is_zero() {
            write_fut.await?;
        } else {
            tokio::time::timeout(timeout, write_fut)
                .await
                .map_err(|_| KvError::Other("i/o timeout".to_string()))??;
        }

        let mut reader = BufReader::new(read_half);
        let read_fut = async {
            let (msg_type, body_opt) = read_stream_with_first_byte(
                &mut reader,
                None,
                &label,
                Some(&self.go_metrics),
                self.stream_decryptor.as_ref(),
            )
            .await
            .map_err(|e| KvError::Other(e.to_string()))?;
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
                    "received invalid msgType ({}), expected pushPullMsg ({})",
                    other as u8,
                    MessageType::PushPull as u8
                ))),
            }
        };
        let res = if timeout.is_zero() {
            read_fut.await
        } else {
            tokio::time::timeout(timeout, read_fut)
                .await
                .map_err(|_| KvError::Other("i/o timeout".to_string()))
                .and_then(|r| r)
        };
        let (remote_nodes, user_state) = res?;
        self.merge_remote_state_full(join, &remote_nodes, &user_state)
            .map_err(KvError::Other)?;
        self.go_metrics
            .measure_since(&["memberlist", "pushPullNode"], started, &[]);
        Ok(())
    }

    // --- joining -----------------------------------------------------------------------------------

    /// dskit `GetRejoinSeedNodes`.
    fn rejoin_seed_nodes(&self) -> Vec<String> {
        if !self.cfg.rejoin_seed_nodes.0.is_empty() {
            self.cfg.rejoin_seed_nodes.0.clone()
        } else {
            self.cfg.join_members.0.clone()
        }
    }

    /// memberlist `resolveAddr`.
    async fn resolve_addr(&self, host_str: &str) -> Result<Vec<String>, String> {
        let mut host_str = host_str;
        if let Some(idx) = host_str.find('/') {
            if idx == 0 {
                return Err("empty node name provided".to_string());
            }
            host_str = &host_str[idx + 1..];
        }
        let with_port = ensure_port(host_str, self.bind_port());
        let (host, port) = match crate::dns::split_host_port(&with_port) {
            Some((h, p)) => (h.to_string(), p.to_string()),
            None => return Err(format!("address {with_port}: missing port in address")),
        };
        let port: u16 = port.parse().map_err(|e| format!("strconv.ParseUint: parsing {port:?}: {e}"))?;
        if let Ok(ip) = host.parse::<IpAddr>() {
            return Ok(vec![SocketAddr::new(ip, port).to_string()]);
        }
        match tokio::net::lookup_host(crate::dns::join_host_port(&host, &port.to_string())).await {
            Ok(addrs) => Ok(addrs.map(|a| a.to_string()).collect()),
            Err(err) => Err(format!("lookup {host}: {err}")),
        }
    }

    /// memberlist `Join`: returns the number of nodes successfully contacted.
    async fn ml_join(&self, existing: &[String]) -> (usize, Option<String>) {
        let mut num_success = 0;
        let mut errs: Vec<String> = Vec::new();
        for exist in existing {
            let addrs = match self.resolve_addr(exist).await {
                Ok(a) => a,
                Err(err) => {
                    let err = format!("failed to resolve {exist}: {err}");
                    warn!("memberlist: {err}");
                    errs.push(err);
                    continue;
                }
            };
            for addr in addrs {
                match self.push_pull_node(&addr, true).await {
                    Ok(()) => num_success += 1,
                    Err(err) => {
                        let err = format!("failed to join {addr}: {err}");
                        debug!("memberlist: {err}");
                        errs.push(err);
                    }
                }
            }
        }
        if num_success > 0 || errs.is_empty() {
            (num_success, None)
        } else {
            (num_success, Some(multierror_text(&errs)))
        }
    }

    /// dskit `JoinMembers`.
    pub async fn join_members(&self, members: &[String]) -> Result<usize, KvError> {
        if self.state() != State::Running {
            return Err(KvError::Other("service not Running".to_string()));
        }
        match self.ml_join(members).await {
            (n, None) => Ok(n),
            (_, Some(err)) => Err(KvError::Other(err)),
        }
    }

    /// dskit `fastJoinMembersOnStartup`.
    async fn fast_join_members_on_startup(&self, ctx: &ServiceContext) -> Result<(), String> {
        let start = Instant::now();
        let (mut nodes, err) = self.discover_members_with_retries(ctx, &self.cfg.join_members.0).await;
        if let Some(err) = err {
            if nodes.is_empty() {
                return Err(err);
            }
        }
        nodes.shuffle(&mut rand::thread_rng());
        let mut to_join = self.timing.retransmit_mult as i64
            * ((nodes.len() as f64 + 1.0).log10().ceil() as i64);
        info!(nodes_found = nodes.len(), to_join, "memberlist fast-join starting");

        let mut total_joined = 0i64;
        let mut nodes: VecDeque<String> = nodes.into();
        while to_join > 0 && !nodes.is_empty() && !ctx.is_canceled() {
            let node = nodes.pop_front().expect("non-empty");
            let (reached, err) = self.ml_join(std::slice::from_ref(&node)).await;
            if let Some(err) = err {
                info!(node, %err, "fast-joining node failed");
            }
            total_joined += reached as i64;
            to_join -= reached as i64;
        }

        let required = self.cfg.abort_if_cluster_fast_join_fails_min_nodes;
        if total_joined < required {
            warn!(joined_nodes = total_joined, required_nodes = required, elapsed_time = ?start.elapsed(), "memberlist fast-join failed to reach minimum required seed nodes");
            return Err(format!(
                "fast-join failed to reach minimum required seed nodes: joined {total_joined}, required {required}"
            ));
        }
        info!(joined_nodes = total_joined, elapsed_time = ?start.elapsed(), "memberlist fast-join finished");
        Ok(())
    }

    /// dskit `joinMembersOnStartup`.
    async fn join_members_on_startup(&self, ctx: &ServiceContext) -> bool {
        if let Some(hook) = &self.before_join_hook {
            hook(ctx.clone()).await;
        }
        let members = self.cfg.join_members.0.clone();
        if members.is_empty() {
            return true;
        }
        info!(phase = "startup", msg = "joining memberlist cluster", join_members = %members.join(","));
        let start = Instant::now();
        let max = self.cfg.max_join_retries.max(0) as usize;
        match self.join_members_with_retries(ctx, &members, max, "startup").await {
            Ok(reached) => {
                info!(phase = "startup", msg = "joining memberlist cluster succeeded", reached_nodes = reached, elapsed_time = ?start.elapsed());
                true
            }
            Err(err) => {
                error!(phase = "startup", msg = "joining memberlist cluster failed", %err, elapsed_time = ?start.elapsed());
                false
            }
        }
    }

    /// dskit `joinMembersWithRetries`. `num_attempts == 0` retries forever (dskit backoff).
    async fn join_members_with_retries(
        &self,
        ctx: &ServiceContext,
        members: &[String],
        num_attempts: usize,
        phase: &'static str,
    ) -> Result<usize, String> {
        let mut boff = Backoff::new(BackoffConfig {
            min_backoff: self.cfg.min_join_backoff.as_std().unwrap_or(Duration::ZERO),
            max_backoff: self.cfg.max_join_backoff.as_std().unwrap_or(Duration::ZERO),
            max_retries: num_attempts,
        });
        let mut err: Option<String> = None;
        let mut joined = 0;
        let mut canceled = false;
        while boff.ongoing() && !ctx.is_canceled() {
            let (n, e) = self.join_members_in_batches(ctx, members).await;
            joined = n;
            err = e;
            if joined > 0 {
                err = None;
                break;
            }
            warn!(
                phase,
                msg = "joining memberlist cluster",
                attempts = boff.num_retries() + 1,
                max_attempts = num_attempts,
                err = err.as_deref().unwrap_or(""),
            );
            let delay = boff.next_delay();
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                _ = ctx.canceled() => { canceled = true; }
            }
            if canceled {
                break;
            }
        }
        if err.is_none() && joined == 0 {
            if ctx.is_canceled() || canceled {
                err = Some("joining memberlist: context canceled".to_string());
            } else if let Some(e) = boff.err() {
                err = Some(format!("joining memberlist: {e}"));
            }
        }
        match err {
            Some(e) => Err(e),
            None => Ok(joined),
        }
    }

    /// dskit `joinMembersInBatches`.
    async fn join_members_in_batches(&self, ctx: &ServiceContext, members: &[String]) -> (usize, Option<String>) {
        const BATCH_SIZE: usize = 100;
        let mut attempted: HashSet<String> = HashSet::new();
        let mut successfully_joined = 0;
        let mut last_err: Option<String> = None;
        let mut nodes: Vec<String> = Vec::new();
        let mut more = true;
        while !ctx.is_canceled() && more {
            let (newly, _) = self.discover_members_with_retries(ctx, members).await;
            if !newly.is_empty() {
                nodes = newly;
            }
            let mut batch = Vec::with_capacity(BATCH_SIZE);
            more = false;
            for n in &nodes {
                if attempted.contains(n) {
                    continue;
                }
                if batch.len() >= BATCH_SIZE {
                    more = true;
                    break;
                }
                batch.push(n.clone());
                attempted.insert(n.clone());
            }
            // dskit joinMembersBatch.
            let mut joined_in_batch = 0;
            let mut batch_err = None;
            for node in &batch {
                if ctx.is_canceled() {
                    batch_err = Some("joining batch: context canceled".to_string());
                    break;
                }
                let (reached, err) = self.ml_join(std::slice::from_ref(node)).await;
                joined_in_batch += reached;
                if err.is_some() {
                    batch_err = err;
                }
            }
            if joined_in_batch > 0 {
                batch_err = None;
            }
            if batch_err.is_some() {
                last_err = batch_err;
            }
            successfully_joined += joined_in_batch;
        }
        if successfully_joined > 0 {
            return (successfully_joined, None);
        }
        if last_err.is_none() {
            return (0, Some("found no nodes to join".to_string()));
        }
        (0, last_err)
    }

    /// dskit `discoverMembers`.
    async fn discover_members(&self, members: &[String]) -> (Vec<String>, Option<String>) {
        if members.is_empty() {
            return (Vec::new(), None);
        }
        let mut ms = Vec::new();
        let mut resolve = Vec::new();
        for m in members {
            if m.contains('+') {
                resolve.push(m.clone());
            } else {
                ms.push(m.clone());
            }
        }
        let err = self.dns.resolve(&resolve).await.err().map(|e| {
            let e = e.to_string();
            warn!(addrs = %resolve.join(","), err = %e, "failed to resolve members");
            e
        });
        ms.extend(self.dns.addresses());
        (ms, err)
    }

    /// dskit `discoverMembersWithRetries`.
    async fn discover_members_with_retries(
        &self,
        ctx: &ServiceContext,
        members: &[String],
    ) -> (Vec<String>, Option<String>) {
        let mut boff = Backoff::new(self.discover_backoff);
        let mut last = (Vec::new(), None);
        while boff.ongoing() && !ctx.is_canceled() {
            last = self.discover_members(members).await;
            if last.1.is_none() {
                return last;
            }
            let delay = boff.next_delay();
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                _ = ctx.canceled() => break,
            }
        }
        last
    }

    // --- accessors ---------------------------------------------------------------------------------

    pub fn node_name(&self) -> &str {
        &self.node_name
    }

    pub fn configured_compression_algorithm(&self) -> &str {
        &self.cfg.compression_algorithm
    }

    pub fn wire_compression_type(&self) -> CompressionType {
        self.compression_algo
    }

    pub fn set_delegate_ready(&self, ready: bool) {
        self.delegate_ready.store(ready, Ordering::Release);
    }

    pub fn num_key_workers(&self) -> usize {
        self.workers.lock().len()
    }

    pub fn local_state_cache_len(&self) -> usize {
        self.local_state_cache.lock().len()
    }

    pub fn local_state_cache_contains(&self, key: &str) -> bool {
        self.local_state_cache.lock().contains_key(key)
    }

    pub fn local_state_cache_counts(&self, key: &str) -> (f64, f64) {
        (
            self.metrics.local_state_cache_hits.with_label_values(&[key]).get(),
            self.metrics.local_state_cache_misses.with_label_values(&[key]).get(),
        )
    }

    pub fn local_state_cache_metric_counts(&self) -> (usize, usize) {
        let hits: usize = self.metrics.local_state_cache_hits.collect().iter().map(|mf| mf.get_metric().len()).sum();
        let misses: usize = self.metrics.local_state_cache_misses.collect().iter().map(|mf| mf.get_metric().len()).sum();
        (hits, misses)
    }

    pub fn store_insert_for_test(
        &self,
        key: &str,
        value: Box<dyn Mergeable>,
        version: usize,
        codec_id: &str,
        deleted: bool,
        update_time: Option<SystemTime>,
    ) {
        self.store.lock().insert(
            key.to_string(),
            ValueDesc {
                value,
                version,
                codec_id: codec_id.to_string(),
                deleted,
                update_time,
            },
        );
    }

    pub fn store_age_entry_for_test(&self, key: &str, update_time: SystemTime) {
        if let Some(entry) = self.store.lock().get_mut(key) {
            entry.update_time = Some(update_time);
        }
    }

    pub fn merge_value_for_key_for_test(
        &self,
        key: &str,
        incoming: &mut dyn Mergeable,
        cas_version: usize,
        codec_id: &str,
        deleted: bool,
        update_time: Option<SystemTime>,
    ) -> Result<usize, KvError> {
        let (_, version, _, _) = self.merge_value_for_key(
            key,
            incoming,
            cas_version,
            codec_id,
            deleted,
            update_time,
        )?;
        Ok(version)
    }

    pub fn enqueue_key_update_for_test(
        &self,
        key: &str,
        value: Bytes,
        codec: Arc<dyn Codec>,
        deleted: bool,
    ) -> bool {
        self.enqueue_key_update(
            key,
            ValueUpdate {
                value,
                codec,
                message_size: 0,
                deleted,
                update_time: Some(SystemTime::now()),
            },
        )
    }

    pub fn add_test_key_watcher(&self, key: &str) -> Arc<Notify> {
        let notify = Arc::new(Notify::new());
        self.watchers
            .lock()
            .entry(key.to_string())
            .or_default()
            .push(WatcherEntry {
                notify: Arc::clone(&notify),
            });
        notify
    }

    pub fn remove_test_key_watcher(&self, key: &str, notify: &Arc<Notify>) {
        let mut w = self.watchers.lock();
        if let Some(list) = w.get_mut(key) {
            list.retain(|e| !Arc::ptr_eq(&e.notify, notify));
            if list.is_empty() {
                w.remove(key);
            }
        }
    }

    pub fn shutdown_workers_for_test(&self) {
        self.shutdown_ctx.cancel();
    }

    /// The dskit `KVConfig` this KV was created with.
    pub fn config(&self) -> &MemberlistConfig {
        &self.cfg
    }

    /// dskit `storeCopy`: deep copy of the whole store, including deleted entries.
    pub fn store_copy(&self) -> HashMap<String, ValueDesc> {
        self.store.lock().iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }

    pub fn node_meta(&self) -> Option<&[u8]> {
        self.node_meta.as_deref()
    }

    /// dskit `GetListeningPort` (only valid once started).
    pub fn listening_port(&self) -> u16 {
        self.transport.get().map(|t| t.auto_bind_port()).unwrap_or(0)
    }

    pub fn advertise_addr(&self) -> String {
        self.transport
            .get()
            .map(|t| t.get_advertised_addr())
            .unwrap_or_default()
    }

    pub fn get_codec(&self, codec_id: &str) -> Option<Arc<dyn Codec>> {
        self.codecs.read().get(codec_id).cloned()
    }

    pub fn register_codec(&self, codec: Arc<dyn Codec>) {
        self.codecs.write().insert(codec.codec_id().to_string(), codec);
    }

    /// memberlist `Members`: nodes that are not dead or left.
    pub fn members(&self) -> Vec<NodeState> {
        self.nodes
            .read()
            .ordered()
            .into_iter()
            .filter(|n| !n.dead_or_left())
            .cloned()
            .collect()
    }

    /// All known nodes including dead/left ones not reaped yet.
    pub fn all_nodes(&self) -> Vec<NodeState> {
        self.nodes.read().ordered().into_iter().cloned().collect()
    }

    /// dskit `getSentAndReceivedMessages`.
    pub fn sent_and_received_messages(&self) -> (Vec<HistoryMessage>, Vec<HistoryMessage>) {
        let h = self.history.lock();
        (h.sent.iter().cloned().collect(), h.received.iter().cloned().collect())
    }

    /// dskit `deleteSentReceivedMessages`.
    pub fn delete_sent_received_messages(&self) {
        let mut h = self.history.lock();
        h.sent.clear();
        h.sent_size = 0;
        h.received.clear();
        h.received_size = 0;
    }

    fn add_received_message(&self, mut msg: HistoryMessage) {
        let limit = self.cfg.message_history_buffer_bytes.max(0) as usize;
        if limit == 0 {
            return;
        }
        let mut h = self.history.lock();
        h.counter += 1;
        msg.id = h.counter;
        let MessageHistory { received, received_size, .. } = &mut *h;
        add_message_to_buffer(received, received_size, limit, msg);
    }

    fn add_sent_message(&self, mut msg: HistoryMessage) {
        let limit = self.cfg.message_history_buffer_bytes.max(0) as usize;
        if limit == 0 {
            return;
        }
        let mut h = self.history.lock();
        h.counter += 1;
        msg.id = h.counter;
        let MessageHistory { sent, sent_size, .. } = &mut *h;
        add_message_to_buffer(sent, sent_size, limit, msg);
    }

    // --- KV operations -----------------------------------------------------------------------------

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
        let desc = {
            let store = self.store.lock();
            store.get(key).cloned()?
        };
        Some((desc.value, desc.version))
    }

    /// dskit `Delete`.
    pub async fn delete_key(&self, key: &str) -> Result<(), KvError> {
        let existing = self.store.lock().get(key).cloned();
        let Some(val) = existing else {
            return Ok(());
        };
        if val.deleted {
            return Ok(());
        }
        let codec = self.get_codec(&val.codec_id).ok_or_else(|| {
            error!(key, codec = %val.codec_id, "could not mark key for deletion due to an invalid codec");
            KvError::Codec(format!("invalid codec: {}", val.codec_id))
        })?;
        let mut cloned = val.value.clone_mergeable();
        let (change, new_ver, deleted, updated) = self.merge_value_for_key(
            key,
            cloned.as_mut(),
            0,
            &val.codec_id,
            true,
            Some(SystemTime::now()),
        )?;
        if new_ver > 0 {
            self.notify_watchers(key);
            if let Some(ch) = change {
                self.broadcast_new_value(key, ch.as_ref(), new_ver, codec.as_ref(), false, deleted, updated);
            }
        }
        info!(key, "successfully marked key for deletion");
        Ok(())
    }

    /// dskit `CAS`.
    pub async fn cas_with_codec(&self, key: &str, codec: &dyn Codec, mut f: CasFn) -> Result<(), KvError> {
        let mut last_err: Option<String> = None;
        let mut last_was_no_change = false;
        let mut version_mismatch = false;
        let min_b = self.cfg.cas_retry_min_backoff.as_std().unwrap_or(Duration::ZERO);
        let mut retry_backoff = (!min_b.is_zero()).then(|| {
            Backoff::new(BackoffConfig {
                min_backoff: min_b,
                max_backoff: self.cfg.cas_retry_max_backoff.as_std().unwrap_or(Duration::ZERO),
                max_retries: 0,
            })
        });

        for _ in 0..self.max_cas_retries {
            self.metrics.cas_attempts.inc();
            if last_was_no_change {
                tokio::time::sleep(NO_CHANGE_DETECTED_RETRY_SLEEP).await;
            } else if last_err.is_some() {
                if let Some(b) = retry_backoff.as_mut() {
                    tokio::time::sleep(b.next_delay()).await;
                }
            }
            last_was_no_change = false;
            version_mismatch = false;

            let (current_any, ver) = match self.get_value(key) {
                Some((v, ver)) => (Some(v), ver),
                None => (None, 0),
            };
            let outcome = match f(current_any.as_deref()) {
                Ok(o) => o,
                Err(cb_err) => {
                    debug!(err = %cb_err.err, retry = cb_err.retry, "CAS attempt failed");
                    last_err = Some(format!("fn returned error: {}", cb_err.err));
                    if !cb_err.retry {
                        break;
                    }
                    continue;
                }
            };
            let (incoming_any, retry_on_err) = match outcome {
                CasOutcome::NoChange => return Ok(()),
                CasOutcome::Update(v) => (v, true),
                CasOutcome::UpdateOnce(v) => (v, false),
            };
            let encoded = codec.encode(incoming_any.as_ref())?;
            let mut incoming = codec.decode_mergeable(&encoded)?;
            match self.merge_value_for_key(
                key,
                incoming.as_mut(),
                ver,
                codec.codec_id(),
                false,
                Some(SystemTime::now()),
            ) {
                Ok((change, new_ver, deleted, updated)) => {
                    if new_ver == 0 {
                        last_was_no_change = true;
                        last_err = Some("no change detected".to_string());
                        if !retry_on_err {
                            break;
                        }
                        continue;
                    }
                    self.metrics.cas_successes.inc();
                    self.notify_watchers(key);
                    if let Some(ch) = change {
                        self.broadcast_new_value(key, ch.as_ref(), new_ver, codec, true, deleted, updated);
                    }
                    return Ok(());
                }
                Err(KvError::CasFailed { .. }) => {
                    version_mismatch = true;
                    last_err = Some("version mismatch".to_string());
                    if !retry_on_err {
                        break;
                    }
                }
                Err(e) => {
                    last_err = Some(format!("merge failed: {e}"));
                    if !retry_on_err {
                        break;
                    }
                }
            }
        }
        let mut err = last_err.unwrap_or_default();
        if version_mismatch {
            err = format!("too many retries: {err}");
        }
        self.metrics.cas_failures.inc();
        Err(KvError::Other(format!("failed to CAS-update key {key}: {err}")))
    }

    /// dskit `mergeValueForKey`.
    fn merge_value_for_key(
        &self,
        key: &str,
        incoming: &mut dyn Mergeable,
        cas_version: usize,
        codec_id: &str,
        deleted: bool,
        update_time: Option<SystemTime>,
    ) -> Result<MergeValueResult, KvError> {
        let mut store = self.store.lock();
        let curr = store.get(key);

        if curr.is_none() && deleted {
            return Ok((None, 0, false, None));
        }
        if cas_version > 0 && curr.map(|c| c.version).unwrap_or(0) != cas_version {
            return Err(KvError::CasFailed {
                key: key.to_string(),
                reason: "version mismatch".to_string(),
            });
        }

        let had_curr = curr.is_some();
        let (curr_version, curr_deleted, curr_update) = match curr {
            Some(c) => (c.version, c.deleted, c.update_time),
            None => (0, false, None),
        };

        let mut new_updated = curr_update;
        let mut new_deleted = curr_deleted;
        if let Some(ut) = update_time {
            if new_updated.is_none_or(|prev| ut > prev) && deleted {
                new_updated = Some(ut);
                new_deleted = deleted;
            }
        }

        let (mut result, mut change) = match curr {
            Some(curr) => {
                let mut v = curr.value.clone_mergeable();
                let ch = v.merge(incoming, cas_version > 0).map_err(KvError::Other)?;
                (v, ch)
            }
            None => {
                let v = incoming.clone_mergeable();
                let ch = Some(v.clone_mergeable());
                (v, ch)
            }
        };

        let change_empty = change.as_ref().is_none_or(|c| c.merge_content().is_empty());
        if change_empty && curr_deleted == new_deleted {
            return Ok((None, 0, curr_deleted, curr_update));
        }

        let left_timeout = self.cfg.left_ingesters_timeout.as_std().unwrap_or(Duration::ZERO);
        if !left_timeout.is_zero() {
            let limit = SystemTime::now().checked_sub(left_timeout);
            let (total, removed) = result.remove_tombstones(limit);
            self.metrics.store_tombstones.with_label_values(&[key]).set(total as f64);
            self.metrics
                .store_removed_tombstones
                .with_label_values(&[key])
                .inc_by(removed as f64);
            if removed > 0 {
                self.local_state_cache.lock().remove(key);
            }
            if let Some(ref mut ch) = change {
                ch.remove_tombstones(limit);
                if ch.merge_content().is_empty() && curr_deleted == new_deleted {
                    if !had_curr {
                        return Ok((None, 0, curr_deleted, curr_update));
                    }
                    let changed_live = store.get(key).is_none_or(|c| {
                        let mut c_live = c.value.clone_mergeable();
                        c_live.remove_tombstones(None);
                        let mut r_live = result.clone_mergeable();
                        r_live.remove_tombstones(None);
                        !c_live.eq_mergeable(r_live.as_ref())
                    });
                    if changed_live {
                        let new_version = curr_version + 1;
                        store.insert(
                            key.to_string(),
                            ValueDesc {
                                value: result,
                                version: new_version,
                                codec_id: codec_id.to_string(),
                                deleted: new_deleted,
                                update_time: new_updated,
                            },
                        );
                        return Ok((None, new_version, new_deleted, new_updated));
                    }
                    // Result equals curr (no net state change); keep curr_version and
                    // return new_version = 0 (errNoChangeDetected).
                    store.insert(
                        key.to_string(),
                        ValueDesc {
                            value: result,
                            version: curr_version,
                            codec_id: codec_id.to_string(),
                            deleted: curr_deleted,
                            update_time: curr_update,
                        },
                    );
                    return Ok((None, 0, curr_deleted, curr_update));
                }
            }
        }

        if (change.is_none() || change.as_ref().is_some_and(|c| c.merge_content().is_empty()))
            && curr_deleted != new_deleted
        {
            change = Some(result.clone_mergeable());
        }

        let new_version = curr_version + 1;
        store.insert(
            key.to_string(),
            ValueDesc {
                value: result,
                version: new_version,
                codec_id: codec_id.to_string(),
                deleted: new_deleted,
                update_time: new_updated,
            },
        );
        Ok((change, new_version, new_deleted, new_updated))
    }

    pub(crate) fn merge_bytes_value_for_key(
        &self,
        key: &str,
        incoming_data: &[u8],
        codec: &dyn Codec,
        deleted: bool,
        update_time: Option<SystemTime>,
    ) -> Result<MergeValueResult, KvError> {
        let data = if incoming_data.is_empty() { EMPTY_SNAPPY_DATA } else { incoming_data };
        let mut decoded = codec
            .decode_mergeable(data)
            .map_err(|e| KvError::Other(format!("failed to decode value: {e}")))?;
        self.merge_value_for_key(key, decoded.as_mut(), 0, codec.codec_id(), deleted, update_time)
    }

    /// dskit `broadcastNewValue`.
    #[allow(clippy::too_many_arguments)]
    pub fn broadcast_new_value(
        &self,
        key: &str,
        change: &dyn Mergeable,
        version: usize,
        codec: &dyn Codec,
        locally_generated: bool,
        deleted: bool,
        update_time: Option<SystemTime>,
    ) {
        if locally_generated && self.state() != State::Running {
            warn!(key, "skipped broadcasting of locally-generated update because memberlist KV is shutting down");
            return;
        }
        let data = match codec.encode_mergeable(change) {
            Ok(d) => d,
            Err(err) => {
                error!(key, version, %err, "failed to encode change");
                self.metrics.messages_to_broadcast_dropped.inc();
                return;
            }
        };
        let kv_pair = KeyValuePair {
            key: key.to_string(),
            value: Bytes::from(data),
            codec: codec.codec_id().to_string(),
            deleted,
            update_time_millis: update_time_millis(update_time),
        };
        let pair_data = kv_pair.encode_to_vec();
        let merged_changes = change.merge_content();
        self.add_sent_message(HistoryMessage {
            id: 0,
            time: SystemTime::now(),
            size: pair_data.len(),
            pair: kv_pair,
            version,
            changes: merged_changes.clone(),
        });

        let l = pair_data.len() as f64;
        let gauge = self.metrics.messages_in_broadcast_queue_bytes.clone();
        let b = Arc::new(RingBroadcast {
            key: key.to_string(),
            content: merged_changes,
            version,
            msg: pair_data,
            finished: Box::new(move || gauge.sub(l)),
        });
        self.metrics.messages_in_broadcast_queue_bytes.add(l);
        if locally_generated {
            self.local_broadcasts.queue_broadcast(b);
        } else {
            self.gossip_broadcasts.queue_broadcast(b);
        }
    }

    /// dskit `notifyWatchers`.
    pub fn notify_watchers(&self, key: &str) {
        let interval = self.cfg.notify_interval.as_std().unwrap_or(Duration::ZERO);
        if interval.is_zero() {
            self.notify_watchers_sync(key);
            return;
        }
        self.key_notifications.lock().insert(key.to_string());
    }

    /// dskit `sendKeyNotifications`.
    pub fn send_key_notifications(&self) {
        let notifs = {
            let mut n = self.key_notifications.lock();
            if n.is_empty() {
                return;
            }
            self.metrics.pending_key_notifications.set(n.len() as f64);
            std::mem::take(&mut *n)
        };
        for key in notifs {
            self.notify_watchers_sync(&key);
        }
    }

    /// dskit `notifyWatchersSync`.
    fn notify_watchers_sync(&self, key: &str) {
        if let Some(list) = self.watchers.lock().get(key) {
            for w in list {
                w.notify.notify_one();
            }
        }
        let pwatchers = self.prefix_watchers.lock();
        for (prefix, list) in pwatchers.iter() {
            if key.starts_with(prefix.as_str()) {
                for pw in list {
                    if pw.tx.try_send(key.to_string()).is_err() {
                        self.metrics
                            .watch_prefix_dropped_notifications
                            .with_label_values(&[prefix])
                            .inc();
                        warn!(prefix, "failed to send notification to prefix watcher");
                    }
                }
            }
        }
    }

    /// dskit `cleanupObsoleteEntries`.
    pub fn cleanup_obsolete_entries(&self) {
        let timeout = self.cfg.obsolete_entries_timeout.as_std().unwrap_or(Duration::ZERO);
        let now = SystemTime::now();
        let mut removed = Vec::new();
        {
            let mut store = self.store.lock();
            store.retain(|k, v| {
                let since = now
                    .duration_since(v.update_time.unwrap_or(UNIX_EPOCH))
                    .unwrap_or(Duration::ZERO);
                if v.deleted && since > timeout {
                    removed.push(k.clone());
                    false
                } else {
                    true
                }
            });
        }
        if removed.is_empty() {
            return;
        }
        {
            let mut cache = self.local_state_cache.lock();
            for k in &removed {
                cache.remove(k);
                let _ = self.metrics.local_state_cache_hits.remove_label_values(&[k]);
                let _ = self.metrics.local_state_cache_misses.remove_label_values(&[k]);
            }
        }
        let mut workers = self.workers.lock();
        for k in &removed {
            workers.remove(k);
        }
    }

    // --- memberlist Delegate: LocalState, MergeRemoteState, NotifyMsg, GetBroadcasts ---------------

    /// dskit `LocalState`.
    pub fn local_state(&self, _join: bool) -> Vec<u8> {
        if !self.delegate_ready.load(Ordering::Acquire) {
            return Vec::new();
        }
        self.metrics.state_pulls.inc();
        let store = self.store.lock();
        let mut buf = Vec::with_capacity(self.last_local_state_size.load(Ordering::Relaxed));
        let sent = SystemTime::now();
        let cache_enabled = self.cfg.local_state_cache_enabled;
        let mut cache = self.local_state_cache.lock();
        for (key, val) in store.iter() {
            let entry = if cache_enabled {
                match cache.get(key) {
                    Some((v, data)) if *v == val.version => {
                        self.metrics.local_state_cache_hits.with_label_values(&[key]).inc();
                        Some(data.clone())
                    }
                    _ => {
                        let e = self.build_local_state_entry(key, val);
                        if let Some(ref d) = e {
                            self.metrics.local_state_cache_misses.with_label_values(&[key]).inc();
                            cache.insert(key.clone(), (val.version, d.clone()));
                        }
                        e
                    }
                }
            } else {
                self.build_local_state_entry(key, val)
            };
            let Some(data) = entry else {
                continue;
            };
            buf.extend_from_slice(&data);
            if self.cfg.message_history_buffer_bytes > 0 {
                if let Ok(pair) = KeyValuePair::decode(&data[LENGTH_PREFIX_SIZE..]) {
                    self.add_sent_message(HistoryMessage {
                        id: 0,
                        time: sent,
                        size: data.len() - LENGTH_PREFIX_SIZE,
                        pair,
                        version: val.version,
                        changes: Vec::new(),
                    });
                }
            }
        }
        self.last_local_state_size.store(buf.len(), Ordering::Relaxed);
        self.metrics.state_pulls_bytes.inc_by(buf.len() as f64);
        buf
    }

    /// dskit `buildLocalStateCacheEntry`.
    fn build_local_state_entry(&self, key: &str, val: &ValueDesc) -> Option<Vec<u8>> {
        let Some(codec) = self.get_codec(&val.codec_id) else {
            error!(codec = %val.codec_id, key, "failed to encode remote state: unknown codec for key");
            return None;
        };
        let encoded = match codec.encode_mergeable(val.value.as_ref()) {
            Ok(e) => e,
            Err(err) => {
                error!(%err, "failed to encode remote state");
                return None;
            }
        };
        let pair = KeyValuePair {
            key: key.to_string(),
            value: Bytes::from(encoded),
            codec: val.codec_id.clone(),
            deleted: val.deleted,
            update_time_millis: update_time_millis(val.update_time),
        };
        let ser = pair.encode_to_vec();
        if ser.len() > u32::MAX as usize {
            error!(key, "value too long");
            return None;
        }
        let mut data = Vec::with_capacity(LENGTH_PREFIX_SIZE + ser.len());
        data.extend_from_slice(&(ser.len() as u32).to_be_bytes());
        data.extend_from_slice(&ser);
        Some(data)
    }

    /// dskit `MergeRemoteState`.
    pub fn merge_remote_state(&self, mut data: &[u8], _join: bool) {
        if !self.delegate_ready.load(Ordering::Acquire) {
            return;
        }
        let received = SystemTime::now();
        self.metrics.state_pushes.inc();
        self.metrics.state_pushes_bytes.inc_by(data.len() as f64);
        let mut parse_err: Option<String> = None;
        while !data.is_empty() {
            if data.len() < LENGTH_PREFIX_SIZE {
                parse_err = Some(format!("not enough data left for another KV Pair: {}", data.len()));
                break;
            }
            let len = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
            data = &data[LENGTH_PREFIX_SIZE..];
            if data.len() < len {
                parse_err = Some(format!(
                    "not enough data left for next KV Pair, expected {len}, remaining {} bytes",
                    data.len()
                ));
                break;
            }
            let kv_pair = match KeyValuePair::decode(&data[..len]) {
                Ok(p) => p,
                Err(err) => {
                    parse_err = Some(format!("failed to parse KV Pair: {err}"));
                    break;
                }
            };
            data = &data[len..];
            let Some(codec) = self.get_codec(&kv_pair.codec) else {
                error!(codec = %kv_pair.codec, key = %kv_pair.key, "failed to parse remote state: unknown codec for key");
                continue;
            };
            let res = self.merge_bytes_value_for_key(
                &kv_pair.key,
                &kv_pair.value,
                codec.as_ref(),
                kv_pair.deleted,
                update_time_from_millis(kv_pair.update_time_millis),
            );
            let (change, new_ver) = match &res {
                Ok((c, v, _, _)) => (c.as_ref().map(|c| c.merge_content()).unwrap_or_default(), *v),
                Err(_) => (Vec::new(), 0),
            };
            self.add_received_message(HistoryMessage {
                id: 0,
                time: received,
                size: len,
                pair: kv_pair.clone(),
                version: new_ver,
                changes: change,
            });
            match res {
                Ok((change, new_ver, deleted, updated)) => {
                    if new_ver > 0 {
                        self.notify_watchers(&kv_pair.key);
                        if let Some(ch) = change {
                            self.broadcast_new_value(&kv_pair.key, ch.as_ref(), new_ver, codec.as_ref(), false, deleted, updated);
                        }
                    }
                }
                Err(err) => error!(key = %kv_pair.key, %err, "failed to store received value"),
            }
        }
        if let Some(err) = parse_err {
            error!(%err, "failed to parse remote state");
        }
    }

    /// dskit `NotifyMsg`.
    pub fn notify_msg(&self, msg: &[u8]) {
        if !self.delegate_ready.load(Ordering::Acquire) {
            return;
        }
        self.metrics.received_broadcasts.inc();
        self.metrics.received_broadcasts_bytes.inc_by(msg.len() as f64);
        let kv_pair = match KeyValuePair::decode(msg) {
            Ok(p) => p,
            Err(err) => {
                warn!(%err, "failed to unmarshal received KV Pair");
                self.metrics.received_broadcasts_invalid.inc();
                return;
            }
        };
        if kv_pair.key.is_empty() {
            warn!("received an invalid KV Pair (empty key)");
            self.metrics.received_broadcasts_invalid.inc();
            return;
        }
        let Some(codec) = self.get_codec(&kv_pair.codec) else {
            self.metrics.received_broadcasts_invalid.inc();
            error!(codec = %kv_pair.codec, "failed to decode received value, unknown codec");
            return;
        };
        let update = ValueUpdate {
            value: kv_pair.value.clone(),
            codec,
            message_size: msg.len(),
            deleted: kv_pair.deleted,
            update_time: update_time_from_millis(kv_pair.update_time_millis),
        };
        if !self.enqueue_key_update(&kv_pair.key, update) {
            self.metrics.received_broadcasts_dropped.inc();
            warn!(key = %kv_pair.key, "notify queue full, dropping message");
        }
    }

    /// dskit `enqueueKeyUpdate`.
    fn enqueue_key_update(&self, key: &str, update: ValueUpdate) -> bool {
        let mut workers = self.workers.lock();
        let tx = match workers.get(key) {
            Some(tx) if !tx.is_closed() => tx.clone(),
            _ => {
                let cap = self.cfg.processed_messages_queue_size.max(1) as usize;
                let (tx, rx) = mpsc::channel(cap);
                if let Some(kv) = self.self_arc() {
                    let key = key.to_string();
                    let tx2 = tx.clone();
                    tokio::spawn(async move { kv.process_value_update(rx, tx2, key).await });
                }
                workers.insert(key.to_string(), tx.clone());
                tx
            }
        };
        tx.try_send(update).is_ok()
    }

    /// dskit `processValueUpdate`.
    async fn process_value_update(
        self: Arc<Self>,
        mut rx: mpsc::Receiver<ValueUpdate>,
        tx: mpsc::Sender<ValueUpdate>,
        key: String,
    ) {
        loop {
            let update = tokio::select! {
                u = rx.recv() => match u { Some(u) => u, None => return },
                _ = self.shutdown_ctx.canceled() => return,
            };
            let res = self.merge_bytes_value_for_key(
                &key,
                &update.value,
                update.codec.as_ref(),
                update.deleted,
                update.update_time,
            );
            let (changes, version, deleted, updated) = match &res {
                Ok((c, v, d, u)) => (c.as_ref().map(|c| c.merge_content()).unwrap_or_default(), *v, *d, *u),
                Err(_) => (Vec::new(), 0, false, None),
            };
            self.add_received_message(HistoryMessage {
                id: 0,
                time: SystemTime::now(),
                size: update.message_size,
                pair: KeyValuePair {
                    key: key.clone(),
                    value: update.value.clone(),
                    codec: update.codec.codec_id().to_string(),
                    deleted,
                    update_time_millis: update_time_millis(updated),
                },
                version,
                changes,
            });
            match res {
                Err(err) => error!(key, %err, "failed to store received value"),
                Ok((change, version, deleted, updated)) => {
                    if version > 0 {
                        self.notify_watchers(&key);
                        if let Some(ch) = change {
                            self.broadcast_new_value(&key, ch.as_ref(), version, update.codec.as_ref(), false, deleted, updated);
                        }
                    }
                }
            }
            if version == 0 && !self.store.lock().contains_key(&key) {
                // dskit tryDeregisterKeyWorker.
                let mut workers = self.workers.lock();
                let same = workers.get(&key).is_some_and(|w| w.same_channel(&tx));
                if same && tx.capacity() == tx.max_capacity() {
                    workers.remove(&key);
                    return;
                }
            }
        }
    }

    /// dskit `GetBroadcasts`: locally generated updates first, then gossiped ones.
    pub fn delegate_get_broadcasts(&self, overhead: usize, limit: usize) -> Vec<Vec<u8>> {
        if !self.delegate_ready.load(Ordering::Acquire) {
            return Vec::new();
        }
        let mut msgs = self.local_broadcasts.get_broadcasts(overhead, limit);
        let mut remaining = limit as i64;
        for m in &msgs {
            remaining -= (overhead + m.len()) as i64;
        }
        if remaining > 0 {
            msgs.extend(self.gossip_broadcasts.get_broadcasts(overhead, remaining as usize));
        }
        msgs
    }
}

/// dskit `generateRandomSuffix`: `fmt.Sprintf("%2x", 4 random bytes)`.
pub(crate) fn generate_random_suffix() -> String {
    let suffix: [u8; 4] = rand::thread_rng().gen();
    suffix.iter().map(|b| format!("{b:02x}")).collect()
}

/// memberlist `ensurePort`.
fn ensure_port(s: &str, port: u16) -> String {
    if crate::dns::split_host_port(s).is_some() {
        return s.to_string();
    }
    let host = s.trim_start_matches('[').trim_end_matches(']');
    crate::dns::join_host_port(host, &port.to_string())
}

/// hashicorp/go-multierror formatting.
fn multierror_text(errs: &[String]) -> String {
    if errs.len() == 1 {
        return format!("1 error occurred:\n\t* {}\n\n", errs[0]);
    }
    let points: Vec<String> = errs.iter().map(|e| format!("* {e}")).collect();
    format!("{} errors occurred:\n\t{}\n\n", errs.len(), points.join("\n\t"))
}

fn update_time_millis(t: Option<SystemTime>) -> i64 {
    t.and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn update_time_from_millis(ms: i64) -> Option<SystemTime> {
    (ms != 0).then(|| UNIX_EPOCH + Duration::from_millis(ms.max(0) as u64))
}

/// [`KvClient`] implementation backed by [`MemberlistKv`] (dskit `memberlist.Client`).
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

    /// dskit `awaitKVRunningOrStopping`.
    async fn await_kv_running_or_stopping(&self) -> Result<(), KvError> {
        match self.kv.state() {
            State::Running | State::Stopping => Ok(()),
            State::New | State::Starting => {
                let res = self.kv.service().await_running().await;
                if self.kv.state() == State::Stopping {
                    return Ok(());
                }
                res.map_err(|e| KvError::Other(e.to_string()))
            }
            s => Err(KvError::Other(format!("unexpected state: {s}"))),
        }
    }
}

impl KvClient for MemberlistKvClient {
    fn list<'a>(&'a self, prefix: &'a str) -> BoxFuture<'a, Result<Vec<String>, KvError>> {
        Box::pin(async move {
            self.await_kv_running_or_stopping().await?;
            Ok(self.kv.list_keys(prefix))
        })
    }

    fn get<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<Box<dyn Any + Send + Sync>>, KvError>> {
        Box::pin(async move {
            self.await_kv_running_or_stopping().await?;
            Ok(self.kv.get_value(key).map(|(v, _)| v))
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), KvError>> {
        Box::pin(async move {
            self.await_kv_running_or_stopping().await?;
            self.kv.delete_key(key).await
        })
    }

    fn cas<'a>(&'a self, key: &'a str, f: CasFn) -> BoxFuture<'a, Result<(), KvError>> {
        Box::pin(async move {
            self.await_kv_running_or_stopping().await?;
            self.kv.cas_with_codec(key, self.codec.as_ref(), f).await
        })
    }

    fn watch_key<'a>(
        &'a self,
        key: &'a str,
        cancel: ServiceContext,
        mut f: Box<dyn FnMut(Box<dyn Any + Send + Sync>) -> bool + Send>,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            if self.await_kv_running_or_stopping().await.is_err() {
                return;
            }
            let notify = Arc::new(Notify::new());
            self.kv
                .watchers
                .lock()
                .entry(key.to_string())
                .or_default()
                .push(WatcherEntry {
                    notify: notify.clone(),
                });
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
                        let val = self
                            .kv
                            .get_value(key)
                            .map(|(v, _)| v)
                            .unwrap_or_else(|| Box::new(KeyDeleted));
                        if !f(val) {
                            break;
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
            if self.await_kv_running_or_stopping().await.is_err() {
                return;
            }
            let buf_size = self.kv.cfg.watch_prefix_buffer_size.max(1) as usize;
            let (tx, mut rx) = mpsc::channel(buf_size);
            let watcher_id = NEXT_PREFIX_WATCHER_ID.fetch_add(1, Ordering::Relaxed);
            self.kv
                .prefix_watchers
                .lock()
                .entry(prefix.to_string())
                .or_default()
                .push(PrefixWatcherEntry { id: watcher_id, tx });
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
#[path = "client_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "upstream_client_tests.rs"]
mod upstream_client_tests;
