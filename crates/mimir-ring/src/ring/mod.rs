//! Classic consistent hash ring.
//!
//! Ported from:
//! - github.com/grafana/dskit/ring/ring.go
//! - github.com/grafana/dskit/ring/token_range.go
//! - github.com/grafana/dskit/ring/util.go

pub mod batch;
pub mod http;
pub mod model;
pub mod replication_set;
pub mod replication_set_tracker;
pub mod replication_strategy;
pub mod ticker;
pub mod token_range;
pub mod util;

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mimir_common::services::{basic_service, ManagedBasicService, ServiceContext, ServiceError};
use mimir_config::ingester::RingConfig;
use mimir_proto::ringpb::{Desc, InstanceDesc, InstanceState};
use parking_lot::RwLock;
use prometheus::{Gauge, GaugeVec, Opts};
use thiserror::Error;
use tracing::info;

use crate::kv::memberlist::MemberlistKv;
use crate::kv::{
    build_kv_client, registerer_with_kv_name, AsRegisterer, KvClient, KvError, Registerer,
};
use crate::prng::{shuffle_shard_expected_instances_per_zone, shuffle_shard_seed, GoRand};
use crate::token_gen::search_token;

use self::model::{
    get_oldest_registered_timestamp, get_tokens_info, merge_tokens,
    read_only_instances_and_oldest_updated, system_time_to_unix_secs, CompareResult, DescExt,
    InstanceDescExt, InstanceInfo,
};
pub use self::model::ring_desc_codec;
use self::replication_strategy::{
    DefaultReplicationStrategy, Operation, ReplicationSet, ReplicationStrategy, REPORTING,
};

const MAX_ZONES_FOR_METRICS: usize = 32;

#[derive(Debug, Error)]
pub enum RingError {
    #[error("empty ring")]
    EmptyRing,
    #[error("instance not found in the ring")]
    InstanceNotFound,
    #[error("too many unhealthy instances in the ring")]
    TooManyUnhealthyInstances,
    #[error("inconsistent ring tokens information")]
    InconsistentTokensInfo,
    #[error("{0}")]
    TooFewReplicas(String),
    #[error("invalid ring config: {0}")]
    InvalidConfig(String),
    /// `dskit/ring.NewWithStoreClientAndStrategy`: `ReplicationFactor must be greater than zero: %d`.
    #[error("ReplicationFactor must be greater than zero: {0}")]
    InvalidReplicationFactor(i64),
    #[error("KV error: {0}")]
    Kv(#[from] KvError),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct SubringCacheKey {
    identifier: String,
    shard_size: usize,
    lookback_period: Duration,
}

#[derive(Clone)]
struct CachedSubringWithLookback {
    subring: Arc<Ring>,
    valid_after: i64,
    valid_before: i64,
}

#[derive(Clone, Debug)]
pub struct RingSettings {
    pub heartbeat_timeout: Duration,
    pub replication_factor: usize,
    pub zone_awareness_enabled: bool,
    pub excluded_zones: Vec<String>,
    pub subring_cache_disabled: bool,
}

impl RingSettings {
    /// Converts RingConfig to RingSettings, rejecting replication_factor <= 0
    /// identically to Go dskit/ring.NewWithStoreClientAndStrategy.
    pub fn try_from_config(cfg: &RingConfig) -> Result<Self, RingError> {
        if cfg.replication_factor <= 0 {
            return Err(RingError::InvalidReplicationFactor(cfg.replication_factor));
        }
        Ok(Self {
            heartbeat_timeout: cfg.heartbeat_timeout.as_std().unwrap_or(Duration::ZERO),
            replication_factor: cfg.replication_factor as usize,
            zone_awareness_enabled: cfg.zone_awareness_enabled,
            excluded_zones: cfg.excluded_zones.0.clone(),
            subring_cache_disabled: false,
        })
    }
}

impl From<&RingConfig> for RingSettings {
    fn from(cfg: &RingConfig) -> Self {
        Self {
            heartbeat_timeout: cfg.heartbeat_timeout.as_std().unwrap_or(Duration::ZERO),
            replication_factor: cfg.replication_factor.max(1) as usize,
            zone_awareness_enabled: cfg.zone_awareness_enabled,
            excluded_zones: cfg.excluded_zones.0.clone(),
            subring_cache_disabled: false,
        }
    }
}

/// `host, _, err := net.SplitHostPort(addr); if err != nil { host = addr }`.
fn split_host_from_addr(addr: &str) -> String {
    crate::dns::net::split_host_port(addr).map_or(addr, |(host, _)| host).to_string()
}

pub fn to_status_page_config(cfg: &RingConfig) -> http::StatusPageConfig {
    let link_template = cfg.status_page_ingester_link_template.clone();
    http::StatusPageConfig {
        hide_tokens_ui_elements: false,
        show_versions: false,
        component_names: HashMap::new(),
        instance_link: Some(Arc::new(move |ingester_id: &str, instance: &InstanceDesc| {
            let mut host = split_host_from_addr(&instance.addr);
            if host.contains(':') {
                host = format!("[{host}]");
            }
            replace_host_and_ingester(&link_template, &host, ingester_id)
        })),
    }
}

/// `strings.NewReplacer("{host}", host, "{ingester}", ingester).Replace(template)`: a single
/// left-to-right pass, so substituted values are never rescanned.
fn replace_host_and_ingester(template: &str, host: &str, ingester: &str) -> String {
    let mut out = String::with_capacity(template.len() + host.len() + ingester.len());
    let mut rest = template;
    while let Some(idx) = rest.find('{') {
        out.push_str(&rest[..idx]);
        let tail = &rest[idx..];
        if let Some(after) = tail.strip_prefix("{host}") {
            out.push_str(host);
            rest = after;
        } else if let Some(after) = tail.strip_prefix("{ingester}") {
            out.push_str(ingester);
            rest = after;
        } else {
            out.push('{');
            rest = &tail[1..];
        }
    }
    out.push_str(rest);
    out
}

#[derive(Clone, Copy, Debug)]
struct TokenLookupEntry {
    instance_idx: u32,
    zone_idx: u16,
    zone_empty: bool,
}

#[derive(Default)]
struct PreindexedTokenTable {
    instance_ids: Vec<String>,
    instances: Vec<InstanceDesc>,
    token_meta: Vec<TokenLookupEntry>,
    total_hosts_per_zone: Vec<usize>,
}

fn build_token_lookup_table(
    ring_desc: &Desc,
    ring_tokens: &[u32],
    ring_instance_by_token: &HashMap<u32, InstanceInfo>,
    ring_zones: &[String],
    instances_count_per_zone: &HashMap<String, usize>,
) -> PreindexedTokenTable {
    let mut instance_ids = Vec::with_capacity(ring_desc.ingesters.len());
    let mut instances = Vec::with_capacity(ring_desc.ingesters.len());
    let mut id_to_idx: HashMap<&str, u32> = HashMap::with_capacity(ring_desc.ingesters.len());
    for (id, inst) in &ring_desc.ingesters {
        let idx = u32::try_from(instances.len()).unwrap_or(u32::MAX);
        id_to_idx.insert(id.as_str(), idx);
        instance_ids.push(id.clone());
        instances.push(inst.clone());
    }
    let mut zone_to_idx: HashMap<&str, u16> = HashMap::with_capacity(ring_zones.len());
    let mut total_hosts_per_zone = Vec::with_capacity(ring_zones.len());
    for (zi, z) in ring_zones.iter().enumerate() {
        zone_to_idx.insert(z.as_str(), u16::try_from(zi).unwrap_or(u16::MAX));
        total_hosts_per_zone.push(instances_count_per_zone.get(z).copied().unwrap_or(0));
    }
    let mut token_meta = Vec::with_capacity(ring_tokens.len());
    for &tok in ring_tokens {
        if let Some(info) = ring_instance_by_token.get(&tok) {
            let instance_idx = id_to_idx
                .get(info.instance_id.as_str())
                .copied()
                .unwrap_or(u32::MAX);
            let zone_idx = zone_to_idx
                .get(info.zone.as_str())
                .copied()
                .unwrap_or(u16::MAX);
            token_meta.push(TokenLookupEntry {
                instance_idx,
                zone_idx,
                zone_empty: info.zone.is_empty(),
            });
        } else {
            token_meta.push(TokenLookupEntry {
                instance_idx: u32::MAX,
                zone_idx: u16::MAX,
                zone_empty: true,
            });
        }
    }
    PreindexedTokenTable {
        instance_ids,
        instances,
        token_meta,
        total_hosts_per_zone,
    }
}

struct RingInner {
    ring_desc: Desc,
    ring_tokens: Vec<u32>,
    ring_tokens_by_zone: HashMap<String, Vec<u32>>,
    oldest_registered_timestamp: i64,
    read_only_instances: Option<usize>,
    oldest_read_only_updated_timestamp: Option<i64>,
    ring_instance_by_token: Arc<HashMap<u32, InstanceInfo>>,
    token_table: PreindexedTokenTable,
    last_topology_change: SystemTime,
    ring_zones: Vec<String>,
    tracked_ring_zones: HashSet<String>,
    instances_with_tokens_count: usize,
    instances_count_per_zone: HashMap<String, usize>,
    instances_with_tokens_count_per_zone: HashMap<String, usize>,
    writable_instances_with_tokens_count: usize,
    writable_instances_with_tokens_count_per_zone: HashMap<String, usize>,
    shuffled_subring_cache: HashMap<SubringCacheKey, Arc<Ring>>,
    shuffled_subring_with_lookback_cache: HashMap<SubringCacheKey, CachedSubringWithLookback>,
}

struct RingMetrics {
    num_members: GaugeVec,
    num_zone_members: GaugeVec,
    total_tokens: Gauge,
    oldest_timestamp: GaugeVec,
}

impl RingMetrics {
    fn new(name: &str, reg: Option<&Registerer>) -> Self {
        let mut const_labels = HashMap::new();
        const_labels.insert("name".to_string(), name.to_string());

        let num_members = GaugeVec::new(
            Opts::new("ring_members", "Number of members in the ring")
                .const_labels(const_labels.clone()),
            &["state"],
        )
        .expect("valid metric");
        let num_zone_members = GaugeVec::new(
            Opts::new(
                "ring_zone_members",
                "Number of ring members for each zone/state pair",
            )
            .const_labels(const_labels.clone()),
            &["zone", "state"],
        )
        .expect("valid metric");
        let total_tokens = Gauge::with_opts(
            Opts::new("ring_tokens_total", "Number of tokens in the ring")
                .const_labels(const_labels.clone()),
        )
        .expect("valid metric");
        let oldest_timestamp = GaugeVec::new(
            Opts::new(
                "ring_oldest_member_timestamp",
                "Timestamp of the oldest member in the ring.",
            )
            .const_labels(const_labels),
            &["state"],
        )
        .expect("valid metric");

        if let Some(r) = reg {
            let _ = r.register(Box::new(num_members.clone()));
            let _ = r.register(Box::new(num_zone_members.clone()));
            let _ = r.register(Box::new(total_tokens.clone()));
            let _ = r.register(Box::new(oldest_timestamp.clone()));
        }

        Self {
            num_members,
            num_zone_members,
            total_tokens,
            oldest_timestamp,
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct GetOptions {
    pub replication_factor: usize,
}

/// Read-only ring interface matching `dskit/ring.ReadRing`.
pub trait ReadRing: Send + Sync {
    fn get(&self, key: u32, op: Operation) -> Result<ReplicationSet, RingError>;
    fn get_with_options(
        &self,
        key: u32,
        op: Operation,
        opts: GetOptions,
    ) -> Result<ReplicationSet, RingError>;
    fn get_all_healthy(&self, op: Operation) -> Result<ReplicationSet, RingError>;
    fn get_replication_set_for_operation(&self, op: Operation) -> Result<ReplicationSet, RingError>;
    fn get_subring_for_operation_states(&self, op: Operation) -> Arc<Ring>;
    fn replication_factor(&self) -> usize;
    fn instances_count(&self) -> usize;
    fn instances_with_tokens_count(&self) -> usize;
    fn get_instance_state(&self, instance_id: &str) -> Result<InstanceState, RingError>;
    fn has_instance(&self, instance_id: &str) -> bool;
    fn cleanup_shuffle_shard_cache(&self, identifier: &str);
    fn get_token_ranges_for_instance(&self, instance_id: &str) -> Result<Vec<u32>, RingError>;
    fn instances_in_zone_count(&self, zone: &str) -> usize;
    fn instances_with_tokens_in_zone_count(&self, zone: &str) -> usize;
    fn writable_instances_with_tokens_count(&self) -> usize;
    fn writable_instances_with_tokens_in_zone_count(&self, zone: &str) -> usize;
    fn zones_count(&self) -> usize;
    fn zones(&self) -> Vec<String>;
}

/// Classic consistent hash ring (`dskit/ring.Ring`).
pub struct Ring {
    name: String,
    key: String,
    settings: RingSettings,
    kv_client: Option<Arc<dyn KvClient>>,
    strategy: Arc<dyn ReplicationStrategy>,
    inner: RwLock<RingInner>,
    metrics: Option<RingMetrics>,
}

impl Ring {
    /// Creates a new [`Ring`] backed by KV store from `RingConfig` (Go `ring.New`).
    /// The KV client registers its metrics through `reg` with
    /// `kv_name="<name>-ring"` like dskit; the ring's own metrics
    /// (`ring_members`, ...) are registered through `reg` too, so they carry
    /// its prefix (Mimir: `cortex_`).
    pub fn new<R: AsRegisterer + ?Sized>(
        cfg: &RingConfig,
        name: &str,
        key: &str,
        memberlist_kv: Option<Arc<MemberlistKv>>,
        reg: Option<&R>,
    ) -> Result<Arc<Self>, RingError> {
        if cfg.replication_factor <= 0 {
            return Err(RingError::InvalidReplicationFactor(cfg.replication_factor));
        }
        let registerer = reg.map(|r| r.as_registerer());
        let kv_reg = registerer_with_kv_name(registerer.as_ref(), &format!("{name}-ring"));
        let store = build_kv_client(
            &cfg.kvstore,
            ring_desc_codec(),
            memberlist_kv,
            kv_reg.as_ref(),
        )?;
        Self::with_store_and_strategy(
            RingSettings::from(cfg),
            name,
            key,
            store,
            Arc::new(DefaultReplicationStrategy),
            registerer.as_ref().map(|r| r as &dyn AsRegisterer),
        )
    }

    pub fn with_store_and_strategy(
        settings: RingSettings,
        name: &str,
        key: &str,
        store: Arc<dyn KvClient>,
        strategy: Arc<dyn ReplicationStrategy>,
        reg: Option<&dyn AsRegisterer>,
    ) -> Result<Arc<Self>, RingError> {
        Self::with_optional_store_and_strategy(settings, name, key, Some(store), strategy, reg)
    }

    pub fn with_optional_store_and_strategy(
        settings: RingSettings,
        name: &str,
        key: &str,
        store: Option<Arc<dyn KvClient>>,
        strategy: Arc<dyn ReplicationStrategy>,
        reg: Option<&dyn AsRegisterer>,
    ) -> Result<Arc<Self>, RingError> {
        let reg = reg.map(|r| r.as_registerer());
        if settings.replication_factor == 0 {
            return Err(RingError::InvalidReplicationFactor(0));
        }
        Ok(Arc::new(Self {
            name: name.to_string(),
            key: key.to_string(),
            settings,
            kv_client: store,
            strategy,
            inner: RwLock::new(RingInner {
                ring_desc: Desc::default(),
                ring_tokens: Vec::new(),
                ring_tokens_by_zone: HashMap::new(),
                oldest_registered_timestamp: 0,
                read_only_instances: None,
                oldest_read_only_updated_timestamp: None,
                ring_instance_by_token: Arc::new(HashMap::new()),
                token_table: PreindexedTokenTable::default(),
                last_topology_change: UNIX_EPOCH,
                ring_zones: Vec::new(),
                tracked_ring_zones: HashSet::new(),
                instances_with_tokens_count: 0,
                instances_count_per_zone: HashMap::new(),
                instances_with_tokens_count_per_zone: HashMap::new(),
                writable_instances_with_tokens_count: 0,
                writable_instances_with_tokens_count_per_zone: HashMap::new(),
                shuffled_subring_cache: HashMap::new(),
                shuffled_subring_with_lookback_cache: HashMap::new(),
            }),
            metrics: Some(RingMetrics::new(name, reg.as_ref())),
        }))
    }

    /// Creates an in-memory [`Ring`] for unit tests without enforcing `replication_factor > 0`
    /// or initializing metrics, matching `newRingForTesting` in `dskit/ring/ring_test.go`.
    pub fn new_for_testing(
        settings: RingSettings,
        strategy: Arc<dyn ReplicationStrategy>,
    ) -> Arc<Self> {
        Arc::new(Self {
            name: String::new(),
            key: String::new(),
            settings,
            kv_client: None,
            strategy,
            inner: RwLock::new(RingInner {
                ring_desc: Desc::default(),
                ring_tokens: Vec::new(),
                ring_tokens_by_zone: HashMap::new(),
                oldest_registered_timestamp: 0,
                read_only_instances: None,
                oldest_read_only_updated_timestamp: None,
                ring_instance_by_token: Arc::new(HashMap::new()),
                token_table: PreindexedTokenTable::default(),
                last_topology_change: UNIX_EPOCH,
                ring_zones: Vec::new(),
                tracked_ring_zones: HashSet::new(),
                instances_with_tokens_count: 0,
                instances_count_per_zone: HashMap::new(),
                instances_with_tokens_count_per_zone: HashMap::new(),
                writable_instances_with_tokens_count: 0,
                writable_instances_with_tokens_count_per_zone: HashMap::new(),
                shuffled_subring_cache: HashMap::new(),
                shuffled_subring_with_lookback_cache: HashMap::new(),
            }),
            metrics: None,
        })
    }

    /// Creates an in-memory [`Ring`] directly from a [`Desc`] (useful for tests & static snapshots).
    pub fn from_desc(
        settings: RingSettings,
        desc: Desc,
        strategy: Arc<dyn ReplicationStrategy>,
    ) -> Result<Arc<Self>, RingError> {
        if settings.replication_factor == 0 {
            return Err(RingError::InvalidReplicationFactor(0));
        }
        let ring = Arc::new(Self {
            name: String::new(),
            key: String::new(),
            settings,
            kv_client: None,
            strategy,
            inner: RwLock::new(RingInner {
                ring_desc: Desc::default(),
                ring_tokens: Vec::new(),
                ring_tokens_by_zone: HashMap::new(),
                oldest_registered_timestamp: 0,
                read_only_instances: None,
                oldest_read_only_updated_timestamp: None,
                ring_instance_by_token: Arc::new(HashMap::new()),
                token_table: PreindexedTokenTable::default(),
                last_topology_change: UNIX_EPOCH,
                ring_zones: Vec::new(),
                tracked_ring_zones: HashSet::new(),
                instances_with_tokens_count: 0,
                instances_count_per_zone: HashMap::new(),
                instances_with_tokens_count_per_zone: HashMap::new(),
                writable_instances_with_tokens_count: 0,
                writable_instances_with_tokens_count_per_zone: HashMap::new(),
                shuffled_subring_cache: HashMap::new(),
                shuffled_subring_with_lookback_cache: HashMap::new(),
            }),
            metrics: None,
        });
        ring.update_ring_state(desc);
        Ok(ring)
    }

    /// Wraps this [`Ring`] as a [`BasicService`] that loads initial state and watches KV changes.
    pub fn into_service(self: Arc<Self>) -> Arc<ManagedBasicService> {
        let r_start = self.clone();
        let r_run = self.clone();
        basic_service(
            Some(Box::new(move |ctx| {
                let r = r_start.clone();
                Box::pin(async move {
                    if let Some(kv) = &r.kv_client {
                        // Go passes the service context to `KVClient.Get`: a KV that waits for a
                        // connection (etcd `WaitForReady`) is abandoned when the service stops.
                        let val = tokio::select! {
                            res = kv.get(&r.key) => res.map_err(|e| {
                                ServiceError::failure(format!("unable to initialise ring state: {e}"))
                            })?,
                            _ = ctx.canceled() => return Err(ServiceError::Canceled),
                        };
                        if let Some(any) = val {
                            if let Ok(desc) = any.downcast::<Desc>() {
                                r.update_ring_state(*desc);
                            }
                        } else {
                            info!("ring doesn't exist in KV store yet");
                        }
                    }
                    Ok(())
                })
            })),
            Some(Box::new(move |ctx: ServiceContext| {
                let r = r_run.clone();
                Box::pin(async move {
                    r.update_ring_metrics();
                    if let Some(kv) = &r.kv_client {
                        let r_cb = r.clone();
                        kv.watch_key(
                            &r.key,
                            ctx,
                            Box::new(move |any| {
                                if any.is::<crate::kv::KeyDeleted>() {
                                    info!("ring doesn't exist in KV store yet");
                                    return true;
                                }
                                if let Ok(desc) = any.downcast::<Desc>() {
                                    r_cb.update_ring_state(*desc);
                                }
                                true
                            }),
                        )
                        .await;
                    } else {
                        ctx.canceled().await;
                    }
                    Ok(())
                })
            })),
            None,
        )
        .with_name(format!("{} ring client", self.name))
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn kv_client(&self) -> Option<&Arc<dyn KvClient>> {
        self.kv_client.as_ref()
    }

    pub fn heartbeat_timeout(&self) -> Duration {
        self.settings.heartbeat_timeout
    }

    pub fn replication_factor(&self) -> usize {
        self.settings.replication_factor
    }

    pub fn desc_snapshot(&self) -> Desc {
        self.inner.read().ring_desc.clone()
    }

    pub fn is_healthy(&self, instance: &InstanceDesc, op: Operation, now: SystemTime) -> bool {
        instance.is_healthy(op, self.settings.heartbeat_timeout, now)
    }

    pub fn update_ring_state(&self, mut ring_desc: Desc) {
        if !self.settings.excluded_zones.is_empty() {
            let excluded: HashSet<&str> =
                self.settings.excluded_zones.iter().map(|s| s.as_str()).collect();
            ring_desc
                .ingesters
                .retain(|_, inst| !excluded.contains(inst.zone.as_str()));
        }
        ring_desc.set_instance_ids();

        let cmp = {
            let inner = self.inner.read();
            inner.ring_desc.ring_compare(&ring_desc)
        };

        if cmp == CompareResult::Equal || cmp == CompareResult::EqualButStatesAndTimestamps {
            let mut updated_metrics = false;
            let mut resolved = false;
            {
                let mut inner = self.inner.write();
                let cmp_locked = inner.ring_desc.ring_compare(&ring_desc);
                if cmp_locked == CompareResult::Equal
                    || cmp_locked == CompareResult::EqualButStatesAndTimestamps
                {
                    inner.ring_desc = ring_desc.clone();
                    if cmp_locked != CompareResult::Equal {
                        let tt = &mut inner.token_table;
                        for (id, dst) in tt.instance_ids.iter().zip(tt.instances.iter_mut()) {
                            if let Some(src) = ring_desc.ingesters.get(id) {
                                *dst = src.clone();
                            }
                        }
                    }
                    updated_metrics = cmp_locked != CompareResult::Equal;
                    resolved = true;
                }
            }
            if resolved {
                if updated_metrics {
                    self.update_ring_metrics();
                }
                return;
            }
        }

        self.set_ring_state_from_desc(ring_desc, true, true, true);
    }

    pub fn set_ring_state_from_desc(
        &self,
        ring_desc: Desc,
        update_metrics: bool,
        update_registered_ts: bool,
        update_read_only: bool,
    ) {
        let now = SystemTime::now();
        let ring_tokens = ring_desc.get_tokens();
        let ring_tokens_by_zone = ring_desc.get_tokens_by_zone();
        let ring_instance_by_token = Arc::new(get_tokens_info(&ring_desc));
        let mut ring_zones: Vec<String> = ring_tokens_by_zone.keys().cloned().collect();
        ring_zones.sort();

        let oldest_registered_timestamp = get_oldest_registered_timestamp(&ring_desc);

        let mut instances_with_tokens_count = 0usize;
        let mut instances_count_per_zone: HashMap<String, usize> = HashMap::new();
        let mut instances_with_tokens_count_per_zone: HashMap<String, usize> = HashMap::new();
        let mut writable_instances_with_tokens_count = 0usize;
        let mut writable_instances_with_tokens_count_per_zone: HashMap<String, usize> =
            HashMap::new();

        for inst in ring_desc.ingesters.values() {
            *instances_count_per_zone.entry(inst.zone.clone()).or_insert(0) += 1;
            if !inst.tokens.is_empty() {
                instances_with_tokens_count += 1;
                *instances_with_tokens_count_per_zone
                    .entry(inst.zone.clone())
                    .or_insert(0) += 1;
                if !inst.read_only {
                    writable_instances_with_tokens_count += 1;
                    *writable_instances_with_tokens_count_per_zone
                        .entry(inst.zone.clone())
                        .or_insert(0) += 1;
                }
            }
        }
        let (ro_count, ro_oldest) = read_only_instances_and_oldest_updated(&ring_desc);
        let token_table = build_token_lookup_table(
            &ring_desc,
            &ring_tokens,
            &ring_instance_by_token,
            &ring_zones,
            &instances_count_per_zone,
        );

        {
            let mut inner = self.inner.write();
            inner.ring_desc = ring_desc;
            inner.ring_tokens = ring_tokens;
            inner.ring_tokens_by_zone = ring_tokens_by_zone;
            inner.ring_instance_by_token = ring_instance_by_token;
            inner.token_table = token_table;
            inner.ring_zones = ring_zones.clone();
            for z in &ring_zones {
                if !inner.tracked_ring_zones.contains(z)
                    && inner.tracked_ring_zones.len() < MAX_ZONES_FOR_METRICS
                {
                    inner.tracked_ring_zones.insert(z.clone());
                }
            }
            inner.instances_with_tokens_count = instances_with_tokens_count;
            inner.instances_count_per_zone = instances_count_per_zone;
            inner.instances_with_tokens_count_per_zone = instances_with_tokens_count_per_zone;
            inner.writable_instances_with_tokens_count = writable_instances_with_tokens_count;
            inner.writable_instances_with_tokens_count_per_zone =
                writable_instances_with_tokens_count_per_zone;
            if update_registered_ts {
                inner.oldest_registered_timestamp = oldest_registered_timestamp;
            }
            inner.last_topology_change = now;
            if update_read_only {
                inner.read_only_instances = Some(ro_count);
                inner.oldest_read_only_updated_timestamp = Some(ro_oldest);
            }
            inner.shuffled_subring_cache.clear();
            inner.shuffled_subring_with_lookback_cache.clear();
        }

        if update_metrics {
            self.update_ring_metrics();
        }
    }

    fn update_ring_metrics(&self) {
        let Some(metrics) = &self.metrics else {
            return;
        };
        let inner = self.inner.read();
        let now = SystemTime::now();

        let states = ["Unhealthy", "ACTIVE", "LEAVING", "PENDING", "JOINING"];
        let mut num_by_state: HashMap<Cow<'static, str>, usize> =
            states.iter().map(|&s| (Cow::Borrowed(s), 0)).collect();
        let mut oldest_ts_by_state: HashMap<Cow<'static, str>, i64> =
            states.iter().map(|&s| (Cow::Borrowed(s), 0)).collect();
        let mut num_by_zone_and_state: HashMap<String, HashMap<Cow<'static, str>, usize>> =
            HashMap::new();

        if self.settings.zone_awareness_enabled {
            for zone in &inner.tracked_ring_zones {
                let m = states.iter().map(|&s| (Cow::Borrowed(s), 0)).collect();
                num_by_zone_and_state.insert(zone.clone(), m);
            }
        }

        for inst in inner.ring_desc.ingesters.values() {
            let st_label: Cow<'static, str> =
                if !inst.is_healthy(REPORTING, self.settings.heartbeat_timeout, now) {
                    Cow::Borrowed("Unhealthy")
                } else {
                    inst.state_display()
                };
            *num_by_state.entry(st_label.clone()).or_insert(0) += 1;
            let oldest = oldest_ts_by_state.entry(st_label.clone()).or_insert(0);
            if *oldest == 0 || inst.timestamp < *oldest {
                *oldest = inst.timestamp;
            }
            if self.settings.zone_awareness_enabled {
                if let Some(by_st) = num_by_zone_and_state.get_mut(&inst.zone) {
                    *by_st.entry(st_label).or_insert(0) += 1;
                }
            }
        }

        for (st, count) in num_by_state {
            metrics
                .num_members
                .with_label_values(&[st.as_ref()])
                .set(count as f64);
        }
        for (st, ts) in oldest_ts_by_state {
            metrics
                .oldest_timestamp
                .with_label_values(&[st.as_ref()])
                .set(ts as f64);
        }
        if self.settings.zone_awareness_enabled {
            for (zone, by_st) in num_by_zone_and_state {
                for (st, count) in by_st {
                    metrics
                        .num_zone_members
                        .with_label_values(&[zone.as_str(), st.as_ref()])
                        .set(count as f64);
                }
            }
        }
        metrics.total_tokens.set(inner.ring_tokens.len() as f64);
    }

    pub fn get(&self, key: u32, op: Operation) -> Result<ReplicationSet, RingError> {
        self.get_at(key, op, self.settings.replication_factor, SystemTime::now())
    }

    pub fn get_with_replication_factor(
        &self,
        key: u32,
        op: Operation,
        replication_factor: usize,
    ) -> Result<ReplicationSet, RingError> {
        self.get_at(key, op, replication_factor, SystemTime::now())
    }

    pub fn get_with_options(
        &self,
        key: u32,
        op: Operation,
        opts: GetOptions,
    ) -> Result<ReplicationSet, RingError> {
        self.get_at(key, op, opts.replication_factor, SystemTime::now())
    }

    pub fn get_at(
        &self,
        key: u32,
        op: Operation,
        mut replication_factor: usize,
        now: SystemTime,
    ) -> Result<ReplicationSet, RingError> {
        let inner = self.inner.read();
        if inner.ring_tokens.is_empty() {
            return Err(RingError::EmptyRing);
        }

        if replication_factor == 0 || replication_factor < self.settings.replication_factor {
            replication_factor = self.settings.replication_factor;
        }
        if replication_factor > self.settings.replication_factor
            && !self.strategy.supports_expanded_replication()
        {
            return Err(RingError::TooFewReplicas(format!(
                "per-call replication factor {replication_factor} cannot exceed the configured replication factor {} with this replication strategy",
                self.settings.replication_factor
            )));
        }

        let instances = Self::find_instances_for_key(
            &inner,
            self.settings.zone_awareness_enabled,
            self.settings.replication_factor,
            key,
            op,
            replication_factor,
        )?;

        let (healthy, max_failures) = self.strategy.filter(
            instances,
            op,
            replication_factor,
            self.settings.heartbeat_timeout,
            self.settings.zone_awareness_enabled,
            now,
        )?;

        Ok(ReplicationSet {
            instances: healthy,
            max_errors: max_failures,
            max_unavailable_zones: 0,
            zone_awareness_enabled: false,
            partition_id: -1,
        })
    }

    fn find_instances_for_key(
        inner: &RingInner,
        zone_awareness_enabled: bool,
        max_zones: usize,
        key: u32,
        op: Operation,
        replication_factor: usize,
    ) -> Result<Vec<InstanceDesc>, RingError> {
        let mut replica_set_size = replication_factor;
        let mut instances = Vec::with_capacity(replica_set_size + 2);
        let n_tokens = inner.ring_tokens.len();
        let start = search_token(&inner.ring_tokens, key);
        let max_instances = inner.token_table.instances.len();

        // Track distinct instance indices on the stack for typical RF <= 16 (zero HashSet alloc).
        let mut seen_inline = [0u32; 16];
        let mut seen_len = 0usize;
        let mut seen_overflow: Option<HashSet<u32>> = None;

        let num_zones = inner.ring_zones.len();
        let total_hosts_per_zone = inner.token_table.total_hosts_per_zone.as_slice();
        let mut examined_inline = [0usize; 16];
        let mut found_inline = [0usize; 16];
        let mut examined_vec = Vec::new();
        let mut found_vec = Vec::new();
        let (examined_hosts_per_zone, found_hosts_per_zone): (&mut [usize], &mut [usize]) =
            if num_zones <= 16 {
                (&mut examined_inline[..num_zones], &mut found_inline[..num_zones])
            } else {
                examined_vec.resize(num_zones, 0);
                found_vec.resize(num_zones, 0);
                (examined_vec.as_mut_slice(), found_vec.as_mut_slice())
            };
        let target_hosts_per_zone = (replication_factor / max_zones.max(1)).max(1);

        let token_meta = inner.token_table.token_meta.as_slice();
        let ring_instances = inner.token_table.instances.as_slice();
        let mut iterations = 0usize;
        let mut i = if start == n_tokens { 0 } else { start };

        while seen_len < max_instances.min(replica_set_size) && iterations < n_tokens {
            if zone_awareness_enabled
                && can_stop_looking(
                    total_hosts_per_zone,
                    found_hosts_per_zone,
                    examined_hosts_per_zone,
                    target_hosts_per_zone,
                )
            {
                break;
            }

            iterations += 1;
            let meta = token_meta[i];
            i += 1;
            if i == n_tokens {
                i = 0;
            }

            if meta.instance_idx == u32::MAX {
                return Err(RingError::InconsistentTokensInfo);
            }

            let already_seen = if let Some(ref set) = seen_overflow {
                set.contains(&meta.instance_idx)
            } else {
                seen_inline[..seen_len].contains(&meta.instance_idx)
            };
            if already_seen {
                continue;
            }

            let mut zone_index = None;
            if zone_awareness_enabled {
                if meta.zone_idx == u16::MAX {
                    return Err(RingError::InconsistentTokensInfo);
                }
                let zi = meta.zone_idx as usize;
                zone_index = Some(zi);

                if !meta.zone_empty {
                    if found_hosts_per_zone[zi] >= target_hosts_per_zone {
                        continue;
                    }
                    examined_hosts_per_zone[zi] += 1;
                }
            }

            if let Some(ref mut set) = seen_overflow {
                set.insert(meta.instance_idx);
            } else if seen_len < seen_inline.len() {
                seen_inline[seen_len] = meta.instance_idx;
            } else {
                let mut set = HashSet::with_capacity(replica_set_size + 4);
                set.extend(seen_inline);
                set.insert(meta.instance_idx);
                seen_overflow = Some(set);
            }
            seen_len += 1;

            let instance = ring_instances[meta.instance_idx as usize].clone();

            if op.should_extend_raw_state(instance.state) {
                replica_set_size += 1;
            } else if zone_awareness_enabled && !meta.zone_empty {
                if let Some(zi) = zone_index {
                    found_hosts_per_zone[zi] += 1;
                }
            }

            instances.push(instance);
        }

        Ok(instances)
    }

    pub fn get_all_healthy(&self, op: Operation) -> Result<ReplicationSet, RingError> {
        self.get_all_healthy_at(op, SystemTime::now())
    }

    pub fn get_all_healthy_at(
        &self,
        op: Operation,
        now: SystemTime,
    ) -> Result<ReplicationSet, RingError> {
        let inner = self.inner.read();
        if inner.ring_desc.ingesters.is_empty() {
            return Err(RingError::EmptyRing);
        }
        let instances: Vec<InstanceDesc> = inner
            .ring_desc
            .ingesters
            .values()
            .filter(|inst| inst.is_healthy(op, self.settings.heartbeat_timeout, now))
            .cloned()
            .collect();
        Ok(ReplicationSet {
            instances,
            max_errors: 0,
            max_unavailable_zones: 0,
            zone_awareness_enabled: false,
            partition_id: -1,
        })
    }

    pub fn get_replication_set_for_operation(
        &self,
        op: Operation,
    ) -> Result<ReplicationSet, RingError> {
        self.get_replication_set_for_operation_at(op, SystemTime::now())
    }

    pub fn get_replication_set_for_operation_at(
        &self,
        op: Operation,
        now: SystemTime,
    ) -> Result<ReplicationSet, RingError> {
        let inner = self.inner.read();
        if inner.ring_tokens.is_empty() {
            return Err(RingError::EmptyRing);
        }

        let mut healthy_instances = Vec::with_capacity(inner.ring_desc.ingesters.len());
        let mut zone_failures = HashSet::new();

        for inst in inner.ring_desc.ingesters.values() {
            if inst.is_healthy(op, self.settings.heartbeat_timeout, now) {
                healthy_instances.push(inst.clone());
            } else {
                zone_failures.insert(inst.zone.clone());
            }
        }

        let mut max_errors = 0usize;
        let mut max_unavailable_zones = 0usize;

        if self.settings.zone_awareness_enabled {
            let num_replicated_zones = inner
                .ring_zones
                .len()
                .min(self.settings.replication_factor);
            let min_success_zones = (num_replicated_zones / 2) + 1;
            max_unavailable_zones = min_success_zones.saturating_sub(1);

            if zone_failures.len() > max_unavailable_zones {
                return Err(RingError::TooManyUnhealthyInstances);
            }
            if !zone_failures.is_empty() {
                healthy_instances.retain(|inst| !zone_failures.contains(&inst.zone));
            }
            max_unavailable_zones -= zone_failures.len();
        } else {
            let mut num_required = inner
                .ring_desc
                .ingesters
                .len()
                .max(self.settings.replication_factor);
            num_required = num_required.saturating_sub(self.settings.replication_factor / 2);

            if healthy_instances.len() < num_required {
                return Err(RingError::TooManyUnhealthyInstances);
            }
            max_errors = healthy_instances.len() - num_required;
        }

        Ok(ReplicationSet {
            instances: healthy_instances,
            max_errors,
            max_unavailable_zones,
            zone_awareness_enabled: self.settings.zone_awareness_enabled,
            partition_id: -1,
        })
    }

    pub fn get_subring_for_operation_states(&self, op: Operation) -> Arc<Ring> {
        let inner = self.inner.read();
        let shard: HashMap<String, InstanceDesc> = inner
            .ring_desc
            .ingesters
            .iter()
            .filter(|(_, inst)| op.is_raw_state_healthy(inst.state))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        self.build_ring_for_shard(&inner, shard)
    }

    pub fn shuffle_shard(self: &Arc<Self>, identifier: &str, size: usize) -> Arc<Ring> {
        self.shuffle_shard_with_lookback(identifier, size, Duration::ZERO, SystemTime::now())
    }

    pub fn shuffle_shard_with_lookback(
        self: &Arc<Self>,
        identifier: &str,
        size: usize,
        lookback_period: Duration,
        now: SystemTime,
    ) -> Arc<Ring> {
        if let Some(cached) = self.get_cached_subring(identifier, size, lookback_period, now) {
            return cached;
        }

        let result = if size == 0 {
            self.filter_out_read_only_instances(lookback_period, now)
        } else {
            self.compute_shuffle_shard(identifier, size, lookback_period, now)
        };

        if !Arc::ptr_eq(&result, self) {
            self.set_cached_subring(identifier, size, lookback_period, now, result.clone());
        }
        result
    }

    fn filter_out_read_only_instances(
        self: &Arc<Self>,
        lookback_period: Duration,
        now: SystemTime,
    ) -> Arc<Ring> {
        let lookback_until = system_time_to_unix_secs(now.checked_sub(lookback_period).unwrap_or(UNIX_EPOCH));
        let inner = self.inner.read();

        if inner.read_only_instances == Some(0) {
            return self.clone();
        }
        if !lookback_period.is_zero() {
            if let Some(oldest) = inner.oldest_read_only_updated_timestamp {
                if oldest >= lookback_until {
                    return self.clone();
                }
            }
        }

        let shard: HashMap<String, InstanceDesc> = inner
            .ring_desc
            .ingesters
            .iter()
            .filter(|(_, inst)| should_include_readonly_in_shard(inst, lookback_period, lookback_until))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        self.build_ring_for_shard(&inner, shard)
    }

    fn compute_shuffle_shard(
        self: &Arc<Self>,
        identifier: &str,
        size: usize,
        lookback_period: Duration,
        now: SystemTime,
    ) -> Arc<Ring> {
        let lookback_until = system_time_to_unix_secs(now.checked_sub(lookback_period).unwrap_or(UNIX_EPOCH));
        let inner = self.inner.read();

        if !lookback_period.is_zero()
            && inner.oldest_registered_timestamp > 0
            && inner.oldest_registered_timestamp >= lookback_until
        {
            return self.clone();
        }

        let (num_per_zone, actual_zones): (usize, Vec<String>) =
            if self.settings.zone_awareness_enabled {
                (
                    shuffle_shard_expected_instances_per_zone(size, inner.ring_zones.len()),
                    inner.ring_zones.clone(),
                )
            } else {
                (size, vec![String::new()])
            };

        let mut shard: HashMap<String, InstanceDesc> =
            HashMap::with_capacity(inner.ring_desc.ingesters.len().min(size));

        for zone in actual_zones {
            let tokens: &[u32] = if self.settings.zone_awareness_enabled {
                let zc = inner.instances_count_per_zone.get(&zone).copied().unwrap_or(0);
                if num_per_zone >= zc {
                    for (id, inst) in &inner.ring_desc.ingesters {
                        if inst.zone == zone
                            && should_include_readonly_in_shard(inst, lookback_period, lookback_until)
                        {
                            shard.insert(id.clone(), inst.clone());
                        }
                    }
                    continue;
                }
                inner
                    .ring_tokens_by_zone
                    .get(&zone)
                    .map(|v| v.as_slice())
                    .unwrap_or(&[])
            } else {
                &inner.ring_tokens
            };

            if tokens.is_empty() {
                continue;
            }

            let mut rng = GoRand::new(shuffle_shard_seed(identifier, &zone));

            for _ in 0..num_per_zone {
                let start = search_token(tokens, rng.uint32());
                let mut found = false;

                for offset in 0..tokens.len() {
                    let tok = tokens[(start + offset) % tokens.len()];
                    let Some(info) = inner.ring_instance_by_token.get(&tok) else {
                        break;
                    };
                    if shard.contains_key(&info.instance_id) {
                        continue;
                    }
                    let Some(inst) = inner.ring_desc.ingesters.get(&info.instance_id) else {
                        continue;
                    };
                    if !should_include_readonly_in_shard(inst, lookback_period, lookback_until) {
                        continue;
                    }
                    shard.insert(info.instance_id.clone(), inst.clone());

                    if !lookback_period.is_zero() && inst.registered_timestamp >= lookback_until {
                        continue;
                    }
                    if !lookback_period.is_zero()
                        && (inst.read_only || inst.read_only_updated_timestamp >= lookback_until)
                    {
                        continue;
                    }
                    found = true;
                    break;
                }

                if !found {
                    break;
                }
            }
        }

        self.build_ring_for_shard(&inner, shard)
    }

    fn build_ring_for_shard(
        &self,
        parent_inner: &RingInner,
        shard: HashMap<String, InstanceDesc>,
    ) -> Arc<Ring> {
        let shard_desc = Desc { ingesters: shard };
        let shard_tokens_by_zone = shard_desc.get_tokens_by_zone();
        let shard_tokens: Vec<u32> = if shard_tokens_by_zone.len() == 1 {
            shard_tokens_by_zone
                .values()
                .next()
                .cloned()
                .unwrap_or_default()
        } else {
            let groups: Vec<Vec<u32>> = shard_tokens_by_zone.values().cloned().collect();
            merge_tokens(&groups)
        };
        let mut zones: Vec<String> = shard_tokens_by_zone.keys().cloned().collect();
        zones.sort();

        let mut instances_with_tokens_count = 0usize;
        let mut instances_count_per_zone: HashMap<String, usize> = HashMap::new();
        let mut instances_with_tokens_count_per_zone: HashMap<String, usize> = HashMap::new();
        let mut writable_instances_with_tokens_count = 0usize;
        let mut writable_instances_with_tokens_count_per_zone: HashMap<String, usize> =
            HashMap::new();

        for inst in shard_desc.ingesters.values() {
            *instances_count_per_zone.entry(inst.zone.clone()).or_insert(0) += 1;
            if !inst.tokens.is_empty() {
                instances_with_tokens_count += 1;
                *instances_with_tokens_count_per_zone
                    .entry(inst.zone.clone())
                    .or_insert(0) += 1;
                if !inst.read_only {
                    writable_instances_with_tokens_count += 1;
                    *writable_instances_with_tokens_count_per_zone
                        .entry(inst.zone.clone())
                        .or_insert(0) += 1;
                }
            }
        }

        let oldest_registered_timestamp = get_oldest_registered_timestamp(&shard_desc);
        let token_table = build_token_lookup_table(
            &shard_desc,
            &shard_tokens,
            &parent_inner.ring_instance_by_token,
            &zones,
            &instances_count_per_zone,
        );

        Arc::new(Ring {
            name: self.name.clone(),
            key: self.key.clone(),
            settings: self.settings.clone(),
            kv_client: None,
            strategy: self.strategy.clone(),
            inner: RwLock::new(RingInner {
                ring_desc: shard_desc,
                ring_tokens: shard_tokens,
                ring_tokens_by_zone: shard_tokens_by_zone,
                oldest_registered_timestamp,
                read_only_instances: None,
                oldest_read_only_updated_timestamp: None,
                ring_instance_by_token: parent_inner.ring_instance_by_token.clone(),
                token_table,
                last_topology_change: parent_inner.last_topology_change,
                ring_zones: zones,
                tracked_ring_zones: HashSet::new(),
                instances_with_tokens_count,
                instances_count_per_zone,
                instances_with_tokens_count_per_zone,
                writable_instances_with_tokens_count,
                writable_instances_with_tokens_count_per_zone,
                shuffled_subring_cache: HashMap::new(),
                shuffled_subring_with_lookback_cache: HashMap::new(),
            }),
            metrics: None,
        })
    }

    fn get_cached_subring(
        self: &Arc<Self>,
        identifier: &str,
        size: usize,
        lookback_period: Duration,
        now: SystemTime,
    ) -> Option<Arc<Ring>> {
        if self.settings.subring_cache_disabled {
            return None;
        }
        let inner = self.inner.read();
        let key = SubringCacheKey {
            identifier: identifier.to_string(),
            shard_size: size,
            lookback_period,
        };
        let cached = if lookback_period.is_zero() {
            inner.shuffled_subring_cache.get(&key).cloned()
        } else {
            let lookback_start =
                system_time_to_unix_secs(now.checked_sub(lookback_period).unwrap_or(UNIX_EPOCH));
            inner
                .shuffled_subring_with_lookback_cache
                .get(&key)
                .filter(|c| lookback_start >= c.valid_after && lookback_start <= c.valid_before)
                .map(|c| c.subring.clone())
        }?;

        if Arc::ptr_eq(&cached, self) {
            return Some(cached);
        }

        let mut c_inner = cached.inner.write();
        let names: Vec<String> = c_inner.ring_desc.ingesters.keys().cloned().collect();
        for name in names {
            if let Some(src) = inner.ring_desc.ingesters.get(&name) {
                if let Some(dst) = c_inner.ring_desc.ingesters.get_mut(&name) {
                    dst.state = src.state;
                    dst.timestamp = src.timestamp;
                }
            }
        }
        drop(c_inner);
        Some(cached)
    }

    fn set_cached_subring(
        &self,
        identifier: &str,
        size: usize,
        lookback_period: Duration,
        now: SystemTime,
        subring: Arc<Ring>,
    ) {
        if self.settings.subring_cache_disabled {
            return;
        }
        let sub_change = subring.inner.read().last_topology_change;
        if lookback_period.is_zero() {
            let mut inner = self.inner.write();
            if inner.last_topology_change == sub_change {
                inner.shuffled_subring_cache.insert(
                    SubringCacheKey {
                        identifier: identifier.to_string(),
                        shard_size: size,
                        lookback_period,
                    },
                    subring,
                );
            }
        } else {
            let lookback_start =
                system_time_to_unix_secs(now.checked_sub(lookback_period).unwrap_or(UNIX_EPOCH));
            let mut valid_before = i64::MAX;
            for inst in subring.inner.read().ring_desc.ingesters.values() {
                if inst.registered_timestamp >= lookback_start {
                    valid_before = valid_before.min(inst.registered_timestamp);
                }
                if inst.read_only_updated_timestamp >= lookback_start {
                    valid_before = valid_before.min(inst.read_only_updated_timestamp);
                }
            }
            let key = SubringCacheKey {
                identifier: identifier.to_string(),
                shard_size: size,
                lookback_period,
            };
            let mut inner = self.inner.write();
            if inner.last_topology_change == sub_change {
                let replace = inner
                    .shuffled_subring_with_lookback_cache
                    .get(&key)
                    .map(|e| e.valid_after < lookback_start)
                    .unwrap_or(true);
                if replace {
                    inner.shuffled_subring_with_lookback_cache.insert(
                        key,
                        CachedSubringWithLookback {
                            subring,
                            valid_after: lookback_start,
                            valid_before,
                        },
                    );
                }
            }
        }
    }

    pub fn cleanup_shuffle_shard_cache(&self, identifier: &str) {
        if self.settings.subring_cache_disabled {
            return;
        }
        let mut inner = self.inner.write();
        inner
            .shuffled_subring_cache
            .retain(|k, _| k.identifier != identifier);
        inner
            .shuffled_subring_with_lookback_cache
            .retain(|k, _| k.identifier != identifier);
    }

    pub fn get_instance(&self, instance_id: &str) -> Result<InstanceDesc, RingError> {
        self.inner
            .read()
            .ring_desc
            .ingesters
            .get(instance_id)
            .cloned()
            .ok_or(RingError::InstanceNotFound)
    }

    pub fn get_instance_state(&self, instance_id: &str) -> Result<InstanceState, RingError> {
        let inst = self.get_instance(instance_id)?;
        InstanceState::try_from(inst.state).map_err(|_| {
            RingError::InvalidConfig(format!("unknown instance state: {}", inst.state))
        })
    }

    pub fn has_instance(&self, instance_id: &str) -> bool {
        self.inner
            .read()
            .ring_desc
            .ingesters
            .contains_key(instance_id)
    }

    pub fn instances_count(&self) -> usize {
        self.inner.read().ring_desc.ingesters.len()
    }

    pub fn instances_with_tokens_count(&self) -> usize {
        self.inner.read().instances_with_tokens_count
    }

    pub fn instances_in_zone_count(&self, zone: &str) -> usize {
        self.inner
            .read()
            .instances_count_per_zone
            .get(zone)
            .copied()
            .unwrap_or(0)
    }

    pub fn instances_with_tokens_in_zone_count(&self, zone: &str) -> usize {
        self.inner
            .read()
            .instances_with_tokens_count_per_zone
            .get(zone)
            .copied()
            .unwrap_or(0)
    }

    pub fn writable_instances_with_tokens_count(&self) -> usize {
        self.inner.read().writable_instances_with_tokens_count
    }

    pub fn writable_instances_with_tokens_in_zone_count(&self, zone: &str) -> usize {
        self.inner
            .read()
            .writable_instances_with_tokens_count_per_zone
            .get(zone)
            .copied()
            .unwrap_or(0)
    }

    pub fn zones_count(&self) -> usize {
        self.inner.read().ring_zones.len()
    }

    pub fn zones(&self) -> Vec<String> {
        self.inner.read().ring_zones.clone()
    }

    pub fn read_only_instance_count(&self) -> usize {
        self.inner.read().read_only_instances.unwrap_or(0)
    }

    pub fn get_token_ranges_for_instance(&self, instance_id: &str) -> Result<Vec<u32>, RingError> {
        let inner = self.inner.read();
        let instance = inner
            .ring_desc
            .ingesters
            .get(instance_id)
            .ok_or(RingError::InstanceNotFound)?;
        if instance.zone.is_empty() {
            return Err(RingError::InvalidConfig("zone not set".to_string()));
        }
        let rf = self.settings.replication_factor;
        let num_zones = inner.ring_tokens_by_zone.len();
        if !self.settings.zone_awareness_enabled || rf != num_zones {
            return Err(RingError::InvalidConfig(
                "can't use ring configuration for computing token ranges".to_string(),
            ));
        }
        let subring_tokens = inner
            .ring_tokens_by_zone
            .get(&instance.zone)
            .filter(|t| !t.is_empty())
            .ok_or_else(|| RingError::InvalidConfig("no tokens for zone".to_string()))?;

        let mut ranges = Vec::with_capacity(2 * (instance.tokens.len() + 1));
        let mut range_end = 0u32;
        let first_token = subring_tokens[0];
        let first_info = inner
            .ring_instance_by_token
            .get(&first_token)
            .ok_or(RingError::InconsistentTokensInfo)?;
        if first_info.instance_id == instance_id {
            range_end = u32::MAX;
        }

        for &token in subring_tokens[1..].iter().rev() {
            let info = inner
                .ring_instance_by_token
                .get(&token)
                .ok_or(RingError::InconsistentTokensInfo)?;
            if range_end == 0 {
                if info.instance_id == instance_id {
                    range_end = token - 1;
                }
            } else if info.instance_id != instance_id {
                ranges.push(range_end);
                ranges.push(token);
                range_end = 0;
            }
        }

        if range_end == 0 {
            if first_info.instance_id == instance_id && first_token != 0 {
                ranges.push(first_token - 1);
                ranges.push(0);
            }
        } else if first_info.instance_id == instance_id {
            ranges.push(range_end);
            ranges.push(0);
        } else {
            ranges.push(range_end);
            ranges.push(first_token);
        }

        ranges.sort_unstable();
        Ok(ranges)
    }
}

impl ReadRing for Ring {
    fn get(&self, key: u32, op: Operation) -> Result<ReplicationSet, RingError> {
        self.get(key, op)
    }

    fn get_with_options(
        &self,
        key: u32,
        op: Operation,
        opts: GetOptions,
    ) -> Result<ReplicationSet, RingError> {
        self.get_with_options(key, op, opts)
    }

    fn get_all_healthy(&self, op: Operation) -> Result<ReplicationSet, RingError> {
        self.get_all_healthy(op)
    }

    fn get_replication_set_for_operation(&self, op: Operation) -> Result<ReplicationSet, RingError> {
        self.get_replication_set_for_operation(op)
    }

    fn get_subring_for_operation_states(&self, op: Operation) -> Arc<Ring> {
        self.get_subring_for_operation_states(op)
    }

    fn replication_factor(&self) -> usize {
        self.replication_factor()
    }

    fn instances_count(&self) -> usize {
        self.instances_count()
    }

    fn instances_with_tokens_count(&self) -> usize {
        self.instances_with_tokens_count()
    }

    fn get_instance_state(&self, instance_id: &str) -> Result<InstanceState, RingError> {
        self.get_instance_state(instance_id)
    }

    fn has_instance(&self, instance_id: &str) -> bool {
        self.has_instance(instance_id)
    }

    fn cleanup_shuffle_shard_cache(&self, identifier: &str) {
        self.cleanup_shuffle_shard_cache(identifier);
    }

    fn get_token_ranges_for_instance(&self, instance_id: &str) -> Result<Vec<u32>, RingError> {
        self.get_token_ranges_for_instance(instance_id)
    }

    fn instances_in_zone_count(&self, zone: &str) -> usize {
        self.instances_in_zone_count(zone)
    }

    fn instances_with_tokens_in_zone_count(&self, zone: &str) -> usize {
        self.instances_with_tokens_in_zone_count(zone)
    }

    fn writable_instances_with_tokens_count(&self) -> usize {
        self.writable_instances_with_tokens_count()
    }

    fn writable_instances_with_tokens_in_zone_count(&self, zone: &str) -> usize {
        self.writable_instances_with_tokens_in_zone_count(zone)
    }

    fn zones_count(&self) -> usize {
        self.zones_count()
    }

    fn zones(&self) -> Vec<String> {
        self.zones()
    }
}

/// Returns a sorted list of all tokens in each entry in `groups_by_name` (each assumed already sorted).
pub fn merge_token_groups(groups_by_name: &HashMap<String, Vec<u32>>) -> Vec<u32> {
    if groups_by_name.len() == 1 {
        return groups_by_name.values().next().cloned().unwrap_or_default();
    }
    let groups: Vec<Vec<u32>> = groups_by_name.values().cloned().collect();
    merge_tokens(&groups)
}

fn can_stop_looking(
    total_hosts_per_zone: &[usize],
    found_hosts_per_zone: &[usize],
    examined_hosts_per_zone: &[usize],
    target_per_zone: usize,
) -> bool {
    for i in 0..total_hosts_per_zone.len() {
        let zone_ok = found_hosts_per_zone[i] >= target_per_zone
            || examined_hosts_per_zone[i] >= total_hosts_per_zone[i];
        if !zone_ok {
            return false;
        }
    }
    true
}

fn should_include_readonly_in_shard(
    inst: &InstanceDesc,
    lookback_period: Duration,
    lookback_until: i64,
) -> bool {
    if !inst.read_only {
        return true;
    }
    if lookback_period.is_zero() {
        return false;
    }
    if inst.read_only_updated_timestamp > 0 && inst.read_only_updated_timestamp < lookback_until {
        return false;
    }
    true
}

#[cfg(test)]
mod new_ring_tests {
    use super::*;

    // dskit/ring/ring.go NewWithStoreClientAndStrategy: ReplicationFactor <= 0 is rejected.
    #[test]
    fn rejects_non_positive_replication_factor() {
        for rf in [0, -1] {
            let mut cfg = RingConfig::default();
            cfg.kvstore.store = "inmemory".to_string();
            cfg.replication_factor = rf;
            let err = match Ring::new::<prometheus::Registry>(&cfg, "ingester", "ring", None, None) {
                Ok(_) => panic!("expected error for replication factor {rf}"),
                Err(e) => e,
            };
            assert_eq!(
                err.to_string(),
                format!("ReplicationFactor must be greater than zero: {rf}")
            );

            let err2 = RingSettings::try_from_config(&cfg).unwrap_err();
            assert_eq!(
                err2.to_string(),
                format!("ReplicationFactor must be greater than zero: {rf}")
            );
        }
    }

    #[test]
    fn c07_unknown_instance_state_is_not_coerced_to_active() {
        use super::replication_strategy::{READ, WRITE, WRITE_NO_EXTEND};
        use mimir_common::services::Service;

        let now = SystemTime::now();
        let now_s = system_time_to_unix_secs(now);
        let mut desc = Desc::default();
        desc.ingesters.insert(
            "unknown-5".to_string(),
            InstanceDesc {
                id: "unknown-5".to_string(),
                addr: "10.0.0.5:9095".to_string(),
                state: 5,
                timestamp: now_s,
                tokens: vec![100, 200],
                ..Default::default()
            },
        );
        desc.ingesters.insert(
            "unknown-neg".to_string(),
            InstanceDesc {
                id: "unknown-neg".to_string(),
                addr: "10.0.0.6:9095".to_string(),
                state: -1,
                timestamp: now_s,
                tokens: vec![300],
                ..Default::default()
            },
        );

        let inst5 = &desc.ingesters["unknown-5"];
        assert!(!inst5.is_healthy(WRITE, Duration::from_secs(60), now));
        assert!(!inst5.is_healthy(WRITE_NO_EXTEND, Duration::from_secs(60), now));
        assert!(!inst5.is_healthy(READ, Duration::from_secs(60), now));
        assert!(inst5.is_healthy(REPORTING, Duration::from_secs(60), now));
        let ready_err = inst5.is_ready(now, Duration::from_secs(60)).unwrap_err();
        assert!(ready_err.contains("in state 5"), "got: {ready_err}");

        let inst_neg = &desc.ingesters["unknown-neg"];
        assert!(!inst_neg.is_healthy(REPORTING, Duration::from_secs(60), now));
        let ready_err_neg = inst_neg.is_ready(now, Duration::from_secs(60)).unwrap_err();
        assert!(ready_err_neg.contains("in state -1"), "got: {ready_err_neg}");

        let ring = Ring::from_desc(
            RingSettings {
                heartbeat_timeout: Duration::from_secs(60),
                replication_factor: 1,
                zone_awareness_enabled: false,
                excluded_zones: Vec::new(),
                subring_cache_disabled: false,
            },
            desc,
            Arc::new(DefaultReplicationStrategy),
        )
        .unwrap();
        assert!(ring.get_subring_for_operation_states(WRITE).instances_count() == 0);
        assert!(ring.get_subring_for_operation_states(READ).instances_count() == 0);
        assert!(ring.get(150, WRITE).is_err());
        assert!(ring.get_instance_state("unknown-5").is_err());

        // C-12: Ring::into_service name includes ring name prefix.
        let kv = Arc::new(crate::kv::inmemory::InMemoryKv::new(ring_desc_codec()));
        let named_ring = Ring::with_store_and_strategy(
            RingSettings {
                heartbeat_timeout: Duration::from_secs(60),
                replication_factor: 1,
                zone_awareness_enabled: false,
                excluded_zones: Vec::new(),
                subring_cache_disabled: false,
            },
            "ingester",
            "ring",
            kv,
            Arc::new(DefaultReplicationStrategy),
            None,
        )
        .unwrap();
        assert_eq!(named_ring.into_service().service_name(), "ingester ring client");
    }
}
