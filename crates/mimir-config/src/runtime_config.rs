//! Runtime configuration loader, hot reloader, and per-tenant limit overrides.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/mimir/runtime_config.go
//! - github.com/grafana/mimir/pkg/util/validation/limits.go (`TenantLimits`, `Overrides`)
//! - github.com/grafana/dskit/runtimeconfig/manager.go
//! - github.com/grafana/dskit/runtimeconfig/config_source.go
//! - github.com/grafana/dskit/runtimeconfig/provider.go (see `provider` submodule)

mod merge;
mod provider;

pub use provider::{
    is_url, sanitize_url_for_metrics, RuntimeConfigHttpObserver, CLUSTER_VALIDATION_LABEL_HEADER,
    RUNTIME_CONFIG_USER_AGENT,
};

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_yaml::{Deserializer as YamlDeserializer, Mapping, Value};

use crate::error::ConfigError;
use crate::ingester::InstanceLimits;
use crate::limits::LimitsConfig;
use crate::server::RuntimeConfigConfig;
use crate::sha256::{sha256, sha256_hex};

use self::provider::{HttpFetcher, Provider};

/// `kv.MultiRuntimeConfig` from `dskit/kv/multi.go`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MultiRuntimeConfig {
    pub primary: String,
    pub mirror_enabled: Option<bool>,
}

/// Top-level `runtime_config` YAML structure (`pkg/mimir/runtime_config.go:runtimeConfigValues`).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RuntimeConfigValues {
    pub overrides: BTreeMap<String, Arc<LimitsConfig>>,
    pub multi_kv_config: MultiRuntimeConfig,
    pub ingester_limits: Option<InstanceLimits>,
    pub distributor_limits: Option<DistributorInstanceLimits>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DistributorInstanceLimits {
    pub max_ingestion_rate: f64,
    pub max_inflight_push_requests: i64,
    pub max_inflight_push_requests_bytes: i64,
}

impl Default for DistributorInstanceLimits {
    fn default() -> Self {
        Self {
            max_ingestion_rate: 0.0,
            max_inflight_push_requests: 2000,
            max_inflight_push_requests_bytes: 0,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum FailurePolicy {
    Strict,
    OptionalOnStartup,
    OptionalKeepLastValueOnFailure,
}

/// `configSource` from `dskit/runtimeconfig/manager.go`.
struct ConfigSource {
    path: String,
    policy: FailurePolicy,
    /// How this source identifies itself in metrics (`assignSourceIDs`).
    source_id: String,
    provider: Provider,
    /// Bytes last applied from this source (only kept for keep-last-value sources).
    last_value: Option<Vec<u8>>,
    /// Digest of the bytes this source contributed to the last applied merge; `None` when it
    /// contributed nothing.
    last_digest: Option<[u8; 32]>,
}

/// Mimir's `-runtime-config.loader` (`pkg/mimir/runtime_config.go`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LoaderKind {
    /// `Config.Loader`: the merged map is marshalled to YAML and decoded strictly.
    Yaml,
    /// `Config.MapLoader`: the merged map is decoded directly (mapstructure).
    Map,
}

fn parse_config_source(entry: &str) -> Result<ConfigSource, ConfigError> {
    let mut path = entry;
    let mut policies = Vec::new();

    loop {
        if let Some(rest) = path.strip_suffix(";optional-on-startup") {
            if rest.is_empty() {
                return Err(ConfigError::RuntimeConfig(format!(
                    "runtime config source {entry:?} has no path"
                )));
            }
            policies.push(FailurePolicy::OptionalOnStartup);
            path = rest;
        } else if let Some(rest) = path.strip_suffix(";optional-keep-last-value-on-failure") {
            if rest.is_empty() {
                return Err(ConfigError::RuntimeConfig(format!(
                    "runtime config source {entry:?} has no path"
                )));
            }
            policies.push(FailurePolicy::OptionalKeepLastValueOnFailure);
            path = rest;
        } else {
            break;
        }
    }

    if policies.len() > 1 {
        return Err(ConfigError::RuntimeConfig(format!(
            "runtime config source {entry:?} has more than one parameter, specify only one of \"optional-on-startup\" and \"optional-keep-last-value-on-failure\""
        )));
    }

    Ok(ConfigSource {
        path: path.to_string(),
        policy: policies.into_iter().next().unwrap_or(FailurePolicy::Strict),
        source_id: String::new(),
        provider: Provider::File,
        last_value: None,
        last_digest: None,
    })
}

/// `assignSourceIDs` from `dskit/runtimeconfig/config_source.go`.
fn assign_source_ids(sources: &mut [ConfigSource]) -> Result<(), ConfigError> {
    let mut occurrences: HashMap<String, usize> = HashMap::new();
    for src in sources.iter_mut() {
        src.source_id = if is_url(&src.path) {
            sanitize_url_for_metrics(&src.path)
        } else {
            src.path.clone()
        };
        *occurrences.entry(src.source_id.clone()).or_default() += 1;
    }
    let mut taken: HashMap<String, usize> = HashMap::new();
    for i in 0..sources.len() {
        if occurrences[&sources[i].source_id] > 1 {
            sources[i].source_id = format!("{}#{i}", sources[i].source_id);
        }
        if let Some(&j) = taken.get(&sources[i].source_id) {
            return Err(ConfigError::RuntimeConfig(format!(
                "runtime config sources {:?} and {:?} both report as {:?} in metrics, rename one of them",
                sources[j].path, sources[i].path, sources[i].source_id
            )));
        }
        taken.insert(sources[i].source_id.clone(), i);
    }
    Ok(())
}

/// Deep-merges `src` YAML mapping onto `dst` (matching `dskit/runtimeconfig` left-to-right merge).
fn merge_yaml_values(dst: &mut Value, src: Value) {
    match (dst, src) {
        (Value::Mapping(dst_map), Value::Mapping(src_map)) => {
            for (k, v) in src_map {
                match dst_map.get_mut(&k) {
                    Some(existing) if existing.is_mapping() && v.is_mapping() => {
                        merge_yaml_values(existing, v);
                    }
                    _ => {
                        dst_map.insert(k, v);
                    }
                }
            }
        }
        (dst_slot, src_val) => *dst_slot = src_val,
    }
}

/// Parses a single runtime-config YAML document (enforcing single-document & strict known fields)
/// and overlays each tenant's overrides onto `default_limits`.
pub fn parse_runtime_config_yaml(
    bytes: &[u8],
    default_limits: &LimitsConfig,
    default_ingester_limits: &InstanceLimits,
) -> Result<RuntimeConfigValues, ConfigError> {
    let mut docs = Vec::new();
    for doc in YamlDeserializer::from_slice(bytes) {
        let mut val = Value::deserialize(doc)
            .map_err(|e| ConfigError::RuntimeConfig(format!("YAML parse error: {e}")))?;
        val.apply_merge()
            .map_err(|e| ConfigError::RuntimeConfig(format!("YAML merge error: {e}")))?;
        docs.push(val);
    }

    if docs.len() > 1 {
        return Err(ConfigError::RuntimeConfig(
            "the provided runtime configuration contains multiple documents".to_string(),
        ));
    }

    let Some(root) = docs.into_iter().next() else {
        return Ok(RuntimeConfigValues::default());
    };

    if root.is_null() {
        return Ok(RuntimeConfigValues::default());
    }

    parse_runtime_config_value(root, default_limits, default_ingester_limits)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRuntimeConfigCheck {
    #[serde(default)]
    overrides: BTreeMap<String, LimitsConfig>,
    #[serde(default)]
    #[allow(dead_code)]
    multi_kv_config: MultiRuntimeConfig,
    #[serde(default)]
    ingester_limits: Option<Value>,
    #[serde(default)]
    distributor_limits: Option<Value>,
}

/// Rewrites scalar keys of the `overrides` mapping to strings, as yaml.v3 does when decoding
/// into `map[string]*validation.Limits` (e.g. `overrides: {12345: ...}` -> tenant `"12345"`).
fn stringify_override_keys(mut root: Value) -> Value {
    if let Some(Value::Mapping(overrides)) = root
        .as_mapping_mut()
        .and_then(|m| m.get_mut(Value::String("overrides".to_string())))
    {
        if !overrides.keys().all(|k| matches!(k, Value::String(_))) {
            let taken = std::mem::take(overrides);
            for (k, v) in taken {
                let key = merge::scalar_key_to_string(&k).map(Value::String).unwrap_or(k);
                overrides.insert(key, v);
            }
        }
    }
    root
}

fn parse_runtime_config_value(
    root: Value,
    default_limits: &LimitsConfig,
    default_ingester_limits: &InstanceLimits,
) -> Result<RuntimeConfigValues, ConfigError> {
    let root = stringify_override_keys(root);
    let root_map = root.as_mapping().ok_or_else(|| {
        ConfigError::RuntimeConfig("expected YAML mapping at runtime config root".to_string())
    })?;

    // Validate all keys strictly first.
    let check: RawRuntimeConfigCheck = serde_yaml::from_value(root.clone())
        .map_err(|e| ConfigError::RuntimeConfig(format!("invalid runtime config: {e}")))?;
    let _ = &check.overrides;

    let mut out = RuntimeConfigValues::default();

    if let Some(multi_val) = root_map.get(Value::String("multi_kv_config".to_string())) {
        if !multi_val.is_null() {
            out.multi_kv_config = serde_yaml::from_value(multi_val.clone())
                .map_err(|e| ConfigError::RuntimeConfig(e.to_string()))?;
        }
    }

    if let Some(ing_val) = check.ingester_limits {
        if !ing_val.is_null() {
            let mut merged = serde_yaml::to_value(default_ingester_limits)
                .map_err(|e| ConfigError::RuntimeConfig(e.to_string()))?;
            merge_yaml_values(&mut merged, ing_val);
            let parsed: InstanceLimits = serde_yaml::from_value(merged)
                .map_err(|e| ConfigError::RuntimeConfig(format!("ingester_limits: {e}")))?;
            out.ingester_limits = Some(parsed);
        }
    }

    if let Some(dist_val) = check.distributor_limits {
        if !dist_val.is_null() {
            let parsed: DistributorInstanceLimits = serde_yaml::from_value(dist_val)
                .map_err(|e| ConfigError::RuntimeConfig(format!("distributor_limits: {e}")))?;
            out.distributor_limits = Some(parsed);
        }
    }

    if let Some(overrides_val) = root_map.get(Value::String("overrides".to_string())) {
        if !overrides_val.is_null() {
            let overrides_map = overrides_val.as_mapping().ok_or_else(|| {
                ConfigError::RuntimeConfig("overrides must be a YAML mapping".to_string())
            })?;
            // Tenant overrides start from the real default limits (Go copies the struct), so
            // secrets must not be masked in this internal round trip.
            let base_limits_val =
                crate::secret::with_secrets_revealed(|| serde_yaml::to_value(default_limits))
                    .map_err(|e| ConfigError::RuntimeConfig(e.to_string()))?;

            for (tenant_k, tenant_v) in overrides_map {
                // yaml.v3 decodes any scalar key (e.g. an unquoted numeric tenant ID) into the
                // `map[string]*Limits` key as its string form.
                let tenant_id = merge::scalar_key_to_string(tenant_k).ok_or_else(|| {
                    ConfigError::RuntimeConfig("tenant ID key must be a string".to_string())
                })?;
                let tenant_id = tenant_id.as_str();

                let mut merged = base_limits_val.clone();
                if let Value::Mapping(ref tenant_map) = tenant_v {
                    // For `active_series_custom_trackers` and `active_series_additional_custom_trackers`,
                    // explicit empty `{}` replaces the base map instead of merging keys.
                    if let (Some(merged_map), Some(override_map)) =
                        (merged.as_mapping_mut(), Some(tenant_map))
                    {
                        for (k, v) in override_map {
                            let key_str = k.as_str().unwrap_or_default();
                            if key_str == "active_series_custom_trackers"
                                || key_str == "active_series_additional_custom_trackers"
                                || key_str == "cost_attribution_trackers"
                                || key_str == "additional_cost_attribution_trackers"
                            {
                                merged_map.insert(k.clone(), v.clone());
                            } else {
                                match merged_map.get_mut(k) {
                                    Some(existing)
                                        if existing.is_mapping() && v.is_mapping() =>
                                    {
                                        merge_yaml_values(existing, v.clone());
                                    }
                                    _ => {
                                        merged_map.insert(k.clone(), v.clone());
                                    }
                                }
                            }
                        }
                    }
                } else if !tenant_v.is_null() {
                    return Err(ConfigError::RuntimeConfig(format!(
                        "tenant {tenant_id:?} override must be a YAML mapping"
                    )));
                }

                let mut limits: LimitsConfig = serde_yaml::from_value(merged).map_err(|e| {
                    ConfigError::RuntimeConfig(format!("tenant {tenant_id:?}: {e}"))
                })?;
                limits.validate().map_err(|e| {
                    ConfigError::RuntimeConfig(format!("tenant {tenant_id:?}: {e}"))
                })?;

                out.overrides
                    .insert(tenant_id.to_string(), Arc::new(limits));
            }
        }
    }

    Ok(out)
}

/// Manages loading and periodic hot-reloading of `runtime_config.file` sources (local files and
/// HTTP/HTTPS URLs), like `dskit/runtimeconfig.Manager`.
pub struct RuntimeConfigManager {
    cfg: RuntimeConfigConfig,
    loader: LoaderKind,
    default_limits: Arc<LimitsConfig>,
    default_ingester_limits: InstanceLimits,
    sources: RwLock<Vec<ConfigSource>>,
    current: RwLock<Arc<RuntimeConfigValues>>,
    last_hash: RwLock<Option<String>>,
    listeners: parking_lot::Mutex<Vec<tokio::sync::mpsc::Sender<Arc<RuntimeConfigValues>>>>,
    http_observer: Arc<RwLock<Option<Arc<dyn RuntimeConfigHttpObserver>>>>,
    stop_flag: Arc<AtomicBool>,
    reloader: Option<JoinHandle<()>>,
}

impl RuntimeConfigManager {
    /// Creates a new `RuntimeConfigManager` and performs the initial load if `cfg.file` is non-empty.
    pub fn new(
        cfg: RuntimeConfigConfig,
        default_limits: LimitsConfig,
        default_ingester_limits: InstanceLimits,
    ) -> Result<Arc<Self>, ConfigError> {
        // pkg/mimir/runtime_config.go:NewRuntimeManager.
        let loader = match cfg.loader.as_str() {
            "map" => LoaderKind::Map,
            "yaml" | "" => LoaderKind::Yaml,
            other => {
                return Err(ConfigError::RuntimeConfig(format!(
                    "invalid runtime config loader {other:?}: supported values are \"map\" and \"yaml\""
                )))
            }
        };

        let mut sources = Vec::with_capacity(cfg.file.0.len());
        for entry in &cfg.file.0 {
            if !entry.is_empty() {
                sources.push(parse_config_source(entry)?);
            }
        }
        assign_source_ids(&mut sources)?;

        // dskit builds one HTTP client shared by all URL sources, only if there is one.
        let http_observer: Arc<RwLock<Option<Arc<dyn RuntimeConfigHttpObserver>>>> =
            Arc::new(RwLock::new(None));
        let mut http: Option<Arc<HttpFetcher>> = None;
        for src in &mut sources {
            if is_url(&src.path) {
                if http.is_none() {
                    http = Some(HttpFetcher::new(&cfg, Arc::clone(&http_observer))?);
                }
                src.provider = Provider::Http(Arc::clone(http.as_ref().expect("set above")));
            }
        }

        let mgr = Arc::new(Self {
            cfg,
            loader,
            default_limits: Arc::new(default_limits),
            default_ingester_limits,
            sources: RwLock::new(sources),
            current: RwLock::new(Arc::new(RuntimeConfigValues::default())),
            last_hash: RwLock::new(None),
            listeners: parking_lot::Mutex::new(Vec::new()),
            http_observer,
            stop_flag: Arc::new(AtomicBool::new(false)),
            reloader: None,
        });

        mgr.load_internal(true).map_err(|e| match e {
            ConfigError::RuntimeConfig(msg) => {
                ConfigError::RuntimeConfig(format!("failed to load runtime config: {msg}"))
            }
            other => other,
        })?;
        Ok(mgr)
    }

    /// Starts a background reloader thread polling every `runtime_config.period`.
    pub fn start_reloader(self: &Arc<Self>) -> JoinHandle<()> {
        let mgr = Arc::clone(self);
        let period = self
            .cfg
            .period
            .as_std()
            .filter(|d| !d.is_zero())
            .unwrap_or_else(|| Duration::from_secs(10));

        thread::spawn(move || {
            while !mgr.stop_flag.load(Ordering::Relaxed) {
                thread::sleep(period);
                if mgr.stop_flag.load(Ordering::Relaxed) {
                    break;
                }
                let _ = mgr.reload();
            }
        })
    }

    /// Signals any background reloader to stop and closes all listener channels
    /// (`Manager.stopping`).
    pub fn stop(&self) {
        self.stop_flag.store(true, Ordering::Relaxed);
        self.listeners.lock().clear();
    }

    /// Reloads runtime configuration sources immediately.
    pub fn reload(&self) -> Result<(), ConfigError> {
        self.load_internal(false)
    }

    /// Returns the currently active `RuntimeConfigValues`.
    pub fn get_config(&self) -> Arc<RuntimeConfigValues> {
        Arc::clone(&self.current.read())
    }

    /// Returns the hex SHA-256 of the currently active runtime config, if any was loaded.
    ///
    /// With `loader: map` this is exactly dskit's value (hash over every contributing source's
    /// name length, name and digest). With `loader: yaml` dskit hashes Go's `yaml.Marshal` of the
    /// merged map; here the merged map is serialized by `serde_yaml`, so the value differs from
    /// Go's although it changes exactly when the merged config changes.
    pub fn config_hash(&self) -> Option<String> {
        self.last_hash.read().clone()
    }

    /// Returns the current `ingester_limits` override, if configured.
    pub fn ingester_limits(&self) -> Option<InstanceLimits> {
        self.current.read().ingester_limits.clone()
    }

    /// Returns the current `multi_kv_config`.
    pub fn multi_kv_config(&self) -> MultiRuntimeConfig {
        self.current.read().multi_kv_config.clone()
    }

    /// `Manager.CreateListenerChannel`: returns a channel receiving every newly applied config.
    /// If the channel buffer is full the update is discarded for that listener; the channel is
    /// closed when the manager stops. Dropping the receiver unsubscribes.
    pub fn create_listener_channel(
        &self,
        buffer: usize,
    ) -> tokio::sync::mpsc::Receiver<Arc<RuntimeConfigValues>> {
        let (tx, rx) = tokio::sync::mpsc::channel(buffer.max(1));
        self.listeners.lock().push(tx);
        rx
    }

    /// Installs the observer notified about runtime-config HTTP requests (request duration per
    /// URL/status and cluster-validation rejections).
    pub fn set_http_observer(&self, observer: Arc<dyn RuntimeConfigHttpObserver>) {
        *self.http_observer.write() = Some(observer);
    }

    /// Builds an `Overrides` handle backed by this manager.
    pub fn overrides(self: &Arc<Self>) -> Overrides {
        Overrides::with_manager(Arc::clone(&self.default_limits), Arc::clone(self))
    }

    /// Replaces in-memory runtime config values (useful for tests).
    pub fn set_values_for_tests(&self, values: RuntimeConfigValues) {
        *self.current.write() = Arc::new(values);
    }

    /// `Manager.callListeners`: non-blocking send; full or closed channels are skipped
    /// (closed ones are dropped).
    fn call_listeners(&self, values: &Arc<RuntimeConfigValues>) {
        self.listeners.lock().retain(|tx| {
            !matches!(
                tx.try_send(Arc::clone(values)),
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_))
            )
        });
    }

    /// `Manager.loadConfig`.
    fn load_internal(&self, initial: bool) -> Result<(), ConfigError> {
        let mut sources = self.sources.write();
        if sources.is_empty() {
            return Ok(());
        }

        struct Loaded {
            raw: Option<Vec<u8>>,
            read_ok: bool,
            digest: Option<[u8; 32]>,
        }
        let mut loaded = Vec::with_capacity(sources.len());

        for src in sources.iter() {
            match src.provider.read(&src.path, &src.source_id) {
                Ok(buf) => {
                    let digest = sha256(&buf);
                    loaded.push(Loaded {
                        raw: Some(buf),
                        read_ok: true,
                        digest: Some(digest),
                    });
                }
                Err(err) => {
                    let err = format!("read {:?}: {err}", src.path);
                    let tolerate = match src.policy {
                        FailurePolicy::Strict => false,
                        FailurePolicy::OptionalOnStartup => initial,
                        FailurePolicy::OptionalKeepLastValueOnFailure => true,
                    };
                    if !tolerate {
                        return Err(ConfigError::RuntimeConfig(err));
                    }
                    match (&src.policy, &src.last_value) {
                        (FailurePolicy::OptionalKeepLastValueOnFailure, Some(prev)) => {
                            loaded.push(Loaded {
                                raw: Some(prev.clone()),
                                read_ok: false,
                                digest: Some(sha256(prev)),
                            })
                        }
                        _ => loaded.push(Loaded {
                            raw: None,
                            read_ok: false,
                            digest: None,
                        }),
                    }
                }
            }
        }

        // Skip the rebuild when nothing changed, but never on the initial load.
        if !initial
            && sources
                .iter()
                .zip(&loaded)
                .all(|(src, l)| src.last_digest == l.digest)
        {
            return Ok(());
        }

        let mut merged = Mapping::new();
        for (src, l) in sources.iter().zip(&loaded) {
            let Some(raw) = &l.raw else { continue };
            let file = merge::unmarshal_maybe_gzipped(&src.path, raw).map_err(|e| {
                ConfigError::RuntimeConfig(format!("unmarshal {:?}: {e}", src.path))
            })?;
            merged = merge::merge_config_maps(&merged, &file, "").map_err(|e| {
                ConfigError::RuntimeConfig(format!(
                    "can't merge {:?} on top of the previous providers: {e}",
                    src.path
                ))
            })?;
        }

        let (values, hash) = match self.loader {
            LoaderKind::Map => {
                // No merged bytes to hash: hash name length, name and digest of every source
                // that contributed.
                let mut buf = Vec::new();
                for (src, l) in sources.iter().zip(&loaded) {
                    let Some(digest) = l.digest else { continue };
                    buf.extend_from_slice(&(src.path.len() as u64).to_be_bytes());
                    buf.extend_from_slice(src.path.as_bytes());
                    buf.extend_from_slice(&digest);
                }
                let hash = sha256_hex(&buf);
                let values = self
                    .decode_map(merged)
                    .map_err(|e| ConfigError::RuntimeConfig(format!("load file: {e}")))?;
                (values, hash)
            }
            LoaderKind::Yaml => {
                let root = Value::Mapping(merged);
                let marshalled = serde_yaml::to_string(&root)
                    .map_err(|e| ConfigError::RuntimeConfig(format!("marshal file: {e}")))?;
                let hash = sha256_hex(marshalled.as_bytes());
                let values = parse_runtime_config_value(
                    root,
                    &self.default_limits,
                    &self.default_ingester_limits,
                )
                .map_err(|e| ConfigError::RuntimeConfig(format!("load file: {}", strip_rc(e))))?;
                (values, hash)
            }
        };

        let values = Arc::new(values);
        *self.current.write() = Arc::clone(&values);
        self.call_listeners(&values);
        *self.last_hash.write() = Some(hash);

        // Preserve per-source merge state for the next load.
        for (src, l) in sources.iter_mut().zip(loaded) {
            src.last_digest = l.digest;
            if l.read_ok && src.policy == FailurePolicy::OptionalKeepLastValueOnFailure {
                src.last_value = l.raw;
            }
        }
        Ok(())
    }

    /// `runtimeConfigLoader.loadFromMap`: decodes the merged map directly. The decoding of each
    /// field is shared with the YAML loader (upstream keeps both loaders equivalent, see
    /// `runtime_config_mapstructure_equivalence_test.go`); what differs is that there is no YAML
    /// round trip, so mapstructure sees `overrides` keys with their original types and rejects
    /// non-string tenant IDs.
    fn decode_map(&self, merged: Mapping) -> Result<RuntimeConfigValues, String> {
        if let Some(Value::Mapping(overrides)) = merged.get("overrides") {
            if let Some(bad) = overrides.keys().find(|k| !matches!(k, Value::String(_))) {
                return Err(format!(
                    "decoding failed due to the following error(s):\n\n'overrides[<interface {{}} Value>]' expected type 'string', got unconvertible type '{}'",
                    merge::go_type_name(Some(bad))
                ));
            }
        }
        parse_runtime_config_value(
            Value::Mapping(merged),
            &self.default_limits,
            &self.default_ingester_limits,
        )
        .map_err(strip_rc)
    }
}

fn strip_rc(e: ConfigError) -> String {
    match e {
        ConfigError::RuntimeConfig(msg) => msg,
        other => other.to_string(),
    }
}

impl Drop for RuntimeConfigManager {
    fn drop(&mut self) {
        self.stop_flag.store(true, Ordering::Relaxed);
        if let Some(handle) = self.reloader.take() {
            let _ = handle.join();
        }
    }
}

type StaticOverridesMap = Arc<RwLock<HashMap<String, Arc<LimitsConfig>>>>;

/// Fast hot-path per-tenant limits resolver (`pkg/util/validation/limits.go:Overrides`).
///
/// Uses `Arc<LimitsConfig>` reads under `parking_lot::RwLock` (via `RuntimeConfigManager` or
/// static map) so the hot `Push` path performs zero heap allocations per lookup.
#[derive(Clone)]
pub struct Overrides {
    default_limits: Arc<RwLock<Arc<LimitsConfig>>>,
    manager: Option<Arc<RuntimeConfigManager>>,
    static_overrides: StaticOverridesMap,
}

impl Overrides {
    /// Creates an `Overrides` with only global defaults (no runtime overrides).
    pub fn new(default_limits: LimitsConfig) -> Self {
        Self {
            default_limits: Arc::new(RwLock::new(Arc::new(default_limits))),
            manager: None,
            static_overrides: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Creates an `Overrides` backed by a `RuntimeConfigManager`.
    pub fn with_manager(
        default_limits: Arc<LimitsConfig>,
        manager: Arc<RuntimeConfigManager>,
    ) -> Self {
        Self {
            default_limits: Arc::new(RwLock::new(default_limits)),
            manager: Some(manager),
            static_overrides: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Creates an `Overrides` with an in-memory map of per-tenant overrides (useful in tests).
    pub fn with_static_map(
        default_limits: LimitsConfig,
        overrides: HashMap<String, LimitsConfig>,
    ) -> Self {
        let map = overrides
            .into_iter()
            .map(|(k, v)| (k, Arc::new(v)))
            .collect();
        Self {
            default_limits: Arc::new(RwLock::new(Arc::new(default_limits))),
            manager: None,
            static_overrides: Arc::new(RwLock::new(map)),
        }
    }

    /// Updates or removes a per-tenant static override dynamically across all clones.
    pub fn set_static_override(&self, tenant_id: &str, limits: Option<LimitsConfig>) {
        let mut g = self.static_overrides.write();
        match limits {
            Some(l) => {
                g.insert(tenant_id.to_string(), Arc::new(l));
            }
            None => {
                g.remove(tenant_id);
            }
        }
    }

    /// Clears all per-tenant static overrides across all clones.
    pub fn clear_static_overrides(&self) {
        self.static_overrides.write().clear();
    }

    /// Replaces the global default `LimitsConfig` across all clones.
    pub fn set_defaults(&self, default_limits: LimitsConfig) {
        *self.default_limits.write() = Arc::new(default_limits);
    }

    /// Returns the effective `LimitsConfig` for `tenant_id` (falling back to global defaults).
    #[inline]
    pub fn for_tenant(&self, tenant_id: &str) -> Arc<LimitsConfig> {
        if let Some(ref mgr) = self.manager {
            let cfg = mgr.get_config();
            if let Some(limits) = cfg.overrides.get(tenant_id) {
                return Arc::clone(limits);
            }
        } else {
            let g = self.static_overrides.read();
            if let Some(limits) = g.get(tenant_id) {
                return Arc::clone(limits);
            }
        }
        Arc::clone(&self.default_limits.read())
    }

    /// Returns a snapshot of the global default `LimitsConfig`.
    #[inline]
    pub fn defaults(&self) -> Arc<LimitsConfig> {
        Arc::clone(&self.default_limits.read())
    }

    #[inline]
    pub fn ingestion_rate(&self, tenant_id: &str) -> f64 {
        self.for_tenant(tenant_id).ingestion_rate
    }

    #[inline]
    pub fn ingestion_burst_size(&self, tenant_id: &str) -> i64 {
        self.for_tenant(tenant_id).ingestion_burst_size
    }

    #[inline]
    pub fn max_global_series_per_user(&self, tenant_id: &str) -> i64 {
        self.for_tenant(tenant_id).max_global_series_per_user
    }

    #[inline]
    pub fn max_global_series_per_metric(&self, tenant_id: &str) -> i64 {
        self.for_tenant(tenant_id).max_global_series_per_metric
    }

    #[inline]
    pub fn max_label_names_per_series(&self, tenant_id: &str) -> i64 {
        self.for_tenant(tenant_id).max_label_names_per_series
    }

    #[inline]
    pub fn max_label_name_length(&self, tenant_id: &str) -> i64 {
        self.for_tenant(tenant_id).max_label_name_length
    }

    #[inline]
    pub fn max_label_value_length(&self, tenant_id: &str) -> i64 {
        self.for_tenant(tenant_id).max_label_value_length
    }

    #[inline]
    pub fn max_global_metadata_per_user(&self, tenant_id: &str) -> i64 {
        self.for_tenant(tenant_id).max_global_metadata_per_user
    }

    #[inline]
    pub fn max_global_metadata_per_metric(&self, tenant_id: &str) -> i64 {
        self.for_tenant(tenant_id).max_global_metadata_per_metric
    }

    #[inline]
    pub fn max_global_exemplars_per_user(&self, tenant_id: &str) -> i64 {
        self.for_tenant(tenant_id).max_global_exemplars_per_user
    }

    #[inline]
    pub fn out_of_order_time_window_nanos(&self, tenant_id: &str) -> i64 {
        self.for_tenant(tenant_id).out_of_order_time_window.0
    }

    #[inline]
    pub fn creation_grace_period_nanos(&self, tenant_id: &str) -> i64 {
        self.for_tenant(tenant_id).creation_grace_period.0
    }

    #[inline]
    pub fn past_grace_period_nanos(&self, tenant_id: &str) -> i64 {
        self.for_tenant(tenant_id).past_grace_period.0
    }

    #[inline]
    pub fn native_histograms_ingestion_enabled(&self, tenant_id: &str) -> bool {
        self.for_tenant(tenant_id).native_histograms_ingestion_enabled
    }

    #[inline]
    pub fn max_native_histogram_buckets(&self, tenant_id: &str) -> i64 {
        self.for_tenant(tenant_id).max_native_histogram_buckets
    }
}

#[cfg(test)]
mod secret_tests {
    use super::*;

    // Go copies the default limits struct before unmarshalling a tenant's overrides, so a
    // secret set in the defaults must survive (and must not become the "********" mask).
    #[test]
    fn tenant_overrides_keep_default_secrets() {
        let mut defaults = LimitsConfig::default();
        defaults.ruler_alertmanager_client_config.basic_auth_password = "pw".to_string();
        let out = parse_runtime_config_yaml(
            b"overrides:\n  t1:\n    max_global_series_per_user: 5\n",
            &defaults,
            &InstanceLimits::default(),
        )
        .unwrap();
        let t1 = out.overrides.get("t1").unwrap();
        assert_eq!(t1.ruler_alertmanager_client_config.basic_auth_password, "pw");
        let rendered = serde_yaml::to_string(t1.as_ref()).unwrap();
        assert!(rendered.contains("basic_auth_password: '********'"));
    }
}
