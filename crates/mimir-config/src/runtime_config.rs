//! Runtime configuration loader, hot reloader, and per-tenant limit overrides.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/mimir/runtime_config.go
//! - github.com/grafana/mimir/pkg/util/validation/limits.go (`TenantLimits`, `Overrides`)
//! - github.com/grafana/dskit/runtimeconfig/manager.go
//! - github.com/grafana/dskit/runtimeconfig/config_source.go

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_yaml::{Deserializer as YamlDeserializer, Value};

use crate::error::ConfigError;
use crate::ingester::InstanceLimits;
use crate::limits::LimitsConfig;
use crate::server::RuntimeConfigConfig;
use crate::sha256::sha256_hex;

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

#[derive(Clone, Debug)]
struct ConfigSource {
    path: String,
    policy: FailurePolicy,
    last_bytes: Option<Vec<u8>>,
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
            "runtime config source {entry:?} has more than one parameter"
        )));
    }

    Ok(ConfigSource {
        path: path.to_string(),
        policy: policies.into_iter().next().unwrap_or(FailurePolicy::Strict),
        last_bytes: None,
    })
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

fn parse_runtime_config_value(
    root: Value,
    default_limits: &LimitsConfig,
    default_ingester_limits: &InstanceLimits,
) -> Result<RuntimeConfigValues, ConfigError> {
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
            let base_limits_val = serde_yaml::to_value(default_limits)
                .map_err(|e| ConfigError::RuntimeConfig(e.to_string()))?;

            for (tenant_k, tenant_v) in overrides_map {
                let tenant_id = tenant_k.as_str().ok_or_else(|| {
                    ConfigError::RuntimeConfig("tenant ID key must be a string".to_string())
                })?;

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

/// Manages loading and periodic hot-reloading of `runtime_config.file` YAML files.
pub struct RuntimeConfigManager {
    cfg: RuntimeConfigConfig,
    default_limits: Arc<LimitsConfig>,
    default_ingester_limits: InstanceLimits,
    sources: RwLock<Vec<ConfigSource>>,
    current: RwLock<Arc<RuntimeConfigValues>>,
    last_hash: RwLock<Option<String>>,
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
        if !cfg.loader.is_empty() && cfg.loader != "yaml" && cfg.loader != "map" {
            return Err(ConfigError::RuntimeConfig(format!(
                "invalid runtime config loader {:?}: supported values are \"map\" and \"yaml\"",
                cfg.loader
            )));
        }

        let mut sources = Vec::with_capacity(cfg.file.0.len());
        for entry in &cfg.file.0 {
            if !entry.is_empty() {
                sources.push(parse_config_source(entry)?);
            }
        }

        let mgr = Arc::new(Self {
            cfg,
            default_limits: Arc::new(default_limits),
            default_ingester_limits,
            sources: RwLock::new(sources),
            current: RwLock::new(Arc::new(RuntimeConfigValues::default())),
            last_hash: RwLock::new(None),
            stop_flag: Arc::new(AtomicBool::new(false)),
            reloader: None,
        });

        mgr.load_internal(true)?;
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

    /// Signals any background reloader to stop.
    pub fn stop(&self) {
        self.stop_flag.store(true, Ordering::Relaxed);
    }

    /// Reloads runtime configuration files from disk immediately.
    pub fn reload(&self) -> Result<(), ConfigError> {
        self.load_internal(false)
    }

    /// Returns the currently active `RuntimeConfigValues`.
    pub fn get_config(&self) -> Arc<RuntimeConfigValues> {
        Arc::clone(&self.current.read())
    }

    /// Returns the SHA-256 hex digest of the last loaded merged runtime config bytes, if any.
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

    /// Builds an `Overrides` handle backed by this manager.
    pub fn overrides(self: &Arc<Self>) -> Overrides {
        Overrides::with_manager(Arc::clone(&self.default_limits), Arc::clone(self))
    }

    fn load_internal(&self, initial: bool) -> Result<(), ConfigError> {
        let mut sources = self.sources.write();
        if sources.is_empty() {
            return Ok(());
        }

        let mut merged_doc: Option<Value> = None;
        let mut combined_bytes = Vec::new();

        for src in sources.iter_mut() {
            let read_res = std::fs::read(Path::new(&src.path));
            let bytes = match read_res {
                Ok(b) => {
                    if src.policy == FailurePolicy::OptionalKeepLastValueOnFailure {
                        src.last_bytes = Some(b.clone());
                    }
                    b
                }
                Err(err) => {
                    let tolerate = match src.policy {
                        FailurePolicy::Strict => false,
                        FailurePolicy::OptionalOnStartup => initial,
                        FailurePolicy::OptionalKeepLastValueOnFailure => true,
                    };
                    if !tolerate {
                        return Err(ConfigError::RuntimeConfig(format!(
                            "failed to read runtime config {}: {err}",
                            src.path
                        )));
                    }
                    if src.policy == FailurePolicy::OptionalKeepLastValueOnFailure {
                        if let Some(ref prev) = src.last_bytes {
                            prev.clone()
                        } else {
                            continue;
                        }
                    } else {
                        continue;
                    }
                }
            };

            combined_bytes.extend_from_slice(&bytes);

            let mut docs = Vec::new();
            for doc in YamlDeserializer::from_slice(&bytes) {
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
            if let Some(val) = docs.into_iter().next() {
                if !val.is_null() {
                    match &mut merged_doc {
                        None => merged_doc = Some(val),
                        Some(existing) => merge_yaml_values(existing, val),
                    }
                }
            }
        }

        let new_hash = sha256_hex(&combined_bytes);
        if self.last_hash.read().as_deref() == Some(new_hash.as_str()) {
            return Ok(());
        }

        let values = match merged_doc {
            None => RuntimeConfigValues::default(),
            Some(root) => parse_runtime_config_value(
                root,
                &self.default_limits,
                &self.default_ingester_limits,
            )?,
        };

        *self.current.write() = Arc::new(values);
        *self.last_hash.write() = Some(new_hash);
        Ok(())
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
    default_limits: Arc<LimitsConfig>,
    manager: Option<Arc<RuntimeConfigManager>>,
    static_overrides: Option<StaticOverridesMap>,
}

impl Overrides {
    /// Creates an `Overrides` with only global defaults (no runtime overrides).
    pub fn new(default_limits: LimitsConfig) -> Self {
        Self {
            default_limits: Arc::new(default_limits),
            manager: None,
            static_overrides: None,
        }
    }

    /// Creates an `Overrides` backed by a `RuntimeConfigManager`.
    pub fn with_manager(
        default_limits: Arc<LimitsConfig>,
        manager: Arc<RuntimeConfigManager>,
    ) -> Self {
        Self {
            default_limits,
            manager: Some(manager),
            static_overrides: None,
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
            default_limits: Arc::new(default_limits),
            manager: None,
            static_overrides: Some(Arc::new(RwLock::new(map))),
        }
    }

    /// Returns the effective `LimitsConfig` for `tenant_id` (falling back to global defaults).
    #[inline]
    pub fn for_tenant(&self, tenant_id: &str) -> Arc<LimitsConfig> {
        if let Some(ref mgr) = self.manager {
            let cfg = mgr.get_config();
            if let Some(limits) = cfg.overrides.get(tenant_id) {
                return Arc::clone(limits);
            }
        } else if let Some(ref map) = self.static_overrides {
            if let Some(limits) = map.read().get(tenant_id) {
                return Arc::clone(limits);
            }
        }
        Arc::clone(&self.default_limits)
    }

    /// Returns the global default `LimitsConfig`.
    #[inline]
    pub fn defaults(&self) -> &LimitsConfig {
        &self.default_limits
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
