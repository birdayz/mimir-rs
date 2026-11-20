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
use std::sync::{Arc, Condvar, Mutex as StdMutex, Weak};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_yaml::{Deserializer as YamlDeserializer, Mapping, Value};

use crate::duration::GoDuration;
use crate::error::ConfigError;
use crate::ingester::InstanceLimits;
use crate::limits::LimitsConfig;
use crate::server::RuntimeConfigConfig;
use crate::types::{NameValidationScheme, OTelTranslationStrategy};
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
    /// Go `TenantLimits map[string]*validation.Limits`: a `null` tenant entry is kept as `None`
    /// (rendered as `t: null` by `/runtime_config`) and resolves to the default limits.
    pub overrides: BTreeMap<String, Option<Arc<LimitsConfig>>>,
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
    parse_runtime_config_yaml_with_defaults(
        bytes,
        default_limits,
        default_ingester_limits,
        &DistributorInstanceLimits::default(),
        GoDuration::from_hours(12),
    )
}

pub fn parse_runtime_config_yaml_with_defaults(
    bytes: &[u8],
    default_limits: &LimitsConfig,
    default_ingester_limits: &InstanceLimits,
    default_distributor_limits: &DistributorInstanceLimits,
    querier_query_store_after: GoDuration,
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

    parse_runtime_config_value(
        root,
        default_limits,
        default_ingester_limits,
        default_distributor_limits,
        querier_query_store_after,
    )
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRuntimeConfigCheck {
    #[serde(default)]
    overrides: BTreeMap<String, Option<LimitsConfig>>,
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

fn merge_ruler_am_client_config(dst: &mut Value, src: Value) {
    let (Some(dst_map), Value::Mapping(src_map)) = (dst.as_mapping_mut(), src) else {
        return;
    };
    for (k, v) in src_map {
        if k.as_str() == Some("oauth2") {
            if let (Some(Value::Mapping(dst_oauth)), Value::Mapping(src_oauth)) =
                (dst_map.get_mut(&k), &v)
            {
                for (ok, ov) in src_oauth {
                    // `oauth2.endpoint_params` is `flagext.LimitsMap[string]`: replaces instead of merging.
                    if ok.as_str() == Some("endpoint_params") {
                        let val = if ov.is_null() {
                            Value::Mapping(Mapping::new())
                        } else {
                            ov.clone()
                        };
                        dst_oauth.insert(ok.clone(), val);
                    } else {
                        dst_oauth.insert(ok.clone(), ov.clone());
                    }
                }
                continue;
            }
        }
        dst_map.insert(k, v);
    }
}

fn parse_runtime_config_value(
    root: Value,
    default_limits: &LimitsConfig,
    default_ingester_limits: &InstanceLimits,
    default_distributor_limits: &DistributorInstanceLimits,
    querier_query_store_after: GoDuration,
) -> Result<RuntimeConfigValues, ConfigError> {
    let mut root = stringify_override_keys(root);
    crate::yaml_v3::normalize_value_for_type("mimir.runtimeConfigValues", &mut root)
        .map_err(ConfigError::RuntimeConfig)?;
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
            let mut merged = serde_yaml::to_value(default_distributor_limits)
                .map_err(|e| ConfigError::RuntimeConfig(e.to_string()))?;
            merge_yaml_values(&mut merged, dist_val);
            let parsed: DistributorInstanceLimits = serde_yaml::from_value(merged)
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

                // yaml.v3 decodes `t: null` into a nil `*validation.Limits`; Go keeps the nil
                // map entry (skipped by validation, falling back to defaults on lookup).
                if tenant_v.is_null() {
                    out.overrides.insert(tenant_id.to_string(), None);
                    continue;
                }

                let mut merged = base_limits_val.clone();
                if let Some(merged_map) = merged.as_mapping_mut() {
                    // Go `Limits.unmarshal` resets these two fields before decoding per-tenant
                    // overrides (`limits.go:671-674`), since they are set during RegisterFlags.
                    merged_map.insert(
                        Value::String("otel_metric_suffixes_enabled".to_string()),
                        Value::Null,
                    );
                    merged_map.insert(
                        Value::String("name_validation_scheme".to_string()),
                        Value::String(String::new()),
                    );
                }
                if let Value::Mapping(ref tenant_map) = tenant_v {
                    // Map fields with custom `UnmarshalYAML` (`flagext.LimitsMap`, `asmodel.CustomTrackersConfig`)
                    // replace the base map (`dskit/flagext/map.go:clear(m.data)`). Plain Go maps
                    // (`cost_attribution_trackers`, `additional_cost_attribution_trackers`) are cloned from
                    // `defaultLimits` and overlaid per key by `yaml.v3` unless explicitly `null`.
                    if let (Some(merged_map), Some(override_map)) =
                        (merged.as_mapping_mut(), Some(tenant_map))
                    {
                        for (k, v) in override_map {
                            let key_str = k.as_str().unwrap_or_default();
                            if matches!(
                                key_str,
                                "active_series_custom_trackers"
                                    | "active_series_additional_custom_trackers"
                                    | "ruler_max_rules_per_rule_group_by_namespace"
                                    | "ruler_max_rule_groups_per_tenant_by_namespace"
                                    | "alertmanager_notification_rate_limit_per_integration"
                            ) {
                                let val = if v.is_null() {
                                    Value::Mapping(Mapping::new())
                                } else {
                                    v.clone()
                                };
                                merged_map.insert(k.clone(), val);
                            } else if matches!(
                                key_str,
                                "cost_attribution_trackers" | "additional_cost_attribution_trackers"
                            ) {
                                if v.is_null() {
                                    merged_map.insert(k.clone(), Value::Mapping(Mapping::new()));
                                } else if let Some(override_entries) = v.as_mapping() {
                                    let entry = merged_map
                                        .entry(k.clone())
                                        .or_insert_with(|| Value::Mapping(Mapping::new()));
                                    if !entry.is_mapping() {
                                        *entry = Value::Mapping(Mapping::new());
                                    }
                                    if let Some(base_entries) = entry.as_mapping_mut() {
                                        for (tk, tv) in override_entries {
                                            if tv.is_null() {
                                                if !base_entries.contains_key(tk) {
                                                    base_entries.insert(
                                                        tk.clone(),
                                                        Value::Mapping(Mapping::new()),
                                                    );
                                                }
                                            } else {
                                                base_entries.insert(tk.clone(), tv.clone());
                                            }
                                        }
                                    }
                                } else {
                                    merged_map.insert(k.clone(), v.clone());
                                }
                            } else if key_str == "ruler_alertmanager_client_config" {
                                if let Some(existing) = merged_map.get_mut(k) {
                                    merge_ruler_am_client_config(existing, v.clone());
                                }
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

                let tenant_set_otel_strategy = tenant_v.as_mapping().is_some_and(|m| {
                    m.contains_key(Value::String("otel_translation_strategy".to_string()))
                });
                let mut limits: LimitsConfig = serde_yaml::from_value(merged).map_err(|e| {
                    ConfigError::RuntimeConfig(format!("tenant {tenant_id:?}: {e}"))
                })?;
                let validation_res = if tenant_set_otel_strategy {
                    limits.validate()
                } else {
                    limits.validate_with_defaults(Some(default_limits))
                };
                validation_res.map_err(|e| {
                    ConfigError::RuntimeConfig(format!("tenant {tenant_id:?}: {e}"))
                })?;
                if limits.query_ingesters_within.0 != 0
                    && querier_query_store_after.0 != 0
                    && querier_query_store_after.0 >= limits.query_ingesters_within.0
                {
                    return Err(ConfigError::RuntimeConfig(format!(
                        "tenant {tenant_id:?}: invalid limits config for querier: the -validation.query-ingesters-within setting must be greater than -querier.query-store-after otherwise queries might return partial results"
                    )));
                }

                out.overrides
                    .insert(tenant_id.to_string(), Some(Arc::new(limits)));
            }
        }
    }

    Ok(out)
}

/// dskit `sourceID`s of the configured runtime config sources, in order (for creating the
/// per-source metric series before the first load, like dskit `New`).
pub fn runtime_config_source_ids(cfg: &RuntimeConfigConfig) -> Result<Vec<String>, ConfigError> {
    let mut sources = Vec::with_capacity(cfg.file.0.len());
    for entry in &cfg.file.0 {
        if !entry.is_empty() {
            sources.push(parse_config_source(entry)?);
        }
    }
    assign_source_ids(&mut sources)?;
    Ok(sources.into_iter().map(|s| s.source_id).collect())
}

/// Read outcome of one runtime config source during a load attempt (dskit
/// `runtime_config_source_last_reload_successful` and the tolerated-failure warning).
#[derive(Clone, Debug)]
pub struct RuntimeConfigSourceStatus {
    /// dskit `sourceID`.
    pub source: String,
    /// `provider.Name()` (the configured path or URL), logged as `source`.
    pub name: String,
    /// `sourceParameters.String()`, e.g. `optional-on-startup`.
    pub parameters: &'static str,
    /// Read or unmarshal error, if any.
    pub error: Option<String>,
    /// Whether a read error was tolerated by the source's failure policy.
    pub tolerated: bool,
}

/// Outcome of one load attempt (dskit `Manager.loadConfig`).
#[derive(Debug)]
pub struct RuntimeConfigLoadEvent<'a> {
    /// The load performed while the manager starts.
    pub initial: bool,
    /// Per-source status, in `runtime_config.file` order. Sources not reached because an
    /// earlier source failed are omitted.
    pub sources: &'a [RuntimeConfigSourceStatus],
    /// `Err` when the attempt failed (the previous configuration stays active).
    pub result: &'a Result<(), ConfigError>,
    /// `runtime_config_hash` of the configuration applied by this attempt; `None` when nothing
    /// was applied (failure, or unchanged sources).
    pub applied_hash: Option<&'a str>,
}

/// Receives every load attempt (metrics and logging live with the caller).
pub trait RuntimeConfigObserver: Send + Sync {
    fn on_load(&self, event: &RuntimeConfigLoadEvent<'_>);
}

fn policy_parameters(p: &FailurePolicy) -> &'static str {
    match p {
        FailurePolicy::Strict => "",
        FailurePolicy::OptionalOnStartup => "optional-on-startup",
        FailurePolicy::OptionalKeepLastValueOnFailure => "optional-keep-last-value-on-failure",
    }
}

/// encoding/json decodes every JSON number into a float64 (dskit hashes the merged map in that
/// form, while values are decoded from integers where they are integral).
fn numbers_to_f64(v: &mut Value) {
    match v {
        Value::Number(n) => {
            if let Some(f) = n.as_f64() {
                *v = Value::Number(serde_yaml::Number::from(f));
            }
        }
        Value::Sequence(seq) => seq.iter_mut().for_each(numbers_to_f64),
        Value::Mapping(m) => m.iter_mut().for_each(|(_, v)| numbers_to_f64(v)),
        Value::Tagged(t) => numbers_to_f64(&mut t.value),
        _ => {}
    }
}

/// Manages loading and periodic hot-reloading of `runtime_config.file` sources (local files and
/// HTTP/HTTPS URLs), like `dskit/runtimeconfig.Manager`.
pub struct RuntimeConfigManagerOptions {
    pub cfg: RuntimeConfigConfig,
    pub default_limits: LimitsConfig,
    pub default_ingester_limits: InstanceLimits,
    pub default_distributor_limits: DistributorInstanceLimits,
    pub querier_query_store_after: GoDuration,
    pub observer: Option<Arc<dyn RuntimeConfigObserver>>,
    pub http_observer: Option<Arc<dyn RuntimeConfigHttpObserver>>,
}

pub struct RuntimeConfigManager {
    cfg: RuntimeConfigConfig,
    loader: LoaderKind,
    default_limits: Arc<LimitsConfig>,
    default_ingester_limits: InstanceLimits,
    default_distributor_limits: DistributorInstanceLimits,
    querier_query_store_after: GoDuration,
    sources: RwLock<Vec<ConfigSource>>,
    current: RwLock<Arc<RuntimeConfigValues>>,
    last_hash: RwLock<Option<String>>,
    listeners: parking_lot::Mutex<Vec<tokio::sync::mpsc::Sender<Arc<RuntimeConfigValues>>>>,
    http_observer: Arc<RwLock<Option<Arc<dyn RuntimeConfigHttpObserver>>>>,
    stop_signal: Arc<(StdMutex<bool>, Condvar)>,
    reloader: parking_lot::Mutex<Option<JoinHandle<()>>>,
    observer: Option<Arc<dyn RuntimeConfigObserver>>,
}

impl RuntimeConfigManager {
    /// Creates a new `RuntimeConfigManager` and performs the initial load if `cfg.file` is non-empty.
    pub fn new(
        cfg: RuntimeConfigConfig,
        default_limits: LimitsConfig,
        default_ingester_limits: InstanceLimits,
    ) -> Result<Arc<Self>, ConfigError> {
        Self::new_with_observer(cfg, default_limits, default_ingester_limits, None)
    }

    /// Like [`Self::new`], reporting every load attempt (including the initial one) to
    /// `observer`.
    pub fn new_with_observer(
        cfg: RuntimeConfigConfig,
        default_limits: LimitsConfig,
        default_ingester_limits: InstanceLimits,
        observer: Option<Arc<dyn RuntimeConfigObserver>>,
    ) -> Result<Arc<Self>, ConfigError> {
        Self::new_with_options(RuntimeConfigManagerOptions {
            cfg,
            default_limits,
            default_ingester_limits,
            default_distributor_limits: DistributorInstanceLimits::default(),
            querier_query_store_after: GoDuration::from_hours(12),
            observer,
            http_observer: None,
        })
    }

    /// Full constructor allowing caller-supplied `distributor.instance_limits`,
    /// `querier.query_store_after`, reload observer, and HTTP observer (wired before initial load).
    pub fn new_with_options(opts: RuntimeConfigManagerOptions) -> Result<Arc<Self>, ConfigError> {
        let RuntimeConfigManagerOptions {
            cfg,
            default_limits,
            default_ingester_limits,
            default_distributor_limits,
            querier_query_store_after,
            observer,
            http_observer: initial_http_observer,
        } = opts;
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
            Arc::new(RwLock::new(initial_http_observer));
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
            default_distributor_limits,
            querier_query_store_after,
            sources: RwLock::new(sources),
            current: RwLock::new(Arc::new(RuntimeConfigValues::default())),
            last_hash: RwLock::new(None),
            listeners: parking_lot::Mutex::new(Vec::new()),
            http_observer,
            stop_signal: Arc::new((StdMutex::new(false), Condvar::new())),
            reloader: parking_lot::Mutex::new(None),
            observer,
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
    ///
    /// Holds a `Weak<Self>` and sleeps on a `Condvar` so `stop()` and `Drop` wake and join
    /// immediately without leaking threads or blocking for `reload_period`.
    pub fn start_reloader(self: &Arc<Self>) {
        let mut guard = self.reloader.lock();
        if guard.is_some() {
            return;
        }
        {
            let (lock, _) = &*self.stop_signal;
            if *lock.lock().unwrap_or_else(|e| e.into_inner()) {
                return;
            }
        }
        let weak: Weak<Self> = Arc::downgrade(self);
        let stop_signal = Arc::clone(&self.stop_signal);
        let period = self
            .cfg
            .period
            .as_std()
            .filter(|d| !d.is_zero())
            .unwrap_or_else(|| Duration::from_secs(10));

        let handle = thread::spawn(move || {
            let (lock, cvar) = &*stop_signal;
            let mut stopped = lock.lock().unwrap_or_else(|e| e.into_inner());
            while !*stopped {
                let (next_guard, _) = cvar
                    .wait_timeout_while(stopped, period, |s| !*s)
                    .unwrap_or_else(|e| e.into_inner());
                stopped = next_guard;
                if *stopped {
                    break;
                }
                drop(stopped);
                let Some(mgr) = weak.upgrade() else {
                    return;
                };
                let _ = mgr.reload();
                drop(mgr);
                stopped = lock.lock().unwrap_or_else(|e| e.into_inner());
            }
        });
        *guard = Some(handle);
    }

    /// Signals any background reloader to stop, wakes it immediately, joins the thread,
    /// and closes all listener channels (`Manager.stopping`).
    pub fn stop(&self) {
        {
            let (lock, cvar) = &*self.stop_signal;
            let mut stopped = lock.lock().unwrap_or_else(|e| e.into_inner());
            *stopped = true;
            cvar.notify_all();
        }
        self.listeners.lock().clear();
        if let Some(handle) = self.reloader.lock().take() {
            if handle.thread().id() != thread::current().id() {
                let _ = handle.join();
            }
        }
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

    /// `Manager.loadConfig`, reported to the observer.
    fn load_internal(&self, initial: bool) -> Result<(), ConfigError> {
        let mut statuses = Vec::new();
        let mut applied_hash = None;
        let result = self.load_sources(initial, &mut statuses, &mut applied_hash);
        if let Some(observer) = &self.observer {
            observer.on_load(&RuntimeConfigLoadEvent {
                initial,
                sources: &statuses,
                result: &result,
                applied_hash: applied_hash.as_deref(),
            });
        }
        result
    }

    fn load_sources(
        &self,
        initial: bool,
        statuses: &mut Vec<RuntimeConfigSourceStatus>,
        applied_hash: &mut Option<String>,
    ) -> Result<(), ConfigError> {
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
            let mut status = RuntimeConfigSourceStatus {
                source: src.source_id.clone(),
                name: src.path.clone(),
                parameters: policy_parameters(&src.policy),
                error: None,
                tolerated: false,
            };
            match src.provider.read(&src.path, &src.source_id) {
                Ok(buf) => {
                    statuses.push(status);
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
                    status.error = Some(err.clone());
                    status.tolerated = tolerate;
                    statuses.push(status);
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
        // The merge as dskit hashes it (JSON sources keep encoding/json float64 numbers).
        let mut hash_merged = Mapping::new();
        for (i, (src, l)) in sources.iter().zip(&loaded).enumerate() {
            let Some(raw) = &l.raw else { continue };
            let fail = |statuses: &mut Vec<RuntimeConfigSourceStatus>, msg: String| {
                // dskit: unmarshal/merge failures also mark the source as failed.
                if let Some(st) = statuses.get_mut(i) {
                    st.error = Some(msg.clone());
                    st.tolerated = false;
                }
                ConfigError::RuntimeConfig(msg)
            };
            let file = match merge::unmarshal_maybe_gzipped(&src.path, raw) {
                Ok(f) => f,
                Err(e) => return Err(fail(statuses, format!("unmarshal {:?}: {e}", src.path))),
            };
            let mut hash_file = file.clone();
            if merge::decoded_as_json(&src.path, raw) {
                for (_, v) in hash_file.iter_mut() {
                    numbers_to_f64(v);
                }
            }
            merged = match merge::merge_config_maps(&merged, &file, "") {
                Ok(m) => m,
                Err(e) => {
                    return Err(fail(
                        statuses,
                        format!("can't merge {:?} on top of the previous providers: {e}", src.path),
                    ))
                }
            };
            if let Ok(m) = merge::merge_config_maps(&hash_merged, &hash_file, "") {
                hash_merged = m;
            }
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
                // sha256 of yaml.v3 `yaml.Marshal(mergedConfig)`.
                let marshalled = crate::yaml_v3_emit::marshal_generic(&Value::Mapping(hash_merged))
                    .map_err(|e| ConfigError::RuntimeConfig(format!("marshal file: {e}")))?;
                let hash = sha256_hex(marshalled.as_bytes());
                let values = parse_runtime_config_value(
                    root,
                    &self.default_limits,
                    &self.default_ingester_limits,
                    &self.default_distributor_limits,
                    self.querier_query_store_after,
                )
                .map_err(|e| ConfigError::RuntimeConfig(format!("load file: {}", strip_rc(e))))?;
                (values, hash)
            }
        };

        let values = Arc::new(values);
        *self.current.write() = Arc::clone(&values);
        self.call_listeners(&values);
        *self.last_hash.write() = Some(hash.clone());
        *applied_hash = Some(hash);

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
            &self.default_distributor_limits,
            self.querier_query_store_after,
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
        self.stop();
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
            if let Some(Some(limits)) = cfg.overrides.get(tenant_id) {
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

    /// Go `Overrides.OTelMetricSuffixesEnabled`: falls back to `default_limits` when unset (`None`).
    #[inline]
    pub fn otel_metric_suffixes_enabled(&self, tenant_id: &str) -> bool {
        if let Some(v) = self.for_tenant(tenant_id).otel_metric_suffixes_enabled {
            return v;
        }
        self.defaults().otel_metric_suffixes_enabled.unwrap_or(false)
    }

    /// Go `Overrides.NameValidationScheme`: falls back to `default_limits` when `Unset`,
    /// then to `Legacy`.
    #[inline]
    pub fn name_validation_scheme(&self, tenant_id: &str) -> NameValidationScheme {
        let scheme = self.for_tenant(tenant_id).name_validation_scheme;
        if scheme != NameValidationScheme::Unset {
            return scheme;
        }
        let def_scheme = self.defaults().name_validation_scheme;
        if def_scheme != NameValidationScheme::Unset {
            return def_scheme;
        }
        NameValidationScheme::Legacy
    }

    /// Go `Overrides.OTelTranslationStrategy`: returns explicit strategy if non-empty, otherwise
    /// derives it from `name_validation_scheme` + `otel_metric_suffixes_enabled`.
    #[inline]
    pub fn otel_translation_strategy(&self, tenant_id: &str) -> OTelTranslationStrategy {
        let strategy = self.for_tenant(tenant_id).otel_translation_strategy.clone();
        if !strategy.0.is_empty() {
            return strategy;
        }
        let suffixes = self.otel_metric_suffixes_enabled(tenant_id);
        let s = match self.name_validation_scheme(tenant_id) {
            NameValidationScheme::Legacy | NameValidationScheme::Unset => {
                if suffixes {
                    "UnderscoreEscapingWithSuffixes"
                } else {
                    "UnderscoreEscapingWithoutSuffixes"
                }
            }
            NameValidationScheme::Utf8 => {
                if suffixes {
                    "NoUTF8EscapingWithSuffixes"
                } else {
                    "NoTranslation"
                }
            }
        };
        OTelTranslationStrategy(s.to_string())
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
        let t1 = out.overrides.get("t1").unwrap().as_ref().unwrap();
        assert_eq!(t1.ruler_alertmanager_client_config.basic_auth_password, "pw");
        let rendered = serde_yaml::to_string(t1.as_ref()).unwrap();
        assert!(rendered.contains("basic_auth_password: '********'"));
    }
}

#[cfg(test)]
mod null_override_tests {
    use super::*;

    /// Go `TenantLimits map[string]*validation.Limits` keeps `t: null` as a nil entry that
    /// `Overrides` resolves to the default limits.
    #[test]
    fn null_tenant_override_is_kept_and_falls_back_to_defaults() {
        let defaults = LimitsConfig {
            max_global_series_per_user: 42,
            ..Default::default()
        };
        let out = parse_runtime_config_yaml(
            b"overrides:\n  t_null: null\n  t1:\n    max_global_series_per_user: 5\n",
            &defaults,
            &InstanceLimits::default(),
        )
        .unwrap();
        assert_eq!(out.overrides.len(), 2);
        assert!(out.overrides["t_null"].is_none());
        assert_eq!(
            out.overrides["t1"].as_ref().unwrap().max_global_series_per_user,
            5
        );

        let mgr = RuntimeConfigManager::new(
            RuntimeConfigConfig::default(),
            defaults,
            InstanceLimits::default(),
        )
        .unwrap();
        mgr.set_values_for_tests(out);
        let overrides = mgr.overrides();
        assert_eq!(overrides.max_global_series_per_user("t_null"), 42);
        assert_eq!(overrides.max_global_series_per_user("t1"), 5);
        assert_eq!(overrides.max_global_series_per_user("other"), 42);
    }

    #[test]
    fn cost_attribution_trackers_overlay_and_otel_inheritance() {
        use crate::limits::{CostAttributionLabel, CostAttributionTrackerConfig};

        let mut defaults = LimitsConfig::default();
        defaults.cost_attribution_trackers.insert(
            "global_base".to_string(),
            CostAttributionTrackerConfig {
                labels: vec![CostAttributionLabel {
                    input: "team".to_string(),
                    output: "team".to_string(),
                }],
                internal: false,
            },
        );
        defaults.additional_cost_attribution_trackers.insert(
            "global_add".to_string(),
            CostAttributionTrackerConfig {
                labels: vec![CostAttributionLabel {
                    input: "env".to_string(),
                    output: "env".to_string(),
                }],
                internal: false,
            },
        );
        defaults.name_validation_scheme = NameValidationScheme::Utf8;
        defaults.otel_metric_suffixes_enabled = Some(true);
        defaults.otel_translation_strategy =
            OTelTranslationStrategy("NoUTF8EscapingWithSuffixes".to_string());

        let out = parse_runtime_config_yaml(
            br#"
overrides:
  t_overlay:
    max_global_series_per_user: 100
    cost_attribution_trackers:
      tenant_base:
        labels:
          - input: service
            output: svc
    additional_cost_attribution_trackers:
      tenant_add:
        labels:
          - input: region
            output: region
  t_null_ca:
    cost_attribution_trackers: null
    otel_metric_suffixes_enabled: false
    name_validation_scheme: legacy
    otel_translation_strategy: UnderscoreEscapingWithoutSuffixes
"#,
            &defaults,
            &InstanceLimits::default(),
        )
        .unwrap();

        let t_ov = out.overrides["t_overlay"].as_ref().unwrap();
        assert!(t_ov.cost_attribution_trackers.contains_key("global_base"));
        assert!(t_ov.cost_attribution_trackers.contains_key("tenant_base"));
        assert!(t_ov
            .additional_cost_attribution_trackers
            .contains_key("global_add"));
        assert!(t_ov
            .additional_cost_attribution_trackers
            .contains_key("tenant_add"));

        let t_null = out.overrides["t_null_ca"].as_ref().unwrap();
        assert!(t_null.cost_attribution_trackers.is_empty());

        let mgr = RuntimeConfigManager::new(
            RuntimeConfigConfig::default(),
            defaults,
            InstanceLimits::default(),
        )
        .unwrap();
        mgr.set_values_for_tests(out);
        let ov = mgr.overrides();

        assert!(ov.otel_metric_suffixes_enabled("t_overlay"));
        assert_eq!(ov.name_validation_scheme("t_overlay"), NameValidationScheme::Utf8);
        assert_eq!(
            ov.otel_translation_strategy("t_overlay").0,
            "NoUTF8EscapingWithSuffixes"
        );
        assert!(!ov.otel_metric_suffixes_enabled("t_null_ca"));
        assert_eq!(
            ov.name_validation_scheme("t_null_ca"),
            NameValidationScheme::Legacy
        );
        assert_eq!(
            ov.otel_translation_strategy("t_null_ca").0,
            "UnderscoreEscapingWithoutSuffixes"
        );
    }

    #[test]
    fn w21_aps_03_reloader_stops_and_drops_immediately_despite_long_period() {
        let cfg = RuntimeConfigConfig {
            period: GoDuration::from_secs(3600),
            ..Default::default()
        };
        let mgr = RuntimeConfigManager::new(
            cfg,
            LimitsConfig::default(),
            InstanceLimits::default(),
        )
        .unwrap();
        mgr.start_reloader();

        // Explicit stop() must wake the Condvar and join immediately (<< 3600s).
        let started = std::time::Instant::now();
        mgr.stop();
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "stop() blocked for {:?}",
            started.elapsed()
        );

        // Dropping the last Arc without calling stop() must also wake & join via Weak + Drop.
        let cfg2 = RuntimeConfigConfig {
            period: GoDuration::from_secs(3600),
            ..Default::default()
        };
        let mgr2 = RuntimeConfigManager::new(
            cfg2,
            LimitsConfig::default(),
            InstanceLimits::default(),
        )
        .unwrap();
        mgr2.start_reloader();
        let started2 = std::time::Instant::now();
        drop(mgr2);
        assert!(
            started2.elapsed() < Duration::from_secs(2),
            "Drop blocked for {:?}",
            started2.elapsed()
        );
    }
}
