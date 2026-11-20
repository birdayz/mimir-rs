//! Top-level Mimir configuration, YAML loader, `common:` inheritance, and validation.
//!
//! Ported from:
//! - github.com/grafana/mimir/cmd/mimir/main.go
//! - github.com/grafana/mimir/pkg/mimir/mimir.go

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_yaml::{Mapping, Value};

use crate::duration::GoDuration;
use crate::env::expand_environment_variables;
use crate::error::{go_path_error, CliAction, ConfigError};
use crate::flags::{is_targetable_module, parse_config_file_parameter, FlagSet, MainFlags};
use crate::ingester::{ClusterValidationConfig, IngesterClientConfig, IngesterConfig};
use crate::limits::LimitsConfig;
use crate::schema::{find_top_block, register_extra_flags, validate_yaml_block};
use crate::server::{
    ActivityTrackerConfig, CompartmentsConfig, MemberlistConfig, RuntimeConfigConfig, ServerConfig,
    UsageStatsConfig,
};
use crate::sha256::sha256_hex;
use crate::storage::{BlocksStorageConfig, IngestStorageConfig, StorageBackendConfig};
use crate::types::StringSliceCSV;

/// `common:` configuration block inherited into storage and client cluster validation configs.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CommonConfig {
    pub storage: StorageBackendConfig,
    pub client_cluster_validation: ClusterValidationConfig,
}

impl CommonConfig {
    pub fn register_flags_into<T: 'static>(
        &self,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        self.storage.register_flags_into_with_dir(
            "common.storage.",
            "",
            f,
            move |t| &get(t).storage,
            move |t| &mut get_mut(t).storage,
        );
        self.client_cluster_validation.register_flags_into(
            "common.client-cluster-validation.",
            f,
            move |t| &get(t).client_cluster_validation,
            move |t| &mut get_mut(t).client_cluster_validation,
        );
    }
}

/// Configuration for reference leak instrumentation (`pkg/mimirpb/ref_leaks.go:InstrumentRefLeaksConfig`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct InstrumentRefLeaksConfig {
    pub percentage: f64,
    pub before_reuse_period: GoDuration,
    pub max_inflight_instrumented_bytes: u64,
}

impl Default for InstrumentRefLeaksConfig {
    fn default() -> Self {
        Self {
            percentage: 0.0,
            before_reuse_period: GoDuration::from_mins(2),
            max_inflight_instrumented_bytes: 0,
        }
    }
}

impl InstrumentRefLeaksConfig {
    pub fn register_flags_into<T: 'static>(
        &self,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        f.f64_var(
            "instrument-reference-leaks.percentage",
            self.percentage,
            "Percentage [0-100] of request or message buffers to instrument for reference leaks. Set to 0 to disable.",
            move |t| &get(t).percentage,
            move |t| &mut get_mut(t).percentage,
        );
        f.go_duration_var(
            "instrument-reference-leaks.before-reuse-period",
            self.before_reuse_period,
            "Period after a buffer instrumented for referenced leaks is nominally freed until the buffer is uninstrumented and effectively freed to be reused. After this period, any lingering references to the buffer may potentially be dereferenced again with no detection.",
            move |t| &get(t).before_reuse_period,
            move |t| &mut get_mut(t).before_reuse_period,
        );
        f.u64_var(
            "instrument-reference-leaks.max-inflight-instrumented-bytes",
            self.max_inflight_instrumented_bytes,
            "Maximum sum of length of buffers instrumented at any given time, in bytes. When surpassed, incoming buffers will not be instrumented, regardless of the configured percentage. Zero means no limit.",
            move |t| &get(t).max_inflight_instrumented_bytes,
            move |t| &mut get_mut(t).max_inflight_instrumented_bytes,
        );
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        if self.percentage != 0.0 {
            return Err(ConfigError::Validation(format!(
                "reference leaks instrumentation not available in {}",
                std::env::consts::OS
            )));
        }
        if self.percentage < 0.0 || self.percentage > 100.0 || self.percentage.is_nan() {
            return Err(ConfigError::Validation(format!(
                "percentage must be in [0-100], got: {}",
                self.percentage
            )));
        }
        if self.before_reuse_period.as_nanos() < 0 {
            return Err(ConfigError::Validation(format!(
                "before-reuse-period must be positive, got: {}",
                self.before_reuse_period
            )));
        }
        Ok(())
    }

    pub fn to_proto_config(&self) -> mimir_proto::ref_leaks::InstrumentRefLeaksConfig {
        mimir_proto::ref_leaks::InstrumentRefLeaksConfig {
            percentage: self.percentage,
            before_reuse_period: self
                .before_reuse_period
                .as_std()
                .unwrap_or(std::time::Duration::ZERO),
            max_inflight_instrumented_bytes: self.max_inflight_instrumented_bytes,
        }
    }
}

/// Core fields of `mimir.Config` deserialized directly into strongly typed Rust structs.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default)]
struct CoreMimirConfig {
    pub target: StringSliceCSV,
    pub multitenancy_enabled: bool,
    pub no_auth_tenant: String,
    pub shutdown_delay: GoDuration,
    pub max_separate_metrics_groups_per_user: i64,
    pub enable_go_runtime_metrics: bool,
    pub include_tenant_id_in_profile_labels: bool,
    pub timeseries_unmarshal_caching_optimization_enabled: bool,
    pub cost_attribution_eviction_interval: GoDuration,
    pub cost_attribution_registry_path: String,
    pub cost_attribution_cleanup_interval: GoDuration,
    pub label_access_control_enabled: bool,

    pub common: CommonConfig,
    pub server: ServerConfig,
    pub ingester_client: IngesterClientConfig,
    pub ingester: IngesterConfig,
    pub limits: LimitsConfig,
    pub ingest_storage: IngestStorageConfig,
    pub compartments: CompartmentsConfig,
    pub blocks_storage: BlocksStorageConfig,
    pub activity_tracker: ActivityTrackerConfig,
    pub runtime_config: RuntimeConfigConfig,
    pub memberlist: MemberlistConfig,
    pub usage_stats: UsageStatsConfig,
    pub instrument_ref_leaks: InstrumentRefLeaksConfig,
}

impl Default for CoreMimirConfig {
    fn default() -> Self {
        Self::from_config(&MimirConfig::default())
    }
}

impl CoreMimirConfig {
    /// Snapshot of the core sections of `d` (the values YAML is unmarshalled on top of).
    fn from_config(d: &MimirConfig) -> Self {
        let d = d.clone();
        Self {
            target: d.target,
            multitenancy_enabled: d.multitenancy_enabled,
            no_auth_tenant: d.no_auth_tenant,
            shutdown_delay: d.shutdown_delay,
            max_separate_metrics_groups_per_user: d.max_separate_metrics_groups_per_user,
            enable_go_runtime_metrics: d.enable_go_runtime_metrics,
            include_tenant_id_in_profile_labels: d.include_tenant_id_in_profile_labels,
            timeseries_unmarshal_caching_optimization_enabled: d
                .timeseries_unmarshal_caching_optimization_enabled,
            cost_attribution_eviction_interval: d.cost_attribution_eviction_interval,
            cost_attribution_registry_path: d.cost_attribution_registry_path,
            cost_attribution_cleanup_interval: d.cost_attribution_cleanup_interval,
            label_access_control_enabled: d.label_access_control_enabled,
            common: d.common,
            server: d.server,
            ingester_client: d.ingester_client,
            ingester: d.ingester,
            limits: d.limits,
            ingest_storage: d.ingest_storage,
            compartments: d.compartments,
            blocks_storage: d.blocks_storage,
            activity_tracker: d.activity_tracker,
            runtime_config: d.runtime_config,
            memberlist: d.memberlist,
            usage_stats: d.usage_stats,
            instrument_ref_leaks: d.instrument_ref_leaks,
        }
    }
}

/// Complete top-level Mimir configuration (`pkg/mimir/mimir.go:Config`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct MimirConfig {
    pub target: StringSliceCSV,
    pub multitenancy_enabled: bool,
    pub no_auth_tenant: String,
    pub shutdown_delay: GoDuration,
    pub max_separate_metrics_groups_per_user: i64,
    pub enable_go_runtime_metrics: bool,
    pub include_tenant_id_in_profile_labels: bool,
    pub timeseries_unmarshal_caching_optimization_enabled: bool,
    pub cost_attribution_eviction_interval: GoDuration,
    pub cost_attribution_registry_path: String,
    pub cost_attribution_cleanup_interval: GoDuration,
    pub label_access_control_enabled: bool,

    #[serde(skip)]
    pub print_config: bool,

    pub common: CommonConfig,
    pub server: ServerConfig,
    pub ingester_client: IngesterClientConfig,
    pub ingester: IngesterConfig,
    pub limits: LimitsConfig,
    pub ingest_storage: IngestStorageConfig,
    pub compartments: CompartmentsConfig,
    pub blocks_storage: BlocksStorageConfig,
    pub activity_tracker: ActivityTrackerConfig,
    pub runtime_config: RuntimeConfigConfig,
    pub memberlist: MemberlistConfig,
    pub usage_stats: UsageStatsConfig,
    pub instrument_ref_leaks: InstrumentRefLeaksConfig,

    /// Other top-level Mimir sections (`api`, `distributor`, `querier`, `frontend`, `ruler`, etc.)
    /// validated against the upstream schema so full-cluster `mimir.yaml` files load cleanly.
    #[serde(flatten)]
    pub extra_sections: BTreeMap<String, Value>,

    #[serde(skip)]
    extra_cli_flag_values: BTreeMap<String, String>,
    #[serde(skip)]
    deprecated_flags_used: Vec<String>,
}

impl Default for MimirConfig {
    fn default() -> Self {
        Self {
            target: StringSliceCSV(vec!["all".to_string()]),
            multitenancy_enabled: true,
            no_auth_tenant: "anonymous".to_string(),
            shutdown_delay: GoDuration::ZERO,
            max_separate_metrics_groups_per_user: 1000,
            enable_go_runtime_metrics: false,
            include_tenant_id_in_profile_labels: true,
            timeseries_unmarshal_caching_optimization_enabled: true,
            cost_attribution_eviction_interval: GoDuration::from_mins(20),
            cost_attribution_registry_path: String::new(),
            cost_attribution_cleanup_interval: GoDuration::from_mins(3),
            label_access_control_enabled: false,
            print_config: false,
            common: CommonConfig::default(),
            server: ServerConfig::default(),
            ingester_client: IngesterClientConfig::default(),
            ingester: IngesterConfig::default(),
            limits: LimitsConfig::default(),
            ingest_storage: IngestStorageConfig::default(),
            compartments: CompartmentsConfig::default(),
            blocks_storage: BlocksStorageConfig::default(),
            activity_tracker: ActivityTrackerConfig::default(),
            runtime_config: RuntimeConfigConfig::default(),
            memberlist: MemberlistConfig::default(),
            usage_stats: UsageStatsConfig::default(),
            instrument_ref_leaks: InstrumentRefLeaksConfig::default(),
            extra_sections: BTreeMap::new(),
            extra_cli_flag_values: BTreeMap::new(),
            deprecated_flags_used: Vec::new(),
        }
    }
}

const CORE_TOP_KEYS: &[&str] = &[
    "target",
    "multitenancy_enabled",
    "no_auth_tenant",
    "shutdown_delay",
    "max_separate_metrics_groups_per_user",
    "enable_go_runtime_metrics",
    "include_tenant_id_in_profile_labels",
    "timeseries_unmarshal_caching_optimization_enabled",
    "cost_attribution_eviction_interval",
    "cost_attribution_registry_path",
    "cost_attribution_cleanup_interval",
    "label_access_control_enabled",
    "common",
    "server",
    "ingester_client",
    "ingester",
    "limits",
    "ingest_storage",
    "compartments",
    "blocks_storage",
    "activity_tracker",
    "runtime_config",
    "memberlist",
    "usage_stats",
    "instrument_ref_leaks",
];

const EXTRA_HIDDEN_TOP_KEYS: &[&str] = &[
    "block_builder",
    "block_builder_scheduler",
    "usage_tracker",
];

const COMMON_CLIENT_CLUSTER_VALIDATION_FLAGS: &[&str] = &[
    "ingester.client.cluster-validation.label",
    "querier.frontend-client.cluster-validation.label",
    "querier.scheduler-client.cluster-validation.label",
    "compactor.scheduler-client.grpc-client-config.cluster-validation.label",
    "block-builder.scheduler.grpc-client-config.cluster-validation.label",
    "query-frontend.grpc-client-config.cluster-validation.label",
    "querier.store-gateway-client.cluster-validation.label",
    "query-frontend.client-cluster-validation.label",
    "query-scheduler.grpc-client-config.cluster-validation.label",
    "ruler.client.cluster-validation.label",
    "ruler.distributor.grpc-client-config.cluster-validation.label",
    "ruler.query-frontend.grpc-client-config.cluster-validation.label",
    "alertmanager.alertmanager-client.cluster-validation.label",
    "distributor.usage-tracker-client.grpc-client-config.cluster-validation.label",
    "runtime-config.http-client-cluster-validation.label",
];

const COMMON_STORAGE_DEST_PREFIXES: &[&str] = &[
    "blocks-storage.",
    "ruler-storage.",
    "alertmanager-storage.",
    "usage-tracker.snapshots-storage.",
];

impl MimirConfig {
    /// Registers all Mimir flags onto `f` directly targeting `MimirConfig`.
    pub fn register_flags(&self, f: &mut FlagSet<Self>) {
        self.register_flags_into(f, |c| c, |c| c);
    }

    /// Registers all Mimir flags onto `f` via projection closures.
    pub fn register_flags_into<T: 'static>(
        &self,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        f.string_slice_csv_var(
            "target",
            &self.target,
            "Comma-separated list of components to include in the instantiated process.",
            move |t| &get(t).target,
            move |t| &mut get_mut(t).target,
        );
        f.bool_var(
            "auth.multitenancy-enabled",
            self.multitenancy_enabled,
            "When set to true, incoming HTTP requests must specify tenant ID in HTTP X-Scope-OrgId header.",
            move |t| &get(t).multitenancy_enabled,
            move |t| &mut get_mut(t).multitenancy_enabled,
        );
        f.string_var(
            "auth.no-auth-tenant",
            &self.no_auth_tenant,
            "Tenant ID to use when multitenancy is disabled.",
            move |t| &get(t).no_auth_tenant,
            move |t| &mut get_mut(t).no_auth_tenant,
        );
        f.bool_var(
            "auth.label-access-control-enabled",
            self.label_access_control_enabled,
            "If enabled, Mimir enforces label-based access control on metric read queries.",
            move |t| &get(t).label_access_control_enabled,
            move |t| &mut get_mut(t).label_access_control_enabled,
        );
        f.bool_var(
            "print.config",
            self.print_config,
            "Print the config and exit.",
            move |t| &get(t).print_config,
            move |t| &mut get_mut(t).print_config,
        );
        f.go_duration_var(
            "shutdown-delay",
            self.shutdown_delay,
            "How long to wait between SIGTERM and shutdown.",
            move |t| &get(t).shutdown_delay,
            move |t| &mut get_mut(t).shutdown_delay,
        );
        f.i64_var(
            "max-separate-metrics-groups-per-user",
            self.max_separate_metrics_groups_per_user,
            "Maximum number of groups allowed per user by which specified distributor and ingester metrics can be further separated.",
            move |t| &get(t).max_separate_metrics_groups_per_user,
            move |t| &mut get_mut(t).max_separate_metrics_groups_per_user,
        );
        f.bool_var(
            "enable-go-runtime-metrics",
            self.enable_go_runtime_metrics,
            "Set to true to enable all Go runtime metrics.",
            move |t| &get(t).enable_go_runtime_metrics,
            move |t| &mut get_mut(t).enable_go_runtime_metrics,
        );
        f.bool_var(
            "timeseries-unmarshal-caching-optimization-enabled",
            self.timeseries_unmarshal_caching_optimization_enabled,
            "Enables optimized marshaling of timeseries.",
            move |t| &get(t).timeseries_unmarshal_caching_optimization_enabled,
            move |t| &mut get_mut(t).timeseries_unmarshal_caching_optimization_enabled,
        );
        f.string_var(
            "cost-attribution.registry-path",
            &self.cost_attribution_registry_path,
            "Defines a custom path for the registry.",
            move |t| &get(t).cost_attribution_registry_path,
            move |t| &mut get_mut(t).cost_attribution_registry_path,
        );
        f.go_duration_var(
            "cost-attribution.eviction-interval",
            self.cost_attribution_eviction_interval,
            "Specifies how often inactive cost attributions are evicted from the counter.",
            move |t| &get(t).cost_attribution_eviction_interval,
            move |t| &mut get_mut(t).cost_attribution_eviction_interval,
        );
        f.go_duration_var(
            "cost-attribution.cleanup-interval",
            self.cost_attribution_cleanup_interval,
            "Time interval at which the cost attribution cleanup process runs.",
            move |t| &get(t).cost_attribution_cleanup_interval,
            move |t| &mut get_mut(t).cost_attribution_cleanup_interval,
        );
        f.bool_var(
            "include-tenant-id-in-profile-labels",
            self.include_tenant_id_in_profile_labels,
            "Include tenant ID in pprof labels for profiling.",
            move |t| &get(t).include_tenant_id_in_profile_labels,
            move |t| &mut get_mut(t).include_tenant_id_in_profile_labels,
        );

        self.server
            .register_flags_into(f, move |t| &get(t).server, move |t| &mut get_mut(t).server);
        self.memberlist.register_flags_into(
            f,
            move |t| &get(t).memberlist,
            move |t| &mut get_mut(t).memberlist,
        );
        self.ingester_client.register_flags_into(
            f,
            move |t| &get(t).ingester_client,
            move |t| &mut get_mut(t).ingester_client,
        );
        self.ingester.register_flags_into(
            f,
            move |t| &get(t).ingester,
            move |t| &mut get_mut(t).ingester,
        );
        self.limits
            .register_flags_into(f, move |t| &get(t).limits, move |t| &mut get_mut(t).limits);
        self.ingest_storage.register_flags_into(
            f,
            move |t| &get(t).ingest_storage,
            move |t| &mut get_mut(t).ingest_storage,
        );
        self.compartments.register_flags_into(
            f,
            move |t| &get(t).compartments,
            move |t| &mut get_mut(t).compartments,
        );
        self.blocks_storage.register_flags_into(
            f,
            move |t| &get(t).blocks_storage,
            move |t| &mut get_mut(t).blocks_storage,
        );
        self.runtime_config.register_flags_into(
            f,
            move |t| &get(t).runtime_config,
            move |t| &mut get_mut(t).runtime_config,
        );
        self.activity_tracker.register_flags_into(
            f,
            move |t| &get(t).activity_tracker,
            move |t| &mut get_mut(t).activity_tracker,
        );
        self.usage_stats.register_flags_into(
            f,
            move |t| &get(t).usage_stats,
            move |t| &mut get_mut(t).usage_stats,
        );
        self.instrument_ref_leaks.register_flags_into(
            f,
            move |t| &get(t).instrument_ref_leaks,
            move |t| &mut get_mut(t).instrument_ref_leaks,
        );
        self.common
            .register_flags_into(f, move |t| &get(t).common, move |t| &mut get_mut(t).common);

        register_extra_flags(
            f,
            move |t| &get(t).extra_cli_flag_values,
            move |t| &mut get_mut(t).extra_cli_flag_values,
        );

        // Deprecated flag registered unconditionally by pkg/usagetracker/tracker.go.
        f.register_raw(
            "usage-tracker.use-global-series-limits",
            false,
            "value",
            "deprecated",
            "",
            move |t, _s| {
                get_mut(t).record_deprecated_flag("usage-tracker.use-global-series-limits");
                Ok(())
            },
            move |_t| "deprecated".to_string(),
        );
    }

    /// Loads YAML configuration from `path`, returning the SHA-256 hex digest of raw bytes BEFORE env expansion.
    pub fn load_config_from_file(
        &mut self,
        path: &Path,
        expand_env: bool,
    ) -> Result<String, ConfigError> {
        use std::io::Read as _;
        let shown = path.display().to_string();
        let mut file = std::fs::File::open(path)
            .map_err(|e| ConfigError::ReadConfigFile(go_path_error("open", &shown, &e)))?;
        let mut raw = Vec::new();
        file.read_to_end(&mut raw)
            .map_err(|e| ConfigError::ReadConfigFile(go_path_error("read", &shown, &e)))?;
        self.load_config_from_bytes(&raw, expand_env)
    }

    /// Loads YAML configuration from raw bytes, returning the SHA-256 hex digest BEFORE env expansion.
    pub fn load_config_from_bytes(
        &mut self,
        raw_bytes: &[u8],
        expand_env: bool,
    ) -> Result<String, ConfigError> {
        let hash_hex = sha256_hex(raw_bytes);

        let expanded;
        let bytes_to_parse = if expand_env {
            expanded = expand_environment_variables(raw_bytes);
            expanded.as_slice()
        } else {
            raw_bytes
        };

        // Go decodes with yaml.v3 `KnownFields(true)`: reject exactly what it rejects, with its
        // error texts (all decode errors, in document order), and convert scalars according to
        // Go yaml.v3 rules (e.g. int/bool -> string, float -> int, YAML 1.1 bools -> bool).
        let Some(doc) = crate::yaml_v3::check_config_document(bytes_to_parse).map_err(ConfigError::Yaml)? else {
            return Ok(hash_hex);
        };
        let mut root: Value = crate::yaml_v3::doc_to_serde_value(&doc).map_err(ConfigError::Yaml)?;

        if root.is_null() {
            return Ok(hash_hex);
        }

        let root_map = root.as_mapping_mut().ok_or_else(|| {
            ConfigError::Yaml("expected YAML mapping at document root".to_string())
        })?;

        // 1. Validate every top-level key against known MimirConfig keys.
        for (k, v) in root_map.iter() {
            let key = k.as_str().ok_or_else(|| {
                ConfigError::Yaml("non-string top-level key in YAML".to_string())
            })?;
            if CORE_TOP_KEYS.contains(&key) {
                continue;
            }
            if let Some(schema_block) = find_top_block(key) {
                validate_yaml_block(v, schema_block, key)?;
                continue;
            }
            if EXTRA_HIDDEN_TOP_KEYS.contains(&key) {
                continue;
            }
            return Err(ConfigError::UnknownField(format!(
                "field {key} not found in type mimir.Config"
            )));
        }

        // 2. Validate `common:` strictly and apply `common:` YAML inheritance before deserializing core sections.
        if let Some(common_val) = root_map.get(Value::String("common".to_string())).cloned() {
            if !common_val.is_null() {
                let _: CommonConfig = serde_yaml::from_value(common_val.clone())
                    .map_err(|e| ConfigError::Yaml(format!("can't unmarshal common config: {e}")))?;

                if let Some(common_map) = common_val.as_mapping() {
                    if let Some(common_storage) =
                        common_map.get(Value::String("storage".to_string()))
                    {
                        if !common_storage.is_null() {
                            apply_common_storage_yaml(root_map, common_storage);
                        }
                    }
                    if let Some(common_ccv) =
                        common_map.get(Value::String("client_cluster_validation".to_string()))
                    {
                        if !common_ccv.is_null() {
                            apply_common_cluster_validation_yaml(root_map, common_ccv);
                        }
                    }
                }
            }
        }

        // 3. Split out extra_sections vs core sections, converting null block values to empty mappings.
        let mut core_map = Mapping::new();
        let mut extras = BTreeMap::new();
        for (k, v) in root_map.iter() {
            let key = k.as_str().unwrap_or_default();
            if CORE_TOP_KEYS.contains(&key) {
                let normalized = if v.is_null() && is_core_block_key(key) {
                    Value::Mapping(Mapping::new())
                } else {
                    v.clone()
                };
                core_map.insert(k.clone(), normalized);
            } else {
                extras.insert(key.to_string(), v.clone());
            }
        }

        // Recursively drop Null values on struct-typed nested blocks? Note serde's #[serde(default)]
        // handles omitted fields; for explicit `null` in nested blocks (e.g. `limits: null`),
        // sanitize_null_mappings ensures Go yaml.v3 compatibility.
        let mut core_val = Value::Mapping(core_map);
        crate::yaml_v3::zero_null_fields_like_yaml_v3(&mut core_val);
        validate_flattened_core_keys(&core_val)?;

        // Go unmarshals the YAML on top of the already-populated (flag default) config, so
        // omitted nested fields keep their context-specific defaults (e.g.
        // `blocks_storage.filesystem.dir: blocks`) instead of the nested type's zero/default.
        // Mirror that by deep-merging the document onto the serialized current values.
        let mut merged_core = crate::secret::with_secrets_revealed(|| {
            serde_yaml::to_value(CoreMimirConfig::from_config(self))
        })
        .map_err(|e| ConfigError::Yaml(e.to_string()))?;
        merge_yaml_onto(&mut merged_core, core_val);

        let prev_name_validation_scheme = self.limits.name_validation_scheme;
        let mut core: CoreMimirConfig = serde_yaml::from_value(merged_core)
            .map_err(|e| ConfigError::Yaml(e.to_string()))?;
        if core.limits.name_validation_scheme == crate::types::NameValidationScheme::Unset {
            core.limits.name_validation_scheme = prev_name_validation_scheme;
        }
        // Go fails in CustomTrackersConfig.UnmarshalYAML (i.e. while parsing the file).
        crate::limits::validate_custom_trackers_yaml(&core.limits)?;

        self.apply_core(core);
        self.extra_sections = extras;

        Ok(hash_hex)
    }

    fn apply_core(&mut self, core: CoreMimirConfig) {
        self.target = core.target;
        self.multitenancy_enabled = core.multitenancy_enabled;
        self.no_auth_tenant = core.no_auth_tenant;
        self.shutdown_delay = core.shutdown_delay;
        self.max_separate_metrics_groups_per_user = core.max_separate_metrics_groups_per_user;
        self.enable_go_runtime_metrics = core.enable_go_runtime_metrics;
        self.include_tenant_id_in_profile_labels = core.include_tenant_id_in_profile_labels;
        self.timeseries_unmarshal_caching_optimization_enabled =
            core.timeseries_unmarshal_caching_optimization_enabled;
        self.cost_attribution_eviction_interval = core.cost_attribution_eviction_interval;
        self.cost_attribution_registry_path = core.cost_attribution_registry_path;
        self.cost_attribution_cleanup_interval = core.cost_attribution_cleanup_interval;
        self.label_access_control_enabled = core.label_access_control_enabled;
        self.common = core.common;
        self.server = core.server;
        self.ingester_client = core.ingester_client;
        self.ingester = core.ingester;
        self.limits = core.limits;
        self.ingest_storage = core.ingest_storage;
        self.compartments = core.compartments;
        self.blocks_storage = core.blocks_storage;
        self.activity_tracker = core.activity_tracker;
        self.runtime_config = core.runtime_config;
        self.memberlist = core.memberlist;
        self.usage_stats = core.usage_stats;
        self.instrument_ref_leaks = core.instrument_ref_leaks;
    }

    pub fn is_module_explicitly_targeted(&self, module: &str) -> bool {
        self.target.0.iter().any(|t| t == module)
    }

    pub fn is_ingester_enabled(&self) -> bool {
        self.is_module_explicitly_targeted("all") || self.is_module_explicitly_targeted("ingester")
    }

    pub fn is_distributor_enabled(&self) -> bool {
        self.is_module_explicitly_targeted("all")
            || self.is_module_explicitly_targeted("distributor")
    }

    pub fn is_query_frontend_enabled(&self) -> bool {
        self.is_module_explicitly_targeted("all")
            || self.is_module_explicitly_targeted("query-frontend")
    }

    pub fn is_querier_enabled(&self) -> bool {
        self.is_module_explicitly_targeted("all") || self.is_module_explicitly_targeted("querier")
    }

    pub fn is_store_gateway_enabled(&self) -> bool {
        self.is_module_explicitly_targeted("all")
            || self.is_module_explicitly_targeted("store-gateway")
    }

    pub fn is_compactor_enabled(&self) -> bool {
        self.is_module_explicitly_targeted("all") || self.is_module_explicitly_targeted("compactor")
    }

    pub fn is_ruler_enabled(&self) -> bool {
        self.is_module_explicitly_targeted("all") || self.is_module_explicitly_targeted("ruler")
    }

    /// Inherits `common.*` CLI flags into target flags that were not explicitly set on the CLI.
    /// Ported from `pkg/mimir/mimir.go:InheritCommonFlagValues`.
    pub fn inherit_common_flag_values<T>(
        target: &mut T,
        fs: &mut FlagSet<T>,
    ) -> Result<(), ConfigError> {
        for suffix in StorageBackendConfig::common_flag_suffixes() {
            let origin = format!("common.storage.{suffix}");
            if !fs.is_set(&origin) {
                continue;
            }
            let Some(origin_val) = fs.get_value(target, &origin) else {
                continue;
            };
            for dest_prefix in COMMON_STORAGE_DEST_PREFIXES {
                let dest = format!("{dest_prefix}{suffix}");
                if fs.is_set(&dest) {
                    continue;
                }
                // Note: do NOT skip when `fs.get_value(target, &dest) == Some(origin_val)` for
                // extra_sections, because `fs.get_value` only reads CLI flags/defaults and would
                // fail to override a non-default YAML value in `extra_sections`.
                fs.set_value(target, &dest, &origin_val).map_err(|e| {
                    ConfigError::CommonInheritance(format!(
                        "can't inherit common flags for {dest_prefix:?}: {e}"
                    ))
                })?;
            }
        }

        let ccv_origin = "common.client-cluster-validation.label";
        if fs.is_set(ccv_origin) {
            if let Some(origin_val) = fs.get_value(target, ccv_origin) {
                for dest in COMMON_CLIENT_CLUSTER_VALIDATION_FLAGS {
                    if fs.is_set(dest) {
                        continue;
                    }
                    fs.set_value(target, dest, &origin_val).map_err(|e| {
                        ConfigError::CommonInheritance(format!(
                            "can't inherit common flags for {dest:?}: {e}"
                        ))
                    })?;
                }
            }
        }

        Ok(())
    }

    /// Records invocation of a deprecated CLI flag (matching `dskit/flagext/deprecated.go`).
    pub fn record_deprecated_flag(&mut self, flag_name: &str) {
        self.deprecated_flags_used.push(flag_name.to_string());
    }

    /// Returns the list of deprecated CLI flags invoked during flag parsing.
    pub fn deprecated_flags_used(&self) -> &[String] {
        &self.deprecated_flags_used
    }

    /// Returns the CLI-set value for a flag belonging to non-ingester sections, if overridden via CLI or common inheritance.
    pub fn extra_cli_flag_value(&self, flag_name: &str) -> Option<&str> {
        self.extra_cli_flag_values.get(flag_name).map(String::as_str)
    }

    /// Validates that `-target` contains at least one valid user-visible module.
    pub fn validate_targets(&self) -> Result<(), ConfigError> {
        if self.target.0.is_empty() {
            return Err(ConfigError::Validation(
                "no target module specified, the -target flag must be set to a valid module (e.g. \"all\")".to_string(),
            ));
        }
        for module in &self.target.0 {
            if !is_targetable_module(module) {
                return Err(ConfigError::Validation(format!(
                    "selected target ({module}) is an internal module, which is not allowed"
                )));
            }
        }
        Ok(())
    }

    /// Effective `distributor.instance_limits` from YAML `extra_sections` + CLI overrides
    /// (`pkg/mimir/runtime_config.go`: `SetDefaultInstanceLimitsForYAMLUnmarshalling`).
    pub fn distributor_instance_limits(
        &self,
    ) -> Result<crate::runtime_config::DistributorInstanceLimits, ConfigError> {
        let mut lim = match self
            .extra_sections
            .get("distributor")
            .and_then(|d| d.get("instance_limits"))
        {
            Some(val) if !val.is_null() => serde_yaml::from_value(val.clone()).map_err(|e| {
                ConfigError::Validation(format!("invalid distributor.instance_limits: {e}"))
            })?,
            _ => crate::runtime_config::DistributorInstanceLimits::default(),
        };
        if let Some(v) = self.extra_cli_flag_value("distributor.instance-limits.max-ingestion-rate") {
            lim.max_ingestion_rate = crate::flags::go_parse_float(v)?;
        }
        if let Some(v) = self.extra_cli_flag_value("distributor.instance-limits.max-inflight-push-requests") {
            lim.max_inflight_push_requests = crate::flags::go_parse_int(v)?;
        }
        if let Some(v) = self.extra_cli_flag_value("distributor.instance-limits.max-inflight-push-requests-bytes") {
            lim.max_inflight_push_requests_bytes = crate::flags::go_parse_int(v)?;
        }
        Ok(lim)
    }

    /// Effective `querier.timeout` from CLI or YAML (default 2m).
    pub fn querier_timeout(&self) -> GoDuration {
        if let Some(v) = self.extra_cli_flag_value("querier.timeout") {
            if let Ok(d) = v.parse::<GoDuration>() {
                return d;
            }
        }
        if let Some(v) = self
            .extra_sections
            .get("querier")
            .and_then(|q| q.get("timeout"))
            .and_then(Value::as_str)
        {
            if let Ok(d) = v.parse::<GoDuration>() {
                return d;
            }
        }
        GoDuration::from_secs(120)
    }

    /// Effective `querier.query_store_after` from CLI or YAML (default 12h).
    pub fn querier_query_store_after(&self) -> GoDuration {
        if let Some(v) = self.extra_cli_flag_value("querier.query-store-after") {
            if let Ok(d) = v.parse::<GoDuration>() {
                return d;
            }
        }
        if let Some(v) = self
            .extra_sections
            .get("querier")
            .and_then(|q| q.get("query_store_after"))
            .and_then(Value::as_str)
        {
            if let Ok(d) = v.parse::<GoDuration>() {
                return d;
            }
        }
        GoDuration::from_hours(12)
    }

    /// Effective `(tenant_federation.enabled, tenant_federation.max_tenants)` from CLI or YAML.
    pub fn tenant_federation_config(&self) -> (bool, i64) {
        let enabled = if let Some(v) = self.extra_cli_flag_value("tenant-federation.enabled") {
            crate::types::parse_go_bool(v).unwrap_or(false)
        } else {
            self.extra_sections
                .get("tenant_federation")
                .and_then(|tf| tf.get("enabled"))
                .and_then(Value::as_bool)
                .unwrap_or(false)
        };
        let max_tenants = if let Some(v) = self.extra_cli_flag_value("tenant-federation.max-tenants") {
            crate::flags::go_parse_int(v).unwrap_or(0)
        } else {
            self.extra_sections
                .get("tenant_federation")
                .and_then(|tf| tf.get("max_tenants"))
                .and_then(Value::as_i64)
                .unwrap_or(0)
        };
        (enabled, max_tenants)
    }

    /// Validates `limits` against cross-component rules (`pkg/mimir/mimir.go:Config.ValidateLimits`).
    pub fn validate_limits(&self, limits: &LimitsConfig) -> Result<(), ConfigError> {
        let query_store_after = self.querier_query_store_after();
        if limits.query_ingesters_within.0 != 0
            && query_store_after.0 != 0
            && query_store_after.0 >= limits.query_ingesters_within.0
        {
            return Err(ConfigError::Validation(
                "invalid limits config for querier: the -validation.query-ingesters-within setting must be greater than -querier.query-store-after otherwise queries might return partial results".to_string(),
            ));
        }
        Ok(())
    }

    /// Validates the full configuration, matching `pkg/mimir/mimir.go:Config.Validate`.
    pub fn validate(&mut self) -> Result<(), ConfigError> {

        self.server.validate()?;
        self.validate_bucket_configs()?;
        self.validate_filesystem_paths()?;
        self.ingest_storage
            .validate()
            .map_err(wrap_validation("invalid ingest storage config"))?;
        self.compartments
            .validate()
            .map_err(wrap_validation("invalid compartments config"))?;

        if self.compartments.enabled {
            if !self.ingest_storage.enabled {
                return Err(ConfigError::Validation(
                    "compartments require ingest storage to be enabled".to_string(),
                ));
            }
            if self
                .ingest_storage
                .migration
                .distributor_send_to_ingesters_enabled
            {
                return Err(ConfigError::Validation(
                    "compartments cannot be enabled together with ingest storage migration's distributor-send-to-ingesters".to_string(),
                ));
            }
            if (self.is_distributor_enabled() || self.is_query_frontend_enabled())
                && !self
                    .ingest_storage
                    .kafka
                    .topic
                    .contains("<read-compartment-id>")
            {
                return Err(ConfigError::Validation(
                    "when compartments are enabled, -ingest-storage.kafka.topic must contain the \"<read-compartment-id>\" placeholder for the distributor and query-frontend".to_string(),
                ));
            }
            if (self.is_ingester_enabled() || self.is_query_frontend_enabled())
                && self.compartments.write.num_compartments > 1
            {
                for addr in &self.ingest_storage.kafka.address.0 {
                    if !addr.contains("<write-compartment-id>") {
                        return Err(ConfigError::Validation(
                            "when compartments are enabled with more than one write compartment, every -ingest-storage.kafka.address must contain the \"<write-compartment-id>\" placeholder for the ingester and query-frontend".to_string(),
                        ));
                    }
                }
            }
            if self.is_querier_enabled()
                && !self
                    .blocks_storage
                    .bucket
                    .bucket_name()
                    .ends_with("<read-compartment-id>")
            {
                return Err(ConfigError::Validation(
                    "when compartments are enabled, the blocks storage bucket name must end with the \"<read-compartment-id>\" placeholder for the querier".to_string(),
                ));
            }
        }

        if self.is_ingester_enabled()
            && !self.ingest_storage.enabled
            && !self.ingester.push_grpc_method_enabled
        {
            return Err(ConfigError::Validation(
                "cannot disable Push gRPC method in ingester, while ingest storage (-ingest-storage.enabled) is not enabled".to_string(),
            ));
        }

        self.memberlist
            .validate()
            .map_err(wrap_validation("invalid memberlist config"))?;
        self.blocks_storage
            .validate_for_target(
                self.ingester.active_series.active_series_metrics_enabled,
                self.is_ingester_enabled(),
            )
            .map_err(wrap_validation("invalid TSDB config"))?;
        if self.is_ingester_enabled() {
            self.blocks_storage
                .bucket
                .backend_cfg
                .validate_mimir_rs_support()?;
        }
        let querier_timeout = self.querier_timeout();
        if querier_timeout.0 > self.server.http_server_write_timeout.0 {
            return Err(ConfigError::Validation(format!(
                "querier timeout ({querier_timeout}) must be lower than or equal to HTTP server write timeout ({})",
                self.server.http_server_write_timeout
            )));
        }
        self.ingester_client
            .validate()
            .map_err(wrap_validation("invalid ingester_client config"))?;

        if let Err(err) = self.ingester.validate(
            self.compartments.enabled,
            self.compartments.read.num_compartments,
        ) {
            // Only fail on spread-minimizing error if ingester is targeted.
            let is_spread_min = err.to_string().contains("spread-minimizing");
            if self.is_ingester_enabled() || !is_spread_min {
                return Err(wrap_validation("invalid ingester config")(err));
            }
        }

        self.usage_stats
            .validate()
            .map_err(wrap_validation("invalid usage stats config"))?;

        self.instrument_ref_leaks
            .validate()
            .map_err(wrap_validation("invalid instrument-ref-leaks config"))?;

        // pkg/api/api.go Config.Validate.
        let level = self.api_response_compression_level()?;
        if !(1..=9).contains(&level) && level != -1 {
            return Err(wrap_validation("invalid API config")(ConfigError::Validation(format!(
                "invalid gzip compression level: {level}, must be between 1 and 9 or -1 for default compression level"
            ))));
        }

        self.limits.validate()?;
        self.validate_limits(&self.limits)?;

        // Go fetches keys and certificates from Vault in the `vault` module (initVault) of every
        // target; mimir-rs has no Vault client, so refuse to start instead of silently using the
        // on-disk paths.
        if self.vault_enabled()? {
            // pkg/vault/vault.go:Config.Validate runs before the CFG-16 unsupported guard.
            let url = self.extra_storage_str(&["vault"], &["url"], "vault.url", "");
            if url.is_empty() {
                return Err(wrap_validation("invalid vault config")(
                    ConfigError::Validation("empty vault URL supplied".to_string()),
                ));
            }
            let mount_path =
                self.extra_storage_str(&["vault"], &["mount_path"], "vault.mount-path", "");
            if mount_path.is_empty() {
                return Err(wrap_validation("invalid vault config")(
                    ConfigError::Validation("empty vault mount path supplied".to_string()),
                ));
            }
            let auth_type =
                self.extra_storage_str(&["vault", "auth"], &["type"], "vault.auth.type", "");
            if !matches!(
                auth_type.as_str(),
                "approle" | "kubernetes" | "userpass" | "token"
            ) {
                return Err(wrap_validation("invalid vault config")(
                    ConfigError::Validation(
                        "invalid vault auth supplied: unsupported auth method".to_string(),
                    ),
                ));
            }
            return Err(ConfigError::Validation(
                "vault integration (-vault.enabled) is not supported by mimir-rs".to_string(),
            ));
        }

        Ok(())
    }

    /// `vault.enabled` / `-vault.enabled` (default false). The `vault` block is kept as an extra
    /// section; an explicitly set flag overrides the YAML value like Go's flag parsing.
    pub fn vault_enabled(&self) -> Result<bool, ConfigError> {
        if let Some(v) = self.extra_cli_flag_value("vault.enabled") {
            // Already type-checked when the flag was parsed.
            return crate::types::parse_go_bool(v);
        }
        Ok(self
            .extra_sections
            .get("vault")
            .and_then(|vault| vault.get("enabled"))
            .and_then(Value::as_bool)
            .unwrap_or(false))
    }

    /// `api.response_compression_level` / `-http.response-compression-level` (default
    /// `gzip.DefaultCompression` = -1). The `api` block is kept as an extra section.
    pub fn api_response_compression_level(&self) -> Result<i64, ConfigError> {
        if let Some(v) = self.extra_cli_flag_value("http.response-compression-level") {
            // Already type-checked when the flag was parsed.
            return crate::flags::go_parse_int(v);
        }
        Ok(self
            .extra_sections
            .get("api")
            .and_then(|api| api.get("response_compression_level"))
            .and_then(Value::as_i64)
            .unwrap_or(-1))
    }

    fn extra_storage_str(
        &self,
        section_path: &[&str],
        field_path: &[&str],
        flag_name: &str,
        default_val: &str,
    ) -> String {
        if let Some(v) = self.extra_cli_flag_value(flag_name) {
            return v.to_string();
        }
        let mut cur = section_path
            .first()
            .and_then(|top| self.extra_sections.get(*top));
        for seg in section_path.iter().skip(1).chain(field_path.iter()) {
            cur = cur.and_then(|v| v.get(*seg));
        }
        cur.and_then(Value::as_str)
            .unwrap_or(default_val)
            .to_string()
    }

    /// Validates that ruler / alertmanager / usage-tracker do not reuse the blocks_storage
    /// bucket + storage_prefix (`pkg/mimir/mimir.go:validateBucketConfigs`).
    fn validate_bucket_configs(&self) -> Result<(), ConfigError> {
        let mut errs: Vec<String> = Vec::new();
        let blocks = &self.blocks_storage.bucket;
        let check = |errs: &mut Vec<String>,
                     label: &str,
                     section_path: &[&str],
                     flag_prefix: &str,
                     default_backend: &str| {
            let backend = self.extra_storage_str(section_path, &["backend"], &format!("{flag_prefix}.backend"), default_backend);
            if (default_backend == "local" && backend == "local")
                || (default_backend == "filesystem" && backend == "filesystem")
            {
                return;
            }
            if backend != blocks.backend_cfg.backend {
                return;
            }
            let prefix = self.extra_storage_str(section_path, &["storage_prefix"], &format!("{flag_prefix}.storage-prefix"), "");
            if prefix != blocks.storage_prefix {
                return;
            }
            match backend.as_str() {
                "s3" => {
                    let b = self.extra_storage_str(section_path, &["s3", "bucket_name"], &format!("{flag_prefix}.s3.bucket-name"), "");
                    if b == blocks.backend_cfg.s3.bucket_name {
                        errs.push(format!("{label}: S3 bucket name and storage prefix cannot be the same as the one used in blocks storage config"));
                    }
                }
                "gcs" => {
                    let b = self.extra_storage_str(section_path, &["gcs", "bucket_name"], &format!("{flag_prefix}.gcs.bucket-name"), "");
                    if b == blocks.backend_cfg.gcs.bucket_name {
                        errs.push(format!("{label}: GCS bucket name and storage prefix cannot be the same as the one used in blocks storage config"));
                    }
                }
                "azure" => {
                    let c = self.extra_storage_str(section_path, &["azure", "container_name"], &format!("{flag_prefix}.azure.container-name"), "");
                    let a = self.extra_storage_str(section_path, &["azure", "account_name"], &format!("{flag_prefix}.azure.account-name"), "");
                    if c == blocks.backend_cfg.azure.container_name && a == blocks.backend_cfg.azure.account_name {
                        errs.push(format!("{label}: Azure container and account names and storage prefix cannot be the same as the ones used in blocks storage config"));
                    }
                }
                "swift" => {
                    let c = self.extra_storage_str(section_path, &["swift", "container_name"], &format!("{flag_prefix}.swift.container-name"), "");
                    let p = self.extra_storage_str(section_path, &["swift", "project_name"], &format!("{flag_prefix}.swift.project-name"), "");
                    if c == blocks.backend_cfg.swift.container_name && p == blocks.backend_cfg.swift.project_name {
                        errs.push(format!("{label}: Swift container and project names and storage prefix cannot be the same as the ones used in blocks storage config"));
                    }
                }
                _ => {}
            }
        };

        if self.is_module_explicitly_targeted("alertmanager") {
            check(&mut errs, "alertmanager storage", &["alertmanager_storage"], "alertmanager-storage", "local");
        }
        if self.is_ruler_enabled() {
            check(&mut errs, "ruler storage", &["ruler_storage"], "ruler-storage", "local");
        }
        if self.is_module_explicitly_targeted("usage-tracker") {
            check(&mut errs, "usage-tracker snapshots storage", &["usage_tracker", "snapshots_storage"], "usage-tracker.snapshots-storage", "filesystem");
        }

        if !errs.is_empty() {
            let msg = if errs.len() == 1 {
                format!("invalid bucket config: {}", errs[0])
            } else {
                format!("invalid bucket config: {} errors occurred:\n\t* {}\n", errs.len(), errs.join("\n\t* "))
            };
            return Err(ConfigError::Validation(msg));
        }
        Ok(())
    }

    fn validate_filesystem_paths(&self) -> Result<(), ConfigError> {
        struct PathEntry {
            name: &'static str,
            cfg_value: String,
            check_value: PathBuf,
        }

        let mut entries: Vec<PathEntry> = Vec::new();

        if (self.is_ingester_enabled()
            || self.is_querier_enabled()
            || self.is_store_gateway_enabled()
            || self.is_compactor_enabled()
            || self.is_ruler_enabled())
            && self.blocks_storage.bucket.backend_cfg.backend == "filesystem"
        {
            let dir = &self.blocks_storage.bucket.backend_cfg.filesystem.dir;
            let joined = go_filepath_join(dir, &self.blocks_storage.bucket.storage_prefix);
            entries.push(PathEntry {
                name: "blocks storage filesystem directory",
                cfg_value: dir.clone(),
                check_value: joined,
            });
        }

        if self.is_ingester_enabled() {
            entries.push(PathEntry {
                name: "tsdb directory",
                cfg_value: self.blocks_storage.tsdb.dir.clone(),
                check_value: PathBuf::from(&self.blocks_storage.tsdb.dir),
            });
        }

        if self.is_store_gateway_enabled() {
            entries.push(PathEntry {
                name: "bucket store sync directory",
                cfg_value: self.blocks_storage.bucket_store.sync_dir.clone(),
                check_value: PathBuf::from(&self.blocks_storage.bucket_store.sync_dir),
            });
        }

        if self.is_compactor_enabled() {
            let dir = self.extra_storage_str(&["compactor"], &["data_dir"], "compactor.data-dir", "./data-compactor/");
            entries.push(PathEntry {
                name: "compactor data directory",
                cfg_value: dir.clone(),
                check_value: PathBuf::from(&dir),
            });
        }

        if self.is_ruler_enabled() {
            let rule_path = self.extra_storage_str(&["ruler"], &["rule_path"], "ruler.rule-path", "./data-ruler/");
            entries.push(PathEntry {
                name: "ruler data directory",
                cfg_value: rule_path.clone(),
                check_value: PathBuf::from(&rule_path),
            });
            let backend = self.extra_storage_str(&["ruler_storage"], &["backend"], "ruler-storage.backend", "filesystem");
            if backend == "filesystem" {
                let dir = self.extra_storage_str(&["ruler_storage"], &["filesystem", "dir"], "ruler-storage.filesystem.dir", "ruler");
                let check_value = go_filepath_join(&dir, "rules");
                entries.push(PathEntry {
                    name: "ruler storage filesystem directory",
                    cfg_value: dir,
                    check_value,
                });
            }
            if backend == "local" {
                let dir = self.extra_storage_str(&["ruler_storage"], &["local", "directory"], "ruler-storage.local.directory", "");
                entries.push(PathEntry {
                    name: "ruler storage local directory",
                    cfg_value: dir.clone(),
                    check_value: PathBuf::from(&dir),
                });
            }
        }

        if self.is_module_explicitly_targeted("alertmanager") {
            let data_dir = self.extra_storage_str(&["alertmanager"], &["data_dir"], "alertmanager.storage.path", "./data-alertmanager/");
            entries.push(PathEntry {
                name: "alertmanager data directory",
                cfg_value: data_dir.clone(),
                check_value: PathBuf::from(&data_dir),
            });
            let backend = self.extra_storage_str(&["alertmanager_storage"], &["backend"], "alertmanager-storage.backend", "filesystem");
            if backend == "filesystem" {
                let dir = self.extra_storage_str(&["alertmanager_storage"], &["filesystem", "dir"], "alertmanager-storage.filesystem.dir", "alertmanager");
                entries.push(PathEntry {
                    name: "alertmanager storage filesystem directory",
                    cfg_value: dir.clone(),
                    check_value: go_filepath_join(&dir, "alerts"),
                });
                entries.push(PathEntry {
                    name: "alertmanager storage filesystem directory",
                    cfg_value: dir.clone(),
                    check_value: go_filepath_join(&dir, "alertmanager"),
                });
            }
            if backend == "local" {
                let dir = self.extra_storage_str(&["alertmanager_storage"], &["local", "path"], "alertmanager-storage.local.path", "");
                entries.push(PathEntry {
                    name: "alertmanager storage local directory",
                    cfg_value: dir.clone(),
                    check_value: PathBuf::from(&dir),
                });
            }
        }

        if self.is_module_explicitly_targeted("usage-tracker") {
            let backend = self.extra_storage_str(&["usage_tracker", "snapshots_storage"], &["backend"], "usage-tracker.snapshots-storage.backend", "filesystem");
            if backend == "filesystem" {
                let dir = self.extra_storage_str(&["usage_tracker", "snapshots_storage"], &["filesystem", "dir"], "usage-tracker.snapshots-storage.filesystem.dir", "usagetrackersnapshots");
                let check_value = go_filepath_join(&dir, "usage-tracker-snapshots");
                entries.push(PathEntry {
                    name: "usage tracker snapshot storage filesystem directory",
                    cfg_value: dir,
                    check_value,
                });
            }
        }

        let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("/"));
        let abs_paths: Vec<Option<String>> = entries
            .iter()
            .map(|e| {
                let p = if e.check_value.is_absolute() {
                    e.check_value.clone()
                } else {
                    cwd.join(&e.check_value)
                };
                Some(clean_path(&p))
            })
            .collect();

        for (i, first) in entries.iter().enumerate() {
            let Some(ref first_abs) = abs_paths[i] else {
                continue;
            };
            for (j, second) in entries.iter().enumerate() {
                if first.name == second.name {
                    continue;
                }
                let Some(ref second_abs) = abs_paths[j] else {
                    continue;
                };
                if is_abs_path_overlapping(first_abs, second_abs) {
                    return Err(ConfigError::Validation(format!(
                        "the configured {} {:?} cannot overlap with the configured {} {:?}; please set different paths, also ensuring one is not a subdirectory of the other one",
                        first.name, first.cfg_value, second.name, second.cfg_value
                    )));
                }
            }
        }

        Ok(())
    }
}

/// `errors.Wrap(err, prefix)` as used by `pkg/mimir/mimir.go:Config.Validate`.
fn wrap_validation(prefix: &'static str) -> impl FnOnce(ConfigError) -> ConfigError {
    move |err| match err {
        ConfigError::Validation(msg) => ConfigError::Validation(format!("{prefix}: {msg}")),
        other => ConfigError::Validation(format!("{prefix}: {other}")),
    }
}

/// Go `filepath.Join(base, elem)`: a leading `/` in `elem` does not discard `base`.
fn go_filepath_join(base: &str, elem: &str) -> PathBuf {
    if elem.is_empty() {
        PathBuf::from(base)
    } else if base.is_empty() {
        PathBuf::from(elem)
    } else {
        let trimmed = elem.trim_start_matches('/');
        if trimmed.is_empty() {
            PathBuf::from(base)
        } else {
            Path::new(base).join(trimmed)
        }
    }
}

fn clean_path(p: &Path) -> String {
    let mut parts: Vec<std::ffi::OsString> = Vec::new();
    for comp in p.components() {
        match comp {
            std::path::Component::RootDir => {}
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                parts.pop();
            }
            std::path::Component::Normal(s) => parts.push(s.to_os_string()),
            std::path::Component::Prefix(pre) => parts.push(pre.as_os_str().to_os_string()),
        }
    }
    if parts.is_empty() {
        "/".to_string()
    } else {
        let mut out = String::new();
        for part in parts {
            out.push('/');
            out.push_str(&part.to_string_lossy());
        }
        out
    }
}

fn is_abs_path_overlapping(first: &str, second: &str) -> bool {
    if first == "/" || second == "/" {
        return true;
    }
    if first == second {
        return true;
    }
    first.starts_with(&format!("{second}/")) || second.starts_with(&format!("{first}/"))
}

fn is_core_block_key(key: &str) -> bool {
    matches!(
        key,
        "common"
            | "server"
            | "ingester_client"
            | "ingester"
            | "limits"
            | "ingest_storage"
            | "compartments"
            | "blocks_storage"
            | "activity_tracker"
            | "runtime_config"
            | "memberlist"
            | "usage_stats"
            | "instrument_ref_leaks"
    )
}

fn check_allowed_keys(val: &Value, allowed: &[&str], path: &str) -> Result<(), ConfigError> {
    if let Some(map) = val.as_mapping() {
        for (k, _) in map {
            let key = k.as_str().ok_or_else(|| {
                ConfigError::Yaml(format!("{path}: non-string YAML key"))
            })?;
            if !allowed.contains(&key) {
                return Err(ConfigError::UnknownField(format!(
                    "field {key} not found in {path}"
                )));
            }
        }
    }
    Ok(())
}

fn validate_flattened_core_keys(core_val: &Value) -> Result<(), ConfigError> {
    let Some(root) = core_val.as_mapping() else {
        return Ok(());
    };
    if let Some(bs) = root.get(Value::String("blocks_storage".to_string())) {
        check_allowed_keys(
            bs,
            &[
                "backend",
                "s3",
                "gcs",
                "azure",
                "swift",
                "filesystem",
                "storage_prefix",
                "bucket_store",
                "tsdb",
            ],
            "blocks_storage",
        )?;
    }
    if let Some(ing) = root.get(Value::String("ingester".to_string())) {
        check_allowed_keys(
            ing,
            &[
                "ring",
                "partition_ring",
                "metadata_retain_period",
                "rate_update_period",
                "active_series_metrics_enabled",
                "active_series_metrics_update_period",
                "active_series_metrics_idle_timeout",
                "tsdb_config_update_period",
                "instance_limits",
                "ignore_series_limit_for_metric_names",
                "read_path_cpu_utilization_limit",
                "read_path_memory_utilization_limit",
                "error_sample_rate",
                "use_ingester_owned_series_for_limits",
                "track_ingester_owned_series",
                "owned_series_update_interval",
                "early_compaction_non_owned_series_enabled",
                "early_compaction_non_owned_series_min_grace_period",
                "early_compaction_non_owned_series_max_grace_period",
                "push_circuit_breaker",
                "read_circuit_breaker",
                "rejection_prioritizer",
                "push_reactive_limiter",
                "read_reactive_limiter",
                "compute_workers",
                "label_values_count",
                "label_churn_enabled",
                "push_grpc_method_enabled",
                "read_compartment_id",
            ],
            "ingester",
        )?;
    }
    if let Some(http) = root
        .get(Value::String("server".to_string()))
        .and_then(|s| s.get("cluster_validation"))
        .and_then(|c| c.get("http"))
    {
        check_allowed_keys(
            http,
            &[
                "enabled",
                "soft_validation",
                "excluded_paths",
                "excluded_user_agents",
            ],
            "server.cluster_validation.http",
        )?;
    }
    Ok(())
}

/// Deep-merges `src` onto `dst`: mappings merge key by key (yaml.v3 decoding into an existing
/// struct/map), every other node replaces the destination value.
fn merge_yaml_onto(dst: &mut Value, src: Value) {
    match (dst, src) {
        (Value::Mapping(dst_map), Value::Mapping(src_map)) => {
            for (k, v) in src_map {
                match dst_map.get_mut(&k) {
                    Some(existing) if existing.is_mapping() && v.is_mapping() => {
                        merge_yaml_onto(existing, v);
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



/// Deep-merges `base` underneath `overlay`: keys in `overlay` win; nested mappings are merged recursively.
fn merge_yaml_underneath(overlay: &mut Value, base: &Value) {
    if overlay.is_null() {
        *overlay = base.clone();
        return;
    }
    if let (Value::Mapping(dest_map), Value::Mapping(base_map)) = (overlay, base) {
        for (k, base_v) in base_map {
            match dest_map.get_mut(k) {
                None => {
                    dest_map.insert(k.clone(), base_v.clone());
                }
                Some(dest_v) if dest_v.is_null() => {
                    *dest_v = base_v.clone();
                }
                Some(dest_v) => {
                    if dest_v.is_mapping() && base_v.is_mapping() {
                        merge_yaml_underneath(dest_v, base_v);
                    }
                }
            }
        }
    }
}

fn ensure_mapping_at<'a>(map: &'a mut Mapping, path: &[&str]) -> &'a mut Value {
    let mut cur_map = map;
    for (idx, seg) in path.iter().enumerate() {
        let key = Value::String((*seg).to_string());
        if !cur_map.contains_key(&key) || cur_map.get(&key).is_some_and(Value::is_null) {
            cur_map.insert(key.clone(), Value::Mapping(Mapping::new()));
        }
        if idx + 1 == path.len() {
            return cur_map.get_mut(&key).unwrap();
        }
        cur_map = cur_map
            .get_mut(&key)
            .unwrap()
            .as_mapping_mut()
            .expect("path segment created as mapping");
    }
    unreachable!()
}

fn apply_common_storage_yaml(root_map: &mut Mapping, common_storage: &Value) {
    let targets: &[&[&str]] = &[
        &["blocks_storage"],
        &["ruler_storage"],
        &["alertmanager_storage"],
        &["usage_tracker", "snapshots_storage"],
    ];
    for path in targets {
        let target_val = ensure_mapping_at(root_map, path);
        merge_yaml_underneath(target_val, common_storage);
    }
}

fn apply_common_cluster_validation_yaml(root_map: &mut Mapping, common_ccv: &Value) {
    let targets: &[&[&str]] = &[
        &["ingester_client", "grpc_client_config", "cluster_validation"],
        &["frontend_worker", "grpc_client_config", "cluster_validation"],
        &[
            "frontend_worker",
            "query_scheduler_grpc_client_config",
            "cluster_validation",
        ],
        &[
            "compactor",
            "scheduler_client",
            "grpc_client_config",
            "cluster_validation",
        ],
        &[
            "block_builder",
            "scheduler_config",
            "grpc_client_config",
            "cluster_validation",
        ],
        &["frontend", "grpc_client_config", "cluster_validation"],
        &["querier", "store_gateway_client", "cluster_validation"],
        &["frontend", "client_cluster_validation"],
        &["query_scheduler", "grpc_client_config", "cluster_validation"],
        &["ruler", "ruler_client", "cluster_validation"],
        &["ruler", "distributor", "grpc_client_config", "cluster_validation"],
        &[
            "ruler",
            "query_frontend",
            "grpc_client_config",
            "cluster_validation",
        ],
        &["alertmanager", "alertmanager_client", "cluster_validation"],
        &[
            "distributor",
            "usage_tracker_client",
            "grpc_client_config",
            "cluster_validation",
        ],
        &["runtime_config", "http_client_cluster_validation"],
    ];

    for path in targets {
        let target_val = ensure_mapping_at(root_map, path);
        merge_yaml_underneath(target_val, common_ccv);
    }
}

/// Result of parsing CLI arguments and loading `-config.file`.
#[derive(Debug)]
pub struct LoadedConfig {
    pub config: MimirConfig,
    pub main_flags: MainFlags,
    pub config_hash_sha256: Option<String>,
    pub action: CliAction,
    pub usage_text: Option<String>,
    pub validation_error: Option<String>,
}

/// Flag target of the `cmd/mimir/main.go` command line (`mimir.Config` + `mainFlags`).
struct CliTarget {
    cfg: MimirConfig,
    main_flags: MainFlags,
}

/// Registers every command-line flag like `main()`: `cfg.RegisterFlags`, the ignored
/// `-config.file` / `-config.expand-env`, and `mainFlags.registerFlags`.
fn build_cli_flag_set(state: &CliTarget) -> FlagSet<CliTarget> {
    let mut fs: FlagSet<CliTarget> = FlagSet::new();
    state
        .cfg
        .register_flags_into(&mut fs, |t| &t.cfg, |t| &mut t.cfg);
    fs.ignored_flag("config.file", false, "Configuration file to load.");
    fs.ignored_flag(
        "config.expand-env",
        true,
        "Expands ${var} or $var in config according to the values of the environment variables.",
    );
    MainFlags::register_flags_into(
        &state.main_flags,
        &mut fs,
        |t| &t.main_flags,
        |t| &mut t.main_flags,
    );
    fs
}

/// Names of all flags accepted on the `mimir` command line.
pub fn cli_flag_names() -> Vec<String> {
    let state = CliTarget {
        cfg: MimirConfig::default(),
        main_flags: MainFlags::default(),
    };
    build_cli_flag_set(&state)
        .flag_names()
        .map(str::to_string)
        .collect()
}

/// Runs the full `cmd/mimir/main.go` startup config pipeline:
/// 1) Struct defaults
/// 2) Pre-scan `-config.file` & `-config.expand-env`, load YAML (computing SHA-256 pre-expansion)
/// 3) Parse CLI flags (overriding YAML)
/// 4) Handle `-help`/`-help-all`/`-version`/`-print.config`/`-modules`
/// 5) `inherit_common_flag_values`
/// 6) `validate()`
pub fn parse_cli_and_load_config(
    prog_name: &str,
    args: &[String],
) -> Result<LoadedConfig, ConfigError> {
    parse_cli_and_load_config_inner(prog_name, args, false)
}

/// Like [`parse_cli_and_load_config`], but mirrors Go `cmd/mimir/main.go` with `testMode = true`:
/// `cfg.Validate()` errors are recorded in `validation_error` instead of aborting before `DumpYaml`.
pub fn parse_cli_and_load_config_test_mode(
    prog_name: &str,
    args: &[String],
) -> Result<LoadedConfig, ConfigError> {
    parse_cli_and_load_config_inner(prog_name, args, true)
}

fn parse_cli_and_load_config_inner(
    prog_name: &str,
    args: &[String],
    test_mode: bool,
) -> Result<LoadedConfig, ConfigError> {
    let mut state = CliTarget {
        cfg: MimirConfig::default(),
        main_flags: MainFlags::default(),
    };

    let (config_file, expand_env) = parse_config_file_parameter(args);

    let config_hash_sha256 = if !config_file.is_empty() {
        Some(
            state
                .cfg
                .load_config_from_file(Path::new(&config_file), expand_env)
                .map_err(|e| ConfigError::LoadConfig {
                    path: config_file.clone(),
                    source: Box::new(e),
                })?,
        )
    } else {
        None
    };

    let mut fs = build_cli_flag_set(&state);

    fs.parse(&mut state, args)?;

    if state.main_flags.print_help || state.main_flags.print_help_all {
        let usage = fs.format_usage(prog_name, state.main_flags.print_help_all);
        return Ok(LoadedConfig {
            config: state.cfg,
            main_flags: state.main_flags.clone(),
            config_hash_sha256,
            action: CliAction::PrintHelp {
                all: state.main_flags.print_help_all,
            },
            usage_text: Some(usage),
            validation_error: None,
        });
    }

    if state.main_flags.print_version {
        return Ok(LoadedConfig {
            config: state.cfg,
            main_flags: state.main_flags,
            config_hash_sha256,
            action: CliAction::PrintVersion,
            usage_text: None,
            validation_error: None,
        });
    }

    MimirConfig::inherit_common_flag_values(&mut state, &mut fs)?;
    let validation_error = match state.cfg.validate() {
        Ok(()) => None,
        Err(err) if test_mode => Some(err.to_string()),
        Err(err) => return Err(err),
    };

    let action = if state.cfg.print_config {
        CliAction::PrintConfig
    } else if state.main_flags.print_modules {
        CliAction::PrintModules
    } else {
        CliAction::Run
    };

    Ok(LoadedConfig {
        config: state.cfg,
        main_flags: state.main_flags,
        config_hash_sha256,
        action,
        usage_text: None,
        validation_error,
    })
}

#[cfg(test)]
mod yaml_defaults_tests {
    use super::*;

    // Flags that Go registers but hides from -help-all (configdoc hidden overrides,
    // `doc:"hidden"` structs/fields, or `deprecated` placeholder values).
    const GO_HIDDEN_FLAGS: &[&str] = &[
        "blocks-storage.tsdb.block-ranges-period",
        "compactor.read-compartment-id",
        "compartments.enabled",
        "compartments.read.num-compartments",
        "compartments.write.num-compartments",
        "distributor.active-series-limit-response-code",
        "distributor.influx-endpoint-enabled",
        "distributor.ingestion-artificial-delay",
        "distributor.max-active-series-per-user",
        "distributor.max-influx-request-size",
        "distributor.otel-start-time-quiet-zero",
        "distributor.write-compartment-id",
        "ingester.push-grpc-method-enabled",
        "ingester.read-compartment-id",
        "server.grpc.recv-buffer-pools-enabled",
        "server.grpc.stats-tracking-enabled",
        "store-gateway.read-compartment-id",
    ];

    // Every flag Go documents in -help-all must be accepted by Rust, and Rust must not
    // accept flags Go doesn't register (hidden ones aside).
    #[test]
    fn cli_flags_match_go_help_all() {
        let rust: std::collections::BTreeSet<String> = cli_flag_names().into_iter().collect();
        let go: std::collections::BTreeSet<String> = crate::help::documented_flag_names()
            .into_iter()
            .map(str::to_string)
            .collect();
        let missing: Vec<_> = go.difference(&rust).collect();
        assert!(missing.is_empty(), "Go flags not registered in Rust: {missing:?}");
        let extra: Vec<_> = rust
            .difference(&go)
            .filter(|n| {
                !GO_HIDDEN_FLAGS.contains(&n.as_str())
                    && !n.starts_with("block-builder")
                    && !n.starts_with("usage-tracker.")
                    && !n.starts_with("distributor.usage-tracker-client.")
            })
            .collect();
        assert!(extra.is_empty(), "Rust flags unknown to Go -help-all: {extra:?}");
    }

    // The YAML document is merged onto the serialized current config, which therefore must
    // round-trip losslessly through serde (secrets revealed).
    #[test]
    fn core_config_serde_round_trip_is_lossless() {
        let d = CoreMimirConfig::default();
        let v = crate::secret::with_secrets_revealed(|| serde_yaml::to_value(&d)).unwrap();
        let back: CoreMimirConfig = serde_yaml::from_value(v).unwrap();
        assert_eq!(back, d);
    }

    // Go: yaml is unmarshalled on top of flag defaults, so `blocks_storage:` without
    // `filesystem.dir` keeps the `blocks` default (RegisterFlagsWithPrefixAndDefaultDirectory).
    #[test]
    fn nested_yaml_keeps_context_specific_defaults() {
        let mut cfg = MimirConfig::default();
        cfg.load_config_from_bytes(b"blocks_storage:\n  tsdb:\n    dir: /tmp/x\n", false)
            .unwrap();
        assert_eq!(cfg.blocks_storage.bucket.backend_cfg.filesystem.dir, "blocks");
        assert_eq!(cfg.blocks_storage.tsdb.dir, "/tmp/x");

        let mut cfg = MimirConfig::default();
        cfg.load_config_from_bytes(b"blocks_storage:\n  filesystem: {}\n", false)
            .unwrap();
        assert_eq!(cfg.blocks_storage.bucket.backend_cfg.filesystem.dir, "blocks");
    }

    #[test]
    fn nested_yaml_keeps_secrets_and_overrides() {
        let mut cfg = MimirConfig::default();
        cfg.ingest_storage.kafka.sasl_password = "keep".to_string();
        cfg.load_config_from_bytes(b"ingest_storage:\n  kafka:\n    topic: t\n", false)
            .unwrap();
        assert_eq!(cfg.ingest_storage.kafka.sasl_password, "keep");
        assert_eq!(cfg.ingest_storage.kafka.topic, "t");
    }

    #[test]
    fn validate_filesystem_paths_handles_leading_slash_prefix_and_non_ingester_dirs() {
        // Leading '/' in storage_prefix must join onto dir like Go filepath.Join, not discard dir.
        let mut cfg = MimirConfig {
            target: StringSliceCSV(vec!["ingester".to_string()]),
            ..Default::default()
        };
        cfg.blocks_storage.bucket.backend_cfg.backend = "filesystem".to_string();
        cfg.blocks_storage.bucket.backend_cfg.filesystem.dir = "/tmp/bucket".to_string();
        cfg.blocks_storage.bucket.storage_prefix = "/sub".to_string();
        cfg.blocks_storage.tsdb.dir = "/tmp/bucket/sub".to_string();
        let err = cfg.validate_filesystem_paths().unwrap_err().to_string();
        assert!(err.contains("cannot overlap"), "expected overlap error, got: {err}");

        // Non-overlapping when prefix resolves to /tmp/bucket/blocks and tsdb is /tmp/bucket/tsdb.
        cfg.blocks_storage.bucket.storage_prefix = "/blocks".to_string();
        cfg.blocks_storage.tsdb.dir = "/tmp/bucket/tsdb".to_string();
        cfg.validate_filesystem_paths().unwrap();

        // Store-gateway + compactor overlap check.
        let yaml = r#"
target: store-gateway,compactor
blocks_storage:
  bucket_store:
    sync_dir: /tmp/data
compactor:
  data_dir: /tmp/data/compactor
"#;
        let mut parsed: MimirConfig = serde_yaml::from_str(yaml).unwrap();
        let err = parsed.validate().unwrap_err().to_string();
        assert!(err.contains("bucket store sync directory"), "got: {err}");
    }

    #[test]
    fn w19_mod_05_vault_validate_runs_before_unsupported_guard() {
        let validate_yaml = |yaml: &str| {
            let mut cfg = MimirConfig::default();
            cfg.load_config_from_bytes(yaml.as_bytes(), false).unwrap();
            cfg.validate().unwrap_err().to_string()
        };

        assert_eq!(
            validate_yaml("target: ingester
vault:
  enabled: true
"),
            "error validating config: invalid vault config: empty vault URL supplied"
        );
        assert_eq!(
            validate_yaml("target: ingester
vault:
  enabled: true
  url: http://vault:8200
"),
            "error validating config: invalid vault config: empty vault mount path supplied"
        );
        assert_eq!(
            validate_yaml(
                "target: ingester
vault:
  enabled: true
  url: http://vault:8200
  mount_path: secret
"
            ),
            "error validating config: invalid vault config: invalid vault auth supplied: unsupported auth method"
        );
        assert_eq!(
            validate_yaml(
                "target: ingester
vault:
  enabled: true
  url: http://vault:8200
  mount_path: secret
  auth:
    type: ldap
"
            ),
            "error validating config: invalid vault config: invalid vault auth supplied: unsupported auth method"
        );
        for auth in ["approle", "kubernetes", "userpass", "token"] {
            let yaml = format!(
                "target: ingester
vault:
  enabled: true
  url: http://vault:8200
  mount_path: secret
  auth:
    type: {auth}
"
            );
            assert_eq!(
                validate_yaml(&yaml),
                "error validating config: vault integration (-vault.enabled) is not supported by mimir-rs"
            );
        }

        // CLI flags override missing YAML fields cleanly.
        let err = parse_cli_and_load_config(
            "mimir",
            &[
                "-target=ingester".to_string(),
                "-vault.enabled".to_string(),
                "-vault.url=http://vault:8200".to_string(),
                "-vault.mount-path=secret".to_string(),
                "-vault.auth.type=token".to_string(),
            ],
        )
        .unwrap_err()
        .to_string();
        assert_eq!(
            err,
            "error validating config: vault integration (-vault.enabled) is not supported by mimir-rs"
        );
    }

    #[test]
    fn w21_aps_04_yaml_null_zeroing_and_scalar_struct_preservation() {
        let dir = tempfile::tempdir().unwrap();
        let write_and_load = |name: &str, yaml: &str, extra_args: &[&str]| {
            let path = dir.path().join(name);
            std::fs::write(&path, yaml).unwrap();
            let mut args = vec![format!("-config.file={}", path.display())];
            args.extend(extra_args.iter().map(|s| (*s).to_string()));
            parse_cli_and_load_config("mimir", &args)
        };

        // 1. target: null zeroes StringSliceCSV to [].
        let loaded = write_and_load("target_null.yaml", "target: null
", &[]).unwrap();
        assert!(loaded.config.target.0.is_empty(), "target={:?}", loaded.config.target.0);

        // 2. blocks_storage.tsdb.block_ranges_period: null zeroes DurationList and fails validation.
        let err = write_and_load(
            "ranges_null.yaml",
            "target: ingester
blocks_storage:
  tsdb:
    block_ranges_period: null
",
            &[],
        )
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("invalid TSDB config: empty block ranges for TSDB"),
            "unexpected err: {err}"
        );

        // 3. ingester.ring.instance_interface_names: null zeroes []string to [].
        // 4. limits.otel_metric_suffixes_enabled: null zeroes *bool to None;
        //    limits.drop_labels: null zeroes []string to [].
        // 5. limits.max_label_name_length: null (scalar) keeps default (1024).
        let loaded = write_and_load(
            "fields_null.yaml",
            r#"
target: ingester
ingester:
  ring:
    instance_interface_names: null
limits:
  otel_metric_suffixes_enabled: null
  drop_labels: null
  max_label_name_length: null
"#,
            &[],
        )
        .unwrap();
        assert!(loaded.config.ingester.ring.instance_interface_names.is_empty());
        assert_eq!(loaded.config.limits.otel_metric_suffixes_enabled, None);
        assert!(loaded.config.limits.drop_labels.is_empty());
        assert_eq!(loaded.config.limits.max_label_name_length, 1024);

        // 6. Struct-level null (limits: null) preserves flag defaults.
        let loaded = write_and_load(
            "struct_null.yaml",
            "target: ingester
limits: null
",
            &[],
        )
        .unwrap();
        assert_eq!(loaded.config.limits.otel_metric_suffixes_enabled, Some(false));
        assert_eq!(loaded.config.limits.max_label_name_length, 1024);
    }

    #[test]
    fn w21_aps_05_querier_write_timeout_tsdb_guard_and_error_texts() {
        // 1. querier.timeout > server.http_server_write_timeout rejected for all targets.
        let err = parse_cli_and_load_config(
            "mimir",
            &[
                "-target=ingester".to_string(),
                "-server.http-write-timeout=30s".to_string(),
            ],
        )
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("querier timeout (2m0s) must be lower than or equal to HTTP server write timeout (30s)"),
            "unexpected err: {err}"
        );

        // Equal timeouts pass.
        assert!(parse_cli_and_load_config(
            "mimir",
            &[
                "-target=ingester".to_string(),
                "-server.http-write-timeout=30s".to_string(),
                "-querier.timeout=30s".to_string(),
            ],
        )
        .is_ok());

        // 2. TSDBConfig::validate_mimir_rs_support is gated on is_ingester_enabled().
        assert!(parse_cli_and_load_config(
            "mimir",
            &[
                "-target=memberlist-kv".to_string(),
                "-blocks-storage.tsdb.block-ranges-period=2h,12h".to_string(),
                "-blocks-storage.tsdb.memory-snapshot-on-shutdown=true".to_string(),
            ],
        )
        .is_ok());
        assert!(parse_cli_and_load_config(
            "mimir",
            &[
                "-target=ingester".to_string(),
                "-blocks-storage.tsdb.block-ranges-period=2h,12h".to_string(),
            ],
        )
        .is_err());

        // 3. Compartment write-placeholder error matches Go verbatim.
        let err = parse_cli_and_load_config(
            "mimir",
            &[
                "-target=ingester".to_string(),
                "-ingest-storage.enabled=true".to_string(),
                "-compartments.enabled=true".to_string(),
                "-compartments.read.num-compartments=1".to_string(),
                "-compartments.write.num-compartments=2".to_string(),
                "-ingest-storage.kafka.topic=ingest".to_string(),
                "-ingest-storage.kafka.address=127.0.0.1:9092".to_string(),
            ],
        )
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("for the ingester and query-frontend"),
            "unexpected err: {err}"
        );

        // 4. Azure same-bucket/prefix error uses "the ones used in blocks storage config".
        let err = parse_cli_and_load_config(
            "mimir",
            &[
                "-target=ruler".to_string(),
                "-blocks-storage.backend=azure".to_string(),
                "-blocks-storage.azure.container-name=c".to_string(),
                "-blocks-storage.azure.account-name=a".to_string(),
                "-blocks-storage.azure.account-key=a2V5".to_string(),
                "-ruler-storage.backend=azure".to_string(),
                "-ruler-storage.azure.container-name=c".to_string(),
                "-ruler-storage.azure.account-name=a".to_string(),
                "-ruler-storage.azure.account-key=a2V5".to_string(),
            ],
        )
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("cannot be the same as the ones used in blocks storage config"),
            "unexpected err: {err}"
        );
    }
}
