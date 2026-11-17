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
use crate::error::{CliAction, ConfigError};
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

    /// Other top-level Mimir sections (`api`, `distributor`, `querier`, `frontend`, `ruler`, etc.)
    /// validated against the upstream schema so full-cluster `mimir.yaml` files load cleanly.
    #[serde(flatten)]
    pub extra_sections: BTreeMap<String, Value>,

    #[serde(skip)]
    extra_cli_flag_values: BTreeMap<String, String>,
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
            extra_sections: BTreeMap::new(),
            extra_cli_flag_values: BTreeMap::new(),
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
        self.common
            .register_flags_into(f, move |t| &get(t).common, move |t| &mut get_mut(t).common);

        register_extra_flags(
            f,
            move |t| &get(t).extra_cli_flag_values,
            move |t| &mut get_mut(t).extra_cli_flag_values,
        );
    }

    /// Loads YAML configuration from `path`, returning the SHA-256 hex digest of raw bytes BEFORE env expansion.
    pub fn load_config_from_file(
        &mut self,
        path: &Path,
        expand_env: bool,
    ) -> Result<String, ConfigError> {
        let raw = std::fs::read(path)?;
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

        let mut root: Value = serde_yaml::from_slice(bytes_to_parse)
            .map_err(|e| ConfigError::Yaml(e.to_string()))?;

        if root.is_null() {
            return Ok(hash_hex);
        }

        root.apply_merge()
            .map_err(|e| ConfigError::Yaml(e.to_string()))?;

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
        sanitize_null_block_values(&mut core_val);
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

        let core: CoreMimirConfig = serde_yaml::from_value(merged_core)
            .map_err(|e| ConfigError::Yaml(e.to_string()))?;
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
                if fs.get_value(target, &dest).as_deref() == Some(origin_val.as_str()) {
                    continue;
                }
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
                    if fs.get_value(target, dest).as_deref() == Some(origin_val.as_str()) {
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

    /// Validates the full configuration, matching `pkg/mimir/mimir.go:Config.Validate`.
    pub fn validate(&mut self) -> Result<(), ConfigError> {

        self.server.validate()?;
        self.validate_filesystem_paths()?;
        self.ingest_storage.validate()?;
        self.compartments.validate()?;

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
                            "when compartments are enabled with more than one write compartment, every -ingest-storage.kafka.address must contain the \"<write-compartment-id>\" placeholder".to_string(),
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

        self.memberlist.validate()?;
        self.blocks_storage
            .validate(self.ingester.active_series.active_series_metrics_enabled)?;
        self.ingester_client.validate()?;

        if let Err(err) = self.ingester.validate(
            self.compartments.enabled,
            self.compartments.read.num_compartments,
        ) {
            // Only fail on spread-minimizing error if ingester is targeted.
            let is_spread_min = err.to_string().contains("spread-minimizing");
            if self.is_ingester_enabled() || !is_spread_min {
                return Err(err);
            }
        }

        self.usage_stats.validate()?;
        self.limits.validate()?;

        Ok(())
    }

    fn validate_filesystem_paths(&self) -> Result<(), ConfigError> {
        struct PathEntry<'a> {
            name: &'static str,
            cfg_value: &'a str,
            check_value: PathBuf,
        }

        let mut entries: Vec<PathEntry<'_>> = Vec::new();

        if (self.is_ingester_enabled()
            || self.is_querier_enabled()
            || self.is_store_gateway_enabled()
            || self.is_compactor_enabled()
            || self.is_ruler_enabled())
            && self.blocks_storage.bucket.backend_cfg.backend == "filesystem"
        {
            let dir = &self.blocks_storage.bucket.backend_cfg.filesystem.dir;
            let joined = if self.blocks_storage.bucket.storage_prefix.is_empty() {
                PathBuf::from(dir)
            } else {
                Path::new(dir).join(&self.blocks_storage.bucket.storage_prefix)
            };
            entries.push(PathEntry {
                name: "blocks storage filesystem directory",
                cfg_value: dir,
                check_value: joined,
            });
        }

        if self.is_ingester_enabled() {
            entries.push(PathEntry {
                name: "tsdb directory",
                cfg_value: &self.blocks_storage.tsdb.dir,
                check_value: PathBuf::from(&self.blocks_storage.tsdb.dir),
            });
        }

        if self.is_store_gateway_enabled() {
            entries.push(PathEntry {
                name: "bucket store sync directory",
                cfg_value: &self.blocks_storage.bucket_store.sync_dir,
                check_value: PathBuf::from(&self.blocks_storage.bucket_store.sync_dir),
            });
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

fn sanitize_null_block_values(val: &mut Value) {
    if let Value::Mapping(map) = val {
        let keys: Vec<Value> = map.keys().cloned().collect();
        for k in keys {
            if let Some(v) = map.get_mut(&k) {
                match v {
                    Value::Null => {
                        map.remove(&k);
                    }
                    Value::Mapping(_) => sanitize_null_block_values(v),
                    _ => {}
                }
            }
        }
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
    struct CliTarget {
        cfg: MimirConfig,
        main_flags: MainFlags,
    }

    let mut state = CliTarget {
        cfg: MimirConfig::default(),
        main_flags: MainFlags::default(),
    };

    let (config_file, expand_env) = parse_config_file_parameter(args);

    let config_hash_sha256 = if !config_file.is_empty() {
        Some(
            state
                .cfg
                .load_config_from_file(Path::new(&config_file), expand_env)?,
        )
    } else {
        None
    };

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
        });
    }

    if state.main_flags.print_version {
        return Ok(LoadedConfig {
            config: state.cfg,
            main_flags: state.main_flags,
            config_hash_sha256,
            action: CliAction::PrintVersion,
            usage_text: None,
        });
    }

    MimirConfig::inherit_common_flag_values(&mut state, &mut fs)?;
    state.cfg.validate()?;

    let action = if state.cfg.print_config {
        CliAction::PrintConfig
    } else if state.main_flags.print_modules {
        CliAction::PrintModules
    } else {
        state.cfg.validate_targets()?;
        CliAction::Run
    };

    Ok(LoadedConfig {
        config: state.cfg,
        main_flags: state.main_flags,
        config_hash_sha256,
        action,
        usage_text: None,
    })
}

#[cfg(test)]
mod yaml_defaults_tests {
    use super::*;

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
}
