//! CLI flag parsing, strict YAML config loading, env expansion, common inheritance, and tenant overrides.
//!
//! Ported from:
//! - github.com/grafana/mimir/cmd/mimir/main.go
//! - github.com/grafana/mimir/pkg/mimir/mimir.go
//! - github.com/grafana/mimir/pkg/ingester/ingester.go
//! - github.com/grafana/mimir/pkg/storage/tsdb/config.go
//! - github.com/grafana/mimir/pkg/storage/bucket/config.go
//! - github.com/grafana/mimir/pkg/storage/ingest/config.go
//! - github.com/grafana/mimir/pkg/util/validation/limits.go
//! - github.com/grafana/mimir/pkg/util/runtimeconfig

pub mod duration;
pub mod env;
pub mod error;
pub mod flags;
pub mod help;
pub mod ingester;
pub mod limits;
pub mod mimir;
pub mod runtime_config;
pub mod schema;
mod schema_hidden;
pub mod secret;
pub mod server;
pub mod sha256;
pub mod storage;
pub mod types;
pub mod yaml_v3_emit;
pub mod yaml_v3;

pub use duration::{
    format_go_duration, format_model_duration, parse_duration, parse_go_duration,
    parse_model_duration, GoDuration, ModelDuration,
};
pub use env::{expand_environment_variables, expand_environment_variables_with};
pub use error::{CliAction, ConfigError};
pub use flags::{
    format_modules, format_version, go_version_str, is_targetable_module, parse_config_file_parameter, FlagSet,
    MainFlags, USER_VISIBLE_MODULES,
};
pub use ingester::{
    ActiveSeriesConfig, BackoffConfig, CircuitBreakerConfig, ClusterValidationConfig, ConsulConfig,
    EtcdConfig, GRPCClientConfig, IngesterClientConfig, IngesterConfig, InstanceLimits,
    KVStoreConfig, LabelValuesCountConfig, MultiKVConfig, PartitionRingConfig, PrioritizerConfig,
    ReactiveLimiterConfig, RingConfig,
};
pub use limits::{
    all_true_booleans_per_tenant, clear_registered_extensions, handle_user_limits_request,
    is_limit_error, largest_positive_non_zero_duration_per_tenant, max_duration_per_tenant,
    min_duration_per_tenant, must_register_extension, new_limit_error, parse_float_chunk_encoding,
    parse_histogram_st_encoding_enabled, smallest_positive_int_per_tenant,
    smallest_positive_non_zero_duration_per_tenant, smallest_positive_non_zero_int_per_tenant,
    validate_blocked_queries, BlockedQueriesConfig, BlockedQuery, BlockedRequest,
    BlockedRequestQueryParam, CostAttributionLabel, CostAttributionTrackerConfig,
    CostAttributionTrackerConfigs, FloatChunkEncoding, LimitError, LimitedQuery, LimitsConfig,
    LimitsExtensionDefault, MetricRelabelConfig, NotificationRateLimitMap, OAuth2ClientConfig,
    RulerAlertmanagerClientConfig, UserLimitsResponse, DEFAULT_FLOAT_CHUNK_ENCODING_VALUE,
    DEFAULT_HISTOGRAM_CHUNK_ENCODING_VALUE, FLOAT_CHUNK_ENCODING_VALUES,
    HISTOGRAM_CHUNK_ENCODING_ST, HISTOGRAM_CHUNK_ENCODING_VALUES, LABEL_VALUE_HASH_LEN,
};
pub use mimir::{
    parse_cli_and_load_config, parse_cli_and_load_config_test_mode, CommonConfig,
    InstrumentRefLeaksConfig, LoadedConfig, MimirConfig,
};
pub use runtime_config::{
    is_url, merge_limits, merge_limits_in_place, parse_runtime_config_yaml,
    parse_runtime_config_yaml_with_defaults, parse_tenant_id_with_metadata,
    runtime_config_source_ids, DistributorInstanceLimits, MultiRuntimeConfig, Overrides,
    RuntimeConfigHttpObserver, RuntimeConfigLoadEvent, RuntimeConfigManager,
    RuntimeConfigManagerOptions, RuntimeConfigObserver, RuntimeConfigSourceStatus,
    RuntimeConfigValues,
};
pub use server::{
    ActivityTrackerConfig, ClusterValidationProtocolConfig, ClusterValidationProtocolConfigForHTTP,
    CompartmentsConfig, CompartmentsReadConfig, CompartmentsWriteConfig, MemberlistConfig,
    PropagationDelayTrackerConfig, RuntimeConfigConfig, ServerClusterValidationConfig,
    ServerConfig, TLSConfig, UsageStatsConfig, ZoneAwareRoutingConfig,
};
pub use sha256::{sha256, sha256_hex};
pub use storage::{
    AzureConfig, BlocksStorageConfig, BucketConfig, BucketHTTPConfig, BucketIndexConfig,
    BucketStoreConfig, ChunksCacheConfig, FilesystemConfig, GCSConfig, InMemoryIndexCacheConfig,
    IndexCacheConfig, IndexHeaderCacheConfig, IndexHeaderConfig, IndexLookupPlanningConfig,
    IngestStorageConfig, KafkaConfig, MemcachedClientConfig, MetadataCacheConfig, MigrationConfig,
    OffsetCatalogueConfig, OrderedConsumptionConfig, S3Config, S3SSEConfig, S3TraceConfig,
    S3_SUPPORTED_STORAGE_CLASSES,
    StorageBackendConfig, SwiftConfig, TSDBConfig,
};
pub use types::{
    parse_go_bool, parse_go_yaml_timestamp, parse_go_yaml_timestamp_dt, CIDRSliceCSV, HumanBytes,
    LabelValueLengthOverLimitStrategy, LimitsMap, LogLevel, NameValidationScheme,
    OTelTranslationStrategy, S3BucketLookupType, SASLMechanism, StringSliceCSV,
    StringSliceCSVMulti,
};
