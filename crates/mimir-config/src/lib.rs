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
pub mod ingester;
pub mod limits;
pub mod mimir;
pub mod runtime_config;
pub mod schema;
pub mod secret;
pub mod server;
pub mod sha256;
pub mod storage;
pub mod types;

pub use duration::{
    format_go_duration, format_model_duration, parse_duration, parse_go_duration,
    parse_model_duration, GoDuration, ModelDuration,
};
pub use env::{expand_environment_variables, expand_environment_variables_with};
pub use error::{CliAction, ConfigError};
pub use flags::{
    format_modules, format_version, is_targetable_module, parse_config_file_parameter, FlagSet,
    MainFlags, USER_VISIBLE_MODULES,
};
pub use ingester::{
    ActiveSeriesConfig, BackoffConfig, CircuitBreakerConfig, ClusterValidationConfig, ConsulConfig,
    EtcdConfig, GRPCClientConfig, IngesterClientConfig, IngesterConfig, InstanceLimits,
    KVStoreConfig, LabelValuesCountConfig, MultiKVConfig, PartitionRingConfig, PrioritizerConfig,
    ReactiveLimiterConfig, RingConfig,
};
pub use limits::{
    BlockedQuery, BlockedRequest, BlockedRequestQueryParam, CostAttributionLabel,
    CostAttributionTrackerConfig, CostAttributionTrackerConfigs, LimitedQuery, LimitsConfig,
    MetricRelabelConfig, OAuth2ClientConfig, RulerAlertmanagerClientConfig,
};
pub use mimir::{parse_cli_and_load_config, CommonConfig, LoadedConfig, MimirConfig};
pub use runtime_config::{
    parse_runtime_config_yaml, DistributorInstanceLimits, MultiRuntimeConfig, Overrides,
    RuntimeConfigManager, RuntimeConfigValues,
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
    StorageBackendConfig, SwiftConfig, TSDBConfig,
};
pub use types::{
    parse_go_bool, HumanBytes, LabelValueLengthOverLimitStrategy, LimitsMap, LogLevel,
    NameValidationScheme, OTelTranslationStrategy, S3BucketLookupType, SASLMechanism,
    StringSliceCSV, StringSliceCSVMulti,
};
