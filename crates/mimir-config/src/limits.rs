//! Per-tenant limits configuration and validation.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/util/validation/limits.go
//! - github.com/grafana/mimir/pkg/util/validation/blocked_query.go
//! - github.com/grafana/mimir/pkg/util/validation/limited_query.go
//! - github.com/grafana/mimir/pkg/util/validation/blocked_request.go
//! - github.com/grafana/mimir/pkg/costattribution/costattributionmodel/*

use std::collections::BTreeMap;
use std::sync::OnceLock;

use serde::{Deserialize, Serialize};

use crate::duration::{GoDuration, ModelDuration};
use crate::error::ConfigError;
use crate::flags::FlagSet;
use crate::types::{
    parse_go_yaml_timestamp, CIDRSliceCSV, LabelValueLengthOverLimitStrategy, LimitsMap,
    NameValidationScheme, OTelTranslationStrategy, StringSliceCSV,
};

pub const MIN_COMPACTOR_PARTIAL_BLOCK_DELETION_DELAY_NANOS: i64 = 4 * 3600 * 1_000_000_000;
pub const LABEL_VALUE_HASH_LEN: usize = "(hash:)".len() + 32 * 2;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BlockedQuery {
    pub pattern: String,
    pub regex: bool,
    pub reason: String,
    pub unaligned_range_queries: bool,
    pub time_range_longer_than: ModelDuration,
    pub step_size_shorter_than: ModelDuration,
    pub id: String,
    pub note: String,
    pub created_by: String,
    pub created_at: Option<String>,
    pub expires_at: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LimitedQuery {
    pub query: String,
    pub allowed_frequency: GoDuration,
    pub reason: String,
    pub id: String,
    pub note: String,
    pub created_by: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BlockedRequestQueryParam {
    pub value: String,
    pub is_regexp: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BlockedRequest {
    pub path: String,
    pub method: String,
    pub query_params: BTreeMap<String, BlockedRequestQueryParam>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CostAttributionLabel {
    pub input: String,
    pub output: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CostAttributionTrackerConfig {
    pub labels: Vec<CostAttributionLabel>,
    pub internal: bool,
}

pub type CostAttributionTrackerConfigs = BTreeMap<String, CostAttributionTrackerConfig>;

fn csv_is_empty(v: &StringSliceCSV) -> bool {
    v.0.is_empty()
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MetricRelabelConfig {
    pub source_labels: Vec<String>,
    pub separator: Option<String>,
    pub regex: Option<String>,
    pub modulus: u64,
    pub target_label: Option<String>,
    pub replacement: Option<String>,
    pub action: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct OAuth2ClientConfig {
    pub client_id: String,
    #[serde(serialize_with = "crate::secret::serialize_flagext_secret")]
    pub client_secret: String,
    pub token_url: String,
    // yaml:"scopes,omitempty"
    #[serde(skip_serializing_if = "csv_is_empty")]
    pub scopes: StringSliceCSV,
    pub endpoint_params: LimitsMap<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RulerAlertmanagerClientConfig {
    pub alertmanager_url: String,
    pub tls_enabled: bool,
    pub tls_cert_path: String,
    pub tls_key_path: String,
    pub tls_ca_path: String,
    pub tls_server_name: String,
    pub tls_insecure_skip_verify: bool,
    pub tls_cipher_suites: String,
    pub tls_min_version: String,
    pub basic_auth_username: String,
    #[serde(serialize_with = "crate::secret::serialize_flagext_secret")]
    pub basic_auth_password: String,
    pub oauth2: OAuth2ClientConfig,
    pub proxy_url: String,
}

impl Default for RulerAlertmanagerClientConfig {
    fn default() -> Self {
        Self {
            alertmanager_url: String::new(),
            tls_enabled: true,
            tls_cert_path: String::new(),
            tls_key_path: String::new(),
            tls_ca_path: String::new(),
            tls_server_name: String::new(),
            tls_insecure_skip_verify: false,
            tls_cipher_suites: String::new(),
            tls_min_version: String::new(),
            basic_auth_username: String::new(),
            basic_auth_password: String::new(),
            oauth2: OAuth2ClientConfig::default(),
            proxy_url: String::new(),
        }
    }
}

impl RulerAlertmanagerClientConfig {
    pub fn register_flags_into<T: 'static>(
        &self,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        f.string_var("ruler.alertmanager-url", &self.alertmanager_url, "Comma-separated list of Alertmanager URLs to send notifications to.", move |t| &get(t).alertmanager_url, move |t| &mut get_mut(t).alertmanager_url);
        f.bool_var("ruler.alertmanager-client.tls-enabled", self.tls_enabled, "Enable TLS for Alertmanager client.", move |t| &get(t).tls_enabled, move |t| &mut get_mut(t).tls_enabled);
        f.string_var("ruler.alertmanager-client.tls-cert-path", &self.tls_cert_path, "Path to the client certificate file for Alertmanager client.", move |t| &get(t).tls_cert_path, move |t| &mut get_mut(t).tls_cert_path);
        f.string_var("ruler.alertmanager-client.tls-key-path", &self.tls_key_path, "Path to the client key file for Alertmanager client.", move |t| &get(t).tls_key_path, move |t| &mut get_mut(t).tls_key_path);
        f.string_var("ruler.alertmanager-client.tls-ca-path", &self.tls_ca_path, "Path to the CA certificate file for Alertmanager client.", move |t| &get(t).tls_ca_path, move |t| &mut get_mut(t).tls_ca_path);
        f.string_var("ruler.alertmanager-client.tls-server-name", &self.tls_server_name, "Override the server name for Alertmanager client TLS.", move |t| &get(t).tls_server_name, move |t| &mut get_mut(t).tls_server_name);
        f.bool_var("ruler.alertmanager-client.tls-insecure-skip-verify", self.tls_insecure_skip_verify, "Skip TLS certificate verification for Alertmanager client.", move |t| &get(t).tls_insecure_skip_verify, move |t| &mut get_mut(t).tls_insecure_skip_verify);
        f.string_var("ruler.alertmanager-client.tls-cipher-suites", &self.tls_cipher_suites, "Comma-separated list of TLS cipher suites for Alertmanager client.", move |t| &get(t).tls_cipher_suites, move |t| &mut get_mut(t).tls_cipher_suites);
        f.string_var("ruler.alertmanager-client.tls-min-version", &self.tls_min_version, "Minimum TLS version for Alertmanager client.", move |t| &get(t).tls_min_version, move |t| &mut get_mut(t).tls_min_version);
        f.string_var("ruler.alertmanager-client.basic-auth-username", &self.basic_auth_username, "Basic auth username for Alertmanager client.", move |t| &get(t).basic_auth_username, move |t| &mut get_mut(t).basic_auth_username);
        f.string_var("ruler.alertmanager-client.basic-auth-password", &self.basic_auth_password, "Basic auth password for Alertmanager client.", move |t| &get(t).basic_auth_password, move |t| &mut get_mut(t).basic_auth_password);
        f.string_var("ruler.alertmanager-client.oauth.client_id", &self.oauth2.client_id, "OAuth2 client ID for Alertmanager client.", move |t| &get(t).oauth2.client_id, move |t| &mut get_mut(t).oauth2.client_id);
        f.string_var("ruler.alertmanager-client.oauth.client_secret", &self.oauth2.client_secret, "OAuth2 client secret for Alertmanager client.", move |t| &get(t).oauth2.client_secret, move |t| &mut get_mut(t).oauth2.client_secret);
        f.string_var("ruler.alertmanager-client.oauth.token_url", &self.oauth2.token_url, "OAuth2 token URL for Alertmanager client.", move |t| &get(t).oauth2.token_url, move |t| &mut get_mut(t).oauth2.token_url);
        f.string_slice_csv_var("ruler.alertmanager-client.oauth.scopes", &self.oauth2.scopes, "OAuth2 scopes for Alertmanager client.", move |t| &get(t).oauth2.scopes, move |t| &mut get_mut(t).oauth2.scopes);
        f.limits_map_var("ruler.alertmanager-client.oauth.endpoint-params", &self.oauth2.endpoint_params, "OAuth2 endpoint params for Alertmanager client.", move |t| &get(t).oauth2.endpoint_params, move |t| &mut get_mut(t).oauth2.endpoint_params);
        f.string_var("ruler.alertmanager-client.proxy-url", &self.proxy_url, "HTTP proxy URL for Alertmanager client.", move |t| &get(t).proxy_url, move |t| &mut get_mut(t).proxy_url);
    }
}

/// Complete per-tenant limits configuration matching `pkg/util/validation/limits.go:Limits`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LimitsConfig {
    // Distributor enforced limits.
    pub max_active_series_per_user: i64,
    pub active_series_limit_response_code: i64,
    pub request_rate: f64,
    pub request_burst_size: i64,
    pub ingestion_rate: f64,
    pub ingestion_burst_size: i64,
    pub ingestion_burst_factor: f64,
    pub accept_ha_samples: bool,
    pub ha_tracker_per_sample_dedupe: bool,
    pub merge_duplicate_timeseries: bool,
    pub ha_cluster_label: String,
    pub ha_replica_label: String,
    pub ha_max_clusters: i64,
    pub ha_tracker_update_timeout: ModelDuration,
    pub ha_tracker_update_timeout_jitter_max: ModelDuration,
    pub ha_tracker_failover_timeout: ModelDuration,
    pub ha_tracker_sample_failover_timeout: ModelDuration,
    pub drop_labels: Vec<String>,
    pub max_label_name_length: i64,
    pub max_label_value_length: i64,
    pub label_value_length_over_limit_strategy: LabelValueLengthOverLimitStrategy,
    pub blocked_label_names_for_label_value_bytes: StringSliceCSV,
    pub max_label_names_per_series: i64,
    pub max_label_names_per_info_series: i64,
    pub max_metadata_length: i64,
    pub max_native_histogram_buckets: i64,
    pub max_exemplars_per_series_per_request: i64,
    pub reduce_native_histogram_over_max_buckets: bool,
    pub creation_grace_period: ModelDuration,
    pub past_grace_period: ModelDuration,
    pub enforce_out_of_order_window_on_distributor: bool,
    pub enforce_metadata_metric_name: bool,
    pub ingestion_tenant_shard_size: i64,
    // Go `yaml:"...,omitempty"` (also for the other `skip_serializing_if` fields below).
    #[serde(skip_serializing_if = "Vec::is_empty", deserialize_with = "deserialize_metric_relabel_configs")]
    pub metric_relabel_configs: Vec<MetricRelabelConfig>,
    pub ingestion_artificial_delay: ModelDuration,

    // Ingester enforced limits.
    pub max_global_series_per_user: i64,
    pub max_global_series_per_metric: i64,
    pub label_value_bytes_metric_threshold: i64,
    pub max_global_metadata_per_user: i64,
    pub max_global_metadata_per_metric: i64,
    pub max_global_exemplars_per_user: i64,
    pub ignore_ooo_exemplars: bool,
    pub early_head_compaction_owned_series_threshold: i64,
    pub early_head_compaction_min_estimated_series_reduction_percentage: i64,
    pub native_histograms_ingestion_enabled: bool,
    pub float_chunk_encoding: String,
    pub histogram_chunk_encoding: String,
    pub active_series_custom_trackers: LimitsMap<String>,
    pub active_series_additional_custom_trackers: LimitsMap<String>,
    pub max_active_series_additional_custom_trackers: i64,
    pub out_of_order_time_window: ModelDuration,
    pub out_of_order_blocks_external_label_enabled: bool,
    pub separate_metrics_group_label: String,

    // Querier enforced limits.
    pub max_fetched_chunks_per_query: i64,
    pub max_estimated_fetched_chunks_per_query_multiplier: f64,
    pub max_fetched_series_per_query: i64,
    pub max_fetched_chunk_bytes_per_query: i64,
    pub max_estimated_memory_consumption_per_query: u64,
    pub max_blocks_per_store_request: i64,
    pub max_query_lookback: ModelDuration,
    pub max_partial_query_length: ModelDuration,
    pub max_query_parallelism: i64,
    pub max_labels_query_length: ModelDuration,
    pub max_series_query_limit: i64,
    pub max_label_names_limit: i64,
    pub max_label_values_limit: i64,
    pub max_cache_freshness: ModelDuration,
    pub max_queriers_per_tenant: i64,
    pub query_sharding_total_shards: i64,
    pub query_sharding_max_sharded_queries: i64,
    pub query_sharding_max_regexp_size_bytes: i64,
    pub cardinality_sharding_max_sharded_queries: i64,
    pub query_ingesters_within: ModelDuration,
    pub enable_delayed_name_removal: bool,

    // Query-frontend limits.
    pub max_total_query_length: ModelDuration,
    pub results_cache_ttl: ModelDuration,
    pub results_cache_ttl_for_out_of_order_time_window: ModelDuration,
    pub results_cache_ttl_for_cardinality_query: ModelDuration,
    pub results_cache_ttl_for_labels_query: ModelDuration,
    pub results_cache_ttl_for_errors: ModelDuration,
    pub cache_unaligned_requests: bool,
    pub max_query_expression_size_bytes: i64,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub blocked_queries: Vec<BlockedQuery>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub limited_queries: Vec<LimitedQuery>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub blocked_requests: Vec<BlockedRequest>,
    pub align_queries_with_step: bool,
    pub enabled_promql_experimental_functions: StringSliceCSV,
    pub enabled_promql_extended_range_selectors: StringSliceCSV,
    pub enabled_promql_binop_fill_modifiers: StringSliceCSV,
    pub prom2_range_compat: bool,
    pub subquery_spin_off_enabled: bool,
    pub labels_query_optimizer_enabled: bool,

    // Cardinality.
    pub cardinality_analysis_enabled: bool,
    pub label_names_and_values_results_max_size_bytes: i64,
    pub label_values_max_cardinality_label_names_per_request: i64,
    pub cardinality_analysis_max_results: i64,
    pub active_series_results_max_size_bytes: i64,

    // Cost attribution.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub cost_attribution_labels_structured: Vec<CostAttributionLabel>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub cost_attribution_trackers: CostAttributionTrackerConfigs,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub additional_cost_attribution_trackers: CostAttributionTrackerConfigs,
    pub cost_attribution_cooldown: ModelDuration,
    pub max_cost_attribution_cardinality: i64,

    // Ruler.
    pub ruler_evaluation_delay_duration: ModelDuration,
    pub ruler_evaluation_consistency_max_delay: ModelDuration,
    pub ruler_tenant_shard_size: i64,
    pub ruler_max_rules_per_rule_group: i64,
    pub ruler_max_rule_groups_per_tenant: i64,
    pub ruler_recording_rules_evaluation_enabled: bool,
    pub ruler_alerting_rules_evaluation_enabled: bool,
    pub ruler_sync_rules_on_changes_enabled: bool,
    pub ruler_max_rules_per_rule_group_by_namespace: LimitsMap<i64>,
    pub ruler_max_rule_groups_per_tenant_by_namespace: LimitsMap<i64>,
    pub ruler_protected_namespaces: StringSliceCSV,
    pub ruler_max_independent_rule_evaluation_concurrency_per_tenant: i64,
    pub ruler_alertmanager_client_config: RulerAlertmanagerClientConfig,
    pub ruler_min_rule_evaluation_interval: ModelDuration,
    pub ruler_max_rule_evaluation_results: i64,

    // Store-gateway.
    pub store_gateway_tenant_shard_size: i64,
    pub store_gateway_tenant_shard_size_per_zone: i64,

    // Compactor.
    pub compactor_blocks_retention_period: ModelDuration,
    pub compactor_split_and_merge_shards: i64,
    pub compactor_ooo_split_and_merge_shards: i64,
    pub compactor_split_groups: i64,
    pub compactor_tenant_shard_size: i64,
    pub compactor_partial_block_deletion_delay: ModelDuration,
    pub compactor_block_upload_enabled: bool,
    pub compactor_block_upload_validation_enabled: bool,
    pub compactor_block_upload_verify_chunks: bool,
    pub compactor_block_upload_max_block_size_bytes: i64,
    pub compactor_max_lookback: ModelDuration,
    pub compactor_max_per_block_upload_concurrency: i64,

    // S3 SSE overrides.
    pub s3_sse_type: String,
    pub s3_sse_kms_key_id: String,
    pub s3_sse_kms_encryption_context: String,

    // Alertmanager.
    pub alertmanager_receivers_firewall_block_cidr_networks: CIDRSliceCSV,
    pub alertmanager_receivers_firewall_block_private_addresses: bool,
    pub alertmanager_notification_rate_limit: f64,
    pub alertmanager_notification_rate_limit_per_integration: LimitsMap<f64>,
    pub alertmanager_max_config_size_bytes: i64,
    pub alertmanager_max_silences_count: i64,
    pub alertmanager_max_silence_size_bytes: i64,
    pub alertmanager_max_templates_count: i64,
    pub alertmanager_max_template_size_bytes: i64,
    pub alertmanager_max_dispatcher_aggregation_groups: i64,
    pub alertmanager_max_alerts_count: i64,
    pub alertmanager_max_alerts_size_bytes: i64,

    // OpenTelemetry.
    pub otel_metric_suffixes_enabled: Option<bool>,
    pub otel_created_timestamp_zero_ingestion_enabled: bool,
    pub promote_otel_resource_attributes: StringSliceCSV,
    pub otel_keep_identifying_resource_attributes: bool,
    pub otel_convert_histograms_to_nhcb: bool,
    pub otel_promote_scope_metadata: bool,
    pub otel_native_delta_ingestion: bool,
    pub otel_translation_strategy: OTelTranslationStrategy,
    pub otel_label_name_underscore_sanitization: bool,
    pub otel_label_name_preserve_multiple_underscores: bool,

    // Ingest storage.
    pub ingest_storage_read_consistency: String,
    pub ingestion_partitions_tenant_shard_size: i64,
    pub ingestion_partitions_tenant_write_shard_size: i64,

    // Name validation scheme.
    pub name_validation_scheme: NameValidationScheme,
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            max_active_series_per_user: 0,
            active_series_limit_response_code: 429,
            request_rate: 0.0,
            request_burst_size: 0,
            ingestion_rate: 10000.0,
            ingestion_burst_size: 200000,
            ingestion_burst_factor: 0.0,
            accept_ha_samples: false,
            ha_tracker_per_sample_dedupe: false,
            merge_duplicate_timeseries: false,
            ha_cluster_label: "cluster".to_string(),
            ha_replica_label: "__replica__".to_string(),
            ha_max_clusters: 100,
            ha_tracker_update_timeout: ModelDuration::from_secs(15),
            ha_tracker_update_timeout_jitter_max: ModelDuration::from_secs(5),
            ha_tracker_failover_timeout: ModelDuration::from_secs(30),
            ha_tracker_sample_failover_timeout: ModelDuration::ZERO,
            drop_labels: Vec::new(),
            max_label_name_length: 1024,
            max_label_value_length: 2048,
            label_value_length_over_limit_strategy: LabelValueLengthOverLimitStrategy::Error,
            blocked_label_names_for_label_value_bytes: StringSliceCSV::default(),
            max_label_names_per_series: 30,
            max_label_names_per_info_series: 80,
            max_metadata_length: 1024,
            max_native_histogram_buckets: 0,
            max_exemplars_per_series_per_request: 0,
            reduce_native_histogram_over_max_buckets: true,
            creation_grace_period: ModelDuration::from_mins(10),
            past_grace_period: ModelDuration::ZERO,
            enforce_out_of_order_window_on_distributor: false,
            enforce_metadata_metric_name: true,
            ingestion_tenant_shard_size: 0,
            metric_relabel_configs: Vec::new(),
            ingestion_artificial_delay: ModelDuration::ZERO,

            max_global_series_per_user: 150000,
            max_global_series_per_metric: 0,
            label_value_bytes_metric_threshold: 0,
            max_global_metadata_per_user: 0,
            max_global_metadata_per_metric: 0,
            max_global_exemplars_per_user: 0,
            ignore_ooo_exemplars: false,
            early_head_compaction_owned_series_threshold: 0,
            early_head_compaction_min_estimated_series_reduction_percentage: 15,
            native_histograms_ingestion_enabled: true,
            float_chunk_encoding: "xor".to_string(),
            histogram_chunk_encoding: "histogram".to_string(),
            active_series_custom_trackers: LimitsMap::new(),
            active_series_additional_custom_trackers: LimitsMap::new(),
            max_active_series_additional_custom_trackers: 0,
            out_of_order_time_window: ModelDuration::ZERO,
            out_of_order_blocks_external_label_enabled: false,
            separate_metrics_group_label: String::new(),

            max_fetched_chunks_per_query: 2_000_000,
            max_estimated_fetched_chunks_per_query_multiplier: 0.0,
            max_fetched_series_per_query: 0,
            max_fetched_chunk_bytes_per_query: 0,
            max_estimated_memory_consumption_per_query: 0,
            max_blocks_per_store_request: 20,
            max_query_lookback: ModelDuration::ZERO,
            max_partial_query_length: ModelDuration::ZERO,
            max_query_parallelism: 14,
            max_labels_query_length: ModelDuration::ZERO,
            max_series_query_limit: 0,
            max_label_names_limit: 0,
            max_label_values_limit: 0,
            max_cache_freshness: ModelDuration::from_mins(10),
            max_queriers_per_tenant: 0,
            query_sharding_total_shards: 16,
            query_sharding_max_sharded_queries: 128,
            query_sharding_max_regexp_size_bytes: 4096,
            cardinality_sharding_max_sharded_queries: 0,
            query_ingesters_within: ModelDuration::from_hours(13),
            enable_delayed_name_removal: false,

            max_total_query_length: ModelDuration::ZERO,
            results_cache_ttl: ModelDuration::from_days(7),
            results_cache_ttl_for_out_of_order_time_window: ModelDuration::from_mins(10),
            results_cache_ttl_for_cardinality_query: ModelDuration::ZERO,
            results_cache_ttl_for_labels_query: ModelDuration::ZERO,
            results_cache_ttl_for_errors: ModelDuration::from_mins(5),
            cache_unaligned_requests: false,
            max_query_expression_size_bytes: 0,
            blocked_queries: Vec::new(),
            limited_queries: Vec::new(),
            blocked_requests: Vec::new(),
            align_queries_with_step: false,
            enabled_promql_experimental_functions: StringSliceCSV::default(),
            enabled_promql_extended_range_selectors: StringSliceCSV::default(),
            enabled_promql_binop_fill_modifiers: StringSliceCSV::default(),
            prom2_range_compat: false,
            subquery_spin_off_enabled: false,
            labels_query_optimizer_enabled: true,

            cardinality_analysis_enabled: false,
            label_names_and_values_results_max_size_bytes: 400 * 1024 * 1024,
            label_values_max_cardinality_label_names_per_request: 100,
            cardinality_analysis_max_results: 500,
            active_series_results_max_size_bytes: 400 * 1024 * 1024,

            cost_attribution_labels_structured: Vec::new(),
            cost_attribution_trackers: CostAttributionTrackerConfigs::new(),
            additional_cost_attribution_trackers: CostAttributionTrackerConfigs::new(),
            cost_attribution_cooldown: ModelDuration::ZERO,
            max_cost_attribution_cardinality: 2000,

            ruler_evaluation_delay_duration: ModelDuration::from_mins(1),
            ruler_evaluation_consistency_max_delay: ModelDuration::ZERO,
            ruler_tenant_shard_size: 0,
            ruler_max_rules_per_rule_group: 20,
            ruler_max_rule_groups_per_tenant: 70,
            ruler_recording_rules_evaluation_enabled: true,
            ruler_alerting_rules_evaluation_enabled: true,
            ruler_sync_rules_on_changes_enabled: true,
            ruler_max_rules_per_rule_group_by_namespace: LimitsMap::new(),
            ruler_max_rule_groups_per_tenant_by_namespace: LimitsMap::new(),
            ruler_protected_namespaces: StringSliceCSV::default(),
            ruler_max_independent_rule_evaluation_concurrency_per_tenant: 4,
            ruler_alertmanager_client_config: RulerAlertmanagerClientConfig::default(),
            ruler_min_rule_evaluation_interval: ModelDuration::ZERO,
            ruler_max_rule_evaluation_results: 0,

            store_gateway_tenant_shard_size: 0,
            store_gateway_tenant_shard_size_per_zone: 0,

            compactor_blocks_retention_period: ModelDuration::ZERO,
            compactor_split_and_merge_shards: 0,
            compactor_ooo_split_and_merge_shards: 0,
            compactor_split_groups: 1,
            compactor_tenant_shard_size: 0,
            compactor_partial_block_deletion_delay: ModelDuration::from_days(1),
            compactor_block_upload_enabled: false,
            compactor_block_upload_validation_enabled: true,
            compactor_block_upload_verify_chunks: true,
            compactor_block_upload_max_block_size_bytes: 0,
            compactor_max_lookback: ModelDuration::ZERO,
            compactor_max_per_block_upload_concurrency: 8,

            s3_sse_type: String::new(),
            s3_sse_kms_key_id: String::new(),
            s3_sse_kms_encryption_context: String::new(),

            alertmanager_receivers_firewall_block_cidr_networks: CIDRSliceCSV::default(),
            alertmanager_receivers_firewall_block_private_addresses: false,
            alertmanager_notification_rate_limit: 0.0,
            alertmanager_notification_rate_limit_per_integration: LimitsMap::new(),
            alertmanager_max_config_size_bytes: 0,
            alertmanager_max_silences_count: 0,
            alertmanager_max_silence_size_bytes: 0,
            alertmanager_max_templates_count: 0,
            alertmanager_max_template_size_bytes: 0,
            alertmanager_max_dispatcher_aggregation_groups: 0,
            alertmanager_max_alerts_count: 0,
            alertmanager_max_alerts_size_bytes: 0,

            otel_metric_suffixes_enabled: Some(false),
            otel_created_timestamp_zero_ingestion_enabled: false,
            promote_otel_resource_attributes: StringSliceCSV::default(),
            otel_keep_identifying_resource_attributes: false,
            otel_convert_histograms_to_nhcb: false,
            otel_promote_scope_metadata: false,
            otel_native_delta_ingestion: false,
            otel_translation_strategy: OTelTranslationStrategy::default(),
            otel_label_name_underscore_sanitization: true,
            otel_label_name_preserve_multiple_underscores: true,

            ingest_storage_read_consistency: "eventual".to_string(),
            ingestion_partitions_tenant_shard_size: 0,
            ingestion_partitions_tenant_write_shard_size: 0,

            name_validation_scheme: NameValidationScheme::Legacy,
        }
    }
}

impl LimitsConfig {
    /// Effective partitions shard size for writes: the write shard size when set (> 0), clamped to
    /// the read shard size; otherwise the read shard size
    /// (Go: `Overrides.EffectiveIngestionPartitionsTenantWriteShardSize`).
    pub fn effective_ingestion_partitions_tenant_write_shard_size(&self) -> i64 {
        let read = self.ingestion_partitions_tenant_shard_size;
        let write = self.ingestion_partitions_tenant_write_shard_size;
        if write <= 0 {
            return read;
        }
        if read > 0 && write > read {
            return read;
        }
        write
    }

    pub fn register_flags_into<T: 'static>(
        &self,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        f.i64_var("distributor.max-active-series-per-user", self.max_active_series_per_user, "Maximum number of active series per user. 0 means no limit.", move |t| &get(t).max_active_series_per_user, move |t| &mut get_mut(t).max_active_series_per_user);
        f.i64_var("distributor.active-series-limit-response-code", self.active_series_limit_response_code, "HTTP response code to use when rejecting series due to the active series limit.", move |t| &get(t).active_series_limit_response_code, move |t| &mut get_mut(t).active_series_limit_response_code);
        f.i64_var("distributor.ingestion-tenant-shard-size", self.ingestion_tenant_shard_size, "The tenant's shard size used by shuffle-sharding.", move |t| &get(t).ingestion_tenant_shard_size, move |t| &mut get_mut(t).ingestion_tenant_shard_size);
        f.f64_var("distributor.request-rate-limit", self.request_rate, "Per-tenant push request rate limit in requests per second. 0 to disable.", move |t| &get(t).request_rate, move |t| &mut get_mut(t).request_rate);
        f.i64_var("distributor.request-burst-size", self.request_burst_size, "Per-tenant allowed push request burst size. 0 to disable.", move |t| &get(t).request_burst_size, move |t| &mut get_mut(t).request_burst_size);
        f.f64_var("distributor.ingestion-rate-limit", self.ingestion_rate, "Per-tenant ingestion rate limit in samples per second.", move |t| &get(t).ingestion_rate, move |t| &mut get_mut(t).ingestion_rate);
        f.i64_var("distributor.ingestion-burst-size", self.ingestion_burst_size, "Per-tenant allowed ingestion burst size (in number of samples).", move |t| &get(t).ingestion_burst_size, move |t| &mut get_mut(t).ingestion_burst_size);
        f.f64_var("distributor.ingestion-burst-factor", self.ingestion_burst_factor, "Per-tenant burst factor which is the maximum burst size allowed as a multiple of the per-tenant ingestion rate.", move |t| &get(t).ingestion_burst_factor, move |t| &mut get_mut(t).ingestion_burst_factor);
        f.bool_var("distributor.ha-tracker.enable-for-all-users", self.accept_ha_samples, "Flag to enable, for all tenants, handling of samples with external labels identifying replicas in an HA Prometheus setup.", move |t| &get(t).accept_ha_samples, move |t| &mut get_mut(t).accept_ha_samples);
        f.bool_var("distributor.ha-tracker.per-sample-dedupe", self.ha_tracker_per_sample_dedupe, "Experimental: evaluate HA deduplication per timeseries within a write request.", move |t| &get(t).ha_tracker_per_sample_dedupe, move |t| &mut get_mut(t).ha_tracker_per_sample_dedupe);
        f.bool_var("distributor.merge-duplicate-timeseries", self.merge_duplicate_timeseries, "Merge timeseries that share the same label set and created timestamp within a single write request.", move |t| &get(t).merge_duplicate_timeseries, move |t| &mut get_mut(t).merge_duplicate_timeseries);
        f.string_var("distributor.ha-tracker.cluster", &self.ha_cluster_label, "Prometheus label to look for in samples to identify a Prometheus HA cluster.", move |t| &get(t).ha_cluster_label, move |t| &mut get_mut(t).ha_cluster_label);
        f.string_var("distributor.ha-tracker.replica", &self.ha_replica_label, "Prometheus label to look for in samples to identify a Prometheus HA replica.", move |t| &get(t).ha_replica_label, move |t| &mut get_mut(t).ha_replica_label);
        f.model_duration_var("distributor.ha-tracker.update-timeout", self.ha_tracker_update_timeout, "Update the timestamp in the KV store for a given cluster/replica only after this amount of time has passed.", move |t| &get(t).ha_tracker_update_timeout, move |t| &mut get_mut(t).ha_tracker_update_timeout);
        f.model_duration_var("distributor.ha-tracker.update-timeout-jitter-max", self.ha_tracker_update_timeout_jitter_max, "Maximum jitter applied to the update timeout.", move |t| &get(t).ha_tracker_update_timeout_jitter_max, move |t| &mut get_mut(t).ha_tracker_update_timeout_jitter_max);
        f.model_duration_var("distributor.ha-tracker.failover-timeout", self.ha_tracker_failover_timeout, "If we don't receive any samples from the accepted replica for a cluster in this amount of time we will failover.", move |t| &get(t).ha_tracker_failover_timeout, move |t| &mut get_mut(t).ha_tracker_failover_timeout);
        f.model_duration_var("distributor.ha-tracker.failover-sample-timeout", self.ha_tracker_sample_failover_timeout, "Additional timeout to use for failover that uses the earliest sample time instead of the current time.", move |t| &get(t).ha_tracker_sample_failover_timeout, move |t| &mut get_mut(t).ha_tracker_sample_failover_timeout);
        f.i64_var("distributor.ha-tracker.max-clusters", self.ha_max_clusters, "Maximum number of clusters that HA tracker will keep track of for a single tenant. 0 to disable the limit.", move |t| &get(t).ha_max_clusters, move |t| &mut get_mut(t).ha_max_clusters);
        f.string_slice_var("distributor.drop-label", &self.drop_labels, "This flag can be used to specify label names to drop during sample ingestion.", move |t| &get(t).drop_labels, move |t| &mut get_mut(t).drop_labels);
        f.i64_var("validation.max-length-label-name", self.max_label_name_length, "Maximum length accepted for label names", move |t| &get(t).max_label_name_length, move |t| &mut get_mut(t).max_label_name_length);
        f.i64_var("validation.max-length-label-value", self.max_label_value_length, "Maximum length accepted for label value. This setting also applies to the metric name", move |t| &get(t).max_label_value_length, move |t| &mut get_mut(t).max_label_value_length);
        f.label_value_strategy_var("validation.label-value-length-over-limit-strategy", self.label_value_length_over_limit_strategy, "What to do for label values over the length limit. Options are: 'error', 'truncate', 'drop'.", move |t| &get(t).label_value_length_over_limit_strategy, move |t| &mut get_mut(t).label_value_length_over_limit_strategy);
        f.string_slice_csv_var("validation.blocked-label-names-for-label-value-bytes", &self.blocked_label_names_for_label_value_bytes, "Label names for which the distinct label value bytes have exceeded the threshold.", move |t| &get(t).blocked_label_names_for_label_value_bytes, move |t| &mut get_mut(t).blocked_label_names_for_label_value_bytes);
        f.i64_var("validation.max-label-names-per-series", self.max_label_names_per_series, "Maximum number of label names per series.", move |t| &get(t).max_label_names_per_series, move |t| &mut get_mut(t).max_label_names_per_series);
        f.i64_var("validation.max-label-names-per-info-series", self.max_label_names_per_info_series, "Maximum number of label names per info series.", move |t| &get(t).max_label_names_per_info_series, move |t| &mut get_mut(t).max_label_names_per_info_series);
        f.i64_var("validation.max-metadata-length", self.max_metadata_length, "Maximum length accepted for metric metadata.", move |t| &get(t).max_metadata_length, move |t| &mut get_mut(t).max_metadata_length);
        f.i64_var("validation.max-native-histogram-buckets", self.max_native_histogram_buckets, "Maximum number of buckets per native histogram sample. 0 to disable the limit.", move |t| &get(t).max_native_histogram_buckets, move |t| &mut get_mut(t).max_native_histogram_buckets);
        f.i64_var("distributor.max-exemplars-per-series-per-request", self.max_exemplars_per_series_per_request, "Maximum number of exemplars per series per request. 0 to disable limit.", move |t| &get(t).max_exemplars_per_series_per_request, move |t| &mut get_mut(t).max_exemplars_per_series_per_request);
        f.bool_var("validation.reduce-native-histogram-over-max-buckets", self.reduce_native_histogram_over_max_buckets, "Whether to reduce or reject native histogram samples with more buckets than the configured limit.", move |t| &get(t).reduce_native_histogram_over_max_buckets, move |t| &mut get_mut(t).reduce_native_histogram_over_max_buckets);
        f.model_duration_var("validation.create-grace-period", self.creation_grace_period, "Controls how far into the future incoming samples and exemplars are accepted compared to the wall clock.", move |t| &get(t).creation_grace_period, move |t| &mut get_mut(t).creation_grace_period);
        f.model_duration_var("validation.past-grace-period", self.past_grace_period, "Controls how far into the past incoming samples and exemplars are accepted compared to the wall clock.", move |t| &get(t).past_grace_period, move |t| &mut get_mut(t).past_grace_period);
        f.bool_var("validation.enforce-out-of-order-window-on-distributor", self.enforce_out_of_order_window_on_distributor, "When enabled and past_grace_period is 0, the distributor rejects samples older than '(now - out_of_order_time_window)'.", move |t| &get(t).enforce_out_of_order_window_on_distributor, move |t| &mut get_mut(t).enforce_out_of_order_window_on_distributor);
        f.bool_var("validation.enforce-metadata-metric-name", self.enforce_metadata_metric_name, "Enforce every metadata has a metric name.", move |t| &get(t).enforce_metadata_metric_name, move |t| &mut get_mut(t).enforce_metadata_metric_name);
        f.opt_bool_var(
            "distributor.otel-metric-suffixes-enabled",
            self.otel_metric_suffixes_enabled.unwrap_or(false),
            "Whether to enable automatic suffixes to names of metrics ingested through OTLP.",
            move |t| &get(t).otel_metric_suffixes_enabled,
            move |t| &mut get_mut(t).otel_metric_suffixes_enabled,
        );
        f.bool_var("distributor.otel-created-timestamp-zero-ingestion-enabled", self.otel_created_timestamp_zero_ingestion_enabled, "Whether to enable translation of OTel start timestamps to Prometheus zero samples in the OTLP endpoint.", move |t| &get(t).otel_created_timestamp_zero_ingestion_enabled, move |t| &mut get_mut(t).otel_created_timestamp_zero_ingestion_enabled);
        f.string_slice_csv_var("distributor.otel-promote-resource-attributes", &self.promote_otel_resource_attributes, "Optionally specify a comma-separated list of OTel resource attributes to promote to labels.", move |t| &get(t).promote_otel_resource_attributes, move |t| &mut get_mut(t).promote_otel_resource_attributes);
        f.bool_var("distributor.otel-keep-identifying-resource-attributes", self.otel_keep_identifying_resource_attributes, "Whether to keep identifying OTel resource attributes in the target_info metric.", move |t| &get(t).otel_keep_identifying_resource_attributes, move |t| &mut get_mut(t).otel_keep_identifying_resource_attributes);
        f.bool_var("distributor.otel-convert-histograms-to-nhcb", self.otel_convert_histograms_to_nhcb, "Whether to convert OTel explicit histograms into native histograms with custom buckets.", move |t| &get(t).otel_convert_histograms_to_nhcb, move |t| &mut get_mut(t).otel_convert_histograms_to_nhcb);
        f.bool_var("distributor.otel-promote-scope-metadata", self.otel_promote_scope_metadata, "Whether to promote OTel scope metadata to corresponding metric labels.", move |t| &get(t).otel_promote_scope_metadata, move |t| &mut get_mut(t).otel_promote_scope_metadata);
        f.bool_var("distributor.otel-native-delta-ingestion", self.otel_native_delta_ingestion, "Whether to enable native ingestion of delta OTLP metrics.", move |t| &get(t).otel_native_delta_ingestion, move |t| &mut get_mut(t).otel_native_delta_ingestion);
        f.otel_strategy_var("distributor.otel-translation-strategy", &self.otel_translation_strategy, "Translation strategy to apply in OTLP endpoint for metric and label names.", move |t| &get(t).otel_translation_strategy, move |t| &mut get_mut(t).otel_translation_strategy);
        f.bool_var("distributor.otel-label-name-underscore-sanitization", self.otel_label_name_underscore_sanitization, "If enabled, prefixes label names starting with a single underscore with `key_` when translating OTel attribute names.", move |t| &get(t).otel_label_name_underscore_sanitization, move |t| &mut get_mut(t).otel_label_name_underscore_sanitization);
        f.bool_var("distributor.otel-label-name-preserve-underscores", self.otel_label_name_preserve_multiple_underscores, "If enabled, keeps multiple consecutive underscores in label names when translating OTel attribute names.", move |t| &get(t).otel_label_name_preserve_multiple_underscores, move |t| &mut get_mut(t).otel_label_name_preserve_multiple_underscores);
        f.model_duration_var("distributor.ingestion-artificial-delay", self.ingestion_artificial_delay, "Target ingestion delay to apply to all tenants.", move |t| &get(t).ingestion_artificial_delay, move |t| &mut get_mut(t).ingestion_artificial_delay);
        f.name_validation_scheme_var("validation.name-validation-scheme", self.name_validation_scheme, "Validation scheme to use for metric and label names.", move |t| &get(t).name_validation_scheme, move |t| &mut get_mut(t).name_validation_scheme);

        // Ingester limits
        f.i64_var("ingester.max-global-series-per-user", self.max_global_series_per_user, "The maximum number of in-memory series per tenant, across the cluster before replication. 0 to disable.", move |t| &get(t).max_global_series_per_user, move |t| &mut get_mut(t).max_global_series_per_user);
        f.i64_var("ingester.max-global-series-per-metric", self.max_global_series_per_metric, "The maximum number of in-memory series per metric name, across the cluster before replication. 0 to disable.", move |t| &get(t).max_global_series_per_metric, move |t| &mut get_mut(t).max_global_series_per_metric);
        f.i64_var("ingester.label-value-bytes-metric-threshold", self.label_value_bytes_metric_threshold, "The total size, in bytes, of the distinct values of a single label name held in memory per tenant.", move |t| &get(t).label_value_bytes_metric_threshold, move |t| &mut get_mut(t).label_value_bytes_metric_threshold);
        f.i64_var("ingester.max-global-metadata-per-user", self.max_global_metadata_per_user, "The maximum number of in-memory metrics with metadata per tenant, across the cluster. 0 to disable.", move |t| &get(t).max_global_metadata_per_user, move |t| &mut get_mut(t).max_global_metadata_per_user);
        f.i64_var("ingester.max-global-metadata-per-metric", self.max_global_metadata_per_metric, "The maximum number of metadata per metric, across the cluster. 0 to disable.", move |t| &get(t).max_global_metadata_per_metric, move |t| &mut get_mut(t).max_global_metadata_per_metric);
        f.i64_var("ingester.max-global-exemplars-per-user", self.max_global_exemplars_per_user, "The maximum number of exemplars in memory, across the cluster. 0 to disable exemplars ingestion.", move |t| &get(t).max_global_exemplars_per_user, move |t| &mut get_mut(t).max_global_exemplars_per_user);
        f.bool_var("ingester.ignore-ooo-exemplars", self.ignore_ooo_exemplars, "Whether to ignore exemplars with out-of-order timestamps.", move |t| &get(t).ignore_ooo_exemplars, move |t| &mut get_mut(t).ignore_ooo_exemplars);
        f.register_raw(
            "ingester.active-series-custom-trackers",
            false,
            "value",
            "{}",
            "Additional active series metrics, matching the provided matchers.",
            move |t, s| {
                // Go: CustomTrackersConfig.Set + customTrackerFlagValueToMap.
                if s.trim().is_empty() {
                    return Ok(());
                }
                let mut source = BTreeMap::new();
                for (i, p) in s.split(';').enumerate() {
                    let (name, matcher) = p.split_once(':').ok_or_else(|| {
                        ConfigError::Flag(format!(
                            "value should be <name>:<matcher>[;<name>:<matcher>]*, but colon was not found in the value {i}: {p:?}"
                        ))
                    })?;
                    let (name, matcher) = (name.trim(), matcher.trim());
                    if name.is_empty() || matcher.is_empty() {
                        return Err(ConfigError::Flag(format!(
                            "semicolon-separated values should be <name>:<matcher>, but one of the sides was empty in the value {i}: {p:?}"
                        )));
                    }
                    if source.contains_key(name) {
                        return Err(ConfigError::Flag(format!(
                            "matcher {name:?} for active series custom trackers is provided twice"
                        )));
                    }
                    source.insert(name.to_string(), matcher.to_string());
                }
                if let Some(v) = CUSTOM_TRACKERS_VALIDATOR.get() {
                    v(&source).map_err(ConfigError::Flag)?;
                }
                let map = &mut get_mut(t).active_series_custom_trackers;
                for (name, matcher) in source {
                    if map.contains_key(&name) {
                        return Err(ConfigError::Flag(format!(
                            "matcher {name:?} for active series custom trackers is provided more than once"
                        )));
                    }
                    map.insert(name, matcher);
                }
                Ok(())
            },
            move |t| {
                let map = &get(t).active_series_custom_trackers;
                if map.is_empty() {
                    "{}".to_string()
                } else {
                    map.iter()
                        .map(|(k, v)| format!("{k}:{v}"))
                        .collect::<Vec<_>>()
                        .join(";")
                }
            },
        );
        f.model_duration_var("ingester.out-of-order-time-window", self.out_of_order_time_window, "Non-zero value enables out-of-order support for most recent samples within the time window.", move |t| &get(t).out_of_order_time_window, move |t| &mut get_mut(t).out_of_order_time_window);
        f.bool_var("ingester.native-histograms-ingestion-enabled", self.native_histograms_ingestion_enabled, "Enable ingestion of native histogram samples.", move |t| &get(t).native_histograms_ingestion_enabled, move |t| &mut get_mut(t).native_histograms_ingestion_enabled);
        f.string_var("blocks-storage.tsdb.float-chunk-encoding", &self.float_chunk_encoding, "Encoding used for float chunks written for this tenant.", move |t| &get(t).float_chunk_encoding, move |t| &mut get_mut(t).float_chunk_encoding);
        f.string_var("blocks-storage.tsdb.histogram-chunk-encoding", &self.histogram_chunk_encoding, "Encoding used for integer and float native histogram chunks written for this tenant.", move |t| &get(t).histogram_chunk_encoding, move |t| &mut get_mut(t).histogram_chunk_encoding);
        f.bool_var("ingester.out-of-order-blocks-external-label-enabled", self.out_of_order_blocks_external_label_enabled, "Whether the shipper should label out-of-order blocks with an external label.", move |t| &get(t).out_of_order_blocks_external_label_enabled, move |t| &mut get_mut(t).out_of_order_blocks_external_label_enabled);
        f.i64_var("ingester.early-head-compaction-owned-series-threshold", self.early_head_compaction_owned_series_threshold, "When the number of owned series for a tenant across the cluster exceeds this threshold, trigger early head compaction.", move |t| &get(t).early_head_compaction_owned_series_threshold, move |t| &mut get_mut(t).early_head_compaction_owned_series_threshold);
        f.i64_var("ingester.early-head-compaction-min-estimated-series-reduction-percentage", self.early_head_compaction_min_estimated_series_reduction_percentage, "Minimum estimated series reduction percentage (0-100) required to trigger per-tenant early compaction.", move |t| &get(t).early_head_compaction_min_estimated_series_reduction_percentage, move |t| &mut get_mut(t).early_head_compaction_min_estimated_series_reduction_percentage);
        f.string_var("validation.separate-metrics-group-label", &self.separate_metrics_group_label, "Label used to define the group label for metrics separation.", move |t| &get(t).separate_metrics_group_label, move |t| &mut get_mut(t).separate_metrics_group_label);
        f.register_raw(
            "validation.cost-attribution-trackers",
            false,
            "value",
            "{}",
            "Base cost attribution trackers configuration as JSON.",
            move |t, s| {
                let parsed: CostAttributionTrackerConfigs = serde_json::from_str(s).map_err(|e| {
                    ConfigError::Flag(format!("invalid cost attribution trackers JSON: {e}"))
                })?;
                get_mut(t).cost_attribution_trackers = parsed;
                Ok(())
            },
            move |t| {
                serde_json::to_string(&get(t).cost_attribution_trackers)
                    .unwrap_or_else(|_| "{}".to_string())
            },
        );
        f.i64_var("validation.max-cost-attribution-cardinality", self.max_cost_attribution_cardinality, "Maximum cardinality of cost attribution labels allowed per user.", move |t| &get(t).max_cost_attribution_cardinality, move |t| &mut get_mut(t).max_cost_attribution_cardinality);
        f.model_duration_var("validation.cost-attribution-cooldown", self.cost_attribution_cooldown, "Defines how long cost attribution stays in overflow before attempting a reset.", move |t| &get(t).cost_attribution_cooldown, move |t| &mut get_mut(t).cost_attribution_cooldown);
        f.i64_var("validation.max-active-series-additional-custom-trackers", self.max_active_series_additional_custom_trackers, "Maximum number of additional custom trackers for active series per tenant.", move |t| &get(t).max_active_series_additional_custom_trackers, move |t| &mut get_mut(t).max_active_series_additional_custom_trackers);

        // Querier / Query-frontend limits
        f.i64_var("querier.max-fetched-chunks-per-query", self.max_fetched_chunks_per_query, "Maximum number of chunks that can be fetched in a single query.", move |t| &get(t).max_fetched_chunks_per_query, move |t| &mut get_mut(t).max_fetched_chunks_per_query);
        f.f64_var("querier.max-estimated-fetched-chunks-per-query-multiplier", self.max_estimated_fetched_chunks_per_query_multiplier, "Maximum number of chunks estimated to be fetched in a single query.", move |t| &get(t).max_estimated_fetched_chunks_per_query_multiplier, move |t| &mut get_mut(t).max_estimated_fetched_chunks_per_query_multiplier);
        f.i64_var("querier.max-fetched-series-per-query", self.max_fetched_series_per_query, "The maximum number of unique series for which a query can fetch samples.", move |t| &get(t).max_fetched_series_per_query, move |t| &mut get_mut(t).max_fetched_series_per_query);
        f.i64_var("querier.max-fetched-chunk-bytes-per-query", self.max_fetched_chunk_bytes_per_query, "The maximum size of all chunks in bytes that a query can fetch.", move |t| &get(t).max_fetched_chunk_bytes_per_query, move |t| &mut get_mut(t).max_fetched_chunk_bytes_per_query);
        f.u64_var("querier.max-estimated-memory-consumption-per-query", self.max_estimated_memory_consumption_per_query, "The maximum estimated memory a single query can consume at once, in bytes.", move |t| &get(t).max_estimated_memory_consumption_per_query, move |t| &mut get_mut(t).max_estimated_memory_consumption_per_query);
        f.i64_var("querier.max-blocks-per-store-request", self.max_blocks_per_store_request, "Maximum number of blocks that a querier will reference in a single request to a store-gateway.", move |t| &get(t).max_blocks_per_store_request, move |t| &mut get_mut(t).max_blocks_per_store_request);
        f.model_duration_var("querier.max-partial-query-length", self.max_partial_query_length, "Limit the time range for partial queries at the querier level.", move |t| &get(t).max_partial_query_length, move |t| &mut get_mut(t).max_partial_query_length);
        f.model_duration_var("querier.max-query-lookback", self.max_query_lookback, "Limit how long back data (series and metadata) can be queried.", move |t| &get(t).max_query_lookback, move |t| &mut get_mut(t).max_query_lookback);
        f.i64_var("querier.max-query-parallelism", self.max_query_parallelism, "Maximum number of split or partial queries scheduled in parallel.", move |t| &get(t).max_query_parallelism, move |t| &mut get_mut(t).max_query_parallelism);
        f.model_duration_var("store.max-labels-query-length", self.max_labels_query_length, "Limit the time range of series, label names and values queries.", move |t| &get(t).max_labels_query_length, move |t| &mut get_mut(t).max_labels_query_length);
        f.i64_var("querier.max-series-query-limit", self.max_series_query_limit, "Maximum number of series the series endpoint queries.", move |t| &get(t).max_series_query_limit, move |t| &mut get_mut(t).max_series_query_limit);
        f.i64_var("querier.max-label-names-limit", self.max_label_names_limit, "Maximum number of names the label names endpoint returns.", move |t| &get(t).max_label_names_limit, move |t| &mut get_mut(t).max_label_names_limit);
        f.i64_var("querier.max-label-values-limit", self.max_label_values_limit, "Maximum number of values the label values endpoint returns.", move |t| &get(t).max_label_values_limit, move |t| &mut get_mut(t).max_label_values_limit);
        f.i64_var("querier.label-names-and-values-results-max-size-bytes", self.label_names_and_values_results_max_size_bytes, "Maximum size in bytes of distinct label names and values.", move |t| &get(t).label_names_and_values_results_max_size_bytes, move |t| &mut get_mut(t).label_names_and_values_results_max_size_bytes);
        f.i64_var("querier.active-series-results-max-size-bytes", self.active_series_results_max_size_bytes, "Maximum size of an active series request result shard in bytes.", move |t| &get(t).active_series_results_max_size_bytes, move |t| &mut get_mut(t).active_series_results_max_size_bytes);
        f.bool_var("querier.cardinality-analysis-enabled", self.cardinality_analysis_enabled, "Enables endpoints used for cardinality analysis.", move |t| &get(t).cardinality_analysis_enabled, move |t| &mut get_mut(t).cardinality_analysis_enabled);
        f.i64_var("querier.label-values-max-cardinality-label-names-per-request", self.label_values_max_cardinality_label_names_per_request, "Maximum number of label names allowed to be queried in a single cardinality API call.", move |t| &get(t).label_values_max_cardinality_label_names_per_request, move |t| &mut get_mut(t).label_values_max_cardinality_label_names_per_request);
        f.i64_var("querier.cardinality-api-max-series-limit", self.cardinality_analysis_max_results, "Maximum number of series that can be requested in a single cardinality API request.", move |t| &get(t).cardinality_analysis_max_results, move |t| &mut get_mut(t).cardinality_analysis_max_results);
        f.model_duration_var("query-frontend.max-cache-freshness", self.max_cache_freshness, "Most recent allowed cacheable result per-tenant.", move |t| &get(t).max_cache_freshness, move |t| &mut get_mut(t).max_cache_freshness);
        f.i64_var("query-frontend.max-queriers-per-tenant", self.max_queriers_per_tenant, "Maximum number of queriers that can handle requests for a single tenant.", move |t| &get(t).max_queriers_per_tenant, move |t| &mut get_mut(t).max_queriers_per_tenant);
        f.i64_var("query-frontend.query-sharding-total-shards", self.query_sharding_total_shards, "The number of shards to use when doing parallelisation via query sharding.", move |t| &get(t).query_sharding_total_shards, move |t| &mut get_mut(t).query_sharding_total_shards);
        f.i64_var("query-frontend.query-sharding-max-sharded-queries", self.query_sharding_max_sharded_queries, "The maximum number of sharded queries that can be run.", move |t| &get(t).query_sharding_max_sharded_queries, move |t| &mut get_mut(t).query_sharding_max_sharded_queries);
        f.i64_var("query-frontend.query-sharding-max-regexp-size-bytes", self.query_sharding_max_regexp_size_bytes, "Disable query sharding for any query containing a regular expression matcher longer than the configured bytes.", move |t| &get(t).query_sharding_max_regexp_size_bytes, move |t| &mut get_mut(t).query_sharding_max_regexp_size_bytes);
        f.i64_var("query-frontend.cardinality-sharding-max-sharded-queries", self.cardinality_sharding_max_sharded_queries, "The max number of sharded queries that can be run for a cardinality request.", move |t| &get(t).cardinality_sharding_max_sharded_queries, move |t| &mut get_mut(t).cardinality_sharding_max_sharded_queries);
        f.model_duration_var("querier.query-ingesters-within", self.query_ingesters_within, "Maximum lookback beyond which queries are not sent to ingester.", move |t| &get(t).query_ingesters_within, move |t| &mut get_mut(t).query_ingesters_within);
        f.bool_var("querier.enable-delayed-name-removal", self.enable_delayed_name_removal, "Enable the experimental Prometheus feature for delayed name removal within MQE.", move |t| &get(t).enable_delayed_name_removal, move |t| &mut get_mut(t).enable_delayed_name_removal);

        // Ruler
        f.model_duration_var("ruler.evaluation-delay-duration", self.ruler_evaluation_delay_duration, "Duration to delay the evaluation of rules.", move |t| &get(t).ruler_evaluation_delay_duration, move |t| &mut get_mut(t).ruler_evaluation_delay_duration);
        f.model_duration_var("ruler.evaluation-consistency-max-delay", self.ruler_evaluation_consistency_max_delay, "The maximum tolerated ingestion delay for eventually consistent rule evaluations.", move |t| &get(t).ruler_evaluation_consistency_max_delay, move |t| &mut get_mut(t).ruler_evaluation_consistency_max_delay);
        f.i64_var("ruler.tenant-shard-size", self.ruler_tenant_shard_size, "The tenant's shard size when sharding is used by ruler.", move |t| &get(t).ruler_tenant_shard_size, move |t| &mut get_mut(t).ruler_tenant_shard_size);
        f.i64_var("ruler.max-rules-per-rule-group", self.ruler_max_rules_per_rule_group, "Maximum number of rules per rule group per-tenant.", move |t| &get(t).ruler_max_rules_per_rule_group, move |t| &mut get_mut(t).ruler_max_rules_per_rule_group);
        f.i64_var("ruler.max-rule-groups-per-tenant", self.ruler_max_rule_groups_per_tenant, "Maximum number of rule groups per-tenant.", move |t| &get(t).ruler_max_rule_groups_per_tenant, move |t| &mut get_mut(t).ruler_max_rule_groups_per_tenant);
        f.bool_var("ruler.recording-rules-evaluation-enabled", self.ruler_recording_rules_evaluation_enabled, "Controls whether recording rules evaluation is enabled.", move |t| &get(t).ruler_recording_rules_evaluation_enabled, move |t| &mut get_mut(t).ruler_recording_rules_evaluation_enabled);
        f.bool_var("ruler.alerting-rules-evaluation-enabled", self.ruler_alerting_rules_evaluation_enabled, "Controls whether alerting rules evaluation is enabled.", move |t| &get(t).ruler_alerting_rules_evaluation_enabled, move |t| &mut get_mut(t).ruler_alerting_rules_evaluation_enabled);
        f.bool_var("ruler.sync-rules-on-changes-enabled", self.ruler_sync_rules_on_changes_enabled, "True to enable a re-sync of the configured rule groups as soon as they're changed.", move |t| &get(t).ruler_sync_rules_on_changes_enabled, move |t| &mut get_mut(t).ruler_sync_rules_on_changes_enabled);
        f.limits_map_var("ruler.max-rules-per-rule-group-by-namespace", &self.ruler_max_rules_per_rule_group_by_namespace, "Maximum number of rules per rule group by namespace.", move |t| &get(t).ruler_max_rules_per_rule_group_by_namespace, move |t| &mut get_mut(t).ruler_max_rules_per_rule_group_by_namespace);
        f.limits_map_var("ruler.max-rule-groups-per-tenant-by-namespace", &self.ruler_max_rule_groups_per_tenant_by_namespace, "Maximum number of rule groups per tenant by namespace.", move |t| &get(t).ruler_max_rule_groups_per_tenant_by_namespace, move |t| &mut get_mut(t).ruler_max_rule_groups_per_tenant_by_namespace);
        f.string_slice_csv_var("ruler.protected-namespaces", &self.ruler_protected_namespaces, "List of namespaces that are protected from modification.", move |t| &get(t).ruler_protected_namespaces, move |t| &mut get_mut(t).ruler_protected_namespaces);
        f.i64_var("ruler.max-independent-rule-evaluation-concurrency-per-tenant", self.ruler_max_independent_rule_evaluation_concurrency_per_tenant, "Maximum number of independent rules that can run concurrently for each tenant.", move |t| &get(t).ruler_max_independent_rule_evaluation_concurrency_per_tenant, move |t| &mut get_mut(t).ruler_max_independent_rule_evaluation_concurrency_per_tenant);
        self.ruler_alertmanager_client_config.register_flags_into(
            f,
            move |t| &get(t).ruler_alertmanager_client_config,
            move |t| &mut get_mut(t).ruler_alertmanager_client_config,
        );
        f.model_duration_var("ruler.min-rule-evaluation-interval", self.ruler_min_rule_evaluation_interval, "Minimum allowable evaluation interval for rule groups.", move |t| &get(t).ruler_min_rule_evaluation_interval, move |t| &mut get_mut(t).ruler_min_rule_evaluation_interval);
        f.i64_var("ruler.max-rule-evaluation-results", self.ruler_max_rule_evaluation_results, "Maximum number of alerts or series one rule can produce.", move |t| &get(t).ruler_max_rule_evaluation_results, move |t| &mut get_mut(t).ruler_max_rule_evaluation_results);

        // Compactor
        f.model_duration_var("compactor.blocks-retention-period", self.compactor_blocks_retention_period, "Delete blocks containing samples older than the specified retention period.", move |t| &get(t).compactor_blocks_retention_period, move |t| &mut get_mut(t).compactor_blocks_retention_period);
        f.i64_var("compactor.split-and-merge-shards", self.compactor_split_and_merge_shards, "The number of shards to use when splitting blocks.", move |t| &get(t).compactor_split_and_merge_shards, move |t| &mut get_mut(t).compactor_split_and_merge_shards);
        f.i64_var("compactor.ooo-split-and-merge-shards", self.compactor_ooo_split_and_merge_shards, "The number of shards to use when splitting out-of-order blocks.", move |t| &get(t).compactor_ooo_split_and_merge_shards, move |t| &mut get_mut(t).compactor_ooo_split_and_merge_shards);
        f.i64_var("compactor.split-groups", self.compactor_split_groups, "Number of groups that blocks for splitting should be grouped into.", move |t| &get(t).compactor_split_groups, move |t| &mut get_mut(t).compactor_split_groups);
        f.i64_var("compactor.compactor-tenant-shard-size", self.compactor_tenant_shard_size, "Max number of compactors that can compact blocks for single tenant.", move |t| &get(t).compactor_tenant_shard_size, move |t| &mut get_mut(t).compactor_tenant_shard_size);
        f.model_duration_var("compactor.partial-block-deletion-delay", self.compactor_partial_block_deletion_delay, "If a partial block hasn't been modified for this time, it will be marked for deletion.", move |t| &get(t).compactor_partial_block_deletion_delay, move |t| &mut get_mut(t).compactor_partial_block_deletion_delay);
        f.bool_var("compactor.block-upload-enabled", self.compactor_block_upload_enabled, "Enable block upload API for the tenant.", move |t| &get(t).compactor_block_upload_enabled, move |t| &mut get_mut(t).compactor_block_upload_enabled);
        f.bool_var("compactor.block-upload-validation-enabled", self.compactor_block_upload_validation_enabled, "Enable block upload validation for the tenant.", move |t| &get(t).compactor_block_upload_validation_enabled, move |t| &mut get_mut(t).compactor_block_upload_validation_enabled);
        f.bool_var("compactor.block-upload-verify-chunks", self.compactor_block_upload_verify_chunks, "Verify chunks when uploading blocks via the upload API for the tenant.", move |t| &get(t).compactor_block_upload_verify_chunks, move |t| &mut get_mut(t).compactor_block_upload_verify_chunks);
        f.i64_var("compactor.block-upload-max-block-size-bytes", self.compactor_block_upload_max_block_size_bytes, "Maximum size in bytes of a block that is allowed to be uploaded or validated.", move |t| &get(t).compactor_block_upload_max_block_size_bytes, move |t| &mut get_mut(t).compactor_block_upload_max_block_size_bytes);
        f.model_duration_var("compactor.max-lookback", self.compactor_max_lookback, "Blocks uploaded before the lookback aren't considered in compactor cycles.", move |t| &get(t).compactor_max_lookback, move |t| &mut get_mut(t).compactor_max_lookback);
        f.i64_var("compactor.max-per-block-upload-concurrency", self.compactor_max_per_block_upload_concurrency, "Maximum number of TSDB segment files that the compactor can upload concurrently per block.", move |t| &get(t).compactor_max_per_block_upload_concurrency, move |t| &mut get_mut(t).compactor_max_per_block_upload_concurrency);

        // Query-frontend cache & query limits
        f.model_duration_var("query-frontend.max-total-query-length", self.max_total_query_length, "Limit the total query time range.", move |t| &get(t).max_total_query_length, move |t| &mut get_mut(t).max_total_query_length);
        f.model_duration_var("query-frontend.results-cache-ttl", self.results_cache_ttl, "Time to live duration for cached query results.", move |t| &get(t).results_cache_ttl, move |t| &mut get_mut(t).results_cache_ttl);
        f.model_duration_var("query-frontend.results-cache-ttl-for-out-of-order-time-window", self.results_cache_ttl_for_out_of_order_time_window, "Time to live duration for cached query results if query falls into out-of-order time window.", move |t| &get(t).results_cache_ttl_for_out_of_order_time_window, move |t| &mut get_mut(t).results_cache_ttl_for_out_of_order_time_window);
        f.model_duration_var("query-frontend.results-cache-ttl-for-cardinality-query", self.results_cache_ttl_for_cardinality_query, "Time to live duration for cached cardinality query results.", move |t| &get(t).results_cache_ttl_for_cardinality_query, move |t| &mut get_mut(t).results_cache_ttl_for_cardinality_query);
        f.model_duration_var("query-frontend.results-cache-ttl-for-labels-query", self.results_cache_ttl_for_labels_query, "Time to live duration for cached label names and label values query results.", move |t| &get(t).results_cache_ttl_for_labels_query, move |t| &mut get_mut(t).results_cache_ttl_for_labels_query);
        f.model_duration_var("query-frontend.results-cache-ttl-for-errors", self.results_cache_ttl_for_errors, "Time to live duration for cached non-transient errors", move |t| &get(t).results_cache_ttl_for_errors, move |t| &mut get_mut(t).results_cache_ttl_for_errors);
        f.bool_var("query-frontend.cache-unaligned-requests", self.cache_unaligned_requests, "Cache requests that are not step-aligned.", move |t| &get(t).cache_unaligned_requests, move |t| &mut get_mut(t).cache_unaligned_requests);
        f.i64_var("query-frontend.max-query-expression-size-bytes", self.max_query_expression_size_bytes, "Max size of the raw query, in bytes.", move |t| &get(t).max_query_expression_size_bytes, move |t| &mut get_mut(t).max_query_expression_size_bytes);
        f.bool_var("query-frontend.align-queries-with-step", self.align_queries_with_step, "Mutate incoming queries to align their start and end with their step.", move |t| &get(t).align_queries_with_step, move |t| &mut get_mut(t).align_queries_with_step);
        f.string_slice_csv_var("query-frontend.enabled-promql-experimental-functions", &self.enabled_promql_experimental_functions, "Enable certain experimental PromQL functions.", move |t| &get(t).enabled_promql_experimental_functions, move |t| &mut get_mut(t).enabled_promql_experimental_functions);
        f.string_slice_csv_var("query-frontend.enabled-promql-extended-range-selectors", &self.enabled_promql_extended_range_selectors, "Deprecated: this setting has no effect.", move |t| &get(t).enabled_promql_extended_range_selectors, move |t| &mut get_mut(t).enabled_promql_extended_range_selectors);
        f.string_slice_csv_var("query-frontend.enabled-promql-binop-fill-modifiers", &self.enabled_promql_binop_fill_modifiers, "Enable certain experimental PromQL binary operation fill modifiers.", move |t| &get(t).enabled_promql_binop_fill_modifiers, move |t| &mut get_mut(t).enabled_promql_binop_fill_modifiers);
        f.bool_var("query-frontend.prom2-range-compat", self.prom2_range_compat, "Rewrite queries using the same range selector and resolution [X:X].", move |t| &get(t).prom2_range_compat, move |t| &mut get_mut(t).prom2_range_compat);
        f.bool_var("query-frontend.subquery-spin-off-enabled", self.subquery_spin_off_enabled, "Enable spinning off subqueries from instant queries as range queries.", move |t| &get(t).subquery_spin_off_enabled, move |t| &mut get_mut(t).subquery_spin_off_enabled);
        f.bool_var("query-frontend.labels-query-optimizer-enabled", self.labels_query_optimizer_enabled, "Enable labels query optimizations.", move |t| &get(t).labels_query_optimizer_enabled, move |t| &mut get_mut(t).labels_query_optimizer_enabled);

        // Store-gateway
        f.i64_var("store-gateway.tenant-shard-size", self.store_gateway_tenant_shard_size, "The tenant's shard size, used when store-gateway sharding is enabled.", move |t| &get(t).store_gateway_tenant_shard_size, move |t| &mut get_mut(t).store_gateway_tenant_shard_size);
        f.i64_var("store-gateway.tenant-shard-size-per-zone", self.store_gateway_tenant_shard_size_per_zone, "The tenant's shard size per availability zone.", move |t| &get(t).store_gateway_tenant_shard_size_per_zone, move |t| &mut get_mut(t).store_gateway_tenant_shard_size_per_zone);

        // Alertmanager
        f.cidr_slice_csv_var("alertmanager.receivers-firewall-block-cidr-networks", &self.alertmanager_receivers_firewall_block_cidr_networks, "Comma-separated list of network CIDRs to block in Alertmanager receiver integrations.", move |t| &get(t).alertmanager_receivers_firewall_block_cidr_networks, move |t| &mut get_mut(t).alertmanager_receivers_firewall_block_cidr_networks);
        f.bool_var("alertmanager.receivers-firewall-block-private-addresses", self.alertmanager_receivers_firewall_block_private_addresses, "True to block private and local addresses in Alertmanager receiver integrations.", move |t| &get(t).alertmanager_receivers_firewall_block_private_addresses, move |t| &mut get_mut(t).alertmanager_receivers_firewall_block_private_addresses);
        f.f64_var("alertmanager.notification-rate-limit", self.alertmanager_notification_rate_limit, "Per-tenant rate limit for sending notifications from Alertmanager.", move |t| &get(t).alertmanager_notification_rate_limit, move |t| &mut get_mut(t).alertmanager_notification_rate_limit);
        f.limits_map_var_validated("alertmanager.notification-rate-limit-per-integration", &self.alertmanager_notification_rate_limit_per_integration, "Per-integration notification rate limits.", validate_integration_limit, move |t| &get(t).alertmanager_notification_rate_limit_per_integration, move |t| &mut get_mut(t).alertmanager_notification_rate_limit_per_integration);
        f.i64_var("alertmanager.max-config-size-bytes", self.alertmanager_max_config_size_bytes, "Maximum size of the Alertmanager configuration for a tenant.", move |t| &get(t).alertmanager_max_config_size_bytes, move |t| &mut get_mut(t).alertmanager_max_config_size_bytes);
        f.i64_var("alertmanager.max-silences-count", self.alertmanager_max_silences_count, "Maximum number of silences that a tenant can have at once.", move |t| &get(t).alertmanager_max_silences_count, move |t| &mut get_mut(t).alertmanager_max_silences_count);
        f.i64_var("alertmanager.max-silence-size-bytes", self.alertmanager_max_silence_size_bytes, "Maximum silence size in bytes.", move |t| &get(t).alertmanager_max_silence_size_bytes, move |t| &mut get_mut(t).alertmanager_max_silence_size_bytes);
        f.i64_var("alertmanager.max-templates-count", self.alertmanager_max_templates_count, "Maximum number of templates in tenant's Alertmanager configuration.", move |t| &get(t).alertmanager_max_templates_count, move |t| &mut get_mut(t).alertmanager_max_templates_count);
        f.i64_var("alertmanager.max-template-size-bytes", self.alertmanager_max_template_size_bytes, "Maximum size of single template in tenant's Alertmanager configuration.", move |t| &get(t).alertmanager_max_template_size_bytes, move |t| &mut get_mut(t).alertmanager_max_template_size_bytes);
        f.i64_var("alertmanager.max-dispatcher-aggregation-groups", self.alertmanager_max_dispatcher_aggregation_groups, "Maximum number of aggregation groups in Alertmanager's dispatcher.", move |t| &get(t).alertmanager_max_dispatcher_aggregation_groups, move |t| &mut get_mut(t).alertmanager_max_dispatcher_aggregation_groups);
        f.i64_var("alertmanager.max-alerts-count", self.alertmanager_max_alerts_count, "Maximum number of alerts that a single tenant can have.", move |t| &get(t).alertmanager_max_alerts_count, move |t| &mut get_mut(t).alertmanager_max_alerts_count);
        f.i64_var("alertmanager.max-alerts-size-bytes", self.alertmanager_max_alerts_size_bytes, "Maximum total size of alerts that a single tenant can have.", move |t| &get(t).alertmanager_max_alerts_size_bytes, move |t| &mut get_mut(t).alertmanager_max_alerts_size_bytes);

        // Ingest storage
        f.string_var("ingest-storage.read-consistency", &self.ingest_storage_read_consistency, "The default consistency level to enforce for queries when using the ingest storage.", move |t| &get(t).ingest_storage_read_consistency, move |t| &mut get_mut(t).ingest_storage_read_consistency);
        f.i64_var("ingest-storage.ingestion-partition-tenant-shard-size", self.ingestion_partitions_tenant_shard_size, "The number of partitions a tenant's data should be sharded to when using the ingest storage.", move |t| &get(t).ingestion_partitions_tenant_shard_size, move |t| &mut get_mut(t).ingestion_partitions_tenant_shard_size);
        f.i64_var("ingest-storage.ingestion-partition-tenant-write-shard-size", self.ingestion_partitions_tenant_write_shard_size, "The maximum number of partitions a tenant's data should be written to when using the ingest storage.", move |t| &get(t).ingestion_partitions_tenant_write_shard_size, move |t| &mut get_mut(t).ingestion_partitions_tenant_write_shard_size);
    }

    /// Validates limits matching `pkg/util/validation/limits.go:Limits.Validate()`.
    pub fn validate(&mut self) -> Result<(), ConfigError> {
        self.validate_with_defaults(None)
    }

    /// Validates limits, falling back `name_validation_scheme` and
    /// `otel_metric_suffixes_enabled` to `defaults` when unset on a per-tenant override
    /// (without mutating `self` so `/runtime_config` preserves unset fields like Go).
    pub fn validate_with_defaults(&mut self, defaults: Option<&LimitsConfig>) -> Result<(), ConfigError> {
        let effective_scheme = match self.name_validation_scheme {
            NameValidationScheme::Unset => defaults
                .map(|d| d.name_validation_scheme)
                .unwrap_or(NameValidationScheme::Unset),
            other => other,
        };
        let validation_scheme = match effective_scheme {
            NameValidationScheme::Unset | NameValidationScheme::Legacy => {
                NameValidationScheme::Legacy
            }
            NameValidationScheme::Utf8 => NameValidationScheme::Utf8,
        };

        let otel_suffixes = self
            .otel_metric_suffixes_enabled
            .or_else(|| defaults.and_then(|d| d.otel_metric_suffixes_enabled))
            .unwrap_or(false);
        match self.otel_translation_strategy.0.as_str() {
            "UnderscoreEscapingWithoutSuffixes" => {
                if validation_scheme != NameValidationScheme::Legacy {
                    return Err(ConfigError::Validation(format!(
                        "OTLP translation strategy {} is not allowed unless validation scheme is legacy",
                        self.otel_translation_strategy
                    )));
                }
                if otel_suffixes {
                    return Err(ConfigError::Validation(format!(
                        "OTLP translation strategy {} is not allowed unless metric suffixes are disabled",
                        self.otel_translation_strategy
                    )));
                }
            }
            "UnderscoreEscapingWithSuffixes" => {
                if validation_scheme != NameValidationScheme::Legacy {
                    return Err(ConfigError::Validation(format!(
                        "OTLP translation strategy {} is not allowed unless validation scheme is legacy",
                        self.otel_translation_strategy
                    )));
                }
                if !otel_suffixes {
                    return Err(ConfigError::Validation(format!(
                        "OTLP translation strategy {} is not allowed unless metric suffixes are enabled",
                        self.otel_translation_strategy
                    )));
                }
            }
            "NoUTF8EscapingWithSuffixes" => {
                if validation_scheme != NameValidationScheme::Utf8 {
                    return Err(ConfigError::Validation(format!(
                        "OTLP translation strategy {} is not allowed unless validation scheme is utf8",
                        self.otel_translation_strategy
                    )));
                }
                if !otel_suffixes {
                    return Err(ConfigError::Validation(format!(
                        "OTLP translation strategy {} is not allowed unless metric suffixes are enabled",
                        self.otel_translation_strategy
                    )));
                }
            }
            "NoTranslation" => {
                if validation_scheme != NameValidationScheme::Utf8 {
                    return Err(ConfigError::Validation(format!(
                        "OTLP translation strategy {} is not allowed unless validation scheme is utf8",
                        self.otel_translation_strategy
                    )));
                }
                if otel_suffixes {
                    return Err(ConfigError::Validation(format!(
                        "OTLP translation strategy {} is not allowed unless metric suffixes are disabled",
                        self.otel_translation_strategy
                    )));
                }
            }
            "" => {}
            other => {
                return Err(ConfigError::Validation(format!(
                    "unsupported OTLP translation strategy {other:?}"
                )));
            }
        }

        if self.max_estimated_fetched_chunks_per_query_multiplier < 1.0
            && self.max_estimated_fetched_chunks_per_query_multiplier != 0.0
        {
            return Err(ConfigError::Validation(
                "invalid value for -querier.max-estimated-fetched-chunks-per-query-multiplier: must be 0 or greater than or equal to 1".to_string(),
            ));
        }

        if !matches!(
            self.ingest_storage_read_consistency.as_str(),
            "eventual" | "strong"
        ) {
            return Err(ConfigError::Validation(
                "invalid ingest storage read consistency (supported values: eventual, strong)"
                    .to_string(),
            ));
        }

        if self.ha_tracker_update_timeout_jitter_max.0 < 0 {
            return Err(ConfigError::Validation(
                "HA tracker max update timeout jitter shouldn't be negative".to_string(),
            ));
        }

        if self.max_blocks_per_store_request < 0 {
            return Err(ConfigError::Validation(
                "-querier.max-blocks-per-store-request must be 0 or greater".to_string(),
            ));
        }

        if !matches!(self.float_chunk_encoding.as_str(), "xor" | "xor2") {
            return Err(ConfigError::Validation(
                "invalid float chunk encoding (supported values: xor, xor2)".to_string(),
            ));
        }
        if !matches!(
            self.histogram_chunk_encoding.as_str(),
            "" | "histogram" | "histogram_st"
        ) {
            return Err(ConfigError::Validation(
                "invalid histogram chunk encoding (supported values: histogram, histogram_st)"
                    .to_string(),
            ));
        }

        if self.ha_tracker_update_timeout.0 > 0 || self.ha_tracker_failover_timeout.0 > 0 {
            let min_failure_timeout = self.ha_tracker_update_timeout.0
                + self.ha_tracker_update_timeout_jitter_max.0
                + 1_000_000_000;
            if self.ha_tracker_failover_timeout.0 < min_failure_timeout {
                return Err(ConfigError::Validation(format!(
                    "HA Tracker failover timeout ({}) must be at least 1s greater than update timeout - max jitter ({})",
                    self.ha_tracker_failover_timeout,
                    ModelDuration(min_failure_timeout)
                )));
            }
        }

        if matches!(
            self.label_value_length_over_limit_strategy,
            LabelValueLengthOverLimitStrategy::Truncate | LabelValueLengthOverLimitStrategy::Drop
        ) && self.max_label_value_length < LABEL_VALUE_HASH_LEN as i64
        {
            return Err(ConfigError::Validation(format!(
                "cannot set -validation.label-value-length-over-limit-strategy to {:?}: label value hash suffix would exceed max label value length of {}",
                self.label_value_length_over_limit_strategy.as_str(),
                self.max_label_value_length
            )));
        }

        if !self.cost_attribution_labels_structured.is_empty() {
            if !self.cost_attribution_trackers.is_empty() {
                return Err(ConfigError::Validation(
                    "cost_attribution_labels_structured and cost_attribution_trackers are mutually exclusive; use cost_attribution_trackers only".to_string(),
                ));
            }
            self.cost_attribution_trackers.insert(
                "cost-attribution".to_string(),
                CostAttributionTrackerConfig {
                    labels: std::mem::take(&mut self.cost_attribution_labels_structured),
                    internal: false,
                },
            );
        }
        for (k, v) in &self.alertmanager_notification_rate_limit_per_integration {
            validate_integration_limit(k, v)?;
        }
        if let Some(v) = CUSTOM_TRACKERS_VALIDATOR.get() {
            v(&self.active_series_custom_trackers).map_err(ConfigError::Validation)?;
            v(&self.active_series_additional_custom_trackers).map_err(ConfigError::Validation)?;
        }
        validate_cost_attribution_trackers(&self.cost_attribution_trackers)?;
        validate_cost_attribution_trackers(&self.additional_cost_attribution_trackers)?;
        for cfg in self.cost_attribution_trackers.values_mut() {
            cfg.labels.sort_by(|a, b| a.input.cmp(&b.input));
        }
        for cfg in self.additional_cost_attribution_trackers.values_mut() {
            cfg.labels.sort_by(|a, b| a.input.cmp(&b.input));
        }

        if self.early_head_compaction_min_estimated_series_reduction_percentage < 0
            || self.early_head_compaction_min_estimated_series_reduction_percentage > 100
        {
            return Err(ConfigError::Validation(
                "early_head_compaction_min_estimated_series_reduction_percentage must be between 0 and 100".to_string(),
            ));
        }

        if self.max_active_series_additional_custom_trackers < 0 {
            return Err(ConfigError::Validation(format!(
                "active_series_additional_custom_trackers validation failed: invalid max custom trackers limit: {}",
                self.max_active_series_additional_custom_trackers
            )));
        }

        if self.max_active_series_additional_custom_trackers > 0
            && self.active_series_additional_custom_trackers.len()
                > self.max_active_series_additional_custom_trackers as usize
        {
            return Err(ConfigError::Validation(format!(
                "active_series_additional_custom_trackers validation failed: number of additional custom trackers ({}) exceeds limit ({})",
                self.active_series_additional_custom_trackers.len(),
                self.max_active_series_additional_custom_trackers
            )));
        }

        for (i, q) in self.blocked_queries.iter().enumerate() {
            if q.pattern.trim().is_empty() {
                return Err(ConfigError::Validation(format!(
                    "blocked_queries[{i}]: pattern is required"
                )));
            }
            if q.regex {
                if let Err(e) = regex::Regex::new(&q.pattern) {
                    return Err(ConfigError::Validation(format!(
                        "blocked_queries[{i}]: invalid regex pattern {:?}: {e}",
                        q.pattern
                    )));
                }
            }
            if let Some(ts) = q.created_at.as_deref() {
                parse_go_yaml_timestamp(ts)?;
            }
            if let Some(ts) = q.expires_at.as_deref() {
                parse_go_yaml_timestamp(ts)?;
            }
        }
        for q in &self.limited_queries {
            if let Some(ts) = q.created_at.as_deref() {
                parse_go_yaml_timestamp(ts)?;
            }
            if let Some(ts) = q.expires_at.as_deref() {
                parse_go_yaml_timestamp(ts)?;
            }
        }

        // Round shard counts up to next power of two, matching validation/limits.go.
        if self.query_sharding_total_shards > 1 {
            self.query_sharding_total_shards =
                next_power_of_two(self.query_sharding_total_shards);
        }
        if self.compactor_split_and_merge_shards > 1 {
            self.compactor_split_and_merge_shards =
                next_power_of_two(self.compactor_split_and_merge_shards);
        }
        if self.compactor_ooo_split_and_merge_shards > 1 {
            self.compactor_ooo_split_and_merge_shards =
                next_power_of_two(self.compactor_ooo_split_and_merge_shards);
        }

        // Normalize blocked label names: drop empty, duplicates, and __name__.
        if !self.blocked_label_names_for_label_value_bytes.0.is_empty() {
            let mut normalized = Vec::new();
            for name in &self.blocked_label_names_for_label_value_bytes.0 {
                if name.is_empty() || name == "__name__" || normalized.contains(name) {
                    continue;
                }
                normalized.push(name.clone());
            }
            self.blocked_label_names_for_label_value_bytes.0 = normalized;
        }

        Ok(())
    }
}

/// Ported from `costattributionmodel.TrackerConfigs.Validate` / `Labels.Validate` / `Label.Validate`.
fn validate_cost_attribution_trackers(tc: &CostAttributionTrackerConfigs) -> Result<(), ConfigError> {
    let label_str = |l: &CostAttributionLabel| format!("{}:{}", l.input, l.output);
    for (name, cfg) in tc {
        if name.is_empty() {
            return Err(ConfigError::Validation(
                "cost attribution tracker name must not be empty".to_string(),
            ));
        }
        let mut outputs = std::collections::HashSet::new();
        for l in &cfg.labels {
            let err = if l.input.is_empty() {
                Some(format!(
                    "cost attribution input label must not be empty: {:?}",
                    label_str(l)
                ))
            } else if l.output.starts_with("__") {
                Some(format!(
                    "invalid cost attribution output label: {:?}",
                    label_str(l)
                ))
            } else {
                let out = if l.output.is_empty() { &l.input } else { &l.output };
                (!outputs.insert(out.clone()))
                    .then(|| format!("duplicate output label: {:?}", label_str(l)))
            };
            if let Some(e) = err {
                return Err(ConfigError::Validation(format!(
                    "cost attribution tracker {name:?}: {e}"
                )));
            }
        }
        if cfg.labels.is_empty() {
            return Err(ConfigError::Validation(format!(
                "cost attribution tracker {name:?} must have at least one label"
            )));
        }
    }
    Ok(())
}

/// Validates an active series custom trackers map (`name -> matchers`), returning the Go
/// `NewCustomTrackersConfig` error on invalid matchers.
pub type CustomTrackersValidator = fn(&BTreeMap<String, String>) -> Result<(), String>;

static CUSTOM_TRACKERS_VALIDATOR: OnceLock<CustomTrackersValidator> = OnceLock::new();

/// Installs the matcher validator used by [`LimitsConfig::validate`] for
/// `active_series_custom_trackers` / `active_series_additional_custom_trackers`. The matcher
/// parser lives in `mimir-ingester` (`activeseries/model`), so the binary wires it at startup;
/// Go fails config (and runtime config) loading on invalid matchers in `UnmarshalYAML`.
pub fn set_active_series_custom_trackers_validator(f: CustomTrackersValidator) {
    let _ = CUSTOM_TRACKERS_VALIDATOR.set(f);
}

/// YAML-decode-time custom trackers validation (Go: `CustomTrackersConfig.UnmarshalYAML`).
pub(crate) fn validate_custom_trackers_yaml(l: &LimitsConfig) -> Result<(), ConfigError> {
    if let Some(v) = CUSTOM_TRACKERS_VALIDATOR.get() {
        v(&l.active_series_custom_trackers).map_err(ConfigError::Yaml)?;
        v(&l.active_series_additional_custom_trackers).map_err(ConfigError::Yaml)?;
    }
    Ok(())
}

const ALLOWED_INTEGRATION_NAMES: &[&str] = &[
    "webhook", "email", "pagerduty", "opsgenie", "wechat", "slack", "victorops",
    "pushover", "sns", "webex", "telegram", "discord", "msteams", "msteamsv2", "jira",
];

pub(crate) fn validate_integration_limit(k: &str, _v: &f64) -> Result<(), ConfigError> {
    if !ALLOWED_INTEGRATION_NAMES.contains(&k) {
        return Err(ConfigError::Flag(format!("unknown integration name: {k}")));
    }
    Ok(())
}

fn deserialize_metric_relabel_configs<'de, D>(
    deserializer: D,
) -> Result<Vec<MetricRelabelConfig>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let items: Vec<Option<MetricRelabelConfig>> = serde::Deserialize::deserialize(deserializer)?;
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        let Some(cfg) = item else {
            return Err(serde::de::Error::custom("invalid metric_relabel_configs"));
        };
        if let Some(action) = cfg.action.as_deref() {
            let lower = action.to_ascii_lowercase();
            if !matches!(
                lower.as_str(),
                "replace" | "keep" | "drop" | "hashmod" | "labelmap" | "labeldrop" | "labelkeep"
                    | "lowercase" | "uppercase" | "keepequal" | "dropequal"
            ) {
                return Err(serde::de::Error::custom(format!(
                    "unknown relabel action {action:?}"
                )));
            }
        }
        if let Some(re) = cfg.regex.as_deref() {
            if let Err(e) = regex::Regex::new(re) {
                return Err(serde::de::Error::custom(format!(
                    "invalid relabel regex {re:?}: {e}"
                )));
            }
        }
        out.push(cfg);
    }
    Ok(out)
}

fn next_power_of_two(n: i64) -> i64 {
    if n <= 1 {
        return n;
    }
    (n as u64).next_power_of_two() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_rejects_negative_max_active_series_additional_custom_trackers() {
        let mut limits = LimitsConfig {
            max_active_series_additional_custom_trackers: -1,
            ..Default::default()
        };
        match limits.validate() {
            Err(ConfigError::Validation(msg)) => assert_eq!(
                msg,
                "active_series_additional_custom_trackers validation failed: invalid max custom trackers limit: -1"
            ),
            other => panic!("expected ConfigError::Validation, got {other:?}"),
        }
    }

    #[test]
    fn validate_rejects_negative_max_label_value_length_with_truncate_or_drop() {
        for strategy in [
            LabelValueLengthOverLimitStrategy::Truncate,
            LabelValueLengthOverLimitStrategy::Drop,
        ] {
            let mut limits = LimitsConfig {
                max_label_value_length: -1,
                label_value_length_over_limit_strategy: strategy,
                ..Default::default()
            };
            match limits.validate() {
                Err(ConfigError::Validation(msg)) => assert!(
                    msg.contains("label value hash suffix would exceed max label value length of -1"),
                    "unexpected validation msg for {strategy:?}: {msg}"
                ),
                other => panic!("expected ConfigError::Validation for {strategy:?}, got {other:?}"),
            }
        }
    }
}
