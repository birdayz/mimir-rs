//! Per-tenant limits configuration and validation.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/util/validation/limits.go
//! - github.com/grafana/mimir/pkg/util/validation/blocked_query.go
//! - github.com/grafana/mimir/pkg/util/validation/limited_query.go
//! - github.com/grafana/mimir/pkg/util/validation/blocked_request.go
//! - github.com/grafana/mimir/pkg/costattribution/costattributionmodel/*

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::OnceLock;

use chrono::{DateTime, Datelike, Utc};
use serde::{Deserialize, Serialize};

use crate::duration::{GoDuration, ModelDuration};
use crate::error::ConfigError;
use crate::flags::FlagSet;
use crate::types::{
    parse_go_yaml_timestamp, parse_go_yaml_timestamp_dt, CIDRSliceCSV,
    LabelValueLengthOverLimitStrategy, LimitsMap, NameValidationScheme, OTelTranslationStrategy,
    StringSliceCSV,
};

pub const MIN_COMPACTOR_PARTIAL_BLOCK_DELETION_DELAY_NANOS: i64 = 4 * 3600 * 1_000_000_000;
pub const LABEL_VALUE_HASH_LEN: usize = "(hash:)".len() + 32 * 2;

fn is_timestamp_expired(expires_at: Option<&str>, now: DateTime<Utc>) -> bool {
    let Some(raw) = expires_at else {
        return false;
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return false;
    }
    let Ok(dt) = parse_go_yaml_timestamp_dt(trimmed) else {
        return false;
    };
    // Go time.Time{} is year 0001-01-01T00:00:00Z.
    if dt.year() <= 1 && dt.timestamp() <= -62_135_596_800 {
        return false;
    }
    now > dt
}

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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<String>,
}

impl BlockedQuery {
    /// Reports whether `expires_at` is set and in the past, relative to `now`.
    pub fn is_expired(&self, now: DateTime<Utc>) -> bool {
        is_timestamp_expired(self.expires_at.as_deref(), now)
    }
}

pub type BlockedQueriesConfig = Vec<BlockedQuery>;

pub fn validate_blocked_queries(queries: &[BlockedQuery]) -> Result<(), ConfigError> {
    for (i, q) in queries.iter().enumerate() {
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
    Ok(())
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

impl LimitedQuery {
    /// Reports whether `expires_at` is set and in the past, relative to `now`.
    pub fn is_expired(&self, now: DateTime<Utc>) -> bool {
        is_timestamp_expired(self.expires_at.as_deref(), now)
    }
}

pub const DEFAULT_FLOAT_CHUNK_ENCODING_VALUE: &str = "xor";
pub const FLOAT_CHUNK_ENCODING_VALUES: &[&str] = &["xor", "xor2"];

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum FloatChunkEncoding {
    Xor,
    Xor2,
}

pub fn parse_float_chunk_encoding(value: &str) -> FloatChunkEncoding {
    match value {
        "xor2" => FloatChunkEncoding::Xor2,
        _ => FloatChunkEncoding::Xor,
    }
}

pub const DEFAULT_HISTOGRAM_CHUNK_ENCODING_VALUE: &str = "histogram";
pub const HISTOGRAM_CHUNK_ENCODING_ST: &str = "histogram_st";
pub const HISTOGRAM_CHUNK_ENCODING_VALUES: &[&str] = &["histogram", "histogram_st"];

pub fn parse_histogram_st_encoding_enabled(value: &str) -> bool {
    value == HISTOGRAM_CHUNK_ENCODING_ST
}

/// Marker error for errors that do not comply with configured limits (`validation.LimitError`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LimitError(pub String);

impl fmt::Display for LimitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for LimitError {}

pub fn new_limit_error(msg: impl Into<String>) -> LimitError {
    LimitError(msg.into())
}

pub fn is_limit_error(err: &(dyn std::error::Error + 'static)) -> bool {
    let mut cur: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(e) = cur {
        if e.downcast_ref::<LimitError>().is_some() {
            return true;
        }
        cur = e.source();
    }
    false
}

/// HTTP JSON response returned by `/api/v1/user_limits` (`validation.UserLimitsResponse`).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct UserLimitsResponse {
    pub compactor_blocks_retention_period_seconds: i64,
    pub ingestion_rate: f64,
    pub ingestion_burst_size: i64,
    pub ingestion_burst_factor: f64,
    pub max_global_series_per_user: i64,
    pub max_global_series_per_metric: i64,
    pub max_global_exemplars_per_user: i64,
    pub max_fetched_chunks_per_query: i64,
    pub max_fetched_series_per_query: i64,
    pub max_fetched_chunk_bytes_per_query: i64,
    pub ruler_max_rules_per_rule_group: i64,
    pub ruler_max_rule_groups_per_tenant: i64,
    pub alertmanager_notification_rate_limit: f64,
    pub alertmanager_max_dispatcher_aggregation_groups: i64,
    pub alertmanager_max_templates_count: i64,
    pub alertmanager_max_alerts_count: i64,
}

impl From<&LimitsConfig> for UserLimitsResponse {
    fn from(l: &LimitsConfig) -> Self {
        Self {
            compactor_blocks_retention_period_seconds: l.compactor_blocks_retention_period.0
                / 1_000_000_000,
            ingestion_rate: l.ingestion_rate,
            ingestion_burst_size: l.ingestion_burst_size,
            ingestion_burst_factor: l.ingestion_burst_factor,
            max_global_series_per_user: l.max_global_series_per_user,
            max_global_series_per_metric: l.max_global_series_per_metric,
            max_global_exemplars_per_user: l.max_global_exemplars_per_user,
            max_fetched_chunks_per_query: l.max_fetched_chunks_per_query,
            max_fetched_series_per_query: l.max_fetched_series_per_query,
            max_fetched_chunk_bytes_per_query: l.max_fetched_chunk_bytes_per_query,
            ruler_max_rules_per_rule_group: l.ruler_max_rules_per_rule_group,
            ruler_max_rule_groups_per_tenant: l.ruler_max_rule_groups_per_tenant,
            alertmanager_notification_rate_limit: l.alertmanager_notification_rate_limit,
            alertmanager_max_dispatcher_aggregation_groups: l
                .alertmanager_max_dispatcher_aggregation_groups,
            alertmanager_max_templates_count: l.alertmanager_max_templates_count,
            alertmanager_max_alerts_count: l.alertmanager_max_alerts_count,
        }
    }
}

/// Pure handler matching `pkg/util/validation/user_limits_handler.go:UserLimitsHandler`.
pub fn handle_user_limits_request(
    org_id_header: Option<&[u8]>,
    overrides: &crate::runtime_config::Overrides,
) -> Result<UserLimitsResponse, (u16, String)> {
    let Some(raw_org) = org_id_header else {
        return Err((401, "no org id".to_string()));
    };
    let user_id = mimir_common::tenant::parse_single_tenant_id_bytes(raw_org)
        .map_err(|e| (401, e.to_string()))?;
    let limits = overrides.for_tenant(&user_id);
    Ok(UserLimitsResponse::from(limits.as_ref()))
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct MetricRelabelConfig {
    pub source_labels: Vec<String>,
    pub separator: Option<String>,
    pub regex: Option<String>,
    pub modulus: u64,
    pub target_label: Option<String>,
    pub replacement: Option<String>,
    pub action: Option<String>,
}

impl Default for MetricRelabelConfig {
    /// Upstream Go `relabel.DefaultRelabelConfig` (`model/relabel/relabel.go:40-46`).
    fn default() -> Self {
        Self {
            source_labels: Vec::new(),
            separator: Some(";".to_string()),
            regex: Some("(.*)".to_string()),
            modulus: 0,
            target_label: None,
            replacement: Some("$1".to_string()),
            action: Some("replace".to_string()),
        }
    }
}

fn deserialize_present_opt<'de, D, T>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer).map(Some)
}

impl<'de> Deserialize<'de> for MetricRelabelConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct RawMetricRelabelConfig {
            #[serde(default)]
            source_labels: Option<Vec<String>>,
            #[serde(default, deserialize_with = "deserialize_present_opt")]
            separator: Option<Option<String>>,
            #[serde(default, deserialize_with = "deserialize_present_opt")]
            regex: Option<Option<String>>,
            #[serde(default)]
            modulus: Option<u64>,
            #[serde(default, deserialize_with = "deserialize_present_opt")]
            target_label: Option<Option<String>>,
            #[serde(default, deserialize_with = "deserialize_present_opt")]
            replacement: Option<Option<String>>,
            #[serde(default, deserialize_with = "deserialize_present_opt")]
            action: Option<Option<String>>,
        }

        let raw = RawMetricRelabelConfig::deserialize(deserializer)?;
        let mut out = Self::default();
        if let Some(source_labels) = raw.source_labels {
            out.source_labels = source_labels;
        }
        if let Some(Some(separator)) = raw.separator {
            out.separator = Some(separator);
        }
        match raw.regex {
            None => {}
            Some(None) => {
                // Go `relabel.Config.UnmarshalYAML`: explicit `regex: null` maps to `""`.
                out.regex = Some(String::new());
            }
            Some(Some(re)) => {
                if let Err(e) = regex::Regex::new(&re) {
                    return Err(serde::de::Error::custom(format!(
                        "invalid relabel regex {re:?}: {e}"
                    )));
                }
                out.regex = Some(re);
            }
        }
        if let Some(modulus) = raw.modulus {
            out.modulus = modulus;
        }
        if let Some(Some(target_label)) = raw.target_label {
            out.target_label = Some(target_label);
        }
        if let Some(Some(replacement)) = raw.replacement {
            out.replacement = Some(replacement);
        }
        if let Some(Some(action)) = raw.action {
            let lower = action.to_ascii_lowercase();
            if !matches!(
                lower.as_str(),
                "replace"
                    | "keep"
                    | "drop"
                    | "hashmod"
                    | "labelmap"
                    | "labeldrop"
                    | "labelkeep"
                    | "lowercase"
                    | "uppercase"
                    | "keepequal"
                    | "dropequal"
            ) {
                return Err(serde::de::Error::custom(format!(
                    "unknown relabel action {action:?}"
                )));
            }
            out.action = Some(lower);
        }
        Ok(out)
    }
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
    #[serde(deserialize_with = "deserialize_notification_rate_limit_map")]
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

    // Registered dynamic extensions (Go: Limits.extensions, skipped in direct serde).
    #[serde(skip)]
    pub extensions: BTreeMap<String, serde_json::Value>,
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
            extensions: BTreeMap::new(),
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
                let validator = active_custom_trackers_validator();
                validator(&source).map_err(ConfigError::Flag)?;
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
                let trimmed = s.trim();
                if trimmed.is_empty() {
                    return Ok(());
                }
                let mut parsed: CostAttributionTrackerConfigs =
                    serde_json::from_str(trimmed).map_err(|e| {
                        ConfigError::Flag(format!("failed to parse cost attribution trackers: {e}"))
                    })?;
                validate_cost_attribution_trackers(&parsed).map_err(|e| match e {
                    ConfigError::Validation(msg) | ConfigError::Flag(msg) => {
                        ConfigError::Flag(msg)
                    }
                    other => other,
                })?;
                for cfg in parsed.values_mut() {
                    cfg.labels.sort_by(|a, b| a.input.cmp(&b.input));
                }
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

    /// Migrates legacy `cost_attribution_labels_structured` into `cost_attribution_trackers`,
    /// matching Go `Limits.migrateCostAttributionLabelsStructured`.
    pub fn migrate_cost_attribution_labels_structured(&mut self) -> Result<(), ConfigError> {
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
        Ok(())
    }

    /// Validates limits matching `pkg/util/validation/limits.go:Limits.Validate()`.
    pub fn validate(&mut self) -> Result<(), ConfigError> {
        let validation_scheme = match self.name_validation_scheme {
            NameValidationScheme::Unset | NameValidationScheme::Legacy => {
                NameValidationScheme::Legacy
            }
            NameValidationScheme::Utf8 => NameValidationScheme::Utf8,
        };

        let otel_suffixes = self.otel_metric_suffixes_enabled.unwrap_or(false);
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

        self.migrate_cost_attribution_labels_structured()?;
        for (k, v) in &self.alertmanager_notification_rate_limit_per_integration {
            validate_integration_limit(k, v)?;
        }
        let validator = active_custom_trackers_validator();
        validator(&self.active_series_custom_trackers).map_err(ConfigError::Validation)?;
        validator(&self.active_series_additional_custom_trackers).map_err(ConfigError::Validation)?;
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
                "active_series_additional_custom_trackers validation failed: the number of custom trackers [{}] exceeds the configured limit [{}]",
                self.active_series_additional_custom_trackers.len(),
                self.max_active_series_additional_custom_trackers
            )));
        }

        validate_blocked_queries(&self.blocked_queries)?;
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

fn active_custom_trackers_validator() -> CustomTrackersValidator {
    CUSTOM_TRACKERS_VALIDATOR
        .get()
        .copied()
        .unwrap_or(default_validate_custom_trackers)
}

/// Built-in fallback matcher validator (matching Alertmanager `labels.ParseMatchers` and
/// `CustomTrackersConfig.NewCustomTrackersConfig`) used when `mimir-cmd` has not installed the
/// `mimir-ingester` hook.
fn default_validate_custom_trackers(m: &BTreeMap<String, String>) -> Result<(), String> {
    const MAX_NUMBER_OF_TRACKERS: usize = u16::MAX as usize;
    if m.len() > MAX_NUMBER_OF_TRACKERS {
        return Err(format!(
            "the number of trackers set [{}] exceeds the maximum number of trackers [{MAX_NUMBER_OF_TRACKERS}]",
            m.len()
        ));
    }
    for (name, matcher) in m {
        validate_am_matchers(matcher)
            .map_err(|e| format!("can't build active series matcher {name}: {e}"))?;
    }
    Ok(())
}

fn validate_am_matchers(s: &str) -> Result<(), String> {
    let s = s.strip_prefix('{').unwrap_or(s);
    let s = s.strip_suffix('}').unwrap_or(s);

    let mut inside_quotes = false;
    let mut escaped = false;
    let mut token = String::new();
    let mut tokens = Vec::new();

    for ch in s.chars() {
        match ch {
            ',' if !inside_quotes => {
                tokens.push(std::mem::take(&mut token));
                continue;
            }
            '"' => {
                if !escaped {
                    inside_quotes = !inside_quotes;
                } else {
                    escaped = false;
                }
            }
            '\\' => {
                escaped = !escaped;
            }
            _ => {
                escaped = false;
            }
        }
        token.push(ch);
    }

    let rem = token.trim();
    if !rem.is_empty() {
        tokens.push(rem.to_string());
    }

    for tok in tokens {
        validate_am_matcher(&tok)?;
    }
    Ok(())
}

fn validate_am_matcher(s: &str) -> Result<(), String> {
    let trimmed = s.trim_start();
    let bytes = trimmed.as_bytes();
    if bytes.is_empty() || !(bytes[0].is_ascii_alphabetic() || bytes[0] == b'_' || bytes[0] == b':')
    {
        return Err(format!("bad matcher format: {s}"));
    }
    let mut i = 1;
    while i < bytes.len()
        && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_' || bytes[i] == b':')
    {
        i += 1;
    }
    let rest = trimmed[i..].trim_start();

    let (is_regex, after_op) = if let Some(r) = rest.strip_prefix("=~") {
        (true, r)
    } else if let Some(r) = rest.strip_prefix("!~") {
        (true, r)
    } else if let Some(r) = rest.strip_prefix("!=") {
        (false, r)
    } else if let Some(r) = rest.strip_prefix('=') {
        (false, r)
    } else {
        return Err(format!("bad matcher format: {s}"));
    };

    let raw_trimmed = after_op.trim();
    let mut raw_value = raw_trimmed;
    let mut expect_trailing_quote = false;
    if let Some(after) = raw_value.strip_prefix('"') {
        raw_value = after;
        expect_trailing_quote = true;
    }

    let mut value = String::with_capacity(raw_value.len());
    let mut escaped = false;
    let mut chars = raw_value.char_indices().peekable();
    while let Some((idx, ch)) = chars.next() {
        let is_last = chars.peek().is_none();
        if escaped {
            escaped = false;
            match ch {
                'n' => value.push('\n'),
                '"' | '\\' => value.push(ch),
                _ => {
                    value.push('\\');
                    value.push(ch);
                }
            }
            continue;
        }
        match ch {
            '\\' => {
                if !is_last {
                    escaped = true;
                    continue;
                }
                value.push('\\');
            }
            '"' => {
                if !expect_trailing_quote || idx + ch.len_utf8() < raw_value.len() {
                    return Err(format!(
                        "matcher value contains unescaped double quote: {raw_trimmed}"
                    ));
                }
                expect_trailing_quote = false;
            }
            _ => value.push(ch),
        }
    }

    if expect_trailing_quote {
        return Err(format!(
            "matcher value contains unescaped double quote: {raw_trimmed}"
        ));
    }

    if is_regex {
        regex::Regex::new(&value).map_err(|e| format!("invalid regex: {e}"))?;
    }
    Ok(())
}

/// YAML-decode-time custom trackers validation (Go: `CustomTrackersConfig.UnmarshalYAML`).
pub(crate) fn validate_custom_trackers_yaml(l: &LimitsConfig) -> Result<(), ConfigError> {
    let validator = active_custom_trackers_validator();
    validator(&l.active_series_custom_trackers).map_err(ConfigError::Yaml)?;
    validator(&l.active_series_additional_custom_trackers).map_err(ConfigError::Yaml)?;
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

fn deserialize_notification_rate_limit_map<'de, D>(
    deserializer: D,
) -> Result<LimitsMap<f64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw: Option<LimitsMap<Option<f64>>> = serde::Deserialize::deserialize(deserializer)?;
    let mut out = LimitsMap::new();
    for (k, v) in raw.unwrap_or_default() {
        let val = v.unwrap_or(0.0);
        if let Err(e) = validate_integration_limit(&k, &val) {
            let msg = match e {
                ConfigError::Flag(m) | ConfigError::Validation(m) => m,
                other => other.to_string(),
            };
            return Err(serde::de::Error::custom(msg));
        }
        out.insert(k, val);
    }
    Ok(out)
}

/// Validated notification rate limit map (`pkg/util/validation/notifications_limit_flag.go`).
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct NotificationRateLimitMap(pub LimitsMap<f64>);

impl NotificationRateLimitMap {
    pub fn new() -> Self {
        Self(LimitsMap::new())
    }

    pub fn with_data(data: LimitsMap<f64>) -> Result<Self, ConfigError> {
        for (k, v) in &data {
            validate_integration_limit(k, v)?;
        }
        Ok(Self(data))
    }

    pub fn set(&mut self, s: &str) -> Result<(), ConfigError> {
        let raw: Option<LimitsMap<Option<f64>>> =
            serde_json::from_str(s).map_err(|e| ConfigError::Flag(e.to_string()))?;
        let mut out = LimitsMap::new();
        for (k, v) in raw.unwrap_or_default() {
            let val = v.unwrap_or(0.0);
            validate_integration_limit(&k, &val)?;
            out.insert(k, val);
        }
        self.0 = out;
        Ok(())
    }
}

impl<'de> serde::Deserialize<'de> for NotificationRateLimitMap {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserialize_notification_rate_limit_map(deserializer).map(Self)
    }
}

impl LimitsConfig {
    /// Canonicalizes non-regex `blocked_queries` patterns via PromQL selector normalization
    /// (`pkg/util/validation/limits.go:canonicalizeQueries`).
    pub fn canonicalize_queries(&mut self) {
        for q in &mut self.blocked_queries {
            if q.regex {
                continue;
            }
            if let Some(canon) = canonicalize_promql_expr(&q.pattern) {
                if canon != q.pattern {
                    q.pattern = canon;
                }
            }
        }
    }

    /// Returns a zeroed `LimitsConfig` (matching Go `Limits{}` before `flagext.DefaultValues`).
    pub fn zeroed() -> Self {
        Self {
            active_series_limit_response_code: 0,
            ingestion_rate: 0.0,
            ingestion_burst_size: 0,
            ha_cluster_label: String::new(),
            ha_replica_label: String::new(),
            ha_max_clusters: 0,
            ha_tracker_update_timeout: ModelDuration::ZERO,
            ha_tracker_update_timeout_jitter_max: ModelDuration::ZERO,
            ha_tracker_failover_timeout: ModelDuration::ZERO,
            max_label_name_length: 0,
            max_label_value_length: 0,
            max_label_names_per_series: 0,
            max_label_names_per_info_series: 0,
            max_metadata_length: 0,
            reduce_native_histogram_over_max_buckets: false,
            creation_grace_period: ModelDuration::ZERO,
            enforce_metadata_metric_name: false,
            max_global_series_per_user: 0,
            early_head_compaction_min_estimated_series_reduction_percentage: 0,
            native_histograms_ingestion_enabled: false,
            float_chunk_encoding: String::new(),
            histogram_chunk_encoding: String::new(),
            max_fetched_chunks_per_query: 0,
            max_blocks_per_store_request: 0,
            max_query_parallelism: 0,
            max_cache_freshness: ModelDuration::ZERO,
            query_sharding_total_shards: 0,
            query_sharding_max_sharded_queries: 0,
            query_sharding_max_regexp_size_bytes: 0,
            query_ingesters_within: ModelDuration::ZERO,
            results_cache_ttl: ModelDuration::ZERO,
            results_cache_ttl_for_out_of_order_time_window: ModelDuration::ZERO,
            results_cache_ttl_for_errors: ModelDuration::ZERO,
            labels_query_optimizer_enabled: false,
            label_names_and_values_results_max_size_bytes: 0,
            label_values_max_cardinality_label_names_per_request: 0,
            cardinality_analysis_max_results: 0,
            active_series_results_max_size_bytes: 0,
            max_cost_attribution_cardinality: 0,
            ruler_evaluation_delay_duration: ModelDuration::ZERO,
            ruler_max_rules_per_rule_group: 0,
            ruler_max_rule_groups_per_tenant: 0,
            ruler_recording_rules_evaluation_enabled: false,
            ruler_alerting_rules_evaluation_enabled: false,
            ruler_sync_rules_on_changes_enabled: false,
            ruler_max_independent_rule_evaluation_concurrency_per_tenant: 0,
            ruler_alertmanager_client_config: RulerAlertmanagerClientConfig {
                tls_enabled: false,
                ..Default::default()
            },
            compactor_split_groups: 0,
            compactor_partial_block_deletion_delay: ModelDuration::ZERO,
            compactor_block_upload_validation_enabled: false,
            compactor_block_upload_verify_chunks: false,
            compactor_max_per_block_upload_concurrency: 0,
            otel_metric_suffixes_enabled: None,
            otel_label_name_underscore_sanitization: false,
            otel_label_name_preserve_multiple_underscores: false,
            ingest_storage_read_consistency: String::new(),
            name_validation_scheme: NameValidationScheme::Unset,
            ..Default::default()
        }
    }

    /// Registers defaults for all extensions active in the current thread
    /// (`Limits.RegisterExtensionsDefaults`).
    pub fn register_extensions_defaults(&mut self) {
        for (name, def_fn) in current_thread_extensions() {
            self.extensions.insert(name, def_fn());
        }
    }

    /// Unmarshals YAML into `LimitsConfig` using `LimitsConfig::default()` as the base
    /// (`Limits.UnmarshalYAML`).
    pub fn unmarshal_yaml(yaml: &str) -> Result<Self, ConfigError> {
        Self::unmarshal_yaml_with_defaults(yaml, &Self::default())
    }

    /// Unmarshals YAML into `LimitsConfig` starting from `defaults`, mirroring Go
    /// `Limits.unmarshal`.
    pub fn unmarshal_yaml_with_defaults(yaml: &str, defaults: &Self) -> Result<Self, ConfigError> {
        let input: serde_yaml::Value =
            serde_yaml::from_str(yaml).map_err(|e| ConfigError::Yaml(e.to_string()))?;
        Self::unmarshal_yaml_value_with_defaults(input, defaults)
    }

    /// Unmarshals JSON into `LimitsConfig` using `LimitsConfig::default()` as the base
    /// (`Limits.UnmarshalJSON`).
    pub fn unmarshal_json(json: &str) -> Result<Self, ConfigError> {
        Self::unmarshal_json_with_defaults(json, &Self::default())
    }

    /// Unmarshals JSON into `LimitsConfig` starting from `defaults`, mirroring Go
    /// `Limits.unmarshal`.
    pub fn unmarshal_json_with_defaults(json: &str, defaults: &Self) -> Result<Self, ConfigError> {
        let json_val: serde_json::Value =
            serde_json::from_str(json).map_err(|e| ConfigError::Yaml(e.to_string()))?;
        let yaml_val =
            serde_yaml::to_value(&json_val).map_err(|e| ConfigError::Yaml(e.to_string()))?;
        Self::unmarshal_yaml_value_with_defaults(yaml_val, defaults)
    }

    /// Unmarshals an already-parsed map/value into `LimitsConfig` starting from `defaults`,
    /// mirroring `DecodeLimitsMap`.
    pub fn unmarshal_map_with_defaults(
        input: serde_yaml::Value,
        defaults: &Self,
    ) -> Result<Self, ConfigError> {
        Self::unmarshal_yaml_value_with_defaults(input, defaults)
    }

    /// Unmarshals `map[string]*Limits` YAML (used in `limits_test.go` override tests).
    pub fn unmarshal_overrides_yaml_with_defaults(
        yaml: &str,
        defaults: &Self,
    ) -> Result<BTreeMap<String, Option<Self>>, ConfigError> {
        let input: serde_yaml::Value =
            serde_yaml::from_str(yaml).map_err(|e| ConfigError::Yaml(e.to_string()))?;
        if input.is_null() {
            return Ok(BTreeMap::new());
        }
        let map = input.as_mapping().ok_or_else(|| {
            ConfigError::Yaml("expected YAML mapping for overrides".to_string())
        })?;
        let mut out = BTreeMap::new();
        for (k, v) in map {
            let key = k
                .as_str()
                .map(str::to_string)
                .or_else(|| serde_yaml::to_string(k).ok().map(|s| s.trim().to_string()))
                .ok_or_else(|| ConfigError::Yaml("non-string override key".to_string()))?;
            if v.is_null() {
                out.insert(key, None);
            } else {
                out.insert(key, Some(Self::unmarshal_yaml_value_with_defaults(v.clone(), defaults)?));
            }
        }
        Ok(out)
    }

    /// Unmarshals `map[string]Limits` (non-pointer value map) YAML matching Go `yaml.v3`:
    /// empty/null entry yields zeroed `LimitsConfig`, non-null calls `unmarshal_yaml_value_with_defaults`.
    pub fn unmarshal_limits_value_map_yaml(
        yaml: &str,
        defaults: &Self,
    ) -> Result<BTreeMap<String, Self>, ConfigError> {
        let input: serde_yaml::Value =
            serde_yaml::from_str(yaml).map_err(|e| ConfigError::Yaml(e.to_string()))?;
        if input.is_null() {
            return Ok(BTreeMap::new());
        }
        let map = input.as_mapping().ok_or_else(|| {
            ConfigError::Yaml("expected YAML mapping for limits map".to_string())
        })?;
        let mut out = BTreeMap::new();
        for (k, v) in map {
            let key = k
                .as_str()
                .map(str::to_string)
                .ok_or_else(|| ConfigError::Yaml("non-string map key".to_string()))?;
            if v.is_null() {
                out.insert(key, Self::zeroed());
            } else {
                out.insert(key, Self::unmarshal_yaml_value_with_defaults(v.clone(), defaults)?);
            }
        }
        Ok(out)
    }

    fn unmarshal_yaml_value_with_defaults(
        input: serde_yaml::Value,
        defaults: &Self,
    ) -> Result<Self, ConfigError> {
        use serde_yaml::{Mapping, Value};

        let mut base = crate::secret::with_secrets_revealed(|| serde_yaml::to_value(defaults))
            .map_err(|e| ConfigError::Yaml(e.to_string()))?;
        if let Some(base_map) = base.as_mapping_mut() {
            base_map.insert(
                Value::String("otel_metric_suffixes_enabled".to_string()),
                Value::Null,
            );
            base_map.insert(
                Value::String("name_validation_scheme".to_string()),
                Value::String(String::new()),
            );
        }

        let mut ext_values: BTreeMap<String, serde_json::Value> = defaults.extensions.clone();
        let registered_exts = current_thread_extensions();
        for (name, def_fn) in &registered_exts {
            ext_values.entry(name.clone()).or_insert_with(def_fn);
        }

        let mut cleaned_input = input;
        if let Some(in_map) = cleaned_input.as_mapping_mut() {
            for (name, _) in &registered_exts {
                if let Some(raw_ext) = in_map.remove(Value::String(name.clone())) {
                    let json_ext = serde_json::to_value(&raw_ext)
                        .map_err(|e| ConfigError::Yaml(e.to_string()))?;
                    ext_values.insert(name.clone(), json_ext);
                }
            }
        }

        if let (Some(merged_map), Some(in_map)) =
            (base.as_mapping_mut(), cleaned_input.as_mapping())
        {
            for (k, v) in in_map {
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
                    } else if let Some(ov_map) = v.as_mapping() {
                        let entry = merged_map
                            .entry(k.clone())
                            .or_insert_with(|| Value::Mapping(Mapping::new()));
                        if !entry.is_mapping() {
                            *entry = Value::Mapping(Mapping::new());
                        }
                        if let Some(base_entries) = entry.as_mapping_mut() {
                            for (tk, tv) in ov_map {
                                if tv.is_null() {
                                    if !base_entries.contains_key(tk) {
                                        base_entries
                                            .insert(tk.clone(), Value::Mapping(Mapping::new()));
                                    }
                                } else {
                                    base_entries.insert(tk.clone(), tv.clone());
                                }
                            }
                        }
                    } else {
                        merged_map.insert(k.clone(), v.clone());
                    }
                } else if v.is_null() {
                    if matches!(
                        key_str,
                        "metric_relabel_configs"
                            | "blocked_queries"
                            | "limited_queries"
                            | "blocked_requests"
                            | "cost_attribution_labels_structured"
                            | "drop_labels"
                    ) {
                        merged_map.insert(k.clone(), Value::Sequence(Vec::new()));
                    } else if key_str == "otel_metric_suffixes_enabled" {
                        merged_map.insert(k.clone(), Value::Null);
                    }
                } else if let Some(existing) = merged_map.get_mut(k) {
                    if existing.is_mapping() && v.is_mapping() {
                        merge_yaml_mappings(existing, v.clone());
                    } else {
                        *existing = v.clone();
                    }
                } else {
                    merged_map.insert(k.clone(), v.clone());
                }
            }
        } else if !cleaned_input.is_null() {
            return Err(ConfigError::Yaml(
                "expected YAML mapping for LimitsConfig".to_string(),
            ));
        }

        let mut limits: LimitsConfig =
            serde_yaml::from_value(base).map_err(|e| ConfigError::Yaml(e.to_string()))?;
        limits.extensions = ext_values;
        validate_custom_trackers_yaml(&limits)?;
        limits
            .migrate_cost_attribution_labels_structured()
            .map_err(|e| match e {
                ConfigError::Validation(msg) | ConfigError::Flag(msg) => ConfigError::Yaml(msg),
                other => other,
            })?;
        limits.validate().map_err(|e| match e {
            ConfigError::Validation(msg) | ConfigError::Flag(msg) => ConfigError::Yaml(msg),
            other => other,
        })?;
        limits.canonicalize_queries();
        Ok(limits)
    }

    /// Serializes `LimitsConfig` (including any registered extensions) to a JSON object.
    pub fn to_json_value_with_extensions(&self) -> Result<serde_json::Value, ConfigError> {
        let mut obj = serde_json::Map::new();
        for (name, zero_fn) in current_thread_extension_zeros() {
            let val = self
                .extensions
                .get(&name)
                .cloned()
                .unwrap_or_else(zero_fn);
            obj.insert(name, val);
        }
        let plain = crate::secret::with_secrets_revealed(|| serde_json::to_value(self))
            .map_err(|e| ConfigError::Yaml(e.to_string()))?;
        if let serde_json::Value::Object(m) = plain {
            for (k, v) in m {
                obj.insert(k, v);
            }
        }
        Ok(serde_json::Value::Object(obj))
    }

    /// Serializes `LimitsConfig` (including any registered extensions) to a `serde_yaml::Value`
    /// preserving Go struct field order (extensions first, then standard fields).
    pub fn to_yaml_value_with_extensions(&self) -> Result<serde_yaml::Value, ConfigError> {
        let mut map = serde_yaml::Mapping::new();
        for (name, zero_fn) in current_thread_extension_zeros() {
            let val = self
                .extensions
                .get(&name)
                .cloned()
                .unwrap_or_else(zero_fn);
            let yv = serde_yaml::to_value(&val).map_err(|e| ConfigError::Yaml(e.to_string()))?;
            map.insert(serde_yaml::Value::String(name), yv);
        }
        let plain = crate::secret::with_secrets_revealed(|| serde_yaml::to_value(self))
            .map_err(|e| ConfigError::Yaml(e.to_string()))?;
        if let serde_yaml::Value::Mapping(m) = plain {
            for (k, v) in m {
                map.insert(k, v);
            }
        }
        Ok(serde_yaml::Value::Mapping(map))
    }

    /// Serializes `LimitsConfig` (including any registered extensions) to a YAML string.
    pub fn to_yaml_with_extensions(&self) -> Result<String, ConfigError> {
        let yaml_val = self.to_yaml_value_with_extensions()?;
        serde_yaml::to_string(&yaml_val).map_err(|e| ConfigError::Yaml(e.to_string()))
    }
}

fn merge_yaml_mappings(dst: &mut serde_yaml::Value, src: serde_yaml::Value) {
    if let (Some(dst_map), serde_yaml::Value::Mapping(src_map)) = (dst.as_mapping_mut(), src) {
        for (k, v) in src_map {
            match dst_map.get_mut(&k) {
                Some(existing) if existing.is_mapping() && v.is_mapping() => {
                    merge_yaml_mappings(existing, v);
                }
                _ => {
                    dst_map.insert(k, v);
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Thread-local dynamic Limits extensions registry (Go: MustRegisterExtension)
// ---------------------------------------------------------------------------------------------

pub trait LimitsExtensionDefault:
    Clone + Default + Serialize + serde::de::DeserializeOwned + 'static
{
    fn extension_default() -> Self;
}

struct RegisteredExtension {
    default_value: fn() -> serde_json::Value,
    zero_value: fn() -> serde_json::Value,
}

thread_local! {
    static REGISTERED_EXTENSIONS: RefCell<Vec<(String, RegisteredExtension)>> = const { RefCell::new(Vec::new()) };
}

fn current_thread_extensions() -> Vec<(String, fn() -> serde_json::Value)> {
    REGISTERED_EXTENSIONS.with(|cell| {
        cell.borrow()
            .iter()
            .map(|(n, e)| (n.clone(), e.default_value))
            .collect()
    })
}

fn current_thread_extension_zeros() -> Vec<(String, fn() -> serde_json::Value)> {
    REGISTERED_EXTENSIONS.with(|cell| {
        cell.borrow()
            .iter()
            .map(|(n, e)| (n.clone(), e.zero_value))
            .collect()
    })
}

fn standard_limits_keys() -> BTreeSet<String> {
    let mut keys = BTreeSet::new();
    if let Ok(serde_json::Value::Object(map)) = serde_json::to_value(LimitsConfig::default()) {
        for k in map.keys() {
            keys.insert(k.clone());
        }
    }
    for extra in [
        "metric_relabel_configs",
        "blocked_queries",
        "limited_queries",
        "blocked_requests",
        "cost_attribution_labels_structured",
        "cost_attribution_trackers",
        "additional_cost_attribution_trackers",
    ] {
        keys.insert(extra.to_string());
    }
    keys
}

/// Clears all extensions registered on the current thread (used in test cleanup).
pub fn clear_registered_extensions() {
    REGISTERED_EXTENSIONS.with(|cell| cell.borrow_mut().clear());
}

pub type ExtensionGetter<E> = Box<dyn Fn(Option<&LimitsConfig>) -> E>;

/// Registers an extension field on `LimitsConfig` for the current thread
/// (`pkg/util/validation/limits.go:MustRegisterExtension`).
pub fn must_register_extension<E: LimitsExtensionDefault>(
    name: &str,
) -> ExtensionGetter<E> {
    assert!(!name.is_empty(), "extension name cannot be empty");
    let std_keys = standard_limits_keys();
    assert!(
        !std_keys.contains(name),
        "extension {name} cannot be registered because it's a standard limits field"
    );
    REGISTERED_EXTENSIONS.with(|cell| {
        let mut list = cell.borrow_mut();
        assert!(
            !list.iter().any(|(n, _)| n == name),
            "extension {name} already registered"
        );
        fn make_default<T: LimitsExtensionDefault>() -> serde_json::Value {
            serde_json::to_value(T::extension_default()).unwrap_or(serde_json::Value::Null)
        }
        fn make_zero<T: LimitsExtensionDefault>() -> serde_json::Value {
            serde_json::to_value(T::default()).unwrap_or(serde_json::Value::Null)
        }
        list.push((
            name.to_string(),
            RegisteredExtension {
                default_value: make_default::<E>,
                zero_value: make_zero::<E>,
            },
        ));
    });
    let key = name.to_string();
    Box::new(move |l: Option<&LimitsConfig>| match l {
        None => E::extension_default(),
        Some(cfg) => match cfg.extensions.get(&key) {
            None => E::default(),
            Some(v) => serde_json::from_value::<E>(v.clone()).unwrap_or_default(),
        },
    })
}

// ---------------------------------------------------------------------------------------------
// Per-tenant aggregation helpers (pkg/util/validation/limits.go:2050-2150)
// ---------------------------------------------------------------------------------------------

pub fn all_true_booleans_per_tenant<S: AsRef<str>>(
    tenant_ids: &[S],
    f: impl Fn(&str) -> bool,
) -> bool {
    tenant_ids.iter().all(|t| f(t.as_ref()))
}

pub fn smallest_positive_int_per_tenant<S: AsRef<str>>(
    tenant_ids: &[S],
    f: impl Fn(&str) -> i64,
) -> i64 {
    let mut result: Option<i64> = None;
    for t in tenant_ids {
        let v = f(t.as_ref());
        if result.is_none_or(|r| v < r) {
            result = Some(v);
        }
    }
    result.unwrap_or(0)
}

pub fn smallest_positive_non_zero_int_per_tenant<S: AsRef<str>>(
    tenant_ids: &[S],
    f: impl Fn(&str) -> i64,
) -> i64 {
    let mut result: Option<i64> = None;
    for t in tenant_ids {
        let v = f(t.as_ref());
        if v > 0 && result.is_none_or(|r| v < r) {
            result = Some(v);
        }
    }
    result.unwrap_or(0)
}

pub fn smallest_positive_non_zero_duration_per_tenant<S: AsRef<str>>(
    tenant_ids: &[S],
    f: impl Fn(&str) -> ModelDuration,
) -> ModelDuration {
    let mut result: Option<i64> = None;
    for t in tenant_ids {
        let v = f(t.as_ref()).0;
        if v > 0 && result.is_none_or(|r| v < r) {
            result = Some(v);
        }
    }
    ModelDuration(result.unwrap_or(0))
}

pub fn largest_positive_non_zero_duration_per_tenant<S: AsRef<str>>(
    tenant_ids: &[S],
    f: impl Fn(&str) -> ModelDuration,
) -> ModelDuration {
    let mut result: i64 = 0;
    for t in tenant_ids {
        let v = f(t.as_ref()).0;
        if v > result {
            result = v;
        }
    }
    ModelDuration(result)
}

pub fn min_duration_per_tenant<S: AsRef<str>>(
    tenant_ids: &[S],
    f: impl Fn(&str) -> ModelDuration,
) -> ModelDuration {
    let mut result: i64 = 0;
    for (idx, t) in tenant_ids.iter().enumerate() {
        let v = f(t.as_ref()).0;
        if idx == 0 || v < result {
            result = v;
        }
    }
    ModelDuration(result)
}

pub fn max_duration_per_tenant<S: AsRef<str>>(
    tenant_ids: &[S],
    f: impl Fn(&str) -> ModelDuration,
) -> ModelDuration {
    let mut result: i64 = 0;
    for t in tenant_ids {
        let v = f(t.as_ref()).0;
        if v > result {
            result = v;
        }
    }
    ModelDuration(result)
}

// ---------------------------------------------------------------------------------------------
// PromQL vector-selector canonicalizer for BlockedQuery (Go: promqlext + VectorSelector.String)
// ---------------------------------------------------------------------------------------------

fn canonicalize_promql_expr(input: &str) -> Option<String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return None;
    }
    let bytes = trimmed.as_bytes();
    let mut i = 0usize;
    let mut paren_depth = 0i32;
    let mut brace_depth = 0i32;
    let mut bracket_depth = 0i32;
    while i < bytes.len() {
        match bytes[i] {
            b'"' | b'\'' | b'`' => {
                let q = bytes[i];
                i += 1;
                let mut closed = false;
                while i < bytes.len() {
                    if q != b'`' && bytes[i] == b'\\' {
                        i += 2;
                        continue;
                    }
                    if bytes[i] == q {
                        i += 1;
                        closed = true;
                        break;
                    }
                    i += 1;
                }
                if !closed {
                    return None;
                }
                continue;
            }
            b'(' => paren_depth += 1,
            b')' => {
                paren_depth -= 1;
                if paren_depth < 0 {
                    return None;
                }
            }
            b'{' => brace_depth += 1,
            b'}' => {
                brace_depth -= 1;
                if brace_depth < 0 {
                    return None;
                }
            }
            b'[' => bracket_depth += 1,
            b']' => {
                bracket_depth -= 1;
                if bracket_depth < 0 {
                    return None;
                }
            }
            _ => {}
        }
        i += 1;
    }
    if paren_depth != 0 || brace_depth != 0 || bracket_depth != 0 {
        return None;
    }

    let mut out = String::with_capacity(trimmed.len());
    let mut pos = 0usize;
    while pos < trimmed.len() {
        let rest = &trimmed[pos..];
        let ch = rest.chars().next()?;
        if matches!(ch, '"' | '\'' | '`') {
            let end = scan_quoted_literal(rest)?;
            out.push_str(&rest[..end]);
            pos += end;
            continue;
        }
        if ch == '{' {
            let close_rel = find_matching_brace(rest)?;
            let inner = &rest[1..close_rel];
            let canon_inner = canonicalize_label_matchers(inner)?;
            out.push('{');
            out.push_str(&canon_inner);
            out.push('}');
            pos += close_rel + 1;
            continue;
        }
        out.push(ch);
        pos += ch.len_utf8();
    }
    Some(out)
}

fn scan_quoted_literal(s: &str) -> Option<usize> {
    let bytes = s.as_bytes();
    let q = *bytes.first()?;
    let mut i = 1usize;
    while i < bytes.len() {
        if q != b'`' && bytes[i] == b'\\' {
            i += 2;
            continue;
        }
        if bytes[i] == q {
            return Some(i + 1);
        }
        i += 1;
    }
    None
}

fn find_matching_brace(s: &str) -> Option<usize> {
    let mut i = 1usize;
    let bytes = s.as_bytes();
    while i < bytes.len() {
        if matches!(bytes[i], b'"' | b'\'' | b'`') {
            let len = scan_quoted_literal(&s[i..])?;
            i += len;
            continue;
        }
        if bytes[i] == b'}' {
            return Some(i);
        }
        if bytes[i] == b'{' {
            return None;
        }
        i += 1;
    }
    None
}

fn canonicalize_label_matchers(inner: &str) -> Option<String> {
    let mut matchers = Vec::new();
    let mut i = 0usize;
    let bytes = inner.as_bytes();
    loop {
        while i < bytes.len() && (bytes[i].is_ascii_whitespace() || bytes[i] == b',') {
            i += 1;
        }
        if i >= bytes.len() {
            break;
        }
        let name_start = i;
        if matches!(bytes[i], b'"' | b'\'' | b'`') {
            let len = scan_quoted_literal(&inner[i..])?;
            i += len;
        } else {
            while i < bytes.len()
                && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_' || bytes[i] == b':')
            {
                i += 1;
            }
        }
        let name = inner[name_start..i].trim();
        if name.is_empty() {
            return None;
        }
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        let op = if inner[i..].starts_with("=~") {
            i += 2;
            "=~"
        } else if inner[i..].starts_with("!~") {
            i += 2;
            "!~"
        } else if inner[i..].starts_with("!=") {
            i += 2;
            "!="
        } else if inner[i..].starts_with('=') {
            i += 1;
            "="
        } else {
            return None;
        };
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= bytes.len() || !matches!(bytes[i], b'"' | b'\'' | b'`') {
            return None;
        }
        let val_len = scan_quoted_literal(&inner[i..])?;
        let val = &inner[i..i + val_len];
        i += val_len;
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i < bytes.len() && bytes[i] != b',' {
            return None;
        }
        matchers.push(format!("{name}{op}{val}"));
    }
    matchers.sort();
    Some(matchers.join(","))
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

    #[test]
    fn w26_s2_07_and_s4_10_custom_trackers_error_wording_and_builtin_validator() {
        let mut limits = LimitsConfig {
            max_active_series_additional_custom_trackers: 1,
            ..Default::default()
        };
        limits
            .active_series_additional_custom_trackers
            .insert("t1".to_string(), r#"{job="a"}"#.to_string());
        limits
            .active_series_additional_custom_trackers
            .insert("t2".to_string(), r#"{job="b"}"#.to_string());
        assert_eq!(
            limits.validate().unwrap_err().to_string(),
            "error validating config: active_series_additional_custom_trackers validation failed: the number of custom trackers [2] exceeds the configured limit [1]"
        );

        let mut bad_matcher = LimitsConfig::default();
        bad_matcher
            .active_series_custom_trackers
            .insert("bad".to_string(), "not a valid matcher".to_string());
        let err = bad_matcher.validate().unwrap_err().to_string();
        assert!(
            err.contains("can't build active series matcher bad"),
            "unexpected err: {err}"
        );

        let mut bad_regex = LimitsConfig::default();
        bad_regex
            .active_series_additional_custom_trackers
            .insert("bad_re".to_string(), r#"{job=~"[unclosed"}"#.to_string());
        let err_re = bad_regex.validate().unwrap_err().to_string();
        assert!(
            err_re.contains("can't build active series matcher bad_re"),
            "unexpected err: {err_re}"
        );
    }
}
