//! Static schema definitions for non-ingester Mimir YAML blocks and CLI flags.
//!
//! Generated from github.com/grafana/mimir/cmd/mimir/config-descriptor.json
//! and github.com/grafana/mimir/pkg/mimir/mimir.go.

use std::collections::BTreeMap;

use serde_yaml::Value;

use crate::duration::{format_go_duration, GoDuration};
use crate::error::ConfigError;
use crate::flags::FlagSet;
use crate::types::parse_go_bool;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum FieldKind {
    Bool,
    Int,
    Float,
    Duration,
    String,
    StringList,
    Other,
    /// `flagext.Secret`: rendered as `********` (or `""` when empty) in YAML output.
    Secret,
    /// `flagext.Bytes`: `64KiB`-style values (`units.ParseBase2Bytes`), rendered with
    /// `units.Base2Bytes.String()` (e.g. `63KiB1023B`).
    Bytes,
    /// `flagext.Secret` with `yaml:",omitempty"`: omitted from YAML output when empty.
    SecretOmitEmpty,
}

#[derive(Copy, Clone, Debug)]
pub struct SchemaField {
    pub name: &'static str,
    pub flag: &'static str,
    pub kind: FieldKind,
    pub default_str: &'static str,
    pub usage: &'static str,
}

#[derive(Copy, Clone, Debug)]
pub struct SchemaBlock {
    pub name: &'static str,
    pub fields: &'static [SchemaField],
    pub blocks: &'static [SchemaBlock],
}

static BLOCK_0_FIELDS: &[SchemaField] = &[
    SchemaField { name: "skip_label_name_validation_header_enabled", flag: "api.skip-label-name-validation-header-enabled", kind: FieldKind::Bool, default_str: "false", usage: "Allows to skip label name validation via X-Mimir-SkipLabelNameValidation header on the http write path. Use with caution as it breaks PromQL. Allowing this for external clients allows any client to send invalid label names. After enabling it, requests with a specific HTTP header set to true will not have label names validated." },
    SchemaField { name: "skip_label_count_validation_header_enabled", flag: "api.skip-label-count-validation-header-enabled", kind: FieldKind::Bool, default_str: "false", usage: "Allows to disable enforcement of the label count limit \"max_label_names_per_series\" via X-Mimir-SkipLabelCountValidation header on the http write path. Allowing this for external clients allows any client to send invalid label counts. After enabling it, requests with a specific HTTP header set to true will not have label counts validated." },
    SchemaField { name: "otlp_translation_headers_enabled", flag: "api.otlp-translation-headers-enabled", kind: FieldKind::Bool, default_str: "false", usage: "Allows controlling OTLP metric name suffix addition and translation strategy via X-Mimir-OTLP-AddSuffixes and X-Mimir-OTLP-TranslationStrategy headers on the OTLP push path. Not recommended for general use." },
    SchemaField { name: "alertmanager_http_prefix", flag: "http.alertmanager-http-prefix", kind: FieldKind::String, default_str: "/alertmanager", usage: "HTTP URL path under which the Alertmanager ui and api will be served." },
    SchemaField { name: "prometheus_http_prefix", flag: "http.prometheus-http-prefix", kind: FieldKind::String, default_str: "/prometheus", usage: "HTTP URL path under which the Prometheus api will be served." },
    SchemaField { name: "response_compression_level", flag: "http.response-compression-level", kind: FieldKind::Int, default_str: "-1", usage: "Compression level for HTTP responses when gzip compression is requested by the client. Valid values are 1 (fastest) to 9 (best compression), or -1 for the default compression level." },
];

static BLOCK_0_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_1_FIELDS: &[SchemaField] = &[
    SchemaField { name: "client_cleanup_period", flag: "distributor.client-cleanup-period", kind: FieldKind::Duration, default_str: "15000000000", usage: "How frequently to clean up clients for ingesters that have gone away." },
    SchemaField { name: "health_check_ingesters", flag: "distributor.health-check-ingesters", kind: FieldKind::Bool, default_str: "true", usage: "Run a health check on each ingester client during periodic cleanup." },
    SchemaField { name: "ingester_health_check_grace_period", flag: "distributor.ingester-health-check-grace-period", kind: FieldKind::Duration, default_str: "0", usage: "The grace period for ingester health checks. If an ingester connection consistently fails health checks for this period, any open connections are closed. The distributor or querier will attempt to reconnect to the ingester if a subsequent request is made to the ingester. Set to 0 to immediately remove ingester connections on the first health check failure." },
];

static BLOCK_1_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_2_FIELDS: &[SchemaField] = &[
    SchemaField { name: "enabled", flag: "distributor.retry-after-header.enabled", kind: FieldKind::Bool, default_str: "true", usage: "Enables inclusion of the Retry-After header in the response: true includes it for client retry guidance, false omits it." },
    SchemaField { name: "min_backoff", flag: "distributor.retry-after-header.min-backoff", kind: FieldKind::Duration, default_str: "6000000000", usage: "Minimum duration of the Retry-After HTTP header in responses to 429/5xx errors. Must be greater than or equal to 1s. Backoff is calculated as MinBackoff*2^(RetryAttempt-1) seconds with random jitter of 50% in either direction. RetryAttempt is the value of the Retry-Attempt HTTP header." },
    SchemaField { name: "max_backoff", flag: "distributor.retry-after-header.max-backoff", kind: FieldKind::Duration, default_str: "96000000000", usage: "Maximum duration of the Retry-After HTTP header in responses to 429/5xx errors. Must be greater than or equal to 1s. Backoff is calculated as MinBackoff*2^(RetryAttempt-1) seconds with random jitter of 50% in either direction. RetryAttempt is the value of the Retry-Attempt HTTP header." },
];

static BLOCK_2_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_3_FIELDS: &[SchemaField] = &[
    SchemaField { name: "host", flag: "distributor.ha-tracker.consul.hostname", kind: FieldKind::String, default_str: "localhost:8500", usage: "Hostname and port of Consul." },
    SchemaField { name: "acl_token", flag: "distributor.ha-tracker.consul.acl-token", kind: FieldKind::Secret, default_str: "", usage: "ACL Token used to interact with Consul." },
    SchemaField { name: "http_client_timeout", flag: "distributor.ha-tracker.consul.client-timeout", kind: FieldKind::Duration, default_str: "20000000000", usage: "HTTP timeout when talking to Consul" },
    SchemaField { name: "consistent_reads", flag: "distributor.ha-tracker.consul.consistent-reads", kind: FieldKind::Bool, default_str: "false", usage: "Enable consistent reads to Consul." },
    SchemaField { name: "watch_rate_limit", flag: "distributor.ha-tracker.consul.watch-rate-limit", kind: FieldKind::Float, default_str: "1", usage: "Rate limit when watching key or prefix in Consul, in requests per second. 0 disables the rate limit." },
    SchemaField { name: "watch_burst_size", flag: "distributor.ha-tracker.consul.watch-burst-size", kind: FieldKind::Int, default_str: "1", usage: "Burst size used in rate limit. Values less than 1 are treated as 1." },
    SchemaField { name: "cas_retry_delay", flag: "distributor.ha-tracker.consul.cas-retry-delay", kind: FieldKind::Duration, default_str: "1000000000", usage: "Maximum duration to wait before retrying a Compare And Swap (CAS) operation." },
];

static BLOCK_3_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_4_FIELDS: &[SchemaField] = &[
    SchemaField { name: "endpoints", flag: "distributor.ha-tracker.etcd.endpoints", kind: FieldKind::StringList, default_str: "", usage: "The etcd endpoints to connect to." },
    SchemaField { name: "dial_timeout", flag: "distributor.ha-tracker.etcd.dial-timeout", kind: FieldKind::Duration, default_str: "10000000000", usage: "The dial timeout for the etcd connection." },
    SchemaField { name: "max_retries", flag: "distributor.ha-tracker.etcd.max-retries", kind: FieldKind::Int, default_str: "10", usage: "The maximum number of retries to do for failed ops." },
    SchemaField { name: "tls_enabled", flag: "distributor.ha-tracker.etcd.tls-enabled", kind: FieldKind::Bool, default_str: "false", usage: "Enable TLS." },
    SchemaField { name: "tls_cert_path", flag: "distributor.ha-tracker.etcd.tls-cert-path", kind: FieldKind::String, default_str: "", usage: "Path to the client certificate, which will be used for authenticating with the server. Also requires the key path to be configured." },
    SchemaField { name: "tls_key_path", flag: "distributor.ha-tracker.etcd.tls-key-path", kind: FieldKind::String, default_str: "", usage: "Path to the key for the client certificate. Also requires the client certificate to be configured." },
    SchemaField { name: "tls_ca_path", flag: "distributor.ha-tracker.etcd.tls-ca-path", kind: FieldKind::String, default_str: "", usage: "Path to the CA certificates to validate server certificate against. If not set, the host's root CA certificates are used." },
    SchemaField { name: "tls_server_name", flag: "distributor.ha-tracker.etcd.tls-server-name", kind: FieldKind::String, default_str: "", usage: "Override the expected name on the server certificate." },
    SchemaField { name: "tls_insecure_skip_verify", flag: "distributor.ha-tracker.etcd.tls-insecure-skip-verify", kind: FieldKind::Bool, default_str: "false", usage: "Skip validating server certificate." },
    SchemaField { name: "tls_cipher_suites", flag: "distributor.ha-tracker.etcd.tls-cipher-suites", kind: FieldKind::String, default_str: "", usage: "Override the default cipher suite list (separated by commas). Allowed values:\n\nSecure Ciphers:\n- TLS_AES_128_GCM_SHA256\n- TLS_AES_256_GCM_SHA384\n- TLS_CHACHA20_POLY1305_SHA256\n- TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA\n- TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA\n- TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256\n- TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256\n- TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256\n- TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256\n\nInsecure Ciphers:\n- TLS_RSA_WITH_RC4_128_SHA\n- TLS_RSA_WITH_3DES_EDE_CBC_SHA\n- TLS_RSA_WITH_AES_128_CBC_SHA\n- TLS_RSA_WITH_AES_256_CBC_SHA\n- TLS_RSA_WITH_AES_128_CBC_SHA256\n- TLS_RSA_WITH_AES_128_GCM_SHA256\n- TLS_RSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_ECDSA_WITH_RC4_128_SHA\n- TLS_ECDHE_RSA_WITH_RC4_128_SHA\n- TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256\n- TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256\n" },
    SchemaField { name: "tls_min_version", flag: "distributor.ha-tracker.etcd.tls-min-version", kind: FieldKind::String, default_str: "", usage: "Override the default minimum TLS version. Allowed values: VersionTLS10, VersionTLS11, VersionTLS12, VersionTLS13" },
    SchemaField { name: "username", flag: "distributor.ha-tracker.etcd.username", kind: FieldKind::String, default_str: "", usage: "Etcd username." },
    SchemaField { name: "password", flag: "distributor.ha-tracker.etcd.password", kind: FieldKind::Secret, default_str: "", usage: "Etcd password." },
];

static BLOCK_4_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_5_FIELDS: &[SchemaField] = &[
    SchemaField { name: "primary", flag: "distributor.ha-tracker.multi.primary", kind: FieldKind::String, default_str: "", usage: "Primary backend storage used by multi-client." },
    SchemaField { name: "secondary", flag: "distributor.ha-tracker.multi.secondary", kind: FieldKind::String, default_str: "", usage: "Secondary backend storage used by multi-client." },
    SchemaField { name: "mirror_enabled", flag: "distributor.ha-tracker.multi.mirror-enabled", kind: FieldKind::Bool, default_str: "false", usage: "Mirror writes to the secondary store." },
    SchemaField { name: "mirror_timeout", flag: "distributor.ha-tracker.multi.mirror-timeout", kind: FieldKind::Duration, default_str: "2000000000", usage: "Timeout for storing a value to the secondary store." },
];

static BLOCK_5_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_6_FIELDS: &[SchemaField] = &[
    SchemaField { name: "store", flag: "distributor.ha-tracker.store", kind: FieldKind::String, default_str: "memberlist", usage: "Backend storage to use for the ring. Supported values are: consul, etcd, inmemory, memberlist, multi." },
    SchemaField { name: "prefix", flag: "distributor.ha-tracker.prefix", kind: FieldKind::String, default_str: "ha-tracker/", usage: "The prefix for the keys in the store. Should end with a /." },
];

static BLOCK_6_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "consul", fields: BLOCK_3_FIELDS, blocks: BLOCK_3_BLOCKS },
    SchemaBlock { name: "etcd", fields: BLOCK_4_FIELDS, blocks: BLOCK_4_BLOCKS },
    SchemaBlock { name: "multi", fields: BLOCK_5_FIELDS, blocks: BLOCK_5_BLOCKS },
];

static BLOCK_7_FIELDS: &[SchemaField] = &[
    SchemaField { name: "enable_ha_tracker", flag: "distributor.ha-tracker.enable", kind: FieldKind::Bool, default_str: "false", usage: "Enable the distributors HA tracker so that it can accept samples from Prometheus HA replicas gracefully (requires labels)." },
    SchemaField { name: "enable_elected_replica_metric", flag: "distributor.ha-tracker.enable-elected-replica-metric", kind: FieldKind::Bool, default_str: "false", usage: "Enable the elected_replica_status metric, which shows the current elected replica. It is disabled by default due to the possible high cardinality of the metric." },
];

static BLOCK_7_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "kvstore", fields: BLOCK_6_FIELDS, blocks: BLOCK_6_BLOCKS },
];

static BLOCK_8_FIELDS: &[SchemaField] = &[
    SchemaField { name: "host", flag: "distributor.ring.consul.hostname", kind: FieldKind::String, default_str: "localhost:8500", usage: "Hostname and port of Consul." },
    SchemaField { name: "acl_token", flag: "distributor.ring.consul.acl-token", kind: FieldKind::Secret, default_str: "", usage: "ACL Token used to interact with Consul." },
    SchemaField { name: "http_client_timeout", flag: "distributor.ring.consul.client-timeout", kind: FieldKind::Duration, default_str: "20000000000", usage: "HTTP timeout when talking to Consul" },
    SchemaField { name: "consistent_reads", flag: "distributor.ring.consul.consistent-reads", kind: FieldKind::Bool, default_str: "false", usage: "Enable consistent reads to Consul." },
    SchemaField { name: "watch_rate_limit", flag: "distributor.ring.consul.watch-rate-limit", kind: FieldKind::Float, default_str: "1", usage: "Rate limit when watching key or prefix in Consul, in requests per second. 0 disables the rate limit." },
    SchemaField { name: "watch_burst_size", flag: "distributor.ring.consul.watch-burst-size", kind: FieldKind::Int, default_str: "1", usage: "Burst size used in rate limit. Values less than 1 are treated as 1." },
    SchemaField { name: "cas_retry_delay", flag: "distributor.ring.consul.cas-retry-delay", kind: FieldKind::Duration, default_str: "1000000000", usage: "Maximum duration to wait before retrying a Compare And Swap (CAS) operation." },
];

static BLOCK_8_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_9_FIELDS: &[SchemaField] = &[
    SchemaField { name: "endpoints", flag: "distributor.ring.etcd.endpoints", kind: FieldKind::StringList, default_str: "", usage: "The etcd endpoints to connect to." },
    SchemaField { name: "dial_timeout", flag: "distributor.ring.etcd.dial-timeout", kind: FieldKind::Duration, default_str: "10000000000", usage: "The dial timeout for the etcd connection." },
    SchemaField { name: "max_retries", flag: "distributor.ring.etcd.max-retries", kind: FieldKind::Int, default_str: "10", usage: "The maximum number of retries to do for failed ops." },
    SchemaField { name: "tls_enabled", flag: "distributor.ring.etcd.tls-enabled", kind: FieldKind::Bool, default_str: "false", usage: "Enable TLS." },
    SchemaField { name: "tls_cert_path", flag: "distributor.ring.etcd.tls-cert-path", kind: FieldKind::String, default_str: "", usage: "Path to the client certificate, which will be used for authenticating with the server. Also requires the key path to be configured." },
    SchemaField { name: "tls_key_path", flag: "distributor.ring.etcd.tls-key-path", kind: FieldKind::String, default_str: "", usage: "Path to the key for the client certificate. Also requires the client certificate to be configured." },
    SchemaField { name: "tls_ca_path", flag: "distributor.ring.etcd.tls-ca-path", kind: FieldKind::String, default_str: "", usage: "Path to the CA certificates to validate server certificate against. If not set, the host's root CA certificates are used." },
    SchemaField { name: "tls_server_name", flag: "distributor.ring.etcd.tls-server-name", kind: FieldKind::String, default_str: "", usage: "Override the expected name on the server certificate." },
    SchemaField { name: "tls_insecure_skip_verify", flag: "distributor.ring.etcd.tls-insecure-skip-verify", kind: FieldKind::Bool, default_str: "false", usage: "Skip validating server certificate." },
    SchemaField { name: "tls_cipher_suites", flag: "distributor.ring.etcd.tls-cipher-suites", kind: FieldKind::String, default_str: "", usage: "Override the default cipher suite list (separated by commas). Allowed values:\n\nSecure Ciphers:\n- TLS_AES_128_GCM_SHA256\n- TLS_AES_256_GCM_SHA384\n- TLS_CHACHA20_POLY1305_SHA256\n- TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA\n- TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA\n- TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256\n- TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256\n- TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256\n- TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256\n\nInsecure Ciphers:\n- TLS_RSA_WITH_RC4_128_SHA\n- TLS_RSA_WITH_3DES_EDE_CBC_SHA\n- TLS_RSA_WITH_AES_128_CBC_SHA\n- TLS_RSA_WITH_AES_256_CBC_SHA\n- TLS_RSA_WITH_AES_128_CBC_SHA256\n- TLS_RSA_WITH_AES_128_GCM_SHA256\n- TLS_RSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_ECDSA_WITH_RC4_128_SHA\n- TLS_ECDHE_RSA_WITH_RC4_128_SHA\n- TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256\n- TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256\n" },
    SchemaField { name: "tls_min_version", flag: "distributor.ring.etcd.tls-min-version", kind: FieldKind::String, default_str: "", usage: "Override the default minimum TLS version. Allowed values: VersionTLS10, VersionTLS11, VersionTLS12, VersionTLS13" },
    SchemaField { name: "username", flag: "distributor.ring.etcd.username", kind: FieldKind::String, default_str: "", usage: "Etcd username." },
    SchemaField { name: "password", flag: "distributor.ring.etcd.password", kind: FieldKind::Secret, default_str: "", usage: "Etcd password." },
];

static BLOCK_9_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_10_FIELDS: &[SchemaField] = &[
    SchemaField { name: "primary", flag: "distributor.ring.multi.primary", kind: FieldKind::String, default_str: "", usage: "Primary backend storage used by multi-client." },
    SchemaField { name: "secondary", flag: "distributor.ring.multi.secondary", kind: FieldKind::String, default_str: "", usage: "Secondary backend storage used by multi-client." },
    SchemaField { name: "mirror_enabled", flag: "distributor.ring.multi.mirror-enabled", kind: FieldKind::Bool, default_str: "false", usage: "Mirror writes to the secondary store." },
    SchemaField { name: "mirror_timeout", flag: "distributor.ring.multi.mirror-timeout", kind: FieldKind::Duration, default_str: "2000000000", usage: "Timeout for storing a value to the secondary store." },
];

static BLOCK_10_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_11_FIELDS: &[SchemaField] = &[
    SchemaField { name: "store", flag: "distributor.ring.store", kind: FieldKind::String, default_str: "memberlist", usage: "Backend storage to use for the ring. Supported values are: consul, etcd, inmemory, memberlist, multi." },
    SchemaField { name: "prefix", flag: "distributor.ring.prefix", kind: FieldKind::String, default_str: "collectors/", usage: "The prefix for the keys in the store. Should end with a /." },
];

static BLOCK_11_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "consul", fields: BLOCK_8_FIELDS, blocks: BLOCK_8_BLOCKS },
    SchemaBlock { name: "etcd", fields: BLOCK_9_FIELDS, blocks: BLOCK_9_BLOCKS },
    SchemaBlock { name: "multi", fields: BLOCK_10_FIELDS, blocks: BLOCK_10_BLOCKS },
];

static BLOCK_12_FIELDS: &[SchemaField] = &[
    SchemaField { name: "heartbeat_period", flag: "distributor.ring.heartbeat-period", kind: FieldKind::Duration, default_str: "15000000000", usage: "Period at which to heartbeat to the ring." },
    SchemaField { name: "heartbeat_timeout", flag: "distributor.ring.heartbeat-timeout", kind: FieldKind::Duration, default_str: "60000000000", usage: "Heartbeat timeout after which Mimir marks distributors as unhealthy in the ring." },
    SchemaField { name: "instance_id", flag: "distributor.ring.instance-id", kind: FieldKind::String, default_str: "<hostname>", usage: "Instance ID to register in the ring." },
    SchemaField { name: "instance_interface_names", flag: "distributor.ring.instance-interface-names", kind: FieldKind::StringList, default_str: "", usage: "List of network interface names to look up when finding the instance IP address." },
    SchemaField { name: "instance_port", flag: "distributor.ring.instance-port", kind: FieldKind::Int, default_str: "0", usage: "Port to advertise in the ring (defaults to -server.grpc-listen-port)." },
    SchemaField { name: "instance_addr", flag: "distributor.ring.instance-addr", kind: FieldKind::String, default_str: "", usage: "IP address to advertise in the ring. Default is auto-detected." },
    SchemaField { name: "instance_enable_ipv6", flag: "distributor.ring.instance-enable-ipv6", kind: FieldKind::Bool, default_str: "false", usage: "Enable using an IPv6 instance address." },
    SchemaField { name: "instance_availability_zone", flag: "distributor.ring.instance-availability-zone", kind: FieldKind::String, default_str: "", usage: "The availability zone where this instance is running. Used for zone-aware rate limiting." },
    SchemaField { name: "auto_forget_unhealthy_periods", flag: "distributor.ring.auto-forget-unhealthy-periods", kind: FieldKind::Int, default_str: "10", usage: "Number of consecutive timeout periods after which Mimir automatically removes an unhealthy instance in the ring. Set to 0 to disable auto-forget." },
];

static BLOCK_12_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "kvstore", fields: BLOCK_11_FIELDS, blocks: BLOCK_11_BLOCKS },
];

static BLOCK_13_FIELDS: &[SchemaField] = &[
    SchemaField { name: "max_ingestion_rate", flag: "distributor.instance-limits.max-ingestion-rate", kind: FieldKind::Float, default_str: "0", usage: "Max ingestion rate (samples/sec) that this distributor will accept. This limit is per-distributor, not per-tenant. Additional push requests will be rejected. Current ingestion rate is computed as exponentially weighted moving average, updated every second. 0 = unlimited." },
    SchemaField { name: "max_inflight_push_requests", flag: "distributor.instance-limits.max-inflight-push-requests", kind: FieldKind::Int, default_str: "2000", usage: "Max inflight push requests that this distributor can handle. This limit is per-distributor, not per-tenant. Additional requests will be rejected. 0 = unlimited." },
    SchemaField { name: "max_inflight_push_requests_bytes", flag: "distributor.instance-limits.max-inflight-push-requests-bytes", kind: FieldKind::Int, default_str: "0", usage: "The sum of the request sizes in bytes of inflight push requests that this distributor can handle. This limit is per-distributor, not per-tenant. Additional requests will be rejected. 0 = unlimited." },
];

static BLOCK_13_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_14_FIELDS: &[SchemaField] = &[
    SchemaField { name: "enabled", flag: "distributor.reactive-limiter.enabled", kind: FieldKind::Bool, default_str: "false", usage: "Enable reactive limiting when making requests to a service" },
    SchemaField { name: "min_limit", flag: "distributor.reactive-limiter.min-limit", kind: FieldKind::Int, default_str: "2", usage: "Minimum inflight requests limit" },
    SchemaField { name: "max_limit", flag: "distributor.reactive-limiter.max-limit", kind: FieldKind::Int, default_str: "200", usage: "Maximum inflight requests limit" },
    SchemaField { name: "initial_limit", flag: "distributor.reactive-limiter.initial-limit", kind: FieldKind::Int, default_str: "20", usage: "Initial inflight requests limit" },
    SchemaField { name: "max_limit_factor", flag: "distributor.reactive-limiter.max-limit-factor", kind: FieldKind::Float, default_str: "5", usage: "The maximum inflight limit as a multiple of current inflight requests" },
    SchemaField { name: "max_limit_factor_decay", flag: "distributor.reactive-limiter.max-limit-factor-decay", kind: FieldKind::Float, default_str: "1", usage: "Logarithmic decay applied to the maxLimitFactor based on current inflight requests" },
    SchemaField { name: "min_limit_factor", flag: "distributor.reactive-limiter.min-limit-factor", kind: FieldKind::Float, default_str: "1.2", usage: "Minimum limit factor when max-limit-factor-decay is applied" },
    SchemaField { name: "recent_window_min_duration", flag: "distributor.reactive-limiter.recent-window-min-duration", kind: FieldKind::Duration, default_str: "1000000000", usage: "Minimum duration of the window that is used to collect recent response time samples" },
    SchemaField { name: "recent_window_max_duration", flag: "distributor.reactive-limiter.recent-window-max-duration", kind: FieldKind::Duration, default_str: "30000000000", usage: "Maximum duration of the window that is used to collect recent response time samples" },
    SchemaField { name: "recent_window_min_samples", flag: "distributor.reactive-limiter.recent-window-min-samples", kind: FieldKind::Int, default_str: "50", usage: "Minimum number of samples that must be recorded in the recent window before updating the limit" },
    SchemaField { name: "recent_quantile", flag: "distributor.reactive-limiter.recent-quantile", kind: FieldKind::Float, default_str: "0.9", usage: "The quantile of recent recorded response times to consider when adjusting the concurrency limit" },
    SchemaField { name: "baseline_window_age", flag: "distributor.reactive-limiter.baseline-window-age", kind: FieldKind::Int, default_str: "10", usage: "The average age of baseline samples aggregated recent samples are added to" },
    SchemaField { name: "correlation_window", flag: "distributor.reactive-limiter.correlation-window", kind: FieldKind::Int, default_str: "50", usage: "How many recent limit and inflight time measurements are stored to detect whether increases in limits correlate with increases in inflight times" },
    SchemaField { name: "initial_rejection_factor", flag: "distributor.reactive-limiter.initial-rejection-factor", kind: FieldKind::Float, default_str: "1", usage: "The number of allowed queued requests, as a multiple of current inflight requests, after which rejections start" },
    SchemaField { name: "max_rejection_factor", flag: "distributor.reactive-limiter.max-rejection-factor", kind: FieldKind::Float, default_str: "2", usage: "The number of allowed queued requests, as a multiple of current inflight requests, after which all requests are rejected" },
];

static BLOCK_14_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_15_FIELDS: &[SchemaField] = &[
    SchemaField { name: "max_recv_msg_size", flag: "distributor.max-recv-msg-size", kind: FieldKind::Int, default_str: "104857600", usage: "Max message size in bytes that the distributors will accept for incoming push requests to the remote write API. If exceeded, the request will be rejected." },
    SchemaField { name: "max_otlp_request_size", flag: "distributor.max-otlp-request-size", kind: FieldKind::Int, default_str: "104857600", usage: "Maximum OTLP request size in bytes that the distributors accept. Requests exceeding this limit are rejected." },
    SchemaField { name: "max_request_pool_buffer_size", flag: "distributor.max-request-pool-buffer-size", kind: FieldKind::Int, default_str: "0", usage: "Max size of the pooled buffers used for marshaling write requests. If 0, no max size is enforced." },
    SchemaField { name: "remote_timeout", flag: "distributor.remote-timeout", kind: FieldKind::Duration, default_str: "2000000000", usage: "Timeout for downstream ingesters." },
    SchemaField { name: "write_requests_buffer_pooling_enabled", flag: "distributor.write-requests-buffer-pooling-enabled", kind: FieldKind::Bool, default_str: "true", usage: "Enable pooling of buffers used for marshaling write requests." },
    SchemaField { name: "reusable_ingester_push_workers", flag: "distributor.reusable-ingester-push-workers", kind: FieldKind::Int, default_str: "2000", usage: "Number of pre-allocated workers used to forward push requests to the ingesters. If 0, no workers will be used and a new goroutine will be spawned for each ingester push request. If not enough workers available, new goroutine will be spawned. (Note: this is a performance optimization, not a limiting feature.)" },
    SchemaField { name: "max_influx_request_size", flag: "distributor.max-influx-request-size", kind: FieldKind::Int, default_str: "104857600", usage: "" },
    SchemaField { name: "write_compartment_id", flag: "distributor.write-compartment-id", kind: FieldKind::Int, default_str: "0", usage: "" },
    SchemaField { name: "influx_endpoint_enabled", flag: "distributor.influx-endpoint-enabled", kind: FieldKind::Bool, default_str: "false", usage: "" },
    SchemaField { name: "start_time_quiet_zero", flag: "distributor.otel-start-time-quiet-zero", kind: FieldKind::Bool, default_str: "false", usage: "" },
];

static USAGE_TRACKER_GRPC_BACKOFF_FIELDS: &[SchemaField] = &[
    SchemaField { name: "min_period", flag: "", kind: FieldKind::Duration, default_str: "100ms", usage: "" },
    SchemaField { name: "max_period", flag: "", kind: FieldKind::Duration, default_str: "10s", usage: "" },
    SchemaField { name: "max_retries", flag: "", kind: FieldKind::Int, default_str: "10", usage: "" },
];
pub use crate::schema_hidden::HIDDEN_TOP_BLOCKS;

static USAGE_TRACKER_GRPC_CCV_FIELDS: &[SchemaField] = &[
    SchemaField { name: "label", flag: "distributor.usage-tracker-client.grpc-client-config.cluster-validation.label", kind: FieldKind::String, default_str: "", usage: "" },
];
static USAGE_TRACKER_GRPC_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "backoff_config", fields: USAGE_TRACKER_GRPC_BACKOFF_FIELDS, blocks: &[] },
    SchemaBlock { name: "cluster_validation", fields: USAGE_TRACKER_GRPC_CCV_FIELDS, blocks: &[] },
];
static USAGE_TRACKER_GRPC_FIELDS: &[SchemaField] = &[
    SchemaField { name: "max_recv_msg_size", flag: "", kind: FieldKind::Int, default_str: "104857600", usage: "" },
    SchemaField { name: "max_send_msg_size", flag: "", kind: FieldKind::Int, default_str: "104857600", usage: "" },
    SchemaField { name: "grpc_compression", flag: "", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "rate_limit", flag: "", kind: FieldKind::Float, default_str: "0", usage: "" },
    SchemaField { name: "rate_limit_burst", flag: "", kind: FieldKind::Int, default_str: "0", usage: "" },
    SchemaField { name: "backoff_on_ratelimits", flag: "", kind: FieldKind::Bool, default_str: "false", usage: "" },
    SchemaField { name: "initial_stream_window_size", flag: "", kind: FieldKind::Bytes, default_str: "63KiB1023B", usage: "" },
    SchemaField { name: "initial_connection_window_size", flag: "", kind: FieldKind::Bytes, default_str: "63KiB1023B", usage: "" },
    SchemaField { name: "tls_enabled", flag: "", kind: FieldKind::Bool, default_str: "false", usage: "" },
    SchemaField { name: "tls_cert_path", flag: "", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "tls_key_path", flag: "", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "tls_ca_path", flag: "", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "tls_server_name", flag: "", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "tls_insecure_skip_verify", flag: "", kind: FieldKind::Bool, default_str: "false", usage: "" },
    SchemaField { name: "tls_cipher_suites", flag: "", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "tls_min_version", flag: "", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "connect_timeout", flag: "", kind: FieldKind::Duration, default_str: "5s", usage: "" },
    SchemaField { name: "connect_backoff_base_delay", flag: "", kind: FieldKind::Duration, default_str: "1s", usage: "" },
    SchemaField { name: "connect_backoff_max_delay", flag: "", kind: FieldKind::Duration, default_str: "5s", usage: "" },
    SchemaField { name: "keepalive_time", flag: "", kind: FieldKind::Duration, default_str: "20s", usage: "" },
    SchemaField { name: "keepalive_timeout", flag: "", kind: FieldKind::Duration, default_str: "10s", usage: "" },
    SchemaField { name: "health_check_grace_period", flag: "", kind: FieldKind::Duration, default_str: "0s", usage: "" },
];
static USAGE_TRACKER_CLIENT_FIELDS: &[SchemaField] = &[
    SchemaField { name: "ignore_rejected_series", flag: "", kind: FieldKind::Bool, default_str: "false", usage: "" },
    SchemaField { name: "ignore_errors", flag: "", kind: FieldKind::Bool, default_str: "false", usage: "" },
    SchemaField { name: "prefer_availability_zone", flag: "", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "requests_hedging_delay", flag: "", kind: FieldKind::Duration, default_str: "100ms", usage: "" },
    SchemaField { name: "reusable_workers", flag: "", kind: FieldKind::Int, default_str: "500", usage: "" },
    SchemaField { name: "tls_enabled", flag: "", kind: FieldKind::Bool, default_str: "false", usage: "" },
    SchemaField { name: "tls_cert_path", flag: "", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "tls_key_path", flag: "", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "tls_ca_path", flag: "", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "tls_server_name", flag: "", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "tls_insecure_skip_verify", flag: "", kind: FieldKind::Bool, default_str: "false", usage: "" },
    SchemaField { name: "tls_cipher_suites", flag: "", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "tls_min_version", flag: "", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "users_close_to_limit_poll_interval", flag: "", kind: FieldKind::Duration, default_str: "1s", usage: "" },
    SchemaField { name: "users_close_to_limit_cache_startup_retries", flag: "", kind: FieldKind::Int, default_str: "3", usage: "" },
    SchemaField { name: "min_series_limit_for_async_tracking", flag: "", kind: FieldKind::Int, default_str: "0", usage: "" },
    SchemaField { name: "batch_delay", flag: "", kind: FieldKind::Duration, default_str: "750ms", usage: "" },
    SchemaField { name: "max_batch_series", flag: "", kind: FieldKind::Int, default_str: "1000000", usage: "" },
    SchemaField { name: "track_series_batch_timeout", flag: "", kind: FieldKind::Duration, default_str: "2s", usage: "" },
    SchemaField { name: "sync_batch_delay", flag: "", kind: FieldKind::Duration, default_str: "50ms", usage: "" },
    SchemaField { name: "sync_batch_independent_partition_timeouts", flag: "", kind: FieldKind::Bool, default_str: "false", usage: "" },
];
static USAGE_TRACKER_CLIENT_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "grpc", fields: USAGE_TRACKER_GRPC_FIELDS, blocks: USAGE_TRACKER_GRPC_BLOCKS },
];

static BLOCK_15_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "pool", fields: BLOCK_1_FIELDS, blocks: BLOCK_1_BLOCKS },
    SchemaBlock { name: "retry_after_header", fields: BLOCK_2_FIELDS, blocks: BLOCK_2_BLOCKS },
    SchemaBlock { name: "ha_tracker", fields: BLOCK_7_FIELDS, blocks: BLOCK_7_BLOCKS },
    SchemaBlock { name: "ring", fields: BLOCK_12_FIELDS, blocks: BLOCK_12_BLOCKS },
    SchemaBlock { name: "instance_limits", fields: BLOCK_13_FIELDS, blocks: BLOCK_13_BLOCKS },
    SchemaBlock { name: "reactive_limiter", fields: BLOCK_14_FIELDS, blocks: BLOCK_14_BLOCKS },
    SchemaBlock { name: "usage_tracker_client", fields: USAGE_TRACKER_CLIENT_FIELDS, blocks: USAGE_TRACKER_CLIENT_BLOCKS },
];

static BLOCK_16_FIELDS: &[SchemaField] = &[
    SchemaField { name: "min_period", flag: "querier.store-gateway-client.backoff-min-period", kind: FieldKind::Duration, default_str: "100000000", usage: "Minimum delay when backing off." },
    SchemaField { name: "max_period", flag: "querier.store-gateway-client.backoff-max-period", kind: FieldKind::Duration, default_str: "10000000000", usage: "Maximum delay when backing off." },
    SchemaField { name: "max_retries", flag: "querier.store-gateway-client.backoff-retries", kind: FieldKind::Int, default_str: "10", usage: "Number of times to backoff and retry before failing." },
];

static BLOCK_16_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_17_FIELDS: &[SchemaField] = &[
    SchemaField { name: "label", flag: "querier.store-gateway-client.cluster-validation.label", kind: FieldKind::String, default_str: "", usage: "Primary cluster validation label." },
];

static BLOCK_17_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_18_FIELDS: &[SchemaField] = &[
    SchemaField { name: "max_recv_msg_size", flag: "querier.store-gateway-client.grpc-max-recv-msg-size", kind: FieldKind::Int, default_str: "104857600", usage: "gRPC client max receive message size (bytes)." },
    SchemaField { name: "max_send_msg_size", flag: "querier.store-gateway-client.grpc-max-send-msg-size", kind: FieldKind::Int, default_str: "104857600", usage: "gRPC client max send message size (bytes)." },
    SchemaField { name: "grpc_compression", flag: "querier.store-gateway-client.grpc-compression", kind: FieldKind::String, default_str: "", usage: "Use compression when sending messages. Supported values are: 'gzip', 'snappy' and '' (disable compression)" },
    SchemaField { name: "rate_limit", flag: "querier.store-gateway-client.grpc-client-rate-limit", kind: FieldKind::Float, default_str: "0", usage: "Rate limit for gRPC client; 0 means disabled." },
    SchemaField { name: "rate_limit_burst", flag: "querier.store-gateway-client.grpc-client-rate-limit-burst", kind: FieldKind::Int, default_str: "0", usage: "Rate limit burst for gRPC client." },
    SchemaField { name: "backoff_on_ratelimits", flag: "querier.store-gateway-client.backoff-on-ratelimits", kind: FieldKind::Bool, default_str: "false", usage: "Enable backoff and retry when we hit rate limits." },
    SchemaField { name: "initial_stream_window_size", flag: "querier.store-gateway-client.initial-stream-window-size", kind: FieldKind::Bytes, default_str: "63KiB1023B", usage: "Initial stream window size. Values less than the default are not supported and are ignored. Setting this to a value other than the default disables the BDP estimator." },
    SchemaField { name: "initial_connection_window_size", flag: "querier.store-gateway-client.initial-connection-window-size", kind: FieldKind::Bytes, default_str: "63KiB1023B", usage: "Initial connection window size. Values less than the default are not supported and are ignored. Setting this to a value other than the default disables the BDP estimator." },
    SchemaField { name: "tls_enabled", flag: "querier.store-gateway-client.tls-enabled", kind: FieldKind::Bool, default_str: "false", usage: "Enable TLS in the gRPC client. This flag needs to be enabled when any other TLS flag is set. If set to false, insecure connection to gRPC server will be used." },
    SchemaField { name: "tls_cert_path", flag: "querier.store-gateway-client.tls-cert-path", kind: FieldKind::String, default_str: "", usage: "Path to the client certificate, which will be used for authenticating with the server. Also requires the key path to be configured." },
    SchemaField { name: "tls_key_path", flag: "querier.store-gateway-client.tls-key-path", kind: FieldKind::String, default_str: "", usage: "Path to the key for the client certificate. Also requires the client certificate to be configured." },
    SchemaField { name: "tls_ca_path", flag: "querier.store-gateway-client.tls-ca-path", kind: FieldKind::String, default_str: "", usage: "Path to the CA certificates to validate server certificate against. If not set, the host's root CA certificates are used." },
    SchemaField { name: "tls_server_name", flag: "querier.store-gateway-client.tls-server-name", kind: FieldKind::String, default_str: "", usage: "Override the expected name on the server certificate." },
    SchemaField { name: "tls_insecure_skip_verify", flag: "querier.store-gateway-client.tls-insecure-skip-verify", kind: FieldKind::Bool, default_str: "false", usage: "Skip validating server certificate." },
    SchemaField { name: "tls_cipher_suites", flag: "querier.store-gateway-client.tls-cipher-suites", kind: FieldKind::String, default_str: "", usage: "Override the default cipher suite list (separated by commas). Allowed values:\n\nSecure Ciphers:\n- TLS_AES_128_GCM_SHA256\n- TLS_AES_256_GCM_SHA384\n- TLS_CHACHA20_POLY1305_SHA256\n- TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA\n- TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA\n- TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256\n- TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256\n- TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256\n- TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256\n\nInsecure Ciphers:\n- TLS_RSA_WITH_RC4_128_SHA\n- TLS_RSA_WITH_3DES_EDE_CBC_SHA\n- TLS_RSA_WITH_AES_128_CBC_SHA\n- TLS_RSA_WITH_AES_256_CBC_SHA\n- TLS_RSA_WITH_AES_128_CBC_SHA256\n- TLS_RSA_WITH_AES_128_GCM_SHA256\n- TLS_RSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_ECDSA_WITH_RC4_128_SHA\n- TLS_ECDHE_RSA_WITH_RC4_128_SHA\n- TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256\n- TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256\n" },
    SchemaField { name: "tls_min_version", flag: "querier.store-gateway-client.tls-min-version", kind: FieldKind::String, default_str: "", usage: "Override the default minimum TLS version. Allowed values: VersionTLS10, VersionTLS11, VersionTLS12, VersionTLS13" },
    SchemaField { name: "connect_timeout", flag: "querier.store-gateway-client.connect-timeout", kind: FieldKind::Duration, default_str: "5000000000", usage: "The maximum amount of time to establish a connection. A value of 0 means default gRPC client connect timeout and backoff." },
    SchemaField { name: "connect_backoff_base_delay", flag: "querier.store-gateway-client.connect-backoff-base-delay", kind: FieldKind::Duration, default_str: "1000000000", usage: "Initial backoff delay after first connection failure. Only relevant if ConnectTimeout > 0." },
    SchemaField { name: "connect_backoff_max_delay", flag: "querier.store-gateway-client.connect-backoff-max-delay", kind: FieldKind::Duration, default_str: "5000000000", usage: "Maximum backoff delay when establishing a connection. Only relevant if ConnectTimeout > 0." },
    SchemaField { name: "keepalive_time", flag: "querier.store-gateway-client.keepalive-time", kind: FieldKind::Duration, default_str: "20000000000", usage: "After a duration of this time if the client doesn't see any activity it pings the server to see if the transport is still alive. This also determines the socket's TCP_USER_TIMEOUT together with keepalive-timeout." },
    SchemaField { name: "keepalive_timeout", flag: "querier.store-gateway-client.keepalive-timeout", kind: FieldKind::Duration, default_str: "10000000000", usage: "After having pinged for keepalive check, the client waits for a duration of this time and if no activity is seen even after that the connection is closed." },
    SchemaField { name: "health_check_grace_period", flag: "querier.store-gateway-client.health-check-grace-period", kind: FieldKind::Duration, default_str: "0", usage: "The grace period for health checks. If a store-gateway connection consistently fails health checks for this period, any open connections are closed. The querier will attempt to reconnect to the store-gateway if a subsequent request is made to the store-gateway. Set to 0 to immediately remove store-gateway connections on the first health check failure." },
];

static BLOCK_18_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "backoff_config", fields: BLOCK_16_FIELDS, blocks: BLOCK_16_BLOCKS },
    SchemaBlock { name: "cluster_validation", fields: BLOCK_17_FIELDS, blocks: BLOCK_17_BLOCKS },
];

static BLOCK_19_FIELDS: &[SchemaField] = &[
    SchemaField { name: "addresses", flag: "querier.mimir-query-engine.range-vector-splitting.memcached.addresses", kind: FieldKind::String, default_str: "", usage: "Comma-separated list of memcached addresses. Each address can be an IP address, hostname, or an entry specified in the DNS Service Discovery format." },
    SchemaField { name: "addresses_lookup_period", flag: "querier.mimir-query-engine.range-vector-splitting.memcached.addresses-lookup-period", kind: FieldKind::Duration, default_str: "30000000000", usage: "How often each address is resolved to an IP address or list of host names." },
    SchemaField { name: "addresses_lookup_pool_size", flag: "querier.mimir-query-engine.range-vector-splitting.memcached.addresses-lookup-pool-size", kind: FieldKind::Int, default_str: "0", usage: "How many idle connections to the DNS resolver are kept open. 0 disables keeping any idle connections open." },
    SchemaField { name: "timeout", flag: "querier.mimir-query-engine.range-vector-splitting.memcached.timeout", kind: FieldKind::Duration, default_str: "200000000", usage: "The socket read/write timeout." },
    SchemaField { name: "connect_timeout", flag: "querier.mimir-query-engine.range-vector-splitting.memcached.connect-timeout", kind: FieldKind::Duration, default_str: "200000000", usage: "The connection timeout." },
    SchemaField { name: "min_idle_connections_headroom_percentage", flag: "querier.mimir-query-engine.range-vector-splitting.memcached.min-idle-connections-headroom-percentage", kind: FieldKind::Float, default_str: "-1", usage: "The minimum number of idle connections to keep open as a percentage (0-100) of the number of recently used idle connections. If negative, idle connections are kept open indefinitely." },
    SchemaField { name: "max_idle_connections", flag: "querier.mimir-query-engine.range-vector-splitting.memcached.max-idle-connections", kind: FieldKind::Int, default_str: "100", usage: "The maximum number of idle connections that will be maintained per address." },
    SchemaField { name: "max_async_concurrency", flag: "querier.mimir-query-engine.range-vector-splitting.memcached.max-async-concurrency", kind: FieldKind::Int, default_str: "50", usage: "The maximum number of concurrent asynchronous operations can occur." },
    SchemaField { name: "max_async_buffer_size", flag: "querier.mimir-query-engine.range-vector-splitting.memcached.max-async-buffer-size", kind: FieldKind::Int, default_str: "25000", usage: "The maximum number of enqueued asynchronous operations allowed." },
    SchemaField { name: "max_get_multi_concurrency", flag: "querier.mimir-query-engine.range-vector-splitting.memcached.max-get-multi-concurrency", kind: FieldKind::Int, default_str: "100", usage: "The maximum number of concurrent connections running get operations. If set to 0, concurrency is unlimited." },
    SchemaField { name: "max_get_multi_batch_size", flag: "querier.mimir-query-engine.range-vector-splitting.memcached.max-get-multi-batch-size", kind: FieldKind::Int, default_str: "100", usage: "The maximum number of keys a single underlying get operation should run. If more keys are specified, internally keys are split into multiple batches and fetched concurrently, honoring the max concurrency. If set to 0, the max batch size is unlimited." },
    SchemaField { name: "max_item_size", flag: "querier.mimir-query-engine.range-vector-splitting.memcached.max-item-size", kind: FieldKind::Int, default_str: "1048576", usage: "The maximum size of an item stored in memcached, in bytes. Bigger items are not stored. If set to 0, no maximum size is enforced." },
    SchemaField { name: "free_list_shards", flag: "querier.mimir-query-engine.range-vector-splitting.memcached.free-list-shards", kind: FieldKind::Int, default_str: "1", usage: "The number of shards to stripe the free list used for connections in the cache client over. Must be a power of 2. Increasing the number of shards reduces lock contention at the cost of higher or wasted memory." },
    SchemaField { name: "tls_enabled", flag: "querier.mimir-query-engine.range-vector-splitting.memcached.tls-enabled", kind: FieldKind::Bool, default_str: "false", usage: "Enable connecting to Memcached with TLS." },
    SchemaField { name: "tls_cert_path", flag: "querier.mimir-query-engine.range-vector-splitting.memcached.tls-cert-path", kind: FieldKind::String, default_str: "", usage: "Path to the client certificate, which will be used for authenticating with the server. Also requires the key path to be configured." },
    SchemaField { name: "tls_key_path", flag: "querier.mimir-query-engine.range-vector-splitting.memcached.tls-key-path", kind: FieldKind::String, default_str: "", usage: "Path to the key for the client certificate. Also requires the client certificate to be configured." },
    SchemaField { name: "tls_ca_path", flag: "querier.mimir-query-engine.range-vector-splitting.memcached.tls-ca-path", kind: FieldKind::String, default_str: "", usage: "Path to the CA certificates to validate server certificate against. If not set, the host's root CA certificates are used." },
    SchemaField { name: "tls_server_name", flag: "querier.mimir-query-engine.range-vector-splitting.memcached.tls-server-name", kind: FieldKind::String, default_str: "", usage: "Override the expected name on the server certificate." },
    SchemaField { name: "tls_insecure_skip_verify", flag: "querier.mimir-query-engine.range-vector-splitting.memcached.tls-insecure-skip-verify", kind: FieldKind::Bool, default_str: "false", usage: "Skip validating server certificate." },
    SchemaField { name: "tls_cipher_suites", flag: "querier.mimir-query-engine.range-vector-splitting.memcached.tls-cipher-suites", kind: FieldKind::String, default_str: "", usage: "Override the default cipher suite list (separated by commas). Allowed values:\n\nSecure Ciphers:\n- TLS_AES_128_GCM_SHA256\n- TLS_AES_256_GCM_SHA384\n- TLS_CHACHA20_POLY1305_SHA256\n- TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA\n- TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA\n- TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256\n- TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256\n- TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256\n- TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256\n\nInsecure Ciphers:\n- TLS_RSA_WITH_RC4_128_SHA\n- TLS_RSA_WITH_3DES_EDE_CBC_SHA\n- TLS_RSA_WITH_AES_128_CBC_SHA\n- TLS_RSA_WITH_AES_256_CBC_SHA\n- TLS_RSA_WITH_AES_128_CBC_SHA256\n- TLS_RSA_WITH_AES_128_GCM_SHA256\n- TLS_RSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_ECDSA_WITH_RC4_128_SHA\n- TLS_ECDHE_RSA_WITH_RC4_128_SHA\n- TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256\n- TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256\n" },
    SchemaField { name: "tls_min_version", flag: "querier.mimir-query-engine.range-vector-splitting.memcached.tls-min-version", kind: FieldKind::String, default_str: "", usage: "Override the default minimum TLS version. Allowed values: VersionTLS10, VersionTLS11, VersionTLS12, VersionTLS13" },
];

static BLOCK_19_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_20_FIELDS: &[SchemaField] = &[
    SchemaField { name: "backend", flag: "querier.mimir-query-engine.range-vector-splitting.backend", kind: FieldKind::String, default_str: "", usage: "Backend for intermediate results cache, if not empty. Supported values: memcached." },
    SchemaField { name: "compression", flag: "querier.mimir-query-engine.range-vector-splitting.compression", kind: FieldKind::String, default_str: "", usage: "Enable cache compression, if not empty. Supported values are: snappy." },
];

static BLOCK_20_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "memcached", fields: BLOCK_19_FIELDS, blocks: BLOCK_19_BLOCKS },
];

static BLOCK_21_FIELDS: &[SchemaField] = &[
    SchemaField { name: "enabled", flag: "querier.mimir-query-engine.range-vector-splitting.enabled", kind: FieldKind::Bool, default_str: "false", usage: "Enable splitting function over range vectors queries into smaller blocks for caching." },
    SchemaField { name: "split_interval", flag: "querier.mimir-query-engine.range-vector-splitting.split-interval", kind: FieldKind::Duration, default_str: "7200000000000", usage: "Time interval used for splitting function over range vectors queries into cacheable blocks." },
    SchemaField { name: "enable_subquery_splitting", flag: "querier.mimir-query-engine.range-vector-splitting.enable-subquery-splitting", kind: FieldKind::Bool, default_str: "false", usage: "Enable splitting subqueries, in addition to range vector selectors. Requires -querier.mimir-query-engine.range-vector-splitting.enabled and -querier.mimir-query-engine.enable-common-subexpression-elimination to also be enabled." },
];

static BLOCK_21_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "intermediate_results_cache", fields: BLOCK_20_FIELDS, blocks: BLOCK_20_BLOCKS },
];

static BLOCK_22_FIELDS: &[SchemaField] = &[
    SchemaField { name: "cache_unconsumed_results", flag: "querier.mimir-query-engine.time-splitting-and-caching.cache-unconsumed-results", kind: FieldKind::Bool, default_str: "true", usage: "Enable caching of query results that were not fully consumed by the query. When enabled, if a query stops reading before all series have been read, the remaining series are read and buffered so that the complete set of results can be cached." },
];

static BLOCK_22_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_23_FIELDS: &[SchemaField] = &[
    SchemaField { name: "bucket_size", flag: "querier.mimir-query-engine.cardinality-estimation.bucket-size", kind: FieldKind::Duration, default_str: "14400000000000", usage: "The duration of each bucket used to store cardinality estimates per selector. Only applies if running splitting and caching inside MQE is enabled with -query-frontend.use-mimir-query-engine-for-splitting-and-caching-results=true." },
    SchemaField { name: "ttl", flag: "querier.mimir-query-engine.cardinality-estimation.ttl", kind: FieldKind::Duration, default_str: "604800000000000", usage: "The time-to-live of each cached cardinality estimate. Only applies if running splitting and caching inside MQE is enabled with -query-frontend.use-mimir-query-engine-for-splitting-and-caching-results=true." },
    SchemaField { name: "max_buckets_read_per_selector", flag: "querier.mimir-query-engine.cardinality-estimation.max-buckets-read-per-selector", kind: FieldKind::Int, default_str: "168", usage: "The maximum number of buckets to attempt to read per selector. If a selector's time range queries more buckets than this limit, buckets over the entire time range are sampled (i.e. the resolution is reduced). Only applies if running splitting and caching inside MQE is enabled with -query-frontend.use-mimir-query-engine-for-splitting-and-caching-results=true." },
    SchemaField { name: "estimate_update_threshold", flag: "querier.mimir-query-engine.cardinality-estimation.estimate-update-threshold", kind: FieldKind::Float, default_str: "0.1", usage: "The minimum difference from the original estimate to trigger storing a new cardinality estimate in the cache. Values are a proportion of the original value (e.g. a value of 0.1 means a new estimate is only written if the new value is 10% higher than the original estimate). Only applies if running splitting and caching inside MQE is enabled with -query-frontend.use-mimir-query-engine-for-splitting-and-caching-results=true." },
    SchemaField { name: "use_cryptographic_hash", flag: "querier.mimir-query-engine.cardinality-estimation.use-cryptographic-hash", kind: FieldKind::Bool, default_str: "false", usage: "If true, use a cryptographic hash function to generate hash keys, and don't store the unhashed version of the cache key in each entry. Only applies if running splitting and caching inside MQE is enabled with -query-frontend.use-mimir-query-engine-for-splitting-and-caching-results=true." },
];

static BLOCK_23_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_24_FIELDS: &[SchemaField] = &[
    SchemaField { name: "enable_common_subexpression_elimination", flag: "querier.mimir-query-engine.enable-common-subexpression-elimination", kind: FieldKind::Bool, default_str: "true", usage: "Enable common subexpression elimination when evaluating queries." },
    SchemaField { name: "enable_subset_selector_elimination", flag: "querier.mimir-query-engine.enable-subset-selector-elimination", kind: FieldKind::Bool, default_str: "true", usage: "Enable subset selector elimination when evaluating queries." },
    SchemaField { name: "enable_range_query_range_vector_common_subexpression_elimination", flag: "querier.mimir-query-engine.enable-range-query-range-vector-common-subexpression-elimination", kind: FieldKind::Bool, default_str: "true", usage: "Enable deduplication of range vector selectors in range queries as part of common subexpression elimination. Requires common subexpression elimination to be enabled." },
    SchemaField { name: "enable_narrow_binary_selectors", flag: "querier.mimir-query-engine.enable-narrow-binary-selectors", kind: FieldKind::Bool, default_str: "false", usage: "Enable generating selectors for one side of a binary expression based on results from the other side." },
    SchemaField { name: "enable_eliminate_deduplicate_and_merge", flag: "querier.mimir-query-engine.enable-eliminate-deduplicate-and-merge", kind: FieldKind::Bool, default_str: "true", usage: "Enable eliminating redundant DeduplicateAndMerge nodes from the query plan when it can be proven that each input series produces a unique output series." },
    SchemaField { name: "enable_propagate_matchers", flag: "querier.mimir-query-engine.enable-propagate-matchers", kind: FieldKind::Bool, default_str: "false", usage: "Enable propagating label matchers across binary expressions." },
    SchemaField { name: "enable_reduce_matchers", flag: "querier.mimir-query-engine.enable-reduce-matchers", kind: FieldKind::Bool, default_str: "true", usage: "Enable eliminating duplicate or redundant matchers that are part of selector expressions." },
    SchemaField { name: "enable_multi_aggregation", flag: "querier.mimir-query-engine.enable-multi-aggregation", kind: FieldKind::Bool, default_str: "true", usage: "Enable computing multiple aggregations over the same data without buffering. Requires common subexpression elimination to be enabled." },
    SchemaField { name: "enable_remove_statically_empty_expressions", flag: "querier.mimir-query-engine.enable-remove-statically-empty-expressions", kind: FieldKind::Bool, default_str: "true", usage: "Enable removing expressions that are guaranteed to produce no results." },
];

static BLOCK_24_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "range_vector_splitting", fields: BLOCK_21_FIELDS, blocks: BLOCK_21_BLOCKS },
    SchemaBlock { name: "time_splitting_and_caching", fields: BLOCK_22_FIELDS, blocks: BLOCK_22_BLOCKS },
    SchemaBlock { name: "cardinality_estimation", fields: BLOCK_23_FIELDS, blocks: BLOCK_23_BLOCKS },
];

static BLOCK_25_FIELDS: &[SchemaField] = &[
    SchemaField { name: "host", flag: "querier.ring.consul.hostname", kind: FieldKind::String, default_str: "localhost:8500", usage: "Hostname and port of Consul." },
    SchemaField { name: "acl_token", flag: "querier.ring.consul.acl-token", kind: FieldKind::Secret, default_str: "", usage: "ACL Token used to interact with Consul." },
    SchemaField { name: "http_client_timeout", flag: "querier.ring.consul.client-timeout", kind: FieldKind::Duration, default_str: "20000000000", usage: "HTTP timeout when talking to Consul" },
    SchemaField { name: "consistent_reads", flag: "querier.ring.consul.consistent-reads", kind: FieldKind::Bool, default_str: "false", usage: "Enable consistent reads to Consul." },
    SchemaField { name: "watch_rate_limit", flag: "querier.ring.consul.watch-rate-limit", kind: FieldKind::Float, default_str: "1", usage: "Rate limit when watching key or prefix in Consul, in requests per second. 0 disables the rate limit." },
    SchemaField { name: "watch_burst_size", flag: "querier.ring.consul.watch-burst-size", kind: FieldKind::Int, default_str: "1", usage: "Burst size used in rate limit. Values less than 1 are treated as 1." },
    SchemaField { name: "cas_retry_delay", flag: "querier.ring.consul.cas-retry-delay", kind: FieldKind::Duration, default_str: "1000000000", usage: "Maximum duration to wait before retrying a Compare And Swap (CAS) operation." },
];

static BLOCK_25_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_26_FIELDS: &[SchemaField] = &[
    SchemaField { name: "endpoints", flag: "querier.ring.etcd.endpoints", kind: FieldKind::StringList, default_str: "", usage: "The etcd endpoints to connect to." },
    SchemaField { name: "dial_timeout", flag: "querier.ring.etcd.dial-timeout", kind: FieldKind::Duration, default_str: "10000000000", usage: "The dial timeout for the etcd connection." },
    SchemaField { name: "max_retries", flag: "querier.ring.etcd.max-retries", kind: FieldKind::Int, default_str: "10", usage: "The maximum number of retries to do for failed ops." },
    SchemaField { name: "tls_enabled", flag: "querier.ring.etcd.tls-enabled", kind: FieldKind::Bool, default_str: "false", usage: "Enable TLS." },
    SchemaField { name: "tls_cert_path", flag: "querier.ring.etcd.tls-cert-path", kind: FieldKind::String, default_str: "", usage: "Path to the client certificate, which will be used for authenticating with the server. Also requires the key path to be configured." },
    SchemaField { name: "tls_key_path", flag: "querier.ring.etcd.tls-key-path", kind: FieldKind::String, default_str: "", usage: "Path to the key for the client certificate. Also requires the client certificate to be configured." },
    SchemaField { name: "tls_ca_path", flag: "querier.ring.etcd.tls-ca-path", kind: FieldKind::String, default_str: "", usage: "Path to the CA certificates to validate server certificate against. If not set, the host's root CA certificates are used." },
    SchemaField { name: "tls_server_name", flag: "querier.ring.etcd.tls-server-name", kind: FieldKind::String, default_str: "", usage: "Override the expected name on the server certificate." },
    SchemaField { name: "tls_insecure_skip_verify", flag: "querier.ring.etcd.tls-insecure-skip-verify", kind: FieldKind::Bool, default_str: "false", usage: "Skip validating server certificate." },
    SchemaField { name: "tls_cipher_suites", flag: "querier.ring.etcd.tls-cipher-suites", kind: FieldKind::String, default_str: "", usage: "Override the default cipher suite list (separated by commas). Allowed values:\n\nSecure Ciphers:\n- TLS_AES_128_GCM_SHA256\n- TLS_AES_256_GCM_SHA384\n- TLS_CHACHA20_POLY1305_SHA256\n- TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA\n- TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA\n- TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256\n- TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256\n- TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256\n- TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256\n\nInsecure Ciphers:\n- TLS_RSA_WITH_RC4_128_SHA\n- TLS_RSA_WITH_3DES_EDE_CBC_SHA\n- TLS_RSA_WITH_AES_128_CBC_SHA\n- TLS_RSA_WITH_AES_256_CBC_SHA\n- TLS_RSA_WITH_AES_128_CBC_SHA256\n- TLS_RSA_WITH_AES_128_GCM_SHA256\n- TLS_RSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_ECDSA_WITH_RC4_128_SHA\n- TLS_ECDHE_RSA_WITH_RC4_128_SHA\n- TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256\n- TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256\n" },
    SchemaField { name: "tls_min_version", flag: "querier.ring.etcd.tls-min-version", kind: FieldKind::String, default_str: "", usage: "Override the default minimum TLS version. Allowed values: VersionTLS10, VersionTLS11, VersionTLS12, VersionTLS13" },
    SchemaField { name: "username", flag: "querier.ring.etcd.username", kind: FieldKind::String, default_str: "", usage: "Etcd username." },
    SchemaField { name: "password", flag: "querier.ring.etcd.password", kind: FieldKind::Secret, default_str: "", usage: "Etcd password." },
];

static BLOCK_26_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_27_FIELDS: &[SchemaField] = &[
    SchemaField { name: "primary", flag: "querier.ring.multi.primary", kind: FieldKind::String, default_str: "", usage: "Primary backend storage used by multi-client." },
    SchemaField { name: "secondary", flag: "querier.ring.multi.secondary", kind: FieldKind::String, default_str: "", usage: "Secondary backend storage used by multi-client." },
    SchemaField { name: "mirror_enabled", flag: "querier.ring.multi.mirror-enabled", kind: FieldKind::Bool, default_str: "false", usage: "Mirror writes to the secondary store." },
    SchemaField { name: "mirror_timeout", flag: "querier.ring.multi.mirror-timeout", kind: FieldKind::Duration, default_str: "2000000000", usage: "Timeout for storing a value to the secondary store." },
];

static BLOCK_27_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_28_FIELDS: &[SchemaField] = &[
    SchemaField { name: "store", flag: "querier.ring.store", kind: FieldKind::String, default_str: "memberlist", usage: "Backend storage to use for the ring. Supported values are: consul, etcd, inmemory, memberlist, multi." },
    SchemaField { name: "prefix", flag: "querier.ring.prefix", kind: FieldKind::String, default_str: "collectors/", usage: "The prefix for the keys in the store. Should end with a /." },
];

static BLOCK_28_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "consul", fields: BLOCK_25_FIELDS, blocks: BLOCK_25_BLOCKS },
    SchemaBlock { name: "etcd", fields: BLOCK_26_FIELDS, blocks: BLOCK_26_BLOCKS },
    SchemaBlock { name: "multi", fields: BLOCK_27_FIELDS, blocks: BLOCK_27_BLOCKS },
];

static BLOCK_29_FIELDS: &[SchemaField] = &[
    SchemaField { name: "heartbeat_period", flag: "querier.ring.heartbeat-period", kind: FieldKind::Duration, default_str: "15000000000", usage: "Period at which to heartbeat to the ring." },
    SchemaField { name: "heartbeat_timeout", flag: "querier.ring.heartbeat-timeout", kind: FieldKind::Duration, default_str: "60000000000", usage: "Heartbeat timeout after which Mimir marks queriers as unhealthy in the ring." },
    SchemaField { name: "instance_id", flag: "querier.ring.instance-id", kind: FieldKind::String, default_str: "<hostname>", usage: "Instance ID to register in the ring." },
    SchemaField { name: "instance_interface_names", flag: "querier.ring.instance-interface-names", kind: FieldKind::StringList, default_str: "", usage: "List of network interface names to look up when finding the instance IP address." },
    SchemaField { name: "instance_port", flag: "querier.ring.instance-port", kind: FieldKind::Int, default_str: "0", usage: "Port to advertise in the ring (defaults to -server.grpc-listen-port)." },
    SchemaField { name: "instance_addr", flag: "querier.ring.instance-addr", kind: FieldKind::String, default_str: "", usage: "IP address to advertise in the ring. Default is auto-detected." },
    SchemaField { name: "instance_enable_ipv6", flag: "querier.ring.instance-enable-ipv6", kind: FieldKind::Bool, default_str: "false", usage: "Enable using an IPv6 instance address." },
    SchemaField { name: "auto_forget_unhealthy_periods", flag: "querier.ring.auto-forget-unhealthy-periods", kind: FieldKind::Int, default_str: "10", usage: "Number of consecutive timeout periods after which Mimir automatically removes an unhealthy instance in the ring. Set to 0 to disable auto-forget." },
];

static BLOCK_29_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "kvstore", fields: BLOCK_28_FIELDS, blocks: BLOCK_28_BLOCKS },
];

static BLOCK_30_FIELDS: &[SchemaField] = &[
    SchemaField { name: "query_store_after", flag: "querier.query-store-after", kind: FieldKind::Duration, default_str: "43200000000000", usage: "The time after which a metric should be queried from storage and not just ingesters. 0 means all queries are sent to store. If this option is enabled, the time range of the query sent to the store-gateway will be manipulated to ensure the query end is not more recent than 'now - query-store-after'." },
    SchemaField { name: "shuffle_sharding_ingesters_enabled", flag: "querier.shuffle-sharding-ingesters-enabled", kind: FieldKind::Bool, default_str: "true", usage: "Fetch in-memory series from the minimum set of required ingesters, selecting only ingesters which may have received series since -querier.query-ingesters-within. If this setting is false or -querier.query-ingesters-within is '0', queriers always query all ingesters (ingesters shuffle sharding on read path is disabled)." },
    SchemaField { name: "prefer_availability_zones", flag: "querier.prefer-availability-zones", kind: FieldKind::String, default_str: "", usage: "Comma-separated list of availability zones to prefer when querying ingesters and store-gateways. All zones in the list are given equal priority." },
    SchemaField { name: "streaming_chunks_per_ingester_series_buffer_size", flag: "querier.streaming-chunks-per-ingester-buffer-size", kind: FieldKind::Int, default_str: "256", usage: "Number of series to buffer per ingester when streaming chunks from ingesters." },
    SchemaField { name: "streaming_chunks_per_store_gateway_series_buffer_size", flag: "querier.streaming-chunks-per-store-gateway-buffer-size", kind: FieldKind::Int, default_str: "256", usage: "Number of series to buffer per store-gateway when streaming chunks from store-gateways." },
    SchemaField { name: "minimize_ingester_requests", flag: "querier.minimize-ingester-requests", kind: FieldKind::Bool, default_str: "true", usage: "If true, when querying ingesters, only the minimum required ingesters required to reach quorum will be queried initially, with other ingesters queried only if needed due to failures from the initial set of ingesters. Enabling this option reduces resource consumption for the happy path at the cost of increased latency for the unhappy path." },
    SchemaField { name: "minimize_ingester_requests_hedging_delay", flag: "querier.minimize-ingester-requests-hedging-delay", kind: FieldKind::Duration, default_str: "0", usage: "Delay before initiating requests to further ingesters when request minimization is enabled and the initially selected set of ingesters have not all responded. Set to 0 to disable hedging. Ignored if -querier.minimize-ingester-requests is not enabled." },
    SchemaField { name: "query_engine", flag: "querier.query-engine", kind: FieldKind::String, default_str: "mimir", usage: "Query engine to use, either 'prometheus' or 'mimir'" },
    SchemaField { name: "enable_query_engine_fallback", flag: "querier.enable-query-engine-fallback", kind: FieldKind::Bool, default_str: "true", usage: "If set to true and the Mimir query engine is in use, fall back to using the Prometheus query engine for any queries not supported by the Mimir query engine." },
    SchemaField { name: "experimental_search_api_enabled", flag: "querier.experimental-search-api-enabled", kind: FieldKind::Bool, default_str: "false", usage: "If set to true, enables the experimental streaming label/value search HTTP endpoints (/api/v1/search/{metric_names,label_names,label_values}). Mirrors Prometheus's experimental.search-api feature gate." },
    SchemaField { name: "max_concurrent_remote_read_queries", flag: "querier.max-concurrent-remote-read-queries", kind: FieldKind::Int, default_str: "2", usage: "Maximum number of remote read queries that can be executed concurrently. 0 or negative values mean unlimited concurrency." },
    SchemaField { name: "max_concurrent", flag: "querier.max-concurrent", kind: FieldKind::Int, default_str: "8", usage: "The number of workers running in each querier process. This setting limits the maximum number of concurrent queries in each querier. The minimum value is four; lower values are ignored and set to the minimum" },
    SchemaField { name: "timeout", flag: "querier.timeout", kind: FieldKind::Duration, default_str: "120000000000", usage: "The timeout for a query. This config option should be set on query-frontend too when query sharding is enabled. This also applies to queries evaluated by the ruler (internally or remotely)." },
    SchemaField { name: "max_samples", flag: "querier.max-samples", kind: FieldKind::Int, default_str: "50000000", usage: "Maximum number of samples a single query can load into memory. This config option should be set on query-frontend too when query sharding is enabled." },
    SchemaField { name: "default_evaluation_interval", flag: "querier.default-evaluation-interval", kind: FieldKind::Duration, default_str: "60000000000", usage: "The default evaluation interval or step size for subqueries. This config option should be set on query-frontend too when query sharding is enabled." },
    SchemaField { name: "lookback_delta", flag: "querier.lookback-delta", kind: FieldKind::Duration, default_str: "300000000000", usage: "Time since the last sample after which a time series is considered stale and ignored by expression evaluations. This config option should be set on query-frontend too when query sharding is enabled." },
    SchemaField { name: "enable_delayed_name_removal_prometheus_engine", flag: "querier.enable-delayed-name-removal-prometheus-engine", kind: FieldKind::Bool, default_str: "false", usage: "Enable the experimental PromQL feature for delayed name removal in the Prometheus engine. Note that this only applies when the Prometheus engine is selected or used as fallback from the Mimir Query Engine." },
];

static BLOCK_30_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "store_gateway_client", fields: BLOCK_18_FIELDS, blocks: BLOCK_18_BLOCKS },
    SchemaBlock { name: "mimir_query_engine", fields: BLOCK_24_FIELDS, blocks: BLOCK_24_BLOCKS },
    SchemaBlock { name: "ring", fields: BLOCK_29_FIELDS, blocks: BLOCK_29_BLOCKS },
];

static BLOCK_31_FIELDS: &[SchemaField] = &[
    SchemaField { name: "min_period", flag: "querier.frontend-client.backoff-min-period", kind: FieldKind::Duration, default_str: "100000000", usage: "Minimum delay when backing off." },
    SchemaField { name: "max_period", flag: "querier.frontend-client.backoff-max-period", kind: FieldKind::Duration, default_str: "10000000000", usage: "Maximum delay when backing off." },
    SchemaField { name: "max_retries", flag: "querier.frontend-client.backoff-retries", kind: FieldKind::Int, default_str: "10", usage: "Number of times to backoff and retry before failing." },
];

static BLOCK_31_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_32_FIELDS: &[SchemaField] = &[
    SchemaField { name: "label", flag: "querier.frontend-client.cluster-validation.label", kind: FieldKind::String, default_str: "", usage: "Primary cluster validation label." },
];

static BLOCK_32_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_33_FIELDS: &[SchemaField] = &[
    SchemaField { name: "max_recv_msg_size", flag: "querier.frontend-client.grpc-max-recv-msg-size", kind: FieldKind::Int, default_str: "104857600", usage: "gRPC client max receive message size (bytes)." },
    SchemaField { name: "max_send_msg_size", flag: "querier.frontend-client.grpc-max-send-msg-size", kind: FieldKind::Int, default_str: "104857600", usage: "gRPC client max send message size (bytes)." },
    SchemaField { name: "grpc_compression", flag: "querier.frontend-client.grpc-compression", kind: FieldKind::String, default_str: "", usage: "Use compression when sending messages. Supported values are: 'gzip', 'snappy', 's2' and '' (disable compression)" },
    SchemaField { name: "rate_limit", flag: "querier.frontend-client.grpc-client-rate-limit", kind: FieldKind::Float, default_str: "0", usage: "Rate limit for gRPC client; 0 means disabled." },
    SchemaField { name: "rate_limit_burst", flag: "querier.frontend-client.grpc-client-rate-limit-burst", kind: FieldKind::Int, default_str: "0", usage: "Rate limit burst for gRPC client." },
    SchemaField { name: "backoff_on_ratelimits", flag: "querier.frontend-client.backoff-on-ratelimits", kind: FieldKind::Bool, default_str: "false", usage: "Enable backoff and retry when we hit rate limits." },
    SchemaField { name: "initial_stream_window_size", flag: "querier.frontend-client.initial-stream-window-size", kind: FieldKind::Bytes, default_str: "63KiB1023B", usage: "Initial stream window size. Values less than the default are not supported and are ignored. Setting this to a value other than the default disables the BDP estimator." },
    SchemaField { name: "initial_connection_window_size", flag: "querier.frontend-client.initial-connection-window-size", kind: FieldKind::Bytes, default_str: "63KiB1023B", usage: "Initial connection window size. Values less than the default are not supported and are ignored. Setting this to a value other than the default disables the BDP estimator." },
    SchemaField { name: "tls_enabled", flag: "querier.frontend-client.tls-enabled", kind: FieldKind::Bool, default_str: "false", usage: "Enable TLS in the gRPC client. This flag needs to be enabled when any other TLS flag is set. If set to false, insecure connection to gRPC server will be used." },
    SchemaField { name: "tls_cert_path", flag: "querier.frontend-client.tls-cert-path", kind: FieldKind::String, default_str: "", usage: "Path to the client certificate, which will be used for authenticating with the server. Also requires the key path to be configured." },
    SchemaField { name: "tls_key_path", flag: "querier.frontend-client.tls-key-path", kind: FieldKind::String, default_str: "", usage: "Path to the key for the client certificate. Also requires the client certificate to be configured." },
    SchemaField { name: "tls_ca_path", flag: "querier.frontend-client.tls-ca-path", kind: FieldKind::String, default_str: "", usage: "Path to the CA certificates to validate server certificate against. If not set, the host's root CA certificates are used." },
    SchemaField { name: "tls_server_name", flag: "querier.frontend-client.tls-server-name", kind: FieldKind::String, default_str: "", usage: "Override the expected name on the server certificate." },
    SchemaField { name: "tls_insecure_skip_verify", flag: "querier.frontend-client.tls-insecure-skip-verify", kind: FieldKind::Bool, default_str: "false", usage: "Skip validating server certificate." },
    SchemaField { name: "tls_cipher_suites", flag: "querier.frontend-client.tls-cipher-suites", kind: FieldKind::String, default_str: "", usage: "Override the default cipher suite list (separated by commas). Allowed values:\n\nSecure Ciphers:\n- TLS_AES_128_GCM_SHA256\n- TLS_AES_256_GCM_SHA384\n- TLS_CHACHA20_POLY1305_SHA256\n- TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA\n- TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA\n- TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256\n- TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256\n- TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256\n- TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256\n\nInsecure Ciphers:\n- TLS_RSA_WITH_RC4_128_SHA\n- TLS_RSA_WITH_3DES_EDE_CBC_SHA\n- TLS_RSA_WITH_AES_128_CBC_SHA\n- TLS_RSA_WITH_AES_256_CBC_SHA\n- TLS_RSA_WITH_AES_128_CBC_SHA256\n- TLS_RSA_WITH_AES_128_GCM_SHA256\n- TLS_RSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_ECDSA_WITH_RC4_128_SHA\n- TLS_ECDHE_RSA_WITH_RC4_128_SHA\n- TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256\n- TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256\n" },
    SchemaField { name: "tls_min_version", flag: "querier.frontend-client.tls-min-version", kind: FieldKind::String, default_str: "", usage: "Override the default minimum TLS version. Allowed values: VersionTLS10, VersionTLS11, VersionTLS12, VersionTLS13" },
    SchemaField { name: "connect_timeout", flag: "querier.frontend-client.connect-timeout", kind: FieldKind::Duration, default_str: "5000000000", usage: "The maximum amount of time to establish a connection. A value of 0 means default gRPC client connect timeout and backoff." },
    SchemaField { name: "connect_backoff_base_delay", flag: "querier.frontend-client.connect-backoff-base-delay", kind: FieldKind::Duration, default_str: "1000000000", usage: "Initial backoff delay after first connection failure. Only relevant if ConnectTimeout > 0." },
    SchemaField { name: "connect_backoff_max_delay", flag: "querier.frontend-client.connect-backoff-max-delay", kind: FieldKind::Duration, default_str: "5000000000", usage: "Maximum backoff delay when establishing a connection. Only relevant if ConnectTimeout > 0." },
    SchemaField { name: "keepalive_time", flag: "querier.frontend-client.keepalive-time", kind: FieldKind::Duration, default_str: "20000000000", usage: "After a duration of this time if the client doesn't see any activity it pings the server to see if the transport is still alive. This also determines the socket's TCP_USER_TIMEOUT together with keepalive-timeout." },
    SchemaField { name: "keepalive_timeout", flag: "querier.frontend-client.keepalive-timeout", kind: FieldKind::Duration, default_str: "10000000000", usage: "After having pinged for keepalive check, the client waits for a duration of this time and if no activity is seen even after that the connection is closed." },
    SchemaField { name: "health_check_grace_period", flag: "querier.frontend-client.health-check-grace-period", kind: FieldKind::Duration, default_str: "0", usage: "The grace period for health checks. If a query-frontend connection consistently fails health checks for this period, any open connections are closed. The querier will attempt to reconnect to the query-frontend if a subsequent request is received from it. Set to 0 to immediately remove query-frontend connections on the first health check failure." },
];

static BLOCK_33_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "backoff_config", fields: BLOCK_31_FIELDS, blocks: BLOCK_31_BLOCKS },
    SchemaBlock { name: "cluster_validation", fields: BLOCK_32_FIELDS, blocks: BLOCK_32_BLOCKS },
];

static BLOCK_34_FIELDS: &[SchemaField] = &[
    SchemaField { name: "min_period", flag: "querier.scheduler-client.backoff-min-period", kind: FieldKind::Duration, default_str: "100000000", usage: "Minimum delay when backing off." },
    SchemaField { name: "max_period", flag: "querier.scheduler-client.backoff-max-period", kind: FieldKind::Duration, default_str: "10000000000", usage: "Maximum delay when backing off." },
    SchemaField { name: "max_retries", flag: "querier.scheduler-client.backoff-retries", kind: FieldKind::Int, default_str: "10", usage: "Number of times to backoff and retry before failing." },
];

static BLOCK_34_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_35_FIELDS: &[SchemaField] = &[
    SchemaField { name: "label", flag: "querier.scheduler-client.cluster-validation.label", kind: FieldKind::String, default_str: "", usage: "Primary cluster validation label." },
];

static BLOCK_35_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_36_FIELDS: &[SchemaField] = &[
    SchemaField { name: "max_recv_msg_size", flag: "querier.scheduler-client.grpc-max-recv-msg-size", kind: FieldKind::Int, default_str: "104857600", usage: "gRPC client max receive message size (bytes)." },
    SchemaField { name: "max_send_msg_size", flag: "querier.scheduler-client.grpc-max-send-msg-size", kind: FieldKind::Int, default_str: "104857600", usage: "gRPC client max send message size (bytes)." },
    SchemaField { name: "grpc_compression", flag: "querier.scheduler-client.grpc-compression", kind: FieldKind::String, default_str: "", usage: "Use compression when sending messages. Supported values are: 'gzip', 'snappy', 's2' and '' (disable compression)" },
    SchemaField { name: "rate_limit", flag: "querier.scheduler-client.grpc-client-rate-limit", kind: FieldKind::Float, default_str: "0", usage: "Rate limit for gRPC client; 0 means disabled." },
    SchemaField { name: "rate_limit_burst", flag: "querier.scheduler-client.grpc-client-rate-limit-burst", kind: FieldKind::Int, default_str: "0", usage: "Rate limit burst for gRPC client." },
    SchemaField { name: "backoff_on_ratelimits", flag: "querier.scheduler-client.backoff-on-ratelimits", kind: FieldKind::Bool, default_str: "false", usage: "Enable backoff and retry when we hit rate limits." },
    SchemaField { name: "initial_stream_window_size", flag: "querier.scheduler-client.initial-stream-window-size", kind: FieldKind::Bytes, default_str: "63KiB1023B", usage: "Initial stream window size. Values less than the default are not supported and are ignored. Setting this to a value other than the default disables the BDP estimator." },
    SchemaField { name: "initial_connection_window_size", flag: "querier.scheduler-client.initial-connection-window-size", kind: FieldKind::Bytes, default_str: "63KiB1023B", usage: "Initial connection window size. Values less than the default are not supported and are ignored. Setting this to a value other than the default disables the BDP estimator." },
    SchemaField { name: "tls_enabled", flag: "querier.scheduler-client.tls-enabled", kind: FieldKind::Bool, default_str: "false", usage: "Enable TLS in the gRPC client. This flag needs to be enabled when any other TLS flag is set. If set to false, insecure connection to gRPC server will be used." },
    SchemaField { name: "tls_cert_path", flag: "querier.scheduler-client.tls-cert-path", kind: FieldKind::String, default_str: "", usage: "Path to the client certificate, which will be used for authenticating with the server. Also requires the key path to be configured." },
    SchemaField { name: "tls_key_path", flag: "querier.scheduler-client.tls-key-path", kind: FieldKind::String, default_str: "", usage: "Path to the key for the client certificate. Also requires the client certificate to be configured." },
    SchemaField { name: "tls_ca_path", flag: "querier.scheduler-client.tls-ca-path", kind: FieldKind::String, default_str: "", usage: "Path to the CA certificates to validate server certificate against. If not set, the host's root CA certificates are used." },
    SchemaField { name: "tls_server_name", flag: "querier.scheduler-client.tls-server-name", kind: FieldKind::String, default_str: "", usage: "Override the expected name on the server certificate." },
    SchemaField { name: "tls_insecure_skip_verify", flag: "querier.scheduler-client.tls-insecure-skip-verify", kind: FieldKind::Bool, default_str: "false", usage: "Skip validating server certificate." },
    SchemaField { name: "tls_cipher_suites", flag: "querier.scheduler-client.tls-cipher-suites", kind: FieldKind::String, default_str: "", usage: "Override the default cipher suite list (separated by commas). Allowed values:\n\nSecure Ciphers:\n- TLS_AES_128_GCM_SHA256\n- TLS_AES_256_GCM_SHA384\n- TLS_CHACHA20_POLY1305_SHA256\n- TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA\n- TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA\n- TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256\n- TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256\n- TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256\n- TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256\n\nInsecure Ciphers:\n- TLS_RSA_WITH_RC4_128_SHA\n- TLS_RSA_WITH_3DES_EDE_CBC_SHA\n- TLS_RSA_WITH_AES_128_CBC_SHA\n- TLS_RSA_WITH_AES_256_CBC_SHA\n- TLS_RSA_WITH_AES_128_CBC_SHA256\n- TLS_RSA_WITH_AES_128_GCM_SHA256\n- TLS_RSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_ECDSA_WITH_RC4_128_SHA\n- TLS_ECDHE_RSA_WITH_RC4_128_SHA\n- TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256\n- TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256\n" },
    SchemaField { name: "tls_min_version", flag: "querier.scheduler-client.tls-min-version", kind: FieldKind::String, default_str: "", usage: "Override the default minimum TLS version. Allowed values: VersionTLS10, VersionTLS11, VersionTLS12, VersionTLS13" },
    SchemaField { name: "connect_timeout", flag: "querier.scheduler-client.connect-timeout", kind: FieldKind::Duration, default_str: "5000000000", usage: "The maximum amount of time to establish a connection. A value of 0 means default gRPC client connect timeout and backoff." },
    SchemaField { name: "connect_backoff_base_delay", flag: "querier.scheduler-client.connect-backoff-base-delay", kind: FieldKind::Duration, default_str: "1000000000", usage: "Initial backoff delay after first connection failure. Only relevant if ConnectTimeout > 0." },
    SchemaField { name: "connect_backoff_max_delay", flag: "querier.scheduler-client.connect-backoff-max-delay", kind: FieldKind::Duration, default_str: "5000000000", usage: "Maximum backoff delay when establishing a connection. Only relevant if ConnectTimeout > 0." },
    SchemaField { name: "keepalive_time", flag: "querier.scheduler-client.keepalive-time", kind: FieldKind::Duration, default_str: "20000000000", usage: "After a duration of this time if the client doesn't see any activity it pings the server to see if the transport is still alive. This also determines the socket's TCP_USER_TIMEOUT together with keepalive-timeout." },
    SchemaField { name: "keepalive_timeout", flag: "querier.scheduler-client.keepalive-timeout", kind: FieldKind::Duration, default_str: "10000000000", usage: "After having pinged for keepalive check, the client waits for a duration of this time and if no activity is seen even after that the connection is closed." },
];

static BLOCK_36_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "backoff_config", fields: BLOCK_34_FIELDS, blocks: BLOCK_34_BLOCKS },
    SchemaBlock { name: "cluster_validation", fields: BLOCK_35_FIELDS, blocks: BLOCK_35_BLOCKS },
];

static BLOCK_37_FIELDS: &[SchemaField] = &[
    SchemaField { name: "scheduler_address", flag: "querier.scheduler-address", kind: FieldKind::String, default_str: "", usage: "Address of the query-scheduler component, in host:port format. The host should resolve to all query-scheduler instances. This option should be set only when query-scheduler component is in use and -query-scheduler.service-discovery-mode is set to 'dns'." },
    SchemaField { name: "dns_lookup_duration", flag: "querier.dns-lookup-period", kind: FieldKind::Duration, default_str: "10000000000", usage: "How often to query DNS for query-frontend or query-scheduler address." },
    SchemaField { name: "id", flag: "querier.id", kind: FieldKind::String, default_str: "", usage: "Querier ID, sent to the query-frontend to identify requests from the same querier. Defaults to hostname." },
];

static BLOCK_37_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "grpc_client_config", fields: BLOCK_33_FIELDS, blocks: BLOCK_33_BLOCKS },
    SchemaBlock { name: "query_scheduler_grpc_client_config", fields: BLOCK_36_FIELDS, blocks: BLOCK_36_BLOCKS },
];

static BLOCK_38_FIELDS: &[SchemaField] = &[
    SchemaField { name: "min_period", flag: "query-frontend.grpc-client-config.backoff-min-period", kind: FieldKind::Duration, default_str: "100000000", usage: "Minimum delay when backing off." },
    SchemaField { name: "max_period", flag: "query-frontend.grpc-client-config.backoff-max-period", kind: FieldKind::Duration, default_str: "10000000000", usage: "Maximum delay when backing off." },
    SchemaField { name: "max_retries", flag: "query-frontend.grpc-client-config.backoff-retries", kind: FieldKind::Int, default_str: "10", usage: "Number of times to backoff and retry before failing." },
];

static BLOCK_38_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_39_FIELDS: &[SchemaField] = &[
    SchemaField { name: "label", flag: "query-frontend.grpc-client-config.cluster-validation.label", kind: FieldKind::String, default_str: "", usage: "Primary cluster validation label." },
];

static BLOCK_39_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_40_FIELDS: &[SchemaField] = &[
    SchemaField { name: "max_recv_msg_size", flag: "query-frontend.grpc-client-config.grpc-max-recv-msg-size", kind: FieldKind::Int, default_str: "104857600", usage: "gRPC client max receive message size (bytes)." },
    SchemaField { name: "max_send_msg_size", flag: "query-frontend.grpc-client-config.grpc-max-send-msg-size", kind: FieldKind::Int, default_str: "104857600", usage: "gRPC client max send message size (bytes)." },
    SchemaField { name: "grpc_compression", flag: "query-frontend.grpc-client-config.grpc-compression", kind: FieldKind::String, default_str: "", usage: "Use compression when sending messages. Supported values are: 'gzip', 'snappy', 's2' and '' (disable compression)" },
    SchemaField { name: "rate_limit", flag: "query-frontend.grpc-client-config.grpc-client-rate-limit", kind: FieldKind::Float, default_str: "0", usage: "Rate limit for gRPC client; 0 means disabled." },
    SchemaField { name: "rate_limit_burst", flag: "query-frontend.grpc-client-config.grpc-client-rate-limit-burst", kind: FieldKind::Int, default_str: "0", usage: "Rate limit burst for gRPC client." },
    SchemaField { name: "backoff_on_ratelimits", flag: "query-frontend.grpc-client-config.backoff-on-ratelimits", kind: FieldKind::Bool, default_str: "false", usage: "Enable backoff and retry when we hit rate limits." },
    SchemaField { name: "initial_stream_window_size", flag: "query-frontend.grpc-client-config.initial-stream-window-size", kind: FieldKind::Bytes, default_str: "63KiB1023B", usage: "Initial stream window size. Values less than the default are not supported and are ignored. Setting this to a value other than the default disables the BDP estimator." },
    SchemaField { name: "initial_connection_window_size", flag: "query-frontend.grpc-client-config.initial-connection-window-size", kind: FieldKind::Bytes, default_str: "63KiB1023B", usage: "Initial connection window size. Values less than the default are not supported and are ignored. Setting this to a value other than the default disables the BDP estimator." },
    SchemaField { name: "tls_enabled", flag: "query-frontend.grpc-client-config.tls-enabled", kind: FieldKind::Bool, default_str: "false", usage: "Enable TLS in the gRPC client. This flag needs to be enabled when any other TLS flag is set. If set to false, insecure connection to gRPC server will be used." },
    SchemaField { name: "tls_cert_path", flag: "query-frontend.grpc-client-config.tls-cert-path", kind: FieldKind::String, default_str: "", usage: "Path to the client certificate, which will be used for authenticating with the server. Also requires the key path to be configured." },
    SchemaField { name: "tls_key_path", flag: "query-frontend.grpc-client-config.tls-key-path", kind: FieldKind::String, default_str: "", usage: "Path to the key for the client certificate. Also requires the client certificate to be configured." },
    SchemaField { name: "tls_ca_path", flag: "query-frontend.grpc-client-config.tls-ca-path", kind: FieldKind::String, default_str: "", usage: "Path to the CA certificates to validate server certificate against. If not set, the host's root CA certificates are used." },
    SchemaField { name: "tls_server_name", flag: "query-frontend.grpc-client-config.tls-server-name", kind: FieldKind::String, default_str: "", usage: "Override the expected name on the server certificate." },
    SchemaField { name: "tls_insecure_skip_verify", flag: "query-frontend.grpc-client-config.tls-insecure-skip-verify", kind: FieldKind::Bool, default_str: "false", usage: "Skip validating server certificate." },
    SchemaField { name: "tls_cipher_suites", flag: "query-frontend.grpc-client-config.tls-cipher-suites", kind: FieldKind::String, default_str: "", usage: "Override the default cipher suite list (separated by commas). Allowed values:\n\nSecure Ciphers:\n- TLS_AES_128_GCM_SHA256\n- TLS_AES_256_GCM_SHA384\n- TLS_CHACHA20_POLY1305_SHA256\n- TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA\n- TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA\n- TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256\n- TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256\n- TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256\n- TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256\n\nInsecure Ciphers:\n- TLS_RSA_WITH_RC4_128_SHA\n- TLS_RSA_WITH_3DES_EDE_CBC_SHA\n- TLS_RSA_WITH_AES_128_CBC_SHA\n- TLS_RSA_WITH_AES_256_CBC_SHA\n- TLS_RSA_WITH_AES_128_CBC_SHA256\n- TLS_RSA_WITH_AES_128_GCM_SHA256\n- TLS_RSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_ECDSA_WITH_RC4_128_SHA\n- TLS_ECDHE_RSA_WITH_RC4_128_SHA\n- TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256\n- TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256\n" },
    SchemaField { name: "tls_min_version", flag: "query-frontend.grpc-client-config.tls-min-version", kind: FieldKind::String, default_str: "", usage: "Override the default minimum TLS version. Allowed values: VersionTLS10, VersionTLS11, VersionTLS12, VersionTLS13" },
    SchemaField { name: "connect_timeout", flag: "query-frontend.grpc-client-config.connect-timeout", kind: FieldKind::Duration, default_str: "5000000000", usage: "The maximum amount of time to establish a connection. A value of 0 means default gRPC client connect timeout and backoff." },
    SchemaField { name: "connect_backoff_base_delay", flag: "query-frontend.grpc-client-config.connect-backoff-base-delay", kind: FieldKind::Duration, default_str: "1000000000", usage: "Initial backoff delay after first connection failure. Only relevant if ConnectTimeout > 0." },
    SchemaField { name: "connect_backoff_max_delay", flag: "query-frontend.grpc-client-config.connect-backoff-max-delay", kind: FieldKind::Duration, default_str: "5000000000", usage: "Maximum backoff delay when establishing a connection. Only relevant if ConnectTimeout > 0." },
    SchemaField { name: "keepalive_time", flag: "query-frontend.grpc-client-config.keepalive-time", kind: FieldKind::Duration, default_str: "20000000000", usage: "After a duration of this time if the client doesn't see any activity it pings the server to see if the transport is still alive. This also determines the socket's TCP_USER_TIMEOUT together with keepalive-timeout." },
    SchemaField { name: "keepalive_timeout", flag: "query-frontend.grpc-client-config.keepalive-timeout", kind: FieldKind::Duration, default_str: "10000000000", usage: "After having pinged for keepalive check, the client waits for a duration of this time and if no activity is seen even after that the connection is closed." },
];

static BLOCK_40_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "backoff_config", fields: BLOCK_38_FIELDS, blocks: BLOCK_38_BLOCKS },
    SchemaBlock { name: "cluster_validation", fields: BLOCK_39_FIELDS, blocks: BLOCK_39_BLOCKS },
];

static BLOCK_41_FIELDS: &[SchemaField] = &[
    SchemaField { name: "addresses", flag: "query-frontend.results-cache.memcached.addresses", kind: FieldKind::String, default_str: "", usage: "Comma-separated list of memcached addresses. Each address can be an IP address, hostname, or an entry specified in the DNS Service Discovery format." },
    SchemaField { name: "addresses_lookup_period", flag: "query-frontend.results-cache.memcached.addresses-lookup-period", kind: FieldKind::Duration, default_str: "30000000000", usage: "How often each address is resolved to an IP address or list of host names." },
    SchemaField { name: "addresses_lookup_pool_size", flag: "query-frontend.results-cache.memcached.addresses-lookup-pool-size", kind: FieldKind::Int, default_str: "0", usage: "How many idle connections to the DNS resolver are kept open. 0 disables keeping any idle connections open." },
    SchemaField { name: "timeout", flag: "query-frontend.results-cache.memcached.timeout", kind: FieldKind::Duration, default_str: "200000000", usage: "The socket read/write timeout." },
    SchemaField { name: "connect_timeout", flag: "query-frontend.results-cache.memcached.connect-timeout", kind: FieldKind::Duration, default_str: "200000000", usage: "The connection timeout." },
    SchemaField { name: "min_idle_connections_headroom_percentage", flag: "query-frontend.results-cache.memcached.min-idle-connections-headroom-percentage", kind: FieldKind::Float, default_str: "-1", usage: "The minimum number of idle connections to keep open as a percentage (0-100) of the number of recently used idle connections. If negative, idle connections are kept open indefinitely." },
    SchemaField { name: "max_idle_connections", flag: "query-frontend.results-cache.memcached.max-idle-connections", kind: FieldKind::Int, default_str: "100", usage: "The maximum number of idle connections that will be maintained per address." },
    SchemaField { name: "max_async_concurrency", flag: "query-frontend.results-cache.memcached.max-async-concurrency", kind: FieldKind::Int, default_str: "50", usage: "The maximum number of concurrent asynchronous operations can occur." },
    SchemaField { name: "max_async_buffer_size", flag: "query-frontend.results-cache.memcached.max-async-buffer-size", kind: FieldKind::Int, default_str: "25000", usage: "The maximum number of enqueued asynchronous operations allowed." },
    SchemaField { name: "max_get_multi_concurrency", flag: "query-frontend.results-cache.memcached.max-get-multi-concurrency", kind: FieldKind::Int, default_str: "100", usage: "The maximum number of concurrent connections running get operations. If set to 0, concurrency is unlimited." },
    SchemaField { name: "max_get_multi_batch_size", flag: "query-frontend.results-cache.memcached.max-get-multi-batch-size", kind: FieldKind::Int, default_str: "100", usage: "The maximum number of keys a single underlying get operation should run. If more keys are specified, internally keys are split into multiple batches and fetched concurrently, honoring the max concurrency. If set to 0, the max batch size is unlimited." },
    SchemaField { name: "max_item_size", flag: "query-frontend.results-cache.memcached.max-item-size", kind: FieldKind::Int, default_str: "1048576", usage: "The maximum size of an item stored in memcached, in bytes. Bigger items are not stored. If set to 0, no maximum size is enforced." },
    SchemaField { name: "free_list_shards", flag: "query-frontend.results-cache.memcached.free-list-shards", kind: FieldKind::Int, default_str: "1", usage: "The number of shards to stripe the free list used for connections in the cache client over. Must be a power of 2. Increasing the number of shards reduces lock contention at the cost of higher or wasted memory." },
    SchemaField { name: "tls_enabled", flag: "query-frontend.results-cache.memcached.tls-enabled", kind: FieldKind::Bool, default_str: "false", usage: "Enable connecting to Memcached with TLS." },
    SchemaField { name: "tls_cert_path", flag: "query-frontend.results-cache.memcached.tls-cert-path", kind: FieldKind::String, default_str: "", usage: "Path to the client certificate, which will be used for authenticating with the server. Also requires the key path to be configured." },
    SchemaField { name: "tls_key_path", flag: "query-frontend.results-cache.memcached.tls-key-path", kind: FieldKind::String, default_str: "", usage: "Path to the key for the client certificate. Also requires the client certificate to be configured." },
    SchemaField { name: "tls_ca_path", flag: "query-frontend.results-cache.memcached.tls-ca-path", kind: FieldKind::String, default_str: "", usage: "Path to the CA certificates to validate server certificate against. If not set, the host's root CA certificates are used." },
    SchemaField { name: "tls_server_name", flag: "query-frontend.results-cache.memcached.tls-server-name", kind: FieldKind::String, default_str: "", usage: "Override the expected name on the server certificate." },
    SchemaField { name: "tls_insecure_skip_verify", flag: "query-frontend.results-cache.memcached.tls-insecure-skip-verify", kind: FieldKind::Bool, default_str: "false", usage: "Skip validating server certificate." },
    SchemaField { name: "tls_cipher_suites", flag: "query-frontend.results-cache.memcached.tls-cipher-suites", kind: FieldKind::String, default_str: "", usage: "Override the default cipher suite list (separated by commas). Allowed values:\n\nSecure Ciphers:\n- TLS_AES_128_GCM_SHA256\n- TLS_AES_256_GCM_SHA384\n- TLS_CHACHA20_POLY1305_SHA256\n- TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA\n- TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA\n- TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256\n- TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256\n- TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256\n- TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256\n\nInsecure Ciphers:\n- TLS_RSA_WITH_RC4_128_SHA\n- TLS_RSA_WITH_3DES_EDE_CBC_SHA\n- TLS_RSA_WITH_AES_128_CBC_SHA\n- TLS_RSA_WITH_AES_256_CBC_SHA\n- TLS_RSA_WITH_AES_128_CBC_SHA256\n- TLS_RSA_WITH_AES_128_GCM_SHA256\n- TLS_RSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_ECDSA_WITH_RC4_128_SHA\n- TLS_ECDHE_RSA_WITH_RC4_128_SHA\n- TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256\n- TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256\n" },
    SchemaField { name: "tls_min_version", flag: "query-frontend.results-cache.memcached.tls-min-version", kind: FieldKind::String, default_str: "", usage: "Override the default minimum TLS version. Allowed values: VersionTLS10, VersionTLS11, VersionTLS12, VersionTLS13" },
];

static BLOCK_41_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_42_FIELDS: &[SchemaField] = &[
    SchemaField { name: "backend", flag: "query-frontend.results-cache.backend", kind: FieldKind::String, default_str: "", usage: "Backend for query-frontend results cache, if not empty. Supported values: memcached." },
    SchemaField { name: "compression", flag: "query-frontend.results-cache.compression", kind: FieldKind::String, default_str: "", usage: "Enable cache compression, if not empty. Supported values are: snappy." },
];

static BLOCK_42_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "memcached", fields: BLOCK_41_FIELDS, blocks: BLOCK_41_BLOCKS },
];

static BLOCK_43_FIELDS: &[SchemaField] = &[
    SchemaField { name: "addresses", flag: "query-frontend.cardinality-estimation-cache.memcached.addresses", kind: FieldKind::String, default_str: "", usage: "Comma-separated list of memcached addresses. Each address can be an IP address, hostname, or an entry specified in the DNS Service Discovery format." },
    SchemaField { name: "addresses_lookup_period", flag: "query-frontend.cardinality-estimation-cache.memcached.addresses-lookup-period", kind: FieldKind::Duration, default_str: "30000000000", usage: "How often each address is resolved to an IP address or list of host names." },
    SchemaField { name: "addresses_lookup_pool_size", flag: "query-frontend.cardinality-estimation-cache.memcached.addresses-lookup-pool-size", kind: FieldKind::Int, default_str: "0", usage: "How many idle connections to the DNS resolver are kept open. 0 disables keeping any idle connections open." },
    SchemaField { name: "timeout", flag: "query-frontend.cardinality-estimation-cache.memcached.timeout", kind: FieldKind::Duration, default_str: "200000000", usage: "The socket read/write timeout." },
    SchemaField { name: "connect_timeout", flag: "query-frontend.cardinality-estimation-cache.memcached.connect-timeout", kind: FieldKind::Duration, default_str: "200000000", usage: "The connection timeout." },
    SchemaField { name: "min_idle_connections_headroom_percentage", flag: "query-frontend.cardinality-estimation-cache.memcached.min-idle-connections-headroom-percentage", kind: FieldKind::Float, default_str: "-1", usage: "The minimum number of idle connections to keep open as a percentage (0-100) of the number of recently used idle connections. If negative, idle connections are kept open indefinitely." },
    SchemaField { name: "max_idle_connections", flag: "query-frontend.cardinality-estimation-cache.memcached.max-idle-connections", kind: FieldKind::Int, default_str: "100", usage: "The maximum number of idle connections that will be maintained per address." },
    SchemaField { name: "max_async_concurrency", flag: "query-frontend.cardinality-estimation-cache.memcached.max-async-concurrency", kind: FieldKind::Int, default_str: "50", usage: "The maximum number of concurrent asynchronous operations can occur." },
    SchemaField { name: "max_async_buffer_size", flag: "query-frontend.cardinality-estimation-cache.memcached.max-async-buffer-size", kind: FieldKind::Int, default_str: "25000", usage: "The maximum number of enqueued asynchronous operations allowed." },
    SchemaField { name: "max_get_multi_concurrency", flag: "query-frontend.cardinality-estimation-cache.memcached.max-get-multi-concurrency", kind: FieldKind::Int, default_str: "100", usage: "The maximum number of concurrent connections running get operations. If set to 0, concurrency is unlimited." },
    SchemaField { name: "max_get_multi_batch_size", flag: "query-frontend.cardinality-estimation-cache.memcached.max-get-multi-batch-size", kind: FieldKind::Int, default_str: "100", usage: "The maximum number of keys a single underlying get operation should run. If more keys are specified, internally keys are split into multiple batches and fetched concurrently, honoring the max concurrency. If set to 0, the max batch size is unlimited." },
    SchemaField { name: "max_item_size", flag: "query-frontend.cardinality-estimation-cache.memcached.max-item-size", kind: FieldKind::Int, default_str: "1048576", usage: "The maximum size of an item stored in memcached, in bytes. Bigger items are not stored. If set to 0, no maximum size is enforced." },
    SchemaField { name: "free_list_shards", flag: "query-frontend.cardinality-estimation-cache.memcached.free-list-shards", kind: FieldKind::Int, default_str: "1", usage: "The number of shards to stripe the free list used for connections in the cache client over. Must be a power of 2. Increasing the number of shards reduces lock contention at the cost of higher or wasted memory." },
    SchemaField { name: "tls_enabled", flag: "query-frontend.cardinality-estimation-cache.memcached.tls-enabled", kind: FieldKind::Bool, default_str: "false", usage: "Enable connecting to Memcached with TLS." },
    SchemaField { name: "tls_cert_path", flag: "query-frontend.cardinality-estimation-cache.memcached.tls-cert-path", kind: FieldKind::String, default_str: "", usage: "Path to the client certificate, which will be used for authenticating with the server. Also requires the key path to be configured." },
    SchemaField { name: "tls_key_path", flag: "query-frontend.cardinality-estimation-cache.memcached.tls-key-path", kind: FieldKind::String, default_str: "", usage: "Path to the key for the client certificate. Also requires the client certificate to be configured." },
    SchemaField { name: "tls_ca_path", flag: "query-frontend.cardinality-estimation-cache.memcached.tls-ca-path", kind: FieldKind::String, default_str: "", usage: "Path to the CA certificates to validate server certificate against. If not set, the host's root CA certificates are used." },
    SchemaField { name: "tls_server_name", flag: "query-frontend.cardinality-estimation-cache.memcached.tls-server-name", kind: FieldKind::String, default_str: "", usage: "Override the expected name on the server certificate." },
    SchemaField { name: "tls_insecure_skip_verify", flag: "query-frontend.cardinality-estimation-cache.memcached.tls-insecure-skip-verify", kind: FieldKind::Bool, default_str: "false", usage: "Skip validating server certificate." },
    SchemaField { name: "tls_cipher_suites", flag: "query-frontend.cardinality-estimation-cache.memcached.tls-cipher-suites", kind: FieldKind::String, default_str: "", usage: "Override the default cipher suite list (separated by commas). Allowed values:\n\nSecure Ciphers:\n- TLS_AES_128_GCM_SHA256\n- TLS_AES_256_GCM_SHA384\n- TLS_CHACHA20_POLY1305_SHA256\n- TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA\n- TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA\n- TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256\n- TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256\n- TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256\n- TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256\n\nInsecure Ciphers:\n- TLS_RSA_WITH_RC4_128_SHA\n- TLS_RSA_WITH_3DES_EDE_CBC_SHA\n- TLS_RSA_WITH_AES_128_CBC_SHA\n- TLS_RSA_WITH_AES_256_CBC_SHA\n- TLS_RSA_WITH_AES_128_CBC_SHA256\n- TLS_RSA_WITH_AES_128_GCM_SHA256\n- TLS_RSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_ECDSA_WITH_RC4_128_SHA\n- TLS_ECDHE_RSA_WITH_RC4_128_SHA\n- TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256\n- TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256\n" },
    SchemaField { name: "tls_min_version", flag: "query-frontend.cardinality-estimation-cache.memcached.tls-min-version", kind: FieldKind::String, default_str: "", usage: "Override the default minimum TLS version. Allowed values: VersionTLS10, VersionTLS11, VersionTLS12, VersionTLS13" },
];

static BLOCK_43_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_44_FIELDS: &[SchemaField] = &[
    SchemaField { name: "backend", flag: "query-frontend.cardinality-estimation-cache.backend", kind: FieldKind::String, default_str: "", usage: "Backend for the query-frontend cardinality estimation cache, if not empty. If empty, cardinality estimates are stored in the results cache. Supported values: memcached." },
];

static BLOCK_44_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "memcached", fields: BLOCK_43_FIELDS, blocks: BLOCK_43_BLOCKS },
];

static BLOCK_45_FIELDS: &[SchemaField] = &[
    SchemaField { name: "label", flag: "query-frontend.client-cluster-validation.label", kind: FieldKind::String, default_str: "", usage: "Primary cluster validation label." },
];

static BLOCK_45_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_46_FIELDS: &[SchemaField] = &[
    SchemaField { name: "log_queries_longer_than", flag: "query-frontend.log-queries-longer-than", kind: FieldKind::Duration, default_str: "0", usage: "(use query-frontend.query-stats-enabled instead) Log queries that are slower than the specified duration. Set to 0 to disable. Set to < 0 to enable on all queries." },
    SchemaField { name: "log_query_request_headers", flag: "query-frontend.log-query-request-headers", kind: FieldKind::String, default_str: "", usage: "Comma-separated list of request header names to include in query logs. Applies to both query stats and slow queries logs." },
    SchemaField { name: "max_body_size", flag: "query-frontend.max-body-size", kind: FieldKind::Int, default_str: "10485760", usage: "Max body size for downstream prometheus." },
    SchemaField { name: "query_stats_enabled", flag: "query-frontend.query-stats-enabled", kind: FieldKind::Bool, default_str: "true", usage: "False to disable query statistics tracking. When enabled, a message with some statistics is logged for every query." },
    SchemaField { name: "active_series_write_timeout", flag: "query-frontend.active-series-write-timeout", kind: FieldKind::Duration, default_str: "300000000000", usage: "Timeout for writing active series responses. 0 means the value from `-server.http-write-timeout` is used." },
    SchemaField { name: "scheduler_address", flag: "query-frontend.scheduler-address", kind: FieldKind::String, default_str: "", usage: "Address of the query-scheduler component, in host:port format. The host should resolve to all query-scheduler instances. This option should be set only when query-scheduler component is in use and -query-scheduler.service-discovery-mode is set to 'dns'." },
    SchemaField { name: "scheduler_dns_lookup_period", flag: "query-frontend.scheduler-dns-lookup-period", kind: FieldKind::Duration, default_str: "10000000000", usage: "How often to resolve the scheduler-address, in order to look for new query-scheduler instances." },
    SchemaField { name: "scheduler_worker_concurrency", flag: "query-frontend.scheduler-worker-concurrency", kind: FieldKind::Int, default_str: "5", usage: "Number of concurrent workers forwarding queries to single query-scheduler." },
    SchemaField { name: "instance_interface_names", flag: "query-frontend.instance-interface-names", kind: FieldKind::StringList, default_str: "", usage: "List of network interface names to look up when finding the instance IP address. This address is sent to query-scheduler and querier, which uses it to send the query response back to query-frontend." },
    SchemaField { name: "instance_enable_ipv6", flag: "query-frontend.instance-enable-ipv6", kind: FieldKind::Bool, default_str: "false", usage: "Enable using a IPv6 instance address (default false)." },
    SchemaField { name: "address", flag: "query-frontend.instance-addr", kind: FieldKind::String, default_str: "", usage: "IP address to advertise to the querier (via scheduler) (default is auto-detected from network interfaces)." },
    SchemaField { name: "port", flag: "query-frontend.instance-port", kind: FieldKind::Int, default_str: "0", usage: "Port to advertise to querier (via scheduler) (defaults to server.grpc-listen-port)." },
    SchemaField { name: "remote_execution_batch_size", flag: "query-frontend.remote-execution-batch-size", kind: FieldKind::Int, default_str: "128", usage: "Maximum number of series to send in a single remote execution response from a querier." },
    SchemaField { name: "remote_execution_series_metadata_batch_size", flag: "query-frontend.remote-execution-series-metadata-batch-size", kind: FieldKind::Int, default_str: "128", usage: "Maximum number of series metadata entries to send in a single remote execution response from a querier." },
    SchemaField { name: "queue_reservation_enabled", flag: "query-frontend.queue-reservation-enabled", kind: FieldKind::Bool, default_str: "false", usage: "Reserve query-scheduler queue slots for the remote execution sub-requests of a query evaluation, before the query-frontend sends them. A reservation holds the smaller of -querier.max-query-parallelism and the number of sub-requests. Requires -query-frontend.enable-remote-execution and -query-frontend.use-mimir-query-engine-for-splitting-and-caching-results, and -query-frontend.use-mimir-query-engine-for-sharding when sharding is enabled. A sub-request that waits for -querier.max-query-parallelism can fail the query when the query-frontend reads the response of another sub-request for longer than -query-scheduler.queue-reservation-timeout. Set the timeout longer than the longest such read. Only query-schedulers with -query-scheduler.queue-reservation-enabled receive reservations." },
    SchemaField { name: "split_queries_by_interval", flag: "query-frontend.split-queries-by-interval", kind: FieldKind::Duration, default_str: "86400000000000", usage: "Split range queries by an interval and execute in parallel. You should use a multiple of 24 hours to optimize querying blocks. 0 to disable it." },
    SchemaField { name: "cache_results", flag: "query-frontend.cache-results", kind: FieldKind::Bool, default_str: "false", usage: "Cache query results." },
    SchemaField { name: "use_mimir_query_engine_for_splitting_and_caching_results", flag: "query-frontend.use-mimir-query-engine-for-splitting-and-caching-results", kind: FieldKind::Bool, default_str: "true", usage: "Set to true to enable performing splitting range queries by interval and caching inside the Mimir query engine (MQE), and spinning off subqueries from instant queries inside MQE. This only has an effect if the corresponding feature is enabled (with -query-frontend.split-queries-by-interval=true, -query-frontend.cache-results=true or -query-frontend.subquery-spin-off-enabled=true, respectively). Requires MQE, remote execution and sharding inside MQE to be enabled." },
    SchemaField { name: "cache_errors", flag: "query-frontend.cache-errors", kind: FieldKind::Bool, default_str: "false", usage: "Cache non-transient errors from queries." },
    SchemaField { name: "max_retries", flag: "query-frontend.max-retries-per-request", kind: FieldKind::Int, default_str: "5", usage: "Maximum number of retries for a single request; beyond this, the downstream error is returned." },
    SchemaField { name: "not_running_timeout", flag: "query-frontend.not-running-timeout", kind: FieldKind::Duration, default_str: "2000000000", usage: "Maximum time to wait for the query-frontend to become ready before rejecting requests received before the frontend was ready. 0 to disable (i.e. fail immediately if a request is received while the frontend is still starting up)" },
    SchemaField { name: "parallelize_shardable_queries", flag: "query-frontend.parallelize-shardable-queries", kind: FieldKind::Bool, default_str: "true", usage: "True to enable query sharding." },
    SchemaField { name: "enable_remote_execution", flag: "query-frontend.enable-remote-execution", kind: FieldKind::Bool, default_str: "true", usage: "If set to true and the Mimir query engine is in use, use remote execution to evaluate queries in queriers." },
    SchemaField { name: "use_mimir_query_engine_for_sharding", flag: "query-frontend.use-mimir-query-engine-for-sharding", kind: FieldKind::Bool, default_str: "true", usage: "Set to true to enable performing query sharding inside the Mimir query engine (MQE). Requires remote execution and MQE to be enabled. Has no effect if sharding is not enabled with -query-frontend.parallelize-shardable-queries=true" },
    SchemaField { name: "rewrite_histogram_queries", flag: "query-frontend.rewrite-histogram-queries", kind: FieldKind::Bool, default_str: "false", usage: "Set to true to enable rewriting histogram queries for a more efficient order of execution." },
    SchemaField { name: "query_sharding_target_series_per_shard", flag: "query-frontend.query-sharding-target-series-per-shard", kind: FieldKind::Int, default_str: "0", usage: "How many series a single sharded partial query should load at most. This is not a strict requirement guaranteed to be honoured by query sharding, but a hint given to the query sharding when the query execution is initially planned. 0 to disable cardinality-based hints." },
    SchemaField { name: "active_series_max_shard_concurrency", flag: "query-frontend.active-series-max-shard-concurrency", kind: FieldKind::Int, default_str: "0", usage: "Maximum number of sharded active series (and active native histogram metrics) sub-requests dispatched and merged concurrently within a single request. This bounds the resource usage caused by fanning out to a large number of shards, both on queriers and on the query-frontend. 0 to disable the limit." },
    SchemaField { name: "active_series_framed_responses", flag: "query-frontend.active-series-framed-responses", kind: FieldKind::Bool, default_str: "false", usage: "Request active series responses from queriers in a length-delimited framed format that the query-frontend can merge using significantly less CPU. Queriers that don't support the format fall back to JSON transparently." },
    SchemaField { name: "subquery_spin_off_simple_subqueries", flag: "query-frontend.subquery-spin-off-simple-subqueries", kind: FieldKind::Bool, default_str: "false", usage: "Set to true to spin off subqueries whose inner expression is considered simple. Has no effect unless subquery spin-off is enabled with -query-frontend.subquery-spin-off-enabled=true." },
    SchemaField { name: "subquery_spin_off_with_excess_downstream_queries", flag: "query-frontend.subquery-spin-off-with-excess-downstream-queries", kind: FieldKind::Bool, default_str: "false", usage: "Set to true to spin off subqueries even when the query contains more downstream queries than spun-off subqueries. Has no effect unless subquery spin-off is enabled with -query-frontend.subquery-spin-off-enabled=true." },
    SchemaField { name: "extra_propagated_headers", flag: "query-frontend.extra-propagated-headers", kind: FieldKind::String, default_str: "", usage: "Comma-separated list of request header names to allow to pass through to the rest of the query path. This is in addition to a list of required headers that the read path needs." },
    SchemaField { name: "query_result_response_format", flag: "query-frontend.query-result-response-format", kind: FieldKind::String, default_str: "protobuf", usage: "Format to use when retrieving query results from queriers. Supported values: json, protobuf" },
    SchemaField { name: "query_engine", flag: "query-frontend.query-engine", kind: FieldKind::String, default_str: "mimir", usage: "Query engine to use, either 'prometheus' or 'mimir'" },
    SchemaField { name: "enable_query_engine_fallback", flag: "query-frontend.enable-query-engine-fallback", kind: FieldKind::Bool, default_str: "true", usage: "If set to true and the Mimir query engine is in use, fall back to using the Prometheus query engine for any queries not supported by the Mimir query engine." },
    SchemaField { name: "wait_for_querier_ring_on_startup", flag: "query-frontend.wait-for-querier-ring-on-startup", kind: FieldKind::Bool, default_str: "true", usage: "If set to true and remote execution is enabled, don't report the query-frontend as ready during startup until it has seen at least one querier in the querier ring, or 30s elapses. Queries fail while the ring is empty, so starting to serve before then means failing queries." },
    SchemaField { name: "max_inflight_metrics_enabled", flag: "query-frontend.max-inflight-metrics-enabled", kind: FieldKind::Bool, default_str: "false", usage: "Enable the cortex_query_frontend_max_inflight_requests and cortex_query_frontend_max_inflight_request_age_seconds metrics, which report the per-tenant peak number of concurrent in-flight requests and the greatest age an in-flight request reached since the last scrape. Disabling it skips per-tenant in-flight tracking on every request." },
];

static BLOCK_46_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "grpc_client_config", fields: BLOCK_40_FIELDS, blocks: BLOCK_40_BLOCKS },
    SchemaBlock { name: "results_cache", fields: BLOCK_42_FIELDS, blocks: BLOCK_42_BLOCKS },
    SchemaBlock { name: "cardinality_estimation_cache", fields: BLOCK_44_FIELDS, blocks: BLOCK_44_BLOCKS },
    SchemaBlock { name: "client_cluster_validation", fields: BLOCK_45_FIELDS, blocks: BLOCK_45_BLOCKS },
];

static BLOCK_47_FIELDS: &[SchemaField] = &[
    SchemaField { name: "host", flag: "compactor.ring.consul.hostname", kind: FieldKind::String, default_str: "localhost:8500", usage: "Hostname and port of Consul." },
    SchemaField { name: "acl_token", flag: "compactor.ring.consul.acl-token", kind: FieldKind::Secret, default_str: "", usage: "ACL Token used to interact with Consul." },
    SchemaField { name: "http_client_timeout", flag: "compactor.ring.consul.client-timeout", kind: FieldKind::Duration, default_str: "20000000000", usage: "HTTP timeout when talking to Consul" },
    SchemaField { name: "consistent_reads", flag: "compactor.ring.consul.consistent-reads", kind: FieldKind::Bool, default_str: "false", usage: "Enable consistent reads to Consul." },
    SchemaField { name: "watch_rate_limit", flag: "compactor.ring.consul.watch-rate-limit", kind: FieldKind::Float, default_str: "1", usage: "Rate limit when watching key or prefix in Consul, in requests per second. 0 disables the rate limit." },
    SchemaField { name: "watch_burst_size", flag: "compactor.ring.consul.watch-burst-size", kind: FieldKind::Int, default_str: "1", usage: "Burst size used in rate limit. Values less than 1 are treated as 1." },
    SchemaField { name: "cas_retry_delay", flag: "compactor.ring.consul.cas-retry-delay", kind: FieldKind::Duration, default_str: "1000000000", usage: "Maximum duration to wait before retrying a Compare And Swap (CAS) operation." },
];

static BLOCK_47_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_48_FIELDS: &[SchemaField] = &[
    SchemaField { name: "endpoints", flag: "compactor.ring.etcd.endpoints", kind: FieldKind::StringList, default_str: "", usage: "The etcd endpoints to connect to." },
    SchemaField { name: "dial_timeout", flag: "compactor.ring.etcd.dial-timeout", kind: FieldKind::Duration, default_str: "10000000000", usage: "The dial timeout for the etcd connection." },
    SchemaField { name: "max_retries", flag: "compactor.ring.etcd.max-retries", kind: FieldKind::Int, default_str: "10", usage: "The maximum number of retries to do for failed ops." },
    SchemaField { name: "tls_enabled", flag: "compactor.ring.etcd.tls-enabled", kind: FieldKind::Bool, default_str: "false", usage: "Enable TLS." },
    SchemaField { name: "tls_cert_path", flag: "compactor.ring.etcd.tls-cert-path", kind: FieldKind::String, default_str: "", usage: "Path to the client certificate, which will be used for authenticating with the server. Also requires the key path to be configured." },
    SchemaField { name: "tls_key_path", flag: "compactor.ring.etcd.tls-key-path", kind: FieldKind::String, default_str: "", usage: "Path to the key for the client certificate. Also requires the client certificate to be configured." },
    SchemaField { name: "tls_ca_path", flag: "compactor.ring.etcd.tls-ca-path", kind: FieldKind::String, default_str: "", usage: "Path to the CA certificates to validate server certificate against. If not set, the host's root CA certificates are used." },
    SchemaField { name: "tls_server_name", flag: "compactor.ring.etcd.tls-server-name", kind: FieldKind::String, default_str: "", usage: "Override the expected name on the server certificate." },
    SchemaField { name: "tls_insecure_skip_verify", flag: "compactor.ring.etcd.tls-insecure-skip-verify", kind: FieldKind::Bool, default_str: "false", usage: "Skip validating server certificate." },
    SchemaField { name: "tls_cipher_suites", flag: "compactor.ring.etcd.tls-cipher-suites", kind: FieldKind::String, default_str: "", usage: "Override the default cipher suite list (separated by commas). Allowed values:\n\nSecure Ciphers:\n- TLS_AES_128_GCM_SHA256\n- TLS_AES_256_GCM_SHA384\n- TLS_CHACHA20_POLY1305_SHA256\n- TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA\n- TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA\n- TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256\n- TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256\n- TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256\n- TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256\n\nInsecure Ciphers:\n- TLS_RSA_WITH_RC4_128_SHA\n- TLS_RSA_WITH_3DES_EDE_CBC_SHA\n- TLS_RSA_WITH_AES_128_CBC_SHA\n- TLS_RSA_WITH_AES_256_CBC_SHA\n- TLS_RSA_WITH_AES_128_CBC_SHA256\n- TLS_RSA_WITH_AES_128_GCM_SHA256\n- TLS_RSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_ECDSA_WITH_RC4_128_SHA\n- TLS_ECDHE_RSA_WITH_RC4_128_SHA\n- TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256\n- TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256\n" },
    SchemaField { name: "tls_min_version", flag: "compactor.ring.etcd.tls-min-version", kind: FieldKind::String, default_str: "", usage: "Override the default minimum TLS version. Allowed values: VersionTLS10, VersionTLS11, VersionTLS12, VersionTLS13" },
    SchemaField { name: "username", flag: "compactor.ring.etcd.username", kind: FieldKind::String, default_str: "", usage: "Etcd username." },
    SchemaField { name: "password", flag: "compactor.ring.etcd.password", kind: FieldKind::Secret, default_str: "", usage: "Etcd password." },
];

static BLOCK_48_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_49_FIELDS: &[SchemaField] = &[
    SchemaField { name: "primary", flag: "compactor.ring.multi.primary", kind: FieldKind::String, default_str: "", usage: "Primary backend storage used by multi-client." },
    SchemaField { name: "secondary", flag: "compactor.ring.multi.secondary", kind: FieldKind::String, default_str: "", usage: "Secondary backend storage used by multi-client." },
    SchemaField { name: "mirror_enabled", flag: "compactor.ring.multi.mirror-enabled", kind: FieldKind::Bool, default_str: "false", usage: "Mirror writes to the secondary store." },
    SchemaField { name: "mirror_timeout", flag: "compactor.ring.multi.mirror-timeout", kind: FieldKind::Duration, default_str: "2000000000", usage: "Timeout for storing a value to the secondary store." },
];

static BLOCK_49_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_50_FIELDS: &[SchemaField] = &[
    SchemaField { name: "store", flag: "compactor.ring.store", kind: FieldKind::String, default_str: "memberlist", usage: "Backend storage to use for the ring. Supported values are: consul, etcd, inmemory, memberlist, multi." },
    SchemaField { name: "prefix", flag: "compactor.ring.prefix", kind: FieldKind::String, default_str: "collectors/", usage: "The prefix for the keys in the store. Should end with a /." },
];

static BLOCK_50_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "consul", fields: BLOCK_47_FIELDS, blocks: BLOCK_47_BLOCKS },
    SchemaBlock { name: "etcd", fields: BLOCK_48_FIELDS, blocks: BLOCK_48_BLOCKS },
    SchemaBlock { name: "multi", fields: BLOCK_49_FIELDS, blocks: BLOCK_49_BLOCKS },
];

static BLOCK_51_FIELDS: &[SchemaField] = &[
    SchemaField { name: "heartbeat_period", flag: "compactor.ring.heartbeat-period", kind: FieldKind::Duration, default_str: "15000000000", usage: "Period at which to heartbeat to the ring." },
    SchemaField { name: "heartbeat_timeout", flag: "compactor.ring.heartbeat-timeout", kind: FieldKind::Duration, default_str: "60000000000", usage: "Heartbeat timeout after which Mimir marks compactors as unhealthy in the ring." },
    SchemaField { name: "instance_id", flag: "compactor.ring.instance-id", kind: FieldKind::String, default_str: "<hostname>", usage: "Instance ID to register in the ring." },
    SchemaField { name: "instance_interface_names", flag: "compactor.ring.instance-interface-names", kind: FieldKind::StringList, default_str: "", usage: "List of network interface names to look up when finding the instance IP address." },
    SchemaField { name: "instance_port", flag: "compactor.ring.instance-port", kind: FieldKind::Int, default_str: "0", usage: "Port to advertise in the ring (defaults to -server.grpc-listen-port)." },
    SchemaField { name: "instance_addr", flag: "compactor.ring.instance-addr", kind: FieldKind::String, default_str: "", usage: "IP address to advertise in the ring. Default is auto-detected." },
    SchemaField { name: "instance_enable_ipv6", flag: "compactor.ring.instance-enable-ipv6", kind: FieldKind::Bool, default_str: "false", usage: "Enable using an IPv6 instance address." },
    SchemaField { name: "wait_stability_min_duration", flag: "compactor.ring.wait-stability-min-duration", kind: FieldKind::Duration, default_str: "0", usage: "Minimum time to wait for ring stability at startup. 0 to disable." },
    SchemaField { name: "wait_stability_max_duration", flag: "compactor.ring.wait-stability-max-duration", kind: FieldKind::Duration, default_str: "300000000000", usage: "Maximum time to wait for ring stability at startup. If the compactor ring keeps changing after this period of time, the compactor will start anyway." },
    SchemaField { name: "wait_active_instance_timeout", flag: "compactor.ring.wait-active-instance-timeout", kind: FieldKind::Duration, default_str: "600000000000", usage: "Timeout for waiting on compactor to become ACTIVE in the ring." },
    SchemaField { name: "auto_forget_unhealthy_periods", flag: "compactor.ring.auto-forget-unhealthy-periods", kind: FieldKind::Int, default_str: "10", usage: "Number of consecutive timeout periods after which Mimir automatically removes an unhealthy instance in the ring. Set to 0 to disable auto-forget." },
];

static BLOCK_51_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "kvstore", fields: BLOCK_50_FIELDS, blocks: BLOCK_50_BLOCKS },
];

static BLOCK_52_FIELDS: &[SchemaField] = &[
    SchemaField { name: "min_period", flag: "compactor.scheduler-client.grpc-client-config.backoff-min-period", kind: FieldKind::Duration, default_str: "100000000", usage: "Minimum delay when backing off." },
    SchemaField { name: "max_period", flag: "compactor.scheduler-client.grpc-client-config.backoff-max-period", kind: FieldKind::Duration, default_str: "10000000000", usage: "Maximum delay when backing off." },
    SchemaField { name: "max_retries", flag: "compactor.scheduler-client.grpc-client-config.backoff-retries", kind: FieldKind::Int, default_str: "10", usage: "Number of times to backoff and retry before failing." },
];

static BLOCK_52_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_53_FIELDS: &[SchemaField] = &[
    SchemaField { name: "label", flag: "compactor.scheduler-client.grpc-client-config.cluster-validation.label", kind: FieldKind::String, default_str: "", usage: "Primary cluster validation label." },
];

static BLOCK_53_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_54_FIELDS: &[SchemaField] = &[
    SchemaField { name: "max_recv_msg_size", flag: "compactor.scheduler-client.grpc-client-config.grpc-max-recv-msg-size", kind: FieldKind::Int, default_str: "104857600", usage: "gRPC client max receive message size (bytes)." },
    SchemaField { name: "max_send_msg_size", flag: "compactor.scheduler-client.grpc-client-config.grpc-max-send-msg-size", kind: FieldKind::Int, default_str: "104857600", usage: "gRPC client max send message size (bytes)." },
    SchemaField { name: "grpc_compression", flag: "compactor.scheduler-client.grpc-client-config.grpc-compression", kind: FieldKind::String, default_str: "", usage: "Use compression when sending messages. Supported values are: 'gzip', 'snappy' and '' (disable compression)" },
    SchemaField { name: "rate_limit", flag: "compactor.scheduler-client.grpc-client-config.grpc-client-rate-limit", kind: FieldKind::Float, default_str: "0", usage: "Rate limit for gRPC client; 0 means disabled." },
    SchemaField { name: "rate_limit_burst", flag: "compactor.scheduler-client.grpc-client-config.grpc-client-rate-limit-burst", kind: FieldKind::Int, default_str: "0", usage: "Rate limit burst for gRPC client." },
    SchemaField { name: "backoff_on_ratelimits", flag: "compactor.scheduler-client.grpc-client-config.backoff-on-ratelimits", kind: FieldKind::Bool, default_str: "false", usage: "Enable backoff and retry when we hit rate limits." },
    SchemaField { name: "initial_stream_window_size", flag: "compactor.scheduler-client.grpc-client-config.initial-stream-window-size", kind: FieldKind::Bytes, default_str: "63KiB1023B", usage: "Initial stream window size. Values less than the default are not supported and are ignored. Setting this to a value other than the default disables the BDP estimator." },
    SchemaField { name: "initial_connection_window_size", flag: "compactor.scheduler-client.grpc-client-config.initial-connection-window-size", kind: FieldKind::Bytes, default_str: "63KiB1023B", usage: "Initial connection window size. Values less than the default are not supported and are ignored. Setting this to a value other than the default disables the BDP estimator." },
    SchemaField { name: "tls_enabled", flag: "compactor.scheduler-client.grpc-client-config.tls-enabled", kind: FieldKind::Bool, default_str: "false", usage: "Enable TLS in the gRPC client. This flag needs to be enabled when any other TLS flag is set. If set to false, insecure connection to gRPC server will be used." },
    SchemaField { name: "tls_cert_path", flag: "compactor.scheduler-client.grpc-client-config.tls-cert-path", kind: FieldKind::String, default_str: "", usage: "Path to the client certificate, which will be used for authenticating with the server. Also requires the key path to be configured." },
    SchemaField { name: "tls_key_path", flag: "compactor.scheduler-client.grpc-client-config.tls-key-path", kind: FieldKind::String, default_str: "", usage: "Path to the key for the client certificate. Also requires the client certificate to be configured." },
    SchemaField { name: "tls_ca_path", flag: "compactor.scheduler-client.grpc-client-config.tls-ca-path", kind: FieldKind::String, default_str: "", usage: "Path to the CA certificates to validate server certificate against. If not set, the host's root CA certificates are used." },
    SchemaField { name: "tls_server_name", flag: "compactor.scheduler-client.grpc-client-config.tls-server-name", kind: FieldKind::String, default_str: "", usage: "Override the expected name on the server certificate." },
    SchemaField { name: "tls_insecure_skip_verify", flag: "compactor.scheduler-client.grpc-client-config.tls-insecure-skip-verify", kind: FieldKind::Bool, default_str: "false", usage: "Skip validating server certificate." },
    SchemaField { name: "tls_cipher_suites", flag: "compactor.scheduler-client.grpc-client-config.tls-cipher-suites", kind: FieldKind::String, default_str: "", usage: "Override the default cipher suite list (separated by commas). Allowed values:\n\nSecure Ciphers:\n- TLS_AES_128_GCM_SHA256\n- TLS_AES_256_GCM_SHA384\n- TLS_CHACHA20_POLY1305_SHA256\n- TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA\n- TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA\n- TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256\n- TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256\n- TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256\n- TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256\n\nInsecure Ciphers:\n- TLS_RSA_WITH_RC4_128_SHA\n- TLS_RSA_WITH_3DES_EDE_CBC_SHA\n- TLS_RSA_WITH_AES_128_CBC_SHA\n- TLS_RSA_WITH_AES_256_CBC_SHA\n- TLS_RSA_WITH_AES_128_CBC_SHA256\n- TLS_RSA_WITH_AES_128_GCM_SHA256\n- TLS_RSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_ECDSA_WITH_RC4_128_SHA\n- TLS_ECDHE_RSA_WITH_RC4_128_SHA\n- TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256\n- TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256\n" },
    SchemaField { name: "tls_min_version", flag: "compactor.scheduler-client.grpc-client-config.tls-min-version", kind: FieldKind::String, default_str: "", usage: "Override the default minimum TLS version. Allowed values: VersionTLS10, VersionTLS11, VersionTLS12, VersionTLS13" },
    SchemaField { name: "connect_timeout", flag: "compactor.scheduler-client.grpc-client-config.connect-timeout", kind: FieldKind::Duration, default_str: "5000000000", usage: "The maximum amount of time to establish a connection. A value of 0 means default gRPC client connect timeout and backoff." },
    SchemaField { name: "connect_backoff_base_delay", flag: "compactor.scheduler-client.grpc-client-config.connect-backoff-base-delay", kind: FieldKind::Duration, default_str: "1000000000", usage: "Initial backoff delay after first connection failure. Only relevant if ConnectTimeout > 0." },
    SchemaField { name: "connect_backoff_max_delay", flag: "compactor.scheduler-client.grpc-client-config.connect-backoff-max-delay", kind: FieldKind::Duration, default_str: "5000000000", usage: "Maximum backoff delay when establishing a connection. Only relevant if ConnectTimeout > 0." },
    SchemaField { name: "keepalive_time", flag: "compactor.scheduler-client.grpc-client-config.keepalive-time", kind: FieldKind::Duration, default_str: "20000000000", usage: "After a duration of this time if the client doesn't see any activity it pings the server to see if the transport is still alive. This also determines the socket's TCP_USER_TIMEOUT together with keepalive-timeout." },
    SchemaField { name: "keepalive_timeout", flag: "compactor.scheduler-client.grpc-client-config.keepalive-timeout", kind: FieldKind::Duration, default_str: "10000000000", usage: "After having pinged for keepalive check, the client waits for a duration of this time and if no activity is seen even after that the connection is closed." },
];

static BLOCK_54_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "backoff_config", fields: BLOCK_52_FIELDS, blocks: BLOCK_52_BLOCKS },
    SchemaBlock { name: "cluster_validation", fields: BLOCK_53_FIELDS, blocks: BLOCK_53_BLOCKS },
];

static BLOCK_55_FIELDS: &[SchemaField] = &[
    SchemaField { name: "addresses", flag: "compactor.scheduler-client.metadata-cache.memcached.addresses", kind: FieldKind::String, default_str: "", usage: "Comma-separated list of memcached addresses. Each address can be an IP address, hostname, or an entry specified in the DNS Service Discovery format." },
    SchemaField { name: "addresses_lookup_period", flag: "compactor.scheduler-client.metadata-cache.memcached.addresses-lookup-period", kind: FieldKind::Duration, default_str: "30000000000", usage: "How often each address is resolved to an IP address or list of host names." },
    SchemaField { name: "addresses_lookup_pool_size", flag: "compactor.scheduler-client.metadata-cache.memcached.addresses-lookup-pool-size", kind: FieldKind::Int, default_str: "0", usage: "How many idle connections to the DNS resolver are kept open. 0 disables keeping any idle connections open." },
    SchemaField { name: "timeout", flag: "compactor.scheduler-client.metadata-cache.memcached.timeout", kind: FieldKind::Duration, default_str: "200000000", usage: "The socket read/write timeout." },
    SchemaField { name: "connect_timeout", flag: "compactor.scheduler-client.metadata-cache.memcached.connect-timeout", kind: FieldKind::Duration, default_str: "200000000", usage: "The connection timeout." },
    SchemaField { name: "min_idle_connections_headroom_percentage", flag: "compactor.scheduler-client.metadata-cache.memcached.min-idle-connections-headroom-percentage", kind: FieldKind::Float, default_str: "-1", usage: "The minimum number of idle connections to keep open as a percentage (0-100) of the number of recently used idle connections. If negative, idle connections are kept open indefinitely." },
    SchemaField { name: "max_idle_connections", flag: "compactor.scheduler-client.metadata-cache.memcached.max-idle-connections", kind: FieldKind::Int, default_str: "100", usage: "The maximum number of idle connections that will be maintained per address." },
    SchemaField { name: "max_async_concurrency", flag: "compactor.scheduler-client.metadata-cache.memcached.max-async-concurrency", kind: FieldKind::Int, default_str: "50", usage: "The maximum number of concurrent asynchronous operations can occur." },
    SchemaField { name: "max_async_buffer_size", flag: "compactor.scheduler-client.metadata-cache.memcached.max-async-buffer-size", kind: FieldKind::Int, default_str: "25000", usage: "The maximum number of enqueued asynchronous operations allowed." },
    SchemaField { name: "max_get_multi_concurrency", flag: "compactor.scheduler-client.metadata-cache.memcached.max-get-multi-concurrency", kind: FieldKind::Int, default_str: "100", usage: "The maximum number of concurrent connections running get operations. If set to 0, concurrency is unlimited." },
    SchemaField { name: "max_get_multi_batch_size", flag: "compactor.scheduler-client.metadata-cache.memcached.max-get-multi-batch-size", kind: FieldKind::Int, default_str: "100", usage: "The maximum number of keys a single underlying get operation should run. If more keys are specified, internally keys are split into multiple batches and fetched concurrently, honoring the max concurrency. If set to 0, the max batch size is unlimited." },
    SchemaField { name: "max_item_size", flag: "compactor.scheduler-client.metadata-cache.memcached.max-item-size", kind: FieldKind::Int, default_str: "1048576", usage: "The maximum size of an item stored in memcached, in bytes. Bigger items are not stored. If set to 0, no maximum size is enforced." },
    SchemaField { name: "free_list_shards", flag: "compactor.scheduler-client.metadata-cache.memcached.free-list-shards", kind: FieldKind::Int, default_str: "1", usage: "The number of shards to stripe the free list used for connections in the cache client over. Must be a power of 2. Increasing the number of shards reduces lock contention at the cost of higher or wasted memory." },
    SchemaField { name: "tls_enabled", flag: "compactor.scheduler-client.metadata-cache.memcached.tls-enabled", kind: FieldKind::Bool, default_str: "false", usage: "Enable connecting to Memcached with TLS." },
    SchemaField { name: "tls_cert_path", flag: "compactor.scheduler-client.metadata-cache.memcached.tls-cert-path", kind: FieldKind::String, default_str: "", usage: "Path to the client certificate, which will be used for authenticating with the server. Also requires the key path to be configured." },
    SchemaField { name: "tls_key_path", flag: "compactor.scheduler-client.metadata-cache.memcached.tls-key-path", kind: FieldKind::String, default_str: "", usage: "Path to the key for the client certificate. Also requires the client certificate to be configured." },
    SchemaField { name: "tls_ca_path", flag: "compactor.scheduler-client.metadata-cache.memcached.tls-ca-path", kind: FieldKind::String, default_str: "", usage: "Path to the CA certificates to validate server certificate against. If not set, the host's root CA certificates are used." },
    SchemaField { name: "tls_server_name", flag: "compactor.scheduler-client.metadata-cache.memcached.tls-server-name", kind: FieldKind::String, default_str: "", usage: "Override the expected name on the server certificate." },
    SchemaField { name: "tls_insecure_skip_verify", flag: "compactor.scheduler-client.metadata-cache.memcached.tls-insecure-skip-verify", kind: FieldKind::Bool, default_str: "false", usage: "Skip validating server certificate." },
    SchemaField { name: "tls_cipher_suites", flag: "compactor.scheduler-client.metadata-cache.memcached.tls-cipher-suites", kind: FieldKind::String, default_str: "", usage: "Override the default cipher suite list (separated by commas). Allowed values:\n\nSecure Ciphers:\n- TLS_AES_128_GCM_SHA256\n- TLS_AES_256_GCM_SHA384\n- TLS_CHACHA20_POLY1305_SHA256\n- TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA\n- TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA\n- TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256\n- TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256\n- TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256\n- TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256\n\nInsecure Ciphers:\n- TLS_RSA_WITH_RC4_128_SHA\n- TLS_RSA_WITH_3DES_EDE_CBC_SHA\n- TLS_RSA_WITH_AES_128_CBC_SHA\n- TLS_RSA_WITH_AES_256_CBC_SHA\n- TLS_RSA_WITH_AES_128_CBC_SHA256\n- TLS_RSA_WITH_AES_128_GCM_SHA256\n- TLS_RSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_ECDSA_WITH_RC4_128_SHA\n- TLS_ECDHE_RSA_WITH_RC4_128_SHA\n- TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256\n- TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256\n" },
    SchemaField { name: "tls_min_version", flag: "compactor.scheduler-client.metadata-cache.memcached.tls-min-version", kind: FieldKind::String, default_str: "", usage: "Override the default minimum TLS version. Allowed values: VersionTLS10, VersionTLS11, VersionTLS12, VersionTLS13" },
];

static BLOCK_55_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_56_FIELDS: &[SchemaField] = &[
    SchemaField { name: "backend", flag: "compactor.scheduler-client.metadata-cache.backend", kind: FieldKind::String, default_str: "", usage: "Backend for metadata cache, if not empty. Supported values: memcached." },
    SchemaField { name: "metafile_content_ttl", flag: "compactor.scheduler-client.metadata-cache.metafile-content-ttl", kind: FieldKind::Duration, default_str: "86400000000000", usage: "How long to cache block metadata content." },
];

static BLOCK_56_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "memcached", fields: BLOCK_55_FIELDS, blocks: BLOCK_55_BLOCKS },
];

static BLOCK_57_FIELDS: &[SchemaField] = &[
    SchemaField { name: "enabled", flag: "compactor.scheduler-client.enabled", kind: FieldKind::Bool, default_str: "false", usage: "Controls whether compactors should contact a scheduler to request work." },
    SchemaField { name: "enable_ring_based_cleanup", flag: "compactor.scheduler-client.enable-ring-based-cleanup", kind: FieldKind::Bool, default_str: "true", usage: "Run the ring-based blocks cleaner and join the compactor ring, which is otherwise unused. Can only be disabled when -compactor.scheduler-client.enabled is true. WARNING: disabling this on every compactor stops cleanup and breaks reads cluster-wide." },
    SchemaField { name: "scheduler_endpoint", flag: "compactor.scheduler-client.scheduler-endpoint", kind: FieldKind::String, default_str: "", usage: "Compactor scheduler endpoint." },
    SchemaField { name: "leasing_min_backoff", flag: "compactor.scheduler-client.leasing-min-backoff", kind: FieldKind::Duration, default_str: "100000000", usage: "Minimum backoff time between scheduler job lease requests." },
    SchemaField { name: "leasing_max_backoff", flag: "compactor.scheduler-client.leasing-max-backoff", kind: FieldKind::Duration, default_str: "120000000000", usage: "Maximum backoff time between scheduler job lease requests." },
    SchemaField { name: "update_interval", flag: "compactor.scheduler-client.update-interval", kind: FieldKind::Duration, default_str: "15000000000", usage: "Interval between scheduler job lease updates." },
    SchemaField { name: "update_min_backoff", flag: "compactor.scheduler-client.update-min-backoff", kind: FieldKind::Duration, default_str: "1000000000", usage: "Minimum backoff time for compaction executor retries when sending scheduler status updates." },
    SchemaField { name: "update_max_backoff", flag: "compactor.scheduler-client.update-max-backoff", kind: FieldKind::Duration, default_str: "32000000000", usage: "Maximum backoff time for compaction executor retries when sending scheduler status updates." },
    SchemaField { name: "compaction_dir_cleanup_interval", flag: "compactor.scheduler-client.compaction-dir-cleanup-interval", kind: FieldKind::Duration, default_str: "1800000000000", usage: "Defines how frequently to clean up the compaction working directory. The directory is cleaned on startup and then only when this interval has elapsed since the last cleanup. Set to 0 to disable periodic cleanup." },
    SchemaField { name: "terminating_final_status_timeout", flag: "compactor.scheduler-client.terminating-final-status-timeout", kind: FieldKind::Duration, default_str: "30000000000", usage: "Timeout for sending a final job status update to the scheduler when the parent context is canceled (e.g. during shutdown)." },
    SchemaField { name: "lanes", flag: "compactor.scheduler-client.lanes", kind: FieldKind::String, default_str: "compact+plan,plan", usage: "Lanes to request for each worker goroutine. Each entry is a '+'-separated list of job types in priority order." },
    SchemaField { name: "enable_interrupted_reassign", flag: "compactor.scheduler-client.enable-interrupted-reassign", kind: FieldKind::Bool, default_str: "true", usage: "Report a distinct job update status to the scheduler when a job is interrupted (e.g., clean shutdown)." },
];

static BLOCK_57_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "grpc_client_config", fields: BLOCK_54_FIELDS, blocks: BLOCK_54_BLOCKS },
    SchemaBlock { name: "metadata_cache", fields: BLOCK_56_FIELDS, blocks: BLOCK_56_BLOCKS },
];

static BLOCK_58_FIELDS: &[SchemaField] = &[
    SchemaField { name: "block_ranges", flag: "compactor.block-ranges", kind: FieldKind::StringList, default_str: "2h0m0s,12h0m0s,24h0m0s", usage: "List of compaction time ranges." },
    SchemaField { name: "block_sync_concurrency", flag: "compactor.block-sync-concurrency", kind: FieldKind::Int, default_str: "8", usage: "Number of goroutines to use when downloading blocks for compaction and uploading resulting blocks." },
    SchemaField { name: "block_health_validation_concurrency", flag: "compactor.block-health-validation-concurrency", kind: FieldKind::Int, default_str: "0", usage: "Number of blocks whose health can be validated concurrently during a compaction job. A nonpositive value means no limit." },
    SchemaField { name: "meta_sync_concurrency", flag: "compactor.meta-sync-concurrency", kind: FieldKind::Int, default_str: "20", usage: "Number of goroutines to use when syncing block meta files from the long term storage." },
    SchemaField { name: "data_dir", flag: "compactor.data-dir", kind: FieldKind::String, default_str: "./data-compactor/", usage: "Directory to temporarily store blocks during compaction. This directory is not required to be persisted between restarts." },
    SchemaField { name: "compaction_interval", flag: "compactor.compaction-interval", kind: FieldKind::Duration, default_str: "3600000000000", usage: "The frequency at which the compaction runs" },
    SchemaField { name: "compaction_retries", flag: "compactor.compaction-retries", kind: FieldKind::Int, default_str: "3", usage: "How many times to retry a failed compaction within a single compaction run." },
    SchemaField { name: "compaction_concurrency", flag: "compactor.compaction-concurrency", kind: FieldKind::Int, default_str: "1", usage: "Max number of concurrent compactions running." },
    SchemaField { name: "first_level_compaction_wait_period", flag: "compactor.first-level-compaction-wait-period", kind: FieldKind::Duration, default_str: "1500000000000", usage: "How long the compactor waits before compacting first-level blocks that are uploaded by the ingesters or block-builders. This configuration option allows for the reduction of cases where the compactor begins to compact blocks before all ingesters have uploaded their blocks to the storage. Does not apply to out-of-order blocks." },
    SchemaField { name: "first_level_compaction_ooo_wait_period", flag: "compactor.first-level-compaction-ooo-wait-period", kind: FieldKind::Duration, default_str: "300000000000", usage: "How long the compactor waits before compacting first-level blocks containing out-of-order samples. When set to 0, out-of-order blocks do not delay compaction." },
    SchemaField { name: "first_level_compaction_skip_future_max_time", flag: "compactor.first-level-compaction-skip-future-max-time", kind: FieldKind::Bool, default_str: "true", usage: "When enabled, the compactor skips first-level compaction jobs if any source block has a MaxTime more recent than the wait period threshold. This prevents premature compaction of blocks that may still receive late-arriving data." },
    SchemaField { name: "cleanup_interval", flag: "compactor.cleanup-interval", kind: FieldKind::Duration, default_str: "900000000000", usage: "How frequently the compactor should run blocks cleanup and maintenance, as well as update the bucket index." },
    SchemaField { name: "cleanup_concurrency", flag: "compactor.cleanup-concurrency", kind: FieldKind::Int, default_str: "20", usage: "Max number of tenants for which blocks cleanup and maintenance should run concurrently." },
    SchemaField { name: "deletion_delay", flag: "compactor.deletion-delay", kind: FieldKind::Duration, default_str: "43200000000000", usage: "Time before a block marked for deletion is deleted from bucket. If not 0, blocks will be marked for deletion and the compactor component will permanently delete blocks marked for deletion from the bucket. If 0, blocks will be deleted straight away. Note that deleting blocks immediately can cause query failures." },
    SchemaField { name: "tenant_cleanup_delay", flag: "compactor.tenant-cleanup-delay", kind: FieldKind::Duration, default_str: "21600000000000", usage: "For tenants marked for deletion, this is the time between deletion of the last block, and doing final cleanup (marker files, debug files) of the tenant." },
    SchemaField { name: "max_compaction_time", flag: "compactor.max-compaction-time", kind: FieldKind::Duration, default_str: "3600000000000", usage: "Max time for starting compactions for a single tenant. After this time no new compactions for the tenant are started before next compaction cycle. This can help in multi-tenant environments to avoid single tenant using all compaction time, but also in single-tenant environments to force new discovery of blocks more often. 0 = disabled." },
    SchemaField { name: "max_opening_blocks_concurrency", flag: "compactor.max-opening-blocks-concurrency", kind: FieldKind::Int, default_str: "1", usage: "Number of goroutines opening blocks before compaction." },
    SchemaField { name: "max_closing_blocks_concurrency", flag: "compactor.max-closing-blocks-concurrency", kind: FieldKind::Int, default_str: "1", usage: "Max number of blocks that can be closed concurrently during split compaction. Note that closing a newly compacted block uses a lot of memory for writing the index." },
    SchemaField { name: "symbols_flushers_concurrency", flag: "compactor.symbols-flushers-concurrency", kind: FieldKind::Int, default_str: "1", usage: "Number of symbols flushers used when doing split compaction." },
    SchemaField { name: "max_block_upload_validation_concurrency", flag: "compactor.max-block-upload-validation-concurrency", kind: FieldKind::Int, default_str: "1", usage: "Max number of uploaded blocks that can be validated concurrently. 0 = no limit." },
    SchemaField { name: "update_blocks_concurrency", flag: "compactor.update-blocks-concurrency", kind: FieldKind::Int, default_str: "1", usage: "Number of goroutines to use when updating blocks metadata during bucket index updates." },
    SchemaField { name: "block_symbol_table_size_threshold", flag: "compactor.block-symbol-table-size-threshold", kind: FieldKind::Int, default_str: "0", usage: "Maximum symbol table size in bytes for a compacted block. When the symbol table of a just-compacted block exceeds this threshold, the block is proactively marked as no-compact. 0 = disabled." },
    SchemaField { name: "enabled_tenants", flag: "compactor.enabled-tenants", kind: FieldKind::String, default_str: "", usage: "Comma separated list of tenants that can be compacted. If specified, only these tenants will be compacted by the compactor, otherwise all tenants can be compacted. Subject to sharding." },
    SchemaField { name: "disabled_tenants", flag: "compactor.disabled-tenants", kind: FieldKind::String, default_str: "", usage: "Comma separated list of tenants that cannot be compacted by the compactor. If specified, and the compactor would normally pick a given tenant for compaction (via -compactor.enabled-tenants or sharding), it will be ignored instead." },
    SchemaField { name: "compaction_jobs_order", flag: "compactor.compaction-jobs-order", kind: FieldKind::String, default_str: "smallest-range-oldest-blocks-first", usage: "The sorting to use when deciding which compaction jobs should run first for a given tenant. Supported values are: smallest-range-oldest-blocks-first, newest-blocks-first." },
    SchemaField { name: "read_compartment_id", flag: "compactor.read-compartment-id", kind: FieldKind::Int, default_str: "0", usage: "" },
];

static BLOCK_58_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "sharding_ring", fields: BLOCK_51_FIELDS, blocks: BLOCK_51_BLOCKS },
    SchemaBlock { name: "scheduler_client", fields: BLOCK_57_FIELDS, blocks: BLOCK_57_BLOCKS },
];

static BLOCK_59_FIELDS: &[SchemaField] = &[
    SchemaField { name: "min_period", flag: "compactor-scheduler.tenant-discovery-backoff.backoff-min-period", kind: FieldKind::Duration, default_str: "100000000", usage: "Minimum delay when backing off." },
    SchemaField { name: "max_period", flag: "compactor-scheduler.tenant-discovery-backoff.backoff-max-period", kind: FieldKind::Duration, default_str: "10000000000", usage: "Maximum delay when backing off." },
    SchemaField { name: "max_retries", flag: "compactor-scheduler.tenant-discovery-backoff.backoff-retries", kind: FieldKind::Int, default_str: "10", usage: "Number of times to backoff and retry before failing." },
];

static BLOCK_59_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_60_FIELDS: &[SchemaField] = &[
    SchemaField { name: "dir", flag: "compactor-scheduler.bbolt.dir", kind: FieldKind::String, default_str: "./data-compactor-scheduler", usage: "The directory where bbolt shard database files are stored for the compactor scheduler." },
    SchemaField { name: "shard_count", flag: "compactor-scheduler.bbolt.shard-count", kind: FieldKind::Int, default_str: "16", usage: "The target number of bbolt database shards for the compactor scheduler." },
];

static BLOCK_60_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_61_FIELDS: &[SchemaField] = &[
    SchemaField { name: "policy", flag: "compactor-scheduler.lane-policy.policy", kind: FieldKind::String, default_str: "simple", usage: "The lane policy the compactor scheduler should use. Valid values: simple" },
];

static BLOCK_61_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_62_FIELDS: &[SchemaField] = &[
    SchemaField { name: "max_leases", flag: "compactor-scheduler.max-leases", kind: FieldKind::Int, default_str: "3", usage: "The maximum number of times a compaction job can be retried before it is removed. Leases that are reassigned due to an interrupted worker do not count against this limit. 0 for no limit." },
    SchemaField { name: "lease_duration", flag: "compactor-scheduler.lease-duration", kind: FieldKind::Duration, default_str: "600000000000", usage: "The duration of time without contact until the scheduler is able to lease a work item to another worker." },
    SchemaField { name: "planning_interval", flag: "compactor-scheduler.planning-interval", kind: FieldKind::Duration, default_str: "1800000000000", usage: "The duration of time between when plan jobs are submitted aligned by UTC. Note that -compactor.first-level-compaction-wait-period is accounted for during alignment of this interval." },
    SchemaField { name: "maintenance_interval", flag: "compactor-scheduler.maintenance-interval", kind: FieldKind::Duration, default_str: "120000000000", usage: "The duration of time between when maintenance tasks are performed on job trackers. This includes lease expiration and plan job submission checks." },
    SchemaField { name: "maintenance_intervals_before_lease_expiration", flag: "compactor-scheduler.maintenance-intervals-before-lease-expiration", kind: FieldKind::Int, default_str: "3", usage: "The number of maintenance intervals before lease expiration is enforced. Nonpositive values are all treated as zero." },
    SchemaField { name: "maintenance_intervals_before_cold_start_planning", flag: "compactor-scheduler.maintenance-intervals-before-cold-start-planning", kind: FieldKind::Int, default_str: "5", usage: "The number of maintenance intervals before planning occurs when starting from no recovered state. Nonpositive values are all treated as zero." },
    SchemaField { name: "tenant_discovery_interval", flag: "compactor-scheduler.tenant-discovery-interval", kind: FieldKind::Duration, default_str: "600000000000", usage: "The duration of time between bucket listings to discover new tenants." },
    SchemaField { name: "persistence_type", flag: "compactor-scheduler.persistence-type", kind: FieldKind::String, default_str: "bbolt", usage: "The type of persistence the compactor scheduler should use. Valid values: none, bbolt" },
    SchemaField { name: "repeated_failure_report_threshold", flag: "compactor-scheduler.repeated-failure-report-threshold", kind: FieldKind::Int, default_str: "2", usage: "The number of times a job can fail before a repeated failure is recorded. Reassignments due to an interrupted worker are not counted as a failure. 0 for no limit." },
];

static BLOCK_62_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "tenant_discovery_backoff", fields: BLOCK_59_FIELDS, blocks: BLOCK_59_BLOCKS },
    SchemaBlock { name: "bbolt", fields: BLOCK_60_FIELDS, blocks: BLOCK_60_BLOCKS },
    SchemaBlock { name: "lane_policy", fields: BLOCK_61_FIELDS, blocks: BLOCK_61_BLOCKS },
];

static BLOCK_63_FIELDS: &[SchemaField] = &[
    SchemaField { name: "host", flag: "store-gateway.sharding-ring.consul.hostname", kind: FieldKind::String, default_str: "localhost:8500", usage: "Hostname and port of Consul." },
    SchemaField { name: "acl_token", flag: "store-gateway.sharding-ring.consul.acl-token", kind: FieldKind::Secret, default_str: "", usage: "ACL Token used to interact with Consul." },
    SchemaField { name: "http_client_timeout", flag: "store-gateway.sharding-ring.consul.client-timeout", kind: FieldKind::Duration, default_str: "20000000000", usage: "HTTP timeout when talking to Consul" },
    SchemaField { name: "consistent_reads", flag: "store-gateway.sharding-ring.consul.consistent-reads", kind: FieldKind::Bool, default_str: "false", usage: "Enable consistent reads to Consul." },
    SchemaField { name: "watch_rate_limit", flag: "store-gateway.sharding-ring.consul.watch-rate-limit", kind: FieldKind::Float, default_str: "1", usage: "Rate limit when watching key or prefix in Consul, in requests per second. 0 disables the rate limit." },
    SchemaField { name: "watch_burst_size", flag: "store-gateway.sharding-ring.consul.watch-burst-size", kind: FieldKind::Int, default_str: "1", usage: "Burst size used in rate limit. Values less than 1 are treated as 1." },
    SchemaField { name: "cas_retry_delay", flag: "store-gateway.sharding-ring.consul.cas-retry-delay", kind: FieldKind::Duration, default_str: "1000000000", usage: "Maximum duration to wait before retrying a Compare And Swap (CAS) operation." },
];

static BLOCK_63_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_64_FIELDS: &[SchemaField] = &[
    SchemaField { name: "endpoints", flag: "store-gateway.sharding-ring.etcd.endpoints", kind: FieldKind::StringList, default_str: "", usage: "The etcd endpoints to connect to." },
    SchemaField { name: "dial_timeout", flag: "store-gateway.sharding-ring.etcd.dial-timeout", kind: FieldKind::Duration, default_str: "10000000000", usage: "The dial timeout for the etcd connection." },
    SchemaField { name: "max_retries", flag: "store-gateway.sharding-ring.etcd.max-retries", kind: FieldKind::Int, default_str: "10", usage: "The maximum number of retries to do for failed ops." },
    SchemaField { name: "tls_enabled", flag: "store-gateway.sharding-ring.etcd.tls-enabled", kind: FieldKind::Bool, default_str: "false", usage: "Enable TLS." },
    SchemaField { name: "tls_cert_path", flag: "store-gateway.sharding-ring.etcd.tls-cert-path", kind: FieldKind::String, default_str: "", usage: "Path to the client certificate, which will be used for authenticating with the server. Also requires the key path to be configured." },
    SchemaField { name: "tls_key_path", flag: "store-gateway.sharding-ring.etcd.tls-key-path", kind: FieldKind::String, default_str: "", usage: "Path to the key for the client certificate. Also requires the client certificate to be configured." },
    SchemaField { name: "tls_ca_path", flag: "store-gateway.sharding-ring.etcd.tls-ca-path", kind: FieldKind::String, default_str: "", usage: "Path to the CA certificates to validate server certificate against. If not set, the host's root CA certificates are used." },
    SchemaField { name: "tls_server_name", flag: "store-gateway.sharding-ring.etcd.tls-server-name", kind: FieldKind::String, default_str: "", usage: "Override the expected name on the server certificate." },
    SchemaField { name: "tls_insecure_skip_verify", flag: "store-gateway.sharding-ring.etcd.tls-insecure-skip-verify", kind: FieldKind::Bool, default_str: "false", usage: "Skip validating server certificate." },
    SchemaField { name: "tls_cipher_suites", flag: "store-gateway.sharding-ring.etcd.tls-cipher-suites", kind: FieldKind::String, default_str: "", usage: "Override the default cipher suite list (separated by commas). Allowed values:\n\nSecure Ciphers:\n- TLS_AES_128_GCM_SHA256\n- TLS_AES_256_GCM_SHA384\n- TLS_CHACHA20_POLY1305_SHA256\n- TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA\n- TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA\n- TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256\n- TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256\n- TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256\n- TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256\n\nInsecure Ciphers:\n- TLS_RSA_WITH_RC4_128_SHA\n- TLS_RSA_WITH_3DES_EDE_CBC_SHA\n- TLS_RSA_WITH_AES_128_CBC_SHA\n- TLS_RSA_WITH_AES_256_CBC_SHA\n- TLS_RSA_WITH_AES_128_CBC_SHA256\n- TLS_RSA_WITH_AES_128_GCM_SHA256\n- TLS_RSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_ECDSA_WITH_RC4_128_SHA\n- TLS_ECDHE_RSA_WITH_RC4_128_SHA\n- TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256\n- TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256\n" },
    SchemaField { name: "tls_min_version", flag: "store-gateway.sharding-ring.etcd.tls-min-version", kind: FieldKind::String, default_str: "", usage: "Override the default minimum TLS version. Allowed values: VersionTLS10, VersionTLS11, VersionTLS12, VersionTLS13" },
    SchemaField { name: "username", flag: "store-gateway.sharding-ring.etcd.username", kind: FieldKind::String, default_str: "", usage: "Etcd username." },
    SchemaField { name: "password", flag: "store-gateway.sharding-ring.etcd.password", kind: FieldKind::Secret, default_str: "", usage: "Etcd password." },
];

static BLOCK_64_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_65_FIELDS: &[SchemaField] = &[
    SchemaField { name: "primary", flag: "store-gateway.sharding-ring.multi.primary", kind: FieldKind::String, default_str: "", usage: "Primary backend storage used by multi-client." },
    SchemaField { name: "secondary", flag: "store-gateway.sharding-ring.multi.secondary", kind: FieldKind::String, default_str: "", usage: "Secondary backend storage used by multi-client." },
    SchemaField { name: "mirror_enabled", flag: "store-gateway.sharding-ring.multi.mirror-enabled", kind: FieldKind::Bool, default_str: "false", usage: "Mirror writes to the secondary store." },
    SchemaField { name: "mirror_timeout", flag: "store-gateway.sharding-ring.multi.mirror-timeout", kind: FieldKind::Duration, default_str: "2000000000", usage: "Timeout for storing a value to the secondary store." },
];

static BLOCK_65_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_66_FIELDS: &[SchemaField] = &[
    SchemaField { name: "store", flag: "store-gateway.sharding-ring.store", kind: FieldKind::String, default_str: "memberlist", usage: "Backend storage to use for the ring. Supported values are: consul, etcd, inmemory, memberlist, multi." },
    SchemaField { name: "prefix", flag: "store-gateway.sharding-ring.prefix", kind: FieldKind::String, default_str: "collectors/", usage: "The prefix for the keys in the store. Should end with a /." },
];

static BLOCK_66_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "consul", fields: BLOCK_63_FIELDS, blocks: BLOCK_63_BLOCKS },
    SchemaBlock { name: "etcd", fields: BLOCK_64_FIELDS, blocks: BLOCK_64_BLOCKS },
    SchemaBlock { name: "multi", fields: BLOCK_65_FIELDS, blocks: BLOCK_65_BLOCKS },
];

static BLOCK_67_FIELDS: &[SchemaField] = &[
    SchemaField { name: "heartbeat_period", flag: "store-gateway.sharding-ring.heartbeat-period", kind: FieldKind::Duration, default_str: "15000000000", usage: "Period at which to heartbeat to the ring." },
    SchemaField { name: "heartbeat_timeout", flag: "store-gateway.sharding-ring.heartbeat-timeout", kind: FieldKind::Duration, default_str: "60000000000", usage: "The heartbeat timeout after which store gateways are considered unhealthy within the ring. This option needs be set both on the store-gateway, querier and ruler when running in microservices mode." },
    SchemaField { name: "auto_forget_enabled", flag: "store-gateway.sharding-ring.auto-forget-enabled", kind: FieldKind::Bool, default_str: "true", usage: "When enabled, a store-gateway is automatically removed from the ring after failing to heartbeat the ring for a period longer than the configured -store-gateway.sharding-ring.auto-forget-unhealthy-periods times the configured -store-gateway.sharding-ring.heartbeat-timeout. This setting is deprecated. Set -store-gateway.sharding-ring.auto-forget-unhealthy-periods to 0 to disable auto-forget." },
    SchemaField { name: "auto_forget_unhealthy_periods", flag: "store-gateway.sharding-ring.auto-forget-unhealthy-periods", kind: FieldKind::Int, default_str: "10", usage: "Number of consecutive timeout periods after which Mimir automatically removes an unhealthy instance in the ring. Set to 0 to disable auto-forget." },
    SchemaField { name: "replication_factor", flag: "store-gateway.sharding-ring.replication-factor", kind: FieldKind::Int, default_str: "3", usage: "The replication factor to use when sharding blocks. This option needs be set both on the store-gateway, querier and ruler when running in microservices mode." },
    SchemaField { name: "tokens_file_path", flag: "store-gateway.sharding-ring.tokens-file-path", kind: FieldKind::String, default_str: "", usage: "File path where tokens are stored. If empty, tokens are not stored at shutdown and restored at startup." },
    SchemaField { name: "num_tokens", flag: "store-gateway.sharding-ring.num-tokens", kind: FieldKind::Int, default_str: "512", usage: "Number of tokens for each store-gateway." },
    SchemaField { name: "zone_awareness_enabled", flag: "store-gateway.sharding-ring.zone-awareness-enabled", kind: FieldKind::Bool, default_str: "false", usage: "True to enable zone-awareness and replicate blocks across different availability zones. This option needs be set both on the store-gateway, querier and ruler when running in microservices mode." },
    SchemaField { name: "excluded_zones", flag: "store-gateway.sharding-ring.excluded-zones", kind: FieldKind::String, default_str: "", usage: "Comma-separated list of zones to exclude from the ring. Instances in excluded zones will be filtered out from the ring. This option needs be set both on the store-gateway, querier and ruler when running in microservices mode." },
    SchemaField { name: "wait_stability_min_duration", flag: "store-gateway.sharding-ring.wait-stability-min-duration", kind: FieldKind::Duration, default_str: "0", usage: "Minimum time to wait for ring stability at startup, if set to positive value." },
    SchemaField { name: "wait_stability_max_duration", flag: "store-gateway.sharding-ring.wait-stability-max-duration", kind: FieldKind::Duration, default_str: "300000000000", usage: "Maximum time to wait for ring stability at startup. If the store-gateway ring keeps changing after this period of time, the store-gateway will start anyway." },
    SchemaField { name: "instance_id", flag: "store-gateway.sharding-ring.instance-id", kind: FieldKind::String, default_str: "<hostname>", usage: "Instance ID to register in the ring." },
    SchemaField { name: "instance_interface_names", flag: "store-gateway.sharding-ring.instance-interface-names", kind: FieldKind::StringList, default_str: "", usage: "List of network interface names to look up when finding the instance IP address." },
    SchemaField { name: "instance_port", flag: "store-gateway.sharding-ring.instance-port", kind: FieldKind::Int, default_str: "0", usage: "Port to advertise in the ring (defaults to -server.grpc-listen-port)." },
    SchemaField { name: "instance_addr", flag: "store-gateway.sharding-ring.instance-addr", kind: FieldKind::String, default_str: "", usage: "IP address to advertise in the ring. Default is auto-detected." },
    SchemaField { name: "instance_enable_ipv6", flag: "store-gateway.sharding-ring.instance-enable-ipv6", kind: FieldKind::Bool, default_str: "false", usage: "Enable using a IPv6 instance address. (default false)" },
    SchemaField { name: "instance_availability_zone", flag: "store-gateway.sharding-ring.instance-availability-zone", kind: FieldKind::String, default_str: "", usage: "The availability zone where this instance is running. Required if zone-awareness is enabled." },
    SchemaField { name: "unregister_on_shutdown", flag: "store-gateway.sharding-ring.unregister-on-shutdown", kind: FieldKind::Bool, default_str: "true", usage: "Unregister from the ring upon clean shutdown." },
];

static BLOCK_67_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "kvstore", fields: BLOCK_66_FIELDS, blocks: BLOCK_66_BLOCKS },
];

static BLOCK_68_FIELDS: &[SchemaField] = &[
    SchemaField { name: "enabled", flag: "store-gateway.dynamic-replication.enabled", kind: FieldKind::Bool, default_str: "false", usage: "Use a higher number of replicas for recent blocks. Useful to spread query load more evenly at the cost of slightly higher disk usage." },
    SchemaField { name: "max_time_threshold", flag: "store-gateway.dynamic-replication.max-time-threshold", kind: FieldKind::Duration, default_str: "90000000000000", usage: "Threshold of the most recent sample in a block used to determine it is eligible for higher than default replication. If a block has samples within this amount of time, it is considered recent and will be owned by more replicas." },
    SchemaField { name: "multiple", flag: "store-gateway.dynamic-replication.multiple", kind: FieldKind::Int, default_str: "5", usage: "Multiple of the default replication factor that should be used for recent blocks. Minimum value is 2" },
];

static BLOCK_68_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_69_FIELDS: &[SchemaField] = &[
    SchemaField { name: "enabled_tenants", flag: "store-gateway.enabled-tenants", kind: FieldKind::String, default_str: "", usage: "Comma separated list of tenants that can be loaded by the store-gateway. If specified, only blocks for these tenants will be loaded by the store-gateway, otherwise all tenants can be loaded. Subject to sharding." },
    SchemaField { name: "disabled_tenants", flag: "store-gateway.disabled-tenants", kind: FieldKind::String, default_str: "", usage: "Comma separated list of tenants that cannot be loaded by the store-gateway. If specified, and the store-gateway would normally load a given tenant for (via -store-gateway.enabled-tenants or sharding), it will be ignored instead." },
    SchemaField { name: "read_compartment_id", flag: "store-gateway.read-compartment-id", kind: FieldKind::Int, default_str: "0", usage: "" },
];

static BLOCK_69_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "sharding_ring", fields: BLOCK_67_FIELDS, blocks: BLOCK_67_BLOCKS },
    SchemaBlock { name: "dynamic_replication", fields: BLOCK_68_FIELDS, blocks: BLOCK_68_BLOCKS },
];

static BLOCK_70_FIELDS: &[SchemaField] = &[
    SchemaField { name: "enabled", flag: "tenant-federation.enabled", kind: FieldKind::Bool, default_str: "false", usage: "If enabled on all services, queries can be federated across multiple tenants. The tenant IDs involved need to be specified separated by a '|' character in the 'X-Scope-OrgID' header." },
    SchemaField { name: "max_concurrent", flag: "tenant-federation.max-concurrent", kind: FieldKind::Int, default_str: "16", usage: "The number of workers used for each tenant federated query. This setting limits the maximum number of per-tenant queries executed at a time for a tenant federated query." },
    SchemaField { name: "max_tenants", flag: "tenant-federation.max-tenants", kind: FieldKind::Int, default_str: "0", usage: "The max number of tenant IDs that may be supplied for a federated query if enabled. 0 to disable the limit." },
];

static BLOCK_70_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_71_FIELDS: &[SchemaField] = &[
    SchemaField { name: "role_id", flag: "vault.auth.approle.role-id", kind: FieldKind::String, default_str: "", usage: "Role ID of the AppRole" },
    SchemaField { name: "secret_id", flag: "vault.auth.approle.secret-id", kind: FieldKind::Secret, default_str: "", usage: "Secret ID issued against the AppRole" },
    SchemaField { name: "wrapping_token", flag: "vault.auth.approle.wrapping-token", kind: FieldKind::Bool, default_str: "false", usage: "Response wrapping token if the Secret ID is response wrapped" },
    SchemaField { name: "mount_path", flag: "vault.auth.approle.mount-path", kind: FieldKind::String, default_str: "", usage: "Path if the Vault backend was mounted using a non-default path" },
];

static BLOCK_71_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_72_FIELDS: &[SchemaField] = &[
    SchemaField { name: "role_name", flag: "vault.auth.kubernetes.role-name", kind: FieldKind::String, default_str: "", usage: "The Kubernetes named role" },
    SchemaField { name: "service_account_token", flag: "vault.auth.kubernetes.service-account-token", kind: FieldKind::SecretOmitEmpty, default_str: "", usage: "The Service Account JWT" },
    SchemaField { name: "service_account_token_path", flag: "vault.auth.kubernetes.service-account-token-path", kind: FieldKind::String, default_str: "", usage: "Path to where the Kubernetes service account token is mounted. By default it lives at /var/run/secrets/kubernetes.io/serviceaccount/token. Field will be used if the service_account_token is not specified." },
    SchemaField { name: "mount_path", flag: "vault.auth.kubernetes.mount-path", kind: FieldKind::String, default_str: "", usage: "Path if the Vault backend was mounted using a non-default path" },
];

static BLOCK_72_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_73_FIELDS: &[SchemaField] = &[
    SchemaField { name: "username", flag: "vault.auth.userpass.username", kind: FieldKind::String, default_str: "", usage: "The userpass auth method username" },
    SchemaField { name: "password", flag: "vault.auth.userpass.password", kind: FieldKind::Secret, default_str: "", usage: "The userpass auth method password" },
    SchemaField { name: "mount_path", flag: "vault.auth.userpass.mount-path", kind: FieldKind::String, default_str: "", usage: "Path if the Vault backend was mounted using a non-default path" },
];

static BLOCK_73_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_74_FIELDS: &[SchemaField] = &[
    SchemaField { name: "token", flag: "vault.auth.token", kind: FieldKind::Secret, default_str: "", usage: "The token used to authenticate against Vault" },
];

static BLOCK_74_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_75_FIELDS: &[SchemaField] = &[
    SchemaField { name: "type", flag: "vault.auth.type", kind: FieldKind::String, default_str: "", usage: "Authentication type to use. Supported types are: approle, kubernetes, userpass, token" },
];

static BLOCK_75_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "approle", fields: BLOCK_71_FIELDS, blocks: BLOCK_71_BLOCKS },
    SchemaBlock { name: "kubernetes", fields: BLOCK_72_FIELDS, blocks: BLOCK_72_BLOCKS },
    SchemaBlock { name: "userpass", fields: BLOCK_73_FIELDS, blocks: BLOCK_73_BLOCKS },
    SchemaBlock { name: "token", fields: BLOCK_74_FIELDS, blocks: BLOCK_74_BLOCKS },
];

static BLOCK_76_FIELDS: &[SchemaField] = &[
    SchemaField { name: "enabled", flag: "vault.enabled", kind: FieldKind::Bool, default_str: "false", usage: "Enables fetching of keys and certificates from Vault" },
    SchemaField { name: "url", flag: "vault.url", kind: FieldKind::String, default_str: "", usage: "Location of the Vault server" },
    SchemaField { name: "mount_path", flag: "vault.mount-path", kind: FieldKind::String, default_str: "", usage: "Location of secrets engine within Vault" },
];

static BLOCK_76_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "auth", fields: BLOCK_75_FIELDS, blocks: BLOCK_75_BLOCKS },
];

static BLOCK_77_FIELDS: &[SchemaField] = &[
    SchemaField { name: "min_period", flag: "ruler.client.backoff-min-period", kind: FieldKind::Duration, default_str: "100000000", usage: "Minimum delay when backing off." },
    SchemaField { name: "max_period", flag: "ruler.client.backoff-max-period", kind: FieldKind::Duration, default_str: "10000000000", usage: "Maximum delay when backing off." },
    SchemaField { name: "max_retries", flag: "ruler.client.backoff-retries", kind: FieldKind::Int, default_str: "10", usage: "Number of times to backoff and retry before failing." },
];

static BLOCK_77_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_78_FIELDS: &[SchemaField] = &[
    SchemaField { name: "label", flag: "ruler.client.cluster-validation.label", kind: FieldKind::String, default_str: "", usage: "Primary cluster validation label." },
];

static BLOCK_78_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_79_FIELDS: &[SchemaField] = &[
    SchemaField { name: "max_recv_msg_size", flag: "ruler.client.grpc-max-recv-msg-size", kind: FieldKind::Int, default_str: "104857600", usage: "gRPC client max receive message size (bytes)." },
    SchemaField { name: "max_send_msg_size", flag: "ruler.client.grpc-max-send-msg-size", kind: FieldKind::Int, default_str: "104857600", usage: "gRPC client max send message size (bytes)." },
    SchemaField { name: "grpc_compression", flag: "ruler.client.grpc-compression", kind: FieldKind::String, default_str: "", usage: "Use compression when sending messages. Supported values are: 'gzip', 'snappy', 's2' and '' (disable compression)" },
    SchemaField { name: "rate_limit", flag: "ruler.client.grpc-client-rate-limit", kind: FieldKind::Float, default_str: "0", usage: "Rate limit for gRPC client; 0 means disabled." },
    SchemaField { name: "rate_limit_burst", flag: "ruler.client.grpc-client-rate-limit-burst", kind: FieldKind::Int, default_str: "0", usage: "Rate limit burst for gRPC client." },
    SchemaField { name: "backoff_on_ratelimits", flag: "ruler.client.backoff-on-ratelimits", kind: FieldKind::Bool, default_str: "false", usage: "Enable backoff and retry when we hit rate limits." },
    SchemaField { name: "initial_stream_window_size", flag: "ruler.client.initial-stream-window-size", kind: FieldKind::Bytes, default_str: "63KiB1023B", usage: "Initial stream window size. Values less than the default are not supported and are ignored. Setting this to a value other than the default disables the BDP estimator." },
    SchemaField { name: "initial_connection_window_size", flag: "ruler.client.initial-connection-window-size", kind: FieldKind::Bytes, default_str: "63KiB1023B", usage: "Initial connection window size. Values less than the default are not supported and are ignored. Setting this to a value other than the default disables the BDP estimator." },
    SchemaField { name: "tls_enabled", flag: "ruler.client.tls-enabled", kind: FieldKind::Bool, default_str: "false", usage: "Enable TLS in the gRPC client. This flag needs to be enabled when any other TLS flag is set. If set to false, insecure connection to gRPC server will be used." },
    SchemaField { name: "tls_cert_path", flag: "ruler.client.tls-cert-path", kind: FieldKind::String, default_str: "", usage: "Path to the client certificate, which will be used for authenticating with the server. Also requires the key path to be configured." },
    SchemaField { name: "tls_key_path", flag: "ruler.client.tls-key-path", kind: FieldKind::String, default_str: "", usage: "Path to the key for the client certificate. Also requires the client certificate to be configured." },
    SchemaField { name: "tls_ca_path", flag: "ruler.client.tls-ca-path", kind: FieldKind::String, default_str: "", usage: "Path to the CA certificates to validate server certificate against. If not set, the host's root CA certificates are used." },
    SchemaField { name: "tls_server_name", flag: "ruler.client.tls-server-name", kind: FieldKind::String, default_str: "", usage: "Override the expected name on the server certificate." },
    SchemaField { name: "tls_insecure_skip_verify", flag: "ruler.client.tls-insecure-skip-verify", kind: FieldKind::Bool, default_str: "false", usage: "Skip validating server certificate." },
    SchemaField { name: "tls_cipher_suites", flag: "ruler.client.tls-cipher-suites", kind: FieldKind::String, default_str: "", usage: "Override the default cipher suite list (separated by commas). Allowed values:\n\nSecure Ciphers:\n- TLS_AES_128_GCM_SHA256\n- TLS_AES_256_GCM_SHA384\n- TLS_CHACHA20_POLY1305_SHA256\n- TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA\n- TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA\n- TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256\n- TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256\n- TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256\n- TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256\n\nInsecure Ciphers:\n- TLS_RSA_WITH_RC4_128_SHA\n- TLS_RSA_WITH_3DES_EDE_CBC_SHA\n- TLS_RSA_WITH_AES_128_CBC_SHA\n- TLS_RSA_WITH_AES_256_CBC_SHA\n- TLS_RSA_WITH_AES_128_CBC_SHA256\n- TLS_RSA_WITH_AES_128_GCM_SHA256\n- TLS_RSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_ECDSA_WITH_RC4_128_SHA\n- TLS_ECDHE_RSA_WITH_RC4_128_SHA\n- TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256\n- TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256\n" },
    SchemaField { name: "tls_min_version", flag: "ruler.client.tls-min-version", kind: FieldKind::String, default_str: "", usage: "Override the default minimum TLS version. Allowed values: VersionTLS10, VersionTLS11, VersionTLS12, VersionTLS13" },
    SchemaField { name: "connect_timeout", flag: "ruler.client.connect-timeout", kind: FieldKind::Duration, default_str: "5000000000", usage: "The maximum amount of time to establish a connection. A value of 0 means default gRPC client connect timeout and backoff." },
    SchemaField { name: "connect_backoff_base_delay", flag: "ruler.client.connect-backoff-base-delay", kind: FieldKind::Duration, default_str: "1000000000", usage: "Initial backoff delay after first connection failure. Only relevant if ConnectTimeout > 0." },
    SchemaField { name: "connect_backoff_max_delay", flag: "ruler.client.connect-backoff-max-delay", kind: FieldKind::Duration, default_str: "5000000000", usage: "Maximum backoff delay when establishing a connection. Only relevant if ConnectTimeout > 0." },
    SchemaField { name: "keepalive_time", flag: "ruler.client.keepalive-time", kind: FieldKind::Duration, default_str: "20000000000", usage: "After a duration of this time if the client doesn't see any activity it pings the server to see if the transport is still alive. This also determines the socket's TCP_USER_TIMEOUT together with keepalive-timeout." },
    SchemaField { name: "keepalive_timeout", flag: "ruler.client.keepalive-timeout", kind: FieldKind::Duration, default_str: "10000000000", usage: "After having pinged for keepalive check, the client waits for a duration of this time and if no activity is seen even after that the connection is closed." },
    SchemaField { name: "health_check_grace_period", flag: "ruler.client.health-check-grace-period", kind: FieldKind::Duration, default_str: "0", usage: "The grace period for health checks. If a ruler connection consistently fails health checks for this period, any open connections are closed. The ruler will attempt to reconnect to that ruler if a subsequent request is made to that ruler. Set to 0 to immediately remove ruler connections on the first health check failure." },
];

static BLOCK_79_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "backoff_config", fields: BLOCK_77_FIELDS, blocks: BLOCK_77_BLOCKS },
    SchemaBlock { name: "cluster_validation", fields: BLOCK_78_FIELDS, blocks: BLOCK_78_BLOCKS },
];

static BLOCK_80_FIELDS: &[SchemaField] = &[
    SchemaField { name: "client_id", flag: "", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "client_secret", flag: "", kind: FieldKind::Secret, default_str: "", usage: "" },
    SchemaField { name: "token_url", flag: "", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "scopes", flag: "", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "endpoint_params", flag: "", kind: FieldKind::Other, default_str: "{}", usage: "" },
];

static BLOCK_80_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_81_FIELDS: &[SchemaField] = &[
    SchemaField { name: "tls_enabled", flag: "", kind: FieldKind::Bool, default_str: "false", usage: "" },
    SchemaField { name: "tls_cert_path", flag: "", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "tls_key_path", flag: "", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "tls_ca_path", flag: "", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "tls_server_name", flag: "", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "tls_insecure_skip_verify", flag: "", kind: FieldKind::Bool, default_str: "false", usage: "" },
    SchemaField { name: "tls_cipher_suites", flag: "", kind: FieldKind::String, default_str: "", usage: "Override the default cipher suite list (separated by commas). Allowed values:\n\nSecure Ciphers:\n- TLS_AES_128_GCM_SHA256\n- TLS_AES_256_GCM_SHA384\n- TLS_CHACHA20_POLY1305_SHA256\n- TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA\n- TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA\n- TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256\n- TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256\n- TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256\n- TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256\n\nInsecure Ciphers:\n- TLS_RSA_WITH_RC4_128_SHA\n- TLS_RSA_WITH_3DES_EDE_CBC_SHA\n- TLS_RSA_WITH_AES_128_CBC_SHA\n- TLS_RSA_WITH_AES_256_CBC_SHA\n- TLS_RSA_WITH_AES_128_CBC_SHA256\n- TLS_RSA_WITH_AES_128_GCM_SHA256\n- TLS_RSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_ECDSA_WITH_RC4_128_SHA\n- TLS_ECDHE_RSA_WITH_RC4_128_SHA\n- TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256\n- TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256\n" },
    SchemaField { name: "tls_min_version", flag: "", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "basic_auth_username", flag: "", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "basic_auth_password", flag: "", kind: FieldKind::Secret, default_str: "", usage: "" },
    SchemaField { name: "proxy_url", flag: "", kind: FieldKind::String, default_str: "", usage: "" },
];

static BLOCK_81_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "oauth2", fields: BLOCK_80_FIELDS, blocks: BLOCK_80_BLOCKS },
];

static BLOCK_82_FIELDS: &[SchemaField] = &[
    SchemaField { name: "host", flag: "ruler.ring.consul.hostname", kind: FieldKind::String, default_str: "localhost:8500", usage: "Hostname and port of Consul." },
    SchemaField { name: "acl_token", flag: "ruler.ring.consul.acl-token", kind: FieldKind::Secret, default_str: "", usage: "ACL Token used to interact with Consul." },
    SchemaField { name: "http_client_timeout", flag: "ruler.ring.consul.client-timeout", kind: FieldKind::Duration, default_str: "20000000000", usage: "HTTP timeout when talking to Consul" },
    SchemaField { name: "consistent_reads", flag: "ruler.ring.consul.consistent-reads", kind: FieldKind::Bool, default_str: "false", usage: "Enable consistent reads to Consul." },
    SchemaField { name: "watch_rate_limit", flag: "ruler.ring.consul.watch-rate-limit", kind: FieldKind::Float, default_str: "1", usage: "Rate limit when watching key or prefix in Consul, in requests per second. 0 disables the rate limit." },
    SchemaField { name: "watch_burst_size", flag: "ruler.ring.consul.watch-burst-size", kind: FieldKind::Int, default_str: "1", usage: "Burst size used in rate limit. Values less than 1 are treated as 1." },
    SchemaField { name: "cas_retry_delay", flag: "ruler.ring.consul.cas-retry-delay", kind: FieldKind::Duration, default_str: "1000000000", usage: "Maximum duration to wait before retrying a Compare And Swap (CAS) operation." },
];

static BLOCK_82_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_83_FIELDS: &[SchemaField] = &[
    SchemaField { name: "endpoints", flag: "ruler.ring.etcd.endpoints", kind: FieldKind::StringList, default_str: "", usage: "The etcd endpoints to connect to." },
    SchemaField { name: "dial_timeout", flag: "ruler.ring.etcd.dial-timeout", kind: FieldKind::Duration, default_str: "10000000000", usage: "The dial timeout for the etcd connection." },
    SchemaField { name: "max_retries", flag: "ruler.ring.etcd.max-retries", kind: FieldKind::Int, default_str: "10", usage: "The maximum number of retries to do for failed ops." },
    SchemaField { name: "tls_enabled", flag: "ruler.ring.etcd.tls-enabled", kind: FieldKind::Bool, default_str: "false", usage: "Enable TLS." },
    SchemaField { name: "tls_cert_path", flag: "ruler.ring.etcd.tls-cert-path", kind: FieldKind::String, default_str: "", usage: "Path to the client certificate, which will be used for authenticating with the server. Also requires the key path to be configured." },
    SchemaField { name: "tls_key_path", flag: "ruler.ring.etcd.tls-key-path", kind: FieldKind::String, default_str: "", usage: "Path to the key for the client certificate. Also requires the client certificate to be configured." },
    SchemaField { name: "tls_ca_path", flag: "ruler.ring.etcd.tls-ca-path", kind: FieldKind::String, default_str: "", usage: "Path to the CA certificates to validate server certificate against. If not set, the host's root CA certificates are used." },
    SchemaField { name: "tls_server_name", flag: "ruler.ring.etcd.tls-server-name", kind: FieldKind::String, default_str: "", usage: "Override the expected name on the server certificate." },
    SchemaField { name: "tls_insecure_skip_verify", flag: "ruler.ring.etcd.tls-insecure-skip-verify", kind: FieldKind::Bool, default_str: "false", usage: "Skip validating server certificate." },
    SchemaField { name: "tls_cipher_suites", flag: "ruler.ring.etcd.tls-cipher-suites", kind: FieldKind::String, default_str: "", usage: "Override the default cipher suite list (separated by commas). Allowed values:\n\nSecure Ciphers:\n- TLS_AES_128_GCM_SHA256\n- TLS_AES_256_GCM_SHA384\n- TLS_CHACHA20_POLY1305_SHA256\n- TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA\n- TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA\n- TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256\n- TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256\n- TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256\n- TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256\n\nInsecure Ciphers:\n- TLS_RSA_WITH_RC4_128_SHA\n- TLS_RSA_WITH_3DES_EDE_CBC_SHA\n- TLS_RSA_WITH_AES_128_CBC_SHA\n- TLS_RSA_WITH_AES_256_CBC_SHA\n- TLS_RSA_WITH_AES_128_CBC_SHA256\n- TLS_RSA_WITH_AES_128_GCM_SHA256\n- TLS_RSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_ECDSA_WITH_RC4_128_SHA\n- TLS_ECDHE_RSA_WITH_RC4_128_SHA\n- TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256\n- TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256\n" },
    SchemaField { name: "tls_min_version", flag: "ruler.ring.etcd.tls-min-version", kind: FieldKind::String, default_str: "", usage: "Override the default minimum TLS version. Allowed values: VersionTLS10, VersionTLS11, VersionTLS12, VersionTLS13" },
    SchemaField { name: "username", flag: "ruler.ring.etcd.username", kind: FieldKind::String, default_str: "", usage: "Etcd username." },
    SchemaField { name: "password", flag: "ruler.ring.etcd.password", kind: FieldKind::Secret, default_str: "", usage: "Etcd password." },
];

static BLOCK_83_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_84_FIELDS: &[SchemaField] = &[
    SchemaField { name: "primary", flag: "ruler.ring.multi.primary", kind: FieldKind::String, default_str: "", usage: "Primary backend storage used by multi-client." },
    SchemaField { name: "secondary", flag: "ruler.ring.multi.secondary", kind: FieldKind::String, default_str: "", usage: "Secondary backend storage used by multi-client." },
    SchemaField { name: "mirror_enabled", flag: "ruler.ring.multi.mirror-enabled", kind: FieldKind::Bool, default_str: "false", usage: "Mirror writes to the secondary store." },
    SchemaField { name: "mirror_timeout", flag: "ruler.ring.multi.mirror-timeout", kind: FieldKind::Duration, default_str: "2000000000", usage: "Timeout for storing a value to the secondary store." },
];

static BLOCK_84_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_85_FIELDS: &[SchemaField] = &[
    SchemaField { name: "store", flag: "ruler.ring.store", kind: FieldKind::String, default_str: "memberlist", usage: "Backend storage to use for the ring. Supported values are: consul, etcd, inmemory, memberlist, multi." },
    SchemaField { name: "prefix", flag: "ruler.ring.prefix", kind: FieldKind::String, default_str: "rulers/", usage: "The prefix for the keys in the store. Should end with a /." },
];

static BLOCK_85_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "consul", fields: BLOCK_82_FIELDS, blocks: BLOCK_82_BLOCKS },
    SchemaBlock { name: "etcd", fields: BLOCK_83_FIELDS, blocks: BLOCK_83_BLOCKS },
    SchemaBlock { name: "multi", fields: BLOCK_84_FIELDS, blocks: BLOCK_84_BLOCKS },
];

static BLOCK_86_FIELDS: &[SchemaField] = &[
    SchemaField { name: "heartbeat_period", flag: "ruler.ring.heartbeat-period", kind: FieldKind::Duration, default_str: "15000000000", usage: "Period at which to heartbeat to the ring." },
    SchemaField { name: "heartbeat_timeout", flag: "ruler.ring.heartbeat-timeout", kind: FieldKind::Duration, default_str: "60000000000", usage: "Heartbeat timeout after which Mimir marks rulers as unhealthy in the ring." },
    SchemaField { name: "instance_id", flag: "ruler.ring.instance-id", kind: FieldKind::String, default_str: "<hostname>", usage: "Instance ID to register in the ring." },
    SchemaField { name: "instance_interface_names", flag: "ruler.ring.instance-interface-names", kind: FieldKind::StringList, default_str: "", usage: "List of network interface names to look up when finding the instance IP address." },
    SchemaField { name: "instance_port", flag: "ruler.ring.instance-port", kind: FieldKind::Int, default_str: "0", usage: "Port to advertise in the ring (defaults to -server.grpc-listen-port)." },
    SchemaField { name: "instance_addr", flag: "ruler.ring.instance-addr", kind: FieldKind::String, default_str: "", usage: "IP address to advertise in the ring. Default is auto-detected." },
    SchemaField { name: "instance_enable_ipv6", flag: "ruler.ring.instance-enable-ipv6", kind: FieldKind::Bool, default_str: "false", usage: "Enable using an IPv6 instance address." },
    SchemaField { name: "auto_forget_unhealthy_periods", flag: "ruler.ring.auto-forget-unhealthy-periods", kind: FieldKind::Int, default_str: "2", usage: "Number of consecutive timeout periods after which Mimir automatically removes an unhealthy instance in the ring. Set to 0 to disable auto-forget." },
    SchemaField { name: "num_tokens", flag: "ruler.ring.num-tokens", kind: FieldKind::Int, default_str: "128", usage: "Number of tokens for each ruler." },
];

static BLOCK_86_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "kvstore", fields: BLOCK_85_FIELDS, blocks: BLOCK_85_BLOCKS },
];

static BLOCK_87_FIELDS: &[SchemaField] = &[
    SchemaField { name: "min_period", flag: "ruler.query-frontend.grpc-client-config.backoff-min-period", kind: FieldKind::Duration, default_str: "100000000", usage: "Minimum delay when backing off." },
    SchemaField { name: "max_period", flag: "ruler.query-frontend.grpc-client-config.backoff-max-period", kind: FieldKind::Duration, default_str: "10000000000", usage: "Maximum delay when backing off." },
    SchemaField { name: "max_retries", flag: "ruler.query-frontend.grpc-client-config.backoff-retries", kind: FieldKind::Int, default_str: "10", usage: "Number of times to backoff and retry before failing." },
];

static BLOCK_87_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_88_FIELDS: &[SchemaField] = &[
    SchemaField { name: "label", flag: "ruler.query-frontend.grpc-client-config.cluster-validation.label", kind: FieldKind::String, default_str: "", usage: "Primary cluster validation label." },
];

static BLOCK_88_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_89_FIELDS: &[SchemaField] = &[
    SchemaField { name: "max_recv_msg_size", flag: "ruler.query-frontend.grpc-client-config.grpc-max-recv-msg-size", kind: FieldKind::Int, default_str: "104857600", usage: "gRPC client max receive message size (bytes)." },
    SchemaField { name: "max_send_msg_size", flag: "ruler.query-frontend.grpc-client-config.grpc-max-send-msg-size", kind: FieldKind::Int, default_str: "104857600", usage: "gRPC client max send message size (bytes)." },
    SchemaField { name: "grpc_compression", flag: "ruler.query-frontend.grpc-client-config.grpc-compression", kind: FieldKind::String, default_str: "", usage: "Use compression when sending messages. Supported values are: 'gzip', 'snappy', 's2' and '' (disable compression)" },
    SchemaField { name: "rate_limit", flag: "ruler.query-frontend.grpc-client-config.grpc-client-rate-limit", kind: FieldKind::Float, default_str: "0", usage: "Rate limit for gRPC client; 0 means disabled." },
    SchemaField { name: "rate_limit_burst", flag: "ruler.query-frontend.grpc-client-config.grpc-client-rate-limit-burst", kind: FieldKind::Int, default_str: "0", usage: "Rate limit burst for gRPC client." },
    SchemaField { name: "backoff_on_ratelimits", flag: "ruler.query-frontend.grpc-client-config.backoff-on-ratelimits", kind: FieldKind::Bool, default_str: "false", usage: "Enable backoff and retry when we hit rate limits." },
    SchemaField { name: "initial_stream_window_size", flag: "ruler.query-frontend.grpc-client-config.initial-stream-window-size", kind: FieldKind::Bytes, default_str: "63KiB1023B", usage: "Initial stream window size. Values less than the default are not supported and are ignored. Setting this to a value other than the default disables the BDP estimator." },
    SchemaField { name: "initial_connection_window_size", flag: "ruler.query-frontend.grpc-client-config.initial-connection-window-size", kind: FieldKind::Bytes, default_str: "63KiB1023B", usage: "Initial connection window size. Values less than the default are not supported and are ignored. Setting this to a value other than the default disables the BDP estimator." },
    SchemaField { name: "tls_enabled", flag: "ruler.query-frontend.grpc-client-config.tls-enabled", kind: FieldKind::Bool, default_str: "false", usage: "Enable TLS in the gRPC client. This flag needs to be enabled when any other TLS flag is set. If set to false, insecure connection to gRPC server will be used." },
    SchemaField { name: "tls_cert_path", flag: "ruler.query-frontend.grpc-client-config.tls-cert-path", kind: FieldKind::String, default_str: "", usage: "Path to the client certificate, which will be used for authenticating with the server. Also requires the key path to be configured." },
    SchemaField { name: "tls_key_path", flag: "ruler.query-frontend.grpc-client-config.tls-key-path", kind: FieldKind::String, default_str: "", usage: "Path to the key for the client certificate. Also requires the client certificate to be configured." },
    SchemaField { name: "tls_ca_path", flag: "ruler.query-frontend.grpc-client-config.tls-ca-path", kind: FieldKind::String, default_str: "", usage: "Path to the CA certificates to validate server certificate against. If not set, the host's root CA certificates are used." },
    SchemaField { name: "tls_server_name", flag: "ruler.query-frontend.grpc-client-config.tls-server-name", kind: FieldKind::String, default_str: "", usage: "Override the expected name on the server certificate." },
    SchemaField { name: "tls_insecure_skip_verify", flag: "ruler.query-frontend.grpc-client-config.tls-insecure-skip-verify", kind: FieldKind::Bool, default_str: "false", usage: "Skip validating server certificate." },
    SchemaField { name: "tls_cipher_suites", flag: "ruler.query-frontend.grpc-client-config.tls-cipher-suites", kind: FieldKind::String, default_str: "", usage: "Override the default cipher suite list (separated by commas). Allowed values:\n\nSecure Ciphers:\n- TLS_AES_128_GCM_SHA256\n- TLS_AES_256_GCM_SHA384\n- TLS_CHACHA20_POLY1305_SHA256\n- TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA\n- TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA\n- TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256\n- TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256\n- TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256\n- TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256\n\nInsecure Ciphers:\n- TLS_RSA_WITH_RC4_128_SHA\n- TLS_RSA_WITH_3DES_EDE_CBC_SHA\n- TLS_RSA_WITH_AES_128_CBC_SHA\n- TLS_RSA_WITH_AES_256_CBC_SHA\n- TLS_RSA_WITH_AES_128_CBC_SHA256\n- TLS_RSA_WITH_AES_128_GCM_SHA256\n- TLS_RSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_ECDSA_WITH_RC4_128_SHA\n- TLS_ECDHE_RSA_WITH_RC4_128_SHA\n- TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256\n- TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256\n" },
    SchemaField { name: "tls_min_version", flag: "ruler.query-frontend.grpc-client-config.tls-min-version", kind: FieldKind::String, default_str: "", usage: "Override the default minimum TLS version. Allowed values: VersionTLS10, VersionTLS11, VersionTLS12, VersionTLS13" },
    SchemaField { name: "connect_timeout", flag: "ruler.query-frontend.grpc-client-config.connect-timeout", kind: FieldKind::Duration, default_str: "5000000000", usage: "The maximum amount of time to establish a connection. A value of 0 means default gRPC client connect timeout and backoff." },
    SchemaField { name: "connect_backoff_base_delay", flag: "ruler.query-frontend.grpc-client-config.connect-backoff-base-delay", kind: FieldKind::Duration, default_str: "1000000000", usage: "Initial backoff delay after first connection failure. Only relevant if ConnectTimeout > 0." },
    SchemaField { name: "connect_backoff_max_delay", flag: "ruler.query-frontend.grpc-client-config.connect-backoff-max-delay", kind: FieldKind::Duration, default_str: "5000000000", usage: "Maximum backoff delay when establishing a connection. Only relevant if ConnectTimeout > 0." },
    SchemaField { name: "keepalive_time", flag: "ruler.query-frontend.grpc-client-config.keepalive-time", kind: FieldKind::Duration, default_str: "20000000000", usage: "After a duration of this time if the client doesn't see any activity it pings the server to see if the transport is still alive. This also determines the socket's TCP_USER_TIMEOUT together with keepalive-timeout." },
    SchemaField { name: "keepalive_timeout", flag: "ruler.query-frontend.grpc-client-config.keepalive-timeout", kind: FieldKind::Duration, default_str: "10000000000", usage: "After having pinged for keepalive check, the client waits for a duration of this time and if no activity is seen even after that the connection is closed." },
];

static BLOCK_89_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "backoff_config", fields: BLOCK_87_FIELDS, blocks: BLOCK_87_BLOCKS },
    SchemaBlock { name: "cluster_validation", fields: BLOCK_88_FIELDS, blocks: BLOCK_88_BLOCKS },
];

static BLOCK_90_FIELDS: &[SchemaField] = &[
    SchemaField { name: "connect_timeout", flag: "ruler.query-frontend.http-client-config.connect-timeout", kind: FieldKind::Duration, default_str: "30000000000", usage: "Timeout for establishing a connection to the query-frontend." },
];

static BLOCK_90_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_91_FIELDS: &[SchemaField] = &[
    SchemaField { name: "address", flag: "ruler.query-frontend.address", kind: FieldKind::String, default_str: "", usage: "Can be either the GRPC listen address of the query-frontend(s) or the HTTP/HTTPS address of a Prometheus-compatible server. Must be a DNS address (prefixed with dns:///) to enable GRPC client side load balancing." },
    SchemaField { name: "query_result_response_format", flag: "ruler.query-frontend.query-result-response-format", kind: FieldKind::String, default_str: "protobuf", usage: "Format to use when retrieving query results from query-frontends. Supported values: json, protobuf" },
    SchemaField { name: "max_retries_rate", flag: "ruler.query-frontend.max-retries-rate", kind: FieldKind::Float, default_str: "170", usage: "Maximum number of retries for failed queries per second." },
    SchemaField { name: "enable_rule_detail_headers", flag: "ruler.query-frontend.enable-rule-detail-headers", kind: FieldKind::Bool, default_str: "false", usage: "Set to true to attach headers identifying the rule being evaluated to the queries sent to the query-frontend: X-Rule-Name, X-Rule-Type, X-Rule-Source, X-Rule-Namespace, and X-Rule-Group. Use it together with -query-frontend.log-query-request-headers to attribute rule queries in the query-frontend logs." },
];

static BLOCK_91_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "grpc_client_config", fields: BLOCK_89_FIELDS, blocks: BLOCK_89_BLOCKS },
    SchemaBlock { name: "http_client_config", fields: BLOCK_90_FIELDS, blocks: BLOCK_90_BLOCKS },
];

static BLOCK_92_FIELDS: &[SchemaField] = &[
    SchemaField { name: "min_period", flag: "ruler.distributor.grpc-client-config.backoff-min-period", kind: FieldKind::Duration, default_str: "100000000", usage: "Minimum delay when backing off." },
    SchemaField { name: "max_period", flag: "ruler.distributor.grpc-client-config.backoff-max-period", kind: FieldKind::Duration, default_str: "10000000000", usage: "Maximum delay when backing off." },
    SchemaField { name: "max_retries", flag: "ruler.distributor.grpc-client-config.backoff-retries", kind: FieldKind::Int, default_str: "10", usage: "Number of times to backoff and retry before failing." },
];

static BLOCK_92_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_93_FIELDS: &[SchemaField] = &[
    SchemaField { name: "label", flag: "ruler.distributor.grpc-client-config.cluster-validation.label", kind: FieldKind::String, default_str: "", usage: "Primary cluster validation label." },
];

static BLOCK_93_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_94_FIELDS: &[SchemaField] = &[
    SchemaField { name: "max_recv_msg_size", flag: "ruler.distributor.grpc-client-config.grpc-max-recv-msg-size", kind: FieldKind::Int, default_str: "104857600", usage: "gRPC client max receive message size (bytes)." },
    SchemaField { name: "max_send_msg_size", flag: "ruler.distributor.grpc-client-config.grpc-max-send-msg-size", kind: FieldKind::Int, default_str: "104857600", usage: "gRPC client max send message size (bytes)." },
    SchemaField { name: "grpc_compression", flag: "ruler.distributor.grpc-client-config.grpc-compression", kind: FieldKind::String, default_str: "", usage: "Use compression when sending messages. Supported values are: 'gzip', 'snappy', 's2' and '' (disable compression)" },
    SchemaField { name: "rate_limit", flag: "ruler.distributor.grpc-client-config.grpc-client-rate-limit", kind: FieldKind::Float, default_str: "0", usage: "Rate limit for gRPC client; 0 means disabled." },
    SchemaField { name: "rate_limit_burst", flag: "ruler.distributor.grpc-client-config.grpc-client-rate-limit-burst", kind: FieldKind::Int, default_str: "0", usage: "Rate limit burst for gRPC client." },
    SchemaField { name: "backoff_on_ratelimits", flag: "ruler.distributor.grpc-client-config.backoff-on-ratelimits", kind: FieldKind::Bool, default_str: "false", usage: "Enable backoff and retry when we hit rate limits." },
    SchemaField { name: "initial_stream_window_size", flag: "ruler.distributor.grpc-client-config.initial-stream-window-size", kind: FieldKind::Bytes, default_str: "63KiB1023B", usage: "Initial stream window size. Values less than the default are not supported and are ignored. Setting this to a value other than the default disables the BDP estimator." },
    SchemaField { name: "initial_connection_window_size", flag: "ruler.distributor.grpc-client-config.initial-connection-window-size", kind: FieldKind::Bytes, default_str: "63KiB1023B", usage: "Initial connection window size. Values less than the default are not supported and are ignored. Setting this to a value other than the default disables the BDP estimator." },
    SchemaField { name: "tls_enabled", flag: "ruler.distributor.grpc-client-config.tls-enabled", kind: FieldKind::Bool, default_str: "false", usage: "Enable TLS in the gRPC client. This flag needs to be enabled when any other TLS flag is set. If set to false, insecure connection to gRPC server will be used." },
    SchemaField { name: "tls_cert_path", flag: "ruler.distributor.grpc-client-config.tls-cert-path", kind: FieldKind::String, default_str: "", usage: "Path to the client certificate, which will be used for authenticating with the server. Also requires the key path to be configured." },
    SchemaField { name: "tls_key_path", flag: "ruler.distributor.grpc-client-config.tls-key-path", kind: FieldKind::String, default_str: "", usage: "Path to the key for the client certificate. Also requires the client certificate to be configured." },
    SchemaField { name: "tls_ca_path", flag: "ruler.distributor.grpc-client-config.tls-ca-path", kind: FieldKind::String, default_str: "", usage: "Path to the CA certificates to validate server certificate against. If not set, the host's root CA certificates are used." },
    SchemaField { name: "tls_server_name", flag: "ruler.distributor.grpc-client-config.tls-server-name", kind: FieldKind::String, default_str: "", usage: "Override the expected name on the server certificate." },
    SchemaField { name: "tls_insecure_skip_verify", flag: "ruler.distributor.grpc-client-config.tls-insecure-skip-verify", kind: FieldKind::Bool, default_str: "false", usage: "Skip validating server certificate." },
    SchemaField { name: "tls_cipher_suites", flag: "ruler.distributor.grpc-client-config.tls-cipher-suites", kind: FieldKind::String, default_str: "", usage: "Override the default cipher suite list (separated by commas). Allowed values:\n\nSecure Ciphers:\n- TLS_AES_128_GCM_SHA256\n- TLS_AES_256_GCM_SHA384\n- TLS_CHACHA20_POLY1305_SHA256\n- TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA\n- TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA\n- TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256\n- TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256\n- TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256\n- TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256\n\nInsecure Ciphers:\n- TLS_RSA_WITH_RC4_128_SHA\n- TLS_RSA_WITH_3DES_EDE_CBC_SHA\n- TLS_RSA_WITH_AES_128_CBC_SHA\n- TLS_RSA_WITH_AES_256_CBC_SHA\n- TLS_RSA_WITH_AES_128_CBC_SHA256\n- TLS_RSA_WITH_AES_128_GCM_SHA256\n- TLS_RSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_ECDSA_WITH_RC4_128_SHA\n- TLS_ECDHE_RSA_WITH_RC4_128_SHA\n- TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256\n- TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256\n" },
    SchemaField { name: "tls_min_version", flag: "ruler.distributor.grpc-client-config.tls-min-version", kind: FieldKind::String, default_str: "", usage: "Override the default minimum TLS version. Allowed values: VersionTLS10, VersionTLS11, VersionTLS12, VersionTLS13" },
    SchemaField { name: "connect_timeout", flag: "ruler.distributor.grpc-client-config.connect-timeout", kind: FieldKind::Duration, default_str: "5000000000", usage: "The maximum amount of time to establish a connection. A value of 0 means default gRPC client connect timeout and backoff." },
    SchemaField { name: "connect_backoff_base_delay", flag: "ruler.distributor.grpc-client-config.connect-backoff-base-delay", kind: FieldKind::Duration, default_str: "1000000000", usage: "Initial backoff delay after first connection failure. Only relevant if ConnectTimeout > 0." },
    SchemaField { name: "connect_backoff_max_delay", flag: "ruler.distributor.grpc-client-config.connect-backoff-max-delay", kind: FieldKind::Duration, default_str: "5000000000", usage: "Maximum backoff delay when establishing a connection. Only relevant if ConnectTimeout > 0." },
    SchemaField { name: "keepalive_time", flag: "ruler.distributor.grpc-client-config.keepalive-time", kind: FieldKind::Duration, default_str: "20000000000", usage: "After a duration of this time if the client doesn't see any activity it pings the server to see if the transport is still alive. This also determines the socket's TCP_USER_TIMEOUT together with keepalive-timeout." },
    SchemaField { name: "keepalive_timeout", flag: "ruler.distributor.grpc-client-config.keepalive-timeout", kind: FieldKind::Duration, default_str: "10000000000", usage: "After having pinged for keepalive check, the client waits for a duration of this time and if no activity is seen even after that the connection is closed." },
];

static BLOCK_94_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "backoff_config", fields: BLOCK_92_FIELDS, blocks: BLOCK_92_BLOCKS },
    SchemaBlock { name: "cluster_validation", fields: BLOCK_93_FIELDS, blocks: BLOCK_93_BLOCKS },
];

static BLOCK_95_FIELDS: &[SchemaField] = &[
    SchemaField { name: "address", flag: "ruler.distributor.address", kind: FieldKind::String, default_str: "", usage: "gRPC listen address of the distributor(s) to push rule-result series to. If empty, the ruler writes using the internal distributor. Use a DNS address (prefixed with dns:///) to enable gRPC client-side load balancing; in Kubernetes, use the distributor headless service on the gRPC port." },
    SchemaField { name: "remote_timeout", flag: "ruler.distributor.remote-timeout", kind: FieldKind::Duration, default_str: "10000000000", usage: "Timeout for requests to remote distributors." },
    SchemaField { name: "max_influx_request_size", flag: "distributor.max-influx-request-size", kind: FieldKind::Int, default_str: "104857600", usage: "" },
    SchemaField { name: "write_compartment_id", flag: "distributor.write-compartment-id", kind: FieldKind::Int, default_str: "0", usage: "" },
    SchemaField { name: "influx_endpoint_enabled", flag: "distributor.influx-endpoint-enabled", kind: FieldKind::Bool, default_str: "false", usage: "" },
    SchemaField { name: "start_time_quiet_zero", flag: "distributor.otel-start-time-quiet-zero", kind: FieldKind::Bool, default_str: "false", usage: "" },
];

static BLOCK_95_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "grpc_client_config", fields: BLOCK_94_FIELDS, blocks: BLOCK_94_BLOCKS },
];

static BLOCK_96_FIELDS: &[SchemaField] = &[
    SchemaField { name: "enabled", flag: "ruler.tenant-federation.enabled", kind: FieldKind::Bool, default_str: "false", usage: "Enable rule groups to query against multiple tenants. The tenant IDs involved need to be in the rule group's 'source_tenants' field. If this flag is set to 'false' when there are federated rule groups that already exist, then these rules groups will be skipped during evaluations." },
];

static BLOCK_96_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_97_FIELDS: &[SchemaField] = &[
    SchemaField { name: "external_url", flag: "ruler.external.url", kind: FieldKind::Other, default_str: "", usage: "URL of alerts return path." },
    SchemaField { name: "evaluation_interval", flag: "ruler.evaluation-interval", kind: FieldKind::Duration, default_str: "60000000000", usage: "How frequently to evaluate rules" },
    SchemaField { name: "poll_interval", flag: "ruler.poll-interval", kind: FieldKind::Duration, default_str: "600000000000", usage: "How frequently the configured rule groups are re-synced from the object storage." },
    SchemaField { name: "rule_path", flag: "ruler.rule-path", kind: FieldKind::String, default_str: "./data-ruler/", usage: "Directory to store temporary rule files loaded by the Prometheus rule managers. This directory is not required to be persisted between restarts." },
    SchemaField { name: "alertmanager_url", flag: "", kind: FieldKind::String, default_str: "", usage: "Deprecated, use limits.ruler_alertmanager_client_config.alertmanager_url instead." },
    SchemaField { name: "alertmanager_refresh_interval", flag: "ruler.alertmanager-refresh-interval", kind: FieldKind::Duration, default_str: "60000000000", usage: "How long to wait between refreshing DNS resolutions of Alertmanager hosts." },
    SchemaField { name: "notification_queue_capacity", flag: "ruler.notification-queue-capacity", kind: FieldKind::Int, default_str: "10000", usage: "Capacity of the queue for notifications to be sent to the Alertmanager." },
    SchemaField { name: "max_notification_batch_size", flag: "ruler.max-notification-batch-size", kind: FieldKind::Int, default_str: "256", usage: "Maximum number of notifications to send to Alertmanager in one request." },
    SchemaField { name: "notification_timeout", flag: "ruler.notification-timeout", kind: FieldKind::Duration, default_str: "10000000000", usage: "HTTP timeout duration when sending notifications to the Alertmanager." },
    SchemaField { name: "for_outage_tolerance", flag: "ruler.for-outage-tolerance", kind: FieldKind::Duration, default_str: "3600000000000", usage: "Max time to tolerate outage for restoring \"for\" state of alert." },
    SchemaField { name: "for_grace_period", flag: "ruler.for-grace-period", kind: FieldKind::Duration, default_str: "120000000000", usage: "This grace period controls which alerts the ruler restores after a restart. Alerts with \"for\" duration lower than this grace period are not restored after a ruler restart. This means that if the alerts have been firing before the ruler restarted, they will now go to pending state and then to firing again after their \"for\" duration expires. Alerts with \"for\" duration greater than or equal to this grace period that have been pending before the ruler restart will remain in pending state for at least this grace period. Alerts with \"for\" duration greater than or equal to this grace period that have been firing before the ruler restart will continue to be firing after the restart." },
    SchemaField { name: "resend_delay", flag: "ruler.resend-delay", kind: FieldKind::Duration, default_str: "300000000000", usage: "Minimum amount of time to wait before resending an alert to Alertmanager." },
    SchemaField { name: "enable_api", flag: "ruler.enable-api", kind: FieldKind::Bool, default_str: "true", usage: "Enable the ruler config API." },
    SchemaField { name: "enabled_tenants", flag: "ruler.enabled-tenants", kind: FieldKind::String, default_str: "", usage: "Comma separated list of tenants whose rules this ruler can evaluate. If specified, only these tenants will be handled by ruler, otherwise this ruler can process rules from all tenants. Subject to sharding." },
    SchemaField { name: "disabled_tenants", flag: "ruler.disabled-tenants", kind: FieldKind::String, default_str: "", usage: "Comma separated list of tenants whose rules this ruler cannot evaluate. If specified, a ruler that would normally pick the specified tenant(s) for processing will ignore them instead. Subject to sharding." },
    SchemaField { name: "query_stats_enabled", flag: "ruler.query-stats-enabled", kind: FieldKind::Bool, default_str: "false", usage: "Report the wall time for ruler queries to complete as a per-tenant metric and as an info level log message." },
    SchemaField { name: "outbound_sync_queue_poll_interval", flag: "ruler.outbound-sync-queue-poll-interval", kind: FieldKind::Duration, default_str: "10000000000", usage: "Interval between sending queued rule sync requests to ruler replicas." },
    SchemaField { name: "inbound_sync_queue_poll_interval", flag: "ruler.inbound-sync-queue-poll-interval", kind: FieldKind::Duration, default_str: "10000000000", usage: "Interval between applying queued incoming rule sync requests." },
    SchemaField { name: "ring_change_debounce", flag: "ruler.ring-change-debounce", kind: FieldKind::Duration, default_str: "0", usage: "How long to wait for the ring to stop changing before syncing rules in response to a ring change. This can reduce duplicate rule evaluation when multiple ring changes happen in quick succession, such as during a rollout. 0 disables debouncing and syncs immediately on every detected ring change, which is the default and historical behaviour. Must be less than -ruler.ring-change-max-debounce." },
    SchemaField { name: "ring_change_max_debounce", flag: "ruler.ring-change-max-debounce", kind: FieldKind::Duration, default_str: "15000000000", usage: "The maximum time to keep postponing a ring-change-triggered sync while the ring keeps changing, so continuous ring churn can't indefinitely delay picking up a ring change. Only used when -ruler.ring-change-debounce is greater than 0." },
    SchemaField { name: "max_independent_rule_evaluation_concurrency", flag: "ruler.max-independent-rule-evaluation-concurrency", kind: FieldKind::Int, default_str: "0", usage: "Number of rules rules that don't have dependencies that we allow to be evaluated concurrently across all tenants. 0 to disable." },
    SchemaField { name: "independent_rule_evaluation_concurrency_min_duration_percentage", flag: "ruler.independent-rule-evaluation-concurrency-min-duration-percentage", kind: FieldKind::Float, default_str: "50", usage: "Minimum threshold of the interval to last rule group runtime duration to allow a rule to be evaluated concurrency. By default, the rule group runtime duration must exceed 50.0% of the evaluation interval." },
    SchemaField { name: "rule_evaluation_write_enabled", flag: "ruler.rule-evaluation-write-enabled", kind: FieldKind::Bool, default_str: "true", usage: "Writes the results of rule evaluation to ingesters or ingest storage when enabled. Use this option for testing purposes. To disable, set to false." },
];

static BLOCK_97_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "ruler_client", fields: BLOCK_79_FIELDS, blocks: BLOCK_79_BLOCKS },
    SchemaBlock { name: "alertmanager_client", fields: BLOCK_81_FIELDS, blocks: BLOCK_81_BLOCKS },
    SchemaBlock { name: "ring", fields: BLOCK_86_FIELDS, blocks: BLOCK_86_BLOCKS },
    SchemaBlock { name: "query_frontend", fields: BLOCK_91_FIELDS, blocks: BLOCK_91_BLOCKS },
    SchemaBlock { name: "distributor", fields: BLOCK_95_FIELDS, blocks: BLOCK_95_BLOCKS },
    SchemaBlock { name: "tenant_federation", fields: BLOCK_96_FIELDS, blocks: BLOCK_96_BLOCKS },
];

static BLOCK_98_FIELDS: &[SchemaField] = &[
    SchemaField { name: "type", flag: "ruler-storage.s3.sse.type", kind: FieldKind::String, default_str: "", usage: "Enable AWS Server Side Encryption. Supported values: SSE-KMS, SSE-S3." },
    SchemaField { name: "kms_key_id", flag: "ruler-storage.s3.sse.kms-key-id", kind: FieldKind::String, default_str: "", usage: "KMS Key ID used to encrypt objects in S3" },
    SchemaField { name: "kms_encryption_context", flag: "ruler-storage.s3.sse.kms-encryption-context", kind: FieldKind::String, default_str: "", usage: "KMS Encryption Context used for object encryption. It expects JSON formatted string." },
];

static BLOCK_98_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_99_FIELDS: &[SchemaField] = &[
    SchemaField { name: "idle_conn_timeout", flag: "ruler-storage.s3.http.idle-conn-timeout", kind: FieldKind::Duration, default_str: "90000000000", usage: "The time an idle connection remains idle before closing." },
    SchemaField { name: "response_header_timeout", flag: "ruler-storage.s3.http.response-header-timeout", kind: FieldKind::Duration, default_str: "120000000000", usage: "The amount of time the client waits for a server's response headers." },
    SchemaField { name: "insecure_skip_verify", flag: "ruler-storage.s3.http.insecure-skip-verify", kind: FieldKind::Bool, default_str: "false", usage: "If the client connects to object storage via HTTPS and this option is enabled, the client accepts any certificate and hostname." },
    SchemaField { name: "tls_handshake_timeout", flag: "ruler-storage.s3.tls-handshake-timeout", kind: FieldKind::Duration, default_str: "10000000000", usage: "Maximum time to wait for a TLS handshake. Set to 0 for no limit." },
    SchemaField { name: "expect_continue_timeout", flag: "ruler-storage.s3.expect-continue-timeout", kind: FieldKind::Duration, default_str: "1000000000", usage: "The time to wait for a server's first response headers after fully writing the request headers if the request has an Expect header. Set to 0 to send the request body immediately." },
    SchemaField { name: "max_idle_connections", flag: "ruler-storage.s3.max-idle-connections", kind: FieldKind::Int, default_str: "100", usage: "Maximum number of idle (keep-alive) connections across all hosts. Set to 0 for no limit." },
    SchemaField { name: "max_idle_connections_per_host", flag: "ruler-storage.s3.max-idle-connections-per-host", kind: FieldKind::Int, default_str: "100", usage: "Maximum number of idle (keep-alive) connections to keep per-host. Set to 0 to use a built-in default value of 2." },
    SchemaField { name: "max_connections_per_host", flag: "ruler-storage.s3.max-connections-per-host", kind: FieldKind::Int, default_str: "0", usage: "Maximum number of connections per host. Set to 0 for no limit. The limit applies per bucket client, and a single process can create several clients for the same storage configuration, so its total connections to a host can exceed this value." },
    SchemaField { name: "force_attempt_http2", flag: "ruler-storage.s3.http.force-attempt-http2", kind: FieldKind::Bool, default_str: "false", usage: "If enabled, the HTTP client attempts HTTP/2 for HTTPS connections. Without this option, a client with a custom TLS configuration uses HTTP/1.1." },
    SchemaField { name: "tls_ca_path", flag: "ruler-storage.s3.http.tls-ca-path", kind: FieldKind::String, default_str: "", usage: "Path to the Certificate Authority (CA) certificates to validate the server certificate. If not set, the host's root CA certificates are used." },
    SchemaField { name: "tls_cert_path", flag: "ruler-storage.s3.http.tls-cert-path", kind: FieldKind::String, default_str: "", usage: "Path to the client certificate, which is used for authenticating with the server. This setting also requires you to configure the key path." },
    SchemaField { name: "tls_key_path", flag: "ruler-storage.s3.http.tls-key-path", kind: FieldKind::String, default_str: "", usage: "Path to the key for the client certificate. This setting also requires you to configure the client certificate." },
    SchemaField { name: "tls_server_name", flag: "ruler-storage.s3.http.tls-server-name", kind: FieldKind::String, default_str: "", usage: "Override the expected name on the server certificate." },
];

static BLOCK_99_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_100_FIELDS: &[SchemaField] = &[
    SchemaField { name: "enabled", flag: "ruler-storage.s3.trace.enabled", kind: FieldKind::Bool, default_str: "false", usage: "When enabled, low-level S3 HTTP operation information is logged at the debug level." },
];

static BLOCK_100_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_101_FIELDS: &[SchemaField] = &[
    SchemaField { name: "endpoint", flag: "ruler-storage.s3.endpoint", kind: FieldKind::String, default_str: "", usage: "The S3 bucket endpoint. It could be an AWS S3 endpoint listed at https://docs.aws.amazon.com/general/latest/gr/s3.html or the address of an S3-compatible service in hostname:port format." },
    SchemaField { name: "region", flag: "ruler-storage.s3.region", kind: FieldKind::String, default_str: "", usage: "S3 region. If unset, the client will issue a S3 GetBucketLocation API call to autodetect it." },
    SchemaField { name: "bucket_name", flag: "ruler-storage.s3.bucket-name", kind: FieldKind::String, default_str: "", usage: "S3 bucket name" },
    SchemaField { name: "secret_access_key", flag: "ruler-storage.s3.secret-access-key", kind: FieldKind::Secret, default_str: "", usage: "S3 secret access key" },
    SchemaField { name: "access_key_id", flag: "ruler-storage.s3.access-key-id", kind: FieldKind::String, default_str: "", usage: "S3 access key ID" },
    SchemaField { name: "session_token", flag: "ruler-storage.s3.session-token", kind: FieldKind::Secret, default_str: "", usage: "S3 session token" },
    SchemaField { name: "insecure", flag: "ruler-storage.s3.insecure", kind: FieldKind::Bool, default_str: "false", usage: "If enabled, use http:// for the S3 endpoint instead of https://. This could be useful in local dev/test environments while using an S3-compatible backend storage, like Minio." },
    SchemaField { name: "signature_version", flag: "ruler-storage.s3.signature-version", kind: FieldKind::String, default_str: "v4", usage: "The signature version to use for authenticating against S3. Supported values are: v4, v2." },
    SchemaField { name: "list_objects_version", flag: "ruler-storage.s3.list-objects-version", kind: FieldKind::String, default_str: "", usage: "Use a specific version of the S3 list object API. Supported values are v1 or v2. Default is unset." },
    SchemaField { name: "bucket_lookup_type", flag: "ruler-storage.s3.bucket-lookup-type", kind: FieldKind::String, default_str: "auto", usage: "Bucket lookup style type, used to access bucket in S3-compatible service. Default is auto. Supported values are: auto, path, virtual-hosted." },
    SchemaField { name: "dualstack_enabled", flag: "ruler-storage.s3.dualstack-enabled", kind: FieldKind::Bool, default_str: "true", usage: "When enabled, direct all AWS S3 requests to the dual-stack IPv4/IPv6 endpoint for the configured region." },
    SchemaField { name: "storage_class", flag: "ruler-storage.s3.storage-class", kind: FieldKind::String, default_str: "", usage: "The S3 storage class to use, not set by default. Details can be found at https://aws.amazon.com/s3/storage-classes/. Supported values are: STANDARD, REDUCED_REDUNDANCY, STANDARD_IA, ONEZONE_IA, INTELLIGENT_TIERING, GLACIER, DEEP_ARCHIVE, OUTPOSTS, GLACIER_IR, SNOW, EXPRESS_ONEZONE, FSX_OPENZFS, FSX_ONTAP" },
    SchemaField { name: "native_aws_auth_enabled", flag: "ruler-storage.s3.native-aws-auth-enabled", kind: FieldKind::Bool, default_str: "false", usage: "If enabled, it will use the default authentication methods of the AWS SDK for go based on known environment variables and known AWS config files." },
    SchemaField { name: "part_size", flag: "ruler-storage.s3.part-size", kind: FieldKind::Int, default_str: "0", usage: "The minimum file size in bytes used for multipart uploads. If 0, the value is optimally computed for each object." },
    SchemaField { name: "send_content_md5", flag: "ruler-storage.s3.send-content-md5", kind: FieldKind::Bool, default_str: "false", usage: "If enabled, a Content-MD5 header is sent with S3 Put Object requests. Consumes more resources to compute the MD5, but may improve compatibility with object storage services that do not support checksums." },
    SchemaField { name: "sts_endpoint", flag: "ruler-storage.s3.sts-endpoint", kind: FieldKind::String, default_str: "", usage: "Accessing S3 resources using temporary, secure credentials provided by AWS Security Token Service." },
];

static BLOCK_101_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "sse", fields: BLOCK_98_FIELDS, blocks: BLOCK_98_BLOCKS },
    SchemaBlock { name: "http", fields: BLOCK_99_FIELDS, blocks: BLOCK_99_BLOCKS },
    SchemaBlock { name: "trace", fields: BLOCK_100_FIELDS, blocks: BLOCK_100_BLOCKS },
];

static BLOCK_102_FIELDS: &[SchemaField] = &[
    SchemaField { name: "idle_conn_timeout", flag: "ruler-storage.gcs.http.idle-conn-timeout", kind: FieldKind::Duration, default_str: "90000000000", usage: "The time an idle connection remains idle before closing." },
    SchemaField { name: "response_header_timeout", flag: "ruler-storage.gcs.http.response-header-timeout", kind: FieldKind::Duration, default_str: "120000000000", usage: "The amount of time the client waits for a server's response headers." },
    SchemaField { name: "insecure_skip_verify", flag: "ruler-storage.gcs.http.insecure-skip-verify", kind: FieldKind::Bool, default_str: "false", usage: "If the client connects to object storage via HTTPS and this option is enabled, the client accepts any certificate and hostname." },
    SchemaField { name: "tls_handshake_timeout", flag: "ruler-storage.gcs.tls-handshake-timeout", kind: FieldKind::Duration, default_str: "10000000000", usage: "Maximum time to wait for a TLS handshake. Set to 0 for no limit." },
    SchemaField { name: "expect_continue_timeout", flag: "ruler-storage.gcs.expect-continue-timeout", kind: FieldKind::Duration, default_str: "1000000000", usage: "The time to wait for a server's first response headers after fully writing the request headers if the request has an Expect header. Set to 0 to send the request body immediately." },
    SchemaField { name: "max_idle_connections", flag: "ruler-storage.gcs.max-idle-connections", kind: FieldKind::Int, default_str: "100", usage: "Maximum number of idle (keep-alive) connections across all hosts. Set to 0 for no limit." },
    SchemaField { name: "max_idle_connections_per_host", flag: "ruler-storage.gcs.max-idle-connections-per-host", kind: FieldKind::Int, default_str: "100", usage: "Maximum number of idle (keep-alive) connections to keep per-host. Set to 0 to use a built-in default value of 2." },
    SchemaField { name: "max_connections_per_host", flag: "ruler-storage.gcs.max-connections-per-host", kind: FieldKind::Int, default_str: "0", usage: "Maximum number of connections per host. Set to 0 for no limit. The limit applies per bucket client, and a single process can create several clients for the same storage configuration, so its total connections to a host can exceed this value." },
    SchemaField { name: "force_attempt_http2", flag: "ruler-storage.gcs.http.force-attempt-http2", kind: FieldKind::Bool, default_str: "false", usage: "If enabled, the HTTP client attempts HTTP/2 for HTTPS connections. Without this option, a client with a custom TLS configuration uses HTTP/1.1." },
    SchemaField { name: "tls_ca_path", flag: "ruler-storage.gcs.http.tls-ca-path", kind: FieldKind::String, default_str: "", usage: "Path to the Certificate Authority (CA) certificates to validate the server certificate. If not set, the host's root CA certificates are used." },
    SchemaField { name: "tls_cert_path", flag: "ruler-storage.gcs.http.tls-cert-path", kind: FieldKind::String, default_str: "", usage: "Path to the client certificate, which is used for authenticating with the server. This setting also requires you to configure the key path." },
    SchemaField { name: "tls_key_path", flag: "ruler-storage.gcs.http.tls-key-path", kind: FieldKind::String, default_str: "", usage: "Path to the key for the client certificate. This setting also requires you to configure the client certificate." },
    SchemaField { name: "tls_server_name", flag: "ruler-storage.gcs.http.tls-server-name", kind: FieldKind::String, default_str: "", usage: "Override the expected name on the server certificate." },
];

static BLOCK_102_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_103_FIELDS: &[SchemaField] = &[
    SchemaField { name: "bucket_name", flag: "ruler-storage.gcs.bucket-name", kind: FieldKind::String, default_str: "", usage: "GCS bucket name" },
    SchemaField { name: "service_account", flag: "ruler-storage.gcs.service-account", kind: FieldKind::Secret, default_str: "", usage: "JSON either from a Google Developers Console client_credentials.json file, or a Google Developers service account key. Needs to be valid JSON, not a filesystem path. If empty, fallback to Google default logic:\n1. A JSON file whose path is specified by the GOOGLE_APPLICATION_CREDENTIALS environment variable. For workload identity federation, refer to https://cloud.google.com/iam/docs/how-to#using-workload-identity-federation on how to generate the JSON configuration file for on-prem/non-Google cloud platforms.\n2. A JSON file in a location known to the gcloud command-line tool: $HOME/.config/gcloud/application_default_credentials.json.\n3. On Google Compute Engine it fetches credentials from the metadata server." },
    SchemaField { name: "enable_upload_retries", flag: "ruler-storage.gcs.enable-upload-retries", kind: FieldKind::Bool, default_str: "true", usage: "Enable automatic retries for GCS uploads using the RetryAlways policy. Uploads will be retried on transient errors. Note: this does not guarantee idempotency." },
    SchemaField { name: "max_retries", flag: "ruler-storage.gcs.max-retries", kind: FieldKind::Int, default_str: "20", usage: "Maximum number of attempts for GCS operations (0 = unlimited, 1 = no retries). Applies to both regular and upload retry modes." },
];

static BLOCK_103_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "http", fields: BLOCK_102_FIELDS, blocks: BLOCK_102_BLOCKS },
];

static BLOCK_104_FIELDS: &[SchemaField] = &[
    SchemaField { name: "idle_conn_timeout", flag: "ruler-storage.azure.http.idle-conn-timeout", kind: FieldKind::Duration, default_str: "90000000000", usage: "The time an idle connection remains idle before closing." },
    SchemaField { name: "response_header_timeout", flag: "ruler-storage.azure.http.response-header-timeout", kind: FieldKind::Duration, default_str: "120000000000", usage: "The amount of time the client waits for a server's response headers." },
    SchemaField { name: "insecure_skip_verify", flag: "ruler-storage.azure.http.insecure-skip-verify", kind: FieldKind::Bool, default_str: "false", usage: "If the client connects to object storage via HTTPS and this option is enabled, the client accepts any certificate and hostname." },
    SchemaField { name: "tls_handshake_timeout", flag: "ruler-storage.azure.tls-handshake-timeout", kind: FieldKind::Duration, default_str: "10000000000", usage: "Maximum time to wait for a TLS handshake. Set to 0 for no limit." },
    SchemaField { name: "expect_continue_timeout", flag: "ruler-storage.azure.expect-continue-timeout", kind: FieldKind::Duration, default_str: "1000000000", usage: "The time to wait for a server's first response headers after fully writing the request headers if the request has an Expect header. Set to 0 to send the request body immediately." },
    SchemaField { name: "max_idle_connections", flag: "ruler-storage.azure.max-idle-connections", kind: FieldKind::Int, default_str: "100", usage: "Maximum number of idle (keep-alive) connections across all hosts. Set to 0 for no limit." },
    SchemaField { name: "max_idle_connections_per_host", flag: "ruler-storage.azure.max-idle-connections-per-host", kind: FieldKind::Int, default_str: "100", usage: "Maximum number of idle (keep-alive) connections to keep per-host. Set to 0 to use a built-in default value of 2." },
    SchemaField { name: "max_connections_per_host", flag: "ruler-storage.azure.max-connections-per-host", kind: FieldKind::Int, default_str: "0", usage: "Maximum number of connections per host. Set to 0 for no limit. The limit applies per bucket client, and a single process can create several clients for the same storage configuration, so its total connections to a host can exceed this value." },
    SchemaField { name: "force_attempt_http2", flag: "ruler-storage.azure.http.force-attempt-http2", kind: FieldKind::Bool, default_str: "false", usage: "If enabled, the HTTP client attempts HTTP/2 for HTTPS connections. Without this option, a client with a custom TLS configuration uses HTTP/1.1." },
    SchemaField { name: "tls_ca_path", flag: "ruler-storage.azure.http.tls-ca-path", kind: FieldKind::String, default_str: "", usage: "Path to the Certificate Authority (CA) certificates to validate the server certificate. If not set, the host's root CA certificates are used." },
    SchemaField { name: "tls_cert_path", flag: "ruler-storage.azure.http.tls-cert-path", kind: FieldKind::String, default_str: "", usage: "Path to the client certificate, which is used for authenticating with the server. This setting also requires you to configure the key path." },
    SchemaField { name: "tls_key_path", flag: "ruler-storage.azure.http.tls-key-path", kind: FieldKind::String, default_str: "", usage: "Path to the key for the client certificate. This setting also requires you to configure the client certificate." },
    SchemaField { name: "tls_server_name", flag: "ruler-storage.azure.http.tls-server-name", kind: FieldKind::String, default_str: "", usage: "Override the expected name on the server certificate." },
];

static BLOCK_104_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_105_FIELDS: &[SchemaField] = &[
    SchemaField { name: "account_name", flag: "ruler-storage.azure.account-name", kind: FieldKind::String, default_str: "", usage: "Azure storage account name" },
    SchemaField { name: "account_key", flag: "ruler-storage.azure.account-key", kind: FieldKind::Secret, default_str: "", usage: "Azure storage account key. If unset, Azure managed identities will be used for authentication instead." },
    SchemaField { name: "connection_string", flag: "ruler-storage.azure.connection-string", kind: FieldKind::Secret, default_str: "", usage: "If `connection-string` is set, the value of `endpoint-suffix` will not be used. Use this method over `account-key` if you need to authenticate via a SAS token. Or if you use the Azurite emulator." },
    SchemaField { name: "container_name", flag: "ruler-storage.azure.container-name", kind: FieldKind::String, default_str: "", usage: "Azure storage container name" },
    SchemaField { name: "endpoint_suffix", flag: "ruler-storage.azure.endpoint-suffix", kind: FieldKind::String, default_str: "", usage: "Azure storage endpoint suffix without schema. The account name will be prefixed to this value to create the FQDN. If set to empty string, default endpoint suffix is used." },
    SchemaField { name: "max_retries", flag: "ruler-storage.azure.max-retries", kind: FieldKind::Int, default_str: "20", usage: "Number of retries for recoverable errors" },
    SchemaField { name: "user_assigned_id", flag: "ruler-storage.azure.user-assigned-id", kind: FieldKind::String, default_str: "", usage: "User assigned managed identity. If empty, then System assigned identity is used." },
];

static BLOCK_105_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "http", fields: BLOCK_104_FIELDS, blocks: BLOCK_104_BLOCKS },
];

static BLOCK_106_FIELDS: &[SchemaField] = &[
    SchemaField { name: "application_credential_id", flag: "ruler-storage.swift.application-credential-id", kind: FieldKind::String, default_str: "", usage: "OpenStack Swift application credential id" },
    SchemaField { name: "application_credential_name", flag: "ruler-storage.swift.application-credential-name", kind: FieldKind::String, default_str: "", usage: "OpenStack Swift application credential name" },
    SchemaField { name: "application_credential_secret", flag: "ruler-storage.swift.application-credential-secret", kind: FieldKind::Secret, default_str: "", usage: "OpenStack Swift application credential secret" },
    SchemaField { name: "auth_version", flag: "ruler-storage.swift.auth-version", kind: FieldKind::Int, default_str: "0", usage: "OpenStack Swift authentication API version. 0 to autodetect." },
    SchemaField { name: "auth_url", flag: "ruler-storage.swift.auth-url", kind: FieldKind::String, default_str: "", usage: "OpenStack Swift authentication URL" },
    SchemaField { name: "username", flag: "ruler-storage.swift.username", kind: FieldKind::String, default_str: "", usage: "OpenStack Swift username." },
    SchemaField { name: "user_domain_name", flag: "ruler-storage.swift.user-domain-name", kind: FieldKind::String, default_str: "", usage: "OpenStack Swift user's domain name." },
    SchemaField { name: "user_domain_id", flag: "ruler-storage.swift.user-domain-id", kind: FieldKind::String, default_str: "", usage: "OpenStack Swift user's domain ID." },
    SchemaField { name: "user_id", flag: "ruler-storage.swift.user-id", kind: FieldKind::String, default_str: "", usage: "OpenStack Swift user ID." },
    SchemaField { name: "password", flag: "ruler-storage.swift.password", kind: FieldKind::Secret, default_str: "", usage: "OpenStack Swift API key." },
    SchemaField { name: "domain_id", flag: "ruler-storage.swift.domain-id", kind: FieldKind::String, default_str: "", usage: "OpenStack Swift user's domain ID." },
    SchemaField { name: "domain_name", flag: "ruler-storage.swift.domain-name", kind: FieldKind::String, default_str: "", usage: "OpenStack Swift user's domain name." },
    SchemaField { name: "project_id", flag: "ruler-storage.swift.project-id", kind: FieldKind::String, default_str: "", usage: "OpenStack Swift project ID (v2,v3 auth only)." },
    SchemaField { name: "project_name", flag: "ruler-storage.swift.project-name", kind: FieldKind::String, default_str: "", usage: "OpenStack Swift project name (v2,v3 auth only)." },
    SchemaField { name: "project_domain_id", flag: "ruler-storage.swift.project-domain-id", kind: FieldKind::String, default_str: "", usage: "ID of the OpenStack Swift project's domain (v3 auth only), only needed if it differs the from user domain." },
    SchemaField { name: "project_domain_name", flag: "ruler-storage.swift.project-domain-name", kind: FieldKind::String, default_str: "", usage: "Name of the OpenStack Swift project's domain (v3 auth only), only needed if it differs from the user domain." },
    SchemaField { name: "region_name", flag: "ruler-storage.swift.region-name", kind: FieldKind::String, default_str: "", usage: "OpenStack Swift Region to use (v2,v3 auth only)." },
    SchemaField { name: "container_name", flag: "ruler-storage.swift.container-name", kind: FieldKind::String, default_str: "", usage: "Name of the OpenStack Swift container to put chunks in." },
    SchemaField { name: "max_retries", flag: "ruler-storage.swift.max-retries", kind: FieldKind::Int, default_str: "3", usage: "Max retries on requests error." },
    SchemaField { name: "connect_timeout", flag: "ruler-storage.swift.connect-timeout", kind: FieldKind::Duration, default_str: "10000000000", usage: "Time after which a connection attempt is aborted." },
    SchemaField { name: "request_timeout", flag: "ruler-storage.swift.request-timeout", kind: FieldKind::Duration, default_str: "5000000000", usage: "Time after which an idle request is aborted. The timeout watchdog is reset each time some data is received, so the timeout triggers after X time no data is received on a request." },
];

static BLOCK_106_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_107_FIELDS: &[SchemaField] = &[
    SchemaField { name: "dir", flag: "ruler-storage.filesystem.dir", kind: FieldKind::String, default_str: "ruler", usage: "Local filesystem storage directory." },
];

static BLOCK_107_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_108_FIELDS: &[SchemaField] = &[
    SchemaField { name: "directory", flag: "ruler-storage.local.directory", kind: FieldKind::String, default_str: "", usage: "Directory to scan for rules" },
];

static BLOCK_108_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_109_FIELDS: &[SchemaField] = &[
    SchemaField { name: "addresses", flag: "ruler-storage.cache.memcached.addresses", kind: FieldKind::String, default_str: "", usage: "Comma-separated list of memcached addresses. Each address can be an IP address, hostname, or an entry specified in the DNS Service Discovery format." },
    SchemaField { name: "addresses_lookup_period", flag: "ruler-storage.cache.memcached.addresses-lookup-period", kind: FieldKind::Duration, default_str: "30000000000", usage: "How often each address is resolved to an IP address or list of host names." },
    SchemaField { name: "addresses_lookup_pool_size", flag: "ruler-storage.cache.memcached.addresses-lookup-pool-size", kind: FieldKind::Int, default_str: "0", usage: "How many idle connections to the DNS resolver are kept open. 0 disables keeping any idle connections open." },
    SchemaField { name: "timeout", flag: "ruler-storage.cache.memcached.timeout", kind: FieldKind::Duration, default_str: "200000000", usage: "The socket read/write timeout." },
    SchemaField { name: "connect_timeout", flag: "ruler-storage.cache.memcached.connect-timeout", kind: FieldKind::Duration, default_str: "200000000", usage: "The connection timeout." },
    SchemaField { name: "min_idle_connections_headroom_percentage", flag: "ruler-storage.cache.memcached.min-idle-connections-headroom-percentage", kind: FieldKind::Float, default_str: "-1", usage: "The minimum number of idle connections to keep open as a percentage (0-100) of the number of recently used idle connections. If negative, idle connections are kept open indefinitely." },
    SchemaField { name: "max_idle_connections", flag: "ruler-storage.cache.memcached.max-idle-connections", kind: FieldKind::Int, default_str: "100", usage: "The maximum number of idle connections that will be maintained per address." },
    SchemaField { name: "max_async_concurrency", flag: "ruler-storage.cache.memcached.max-async-concurrency", kind: FieldKind::Int, default_str: "50", usage: "The maximum number of concurrent asynchronous operations can occur." },
    SchemaField { name: "max_async_buffer_size", flag: "ruler-storage.cache.memcached.max-async-buffer-size", kind: FieldKind::Int, default_str: "25000", usage: "The maximum number of enqueued asynchronous operations allowed." },
    SchemaField { name: "max_get_multi_concurrency", flag: "ruler-storage.cache.memcached.max-get-multi-concurrency", kind: FieldKind::Int, default_str: "100", usage: "The maximum number of concurrent connections running get operations. If set to 0, concurrency is unlimited." },
    SchemaField { name: "max_get_multi_batch_size", flag: "ruler-storage.cache.memcached.max-get-multi-batch-size", kind: FieldKind::Int, default_str: "100", usage: "The maximum number of keys a single underlying get operation should run. If more keys are specified, internally keys are split into multiple batches and fetched concurrently, honoring the max concurrency. If set to 0, the max batch size is unlimited." },
    SchemaField { name: "max_item_size", flag: "ruler-storage.cache.memcached.max-item-size", kind: FieldKind::Int, default_str: "1048576", usage: "The maximum size of an item stored in memcached, in bytes. Bigger items are not stored. If set to 0, no maximum size is enforced." },
    SchemaField { name: "free_list_shards", flag: "ruler-storage.cache.memcached.free-list-shards", kind: FieldKind::Int, default_str: "1", usage: "The number of shards to stripe the free list used for connections in the cache client over. Must be a power of 2. Increasing the number of shards reduces lock contention at the cost of higher or wasted memory." },
    SchemaField { name: "tls_enabled", flag: "ruler-storage.cache.memcached.tls-enabled", kind: FieldKind::Bool, default_str: "false", usage: "Enable connecting to Memcached with TLS." },
    SchemaField { name: "tls_cert_path", flag: "ruler-storage.cache.memcached.tls-cert-path", kind: FieldKind::String, default_str: "", usage: "Path to the client certificate, which will be used for authenticating with the server. Also requires the key path to be configured." },
    SchemaField { name: "tls_key_path", flag: "ruler-storage.cache.memcached.tls-key-path", kind: FieldKind::String, default_str: "", usage: "Path to the key for the client certificate. Also requires the client certificate to be configured." },
    SchemaField { name: "tls_ca_path", flag: "ruler-storage.cache.memcached.tls-ca-path", kind: FieldKind::String, default_str: "", usage: "Path to the CA certificates to validate server certificate against. If not set, the host's root CA certificates are used." },
    SchemaField { name: "tls_server_name", flag: "ruler-storage.cache.memcached.tls-server-name", kind: FieldKind::String, default_str: "", usage: "Override the expected name on the server certificate." },
    SchemaField { name: "tls_insecure_skip_verify", flag: "ruler-storage.cache.memcached.tls-insecure-skip-verify", kind: FieldKind::Bool, default_str: "false", usage: "Skip validating server certificate." },
    SchemaField { name: "tls_cipher_suites", flag: "ruler-storage.cache.memcached.tls-cipher-suites", kind: FieldKind::String, default_str: "", usage: "Override the default cipher suite list (separated by commas). Allowed values:\n\nSecure Ciphers:\n- TLS_AES_128_GCM_SHA256\n- TLS_AES_256_GCM_SHA384\n- TLS_CHACHA20_POLY1305_SHA256\n- TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA\n- TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA\n- TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256\n- TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256\n- TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256\n- TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256\n\nInsecure Ciphers:\n- TLS_RSA_WITH_RC4_128_SHA\n- TLS_RSA_WITH_3DES_EDE_CBC_SHA\n- TLS_RSA_WITH_AES_128_CBC_SHA\n- TLS_RSA_WITH_AES_256_CBC_SHA\n- TLS_RSA_WITH_AES_128_CBC_SHA256\n- TLS_RSA_WITH_AES_128_GCM_SHA256\n- TLS_RSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_ECDSA_WITH_RC4_128_SHA\n- TLS_ECDHE_RSA_WITH_RC4_128_SHA\n- TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256\n- TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256\n" },
    SchemaField { name: "tls_min_version", flag: "ruler-storage.cache.memcached.tls-min-version", kind: FieldKind::String, default_str: "", usage: "Override the default minimum TLS version. Allowed values: VersionTLS10, VersionTLS11, VersionTLS12, VersionTLS13" },
];

static BLOCK_109_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_110_FIELDS: &[SchemaField] = &[
    SchemaField { name: "backend", flag: "ruler-storage.cache.backend", kind: FieldKind::String, default_str: "", usage: "Backend for ruler storage cache, if not empty. The cache is supported for any storage backend except \"local\". Supported values: memcached." },
];

static BLOCK_110_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "memcached", fields: BLOCK_109_FIELDS, blocks: BLOCK_109_BLOCKS },
];

static BLOCK_111_FIELDS: &[SchemaField] = &[
    SchemaField { name: "backend", flag: "ruler-storage.backend", kind: FieldKind::String, default_str: "filesystem", usage: "Backend storage to use. Supported backends are: s3, gcs, azure, swift, filesystem, local." },
    SchemaField { name: "storage_prefix", flag: "ruler-storage.storage-prefix", kind: FieldKind::String, default_str: "", usage: "Prefix for all objects stored in the backend storage. For simplicity, it may only contain digits and English alphabet letters." },
];

static BLOCK_111_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "s3", fields: BLOCK_101_FIELDS, blocks: BLOCK_101_BLOCKS },
    SchemaBlock { name: "gcs", fields: BLOCK_103_FIELDS, blocks: BLOCK_103_BLOCKS },
    SchemaBlock { name: "azure", fields: BLOCK_105_FIELDS, blocks: BLOCK_105_BLOCKS },
    SchemaBlock { name: "swift", fields: BLOCK_106_FIELDS, blocks: BLOCK_106_BLOCKS },
    SchemaBlock { name: "filesystem", fields: BLOCK_107_FIELDS, blocks: BLOCK_107_BLOCKS },
    SchemaBlock { name: "local", fields: BLOCK_108_FIELDS, blocks: BLOCK_108_BLOCKS },
    SchemaBlock { name: "cache", fields: BLOCK_110_FIELDS, blocks: BLOCK_110_BLOCKS },
];

static BLOCK_112_FIELDS: &[SchemaField] = &[
    SchemaField { name: "host", flag: "alertmanager.sharding-ring.consul.hostname", kind: FieldKind::String, default_str: "localhost:8500", usage: "Hostname and port of Consul." },
    SchemaField { name: "acl_token", flag: "alertmanager.sharding-ring.consul.acl-token", kind: FieldKind::Secret, default_str: "", usage: "ACL Token used to interact with Consul." },
    SchemaField { name: "http_client_timeout", flag: "alertmanager.sharding-ring.consul.client-timeout", kind: FieldKind::Duration, default_str: "20000000000", usage: "HTTP timeout when talking to Consul" },
    SchemaField { name: "consistent_reads", flag: "alertmanager.sharding-ring.consul.consistent-reads", kind: FieldKind::Bool, default_str: "false", usage: "Enable consistent reads to Consul." },
    SchemaField { name: "watch_rate_limit", flag: "alertmanager.sharding-ring.consul.watch-rate-limit", kind: FieldKind::Float, default_str: "1", usage: "Rate limit when watching key or prefix in Consul, in requests per second. 0 disables the rate limit." },
    SchemaField { name: "watch_burst_size", flag: "alertmanager.sharding-ring.consul.watch-burst-size", kind: FieldKind::Int, default_str: "1", usage: "Burst size used in rate limit. Values less than 1 are treated as 1." },
    SchemaField { name: "cas_retry_delay", flag: "alertmanager.sharding-ring.consul.cas-retry-delay", kind: FieldKind::Duration, default_str: "1000000000", usage: "Maximum duration to wait before retrying a Compare And Swap (CAS) operation." },
];

static BLOCK_112_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_113_FIELDS: &[SchemaField] = &[
    SchemaField { name: "endpoints", flag: "alertmanager.sharding-ring.etcd.endpoints", kind: FieldKind::StringList, default_str: "", usage: "The etcd endpoints to connect to." },
    SchemaField { name: "dial_timeout", flag: "alertmanager.sharding-ring.etcd.dial-timeout", kind: FieldKind::Duration, default_str: "10000000000", usage: "The dial timeout for the etcd connection." },
    SchemaField { name: "max_retries", flag: "alertmanager.sharding-ring.etcd.max-retries", kind: FieldKind::Int, default_str: "10", usage: "The maximum number of retries to do for failed ops." },
    SchemaField { name: "tls_enabled", flag: "alertmanager.sharding-ring.etcd.tls-enabled", kind: FieldKind::Bool, default_str: "false", usage: "Enable TLS." },
    SchemaField { name: "tls_cert_path", flag: "alertmanager.sharding-ring.etcd.tls-cert-path", kind: FieldKind::String, default_str: "", usage: "Path to the client certificate, which will be used for authenticating with the server. Also requires the key path to be configured." },
    SchemaField { name: "tls_key_path", flag: "alertmanager.sharding-ring.etcd.tls-key-path", kind: FieldKind::String, default_str: "", usage: "Path to the key for the client certificate. Also requires the client certificate to be configured." },
    SchemaField { name: "tls_ca_path", flag: "alertmanager.sharding-ring.etcd.tls-ca-path", kind: FieldKind::String, default_str: "", usage: "Path to the CA certificates to validate server certificate against. If not set, the host's root CA certificates are used." },
    SchemaField { name: "tls_server_name", flag: "alertmanager.sharding-ring.etcd.tls-server-name", kind: FieldKind::String, default_str: "", usage: "Override the expected name on the server certificate." },
    SchemaField { name: "tls_insecure_skip_verify", flag: "alertmanager.sharding-ring.etcd.tls-insecure-skip-verify", kind: FieldKind::Bool, default_str: "false", usage: "Skip validating server certificate." },
    SchemaField { name: "tls_cipher_suites", flag: "alertmanager.sharding-ring.etcd.tls-cipher-suites", kind: FieldKind::String, default_str: "", usage: "Override the default cipher suite list (separated by commas). Allowed values:\n\nSecure Ciphers:\n- TLS_AES_128_GCM_SHA256\n- TLS_AES_256_GCM_SHA384\n- TLS_CHACHA20_POLY1305_SHA256\n- TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA\n- TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA\n- TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256\n- TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256\n- TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256\n- TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256\n\nInsecure Ciphers:\n- TLS_RSA_WITH_RC4_128_SHA\n- TLS_RSA_WITH_3DES_EDE_CBC_SHA\n- TLS_RSA_WITH_AES_128_CBC_SHA\n- TLS_RSA_WITH_AES_256_CBC_SHA\n- TLS_RSA_WITH_AES_128_CBC_SHA256\n- TLS_RSA_WITH_AES_128_GCM_SHA256\n- TLS_RSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_ECDSA_WITH_RC4_128_SHA\n- TLS_ECDHE_RSA_WITH_RC4_128_SHA\n- TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256\n- TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256\n" },
    SchemaField { name: "tls_min_version", flag: "alertmanager.sharding-ring.etcd.tls-min-version", kind: FieldKind::String, default_str: "", usage: "Override the default minimum TLS version. Allowed values: VersionTLS10, VersionTLS11, VersionTLS12, VersionTLS13" },
    SchemaField { name: "username", flag: "alertmanager.sharding-ring.etcd.username", kind: FieldKind::String, default_str: "", usage: "Etcd username." },
    SchemaField { name: "password", flag: "alertmanager.sharding-ring.etcd.password", kind: FieldKind::Secret, default_str: "", usage: "Etcd password." },
];

static BLOCK_113_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_114_FIELDS: &[SchemaField] = &[
    SchemaField { name: "primary", flag: "alertmanager.sharding-ring.multi.primary", kind: FieldKind::String, default_str: "", usage: "Primary backend storage used by multi-client." },
    SchemaField { name: "secondary", flag: "alertmanager.sharding-ring.multi.secondary", kind: FieldKind::String, default_str: "", usage: "Secondary backend storage used by multi-client." },
    SchemaField { name: "mirror_enabled", flag: "alertmanager.sharding-ring.multi.mirror-enabled", kind: FieldKind::Bool, default_str: "false", usage: "Mirror writes to the secondary store." },
    SchemaField { name: "mirror_timeout", flag: "alertmanager.sharding-ring.multi.mirror-timeout", kind: FieldKind::Duration, default_str: "2000000000", usage: "Timeout for storing a value to the secondary store." },
];

static BLOCK_114_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_115_FIELDS: &[SchemaField] = &[
    SchemaField { name: "store", flag: "alertmanager.sharding-ring.store", kind: FieldKind::String, default_str: "memberlist", usage: "Backend storage to use for the ring. Supported values are: consul, etcd, inmemory, memberlist, multi." },
    SchemaField { name: "prefix", flag: "alertmanager.sharding-ring.prefix", kind: FieldKind::String, default_str: "alertmanagers/", usage: "The prefix for the keys in the store. Should end with a /." },
];

static BLOCK_115_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "consul", fields: BLOCK_112_FIELDS, blocks: BLOCK_112_BLOCKS },
    SchemaBlock { name: "etcd", fields: BLOCK_113_FIELDS, blocks: BLOCK_113_BLOCKS },
    SchemaBlock { name: "multi", fields: BLOCK_114_FIELDS, blocks: BLOCK_114_BLOCKS },
];

static BLOCK_116_FIELDS: &[SchemaField] = &[
    SchemaField { name: "heartbeat_period", flag: "alertmanager.sharding-ring.heartbeat-period", kind: FieldKind::Duration, default_str: "15000000000", usage: "Period at which to heartbeat to the ring." },
    SchemaField { name: "heartbeat_timeout", flag: "alertmanager.sharding-ring.heartbeat-timeout", kind: FieldKind::Duration, default_str: "60000000000", usage: "Heartbeat timeout after which Mimir marks alertmanagers as unhealthy in the ring." },
    SchemaField { name: "instance_id", flag: "alertmanager.sharding-ring.instance-id", kind: FieldKind::String, default_str: "<hostname>", usage: "Instance ID to register in the ring." },
    SchemaField { name: "instance_interface_names", flag: "alertmanager.sharding-ring.instance-interface-names", kind: FieldKind::StringList, default_str: "", usage: "List of network interface names to look up when finding the instance IP address." },
    SchemaField { name: "instance_port", flag: "alertmanager.sharding-ring.instance-port", kind: FieldKind::Int, default_str: "0", usage: "Port to advertise in the ring (defaults to -server.grpc-listen-port)." },
    SchemaField { name: "instance_addr", flag: "alertmanager.sharding-ring.instance-addr", kind: FieldKind::String, default_str: "", usage: "IP address to advertise in the ring. Default is auto-detected." },
    SchemaField { name: "instance_enable_ipv6", flag: "alertmanager.sharding-ring.instance-enable-ipv6", kind: FieldKind::Bool, default_str: "false", usage: "Enable using an IPv6 instance address." },
    SchemaField { name: "replication_factor", flag: "alertmanager.sharding-ring.replication-factor", kind: FieldKind::Int, default_str: "3", usage: "The replication factor to use when sharding the alertmanager." },
    SchemaField { name: "zone_awareness_enabled", flag: "alertmanager.sharding-ring.zone-awareness-enabled", kind: FieldKind::Bool, default_str: "false", usage: "True to enable zone-awareness and replicate alerts across different availability zones." },
    SchemaField { name: "instance_availability_zone", flag: "alertmanager.sharding-ring.instance-availability-zone", kind: FieldKind::String, default_str: "", usage: "The availability zone where this instance is running. Required if zone-awareness is enabled." },
];

static BLOCK_116_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "kvstore", fields: BLOCK_115_FIELDS, blocks: BLOCK_115_BLOCKS },
];

static BLOCK_117_FIELDS: &[SchemaField] = &[
    SchemaField { name: "min_period", flag: "alertmanager.alertmanager-client.backoff-min-period", kind: FieldKind::Duration, default_str: "100000000", usage: "Minimum delay when backing off." },
    SchemaField { name: "max_period", flag: "alertmanager.alertmanager-client.backoff-max-period", kind: FieldKind::Duration, default_str: "10000000000", usage: "Maximum delay when backing off." },
    SchemaField { name: "max_retries", flag: "alertmanager.alertmanager-client.backoff-retries", kind: FieldKind::Int, default_str: "10", usage: "Number of times to backoff and retry before failing." },
];

static BLOCK_117_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_118_FIELDS: &[SchemaField] = &[
    SchemaField { name: "label", flag: "alertmanager.alertmanager-client.cluster-validation.label", kind: FieldKind::String, default_str: "", usage: "Primary cluster validation label." },
];

static BLOCK_118_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_119_FIELDS: &[SchemaField] = &[
    SchemaField { name: "remote_timeout", flag: "alertmanager.alertmanager-client.remote-timeout", kind: FieldKind::Duration, default_str: "2000000000", usage: "Timeout for downstream alertmanagers." },
    SchemaField { name: "max_recv_msg_size", flag: "alertmanager.alertmanager-client.grpc-max-recv-msg-size", kind: FieldKind::Int, default_str: "104857600", usage: "gRPC client max receive message size (bytes)." },
    SchemaField { name: "max_send_msg_size", flag: "alertmanager.alertmanager-client.grpc-max-send-msg-size", kind: FieldKind::Int, default_str: "104857600", usage: "gRPC client max send message size (bytes)." },
    SchemaField { name: "grpc_compression", flag: "alertmanager.alertmanager-client.grpc-compression", kind: FieldKind::String, default_str: "", usage: "Use compression when sending messages. Supported values are: 'gzip', 'snappy', 's2' and '' (disable compression)" },
    SchemaField { name: "rate_limit", flag: "alertmanager.alertmanager-client.grpc-client-rate-limit", kind: FieldKind::Float, default_str: "0", usage: "Rate limit for gRPC client; 0 means disabled." },
    SchemaField { name: "rate_limit_burst", flag: "alertmanager.alertmanager-client.grpc-client-rate-limit-burst", kind: FieldKind::Int, default_str: "0", usage: "Rate limit burst for gRPC client." },
    SchemaField { name: "backoff_on_ratelimits", flag: "alertmanager.alertmanager-client.backoff-on-ratelimits", kind: FieldKind::Bool, default_str: "false", usage: "Enable backoff and retry when we hit rate limits." },
    SchemaField { name: "initial_stream_window_size", flag: "alertmanager.alertmanager-client.initial-stream-window-size", kind: FieldKind::Bytes, default_str: "63KiB1023B", usage: "Initial stream window size. Values less than the default are not supported and are ignored. Setting this to a value other than the default disables the BDP estimator." },
    SchemaField { name: "initial_connection_window_size", flag: "alertmanager.alertmanager-client.initial-connection-window-size", kind: FieldKind::Bytes, default_str: "63KiB1023B", usage: "Initial connection window size. Values less than the default are not supported and are ignored. Setting this to a value other than the default disables the BDP estimator." },
    SchemaField { name: "tls_enabled", flag: "alertmanager.alertmanager-client.tls-enabled", kind: FieldKind::Bool, default_str: "false", usage: "Enable TLS in the gRPC client. This flag needs to be enabled when any other TLS flag is set. If set to false, insecure connection to gRPC server will be used." },
    SchemaField { name: "tls_cert_path", flag: "alertmanager.alertmanager-client.tls-cert-path", kind: FieldKind::String, default_str: "", usage: "Path to the client certificate, which will be used for authenticating with the server. Also requires the key path to be configured." },
    SchemaField { name: "tls_key_path", flag: "alertmanager.alertmanager-client.tls-key-path", kind: FieldKind::String, default_str: "", usage: "Path to the key for the client certificate. Also requires the client certificate to be configured." },
    SchemaField { name: "tls_ca_path", flag: "alertmanager.alertmanager-client.tls-ca-path", kind: FieldKind::String, default_str: "", usage: "Path to the CA certificates to validate server certificate against. If not set, the host's root CA certificates are used." },
    SchemaField { name: "tls_server_name", flag: "alertmanager.alertmanager-client.tls-server-name", kind: FieldKind::String, default_str: "", usage: "Override the expected name on the server certificate." },
    SchemaField { name: "tls_insecure_skip_verify", flag: "alertmanager.alertmanager-client.tls-insecure-skip-verify", kind: FieldKind::Bool, default_str: "false", usage: "Skip validating server certificate." },
    SchemaField { name: "tls_cipher_suites", flag: "alertmanager.alertmanager-client.tls-cipher-suites", kind: FieldKind::String, default_str: "", usage: "Override the default cipher suite list (separated by commas). Allowed values:\n\nSecure Ciphers:\n- TLS_AES_128_GCM_SHA256\n- TLS_AES_256_GCM_SHA384\n- TLS_CHACHA20_POLY1305_SHA256\n- TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA\n- TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA\n- TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256\n- TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256\n- TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256\n- TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256\n\nInsecure Ciphers:\n- TLS_RSA_WITH_RC4_128_SHA\n- TLS_RSA_WITH_3DES_EDE_CBC_SHA\n- TLS_RSA_WITH_AES_128_CBC_SHA\n- TLS_RSA_WITH_AES_256_CBC_SHA\n- TLS_RSA_WITH_AES_128_CBC_SHA256\n- TLS_RSA_WITH_AES_128_GCM_SHA256\n- TLS_RSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_ECDSA_WITH_RC4_128_SHA\n- TLS_ECDHE_RSA_WITH_RC4_128_SHA\n- TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256\n- TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256\n" },
    SchemaField { name: "tls_min_version", flag: "alertmanager.alertmanager-client.tls-min-version", kind: FieldKind::String, default_str: "", usage: "Override the default minimum TLS version. Allowed values: VersionTLS10, VersionTLS11, VersionTLS12, VersionTLS13" },
    SchemaField { name: "connect_timeout", flag: "alertmanager.alertmanager-client.connect-timeout", kind: FieldKind::Duration, default_str: "5000000000", usage: "The maximum amount of time to establish a connection. A value of 0 means default gRPC client connect timeout and backoff." },
    SchemaField { name: "connect_backoff_base_delay", flag: "alertmanager.alertmanager-client.connect-backoff-base-delay", kind: FieldKind::Duration, default_str: "1000000000", usage: "Initial backoff delay after first connection failure. Only relevant if ConnectTimeout > 0." },
    SchemaField { name: "connect_backoff_max_delay", flag: "alertmanager.alertmanager-client.connect-backoff-max-delay", kind: FieldKind::Duration, default_str: "5000000000", usage: "Maximum backoff delay when establishing a connection. Only relevant if ConnectTimeout > 0." },
    SchemaField { name: "keepalive_time", flag: "alertmanager.alertmanager-client.keepalive-time", kind: FieldKind::Duration, default_str: "20000000000", usage: "After a duration of this time if the client doesn't see any activity it pings the server to see if the transport is still alive. This also determines the socket's TCP_USER_TIMEOUT together with keepalive-timeout." },
    SchemaField { name: "keepalive_timeout", flag: "alertmanager.alertmanager-client.keepalive-timeout", kind: FieldKind::Duration, default_str: "10000000000", usage: "After having pinged for keepalive check, the client waits for a duration of this time and if no activity is seen even after that the connection is closed." },
    SchemaField { name: "health_check_grace_period", flag: "alertmanager.alertmanager-client.health-check-grace-period", kind: FieldKind::Duration, default_str: "0", usage: "The grace period for health checks. If an alertmanager connection consistently fails health checks for this period, any open connections are closed. The alertmanager will attempt to reconnect to that alertmanager if a subsequent request is made to that alertmanager. Set to 0 to immediately remove alertmanager connections on the first health check failure." },
];

static BLOCK_119_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "backoff_config", fields: BLOCK_117_FIELDS, blocks: BLOCK_117_BLOCKS },
    SchemaBlock { name: "cluster_validation", fields: BLOCK_118_FIELDS, blocks: BLOCK_118_BLOCKS },
];

static BLOCK_120_FIELDS: &[SchemaField] = &[
    SchemaField { name: "data_dir", flag: "alertmanager.storage.path", kind: FieldKind::String, default_str: "./data-alertmanager/", usage: "Directory to store Alertmanager state and temporarily configuration files. The content of this directory is not required to be persisted between restarts unless Alertmanager replication has been disabled." },
    SchemaField { name: "retention", flag: "alertmanager.storage.retention", kind: FieldKind::Duration, default_str: "432000000000000", usage: "How long should we store stateful data (notification logs and silences). For notification log entries, refers to how long should we keep entries before they expire and are deleted. For silences, refers to how long should tenants view silences after they expire and are deleted." },
    SchemaField { name: "external_url", flag: "alertmanager.web.external-url", kind: FieldKind::Other, default_str: "http://localhost:8080/alertmanager", usage: "The URL under which Alertmanager is externally reachable (eg. could be different than -http.alertmanager-http-prefix in case Alertmanager is served via a reverse proxy). This setting is used both to configure the internal requests router and to generate links in alert templates. If the external URL has a path portion, it will be used to prefix all HTTP endpoints served by Alertmanager, both the UI and API." },
    SchemaField { name: "poll_interval", flag: "alertmanager.configs.poll-interval", kind: FieldKind::Duration, default_str: "15000000000", usage: "How frequently to poll Alertmanager configs." },
    SchemaField { name: "max_recv_msg_size", flag: "alertmanager.max-recv-msg-size", kind: FieldKind::Int, default_str: "104857600", usage: "Maximum size (bytes) of an accepted HTTP request body." },
    SchemaField { name: "state_read_timeout", flag: "alertmanager.storage.state-read-timeout", kind: FieldKind::Duration, default_str: "15000000000", usage: "Timeout for reading the state from object storage during the initial sync. Set to `0` for no timeout." },
    SchemaField { name: "fallback_config_file", flag: "alertmanager.configs.fallback", kind: FieldKind::String, default_str: "", usage: "Filename of fallback config to use if none specified for instance." },
    SchemaField { name: "peer_timeout", flag: "alertmanager.peer-timeout", kind: FieldKind::Duration, default_str: "15000000000", usage: "Time to wait between peers to send notifications." },
    SchemaField { name: "enable_api", flag: "alertmanager.enable-api", kind: FieldKind::Bool, default_str: "true", usage: "Enable the alertmanager config API." },
    SchemaField { name: "strict_initialization_idle_grace_period", flag: "alertmanager.strict-initialization-idle-grace-period", kind: FieldKind::Duration, default_str: "300000000000", usage: "Duration to wait before shutting down an idle Alertmanager using a default/empty configuration when strict initialization is enabled." },
    SchemaField { name: "max_concurrent_get_requests_per_tenant", flag: "alertmanager.max-concurrent-get-requests-per-tenant", kind: FieldKind::Int, default_str: "0", usage: "Maximum number of concurrent GET requests allowed per tenant. The zero value (and negative values) result in a limit of GOMAXPROCS or 8, whichever is larger. Status code 503 is served for GET requests that would exceed the concurrency limit." },
    SchemaField { name: "persist_interval", flag: "alertmanager.persist-interval", kind: FieldKind::Duration, default_str: "900000000000", usage: "The interval between persisting the current alertmanager state (notification log and silences) to object storage. This is only used when sharding is enabled. This state is read when all replicas for a shard can not be contacted. In this scenario, having persisted the state more frequently will result in potentially fewer lost silences, and fewer duplicate notifications." },
    SchemaField { name: "enable_state_cleanup", flag: "alertmanager.enable-state-cleanup", kind: FieldKind::Bool, default_str: "true", usage: "Enables periodic cleanup of alertmanager stateful data (notification logs and silences) from object storage. When enabled, data is removed for any tenant that does not have a configuration." },
    SchemaField { name: "strict_initialization", flag: "alertmanager.strict-initialization-enabled", kind: FieldKind::Bool, default_str: "false", usage: "Skip initializing Alertmanagers for tenants without a non-default, non-empty configuration. For Grafana Alertmanager tenants, configurations not marked as 'promoted' will also be skipped." },
    SchemaField { name: "utf8_strict_mode", flag: "alertmanager.utf8-strict-mode-enabled", kind: FieldKind::Bool, default_str: "false", usage: "Enable UTF-8 strict mode. Allows UTF-8 characters in the matchers for routes and inhibition rules, in silences, and in the labels for alerts. It is recommended that all tenants run the `migrate-utf8` command in mimirtool before enabling this mode. Otherwise, some tenant configurations might fail to load. For more information, refer to [Enable UTF-8](https://grafana.com/docs/mimir/<MIMIR_VERSION>/references/architecture/components/alertmanager/#enable-utf-8)." },
    SchemaField { name: "log_parsing_label_matchers", flag: "alertmanager.log-parsing-label-matchers", kind: FieldKind::Bool, default_str: "false", usage: "Enable logging when parsing label matchers. This flag is intended to be used with -alertmanager.utf8-strict-mode-enabled to validate UTF-8 strict mode is working as intended." },
    SchemaField { name: "utf8_migration_logging", flag: "alertmanager.utf8-migration-logging-enabled", kind: FieldKind::Bool, default_str: "false", usage: "Enable logging of tenant configurations that are incompatible with UTF-8 strict mode." },
    SchemaField { name: "require_explicit_sns_credentials", flag: "alertmanager.require-explicit-sns-credentials", kind: FieldKind::Bool, default_str: "true", usage: "If true, SNS receivers must have explicit credentials configured, rather than inheriting any AWS credentials available in the environment. Note that setting this value to false could allow anyone with access to configure SNS receivers to send credentials to an arbitrary endpoint." },
    SchemaField { name: "tenants_exempt_from_config_validation_for_existing_config", flag: "alertmanager.tenants-exempt-from-config-validation-for-existing-config", kind: FieldKind::StringList, default_str: "", usage: "List of tenants that are exempt from config validation for existing config. Newly uploaded config will be required to pass validation." },
];

static BLOCK_120_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "sharding_ring", fields: BLOCK_116_FIELDS, blocks: BLOCK_116_BLOCKS },
    SchemaBlock { name: "alertmanager_client", fields: BLOCK_119_FIELDS, blocks: BLOCK_119_BLOCKS },
];

static BLOCK_121_FIELDS: &[SchemaField] = &[
    SchemaField { name: "type", flag: "alertmanager-storage.s3.sse.type", kind: FieldKind::String, default_str: "", usage: "Enable AWS Server Side Encryption. Supported values: SSE-KMS, SSE-S3." },
    SchemaField { name: "kms_key_id", flag: "alertmanager-storage.s3.sse.kms-key-id", kind: FieldKind::String, default_str: "", usage: "KMS Key ID used to encrypt objects in S3" },
    SchemaField { name: "kms_encryption_context", flag: "alertmanager-storage.s3.sse.kms-encryption-context", kind: FieldKind::String, default_str: "", usage: "KMS Encryption Context used for object encryption. It expects JSON formatted string." },
];

static BLOCK_121_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_122_FIELDS: &[SchemaField] = &[
    SchemaField { name: "idle_conn_timeout", flag: "alertmanager-storage.s3.http.idle-conn-timeout", kind: FieldKind::Duration, default_str: "90000000000", usage: "The time an idle connection remains idle before closing." },
    SchemaField { name: "response_header_timeout", flag: "alertmanager-storage.s3.http.response-header-timeout", kind: FieldKind::Duration, default_str: "120000000000", usage: "The amount of time the client waits for a server's response headers." },
    SchemaField { name: "insecure_skip_verify", flag: "alertmanager-storage.s3.http.insecure-skip-verify", kind: FieldKind::Bool, default_str: "false", usage: "If the client connects to object storage via HTTPS and this option is enabled, the client accepts any certificate and hostname." },
    SchemaField { name: "tls_handshake_timeout", flag: "alertmanager-storage.s3.tls-handshake-timeout", kind: FieldKind::Duration, default_str: "10000000000", usage: "Maximum time to wait for a TLS handshake. Set to 0 for no limit." },
    SchemaField { name: "expect_continue_timeout", flag: "alertmanager-storage.s3.expect-continue-timeout", kind: FieldKind::Duration, default_str: "1000000000", usage: "The time to wait for a server's first response headers after fully writing the request headers if the request has an Expect header. Set to 0 to send the request body immediately." },
    SchemaField { name: "max_idle_connections", flag: "alertmanager-storage.s3.max-idle-connections", kind: FieldKind::Int, default_str: "100", usage: "Maximum number of idle (keep-alive) connections across all hosts. Set to 0 for no limit." },
    SchemaField { name: "max_idle_connections_per_host", flag: "alertmanager-storage.s3.max-idle-connections-per-host", kind: FieldKind::Int, default_str: "100", usage: "Maximum number of idle (keep-alive) connections to keep per-host. Set to 0 to use a built-in default value of 2." },
    SchemaField { name: "max_connections_per_host", flag: "alertmanager-storage.s3.max-connections-per-host", kind: FieldKind::Int, default_str: "0", usage: "Maximum number of connections per host. Set to 0 for no limit. The limit applies per bucket client, and a single process can create several clients for the same storage configuration, so its total connections to a host can exceed this value." },
    SchemaField { name: "force_attempt_http2", flag: "alertmanager-storage.s3.http.force-attempt-http2", kind: FieldKind::Bool, default_str: "false", usage: "If enabled, the HTTP client attempts HTTP/2 for HTTPS connections. Without this option, a client with a custom TLS configuration uses HTTP/1.1." },
    SchemaField { name: "tls_ca_path", flag: "alertmanager-storage.s3.http.tls-ca-path", kind: FieldKind::String, default_str: "", usage: "Path to the Certificate Authority (CA) certificates to validate the server certificate. If not set, the host's root CA certificates are used." },
    SchemaField { name: "tls_cert_path", flag: "alertmanager-storage.s3.http.tls-cert-path", kind: FieldKind::String, default_str: "", usage: "Path to the client certificate, which is used for authenticating with the server. This setting also requires you to configure the key path." },
    SchemaField { name: "tls_key_path", flag: "alertmanager-storage.s3.http.tls-key-path", kind: FieldKind::String, default_str: "", usage: "Path to the key for the client certificate. This setting also requires you to configure the client certificate." },
    SchemaField { name: "tls_server_name", flag: "alertmanager-storage.s3.http.tls-server-name", kind: FieldKind::String, default_str: "", usage: "Override the expected name on the server certificate." },
];

static BLOCK_122_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_123_FIELDS: &[SchemaField] = &[
    SchemaField { name: "enabled", flag: "alertmanager-storage.s3.trace.enabled", kind: FieldKind::Bool, default_str: "false", usage: "When enabled, low-level S3 HTTP operation information is logged at the debug level." },
];

static BLOCK_123_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_124_FIELDS: &[SchemaField] = &[
    SchemaField { name: "endpoint", flag: "alertmanager-storage.s3.endpoint", kind: FieldKind::String, default_str: "", usage: "The S3 bucket endpoint. It could be an AWS S3 endpoint listed at https://docs.aws.amazon.com/general/latest/gr/s3.html or the address of an S3-compatible service in hostname:port format." },
    SchemaField { name: "region", flag: "alertmanager-storage.s3.region", kind: FieldKind::String, default_str: "", usage: "S3 region. If unset, the client will issue a S3 GetBucketLocation API call to autodetect it." },
    SchemaField { name: "bucket_name", flag: "alertmanager-storage.s3.bucket-name", kind: FieldKind::String, default_str: "", usage: "S3 bucket name" },
    SchemaField { name: "secret_access_key", flag: "alertmanager-storage.s3.secret-access-key", kind: FieldKind::Secret, default_str: "", usage: "S3 secret access key" },
    SchemaField { name: "access_key_id", flag: "alertmanager-storage.s3.access-key-id", kind: FieldKind::String, default_str: "", usage: "S3 access key ID" },
    SchemaField { name: "session_token", flag: "alertmanager-storage.s3.session-token", kind: FieldKind::Secret, default_str: "", usage: "S3 session token" },
    SchemaField { name: "insecure", flag: "alertmanager-storage.s3.insecure", kind: FieldKind::Bool, default_str: "false", usage: "If enabled, use http:// for the S3 endpoint instead of https://. This could be useful in local dev/test environments while using an S3-compatible backend storage, like Minio." },
    SchemaField { name: "signature_version", flag: "alertmanager-storage.s3.signature-version", kind: FieldKind::String, default_str: "v4", usage: "The signature version to use for authenticating against S3. Supported values are: v4, v2." },
    SchemaField { name: "list_objects_version", flag: "alertmanager-storage.s3.list-objects-version", kind: FieldKind::String, default_str: "", usage: "Use a specific version of the S3 list object API. Supported values are v1 or v2. Default is unset." },
    SchemaField { name: "bucket_lookup_type", flag: "alertmanager-storage.s3.bucket-lookup-type", kind: FieldKind::String, default_str: "auto", usage: "Bucket lookup style type, used to access bucket in S3-compatible service. Default is auto. Supported values are: auto, path, virtual-hosted." },
    SchemaField { name: "dualstack_enabled", flag: "alertmanager-storage.s3.dualstack-enabled", kind: FieldKind::Bool, default_str: "true", usage: "When enabled, direct all AWS S3 requests to the dual-stack IPv4/IPv6 endpoint for the configured region." },
    SchemaField { name: "storage_class", flag: "alertmanager-storage.s3.storage-class", kind: FieldKind::String, default_str: "", usage: "The S3 storage class to use, not set by default. Details can be found at https://aws.amazon.com/s3/storage-classes/. Supported values are: STANDARD, REDUCED_REDUNDANCY, STANDARD_IA, ONEZONE_IA, INTELLIGENT_TIERING, GLACIER, DEEP_ARCHIVE, OUTPOSTS, GLACIER_IR, SNOW, EXPRESS_ONEZONE, FSX_OPENZFS, FSX_ONTAP" },
    SchemaField { name: "native_aws_auth_enabled", flag: "alertmanager-storage.s3.native-aws-auth-enabled", kind: FieldKind::Bool, default_str: "false", usage: "If enabled, it will use the default authentication methods of the AWS SDK for go based on known environment variables and known AWS config files." },
    SchemaField { name: "part_size", flag: "alertmanager-storage.s3.part-size", kind: FieldKind::Int, default_str: "0", usage: "The minimum file size in bytes used for multipart uploads. If 0, the value is optimally computed for each object." },
    SchemaField { name: "send_content_md5", flag: "alertmanager-storage.s3.send-content-md5", kind: FieldKind::Bool, default_str: "false", usage: "If enabled, a Content-MD5 header is sent with S3 Put Object requests. Consumes more resources to compute the MD5, but may improve compatibility with object storage services that do not support checksums." },
    SchemaField { name: "sts_endpoint", flag: "alertmanager-storage.s3.sts-endpoint", kind: FieldKind::String, default_str: "", usage: "Accessing S3 resources using temporary, secure credentials provided by AWS Security Token Service." },
];

static BLOCK_124_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "sse", fields: BLOCK_121_FIELDS, blocks: BLOCK_121_BLOCKS },
    SchemaBlock { name: "http", fields: BLOCK_122_FIELDS, blocks: BLOCK_122_BLOCKS },
    SchemaBlock { name: "trace", fields: BLOCK_123_FIELDS, blocks: BLOCK_123_BLOCKS },
];

static BLOCK_125_FIELDS: &[SchemaField] = &[
    SchemaField { name: "idle_conn_timeout", flag: "alertmanager-storage.gcs.http.idle-conn-timeout", kind: FieldKind::Duration, default_str: "90000000000", usage: "The time an idle connection remains idle before closing." },
    SchemaField { name: "response_header_timeout", flag: "alertmanager-storage.gcs.http.response-header-timeout", kind: FieldKind::Duration, default_str: "120000000000", usage: "The amount of time the client waits for a server's response headers." },
    SchemaField { name: "insecure_skip_verify", flag: "alertmanager-storage.gcs.http.insecure-skip-verify", kind: FieldKind::Bool, default_str: "false", usage: "If the client connects to object storage via HTTPS and this option is enabled, the client accepts any certificate and hostname." },
    SchemaField { name: "tls_handshake_timeout", flag: "alertmanager-storage.gcs.tls-handshake-timeout", kind: FieldKind::Duration, default_str: "10000000000", usage: "Maximum time to wait for a TLS handshake. Set to 0 for no limit." },
    SchemaField { name: "expect_continue_timeout", flag: "alertmanager-storage.gcs.expect-continue-timeout", kind: FieldKind::Duration, default_str: "1000000000", usage: "The time to wait for a server's first response headers after fully writing the request headers if the request has an Expect header. Set to 0 to send the request body immediately." },
    SchemaField { name: "max_idle_connections", flag: "alertmanager-storage.gcs.max-idle-connections", kind: FieldKind::Int, default_str: "100", usage: "Maximum number of idle (keep-alive) connections across all hosts. Set to 0 for no limit." },
    SchemaField { name: "max_idle_connections_per_host", flag: "alertmanager-storage.gcs.max-idle-connections-per-host", kind: FieldKind::Int, default_str: "100", usage: "Maximum number of idle (keep-alive) connections to keep per-host. Set to 0 to use a built-in default value of 2." },
    SchemaField { name: "max_connections_per_host", flag: "alertmanager-storage.gcs.max-connections-per-host", kind: FieldKind::Int, default_str: "0", usage: "Maximum number of connections per host. Set to 0 for no limit. The limit applies per bucket client, and a single process can create several clients for the same storage configuration, so its total connections to a host can exceed this value." },
    SchemaField { name: "force_attempt_http2", flag: "alertmanager-storage.gcs.http.force-attempt-http2", kind: FieldKind::Bool, default_str: "false", usage: "If enabled, the HTTP client attempts HTTP/2 for HTTPS connections. Without this option, a client with a custom TLS configuration uses HTTP/1.1." },
    SchemaField { name: "tls_ca_path", flag: "alertmanager-storage.gcs.http.tls-ca-path", kind: FieldKind::String, default_str: "", usage: "Path to the Certificate Authority (CA) certificates to validate the server certificate. If not set, the host's root CA certificates are used." },
    SchemaField { name: "tls_cert_path", flag: "alertmanager-storage.gcs.http.tls-cert-path", kind: FieldKind::String, default_str: "", usage: "Path to the client certificate, which is used for authenticating with the server. This setting also requires you to configure the key path." },
    SchemaField { name: "tls_key_path", flag: "alertmanager-storage.gcs.http.tls-key-path", kind: FieldKind::String, default_str: "", usage: "Path to the key for the client certificate. This setting also requires you to configure the client certificate." },
    SchemaField { name: "tls_server_name", flag: "alertmanager-storage.gcs.http.tls-server-name", kind: FieldKind::String, default_str: "", usage: "Override the expected name on the server certificate." },
];

static BLOCK_125_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_126_FIELDS: &[SchemaField] = &[
    SchemaField { name: "bucket_name", flag: "alertmanager-storage.gcs.bucket-name", kind: FieldKind::String, default_str: "", usage: "GCS bucket name" },
    SchemaField { name: "service_account", flag: "alertmanager-storage.gcs.service-account", kind: FieldKind::Secret, default_str: "", usage: "JSON either from a Google Developers Console client_credentials.json file, or a Google Developers service account key. Needs to be valid JSON, not a filesystem path. If empty, fallback to Google default logic:\n1. A JSON file whose path is specified by the GOOGLE_APPLICATION_CREDENTIALS environment variable. For workload identity federation, refer to https://cloud.google.com/iam/docs/how-to#using-workload-identity-federation on how to generate the JSON configuration file for on-prem/non-Google cloud platforms.\n2. A JSON file in a location known to the gcloud command-line tool: $HOME/.config/gcloud/application_default_credentials.json.\n3. On Google Compute Engine it fetches credentials from the metadata server." },
    SchemaField { name: "enable_upload_retries", flag: "alertmanager-storage.gcs.enable-upload-retries", kind: FieldKind::Bool, default_str: "true", usage: "Enable automatic retries for GCS uploads using the RetryAlways policy. Uploads will be retried on transient errors. Note: this does not guarantee idempotency." },
    SchemaField { name: "max_retries", flag: "alertmanager-storage.gcs.max-retries", kind: FieldKind::Int, default_str: "20", usage: "Maximum number of attempts for GCS operations (0 = unlimited, 1 = no retries). Applies to both regular and upload retry modes." },
];

static BLOCK_126_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "http", fields: BLOCK_125_FIELDS, blocks: BLOCK_125_BLOCKS },
];

static BLOCK_127_FIELDS: &[SchemaField] = &[
    SchemaField { name: "idle_conn_timeout", flag: "alertmanager-storage.azure.http.idle-conn-timeout", kind: FieldKind::Duration, default_str: "90000000000", usage: "The time an idle connection remains idle before closing." },
    SchemaField { name: "response_header_timeout", flag: "alertmanager-storage.azure.http.response-header-timeout", kind: FieldKind::Duration, default_str: "120000000000", usage: "The amount of time the client waits for a server's response headers." },
    SchemaField { name: "insecure_skip_verify", flag: "alertmanager-storage.azure.http.insecure-skip-verify", kind: FieldKind::Bool, default_str: "false", usage: "If the client connects to object storage via HTTPS and this option is enabled, the client accepts any certificate and hostname." },
    SchemaField { name: "tls_handshake_timeout", flag: "alertmanager-storage.azure.tls-handshake-timeout", kind: FieldKind::Duration, default_str: "10000000000", usage: "Maximum time to wait for a TLS handshake. Set to 0 for no limit." },
    SchemaField { name: "expect_continue_timeout", flag: "alertmanager-storage.azure.expect-continue-timeout", kind: FieldKind::Duration, default_str: "1000000000", usage: "The time to wait for a server's first response headers after fully writing the request headers if the request has an Expect header. Set to 0 to send the request body immediately." },
    SchemaField { name: "max_idle_connections", flag: "alertmanager-storage.azure.max-idle-connections", kind: FieldKind::Int, default_str: "100", usage: "Maximum number of idle (keep-alive) connections across all hosts. Set to 0 for no limit." },
    SchemaField { name: "max_idle_connections_per_host", flag: "alertmanager-storage.azure.max-idle-connections-per-host", kind: FieldKind::Int, default_str: "100", usage: "Maximum number of idle (keep-alive) connections to keep per-host. Set to 0 to use a built-in default value of 2." },
    SchemaField { name: "max_connections_per_host", flag: "alertmanager-storage.azure.max-connections-per-host", kind: FieldKind::Int, default_str: "0", usage: "Maximum number of connections per host. Set to 0 for no limit. The limit applies per bucket client, and a single process can create several clients for the same storage configuration, so its total connections to a host can exceed this value." },
    SchemaField { name: "force_attempt_http2", flag: "alertmanager-storage.azure.http.force-attempt-http2", kind: FieldKind::Bool, default_str: "false", usage: "If enabled, the HTTP client attempts HTTP/2 for HTTPS connections. Without this option, a client with a custom TLS configuration uses HTTP/1.1." },
    SchemaField { name: "tls_ca_path", flag: "alertmanager-storage.azure.http.tls-ca-path", kind: FieldKind::String, default_str: "", usage: "Path to the Certificate Authority (CA) certificates to validate the server certificate. If not set, the host's root CA certificates are used." },
    SchemaField { name: "tls_cert_path", flag: "alertmanager-storage.azure.http.tls-cert-path", kind: FieldKind::String, default_str: "", usage: "Path to the client certificate, which is used for authenticating with the server. This setting also requires you to configure the key path." },
    SchemaField { name: "tls_key_path", flag: "alertmanager-storage.azure.http.tls-key-path", kind: FieldKind::String, default_str: "", usage: "Path to the key for the client certificate. This setting also requires you to configure the client certificate." },
    SchemaField { name: "tls_server_name", flag: "alertmanager-storage.azure.http.tls-server-name", kind: FieldKind::String, default_str: "", usage: "Override the expected name on the server certificate." },
];

static BLOCK_127_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_128_FIELDS: &[SchemaField] = &[
    SchemaField { name: "account_name", flag: "alertmanager-storage.azure.account-name", kind: FieldKind::String, default_str: "", usage: "Azure storage account name" },
    SchemaField { name: "account_key", flag: "alertmanager-storage.azure.account-key", kind: FieldKind::Secret, default_str: "", usage: "Azure storage account key. If unset, Azure managed identities will be used for authentication instead." },
    SchemaField { name: "connection_string", flag: "alertmanager-storage.azure.connection-string", kind: FieldKind::Secret, default_str: "", usage: "If `connection-string` is set, the value of `endpoint-suffix` will not be used. Use this method over `account-key` if you need to authenticate via a SAS token. Or if you use the Azurite emulator." },
    SchemaField { name: "container_name", flag: "alertmanager-storage.azure.container-name", kind: FieldKind::String, default_str: "", usage: "Azure storage container name" },
    SchemaField { name: "endpoint_suffix", flag: "alertmanager-storage.azure.endpoint-suffix", kind: FieldKind::String, default_str: "", usage: "Azure storage endpoint suffix without schema. The account name will be prefixed to this value to create the FQDN. If set to empty string, default endpoint suffix is used." },
    SchemaField { name: "max_retries", flag: "alertmanager-storage.azure.max-retries", kind: FieldKind::Int, default_str: "20", usage: "Number of retries for recoverable errors" },
    SchemaField { name: "user_assigned_id", flag: "alertmanager-storage.azure.user-assigned-id", kind: FieldKind::String, default_str: "", usage: "User assigned managed identity. If empty, then System assigned identity is used." },
];

static BLOCK_128_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "http", fields: BLOCK_127_FIELDS, blocks: BLOCK_127_BLOCKS },
];

static BLOCK_129_FIELDS: &[SchemaField] = &[
    SchemaField { name: "application_credential_id", flag: "alertmanager-storage.swift.application-credential-id", kind: FieldKind::String, default_str: "", usage: "OpenStack Swift application credential id" },
    SchemaField { name: "application_credential_name", flag: "alertmanager-storage.swift.application-credential-name", kind: FieldKind::String, default_str: "", usage: "OpenStack Swift application credential name" },
    SchemaField { name: "application_credential_secret", flag: "alertmanager-storage.swift.application-credential-secret", kind: FieldKind::Secret, default_str: "", usage: "OpenStack Swift application credential secret" },
    SchemaField { name: "auth_version", flag: "alertmanager-storage.swift.auth-version", kind: FieldKind::Int, default_str: "0", usage: "OpenStack Swift authentication API version. 0 to autodetect." },
    SchemaField { name: "auth_url", flag: "alertmanager-storage.swift.auth-url", kind: FieldKind::String, default_str: "", usage: "OpenStack Swift authentication URL" },
    SchemaField { name: "username", flag: "alertmanager-storage.swift.username", kind: FieldKind::String, default_str: "", usage: "OpenStack Swift username." },
    SchemaField { name: "user_domain_name", flag: "alertmanager-storage.swift.user-domain-name", kind: FieldKind::String, default_str: "", usage: "OpenStack Swift user's domain name." },
    SchemaField { name: "user_domain_id", flag: "alertmanager-storage.swift.user-domain-id", kind: FieldKind::String, default_str: "", usage: "OpenStack Swift user's domain ID." },
    SchemaField { name: "user_id", flag: "alertmanager-storage.swift.user-id", kind: FieldKind::String, default_str: "", usage: "OpenStack Swift user ID." },
    SchemaField { name: "password", flag: "alertmanager-storage.swift.password", kind: FieldKind::Secret, default_str: "", usage: "OpenStack Swift API key." },
    SchemaField { name: "domain_id", flag: "alertmanager-storage.swift.domain-id", kind: FieldKind::String, default_str: "", usage: "OpenStack Swift user's domain ID." },
    SchemaField { name: "domain_name", flag: "alertmanager-storage.swift.domain-name", kind: FieldKind::String, default_str: "", usage: "OpenStack Swift user's domain name." },
    SchemaField { name: "project_id", flag: "alertmanager-storage.swift.project-id", kind: FieldKind::String, default_str: "", usage: "OpenStack Swift project ID (v2,v3 auth only)." },
    SchemaField { name: "project_name", flag: "alertmanager-storage.swift.project-name", kind: FieldKind::String, default_str: "", usage: "OpenStack Swift project name (v2,v3 auth only)." },
    SchemaField { name: "project_domain_id", flag: "alertmanager-storage.swift.project-domain-id", kind: FieldKind::String, default_str: "", usage: "ID of the OpenStack Swift project's domain (v3 auth only), only needed if it differs the from user domain." },
    SchemaField { name: "project_domain_name", flag: "alertmanager-storage.swift.project-domain-name", kind: FieldKind::String, default_str: "", usage: "Name of the OpenStack Swift project's domain (v3 auth only), only needed if it differs from the user domain." },
    SchemaField { name: "region_name", flag: "alertmanager-storage.swift.region-name", kind: FieldKind::String, default_str: "", usage: "OpenStack Swift Region to use (v2,v3 auth only)." },
    SchemaField { name: "container_name", flag: "alertmanager-storage.swift.container-name", kind: FieldKind::String, default_str: "", usage: "Name of the OpenStack Swift container to put chunks in." },
    SchemaField { name: "max_retries", flag: "alertmanager-storage.swift.max-retries", kind: FieldKind::Int, default_str: "3", usage: "Max retries on requests error." },
    SchemaField { name: "connect_timeout", flag: "alertmanager-storage.swift.connect-timeout", kind: FieldKind::Duration, default_str: "10000000000", usage: "Time after which a connection attempt is aborted." },
    SchemaField { name: "request_timeout", flag: "alertmanager-storage.swift.request-timeout", kind: FieldKind::Duration, default_str: "5000000000", usage: "Time after which an idle request is aborted. The timeout watchdog is reset each time some data is received, so the timeout triggers after X time no data is received on a request." },
];

static BLOCK_129_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_130_FIELDS: &[SchemaField] = &[
    SchemaField { name: "dir", flag: "alertmanager-storage.filesystem.dir", kind: FieldKind::String, default_str: "alertmanager", usage: "Local filesystem storage directory." },
];

static BLOCK_130_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_131_FIELDS: &[SchemaField] = &[
    SchemaField { name: "path", flag: "alertmanager-storage.local.path", kind: FieldKind::String, default_str: "", usage: "Path at which alertmanager configurations are stored." },
];

static BLOCK_131_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_132_FIELDS: &[SchemaField] = &[
    SchemaField { name: "backend", flag: "alertmanager-storage.backend", kind: FieldKind::String, default_str: "filesystem", usage: "Backend storage to use. Supported backends are: s3, gcs, azure, swift, filesystem, local." },
    SchemaField { name: "storage_prefix", flag: "alertmanager-storage.storage-prefix", kind: FieldKind::String, default_str: "", usage: "Prefix for all objects stored in the backend storage. For simplicity, it may only contain digits and English alphabet letters." },
];

static BLOCK_132_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "s3", fields: BLOCK_124_FIELDS, blocks: BLOCK_124_BLOCKS },
    SchemaBlock { name: "gcs", fields: BLOCK_126_FIELDS, blocks: BLOCK_126_BLOCKS },
    SchemaBlock { name: "azure", fields: BLOCK_128_FIELDS, blocks: BLOCK_128_BLOCKS },
    SchemaBlock { name: "swift", fields: BLOCK_129_FIELDS, blocks: BLOCK_129_BLOCKS },
    SchemaBlock { name: "filesystem", fields: BLOCK_130_FIELDS, blocks: BLOCK_130_BLOCKS },
    SchemaBlock { name: "local", fields: BLOCK_131_FIELDS, blocks: BLOCK_131_BLOCKS },
];

static BLOCK_133_FIELDS: &[SchemaField] = &[
    SchemaField { name: "min_period", flag: "query-scheduler.grpc-client-config.backoff-min-period", kind: FieldKind::Duration, default_str: "100000000", usage: "Minimum delay when backing off." },
    SchemaField { name: "max_period", flag: "query-scheduler.grpc-client-config.backoff-max-period", kind: FieldKind::Duration, default_str: "10000000000", usage: "Maximum delay when backing off." },
    SchemaField { name: "max_retries", flag: "query-scheduler.grpc-client-config.backoff-retries", kind: FieldKind::Int, default_str: "10", usage: "Number of times to backoff and retry before failing." },
];

static BLOCK_133_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_134_FIELDS: &[SchemaField] = &[
    SchemaField { name: "label", flag: "query-scheduler.grpc-client-config.cluster-validation.label", kind: FieldKind::String, default_str: "", usage: "Primary cluster validation label." },
];

static BLOCK_134_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_135_FIELDS: &[SchemaField] = &[
    SchemaField { name: "max_recv_msg_size", flag: "query-scheduler.grpc-client-config.grpc-max-recv-msg-size", kind: FieldKind::Int, default_str: "104857600", usage: "gRPC client max receive message size (bytes)." },
    SchemaField { name: "max_send_msg_size", flag: "query-scheduler.grpc-client-config.grpc-max-send-msg-size", kind: FieldKind::Int, default_str: "104857600", usage: "gRPC client max send message size (bytes)." },
    SchemaField { name: "grpc_compression", flag: "query-scheduler.grpc-client-config.grpc-compression", kind: FieldKind::String, default_str: "", usage: "Use compression when sending messages. Supported values are: 'gzip', 'snappy', 's2' and '' (disable compression)" },
    SchemaField { name: "rate_limit", flag: "query-scheduler.grpc-client-config.grpc-client-rate-limit", kind: FieldKind::Float, default_str: "0", usage: "Rate limit for gRPC client; 0 means disabled." },
    SchemaField { name: "rate_limit_burst", flag: "query-scheduler.grpc-client-config.grpc-client-rate-limit-burst", kind: FieldKind::Int, default_str: "0", usage: "Rate limit burst for gRPC client." },
    SchemaField { name: "backoff_on_ratelimits", flag: "query-scheduler.grpc-client-config.backoff-on-ratelimits", kind: FieldKind::Bool, default_str: "false", usage: "Enable backoff and retry when we hit rate limits." },
    SchemaField { name: "initial_stream_window_size", flag: "query-scheduler.grpc-client-config.initial-stream-window-size", kind: FieldKind::Bytes, default_str: "63KiB1023B", usage: "Initial stream window size. Values less than the default are not supported and are ignored. Setting this to a value other than the default disables the BDP estimator." },
    SchemaField { name: "initial_connection_window_size", flag: "query-scheduler.grpc-client-config.initial-connection-window-size", kind: FieldKind::Bytes, default_str: "63KiB1023B", usage: "Initial connection window size. Values less than the default are not supported and are ignored. Setting this to a value other than the default disables the BDP estimator." },
    SchemaField { name: "tls_enabled", flag: "query-scheduler.grpc-client-config.tls-enabled", kind: FieldKind::Bool, default_str: "false", usage: "Enable TLS in the gRPC client. This flag needs to be enabled when any other TLS flag is set. If set to false, insecure connection to gRPC server will be used." },
    SchemaField { name: "tls_cert_path", flag: "query-scheduler.grpc-client-config.tls-cert-path", kind: FieldKind::String, default_str: "", usage: "Path to the client certificate, which will be used for authenticating with the server. Also requires the key path to be configured." },
    SchemaField { name: "tls_key_path", flag: "query-scheduler.grpc-client-config.tls-key-path", kind: FieldKind::String, default_str: "", usage: "Path to the key for the client certificate. Also requires the client certificate to be configured." },
    SchemaField { name: "tls_ca_path", flag: "query-scheduler.grpc-client-config.tls-ca-path", kind: FieldKind::String, default_str: "", usage: "Path to the CA certificates to validate server certificate against. If not set, the host's root CA certificates are used." },
    SchemaField { name: "tls_server_name", flag: "query-scheduler.grpc-client-config.tls-server-name", kind: FieldKind::String, default_str: "", usage: "Override the expected name on the server certificate." },
    SchemaField { name: "tls_insecure_skip_verify", flag: "query-scheduler.grpc-client-config.tls-insecure-skip-verify", kind: FieldKind::Bool, default_str: "false", usage: "Skip validating server certificate." },
    SchemaField { name: "tls_cipher_suites", flag: "query-scheduler.grpc-client-config.tls-cipher-suites", kind: FieldKind::String, default_str: "", usage: "Override the default cipher suite list (separated by commas). Allowed values:\n\nSecure Ciphers:\n- TLS_AES_128_GCM_SHA256\n- TLS_AES_256_GCM_SHA384\n- TLS_CHACHA20_POLY1305_SHA256\n- TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA\n- TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA\n- TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256\n- TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256\n- TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256\n- TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256\n\nInsecure Ciphers:\n- TLS_RSA_WITH_RC4_128_SHA\n- TLS_RSA_WITH_3DES_EDE_CBC_SHA\n- TLS_RSA_WITH_AES_128_CBC_SHA\n- TLS_RSA_WITH_AES_256_CBC_SHA\n- TLS_RSA_WITH_AES_128_CBC_SHA256\n- TLS_RSA_WITH_AES_128_GCM_SHA256\n- TLS_RSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_ECDSA_WITH_RC4_128_SHA\n- TLS_ECDHE_RSA_WITH_RC4_128_SHA\n- TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256\n- TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256\n" },
    SchemaField { name: "tls_min_version", flag: "query-scheduler.grpc-client-config.tls-min-version", kind: FieldKind::String, default_str: "", usage: "Override the default minimum TLS version. Allowed values: VersionTLS10, VersionTLS11, VersionTLS12, VersionTLS13" },
    SchemaField { name: "connect_timeout", flag: "query-scheduler.grpc-client-config.connect-timeout", kind: FieldKind::Duration, default_str: "5000000000", usage: "The maximum amount of time to establish a connection. A value of 0 means default gRPC client connect timeout and backoff." },
    SchemaField { name: "connect_backoff_base_delay", flag: "query-scheduler.grpc-client-config.connect-backoff-base-delay", kind: FieldKind::Duration, default_str: "1000000000", usage: "Initial backoff delay after first connection failure. Only relevant if ConnectTimeout > 0." },
    SchemaField { name: "connect_backoff_max_delay", flag: "query-scheduler.grpc-client-config.connect-backoff-max-delay", kind: FieldKind::Duration, default_str: "5000000000", usage: "Maximum backoff delay when establishing a connection. Only relevant if ConnectTimeout > 0." },
    SchemaField { name: "keepalive_time", flag: "query-scheduler.grpc-client-config.keepalive-time", kind: FieldKind::Duration, default_str: "20000000000", usage: "After a duration of this time if the client doesn't see any activity it pings the server to see if the transport is still alive. This also determines the socket's TCP_USER_TIMEOUT together with keepalive-timeout." },
    SchemaField { name: "keepalive_timeout", flag: "query-scheduler.grpc-client-config.keepalive-timeout", kind: FieldKind::Duration, default_str: "10000000000", usage: "After having pinged for keepalive check, the client waits for a duration of this time and if no activity is seen even after that the connection is closed." },
];

static BLOCK_135_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "backoff_config", fields: BLOCK_133_FIELDS, blocks: BLOCK_133_BLOCKS },
    SchemaBlock { name: "cluster_validation", fields: BLOCK_134_FIELDS, blocks: BLOCK_134_BLOCKS },
];

static BLOCK_136_FIELDS: &[SchemaField] = &[
    SchemaField { name: "host", flag: "query-scheduler.ring.consul.hostname", kind: FieldKind::String, default_str: "localhost:8500", usage: "Hostname and port of Consul." },
    SchemaField { name: "acl_token", flag: "query-scheduler.ring.consul.acl-token", kind: FieldKind::Secret, default_str: "", usage: "ACL Token used to interact with Consul." },
    SchemaField { name: "http_client_timeout", flag: "query-scheduler.ring.consul.client-timeout", kind: FieldKind::Duration, default_str: "20000000000", usage: "HTTP timeout when talking to Consul" },
    SchemaField { name: "consistent_reads", flag: "query-scheduler.ring.consul.consistent-reads", kind: FieldKind::Bool, default_str: "false", usage: "Enable consistent reads to Consul." },
    SchemaField { name: "watch_rate_limit", flag: "query-scheduler.ring.consul.watch-rate-limit", kind: FieldKind::Float, default_str: "1", usage: "Rate limit when watching key or prefix in Consul, in requests per second. 0 disables the rate limit." },
    SchemaField { name: "watch_burst_size", flag: "query-scheduler.ring.consul.watch-burst-size", kind: FieldKind::Int, default_str: "1", usage: "Burst size used in rate limit. Values less than 1 are treated as 1." },
    SchemaField { name: "cas_retry_delay", flag: "query-scheduler.ring.consul.cas-retry-delay", kind: FieldKind::Duration, default_str: "1000000000", usage: "Maximum duration to wait before retrying a Compare And Swap (CAS) operation." },
];

static BLOCK_136_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_137_FIELDS: &[SchemaField] = &[
    SchemaField { name: "endpoints", flag: "query-scheduler.ring.etcd.endpoints", kind: FieldKind::StringList, default_str: "", usage: "The etcd endpoints to connect to." },
    SchemaField { name: "dial_timeout", flag: "query-scheduler.ring.etcd.dial-timeout", kind: FieldKind::Duration, default_str: "10000000000", usage: "The dial timeout for the etcd connection." },
    SchemaField { name: "max_retries", flag: "query-scheduler.ring.etcd.max-retries", kind: FieldKind::Int, default_str: "10", usage: "The maximum number of retries to do for failed ops." },
    SchemaField { name: "tls_enabled", flag: "query-scheduler.ring.etcd.tls-enabled", kind: FieldKind::Bool, default_str: "false", usage: "Enable TLS." },
    SchemaField { name: "tls_cert_path", flag: "query-scheduler.ring.etcd.tls-cert-path", kind: FieldKind::String, default_str: "", usage: "Path to the client certificate, which will be used for authenticating with the server. Also requires the key path to be configured." },
    SchemaField { name: "tls_key_path", flag: "query-scheduler.ring.etcd.tls-key-path", kind: FieldKind::String, default_str: "", usage: "Path to the key for the client certificate. Also requires the client certificate to be configured." },
    SchemaField { name: "tls_ca_path", flag: "query-scheduler.ring.etcd.tls-ca-path", kind: FieldKind::String, default_str: "", usage: "Path to the CA certificates to validate server certificate against. If not set, the host's root CA certificates are used." },
    SchemaField { name: "tls_server_name", flag: "query-scheduler.ring.etcd.tls-server-name", kind: FieldKind::String, default_str: "", usage: "Override the expected name on the server certificate." },
    SchemaField { name: "tls_insecure_skip_verify", flag: "query-scheduler.ring.etcd.tls-insecure-skip-verify", kind: FieldKind::Bool, default_str: "false", usage: "Skip validating server certificate." },
    SchemaField { name: "tls_cipher_suites", flag: "query-scheduler.ring.etcd.tls-cipher-suites", kind: FieldKind::String, default_str: "", usage: "Override the default cipher suite list (separated by commas). Allowed values:\n\nSecure Ciphers:\n- TLS_AES_128_GCM_SHA256\n- TLS_AES_256_GCM_SHA384\n- TLS_CHACHA20_POLY1305_SHA256\n- TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA\n- TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA\n- TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256\n- TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256\n- TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256\n- TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256\n\nInsecure Ciphers:\n- TLS_RSA_WITH_RC4_128_SHA\n- TLS_RSA_WITH_3DES_EDE_CBC_SHA\n- TLS_RSA_WITH_AES_128_CBC_SHA\n- TLS_RSA_WITH_AES_256_CBC_SHA\n- TLS_RSA_WITH_AES_128_CBC_SHA256\n- TLS_RSA_WITH_AES_128_GCM_SHA256\n- TLS_RSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_ECDSA_WITH_RC4_128_SHA\n- TLS_ECDHE_RSA_WITH_RC4_128_SHA\n- TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256\n- TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256\n" },
    SchemaField { name: "tls_min_version", flag: "query-scheduler.ring.etcd.tls-min-version", kind: FieldKind::String, default_str: "", usage: "Override the default minimum TLS version. Allowed values: VersionTLS10, VersionTLS11, VersionTLS12, VersionTLS13" },
    SchemaField { name: "username", flag: "query-scheduler.ring.etcd.username", kind: FieldKind::String, default_str: "", usage: "Etcd username." },
    SchemaField { name: "password", flag: "query-scheduler.ring.etcd.password", kind: FieldKind::Secret, default_str: "", usage: "Etcd password." },
];

static BLOCK_137_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_138_FIELDS: &[SchemaField] = &[
    SchemaField { name: "primary", flag: "query-scheduler.ring.multi.primary", kind: FieldKind::String, default_str: "", usage: "Primary backend storage used by multi-client." },
    SchemaField { name: "secondary", flag: "query-scheduler.ring.multi.secondary", kind: FieldKind::String, default_str: "", usage: "Secondary backend storage used by multi-client." },
    SchemaField { name: "mirror_enabled", flag: "query-scheduler.ring.multi.mirror-enabled", kind: FieldKind::Bool, default_str: "false", usage: "Mirror writes to the secondary store." },
    SchemaField { name: "mirror_timeout", flag: "query-scheduler.ring.multi.mirror-timeout", kind: FieldKind::Duration, default_str: "2000000000", usage: "Timeout for storing a value to the secondary store." },
];

static BLOCK_138_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_139_FIELDS: &[SchemaField] = &[
    SchemaField { name: "store", flag: "query-scheduler.ring.store", kind: FieldKind::String, default_str: "memberlist", usage: "Backend storage to use for the ring. Supported values are: consul, etcd, inmemory, memberlist, multi." },
    SchemaField { name: "prefix", flag: "query-scheduler.ring.prefix", kind: FieldKind::String, default_str: "collectors/", usage: "The prefix for the keys in the store. Should end with a /." },
];

static BLOCK_139_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "consul", fields: BLOCK_136_FIELDS, blocks: BLOCK_136_BLOCKS },
    SchemaBlock { name: "etcd", fields: BLOCK_137_FIELDS, blocks: BLOCK_137_BLOCKS },
    SchemaBlock { name: "multi", fields: BLOCK_138_FIELDS, blocks: BLOCK_138_BLOCKS },
];

static BLOCK_140_FIELDS: &[SchemaField] = &[
    SchemaField { name: "heartbeat_period", flag: "query-scheduler.ring.heartbeat-period", kind: FieldKind::Duration, default_str: "15000000000", usage: "Period at which to heartbeat to the ring." },
    SchemaField { name: "heartbeat_timeout", flag: "query-scheduler.ring.heartbeat-timeout", kind: FieldKind::Duration, default_str: "60000000000", usage: "The heartbeat timeout after which query-schedulers are considered unhealthy within the ring. When query-scheduler ring-based service discovery is enabled, this option needs be set on query-schedulers, query-frontends and queriers." },
    SchemaField { name: "auto_forget_unhealthy_periods", flag: "query-scheduler.ring.auto-forget-unhealthy-periods", kind: FieldKind::Int, default_str: "10", usage: "Number of consecutive timeout periods after which Mimir automatically removes an unhealthy instance in the ring. Set to 0 to disable auto-forget." },
    SchemaField { name: "instance_id", flag: "query-scheduler.ring.instance-id", kind: FieldKind::String, default_str: "<hostname>", usage: "Instance ID to register in the ring." },
    SchemaField { name: "instance_interface_names", flag: "query-scheduler.ring.instance-interface-names", kind: FieldKind::StringList, default_str: "", usage: "List of network interface names to look up when finding the instance IP address." },
    SchemaField { name: "instance_port", flag: "query-scheduler.ring.instance-port", kind: FieldKind::Int, default_str: "0", usage: "Port to advertise in the ring (defaults to -server.grpc-listen-port)." },
    SchemaField { name: "instance_addr", flag: "query-scheduler.ring.instance-addr", kind: FieldKind::String, default_str: "", usage: "IP address to advertise in the ring. Default is auto-detected." },
    SchemaField { name: "instance_enable_ipv6", flag: "query-scheduler.ring.instance-enable-ipv6", kind: FieldKind::Bool, default_str: "false", usage: "Enable using a IPv6 instance address. (default false)" },
];

static BLOCK_140_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "kvstore", fields: BLOCK_139_FIELDS, blocks: BLOCK_139_BLOCKS },
];

static BLOCK_141_FIELDS: &[SchemaField] = &[
    SchemaField { name: "max_outstanding_requests_per_tenant", flag: "query-scheduler.max-outstanding-requests-per-tenant", kind: FieldKind::Int, default_str: "100", usage: "Maximum number of outstanding requests per tenant per query-scheduler. In-flight requests above this limit will fail with HTTP response status code 429. When -query-scheduler.queue-reservation-enabled is true, the idle slots of each queue reservation also count against this limit." },
    SchemaField { name: "querier_forget_delay", flag: "query-scheduler.querier-forget-delay", kind: FieldKind::Duration, default_str: "0", usage: "If a querier disconnects without sending notification about graceful shutdown, the query-scheduler will keep the querier in the tenant's shard until the forget delay has passed. This feature is useful to reduce the blast radius when shuffle-sharding is enabled." },
    SchemaField { name: "queue_max_wait_metric_enabled", flag: "query-scheduler.queue-max-wait-metric-enabled", kind: FieldKind::Bool, default_str: "true", usage: "Enable the cortex_query_scheduler_queue_max_wait_seconds metric, which reports how long the oldest request still waiting in the queue has been waiting. Disabling it skips the per-tick scan over inflight requests." },
    SchemaField { name: "max_queue_length_metric_enabled", flag: "query-scheduler.max-queue-length-metric-enabled", kind: FieldKind::Bool, default_str: "false", usage: "Enable the cortex_query_scheduler_max_queue_length metric, which reports the per-tenant peak queue length observed since the last scrape. Disabling it skips per-tenant peak tracking on enqueue and dequeue." },
    SchemaField { name: "queue_reservation_enabled", flag: "query-scheduler.queue-reservation-enabled", kind: FieldKind::Bool, default_str: "false", usage: "Enable queue reservation: a query-frontend that supports it reserves queue slots for a group of sub-requests before it sends them. The query-scheduler admits or rejects each reservation as a whole. An admitted sub-request uses an idle slot of its reservation, so the per-tenant limit does not reject it. A dequeue returns the slot to the reservation. Requires a query-frontend that also supports queue reservation; has no effect otherwise." },
    SchemaField { name: "queue_reservation_timeout", flag: "query-scheduler.queue-reservation-timeout", kind: FieldKind::Duration, default_str: "5000000000", usage: "Time after which the query-scheduler closes an open reservation and releases its idle queue slots, when no sub-request of the reservation is queued or running and none was inserted or finished in that time. Only applies when -query-scheduler.queue-reservation-enabled is true." },
    SchemaField { name: "service_discovery_mode", flag: "query-scheduler.service-discovery-mode", kind: FieldKind::String, default_str: "dns", usage: "Service discovery mode that query-frontends and queriers use to find query-scheduler instances. When query-scheduler ring-based service discovery is enabled, this option needs be set on query-schedulers, query-frontends and queriers. Supported values are: dns, ring." },
    SchemaField { name: "max_used_instances", flag: "query-scheduler.max-used-instances", kind: FieldKind::Int, default_str: "0", usage: "The maximum number of query-scheduler instances to use, regardless how many replicas are running. This option can be set only when -query-scheduler.service-discovery-mode is set to 'ring'. 0 to use all available query-scheduler instances." },
];

static BLOCK_141_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "grpc_client_config", fields: BLOCK_135_FIELDS, blocks: BLOCK_135_BLOCKS },
    SchemaBlock { name: "ring", fields: BLOCK_140_FIELDS, blocks: BLOCK_140_BLOCKS },
];

static BLOCK_142_FIELDS: &[SchemaField] = &[
    SchemaField { name: "host", flag: "overrides-exporter.ring.consul.hostname", kind: FieldKind::String, default_str: "localhost:8500", usage: "Hostname and port of Consul." },
    SchemaField { name: "acl_token", flag: "overrides-exporter.ring.consul.acl-token", kind: FieldKind::Secret, default_str: "", usage: "ACL Token used to interact with Consul." },
    SchemaField { name: "http_client_timeout", flag: "overrides-exporter.ring.consul.client-timeout", kind: FieldKind::Duration, default_str: "20000000000", usage: "HTTP timeout when talking to Consul" },
    SchemaField { name: "consistent_reads", flag: "overrides-exporter.ring.consul.consistent-reads", kind: FieldKind::Bool, default_str: "false", usage: "Enable consistent reads to Consul." },
    SchemaField { name: "watch_rate_limit", flag: "overrides-exporter.ring.consul.watch-rate-limit", kind: FieldKind::Float, default_str: "1", usage: "Rate limit when watching key or prefix in Consul, in requests per second. 0 disables the rate limit." },
    SchemaField { name: "watch_burst_size", flag: "overrides-exporter.ring.consul.watch-burst-size", kind: FieldKind::Int, default_str: "1", usage: "Burst size used in rate limit. Values less than 1 are treated as 1." },
    SchemaField { name: "cas_retry_delay", flag: "overrides-exporter.ring.consul.cas-retry-delay", kind: FieldKind::Duration, default_str: "1000000000", usage: "Maximum duration to wait before retrying a Compare And Swap (CAS) operation." },
];

static BLOCK_142_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_143_FIELDS: &[SchemaField] = &[
    SchemaField { name: "endpoints", flag: "overrides-exporter.ring.etcd.endpoints", kind: FieldKind::StringList, default_str: "", usage: "The etcd endpoints to connect to." },
    SchemaField { name: "dial_timeout", flag: "overrides-exporter.ring.etcd.dial-timeout", kind: FieldKind::Duration, default_str: "10000000000", usage: "The dial timeout for the etcd connection." },
    SchemaField { name: "max_retries", flag: "overrides-exporter.ring.etcd.max-retries", kind: FieldKind::Int, default_str: "10", usage: "The maximum number of retries to do for failed ops." },
    SchemaField { name: "tls_enabled", flag: "overrides-exporter.ring.etcd.tls-enabled", kind: FieldKind::Bool, default_str: "false", usage: "Enable TLS." },
    SchemaField { name: "tls_cert_path", flag: "overrides-exporter.ring.etcd.tls-cert-path", kind: FieldKind::String, default_str: "", usage: "Path to the client certificate, which will be used for authenticating with the server. Also requires the key path to be configured." },
    SchemaField { name: "tls_key_path", flag: "overrides-exporter.ring.etcd.tls-key-path", kind: FieldKind::String, default_str: "", usage: "Path to the key for the client certificate. Also requires the client certificate to be configured." },
    SchemaField { name: "tls_ca_path", flag: "overrides-exporter.ring.etcd.tls-ca-path", kind: FieldKind::String, default_str: "", usage: "Path to the CA certificates to validate server certificate against. If not set, the host's root CA certificates are used." },
    SchemaField { name: "tls_server_name", flag: "overrides-exporter.ring.etcd.tls-server-name", kind: FieldKind::String, default_str: "", usage: "Override the expected name on the server certificate." },
    SchemaField { name: "tls_insecure_skip_verify", flag: "overrides-exporter.ring.etcd.tls-insecure-skip-verify", kind: FieldKind::Bool, default_str: "false", usage: "Skip validating server certificate." },
    SchemaField { name: "tls_cipher_suites", flag: "overrides-exporter.ring.etcd.tls-cipher-suites", kind: FieldKind::String, default_str: "", usage: "Override the default cipher suite list (separated by commas). Allowed values:\n\nSecure Ciphers:\n- TLS_AES_128_GCM_SHA256\n- TLS_AES_256_GCM_SHA384\n- TLS_CHACHA20_POLY1305_SHA256\n- TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA\n- TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA\n- TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256\n- TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256\n- TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256\n- TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256\n\nInsecure Ciphers:\n- TLS_RSA_WITH_RC4_128_SHA\n- TLS_RSA_WITH_3DES_EDE_CBC_SHA\n- TLS_RSA_WITH_AES_128_CBC_SHA\n- TLS_RSA_WITH_AES_256_CBC_SHA\n- TLS_RSA_WITH_AES_128_CBC_SHA256\n- TLS_RSA_WITH_AES_128_GCM_SHA256\n- TLS_RSA_WITH_AES_256_GCM_SHA384\n- TLS_ECDHE_ECDSA_WITH_RC4_128_SHA\n- TLS_ECDHE_RSA_WITH_RC4_128_SHA\n- TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA\n- TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256\n- TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256\n" },
    SchemaField { name: "tls_min_version", flag: "overrides-exporter.ring.etcd.tls-min-version", kind: FieldKind::String, default_str: "", usage: "Override the default minimum TLS version. Allowed values: VersionTLS10, VersionTLS11, VersionTLS12, VersionTLS13" },
    SchemaField { name: "username", flag: "overrides-exporter.ring.etcd.username", kind: FieldKind::String, default_str: "", usage: "Etcd username." },
    SchemaField { name: "password", flag: "overrides-exporter.ring.etcd.password", kind: FieldKind::Secret, default_str: "", usage: "Etcd password." },
];

static BLOCK_143_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_144_FIELDS: &[SchemaField] = &[
    SchemaField { name: "primary", flag: "overrides-exporter.ring.multi.primary", kind: FieldKind::String, default_str: "", usage: "Primary backend storage used by multi-client." },
    SchemaField { name: "secondary", flag: "overrides-exporter.ring.multi.secondary", kind: FieldKind::String, default_str: "", usage: "Secondary backend storage used by multi-client." },
    SchemaField { name: "mirror_enabled", flag: "overrides-exporter.ring.multi.mirror-enabled", kind: FieldKind::Bool, default_str: "false", usage: "Mirror writes to the secondary store." },
    SchemaField { name: "mirror_timeout", flag: "overrides-exporter.ring.multi.mirror-timeout", kind: FieldKind::Duration, default_str: "2000000000", usage: "Timeout for storing a value to the secondary store." },
];

static BLOCK_144_BLOCKS: &[SchemaBlock] = &[

];

static BLOCK_145_FIELDS: &[SchemaField] = &[
    SchemaField { name: "store", flag: "overrides-exporter.ring.store", kind: FieldKind::String, default_str: "memberlist", usage: "Backend storage to use for the ring. Supported values are: consul, etcd, inmemory, memberlist, multi." },
    SchemaField { name: "prefix", flag: "overrides-exporter.ring.prefix", kind: FieldKind::String, default_str: "collectors/", usage: "The prefix for the keys in the store. Should end with a /." },
];

static BLOCK_145_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "consul", fields: BLOCK_142_FIELDS, blocks: BLOCK_142_BLOCKS },
    SchemaBlock { name: "etcd", fields: BLOCK_143_FIELDS, blocks: BLOCK_143_BLOCKS },
    SchemaBlock { name: "multi", fields: BLOCK_144_FIELDS, blocks: BLOCK_144_BLOCKS },
];

static BLOCK_146_FIELDS: &[SchemaField] = &[
    SchemaField { name: "enabled", flag: "overrides-exporter.ring.enabled", kind: FieldKind::Bool, default_str: "false", usage: "Enable the ring used by override-exporters to deduplicate exported limit metrics." },
    SchemaField { name: "heartbeat_period", flag: "overrides-exporter.ring.heartbeat-period", kind: FieldKind::Duration, default_str: "15000000000", usage: "Period at which to heartbeat to the ring." },
    SchemaField { name: "heartbeat_timeout", flag: "overrides-exporter.ring.heartbeat-timeout", kind: FieldKind::Duration, default_str: "60000000000", usage: "Heartbeat timeout after which Mimir marks overrides-exporters as unhealthy in the ring." },
    SchemaField { name: "instance_id", flag: "overrides-exporter.ring.instance-id", kind: FieldKind::String, default_str: "<hostname>", usage: "Instance ID to register in the ring." },
    SchemaField { name: "instance_interface_names", flag: "overrides-exporter.ring.instance-interface-names", kind: FieldKind::StringList, default_str: "", usage: "List of network interface names to look up when finding the instance IP address." },
    SchemaField { name: "instance_port", flag: "overrides-exporter.ring.instance-port", kind: FieldKind::Int, default_str: "0", usage: "Port to advertise in the ring (defaults to -server.grpc-listen-port)." },
    SchemaField { name: "instance_addr", flag: "overrides-exporter.ring.instance-addr", kind: FieldKind::String, default_str: "", usage: "IP address to advertise in the ring. Default is auto-detected." },
    SchemaField { name: "instance_enable_ipv6", flag: "overrides-exporter.ring.instance-enable-ipv6", kind: FieldKind::Bool, default_str: "false", usage: "Enable using an IPv6 instance address." },
    SchemaField { name: "wait_stability_min_duration", flag: "overrides-exporter.ring.wait-stability-min-duration", kind: FieldKind::Duration, default_str: "0", usage: "Minimum time to wait for ring stability at startup, if set to positive value. Set to 0 to disable." },
    SchemaField { name: "wait_stability_max_duration", flag: "overrides-exporter.ring.wait-stability-max-duration", kind: FieldKind::Duration, default_str: "300000000000", usage: "Maximum time to wait for ring stability at startup. If the overrides-exporter ring keeps changing after this period of time, it will start anyway." },
    SchemaField { name: "auto_forget_unhealthy_periods", flag: "overrides-exporter.ring.auto-forget-unhealthy-periods", kind: FieldKind::Int, default_str: "4", usage: "Number of consecutive timeout periods after which Mimir automatically removes an unhealthy instance in the ring. Set to 0 to disable auto-forget." },
];

static BLOCK_146_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "kvstore", fields: BLOCK_145_FIELDS, blocks: BLOCK_145_BLOCKS },
];

static BLOCK_147_FIELDS: &[SchemaField] = &[
    SchemaField { name: "enabled_metrics", flag: "overrides-exporter.enabled-metrics", kind: FieldKind::String, default_str: "ingestion_burst_size,ingestion_rate,max_fetched_chunk_bytes_per_query,max_fetched_chunks_per_query,max_fetched_series_per_query,max_global_exemplars_per_user,max_global_series_per_metric,max_global_series_per_user,ruler_max_rule_groups_per_tenant,ruler_max_rules_per_rule_group", usage: "Comma-separated list of metrics to include in the exporter. Metric names must match yaml tags from the limits section of the configuration." },
];

static BLOCK_147_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "ring", fields: BLOCK_146_FIELDS, blocks: BLOCK_146_BLOCKS },
];

static BLOCK_148_FIELDS: &[SchemaField] = &[
    SchemaField { name: "percentage", flag: "instrument-reference-leaks.percentage", kind: FieldKind::Float, default_str: "0", usage: "Percentage [0-100] of request or message buffers to instrument for reference leaks. Set to 0 to disable." },
    SchemaField { name: "before_reuse_period", flag: "instrument-reference-leaks.before-reuse-period", kind: FieldKind::Duration, default_str: "120000000000", usage: "Period after a buffer instrumented for referenced leaks is nominally freed until the buffer is uninstrumented and effectively freed to be reused. After this period, any lingering references to the buffer may potentially be dereferenced again with no detection." },
    SchemaField { name: "max_inflight_instrumented_bytes", flag: "instrument-reference-leaks.max-inflight-instrumented-bytes", kind: FieldKind::Int, default_str: "0", usage: "Maximum sum of length of buffers instrumented at any given time, in bytes. When surpassed, incoming buffers will not be instrumented, regardless of the configured percentage. Zero means no limit." },
];

static BLOCK_148_BLOCKS: &[SchemaBlock] = &[

];

pub static EXTRA_TOP_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "api", fields: BLOCK_0_FIELDS, blocks: BLOCK_0_BLOCKS },
    SchemaBlock { name: "distributor", fields: BLOCK_15_FIELDS, blocks: BLOCK_15_BLOCKS },
    SchemaBlock { name: "querier", fields: BLOCK_30_FIELDS, blocks: BLOCK_30_BLOCKS },
    SchemaBlock { name: "frontend_worker", fields: BLOCK_37_FIELDS, blocks: BLOCK_37_BLOCKS },
    SchemaBlock { name: "frontend", fields: BLOCK_46_FIELDS, blocks: BLOCK_46_BLOCKS },
    SchemaBlock { name: "compactor", fields: BLOCK_58_FIELDS, blocks: BLOCK_58_BLOCKS },
    SchemaBlock { name: "compactor_scheduler", fields: BLOCK_62_FIELDS, blocks: BLOCK_62_BLOCKS },
    SchemaBlock { name: "store_gateway", fields: BLOCK_69_FIELDS, blocks: BLOCK_69_BLOCKS },
    SchemaBlock { name: "tenant_federation", fields: BLOCK_70_FIELDS, blocks: BLOCK_70_BLOCKS },
    SchemaBlock { name: "vault", fields: BLOCK_76_FIELDS, blocks: BLOCK_76_BLOCKS },
    SchemaBlock { name: "ruler", fields: BLOCK_97_FIELDS, blocks: BLOCK_97_BLOCKS },
    SchemaBlock { name: "ruler_storage", fields: BLOCK_111_FIELDS, blocks: BLOCK_111_BLOCKS },
    SchemaBlock { name: "alertmanager", fields: BLOCK_120_FIELDS, blocks: BLOCK_120_BLOCKS },
    SchemaBlock { name: "alertmanager_storage", fields: BLOCK_132_FIELDS, blocks: BLOCK_132_BLOCKS },
    SchemaBlock { name: "query_scheduler", fields: BLOCK_141_FIELDS, blocks: BLOCK_141_BLOCKS },
    SchemaBlock { name: "overrides_exporter", fields: BLOCK_147_FIELDS, blocks: BLOCK_147_BLOCKS },
    SchemaBlock { name: "instrument_ref_leaks", fields: BLOCK_148_FIELDS, blocks: BLOCK_148_BLOCKS },
];

pub fn find_top_block(name: &str) -> Option<&'static SchemaBlock> {
    EXTRA_TOP_BLOCKS.iter().find(|b| b.name == name)
}

/// Validates that a YAML value contains only known keys for `block`.
pub fn validate_yaml_block(
    val: &Value,
    block: &SchemaBlock,
    path: &str,
) -> Result<(), ConfigError> {
    if val.is_null() {
        return Ok(());
    }
    let map = val.as_mapping().ok_or_else(|| {
        ConfigError::Yaml(format!("{path}: expected YAML mapping"))
    })?;
    for (k, v) in map {
        let key = k.as_str().ok_or_else(|| {
            ConfigError::Yaml(format!("{path}: non-string YAML key"))
        })?;
        if let Some(sub) = block.blocks.iter().find(|b| b.name == key) {
            validate_yaml_block(v, sub, &format!("{path}.{key}"))?;
        } else if !block.fields.iter().any(|f| f.name == key) {
            return Err(ConfigError::UnknownField(format!(
                "field {key} not found in {path}"
            )));
        }
    }
    Ok(())
}

/// Registers all CLI flags belonging to the non-ingester Mimir blocks in `FlagSet<T>`.
pub fn register_extra_flags<T: 'static>(
    f: &mut FlagSet<T>,
    get: impl Fn(&T) -> &BTreeMap<String, String> + Copy + 'static,
    get_mut: impl Fn(&mut T) -> &mut BTreeMap<String, String> + Copy + 'static,
) {
    fn walk<T: 'static>(
        block: &SchemaBlock,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &BTreeMap<String, String> + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut BTreeMap<String, String> + Copy + 'static,
    ) {
        for field in block.fields {
            if field.flag.is_empty() {
                continue;
            }
            let flag_name = field.flag.to_string();
            let kind = field.kind;
            let is_bool = kind == FieldKind::Bool;
            let type_name = match kind {
                FieldKind::Bool => "",
                FieldKind::Int => "int",
                FieldKind::Float => "float",
                FieldKind::Duration => "duration",
                FieldKind::String | FieldKind::Secret | FieldKind::SecretOmitEmpty => "string",
                FieldKind::StringList | FieldKind::Other | FieldKind::Bytes => "value",
            };
            let def = if kind == FieldKind::Duration {
                field
                    .default_str
                    .parse::<i64>()
                    .map(format_go_duration)
                    .unwrap_or_else(|_| field.default_str.to_string())
            } else {
                field.default_str.to_string()
            };
            let def_for_get = def.clone();
            let name_for_set = flag_name.clone();
            let name_for_get = flag_name.clone();
            f.register_raw(
                flag_name,
                is_bool,
                type_name,
                def,
                field.usage,
                move |t, s| {
                    match kind {
                        FieldKind::Bool => {
                            parse_go_bool(s)?;
                        }
                        FieldKind::Int => {
                            crate::flags::go_parse_int(s)?;
                        }
                        FieldKind::Float => {
                            crate::flags::go_parse_float(s)?;
                        }
                        FieldKind::Duration => {
                            // flag.durationValue.Set maps every parse error to errParse.
                            s.parse::<GoDuration>()
                                .map_err(|_| ConfigError::Flag("parse error".to_string()))?;
                        }
                        FieldKind::Bytes => {
                            crate::types::HumanBytes::parse(s)?;
                        }
                        FieldKind::String
                        | FieldKind::StringList
                        | FieldKind::Other
                        | FieldKind::Secret
                        | FieldKind::SecretOmitEmpty => {}
                    }
                    get_mut(t).insert(name_for_set.clone(), s.to_string());
                    Ok(())
                },
                move |t| {
                    get(t)
                        .get(&name_for_get)
                        .cloned()
                        .unwrap_or_else(|| def_for_get.clone())
                },
            );
            if field.name == "instance_interface_names" {
                // `*flagext.StringSlice` with `doc:"default=[<private network interfaces>]"`.
                f.set_usage_default(
                    field.flag,
                    "string",
                    crate::ingester::PRIVATE_NETWORK_INTERFACES_DOC_DEFAULT,
                );
            }
        }
        for sub in block.blocks {
            walk(sub, f, get, get_mut);
        }
    }

    for block in EXTRA_TOP_BLOCKS {
        walk(block, f, get, get_mut);
    }

    for (name, is_bool, type_name) in [
        ("block-builder.instance-id", false, "string"),
        ("block-builder.data-dir", false, "string"),
        ("block-builder.apply-max-global-series-per-user-below", false, "int"),
        ("block-builder.generate-sparse-index-headers", true, ""),
        ("block-builder.verify-blocks-before-upload", true, ""),
        ("block-builder.scheduler.address", false, "string"),
        ("block-builder.scheduler.update-interval", false, "duration"),
        ("block-builder.scheduler.max-update-age", false, "duration"),
        ("block-builder.scheduler.grpc-client-config.cluster-validation.label", false, "string"),
        ("block-builder-scheduler.consumer-group", false, "string"),
        ("block-builder-scheduler.scheduling-interval", false, "duration"),
        ("block-builder-scheduler.job-size", false, "duration"),
        ("block-builder-scheduler.startup-observe-time", false, "duration"),
        ("block-builder-scheduler.job-lease-expiry", false, "duration"),
        ("block-builder-scheduler.lookback-on-no-commit", false, "duration"),
        ("block-builder-scheduler.max-scan-age", false, "duration"),
        ("block-builder-scheduler.max-jobs-per-partition", false, "int"),
        ("block-builder-scheduler.enqueue-interval", false, "duration"),
        ("block-builder-scheduler.job-failures-allowed", false, "int"),
        ("usage-tracker.enabled", true, ""),
        ("usage-tracker.partitions", false, "int"),
    ] {
        let name_s = name.to_string();
        let name_get = name_s.clone();
        f.register_raw(
            name_s.clone(),
            is_bool,
            type_name,
            if is_bool { "false" } else { "" },
            "",
            move |t, s| {
                if is_bool {
                    parse_go_bool(s)?;
                } else if type_name == "int" {
                    crate::flags::go_parse_int(s)?;
                } else if type_name == "duration" {
                    s.parse::<GoDuration>()
                        .map_err(|_| ConfigError::Flag("parse error".to_string()))?;
                }
                get_mut(t).insert(name_s.clone(), s.to_string());
                Ok(())
            },
            move |t| get(t).get(&name_get).cloned().unwrap_or_default(),
        );
    }

    // `continuoustest.Config` (`yaml:"-"`, pkg/continuoustest/{manager,client,*_test}.go): flags
    // are registered for every target in Go and only consumed by `-target=continuous-test`
    // (not ported), so they are accepted and validated like Go but have no effect elsewhere.
    for (name, kind) in [
        ("tests.basic-auth-password", "string"),
        ("tests.basic-auth-user", "string"),
        ("tests.bearer-token", "string"),
        ("tests.client.cluster-validation.label", "string"),
        ("tests.client.user-agent", "string"),
        ("tests.ingest-storage-record.consumer-group", "string"),
        ("tests.ingest-storage-record.enabled", "bool"),
        ("tests.ingest-storage-record.max-jump-size", "int"),
        ("tests.ingest-storage-record.max-records-per-run", "int"),
        ("tests.ingest-storage-record.records-processed-percent", "int"),
        ("tests.read-endpoint", "url"),
        ("tests.read-timeout", "duration"),
        ("tests.run-interval", "duration"),
        ("tests.send-chunks-debugging-header", "bool"),
        ("tests.smoke-test", "bool"),
        ("tests.tenant-id", "string"),
        ("tests.write-batch-size", "int"),
        ("tests.write-endpoint", "url"),
        ("tests.write-protocol", "write-protocol"),
        ("tests.write-read-ooo-test.enabled", "bool"),
        ("tests.write-read-ooo-test.max-ooo-lag", "duration"),
        ("tests.write-read-ooo-test.max-query-age", "duration"),
        ("tests.write-read-ooo-test.num-series", "int"),
        ("tests.write-read-series-test.fan-out-query-enabled", "bool"),
        ("tests.write-read-series-test.float-samples-enabled", "bool"),
        ("tests.write-read-series-test.histogram-samples-enabled", "bool"),
        ("tests.write-read-series-test.max-query-age", "duration"),
        ("tests.write-read-series-test.num-series", "int"),
        ("tests.write-timeout", "duration"),
    ] {
        let name_s = name.to_string();
        let name_get = name_s.clone();
        let is_bool = kind == "bool";
        f.register_raw(
            name_s.clone(),
            is_bool,
            match kind {
                "bool" => "",
                "int" => "int",
                "duration" => "duration",
                "write-protocol" => "value",
                _ => "string",
            },
            if is_bool { "false" } else { "" },
            "",
            move |t, s| {
                match kind {
                    "bool" => {
                        parse_go_bool(s)?;
                    }
                    "int" => {
                        crate::flags::go_parse_int(s)?;
                    }
                    "duration" => {
                        s.parse::<GoDuration>()
                            .map_err(|_| ConfigError::Flag("parse error".to_string()))?;
                    }
                    "write-protocol"
                        if !matches!(s, "prometheus" | "prometheus2" | "otlp-http") =>
                    {
                        return Err(ConfigError::Flag(format!("invalid write protocol {s:?}")));
                    }
                    _ => {}
                }
                get_mut(t).insert(name_s.clone(), s.to_string());
                Ok(())
            },
            move |t| get(t).get(&name_get).cloned().unwrap_or_default(),
        );
    }

    for suffix in crate::storage::StorageBackendConfig::common_flag_suffixes() {
        let name_s = format!("usage-tracker.snapshots-storage.{suffix}");
        let name_get = name_s.clone();
        let is_bool = matches!(
            *suffix,
            "s3.insecure"
                | "s3.native-aws-auth-enabled"
                | "s3.send-content-md5"
                | "s3.dualstack-enabled"
                | "s3.http.insecure-skip-verify"
                | "s3.http.force-attempt-http2"
                | "s3.trace.enabled"
                | "gcs.enable-upload-retries"
                | "gcs.http.insecure-skip-verify"
                | "gcs.http.force-attempt-http2"
                | "azure.http.insecure-skip-verify"
                | "azure.http.force-attempt-http2"
        );
        let type_name = if is_bool { "" } else { "string" };
        f.register_raw(
            name_s.clone(),
            is_bool,
            type_name,
            if is_bool { "false" } else { "" },
            "",
            move |t, s| {
                if is_bool {
                    parse_go_bool(s)?;
                }
                get_mut(t).insert(name_s.clone(), s.to_string());
                Ok(())
            },
            move |t| get(t).get(&name_get).cloned().unwrap_or_default(),
        );
    }
}
