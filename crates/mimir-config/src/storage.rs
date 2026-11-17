//! Object storage, TSDB blocks storage, and Kafka ingest storage configuration.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/storage/bucket/*
//! - github.com/grafana/mimir/pkg/storage/tsdb/*
//! - github.com/grafana/mimir/pkg/storage/ingest/config.go
//! - github.com/grafana/mimir/pkg/storage/ingest/heap_merger.go
//! - github.com/grafana/mimir/pkg/storage/indexheader/header.go
//! - github.com/grafana/mimir/pkg/ingester/lookupplan/config.go

use serde::{Deserialize, Serialize};

use crate::duration::GoDuration;
use crate::error::ConfigError;
use crate::flags::FlagSet;
use crate::types::{LimitsMap, S3BucketLookupType, SASLMechanism, StringSliceCSV};

pub const MIN_PRODUCER_RECORD_DATA_BYTES_LIMIT: i64 = 1024 * 1024;
pub const MAX_PRODUCER_RECORD_DATA_BYTES_LIMIT: i64 = 16_000_000 - 16_384;
pub const MIN_KAFKA_REQUEST_TIMEOUT_OVERHEAD_NANOS: i64 = 100_000_000; // 100ms

pub const TSDB_MIN_WRITE_BUFFER_SIZE: i64 = 64 * 1024;
pub const TSDB_MAX_WRITE_BUFFER_SIZE: i64 = 8 * 1024 * 1024;
pub const TSDB_DEFAULT_WRITE_BUFFER_SIZE: i64 = 4 * 1024 * 1024;
pub const TSDB_DEFAULT_WAL_SEGMENT_SIZE: i64 = 128 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BucketHTTPConfig {
    pub idle_conn_timeout: GoDuration,
    pub response_header_timeout: GoDuration,
    pub insecure_skip_verify: bool,
    pub tls_handshake_timeout: GoDuration,
    pub expect_continue_timeout: GoDuration,
    pub max_idle_connections: i64,
    pub max_idle_connections_per_host: i64,
    pub max_connections_per_host: i64,
    pub force_attempt_http2: bool,
    pub tls_ca_path: String,
    pub tls_cert_path: String,
    pub tls_key_path: String,
    pub tls_server_name: String,
}

impl Default for BucketHTTPConfig {
    fn default() -> Self {
        Self {
            idle_conn_timeout: GoDuration::from_secs(90),
            response_header_timeout: GoDuration::from_mins(2),
            insecure_skip_verify: false,
            tls_handshake_timeout: GoDuration::from_secs(10),
            expect_continue_timeout: GoDuration::from_secs(1),
            max_idle_connections: 100,
            max_idle_connections_per_host: 100,
            max_connections_per_host: 0,
            force_attempt_http2: false,
            tls_ca_path: String::new(),
            tls_cert_path: String::new(),
            tls_key_path: String::new(),
            tls_server_name: String::new(),
        }
    }
}

impl BucketHTTPConfig {
    pub fn register_flags_into<T: 'static>(
        &self,
        prefix: &str,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        f.go_duration_var(&format!("{prefix}http.idle-conn-timeout"), self.idle_conn_timeout, "The time an idle connection remains idle before closing.", move |t| &get(t).idle_conn_timeout, move |t| &mut get_mut(t).idle_conn_timeout);
        f.go_duration_var(&format!("{prefix}http.response-header-timeout"), self.response_header_timeout, "The amount of time the client waits for a server's response headers.", move |t| &get(t).response_header_timeout, move |t| &mut get_mut(t).response_header_timeout);
        f.bool_var(&format!("{prefix}http.insecure-skip-verify"), self.insecure_skip_verify, "If the client connects to object storage via HTTPS and this option is enabled, the client accepts any certificate and hostname.", move |t| &get(t).insecure_skip_verify, move |t| &mut get_mut(t).insecure_skip_verify);
        f.go_duration_var(&format!("{prefix}tls-handshake-timeout"), self.tls_handshake_timeout, "Maximum time to wait for a TLS handshake. Set to 0 for no limit.", move |t| &get(t).tls_handshake_timeout, move |t| &mut get_mut(t).tls_handshake_timeout);
        f.go_duration_var(&format!("{prefix}expect-continue-timeout"), self.expect_continue_timeout, "The time to wait for a server's first response headers after fully writing the request headers.", move |t| &get(t).expect_continue_timeout, move |t| &mut get_mut(t).expect_continue_timeout);
        f.i64_var(&format!("{prefix}max-idle-connections"), self.max_idle_connections, "Maximum number of idle (keep-alive) connections across all hosts.", move |t| &get(t).max_idle_connections, move |t| &mut get_mut(t).max_idle_connections);
        f.i64_var(&format!("{prefix}max-idle-connections-per-host"), self.max_idle_connections_per_host, "Maximum number of idle (keep-alive) connections to keep per-host.", move |t| &get(t).max_idle_connections_per_host, move |t| &mut get_mut(t).max_idle_connections_per_host);
        f.i64_var(&format!("{prefix}max-connections-per-host"), self.max_connections_per_host, "Maximum number of connections per host.", move |t| &get(t).max_connections_per_host, move |t| &mut get_mut(t).max_connections_per_host);
        f.bool_var(&format!("{prefix}http.force-attempt-http2"), self.force_attempt_http2, "If enabled, the HTTP client attempts HTTP/2 for HTTPS connections.", move |t| &get(t).force_attempt_http2, move |t| &mut get_mut(t).force_attempt_http2);
        f.string_var(&format!("{prefix}http.tls-ca-path"), &self.tls_ca_path, "Path to the Certificate Authority (CA) certificates to validate the server certificate.", move |t| &get(t).tls_ca_path, move |t| &mut get_mut(t).tls_ca_path);
        f.string_var(&format!("{prefix}http.tls-cert-path"), &self.tls_cert_path, "Path to the client certificate.", move |t| &get(t).tls_cert_path, move |t| &mut get_mut(t).tls_cert_path);
        f.string_var(&format!("{prefix}http.tls-key-path"), &self.tls_key_path, "Path to the key for the client certificate.", move |t| &get(t).tls_key_path, move |t| &mut get_mut(t).tls_key_path);
        f.string_var(&format!("{prefix}http.tls-server-name"), &self.tls_server_name, "Override the expected name on the server certificate.", move |t| &get(t).tls_server_name, move |t| &mut get_mut(t).tls_server_name);
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct S3SSEConfig {
    #[serde(rename = "type")]
    pub sse_type: String,
    pub kms_key_id: String,
    pub kms_encryption_context: String,
}

impl S3SSEConfig {
    pub fn register_flags_into<T: 'static>(
        &self,
        prefix: &str,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        f.string_var(&format!("{prefix}type"), &self.sse_type, "Enable AWS Server Side Encryption. Supported values: SSE-KMS, SSE-S3.", move |t| &get(t).sse_type, move |t| &mut get_mut(t).sse_type);
        f.string_var(&format!("{prefix}kms-key-id"), &self.kms_key_id, "KMS Key ID used to encrypt objects in S3", move |t| &get(t).kms_key_id, move |t| &mut get_mut(t).kms_key_id);
        f.string_var(&format!("{prefix}kms-encryption-context"), &self.kms_encryption_context, "KMS Encryption Context used for object encryption. It expects JSON formatted string.", move |t| &get(t).kms_encryption_context, move |t| &mut get_mut(t).kms_encryption_context);
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if !self.sse_type.is_empty() && self.sse_type != "SSE-KMS" && self.sse_type != "SSE-S3" {
            return Err(ConfigError::Validation(
                "unsupported S3 SSE type".to_string(),
            ));
        }
        if !self.kms_encryption_context.is_empty() {
            let parsed: Result<serde_json::Value, _> =
                serde_json::from_str(&self.kms_encryption_context);
            match parsed {
                Ok(serde_json::Value::Object(_)) => {}
                _ => {
                    return Err(ConfigError::Validation(
                        "invalid S3 SSE encryption context".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct S3TraceConfig {
    pub enabled: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct S3Config {
    pub endpoint: String,
    pub region: String,
    pub bucket_name: String,
    #[serde(serialize_with = "crate::secret::serialize_flagext_secret")]
    pub secret_access_key: String,
    pub access_key_id: String,
    #[serde(serialize_with = "crate::secret::serialize_flagext_secret")]
    pub session_token: String,
    pub insecure: bool,
    pub signature_version: String,
    pub list_objects_version: String,
    pub bucket_lookup_type: S3BucketLookupType,
    pub dualstack_enabled: bool,
    pub storage_class: String,
    pub native_aws_auth_enabled: bool,
    pub part_size: u64,
    pub send_content_md5: bool,
    pub sts_endpoint: String,
    pub sse: S3SSEConfig,
    pub http: BucketHTTPConfig,
    pub trace: S3TraceConfig,
}

impl Default for S3Config {
    fn default() -> Self {
        Self {
            endpoint: String::new(),
            region: String::new(),
            bucket_name: String::new(),
            secret_access_key: String::new(),
            access_key_id: String::new(),
            session_token: String::new(),
            insecure: false,
            signature_version: "v4".to_string(),
            list_objects_version: String::new(),
            bucket_lookup_type: S3BucketLookupType::Auto,
            dualstack_enabled: true,
            storage_class: String::new(),
            native_aws_auth_enabled: false,
            part_size: 0,
            send_content_md5: false,
            sts_endpoint: String::new(),
            sse: S3SSEConfig::default(),
            http: BucketHTTPConfig::default(),
            trace: S3TraceConfig::default(),
        }
    }
}

impl S3Config {
    pub fn register_flags_into<T: 'static>(
        &self,
        prefix: &str,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        f.string_var(&format!("{prefix}s3.access-key-id"), &self.access_key_id, "S3 access key ID", move |t| &get(t).access_key_id, move |t| &mut get_mut(t).access_key_id);
        f.string_var(&format!("{prefix}s3.secret-access-key"), &self.secret_access_key, "S3 secret access key", move |t| &get(t).secret_access_key, move |t| &mut get_mut(t).secret_access_key);
        f.string_var(&format!("{prefix}s3.session-token"), &self.session_token, "S3 session token", move |t| &get(t).session_token, move |t| &mut get_mut(t).session_token);
        f.string_var(&format!("{prefix}s3.bucket-name"), &self.bucket_name, "S3 bucket name", move |t| &get(t).bucket_name, move |t| &mut get_mut(t).bucket_name);
        f.string_var(&format!("{prefix}s3.region"), &self.region, "S3 region.", move |t| &get(t).region, move |t| &mut get_mut(t).region);
        f.string_var(&format!("{prefix}s3.endpoint"), &self.endpoint, "The S3 bucket endpoint.", move |t| &get(t).endpoint, move |t| &mut get_mut(t).endpoint);
        f.bool_var(&format!("{prefix}s3.insecure"), self.insecure, "If enabled, use http:// for the S3 endpoint instead of https://.", move |t| &get(t).insecure, move |t| &mut get_mut(t).insecure);
        f.string_var(&format!("{prefix}s3.signature-version"), &self.signature_version, "The signature version to use for authenticating against S3. Supported values are: v4, v2.", move |t| &get(t).signature_version, move |t| &mut get_mut(t).signature_version);
        f.string_var(&format!("{prefix}s3.list-objects-version"), &self.list_objects_version, "Use a specific version of the S3 list object API. Supported values are v1 or v2.", move |t| &get(t).list_objects_version, move |t| &mut get_mut(t).list_objects_version);
        f.string_var(&format!("{prefix}s3.storage-class"), &self.storage_class, "The S3 storage class to use, not set by default.", move |t| &get(t).storage_class, move |t| &mut get_mut(t).storage_class);
        f.bool_var(&format!("{prefix}s3.native-aws-auth-enabled"), self.native_aws_auth_enabled, "If enabled, it will use the default authentication methods of the AWS SDK.", move |t| &get(t).native_aws_auth_enabled, move |t| &mut get_mut(t).native_aws_auth_enabled);
        f.u64_var(&format!("{prefix}s3.part-size"), self.part_size, "The minimum file size in bytes used for multipart uploads.", move |t| &get(t).part_size, move |t| &mut get_mut(t).part_size);
        f.bool_var(&format!("{prefix}s3.send-content-md5"), self.send_content_md5, "If enabled, a Content-MD5 header is sent with S3 Put Object requests.", move |t| &get(t).send_content_md5, move |t| &mut get_mut(t).send_content_md5);
        f.s3_bucket_lookup_type_var(&format!("{prefix}s3.bucket-lookup-type"), self.bucket_lookup_type, "Bucket lookup style type, used to access bucket in S3-compatible service.", move |t| &get(t).bucket_lookup_type, move |t| &mut get_mut(t).bucket_lookup_type);
        f.bool_var(&format!("{prefix}s3.dualstack-enabled"), self.dualstack_enabled, "When enabled, direct all AWS S3 requests to the dual-stack IPv4/IPv6 endpoint.", move |t| &get(t).dualstack_enabled, move |t| &mut get_mut(t).dualstack_enabled);
        f.string_var(&format!("{prefix}s3.sts-endpoint"), &self.sts_endpoint, "Accessing S3 resources using temporary, secure credentials provided by AWS Security Token Service.", move |t| &get(t).sts_endpoint, move |t| &mut get_mut(t).sts_endpoint);
        self.sse.register_flags_into(&format!("{prefix}s3.sse."), f, move |t| &get(t).sse, move |t| &mut get_mut(t).sse);
        self.http.register_flags_into(&format!("{prefix}s3."), f, move |t| &get(t).http, move |t| &mut get_mut(t).http);
        f.bool_var(&format!("{prefix}s3.trace.enabled"), self.trace.enabled, "Enable S3 client HTTP tracing.", move |t| &get(t).trace.enabled, move |t| &mut get_mut(t).trace.enabled);
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.signature_version != "v4" && self.signature_version != "v2" {
            return Err(ConfigError::Validation(
                "unsupported signature version (supported values: v4, v2)".to_string(),
            ));
        }
        if !self.endpoint.is_empty() && !self.bucket_name.is_empty() {
            let first = self.endpoint.split('.').next().unwrap_or("");
            if !first.is_empty() && first == self.bucket_name {
                return Err(ConfigError::Validation(
                    "the endpoint must not prefixed with the bucket name".to_string(),
                ));
            }
        }
        self.sse.validate()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GCSConfig {
    pub bucket_name: String,
    #[serde(serialize_with = "crate::secret::serialize_flagext_secret")]
    pub service_account: String,
    pub enable_upload_retries: bool,
    pub max_retries: i64,
    pub http: BucketHTTPConfig,
}

impl Default for GCSConfig {
    fn default() -> Self {
        Self {
            bucket_name: String::new(),
            service_account: String::new(),
            enable_upload_retries: true,
            max_retries: 20,
            http: BucketHTTPConfig::default(),
        }
    }
}

impl GCSConfig {
    pub fn register_flags_into<T: 'static>(
        &self,
        prefix: &str,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        f.string_var(&format!("{prefix}gcs.bucket-name"), &self.bucket_name, "GCS bucket name", move |t| &get(t).bucket_name, move |t| &mut get_mut(t).bucket_name);
        f.string_var(&format!("{prefix}gcs.service-account"), &self.service_account, "JSON service account key.", move |t| &get(t).service_account, move |t| &mut get_mut(t).service_account);
        f.bool_var(&format!("{prefix}gcs.enable-upload-retries"), self.enable_upload_retries, "Enable automatic retries for GCS uploads.", move |t| &get(t).enable_upload_retries, move |t| &mut get_mut(t).enable_upload_retries);
        f.i64_var(&format!("{prefix}gcs.max-retries"), self.max_retries, "Maximum number of attempts for GCS operations.", move |t| &get(t).max_retries, move |t| &mut get_mut(t).max_retries);
        self.http.register_flags_into(&format!("{prefix}gcs."), f, move |t| &get(t).http, move |t| &mut get_mut(t).http);
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AzureConfig {
    pub account_name: String,
    #[serde(serialize_with = "crate::secret::serialize_flagext_secret")]
    pub account_key: String,
    #[serde(serialize_with = "crate::secret::serialize_flagext_secret")]
    pub connection_string: String,
    pub container_name: String,
    pub endpoint_suffix: String,
    pub max_retries: i64,
    pub user_assigned_id: String,
    pub http: BucketHTTPConfig,
}

impl Default for AzureConfig {
    fn default() -> Self {
        Self {
            account_name: String::new(),
            account_key: String::new(),
            connection_string: String::new(),
            container_name: String::new(),
            endpoint_suffix: String::new(),
            max_retries: 20,
            user_assigned_id: String::new(),
            http: BucketHTTPConfig::default(),
        }
    }
}

impl AzureConfig {
    pub fn register_flags_into<T: 'static>(
        &self,
        prefix: &str,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        f.string_var(&format!("{prefix}azure.account-name"), &self.account_name, "Azure storage account name", move |t| &get(t).account_name, move |t| &mut get_mut(t).account_name);
        f.string_var(&format!("{prefix}azure.account-key"), &self.account_key, "Azure storage account key.", move |t| &get(t).account_key, move |t| &mut get_mut(t).account_key);
        f.string_var(&format!("{prefix}azure.connection-string"), &self.connection_string, "Azure storage connection string.", move |t| &get(t).connection_string, move |t| &mut get_mut(t).connection_string);
        f.string_var(&format!("{prefix}azure.container-name"), &self.container_name, "Azure storage container name", move |t| &get(t).container_name, move |t| &mut get_mut(t).container_name);
        f.string_var(&format!("{prefix}azure.endpoint-suffix"), &self.endpoint_suffix, "Azure storage endpoint suffix without schema.", move |t| &get(t).endpoint_suffix, move |t| &mut get_mut(t).endpoint_suffix);
        f.i64_var(&format!("{prefix}azure.max-retries"), self.max_retries, "Number of retries for recoverable errors", move |t| &get(t).max_retries, move |t| &mut get_mut(t).max_retries);
        f.string_var(&format!("{prefix}azure.user-assigned-id"), &self.user_assigned_id, "User assigned managed identity.", move |t| &get(t).user_assigned_id, move |t| &mut get_mut(t).user_assigned_id);
        self.http.register_flags_into(&format!("{prefix}azure."), f, move |t| &get(t).http, move |t| &mut get_mut(t).http);
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SwiftConfig {
    pub application_credential_id: String,
    pub application_credential_name: String,
    #[serde(serialize_with = "crate::secret::serialize_flagext_secret")]
    pub application_credential_secret: String,
    pub auth_version: i64,
    pub auth_url: String,
    pub username: String,
    pub user_domain_name: String,
    pub user_domain_id: String,
    pub user_id: String,
    #[serde(serialize_with = "crate::secret::serialize_flagext_secret")]
    pub password: String,
    pub domain_id: String,
    pub domain_name: String,
    pub project_id: String,
    pub project_name: String,
    pub project_domain_id: String,
    pub project_domain_name: String,
    pub region_name: String,
    pub container_name: String,
    pub max_retries: i64,
    pub connect_timeout: GoDuration,
    pub request_timeout: GoDuration,
}

impl Default for SwiftConfig {
    fn default() -> Self {
        Self {
            application_credential_id: String::new(),
            application_credential_name: String::new(),
            application_credential_secret: String::new(),
            auth_version: 0,
            auth_url: String::new(),
            username: String::new(),
            user_domain_name: String::new(),
            user_domain_id: String::new(),
            user_id: String::new(),
            password: String::new(),
            domain_id: String::new(),
            domain_name: String::new(),
            project_id: String::new(),
            project_name: String::new(),
            project_domain_id: String::new(),
            project_domain_name: String::new(),
            region_name: String::new(),
            container_name: String::new(),
            max_retries: 3,
            connect_timeout: GoDuration::from_secs(10),
            request_timeout: GoDuration::from_secs(5),
        }
    }
}

impl SwiftConfig {
    pub fn register_flags_into<T: 'static>(
        &self,
        prefix: &str,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        f.string_var(&format!("{prefix}swift.application-credential-id"), &self.application_credential_id, "OpenStack Swift application credential id", move |t| &get(t).application_credential_id, move |t| &mut get_mut(t).application_credential_id);
        f.string_var(&format!("{prefix}swift.application-credential-name"), &self.application_credential_name, "OpenStack Swift application credential name", move |t| &get(t).application_credential_name, move |t| &mut get_mut(t).application_credential_name);
        f.string_var(&format!("{prefix}swift.application-credential-secret"), &self.application_credential_secret, "OpenStack Swift application credential secret", move |t| &get(t).application_credential_secret, move |t| &mut get_mut(t).application_credential_secret);
        f.i64_var(&format!("{prefix}swift.auth-version"), self.auth_version, "OpenStack Swift authentication API version. 0 to autodetect.", move |t| &get(t).auth_version, move |t| &mut get_mut(t).auth_version);
        f.string_var(&format!("{prefix}swift.auth-url"), &self.auth_url, "OpenStack Swift authentication URL", move |t| &get(t).auth_url, move |t| &mut get_mut(t).auth_url);
        f.string_var(&format!("{prefix}swift.username"), &self.username, "OpenStack Swift username.", move |t| &get(t).username, move |t| &mut get_mut(t).username);
        f.string_var(&format!("{prefix}swift.user-domain-name"), &self.user_domain_name, "OpenStack Swift user's domain name.", move |t| &get(t).user_domain_name, move |t| &mut get_mut(t).user_domain_name);
        f.string_var(&format!("{prefix}swift.user-domain-id"), &self.user_domain_id, "OpenStack Swift user's domain ID.", move |t| &get(t).user_domain_id, move |t| &mut get_mut(t).user_domain_id);
        f.string_var(&format!("{prefix}swift.user-id"), &self.user_id, "OpenStack Swift user ID.", move |t| &get(t).user_id, move |t| &mut get_mut(t).user_id);
        f.string_var(&format!("{prefix}swift.password"), &self.password, "OpenStack Swift API key.", move |t| &get(t).password, move |t| &mut get_mut(t).password);
        f.string_var(&format!("{prefix}swift.domain-id"), &self.domain_id, "OpenStack Swift user's domain ID.", move |t| &get(t).domain_id, move |t| &mut get_mut(t).domain_id);
        f.string_var(&format!("{prefix}swift.domain-name"), &self.domain_name, "OpenStack Swift user's domain name.", move |t| &get(t).domain_name, move |t| &mut get_mut(t).domain_name);
        f.string_var(&format!("{prefix}swift.project-id"), &self.project_id, "OpenStack Swift project ID.", move |t| &get(t).project_id, move |t| &mut get_mut(t).project_id);
        f.string_var(&format!("{prefix}swift.project-name"), &self.project_name, "OpenStack Swift project name.", move |t| &get(t).project_name, move |t| &mut get_mut(t).project_name);
        f.string_var(&format!("{prefix}swift.project-domain-id"), &self.project_domain_id, "ID of the OpenStack Swift project's domain.", move |t| &get(t).project_domain_id, move |t| &mut get_mut(t).project_domain_id);
        f.string_var(&format!("{prefix}swift.project-domain-name"), &self.project_domain_name, "Name of the OpenStack Swift project's domain.", move |t| &get(t).project_domain_name, move |t| &mut get_mut(t).project_domain_name);
        f.string_var(&format!("{prefix}swift.region-name"), &self.region_name, "OpenStack Swift Region to use.", move |t| &get(t).region_name, move |t| &mut get_mut(t).region_name);
        f.string_var(&format!("{prefix}swift.container-name"), &self.container_name, "Name of the OpenStack Swift container to put chunks in.", move |t| &get(t).container_name, move |t| &mut get_mut(t).container_name);
        f.i64_var(&format!("{prefix}swift.max-retries"), self.max_retries, "Max retries on requests error.", move |t| &get(t).max_retries, move |t| &mut get_mut(t).max_retries);
        f.go_duration_var(&format!("{prefix}swift.connect-timeout"), self.connect_timeout, "Time after which a connection attempt is aborted.", move |t| &get(t).connect_timeout, move |t| &mut get_mut(t).connect_timeout);
        f.go_duration_var(&format!("{prefix}swift.request-timeout"), self.request_timeout, "Time after which an idle request is aborted.", move |t| &get(t).request_timeout, move |t| &mut get_mut(t).request_timeout);
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FilesystemConfig {
    pub dir: String,
}

impl FilesystemConfig {
    pub fn register_flags_into_with_dir<T: 'static>(
        &self,
        prefix: &str,
        default_dir: &str,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        f.string_var(
            &format!("{prefix}filesystem.dir"),
            default_dir,
            "Local filesystem storage directory.",
            move |t| &get(t).dir,
            move |t| &mut get_mut(t).dir,
        );
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StorageBackendConfig {
    pub backend: String,
    pub s3: S3Config,
    pub gcs: GCSConfig,
    pub azure: AzureConfig,
    pub swift: SwiftConfig,
    pub filesystem: FilesystemConfig,
}

impl Default for StorageBackendConfig {
    fn default() -> Self {
        Self {
            backend: "filesystem".to_string(),
            s3: S3Config::default(),
            gcs: GCSConfig::default(),
            azure: AzureConfig::default(),
            swift: SwiftConfig::default(),
            filesystem: FilesystemConfig::default(),
        }
    }
}

impl StorageBackendConfig {
    pub fn register_flags_into_with_dir<T: 'static>(
        &self,
        prefix: &str,
        default_dir: &str,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        self.s3.register_flags_into(prefix, f, move |t| &get(t).s3, move |t| &mut get_mut(t).s3);
        self.gcs.register_flags_into(prefix, f, move |t| &get(t).gcs, move |t| &mut get_mut(t).gcs);
        self.azure.register_flags_into(prefix, f, move |t| &get(t).azure, move |t| &mut get_mut(t).azure);
        self.swift.register_flags_into(prefix, f, move |t| &get(t).swift, move |t| &mut get_mut(t).swift);
        self.filesystem
            .register_flags_into_with_dir(prefix, default_dir, f, move |t| &get(t).filesystem, move |t| &mut get_mut(t).filesystem);
        f.string_var(&format!("{prefix}backend"), &self.backend, "Backend storage to use. Supported backends are: s3, gcs, azure, swift, filesystem.", move |t| &get(t).backend, move |t| &mut get_mut(t).backend);
    }

    pub fn validate_with_extra_backends(&self, extra: &[&str]) -> Result<(), ConfigError> {
        let supported = ["s3", "gcs", "azure", "swift", "filesystem"];
        if !supported.contains(&self.backend.as_str()) && !extra.contains(&self.backend.as_str()) {
            return Err(ConfigError::Validation(
                "unsupported storage backend".to_string(),
            ));
        }
        if self.backend == "s3" {
            self.s3.validate()?;
        }
        Ok(())
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        self.validate_with_extra_backends(&[])
    }

    /// Returns the flag suffixes relative to `<prefix>` belonging to `StorageBackendConfig`.
    pub fn common_flag_suffixes() -> &'static [&'static str] {
        &[
            "backend",
            "filesystem.dir",
            "s3.access-key-id",
            "s3.secret-access-key",
            "s3.session-token",
            "s3.bucket-name",
            "s3.region",
            "s3.endpoint",
            "s3.insecure",
            "s3.signature-version",
            "s3.list-objects-version",
            "s3.storage-class",
            "s3.native-aws-auth-enabled",
            "s3.part-size",
            "s3.send-content-md5",
            "s3.bucket-lookup-type",
            "s3.dualstack-enabled",
            "s3.sts-endpoint",
            "s3.sse.type",
            "s3.sse.kms-key-id",
            "s3.sse.kms-encryption-context",
            "s3.http.idle-conn-timeout",
            "s3.http.response-header-timeout",
            "s3.http.insecure-skip-verify",
            "s3.tls-handshake-timeout",
            "s3.expect-continue-timeout",
            "s3.max-idle-connections",
            "s3.max-idle-connections-per-host",
            "s3.max-connections-per-host",
            "s3.http.force-attempt-http2",
            "s3.http.tls-ca-path",
            "s3.http.tls-cert-path",
            "s3.http.tls-key-path",
            "s3.http.tls-server-name",
            "s3.trace.enabled",
            "gcs.bucket-name",
            "gcs.service-account",
            "gcs.enable-upload-retries",
            "gcs.max-retries",
            "gcs.http.idle-conn-timeout",
            "gcs.http.response-header-timeout",
            "gcs.http.insecure-skip-verify",
            "gcs.tls-handshake-timeout",
            "gcs.expect-continue-timeout",
            "gcs.max-idle-connections",
            "gcs.max-idle-connections-per-host",
            "gcs.max-connections-per-host",
            "gcs.http.force-attempt-http2",
            "gcs.http.tls-ca-path",
            "gcs.http.tls-cert-path",
            "gcs.http.tls-key-path",
            "gcs.http.tls-server-name",
            "azure.account-name",
            "azure.account-key",
            "azure.connection-string",
            "azure.container-name",
            "azure.endpoint-suffix",
            "azure.max-retries",
            "azure.user-assigned-id",
            "azure.http.idle-conn-timeout",
            "azure.http.response-header-timeout",
            "azure.http.insecure-skip-verify",
            "azure.tls-handshake-timeout",
            "azure.expect-continue-timeout",
            "azure.max-idle-connections",
            "azure.max-idle-connections-per-host",
            "azure.max-connections-per-host",
            "azure.http.force-attempt-http2",
            "azure.http.tls-ca-path",
            "azure.http.tls-cert-path",
            "azure.http.tls-key-path",
            "azure.http.tls-server-name",
            "swift.application-credential-id",
            "swift.application-credential-name",
            "swift.application-credential-secret",
            "swift.auth-version",
            "swift.auth-url",
            "swift.username",
            "swift.user-domain-name",
            "swift.user-domain-id",
            "swift.user-id",
            "swift.password",
            "swift.domain-id",
            "swift.domain-name",
            "swift.project-id",
            "swift.project-name",
            "swift.project-domain-id",
            "swift.project-domain-name",
            "swift.region-name",
            "swift.container-name",
            "swift.max-retries",
            "swift.connect-timeout",
            "swift.request-timeout",
        ]
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct BucketConfig {
    #[serde(flatten)]
    pub backend_cfg: StorageBackendConfig,
    pub storage_prefix: String,
}

impl Default for BucketConfig {
    fn default() -> Self {
        let mut backend_cfg = StorageBackendConfig::default();
        backend_cfg.filesystem.dir = "blocks".to_string();
        Self {
            backend_cfg,
            storage_prefix: String::new(),
        }
    }
}

impl BucketConfig {
    pub fn register_flags_into_with_dir<T: 'static>(
        &self,
        prefix: &str,
        default_dir: &str,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        self.backend_cfg
            .register_flags_into_with_dir(prefix, default_dir, f, move |t| &get(t).backend_cfg, move |t| &mut get_mut(t).backend_cfg);
        f.string_var(&format!("{prefix}storage-prefix"), &self.storage_prefix, "Prefix for all objects stored in the backend storage.", move |t| &get(t).storage_prefix, move |t| &mut get_mut(t).storage_prefix);
    }

    pub fn bucket_name(&self) -> &str {
        match self.backend_cfg.backend.as_str() {
            "s3" => &self.backend_cfg.s3.bucket_name,
            "gcs" => &self.backend_cfg.gcs.bucket_name,
            "azure" => &self.backend_cfg.azure.container_name,
            "swift" => &self.backend_cfg.swift.container_name,
            "filesystem" => &self.backend_cfg.filesystem.dir,
            _ => "",
        }
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if !self.storage_prefix.is_empty()
            && !self.storage_prefix.bytes().all(|b| b.is_ascii_alphanumeric())
        {
            return Err(ConfigError::Validation(
                "storage prefix contains invalid characters, it may only contain digits and English alphabet letters".to_string(),
            ));
        }
        self.backend_cfg.validate()
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MemcachedClientConfig {
    pub addresses: String,
    pub addresses_lookup_period: GoDuration,
    pub addresses_lookup_pool_size: i64,
    pub timeout: GoDuration,
    pub connect_timeout: GoDuration,
    pub min_idle_connections_headroom_percentage: f64,
    pub max_idle_connections: i64,
    pub max_async_concurrency: i64,
    pub max_async_buffer_size: i64,
    pub max_get_multi_concurrency: i64,
    pub max_get_multi_batch_size: i64,
    pub max_item_size: i64,
    pub free_list_shards: i64,
    pub tls_enabled: bool,
    pub tls_cert_path: String,
    pub tls_key_path: String,
    pub tls_ca_path: String,
    pub tls_server_name: String,
    pub tls_insecure_skip_verify: bool,
    pub tls_cipher_suites: String,
    pub tls_min_version: String,
}

impl Default for MemcachedClientConfig {
    fn default() -> Self {
        Self {
            addresses: String::new(),
            addresses_lookup_period: GoDuration::from_secs(30),
            addresses_lookup_pool_size: 0,
            timeout: GoDuration::from_millis(200),
            connect_timeout: GoDuration::from_millis(200),
            min_idle_connections_headroom_percentage: -1.0,
            max_idle_connections: 100,
            max_async_concurrency: 50,
            max_async_buffer_size: 25000,
            max_get_multi_concurrency: 100,
            max_get_multi_batch_size: 100,
            max_item_size: 1024 * 1024,
            free_list_shards: 1,
            tls_enabled: false,
            tls_cert_path: String::new(),
            tls_key_path: String::new(),
            tls_ca_path: String::new(),
            tls_server_name: String::new(),
            tls_insecure_skip_verify: false,
            tls_cipher_suites: String::new(),
            tls_min_version: String::new(),
        }
    }
}

impl MemcachedClientConfig {
    pub fn register_flags_into<T: 'static>(
        &self,
        prefix: &str,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        f.string_var(&format!("{prefix}addresses"), &self.addresses, "Memcached addresses.", move |t| &get(t).addresses, move |t| &mut get_mut(t).addresses);
        f.go_duration_var(&format!("{prefix}addresses-lookup-period"), self.addresses_lookup_period, "How often to re-resolve memcached addresses.", move |t| &get(t).addresses_lookup_period, move |t| &mut get_mut(t).addresses_lookup_period);
        f.i64_var(&format!("{prefix}addresses-lookup-pool-size"), self.addresses_lookup_pool_size, "Size of DNS lookup pool.", move |t| &get(t).addresses_lookup_pool_size, move |t| &mut get_mut(t).addresses_lookup_pool_size);
        f.go_duration_var(&format!("{prefix}timeout"), self.timeout, "Socket read/write timeout.", move |t| &get(t).timeout, move |t| &mut get_mut(t).timeout);
        f.go_duration_var(&format!("{prefix}connect-timeout"), self.connect_timeout, "Socket connect timeout.", move |t| &get(t).connect_timeout, move |t| &mut get_mut(t).connect_timeout);
        f.f64_var(&format!("{prefix}min-idle-connections-headroom-percentage"), self.min_idle_connections_headroom_percentage, "Minimum idle connections headroom percentage.", move |t| &get(t).min_idle_connections_headroom_percentage, move |t| &mut get_mut(t).min_idle_connections_headroom_percentage);
        f.i64_var(&format!("{prefix}max-idle-connections"), self.max_idle_connections, "Maximum idle connections.", move |t| &get(t).max_idle_connections, move |t| &mut get_mut(t).max_idle_connections);
        f.i64_var(&format!("{prefix}max-async-concurrency"), self.max_async_concurrency, "Maximum async concurrency.", move |t| &get(t).max_async_concurrency, move |t| &mut get_mut(t).max_async_concurrency);
        f.i64_var(&format!("{prefix}max-async-buffer-size"), self.max_async_buffer_size, "Maximum async buffer size.", move |t| &get(t).max_async_buffer_size, move |t| &mut get_mut(t).max_async_buffer_size);
        f.i64_var(&format!("{prefix}max-get-multi-concurrency"), self.max_get_multi_concurrency, "Maximum GetMulti concurrency.", move |t| &get(t).max_get_multi_concurrency, move |t| &mut get_mut(t).max_get_multi_concurrency);
        f.i64_var(&format!("{prefix}max-get-multi-batch-size"), self.max_get_multi_batch_size, "Maximum GetMulti batch size.", move |t| &get(t).max_get_multi_batch_size, move |t| &mut get_mut(t).max_get_multi_batch_size);
        f.i64_var(&format!("{prefix}max-item-size"), self.max_item_size, "Maximum item size.", move |t| &get(t).max_item_size, move |t| &mut get_mut(t).max_item_size);
        f.i64_var(&format!("{prefix}free-list-shards"), self.free_list_shards, "Number of free list shards.", move |t| &get(t).free_list_shards, move |t| &mut get_mut(t).free_list_shards);
        f.bool_var(&format!("{prefix}tls-enabled"), self.tls_enabled, "Enable TLS.", move |t| &get(t).tls_enabled, move |t| &mut get_mut(t).tls_enabled);
        f.string_var(&format!("{prefix}tls-cert-path"), &self.tls_cert_path, "TLS client cert path.", move |t| &get(t).tls_cert_path, move |t| &mut get_mut(t).tls_cert_path);
        f.string_var(&format!("{prefix}tls-key-path"), &self.tls_key_path, "TLS client key path.", move |t| &get(t).tls_key_path, move |t| &mut get_mut(t).tls_key_path);
        f.string_var(&format!("{prefix}tls-ca-path"), &self.tls_ca_path, "TLS CA cert path.", move |t| &get(t).tls_ca_path, move |t| &mut get_mut(t).tls_ca_path);
        f.string_var(&format!("{prefix}tls-server-name"), &self.tls_server_name, "TLS server name.", move |t| &get(t).tls_server_name, move |t| &mut get_mut(t).tls_server_name);
        f.bool_var(&format!("{prefix}tls-insecure-skip-verify"), self.tls_insecure_skip_verify, "Skip TLS verification.", move |t| &get(t).tls_insecure_skip_verify, move |t| &mut get_mut(t).tls_insecure_skip_verify);
        f.string_var(&format!("{prefix}tls-cipher-suites"), &self.tls_cipher_suites, "TLS cipher suites.", move |t| &get(t).tls_cipher_suites, move |t| &mut get_mut(t).tls_cipher_suites);
        f.string_var(&format!("{prefix}tls-min-version"), &self.tls_min_version, "Minimum TLS version.", move |t| &get(t).tls_min_version, move |t| &mut get_mut(t).tls_min_version);
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct InMemoryIndexCacheConfig {
    pub max_size_bytes: u64,
}

impl Default for InMemoryIndexCacheConfig {
    fn default() -> Self {
        Self {
            max_size_bytes: 1024 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct IndexCacheConfig {
    pub backend: String,
    pub memcached: MemcachedClientConfig,
    pub inmemory: InMemoryIndexCacheConfig,
}

impl Default for IndexCacheConfig {
    fn default() -> Self {
        Self {
            backend: "inmemory".to_string(),
            memcached: MemcachedClientConfig::default(),
            inmemory: InMemoryIndexCacheConfig::default(),
        }
    }
}

impl IndexCacheConfig {
    pub fn register_flags_into<T: 'static>(
        &self,
        prefix: &str,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        f.string_var(&format!("{prefix}backend"), &self.backend, "The index cache backend type. Supported values: inmemory, memcached.", move |t| &get(t).backend, move |t| &mut get_mut(t).backend);
        f.u64_var(&format!("{prefix}inmemory.max-size-bytes"), self.inmemory.max_size_bytes, "Maximum size in bytes of in-memory index cache.", move |t| &get(t).inmemory.max_size_bytes, move |t| &mut get_mut(t).inmemory.max_size_bytes);
        self.memcached
            .register_flags_into(&format!("{prefix}memcached."), f, move |t| &get(t).memcached, move |t| &mut get_mut(t).memcached);
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.backend != "inmemory" && self.backend != "memcached" {
            return Err(ConfigError::Validation(
                "unsupported index cache backend".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct IndexHeaderCacheConfig {
    pub backend: String,
    pub memcached: MemcachedClientConfig,
    pub attributes_ttl: GoDuration,
    pub attributes_in_memory_max_items: i64,
    pub subrange_ttl: GoDuration,
    pub subrange_in_memory_max_items: i64,
    pub max_get_range_requests: i64,
}

impl Default for IndexHeaderCacheConfig {
    fn default() -> Self {
        Self {
            backend: String::new(),
            memcached: MemcachedClientConfig::default(),
            attributes_ttl: GoDuration::from_hours(168),
            attributes_in_memory_max_items: 10000,
            subrange_ttl: GoDuration::from_hours(24),
            subrange_in_memory_max_items: 100000,
            max_get_range_requests: 3,
        }
    }
}

impl IndexHeaderCacheConfig {
    pub fn register_flags_into<T: 'static>(
        &self,
        prefix: &str,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        f.string_var(&format!("{prefix}backend"), &self.backend, "Backend for index-header cache, if not empty.", move |t| &get(t).backend, move |t| &mut get_mut(t).backend);
        self.memcached
            .register_flags_into(&format!("{prefix}memcached."), f, move |t| &get(t).memcached, move |t| &mut get_mut(t).memcached);
        f.go_duration_var("blocks-storage.bucket-store.index-header-cache.attributes-ttl", self.attributes_ttl, "TTL for caching object attributes of the block index.", move |t| &get(t).attributes_ttl, move |t| &mut get_mut(t).attributes_ttl);
        f.i64_var(&format!("{prefix}attributes-in-memory-max-items"), self.attributes_in_memory_max_items, "Maximum number of individual attributes items to keep in in-memory LRU cache.", move |t| &get(t).attributes_in_memory_max_items, move |t| &mut get_mut(t).attributes_in_memory_max_items);
        f.go_duration_var(&format!("{prefix}subrange-ttl"), self.subrange_ttl, "TTL for caching individual index-header subranges.", move |t| &get(t).subrange_ttl, move |t| &mut get_mut(t).subrange_ttl);
        f.i64_var(&format!("{prefix}subrange-in-memory-max-items"), self.subrange_in_memory_max_items, "Maximum number of individual subrange items to keep in in-memory LRU cache.", move |t| &get(t).subrange_in_memory_max_items, move |t| &mut get_mut(t).subrange_in_memory_max_items);
        f.i64_var(&format!("{prefix}max-get-range-requests"), self.max_get_range_requests, "Maximum number of sub-GetRange requests.", move |t| &get(t).max_get_range_requests, move |t| &mut get_mut(t).max_get_range_requests);
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if !self.backend.is_empty() && self.backend != "memcached" {
            return Err(ConfigError::Validation(format!(
                "unsupported cache backend: {}",
                self.backend
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ChunksCacheConfig {
    pub backend: String,
    pub memcached: MemcachedClientConfig,
    pub max_get_range_requests: i64,
    pub attributes_ttl: GoDuration,
    pub attributes_in_memory_max_items: i64,
    pub subrange_ttl: GoDuration,
}

impl Default for ChunksCacheConfig {
    fn default() -> Self {
        Self {
            backend: String::new(),
            memcached: MemcachedClientConfig::default(),
            max_get_range_requests: 3,
            attributes_ttl: GoDuration::from_hours(168),
            attributes_in_memory_max_items: 50000,
            subrange_ttl: GoDuration::from_hours(24),
        }
    }
}

impl ChunksCacheConfig {
    pub fn register_flags_into<T: 'static>(
        &self,
        prefix: &str,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        f.string_var(&format!("{prefix}backend"), &self.backend, "Backend for chunks cache, if not empty.", move |t| &get(t).backend, move |t| &mut get_mut(t).backend);
        self.memcached
            .register_flags_into(&format!("{prefix}memcached."), f, move |t| &get(t).memcached, move |t| &mut get_mut(t).memcached);
        f.i64_var(&format!("{prefix}max-get-range-requests"), self.max_get_range_requests, "Maximum number of sub-GetRange requests.", move |t| &get(t).max_get_range_requests, move |t| &mut get_mut(t).max_get_range_requests);
        f.go_duration_var(&format!("{prefix}attributes-ttl"), self.attributes_ttl, "TTL for caching object attributes for chunks.", move |t| &get(t).attributes_ttl, move |t| &mut get_mut(t).attributes_ttl);
        f.i64_var(&format!("{prefix}attributes-in-memory-max-items"), self.attributes_in_memory_max_items, "Maximum number of object attribute items to keep in in-memory LRU cache.", move |t| &get(t).attributes_in_memory_max_items, move |t| &mut get_mut(t).attributes_in_memory_max_items);
        f.go_duration_var(&format!("{prefix}subrange-ttl"), self.subrange_ttl, "TTL for caching individual chunks subranges.", move |t| &get(t).subrange_ttl, move |t| &mut get_mut(t).subrange_ttl);
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if !self.backend.is_empty() && self.backend != "memcached" {
            return Err(ConfigError::Validation(format!(
                "unsupported cache backend: {}",
                self.backend
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MetadataCacheConfig {
    pub backend: String,
    pub memcached: MemcachedClientConfig,
    pub tenants_list_ttl: GoDuration,
    pub tenant_blocks_list_ttl: GoDuration,
    pub chunks_list_ttl: GoDuration,
    pub metafile_exists_ttl: GoDuration,
    pub metafile_doesnt_exist_ttl: GoDuration,
    pub metafile_content_ttl: GoDuration,
    pub metafile_max_size_bytes: i64,
    pub metafile_attributes_ttl: GoDuration,
    pub block_index_attributes_ttl: GoDuration,
    pub bucket_index_content_ttl: GoDuration,
    pub bucket_index_max_size_bytes: i64,
}

impl Default for MetadataCacheConfig {
    fn default() -> Self {
        Self {
            backend: String::new(),
            memcached: MemcachedClientConfig::default(),
            tenants_list_ttl: GoDuration::from_mins(15),
            tenant_blocks_list_ttl: GoDuration::from_mins(5),
            chunks_list_ttl: GoDuration::from_hours(24),
            metafile_exists_ttl: GoDuration::from_hours(2),
            metafile_doesnt_exist_ttl: GoDuration::from_mins(5),
            metafile_content_ttl: GoDuration::from_hours(24),
            metafile_max_size_bytes: 1024 * 1024,
            metafile_attributes_ttl: GoDuration::from_hours(168),
            block_index_attributes_ttl: GoDuration::from_hours(168),
            bucket_index_content_ttl: GoDuration::from_mins(5),
            bucket_index_max_size_bytes: 1024 * 1024,
        }
    }
}

impl MetadataCacheConfig {
    pub fn register_flags_into<T: 'static>(
        &self,
        prefix: &str,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        f.string_var(&format!("{prefix}backend"), &self.backend, "Backend for metadata cache, if not empty.", move |t| &get(t).backend, move |t| &mut get_mut(t).backend);
        self.memcached
            .register_flags_into(&format!("{prefix}memcached."), f, move |t| &get(t).memcached, move |t| &mut get_mut(t).memcached);
        f.go_duration_var(&format!("{prefix}tenants-list-ttl"), self.tenants_list_ttl, "How long to cache list of tenants in the bucket.", move |t| &get(t).tenants_list_ttl, move |t| &mut get_mut(t).tenants_list_ttl);
        f.go_duration_var(&format!("{prefix}tenant-blocks-list-ttl"), self.tenant_blocks_list_ttl, "How long to cache list of blocks for each tenant.", move |t| &get(t).tenant_blocks_list_ttl, move |t| &mut get_mut(t).tenant_blocks_list_ttl);
        f.go_duration_var(&format!("{prefix}chunks-list-ttl"), self.chunks_list_ttl, "How long to cache list of chunks for a block.", move |t| &get(t).chunks_list_ttl, move |t| &mut get_mut(t).chunks_list_ttl);
        f.go_duration_var(&format!("{prefix}metafile-exists-ttl"), self.metafile_exists_ttl, "How long to cache information that block metafile exists.", move |t| &get(t).metafile_exists_ttl, move |t| &mut get_mut(t).metafile_exists_ttl);
        f.go_duration_var(&format!("{prefix}metafile-doesnt-exist-ttl"), self.metafile_doesnt_exist_ttl, "How long to cache information that block metafile doesn't exist.", move |t| &get(t).metafile_doesnt_exist_ttl, move |t| &mut get_mut(t).metafile_doesnt_exist_ttl);
        f.go_duration_var(&format!("{prefix}metafile-content-ttl"), self.metafile_content_ttl, "How long to cache content of the metafile.", move |t| &get(t).metafile_content_ttl, move |t| &mut get_mut(t).metafile_content_ttl);
        f.i64_var(&format!("{prefix}metafile-max-size-bytes"), self.metafile_max_size_bytes, "Maximum size of metafile content to cache in bytes.", move |t| &get(t).metafile_max_size_bytes, move |t| &mut get_mut(t).metafile_max_size_bytes);
        f.go_duration_var(&format!("{prefix}metafile-attributes-ttl"), self.metafile_attributes_ttl, "How long to cache attributes of the block metafile.", move |t| &get(t).metafile_attributes_ttl, move |t| &mut get_mut(t).metafile_attributes_ttl);
        f.go_duration_var(&format!("{prefix}block-index-attributes-ttl"), self.block_index_attributes_ttl, "How long to cache attributes of the block index.", move |t| &get(t).block_index_attributes_ttl, move |t| &mut get_mut(t).block_index_attributes_ttl);
        f.go_duration_var(&format!("{prefix}bucket-index-content-ttl"), self.bucket_index_content_ttl, "How long to cache content of the bucket index.", move |t| &get(t).bucket_index_content_ttl, move |t| &mut get_mut(t).bucket_index_content_ttl);
        f.i64_var(&format!("{prefix}bucket-index-max-size-bytes"), self.bucket_index_max_size_bytes, "Maximum size of bucket index content to cache in bytes.", move |t| &get(t).bucket_index_max_size_bytes, move |t| &mut get_mut(t).bucket_index_max_size_bytes);
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if !self.backend.is_empty() && self.backend != "memcached" {
            return Err(ConfigError::Validation(format!(
                "unsupported cache backend: {}",
                self.backend
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BucketIndexConfig {
    pub update_on_error_interval: GoDuration,
    pub idle_timeout: GoDuration,
    pub max_stale_period: GoDuration,
}

impl Default for BucketIndexConfig {
    fn default() -> Self {
        Self {
            update_on_error_interval: GoDuration::from_mins(1),
            idle_timeout: GoDuration::from_hours(1),
            max_stale_period: GoDuration::from_hours(1),
        }
    }
}

impl BucketIndexConfig {
    pub fn register_flags_into<T: 'static>(
        &self,
        prefix: &str,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        f.go_duration_var(&format!("{prefix}update-on-error-interval"), self.update_on_error_interval, "How frequently a bucket index should be tried to load again.", move |t| &get(t).update_on_error_interval, move |t| &mut get_mut(t).update_on_error_interval);
        f.go_duration_var(&format!("{prefix}idle-timeout"), self.idle_timeout, "How long an unused bucket index should be cached.", move |t| &get(t).idle_timeout, move |t| &mut get_mut(t).idle_timeout);
        f.go_duration_var(&format!("{prefix}max-stale-period"), self.max_stale_period, "The maximum allowed age of a bucket index.", move |t| &get(t).max_stale_period, move |t| &mut get_mut(t).max_stale_period);
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct IndexHeaderConfig {
    pub max_idle_file_handles: u64,
    pub lazy_loading_enabled: bool,
    pub lazy_loading_idle_timeout: GoDuration,
    pub lazy_loading_concurrency: i64,
    pub lazy_loading_concurrency_queue_timeout: GoDuration,
    pub verify_on_load: bool,
    pub version: i64,
}

impl Default for IndexHeaderConfig {
    fn default() -> Self {
        Self {
            max_idle_file_handles: 1,
            lazy_loading_enabled: true,
            lazy_loading_idle_timeout: GoDuration::from_mins(60),
            lazy_loading_concurrency: 4,
            lazy_loading_concurrency_queue_timeout: GoDuration::from_secs(5),
            verify_on_load: false,
            version: 1,
        }
    }
}

impl IndexHeaderConfig {
    pub fn register_flags_into<T: 'static>(
        &self,
        prefix: &str,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        f.u64_var(&format!("{prefix}max-idle-file-handles"), self.max_idle_file_handles, "Maximum number of idle file handles.", move |t| &get(t).max_idle_file_handles, move |t| &mut get_mut(t).max_idle_file_handles);
        f.bool_var(&format!("{prefix}lazy-loading-enabled"), self.lazy_loading_enabled, "If enabled, store-gateway will lazy load an index-header.", move |t| &get(t).lazy_loading_enabled, move |t| &mut get_mut(t).lazy_loading_enabled);
        f.go_duration_var(&format!("{prefix}lazy-loading-idle-timeout"), self.lazy_loading_idle_timeout, "Idle timeout for index-headers.", move |t| &get(t).lazy_loading_idle_timeout, move |t| &mut get_mut(t).lazy_loading_idle_timeout);
        f.i64_var(&format!("{prefix}lazy-loading-concurrency"), self.lazy_loading_concurrency, "Maximum number of concurrent index header loads across all tenants.", move |t| &get(t).lazy_loading_concurrency, move |t| &mut get_mut(t).lazy_loading_concurrency);
        f.go_duration_var(&format!("{prefix}lazy-loading-concurrency-queue-timeout"), self.lazy_loading_concurrency_queue_timeout, "Timeout for the queue of index header loads.", move |t| &get(t).lazy_loading_concurrency_queue_timeout, move |t| &mut get_mut(t).lazy_loading_concurrency_queue_timeout);
        f.bool_var(&format!("{prefix}verify-on-load"), self.verify_on_load, "If true, verify the checksum of index headers upon loading them.", move |t| &get(t).verify_on_load, move |t| &mut get_mut(t).verify_on_load);
        f.i64_var(&format!("{prefix}version"), self.version, "Index-header format version.", move |t| &get(t).version, move |t| &mut get_mut(t).version);
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.lazy_loading_concurrency < 0 {
            return Err(ConfigError::Validation(
                "invalid index-header lazy loading max concurrency; must be non-negative"
                    .to_string(),
            ));
        }
        if self.version != 1 && self.version != 2 {
            return Err(ConfigError::Validation(format!(
                "invalid index-header version; must be 1 or 2: {}",
                self.version
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BucketStoreConfig {
    pub sync_dir: String,
    pub sync_interval: GoDuration,
    pub max_concurrent: i64,
    pub max_concurrent_queue_timeout: GoDuration,
    pub tenant_sync_concurrency: i64,
    pub block_sync_concurrency: i64,
    pub meta_sync_concurrency: i64,
    pub index_cache: IndexCacheConfig,
    pub index_header_cache: IndexHeaderCacheConfig,
    pub chunks_cache: ChunksCacheConfig,
    pub metadata_cache: MetadataCacheConfig,
    pub ignore_deletion_mark_delay: GoDuration,
    pub ignore_deletion_mark_while_querying_delay: GoDuration,
    pub bucket_index: BucketIndexConfig,
    pub ignore_blocks_within: GoDuration,
    pub series_hash_cache_max_size_bytes: u64,
    pub partitioner_max_gap_bytes: u64,
    pub partitioner_max_gap_bytes_chunks: u64,
    pub postings_offsets_in_mem_sampling: i64,
    pub index_header: IndexHeaderConfig,
    pub streaming_series_batch_size: i64,
    pub series_fetch_preference: f64,
}

impl Default for BucketStoreConfig {
    fn default() -> Self {
        Self {
            sync_dir: "./tsdb-sync/".to_string(),
            sync_interval: GoDuration::from_mins(15),
            max_concurrent: 200,
            max_concurrent_queue_timeout: GoDuration::from_secs(5),
            tenant_sync_concurrency: 1,
            block_sync_concurrency: 4,
            meta_sync_concurrency: 20,
            index_cache: IndexCacheConfig::default(),
            index_header_cache: IndexHeaderCacheConfig::default(),
            chunks_cache: ChunksCacheConfig::default(),
            metadata_cache: MetadataCacheConfig::default(),
            ignore_deletion_mark_delay: GoDuration::from_hours(1),
            ignore_deletion_mark_while_querying_delay: GoDuration::from_mins(50),
            bucket_index: BucketIndexConfig::default(),
            ignore_blocks_within: GoDuration::from_hours(10),
            series_hash_cache_max_size_bytes: 1024 * 1024 * 1024,
            partitioner_max_gap_bytes: 512 * 1024,
            partitioner_max_gap_bytes_chunks: 0,
            postings_offsets_in_mem_sampling: 32,
            index_header: IndexHeaderConfig::default(),
            streaming_series_batch_size: 5000,
            series_fetch_preference: 0.75,
        }
    }
}

impl BucketStoreConfig {
    pub fn register_flags_into<T: 'static>(
        &self,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        self.index_cache
            .register_flags_into("blocks-storage.bucket-store.index-cache.", f, move |t| &get(t).index_cache, move |t| &mut get_mut(t).index_cache);
        self.index_header_cache
            .register_flags_into("blocks-storage.bucket-store.index-header-cache.", f, move |t| &get(t).index_header_cache, move |t| &mut get_mut(t).index_header_cache);
        self.chunks_cache
            .register_flags_into("blocks-storage.bucket-store.chunks-cache.", f, move |t| &get(t).chunks_cache, move |t| &mut get_mut(t).chunks_cache);
        self.metadata_cache
            .register_flags_into("blocks-storage.bucket-store.metadata-cache.", f, move |t| &get(t).metadata_cache, move |t| &mut get_mut(t).metadata_cache);
        self.bucket_index
            .register_flags_into("blocks-storage.bucket-store.bucket-index.", f, move |t| &get(t).bucket_index, move |t| &mut get_mut(t).bucket_index);
        self.index_header
            .register_flags_into("blocks-storage.bucket-store.index-header.", f, move |t| &get(t).index_header, move |t| &mut get_mut(t).index_header);

        f.string_var("blocks-storage.bucket-store.sync-dir", &self.sync_dir, "Directory to store synchronized TSDB index headers.", move |t| &get(t).sync_dir, move |t| &mut get_mut(t).sync_dir);
        f.go_duration_var("blocks-storage.bucket-store.sync-interval", self.sync_interval, "How frequently to scan the bucket.", move |t| &get(t).sync_interval, move |t| &mut get_mut(t).sync_interval);
        f.u64_var("blocks-storage.bucket-store.series-hash-cache-max-size-bytes", self.series_hash_cache_max_size_bytes, "Max size - in bytes - of the in-memory series hash cache.", move |t| &get(t).series_hash_cache_max_size_bytes, move |t| &mut get_mut(t).series_hash_cache_max_size_bytes);
        f.i64_var("blocks-storage.bucket-store.max-concurrent", self.max_concurrent, "Max number of concurrent queries to execute against the long-term storage.", move |t| &get(t).max_concurrent, move |t| &mut get_mut(t).max_concurrent);
        f.go_duration_var("blocks-storage.bucket-store.max-concurrent-queue-timeout", self.max_concurrent_queue_timeout, "Timeout for the queue of queries waiting for execution.", move |t| &get(t).max_concurrent_queue_timeout, move |t| &mut get_mut(t).max_concurrent_queue_timeout);
        f.i64_var("blocks-storage.bucket-store.tenant-sync-concurrency", self.tenant_sync_concurrency, "Maximum number of concurrent tenants synching blocks.", move |t| &get(t).tenant_sync_concurrency, move |t| &mut get_mut(t).tenant_sync_concurrency);
        f.i64_var("blocks-storage.bucket-store.block-sync-concurrency", self.block_sync_concurrency, "Maximum number of concurrent blocks synching per tenant.", move |t| &get(t).block_sync_concurrency, move |t| &mut get_mut(t).block_sync_concurrency);
        f.i64_var("blocks-storage.bucket-store.meta-sync-concurrency", self.meta_sync_concurrency, "Number of goroutines to use when syncing block meta files.", move |t| &get(t).meta_sync_concurrency, move |t| &mut get_mut(t).meta_sync_concurrency);
        f.go_duration_var("blocks-storage.bucket-store.ignore-deletion-marks-delay", self.ignore_deletion_mark_delay, "Duration after which the blocks marked for deletion will be filtered out.", move |t| &get(t).ignore_deletion_mark_delay, move |t| &mut get_mut(t).ignore_deletion_mark_delay);
        f.go_duration_var("blocks-storage.bucket-store.ignore-deletion-marks-while-querying-delay", self.ignore_deletion_mark_while_querying_delay, "Duration after which blocks marked for deletion will still be queried.", move |t| &get(t).ignore_deletion_mark_while_querying_delay, move |t| &mut get_mut(t).ignore_deletion_mark_while_querying_delay);
        f.go_duration_var("blocks-storage.bucket-store.ignore-blocks-within", self.ignore_blocks_within, "Blocks with minimum time within this duration are ignored.", move |t| &get(t).ignore_blocks_within, move |t| &mut get_mut(t).ignore_blocks_within);
        f.i64_var("blocks-storage.bucket-store.posting-offsets-in-mem-sampling", self.postings_offsets_in_mem_sampling, "Controls what is the ratio of postings offsets that the store will hold in memory.", move |t| &get(t).postings_offsets_in_mem_sampling, move |t| &mut get_mut(t).postings_offsets_in_mem_sampling);
        f.u64_var("blocks-storage.bucket-store.partitioner-max-gap-bytes", self.partitioner_max_gap_bytes, "Max size - in bytes - of a gap for which the partitioner aggregates two GET requests.", move |t| &get(t).partitioner_max_gap_bytes, move |t| &mut get_mut(t).partitioner_max_gap_bytes);
        f.u64_var("blocks-storage.bucket-store.partitioner-max-gap-bytes-chunks", self.partitioner_max_gap_bytes_chunks, "Max size - in bytes - of a gap when requesting chunks.", move |t| &get(t).partitioner_max_gap_bytes_chunks, move |t| &mut get_mut(t).partitioner_max_gap_bytes_chunks);
        f.i64_var("blocks-storage.bucket-store.batch-series-size", self.streaming_series_batch_size, "This option controls how many series to fetch per batch.", move |t| &get(t).streaming_series_batch_size, move |t| &mut get_mut(t).streaming_series_batch_size);
        f.f64_var("blocks-storage.bucket-store.series-fetch-preference", self.series_fetch_preference, "Trade-off in fetching series versus fetching postings.", move |t| &get(t).series_fetch_preference, move |t| &mut get_mut(t).series_fetch_preference);
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.streaming_series_batch_size <= 0 {
            return Err(ConfigError::Validation(
                "invalid store-gateway streaming batch size".to_string(),
            ));
        }
        if self.ignore_deletion_mark_while_querying_delay >= self.ignore_deletion_mark_delay {
            return Err(ConfigError::Validation(
                "value for -blocks-storage.bucket-store.ignore-deletion-marks-while-querying-delay must be less than -blocks-storage.bucket-store.ignore-deletion-marks-delay".to_string(),
            ));
        }
        if self.ignore_deletion_mark_while_querying_delay.0 <= 3 * self.sync_interval.0 {
            return Err(ConfigError::Validation(
                "value for -blocks-storage.bucket-store.ignore-deletion-marks-while-querying-delay must be greater than 3× -blocks-storage.bucket-store.sync-interval".to_string(),
            ));
        }
        self.index_cache.validate()?;
        self.chunks_cache.validate()?;
        self.index_header_cache.validate()?;
        self.metadata_cache.validate()?;
        if self.series_fetch_preference <= 0.0 {
            return Err(ConfigError::Validation(
                "invalid worst-case series preference; must be positive".to_string(),
            ));
        }
        self.index_header.validate()
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct IndexLookupPlanningConfig {
    pub retrieved_posting_cost: f64,
    pub retrieved_series_cost: f64,
    pub retrieved_posting_list_cost: f64,
    pub min_series_per_block_for_query_planning: u64,
    pub label_cardinality_for_larger_sketch: u64,
    pub label_cardinality_for_smaller_sketch: u64,
    pub index_lookup_planning_enabled: bool,
    pub index_lookup_planning_comparison_portion: f64,
    pub statistics_collection_frequency: GoDuration,
}

impl Default for IndexLookupPlanningConfig {
    fn default() -> Self {
        Self {
            retrieved_posting_cost: 0.01,
            retrieved_series_cost: 15.0,
            retrieved_posting_list_cost: 10.0,
            min_series_per_block_for_query_planning: 10_000,
            label_cardinality_for_larger_sketch: 1_000_000,
            label_cardinality_for_smaller_sketch: 1_000,
            index_lookup_planning_enabled: false,
            index_lookup_planning_comparison_portion: 0.0,
            statistics_collection_frequency: GoDuration::from_hours(1),
        }
    }
}

impl IndexLookupPlanningConfig {
    pub fn register_flags_into<T: 'static>(
        &self,
        prefix: &str,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        f.bool_var(&format!("{prefix}enabled"), self.index_lookup_planning_enabled, "Controls the collection of statistics and whether to defer some vector selector matchers to sequential scans.", move |t| &get(t).index_lookup_planning_enabled, move |t| &mut get_mut(t).index_lookup_planning_enabled);
        f.go_duration_var(&format!("{prefix}statistics-collection-frequency"), self.statistics_collection_frequency, "How frequently to collect block statistics.", move |t| &get(t).statistics_collection_frequency, move |t| &mut get_mut(t).statistics_collection_frequency);
        f.f64_var(&format!("{prefix}comparison-portion"), self.index_lookup_planning_comparison_portion, "Portion of queries where a mirrored chunk querier compares results.", move |t| &get(t).index_lookup_planning_comparison_portion, move |t| &mut get_mut(t).index_lookup_planning_comparison_portion);
        f.f64_var(&format!("{prefix}retrieved-posting-cost"), self.retrieved_posting_cost, "Cost for iterating postings.", move |t| &get(t).retrieved_posting_cost, move |t| &mut get_mut(t).retrieved_posting_cost);
        f.f64_var(&format!("{prefix}retrieved-series-cost"), self.retrieved_series_cost, "Cost for retrieving series.", move |t| &get(t).retrieved_series_cost, move |t| &mut get_mut(t).retrieved_series_cost);
        f.f64_var(&format!("{prefix}retrieved-posting-list-cost"), self.retrieved_posting_list_cost, "Cost for retrieving the posting list.", move |t| &get(t).retrieved_posting_list_cost, move |t| &mut get_mut(t).retrieved_posting_list_cost);
        f.u64_var(&format!("{prefix}min-series-per-block-for-query-planning"), self.min_series_per_block_for_query_planning, "Minimum number of series a block must have for query planning.", move |t| &get(t).min_series_per_block_for_query_planning, move |t| &mut get_mut(t).min_series_per_block_for_query_planning);
        f.u64_var(&format!("{prefix}label-cardinality-for-larger-sketch"), self.label_cardinality_for_larger_sketch, "Number of series for a label name above which larger count-min sketches are used.", move |t| &get(t).label_cardinality_for_larger_sketch, move |t| &mut get_mut(t).label_cardinality_for_larger_sketch);
        f.u64_var(&format!("{prefix}label-cardinality-for-smaller-sketch"), self.label_cardinality_for_smaller_sketch, "Number of series for a label name below which smaller count-min sketches are used.", move |t| &get(t).label_cardinality_for_smaller_sketch, move |t| &mut get_mut(t).label_cardinality_for_smaller_sketch);
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if !self.index_lookup_planning_enabled {
            return Ok(());
        }
        if self.label_cardinality_for_smaller_sketch > self.label_cardinality_for_larger_sketch {
            return Err(ConfigError::Validation(
                "cardinality limit for smaller sketches cannot be larger than cardinality limit for larger sketches".to_string(),
            ));
        }
        if self.statistics_collection_frequency.0 <= 0 {
            return Err(ConfigError::Validation(
                "head statistics collection frequency must be a non-negative duration. 0 to disable".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct OffsetCatalogueConfig {
    pub enabled: bool,
    pub sync_concurrency: i64,
}

impl Default for OffsetCatalogueConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            sync_concurrency: 10,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TSDBConfig {
    pub dir: String,
    pub block_ranges_period: Vec<GoDuration>,
    pub retention_period: GoDuration,
    pub ship_interval: GoDuration,
    pub ship_concurrency: i64,
    pub head_compaction_interval: GoDuration,
    pub head_compaction_concurrency: i64,
    pub head_compaction_idle_timeout: GoDuration,
    pub head_chunks_write_buffer_size_bytes: i64,
    pub head_chunks_end_time_variance: f64,
    pub stripe_size: i64,
    pub wal_compression_enabled: bool,
    pub wal_segment_size_bytes: i64,
    pub wal_replay_concurrency: i64,
    pub flush_blocks_on_shutdown: bool,
    pub close_idle_tsdb_timeout: GoDuration,
    pub close_idle_tsdb_when_shipping_disabled: bool,
    pub memory_snapshot_on_shutdown: bool,
    pub head_chunks_write_queue_size: i64,
    pub bigger_out_of_order_blocks_for_old_samples: bool,
    pub series_hash_cache_max_size_bytes: u64,
    pub out_of_order_capacity_max: i64,
    pub shared_postings_for_matchers_cache: bool,
    pub head_postings_for_matchers_cache_invalidation: bool,
    pub head_postings_for_matchers_cache_versions: i64,
    pub head_postings_for_matchers_cache_ttl: GoDuration,
    pub head_postings_for_matchers_cache_max_bytes: i64,
    pub head_postings_for_matchers_cache_force: bool,
    pub block_postings_for_matchers_cache_ttl: GoDuration,
    pub block_postings_for_matchers_cache_max_bytes: i64,
    pub block_postings_for_matchers_cache_force: bool,
    pub early_head_compaction_min_in_memory_series: i64,
    pub early_head_compaction_min_estimated_series_reduction_percentage: i64,
    pub timely_head_compaction_enabled: bool,
    pub index_lookup_planning: IndexLookupPlanningConfig,
    pub offset_catalogue: OffsetCatalogueConfig,
    #[serde(skip)]
    pub keep_user_tsdb_open_on_shutdown: bool,
    #[serde(skip)]
    pub close_idle_tsdb_interval: GoDuration,
    #[serde(skip)]
    pub head_compaction_interval_jitter_enabled: bool,
}

impl Default for TSDBConfig {
    fn default() -> Self {
        Self {
            dir: "./tsdb/".to_string(),
            block_ranges_period: vec![GoDuration::from_hours(2)],
            retention_period: GoDuration::from_hours(13),
            ship_interval: GoDuration::from_mins(1),
            ship_concurrency: 10,
            head_compaction_interval: GoDuration::from_mins(1),
            head_compaction_concurrency: 1,
            head_compaction_idle_timeout: GoDuration::from_hours(1),
            head_chunks_write_buffer_size_bytes: TSDB_DEFAULT_WRITE_BUFFER_SIZE,
            head_chunks_end_time_variance: 0.0,
            stripe_size: 16384,
            wal_compression_enabled: false,
            wal_segment_size_bytes: TSDB_DEFAULT_WAL_SEGMENT_SIZE,
            wal_replay_concurrency: 0,
            flush_blocks_on_shutdown: false,
            close_idle_tsdb_timeout: GoDuration::from_hours(13),
            close_idle_tsdb_when_shipping_disabled: false,
            memory_snapshot_on_shutdown: false,
            head_chunks_write_queue_size: 1_000_000,
            bigger_out_of_order_blocks_for_old_samples: false,
            series_hash_cache_max_size_bytes: 350 * 1024 * 1024,
            out_of_order_capacity_max: 32,
            shared_postings_for_matchers_cache: false,
            head_postings_for_matchers_cache_invalidation: false,
            head_postings_for_matchers_cache_versions: 2_097_152,
            head_postings_for_matchers_cache_ttl: GoDuration::from_secs(10),
            head_postings_for_matchers_cache_max_bytes: 100 * 1024 * 1024,
            head_postings_for_matchers_cache_force: false,
            block_postings_for_matchers_cache_ttl: GoDuration::from_secs(10),
            block_postings_for_matchers_cache_max_bytes: 100 * 1024 * 1024,
            block_postings_for_matchers_cache_force: false,
            early_head_compaction_min_in_memory_series: 0,
            early_head_compaction_min_estimated_series_reduction_percentage: 15,
            timely_head_compaction_enabled: false,
            index_lookup_planning: IndexLookupPlanningConfig::default(),
            offset_catalogue: OffsetCatalogueConfig::default(),
            keep_user_tsdb_open_on_shutdown: false,
            close_idle_tsdb_interval: GoDuration::from_secs(5 * 60),
            head_compaction_interval_jitter_enabled: true,
        }
    }
}

impl TSDBConfig {
    pub fn register_flags_into<T: 'static>(
        &self,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        f.string_var("blocks-storage.tsdb.dir", &self.dir, "Directory to store TSDBs (including WAL) in the ingesters.", move |t| &get(t).dir, move |t| &mut get_mut(t).dir);
        f.register_raw(
            "blocks-storage.tsdb.block-ranges-period",
            false,
            "value",
            "2h0m0s",
            "TSDB blocks range period.",
            move |t, s| {
                let mut list = Vec::new();
                for part in s.split(',') {
                    list.push(part.parse::<GoDuration>()?);
                }
                get_mut(t).block_ranges_period = list;
                Ok(())
            },
            move |t| {
                get(t)
                    .block_ranges_period
                    .iter()
                    .map(|d| d.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            },
        );
        f.go_duration_var("blocks-storage.tsdb.retention-period", self.retention_period, "TSDB blocks retention before a block is removed.", move |t| &get(t).retention_period, move |t| &mut get_mut(t).retention_period);
        f.go_duration_var("blocks-storage.tsdb.ship-interval", self.ship_interval, "How frequently the TSDB blocks are scanned and new ones are shipped to the storage.", move |t| &get(t).ship_interval, move |t| &mut get_mut(t).ship_interval);
        f.i64_var("blocks-storage.tsdb.ship-concurrency", self.ship_concurrency, "Maximum number of tenants concurrently shipping blocks to the storage.", move |t| &get(t).ship_concurrency, move |t| &mut get_mut(t).ship_concurrency);
        f.u64_var("blocks-storage.tsdb.series-hash-cache-max-size-bytes", self.series_hash_cache_max_size_bytes, "Max size - in bytes - of the in-memory series hash cache.", move |t| &get(t).series_hash_cache_max_size_bytes, move |t| &mut get_mut(t).series_hash_cache_max_size_bytes);
        f.go_duration_var("blocks-storage.tsdb.head-compaction-interval", self.head_compaction_interval, "How frequently the ingester checks whether the TSDB head should be compacted.", move |t| &get(t).head_compaction_interval, move |t| &mut get_mut(t).head_compaction_interval);
        f.i64_var("blocks-storage.tsdb.head-compaction-concurrency", self.head_compaction_concurrency, "Maximum number of tenants concurrently compacting TSDB head into a new block", move |t| &get(t).head_compaction_concurrency, move |t| &mut get_mut(t).head_compaction_concurrency);
        f.go_duration_var("blocks-storage.tsdb.head-compaction-idle-timeout", self.head_compaction_idle_timeout, "If TSDB head is idle for this duration, it is compacted.", move |t| &get(t).head_compaction_idle_timeout, move |t| &mut get_mut(t).head_compaction_idle_timeout);
        f.i64_var("blocks-storage.tsdb.head-chunks-write-buffer-size-bytes", self.head_chunks_write_buffer_size_bytes, "The write buffer size used by the head chunks mapper.", move |t| &get(t).head_chunks_write_buffer_size_bytes, move |t| &mut get_mut(t).head_chunks_write_buffer_size_bytes);
        f.f64_var("blocks-storage.tsdb.head-chunks-end-time-variance", self.head_chunks_end_time_variance, "How much variance (as percentage between 0 and 1) should be applied to the chunk end time.", move |t| &get(t).head_chunks_end_time_variance, move |t| &mut get_mut(t).head_chunks_end_time_variance);
        f.i64_var("blocks-storage.tsdb.stripe-size", self.stripe_size, "The number of shards of series to use in TSDB (must be a power of 2).", move |t| &get(t).stripe_size, move |t| &mut get_mut(t).stripe_size);
        f.bool_var("blocks-storage.tsdb.wal-compression-enabled", self.wal_compression_enabled, "True to enable TSDB WAL compression.", move |t| &get(t).wal_compression_enabled, move |t| &mut get_mut(t).wal_compression_enabled);
        f.i64_var("blocks-storage.tsdb.wal-segment-size-bytes", self.wal_segment_size_bytes, "TSDB WAL segments files max size (bytes).", move |t| &get(t).wal_segment_size_bytes, move |t| &mut get_mut(t).wal_segment_size_bytes);
        f.i64_var("blocks-storage.tsdb.wal-replay-concurrency", self.wal_replay_concurrency, "Maximum number of CPUs that can simultaneously process WAL replay.", move |t| &get(t).wal_replay_concurrency, move |t| &mut get_mut(t).wal_replay_concurrency);
        f.bool_var("blocks-storage.tsdb.flush-blocks-on-shutdown", self.flush_blocks_on_shutdown, "True to flush blocks to storage on shutdown.", move |t| &get(t).flush_blocks_on_shutdown, move |t| &mut get_mut(t).flush_blocks_on_shutdown);
        f.go_duration_var("blocks-storage.tsdb.close-idle-tsdb-timeout", self.close_idle_tsdb_timeout, "If TSDB has not received any data for this duration, close and delete from local disk.", move |t| &get(t).close_idle_tsdb_timeout, move |t| &mut get_mut(t).close_idle_tsdb_timeout);
        f.bool_var("blocks-storage.tsdb.close-idle-tsdb-when-shipping-disabled", self.close_idle_tsdb_when_shipping_disabled, "True to allow closing of idle TSDBs even when block shipping is disabled.", move |t| &get(t).close_idle_tsdb_when_shipping_disabled, move |t| &mut get_mut(t).close_idle_tsdb_when_shipping_disabled);
        f.bool_var("blocks-storage.tsdb.memory-snapshot-on-shutdown", self.memory_snapshot_on_shutdown, "True to enable snapshotting of in-memory TSDB data on disk when shutting down.", move |t| &get(t).memory_snapshot_on_shutdown, move |t| &mut get_mut(t).memory_snapshot_on_shutdown);
        f.i64_var("blocks-storage.tsdb.head-chunks-write-queue-size", self.head_chunks_write_queue_size, "The size of the write queue used by the head chunks mapper.", move |t| &get(t).head_chunks_write_queue_size, move |t| &mut get_mut(t).head_chunks_write_queue_size);
        f.i64_var("blocks-storage.tsdb.out-of-order-capacity-max", self.out_of_order_capacity_max, "Maximum capacity for out of order chunks.", move |t| &get(t).out_of_order_capacity_max, move |t| &mut get_mut(t).out_of_order_capacity_max);
        f.bool_var("blocks-storage.tsdb.shared-postings-for-matchers-cache", self.shared_postings_for_matchers_cache, "Whether postings for matchers cache should be shared across blocks.", move |t| &get(t).shared_postings_for_matchers_cache, move |t| &mut get_mut(t).shared_postings_for_matchers_cache);
        f.bool_var("blocks-storage.tsdb.head-postings-for-matchers-cache-invalidation", self.head_postings_for_matchers_cache_invalidation, "Whether head block postings should be tracked and invalidated.", move |t| &get(t).head_postings_for_matchers_cache_invalidation, move |t| &mut get_mut(t).head_postings_for_matchers_cache_invalidation);
        f.i64_var("blocks-storage.tsdb.head-postings-for-matchers-cache-versions", self.head_postings_for_matchers_cache_versions, "The size of the metric versions cache in each ingester.", move |t| &get(t).head_postings_for_matchers_cache_versions, move |t| &mut get_mut(t).head_postings_for_matchers_cache_versions);
        f.go_duration_var("blocks-storage.tsdb.head-postings-for-matchers-cache-ttl", self.head_postings_for_matchers_cache_ttl, "How long to cache postings for matchers in the Head and OOOHead.", move |t| &get(t).head_postings_for_matchers_cache_ttl, move |t| &mut get_mut(t).head_postings_for_matchers_cache_ttl);
        f.i64_var("blocks-storage.tsdb.head-postings-for-matchers-cache-max-bytes", self.head_postings_for_matchers_cache_max_bytes, "Maximum size, in bytes, of the cache for postings for matchers.", move |t| &get(t).head_postings_for_matchers_cache_max_bytes, move |t| &mut get_mut(t).head_postings_for_matchers_cache_max_bytes);
        f.bool_var("blocks-storage.tsdb.head-postings-for-matchers-cache-force", self.head_postings_for_matchers_cache_force, "Force the cache to be used for postings for matchers in the Head and OOOHead.", move |t| &get(t).head_postings_for_matchers_cache_force, move |t| &mut get_mut(t).head_postings_for_matchers_cache_force);
        f.go_duration_var("blocks-storage.tsdb.block-postings-for-matchers-cache-ttl", self.block_postings_for_matchers_cache_ttl, "How long to cache postings for matchers in each compacted block.", move |t| &get(t).block_postings_for_matchers_cache_ttl, move |t| &mut get_mut(t).block_postings_for_matchers_cache_ttl);
        f.i64_var("blocks-storage.tsdb.block-postings-for-matchers-cache-max-bytes", self.block_postings_for_matchers_cache_max_bytes, "Maximum size in bytes of the cache for postings for matchers in each compacted block.", move |t| &get(t).block_postings_for_matchers_cache_max_bytes, move |t| &mut get_mut(t).block_postings_for_matchers_cache_max_bytes);
        f.bool_var("blocks-storage.tsdb.block-postings-for-matchers-cache-force", self.block_postings_for_matchers_cache_force, "Force the cache to be used for postings for matchers in compacted blocks.", move |t| &get(t).block_postings_for_matchers_cache_force, move |t| &mut get_mut(t).block_postings_for_matchers_cache_force);
        f.i64_var("blocks-storage.tsdb.early-head-compaction-min-in-memory-series", self.early_head_compaction_min_in_memory_series, "Minimum in-memory series to trigger early head compaction.", move |t| &get(t).early_head_compaction_min_in_memory_series, move |t| &mut get_mut(t).early_head_compaction_min_in_memory_series);
        f.i64_var("blocks-storage.tsdb.early-head-compaction-min-estimated-series-reduction-percentage", self.early_head_compaction_min_estimated_series_reduction_percentage, "Minimum estimated series reduction percentage (0-100).", move |t| &get(t).early_head_compaction_min_estimated_series_reduction_percentage, move |t| &mut get_mut(t).early_head_compaction_min_estimated_series_reduction_percentage);
        f.bool_var("blocks-storage.tsdb.timely-head-compaction-enabled", self.timely_head_compaction_enabled, "Allows head compaction to happen when the min block range can no longer be appended.", move |t| &get(t).timely_head_compaction_enabled, move |t| &mut get_mut(t).timely_head_compaction_enabled);
        f.bool_var("blocks-storage.tsdb.bigger-out-of-order-blocks-for-old-samples", self.bigger_out_of_order_blocks_for_old_samples, "When enabled, ingester produces 24h blocks for out-of-order data.", move |t| &get(t).bigger_out_of_order_blocks_for_old_samples, move |t| &mut get_mut(t).bigger_out_of_order_blocks_for_old_samples);
        self.index_lookup_planning
            .register_flags_into("blocks-storage.tsdb.index-lookup-planning.", f, move |t| &get(t).index_lookup_planning, move |t| &mut get_mut(t).index_lookup_planning);
        f.bool_var("blocks-storage.tsdb.offset-catalogue.enabled", self.offset_catalogue.enabled, "Controls the maintaining of kafka offset catalogue per block.", move |t| &get(t).offset_catalogue.enabled, move |t| &mut get_mut(t).offset_catalogue.enabled);
        f.i64_var("blocks-storage.tsdb.offset-catalogue.sync-concurrency", self.offset_catalogue.sync_concurrency, "Maximum number of tenants concurrently syncing offset catalogue to disk.", move |t| &get(t).offset_catalogue.sync_concurrency, move |t| &mut get_mut(t).offset_catalogue.sync_concurrency);
    }

    pub fn validate(&self, active_series_enabled: bool) -> Result<(), ConfigError> {
        if self.ship_interval.0 > 0 && self.ship_concurrency <= 0 {
            return Err(ConfigError::Validation(
                "invalid TSDB ship concurrency".to_string(),
            ));
        }
        if self.head_compaction_interval.0 <= 0
            || self.head_compaction_interval.0 > 15 * 60 * 1_000_000_000
        {
            return Err(ConfigError::Validation(
                "invalid TSDB compaction interval".to_string(),
            ));
        }
        if self.head_compaction_concurrency <= 0 {
            return Err(ConfigError::Validation(
                "invalid TSDB compaction concurrency".to_string(),
            ));
        }
        if self.head_chunks_write_buffer_size_bytes < TSDB_MIN_WRITE_BUFFER_SIZE
            || self.head_chunks_write_buffer_size_bytes > TSDB_MAX_WRITE_BUFFER_SIZE
            || self.head_chunks_write_buffer_size_bytes % 1024 != 0
        {
            return Err(ConfigError::Validation(format!(
                "head chunks write buffer size must be a multiple of 1024 between {} and {}",
                TSDB_MIN_WRITE_BUFFER_SIZE, TSDB_MAX_WRITE_BUFFER_SIZE
            )));
        }
        if self.stripe_size <= 1 || (self.stripe_size & (self.stripe_size - 1)) != 0 {
            return Err(ConfigError::Validation(
                "invalid TSDB stripe size".to_string(),
            ));
        }
        if self.block_ranges_period.is_empty() {
            return Err(ConfigError::Validation(
                "empty block ranges for TSDB".to_string(),
            ));
        }
        if self.wal_segment_size_bytes <= 0 {
            return Err(ConfigError::Validation(
                "invalid TSDB WAL segment size bytes".to_string(),
            ));
        }
        if self.wal_replay_concurrency < 0 {
            return Err(ConfigError::Validation(
                "invalid TSDB WAL replay concurrency".to_string(),
            ));
        }
        if self.early_head_compaction_min_in_memory_series > 0 && !active_series_enabled {
            return Err(ConfigError::Validation(
                "early compaction requires -ingester.active-series-metrics-enabled to be enabled"
                    .to_string(),
            ));
        }
        if self.early_head_compaction_min_estimated_series_reduction_percentage < 0
            || self.early_head_compaction_min_estimated_series_reduction_percentage > 100
        {
            return Err(ConfigError::Validation(
                "early compaction minimum series reduction percentage must be a value between 0 and 100 (included)".to_string(),
            ));
        }
        self.index_lookup_planning.validate()
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct BlocksStorageConfig {
    #[serde(flatten)]
    pub bucket: BucketConfig,
    pub bucket_store: BucketStoreConfig,
    pub tsdb: TSDBConfig,
}

impl BlocksStorageConfig {
    pub fn register_flags_into<T: 'static>(
        &self,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        self.bucket
            .register_flags_into_with_dir("blocks-storage.", "blocks", f, move |t| &get(t).bucket, move |t| &mut get_mut(t).bucket);
        self.bucket_store.register_flags_into(f, move |t| &get(t).bucket_store, move |t| &mut get_mut(t).bucket_store);
        self.tsdb.register_flags_into(f, move |t| &get(t).tsdb, move |t| &mut get_mut(t).tsdb);
    }

    pub fn validate(&self, active_series_enabled: bool) -> Result<(), ConfigError> {
        self.bucket.validate()?;
        self.tsdb.validate(active_series_enabled)?;
        self.bucket_store.validate()
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MigrationConfig {
    pub distributor_send_to_ingesters_enabled: bool,
    pub ignore_ingest_storage_errors: bool,
    pub ingest_storage_max_wait_time: GoDuration,
}

impl MigrationConfig {
    pub fn register_flags_into<T: 'static>(
        &self,
        prefix: &str,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        f.bool_var(&format!("{prefix}distributor-send-to-ingesters-enabled"), self.distributor_send_to_ingesters_enabled, "When both this option and ingest storage are enabled, distributors write to both Kafka and ingesters.", move |t| &get(t).distributor_send_to_ingesters_enabled, move |t| &mut get_mut(t).distributor_send_to_ingesters_enabled);
        f.bool_var(&format!("{prefix}ignore-ingest-storage-errors"), self.ignore_ingest_storage_errors, "When enabled, errors writing to ingest storage are logged but do not affect write success.", move |t| &get(t).ignore_ingest_storage_errors, move |t| &mut get_mut(t).ignore_ingest_storage_errors);
        f.go_duration_var(&format!("{prefix}ingest-storage-max-wait-time"), self.ingest_storage_max_wait_time, "The maximum time a write request that goes through the ingest storage waits before it times out.", move |t| &get(t).ingest_storage_max_wait_time, move |t| &mut get_mut(t).ingest_storage_max_wait_time);
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct OrderedConsumptionConfig {
    pub enabled: bool,
    pub max_batch_records: i64,
    pub max_batch_wait: GoDuration,
    pub input_buffer_size: i64,
}

impl Default for OrderedConsumptionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_batch_records: 1024,
            max_batch_wait: GoDuration::from_millis(50),
            input_buffer_size: 0,
        }
    }
}

impl OrderedConsumptionConfig {
    pub fn register_flags_into<T: 'static>(
        &self,
        prefix: &str,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        f.bool_var(&format!("{prefix}enabled"), self.enabled, "Whether records from all write compartments' Kafka clusters are consumed in best-effort timestamp order.", move |t| &get(t).enabled, move |t| &mut get_mut(t).enabled);
        f.i64_var(&format!("{prefix}max-batch-records"), self.max_batch_records, "Soft upper bound on records buffered before forcing a flush.", move |t| &get(t).max_batch_records, move |t| &mut get_mut(t).max_batch_records);
        f.go_duration_var(&format!("{prefix}max-batch-wait"), self.max_batch_wait, "Maximum time records sit buffered before being flushed.", move |t| &get(t).max_batch_wait, move |t| &mut get_mut(t).max_batch_wait);
        f.i64_var(&format!("{prefix}input-buffer-size"), self.input_buffer_size, "Buffer size of the channel into which per-cluster consumers submit records.", move |t| &get(t).input_buffer_size, move |t| &mut get_mut(t).input_buffer_size);
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if !self.enabled {
            return Ok(());
        }
        if self.max_batch_records <= 0 {
            return Err(ConfigError::Validation(
                "ingest-storage.ordered-consumption.max-batch-records must be greater than 0"
                    .to_string(),
            ));
        }
        if self.max_batch_wait.0 <= 0 {
            return Err(ConfigError::Validation(
                "ingest-storage.ordered-consumption.max-batch-wait must be greater than 0"
                    .to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct KafkaConfig {
    pub backend: String,
    pub address: StringSliceCSV,
    pub topic: String,
    pub client_id: String,
    pub client_rack: String,
    pub dial_timeout: GoDuration,
    pub write_timeout: GoDuration,
    pub write_timeout_overhead: GoDuration,

    // Warpstream-only settings.
    pub warpstream_health_check_slow_multiplier: f64,
    pub warpstream_health_check_max_slow_fraction: f64,
    pub warpstream_health_check_faulty_threshold: f64,
    pub warpstream_health_check_max_faulty_fraction: f64,
    pub warpstream_hedge_min_delay: GoDuration,
    pub warpstream_hedge_max_agents: i64,
    pub warpstream_demoter_probe_interval: GoDuration,

    // SASL
    pub sasl_mechanism: SASLMechanism,
    pub sasl_username: String,
    #[serde(serialize_with = "crate::secret::serialize_flagext_secret")]
    pub sasl_password: String,
    #[serde(serialize_with = "crate::secret::serialize_flagext_secret")]
    pub sasl_oauthbearer_token: String,
    pub sasl_oauthbearer_zid: String,
    pub sasl_oauthbearer_extensions: LimitsMap<String>,
    pub sasl_oauthbearer_file_path: String,
    pub sasl_oauthbearer_http_socket_path: String,
    pub sasl_oauthbearer_http_socket_timeout: GoDuration,
    #[serde(serialize_with = "crate::secret::serialize_flagext_secret")]
    pub sasl_msk_iam_access_key: String,
    #[serde(serialize_with = "crate::secret::serialize_flagext_secret")]
    pub sasl_msk_iam_secret_key: String,
    #[serde(serialize_with = "crate::secret::serialize_flagext_secret")]
    pub sasl_msk_iam_session_token: String,
    pub sasl_msk_iam_user_agent: String,
    pub sasl_msk_iam_file_path: String,
    pub sasl_msk_iam_http_socket_path: String,
    pub sasl_msk_iam_http_socket_timeout: GoDuration,

    // TLS
    pub tls_enabled: bool,
    pub tls_cert_path: String,
    pub tls_key_path: String,
    pub tls_ca_path: String,
    pub tls_server_name: String,
    pub tls_insecure_skip_verify: bool,
    pub tls_cipher_suites: String,
    pub tls_min_version: String,

    pub consumer_group: String,
    pub consumer_group_offset_commit_interval: GoDuration,
    pub consumer_group_offset_commit_file_enforced: bool,
    pub last_produced_offset_poll_interval: GoDuration,
    pub last_produced_offset_retry_timeout: GoDuration,
    pub consume_from_position_at_startup: String,
    pub consume_from_timestamp_at_startup: i64,
    pub target_consumer_lag_at_startup: GoDuration,
    pub max_consumer_lag_at_startup: GoDuration,

    pub auto_create_topic_enabled: bool,
    pub auto_create_topic_default_partitions: i64,
    pub producer_max_record_size_bytes: i64,
    pub producer_max_buffered_bytes: i64,
    pub producer_compression: String,
    pub wait_strong_read_consistency_timeout: GoDuration,
    pub producer_record_version: i64,
    pub fetch_max_wait: GoDuration,
    pub fetch_concurrency_max: i64,
    pub use_compressed_bytes_as_fetch_max_bytes: bool,
    pub max_buffered_bytes: i64,
    pub ingestion_concurrency_max: i64,
    pub ingestion_concurrency_batch_size: i64,
    pub ingestion_concurrency_queue_capacity: i64,
    pub ingestion_concurrency_target_flushes_per_shard: i64,
    pub ingestion_concurrency_estimated_bytes_per_sample: i64,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            backend: "kafka".to_string(),
            address: StringSliceCSV::default(),
            topic: String::new(),
            client_id: String::new(),
            client_rack: String::new(),
            dial_timeout: GoDuration::from_secs(2),
            write_timeout: GoDuration::from_secs(10),
            write_timeout_overhead: GoDuration::from_secs(2),
            warpstream_health_check_slow_multiplier: 2.0,
            warpstream_health_check_max_slow_fraction: 0.3,
            warpstream_health_check_faulty_threshold: 0.2,
            warpstream_health_check_max_faulty_fraction: 0.3,
            warpstream_hedge_min_delay: GoDuration::from_millis(500),
            warpstream_hedge_max_agents: 3,
            warpstream_demoter_probe_interval: GoDuration::from_secs(1),
            sasl_mechanism: SASLMechanism::Plain,
            sasl_username: String::new(),
            sasl_password: String::new(),
            sasl_oauthbearer_token: String::new(),
            sasl_oauthbearer_zid: String::new(),
            sasl_oauthbearer_extensions: LimitsMap::new(),
            sasl_oauthbearer_file_path: String::new(),
            sasl_oauthbearer_http_socket_path: String::new(),
            sasl_oauthbearer_http_socket_timeout: GoDuration::from_secs(10),
            sasl_msk_iam_access_key: String::new(),
            sasl_msk_iam_secret_key: String::new(),
            sasl_msk_iam_session_token: String::new(),
            sasl_msk_iam_user_agent: String::new(),
            sasl_msk_iam_file_path: String::new(),
            sasl_msk_iam_http_socket_path: String::new(),
            sasl_msk_iam_http_socket_timeout: GoDuration::from_secs(10),
            tls_enabled: false,
            tls_cert_path: String::new(),
            tls_key_path: String::new(),
            tls_ca_path: String::new(),
            tls_server_name: String::new(),
            tls_insecure_skip_verify: false,
            tls_cipher_suites: String::new(),
            tls_min_version: String::new(),
            consumer_group: String::new(),
            consumer_group_offset_commit_interval: GoDuration::from_secs(1),
            consumer_group_offset_commit_file_enforced: false,
            last_produced_offset_poll_interval: GoDuration::from_secs(1),
            last_produced_offset_retry_timeout: GoDuration::from_secs(10),
            consume_from_position_at_startup: "last-offset".to_string(),
            consume_from_timestamp_at_startup: 0,
            target_consumer_lag_at_startup: GoDuration::from_secs(2),
            max_consumer_lag_at_startup: GoDuration::from_secs(15),
            auto_create_topic_enabled: true,
            auto_create_topic_default_partitions: -1,
            producer_max_record_size_bytes: MAX_PRODUCER_RECORD_DATA_BYTES_LIMIT,
            producer_max_buffered_bytes: 1024 * 1024 * 1024,
            producer_compression: String::new(),
            wait_strong_read_consistency_timeout: GoDuration::from_secs(20),
            producer_record_version: 2,
            fetch_max_wait: GoDuration::from_secs(5),
            fetch_concurrency_max: 12,
            use_compressed_bytes_as_fetch_max_bytes: true,
            max_buffered_bytes: 1_000_000_000,
            ingestion_concurrency_max: 8,
            ingestion_concurrency_batch_size: 150,
            ingestion_concurrency_queue_capacity: 3,
            ingestion_concurrency_target_flushes_per_shard: 40,
            ingestion_concurrency_estimated_bytes_per_sample: 200,
        }
    }
}

impl KafkaConfig {
    pub fn register_flags_into<T: 'static>(
        &self,
        prefix: &str,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        f.string_var(&format!("{prefix}backend"), &self.backend, "The Kafka backend implementation. Supported values: kafka, warpstream.", move |t| &get(t).backend, move |t| &mut get_mut(t).backend);
        f.string_slice_csv_var(&format!("{prefix}address"), &self.address, "The Kafka seed broker address, or a comma-separated list of seed broker addresses.", move |t| &get(t).address, move |t| &mut get_mut(t).address);
        f.f64_var(&format!("{prefix}warpstream-health-check-slow-multiplier"), self.warpstream_health_check_slow_multiplier, "Mark an agent as slow when its window-average latency exceeds this multiple.", move |t| &get(t).warpstream_health_check_slow_multiplier, move |t| &mut get_mut(t).warpstream_health_check_slow_multiplier);
        f.f64_var(&format!("{prefix}warpstream-health-check-max-slow-fraction"), self.warpstream_health_check_max_slow_fraction, "Suppress slow-based hedging when more than this fraction of agents are slow.", move |t| &get(t).warpstream_health_check_max_slow_fraction, move |t| &mut get_mut(t).warpstream_health_check_max_slow_fraction);
        f.f64_var(&format!("{prefix}warpstream-health-check-faulty-threshold"), self.warpstream_health_check_faulty_threshold, "Mark an agent as faulty when its observed error rate exceeds this fraction.", move |t| &get(t).warpstream_health_check_faulty_threshold, move |t| &mut get_mut(t).warpstream_health_check_faulty_threshold);
        f.f64_var(&format!("{prefix}warpstream-health-check-max-faulty-fraction"), self.warpstream_health_check_max_faulty_fraction, "Suppress faulty-based hedging when more than this fraction of agents are faulty.", move |t| &get(t).warpstream_health_check_max_faulty_fraction, move |t| &mut get_mut(t).warpstream_health_check_max_faulty_fraction);
        f.go_duration_var(&format!("{prefix}warpstream-hedge-min-delay"), self.warpstream_hedge_min_delay, "Floor on the dynamically-computed hedge delay.", move |t| &get(t).warpstream_hedge_min_delay, move |t| &mut get_mut(t).warpstream_hedge_min_delay);
        f.i64_var(&format!("{prefix}warpstream-hedge-max-agents"), self.warpstream_hedge_max_agents, "Cap on how many per-partition candidates the hedge fanout considers.", move |t| &get(t).warpstream_hedge_max_agents, move |t| &mut get_mut(t).warpstream_hedge_max_agents);
        f.go_duration_var(&format!("{prefix}warpstream-demoter-probe-interval"), self.warpstream_demoter_probe_interval, "Minimum wall-clock gap between probes to a demoted agent.", move |t| &get(t).warpstream_demoter_probe_interval, move |t| &mut get_mut(t).warpstream_demoter_probe_interval);
        f.string_var(&format!("{prefix}topic"), &self.topic, "The Kafka topic name.", move |t| &get(t).topic, move |t| &mut get_mut(t).topic);
        f.string_var(&format!("{prefix}client-id"), &self.client_id, "The Kafka client ID.", move |t| &get(t).client_id, move |t| &mut get_mut(t).client_id);
        f.string_var(&format!("{prefix}client-rack"), &self.client_rack, "The rack identifier for this Kafka client.", move |t| &get(t).client_rack, move |t| &mut get_mut(t).client_rack);
        f.go_duration_var(&format!("{prefix}dial-timeout"), self.dial_timeout, "The maximum time allowed to establish the TCP connection to a Kafka broker.", move |t| &get(t).dial_timeout, move |t| &mut get_mut(t).dial_timeout);
        f.go_duration_var(&format!("{prefix}write-timeout"), self.write_timeout, "How long to wait for an incoming write request to be committed to Kafka.", move |t| &get(t).write_timeout, move |t| &mut get_mut(t).write_timeout);
        f.go_duration_var(&format!("{prefix}write-timeout-overhead"), self.write_timeout_overhead, "Additional time added on top of the write timeout.", move |t| &get(t).write_timeout_overhead, move |t| &mut get_mut(t).write_timeout_overhead);
        f.string_var(&format!("{prefix}consumer-group"), &self.consumer_group, "The consumer group used by the consumer.", move |t| &get(t).consumer_group, move |t| &mut get_mut(t).consumer_group);
        f.go_duration_var(&format!("{prefix}consumer-group-offset-commit-interval"), self.consumer_group_offset_commit_interval, "How frequently a consumer should commit the consumed offset to Kafka.", move |t| &get(t).consumer_group_offset_commit_interval, move |t| &mut get_mut(t).consumer_group_offset_commit_interval);
        f.bool_var(&format!("{prefix}consumer-group-offset-commit-file-enforced"), self.consumer_group_offset_commit_file_enforced, "When true, the file-based offset stored in the TSDB directory is enforced at startup.", move |t| &get(t).consumer_group_offset_commit_file_enforced, move |t| &mut get_mut(t).consumer_group_offset_commit_file_enforced);
        f.go_duration_var(&format!("{prefix}last-produced-offset-poll-interval"), self.last_produced_offset_poll_interval, "How frequently to poll the last produced offset.", move |t| &get(t).last_produced_offset_poll_interval, move |t| &mut get_mut(t).last_produced_offset_poll_interval);
        f.go_duration_var(&format!("{prefix}last-produced-offset-retry-timeout"), self.last_produced_offset_retry_timeout, "How long to retry a failed request to get the last produced offset.", move |t| &get(t).last_produced_offset_retry_timeout, move |t| &mut get_mut(t).last_produced_offset_retry_timeout);
        f.string_var(&format!("{prefix}consume-from-position-at-startup"), &self.consume_from_position_at_startup, "From which position to start consuming the partition at startup.", move |t| &get(t).consume_from_position_at_startup, move |t| &mut get_mut(t).consume_from_position_at_startup);
        f.i64_var(&format!("{prefix}consume-from-timestamp-at-startup"), self.consume_from_timestamp_at_startup, "Milliseconds timestamp after which the consumption of the partition starts.", move |t| &get(t).consume_from_timestamp_at_startup, move |t| &mut get_mut(t).consume_from_timestamp_at_startup);
        f.go_duration_var(&format!("{prefix}target-consumer-lag-at-startup"), self.target_consumer_lag_at_startup, "The best-effort maximum lag a consumer tries to achieve at startup.", move |t| &get(t).target_consumer_lag_at_startup, move |t| &mut get_mut(t).target_consumer_lag_at_startup);
        f.go_duration_var(&format!("{prefix}max-consumer-lag-at-startup"), self.max_consumer_lag_at_startup, "The guaranteed maximum lag before a consumer is considered caught up.", move |t| &get(t).max_consumer_lag_at_startup, move |t| &mut get_mut(t).max_consumer_lag_at_startup);
        f.bool_var(&format!("{prefix}auto-create-topic-enabled"), self.auto_create_topic_enabled, "Enable auto-creation of Kafka topic on startup.", move |t| &get(t).auto_create_topic_enabled, move |t| &mut get_mut(t).auto_create_topic_enabled);
        f.i64_var(&format!("{prefix}auto-create-topic-default-partitions"), self.auto_create_topic_default_partitions, "Default partitions when auto-creating Kafka topic.", move |t| &get(t).auto_create_topic_default_partitions, move |t| &mut get_mut(t).auto_create_topic_default_partitions);
        f.i64_var(&format!("{prefix}producer-max-record-size-bytes"), self.producer_max_record_size_bytes, "Maximum size of a Kafka record data generated by the producer.", move |t| &get(t).producer_max_record_size_bytes, move |t| &mut get_mut(t).producer_max_record_size_bytes);
        f.i64_var(&format!("{prefix}producer-max-buffered-bytes"), self.producer_max_buffered_bytes, "Maximum size of buffered produced records sent to Kafka.", move |t| &get(t).producer_max_buffered_bytes, move |t| &mut get_mut(t).producer_max_buffered_bytes);
        f.string_var(&format!("{prefix}producer-compression"), &self.producer_compression, "Compression codec used by the Kafka producer.", move |t| &get(t).producer_compression, move |t| &mut get_mut(t).producer_compression);
        f.go_duration_var(&format!("{prefix}wait-strong-read-consistency-timeout"), self.wait_strong_read_consistency_timeout, "Maximum wait for strong read consistency.", move |t| &get(t).wait_strong_read_consistency_timeout, move |t| &mut get_mut(t).wait_strong_read_consistency_timeout);
        f.i64_var(&format!("{prefix}producer-record-version"), self.producer_record_version, "The record version that this producer sends.", move |t| &get(t).producer_record_version, move |t| &mut get_mut(t).producer_record_version);
        f.go_duration_var(&format!("{prefix}fetch-max-wait"), self.fetch_max_wait, "Maximum time a Kafka broker waits before returning Fetch response.", move |t| &get(t).fetch_max_wait, move |t| &mut get_mut(t).fetch_max_wait);
        f.i64_var(&format!("{prefix}fetch-concurrency-max"), self.fetch_concurrency_max, "Maximum number of concurrent fetch requests during startup.", move |t| &get(t).fetch_concurrency_max, move |t| &mut get_mut(t).fetch_concurrency_max);
        f.bool_var(&format!("{prefix}use-compressed-bytes-as-fetch-max-bytes"), self.use_compressed_bytes_as_fetch_max_bytes, "Compute MaxBytes using compressed size of previous records.", move |t| &get(t).use_compressed_bytes_as_fetch_max_bytes, move |t| &mut get_mut(t).use_compressed_bytes_as_fetch_max_bytes);
        f.i64_var(&format!("{prefix}max-buffered-bytes"), self.max_buffered_bytes, "Maximum number of buffered records ready to be processed.", move |t| &get(t).max_buffered_bytes, move |t| &mut get_mut(t).max_buffered_bytes);
        f.i64_var(&format!("{prefix}ingestion-concurrency-max"), self.ingestion_concurrency_max, "Maximum number of concurrent ingestion streams to the TSDB head.", move |t| &get(t).ingestion_concurrency_max, move |t| &mut get_mut(t).ingestion_concurrency_max);
        f.i64_var(&format!("{prefix}ingestion-concurrency-batch-size"), self.ingestion_concurrency_batch_size, "Number of timeseries to batch together before ingesting to the TSDB head.", move |t| &get(t).ingestion_concurrency_batch_size, move |t| &mut get_mut(t).ingestion_concurrency_batch_size);
        f.i64_var(&format!("{prefix}ingestion-concurrency-queue-capacity"), self.ingestion_concurrency_queue_capacity, "Number of batches to prepare and queue.", move |t| &get(t).ingestion_concurrency_queue_capacity, move |t| &mut get_mut(t).ingestion_concurrency_queue_capacity);
        f.i64_var(&format!("{prefix}ingestion-concurrency-target-flushes-per-shard"), self.ingestion_concurrency_target_flushes_per_shard, "Expected number of times to ingest timeseries to the TSDB head after batching.", move |t| &get(t).ingestion_concurrency_target_flushes_per_shard, move |t| &mut get_mut(t).ingestion_concurrency_target_flushes_per_shard);
        f.i64_var(&format!("{prefix}ingestion-concurrency-estimated-bytes-per-sample"), self.ingestion_concurrency_estimated_bytes_per_sample, "Estimated number of bytes per sample.", move |t| &get(t).ingestion_concurrency_estimated_bytes_per_sample, move |t| &mut get_mut(t).ingestion_concurrency_estimated_bytes_per_sample);

        // SASL
        f.sasl_mechanism_var(&format!("{prefix}sasl-mechanism"), self.sasl_mechanism, "The SASL mechanism used to authenticate to Kafka.", move |t| &get(t).sasl_mechanism, move |t| &mut get_mut(t).sasl_mechanism);
        f.string_var(&format!("{prefix}sasl-username"), &self.sasl_username, "The username used to authenticate to Kafka using SASL.", move |t| &get(t).sasl_username, move |t| &mut get_mut(t).sasl_username);
        f.string_var(&format!("{prefix}sasl-password"), &self.sasl_password, "The password used to authenticate to Kafka using SASL.", move |t| &get(t).sasl_password, move |t| &mut get_mut(t).sasl_password);
        f.string_var(&format!("{prefix}sasl-oauthbearer-token"), &self.sasl_oauthbearer_token, "The OAuth token to use to authenticate to Kafka.", move |t| &get(t).sasl_oauthbearer_token, move |t| &mut get_mut(t).sasl_oauthbearer_token);
        f.string_var(&format!("{prefix}sasl-oauthbearer-zid"), &self.sasl_oauthbearer_zid, "Optional authorization ID to use when authenticating with OAUTHBEARER.", move |t| &get(t).sasl_oauthbearer_zid, move |t| &mut get_mut(t).sasl_oauthbearer_zid);
        f.limits_map_var(&format!("{prefix}sasl-oauthbearer-extensions"), &self.sasl_oauthbearer_extensions, "Optional additional OAuth extensions as JSON object.", move |t| &get(t).sasl_oauthbearer_extensions, move |t| &mut get_mut(t).sasl_oauthbearer_extensions);
        f.string_var(&format!("{prefix}sasl-oauthbearer-file-path"), &self.sasl_oauthbearer_file_path, "Path to a file containing an OAuth token.", move |t| &get(t).sasl_oauthbearer_file_path, move |t| &mut get_mut(t).sasl_oauthbearer_file_path);
        f.string_var(&format!("{prefix}sasl-oauthbearer-http-socket-path"), &self.sasl_oauthbearer_http_socket_path, "Path to a Unix domain socket to fetch an OAuth token from via HTTP.", move |t| &get(t).sasl_oauthbearer_http_socket_path, move |t| &mut get_mut(t).sasl_oauthbearer_http_socket_path);
        f.go_duration_var(&format!("{prefix}sasl-oauthbearer-http-socket-timeout"), self.sasl_oauthbearer_http_socket_timeout, "Timeout for requesting the token from the HTTP socket.", move |t| &get(t).sasl_oauthbearer_http_socket_timeout, move |t| &mut get_mut(t).sasl_oauthbearer_http_socket_timeout);
        f.string_var(&format!("{prefix}sasl-msk-iam-access-key"), &self.sasl_msk_iam_access_key, "AWS access key ID for SASL AWS_MSK_IAM.", move |t| &get(t).sasl_msk_iam_access_key, move |t| &mut get_mut(t).sasl_msk_iam_access_key);
        f.string_var(&format!("{prefix}sasl-msk-iam-secret-key"), &self.sasl_msk_iam_secret_key, "AWS secret access key for SASL AWS_MSK_IAM.", move |t| &get(t).sasl_msk_iam_secret_key, move |t| &mut get_mut(t).sasl_msk_iam_secret_key);
        f.string_var(&format!("{prefix}sasl-msk-iam-session-token"), &self.sasl_msk_iam_session_token, "Optional AWS session token for SASL AWS_MSK_IAM.", move |t| &get(t).sasl_msk_iam_session_token, move |t| &mut get_mut(t).sasl_msk_iam_session_token);
        f.string_var(&format!("{prefix}sasl-msk-iam-user-agent"), &self.sasl_msk_iam_user_agent, "Optional user agent for SASL AWS_MSK_IAM.", move |t| &get(t).sasl_msk_iam_user_agent, move |t| &mut get_mut(t).sasl_msk_iam_user_agent);
        f.string_var(&format!("{prefix}sasl-msk-iam-file-path"), &self.sasl_msk_iam_file_path, "Path to a file containing AWS credentials.", move |t| &get(t).sasl_msk_iam_file_path, move |t| &mut get_mut(t).sasl_msk_iam_file_path);
        f.string_var(&format!("{prefix}sasl-msk-iam-http-socket-path"), &self.sasl_msk_iam_http_socket_path, "Path to a Unix domain socket to fetch AWS credentials from via HTTP.", move |t| &get(t).sasl_msk_iam_http_socket_path, move |t| &mut get_mut(t).sasl_msk_iam_http_socket_path);
        f.go_duration_var(&format!("{prefix}sasl-msk-iam-http-socket-timeout"), self.sasl_msk_iam_http_socket_timeout, "Timeout for requesting AWS credentials from the HTTP socket.", move |t| &get(t).sasl_msk_iam_http_socket_timeout, move |t| &mut get_mut(t).sasl_msk_iam_http_socket_timeout);

        // TLS
        f.bool_var(&format!("{prefix}tls-enabled"), self.tls_enabled, "Enable TLS for the Kafka client connection.", move |t| &get(t).tls_enabled, move |t| &mut get_mut(t).tls_enabled);
        f.string_var(&format!("{prefix}tls-cert-path"), &self.tls_cert_path, "Path to the client certificate.", move |t| &get(t).tls_cert_path, move |t| &mut get_mut(t).tls_cert_path);
        f.string_var(&format!("{prefix}tls-key-path"), &self.tls_key_path, "Path to the client key.", move |t| &get(t).tls_key_path, move |t| &mut get_mut(t).tls_key_path);
        f.string_var(&format!("{prefix}tls-ca-path"), &self.tls_ca_path, "Path to the CA certificate.", move |t| &get(t).tls_ca_path, move |t| &mut get_mut(t).tls_ca_path);
        f.string_var(&format!("{prefix}tls-server-name"), &self.tls_server_name, "Override the expected server name.", move |t| &get(t).tls_server_name, move |t| &mut get_mut(t).tls_server_name);
        f.bool_var(&format!("{prefix}tls-insecure-skip-verify"), self.tls_insecure_skip_verify, "Skip TLS certificate verification.", move |t| &get(t).tls_insecure_skip_verify, move |t| &mut get_mut(t).tls_insecure_skip_verify);
        f.string_var(&format!("{prefix}tls-cipher-suites"), &self.tls_cipher_suites, "TLS cipher suites.", move |t| &get(t).tls_cipher_suites, move |t| &mut get_mut(t).tls_cipher_suites);
        f.string_var(&format!("{prefix}tls-min-version"), &self.tls_min_version, "Minimum TLS version.", move |t| &get(t).tls_min_version, move |t| &mut get_mut(t).tls_min_version);
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.backend != "kafka" && self.backend != "warpstream" {
            return Err(ConfigError::Validation(
                "the configured kafka backend is invalid, must be one of: kafka, warpstream"
                    .to_string(),
            ));
        }
        if self.address.0.is_empty() {
            return Err(ConfigError::Validation(
                "the Kafka address has not been configured".to_string(),
            ));
        }
        if self.topic.is_empty() {
            return Err(ConfigError::Validation(
                "the Kafka topic has not been configured".to_string(),
            ));
        }
        if !matches!(
            self.consume_from_position_at_startup.as_str(),
            "last-offset" | "start" | "end" | "timestamp"
        ) {
            return Err(ConfigError::Validation(
                "the configured consume position is invalid".to_string(),
            ));
        }
        if self.consume_from_position_at_startup == "timestamp" {
            if self.consume_from_timestamp_at_startup < 900_000_000_000 {
                return Err(ConfigError::Validation(
                    "the configured consume position is invalid: configured timestamp must be a millisecond timestamp".to_string(),
                ));
            }
        } else if self.consume_from_timestamp_at_startup > 0 {
            return Err(ConfigError::Validation(
                "the configured consume position is invalid: configured consume position must be set to \"timestamp\"".to_string(),
            ));
        }
        if self.producer_max_record_size_bytes < MIN_PRODUCER_RECORD_DATA_BYTES_LIMIT
            || self.producer_max_record_size_bytes > MAX_PRODUCER_RECORD_DATA_BYTES_LIMIT
        {
            return Err(ConfigError::Validation(format!(
                "the configured producer max record size bytes must be a value between {} and {}",
                MIN_PRODUCER_RECORD_DATA_BYTES_LIMIT, MAX_PRODUCER_RECORD_DATA_BYTES_LIMIT
            )));
        }
        if !matches!(
            self.producer_compression.as_str(),
            "" | "none" | "gzip" | "snappy" | "lz4" | "zstd"
        ) {
            return Err(ConfigError::Validation(
                "the configured Kafka producer compression codec is invalid".to_string(),
            ));
        }
        if (self.target_consumer_lag_at_startup.0 != 0)
            != (self.max_consumer_lag_at_startup.0 != 0)
        {
            return Err(ConfigError::Validation(
                "the target and max consumer lag at startup must be either both set to 0 or to a value greater than 0".to_string(),
            ));
        }
        if self.max_consumer_lag_at_startup.0 < self.target_consumer_lag_at_startup.0 {
            return Err(ConfigError::Validation(
                "the configured max consumer lag at startup must greater or equal than the configured target consumer lag".to_string(),
            ));
        }
        if !(0..=2).contains(&self.producer_record_version) {
            return Err(ConfigError::Validation(
                "invalid record format version".to_string(),
            ));
        }
        if self.fetch_max_wait.0 < 5 * 1_000_000_000 || self.fetch_max_wait.0 > 30 * 1_000_000_000
        {
            return Err(ConfigError::Validation(
                "the Kafka fetch max wait must be between 5s and 30s".to_string(),
            ));
        }
        if self.fetch_concurrency_max < 0 {
            return Err(ConfigError::Validation(
                "ingest-storage.kafka.fetch-concurrency-max must be greater than or equal to 0"
                    .to_string(),
            ));
        }
        if self.max_buffered_bytes >= i32::MAX as i64 {
            return Err(ConfigError::Validation(format!(
                "ingest-storage.kafka.max-buffered-bytes must be less than {}",
                i32::MAX
            )));
        }
        if self.ingestion_concurrency_max < 0 {
            return Err(ConfigError::Validation(
                "ingest-storage.kafka.ingestion-concurrency-max must either be set to 0 or to a value greater than 0".to_string(),
            ));
        }
        if self.ingestion_concurrency_max >= 1
            && (self.ingestion_concurrency_batch_size <= 0
                || self.ingestion_concurrency_queue_capacity <= 0
                || self.ingestion_concurrency_estimated_bytes_per_sample <= 0
                || self.ingestion_concurrency_target_flushes_per_shard <= 0)
        {
            return Err(ConfigError::Validation(
                "ingest-storage.kafka.ingestion-concurrency-* params must be greater than 0"
                    .to_string(),
            ));
        }
        if self.auto_create_topic_enabled
            && self.auto_create_topic_default_partitions != -1
            && self.auto_create_topic_default_partitions < 1
        {
            return Err(ConfigError::Validation(
                "ingest-storage.kafka.auto-create-topic-default-partitions must be -1 or greater than 0".to_string(),
            ));
        }
        self.validate_sasl()?;
        if self.tls_enabled {
            self.tls_client_config().get_tls_config().map_err(|e| {
                ConfigError::Validation(format!("invalid Kafka TLS config: {e}"))
            })?;
        }
        if self.write_timeout_overhead.0 < MIN_KAFKA_REQUEST_TIMEOUT_OVERHEAD_NANOS {
            return Err(ConfigError::Validation(format!(
                "ingest-storage.kafka.write-timeout-overhead must be at least 100ms, is {}",
                self.write_timeout_overhead
            )));
        }
        if self.backend == "warpstream" && self.write_timeout.0 < self.write_timeout_overhead.0 * 2
        {
            return Err(ConfigError::Validation(
                "ingest-storage.kafka.write-timeout must be greater than or equal to twice write-timeout-overhead when backend=warpstream".to_string(),
            ));
        }
        Ok(())
    }

    /// The inlined dskit `tls.ClientConfig` (`tls_*` keys) of this Kafka client config.
    pub fn tls_client_config(&self) -> mimir_common::tls::ClientConfig {
        mimir_common::tls::ClientConfig {
            cert_path: self.tls_cert_path.clone(),
            key_path: self.tls_key_path.clone(),
            ca_path: self.tls_ca_path.clone(),
            server_name: self.tls_server_name.clone(),
            insecure_skip_verify: self.tls_insecure_skip_verify,
            cipher_suites: self.tls_cipher_suites.clone(),
            min_version: self.tls_min_version.clone(),
        }
    }

    fn validate_sasl(&self) -> Result<(), ConfigError> {
        match self.sasl_mechanism {
            SASLMechanism::Plain => {
                if self.sasl_username.is_empty() != self.sasl_password.is_empty() {
                    return Err(ConfigError::Validation(
                        "the SASL username and password must be both configured to enable SASL authentication".to_string(),
                    ));
                }
            }
            SASLMechanism::ScramSha256 | SASLMechanism::ScramSha512 => {
                if self.sasl_username.is_empty() || self.sasl_password.is_empty() {
                    return Err(ConfigError::Validation(
                        "the SASL username and password must be both configured to enable SASL authentication".to_string(),
                    ));
                }
            }
            SASLMechanism::Oauthbearer => {
                let count = [!self.sasl_oauthbearer_token.is_empty(),
                    !self.sasl_oauthbearer_file_path.is_empty(),
                    !self.sasl_oauthbearer_http_socket_path.is_empty()]
                .into_iter()
                .filter(|&b| b)
                .count();
                if count != 1 {
                    return Err(ConfigError::Validation(
                        "exactly one of OAuth token, file path, or HTTP socket path must be configured to enable SASL OAUTHBEARER authentication".to_string(),
                    ));
                }
            }
            SASLMechanism::MskIam => {
                let has_key = !self.sasl_msk_iam_access_key.is_empty();
                let has_secret = !self.sasl_msk_iam_secret_key.is_empty();
                if has_key != has_secret {
                    return Err(ConfigError::Validation(
                        "both access key and secret key must be configured for static AWS_MSK_IAM credentials".to_string(),
                    ));
                }
                let count = [
                    has_key && has_secret,
                    !self.sasl_msk_iam_file_path.is_empty(),
                    !self.sasl_msk_iam_http_socket_path.is_empty(),
                ]
                .into_iter()
                .filter(|&b| b)
                .count();
                if count != 1 {
                    return Err(ConfigError::Validation(
                        "exactly one of static credentials, file path, or HTTP socket path must be configured to enable SASL AWS_MSK_IAM authentication".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct IngestStorageConfig {
    pub enabled: bool,
    pub kafka: KafkaConfig,
    pub migration: MigrationConfig,
    pub ordered_consumption: OrderedConsumptionConfig,
    pub write_logs_fsync_before_kafka_commit_concurrency: i64,
}

impl Default for IngestStorageConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            kafka: KafkaConfig::default(),
            migration: MigrationConfig::default(),
            ordered_consumption: OrderedConsumptionConfig::default(),
            write_logs_fsync_before_kafka_commit_concurrency: 4,
        }
    }
}

impl IngestStorageConfig {
    pub fn register_flags_into<T: 'static>(
        &self,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        f.bool_var("ingest-storage.enabled", self.enabled, "True to enable the ingestion via object storage.", move |t| &get(t).enabled, move |t| &mut get_mut(t).enabled);
        f.i64_var("ingest-storage.write-logs-fsync-before-kafka-commit-concurrency", self.write_logs_fsync_before_kafka_commit_concurrency, "Number of tenants to concurrently fsync WAL and WBL before Kafka offsets are committed, must be at least 1.", move |t| &get(t).write_logs_fsync_before_kafka_commit_concurrency, move |t| &mut get_mut(t).write_logs_fsync_before_kafka_commit_concurrency);
        self.kafka
            .register_flags_into("ingest-storage.kafka.", f, move |t| &get(t).kafka, move |t| &mut get_mut(t).kafka);
        self.migration
            .register_flags_into("ingest-storage.migration.", f, move |t| &get(t).migration, move |t| &mut get_mut(t).migration);
        self.ordered_consumption
            .register_flags_into("ingest-storage.ordered-consumption.", f, move |t| &get(t).ordered_consumption, move |t| &mut get_mut(t).ordered_consumption);
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if !self.enabled {
            return Ok(());
        }
        if self.write_logs_fsync_before_kafka_commit_concurrency < 1 {
            return Err(ConfigError::Validation(format!(
                "the configured number of tenants to fsync concurrently before Kafka offsets are committed must be at least 1, is {}",
                self.write_logs_fsync_before_kafka_commit_concurrency
            )));
        }
        self.kafka.validate()?;
        self.ordered_consumption.validate()
    }
}
