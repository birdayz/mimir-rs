//! Server, memberlist, runtime_config, activity_tracker, usage_stats, and compartments configs.
//!
//! Ported from:
//! - github.com/grafana/dskit/server/server.go
//! - github.com/grafana/dskit/clusterutil/cluster_validation_config.go
//! - github.com/grafana/dskit/kv/memberlist/memberlist_client.go
//! - github.com/grafana/dskit/kv/memberlist/tcp_transport.go
//! - github.com/grafana/dskit/kv/memberlist/node_zone_aware_routing.go
//! - github.com/grafana/dskit/kv/memberlist/propagation_tracker.go
//! - github.com/grafana/mimir/pkg/mimir/runtime_config.go
//! - github.com/grafana/mimir/pkg/util/activitytracker/activity_tracker.go
//! - github.com/grafana/mimir/pkg/usagestats/reporter.go
//! - github.com/grafana/mimir/pkg/compartments/config.go

use serde::{Deserialize, Serialize};

use crate::duration::GoDuration;
use crate::error::ConfigError;
use crate::flags::FlagSet;
use crate::ingester::ClusterValidationConfig;
use crate::types::{LogLevel, StringSliceCSV, StringSliceCSVMulti};

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TLSConfig {
    pub cert: String,
    #[serde(serialize_with = "crate::secret::serialize_prometheus_secret")]
    pub key: String,
    pub client_ca: String,
    pub cert_file: String,
    pub key_file: String,
    pub client_auth_type: String,
    pub client_ca_file: String,
}

impl TLSConfig {
    pub fn register_flags_into<T: 'static>(
        &self,
        prefix: &str,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        f.string_var(&format!("{prefix}cert-path"), &self.cert_file, "TLS server cert path.", move |t| &get(t).cert_file, move |t| &mut get_mut(t).cert_file);
        f.string_var(&format!("{prefix}key-path"), &self.key_file, "TLS server key path.", move |t| &get(t).key_file, move |t| &mut get_mut(t).key_file);
        f.string_var(&format!("{prefix}client-auth"), &self.client_auth_type, "TLS Client Auth type.", move |t| &get(t).client_auth_type, move |t| &mut get_mut(t).client_auth_type);
        f.string_var(&format!("{prefix}ca-path"), &self.client_ca_file, "TLS Client CA path.", move |t| &get(t).client_ca_file, move |t| &mut get_mut(t).client_ca_file);
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ClusterValidationProtocolConfig {
    pub enabled: bool,
    pub soft_validation: bool,
}

impl ClusterValidationProtocolConfig {
    pub fn register_flags_into<T: 'static>(
        &self,
        prefix: &str,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        f.bool_var(&format!("{prefix}soft-validation"), self.soft_validation, "When enabled, soft cluster label validation is executed.", move |t| &get(t).soft_validation, move |t| &mut get_mut(t).soft_validation);
        f.bool_var(&format!("{prefix}enabled"), self.enabled, "When enabled, cluster label validation is executed.", move |t| &get(t).enabled, move |t| &mut get_mut(t).enabled);
    }

    pub fn validate(&self, proto: &str, label: &str) -> Result<(), ConfigError> {
        if label.is_empty() {
            if self.enabled || self.soft_validation {
                return Err(ConfigError::Validation(format!(
                    "{proto}: validation cannot be enabled if cluster validation label is not configured"
                )));
            }
            return Ok(());
        }
        if !self.enabled && self.soft_validation {
            return Err(ConfigError::Validation(format!(
                "{proto}: soft validation can be enabled only if cluster validation is enabled"
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ClusterValidationProtocolConfigForHTTP {
    #[serde(flatten)]
    pub protocol: ClusterValidationProtocolConfig,
    pub excluded_paths: StringSliceCSV,
    pub excluded_user_agents: StringSliceCSV,
}

impl ClusterValidationProtocolConfigForHTTP {
    pub fn register_flags_into<T: 'static>(
        &self,
        prefix: &str,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        self.protocol.register_flags_into(prefix, f, move |t| &get(t).protocol, move |t| &mut get_mut(t).protocol);
        f.string_slice_csv_var(&format!("{prefix}excluded-paths"), &self.excluded_paths, "Comma-separated list of url paths that are excluded from the cluster validation check.", move |t| &get(t).excluded_paths, move |t| &mut get_mut(t).excluded_paths);
        f.string_slice_csv_var(&format!("{prefix}excluded-user-agents"), &self.excluded_user_agents, "Comma-separated list of user agents that are excluded from the cluster validation check.", move |t| &get(t).excluded_user_agents, move |t| &mut get_mut(t).excluded_user_agents);
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ServerClusterValidationConfig {
    pub label: String,
    pub additional_labels: StringSliceCSV,
    pub grpc: ClusterValidationProtocolConfig,
    pub http: ClusterValidationProtocolConfigForHTTP,
}

impl ServerClusterValidationConfig {
    pub fn register_flags_into<T: 'static>(
        &self,
        prefix: &str,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        f.string_var(&format!("{prefix}label"), &self.label, "Primary cluster validation label.", move |t| &get(t).label, move |t| &mut get_mut(t).label);
        f.string_slice_csv_var(&format!("{prefix}additional-labels"), &self.additional_labels, "Comma-separated list of additional cluster validation labels.", move |t| &get(t).additional_labels, move |t| &mut get_mut(t).additional_labels);
        self.grpc
            .register_flags_into(&format!("{prefix}grpc."), f, move |t| &get(t).grpc, move |t| &mut get_mut(t).grpc);
        self.http
            .register_flags_into(&format!("{prefix}http."), f, move |t| &get(t).http, move |t| &mut get_mut(t).http);
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if !self.additional_labels.0.is_empty() && self.label.is_empty() {
            return Err(ConfigError::Validation(
                "additional cluster validation labels require primary label to be set".to_string(),
            ));
        }
        self.grpc.validate("grpc", &self.label)?;
        self.http.protocol.validate("http", &self.label)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ServerConfig {
    pub http_listen_network: String,
    pub http_listen_address: String,
    pub http_listen_port: i64,
    pub http_listen_conn_limit: i64,
    pub grpc_listen_network: String,
    pub grpc_listen_address: String,
    pub grpc_listen_port: i64,
    pub grpc_listen_conn_limit: i64,
    pub grpc_collect_max_streams_by_conn: bool,
    pub proxy_protocol_enabled: bool,
    pub tls_cipher_suites: String,
    pub tls_min_version: String,
    pub tls_curve_preferences: String,
    pub http_tls_config: TLSConfig,
    pub grpc_tls_config: TLSConfig,
    pub register_instrumentation: bool,
    pub metrics_cache_duration: GoDuration,
    pub report_grpc_codes_in_instrumentation_label_enabled: bool,
    pub graceful_shutdown_timeout: GoDuration,
    pub http_server_read_timeout: GoDuration,
    pub http_server_read_header_timeout: GoDuration,
    pub http_server_write_timeout: GoDuration,
    pub http_server_idle_timeout: GoDuration,
    pub http_log_closed_connections_without_response_enabled: bool,
    pub grpc_server_max_recv_msg_size: i64,
    pub grpc_server_max_send_msg_size: i64,
    pub grpc_server_max_concurrent_streams: u64,
    pub grpc_server_max_connection_idle: GoDuration,
    pub grpc_server_max_connection_age: GoDuration,
    pub grpc_server_max_connection_age_grace: GoDuration,
    pub grpc_server_keepalive_time: GoDuration,
    pub grpc_server_keepalive_timeout: GoDuration,
    pub grpc_server_min_time_between_pings: GoDuration,
    pub grpc_server_ping_without_stream_allowed: bool,
    pub grpc_server_num_workers: i64,
    pub grpc_server_stats_tracking_enabled: bool,
    pub grpc_server_recv_buffer_pools_enabled: bool,
    pub grpc_server_read_buffer_size: i64,
    pub grpc_server_write_buffer_size: i64,
    pub log_format: String,
    pub log_level: LogLevel,
    pub log_source_ips_enabled: bool,
    pub log_source_ips_full: bool,
    pub log_source_ips_header: String,
    pub log_source_ips_regex: String,
    pub log_request_headers: bool,
    pub log_request_at_info_level_enabled: bool,
    pub log_request_exclude_headers_list: String,
    pub trace_request_headers: bool,
    pub trace_request_exclude_headers_list: String,
    pub http_path_prefix: String,
    pub cluster_validation: ServerClusterValidationConfig,
    pub create_new_traces: bool,
    pub enable_open_metrics_text_created_samples: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            http_listen_network: "tcp".to_string(),
            http_listen_address: String::new(),
            // Mimir overrides dskit server's default 80 -> 8080.
            http_listen_port: 8080,
            http_listen_conn_limit: 0,
            grpc_listen_network: "tcp".to_string(),
            grpc_listen_address: String::new(),
            grpc_listen_port: 9095,
            grpc_listen_conn_limit: 0,
            grpc_collect_max_streams_by_conn: true,
            proxy_protocol_enabled: false,
            tls_cipher_suites: String::new(),
            tls_min_version: String::new(),
            tls_curve_preferences: String::new(),
            http_tls_config: TLSConfig::default(),
            grpc_tls_config: TLSConfig::default(),
            register_instrumentation: true,
            metrics_cache_duration: GoDuration::ZERO,
            // Mimir overrides to true.
            report_grpc_codes_in_instrumentation_label_enabled: true,
            graceful_shutdown_timeout: GoDuration::from_secs(30),
            http_server_read_timeout: GoDuration::from_secs(30),
            http_server_read_header_timeout: GoDuration::ZERO,
            // Mimir overrides to 2m.
            http_server_write_timeout: GoDuration::from_mins(2),
            http_server_idle_timeout: GoDuration::from_mins(2),
            http_log_closed_connections_without_response_enabled: false,
            // Mimir overrides to 100 MiB.
            grpc_server_max_recv_msg_size: 100 * 1024 * 1024,
            grpc_server_max_send_msg_size: 100 * 1024 * 1024,
            grpc_server_max_concurrent_streams: 100,
            grpc_server_max_connection_idle: GoDuration(i64::MAX),
            grpc_server_max_connection_age: GoDuration(i64::MAX),
            grpc_server_max_connection_age_grace: GoDuration(i64::MAX),
            grpc_server_keepalive_time: GoDuration::from_hours(2),
            grpc_server_keepalive_timeout: GoDuration::from_secs(20),
            // Mimir overrides to 10s and true.
            grpc_server_min_time_between_pings: GoDuration::from_secs(10),
            grpc_server_ping_without_stream_allowed: true,
            // Mimir overrides to 100.
            grpc_server_num_workers: 100,
            grpc_server_stats_tracking_enabled: true,
            grpc_server_recv_buffer_pools_enabled: false,
            grpc_server_read_buffer_size: 32 * 1024,
            grpc_server_write_buffer_size: 32 * 1024,
            log_format: "logfmt".to_string(),
            log_level: LogLevel::Info,
            log_source_ips_enabled: false,
            log_source_ips_full: false,
            log_source_ips_header: String::new(),
            log_source_ips_regex: String::new(),
            log_request_headers: false,
            log_request_at_info_level_enabled: false,
            log_request_exclude_headers_list: String::new(),
            trace_request_headers: false,
            trace_request_exclude_headers_list: String::new(),
            http_path_prefix: String::new(),
            cluster_validation: ServerClusterValidationConfig::default(),
            create_new_traces: false,
            enable_open_metrics_text_created_samples: false,
        }
    }
}

impl ServerConfig {
    pub fn register_flags_into<T: 'static>(
        &self,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        f.string_var("server.http-listen-address", &self.http_listen_address, "HTTP server listen address.", move |t| &get(t).http_listen_address, move |t| &mut get_mut(t).http_listen_address);
        f.string_var("server.http-listen-network", &self.http_listen_network, "HTTP server listen network, default tcp", move |t| &get(t).http_listen_network, move |t| &mut get_mut(t).http_listen_network);
        f.string_var("server.tls-cipher-suites", &self.tls_cipher_suites, "Comma-separated list of cipher suites to use.", move |t| &get(t).tls_cipher_suites, move |t| &mut get_mut(t).tls_cipher_suites);
        f.string_var("server.tls-min-version", &self.tls_min_version, "Minimum TLS version to use.", move |t| &get(t).tls_min_version, move |t| &mut get_mut(t).tls_min_version);
        f.string_var("server.tls-curve-preferences", &self.tls_curve_preferences, "Comma-separated list of TLS curve/group preferences.", move |t| &get(t).tls_curve_preferences, move |t| &mut get_mut(t).tls_curve_preferences);
        self.http_tls_config
            .register_flags_into("server.http-tls-", f, move |t| &get(t).http_tls_config, move |t| &mut get_mut(t).http_tls_config);
        self.grpc_tls_config
            .register_flags_into("server.grpc-tls-", f, move |t| &get(t).grpc_tls_config, move |t| &mut get_mut(t).grpc_tls_config);
        f.i64_var("server.http-listen-port", self.http_listen_port, "HTTP server listen port.", move |t| &get(t).http_listen_port, move |t| &mut get_mut(t).http_listen_port);
        f.i64_var("server.http-conn-limit", self.http_listen_conn_limit, "Maximum number of simultaneous http connections, <=0 to disable", move |t| &get(t).http_listen_conn_limit, move |t| &mut get_mut(t).http_listen_conn_limit);
        f.string_var("server.grpc-listen-network", &self.grpc_listen_network, "gRPC server listen network", move |t| &get(t).grpc_listen_network, move |t| &mut get_mut(t).grpc_listen_network);
        f.string_var("server.grpc-listen-address", &self.grpc_listen_address, "gRPC server listen address.", move |t| &get(t).grpc_listen_address, move |t| &mut get_mut(t).grpc_listen_address);
        f.i64_var("server.grpc-listen-port", self.grpc_listen_port, "gRPC server listen port.", move |t| &get(t).grpc_listen_port, move |t| &mut get_mut(t).grpc_listen_port);
        f.i64_var("server.grpc-conn-limit", self.grpc_listen_conn_limit, "Maximum number of simultaneous grpc connections, <=0 to disable", move |t| &get(t).grpc_listen_conn_limit, move |t| &mut get_mut(t).grpc_listen_conn_limit);
        f.bool_var("server.grpc-collect-max-streams-by-conn", self.grpc_collect_max_streams_by_conn, "If true, the max streams by connection gauge will be collected.", move |t| &get(t).grpc_collect_max_streams_by_conn, move |t| &mut get_mut(t).grpc_collect_max_streams_by_conn);
        f.bool_var("server.proxy-protocol-enabled", self.proxy_protocol_enabled, "Enable PROXY protocol for HTTP and gRPC servers.", move |t| &get(t).proxy_protocol_enabled, move |t| &mut get_mut(t).proxy_protocol_enabled);
        f.bool_var("server.register-instrumentation", self.register_instrumentation, "Register the instrumentation handlers (/metrics etc).", move |t| &get(t).register_instrumentation, move |t| &mut get_mut(t).register_instrumentation);
        f.go_duration_var("server.metrics-cache-duration", self.metrics_cache_duration, "Cache successful /metrics responses for this duration.", move |t| &get(t).metrics_cache_duration, move |t| &mut get_mut(t).metrics_cache_duration);
        f.bool_var("server.report-grpc-codes-in-instrumentation-label-enabled", self.report_grpc_codes_in_instrumentation_label_enabled, "Report gRPC statuses in instrumentation labels with their string representations.", move |t| &get(t).report_grpc_codes_in_instrumentation_label_enabled, move |t| &mut get_mut(t).report_grpc_codes_in_instrumentation_label_enabled);
        f.go_duration_var("server.graceful-shutdown-timeout", self.graceful_shutdown_timeout, "Timeout for graceful shutdowns", move |t| &get(t).graceful_shutdown_timeout, move |t| &mut get_mut(t).graceful_shutdown_timeout);
        f.go_duration_var("server.http-read-timeout", self.http_server_read_timeout, "Read timeout for entire HTTP request.", move |t| &get(t).http_server_read_timeout, move |t| &mut get_mut(t).http_server_read_timeout);
        f.go_duration_var("server.http-read-header-timeout", self.http_server_read_header_timeout, "Read timeout for HTTP request headers.", move |t| &get(t).http_server_read_header_timeout, move |t| &mut get_mut(t).http_server_read_header_timeout);
        f.go_duration_var("server.http-write-timeout", self.http_server_write_timeout, "Write timeout for HTTP server", move |t| &get(t).http_server_write_timeout, move |t| &mut get_mut(t).http_server_write_timeout);
        f.go_duration_var("server.http-idle-timeout", self.http_server_idle_timeout, "Idle timeout for HTTP server", move |t| &get(t).http_server_idle_timeout, move |t| &mut get_mut(t).http_server_idle_timeout);
        f.bool_var("server.http-log-closed-connections-without-response-enabled", self.http_log_closed_connections_without_response_enabled, "Log closed connections that did not receive any response.", move |t| &get(t).http_log_closed_connections_without_response_enabled, move |t| &mut get_mut(t).http_log_closed_connections_without_response_enabled);
        f.i64_var("server.grpc-max-recv-msg-size-bytes", self.grpc_server_max_recv_msg_size, "Limit on the size of a gRPC message this server can receive (bytes).", move |t| &get(t).grpc_server_max_recv_msg_size, move |t| &mut get_mut(t).grpc_server_max_recv_msg_size);
        f.i64_var("server.grpc-max-send-msg-size-bytes", self.grpc_server_max_send_msg_size, "Limit on the size of a gRPC message this server can send (bytes).", move |t| &get(t).grpc_server_max_send_msg_size, move |t| &mut get_mut(t).grpc_server_max_send_msg_size);
        f.u64_var("server.grpc-max-concurrent-streams", self.grpc_server_max_concurrent_streams, "Limit on the number of concurrent streams for gRPC calls per client connection", move |t| &get(t).grpc_server_max_concurrent_streams, move |t| &mut get_mut(t).grpc_server_max_concurrent_streams);
        f.go_duration_var("server.grpc.keepalive.max-connection-idle", self.grpc_server_max_connection_idle, "The duration after which an idle connection should be closed.", move |t| &get(t).grpc_server_max_connection_idle, move |t| &mut get_mut(t).grpc_server_max_connection_idle);
        f.go_duration_var("server.grpc.keepalive.max-connection-age", self.grpc_server_max_connection_age, "The duration for the maximum amount of time a connection may exist before it will be closed.", move |t| &get(t).grpc_server_max_connection_age, move |t| &mut get_mut(t).grpc_server_max_connection_age);
        f.go_duration_var("server.grpc.keepalive.max-connection-age-grace", self.grpc_server_max_connection_age_grace, "An additive period after max-connection-age after which the connection will be forcibly closed.", move |t| &get(t).grpc_server_max_connection_age_grace, move |t| &mut get_mut(t).grpc_server_max_connection_age_grace);
        f.go_duration_var("server.grpc.keepalive.time", self.grpc_server_keepalive_time, "Duration after which a keepalive probe is sent in case of no activity.", move |t| &get(t).grpc_server_keepalive_time, move |t| &mut get_mut(t).grpc_server_keepalive_time);
        f.go_duration_var("server.grpc.keepalive.timeout", self.grpc_server_keepalive_timeout, "After having pinged for keepalive check, the duration after which an idle connection should be closed.", move |t| &get(t).grpc_server_keepalive_timeout, move |t| &mut get_mut(t).grpc_server_keepalive_timeout);
        f.go_duration_var("server.grpc.keepalive.min-time-between-pings", self.grpc_server_min_time_between_pings, "Minimum amount of time a client should wait before sending a keepalive ping.", move |t| &get(t).grpc_server_min_time_between_pings, move |t| &mut get_mut(t).grpc_server_min_time_between_pings);
        f.bool_var("server.grpc.keepalive.ping-without-stream-allowed", self.grpc_server_ping_without_stream_allowed, "If true, server allows keepalive pings even when there are no active streams.", move |t| &get(t).grpc_server_ping_without_stream_allowed, move |t| &mut get_mut(t).grpc_server_ping_without_stream_allowed);
        f.bool_var("server.grpc.stats-tracking-enabled", self.grpc_server_stats_tracking_enabled, "If true, request/response bytes and inflight requests metrics will be tracked.", move |t| &get(t).grpc_server_stats_tracking_enabled, move |t| &mut get_mut(t).grpc_server_stats_tracking_enabled);
        f.bool_var("server.grpc.recv-buffer-pools-enabled", self.grpc_server_recv_buffer_pools_enabled, "Deprecated option, has no effect.", move |t| &get(t).grpc_server_recv_buffer_pools_enabled, move |t| &mut get_mut(t).grpc_server_recv_buffer_pools_enabled);
        f.i64_var("server.grpc.num-workers", self.grpc_server_num_workers, "Configures the amount of GRPC server workers used to serve the requests.", move |t| &get(t).grpc_server_num_workers, move |t| &mut get_mut(t).grpc_server_num_workers);
        f.i64_var("server.grpc.read-buffer-size-bytes", self.grpc_server_read_buffer_size, "Size of the read buffer for each gRPC connection (bytes).", move |t| &get(t).grpc_server_read_buffer_size, move |t| &mut get_mut(t).grpc_server_read_buffer_size);
        f.i64_var("server.grpc.write-buffer-size-bytes", self.grpc_server_write_buffer_size, "Size of the write buffer for each gRPC connection (bytes).", move |t| &get(t).grpc_server_write_buffer_size, move |t| &mut get_mut(t).grpc_server_write_buffer_size);
        f.string_var("server.path-prefix", &self.http_path_prefix, "Base path to serve all API routes from (e.g. /v1/)", move |t| &get(t).http_path_prefix, move |t| &mut get_mut(t).http_path_prefix);
        f.string_var("log.format", &self.log_format, "Output log messages in the given format. Valid formats: [logfmt, json]", move |t| &get(t).log_format, move |t| &mut get_mut(t).log_format);
        f.log_level_var("log.level", self.log_level, "Only log messages with the given severity or above. Valid levels: [debug, info, warn, error]", move |t| &get(t).log_level, move |t| &mut get_mut(t).log_level);
        f.bool_var("server.log-source-ips-enabled", self.log_source_ips_enabled, "Optionally log the source IPs.", move |t| &get(t).log_source_ips_enabled, move |t| &mut get_mut(t).log_source_ips_enabled);
        f.bool_var("server.log-source-ips-full", self.log_source_ips_full, "Log all source IPs instead of only the originating one.", move |t| &get(t).log_source_ips_full, move |t| &mut get_mut(t).log_source_ips_full);
        f.string_var("server.log-source-ips-header", &self.log_source_ips_header, "Header field storing the source IPs.", move |t| &get(t).log_source_ips_header, move |t| &mut get_mut(t).log_source_ips_header);
        f.string_var("server.log-source-ips-regex", &self.log_source_ips_regex, "Regex for matching the source IPs.", move |t| &get(t).log_source_ips_regex, move |t| &mut get_mut(t).log_source_ips_regex);
        f.bool_var("server.log-request-headers", self.log_request_headers, "Optionally log request headers.", move |t| &get(t).log_request_headers, move |t| &mut get_mut(t).log_request_headers);
        f.bool_var("server.log-request-at-info-level-enabled", self.log_request_at_info_level_enabled, "Optionally log requests at info level instead of debug level.", move |t| &get(t).log_request_at_info_level_enabled, move |t| &mut get_mut(t).log_request_at_info_level_enabled);
        f.string_var("server.log-request-headers-exclude-list", &self.log_request_exclude_headers_list, "Comma separated list of headers to exclude from request logging.", move |t| &get(t).log_request_exclude_headers_list, move |t| &mut get_mut(t).log_request_exclude_headers_list);
        f.bool_var("server.trace-request-headers", self.trace_request_headers, "Optionally add request headers to traces.", move |t| &get(t).trace_request_headers, move |t| &mut get_mut(t).trace_request_headers);
        f.string_var("server.trace-request-headers-exclude-list", &self.trace_request_exclude_headers_list, "Comma separated list of headers to exclude from tracing.", move |t| &get(t).trace_request_exclude_headers_list, move |t| &mut get_mut(t).trace_request_exclude_headers_list);
        self.cluster_validation
            .register_flags_into("server.cluster-validation.", f, move |t| &get(t).cluster_validation, move |t| &mut get_mut(t).cluster_validation);
        f.bool_var("server.create-new-traces", self.create_new_traces, "Create new traces instead of continuing existing ones.", move |t| &get(t).create_new_traces, move |t| &mut get_mut(t).create_new_traces);
        f.bool_var("server.enable-open-metrics-text-created-samples", self.enable_open_metrics_text_created_samples, "Enable OpenMetrics text created samples.", move |t| &get(t).enable_open_metrics_text_created_samples, move |t| &mut get_mut(t).enable_open_metrics_text_created_samples);
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.enable_open_metrics_text_created_samples && !self.register_instrumentation {
            return Err(ConfigError::Validation(
                "server.enable-open-metrics-text-created-samples can only be used if server.register-instrumentation is set to true".to_string(),
            ));
        }
        self.cluster_validation.validate()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ZoneAwareRoutingConfig {
    pub enabled: bool,
    pub instance_availability_zone: String,
    pub role: String,
}

impl Default for ZoneAwareRoutingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            instance_availability_zone: String::new(),
            role: "member".to_string(),
        }
    }
}

impl ZoneAwareRoutingConfig {
    pub fn register_flags_into<T: 'static>(
        &self,
        prefix: &str,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        f.bool_var(&format!("{prefix}enabled"), self.enabled, "Enable zone-aware routing for memberlist gossip.", move |t| &get(t).enabled, move |t| &mut get_mut(t).enabled);
        f.string_var(&format!("{prefix}instance-availability-zone"), &self.instance_availability_zone, "Availability zone where this node is running.", move |t| &get(t).instance_availability_zone, move |t| &mut get_mut(t).instance_availability_zone);
        f.string_var(&format!("{prefix}role"), &self.role, "Role of this node in the cluster. Valid values: member, bridge.", move |t| &get(t).role, move |t| &mut get_mut(t).role);
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if !self.enabled {
            return Ok(());
        }
        if self.instance_availability_zone.is_empty() {
            return Err(ConfigError::Validation(
                "zone-aware routing is enabled but zone is not set".to_string(),
            ));
        }
        if self.instance_availability_zone.len() > 16 {
            return Err(ConfigError::Validation(format!(
                "zone name too long: {} bytes (max 16)",
                self.instance_availability_zone.len()
            )));
        }
        if self.role != "member" && self.role != "bridge" {
            return Err(ConfigError::Validation(format!(
                "invalid role: {} (valid values: member, bridge)",
                self.role
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PropagationDelayTrackerConfig {
    pub enabled: bool,
    pub beacon_interval: GoDuration,
    pub beacon_lifetime: GoDuration,
    pub log_beacons_latency_longer_than: GoDuration,
}

impl Default for PropagationDelayTrackerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            beacon_interval: GoDuration::from_mins(1),
            beacon_lifetime: GoDuration::from_mins(10),
            log_beacons_latency_longer_than: GoDuration::ZERO,
        }
    }
}

impl PropagationDelayTrackerConfig {
    pub fn register_flags_into<T: 'static>(
        &self,
        prefix: &str,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        f.bool_var(&format!("{prefix}enabled"), self.enabled, "Enable the propagation delay tracker.", move |t| &get(t).enabled, move |t| &mut get_mut(t).enabled);
        f.go_duration_var(&format!("{prefix}beacon-interval"), self.beacon_interval, "How often to publish beacons for propagation tracking.", move |t| &get(t).beacon_interval, move |t| &mut get_mut(t).beacon_interval);
        f.go_duration_var(&format!("{prefix}beacon-lifetime"), self.beacon_lifetime, "How long a beacon lives before being garbage collected.", move |t| &get(t).beacon_lifetime, move |t| &mut get_mut(t).beacon_lifetime);
        f.go_duration_var(&format!("{prefix}log-beacons-latency-longer-than"), self.log_beacons_latency_longer_than, "Log warning when beacon propagation delay exceeds this threshold.", move |t| &get(t).log_beacons_latency_longer_than, move |t| &mut get_mut(t).log_beacons_latency_longer_than);
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if !self.enabled {
            return Ok(());
        }
        if self.beacon_interval.0 <= 0 {
            return Err(ConfigError::Validation(
                "propagation delay tracker beacon interval must be greater than 0".to_string(),
            ));
        }
        if self.beacon_lifetime.0 <= 0 {
            return Err(ConfigError::Validation(
                "propagation delay tracker beacon lifetime must be greater than 0".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MemberlistConfig {
    pub node_name: String,
    pub randomize_node_name: bool,
    pub stream_timeout: GoDuration,
    pub retransmit_factor: i64,
    pub pull_push_interval: GoDuration,
    pub gossip_interval: GoDuration,
    pub gossip_nodes: i64,
    pub gossip_to_dead_nodes_time: GoDuration,
    pub dead_node_reclaim_time: GoDuration,
    pub compression_enabled: bool,
    pub notify_interval: GoDuration,
    pub received_messages_queue_size: i64,
    pub processed_messages_queue_size: i64,
    pub compression_algorithm: String,
    pub advertise_addr: String,
    pub advertise_port: i64,
    pub cluster_label: String,
    pub cluster_label_verification_disabled: bool,
    pub join_members: StringSliceCSVMulti,
    pub min_join_backoff: GoDuration,
    pub max_join_backoff: GoDuration,
    pub max_join_retries: i64,
    pub abort_if_cluster_fast_join_fails: bool,
    pub abort_if_cluster_fast_join_fails_min_nodes: i64,
    pub abort_if_cluster_join_fails: bool,
    pub rejoin_interval: GoDuration,
    pub rejoin_seed_nodes: StringSliceCSVMulti,
    pub left_ingesters_timeout: GoDuration,
    pub obsolete_entries_timeout: GoDuration,
    pub leave_timeout: GoDuration,
    pub broadcast_timeout_for_local_updates_on_shutdown: GoDuration,
    pub message_history_buffer_bytes: i64,
    pub watch_prefix_buffer_size: i64,
    pub cas_retry_min_backoff: GoDuration,
    pub cas_retry_max_backoff: GoDuration,
    pub local_state_cache_enabled: bool,
    pub bind_addr: Vec<String>,
    pub bind_port: i64,
    pub packet_dial_timeout: GoDuration,
    pub packet_write_timeout: GoDuration,
    pub max_concurrent_writes: i64,
    pub acquire_writer_timeout: GoDuration,
    pub tls_enabled: bool,
    pub tls_cert_path: String,
    pub tls_key_path: String,
    pub tls_ca_path: String,
    pub tls_server_name: String,
    pub tls_insecure_skip_verify: bool,
    pub tls_cipher_suites: String,
    pub tls_min_version: String,
    pub zone_aware_routing: ZoneAwareRoutingConfig,
    pub propagation_delay_tracker: PropagationDelayTrackerConfig,
}

impl Default for MemberlistConfig {
    fn default() -> Self {
        Self {
            node_name: String::new(),
            randomize_node_name: true,
            stream_timeout: GoDuration::from_secs(2),
            retransmit_factor: 4,
            pull_push_interval: GoDuration::from_secs(30),
            gossip_interval: GoDuration::from_millis(200),
            gossip_nodes: 3,
            gossip_to_dead_nodes_time: GoDuration::from_secs(30),
            dead_node_reclaim_time: GoDuration::ZERO,
            compression_enabled: true,
            notify_interval: GoDuration::ZERO,
            received_messages_queue_size: 1024,
            processed_messages_queue_size: 1024,
            compression_algorithm: "lzw".to_string(),
            advertise_addr: String::new(),
            advertise_port: 7946,
            cluster_label: String::new(),
            cluster_label_verification_disabled: false,
            join_members: StringSliceCSVMulti::default(),
            min_join_backoff: GoDuration::from_secs(1),
            max_join_backoff: GoDuration::from_mins(1),
            max_join_retries: 10,
            abort_if_cluster_fast_join_fails: false,
            abort_if_cluster_fast_join_fails_min_nodes: 1,
            abort_if_cluster_join_fails: false,
            rejoin_interval: GoDuration::ZERO,
            rejoin_seed_nodes: StringSliceCSVMulti::default(),
            left_ingesters_timeout: GoDuration::from_mins(5),
            obsolete_entries_timeout: GoDuration::from_secs(30),
            leave_timeout: GoDuration::from_secs(20),
            broadcast_timeout_for_local_updates_on_shutdown: GoDuration::from_secs(10),
            message_history_buffer_bytes: 0,
            watch_prefix_buffer_size: 128,
            cas_retry_min_backoff: GoDuration::ZERO,
            cas_retry_max_backoff: GoDuration::from_secs(10),
            local_state_cache_enabled: false,
            bind_addr: Vec::new(),
            bind_port: 7946,
            // Mimir overrides these 4 transport defaults.
            packet_dial_timeout: GoDuration::from_millis(500),
            packet_write_timeout: GoDuration::from_millis(500),
            max_concurrent_writes: 5,
            acquire_writer_timeout: GoDuration::from_secs(1),
            tls_enabled: false,
            tls_cert_path: String::new(),
            tls_key_path: String::new(),
            tls_ca_path: String::new(),
            tls_server_name: String::new(),
            tls_insecure_skip_verify: false,
            tls_cipher_suites: String::new(),
            tls_min_version: String::new(),
            zone_aware_routing: ZoneAwareRoutingConfig::default(),
            propagation_delay_tracker: PropagationDelayTrackerConfig::default(),
        }
    }
}

impl MemberlistConfig {
    pub fn register_flags_into<T: 'static>(
        &self,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        f.string_var("memberlist.nodename", &self.node_name, "Name of the node in memberlist cluster. Defaults to hostname.", move |t| &get(t).node_name, move |t| &mut get_mut(t).node_name);
        f.bool_var("memberlist.randomize-node-name", self.randomize_node_name, "Add random suffix to the node name.", move |t| &get(t).randomize_node_name, move |t| &mut get_mut(t).randomize_node_name);
        f.go_duration_var("memberlist.stream-timeout", self.stream_timeout, "The timeout for establishing a connection with a remote node.", move |t| &get(t).stream_timeout, move |t| &mut get_mut(t).stream_timeout);
        f.i64_var("memberlist.retransmit-factor", self.retransmit_factor, "Multiplication factor used when sending out messages.", move |t| &get(t).retransmit_factor, move |t| &mut get_mut(t).retransmit_factor);
        f.string_slice_csv_multi_var("memberlist.join", &self.join_members, "Other cluster members to join.", move |t| &get(t).join_members, move |t| &mut get_mut(t).join_members);
        f.go_duration_var("memberlist.min-join-backoff", self.min_join_backoff, "Min backoff duration to join other cluster members.", move |t| &get(t).min_join_backoff, move |t| &mut get_mut(t).min_join_backoff);
        f.go_duration_var("memberlist.max-join-backoff", self.max_join_backoff, "Max backoff duration to join other cluster members.", move |t| &get(t).max_join_backoff, move |t| &mut get_mut(t).max_join_backoff);
        f.i64_var("memberlist.max-join-retries", self.max_join_retries, "Max number of retries to join other cluster members.", move |t| &get(t).max_join_retries, move |t| &mut get_mut(t).max_join_retries);
        f.bool_var("memberlist.abort-if-fast-join-fails", self.abort_if_cluster_fast_join_fails, "Abort if this node fails the fast memberlist cluster joining procedure at startup.", move |t| &get(t).abort_if_cluster_fast_join_fails, move |t| &mut get_mut(t).abort_if_cluster_fast_join_fails);
        f.i64_var("memberlist.abort-if-fast-join-fails-min-nodes", self.abort_if_cluster_fast_join_fails_min_nodes, "Minimum number of seed nodes that must be successfully joined during fast-join.", move |t| &get(t).abort_if_cluster_fast_join_fails_min_nodes, move |t| &mut get_mut(t).abort_if_cluster_fast_join_fails_min_nodes);
        f.bool_var("memberlist.abort-if-join-fails", self.abort_if_cluster_join_fails, "Abort if this node fails to join memberlist cluster at startup.", move |t| &get(t).abort_if_cluster_join_fails, move |t| &mut get_mut(t).abort_if_cluster_join_fails);
        f.go_duration_var("memberlist.rejoin-interval", self.rejoin_interval, "If not 0, how often to rejoin the cluster.", move |t| &get(t).rejoin_interval, move |t| &mut get_mut(t).rejoin_interval);
        f.string_slice_csv_multi_var("memberlist.rejoin-seed-nodes", &self.rejoin_seed_nodes, "Seed nodes to use for periodic rejoin.", move |t| &get(t).rejoin_seed_nodes, move |t| &mut get_mut(t).rejoin_seed_nodes);
        f.go_duration_var("memberlist.left-ingesters-timeout", self.left_ingesters_timeout, "How long to keep LEFT ingesters in the ring.", move |t| &get(t).left_ingesters_timeout, move |t| &mut get_mut(t).left_ingesters_timeout);
        f.go_duration_var("memberlist.obsolete-entries-timeout", self.obsolete_entries_timeout, "How long to keep obsolete entries in the KV store.", move |t| &get(t).obsolete_entries_timeout, move |t| &mut get_mut(t).obsolete_entries_timeout);
        f.go_duration_var("memberlist.leave-timeout", self.leave_timeout, "Timeout for leaving memberlist cluster.", move |t| &get(t).leave_timeout, move |t| &mut get_mut(t).leave_timeout);
        f.go_duration_var("memberlist.gossip-interval", self.gossip_interval, "How often to gossip.", move |t| &get(t).gossip_interval, move |t| &mut get_mut(t).gossip_interval);
        f.i64_var("memberlist.gossip-nodes", self.gossip_nodes, "How many nodes to gossip to.", move |t| &get(t).gossip_nodes, move |t| &mut get_mut(t).gossip_nodes);
        f.go_duration_var("memberlist.pullpush-interval", self.pull_push_interval, "How often to use pull/push sync.", move |t| &get(t).pull_push_interval, move |t| &mut get_mut(t).pull_push_interval);
        f.go_duration_var("memberlist.gossip-to-dead-nodes-time", self.gossip_to_dead_nodes_time, "How long to keep gossiping to dead nodes.", move |t| &get(t).gossip_to_dead_nodes_time, move |t| &mut get_mut(t).gossip_to_dead_nodes_time);
        f.go_duration_var("memberlist.dead-node-reclaim-time", self.dead_node_reclaim_time, "How soon can dead node's name be reclaimed with new address.", move |t| &get(t).dead_node_reclaim_time, move |t| &mut get_mut(t).dead_node_reclaim_time);
        f.i64_var("memberlist.message-history-buffer-bytes", self.message_history_buffer_bytes, "Space used for keeping received and sent messages in memory.", move |t| &get(t).message_history_buffer_bytes, move |t| &mut get_mut(t).message_history_buffer_bytes);
        f.bool_var("memberlist.local-state-cache-enabled", self.local_state_cache_enabled, "Cache the serialized form of each key.", move |t| &get(t).local_state_cache_enabled, move |t| &mut get_mut(t).local_state_cache_enabled);
        f.bool_var("memberlist.compression-enabled", self.compression_enabled, "Enable message compression.", move |t| &get(t).compression_enabled, move |t| &mut get_mut(t).compression_enabled);
        f.string_var("memberlist.compression-algorithm", &self.compression_algorithm, "Compression algorithm used for outgoing messages.", move |t| &get(t).compression_algorithm, move |t| &mut get_mut(t).compression_algorithm);
        f.go_duration_var("memberlist.notify-interval", self.notify_interval, "How frequently to notify watchers when a key changes.", move |t| &get(t).notify_interval, move |t| &mut get_mut(t).notify_interval);
        f.i64_var("memberlist.received-messages-queue-size", self.received_messages_queue_size, "Size of the internal queue for messages received from other nodes.", move |t| &get(t).received_messages_queue_size, move |t| &mut get_mut(t).received_messages_queue_size);
        f.i64_var("memberlist.processed-messages-queue-size", self.processed_messages_queue_size, "Size of the per-key internal queue for processing messages.", move |t| &get(t).processed_messages_queue_size, move |t| &mut get_mut(t).processed_messages_queue_size);
        f.string_var("memberlist.advertise-addr", &self.advertise_addr, "Gossip address to advertise to other members in the cluster.", move |t| &get(t).advertise_addr, move |t| &mut get_mut(t).advertise_addr);
        f.i64_var("memberlist.advertise-port", self.advertise_port, "Gossip port to advertise to other members in the cluster.", move |t| &get(t).advertise_port, move |t| &mut get_mut(t).advertise_port);
        f.string_var("memberlist.cluster-label", &self.cluster_label, "Optional cluster label string to include in outbound packets.", move |t| &get(t).cluster_label, move |t| &mut get_mut(t).cluster_label);
        f.bool_var("memberlist.cluster-label-verification-disabled", self.cluster_label_verification_disabled, "Disable verification of inbound cluster label.", move |t| &get(t).cluster_label_verification_disabled, move |t| &mut get_mut(t).cluster_label_verification_disabled);
        f.go_duration_var("memberlist.broadcast-timeout-for-local-updates-on-shutdown", self.broadcast_timeout_for_local_updates_on_shutdown, "Timeout for broadcasting local updates when shutting down.", move |t| &get(t).broadcast_timeout_for_local_updates_on_shutdown, move |t| &mut get_mut(t).broadcast_timeout_for_local_updates_on_shutdown);
        f.i64_var("memberlist.watch-prefix-buffer-size", self.watch_prefix_buffer_size, "Size of the buffered channel for WatchPrefix.", move |t| &get(t).watch_prefix_buffer_size, move |t| &mut get_mut(t).watch_prefix_buffer_size);
        f.go_duration_var("memberlist.cas-retry-min-backoff", self.cas_retry_min_backoff, "Minimum delay between CAS retries after a version mismatch.", move |t| &get(t).cas_retry_min_backoff, move |t| &mut get_mut(t).cas_retry_min_backoff);
        f.go_duration_var("memberlist.cas-retry-max-backoff", self.cas_retry_max_backoff, "Maximum delay between CAS retries after a version mismatch.", move |t| &get(t).cas_retry_max_backoff, move |t| &mut get_mut(t).cas_retry_max_backoff);
        f.string_slice_var("memberlist.bind-addr", &self.bind_addr, "IP address to listen on for gossip messages.", move |t| &get(t).bind_addr, move |t| &mut get_mut(t).bind_addr);
        f.i64_var("memberlist.bind-port", self.bind_port, "Port to listen on for gossip messages.", move |t| &get(t).bind_port, move |t| &mut get_mut(t).bind_port);
        f.go_duration_var("memberlist.packet-dial-timeout", self.packet_dial_timeout, "Timeout used when connecting to other nodes to send packet.", move |t| &get(t).packet_dial_timeout, move |t| &mut get_mut(t).packet_dial_timeout);
        f.go_duration_var("memberlist.packet-write-timeout", self.packet_write_timeout, "Timeout for writing 'packet' data.", move |t| &get(t).packet_write_timeout, move |t| &mut get_mut(t).packet_write_timeout);
        f.i64_var("memberlist.max-concurrent-writes", self.max_concurrent_writes, "Maximum number of concurrent writes to other nodes.", move |t| &get(t).max_concurrent_writes, move |t| &mut get_mut(t).max_concurrent_writes);
        f.go_duration_var("memberlist.acquire-writer-timeout", self.acquire_writer_timeout, "Timeout for acquiring one of the concurrent write slots.", move |t| &get(t).acquire_writer_timeout, move |t| &mut get_mut(t).acquire_writer_timeout);
        f.bool_var("memberlist.tls-enabled", self.tls_enabled, "Enable TLS on the memberlist transport layer.", move |t| &get(t).tls_enabled, move |t| &mut get_mut(t).tls_enabled);
        f.string_var("memberlist.tls-cert-path", &self.tls_cert_path, "Path to TLS client certificate.", move |t| &get(t).tls_cert_path, move |t| &mut get_mut(t).tls_cert_path);
        f.string_var("memberlist.tls-key-path", &self.tls_key_path, "Path to TLS client key.", move |t| &get(t).tls_key_path, move |t| &mut get_mut(t).tls_key_path);
        f.string_var("memberlist.tls-ca-path", &self.tls_ca_path, "Path to TLS CA certificate.", move |t| &get(t).tls_ca_path, move |t| &mut get_mut(t).tls_ca_path);
        f.string_var("memberlist.tls-server-name", &self.tls_server_name, "TLS server name.", move |t| &get(t).tls_server_name, move |t| &mut get_mut(t).tls_server_name);
        f.bool_var("memberlist.tls-insecure-skip-verify", self.tls_insecure_skip_verify, "Skip TLS certificate verification.", move |t| &get(t).tls_insecure_skip_verify, move |t| &mut get_mut(t).tls_insecure_skip_verify);
        f.string_var("memberlist.tls-cipher-suites", &self.tls_cipher_suites, "TLS cipher suites.", move |t| &get(t).tls_cipher_suites, move |t| &mut get_mut(t).tls_cipher_suites);
        f.string_var("memberlist.tls-min-version", &self.tls_min_version, "Minimum TLS version.", move |t| &get(t).tls_min_version, move |t| &mut get_mut(t).tls_min_version);
        f.ignored_flag("memberlist.transport-debug", true, "Log debug transport messages.");
        self.zone_aware_routing
            .register_flags_into("memberlist.zone-aware-routing.", f, move |t| &get(t).zone_aware_routing, move |t| &mut get_mut(t).zone_aware_routing);
        self.propagation_delay_tracker
            .register_flags_into("memberlist.propagation-delay-tracker.", f, move |t| &get(t).propagation_delay_tracker, move |t| &mut get_mut(t).propagation_delay_tracker);
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.received_messages_queue_size <= 0 {
            return Err(ConfigError::Validation(
                "memberlist received messages queue size must be greater than 0".to_string(),
            ));
        }
        if self.processed_messages_queue_size <= 0 {
            return Err(ConfigError::Validation(
                "memberlist processed messages queue size must be greater than 0".to_string(),
            ));
        }
        if !self.compression_algorithm.is_empty()
            && self.compression_algorithm != "lzw"
            && self.compression_algorithm != "snappy"
        {
            return Err(ConfigError::Validation(format!(
                "memberlist compression algorithm {:?} is not supported, valid values: lzw, snappy",
                self.compression_algorithm
            )));
        }
        self.zone_aware_routing.validate()?;
        self.propagation_delay_tracker.validate()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RuntimeConfigConfig {
    pub period: GoDuration,
    pub file: StringSliceCSV,
    pub http_client_timeout: GoDuration,
    pub http_client_cluster_validation: ClusterValidationConfig,
    pub http_client_disable_keep_alives: bool,
    pub loader: String,
}

impl Default for RuntimeConfigConfig {
    fn default() -> Self {
        Self {
            period: GoDuration::from_secs(10),
            file: StringSliceCSV::default(),
            http_client_timeout: GoDuration::from_secs(30),
            http_client_cluster_validation: ClusterValidationConfig::default(),
            http_client_disable_keep_alives: true,
            loader: "yaml".to_string(),
        }
    }
}

impl RuntimeConfigConfig {
    pub fn register_flags_into<T: 'static>(
        &self,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        f.string_slice_csv_var("runtime-config.file", &self.file, "Comma separated list of yaml files or URLs with the configuration that can be updated at runtime.", move |t| &get(t).file, move |t| &mut get_mut(t).file);
        f.go_duration_var("runtime-config.reload-period", self.period, "How often to check runtime config files.", move |t| &get(t).period, move |t| &mut get_mut(t).period);
        f.go_duration_var("runtime-config.http-client-timeout", self.http_client_timeout, "HTTP client timeout when fetching runtime config from URLs.", move |t| &get(t).http_client_timeout, move |t| &mut get_mut(t).http_client_timeout);
        f.bool_var("runtime-config.http-client-disable-keep-alives", self.http_client_disable_keep_alives, "Disable HTTP keep-alives for the runtime config HTTP client.", move |t| &get(t).http_client_disable_keep_alives, move |t| &mut get_mut(t).http_client_disable_keep_alives);
        self.http_client_cluster_validation
            .register_flags_into("runtime-config.http-client-cluster-validation.", f, move |t| &get(t).http_client_cluster_validation, move |t| &mut get_mut(t).http_client_cluster_validation);
        f.string_var("runtime-config.loader", &self.loader, "Method used to decode the runtime configuration files. Supported values are: \"map\" and \"yaml\".", move |t| &get(t).loader, move |t| &mut get_mut(t).loader);
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ActivityTrackerConfig {
    pub filepath: String,
    pub max_entries: i64,
}

impl Default for ActivityTrackerConfig {
    fn default() -> Self {
        Self {
            filepath: "./metrics-activity.log".to_string(),
            max_entries: 1024,
        }
    }
}

impl ActivityTrackerConfig {
    pub fn register_flags_into<T: 'static>(
        &self,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        f.string_var("activity-tracker.filepath", &self.filepath, "File where ongoing activities are stored. If empty, activity tracking is disabled.", move |t| &get(t).filepath, move |t| &mut get_mut(t).filepath);
        f.i64_var("activity-tracker.max-entries", self.max_entries, "Max number of concurrent activities that can be tracked.", move |t| &get(t).max_entries, move |t| &mut get_mut(t).max_entries);
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct UsageStatsConfig {
    pub enabled: bool,
    pub installation_mode: String,
}

impl Default for UsageStatsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            installation_mode: "custom".to_string(),
        }
    }
}

impl UsageStatsConfig {
    pub fn register_flags_into<T: 'static>(
        &self,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        f.bool_var("usage-stats.enabled", self.enabled, "Enable anonymous usage reporting.", move |t| &get(t).enabled, move |t| &mut get_mut(t).enabled);
        f.string_var("usage-stats.installation-mode", &self.installation_mode, "Installation mode. Supported values: custom, helm, jsonnet.", move |t| &get(t).installation_mode, move |t| &mut get_mut(t).installation_mode);
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if !matches!(
            self.installation_mode.as_str(),
            "custom" | "helm" | "jsonnet"
        ) {
            return Err(ConfigError::Validation(format!(
                "unsupported installation mode: {:?}",
                self.installation_mode
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CompartmentsReadConfig {
    pub num_compartments: i64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CompartmentsWriteConfig {
    pub num_compartments: i64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CompartmentsConfig {
    pub enabled: bool,
    pub read: CompartmentsReadConfig,
    pub write: CompartmentsWriteConfig,
}

impl CompartmentsConfig {
    pub fn register_flags_into<T: 'static>(
        &self,
        f: &mut FlagSet<T>,
        get: impl Fn(&T) -> &Self + Copy + 'static,
        get_mut: impl Fn(&mut T) -> &mut Self + Copy + 'static,
    ) {
        f.bool_var("compartments.enabled", self.enabled, "Whether the compartments architecture is enabled.", move |t| &get(t).enabled, move |t| &mut get_mut(t).enabled);
        f.i64_var("compartments.read.num-compartments", self.read.num_compartments, "The number of read compartments.", move |t| &get(t).read.num_compartments, move |t| &mut get_mut(t).read.num_compartments);
        f.i64_var("compartments.write.num-compartments", self.write.num_compartments, "The number of write compartments.", move |t| &get(t).write.num_compartments, move |t| &mut get_mut(t).write.num_compartments);
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if !self.enabled {
            return Ok(());
        }
        if self.read.num_compartments <= 0 {
            return Err(ConfigError::Validation(
                "compartments read.num-compartments must be greater than 0 when compartments are enabled".to_string(),
            ));
        }
        if self.read.num_compartments > 256 {
            return Err(ConfigError::Validation(
                "compartments read.num-compartments must not be greater than 256".to_string(),
            ));
        }
        if self.write.num_compartments <= 0 {
            return Err(ConfigError::Validation(
                "compartments write.num-compartments must be greater than 0 when compartments are enabled".to_string(),
            ));
        }
        Ok(())
    }
}
