//! Hidden-doc top-level config schemas (`block_builder`, `block_builder_scheduler`,
//! `usage_tracker`), matching Go `mimir.Config` (`doc:"hidden"` in Go hides CLI flags from
//! `-help-all`, not from YAML marshaling in `/config` and `-print.config`).

use crate::schema::{FieldKind, SchemaBlock, SchemaField};

// ---- block_builder ----

static BB_GRPC_BACKOFF_FIELDS: &[SchemaField] = &[
    SchemaField { name: "min_period", flag: "block-builder.scheduler.grpc-client-config.backoff-min-period", kind: FieldKind::Duration, default_str: "100ms", usage: "" },
    SchemaField { name: "max_period", flag: "block-builder.scheduler.grpc-client-config.backoff-max-period", kind: FieldKind::Duration, default_str: "10s", usage: "" },
    SchemaField { name: "max_retries", flag: "block-builder.scheduler.grpc-client-config.backoff-retries", kind: FieldKind::Int, default_str: "10", usage: "" },
];

static BB_GRPC_CCV_FIELDS: &[SchemaField] = &[
    SchemaField { name: "label", flag: "block-builder.scheduler.grpc-client-config.cluster-validation.label", kind: FieldKind::String, default_str: "", usage: "" },
];

static BB_GRPC_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "backoff_config", fields: BB_GRPC_BACKOFF_FIELDS, blocks: &[] },
    SchemaBlock { name: "cluster_validation", fields: BB_GRPC_CCV_FIELDS, blocks: &[] },
];

static BB_GRPC_FIELDS: &[SchemaField] = &[
    SchemaField { name: "max_recv_msg_size", flag: "block-builder.scheduler.grpc-client-config.grpc-max-recv-msg-size", kind: FieldKind::Int, default_str: "104857600", usage: "" },
    SchemaField { name: "max_send_msg_size", flag: "block-builder.scheduler.grpc-client-config.grpc-max-send-msg-size", kind: FieldKind::Int, default_str: "104857600", usage: "" },
    SchemaField { name: "grpc_compression", flag: "block-builder.scheduler.grpc-client-config.grpc-compression", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "rate_limit", flag: "block-builder.scheduler.grpc-client-config.grpc-client-rate-limit", kind: FieldKind::Float, default_str: "0", usage: "" },
    SchemaField { name: "rate_limit_burst", flag: "block-builder.scheduler.grpc-client-config.grpc-client-rate-limit-burst", kind: FieldKind::Int, default_str: "0", usage: "" },
    SchemaField { name: "backoff_on_ratelimits", flag: "block-builder.scheduler.grpc-client-config.backoff-on-ratelimits", kind: FieldKind::Bool, default_str: "false", usage: "" },
    SchemaField { name: "initial_stream_window_size", flag: "block-builder.scheduler.grpc-client-config.initial-stream-window-size", kind: FieldKind::Bytes, default_str: "63KiB1023B", usage: "" },
    SchemaField { name: "initial_connection_window_size", flag: "block-builder.scheduler.grpc-client-config.initial-connection-window-size", kind: FieldKind::Bytes, default_str: "63KiB1023B", usage: "" },
    SchemaField { name: "tls_enabled", flag: "block-builder.scheduler.grpc-client-config.tls-enabled", kind: FieldKind::Bool, default_str: "false", usage: "" },
    SchemaField { name: "tls_cert_path", flag: "block-builder.scheduler.grpc-client-config.tls-cert-path", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "tls_key_path", flag: "block-builder.scheduler.grpc-client-config.tls-key-path", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "tls_ca_path", flag: "block-builder.scheduler.grpc-client-config.tls-ca-path", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "tls_server_name", flag: "block-builder.scheduler.grpc-client-config.tls-server-name", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "tls_insecure_skip_verify", flag: "block-builder.scheduler.grpc-client-config.tls-insecure-skip-verify", kind: FieldKind::Bool, default_str: "false", usage: "" },
    SchemaField { name: "tls_cipher_suites", flag: "block-builder.scheduler.grpc-client-config.tls-cipher-suites", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "tls_min_version", flag: "block-builder.scheduler.grpc-client-config.tls-min-version", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "connect_timeout", flag: "block-builder.scheduler.grpc-client-config.connect-timeout", kind: FieldKind::Duration, default_str: "5s", usage: "" },
    SchemaField { name: "connect_backoff_base_delay", flag: "block-builder.scheduler.grpc-client-config.connect-backoff-base-delay", kind: FieldKind::Duration, default_str: "1s", usage: "" },
    SchemaField { name: "connect_backoff_max_delay", flag: "block-builder.scheduler.grpc-client-config.connect-backoff-max-delay", kind: FieldKind::Duration, default_str: "5s", usage: "" },
    SchemaField { name: "keepalive_time", flag: "block-builder.scheduler.grpc-client-config.keepalive-time", kind: FieldKind::Duration, default_str: "20s", usage: "" },
    SchemaField { name: "keepalive_timeout", flag: "block-builder.scheduler.grpc-client-config.keepalive-timeout", kind: FieldKind::Duration, default_str: "10s", usage: "" },
];

static BB_SCHEDULER_CONFIG_FIELDS: &[SchemaField] = &[
    SchemaField { name: "address", flag: "block-builder.scheduler.address", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "update_interval", flag: "block-builder.scheduler.update-interval", kind: FieldKind::Duration, default_str: "20s", usage: "" },
    SchemaField { name: "max_update_age", flag: "block-builder.scheduler.max-update-age", kind: FieldKind::Duration, default_str: "30m0s", usage: "" },
];

static BB_SCHEDULER_CONFIG_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "grpc_client_config", fields: BB_GRPC_FIELDS, blocks: BB_GRPC_BLOCKS },
];

static BLOCK_BUILDER_FIELDS: &[SchemaField] = &[
    SchemaField { name: "instance_id", flag: "block-builder.instance-id", kind: FieldKind::String, default_str: "<hostname>", usage: "" },
    SchemaField { name: "data_dir", flag: "block-builder.data-dir", kind: FieldKind::String, default_str: "./data-block-builder/", usage: "" },
    SchemaField { name: "apply_max_global_series_per_user_below", flag: "block-builder.apply-max-global-series-per-user-below", kind: FieldKind::Int, default_str: "0", usage: "" },
    SchemaField { name: "generate_sparse_index_headers", flag: "block-builder.generate-sparse-index-headers", kind: FieldKind::Bool, default_str: "false", usage: "" },
    SchemaField { name: "verify_blocks_before_upload", flag: "block-builder.verify-blocks-before-upload", kind: FieldKind::Bool, default_str: "false", usage: "" },
];

static BLOCK_BUILDER_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "scheduler_config", fields: BB_SCHEDULER_CONFIG_FIELDS, blocks: BB_SCHEDULER_CONFIG_BLOCKS },
];

// ---- block_builder_scheduler ----

static BLOCK_BUILDER_SCHEDULER_FIELDS: &[SchemaField] = &[
    SchemaField { name: "consumer_group", flag: "block-builder-scheduler.consumer-group", kind: FieldKind::String, default_str: "block-builder", usage: "" },
    SchemaField { name: "scheduling_interval", flag: "block-builder-scheduler.scheduling-interval", kind: FieldKind::Duration, default_str: "1m0s", usage: "" },
    SchemaField { name: "job_size", flag: "block-builder-scheduler.job-size", kind: FieldKind::Duration, default_str: "1h0m0s", usage: "" },
    SchemaField { name: "startup_observe_time", flag: "block-builder-scheduler.startup-observe-time", kind: FieldKind::Duration, default_str: "1m0s", usage: "" },
    SchemaField { name: "job_lease_expiry", flag: "block-builder-scheduler.job-lease-expiry", kind: FieldKind::Duration, default_str: "2m0s", usage: "" },
    SchemaField { name: "lookback_on_no_commit", flag: "block-builder-scheduler.lookback-on-no-commit", kind: FieldKind::Duration, default_str: "6h0m0s", usage: "" },
    SchemaField { name: "max_scan_age", flag: "block-builder-scheduler.max-scan-age", kind: FieldKind::Duration, default_str: "12h0m0s", usage: "" },
    SchemaField { name: "max_jobs_per_partition", flag: "block-builder-scheduler.max-jobs-per-partition", kind: FieldKind::Int, default_str: "1", usage: "" },
    SchemaField { name: "enqueue_interval", flag: "block-builder-scheduler.enqueue-interval", kind: FieldKind::Duration, default_str: "2s", usage: "" },
    SchemaField { name: "job_failures_allowed", flag: "block-builder-scheduler.job-failures-allowed", kind: FieldKind::Int, default_str: "2", usage: "" },
];

// ---- usage_tracker ----

macro_rules! ut_kv_consul_fields {
    ($prefix:expr) => {
        &[
            SchemaField { name: "host", flag: concat!($prefix, "consul.hostname"), kind: FieldKind::String, default_str: "localhost:8500", usage: "" },
            SchemaField { name: "acl_token", flag: concat!($prefix, "consul.acl-token"), kind: FieldKind::Secret, default_str: "", usage: "" },
            SchemaField { name: "http_client_timeout", flag: concat!($prefix, "consul.client-timeout"), kind: FieldKind::Duration, default_str: "20s", usage: "" },
            SchemaField { name: "consistent_reads", flag: concat!($prefix, "consul.consistent-reads"), kind: FieldKind::Bool, default_str: "false", usage: "" },
            SchemaField { name: "watch_rate_limit", flag: concat!($prefix, "consul.watch-rate-limit"), kind: FieldKind::Float, default_str: "1", usage: "" },
            SchemaField { name: "watch_burst_size", flag: concat!($prefix, "consul.watch-burst-size"), kind: FieldKind::Int, default_str: "1", usage: "" },
            SchemaField { name: "cas_retry_delay", flag: concat!($prefix, "consul.cas-retry-delay"), kind: FieldKind::Duration, default_str: "1s", usage: "" },
        ]
    };
}

macro_rules! ut_kv_etcd_fields {
    ($prefix:expr) => {
        &[
            SchemaField { name: "endpoints", flag: concat!($prefix, "etcd.endpoints"), kind: FieldKind::StringList, default_str: "", usage: "" },
            SchemaField { name: "dial_timeout", flag: concat!($prefix, "etcd.dial-timeout"), kind: FieldKind::Duration, default_str: "10s", usage: "" },
            SchemaField { name: "max_retries", flag: concat!($prefix, "etcd.max-retries"), kind: FieldKind::Int, default_str: "10", usage: "" },
            SchemaField { name: "tls_enabled", flag: concat!($prefix, "etcd.tls-enabled"), kind: FieldKind::Bool, default_str: "false", usage: "" },
            SchemaField { name: "tls_cert_path", flag: concat!($prefix, "etcd.tls-cert-path"), kind: FieldKind::String, default_str: "", usage: "" },
            SchemaField { name: "tls_key_path", flag: concat!($prefix, "etcd.tls-key-path"), kind: FieldKind::String, default_str: "", usage: "" },
            SchemaField { name: "tls_ca_path", flag: concat!($prefix, "etcd.tls-ca-path"), kind: FieldKind::String, default_str: "", usage: "" },
            SchemaField { name: "tls_server_name", flag: concat!($prefix, "etcd.tls-server-name"), kind: FieldKind::String, default_str: "", usage: "" },
            SchemaField { name: "tls_insecure_skip_verify", flag: concat!($prefix, "etcd.tls-insecure-skip-verify"), kind: FieldKind::Bool, default_str: "false", usage: "" },
            SchemaField { name: "tls_cipher_suites", flag: concat!($prefix, "etcd.tls-cipher-suites"), kind: FieldKind::String, default_str: "", usage: "" },
            SchemaField { name: "tls_min_version", flag: concat!($prefix, "etcd.tls-min-version"), kind: FieldKind::String, default_str: "", usage: "" },
            SchemaField { name: "username", flag: concat!($prefix, "etcd.username"), kind: FieldKind::String, default_str: "", usage: "" },
            SchemaField { name: "password", flag: concat!($prefix, "etcd.password"), kind: FieldKind::Secret, default_str: "", usage: "" },
        ]
    };
}

macro_rules! ut_kv_multi_fields {
    ($prefix:expr) => {
        &[
            SchemaField { name: "primary", flag: concat!($prefix, "multi.primary"), kind: FieldKind::String, default_str: "", usage: "" },
            SchemaField { name: "secondary", flag: concat!($prefix, "multi.secondary"), kind: FieldKind::String, default_str: "", usage: "" },
            SchemaField { name: "mirror_enabled", flag: concat!($prefix, "multi.mirror-enabled"), kind: FieldKind::Bool, default_str: "false", usage: "" },
            SchemaField { name: "mirror_timeout", flag: concat!($prefix, "multi.mirror-timeout"), kind: FieldKind::Duration, default_str: "2s", usage: "" },
        ]
    };
}

macro_rules! ut_kv_fields {
    ($prefix:expr) => {
        &[
            SchemaField { name: "store", flag: concat!($prefix, "store"), kind: FieldKind::String, default_str: "memberlist", usage: "" },
            SchemaField { name: "prefix", flag: concat!($prefix, "prefix"), kind: FieldKind::String, default_str: "collectors/", usage: "" },
        ]
    };
}

static UT_IR_KV_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "consul", fields: ut_kv_consul_fields!("usage-tracker.instance-ring."), blocks: &[] },
    SchemaBlock { name: "etcd", fields: ut_kv_etcd_fields!("usage-tracker.instance-ring."), blocks: &[] },
    SchemaBlock { name: "multi", fields: ut_kv_multi_fields!("usage-tracker.instance-ring."), blocks: &[] },
];

static UT_PR_KV_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "consul", fields: ut_kv_consul_fields!("usage-tracker.partition-ring."), blocks: &[] },
    SchemaBlock { name: "etcd", fields: ut_kv_etcd_fields!("usage-tracker.partition-ring."), blocks: &[] },
    SchemaBlock { name: "multi", fields: ut_kv_multi_fields!("usage-tracker.partition-ring."), blocks: &[] },
];

static UT_INSTANCE_RING_FIELDS: &[SchemaField] = &[
    SchemaField { name: "heartbeat_period", flag: "usage-tracker.instance-ring.heartbeat-period", kind: FieldKind::Duration, default_str: "15s", usage: "" },
    SchemaField { name: "heartbeat_timeout", flag: "usage-tracker.instance-ring.heartbeat-timeout", kind: FieldKind::Duration, default_str: "1m0s", usage: "" },
    SchemaField { name: "auto_forget_unhealthy_periods", flag: "usage-tracker.auto-forget-unhealthy-periods", kind: FieldKind::Int, default_str: "4", usage: "" },
    SchemaField { name: "instance_id", flag: "usage-tracker.instance-ring.instance-id", kind: FieldKind::String, default_str: "<hostname>", usage: "" },
    SchemaField { name: "instance_interface_names", flag: "usage-tracker.instance-ring.instance-interface-names", kind: FieldKind::StringList, default_str: "", usage: "" },
    SchemaField { name: "instance_port", flag: "usage-tracker.instance-ring.instance-port", kind: FieldKind::Int, default_str: "0", usage: "" },
    SchemaField { name: "instance_addr", flag: "usage-tracker.instance-ring.instance-addr", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "instance_availability_zone", flag: "usage-tracker.instance-ring.instance-availability-zone", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "instance_enable_ipv6", flag: "usage-tracker.instance-ring.instance-enable-ipv6", kind: FieldKind::Bool, default_str: "false", usage: "" },
];

static UT_INSTANCE_RING_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "kvstore", fields: ut_kv_fields!("usage-tracker.instance-ring."), blocks: UT_IR_KV_BLOCKS },
];

static UT_PARTITION_RING_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "kvstore", fields: ut_kv_fields!("usage-tracker.partition-ring."), blocks: UT_PR_KV_BLOCKS },
];

macro_rules! ut_kafka_fields {
    ($prefix:expr, $tls_prefix:expr) => {
        &[
            SchemaField { name: "backend", flag: concat!($prefix, "backend"), kind: FieldKind::String, default_str: "kafka", usage: "" },
            SchemaField { name: "address", flag: concat!($prefix, "address"), kind: FieldKind::String, default_str: "", usage: "" },
            SchemaField { name: "topic", flag: concat!($prefix, "topic"), kind: FieldKind::String, default_str: "", usage: "" },
            SchemaField { name: "client_id", flag: concat!($prefix, "client-id"), kind: FieldKind::String, default_str: "", usage: "" },
            SchemaField { name: "client_rack", flag: concat!($prefix, "client-rack"), kind: FieldKind::String, default_str: "", usage: "" },
            SchemaField { name: "dial_timeout", flag: concat!($prefix, "dial-timeout"), kind: FieldKind::Duration, default_str: "2s", usage: "" },
            SchemaField { name: "write_timeout", flag: concat!($prefix, "write-timeout"), kind: FieldKind::Duration, default_str: "10s", usage: "" },
            SchemaField { name: "write_timeout_overhead", flag: concat!($prefix, "write-timeout-overhead"), kind: FieldKind::Duration, default_str: "2s", usage: "" },
            SchemaField { name: "warpstream_health_check_slow_multiplier", flag: concat!($prefix, "warpstream-health-check-slow-multiplier"), kind: FieldKind::Int, default_str: "2", usage: "" },
            SchemaField { name: "warpstream_health_check_max_slow_fraction", flag: concat!($prefix, "warpstream-health-check-max-slow-fraction"), kind: FieldKind::Float, default_str: "0.3", usage: "" },
            SchemaField { name: "warpstream_health_check_faulty_threshold", flag: concat!($prefix, "warpstream-health-check-faulty-threshold"), kind: FieldKind::Float, default_str: "0.2", usage: "" },
            SchemaField { name: "warpstream_health_check_max_faulty_fraction", flag: concat!($prefix, "warpstream-health-check-max-faulty-fraction"), kind: FieldKind::Float, default_str: "0.3", usage: "" },
            SchemaField { name: "warpstream_hedge_min_delay", flag: concat!($prefix, "warpstream-hedge-min-delay"), kind: FieldKind::Duration, default_str: "500ms", usage: "" },
            SchemaField { name: "warpstream_hedge_max_agents", flag: concat!($prefix, "warpstream-hedge-max-agents"), kind: FieldKind::Int, default_str: "3", usage: "" },
            SchemaField { name: "warpstream_demoter_probe_interval", flag: concat!($prefix, "warpstream-demoter-probe-interval"), kind: FieldKind::Duration, default_str: "1s", usage: "" },
            SchemaField { name: "sasl_mechanism", flag: concat!($prefix, "sasl-mechanism"), kind: FieldKind::String, default_str: "PLAIN", usage: "" },
            SchemaField { name: "sasl_username", flag: concat!($prefix, "sasl-username"), kind: FieldKind::String, default_str: "", usage: "" },
            SchemaField { name: "sasl_password", flag: concat!($prefix, "sasl-password"), kind: FieldKind::Secret, default_str: "", usage: "" },
            SchemaField { name: "sasl_oauthbearer_token", flag: concat!($prefix, "sasl-oauthbearer-token"), kind: FieldKind::Secret, default_str: "", usage: "" },
            SchemaField { name: "sasl_oauthbearer_zid", flag: concat!($prefix, "sasl-oauthbearer-zid"), kind: FieldKind::String, default_str: "", usage: "" },
            SchemaField { name: "sasl_oauthbearer_extensions", flag: concat!($prefix, "sasl-oauthbearer-extensions"), kind: FieldKind::Other, default_str: "{}", usage: "" },
            SchemaField { name: "sasl_oauthbearer_file_path", flag: concat!($prefix, "sasl-oauthbearer-file-path"), kind: FieldKind::String, default_str: "", usage: "" },
            SchemaField { name: "sasl_oauthbearer_http_socket_path", flag: concat!($prefix, "sasl-oauthbearer-http-socket-path"), kind: FieldKind::String, default_str: "", usage: "" },
            SchemaField { name: "sasl_oauthbearer_http_socket_timeout", flag: concat!($prefix, "sasl-oauthbearer-http-socket-timeout"), kind: FieldKind::Duration, default_str: "10s", usage: "" },
            SchemaField { name: "sasl_msk_iam_access_key", flag: concat!($prefix, "sasl-msk-iam-access-key"), kind: FieldKind::Secret, default_str: "", usage: "" },
            SchemaField { name: "sasl_msk_iam_secret_key", flag: concat!($prefix, "sasl-msk-iam-secret-key"), kind: FieldKind::Secret, default_str: "", usage: "" },
            SchemaField { name: "sasl_msk_iam_session_token", flag: concat!($prefix, "sasl-msk-iam-session-token"), kind: FieldKind::Secret, default_str: "", usage: "" },
            SchemaField { name: "sasl_msk_iam_user_agent", flag: concat!($prefix, "sasl-msk-iam-user-agent"), kind: FieldKind::String, default_str: "", usage: "" },
            SchemaField { name: "sasl_msk_iam_file_path", flag: concat!($prefix, "sasl-msk-iam-file-path"), kind: FieldKind::String, default_str: "", usage: "" },
            SchemaField { name: "sasl_msk_iam_http_socket_path", flag: concat!($prefix, "sasl-msk-iam-http-socket-path"), kind: FieldKind::String, default_str: "", usage: "" },
            SchemaField { name: "sasl_msk_iam_http_socket_timeout", flag: concat!($prefix, "sasl-msk-iam-http-socket-timeout"), kind: FieldKind::Duration, default_str: "10s", usage: "" },
            SchemaField { name: "tls_enabled", flag: concat!($prefix, "tls-enabled"), kind: FieldKind::Bool, default_str: "false", usage: "" },
            SchemaField { name: "tls_cert_path", flag: concat!($tls_prefix, ".tls-cert-path"), kind: FieldKind::String, default_str: "", usage: "" },
            SchemaField { name: "tls_key_path", flag: concat!($tls_prefix, ".tls-key-path"), kind: FieldKind::String, default_str: "", usage: "" },
            SchemaField { name: "tls_ca_path", flag: concat!($tls_prefix, ".tls-ca-path"), kind: FieldKind::String, default_str: "", usage: "" },
            SchemaField { name: "tls_server_name", flag: concat!($tls_prefix, ".tls-server-name"), kind: FieldKind::String, default_str: "", usage: "" },
            SchemaField { name: "tls_insecure_skip_verify", flag: concat!($tls_prefix, ".tls-insecure-skip-verify"), kind: FieldKind::Bool, default_str: "false", usage: "" },
            SchemaField { name: "tls_cipher_suites", flag: concat!($tls_prefix, ".tls-cipher-suites"), kind: FieldKind::String, default_str: "", usage: "" },
            SchemaField { name: "tls_min_version", flag: concat!($tls_prefix, ".tls-min-version"), kind: FieldKind::String, default_str: "", usage: "" },
            SchemaField { name: "consumer_group", flag: concat!($prefix, "consumer-group"), kind: FieldKind::String, default_str: "", usage: "" },
            SchemaField { name: "consumer_group_offset_commit_interval", flag: concat!($prefix, "consumer-group-offset-commit-interval"), kind: FieldKind::Duration, default_str: "1s", usage: "" },
            SchemaField { name: "consumer_group_offset_commit_file_enforced", flag: concat!($prefix, "consumer-group-offset-commit-file-enforced"), kind: FieldKind::Bool, default_str: "false", usage: "" },
            SchemaField { name: "last_produced_offset_poll_interval", flag: concat!($prefix, "last-produced-offset-poll-interval"), kind: FieldKind::Duration, default_str: "1s", usage: "" },
            SchemaField { name: "last_produced_offset_retry_timeout", flag: concat!($prefix, "last-produced-offset-retry-timeout"), kind: FieldKind::Duration, default_str: "10s", usage: "" },
            SchemaField { name: "consume_from_position_at_startup", flag: concat!($prefix, "consume-from-position-at-startup"), kind: FieldKind::String, default_str: "last-offset", usage: "" },
            SchemaField { name: "consume_from_timestamp_at_startup", flag: concat!($prefix, "consume-from-timestamp-at-startup"), kind: FieldKind::Int, default_str: "0", usage: "" },
            SchemaField { name: "target_consumer_lag_at_startup", flag: concat!($prefix, "target-consumer-lag-at-startup"), kind: FieldKind::Duration, default_str: "2s", usage: "" },
            SchemaField { name: "max_consumer_lag_at_startup", flag: concat!($prefix, "max-consumer-lag-at-startup"), kind: FieldKind::Duration, default_str: "15s", usage: "" },
            SchemaField { name: "auto_create_topic_enabled", flag: concat!($prefix, "auto-create-topic-enabled"), kind: FieldKind::Bool, default_str: "true", usage: "" },
            SchemaField { name: "auto_create_topic_default_partitions", flag: concat!($prefix, "auto-create-topic-default-partitions"), kind: FieldKind::Int, default_str: "-1", usage: "" },
            SchemaField { name: "producer_max_record_size_bytes", flag: concat!($prefix, "producer-max-record-size-bytes"), kind: FieldKind::Int, default_str: "15983616", usage: "" },
            SchemaField { name: "producer_max_buffered_bytes", flag: concat!($prefix, "producer-max-buffered-bytes"), kind: FieldKind::Int, default_str: "1073741824", usage: "" },
            SchemaField { name: "producer_compression", flag: concat!($prefix, "producer-compression"), kind: FieldKind::String, default_str: "", usage: "" },
            SchemaField { name: "wait_strong_read_consistency_timeout", flag: concat!($prefix, "wait-strong-read-consistency-timeout"), kind: FieldKind::Duration, default_str: "20s", usage: "" },
            SchemaField { name: "producer_record_version", flag: concat!($prefix, "producer-record-version"), kind: FieldKind::Int, default_str: "2", usage: "" },
            SchemaField { name: "fetch_max_wait", flag: concat!($prefix, "fetch-max-wait"), kind: FieldKind::Duration, default_str: "5s", usage: "" },
            SchemaField { name: "fetch_concurrency_max", flag: concat!($prefix, "fetch-concurrency-max"), kind: FieldKind::Int, default_str: "12", usage: "" },
            SchemaField { name: "use_compressed_bytes_as_fetch_max_bytes", flag: concat!($prefix, "use-compressed-bytes-as-fetch-max-bytes"), kind: FieldKind::Bool, default_str: "true", usage: "" },
            SchemaField { name: "max_buffered_bytes", flag: concat!($prefix, "max-buffered-bytes"), kind: FieldKind::Int, default_str: "1000000000", usage: "" },
            SchemaField { name: "ingestion_concurrency_max", flag: concat!($prefix, "ingestion-concurrency-max"), kind: FieldKind::Int, default_str: "8", usage: "" },
            SchemaField { name: "ingestion_concurrency_batch_size", flag: concat!($prefix, "ingestion-concurrency-batch-size"), kind: FieldKind::Int, default_str: "150", usage: "" },
            SchemaField { name: "ingestion_concurrency_queue_capacity", flag: concat!($prefix, "ingestion-concurrency-queue-capacity"), kind: FieldKind::Int, default_str: "3", usage: "" },
            SchemaField { name: "ingestion_concurrency_target_flushes_per_shard", flag: concat!($prefix, "ingestion-concurrency-target-flushes-per-shard"), kind: FieldKind::Int, default_str: "40", usage: "" },
            SchemaField { name: "ingestion_concurrency_estimated_bytes_per_sample", flag: concat!($prefix, "ingestion-concurrency-estimated-bytes-per-sample"), kind: FieldKind::Int, default_str: "200", usage: "" },
        ]
    };
}

static UT_EVENTS_STORAGE_WRITER_FIELDS: &[SchemaField] = ut_kafka_fields!("usage-tracker.events-storage.writer.", "usage-tracker.events-storage.writer");
static UT_EVENTS_STORAGE_READER_FIELDS: &[SchemaField] = ut_kafka_fields!("usage-tracker.events-storage.reader.", "usage-tracker.events-storage.reader");
static UT_SNAPSHOTS_METADATA_WRITER_FIELDS: &[SchemaField] = ut_kafka_fields!("usage-tracker.snapshots-metadata.writer.", "usage-tracker.snapshots-metadata.writer");
static UT_SNAPSHOTS_METADATA_READER_FIELDS: &[SchemaField] = ut_kafka_fields!("usage-tracker.snapshots-metadata.reader.", "usage-tracker.snapshots-metadata.reader");

static UT_STOR_S3_SSE_FIELDS: &[SchemaField] = &[
    SchemaField { name: "type", flag: "usage-tracker.snapshots-storage.s3.sse.type", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "kms_key_id", flag: "usage-tracker.snapshots-storage.s3.sse.kms-key-id", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "kms_encryption_context", flag: "usage-tracker.snapshots-storage.s3.sse.kms-encryption-context", kind: FieldKind::String, default_str: "", usage: "" },
];

static UT_STOR_S3_HTTP_FIELDS: &[SchemaField] = &[
    SchemaField { name: "idle_conn_timeout", flag: "usage-tracker.snapshots-storage.s3.http.idle-conn-timeout", kind: FieldKind::Duration, default_str: "90000000000", usage: "" },
    SchemaField { name: "response_header_timeout", flag: "usage-tracker.snapshots-storage.s3.http.response-header-timeout", kind: FieldKind::Duration, default_str: "120000000000", usage: "" },
    SchemaField { name: "insecure_skip_verify", flag: "usage-tracker.snapshots-storage.s3.http.insecure-skip-verify", kind: FieldKind::Bool, default_str: "false", usage: "" },
    SchemaField { name: "tls_handshake_timeout", flag: "usage-tracker.snapshots-storage.s3.tls-handshake-timeout", kind: FieldKind::Duration, default_str: "10000000000", usage: "" },
    SchemaField { name: "expect_continue_timeout", flag: "usage-tracker.snapshots-storage.s3.expect-continue-timeout", kind: FieldKind::Duration, default_str: "1000000000", usage: "" },
    SchemaField { name: "max_idle_connections", flag: "usage-tracker.snapshots-storage.s3.max-idle-connections", kind: FieldKind::Int, default_str: "100", usage: "" },
    SchemaField { name: "max_idle_connections_per_host", flag: "usage-tracker.snapshots-storage.s3.max-idle-connections-per-host", kind: FieldKind::Int, default_str: "100", usage: "" },
    SchemaField { name: "max_connections_per_host", flag: "usage-tracker.snapshots-storage.s3.max-connections-per-host", kind: FieldKind::Int, default_str: "0", usage: "" },
    SchemaField { name: "force_attempt_http2", flag: "usage-tracker.snapshots-storage.s3.http.force-attempt-http2", kind: FieldKind::Bool, default_str: "false", usage: "" },
    SchemaField { name: "tls_ca_path", flag: "usage-tracker.snapshots-storage.s3.http.tls-ca-path", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "tls_cert_path", flag: "usage-tracker.snapshots-storage.s3.http.tls-cert-path", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "tls_key_path", flag: "usage-tracker.snapshots-storage.s3.http.tls-key-path", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "tls_server_name", flag: "usage-tracker.snapshots-storage.s3.http.tls-server-name", kind: FieldKind::String, default_str: "", usage: "" },
];

static UT_STOR_S3_TRACE_FIELDS: &[SchemaField] = &[
    SchemaField { name: "enabled", flag: "usage-tracker.snapshots-storage.s3.trace.enabled", kind: FieldKind::Bool, default_str: "false", usage: "" },
];

static UT_STOR_S3_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "sse", fields: UT_STOR_S3_SSE_FIELDS, blocks: &[] },
    SchemaBlock { name: "http", fields: UT_STOR_S3_HTTP_FIELDS, blocks: &[] },
    SchemaBlock { name: "trace", fields: UT_STOR_S3_TRACE_FIELDS, blocks: &[] },
];

static UT_STOR_S3_FIELDS: &[SchemaField] = &[
    SchemaField { name: "endpoint", flag: "usage-tracker.snapshots-storage.s3.endpoint", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "region", flag: "usage-tracker.snapshots-storage.s3.region", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "bucket_name", flag: "usage-tracker.snapshots-storage.s3.bucket-name", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "secret_access_key", flag: "usage-tracker.snapshots-storage.s3.secret-access-key", kind: FieldKind::Secret, default_str: "", usage: "" },
    SchemaField { name: "access_key_id", flag: "usage-tracker.snapshots-storage.s3.access-key-id", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "session_token", flag: "usage-tracker.snapshots-storage.s3.session-token", kind: FieldKind::Secret, default_str: "", usage: "" },
    SchemaField { name: "insecure", flag: "usage-tracker.snapshots-storage.s3.insecure", kind: FieldKind::Bool, default_str: "false", usage: "" },
    SchemaField { name: "signature_version", flag: "usage-tracker.snapshots-storage.s3.signature-version", kind: FieldKind::String, default_str: "v4", usage: "" },
    SchemaField { name: "list_objects_version", flag: "usage-tracker.snapshots-storage.s3.list-objects-version", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "bucket_lookup_type", flag: "usage-tracker.snapshots-storage.s3.bucket-lookup-type", kind: FieldKind::String, default_str: "auto", usage: "" },
    SchemaField { name: "dualstack_enabled", flag: "usage-tracker.snapshots-storage.s3.dualstack-enabled", kind: FieldKind::Bool, default_str: "true", usage: "" },
    SchemaField { name: "storage_class", flag: "usage-tracker.snapshots-storage.s3.storage-class", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "native_aws_auth_enabled", flag: "usage-tracker.snapshots-storage.s3.native-aws-auth-enabled", kind: FieldKind::Bool, default_str: "false", usage: "" },
    SchemaField { name: "part_size", flag: "usage-tracker.snapshots-storage.s3.part-size", kind: FieldKind::Int, default_str: "0", usage: "" },
    SchemaField { name: "send_content_md5", flag: "usage-tracker.snapshots-storage.s3.send-content-md5", kind: FieldKind::Bool, default_str: "false", usage: "" },
    SchemaField { name: "sts_endpoint", flag: "usage-tracker.snapshots-storage.s3.sts-endpoint", kind: FieldKind::String, default_str: "", usage: "" },
];

static UT_STOR_GCS_HTTP_FIELDS: &[SchemaField] = &[
    SchemaField { name: "idle_conn_timeout", flag: "usage-tracker.snapshots-storage.gcs.http.idle-conn-timeout", kind: FieldKind::Duration, default_str: "90000000000", usage: "" },
    SchemaField { name: "response_header_timeout", flag: "usage-tracker.snapshots-storage.gcs.http.response-header-timeout", kind: FieldKind::Duration, default_str: "120000000000", usage: "" },
    SchemaField { name: "insecure_skip_verify", flag: "usage-tracker.snapshots-storage.gcs.http.insecure-skip-verify", kind: FieldKind::Bool, default_str: "false", usage: "" },
    SchemaField { name: "tls_handshake_timeout", flag: "usage-tracker.snapshots-storage.gcs.tls-handshake-timeout", kind: FieldKind::Duration, default_str: "10000000000", usage: "" },
    SchemaField { name: "expect_continue_timeout", flag: "usage-tracker.snapshots-storage.gcs.expect-continue-timeout", kind: FieldKind::Duration, default_str: "1000000000", usage: "" },
    SchemaField { name: "max_idle_connections", flag: "usage-tracker.snapshots-storage.gcs.max-idle-connections", kind: FieldKind::Int, default_str: "100", usage: "" },
    SchemaField { name: "max_idle_connections_per_host", flag: "usage-tracker.snapshots-storage.gcs.max-idle-connections-per-host", kind: FieldKind::Int, default_str: "100", usage: "" },
    SchemaField { name: "max_connections_per_host", flag: "usage-tracker.snapshots-storage.gcs.max-connections-per-host", kind: FieldKind::Int, default_str: "0", usage: "" },
    SchemaField { name: "force_attempt_http2", flag: "usage-tracker.snapshots-storage.gcs.http.force-attempt-http2", kind: FieldKind::Bool, default_str: "false", usage: "" },
    SchemaField { name: "tls_ca_path", flag: "usage-tracker.snapshots-storage.gcs.http.tls-ca-path", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "tls_cert_path", flag: "usage-tracker.snapshots-storage.gcs.http.tls-cert-path", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "tls_key_path", flag: "usage-tracker.snapshots-storage.gcs.http.tls-key-path", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "tls_server_name", flag: "usage-tracker.snapshots-storage.gcs.http.tls-server-name", kind: FieldKind::String, default_str: "", usage: "" },
];

static UT_STOR_GCS_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "http", fields: UT_STOR_GCS_HTTP_FIELDS, blocks: &[] },
];

static UT_STOR_GCS_FIELDS: &[SchemaField] = &[
    SchemaField { name: "bucket_name", flag: "usage-tracker.snapshots-storage.gcs.bucket-name", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "service_account", flag: "usage-tracker.snapshots-storage.gcs.service-account", kind: FieldKind::Secret, default_str: "", usage: "" },
    SchemaField { name: "enable_upload_retries", flag: "usage-tracker.snapshots-storage.gcs.enable-upload-retries", kind: FieldKind::Bool, default_str: "true", usage: "" },
    SchemaField { name: "max_retries", flag: "usage-tracker.snapshots-storage.gcs.max-retries", kind: FieldKind::Int, default_str: "20", usage: "" },
];

static UT_STOR_AZURE_HTTP_FIELDS: &[SchemaField] = &[
    SchemaField { name: "idle_conn_timeout", flag: "usage-tracker.snapshots-storage.azure.http.idle-conn-timeout", kind: FieldKind::Duration, default_str: "90000000000", usage: "" },
    SchemaField { name: "response_header_timeout", flag: "usage-tracker.snapshots-storage.azure.http.response-header-timeout", kind: FieldKind::Duration, default_str: "120000000000", usage: "" },
    SchemaField { name: "insecure_skip_verify", flag: "usage-tracker.snapshots-storage.azure.http.insecure-skip-verify", kind: FieldKind::Bool, default_str: "false", usage: "" },
    SchemaField { name: "tls_handshake_timeout", flag: "usage-tracker.snapshots-storage.azure.tls-handshake-timeout", kind: FieldKind::Duration, default_str: "10000000000", usage: "" },
    SchemaField { name: "expect_continue_timeout", flag: "usage-tracker.snapshots-storage.azure.expect-continue-timeout", kind: FieldKind::Duration, default_str: "1000000000", usage: "" },
    SchemaField { name: "max_idle_connections", flag: "usage-tracker.snapshots-storage.azure.max-idle-connections", kind: FieldKind::Int, default_str: "100", usage: "" },
    SchemaField { name: "max_idle_connections_per_host", flag: "usage-tracker.snapshots-storage.azure.max-idle-connections-per-host", kind: FieldKind::Int, default_str: "100", usage: "" },
    SchemaField { name: "max_connections_per_host", flag: "usage-tracker.snapshots-storage.azure.max-connections-per-host", kind: FieldKind::Int, default_str: "0", usage: "" },
    SchemaField { name: "force_attempt_http2", flag: "usage-tracker.snapshots-storage.azure.http.force-attempt-http2", kind: FieldKind::Bool, default_str: "false", usage: "" },
    SchemaField { name: "tls_ca_path", flag: "usage-tracker.snapshots-storage.azure.http.tls-ca-path", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "tls_cert_path", flag: "usage-tracker.snapshots-storage.azure.http.tls-cert-path", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "tls_key_path", flag: "usage-tracker.snapshots-storage.azure.http.tls-key-path", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "tls_server_name", flag: "usage-tracker.snapshots-storage.azure.http.tls-server-name", kind: FieldKind::String, default_str: "", usage: "" },
];

static UT_STOR_AZURE_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "http", fields: UT_STOR_AZURE_HTTP_FIELDS, blocks: &[] },
];

static UT_STOR_AZURE_FIELDS: &[SchemaField] = &[
    SchemaField { name: "account_name", flag: "usage-tracker.snapshots-storage.azure.account-name", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "account_key", flag: "usage-tracker.snapshots-storage.azure.account-key", kind: FieldKind::Secret, default_str: "", usage: "" },
    SchemaField { name: "connection_string", flag: "usage-tracker.snapshots-storage.azure.connection-string", kind: FieldKind::Secret, default_str: "", usage: "" },
    SchemaField { name: "container_name", flag: "usage-tracker.snapshots-storage.azure.container-name", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "endpoint_suffix", flag: "usage-tracker.snapshots-storage.azure.endpoint-suffix", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "max_retries", flag: "usage-tracker.snapshots-storage.azure.max-retries", kind: FieldKind::Int, default_str: "20", usage: "" },
    SchemaField { name: "user_assigned_id", flag: "usage-tracker.snapshots-storage.azure.user-assigned-id", kind: FieldKind::String, default_str: "", usage: "" },
];

static UT_STOR_SWIFT_FIELDS: &[SchemaField] = &[
    SchemaField { name: "application_credential_id", flag: "usage-tracker.snapshots-storage.swift.application-credential-id", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "application_credential_name", flag: "usage-tracker.snapshots-storage.swift.application-credential-name", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "application_credential_secret", flag: "usage-tracker.snapshots-storage.swift.application-credential-secret", kind: FieldKind::Secret, default_str: "", usage: "" },
    SchemaField { name: "auth_version", flag: "usage-tracker.snapshots-storage.swift.auth-version", kind: FieldKind::Int, default_str: "0", usage: "" },
    SchemaField { name: "auth_url", flag: "usage-tracker.snapshots-storage.swift.auth-url", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "username", flag: "usage-tracker.snapshots-storage.swift.username", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "user_domain_name", flag: "usage-tracker.snapshots-storage.swift.user-domain-name", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "user_domain_id", flag: "usage-tracker.snapshots-storage.swift.user-domain-id", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "user_id", flag: "usage-tracker.snapshots-storage.swift.user-id", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "password", flag: "usage-tracker.snapshots-storage.swift.password", kind: FieldKind::Secret, default_str: "", usage: "" },
    SchemaField { name: "domain_id", flag: "usage-tracker.snapshots-storage.swift.domain-id", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "domain_name", flag: "usage-tracker.snapshots-storage.swift.domain-name", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "project_id", flag: "usage-tracker.snapshots-storage.swift.project-id", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "project_name", flag: "usage-tracker.snapshots-storage.swift.project-name", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "project_domain_id", flag: "usage-tracker.snapshots-storage.swift.project-domain-id", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "project_domain_name", flag: "usage-tracker.snapshots-storage.swift.project-domain-name", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "region_name", flag: "usage-tracker.snapshots-storage.swift.region-name", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "container_name", flag: "usage-tracker.snapshots-storage.swift.container-name", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "max_retries", flag: "usage-tracker.snapshots-storage.swift.max-retries", kind: FieldKind::Int, default_str: "3", usage: "" },
    SchemaField { name: "connect_timeout", flag: "usage-tracker.snapshots-storage.swift.connect-timeout", kind: FieldKind::Duration, default_str: "10000000000", usage: "" },
    SchemaField { name: "request_timeout", flag: "usage-tracker.snapshots-storage.swift.request-timeout", kind: FieldKind::Duration, default_str: "5000000000", usage: "" },
];

static UT_STOR_FS_FIELDS: &[SchemaField] = &[
    SchemaField { name: "dir", flag: "usage-tracker.snapshots-storage.filesystem.dir", kind: FieldKind::String, default_str: "usagetrackersnapshots", usage: "" },
];

static UT_SNAPSHOTS_STORAGE_FIELDS: &[SchemaField] = &[
    SchemaField { name: "backend", flag: "usage-tracker.snapshots-storage.backend", kind: FieldKind::String, default_str: "filesystem", usage: "" },
    SchemaField { name: "storage_prefix", flag: "usage-tracker.snapshots-storage.storage-prefix", kind: FieldKind::String, default_str: "", usage: "" },
];

static UT_SNAPSHOTS_STORAGE_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "s3", fields: UT_STOR_S3_FIELDS, blocks: UT_STOR_S3_BLOCKS },
    SchemaBlock { name: "gcs", fields: UT_STOR_GCS_FIELDS, blocks: UT_STOR_GCS_BLOCKS },
    SchemaBlock { name: "azure", fields: UT_STOR_AZURE_FIELDS, blocks: UT_STOR_AZURE_BLOCKS },
    SchemaBlock { name: "swift", fields: UT_STOR_SWIFT_FIELDS, blocks: &[] },
    SchemaBlock { name: "filesystem", fields: UT_STOR_FS_FIELDS, blocks: &[] },
];

static UT_LOAD_BACKOFF_FIELDS: &[SchemaField] = &[
    SchemaField { name: "min_period", flag: "usage-tracker.snapshots-load-backoff.backoff-min-period", kind: FieldKind::Duration, default_str: "100ms", usage: "" },
    SchemaField { name: "max_period", flag: "usage-tracker.snapshots-load-backoff.backoff-max-period", kind: FieldKind::Duration, default_str: "10s", usage: "" },
    SchemaField { name: "max_retries", flag: "usage-tracker.snapshots-load-backoff.backoff-retries", kind: FieldKind::Int, default_str: "10", usage: "" },
];

static USAGE_TRACKER_FIELDS: &[SchemaField] = &[
    SchemaField { name: "enabled", flag: "usage-tracker.enabled", kind: FieldKind::Bool, default_str: "false", usage: "" },
    SchemaField { name: "partitions", flag: "usage-tracker.partitions", kind: FieldKind::Int, default_str: "64", usage: "" },
    SchemaField { name: "partition_reconcile_interval", flag: "usage-tracker.partition-reconcile-interval", kind: FieldKind::Duration, default_str: "10s", usage: "" },
    SchemaField { name: "lost_partitions_shutdown_grace_period", flag: "usage-tracker.lost-partitions-shutdown-grace-period", kind: FieldKind::Duration, default_str: "30s", usage: "" },
    SchemaField { name: "max_partitions_to_create_per_reconcile", flag: "usage-tracker.max-partitions-to-create-per-reconcile", kind: FieldKind::Int, default_str: "1", usage: "" },
    SchemaField { name: "idle_timeout", flag: "usage-tracker.idle-timeout", kind: FieldKind::Duration, default_str: "20m0s", usage: "" },
    SchemaField { name: "max_pending_created_series_events", flag: "usage-tracker.created-series-events-max-pending", kind: FieldKind::Int, default_str: "10000", usage: "" },
    SchemaField { name: "created_series_events_max_batch_size_bytes", flag: "usage-tracker.created-series-events-max-batch-size-bytes", kind: FieldKind::Int, default_str: "1048576", usage: "" },
    SchemaField { name: "created_series_events_batch_ttl", flag: "usage-tracker.created-series-events-batch-ttl", kind: FieldKind::Duration, default_str: "250ms", usage: "" },
    SchemaField { name: "created_series_events_publish_concurrency", flag: "usage-tracker.created-series-events-publish-concurrency", kind: FieldKind::Int, default_str: "10", usage: "" },
    SchemaField { name: "skip_snapshot_loading_at_startup", flag: "usage-tracker.skip-snapshot-loading-at-startup", kind: FieldKind::Bool, default_str: "false", usage: "" },
    SchemaField { name: "snapshot_interval_jitter", flag: "usage-tracker.snapshot-interval-jitter", kind: FieldKind::Float, default_str: "0.1", usage: "" },
    SchemaField { name: "target_snapshot_file_size_bytes", flag: "usage-tracker.target-snapshot-file-size-bytes", kind: FieldKind::Int, default_str: "104857600", usage: "" },
    SchemaField { name: "snapshot_cleanup_interval", flag: "usage-tracker.snapshot-cleanup-interval", kind: FieldKind::Duration, default_str: "1h0m0s", usage: "" },
    SchemaField { name: "snapshot_cleanup_interval_jitter", flag: "usage-tracker.snapshot-cleanup-interval-jitter", kind: FieldKind::Float, default_str: "0.25", usage: "" },
    SchemaField { name: "max_events_fetch_size", flag: "usage-tracker.max-events-fetch-size", kind: FieldKind::Int, default_str: "100", usage: "" },
    SchemaField { name: "user_close_to_limit_percentage_threshold", flag: "usage-tracker.user-close-to-limit-percentage-threshold", kind: FieldKind::Int, default_str: "90", usage: "" },
    SchemaField { name: "enable_verbose_series_creation_deletion_prometheus_metrics", flag: "usage-tracker.enable-verbose-series-creation-deletion-prometheus-metrics", kind: FieldKind::Bool, default_str: "false", usage: "" },
    SchemaField { name: "min_time_between_shards_cleanup", flag: "usage-tracker.min-time-between-shards-cleanup", kind: FieldKind::Duration, default_str: "25ms", usage: "" },
    SchemaField { name: "tenantshard_impl_version", flag: "usage-tracker.tenantshard-impl-version", kind: FieldKind::Int, default_str: "2", usage: "" },
    SchemaField { name: "num_shards", flag: "usage-tracker.num-shards", kind: FieldKind::Int, default_str: "16", usage: "" },
];

static USAGE_TRACKER_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "instance_ring", fields: UT_INSTANCE_RING_FIELDS, blocks: UT_INSTANCE_RING_BLOCKS },
    SchemaBlock { name: "partition_ring", fields: &[], blocks: UT_PARTITION_RING_BLOCKS },
    SchemaBlock { name: "events_storage_writer", fields: UT_EVENTS_STORAGE_WRITER_FIELDS, blocks: &[] },
    SchemaBlock { name: "events_storage_reader", fields: UT_EVENTS_STORAGE_READER_FIELDS, blocks: &[] },
    SchemaBlock { name: "snapshots_metadata_writer", fields: UT_SNAPSHOTS_METADATA_WRITER_FIELDS, blocks: &[] },
    SchemaBlock { name: "snapshots_metadata_reader", fields: UT_SNAPSHOTS_METADATA_READER_FIELDS, blocks: &[] },
    SchemaBlock { name: "snapshots_storage", fields: UT_SNAPSHOTS_STORAGE_FIELDS, blocks: UT_SNAPSHOTS_STORAGE_BLOCKS },
    SchemaBlock { name: "snapshots_load_backoff", fields: UT_LOAD_BACKOFF_FIELDS, blocks: &[] },
];

pub static HIDDEN_TOP_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "block_builder", fields: BLOCK_BUILDER_FIELDS, blocks: BLOCK_BUILDER_BLOCKS },
    SchemaBlock { name: "block_builder_scheduler", fields: BLOCK_BUILDER_SCHEDULER_FIELDS, blocks: &[] },
    SchemaBlock { name: "usage_tracker", fields: USAGE_TRACKER_FIELDS, blocks: USAGE_TRACKER_BLOCKS },
];
