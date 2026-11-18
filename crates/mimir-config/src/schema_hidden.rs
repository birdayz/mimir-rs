//! Hidden-doc top-level config schemas (`block_builder`, `block_builder_scheduler`,
//! `usage_tracker`), matching Go `mimir.Config` (`doc:"hidden"` in Go hides CLI flags from
//! `-help-all`, not from YAML marshaling in `/config` and `-print.config`).

use crate::schema::{FieldKind, SchemaBlock, SchemaField};

// ---- block_builder ----

static BB_GRPC_BACKOFF_FIELDS: &[SchemaField] = &[
    SchemaField { name: "min_period", flag: "", kind: FieldKind::Duration, default_str: "100ms", usage: "" },
    SchemaField { name: "max_period", flag: "", kind: FieldKind::Duration, default_str: "10s", usage: "" },
    SchemaField { name: "max_retries", flag: "", kind: FieldKind::Int, default_str: "10", usage: "" },
];

static BB_GRPC_CCV_FIELDS: &[SchemaField] = &[
    SchemaField { name: "label", flag: "block-builder.scheduler.grpc-client-config.cluster-validation.label", kind: FieldKind::String, default_str: "", usage: "" },
];

static BB_GRPC_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "backoff_config", fields: BB_GRPC_BACKOFF_FIELDS, blocks: &[] },
    SchemaBlock { name: "cluster_validation", fields: BB_GRPC_CCV_FIELDS, blocks: &[] },
];

static BB_GRPC_FIELDS: &[SchemaField] = &[
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

static UT_KV_CONSUL_FIELDS: &[SchemaField] = &[
    SchemaField { name: "host", flag: "", kind: FieldKind::String, default_str: "localhost:8500", usage: "" },
    SchemaField { name: "acl_token", flag: "", kind: FieldKind::Secret, default_str: "", usage: "" },
    SchemaField { name: "http_client_timeout", flag: "", kind: FieldKind::Duration, default_str: "20s", usage: "" },
    SchemaField { name: "consistent_reads", flag: "", kind: FieldKind::Bool, default_str: "false", usage: "" },
    SchemaField { name: "watch_rate_limit", flag: "", kind: FieldKind::Float, default_str: "1", usage: "" },
    SchemaField { name: "watch_burst_size", flag: "", kind: FieldKind::Int, default_str: "1", usage: "" },
    SchemaField { name: "cas_retry_delay", flag: "", kind: FieldKind::Duration, default_str: "1s", usage: "" },
];

static UT_KV_ETCD_FIELDS: &[SchemaField] = &[
    SchemaField { name: "endpoints", flag: "", kind: FieldKind::StringList, default_str: "", usage: "" },
    SchemaField { name: "dial_timeout", flag: "", kind: FieldKind::Duration, default_str: "10s", usage: "" },
    SchemaField { name: "max_retries", flag: "", kind: FieldKind::Int, default_str: "10", usage: "" },
    SchemaField { name: "tls_enabled", flag: "", kind: FieldKind::Bool, default_str: "false", usage: "" },
    SchemaField { name: "tls_cert_path", flag: "", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "tls_key_path", flag: "", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "tls_ca_path", flag: "", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "tls_server_name", flag: "", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "tls_insecure_skip_verify", flag: "", kind: FieldKind::Bool, default_str: "false", usage: "" },
    SchemaField { name: "tls_cipher_suites", flag: "", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "tls_min_version", flag: "", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "username", flag: "", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "password", flag: "", kind: FieldKind::Secret, default_str: "", usage: "" },
];

static UT_KV_MULTI_FIELDS: &[SchemaField] = &[
    SchemaField { name: "primary", flag: "", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "secondary", flag: "", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "mirror_enabled", flag: "", kind: FieldKind::Bool, default_str: "false", usage: "" },
    SchemaField { name: "mirror_timeout", flag: "", kind: FieldKind::Duration, default_str: "2s", usage: "" },
];

static UT_KV_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "consul", fields: UT_KV_CONSUL_FIELDS, blocks: &[] },
    SchemaBlock { name: "etcd", fields: UT_KV_ETCD_FIELDS, blocks: &[] },
    SchemaBlock { name: "multi", fields: UT_KV_MULTI_FIELDS, blocks: &[] },
];

static UT_KV_FIELDS: &[SchemaField] = &[
    SchemaField { name: "store", flag: "", kind: FieldKind::String, default_str: "memberlist", usage: "" },
    SchemaField { name: "prefix", flag: "", kind: FieldKind::String, default_str: "collectors/", usage: "" },
];

static UT_INSTANCE_RING_FIELDS: &[SchemaField] = &[
    SchemaField { name: "heartbeat_period", flag: "", kind: FieldKind::Duration, default_str: "15s", usage: "" },
    SchemaField { name: "heartbeat_timeout", flag: "", kind: FieldKind::Duration, default_str: "1m0s", usage: "" },
    SchemaField { name: "auto_forget_unhealthy_periods", flag: "", kind: FieldKind::Int, default_str: "4", usage: "" },
    SchemaField { name: "instance_id", flag: "", kind: FieldKind::String, default_str: "<hostname>", usage: "" },
    SchemaField { name: "instance_interface_names", flag: "", kind: FieldKind::StringList, default_str: "", usage: "" },
    SchemaField { name: "instance_port", flag: "", kind: FieldKind::Int, default_str: "0", usage: "" },
    SchemaField { name: "instance_addr", flag: "", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "instance_availability_zone", flag: "", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "instance_enable_ipv6", flag: "", kind: FieldKind::Bool, default_str: "false", usage: "" },
];

static UT_INSTANCE_RING_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "kvstore", fields: UT_KV_FIELDS, blocks: UT_KV_BLOCKS },
];

static UT_PARTITION_RING_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "kvstore", fields: UT_KV_FIELDS, blocks: UT_KV_BLOCKS },
];

static UT_KAFKA_FIELDS: &[SchemaField] = &[
    SchemaField { name: "backend", flag: "", kind: FieldKind::String, default_str: "kafka", usage: "" },
    SchemaField { name: "address", flag: "", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "topic", flag: "", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "client_id", flag: "", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "client_rack", flag: "", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "dial_timeout", flag: "", kind: FieldKind::Duration, default_str: "2s", usage: "" },
    SchemaField { name: "write_timeout", flag: "", kind: FieldKind::Duration, default_str: "10s", usage: "" },
    SchemaField { name: "write_timeout_overhead", flag: "", kind: FieldKind::Duration, default_str: "2s", usage: "" },
    SchemaField { name: "warpstream_health_check_slow_multiplier", flag: "", kind: FieldKind::Int, default_str: "2", usage: "" },
    SchemaField { name: "warpstream_health_check_max_slow_fraction", flag: "", kind: FieldKind::Float, default_str: "0.3", usage: "" },
    SchemaField { name: "warpstream_health_check_faulty_threshold", flag: "", kind: FieldKind::Float, default_str: "0.2", usage: "" },
    SchemaField { name: "warpstream_health_check_max_faulty_fraction", flag: "", kind: FieldKind::Float, default_str: "0.3", usage: "" },
    SchemaField { name: "warpstream_hedge_min_delay", flag: "", kind: FieldKind::Duration, default_str: "500ms", usage: "" },
    SchemaField { name: "warpstream_hedge_max_agents", flag: "", kind: FieldKind::Int, default_str: "3", usage: "" },
    SchemaField { name: "warpstream_demoter_probe_interval", flag: "", kind: FieldKind::Duration, default_str: "1s", usage: "" },
    SchemaField { name: "sasl_mechanism", flag: "", kind: FieldKind::String, default_str: "PLAIN", usage: "" },
    SchemaField { name: "sasl_username", flag: "", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "sasl_password", flag: "", kind: FieldKind::Secret, default_str: "", usage: "" },
    SchemaField { name: "sasl_oauthbearer_token", flag: "", kind: FieldKind::Secret, default_str: "", usage: "" },
    SchemaField { name: "sasl_oauthbearer_zid", flag: "", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "sasl_oauthbearer_extensions", flag: "", kind: FieldKind::Other, default_str: "{}", usage: "" },
    SchemaField { name: "sasl_oauthbearer_file_path", flag: "", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "sasl_oauthbearer_http_socket_path", flag: "", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "sasl_oauthbearer_http_socket_timeout", flag: "", kind: FieldKind::Duration, default_str: "10s", usage: "" },
    SchemaField { name: "sasl_msk_iam_access_key", flag: "", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "sasl_msk_iam_secret_key", flag: "", kind: FieldKind::Secret, default_str: "", usage: "" },
    SchemaField { name: "sasl_msk_iam_session_token", flag: "", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "sasl_msk_iam_user_agent", flag: "", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "sasl_msk_iam_file_path", flag: "", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "sasl_msk_iam_http_socket_path", flag: "", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "sasl_msk_iam_http_socket_timeout", flag: "", kind: FieldKind::Duration, default_str: "10s", usage: "" },
    SchemaField { name: "tls_enabled", flag: "", kind: FieldKind::Bool, default_str: "false", usage: "" },
    SchemaField { name: "tls_cert_path", flag: "", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "tls_key_path", flag: "", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "tls_ca_path", flag: "", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "tls_server_name", flag: "", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "tls_insecure_skip_verify", flag: "", kind: FieldKind::Bool, default_str: "false", usage: "" },
    SchemaField { name: "tls_cipher_suites", flag: "", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "tls_min_version", flag: "", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "consumer_group", flag: "", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "consumer_group_offset_commit_interval", flag: "", kind: FieldKind::Duration, default_str: "1s", usage: "" },
    SchemaField { name: "consumer_group_offset_commit_file_enforced", flag: "", kind: FieldKind::Bool, default_str: "false", usage: "" },
    SchemaField { name: "last_produced_offset_poll_interval", flag: "", kind: FieldKind::Duration, default_str: "1s", usage: "" },
    SchemaField { name: "last_produced_offset_retry_timeout", flag: "", kind: FieldKind::Duration, default_str: "10s", usage: "" },
    SchemaField { name: "consume_from_position_at_startup", flag: "", kind: FieldKind::String, default_str: "last-offset", usage: "" },
    SchemaField { name: "consume_from_timestamp_at_startup", flag: "", kind: FieldKind::Int, default_str: "0", usage: "" },
    SchemaField { name: "target_consumer_lag_at_startup", flag: "", kind: FieldKind::Duration, default_str: "2s", usage: "" },
    SchemaField { name: "max_consumer_lag_at_startup", flag: "", kind: FieldKind::Duration, default_str: "15s", usage: "" },
    SchemaField { name: "auto_create_topic_enabled", flag: "", kind: FieldKind::Bool, default_str: "true", usage: "" },
    SchemaField { name: "auto_create_topic_default_partitions", flag: "", kind: FieldKind::Int, default_str: "-1", usage: "" },
    SchemaField { name: "producer_max_record_size_bytes", flag: "", kind: FieldKind::Int, default_str: "15983616", usage: "" },
    SchemaField { name: "producer_max_buffered_bytes", flag: "", kind: FieldKind::Int, default_str: "1073741824", usage: "" },
    SchemaField { name: "producer_compression", flag: "", kind: FieldKind::String, default_str: "", usage: "" },
    SchemaField { name: "wait_strong_read_consistency_timeout", flag: "", kind: FieldKind::Duration, default_str: "20s", usage: "" },
    SchemaField { name: "producer_record_version", flag: "", kind: FieldKind::Int, default_str: "2", usage: "" },
    SchemaField { name: "fetch_max_wait", flag: "", kind: FieldKind::Duration, default_str: "5s", usage: "" },
    SchemaField { name: "fetch_concurrency_max", flag: "", kind: FieldKind::Int, default_str: "12", usage: "" },
    SchemaField { name: "use_compressed_bytes_as_fetch_max_bytes", flag: "", kind: FieldKind::Bool, default_str: "true", usage: "" },
    SchemaField { name: "max_buffered_bytes", flag: "", kind: FieldKind::Int, default_str: "1000000000", usage: "" },
    SchemaField { name: "ingestion_concurrency_max", flag: "", kind: FieldKind::Int, default_str: "8", usage: "" },
    SchemaField { name: "ingestion_concurrency_batch_size", flag: "", kind: FieldKind::Int, default_str: "150", usage: "" },
    SchemaField { name: "ingestion_concurrency_queue_capacity", flag: "", kind: FieldKind::Int, default_str: "3", usage: "" },
    SchemaField { name: "ingestion_concurrency_target_flushes_per_shard", flag: "", kind: FieldKind::Int, default_str: "40", usage: "" },
    SchemaField { name: "ingestion_concurrency_estimated_bytes_per_sample", flag: "", kind: FieldKind::Int, default_str: "200", usage: "" },
];

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
    SchemaField { name: "min_period", flag: "", kind: FieldKind::Duration, default_str: "100ms", usage: "" },
    SchemaField { name: "max_period", flag: "", kind: FieldKind::Duration, default_str: "10s", usage: "" },
    SchemaField { name: "max_retries", flag: "", kind: FieldKind::Int, default_str: "10", usage: "" },
];

static USAGE_TRACKER_FIELDS: &[SchemaField] = &[
    SchemaField { name: "enabled", flag: "usage-tracker.enabled", kind: FieldKind::Bool, default_str: "false", usage: "" },
    SchemaField { name: "partitions", flag: "usage-tracker.partitions", kind: FieldKind::Int, default_str: "64", usage: "" },
    SchemaField { name: "partition_reconcile_interval", flag: "", kind: FieldKind::Duration, default_str: "10s", usage: "" },
    SchemaField { name: "lost_partitions_shutdown_grace_period", flag: "", kind: FieldKind::Duration, default_str: "30s", usage: "" },
    SchemaField { name: "max_partitions_to_create_per_reconcile", flag: "", kind: FieldKind::Int, default_str: "1", usage: "" },
    SchemaField { name: "idle_timeout", flag: "", kind: FieldKind::Duration, default_str: "20m0s", usage: "" },
    SchemaField { name: "max_pending_created_series_events", flag: "", kind: FieldKind::Int, default_str: "10000", usage: "" },
    SchemaField { name: "created_series_events_max_batch_size_bytes", flag: "", kind: FieldKind::Int, default_str: "1048576", usage: "" },
    SchemaField { name: "created_series_events_batch_ttl", flag: "", kind: FieldKind::Duration, default_str: "250ms", usage: "" },
    SchemaField { name: "created_series_events_publish_concurrency", flag: "", kind: FieldKind::Int, default_str: "10", usage: "" },
    SchemaField { name: "skip_snapshot_loading_at_startup", flag: "", kind: FieldKind::Bool, default_str: "false", usage: "" },
    SchemaField { name: "snapshot_interval_jitter", flag: "", kind: FieldKind::Float, default_str: "0.1", usage: "" },
    SchemaField { name: "target_snapshot_file_size_bytes", flag: "", kind: FieldKind::Int, default_str: "104857600", usage: "" },
    SchemaField { name: "snapshot_cleanup_interval", flag: "", kind: FieldKind::Duration, default_str: "1h0m0s", usage: "" },
    SchemaField { name: "snapshot_cleanup_interval_jitter", flag: "", kind: FieldKind::Float, default_str: "0.25", usage: "" },
    SchemaField { name: "max_events_fetch_size", flag: "", kind: FieldKind::Int, default_str: "100", usage: "" },
    SchemaField { name: "user_close_to_limit_percentage_threshold", flag: "", kind: FieldKind::Int, default_str: "90", usage: "" },
    SchemaField { name: "enable_verbose_series_creation_deletion_prometheus_metrics", flag: "", kind: FieldKind::Bool, default_str: "false", usage: "" },
    SchemaField { name: "min_time_between_shards_cleanup", flag: "", kind: FieldKind::Duration, default_str: "25ms", usage: "" },
    SchemaField { name: "tenantshard_impl_version", flag: "", kind: FieldKind::Int, default_str: "2", usage: "" },
    SchemaField { name: "num_shards", flag: "", kind: FieldKind::Int, default_str: "16", usage: "" },
];

static USAGE_TRACKER_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "instance_ring", fields: UT_INSTANCE_RING_FIELDS, blocks: UT_INSTANCE_RING_BLOCKS },
    SchemaBlock { name: "partition_ring", fields: &[], blocks: UT_PARTITION_RING_BLOCKS },
    SchemaBlock { name: "events_storage_writer", fields: UT_KAFKA_FIELDS, blocks: &[] },
    SchemaBlock { name: "events_storage_reader", fields: UT_KAFKA_FIELDS, blocks: &[] },
    SchemaBlock { name: "snapshots_metadata_writer", fields: UT_KAFKA_FIELDS, blocks: &[] },
    SchemaBlock { name: "snapshots_metadata_reader", fields: UT_KAFKA_FIELDS, blocks: &[] },
    SchemaBlock { name: "snapshots_storage", fields: UT_SNAPSHOTS_STORAGE_FIELDS, blocks: UT_SNAPSHOTS_STORAGE_BLOCKS },
    SchemaBlock { name: "snapshots_load_backoff", fields: UT_LOAD_BACKOFF_FIELDS, blocks: &[] },
];

pub static HIDDEN_TOP_BLOCKS: &[SchemaBlock] = &[
    SchemaBlock { name: "block_builder", fields: BLOCK_BUILDER_FIELDS, blocks: BLOCK_BUILDER_BLOCKS },
    SchemaBlock { name: "block_builder_scheduler", fields: BLOCK_BUILDER_SCHEDULER_FIELDS, blocks: &[] },
    SchemaBlock { name: "usage_tracker", fields: USAGE_TRACKER_FIELDS, blocks: USAGE_TRACKER_BLOCKS },
];
