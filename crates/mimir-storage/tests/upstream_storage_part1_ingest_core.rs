//! 1:1 upstream test ports for `pkg/storage/ingest/` (core/non-TCP-broker files: 8 files, 39 tests).

use std::collections::{BinaryHeap, HashMap, HashSet};
use std::fs;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use mimir_common::logging::{format_logfmt_line, LogLevel, Logger};
use mimir_config::{
    GoDuration, IngestStorageConfig, KafkaConfig, OrderedConsumptionConfig, SASLMechanism,
    StringSliceCSV,
};
use mimir_proto::ringpb::{PartitionRingDesc, PartitionState};
use mimir_storage::ingest::heap_merger::MergedBatchConsumer;
use mimir_storage::ingest::{
    add_partition_with_derived_tokens, add_partition_with_stored_tokens, divide_budget,
    get_consumer_group, merge_topics_partitions_offsets, to_warpstream_client_options,
    with_read_compartment_suffix, write_compartment_config, write_compartment_configs,
    FetchedRecord, FixtureConfig, FixtureGenerator, FixtureTenantClassConfig, HeapMerger,
    HeapMergerMetrics, InMemoryPartitionKv, KafkaLogger, MergeableRecord, OffsetFile,
    OffsetWatcherError, PartitionOffsetWatcher, PartitionOffsets, PartitionRingSnapshot,
    PartitionRingWatchers, PartitionTokenTable, TopicsPartitionsOffsets,
    DEFAULT_METADATA_REFRESH_INTERVAL, DEFAULT_PRODUCER_LINGER,
    MAX_PRODUCER_RECORD_DATA_BYTES_LIMIT, MIN_PRODUCER_RECORD_DATA_BYTES_LIMIT,
    OFFSET_FILE_VERSION, PRODUCER_BATCH_MAX_BYTES,
};
use parking_lot::Mutex as SyncMutex;
use prometheus::{Registry, TextEncoder};

// ===========================================================================
// pkg/storage/ingest/config_test.go (7 tests)
// ===========================================================================

// Upstream: pkg/storage/ingest/config_test.go::TestConfig_Validate
#[test]
fn test_config_validate() {
    type SetupFn = fn(&mut IngestStorageConfig);
    struct Case {
        name: &'static str,
        setup: SetupFn,
        expected_err: Option<&'static str>,
    }

    let mut cases: Vec<Case> = vec![
        Case {
            name: "should pass with the default config",
            setup: |_| {},
            expected_err: None,
        },
        Case {
            name: "should fail if ingest storage is enabled and Kafka address is not configured",
            setup: |cfg| {
                cfg.enabled = true;
                cfg.kafka.topic = "test".to_string();
            },
            expected_err: Some("the Kafka address has not been configured"),
        },
        Case {
            name: "should fail if ingest storage is enabled and Kafka topic is not configured",
            setup: |cfg| {
                cfg.enabled = true;
                cfg.kafka.address = StringSliceCSV(vec!["localhost".to_string()]);
            },
            expected_err: Some("the Kafka topic has not been configured"),
        },
        Case {
            name: "should pass if ingest storage is enabled and required config is set",
            setup: |cfg| {
                cfg.enabled = true;
                cfg.kafka.address = StringSliceCSV(vec!["localhost".to_string()]);
                cfg.kafka.topic = "test".to_string();
            },
            expected_err: None,
        },
        Case {
            name: "should pass if backend is explicitly set to warpstream",
            setup: |cfg| {
                cfg.enabled = true;
                cfg.kafka.address = StringSliceCSV(vec!["localhost".to_string()]);
                cfg.kafka.topic = "test".to_string();
                cfg.kafka.backend = "warpstream".to_string();
            },
            expected_err: None,
        },
        Case {
            name: "should fail if backend is warpstream and write timeout < 2 * overhead",
            setup: |cfg| {
                cfg.enabled = true;
                cfg.kafka.address = StringSliceCSV(vec!["localhost".to_string()]);
                cfg.kafka.topic = "test".to_string();
                cfg.kafka.backend = "warpstream".to_string();
                cfg.kafka.write_timeout = GoDuration::from_millis(3999);
            },
            expected_err: Some("write-timeout must be greater than or equal to twice write-timeout-overhead"),
        },
        Case {
            name: "should pass if backend is warpstream and write timeout == 2 * overhead",
            setup: |cfg| {
                cfg.enabled = true;
                cfg.kafka.address = StringSliceCSV(vec!["localhost".to_string()]);
                cfg.kafka.topic = "test".to_string();
                cfg.kafka.backend = "warpstream".to_string();
                cfg.kafka.write_timeout = GoDuration::from_secs(4);
            },
            expected_err: None,
        },
        Case {
            name: "should pass if backend is warpstream and write timeout > 2 * overhead",
            setup: |cfg| {
                cfg.enabled = true;
                cfg.kafka.address = StringSliceCSV(vec!["localhost".to_string()]);
                cfg.kafka.topic = "test".to_string();
                cfg.kafka.backend = "warpstream".to_string();
                cfg.kafka.write_timeout = GoDuration::from_millis(4001);
            },
            expected_err: None,
        },
        Case {
            name: "should honor custom write timeout overhead for warpstream floor",
            setup: |cfg| {
                cfg.enabled = true;
                cfg.kafka.address = StringSliceCSV(vec!["localhost".to_string()]);
                cfg.kafka.topic = "test".to_string();
                cfg.kafka.backend = "warpstream".to_string();
                cfg.kafka.write_timeout_overhead = GoDuration::from_millis(500);
                cfg.kafka.write_timeout = GoDuration::from_millis(999);
            },
            expected_err: Some("write-timeout must be greater than or equal to twice write-timeout-overhead"),
        },
        Case {
            name: "should fail if write timeout overhead is below minimum",
            setup: |cfg| {
                cfg.enabled = true;
                cfg.kafka.address = StringSliceCSV(vec!["localhost".to_string()]);
                cfg.kafka.topic = "test".to_string();
                cfg.kafka.write_timeout_overhead = GoDuration::from_millis(99);
            },
            expected_err: Some("write-timeout-overhead must be at least 100ms"),
        },
        Case {
            name: "should pass if write timeout overhead equals minimum",
            setup: |cfg| {
                cfg.enabled = true;
                cfg.kafka.address = StringSliceCSV(vec!["localhost".to_string()]);
                cfg.kafka.topic = "test".to_string();
                cfg.kafka.write_timeout_overhead = GoDuration::from_millis(100);
            },
            expected_err: None,
        },
        Case {
            name: "should not enforce warpstream write timeout floor for kafka backend",
            setup: |cfg| {
                cfg.enabled = true;
                cfg.kafka.address = StringSliceCSV(vec!["localhost".to_string()]);
                cfg.kafka.topic = "test".to_string();
                cfg.kafka.write_timeout = GoDuration::from_secs(1);
            },
            expected_err: None,
        },
        Case {
            name: "should fail if backend is empty",
            setup: |cfg| {
                cfg.enabled = true;
                cfg.kafka.address = StringSliceCSV(vec!["localhost".to_string()]);
                cfg.kafka.topic = "test".to_string();
                cfg.kafka.backend = String::new();
            },
            expected_err: Some("the configured kafka backend is invalid"),
        },
        Case {
            name: "should fail if backend is unknown",
            setup: |cfg| {
                cfg.enabled = true;
                cfg.kafka.address = StringSliceCSV(vec!["localhost".to_string()]);
                cfg.kafka.topic = "test".to_string();
                cfg.kafka.backend = "confluent".to_string();
            },
            expected_err: Some("the configured kafka backend is invalid"),
        },
        Case {
            name: "should fail if backend value is not lowercase",
            setup: |cfg| {
                cfg.enabled = true;
                cfg.kafka.address = StringSliceCSV(vec!["localhost".to_string()]);
                cfg.kafka.topic = "test".to_string();
                cfg.kafka.backend = "Kafka".to_string();
            },
            expected_err: Some("the configured kafka backend is invalid"),
        },
        Case {
            name: "should fail if consume position is invalid",
            setup: |cfg| {
                cfg.enabled = true;
                cfg.kafka.address = StringSliceCSV(vec!["localhost".to_string()]);
                cfg.kafka.topic = "test".to_string();
                cfg.kafka.consume_from_position_at_startup = "middle".to_string();
            },
            expected_err: Some("the configured consume position is invalid"),
        },
        Case {
            name: "should fail if consume timestamp set when position is not timestamp",
            setup: |cfg| {
                cfg.enabled = true;
                cfg.kafka.address = StringSliceCSV(vec!["localhost".to_string()]);
                cfg.kafka.topic = "test".to_string();
                cfg.kafka.consume_from_position_at_startup = "end".to_string();
                cfg.kafka.consume_from_timestamp_at_startup = 1_700_000_000_000;
            },
            expected_err: Some("configured consume position must be set to \"timestamp\""),
        },
        Case {
            name: "should fail if consume position is timestamp but timestamp is invalid",
            setup: |cfg| {
                cfg.enabled = true;
                cfg.kafka.address = StringSliceCSV(vec!["localhost".to_string()]);
                cfg.kafka.topic = "test".to_string();
                cfg.kafka.consume_from_position_at_startup = "timestamp".to_string();
                cfg.kafka.consume_from_timestamp_at_startup = 0;
            },
            expected_err: Some("configured timestamp must be a millisecond timestamp"),
        },
        Case {
            name: "should fail if producer max record size bytes too low",
            setup: |cfg| {
                cfg.enabled = true;
                cfg.kafka.address = StringSliceCSV(vec!["localhost".to_string()]);
                cfg.kafka.topic = "test".to_string();
                cfg.kafka.producer_max_record_size_bytes = MIN_PRODUCER_RECORD_DATA_BYTES_LIMIT as i64 - 1;
            },
            expected_err: Some("producer max record size bytes must be a value between"),
        },
        Case {
            name: "should fail if producer max record size bytes too high",
            setup: |cfg| {
                cfg.enabled = true;
                cfg.kafka.address = StringSliceCSV(vec!["localhost".to_string()]);
                cfg.kafka.topic = "test".to_string();
                cfg.kafka.producer_max_record_size_bytes = MAX_PRODUCER_RECORD_DATA_BYTES_LIMIT as i64 + 1;
            },
            expected_err: Some("producer max record size bytes must be a value between"),
        },
        Case {
            name: "should pass if producer compression is none",
            setup: |cfg| {
                cfg.enabled = true;
                cfg.kafka.address = StringSliceCSV(vec!["localhost".to_string()]);
                cfg.kafka.topic = "test".to_string();
                cfg.kafka.producer_compression = "none".to_string();
            },
            expected_err: None,
        },
        Case {
            name: "should pass if producer compression is zstd",
            setup: |cfg| {
                cfg.enabled = true;
                cfg.kafka.address = StringSliceCSV(vec!["localhost".to_string()]);
                cfg.kafka.topic = "test".to_string();
                cfg.kafka.producer_compression = "zstd".to_string();
            },
            expected_err: None,
        },
        Case {
            name: "should fail if producer compression is unsupported",
            setup: |cfg| {
                cfg.enabled = true;
                cfg.kafka.address = StringSliceCSV(vec!["localhost".to_string()]);
                cfg.kafka.topic = "test".to_string();
                cfg.kafka.producer_compression = "brotli".to_string();
            },
            expected_err: Some("the configured Kafka producer compression codec is invalid"),
        },
        Case {
            name: "should fail if target consumer lag enabled but max consumer lag is 0",
            setup: |cfg| {
                cfg.enabled = true;
                cfg.kafka.address = StringSliceCSV(vec!["localhost".to_string()]);
                cfg.kafka.topic = "test".to_string();
                cfg.kafka.target_consumer_lag_at_startup = GoDuration::from_secs(2);
                cfg.kafka.max_consumer_lag_at_startup = GoDuration(0);
            },
            expected_err: Some("target and max consumer lag"),
        },
        Case {
            name: "should fail if max consumer lag enabled but target consumer lag is 0",
            setup: |cfg| {
                cfg.enabled = true;
                cfg.kafka.address = StringSliceCSV(vec!["localhost".to_string()]);
                cfg.kafka.topic = "test".to_string();
                cfg.kafka.target_consumer_lag_at_startup = GoDuration(0);
                cfg.kafka.max_consumer_lag_at_startup = GoDuration::from_secs(2);
            },
            expected_err: Some("target and max consumer lag"),
        },
        Case {
            name: "should fail if target consumer lag > max consumer lag",
            setup: |cfg| {
                cfg.enabled = true;
                cfg.kafka.address = StringSliceCSV(vec!["localhost".to_string()]);
                cfg.kafka.topic = "test".to_string();
                cfg.kafka.target_consumer_lag_at_startup = GoDuration::from_secs(2);
                cfg.kafka.max_consumer_lag_at_startup = GoDuration::from_secs(1);
            },
            expected_err: Some("max consumer lag at startup must greater or equal"),
        },
        Case {
            name: "should fail if SASL username set without password",
            setup: |cfg| {
                cfg.enabled = true;
                cfg.kafka.address = StringSliceCSV(vec!["localhost".to_string()]);
                cfg.kafka.topic = "test".to_string();
                cfg.kafka.sasl_username = "mimir".to_string();
            },
            expected_err: Some("SASL username and password must be both configured"),
        },
        Case {
            name: "should fail if SASL password set without username",
            setup: |cfg| {
                cfg.enabled = true;
                cfg.kafka.address = StringSliceCSV(vec!["localhost".to_string()]);
                cfg.kafka.topic = "test".to_string();
                cfg.kafka.sasl_password = "supersecret".to_string();
            },
            expected_err: Some("SASL username and password must be both configured"),
        },
        Case {
            name: "should pass if both SASL username and password configured",
            setup: |cfg| {
                cfg.enabled = true;
                cfg.kafka.address = StringSliceCSV(vec!["localhost".to_string()]);
                cfg.kafka.topic = "test".to_string();
                cfg.kafka.sasl_username = "mimir".to_string();
                cfg.kafka.sasl_password = "supersecret".to_string();
            },
            expected_err: None,
        },
        Case {
            name: "should fail if AWS_MSK_IAM has no credentials configured",
            setup: |cfg| {
                cfg.enabled = true;
                cfg.kafka.address = StringSliceCSV(vec!["localhost".to_string()]);
                cfg.kafka.topic = "test".to_string();
                cfg.kafka.sasl_mechanism = SASLMechanism::MskIam;
            },
            expected_err: Some("AWS_MSK_IAM"),
        },
        Case {
            name: "should fail if AWS_MSK_IAM has only access key",
            setup: |cfg| {
                cfg.enabled = true;
                cfg.kafka.address = StringSliceCSV(vec!["localhost".to_string()]);
                cfg.kafka.topic = "test".to_string();
                cfg.kafka.sasl_mechanism = SASLMechanism::MskIam;
                cfg.kafka.sasl_msk_iam_access_key = "AKID".to_string();
            },
            expected_err: Some("both access key and secret key must be configured"),
        },
        Case {
            name: "should fail if AWS_MSK_IAM has both file path and HTTP socket path",
            setup: |cfg| {
                cfg.enabled = true;
                cfg.kafka.address = StringSliceCSV(vec!["localhost".to_string()]);
                cfg.kafka.topic = "test".to_string();
                cfg.kafka.sasl_mechanism = SASLMechanism::MskIam;
                cfg.kafka.sasl_msk_iam_file_path = "/path/to/creds.json".to_string();
                cfg.kafka.sasl_msk_iam_http_socket_path = "/tmp/aws.sock".to_string();
            },
            expected_err: Some("AWS_MSK_IAM"),
        },
        Case {
            name: "should succeed if AWS_MSK_IAM has static credentials",
            setup: |cfg| {
                cfg.enabled = true;
                cfg.kafka.address = StringSliceCSV(vec!["localhost".to_string()]);
                cfg.kafka.topic = "test".to_string();
                cfg.kafka.sasl_mechanism = SASLMechanism::MskIam;
                cfg.kafka.sasl_msk_iam_access_key = "AKID".to_string();
                cfg.kafka.sasl_msk_iam_secret_key = "secret".to_string();
            },
            expected_err: None,
        },
        Case {
            name: "should succeed if AWS_MSK_IAM has file path",
            setup: |cfg| {
                cfg.enabled = true;
                cfg.kafka.address = StringSliceCSV(vec!["localhost".to_string()]);
                cfg.kafka.topic = "test".to_string();
                cfg.kafka.sasl_mechanism = SASLMechanism::MskIam;
                cfg.kafka.sasl_msk_iam_file_path = "/path/to/creds.json".to_string();
            },
            expected_err: None,
        },
        Case {
            name: "should succeed if AWS_MSK_IAM has HTTP socket path",
            setup: |cfg| {
                cfg.enabled = true;
                cfg.kafka.address = StringSliceCSV(vec!["localhost".to_string()]);
                cfg.kafka.topic = "test".to_string();
                cfg.kafka.sasl_mechanism = SASLMechanism::MskIam;
                cfg.kafka.sasl_msk_iam_http_socket_path = "/tmp/aws.sock".to_string();
            },
            expected_err: None,
        },
        Case {
            name: "should fail if AWS_MSK_IAM has both static credentials and file path",
            setup: |cfg| {
                cfg.enabled = true;
                cfg.kafka.address = StringSliceCSV(vec!["localhost".to_string()]);
                cfg.kafka.topic = "test".to_string();
                cfg.kafka.sasl_mechanism = SASLMechanism::MskIam;
                cfg.kafka.sasl_msk_iam_access_key = "AKID".to_string();
                cfg.kafka.sasl_msk_iam_secret_key = "secret".to_string();
                cfg.kafka.sasl_msk_iam_file_path = "/path/to/creds.json".to_string();
            },
            expected_err: Some("AWS_MSK_IAM"),
        },
        Case {
            name: "should fail if OAUTHBEARER has no token source",
            setup: |cfg| {
                cfg.enabled = true;
                cfg.kafka.address = StringSliceCSV(vec!["localhost".to_string()]);
                cfg.kafka.topic = "test".to_string();
                cfg.kafka.sasl_mechanism = SASLMechanism::Oauthbearer;
            },
            expected_err: Some("OAUTHBEARER"),
        },
        Case {
            name: "should fail if OAUTHBEARER has both token and file path",
            setup: |cfg| {
                cfg.enabled = true;
                cfg.kafka.address = StringSliceCSV(vec!["localhost".to_string()]);
                cfg.kafka.topic = "test".to_string();
                cfg.kafka.sasl_mechanism = SASLMechanism::Oauthbearer;
                cfg.kafka.sasl_oauthbearer_token = "foo".to_string();
                cfg.kafka.sasl_oauthbearer_file_path = "bar".to_string();
            },
            expected_err: Some("OAUTHBEARER"),
        },
        Case {
            name: "should succeed if OAUTHBEARER has token",
            setup: |cfg| {
                cfg.enabled = true;
                cfg.kafka.address = StringSliceCSV(vec!["localhost".to_string()]);
                cfg.kafka.topic = "test".to_string();
                cfg.kafka.sasl_mechanism = SASLMechanism::Oauthbearer;
                cfg.kafka.sasl_oauthbearer_token = "foo".to_string();
            },
            expected_err: None,
        },
        Case {
            name: "should succeed if OAUTHBEARER has file path",
            setup: |cfg| {
                cfg.enabled = true;
                cfg.kafka.address = StringSliceCSV(vec!["localhost".to_string()]);
                cfg.kafka.topic = "test".to_string();
                cfg.kafka.sasl_mechanism = SASLMechanism::Oauthbearer;
                cfg.kafka.sasl_oauthbearer_file_path = "foo".to_string();
            },
            expected_err: None,
        },
        Case {
            name: "should succeed if OAUTHBEARER has HTTP socket path",
            setup: |cfg| {
                cfg.enabled = true;
                cfg.kafka.address = StringSliceCSV(vec!["localhost".to_string()]);
                cfg.kafka.topic = "test".to_string();
                cfg.kafka.sasl_mechanism = SASLMechanism::Oauthbearer;
                cfg.kafka.sasl_oauthbearer_http_socket_path = "/tmp/oauth.sock".to_string();
            },
            expected_err: None,
        },
        Case {
            name: "should fail if OAUTHBEARER has both file path and HTTP socket path",
            setup: |cfg| {
                cfg.enabled = true;
                cfg.kafka.address = StringSliceCSV(vec!["localhost".to_string()]);
                cfg.kafka.topic = "test".to_string();
                cfg.kafka.sasl_mechanism = SASLMechanism::Oauthbearer;
                cfg.kafka.sasl_oauthbearer_file_path = "foo".to_string();
                cfg.kafka.sasl_oauthbearer_http_socket_path = "/tmp/oauth.sock".to_string();
            },
            expected_err: Some("OAUTHBEARER"),
        },
        Case {
            name: "should fail if OAUTHBEARER has both token and HTTP socket path",
            setup: |cfg| {
                cfg.enabled = true;
                cfg.kafka.address = StringSliceCSV(vec!["localhost".to_string()]);
                cfg.kafka.topic = "test".to_string();
                cfg.kafka.sasl_mechanism = SASLMechanism::Oauthbearer;
                cfg.kafka.sasl_oauthbearer_token = "foo".to_string();
                cfg.kafka.sasl_oauthbearer_http_socket_path = "/tmp/oauth.sock".to_string();
            },
            expected_err: Some("OAUTHBEARER"),
        },
        Case {
            name: "should fail if max ingestion concurrency < 0",
            setup: |cfg| {
                cfg.enabled = true;
                cfg.kafka.address = StringSliceCSV(vec!["localhost".to_string()]);
                cfg.kafka.topic = "test".to_string();
                cfg.kafka.ingestion_concurrency_max = -1;
            },
            expected_err: Some("ingestion-concurrency-max"),
        },
        Case {
            name: "should pass if max ingestion concurrency is 0",
            setup: |cfg| {
                cfg.enabled = true;
                cfg.kafka.address = StringSliceCSV(vec!["localhost".to_string()]);
                cfg.kafka.topic = "test".to_string();
                cfg.kafka.ingestion_concurrency_max = 0;
            },
            expected_err: None,
        },
        Case {
            name: "should fail if ingestion concurrency batch size < 0",
            setup: |cfg| {
                cfg.enabled = true;
                cfg.kafka.address = StringSliceCSV(vec!["localhost".to_string()]);
                cfg.kafka.topic = "test".to_string();
                cfg.kafka.ingestion_concurrency_max = 5;
                cfg.kafka.ingestion_concurrency_batch_size = -1;
            },
            expected_err: Some("ingestion-concurrency-* params"),
        },
        Case {
            name: "should fail if ingestion concurrency queue capacity < 0",
            setup: |cfg| {
                cfg.enabled = true;
                cfg.kafka.address = StringSliceCSV(vec!["localhost".to_string()]);
                cfg.kafka.topic = "test".to_string();
                cfg.kafka.ingestion_concurrency_max = 5;
                cfg.kafka.ingestion_concurrency_queue_capacity = -1;
            },
            expected_err: Some("ingestion-concurrency-* params"),
        },
        Case {
            name: "should fail if ingestion concurrency estimated bytes per sample < 0",
            setup: |cfg| {
                cfg.enabled = true;
                cfg.kafka.address = StringSliceCSV(vec!["localhost".to_string()]);
                cfg.kafka.topic = "test".to_string();
                cfg.kafka.ingestion_concurrency_max = 5;
                cfg.kafka.ingestion_concurrency_estimated_bytes_per_sample = -1;
            },
            expected_err: Some("ingestion-concurrency-* params"),
        },
        Case {
            name: "should fail if ingestion concurrency target flushes per shard < 0",
            setup: |cfg| {
                cfg.enabled = true;
                cfg.kafka.address = StringSliceCSV(vec!["localhost".to_string()]);
                cfg.kafka.topic = "test".to_string();
                cfg.kafka.ingestion_concurrency_max = 5;
                cfg.kafka.ingestion_concurrency_target_flushes_per_shard = -1;
            },
            expected_err: Some("ingestion-concurrency-* params"),
        },
        Case {
            name: "should fail when auto create topic default partitions < 1 (and != -1)",
            setup: |cfg| {
                cfg.enabled = true;
                cfg.kafka.address = StringSliceCSV(vec!["localhost".to_string()]);
                cfg.kafka.topic = "test".to_string();
                cfg.kafka.auto_create_topic_enabled = true;
                cfg.kafka.auto_create_topic_default_partitions = -100;
            },
            expected_err: Some("auto-create-topic-default-partitions must be -1 or greater than 0"),
        },
        Case {
            name: "should pass when auto create topic default partitions is -1",
            setup: |cfg| {
                cfg.enabled = true;
                cfg.kafka.address = StringSliceCSV(vec!["localhost".to_string()]);
                cfg.kafka.topic = "test".to_string();
                cfg.kafka.auto_create_topic_enabled = true;
                cfg.kafka.auto_create_topic_default_partitions = -1;
            },
            expected_err: None,
        },
        Case {
            name: "should fail if Kafka fetch max wait < 5s",
            setup: |cfg| {
                cfg.enabled = true;
                cfg.kafka.address = StringSliceCSV(vec!["localhost".to_string()]);
                cfg.kafka.topic = "test".to_string();
                cfg.kafka.fetch_max_wait = GoDuration::from_secs(2);
            },
            expected_err: Some("Kafka fetch max wait must be between 5s and 30s"),
        },
        Case {
            name: "should fail if Kafka fetch max wait > 30s",
            setup: |cfg| {
                cfg.enabled = true;
                cfg.kafka.address = StringSliceCSV(vec!["localhost".to_string()]);
                cfg.kafka.topic = "test".to_string();
                cfg.kafka.fetch_max_wait = GoDuration::from_secs(32);
            },
            expected_err: Some("Kafka fetch max wait must be between 5s and 30s"),
        },
        Case {
            name: "should fail if fsync concurrency < 1",
            setup: |cfg| {
                cfg.enabled = true;
                cfg.write_logs_fsync_before_kafka_commit_concurrency = 0;
            },
            expected_err: Some("must be at least 1"),
        },
    ];

    for mech in [SASLMechanism::ScramSha256, SASLMechanism::ScramSha512] {
        for (missing, setup) in [
            ("username", (|cfg: &mut IngestStorageConfig| cfg.kafka.sasl_password = "supersecret".to_string()) as SetupFn),
            ("password", (|cfg: &mut IngestStorageConfig| cfg.kafka.sasl_username = "mimir".to_string()) as SetupFn),
            ("username and password", (|_: &mut IngestStorageConfig| {}) as SetupFn),
        ] {
            let mut cfg = IngestStorageConfig {
                enabled: true,
                ..Default::default()
            };
            cfg.kafka.address = StringSliceCSV(vec!["localhost".to_string()]);
            cfg.kafka.topic = "test".to_string();
            cfg.kafka.sasl_mechanism = mech;
            setup(&mut cfg);
            let err = cfg
                .validate()
                .expect_err(&format!("missing {missing} for {mech:?}"));
            assert!(err
                .to_string()
                .contains("SASL username and password must be both configured"));
        }
    }

    for tc in cases.drain(..) {
        let mut cfg = IngestStorageConfig::default();
        (tc.setup)(&mut cfg);
        let res = cfg.validate();
        match tc.expected_err {
            None => assert!(res.is_ok(), "{}: unexpected error {:?}", tc.name, res),
            Some(sub) => {
                let err = res.expect_err(&format!("{}: expected error", tc.name));
                assert!(
                    err.to_string().contains(sub),
                    "{}: expected {:?} in {:?}",
                    tc.name,
                    sub,
                    err.to_string()
                );
            }
        }
    }
}

// Upstream: pkg/storage/ingest/config_test.go::TestConfig_GetConsumerGroup
#[test]
fn test_config_get_consumer_group() {
    let cases = [
        ("", "ingester-zone-a-1", 1, "ingester-zone-a-1"),
        ("ingester-a", "ingester-zone-a-1", 1, "ingester-a"),
        (
            "ingester-zone-a-partition-<partition>",
            "ingester-zone-a-1",
            1,
            "ingester-zone-a-partition-1",
        ),
    ];

    for (cg, instance_id, partition_id, expected) in cases {
        let cfg = KafkaConfig {
            consumer_group: cg.to_string(),
            ..Default::default()
        };
        assert_eq!(get_consumer_group(&cfg, instance_id, partition_id), expected);
    }
}

// Upstream: pkg/storage/ingest/config_test.go::TestExhaustiveSASLMechanismOptions
#[test]
fn test_exhaustive_sasl_mechanism_options() {
    let options = [
        ("PLAIN", SASLMechanism::Plain),
        ("SCRAM-SHA-256", SASLMechanism::ScramSha256),
        ("SCRAM-SHA-512", SASLMechanism::ScramSha512),
        ("OAUTHBEARER", SASLMechanism::Oauthbearer),
        ("AWS_MSK_IAM", SASLMechanism::MskIam),
    ];

    for (s, mech) in options {
        let parsed = SASLMechanism::parse(s).unwrap();
        assert_eq!(parsed, mech);
        let cfg = KafkaConfig {
            address: StringSliceCSV(vec!["localhost".to_string()]),
            topic: "test".to_string(),
            sasl_mechanism: mech,
            ..Default::default()
        };
        // Validate must not complain about invalid mechanism.
        if let Err(err) = cfg.validate() {
            assert!(!err.to_string().contains("invalid SASL mechanism"));
        }
    }
}

// Upstream: pkg/storage/ingest/config_test.go::TestKafkaConfig_WriteCompartmentConfig
#[test]
fn test_kafka_config_write_compartment_config() {
    let base = KafkaConfig {
        address: StringSliceCSV(vec!["kafka-wc-<write-compartment-id>:9092".to_string()]),
        topic: "ingest-rc-<read-compartment-id>".to_string(),
        sasl_username: "user-wc-<write-compartment-id>".to_string(),
        sasl_password: "pass-wc-<write-compartment-id>".to_string(),
        ..Default::default()
    };

    for wc in 0..3 {
        let cfg = write_compartment_config(&base, wc);
        assert_eq!(cfg.address.0, vec![format!("kafka-wc-{wc}:9092")]);
        assert_eq!(cfg.sasl_username, format!("user-wc-{wc}"));
        assert_eq!(cfg.sasl_password, format!("pass-wc-{wc}"));
        assert_eq!(cfg.topic, "ingest-rc-<read-compartment-id>");
        assert_eq!(cfg.sasl_mechanism, base.sasl_mechanism);
    }
}

// Upstream: pkg/storage/ingest/config_test.go::TestWriteCompartmentConfigs
#[test]
fn test_write_compartment_configs() {
    let new_base = || KafkaConfig {
        address: StringSliceCSV(vec!["kafka-wc-<write-compartment-id>:9092".to_string()]),
        topic: "ingest-rc-<read-compartment-id>".to_string(),
        sasl_username: "user-wc-<write-compartment-id>".to_string(),
        sasl_password: "pass-wc-<write-compartment-id>".to_string(),
        fetch_concurrency_max: 12,
        max_buffered_bytes: 1_000_000_000,
        ingestion_concurrency_max: 8,
        ..Default::default()
    };

    // Resolves each cluster, applies topic, divides per-client budgets.
    {
        let num_compartments = 4;
        let cfgs = write_compartment_configs(&new_base(), num_compartments, "ingest-rc-2");
        assert_eq!(cfgs.len(), num_compartments);
        for (wc, cfg) in cfgs.iter().enumerate() {
            assert_eq!(cfg.address.0, vec![format!("kafka-wc-{wc}:9092")]);
            assert_eq!(cfg.sasl_username, format!("user-wc-{wc}"));
            assert_eq!(cfg.sasl_password, format!("pass-wc-{wc}"));
            assert_eq!(cfg.topic, "ingest-rc-2");
            assert_eq!(cfg.fetch_concurrency_max, 12 / 4);
            assert_eq!(cfg.max_buffered_bytes, 1_000_000_000 / 4);
            assert_eq!(cfg.ingestion_concurrency_max, 8 / 4);
        }
    }

    // Single compartment keeps full budget.
    {
        let cfgs = write_compartment_configs(&new_base(), 1, "ingest-rc-0");
        assert_eq!(cfgs.len(), 1);
        assert_eq!(cfgs[0].fetch_concurrency_max, 12);
        assert_eq!(cfgs[0].max_buffered_bytes, 1_000_000_000);
        assert_eq!(cfgs[0].ingestion_concurrency_max, 8);
    }

    // Positive budget never collapses to zero.
    {
        let mut base = new_base();
        base.fetch_concurrency_max = 3;
        let cfgs = write_compartment_configs(&base, 8, "ingest-rc-0");
        assert_eq!(cfgs.len(), 8);
        for cfg in cfgs {
            assert_eq!(cfg.fetch_concurrency_max, 1);
        }
    }

    // Disabled budget stays disabled.
    {
        let mut base = new_base();
        base.fetch_concurrency_max = 0;
        base.max_buffered_bytes = 0;
        base.ingestion_concurrency_max = 0;
        let cfgs = write_compartment_configs(&base, 4, "ingest-rc-0");
        assert_eq!(cfgs.len(), 4);
        for cfg in cfgs {
            assert_eq!(cfg.fetch_concurrency_max, 0);
            assert_eq!(cfg.max_buffered_bytes, 0);
            assert_eq!(cfg.ingestion_concurrency_max, 0);
        }
    }
}

// Upstream: pkg/storage/ingest/config_test.go::TestDivideBudget
#[test]
fn test_divide_budget() {
    let cases = [
        ("divides evenly", 12, 4, 3),
        ("floors to integer division", 10, 3, 3),
        ("single compartment keeps full budget", 12, 1, 12),
        ("zero compartments keep full budget", 12, 0, 12),
        ("positive budget never collapses to zero", 3, 8, 1),
        ("disabled budget stays disabled", 0, 4, 0),
        ("negative budget left untouched", -1, 4, -1),
    ];
    for (name, budget, num_compartments, expected) in cases {
        assert_eq!(divide_budget(budget, num_compartments), expected, "{name}");
    }
}

// Upstream: pkg/storage/ingest/config_test.go::TestKafkaConfig_ToWarpstreamClientOptions
#[test]
fn test_kafka_config_to_warpstream_client_options() {
    let base_config = || KafkaConfig {
        backend: "warpstream".to_string(),
        address: StringSliceCSV(vec!["a:9092".to_string(), "b:9092".to_string()]),
        topic: "ingest".to_string(),
        client_id: "client-1".to_string(),
        dial_timeout: GoDuration::from_secs(3),
        write_timeout: GoDuration::from_secs(7),
        write_timeout_overhead: GoDuration::from_secs(2),
        warpstream_health_check_slow_multiplier: 1.5,
        warpstream_health_check_max_slow_fraction: 0.4,
        warpstream_health_check_faulty_threshold: 0.06,
        warpstream_health_check_max_faulty_fraction: 0.5,
        warpstream_hedge_min_delay: GoDuration::from_millis(15),
        warpstream_hedge_max_agents: 4,
        warpstream_demoter_probe_interval: GoDuration::from_secs(2),
        ..Default::default()
    };

    // Maps warpstream-relevant fields.
    {
        let opts = to_warpstream_client_options(&base_config()).unwrap();
        assert_eq!(opts.address, vec!["a:9092", "b:9092"]);
        assert_eq!(opts.topic, "ingest");
        assert_eq!(opts.client_id, "client-1");
        assert_eq!(opts.dial_timeout, Duration::from_secs(3));
        assert_eq!(opts.write_timeout, Duration::from_secs(7));
        assert_eq!(opts.linger, DEFAULT_PRODUCER_LINGER);
        assert_eq!(opts.batch_max_bytes, PRODUCER_BATCH_MAX_BYTES as i32);
        assert_eq!(opts.metadata_refresh_interval, DEFAULT_METADATA_REFRESH_INTERVAL);
        assert_eq!(opts.cluster_stats_ttl, Duration::from_secs(1));
        assert_eq!(opts.health_check_slow_multiplier, 1.5);
        assert_eq!(opts.health_check_max_slow_fraction, 0.4);
        assert_eq!(opts.health_check_faulty_threshold, 0.06);
        assert_eq!(opts.health_check_max_faulty_fraction, 0.5);
        assert_eq!(opts.hedger_min_hedge_delay, Duration::from_millis(15));
        assert_eq!(opts.hedger_max_hedge_agents, 4);
        assert_eq!(opts.demoter_probe_interval, Duration::from_secs(2));
        assert_eq!(opts.produce_request_timeout, Duration::from_secs(5));
        assert_eq!(opts.produce_request_timeout_overhead, Duration::from_secs(2));
        assert!(!opts.tls_enabled);
    }

    // Honors custom write timeout overhead.
    {
        let mut cfg = base_config();
        cfg.write_timeout_overhead = GoDuration::from_secs(1);
        let opts = to_warpstream_client_options(&cfg).unwrap();
        assert_eq!(opts.produce_request_timeout, Duration::from_secs(6));
        assert_eq!(opts.produce_request_timeout_overhead, Duration::from_secs(1));
    }

    // Enables TLS when configured.
    {
        let mut cfg = base_config();
        cfg.tls_enabled = true;
        cfg.tls_insecure_skip_verify = true;
        let opts = to_warpstream_client_options(&cfg).unwrap();
        assert!(opts.tls_enabled);
        assert!(opts.tls_insecure_skip_verify);
    }

    // Fails when TLS enabled but misconfigured.
    {
        let mut cfg = base_config();
        cfg.tls_enabled = true;
        cfg.tls_cert_path = "/does/not/exist.crt".to_string();
        assert!(to_warpstream_client_options(&cfg).is_err());
    }
}

// ===========================================================================
// pkg/storage/ingest/fixture_generator_test.go (5 tests)
// ===========================================================================

fn create_test_fixture_config(
    total_unique_timeseries: usize,
    total_samples: usize,
) -> FixtureConfig {
    FixtureConfig {
        total_unique_timeseries,
        total_samples,
        small_tenants: FixtureTenantClassConfig {
            tenant_percent: 40,
            timeseries_percent: 5,
            avg_timeseries_per_req: 5.0,
        },
        medium_tenants: FixtureTenantClassConfig {
            tenant_percent: 44,
            timeseries_percent: 35,
            avg_timeseries_per_req: 5.0,
        },
        large_tenants: FixtureTenantClassConfig {
            tenant_percent: 16,
            timeseries_percent: 60,
            avg_timeseries_per_req: 5.5,
        },
        avg_labels_per_timeseries: 14,
        avg_label_name_length: 10,
        avg_label_value_length: 21,
        avg_metric_name_length: 36,
        num_unique_label_names: 100,
        num_unique_label_values: 1000,
    }
}

// Upstream: pkg/storage/ingest/fixture_generator_test.go::TestFixtureConfig_Validate
#[test]
fn test_fixture_config_validate() {
    assert!(create_test_fixture_config(100, 200).validate().is_ok());
    assert!(create_test_fixture_config(0, 200).validate().is_err());
    assert!(create_test_fixture_config(100, 99).validate().is_err());

    {
        let mut cfg = create_test_fixture_config(100, 200);
        cfg.small_tenants.tenant_percent = 50;
        cfg.medium_tenants.tenant_percent = 30;
        cfg.large_tenants.tenant_percent = 10;
        assert!(cfg.validate().is_err());
    }
    {
        let mut cfg = create_test_fixture_config(100, 200);
        cfg.small_tenants.timeseries_percent = 10;
        cfg.medium_tenants.timeseries_percent = 20;
        cfg.large_tenants.timeseries_percent = 60;
        assert!(cfg.validate().is_err());
    }
    {
        let mut cfg = create_test_fixture_config(100, 200);
        cfg.small_tenants.tenant_percent = -10;
        cfg.medium_tenants.tenant_percent = 60;
        cfg.large_tenants.tenant_percent = 50;
        assert!(cfg.validate().is_err());
    }
    {
        let mut cfg = create_test_fixture_config(100, 200);
        cfg.small_tenants.avg_timeseries_per_req = 0.0;
        assert!(cfg.validate().is_err());
    }
    {
        let mut cfg = create_test_fixture_config(100, 200);
        cfg.avg_labels_per_timeseries = 0;
        assert!(cfg.validate().is_err());
    }
}

// Upstream: pkg/storage/ingest/fixture_generator_test.go::TestNewFixtureGenerator
#[test]
fn test_new_fixture_generator() {
    assert!(FixtureGenerator::new(create_test_fixture_config(0, 200), 10, 12345).is_err());

    let mut cfg = create_test_fixture_config(100, 200);
    cfg.small_tenants.tenant_percent = 100;
    cfg.small_tenants.timeseries_percent = 0;
    cfg.medium_tenants.tenant_percent = 0;
    cfg.medium_tenants.timeseries_percent = 50;
    cfg.large_tenants.tenant_percent = 0;
    cfg.large_tenants.timeseries_percent = 50;
    let err = FixtureGenerator::new(cfg, 10, 12345).err().unwrap();
    assert!(err.contains("zero total timeseries"), "got: {err}");
}

// Upstream: pkg/storage/ingest/fixture_generator_test.go::TestFixtureGenerator_GenerateNextWriteRequest
#[test]
fn test_fixture_generator_generate_next_write_request() {
    // 1. Deterministic output
    {
        let cfg = create_test_fixture_config(100, 200);
        let mut gen1 = FixtureGenerator::new(cfg.clone(), 5, 42).unwrap();
        let mut gen2 = FixtureGenerator::new(cfg, 5, 42).unwrap();

        for i in 0..20 {
            let (t1, r1) = gen1.generate_next_write_request(1000 + i).unwrap();
            let (t2, r2) = gen2.generate_next_write_request(1000 + i).unwrap();
            assert_eq!(t1, t2);
            let r1 = r1.unwrap();
            let r2 = r2.unwrap();
            assert_eq!(r1.timeseries.len(), r2.timeseries.len());
            for (ts1, ts2) in r1.timeseries.iter().zip(r2.timeseries.iter()) {
                assert_eq!(ts1.labels, ts2.labels);
                assert_eq!(ts1.samples, ts2.samples);
            }
        }
    }

    // 2. Honor configured number of unique series via rotation
    {
        const NUM_UNIQUE: usize = 100;
        let mut cfg = create_test_fixture_config(NUM_UNIQUE, 200);
        cfg.small_tenants.tenant_percent = 0;
        cfg.small_tenants.timeseries_percent = 0;
        cfg.small_tenants.avg_timeseries_per_req = 0.0;
        cfg.medium_tenants.tenant_percent = 0;
        cfg.medium_tenants.timeseries_percent = 0;
        cfg.medium_tenants.avg_timeseries_per_req = 0.0;
        cfg.large_tenants.tenant_percent = 100;
        cfg.large_tenants.timeseries_percent = 100;
        cfg.large_tenants.avg_timeseries_per_req = 5.0;

        let mut gen = FixtureGenerator::new(cfg, 1, 12345).unwrap();
        let mut all_metrics = HashSet::new();
        for i in 0..30 {
            let (_, req) = gen.generate_next_write_request(1000 + i).unwrap();
            for ts in req.unwrap().timeseries {
                for lbl in ts.labels {
                    if lbl.name.as_ref() == b"__name__" {
                        all_metrics.insert(String::from_utf8(lbl.value.to_vec()).unwrap());
                    }
                }
            }
        }
        assert_eq!(all_metrics.len(), NUM_UNIQUE);
    }

    // 3. Interleave tenants
    {
        let cfg = create_test_fixture_config(1000, 2000);
        let mut gen = FixtureGenerator::new(cfg, 10, 12345).unwrap();
        let mut seq = Vec::with_capacity(1000);
        for i in 0..1000 {
            let (tid, _) = gen.generate_next_write_request(i).unwrap();
            seq.push(tid);
        }
        let mut cur = String::new();
        let mut run = 0usize;
        let mut max_run = 0usize;
        let mut unique = HashSet::new();
        for tid in seq {
            if tid == cur {
                run += 1;
                max_run = max_run.max(run);
            } else {
                cur = tid.clone();
                run = 1;
            }
            unique.insert(tid);
        }
        assert!(max_run <= 20, "max consecutive run was {max_run}");
        assert!(unique.len() >= 5, "unique tenants: {}", unique.len());
    }
}

// Upstream: pkg/storage/ingest/fixture_generator_test.go::TestFixtureGenerator_TenantDistribution
#[test]
fn test_fixture_generator_tenant_distribution() {
    let cfg = create_test_fixture_config(10000, 20000);
    let mut gen = FixtureGenerator::new(cfg.clone(), 100, 12345).unwrap();

    let num_small = 40;
    let num_medium = 44;
    let total = 10000usize;
    let mut small = 0usize;
    let mut medium = 0usize;
    let mut large = 0usize;

    for _ in 0..total {
        let idx = gen.select_next_tenant();
        if idx < num_small {
            small += 1;
        } else if idx < num_small + num_medium {
            medium += 1;
        } else {
            large += 1;
        }
    }

    let small_pct = (small as f64) * 100.0 / (total as f64);
    let med_pct = (medium as f64) * 100.0 / (total as f64);
    let large_pct = (large as f64) * 100.0 / (total as f64);

    assert!((small_pct - cfg.small_tenants.timeseries_percent as f64).abs() <= 3.0);
    assert!((med_pct - cfg.medium_tenants.timeseries_percent as f64).abs() <= 3.0);
    assert!((large_pct - cfg.large_tenants.timeseries_percent as f64).abs() <= 3.0);
}

// Upstream: pkg/storage/ingest/fixture_generator_test.go::TestFixtureGenerator_generateSeriesLabels
#[test]
fn test_fixture_generator_generate_series_labels() {
    let mut cfg = create_test_fixture_config(100, 200);
    cfg.avg_labels_per_timeseries = 10;
    cfg.num_unique_label_names = 50;
    cfg.num_unique_label_values = 100;
    let gen = FixtureGenerator::new(cfg.clone(), 1, 12345).unwrap();

    for idx in 0..20 {
        assert_eq!(gen.generate_series_labels(idx), gen.generate_series_labels(idx));
    }

    let lbls = gen.generate_series_labels(0);
    assert_eq!(lbls.len(), cfg.avg_labels_per_timeseries + 1);

    let mut unique_names = HashSet::new();
    let mut unique_vals = HashSet::new();
    for idx in 0..(cfg.num_unique_label_names * cfg.num_unique_label_values) {
        for lp in gen.generate_series_labels(idx) {
            if lp.name.as_ref() != b"__name__" {
                unique_names.insert(lp.name);
                unique_vals.insert(lp.value);
            }
        }
    }
    assert_eq!(unique_names.len(), cfg.num_unique_label_names);
    assert_eq!(unique_vals.len(), cfg.num_unique_label_values);
}

// ===========================================================================
// pkg/storage/ingest/heap_merger_test.go (8 tests)
// ===========================================================================

fn mk_rec(offset: i64, ts_ms: i64) -> FetchedRecord {
    FetchedRecord {
        offset,
        timestamp_ms: ts_ms,
        key: Bytes::from_static(b"t1"),
        value: Bytes::new(),
        headers: Vec::new(),
    }
}

// Upstream: pkg/storage/ingest/heap_merger_test.go::TestOrderedConsumptionConfig_Validate
#[test]
fn test_ordered_consumption_config_validate() {
    let valid = OrderedConsumptionConfig {
        enabled: true,
        max_batch_records: 1024,
        max_batch_wait: GoDuration::from_millis(50),
        input_buffer_size: 0,
    };
    assert!(valid.validate().is_ok());

    {
        let mut cfg = valid.clone();
        cfg.max_batch_records = 0;
        assert!(cfg.validate().unwrap_err().to_string().contains("max-batch-records"));
    }
    {
        let mut cfg = valid;
        cfg.max_batch_wait = GoDuration(0);
        assert!(cfg.validate().unwrap_err().to_string().contains("max-batch-wait"));
    }
    {
        let cfg = OrderedConsumptionConfig {
            enabled: false,
            max_batch_records: 0,
            max_batch_wait: GoDuration(0),
            ..Default::default()
        };
        assert!(cfg.validate().is_ok());
    }
}

// Upstream: pkg/storage/ingest/heap_merger_test.go::TestRecordHeap_Ordering
#[test]
fn test_record_heap_ordering() {
    // 1. Orders by ascending timestamp
    {
        let mut heap = BinaryHeap::new();
        heap.push(MergeableRecord {
            record: mk_rec(3, 30),
            kafka_cluster_id: 0,
        });
        heap.push(MergeableRecord {
            record: mk_rec(1, 10),
            kafka_cluster_id: 0,
        });
        heap.push(MergeableRecord {
            record: mk_rec(2, 20),
            kafka_cluster_id: 0,
        });
        let mut got = Vec::new();
        while let Some(item) = heap.pop() {
            got.push(item.record.offset);
        }
        assert_eq!(got, vec![1, 2, 3]);
    }

    // 2. Timestamp ties break by cluster then offset
    {
        let ts = 1_234_000;
        let mut heap = BinaryHeap::new();
        heap.push(MergeableRecord {
            record: mk_rec(5, ts),
            kafka_cluster_id: 2,
        });
        heap.push(MergeableRecord {
            record: mk_rec(1, ts),
            kafka_cluster_id: 0,
        });
        heap.push(MergeableRecord {
            record: mk_rec(2, ts),
            kafka_cluster_id: 0,
        });
        heap.push(MergeableRecord {
            record: mk_rec(7, ts),
            kafka_cluster_id: 1,
        });
        let mut got = Vec::new();
        while let Some(item) = heap.pop() {
            got.push((item.kafka_cluster_id, item.record.offset));
        }
        assert_eq!(got, vec![(0, 1), (0, 2), (1, 7), (2, 5)]);
    }
}

fn new_recording_consumer(
    err_msg: Option<String>,
) -> (Arc<SyncMutex<Vec<FetchedRecord>>>, MergedBatchConsumer) {
    let recs = Arc::new(SyncMutex::new(Vec::new()));
    let recs_c = Arc::clone(&recs);
    let cb: MergedBatchConsumer = Arc::new(move |batch| {
        let recs_c = Arc::clone(&recs_c);
        let err_msg = err_msg.clone();
        Box::pin(async move {
            recs_c.lock().extend(batch);
            match err_msg {
                Some(e) => Err(e),
                None => Ok(()),
            }
        })
    });
    (recs, cb)
}

// Upstream: pkg/storage/ingest/heap_merger_test.go::TestHeapMerger_Ordering
#[tokio::test]
async fn test_heap_merger_ordering() {
    // Single cluster preserves submission order
    {
        let (recs, cb) = new_recording_consumer(None);
        let cfg = OrderedConsumptionConfig {
            enabled: true,
            max_batch_records: 3,
            max_batch_wait: GoDuration::from_millis(10),
            input_buffer_size: 0,
        };
        let m = HeapMerger::start(&cfg, cb, HeapMergerMetrics::new(None));
        m.submit_and_wait(0, vec![mk_rec(1, 1), mk_rec(2, 2), mk_rec(3, 3)])
            .await
            .unwrap();
        m.stop().await;
        let got: Vec<i64> = recs.lock().iter().map(|r| r.offset).collect();
        assert_eq!(got, vec![1, 2, 3]);
    }

    // Two clusters interleave by timestamp
    {
        let (recs, cb) = new_recording_consumer(None);
        let cfg = OrderedConsumptionConfig {
            enabled: true,
            max_batch_records: 100,
            max_batch_wait: GoDuration::from_millis(100),
            input_buffer_size: 0,
        };
        let m = HeapMerger::start(&cfg, cb, HeapMergerMetrics::new(None));
        let (r0, r1) = tokio::join!(
            m.submit_and_wait(0, vec![mk_rec(10, 1), mk_rec(11, 3), mk_rec(12, 5)]),
            m.submit_and_wait(1, vec![mk_rec(20, 2), mk_rec(21, 4), mk_rec(22, 6)]),
        );
        r0.unwrap();
        r1.unwrap();
        m.stop().await;
        let got = recs.lock().clone();
        assert_eq!(got.len(), 6);
        for w in got.windows(2) {
            assert!(w[1].timestamp_ms >= w[0].timestamp_ms);
        }
    }
}

// Upstream: pkg/storage/ingest/heap_merger_test.go::TestHeapMerger_ErrorPropagatesToAllSubmittersInBatch
#[tokio::test]
async fn test_heap_merger_error_propagates_to_all_submitters_in_batch() {
    let (_, cb) = new_recording_consumer(Some("downstream boom".to_string()));
    let cfg = OrderedConsumptionConfig {
        enabled: true,
        max_batch_records: 100,
        max_batch_wait: GoDuration::from_millis(30),
        input_buffer_size: 0,
    };
    let m = HeapMerger::start(&cfg, cb, HeapMergerMetrics::new(None));
    let (e0, e1) = tokio::join!(
        m.submit_and_wait(0, vec![mk_rec(1, 100)]),
        m.submit_and_wait(1, vec![mk_rec(2, 100)]),
    );
    assert!(e0.unwrap_err().contains("downstream boom"));
    assert!(e1.unwrap_err().contains("downstream boom"));
    m.stop().await;
}

// Upstream: pkg/storage/ingest/heap_merger_test.go::TestHeapMerger_EmptyConsumeIsNoop
#[tokio::test]
async fn test_heap_merger_empty_consume_is_noop() {
    let (recs, cb) = new_recording_consumer(None);
    let cfg = OrderedConsumptionConfig {
        enabled: true,
        max_batch_records: 100,
        max_batch_wait: GoDuration::from_millis(30),
        input_buffer_size: 0,
    };
    let m = HeapMerger::start(&cfg, cb, HeapMergerMetrics::new(None));
    m.submit_and_wait(0, vec![]).await.unwrap();
    m.stop().await;
    assert!(recs.lock().is_empty());
}

// Upstream: pkg/storage/ingest/heap_merger_test.go::TestHeapMerger_TimerFlush
#[tokio::test]
async fn test_heap_merger_timer_flush() {
    let (recs, cb) = new_recording_consumer(None);
    let cfg = OrderedConsumptionConfig {
        enabled: true,
        max_batch_records: 100,
        max_batch_wait: GoDuration::from_millis(20),
        input_buffer_size: 0,
    };
    let m = HeapMerger::start(&cfg, cb, HeapMergerMetrics::new(None));
    m.submit_and_wait(0, vec![mk_rec(1, 1000)]).await.unwrap();
    assert_eq!(recs.lock().len(), 1);
    m.submit_and_wait(0, vec![mk_rec(2, 2000)]).await.unwrap();
    assert_eq!(recs.lock().len(), 2);
    m.stop().await;
}

// Upstream: pkg/storage/ingest/heap_merger_test.go::TestHeapMerger_ShutdownAcksPendingRecords
#[tokio::test]
async fn test_heap_merger_shutdown_acks_pending_records() {
    let (recs, cb) = new_recording_consumer(None);
    let cfg = OrderedConsumptionConfig {
        enabled: true,
        max_batch_records: 100,
        max_batch_wait: GoDuration::from_hours(1),
        input_buffer_size: 0,
    };
    let m = Arc::new(HeapMerger::start(&cfg, cb, HeapMergerMetrics::new(None)));
    let m2 = Arc::clone(&m);
    let handle = tokio::spawn(async move { m2.submit_and_wait(0, vec![mk_rec(1, 1000)]).await });

    tokio::time::sleep(Duration::from_millis(20)).await;
    m.shutdown_and_drain().await;

    let res = tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("submitter did not return after shutdown")
        .unwrap();
    assert!(res.is_err());
    assert!(recs.lock().is_empty());
}

// Upstream: pkg/storage/ingest/heap_merger_test.go::TestHeapMerger_CountsOutOfOrderEmissionsAcrossBatches
#[tokio::test]
async fn test_heap_merger_counts_out_of_order_emissions_across_batches() {
    let (_, cb) = new_recording_consumer(None);
    let reg = Registry::new();
    let cfg = OrderedConsumptionConfig {
        enabled: true,
        max_batch_records: 1,
        max_batch_wait: GoDuration::from_millis(5),
        input_buffer_size: 0,
    };
    let m = HeapMerger::start(&cfg, cb, HeapMergerMetrics::new(Some(&reg)));
    m.submit_and_wait(0, vec![mk_rec(1, 50)]).await.unwrap();
    m.submit_and_wait(0, vec![mk_rec(2, 10)]).await.unwrap();
    m.stop().await;

    let text = TextEncoder::new().encode_to_string(&reg.gather()).unwrap();
    assert!(text.contains("cortex_ingest_storage_ordered_consumption_records_total 2"));
    assert!(text.contains("cortex_ingest_storage_ordered_consumption_out_of_order_records_total 1"));
}

// ===========================================================================
// pkg/storage/ingest/kmeta/partitions_offsets_test.go (2 tests)
// ===========================================================================

// Upstream: pkg/storage/ingest/kmeta/partitions_offsets_test.go::TestPartitionOffsets_ForKafkaCluster
#[test]
fn test_partition_offsets_for_kafka_cluster() {
    let offsets = PartitionOffsets::new_multi_cluster(vec![10, 20, -1]);
    assert_eq!(offsets.num_kafka_clusters(), 3);
    assert_eq!(offsets.for_kafka_cluster(0), 10);
    assert_eq!(offsets.for_kafka_cluster(1), 20);
    assert_eq!(offsets.for_kafka_cluster(2), -1);
    assert_eq!(offsets.for_kafka_cluster(3), -1);
    assert_eq!(offsets.for_kafka_cluster(usize::MAX), -1);

    let single = PartitionOffsets::new_single_cluster(7);
    assert_eq!(single.num_kafka_clusters(), 1);
    assert_eq!(single.for_kafka_cluster(0), 7);
    assert_eq!(single.for_kafka_cluster(1), -1);
}

// Upstream: pkg/storage/ingest/kmeta/partitions_offsets_test.go::TestMergeTopicsPartitionsOffsets
#[test]
fn test_merge_topics_partitions_offsets() {
    // 1. No clusters
    assert_eq!(
        merge_topics_partitions_offsets(&[]),
        TopicsPartitionsOffsets::new()
    );

    // 2. Single cluster, single topic, multiple partitions
    {
        let per = vec![HashMap::from([(
            "topic-0".to_string(),
            HashMap::from([(0, 5), (1, -1)]),
        )])];
        let expected = HashMap::from([(
            "topic-0".to_string(),
            HashMap::from([
                (0, PartitionOffsets::new_multi_cluster(vec![5])),
                (1, PartitionOffsets::new_multi_cluster(vec![-1])),
            ]),
        )]);
        assert_eq!(merge_topics_partitions_offsets(&per), expected);
    }

    // 3. Two clusters, full overlap
    {
        let per = vec![
            HashMap::from([("topic-0".to_string(), HashMap::from([(0, 2), (1, 1)]))]),
            HashMap::from([("topic-0".to_string(), HashMap::from([(0, 0), (1, 3)]))]),
        ];
        let expected = HashMap::from([(
            "topic-0".to_string(),
            HashMap::from([
                (0, PartitionOffsets::new_multi_cluster(vec![2, 0])),
                (1, PartitionOffsets::new_multi_cluster(vec![1, 3])),
            ]),
        )]);
        assert_eq!(merge_topics_partitions_offsets(&per), expected);
    }

    // 4. Two clusters, partial overlap defaults absent topic-partitions to -1
    {
        let per = vec![
            HashMap::from([
                (
                    "topic-0".to_string(),
                    HashMap::from([(0, 2), (1, 1), (2, 0)]),
                ),
                ("topic-1".to_string(), HashMap::from([(0, 0)])),
            ]),
            HashMap::from([("topic-0".to_string(), HashMap::from([(0, 0)]))]),
        ];
        let expected = HashMap::from([
            (
                "topic-0".to_string(),
                HashMap::from([
                    (0, PartitionOffsets::new_multi_cluster(vec![2, 0])),
                    (1, PartitionOffsets::new_multi_cluster(vec![1, -1])),
                    (2, PartitionOffsets::new_multi_cluster(vec![0, -1])),
                ]),
            ),
            (
                "topic-1".to_string(),
                HashMap::from([(0, PartitionOffsets::new_multi_cluster(vec![0, -1]))]),
            ),
        ]);
        assert_eq!(merge_topics_partitions_offsets(&per), expected);
    }

    // 5. Topic present only on the second cluster
    {
        let per = vec![
            HashMap::new(),
            HashMap::from([("topic-1".to_string(), HashMap::from([(0, 7)]))]),
        ];
        let expected = HashMap::from([(
            "topic-1".to_string(),
            HashMap::from([(0, PartitionOffsets::new_multi_cluster(vec![-1, 7]))]),
        )]);
        assert_eq!(merge_topics_partitions_offsets(&per), expected);
    }
}

// ===========================================================================
// pkg/storage/ingest/logger_test.go (2 tests)
// ===========================================================================

struct BufLogfmtLogger(SyncMutex<String>);

impl Logger for BufLogfmtLogger {
    fn log(&self, level: LogLevel, kvs: &[(&str, &str)]) {
        let mut pairs = Vec::with_capacity(1 + kvs.len());
        pairs.push(("level", level.as_str()));
        pairs.extend_from_slice(kvs);
        let line = format_logfmt_line(&pairs);
        self.0.lock().push_str(&line);
    }
}

// Upstream: pkg/storage/ingest/logger_test.go::TestKafkaLogger_Log
#[test]
fn test_kafka_logger_log() {
    for (lev, expected_level) in [
        (LogLevel::Debug, "level=debug"),
        (LogLevel::Info, "level=info"),
        (LogLevel::Warn, "level=warn"),
        (LogLevel::Error, "level=error"),
    ] {
        let buf = Arc::new(BufLogfmtLogger(SyncMutex::new(String::new())));
        let l = KafkaLogger::new(buf.clone());
        l.log(lev, "the message", &[("key", "value")]);
        let out = buf.0.lock().clone();
        assert!(out.contains(expected_level), "out: {out}");
        assert!(out.contains("component=kafka_client"), "out: {out}");
        assert!(out.contains(r#"msg="the message""#), "out: {out}");
        assert!(out.contains("key=value"), "out: {out}");
    }
}

// Upstream: pkg/storage/ingest/logger_test.go::TestKafkaLogger_Level
#[test]
fn test_kafka_logger_level() {
    let buf = Arc::new(BufLogfmtLogger(SyncMutex::new(String::new())));
    let l = KafkaLogger::new(buf);
    assert_eq!(l.level(), LogLevel::Info);
}

// ===========================================================================
// pkg/storage/ingest/offset_file_test.go (8 tests)
// ===========================================================================

const TEST_PARTITION_ID: i32 = 1;

// Upstream: pkg/storage/ingest/offset_file_test.go::TestOffsetFile_ReadWrite
#[test]
fn test_offset_file_read_write() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("partition-1.offset");
    let f = OffsetFile::new(&path, TEST_PARTITION_ID);

    assert_eq!(f.read(), None);

    f.write(12345).unwrap();
    assert!(path.exists());
    assert_eq!(f.read(), Some(12345));

    f.write(67890).unwrap();
    assert_eq!(f.read(), Some(67890));
}

// Upstream: pkg/storage/ingest/offset_file_test.go::TestOffsetFile_WriteCreatesDirectory
#[test]
fn test_offset_file_write_creates_directory() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("nested").join("path").join("partition-1.offset");
    let f = OffsetFile::new(&path, TEST_PARTITION_ID);

    f.write(1).unwrap();
    assert_eq!(f.read(), Some(1));
}

// Upstream: pkg/storage/ingest/offset_file_test.go::TestOffsetFile_ReadInvalidContent
#[test]
fn test_offset_file_read_invalid_content() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("invalid.offset");
    fs::write(&path, b"not-valid-json").unwrap();
    assert_eq!(OffsetFile::new(&path, TEST_PARTITION_ID).read(), None);
}

// Upstream: pkg/storage/ingest/offset_file_test.go::TestOffsetFile_ReadEmptyFile
#[test]
fn test_offset_file_read_empty_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("empty.offset");
    fs::write(&path, b"").unwrap();
    assert_eq!(OffsetFile::new(&path, TEST_PARTITION_ID).read(), None);
}

// Upstream: pkg/storage/ingest/offset_file_test.go::TestOffsetFile_ReadWrongPartitionID
#[test]
fn test_offset_file_read_wrong_partition_id() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("partition.offset");
    OffsetFile::new(&path, 1).write(42).unwrap();
    assert_eq!(OffsetFile::new(&path, 2).read(), None);
}

// Upstream: pkg/storage/ingest/offset_file_test.go::TestOffsetFile_ReadWrongVersion
#[test]
fn test_offset_file_read_wrong_version() {
    assert_eq!(OFFSET_FILE_VERSION, 1);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("version.offset");
    fs::write(
        &path,
        format!(r#"{{"version":2,"partition_id":{TEST_PARTITION_ID},"offset":0}}"#),
    )
    .unwrap();
    assert_eq!(OffsetFile::new(&path, TEST_PARTITION_ID).read(), None);
}

// Upstream: pkg/storage/ingest/offset_file_test.go::TestOffsetFile_AtomicWrite
#[test]
fn test_offset_file_atomic_write() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("atomic.offset");
    let f = OffsetFile::new(&path, TEST_PARTITION_ID);

    f.write(100).unwrap();
    assert!(!path.with_extension("offset.tmp").exists());
    assert_eq!(f.read(), Some(100));
}

// Upstream: pkg/storage/ingest/offset_file_test.go::TestOffsetFile_Concurrent
#[test]
fn test_offset_file_concurrent() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("concurrent.offset");
    let f = Arc::new(OffsetFile::new(&path, TEST_PARTITION_ID));

    let f1 = Arc::clone(&f);
    let h1 = std::thread::spawn(move || {
        for i in 0..100 {
            f1.write(i).unwrap();
        }
    });
    let f2 = Arc::clone(&f);
    let h2 = std::thread::spawn(move || {
        for _ in 0..100 {
            let _ = f2.read();
        }
    });

    h1.join().unwrap();
    h2.join().unwrap();

    let off = f.read().unwrap();
    assert!((0..100).contains(&off));
}

// ===========================================================================
// pkg/storage/ingest/partition_offset_watcher_test.go (2 tests)
// ===========================================================================

// Upstream: pkg/storage/ingest/partition_offset_watcher_test.go::TestPartitionOffsetWatcher
#[tokio::test]
async fn test_partition_offset_watcher() {
    // 1. Single waiter
    {
        let w = Arc::new(PartitionOffsetWatcher::new());
        let done = Arc::new(AtomicBool::new(false));
        let w2 = Arc::clone(&w);
        let d2 = Arc::clone(&done);
        let h = tokio::spawn(async move {
            w2.wait_for_offset(2, Duration::from_secs(5)).await.unwrap();
            d2.store(true, Ordering::SeqCst);
        });
        tokio::time::sleep(Duration::from_millis(15)).await;
        assert!(!done.load(Ordering::SeqCst));
        w.notify(1);
        tokio::time::sleep(Duration::from_millis(15)).await;
        assert!(!done.load(Ordering::SeqCst));
        w.notify(2);
        h.await.unwrap();
        assert!(done.load(Ordering::SeqCst));
    }

    // 2. Two waiters waiting for the same offset
    {
        let w = Arc::new(PartitionOffsetWatcher::new());
        let d1 = Arc::new(AtomicBool::new(false));
        let d2 = Arc::new(AtomicBool::new(false));
        let h1 = {
            let w = Arc::clone(&w);
            let d = Arc::clone(&d1);
            tokio::spawn(async move {
                w.wait_for_offset(2, Duration::from_secs(5)).await.unwrap();
                d.store(true, Ordering::SeqCst);
            })
        };
        let h2 = {
            let w = Arc::clone(&w);
            let d = Arc::clone(&d2);
            tokio::spawn(async move {
                w.wait_for_offset(2, Duration::from_secs(5)).await.unwrap();
                d.store(true, Ordering::SeqCst);
            })
        };
        tokio::time::sleep(Duration::from_millis(15)).await;
        w.notify(1);
        tokio::time::sleep(Duration::from_millis(15)).await;
        assert!(!d1.load(Ordering::SeqCst));
        assert!(!d2.load(Ordering::SeqCst));
        w.notify(2);
        h1.await.unwrap();
        h2.await.unwrap();
        assert!(d1.load(Ordering::SeqCst));
        assert!(d2.load(Ordering::SeqCst));
    }

    // 3. Two waiters waiting for different offsets
    {
        let w = Arc::new(PartitionOffsetWatcher::new());
        let d1 = Arc::new(AtomicBool::new(false));
        let d2 = Arc::new(AtomicBool::new(false));
        let h1 = {
            let w = Arc::clone(&w);
            let d = Arc::clone(&d1);
            tokio::spawn(async move {
                w.wait_for_offset(1, Duration::from_secs(5)).await.unwrap();
                d.store(true, Ordering::SeqCst);
            })
        };
        let h2 = {
            let w = Arc::clone(&w);
            let d = Arc::clone(&d2);
            tokio::spawn(async move {
                w.wait_for_offset(2, Duration::from_secs(5)).await.unwrap();
                d.store(true, Ordering::SeqCst);
            })
        };
        tokio::time::sleep(Duration::from_millis(15)).await;
        w.notify(1);
        h1.await.unwrap();
        assert!(d1.load(Ordering::SeqCst));
        assert!(!d2.load(Ordering::SeqCst));
        w.notify(2);
        h2.await.unwrap();
        assert!(d2.load(Ordering::SeqCst));
    }

    // 4. Immediately returns if already consumed
    {
        let w = PartitionOffsetWatcher::new();
        w.notify(10);
        w.wait_for_offset(7, Duration::from_millis(10)).await.unwrap();
    }

    // 5. Immediately returns if -1
    {
        let w = PartitionOffsetWatcher::new();
        w.wait_for_offset(-1, Duration::from_millis(10)).await.unwrap();
    }

    // 6. Stop wakes waiters and notify after stop is a no-op
    {
        let w = Arc::new(PartitionOffsetWatcher::new());
        let h1 = {
            let w = Arc::clone(&w);
            tokio::spawn(async move { w.wait_for_offset(1, Duration::from_secs(5)).await })
        };
        let h2 = {
            let w = Arc::clone(&w);
            tokio::spawn(async move { w.wait_for_offset(2, Duration::from_secs(5)).await })
        };
        tokio::time::sleep(Duration::from_millis(15)).await;
        w.stop();
        assert_eq!(h1.await.unwrap(), Err(OffsetWatcherError::Stopped));
        assert_eq!(h2.await.unwrap(), Err(OffsetWatcherError::Stopped));
        assert_eq!(
            w.wait_for_offset(3, Duration::from_secs(5)).await,
            Err(OffsetWatcherError::Stopped)
        );
        w.notify(1);
        assert_eq!(w.last_consumed_offset(), -1);
    }
}

// Upstream: pkg/storage/ingest/partition_offset_watcher_test.go::TestPartitionOffsetWatcher_Concurrency
#[tokio::test]
async fn test_partition_offset_watcher_concurrency() {
    const NUM_TOTAL_NOTIFICATIONS: i64 = 10_000;
    const NUM_NOTIFYING: usize = 10;
    const NUM_WATCHING: usize = 200;

    let w = Arc::new(PartitionOffsetWatcher::new());
    let last_notified = Arc::new(AtomicI64::new(0));
    let last_watched = Arc::new(AtomicI64::new(0));
    let notifications = Arc::new(AtomicI64::new(0));
    let done = Arc::new(AtomicBool::new(false));

    let mut handles = Vec::new();
    for _ in 0..NUM_WATCHING {
        let w = Arc::clone(&w);
        let lw = Arc::clone(&last_watched);
        let d = Arc::clone(&done);
        handles.push(tokio::spawn(async move {
            while !d.load(Ordering::Relaxed) {
                let target = lw.fetch_add(1, Ordering::Relaxed) + 1;
                let _ = w.wait_for_offset(target, Duration::from_millis(10)).await;
            }
        }));
    }

    for tid in 0..NUM_NOTIFYING {
        let w = Arc::clone(&w);
        let ln = Arc::clone(&last_notified);
        let lw = Arc::clone(&last_watched);
        let notif = Arc::clone(&notifications);
        let d = Arc::clone(&done);
        handles.push(tokio::spawn(async move {
            let mut rng_state = (tid as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15);
            while !d.load(Ordering::Relaxed) {
                let lo = ln.load(Ordering::Relaxed);
                let hi = lw.load(Ordering::Relaxed);
                let range = hi - lo;
                if range <= 0 {
                    tokio::task::yield_now().await;
                    continue;
                }
                rng_state = rng_state.wrapping_mul(6364136223846793005).wrapping_add(1);
                let offset = lo + ((rng_state as i64).abs() % range);
                ln.store(offset, Ordering::Relaxed);
                w.notify(offset);
                if notif.fetch_add(1, Ordering::Relaxed) + 1 >= NUM_TOTAL_NOTIFICATIONS {
                    d.store(true, Ordering::Relaxed);
                    w.stop();
                    break;
                }
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }
}

// ===========================================================================
// pkg/storage/ingest/partition_ring_watchers_test.go (5 tests)
// ===========================================================================

const TEST_RING_NAME: &str = "ingester-partitions";
const TEST_RING_KEY: &str = "ingester-partitions";

fn write_partitions_stored(kv: &InMemoryPartitionKv, key: &str, ids: &[i32]) {
    let mut desc = PartitionRingDesc::default();
    for &id in ids {
        add_partition_with_stored_tokens(
            &mut desc,
            id,
            PartitionState::PartitionActive,
            vec![(id as u32 + 1) * 1000],
        );
    }
    kv.put(key, desc);
}

fn write_partitions_derived(kv: &InMemoryPartitionKv, key: &str, ids: &[i32]) {
    let mut desc = PartitionRingDesc::default();
    for &id in ids {
        add_partition_with_derived_tokens(&mut desc, id, PartitionState::PartitionActive);
    }
    kv.put(key, desc);
}

// Upstream: pkg/storage/ingest/partition_ring_watchers_test.go::TestPartitionRingWatchers_Disabled
#[tokio::test]
async fn test_partition_ring_watchers_disabled() {
    let kv = InMemoryPartitionKv::new();
    write_partitions_stored(&kv, TEST_RING_KEY, &[0, 1, 2]);

    let w = PartitionRingWatchers::new(
        false,
        0,
        TEST_RING_NAME,
        TEST_RING_KEY,
        kv,
        None,
        None,
    )
    .unwrap();
    w.start().await.unwrap();

    assert_eq!(w.count(), 1);
    assert_eq!(w.partition_ring(0).partitions_count(), 3);
    assert!(Arc::ptr_eq(w.watcher(0), &w.all()[0]));

    w.stop().await;
}

// Upstream: pkg/storage/ingest/partition_ring_watchers_test.go::TestPartitionRingWatchers_Enabled
#[tokio::test]
async fn test_partition_ring_watchers_enabled() {
    let kv = InMemoryPartitionKv::new();
    write_partitions_stored(&kv, &with_read_compartment_suffix(TEST_RING_KEY, 0), &[0]);
    write_partitions_stored(&kv, &with_read_compartment_suffix(TEST_RING_KEY, 1), &[0, 1]);
    write_partitions_stored(
        &kv,
        &with_read_compartment_suffix(TEST_RING_KEY, 2),
        &[0, 1, 2],
    );

    let w = PartitionRingWatchers::new(
        true,
        3,
        TEST_RING_NAME,
        TEST_RING_KEY,
        kv,
        None,
        None,
    )
    .unwrap();
    w.start().await.unwrap();

    assert_eq!(w.count(), 3);
    assert_eq!(w.all().len(), 3);
    assert_eq!(w.partition_ring(0).partitions_count(), 1);
    assert_eq!(w.partition_ring(1).partitions_count(), 2);
    assert_eq!(w.partition_ring(2).partitions_count(), 3);
    assert!(Arc::ptr_eq(
        &w.watcher(1).partition_ring(),
        &w.partition_ring(1)
    ));

    w.stop().await;
}

// Upstream: pkg/storage/ingest/partition_ring_watchers_test.go::TestPartitionRingWatchers_DoesNotPanicWithRealRegistererAndMultipleCompartments
#[tokio::test]
async fn test_partition_ring_watchers_does_not_panic_with_real_registerer_and_multiple_compartments() {
    let kv = InMemoryPartitionKv::new();
    let reg = Registry::new();

    let w = PartitionRingWatchers::new(
        true,
        4,
        TEST_RING_NAME,
        TEST_RING_KEY,
        kv,
        None,
        Some(&reg),
    )
    .unwrap();
    assert_eq!(w.count(), 4);
    w.start().await.unwrap();
    w.stop().await;
}

// Upstream: pkg/storage/ingest/partition_ring_watchers_test.go::TestPartitionRingWatchers_ReturnsErrorWhenEnabledWithNoCompartments
#[test]
fn test_partition_ring_watchers_returns_error_when_enabled_with_no_compartments() {
    let kv = InMemoryPartitionKv::new();
    for num in [0, -1] {
        let res = PartitionRingWatchers::new(
            true,
            num,
            TEST_RING_NAME,
            TEST_RING_KEY,
            Arc::clone(&kv),
            None,
            None,
        );
        assert!(res.is_err());
    }
}

// Upstream: pkg/storage/ingest/partition_ring_watchers_test.go::TestPartitionRingWatchers_ResolvesDerivedTokensWithTokenGenerator
#[tokio::test]
async fn test_partition_ring_watchers_resolves_derived_tokens_with_token_generator() {
    let partition_ids = [0i32, 1, 2];
    let token_table = Arc::new(PartitionTokenTable::new(partition_ids.len() as i32).unwrap());

    let mut stored_desc = PartitionRingDesc::default();
    for &id in &partition_ids {
        add_partition_with_stored_tokens(
            &mut stored_desc,
            id,
            PartitionState::PartitionActive,
            token_table.tokens_for(id).unwrap().to_vec(),
        );
    }
    let stored_ring = PartitionRingSnapshot::new(stored_desc, None).unwrap();

    for (enabled, num_c, keys) in [
        (false, 0, vec![TEST_RING_KEY.to_string()]),
        (
            true,
            2,
            vec![
                with_read_compartment_suffix(TEST_RING_KEY, 0),
                with_read_compartment_suffix(TEST_RING_KEY, 1),
            ],
        ),
    ] {
        let kv = InMemoryPartitionKv::new();
        for k in &keys {
            write_partitions_derived(&kv, k, &partition_ids);
        }

        let w = PartitionRingWatchers::new(
            enabled,
            num_c,
            TEST_RING_NAME,
            TEST_RING_KEY,
            kv,
            Some(Arc::clone(&token_table)),
            None,
        )
        .unwrap();
        w.start().await.unwrap();
        assert_eq!(w.count(), keys.len());

        for c in 0..keys.len() {
            for &id in &partition_ids {
                let expected = stored_ring.get_token_ranges_for_partition(id).unwrap();
                let actual = w.partition_ring(c).get_token_ranges_for_partition(id).unwrap();
                assert_eq!(actual, expected, "compartment {c}, partition {id}");
            }
        }
        w.stop().await;
    }
}
