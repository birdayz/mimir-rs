//! Unit and upstream-ported tests for `mimir-config`.
//!
//! Ported from:
//! - github.com/grafana/mimir/cmd/mimir/main_test.go
//! - github.com/grafana/mimir/pkg/mimir/mimir_config_test.go
//! - github.com/grafana/mimir/pkg/mimir/runtime_config_test.go
//! - github.com/grafana/mimir/pkg/storage/ingest/config_test.go

use std::fs;
use std::time::Duration;

use mimir_config::{
    expand_environment_variables_with, format_modules, format_version, parse_cli_and_load_config,
    parse_config_file_parameter, parse_duration, parse_go_duration, parse_model_duration,
    parse_runtime_config_yaml, sha256_hex, CliAction, GoDuration, InstanceLimits, LimitsConfig,
    MimirConfig, ModelDuration, RuntimeConfigConfig, RuntimeConfigManager,
};
use tempfile::tempdir;

#[test]
fn test_parse_config_file_parameter_upstream_cases() {
    let cases = [
        ("", "", false),
        ("--foo", "", false),
        ("-f -a", "", false),
        ("--config.file=foo", "foo", false),
        ("--config.file foo", "foo", false),
        ("--config.file=foo --config.expand-env", "foo", true),
        ("--config.expand-env --config.file=foo", "foo", true),
        ("--opt1 --config.file=foo", "foo", false),
        ("--opt1 --config.file foo", "foo", false),
        ("--opt1 --config.file=foo --config.expand-env", "foo", true),
        ("--opt1 --config.expand-env --config.file=foo", "foo", true),
        ("--config.file=foo --opt1", "foo", false),
        ("--config.file foo --opt1", "foo", false),
        ("--config.file=foo --opt1 --config.expand-env", "foo", true),
        ("--config.expand-env --opt1 --config.file=foo", "foo", true),
    ];

    for (raw, expected_file, expected_expand) in cases {
        let args: Vec<String> = if raw.is_empty() {
            Vec::new()
        } else {
            raw.split(' ').map(String::from).collect()
        };
        let (file, expand) = parse_config_file_parameter(&args);
        assert_eq!(file, expected_file, "args={raw:?}");
        assert_eq!(expand, expected_expand, "args={raw:?}");
    }
}

#[test]
fn test_expand_environment_variables_upstream_cases() {
    let env = |k: &str| match k {
        "y" => Some("y".to_string()),
        "MULTILINE" => Some("{\n\t\t\t\"foo\": \"bar\"\n\t\t}".to_string()),
        _ => None,
    };

    assert_eq!(
        String::from_utf8(expand_environment_variables_with(b"x$y", env)).unwrap(),
        "xy"
    );
    assert_eq!(
        String::from_utf8(expand_environment_variables_with(b"x${y}", env)).unwrap(),
        "xy"
    );
    // Case-sensitive
    assert_eq!(
        String::from_utf8(expand_environment_variables_with(b"x$Y", env)).unwrap(),
        "x"
    );
    assert_eq!(
        String::from_utf8(expand_environment_variables_with(b"x${Y}", env)).unwrap(),
        "x"
    );
    // Defaults only with braces
    assert_eq!(
        String::from_utf8(expand_environment_variables_with(b"x${Z:D}", env)).unwrap(),
        "xD"
    );
    assert_eq!(
        String::from_utf8(expand_environment_variables_with(b"x${Z:A B C D}", env)).unwrap(),
        "xA B C D"
    );
    assert_eq!(
        String::from_utf8(expand_environment_variables_with(b"x${Z:}", env)).unwrap(),
        "x"
    );
    assert_eq!(
        String::from_utf8(expand_environment_variables_with(b"x$y:D", env)).unwrap(),
        "xy:D"
    );
    // Multiline strips \n
    assert_eq!(
        String::from_utf8(expand_environment_variables_with(b"x$MULTILINE", env)).unwrap(),
        "x{\t\t\t\"foo\": \"bar\"\t\t}"
    );
}

#[test]
fn test_config_sha256_computed_before_env_expansion() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("mimir.yaml");
    let raw = b"target: ${MIMIR_TEST_TARGET:ingester}\n";
    fs::write(&path, raw).unwrap();

    let expected_hash = sha256_hex(raw);
    let args = vec![
        format!("-config.file={}", path.display()),
        "-config.expand-env".to_string(),
    ];
    let loaded = parse_cli_and_load_config("mimir", &args).unwrap();
    assert_eq!(loaded.config_hash_sha256.as_deref(), Some(expected_hash.as_str()));
    assert_eq!(loaded.config.target.0, vec!["ingester"]);
}

#[test]
fn test_yaml_and_cli_precedence_and_common_inheritance() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("mimir.yaml");
    fs::write(
        &path,
        r#"
target: distributor
common:
  storage:
    backend: s3
    s3:
      region: common-region
      endpoint: s3.example.com
  client_cluster_validation:
    label: common-cluster
blocks_storage:
  s3:
    region: blocks-storage-region
    bucket_name: my-blocks
ingester_client:
  grpc_client_config:
    cluster_validation:
      label: ''
"#,
    )
    .unwrap();

    let args = vec![
        format!("-config.file={}", path.display()),
        "-target=ingester,memberlist-kv".to_string(),
        "-ingester.ring.replication-factor=5".to_string(),
        "-ingester.push-grpc-method-enabled=false".to_string(),
        "-ingest-storage.enabled".to_string(),
        "-ingest-storage.kafka.address=localhost:9092".to_string(),
        "-ingest-storage.kafka.topic=ingest".to_string(),
    ];

    let loaded = parse_cli_and_load_config("mimir", &args).unwrap();
    let cfg = &loaded.config;
    assert_eq!(cfg.target.0, vec!["ingester", "memberlist-kv"]);
    assert_eq!(cfg.ingester.ring.replication_factor, 5);
    assert!(!cfg.ingester.push_grpc_method_enabled);
    assert_eq!(cfg.blocks_storage.bucket.backend_cfg.backend, "s3");
    assert_eq!(
        cfg.blocks_storage.bucket.backend_cfg.s3.region,
        "blocks-storage-region"
    );
    assert_eq!(
        cfg.blocks_storage.bucket.backend_cfg.s3.endpoint,
        "s3.example.com"
    );
    assert_eq!(
        cfg.blocks_storage.bucket.backend_cfg.s3.bucket_name,
        "my-blocks"
    );
    // Explicit empty label in YAML overrides common.client_cluster_validation
    assert_eq!(cfg.ingester_client.grpc_client_config.cluster_validation.label, "");
    // Other targets inherit common.client_cluster_validation
    assert_eq!(
        cfg.runtime_config.http_client_cluster_validation.label,
        "common-cluster"
    );
}

#[test]
fn test_common_cli_flag_inheritance() {
    let args: Vec<String> = vec![
        "-common.storage.backend=s3",
        "-common.storage.s3.region=common-region",
        "-blocks-storage.s3.region=blocks-storage-region",
        "-ruler-storage.backend=s3",
        "-ruler-storage.s3.bucket-name=ruler-bucket",
        "-common.client-cluster-validation.label=cli-cluster",
        "-ingester.client.cluster-validation.label=",
    ]
    .into_iter()
    .map(String::from)
    .collect();

    let loaded = parse_cli_and_load_config("mimir", &args).unwrap();
    let cfg = &loaded.config;
    assert_eq!(cfg.blocks_storage.bucket.backend_cfg.backend, "s3");
    assert_eq!(
        cfg.blocks_storage.bucket.backend_cfg.s3.region,
        "blocks-storage-region"
    );
    assert_eq!(cfg.ingester_client.grpc_client_config.cluster_validation.label, "");
    assert_eq!(
        cfg.runtime_config.http_client_cluster_validation.label,
        "cli-cluster"
    );
    assert_eq!(
        cfg.extra_cli_flag_value("ruler-storage.s3.region"),
        Some("common-region")
    );
    assert_eq!(
        cfg.extra_cli_flag_value("querier.store-gateway-client.cluster-validation.label"),
        Some("cli-cluster")
    );
}

#[test]
fn test_strict_unknown_field_rejection() {
    let mut cfg = MimirConfig::default();
    let err = cfg
        .load_config_from_bytes(b"unknown_top_field: 123\n", false)
        .unwrap_err();
    assert_eq!(err.exit_code(), 1);
    assert!(err.to_string().contains("unknown_top_field"));

    let mut cfg = MimirConfig::default();
    let err = cfg
        .load_config_from_bytes(b"common:\n  unknown: value\n", false)
        .unwrap_err();
    assert_eq!(err.exit_code(), 1);

    let mut cfg = MimirConfig::default();
    let err = cfg
        .load_config_from_bytes(b"querier:\n  bogus_option: true\n", false)
        .unwrap_err();
    assert_eq!(err.exit_code(), 1);
    assert!(err.to_string().contains("bogus_option"));

    let mut cfg = MimirConfig::default();
    let err = cfg
        .load_config_from_bytes(b"ingester:\n  bogus_ingester_field: 1\n", false)
        .unwrap_err();
    assert_eq!(err.exit_code(), 1);
}

#[test]
fn test_empty_node_in_yaml_keeps_defaults() {
    let mut cfg = MimirConfig::default();
    cfg.load_config_from_bytes(b"querier:\nlimits:\n", false)
        .unwrap();
    assert_eq!(cfg.limits.max_global_series_per_user, 150000);
    assert_eq!(cfg.server.http_listen_port, 8080);
}

#[test]
fn test_full_cluster_yaml_parses_cleanly_for_ingester_target() {
    let yaml = br#"
target: ingester
multitenancy_enabled: true
server:
  http_listen_port: 8080
  grpc_listen_port: 9095
distributor:
  pool:
    health_check_ingesters: true
  ring:
    kvstore:
      store: memberlist
querier:
  query_store_after: 12h
  max_concurrent: 16
compactor:
  data_dir: /data/compactor
store_gateway:
  sharding_ring:
    replication_factor: 3
ruler:
  enable_api: true
ingester:
  ring:
    replication_factor: 3
    kvstore:
      store: memberlist
blocks_storage:
  backend: filesystem
  filesystem:
    dir: /tmp/mimir-blocks
  tsdb:
    dir: /tmp/mimir-tsdb
limits:
  ingestion_rate: 50000
  max_global_series_per_user: 500000
  out_of_order_time_window: 2h
"#;

    let mut cfg = MimirConfig::default();
    cfg.load_config_from_bytes(yaml, false).unwrap();
    cfg.validate().unwrap();
    assert_eq!(cfg.target.0, vec!["ingester"]);
    assert_eq!(cfg.limits.ingestion_rate, 50000.0);
    assert_eq!(
        cfg.limits.out_of_order_time_window,
        ModelDuration::from_hours(2)
    );
}

#[test]
fn test_cli_special_flags() {
    let ver = parse_cli_and_load_config("mimir", &["-version".to_string()]).unwrap();
    assert_eq!(ver.action, CliAction::PrintVersion);
    assert!(format_version().contains("Mimir, version"));

    let mods = parse_cli_and_load_config(
        "mimir",
        &["-modules".to_string(), "-target=blah".to_string()],
    )
    .unwrap();
    assert_eq!(mods.action, CliAction::PrintModules);
    let mod_out = format_modules();
    assert!(mod_out.contains("ingester (in: all)\n"));
    assert!(mod_out.contains("memberlist-kv (in:"));

    let help = parse_cli_and_load_config("mimir", &["-help".to_string()]).unwrap();
    assert_eq!(help.action, CliAction::PrintHelp { all: false });
    assert!(help.usage_text.unwrap().contains("Usage of mimir:"));

    let help_all = parse_cli_and_load_config("mimir", &["--help-all".to_string()]).unwrap();
    assert_eq!(help_all.action, CliAction::PrintHelp { all: true });
}

#[test]
fn test_validation_errors() {
    // Overlapping filesystem paths
    let mut cfg = MimirConfig::default();
    cfg.blocks_storage.bucket.backend_cfg.filesystem.dir = "/var/mimir/data".to_string();
    cfg.blocks_storage.tsdb.dir = "/var/mimir/data/tsdb".to_string();
    assert!(cfg.validate().is_err());

    // Push gRPC disabled without ingest storage
    let mut cfg = MimirConfig::default();
    cfg.ingester.push_grpc_method_enabled = false;
    cfg.ingest_storage.enabled = false;
    assert!(cfg.validate().is_err());

    // TSDB stripe_size must be power of 2
    let mut cfg = MimirConfig::default();
    cfg.blocks_storage.tsdb.stripe_size = 1000;
    assert!(cfg.validate().is_err());

    // Ingest storage Kafka validation
    let mut cfg = MimirConfig::default();
    cfg.ingest_storage.enabled = true;
    assert!(cfg.validate().is_err());
    cfg.ingest_storage.kafka.address.set("localhost:9092");
    cfg.ingest_storage.kafka.topic = "mimir".to_string();
    assert!(cfg.validate().is_ok());
}

#[test]
fn test_durations_comprehensive() {
    assert_eq!(parse_go_duration("1h30m10s").unwrap(), 5410 * 1_000_000_000);
    assert_eq!(parse_go_duration("0").unwrap(), 0);
    assert_eq!(parse_model_duration("2w3d").unwrap(), 17 * 86400 * 1_000_000_000);
    assert_eq!(parse_duration("7d").unwrap(), 7 * 86400 * 1_000_000_000);
    assert_eq!(
        GoDuration::from_secs(65).to_string(),
        "1m5s"
    );
    assert_eq!(ModelDuration::from_days(1).to_string(), "1d");
}

#[test]
fn test_runtime_config_anchors_overrides_and_reload() {
    let default_limits = LimitsConfig {
        ingestion_rate: 25000.0,
        max_global_series_per_user: 150000,
        ..LimitsConfig::default()
    };

    // Anchored YAML (ported from TestRuntimeConfigLoader_ShouldLoadAnchoredYAML)
    let anchored = br#"
overrides:
  '1234': &id001
    ingestion_burst_size: 15000
    ingestion_rate: 1500
    max_global_series_per_metric: 7000
    max_global_series_per_user: 15000
  '1235': *id001
  '1236': *id001
"#;
    let parsed =
        parse_runtime_config_yaml(anchored, &default_limits, &InstanceLimits::default()).unwrap();
    assert_eq!(parsed.overrides.len(), 3);
    for tid in ["1234", "1235", "1236"] {
        let l = parsed.overrides[tid].as_ref().unwrap();
        assert_eq!(l.ingestion_rate, 1500.0);
        assert_eq!(l.ingestion_burst_size, 15000);
        assert_eq!(l.max_global_series_per_user, 15000);
        assert_eq!(l.max_global_series_per_metric, 7000);
    }

    // Multiple documents rejected
    let multi_doc = b"---\noverrides:\n  '1234':\n    ingestion_rate: 1\n---\n";
    let err = parse_runtime_config_yaml(multi_doc, &default_limits, &InstanceLimits::default())
        .unwrap_err();
    assert!(err.to_string().contains("multiple documents"));

    // Empty file allowed & ingester_limits stays None
    let empty = parse_runtime_config_yaml(
        b"# empty\n",
        &default_limits,
        &InstanceLimits::default(),
    )
    .unwrap();
    assert!(empty.overrides.is_empty());
    assert!(empty.ingester_limits.is_none());

    // Live reload via RuntimeConfigManager + Overrides
    let dir = tempdir().unwrap();
    let rt_path = dir.path().join("runtime.yaml");
    fs::write(
        &rt_path,
        b"overrides:\n  tenant-a:\n    max_global_series_per_user: 42000\ningester_limits:\n  max_series: 500000\n",
    )
    .unwrap();

    let mut rt_cfg = RuntimeConfigConfig::default();
    rt_cfg.file.0 = vec![rt_path.to_string_lossy().to_string()];
    rt_cfg.period = GoDuration::from_millis(50);

    let mgr =
        RuntimeConfigManager::new(rt_cfg, default_limits, InstanceLimits::default()).unwrap();
    let overrides = mgr.overrides();

    assert_eq!(overrides.max_global_series_per_user("tenant-a"), 42000);
    assert_eq!(overrides.ingestion_rate("tenant-a"), 25000.0); // inherited from global
    assert_eq!(overrides.max_global_series_per_user("tenant-b"), 150000); // fallback
    assert_eq!(mgr.ingester_limits().unwrap().max_series, 500000);

    // Modify file and reload
    fs::write(
        &rt_path,
        b"overrides:\n  tenant-a:\n    max_global_series_per_user: 99000\n  tenant-b:\n    out_of_order_time_window: 3d\n",
    )
    .unwrap();
    mgr.reload().unwrap();
    assert_eq!(overrides.max_global_series_per_user("tenant-a"), 99000);
    assert_eq!(
        overrides.out_of_order_time_window_nanos("tenant-b"),
        Duration::from_secs(3 * 86400).as_nanos() as i64
    );
    assert!(mgr.ingester_limits().is_none());
}

// Upstream: pkg/storage/ingest/config.go KafkaConfig.Validate (`invalid Kafka TLS config: %w`)
// + dskit crypto/tls ClientConfig.GetTLSConfig (OBSERV-1374).
#[test]
fn test_kafka_config_validate_tls() {
    let fixtures = format!(
        "{}/../mimir-storage/tests/fixtures/kafka_tls",
        env!("CARGO_MANIFEST_DIR")
    );
    let base = || mimir_config::KafkaConfig {
        address: mimir_config::StringSliceCSV(vec!["localhost:9092".to_string()]),
        topic: "ingest".to_string(),
        ..Default::default()
    };
    let validation_err = |cfg: &mimir_config::KafkaConfig| match cfg.validate() {
        Err(mimir_config::ConfigError::Validation(msg)) => msg,
        other => panic!("expected validation error, got {other:?}"),
    };

    // TLS settings are not resolved while TLS is disabled (as in Go).
    let mut cfg = base();
    cfg.tls_cert_path = "/does/not/exist.crt".to_string();
    cfg.validate().unwrap();

    // A client cert without a key fails GetTLSConfig.
    cfg.tls_enabled = true;
    assert_eq!(
        validation_err(&cfg),
        "invalid Kafka TLS config: certificate given but no key configured"
    );

    let mut cfg = base();
    cfg.tls_enabled = true;
    cfg.tls_ca_path = "/does/not/exist.crt".to_string();
    assert_eq!(
        validation_err(&cfg),
        "invalid Kafka TLS config: error loading ca cert: /does/not/exist.crt: open /does/not/exist.crt: no such file or directory"
    );

    let mut cfg = base();
    cfg.tls_enabled = true;
    cfg.tls_min_version = "VersionTLS99".to_string();
    assert_eq!(
        validation_err(&cfg),
        "invalid Kafka TLS config: unknown minimum TLS version: \"VersionTLS99\""
    );

    let mut cfg = base();
    cfg.tls_enabled = true;
    cfg.tls_cipher_suites = "NOPE".to_string();
    assert_eq!(
        validation_err(&cfg),
        "invalid Kafka TLS config: unsupported cipher suite: \"NOPE\""
    );

    let mut cfg = base();
    cfg.tls_enabled = true;
    cfg.tls_ca_path = format!("{fixtures}/ca.crt");
    cfg.tls_cert_path = format!("{fixtures}/client.crt");
    cfg.tls_key_path = format!("{fixtures}/client.key");
    cfg.tls_server_name = "kafka.mimir-rs.test".to_string();
    cfg.tls_min_version = "VersionTLS13".to_string();
    cfg.validate().unwrap();
}

/// pkg/ingester/ingester_ring.go:140 computes the `instance_interface_names` default at flag
/// registration time (`netutil.PrivateNetworkInterfacesWithFallback([eth0 en0])`), while
/// `-help` shows the `doc:"default=[<private network interfaces>]"` placeholder, unquoted,
/// with type "string" (`*flagext.StringSlice`).
#[test]
fn instance_interface_names_default_and_help() {
    let cfg = mimir_config::MimirConfig::default();
    assert_eq!(
        cfg.ingester.ring.instance_interface_names,
        mimir_common::netutil::default_instance_interface_names()
    );
    assert!(!cfg.ingester.ring.instance_interface_names.is_empty());

    let mut fs = mimir_config::FlagSet::new();
    cfg.register_flags(&mut fs);
    let usage = fs.format_usage("mimir", true);
    for flag in [
        "ingester.ring.instance-interface-names",
        "distributor.ring.instance-interface-names",
        "store-gateway.sharding-ring.instance-interface-names",
    ] {
        let mut lines = usage.lines().skip_while(|l| *l != format!("  -{flag} string"));
        assert!(lines.next().is_some(), "-{flag} string missing from usage");
        let help = lines.next().unwrap();
        assert!(
            help.ends_with(" (default [<private network interfaces>])"),
            "-{flag}: {help}"
        );
    }
}

/// CFG-16: Go fetches secrets from Vault (`pkg/mimir/modules.go` `initVault`) when
/// `vault.enabled` is true; mimir-rs has no Vault client and must refuse to start instead of
/// silently using the configured on-disk paths.
#[test]
fn vault_enabled_is_rejected_explicitly() {
    const MSG: &str =
        "error validating config: vault integration (-vault.enabled) is not supported by mimir-rs";
    let dir = tempdir().unwrap();
    let file = dir.path().join("mimir.yaml");
    fs::write(
        &file,
        "target: ingester\nvault:\n  enabled: true\n  url: http://vault:8200\n  mount_path: secret\n  auth:\n    type: token\n",
    )
    .unwrap();
    let cfg_arg = format!("-config.file={}", file.display());

    // YAML.
    let err = parse_cli_and_load_config("mimir", std::slice::from_ref(&cfg_arg))
        .expect_err("vault.enabled: true must be rejected");
    assert_eq!(err.to_string(), MSG);

    // Flag overriding YAML disables it again.
    let loaded =
        parse_cli_and_load_config("mimir", &[cfg_arg, "-vault.enabled=false".to_string()])
            .unwrap();
    assert!(!loaded.config.vault_enabled().unwrap());

    // Flags only (bool flag without value) with a config passing `vault.Config.Validate`.
    let err = parse_cli_and_load_config(
        "mimir",
        &[
            "-target=ingester".to_string(),
            "-vault.enabled".to_string(),
            "-vault.url=http://vault:8200".to_string(),
            "-vault.mount-path=secret".to_string(),
            "-vault.auth.type=token".to_string(),
        ],
    )
    .expect_err("-vault.enabled must be rejected");
    assert_eq!(err.to_string(), MSG);

    // pkg/vault/vault.go `Config.Validate` (wrapped with "invalid vault config" by
    // pkg/mimir/mimir.go) runs first, so an incomplete Vault config reports Go's own error.
    let err = parse_cli_and_load_config(
        "mimir",
        &["-target=ingester".to_string(), "-vault.enabled".to_string()],
    )
    .expect_err("-vault.enabled must be rejected");
    assert_eq!(
        err.to_string(),
        "error validating config: invalid vault config: empty vault URL supplied"
    );

    // Disabled (default) or explicitly false: accepted.
    let mut cfg = MimirConfig::default();
    cfg.load_config_from_bytes(b"vault:\n  enabled: false\n", false)
        .unwrap();
    assert!(!cfg.vault_enabled().unwrap());
    cfg.validate().unwrap();
}

#[test]
fn w27_a_05_metric_relabel_configs_defaults_action_lowercase_regex_null_and_yaml_emit() {
    use mimir_config::{
        parse_runtime_config_yaml, yaml_v3, yaml_v3_emit, InstanceLimits, LimitsConfig,
        MetricRelabelConfig,
    };

    // 1. Upstream TestMetricRelabelConfigLimitsLoadingFrom (YAML + map/runtime_config)
    let yaml_cfg = r#"
limits:
  metric_relabel_configs:
    - action: DROP
      source_labels: [le]
      regex: .+
"#;
    let mut cfg = MimirConfig::default();
    cfg.load_config_from_bytes(yaml_cfg.as_bytes(), false)
        .unwrap();
    assert_eq!(
        cfg.limits.metric_relabel_configs,
        vec![MetricRelabelConfig {
            source_labels: vec!["le".to_string()],
            separator: Some(";".to_string()),
            regex: Some(".+".to_string()),
            modulus: 0,
            target_label: None,
            replacement: Some("$1".to_string()),
            action: Some("drop".to_string()),
        }]
    );

    let rt_yaml = br#"
overrides:
  tenant-a:
    metric_relabel_configs:
      - action: DROP
        source_labels: [le]
        regex: .+
      - source_labels: [__name__]
        regex: null
      - target_label: foo
"#;
    let rt = parse_runtime_config_yaml(rt_yaml, &LimitsConfig::default(), &InstanceLimits::default())
        .unwrap();
    let ta = rt.overrides["tenant-a"].as_ref().unwrap();
    assert_eq!(
        ta.metric_relabel_configs,
        vec![
            MetricRelabelConfig {
                source_labels: vec!["le".to_string()],
                separator: Some(";".to_string()),
                regex: Some(".+".to_string()),
                modulus: 0,
                target_label: None,
                replacement: Some("$1".to_string()),
                action: Some("drop".to_string()),
            },
            MetricRelabelConfig {
                source_labels: vec!["__name__".to_string()],
                separator: Some(";".to_string()),
                regex: Some(String::new()),
                modulus: 0,
                target_label: None,
                replacement: Some("$1".to_string()),
                action: Some("replace".to_string()),
            },
            MetricRelabelConfig {
                source_labels: vec![],
                separator: Some(";".to_string()),
                regex: Some("(.*)".to_string()),
                modulus: 0,
                target_label: Some("foo".to_string()),
                replacement: Some("$1".to_string()),
                action: Some("replace".to_string()),
            },
        ]
    );

    // 2. Main config regex: null maps to "", and YAML marshal emits Go-compatible fields:
    //    separator: ;, replacement: $1, action: replace, regex: "" when empty,
    //    and omits regex ONLY when equal to default "(.*)".
    let main_yaml = r#"
limits:
  metric_relabel_configs:
    - source_labels: [__name__]
      regex: null
    - source_labels: [__name__, job]
      target_label: bar
      action: KEEP
"#;
    let mut cfg2 = MimirConfig::default();
    cfg2.load_config_from_bytes(main_yaml.as_bytes(), false)
        .unwrap();
    assert_eq!(
        cfg2.limits.metric_relabel_configs[0].regex.as_deref(),
        Some("")
    );
    assert_eq!(
        cfg2.limits.metric_relabel_configs[1].regex.as_deref(),
        Some("(.*)")
    );
    assert_eq!(
        cfg2.limits.metric_relabel_configs[1].action.as_deref(),
        Some("keep")
    );

    let mut val = serde_yaml::to_value(&cfg2.limits).unwrap();
    yaml_v3::order_like_go_type("validation.Limits", &mut val).unwrap();
    let rendered = yaml_v3_emit::marshal_struct(&val).unwrap();
    assert!(rendered.contains("source_labels: [__name__]"), "rendered:\n{rendered}");
    assert!(rendered.contains("source_labels: [__name__, job]"), "rendered:\n{rendered}");
    assert!(rendered.contains("separator: ;"), "rendered:\n{rendered}");
    assert!(rendered.contains("replacement: $1"), "rendered:\n{rendered}");
    assert!(rendered.contains("action: replace"), "rendered:\n{rendered}");
    assert!(rendered.contains("action: keep"), "rendered:\n{rendered}");
    assert!(rendered.contains("regex: \"\""), "rendered:\n{rendered}");
    assert!(!rendered.contains("(.*)"), "default regex must be omitted:\n{rendered}");
    assert!(!rendered.contains("separator: null"), "rendered:\n{rendered}");
    assert!(!rendered.contains("replacement: null"), "rendered:\n{rendered}");
}

#[test]
fn w27_a_07_label_value_length_over_limit_strategy_error_message() {
    let err = mimir_config::LabelValueLengthOverLimitStrategy::parse("bogus").unwrap_err();
    assert_eq!(
        err.to_string(),
        r#"invalid flag: unrecognized LabelValueLengthOverLimitStrategy: "bogus""#
    );
}
