//! Differential conformance tests comparing `mimir-config` against upstream `target/go-mimir`.
//!
//! Ported from:
//! - github.com/grafana/mimir/cmd/mimir/main_test.go
//! - github.com/grafana/mimir/pkg/mimir/mimir_config_test.go

use std::fs;
use std::process::Command;

use mimir_config::{
    format_modules, format_version, parse_cli_and_load_config, sha256_hex, CliAction, MimirConfig,
};
use mimir_conformance::oracle::{ensure_go_oracle_built, go_mimir_bin, run_go_mimir};
use tempfile::tempdir;

#[test]
fn conformance_modules_output_matches_go_mimir_verbatim() {
    ensure_go_oracle_built().expect("go-mimir must build");

    let out = run_go_mimir(&["-modules"]).expect("run go-mimir -modules");
    assert!(
        out.status.success(),
        "go-mimir -modules failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let go_modules = String::from_utf8(out.stdout).expect("utf-8 stdout");
    let rust_modules = format_modules();

    assert_eq!(
        rust_modules, go_modules,
        "Rust -modules output must match Go mimir -modules verbatim"
    );

    let loaded = parse_cli_and_load_config("mimir", &["-modules".to_string()]).unwrap();
    assert_eq!(loaded.action, CliAction::PrintModules);
}

#[test]
fn conformance_version_and_help_flags_match_go_mimir() {
    ensure_go_oracle_built().expect("go-mimir must build");

    // -version
    let out = run_go_mimir(&["-version"]).expect("run go-mimir -version");
    assert!(out.status.success());
    let go_ver = String::from_utf8(out.stdout).unwrap();
    let rust_ver = format_version();

    let go_lines: Vec<&str> = go_ver.lines().collect();
    let rust_lines: Vec<&str> = rust_ver.lines().collect();
    assert_eq!(go_lines.len(), 3);
    assert_eq!(rust_lines.len(), 3);
    assert_eq!(rust_lines[0], go_lines[0], "header line must match");
    assert_eq!(rust_lines[2], go_lines[2], "platform line must match");

    let loaded_ver = parse_cli_and_load_config("mimir", &["--version".to_string()]).unwrap();
    assert_eq!(loaded_ver.action, CliAction::PrintVersion);

    // -help, -h, -help-all exit with code 2 in Go Mimir
    for flag in ["-help", "-h", "-help-all"] {
        let go_help = run_go_mimir(&[flag]).expect("run go-mimir help");
        assert_eq!(go_help.status.code(), Some(2), "go-mimir {flag} exit code");
        let go_stdout = String::from_utf8_lossy(&go_help.stdout);
        assert!(go_stdout.contains("Usage of"));

        let loaded = parse_cli_and_load_config("mimir", &[flag.to_string()]).unwrap();
        assert_eq!(
            loaded.action,
            CliAction::PrintHelp {
                all: flag == "-help-all"
            }
        );
        assert!(loaded.usage_text.unwrap().contains("Usage of mimir:"));
    }
}

#[test]
fn conformance_invalid_cli_flags_exit_code_2() {
    ensure_go_oracle_built().expect("go-mimir must build");

    let invalid_cases: &[&[&str]] = &[
        &["-unknown-flag"],
        &["--does.not.exist=1"],
        &["-server.http-listen-port=not-an-int"],
        &["-ingester.ring.heartbeat-period=not-a-duration"],
        &["-ingester.push-grpc-method-enabled=maybe"],
    ];

    for args in invalid_cases {
        let go_out = run_go_mimir(args).expect("run go-mimir invalid flag");
        assert_eq!(
            go_out.status.code(),
            Some(2),
            "expected exit code 2 from go-mimir for {args:?}, stderr={}",
            String::from_utf8_lossy(&go_out.stderr)
        );

        let rust_args: Vec<String> = args.iter().map(|s| (*s).to_string()).collect();
        let rust_err = parse_cli_and_load_config("mimir", &rust_args).unwrap_err();
        assert_eq!(
            rust_err.exit_code(),
            2,
            "expected exit code 2 from rust for {args:?}: {rust_err}"
        );
    }
}

#[test]
fn conformance_unknown_yaml_fields_rejected_matching_go() {
    ensure_go_oracle_built().expect("go-mimir must build");

    let bad_yamls = [
        "bogus_top_level_key: 123\n",
        "common:\n  unknown_common: true\n",
        "ingester:\n  nonexistent_ingester_field: 42\n",
        "blocks_storage:\n  tsdb:\n    nonexistent_tsdb_field: 1\n",
        "limits:\n  nonexistent_limit: 99\n",
        "querier:\n  nonexistent_querier_field: abc\n",
    ];

    let dir = tempdir().unwrap();
    for (idx, yaml) in bad_yamls.iter().enumerate() {
        let path = dir.path().join(format!("bad_{idx}.yaml"));
        fs::write(&path, yaml).unwrap();
        let cfg_flag = format!("-config.file={}", path.display());

        let go_out = run_go_mimir(&[&cfg_flag, "-print.config"]).expect("run go-mimir");
        assert_eq!(
            go_out.status.code(),
            Some(1),
            "go-mimir should reject YAML #{idx}: {yaml}"
        );

        let rust_err = parse_cli_and_load_config("mimir", &[cfg_flag]).unwrap_err();
        assert_eq!(
            rust_err.exit_code(),
            1,
            "rust should reject YAML #{idx} with exit code 1: {rust_err}"
        );
    }
}

#[test]
fn conformance_validation_failures_match_go() {
    ensure_go_oracle_built().expect("go-mimir must build");

    let invalid_arg_sets: &[&[&str]] = &[
        // Overlapping tsdb & blocks dirs
        &[
            "-target=ingester",
            "-blocks-storage.backend=filesystem",
            "-blocks-storage.filesystem.dir=/tmp/mimir-overlap-test",
            "-blocks-storage.tsdb.dir=/tmp/mimir-overlap-test/tsdb",
        ],
        // Push gRPC disabled without ingest storage
        &[
            "-target=ingester",
            "-ingester.push-grpc-method-enabled=false",
            "-ingest-storage.enabled=false",
        ],
        // Non-power-of-two TSDB stripe size
        &["-target=ingester", "-blocks-storage.tsdb.stripe-size=1000"],
        // Ingest storage enabled without kafka address/topic
        &["-target=ingester", "-ingest-storage.enabled=true"],
    ];

    for args in invalid_arg_sets {
        let mut go_args = args.to_vec();
        go_args.push("-print.config");
        let go_out = run_go_mimir(&go_args).expect("run go-mimir");
        assert_eq!(
            go_out.status.code(),
            Some(1),
            "go-mimir validation should fail for {args:?}"
        );

        let rust_args: Vec<String> = args.iter().map(|s| (*s).to_string()).collect();
        let rust_err = parse_cli_and_load_config("mimir", &rust_args).unwrap_err();
        assert_eq!(
            rust_err.exit_code(),
            1,
            "rust validation should fail with exit code 1 for {args:?}: {rust_err}"
        );
    }
}

#[test]
fn conformance_valid_yaml_common_inheritance_expand_env_and_cli_override_match_go() {
    ensure_go_oracle_built().expect("go-mimir must build");

    let dir = tempdir().unwrap();
    let tsdb_dir = dir.path().join("tsdb");
    let cfg_path = dir.path().join("mimir.yaml");

    let yaml = format!(
        r#"
target: distributor
multitenancy_enabled: true
common:
  storage:
    backend: s3
    s3:
      region: ${{TEST_COMMON_REGION:us-west-2}}
      endpoint: s3.example.com
  client_cluster_validation:
    label: ${{TEST_CLUSTER_LABEL}}
blocks_storage:
  s3:
    bucket_name: my-mimir-blocks
    region: blocks-override-region
  tsdb:
    dir: {}
    retention_period: 13h
    stripe_size: 8192
    wal_compression_enabled: true
ingester:
  ring:
    replication_factor: 3
    heartbeat_period: 10s
    kvstore:
      store: memberlist
  push_grpc_method_enabled: true
ingester_client:
  grpc_client_config:
    cluster_validation:
      label: ''
limits:
  ingestion_rate: 45000
  ingestion_burst_size: 90000
  max_global_series_per_user: 250000
  out_of_order_time_window: 1h30m
  native_histograms_ingestion_enabled: true
"#,
        tsdb_dir.display()
    );

    fs::write(&cfg_path, yaml.as_bytes()).unwrap();
    let expected_sha256 = sha256_hex(yaml.as_bytes());

    let cfg_flag = format!("-config.file={}", cfg_path.display());
    let args = [
        cfg_flag.as_str(),
        "-config.expand-env",
        "--target=ingester,memberlist-kv",
        "-ingester.ring.replication-factor=5",
        "-server.http-listen-port",
        "18080",
        "-print.config",
    ];

    // Run real Go mimir with env vars set (including newline in TEST_CLUSTER_LABEL to verify \n stripping!)
    let go_out = Command::new(go_mimir_bin())
        .args(args)
        .env("TEST_COMMON_REGION", "")
        .env("TEST_CLUSTER_LABEL", "prod-\ncluster-1\n")
        .output()
        .expect("run go-mimir");
    assert!(
        go_out.status.success(),
        "go-mimir failed: {}",
        String::from_utf8_lossy(&go_out.stderr)
    );
    let mut go_cfg = MimirConfig::default();
    go_cfg
        .load_config_from_bytes(&go_out.stdout, false)
        .expect("Rust MimirConfig must parse go-mimir -print.config YAML cleanly");

    // Set env vars for Rust (unique env names so no conflict with other tests)
    std::env::set_var("TEST_COMMON_REGION", "");
    std::env::set_var("TEST_CLUSTER_LABEL", "prod-\ncluster-1\n");

    let rust_args: Vec<String> = args
        .iter()
        .filter(|&&a| a != "-print.config")
        .map(|s| (*s).to_string())
        .collect();
    let loaded = parse_cli_and_load_config("mimir", &rust_args).unwrap();
    let cfg = &loaded.config;

    assert_eq!(
        loaded.config_hash_sha256.as_deref(),
        Some(expected_sha256.as_str())
    );

    // Compare Rust parsed config against Go's -print.config parsed values!
    assert_eq!(cfg.target, go_cfg.target);
    assert_eq!(cfg.server.http_listen_port, go_cfg.server.http_listen_port);
    assert_eq!(
        cfg.ingester.ring.replication_factor,
        go_cfg.ingester.ring.replication_factor
    );
    assert_eq!(
        cfg.ingester.ring.heartbeat_period,
        go_cfg.ingester.ring.heartbeat_period
    );
    assert_eq!(
        cfg.blocks_storage.bucket.backend_cfg.backend,
        go_cfg.blocks_storage.bucket.backend_cfg.backend
    );
    assert_eq!(
        cfg.blocks_storage.bucket.backend_cfg.s3.region,
        go_cfg.blocks_storage.bucket.backend_cfg.s3.region
    );
    assert_eq!(
        cfg.blocks_storage.bucket.backend_cfg.s3.endpoint,
        go_cfg.blocks_storage.bucket.backend_cfg.s3.endpoint
    );
    assert_eq!(
        cfg.blocks_storage.bucket.backend_cfg.s3.bucket_name,
        go_cfg.blocks_storage.bucket.backend_cfg.s3.bucket_name
    );
    assert_eq!(
        cfg.blocks_storage.tsdb.stripe_size,
        go_cfg.blocks_storage.tsdb.stripe_size
    );
    assert_eq!(
        cfg.blocks_storage.tsdb.wal_compression_enabled,
        go_cfg.blocks_storage.tsdb.wal_compression_enabled
    );
    assert_eq!(
        cfg.ingester_client
            .grpc_client_config
            .cluster_validation
            .label,
        go_cfg
            .ingester_client
            .grpc_client_config
            .cluster_validation
            .label
    );
    assert_eq!(
        cfg.runtime_config.http_client_cluster_validation.label,
        go_cfg.runtime_config.http_client_cluster_validation.label
    );
    assert_eq!(
        cfg.runtime_config.http_client_cluster_validation.label,
        "prod-cluster-1"
    );
    assert_eq!(cfg.limits.ingestion_rate, go_cfg.limits.ingestion_rate);
    assert_eq!(
        cfg.limits.max_global_series_per_user,
        go_cfg.limits.max_global_series_per_user
    );
    assert_eq!(
        cfg.limits.out_of_order_time_window,
        go_cfg.limits.out_of_order_time_window
    );
    assert_eq!(
        cfg.limits.native_histograms_ingestion_enabled,
        go_cfg.limits.native_histograms_ingestion_enabled
    );
}
