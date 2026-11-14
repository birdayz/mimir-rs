//! 1:1 upstream-ported tests from `cmd/mimir/main_test.go` + full binary end-to-end test.

use std::fs;
use std::net::TcpListener;
use std::process::{Command, Stdio};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use mimir_cmd::grpc::{HealthCheckRequest, HealthCheckResponse};
use mimir_cmd::run_cli_test_mode;
use mimir_config::{
    expand_environment_variables, parse_config_file_parameter, FlagSet, MainFlags, MimirConfig,
};
use mimir_proto::cortexpb::{LabelPair, Sample, TimeSeries, WriteRequest};
use mimir_proto::httpgrpcpb::http_client::HttpClient;
use mimir_proto::httpgrpcpb::{Header as HttpGrpcHeader, HttpRequest};
use mimir_proto::ingesterpb::ingester_client::IngesterClient;
use mimir_proto::ingesterpb::{LabelMatcher, MatchType, QueryRequest};
use serde_yaml::Value;
use tempfile::tempdir;
use tonic::metadata::MetadataValue;
use tonic::Request;

static ENV_LOCK: Mutex<()> = Mutex::new(());

fn run_single_test(
    arguments: &[&str],
    config_yaml: Option<&str>,
    stdout_message: &str,
    stderr_message: &str,
    stdout_excluded: &str,
    stderr_excluded: &str,
) -> (String, String, i32) {
    let dir = tempdir().unwrap();
    let mut args: Vec<String> = arguments.iter().map(|s| (*s).to_string()).collect();

    if let Some(yaml) = config_yaml {
        let fpath = dir.path().join("test.yaml");
        fs::write(&fpath, yaml).unwrap();
        args.push("-config.file".to_string());
        args.push(fpath.to_string_lossy().to_string());
    }

    let out = run_cli_test_mode("./mimir", &args);
    assert!(
        out.stdout.contains(stdout_message),
        "Expected on stdout: {stdout_message:?}, stdout:\n{}",
        out.stdout
    );
    assert!(
        out.stderr.contains(stderr_message),
        "Expected on stderr: {stderr_message:?}, stderr:\n{}",
        out.stderr
    );
    if !stdout_excluded.is_empty() {
        assert!(
            !out.stdout.contains(stdout_excluded),
            "Unexpected output on stdout: {stdout_excluded:?}, stdout:\n{}",
            out.stdout
        );
    }
    if !stderr_excluded.is_empty() {
        assert!(
            !out.stderr.contains(stderr_excluded),
            "Unexpected output on stderr: {stderr_excluded:?}, stderr:\n{}",
            out.stderr
        );
    }
    (out.stdout, out.stderr, out.exit_code)
}

// Upstream: cmd/mimir/main_test.go::TestFlagParsing
#[test]
fn test_flag_parsing() {
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::set_var("TARGET", "ingester");

    // 1. help-short
    run_single_test(&["-h"], None, "Usage of", "", "", "Usage of");

    // 2. help
    run_single_test(&["-help"], None, "Usage of", "", "", "Usage of");

    // 3. help-all
    run_single_test(&["-help-all"], None, "Usage of", "", "", "Usage of");

    // 4. unknown flag
    run_single_test(
        &["-unknown.flag"],
        None,
        "",
        "Run with -help to get a list of available parameters",
        "Usage of",
        "Usage of",
    );

    // 5. new flag, with config
    run_single_test(
        &["-mem-ballast-size-bytes=100000"],
        Some("target: ingester"),
        "target: ingester",
        "",
        "",
        "",
    );

    // 6. default values
    run_single_test(&[], None, "target: all\n", "", "", "");

    // 7. config
    run_single_test(&[], Some("target: ingester"), "target: ingester\n", "", "", "");

    // 8. config with expand-env
    run_single_test(
        &["-config.expand-env"],
        Some("target: $TARGET"),
        "target: ingester\n",
        "",
        "",
        "",
    );

    // 9. config with arguments override
    run_single_test(
        &["-target=distributor"],
        Some("target: ingester"),
        "target: distributor\n",
        "",
        "",
        "",
    );

    // 10. user visible module listing
    run_single_test(&["-modules"], None, "ingester (in: all)\n", "", "", "ingester\n");

    // 11. user visible module listing flag take precedence over target flag
    run_single_test(
        &["-modules", "-target=blah"],
        None,
        "ingester (in: all)\n",
        "",
        "",
        "ingester\n",
    );

    // 12. root level configuration option specified as an empty node in YAML does not set entire config to zero value
    {
        let (stdout, _, code) = run_single_test(&[], Some("querier:"), "", "", "", "");
        assert_eq!(code, 0);
        let parsed: Value = serde_yaml::from_str(&stdout).unwrap();
        let query_store_after = parsed["querier"]["query_store_after"]
            .as_str()
            .expect("querier.query_store_after must be present and non-empty");
        assert_ne!(query_store_after, "0s");
        assert_eq!(query_store_after, "12h0m0s");
    }

    // 13. version
    run_single_test(&["-version"], None, "Mimir, version", "", "", "");

    // 14. common yaml inheritance with common config in the first place
    {
        let yaml = r#"
common:
  storage:
    backend: s3
    s3:
      region: common-region
blocks_storage:
  s3:
    region: blocks-storage-region
ruler_storage:
  backend: s3
  s3:
    bucket_name: ruler-bucket
alertmanager_storage:
  backend: local
"#;
        let (stdout, _, code) = run_single_test(&[], Some(yaml), "", "", "", "");
        assert_eq!(code, 0);
        let cfg: Value = serde_yaml::from_str(&stdout).unwrap();
        assert_eq!(cfg["blocks_storage"]["backend"].as_str(), Some("s3"));
        assert_eq!(
            cfg["blocks_storage"]["s3"]["region"].as_str(),
            Some("blocks-storage-region")
        );
        assert_eq!(cfg["ruler_storage"]["backend"].as_str(), Some("s3"));
        assert_eq!(
            cfg["ruler_storage"]["s3"]["region"].as_str(),
            Some("common-region")
        );
        assert_eq!(
            cfg["ruler_storage"]["s3"]["bucket_name"].as_str(),
            Some("ruler-bucket")
        );
        assert_eq!(cfg["alertmanager_storage"]["backend"].as_str(), Some("local"));
        assert_eq!(
            cfg["alertmanager_storage"]["s3"]["region"].as_str(),
            Some("common-region")
        );
    }

    // 15. common yaml inheritance with common config in the last place
    {
        let yaml = r#"
blocks_storage:
  s3:
    region: blocks-storage-region
ruler_storage:
  backend: s3
  s3:
    bucket_name: ruler-bucket
alertmanager_storage:
  backend: local
common:
  storage:
    backend: s3
    s3:
      region: common-region
"#;
        let (stdout, _, code) = run_single_test(&[], Some(yaml), "", "", "", "");
        assert_eq!(code, 0);
        let cfg: Value = serde_yaml::from_str(&stdout).unwrap();
        assert_eq!(cfg["blocks_storage"]["backend"].as_str(), Some("s3"));
        assert_eq!(
            cfg["blocks_storage"]["s3"]["region"].as_str(),
            Some("blocks-storage-region")
        );
        assert_eq!(cfg["ruler_storage"]["backend"].as_str(), Some("s3"));
        assert_eq!(
            cfg["ruler_storage"]["s3"]["region"].as_str(),
            Some("common-region")
        );
        assert_eq!(
            cfg["ruler_storage"]["s3"]["bucket_name"].as_str(),
            Some("ruler-bucket")
        );
        assert_eq!(cfg["alertmanager_storage"]["backend"].as_str(), Some("local"));
        assert_eq!(
            cfg["alertmanager_storage"]["s3"]["region"].as_str(),
            Some("common-region")
        );
    }

    // 16. common yaml sets a value but specific config reverts it back
    {
        let yaml = r#"
common:
  storage:
    backend: s3
    s3:
      region: common-region
blocks_storage:
  s3:
    region: ""
"#;
        let (stdout, _, code) = run_single_test(&[], Some(yaml), "", "", "", "");
        assert_eq!(code, 0);
        let cfg: Value = serde_yaml::from_str(&stdout).unwrap();
        assert_eq!(cfg["blocks_storage"]["s3"]["region"].as_str(), Some(""));
        assert_eq!(
            cfg["ruler_storage"]["s3"]["region"].as_str(),
            Some("common-region")
        );
    }

    // 17. common yaml unmarshaling is strict
    {
        let yaml = r#"
common:
  unknown: value
"#;
        let (_, stderr, code) = run_single_test(&[], Some(yaml), "", "unknown", "", "");
        assert_eq!(code, 1);
        assert!(stderr.contains("unknown"));
    }

    // 18. common yaml overridden by a common flag and specific flag
    {
        let yaml = r#"
common:
  storage:
    backend: s3
ruler_storage:
  backend: local
"#;
        let (stdout, _, code) = run_single_test(
            &[
                "-common.storage.backend=swift",
                "-blocks-storage.backend=gcs",
            ],
            Some(yaml),
            "",
            "",
            "",
            "",
        );
        assert_eq!(code, 0);
        let cfg: Value = serde_yaml::from_str(&stdout).unwrap();
        assert_eq!(cfg["blocks_storage"]["backend"].as_str(), Some("gcs"));
        assert_eq!(cfg["ruler_storage"]["backend"].as_str(), Some("swift"));
    }

    // 19. common flag inheritance
    {
        let (stdout, _, code) = run_single_test(
            &[
                "-common.storage.backend=s3",
                "-common.storage.s3.region=common-region",
                "-blocks-storage.s3.region=blocks-storage-region",
                "-ruler-storage.backend=s3",
                "-ruler-storage.s3.bucket-name=ruler-bucket",
                "-alertmanager-storage.backend=local",
            ],
            None,
            "",
            "",
            "",
            "",
        );
        assert_eq!(code, 0);
        let cfg: Value = serde_yaml::from_str(&stdout).unwrap();
        assert_eq!(cfg["blocks_storage"]["backend"].as_str(), Some("s3"));
        assert_eq!(
            cfg["blocks_storage"]["s3"]["region"].as_str(),
            Some("blocks-storage-region")
        );
        assert_eq!(cfg["ruler_storage"]["backend"].as_str(), Some("s3"));
        assert_eq!(
            cfg["ruler_storage"]["s3"]["region"].as_str(),
            Some("common-region")
        );
        assert_eq!(
            cfg["ruler_storage"]["s3"]["bucket_name"].as_str(),
            Some("ruler-bucket")
        );
        assert_eq!(cfg["alertmanager_storage"]["backend"].as_str(), Some("local"));
        assert_eq!(
            cfg["alertmanager_storage"]["s3"]["region"].as_str(),
            Some("common-region")
        );
    }
}

// Upstream: cmd/mimir/main_test.go::TestExpandEnvironmentVariables
#[test]
fn test_expand_environment_variables() {
    let _guard = ENV_LOCK.lock().unwrap();
    let cases = [
        ("x$y", "xy", "y"),
        ("x${y}", "xy", "y"),
        ("x$Y", "x", "y"),
        ("x${Y}", "x", "y"),
        ("x${Z:D}", "xD", "y"),
        ("x${Z:A B C D}", "xA B C D", "y"),
        ("x${Z:}", "x", "y"),
        ("x$y:D", "xy:D", "y"),
        (
            "x$y",
            "x{\t\t\t\"foo\": \"bar\"\t\t}",
            "{\n\t\t\t\"foo\": \"bar\"\n\t\t}",
        ),
    ];

    for (input, expected, env_val) in cases {
        std::env::set_var("y", env_val);
        let output = expand_environment_variables(input.as_bytes());
        assert_eq!(
            String::from_utf8(output).unwrap(),
            expected,
            "Input: {input}"
        );
    }
}

// Upstream: cmd/mimir/main_test.go::TestWithGoogleCloudServiceAccountEnvVariable
#[test]
fn test_with_google_cloud_service_account_env_variable() {
    let _guard = ENV_LOCK.lock().unwrap();
    let configuration = "\n\tcommon:\n\t  storage:\n\t\tgcs:\n\t\t  service_account: >-\n\t\t    ${COMMON_STORAGE_GCS_SERVICE_ACCOUNT}\n\n\tblocks_storage:\n\t  storage_prefix: monitoringmetricsv1blocks\n\t  tsdb:\n\t\tflush_blocks_on_shutdown: true\n\t";
    let service_account_env_value = "{\n\t  \"type\": \"service_account\",\n\t  \"project_id\": \"my-project\",\n\t  \"private_key_id\": \"1234abc\",\n\t  \"private_key\": \"-----BEGIN PRIVATE KEY-----\\n\\n-----END PRIVATE KEY-----\\n\",\n\t  \"client_email\": \"test@my-project.iam.gserviceaccount.com\",\n\t  \"client_id\": \"5678\",\n\t  \"auth_uri\": \"https://accounts.google.com/o/oauth2/auth\",\n\t  \"token_uri\": \"https://oauth2.googleapis.com/token\",\n\t  \"auth_provider_x509_cert_url\": \"https://www.googleapis.com/oauth2/v1/certs\",\n\t  \"client_x509_cert_url\": \"https://www.googleapis.com/robot/v1/metadata/x509/test%40my-project.iam.gserviceaccount.com\"\n\t}";
    let expected_result = "\n\tcommon:\n\t  storage:\n\t\tgcs:\n\t\t  service_account: >-\n\t\t    {\t  \"type\": \"service_account\",\t  \"project_id\": \"my-project\",\t  \"private_key_id\": \"1234abc\",\t  \"private_key\": \"-----BEGIN PRIVATE KEY-----\\n\\n-----END PRIVATE KEY-----\\n\",\t  \"client_email\": \"test@my-project.iam.gserviceaccount.com\",\t  \"client_id\": \"5678\",\t  \"auth_uri\": \"https://accounts.google.com/o/oauth2/auth\",\t  \"token_uri\": \"https://oauth2.googleapis.com/token\",\t  \"auth_provider_x509_cert_url\": \"https://www.googleapis.com/oauth2/v1/certs\",\t  \"client_x509_cert_url\": \"https://www.googleapis.com/robot/v1/metadata/x509/test%40my-project.iam.gserviceaccount.com\"\t}\n\n\tblocks_storage:\n\t  storage_prefix: monitoringmetricsv1blocks\n\t  tsdb:\n\t\tflush_blocks_on_shutdown: true\n\t";

    std::env::set_var(
        "COMMON_STORAGE_GCS_SERVICE_ACCOUNT",
        service_account_env_value,
    );
    let output = expand_environment_variables(configuration.as_bytes());
    assert_eq!(String::from_utf8(output).unwrap(), expected_result);
}

// Upstream: cmd/mimir/main_test.go::TestParseConfigFileParameter
#[test]
fn test_parse_config_file_parameter() {
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
        let args: Vec<String> = raw.split(' ').map(String::from).collect();
        let (config_file, expand_env) = parse_config_file_parameter(&args);
        assert_eq!(config_file, expected_file, "args={raw:?}");
        assert_eq!(expand_env, expected_expand, "args={raw:?}");
    }
}

// Upstream: cmd/mimir/main_test.go::TestFieldCategoryOverridesNotStale
#[test]
fn test_field_category_overrides_not_stale() {
    struct Target {
        cfg: MimirConfig,
        mf: MainFlags,
    }
    let state = Target {
        cfg: MimirConfig::default(),
        mf: MainFlags::default(),
    };
    let mut fs: FlagSet<Target> = FlagSet::new();
    state
        .cfg
        .register_flags_into(&mut fs, |t| &t.cfg, |t| &mut t.cfg);
    fs.ignored_flag("config.file", false, "Configuration file to load.");
    fs.ignored_flag(
        "config.expand-env",
        true,
        "Expands ${var} or $var in config according to the values of the environment variables.",
    );
    MainFlags::register_flags_into(&state.mf, &mut fs, |t| &t.mf, |t| &mut t.mf);

    // Every entry in pkg/util/configdoc/fieldcategory.go:categoryOverrides
    let category_overrides = [
        "server.graceful-shutdown-timeout",
        "server.grpc-conn-limit",
        "server.grpc-listen-network",
        "server.grpc-max-concurrent-streams",
        "server.grpc-max-recv-msg-size-bytes",
        "server.grpc-max-send-msg-size-bytes",
        "server.grpc-tls-ca-path",
        "server.grpc-tls-cert-path",
        "server.grpc-tls-client-auth",
        "server.grpc-tls-key-path",
        "server.grpc.keepalive.max-connection-age",
        "server.grpc.keepalive.max-connection-age-grace",
        "server.grpc.keepalive.max-connection-idle",
        "server.grpc.keepalive.min-time-between-pings",
        "server.grpc.keepalive.ping-without-stream-allowed",
        "server.grpc.keepalive.time",
        "server.grpc.keepalive.timeout",
        "server.grpc.num-workers",
        "server.http-conn-limit",
        "server.http-idle-timeout",
        "server.http-listen-network",
        "server.http-read-timeout",
        "server.http-tls-ca-path",
        "server.http-tls-cert-path",
        "server.http-tls-client-auth",
        "server.http-tls-key-path",
        "server.http-write-timeout",
        "server.log-source-ips-enabled",
        "server.log-source-ips-header",
        "server.log-source-ips-regex",
        "server.path-prefix",
        "server.register-instrumentation",
        "server.log-request-at-info-level-enabled",
        "server.proxy-protocol-enabled",
        "config.file",
        "config.expand-env",
    ];

    for flag_name in category_overrides {
        assert!(
            fs.get_value(&state, flag_name).is_some(),
            "stale category override flag: {flag_name}"
        );
    }
}

fn free_tcp_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

#[tokio::test]
async fn test_mimir_binary_ingester_e2e_grpc_http_metrics_and_sigterm() {
    let dir = tempdir().unwrap();
    let tsdb_dir = dir.path().join("tsdb");
    let blocks_dir = dir.path().join("blocks");
    let activity_log = dir.path().join("activity.log");
    let runtime_cfg_path = dir.path().join("runtime.yaml");
    fs::write(
        &runtime_cfg_path,
        "overrides:\n  tenant-e2e:\n    max_global_series_per_user: 50000\n",
    )
    .unwrap();

    let http_port = free_tcp_port();
    let grpc_port = free_tcp_port();

    let config_path = dir.path().join("mimir.yaml");
    let config_yaml = format!(
        r#"
target: ingester
multitenancy_enabled: true
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http_port}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc_port}
ingester:
  ring:
    kvstore:
      store: inmemory
    instance_id: ingester-0
    instance_addr: 127.0.0.1
    min_ready_duration: 0s
    replication_factor: 1
blocks_storage:
  backend: filesystem
  filesystem:
    dir: {blocks_dir}
  tsdb:
    dir: {tsdb_dir}
    ship_interval: 0s
activity_tracker:
  filepath: {activity_log}
  max_entries: 16
runtime_config:
  file: {runtime_cfg_path}
"#,
        blocks_dir = blocks_dir.display(),
        tsdb_dir = tsdb_dir.display(),
        activity_log = activity_log.display(),
        runtime_cfg_path = runtime_cfg_path.display(),
    );
    fs::write(&config_path, config_yaml).unwrap();

    let bin = env!("CARGO_BIN_EXE_mimir");
    let mut child = Command::new(bin)
        .arg(format!("-config.file={}", config_path.display()))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn mimir binary");

    let http_client = reqwest::Client::new();
    let ready_url = format!("http://127.0.0.1:{http_port}/ready");

    // 1. Wait for GET /ready == 200
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Ok(resp) = http_client.get(&ready_url).send().await {
            if resp.status().as_u16() == 200 {
                let body = resp.text().await.unwrap();
                assert_eq!(body, "ready\n");
                break;
            }
        }
        assert!(
            Instant::now() < deadline,
            "mimir binary did not become ready within 20s"
        );
        if let Some(status) = child.try_wait().unwrap() {
            panic!("mimir binary exited prematurely with {status}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // 2. Push samples over gRPC
    let grpc_endpoint = format!("http://127.0.0.1:{grpc_port}");
    let channel = tonic::transport::Endpoint::from_shared(grpc_endpoint.clone())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut ing_client = IngesterClient::new(channel.clone());

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    let write_req = WriteRequest {
        timeseries: vec![TimeSeries {
            labels: vec![
                LabelPair {
                    name: bytes::Bytes::from_static(b"__name__"),
                    value: bytes::Bytes::from_static(b"e2e_test_metric"),
                },
                LabelPair {
                    name: bytes::Bytes::from_static(b"job"),
                    value: bytes::Bytes::from_static(b"e2e"),
                },
            ],
            samples: vec![
                Sample {
                    value: 42.5,
                    timestamp_ms: now_ms - 1000,
                },
                Sample {
                    value: 99.0,
                    timestamp_ms: now_ms,
                },
            ],
            exemplars: Vec::new(),
            histograms: Vec::new(),
            created_timestamp: 0,
        }],
        ..Default::default()
    };

    let mut push_req = Request::new(write_req);
    push_req
        .metadata_mut()
        .insert("x-scope-orgid", MetadataValue::from_static("tenant-e2e"));
    ing_client.push(push_req).await.expect("push samples");

    // 3. Query samples back via QueryStream
    let mut query_req = Request::new(QueryRequest {
        start_timestamp_ms: now_ms - 60_000,
        end_timestamp_ms: now_ms + 60_000,
        matchers: vec![LabelMatcher {
            r#type: MatchType::Equal as i32,
            name: "__name__".to_string(),
            value: "e2e_test_metric".to_string(),
        }],
        streaming_chunks_batch_size: 0,
    });
    query_req
        .metadata_mut()
        .insert("x-scope-orgid", MetadataValue::from_static("tenant-e2e"));
    let mut stream = ing_client
        .query_stream(query_req)
        .await
        .expect("query_stream")
        .into_inner();
    let mut total_series = 0usize;
    while let Some(resp) = stream.message().await.unwrap() {
        total_series += resp.streaming_series.len();
    }
    assert!(total_series >= 1, "expected at least 1 queried series");

    // 4. Scrape GET /metrics and verify expected global + ingester metrics
    let metrics_text = http_client
        .get(format!("http://127.0.0.1:{http_port}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(metrics_text.contains("cortex_config_hash{sha256=\""));
    assert!(metrics_text.contains("mimir_build_info{"));
    assert!(metrics_text.contains("cortex_build_info{"));
    assert!(metrics_text.contains("cortex_ingester_ingested_samples_total"));

    // 5. Verify HTTP admin endpoints (/api/v1/status/buildinfo, /config, /config?mode=diff, /runtime_config, /api/v1/user_limits, /services)
    let buildinfo: serde_json::Value = http_client
        .get(format!(
            "http://127.0.0.1:{http_port}/api/v1/status/buildinfo"
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(buildinfo["status"], "success");
    assert!(buildinfo["data"]["features"]["ingest_storage"].is_string());

    let limits_resp: serde_json::Value = http_client
        .get(format!("http://127.0.0.1:{http_port}/api/v1/user_limits"))
        .header("X-Scope-OrgId", "tenant-e2e")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(limits_resp["max_global_series_per_user"], 50000);

    let cfg_diff = http_client
        .get(format!("http://127.0.0.1:{http_port}/config?mode=diff"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(cfg_diff.contains("ingester"));

    let rt_cfg = http_client
        .get(format!("http://127.0.0.1:{http_port}/runtime_config"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(rt_cfg.contains("tenant-e2e"));

    let svcs_resp: serde_json::Value = http_client
        .get(format!("http://127.0.0.1:{http_port}/services"))
        .header("Accept", "application/json")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(svcs_resp["services"].is_array());

    // 6. Test /httpgrpc.HTTP/Handle bridge
    let mut http_grpc_client = HttpClient::new(channel.clone());
    let http_grpc_resp = http_grpc_client
        .handle(HttpRequest {
            method: "GET".to_string(),
            url: "/api/v1/status/buildinfo".to_string(),
            headers: vec![HttpGrpcHeader {
                key: "Accept".to_string(),
                values: vec!["application/json".to_string()],
            }],
            body: bytes::Bytes::new(),
        })
        .await
        .expect("httpgrpc /api/v1/status/buildinfo")
        .into_inner();
    assert_eq!(http_grpc_resp.code, 200);
    let bridged_json: serde_json::Value = serde_json::from_slice(&http_grpc_resp.body).unwrap();
    assert_eq!(bridged_json["status"], "success");

    // 7. Test /grpc.health.v1.Health/Check
    let mut health_client = tonic::client::Grpc::new(channel.clone());
    health_client.ready().await.unwrap();
    let health_resp: HealthCheckResponse = health_client
        .unary(
            Request::new(HealthCheckRequest {
                service: String::new(),
            }),
            http::uri::PathAndQuery::from_static("/grpc.health.v1.Health/Check"),
            tonic::codec::ProstCodec::default(),
        )
        .await
        .expect("health check")
        .into_inner();
    assert_eq!(health_resp.status, 1); // SERVING

    drop(health_client);
    drop(http_grpc_client);
    drop(ing_client);
    drop(channel);
    drop(http_client);

    // 8. Send SIGTERM and verify clean exit 0
    #[cfg(unix)]
    {
        let pid = child.id().to_string();
        let kill_status = Command::new("kill")
            .args(["-TERM", &pid])
            .status()
            .expect("send SIGTERM");
        assert!(kill_status.success());
    }

    let output = tokio::task::spawn_blocking(move || child.wait_with_output())
        .await
        .unwrap()
        .expect("wait for mimir binary exit");
    assert_eq!(
        output.status.code(),
        Some(0),
        "expected clean exit 0 on SIGTERM, got {:?}, stderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test]
async fn test_mimir_binary_path_prefix_and_unsupported_target() {
    // 1. Unsupported standalone target returns explicit non-zero error.
    let bin = env!("CARGO_BIN_EXE_mimir");
    let out = Command::new(bin)
        .arg("-target=compactor")
        .output()
        .expect("run mimir -target=compactor");
    assert_ne!(out.status.code(), Some(0));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("compactor") && stderr.contains("not yet implemented"),
        "expected explicit unsupported target error, got: {stderr}"
    );

    // 2. server.http_path_prefix + /ingester/shutdown graceful exit
    let dir = tempdir().unwrap();
    let http_port = free_tcp_port();
    let grpc_port = free_tcp_port();
    let config_path = dir.path().join("mimir.yaml");
    fs::write(
        &config_path,
        format!(
            r#"
target: ingester
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http_port}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc_port}
  http_path_prefix: /mimir
ingester:
  ring:
    kvstore:
      store: inmemory
    instance_id: ingester-0
    instance_addr: 127.0.0.1
    min_ready_duration: 0s
    replication_factor: 1
blocks_storage:
  backend: filesystem
  filesystem:
    dir: {}
  tsdb:
    dir: {}
    ship_interval: 0s
activity_tracker:
  filepath: ""
"#,
            dir.path().join("blocks").display(),
            dir.path().join("tsdb").display()
        ),
    )
    .unwrap();

    let child = Command::new(bin)
        .arg(format!("-config.file={}", config_path.display()))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn mimir");

    let client = reqwest::Client::new();
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Ok(resp) = client
            .get(format!("http://127.0.0.1:{http_port}/mimir/ready"))
            .send()
            .await
        {
            if resp.status().as_u16() == 200 {
                break;
            }
        }
        assert!(Instant::now() < deadline, "did not become ready");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Index page and ring status under /mimir prefix
    let index_resp = client
        .get(format!("http://127.0.0.1:{http_port}/mimir/"))
        .send()
        .await
        .unwrap();
    assert_eq!(index_resp.status().as_u16(), 200);
    let index_body = index_resp.text().await.unwrap();
    assert!(index_body.contains("Grafana Mimir Admin"));
    assert!(index_body.contains("/mimir/ingester/ring"));

    // Trigger /mimir/ingester/shutdown and verify clean exit 0
    let shutdown_resp = client
        .post(format!(
            "http://127.0.0.1:{http_port}/mimir/ingester/shutdown"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(shutdown_resp.status().as_u16(), 204);
    drop(client);

    let output = tokio::task::spawn_blocking(move || child.wait_with_output())
        .await
        .unwrap()
        .expect("wait for exit");
    assert_eq!(
        output.status.code(),
        Some(0),
        "expected exit 0 on /ingester/shutdown, stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
