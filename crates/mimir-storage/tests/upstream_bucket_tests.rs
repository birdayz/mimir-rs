//! 1:1 upstream test ports for Mimir `pkg/storage/bucket/` (8 files, 18 tests).

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine;
use bytes::Bytes;
use mimir_config::{
    parse_cli_and_load_config_test_mode, AzureConfig, BucketConfig, BucketHTTPConfig,
    FilesystemConfig, GoDuration, S3Config, S3SSEConfig, StorageBackendConfig,
};
use mimir_storage::bucket::s3::{
    build_minio_sse, parse_kms_encryption_context, S3Bucket, ServerSideEncryption, SSE_KMS,
};
use mimir_storage::bucket::{
    azure, exthttp, read_compartment_config, BucketClient, BucketError, ObjectUploadOptions,
    ObjectVersion, TenantConfigProvider, UploadCondition, READ_COMPARTMENT_ID_PLACEHOLDER,
};
use prometheus::Registry;
use reqwest::header::HeaderMap;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

fn load_blocks_bucket_from_yaml(
    yaml_body: &str,
    extra_args: &[&str],
) -> Result<(BucketConfig, Option<String>, Option<String>), mimir_config::ConfigError> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("config.yaml");
    let full = format!("target: ingester\nblocks_storage:\n{yaml_body}");
    std::fs::write(&path, full).unwrap();
    let mut args = vec![format!("-config.file={}", path.display())];
    for a in extra_args {
        args.push((*a).to_string());
    }
    let loaded = parse_cli_and_load_config_test_mode("mimir", &args)?;
    Ok((
        loaded.config.blocks_storage.bucket,
        loaded.validation_error,
        loaded.usage_text,
    ))
}

fn indent_yaml(raw: &str) -> String {
    raw.lines()
        .map(|l| {
            if l.trim().is_empty() {
                String::new()
            } else {
                format!("  {l}")
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

// ===========================================================================
// pkg/storage/bucket/azure/bucket_client_test.go
// ===========================================================================

// Upstream: pkg/storage/bucket/azure/bucket_client_test.go::TestNewBucketClient
#[test]
fn test_azure_new_bucket_client() {
    let base_cfg = || AzureConfig {
        account_name: "test".into(),
        account_key: "test".into(),
        container_name: "test".into(),
        max_retries: 3,
        ..Default::default()
    };

    // empty endpoint should be replaced with default
    {
        let cfg = base_cfg();
        let az_cfg = azure::thanos_config(&cfg);
        assert_eq!(az_cfg.storage_account_name, "test");
        assert_eq!(az_cfg.storage_account_key, "test");
        assert_eq!(az_cfg.storage_connection_string, "");
        assert_eq!(az_cfg.container_name, "test");
        assert_eq!(az_cfg.max_retries, 3);
        assert_eq!(az_cfg.user_assigned_id, "");
        assert_eq!(az_cfg.http, cfg.http);
        assert_eq!(az_cfg.endpoint, azure::DEFAULT_ENDPOINT);
        assert!(az_cfg.validate().is_ok());
    }

    // non-empty endpoint should be kept
    {
        let cfg = AzureConfig {
            endpoint_suffix: "test-endpoint".into(),
            ..base_cfg()
        };
        let az_cfg = azure::thanos_config(&cfg);
        assert_eq!(az_cfg.endpoint, "test-endpoint");
        assert!(az_cfg.validate().is_ok());
    }

    // endpoint using connection string
    {
        let cfg = AzureConfig {
            connection_string: "test-connection-string".into(),
            endpoint_suffix: "test-endpoint".into(),
            ..base_cfg()
        };
        let az_cfg = azure::thanos_config(&cfg);
        assert_eq!(az_cfg.storage_connection_string, "test-connection-string");
        assert_eq!(az_cfg.endpoint, "test-endpoint");
    }
}

// ===========================================================================
// pkg/storage/bucket/bucket_util_test.go
// ===========================================================================

// Upstream: pkg/storage/bucket/bucket_util_test.go::TestDeletePrefix
#[tokio::test]
async fn test_delete_prefix() {
    let mem = BucketClient::new_in_memory("test");
    for k in [
        "obj",
        "prefix/1",
        "prefix/2",
        "prefix/sub1/3",
        "prefix/sub2/4",
        "outside/obj",
    ] {
        mem.upload(k, Bytes::from_static(b"hello")).await.unwrap();
    }

    let del = mem.delete_prefix("prefix").await.unwrap();
    assert_eq!(del, 4);

    let remaining = mem.list_all("").await.unwrap();
    assert_eq!(remaining.len(), 2);
    assert_eq!(remaining, vec!["obj".to_string(), "outside/obj".to_string()]);
}

// ===========================================================================
// pkg/storage/bucket/client_test.go
// ===========================================================================

// Upstream: pkg/storage/bucket/client_test.go::TestConfig_ReadCompartmentConfig
#[test]
fn test_config_read_compartment_config() {
    let placeholder = READ_COMPARTMENT_ID_PLACEHOLDER;
    let template_name = format!("blocks-rc-{placeholder}");
    let want_name = "blocks-rc-2";

    let backends = ["s3", "gcs", "azure", "swift", "filesystem"];
    for backend in backends {
        let mut cfg = BucketConfig::default();
        cfg.backend_cfg.backend = backend.to_string();
        match backend {
            "s3" => cfg.backend_cfg.s3.bucket_name = template_name.clone(),
            "gcs" => cfg.backend_cfg.gcs.bucket_name = template_name.clone(),
            "azure" => cfg.backend_cfg.azure.container_name = template_name.clone(),
            "swift" => cfg.backend_cfg.swift.container_name = template_name.clone(),
            "filesystem" => cfg.backend_cfg.filesystem.dir = template_name.clone(),
            _ => unreachable!(),
        }
        assert_eq!(cfg.bucket_name(), template_name);

        let got = read_compartment_config(&cfg, 2);
        let got_field = match backend {
            "s3" => &got.backend_cfg.s3.bucket_name,
            "gcs" => &got.backend_cfg.gcs.bucket_name,
            "azure" => &got.backend_cfg.azure.container_name,
            "swift" => &got.backend_cfg.swift.container_name,
            "filesystem" => &got.backend_cfg.filesystem.dir,
            _ => unreachable!(),
        };
        assert_eq!(got_field, want_name);
        assert_eq!(got.bucket_name(), want_name);

        // Original config is unchanged.
        let orig_field = match backend {
            "s3" => &cfg.backend_cfg.s3.bucket_name,
            "gcs" => &cfg.backend_cfg.gcs.bucket_name,
            "azure" => &cfg.backend_cfg.azure.container_name,
            "swift" => &cfg.backend_cfg.swift.container_name,
            "filesystem" => &cfg.backend_cfg.filesystem.dir,
            _ => unreachable!(),
        };
        assert_eq!(orig_field, &template_name);
    }

    // no-op when the placeholder is absent
    let mut cfg = BucketConfig::default();
    cfg.backend_cfg.backend = "s3".to_string();
    cfg.backend_cfg.s3.bucket_name = "blocks".to_string();
    assert_eq!(
        read_compartment_config(&cfg, 2).backend_cfg.s3.bucket_name,
        "blocks"
    );
}

const CONFIG_WITH_S3_BACKEND: &str = r#"
backend: s3
s3:
  endpoint:          localhost
  bucket_name:       test
  access_key_id:     xxx
  secret_access_key: yyy
  session_token:     zzz
  insecure:          true
"#;

const CONFIG_WITH_GCS_BACKEND: &str = r#"
backend: gcs
gcs:
  bucket_name:     test
  service_account: |-
    {
      "type": "service_account",
      "project_id": "id",
      "private_key_id": "id",
      "private_key": "-----BEGIN PRIVATE KEY-----\nSOMETHING\n-----END PRIVATE KEY-----\n",
      "client_email": "test@test.com",
      "client_id": "12345",
      "auth_uri": "https://accounts.google.com/o/oauth2/auth",
      "token_uri": "https://oauth2.googleapis.com/token",
      "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
      "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/test%40test.com"
    }
"#;

const CONFIG_WITH_UNKNOWN_BACKEND: &str = r#"
backend: unknown
"#;

// Upstream: pkg/storage/bucket/client_test.go::TestNewClient
#[test]
fn test_new_client() {
    // should create an S3 bucket
    {
        let (cfg, val_err, _) =
            load_blocks_bucket_from_yaml(&indent_yaml(CONFIG_WITH_S3_BACKEND), &[]).unwrap();
        assert!(val_err.is_none(), "unexpected validation error: {val_err:?}");
        let bkt = BucketClient::new_client(&cfg, "test", None).unwrap();
        assert_eq!(bkt.provider(), "S3");
        assert_eq!(bkt.name(), "test");
    }

    // should create a GCS bucket
    {
        let (cfg, val_err, _) =
            load_blocks_bucket_from_yaml(&indent_yaml(CONFIG_WITH_GCS_BACKEND), &[]).unwrap();
        assert!(val_err.is_none(), "unexpected validation error: {val_err:?}");
        let bkt = BucketClient::new_client(&cfg, "test", None).unwrap();
        assert_eq!(bkt.provider(), "GCS");
        assert_eq!(bkt.name(), "test");
    }

    // should return error on unknown backend
    {
        let (cfg, val_err, _) =
            load_blocks_bucket_from_yaml(&indent_yaml(CONFIG_WITH_UNKNOWN_BACKEND), &[]).unwrap();
        assert!(val_err.is_some());
        let err = BucketClient::new_client(&cfg, "test", None).unwrap_err();
        assert!(
            matches!(err, BucketError::UnsupportedBackend(ref b) if b == "unknown"),
            "expected UnsupportedBackend(\"unknown\"), got {err:?}"
        );
    }
}

// Upstream: pkg/storage/bucket/client_test.go::TestNewClient_MetricsOfMultipleBucketsUnderSameComponent
#[test]
fn test_new_client_metrics_of_multiple_buckets_under_same_component() {
    let (mut cfg, val_err, _) =
        load_blocks_bucket_from_yaml(&indent_yaml(CONFIG_WITH_S3_BACKEND), &[]).unwrap();
    assert!(val_err.is_none());
    cfg.backend_cfg.s3.bucket_name = format!("blocks-rc-{READ_COMPARTMENT_ID_PLACEHOLDER}");

    let reg = Registry::new();
    for compartment_id in 0..2 {
        let comp_cfg = read_compartment_config(&cfg, compartment_id);
        let bkt = BucketClient::new_client(&comp_cfg, "querier", Some(&reg)).unwrap();
        assert_eq!(bkt.name(), format!("blocks-rc-{compartment_id}"));
    }

    let families = reg.gather();
    let matching: Vec<_> = families
        .iter()
        .filter(|mf| mf.get_name() == "thanos_objstore_bucket_last_successful_upload_time")
        .flat_map(|mf| mf.get_metric())
        .collect();
    assert_eq!(matching.len(), 2);

    for expected_bucket in ["blocks-rc-0", "blocks-rc-1"] {
        let m = matching
            .iter()
            .find(|m| {
                m.get_label()
                    .iter()
                    .any(|lp| lp.get_name() == "bucket" && lp.get_value() == expected_bucket)
                    && m.get_label()
                        .iter()
                        .any(|lp| lp.get_name() == "component" && lp.get_value() == "querier")
            })
            .unwrap_or_else(|| panic!("missing metric for bucket {expected_bucket}"));
        assert_eq!(m.get_gauge().get_value(), 0.0);
    }
}

// Upstream: pkg/storage/bucket/client_test.go::TestClientMock_MockGet
#[tokio::test]
async fn test_client_mock_mock_get() {
    let expected = Bytes::from_static(b"body");
    let m = Arc::new(BucketClient::new_in_memory("test"));
    m.upload("test", expected.clone()).await.unwrap();

    let mut handles = Vec::with_capacity(1000);
    for _ in 0..1000 {
        let m = Arc::clone(&m);
        let expected = expected.clone();
        handles.push(tokio::spawn(async move {
            let actual = m.download("test").await.unwrap();
            assert_eq!(actual, expected);
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
}

// Upstream: pkg/storage/bucket/client_test.go::TestClient_ConfigValidation
#[test]
fn test_client_config_validation() {
    let cases = [
        ("valid storage_prefix", "filesystem", "helloworld", None),
        (
            "storage_prefix non-alphanumeric characters",
            "filesystem",
            "hello-world!",
            Some("storage prefix contains invalid characters"),
        ),
        (
            "storage_prefix suffixed with a slash (non-alphanumeric)",
            "filesystem",
            "helloworld/",
            Some("storage prefix contains invalid characters"),
        ),
        (
            "storage_prefix that has some character strings that have a meaning in unix paths (..)",
            "filesystem",
            "..",
            Some("storage prefix contains invalid characters"),
        ),
        (
            "storage_prefix that has some character strings that have a meaning in unix paths (.)",
            "filesystem",
            ".",
            Some("storage prefix contains invalid characters"),
        ),
        (
            "unsupported backend",
            "flash drive",
            "",
            Some("unsupported storage backend"),
        ),
    ];

    for (name, backend, prefix, expected_err) in cases {
        let cfg = BucketConfig {
            backend_cfg: StorageBackendConfig {
                backend: backend.to_string(),
                ..Default::default()
            },
            storage_prefix: prefix.to_string(),
        };
        let res = cfg.validate();
        match expected_err {
            None => assert!(res.is_ok(), "{name}: unexpected err: {res:?}"),
            Some(substr) => {
                let err = res.unwrap_err().to_string();
                assert!(
                    err.contains(substr),
                    "{name}: expected {substr:?}, got {err:?}"
                );
            }
        }
    }
}

// Upstream: pkg/storage/bucket/client_test.go::TestNewPrefixedBucketClient
#[tokio::test]
async fn test_new_prefixed_bucket_client() {
    // with prefix
    {
        let temp_dir = TempDir::new().unwrap();
        let cfg = BucketConfig {
            backend_cfg: StorageBackendConfig {
                backend: "filesystem".to_string(),
                filesystem: FilesystemConfig {
                    dir: temp_dir.path().to_string_lossy().into_owned(),
                },
                ..Default::default()
            },
            storage_prefix: "prefix".to_string(),
        };
        let client = BucketClient::new_client(&cfg, "test", None).unwrap();
        client
            .upload("file", Bytes::from_static(b"content"))
            .await
            .unwrap();
        let got = client.download("file").await.unwrap();
        assert_eq!(got.as_ref(), b"content");

        let file_path: PathBuf = temp_dir.path().join("prefix").join("file");
        assert!(file_path.exists());
        assert_eq!(std::fs::read(&file_path).unwrap(), b"content");
    }

    // without prefix
    {
        let temp_dir = TempDir::new().unwrap();
        let cfg = BucketConfig {
            backend_cfg: StorageBackendConfig {
                backend: "filesystem".to_string(),
                filesystem: FilesystemConfig {
                    dir: temp_dir.path().to_string_lossy().into_owned(),
                },
                ..Default::default()
            },
            storage_prefix: String::new(),
        };
        let client = BucketClient::new_client(&cfg, "test", None).unwrap();
        client
            .upload("file", Bytes::from_static(b"content"))
            .await
            .unwrap();
        let got = client.download("file").await.unwrap();
        assert_eq!(got.as_ref(), b"content");

        let file_path: PathBuf = temp_dir.path().join("file");
        assert!(file_path.exists());
        assert_eq!(std::fs::read(&file_path).unwrap(), b"content");
    }
}

// ===========================================================================
// pkg/storage/bucket/common/http_test.go
// ===========================================================================

// Upstream: pkg/storage/bucket/common/http_test.go::TestHTTPConfig_ToExtHTTP_DefaultTransport
#[test]
fn test_http_config_to_exthttp_default_transport() {
    let cfg = BucketHTTPConfig {
        idle_conn_timeout: GoDuration::from_secs(60),
        response_header_timeout: GoDuration::from_secs(120),
        insecure_skip_verify: true,
        tls_handshake_timeout: GoDuration::from_secs(180),
        expect_continue_timeout: GoDuration::from_secs(240),
        max_idle_connections: 10,
        max_idle_connections_per_host: 20,
        max_connections_per_host: 30,
        force_attempt_http2: true,
        tls_server_name: "server".to_string(),
        ..Default::default()
    };

    assert_eq!(exthttp::pool_idle_timeout(&cfg), Some(Duration::from_secs(60)));
    assert_eq!(
        exthttp::connect_timeout(&cfg),
        Some(exthttp::DIAL_TIMEOUT + Duration::from_secs(180))
    );
    assert_eq!(exthttp::max_idle_per_host(&cfg), 10); // min(per_host=20, max_idle=10)
    assert!(exthttp::client_options(&cfg, None).is_ok());
    assert!(exthttp::reqwest_client(&cfg, None, false).is_ok());

    // S3 HttpTransport builds with BucketHTTPConfig including force_attempt_http2: true.
    let s3_cfg = S3Config {
        endpoint: "localhost:9000".into(),
        bucket_name: "bkt".into(),
        http: cfg.clone(),
        ..Default::default()
    };
    assert!(S3Bucket::new(&s3_cfg, "test").is_ok());
}

// ===========================================================================
// pkg/storage/bucket/gcs/bucket_client_test.go
// ===========================================================================

// Upstream: pkg/storage/bucket/gcs/bucket_client_test.go::TestRetryAlwaysBucketUpload
#[tokio::test]
async fn test_retry_always_bucket_upload() {
    #[derive(Clone, Debug)]
    struct CapturedReq {
        method: String,
        target: String,
        headers: HashMap<String, String>,
    }

    async fn start_mock_gcs(
    ) -> (
        String,
        Arc<Mutex<Vec<CapturedReq>>>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let log: Arc<Mutex<Vec<CapturedReq>>> = Arc::new(Mutex::new(Vec::new()));
        let log_srv = Arc::clone(&log);

        let srv = tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    break;
                };
                let log_c = Arc::clone(&log_srv);
                tokio::spawn(async move {
                    loop {
                        let mut buf = Vec::new();
                        let mut tmp = [0u8; 4096];
                        let hdr_end = loop {
                            let Ok(n) = sock.read(&mut tmp).await else {
                                return;
                            };
                            if n == 0 {
                                return;
                            }
                            buf.extend_from_slice(&tmp[..n]);
                            if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                                break pos;
                            }
                        };
                        let head = String::from_utf8_lossy(&buf[..hdr_end]).into_owned();
                        let mut lines = head.split("\r\n");
                        let first = lines.next().unwrap_or("");
                        let mut parts = first.split_whitespace();
                        let method = parts.next().unwrap_or("").to_string();
                        let target = parts.next().unwrap_or("").to_string();
                        let headers: HashMap<String, String> = lines
                            .filter_map(|l| l.split_once(':'))
                            .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string()))
                            .collect();
                        let content_len: usize = headers
                            .get("content-length")
                            .and_then(|v| v.parse().ok())
                            .unwrap_or(0);
                        buf.drain(..hdr_end + 4);
                        while buf.len() < content_len {
                            let Ok(n) = sock.read(&mut tmp).await else {
                                return;
                            };
                            if n == 0 {
                                break;
                            }
                            buf.extend_from_slice(&tmp[..n]);
                        }
                        log_c.lock().unwrap().push(CapturedReq {
                            method,
                            target,
                            headers,
                        });
                        let body = r#"{"bucket":"buck","name":"obj","generation":"1","metageneration":"1"}"#;
                        let resp = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
                            body.len()
                        );
                        if sock.write_all(resp.as_bytes()).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        (format!("http://{addr}"), log, srv)
    }

    let make_gcs_bkt = |url: &str| {
        let mut cfg = BucketConfig::default();
        cfg.backend_cfg.backend = "gcs".into();
        cfg.backend_cfg.gcs.bucket_name = "buck".into();
        cfg.backend_cfg.gcs.enable_upload_retries = true;
        cfg.backend_cfg.gcs.service_account = format!(
            r#"{{"type":"service_account","private_key":"unused","private_key_id":"x","client_email":"x@y","gcs_base_url":"{url}","disable_oauth":true}}"#
        );
        BucketClient::new_client(&cfg, "test", None).unwrap()
    };

    // 1. applies conditional options
    {
        let (url, log, srv) = start_mock_gcs().await;
        let bkt = make_gcs_bkt(&url);

        bkt.upload_with_options(
            "obj",
            Bytes::from_static(b"data"),
            ObjectUploadOptions {
                condition: Some(UploadCondition::IfNotExists),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        bkt.upload_with_options(
            "obj",
            Bytes::from_static(b"data"),
            ObjectUploadOptions {
                condition: Some(UploadCondition::IfMatch(ObjectVersion::Generation(
                    "1234".into(),
                ))),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        bkt.upload_with_options(
            "obj",
            Bytes::from_static(b"data"),
            ObjectUploadOptions {
                condition: Some(UploadCondition::IfNotMatch(ObjectVersion::Generation(
                    "1234".into(),
                ))),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let reqs = log.lock().unwrap().clone();
        assert_eq!(reqs.len(), 3);
        assert_eq!(reqs[0].method, "PUT");
        assert_eq!(reqs[0].target, "/buck/obj");
        assert_eq!(
            reqs[0].headers.get("x-goog-if-generation-match").map(String::as_str),
            Some("0")
        );
        assert_eq!(
            reqs[1].headers.get("x-goog-if-generation-match").map(String::as_str),
            Some("1234")
        );
        assert_eq!(
            reqs[2]
                .headers
                .get("x-goog-if-generation-not-match")
                .map(String::as_str),
            Some("1234")
        );
        srv.abort();
    }

    // 2. applies content type with default chunk size
    {
        let (url, log, srv) = start_mock_gcs().await;
        let bkt = make_gcs_bkt(&url);
        bkt.upload_with_options(
            "obj",
            Bytes::from_static(b"data"),
            ObjectUploadOptions {
                content_type: Some("application/custom".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let reqs = log.lock().unwrap().clone();
        assert_eq!(reqs.len(), 1);
        assert_eq!(
            reqs[0].headers.get("content-type").map(String::as_str),
            Some("application/custom")
        );
        srv.abort();
    }

    // 3. rejects invalid conditions without sending any HTTP request
    {
        let (url, log, srv) = start_mock_gcs().await;
        let bkt = make_gcs_bkt(&url);

        let err1 = bkt
            .upload_with_options(
                "obj",
                Bytes::from_static(b"data"),
                ObjectUploadOptions {
                    condition: Some(UploadCondition::IfMatch(ObjectVersion::ETag("etag".into()))),
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(err1, BucketError::UploadOptionInvalid(_)), "{err1:?}");

        let err2 = bkt
            .upload_with_options(
                "obj",
                Bytes::from_static(b"data"),
                ObjectUploadOptions {
                    condition: Some(UploadCondition::IfMatch(ObjectVersion::Generation(
                        "not-int".into(),
                    ))),
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(err2, BucketError::UploadOptionInvalid(_)), "{err2:?}");

        assert!(
            log.lock().unwrap().is_empty(),
            "invalid upload options must not issue HTTP requests"
        );
        srv.abort();
    }
}

// ===========================================================================
// pkg/storage/bucket/prefixed_bucket_client_test.go
// ===========================================================================

// Upstream: pkg/storage/bucket/prefixed_bucket_client_test.go::TestPrefixedBucketClient
#[tokio::test]
async fn test_prefixed_bucket_client() {
    let base = BucketClient::new_in_memory("test");
    let client = base.with_prefix("p");

    // Upload
    client
        .upload("file", Bytes::from_static(b"123"))
        .await
        .unwrap();
    assert!(base.exists("p/file").await.unwrap());

    // Iter
    base.upload("p/dir/1", Bytes::from_static(b"1"))
        .await
        .unwrap();
    let entries = client.iter_dir("dir").await.unwrap();
    assert_eq!(entries, vec!["dir/1".to_string()]);

    // Get
    assert_eq!(client.download("file").await.unwrap().as_ref(), b"123");

    // GetRange
    assert_eq!(
        client.get_range("file", 0, 2).await.unwrap().as_ref(),
        b"12"
    );

    // Exists
    assert!(client.exists("file").await.unwrap());

    // Attributes
    let attrs = client.attributes("file").await.unwrap();
    assert_eq!(attrs.size, 3);

    // Delete
    client.delete("file").await.unwrap();
    assert!(!base.exists("p/file").await.unwrap());
}

// ===========================================================================
// pkg/storage/bucket/s3/config_test.go
// ===========================================================================

// Upstream: pkg/storage/bucket/s3/config_test.go::TestSSEConfig_Validate
#[test]
fn test_sse_config_validate() {
    assert!(S3SSEConfig::default().validate().is_ok());
    assert!(S3SSEConfig {
        sse_type: "unknown".into(),
        ..Default::default()
    }
    .validate()
    .unwrap_err()
    .to_string()
    .contains("unsupported S3 SSE type"));

    assert!(S3SSEConfig {
        sse_type: SSE_KMS.into(),
        kms_encryption_context: "!{}!".into(),
        ..Default::default()
    }
    .validate()
    .unwrap_err()
    .to_string()
    .contains("invalid S3 SSE encryption context"));

    assert!(S3SSEConfig {
        sse_type: SSE_KMS.into(),
        kms_encryption_context: r#"{"department": "10103.0"}"#.into(),
        ..Default::default()
    }
    .validate()
    .is_ok());
}

// Upstream: pkg/storage/bucket/s3/config_test.go::TestConfig_Validate
#[test]
fn test_s3_config_validate() {
    let base = || S3Config {
        endpoint: "s3.eu-central-1.amazonaws.com".into(),
        bucket_name: "mimir-block".into(),
        signature_version: "v4".into(),
        storage_class: "STANDARD".into(),
        ..Default::default()
    };

    // should pass with default config
    assert!(base().validate().is_ok());

    // should fail if invalid storage class is set
    assert!(S3Config {
        signature_version: "v2".into(),
        storage_class: "foo".into(),
        ..Default::default()
    }
    .validate()
    .unwrap_err()
    .to_string()
    .contains("unsupported S3 storage class"));

    // should pass if valid storage signature version is set
    assert!(S3Config {
        signature_version: "v2".into(),
        ..Default::default()
    }
    .validate()
    .is_ok());

    // should fail if invalid signature version is set
    assert!(S3Config {
        signature_version: "v5".into(),
        ..Default::default()
    }
    .validate()
    .unwrap_err()
    .to_string()
    .contains("unsupported signature version"));

    // should fail on invalid endpoint prefix
    assert!(S3Config {
        endpoint: "mimir-blocks.s3.eu-central-1.amazonaws.com".into(),
        bucket_name: "mimir-blocks".into(),
        signature_version: "v4".into(),
        storage_class: "STANDARD".into(),
        ..Default::default()
    }
    .validate()
    .unwrap_err()
    .to_string()
    .contains("the endpoint must not prefixed with the bucket name"));

    // should pass if native_aws_auth_enabled is set
    assert!(S3Config {
        signature_version: "v4".into(),
        native_aws_auth_enabled: true,
        ..Default::default()
    }
    .validate()
    .is_ok());

    // should pass with using sts endpoint
    assert!(S3Config {
        endpoint: String::new(),
        sts_endpoint: "https://sts.eu-central-1.amazonaws.com".into(),
        ..base()
    }
    .validate()
    .is_ok());

    // should not pass with using sts endpoint as its using an invalid url
    assert!(S3Config {
        endpoint: String::new(),
        sts_endpoint: "sts.eu-central-1.amazonaws.com".into(),
        ..base()
    }
    .validate()
    .unwrap_err()
    .to_string()
    .contains("sts-endpoint must be a valid url"));
}

// Upstream: pkg/storage/bucket/s3/config_test.go::TestSSEConfig_BuildMinioConfig
#[test]
fn test_sse_config_build_minio_config() {
    let header_tuple = |sse: ServerSideEncryption| {
        let mut h = HeaderMap::new();
        sse.marshal(&mut h).unwrap();
        let g = |k: &str| {
            h.get(k)
                .map(|v| v.to_str().unwrap().to_string())
                .unwrap_or_default()
        };
        (
            g("x-amz-server-side-encryption"),
            g("x-amz-server-side-encryption-aws-kms-key-id"),
            g("x-amz-server-side-encryption-context"),
        )
    };

    // SSE KMS without encryption context
    let sse = build_minio_sse(SSE_KMS, "test-key", "").unwrap().unwrap();
    assert_eq!(
        header_tuple(sse),
        ("aws:kms".into(), "test-key".into(), String::new())
    );

    // SSE KMS with encryption context
    let sse = build_minio_sse(SSE_KMS, "test-key", r#"{"department":"10103.0"}"#)
        .unwrap()
        .unwrap();
    assert_eq!(
        header_tuple(sse),
        (
            "aws:kms".into(),
            "test-key".into(),
            base64::engine::general_purpose::STANDARD.encode(r#"{"department":"10103.0"}"#),
        )
    );
}

// Upstream: pkg/storage/bucket/s3/config_test.go::TestParseKMSEncryptionContext
#[test]
fn test_parse_kms_encryption_context() {
    assert_eq!(parse_kms_encryption_context("").unwrap(), None);

    let mut expected = BTreeMap::new();
    expected.insert("department".to_string(), "10103.0".to_string());
    assert_eq!(
        parse_kms_encryption_context(r#"{"department": "10103.0"}"#).unwrap(),
        Some(expected)
    );
}

// Upstream: pkg/storage/bucket/s3/config_test.go::TestConfigParsesCredentialsInlineWithSessionToken
#[test]
fn test_config_parses_credentials_inline_with_session_token() {
    let yaml_cfg = r#"
backend: s3
s3:
  access_key_id: access key id
  secret_access_key: secret access key
  session_token: session token
"#;
    let (cfg, _, _) = load_blocks_bucket_from_yaml(&indent_yaml(yaml_cfg), &[]).unwrap();
    assert_eq!(cfg.backend_cfg.s3.access_key_id, "access key id");
    assert_eq!(cfg.backend_cfg.s3.secret_access_key, "secret access key");
    assert_eq!(cfg.backend_cfg.s3.session_token, "session token");
}

// Upstream: pkg/storage/bucket/s3/config_test.go::TestConfigRedactsCredentials
#[test]
fn test_config_redacts_credentials() {
    let cfg = S3Config {
        access_key_id: "access key id".into(),
        secret_access_key: "secret access key".into(),
        session_token: "session token".into(),
        ..Default::default()
    };

    let serialized = serde_json::to_string(&cfg).unwrap();
    assert!(serialized.contains("access key id"));
    assert!(!serialized.contains("secret access key"));
    assert!(!serialized.contains("session token"));
    assert!(serialized.contains("********"));

    // Also verify loaded MimirConfig blocks_storage.bucket.s3 redacts secrets on serialization.
    let yaml_cfg = r#"
backend: s3
s3:
  access_key_id: access key id
  secret_access_key: secret access key
  session_token: session token
"#;
    let (loaded_bkt, _, _) = load_blocks_bucket_from_yaml(&indent_yaml(yaml_cfg), &[]).unwrap();
    let loaded_out = serde_json::to_string(&loaded_bkt.backend_cfg.s3).unwrap();
    assert!(loaded_out.contains("access key id"));
    assert!(!loaded_out.contains("secret access key"));
    assert!(!loaded_out.contains("session token"));
}

// ===========================================================================
// pkg/storage/bucket/sse_bucket_client_test.go
// ===========================================================================

// Upstream: pkg/storage/bucket/sse_bucket_client_test.go::TestSSEBucketClient_Upload_ShouldInjectCustomSSEConfig
#[tokio::test]
async fn test_sse_bucket_client_upload_should_inject_custom_sse_config() {
    #[derive(Default)]
    struct MockTenantConfigProvider {
        sse_type: Mutex<String>,
        key_id: Mutex<String>,
        ctx: Mutex<String>,
    }
    impl TenantConfigProvider for MockTenantConfigProvider {
        fn s3_sse_type(&self, _: &str) -> String {
            self.sse_type.lock().unwrap().clone()
        }
        fn s3_sse_kms_key_id(&self, _: &str) -> String {
            self.key_id.lock().unwrap().clone()
        }
        fn s3_sse_kms_encryption_context(&self, _: &str) -> String {
            self.ctx.lock().unwrap().clone()
        }
    }

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let seen_headers: Arc<Mutex<Vec<HashMap<String, String>>>> = Arc::new(Mutex::new(Vec::new()));
    let seen_srv = Arc::clone(&seen_headers);

    let srv = tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                break;
            };
            let seen_c = Arc::clone(&seen_srv);
            tokio::spawn(async move {
                loop {
                    let mut buf = Vec::new();
                    let mut tmp = [0u8; 4096];
                    let hdr_end = loop {
                        let Ok(n) = sock.read(&mut tmp).await else {
                            return;
                        };
                        if n == 0 {
                            return;
                        }
                        buf.extend_from_slice(&tmp[..n]);
                        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                            break pos;
                        }
                    };
                    let head = String::from_utf8_lossy(&buf[..hdr_end]).into_owned();
                    let mut lines = head.split("\r\n");
                    let _first = lines.next();
                    let headers: HashMap<String, String> = lines
                        .filter_map(|l| l.split_once(':'))
                        .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string()))
                        .collect();
                    let is_chunked = headers
                        .get("transfer-encoding")
                        .is_some_and(|v| v.eq_ignore_ascii_case("chunked"));
                    buf.drain(..hdr_end + 4);
                    if is_chunked {
                        while !buf.windows(5).any(|w| w == b"0\r\n\r\n") {
                            let Ok(n) = sock.read(&mut tmp).await else {
                                return;
                            };
                            if n == 0 {
                                break;
                            }
                            buf.extend_from_slice(&tmp[..n]);
                        }
                    } else {
                        let content_len: usize = headers
                            .get("content-length")
                            .and_then(|v| v.parse().ok())
                            .unwrap_or(0);
                        while buf.len() < content_len {
                            let Ok(n) = sock.read(&mut tmp).await else {
                                return;
                            };
                            if n == 0 {
                                break;
                            }
                            buf.extend_from_slice(&tmp[..n]);
                        }
                    }
                    seen_c.lock().unwrap().push(headers);
                    let resp = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
                    if sock.write_all(resp.as_bytes()).await.is_err() {
                        return;
                    }
                }
            });
        }
    });

    const KMS_KEY_ID: &str = "ABC";
    const KMS_ENCRYPTION_CONTEXT: &str = r#"{"department":"10103.0"}"#;

    for with_expected_errs in [false, true] {
        seen_headers.lock().unwrap().clear();
        let s3_cfg = S3Config {
            endpoint: addr.to_string(),
            region: "test".into(),
            bucket_name: "test-bucket".into(),
            access_key_id: "test".into(),
            secret_access_key: "test".into(),
            insecure: true,
            ..Default::default()
        };
        let s3_bkt = S3Bucket::new(&s3_cfg, "test").unwrap();
        let root = BucketClient::from_s3(s3_bkt);
        let cfg_provider = Arc::new(MockTenantConfigProvider::default());

        let sse_bkt = if with_expected_errs {
            root.with_expected_not_found()
                .for_user_with_config("user-1", Arc::clone(&cfg_provider) as _)
        } else {
            root.for_user_with_config("user-1", Arc::clone(&cfg_provider) as _)
        };

        // 1. No KMS key ID configured
        sse_bkt
            .upload("test", Bytes::from_static(b"test"))
            .await
            .unwrap();
        {
            let h = seen_headers.lock().unwrap().last().unwrap().clone();
            assert_eq!(h.get("x-amz-server-side-encryption"), None);
            assert_eq!(h.get("x-amz-server-side-encryption-aws-kms-key-id"), None);
            assert_eq!(h.get("x-amz-server-side-encryption-context"), None);
        }

        // 2. KMS key ID without encryption context
        *cfg_provider.sse_type.lock().unwrap() = SSE_KMS.into();
        *cfg_provider.key_id.lock().unwrap() = KMS_KEY_ID.into();
        sse_bkt
            .upload("test", Bytes::from_static(b"test"))
            .await
            .unwrap();
        {
            let h = seen_headers.lock().unwrap().last().unwrap().clone();
            assert_eq!(
                h.get("x-amz-server-side-encryption").map(String::as_str),
                Some("aws:kms")
            );
            assert_eq!(
                h.get("x-amz-server-side-encryption-aws-kms-key-id")
                    .map(String::as_str),
                Some(KMS_KEY_ID)
            );
            assert_eq!(h.get("x-amz-server-side-encryption-context"), None);
        }

        // 3. KMS key ID and encryption context
        *cfg_provider.ctx.lock().unwrap() = KMS_ENCRYPTION_CONTEXT.into();
        sse_bkt
            .upload("test", Bytes::from_static(b"test"))
            .await
            .unwrap();
        {
            let h = seen_headers.lock().unwrap().last().unwrap().clone();
            assert_eq!(
                h.get("x-amz-server-side-encryption").map(String::as_str),
                Some("aws:kms")
            );
            assert_eq!(
                h.get("x-amz-server-side-encryption-aws-kms-key-id")
                    .map(String::as_str),
                Some(KMS_KEY_ID)
            );
            assert_eq!(
                h.get("x-amz-server-side-encryption-context")
                    .map(String::as_str),
                Some(
                    base64::engine::general_purpose::STANDARD
                        .encode(KMS_ENCRYPTION_CONTEXT)
                        .as_str()
                )
            );
        }
    }

    srv.abort();
}
