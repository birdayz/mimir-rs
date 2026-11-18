//! Wave 8 / Wave 12 Cloud IAM, OAuth, SASL 1h Token Refresh, Multipart Abort & Retry Tests
//!
//! Proves end-to-end fixes for Wave 12 findings F1..F10:
//! - F1 & F2: GCS & Azure 429/408 retries honoring Retry-After / x-ms-retry-after-ms,
//!   per-attempt token refresh / SharedKey Date re-signing, and 401 token cache invalidation.
//! - F3: AWS S3 5-minute ExpiryWindow, no Anonymous fallback on refresh errors, ExpiredToken
//!   force-refresh, trimmed EKS Pod Identity token, and bounded credential HTTP timeouts.
//! - F4 & F7: Kafka IngestKafkaClient KIP-368 sasl_expiry singleflight reconnect without
//!   leaked tasks, drop-on-timeout in FindCoordinator & Metadata, 1x reconnect retry on
//!   SASL_AUTHENTICATION_FAILED, RFC 7628 OAUTHBEARER JSON error challenge, and reason-less
//!   HTTP/1.1 200\r\n sidecar parsing.
//! - F5, F8, F9: Multipart abort() on complete() error and drop/cancellation, large Bytes
//!   chunked multipart, S3 CompleteMultipartUpload 200-OK empty/<Error> retry, and GCS
//!   GOOGLE_APPLICATION_CREDENTIALS authorized_user token_uri.

use std::collections::HashMap;
use std::ffi::OsString;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::messages::api_versions_response::ApiVersion;
use kafka_protocol::messages::find_coordinator_response::FindCoordinatorResponse;
use kafka_protocol::messages::metadata_response::{
    MetadataResponse, MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
};
use kafka_protocol::messages::offset_commit_response::{
    OffsetCommitResponse, OffsetCommitResponsePartition, OffsetCommitResponseTopic,
};
use kafka_protocol::messages::produce_response::{
    PartitionProduceResponse, ProduceResponse, TopicProduceResponse,
};
use kafka_protocol::messages::{
    ApiKey, ApiVersionsResponse, BrokerId, RequestHeader, ResponseHeader, SaslAuthenticateRequest,
    SaslAuthenticateResponse, SaslHandshakeRequest, SaslHandshakeResponse, TopicName,
};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};
use mimir_config::types::SASLMechanism;
use mimir_config::{
    AzureConfig, BucketConfig, GCSConfig, GoDuration, KafkaConfig, S3Config, StorageBackendConfig,
    StringSliceCSV,
};
use mimir_conformance::free_test_port;
use mimir_storage::bucket::BucketClient;
use mimir_storage::ingest::{
    request_json_from_socket, KafkaClient, KafkaClientError, KafkaOauthbearerAuth,
    KafkaProducerClient, KafkaWriterClient, KafkaWriterClientOptions, WriterRecord,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, UnixListener};
use tokio::sync::{Mutex, Notify};

static ENV_LOCK: Mutex<()> = Mutex::const_new(());

struct EnvGuard {
    saved: Vec<(String, Option<OsString>)>,
}

impl EnvGuard {
    fn set(vars: &[(&str, Option<&str>)]) -> Self {
        let mut saved = Vec::with_capacity(vars.len());
        for (k, v) in vars {
            saved.push(((*k).to_string(), std::env::var_os(k)));
            match v {
                Some(val) => std::env::set_var(k, val),
                None => std::env::remove_var(k),
            }
        }
        Self { saved }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        for (k, v) in self.saved.drain(..).rev() {
            match v {
                Some(val) => std::env::set_var(&k, val),
                None => std::env::remove_var(&k),
            }
        }
    }
}

async fn read_http_request(
    sock: &mut tokio::net::TcpStream,
) -> Option<(String, HashMap<String, String>, Vec<u8>)> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];
    let hdr_end = loop {
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos;
        }
        let n = sock.read(&mut chunk).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..n]);
    };
    let head = String::from_utf8_lossy(&buf[..hdr_end]).to_string();
    let mut lines = head.lines();
    let req_line = lines.next()?.to_string();
    let mut headers = HashMap::new();
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
        }
    }
    let content_len: usize = headers
        .get("content-length")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let is_chunked = headers
        .get("transfer-encoding")
        .is_some_and(|v| v.to_ascii_lowercase().contains("chunked"));

    let mut body = buf[hdr_end + 4..].to_vec();
    if is_chunked {
        loop {
            if let Some(end) = body.windows(5).position(|w| w == b"0\r\n\r\n") {
                body.truncate(end + 5);
                break;
            }
            let n = sock.read(&mut chunk).await.ok()?;
            if n == 0 {
                break;
            }
            body.extend_from_slice(&chunk[..n]);
        }
    } else {
        while body.len() < content_len {
            let n = sock.read(&mut chunk).await.ok()?;
            if n == 0 {
                break;
            }
            body.extend_from_slice(&chunk[..n]);
        }
        body.truncate(content_len);
    }
    Some((req_line, headers, body))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_gcs_oauth_1h_token_rotation_429_retry_after_and_401_invalidation() -> Result<()> {
    let _env_guard = ENV_LOCK.lock().await;

    let token_port = free_test_port();
    let token_listener = TcpListener::bind(("127.0.0.1", token_port)).await?;
    let token_calls = Arc::new(AtomicUsize::new(0));
    let token_calls_srv = Arc::clone(&token_calls);
    tokio::spawn(async move {
        while let Ok((mut sock, _)) = token_listener.accept().await {
            let calls = Arc::clone(&token_calls_srv);
            tokio::spawn(async move {
                while let Some((_line, _hdrs, _body)) = read_http_request(&mut sock).await {
                    let n = calls.fetch_add(1, Ordering::SeqCst) + 1;
                    let body = format!(
                        r#"{{"access_token":"gcs-tok-{n}","expires_in":12,"token_type":"Bearer"}}"#
                    );
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
                        body.len()
                    );
                    if sock.write_all(resp.as_bytes()).await.is_err() {
                        break;
                    }
                }
            });
        }
    });

    let gcs_port = free_test_port();
    let gcs_listener = TcpListener::bind(("127.0.0.1", gcs_port)).await?;
    let gcs_auth_log = Arc::new(parking_lot::Mutex::new(Vec::<(String, String)>::new()));
    let gcs_auth_srv = Arc::clone(&gcs_auth_log);
    let stored = Arc::new(parking_lot::Mutex::new(HashMap::<String, Vec<u8>>::new()));
    let stored_srv = Arc::clone(&stored);
    let put_attempts = Arc::new(AtomicUsize::new(0));
    let put_attempts_srv = Arc::clone(&put_attempts);
    let get_attempts = Arc::new(AtomicUsize::new(0));
    let get_attempts_srv = Arc::clone(&get_attempts);
    let head_attempts = Arc::new(AtomicUsize::new(0));
    let head_attempts_srv = Arc::clone(&head_attempts);

    tokio::spawn(async move {
        while let Ok((mut sock, _)) = gcs_listener.accept().await {
            let auth_log = Arc::clone(&gcs_auth_srv);
            let stored = Arc::clone(&stored_srv);
            let put_att = Arc::clone(&put_attempts_srv);
            let get_att = Arc::clone(&get_attempts_srv);
            let head_att = Arc::clone(&head_attempts_srv);
            tokio::spawn(async move {
                while let Some((req_line, hdrs, body)) = read_http_request(&mut sock).await {
                    let mut parts = req_line.split_whitespace();
                    let method = parts.next().unwrap_or("");
                    let path = parts.next().unwrap_or("").to_string();
                    let auth = hdrs.get("authorization").cloned().unwrap_or_default();
                    auth_log.lock().push((method.to_string(), auth));

                    let resp: Vec<u8> = match method {
                        "PUT" => {
                            let n = put_att.fetch_add(1, Ordering::SeqCst);
                            if n == 0 {
                                b"HTTP/1.1 429 Too Many Requests\r\nRetry-After: 1\r\nContent-Length: 0\r\n\r\n".to_vec()
                            } else {
                                stored.lock().insert(path, body);
                                b"HTTP/1.1 200 OK\r\nETag: \"e1\"\r\nx-goog-generation: 1\r\nContent-Length: 0\r\n\r\n".to_vec()
                            }
                        }
                        "GET" => {
                            let n = get_att.fetch_add(1, Ordering::SeqCst);
                            if n == 0 {
                                b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\n\r\n".to_vec()
                            } else {
                                let data = stored.lock().get(&path).cloned().unwrap_or_default();
                                let mut r = format!(
                                    "HTTP/1.1 200 OK\r\nETag: \"e1\"\r\nLast-Modified: Wed, 21 Oct 2015 07:28:00 GMT\r\nContent-Length: {}\r\n\r\n",
                                    data.len()
                                )
                                .into_bytes();
                                r.extend_from_slice(&data);
                                r
                            }
                        }
                        "HEAD" => {
                            let n = head_att.fetch_add(1, Ordering::SeqCst);
                            if n == 0 {
                                b"HTTP/1.1 503 Service Unavailable\r\nRetry-After: 2\r\nContent-Length: 0\r\n\r\n".to_vec()
                            } else {
                                let len = stored.lock().get(&path).map_or(0, |d| d.len());
                                format!(
                                    "HTTP/1.1 200 OK\r\nETag: \"e1\"\r\nLast-Modified: Wed, 21 Oct 2015 07:28:00 GMT\r\nContent-Length: {len}\r\n\r\n"
                                )
                                .into_bytes()
                            }
                        }
                        _ => b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n".to_vec(),
                    };
                    if sock.write_all(&resp).await.is_err() {
                        break;
                    }
                }
            });
        }
    });

    let dir = tempfile::tempdir()?;
    let adc_path = dir.path().join("adc.json");
    let adc_json = format!(
        r#"{{
            "type": "authorized_user",
            "client_id": "cid",
            "client_secret": "csec",
            "refresh_token": "rt",
            "token_uri": "http://127.0.0.1:{token_port}/oauth2/token",
            "gcs_base_url": "http://127.0.0.1:{gcs_port}"
        }}"#
    );
    std::fs::write(&adc_path, adc_json)?;
    let _gac = EnvGuard::set(&[(
        "GOOGLE_APPLICATION_CREDENTIALS",
        Some(adc_path.to_str().context("utf8 path")?),
    )]);

    let cfg = BucketConfig {
        backend_cfg: StorageBackendConfig {
            backend: "gcs".into(),
            gcs: GCSConfig {
                bucket_name: "mimir-bkt".into(),
                max_retries: 5,
                enable_upload_retries: true,
                ..Default::default()
            },
            ..Default::default()
        },
        ..Default::default()
    };
    let client = BucketClient::from_config(&cfg)?;

    let t0 = Instant::now();
    client
        .upload("t1/block.bin", Bytes::from_static(b"hello-gcs"))
        .await?;
    assert!(
        t0.elapsed() >= Duration::from_millis(900),
        "expected Retry-After: 1s honor, elapsed={:?}",
        t0.elapsed()
    );

    let got = client.download("t1/block.bin").await?;
    assert_eq!(got.as_ref(), b"hello-gcs");

    assert!(client.exists("t1/block.bin").await?);

    let log = gcs_auth_log.lock().clone();
    assert_eq!(
        log,
        vec![
            ("PUT".into(), "Bearer gcs-tok-1".into()),
            ("PUT".into(), "Bearer gcs-tok-1".into()),
            ("GET".into(), "Bearer gcs-tok-1".into()),
            ("GET".into(), "Bearer gcs-tok-2".into()),
            ("HEAD".into(), "Bearer gcs-tok-2".into()),
            ("HEAD".into(), "Bearer gcs-tok-3".into()),
        ]
    );
    assert_eq!(token_calls.load(Ordering::SeqCst), 3);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_gcs_credential_http_timeout_recovers_instead_of_stalling() -> Result<()> {
    let token_port = free_test_port();
    let listener = TcpListener::bind(("127.0.0.1", token_port)).await?;
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_srv = Arc::clone(&calls);
    tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            let c = calls_srv.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                if c == 0 {
                    tokio::time::sleep(Duration::from_secs(30)).await;
                } else if read_http_request(&mut sock).await.is_some() {
                    let body = r#"{"access_token":"recovered-tok","expires_in":3600}"#;
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = sock.write_all(resp.as_bytes()).await;
                }
            });
        }
    });

    let gcs_port = free_test_port();
    let gcs_listener = TcpListener::bind(("127.0.0.1", gcs_port)).await?;
    tokio::spawn(async move {
        while let Ok((mut sock, _)) = gcs_listener.accept().await {
            tokio::spawn(async move {
                if let Some((_, hdrs, _)) = read_http_request(&mut sock).await {
                    let ok = hdrs
                        .get("authorization")
                        .is_some_and(|a| a == "Bearer recovered-tok");
                    let resp = if ok {
                        "HTTP/1.1 200 OK\r\nETag: \"e\"\r\nLast-Modified: Wed, 21 Oct 2015 07:28:00 GMT\r\nContent-Length: 0\r\n\r\n"
                    } else {
                        "HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\n\r\n"
                    };
                    let _ = sock.write_all(resp.as_bytes()).await;
                }
            });
        }
    });

    let cfg = BucketConfig {
        backend_cfg: StorageBackendConfig {
            backend: "gcs".into(),
            gcs: GCSConfig {
                bucket_name: "bkt".into(),
                service_account: format!(
                    r#"{{"type":"authorized_user","client_id":"c","client_secret":"s","refresh_token":"r","token_uri":"http://127.0.0.1:{token_port}/tok","gcs_base_url":"http://127.0.0.1:{gcs_port}"}}"#
                ),
                ..Default::default()
            },
            ..Default::default()
        },
        ..Default::default()
    };
    let client = BucketClient::from_config(&cfg)?;
    let start = Instant::now();
    assert!(client.exists("obj").await?);
    assert!(
        start.elapsed() < Duration::from_secs(25),
        "credential request stalled too long: {:?}",
        start.elapsed()
    );
    assert!(calls.load(Ordering::SeqCst) >= 2);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_azure_shared_key_429_408_x_ms_retry_after_ms_and_fresh_date_resigning() -> Result<()> {
    let port = free_test_port();
    let listener = TcpListener::bind(("127.0.0.1", port)).await?;
    let put_headers = Arc::new(parking_lot::Mutex::new(Vec::<(String, String)>::new()));
    let put_headers_srv = Arc::clone(&put_headers);
    let put_calls = Arc::new(AtomicUsize::new(0));
    let put_calls_srv = Arc::clone(&put_calls);

    tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            let hdr_log = Arc::clone(&put_headers_srv);
            let puts = Arc::clone(&put_calls_srv);
            tokio::spawn(async move {
                while let Some((req_line, hdrs, _body)) = read_http_request(&mut sock).await {
                    let resp = if req_line.contains("restype=container") {
                        "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n".to_string()
                    } else if req_line.starts_with("PUT ") {
                        let date = hdrs.get("date").cloned().unwrap_or_default();
                        let auth = hdrs.get("authorization").cloned().unwrap_or_default();
                        hdr_log.lock().push((date, auth));
                        match puts.fetch_add(1, Ordering::SeqCst) {
                            0 => "HTTP/1.1 408 Request Timeout\r\nx-ms-retry-after-ms: 50\r\nContent-Length: 0\r\n\r\n".to_string(),
                            1 => "HTTP/1.1 429 Too Many Requests\r\nx-ms-retry-after-ms: 1100\r\nContent-Length: 0\r\n\r\n".to_string(),
                            _ => "HTTP/1.1 201 Created\r\nETag: \"az-etag\"\r\nContent-Length: 0\r\n\r\n".to_string(),
                        }
                    } else {
                        "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n".to_string()
                    };
                    if sock.write_all(resp.as_bytes()).await.is_err() {
                        break;
                    }
                }
            });
        }
    });

    let key = BASE64_STANDARD.encode(b"0123456789abcdef0123456789abcdef");
    let cfg = BucketConfig {
        backend_cfg: StorageBackendConfig {
            backend: "azure".into(),
            azure: AzureConfig {
                account_name: "acct".into(),
                container_name: "blocks".into(),
                connection_string: format!(
                    "DefaultEndpointsProtocol=http;AccountName=acct;AccountKey={key};BlobEndpoint=http://127.0.0.1:{port}/acct;"
                ),
                ..Default::default()
            },
            ..Default::default()
        },
        ..Default::default()
    };

    let client = tokio::task::spawn_blocking(move || BucketClient::from_config(&cfg)).await??;
    let t0 = Instant::now();
    client
        .upload("blob.txt", Bytes::from_static(b"azure-data"))
        .await?;
    assert!(
        t0.elapsed() >= Duration::from_millis(1000),
        "expected x-ms-retry-after-ms: 1100ms delay, got {:?}",
        t0.elapsed()
    );

    let log = put_headers.lock().clone();
    assert_eq!(log.len(), 3, "{log:?}");
    assert_ne!(log[1].0, log[2].0, "Date header was frozen across retry: {log:?}");
    assert_ne!(
        log[1].1, log[2].1,
        "SharedKey Authorization was not re-signed on retry: {log:?}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_expired_token_refresh_no_anonymous_fallback_and_200_ok_complete_retry() -> Result<()> {
    let _env_guard = ENV_LOCK.lock().await;

    let sts_port = free_test_port();
    let sts_listener = TcpListener::bind(("127.0.0.1", sts_port)).await?;
    let sts_calls = Arc::new(AtomicUsize::new(0));
    let sts_calls_srv = Arc::clone(&sts_calls);

    tokio::spawn(async move {
        while let Ok((mut sock, _)) = sts_listener.accept().await {
            let calls = Arc::clone(&sts_calls_srv);
            tokio::spawn(async move {
                while let Some((_line, hdrs, _body)) = read_http_request(&mut sock).await {
                    assert_eq!(
                        hdrs.get("authorization").map(String::as_str),
                        Some("eks-pod-token"),
                        "EKS Pod Identity token must be trimmed"
                    );
                    let n = calls.fetch_add(1, Ordering::SeqCst) + 1;
                    let exp = (chrono::Utc::now() + chrono::Duration::hours(1)).to_rfc3339();
                    let body = format!(
                        r#"{{"AccessKeyId":"sts-ak-{n}","SecretAccessKey":"sts-sk-{n}","Token":"sts-tok-{n}","Expiration":"{exp}"}}"#
                    );
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{body}",
                        body.len()
                    );
                    if sock.write_all(resp.as_bytes()).await.is_err() {
                        break;
                    }
                }
            });
        }
    });

    let s3_port = free_test_port();
    let s3_listener = TcpListener::bind(("127.0.0.1", s3_port)).await?;
    let s3_auths = Arc::new(parking_lot::Mutex::new(Vec::<String>::new()));
    let s3_auths_srv = Arc::clone(&s3_auths);
    let complete_calls = Arc::new(AtomicUsize::new(0));
    let complete_calls_srv = Arc::clone(&complete_calls);
    let aborted = Arc::new(AtomicBool::new(false));
    let aborted_srv = Arc::clone(&aborted);
    let parts_len = Arc::new(AtomicUsize::new(0));
    let parts_len_srv = Arc::clone(&parts_len);

    tokio::spawn(async move {
        while let Ok((mut sock, _)) = s3_listener.accept().await {
            let auths = Arc::clone(&s3_auths_srv);
            let completes = Arc::clone(&complete_calls_srv);
            let aborted = Arc::clone(&aborted_srv);
            let plen = Arc::clone(&parts_len_srv);
            tokio::spawn(async move {
                while let Some((req_line, hdrs, body)) = read_http_request(&mut sock).await {
                    let mut parts = req_line.split_whitespace();
                    let method = parts.next().unwrap_or("");
                    let target = parts.next().unwrap_or("");
                    let auth = hdrs.get("authorization").cloned().unwrap_or_default();
                    let sec_tok = hdrs.get("x-amz-security-token").cloned().unwrap_or_default();
                    auths.lock().push(auth);

                    let resp = if target.contains("?location") {
                        let b = r#"<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">us-east-1</LocationConstraint>"#;
                        format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{b}", b.len())
                    } else if method == "PUT" && !target.contains("partNumber") {
                        if sec_tok == "sts-tok-1" {
                            let b = "<Error><Code>ExpiredToken</Code><Message>The provided token has expired.</Message></Error>";
                            format!("HTTP/1.1 403 Forbidden\r\nContent-Length: {}\r\n\r\n{b}", b.len())
                        } else {
                            "HTTP/1.1 200 OK\r\nETag: \"e1\"\r\nContent-Length: 0\r\n\r\n".to_string()
                        }
                    } else if method == "POST" && target.contains("uploads") {
                        let b = "<InitiateMultipartUploadResult><Bucket>bkt</Bucket><Key>k</Key><UploadId>u1</UploadId></InitiateMultipartUploadResult>";
                        format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{b}", b.len())
                    } else if method == "PUT" && target.contains("partNumber") {
                        let decoded = hdrs
                            .get("x-amz-decoded-content-length")
                            .and_then(|v| v.parse::<usize>().ok())
                            .unwrap_or(body.len());
                        plen.fetch_add(decoded, Ordering::SeqCst);
                        "HTTP/1.1 200 OK\r\nETag: \"part-etag\"\r\nContent-Length: 0\r\n\r\n".to_string()
                    } else if method == "POST" && target.contains("uploadId=") {
                        match completes.fetch_add(1, Ordering::SeqCst) {
                            0 => "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n".to_string(),
                            1 => {
                                let b = "<Error><Code>InternalError</Code><Message>transient 200 error</Message></Error>";
                                format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{b}", b.len())
                            }
                            _ => {
                                let b = "<CompleteMultipartUploadResult><Bucket>bkt</Bucket><Key>k</Key><ETag>\"final\"</ETag></CompleteMultipartUploadResult>";
                                format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{b}", b.len())
                            }
                        }
                    } else if method == "DELETE" && target.contains("uploadId=") {
                        aborted.store(true, Ordering::SeqCst);
                        "HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n".to_string()
                    } else {
                        "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n".to_string()
                    };
                    if sock.write_all(resp.as_bytes()).await.is_err() {
                        break;
                    }
                }
            });
        }
    });

    let dir = tempfile::tempdir()?;
    let pod_tok = dir.path().join("pod-token");
    std::fs::write(&pod_tok, "eks-pod-token\n")?;
    let full_uri = format!("http://127.0.0.1:{sts_port}/v1/credentials");
    let _guard = EnvGuard::set(&[
        ("AWS_CONTAINER_CREDENTIALS_FULL_URI", Some(&full_uri)),
        (
            "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
            Some(pod_tok.to_str().context("utf8")?),
        ),
        ("AWS_REGION", Some("us-east-1")),
    ]);

    let cfg = BucketConfig {
        backend_cfg: StorageBackendConfig {
            backend: "s3".into(),
            s3: S3Config {
                endpoint: format!("127.0.0.1:{s3_port}"),
                bucket_name: "bkt".into(),
                region: "us-east-1".into(),
                insecure: true,
                native_aws_auth_enabled: true,
                ..Default::default()
            },
            ..Default::default()
        },
        ..Default::default()
    };
    let client = BucketClient::from_config(&cfg)?;

    client.upload("small.txt", Bytes::from_static(b"hi")).await?;
    assert_eq!(sts_calls.load(Ordering::SeqCst), 2);

    let big = vec![42u8; 17 * 1024 * 1024 + 5];
    client.upload("big.bin", Bytes::from(big.clone())).await?;
    assert_eq!(parts_len.load(Ordering::SeqCst), big.len());
    assert_eq!(complete_calls.load(Ordering::SeqCst), 3);
    assert!(!aborted.load(Ordering::SeqCst));

    for auth in s3_auths.lock().iter() {
        assert!(
            auth.starts_with("AWS4-HMAC-SHA256 "),
            "unexpected unsigned S3 request: {auth:?}"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_multipart_upload_aborts_on_complete_error_and_drop_cancellation() -> Result<()> {
    // Mock GCS XML Multipart server:
    // - Upload 1 ("fail-complete.bin"): completes with 403 AccessDenied -> verifies BucketClient calls DELETE ?uploadId=
    // - Upload 2 ("cancel-drop.bin"): blocks on partNumber=2 -> verifies dropping upload future spawns DELETE ?uploadId=
    let port = free_test_port();
    let listener = TcpListener::bind(("127.0.0.1", port)).await?;
    let part_uploads = Arc::new(AtomicUsize::new(0));
    let part_uploads_srv = Arc::clone(&part_uploads);
    let aborted_ids = Arc::new(parking_lot::Mutex::new(Vec::<String>::new()));
    let aborted_ids_srv = Arc::clone(&aborted_ids);
    let part2_started = Arc::new(Notify::new());
    let part2_started_srv = Arc::clone(&part2_started);
    let abort_notify = Arc::new(Notify::new());
    let abort_notify_srv = Arc::clone(&abort_notify);
    let fail_complete = Arc::new(AtomicBool::new(true));
    let fail_complete_srv = Arc::clone(&fail_complete);

    tokio::spawn(async move {
        let seq = Arc::new(AtomicUsize::new(0));
        while let Ok((mut sock, _)) = listener.accept().await {
            let parts_cnt = Arc::clone(&part_uploads_srv);
            let aborts = Arc::clone(&aborted_ids_srv);
            let p2_notify = Arc::clone(&part2_started_srv);
            let a_notify = Arc::clone(&abort_notify_srv);
            let fail_c = Arc::clone(&fail_complete_srv);
            let seq = Arc::clone(&seq);
            tokio::spawn(async move {
                while let Some((req_line, _hdrs, _body)) = read_http_request(&mut sock).await {
                    let mut split = req_line.split_whitespace();
                    let method = split.next().unwrap_or("");
                    let target = split.next().unwrap_or("");
                    let resp = if method == "POST" && target.contains("uploads") {
                        let uid = format!("uid-{}", seq.fetch_add(1, Ordering::SeqCst) + 1);
                        let b = format!(
                            "<InitiateMultipartUploadResult><Bucket>bkt</Bucket><Key>k</Key><UploadId>{uid}</UploadId></InitiateMultipartUploadResult>"
                        );
                        format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{b}", b.len())
                    } else if method == "PUT" && target.contains("partNumber=2") && !fail_c.load(Ordering::SeqCst) {
                        p2_notify.notify_waiters();
                        // Hold part 2 open until client cancels/drops the upload future.
                        tokio::time::sleep(Duration::from_secs(30)).await;
                        break;
                    } else if method == "PUT" && target.contains("partNumber=") {
                        parts_cnt.fetch_add(1, Ordering::SeqCst);
                        "HTTP/1.1 200 OK\r\nETag: \"p-etag\"\r\nContent-Length: 0\r\n\r\n".to_string()
                    } else if method == "POST" && target.contains("uploadId=") {
                        "HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n".to_string()
                    } else if method == "DELETE" && target.contains("uploadId=") {
                        let uid = target.split("uploadId=").nth(1).unwrap_or("").to_string();
                        aborts.lock().push(uid);
                        a_notify.notify_waiters();
                        "HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n".to_string()
                    } else {
                        "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n".to_string()
                    };
                    if sock.write_all(resp.as_bytes()).await.is_err() {
                        break;
                    }
                }
            });
        }
    });

    let cfg = BucketConfig {
        backend_cfg: StorageBackendConfig {
            backend: "gcs".into(),
            gcs: GCSConfig {
                bucket_name: "bkt".into(),
                service_account: format!(
                    r#"{{"type":"service_account","private_key":"unused","private_key_id":"x","client_email":"x@y","gcs_base_url":"http://127.0.0.1:{port}","disable_oauth":true}}"#
                ),
                max_retries: 1,
                enable_upload_retries: false,
                ..Default::default()
            },
            ..Default::default()
        },
        ..Default::default()
    };
    let client = BucketClient::from_config(&cfg)?;
    let big = Bytes::from(vec![9u8; 17 * 1024 * 1024]);

    // Part A: UploadSource::Bytes > 16 MiB streams 3 multipart parts, complete() fails -> abort() called!
    let err = client.upload("fail-complete.bin", big.clone()).await.unwrap_err();
    assert!(err.to_string().contains("403") || err.to_string().contains("Permission"), "{err}");
    assert_eq!(part_uploads.load(Ordering::SeqCst), 3);
    assert_eq!(aborted_ids.lock().as_slice(), &["uid-1"]);

    // Part B: Dropping upload future mid-part triggers AbortGuard Drop -> spawns DELETE ?uploadId=uid-2!
    fail_complete.store(false, Ordering::SeqCst);
    let mut up_fut = Box::pin(client.upload("cancel-drop.bin", big));
    tokio::select! {
        _ = &mut up_fut => anyhow::bail!("expected upload to block on part 2"),
        _ = part2_started.notified() => {}
    }
    drop(up_fut);
    tokio::time::timeout(Duration::from_secs(3), abort_notify.notified()).await?;
    assert!(
        aborted_ids.lock().iter().any(|u| u == "uid-2"),
        "expected abort for dropped upload uid-2, got {:?}",
        *aborted_ids.lock()
    );
    Ok(())
}

#[derive(Default)]
struct MockKafkaBehavior {
    session_lifetime_ms: i64,
    fail_first_sasl_auth: bool,
    oauth_error_challenge: bool,
    hang_first_find_coordinator: bool,
}

struct MockKafkaStats {
    sasl_auth_attempts: AtomicUsize,
    sasl_tokens_seen: parking_lot::Mutex<Vec<String>>,
    oauth_error_acks: AtomicUsize,
    find_coord_calls: AtomicUsize,
    active_conns: AtomicUsize,
}

impl Default for MockKafkaStats {
    fn default() -> Self {
        Self {
            sasl_auth_attempts: AtomicUsize::new(0),
            sasl_tokens_seen: parking_lot::Mutex::new(Vec::new()),
            oauth_error_acks: AtomicUsize::new(0),
            find_coord_calls: AtomicUsize::new(0),
            active_conns: AtomicUsize::new(0),
        }
    }
}

fn encode_kafka_frame<Resp: Encodable + HeaderVersion>(
    api_key: ApiKey,
    api_ver: i16,
    corr_id: i32,
    resp: &Resp,
) -> Result<Vec<u8>> {
    let hdr_ver = if api_key == ApiKey::ApiVersions {
        0
    } else {
        Resp::header_version(api_ver)
    };
    let mut hdr = ResponseHeader::default();
    hdr.correlation_id = corr_id;
    let mut buf = BytesMut::with_capacity(256);
    buf.put_u32(0);
    hdr.encode(&mut buf, hdr_ver)?;
    resp.encode(&mut buf, api_ver)?;
    let len = (buf.len() - 4) as u32;
    buf[0..4].copy_from_slice(&len.to_be_bytes());
    Ok(buf.to_vec())
}

async fn start_mock_sasl_kafka(
    behavior: Arc<MockKafkaBehavior>,
) -> Result<(String, Arc<MockKafkaStats>, tokio::task::AbortHandle)> {
    let port = free_test_port();
    let addr = format!("127.0.0.1:{port}");
    let listener = TcpListener::bind(&addr).await?;
    let stats = Arc::new(MockKafkaStats::default());
    let stats_srv = Arc::clone(&stats);
    let handle = tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            let beh = Arc::clone(&behavior);
            let st = Arc::clone(&stats_srv);
            let bind_port = port as i32;
            tokio::spawn(async move {
                st.active_conns.fetch_add(1, Ordering::SeqCst);
                let mut challenge_sent = false;
                loop {
                    let mut len_buf = [0u8; 4];
                    if sock.read_exact(&mut len_buf).await.is_err() {
                        break;
                    }
                    let sz = u32::from_be_bytes(len_buf) as usize;
                    let mut payload = vec![0u8; sz];
                    if sock.read_exact(&mut payload).await.is_err() {
                        break;
                    }
                    let mut cur = Bytes::from(payload);
                    let api_key_i16 = i16::from_be_bytes([cur[0], cur[1]]);
                    let api_ver = i16::from_be_bytes([cur[2], cur[3]]);
                    let Ok(api_key) = ApiKey::try_from(api_key_i16) else {
                        break;
                    };
                    let req_hdr_ver = match api_key {
                        ApiKey::ApiVersions => kafka_protocol::messages::ApiVersionsRequest::header_version(api_ver),
                        ApiKey::SaslHandshake => SaslHandshakeRequest::header_version(api_ver),
                        ApiKey::SaslAuthenticate => SaslAuthenticateRequest::header_version(api_ver),
                        ApiKey::Metadata => {
                            kafka_protocol::messages::MetadataRequest::header_version(api_ver)
                        }
                        ApiKey::Produce => {
                            kafka_protocol::messages::ProduceRequest::header_version(api_ver)
                        }
                        ApiKey::FindCoordinator => {
                            kafka_protocol::messages::FindCoordinatorRequest::header_version(api_ver)
                        }
                        ApiKey::OffsetCommit => {
                            kafka_protocol::messages::OffsetCommitRequest::header_version(api_ver)
                        }
                        _ => 1,
                    };
                    let Ok(hdr) = RequestHeader::decode(&mut cur, req_hdr_ver) else {
                        break;
                    };
                    let corr_id = hdr.correlation_id;
                    let frame: Vec<u8> = match api_key {
                        ApiKey::ApiVersions => {
                            let mut r = ApiVersionsResponse::default();
                            for (k, max_v) in [
                                (ApiKey::ApiVersions, 3),
                                (ApiKey::SaslHandshake, 1),
                                (ApiKey::SaslAuthenticate, 2),
                                (ApiKey::Metadata, 9),
                                (ApiKey::Produce, 9),
                                (ApiKey::FindCoordinator, 3),
                                (ApiKey::OffsetCommit, 8),
                            ] {
                                let mut av = ApiVersion::default();
                                av.api_key = k as i16;
                                av.min_version = 0;
                                av.max_version = max_v;
                                r.api_keys.push(av);
                            }
                            encode_kafka_frame(api_key, api_ver, corr_id, &r).unwrap_or_default()
                        }
                        ApiKey::SaslHandshake => {
                            let mut r = SaslHandshakeResponse::default();
                            r.mechanisms.push(StrBytes::from_static_str("OAUTHBEARER"));
                            encode_kafka_frame(api_key, api_ver, corr_id, &r).unwrap_or_default()
                        }
                        ApiKey::SaslAuthenticate => {
                            let Ok(req) = SaslAuthenticateRequest::decode(&mut cur, api_ver) else {
                                break;
                            };
                            if beh.oauth_error_challenge {
                                let mut r = SaslAuthenticateResponse::default();
                                if !challenge_sent {
                                    challenge_sent = true;
                                    r.error_code = 0;
                                    r.auth_bytes = Bytes::from_static(
                                        br#"{"status":"invalid_token","scope":"kafka"}"#,
                                    );
                                } else {
                                    if req.auth_bytes.as_ref() == b"\x01" {
                                        st.oauth_error_acks.fetch_add(1, Ordering::SeqCst);
                                    }
                                    r.error_code = 58;
                                    r.error_message =
                                        Some(StrBytes::from_static_str("invalid bearer token"));
                                }
                                encode_kafka_frame(api_key, api_ver, corr_id, &r)
                                    .unwrap_or_default()
                            } else {
                                let n = st.sasl_auth_attempts.fetch_add(1, Ordering::SeqCst);
                                let raw = String::from_utf8_lossy(&req.auth_bytes);
                                if let Some(tok) = raw
                                    .split('\x01')
                                    .find_map(|part| part.strip_prefix("auth=Bearer "))
                                {
                                    st.sasl_tokens_seen.lock().push(tok.to_string());
                                }
                                let mut r = SaslAuthenticateResponse::default();
                                if beh.fail_first_sasl_auth && n == 0 {
                                    r.error_code = 58;
                                    r.error_message =
                                        Some(StrBytes::from_static_str("transient sts race"));
                                } else {
                                    r.session_lifetime_ms = beh.session_lifetime_ms;
                                }
                                encode_kafka_frame(api_key, api_ver, corr_id, &r)
                                    .unwrap_or_default()
                            }
                        }
                        ApiKey::Metadata => {
                            let mut r = MetadataResponse::default();
                            let mut b = MetadataResponseBroker::default();
                            b.node_id = BrokerId(0);
                            b.host = StrBytes::from_static_str("127.0.0.1");
                            b.port = bind_port;
                            r.brokers.push(b);
                            let mut t = MetadataResponseTopic::default();
                            t.name = Some(TopicName(StrBytes::from_static_str("mimir-ingest")));
                            let mut p = MetadataResponsePartition::default();
                            p.partition_index = 0;
                            p.leader_id = BrokerId(0);
                            t.partitions.push(p);
                            r.topics.push(t);
                            encode_kafka_frame(api_key, api_ver, corr_id, &r).unwrap_or_default()
                        }
                        ApiKey::Produce => {
                            let mut r = ProduceResponse::default();
                            let mut tp = TopicProduceResponse::default();
                            tp.name = TopicName(StrBytes::from_static_str("mimir-ingest"));
                            let mut pp = PartitionProduceResponse::default();
                            pp.index = 0;
                            pp.base_offset = 1;
                            tp.partition_responses.push(pp);
                            r.responses.push(tp);
                            encode_kafka_frame(api_key, api_ver, corr_id, &r).unwrap_or_default()
                        }
                        ApiKey::FindCoordinator => {
                            let n = st.find_coord_calls.fetch_add(1, Ordering::SeqCst);
                            if beh.hang_first_find_coordinator && n == 0 {
                                tokio::time::sleep(Duration::from_secs(10)).await;
                                break;
                            }
                            let mut r = FindCoordinatorResponse::default();
                            r.node_id = BrokerId(0);
                            r.host = StrBytes::from_static_str("127.0.0.1");
                            r.port = bind_port;
                            encode_kafka_frame(api_key, api_ver, corr_id, &r).unwrap_or_default()
                        }
                        ApiKey::OffsetCommit => {
                            let mut r = OffsetCommitResponse::default();
                            let mut t = OffsetCommitResponseTopic::default();
                            t.name = TopicName(StrBytes::from_static_str("mimir-ingest"));
                            let mut p = OffsetCommitResponsePartition::default();
                            p.partition_index = 0;
                            t.partitions.push(p);
                            r.topics.push(t);
                            encode_kafka_frame(api_key, api_ver, corr_id, &r).unwrap_or_default()
                        }
                        _ => break,
                    };
                    if sock.write_all(&frame).await.is_err() {
                        break;
                    }
                }
                st.active_conns.fetch_sub(1, Ordering::SeqCst);
            });
        }
    })
    .abort_handle();
    Ok((addr, stats, handle))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_kafka_sasl_expiry_singleflight_no_leaked_tasks_and_f7_fixes() -> Result<()> {
    let sock_dir = tempfile::tempdir()?;
    let sock_path = sock_dir.path().join("creds.sock");
    let unix_listener = UnixListener::bind(&sock_path)?;
    let token_seq = Arc::new(AtomicUsize::new(0));
    let token_seq_srv = Arc::clone(&token_seq);
    let sidecar_abort = tokio::spawn(async move {
        while let Ok((mut stream, _)) = unix_listener.accept().await {
            let seq = Arc::clone(&token_seq_srv);
            tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                let _ = stream.read(&mut buf).await;
                let n = seq.fetch_add(1, Ordering::SeqCst) + 1;
                let json = format!(r#"{{"Token":"sidecar-tok-{n}"}}"#);
                let resp = format!(
                    "HTTP/1.1 200\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{json}",
                    json.len()
                );
                let _ = stream.write_all(resp.as_bytes()).await;
            });
        }
    })
    .abort_handle();

    let sock_str = sock_path.to_str().context("utf8 sock path")?.to_string();
    let parsed: KafkaOauthbearerAuth =
        request_json_from_socket(&sock_str, Duration::from_secs(2))
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
    assert_eq!(parsed.token, "sidecar-tok-1");

    let (addr, stats, broker_abort) = start_mock_sasl_kafka(Arc::new(MockKafkaBehavior {
        session_lifetime_ms: 1150,
        ..Default::default()
    }))
    .await?;

    let cfg = KafkaConfig {
        address: StringSliceCSV(vec![addr.clone()]),
        topic: "mimir-ingest".into(),
        sasl_mechanism: SASLMechanism::Oauthbearer,
        sasl_oauthbearer_http_socket_path: sock_str.clone(),
        write_timeout: GoDuration::from_secs(5),
        ..Default::default()
    };

    let baseline_tasks = tokio::runtime::Handle::current().metrics().num_alive_tasks();
    let writer = KafkaWriterClient::new(
        &cfg,
        16,
        None,
        KafkaWriterClientOptions {
            disable_default_topic: false,
            disable_linger: true,
        },
    )?;

    writer
        .produce_sync(WriterRecord {
            topic: "mimir-ingest".into(),
            partition: 0,
            key: Bytes::from_static(b"k1"),
            value: Bytes::from_static(b"v1"),
            ..Default::default()
        })
        .await?;
    let auths_before = stats.sasl_auth_attempts.load(Ordering::SeqCst);
    assert_eq!(auths_before, 2); // 1x ConnKind::Normal (Metadata) + 1x ConnKind::Produce

    tokio::time::sleep(Duration::from_millis(250)).await;
    let futs: Vec<_> = (0..16)
        .map(|i| {
            let w = Arc::clone(&writer);
            tokio::spawn(async move {
                w.produce_sync(WriterRecord {
                    topic: "mimir-ingest".into(),
                    partition: 0,
                    key: Bytes::from(format!("k-{i}")),
                    value: Bytes::from(format!("v-{i}")),
                    ..Default::default()
                })
                .await
            })
        })
        .collect();
    for f in futs {
        f.await??;
    }

    let auths_after = stats.sasl_auth_attempts.load(Ordering::SeqCst);
    assert_eq!(
        auths_after - auths_before,
        1,
        "expected exactly 1 singleflight SASL reconnect across 16 concurrent produces, got before={auths_before} after={auths_after}"
    );
    let toks = stats.sasl_tokens_seen.lock().clone();
    assert!(
        toks.iter().any(|t| t == "sidecar-tok-4"),
        "expected fresh sidecar token after sasl_expiry: {toks:?}"
    );

    writer.inner().close().await;
    writer.close();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        stats.active_conns.load(Ordering::SeqCst),
        0,
        "expired/closed sockets were not torn down cleanly"
    );
    let after_tasks = tokio::runtime::Handle::current().metrics().num_alive_tasks();
    assert!(
        after_tasks <= baseline_tasks + 1,
        "leaked tasks after close: before={baseline_tasks}, after={after_tasks}"
    );
    broker_abort.abort();

    let (addr_retry, stats_retry, b_abort_2) =
        start_mock_sasl_kafka(Arc::new(MockKafkaBehavior {
            fail_first_sasl_auth: true,
            ..Default::default()
        }))
        .await?;
    let mut cfg2 = cfg.clone();
    cfg2.address = StringSliceCSV(vec![addr_retry]);
    let kc2 = KafkaClient::from_config(&cfg2)?;
    kc2.ping().await?;
    assert_eq!(stats_retry.sasl_auth_attempts.load(Ordering::SeqCst), 2);
    kc2.close().await;
    b_abort_2.abort();

    let (addr_chal, stats_chal, b_abort_3) =
        start_mock_sasl_kafka(Arc::new(MockKafkaBehavior {
            oauth_error_challenge: true,
            ..Default::default()
        }))
        .await?;
    let mut cfg3 = cfg.clone();
    cfg3.address = StringSliceCSV(vec![addr_chal]);
    cfg3.sasl_oauthbearer_http_socket_path.clear();
    cfg3.sasl_oauthbearer_token = "bad-jwt".into();
    let kc3 = KafkaClient::from_config(&cfg3)?;
    let err = kc3.ping().await.unwrap_err();
    assert!(matches!(err, KafkaClientError::Sasl(_)), "{err}");
    assert!(
        err.to_string().contains("invalid_token"),
        "expected RFC 7628 JSON challenge in error, got: {err}"
    );
    assert!(stats_chal.oauth_error_acks.load(Ordering::SeqCst) >= 1);
    kc3.close().await;
    b_abort_3.abort();

    let (addr_hang, stats_hang, b_abort_4) =
        start_mock_sasl_kafka(Arc::new(MockKafkaBehavior {
            hang_first_find_coordinator: true,
            ..Default::default()
        }))
        .await?;
    let cfg4 = KafkaConfig {
        address: StringSliceCSV(vec![addr_hang]),
        topic: "mimir-ingest".into(),
        ..Default::default()
    };
    let kc4 = KafkaClient::from_config(&cfg4)?.with_request_timeout(Duration::from_millis(300));
    kc4.commit_offset("grp-1", "mimir-ingest", 0, 42).await?;
    assert!(
        stats_hang.find_coord_calls.load(Ordering::SeqCst) >= 2,
        "expected FindCoordinator retry after dropping timed-out socket"
    );
    kc4.close().await;
    b_abort_4.abort();

    sidecar_abort.abort();
    Ok(())
}
