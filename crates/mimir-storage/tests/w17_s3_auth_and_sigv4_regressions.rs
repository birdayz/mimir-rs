//! Wave 17 regression tests for S3 auth, SigV4 wire parity, and fallible header handling
//! (BUG-W17-S3-01 .. BUG-W17-S3-05).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use mimir_config::{S3BucketLookupType, S3Config, S3SSEConfig};
use mimir_storage::bucket::s3::{
    build_minio_sse, build_thanos_sse, S3Bucket, ServerSideEncryption, UploadSource, SSE_KMS,
};
use reqwest::header::HeaderMap;
use ring::{digest, hmac};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Clone, Debug)]
struct RecordedHttpReq {
    request_line: String,
    headers_raw: Vec<(String, String)>,
    #[allow(dead_code)]
    body: String,
}

async fn start_mock_http<F>(handler: F) -> (String, Arc<Mutex<Vec<RecordedHttpReq>>>)
where
    F: Fn(&RecordedHttpReq) -> (u16, String) + Send + Sync + 'static,
{
    let listener = tokio::net::TcpListener::bind("localhost:0")
        .await
        .expect("bind mock http");
    let addr = listener.local_addr().expect("local addr");
    let endpoint = if addr.is_ipv6() {
        format!("Localhost:{}", addr.port())
    } else {
        format!("127.0.0.1:{}", addr.port())
    };
    let log = Arc::new(Mutex::new(Vec::new()));
    let log_c = Arc::clone(&log);
    let handler = Arc::new(handler);

    tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                return;
            };
            let log_c = Arc::clone(&log_c);
            let handler = Arc::clone(&handler);
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let head_end = loop {
                    if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                        break pos;
                    }
                    let mut tmp = [0u8; 4096];
                    match sock.read(&mut tmp).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => buf.extend_from_slice(&tmp[..n]),
                    }
                };
                let head = String::from_utf8_lossy(&buf[..head_end]).into_owned();
                let mut lines = head.split("\r\n");
                let request_line = lines.next().unwrap_or_default().to_string();
                let headers_raw: Vec<(String, String)> = lines
                    .filter_map(|l| l.split_once(':'))
                    .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
                    .collect();
                let content_len: usize = headers_raw
                    .iter()
                    .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
                    .and_then(|(_, v)| v.parse().ok())
                    .unwrap_or(0);
                buf.drain(..head_end + 4);
                while buf.len() < content_len {
                    let mut tmp = [0u8; 4096];
                    match sock.read(&mut tmp).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => buf.extend_from_slice(&tmp[..n]),
                    }
                }
                let body = String::from_utf8_lossy(&buf[..content_len]).into_owned();
                buf.drain(..content_len);

                let rec = RecordedHttpReq {
                    request_line,
                    headers_raw,
                    body,
                };
                log_c.lock().unwrap().push(rec.clone());
                let (status, resp_body) = handler(&rec);
                let resp = format!(
                    "HTTP/1.1 {status} OK\r\nLast-Modified: Wed, 21 Oct 2015 07:28:00 GMT\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{resp_body}",
                    resp_body.len()
                );
                let _ = sock.write_all(resp.as_bytes()).await;
                let _ = sock.shutdown().await;
            });
        }
    });

    (endpoint, log)
}

async fn start_hanging_imds_token_server(request_log: Arc<Mutex<Vec<String>>>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                return;
            };
            let log = Arc::clone(&request_log);
            tokio::spawn(async move {
                let mut buf = [0u8; 2048];
                let n = sock.read(&mut buf).await.unwrap_or(0);
                let head = String::from_utf8_lossy(&buf[..n]);
                let first = head.lines().next().unwrap_or_default().to_string();
                log.lock().unwrap().push(first.clone());
                if first.starts_with("PUT /latest/api/token") {
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                } else {
                    let body = if first.contains("security-credentials/role1") {
                        r#"{"Code":"Success","AccessKeyId":"imds-v1-ak","SecretAccessKey":"imds-v1-sk","Token":"t","Expiration":"2099-01-01T00:00:00Z"}"#
                    } else {
                        "role1"
                    };
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = sock.write_all(resp.as_bytes()).await;
                }
            });
        }
    });
    format!("http://{addr}")
}

fn base_sdk_cfg(endpoint: &str) -> S3Config {
    S3Config {
        endpoint: endpoint.to_string(),
        bucket_name: "MyBucket".to_string(),
        region: "us-east-1".to_string(),
        insecure: true,
        native_aws_auth_enabled: true,
        bucket_lookup_type: S3BucketLookupType::Path,
        ..Default::default()
    }
}

fn env_map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect()
}

fn empty_aws_creds_json(ak: &str) -> String {
    format!(
        r#"{{"AccessKeyId":"{ak}","SecretAccessKey":"sk1","Token":"tok1","Expiration":"2099-01-01T00:00:00Z"}}"#
    )
}

#[tokio::test]
async fn bug_w17_s3_01_container_full_uri_host_and_scheme_allowlist() {
    let (s3_ep, _s3_log) = start_mock_http(|_| (404, String::new())).await;

    // Disallowed plaintext http:// non-loopback/non-ECS/non-EKS IP must fail before connecting.
    let bucket = S3Bucket::new_with_env_map(
        &base_sdk_cfg(&s3_ep),
        "ingester",
        env_map(&[
            ("HOME", "/nonexistent"),
            (
                "AWS_CONTAINER_CREDENTIALS_FULL_URI",
                "http://169.254.169.254/latest/creds",
            ),
            ("AWS_CONTAINER_AUTHORIZATION_TOKEN", "secret-bearer"),
        ]),
    )
    .expect("bucket");
    let err = bucket
        .exists("obj")
        .await
        .expect_err("must reject IMDS IP as container URI");
    assert!(
        err.to_string()
            .contains("only loopback/ecs/eks hosts are allowed"),
        "unexpected error: {err}"
    );

    // Arbitrary public IP over http:// must also be rejected.
    let bucket = S3Bucket::new_with_env_map(
        &base_sdk_cfg(&s3_ep),
        "ingester",
        env_map(&[
            ("HOME", "/nonexistent"),
            (
                "AWS_CONTAINER_CREDENTIALS_FULL_URI",
                "http://93.184.216.34/steal",
            ),
            ("AWS_CONTAINER_AUTHORIZATION_TOKEN", "secret-bearer"),
        ]),
    )
    .expect("bucket");
    let err = bucket
        .exists("obj")
        .await
        .expect_err("must reject external http host");
    assert!(
        err.to_string()
            .contains("only loopback/ecs/eks hosts are allowed"),
        "unexpected error: {err}"
    );

    // Non-http/https scheme must be rejected.
    let bucket = S3Bucket::new_with_env_map(
        &base_sdk_cfg(&s3_ep),
        "ingester",
        env_map(&[
            ("HOME", "/nonexistent"),
            ("AWS_CONTAINER_CREDENTIALS_FULL_URI", "file:///etc/passwd"),
        ]),
    )
    .expect("bucket");
    let err = bucket
        .exists("obj")
        .await
        .expect_err("must reject file:// scheme");
    assert!(err.to_string().contains("scheme"), "unexpected error: {err}");

    // Loopback http:// succeeds.
    let (creds_ep, creds_log) =
        start_mock_http(|_| (200, empty_aws_creds_json("ecs-loopback-ak"))).await;
    let bucket = S3Bucket::new_with_env_map(
        &base_sdk_cfg(&s3_ep),
        "ingester",
        env_map(&[
            ("HOME", "/nonexistent"),
            (
                "AWS_CONTAINER_CREDENTIALS_FULL_URI",
                &format!("http://{creds_ep}/creds"),
            ),
            ("AWS_CONTAINER_AUTHORIZATION_TOKEN", "tok-ok"),
        ]),
    )
    .expect("bucket");
    assert!(!bucket.exists("obj").await.expect("exists should succeed"));
    assert_eq!(creds_log.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn bug_w17_s3_02_imds_v1_fallback_disabled_and_ipv6_mode() {
    let (s3_ep, s3_log) = start_mock_http(|_| (200, String::new())).await;

    // 1. IMDSv1 fallback on 403, 404, 405 from PUT /latest/api/token.
    for code in [403u16, 404, 405] {
        let imds_reqs = Arc::new(Mutex::new(Vec::<String>::new()));
        let imds_reqs_c = Arc::clone(&imds_reqs);
        let (imds_ep, _) = start_mock_http(move |req| {
            imds_reqs_c.lock().unwrap().push(req.request_line.clone());
            if req.request_line.starts_with("PUT /latest/api/token") {
                (code, "denied".to_string())
            } else if req.request_line.contains("security-credentials/my-role") {
                (
                    200,
                    r#"{"Code":"Success","AccessKeyId":"imds-ak","SecretAccessKey":"imds-sk","Token":"t","Expiration":"2099-01-01T00:00:00Z"}"#
                        .to_string(),
                )
            } else {
                (200, "my-role".to_string())
            }
        })
        .await;
        let bucket = S3Bucket::new_with_env_map(
            &base_sdk_cfg(&s3_ep),
            "ingester",
            env_map(&[
                ("HOME", "/nonexistent"),
                (
                    "AWS_EC2_METADATA_SERVICE_ENDPOINT",
                    &format!("http://{imds_ep}"),
                ),
            ]),
        )
        .expect("bucket");
        assert!(bucket.exists("k").await.expect("fallback must succeed"));
        let log = imds_reqs.lock().unwrap();
        assert!(log[0].starts_with("PUT /latest/api/token"));
        assert!(log.iter().any(|l| l.contains("security-credentials/my-role")));
    }

    // 2. HTTP 400 from PUT /latest/api/token must NOT fall back to IMDSv1.
    let imds_400_reqs = Arc::new(Mutex::new(Vec::<String>::new()));
    let imds_400_c = Arc::clone(&imds_400_reqs);
    let (imds_400_ep, _) = start_mock_http(move |req| {
        imds_400_c.lock().unwrap().push(req.request_line.clone());
        (400, "bad request".to_string())
    })
    .await;
    let bucket = S3Bucket::new_with_env_map(
        &base_sdk_cfg(&s3_ep),
        "ingester",
        env_map(&[
            ("HOME", "/nonexistent"),
            (
                "AWS_EC2_METADATA_SERVICE_ENDPOINT",
                &format!("http://{imds_400_ep}"),
            ),
        ]),
    )
    .expect("bucket");
    let err = bucket.exists("k").await.expect_err("400 must be terminal");
    assert!(err.to_string().contains("400"), "{err}");
    assert_eq!(imds_400_reqs.lock().unwrap().len(), 1);

    // 3. AWS_EC2_METADATA_V1_DISABLED=true must abort on token timeout without sending IMDSv1 GET.
    let hanging_log = Arc::new(Mutex::new(Vec::new()));
    let hanging_ep = start_hanging_imds_token_server(Arc::clone(&hanging_log)).await;
    let bucket = S3Bucket::new_with_env_map(
        &base_sdk_cfg(&s3_ep),
        "ingester",
        env_map(&[
            ("HOME", "/nonexistent"),
            ("AWS_EC2_METADATA_SERVICE_ENDPOINT", &hanging_ep),
            ("AWS_EC2_METADATA_V1_DISABLED", "true"),
        ]),
    )
    .expect("bucket");
    let err = bucket
        .exists("k")
        .await
        .expect_err("v1 disabled must fail on token timeout");
    assert!(err.to_string().contains("timeout"), "{err}");
    assert!(
        hanging_log
            .lock()
            .unwrap()
            .iter()
            .all(|l| l.starts_with("PUT /latest/api/token")),
        "must never send IMDSv1 GET when v1 disabled: {:?}",
        *hanging_log.lock().unwrap()
    );

    // 4. AWS_EC2_METADATA_SERVICE_ENDPOINT_MODE rejects invalid values and accepts IPv6.
    let bucket = S3Bucket::new_with_env_map(
        &base_sdk_cfg(&s3_ep),
        "ingester",
        env_map(&[
            ("HOME", "/nonexistent"),
            ("AWS_EC2_METADATA_SERVICE_ENDPOINT_MODE", "bogus"),
        ]),
    )
    .expect("bucket");
    let err = bucket.exists("k").await.expect_err("invalid mode must error");
    assert!(
        err.to_string().contains("must be either IPv6 or IPv4"),
        "{err}"
    );
    let _ = s3_log;
}

#[tokio::test]
async fn bug_w17_s3_03_web_identity_precedes_aws_profile_and_checks_role_arn() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::create_dir_all(dir.path().join(".aws")).expect("mkdir");
    std::fs::write(
        dir.path().join(".aws/config"),
        "[profile prod]\nregion = us-east-1\n[profile p2]\nregion = us-east-1\n",
    )
    .expect("write config");
    std::fs::write(
        dir.path().join(".aws/credentials"),
        "[prod]\naws_access_key_id = profile-ak\naws_secret_access_key = profile-sk\n[p2]\naws_access_key_id = def-prof-ak\naws_secret_access_key = def-prof-sk\n",
    )
    .expect("write creds");
    let tok_file = dir.path().join("web-id-token");
    std::fs::write(&tok_file, "jwt-token-body").expect("write tok");

    let (s3_ep, s3_log) = start_mock_http(|_| (200, String::new())).await;

    // 1. AWS_PROFILE=prod must NOT preempt AWS_WEB_IDENTITY_TOKEN_FILE; empty AWS_ROLE_ARN must fail cleanly.
    let bucket = S3Bucket::new_with_env_map(
        &base_sdk_cfg(&s3_ep),
        "ingester",
        env_map(&[
            ("HOME", dir.path().to_str().unwrap()),
            ("AWS_PROFILE", "prod"),
            ("AWS_WEB_IDENTITY_TOKEN_FILE", tok_file.to_str().unwrap()),
        ]),
    )
    .expect("bucket");
    let err = bucket
        .exists("k")
        .await
        .expect_err("missing role arn must fail");
    assert!(err.to_string().contains("role ARN is not set"), "{err}");

    // 2. AWS_DEFAULT_PROFILE works when AWS_PROFILE is unset.
    let bucket = S3Bucket::new_with_env_map(
        &base_sdk_cfg(&s3_ep),
        "ingester",
        env_map(&[
            ("HOME", dir.path().to_str().unwrap()),
            ("AWS_DEFAULT_PROFILE", "p2"),
        ]),
    )
    .expect("bucket");
    assert!(bucket.exists("k").await.expect("exists"));
    let auth = s3_log
        .lock()
        .unwrap()
        .last()
        .unwrap()
        .headers_raw
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("authorization"))
        .map(|(_, v)| v.clone())
        .unwrap_or_default();
    assert!(
        auth.contains("Credential=def-prof-ak/"),
        "expected AWS_DEFAULT_PROFILE credentials, got: {auth}"
    );
}

#[tokio::test]
async fn bug_w17_s3_04_mixed_case_host_and_dot_segment_sigv4_wire_parity() {
    let (s3_ep_raw, s3_log) = start_mock_http(|_| (200, String::new())).await;
    let port = s3_ep_raw.rsplit(':').next().expect("port");
    let s3_ep = format!("Localhost:{port}");

    let cfg = S3Config {
        endpoint: s3_ep.clone(),
        bucket_name: "MyBucket".to_string(),
        region: "us-east-1".to_string(),
        access_key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
        secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
        insecure: true,
        bucket_lookup_type: S3BucketLookupType::Path,
        ..Default::default()
    };
    let bucket = S3Bucket::new_with_env_map(&cfg, "ingester", HashMap::new()).expect("bucket");

    // Object key with . and .. segments must have identical wire path and signed CanonicalURI.
    let ok = bucket
        .exists("tenant/./sub/../obj")
        .await
        .expect("exists with dot segments");
    assert!(ok);

    let req = s3_log.lock().unwrap().last().cloned().expect("recorded req");
    assert_eq!(
        req.request_line,
        "HEAD /MyBucket/tenant/obj HTTP/1.1"
    );

    let wire_host = req
        .headers_raw
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("host"))
        .map(|(_, v)| v.clone())
        .expect("host header");
    assert_eq!(wire_host, s3_ep, "mixed-case Host must be preserved on wire");

    // Recompute SigV4 signature directly from the exact wire request-target and Host header!
    let auth = req
        .headers_raw
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("authorization"))
        .map(|(_, v)| v.clone())
        .expect("auth header");
    let amz_date = req
        .headers_raw
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("x-amz-date"))
        .map(|(_, v)| v.clone())
        .expect("x-amz-date");
    let payload_hash = req
        .headers_raw
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("x-amz-content-sha256"))
        .map(|(_, v)| v.clone())
        .expect("x-amz-content-sha256");

    let wire_path = req
        .request_line
        .split_whitespace()
        .nth(1)
        .expect("path");
    let canonical = format!(
        "HEAD\n{wire_path}\n\nhost:{wire_host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n\nhost;x-amz-content-sha256;x-amz-date\n{payload_hash}"
    );
    let yyyymmdd = &amz_date[..8];
    let scope = format!("{yyyymmdd}/us-east-1/s3/aws4_request");
    let canonical_hash = hex::encode(digest::digest(&digest::SHA256, canonical.as_bytes()));
    let string_to_sign = format!("AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{canonical_hash}");

    let k_date = hmac::sign(
        &hmac::Key::new(
            hmac::HMAC_SHA256,
            b"AWS4wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        ),
        yyyymmdd.as_bytes(),
    );
    let k_region = hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, k_date.as_ref()), b"us-east-1");
    let k_service = hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, k_region.as_ref()), b"s3");
    let k_signing = hmac::sign(
        &hmac::Key::new(hmac::HMAC_SHA256, k_service.as_ref()),
        b"aws4_request",
    );
    let expected_sig = hex::encode(hmac::sign(
        &hmac::Key::new(hmac::HMAC_SHA256, k_signing.as_ref()),
        string_to_sign.as_bytes(),
    ));
    let wire_sig = auth
        .split("Signature=")
        .nth(1)
        .expect("Signature=")
        .to_string();
    assert_eq!(wire_sig, expected_sig, "wire SigV4 signature must match");
}

#[tokio::test]
async fn bug_w17_s3_05_fallible_header_construction_never_panics() {
    let err = build_minio_sse(SSE_KMS, "key-with-newline\n", "")
        .expect_err("invalid header bytes in kms_key_id must return Err");
    assert!(err.contains("invalid kms_key_id"), "{err}");

    let err = build_thanos_sse(&S3SSEConfig {
        sse_type: SSE_KMS.to_string(),
        kms_key_id: "bad\rkey".to_string(),
        ..Default::default()
    })
    .expect_err("invalid header bytes in build_thanos_sse must return Err");
    assert!(err.contains("invalid kms_key_id"), "{err}");

    let mut headers = HeaderMap::new();
    let sse = ServerSideEncryption::Kms {
        key_id: "bad\nvalue".to_string(),
        context: None,
    };
    let err = sse
        .marshal(&mut headers)
        .expect_err("marshal with invalid header bytes must return Err");
    assert!(err.contains("invalid SSE header"), "{err}");

    // Invalid AWS_SESSION_TOKEN must return Err rather than panicking in SignableRequest::set_header.
    let (s3_ep, _) = start_mock_http(|_| (200, String::new())).await;
    let bucket = S3Bucket::new_with_env_map(
        &base_sdk_cfg(&s3_ep),
        "ingester",
        env_map(&[
            ("HOME", "/nonexistent"),
            ("AWS_ACCESS_KEY_ID", "ak"),
            ("AWS_SECRET_ACCESS_KEY", "sk"),
            ("AWS_SESSION_TOKEN", "bad\ntoken"),
        ]),
    )
    .expect("bucket");
    let err = bucket
        .upload("obj", UploadSource::Bytes(Bytes::from_static(b"hi")), None)
        .await
        .expect_err("invalid session token must return Err");
    assert!(err.to_string().contains("invalid header value"), "{err}");
}
