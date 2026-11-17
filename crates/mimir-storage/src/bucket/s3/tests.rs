//! Tests for the S3 bucket (thanos objstore S3 provider + minio-go port), including ports of
//! Mimir `pkg/storage/bucket/s3/config_test.go` and `sse_bucket_client_test.go`.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::*;
use crate::bucket::{BucketClient, TenantConfigProvider};

/// A request seen by [`FakeS3`].
#[derive(Clone, Debug)]
pub(crate) struct Seen {
    pub method: String,
    pub target: String,
    pub headers: HashMap<String, String>,
    #[allow(dead_code)]
    pub body_len: usize,
}

impl Seen {
    pub fn h(&self, k: &str) -> &str {
        self.headers.get(k).map(String::as_str).unwrap_or("")
    }
}

#[derive(Default)]
struct FakeState {
    objects: BTreeMap<String, Vec<u8>>,
    uploads: HashMap<String, BTreeMap<u64, Vec<u8>>>,
    seen: Vec<Seen>,
    next_upload: u64,
    fail_next: Vec<(u16, String)>,
}

/// A minimal path-style S3 server (enough for every operation the bucket client performs).
pub(crate) struct FakeS3 {
    pub endpoint: String,
    state: Arc<Mutex<FakeState>>,
}

fn decode_aws_chunked(body: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < body.len() {
        let line_end = body[i..].windows(2).position(|w| w == b"\r\n").unwrap() + i;
        let header = std::str::from_utf8(&body[i..line_end]).unwrap();
        let size = usize::from_str_radix(header.split(';').next().unwrap(), 16).unwrap();
        let start = line_end + 2;
        out.extend_from_slice(&body[start..start + size]);
        i = start + size + 2;
        if size == 0 {
            break;
        }
    }
    out
}

fn query_of(target: &str) -> HashMap<String, String> {
    target
        .split_once('?')
        .map(|(_, q)| {
            q.split('&')
                .filter(|s| !s.is_empty())
                .map(|kv| {
                    let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
                    (k.to_string(), super::query_unescape(v).unwrap())
                })
                .collect()
        })
        .unwrap_or_default()
}

impl FakeS3 {
    pub async fn start(region: &str) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let state: Arc<Mutex<FakeState>> = Arc::default();
        let st = Arc::clone(&state);
        let region = region.to_string();
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else { return };
                let st = Arc::clone(&st);
                let region = region.clone();
                tokio::spawn(async move {
                    let mut buf: Vec<u8> = Vec::new();
                    loop {
                        // Read one request.
                        let head_end = loop {
                            if let Some(p) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                                break p;
                            }
                            let mut chunk = [0u8; 65536];
                            match sock.read(&mut chunk).await {
                                Ok(0) | Err(_) => return,
                                Ok(n) => buf.extend_from_slice(&chunk[..n]),
                            }
                        };
                        let head = String::from_utf8_lossy(&buf[..head_end]).into_owned();
                        let mut lines = head.lines();
                        let first = lines.next().unwrap_or("").to_string();
                        let headers: HashMap<String, String> = lines
                            .filter_map(|l| l.split_once(':'))
                            .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string()))
                            .collect();
                        buf.drain(..head_end + 4);
                        let mut body = Vec::new();
                        if headers.get("transfer-encoding").map(String::as_str) == Some("chunked") {
                            loop {
                                let line_end = loop {
                                    if let Some(p) = buf.windows(2).position(|w| w == b"\r\n") {
                                        break p;
                                    }
                                    let mut chunk = [0u8; 65536];
                                    match sock.read(&mut chunk).await {
                                        Ok(0) | Err(_) => return,
                                        Ok(n) => buf.extend_from_slice(&chunk[..n]),
                                    }
                                };
                                let size = usize::from_str_radix(
                                    std::str::from_utf8(&buf[..line_end]).unwrap().trim(),
                                    16,
                                )
                                .unwrap();
                                while buf.len() < line_end + 2 + size + 2 {
                                    let mut chunk = [0u8; 65536];
                                    match sock.read(&mut chunk).await {
                                        Ok(0) | Err(_) => return,
                                        Ok(n) => buf.extend_from_slice(&chunk[..n]),
                                    }
                                }
                                body.extend_from_slice(&buf[line_end + 2..line_end + 2 + size]);
                                buf.drain(..line_end + 2 + size + 2);
                                if size == 0 {
                                    break;
                                }
                            }
                        } else {
                            let len: usize = headers
                                .get("content-length")
                                .and_then(|v| v.parse().ok())
                                .unwrap_or(0);
                            while buf.len() < len {
                                let mut chunk = [0u8; 65536];
                                match sock.read(&mut chunk).await {
                                    Ok(0) | Err(_) => return,
                                    Ok(n) => buf.extend_from_slice(&chunk[..n]),
                                }
                            }
                            body = buf.drain(..len).collect();
                        }
                        let mut parts = first.split_whitespace();
                        let method = parts.next().unwrap_or("").to_string();
                        let target = parts.next().unwrap_or("").to_string();
                        let resp = Self::handle(&st, &region, &method, &target, &headers, body);
                        if sock.write_all(&resp).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        Self { endpoint, state }
    }

    fn handle(
        st: &Mutex<FakeState>,
        region: &str,
        method: &str,
        target: &str,
        headers: &HashMap<String, String>,
        body: Vec<u8>,
    ) -> Vec<u8> {
        let mut st = st.lock().unwrap();
        st.seen.push(Seen {
            method: method.to_string(),
            target: target.to_string(),
            headers: headers.clone(),
            body_len: body.len(),
        });
        let resp = |status: u16, extra: &str, body: &[u8]| {
            let mut r = format!(
                "HTTP/1.1 {status} X\r\nContent-Length: {}\r\n{extra}\r\n",
                body.len()
            )
            .into_bytes();
            r.extend_from_slice(body);
            r
        };
        if !st.fail_next.is_empty() {
            let (status, body) = st.fail_next.remove(0);
            return resp(status, "", body.as_bytes());
        }
        let body = if headers.get("x-amz-content-sha256").map(String::as_str)
            == Some("STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
        {
            decode_aws_chunked(&body)
        } else {
            body
        };
        let path = target.split('?').next().unwrap();
        let q = query_of(target);
        let mut segs = path.trim_start_matches('/').splitn(2, '/');
        let _bucket = segs.next().unwrap_or("");
        let key = segs.next().unwrap_or("").to_string();
        let lm = "Last-Modified: Mon, 02 Jan 2006 15:04:05 GMT\r\n";
        match method {
            "GET" if q.contains_key("location") => resp(
                200,
                "",
                format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<LocationConstraint xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">{region}</LocationConstraint>").as_bytes(),
            ),
            "GET" if key.is_empty() => {
                let prefix = q.get("prefix").cloned().unwrap_or_default();
                let delimiter = q.get("delimiter").cloned().unwrap_or_default();
                let mut contents = String::new();
                let mut prefixes = std::collections::BTreeSet::new();
                for k in st.objects.keys().filter(|k| k.starts_with(&prefix)) {
                    let rest = &k[prefix.len()..];
                    if !delimiter.is_empty() {
                        if let Some(i) = rest.find(&delimiter) {
                            prefixes.insert(format!("{prefix}{}", &rest[..i + 1]));
                            continue;
                        }
                    }
                    contents.push_str(&format!(
                        "<Contents><Key>{}</Key><LastModified>2006-01-02T15:04:05.000Z</LastModified><Size>1</Size></Contents>",
                        signer::encode_path(k)
                    ));
                }
                let cps: String = prefixes
                    .iter()
                    .map(|p| format!("<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>", signer::encode_path(p)))
                    .collect();
                resp(
                    200,
                    "",
                    format!("<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>b</Name><Prefix>{prefix}</Prefix><EncodingType>url</EncodingType><IsTruncated>false</IsTruncated>{contents}{cps}</ListBucketResult>").as_bytes(),
                )
            }
            "PUT" if q.contains_key("partNumber") => {
                let n: u64 = q["partNumber"].parse().unwrap();
                st.uploads.get_mut(&q["uploadId"]).unwrap().insert(n, body);
                resp(200, &format!("ETag: \"etag-{n}\"\r\n"), b"")
            }
            "PUT" => {
                st.objects.insert(key, body);
                resp(200, "ETag: \"x\"\r\n", b"")
            }
            "POST" if q.contains_key("uploads") => {
                st.next_upload += 1;
                let id = format!("upload-{}", st.next_upload);
                st.uploads.insert(id.clone(), BTreeMap::new());
                resp(
                    200,
                    "",
                    format!("<InitiateMultipartUploadResult><Bucket>b</Bucket><Key>{key}</Key><UploadId>{id}</UploadId></InitiateMultipartUploadResult>").as_bytes(),
                )
            }
            "POST" if q.contains_key("uploadId") => {
                let parts = st.uploads.remove(&q["uploadId"]).unwrap();
                let data: Vec<u8> = parts.into_values().flatten().collect();
                st.objects.insert(key.clone(), data);
                resp(
                    200,
                    "",
                    format!("<CompleteMultipartUploadResult><Location>l</Location><Bucket>b</Bucket><Key>{key}</Key><ETag>\"m\"</ETag></CompleteMultipartUploadResult>").as_bytes(),
                )
            }
            "DELETE" if q.contains_key("uploadId") => {
                st.uploads.remove(&q["uploadId"]);
                resp(204, "", b"")
            }
            "DELETE" => {
                st.objects.remove(&key);
                resp(204, "", b"")
            }
            "HEAD" => match st.objects.get(&key) {
                Some(o) => {
                    let mut r = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n{lm}ETag: \"x\"\r\n\r\n",
                        o.len()
                    )
                    .into_bytes();
                    r.truncate(r.len());
                    r
                }
                None => b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n".to_vec(),
            },
            "GET" => match st.objects.get(&key) {
                Some(o) => match headers.get("range") {
                    Some(r) => {
                        let spec = r.trim_start_matches("bytes=");
                        let (a, b) = spec.split_once('-').unwrap();
                        let a: usize = a.parse().unwrap();
                        let b: usize = if b.is_empty() { o.len() - 1 } else { b.parse().unwrap() };
                        resp(206, lm, &o[a..=b.min(o.len() - 1)])
                    }
                    None => resp(200, lm, o),
                },
                None => resp(
                    404,
                    "",
                    b"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message><Key>k</Key></Error>",
                ),
            },
            _ => resp(400, "", b""),
        }
    }

    pub fn seen(&self) -> Vec<Seen> {
        self.state.lock().unwrap().seen.clone()
    }

    pub fn clear_seen(&self) {
        self.state.lock().unwrap().seen.clear();
    }

    pub fn object(&self, key: &str) -> Option<Vec<u8>> {
        self.state.lock().unwrap().objects.get(key).cloned()
    }

    pub fn fail_next(&self, status: u16, body: &str) {
        self.state
            .lock()
            .unwrap()
            .fail_next
            .push((status, body.to_string()));
    }
}

fn s3_cfg(endpoint: &str, region: &str) -> S3Config {
    S3Config {
        endpoint: endpoint.to_string(),
        region: region.to_string(),
        bucket_name: "test-bucket".to_string(),
        access_key_id: "test".to_string(),
        secret_access_key: "test".to_string(),
        insecure: true,
        ..Default::default()
    }
}

fn no_env() -> credentials::Env {
    Arc::new(|_: &str| String::new())
}

fn bucket(cfg: &S3Config) -> S3Bucket {
    S3Bucket::new_with_env(cfg, "ingester", no_env()).unwrap()
}

// ---- Mimir pkg/storage/bucket/s3/config_test.go -------------------------------------------

// TestSSEConfig_Validate.
#[test]
fn test_sse_config_validate() {
    let v = |t: &str, ctx: &str| {
        mimir_config::S3SSEConfig {
            sse_type: t.into(),
            kms_key_id: String::new(),
            kms_encryption_context: ctx.into(),
        }
        .validate()
        .map_err(|e| e.to_string())
    };
    assert!(v("", "").is_ok());
    assert!(v("unknown", "").unwrap_err().contains("unsupported S3 SSE type"));
    assert!(v(SSE_KMS, "!{}!").unwrap_err().contains("invalid S3 SSE encryption context"));
    assert!(v(SSE_KMS, r#"{"department": "10103.0"}"#).is_ok());
    // Go json.Unmarshal into map[string]string rejects non-string values.
    assert!(v(SSE_KMS, r#"{"department": 1}"#).unwrap_err().contains("invalid S3 SSE encryption context"));
}

// TestConfig_Validate.
#[test]
fn test_config_validate() {
    let base = || S3Config {
        endpoint: "s3.eu-central-1.amazonaws.com".into(),
        bucket_name: "mimir-block".into(),
        signature_version: "v4".into(),
        storage_class: "STANDARD".into(),
        ..Default::default()
    };
    assert!(base().validate().is_ok());
    let c = S3Config {
        signature_version: "v2".into(),
        storage_class: "foo".into(),
        ..Default::default()
    };
    assert!(c.validate().unwrap_err().to_string().contains("unsupported S3 storage class"));
    let c = S3Config {
        endpoint: "mimir-blocks.s3.eu-central-1.amazonaws.com".into(),
        bucket_name: "mimir-blocks".into(),
        ..base()
    };
    assert!(c.validate().unwrap_err().to_string().contains("the endpoint must not prefixed with the bucket name"));
    let c = S3Config {
        native_aws_auth_enabled: true,
        ..Default::default()
    };
    assert!(c.validate().is_ok());
    let c = S3Config {
        endpoint: String::new(),
        sts_endpoint: "https://sts.eu-central-1.amazonaws.com".into(),
        ..base()
    };
    assert!(c.validate().is_ok());
    let c = S3Config {
        endpoint: String::new(),
        sts_endpoint: "sts.eu-central-1.amazonaws.com".into(),
        ..base()
    };
    assert!(c.validate().unwrap_err().to_string().contains("sts-endpoint must be a valid url"));
}

// TestSSEConfig_BuildMinioConfig.
#[test]
fn test_sse_config_build_minio_config() {
    let headers = |sse: ServerSideEncryption| {
        let mut h = HeaderMap::new();
        sse.marshal(&mut h);
        let g = |k: &str| h.get(k).map(|v| v.to_str().unwrap().to_string()).unwrap_or_default();
        (
            g("x-amz-server-side-encryption"),
            g("x-amz-server-side-encryption-aws-kms-key-id"),
            g("x-amz-server-side-encryption-context"),
        )
    };
    let sse = build_minio_sse(SSE_KMS, "test-key", "").unwrap().unwrap();
    assert_eq!(headers(sse), ("aws:kms".into(), "test-key".into(), String::new()));
    let sse = build_minio_sse(SSE_KMS, "test-key", "{\"department\":\"10103.0\"}")
        .unwrap()
        .unwrap();
    assert_eq!(
        headers(sse),
        (
            "aws:kms".into(),
            "test-key".into(),
            base64::engine::general_purpose::STANDARD.encode("{\"department\":\"10103.0\"}")
        )
    );
    assert_eq!(build_minio_sse(SSE_S3, "", "").unwrap(), Some(ServerSideEncryption::S3));
    assert_eq!(build_minio_sse("", "x", "").unwrap(), None);
    assert_eq!(build_minio_sse("bad", "", "").unwrap_err(), "unsupported S3 SSE type");
}

// TestParseKMSEncryptionContext.
#[test]
fn test_parse_kms_encryption_context() {
    assert_eq!(parse_kms_encryption_context("").unwrap(), None);
    let mut expected = BTreeMap::new();
    expected.insert("department".to_string(), "10103.0".to_string());
    assert_eq!(
        parse_kms_encryption_context(r#"{"department": "10103.0"}"#).unwrap(),
        Some(expected)
    );
    assert_eq!(parse_kms_encryption_context("null").unwrap(), None);
}

// thanos: the bucket-level SSE-KMS without context sends base64("{}") (nil map -> empty map),
// with Go json.Marshal key ordering and HTML escaping.
#[test]
fn test_build_thanos_sse() {
    let sse = build_thanos_sse(&mimir_config::S3SSEConfig {
        sse_type: SSE_KMS.into(),
        kms_key_id: "k".into(),
        kms_encryption_context: String::new(),
    })
    .unwrap()
    .unwrap();
    assert_eq!(
        sse,
        ServerSideEncryption::Kms {
            key_id: "k".into(),
            context: Some(b"{}".to_vec())
        }
    );
    let sse = build_thanos_sse(&mimir_config::S3SSEConfig {
        sse_type: SSE_KMS.into(),
        kms_key_id: "k".into(),
        kms_encryption_context: r#"{"z":"1","a":"<&>"}"#.into(),
    })
    .unwrap()
    .unwrap();
    assert_eq!(
        sse,
        ServerSideEncryption::Kms {
            key_id: "k".into(),
            context: Some(br#"{"a":"\u003c\u0026\u003e","z":"1"}"#.to_vec())
        }
    );
    assert_eq!(
        build_thanos_sse(&mimir_config::S3SSEConfig {
            sse_type: SSE_S3.into(),
            ..Default::default()
        })
        .unwrap(),
        Some(ServerSideEncryption::S3)
    );
}

// ---- thanos/minio construction errors ----------------------------------------------------

#[test]
fn construction_errors() {
    let err = |c: S3Config| S3Bucket::new_with_env(&c, "ingester", no_env()).unwrap_err();
    assert_eq!(err(s3_cfg("", "")), "no s3 endpoint in config file");
    let mut c = s3_cfg("s3.amazonaws.com", "");
    c.native_aws_auth_enabled = true;
    assert_eq!(err(c), "aws_sdk_auth and access_key are mutually exclusive configurations");
    let mut c = s3_cfg("s3.amazonaws.com", "");
    c.access_key_id.clear();
    assert!(err(c).starts_with("no s3 access_key specified while secret_key is present"));
    let mut c = s3_cfg("s3.amazonaws.com", "");
    c.secret_access_key.clear();
    assert!(err(c).starts_with("no s3 secret_key specified while access_key is present"));
    let mut c = s3_cfg("s3.amazonaws.com", "");
    c.sse.sse_type = SSE_KMS.into();
    assert_eq!(err(c), "kms_key_id must be set if sse_config.type is set to 'SSE-KMS'");
    assert_eq!(
        err(s3_cfg("s3.amazonaws.com/path", "")),
        "initialize s3 client: Endpoint url cannot have fully qualified paths."
    );
    let mut c = s3_cfg("s3.amazonaws.com", "");
    c.list_objects_version = "v9".into();
    assert_eq!(
        err(c),
        "Initialize s3 client list objects version: Unsupported version \"v9\" was provided. Supported values are v1, v2"
    );
    let mut c = s3_cfg("s3.amazonaws.com", "");
    c.http.tls_ca_path = "/does/not/exist".into();
    assert!(err(c).starts_with("unable to load specified CA cert /does/not/exist"));
}

// ---- addressing (minio-go makeTargetURL / region) ----------------------------------------

#[test]
fn addressing_follows_minio() {
    let b = |endpoint: &str, region: &str, lookup: mimir_config::S3BucketLookupType, dual: bool| {
        let mut c = s3_cfg(endpoint, region);
        c.insecure = false;
        c.bucket_name = "mimir-blocks".into();
        c.bucket_lookup_type = lookup;
        c.dualstack_enabled = dual;
        bucket(&c)
    };
    use mimir_config::S3BucketLookupType::*;
    // jsonnet default: dualstack endpoint, no region.
    let s = b("s3.dualstack.eu-west-1.amazonaws.com", "", Auto, true);
    assert_eq!(s.client.region, "eu-west-1");
    let loc = s.client.region.clone();
    assert_eq!(
        s.client.make_target_url("mimir-blocks", "t1/a", &loc, s.client.is_virtual_host_style_request("mimir-blocks")),
        ("mimir-blocks.s3.dualstack.eu-west-1.amazonaws.com".to_string(), "/t1/a".to_string())
    );
    let s = b("s3.eu-central-1.amazonaws.com", "", Auto, false);
    assert_eq!(
        s.client.make_target_url("mimir-blocks", "k", "eu-central-1", true).0,
        "mimir-blocks.s3.eu-central-1.amazonaws.com"
    );
    let s = b("s3.amazonaws.com", "", Auto, true);
    assert_eq!(s.client.region, "");
    assert_eq!(
        s.client.make_target_url("mimir-blocks", "k", "ap-southeast-2", true).0,
        "mimir-blocks.s3.dualstack.ap-southeast-2.amazonaws.com"
    );
    let s = b("minio.local:443", "us-west-2", VirtualHost, true);
    assert!(s.client.is_virtual_host_style_request("mimir-blocks"));
    assert_eq!(
        s.client.make_target_url("mimir-blocks", "k", "us-west-2", true),
        ("mimir-blocks.minio.local".to_string(), "/k".to_string())
    );
    let s = b("minio.local:9000", "", Auto, true);
    assert!(!s.client.is_virtual_host_style_request("mimir-blocks"));
    assert_eq!(
        s.client.make_target_url("mimir-blocks", "k", "us-east-1", false),
        ("minio.local:9000".to_string(), "/mimir-blocks/k".to_string())
    );
}

// ---- end-to-end against the fake S3 ------------------------------------------------------

#[tokio::test]
async fn region_discovered_with_get_bucket_location() {
    let fake = FakeS3::start("eu-west-1").await;
    let b = bucket(&s3_cfg(&fake.endpoint, ""));
    b.upload("t1/a", UploadSource::Bytes(Bytes::from_static(b"hello")), None)
        .await
        .unwrap();
    b.upload("t1/b", UploadSource::Bytes(Bytes::from_static(b"world")), None)
        .await
        .unwrap();
    let seen = fake.seen();
    assert_eq!(seen.len(), 3, "{seen:?}");
    assert_eq!(seen[0].method, "GET");
    assert_eq!(seen[0].target, "/test-bucket/?location=");
    assert!(seen[0].h("authorization").contains("/us-east-1/s3/aws4_request"), "{seen:?}");
    assert_eq!(seen[0].h("x-amz-content-sha256"), signer::EMPTY_SHA256_HEX);
    for s in &seen[1..] {
        assert_eq!(s.method, "PUT");
        assert!(s.h("authorization").contains("/eu-west-1/s3/aws4_request"), "{s:?}");
    }
}

/// Mimir sse_bucket_client_test.go TestSSEBucketClient_Upload_ShouldInjectCustomSSEConfig.
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

#[tokio::test]
async fn test_sse_bucket_client_upload_should_inject_custom_sse_config() {
    let fake = FakeS3::start("us-east-1").await;
    let s3 = bucket(&s3_cfg(&fake.endpoint, "test"));
    let root = BucketClient::from_backend(crate::bucket::Backend::S3(Arc::new(s3)), "test-bucket".into(), "S3");
    let provider = Arc::new(MockTenantConfigProvider::default());
    let user = root.for_user_with_config("user-1", provider.clone());

    let last = |fake: &FakeS3| fake.seen().last().unwrap().clone();
    user.upload("test", Bytes::from_static(b"test")).await.unwrap();
    let r = last(&fake);
    assert_eq!(r.target, "/test-bucket/user-1/test");
    assert_eq!(r.h("x-amz-server-side-encryption"), "");
    assert_eq!(r.h("x-amz-server-side-encryption-aws-kms-key-id"), "");
    assert_eq!(r.h("x-amz-server-side-encryption-context"), "");

    *provider.sse_type.lock().unwrap() = SSE_KMS.into();
    *provider.key_id.lock().unwrap() = "ABC".into();
    user.upload("test", Bytes::from_static(b"test")).await.unwrap();
    let r = last(&fake);
    assert_eq!(r.h("x-amz-server-side-encryption"), "aws:kms");
    assert_eq!(r.h("x-amz-server-side-encryption-aws-kms-key-id"), "ABC");
    assert_eq!(r.h("x-amz-server-side-encryption-context"), "");

    *provider.ctx.lock().unwrap() = "{\"department\":\"10103.0\"}".into();
    user.upload("test", Bytes::from_static(b"test")).await.unwrap();
    let r = last(&fake);
    assert_eq!(r.h("x-amz-server-side-encryption"), "aws:kms");
    assert_eq!(r.h("x-amz-server-side-encryption-aws-kms-key-id"), "ABC");
    assert_eq!(
        r.h("x-amz-server-side-encryption-context"),
        base64::engine::general_purpose::STANDARD.encode("{\"department\":\"10103.0\"}")
    );
    // The SSE headers are signed (AWS rejects unsigned x-amz-* headers).
    assert!(r
        .h("authorization")
        .contains("x-amz-server-side-encryption;x-amz-server-side-encryption-aws-kms-key-id;x-amz-server-side-encryption-context"));

    // Invalid tenant override fails the upload with the Go error text.
    *provider.sse_type.lock().unwrap() = "bogus".into();
    let err = user.upload("test", Bytes::from_static(b"test")).await.unwrap_err();
    assert_eq!(
        err.to_string(),
        "unable to customise S3 SSE config for tenant user-1: unsupported S3 SSE type"
    );
}

#[tokio::test]
async fn bucket_sse_storage_class_and_md5_headers() {
    let fake = FakeS3::start("us-east-1").await;
    let mut c = s3_cfg(&fake.endpoint, "us-east-1");
    c.sse.sse_type = SSE_KMS.into();
    c.sse.kms_key_id = "my-kms-key".into();
    c.sse.kms_encryption_context = r#"{"team":"obs"}"#.into();
    c.storage_class = "STANDARD_IA".into();
    c.send_content_md5 = true;
    let b = bucket(&c);
    b.upload("t1/meta.json", UploadSource::Bytes(Bytes::from_static(b"hello")), None)
        .await
        .unwrap();
    let r = fake.seen().last().unwrap().clone();
    assert_eq!(r.method, "PUT");
    assert_eq!(r.h("content-type"), "application/octet-stream");
    assert_eq!(r.h("x-amz-server-side-encryption"), "aws:kms");
    assert_eq!(r.h("x-amz-server-side-encryption-aws-kms-key-id"), "my-kms-key");
    assert_eq!(r.h("x-amz-server-side-encryption-context"), "eyJ0ZWFtIjoib2JzIn0=");
    assert_eq!(r.h("x-amz-storage-class"), "STANDARD_IA");
    assert_eq!(r.h("content-md5"), "XUFAKrxLKna5cZ2REBfFkg==");
    // Plain HTTP: streaming signature with 64KiB aws-chunked body.
    assert_eq!(r.h("x-amz-content-sha256"), "STREAMING-AWS4-HMAC-SHA256-PAYLOAD");
    assert_eq!(r.h("x-amz-decoded-content-length"), "5");
    assert!(r.h("user-agent").starts_with("MinIO ("), "{}", r.h("user-agent"));
    assert!(r.h("user-agent").contains(" minio-go/v7.0.98 thanos-ingester/"));
    assert_eq!(fake.object("t1/meta.json").unwrap(), b"hello");

    // SSE-S3.
    let mut c = s3_cfg(&fake.endpoint, "us-east-1");
    c.sse.sse_type = SSE_S3.into();
    let b = bucket(&c);
    b.upload("x", UploadSource::Bytes(Bytes::from_static(b"1")), None).await.unwrap();
    assert_eq!(fake.seen().last().unwrap().h("x-amz-server-side-encryption"), "AES256");
}

#[tokio::test]
async fn crud_iter_and_not_found() {
    let fake = FakeS3::start("us-east-1").await;
    let b = bucket(&s3_cfg(&fake.endpoint, "us-east-1"));
    for k in ["t1/01A/meta.json", "t1/01A/chunks/000001", "t1/01B/index", "t1/x"] {
        b.upload(k, UploadSource::Bytes(Bytes::from(k.as_bytes().to_vec())), None)
            .await
            .unwrap();
    }
    assert_eq!(b.get("t1/x").await.unwrap().as_ref(), b"t1/x");
    assert_eq!(b.get_range("t1/01B/index", 3, 3).await.unwrap().as_ref(), b"01B");
    assert!(b.exists("t1/x").await.unwrap());
    assert!(!b.exists("t1/missing").await.unwrap());
    let e = b.get("t1/missing").await.unwrap_err();
    assert!(e.is_obj_not_found());
    let a = b.attributes("t1/x").await.unwrap();
    assert_eq!(a.size, 4);
    assert_eq!(a.last_modified.timestamp(), 1136214245);
    assert!(b.attributes("nope").await.unwrap_err().is_obj_not_found());

    assert_eq!(b.iter("t1", false).await.unwrap(), vec!["t1/x", "t1/01A/", "t1/01B/"]);
    assert_eq!(
        b.iter("t1/01A/", true).await.unwrap(),
        vec!["t1/01A/chunks/000001", "t1/01A/meta.json"]
    );
    let seen = fake.seen();
    let list = seen.iter().find(|s| s.target.contains("list-type=2")).unwrap();
    assert_eq!(
        list.target,
        "/test-bucket/?delimiter=%2F&encoding-type=url&fetch-owner=true&list-type=2&prefix=t1%2F"
    );
    b.delete("t1/x").await.unwrap();
    assert!(!b.exists("t1/x").await.unwrap());

    // list_objects_version v1.
    let mut c = s3_cfg(&fake.endpoint, "us-east-1");
    c.list_objects_version = "v1".into();
    let b1 = bucket(&c);
    fake.clear_seen();
    assert_eq!(b1.iter("t1/", false).await.unwrap(), vec!["t1/01A/", "t1/01B/"]);
    assert_eq!(
        fake.seen()[0].target,
        "/test-bucket/?delimiter=%2F&encoding-type=url&prefix=t1%2F"
    );
}

#[tokio::test]
async fn multipart_parallel_and_md5_sequential() {
    let fake = FakeS3::start("us-east-1").await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("chunk");
    let size = 16 * 1024 * 1024 + 70 * 1024;
    let data: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
    std::fs::write(&path, &data).unwrap();

    for md5 in [false, true] {
        fake.clear_seen();
        let mut c = s3_cfg(&fake.endpoint, "us-east-1");
        c.send_content_md5 = md5;
        c.storage_class = "STANDARD_IA".into();
        let b = bucket(&c);
        b.upload(
            "t1/blk/chunks/000001",
            UploadSource::File {
                path: path.clone(),
                len: size as u64,
            },
            None,
        )
        .await
        .unwrap();
        assert_eq!(fake.object("t1/blk/chunks/000001").unwrap(), data, "md5={md5}");
        let seen = fake.seen();
        let methods: Vec<String> = seen
            .iter()
            .map(|s| format!("{} {}", s.method, s.target.split('?').nth(1).unwrap_or("")))
            .collect();
        // OptimalPartInfo(16MiB+70KiB, 0) -> 2 parts of 16MiB / 70KiB.
        assert_eq!(seen.len(), 4, "{methods:?}");
        assert_eq!(methods[0], "POST uploads=");
        assert_eq!(seen[0].h("x-amz-storage-class"), "STANDARD_IA");
        assert_eq!(seen[0].h("content-type"), "application/octet-stream");
        let parts: Vec<&Seen> = seen.iter().filter(|s| s.target.contains("partNumber")).collect();
        assert_eq!(parts.len(), 2);
        for p in &parts {
            assert_eq!(p.h("x-amz-storage-class"), "");
            assert_eq!(p.h("content-type"), "");
            assert_eq!(!p.h("content-md5").is_empty(), md5);
        }
        let complete = seen.last().unwrap();
        assert_eq!(complete.method, "POST");
        assert!(complete.target.starts_with("/test-bucket/t1/blk/chunks/000001?uploadId=upload-"));
        assert_eq!(complete.h("x-amz-content-sha256").len(), 64);
    }
}

#[tokio::test]
async fn retries_on_internal_error_and_503() {
    let fake = FakeS3::start("us-east-1").await;
    let b = bucket(&s3_cfg(&fake.endpoint, "us-east-1"));
    fake.fail_next(503, "");
    fake.fail_next(500, "<Error><Code>InternalError</Code><Message>boom</Message></Error>");
    b.upload("k", UploadSource::Bytes(Bytes::from_static(b"v")), None)
        .await
        .unwrap();
    assert_eq!(fake.seen().len(), 3);
    // Non-retryable error: returned immediately with the thanos wrapping.
    fake.clear_seen();
    fake.fail_next(403, "<Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>");
    let e = b
        .upload("k", UploadSource::Bytes(Bytes::from_static(b"v")), None)
        .await
        .unwrap_err();
    assert_eq!(e.to_string(), "upload s3 object: Access Denied");
    assert!(e.is_access_denied());
    assert_eq!(fake.seen().len(), 1);
}

#[test]
fn optimal_part_info_matches_minio() {
    // minio-go api-put-object-common_test.go style cases.
    assert_eq!(optimal_part_info(16 * 1024 * 1024 + 1, 0).unwrap(), (2, 16 * 1024 * 1024, 1));
    let gib = 1024 * 1024 * 1024u64;
    let (n, p, l) = optimal_part_info(5 * gib, 0).unwrap();
    assert_eq!((n, p), (320, 16 * 1024 * 1024));
    assert_eq!(l, 5 * gib - 319 * p);
    // Configured part size.
    assert_eq!(optimal_part_info(100 * 1024 * 1024, 64 * 1024 * 1024).unwrap(), (2, 64 * 1024 * 1024, 36 * 1024 * 1024));
    assert!(optimal_part_info(6 * 1024 * 1024, 1024 * 1024).unwrap_err().to_string().contains("smaller than allowed minimum of 5MiB"));
    assert!(optimal_part_info(100 * 1024 * 1024, 1024).unwrap_err().to_string().contains("Part size * max_parts(10000) is lesser than input objectSize."));
    // Large objects: 10000 parts limit rounds up to multiples of 16MiB.
    let (n, p, _) = optimal_part_info(200 * gib, 0).unwrap();
    assert_eq!(p, 32 * 1024 * 1024);
    assert!(n <= 10000);
}

#[test]
fn list_page_decoding() {
    let page = parse_list_page(br#"<ListBucketResult><EncodingType>url</EncodingType><IsTruncated>true</IsTruncated><NextContinuationToken>tok</NextContinuationToken><Contents><Key>a%2Bb+c</Key></Contents><CommonPrefixes><Prefix>d%2F</Prefix></CommonPrefixes></ListBucketResult>"#).unwrap();
    assert_eq!(page.keys, vec!["a+b c"]);
    assert_eq!(page.prefixes, vec!["d/"]);
    assert!(page.is_truncated);
    assert_eq!(page.next_continuation_token, "tok");
}
