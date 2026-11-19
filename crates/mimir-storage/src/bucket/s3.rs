//! S3 backend for the blocks-storage bucket client: a port of the thanos objstore S3 provider and
//! the parts of minio-go it relies on (no `object_store` involved, so every header and request
//! matches Go Mimir).
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/storage/bucket/s3/bucket_client.go (`newS3Config`)
//! - github.com/grafana/mimir/pkg/storage/bucket/s3/config.go (`SSEConfig.BuildThanosConfig`,
//!   `SSEConfig.BuildMinioConfig`, `parseKMSEncryptionContext`)
//! - github.com/thanos-io/objstore/providers/s3/s3.go (`NewBucketWithConfig`, `validate`,
//!   `Upload`, `getRange`, `Exists`, `Attributes`, `Delete`, `IterWithAttributes`,
//!   `IsObjNotFoundErr`, `getServerSideEncryption`)
//! - github.com/minio/minio-go/v7/api-put-object.go, api-put-object-streaming.go,
//!   api-put-object-multipart.go, api-put-object-common.go (`PutObject`, `putObject`,
//!   `putObjectDo`, `putObjectMultipartStreamFromReadAt`,
//!   `putObjectMultipartStreamOptionalChecksum`, `OptimalPartInfo`, `initiateMultipartUpload`,
//!   `uploadPart`, `completeMultipartUpload`, `abortMultipartUpload`)
//! - github.com/minio/minio-go/v7/api-get-object.go, api-get-options.go, api-stat.go,
//!   api-remove.go, api-list.go, utils.go (`ToObjectInfo`, `trimEtag`)
//! - github.com/minio/minio-go/v7/pkg/encrypt/server-side.go (`NewSSE`, `NewSSEKMS`, `Marshal`)
//!
//! Region handling follows minio-go: an explicit `region` wins; otherwise the region is derived
//! from an AWS endpoint hostname; otherwise the bucket location is discovered once with
//! `GetBucketLocation` (signed for `us-east-1`) and cached. AWS requests go to the regional
//! (dual-stack unless `dualstack_enabled: false`) endpoint of the bucket location.

mod client;
mod credentials;
mod signer;
mod transport;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use base64::Engine;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use md5::{Digest, Md5};
use mimir_config::{S3Config, S3SSEConfig};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};

pub use client::set_app_version;
use client::{err_invalid_argument, Body, Client, ClientOptions, RequestMetadata, S3Error};

use super::s3utils;

/// Mimir `SSEKMS` / `SSES3`.
pub const SSE_KMS: &str = "SSE-KMS";
pub const SSE_S3: &str = "SSE-S3";

/// minio-go constants (`constants.go`).
const ABS_MIN_PART_SIZE: u64 = 1024 * 1024 * 5;
const MIN_PART_SIZE: u64 = 1024 * 1024 * 16;
const MAX_PARTS_COUNT: u64 = 10000;
const MAX_PART_SIZE: u64 = 1024 * 1024 * 1024 * 5;
const MAX_OBJECT_SIZE: u64 = MAX_PART_SIZE * MAX_PARTS_COUNT;
/// thanos pins `NumThreads: 4`.
const NUM_THREADS: usize = 4;

pub use mimir_config::S3_SUPPORTED_STORAGE_CLASSES as SUPPORTED_STORAGE_CLASSES;

/// minio-go `encrypt.ServerSide` (the variants Mimir can configure).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ServerSideEncryption {
    /// `encrypt.NewSSE()`.
    S3,
    /// `encrypt.NewSSEKMS(key, context)`; `context` is the serialized JSON (None = no context).
    Kms { key_id: String, context: Option<Vec<u8>> },
}

impl ServerSideEncryption {
    /// `ServerSide.Marshal`.
    pub fn marshal(&self, h: &mut HeaderMap) -> Result<(), String> {
        let set = |h: &mut HeaderMap, k: &'static str, v: &str| -> Result<(), String> {
            let val = HeaderValue::from_str(v)
                .map_err(|e| format!("invalid SSE header {k}: {e}"))?;
            h.insert(HeaderName::from_static(k), val);
            Ok(())
        };
        match self {
            ServerSideEncryption::S3 => set(h, "x-amz-server-side-encryption", "AES256")?,
            ServerSideEncryption::Kms { key_id, context } => {
                set(h, "x-amz-server-side-encryption", "aws:kms")?;
                if !key_id.is_empty() {
                    set(h, "x-amz-server-side-encryption-aws-kms-key-id", key_id)?;
                }
                if let Some(ctx) = context {
                    set(
                        h,
                        "x-amz-server-side-encryption-context",
                        &base64::engine::general_purpose::STANDARD.encode(ctx),
                    )?;
                }
            }
        }
        Ok(())
    }
}

/// Go `encoding/json` string encoding (HTML-safe escaping, as `json.Marshal` does).
fn go_json_string(s: &str, out: &mut String) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '<' => out.push_str("\\u003c"),
            '>' => out.push_str("\\u003e"),
            '&' => out.push_str("\\u0026"),
            '\u{2028}' => out.push_str("\\u2028"),
            '\u{2029}' => out.push_str("\\u2029"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
}

/// Go `json.Marshal(map[string]string)` (keys sorted).
fn go_json_marshal_map(m: &BTreeMap<String, String>) -> Vec<u8> {
    let mut out = String::from("{");
    for (i, (k, v)) in m.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        go_json_string(k, &mut out);
        out.push(':');
        go_json_string(v, &mut out);
    }
    out.push('}');
    out.into_bytes()
}

/// Mimir `parseKMSEncryptionContext` (`json.Unmarshal` into `map[string]string`; `null` and
/// `""` yield a nil map).
pub fn parse_kms_encryption_context(data: &str) -> Result<Option<BTreeMap<String, String>>, String> {
    if data.is_empty() {
        return Ok(None);
    }
    serde_json::from_str::<Option<BTreeMap<String, String>>>(data)
        .map_err(|e| format!("unable to parse KMS encryption context: {e}"))
}

/// Mimir `SSEConfig.BuildThanosConfig` + thanos `NewBucketWithConfig` SSE construction (a nil
/// KMS context becomes an empty map, i.e. the context header carries `{}`).
pub fn build_thanos_sse(cfg: &S3SSEConfig) -> Result<Option<ServerSideEncryption>, String> {
    match cfg.sse_type.as_str() {
        "" => Ok(None),
        SSE_KMS => {
            if !cfg.kms_key_id.is_empty() {
                HeaderValue::from_str(&cfg.kms_key_id)
                    .map_err(|e| format!("invalid kms_key_id header value: {e}"))?;
            }
            let ctx = parse_kms_encryption_context(&cfg.kms_encryption_context)?.unwrap_or_default();
            Ok(Some(ServerSideEncryption::Kms {
                key_id: cfg.kms_key_id.clone(),
                context: Some(go_json_marshal_map(&ctx)),
            }))
        }
        SSE_S3 => Ok(Some(ServerSideEncryption::S3)),
        _ => Err("unsupported S3 SSE type".to_string()),
    }
}

/// Mimir `SSEConfig.BuildMinioConfig` (used for per-tenant overrides; a nil KMS context sends no
/// context header).
pub fn build_minio_sse(
    sse_type: &str,
    kms_key_id: &str,
    kms_encryption_context: &str,
) -> Result<Option<ServerSideEncryption>, String> {
    match sse_type {
        "" => Ok(None),
        SSE_KMS => {
            if !kms_key_id.is_empty() {
                HeaderValue::from_str(kms_key_id)
                    .map_err(|e| format!("invalid kms_key_id header value: {e}"))?;
            }
            let ctx = parse_kms_encryption_context(kms_encryption_context)?;
            Ok(Some(ServerSideEncryption::Kms {
                key_id: kms_key_id.to_string(),
                context: ctx.map(|m| go_json_marshal_map(&m)),
            }))
        }
        SSE_S3 => Ok(Some(ServerSideEncryption::S3)),
        _ => Err("unsupported S3 SSE type".to_string()),
    }
}

/// thanos `validate(conf)`.
fn thanos_validate(cfg: &S3Config) -> Result<(), String> {
    if cfg.endpoint.is_empty() {
        return Err("no s3 endpoint in config file".to_string());
    }
    if cfg.native_aws_auth_enabled && !cfg.access_key_id.is_empty() {
        return Err("aws_sdk_auth and access_key are mutually exclusive configurations".to_string());
    }
    if cfg.access_key_id.is_empty() && !cfg.secret_access_key.is_empty() {
        return Err("no s3 access_key specified while secret_key is present in config file; either both should be present in config or envvars/IAM should be used.".to_string());
    }
    if !cfg.access_key_id.is_empty() && cfg.secret_access_key.is_empty() {
        return Err("no s3 secret_key specified while access_key is present in config file; either both should be present in config or envvars/IAM should be used.".to_string());
    }
    if cfg.sse.sse_type == SSE_KMS && cfg.sse.kms_key_id.is_empty() {
        return Err("kms_key_id must be set if sse_config.type is set to 'SSE-KMS'".to_string());
    }
    Ok(())
}

/// Errors of S3 bucket operations.
#[derive(Debug)]
pub struct S3BucketError {
    /// Context prefix added by thanos (`upload s3 object`, `stat s3 object`, ...).
    context: Option<&'static str>,
    inner: S3Error,
}

impl S3BucketError {
    fn new(inner: S3Error) -> Self {
        Self { context: None, inner }
    }
    fn wrap(context: &'static str, inner: S3Error) -> Self {
        Self {
            context: Some(context),
            inner,
        }
    }
    /// thanos `IsObjNotFoundErr`.
    pub fn is_obj_not_found(&self) -> bool {
        self.inner.code() == "NoSuchKey"
    }
    /// thanos `IsAccessDeniedErr`.
    pub fn is_access_denied(&self) -> bool {
        self.inner.code() == "AccessDenied"
    }
    /// The S3 error code (empty for transport errors).
    pub fn code(&self) -> &str {
        self.inner.code()
    }
}

impl std::fmt::Display for S3BucketError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.context {
            Some(c) => write!(f, "{c}: {}", self.inner),
            None => self.inner.fmt(f),
        }
    }
}

impl std::error::Error for S3BucketError {}

/// Object attributes returned by [`S3Bucket::attributes`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct S3ObjectInfo {
    pub size: i64,
    pub last_modified: DateTime<Utc>,
    pub etag: String,
}

/// Source of an upload.
#[derive(Clone, Debug)]
pub enum UploadSource {
    Bytes(Bytes),
    File { path: PathBuf, len: u64 },
}

impl UploadSource {
    fn body(&self) -> Body {
        match self {
            UploadSource::Bytes(b) => Body::Bytes(b.clone()),
            UploadSource::File { path, len } => Body::File {
                path: path.clone(),
                offset: 0,
                len: *len,
            },
        }
    }
    fn section(&self, offset: u64, len: u64) -> Body {
        match self {
            UploadSource::Bytes(b) => Body::Bytes(b.slice(offset as usize..(offset + len) as usize)),
            UploadSource::File { path, .. } => Body::File {
                path: path.clone(),
                offset,
                len,
            },
        }
    }
}

/// thanos objstore S3 `Bucket`.
pub struct S3Bucket {
    client: Arc<Client>,
    name: String,
    default_sse: Option<ServerSideEncryption>,
    storage_class: String,
    part_size: u64,
    list_objects_v1: bool,
    send_content_md5: bool,
}

impl std::fmt::Debug for S3Bucket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Bucket")
            .field("name", &self.name)
            .field("endpoint", &self.client.endpoint)
            .finish_non_exhaustive()
    }
}

/// minio-go `OptimalPartInfo(objectSize, configuredPartSize)` for a known size.
fn optimal_part_info(object_size: u64, configured_part_size: u64) -> Result<(u64, u64, u64), S3Error> {
    if object_size > MAX_OBJECT_SIZE {
        return Err(err_entity_too_large(object_size, MAX_OBJECT_SIZE));
    }
    let part_size_flt: f64 = if configured_part_size > 0 {
        if configured_part_size > object_size {
            return Err(err_entity_too_large(configured_part_size, object_size));
        }
        if object_size > configured_part_size * MAX_PARTS_COUNT {
            return Err(err_invalid_argument(
                "Part size * max_parts(10000) is lesser than input objectSize.",
            ));
        }
        if configured_part_size < ABS_MIN_PART_SIZE {
            return Err(err_invalid_argument(
                "Input part size is smaller than allowed minimum of 5MiB.",
            ));
        }
        if configured_part_size > MAX_PART_SIZE {
            return Err(err_invalid_argument(
                "Input part size is bigger than allowed maximum of 5GiB.",
            ));
        }
        configured_part_size as f64
    } else {
        let p = (object_size as f64) / (MAX_PARTS_COUNT as f64);
        (p / MIN_PART_SIZE as f64).ceil() * MIN_PART_SIZE as f64
    };
    let total = (object_size as f64 / part_size_flt).ceil() as u64;
    let part_size = part_size_flt as u64;
    let last = object_size - (total - 1) * part_size;
    Ok((total, part_size, last))
}

/// minio-go `errEntityTooLarge`.
fn err_entity_too_large(total: u64, max: u64) -> S3Error {
    err_invalid_argument_code(
        "EntityTooLarge",
        format!("Your proposed upload size ‘{total}’ exceeds the maximum allowed object size ‘{max}’ for single PUT operation."),
    )
}

fn err_invalid_argument_code(code: &str, message: String) -> S3Error {
    S3Error::Response(Box::new(client::ErrorResponse {
        status_code: 400,
        code: code.to_string(),
        message,
        ..Default::default()
    }))
}

/// minio-go `trimEtag`.
fn trim_etag(etag: &str) -> String {
    etag.strip_prefix('"')
        .unwrap_or(etag)
        .strip_suffix('"')
        .unwrap_or(etag.strip_prefix('"').unwrap_or(etag))
        .to_string()
}

/// Go `encoding/xml.EscapeText`.
fn xml_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => out.push_str("&#34;"),
            '\'' => out.push_str("&#39;"),
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '\t' => out.push_str("&#x9;"),
            '\n' => out.push_str("&#xA;"),
            '\r' => out.push_str("&#xD;"),
            c => out.push(c),
        }
    }
    out
}

/// Go `url.QueryUnescape` (minio-go `decodeS3Name` with `encoding-type=url`).
fn query_unescape(s: &str) -> Result<String, String> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'%' => {
                let hex = s.get(i + 1..i + 3).ok_or_else(|| {
                    format!("invalid URL escape {:?}", &s[i..(i + 3).min(s.len())])
                })?;
                let v = u8::from_str_radix(hex, 16)
                    .map_err(|_| format!("invalid URL escape {:?}", &s[i..i + 3]))?;
                out.push(v);
                i += 3;
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8(out).map_err(|e| e.to_string())
}

/// One page of a ListObjects(V2) response.
#[derive(Default, Debug)]
struct ListPage {
    keys: Vec<String>,
    prefixes: Vec<String>,
    is_truncated: bool,
    next_continuation_token: String,
    next_marker: String,
    encoding_type: String,
}

fn parse_list_page(body: &[u8]) -> Result<ListPage, S3Error> {
    use quick_xml::events::Event;
    let mut reader = quick_xml::Reader::from_reader(body);
    let mut buf = Vec::new();
    let mut path: Vec<String> = Vec::new();
    let mut page = ListPage::default();
    let mut text = String::new();
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => {
                path.push(String::from_utf8_lossy(e.local_name().as_ref()).into_owned());
                text.clear();
            }
            Ok(Event::Text(t)) => {
                text.push_str(&t.unescape().map_err(|e| S3Error::Other(e.to_string()))?)
            }
            Ok(Event::End(_)) => {
                let p: Vec<&str> = path.iter().map(String::as_str).collect();
                match p.as_slice() {
                    [_, "Contents", "Key"] => page.keys.push(text.clone()),
                    [_, "CommonPrefixes", "Prefix"] => page.prefixes.push(text.clone()),
                    [_, "IsTruncated"] => page.is_truncated = text.trim() == "true",
                    [_, "NextContinuationToken"] => page.next_continuation_token = text.clone(),
                    [_, "NextMarker"] => page.next_marker = text.clone(),
                    [_, "EncodingType"] => page.encoding_type = text.clone(),
                    _ => {}
                }
                path.pop();
                text.clear();
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(e) => return Err(S3Error::Other(e.to_string())),
        }
        buf.clear();
    }
    if path.is_empty() && page.keys.is_empty() && page.prefixes.is_empty() && body.iter().all(|b| b.is_ascii_whitespace()) {
        return Err(S3Error::Other("EOF".to_string()));
    }
    if page.encoding_type == "url" {
        for k in page.keys.iter_mut().chain(page.prefixes.iter_mut()) {
            *k = query_unescape(k).map_err(S3Error::Other)?;
        }
        if !page.next_marker.is_empty() {
            page.next_marker = query_unescape(&page.next_marker).map_err(S3Error::Other)?;
        }
    }
    Ok(page)
}

/// minio-go `s3utils.CheckValidBucketName`.
fn check_valid_bucket_name(name: &str) -> Result<(), String> {
    if name.trim().is_empty() {
        return Err("Bucket name cannot be empty".into());
    }
    if name.len() < 3 {
        return Err("Bucket name cannot be shorter than 3 characters".into());
    }
    if name.len() > 63 {
        return Err("Bucket name cannot be longer than 63 characters".into());
    }
    let parts: Vec<&str> = name.split('.').collect();
    if parts.len() == 4 && parts.iter().all(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit())) {
        return Err("Bucket name cannot be an ip address".into());
    }
    if name.contains("..") || name.contains(".-") || name.contains("-.") {
        return Err("Bucket name contains invalid characters".into());
    }
    let b = name.as_bytes();
    let ok_edge = |c: u8| c.is_ascii_alphanumeric();
    let ok_mid = |c: u8| c.is_ascii_alphanumeric() || matches!(c, b'.' | b'-' | b'_' | b':');
    if !ok_edge(b[0]) || !ok_edge(b[b.len() - 1]) || !b[1..b.len() - 1].iter().all(|&c| ok_mid(c)) {
        return Err("Bucket name contains invalid characters".into());
    }
    Ok(())
}

/// minio-go `s3utils.CheckValidObjectName`.
fn check_valid_object_name(name: &str) -> Result<(), S3Error> {
    if name.trim().is_empty() {
        return Err(S3Error::Other("Object name cannot be empty".into()));
    }
    if name.len() > 1024 {
        return Err(S3Error::Other("Object name cannot be longer than 1024 characters".into()));
    }
    Ok(())
}

/// minio-go `PutObjectOptions` (the fields thanos sets).
struct PutOptions {
    sse: Option<ServerSideEncryption>,
    storage_class: String,
    part_size: u64,
    send_content_md5: bool,
}

impl PutOptions {
    /// `PutObjectOptions.Header()`.
    fn header(&self) -> Result<HeaderMap, S3Error> {
        let mut h = HeaderMap::new();
        h.insert("content-type", HeaderValue::from_static("application/octet-stream"));
        if let Some(sse) = &self.sse {
            sse.marshal(&mut h).map_err(S3Error::Other)?;
        }
        if !self.storage_class.is_empty() {
            let val = HeaderValue::from_str(&self.storage_class)
                .map_err(|e| S3Error::Other(format!("invalid storage_class header: {e}")))?;
            h.insert("x-amz-storage-class", val);
        }
        Ok(h)
    }
}

impl S3Bucket {
    /// thanos `NewBucketWithConfig` with the config built by Mimir `newS3Config`.
    pub fn new(cfg: &S3Config, component: &str) -> Result<Self, String> {
        Self::new_with_env(cfg, component, credentials::process_env())
    }

    /// Constructs an [`S3Bucket`] with an explicit environment map (for tests).
    #[doc(hidden)]
    pub fn new_with_env_map(
        cfg: &S3Config,
        component: &str,
        env_map: std::collections::HashMap<String, String>,
    ) -> Result<Self, String> {
        let env: credentials::Env =
            Arc::new(move |k| env_map.get(k).cloned().unwrap_or_default());
        Self::new_with_env(cfg, component, env)
    }

    fn new_with_env(cfg: &S3Config, component: &str, env: credentials::Env) -> Result<Self, String> {
        // Mimir newS3Config: SSE first.
        let sse = build_thanos_sse(&cfg.sse)?;
        thanos_validate(cfg)?;
        let http = transport::HttpTransport::new(&cfg.http)?;
        let endpoint = s3utils::get_endpoint_url(&cfg.endpoint, !cfg.insecure)
            .map_err(|e| format!("initialize s3 client: {e}"))?;
        if s3utils::is_amazon_endpoint(&endpoint.hostname) && is_s3_express_bucket(&cfg.bucket_name) {
            return Err(format!(
                "S3 Express directory bucket {:?} is not supported by mimir-rs",
                cfg.bucket_name
            ));
        }
        let creds = credentials::new_credentials(
            &credentials::CredentialsConfig {
                aws_sdk_auth: cfg.native_aws_auth_enabled,
                access_key: cfg.access_key_id.clone(),
                secret_key: cfg.secret_access_key.clone(),
                session_token: cfg.session_token.clone(),
                region: cfg.region.clone(),
                sts_endpoint: cfg.sts_endpoint.clone(),
            },
            env,
        );
        let client = Client::new(ClientOptions {
            endpoint,
            secure: !cfg.insecure,
            region: cfg.region.clone(),
            lookup: cfg.bucket_lookup_type,
            dualstack_enabled: cfg.dualstack_enabled,
            credentials: creds,
            http,
            component: component.to_string(),
            trace: cfg.trace.enabled,
        });
        if !cfg.list_objects_version.is_empty()
            && cfg.list_objects_version != "v1"
            && cfg.list_objects_version != "v2"
        {
            return Err(format!(
                "Initialize s3 client list objects version: Unsupported version {:?} was provided. Supported values are v1, v2",
                cfg.list_objects_version
            ));
        }
        Ok(Self {
            client: Arc::new(client),
            name: cfg.bucket_name.clone(),
            default_sse: sse,
            storage_class: cfg.storage_class.clone(),
            part_size: cfg.part_size,
            list_objects_v1: cfg.list_objects_version == "v1",
            send_content_md5: cfg.send_content_md5,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    fn bucket_check(&self) -> Result<(), S3Error> {
        check_valid_bucket_name(&self.name).map_err(S3Error::Other)
    }

    /// thanos `Upload` (the SSE override is the context value set by Mimir's `SSEBucketClient`).
    pub async fn upload(
        &self,
        name: &str,
        source: UploadSource,
        sse_override: Option<ServerSideEncryption>,
    ) -> Result<(), S3BucketError> {
        let size = source.body().len();
        let mut part_size = self.part_size;
        if size < part_size {
            part_size = 0;
        }
        let opts = PutOptions {
            sse: sse_override.or_else(|| self.default_sse.clone()),
            storage_class: self.storage_class.clone(),
            part_size,
            send_content_md5: self.send_content_md5,
        };
        self.put_object(name, &source, size, &opts)
            .await
            .map_err(|e| S3BucketError::wrap("upload s3 object", e))
    }

    /// minio-go `PutObject` for a known size and a seekable (ReaderAt) source.
    async fn put_object(
        &self,
        name: &str,
        source: &UploadSource,
        size: u64,
        opts: &PutOptions,
    ) -> Result<(), S3Error> {
        if size > MAX_OBJECT_SIZE {
            return Err(err_entity_too_large(size, MAX_OBJECT_SIZE));
        }
        if s3utils::is_google_endpoint(&self.client.endpoint.hostname) {
            return self.put_object_single(name, source, size, opts).await;
        }
        let part_size = if opts.part_size == 0 { MIN_PART_SIZE } else { opts.part_size };
        if size <= part_size {
            return self.put_object_single(name, source, size, opts).await;
        }
        if opts.send_content_md5 {
            self.put_object_multipart_sequential(name, source, size, opts).await
        } else {
            self.put_object_multipart_parallel(name, source, size, opts).await
        }
    }

    /// `putObject` + `putObjectDo`.
    async fn put_object_single(
        &self,
        name: &str,
        source: &UploadSource,
        size: u64,
        opts: &PutOptions,
    ) -> Result<(), S3Error> {
        self.bucket_check()?;
        check_valid_object_name(name)?;
        let body = source.body();
        let mut meta = RequestMetadata::new(&self.name, name);
        if opts.send_content_md5 {
            let data = body.read_all().await.map_err(|e| S3Error::Other(e.to_string()))?;
            meta.content_md5_base64 =
                base64::engine::general_purpose::STANDARD.encode(Md5::digest(&data));
            meta.body = Body::Bytes(data);
        } else {
            meta.body = body;
        }
        let _ = size;
        meta.custom_header = opts.header()?;
        meta.stream_sha256 = true;
        let resp = self.client.execute_method("PUT", meta).await?;
        if resp.status != 200 {
            return Err(response_error(&resp, &self.name, name));
        }
        Ok(())
    }

    /// `initiateMultipartUpload`.
    async fn initiate_multipart_upload(&self, name: &str, opts: &PutOptions) -> Result<String, S3Error> {
        self.bucket_check()?;
        check_valid_object_name(name)?;
        let mut meta = RequestMetadata::new(&self.name, name);
        meta.query = vec![("uploads".into(), String::new())];
        meta.custom_header = opts.header()?;
        let resp = self.client.execute_method("POST", meta).await?;
        if resp.status != 200 {
            return Err(response_error(&resp, &self.name, name));
        }
        let (_, fields) = client::xml_root_children(&resp.body)
            .ok_or_else(|| S3Error::Other("EOF".to_string()))?;
        Ok(fields
            .into_iter()
            .find(|(k, _)| k == "UploadId")
            .map(|(_, v)| v)
            .unwrap_or_default())
    }

    /// `uploadPart`: returns the trimmed ETag.
    async fn upload_part(
        &self,
        name: &str,
        upload_id: &str,
        part_number: u64,
        body: Body,
        md5_base64: String,
    ) -> Result<String, S3Error> {
        if body.len() > MAX_PART_SIZE {
            return Err(err_entity_too_large(body.len(), MAX_PART_SIZE));
        }
        let mut meta = RequestMetadata::new(&self.name, name);
        meta.query = vec![
            ("partNumber".into(), part_number.to_string()),
            ("uploadId".into(), upload_id.to_string()),
        ];
        meta.body = body;
        meta.content_md5_base64 = md5_base64;
        meta.stream_sha256 = true;
        let resp = self.client.execute_method("PUT", meta).await?;
        if resp.status != 200 {
            return Err(response_error(&resp, &self.name, name));
        }
        Ok(trim_etag(resp.header("etag")))
    }

    /// `completeMultipartUpload`.
    async fn complete_multipart_upload(
        &self,
        name: &str,
        upload_id: &str,
        parts: &[(u64, String)],
        sse: &Option<ServerSideEncryption>,
    ) -> Result<(), S3Error> {
        let mut xml = String::from(
            "<CompleteMultipartUpload xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">",
        );
        for (n, etag) in parts {
            xml.push_str(&format!(
                "<Part><PartNumber>{n}</PartNumber><ETag>{}</ETag></Part>",
                xml_escape(etag)
            ));
        }
        xml.push_str("</CompleteMultipartUpload>");
        let body = Bytes::from(xml);
        let mut headers = PutOptions {
            sse: sse.clone(),
            storage_class: String::new(),
            part_size: 0,
            send_content_md5: false,
        }
        .header()?;
        if s3utils::is_amazon_endpoint(&self.client.endpoint.hostname) {
            headers.remove("x-amz-server-side-encryption-aws-kms-key-id");
            headers.remove("x-amz-server-side-encryption");
            headers.remove("x-amz-server-side-encryption-context");
        }
        let mut meta = RequestMetadata::new(&self.name, name);
        meta.query = vec![("uploadId".into(), upload_id.to_string())];
        meta.content_sha256_hex = signer::sum256_hex(&body);
        meta.body = Body::Bytes(body);
        meta.custom_header = headers;
        meta.expect_200_ok_with_error = true;
        let resp = self.client.execute_method("POST", meta).await?;
        if resp.status != 200 {
            return Err(response_error(&resp, &self.name, name));
        }
        match client::xml_root_children(&resp.body) {
            None => Err(S3Error::Other("EOF".to_string())),
            Some((_, fields)) => {
                let bucket = fields.iter().find(|(k, _)| k == "Bucket").map(|(_, v)| v.as_str());
                if bucket.unwrap_or("").is_empty() {
                    // Decode the body as an <Error> document (minio-go ErrorResponse).
                    return Err(match client::xml_root_children(&resp.body) {
                        Some((root, fields)) if root == "Error" => {
                            let mut err = client::ErrorResponse::default();
                            for (k, v) in fields {
                                match k.as_str() {
                                    "Code" => err.code = v,
                                    "Message" => err.message = v,
                                    "BucketName" => err.bucket_name = v,
                                    "Key" => err.key = v,
                                    "RequestId" => err.request_id = v,
                                    "HostId" => err.host_id = v,
                                    "Region" => err.region = v,
                                    _ => {}
                                }
                            }
                            S3Error::Response(Box::new(err))
                        }
                        Some((root, _)) => S3Error::Other(format!(
                            "expected element type <Error> but have <{root}>"
                        )),
                        None => S3Error::Other("EOF".to_string()),
                    });
                }
                Ok(())
            }
        }
    }

    /// `abortMultipartUpload` (errors are ignored by the callers, as in minio-go).
    async fn abort_multipart_upload(&self, name: &str, upload_id: &str) {
        let mut meta = RequestMetadata::new(&self.name, name);
        meta.query = vec![("uploadId".into(), upload_id.to_string())];
        meta.content_sha256_hex = signer::EMPTY_SHA256_HEX.to_string();
        if let Err(e) = self.client.execute_method("DELETE", meta).await {
            tracing::debug!(err = %e, object = name, "abort multipart upload failed");
        }
    }

    /// `putObjectMultipartStreamFromReadAt` (4 parallel part uploads).
    async fn put_object_multipart_parallel(
        &self,
        name: &str,
        source: &UploadSource,
        size: u64,
        opts: &PutOptions,
    ) -> Result<(), S3Error> {
        self.bucket_check()?;
        check_valid_object_name(name)?;
        let (total, part_size, last_part_size) = optimal_part_info(size, opts.part_size)?;
        let upload_id = self.initiate_multipart_upload(name, opts).await?;
        let mut abort_guard = S3AbortOnDrop::new(&self.client, &self.name, name, upload_id.clone());
        let result = async {
            let next = std::sync::atomic::AtomicU64::new(1);
            let results = parking_lot::Mutex::new(Vec::<(u64, String)>::new());
            let worker = || async {
                loop {
                    let n = next.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    if n > total {
                        return Ok::<(), S3Error>(());
                    }
                    let (offset, len) = if n == total {
                        (size - last_part_size, last_part_size)
                    } else {
                        ((n - 1) * part_size, part_size)
                    };
                    let etag = self
                        .upload_part(name, &upload_id, n, source.section(offset, len), String::new())
                        .await?;
                    results.lock().push((n, etag));
                }
            };
            let workers: Vec<_> = (0..NUM_THREADS).map(|_| worker()).collect();
            futures::future::try_join_all(workers).await?;
            let mut parts = results.into_inner();
            parts.sort_by_key(|(n, _)| *n);
            Ok::<_, S3Error>(parts)
        }
        .await;
        let parts = match result {
            Ok(p) => p,
            Err(e) => {
                abort_guard.disarm();
                self.abort_multipart_upload(name, &upload_id).await;
                return Err(e);
            }
        };
        if let Err(e) = self
            .complete_multipart_upload(name, &upload_id, &parts, &opts.sse)
            .await
        {
            abort_guard.disarm();
            self.abort_multipart_upload(name, &upload_id).await;
            return Err(e);
        }
        abort_guard.disarm();
        Ok(())
    }

    /// `putObjectMultipartStreamOptionalChecksum` (sequential, `Content-Md5` per part).
    async fn put_object_multipart_sequential(
        &self,
        name: &str,
        source: &UploadSource,
        size: u64,
        opts: &PutOptions,
    ) -> Result<(), S3Error> {
        self.bucket_check()?;
        check_valid_object_name(name)?;
        let (total, part_size, last_part_size) = optimal_part_info(size, opts.part_size)?;
        let upload_id = self.initiate_multipart_upload(name, opts).await?;
        let mut abort_guard = S3AbortOnDrop::new(&self.client, &self.name, name, upload_id.clone());
        let mut parts = Vec::new();
        for n in 1..=total {
            let (offset, len) = if n == total {
                (size - last_part_size, last_part_size)
            } else {
                ((n - 1) * part_size, part_size)
            };
            let data = match source.section(offset, len).read_all().await {
                Ok(d) => d,
                Err(e) => {
                    abort_guard.disarm();
                    self.abort_multipart_upload(name, &upload_id).await;
                    return Err(S3Error::Other(e.to_string()));
                }
            };
            let md5 = if opts.send_content_md5 {
                base64::engine::general_purpose::STANDARD.encode(Md5::digest(&data))
            } else {
                String::new()
            };
            match self.upload_part(name, &upload_id, n, Body::Bytes(data), md5).await {
                Ok(etag) => parts.push((n, etag)),
                Err(e) => {
                    abort_guard.disarm();
                    self.abort_multipart_upload(name, &upload_id).await;
                    return Err(e);
                }
            }
        }
        if let Err(e) = self
            .complete_multipart_upload(name, &upload_id, &parts, &opts.sse)
            .await
        {
            abort_guard.disarm();
            self.abort_multipart_upload(name, &upload_id).await;
            return Err(e);
        }
        abort_guard.disarm();
        Ok(())
    }

    /// thanos `getRange` (`length == -1` reads to the end).
    pub async fn get_range(&self, name: &str, off: i64, length: i64) -> Result<Bytes, S3BucketError> {
        let mut headers = HeaderMap::new();
        if length != -1 {
            let (start, end) = (off, off + length - 1);
            let range = range_header(start, end).map_err(S3BucketError::new)?;
            let val = HeaderValue::from_str(&range)
                .map_err(|e| S3BucketError::new(S3Error::Other(format!("invalid range header: {e}"))))?;
            headers.insert("range", val);
        } else if off > 0 {
            let val = HeaderValue::from_str(&format!("bytes={off}-"))
                .map_err(|e| S3BucketError::new(S3Error::Other(format!("invalid range header: {e}"))))?;
            headers.insert("range", val);
        }
        self.bucket_check().map_err(S3BucketError::new)?;
        check_valid_object_name(name).map_err(S3BucketError::new)?;
        let mut meta = RequestMetadata::new(&self.name, name);
        meta.custom_header = headers;
        meta.content_sha256_hex = signer::EMPTY_SHA256_HEX.to_string();
        let resp = self
            .client
            .execute_method("GET", meta)
            .await
            .map_err(S3BucketError::new)?;
        if resp.status != 200 && resp.status != 206 {
            return Err(S3BucketError::new(response_error(&resp, &self.name, name)));
        }
        Ok(resp.body)
    }

    /// thanos `Get`.
    pub async fn get(&self, name: &str) -> Result<Bytes, S3BucketError> {
        self.get_range(name, 0, -1).await
    }

    /// minio-go `StatObject`.
    async fn stat_object(&self, name: &str) -> Result<S3ObjectInfo, S3Error> {
        if let Err(e) = check_valid_bucket_name(&self.name) {
            return Err(err_invalid_argument_code("InvalidBucketName", e));
        }
        if name.trim().is_empty() {
            return Err(err_invalid_argument_code(
                "XMinioInvalidObjectName",
                "Object name cannot be empty".into(),
            ));
        }
        let mut meta = RequestMetadata::new(&self.name, name);
        meta.content_sha256_hex = signer::EMPTY_SHA256_HEX.to_string();
        let resp = self.client.execute_method("HEAD", meta).await?;
        if resp.status != 200 && resp.status != 206 {
            return Err(response_error(&resp, &self.name, name));
        }
        to_object_info(&resp, &self.name, name)
    }

    /// thanos `Exists`.
    pub async fn exists(&self, name: &str) -> Result<bool, S3BucketError> {
        match self.stat_object(name).await {
            Ok(_) => Ok(true),
            Err(e) if e.code() == "NoSuchKey" => Ok(false),
            Err(e) => Err(S3BucketError::wrap("stat s3 object", e)),
        }
    }

    /// thanos `Attributes`.
    pub async fn attributes(&self, name: &str) -> Result<S3ObjectInfo, S3BucketError> {
        self.stat_object(name).await.map_err(S3BucketError::new)
    }

    /// thanos `Delete` (minio-go `RemoveObject`).
    pub async fn delete(&self, name: &str) -> Result<(), S3BucketError> {
        self.bucket_check().map_err(S3BucketError::new)?;
        check_valid_object_name(name).map_err(S3BucketError::new)?;
        let mut meta = RequestMetadata::new(&self.name, name);
        meta.content_sha256_hex = signer::EMPTY_SHA256_HEX.to_string();
        let resp = self
            .client
            .execute_method("DELETE", meta)
            .await
            .map_err(S3BucketError::new)?;
        if resp.status != 204 {
            return Err(S3BucketError::new(response_error(&resp, &self.name, name)));
        }
        Ok(())
    }

    /// thanos `Iter` / `IterWithAttributes` (minio-go `ListObjects`, V2 unless
    /// `list_objects_version: v1` or a snowball location). Entries are returned in listing
    /// order (per page: objects, then common prefixes).
    pub async fn iter(&self, dir: &str, recursive: bool) -> Result<Vec<String>, S3BucketError> {
        let dir = if dir.is_empty() {
            String::new()
        } else {
            format!("{}/", dir.trim_end_matches('/'))
        };
        self.bucket_check().map_err(S3BucketError::new)?;
        let delimiter = if recursive { "" } else { "/" };
        let use_v1 = self.list_objects_v1
            || self.client.cached_location(&self.name).as_deref() == Some("snowball");
        let mut out = Vec::new();
        let mut token = String::new();
        let mut marker = String::new();
        loop {
            let mut meta = RequestMetadata::new(&self.name, "");
            meta.content_sha256_hex = signer::EMPTY_SHA256_HEX.to_string();
            if use_v1 {
                meta.query = vec![
                    ("prefix".into(), dir.clone()),
                    ("delimiter".into(), delimiter.into()),
                ];
                if !marker.is_empty() {
                    meta.query.push(("marker".into(), marker.clone()));
                }
                meta.query.push(("encoding-type".into(), "url".into()));
            } else {
                meta.query = vec![
                    ("list-type".into(), "2".into()),
                    ("encoding-type".into(), "url".into()),
                    ("prefix".into(), dir.clone()),
                    ("delimiter".into(), delimiter.into()),
                ];
                if !token.is_empty() {
                    meta.query.push(("continuation-token".into(), token.clone()));
                }
                meta.query.push(("fetch-owner".into(), "true".into()));
            }
            let resp = self
                .client
                .execute_method("GET", meta)
                .await
                .map_err(S3BucketError::new)?;
            if resp.status != 200 {
                return Err(S3BucketError::new(response_error(&resp, &self.name, "")));
            }
            let page = parse_list_page(&resp.body).map_err(S3BucketError::new)?;
            if !use_v1 && page.is_truncated && page.next_continuation_token.is_empty() {
                return Err(S3BucketError::new(S3Error::Response(Box::new(client::ErrorResponse {
                    code: "NotImplemented".into(),
                    message: "Truncated response should have continuation token set".into(),
                    ..Default::default()
                }))));
            }
            for key in &page.keys {
                marker = key.clone();
            }
            for key in page.keys.into_iter().chain(page.prefixes) {
                if key.is_empty() || key == dir {
                    continue;
                }
                out.push(key);
            }
            if use_v1 {
                if !page.next_marker.is_empty() {
                    marker = page.next_marker;
                }
            } else if !page.next_continuation_token.is_empty() {
                token = page.next_continuation_token;
            }
            if !page.is_truncated {
                return Ok(out);
            }
        }
    }
}

/// minio-go `IsS3ExpressBucket`.
fn is_s3_express_bucket(name: &str) -> bool {
    let parts: Vec<&str> = name.split("--").collect();
    parts.len() == 3 && parts[2] == "x-s3"
}

/// `GetObjectOptions.SetRange`.
fn range_header(start: i64, end: i64) -> Result<String, S3Error> {
    if start == 0 && end < 0 {
        Ok(format!("bytes={end}"))
    } else if 0 < start && end == 0 {
        Ok(format!("bytes={start}-"))
    } else if 0 <= start && start <= end {
        Ok(format!("bytes={start}-{end}"))
    } else {
        Err(err_invalid_argument(format!(
            "Invalid range specified: start={start} end={end}"
        )))
    }
}

fn response_error(resp: &client::Response, bucket: &str, object: &str) -> S3Error {
    let mut e = client::http_resp_to_error_response(resp.status, &resp.headers, &resp.body, bucket, object)
        .unwrap_or_default();
    if e.code.is_empty() && e.message.is_empty() {
        // A 2xx answer that is not the expected one (e.g. 200 instead of 204 on delete):
        // minio-go's xml decode fails with EOF and falls back to the status text.
        let reason = reqwest::StatusCode::from_u16(resp.status)
            .ok()
            .and_then(|s| s.canonical_reason())
            .unwrap_or("");
        e.code = format!("{} {reason}", resp.status);
        e.message = e.code.clone();
    }
    S3Error::Response(Box::new(e))
}

/// minio-go `ToObjectInfo` (size, mtime, ETag).
fn to_object_info(resp: &client::Response, bucket: &str, object: &str) -> Result<S3ObjectInfo, S3Error> {
    let internal = |message: String| {
        S3Error::Response(Box::new(client::ErrorResponse {
            code: "InternalError".into(),
            message,
            bucket_name: bucket.into(),
            key: object.into(),
            request_id: resp.header("x-amz-request-id").into(),
            host_id: resp.header("x-amz-id-2").into(),
            region: resp.header("x-amz-bucket-region").into(),
            ..Default::default()
        }))
    };
    let size = match resp.header("content-length") {
        "" => -1,
        s => s.parse::<i64>().map_err(|e| {
            internal(format!("Content-Length is not an integer, failed with {e}"))
        })?,
    };
    let lm = resp.header("last-modified");
    let mut mtime = DateTime::parse_from_rfc2822(lm)
        .map(|d| d.with_timezone(&Utc))
        .or_else(|_| {
            chrono::NaiveDateTime::parse_from_str(lm, "%a, %d %b %Y %H:%M:%S GMT").map(|n| n.and_utc())
        })
        .map_err(|e| internal(format!("Last-Modified time format is invalid, failed with {e}")))?;
    let src = resp.header("x-minio-source-mtime");
    if !src.is_empty() {
        mtime = DateTime::parse_from_rfc3339(src)
            .map(|d| d.with_timezone(&Utc))
            .map_err(|e| internal(format!("X-Minio-Source-Mtime is not in supported format: {e}")))?;
    }
    Ok(S3ObjectInfo {
        size,
        last_modified: mtime,
        etag: trim_etag(resp.header("etag")),
    })
}

struct S3AbortOnDrop {
    client: Arc<Client>,
    bucket: String,
    key: String,
    upload_id: Option<String>,
}

impl S3AbortOnDrop {
    fn new(client: &Arc<Client>, bucket: &str, key: &str, upload_id: String) -> Self {
        Self {
            client: Arc::clone(client),
            bucket: bucket.to_string(),
            key: key.to_string(),
            upload_id: Some(upload_id),
        }
    }

    fn disarm(&mut self) {
        self.upload_id = None;
    }
}

impl Drop for S3AbortOnDrop {
    fn drop(&mut self) {
        if let Some(upload_id) = self.upload_id.take() {
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                let client = Arc::clone(&self.client);
                let bucket = std::mem::take(&mut self.bucket);
                let key = std::mem::take(&mut self.key);
                handle.spawn(async move {
                    let mut meta = RequestMetadata::new(&bucket, &key);
                    meta.query = vec![("uploadId".into(), upload_id)];
                    meta.content_sha256_hex = signer::EMPTY_SHA256_HEX.to_string();
                    let _ = client.execute_method("DELETE", meta).await;
                });
            }
        }
    }
}

#[cfg(test)]
#[allow(unused_must_use)]
mod tests;
