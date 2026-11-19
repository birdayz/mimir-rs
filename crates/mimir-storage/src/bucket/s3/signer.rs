//! AWS Signature Version 4 (header and streaming/chunked) exactly as minio-go signs requests.
//!
//! Ported from:
//! - github.com/minio/minio-go/v7/pkg/signer/request-signature-v4.go (`signV4`,
//!   `getCanonicalRequest`, `getCanonicalHeaders`, `getSignedHeaders`, `getSigningKey`, ...)
//! - github.com/minio/minio-go/v7/pkg/signer/request-signature-streaming.go (`StreamingSignV4`,
//!   `prepareStreamingRequest`, `getStreamLength`, `buildChunkSignature`, `StreamingReader`)
//! - github.com/minio/minio-go/v7/pkg/signer/utils.go (`signV4TrimAll`, `getHostAddr`)
//! - github.com/minio/minio-go/v7/pkg/s3utils/utils.go (`EncodePath`)
//! - Go `net/url` `QueryEscape` / `Values.Encode` (canonical query string)
//!
//! Signature V2 is intentionally absent: with the thanos objstore S3 provider the configured
//! `signature_version: v2` never reaches minio-go's signer (thanos only overrides `Retrieve()`,
//! while minio-go's credential chain calls `RetrieveWithCredContext()`), so Go Mimir always signs
//! with V4. mimir-rs reproduces that behaviour.

use chrono::{DateTime, Utc};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use ring::{digest, hmac};

pub(crate) const SIGN_V4_ALGORITHM: &str = "AWS4-HMAC-SHA256";
pub(crate) const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";
pub(crate) const EMPTY_SHA256_HEX: &str =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
pub(crate) const STREAMING_SIGN_ALGORITHM: &str = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";
const STREAMING_PAYLOAD_HDR: &str = "AWS4-HMAC-SHA256-PAYLOAD";
/// minio-go `payloadChunkSize`.
pub(crate) const PAYLOAD_CHUNK_SIZE: u64 = 64 * 1024;
const CHUNK_SIG_CONST_LEN: u64 = 17; // ";chunk-signature="
const SIGNATURE_STR_LEN: u64 = 64;
const CRLF_LEN: u64 = 2;

pub(crate) const SERVICE_TYPE_S3: &str = "s3";
pub(crate) const SERVICE_TYPE_S3_OUTPOSTS: &str = "s3-outposts";

const ISO8601_DATE_FORMAT: &str = "%Y%m%dT%H%M%SZ";
const YYYYMMDD: &str = "%Y%m%d";

/// The parts of an outgoing request that take part in signing.
#[derive(Clone, Debug)]
pub(crate) struct SignableRequest {
    pub method: &'static str,
    /// Go `req.Host` (`host[:port]`).
    pub host: String,
    /// Decoded URL path (Go `req.URL.Path`), e.g. `/bucket/object`.
    pub path: String,
    /// Query parameters (Go `url.Values`).
    pub query: Vec<(String, String)>,
    pub headers: HeaderMap,
}

impl SignableRequest {
    pub fn set_header(&mut self, name: &str, value: &str) -> Result<(), String> {
        let name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|e| format!("invalid header name {name:?}: {e}"))?;
        let value = HeaderValue::from_str(value)
            .map_err(|e| format!("invalid header value for {name}: {e}"))?;
        self.headers.insert(name, value);
        Ok(())
    }

    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(|v| v.to_str().ok())
    }
}

pub(crate) fn sum256(data: &[u8]) -> Vec<u8> {
    digest::digest(&digest::SHA256, data).as_ref().to_vec()
}

pub(crate) fn sum256_hex(data: &[u8]) -> String {
    hex::encode(sum256(data))
}

fn sum_hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    let key = hmac::Key::new(hmac::HMAC_SHA256, key);
    hmac::sign(&key, data).as_ref().to_vec()
}

/// `getSigningKey`.
fn get_signing_key(secret: &str, loc: &str, t: DateTime<Utc>, service_type: &str) -> Vec<u8> {
    let date = sum_hmac(
        format!("AWS4{secret}").as_bytes(),
        t.format(YYYYMMDD).to_string().as_bytes(),
    );
    let location = sum_hmac(&date, loc.as_bytes());
    let service = sum_hmac(&location, service_type.as_bytes());
    sum_hmac(&service, b"aws4_request")
}

fn get_signature(signing_key: &[u8], string_to_sign: &str) -> String {
    hex::encode(sum_hmac(signing_key, string_to_sign.as_bytes()))
}

/// `getScope`.
fn get_scope(location: &str, t: DateTime<Utc>, service_type: &str) -> String {
    format!(
        "{}/{}/{}/aws4_request",
        t.format(YYYYMMDD),
        location,
        service_type
    )
}

/// `GetCredential`.
fn get_credential(access_key_id: &str, location: &str, t: DateTime<Utc>, service: &str) -> String {
    format!("{access_key_id}/{}", get_scope(location, t, service))
}

/// `signV4TrimAll`: `strings.Join(strings.Fields(input), " ")`.
pub(crate) fn sign_v4_trim_all(input: &str) -> String {
    input.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Go `url.QueryEscape`.
pub(crate) fn go_query_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Go `url.Values.Encode()` (keys sorted, values in insertion order) with `+` replaced by `%20`,
/// i.e. the canonical query minio-go signs and then sends (`getCanonicalRequest` rewrites
/// `req.URL.RawQuery`).
pub(crate) fn canonical_query(query: &[(String, String)]) -> String {
    let mut keys: Vec<&str> = query.iter().map(|(k, _)| k.as_str()).collect();
    keys.sort();
    keys.dedup();
    let mut out = String::new();
    for k in keys {
        let key = go_query_escape(k);
        for (_, v) in query.iter().filter(|(qk, _)| qk == k) {
            if !out.is_empty() {
                out.push('&');
            }
            out.push_str(&key);
            out.push('=');
            out.push_str(&go_query_escape(v));
        }
    }
    out.replace('+', "%20")
}

/// Encodes `raw_path` via [`encode_path`] and normalizes dot-segments identically to
/// `reqwest::Url::parse` (WHATWG URL) so SigV4 `CanonicalURI` always matches the wire path.
pub(crate) fn canonical_uri_path(raw_path: &str) -> String {
    let encoded = encode_path(raw_path);
    if !encoded.contains('.') && !encoded.contains("%2e") && !encoded.contains("%2E") {
        return encoded;
    }
    let probe = if encoded.starts_with('/') {
        format!("http://localhost{encoded}")
    } else {
        format!("http://localhost/{encoded}")
    };
    reqwest::Url::parse(&probe)
        .map(|u| u.path().to_string())
        .unwrap_or(encoded)
}

/// `s3utils.EncodePath`.
pub(crate) fn encode_path(path_name: &str) -> String {
    let reserved = path_name.bytes().all(|b| {
        b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~' | b'/')
    });
    if reserved && !path_name.is_empty() {
        return path_name.to_string();
    }
    let mut out = String::with_capacity(path_name.len());
    for c in path_name.chars() {
        if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | '~' | '/') {
            out.push(c);
            continue;
        }
        let mut buf = [0u8; 4];
        for b in c.encode_utf8(&mut buf).bytes() {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// minio-go's `v4IgnoredHeaders` / `ignoredStreamingHeaders`.
fn is_ignored(name: &str, streaming: bool) -> bool {
    match name {
        "authorization" | "user-agent" => true,
        "accept-encoding" => !streaming,
        "content-type" => streaming,
        _ => false,
    }
}

fn sorted_header_names(req: &SignableRequest, streaming: bool) -> Vec<String> {
    let mut names: Vec<String> = req
        .headers
        .keys()
        .map(|k| k.as_str().to_string())
        .filter(|k| !is_ignored(k, streaming))
        .collect();
    names.dedup();
    if !names.iter().any(|k| k == "host") {
        names.push("host".to_string());
    }
    names.sort();
    names
}

/// `getCanonicalHeaders`.
fn canonical_headers(req: &SignableRequest, streaming: bool) -> String {
    let mut buf = String::new();
    for k in sorted_header_names(req, streaming) {
        buf.push_str(&k);
        buf.push(':');
        if k == "host" {
            buf.push_str(&req.host);
        } else {
            let values: Vec<String> = req
                .headers
                .get_all(k.as_str())
                .iter()
                .map(|v| sign_v4_trim_all(&String::from_utf8_lossy(v.as_bytes())))
                .collect();
            buf.push_str(&values.join(","));
        }
        buf.push('\n');
    }
    buf
}

/// `getSignedHeaders`.
fn signed_headers(req: &SignableRequest, streaming: bool) -> String {
    sorted_header_names(req, streaming).join(";")
}

/// `getHashedPayload`.
fn hashed_payload(req: &SignableRequest) -> String {
    match req.header("x-amz-content-sha256") {
        Some(v) if !v.is_empty() => v.to_string(),
        _ => UNSIGNED_PAYLOAD.to_string(),
    }
}

/// `getCanonicalRequest`.
fn canonical_request(req: &SignableRequest, streaming: bool, hashed_payload: &str) -> String {
    [
        req.method.to_string(),
        canonical_uri_path(&req.path),
        canonical_query(&req.query),
        canonical_headers(req, streaming),
        signed_headers(req, streaming),
        hashed_payload.to_string(),
    ]
    .join("\n")
}

/// `getStringToSignV4`.
fn string_to_sign_v4(t: DateTime<Utc>, location: &str, canonical: &str, service: &str) -> String {
    format!(
        "{SIGN_V4_ALGORITHM}\n{}\n{}\n{}",
        t.format(ISO8601_DATE_FORMAT),
        get_scope(location, t, service),
        sum256_hex(canonical.as_bytes())
    )
}

/// `signV4` without trailer (minio-go `SignV4` / `SignV4Outposts`).
pub(crate) fn sign_v4(
    req: &mut SignableRequest,
    access_key_id: &str,
    secret_access_key: &str,
    session_token: &str,
    location: &str,
    service_type: &str,
    t: DateTime<Utc>,
) -> Result<(), String> {
    if access_key_id.is_empty() || secret_access_key.is_empty() {
        return Ok(());
    }
    req.set_header("X-Amz-Date", &t.format(ISO8601_DATE_FORMAT).to_string())?;
    if !session_token.is_empty() && req.header("x-amz-s3session-token").is_none() {
        req.set_header("X-Amz-Security-Token", session_token)?;
    }
    let hashed = hashed_payload(req);
    let canonical = canonical_request(req, false, &hashed);
    let string_to_sign = string_to_sign_v4(t, location, &canonical, service_type);
    let signing_key = get_signing_key(secret_access_key, location, t, service_type);
    let credential = get_credential(access_key_id, location, t, service_type);
    let signed = signed_headers(req, false);
    let signature = get_signature(&signing_key, &string_to_sign);
    req.set_header(
        "Authorization",
        &format!(
            "{SIGN_V4_ALGORITHM} Credential={credential}, SignedHeaders={signed}, Signature={signature}"
        ),
    )?;
    Ok(())
}

/// `getSignedChunkLength`.
fn signed_chunk_length(chunk_data_size: u64) -> u64 {
    format!("{chunk_data_size:x}").len() as u64
        + CHUNK_SIG_CONST_LEN
        + SIGNATURE_STR_LEN
        + CRLF_LEN
        + chunk_data_size
        + CRLF_LEN
}

/// `getStreamLength` (no trailers).
pub(crate) fn stream_length(data_len: u64, chunk_size: u64) -> u64 {
    if data_len == 0 {
        return 0;
    }
    let chunks = data_len / chunk_size;
    let remaining = data_len % chunk_size;
    let mut len = chunks * signed_chunk_length(chunk_size);
    if remaining > 0 {
        len += signed_chunk_length(remaining);
    }
    len + signed_chunk_length(0)
}

/// Chunk signer for an `aws-chunked` (`STREAMING-AWS4-HMAC-SHA256-PAYLOAD`) body
/// (minio-go `StreamingReader`).
#[derive(Clone, Debug)]
pub(crate) struct ChunkSigner {
    secret_access_key: String,
    region: String,
    service_type: &'static str,
    req_time: DateTime<Utc>,
    #[cfg_attr(not(test), allow(dead_code))]
    pub seed_signature: String,
    prev_signature: String,
}

impl ChunkSigner {
    /// `buildChunkSignature`.
    fn chunk_signature(&self, chunk_checksum: &str) -> String {
        let string_to_sign = [
            STREAMING_PAYLOAD_HDR.to_string(),
            self.req_time.format(ISO8601_DATE_FORMAT).to_string(),
            get_scope(&self.region, self.req_time, self.service_type),
            self.prev_signature.clone(),
            EMPTY_SHA256_HEX.to_string(),
            chunk_checksum.to_string(),
        ]
        .join("\n");
        let key = get_signing_key(
            &self.secret_access_key,
            &self.region,
            self.req_time,
            self.service_type,
        );
        get_signature(&key, &string_to_sign)
    }

    /// `signChunk`: appends `<hex len>;chunk-signature=<sig>\r\n<data>\r\n` to `out`.
    pub fn sign_chunk(&mut self, data: &[u8], out: &mut Vec<u8>) {
        let signature = self.chunk_signature(&sum256_hex(data));
        self.prev_signature = signature.clone();
        out.extend_from_slice(format!("{:x};chunk-signature={}\r\n", data.len(), signature).as_bytes());
        out.extend_from_slice(data);
        out.extend_from_slice(b"\r\n");
    }
}

/// `StreamingSignV4` (`prepareStreamingRequest` + seed signature + Authorization header).
/// Returns the signer for the body chunks and the on-the-wire Content-Length.
#[allow(clippy::too_many_arguments)]
pub(crate) fn streaming_sign_v4(
    req: &mut SignableRequest,
    access_key_id: &str,
    secret_access_key: &str,
    session_token: &str,
    region: &str,
    data_len: u64,
    req_time: DateTime<Utc>,
    service_type: &'static str,
) -> Result<(ChunkSigner, u64), String> {
    req.set_header("X-Amz-Content-Sha256", STREAMING_SIGN_ALGORITHM)?;
    if !session_token.is_empty() {
        req.set_header("X-Amz-Security-Token", session_token)?;
    }
    req.set_header("X-Amz-Date", &req_time.format(ISO8601_DATE_FORMAT).to_string())?;
    let content_length = stream_length(data_len, PAYLOAD_CHUNK_SIZE);
    req.set_header("x-amz-decoded-content-length", &data_len.to_string())?;

    let hashed = hashed_payload(req);
    let canonical = canonical_request(req, true, &hashed);
    let string_to_sign = string_to_sign_v4(req_time, region, &canonical, service_type);
    let key = get_signing_key(secret_access_key, region, req_time, service_type);
    let seed_signature = get_signature(&key, &string_to_sign);

    let credential = get_credential(access_key_id, region, req_time, service_type);
    let auth = format!(
        "{SIGN_V4_ALGORITHM} Credential={credential},SignedHeaders={},Signature={seed_signature}",
        signed_headers(req, true)
    );
    req.set_header("Authorization", &auth)?;
    Ok((
        ChunkSigner {
            secret_access_key: secret_access_key.to_string(),
            region: region.to_string(),
            service_type,
            req_time,
            prev_signature: seed_signature.clone(),
            seed_signature,
        },
        content_length,
    ))
}

/// Encodes a whole payload as minio-go's `StreamingReader` would emit it.
#[cfg(test)]
pub(crate) fn encode_streaming_body(signer: &mut ChunkSigner, data: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    for chunk in data.chunks(PAYLOAD_CHUNK_SIZE as usize) {
        signer.sign_chunk(chunk, &mut out);
    }
    signer.sign_chunk(&[], &mut out);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn t() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2013, 5, 24, 0, 0, 0).unwrap()
    }

    const AK: &str = "AKIAIOSFODNN7EXAMPLE";
    const SK: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";

    fn put_req() -> SignableRequest {
        let mut r = SignableRequest {
            method: "PUT",
            host: "s3.amazonaws.com".into(),
            path: "/examplebucket/chunkObject.txt".into(),
            query: vec![],
            headers: HeaderMap::new(),
        };
        r.set_header("x-amz-storage-class", "REDUCED_REDUNDANCY").unwrap();
        r
    }

    // minio-go pkg/signer/request-signature-streaming_test.go TestGetSeedSignature.
    #[test]
    fn test_get_seed_signature() {
        let mut req = put_req();
        let (signer, _) = streaming_sign_v4(&mut req, AK, SK, "", "us-east-1", 66560, t(), SERVICE_TYPE_S3).unwrap();
        assert_eq!(
            signer.seed_signature,
            "38cab3af09aa15ddf29e26e36236f60fb6bfb6243a20797ae9a8183674526079"
        );
    }

    // TestChunkSignature.
    #[test]
    fn test_chunk_signature() {
        let signer = ChunkSigner {
            secret_access_key: SK.into(),
            region: "us-east-1".into(),
            service_type: SERVICE_TYPE_S3,
            req_time: t(),
            seed_signature: String::new(),
            prev_signature: "4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9".into(),
        };
        let data = vec![b'a'; 65536];
        assert_eq!(
            signer.chunk_signature(&sum256_hex(&data)),
            "ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648"
        );
    }

    // TestSetStreamingAuthorization.
    #[test]
    fn test_set_streaming_authorization() {
        let mut req = put_req();
        streaming_sign_v4(&mut req, AK, SK, "", "us-east-1", 65 * 1024, t(), SERVICE_TYPE_S3).unwrap();
        assert_eq!(
            req.header("authorization").unwrap(),
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-storage-class,Signature=38cab3af09aa15ddf29e26e36236f60fb6bfb6243a20797ae9a8183674526079"
        );
    }

    // TestStreamingReader + AWS documented example (66560 bytes -> 64KiB + 1KiB + final chunk).
    #[test]
    fn test_streaming_body_matches_aws_example() {
        let mut req = put_req();
        let (mut signer, content_length) =
            streaming_sign_v4(&mut req, AK, SK, "", "us-east-1", 66560, t(), SERVICE_TYPE_S3).unwrap();
        let body = encode_streaming_body(&mut signer, &vec![b'a'; 66560]);
        assert_eq!(body.len() as u64, content_length);
        assert_eq!(content_length, 66824);
        // 64KiB + 1KiB + final chunk, each signature chained to the previous one.
        let text = String::from_utf8_lossy(&body);
        let headers: Vec<&str> = text
            .split("\r\n")
            .filter(|l| l.contains(";chunk-signature="))
            .collect();
        assert_eq!(headers.len(), 3);
        assert!(headers[0].starts_with("10000;chunk-signature="));
        assert!(headers[1].starts_with("400;chunk-signature="));
        assert!(headers[2].starts_with("0;chunk-signature="));
        assert!(text.ends_with("\r\n\r\n"));
        let mut check = ChunkSigner {
            prev_signature: signer.seed_signature.clone(),
            ..signer.clone()
        };
        let mut expected = Vec::new();
        check.sign_chunk(&vec![b'a'; 65536], &mut expected);
        assert_eq!(&body[..expected.len()], &expected[..]);
    }

    // AWS SigV4 test-suite style check of the header signer (GET object with Range).
    #[test]
    fn test_sign_v4_get_object_aws_example() {
        let mut req = SignableRequest {
            method: "GET",
            host: "examplebucket.s3.amazonaws.com".into(),
            path: "/test.txt".into(),
            query: vec![],
            headers: HeaderMap::new(),
        };
        req.set_header("Range", "bytes=0-9").unwrap();
        req.set_header("X-Amz-Content-Sha256", EMPTY_SHA256_HEX).unwrap();
        sign_v4(&mut req, AK, SK, "", "us-east-1", SERVICE_TYPE_S3, t()).unwrap();
        // Expected values from the AWS "GET Object" SigV4 example.
        assert_eq!(
            req.header("authorization").unwrap(),
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
        );
    }

    // AWS "GET Bucket (List Objects)" example: canonical query sorting/encoding.
    #[test]
    fn test_sign_v4_list_objects_aws_example() {
        let mut req = SignableRequest {
            method: "GET",
            host: "examplebucket.s3.amazonaws.com".into(),
            path: "/".into(),
            query: vec![("prefix".into(), "J".into()), ("max-keys".into(), "2".into())],
            headers: HeaderMap::new(),
        };
        req.set_header("X-Amz-Content-Sha256", EMPTY_SHA256_HEX).unwrap();
        sign_v4(&mut req, AK, SK, "", "us-east-1", SERVICE_TYPE_S3, t()).unwrap();
        assert_eq!(
            req.header("authorization").unwrap(),
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7"
        );
    }

    // minio-go pkg/signer/utils_test.go TestSignV4TrimAll.
    #[test]
    fn test_sign_v4_trim_all() {
        for (input, want) in [
            ("本語", "本語"),
            (" abc ", "abc"),
            (" a b ", "a b"),
            ("a b ", "a b"),
            ("a  b", "a b"),
            ("a   b", "a b"),
            ("   a   b  c   ", "a b c"),
            ("a \t b  c   ", "a b c"),
            ("\"a \t b  c   ", "\"a b c"),
            (" \t\n\u{000b}\r\u{000c}a \t\n\u{000b}\r\u{000c} b \t\n\u{000b}\r\u{000c} c \t\n\u{000b}\r\u{000c}", "a b c"),
        ] {
            assert_eq!(sign_v4_trim_all(input), want, "{input:?}");
        }
    }

    // minio-go pkg/s3utils/utils_test.go TestEncodePath + Go url.Values.Encode.
    #[test]
    fn test_encode_path_and_query() {
        for (input, want) in [
            ("thisisthe%url", "thisisthe%25url"),
            ("本語", "%E6%9C%AC%E8%AA%9E"),
            ("本語.1", "%E6%9C%AC%E8%AA%9E.1"),
            (">123", "%3E123"),
            ("myurl#link", "myurl%23link"),
            ("space in url", "space%20in%20url"),
            ("url+path", "url%2Bpath"),
            ("/tenant/01ABC/chunks/000001", "/tenant/01ABC/chunks/000001"),
        ] {
            assert_eq!(encode_path(input), want, "{input}");
        }
        let q = vec![
            ("prefix".to_string(), "t1/a b".to_string()),
            ("list-type".to_string(), "2".to_string()),
            ("delimiter".to_string(), "/".to_string()),
            ("uploads".to_string(), String::new()),
        ];
        assert_eq!(
            canonical_query(&q),
            "delimiter=%2F&list-type=2&prefix=t1%2Fa%20b&uploads="
        );
    }
}
