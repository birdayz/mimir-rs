//! Multi-tenant ID validation, parsing, normalization, metadata, and HTTP/gRPC header extraction.
//!
//! Ported from:
//! - github.com/grafana/dskit/tenant/tenant.go
//! - github.com/grafana/dskit/tenant/resolver.go
//! - github.com/grafana/dskit/tenant/metadata.go
//! - github.com/grafana/dskit/user/id.go
//! - github.com/grafana/dskit/user/http.go
//! - github.com/grafana/dskit/user/grpc.go

use std::fmt;

use hyper::header::HeaderValue;
use hyper::HeaderMap;
use thiserror::Error;
use tonic::metadata::{MetadataMap, MetadataValue};

/// Max length of a single tenant ID in bytes.
pub const MAX_TENANT_ID_LENGTH: usize = 150;

/// Max length of tenant metadata in bytes.
pub const MAX_METADATA_LENGTH: usize = 64;

/// Separator used when multiple tenant IDs are concatenated.
pub const TENANT_IDS_SEPARATOR: char = '|';

/// Separator between tenant ID and metadata (e.g. `123456:product=k6`).
pub const METADATA_SEPARATOR: char = ':';

/// Separator between key and value in metadata (e.g. `source=test`).
pub const METADATA_KV_SEPARATOR: char = '=';

/// HTTP header carrying the tenant / org ID.
pub const ORG_ID_HEADER_NAME: &str = "X-Scope-OrgID";

/// HTTP header carrying the user ID.
pub const USER_ID_HEADER_NAME: &str = "X-Scope-UserID";

/// Lowercase header/metadata name used in HTTP/2 and gRPC.
pub const LOWER_ORG_ID_HEADER_NAME: &str = "x-scope-orgid";

const fn build_valid_tenant_id_chars() -> [bool; 256] {
    let mut table = [false; 256];
    let mut c = b'a';
    while c <= b'z' {
        table[c as usize] = true;
        c += 1;
    }
    let mut c = b'A';
    while c <= b'Z' {
        table[c as usize] = true;
        c += 1;
    }
    let mut c = b'0';
    while c <= b'9' {
        table[c as usize] = true;
        c += 1;
    }
    let specials = b"!-_.*'()";
    let mut i = 0;
    while i < specials.len() {
        table[specials[i] as usize] = true;
        i += 1;
    }
    table
}

const fn build_valid_metadata_chars() -> [bool; 256] {
    let mut table = [false; 256];
    table[METADATA_SEPARATOR as usize] = true;
    table[METADATA_KV_SEPARATOR as usize] = true;
    let mut c = b'a';
    while c <= b'z' {
        table[c as usize] = true;
        c += 1;
    }
    let mut c = b'A';
    while c <= b'Z' {
        table[c as usize] = true;
        c += 1;
    }
    let mut c = b'0';
    while c <= b'9' {
        table[c as usize] = true;
        c += 1;
    }
    table[b'-' as usize] = true;
    table[b'_' as usize] = true;
    table
}

/// 256-entry ASCII lookup table for valid tenant ID characters (`a-zA-Z0-9!-_.*'()`).
pub static VALID_TENANT_ID_CHARS: [bool; 256] = build_valid_tenant_id_chars();

/// 256-entry ASCII lookup table for valid tenant metadata characters (`:=a-zA-Z0-9-_`).
pub static VALID_METADATA_CHARS: [bool; 256] = build_valid_metadata_chars();

/// Errors returned by tenant / user ID validation, parsing, and header extraction.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TenantError {
    #[error("no org id")]
    NoOrgID,

    #[error("different org ID already present")]
    DifferentOrgIDPresent,

    #[error("multiple org IDs present")]
    TooManyOrgIDs,

    #[error("no user id")]
    NoUserID,

    #[error("different user ID already present")]
    DifferentUserIDPresent,

    #[error("tenant ID '{tenant_id}' contains unsupported character '{ch}'")]
    UnsupportedCharacter { tenant_id: String, ch: char },

    #[error("tenant ID is too long: max {MAX_TENANT_ID_LENGTH} characters")]
    TooLong,

    #[error("tenant ID is '.' or '..'")]
    UnsafeTenantId,

    #[error("metadata '{metadata}' contains unsupported character '{ch}'")]
    MetadataUnsupportedCharacter { metadata: String, ch: char },

    #[error("metadata too long: {len}")]
    MetadataTooLong { len: usize },

    #[error("malformed tenant metadata: {raw:?} ({reason})")]
    MalformedMetadata { raw: String, reason: String },
}

/// Validates a single tenant ID against dskit's exact rules:
/// - All bytes must be in `VALID_TENANT_ID_CHARS` (`a-zA-Z0-9!-_.*'()`)
/// - Length <= `MAX_TENANT_ID_LENGTH` (150)
/// - Not `"."` or `".."`
pub fn valid_tenant_id(s: &str) -> Result<(), TenantError> {
    for &b in s.as_bytes() {
        if !VALID_TENANT_ID_CHARS[b as usize] {
            return Err(TenantError::UnsupportedCharacter {
                tenant_id: s.to_string(),
                ch: char::from(b),
            });
        }
    }
    if s.len() > MAX_TENANT_ID_LENGTH {
        return Err(TenantError::TooLong);
    }
    if s == "." || s == ".." {
        return Err(TenantError::UnsafeTenantId);
    }
    Ok(())
}

/// Validates tenant metadata characters and length.
pub fn valid_metadata(s: &str) -> Result<(), TenantError> {
    for &b in s.as_bytes() {
        if !VALID_METADATA_CHARS[b as usize] {
            return Err(TenantError::MetadataUnsupportedCharacter {
                metadata: s.to_string(),
                ch: char::from(b),
            });
        }
    }
    if s.len() > MAX_METADATA_LENGTH {
        return Err(TenantError::MetadataTooLong { len: s.len() });
    }
    Ok(())
}

/// Removes metadata from an orgID without validating the input.
pub fn trim_metadata(org_id: &str) -> &str {
    match org_id.find(METADATA_SEPARATOR) {
        Some(idx) => &org_id[..idx],
        None => org_id,
    }
}

/// Splits an orgID into `(tenant_id, metadata_with_leading_colon)`.
pub fn split_tenant_and_metadata(org_id: &str) -> (&str, &str) {
    match org_id.find(METADATA_SEPARATOR) {
        Some(idx) => (&org_id[..idx], &org_id[idx..]),
        None => (org_id, ""),
    }
}

/// Validated, colon-delimited, lexicographically sorted tenant ID metadata.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct Metadata {
    source: String,
}

impl Metadata {
    /// Wraps an already-validated metadata string (e.g. `:key=value`).
    pub fn new(source: impl Into<String>) -> Self {
        Self {
            source: source.into(),
        }
    }

    /// Validates and constructs a [`Metadata`].
    pub fn parse(source: &str) -> Result<Self, TenantError> {
        valid_metadata(source)?;
        if source.is_empty() {
            return Ok(Self::default());
        }

        if !source.starts_with(METADATA_SEPARATOR) {
            return Err(TenantError::MalformedMetadata {
                raw: source.to_string(),
                reason: format!("missing '{METADATA_SEPARATOR}' at start"),
            });
        }

        let mut remaining = source;
        let mut prev_key: Option<&str> = None;
        while !remaining.is_empty() {
            let next = match remaining[1..].find(METADATA_SEPARATOR) {
                Some(idx) => idx + 1,
                None => remaining.len(),
            };
            let kv = &remaining[..next];
            remaining = &remaining[next..];

            let Some((key, _val)) = kv[1..].split_once(METADATA_KV_SEPARATOR) else {
                return Err(TenantError::MalformedMetadata {
                    raw: source.to_string(),
                    reason: format!("no key-value separator '{METADATA_KV_SEPARATOR}' in {kv:?}"),
                });
            };
            if let Some(prev) = prev_key {
                if prev >= key {
                    return Err(TenantError::MalformedMetadata {
                        raw: source.to_string(),
                        reason: "keys must be unique and sorted".to_string(),
                    });
                }
            }
            prev_key = Some(key);
        }

        Ok(Self {
            source: source.to_string(),
        })
    }

    pub fn encode(&self) -> &str {
        &self.source
    }

    pub fn is_empty(&self) -> bool {
        self.source.is_empty()
    }

    pub fn iter(&self) -> MetadataIter<'_> {
        MetadataIter {
            remaining: &self.source,
        }
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.iter().find_map(|(k, v)| (k == key).then_some(v))
    }

    pub fn has(&self, key: &str) -> bool {
        self.get(key).is_some()
    }

    pub fn set(&mut self, key: &str, val: &str) {
        let mut out = String::with_capacity(self.source.len() + key.len() + val.len() + 2);
        let mut inserted = false;
        for (prev_key, prev_val) in self.iter() {
            if inserted {
                write_metadata_kv(&mut out, prev_key, prev_val);
                continue;
            }
            match prev_key.cmp(key) {
                std::cmp::Ordering::Less => write_metadata_kv(&mut out, prev_key, prev_val),
                std::cmp::Ordering::Equal => {
                    write_metadata_kv(&mut out, key, val);
                    inserted = true;
                }
                std::cmp::Ordering::Greater => {
                    write_metadata_kv(&mut out, key, val);
                    write_metadata_kv(&mut out, prev_key, prev_val);
                    inserted = true;
                }
            }
        }
        if !inserted {
            write_metadata_kv(&mut out, key, val);
        }
        self.source = out;
    }

    pub fn with(mut self, key: &str, val: &str) -> Self {
        self.set(key, val);
        self
    }

    pub fn with_tenant(&self, tenant_id: &str) -> String {
        format!("{tenant_id}{}", self.source)
    }
}

fn write_metadata_kv(into: &mut String, key: &str, val: &str) {
    into.push(METADATA_SEPARATOR);
    into.push_str(key);
    into.push(METADATA_KV_SEPARATOR);
    into.push_str(val);
}

impl fmt::Display for Metadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.source)
    }
}

pub struct MetadataIter<'a> {
    remaining: &'a str,
}

impl<'a> Iterator for MetadataIter<'a> {
    type Item = (&'a str, &'a str);

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining.is_empty() {
            return None;
        }
        let next = match self.remaining[1..].find(METADATA_SEPARATOR) {
            Some(idx) => idx + 1,
            None => self.remaining.len(),
        };
        let kv = &self.remaining[1..next];
        self.remaining = &self.remaining[next..];
        kv.split_once(METADATA_KV_SEPARATOR)
    }
}

/// Normalizes a list of tenant IDs by sorting lexicographically and de-duplicating.
pub fn normalize_tenant_ids(mut tenant_ids: Vec<String>) -> Vec<String> {
    tenant_ids.sort();
    tenant_ids.dedup();
    tenant_ids
}

/// Concatenates tenant IDs with `'|'`.
pub fn join_tenant_ids<S: AsRef<str>>(tenant_ids: &[S]) -> String {
    let mut out = String::new();
    for (i, id) in tenant_ids.iter().enumerate() {
        if i > 0 {
            out.push(TENANT_IDS_SEPARATOR);
        }
        out.push_str(id.as_ref());
    }
    out
}

/// Extracts, validates, and normalizes all `|`-separated tenant IDs from an orgID string
/// (stripping any `:key=value` metadata from each part).
pub fn parse_tenant_ids(org_id: &str) -> Result<Vec<String>, TenantError> {
    let mut ids = Vec::new();
    for part in org_id.split(TENANT_IDS_SEPARATOR) {
        let tenant_id = trim_metadata(part);
        valid_tenant_id(tenant_id)?;
        ids.push(tenant_id.to_string());
    }
    Ok(normalize_tenant_ids(ids))
}

/// Extracts and validates a single tenant ID from an orgID string (stripping metadata).
/// Multiple identical tenant IDs separated by `'|'` (e.g. `a|a` or `a:k=v|a`) are allowed,
/// while differing tenant IDs return [`TenantError::TooManyOrgIDs`].
pub fn parse_single_tenant_id(org_id: &str) -> Result<String, TenantError> {
    let mut parts = org_id.split(TENANT_IDS_SEPARATOR);
    let first = parts.next().unwrap_or("");
    let tenant_id = trim_metadata(first);
    valid_tenant_id(tenant_id)?;
    for part in parts {
        if tenant_id != trim_metadata(part) {
            return Err(TenantError::TooManyOrgIDs);
        }
    }
    Ok(tenant_id.to_string())
}

/// Extracts a single tenant ID and optional metadata from an orgID string.
pub fn parse_with_metadata(org_ids: &str) -> Result<(String, Metadata), TenantError> {
    let mut parts = org_ids.split(TENANT_IDS_SEPARATOR);
    let first = parts.next().unwrap_or("");
    let (tenant_id, metadata_str) = split_tenant_and_metadata(first);
    valid_tenant_id(tenant_id)?;
    for next_org_id in parts {
        if first != next_org_id {
            return Err(TenantError::TooManyOrgIDs);
        }
    }
    let metadata = Metadata::parse(metadata_str)?;
    Ok((tenant_id.to_string(), metadata))
}

/// Tenant resolver interface matching `dskit/tenant.Resolver`.
pub trait Resolver: Send + Sync {
    fn tenant_id(&self, org_id: Option<&str>) -> Result<String, TenantError>;
    fn tenant_ids(&self, org_id: Option<&str>) -> Result<Vec<String>, TenantError>;
}

/// Multi-tenant resolver allowing `|`-separated tenant IDs.
#[derive(Debug, Default, Clone, Copy)]
pub struct MultiResolver;

impl MultiResolver {
    pub fn new() -> Self {
        Self
    }
}

impl Resolver for MultiResolver {
    fn tenant_id(&self, org_id: Option<&str>) -> Result<String, TenantError> {
        let org_id = org_id.ok_or(TenantError::NoOrgID)?;
        parse_single_tenant_id(org_id)
    }

    fn tenant_ids(&self, org_id: Option<&str>) -> Result<Vec<String>, TenantError> {
        let org_id = org_id.ok_or(TenantError::NoOrgID)?;
        parse_tenant_ids(org_id)
    }
}

/// Extracts the org ID from an arbitrary sequence of `(header_name, header_value)` pairs
/// using case-insensitive lookup for `X-Scope-OrgID`.
///
/// Rejects missing/empty headers with [`TenantError::NoOrgID`] and multiple inconsistent
/// header values with [`TenantError::TooManyOrgIDs`].
pub fn extract_org_id_from_headers<'a, I>(headers: I) -> Result<String, TenantError>
where
    I: IntoIterator<Item = (&'a str, &'a str)>,
{
    let mut found: Option<&'a str> = None;
    for (name, value) in headers {
        if name.eq_ignore_ascii_case(ORG_ID_HEADER_NAME) {
            if value.is_empty() {
                return Err(TenantError::NoOrgID);
            }
            match found {
                None => found = Some(value),
                Some(prev) if prev == value => {}
                Some(_) => return Err(TenantError::TooManyOrgIDs),
            }
        }
    }
    found.map(|s| s.to_string()).ok_or(TenantError::NoOrgID)
}

/// Extracts the org ID from a [`hyper::HeaderMap`] (case-insensitive `X-Scope-OrgID`).
pub fn extract_org_id_from_http_headers(headers: &HeaderMap) -> Result<String, TenantError> {
    let mut found: Option<&str> = None;
    for value in headers.get_all(LOWER_ORG_ID_HEADER_NAME) {
        let val_str = value.to_str().map_err(|_| TenantError::NoOrgID)?;
        if val_str.is_empty() {
            return Err(TenantError::NoOrgID);
        }
        match found {
            None => found = Some(val_str),
            Some(prev) if prev == val_str => {}
            Some(_) => return Err(TenantError::TooManyOrgIDs),
        }
    }
    found.map(|s| s.to_string()).ok_or(TenantError::NoOrgID)
}

/// Extracts a validated single tenant ID directly from HTTP headers.
pub fn extract_tenant_id_from_http_headers(headers: &HeaderMap) -> Result<String, TenantError> {
    let org_id = extract_org_id_from_http_headers(headers)?;
    parse_single_tenant_id(&org_id)
}

/// Extracts validated, normalized tenant IDs from HTTP headers.
pub fn extract_tenant_ids_from_http_headers(
    headers: &HeaderMap,
) -> Result<Vec<String>, TenantError> {
    let org_id = extract_org_id_from_http_headers(headers)?;
    parse_tenant_ids(&org_id)
}

/// Injects `org_id` into HTTP headers under `X-Scope-OrgID`, returning
/// [`TenantError::DifferentOrgIDPresent`] if a different org ID is already set.
pub fn inject_org_id_into_http_headers(
    headers: &mut HeaderMap,
    org_id: &str,
) -> Result<(), TenantError> {
    if let Some(existing) = headers.get(LOWER_ORG_ID_HEADER_NAME) {
        let existing_str = existing.to_str().unwrap_or("");
        if !existing_str.is_empty() && existing_str != org_id {
            return Err(TenantError::DifferentOrgIDPresent);
        }
    }
    let header_val = HeaderValue::from_str(org_id).map_err(|_| TenantError::UnsupportedCharacter {
        tenant_id: org_id.to_string(),
        ch: '\0',
    })?;
    headers.insert(LOWER_ORG_ID_HEADER_NAME, header_val);
    Ok(())
}

/// Extracts the org ID from gRPC incoming metadata (`x-scope-orgid`), like dskit
/// `user.ExtractFromGRPCRequest`: anything but exactly one metadata value (missing or repeated,
/// even with identical values) is [`TenantError::NoOrgID`].
///
/// Known divergence: an empty value is rejected with [`TenantError::NoOrgID`] too, whereas Go
/// accepts the empty tenant ID (and would place its TSDB in the TSDB root directory).
pub fn extract_org_id_from_grpc_metadata(metadata: &MetadataMap) -> Result<String, TenantError> {
    let mut values = metadata.get_all(LOWER_ORG_ID_HEADER_NAME).iter();
    let (Some(value), None) = (values.next(), values.next()) else {
        return Err(TenantError::NoOrgID);
    };
    match value.to_str() {
        Ok(s) if !s.is_empty() => Ok(s.to_string()),
        _ => Err(TenantError::NoOrgID),
    }
}

/// Extracts a validated single tenant ID from gRPC incoming metadata.
pub fn extract_tenant_id_from_grpc_metadata(metadata: &MetadataMap) -> Result<String, TenantError> {
    let org_id = extract_org_id_from_grpc_metadata(metadata)?;
    parse_single_tenant_id(&org_id)
}

/// Extracts validated, normalized tenant IDs from gRPC incoming metadata.
pub fn extract_tenant_ids_from_grpc_metadata(
    metadata: &MetadataMap,
) -> Result<Vec<String>, TenantError> {
    let org_id = extract_org_id_from_grpc_metadata(metadata)?;
    parse_tenant_ids(&org_id)
}

/// Injects `org_id` into gRPC outgoing metadata under `x-scope-orgid`, matching `dskit/user`.
pub fn inject_org_id_into_grpc_metadata(
    metadata: &mut MetadataMap,
    org_id: &str,
) -> Result<(), TenantError> {
    let existing: Vec<_> = metadata.get_all(LOWER_ORG_ID_HEADER_NAME).iter().collect();
    if !existing.is_empty() {
        if existing.len() == 1 {
            let existing_str = existing[0].to_str().unwrap_or("");
            if existing_str != org_id {
                return Err(TenantError::DifferentOrgIDPresent);
            }
            return Ok(());
        }
        return Err(TenantError::TooManyOrgIDs);
    }
    let val: MetadataValue<_> = org_id
        .parse()
        .map_err(|_| TenantError::UnsupportedCharacter {
            tenant_id: org_id.to_string(),
            ch: '\0',
        })?;
    metadata.insert(LOWER_ORG_ID_HEADER_NAME, val);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_tenant_id() {
        assert!(valid_tenant_id("tenant-1").is_ok());
        assert!(valid_tenant_id("a_b.c*d'e(f)g!0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ").is_ok());
        assert!(valid_tenant_id("").is_ok());

        let max_len = "a".repeat(MAX_TENANT_ID_LENGTH);
        assert!(valid_tenant_id(&max_len).is_ok());

        let too_long = "a".repeat(MAX_TENANT_ID_LENGTH + 1);
        assert_eq!(
            valid_tenant_id(&too_long).unwrap_err().to_string(),
            "tenant ID is too long: max 150 characters"
        );

        assert_eq!(
            valid_tenant_id(".").unwrap_err().to_string(),
            "tenant ID is '.' or '..'"
        );
        assert_eq!(
            valid_tenant_id("..").unwrap_err().to_string(),
            "tenant ID is '.' or '..'"
        );
        assert!(valid_tenant_id("...").is_ok());
        assert!(valid_tenant_id("a..b").is_ok());

        assert_eq!(
            valid_tenant_id("foo/bar").unwrap_err().to_string(),
            "tenant ID 'foo/bar' contains unsupported character '/'"
        );
        assert_eq!(
            valid_tenant_id("foo|bar").unwrap_err().to_string(),
            "tenant ID 'foo|bar' contains unsupported character '|'"
        );
        assert_eq!(
            valid_tenant_id("foo:bar").unwrap_err().to_string(),
            "tenant ID 'foo:bar' contains unsupported character ':'"
        );
        assert_eq!(
            valid_tenant_id("foo bar").unwrap_err().to_string(),
            "tenant ID 'foo bar' contains unsupported character ' '"
        );
    }

    #[test]
    fn test_normalize_and_parse_tenant_ids() {
        let normalized = normalize_tenant_ids(vec![
            "b".to_string(),
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
        ]);
        assert_eq!(normalized, vec!["a", "b", "c"]);
        assert_eq!(join_tenant_ids(&normalized), "a|b|c");

        assert_eq!(
            parse_tenant_ids("b|a:k=v|b|c").unwrap(),
            vec!["a", "b", "c"]
        );
        assert!(parse_tenant_ids("a|..").is_err());

        assert_eq!(parse_single_tenant_id("a|a:k=v").unwrap(), "a");
        assert_eq!(
            parse_single_tenant_id("a|b").unwrap_err(),
            TenantError::TooManyOrgIDs
        );

        let (tid, meta) = parse_with_metadata("123456:a=b:c=d").unwrap();
        assert_eq!(tid, "123456");
        assert_eq!(meta.get("a"), Some("b"));
        assert_eq!(meta.get("c"), Some("d"));
        assert_eq!(
            parse_with_metadata("a:k=v|a:k=v2").unwrap_err(),
            TenantError::TooManyOrgIDs
        );
    }

    #[test]
    fn test_http_and_grpc_extraction() {
        let mut headers = HeaderMap::new();
        assert_eq!(
            extract_org_id_from_http_headers(&headers).unwrap_err(),
            TenantError::NoOrgID
        );

        headers.insert("x-scope-orgid", "tenant-1".parse().unwrap());
        assert_eq!(
            extract_org_id_from_http_headers(&headers).unwrap(),
            "tenant-1"
        );

        headers.append("X-Scope-OrgID", "tenant-1".parse().unwrap());
        assert_eq!(
            extract_org_id_from_http_headers(&headers).unwrap(),
            "tenant-1"
        );

        headers.append("X-Scope-OrgID", "tenant-2".parse().unwrap());
        assert_eq!(
            extract_org_id_from_http_headers(&headers).unwrap_err(),
            TenantError::TooManyOrgIDs
        );

        assert_eq!(
            extract_org_id_from_headers([
                ("X-Scope-OrgID", "t1"),
                ("x-scope-orgid", "t2"),
            ])
            .unwrap_err(),
            TenantError::TooManyOrgIDs
        );
        assert_eq!(
            extract_org_id_from_headers([("X-SCOPE-ORGID", "t1")]).unwrap(),
            "t1"
        );

        let mut md = MetadataMap::new();
        assert_eq!(
            extract_org_id_from_grpc_metadata(&md).unwrap_err(),
            TenantError::NoOrgID
        );
        inject_org_id_into_grpc_metadata(&mut md, "tenant-a").unwrap();
        assert_eq!(
            extract_org_id_from_grpc_metadata(&md).unwrap(),
            "tenant-a"
        );
        assert_eq!(
            inject_org_id_into_grpc_metadata(&mut md, "tenant-b").unwrap_err(),
            TenantError::DifferentOrgIDPresent
        );
        md.append("x-scope-orgid", "tenant-b".parse().unwrap());
        assert_eq!(
            extract_org_id_from_grpc_metadata(&md).unwrap_err(),
            TenantError::NoOrgID
        );
        // dskit: exactly one value, even repeated identical values are "no org id".
        let mut md = MetadataMap::new();
        md.append("x-scope-orgid", "tenant-a".parse().unwrap());
        md.append("x-scope-orgid", "tenant-a".parse().unwrap());
        assert_eq!(
            extract_org_id_from_grpc_metadata(&md).unwrap_err(),
            TenantError::NoOrgID
        );
    }
}
