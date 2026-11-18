//! Mimir's per-route response compression.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/util/gziphandler/gzip.go (`GzipHandler`,
//!   `GzipResponseWriter`, `requestAcceptance`, `parseEncodings`, `parseCoding`,
//!   `DefaultMinSize` = 1400): `Vary: Accept-Encoding` on every response, compression only when
//!   the client accepts gzip, the response is not already encoded and it reaches the minimum size
//!   (or the client rejects the identity encoding); a missing `Content-Type` is sniffed.
//! - github.com/grafana/mimir/pkg/api/api.go (`newRoute`: `GzipHandler(handler,
//!   -http.response-compression-level)` on every route registered with `gzipEnabled = true`).
//!
//! The handler is applied as a route layer of the API router, so it also covers requests that
//! arrive over `httpgrpc.HTTP/Handle`, like Go's per-route wrapping. Routes Mimir registers
//! without gzip are listed in [`NOT_GZIPPED_ROUTES`].

use std::collections::HashMap;
use std::io::Write as _;

use axum::body::Body;
use axum::extract::{MatchedPath, Request, State};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use http::{header, HeaderMap, HeaderValue, StatusCode};
use http_body_util::BodyExt;

/// gziphandler `DefaultMinSize`.
const DEFAULT_MIN_SIZE: usize = 1400;

/// Route templates (relative to `-server.path-prefix`) that Go serves without the gzip handler:
/// `/ready` (`Server.HTTP.Path`), `/metrics` (promhttp negotiates its own compression),
/// `/debug/pprof` (`http.DefaultServeMux`) and `/ingester/unregister-on-shutdown`
/// (`RegisterRoute(..., gzipEnabled=false, ...)`).
pub const NOT_GZIPPED_ROUTES: &[&str] = &["/ready", "/metrics", "/ingester/unregister-on-shutdown"];

/// `-http.response-compression-level` (`gzip.DefaultCompression` = -1, or 1..=9).
#[derive(Clone, Debug)]
pub struct GzipConfig {
    pub level: i64,
    pub cost_attribution_path: Option<String>,
}

impl GzipConfig {
    fn compression(self) -> flate2::Compression {
        match u32::try_from(self.level) {
            Ok(l @ 1..=9) => flate2::Compression::new(l),
            // gzip.DefaultCompression is level 6, like flate2's default.
            _ => flate2::Compression::default(),
        }
    }
}

fn route_is_gzipped(path: &str, cost_attribution_path: Option<&str>) -> bool {
    if path.contains("/debug/pprof") || NOT_GZIPPED_ROUTES.iter().any(|r| path.ends_with(r)) {
        return false;
    }
    if let Some(ca) = cost_attribution_path {
        if path == ca || path.ends_with(ca) {
            return false;
        }
    }
    true
}

/// gziphandler `parseCoding`: the q-value of the last parameter wins (each parameter resets it
/// to 1), clamped to [0, 1]; an unparsable q-value drops the coding.
fn parse_coding(s: &str) -> Option<(String, f64)> {
    let mut coding = String::new();
    let mut qvalue = 1.0;
    let mut failed = false;
    for (n, part) in s.split(';').enumerate() {
        let part = part.trim();
        qvalue = 1.0;
        if n == 0 {
            coding = part.to_lowercase();
        } else if let Some(q) = part.strip_prefix("q=") {
            match q.parse::<f64>() {
                Ok(v) => qvalue = v.clamp(0.0, 1.0),
                Err(_) => {
                    // strconv.ParseFloat returns 0 alongside the error.
                    qvalue = 0.0;
                    failed = true;
                }
            }
        }
    }
    (!coding.is_empty() && !failed).then_some((coding, qvalue))
}

/// gziphandler `requestAcceptance`: `(acceptsGzip, rejectsIdentity)`, from the first
/// `Accept-Encoding` header (Go `Header.Get`).
fn request_acceptance(headers: &HeaderMap) -> (bool, bool) {
    let value = headers
        .get(header::ACCEPT_ENCODING)
        .map(|v| String::from_utf8_lossy(v.as_bytes()).into_owned())
        .unwrap_or_default();
    let codings: HashMap<String, f64> = value.split(',').filter_map(parse_coding).collect();
    let identity = codings.get("identity");
    let wildcard = codings.get("*");
    let rejects_identity = identity == Some(&0.0) || (identity.is_none() && wildcard == Some(&0.0));
    let gzip = codings.get("gzip");
    let accepts_gzip = gzip.is_some_and(|q| *q > 0.0) || (gzip.is_none() && wildcard.is_some_and(|q| *q > 0.0));
    (accepts_gzip, rejects_identity)
}

/// Go `http.DetectContentType` for the bodies admin handlers produce.
fn detect_content_type(data: &[u8]) -> &'static str {
    let data = &data[..data.len().min(512)];
    let trimmed = {
        let start = data
            .iter()
            .position(|b| !matches!(b, b'\t' | b'\n' | b'\x0c' | b'\r' | b' '))
            .unwrap_or(data.len());
        &data[start..]
    };
    const HTML_SIGS: &[&[u8]] = &[
        b"<!DOCTYPE HTML",
        b"<HTML",
        b"<HEAD",
        b"<SCRIPT",
        b"<IFRAME",
        b"<H1",
        b"<DIV",
        b"<FONT",
        b"<TABLE",
        b"<A",
        b"<STYLE",
        b"<TITLE",
        b"<B",
        b"<BODY",
        b"<BR",
        b"<P",
        b"<!--",
    ];
    for sig in HTML_SIGS {
        if trimmed.len() > sig.len()
            && trimmed[..sig.len()].eq_ignore_ascii_case(sig)
            && matches!(trimmed[sig.len()], b' ' | b'>')
        {
            return "text/html; charset=utf-8";
        }
    }
    if trimmed.starts_with(b"<?xml") {
        return "text/xml; charset=utf-8";
    }
    if data.starts_with(&[0x1f, 0x8b, 0x08]) {
        return "application/x-gzip";
    }
    // Go's textSig: no "binary data bytes".
    if data
        .iter()
        .any(|b| matches!(b, 0x00..=0x08 | 0x0b | 0x0e..=0x1a | 0x1c..=0x1f))
    {
        return "application/octet-stream";
    }
    "text/plain; charset=utf-8"
}

/// Route layer: Mimir `gziphandler.GzipHandler(handler, level)`.
pub async fn gzip_handler(State(cfg): State<GzipConfig>, req: Request, next: Next) -> Response {
    let gzipped_route = req
        .extensions()
        .get::<MatchedPath>()
        .is_some_and(|p| route_is_gzipped(p.as_str(), cfg.cost_attribution_path.as_deref()));
    if !gzipped_route {
        return next.run(req).await;
    }
    let (accepts_gzip, rejects_identity) = request_acceptance(req.headers());
    let mut resp = next.run(req).await;
    if !resp.headers().contains_key(header::VARY) {
        resp.headers_mut()
            .append(header::VARY, HeaderValue::from_static("Accept-Encoding"));
    }
    if !accepts_gzip || resp.headers().contains_key(header::CONTENT_ENCODING) {
        return resp;
    }
    let content_length = resp
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(0);
    if content_length > 0 && content_length < DEFAULT_MIN_SIZE && !rejects_identity {
        return resp;
    }

    let (mut parts, body) = resp.into_parts();
    let data = match body.collect().await {
        Ok(c) => c.to_bytes(),
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    // Nothing written, or below the minimum size without a known larger length: plain.
    if data.is_empty() || (data.len() < DEFAULT_MIN_SIZE && content_length == 0 && !rejects_identity) {
        return Response::from_parts(parts, Body::from(data));
    }

    let mut enc = flate2::write::GzEncoder::new(Vec::with_capacity(data.len() / 4 + 64), cfg.compression());
    let gz = match enc.write_all(&data).and_then(|()| enc.finish()) {
        Ok(gz) => gz,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    if !parts.headers.contains_key(header::CONTENT_TYPE) {
        parts
            .headers
            .insert(header::CONTENT_TYPE, HeaderValue::from_static(detect_content_type(&data)));
    }
    parts
        .headers
        .insert(header::CONTENT_ENCODING, HeaderValue::from_static("gzip"));
    parts.headers.remove(header::CONTENT_LENGTH);
    Response::from_parts(parts, Body::from(gz))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn acceptance(v: Option<&str>) -> (bool, bool) {
        let mut h = HeaderMap::new();
        if let Some(v) = v {
            h.insert(header::ACCEPT_ENCODING, v.parse().unwrap());
        }
        request_acceptance(&h)
    }

    #[test]
    fn request_acceptance_like_gziphandler() {
        assert_eq!(acceptance(None), (false, false));
        assert_eq!(acceptance(Some("gzip")), (true, false));
        assert_eq!(acceptance(Some("gzip, deflate, br")), (true, false));
        assert_eq!(acceptance(Some("*")), (true, false));
        assert_eq!(acceptance(Some("*;q=0")), (false, true));
        assert_eq!(acceptance(Some("gzip;q=0")), (false, false));
        assert_eq!(acceptance(Some("identity;q=0, gzip")), (true, true));
        assert_eq!(acceptance(Some("gzip;q=0;foo")), (true, false));
        assert_eq!(acceptance(Some("gzip;q=x")), (false, false));
        assert_eq!(acceptance(Some("GZIP;q=2")), (true, false));
    }

    #[test]
    fn excluded_routes() {
        assert!(route_is_gzipped("/config", None));
        assert!(route_is_gzipped("/ingester/ring", None));
        assert!(!route_is_gzipped("/ready", None));
        assert!(!route_is_gzipped("/prefix/metrics", None));
        assert!(!route_is_gzipped("/debug/pprof/profile", None));
        assert!(!route_is_gzipped("/ingester/unregister-on-shutdown", None));
        assert!(!route_is_gzipped("/usage-metrics", Some("/usage-metrics")));
    }

    #[test]
    fn sniffing() {
        assert_eq!(detect_content_type(b"  <html><body>"), "text/html; charset=utf-8");
        assert_eq!(detect_content_type(b"{\"a\":1}"), "text/plain; charset=utf-8");
        assert_eq!(detect_content_type(b"\x00\x01"), "application/octet-stream");
    }
}
