//! Prometheus exposition formats and content negotiation for `/metrics`, byte-compatible with
//! Go `client_golang` `promhttp.HandlerFor(gatherer, HandlerOpts{EnableOpenMetrics: true})` as
//! registered by dskit `server.registerInstrumentation`.
//!
//! Ported from:
//! - github.com/prometheus/common/expfmt/encode.go (`NegotiateAccept`, `matchFormat`)
//! - github.com/prometheus/common/expfmt/expfmt.go (format strings)
//! - github.com/prometheus/common/expfmt/text_create.go (`MetricFamilyToText`, `writeFloat`)
//! - github.com/prometheus/common/expfmt/openmetrics_create.go (`MetricFamilyToOpenMetrics`)
//! - github.com/munnerz/goautoneg/autoneg.go (`ParseAccept`)
//! - github.com/prometheus/client_golang/prometheus/promhttp/http.go (`negotiateEncodingWriter`)
//! - github.com/prometheus/client_golang/internal/github.com/golang/gddo/httputil
//!   (`NegotiateContentEncoding`)
//!
//! Not supported (never negotiated, so the response falls back like Go does when a format is not
//! offered): the debugging protobuf text formats `encoding=text` / `encoding=compact-text`, and
//! OpenMetrics 2.0 (not offered by Go's `DefaultOpenMetricsAcceptedFormats` either).

use std::collections::HashMap;
use std::io::Write as _;

use prometheus::proto::{self, MetricFamily, MetricType};

pub const FMT_TEXT: &str = "text/plain; version=0.0.4; charset=utf-8";
pub const FMT_PROTO_DELIM: &str =
    "application/vnd.google.protobuf; proto=io.prometheus.client.MetricFamily; encoding=delimited";
pub const FMT_OPENMETRICS_1_0_0: &str = "application/openmetrics-text; version=1.0.0; charset=utf-8";
pub const FMT_OPENMETRICS_0_0_1: &str = "application/openmetrics-text; version=0.0.1; charset=utf-8";

/// `model.NameEscapingScheme` default (`UnderscoreEscaping`).
const DEFAULT_ESCAPING: &str = "underscores";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FormatType {
    Text,
    ProtoDelim,
    OpenMetrics,
}

/// A negotiated exposition format: the exact `Content-Type` value Go would send.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Format {
    pub content_type: String,
    pub kind: FormatType,
}

#[derive(Clone, Debug)]
struct Accept {
    typ: String,
    sub_type: String,
    q: f64,
    params: HashMap<String, String>,
}

fn next_split_element(s: &str, sep: char) -> (&str, &str) {
    match s.find(sep) {
        Some(i) => (&s[..i], &s[i + 1..]),
        None => (s, ""),
    }
}

fn trim_spaces(s: &str) -> &str {
    s.trim_matches(' ')
}

/// goautoneg `ParseAccept`, including its (non-total) ordering comparator applied with the
/// insertion sort Go's `slices.SortFunc` uses for short slices.
fn parse_accept(header: &str) -> Vec<Accept> {
    let mut accept = Vec::new();
    let mut remaining = header;
    while !remaining.is_empty() {
        let (part, rest) = next_split_element(remaining, ',');
        remaining = rest;
        let part = trim_spaces(part);
        let mut a = Accept {
            typ: String::new(),
            sub_type: String::new(),
            q: 1.0,
            params: HashMap::new(),
        };
        let (sp, mut remaining_part) = next_split_element(part, ';');
        let (sp0, sp_remaining) = next_split_element(sp, '/');
        a.typ = trim_spaces(sp0).to_string();
        if sp_remaining.is_empty() {
            if a.typ == "*" {
                a.sub_type = "*".to_string();
            } else {
                continue;
            }
        } else {
            let (sp1, rest) = next_split_element(sp_remaining, '/');
            if !rest.is_empty() {
                continue;
            }
            a.sub_type = trim_spaces(sp1).to_string();
        }
        while !remaining_part.is_empty() {
            let (sp, rest) = next_split_element(remaining_part, ';');
            remaining_part = rest;
            let (sp0, sp_remaining) = next_split_element(sp, '=');
            if sp_remaining.is_empty() {
                continue;
            }
            let (sp1, rest) = next_split_element(sp_remaining, '=');
            if !rest.is_empty() {
                continue;
            }
            let token = trim_spaces(sp0);
            if token == "q" {
                // strconv.ParseFloat(sp1, 32): errors yield 0.
                a.q = sp1.parse::<f32>().map(f64::from).unwrap_or(0.0);
            } else {
                a.params
                    .insert(token.to_string(), trim_spaces(sp1).to_string());
            }
        }
        accept.push(a);
    }
    let cmp_less = |a: &Accept, b: &Accept| -> bool {
        a.q > b.q
            || (a.typ != "*" && b.typ == "*")
            || (a.sub_type != "*" && b.sub_type == "*")
            || a.params.len() > b.params.len()
    };
    for i in 1..accept.len() {
        let mut j = i;
        while j > 0 && cmp_less(&accept[j], &accept[j - 1]) {
            accept.swap(j, j - 1);
            j -= 1;
        }
    }
    accept
}

struct Target {
    format: &'static str,
    kind: FormatType,
    typ: &'static str,
    sub_type: &'static str,
    params: &'static [(&'static str, &'static str)],
}

/// `expfmt.DefaultOpenMetricsAcceptedFormats()` minus the protobuf debugging text formats.
const ACCEPTED: &[Target] = &[
    Target {
        format: FMT_OPENMETRICS_1_0_0,
        kind: FormatType::OpenMetrics,
        typ: "application",
        sub_type: "openmetrics-text",
        params: &[("version", "1.0.0")],
    },
    Target {
        format: FMT_OPENMETRICS_0_0_1,
        kind: FormatType::OpenMetrics,
        typ: "application",
        sub_type: "openmetrics-text",
        params: &[("version", "0.0.1")],
    },
    Target {
        format: FMT_PROTO_DELIM,
        kind: FormatType::ProtoDelim,
        typ: "application",
        sub_type: "vnd.google.protobuf",
        params: &[
            ("proto", "io.prometheus.client.MetricFamily"),
            ("encoding", "delimited"),
        ],
    },
    Target {
        format: FMT_TEXT,
        kind: FormatType::Text,
        typ: "text",
        sub_type: "plain",
        params: &[("version", "0.0.4")],
    },
];

fn match_format(ac: &Accept, t: &Target) -> bool {
    if ac.typ != "*" && ac.typ != t.typ {
        return false;
    }
    if ac.sub_type != "*" && ac.sub_type != t.sub_type {
        return false;
    }
    let mut ac_version = ac.params.get("version").cloned().unwrap_or_default();
    if ac_version.is_empty() && ac.typ == "application" && ac.sub_type == "openmetrics-text" {
        ac_version = "0.0.1".to_string();
    }
    if ac_version.is_empty() && ac.typ == "text" && ac.sub_type == "plain" {
        ac_version = "0.0.4".to_string();
    }
    for (k, v) in t.params {
        let ac_val = if *k == "version" {
            ac_version.as_str()
        } else {
            ac.params.get(*k).map(String::as_str).unwrap_or("")
        };
        if ac_val != *v {
            return false;
        }
    }
    true
}

fn escaping_for(ac: &Accept) -> &str {
    match ac.params.get("escaping").map(String::as_str) {
        Some(p @ ("allow-utf-8" | "underscores" | "dots" | "values")) => p,
        _ => DEFAULT_ESCAPING,
    }
}

/// Go `expfmt.NegotiateAccept(h, DefaultOpenMetricsAcceptedFormats()...)` (or
/// `DefaultAcceptedFormats()` when `enable_open_metrics == false`).
pub fn negotiate_with_options(accept_header: Option<&str>, enable_open_metrics: bool) -> Format {
    let offered = if enable_open_metrics {
        ACCEPTED
    } else {
        &ACCEPTED[2..]
    };
    for ac in parse_accept(accept_header.unwrap_or("")) {
        for t in offered {
            if match_format(&ac, t) {
                return Format {
                    content_type: format!("{}; escaping={}", t.format, escaping_for(&ac)),
                    kind: t.kind,
                };
            }
        }
    }
    Format {
        content_type: format!("{FMT_TEXT}; escaping={DEFAULT_ESCAPING}"),
        kind: FormatType::Text,
    }
}

/// Go `expfmt.NegotiateAccept(h, DefaultOpenMetricsAcceptedFormats()...)`.
pub fn negotiate(accept_header: Option<&str>) -> Format {
    negotiate_with_options(accept_header, true)
}

/// gddo `httputil.NegotiateContentEncoding(r, ["identity", "gzip"])` as used by promhttp.
/// Returns `Some("gzip")`, `Some("identity")`, or `None` when nothing is acceptable (Go's "").
pub fn negotiate_content_encoding(accept_encoding: Option<&str>) -> Option<&'static str> {
    let specs = parse_accept_encoding(accept_encoding.unwrap_or(""));
    let mut best_offer = "identity";
    let mut best_q = -1.0f64;
    for offer in ["identity", "gzip"] {
        for (value, q) in &specs {
            if *q > best_q && (value == "*" || value == offer) {
                best_q = *q;
                best_offer = offer;
            }
        }
    }
    if best_q == 0.0 {
        return None;
    }
    Some(best_offer)
}

fn parse_accept_encoding(h: &str) -> Vec<(String, f64)> {
    let mut out = Vec::new();
    for part in h.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let mut it = part.split(';');
        let value = it.next().unwrap_or("").trim().to_ascii_lowercase();
        if value.is_empty() {
            continue;
        }
        let mut q = 1.0;
        for p in it {
            let p = p.trim();
            if let Some(v) = p.strip_prefix("q=").or_else(|| p.strip_prefix("Q=")) {
                q = v.trim().parse::<f64>().unwrap_or(0.0);
            }
        }
        out.push((value, q));
    }
    out
}

/// Go `strconv.FormatFloat(f, 'g', -1, 64)`.
pub fn format_go_float_g(f: f64) -> String {
    if f.is_nan() {
        return "NaN".to_string();
    }
    if f.is_infinite() {
        return if f > 0.0 { "+Inf".to_string() } else { "-Inf".to_string() };
    }
    if f == 0.0 {
        return if f.is_sign_negative() { "-0".to_string() } else { "0".to_string() };
    }
    // Shortest round-trip digits and decimal exponent from Rust's `{:e}`.
    let e = format!("{:e}", f.abs());
    let (mant, exp) = e.split_once('e').expect("{:e} has an exponent");
    let exp: i32 = exp.parse().expect("valid exponent");
    let digits: String = mant.chars().filter(|c| *c != '.').collect();
    let neg = if f < 0.0 { "-" } else { "" };
    if !(-4..6).contains(&exp) {
        // %e: d.ddddde±XX (at least two exponent digits).
        let mut s = String::from(neg);
        s.push_str(&digits[..1]);
        if digits.len() > 1 {
            s.push('.');
            s.push_str(&digits[1..]);
        }
        s.push('e');
        s.push(if exp < 0 { '-' } else { '+' });
        let a = exp.unsigned_abs();
        if a < 10 {
            s.push('0');
        }
        s.push_str(&a.to_string());
        return s;
    }
    // %f with the minimal number of digits.
    let mut s = String::from(neg);
    if exp < 0 {
        s.push_str("0.");
        for _ in 0..(-exp - 1) {
            s.push('0');
        }
        s.push_str(&digits);
    } else {
        let int_len = (exp + 1) as usize;
        if digits.len() <= int_len {
            s.push_str(&digits);
            for _ in digits.len()..int_len {
                s.push('0');
            }
        } else {
            s.push_str(&digits[..int_len]);
            s.push('.');
            s.push_str(&digits[int_len..]);
        }
    }
    s
}

/// Text format `writeFloat`.
fn text_float(f: f64) -> String {
    if f == 1.0 {
        "1".to_string()
    } else if f == 0.0 {
        "0".to_string()
    } else if f == -1.0 {
        "-1".to_string()
    } else {
        format_go_float_g(f)
    }
}

/// OpenMetrics `writeOpenMetricsFloat`.
fn om_float(f: f64) -> String {
    if f == 1.0 {
        "1.0".to_string()
    } else if f == 0.0 {
        "0.0".to_string()
    } else if f == -1.0 {
        "-1.0".to_string()
    } else if f.is_nan() || f.is_infinite() {
        format_go_float_g(f)
    } else {
        let mut s = format_go_float_g(f);
        if !s.contains('e') && !s.contains('.') {
            s.push_str(".0");
        }
        s
    }
}

fn escape(s: &str, include_double_quote: bool) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '"' if include_double_quote => out.push_str("\\\""),
            _ => out.push(c),
        }
    }
    out
}

fn sorted_labels(m: &proto::Metric) -> Vec<&proto::LabelPair> {
    let mut l: Vec<&proto::LabelPair> = m.get_label().iter().collect();
    l.sort_by(|a, b| a.get_name().cmp(b.get_name()));
    l
}

fn write_name_and_labels(
    out: &mut Vec<u8>,
    name: &str,
    labels: &[&proto::LabelPair],
    extra: Option<(&str, String)>,
) {
    out.extend_from_slice(name.as_bytes());
    if labels.is_empty() && extra.is_none() {
        return;
    }
    let mut sep = b'{';
    for lp in labels {
        out.push(sep);
        out.extend_from_slice(lp.get_name().as_bytes());
        out.extend_from_slice(b"=\"");
        out.extend_from_slice(escape(lp.get_value(), true).as_bytes());
        out.push(b'"');
        sep = b',';
    }
    if let Some((k, v)) = extra {
        out.push(sep);
        out.extend_from_slice(k.as_bytes());
        out.extend_from_slice(b"=\"");
        out.extend_from_slice(v.as_bytes());
        out.push(b'"');
    }
    out.push(b'}');
}

fn text_sample(
    out: &mut Vec<u8>,
    name: &str,
    labels: &[&proto::LabelPair],
    extra: Option<(&str, String)>,
    value: f64,
    ts: Option<i64>,
) {
    write_name_and_labels(out, name, labels, extra);
    out.push(b' ');
    out.extend_from_slice(text_float(value).as_bytes());
    if let Some(ts) = ts {
        let _ = write!(out, " {ts}");
    }
    out.push(b'\n');
}

/// Go `expfmt.MetricFamilyToText` for every family (families must be sorted by the gatherer).
pub fn encode_text(families: &[MetricFamily]) -> Vec<u8> {
    let mut out = Vec::with_capacity(64 * 1024);
    for mf in families {
        let name = mf.get_name();
        if mf.get_metric().is_empty() {
            continue;
        }
        out.extend_from_slice(b"# HELP ");
        out.extend_from_slice(name.as_bytes());
        out.push(b' ');
        out.extend_from_slice(escape(mf.get_help(), false).as_bytes());
        out.push(b'\n');
        let typ = match mf.get_field_type() {
            MetricType::COUNTER => "counter",
            MetricType::GAUGE => "gauge",
            MetricType::SUMMARY => "summary",
            MetricType::UNTYPED => "untyped",
            MetricType::HISTOGRAM => "histogram",
        };
        let _ = writeln!(out, "# TYPE {name} {typ}");
        for m in mf.get_metric() {
            let labels = sorted_labels(m);
            let ts = m.has_timestamp_ms().then(|| m.get_timestamp_ms());
            match mf.get_field_type() {
                MetricType::COUNTER => {
                    text_sample(&mut out, name, &labels, None, m.get_counter().get_value(), ts)
                }
                MetricType::GAUGE => {
                    text_sample(&mut out, name, &labels, None, m.get_gauge().get_value(), ts)
                }
                MetricType::UNTYPED => {
                    text_sample(&mut out, name, &labels, None, m.get_untyped().get_value(), ts)
                }
                MetricType::SUMMARY => {
                    let s = m.get_summary();
                    for q in s.get_quantile() {
                        text_sample(
                            &mut out,
                            name,
                            &labels,
                            Some(("quantile", text_float(q.get_quantile()))),
                            q.get_value(),
                            ts,
                        );
                    }
                    text_sample(&mut out, &format!("{name}_sum"), &labels, None, s.get_sample_sum(), ts);
                    text_sample(
                        &mut out,
                        &format!("{name}_count"),
                        &labels,
                        None,
                        s.get_sample_count() as f64,
                        ts,
                    );
                }
                MetricType::HISTOGRAM => {
                    let h = m.get_histogram();
                    let bucket_name = format!("{name}_bucket");
                    let mut inf_seen = false;
                    for b in h.get_bucket() {
                        text_sample(
                            &mut out,
                            &bucket_name,
                            &labels,
                            Some(("le", text_float(b.get_upper_bound()))),
                            b.get_cumulative_count() as f64,
                            ts,
                        );
                        if b.get_upper_bound().is_infinite() && b.get_upper_bound() > 0.0 {
                            inf_seen = true;
                        }
                    }
                    if !inf_seen {
                        text_sample(
                            &mut out,
                            &bucket_name,
                            &labels,
                            Some(("le", "+Inf".to_string())),
                            h.get_sample_count() as f64,
                            ts,
                        );
                    }
                    text_sample(&mut out, &format!("{name}_sum"), &labels, None, h.get_sample_sum(), ts);
                    text_sample(
                        &mut out,
                        &format!("{name}_count"),
                        &labels,
                        None,
                        h.get_sample_count() as f64,
                        ts,
                    );
                }
            }
        }
    }
    out
}

enum OmValue {
    Float(f64),
    Uint(u64),
}

fn om_sample(
    out: &mut Vec<u8>,
    name: &str,
    labels: &[&proto::LabelPair],
    extra: Option<(&str, String)>,
    value: OmValue,
    ts: Option<i64>,
) {
    write_name_and_labels(out, name, labels, extra);
    out.push(b' ');
    match value {
        OmValue::Float(f) => out.extend_from_slice(om_float(f).as_bytes()),
        OmValue::Uint(u) => {
            let _ = write!(out, "{u}");
        }
    }
    if let Some(ts) = ts {
        out.push(b' ');
        out.extend_from_slice(om_float(ts as f64 / 1000.0).as_bytes());
    }
    out.push(b'\n');
}

/// Go `expfmt.MetricFamilyToOpenMetrics` for every family plus `FinalizeOpenMetrics` (`# EOF`).
/// `_created` lines are only emitted with `EnableOpenMetricsTextCreatedSamples`, for which the
/// `prometheus` crate does not record created timestamps on counters/summaries; histograms from
/// [`mimir_common::native_histogram`] carry one.
pub fn encode_openmetrics(families: &[MetricFamily]) -> Vec<u8> {
    let mut out = Vec::with_capacity(64 * 1024);
    for mf in families {
        let name = mf.get_name();
        if mf.get_metric().is_empty() {
            continue;
        }
        let typ = mf.get_field_type();
        let mut compliant = name.to_string();
        if typ == MetricType::COUNTER && name.ends_with("_total") {
            compliant.truncate(name.len() - 6);
        }
        out.extend_from_slice(b"# HELP ");
        out.extend_from_slice(compliant.as_bytes());
        out.push(b' ');
        out.extend_from_slice(escape(mf.get_help(), true).as_bytes());
        out.push(b'\n');
        let t = match typ {
            MetricType::COUNTER if name.ends_with("_total") => "counter",
            MetricType::COUNTER | MetricType::UNTYPED => "unknown",
            MetricType::GAUGE => "gauge",
            MetricType::SUMMARY => "summary",
            MetricType::HISTOGRAM => "histogram",
        };
        let _ = writeln!(out, "# TYPE {compliant} {t}");
        if typ == MetricType::COUNTER && name.ends_with("_total") {
            compliant.push_str("_total");
        }
        for m in mf.get_metric() {
            let labels = sorted_labels(m);
            let ts = m.has_timestamp_ms().then(|| m.get_timestamp_ms());
            match typ {
                MetricType::COUNTER => om_sample(
                    &mut out,
                    &compliant,
                    &labels,
                    None,
                    OmValue::Float(m.get_counter().get_value()),
                    ts,
                ),
                MetricType::GAUGE => om_sample(
                    &mut out,
                    &compliant,
                    &labels,
                    None,
                    OmValue::Float(m.get_gauge().get_value()),
                    ts,
                ),
                MetricType::UNTYPED => om_sample(
                    &mut out,
                    &compliant,
                    &labels,
                    None,
                    OmValue::Float(m.get_untyped().get_value()),
                    ts,
                ),
                MetricType::SUMMARY => {
                    let s = m.get_summary();
                    for q in s.get_quantile() {
                        om_sample(
                            &mut out,
                            &compliant,
                            &labels,
                            Some(("quantile", om_float(q.get_quantile()))),
                            OmValue::Float(q.get_value()),
                            ts,
                        );
                    }
                    om_sample(
                        &mut out,
                        &format!("{compliant}_sum"),
                        &labels,
                        None,
                        OmValue::Float(s.get_sample_sum()),
                        ts,
                    );
                    om_sample(
                        &mut out,
                        &format!("{compliant}_count"),
                        &labels,
                        None,
                        OmValue::Uint(s.get_sample_count()),
                        ts,
                    );
                }
                MetricType::HISTOGRAM => {
                    let h = m.get_histogram();
                    let bucket_name = format!("{compliant}_bucket");
                    let mut inf_seen = false;
                    for b in h.get_bucket() {
                        om_sample(
                            &mut out,
                            &bucket_name,
                            &labels,
                            Some(("le", om_float(b.get_upper_bound()))),
                            OmValue::Uint(b.get_cumulative_count()),
                            ts,
                        );
                        if b.get_upper_bound().is_infinite() && b.get_upper_bound() > 0.0 {
                            inf_seen = true;
                        }
                    }
                    if !inf_seen {
                        om_sample(
                            &mut out,
                            &bucket_name,
                            &labels,
                            Some(("le", "+Inf".to_string())),
                            OmValue::Uint(h.get_sample_count()),
                            ts,
                        );
                    }
                    om_sample(
                        &mut out,
                        &format!("{compliant}_sum"),
                        &labels,
                        None,
                        OmValue::Float(h.get_sample_sum()),
                        ts,
                    );
                    om_sample(
                        &mut out,
                        &format!("{compliant}_count"),
                        &labels,
                        None,
                        OmValue::Uint(h.get_sample_count()),
                        ts,
                    );
                }
            }
        }
    }
    out.extend_from_slice(b"# EOF\n");
    out
}

/// Length-delimited `io.prometheus.client.MetricFamily` messages (`protodelim.MarshalTo`).
/// Native histogram fields attached as unknown fields by
/// [`mimir_common::native_histogram::HistogramVec`] are serialized verbatim.
pub fn encode_protobuf(families: &[MetricFamily]) -> Result<Vec<u8>, prometheus::Error> {
    use prometheus::Encoder as _;
    let mut out = Vec::with_capacity(64 * 1024);
    prometheus::ProtobufEncoder::new().encode(families, &mut out)?;
    Ok(out)
}

/// Encodes `families` in the negotiated format.
pub fn encode(families: &[MetricFamily], format: &Format) -> Result<Vec<u8>, prometheus::Error> {
    Ok(match format.kind {
        FormatType::Text => encode_text(families),
        FormatType::OpenMetrics => encode_openmetrics(families),
        FormatType::ProtoDelim => encode_protobuf(families)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn go_float_formatting() {
        for (f, want) in [
            (2621440.0, "2.62144e+06"),
            (1794871684.6342194, "1.7948716846342194e+09"),
            (1073741824.0, "1.073741824e+09"),
            (0.005, "0.005"),
            (2.5, "2.5"),
            (100.0, "100"),
            (123456.0, "123456"),
            (1e6, "1e+06"),
            (1e-5, "1e-05"),
            (0.0001, "0.0001"),
            (-12.5, "-12.5"),
            (1e21, "1e+21"),
            (5e-324, "5e-324"),
            (f64::INFINITY, "+Inf"),
            (f64::NEG_INFINITY, "-Inf"),
        ] {
            assert_eq!(format_go_float_g(f), want, "{f}");
        }
        assert_eq!(om_float(4.0), "4.0");
        assert_eq!(om_float(1e6), "1e+06");
        assert_eq!(om_float(0.25), "0.25");
    }

    #[test]
    fn negotiation_matches_go() {
        let n = |h: &str| negotiate(Some(h)).content_type;
        assert_eq!(negotiate(None).content_type, "text/plain; version=0.0.4; charset=utf-8; escaping=underscores");
        // Prometheus 3 default scrape protocols (no native histograms).
        assert_eq!(
            n("application/openmetrics-text;version=1.0.0;escaping=allow-utf-8;q=0.5,application/openmetrics-text;version=0.0.1;q=0.4,text/plain;version=1.0.0;escaping=allow-utf-8;q=0.3,text/plain;version=0.0.4;q=0.2,*/*;q=0.1"),
            "application/openmetrics-text; version=1.0.0; charset=utf-8; escaping=allow-utf-8"
        );
        // Prometheus with native histograms enabled.
        assert_eq!(
            n("application/vnd.google.protobuf;proto=io.prometheus.client.MetricFamily;encoding=delimited;q=0.7,text/plain;version=0.0.4;q=0.3"),
            "application/vnd.google.protobuf; proto=io.prometheus.client.MetricFamily; encoding=delimited; escaping=underscores"
        );
        assert_eq!(n("application/openmetrics-text"), "application/openmetrics-text; version=0.0.1; charset=utf-8; escaping=underscores");
        assert_eq!(n("*/*"), "text/plain; version=0.0.4; charset=utf-8; escaping=underscores");
        assert_eq!(n("text/html"), "text/plain; version=0.0.4; charset=utf-8; escaping=underscores");
        assert_eq!(
            negotiate_with_options(Some("application/openmetrics-text;version=1.0.0,*/*;q=0.1"), false).content_type,
            "text/plain; version=0.0.4; charset=utf-8; escaping=underscores"
        );
        assert_eq!(
            negotiate_with_options(Some("application/vnd.google.protobuf;proto=io.prometheus.client.MetricFamily;encoding=delimited"), false).content_type,
            "application/vnd.google.protobuf; proto=io.prometheus.client.MetricFamily; encoding=delimited; escaping=underscores"
        );
    }

    #[test]
    fn content_encoding_negotiation_matches_gddo() {
        assert_eq!(negotiate_content_encoding(Some("gzip")), Some("gzip"));
        assert_eq!(negotiate_content_encoding(None), Some("identity"));
        assert_eq!(negotiate_content_encoding(Some("gzip, identity")), Some("identity"));
        assert_eq!(negotiate_content_encoding(Some("identity;q=0.5, gzip")), Some("gzip"));
        assert_eq!(negotiate_content_encoding(Some("*;q=0")), None);
        assert_eq!(negotiate_content_encoding(Some("br")), Some("identity"));
    }

    fn sample_families() -> Vec<MetricFamily> {
        let reg = prometheus::Registry::new();
        let c = prometheus::IntCounterVec::new(prometheus::Opts::new("x_total", "help \\ \"q\"\nl"), &["b", "a"]).unwrap();
        c.with_label_values(&["2", "1"]).inc_by(3);
        reg.register(Box::new(c)).unwrap();
        let h = prometheus::Histogram::with_opts(
            prometheus::HistogramOpts::new("y_seconds", "h").buckets(vec![0.5, 1e6]),
        )
        .unwrap();
        h.observe(0.25);
        reg.register(Box::new(h)).unwrap();
        reg.gather()
    }

    #[test]
    fn text_encoding_matches_go() {
        let got = String::from_utf8(encode_text(&sample_families())).unwrap();
        assert_eq!(
            got,
            "# HELP x_total help \\\\ \"q\"\\nl\n# TYPE x_total counter\nx_total{a=\"1\",b=\"2\"} 3\n\
             # HELP y_seconds h\n# TYPE y_seconds histogram\ny_seconds_bucket{le=\"0.5\"} 1\ny_seconds_bucket{le=\"1e+06\"} 1\n\
             y_seconds_bucket{le=\"+Inf\"} 1\ny_seconds_sum 0.25\ny_seconds_count 1\n"
        );
    }

    #[test]
    fn openmetrics_encoding_matches_go() {
        let got = String::from_utf8(encode_openmetrics(&sample_families())).unwrap();
        assert_eq!(
            got,
            "# HELP x help \\\\ \\\"q\\\"\\nl\n# TYPE x counter\nx_total{a=\"1\",b=\"2\"} 3.0\n\
             # HELP y_seconds h\n# TYPE y_seconds histogram\ny_seconds_bucket{le=\"0.5\"} 1\ny_seconds_bucket{le=\"1e+06\"} 1\n\
             y_seconds_bucket{le=\"+Inf\"} 1\ny_seconds_sum 0.25\ny_seconds_count 1\n# EOF\n"
        );
    }
}
