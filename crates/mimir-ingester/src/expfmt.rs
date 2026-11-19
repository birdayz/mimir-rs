//! Prometheus exposition formats and content negotiation for `/metrics`, byte-compatible with
//! Go `client_golang` `promhttp.HandlerFor(gatherer, HandlerOpts{EnableOpenMetrics: true})` as
//! registered by dskit `server.registerInstrumentation`.
//!
//! Ported from:
//! - github.com/prometheus/common/expfmt/encode.go (`NegotiateAccept`, `matchFormat`)
//! - github.com/prometheus/common/expfmt/expfmt.go (format strings, `Format.ToEscapingScheme`)
//! - github.com/prometheus/common/model/metric.go (`EscapeMetricFamily`, `EscapeName`,
//!   `LegacyValidation.IsValidMetricName`)
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

use std::borrow::Cow;
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

/// Go `model.EscapingScheme`, selected by the negotiated `escaping` content-type parameter.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EscapingScheme {
    /// `allow-utf-8` (`model.NoEscaping`): names are emitted verbatim, quoted when not legacy-valid.
    NoEscaping,
    /// `underscores` (`model.UnderscoreEscaping`, the default `model.NameEscapingScheme`).
    Underscores,
    /// `dots` (`model.DotsEscaping`).
    Dots,
    /// `values` (`model.ValueEncodingEscaping`).
    Values,
}

impl Format {
    /// Go `expfmt.Format.ToEscapingScheme`.
    pub fn escaping_scheme(&self) -> EscapingScheme {
        for p in self.content_type.split(';') {
            let toks: Vec<&str> = p.split('=').collect();
            if toks.len() != 2 {
                continue;
            }
            if toks[0].trim() == "escaping" {
                return match toks[1].trim() {
                    "allow-utf-8" => EscapingScheme::NoEscaping,
                    "dots" => EscapingScheme::Dots,
                    "values" => EscapingScheme::Values,
                    // "underscores", and `model.NameEscapingScheme` for anything unknown.
                    _ => EscapingScheme::Underscores,
                };
            }
        }
        EscapingScheme::Underscores
    }
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

/// Go `model.isValidLegacyRune` (`i` is the byte offset of the rune, like Go's `range`).
fn is_valid_legacy_rune(c: char, i: usize) -> bool {
    c.is_ascii_alphabetic() || c == '_' || c == ':' || (c.is_ascii_digit() && i > 0)
}

/// Legacy Prometheus metric name check (`^[a-zA-Z_:][a-zA-Z0-9_:]*$`, Go `model.LegacyValidation.IsValidMetricName`).
pub fn is_valid_legacy_metric_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    if bytes.is_empty() {
        return false;
    }
    let first = bytes[0];
    if !(first.is_ascii_alphabetic() || first == b'_' || first == b':') {
        return false;
    }
    bytes[1..]
        .iter()
        .all(|&b| b.is_ascii_alphanumeric() || b == b'_' || b == b':')
}

/// Go `model.EscapeName`.
pub fn escape_name(name: &str, scheme: EscapingScheme) -> Cow<'_, str> {
    if name.is_empty() {
        return Cow::Borrowed(name);
    }
    match scheme {
        EscapingScheme::NoEscaping => Cow::Borrowed(name),
        EscapingScheme::Underscores => {
            if is_valid_legacy_metric_name(name) {
                return Cow::Borrowed(name);
            }
            let mut escaped = String::with_capacity(name.len());
            for (i, c) in name.char_indices() {
                escaped.push(if is_valid_legacy_rune(c, i) { c } else { '_' });
            }
            Cow::Owned(escaped)
        }
        EscapingScheme::Dots => {
            // No early return for legacy-valid names: underscores are still escaped.
            let mut escaped = String::with_capacity(name.len() + 8);
            for (i, c) in name.char_indices() {
                match c {
                    '_' => escaped.push_str("__"),
                    '.' => escaped.push_str("_dot_"),
                    c if is_valid_legacy_rune(c, i) => escaped.push(c),
                    _ => escaped.push_str("__"),
                }
            }
            Cow::Owned(escaped)
        }
        EscapingScheme::Values => {
            if is_valid_legacy_metric_name(name) {
                return Cow::Borrowed(name);
            }
            let mut escaped = String::with_capacity(name.len() + 16);
            escaped.push_str("U__");
            for (i, c) in name.char_indices() {
                match c {
                    '_' => escaped.push_str("__"),
                    c if is_valid_legacy_rune(c, i) => escaped.push(c),
                    // Go's `!utf8.ValidRune` branch (`_FFFD_`) is unreachable: a `str` only holds
                    // valid scalar values.
                    c => escaped.push_str(&format!("_{:x}_", c as u32)),
                }
            }
            Cow::Owned(escaped)
        }
    }
}

/// The metric family name after Go `model.EscapeMetricFamily`: legacy-valid names are kept as-is
/// under every scheme.
pub fn escape_family_name(name: &str, scheme: EscapingScheme) -> Cow<'_, str> {
    if scheme == EscapingScheme::NoEscaping || is_valid_legacy_metric_name(name) {
        Cow::Borrowed(name)
    } else {
        escape_name(name, scheme)
    }
}

/// One label after Go `model.EscapeMetricFamily`: the value of `__name__` and every other label
/// name are escaped unless they are legacy-valid metric names.
fn escape_label_pair<'a>(
    name: &'a str,
    value: &'a str,
    scheme: EscapingScheme,
) -> (Cow<'a, str>, Cow<'a, str>) {
    if scheme == EscapingScheme::NoEscaping {
        return (Cow::Borrowed(name), Cow::Borrowed(value));
    }
    if name == "__name__" {
        if is_valid_legacy_metric_name(value) {
            return (Cow::Borrowed(name), Cow::Borrowed(value));
        }
        return (Cow::Borrowed(name), escape_name(value, scheme));
    }
    if is_valid_legacy_metric_name(name) {
        return (Cow::Borrowed(name), Cow::Borrowed(value));
    }
    (escape_name(name, scheme), Cow::Borrowed(value))
}

/// Go `model.metricNeedsEscaping` (an empty name or value escapes to itself).
fn label_needs_escaping(l: &proto::LabelPair) -> bool {
    if l.get_name() == "__name__" {
        return !l.get_value().is_empty() && !is_valid_legacy_metric_name(l.get_value());
    }
    !l.get_name().is_empty() && !is_valid_legacy_metric_name(l.get_name())
}

/// Go `model.EscapeMetricFamily`: borrows `mf` when nothing needs escaping.
pub fn escape_metric_family(mf: &MetricFamily, scheme: EscapingScheme) -> Cow<'_, MetricFamily> {
    if scheme == EscapingScheme::NoEscaping {
        return Cow::Borrowed(mf);
    }
    let name_needs_escaping =
        !mf.get_name().is_empty() && !is_valid_legacy_metric_name(mf.get_name());
    let labels_need_escaping = mf
        .get_metric()
        .iter()
        .any(|m| m.get_label().iter().any(label_needs_escaping));
    if !name_needs_escaping && !labels_need_escaping {
        return Cow::Borrowed(mf);
    }
    let mut out = mf.clone();
    if name_needs_escaping {
        out.set_name(escape_name(mf.get_name(), scheme).into_owned());
    }
    for m in out.mut_metric().iter_mut() {
        for l in m.mut_label().iter_mut() {
            if !label_needs_escaping(l) {
                continue;
            }
            if l.get_name() == "__name__" {
                let v = escape_name(l.get_value(), scheme).into_owned();
                l.set_value(v);
            } else {
                let n = escape_name(l.get_name(), scheme).into_owned();
                l.set_name(n);
            }
        }
    }
    Cow::Owned(out)
}

/// The labels of `m` in exposition order (sorted by their original name, like client_golang
/// gathers them), escaped per `scheme` like Go `model.EscapeMetricFamily` does before encoding.
pub fn escaped_sorted_labels(
    m: &proto::Metric,
    scheme: EscapingScheme,
) -> Vec<(Cow<'_, str>, Cow<'_, str>)> {
    let mut l: Vec<&proto::LabelPair> = m.get_label().iter().collect();
    l.sort_by(|a, b| a.get_name().cmp(b.get_name()));
    l.into_iter()
        .map(|lp| escape_label_pair(lp.get_name(), lp.get_value(), scheme))
        .collect()
}

/// Go `expfmt.writeName`, used for metric names and label names alike: as-is when the name
/// passes the legacy *metric* name check (so `:` never forces quoting), else double-quoted and
/// escaped.
pub fn write_metric_name(out: &mut Vec<u8>, name: &str) {
    if is_valid_legacy_metric_name(name) {
        out.extend_from_slice(name.as_bytes());
    } else {
        out.push(b'"');
        out.extend_from_slice(escape(name, true).as_bytes());
        out.push(b'"');
    }
}

/// Go `expfmt.writeNameAndLabelPairs` / `writeOpenMetricsNameAndLabelPairs`. `extra_value` is
/// the already formatted `le` / `quantile` value.
pub fn write_name_and_label_pairs(
    out: &mut Vec<u8>,
    name: &str,
    labels: &[(Cow<'_, str>, Cow<'_, str>)],
    extra: Option<(&str, &str)>,
) {
    let mut sep = b'{';
    let mut metric_inside_braces = false;
    if !name.is_empty() {
        // A name failing the legacy validity check goes inside the braces, quoted.
        if !is_valid_legacy_metric_name(name) {
            metric_inside_braces = true;
            out.push(sep);
            sep = b',';
        }
        write_metric_name(out, name);
    }
    if labels.is_empty() && extra.is_none() {
        if metric_inside_braces {
            out.push(b'}');
        }
        return;
    }
    for (k, v) in labels {
        out.push(sep);
        write_metric_name(out, k);
        out.extend_from_slice(b"=\"");
        out.extend_from_slice(escape(v, true).as_bytes());
        out.push(b'"');
        sep = b',';
    }
    if let Some((k, v)) = extra {
        out.push(sep);
        // Go writes the additional label name with a plain `WriteString`.
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
    labels: &[(Cow<'_, str>, Cow<'_, str>)],
    extra: Option<(&str, String)>,
    value: f64,
    ts: Option<i64>,
) {
    write_name_and_label_pairs(out, name, labels, extra.as_ref().map(|(k, v)| (*k, v.as_str())));
    out.push(b' ');
    out.extend_from_slice(text_float(value).as_bytes());
    if let Some(ts) = ts {
        let _ = write!(out, " {ts}");
    }
    out.push(b'\n');
}

/// Go `expfmt.MetricFamilyToText` for every family (families must be sorted by the gatherer),
/// without name escaping (`escaping=allow-utf-8`).
pub fn encode_text(families: &[MetricFamily]) -> Vec<u8> {
    encode_text_with_escaping(families, EscapingScheme::NoEscaping)
}

/// Go `expfmt.NewEncoder` for the text format:
/// `MetricFamilyToText(w, model.EscapeMetricFamily(v, scheme))` for every family.
pub fn encode_text_with_escaping(families: &[MetricFamily], scheme: EscapingScheme) -> Vec<u8> {
    let mut out = Vec::with_capacity(64 * 1024);
    for mf in families {
        let name = escape_family_name(mf.get_name(), scheme);
        let name: &str = &name;
        if mf.get_metric().is_empty() {
            continue;
        }
        out.extend_from_slice(b"# HELP ");
        write_metric_name(&mut out, name);
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
        out.extend_from_slice(b"# TYPE ");
        write_metric_name(&mut out, name);
        out.push(b' ');
        out.extend_from_slice(typ.as_bytes());
        out.push(b'\n');
        for m in mf.get_metric() {
            let labels = escaped_sorted_labels(m, scheme);
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
    labels: &[(Cow<'_, str>, Cow<'_, str>)],
    extra: Option<(&str, String)>,
    value: OmValue,
    ts: Option<i64>,
) {
    write_name_and_label_pairs(out, name, labels, extra.as_ref().map(|(k, v)| (*k, v.as_str())));
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
/// [`mimir_common::native_histogram`] carry one. No name escaping (`escaping=allow-utf-8`).
pub fn encode_openmetrics(families: &[MetricFamily]) -> Vec<u8> {
    encode_openmetrics_with_escaping(families, EscapingScheme::NoEscaping)
}

/// Go `expfmt.NewEncoder` for OpenMetrics 0.0.1 / 1.0.0:
/// `MetricFamilyToOpenMetrics(w, model.EscapeMetricFamily(v, scheme))` for every family.
pub fn encode_openmetrics_with_escaping(
    families: &[MetricFamily],
    scheme: EscapingScheme,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(64 * 1024);
    for mf in families {
        let name = escape_family_name(mf.get_name(), scheme);
        let name: &str = &name;
        if mf.get_metric().is_empty() {
            continue;
        }
        let typ = mf.get_field_type();
        let mut compliant = name.to_string();
        if typ == MetricType::COUNTER && name.ends_with("_total") {
            compliant.truncate(name.len() - 6);
        }
        out.extend_from_slice(b"# HELP ");
        write_metric_name(&mut out, &compliant);
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
        out.extend_from_slice(b"# TYPE ");
        write_metric_name(&mut out, &compliant);
        out.push(b' ');
        out.extend_from_slice(t.as_bytes());
        out.push(b'\n');
        if typ == MetricType::COUNTER && name.ends_with("_total") {
            compliant.push_str("_total");
        }
        for m in mf.get_metric() {
            let labels = escaped_sorted_labels(m, scheme);
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

/// Encodes `families` in the negotiated format (Go `expfmt.NewEncoder`): names are escaped with
/// the format's escaping scheme before encoding.
pub fn encode(families: &[MetricFamily], format: &Format) -> Result<Vec<u8>, prometheus::Error> {
    let scheme = format.escaping_scheme();
    Ok(match format.kind {
        FormatType::Text => encode_text_with_escaping(families, scheme),
        FormatType::OpenMetrics => encode_openmetrics_with_escaping(families, scheme),
        FormatType::ProtoDelim => {
            let escaped: Vec<Cow<'_, MetricFamily>> = families
                .iter()
                .map(|mf| escape_metric_family(mf, scheme))
                .collect();
            if escaped.iter().all(|mf| matches!(mf, Cow::Borrowed(_))) {
                encode_protobuf(families)?
            } else {
                let owned: Vec<MetricFamily> = escaped.into_iter().map(Cow::into_owned).collect();
                encode_protobuf(&owned)?
            }
        }
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

    #[test]
    fn utf8_metric_and_label_names_quoted_per_go_expfmt() {
        assert!(is_valid_legacy_metric_name("a:b_1"));
        assert!(!is_valid_legacy_metric_name("a.b"));
        assert!(!is_valid_legacy_metric_name("1abc"));

        let mut mf = MetricFamily::default();
        mf.set_name("metric.name".to_string());
        mf.set_help("h".to_string());
        mf.set_field_type(MetricType::GAUGE);
        let mut lp1 = proto::LabelPair::default();
        lp1.set_name("k8s.namespace.name".to_string());
        lp1.set_value("prod".to_string());
        let mut lp2 = proto::LabelPair::default();
        lp2.set_name("label".to_string());
        lp2.set_value("val".to_string());
        let mut m = proto::Metric::default();
        m.set_label(vec![lp1, lp2].into());
        let mut g = proto::Gauge::default();
        g.set_value(1.0);
        m.set_gauge(g);
        mf.mut_metric().push(m);

        let text = String::from_utf8(encode_text(std::slice::from_ref(&mf))).unwrap();
        assert_eq!(
            text,
            "# HELP \"metric.name\" h\n# TYPE \"metric.name\" gauge\n{\"metric.name\",\"k8s.namespace.name\"=\"prod\",label=\"val\"} 1\n"
        );
    }

    /// prometheus/common `model/metric_test.go` `TestEscapeName`.
    #[test]
    fn escape_name_matches_upstream_vectors() {
        // (input, underscores, dots, values)
        let cases = [
            ("", "", "", ""),
            (
                "no:escaping_required",
                "no:escaping_required",
                "no:escaping__required",
                "no:escaping_required",
            ),
            (
                "mysystem.prod.west.cpu.load",
                "mysystem_prod_west_cpu_load",
                "mysystem_dot_prod_dot_west_dot_cpu_dot_load",
                "U__mysystem_2e_prod_2e_west_2e_cpu_2e_load",
            ),
            (
                "mysystem.prod.west.cpu.load_total",
                "mysystem_prod_west_cpu_load_total",
                "mysystem_dot_prod_dot_west_dot_cpu_dot_load__total",
                "U__mysystem_2e_prod_2e_west_2e_cpu_2e_load__total",
            ),
            (
                "http.status:sum",
                "http_status:sum",
                "http_dot_status:sum",
                "U__http_2e_status:sum",
            ),
            (
                "label with 😱",
                "label_with__",
                "label__with____",
                "U__label_20_with_20__1f631_",
            ),
            ("花火", "__", "____", "U___82b1__706b_"),
            (
                "label with \u{100}",
                "label_with__",
                "label__with____",
                "U__label_20_with_20__100_",
            ),
        ];
        for (input, underscores, dots, values) in cases {
            assert_eq!(escape_name(input, EscapingScheme::NoEscaping), input);
            assert_eq!(escape_name(input, EscapingScheme::Underscores), underscores, "{input}");
            assert_eq!(escape_name(input, EscapingScheme::Dots), dots, "{input}");
            assert_eq!(escape_name(input, EscapingScheme::Values), values, "{input}");
        }
    }

    fn utf8_family() -> MetricFamily {
        let mut mf = MetricFamily::default();
        mf.set_name("http.requests_total".to_string());
        mf.set_help("h".to_string());
        mf.set_field_type(MetricType::COUNTER);
        let lp = |n: &str, v: &str| {
            let mut lp = proto::LabelPair::default();
            lp.set_name(n.to_string());
            lp.set_value(v.to_string());
            lp
        };
        let mut m = proto::Metric::default();
        // Sorted by original name, as gathered: '.' (0x2e) < '_' (0x5f) < 'a'.
        m.set_label(
            vec![
                lp("__name__", "other.name"),
                lp("a.b", "1"),
                lp("a_a", "2"),
                lp("team:name", "obs"),
            ]
            .into(),
        );
        let mut c = proto::Counter::default();
        c.set_value(3.0);
        m.set_counter(c);
        mf.mut_metric().push(m);
        mf
    }

    /// Go `expfmt.NewEncoder` escapes with the negotiated scheme (`model.EscapeMetricFamily`)
    /// before encoding; only `escaping=allow-utf-8` emits quoted UTF-8 names. `writeName` checks
    /// label names with the legacy *metric* name rule, so `team:name` is never quoted.
    #[test]
    fn encode_escapes_names_with_negotiated_scheme() {
        let fams = [utf8_family()];
        let text = |accept: Option<&str>| {
            let format = negotiate(accept);
            (
                format.content_type.clone(),
                String::from_utf8(encode(&fams, &format).unwrap()).unwrap(),
            )
        };

        let (ct, body) = text(None);
        assert_eq!(ct, "text/plain; version=0.0.4; charset=utf-8; escaping=underscores");
        assert_eq!(
            body,
            "# HELP http_requests_total h\n# TYPE http_requests_total counter\n\
             http_requests_total{__name__=\"other_name\",a_b=\"1\",a_a=\"2\",team:name=\"obs\"} 3\n"
        );

        let (ct, body) = text(Some("text/plain;version=0.0.4;escaping=allow-utf-8"));
        assert_eq!(ct, "text/plain; version=0.0.4; charset=utf-8; escaping=allow-utf-8");
        assert_eq!(
            body,
            "# HELP \"http.requests_total\" h\n# TYPE \"http.requests_total\" counter\n\
             {\"http.requests_total\",__name__=\"other.name\",\"a.b\"=\"1\",a_a=\"2\",team:name=\"obs\"} 3\n"
        );

        let (_, body) = text(Some("text/plain;version=0.0.4;escaping=dots"));
        assert_eq!(
            body,
            "# HELP http_dot_requests__total h\n# TYPE http_dot_requests__total counter\n\
             http_dot_requests__total{__name__=\"other_dot_name\",a_dot_b=\"1\",a_a=\"2\",team:name=\"obs\"} 3\n"
        );

        let (_, body) = text(Some("text/plain;version=0.0.4;escaping=values"));
        assert_eq!(
            body,
            "# HELP U__http_2e_requests__total h\n# TYPE U__http_2e_requests__total counter\n\
             U__http_2e_requests__total{__name__=\"U__other_2e_name\",U__a_2e_b=\"1\",a_a=\"2\",team:name=\"obs\"} 3\n"
        );

        // OpenMetrics: the `_total` handling runs on the escaped family name.
        let (ct, body) = text(Some("application/openmetrics-text;version=1.0.0"));
        assert_eq!(ct, "application/openmetrics-text; version=1.0.0; charset=utf-8; escaping=underscores");
        assert_eq!(
            body,
            "# HELP http_requests h\n# TYPE http_requests counter\n\
             http_requests_total{__name__=\"other_name\",a_b=\"1\",a_a=\"2\",team:name=\"obs\"} 3.0\n# EOF\n"
        );

        // Protobuf: the family is escaped, legacy-only families are passed through untouched.
        let escaped = escape_metric_family(&fams[0], EscapingScheme::Underscores);
        assert!(matches!(escaped, Cow::Owned(_)));
        assert_eq!(escaped.get_name(), "http_requests_total");
        let names: Vec<(&str, &str)> = escaped.get_metric()[0]
            .get_label()
            .iter()
            .map(|l| (l.get_name(), l.get_value()))
            .collect();
        assert_eq!(
            names,
            vec![("__name__", "other_name"), ("a_b", "1"), ("a_a", "2"), ("team:name", "obs")]
        );
        assert!(matches!(
            escape_metric_family(&fams[0], EscapingScheme::NoEscaping),
            Cow::Borrowed(_)
        ));
        for mf in sample_families() {
            for scheme in [EscapingScheme::Underscores, EscapingScheme::Dots, EscapingScheme::Values] {
                assert!(matches!(escape_metric_family(&mf, scheme), Cow::Borrowed(_)));
            }
        }
        let format = negotiate(Some(
            "application/vnd.google.protobuf;proto=io.prometheus.client.MetricFamily;encoding=delimited",
        ));
        let escaped_fams = [escaped.into_owned()];
        assert_eq!(
            encode(&fams, &format).unwrap(),
            encode_protobuf(&escaped_fams).unwrap()
        );
    }
}
