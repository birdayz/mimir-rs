//! Go value formatting used by the memberlist status page (`/memberlist`).
//!
//! Ported from:
//! - Go `encoding/json` (`Encoder.Encode` with the default `SetEscapeHTML(true)`, `SetIndent`,
//!   `time.Time.MarshalJSON` = RFC3339Nano, `[]byte` = standard base64, `net.IP.MarshalText`)
//! - Go `fmt` `%#v` for the integer / slice / map shapes of the gogo-generated ring protos
//! - Go `net.IP.String`, `net.JoinHostPort`
//!
//! Struct field order is part of Go's output, so values are built as ordered [`GoJson`] trees
//! rather than through serde (whose map ordering differs from Go's sorted map keys).

use std::fmt::Write as _;

use base64::Engine as _;
use chrono::{DateTime, Datelike, FixedOffset, Timelike};

/// A JSON value in the exact shape Go's `encoding/json` produces it.
#[derive(Clone, Debug, PartialEq)]
pub enum GoJson {
    Null,
    Bool(bool),
    Int(i128),
    Str(String),
    Array(Vec<GoJson>),
    /// Struct fields (declaration order) or map entries (already sorted like Go).
    Object(Vec<(String, GoJson)>),
}

impl GoJson {
    pub fn str(s: impl Into<String>) -> Self {
        GoJson::Str(s.into())
    }

    pub fn int(v: impl Into<i128>) -> Self {
        GoJson::Int(v.into())
    }

    /// Go `[]byte`: `null` when nil, standard padded base64 otherwise.
    pub fn bytes(b: Option<&[u8]>) -> Self {
        match b {
            None => GoJson::Null,
            Some(b) => GoJson::Str(base64::engine::general_purpose::STANDARD.encode(b)),
        }
    }

    /// Go `[]string`: `null` when nil (empty is treated as nil, like a decoded/appended Go slice).
    pub fn strings(v: &[String]) -> Self {
        if v.is_empty() {
            GoJson::Null
        } else {
            GoJson::Array(v.iter().map(|s| GoJson::Str(s.clone())).collect())
        }
    }

    /// Go `time.Time` (`MarshalJSON` = RFC3339Nano).
    pub fn time(t: &DateTime<FixedOffset>) -> Self {
        GoJson::Str(format_rfc3339_nano(t))
    }

    /// Go's map encoding: keys are sorted by their string form.
    pub fn sorted_map(mut entries: Vec<(String, GoJson)>) -> Self {
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        GoJson::Object(entries)
    }

    /// `json.NewEncoder(w).Encode(v)`: compact, HTML-escaped, trailing newline.
    pub fn encode(&self) -> String {
        let mut out = String::new();
        self.write(&mut out, None, 0);
        out.push('\n');
        out
    }

    /// `enc.SetIndent("", indent); enc.Encode(v)`.
    pub fn encode_indent(&self, indent: &str) -> String {
        let mut out = String::new();
        self.write(&mut out, Some(indent), 0);
        out.push('\n');
        out
    }

    fn write(&self, out: &mut String, indent: Option<&str>, depth: usize) {
        let newline = |out: &mut String, depth: usize| {
            if let Some(ind) = indent {
                out.push('\n');
                for _ in 0..depth {
                    out.push_str(ind);
                }
            }
        };
        match self {
            GoJson::Null => out.push_str("null"),
            GoJson::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
            GoJson::Int(v) => {
                let _ = write!(out, "{v}");
            }
            GoJson::Str(s) => write_json_string(out, s),
            GoJson::Array(items) => {
                if items.is_empty() {
                    out.push_str("[]");
                    return;
                }
                out.push('[');
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    newline(out, depth + 1);
                    item.write(out, indent, depth + 1);
                }
                newline(out, depth);
                out.push(']');
            }
            GoJson::Object(fields) => {
                if fields.is_empty() {
                    out.push_str("{}");
                    return;
                }
                out.push('{');
                for (i, (k, v)) in fields.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    newline(out, depth + 1);
                    write_json_string(out, k);
                    out.push(':');
                    if indent.is_some() {
                        out.push(' ');
                    }
                    v.write(out, indent, depth + 1);
                }
                newline(out, depth);
                out.push('}');
            }
        }
    }
}

/// Go `encoding/json` string encoding with the default `EscapeHTML(true)`.
pub fn write_json_string(out: &mut String, s: &str) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{2028}' => out.push_str("\\u2028"),
            '\u{2029}' => out.push_str("\\u2029"),
            c if (c as u32) < 0x20 || c == '<' || c == '>' || c == '&' => {
                let b = c as u32 as usize;
                out.push_str("\\u00");
                out.push(HEX[b >> 4] as char);
                out.push(HEX[b & 0xf] as char);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

fn push_offset(out: &mut String, t: &DateTime<FixedOffset>, colon: bool, z_for_utc: bool) {
    let off = t.offset().local_minus_utc();
    if off == 0 && z_for_utc {
        out.push('Z');
        return;
    }
    let sign = if off < 0 { '-' } else { '+' };
    let off = off.unsigned_abs();
    let (h, m) = (off / 3600, (off / 60) % 60);
    if colon {
        let _ = write!(out, "{sign}{h:02}:{m:02}");
    } else {
        let _ = write!(out, "{sign}{h:02}{m:02}");
    }
}

fn push_date_time(out: &mut String, t: &DateTime<FixedOffset>, sep: char) {
    let _ = write!(
        out,
        "{:04}-{:02}-{:02}{sep}{:02}:{:02}:{:02}",
        t.year(),
        t.month(),
        t.day(),
        t.hour(),
        t.minute(),
        t.second()
    );
}

/// Go `"2006-01-02T15:04:05.999999999Z07:00"`.
pub fn format_rfc3339_nano(t: &DateTime<FixedOffset>) -> String {
    let mut out = String::with_capacity(35);
    push_date_time(&mut out, t, 'T');
    push_trimmed_nanos(&mut out, t.nanosecond());
    push_offset(&mut out, t, true, true);
    out
}

fn push_trimmed_nanos(out: &mut String, nanos: u32) {
    if nanos == 0 {
        return;
    }
    let frac = format!("{nanos:09}");
    out.push('.');
    out.push_str(frac.trim_end_matches('0'));
}

/// Go `t.Format("2006-01-02 15:04:05.000")`.
pub fn format_date_time_millis(t: &DateTime<FixedOffset>) -> String {
    let mut out = String::with_capacity(23);
    push_date_time(&mut out, t, ' ');
    let _ = write!(out, ".{:03}", t.nanosecond() / 1_000_000);
    out
}

/// Go `t.Format("15:04:05.000")`.
pub fn format_time_millis(t: &DateTime<FixedOffset>) -> String {
    format!(
        "{:02}:{:02}:{:02}.{:03}",
        t.hour(),
        t.minute(),
        t.second(),
        t.nanosecond() / 1_000_000
    )
}

/// Go `time.Time.String()` without a monotonic clock reading:
/// `"2006-01-02 15:04:05.999999999 -0700 MST"`. Go prints the zone abbreviation; Rust has no
/// tz database abbreviations, so a zero offset prints `UTC` and other offsets print the numeric
/// offset (what Go prints for an unnamed fixed zone).
pub fn format_go_time_string(t: &DateTime<FixedOffset>) -> String {
    let mut out = String::with_capacity(40);
    push_date_time(&mut out, t, ' ');
    push_trimmed_nanos(&mut out, t.nanosecond());
    out.push(' ');
    push_offset(&mut out, t, false, false);
    out.push(' ');
    if t.offset().local_minus_utc() == 0 {
        out.push_str("UTC");
    } else {
        push_offset(&mut out, t, false, false);
    }
    out
}

/// Go's zero `time.Time{}` (`0001-01-01 00:00:00 +0000 UTC`).
pub fn go_zero_time() -> DateTime<FixedOffset> {
    DateTime::from_timestamp(-62_135_596_800, 0)
        .expect("Go zero time is representable")
        .fixed_offset()
}

/// Go `net.IP.String()`.
pub fn go_ip_string(ip: &[u8]) -> String {
    match ip.len() {
        0 => "<nil>".to_string(),
        4 => {
            let a: [u8; 4] = ip.try_into().expect("4 bytes");
            std::net::Ipv4Addr::from(a).to_string()
        }
        16 => {
            let a: [u8; 16] = ip.try_into().expect("16 bytes");
            std::net::Ipv6Addr::from(a).to_canonical().to_string()
        }
        _ => {
            let mut s = String::from("?");
            for b in ip {
                let _ = write!(s, "{b:02x}");
            }
            s
        }
    }
}

/// Go `net.IP.MarshalText` (used by `encoding/json` for `net.IP`).
pub fn go_ip_marshal_text(ip: &[u8]) -> Result<String, String> {
    match ip.len() {
        0 => Ok(String::new()),
        4 | 16 => Ok(go_ip_string(ip)),
        _ => {
            let mut hex = String::new();
            for b in ip {
                let _ = write!(hex, "{b:02x}");
            }
            Err(format!("address {hex}: invalid IP address"))
        }
    }
}

/// Go `net.JoinHostPort`.
pub fn join_host_port(host: &str, port: u16) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

/// Go `fmt.Sprintf("%#v", s)` for a string (`strconv.Quote`).
pub fn go_sharp_v_str(s: &str) -> String {
    crate::dns::go_quote(s)
}

/// Go `%#v` for an unsigned integer (`0x` hex).
pub fn go_sharp_v_uint(v: u64) -> String {
    format!("{v:#x}")
}

/// Go `%#v` for `[]uint32`.
pub fn go_sharp_v_u32_slice(v: &[u32]) -> String {
    if v.is_empty() {
        return "[]uint32(nil)".to_string();
    }
    let mut out = String::from("[]uint32{");
    for (i, t) in v.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        out.push_str(&go_sharp_v_uint(u64::from(*t)));
    }
    out.push('}');
    out
}

/// Go `%#v` for `map[uint64]uint64` (sorted keys; nil when empty).
pub fn go_sharp_v_u64_map<'a>(entries: impl IntoIterator<Item = (&'a u64, &'a u64)>) -> String {
    let mut kv: Vec<(u64, u64)> = entries.into_iter().map(|(k, v)| (*k, *v)).collect();
    if kv.is_empty() {
        return "map[uint64]uint64(nil)".to_string();
    }
    kv.sort_unstable();
    let mut out = String::from("map[uint64]uint64{");
    for (i, (k, v)) in kv.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        let _ = write!(out, "{}:{}", go_sharp_v_uint(*k), go_sharp_v_uint(*v));
    }
    out.push('}');
    out
}

/// `github.com/dustin/go-humanize.IBytes`.
pub fn humanize_ibytes(s: u64) -> String {
    const SIZES: [&str; 7] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"];
    if s < 10 {
        return format!("{s} B");
    }
    let base = 1024f64;
    let e = ((s as f64).ln() / base.ln()).floor();
    let suffix = SIZES[e as usize];
    let val = (s as f64 / base.powf(e) * 10.0 + 0.5).floor() / 10.0;
    if val < 10.0 {
        format!("{} {suffix}", go_format_float_prec(val, 1))
    } else {
        format!("{} {suffix}", go_format_float_prec(val, 0))
    }
}

/// Go `fmt.Sprintf("%.Nf", v)` (round-half-even on the exact binary value, like Rust).
fn go_format_float_prec(v: f64, prec: usize) -> String {
    format!("{v:.prec$}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_escapes_like_go() {
        let mut s = String::new();
        write_json_string(&mut s, "a<b>&\"\\\n\u{1}\u{2028}é");
        assert_eq!(s, r#""a\u003cb\u003e\u0026\"\\\n\u0001\u2028é""#);
    }

    #[test]
    fn json_indent_matches_go() {
        let v = GoJson::Object(vec![
            ("a".into(), GoJson::Array(vec![GoJson::int(1), GoJson::int(2)])),
            ("b".into(), GoJson::Object(vec![])),
            ("c".into(), GoJson::Array(vec![])),
            ("d".into(), GoJson::Null),
        ]);
        assert_eq!(v.encode(), "{\"a\":[1,2],\"b\":{},\"c\":[],\"d\":null}\n");
        assert_eq!(
            v.encode_indent("    "),
            "{\n    \"a\": [\n        1,\n        2\n    ],\n    \"b\": {},\n    \"c\": [],\n    \"d\": null\n}\n"
        );
    }

    #[test]
    fn times_match_go_layouts() {
        let t = DateTime::from_timestamp(1_700_000_000, 120_000_000)
            .unwrap()
            .with_timezone(&FixedOffset::east_opt(3600 + 1800).unwrap());
        assert_eq!(format_rfc3339_nano(&t), "2023-11-14T23:43:20.12+01:30");
        assert_eq!(format_date_time_millis(&t), "2023-11-14 23:43:20.120");
        assert_eq!(format_time_millis(&t), "23:43:20.120");
        assert_eq!(format_go_time_string(&t), "2023-11-14 23:43:20.12 +0130 +0130");
        assert_eq!(format_rfc3339_nano(&go_zero_time()), "0001-01-01T00:00:00Z");
        assert_eq!(format_go_time_string(&go_zero_time()), "0001-01-01 00:00:00 +0000 UTC");
    }

    #[test]
    fn ip_strings_match_go() {
        let mapped = std::net::Ipv4Addr::new(10, 42, 2, 9).to_ipv6_mapped().octets();
        assert_eq!(go_ip_string(&mapped), "10.42.2.9");
        assert_eq!(go_ip_string(&[127, 0, 0, 1]), "127.0.0.1");
        assert_eq!(go_ip_string(&[]), "<nil>");
        assert_eq!(go_ip_string(&[1, 2]), "?0102");
        assert_eq!(go_ip_marshal_text(&[]).unwrap(), "");
        assert_eq!(go_ip_marshal_text(&[1, 2]).unwrap_err(), "address 0102: invalid IP address");
        assert_eq!(join_host_port("::1", 7946), "[::1]:7946");
        assert_eq!(join_host_port("10.0.0.1", 7946), "10.0.0.1:7946");
    }

    // Values from github.com/dustin/go-humanize bytes_test.go (IBytes cases).
    #[test]
    fn humanize_ibytes_matches_go() {
        for (n, want) in [
            (0u64, "0 B"),
            (1, "1 B"),
            (803, "803 B"),
            (1023, "1023 B"),
            (1024, "1.0 KiB"),
            (1024 * 1024, "1.0 MiB"),
            (1 << 30, "1.0 GiB"),
            (1 << 40, "1.0 TiB"),
            (1 << 50, "1.0 PiB"),
            (1 << 60, "1.0 EiB"),
            (82_854_982, "79 MiB"),
            (15_361, "15 KiB"),
            (2602, "2.5 KiB"),
        ] {
            assert_eq!(humanize_ibytes(n), want, "IBytes({n})");
        }
    }

    #[test]
    fn sharp_v_shapes() {
        assert_eq!(go_sharp_v_u32_slice(&[]), "[]uint32(nil)");
        assert_eq!(go_sharp_v_u32_slice(&[1, 255]), "[]uint32{0x1, 0xff}");
        let m: std::collections::HashMap<u64, u64> = [(2, 3), (1, 0)].into_iter().collect();
        assert_eq!(go_sharp_v_u64_map(&m), "map[uint64]uint64{0x1:0x0, 0x2:0x3}");
        assert_eq!(go_sharp_v_str("a\"b"), "\"a\\\"b\"");
    }
}
