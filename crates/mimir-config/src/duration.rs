//! Go `time.Duration` and Prometheus `model.Duration` parsers, formatters, and serde codecs.
//!
//! Ported from:
//! - Go stdlib `time.ParseDuration` / `time.Duration.String`
//! - github.com/prometheus/common/model/time.go (`ParseDuration`, `Duration.String`)

use std::fmt;
use std::str::FromStr;
use std::time::Duration as StdDuration;

use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::error::ConfigError;

/// Nanosecond duration matching Go `time.Duration` (`int64`).
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GoDuration(pub i64);

impl GoDuration {
    pub const ZERO: Self = Self(0);

    pub const fn from_nanos(nanos: i64) -> Self {
        Self(nanos)
    }

    pub const fn from_millis(ms: i64) -> Self {
        Self(ms * 1_000_000)
    }

    pub const fn from_secs(secs: i64) -> Self {
        Self(secs * 1_000_000_000)
    }

    pub const fn from_mins(mins: i64) -> Self {
        Self(mins * 60 * 1_000_000_000)
    }

    pub const fn from_hours(hours: i64) -> Self {
        Self(hours * 3600 * 1_000_000_000)
    }

    pub const fn as_nanos(self) -> i64 {
        self.0
    }

    pub const fn as_millis(self) -> i64 {
        self.0 / 1_000_000
    }

    pub fn as_std(self) -> Option<StdDuration> {
        if self.0 < 0 {
            None
        } else {
            Some(StdDuration::from_nanos(self.0 as u64))
        }
    }
}

impl FromStr for GoDuration {
    type Err = ConfigError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_go_duration(s).map(Self)
    }
}

impl fmt::Display for GoDuration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&format_go_duration(self.0))
    }
}

impl Serialize for GoDuration {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for GoDuration {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct GoDurationVisitor;

        impl<'de> Visitor<'de> for GoDurationVisitor {
            type Value = GoDuration;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a Go duration string (e.g. '1h30m', '500ms') or 0")
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                parse_go_duration(v).map(GoDuration).map_err(E::custom)
            }

            fn visit_i64<E: de::Error>(self, v: i64) -> Result<Self::Value, E> {
                if v == 0 {
                    Ok(GoDuration(0))
                } else {
                    Err(E::custom(format!(
                        "cannot unmarshal non-zero integer {v} into time.Duration"
                    )))
                }
            }

            fn visit_u64<E: de::Error>(self, v: u64) -> Result<Self::Value, E> {
                if v == 0 {
                    Ok(GoDuration(0))
                } else {
                    Err(E::custom(format!(
                        "cannot unmarshal non-zero integer {v} into time.Duration"
                    )))
                }
            }
        }

        deserializer.deserialize_any(GoDurationVisitor)
    }
}

/// Nanosecond duration matching Prometheus `model.Duration` (supports `y`, `w`, `d`, `h`, `m`, `s`, `ms`).
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ModelDuration(pub i64);

impl ModelDuration {
    pub const ZERO: Self = Self(0);

    pub const fn from_nanos(nanos: i64) -> Self {
        Self(nanos)
    }

    pub const fn from_millis(ms: i64) -> Self {
        Self(ms * 1_000_000)
    }

    pub const fn from_secs(secs: i64) -> Self {
        Self(secs * 1_000_000_000)
    }

    pub const fn from_mins(mins: i64) -> Self {
        Self(mins * 60 * 1_000_000_000)
    }

    pub const fn from_hours(hours: i64) -> Self {
        Self(hours * 3600 * 1_000_000_000)
    }

    pub const fn from_days(days: i64) -> Self {
        Self(days * 24 * 3600 * 1_000_000_000)
    }

    pub const fn as_nanos(self) -> i64 {
        self.0
    }

    pub const fn as_millis(self) -> i64 {
        self.0 / 1_000_000
    }

    pub fn as_std(self) -> Option<StdDuration> {
        if self.0 < 0 {
            None
        } else {
            Some(StdDuration::from_nanos(self.0 as u64))
        }
    }
}

impl FromStr for ModelDuration {
    type Err = ConfigError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_model_duration(s).map(Self)
    }
}

impl fmt::Display for ModelDuration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&format_model_duration(self.0))
    }
}

impl Serialize for ModelDuration {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for ModelDuration {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ModelDurationVisitor;

        impl<'de> Visitor<'de> for ModelDurationVisitor {
            type Value = ModelDuration;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a Prometheus model.Duration string (e.g. '7d', '10m') or 0")
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                parse_model_duration(v).map(ModelDuration).map_err(E::custom)
            }

            fn visit_i64<E: de::Error>(self, v: i64) -> Result<Self::Value, E> {
                if v == 0 {
                    Ok(ModelDuration(0))
                } else {
                    Err(E::custom(format!("not a valid duration string: {v}")))
                }
            }

            fn visit_u64<E: de::Error>(self, v: u64) -> Result<Self::Value, E> {
                if v == 0 {
                    Ok(ModelDuration(0))
                } else {
                    Err(E::custom(format!("not a valid duration string: {v}")))
                }
            }
        }

        deserializer.deserialize_any(ModelDurationVisitor)
    }
}

/// Parses a duration using Go `time.ParseDuration` rules (`ns`, `us`, `µs`, `μs`, `ms`, `s`, `m`, `h`).
pub fn parse_go_duration(s: &str) -> Result<i64, ConfigError> {
    let orig = s;
    // Go `time.parseDurationError.Error()`: `time: <message> "<orig>"`.
    let err = |message: &str| ConfigError::Duration {
        input: orig.to_string(),
        reason: format!("time: {message} {orig:?}"),
    };

    if s.is_empty() {
        return Err(err("invalid duration"));
    }

    let (neg, mut rest) = if let Some(r) = s.strip_prefix('-') {
        (true, r)
    } else if let Some(r) = s.strip_prefix('+') {
        (false, r)
    } else {
        (false, s)
    };

    if rest == "0" {
        return Ok(0);
    }
    if rest.is_empty() {
        return Err(err("invalid duration"));
    }

    let mut d: u64 = 0;

    while !rest.is_empty() {
        // Must start with digit or '.'
        let bytes = rest.as_bytes();
        if !bytes[0].is_ascii_digit() && bytes[0] != b'.' {
            return Err(err("invalid duration"));
        }

        let (v, f, scale, after_num) = parse_leading_number(rest).ok_or_else(|| err("invalid duration"))?;
        rest = after_num;

        // Consume unit (up to next digit or '.').
        let unit_end = rest
            .char_indices()
            .find(|&(_, c)| c.is_ascii_digit() || c == '.')
            .map(|(idx, _)| idx)
            .unwrap_or(rest.len());

        if unit_end == 0 {
            return Err(err("missing unit in duration"));
        }
        let unit_str = &rest[..unit_end];
        rest = &rest[unit_end..];

        let unit: u64 = match unit_str {
            "ns" => 1,
            "us" | "µs" | "μs" => 1_000,
            "ms" => 1_000_000,
            "s" => 1_000_000_000,
            "m" => 60 * 1_000_000_000,
            "h" => 3_600 * 1_000_000_000,
            _ => return Err(err(&format!("unknown unit {unit_str:?} in duration"))),
        };

        if v > (1u64 << 63) / unit {
            return Err(err("invalid duration"));
        }
        let mut add = v * unit;
        if f > 0 {
            // Compute frac * unit / scale in floating/u128 matching Go's float64 conversion.
            let frac_val = ((f as f64) * ((unit as f64) / scale)) as u64;
            let n = add.wrapping_add(frac_val);
            if n < add || n > (1u64 << 63) {
                return Err(err("invalid duration"));
            }
            add = n;
        }
        let next = d.wrapping_add(add);
        if next < d || next > (1u64 << 63) {
            return Err(err("invalid duration"));
        }
        d = next;
    }

    if neg {
        if d == (1u64 << 63) {
            return Ok(i64::MIN);
        }
        return Ok(-(d as i64));
    }

    if d > i64::MAX as u64 {
        return Err(err("invalid duration"));
    }
    Ok(d as i64)
}

fn parse_leading_number(s: &str) -> Option<(u64, u64, f64, &str)> {
    let bytes = s.as_bytes();
    let mut i = 0;

    // Integer part
    let mut v: u64 = 0;
    let start = i;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        let dig = (bytes[i] - b'0') as u64;
        if v > (1u64 << 63) / 10 {
            return None;
        }
        v = v * 10 + dig;
        if v > (1u64 << 63) {
            return None;
        }
        i += 1;
    }
    let pre_digits = i > start;

    let mut f: u64 = 0;
    let mut scale: f64 = 1.0;
    let mut post_digits = false;

    if i < bytes.len() && bytes[i] == b'.' {
        i += 1;
        let frac_start = i;
        let mut overflow = false;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            if !overflow {
                let dig = (bytes[i] - b'0') as u64;
                if f > (1u64 << 63) / 10 {
                    overflow = true;
                } else {
                    f = f * 10 + dig;
                    scale *= 10.0;
                }
            }
            i += 1;
        }
        post_digits = i > frac_start;
    }

    if !pre_digits && !post_digits {
        return None;
    }

    Some((v, f, scale, &s[i..]))
}

/// Parses a duration using Prometheus `model.ParseDuration` rules (`y`, `w`, `d`, `h`, `m`, `s`, `ms`).
pub fn parse_model_duration(s: &str) -> Result<i64, ConfigError> {
    let orig = s;
    // Messages of prometheus/common/model.ParseDuration.
    let err = |reason: &str| ConfigError::Duration {
        input: orig.to_string(),
        reason: match reason {
            "not a valid duration string" => format!("not a valid duration string: {orig:?}"),
            other => other.to_string(),
        },
    };

    match s {
        "0" => return Ok(0),
        "" => return Err(err("empty duration string")),
        _ => {}
    }

    let mut rest = s;
    let mut dur: u64 = 0;
    let mut last_unit_pos = 0u8;

    while !rest.is_empty() {
        let bytes = rest.as_bytes();
        if !bytes[0].is_ascii_digit() {
            return Err(err("not a valid duration string"));
        }
        let mut i = 0;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
        let v: u64 = rest[..i]
            .parse()
            .map_err(|_| err("not a valid duration string"))?;
        rest = &rest[i..];

        let bytes = rest.as_bytes();
        let mut u_end = 0;
        while u_end < bytes.len() && !bytes[u_end].is_ascii_digit() {
            u_end += 1;
        }
        if u_end == 0 {
            return Err(err("not a valid duration string"));
        }
        let u = &rest[..u_end];
        rest = &rest[u_end..];

        let (pos, mult): (u8, u64) = match u {
            "ms" => (7, 1_000_000),
            "s" => (6, 1_000_000_000),
            "m" => (5, 60 * 1_000_000_000),
            "h" => (4, 3_600 * 1_000_000_000),
            "d" => (3, 24 * 3_600 * 1_000_000_000),
            "w" => (2, 7 * 24 * 3_600 * 1_000_000_000),
            "y" => (1, 365 * 24 * 3_600 * 1_000_000_000),
            _ => return Err(err(&format!("unknown unit {u:?} in duration {orig:?}"))),
        };

        if pos <= last_unit_pos {
            return Err(err("not a valid duration string"));
        }
        last_unit_pos = pos;

        if v > (1u64 << 63) / mult {
            return Err(err("duration out of range"));
        }
        dur = dur
            .checked_add(v * mult)
            .ok_or_else(|| err("duration out of range"))?;
        if dur > (1u64 << 63) - 1 {
            return Err(err("duration out of range"));
        }
    }

    Ok(dur as i64)
}

/// Parses a duration accepting either Go `time.ParseDuration` or Prometheus `model.ParseDuration` syntax
/// (supporting `ns`, `us`, `µs`, `ms`, `s`, `m`, `h`, `d`, `w`, `y`, compound forms, and `"0"`).
pub fn parse_duration(s: &str) -> Result<i64, ConfigError> {
    if let Ok(d) = parse_go_duration(s) {
        return Ok(d);
    }
    parse_model_duration(s)
}

/// Formats nanoseconds identically to Go's `time.Duration.String()`.
pub fn format_go_duration(nanos: i64) -> String {
    if nanos == 0 {
        return "0s".to_string();
    }

    let neg = nanos < 0;
    let mut u = if neg {
        nanos.wrapping_neg() as u64
    } else {
        nanos as u64
    };

    let mut out = String::new();
    if neg {
        out.push('-');
    }

    if u < 1_000_000_000 {
        // Sub-second: µs or ms or ns
        let prec;
        if u < 1_000 {
            return format!("{out}{u}ns");
        } else if u < 1_000_000 {
            prec = 3;
            let (whole, frac) = (u / 1_000, u % 1_000);
            out.push_str(&whole.to_string());
            push_frac(&mut out, frac, prec);
            out.push_str("µs");
        } else {
            prec = 6;
            let (whole, frac) = (u / 1_000_000, u % 1_000_000);
            out.push_str(&whole.to_string());
            push_frac(&mut out, frac, prec);
            out.push_str("ms");
        }
        return out;
    }

    let hours = u / 3_600_000_000_000;
    u %= 3_600_000_000_000;
    let mins = u / 60_000_000_000;
    u %= 60_000_000_000;
    let secs = u / 1_000_000_000;
    let frac = u % 1_000_000_000;

    if hours > 0 {
        out.push_str(&hours.to_string());
        out.push('h');
        out.push_str(&mins.to_string());
        out.push('m');
    } else if mins > 0 {
        out.push_str(&mins.to_string());
        out.push('m');
    }

    out.push_str(&secs.to_string());
    push_frac(&mut out, frac, 9);
    out.push('s');
    out
}

fn push_frac(out: &mut String, frac: u64, width: usize) {
    if frac == 0 {
        return;
    }
    let s = format!("{frac:0width$}");
    let trimmed = s.trim_end_matches('0');
    out.push('.');
    out.push_str(trimmed);
}

/// Formats nanoseconds identically to Prometheus `model.Duration.String()`.
pub fn format_model_duration(nanos: i64) -> String {
    let mut ms = nanos / 1_000_000;
    if ms == 0 {
        return "0s".to_string();
    }
    let mut out = String::new();
    if ms < 0 {
        out.push('-');
        ms = ms.wrapping_neg();
    }

    let units: &[(&str, i64, bool)] = &[
        ("y", 1000 * 60 * 60 * 24 * 365, true),
        ("w", 1000 * 60 * 60 * 24 * 7, true),
        ("d", 1000 * 60 * 60 * 24, false),
        ("h", 1000 * 60 * 60, false),
        ("m", 1000 * 60, false),
        ("s", 1000, false),
        ("ms", 1, false),
    ];

    for &(unit, mult, exact) in units {
        if exact && ms % mult != 0 {
            continue;
        }
        let v = ms / mult;
        if v > 0 {
            out.push_str(&format!("{v}{unit}"));
            ms -= v * mult;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn go_duration_parsing_and_formatting() {
        assert_eq!(parse_go_duration("0").unwrap(), 0);
        assert_eq!(parse_go_duration("-0").unwrap(), 0);
        assert_eq!(parse_go_duration("15s").unwrap(), 15_000_000_000);
        assert_eq!(parse_go_duration("500ms").unwrap(), 500_000_000);
        assert_eq!(parse_go_duration("250us").unwrap(), 250_000);
        assert_eq!(parse_go_duration("250µs").unwrap(), 250_000);
        assert_eq!(parse_go_duration("10ns").unwrap(), 10);
        assert_eq!(
            parse_go_duration("1h30m10s").unwrap(),
            (3600 + 1800 + 10) * 1_000_000_000
        );
        assert_eq!(parse_go_duration("1.5h").unwrap(), 5400 * 1_000_000_000);
        assert_eq!(parse_go_duration("-5m").unwrap(), -300 * 1_000_000_000);
        assert!(parse_go_duration("1d").is_err());
        assert!(parse_go_duration("").is_err());

        assert_eq!(GoDuration::from_secs(0).to_string(), "0s");
        assert_eq!(GoDuration::from_secs(90).to_string(), "1m30s");
        assert_eq!(GoDuration::from_hours(1).to_string(), "1h0m0s");
        assert_eq!(GoDuration::from_millis(500).to_string(), "500ms");
    }

    #[test]
    fn model_duration_parsing_and_formatting() {
        assert_eq!(parse_model_duration("0").unwrap(), 0);
        assert_eq!(parse_model_duration("7d").unwrap(), 7 * 86_400_000_000_000);
        assert_eq!(
            parse_model_duration("1y2w3d4h5m6s7ms").unwrap(),
            ((365 + 14 + 3) * 86400 + 4 * 3600 + 5 * 60 + 6) * 1_000_000_000 + 7_000_000
        );
        assert!(parse_model_duration("1h2d").is_err());
        assert!(parse_model_duration("-1h").is_err());
        assert_eq!(ModelDuration::from_days(7).to_string(), "1w");
        assert_eq!(ModelDuration::from_days(2).to_string(), "2d");
        assert_eq!(ModelDuration::from_mins(10).to_string(), "10m");
    }
}
