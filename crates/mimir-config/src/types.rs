//! Go / dskit / Mimir configuration value types and parsers.
//!
//! Ported from:
//! - github.com/grafana/dskit/flagext/*
//! - github.com/grafana/dskit/log/level.go
//! - github.com/grafana/mimir/pkg/util/validation/*
//! - github.com/grafana/mimir/pkg/storage/ingest/config.go

use std::collections::BTreeMap;
use std::fmt;
use std::ops::{Deref, DerefMut};

use serde::de::{self, Deserializer, SeqAccess, Visitor};
use serde::ser::Serializer;
use serde::{Deserialize, Serialize};

use crate::error::ConfigError;

/// `flagext.StringSliceCSV`: comma-separated string in both CLI flags and YAML string values.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StringSliceCSV(pub Vec<String>);

impl StringSliceCSV {
    pub fn set(&mut self, s: &str) {
        if s.is_empty() {
            self.0.clear();
        } else {
            self.0 = s.split(',').map(str::to_string).collect();
        }
    }
}

impl Deref for StringSliceCSV {
    type Target = Vec<String>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for StringSliceCSV {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl fmt::Display for StringSliceCSV {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0.join(","))
    }
}

impl Serialize for StringSliceCSV {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for StringSliceCSV {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        let mut out = StringSliceCSV::default();
        out.set(&s);
        Ok(out)
    }
}

/// `flagext.StringSliceCSVMulti`: supports comma-separated CLI flags AND repeated invocations,
/// and either comma-separated string or YAML sequence in YAML.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StringSliceCSVMulti(pub Vec<String>);

impl StringSliceCSVMulti {
    pub fn append_csv(&mut self, s: &str) {
        if !s.is_empty() {
            self.0.extend(s.split(',').map(str::to_string));
        }
    }
}

impl Deref for StringSliceCSVMulti {
    type Target = Vec<String>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for StringSliceCSVMulti {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl fmt::Display for StringSliceCSVMulti {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0.join(","))
    }
}

impl Serialize for StringSliceCSVMulti {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for StringSliceCSVMulti {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct MultiVisitor;

        impl<'de> Visitor<'de> for MultiVisitor {
            type Value = StringSliceCSVMulti;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a comma-separated string or list of strings")
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                if v.is_empty() {
                    Ok(StringSliceCSVMulti(Vec::new()))
                } else {
                    Ok(StringSliceCSVMulti(
                        v.split(',').map(str::to_string).collect(),
                    ))
                }
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let mut items = Vec::new();
                while let Some(item) = seq.next_element::<String>()? {
                    items.push(item);
                }
                Ok(StringSliceCSVMulti(items))
            }

            fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(StringSliceCSVMulti(Vec::new()))
            }
        }

        deserializer.deserialize_any(MultiVisitor)
    }
}

/// `flagext.Bytes`: human-readable base-2 byte counts (`alecthomas/units.ParseBase2Bytes`,
/// e.g. `64KiB`, `1.5MiB`, `1MB`, `0`).
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct HumanBytes(pub u64);

/// Ported 1:1 from `github.com/alecthomas/units/util.go` (`leadingInt`).
fn units_leading_int(s: &str) -> Result<(i64, &str), ()> {
    const MAX_LEADING_INT: i64 = (i64::MAX - 9) / 10; // (1<<63 - 10) / 10
    let bytes = s.as_bytes();
    let mut x: i64 = 0;
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        if !c.is_ascii_digit() {
            break;
        }
        if x >= MAX_LEADING_INT {
            return Err(());
        }
        x = x * 10 + i64::from(c - b'0');
        i += 1;
    }
    Ok((x, &s[i..]))
}

/// Ported 1:1 from `github.com/alecthomas/units/util.go` (`ParseUnit`).
fn units_parse_unit(mut s: &str, lookup: fn(&str) -> Option<f64>) -> Result<i64, String> {
    let orig = s;
    let mut f: f64 = 0.0;
    let mut neg = false;

    if let Some(first) = s.as_bytes().first().copied() {
        if first == b'-' || first == b'+' {
            neg = first == b'-';
            s = &s[1..];
        }
    }
    if s == "0" {
        return Ok(0);
    }
    if s.is_empty() {
        return Err(format!("units: invalid {orig}"));
    }

    while !s.is_empty() {
        let first = s.as_bytes()[0];
        if !(first == b'.' || first.is_ascii_digit()) {
            return Err(format!("units: invalid {orig}"));
        }

        let pl = s.len();
        let (x, rem) = units_leading_int(s).map_err(|()| format!("units: invalid {orig}"))?;
        s = rem;
        let mut g = x as f64;
        let pre = pl != s.len();

        let mut post = false;
        if s.starts_with('.') {
            s = &s[1..];
            let pl_frac = s.len();
            let (frac, rem_frac) =
                units_leading_int(s).map_err(|()| format!("units: invalid {orig}"))?;
            s = rem_frac;
            let mut scale = 1.0_f64;
            for _ in 0..(pl_frac - s.len()) {
                scale *= 10.0;
            }
            g += (frac as f64) / scale;
            post = pl_frac != s.len();
        }
        if !pre && !post {
            return Err(format!("units: invalid {orig}"));
        }

        let bytes = s.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            let c = bytes[i];
            if c == b'.' || c.is_ascii_digit() {
                break;
            }
            i += 1;
        }
        let u = &s[..i];
        s = &s[i..];
        let unit = lookup(u).ok_or_else(|| format!("units: unknown unit {u} in {orig}"))?;
        f += g * unit;
    }

    if neg {
        f = -f;
    }
    if f < (-1_i64 << 63) as f64 || f > i64::MAX as f64 {
        return Err("units: overflow parsing unit".to_string());
    }
    Ok(f as i64)
}

fn base2_iec_unit(u: &str) -> Option<f64> {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    const GIB: f64 = MIB * 1024.0;
    const TIB: f64 = GIB * 1024.0;
    const PIB: f64 = TIB * 1024.0;
    const EIB: f64 = PIB * 1024.0;
    match u {
        "B" => Some(1.0),
        "KiB" => Some(KIB),
        "MiB" => Some(MIB),
        "GiB" => Some(GIB),
        "TiB" => Some(TIB),
        "PiB" => Some(PIB),
        "EiB" => Some(EIB),
        _ => None,
    }
}

fn base2_old_unit(u: &str) -> Option<f64> {
    const KB: f64 = 1024.0;
    const MB: f64 = KB * 1024.0;
    const GB: f64 = MB * 1024.0;
    const TB: f64 = GB * 1024.0;
    const PB: f64 = TB * 1024.0;
    const EB: f64 = PB * 1024.0;
    match u {
        "B" => Some(1.0),
        "KB" => Some(KB),
        "MB" => Some(MB),
        "GB" => Some(GB),
        "TB" => Some(TB),
        "PB" => Some(PB),
        "EB" => Some(EB),
        _ => None,
    }
}

impl HumanBytes {
    /// Parses `s` with Go `alecthomas/units.ParseBase2Bytes` semantics and returns the raw
    /// Go error message on failure (without `ConfigError::Flag`'s `"invalid flag: "` prefix).
    pub fn parse_str(s: &str) -> Result<u64, String> {
        let n = match units_parse_unit(s, base2_iec_unit) {
            Ok(n) => n,
            Err(_) => units_parse_unit(s, base2_old_unit)?,
        };
        Ok(n as u64)
    }

    pub fn parse(s: &str) -> Result<u64, ConfigError> {
        Self::parse_str(s).map_err(ConfigError::Flag)
    }
}

/// dskit `flagext.Bytes.String()` = `units.Base2Bytes.String()` (alecthomas/units `ToString(n,
/// 1024, "iB", "B")`): e.g. `65535` -> `63KiB1023B`, `0` -> `0B`, `1048576` -> `1MiB`. Used for the
/// flag default and the YAML (`MarshalYAML`) rendering.
impl fmt::Display for HumanBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        const SI_UNITS: [&str; 7] = ["", "K", "M", "G", "T", "P", "E"];
        // Go converts to int64 first.
        let mut n = self.0 as i64;
        let mut parts: [Option<(i64, &str, &str)>; 7] = [None; 7];
        for (i, unit) in SI_UNITS.iter().enumerate() {
            if n % 1024 != 0 || (i == 0 && n == 0) {
                parts[i] = Some((n % 1024, unit, if i == 0 { "B" } else { "iB" }));
            }
            n /= 1024;
            if n == 0 {
                break;
            }
        }
        for (v, unit, suffix) in parts.iter().rev().flatten() {
            write!(f, "{v}{unit}{suffix}")?;
        }
        Ok(())
    }
}

impl Serialize for HumanBytes {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for HumanBytes {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct BytesVisitor;
        impl<'de> Visitor<'de> for BytesVisitor {
            type Value = HumanBytes;
            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a byte size string (e.g. '64KiB')")
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> Result<Self::Value, E> {
                HumanBytes::parse_str(&v.to_string())
                    .map(HumanBytes)
                    .map_err(E::custom)
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> Result<Self::Value, E> {
                HumanBytes::parse_str(&v.to_string())
                    .map(HumanBytes)
                    .map_err(E::custom)
            }
            fn visit_f64<E: de::Error>(self, v: f64) -> Result<Self::Value, E> {
                HumanBytes::parse_str(&v.to_string())
                    .map(HumanBytes)
                    .map_err(E::custom)
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                HumanBytes::parse_str(v).map(HumanBytes).map_err(E::custom)
            }
        }
        deserializer.deserialize_any(BytesVisitor)
    }
}

/// `dskit/log.Level`: valid values `[debug, info, warn, error]`.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Debug,
    #[default]
    Info,
    Warn,
    Error,
}

impl LogLevel {
    pub fn parse(s: &str) -> Result<Self, ConfigError> {
        match s {
            "debug" => Ok(Self::Debug),
            "info" => Ok(Self::Info),
            "warn" => Ok(Self::Warn),
            "error" => Ok(Self::Error),
            _ => Err(ConfigError::Flag(format!("unrecognized log level {s:?}"))),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Debug => "debug",
            Self::Info => "info",
            Self::Warn => "warn",
            Self::Error => "error",
        }
    }
}

impl fmt::Display for LogLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for LogLevel {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Self::parse(&s).map_err(de::Error::custom)
    }
}

/// `validation.LabelValueLengthOverLimitStrategy`
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum LabelValueLengthOverLimitStrategy {
    #[default]
    Error,
    Truncate,
    Drop,
}

impl LabelValueLengthOverLimitStrategy {
    pub fn parse(s: &str) -> Result<Self, ConfigError> {
        match s {
            "" | "error" => Ok(Self::Error),
            "truncate" => Ok(Self::Truncate),
            "drop" => Ok(Self::Drop),
            _ => Err(ConfigError::Flag(format!(
                "unrecognized LabelValueLengthOverLimitStrategy: {s:?}"
            ))),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Error => "error",
            Self::Truncate => "truncate",
            Self::Drop => "drop",
        }
    }
}

impl fmt::Display for LabelValueLengthOverLimitStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for LabelValueLengthOverLimitStrategy {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Self::parse(&s).map_err(de::Error::custom)
    }
}

/// `validation.OTelTranslationStrategyValue`
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct OTelTranslationStrategy(pub String);

impl OTelTranslationStrategy {
    pub fn parse(s: &str) -> Result<Self, ConfigError> {
        match s {
            ""
            | "UnderscoreEscapingWithSuffixes"
            | "UnderscoreEscapingWithoutSuffixes"
            | "NoUTF8EscapingWithSuffixes"
            | "NoTranslation" => Ok(Self(s.to_string())),
            _ => Err(ConfigError::Flag(format!(
                "unrecognized OTel translation strategy {s:?}"
            ))),
        }
    }

    pub fn set(&mut self, text: &str) -> Result<(), ConfigError> {
        *self = Self::parse(text)?;
        Ok(())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for OTelTranslationStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for OTelTranslationStrategy {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Self::parse(&s).map_err(de::Error::custom)
    }
}

/// Prometheus `model.ValidationScheme`.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum NameValidationScheme {
    #[default]
    Unset,
    Legacy,
    Utf8,
}

impl NameValidationScheme {
    /// Parses `ValidationScheme` for YAML/JSON deserialization: `""` yields `Unset`, `"unset"` is rejected.
    pub fn parse(s: &str) -> Result<Self, ConfigError> {
        match s {
            "" => Ok(Self::Unset),
            "legacy" => Ok(Self::Legacy),
            "utf8" => Ok(Self::Utf8),
            _ => Err(ConfigError::Flag(format!(
                "unrecognized ValidationScheme: {s:?}"
            ))),
        }
    }

    /// Go `ValidationScheme.Set`: `""` is a no-op (keeps `current`); `"unset"` is rejected.
    pub fn parse_cli(s: &str, current: Self) -> Result<Self, ConfigError> {
        match s {
            "" => Ok(current),
            "legacy" => Ok(Self::Legacy),
            "utf8" => Ok(Self::Utf8),
            _ => Err(ConfigError::Flag(format!(
                "unrecognized ValidationScheme: {s:?}"
            ))),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unset => "unset",
            Self::Legacy => "legacy",
            Self::Utf8 => "utf8",
        }
    }
}

impl fmt::Display for NameValidationScheme {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Serialize for NameValidationScheme {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Unset => serializer.serialize_str(""),
            Self::Legacy => serializer.serialize_str("legacy"),
            Self::Utf8 => serializer.serialize_str("utf8"),
        }
    }
}

impl<'de> Deserialize<'de> for NameValidationScheme {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct SchemeVisitor;
        impl<'de> Visitor<'de> for SchemeVisitor {
            type Value = NameValidationScheme;
            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("validation scheme ('legacy' or 'utf8')")
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                NameValidationScheme::parse(v).map_err(E::custom)
            }
            fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(NameValidationScheme::Unset)
            }
        }
        deserializer.deserialize_any(SchemeVisitor)
    }
}

/// `ingest.SASLMechanism`
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub enum SASLMechanism {
    #[default]
    #[serde(rename = "PLAIN")]
    Plain,
    #[serde(rename = "SCRAM-SHA-256")]
    ScramSha256,
    #[serde(rename = "SCRAM-SHA-512")]
    ScramSha512,
    #[serde(rename = "OAUTHBEARER")]
    Oauthbearer,
    #[serde(rename = "AWS_MSK_IAM")]
    MskIam,
}

pub const INVALID_SASL_MECHANISM_ERR: &str = "the configured SASL mechanism is invalid, must be one of: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER, AWS_MSK_IAM";

impl SASLMechanism {
    pub fn parse(s: &str) -> Result<Self, ConfigError> {
        match s {
            "PLAIN" => Ok(Self::Plain),
            "SCRAM-SHA-256" => Ok(Self::ScramSha256),
            "SCRAM-SHA-512" => Ok(Self::ScramSha512),
            "OAUTHBEARER" => Ok(Self::Oauthbearer),
            "AWS_MSK_IAM" => Ok(Self::MskIam),
            _ => Err(ConfigError::Flag(INVALID_SASL_MECHANISM_ERR.to_string())),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Plain => "PLAIN",
            Self::ScramSha256 => "SCRAM-SHA-256",
            Self::ScramSha512 => "SCRAM-SHA-512",
            Self::Oauthbearer => "OAUTHBEARER",
            Self::MskIam => "AWS_MSK_IAM",
        }
    }
}

impl fmt::Display for SASLMechanism {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for SASLMechanism {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Self::parse(&s).map_err(de::Error::custom)
    }
}

/// `s3.BucketLookupType`
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum S3BucketLookupType {
    #[default]
    Auto,
    #[serde(rename = "virtual-hosted")]
    VirtualHost,
    Path,
}

impl S3BucketLookupType {
    pub fn parse(s: &str) -> Result<Self, ConfigError> {
        match s {
            "auto" => Ok(Self::Auto),
            "virtual-hosted" => Ok(Self::VirtualHost),
            "path" => Ok(Self::Path),
            // thanos BucketLookupType.UnmarshalYAML / Mimir bucketLookupTypeValue.Set.
            _ => Err(ConfigError::Flag(format!(
                "unsupported bucket lookup type: {s}"
            ))),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::VirtualHost => "virtual-hosted",
            Self::Path => "path",
        }
    }
}

impl fmt::Display for S3BucketLookupType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for S3BucketLookupType {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Self::parse(&s).map_err(de::Error::custom)
    }
}

/// `flagext.CIDRSliceCSV`: comma-separated list of network CIDRs (parsed like Go `net.ParseCIDR`,
/// canonicalized to masked network addresses, and appended on repeated CLI invocations).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CIDRSliceCSV(pub Vec<String>);

impl CIDRSliceCSV {
    pub fn parse_cidr(part: &str) -> Result<String, ConfigError> {
        let Some((ip_str, prefix_str)) = part.split_once('/') else {
            return Err(ConfigError::Flag(format!(
                "cidr: {part}: invalid CIDR address: {part}"
            )));
        };
        let ip: std::net::IpAddr = ip_str.parse().map_err(|_| {
            ConfigError::Flag(format!("cidr: {part}: invalid CIDR address: {part}"))
        })?;
        if prefix_str.is_empty() || !prefix_str.bytes().all(|b| b.is_ascii_digit()) {
            return Err(ConfigError::Flag(format!(
                "cidr: {part}: invalid CIDR address: {part}"
            )));
        }
        let prefix: u32 = prefix_str.parse().map_err(|_| {
            ConfigError::Flag(format!("cidr: {part}: invalid CIDR address: {part}"))
        })?;
        match ip {
            std::net::IpAddr::V4(v4) => {
                if prefix > 32 {
                    return Err(ConfigError::Flag(format!(
                        "cidr: {part}: invalid CIDR address: {part}"
                    )));
                }
                let mask = if prefix == 0 { 0 } else { !0u32 << (32 - prefix) };
                let masked = std::net::Ipv4Addr::from(u32::from(v4) & mask);
                Ok(format!("{masked}/{prefix}"))
            }
            std::net::IpAddr::V6(v6) => {
                if prefix > 128 {
                    return Err(ConfigError::Flag(format!(
                        "cidr: {part}: invalid CIDR address: {part}"
                    )));
                }
                let mask = if prefix == 0 {
                    0
                } else {
                    !0u128 << (128 - prefix)
                };
                let masked = std::net::Ipv6Addr::from(u128::from(v6) & mask);
                Ok(format!("{masked}/{prefix}"))
            }
        }
    }

    /// CLI `CIDRSliceCSV.Set`: splits on comma, validates each CIDR, and appends.
    pub fn set(&mut self, s: &str) -> Result<(), ConfigError> {
        for part in s.split(',') {
            self.0.push(Self::parse_cidr(part)?);
        }
        Ok(())
    }

    /// YAML `CIDRSliceCSV.UnmarshalYAML`: empty string clears, otherwise splits and validates.
    pub fn set_yaml(&mut self, s: &str) -> Result<(), ConfigError> {
        self.0.clear();
        if s.is_empty() {
            return Ok(());
        }
        self.set(s)
    }
}

impl Deref for CIDRSliceCSV {
    type Target = Vec<String>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for CIDRSliceCSV {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl fmt::Display for CIDRSliceCSV {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0.join(","))
    }
}

impl Serialize for CIDRSliceCSV {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for CIDRSliceCSV {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct CidrVisitor;
        impl<'de> Visitor<'de> for CidrVisitor {
            type Value = CIDRSliceCSV;
            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a comma-separated list of CIDRs")
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                let mut out = CIDRSliceCSV::default();
                out.set_yaml(v).map_err(E::custom)?;
                Ok(out)
            }
            fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(CIDRSliceCSV::default())
            }
        }
        deserializer.deserialize_any(CidrVisitor)
    }
}

/// Parses a YAML/Go `time.Time` timestamp string (RFC3339 / RFC3339Nano / ISO date / space-separated datetime).
pub fn parse_go_yaml_timestamp_dt(s: &str) -> Result<chrono::DateTime<chrono::Utc>, ConfigError> {
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return Ok(dt.with_timezone(&chrono::Utc));
    }
    if let Ok(d) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        if let Some(ndt) = d.and_hms_opt(0, 0, 0) {
            return Ok(ndt.and_utc());
        }
    }
    for fmt in [
        "%Y-%m-%d %H:%M:%S%.f %z",
        "%Y-%m-%d %H:%M:%S%.fZ",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S%.f",
    ] {
        if let Ok(dt) = chrono::DateTime::parse_from_str(s, fmt) {
            return Ok(dt.with_timezone(&chrono::Utc));
        }
        if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(s, fmt) {
            return Ok(ndt.and_utc());
        }
    }
    Err(ConfigError::Validation(format!(
        "cannot parse {s:?} as time.Time"
    )))
}

/// Validates a YAML/Go `time.Time` timestamp string (RFC3339 / RFC3339Nano / ISO date / space-separated datetime).
pub fn parse_go_yaml_timestamp(s: &str) -> Result<(), ConfigError> {
    parse_go_yaml_timestamp_dt(s).map(|_| ())
}

/// `flagext.LimitsMap[T]`: YAML map or JSON object on CLI.
pub type LimitsMap<T> = BTreeMap<String, T>;

/// Parses Go `strconv.ParseBool` values (`1`, `t`, `T`, `TRUE`, `true`, `True`, `0`, `f`, `F`, `FALSE`, `false`, `False`).
pub fn parse_go_bool(s: &str) -> Result<bool, ConfigError> {
    match s {
        "1" | "t" | "T" | "TRUE" | "true" | "True" => Ok(true),
        "0" | "f" | "F" | "FALSE" | "false" | "False" => Ok(false),
        _ => Err(ConfigError::Flag(format!("invalid boolean value {s:?}"))),
    }
}

#[cfg(test)]
mod s3_bucket_lookup_type_tests {
    use super::*;

    // thanos objstore s3 BucketLookupType: "auto", "virtual-hosted", "path".
    #[test]
    fn values_match_thanos() {
        for (s, v) in [
            ("auto", S3BucketLookupType::Auto),
            ("virtual-hosted", S3BucketLookupType::VirtualHost),
            ("path", S3BucketLookupType::Path),
        ] {
            assert_eq!(S3BucketLookupType::parse(s).unwrap(), v);
            assert_eq!(v.as_str(), s);
            let yaml: S3BucketLookupType = serde_yaml::from_str(s).unwrap();
            assert_eq!(yaml, v);
            assert_eq!(serde_yaml::to_string(&v).unwrap().trim(), s);
        }
        assert_eq!(
            S3BucketLookupType::parse("virtual-host").unwrap_err().to_string(),
            ConfigError::Flag("unsupported bucket lookup type: virtual-host".into()).to_string()
        );
    }
}

#[cfg(test)]
mod human_bytes_tests {
    use super::*;

    // alecthomas/units util_test.go / Go `units.Base2Bytes(n).String()`.
    #[test]
    fn display_matches_go_base2_bytes_string() {
        for (n, want) in [
            (0u64, "0B"),
            (1, "1B"),
            (1023, "1023B"),
            (1024, "1KiB"),
            (1025, "1KiB1B"),
            (65535, "63KiB1023B"),
            (1 << 20, "1MiB"),
            (104_857_600, "100MiB"),
            ((1 << 30) + (1 << 20) + 5, "1GiB1MiB5B"),
            (1 << 60, "1EiB"),
        ] {
            assert_eq!(HumanBytes(n).to_string(), want, "{n}");
            assert_eq!(HumanBytes::parse(want).unwrap(), n, "{want}");
        }
    }

    #[test]
    fn parse_matches_alecthomas_units_parse_base2_bytes() {
        for (input, want) in [
            ("0", 0u64),
            ("+0", 0),
            ("-0", 0),
            ("0B", 0),
            ("64KiB", 65_536),
            ("+64KiB", 65_536),
            ("64KB", 65_536),
            ("1.5MiB", 1_572_864),
            (".5KiB", 512),
            ("1.KiB", 1_024),
            ("1KiB512B", 1_536),
            ("1KB512B", 1_536),
            ("-1KiB", (-1024_i64) as u64),
        ] {
            assert_eq!(HumanBytes::parse(input).unwrap(), want, "input={input:?}");
        }

        for (bad, want_err) in [
            ("65536", "units: unknown unit  in 65536"),
            ("64K", "units: unknown unit K in 64K"),
            ("1M", "units: unknown unit M in 1M"),
            ("64kB", "units: unknown unit kB in 64kB"),
            (" 64KiB", "units: invalid  64KiB"),
            ("64KiB ", "units: unknown unit KiB  in 64KiB "),
            ("1KiB1MB", "units: unknown unit KiB in 1KiB1MB"),
            ("", "units: invalid "),
            ("+", "units: invalid +"),
            ("-", "units: invalid -"),
            (".KiB", "units: invalid .KiB"),
            ("99999999999999999999B", "units: invalid 99999999999999999999B"),
            ("10EB", "units: overflow parsing unit"),
            ("10EiB", "units: unknown unit EiB in 10EiB"),
        ] {
            let err = HumanBytes::parse_str(bad).unwrap_err();
            assert_eq!(err, want_err, "bad={bad:?}");
        }
    }

    #[test]
    fn yaml_deserialization_rejects_non_zero_integers_and_invalid_units() {
        assert_eq!(serde_yaml::from_str::<HumanBytes>("0").unwrap(), HumanBytes(0));
        assert_eq!(
            serde_yaml::from_str::<HumanBytes>(r#""1.5MiB""#).unwrap(),
            HumanBytes(1_572_864)
        );
        let int_err = serde_yaml::from_str::<HumanBytes>("1048576")
            .unwrap_err()
            .to_string();
        assert!(
            int_err.contains("units: unknown unit  in 1048576"),
            "unexpected err: {int_err}"
        );

        let mut bad_cfg = crate::MimirConfig::default();
        let yaml_int_err = bad_cfg
            .load_config_from_bytes(
                b"ingester_client:
  grpc_client_config:
    initial_stream_window_size: 1048576
",
                false,
            )
            .unwrap_err()
            .to_string();
        assert!(
            yaml_int_err.contains("units: unknown unit  in 1048576"),
            "unexpected yaml err: {yaml_int_err}"
        );

        let mut yaml_zero = crate::MimirConfig::default();
        yaml_zero
            .load_config_from_bytes(
                b"ingester_client:
  grpc_client_config:
    initial_stream_window_size: 0
",
                false,
            )
            .unwrap();
        assert_eq!(yaml_zero.ingester_client.grpc_client_config.initial_stream_window_size.0, 0);

        let mut yaml_frac = crate::MimirConfig::default();
        yaml_frac
            .load_config_from_bytes(
                b"ingester_client:
  grpc_client_config:
    initial_stream_window_size: 1.5MiB
",
                false,
            )
            .unwrap();
        assert_eq!(
            yaml_frac.ingester_client.grpc_client_config.initial_stream_window_size.0,
            1_572_864
        );
    }
}
