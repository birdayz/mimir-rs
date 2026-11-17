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

/// `flagext.Bytes`: human-readable base-2/base-10 byte counts (e.g. `64KiB`, `1MB`, `65535`).
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct HumanBytes(pub u64);

impl HumanBytes {
    pub fn parse(s: &str) -> Result<u64, ConfigError> {
        let mut rest = s.trim();
        if rest.is_empty() {
            return Err(ConfigError::Flag("empty byte size".to_string()));
        }
        let mut total: u64 = 0;
        while !rest.is_empty() {
            let num_end = rest
                .find(|c: char| !c.is_ascii_digit())
                .unwrap_or(rest.len());
            if num_end == 0 {
                return Err(ConfigError::Flag(format!("invalid byte size {s:?}")));
            }
            let num_str = &rest[..num_end];
            rest = &rest[num_end..];
            let val: u64 = num_str.parse().map_err(|e| {
                ConfigError::Flag(format!("invalid byte size {s:?}: {e}"))
            })?;

            let unit_end = rest
                .find(|c: char| c.is_ascii_digit())
                .unwrap_or(rest.len());
            let unit_str = rest[..unit_end].trim();
            rest = &rest[unit_end..];

            let mult: u64 = match unit_str {
                "" | "B" => 1,
                "KiB" | "KB" | "K" | "kB" => 1024,
                "MiB" | "MB" | "M" => 1024 * 1024,
                "GiB" | "GB" | "G" => 1024 * 1024 * 1024,
                "TiB" | "TB" | "T" => 1024 * 1024 * 1024 * 1024,
                "PiB" | "PB" | "P" => 1024 * 1024 * 1024 * 1024 * 1024,
                "EiB" | "EB" | "E" => 1024 * 1024 * 1024 * 1024 * 1024 * 1024,
                _ => {
                    return Err(ConfigError::Flag(format!(
                        "unknown byte unit {unit_str:?} in {s:?}"
                    )));
                }
            };
            let part = val
                .checked_mul(mult)
                .ok_or_else(|| ConfigError::Flag(format!("byte size overflow in {s:?}")))?;
            total = total
                .checked_add(part)
                .ok_or_else(|| ConfigError::Flag(format!("byte size overflow in {s:?}")))?;
        }
        Ok(total)
    }
}

impl fmt::Display for HumanBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}B", self.0)
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
                formatter.write_str("a byte size string (e.g. '64KiB') or integer")
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> Result<Self::Value, E> {
                Ok(HumanBytes(v))
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> Result<Self::Value, E> {
                u64::try_from(v)
                    .map(HumanBytes)
                    .map_err(|_| E::custom(format!("negative byte size: {v}")))
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                HumanBytes::parse(v).map(HumanBytes).map_err(E::custom)
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
                "unrecognized label_value_length_over_limit_strategy {s:?}"
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
            | "underscore_escaping_with_suffixes"
            | "underscore_escaping_without_suffixes"
            | "no_utf8_escaping_with_suffixes"
            | "no_translation" => Ok(Self(s.to_string())),
            _ => Err(ConfigError::Flag(format!(
                "unrecognized OTel translation strategy {s:?}"
            ))),
        }
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
    pub fn parse(s: &str) -> Result<Self, ConfigError> {
        match s {
            "" | "unset" => Ok(Self::Unset),
            "legacy" => Ok(Self::Legacy),
            "utf8" => Ok(Self::Utf8),
            _ => Err(ConfigError::Flag(format!(
                "unrecognized name validation scheme: {s}"
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
            Self::Unset => serializer.serialize_i32(0),
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
            fn visit_i64<E: de::Error>(self, v: i64) -> Result<Self::Value, E> {
                match v {
                    0 => Ok(NameValidationScheme::Unset),
                    1 => Ok(NameValidationScheme::Legacy),
                    2 => Ok(NameValidationScheme::Utf8),
                    _ => Err(E::custom(format!("unrecognized name validation scheme: {v}"))),
                }
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> Result<Self::Value, E> {
                self.visit_i64(v as i64)
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

impl SASLMechanism {
    pub fn parse(s: &str) -> Result<Self, ConfigError> {
        match s {
            "PLAIN" => Ok(Self::Plain),
            "SCRAM-SHA-256" => Ok(Self::ScramSha256),
            "SCRAM-SHA-512" => Ok(Self::ScramSha512),
            "OAUTHBEARER" => Ok(Self::Oauthbearer),
            "AWS_MSK_IAM" => Ok(Self::MskIam),
            _ => Err(ConfigError::Flag(
                "the configured SASL mechanism is invalid, must be one of: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER, AWS_MSK_IAM".to_string(),
            )),
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
