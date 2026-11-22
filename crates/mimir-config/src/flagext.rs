//! Additional dskit `flagext` types, env-var binding, flag parsing, and flag registration tracking.
//!
//! Ported from:
//! - github.com/grafana/dskit/flagext/day.go
//! - github.com/grafana/dskit/flagext/env.go
//! - github.com/grafana/dskit/flagext/map.go
//! - github.com/grafana/dskit/flagext/parse.go
//! - github.com/grafana/dskit/flagext/register.go
//! - github.com/grafana/dskit/flagext/time.go
//! - github.com/grafana/dskit/flagext/url.go

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, Utc};
use serde::de::{self, Deserializer};
use serde::ser::Serializer;
use serde::{Deserialize, Serialize};

use crate::error::ConfigError;
use crate::flags::FlagSet;

const SECONDS_IN_DAY: i64 = 24 * 60 * 60;

// ---------------------------------------------------------------------------------------------
// flagext/day.go
// ---------------------------------------------------------------------------------------------

/// `flagext.DayValue`: a UTC midnight day value usable as a flag and YAML field.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct DayValue {
    unix_secs: i64,
    set: bool,
}

impl DayValue {
    /// `NewDayValue`: rounds `unix_secs` down to the nearest UTC midnight.
    pub fn new(unix_secs: i64) -> Self {
        Self {
            unix_secs: unix_secs.div_euclid(SECONDS_IN_DAY) * SECONDS_IN_DAY,
            set: true,
        }
    }

    pub fn unix_secs(&self) -> i64 {
        self.unix_secs
    }

    pub fn is_set(&self) -> bool {
        self.set
    }

    pub fn set(&mut self, s: &str) -> Result<(), ConfigError> {
        let date = NaiveDate::parse_from_str(s, "%Y-%m-%d")
            .map_err(|e| ConfigError::Flag(format!("failed to parse day {s:?}: {e}")))?;
        let dt = date
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| ConfigError::Flag(format!("invalid date {s:?}")))?
            .and_utc();
        self.unix_secs = dt.timestamp();
        self.set = true;
        Ok(())
    }

    fn format_day(&self) -> String {
        let dt = DateTime::<Utc>::from_timestamp(self.unix_secs, 0).unwrap_or(DateTime::UNIX_EPOCH);
        dt.format("%Y-%m-%d").to_string()
    }
}

impl fmt::Display for DayValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let dt = DateTime::<Utc>::from_timestamp(self.unix_secs, 0).unwrap_or(DateTime::UNIX_EPOCH);
        f.write_str(&dt.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
    }
}

impl Serialize for DayValue {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.format_day())
    }
}

impl<'de> Deserialize<'de> for DayValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        let mut out = DayValue::default();
        out.set(&s).map_err(de::Error::custom)?;
        Ok(out)
    }
}

// ---------------------------------------------------------------------------------------------
// flagext/time.go
// ---------------------------------------------------------------------------------------------

/// `flagext.Time`: a timestamp usable as a flag or in YAML config (`"0"` means zero time).
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct FlagTime(pub Option<DateTime<FixedOffset>>);

impl FlagTime {
    pub fn is_zero(&self) -> bool {
        self.0.is_none()
    }

    pub fn set(&mut self, s: &str) -> Result<(), ConfigError> {
        if s == "0" {
            self.0 = None;
            return Ok(());
        }
        let utc = FixedOffset::east_opt(0).expect("UTC offset");
        if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
            if let Some(ndt) = d.and_hms_opt(0, 0, 0) {
                self.0 = Some(DateTime::from_naive_utc_and_offset(ndt, utc));
                return Ok(());
            }
        }
        if let Ok(ndt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M") {
            self.0 = Some(DateTime::from_naive_utc_and_offset(ndt, utc));
            return Ok(());
        }
        if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
            self.0 = Some(dt);
            return Ok(());
        }
        Err(ConfigError::Flag(format!("failed to parse time: {s:?}")))
    }
}

impl fmt::Display for FlagTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            None => f.write_str("0"),
            Some(dt) => f.write_str(&dt.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)),
        }
    }
}

impl Serialize for FlagTime {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for FlagTime {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        let mut out = FlagTime::default();
        out.set(&s).map_err(de::Error::custom)?;
        Ok(out)
    }
}

// ---------------------------------------------------------------------------------------------
// flagext/url.go
// ---------------------------------------------------------------------------------------------

/// `flagext.URLValue`: a URL usable as a flag and YAML field, masking passwords on YAML marshal.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct URLValue {
    raw: Option<String>,
}

impl URLValue {
    pub fn set(&mut self, s: &str) -> Result<(), ConfigError> {
        // Validate with reqwest::Url if absolute; Go's net/url.Parse also accepts relative paths.
        if s.contains("://") {
            reqwest::Url::parse(s).map_err(|e| ConfigError::Flag(e.to_string()))?;
        }
        self.raw = Some(s.to_string());
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.raw.is_none()
    }

    /// Matches Go `url.URL.Redacted()`: masks any password in userinfo with `"xxxxx"`.
    pub fn redacted(&self) -> String {
        let Some(ref raw) = self.raw else {
            return String::new();
        };
        let Some(scheme_end) = raw.find("://") else {
            return raw.clone();
        };
        let after_scheme = scheme_end + 3;
        let authority_end = raw[after_scheme..]
            .find(['/', '?', '#'])
            .map_or(raw.len(), |idx| after_scheme + idx);
        let authority = &raw[after_scheme..authority_end];
        let Some(at_pos) = authority.rfind('@') else {
            return raw.clone();
        };
        let userinfo = &authority[..at_pos];
        let Some(colon_pos) = userinfo.find(':') else {
            return raw.clone();
        };
        let mut out = String::with_capacity(raw.len());
        out.push_str(&raw[..after_scheme + colon_pos + 1]);
        out.push_str("xxxxx");
        out.push_str(&raw[after_scheme + at_pos..]);
        out
    }
}

impl fmt::Display for URLValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.raw.as_deref().unwrap_or(""))
    }
}

impl Serialize for URLValue {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.redacted())
    }
}

impl<'de> Deserialize<'de> for URLValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        let mut out = URLValue::default();
        if !s.is_empty() {
            out.set(&s).map_err(de::Error::custom)?;
        }
        Ok(out)
    }
}

// ---------------------------------------------------------------------------------------------
// flagext/map.go
// ---------------------------------------------------------------------------------------------

pub type LimitsMapValidator<T> = Arc<dyn Fn(&str, &T) -> Result<(), String> + Send + Sync>;

/// `flagext.LimitsMap[T]`: validated map supporting JSON CLI flags, YAML marshaling/unmarshaling,
/// initialization tracking, deep cloning, and atomic validation on update.
pub struct ValidatedLimitsMap<T> {
    data: Option<BTreeMap<String, T>>,
    validator: Option<LimitsMapValidator<T>>,
}

impl<T: fmt::Debug> fmt::Debug for ValidatedLimitsMap<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ValidatedLimitsMap")
            .field("data", &self.data)
            .finish()
    }
}

impl<T: Clone> Clone for ValidatedLimitsMap<T> {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            validator: self.validator.clone(),
        }
    }
}

impl<T: PartialEq> PartialEq for ValidatedLimitsMap<T> {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
    }
}

impl<T: Eq> Eq for ValidatedLimitsMap<T> {}

impl<T> Default for ValidatedLimitsMap<T> {
    fn default() -> Self {
        Self {
            data: Some(BTreeMap::new()),
            validator: None,
        }
    }
}

impl<T> ValidatedLimitsMap<T> {
    pub fn new(validator: Option<LimitsMapValidator<T>>) -> Self {
        Self::new_with_data(Some(BTreeMap::new()), validator)
    }

    pub fn new_with_data(
        data: Option<BTreeMap<String, T>>,
        validator: Option<LimitsMapValidator<T>>,
    ) -> Self {
        Self { data, validator }
    }

    pub fn uninitialized() -> Self {
        Self {
            data: None,
            validator: None,
        }
    }

    pub fn is_initialized(&self) -> bool {
        self.data.is_some()
    }

    pub fn read(&self) -> Option<&BTreeMap<String, T>> {
        self.data.as_ref()
    }

    pub fn read_mut(&mut self) -> Option<&mut BTreeMap<String, T>> {
        self.data.as_mut()
    }

    pub fn update_map(&mut self, new_map: BTreeMap<String, T>) -> Result<(), String> {
        if let Some(ref validator) = self.validator {
            for (k, v) in &new_map {
                validator(k, v)?;
            }
        }
        let target = self.data.get_or_insert_with(BTreeMap::new);
        target.clear();
        target.extend(new_map);
        Ok(())
    }
}

impl<T> ValidatedLimitsMap<T>
where
    T: serde::de::DeserializeOwned,
{
    pub fn set(&mut self, s: &str) -> Result<(), String> {
        let new_map: BTreeMap<String, T> = serde_json::from_str(s).map_err(|e| e.to_string())?;
        self.update_map(new_map)
    }

    pub fn unmarshal_yaml(&mut self, input: &str) -> Result<(), String> {
        let new_map: BTreeMap<String, T> = serde_yaml::from_str(input).map_err(|e| e.to_string())?;
        self.update_map(new_map)
    }
}

impl<T: Serialize> fmt::Display for ValidatedLimitsMap<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.data {
            None => f.write_str("null"),
            Some(map) => match serde_json::to_value(map) {
                Ok(mut val) => {
                    normalize_json_integral_floats(&mut val);
                    match serde_json::to_string(&val) {
                        Ok(s) => f.write_str(&s),
                        Err(e) => write!(f, "failed to marshal: {e}"),
                    }
                }
                Err(e) => write!(f, "failed to marshal: {e}"),
            },
        }
    }
}

fn normalize_json_integral_floats(v: &mut serde_json::Value) {
    match v {
        serde_json::Value::Number(n) => {
            if let Some(f) = n.as_f64() {
                if f.fract() == 0.0 && f >= i64::MIN as f64 && f <= i64::MAX as f64 {
                    *n = serde_json::Number::from(f as i64);
                }
            }
        }
        serde_json::Value::Array(arr) => arr.iter_mut().for_each(normalize_json_integral_floats),
        serde_json::Value::Object(obj) => obj.values_mut().for_each(normalize_json_integral_floats),
        _ => {}
    }
}

impl<T: Serialize> Serialize for ValidatedLimitsMap<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.data.serialize(serializer)
    }
}

impl<'de, T: serde::de::DeserializeOwned> Deserialize<'de> for ValidatedLimitsMap<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let map = Option::<BTreeMap<String, T>>::deserialize(deserializer)?;
        Ok(Self {
            data: map,
            validator: None,
        })
    }
}

// ---------------------------------------------------------------------------------------------
// flagext/env.go, flagext/parse.go, flagext/register.go
// ---------------------------------------------------------------------------------------------

/// Converts a flag name and prefix to its corresponding environment variable name
/// (`dskit/flagext.FlagToEnvVar`).
pub fn flag_to_env_var(prefix: &str, flag_name: &str) -> String {
    let env_var = flag_name.to_ascii_uppercase().replace(['.', '-'], "_");
    if !prefix.is_empty() {
        let norm_prefix = prefix.to_ascii_uppercase().replace(['.', '-'], "_");
        format!("{norm_prefix}_{env_var}")
    } else {
        env_var
    }
}

/// `flagext.ParseFlagsAndArgumentsWithEnv`.
pub fn parse_flags_and_arguments_with_env<'a, T>(
    fs: &mut FlagSet<T>,
    target: &mut T,
    args: &'a [String],
    prefix: &str,
) -> Result<&'a [String], ConfigError> {
    let rest = fs.parse(target, args)?;
    fs.set_flags_from_env(target, prefix)?;
    Ok(rest)
}

/// `flagext.ParseFlagsWithoutArgumentsWithEnv`.
pub fn parse_flags_without_arguments_with_env<T>(
    fs: &mut FlagSet<T>,
    target: &mut T,
    args: &[String],
    prefix: &str,
) -> Result<(), ConfigError> {
    let rest = fs.parse(target, args)?;
    fs.set_flags_from_env(target, prefix)?;
    if !rest.is_empty() {
        return Err(ConfigError::Flag(format!(
            "the command does not support any argument, but some were provided: {}",
            rest.join(" ")
        )));
    }
    Ok(())
}

/// `flagext.RegisteredFlags`: tracks flags registered by a callback under a given prefix.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegisteredFlags {
    pub prefix: String,
    /// Map of unprefixed flag name -> full registered flag name.
    pub flags: BTreeMap<String, String>,
}

/// `flagext.TrackRegisteredFlags`.
pub fn track_registered_flags<T>(
    prefix: &str,
    fs: &mut FlagSet<T>,
    register: impl FnOnce(&str, &mut FlagSet<T>),
) -> RegisteredFlags {
    let old: BTreeSet<String> = fs.flag_names().map(str::to_string).collect();
    register(prefix, fs);
    let mut flags = BTreeMap::new();
    for name in fs.flag_names() {
        if let Some(stripped) = name.strip_prefix(prefix) {
            if !old.contains(name) {
                flags.insert(stripped.to_string(), name.to_string());
            }
        }
    }
    RegisteredFlags {
        prefix: prefix.to_string(),
        flags,
    }
}
