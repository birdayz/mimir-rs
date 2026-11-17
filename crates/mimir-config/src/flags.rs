//! Go `flag.FlagSet` compatible CLI parser, main flags, `-version`, `-modules`, and `-help` formatters.
//!
//! Ported from:
//! - Go stdlib `flag` package
//! - github.com/grafana/mimir/cmd/mimir/main.go
//! - github.com/grafana/mimir/pkg/mimir/modules.go
//! - github.com/grafana/mimir/pkg/util/version/info.go

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use crate::duration::{GoDuration, ModelDuration};
use crate::error::ConfigError;
use crate::types::{
    parse_go_bool, HumanBytes, LabelValueLengthOverLimitStrategy, LimitsMap, LogLevel,
    NameValidationScheme, OTelTranslationStrategy, S3BucketLookupType, SASLMechanism,
    StringSliceCSV, StringSliceCSVMulti,
};

type FlagSetter<T> = Box<dyn FnMut(&mut T, &str) -> Result<(), ConfigError>>;
type FlagGetter<T> = Box<dyn Fn(&T) -> String>;

/// Metadata and callbacks for a single CLI flag operating on target `T`.
pub struct FlagEntry<T> {
    pub name: String,
    pub is_bool: bool,
    pub type_name: &'static str,
    pub default_str: String,
    pub usage: String,
    /// Go only quotes `-help` defaults of flags whose value kind is `reflect.String`; e.g.
    /// `*flagext.StringSlice` renders as type "string" but with an unquoted default.
    pub usage_default_unquoted: bool,
    pub was_set: bool,
    setter: FlagSetter<T>,
    getter: FlagGetter<T>,
}

/// Go `flag.FlagSet` compatible flag registry and command-line parser.
pub struct FlagSet<T> {
    flags: BTreeMap<String, FlagEntry<T>>,
}

impl<T> Default for FlagSet<T> {
    fn default() -> Self {
        Self {
            flags: BTreeMap::new(),
        }
    }
}

impl<T> FlagSet<T> {
    pub fn new() -> Self {
        Self::default()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn register_raw<S, G>(
        &mut self,
        name: impl Into<String>,
        is_bool: bool,
        type_name: &'static str,
        default_str: impl Into<String>,
        usage: impl Into<String>,
        setter: S,
        getter: G,
    ) where
        S: FnMut(&mut T, &str) -> Result<(), ConfigError> + 'static,
        G: Fn(&T) -> String + 'static,
    {
        let name = name.into();
        self.flags.insert(
            name.clone(),
            FlagEntry {
                name,
                is_bool,
                type_name,
                default_str: default_str.into(),
                usage: usage.into(),
                usage_default_unquoted: false,
                was_set: false,
                setter: Box::new(setter),
                getter: Box::new(getter),
            },
        );
    }

    /// Overrides how an already registered flag is rendered by `-help`: the type name
    /// (`usage.getFlagName`) and the default (`usage.getFlagDefault`, i.e. a struct field's
    /// `doc:"default=..."` tag). The flag's real default value is unaffected.
    pub fn set_usage_default(&mut self, name: &str, type_name: &'static str, default_str: &str) {
        if let Some(entry) = self.flags.get_mut(name) {
            entry.type_name = type_name;
            entry.default_str = default_str.to_string();
            entry.usage_default_unquoted = true;
        }
    }

    pub fn bool_var(
        &mut self,
        name: &str,
        default_val: bool,
        usage: &str,
        get: impl Fn(&T) -> &bool + 'static,
        get_mut: impl Fn(&mut T) -> &mut bool + 'static,
    ) {
        self.register_raw(
            name,
            true,
            "",
            default_val.to_string(),
            usage,
            move |t, s| {
                *get_mut(t) = parse_go_bool(s)?;
                Ok(())
            },
            move |t| (*get(t)).to_string(),
        );
    }

    pub fn opt_bool_var(
        &mut self,
        name: &str,
        default_val: bool,
        usage: &str,
        get: impl Fn(&T) -> &Option<bool> + 'static,
        get_mut: impl Fn(&mut T) -> &mut Option<bool> + 'static,
    ) {
        self.register_raw(
            name,
            true,
            "",
            default_val.to_string(),
            usage,
            move |t, s| {
                *get_mut(t) = Some(parse_go_bool(s)?);
                Ok(())
            },
            move |t| get(t).unwrap_or(false).to_string(),
        );
    }

    pub fn string_var(
        &mut self,
        name: &str,
        default_val: &str,
        usage: &str,
        get: impl Fn(&T) -> &String + 'static,
        get_mut: impl Fn(&mut T) -> &mut String + 'static,
    ) {
        self.register_raw(
            name,
            false,
            "string",
            default_val.to_string(),
            usage,
            move |t, s| {
                *get_mut(t) = s.to_string();
                Ok(())
            },
            move |t| get(t).clone(),
        );
    }

    pub fn i64_var(
        &mut self,
        name: &str,
        default_val: i64,
        usage: &str,
        get: impl Fn(&T) -> &i64 + 'static,
        get_mut: impl Fn(&mut T) -> &mut i64 + 'static,
    ) {
        self.register_raw(
            name,
            false,
            "int",
            default_val.to_string(),
            usage,
            move |t, s| {
                *get_mut(t) = go_parse_int(s)?;
                Ok(())
            },
            move |t| (*get(t)).to_string(),
        );
    }

    pub fn u64_var(
        &mut self,
        name: &str,
        default_val: u64,
        usage: &str,
        get: impl Fn(&T) -> &u64 + 'static,
        get_mut: impl Fn(&mut T) -> &mut u64 + 'static,
    ) {
        self.register_raw(
            name,
            false,
            "uint",
            default_val.to_string(),
            usage,
            move |t, s| {
                *get_mut(t) = go_parse_uint(s)?;
                Ok(())
            },
            move |t| (*get(t)).to_string(),
        );
    }

    pub fn f64_var(
        &mut self,
        name: &str,
        default_val: f64,
        usage: &str,
        get: impl Fn(&T) -> &f64 + 'static,
        get_mut: impl Fn(&mut T) -> &mut f64 + 'static,
    ) {
        self.register_raw(
            name,
            false,
            "float",
            default_val.to_string(),
            usage,
            move |t, s| {
                *get_mut(t) = go_parse_float(s)?;
                Ok(())
            },
            move |t| (*get(t)).to_string(),
        );
    }

    pub fn go_duration_var(
        &mut self,
        name: &str,
        default_val: GoDuration,
        usage: &str,
        get: impl Fn(&T) -> &GoDuration + 'static,
        get_mut: impl Fn(&mut T) -> &mut GoDuration + 'static,
    ) {
        self.register_raw(
            name,
            false,
            "duration",
            default_val.to_string(),
            usage,
            move |t, s| {
                // flag.durationValue.Set maps every time.ParseDuration error to errParse.
                *get_mut(t) = s.parse().map_err(|_| parse_error())?;
                Ok(())
            },
            move |t| (*get(t)).to_string(),
        );
    }

    pub fn model_duration_var(
        &mut self,
        name: &str,
        default_val: ModelDuration,
        usage: &str,
        get: impl Fn(&T) -> &ModelDuration + 'static,
        get_mut: impl Fn(&mut T) -> &mut ModelDuration + 'static,
    ) {
        self.register_raw(
            name,
            false,
            "duration",
            default_val.to_string(),
            usage,
            move |t, s| {
                *get_mut(t) = s.parse()?;
                Ok(())
            },
            move |t| (*get(t)).to_string(),
        );
    }

    pub fn string_slice_csv_var(
        &mut self,
        name: &str,
        default_val: &StringSliceCSV,
        usage: &str,
        get: impl Fn(&T) -> &StringSliceCSV + 'static,
        get_mut: impl Fn(&mut T) -> &mut StringSliceCSV + 'static,
    ) {
        self.register_raw(
            name,
            false,
            "value",
            default_val.to_string(),
            usage,
            move |t, s| {
                get_mut(t).set(s);
                Ok(())
            },
            move |t| get(t).to_string(),
        );
    }

    pub fn string_slice_csv_multi_var(
        &mut self,
        name: &str,
        default_val: &StringSliceCSVMulti,
        usage: &str,
        get: impl Fn(&T) -> &StringSliceCSVMulti + 'static,
        get_mut: impl Fn(&mut T) -> &mut StringSliceCSVMulti + 'static,
    ) {
        self.register_raw(
            name,
            false,
            "value",
            default_val.to_string(),
            usage,
            move |t, s| {
                get_mut(t).append_csv(s);
                Ok(())
            },
            move |t| get(t).to_string(),
        );
    }

    pub fn string_slice_var(
        &mut self,
        name: &str,
        default_val: &[String],
        usage: &str,
        get: impl Fn(&T) -> &Vec<String> + 'static,
        get_mut: impl Fn(&mut T) -> &mut Vec<String> + 'static,
    ) {
        self.register_raw(
            name,
            false,
            "value",
            format!("[{}]", default_val.join(" ")),
            usage,
            move |t, s| {
                get_mut(t).push(s.to_string());
                Ok(())
            },
            move |t| format!("[{}]", get(t).join(" ")),
        );
    }

    pub fn human_bytes_var(
        &mut self,
        name: &str,
        default_val: HumanBytes,
        usage: &str,
        get: impl Fn(&T) -> &HumanBytes + 'static,
        get_mut: impl Fn(&mut T) -> &mut HumanBytes + 'static,
    ) {
        self.register_raw(
            name,
            false,
            "value",
            default_val.to_string(),
            usage,
            move |t, s| {
                *get_mut(t) = HumanBytes(HumanBytes::parse(s)?);
                Ok(())
            },
            move |t| (*get(t)).to_string(),
        );
    }

    pub fn log_level_var(
        &mut self,
        name: &str,
        default_val: LogLevel,
        usage: &str,
        get: impl Fn(&T) -> &LogLevel + 'static,
        get_mut: impl Fn(&mut T) -> &mut LogLevel + 'static,
    ) {
        self.register_raw(
            name,
            false,
            "value",
            default_val.to_string(),
            usage,
            move |t, s| {
                *get_mut(t) = LogLevel::parse(s)?;
                Ok(())
            },
            move |t| (*get(t)).to_string(),
        );
    }

    pub fn label_value_strategy_var(
        &mut self,
        name: &str,
        default_val: LabelValueLengthOverLimitStrategy,
        usage: &str,
        get: impl Fn(&T) -> &LabelValueLengthOverLimitStrategy + 'static,
        get_mut: impl Fn(&mut T) -> &mut LabelValueLengthOverLimitStrategy + 'static,
    ) {
        self.register_raw(
            name,
            false,
            "value",
            default_val.to_string(),
            usage,
            move |t, s| {
                *get_mut(t) = LabelValueLengthOverLimitStrategy::parse(s)?;
                Ok(())
            },
            move |t| (*get(t)).to_string(),
        );
    }

    pub fn otel_strategy_var(
        &mut self,
        name: &str,
        default_val: &OTelTranslationStrategy,
        usage: &str,
        get: impl Fn(&T) -> &OTelTranslationStrategy + 'static,
        get_mut: impl Fn(&mut T) -> &mut OTelTranslationStrategy + 'static,
    ) {
        self.register_raw(
            name,
            false,
            "value",
            default_val.to_string(),
            usage,
            move |t, s| {
                *get_mut(t) = OTelTranslationStrategy::parse(s)?;
                Ok(())
            },
            move |t| get(t).to_string(),
        );
    }

    pub fn name_validation_scheme_var(
        &mut self,
        name: &str,
        default_val: NameValidationScheme,
        usage: &str,
        get: impl Fn(&T) -> &NameValidationScheme + 'static,
        get_mut: impl Fn(&mut T) -> &mut NameValidationScheme + 'static,
    ) {
        self.register_raw(
            name,
            false,
            "value",
            default_val.to_string(),
            usage,
            move |t, s| {
                *get_mut(t) = NameValidationScheme::parse(s)?;
                Ok(())
            },
            move |t| (*get(t)).to_string(),
        );
    }

    pub fn s3_bucket_lookup_type_var(
        &mut self,
        name: &str,
        default_val: S3BucketLookupType,
        usage: &str,
        get: impl Fn(&T) -> &S3BucketLookupType + 'static,
        get_mut: impl Fn(&mut T) -> &mut S3BucketLookupType + 'static,
    ) {
        self.register_raw(
            name,
            false,
            "value",
            default_val.to_string(),
            usage,
            move |t, s| {
                *get_mut(t) = S3BucketLookupType::parse(s)?;
                Ok(())
            },
            move |t| (*get(t)).to_string(),
        );
    }

    pub fn sasl_mechanism_var(
        &mut self,
        name: &str,
        default_val: SASLMechanism,
        usage: &str,
        get: impl Fn(&T) -> &SASLMechanism + 'static,
        get_mut: impl Fn(&mut T) -> &mut SASLMechanism + 'static,
    ) {
        self.register_raw(
            name,
            false,
            "string",
            default_val.to_string(),
            usage,
            move |t, s| {
                *get_mut(t) = SASLMechanism::parse(s)?;
                Ok(())
            },
            move |t| (*get(t)).to_string(),
        );
    }

    pub fn limits_map_var<V>(
        &mut self,
        name: &str,
        default_val: &LimitsMap<V>,
        usage: &str,
        get: impl Fn(&T) -> &LimitsMap<V> + 'static,
        get_mut: impl Fn(&mut T) -> &mut LimitsMap<V> + 'static,
    ) where
        V: serde::de::DeserializeOwned + serde::Serialize + 'static,
    {
        let def = serde_json::to_string(default_val).unwrap_or_else(|_| "{}".to_string());
        self.register_raw(
            name,
            false,
            "value",
            def,
            usage,
            move |t, s| {
                let m: LimitsMap<V> = serde_json::from_str(s)
                    .map_err(|e| ConfigError::Flag(format!("invalid JSON map: {e}")))?;
                *get_mut(t) = m;
                Ok(())
            },
            move |t| serde_json::to_string(get(t)).unwrap_or_else(|_| "{}".to_string()),
        );
    }

    /// Registers an ignored flag (e.g. `-config.file`, `-config.expand-env`, parsed in pre-scan).
    pub fn ignored_flag(&mut self, name: &str, is_bool: bool, usage: &str) {
        self.register_raw(
            name,
            is_bool,
            if is_bool { "" } else { "string" },
            if is_bool { "false" } else { "" },
            usage,
            |_, _| Ok(()),
            |_| String::new(),
        );
    }

    pub fn is_set(&self, name: &str) -> bool {
        self.flags.get(name).is_some_and(|f| f.was_set)
    }

    pub fn get_value(&self, target: &T, name: &str) -> Option<String> {
        self.flags.get(name).map(|f| (f.getter)(target))
    }

    pub fn set_value(&mut self, target: &mut T, name: &str, val: &str) -> Result<(), ConfigError> {
        let entry = self
            .flags
            .get_mut(name)
            .ok_or_else(|| ConfigError::Flag(format!("flag provided but not defined: -{name}")))?;
        (entry.setter)(target, val)
    }

    pub fn set_flags(&self) -> BTreeSet<String> {
        self.flags
            .values()
            .filter(|f| f.was_set)
            .map(|f| f.name.clone())
            .collect()
    }

    /// Parses command-line arguments (not including argv[0]), returning remaining positional args.
    /// Semantics match Go `flag.FlagSet.Parse` 1:1.
    pub fn parse<'a>(
        &mut self,
        target: &mut T,
        args: &'a [String],
    ) -> Result<&'a [String], ConfigError> {
        let mut idx = 0;
        while idx < args.len() {
            let s = &args[idx];
            if s.len() < 2 || !s.starts_with('-') {
                return Ok(&args[idx..]);
            }
            let mut num_minuses = 1;
            if s.as_bytes()[1] == b'-' {
                num_minuses += 1;
                if s.len() == 2 {
                    return Ok(&args[idx + 1..]);
                }
            }
            let mut name = &s[num_minuses..];
            if name.is_empty() || name.starts_with('-') || name.starts_with('=') {
                return Err(ConfigError::Flag(format!("bad flag syntax: {s}")));
            }

            idx += 1;
            let mut has_value = false;
            let mut value = "";
            if let Some(eq_pos) = name.find('=') {
                value = &name[eq_pos + 1..];
                name = &name[..eq_pos];
                has_value = true;
            }

            let entry = self.flags.get_mut(name).ok_or_else(|| {
                ConfigError::Flag(format!("flag provided but not defined: -{name}"))
            })?;

            if entry.is_bool {
                if has_value {
                    (entry.setter)(target, value).map_err(|_| {
                        // flag.boolValue.Set maps every strconv.ParseBool error to errParse.
                        ConfigError::Flag(format!(
                            "invalid boolean value {value:?} for -{name}: parse error"
                        ))
                    })?;
                } else {
                    (entry.setter)(target, "true").map_err(|e| {
                        ConfigError::Flag(format!(
                            "invalid boolean flag {name}: {}",
                            flag_set_error_text(&e)
                        ))
                    })?;
                }
            } else {
                let v = if has_value {
                    value
                } else if idx < args.len() {
                    let next = args[idx].as_str();
                    idx += 1;
                    next
                } else {
                    return Err(ConfigError::Flag(format!(
                        "flag needs an argument: -{name}"
                    )));
                };
                (entry.setter)(target, v).map_err(|e| {
                    ConfigError::Flag(format!(
                        "invalid value {v:?} for flag -{name}: {}",
                        flag_set_error_text(&e)
                    ))
                })?;
            }

            entry.was_set = true;
        }
        Ok(&args[idx..])
    }

    /// Formats `-help` / `-help-all` output matching `pkg/util/usage/usage.go:Usage`
    /// (see [`crate::help`]), restricted to the flags registered in this set.
    pub fn format_usage(&self, prog_name: &str, all: bool) -> String {
        crate::help::format_usage(prog_name, all, |name| self.flags.contains_key(name))
    }

    /// Names of all registered flags.
    pub fn flag_names(&self) -> impl Iterator<Item = &str> {
        self.flags.keys().map(String::as_str)
    }
}

/// Text of a `flag.Value.Set` error as Go prints it (`%v`), without the Rust-side
/// `ConfigError` display prefix.
fn flag_set_error_text(e: &ConfigError) -> String {
    match e {
        ConfigError::Flag(msg) => msg.clone(),
        other => other.to_string(),
    }
}

/// Go `flag` package `errParse`.
fn parse_error() -> ConfigError {
    ConfigError::Flag("parse error".to_string())
}

/// Go `flag` package `errRange`.
fn range_error() -> ConfigError {
    ConfigError::Flag("value out of range".to_string())
}

/// Go `strconv.underscoreOK`: underscores must separate digits (a base prefix counts as a
/// digit), never doubled or trailing.
fn go_underscore_ok(s: &str) -> bool {
    let s = s.strip_prefix(['-', '+']).unwrap_or(s);
    let b = s.as_bytes();
    let mut saw = b'^';
    let mut i = 0;
    let mut hex = false;
    if b.len() >= 2 && b[0] == b'0' && matches!(b[1].to_ascii_lowercase(), b'b' | b'o' | b'x') {
        i = 2;
        saw = b'0';
        hex = b[1].eq_ignore_ascii_case(&b'x');
    }
    while i < b.len() {
        let c = b[i];
        if c.is_ascii_digit() || (hex && (b'a'..=b'f').contains(&c.to_ascii_lowercase())) {
            saw = b'0';
        } else if c == b'_' {
            if saw != b'0' {
                return false;
            }
            saw = b'_';
        } else {
            if saw == b'_' {
                return false;
            }
            saw = b'!';
        }
        i += 1;
    }
    saw != b'_'
}

/// Splits Go `strconv.ParseInt(s, 0, 64)` input into (negative, base, digits), handling the
/// sign, `0x`/`0o`/`0b`/leading-`0` prefixes and `_` separators.
fn go_int_literal(s: &str, allow_sign: bool) -> Option<(bool, u32, String)> {
    let (neg, body) = match s.as_bytes().first() {
        Some(b'-') if allow_sign => (true, &s[1..]),
        Some(b'+') if allow_sign => (false, &s[1..]),
        _ => (false, s),
    };
    if body.is_empty() {
        return None;
    }
    let lower = body.to_ascii_lowercase();
    let (base, digits) = if let Some(d) = lower.strip_prefix("0x") {
        (16, d)
    } else if let Some(d) = lower.strip_prefix("0o") {
        (8, d)
    } else if let Some(d) = lower.strip_prefix("0b") {
        (2, d)
    } else if lower.len() > 1 && lower.starts_with('0') {
        (8, &lower[1..])
    } else {
        (10, lower.as_str())
    };
    if digits.contains('_') && !go_underscore_ok(body) {
        return None;
    }
    let digits: String = digits.chars().filter(|c| *c != '_').collect();
    if digits.is_empty() || !digits.chars().all(|c| c.is_digit(base)) {
        return None;
    }
    Some((neg, base, digits))
}

/// Go `flag.IntVar`/`Int64Var` parsing (`strconv.ParseInt(s, 0, 64)` + `numError`).
pub fn go_parse_int(s: &str) -> Result<i64, ConfigError> {
    let (neg, base, digits) = go_int_literal(s, true).ok_or_else(parse_error)?;
    let mag = u128::from_str_radix(&digits, base).map_err(|_| range_error())?;
    let v: i128 = if neg { -(mag as i128) } else { mag as i128 };
    i64::try_from(v).map_err(|_| range_error())
}

/// Go `flag.UintVar`/`Uint64Var` parsing (`strconv.ParseUint(s, 0, 64)` + `numError`).
pub fn go_parse_uint(s: &str) -> Result<u64, ConfigError> {
    let (_, base, digits) = go_int_literal(s, false).ok_or_else(parse_error)?;
    let mag = u128::from_str_radix(&digits, base).map_err(|_| range_error())?;
    u64::try_from(mag).map_err(|_| range_error())
}

/// Go `flag.Float64Var` parsing (`strconv.ParseFloat(s, 64)` + `numError`).
pub fn go_parse_float(s: &str) -> Result<f64, ConfigError> {
    let lower = s.to_ascii_lowercase();
    let unsigned = lower.trim_start_matches(['+', '-']);
    if matches!(unsigned, "inf" | "infinity" | "nan") {
        return s.parse::<f64>().map_err(|_| parse_error());
    }
    if s.contains('_') && !go_underscore_ok(s) {
        return Err(parse_error());
    }
    let v: f64 = s.replace('_', "").parse().map_err(|_| parse_error())?;
    if v.is_infinite() {
        return Err(range_error());
    }
    Ok(v)
}

/// Scans `args` for `-config.file` and `-config.expand-env` before full flag parsing,
/// matching `parseConfigFileParameter` in `cmd/mimir/main.go`.
pub fn parse_config_file_parameter(args: &[String]) -> (String, bool) {
    #[derive(Default)]
    struct ConfigScan {
        config_file: String,
        expand_env: bool,
    }

    let mut scan = ConfigScan::default();
    for offset in 0..args.len() {
        let mut fs: FlagSet<ConfigScan> = FlagSet::new();
        fs.string_var(
            "config.file",
            "",
            "",
            |s| &s.config_file,
            |s| &mut s.config_file,
        );
        fs.bool_var(
            "config.expand-env",
            false,
            "",
            |s| &s.expand_env,
            |s| &mut s.expand_env,
        );
        let _ = fs.parse(&mut scan, &args[offset..]);
    }

    (scan.config_file, scan.expand_env)
}

/// Process-level flags from `cmd/mimir/main.go` (`mainFlags`).
#[derive(Clone, Debug, PartialEq)]
pub struct MainFlags {
    pub ballast_bytes: i64,
    pub mutex_profile_fraction: i64,
    pub block_profile_rate: i64,
    pub rate_limited_logs_enabled: bool,
    pub rate_limited_logs_per_second: f64,
    pub rate_limited_logs_burst_size: i64,
    pub print_version: bool,
    pub print_modules: bool,
    pub print_help: bool,
    pub print_help_all: bool,
}

impl Default for MainFlags {
    fn default() -> Self {
        Self {
            ballast_bytes: 0,
            mutex_profile_fraction: 0,
            block_profile_rate: 0,
            rate_limited_logs_enabled: false,
            rate_limited_logs_per_second: 10000.0,
            rate_limited_logs_burst_size: 1000,
            print_version: false,
            print_modules: false,
            print_help: false,
            print_help_all: false,
        }
    }
}

impl MainFlags {
    pub fn register_flags_into<T: 'static>(
        defaults: &Self,
        f: &mut FlagSet<T>,
        get: fn(&T) -> &Self,
        get_mut: fn(&mut T) -> &mut Self,
    ) {
        f.i64_var(
            "mem-ballast-size-bytes",
            defaults.ballast_bytes,
            "Size of memory ballast to allocate.",
            move |t| &get(t).ballast_bytes,
            move |t| &mut get_mut(t).ballast_bytes,
        );
        f.i64_var(
            "debug.mutex-profile-fraction",
            defaults.mutex_profile_fraction,
            "Fraction of mutex contention events that are reported in the mutex profile.",
            move |t| &get(t).mutex_profile_fraction,
            move |t| &mut get_mut(t).mutex_profile_fraction,
        );
        f.i64_var(
            "debug.block-profile-rate",
            defaults.block_profile_rate,
            "Fraction of goroutine blocking events that are reported in the blocking profile.",
            move |t| &get(t).block_profile_rate,
            move |t| &mut get_mut(t).block_profile_rate,
        );
        f.bool_var(
            "log.rate-limit-enabled",
            defaults.rate_limited_logs_enabled,
            "Use rate limited logger to reduce the number of logged messages per second.",
            move |t| &get(t).rate_limited_logs_enabled,
            move |t| &mut get_mut(t).rate_limited_logs_enabled,
        );
        f.f64_var(
            "log.rate-limit-logs-per-second",
            defaults.rate_limited_logs_per_second,
            "Maximum number of messages per second to be logged.",
            move |t| &get(t).rate_limited_logs_per_second,
            move |t| &mut get_mut(t).rate_limited_logs_per_second,
        );
        f.i64_var(
            "log.rate-limit-logs-burst-size",
            defaults.rate_limited_logs_burst_size,
            "Burst size, i.e., maximum number of messages that can be logged at once.",
            move |t| &get(t).rate_limited_logs_burst_size,
            move |t| &mut get_mut(t).rate_limited_logs_burst_size,
        );
        f.bool_var(
            "version",
            defaults.print_version,
            "Print application version and exit.",
            move |t| &get(t).print_version,
            move |t| &mut get_mut(t).print_version,
        );
        f.bool_var(
            "modules",
            defaults.print_modules,
            "List available values that can be used as target.",
            move |t| &get(t).print_modules,
            move |t| &mut get_mut(t).print_modules,
        );
        f.bool_var(
            "help",
            defaults.print_help,
            "Print basic help.",
            move |t| &get(t).print_help,
            move |t| &mut get_mut(t).print_help,
        );
        f.bool_var(
            "h",
            defaults.print_help,
            "Print basic help.",
            move |t| &get(t).print_help,
            move |t| &mut get_mut(t).print_help,
        );
        f.bool_var(
            "help-all",
            defaults.print_help_all,
            "Print help, also including advanced and experimental parameters.",
            move |t| &get(t).print_help_all,
            move |t| &mut get_mut(t).print_help_all,
        );
    }
}

/// Toolchain version reported where Go reports `runtime.Version()` (`version.GoVersion`):
/// the `go version:` line of `-version`. Rust builds report the compiler, e.g. `rustc1.86.0`
/// (captured by `build.rs`), mirroring Go's `go1.25.3` format.
pub fn go_version_str() -> &'static str {
    env!("MIMIR_RUSTC_VERSION")
}

/// Formats `-version` output matching `version.Print("Mimir")` in `pkg/util/version/info.go`.
pub fn format_version() -> String {
    let version = option_env!("MIMIR_VERSION").unwrap_or("unknown");
    let branch = option_env!("MIMIR_BRANCH").unwrap_or("unknown");
    let revision = option_env!("MIMIR_REVISION").unwrap_or("unknown");
    // runtime.GOOS + "/" + runtime.GOARCH
    let os = std::env::consts::OS;
    let arch = match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        "x86" => "386",
        "powerpc64" => "ppc64",
        "loongarch64" => "loong64",
        other => other,
    };
    format!(
        "Mimir, version {version} (branch: {branch}, revision: {revision})\n  go version:       {}\n  platform:         {os}/{arch}",
        go_version_str()
    )
}

/// All user-visible Mimir module names, matching `pkg/mimir/modules.go`.
pub const USER_VISIBLE_MODULES: &[&str] = &[
    "alertmanager",
    "all",
    "block-builder",
    "block-builder-scheduler",
    "compactor",
    "compactor-scheduler",
    "continuous-test",
    "distributor",
    "ingester",
    "memberlist-kv",
    "overrides-exporter",
    "querier",
    "query-frontend",
    "query-scheduler",
    "ruler",
    "store-gateway",
    "usage-tracker",
];

/// Formats `-modules` output matching `cmd/mimir/main.go` and `pkg/mimir/modules.go`.
pub fn format_modules() -> String {
    let deps: &[(&str, &[&str])] = &[
        ("api", &["server", "activity-tracker"]),
        ("alertmanager", &["api", "memberlist-kv", "overrides", "vault"]),
        ("block-builder", &["api", "overrides"]),
        ("block-builder-scheduler", &["api"]),
        ("cache-key-generator", &["query-frontend-codec"]),
        ("compactor", &["api", "memberlist-kv", "overrides", "vault"]),
        ("compactor-scheduler", &["api"]),
        ("continuous-test", &["api"]),
        ("cost-attribution-service", &["api", "overrides"]),
        ("distributor", &["distributor-service", "api", "active-groups-cleanup-service", "vault"]),
        ("distributor-service", &["ingester-ring", "ingester-partitions-ring", "overrides", "vault", "cost-attribution-service", "usage-tracker-instance-ring", "usage-tracker-partition-ring"]),
        ("ingester", &["ingester-service", "api", "active-groups-cleanup-service", "vault"]),
        ("ingester-partitions-ring", &["memberlist-kv", "ingester-ring", "api"]),
        ("ingester-ring", &["api", "runtime-config", "memberlist-kv", "vault"]),
        ("ingester-service", &["ingester-ring", "ingester-partitions-ring", "overrides", "runtime-config", "memberlist-kv", "cost-attribution-service"]),
        ("memberlist-kv", &["api", "vault"]),
        ("overrides", &["runtime-config"]),
        ("overrides-exporter", &["overrides", "memberlist-kv", "vault"]),
        ("querier", &["tenant-federation", "vault", "querier-lifecycler"]),
        ("querier-lifecycler", &["api", "runtime-config", "memberlist-kv", "vault"]),
        ("querier-query-planner", &["api", "overrides", "cache-key-generator"]),
        ("querier-ring", &["api", "runtime-config", "memberlist-kv", "vault"]),
        ("query-frontend", &["query-frontend-tripperware", "memberlist-kv", "vault"]),
        ("query-frontend-topic-offsets-reader", &["ingester-partitions-ring"]),
        ("query-frontend-tripperware", &["api", "overrides", "query-frontend-codec", "query-frontend-topic-offsets-reader", "querier-ring", "query-frontend-cache-client", "cache-key-generator"]),
        ("query-scheduler", &["api", "overrides", "memberlist-kv", "vault"]),
        ("queryable", &["overrides", "distributor-service", "ingester-ring", "ingester-partitions-ring", "api", "store-queryable", "memberlist-kv", "querier-query-planner", "querier-intermediate-cache-client"]),
        ("ruler", &["api", "memberlist-kv", "overrides", "ruler-storage", "runtime-config", "vault", "distributor-service", "store-queryable", "querier-query-planner"]),
        ("ruler-distributor-client", &["vault"]),
        ("ruler-storage", &["overrides"]),
        ("runtime-config", &["api"]),
        ("server", &["activity-tracker", "sanity-check", "usage-stats"]),
        ("store-gateway", &["api", "overrides", "memberlist-kv", "vault"]),
        ("store-queryable", &["overrides", "memberlist-kv"]),
        ("tenant-federation", &["queryable"]),
        ("usage-tracker", &["api", "overrides", "usage-tracker-instance-ring", "usage-tracker-partition-ring"]),
        ("usage-tracker-instance-ring", &["memberlist-kv"]),
        ("usage-tracker-partition-ring", &["memberlist-kv", "usage-tracker-instance-ring"]),
        ("all", &["query-frontend", "query-scheduler", "querier", "ingester", "distributor", "store-gateway", "ruler", "compactor"]),
    ];

    let dep_map: BTreeMap<&str, &[&str]> = deps.iter().copied().collect();
    let mut dependents_by_module: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();

    for &m in USER_VISIBLE_MODULES {
        let mut visited = BTreeSet::new();
        let mut stack = vec![m];
        while let Some(cur) = stack.pop() {
            if let Some(children) = dep_map.get(cur) {
                for &child in *children {
                    if visited.insert(child) {
                        stack.push(child);
                    }
                }
            }
        }
        for dep in visited {
            dependents_by_module.entry(dep).or_default().insert(m);
        }
    }

    let mut out = String::new();
    for &m in USER_VISIBLE_MODULES {
        out.push_str(m);
        if let Some(dependents) = dependents_by_module.get(m) {
            let joined: Vec<&str> = dependents.iter().copied().collect();
            let _ = write!(out, " (in: {})", joined.join(", "));
        }
        out.push('\n');
    }
    out
}

/// Returns whether `module` is a valid user-visible target module name.
pub fn is_targetable_module(module: &str) -> bool {
    USER_VISIBLE_MODULES.contains(&module)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct TestTarget {
        target: StringSliceCSV,
        port: i64,
        push_enabled: bool,
        expand_env: bool,
    }

    #[test]
    fn parses_single_and_double_dash_flags() {
        let mut t = TestTarget {
            target: StringSliceCSV(vec!["all".to_string()]),
            port: 8080,
            push_enabled: true,
            expand_env: false,
        };

        let mut fs: FlagSet<TestTarget> = FlagSet::new();
        fs.string_slice_csv_var(
            "target",
            &t.target,
            "Target components.",
            |x| &x.target,
            |x| &mut x.target,
        );
        fs.i64_var(
            "server.http-listen-port",
            t.port,
            "HTTP port.",
            |x| &x.port,
            |x| &mut x.port,
        );
        fs.bool_var(
            "ingester.push-grpc-method-enabled",
            t.push_enabled,
            "Push gRPC.",
            |x| &x.push_enabled,
            |x| &mut x.push_enabled,
        );
        fs.bool_var(
            "config.expand-env",
            t.expand_env,
            "Expand env.",
            |x| &x.expand_env,
            |x| &mut x.expand_env,
        );

        let args: Vec<String> = vec![
            "--target=ingester,memberlist-kv",
            "-server.http-listen-port",
            "9080",
            "-ingester.push-grpc-method-enabled=false",
            "-config.expand-env",
        ]
        .into_iter()
        .map(String::from)
        .collect();

        let rem = fs.parse(&mut t, &args).unwrap();
        assert!(rem.is_empty());
        assert_eq!(t.target.0, vec!["ingester", "memberlist-kv"]);
        assert_eq!(t.port, 9080);
        assert!(!t.push_enabled);
        assert!(t.expand_env);
        assert!(fs.is_set("target"));
        assert!(fs.is_set("config.expand-env"));
    }

    // Go strconv.ParseInt(s, 0, 64) / ParseUint / ParseFloat + flag.numError.
    #[test]
    fn go_numeric_flag_parsing() {
        assert_eq!(go_parse_int("0x1F").unwrap(), 31);
        assert_eq!(go_parse_int("-0o17").unwrap(), -15);
        assert_eq!(go_parse_int("017").unwrap(), 15);
        assert_eq!(go_parse_int("0b101").unwrap(), 5);
        assert_eq!(go_parse_int("1_000").unwrap(), 1000);
        assert_eq!(go_parse_int("+7").unwrap(), 7);
        for bad in ["", "abc", "1__0", "1_", "_1", "08", "0x", "1.5"] {
            assert_eq!(go_parse_int(bad).unwrap_err().to_string(), "invalid flag: parse error", "{bad}");
        }
        assert_eq!(
            go_parse_int("99999999999999999999").unwrap_err().to_string(),
            "invalid flag: value out of range"
        );
        assert!(go_parse_uint("-1").is_err());
        assert_eq!(go_parse_uint("18446744073709551615").unwrap(), u64::MAX);
        assert_eq!(go_parse_float("1e3").unwrap(), 1000.0);
        assert_eq!(go_parse_float("1_000.5").unwrap(), 1000.5);
        assert!(go_parse_float("+Inf").unwrap().is_infinite());
        assert_eq!(go_parse_float("1e400").unwrap_err().to_string(), "invalid flag: value out of range");
    }

    // Go flag.FlagSet.parseOne error texts.
    #[test]
    fn flag_value_error_texts_match_go() {
        let mut t = TestTarget::default();
        let mut fs: FlagSet<TestTarget> = FlagSet::new();
        fs.i64_var("port", 0, "", |x| &x.port, |x| &mut x.port);
        fs.bool_var("push", false, "", |x| &x.push_enabled, |x| &mut x.push_enabled);
        let err = fs.parse(&mut t, &["-port=abc".to_string()]).unwrap_err();
        assert_eq!(err.to_string(), "invalid flag: invalid value \"abc\" for flag -port: parse error");
        let err = fs.parse(&mut t, &["-push=maybe".to_string()]).unwrap_err();
        assert_eq!(err.to_string(), "invalid flag: invalid boolean value \"maybe\" for -push: parse error");
        let err = fs.parse(&mut t, &["-port".to_string()]).unwrap_err();
        assert_eq!(err.to_string(), "invalid flag: flag needs an argument: -port");
    }

    // pkg/util/version/info.go Print("Mimir").
    #[test]
    fn version_output_format() {
        let out = format_version();
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines.len(), 3);
        assert!(lines[0].starts_with("Mimir, version "));
        assert!(lines[0].contains(" (branch: ") && lines[0].contains(", revision: "));
        assert_eq!(lines[1], format!("  go version:       {}", go_version_str()));
        assert!(lines[2].starts_with("  platform:         "));
        assert!(go_version_str().starts_with("rustc"));
    }

    #[test]
    fn unknown_flag_returns_exit_code_2() {
        let mut t = TestTarget::default();
        let mut fs: FlagSet<TestTarget> = FlagSet::new();
        let args = vec!["-unknown-flag".to_string()];
        let err = fs.parse(&mut t, &args).unwrap_err();
        assert_eq!(err.exit_code(), 2);
        assert!(err.to_string().contains("flag provided but not defined"));
    }

    #[test]
    fn parse_config_file_parameter_finds_flags_past_unknowns() {
        let args: Vec<String> = vec![
            "-unknown",
            "-config.expand-env",
            "--config.file=/etc/mimir.yaml",
            "-target=ingester",
        ]
        .into_iter()
        .map(String::from)
        .collect();
        let (file, expand) = parse_config_file_parameter(&args);
        assert_eq!(file, "/etc/mimir.yaml");
        assert!(expand);
    }
}
