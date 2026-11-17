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
                let v: i64 = s
                    .parse()
                    .map_err(|_| ConfigError::Flag("parse error".to_string()))?;
                *get_mut(t) = v;
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
                let v: u64 = s
                    .parse()
                    .map_err(|_| ConfigError::Flag("parse error".to_string()))?;
                *get_mut(t) = v;
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
                let v: f64 = s
                    .parse()
                    .map_err(|_| ConfigError::Flag("parse error".to_string()))?;
                *get_mut(t) = v;
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
                *get_mut(t) = s.parse()?;
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
                let v = if has_value { value } else { "true" };
                (entry.setter)(target, v).map_err(|e| {
                    ConfigError::Flag(format!("invalid boolean value {v:?} for -{name}: {e}"))
                })?;
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
                    ConfigError::Flag(format!("invalid value {v:?} for flag -{name}: {e}"))
                })?;
            }

            entry.was_set = true;
        }
        Ok(&args[idx..])
    }

    /// Formats usage output matching Go `flag.FlagSet.PrintDefaults()` / `usage.Usage`.
    pub fn format_usage(&self, prog_name: &str, _all: bool) -> String {
        let mut out = format!("Usage of {prog_name}:\n");
        for entry in self.flags.values() {
            if entry.usage.is_empty() && entry.name != "h" && entry.name != "help" {
                continue;
            }
            let mut line = format!("  -{}", entry.name);
            if !entry.type_name.is_empty() {
                let _ = write!(line, " {}", entry.type_name);
            }
            out.push_str(&line);
            out.push('\n');
            if !entry.usage.is_empty() {
                out.push_str("    \t");
                out.push_str(&entry.usage.replace('\n', "\n    \t"));
                if !is_zero_default(&entry.default_str, entry.is_bool) {
                    if entry.type_name == "string" && !entry.usage_default_unquoted {
                        let _ = write!(out, " (default {:?})", entry.default_str);
                    } else {
                        let _ = write!(out, " (default {})", entry.default_str);
                    }
                }
                out.push('\n');
            }
        }
        out
    }
}

fn is_zero_default(s: &str, is_bool: bool) -> bool {
    if is_bool {
        s == "false" || s.is_empty()
    } else {
        matches!(s, "" | "0" | "0s" | "false" | "[]" | "{}")
    }
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

/// Formats `-version` output matching `pkg/util/version/info.go`.
pub fn format_version() -> String {
    let version = option_env!("MIMIR_VERSION").unwrap_or("unknown");
    let branch = option_env!("MIMIR_BRANCH").unwrap_or("unknown");
    let revision = option_env!("MIMIR_REVISION").unwrap_or("unknown");
    let os = std::env::consts::OS;
    let arch = match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        other => other,
    };
    format!(
        "Mimir, version {version} (branch: {branch}, revision: {revision})\n  mimir-rs:         {}\n  platform:         {os}/{arch}",
        env!("CARGO_PKG_VERSION")
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
