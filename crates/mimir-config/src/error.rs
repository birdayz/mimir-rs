//! Configuration error types for `mimir-config`.
//!
//! Ported from:
//! - github.com/grafana/mimir/cmd/mimir/main.go
//! - github.com/grafana/mimir/pkg/mimir/mimir.go

use std::fmt;

/// Errors returned while parsing CLI flags, loading YAML configs, or validating configuration.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("error reading config file: {0}")]
    Io(#[from] std::io::Error),

    /// `LoadConfig` read failure, formatted like Go's `*fs.PathError` (`open <path>: <errno text>`).
    #[error("error reading config file: {0}")]
    ReadConfigFile(String),

    /// `cmd/mimir/main.go`: `error loading config from %s: %v` around any `LoadConfig` error.
    #[error("error loading config from {path}: {source}")]
    LoadConfig {
        path: String,
        source: Box<ConfigError>,
    },

    #[error("error parsing config file: {0}")]
    Yaml(String),

    #[error("unknown configuration field: {0}")]
    UnknownField(String),

    /// `reason` is the exact Go error text (`time.ParseDuration` / `model.ParseDuration`).
    #[error("{reason}")]
    Duration { input: String, reason: String },

    #[error("invalid flag: {0}")]
    Flag(String),

    #[error("error inheriting common flag values: {0}")]
    CommonInheritance(String),

    #[error("error validating config: {0}")]
    Validation(String),

    #[error("error loading runtime config: {0}")]
    RuntimeConfig(String),
}

impl ConfigError {
    /// Returns the process exit code matching `cmd/mimir/main.go`.
    /// CLI flag errors exit with code 2; config load/validation errors exit with code 1.
    pub fn exit_code(&self) -> i32 {
        match self {
            ConfigError::Flag(_) => 2,
            _ => 1,
        }
    }
}

/// Formats an I/O error like Go's `*fs.PathError`: `<op> <path>: <errno text>`, where the errno
/// text is the lower-cased C `strerror` message (Go's `syscall.Errno.Error()` table).
pub fn go_path_error(op: &str, path: &str, err: &std::io::Error) -> String {
    let msg = err.to_string();
    let msg = match msg.find(" (os error ") {
        Some(idx) if err.raw_os_error().is_some() => msg[..idx].to_string(),
        _ => msg,
    };
    let mut chars = msg.chars();
    let msg = match chars.next() {
        Some(c) => c.to_lowercase().collect::<String>() + chars.as_str(),
        None => msg,
    };
    format!("{op} {path}: {msg}")
}

/// Exit action requested by special CLI flags (`-version`, `-modules`, `-help`, `-help-all`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CliAction {
    /// Normal run (start application).
    Run,
    /// Print version and exit with status 0.
    PrintVersion,
    /// Print modules and exit with status 0.
    PrintModules,
    /// Print usage (all = false for `-help`/`-h`, true for `-help-all`) and exit with status 2.
    PrintHelp { all: bool },
    /// Print loaded config as YAML (`-print.config`) and exit with status 0.
    PrintConfig,
}

impl fmt::Display for CliAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
