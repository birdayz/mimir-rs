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

    #[error("error parsing config file: {0}")]
    Yaml(String),

    #[error("unknown configuration field: {0}")]
    UnknownField(String),

    #[error("invalid duration {input:?}: {reason}")]
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
