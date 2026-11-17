//! Global build info and config hash Prometheus metrics.
//!
//! Ported from:
//! - github.com/grafana/mimir/cmd/mimir/main.go (`cortex_config_hash`)
//! - github.com/grafana/mimir/pkg/util/version/info.go (`mimir_build_info`, `cortex_build_info`)

use std::collections::HashMap;

use prometheus::{Gauge, GaugeVec, Opts, Registry};

pub fn version_str() -> &'static str {
    option_env!("MIMIR_VERSION").unwrap_or("unknown")
}

pub fn revision_str() -> &'static str {
    option_env!("MIMIR_REVISION").unwrap_or("unknown")
}

pub fn branch_str() -> &'static str {
    option_env!("MIMIR_BRANCH").unwrap_or("unknown")
}

pub fn build_user_str() -> &'static str {
    option_env!("MIMIR_BUILD_USER").unwrap_or("unknown")
}

pub fn build_date_str() -> &'static str {
    option_env!("MIMIR_BUILD_DATE").unwrap_or("unknown")
}

/// `github.com/prometheus/common/version.Info()`, logged with "Starting application".
pub fn version_info() -> String {
    format!(
        "(version={}, branch={}, revision={})",
        version_str(),
        branch_str(),
        revision_str()
    )
}

/// `version.GoVersion` equivalent (shared with `-version`): the Rust toolchain, e.g. `rustc1.86.0`.
pub fn go_version_str() -> String {
    mimir_config::go_version_str().to_string()
}

/// Registers the `process_*` collectors Go Mimir registers in pkg/mimir/mimir.go
/// (`collectors.NewProcessCollector` + `process.NewProcessCollector`), when procfs is available.
pub fn register_process_collector(registry: &Registry) -> Result<(), prometheus::Error> {
    if let Some(c) = crate::process_collector::ProcessCollector::for_self() {
        registry.register(Box::new(c))?;
    }
    Ok(())
}

/// Registers `mimir_build_info`, `cortex_build_info`, and `cortex_config_hash` on `registry`.
pub fn register_global_metrics(
    registry: &Registry,
    config_hash_sha256: Option<&str>,
) -> Result<(), prometheus::Error> {
    let goversion = go_version_str();

    for program in ["mimir", "cortex"] {
        let mut const_labels = HashMap::new();
        const_labels.insert("version".to_string(), version_str().to_string());
        const_labels.insert("revision".to_string(), revision_str().to_string());
        const_labels.insert("branch".to_string(), branch_str().to_string());
        const_labels.insert("goversion".to_string(), goversion.clone());

        let opts = Opts::new(
            format!("{program}_build_info"),
            format!(
                "A metric with a constant '1' value labeled by version, revision, branch, and goversion from which {program} was built."
            ),
        )
        .const_labels(const_labels);
        let gauge = Gauge::with_opts(opts)?;
        gauge.set(1.0);
        registry.register(Box::new(gauge))?;
    }

    let config_hash = GaugeVec::new(
        Opts::new(
            "cortex_config_hash",
            "Hash of the currently active config file.",
        ),
        &["sha256"],
    )?;
    if let Some(sha) = config_hash_sha256 {
        config_hash.with_label_values(&[sha]).set(1.0);
    }
    registry.register(Box::new(config_hash))?;

    Ok(())
}
