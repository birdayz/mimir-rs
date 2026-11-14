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

pub fn go_version_str() -> String {
    format!("rustc-{}", env!("CARGO_PKG_VERSION"))
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
