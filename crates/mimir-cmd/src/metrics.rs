//! Global build info and config hash Prometheus metrics.
//!
//! Ported from:
//! - github.com/grafana/mimir/cmd/mimir/main.go (`cortex_config_hash`)
//! - github.com/grafana/mimir/pkg/util/version/info.go (`mimir_build_info`, `cortex_build_info`)

use std::collections::HashMap;

use prometheus::{Counter, Gauge, GaugeVec, Opts, Registry};

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

    register_go_package_init_metrics(registry)?;

    Ok(())
}

/// Registers Go Mimir package-init global metrics that Go's `promauto` / `prometheus.MustRegister`
/// populate on `prometheus.DefaultRegisterer` regardless of which `-target` is active.
fn register_go_package_init_metrics(registry: &Registry) -> Result<(), prometheus::Error> {
    // .ref/mimir/pkg/distributor/ingester_client_pool.go:21
    let ingester_clients = Gauge::new(
        "cortex_distributor_ingester_clients",
        "The current number of ingester clients.",
    )?;
    registry.register(Box::new(ingester_clients))?;

    // .ref/mimir/vendor/github.com/grafana/dskit/flagext/deprecated.go:15
    let deprecated_flags = Counter::new(
        "deprecated_flags_inuse_total",
        "The number of deprecated flags currently set.",
    )?;
    registry.register(Box::new(deprecated_flags))?;

    // .ref/mimir/vendor/github.com/prometheus/prometheus/storage/remote/write.go:43,49,55
    let remote_samples_in = Counter::new(
        "prometheus_remote_storage_samples_in_total",
        "Samples in to remote storage, compare to samples out for queue managers. Deprecated, check prometheus_wal_watcher_records_read_total and prometheus_remote_storage_samples_dropped_total",
    )?;
    registry.register(Box::new(remote_samples_in))?;

    let remote_exemplars_in = Counter::new(
        "prometheus_remote_storage_exemplars_in_total",
        "Exemplars in to remote storage, compare to exemplars out for queue managers. Deprecated, check prometheus_wal_watcher_records_read_total and prometheus_remote_storage_exemplars_dropped_total",
    )?;
    registry.register(Box::new(remote_exemplars_in))?;

    let remote_histograms_in = Counter::new(
        "prometheus_remote_storage_histograms_in_total",
        "HistogramSamples in to remote storage, compare to histograms out for queue managers. Deprecated, check prometheus_wal_watcher_records_read_total and prometheus_remote_storage_histograms_dropped_total",
    )?;
    registry.register(Box::new(remote_histograms_in))?;

    // .ref/mimir/vendor/github.com/prometheus/prometheus/storage/remote/intern.go:32
    let string_interner_zero_releases = Counter::new(
        "prometheus_remote_storage_string_interner_zero_reference_releases_total",
        "The number of times release has been called for strings that are not interned.",
    )?;
    registry.register(Box::new(string_interner_zero_releases))?;

    // .ref/mimir/vendor/github.com/prometheus/prometheus/template/template.go:46,50
    let template_expansion_failures = Counter::new(
        "prometheus_template_text_expansion_failures_total",
        "The total number of template text expansion failures.",
    )?;
    registry.register(Box::new(template_expansion_failures))?;

    let template_expansions = Counter::new(
        "prometheus_template_text_expansions_total",
        "The total number of template text expansions.",
    )?;
    registry.register(Box::new(template_expansions))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::proto::MetricType;

    #[test]
    fn test_register_global_metrics_includes_go_package_inits() {
        let reg = Registry::new();
        register_global_metrics(&reg, Some("abc123")).unwrap();

        let families = reg.gather();
        let find = |name: &str| {
            families
                .iter()
                .find(|f| f.get_name() == name)
                .unwrap_or_else(|| panic!("missing metric family {name}"))
        };

        let expected: &[(&str, MetricType, &str)] = &[
            (
                "cortex_distributor_ingester_clients",
                MetricType::GAUGE,
                "The current number of ingester clients.",
            ),
            (
                "deprecated_flags_inuse_total",
                MetricType::COUNTER,
                "The number of deprecated flags currently set.",
            ),
            (
                "prometheus_remote_storage_samples_in_total",
                MetricType::COUNTER,
                "Samples in to remote storage, compare to samples out for queue managers. Deprecated, check prometheus_wal_watcher_records_read_total and prometheus_remote_storage_samples_dropped_total",
            ),
            (
                "prometheus_remote_storage_exemplars_in_total",
                MetricType::COUNTER,
                "Exemplars in to remote storage, compare to exemplars out for queue managers. Deprecated, check prometheus_wal_watcher_records_read_total and prometheus_remote_storage_exemplars_dropped_total",
            ),
            (
                "prometheus_remote_storage_histograms_in_total",
                MetricType::COUNTER,
                "HistogramSamples in to remote storage, compare to histograms out for queue managers. Deprecated, check prometheus_wal_watcher_records_read_total and prometheus_remote_storage_histograms_dropped_total",
            ),
            (
                "prometheus_remote_storage_string_interner_zero_reference_releases_total",
                MetricType::COUNTER,
                "The number of times release has been called for strings that are not interned.",
            ),
            (
                "prometheus_template_text_expansion_failures_total",
                MetricType::COUNTER,
                "The total number of template text expansion failures.",
            ),
            (
                "prometheus_template_text_expansions_total",
                MetricType::COUNTER,
                "The total number of template text expansions.",
            ),
        ];

        for (name, ty, help) in expected {
            let mf = find(name);
            assert_eq!(mf.get_field_type(), *ty, "type mismatch for {name}");
            assert_eq!(mf.get_help(), *help, "help mismatch for {name}");
            let m = &mf.get_metric()[0];
            assert!(m.get_label().is_empty(), "unexpected labels on {name}");
            let val = match ty {
                MetricType::GAUGE => m.get_gauge().get_value(),
                MetricType::COUNTER => m.get_counter().get_value(),
                _ => unreachable!(),
            };
            assert_eq!(val, 0.0, "expected initial value 0 for {name}");
        }
    }
}
