//! Runtime-config reload metrics and logging.
//!
//! Ported from:
//! - github.com/grafana/dskit/runtimeconfig/manager.go (`New`: the
//!   `runtime_config_last_reload_successful`, `runtime_config_source_last_reload_successful{source}`
//!   and `runtime_config_hash{sha256}` gauges, wrapped with the `config` label; `loadConfig`
//!   metric updates and the tolerated-source warning; `loop`: `failed to load config` error log)
//! - github.com/grafana/mimir/pkg/mimir/modules.go (`initRuntimeConfig`: name
//!   `mimir-runtime-config`, registerer prefixed with `cortex_`)

use std::sync::Arc;

use mimir_common::logging::{LogLevel, Logger};
use std::time::Duration;

use mimir_config::{
    is_url, RuntimeConfigHttpObserver, RuntimeConfigLoadEvent, RuntimeConfigObserver,
};
use prometheus::{CounterVec, Gauge, GaugeVec, HistogramOpts, HistogramVec, Opts, Registry};

/// Name Mimir gives its runtime config manager (`config` label value).
pub const RUNTIME_CONFIG_NAME: &str = "mimir-runtime-config";

pub struct RuntimeConfigMetrics {
    config_load_success: Gauge,
    source_load_success: GaugeVec,
    config_hash: GaugeVec,
    http_duration: Option<HistogramVec>,
    invalid_cluster_validations: Option<CounterVec>,
    cluster_validation_label: String,
    load_path: String,
    logger: Arc<dyn Logger>,
}

impl RuntimeConfigMetrics {
    /// Registers the gauges and creates every source's series at 0 ("nothing read yet").
    pub fn new(registry: &Registry, logger: Arc<dyn Logger>, sources: &[String]) -> prometheus::Result<Self> {
        Self::new_with_http(registry, logger, sources, &[], "")
    }

    /// Registers gauges plus HTTP runtime-config duration & cluster-validation metrics when any
    /// source is an HTTP/HTTPS URL (`dskit/runtimeconfig/manager.go:158-184, 558-572`).
    pub fn new_with_http(
        registry: &Registry,
        logger: Arc<dyn Logger>,
        sources: &[String],
        raw_files: &[String],
        cluster_validation_label: &str,
    ) -> prometheus::Result<Self> {
        let opts = |name: &str, help: &str| {
            Opts::new(name, help)
                .namespace("cortex")
                .const_label("config", RUNTIME_CONFIG_NAME)
        };
        let config_load_success = Gauge::with_opts(opts(
            "runtime_config_last_reload_successful",
            "Whether the last runtime-config reload attempt was successful.",
        ))?;
        let source_load_success = GaugeVec::new(
            opts(
                "runtime_config_source_last_reload_successful",
                "Whether the last read of each individual runtime-config source was successful. A source whose failure is tolerated can be 0 while runtime_config_last_reload_successful is 1.",
            ),
            &["source"],
        )?;
        let config_hash = GaugeVec::new(
            opts(
                "runtime_config_hash",
                "Hash of the currently active runtime configuration, merged from all configured files.",
            ),
            &["sha256"],
        )?;
        registry.register(Box::new(config_load_success.clone()))?;
        registry.register(Box::new(source_load_success.clone()))?;
        registry.register(Box::new(config_hash.clone()))?;
        for s in sources {
            source_load_success.with_label_values(&[s]).set(0.0);
        }

        let has_url = raw_files.iter().any(|p| is_url(p));
        let http_duration = if has_url {
            let h = HistogramVec::new(
                HistogramOpts::new(
                    "runtime_config_http_request_duration_seconds",
                    "Time spent fetching runtime config from HTTP URLs.",
                )
                .namespace("cortex")
                .const_label("config", RUNTIME_CONFIG_NAME)
                .buckets(prometheus::DEFAULT_BUCKETS.to_vec()),
                &["url", "status_code"],
            )?;
            registry.register(Box::new(h.clone()))?;
            Some(h)
        } else {
            None
        };

        let invalid_cluster_validations = if has_url && !cluster_validation_label.is_empty() {
            let c = CounterVec::new(
                Opts::new(
                    "client_invalid_cluster_validation_label_requests_total",
                    "Number of requests with invalid cluster validation label.",
                )
                .namespace("cortex")
                .const_label("client", format!("runtime-config/{RUNTIME_CONFIG_NAME}"))
                .const_label("protocol", "http"),
                &["method"],
            )?;
            registry.register(Box::new(c.clone()))?;
            Some(c)
        } else {
            None
        };

        Ok(Self {
            config_load_success,
            source_load_success,
            config_hash,
            http_duration,
            invalid_cluster_validations,
            cluster_validation_label: cluster_validation_label.to_string(),
            load_path: raw_files.join(","),
            logger,
        })
    }
}

impl RuntimeConfigHttpObserver for RuntimeConfigMetrics {
    fn observe_request(&self, url_for_metrics: &str, status_code: &str, elapsed: Duration) {
        if let Some(ref h) = self.http_duration {
            h.with_label_values(&[url_for_metrics, status_code])
                .observe(elapsed.as_secs_f64());
        }
    }

    fn invalid_cluster_validation(&self, msg: &str, method: &str) {
        self.logger.log(
            LogLevel::Warn,
            &[
                ("msg", msg),
                ("method", method),
                ("cluster_validation_label", &self.cluster_validation_label),
                ("component", "runtimeconfig"),
                ("load_path", &self.load_path),
            ],
        );
        if let Some(ref c) = self.invalid_cluster_validations {
            c.with_label_values(&[method]).inc();
        }
    }
}

impl RuntimeConfigObserver for RuntimeConfigMetrics {
    fn on_load(&self, event: &RuntimeConfigLoadEvent<'_>) {
        for st in event.sources {
            let ok = st.error.is_none();
            self.source_load_success
                .with_label_values(&[&st.source])
                .set(if ok { 1.0 } else { 0.0 });
            if let (Some(err), true) = (&st.error, st.tolerated) {
                self.logger.log(
                    LogLevel::Warn,
                    &[
                        ("msg", "failed to load runtime config source, continuing anyway"),
                        ("source", &st.name),
                        ("parameters", st.parameters),
                        ("err", err),
                    ],
                );
            }
        }
        match event.result {
            Ok(()) => {
                self.config_load_success.set(1.0);
                if let Some(hash) = event.applied_hash {
                    self.config_hash.reset();
                    self.config_hash.with_label_values(&[hash]).set(1.0);
                }
            }
            Err(err) => {
                self.config_load_success.set(0.0);
                // The initial load failure fails startup instead (Manager.starting).
                if !event.initial {
                    let err = err.to_string();
                    self.logger
                        .log(LogLevel::Error, &[("msg", "failed to load config"), ("err", &err)]);
                }
            }
        }
    }
}
