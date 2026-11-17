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
use mimir_config::{RuntimeConfigLoadEvent, RuntimeConfigObserver};
use prometheus::{Gauge, GaugeVec, Opts, Registry};

/// Name Mimir gives its runtime config manager (`config` label value).
pub const RUNTIME_CONFIG_NAME: &str = "mimir-runtime-config";

pub struct RuntimeConfigMetrics {
    config_load_success: Gauge,
    source_load_success: GaugeVec,
    config_hash: GaugeVec,
    logger: Arc<dyn Logger>,
}

impl RuntimeConfigMetrics {
    /// Registers the gauges and creates every source's series at 0 ("nothing read yet").
    pub fn new(registry: &Registry, logger: Arc<dyn Logger>, sources: &[String]) -> prometheus::Result<Self> {
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
        Ok(Self {
            config_load_success,
            source_load_success,
            config_hash,
            logger,
        })
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
