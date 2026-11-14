//! Push and read reactive limiters matching `pkg/ingester/reactivelimiter.go`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use mimir_common::limiter::{ReactiveLimiter, ReactiveLimiterConfig as CommonRlCfg};
use mimir_config::ReactiveLimiterConfig;
use prometheus::core::{Collector, Desc};
use prometheus::proto::MetricFamily;
use prometheus::Registry;

fn to_common_rl_cfg(cfg: &ReactiveLimiterConfig) -> CommonRlCfg {
    CommonRlCfg {
        enabled: cfg.enabled,
        min_limit: cfg.min_limit as usize,
        max_limit: cfg.max_limit as usize,
        initial_limit: cfg.initial_limit as usize,
        max_limit_factor: cfg.max_limit_factor,
        max_limit_factor_decay: cfg.max_limit_factor_decay,
        min_limit_factor: cfg.min_limit_factor,
        recent_window_min_duration: Duration::from_nanos(
            cfg.recent_window_min_duration.as_nanos().max(0) as u64,
        ),
        recent_window_max_duration: Duration::from_nanos(
            cfg.recent_window_max_duration.as_nanos().max(0) as u64,
        ),
        recent_window_min_samples: cfg.recent_window_min_samples as usize,
        recent_quantile: cfg.recent_quantile,
        baseline_window_age: cfg.baseline_window_age as usize,
        correlation_window: cfg.correlation_window as usize,
        initial_rejection_factor: cfg.initial_rejection_factor,
        max_rejection_factor: cfg.max_rejection_factor,
    }
}

struct RlGaugeCollector {
    desc: Desc,
    f: Box<dyn Fn() -> f64 + Send + Sync>,
}

impl RlGaugeCollector {
    fn new(
        name: &str,
        help: &str,
        request_type: &str,
        f: impl Fn() -> f64 + Send + Sync + 'static,
    ) -> Self {
        let mut labels = HashMap::new();
        labels.insert("request_type".to_string(), request_type.to_string());
        Self {
            desc: Desc::new(name.to_string(), help.to_string(), vec![], labels)
                .expect("valid rl gauge desc"),
            f: Box::new(f),
        }
    }
}

impl Collector for RlGaugeCollector {
    fn desc(&self) -> Vec<&Desc> {
        vec![&self.desc]
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let mut g = prometheus::proto::Gauge::default();
        g.set_value((self.f)());
        let mut m = prometheus::proto::Metric::default();
        m.set_gauge(g);
        let pairs = self.desc.const_label_pairs.to_vec();
        m.set_label(pairs.into());
        let mut mf = MetricFamily::default();
        mf.set_name(self.desc.fq_name.clone());
        mf.set_help(self.desc.help.clone());
        mf.set_field_type(prometheus::proto::MetricType::GAUGE);
        mf.set_metric(vec![m].into());
        vec![mf]
    }
}

fn register_rl_metrics(limiter: &Arc<ReactiveLimiter>, request_type: &str, reg: Option<&Registry>) {
    let Some(r) = reg else {
        return;
    };
    let l1 = Arc::clone(limiter);
    let _ = r.register(Box::new(RlGaugeCollector::new(
        "cortex_ingester_reactive_limiter_inflight_limit",
        "Ingester reactive limiter inflight request limit.",
        request_type,
        move || l1.limit() as f64,
    )));
    let l2 = Arc::clone(limiter);
    let _ = r.register(Box::new(RlGaugeCollector::new(
        "cortex_ingester_reactive_limiter_inflight_requests",
        "Ingester reactive limiter inflight requests.",
        request_type,
        move || l2.max_inflight() as f64,
    )));
    let l3 = Arc::clone(limiter);
    let _ = r.register(Box::new(RlGaugeCollector::new(
        "cortex_ingester_reactive_limiter_queued_requests",
        "Ingester reactive limiter queued requests.",
        request_type,
        move || l3.queued() as f64,
    )));
}

pub struct IngesterReactiveLimiter {
    pub push: Option<Arc<ReactiveLimiter>>,
    pub read: Option<Arc<ReactiveLimiter>>,
}

impl IngesterReactiveLimiter {
    pub fn new(
        push_cfg: &ReactiveLimiterConfig,
        read_cfg: &ReactiveLimiterConfig,
        reg: Option<&Registry>,
    ) -> Result<Self, String> {
        let push = if push_cfg.enabled {
            let l = ReactiveLimiter::new(to_common_rl_cfg(push_cfg))?;
            register_rl_metrics(&l, "push", reg);
            Some(l)
        } else {
            None
        };
        let read = if read_cfg.enabled {
            let l = ReactiveLimiter::new(to_common_rl_cfg(read_cfg))?;
            register_rl_metrics(&l, "read", reg);
            Some(l)
        } else {
            None
        };
        Ok(Self { push, read })
    }
}
