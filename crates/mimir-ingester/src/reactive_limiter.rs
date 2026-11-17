//! Push and read reactive limiters matching `pkg/ingester/reactivelimiter.go`.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/ingester/reactivelimiter.go
//! - github.com/grafana/mimir/pkg/util/reactivelimiter/reactivelimiter.go (`New`, `NewWithPriority`)
//! - github.com/failsafe-go/failsafe-go/adaptivelimiter/prioritylimiter.go (`priorityLimiter`)
//! - github.com/failsafe-go/failsafe-go/adaptivelimiter/adaptivelimiter.go (`BuildPrioritized`)

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use mimir_common::limiter::{
    ReactiveLimitError, ReactiveLimiter, ReactiveLimiterConfig as CommonRlCfg, ReactivePermit,
};
use mimir_config::{PrioritizerConfig, ReactiveLimiterConfig};
use prometheus::core::{Collector, Desc};
use prometheus::proto::MetricFamily;
use prometheus::Registry;

use crate::prioritizer::{Prioritizer, Priority, QueueStats};

const REACTIVE_LIMITER_REQUEST_TYPE_LABEL: &str = "request_type";

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

/// `prometheus.NewGaugeFunc` equivalent with const labels.
pub(crate) struct FnGauge {
    desc: Desc,
    f: Box<dyn Fn() -> f64 + Send + Sync>,
}

impl FnGauge {
    pub(crate) fn new(
        name: &str,
        help: &str,
        const_labels: &[(&str, &str)],
        f: impl Fn() -> f64 + Send + Sync + 'static,
    ) -> Self {
        let labels = const_labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect::<HashMap<_, _>>();
        Self {
            desc: Desc::new(name.to_string(), help.to_string(), vec![], labels)
                .expect("valid gauge desc"),
            f: Box::new(f),
        }
    }
}

impl Collector for FnGauge {
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

/// A reactive limiter, optionally prioritized through a shared [`Prioritizer`]
/// (Go: `reactivelimiter.ReactiveLimiter` implemented by `reactiveLimiter` / `priorityLimiter`).
pub struct IngesterRl {
    limiter: Arc<ReactiveLimiter>,
    prioritized: Option<(Arc<Prioritizer>, Priority)>,
    initial_rejection_factor: f64,
    max_rejection_factor: f64,
}

impl IngesterRl {
    /// Go: `reactivelimiter.New`.
    pub fn new(cfg: &ReactiveLimiterConfig) -> Result<Arc<Self>, String> {
        let c = to_common_rl_cfg(cfg);
        Ok(Arc::new(Self {
            initial_rejection_factor: c.initial_rejection_factor,
            max_rejection_factor: c.max_rejection_factor,
            limiter: ReactiveLimiter::new(c)?,
            prioritized: None,
        }))
    }

    /// Go: `reactivelimiter.NewWithPriority` -> `Builder.BuildPrioritized(prioritizer)`.
    pub fn new_with_priority(
        cfg: &ReactiveLimiterConfig,
        priority: Priority,
        prioritizer: Arc<Prioritizer>,
    ) -> Result<Arc<Self>, String> {
        let mut c = to_common_rl_cfg(cfg);
        if c.initial_rejection_factor == 0.0 && c.max_rejection_factor == 0.0 {
            c.initial_rejection_factor = 2.0;
            c.max_rejection_factor = 3.0;
        }
        let rl = Arc::new(Self {
            initial_rejection_factor: c.initial_rejection_factor,
            max_rejection_factor: c.max_rejection_factor,
            limiter: ReactiveLimiter::new(c)?,
            prioritized: Some((Arc::clone(&prioritizer), priority)),
        });
        let weak = Arc::downgrade(&rl);
        prioritizer.register(move || {
            weak.upgrade()
                .map(|l| l.queue_stats())
                .unwrap_or_default()
        });
        Ok(rl)
    }

    /// Go: `queueingLimiter.getQueueStats()`.
    pub fn queue_stats(&self) -> QueueStats {
        let limit = self.limiter.limit();
        QueueStats {
            limit,
            queued: self.limiter.queued(),
            rejection_threshold: (limit as f64 * self.initial_rejection_factor) as usize,
            max_queue: (limit as f64 * self.max_rejection_factor) as usize,
        }
    }

    /// Go: `priorityLimiter.CanAcquirePermitWithLevel`.
    fn can_acquire_permit_with_level(&self, prioritizer: &Prioritizer, level: usize) -> bool {
        // When this is the only limiter used by the prioritizer, reject based on capacity.
        if prioritizer.registered_policies() == 1 {
            if !self.limiter.is_full() {
                return true;
            }
            let max_queue = (self.limiter.limit() as f64 * self.max_rejection_factor) as usize;
            if self.limiter.queued() >= max_queue {
                return false;
            }
        }
        level as i64 >= i64::from(prioritizer.rejection_threshold())
    }

    pub fn can_acquire_permit(&self) -> bool {
        match self.prioritized {
            None => self.limiter.can_acquire_permit(),
            Some((ref p, prio)) => self.can_acquire_permit_with_level(p, prio.random_level()),
        }
    }

    pub async fn acquire_permit(&self) -> Result<ReactivePermit, ReactiveLimitError> {
        match self.prioritized {
            None => self.limiter.acquire_permit().await,
            Some((ref p, prio)) => {
                // Go: priorityLimiter.AcquirePermitWithLevel.
                let level = prio.random_level();
                if !self.can_acquire_permit_with_level(p, level) {
                    return Err(ReactiveLimitError::LimitExceeded);
                }
                // Record levels when overloaded.
                if self.limiter.is_full() {
                    p.level_tracker.record_level(level);
                }
                self.limiter.acquire_permit_unchecked().await
            }
        }
    }

    pub fn reset(&self) {
        self.limiter.reset();
    }

    pub fn limit(&self) -> usize {
        self.limiter.limit()
    }

    pub fn inflight(&self) -> usize {
        self.limiter.inflight()
    }

    pub fn max_inflight(&self) -> usize {
        self.limiter.max_inflight()
    }

    pub fn queued(&self) -> usize {
        self.limiter.queued()
    }

    pub fn inner(&self) -> &Arc<ReactiveLimiter> {
        &self.limiter
    }
}

fn register_rl_metrics(limiter: &Arc<IngesterRl>, request_type: &str, reg: Option<&Registry>) {
    let Some(r) = reg else {
        return;
    };
    let labels = [(REACTIVE_LIMITER_REQUEST_TYPE_LABEL, request_type)];
    let l1 = Arc::clone(limiter);
    let _ = r.register(Box::new(FnGauge::new(
        "cortex_ingester_reactive_limiter_inflight_limit",
        "Ingester reactive limiter inflight request limit.",
        &labels,
        move || l1.limit() as f64,
    )));
    let l2 = Arc::clone(limiter);
    let _ = r.register(Box::new(FnGauge::new(
        "cortex_ingester_reactive_limiter_inflight_requests",
        "Ingester reactive limiter inflight requests.",
        &labels,
        move || l2.max_inflight() as f64,
    )));
    let l3 = Arc::clone(limiter);
    let _ = r.register(Box::new(FnGauge::new(
        "cortex_ingester_reactive_limiter_queued_requests",
        "Ingester reactive limiter queued requests.",
        &labels,
        move || l3.queued() as f64,
    )));
}

fn register_prioritizer_metrics(prioritizer: &Arc<Prioritizer>, reg: Option<&Registry>) {
    let Some(r) = reg else {
        return;
    };
    let p1 = Arc::clone(prioritizer);
    let _ = r.register(Box::new(FnGauge::new(
        "cortex_ingester_rejection_rate",
        "The prioritized rate at which requests should be rejected.",
        &[],
        move || p1.rejection_rate(),
    )));
    let p2 = Arc::clone(prioritizer);
    let _ = r.register(Box::new(FnGauge::new(
        "cortex_ingester_rejection_threshold",
        "The priority threshold below which requests should be rejected.",
        &[],
        move || f64::from(p2.rejection_threshold()),
    )));
}

/// The ingester reactive limiter: one limiter for push and one for read requests, plus a shared
/// Prioritizer when both are enabled (Go: `ingesterReactiveLimiter`).
pub struct IngesterReactiveLimiter {
    pub prioritizer: Option<Arc<Prioritizer>>,
    /// Calibration interval of the prioritizer timer service (only when `prioritizer` is set).
    pub calibration_interval: Duration,
    pub push: Option<Arc<IngesterRl>>,
    pub read: Option<Arc<IngesterRl>>,
}

impl IngesterReactiveLimiter {
    /// Go: `newIngesterReactiveLimiter`.
    pub fn new(
        prioritizer_cfg: &PrioritizerConfig,
        push_cfg: &ReactiveLimiterConfig,
        read_cfg: &ReactiveLimiterConfig,
        reg: Option<&Registry>,
    ) -> Result<Self, String> {
        let prioritizer = if push_cfg.enabled && read_cfg.enabled {
            let p = Arc::new(Prioritizer::new());
            register_prioritizer_metrics(&p, reg);
            Some(p)
        } else {
            None
        };
        let build = |cfg: &ReactiveLimiterConfig,
                     request_type: &str,
                     priority: Priority|
         -> Result<Option<Arc<IngesterRl>>, String> {
            if !cfg.enabled {
                return Ok(None);
            }
            let l = match prioritizer {
                Some(ref p) => IngesterRl::new_with_priority(cfg, priority, Arc::clone(p))?,
                None => IngesterRl::new(cfg)?,
            };
            register_rl_metrics(&l, request_type, reg);
            Ok(Some(l))
        };
        let push = build(push_cfg, "push", Priority::Low)?;
        let read = build(read_cfg, "read", Priority::High)?;
        Ok(Self {
            calibration_interval: Duration::from_nanos(
                prioritizer_cfg.calibration_interval.as_nanos().max(0) as u64,
            ),
            prioritizer,
            push,
            read,
        })
    }

    /// Runs the prioritizer calibration timer service until `stop` resolves
    /// (Go: `services.NewTimerService(CalibrationInterval, nil, l.update, nil)`).
    /// Returns immediately when no prioritizer is configured.
    pub async fn run_calibration(
        prioritizer: Arc<Prioritizer>,
        interval: Duration,
        stop: impl std::future::Future<Output = ()>,
    ) {
        tokio::pin!(stop);
        let interval = interval.max(Duration::from_millis(1));
        let mut ticker =
            tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
        loop {
            tokio::select! {
                _ = &mut stop => break,
                _ = ticker.tick() => prioritizer.calibrate(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enabled_cfg() -> ReactiveLimiterConfig {
        ReactiveLimiterConfig {
            enabled: true,
            ..ReactiveLimiterConfig::default()
        }
    }

    fn gauge(reg: &Registry, name: &str) -> Option<f64> {
        reg.gather()
            .into_iter()
            .find(|mf| mf.get_name() == name)
            .map(|mf| mf.get_metric()[0].get_gauge().get_value())
    }

    // Upstream: pkg/ingester/reactivelimiter_test.go::Test_ingesterReactiveLimiter
    // ("newReactiveLimiter with Prioritizer": building a prioritized limiter works).
    #[test]
    fn new_reactive_limiter_with_prioritizer() {
        let mut cfg = ReactiveLimiterConfig {
            enabled: true,
            ..ReactiveLimiterConfig::default()
        };
        cfg.max_limit_factor = 1.0;
        cfg.min_limit_factor = 1.2;
        cfg.initial_rejection_factor = 1.0;
        cfg.max_rejection_factor = 2.0;
        let p = Arc::new(Prioritizer::new());
        let l = IngesterRl::new_with_priority(&cfg, Priority::High, Arc::clone(&p)).unwrap();
        assert_eq!(p.registered_policies(), 1);
        assert!(l.can_acquire_permit());
    }

    #[test]
    fn new_ingester_reactive_limiter() {
        let pcfg = PrioritizerConfig::default();
        let on = enabled_cfg();
        let off = ReactiveLimiterConfig::default();

        // Only push enabled: no prioritizer.
        let reg = Registry::new();
        let l = IngesterReactiveLimiter::new(&pcfg, &on, &off, Some(&reg)).unwrap();
        assert!(l.prioritizer.is_none());
        assert!(l.push.is_some());
        assert!(l.read.is_none());
        assert!(gauge(&reg, "cortex_ingester_rejection_rate").is_none());

        // Only read enabled: no prioritizer.
        let l = IngesterReactiveLimiter::new(&pcfg, &off, &on, None).unwrap();
        assert!(l.prioritizer.is_none());
        assert!(l.push.is_none());
        assert!(l.read.is_some());

        // Both enabled: shared prioritizer with both limiters registered.
        let reg = Registry::new();
        let l = IngesterReactiveLimiter::new(&pcfg, &on, &on, Some(&reg)).unwrap();
        let p = l.prioritizer.clone().unwrap();
        assert_eq!(p.registered_policies(), 2);
        assert_eq!(l.calibration_interval, Duration::from_secs(1));
        assert_eq!(gauge(&reg, "cortex_ingester_rejection_rate"), Some(0.0));
        assert_eq!(gauge(&reg, "cortex_ingester_rejection_threshold"), Some(0.0));
        assert!(l.push.as_ref().unwrap().can_acquire_permit());
        assert!(l.read.as_ref().unwrap().can_acquire_permit());
    }

    #[tokio::test]
    async fn prioritized_limiter_rejects_below_threshold() {
        let mut cfg = enabled_cfg();
        cfg.min_limit = 1;
        cfg.initial_limit = 1;
        cfg.max_limit = 1;
        let l = IngesterReactiveLimiter::new(&PrioritizerConfig::default(), &cfg, &cfg, None)
            .unwrap();
        let p = l.prioritizer.clone().unwrap();
        let push = l.push.clone().unwrap();
        let read = l.read.clone().unwrap();

        // Fill both limiters (limit 1 each).
        let pp = push.acquire_permit().await.unwrap();
        let rp = read.acquire_permit().await.unwrap();

        // Queue enough waiters on push to exceed the combined max queue (2 * 3 = 6).
        let mut waiters = Vec::new();
        for _ in 0..6 {
            let push_c = Arc::clone(&push);
            waiters.push(tokio::spawn(async move {
                // Each records a low-priority level since the limiter is full.
                let _ = push_c.acquire_permit().await;
            }));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(push.queued(), 6);

        p.calibrate();
        assert_eq!(p.rejection_rate(), 1.0);
        let thresh = p.rejection_threshold();
        assert!((100..200).contains(&thresh), "threshold={thresh}");
        // Pushes (levels 100-199) are mostly rejected, reads (300-399) always admitted.
        assert!(read.can_acquire_permit());
        let rejected = (0..200).filter(|_| !push.can_acquire_permit()).count();
        assert!(rejected > 0 || thresh == 100);

        drop(pp);
        drop(rp);
        for w in waiters {
            let _ = tokio::time::timeout(Duration::from_secs(5), w).await;
        }
    }

    #[tokio::test]
    async fn calibration_service_calibrates_and_stops() {
        let p = Arc::new(Prioritizer::new());
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let c = Arc::clone(&calls);
        p.register(move || {
            c.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            QueueStats::default()
        });
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let h = tokio::spawn(IngesterReactiveLimiter::run_calibration(
            Arc::clone(&p),
            Duration::from_millis(10),
            async move {
                let _ = rx.await;
            },
        ));
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _ = tx.send(());
        h.await.unwrap();
        assert!(calls.load(std::sync::atomic::Ordering::Relaxed) >= 3);
    }
}
