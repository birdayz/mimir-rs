//! Server-side push & read circuit breakers matching `pkg/ingester/circuitbreaker.go`.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use mimir_config::CircuitBreakerConfig;
use parking_lot::Mutex;
use prometheus::{Counter, CounterVec, Opts, Registry};
use tracing::{info, warn};

use crate::errors::IngesterError;

pub const CIRCUIT_BREAKER_PUSH_REQUEST_TYPE: &str = "push";
pub const CIRCUIT_BREAKER_READ_REQUEST_TYPE: &str = "read";

const RESULT_SUCCESS: &str = "success";
const RESULT_ERROR: &str = "error";
const RESULT_OPEN: &str = "circuit_breaker_open";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum BreakerState {
    Closed = 0,
    Open = 1,
    HalfOpen = 2,
}

impl BreakerState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Closed => "Closed",
            Self::Open => "Open",
            Self::HalfOpen => "HalfOpen",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
enum ActivationState {
    Inactive = 0,
    Pending = 1,
    Activating = 2,
    Active = 3,
}

struct BreakerMetrics {
    transitions: CounterVec,
    results: CounterVec,
    request_timeouts: Counter,
}

impl BreakerMetrics {
    fn new(reg: Option<&Registry>, request_type: &str) -> Self {
        let mut cl = HashMap::new();
        cl.insert("request_type".to_string(), request_type.to_string());

        let transitions = CounterVec::new(
            Opts::new(
                "cortex_ingester_circuit_breaker_transitions_total",
                "Number of times the circuit breaker has entered a state.",
            )
            .const_labels(cl.clone()),
            &["state"],
        )
        .expect("valid cb transitions counter");

        let results = CounterVec::new(
            Opts::new(
                "cortex_ingester_circuit_breaker_results_total",
                "Results of executing requests via the circuit breaker.",
            )
            .const_labels(cl.clone()),
            &["result"],
        )
        .expect("valid cb results counter");

        let request_timeouts = Counter::with_opts(
            Opts::new(
                "cortex_ingester_circuit_breaker_request_timeouts_total",
                "Number of times the circuit breaker recorded a request that reached timeout.",
            )
            .const_labels(cl),
        )
        .expect("valid cb timeouts counter");

        for s in ["Open", "HalfOpen", "Closed"] {
            let _ = transitions.with_label_values(&[s]);
        }
        for r in [RESULT_SUCCESS, RESULT_ERROR, RESULT_OPEN] {
            let _ = results.with_label_values(&[r]);
        }

        if let Some(reg) = reg {
            let _ = reg.register(Box::new(transitions.clone()));
            let _ = reg.register(Box::new(results.clone()));
            let _ = reg.register(Box::new(request_timeouts.clone()));
        }

        Self {
            transitions,
            results,
            request_timeouts,
        }
    }
}

struct BreakerInner {
    state: BreakerState,
    opened_at: Option<Instant>,
    half_open_in_flight: usize,
    half_open_successes: usize,
    outcomes: VecDeque<(Instant, bool)>, // (timestamp, is_failure)
}

pub struct CircuitBreaker {
    cfg: CircuitBreakerConfig,
    request_type: &'static str,
    cooldown: Duration,
    thresholding_period: Duration,
    request_timeout: Duration,
    initial_delay: Duration,
    test_mode_count_based: bool,
    activation_state: AtomicU8,
    activation_needed: AtomicBool,
    inner: Mutex<BreakerInner>,
    metrics: BreakerMetrics,
}

impl CircuitBreaker {
    pub fn new(
        cfg: &CircuitBreakerConfig,
        reg: Option<&Registry>,
        request_type: &'static str,
        test_mode_count_based: bool,
    ) -> Option<Arc<Self>> {
        if !cfg.enabled {
            return None;
        }
        let cooldown = Duration::from_nanos(cfg.cooldown_period.as_nanos().max(1) as u64);
        let thresholding_period =
            Duration::from_nanos(cfg.thresholding_period.as_nanos().max(1) as u64);
        let request_timeout =
            Duration::from_nanos(cfg.request_timeout.as_nanos().max(1) as u64);
        let initial_delay = Duration::from_nanos(cfg.initial_delay.as_nanos().max(0) as u64);

        Some(Arc::new(Self {
            cfg: cfg.clone(),
            request_type,
            cooldown,
            thresholding_period,
            request_timeout,
            initial_delay,
            test_mode_count_based,
            activation_state: AtomicU8::new(ActivationState::Inactive as u8),
            activation_needed: AtomicBool::new(false),
            inner: Mutex::new(BreakerInner {
                state: BreakerState::Closed,
                opened_at: None,
                half_open_in_flight: 0,
                half_open_successes: 0,
                outcomes: VecDeque::new(),
            }),
            metrics: BreakerMetrics::new(reg, request_type),
        }))
    }

    pub fn is_active(&self) -> bool {
        self.activation_state.load(Ordering::Acquire) == ActivationState::Active as u8
    }

    pub fn state(&self) -> BreakerState {
        let mut inner = self.inner.lock();
        self.maybe_transition_to_half_open(&mut inner, Instant::now());
        inner.state
    }

    pub fn is_open(&self) -> bool {
        self.is_active() && self.state() == BreakerState::Open
    }

    pub fn remaining_delay(&self) -> Duration {
        let inner = self.inner.lock();
        if inner.state != BreakerState::Open {
            return Duration::ZERO;
        }
        match inner.opened_at {
            Some(at) => self.cooldown.saturating_sub(at.elapsed()),
            None => Duration::ZERO,
        }
    }

    pub fn activate(self: &Arc<Self>) {
        let _ = self.activation_state.compare_exchange(
            ActivationState::Inactive as u8,
            if !self.initial_delay.is_zero() {
                self.activation_needed.store(true, Ordering::Release);
                ActivationState::Pending as u8
            } else {
                info!(request_type = self.request_type, "activating circuit breaker");
                ActivationState::Active as u8
            },
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    pub fn deactivate(&self) {
        self.activation_needed.store(false, Ordering::Release);
        let prev = self
            .activation_state
            .swap(ActivationState::Inactive as u8, Ordering::AcqRel);
        if prev != ActivationState::Inactive as u8 {
            info!(request_type = self.request_type, "deactivating circuit breaker");
        }
    }

    fn schedule_activation(self: &Arc<Self>) {
        if self
            .activation_state
            .compare_exchange(
                ActivationState::Pending as u8,
                ActivationState::Activating as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return;
        }
        let delay = self.initial_delay;
        let me = Arc::clone(self);
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            let _ = me.activation_state.compare_exchange(
                ActivationState::Activating as u8,
                ActivationState::Active as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
        });
    }

    fn maybe_transition_to_half_open(&self, inner: &mut BreakerInner, now: Instant) {
        if inner.state == BreakerState::Open {
            if let Some(opened_at) = inner.opened_at {
                if now.saturating_duration_since(opened_at) >= self.cooldown {
                    inner.state = BreakerState::HalfOpen;
                    inner.half_open_in_flight = 0;
                    inner.half_open_successes = 0;
                    self.metrics.transitions.with_label_values(&["HalfOpen"]).inc();
                    info!(request_type = self.request_type, "circuit breaker is half-open");
                }
            }
        }
    }

    pub fn try_acquire_permit(self: &Arc<Self>) -> Result<Option<BreakerPermit>, IngesterError> {
        let act = self.activation_state.load(Ordering::Acquire);
        if act == ActivationState::Inactive as u8 || act == ActivationState::Activating as u8 {
            return Ok(None);
        }
        if act == ActivationState::Pending as u8 {
            if self.activation_needed.swap(false, Ordering::AcqRel) {
                self.schedule_activation();
            }
            return Ok(None);
        }

        let now = Instant::now();
        let mut inner = self.inner.lock();
        self.maybe_transition_to_half_open(&mut inner, now);

        match inner.state {
            BreakerState::Closed => Ok(Some(BreakerPermit {
                breaker: Arc::clone(self),
                is_half_open: false,
            })),
            BreakerState::Open => {
                let rem = inner
                    .opened_at
                    .map(|at| self.cooldown.saturating_sub(now.saturating_duration_since(at)))
                    .unwrap_or(Duration::ZERO);
                self.metrics.results.with_label_values(&[RESULT_OPEN]).inc();
                Err(IngesterError::CircuitBreakerOpen {
                    request_type: self.request_type.to_string(),
                    remaining_delay: rem,
                })
            }
            BreakerState::HalfOpen => {
                // Allow a single probe at a time while half-open
                if inner.half_open_in_flight >= 1 {
                    self.metrics.results.with_label_values(&[RESULT_OPEN]).inc();
                    return Err(IngesterError::CircuitBreakerOpen {
                        request_type: self.request_type.to_string(),
                        remaining_delay: Duration::ZERO,
                    });
                }
                inner.half_open_in_flight += 1;
                Ok(Some(BreakerPermit {
                    breaker: Arc::clone(self),
                    is_half_open: true,
                }))
            }
        }
    }

    pub fn finish_request(&self, duration: Duration, timed_out_or_deadline: bool, is_half_open: bool) {
        if !self.is_active() {
            return;
        }
        let is_failure = timed_out_or_deadline || duration > self.request_timeout;
        let now = Instant::now();
        let mut inner = self.inner.lock();

        if is_half_open && inner.state == BreakerState::HalfOpen {
            inner.half_open_in_flight = inner.half_open_in_flight.saturating_sub(1);
        }

        if is_failure {
            self.metrics.request_timeouts.inc();
            self.metrics.results.with_label_values(&[RESULT_ERROR]).inc();

            if inner.state == BreakerState::HalfOpen {
                inner.state = BreakerState::Open;
                inner.opened_at = Some(now);
                self.metrics.transitions.with_label_values(&["Open"]).inc();
                warn!(request_type = self.request_type, "circuit breaker is open");
                return;
            }

            if inner.state == BreakerState::Closed {
                inner.outcomes.push_back((now, true));
                self.prune_outcomes(&mut inner, now);
                if self.should_open(&inner) {
                    inner.state = BreakerState::Open;
                    inner.opened_at = Some(now);
                    inner.outcomes.clear();
                    self.metrics.transitions.with_label_values(&["Open"]).inc();
                    warn!(request_type = self.request_type, "circuit breaker is open");
                }
            }
        } else {
            self.metrics.results.with_label_values(&[RESULT_SUCCESS]).inc();
            if inner.state == BreakerState::HalfOpen {
                inner.half_open_successes += 1;
                if inner.half_open_successes >= 1 {
                    inner.state = BreakerState::Closed;
                    inner.opened_at = None;
                    inner.outcomes.clear();
                    self.metrics.transitions.with_label_values(&["Closed"]).inc();
                    info!(request_type = self.request_type, "circuit breaker is closed");
                }
            } else if inner.state == BreakerState::Closed {
                inner.outcomes.push_back((now, false));
                self.prune_outcomes(&mut inner, now);
            }
        }
    }

    fn prune_outcomes(&self, inner: &mut BreakerInner, now: Instant) {
        if self.test_mode_count_based {
            while inner.outcomes.len() > self.cfg.failure_execution_threshold.max(1) as usize {
                inner.outcomes.pop_front();
            }
            return;
        }
        let cutoff = now.checked_sub(self.thresholding_period);
        if let Some(c) = cutoff {
            while let Some(&(ts, _)) = inner.outcomes.front() {
                if ts < c {
                    inner.outcomes.pop_front();
                } else {
                    break;
                }
            }
        }
    }

    fn should_open(&self, inner: &BreakerInner) -> bool {
        if self.test_mode_count_based {
            let failures = inner.outcomes.iter().filter(|(_, f)| *f).count() as u64;
            return failures >= self.cfg.failure_threshold_percentage.max(1);
        }
        let total = inner.outcomes.len() as u64;
        if total < self.cfg.failure_execution_threshold {
            return false;
        }
        let failures = inner.outcomes.iter().filter(|(_, f)| *f).count() as u64;
        (failures * 100) >= (total * self.cfg.failure_threshold_percentage)
    }
}

pub struct BreakerPermit {
    breaker: Arc<CircuitBreaker>,
    is_half_open: bool,
}

impl BreakerPermit {
    pub fn finish(self, duration: Duration, deadline_exceeded: bool) {
        self.breaker
            .finish_request(duration, deadline_exceeded, self.is_half_open);
    }
}

pub struct IngesterCircuitBreaker {
    pub push: Option<Arc<CircuitBreaker>>,
    pub read: Option<Arc<CircuitBreaker>>,
}

impl IngesterCircuitBreaker {
    pub fn new(
        push_cfg: &CircuitBreakerConfig,
        read_cfg: &CircuitBreakerConfig,
        reg: Option<&Registry>,
    ) -> Self {
        Self {
            push: CircuitBreaker::new(push_cfg, reg, CIRCUIT_BREAKER_PUSH_REQUEST_TYPE, false),
            read: CircuitBreaker::new(read_cfg, reg, CIRCUIT_BREAKER_READ_REQUEST_TYPE, false),
        }
    }

    pub fn try_acquire_push_permit(&self) -> Result<Option<BreakerPermit>, IngesterError> {
        match &self.push {
            Some(cb) => cb.try_acquire_permit(),
            None => Ok(None),
        }
    }

    pub fn try_acquire_read_permit(&self) -> Result<Option<BreakerPermit>, IngesterError> {
        let Some(ref read_cb) = self.read else {
            return Ok(None);
        };
        if !read_cb.is_active() {
            return Ok(None);
        }
        if let Some(ref push_cb) = self.push {
            if push_cb.is_open() {
                return Err(IngesterError::CircuitBreakerOpen {
                    request_type: CIRCUIT_BREAKER_PUSH_REQUEST_TYPE.to_string(),
                    remaining_delay: push_cb.remaining_delay(),
                });
            }
        }
        read_cb.try_acquire_permit()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn circuit_breaker_opens_and_recovers_via_half_open_probe() {
        let cfg = CircuitBreakerConfig {
            enabled: true,
            failure_threshold_percentage: 2,
            failure_execution_threshold: 5,
            cooldown_period: mimir_config::GoDuration::from_millis(25),
            request_timeout: mimir_config::GoDuration::from_millis(10),
            ..Default::default()
        };

        let reg = Registry::new();
        let cb = CircuitBreaker::new(&cfg, Some(&reg), "push", true).unwrap();
        cb.activate();
        assert_eq!(cb.state(), BreakerState::Closed);

        // Two timeouts open the breaker in test_mode_count_based
        for _ in 0..2 {
            let p = cb.try_acquire_permit().unwrap().unwrap();
            p.finish(Duration::from_millis(20), true);
        }
        assert_eq!(cb.state(), BreakerState::Open);
        assert!(cb.try_acquire_permit().is_err());

        std::thread::sleep(Duration::from_millis(35));
        assert_eq!(cb.state(), BreakerState::HalfOpen);

        // First half-open probe succeeds -> Closed
        let p = cb.try_acquire_permit().unwrap().unwrap();
        p.finish(Duration::from_millis(1), false);
        assert_eq!(cb.state(), BreakerState::Closed);
    }
}
