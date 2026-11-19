//! Server-side push & read circuit breakers matching `pkg/ingester/circuitbreaker.go`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use mimir_config::CircuitBreakerConfig;
use parking_lot::Mutex;
use prometheus::core::{Collector, Desc};
use prometheus::proto::MetricFamily;
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
            Self::Closed => "closed",
            Self::Open => "open",
            Self::HalfOpen => "half-open",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum ActivationState {
    Inactive = 0,
    Pending = 1,
    Activating = 2,
    Active = 3,
}

/// Error classification passed to `CircuitBreaker::try_record_failure` / `record_result`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BreakerError {
    Canceled,
    DeadlineExceeded,
    Status(tonic::Code),
    Ingester(IngesterError),
    Other(String),
}

impl BreakerError {
    pub fn is_deadline_exceeded(&self) -> bool {
        match self {
            Self::DeadlineExceeded => true,
            Self::Status(code) => *code == tonic::Code::DeadlineExceeded,
            Self::Ingester(e) => e.is_deadline_exceeded(),
            _ => false,
        }
    }

    pub fn from_status(status: &tonic::Status) -> Self {
        Self::Status(status.code())
    }
}

struct CurrentStateCollector {
    desc: Desc,
    target_state: BreakerState,
    state_fn: Box<dyn Fn() -> BreakerState + Send + Sync>,
}

impl CurrentStateCollector {
    fn new(
        request_type: &str,
        target_state: BreakerState,
        state_fn: impl Fn() -> BreakerState + Send + Sync + 'static,
    ) -> Self {
        let mut const_labels = HashMap::new();
        const_labels.insert("request_type".to_string(), request_type.to_string());
        const_labels.insert("state".to_string(), target_state.as_str().to_string());
        let desc = Desc::new(
            "cortex_ingester_circuit_breaker_current_state".to_string(),
            "Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name."
                .to_string(),
            vec![],
            const_labels,
        )
        .expect("valid cb current_state desc");
        Self {
            desc,
            target_state,
            state_fn: Box::new(state_fn),
        }
    }
}

impl Collector for CurrentStateCollector {
    fn desc(&self) -> Vec<&Desc> {
        vec![&self.desc]
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let val = if (self.state_fn)() == self.target_state {
            1.0
        } else {
            0.0
        };
        let mut g = prometheus::proto::Gauge::default();
        g.set_value(val);
        let mut m = prometheus::proto::Metric::default();
        m.set_gauge(g);
        m.set_label(self.desc.const_label_pairs.to_vec().into());
        let mut mf = MetricFamily::default();
        mf.set_name(self.desc.fq_name.clone());
        mf.set_help(self.desc.help.clone());
        mf.set_field_type(prometheus::proto::MetricType::GAUGE);
        mf.set_metric(vec![m].into());
        vec![mf]
    }
}

struct BreakerMetrics {
    transitions: CounterVec,
    results: CounterVec,
    request_timeouts: Counter,
}

impl BreakerMetrics {
    fn new(
        reg: Option<&Registry>,
        request_type: &str,
        state_fn: impl Fn() -> BreakerState + Send + Sync + Clone + 'static,
    ) -> Self {
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

        for s in [
            BreakerState::Open,
            BreakerState::HalfOpen,
            BreakerState::Closed,
        ] {
            let _ = transitions.with_label_values(&[s.as_str()]);
            if let Some(reg) = reg {
                let sf = state_fn.clone();
                let _ = reg.register(Box::new(CurrentStateCollector::new(
                    request_type,
                    s,
                    sf,
                )));
            }
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

const DEFAULT_BUCKET_COUNT: usize = 10;

/// Matches `failsafe-go/internal/util/util.go:Round`: 2-decimal rounding.
#[inline]
fn round_2_decimals(v: f64) -> f64 {
    (v * 100.0).round() / 100.0
}

/// Circular bit-ring stats matching `failsafe-go/internal/util/stats.go:countingStats`.
struct CountingStats {
    bits: Vec<u64>,
    len: usize,
    head: usize,
    occupied_bits: usize,
    successes: usize,
    failures: usize,
}

impl CountingStats {
    fn new(size: usize) -> Self {
        let len = size.max(1);
        Self {
            bits: vec![0; len.div_ceil(64)],
            len,
            head: 0,
            occupied_bits: 0,
            successes: 0,
            failures: 0,
        }
    }

    #[inline]
    fn get_bit(&self, idx: usize) -> bool {
        (self.bits[idx / 64] >> (idx % 64)) & 1 != 0
    }

    #[inline]
    fn set_bit(&mut self, idx: usize, val: bool) {
        let word = &mut self.bits[idx / 64];
        let mask = 1u64 << (idx % 64);
        if val {
            *word |= mask;
        } else {
            *word &= !mask;
        }
    }

    fn set_next(&mut self, value: bool) {
        if self.occupied_bits < self.len {
            self.occupied_bits += 1;
        } else if self.get_bit(self.head) {
            self.successes = self.successes.saturating_sub(1);
        } else {
            self.failures = self.failures.saturating_sub(1);
        }
        if value {
            self.successes += 1;
        } else {
            self.failures += 1;
        }
        self.set_bit(self.head, value);
        self.head = (self.head + 1) % self.len;
    }

    #[inline]
    fn execution_count(&self) -> usize {
        self.occupied_bits
    }

    #[inline]
    fn failure_count(&self) -> usize {
        self.failures
    }

    #[inline]
    fn success_count(&self) -> usize {
        self.successes
    }

    #[inline]
    fn failure_rate(&self) -> f64 {
        if self.occupied_bits == 0 {
            0.0
        } else {
            round_2_decimals(self.failures as f64 / self.occupied_bits as f64)
        }
    }

    #[inline]
    fn success_rate(&self) -> f64 {
        if self.occupied_bits == 0 {
            0.0
        } else {
            round_2_decimals(self.successes as f64 / self.occupied_bits as f64)
        }
    }

    #[inline]
    fn record_failure(&mut self) {
        self.set_next(false);
    }

    #[inline]
    fn record_success(&mut self) {
        self.set_next(true);
    }

    fn reset(&mut self) {
        self.bits.fill(0);
        self.head = 0;
        self.occupied_bits = 0;
        self.successes = 0;
        self.failures = 0;
    }
}

#[derive(Clone, Copy, Default)]
struct TimedBucket {
    successes: usize,
    failures: usize,
}

/// O(1) 10-bucket sliding time window matching `failsafe-go/internal/util/stats.go:timedStats`
/// and `windows.go:BucketedWindow`.
struct TimedStats {
    /// Monotonic anchor paired with `epoch_unix_nanos`: Go's `BucketedWindow` derives the head
    /// bucket from `Clock.Now().UnixNano() / BucketNanos`, i.e. bucket boundaries are aligned to
    /// the Unix epoch rather than to the breaker's creation time.
    epoch: Instant,
    epoch_unix_nanos: u128,
    bucket_nanos: u128,
    buckets: [TimedBucket; DEFAULT_BUCKET_COUNT],
    summary: TimedBucket,
    head_time: u64,
}

impl TimedStats {
    fn new(thresholding_period: Duration, epoch: Instant, epoch_unix_nanos: u128) -> Self {
        let bucket_nanos = (thresholding_period / DEFAULT_BUCKET_COUNT as u32)
            .as_nanos()
            .max(1);
        Self {
            epoch,
            epoch_unix_nanos,
            bucket_nanos,
            buckets: [TimedBucket::default(); DEFAULT_BUCKET_COUNT],
            summary: TimedBucket::default(),
            head_time: 0,
        }
    }

    fn expire_buckets(&mut self, now: Instant) -> &mut TimedBucket {
        let unix_nanos = self.epoch_unix_nanos + now.saturating_duration_since(self.epoch).as_nanos();
        let new_head = (unix_nanos / self.bucket_nanos) as u64;
        if new_head > self.head_time {
            let buckets_to_move = (DEFAULT_BUCKET_COUNT as u64).min(new_head - self.head_time);
            for i in 0..buckets_to_move {
                let idx = ((self.head_time + i + 1) % DEFAULT_BUCKET_COUNT as u64) as usize;
                let bucket = &mut self.buckets[idx];
                self.summary.successes = self.summary.successes.saturating_sub(bucket.successes);
                self.summary.failures = self.summary.failures.saturating_sub(bucket.failures);
                *bucket = TimedBucket::default();
            }
            self.head_time = new_head;
        }
        &mut self.buckets[(self.head_time % DEFAULT_BUCKET_COUNT as u64) as usize]
    }

    #[inline]
    fn execution_count(&self) -> usize {
        self.summary.successes + self.summary.failures
    }

    #[inline]
    fn failure_count(&self) -> usize {
        self.summary.failures
    }

    #[inline]
    fn failure_rate(&self) -> f64 {
        let executions = self.execution_count();
        if executions == 0 {
            0.0
        } else {
            round_2_decimals(self.summary.failures as f64 / executions as f64)
        }
    }

    #[inline]
    fn record_failure(&mut self, now: Instant) {
        self.expire_buckets(now).failures += 1;
        self.summary.failures += 1;
    }

    #[inline]
    fn record_success(&mut self, now: Instant) {
        self.expire_buckets(now).successes += 1;
        self.summary.successes += 1;
    }
}

enum ClosedStats {
    Timed(TimedStats),
    Counting(CountingStats),
}

impl ClosedStats {
    #[inline]
    fn execution_count(&self) -> usize {
        match self {
            Self::Timed(s) => s.execution_count(),
            Self::Counting(s) => s.execution_count(),
        }
    }

    #[inline]
    fn failure_count(&self) -> usize {
        match self {
            Self::Timed(s) => s.failure_count(),
            Self::Counting(s) => s.failure_count(),
        }
    }

    #[inline]
    fn failure_rate(&self) -> f64 {
        match self {
            Self::Timed(s) => s.failure_rate(),
            Self::Counting(s) => s.failure_rate(),
        }
    }

    #[inline]
    fn record_failure(&mut self, now: Instant) {
        match self {
            Self::Timed(s) => s.record_failure(now),
            Self::Counting(s) => s.record_failure(),
        }
    }

    #[inline]
    fn record_success(&mut self, now: Instant) {
        match self {
            Self::Timed(s) => s.record_success(now),
            Self::Counting(s) => s.record_success(),
        }
    }
}

struct BreakerInner {
    state: BreakerState,
    opened_at: Option<Instant>,
    half_open_in_flight: usize,
    half_open_stats: CountingStats,
    closed_stats: ClosedStats,
}

pub struct CircuitBreaker {
    cfg: CircuitBreakerConfig,
    request_type: &'static str,
    cooldown: Duration,
    thresholding_period: Duration,
    /// `(Instant, UnixNano)` pair captured at construction, used to align `TimedStats` buckets
    /// to the Unix epoch like failsafe-go's wall-clock `BucketedWindow`.
    unix_anchor: (Instant, u128),
    request_timeout: Duration,
    initial_delay: Duration,
    test_mode_count_based: AtomicBool,
    test_request_delay_ns: AtomicU64,
    activation_state: AtomicU8,
    activation_needed: AtomicBool,
    activation_gen: AtomicU64,
    acquired_permits: AtomicI64,
    recorded_successes: AtomicI64,
    recorded_failures: AtomicI64,
    state_mtx: Mutex<()>,
    inner: Arc<Mutex<BreakerInner>>,
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

        let now = Instant::now();
        let unix_anchor = (
            now,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0),
        );
        let closed_stats = if test_mode_count_based {
            ClosedStats::Counting(CountingStats::new(
                cfg.failure_threshold_percentage.max(1) as usize,
            ))
        } else {
            ClosedStats::Timed(TimedStats::new(thresholding_period, unix_anchor.0, unix_anchor.1))
        };
        let half_open_cap = if test_mode_count_based {
            cfg.failure_threshold_percentage.max(1) as usize
        } else {
            cfg.failure_execution_threshold.max(1) as usize
        };
        let inner = Arc::new(Mutex::new(BreakerInner {
            state: BreakerState::Closed,
            opened_at: None,
            half_open_in_flight: 0,
            half_open_stats: CountingStats::new(half_open_cap),
            closed_stats,
        }));
        let inner_for_gauge = Arc::clone(&inner);
        let metrics = BreakerMetrics::new(reg, request_type, move || {
            inner_for_gauge.lock().state
        });

        Some(Arc::new(Self {
            cfg: cfg.clone(),
            request_type,
            cooldown,
            thresholding_period,
            unix_anchor,
            request_timeout,
            initial_delay,
            test_mode_count_based: AtomicBool::new(test_mode_count_based),
            test_request_delay_ns: AtomicU64::new(0),
            activation_state: AtomicU8::new(ActivationState::Inactive as u8),
            activation_needed: AtomicBool::new(false),
            activation_gen: AtomicU64::new(0),
            acquired_permits: AtomicI64::new(0),
            recorded_successes: AtomicI64::new(0),
            recorded_failures: AtomicI64::new(0),
            state_mtx: Mutex::new(()),
            inner,
            metrics,
        }))
    }

    fn new_closed_stats(&self) -> ClosedStats {
        if self.test_mode_count_based.load(Ordering::Acquire) {
            ClosedStats::Counting(CountingStats::new(
                self.cfg.failure_threshold_percentage.max(1) as usize,
            ))
        } else {
            ClosedStats::Timed(TimedStats::new(
                self.thresholding_period,
                self.unix_anchor.0,
                self.unix_anchor.1,
            ))
        }
    }

    pub fn enable_test_mode(&self) {
        self.test_mode_count_based.store(true, Ordering::Release);
        let mut inner = self.inner.lock();
        inner.closed_stats = ClosedStats::Counting(CountingStats::new(
            self.cfg.failure_threshold_percentage.max(1) as usize,
        ));
        inner.half_open_stats = CountingStats::new(self.half_open_capacity());
    }

    pub fn set_test_request_delay(&self, delay: Duration) {
        self.test_request_delay_ns
            .store(delay.as_nanos().min(u64::MAX as u128) as u64, Ordering::Release);
    }

    pub fn test_request_delay(&self) -> Duration {
        Duration::from_nanos(self.test_request_delay_ns.load(Ordering::Acquire))
    }

    pub fn acquired_permits(&self) -> i64 {
        self.acquired_permits.load(Ordering::Acquire)
    }

    pub fn recorded_successes(&self) -> i64 {
        self.recorded_successes.load(Ordering::Acquire)
    }

    pub fn recorded_failures(&self) -> i64 {
        self.recorded_failures.load(Ordering::Acquire)
    }

    pub fn activation_state(&self) -> ActivationState {
        match self.activation_state.load(Ordering::Acquire) {
            0 => ActivationState::Inactive,
            1 => ActivationState::Pending,
            2 => ActivationState::Activating,
            _ => ActivationState::Active,
        }
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

    pub fn force_open(&self) {
        let now = Instant::now();
        let mut inner = self.inner.lock();
        if inner.state != BreakerState::Open {
            inner.state = BreakerState::Open;
            inner.opened_at = Some(now);
            inner.half_open_in_flight = 0;
            inner.half_open_stats.reset();
            inner.closed_stats = self.new_closed_stats();
            self.metrics
                .transitions
                .with_label_values(&[BreakerState::Open.as_str()])
                .inc();
        }
    }

    pub fn force_close(&self) {
        let mut inner = self.inner.lock();
        if inner.state != BreakerState::Closed {
            inner.state = BreakerState::Closed;
            inner.opened_at = None;
            inner.half_open_in_flight = 0;
            inner.half_open_stats.reset();
            inner.closed_stats = self.new_closed_stats();
            self.metrics
                .transitions
                .with_label_values(&[BreakerState::Closed.as_str()])
                .inc();
        }
    }

    pub fn force_half_open(&self) {
        let mut inner = self.inner.lock();
        if inner.state != BreakerState::HalfOpen {
            inner.state = BreakerState::HalfOpen;
            inner.opened_at = None;
            inner.half_open_in_flight = 0;
            inner.half_open_stats = CountingStats::new(self.half_open_capacity());
            self.metrics
                .transitions
                .with_label_values(&[BreakerState::HalfOpen.as_str()])
                .inc();
        }
    }

    pub fn activate(&self) {
        let _g = self.state_mtx.lock();
        if self.activation_state.load(Ordering::Acquire) != ActivationState::Inactive as u8 {
            return;
        }
        if !self.initial_delay.is_zero() {
            self.activation_needed.store(true, Ordering::Release);
            self.activation_state
                .store(ActivationState::Pending as u8, Ordering::Release);
        } else {
            info!(request_type = self.request_type, "activating circuit breaker");
            self.activation_state
                .store(ActivationState::Active as u8, Ordering::Release);
        }
    }

    pub fn deactivate(&self) {
        let _g = self.state_mtx.lock();
        if self.activation_state.load(Ordering::Acquire) == ActivationState::Inactive as u8 {
            return;
        }
        info!(request_type = self.request_type, "deactivating circuit breaker");
        self.activation_needed.store(false, Ordering::Release);
        self.activation_gen.fetch_add(1, Ordering::AcqRel);
        self.activation_state
            .store(ActivationState::Inactive as u8, Ordering::Release);
    }

    pub fn schedule_activation(self: &Arc<Self>) {
        let gen = {
            let _g = self.state_mtx.lock();
            if self.activation_state.load(Ordering::Acquire) != ActivationState::Pending as u8 {
                return;
            }
            self.activation_state
                .store(ActivationState::Activating as u8, Ordering::Release);
            self.activation_gen.load(Ordering::Acquire)
        };
        let delay = self.initial_delay;
        let me = Arc::clone(self);
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                tokio::time::sleep(delay).await;
                let _g = me.state_mtx.lock();
                if me.activation_gen.load(Ordering::Acquire) != gen {
                    return;
                }
                if me.activation_state.load(Ordering::Acquire) != ActivationState::Activating as u8 {
                    return;
                }
                info!(request_type = me.request_type, "activating circuit breaker");
                me.activation_state
                    .store(ActivationState::Active as u8, Ordering::Release);
            });
        } else {
            std::thread::spawn(move || {
                std::thread::sleep(delay);
                let _g = me.state_mtx.lock();
                if me.activation_gen.load(Ordering::Acquire) != gen {
                    return;
                }
                if me.activation_state.load(Ordering::Acquire) != ActivationState::Activating as u8 {
                    return;
                }
                me.activation_state
                    .store(ActivationState::Active as u8, Ordering::Release);
            });
        }
    }

    fn half_open_capacity(&self) -> usize {
        if self.test_mode_count_based.load(Ordering::Acquire) {
            self.cfg.failure_threshold_percentage.max(1) as usize
        } else {
            self.cfg.failure_execution_threshold.max(1) as usize
        }
    }

    fn maybe_transition_to_half_open(&self, inner: &mut BreakerInner, now: Instant) {
        if inner.state == BreakerState::Open {
            if let Some(opened_at) = inner.opened_at {
                if now.saturating_duration_since(opened_at) >= self.cooldown {
                    inner.state = BreakerState::HalfOpen;
                    inner.half_open_in_flight = 0;
                    inner.half_open_stats = CountingStats::new(self.half_open_capacity());
                    self.metrics
                        .transitions
                        .with_label_values(&[BreakerState::HalfOpen.as_str()])
                        .inc();
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
            BreakerState::Closed => {
                self.acquired_permits.fetch_add(1, Ordering::AcqRel);
                Ok(Some(BreakerPermit {
                    breaker: Arc::clone(self),
                    is_half_open: false,
                    finished: false,
                }))
            }
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
                let cap = self.half_open_capacity();
                if inner.half_open_in_flight >= cap {
                    self.metrics.results.with_label_values(&[RESULT_OPEN]).inc();
                    return Err(IngesterError::CircuitBreakerOpen {
                        request_type: self.request_type.to_string(),
                        remaining_delay: Duration::ZERO,
                    });
                }
                inner.half_open_in_flight += 1;
                self.acquired_permits.fetch_add(1, Ordering::AcqRel);
                Ok(Some(BreakerPermit {
                    breaker: Arc::clone(self),
                    is_half_open: true,
                    finished: false,
                }))
            }
        }
    }

    /// Mirrors Go `circuitBreaker.tryRecordFailure(err)`:
    /// Only deadline-exceeded errors trip the breaker.
    pub fn try_record_failure(&self, err: Option<&BreakerError>) -> bool {
        let Some(err) = err else {
            return false;
        };
        if err.is_deadline_exceeded() {
            self.metrics.request_timeouts.inc();
            self.metrics.results.with_label_values(&[RESULT_ERROR]).inc();
            self.record_failure_inner(false);
            return true;
        }
        false
    }

    pub fn record_result(&self, errs: &[BreakerError]) -> Option<BreakerError> {
        if !self.is_active() {
            return None;
        }
        for err in errs {
            if self.try_record_failure(Some(err)) {
                return Some(err.clone());
            }
        }
        self.record_success_inner(false);
        self.metrics.results.with_label_values(&[RESULT_SUCCESS]).inc();
        None
    }

    pub fn finish_request_with_max_duration(
        &self,
        mut actual_duration: Duration,
        maximum_allowed_duration: Duration,
        err: Option<&BreakerError>,
        is_half_open: bool,
    ) -> Option<BreakerError> {
        if !self.is_active() {
            return None;
        }
        if self.test_mode_count_based.load(Ordering::Acquire) {
            actual_duration += self.test_request_delay();
        }
        let deadline_exceeded = maximum_allowed_duration < actual_duration;
        if let Some(e) = err {
            if e.is_deadline_exceeded() {
                self.metrics.request_timeouts.inc();
                self.metrics.results.with_label_values(&[RESULT_ERROR]).inc();
                self.record_failure_inner(is_half_open);
                return Some(e.clone());
            }
        }
        if deadline_exceeded {
            self.metrics.request_timeouts.inc();
            self.metrics.results.with_label_values(&[RESULT_ERROR]).inc();
            self.record_failure_inner(is_half_open);
            return Some(BreakerError::DeadlineExceeded);
        }
        self.record_success_inner(is_half_open);
        self.metrics.results.with_label_values(&[RESULT_SUCCESS]).inc();
        None
    }

    pub fn finish_request(&self, duration: Duration, timed_out_or_deadline: bool, is_half_open: bool) {
        let err = if timed_out_or_deadline {
            Some(BreakerError::DeadlineExceeded)
        } else {
            None
        };
        let _ = self.finish_request_with_max_duration(
            duration,
            self.request_timeout,
            err.as_ref(),
            is_half_open,
        );
    }

    fn abort_permit(&self, is_half_open: bool) {
        if is_half_open {
            let mut inner = self.inner.lock();
            if inner.state == BreakerState::HalfOpen {
                inner.half_open_in_flight = inner.half_open_in_flight.saturating_sub(1);
            }
        }
    }

    fn record_failure_inner(&self, is_half_open: bool) {
        self.recorded_failures.fetch_add(1, Ordering::AcqRel);
        let now = Instant::now();
        let mut inner = self.inner.lock();
        if is_half_open && inner.state == BreakerState::HalfOpen {
            inner.half_open_in_flight = inner.half_open_in_flight.saturating_sub(1);
        }
        match inner.state {
            BreakerState::HalfOpen => {
                inner.half_open_stats.record_failure();
                self.check_half_open_threshold(&mut inner, now);
            }
            BreakerState::Closed => {
                inner.closed_stats.record_failure(now);
                self.check_closed_threshold(&mut inner, now);
            }
            BreakerState::Open => {}
        }
    }

    fn record_success_inner(&self, is_half_open: bool) {
        self.recorded_successes.fetch_add(1, Ordering::AcqRel);
        let now = Instant::now();
        let mut inner = self.inner.lock();
        if is_half_open && inner.state == BreakerState::HalfOpen {
            inner.half_open_in_flight = inner.half_open_in_flight.saturating_sub(1);
        }
        match inner.state {
            BreakerState::HalfOpen => {
                inner.half_open_stats.record_success();
                self.check_half_open_threshold(&mut inner, now);
            }
            BreakerState::Closed => {
                inner.closed_stats.record_success(now);
                self.check_closed_threshold(&mut inner, now);
            }
            BreakerState::Open => {}
        }
    }

    fn check_closed_threshold(&self, inner: &mut BreakerInner, now: Instant) {
        let should_open = if self.test_mode_count_based.load(Ordering::Acquire) {
            inner.closed_stats.failure_count()
                >= self.cfg.failure_threshold_percentage.max(1) as usize
        } else {
            let exec_threshold = self.cfg.failure_execution_threshold as usize;
            if inner.closed_stats.execution_count() >= exec_threshold {
                let rate_threshold = self.cfg.failure_threshold_percentage as f64 / 100.0;
                if rate_threshold != 0.0 {
                    inner.closed_stats.failure_rate() >= rate_threshold
                } else {
                    true
                }
            } else {
                false
            }
        };
        if should_open {
            inner.state = BreakerState::Open;
            inner.opened_at = Some(now);
            inner.closed_stats = self.new_closed_stats();
            self.metrics
                .transitions
                .with_label_values(&[BreakerState::Open.as_str()])
                .inc();
            warn!(request_type = self.request_type, "circuit breaker is open");
        }
    }

    fn check_half_open_threshold(&self, inner: &mut BreakerInner, now: Instant) {
        let (failures_exceeded, successes_exceeded) =
            if self.test_mode_count_based.load(Ordering::Acquire) {
                let cap = self.cfg.failure_threshold_percentage.max(1) as usize;
                (
                    inner.half_open_stats.failure_count() >= cap,
                    inner.half_open_stats.success_count() > 0,
                )
            } else {
                let exec_threshold = self.cfg.failure_execution_threshold as usize;
                let exec_exceeded = inner.half_open_stats.execution_count() >= exec_threshold;
                let rate_threshold = self.cfg.failure_threshold_percentage as f64 / 100.0;
                if rate_threshold != 0.0 {
                    (
                        exec_exceeded && inner.half_open_stats.failure_rate() >= rate_threshold,
                        exec_exceeded && inner.half_open_stats.success_rate() > 1.0 - rate_threshold,
                    )
                } else {
                    (true, inner.half_open_stats.success_count() > 0)
                }
            };
        if successes_exceeded {
            inner.state = BreakerState::Closed;
            inner.opened_at = None;
            inner.half_open_in_flight = 0;
            inner.half_open_stats.reset();
            inner.closed_stats = self.new_closed_stats();
            self.metrics
                .transitions
                .with_label_values(&[BreakerState::Closed.as_str()])
                .inc();
            info!(request_type = self.request_type, "circuit breaker is closed");
        } else if failures_exceeded {
            inner.state = BreakerState::Open;
            inner.opened_at = Some(now);
            inner.half_open_in_flight = 0;
            inner.half_open_stats.reset();
            self.metrics
                .transitions
                .with_label_values(&[BreakerState::Open.as_str()])
                .inc();
            warn!(request_type = self.request_type, "circuit breaker is open");
        }
    }
}

/// Extension trait matching Go's nil-safe `*circuitBreaker` methods.
pub trait OptionalCircuitBreakerExt {
    fn try_record_failure_opt(&self, err: Option<&BreakerError>) -> bool;
    fn is_active_opt(&self) -> bool;
    fn activate_opt(&self);
    fn deactivate_opt(&self);
    fn is_open_opt(&self) -> bool;
    fn schedule_activation_opt(&self);
    fn try_acquire_permit_opt(&self) -> Result<Option<BreakerPermit>, IngesterError>;
    fn finish_request_opt(
        &self,
        actual_duration: Duration,
        max_duration: Duration,
        err: Option<&BreakerError>,
    ) -> Option<BreakerError>;
    fn record_result_opt(&self, errs: &[BreakerError]) -> Option<BreakerError>;
}

impl OptionalCircuitBreakerExt for Option<Arc<CircuitBreaker>> {
    fn try_record_failure_opt(&self, err: Option<&BreakerError>) -> bool {
        match self {
            Some(cb) => cb.try_record_failure(err),
            None => false,
        }
    }

    fn is_active_opt(&self) -> bool {
        self.as_ref().is_some_and(|cb| cb.is_active())
    }

    fn activate_opt(&self) {
        if let Some(cb) = self {
            cb.activate();
        }
    }

    fn deactivate_opt(&self) {
        if let Some(cb) = self {
            cb.deactivate();
        }
    }

    fn is_open_opt(&self) -> bool {
        self.as_ref().is_some_and(|cb| cb.is_open())
    }

    fn schedule_activation_opt(&self) {
        if let Some(cb) = self {
            cb.schedule_activation();
        }
    }

    fn try_acquire_permit_opt(&self) -> Result<Option<BreakerPermit>, IngesterError> {
        match self {
            Some(cb) => cb.try_acquire_permit(),
            None => Ok(None),
        }
    }

    fn finish_request_opt(
        &self,
        actual_duration: Duration,
        max_duration: Duration,
        err: Option<&BreakerError>,
    ) -> Option<BreakerError> {
        self.as_ref().and_then(|cb| {
            cb.finish_request_with_max_duration(actual_duration, max_duration, err, false)
        })
    }

    fn record_result_opt(&self, errs: &[BreakerError]) -> Option<BreakerError> {
        self.as_ref().and_then(|cb| cb.record_result(errs))
    }
}

pub struct BreakerPermit {
    breaker: Arc<CircuitBreaker>,
    is_half_open: bool,
    finished: bool,
}

impl BreakerPermit {
    pub fn finish(mut self, duration: Duration, deadline_exceeded: bool) {
        self.finished = true;
        self.breaker
            .finish_request(duration, deadline_exceeded, self.is_half_open);
    }

    pub fn finish_with_err(mut self, duration: Duration, err: Option<&BreakerError>) {
        self.finished = true;
        let _ = self.breaker.finish_request_with_max_duration(
            duration,
            self.breaker.request_timeout,
            err,
            self.is_half_open,
        );
    }
}

impl Drop for BreakerPermit {
    fn drop(&mut self) {
        if !self.finished {
            self.finished = true;
            self.breaker.abort_permit(self.is_half_open);
        }
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
        Self::new_with_test_mode(push_cfg, read_cfg, reg, false)
    }

    pub fn new_with_test_mode(
        push_cfg: &CircuitBreakerConfig,
        read_cfg: &CircuitBreakerConfig,
        reg: Option<&Registry>,
        test_mode_count_based: bool,
    ) -> Self {
        Self {
            push: CircuitBreaker::new(
                push_cfg,
                reg,
                CIRCUIT_BREAKER_PUSH_REQUEST_TYPE,
                test_mode_count_based,
            ),
            read: CircuitBreaker::new(
                read_cfg,
                reg,
                CIRCUIT_BREAKER_READ_REQUEST_TYPE,
                test_mode_count_based,
            ),
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

    #[test]
    fn scrape_does_not_suppress_half_open_transition_metric_and_ingester_deadline_counted() {
        let cfg = CircuitBreakerConfig {
            enabled: true,
            failure_threshold_percentage: 2,
            failure_execution_threshold: 5,
            cooldown_period: mimir_config::GoDuration::from_millis(20),
            request_timeout: mimir_config::GoDuration::from_millis(100),
            ..Default::default()
        };
        let reg = Registry::new();
        let cb = CircuitBreaker::new(&cfg, Some(&reg), "push", true).unwrap();
        cb.activate();
        cb.force_open();
        std::thread::sleep(Duration::from_millis(30));

        // Scraping /metrics before a request arrives must not skip the half-open transition counter.
        let _ = reg.gather();
        let p = cb.try_acquire_permit().unwrap().unwrap();
        p.finish(Duration::from_millis(1), false);

        let families = reg.gather();
        let half_open_count = families
            .iter()
            .find(|f| f.get_name() == "cortex_ingester_circuit_breaker_transitions_total")
            .and_then(|f| {
                f.get_metric().iter().find(|m| {
                    m.get_label()
                        .iter()
                        .any(|lp| lp.get_name() == "state" && lp.get_value() == "half-open")
                })
            })
            .map(|m| m.get_counter().get_value());
        assert_eq!(half_open_count, Some(1.0));

        let be = BreakerError::Ingester(IngesterError::DeadlineExceeded {
            message: "deadline".to_string(),
        });
        assert!(be.is_deadline_exceeded());
    }

    #[test]
    fn half_open_permits_multiple_probes_and_drop_releases_slot() {
        let cfg = CircuitBreakerConfig {
            enabled: true,
            failure_threshold_percentage: 50,
            failure_execution_threshold: 3,
            cooldown_period: mimir_config::GoDuration::from_millis(10),
            request_timeout: mimir_config::GoDuration::from_millis(100),
            ..Default::default()
        };
        let cb = CircuitBreaker::new(&cfg, None, "push", false).unwrap();
        cb.activate();
        cb.force_half_open();

        // Up to failure_execution_threshold (3) concurrent probes allowed in half-open.
        let p1 = cb.try_acquire_permit().unwrap().unwrap();
        let p2 = cb.try_acquire_permit().unwrap().unwrap();
        let p3 = cb.try_acquire_permit().unwrap().unwrap();
        assert!(cb.try_acquire_permit().is_err());

        // Dropping p2 without finish() releases its half-open slot via RAII Drop.
        drop(p2);
        let p4 = cb.try_acquire_permit().unwrap().unwrap();
        assert!(cb.try_acquire_permit().is_err());

        p1.finish(Duration::from_millis(1), false);
        p3.finish(Duration::from_millis(1), false);
        assert_eq!(cb.state(), BreakerState::HalfOpen);
        p4.finish(Duration::from_millis(1), false);
        assert_eq!(cb.state(), BreakerState::Closed);
    }

    #[test]
    fn timed_stats_10_buckets_expires_old_buckets_in_o1() {
        let epoch = Instant::now();
        let mut stats = TimedStats::new(Duration::from_secs(10), epoch, 0);

        // Bucket 0: 50 failures + 50 successes at +0.5s
        let t0 = epoch + Duration::from_millis(500);
        for _ in 0..50 {
            stats.record_failure(t0);
            stats.record_success(t0);
        }
        assert_eq!(stats.execution_count(), 100);
        assert_eq!(stats.failure_count(), 50);
        assert_eq!(stats.failure_rate(), 0.50);

        // Bucket 5 (+5.5s): 10 successes
        let t5 = epoch + Duration::from_millis(5500);
        for _ in 0..10 {
            stats.record_success(t5);
        }
        assert_eq!(stats.execution_count(), 110);
        assert_eq!(stats.failure_count(), 50);

        // Bucket 10 (+10.5s): wraps to bucket 0 and expires bucket 0's 100 records.
        let t10 = epoch + Duration::from_millis(10500);
        stats.record_success(t10);
        assert_eq!(stats.execution_count(), 11);
        assert_eq!(stats.failure_count(), 0);
    }

    #[test]
    fn counting_stats_ring_buffer_overwrites_oldest() {
        let mut stats = CountingStats::new(4);
        for _ in 0..4 {
            stats.record_failure();
        }
        assert_eq!(stats.execution_count(), 4);
        assert_eq!(stats.failure_count(), 4);
        assert_eq!(stats.failure_rate(), 1.0);

        stats.record_success();
        stats.record_success();
        assert_eq!(stats.execution_count(), 4);
        assert_eq!(stats.failure_count(), 2);
        assert_eq!(stats.success_count(), 2);
        assert_eq!(stats.failure_rate(), 0.50);
        assert_eq!(stats.success_rate(), 0.50);
    }

    #[test]
    fn closed_state_rounds_failure_rate_to_2_decimals() {
        let cfg = CircuitBreakerConfig {
            enabled: true,
            failure_threshold_percentage: 10,
            failure_execution_threshold: 1000,
            thresholding_period: mimir_config::GoDuration::from_secs(60),
            cooldown_period: mimir_config::GoDuration::from_secs(30),
            request_timeout: mimir_config::GoDuration::from_secs(10),
            ..Default::default()
        };

        // 94 / 1000 = 0.094 -> Round = 0.09 < 0.10 -> stays Closed.
        let cb1 = CircuitBreaker::new(&cfg, None, "push", false).unwrap();
        cb1.activate();
        for _ in 0..94 {
            let p = cb1.try_acquire_permit().unwrap().unwrap();
            p.finish(Duration::from_secs(1), true);
        }
        for _ in 0..906 {
            let p = cb1.try_acquire_permit().unwrap().unwrap();
            p.finish(Duration::from_millis(1), false);
        }
        assert_eq!(cb1.state(), BreakerState::Closed);

        // 95 / 1000 = 0.095 -> Round = 0.10 >= 0.10 -> opens (even when the 1000th is a success).
        let cb2 = CircuitBreaker::new(&cfg, None, "push", false).unwrap();
        cb2.activate();
        for _ in 0..95 {
            let p = cb2.try_acquire_permit().unwrap().unwrap();
            p.finish(Duration::from_secs(1), true);
        }
        for _ in 0..905 {
            let p = cb2.try_acquire_permit().unwrap().unwrap();
            p.finish(Duration::from_millis(1), false);
        }
        assert_eq!(cb2.state(), BreakerState::Open);
    }

    #[test]
    fn half_open_evaluates_both_directions_at_execution_threshold() {
        let cfg = CircuitBreakerConfig {
            enabled: true,
            failure_threshold_percentage: 10,
            failure_execution_threshold: 100,
            thresholding_period: mimir_config::GoDuration::from_secs(60),
            cooldown_period: mimir_config::GoDuration::from_secs(30),
            request_timeout: mimir_config::GoDuration::from_secs(10),
            ..Default::default()
        };

        // 10 failures then 90 successes: at the 100th record (the 90th success),
        // failure_rate = 0.10 >= 0.10 and success_rate = 0.90 is NOT > 0.90 -> re-opens immediately!
        let cb = CircuitBreaker::new(&cfg, None, "push", false).unwrap();
        cb.activate();
        cb.force_half_open();
        for _ in 0..10 {
            let p = cb.try_acquire_permit().unwrap().unwrap();
            p.finish(Duration::from_secs(1), true);
        }
        for _ in 0..89 {
            let p = cb.try_acquire_permit().unwrap().unwrap();
            p.finish(Duration::from_millis(1), false);
            assert_eq!(cb.state(), BreakerState::HalfOpen);
        }
        let p100 = cb.try_acquire_permit().unwrap().unwrap();
        p100.finish(Duration::from_millis(1), false);
        assert_eq!(cb.state(), BreakerState::Open);

        // 9 failures then 91 successes: at the 100th record, success_rate = 0.91 > 0.90 -> closes!
        cb.force_half_open();
        for _ in 0..9 {
            let p = cb.try_acquire_permit().unwrap().unwrap();
            p.finish(Duration::from_secs(1), true);
        }
        for _ in 0..90 {
            let p = cb.try_acquire_permit().unwrap().unwrap();
            p.finish(Duration::from_millis(1), false);
            assert_eq!(cb.state(), BreakerState::HalfOpen);
        }
        let p100 = cb.try_acquire_permit().unwrap().unwrap();
        p100.finish(Duration::from_millis(1), false);
        assert_eq!(cb.state(), BreakerState::Closed);
    }
}
