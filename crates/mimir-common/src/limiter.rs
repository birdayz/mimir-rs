//! Rate limiters, inflight request/byte limiters, concurrency semaphores, and Vegas/AIMD ReactiveLimiter.
//!
//! Ported from:
//! - github.com/grafana/dskit/limiter/rate_limiter.go
//! - golang.org/x/time/rate/rate.go
//! - github.com/grafana/mimir/pkg/ingester/limiter.go
//! - github.com/grafana/mimir/pkg/ingester/ingester_push.go
//! - github.com/grafana/mimir/pkg/util/reactivelimiter/reactivelimiter.go
//! - github.com/failsafe-go/failsafe-go/adaptivelimiter/*

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};
use rand::Rng;
use thiserror::Error;
use tokio::sync::{Notify, Semaphore, SemaphorePermit};

/// Infinite rate limit matching `golang.org/x/time/rate.Inf`.
pub const RATE_INF: f64 = f64::MAX;

/// Duration returned by [`Reservation::delay_from`] when reservation failed.
pub const INF_DURATION: Duration = Duration::MAX;

/// Pluggable rate-limit strategy matching `dskit/limiter.RateLimiterStrategy`.
pub trait RateLimiterStrategy: Send + Sync {
    fn limit(&self, tenant_id: &str) -> f64;
    fn burst(&self, tenant_id: &str) -> usize;
}

/// Ring-scaling strategy matching `pkg/ingester.limiterRingStrategy`.
pub trait LimiterRingStrategy: Send + Sync {
    fn convert_global_to_local_limit(&self, user_id: &str, global_limit: usize) -> usize;
    fn get_shard_size(&self, user_id: &str) -> usize;
}

/// Simple local per-tenant or constant rate limit strategy.
#[derive(Debug, Clone)]
pub struct LocalRateLimiterStrategy {
    pub limit: f64,
    pub burst: usize,
}

impl RateLimiterStrategy for LocalRateLimiterStrategy {
    fn limit(&self, _tenant_id: &str) -> f64 {
        self.limit
    }

    fn burst(&self, _tenant_id: &str) -> usize {
        self.burst
    }
}

/// Ring-scaled strategy that divides a global rate limit across active ring instances/partitions.
pub struct RingScaledRateLimiterStrategy {
    global_limit: Box<dyn Fn(&str) -> f64 + Send + Sync>,
    global_burst: Box<dyn Fn(&str) -> usize + Send + Sync>,
    ring_strategy: Arc<dyn LimiterRingStrategy>,
}

impl RingScaledRateLimiterStrategy {
    pub fn new<FL, FB>(
        global_limit: FL,
        global_burst: FB,
        ring_strategy: Arc<dyn LimiterRingStrategy>,
    ) -> Self
    where
        FL: Fn(&str) -> f64 + Send + Sync + 'static,
        FB: Fn(&str) -> usize + Send + Sync + 'static,
    {
        Self {
            global_limit: Box::new(global_limit),
            global_burst: Box::new(global_burst),
            ring_strategy,
        }
    }
}

impl RateLimiterStrategy for RingScaledRateLimiterStrategy {
    fn limit(&self, tenant_id: &str) -> f64 {
        let g = (self.global_limit)(tenant_id);
        if g <= 0.0 || g == RATE_INF {
            return g;
        }
        let local = self
            .ring_strategy
            .convert_global_to_local_limit(tenant_id, g as usize);
        if local == 0 {
            RATE_INF
        } else {
            local as f64
        }
    }

    fn burst(&self, tenant_id: &str) -> usize {
        let g = (self.global_burst)(tenant_id);
        if g == 0 {
            return 0;
        }
        let local = self
            .ring_strategy
            .convert_global_to_local_limit(tenant_id, g);
        if local == 0 {
            g
        } else {
            local
        }
    }
}

/// Reservation returned by [`TokenBucket::reserve_n`].
#[derive(Debug, Clone)]
pub struct Reservation {
    pub ok: bool,
    pub tokens: usize,
    pub time_to_act: Instant,
    pub limit: f64,
}

impl Reservation {
    pub fn ok(&self) -> bool {
        self.ok
    }

    pub fn delay(&self) -> Duration {
        self.delay_from(Instant::now())
    }

    pub fn delay_from(&self, t: Instant) -> Duration {
        if !self.ok {
            return INF_DURATION;
        }
        self.time_to_act.saturating_duration_since(t)
    }
}

#[derive(Debug)]
struct TokenBucketInner {
    limit: f64,
    burst: usize,
    tokens: f64,
    last: Option<Instant>,
    last_event: Option<Instant>,
}

/// Token-bucket rate limiter matching `golang.org/x/time/rate.Limiter`.
#[derive(Debug)]
pub struct TokenBucket {
    inner: Mutex<TokenBucketInner>,
}

impl TokenBucket {
    pub fn new(limit: f64, burst: usize) -> Self {
        Self {
            inner: Mutex::new(TokenBucketInner {
                limit,
                burst,
                tokens: burst as f64,
                last: None,
                last_event: None,
            }),
        }
    }

    pub fn limit(&self) -> f64 {
        self.inner.lock().limit
    }

    pub fn burst(&self) -> usize {
        self.inner.lock().burst
    }

    pub fn allow(&self) -> bool {
        self.allow_n(Instant::now(), 1)
    }

    pub fn allow_n(&self, now: Instant, n: usize) -> bool {
        self.reserve_n_internal(now, n, Duration::ZERO).ok
    }

    pub fn reserve_n(&self, now: Instant, n: usize) -> Reservation {
        self.reserve_n_internal(now, n, INF_DURATION)
    }

    pub fn cancel_at(&self, now: Instant, reservation: &Reservation) {
        if !reservation.ok {
            return;
        }
        let mut inner = self.inner.lock();
        if inner.limit == RATE_INF || reservation.tokens == 0 || reservation.time_to_act < now {
            return;
        }
        let elapsed_after = inner
            .last_event
            .map(|le| le.saturating_duration_since(reservation.time_to_act))
            .unwrap_or(Duration::ZERO);
        let restore_tokens =
            reservation.tokens as f64 - tokens_from_duration(inner.limit, elapsed_after);
        if restore_tokens <= 0.0 {
            return;
        }
        let mut tokens = advance_tokens(&inner, now) + restore_tokens;
        let burst_f = inner.burst as f64;
        if tokens > burst_f {
            tokens = burst_f;
        }
        inner.last = Some(now);
        inner.tokens = tokens;
        if inner.last_event == Some(reservation.time_to_act) {
            let back = duration_from_tokens(inner.limit, reservation.tokens as f64);
            if let Some(prev) = reservation.time_to_act.checked_sub(back) {
                if prev >= now {
                    inner.last_event = Some(prev);
                }
            }
        }
    }

    pub fn tokens_at(&self, now: Instant) -> f64 {
        let inner = self.inner.lock();
        if inner.limit == RATE_INF {
            return f64::INFINITY;
        }
        advance_tokens(&inner, now)
    }

    pub fn set_limit_at(&self, now: Instant, new_limit: f64) {
        let mut inner = self.inner.lock();
        let tokens = advance_tokens(&inner, now);
        inner.last = Some(now);
        inner.tokens = tokens;
        inner.limit = new_limit;
    }

    pub fn set_burst_at(&self, now: Instant, new_burst: usize) {
        let mut inner = self.inner.lock();
        let tokens = advance_tokens(&inner, now);
        inner.last = Some(now);
        inner.tokens = tokens;
        inner.burst = new_burst;
    }

    fn reserve_n_internal(
        &self,
        now: Instant,
        n: usize,
        max_future_reserve: Duration,
    ) -> Reservation {
        let mut inner = self.inner.lock();
        if inner.limit == RATE_INF {
            return Reservation {
                ok: true,
                tokens: n,
                time_to_act: now,
                limit: inner.limit,
            };
        }

        let mut tokens = advance_tokens(&inner, now);
        tokens -= n as f64;

        let wait_duration = if tokens < 0.0 {
            duration_from_tokens(inner.limit, -tokens)
        } else {
            Duration::ZERO
        };

        let ok = n <= inner.burst && wait_duration <= max_future_reserve;
        let time_to_act = if ok {
            now.checked_add(wait_duration).unwrap_or(now)
        } else {
            now
        };

        if ok {
            inner.last = Some(now);
            inner.tokens = tokens;
            inner.last_event = Some(time_to_act);
        }

        Reservation {
            ok,
            tokens: if ok { n } else { 0 },
            time_to_act,
            limit: inner.limit,
        }
    }
}

fn advance_tokens(inner: &TokenBucketInner, now: Instant) -> f64 {
    let last = inner.last.unwrap_or(now);
    let elapsed = now.saturating_duration_since(last);
    let delta = tokens_from_duration(inner.limit, elapsed);
    let tokens = inner.tokens + delta;
    let burst = inner.burst as f64;
    if tokens > burst {
        burst
    } else {
        tokens
    }
}

fn tokens_from_duration(limit: f64, d: Duration) -> f64 {
    if limit <= 0.0 {
        return 0.0;
    }
    d.as_secs_f64() * limit
}

fn duration_from_tokens(limit: f64, tokens: f64) -> Duration {
    if limit <= 0.0 {
        return INF_DURATION;
    }
    let secs = tokens / limit;
    if !secs.is_finite() || secs >= (i64::MAX as f64 / 1e9) {
        return INF_DURATION;
    }
    Duration::from_secs_f64(secs.max(0.0))
}

struct TenantLimiterEntry {
    limiter: Arc<TokenBucket>,
    recheck_at: Instant,
}

/// Multi-tenant local rate limiter matching `dskit/limiter.RateLimiter`.
pub struct RateLimiter {
    strategy: Arc<dyn RateLimiterStrategy>,
    recheck_period: Duration,
    tenants: RwLock<HashMap<String, TenantLimiterEntry>>,
}

impl RateLimiter {
    pub fn new(strategy: Arc<dyn RateLimiterStrategy>, recheck_period: Duration) -> Self {
        Self {
            strategy,
            recheck_period,
            tenants: RwLock::new(HashMap::new()),
        }
    }

    pub fn allow_n(&self, now: Instant, tenant_id: &str, n: usize) -> bool {
        self.get_tenant_limiter(now, tenant_id).allow_n(now, n)
    }

    pub fn reserve_n(&self, now: Instant, tenant_id: &str, n: usize) -> (Arc<TokenBucket>, Reservation) {
        let lim = self.get_tenant_limiter(now, tenant_id);
        let res = lim.reserve_n(now, n);
        (lim, res)
    }

    pub async fn wait_n(&self, tenant_id: &str, n: usize) -> Result<(), String> {
        let now = Instant::now();
        let lim = self.get_tenant_limiter(now, tenant_id);
        let burst = lim.burst();
        let limit = lim.limit();
        if n > burst && limit != RATE_INF {
            return Err(format!("rate: Wait(n={n}) exceeds limiter's burst {burst}"));
        }
        let res = lim.reserve_n(now, n);
        if !res.ok {
            return Err(format!("rate: Wait(n={n}) rejected"));
        }
        let delay = res.delay_from(now);
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        Ok(())
    }

    pub fn limit(&self, now: Instant, tenant_id: &str) -> f64 {
        self.get_tenant_limiter(now, tenant_id).limit()
    }

    pub fn tokens_at(&self, now: Instant, tenant_id: &str) -> f64 {
        self.get_tenant_limiter(now, tenant_id).tokens_at(now)
    }

    pub fn burst(&self, now: Instant, tenant_id: &str) -> usize {
        self.get_tenant_limiter(now, tenant_id).burst()
    }

    pub fn remove_stale_entries(&self, cutoff: Instant) -> usize {
        let mut tenants = self.tenants.write();
        let before = tenants.len();
        tenants.retain(|_, entry| entry.recheck_at >= cutoff);
        before - tenants.len()
    }

    fn get_tenant_limiter(&self, now: Instant, tenant_id: &str) -> Arc<TokenBucket> {
        {
            let tenants = self.tenants.read();
            if let Some(entry) = tenants.get(tenant_id) {
                if now < entry.recheck_at {
                    return entry.limiter.clone();
                }
                // Need recheck
                drop(tenants);
                return self.recheck_tenant_limiter(now, tenant_id);
            }
        }

        let limit = self.strategy.limit(tenant_id);
        let burst = self.strategy.burst(tenant_id);
        let new_lim = Arc::new(TokenBucket::new(limit, burst));

        let mut tenants = self.tenants.write();
        let entry = tenants
            .entry(tenant_id.to_string())
            .or_insert_with(|| TenantLimiterEntry {
                limiter: new_lim,
                recheck_at: now + self.recheck_period,
            });
        entry.limiter.clone()
    }

    fn recheck_tenant_limiter(&self, now: Instant, tenant_id: &str) -> Arc<TokenBucket> {
        let limit = self.strategy.limit(tenant_id);
        let burst = self.strategy.burst(tenant_id);

        let mut tenants = self.tenants.write();
        let entry = tenants
            .entry(tenant_id.to_string())
            .or_insert_with(|| TenantLimiterEntry {
                limiter: Arc::new(TokenBucket::new(limit, burst)),
                recheck_at: now + self.recheck_period,
            });

        if now < entry.recheck_at {
            return entry.limiter.clone();
        }

        if (entry.limiter.limit() - limit).abs() > f64::EPSILON {
            entry.limiter.set_limit_at(now, limit);
        }
        if entry.limiter.burst() != burst {
            entry.limiter.set_burst_at(now, burst);
        }
        entry.recheck_at = now + self.recheck_period;
        entry.limiter.clone()
    }
}

/// Error returned when an inflight request or byte limit is exceeded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum InflightLimitError {
    #[error("max inflight push requests exceeded")]
    MaxInflightRequests,
    #[error("max inflight push requests bytes exceeded")]
    MaxInflightBytes,
}

/// Lock-free atomic inflight request count and byte limiter (`pkg/ingester/ingester_push.go`).
#[derive(Debug, Default)]
pub struct InflightLimiter {
    inflight_requests: AtomicI64,
    inflight_bytes: AtomicI64,
}

impl InflightLimiter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn inflight_requests(&self) -> i64 {
        self.inflight_requests.load(Ordering::Relaxed)
    }

    pub fn inflight_bytes(&self) -> i64 {
        self.inflight_bytes.load(Ordering::Relaxed)
    }

    /// Acquires an inflight permit for a request of `req_bytes`, checking `max_requests`
    /// and `max_bytes` (`0` = unlimited). Automatically releases on drop.
    pub fn try_acquire(
        &self,
        req_bytes: i64,
        max_requests: i64,
        max_bytes: i64,
    ) -> Result<InflightPermit<'_>, InflightLimitError> {
        let inflight = self.inflight_requests.fetch_add(1, Ordering::AcqRel) + 1;
        let mut reject_equal_inflight_bytes = false;
        let inflight_bytes = if req_bytes > 0 {
            self.inflight_bytes.fetch_add(req_bytes, Ordering::AcqRel) + req_bytes
        } else {
            reject_equal_inflight_bytes = true;
            self.inflight_bytes.load(Ordering::Acquire)
        };

        let permit = InflightPermit {
            limiter: self,
            bytes: req_bytes.max(0),
        };

        if max_requests > 0 && inflight > max_requests {
            drop(permit);
            return Err(InflightLimitError::MaxInflightRequests);
        }

        if max_bytes > 0
            && ((reject_equal_inflight_bytes && inflight_bytes >= max_bytes)
                || inflight_bytes > max_bytes)
        {
            drop(permit);
            return Err(InflightLimitError::MaxInflightBytes);
        }

        Ok(permit)
    }
}

pub struct InflightPermit<'a> {
    limiter: &'a InflightLimiter,
    bytes: i64,
}

impl Drop for InflightPermit<'_> {
    fn drop(&mut self) {
        self.limiter.inflight_requests.fetch_sub(1, Ordering::AcqRel);
        if self.bytes > 0 {
            self.limiter
                .inflight_bytes
                .fetch_sub(self.bytes, Ordering::AcqRel);
        }
    }
}

/// Async concurrency semaphore for bounded request/byte admission.
#[derive(Debug)]
pub struct ConcurrencySemaphore {
    sem: Semaphore,
    max_permits: usize,
}

impl ConcurrencySemaphore {
    pub fn new(permits: usize) -> Self {
        Self {
            sem: Semaphore::new(permits),
            max_permits: permits,
        }
    }

    pub fn max_permits(&self) -> usize {
        self.max_permits
    }

    pub fn available(&self) -> usize {
        self.sem.available_permits()
    }

    pub async fn acquire(&self, n: u32) -> Option<SemaphorePermit<'_>> {
        self.sem.acquire_many(n).await.ok()
    }

    pub fn try_acquire(&self, n: u32) -> Option<SemaphorePermit<'_>> {
        self.sem.try_acquire_many(n).ok()
    }
}

// ---------------------------------------------------------------------------
// ReactiveLimiter (pkg/util/reactivelimiter + failsafe-go/adaptivelimiter)
// ---------------------------------------------------------------------------

pub const DEFAULT_INITIAL_REJECTION_FACTOR: f64 = 2.0;
pub const DEFAULT_MAX_REJECTION_FACTOR: f64 = 3.0;
const WARMUP_SAMPLES: usize = 10;
const SMOOTHED_SAMPLES: usize = 5;

#[derive(Debug, Clone, PartialEq)]
pub struct ReactiveLimiterConfig {
    pub enabled: bool,
    pub min_limit: usize,
    pub max_limit: usize,
    pub initial_limit: usize,
    pub max_limit_factor: f64,
    pub max_limit_factor_decay: f64,
    pub min_limit_factor: f64,
    pub recent_window_min_duration: Duration,
    pub recent_window_max_duration: Duration,
    pub recent_window_min_samples: usize,
    pub recent_quantile: f64,
    pub baseline_window_age: usize,
    pub correlation_window: usize,
    pub initial_rejection_factor: f64,
    pub max_rejection_factor: f64,
}

impl Default for ReactiveLimiterConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            min_limit: 2,
            max_limit: 200,
            initial_limit: 20,
            max_limit_factor: 5.0,
            max_limit_factor_decay: 1.0,
            min_limit_factor: 1.2,
            recent_window_min_duration: Duration::from_secs(1),
            recent_window_max_duration: Duration::from_secs(30),
            recent_window_min_samples: 50,
            recent_quantile: 0.9,
            baseline_window_age: 10,
            correlation_window: 50,
            initial_rejection_factor: DEFAULT_INITIAL_REJECTION_FACTOR,
            max_rejection_factor: DEFAULT_MAX_REJECTION_FACTOR,
        }
    }
}

impl ReactiveLimiterConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.min_limit_factor < 1.0 {
            return Err("min_limit_factor must be >= 1".to_string());
        }
        if self.min_limit > self.max_limit {
            return Err("min_limit must be <= max_limit".to_string());
        }
        if self.initial_limit < self.min_limit || self.initial_limit > self.max_limit {
            return Err("initial_limit must be between min_limit and max_limit".to_string());
        }
        if self.max_limit_factor < 1.0 {
            return Err("max_limit_factor must be >= 1".to_string());
        }
        if self.recent_window_min_duration > self.recent_window_max_duration {
            return Err("recent_window_min_duration must be <= recent_window_max_duration".to_string());
        }
        if self.initial_rejection_factor != 0.0 || self.max_rejection_factor != 0.0 {
            if self.initial_rejection_factor < 1.0 || self.max_rejection_factor < 1.0 {
                return Err("rejection factors must be >= 1".to_string());
            }
            if self.initial_rejection_factor > self.max_rejection_factor {
                return Err(
                    "initial_rejection_factor must be <= max_rejection_factor".to_string(),
                );
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum ReactiveLimitError {
    #[error("limit exceeded")]
    LimitExceeded,
    #[error("wait time exceeded")]
    WaitExceeded,
}

#[derive(Debug)]
struct Ewma {
    warmup_samples: usize,
    smoothing_factor: f64,
    count: usize,
    value: f64,
    sum: f64,
}

impl Ewma {
    fn new(age: usize, warmup_samples: usize) -> Self {
        Self {
            warmup_samples,
            smoothing_factor: 2.0 / (age as f64 + 1.0),
            count: 0,
            value: 0.0,
            sum: 0.0,
        }
    }

    fn add(&mut self, new_value: f64) -> f64 {
        if self.count < self.warmup_samples {
            self.count += 1;
            self.sum += new_value;
            self.value = self.sum / self.count as f64;
        } else {
            self.value = self.value * (1.0 - self.smoothing_factor)
                + new_value * self.smoothing_factor;
        }
        self.value
    }

    fn reset(&mut self) {
        self.count = 0;
        self.value = 0.0;
        self.sum = 0.0;
    }
}

#[derive(Debug)]
struct MedianFilter {
    values: Vec<f64>,
    sorted: Vec<f64>,
    index: usize,
    size: usize,
}

impl MedianFilter {
    fn new(size: usize) -> Self {
        let cap = size.max(1);
        Self {
            values: vec![0.0; cap],
            sorted: vec![0.0; cap],
            index: 0,
            size: 0,
        }
    }

    fn add(&mut self, value: f64) -> f64 {
        self.values[self.index] = value;
        self.index = (self.index + 1) % self.values.len();
        if self.size < self.values.len() - 1 {
            self.size += 1;
            return value;
        }
        self.sorted.copy_from_slice(&self.values);
        self.sorted
            .sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        self.sorted[self.sorted.len() / 2]
    }

    fn reset(&mut self) {
        self.values.fill(0.0);
        self.sorted.fill(0.0);
        self.index = 0;
        self.size = 0;
    }
}

#[derive(Debug)]
struct RollingSum {
    samples: Vec<f64>,
    size: usize,
    index: usize,
    sum_y: f64,
    sum_squares: f64,
}

impl RollingSum {
    fn new(capacity: usize) -> Self {
        Self {
            samples: vec![0.0; capacity.max(1)],
            size: 0,
            index: 0,
            sum_y: 0.0,
            sum_squares: 0.0,
        }
    }

    fn add(&mut self, value: f64) -> (f64, bool) {
        let mut old_value = 0.0;
        let mut full = false;
        if value != 0.0 {
            if self.size == self.samples.len() {
                full = true;
                old_value = self.samples[self.index];
                self.sum_y -= old_value;
                self.sum_squares -= old_value * old_value;
            } else {
                self.size += 1;
            }
            self.samples[self.index] = value;
            self.sum_y += value;
            self.sum_squares += value * value;
            self.index = (self.index + 1) % self.samples.len();
        }
        (old_value, full)
    }

    fn calculate_cv(&self) -> (f64, f64, f64) {
        if self.size < 2 {
            return (f64::NAN, f64::NAN, f64::NAN);
        }
        let mean = self.sum_y / self.size as f64;
        let variance = (self.sum_squares / self.size as f64) - (mean * mean);
        if variance < 0.0 || mean == 0.0 {
            return (f64::NAN, f64::NAN, f64::NAN);
        }
        let cv = variance.sqrt() / mean;
        (cv, mean, variance)
    }

    fn reset(&mut self) {
        self.samples.fill(0.0);
        self.size = 0;
        self.index = 0;
        self.sum_y = 0.0;
        self.sum_squares = 0.0;
    }
}

#[derive(Debug)]
struct CorrelationWindow {
    warmup_samples: usize,
    x_samples: RollingSum,
    y_samples: RollingSum,
    corr_sum_xy: f64,
}

impl CorrelationWindow {
    fn new(capacity: usize, warmup_samples: usize) -> Self {
        Self {
            warmup_samples,
            x_samples: RollingSum::new(capacity),
            y_samples: RollingSum::new(capacity),
            corr_sum_xy: 0.0,
        }
    }

    fn add(&mut self, x: f64, y: f64) -> (f64, f64, f64) {
        if x.is_infinite() || y.is_infinite() {
            return (0.0, 0.0, 0.0);
        }
        let (old_x, full) = self.x_samples.add(x);
        let (old_y, _) = self.y_samples.add(y);
        let (cv_x, mean_x, var_x) = self.x_samples.calculate_cv();
        let (cv_y, mean_y, var_y) = self.y_samples.calculate_cv();

        if full {
            self.corr_sum_xy -= old_x * old_y;
        }
        self.corr_sum_xy += x * y;

        if cv_x.is_nan() || cv_y.is_nan() {
            return (0.0, 0.0, 0.0);
        }
        if self.x_samples.size < self.warmup_samples {
            return (0.0, 0.0, 0.0);
        }
        let min_cv = 0.01;
        if cv_x < min_cv || cv_y < min_cv {
            return (0.0, cv_x, cv_y);
        }
        let covariance = (self.corr_sum_xy / self.x_samples.size as f64) - (mean_x * mean_y);
        let correlation = covariance / (var_x.sqrt() * var_y.sqrt());
        (correlation, cv_x, cv_y)
    }

    fn reset(&mut self) {
        self.x_samples.reset();
        self.y_samples.reset();
        self.corr_sum_xy = 0.0;
    }
}

#[derive(Debug, Default)]
struct RecentRttWindow {
    min_rtt: Duration,
    max_inflight: usize,
    size: usize,
    samples: Vec<f64>,
}

impl RecentRttWindow {
    fn add(&mut self, rtt: Duration, inflight: usize) {
        if self.size == 0 {
            self.min_rtt = rtt;
            self.max_inflight = inflight;
        } else {
            self.min_rtt = self.min_rtt.min(rtt);
            self.max_inflight = self.max_inflight.max(inflight);
        }
        self.size += 1;
        // Keep bounded memory if high volume arrives within a single window.
        if self.samples.len() < 8192 {
            self.samples.push(rtt.as_nanos() as f64);
        } else {
            let idx = rand::thread_rng().gen_range(0..self.size);
            if idx < self.samples.len() {
                self.samples[idx] = rtt.as_nanos() as f64;
            }
        }
    }

    fn quantile(&mut self, q: f64) -> f64 {
        if self.samples.is_empty() {
            return 0.0;
        }
        self.samples
            .sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let q = q.clamp(0.0, 1.0);
        let pos = ((self.samples.len() - 1) as f64 * q).round() as usize;
        self.samples[pos.min(self.samples.len() - 1)]
    }

    fn reset(&mut self) {
        self.min_rtt = Duration::ZERO;
        self.max_inflight = 0;
        self.size = 0;
        self.samples.clear();
    }
}

#[derive(Debug)]
struct DynamicSemaphoreState {
    size: usize,
    used: usize,
    waiters: VecDeque<Arc<Notify>>,
}

/// Resizable FIFO async semaphore matching `failsafe-go/internal/util.DynamicSemaphore`.
#[derive(Debug)]
pub struct DynamicSemaphore {
    state: Mutex<DynamicSemaphoreState>,
}

impl DynamicSemaphore {
    pub fn new(size: usize) -> Self {
        Self {
            state: Mutex::new(DynamicSemaphoreState {
                size,
                used: 0,
                waiters: VecDeque::new(),
            }),
        }
    }

    pub fn try_acquire(&self) -> bool {
        let mut st = self.state.lock();
        if st.used < st.size {
            st.used += 1;
            true
        } else {
            false
        }
    }

    pub async fn acquire(&self) -> Result<(), ReactiveLimitError> {
        let notify = {
            let mut st = self.state.lock();
            if st.used < st.size {
                st.used += 1;
                return Ok(());
            }
            let n = Arc::new(Notify::new());
            st.waiters.push_back(n.clone());
            n
        };
        notify.notified().await;
        Ok(())
    }

    pub async fn acquire_with_max_wait(
        &self,
        max_wait: Duration,
    ) -> Result<(), ReactiveLimitError> {
        let notify = {
            let mut st = self.state.lock();
            if st.used < st.size {
                st.used += 1;
                return Ok(());
            }
            if max_wait.is_zero() {
                return Err(ReactiveLimitError::WaitExceeded);
            }
            let n = Arc::new(Notify::new());
            st.waiters.push_back(n.clone());
            n
        };

        if tokio::time::timeout(max_wait, notify.notified())
            .await
            .is_err()
        {
            let mut st = self.state.lock();
            // Remove our waiter if it wasn't already woken & given a permit
            if let Some(pos) = st.waiters.iter().position(|w| Arc::ptr_eq(w, &notify)) {
                st.waiters.remove(pos);
                return Err(ReactiveLimitError::WaitExceeded);
            }
            // Waiter was already popped (meaning `used` was incremented for us).
            // Release that permit so we don't leak it, or note: in Go `drainWaiter` checks `select { case <-waiter: return nil }`.
            // Actually if popped, it's already granted! Match Go: return Ok(()).
            return Ok(());
        }
        Ok(())
    }

    pub fn release(&self) {
        let mut st = self.state.lock();
        assert!(st.used > 0, "DynamicSemaphore: release without matching acquire");
        st.used -= 1;
        while st.used < st.size {
            if let Some(w) = st.waiters.pop_front() {
                st.used += 1;
                w.notify_one();
            } else {
                break;
            }
        }
    }

    pub fn set_size(&self, new_size: usize) {
        let mut st = self.state.lock();
        if new_size > st.size {
            while st.used < new_size {
                if let Some(w) = st.waiters.pop_front() {
                    st.used += 1;
                    w.notify_one();
                } else {
                    break;
                }
            }
        }
        st.size = new_size;
    }

    pub fn is_full(&self) -> bool {
        let st = self.state.lock();
        st.used >= st.size
    }

    pub fn waiters(&self) -> usize {
        self.state.lock().waiters.len()
    }

    pub fn used(&self) -> usize {
        self.state.lock().used
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LimitChange {
    Increase,
    Decrease,
    Hold,
}

#[inline]
fn log10_func(factor: usize, limit: usize) -> usize {
    let base = if limit < 100 {
        1
    } else if limit < 1000 {
        2
    } else {
        (limit as f64).log10() as usize
    };
    factor * base
}

struct ReactiveLimiterInner {
    limit: f64,
    recent_rtt: RecentRttWindow,
    last_max_inflight: usize,
    median_filter: MedianFilter,
    smoothed_recent_rtt: Ewma,
    baseline_rtt: Ewma,
    next_update_time: Instant,
    throughput_correlation: CorrelationWindow,
    rtt_correlation: CorrelationWindow,
}

/// Vegas/gradient-based adaptive concurrency limiter (`pkg/util/reactivelimiter`).
pub struct ReactiveLimiter {
    cfg: ReactiveLimiterConfig,
    semaphore: DynamicSemaphore,
    inner: Mutex<ReactiveLimiterInner>,
    queueing_enabled: bool,
}

impl ReactiveLimiter {
    pub fn new(cfg: ReactiveLimiterConfig) -> Result<Arc<Self>, String> {
        cfg.validate()?;
        let queueing_enabled =
            cfg.initial_rejection_factor > 0.0 && cfg.max_rejection_factor > 0.0;
        let initial = cfg.initial_limit;
        Ok(Arc::new(Self {
            semaphore: DynamicSemaphore::new(initial),
            inner: Mutex::new(ReactiveLimiterInner {
                limit: initial as f64,
                recent_rtt: RecentRttWindow::default(),
                last_max_inflight: 0,
                median_filter: MedianFilter::new(SMOOTHED_SAMPLES),
                smoothed_recent_rtt: Ewma::new(SMOOTHED_SAMPLES, WARMUP_SAMPLES),
                baseline_rtt: Ewma::new(cfg.baseline_window_age, WARMUP_SAMPLES),
                next_update_time: Instant::now(),
                throughput_correlation: CorrelationWindow::new(
                    cfg.correlation_window,
                    WARMUP_SAMPLES,
                ),
                rtt_correlation: CorrelationWindow::new(cfg.correlation_window, WARMUP_SAMPLES),
            }),
            queueing_enabled,
            cfg,
        }))
    }

    pub fn can_acquire_permit(&self) -> bool {
        if self.semaphore.is_full() {
            if !self.queueing_enabled {
                return false;
            }
            let limit = self.limit();
            let queued = self.queued();
            let rejection_threshold = (limit as f64 * self.cfg.initial_rejection_factor) as usize;
            let max_queue = (limit as f64 * self.cfg.max_rejection_factor) as usize;
            if queued < rejection_threshold {
                return true;
            }
            if queued >= max_queue {
                return false;
            }
            let rate = (queued - rejection_threshold) as f64
                / (max_queue - rejection_threshold) as f64;
            if rate >= 1.0 || rate >= rand::thread_rng().gen::<f64>() {
                return false;
            }
            return true;
        }
        true
    }

    pub async fn acquire_permit(self: &Arc<Self>) -> Result<ReactivePermit, ReactiveLimitError> {
        if self.queueing_enabled && !self.can_acquire_permit() {
            return Err(ReactiveLimitError::LimitExceeded);
        }
        self.semaphore.acquire().await?;
        Ok(ReactivePermit {
            limiter: self.clone(),
            start_time: Instant::now(),
            current_inflight: self.semaphore.used(),
            finished: false,
        })
    }

    /// Acquires a permit from the underlying semaphore, blocking if needed, without the
    /// queueing rejection check (Go: `adaptiveLimiter.AcquirePermit`, used by `priorityLimiter`
    /// after its own prioritized admission check).
    pub async fn acquire_permit_unchecked(
        self: &Arc<Self>,
    ) -> Result<ReactivePermit, ReactiveLimitError> {
        self.semaphore.acquire().await?;
        Ok(ReactivePermit {
            limiter: self.clone(),
            start_time: Instant::now(),
            current_inflight: self.semaphore.used(),
            finished: false,
        })
    }

    /// Whether the limiter's semaphore is full (Go: `adaptiveLimiter.semaphore.IsFull()`).
    pub fn is_full(&self) -> bool {
        self.semaphore.is_full()
    }

    pub fn try_acquire_permit(self: &Arc<Self>) -> Option<ReactivePermit> {
        if !self.semaphore.try_acquire() {
            return None;
        }
        Some(ReactivePermit {
            limiter: self.clone(),
            start_time: Instant::now(),
            current_inflight: self.semaphore.used(),
            finished: false,
        })
    }

    pub fn limit(&self) -> usize {
        self.inner.lock().limit as usize
    }

    pub fn inflight(&self) -> usize {
        self.semaphore.used()
    }

    pub fn max_inflight(&self) -> usize {
        self.inner.lock().last_max_inflight
    }

    pub fn queued(&self) -> usize {
        self.semaphore.waiters()
    }

    pub fn reset(&self) {
        let mut inner = self.inner.lock();
        self.semaphore.set_size(self.cfg.initial_limit);
        inner.limit = self.cfg.initial_limit as f64;
        inner.recent_rtt.reset();
        inner.median_filter.reset();
        inner.smoothed_recent_rtt.reset();
        inner.baseline_rtt.reset();
        inner.next_update_time = Instant::now();
        inner.rtt_correlation.reset();
        inner.throughput_correlation.reset();
    }

    /// Records a sample with explicit timestamp and RTT (useful for deterministic testing).
    pub fn record_sample(&self, now: Instant, rtt: Duration, inflight: usize, dropped: bool) {
        let mut inner = self.inner.lock();
        if !dropped {
            inner.recent_rtt.add(rtt, inflight);
        }

        if now > inner.next_update_time && inner.recent_rtt.size >= self.cfg.recent_window_min_samples
        {
            let quantile = inner.recent_rtt.quantile(self.cfg.recent_quantile);
            let filtered_rtt = inner.median_filter.add(quantile);
            let smoothed_rtt = inner.smoothed_recent_rtt.add(filtered_rtt);
            let max_inflight = inner.recent_rtt.max_inflight;
            inner.last_max_inflight = max_inflight;
            let new_limit = self.update_limit(&mut inner, smoothed_rtt, max_inflight);
            let min_rtt = inner.recent_rtt.min_rtt;
            inner.recent_rtt.reset();
            let min_window_time = min_rtt.saturating_mul(2).max(self.cfg.recent_window_min_duration);
            inner.next_update_time = now + min_window_time.min(self.cfg.recent_window_max_duration);
            self.semaphore.set_size(new_limit as usize);
        }

        self.semaphore.release();
    }

    fn update_limit(
        &self,
        inner: &mut ReactiveLimiterInner,
        recent_rtt: f64,
        inflight: usize,
    ) -> f64 {
        let baseline_rtt = inner.baseline_rtt.add(recent_rtt);
        let gradient = if recent_rtt > 0.0 {
            baseline_rtt / recent_rtt
        } else {
            1.0
        };
        let queue_size = (inner.limit * (1.0 - gradient)).ceil() as isize;

        let throughput = if recent_rtt > 0.0 {
            inflight as f64 / (recent_rtt / 1e9)
        } else {
            0.0
        };
        let (throughput_corr, _, throughput_cv) = inner
            .throughput_correlation
            .add(inflight as f64, throughput);
        let (rtt_corr, _, _) = inner.rtt_correlation.add(inflight as f64, recent_rtt);

        let overloaded = self.semaphore.is_full();
        let alpha = log10_func(3, inner.limit as usize) as isize;
        let beta = log10_func(6, inner.limit as usize) as isize;

        let change = compute_change(
            queue_size,
            alpha,
            beta,
            overloaded,
            throughput_corr,
            throughput_cv,
            rtt_corr,
        );

        let old_limit = inner.limit;
        let mut new_limit = match change {
            LimitChange::Decrease => old_limit - log10_func(1, old_limit as usize) as f64,
            LimitChange::Increase => old_limit + log10_func(1, old_limit as usize) as f64,
            LimitChange::Hold => old_limit,
        };

        let max_limit = self.compute_max_limit(inflight);
        if new_limit > max_limit {
            if old_limit > max_limit {
                new_limit = old_limit - log10_func(1, old_limit as usize) as f64;
            } else {
                new_limit = max_limit;
            }
        }

        let min_l = self.cfg.min_limit.max(1) as f64;
        let max_l = self.cfg.max_limit as f64;
        new_limit = new_limit.clamp(min_l, max_l);
        inner.limit = new_limit;
        new_limit
    }

    fn compute_max_limit(&self, inflight: usize) -> f64 {
        let mut effective_factor = self.cfg.max_limit_factor;
        if self.cfg.max_limit_factor_decay > 0.0 && inflight > 0 {
            effective_factor =
                self.cfg.max_limit_factor - (self.cfg.max_limit_factor_decay * (inflight as f64).log10());
            effective_factor = effective_factor.max(self.cfg.min_limit_factor);
        }
        inflight as f64 * effective_factor
    }
}

fn compute_change(
    queue_size: isize,
    alpha: isize,
    beta: isize,
    overloaded: bool,
    throughput_corr: f64,
    throughput_cv: f64,
    rtt_corr: f64,
) -> LimitChange {
    if queue_size > beta
        || (overloaded && throughput_corr < 0.0)
        || (overloaded && throughput_corr < 0.3 && rtt_corr > 0.5)
        || (overloaded && throughput_cv < 0.2 && rtt_corr > 0.5)
    {
        LimitChange::Decrease
    } else if queue_size < alpha {
        LimitChange::Increase
    } else {
        LimitChange::Hold
    }
}

/// Permit acquired from a [`ReactiveLimiter`]. Call [`record`] or [`drop_permit`] on completion;
/// if dropped without calling either, it behaves as [`drop_permit`].
pub struct ReactivePermit {
    limiter: Arc<ReactiveLimiter>,
    start_time: Instant,
    current_inflight: usize,
    finished: bool,
}

impl ReactivePermit {
    pub fn record(mut self) {
        self.finished = true;
        let now = Instant::now();
        self.limiter.record_sample(
            now,
            now.saturating_duration_since(self.start_time),
            self.current_inflight,
            false,
        );
    }

    pub fn record_with_rtt(mut self, now: Instant, rtt: Duration) {
        self.finished = true;
        self.limiter
            .record_sample(now, rtt, self.current_inflight, false);
    }

    pub fn drop_permit(mut self) {
        self.finished = true;
        let now = Instant::now();
        self.limiter.record_sample(
            now,
            now.saturating_duration_since(self.start_time),
            self.current_inflight,
            true,
        );
    }
}

impl Drop for ReactivePermit {
    fn drop(&mut self) {
        if !self.finished {
            self.finished = true;
            let now = Instant::now();
            self.limiter.record_sample(
                now,
                now.saturating_duration_since(self.start_time),
                self.current_inflight,
                true,
            );
        }
    }
}

/// Helper counter for active tracking if needed.
#[derive(Debug, Default)]
pub struct AtomicCounter(AtomicUsize);

impl AtomicCounter {
    pub fn inc(&self) -> usize {
        self.0.fetch_add(1, Ordering::Relaxed) + 1
    }
    pub fn dec(&self) -> usize {
        self.0.fetch_sub(1, Ordering::Relaxed) - 1
    }
    pub fn get(&self) -> usize {
        self.0.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_token_bucket_rate_limiter() {
        let start = Instant::now();
        let strategy = Arc::new(LocalRateLimiterStrategy {
            limit: 10.0,
            burst: 5,
        });
        let rl = RateLimiter::new(strategy, Duration::from_secs(1));

        assert!(rl.allow_n(start, "t1", 5));
        assert!(!rl.allow_n(start, "t1", 1));
        // After 200ms at 10/s -> 2 tokens
        let t2 = start + Duration::from_millis(200);
        assert!(rl.allow_n(t2, "t1", 2));
        assert!(!rl.allow_n(t2, "t1", 1));
    }

    #[test]
    fn test_inflight_limiter() {
        let lim = InflightLimiter::new();
        let p1 = lim.try_acquire(100, 2, 150).unwrap();
        assert_eq!(lim.inflight_requests(), 1);
        assert_eq!(lim.inflight_bytes(), 100);

        // 0-byte request when inflight_bytes == 100 < 150 succeeds
        let p2 = lim.try_acquire(0, 2, 150).unwrap();
        // 3rd request exceeds max_requests = 2
        assert_eq!(
            lim.try_acquire(10, 2, 150).err(),
            Some(InflightLimitError::MaxInflightRequests)
        );
        drop(p2);
        // Exceeding bytes
        assert_eq!(
            lim.try_acquire(60, 2, 150).err(),
            Some(InflightLimitError::MaxInflightBytes)
        );
        drop(p1);
        assert_eq!(lim.inflight_requests(), 0);
        assert_eq!(lim.inflight_bytes(), 0);
    }

    #[test]
    fn test_reactive_limiter_aimd() {
        let cfg = ReactiveLimiterConfig {
            min_limit: 2,
            max_limit: 100,
            initial_limit: 10,
            recent_window_min_samples: 10,
            recent_window_min_duration: Duration::from_millis(10),
            ..Default::default()
        };
        let rl = ReactiveLimiter::new(cfg).unwrap();
        assert_eq!(rl.limit(), 10);

        let mut now = Instant::now();
        // Hold 10 permits concurrently so max_inflight = 10 (max_limit = 10 * 4.0 = 40)
        // Warmup with fast RTT (1ms) -> limit should increase above 10
        for _ in 0..12 {
            now += Duration::from_millis(20);
            let mut permits = Vec::new();
            while let Some(p) = rl.try_acquire_permit() {
                permits.push(p);
            }
            for p in permits {
                p.record_with_rtt(now, Duration::from_millis(1));
            }
        }
        assert!(rl.limit() > 10, "expected limit to grow, got {}", rl.limit());
    }
}
