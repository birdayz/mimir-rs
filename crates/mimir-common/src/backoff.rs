//! Exponential backoff with jitter.
//!
//! Ported from github.com/grafana/dskit/backoff/backoff.go.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rand::Rng;
use thiserror::Error;

/// Configuration for exponential [`Backoff`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackoffConfig {
    /// Minimum delay when backing off (default: 100ms).
    pub min_backoff: Duration,
    /// Maximum delay when backing off (default: 10s).
    pub max_backoff: Duration,
    /// Maximum number of retries before giving up; 0 means infinite retries (default: 10).
    pub max_retries: usize,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            min_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(10),
            max_retries: 10,
        }
    }
}

/// Reason a [`Backoff`] terminated.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum BackoffError {
    #[error("context canceled")]
    Canceled,
    #[error("context deadline exceeded")]
    DeadlineExceeded,
    #[error("{0}")]
    Cause(String),
    #[error("terminated after {0} retries")]
    MaxRetriesReached(usize),
}

/// Exponential backoff with randomized jitter within `[next_delay_min, next_delay_max)`.
#[derive(Debug, Clone)]
pub struct Backoff {
    cfg: BackoffConfig,
    canceled: Option<Arc<AtomicBool>>,
    deadline: Option<std::time::Instant>,
    cause: Option<Arc<parking_lot::Mutex<Option<String>>>>,
    num_retries: usize,
    next_delay_min: Duration,
    next_delay_max: Duration,
}

impl Backoff {
    /// Creates a new [`Backoff`] with the given configuration.
    pub fn new(cfg: BackoffConfig) -> Self {
        Self {
            cfg,
            canceled: None,
            deadline: None,
            cause: None,
            num_retries: 0,
            next_delay_min: cfg.min_backoff,
            next_delay_max: double_duration(cfg.min_backoff, cfg.max_backoff),
        }
    }

    /// Creates a new [`Backoff`] that also stops when `canceled` becomes `true`.
    pub fn with_cancel_flag(cfg: BackoffConfig, canceled: Arc<AtomicBool>) -> Self {
        let mut b = Self::new(cfg);
        b.canceled = Some(canceled);
        b
    }

    /// Creates a new [`Backoff`] with a cancellation flag and optional cause string.
    pub fn with_cancel_cause(
        cfg: BackoffConfig,
        canceled: Arc<AtomicBool>,
        cause: Arc<parking_lot::Mutex<Option<String>>>,
    ) -> Self {
        let mut b = Self::with_cancel_flag(cfg, canceled);
        b.cause = Some(cause);
        b
    }

    /// Creates a new [`Backoff`] with a deadline and optional cause string.
    pub fn with_deadline(
        cfg: BackoffConfig,
        deadline: std::time::Instant,
        canceled: Option<Arc<AtomicBool>>,
        cause: Option<Arc<parking_lot::Mutex<Option<String>>>>,
    ) -> Self {
        let mut b = Self::new(cfg);
        b.deadline = Some(deadline);
        b.canceled = canceled;
        b.cause = cause;
        b
    }

    /// Resets the backoff back to its initial condition.
    pub fn reset(&mut self) {
        self.num_retries = 0;
        self.next_delay_min = self.cfg.min_backoff;
        self.next_delay_max = double_duration(self.cfg.min_backoff, self.cfg.max_backoff);
    }

    fn is_canceled(&self) -> bool {
        self.canceled
            .as_ref()
            .is_some_and(|flag| flag.load(Ordering::Acquire))
    }

    fn is_deadline_exceeded(&self) -> bool {
        self.deadline
            .is_some_and(|dl| std::time::Instant::now() >= dl)
    }

    /// Returns `true` if caller should keep going.
    pub fn ongoing(&self) -> bool {
        !self.is_canceled()
            && !self.is_deadline_exceeded()
            && (self.cfg.max_retries == 0 || self.num_retries < self.cfg.max_retries)
    }

    /// Returns the reason for terminating the backoff, or `None` if it didn't terminate.
    pub fn err(&self) -> Option<BackoffError> {
        if self.is_canceled() {
            return Some(BackoffError::Canceled);
        }
        if self.is_deadline_exceeded() {
            return Some(BackoffError::DeadlineExceeded);
        }
        if self.cfg.max_retries != 0 && self.num_retries >= self.cfg.max_retries {
            return Some(BackoffError::MaxRetriesReached(self.num_retries));
        }
        None
    }

    /// Like [`Backoff::err`], but returns the configured cause if terminated via cancellation or deadline.
    pub fn err_cause(&self) -> Option<BackoffError> {
        if self.is_canceled() || self.is_deadline_exceeded() {
            if let Some(ref cause) = self.cause {
                if let Some(msg) = cause.lock().clone() {
                    return Some(BackoffError::Cause(msg));
                }
            }
        }
        self.err()
    }

    /// Returns the number of retries attempted so far.
    pub fn num_retries(&self) -> usize {
        self.num_retries
    }

    /// Increments the retry count, computes the next jittered delay, and advances the backoff window.
    pub fn next_delay(&mut self) -> Duration {
        self.num_retries += 1;

        if self.next_delay_min >= self.next_delay_max {
            return self.next_delay_min;
        }

        let range = self.next_delay_max - self.next_delay_min;
        let range_nanos = range.as_nanos().min(u64::MAX as u128) as u64;
        let jitter = if range_nanos > 0 {
            Duration::from_nanos(rand::thread_rng().gen_range(0..range_nanos))
        } else {
            Duration::ZERO
        };
        let sleep_time = self.next_delay_min + jitter;

        if self.next_delay_max < self.cfg.max_backoff {
            self.next_delay_min = double_duration(self.next_delay_min, self.cfg.max_backoff);
            self.next_delay_max = double_duration(self.next_delay_max, self.cfg.max_backoff);
        }

        sleep_time
    }

    /// Sleeps asynchronously for the backoff time and advances retry state.
    pub async fn wait(&mut self) {
        let sleep_time = self.next_delay();
        if self.ongoing() && !sleep_time.is_zero() {
            tokio::time::sleep(sleep_time).await;
        }
    }
}

fn double_duration(value: Duration, max: Duration) -> Duration {
    let doubled = value.saturating_mul(2);
    if doubled <= max {
        doubled
    } else {
        max
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backoff_progression_and_limits() {
        let cfg = BackoffConfig {
            min_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(50),
            max_retries: 4,
        };
        let mut b = Backoff::new(cfg);
        assert!(b.ongoing());
        assert_eq!(b.err(), None);

        let d1 = b.next_delay();
        assert!(d1 >= Duration::from_millis(10) && d1 < Duration::from_millis(20));
        assert!(b.ongoing());

        let d2 = b.next_delay();
        assert!(d2 >= Duration::from_millis(20) && d2 < Duration::from_millis(40));

        let d3 = b.next_delay();
        assert!(d3 >= Duration::from_millis(40) && d3 < Duration::from_millis(50));

        let d4 = b.next_delay();
        assert!(d4 >= Duration::from_millis(40) && d4 < Duration::from_millis(50));
        assert!(!b.ongoing());
        assert_eq!(b.num_retries(), 4);
        assert_eq!(b.err(), Some(BackoffError::MaxRetriesReached(4)));

        b.reset();
        assert!(b.ongoing());
        assert_eq!(b.num_retries(), 0);
    }

    #[test]
    fn test_backoff_cancel() {
        let canceled = Arc::new(AtomicBool::new(false));
        let mut b = Backoff::with_cancel_flag(BackoffConfig::default(), canceled.clone());
        assert!(b.ongoing());
        canceled.store(true, Ordering::Release);
        assert!(!b.ongoing());
        assert_eq!(b.err(), Some(BackoffError::Canceled));
        let _ = b.next_delay();
        assert_eq!(b.err(), Some(BackoffError::Canceled));
    }
}
