//! Suspicion timers and node-health awareness.
//!
//! Ported from github.com/hashicorp/memberlist/suspicion.go and awareness.go (grafana fork).

use std::collections::HashSet;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::Notify;
use tokio::time::Instant;

use super::gometrics::GoMetrics;

type TimeoutFn = Box<dyn Fn(i32) + Send + Sync>;

/// Go `suspicion`: manages the suspect timer for a node and provides an interface to accelerate
/// the timeout as independent confirmations arrive.
pub struct Suspicion {
    n: AtomicI32,
    k: i32,
    min: Duration,
    max: Duration,
    start: Instant,
    confirmations: Mutex<HashSet<String>>,
    /// `Some(deadline)` while the timer is armed; `None` once fired or stopped.
    deadline: Mutex<Option<Instant>>,
    rearm: Notify,
    timeout_fn: TimeoutFn,
}

/// Go `remainingSuspicionTime`, in nanoseconds (may be negative, like the Go `time.Duration`).
pub fn remaining_suspicion_time(n: i32, k: i32, elapsed: Duration, min: Duration, max: Duration) -> i64 {
    let frac = ((n as f64) + 1.0).ln() / ((k as f64) + 1.0).ln();
    let raw = max.as_secs_f64() - frac * (max.as_secs_f64() - min.as_secs_f64());
    let mut timeout = (1000.0 * raw).floor() as i64 * 1_000_000;
    let min_ns = min.as_nanos() as i64;
    if timeout < min_ns {
        timeout = min_ns;
    }
    timeout - elapsed.as_nanos() as i64
}

impl Suspicion {
    /// Go `newSuspicion`. Must be called inside a tokio runtime.
    pub fn new(from: &str, k: i32, min: Duration, max: Duration, f: TimeoutFn) -> Arc<Self> {
        let mut confirmations = HashSet::new();
        confirmations.insert(from.to_string());
        let timeout = if k < 1 { min } else { max };
        let start = Instant::now();
        let s = Arc::new(Self {
            n: AtomicI32::new(0),
            k,
            min,
            max,
            start,
            confirmations: Mutex::new(confirmations),
            deadline: Mutex::new(Some(start + timeout)),
            rearm: Notify::new(),
            timeout_fn: f,
        });
        let timer = Arc::clone(&s);
        tokio::spawn(async move { timer.run_timer().await });
        s
    }

    async fn run_timer(self: Arc<Self>) {
        loop {
            let Some(deadline) = *self.deadline.lock() else {
                return;
            };
            tokio::select! {
                _ = tokio::time::sleep_until(deadline) => {
                    let fire = {
                        let mut d = self.deadline.lock();
                        if *d == Some(deadline) {
                            *d = None;
                            true
                        } else {
                            false
                        }
                    };
                    if fire {
                        (self.timeout_fn)(self.n.load(Ordering::Acquire));
                        return;
                    }
                }
                _ = self.rearm.notified() => {}
            }
        }
    }

    /// Go `Confirm`: registers a possibly-new peer confirmation; returns true if it was new.
    pub fn confirm(self: &Arc<Self>, from: &str) -> bool {
        if self.n.load(Ordering::Acquire) >= self.k {
            return false;
        }
        {
            let mut c = self.confirmations.lock();
            if c.contains(from) {
                return false;
            }
            c.insert(from.to_string());
        }
        let n = self.n.fetch_add(1, Ordering::AcqRel) + 1;
        let elapsed = self.start.elapsed();
        let remaining = remaining_suspicion_time(n, self.k, elapsed, self.min, self.max);
        let remaining = (remaining > 0).then(|| Duration::from_nanos(remaining as u64));
        let mut d = self.deadline.lock();
        if d.is_some() {
            match remaining {
                Some(r) => {
                    *d = Some(Instant::now() + r);
                    drop(d);
                    self.rearm.notify_one();
                }
                None => {
                    *d = None;
                    drop(d);
                    self.rearm.notify_one();
                    let me = Arc::clone(self);
                    tokio::spawn(async move {
                        (me.timeout_fn)(me.n.load(Ordering::Acquire));
                    });
                }
            }
        }
        true
    }

    /// Stops the timer without firing (used on shutdown).
    pub fn stop(&self) {
        *self.deadline.lock() = None;
        self.rearm.notify_one();
    }
}

/// Go `awareness`: tracks our own health score (lower is better).
pub struct Awareness {
    max: i32,
    score: Mutex<i32>,
    go_metrics: Arc<GoMetrics>,
}

impl Awareness {
    pub fn new(max: i32, go_metrics: Arc<GoMetrics>) -> Self {
        Self {
            max,
            score: Mutex::new(0),
            go_metrics,
        }
    }

    /// Go `ApplyDelta`.
    pub fn apply_delta(&self, delta: i32) {
        let (initial, fin) = {
            let mut s = self.score.lock();
            let initial = *s;
            *s += delta;
            if *s < 0 {
                *s = 0;
            } else if *s > self.max - 1 {
                *s = self.max - 1;
            }
            (initial, *s)
        };
        if initial != fin {
            self.go_metrics
                .set_gauge(&["memberlist", "health", "score"], fin as f64, &[]);
        }
    }

    /// Go `GetHealthScore`.
    pub fn get_health_score(&self) -> i32 {
        *self.score.lock()
    }

    /// Go `ScaleTimeout`.
    pub fn scale_timeout(&self, timeout: Duration) -> Duration {
        let score = *self.score.lock();
        timeout * (score as u32 + 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Upstream: github.com/hashicorp/memberlist/suspicion_test.go::TestSuspicion_remainingSuspicionTime
    #[test]
    fn test_suspicion_remaining_suspicion_time() {
        let s = |x: i64| x * 1_000_000_000;
        let d = Duration::from_secs;
        let cases: Vec<(i32, i32, Duration, Duration, Duration, i64)> = vec![
            (0, 3, d(0), d(2), d(30), s(30)),
            (1, 3, d(2), d(2), d(30), s(14)),
            (2, 3, d(3), d(2), d(30), 4810 * 1_000_000),
            (3, 3, d(4), d(2), d(30), s(-2)),
            (4, 3, d(5), d(2), d(30), s(-3)),
            (5, 3, d(10), d(2), d(30), s(-8)),
        ];
        for (i, (n, k, elapsed, min, max, expected)) in cases.into_iter().enumerate() {
            assert_eq!(remaining_suspicion_time(n, k, elapsed, min, max), expected, "case {i}");
        }
    }

    // Upstream: github.com/hashicorp/memberlist/awareness_test.go::TestAwareness
    #[test]
    fn test_awareness() {
        let sec = Duration::from_secs;
        let cases: [(i32, i32, Duration); 14] = [
            (0, 0, sec(1)),
            (-1, 0, sec(1)),
            (-10, 0, sec(1)),
            (1, 1, sec(2)),
            (-1, 0, sec(1)),
            (10, 7, sec(8)),
            (-1, 6, sec(7)),
            (-1, 5, sec(6)),
            (-1, 4, sec(5)),
            (-1, 3, sec(4)),
            (-1, 2, sec(3)),
            (-1, 1, sec(2)),
            (-1, 0, sec(1)),
            (-1, 0, sec(1)),
        ];
        let a = Awareness::new(8, Arc::new(GoMetrics::new()));
        for (i, (delta, score, timeout)) in cases.iter().enumerate() {
            a.apply_delta(*delta);
            assert_eq!(a.get_health_score(), *score, "case {i}");
            assert_eq!(a.scale_timeout(sec(1)), *timeout, "case {i}");
        }
    }

    fn channel_fn() -> (TimeoutFn, tokio::sync::mpsc::UnboundedReceiver<i32>) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<i32>();
        (Box::new(move |n| {
            let _ = tx.send(n);
        }), rx)
    }

    // Upstream: github.com/hashicorp/memberlist/suspicion_test.go::TestSuspicion_Timer
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_suspicion_timer() {
        const K: i32 = 3;
        let min = Duration::from_millis(500);
        let max = Duration::from_secs(2);
        let ms = Duration::from_millis;
        type Case = (i32, Vec<(&'static str, bool)>, Duration);
        let cases: Vec<Case> = vec![
            (0, vec![], max),
            (1, vec![("me", false), ("foo", true)], ms(1250)),
            (1, vec![("me", false), ("foo", true), ("foo", false), ("foo", false)], ms(1250)),
            (2, vec![("me", false), ("foo", true), ("bar", true)], ms(810)),
            (3, vec![("me", false), ("foo", true), ("bar", true), ("baz", true)], min),
            (3, vec![("me", false), ("foo", true), ("bar", true), ("baz", true), ("zoo", false)], min),
        ];
        for (i, (num_confirmations, confirmations, expected)) in cases.into_iter().enumerate() {
            let (f, mut rx) = channel_fn();
            let s = Suspicion::new("me", K, min, max, f);
            let fudge = ms(25);
            for (from, new_info) in &confirmations {
                tokio::time::sleep(fudge).await;
                assert_eq!(s.confirm(from), *new_info, "case {i}: newInfo mismatch for {from}");
            }
            let already = fudge * confirmations.len() as u32;
            tokio::time::sleep(expected - already - fudge).await;
            assert!(rx.try_recv().is_err(), "case {i}: should not have fired");
            tokio::time::sleep(2 * fudge).await;
            let n = rx.try_recv().unwrap_or_else(|_| panic!("case {i}: should have fired"));
            assert_eq!(n, num_confirmations, "case {i}");
            s.confirm("late");
            tokio::time::sleep(expected + 2 * fudge).await;
            assert!(rx.try_recv().is_err(), "case {i}: should not have fired again");
        }
    }

    // Upstream: github.com/hashicorp/memberlist/suspicion_test.go::TestSuspicion_Timer_ZeroK
    #[tokio::test]
    async fn test_suspicion_timer_zero_k() {
        let (f, mut rx) = channel_fn();
        let s = Suspicion::new("me", 0, Duration::from_millis(25), Duration::from_secs(30), f);
        assert!(!s.confirm("foo"), "should not provide new information");
        tokio::time::timeout(Duration::from_millis(50), rx.recv())
            .await
            .expect("should have fired");
    }

    // Upstream: github.com/hashicorp/memberlist/suspicion_test.go::TestSuspicion_Timer_Immediate
    #[tokio::test]
    async fn test_suspicion_timer_immediate() {
        let (f, mut rx) = channel_fn();
        let s = Suspicion::new("me", 1, Duration::from_millis(100), Duration::from_secs(30), f);
        tokio::time::sleep(Duration::from_millis(200)).await;
        s.confirm("foo");
        tokio::time::timeout(Duration::from_millis(25), rx.recv())
            .await
            .expect("should have fired");
    }
}
