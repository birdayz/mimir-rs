//! Priority-based rejection Prioritizer shared by the ingester push and read reactive limiters.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/util/reactivelimiter/prioritizer.go
//! - github.com/failsafe-go/failsafe-go/priority/priority.go (Priority, level ranges, LevelTracker)
//! - github.com/failsafe-go/failsafe-go/internal/prioritizer.go (BasePrioritizer)
//! - github.com/failsafe-go/failsafe-go/adaptivelimiter/prioritizer.go (queueRejectionStrategy)
//! - github.com/failsafe-go/failsafe-go/adaptivelimiter/queueinglimiter.go (queueStats)
//!
//! No `UsageTracker` is configured by Mimir, so usage-based levels are not ported.

use std::sync::atomic::{AtomicI32, Ordering};

use parking_lot::Mutex;
use rand::Rng;
use tracing::debug;

/// Total number of priority levels (0-499).
pub const TOTAL_LEVELS: usize = 500;

/// Execution priority (Go: `priority.Priority`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Priority {
    VeryLow = 0,
    Low = 1,
    Medium = 2,
    High = 3,
    VeryHigh = 4,
}

impl Priority {
    /// Go: `priorityLevelRanges`.
    pub fn level_range(self) -> (usize, usize) {
        match self {
            Priority::VeryLow => (0, 99),
            Priority::Low => (100, 199),
            Priority::Medium => (200, 299),
            Priority::High => (300, 399),
            Priority::VeryHigh => (400, 499),
        }
    }

    pub fn min_level(self) -> usize {
        self.level_range().0
    }

    pub fn max_level(self) -> usize {
        self.level_range().1
    }

    /// Go: `Priority.RandomLevel()`.
    pub fn random_level(self) -> usize {
        let (lo, hi) = self.level_range();
        rand::thread_rng().gen_range(lo..=hi)
    }
}

struct LevelTrackerState {
    window: Vec<usize>,
    level_counts: Vec<usize>,
    head: usize,
    filled: bool,
}

/// Windowed level tracker (Go: `priority.windowedLevelTracker`).
pub struct LevelTracker {
    state: Mutex<LevelTrackerState>,
}

impl LevelTracker {
    pub fn new(window_size: usize) -> Self {
        Self {
            state: Mutex::new(LevelTrackerState {
                window: vec![0; window_size],
                level_counts: vec![0; TOTAL_LEVELS],
                head: 0,
                filled: false,
            }),
        }
    }

    pub fn record_level(&self, level: usize) {
        let mut st = self.state.lock();
        if st.filled {
            let old = st.window[st.head];
            st.level_counts[old] -= 1;
        }
        let head = st.head;
        st.window[head] = level;
        st.level_counts[level] += 1;
        st.head += 1;
        if st.head >= st.window.len() {
            st.head = 0;
            st.filled = true;
        }
    }

    /// Returns the level at the quantile among all recorded levels, else 0.
    pub fn get_level(&self, quantile: f64) -> usize {
        let st = self.state.lock();
        let current_size = if st.filled { st.window.len() } else { st.head };
        if current_size > 0 {
            let target = ((current_size as f64 * quantile).ceil() as usize).max(1);
            let mut counted = 0;
            for (level, &count) in st.level_counts.iter().enumerate() {
                counted += count;
                if counted >= target {
                    return level;
                }
            }
        }
        0
    }
}

/// Go: `adaptivelimiter.queueStats`.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct QueueStats {
    pub limit: usize,
    pub queued: usize,
    pub rejection_threshold: usize,
    pub max_queue: usize,
}

impl QueueStats {
    /// Go: `queueStats.ComputeRejectionRate()`.
    pub fn compute_rejection_rate(&self) -> f64 {
        if self.queued < self.rejection_threshold {
            return 0.0;
        }
        if self.queued >= self.max_queue {
            return 1.0;
        }
        (self.queued - self.rejection_threshold) as f64
            / (self.max_queue - self.rejection_threshold) as f64
    }
}

type StatsFn = Box<dyn Fn() -> QueueStats + Send + Sync>;

struct PrioritizerState {
    stats_funcs: Vec<StatsFn>,
    rejection_rate: f64,
}

/// Go: `internal.BasePrioritizer[*queueStats]` built by `adaptivelimiter.NewPrioritizerBuilder()`.
pub struct Prioritizer {
    state: Mutex<PrioritizerState>,
    num_stats: AtomicI32,
    rejection_thresh: AtomicI32,
    pub level_tracker: LevelTracker,
}

impl Default for Prioritizer {
    fn default() -> Self {
        Self::new()
    }
}

impl Prioritizer {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(PrioritizerState {
                stats_funcs: Vec::new(),
                rejection_rate: 0.0,
            }),
            num_stats: AtomicI32::new(0),
            rejection_thresh: AtomicI32::new(0),
            level_tracker: LevelTracker::new(1000),
        }
    }

    pub fn register(&self, f: impl Fn() -> QueueStats + Send + Sync + 'static) {
        let mut st = self.state.lock();
        st.stats_funcs.push(Box::new(f));
        self.num_stats.fetch_add(1, Ordering::AcqRel);
    }

    pub fn rejection_rate(&self) -> f64 {
        self.state.lock().rejection_rate
    }

    pub fn rejection_threshold(&self) -> i32 {
        self.rejection_thresh.load(Ordering::Acquire)
    }

    pub fn registered_policies(&self) -> i32 {
        self.num_stats.load(Ordering::Acquire)
    }

    /// Computes a combined rejection rate and threshold based on all registered limiters.
    pub fn calibrate(&self) {
        let mut st = self.state.lock();
        let mut combined = QueueStats::default();
        for f in &st.stats_funcs {
            let s = f();
            combined.limit += s.limit;
            combined.queued += s.queued;
            combined.rejection_threshold += s.rejection_threshold;
            combined.max_queue += s.max_queue;
        }
        st.rejection_rate = combined.compute_rejection_rate();
        let new_thresh = if st.rejection_rate > 0.0 {
            self.level_tracker.get_level(st.rejection_rate) as i32
        } else {
            0
        };
        let rate = st.rejection_rate;
        drop(st);
        self.rejection_thresh.store(new_thresh, Ordering::Release);
        debug!(
            newRate = %format!("{rate:.2}"),
            newThresh = new_thresh,
            limit = combined.limit,
            queued = combined.queued,
            "prioritizer calibration"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    // Behaviour of failsafe-go priority.windowedLevelTracker.GetLevel (upstream tests are not vendored).
    #[test]
    fn level_tracker_get_level() {
        let lt = LevelTracker::new(100);
        assert_eq!(lt.get_level(0.5), 0, "empty tracker");
        for i in 0..100 {
            lt.record_level(i);
        }
        assert_eq!(lt.get_level(0.0), 0);
        assert_eq!(lt.get_level(0.5), 49);
        assert_eq!(lt.get_level(0.9), 89);
        assert_eq!(lt.get_level(1.0), 99);
    }

    #[test]
    fn level_tracker_window_rolls_over() {
        let lt = LevelTracker::new(10);
        for _ in 0..10 {
            lt.record_level(5);
        }
        for _ in 0..10 {
            lt.record_level(300);
        }
        // All old levels have been evicted.
        assert_eq!(lt.get_level(0.1), 300);
    }

    // Behaviour of failsafe-go priority.Priority.RandomLevel.
    #[test]
    fn priority_random_level_within_range() {
        for p in [
            Priority::VeryLow,
            Priority::Low,
            Priority::Medium,
            Priority::High,
            Priority::VeryHigh,
        ] {
            for _ in 0..100 {
                let l = p.random_level();
                assert!(l >= p.min_level() && l <= p.max_level());
            }
        }
    }

    // Behaviour of failsafe-go adaptivelimiter.queueStats.ComputeRejectionRate.
    #[test]
    fn queue_stats_rejection_rate() {
        let s = |queued| QueueStats {
            limit: 10,
            queued,
            rejection_threshold: 20,
            max_queue: 30,
        };
        assert_eq!(s(0).compute_rejection_rate(), 0.0);
        assert_eq!(s(19).compute_rejection_rate(), 0.0);
        assert_eq!(s(20).compute_rejection_rate(), 0.0);
        assert_eq!(s(25).compute_rejection_rate(), 0.5);
        assert_eq!(s(30).compute_rejection_rate(), 1.0);
        assert_eq!(s(40).compute_rejection_rate(), 1.0);
    }

    // Behaviour of failsafe-go internal.BasePrioritizer.Calibrate with queueRejectionStrategy.
    #[test]
    fn calibrate_combines_stats_and_sets_threshold() {
        let p = Prioritizer::new();
        let q1 = Arc::new(parking_lot::Mutex::new(QueueStats {
            limit: 10,
            queued: 0,
            rejection_threshold: 20,
            max_queue: 30,
        }));
        let q2 = Arc::new(parking_lot::Mutex::new(QueueStats {
            limit: 10,
            queued: 0,
            rejection_threshold: 20,
            max_queue: 30,
        }));
        let (a, b) = (Arc::clone(&q1), Arc::clone(&q2));
        p.register(move || *a.lock());
        p.register(move || *b.lock());
        assert_eq!(p.registered_policies(), 2);

        p.calibrate();
        assert_eq!(p.rejection_rate(), 0.0);
        assert_eq!(p.rejection_threshold(), 0);

        for level in 100..200 {
            p.level_tracker.record_level(level);
        }
        for level in 300..400 {
            p.level_tracker.record_level(level);
        }
        // Combined: threshold 40, max 60, queued 50 => rate 0.5.
        q1.lock().queued = 25;
        q2.lock().queued = 25;
        p.calibrate();
        assert_eq!(p.rejection_rate(), 0.5);
        assert_eq!(p.rejection_threshold(), 199);

        q1.lock().queued = 30;
        q2.lock().queued = 30;
        p.calibrate();
        assert_eq!(p.rejection_rate(), 1.0);
        assert_eq!(p.rejection_threshold(), 399);

        q1.lock().queued = 0;
        q2.lock().queued = 0;
        p.calibrate();
        assert_eq!(p.rejection_rate(), 0.0);
        assert_eq!(p.rejection_threshold(), 0);
    }
}
