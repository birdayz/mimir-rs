//! CPU/memory utilization based read-path limiter.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/util/limiter/utilization.go
//! - github.com/grafana/mimir/pkg/util/limiter/utilization_test.go (tests below)
//! - github.com/grafana/mimir/pkg/util/math/rate.go (`EwmaRate`, reused from `user_tsdb`)
//!
//! Wired from `pkg/ingester/ingester.go` (`NewUtilizationBasedLimiter` when
//! `read_path_cpu_utilization_limit > 0 || read_path_memory_utilization_limit > 0`, registered with
//! the `cortex_ingester_` prefix and started as a 1s timer service after the ring lifecycler).
//!
//! Memory: Go reads the Go runtime heap (`/memory/classes/heap/objects:bytes`). A Rust process has
//! no Go heap; the closest equivalent that excludes file-backed mmap pages (TSDB chunks, index) is
//! the process' anonymous resident memory (`RssAnon` in `/proc/self/status`), which covers the
//! allocator heap. CPU time is read from `/proc/self/stat` exactly like `procfs.ProcStat.CPUTime()`
//! (`(utime + stime) / USER_HZ`, USER_HZ = 100).

use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use prometheus::Registry;
use tracing::{info, warn};

use crate::ingester::UtilizationBasedLimiter as UtilizationBasedLimiterTrait;
use crate::reactive_limiter::FnGauge;
use crate::user_tsdb::EwmaRate;

/// Interval for updating resource (CPU/memory) utilization.
pub const RESOURCE_UTILIZATION_UPDATE_INTERVAL: Duration = Duration::from_secs(1);

/// How long is the sliding window used to compute the moving average.
pub const RESOURCE_UTILIZATION_SLIDING_WINDOW: Duration = Duration::from_secs(60);

/// `procfs` userHZ constant used by `ProcStat.CPUTime()`.
const USER_HZ: f64 = 100.0;

/// Scans CPU time (seconds) and memory usage (bytes).
pub trait UtilizationScanner: Send {
    fn scan(&mut self) -> Result<(f64, u64), String>;
}

/// Linux `/proc/self` scanner (Go: `combinedScanner`).
pub struct ProcScanner {
    stat_path: String,
    status_path: String,
}

impl ProcScanner {
    /// Go: `newCombinedScanner()`. Fails on platforms without `/proc/self`.
    pub fn new() -> Result<Self, String> {
        Self::with_paths("/proc/self/stat", "/proc/self/status")
    }

    pub fn with_paths(stat_path: &str, status_path: &str) -> Result<Self, String> {
        let s = Self {
            stat_path: stat_path.to_string(),
            status_path: status_path.to_string(),
        };
        // procfs.Self() fails when /proc is not mounted; probe it the same way.
        std::fs::metadata(&s.stat_path).map_err(|e| e.to_string())?;
        Ok(s)
    }
}

/// Parses `(utime + stime) / USER_HZ` from the contents of `/proc/<pid>/stat`.
pub fn parse_proc_stat_cpu_time(stat: &str) -> Result<f64, String> {
    // The comm field may contain spaces and parentheses; fields resume after the last ')'.
    let rest = stat
        .rfind(')')
        .map(|i| &stat[i + 1..])
        .ok_or_else(|| "unexpected format: missing ')'".to_string())?;
    // rest fields start at field 3 (state). utime is field 14, stime is field 15.
    let fields: Vec<&str> = rest.split_whitespace().collect();
    if fields.len() < 13 {
        return Err(format!("unexpected number of fields in stat: {}", fields.len()));
    }
    let utime: u64 = fields[11].parse().map_err(|e| format!("invalid utime: {e}"))?;
    let stime: u64 = fields[12].parse().map_err(|e| format!("invalid stime: {e}"))?;
    Ok((utime + stime) as f64 / USER_HZ)
}

/// Parses `RssAnon` (bytes) from the contents of `/proc/<pid>/status`.
pub fn parse_proc_status_rss_anon(status: &str) -> Result<u64, String> {
    for line in status.lines() {
        if let Some(v) = line.strip_prefix("RssAnon:") {
            let kb = v
                .trim()
                .trim_end_matches("kB")
                .trim()
                .parse::<u64>()
                .map_err(|e| format!("invalid RssAnon: {e}"))?;
            return Ok(kb * 1024);
        }
    }
    Err("RssAnon not found in status".to_string())
}

impl UtilizationScanner for ProcScanner {
    fn scan(&mut self) -> Result<(f64, u64), String> {
        let stat = std::fs::read_to_string(&self.stat_path)
            .map_err(|e| format!("failed to get process stats: {e}"))?;
        let cpu = parse_proc_stat_cpu_time(&stat)
            .map_err(|e| format!("failed to get process stats: {e}"))?;
        let status = std::fs::read_to_string(&self.status_path)
            .map_err(|e| format!("failed to get process memory: {e}"))?;
        let mem = parse_proc_status_rss_anon(&status)
            .map_err(|e| format!("failed to get process memory: {e}"))?;
        Ok((cpu, mem))
    }
}

/// Circular buffer of CPU samples (Go: `cpuSampleBuffer`).
pub struct CpuSampleBuffer {
    samples: Vec<f64>,
    head: usize,
}

impl CpuSampleBuffer {
    pub fn new(size: usize) -> Self {
        Self {
            samples: vec![0.0; size],
            head: 0,
        }
    }

    pub fn add(&mut self, sample: f64) {
        self.samples[self.head] = sample;
        self.head = (self.head + 1) % self.samples.len();
    }
}

impl std::fmt::Display for CpuSampleBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let n = self.samples.len();
        for i in 0..n {
            write!(f, "{:.2}", self.samples[(self.head + i) % n])?;
            if i < n - 1 {
                f.write_str(",")?;
            }
        }
        Ok(())
    }
}

const REASON_NONE: u8 = 0;
const REASON_MEMORY: u8 = 1;
const REASON_CPU: u8 = 2;

fn reason_str(r: u8) -> &'static str {
    match r {
        REASON_MEMORY => "memory",
        REASON_CPU => "cpu",
        _ => "",
    }
}

struct State {
    scanner: Option<Box<dyn UtilizationScanner>>,
    last_cpu_time: f64,
    first_cpu_update: Option<Instant>,
    last_cpu_update: Option<Instant>,
    cpu_samples: Option<CpuSampleBuffer>,
}

/// Go: `UtilizationBasedLimiter`.
pub struct UtilizationBasedLimiter {
    /// Memory limit in bytes. The limit is enabled if the value is > 0.
    memory_limit: u64,
    /// CPU limit in cores. The limit is enabled if the value is > 0.
    cpu_limit: f64,
    cpu_moving_avg: EwmaRate,
    limiting_reason: AtomicU8,
    curr_cpu_util: AtomicU64,
    curr_heap_size: AtomicU64,
    state: Mutex<State>,
}

impl UtilizationBasedLimiter {
    /// Go: `NewUtilizationBasedLimiter(cpuLimit, memoryLimit, logCPUSamples, logger, reg)`.
    /// `reg` must already carry the `cortex_ingester_` prefix semantics: metric names are
    /// registered with the full `cortex_ingester_utilization_limiter_*` names when
    /// `metric_prefix` is `"cortex_ingester_"`.
    pub fn new(
        cpu_limit: f64,
        memory_limit: u64,
        log_cpu_samples: bool,
        metric_prefix: &str,
        reg: Option<&Registry>,
    ) -> Arc<Self> {
        // Calculate alpha for a minute long window.
        let alpha = 2.0
            / (RESOURCE_UTILIZATION_SLIDING_WINDOW.as_secs_f64()
                / RESOURCE_UTILIZATION_UPDATE_INTERVAL.as_secs_f64()
                + 1.0);
        let cpu_samples = log_cpu_samples
            .then(|| CpuSampleBuffer::new(RESOURCE_UTILIZATION_SLIDING_WINDOW.as_secs() as usize));
        let l = Arc::new(Self {
            memory_limit,
            cpu_limit,
            cpu_moving_avg: EwmaRate::new(alpha, RESOURCE_UTILIZATION_UPDATE_INTERVAL),
            limiting_reason: AtomicU8::new(REASON_NONE),
            curr_cpu_util: AtomicU64::new(0f64.to_bits()),
            curr_heap_size: AtomicU64::new(0),
            state: Mutex::new(State {
                scanner: None,
                last_cpu_time: 0.0,
                first_cpu_update: None,
                last_cpu_update: None,
                cpu_samples,
            }),
        });
        if let Some(r) = reg {
            let l1 = Arc::clone(&l);
            let _ = r.register(Box::new(FnGauge::new(
                &format!("{metric_prefix}utilization_limiter_current_cpu_load"),
                "Current average CPU load calculated by utilization based limiter.",
                &[],
                move || f64::from_bits(l1.curr_cpu_util.load(Ordering::Relaxed)),
            )));
            let l2 = Arc::clone(&l);
            let _ = r.register(Box::new(FnGauge::new(
                &format!("{metric_prefix}utilization_limiter_current_memory_usage_bytes"),
                "Current memory usage calculated by utilization based limiter.",
                &[],
                move || l2.curr_heap_size.load(Ordering::Relaxed) as f64,
            )));
        }
        l
    }

    /// Replaces the scanner (tests; Go sets `lim.utilizationScanner` directly).
    pub fn set_scanner(&self, scanner: Box<dyn UtilizationScanner>) {
        self.state.lock().scanner = Some(scanner);
    }

    /// Go: `starting()`. Creates the platform scanner unless one was injected.
    pub fn starting(&self) -> Result<(), String> {
        let mut st = self.state.lock();
        if st.scanner.is_none() {
            let s = ProcScanner::new().map_err(|e| {
                format!(
                    "unable to detect CPU/memory utilization, unsupported platform. Please disable utilization based limiting: {e}"
                )
            })?;
            st.scanner = Some(Box::new(s));
        }
        Ok(())
    }

    /// Returns the current reason for limiting, if any. Empty means limiting is disabled.
    pub fn limiting_reason_str(&self) -> &'static str {
        reason_str(self.limiting_reason.load(Ordering::Acquire))
    }

    pub fn current_cpu_load(&self) -> f64 {
        f64::from_bits(self.curr_cpu_util.load(Ordering::Relaxed))
    }

    pub fn current_memory_usage_bytes(&self) -> u64 {
        self.curr_heap_size.load(Ordering::Relaxed)
    }

    /// Last CPU time counter used for the moving average (test visibility).
    pub fn last_cpu_time(&self) -> f64 {
        self.state.lock().last_cpu_time
    }

    /// Comma-separated CPU samples window, if sample logging is enabled.
    pub fn cpu_samples_string(&self) -> Option<String> {
        self.state.lock().cpu_samples.as_ref().map(ToString::to_string)
    }

    /// Go: `compute(nowFn)`. Must be called every `RESOURCE_UTILIZATION_UPDATE_INTERVAL`.
    pub fn compute(&self, now_fn: impl FnOnce() -> Instant) -> (f64, u64) {
        let mut st = self.state.lock();
        let scanned = match st.scanner.as_mut() {
            Some(s) => s.scan(),
            None => Err("utilization scanner not started".to_string()),
        };
        let (cpu_time, curr_heap_size) = match scanned {
            Ok(v) => v,
            Err(e) => {
                warn!(context = "read path", err = %e, "failed to get CPU and memory stats");
                // Disable any limiting, since we can't tell resource utilization.
                self.limiting_reason.store(REASON_NONE, Ordering::Release);
                return (0.0, 0);
            }
        };

        // Get wall time after CPU time.
        let now = now_fn();
        self.curr_heap_size.store(curr_heap_size, Ordering::Relaxed);

        if let Some(prev_update) = st.last_cpu_update {
            let since = now.saturating_duration_since(prev_update);
            // Skip updates when called twice in very short succession.
            if since > RESOURCE_UTILIZATION_UPDATE_INTERVAL / 2 {
                let cpu_util = (cpu_time - st.last_cpu_time) / since.as_secs_f64();
                self.cpu_moving_avg.add((cpu_util * 100.0) as i64);
                self.cpu_moving_avg.tick();
                if let Some(ref mut b) = st.cpu_samples {
                    b.add(cpu_util);
                }
                st.last_cpu_update = Some(now);
                st.last_cpu_time = cpu_time;
            }
        } else {
            st.last_cpu_update = Some(now);
            st.last_cpu_time = cpu_time;
        }

        // Warmup period equal to the sliding window: reported CPU utilization is 0.
        let mut curr_cpu_util = 0.0;
        match st.first_cpu_update {
            None => st.first_cpu_update = Some(now),
            Some(first) => {
                if now.saturating_duration_since(first) >= RESOURCE_UTILIZATION_SLIDING_WINDOW {
                    curr_cpu_util = self.cpu_moving_avg.rate() / 100.0;
                    self.curr_cpu_util
                        .store(curr_cpu_util.to_bits(), Ordering::Relaxed);
                }
            }
        }

        let reason = if self.memory_limit > 0 && curr_heap_size >= self.memory_limit {
            REASON_MEMORY
        } else if self.cpu_limit > 0.0 && curr_cpu_util >= self.cpu_limit {
            REASON_CPU
        } else {
            REASON_NONE
        };

        let enable = reason != REASON_NONE;
        let prev_enable = self.limiting_reason.load(Ordering::Acquire) != REASON_NONE;
        if enable == prev_enable {
            return (curr_cpu_util, curr_heap_size);
        }

        if enable {
            let samples = st.cpu_samples.as_ref().map(ToString::to_string);
            info!(
                context = "read path",
                reason = reason_str(reason),
                memory_limit = %format_memory_limit(self.memory_limit),
                memory_utilization = %format_memory(curr_heap_size),
                cpu_limit = %format_cpu_limit(self.cpu_limit),
                cpu_utilization = %format_cpu(curr_cpu_util),
                source_samples = samples.as_deref().unwrap_or(""),
                "enabling resource utilization based limiting"
            );
        } else {
            info!(
                context = "read path",
                memory_limit = %format_memory_limit(self.memory_limit),
                memory_utilization = %format_memory(curr_heap_size),
                cpu_limit = %format_cpu_limit(self.cpu_limit),
                cpu_utilization = %format_cpu(curr_cpu_util),
                "disabling resource utilization based limiting"
            );
        }

        self.limiting_reason.store(reason, Ordering::Release);
        (curr_cpu_util, curr_heap_size)
    }

    /// Runs the timer service (Go: `services.NewTimerService(resourceUtilizationUpdateInterval, ...)`)
    /// until `stop` resolves.
    pub async fn run(self: Arc<Self>, stop: impl std::future::Future<Output = ()>) {
        tokio::pin!(stop);
        let start = tokio::time::Instant::now() + RESOURCE_UTILIZATION_UPDATE_INTERVAL;
        let mut ticker = tokio::time::interval_at(start, RESOURCE_UTILIZATION_UPDATE_INTERVAL);
        loop {
            tokio::select! {
                _ = &mut stop => break,
                _ = ticker.tick() => {
                    let me = Arc::clone(&self);
                    // /proc reads are blocking syscalls; keep them off the async workers.
                    let _ = tokio::task::spawn_blocking(move || {
                        me.compute(Instant::now);
                    })
                    .await;
                }
            }
        }
    }
}

impl UtilizationBasedLimiterTrait for UtilizationBasedLimiter {
    fn limiting_reason(&self) -> Option<String> {
        let r = self.limiting_reason_str();
        if r.is_empty() {
            None
        } else {
            Some(r.to_string())
        }
    }
}

pub fn format_cpu(value: f64) -> String {
    format!("{value:.2}")
}

pub fn format_cpu_limit(limit: f64) -> String {
    if limit == 0.0 {
        return "disabled".to_string();
    }
    format_cpu(limit)
}

pub fn format_memory(value: u64) -> String {
    value.to_string()
}

pub fn format_memory_limit(limit: u64) -> String {
    if limit == 0 {
        return "disabled".to_string();
    }
    format_memory(limit)
}

#[cfg(test)]
mod tests {
    use super::*;

    const GIGABYTE: u64 = 1024 * 1024 * 1024;

    #[derive(Default)]
    struct FakeScanner {
        total_time: f64,
        counter: usize,
        memory_utilization: Arc<AtomicU64>,
    }

    impl UtilizationScanner for FakeScanner {
        fn scan(&mut self) -> Result<(f64, u64), String> {
            self.total_time += 1.0 / (60 - self.counter) as f64;
            self.counter += 1;
            self.counter %= 60;
            Ok((self.total_time, self.memory_utilization.load(Ordering::Relaxed)))
        }
    }

    struct PreRecordedScanner {
        instant_cpu_values: Vec<f64>,
        pos: usize,
        total: f64,
    }

    impl PreRecordedScanner {
        fn new(values: Vec<f64>) -> Self {
            Self {
                instant_cpu_values: values,
                pos: 0,
                total: 0.0,
            }
        }
    }

    impl UtilizationScanner for PreRecordedScanner {
        fn scan(&mut self) -> Result<(f64, u64), String> {
            if self.pos >= self.instant_cpu_values.len() {
                return Ok((self.total, 0));
            }
            self.total += self.instant_cpu_values[self.pos];
            self.pos += 1;
            Ok((self.total, 0))
        }
    }

    fn setup(
        cpu_limit: f64,
        memory_limit: u64,
        log: bool,
    ) -> (Arc<UtilizationBasedLimiter>, Arc<AtomicU64>, Registry) {
        let reg = Registry::new();
        let lim = UtilizationBasedLimiter::new(cpu_limit, memory_limit, log, "", Some(&reg));
        let mem = Arc::new(AtomicU64::new(0));
        lim.set_scanner(Box::new(FakeScanner {
            memory_utilization: Arc::clone(&mem),
            ..Default::default()
        }));
        assert_eq!(lim.limiting_reason_str(), "", "Limiting should initially be disabled");
        (lim, mem, reg)
    }

    fn gather(reg: &Registry) -> (f64, f64) {
        let mut cpu = f64::NAN;
        let mut mem = f64::NAN;
        for mf in reg.gather() {
            let v = mf.get_metric()[0].get_gauge().get_value();
            match mf.get_name() {
                "utilization_limiter_current_cpu_load" => cpu = v,
                "utilization_limiter_current_memory_usage_bytes" => mem = v,
                _ => {}
            }
        }
        (cpu, mem)
    }

    fn warmup(lim: &UtilizationBasedLimiter, tim: &mut Instant) {
        for _ in 0..RESOURCE_UTILIZATION_SLIDING_WINDOW.as_secs() {
            let t = *tim;
            lim.compute(|| t);
            *tim += RESOURCE_UTILIZATION_UPDATE_INTERVAL;
        }
    }

    // Upstream: pkg/util/limiter/utilization_test.go::TestUtilizationBasedLimiter
    #[test]
    fn test_utilization_based_limiter() {
        // CPU based limiting should be enabled if set to a value greater than 0
        {
            let mut tim = Instant::now();
            let (lim, _, reg) = setup(0.11, GIGABYTE, true);
            warmup(&lim, &mut tim);
            for _ in 0..59 {
                let t = tim;
                lim.compute(|| t);
                tim += RESOURCE_UTILIZATION_UPDATE_INTERVAL;
                assert_eq!(lim.limiting_reason_str(), "", "Limiting should be disabled");
            }
            let t = tim;
            lim.compute(|| t);
            tim += RESOURCE_UTILIZATION_UPDATE_INTERVAL;
            assert_eq!(lim.limiting_reason_str(), "cpu");
            for _ in 0..5 {
                let t = tim;
                lim.compute(|| t);
                tim += RESOURCE_UTILIZATION_UPDATE_INTERVAL;
            }
            assert_eq!(lim.limiting_reason_str(), "", "Limiting should be disabled again");
            let (cpu, mem) = gather(&reg);
            assert!((cpu - 0.10803555562923002).abs() < 1e-12, "cpu={cpu}");
            assert_eq!(mem, 0.0);
        }

        // CPU based limiting should be disabled if set to 0
        {
            let mut tim = Instant::now();
            let (lim, _, reg) = setup(0.0, GIGABYTE, true);
            warmup(&lim, &mut tim);
            for _ in 0..60 {
                let t = tim;
                lim.compute(|| t);
                tim += RESOURCE_UTILIZATION_UPDATE_INTERVAL;
                assert_eq!(lim.limiting_reason_str(), "");
            }
            let (cpu, mem) = gather(&reg);
            assert!((cpu - 0.12581711205891943).abs() < 1e-12, "cpu={cpu}");
            assert_eq!(mem, 0.0);
        }

        // memory based limiting should be enabled if set to a value greater than 0
        {
            let tim = Instant::now();
            let (lim, mem, reg) = setup(0.11, GIGABYTE, true);
            lim.compute(|| tim);
            mem.store(GIGABYTE, Ordering::Relaxed);
            lim.compute(|| tim);
            assert_eq!(lim.limiting_reason_str(), "memory");
            mem.store(GIGABYTE - 1, Ordering::Relaxed);
            lim.compute(|| tim + RESOURCE_UTILIZATION_UPDATE_INTERVAL);
            assert_eq!(lim.limiting_reason_str(), "");
            let (cpu, m) = gather(&reg);
            assert_eq!(cpu, 0.0);
            assert_eq!(m, 1.073741823e9);
        }

        // memory based limiting should be disabled if set to 0
        {
            let tim = Instant::now();
            let (lim, mem, reg) = setup(0.11, 0, true);
            lim.compute(|| tim);
            mem.store(GIGABYTE, Ordering::Relaxed);
            lim.compute(|| tim);
            assert_eq!(lim.limiting_reason_str(), "");
            let (cpu, m) = gather(&reg);
            assert_eq!(cpu, 0.0);
            assert_eq!(m, 1.073741824e9);
        }

        // limiting should work without CPU samples logging
        {
            let mut tim = Instant::now();
            let (lim, _, _) = setup(0.11, GIGABYTE, false);
            warmup(&lim, &mut tim);
            for _ in 0..59 {
                let t = tim;
                lim.compute(|| t);
                tim += RESOURCE_UTILIZATION_UPDATE_INTERVAL;
                assert_eq!(lim.limiting_reason_str(), "");
            }
            let t = tim;
            lim.compute(|| t);
            assert_eq!(lim.limiting_reason_str(), "cpu");
            assert!(lim.cpu_samples_string().is_none());
        }

        // the limiter should collect the last 60 CPU samples
        {
            let inst: Vec<f64> = (1..=62).map(f64::from).collect();
            let lim = UtilizationBasedLimiter::new(1.0, 0, true, "", Some(&Registry::new()));
            lim.set_scanner(Box::new(PreRecordedScanner::new(inst.clone())));
            let mut ts = Instant::now();
            for _ in 0..inst.len() {
                let t = ts;
                lim.compute(|| t);
                ts += RESOURCE_UTILIZATION_UPDATE_INTERVAL;
            }
            let exp = inst[2..62]
                .iter()
                .map(|v| format!("{v:.2}"))
                .collect::<Vec<_>>()
                .join(",");
            assert_eq!(lim.cpu_samples_string().unwrap(), exp);
        }
    }

    // Upstream: pkg/util/limiter/utilization_test.go::TestFormatCPU
    #[test]
    fn test_format_cpu() {
        assert_eq!(format_cpu(0.0), "0.00");
        assert_eq!(format_cpu(0.11), "0.11");
        assert_eq!(format_cpu(0.111), "0.11");
        assert_eq!(format_cpu(0.115), "0.12");
        assert_eq!(format_cpu(2.1), "2.10");
    }

    // Upstream: pkg/util/limiter/utilization_test.go::TestFormatCPULimit
    #[test]
    fn test_format_cpu_limit() {
        assert_eq!(format_cpu_limit(0.0), "disabled");
        assert_eq!(format_cpu_limit(0.111), "0.11");
        assert_eq!(format_cpu_limit(0.115), "0.12");
    }

    // Upstream: pkg/util/limiter/utilization_test.go::TestFormatMemory
    #[test]
    fn test_format_memory() {
        assert_eq!(format_memory(0), "0");
        assert_eq!(format_memory(GIGABYTE), "1073741824");
        assert_eq!(format_memory(GIGABYTE + 1), "1073741825");
    }

    // Upstream: pkg/util/limiter/utilization_test.go::TestFormatMemoryLimit
    #[test]
    fn test_format_memory_limit() {
        assert_eq!(format_memory_limit(0), "disabled");
        assert_eq!(format_memory_limit(GIGABYTE), "1073741824");
        assert_eq!(format_memory_limit(GIGABYTE + 1), "1073741825");
    }

    fn const_cpu(count: usize, value: f64) -> Vec<f64> {
        vec![value; count]
    }

    fn linear_cpu(count: usize, from: f64, step: f64) -> Vec<f64> {
        (0..count).map(|i| from + i as f64 * step).collect()
    }

    // Upstream: pkg/util/limiter/utilization_test.go::TestUtilizationBasedLimiter_CPUUtilizationSensitivity
    #[test]
    fn cpu_utilization_sensitivity() {
        let spike = [10.0, 9.0, 8.0, 7.0, 6.0, 5.0, 4.0, 3.0, 2.0, 1.0];
        let cases: Vec<(&str, Vec<f64>, f64)> = vec![
            ("2 minutes idle", const_cpu(120, 0.0), 0.0),
            ("2 minutes at constant utilization", const_cpu(120, 2.0), 2.0),
            (
                "1 minute idle + 10 seconds spike + 50 seconds idle",
                [const_cpu(60, 0.0), spike.to_vec(), const_cpu(50, 0.0)].concat(),
                1.49,
            ),
            (
                "10 seconds spike + 110 seconds idle",
                [spike.to_vec(), const_cpu(110, 0.0)].concat(),
                1.44,
            ),
            (
                "1 minute base utilization + 10 seconds spike + 50 seconds base utilization",
                [const_cpu(60, 1.0), spike.to_vec(), const_cpu(50, 1.0)].concat(),
                2.25,
            ),
            (
                "1 minute base utilization + 10 seconds steady spike + 50 seconds base utilization",
                [const_cpu(60, 1.0), const_cpu(10, 10.0), const_cpu(50, 1.0)].concat(),
                3.55,
            ),
            (
                "1 minute base utilization + 30 seconds steady spike + 30 seconds base utilization",
                [const_cpu(60, 1.0), const_cpu(30, 10.0), const_cpu(30, 1.0)].concat(),
                6.69,
            ),
            (
                "linear increase and then linear decrease utilization",
                [linear_cpu(60, 0.0, 0.1), linear_cpu(60, 60.0 * 0.1, -0.1)].concat(),
                4.13,
            ),
        ];
        for (name, values, expected_max) in cases {
            let lim = UtilizationBasedLimiter::new(1.0, 0, true, "", Some(&Registry::new()));
            let n = values.len();
            lim.set_scanner(Box::new(PreRecordedScanner::new(values)));
            let mut min = f64::MAX;
            let mut max = f64::MIN;
            let mut ts = Instant::now();
            for _ in 0..n {
                let t = ts;
                let (cpu, _) = lim.compute(|| t);
                ts += RESOURCE_UTILIZATION_UPDATE_INTERVAL;
                min = min.min(cpu);
                max = max.max(cpu);
            }
            assert!(min.abs() <= 0.01, "{name}: min={min}");
            assert!((max - expected_max).abs() <= 0.01, "{name}: max={max}");
        }
    }

    // Upstream: pkg/util/limiter/utilization_test.go::TestUtilizationBasedLimiter_ShouldNotUpdateCPUIfElapsedVeryShortTimeSincePreviousUpdate
    #[test]
    fn should_not_update_cpu_if_elapsed_very_short_time() {
        let mut now = Instant::now();
        let lim = UtilizationBasedLimiter::new(1.0, 0, true, "", None);
        lim.set_scanner(Box::new(PreRecordedScanner::new(const_cpu(10, 1.0))));
        let t = now;
        lim.compute(|| t);
        assert!((lim.last_cpu_time() - 1.0).abs() < 1e-4);
        for expected in 2..=5 {
            now += RESOURCE_UTILIZATION_UPDATE_INTERVAL;
            let t = now;
            lim.compute(|| t);
            assert!((lim.last_cpu_time() - expected as f64).abs() < 1e-4);
        }
        for _ in 0..5 {
            now += RESOURCE_UTILIZATION_UPDATE_INTERVAL / 10;
            let t = now;
            lim.compute(|| t);
            assert!((lim.last_cpu_time() - 5.0).abs() < 1e-4);
        }
        now += RESOURCE_UTILIZATION_UPDATE_INTERVAL / 10;
        let t = now;
        lim.compute(|| t);
        assert!((lim.last_cpu_time() - 10.0).abs() < 1e-4);
    }

    #[test]
    fn proc_parsing() {
        let stat = "1234 (my (weird) proc) S 1 1 1 0 -1 4194560 100 0 0 0 250 50 0 0 20 0 8 0 100 0 0";
        assert!((parse_proc_stat_cpu_time(stat).unwrap() - 3.0).abs() < 1e-9);
        let status = "Name:\tmimir\nVmRSS:\t  2048 kB\nRssAnon:\t    1024 kB\nRssFile:\t 1024 kB\n";
        assert_eq!(parse_proc_status_rss_anon(status).unwrap(), 1024 * 1024);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn proc_scanner_reads_self() {
        let mut s = ProcScanner::new().unwrap();
        let (cpu, mem) = s.scan().unwrap();
        assert!(cpu >= 0.0);
        assert!(mem > 0);
    }
}
