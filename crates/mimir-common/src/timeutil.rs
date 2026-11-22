//! Ported from github.com/grafana/dskit/timeutil/variable_ticker.go.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, Notify};

/// Idempotent stop handle for a variable ticker.
/// Holding a clone of the sender ensures the receiver channel is NOT closed on `stop()`,
/// matching Go's `time.Ticker.Stop()` semantics.
#[derive(Clone)]
pub struct VariableTickerStop {
    stopped: Arc<AtomicBool>,
    notify: Arc<Notify>,
    _keep_open: mpsc::Sender<Instant>,
}

impl VariableTickerStop {
    pub fn stop(&self) {
        if !self.stopped.swap(true, Ordering::AcqRel) {
            self.notify.notify_waiters();
        }
    }
}

/// Wraps a ticker that changes its period after each tick according to `durations`,
/// preserving the last configured duration once earlier ones have fired.
///
/// Panics if `durations` is empty.
pub fn new_variable_ticker(durations: &[Duration]) -> (VariableTickerStop, mpsc::Receiver<Instant>) {
    assert!(!durations.is_empty(), "at least 1 duration required");

    let (tx, rx) = mpsc::channel(1);
    let stopped = Arc::new(AtomicBool::new(false));
    let notify = Arc::new(Notify::new());

    let stop_handle = VariableTickerStop {
        stopped: Arc::clone(&stopped),
        notify: Arc::clone(&notify),
        _keep_open: tx.clone(),
    };

    let durs = durations.to_vec();
    tokio::spawn(async move {
        let mut idx = 0usize;
        let mut current = durs[0];
        let mut next_tick = tokio::time::Instant::now() + current;

        loop {
            if stopped.load(Ordering::Acquire) {
                return;
            }
            tokio::select! {
                biased;
                _ = notify.notified() => {
                    return;
                }
                _ = tokio::time::sleep_until(next_tick) => {
                    if stopped.load(Ordering::Acquire) {
                        return;
                    }
                    let ts = Instant::now();
                    if idx + 1 < durs.len() {
                        idx += 1;
                        current = durs[idx];
                    }
                    next_tick += current;
                    // Non-blocking send matching Go's select { case ticks <- ts: case <-stopped: return }
                    let _ = tx.try_send(ts);
                }
            }
        }
    });

    (stop_handle, rx)
}
