//! Disableable ticker wrapper.
//!
//! Ported from github.com/grafana/dskit/ring/ticker.go

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, Notify};

/// Stop handle returned by [`new_disableable_ticker`].
pub struct TickerStop {
    notify: Option<Arc<Notify>>,
}

impl TickerStop {
    /// Stops the ticker task and releases its channel.
    pub fn stop(&self) {
        if let Some(n) = &self.notify {
            n.notify_waiters();
        }
    }
}

/// Wraps an interval ticker, allowing it to be disabled by passing `Duration::ZERO`.
/// Returns a stop handle and an optional receiver channel (`None` when disabled).
pub fn new_disableable_ticker(
    interval: Duration,
) -> (TickerStop, Option<mpsc::Receiver<tokio::time::Instant>>) {
    if interval.is_zero() {
        return (TickerStop { notify: None }, None);
    }

    let (tx, rx) = mpsc::channel(1);
    let stop_notify = Arc::new(Notify::new());
    let stop_clone = Arc::clone(&stop_notify);

    tokio::spawn(async move {
        let start = tokio::time::Instant::now() + interval;
        let mut ticker = tokio::time::interval_at(start, interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = stop_clone.notified() => break,
                ts = ticker.tick() => {
                    // Match Go's 1-element buffered ticker channel: drop if receiver is full,
                    // exit if receiver was dropped.
                    if let Err(mpsc::error::TrySendError::Closed(_)) = tx.try_send(ts) {
                        break;
                    }
                }
            }
        }
    });

    (
        TickerStop {
            notify: Some(stop_notify),
        },
        Some(rx),
    )
}
