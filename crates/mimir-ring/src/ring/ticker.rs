//! Disableable ticker wrapper.
//!
//! Ported from github.com/grafana/dskit/ring/ticker.go

use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

/// Stop handle returned by [`new_disableable_ticker`].
pub struct TickerStop {
    stop_tx: Option<watch::Sender<bool>>,
    task: Option<JoinHandle<()>>,
}

impl TickerStop {
    /// Stops the ticker task and releases its channel.
    pub fn stop(&self) {
        if let Some(tx) = &self.stop_tx {
            let _ = tx.send(true);
        }
    }
}

impl Drop for TickerStop {
    fn drop(&mut self) {
        self.stop();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

/// Wraps an interval ticker, allowing it to be disabled by passing `Duration::ZERO`.
/// Returns a stop handle and an optional receiver channel (`None` when disabled).
pub fn new_disableable_ticker(
    interval: Duration,
) -> (TickerStop, Option<mpsc::Receiver<tokio::time::Instant>>) {
    if interval.is_zero() {
        return (
            TickerStop {
                stop_tx: None,
                task: None,
            },
            None,
        );
    }

    let (tx, rx) = mpsc::channel(1);
    let (stop_tx, mut stop_rx) = watch::channel(false);

    let task = tokio::spawn(async move {
        if *stop_rx.borrow_and_update() {
            return;
        }
        let start = tokio::time::Instant::now() + interval;
        let mut ticker = tokio::time::interval_at(start, interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                biased;
                res = stop_rx.changed() => {
                    if res.is_err() || *stop_rx.borrow() {
                        break;
                    }
                }
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
            stop_tx: Some(stop_tx),
            task: Some(task),
        },
        Some(rx),
    )
}
