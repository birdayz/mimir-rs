//! Tracks the last consumed Kafka offset and notifies waiters for strong read consistency.
//!
//! Ported from github.com/grafana/mimir/pkg/storage/ingest/partition_offset_watcher.go

use std::time::Duration;

use thiserror::Error;
use tokio::sync::watch;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum OffsetWatcherError {
    #[error("partition offset watcher is stopped")]
    Stopped,
    #[error("wait strong read consistency timeout exceeded after {0:?}")]
    Timeout(Duration),
}

#[derive(Debug, Clone, Copy)]
struct WatcherState {
    last_consumed_offset: i64,
    stopped: bool,
}

/// Thread-safe offset progress tracker supporting monotonic `notify` and `wait_for_offset`.
/// Initialized with `last_consumed_offset = -1` (nothing consumed yet; `0` is a valid offset).
pub struct PartitionOffsetWatcher {
    tx: watch::Sender<WatcherState>,
    rx: watch::Receiver<WatcherState>,
}

impl Default for PartitionOffsetWatcher {
    fn default() -> Self {
        Self::new()
    }
}

impl PartitionOffsetWatcher {
    pub fn new() -> Self {
        let (tx, rx) = watch::channel(WatcherState {
            last_consumed_offset: -1,
            stopped: false,
        });
        Self { tx, rx }
    }

    /// Stops the watcher and wakes all waiting tasks with `OffsetWatcherError::Stopped`.
    pub fn stop(&self) {
        self.tx.send_modify(|s| {
            s.stopped = true;
        });
    }

    /// Monotonically updates `last_consumed_offset` and wakes any waiters whose target offset
    /// is `<= last_consumed_offset`.
    pub fn notify(&self, last_consumed_offset: i64) {
        self.tx.send_if_modified(|s| {
            if s.stopped || last_consumed_offset <= s.last_consumed_offset {
                return false;
            }
            s.last_consumed_offset = last_consumed_offset;
            true
        });
    }

    /// Returns the last consumed offset (`-1` if nothing consumed yet).
    pub fn last_consumed_offset(&self) -> i64 {
        self.rx.borrow().last_consumed_offset
    }

    /// Waits until `target_offset <= last_consumed_offset` or `timeout` elapses.
    /// Negative `target_offset` returns `Ok(())` immediately (empty partition).
    pub async fn wait_for_offset(
        &self,
        target_offset: i64,
        timeout: Duration,
    ) -> Result<(), OffsetWatcherError> {
        if target_offset < 0 {
            return Ok(());
        }

        let mut rx = self.rx.clone();
        {
            let state = *rx.borrow_and_update();
            if state.stopped {
                return Err(OffsetWatcherError::Stopped);
            }
            if target_offset <= state.last_consumed_offset {
                return Ok(());
            }
        }

        let wait_fut = async {
            loop {
                if rx.changed().await.is_err() {
                    return Err(OffsetWatcherError::Stopped);
                }
                let state = *rx.borrow_and_update();
                if state.stopped {
                    return Err(OffsetWatcherError::Stopped);
                }
                if target_offset <= state.last_consumed_offset {
                    return Ok(());
                }
            }
        };

        if timeout.is_zero() {
            wait_fut.await
        } else {
            tokio::time::timeout(timeout, wait_fut)
                .await
                .map_err(|_| OffsetWatcherError::Timeout(timeout))?
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_watcher_notify_and_wait() {
        let w = Arc::new(PartitionOffsetWatcher::new());
        assert_eq!(w.last_consumed_offset(), -1);

        // Negative offset returns immediately.
        w.wait_for_offset(-1, Duration::from_millis(50)).await.unwrap();

        // Waiting on offset 5 times out if not notified.
        let err = w.wait_for_offset(5, Duration::from_millis(30)).await.unwrap_err();
        assert_eq!(err, OffsetWatcherError::Timeout(Duration::from_millis(30)));

        let w2 = Arc::clone(&w);
        let handle = tokio::spawn(async move { w2.wait_for_offset(10, Duration::from_secs(2)).await });

        tokio::time::sleep(Duration::from_millis(10)).await;
        w.notify(5);
        // Not yet 10; monotonic check ignores lower value.
        w.notify(3);
        assert_eq!(w.last_consumed_offset(), 5);

        w.notify(10);
        handle.await.unwrap().unwrap();

        // Already satisfied returns immediately.
        w.wait_for_offset(8, Duration::from_millis(1)).await.unwrap();

        // Stop wakes waiters.
        let w3 = Arc::clone(&w);
        let h2 = tokio::spawn(async move { w3.wait_for_offset(99, Duration::from_secs(5)).await });
        tokio::time::sleep(Duration::from_millis(10)).await;
        w.stop();
        assert_eq!(h2.await.unwrap().unwrap_err(), OffsetWatcherError::Stopped);
    }
}
