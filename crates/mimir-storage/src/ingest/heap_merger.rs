//! Best-effort timestamp-ordered min-heap merger across multiple Kafka write compartments.
//!
//! Ported from github.com/grafana/mimir/pkg/storage/ingest/heap_merger.go

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::time::{Duration, Instant};

use mimir_config::OrderedConsumptionConfig;
use prometheus::{Counter, Histogram, HistogramOpts, Opts, Registry};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::warn;

use super::kafka_client::FetchedRecord;

/// A record submitted from a per-cluster consumer to the `HeapMerger`, ordered by
/// `(timestamp_ms ASC, kafka_cluster_id ASC, offset ASC)`.
#[derive(Debug, Clone)]
pub struct MergeableRecord {
    pub record: FetchedRecord,
    pub kafka_cluster_id: usize,
}

impl PartialEq for MergeableRecord {
    fn eq(&self, other: &Self) -> bool {
        self.record.timestamp_ms == other.record.timestamp_ms
            && self.kafka_cluster_id == other.kafka_cluster_id
            && self.record.offset == other.record.offset
    }
}

impl Eq for MergeableRecord {}

impl Ord for MergeableRecord {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse order for BinaryHeap (Rust BinaryHeap is a max-heap, we want min-heap!).
        other
            .record
            .timestamp_ms
            .cmp(&self.record.timestamp_ms)
            .then_with(|| other.kafka_cluster_id.cmp(&self.kafka_cluster_id))
            .then_with(|| other.record.offset.cmp(&self.record.offset))
    }
}

impl PartialOrd for MergeableRecord {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

struct HeapItem {
    entry: MergeableRecord,
    ack: oneshot::Sender<Result<(), String>>,
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.entry == other.entry
    }
}
impl Eq for HeapItem {}
impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        self.entry.cmp(&other.entry)
    }
}
impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone)]
pub struct HeapMergerMetrics {
    pub pushed_records: Counter,
    pub out_of_order_emits: Counter,
    pub batch_flush_latency: Histogram,
}

impl HeapMergerMetrics {
    pub fn new(registry: Option<&Registry>) -> Self {
        let pushed_records = Counter::with_opts(Opts::new(
            "cortex_ingest_storage_ordered_consumption_records_total",
            "Total number of records pushed by ordered consumption. The baseline against which the out-of-order records fraction can be computed.",
        ))
        .expect("valid metric");
        let out_of_order_emits = Counter::with_opts(Opts::new(
            "cortex_ingest_storage_ordered_consumption_out_of_order_records_total",
            "Total number of records pushed with a timestamp older than the most recently pushed record. This is the residual cross-cluster out-of-order that ordered consumption could not absorb.",
        ))
        .expect("valid metric");
        let batch_flush_latency = Histogram::with_opts(HistogramOpts::new(
            "cortex_ingest_storage_ordered_consumption_batch_flush_latency_seconds",
            "Elapsed time from the first record entering a batch to the batch finishing its downstream push.",
        ))
        .expect("valid metric");

        if let Some(reg) = registry {
            let _ = reg.register(Box::new(pushed_records.clone()));
            let _ = reg.register(Box::new(out_of_order_emits.clone()));
            let _ = reg.register(Box::new(batch_flush_latency.clone()));
        }

        Self {
            pushed_records,
            out_of_order_emits,
            batch_flush_latency,
        }
    }
}

/// Asynchronous batch consumer callback invoked by `HeapMerger` on merged, timestamp-sorted batches.
pub type MergedBatchConsumer = ArcConsumer;
pub type ArcConsumer = std::sync::Arc<
    dyn Fn(
            Vec<FetchedRecord>,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send>>
        + Send
        + Sync,
>;

/// Synchronous min-heap sorter helper for merging pre-collected multi-cluster record slices
/// in `(timestamp_ms ASC, kafka_cluster_id ASC, offset ASC)` order.
pub fn merge_records_in_timestamp_order(
    batches: impl IntoIterator<Item = (usize, Vec<FetchedRecord>)>,
) -> Vec<MergeableRecord> {
    let mut heap = BinaryHeap::new();
    for (cluster_id, recs) in batches {
        for record in recs {
            heap.push(MergeableRecord {
                record,
                kafka_cluster_id: cluster_id,
            });
        }
    }
    let mut out = Vec::with_capacity(heap.len());
    while let Some(item) = heap.pop() {
        out.push(item);
    }
    out
}

/// Async `HeapMerger` service receiving records from multiple cluster readers, ordering them in a
/// min-heap by `(timestamp_ms, kafka_cluster_id, offset)`, and flushing batches downstream.
struct HeapMergerPending {
    input_rx: mpsc::Receiver<HeapItem>,
    stop_rx: tokio::sync::watch::Receiver<bool>,
    consumer: MergedBatchConsumer,
    metrics: HeapMergerMetrics,
    max_batch_records: usize,
    max_batch_wait: Duration,
}

pub struct HeapMerger {
    input_tx: mpsc::Sender<HeapItem>,
    stop_tx: tokio::sync::watch::Sender<bool>,
    pending: parking_lot::Mutex<Option<HeapMergerPending>>,
    handle: tokio::sync::Mutex<Option<JoinHandle<()>>>,
}

impl HeapMerger {
    /// Constructs a `HeapMerger` without spawning a background task yet.
    /// Call [`HeapMerger::ensure_started`] inside an active Tokio runtime to start processing.
    pub fn new(
        cfg: &OrderedConsumptionConfig,
        consumer: MergedBatchConsumer,
        metrics: HeapMergerMetrics,
    ) -> Self {
        let max_batch_records = (cfg.max_batch_records.max(1)) as usize;
        let max_batch_wait = Duration::from_nanos(cfg.max_batch_wait.as_nanos().max(0) as u64)
            .max(Duration::from_millis(1));
        let buf_size = if cfg.input_buffer_size <= 0 {
            max_batch_records * 2
        } else {
            cfg.input_buffer_size as usize
        };

        let (input_tx, input_rx) = mpsc::channel::<HeapItem>(buf_size);
        let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);

        Self {
            input_tx,
            stop_tx,
            pending: parking_lot::Mutex::new(Some(HeapMergerPending {
                input_rx,
                stop_rx,
                consumer,
                metrics,
                max_batch_records,
                max_batch_wait,
            })),
            handle: tokio::sync::Mutex::new(None),
        }
    }

    /// Spawns the background merger task if it has not been started yet.
    pub fn ensure_started(&self) {
        let Some(HeapMergerPending {
            mut input_rx,
            mut stop_rx,
            consumer,
            metrics,
            max_batch_records,
            max_batch_wait,
        }) = self.pending.lock().take()
        else {
            return;
        };

        let handle = tokio::spawn(async move {
            let mut heap: BinaryHeap<HeapItem> = BinaryHeap::with_capacity(max_batch_records);
            let mut batch_start: Option<Instant> = None;
            let mut last_emitted_ts: Option<i64> = None;
            let sleep = tokio::time::sleep(Duration::from_secs(3600));
            tokio::pin!(sleep);
            let mut timer_active = false;

            loop {
                tokio::select! {
                    biased;
                    res = stop_rx.changed() => {
                        if res.is_err() || *stop_rx.borrow() {
                            Self::drain_heap(&mut heap, &mut input_rx, "heap merger stopped");
                            break;
                        }
                    }
                    maybe_item = input_rx.recv() => {
                        match maybe_item {
                            Some(item) => {
                                if heap.is_empty() {
                                    batch_start = Some(Instant::now());
                                }
                                heap.push(item);
                                if !timer_active {
                                    sleep.as_mut().reset(tokio::time::Instant::now() + max_batch_wait);
                                    timer_active = true;
                                }
                                if heap.len() >= max_batch_records {
                                    timer_active = false;
                                    Self::flush_heap(
                                        &mut heap,
                                        &consumer,
                                        &metrics,
                                        &mut batch_start,
                                        &mut last_emitted_ts,
                                    ).await;
                                }
                            }
                            None => {
                                Self::flush_heap(
                                    &mut heap,
                                    &consumer,
                                    &metrics,
                                    &mut batch_start,
                                    &mut last_emitted_ts,
                                ).await;
                                break;
                            }
                        }
                    }
                    _ = &mut sleep, if timer_active => {
                        timer_active = false;
                        Self::flush_heap(
                            &mut heap,
                            &consumer,
                            &metrics,
                            &mut batch_start,
                            &mut last_emitted_ts,
                        ).await;
                    }
                }
            }
        });

        if let Ok(mut guard) = self.handle.try_lock() {
            *guard = Some(handle);
        }
    }

    pub fn start(
        cfg: &OrderedConsumptionConfig,
        consumer: MergedBatchConsumer,
        metrics: HeapMergerMetrics,
    ) -> Self {
        let m = Self::new(cfg, consumer, metrics);
        m.ensure_started();
        m
    }

    fn drain_heap(
        heap: &mut BinaryHeap<HeapItem>,
        input_rx: &mut mpsc::Receiver<HeapItem>,
        err_msg: &str,
    ) {
        while let Some(item) = heap.pop() {
            let _ = item.ack.send(Err(err_msg.to_string()));
        }
        while let Ok(item) = input_rx.try_recv() {
            let _ = item.ack.send(Err(err_msg.to_string()));
        }
    }

    async fn flush_heap(
        heap: &mut BinaryHeap<HeapItem>,
        consumer: &MergedBatchConsumer,
        metrics: &HeapMergerMetrics,
        batch_start: &mut Option<Instant>,
        last_emitted_ts: &mut Option<i64>,
    ) {
        if heap.is_empty() {
            return;
        }
        let mut acks = Vec::with_capacity(heap.len());
        let mut records = Vec::with_capacity(heap.len());
        while let Some(item) = heap.pop() {
            records.push(item.entry.record);
            acks.push(item.ack);
        }

        let timestamps: Vec<i64> = records.iter().map(|r| r.timestamp_ms).collect();
        let res = (consumer)(records).await;
        match &res {
            Ok(()) => {
                let mut ooo = 0u64;
                for ts in timestamps {
                    if let Some(prev) = *last_emitted_ts {
                        if ts < prev {
                            ooo += 1;
                        }
                    }
                    if last_emitted_ts.is_none_or(|prev| ts > prev) {
                        *last_emitted_ts = Some(ts);
                    }
                }
                metrics.pushed_records.inc_by(acks.len() as f64);
                metrics.out_of_order_emits.inc_by(ooo as f64);
            }
            Err(e) => {
                warn!(err = %e, records = acks.len(), "downstream consumer returned error from merged batch");
            }
        }
        if let Some(start) = batch_start.take() {
            metrics.batch_flush_latency.observe(start.elapsed().as_secs_f64());
        }
        for ack in acks {
            let _ = ack.send(res.clone());
        }
    }

    /// Submits a batch of records from `kafka_cluster_id` and awaits downstream forwarding.
    pub async fn submit_and_wait(
        &self,
        kafka_cluster_id: usize,
        records: Vec<FetchedRecord>,
    ) -> Result<(), String> {
        if records.is_empty() {
            return Ok(());
        }
        let mut receivers = Vec::with_capacity(records.len());
        for record in records {
            let (tx, rx) = oneshot::channel();
            let item = HeapItem {
                entry: MergeableRecord {
                    record,
                    kafka_cluster_id,
                },
                ack: tx,
            };
            self.input_tx
                .send(item)
                .await
                .map_err(|_| "heap merger stopped".to_string())?;
            receivers.push(rx);
        }
        let mut first_err = None;
        for rx in receivers {
            match rx.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                }
                Err(_) => {
                    if first_err.is_none() {
                        first_err = Some("heap merger dropped ack".to_string());
                    }
                }
            }
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Signals shutdown without awaiting completion (safe to call from sync cleanup paths).
    pub fn stop_async(&self) {
        let _ = self.stop_tx.send(true);
    }

    /// Immediately stops the merger and drains any pending records in the heap with an error
    /// without forwarding them downstream (matching `HeapMerger.drain` in Go).
    pub async fn shutdown_and_drain(&self) {
        let _ = self.stop_tx.send(true);
        if let Some(mut pending) = self.pending.lock().take() {
            let mut heap = BinaryHeap::new();
            Self::drain_heap(&mut heap, &mut pending.input_rx, "heap merger stopped");
        }
        if let Some(handle) = self.handle.lock().await.take() {
            let _ = handle.await;
        }
    }

    pub async fn stop(self) {
        drop(self.input_tx);
        drop(self.pending.lock().take());
        if let Some(handle) = self.handle.lock().await.take() {
            let _ = handle.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::sync::Arc;

    fn mk_rec(offset: i64, ts: i64) -> FetchedRecord {
        FetchedRecord {
            offset,
            timestamp_ms: ts,
            key: Bytes::from_static(b"t1"),
            value: Bytes::new(),
            headers: Vec::new(),
        }
    }

    #[test]
    fn test_merge_records_in_timestamp_order() {
        let b0 = vec![mk_rec(0, 300), mk_rec(1, 100)];
        let b1 = vec![mk_rec(0, 200), mk_rec(1, 100)];
        let merged = merge_records_in_timestamp_order([(0, b0), (1, b1)]);
        let keys: Vec<(i64, usize, i64)> = merged
            .iter()
            .map(|m| (m.record.timestamp_ms, m.kafka_cluster_id, m.record.offset))
            .collect();
        assert_eq!(keys, vec![(100, 0, 1), (100, 1, 1), (200, 1, 0), (300, 0, 0)]);
    }

    #[tokio::test]
    async fn test_async_heap_merger() {
        let collected = Arc::new(parking_lot::Mutex::new(Vec::<i64>::new()));
        let collected_c = Arc::clone(&collected);
        let consumer: MergedBatchConsumer = Arc::new(move |recs| {
            let c = Arc::clone(&collected_c);
            Box::pin(async move {
                for r in recs {
                    c.lock().push(r.timestamp_ms);
                }
                Ok(())
            })
        });

        let cfg = OrderedConsumptionConfig {
            enabled: true,
            max_batch_records: 4,
            max_batch_wait: mimir_config::GoDuration::from_millis(20),
            ..Default::default()
        };

        let merger = HeapMerger::start(&cfg, consumer, HeapMergerMetrics::new(None));
        let (r0, r1) = tokio::join!(
            merger.submit_and_wait(0, vec![mk_rec(0, 50), mk_rec(1, 20)]),
            merger.submit_and_wait(1, vec![mk_rec(0, 10), mk_rec(1, 30)]),
        );
        r0.unwrap();
        r1.unwrap();
        merger.stop().await;

        assert_eq!(*collected.lock(), vec![10, 20, 30, 50]);
    }
}
