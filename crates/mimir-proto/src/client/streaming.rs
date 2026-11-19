//! Ported from github.com/grafana/mimir/pkg/ingester/client/streaming.go,
//! streaming_test_utils.go, and pkg/util/limiter/{query_limiter.go,memory_consumption.go,errors.go}.

use std::collections::HashSet;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use prometheus::Counter;
use prost::Message as _;
use tokio::sync::{mpsc, oneshot, watch};

use crate::compat::PromLabels;
use crate::cortexpb;
use crate::ingesterpb;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LimitError(pub String);

impl std::fmt::Display for LimitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for LimitError {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StreamError {
    Canceled(String),
    Limit(LimitError),
    Other(String),
}

impl StreamError {
    pub fn is_canceled(&self) -> bool {
        matches!(self, StreamError::Canceled(_))
    }

    pub fn is_limit(&self) -> bool {
        matches!(self, StreamError::Limit(_))
    }
}

impl std::fmt::Display for StreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamError::Canceled(m) | StreamError::Other(m) => f.write_str(m),
            StreamError::Limit(l) => f.write_str(&l.0),
        }
    }
}

impl std::error::Error for StreamError {}

const CARDINALITY_STRATEGY: &str = "Consider reducing the time range and/or number of series selected by the query. One way to reduce the number of selected series is to add more label matchers to the query";

/// Ported from `limiter.QueryLimiter` in `pkg/util/limiter/query_limiter.go`.
pub struct QueryLimiter {
    unique_series: Mutex<HashSet<u64>>,
    chunk_bytes_count: AtomicI64,
    chunk_count: AtomicI64,
    estimated_chunk_count: AtomicI64,
    max_series_per_query: usize,
    max_chunk_bytes_per_query: usize,
    max_chunks_per_query: usize,
    max_estimated_chunks_per_query: usize,
}

impl QueryLimiter {
    pub fn new(
        max_series_per_query: usize,
        max_chunk_bytes_per_query: usize,
        max_chunks_per_query: usize,
        max_estimated_chunks_per_query: usize,
    ) -> Self {
        Self {
            unique_series: Mutex::new(HashSet::new()),
            chunk_bytes_count: AtomicI64::new(0),
            chunk_count: AtomicI64::new(0),
            estimated_chunk_count: AtomicI64::new(0),
            max_series_per_query,
            max_chunk_bytes_per_query,
            max_chunks_per_query,
            max_estimated_chunks_per_query,
        }
    }

    pub fn add_series_hash(&self, fingerprint: u64) -> Result<(), LimitError> {
        if self.max_series_per_query == 0 {
            return Ok(());
        }
        let mut set = self.unique_series.lock();
        set.insert(fingerprint);
        if set.len() > self.max_series_per_query {
            return Err(LimitError(format!(
                "the query exceeded the maximum number of series (limit: {} series) (err-mimir-max-series-per-query). {CARDINALITY_STRATEGY}. Otherwise, to adjust the related per-tenant limit, configure -querier.max-fetched-series-per-query, or contact your service administrator.",
                self.max_series_per_query
            )));
        }
        Ok(())
    }

    pub fn add_chunk_bytes(&self, chunk_size_in_bytes: usize) -> Result<(), LimitError> {
        if self.max_chunk_bytes_per_query == 0 {
            return Ok(());
        }
        let total_bytes = self
            .chunk_bytes_count
            .fetch_add(chunk_size_in_bytes as i64, Ordering::SeqCst)
            + chunk_size_in_bytes as i64;
        if total_bytes > self.max_chunk_bytes_per_query as i64 {
            return Err(LimitError(format!(
                "the query exceeded the aggregated chunks size limit (limit: {} bytes) (err-mimir-max-chunks-bytes-per-query). {CARDINALITY_STRATEGY}. Otherwise, to adjust the related per-tenant limit, configure -querier.max-fetched-chunk-bytes-per-query, or contact your service administrator.",
                self.max_chunk_bytes_per_query
            )));
        }
        Ok(())
    }

    pub fn add_chunks(&self, count: usize) -> Result<(), LimitError> {
        if self.max_chunks_per_query == 0 {
            return Ok(());
        }
        let total = self
            .chunk_count
            .fetch_add(count as i64, Ordering::SeqCst)
            + count as i64;
        if total > self.max_chunks_per_query as i64 {
            return Err(LimitError(format!(
                "the query exceeded the maximum number of chunks (limit: {} chunks) (err-mimir-max-chunks-per-query). {CARDINALITY_STRATEGY}. Otherwise, to adjust the related per-tenant limit, configure -querier.max-fetched-chunks-per-query, or contact your service administrator.",
                self.max_chunks_per_query
            )));
        }
        Ok(())
    }

    pub fn add_estimated_chunks(&self, count: usize) -> Result<(), LimitError> {
        if self.max_estimated_chunks_per_query == 0 {
            return Ok(());
        }
        let total = self
            .estimated_chunk_count
            .fetch_add(count as i64, Ordering::SeqCst)
            + count as i64;
        if total > self.max_estimated_chunks_per_query as i64 {
            return Err(LimitError(format!(
                "the estimated number of chunks for the query exceeded the maximum allowed (limit: {} chunks) (err-mimir-max-estimated-chunks-per-query). {CARDINALITY_STRATEGY}. Otherwise, to adjust the related per-tenant limit, configure -querier.max-estimated-chunks-per-query-multiplier, or contact your service administrator.",
                self.max_estimated_chunks_per_query
            )));
        }
        Ok(())
    }
}

/// Ported from `limiter.MemoryConsumptionTracker` in `pkg/util/limiter/memory_consumption.go`.
pub struct MemoryConsumptionTracker {
    max_estimated_memory_bytes: u64,
    current_bytes: AtomicU64,
    rejections: Option<Counter>,
}

impl MemoryConsumptionTracker {
    pub fn new(max_estimated_memory_bytes: u64, rejections: Option<Counter>) -> Self {
        Self {
            max_estimated_memory_bytes,
            current_bytes: AtomicU64::new(0),
            rejections,
        }
    }

    pub fn unlimited() -> Self {
        Self::new(0, None)
    }

    pub fn increase_memory_consumption(&self, bytes: u64) -> Result<(), LimitError> {
        let new_total = self.current_bytes.fetch_add(bytes, Ordering::SeqCst) + bytes;
        if self.max_estimated_memory_bytes > 0 && new_total > self.max_estimated_memory_bytes {
            self.current_bytes.fetch_sub(bytes, Ordering::SeqCst);
            if let Some(ref counter) = self.rejections {
                counter.inc();
            }
            return Err(LimitError(format!(
                "the query exceeded the maximum allowed estimated amount of memory consumed by a single query (limit: {} bytes) (err-mimir-max-estimated-memory-consumption-per-query). {CARDINALITY_STRATEGY}. Otherwise, to adjust the related per-tenant limit, configure -querier.max-estimated-memory-consumption-per-query, or contact your service administrator.",
                self.max_estimated_memory_bytes
            )));
        }
        Ok(())
    }

    pub fn decrease_memory_consumption(&self, bytes: u64) {
        self.current_bytes.fetch_sub(bytes, Ordering::SeqCst);
    }

    pub fn current_estimated_memory_consumption_bytes(&self) -> u64 {
        self.current_bytes.load(Ordering::SeqCst)
    }
}

#[tonic::async_trait]
pub trait QueryStreamClient: Send + 'static {
    async fn recv(&mut self) -> Result<Option<ingesterpb::QueryStreamResponse>, StreamError>;
    fn close_send(&mut self);
}

#[tonic::async_trait]
impl QueryStreamClient for tonic::codec::Streaming<ingesterpb::QueryStreamResponse> {
    async fn recv(&mut self) -> Result<Option<ingesterpb::QueryStreamResponse>, StreamError> {
        match self.message().await {
            Ok(Some(msg)) => Ok(Some(msg)),
            Ok(None) => Ok(None),
            Err(status) => {
                if status.code() == tonic::Code::Cancelled {
                    Err(StreamError::Canceled(status.message().to_string()))
                } else {
                    Err(StreamError::Other(status.to_string()))
                }
            }
        }
    }

    fn close_send(&mut self) {}
}

pub struct StreamingSeries {
    pub labels: PromLabels,
    pub sources: Vec<StreamingSeriesSource>,
}

pub struct StreamingSeriesSource {
    pub stream_reader: SeriesChunksStreamReader,
    pub series_index: u64,
}

/// Ported from `SeriesChunksStreamReader` in `pkg/ingester/client/streaming.go`.
pub struct SeriesChunksStreamReader {
    parent_cancel: watch::Receiver<bool>,
    client: Option<Box<dyn QueryStreamClient>>,
    expected_series_count: usize,
    query_limiter: Arc<QueryLimiter>,
    memory_tracker: Arc<MemoryConsumptionTracker>,
    cleanup: Option<Arc<dyn Fn() + Send + Sync>>,
    ingester_name: String,

    series_message_rx: Option<mpsc::Receiver<ingesterpb::QueryStreamResponse>>,
    error_rx: Option<oneshot::Receiver<Option<StreamError>>>,
    err: Option<StreamError>,
    last_message: Option<ingesterpb::QueryStreamResponse>,
    series_batch: Vec<ingesterpb::QueryStreamSeriesChunks>,
    series_batch_offset: usize,
}

impl SeriesChunksStreamReader {
    pub fn new(
        parent_cancel: watch::Receiver<bool>,
        client: Box<dyn QueryStreamClient>,
        ingester_name: impl Into<String>,
        expected_series_count: usize,
        query_limiter: Arc<QueryLimiter>,
        memory_tracker: Arc<MemoryConsumptionTracker>,
        cleanup: Arc<dyn Fn() + Send + Sync>,
    ) -> Self {
        Self {
            parent_cancel,
            client: Some(client),
            expected_series_count,
            query_limiter,
            memory_tracker,
            cleanup: Some(cleanup),
            ingester_name: ingester_name.into(),
            series_message_rx: None,
            error_rx: None,
            err: None,
            last_message: None,
            series_batch: Vec::new(),
            series_batch_offset: 0,
        }
    }

    pub fn get_name(&self) -> &str {
        &self.ingester_name
    }

    pub fn close(&mut self) {
        if let Some(mut client) = self.client.take() {
            client.close_send();
        }
        if let Some(cleanup) = self.cleanup.take() {
            cleanup();
        }
    }

    pub fn free_buffer(&mut self) {
        if let Some(msg) = self.last_message.take() {
            self.memory_tracker
                .decrease_memory_consumption(msg.encoded_len() as u64);
        }
    }

    fn set_last_message(&mut self, msg: ingesterpb::QueryStreamResponse) -> Result<(), StreamError> {
        if self.last_message.is_some() {
            return Err(StreamError::Other(
                "must call FreeBuffer() before storing the next message - this indicates a bug"
                    .to_string(),
            ));
        }
        self.memory_tracker
            .increase_memory_consumption(msg.encoded_len() as u64)
            .map_err(StreamError::Limit)?;
        self.last_message = Some(msg);
        Ok(())
    }

    pub fn start_buffering(&mut self) {
        let (msg_tx, msg_rx) = mpsc::channel(1);
        let (err_tx, err_rx) = oneshot::channel();
        self.series_message_rx = Some(msg_rx);
        self.error_rx = Some(err_rx);

        let mut client = self
            .client
            .take()
            .expect("start_buffering called without client");
        let mut parent_cancel = self.parent_cancel.clone();
        let expected_series_count = self.expected_series_count;
        let query_limiter = Arc::clone(&self.query_limiter);
        let cleanup = self.cleanup.take();

        tokio::spawn(async move {
            let res = Self::read_stream(
                &mut *client,
                &mut parent_cancel,
                expected_series_count,
                &query_limiter,
                msg_tx,
            )
            .await;

            client.close_send();
            if let Some(cb) = cleanup {
                cb();
            }

            let _ = err_tx.send(res.err());
        });
    }

    async fn read_stream(
        client: &mut dyn QueryStreamClient,
        parent_cancel: &mut watch::Receiver<bool>,
        expected_series_count: usize,
        query_limiter: &QueryLimiter,
        msg_tx: mpsc::Sender<ingesterpb::QueryStreamResponse>,
    ) -> Result<(), StreamError> {
        let mut total_series = 0usize;

        loop {
            match client.recv().await {
                Ok(None) => {
                    if total_series < expected_series_count {
                        return Err(StreamError::Other(format!(
                            "expected to receive {expected_series_count} series, but got EOF after receiving {total_series} series"
                        )));
                    }
                    return Ok(());
                }
                Err(err) => {
                    if matches!(err, StreamError::Canceled(_)) && *parent_cancel.borrow() {
                        return Err(StreamError::Canceled(
                            "aborted stream because query was cancelled: context canceled"
                                .to_string(),
                        ));
                    }
                    return Err(err);
                }
                Ok(Some(msg)) => {
                    if msg.streaming_series_chunks.is_empty() {
                        continue;
                    }

                    total_series += msg.streaming_series_chunks.len();
                    if total_series > expected_series_count {
                        return Err(StreamError::Other(format!(
                            "expected to receive only {expected_series_count} series, but received at least {total_series} series"
                        )));
                    }

                    let mut chunk_bytes = 0usize;
                    for s in &msg.streaming_series_chunks {
                        for c in &s.chunks {
                            chunk_bytes += c.encoded_len();
                        }
                    }

                    query_limiter
                        .add_chunk_bytes(chunk_bytes)
                        .map_err(StreamError::Limit)?;

                    if *parent_cancel.borrow() {
                        return Err(StreamError::Canceled(
                            "aborted stream because query was cancelled: context canceled"
                                .to_string(),
                        ));
                    }

                    tokio::select! {
                        biased;
                        _ = parent_cancel.changed(), if *parent_cancel.borrow() => {
                            return Err(StreamError::Canceled(
                                "aborted stream because query was cancelled: context canceled".to_string(),
                            ));
                        }
                        send_res = msg_tx.send(msg) => {
                            if send_res.is_err() {
                                return Ok(());
                            }
                        }
                    }
                }
            }
        }
    }

    pub async fn get_chunks(&mut self, series_index: u64) -> Result<Vec<ingesterpb::Chunk>, StreamError> {
        if let Some(ref prev_err) = self.err {
            return Err(match prev_err {
                StreamError::Canceled(m) => StreamError::Canceled(format!(
                    "attempted to read series at index {series_index} from ingester chunks stream, but the stream previously failed and returned an error: {m}"
                )),
                StreamError::Limit(l) => StreamError::Limit(LimitError(format!(
                    "attempted to read series at index {series_index} from ingester chunks stream, but the stream previously failed and returned an error: {}",
                    l.0
                ))),
                StreamError::Other(m) => StreamError::Other(format!(
                    "attempted to read series at index {series_index} from ingester chunks stream, but the stream previously failed and returned an error: {m}"
                )),
            });
        }

        let res = self.get_chunks_inner(series_index).await;
        if let Err(ref e) = res {
            self.err = Some(e.clone());
        }
        res
    }

    async fn get_chunks_inner(
        &mut self,
        series_index: u64,
    ) -> Result<Vec<ingesterpb::Chunk>, StreamError> {
        if self.series_batch_offset >= self.series_batch.len() {
            self.read_next_batch(series_index).await?;
        }

        let series = self.series_batch[self.series_batch_offset].clone();
        self.series_batch_offset += 1;
        if self.series_batch_offset >= self.series_batch.len() {
            self.series_batch.clear();
            self.series_batch_offset = 0;
        }

        if series.series_index != series_index {
            return Err(StreamError::Other(format!(
                "attempted to read series at index {series_index} from ingester chunks stream, but the stream has series with index {}",
                series.series_index
            )));
        }

        if series_index as usize == self.expected_series_count.saturating_sub(1) {
            if let Some(err_rx) = self.error_rx.take() {
                if let Ok(Some(err)) = err_rx.await {
                    return Err(match err {
                        StreamError::Limit(l) => StreamError::Limit(LimitError(format!(
                            "attempted to read series at index {series_index} from ingester chunks stream, but the stream has failed: {}",
                            l.0
                        ))),
                        StreamError::Canceled(m) => StreamError::Canceled(format!(
                            "attempted to read series at index {series_index} from ingester chunks stream, but the stream has failed: {m}"
                        )),
                        StreamError::Other(m) => StreamError::Other(format!(
                            "attempted to read series at index {series_index} from ingester chunks stream, but the stream has failed: {m}"
                        )),
                    });
                }
            }
        }

        Ok(series.chunks)
    }

    async fn read_next_batch(&mut self, series_index: u64) -> Result<(), StreamError> {
        self.free_buffer();

        let msg = match self.series_message_rx.as_mut() {
            Some(rx) => rx.recv().await,
            None => None,
        };

        let Some(msg) = msg else {
            if let Some(err_rx) = self.error_rx.take() {
                if let Ok(Some(err)) = err_rx.await {
                    return Err(match err {
                        StreamError::Limit(l) => StreamError::Limit(l),
                        StreamError::Canceled(m) => StreamError::Canceled(format!(
                            "attempted to read series at index {series_index} from ingester chunks stream, but the stream has failed: {m}"
                        )),
                        StreamError::Other(m) => StreamError::Other(format!(
                            "attempted to read series at index {series_index} from ingester chunks stream, but the stream has failed: {m}"
                        )),
                    });
                }
            }

            return Err(StreamError::Other(format!(
                "attempted to read series at index {series_index} from ingester chunks stream, but the stream has already been exhausted (was expecting {} series)",
                self.expected_series_count
            )));
        };

        let batch = msg.streaming_series_chunks.clone();
        self.set_last_message(msg)?;
        self.series_batch = batch;
        self.series_batch_offset = 0;
        Ok(())
    }
}

/// Prometheus `model.SamplePair` equivalent.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SamplePair {
    pub timestamp: i64,
    pub value: f64,
}

/// Merges and dedupes two sets of already sorted sample pairs.
/// Ported from `mergeSampleSets` in `pkg/ingester/client/streaming_test_utils.go`.
pub fn merge_sample_sets(a: &[SamplePair], b: &[SamplePair]) -> Vec<SamplePair> {
    let mut result = Vec::with_capacity(a.len() + b.len());
    let (mut i, mut j) = (0usize, 0usize);
    while i < a.len() && j < b.len() {
        if a[i].timestamp < b[j].timestamp {
            result.push(a[i]);
            i += 1;
        } else if a[i].timestamp > b[j].timestamp {
            result.push(b[j]);
            j += 1;
        } else {
            result.push(a[i]);
            i += 1;
            j += 1;
        }
    }
    result.extend_from_slice(&a[i..]);
    result.extend_from_slice(&b[j..]);
    result
}

/// Merges and dedupes two sets of already sorted histograms.
/// Ported from `mergeHistogramSets` in `pkg/ingester/client/streaming_test_utils.go`.
pub fn merge_histogram_sets(
    a: &[cortexpb::Histogram],
    b: &[cortexpb::Histogram],
) -> Vec<cortexpb::Histogram> {
    let mut result = Vec::with_capacity(a.len() + b.len());
    let (mut i, mut j) = (0usize, 0usize);
    while i < a.len() && j < b.len() {
        if a[i].timestamp < b[j].timestamp {
            result.push(a[i].clone());
            i += 1;
        } else if a[i].timestamp > b[j].timestamp {
            result.push(b[j].clone());
            j += 1;
        } else {
            result.push(a[i].clone());
            i += 1;
            j += 1;
        }
    }
    result.extend_from_slice(&a[i..]);
    result.extend_from_slice(&b[j..]);
    result
}
