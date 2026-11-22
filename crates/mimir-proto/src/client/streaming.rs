//! Ported from github.com/grafana/mimir/pkg/ingester/client/streaming.go,
//! streaming_test_utils.go, pkg/querier/stats/query_metrics.go, and
//! pkg/util/limiter/{query_limiter.go,memory_consumption.go,series_labels_deduplicator.go,errors.go}.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::SystemTime;

use parking_lot::Mutex;
use prometheus::core::{Collector, Desc};
use prometheus::proto::{Gauge as ProtoGauge, Metric as ProtoMetric, MetricFamily, MetricType as PromMetricType};
use prometheus::{Counter, CounterVec, Opts, Registry};
use prost::Message as _;
use tokio::sync::{mpsc, oneshot, watch};

use crate::compat::PromLabels;
use crate::cortexpb;
use crate::ingesterpb;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LimitError(pub String);

impl fmt::Display for LimitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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

impl fmt::Display for StreamError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StreamError::Canceled(m) | StreamError::Other(m) => f.write_str(m),
            StreamError::Limit(l) => f.write_str(&l.0),
        }
    }
}

impl std::error::Error for StreamError {}

const CARDINALITY_STRATEGY: &str = "Consider reducing the time range and/or number of series selected by the query. One way to reduce the number of selected series is to add more label matchers to the query";

pub const REJECT_REASON_MAX_SERIES: &str = "max-fetched-series-per-query";
pub const REJECT_REASON_MAX_CHUNK_BYTES: &str = "max-fetched-chunk-bytes-per-query";
pub const REJECT_REASON_MAX_CHUNKS: &str = "max-fetched-chunks-per-query";
pub const REJECT_REASON_MAX_ESTIMATED_CHUNKS: &str = "max-estimated-fetched-chunks-per-query";
pub const REJECT_REASON_MAX_ESTIMATED_QUERY_MEMORY_CONSUMPTION: &str =
    "max-estimated-memory-consumption-per-query";

const REJECT_REASONS: [&str; 5] = [
    REJECT_REASON_MAX_SERIES,
    REJECT_REASON_MAX_CHUNK_BYTES,
    REJECT_REASON_MAX_CHUNKS,
    REJECT_REASON_MAX_ESTIMATED_CHUNKS,
    REJECT_REASON_MAX_ESTIMATED_QUERY_MEMORY_CONSUMPTION,
];

/// Ported from `stats.QueryMetrics` in `pkg/querier/stats/query_metrics.go`.
#[derive(Clone)]
pub struct QueryMetrics {
    pub ingester_chunks_deduplicated: Counter,
    pub ingester_chunks_total: Counter,
    pub queries_rejected_total: CounterVec,
    pub queries_executed_total: CounterVec,
}

impl QueryMetrics {
    pub fn new(reg: Option<&Registry>) -> Self {
        let ingester_chunks_deduplicated = Counter::with_opts(Opts::new(
            "cortex_distributor_query_ingester_chunks_deduped_total",
            "Number of chunks received from ingesters at query time but discarded as duplicates.",
        ))
        .expect("valid metric");
        let ingester_chunks_total = Counter::with_opts(Opts::new(
            "cortex_distributor_query_ingester_chunks_total",
            "Number of chunks received from ingesters at query time, including any chunks discarded as duplicates.",
        ))
        .expect("valid metric");
        let queries_rejected_total = CounterVec::new(
            Opts::new(
                "cortex_querier_queries_rejected_total",
                "Number of queries that were rejected, for example because they exceeded a limit.",
            ),
            &["reason"],
        )
        .expect("valid metric");
        let queries_executed_total = CounterVec::new(
            Opts::new(
                "cortex_querier_queries_storage_type_total",
                "Number of PromQL queries that were executed against a particular storage type.",
            ),
            &["storage"],
        )
        .expect("valid metric");

        for reason in REJECT_REASONS {
            queries_rejected_total.with_label_values(&[reason]);
        }

        if let Some(r) = reg {
            let _ = r.register(Box::new(ingester_chunks_deduplicated.clone()));
            let _ = r.register(Box::new(ingester_chunks_total.clone()));
            let _ = r.register(Box::new(queries_rejected_total.clone()));
            let _ = r.register(Box::new(queries_executed_total.clone()));
        }

        Self {
            ingester_chunks_deduplicated,
            ingester_chunks_total,
            queries_rejected_total,
            queries_executed_total,
        }
    }
}

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
    query_metrics: Option<Arc<QueryMetrics>>,
}

impl QueryLimiter {
    pub fn new(
        max_series_per_query: usize,
        max_chunk_bytes_per_query: usize,
        max_chunks_per_query: usize,
        max_estimated_chunks_per_query: usize,
    ) -> Self {
        Self::with_metrics(
            max_series_per_query,
            max_chunk_bytes_per_query,
            max_chunks_per_query,
            max_estimated_chunks_per_query,
            None,
        )
    }

    pub fn with_metrics(
        max_series_per_query: usize,
        max_chunk_bytes_per_query: usize,
        max_chunks_per_query: usize,
        max_estimated_chunks_per_query: usize,
        query_metrics: Option<Arc<QueryMetrics>>,
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
            query_metrics,
        }
    }

    pub fn add_series(&self, series_labels: &PromLabels) -> Result<(), LimitError> {
        if self.max_series_per_query == 0 {
            return Ok(());
        }
        self.add_series_hash(series_labels.hash())
    }

    pub fn add_series_hash(&self, fingerprint: u64) -> Result<(), LimitError> {
        if self.max_series_per_query == 0 {
            return Ok(());
        }
        let mut set = self.unique_series.lock();
        let before = set.len();
        set.insert(fingerprint);
        let after = set.len();
        if after > self.max_series_per_query {
            if before <= self.max_series_per_query {
                if let Some(ref m) = self.query_metrics {
                    m.queries_rejected_total
                        .with_label_values(&[REJECT_REASON_MAX_SERIES])
                        .inc();
                }
            }
            return Err(LimitError(format!(
                "the query exceeded the maximum number of series (limit: {} series) (err-mimir-max-series-per-query). {CARDINALITY_STRATEGY}. Otherwise, to adjust the related per-tenant limit, configure -querier.max-fetched-series-per-query, or contact your service administrator.",
                self.max_series_per_query
            )));
        }
        Ok(())
    }

    pub fn unique_series_count(&self) -> usize {
        self.unique_series.lock().len()
    }

    pub fn add_chunk_bytes(&self, chunk_size_in_bytes: usize) -> Result<(), LimitError> {
        if self.max_chunk_bytes_per_query == 0 {
            return Ok(());
        }
        let prev = self
            .chunk_bytes_count
            .fetch_add(chunk_size_in_bytes as i64, Ordering::SeqCst);
        let total_bytes = prev + chunk_size_in_bytes as i64;
        if total_bytes > self.max_chunk_bytes_per_query as i64 {
            if prev <= self.max_chunk_bytes_per_query as i64 {
                if let Some(ref m) = self.query_metrics {
                    m.queries_rejected_total
                        .with_label_values(&[REJECT_REASON_MAX_CHUNK_BYTES])
                        .inc();
                }
            }
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
        let prev = self.chunk_count.fetch_add(count as i64, Ordering::SeqCst);
        let total = prev + count as i64;
        if total > self.max_chunks_per_query as i64 {
            if prev <= self.max_chunks_per_query as i64 {
                if let Some(ref m) = self.query_metrics {
                    m.queries_rejected_total
                        .with_label_values(&[REJECT_REASON_MAX_CHUNKS])
                        .inc();
                }
            }
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
        let prev = self
            .estimated_chunk_count
            .fetch_add(count as i64, Ordering::SeqCst);
        let total = prev + count as i64;
        if total > self.max_estimated_chunks_per_query as i64 {
            if prev <= self.max_estimated_chunks_per_query as i64 {
                if let Some(ref m) = self.query_metrics {
                    m.queries_rejected_total
                        .with_label_values(&[REJECT_REASON_MAX_ESTIMATED_CHUNKS])
                        .inc();
                }
            }
            return Err(LimitError(format!(
                "the estimated number of chunks for the query exceeded the maximum allowed (limit: {} chunks) (err-mimir-max-estimated-chunks-per-query). {CARDINALITY_STRATEGY}. Otherwise, to adjust the related per-tenant limit, configure -querier.max-estimated-chunks-per-query-multiplier, or contact your service administrator.",
                self.max_estimated_chunks_per_query
            )));
        }
        Ok(())
    }
}

/// Ported from `limiter.MemoryConsumptionSource` in `pkg/util/limiter/memory_consumption.go`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(usize)]
pub enum MemoryConsumptionSource {
    IngesterChunks = 0,
    StoreGatewayChunks = 1,
    FPointSlices = 2,
    HPointSlices = 3,
    Vectors = 4,
    Float64Slices = 5,
    IntSlices = 6,
    IntSliceSlice = 7,
    Int64Slices = 8,
    BoolSlices = 9,
    HistogramPointerSlices = 10,
    SeriesMetadataSlices = 11,
    BucketSlices = 12,
    BucketsSlices = 13,
    QuantileGroupSlices = 14,
    TopKBottomKInstantQuerySeriesSlices = 15,
    TopKBottomKRangeQuerySeriesSlices = 16,
    Labels = 17,
    CounterResetHintSlices = 18,
    SeriesGroupPairSlices = 19,
    BucketGroupPointerSlices = 20,
    GroupPointerSlices = 21,
    AggregationGroup = 22,
    BufferedQuerierResponses = 23,
    CachedResponses = 24,
    BinaryOperationOutputSeries = 25,
}

pub const MEMORY_CONSUMPTION_SOURCE_COUNT: usize = 26;

impl MemoryConsumptionSource {
    pub const ALL: [MemoryConsumptionSource; MEMORY_CONSUMPTION_SOURCE_COUNT] = [
        Self::IngesterChunks,
        Self::StoreGatewayChunks,
        Self::FPointSlices,
        Self::HPointSlices,
        Self::Vectors,
        Self::Float64Slices,
        Self::IntSlices,
        Self::IntSliceSlice,
        Self::Int64Slices,
        Self::BoolSlices,
        Self::HistogramPointerSlices,
        Self::SeriesMetadataSlices,
        Self::BucketSlices,
        Self::BucketsSlices,
        Self::QuantileGroupSlices,
        Self::TopKBottomKInstantQuerySeriesSlices,
        Self::TopKBottomKRangeQuerySeriesSlices,
        Self::Labels,
        Self::CounterResetHintSlices,
        Self::SeriesGroupPairSlices,
        Self::BucketGroupPointerSlices,
        Self::GroupPointerSlices,
        Self::AggregationGroup,
        Self::BufferedQuerierResponses,
        Self::CachedResponses,
        Self::BinaryOperationOutputSeries,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::IngesterChunks => "ingester chunks",
            Self::StoreGatewayChunks => "store-gateway chunks",
            Self::FPointSlices => "[]promql.FPoint",
            Self::HPointSlices => "[]promql.HPoint",
            Self::Vectors => "promql.Vector",
            Self::Float64Slices => "[]float64",
            Self::IntSlices => "[]int",
            Self::IntSliceSlice => "[][]int",
            Self::Int64Slices => "[]int64",
            Self::CounterResetHintSlices => "[]CounterResetHint",
            Self::BoolSlices => "[]bool",
            Self::HistogramPointerSlices => "[]*histogram.FloatHistogram",
            Self::SeriesMetadataSlices => "[]SeriesMetadata",
            Self::BucketSlices => "[]promql.Bucket",
            Self::BucketsSlices => "[]promql.Buckets",
            Self::QuantileGroupSlices => "[]aggregations.qGroup",
            Self::TopKBottomKInstantQuerySeriesSlices => "[]topkbottom.instantQuerySeries",
            Self::TopKBottomKRangeQuerySeriesSlices => "[]topkbottom.rangeQuerySeries",
            Self::Labels => "labels.Labels",
            Self::SeriesGroupPairSlices => "[]functions.seriesGroupPair",
            Self::BucketGroupPointerSlices => "[]*functions.bucketGroup",
            Self::GroupPointerSlices => "[]*aggregations.group",
            Self::AggregationGroup => "aggregation.AggregationGroup",
            Self::BufferedQuerierResponses => "buffered querier responses",
            Self::CachedResponses => "cached responses",
            Self::BinaryOperationOutputSeries => "binary operation output series",
        }
    }
}

impl fmt::Display for MemoryConsumptionSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug)]
struct TrackerState {
    current_bytes: u64,
    peak_bytes: u64,
    by_source: [u64; MEMORY_CONSUMPTION_SOURCE_COUNT],
    have_recorded_rejection: bool,
}

impl Default for TrackerState {
    fn default() -> Self {
        Self {
            current_bytes: 0,
            peak_bytes: 0,
            by_source: [0u64; MEMORY_CONSUMPTION_SOURCE_COUNT],
            have_recorded_rejection: false,
        }
    }
}

/// Ported from `limiter.MemoryConsumptionTracker` in `pkg/util/limiter/memory_consumption.go`.
pub struct MemoryConsumptionTracker {
    max_estimated_memory_bytes: u64,
    state: Mutex<TrackerState>,
    rejections: Option<Counter>,
    query_description: String,
    trace_id: String,
    producer: Option<std::sync::Weak<InflightMemoryConsumptionTracker>>,
    parent: Option<Arc<MemoryConsumptionTracker>>,
    start_time: Option<SystemTime>,
}

impl fmt::Debug for MemoryConsumptionTracker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryConsumptionTracker")
            .field("max_estimated_memory_bytes", &self.max_estimated_memory_bytes)
            .field("query_description", &self.query_description)
            .field("trace_id", &self.trace_id)
            .finish()
    }
}

impl MemoryConsumptionTracker {
    pub fn new(max_estimated_memory_bytes: u64, rejections: Option<Counter>) -> Self {
        Self::new_with_details(max_estimated_memory_bytes, rejections, "", None)
    }

    pub fn new_with_details(
        max_estimated_memory_bytes: u64,
        rejections: Option<Counter>,
        query_description: impl Into<String>,
        trace_id: Option<String>,
    ) -> Self {
        Self {
            max_estimated_memory_bytes,
            state: Mutex::new(TrackerState::default()),
            rejections,
            query_description: query_description.into(),
            trace_id: trace_id.unwrap_or_default(),
            producer: None,
            parent: None,
            start_time: None,
        }
    }

    pub fn unlimited() -> Self {
        Self::new(0, None)
    }

    pub fn max_estimated_memory_consumption_bytes(&self) -> u64 {
        self.max_estimated_memory_bytes
    }

    pub fn query_description(&self) -> &str {
        &self.query_description
    }

    pub fn increase_memory_consumption(&self, bytes: u64) -> Result<(), LimitError> {
        self.increase_memory_consumption_by_source(bytes, MemoryConsumptionSource::IngesterChunks)
    }

    pub fn increase_memory_consumption_by_source(
        &self,
        bytes: u64,
        source: MemoryConsumptionSource,
    ) -> Result<(), LimitError> {
        if let Some(ref parent) = self.parent {
            parent.increase_memory_consumption_by_source(bytes, source)?;
        }

        let mut st = self.state.lock();
        if self.max_estimated_memory_bytes > 0
            && st.current_bytes.saturating_add(bytes) > self.max_estimated_memory_bytes
        {
            if !st.have_recorded_rejection {
                st.have_recorded_rejection = true;
                if let Some(ref counter) = self.rejections {
                    counter.inc();
                }
            }
            return Err(LimitError(format!(
                "the query exceeded the maximum allowed estimated amount of memory consumed by a single query (limit: {} bytes) (err-mimir-max-estimated-memory-consumption-per-query). {CARDINALITY_STRATEGY}. Otherwise, to adjust the related per-tenant limit, configure -querier.max-estimated-memory-consumption-per-query, or contact your service administrator.",
                self.max_estimated_memory_bytes
            )));
        }

        st.by_source[source as usize] += bytes;
        st.current_bytes += bytes;
        st.peak_bytes = st.peak_bytes.max(st.current_bytes);
        Ok(())
    }

    pub fn decrease_memory_consumption(&self, bytes: u64) {
        self.decrease_memory_consumption_by_source(bytes, MemoryConsumptionSource::IngesterChunks);
    }

    pub fn decrease_memory_consumption_by_source(
        &self,
        bytes: u64,
        source: MemoryConsumptionSource,
    ) {
        if let Some(ref parent) = self.parent {
            parent.decrease_memory_consumption_by_source(bytes, source);
        }

        let mut st = self.state.lock();
        let current_src = st.by_source[source as usize];
        if bytes > current_src {
            let trace_description = if self.trace_id.is_empty() {
                String::new()
            } else {
                format!(" (trace ID: {})", self.trace_id)
            };
            panic!(
                "Estimated memory consumption of all instances of {} in this query is {} bytes when trying to return {} bytes. This indicates something has been returned to a pool more than once, which is a bug. The affected query is: {}{}",
                source, current_src, bytes, self.query_description, trace_description
            );
        }
        st.current_bytes -= bytes;
        st.by_source[source as usize] -= bytes;
    }

    pub fn increase_memory_consumption_for_labels(
        &self,
        labels: &PromLabels,
    ) -> Result<(), LimitError> {
        self.increase_memory_consumption_by_source(
            labels.byte_size() as u64,
            MemoryConsumptionSource::Labels,
        )
    }

    pub fn decrease_memory_consumption_for_labels(&self, labels: &PromLabels) {
        self.decrease_memory_consumption_by_source(
            labels.byte_size() as u64,
            MemoryConsumptionSource::Labels,
        );
    }

    pub fn current_estimated_memory_consumption_bytes(&self) -> u64 {
        self.state.lock().current_bytes
    }

    pub fn peak_estimated_memory_consumption_bytes(&self) -> u64 {
        self.state.lock().peak_bytes
    }

    pub fn current_estimated_memory_consumption_bytes_by_source(
        &self,
        source: MemoryConsumptionSource,
    ) -> u64 {
        self.state.lock().by_source[source as usize]
    }

    pub fn describe_current_memory_consumption(&self) -> String {
        let st = self.state.lock();
        let mut out = String::new();
        for src in MemoryConsumptionSource::ALL {
            out.push_str(src.as_str());
            out.push_str(": ");
            out.push_str(&st.by_source[src as usize].to_string());
            out.push_str(" B \n");
        }
        out
    }

    pub fn new_nested_memory_consumption_tracker(
        self: &Arc<Self>,
        query_description: impl Into<String>,
        trace_id: Option<String>,
    ) -> Arc<Self> {
        let producer = self
            .producer
            .as_ref()
            .and_then(std::sync::Weak::upgrade)
            .expect("cannot nest a tracker not created via a InflightMemoryConsumptionTracker");
        Arc::new(Self {
            max_estimated_memory_bytes: self.max_estimated_memory_bytes,
            state: Mutex::new(TrackerState::default()),
            rejections: producer.queries_rejected.clone(),
            query_description: query_description.into(),
            trace_id: trace_id.unwrap_or_default(),
            producer: None,
            parent: Some(Arc::clone(self)),
            start_time: None,
        })
    }
}

/// Ported from `limiter.InflightMemoryConsumptionTracker` in `pkg/util/limiter/memory_consumption.go`.
pub struct InflightMemoryConsumptionTracker {
    inflight: Mutex<HashMap<usize, Arc<MemoryConsumptionTracker>>>,
    max_desc: Desc,
    current_desc: Desc,
    peak_desc: Desc,
    sampled_desc: Desc,
    max_age_desc: Desc,
    now: Mutex<Arc<dyn Fn() -> SystemTime + Send + Sync>>,
    queries_rejected: Option<Counter>,
    force_unlimited: AtomicBool,
}

#[derive(Clone)]
struct InflightCollector(Arc<InflightMemoryConsumptionTracker>);

impl Collector for InflightCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.0.descs()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        self.0.collect_families()
    }
}

impl InflightMemoryConsumptionTracker {
    pub fn new_unlimited(reg: Option<&Registry>) -> Arc<Self> {
        let t = Self::new(reg, None);
        t.force_unlimited.store(true, Ordering::Release);
        t
    }

    pub fn new(reg: Option<&Registry>, queries_rejected: Option<Counter>) -> Arc<Self> {
        let t = Arc::new(Self {
            inflight: Mutex::new(HashMap::new()),
            max_desc: Desc::new(
                "cortex_querier_inflight_query_max_estimated_memory_consumption_limit_bytes".to_string(),
                "Total of the max estimated memory consumption limit across all in-flight queries.".to_string(),
                vec![],
                HashMap::new(),
            )
            .expect("valid desc"),
            current_desc: Desc::new(
                "cortex_querier_inflight_query_current_estimated_memory_consumption_bytes".to_string(),
                "Total current estimated memory consumption across all in-flight queries.".to_string(),
                vec![],
                HashMap::new(),
            )
            .expect("valid desc"),
            peak_desc: Desc::new(
                "cortex_querier_inflight_query_peak_estimated_memory_consumption_bytes".to_string(),
                "Total peak estimated memory consumption across all in-flight queries.".to_string(),
                vec![],
                HashMap::new(),
            )
            .expect("valid desc"),
            sampled_desc: Desc::new(
                "cortex_querier_inflight_query_sampled_count".to_string(),
                "Number of in-flight memory consumption trackers accumulated during the last metrics collection.".to_string(),
                vec![],
                HashMap::new(),
            )
            .expect("valid desc"),
            max_age_desc: Desc::new(
                "cortex_querier_inflight_query_max_age_seconds".to_string(),
                "Age in seconds of the oldest in-flight query memory consumption tracker. Zero when there are no in-flight queries.".to_string(),
                vec![],
                HashMap::new(),
            )
            .expect("valid desc"),
            now: Mutex::new(Arc::new(SystemTime::now)),
            queries_rejected,
            force_unlimited: AtomicBool::new(false),
        });
        if let Some(r) = reg {
            r.register(Box::new(InflightCollector(Arc::clone(&t))))
                .expect("register InflightMemoryConsumptionTracker");
        }
        t
    }

    pub fn set_clock(&self, clock: Arc<dyn Fn() -> SystemTime + Send + Sync>) {
        *self.now.lock() = clock;
    }

    fn current_time(&self) -> SystemTime {
        let f = self.now.lock().clone();
        f()
    }

    pub fn new_memory_consumption_tracker(
        self: &Arc<Self>,
        mut max_estimated_memory_bytes: u64,
        query_description: impl Into<String>,
        trace_id: Option<String>,
    ) -> Arc<MemoryConsumptionTracker> {
        if self.force_unlimited.load(Ordering::Acquire) {
            max_estimated_memory_bytes = 0;
        }
        let start = self.current_time();
        let tracker = Arc::new(MemoryConsumptionTracker {
            max_estimated_memory_bytes,
            state: Mutex::new(TrackerState::default()),
            rejections: self.queries_rejected.clone(),
            query_description: query_description.into(),
            trace_id: trace_id.unwrap_or_default(),
            producer: Some(Arc::downgrade(self)),
            parent: None,
            start_time: Some(start),
        });
        let key = Arc::as_ptr(&tracker) as usize;
        self.inflight.lock().insert(key, Arc::clone(&tracker));
        tracker
    }

    pub fn deregister(self: &Arc<Self>, tracker: &Arc<MemoryConsumptionTracker>) {
        if tracker.parent.is_some() || tracker.producer.is_none() {
            return;
        }
        if let Some(ref prod) = tracker.producer {
            if !std::ptr::eq(prod.as_ptr(), Arc::as_ptr(self)) {
                panic!("cannot deregister inflight memory consumption tracker - the given tracker was allocated by another InflightMemoryConsumptionTracker");
            }
        }
        let key = Arc::as_ptr(tracker) as usize;
        self.inflight.lock().remove(&key);
    }

    pub fn is_tracking(self: &Arc<Self>, tracker: &Arc<MemoryConsumptionTracker>) -> bool {
        match tracker.producer {
            Some(ref prod) if std::ptr::eq(prod.as_ptr(), Arc::as_ptr(self)) => {
                let key = Arc::as_ptr(tracker) as usize;
                self.inflight.lock().contains_key(&key)
            }
            _ => false,
        }
    }

    fn descs(&self) -> Vec<&Desc> {
        vec![
            &self.max_desc,
            &self.current_desc,
            &self.peak_desc,
            &self.sampled_desc,
            &self.max_age_desc,
        ]
    }

    fn collect_families(&self) -> Vec<MetricFamily> {
        let trackers: Vec<Arc<MemoryConsumptionTracker>> =
            self.inflight.lock().values().cloned().collect();
        let mut max_bytes = 0.0f64;
        let mut current_bytes = 0.0f64;
        let mut peak_bytes = 0.0f64;
        let mut oldest_start: Option<SystemTime> = None;
        let sampled = trackers.len();

        for tr in &trackers {
            max_bytes += tr.max_estimated_memory_bytes as f64;
            current_bytes += tr.current_estimated_memory_consumption_bytes() as f64;
            peak_bytes += tr.peak_estimated_memory_consumption_bytes() as f64;
            if let Some(st) = tr.start_time {
                oldest_start = Some(match oldest_start {
                    None => st,
                    Some(cur) if st < cur => st,
                    Some(cur) => cur,
                });
            }
        }

        let max_age_seconds = match oldest_start {
            Some(oldest) => {
                let now = self.current_time();
                match now.duration_since(oldest) {
                    Ok(d) => d.as_secs_f64(),
                    Err(e) => -e.duration().as_secs_f64(),
                }
            }
            None => 0.0,
        };

        let make_gauge = |desc: &Desc, val: f64| -> MetricFamily {
            let mut g = ProtoGauge::default();
            g.set_value(val);
            let mut m = ProtoMetric::default();
            m.set_gauge(g);
            let mut mf = MetricFamily::default();
            mf.set_name(desc.fq_name.clone());
            mf.set_help(desc.help.clone());
            mf.set_field_type(PromMetricType::GAUGE);
            mf.set_metric(vec![m].into());
            mf
        };

        vec![
            make_gauge(&self.max_desc, max_bytes),
            make_gauge(&self.current_desc, current_bytes),
            make_gauge(&self.peak_desc, peak_bytes),
            make_gauge(&self.sampled_desc, sampled as f64),
            make_gauge(&self.max_age_desc, max_age_seconds),
        ]
    }
}

/// Metrics for series label deduplication (`pkg/util/limiter/series_labels_deduplicator.go`).
#[derive(Clone)]
pub struct SeriesDeduplicatorMetrics {
    pub series_labels_total: Counter,
    pub series_labels_deduplicated: Counter,
}

impl SeriesDeduplicatorMetrics {
    pub fn new(reg: Option<&Registry>) -> Self {
        let series_labels_total = Counter::with_opts(Opts::new(
            "cortex_querier_labels_deduplicator_processed_total",
            "Total number of series labels processed by the deduplicator, including both unique and duplicate series.",
        ))
        .expect("valid metric");
        let series_labels_deduplicated = Counter::with_opts(Opts::new(
            "cortex_querier_labels_deduplicator_deduplicated_total",
            "Number of series labels that were deduplicated by reusing existing labels instead of creating new ones.",
        ))
        .expect("valid metric");
        if let Some(r) = reg {
            let _ = r.register(Box::new(series_labels_total.clone()));
            let _ = r.register(Box::new(series_labels_deduplicated.clone()));
        }
        Self {
            series_labels_total,
            series_labels_deduplicated,
        }
    }
}

#[derive(Default)]
struct DeduplicatorState {
    unique_series: HashMap<u64, PromLabels>,
    conflict_series: Option<HashMap<u64, Vec<PromLabels>>>,
}

/// Ported from `limiter.SeriesLabelsDeduplicator` in `pkg/util/limiter/series_labels_deduplicator.go`.
pub struct SeriesLabelsDeduplicator {
    state: Mutex<DeduplicatorState>,
    hash_func: Box<dyn Fn(&PromLabels) -> u64 + Send + Sync>,
    metrics: SeriesDeduplicatorMetrics,
}

impl SeriesLabelsDeduplicator {
    pub fn new(metrics: SeriesDeduplicatorMetrics) -> Self {
        Self::with_hash_func(metrics, Box::new(PromLabels::hash))
    }

    pub fn with_hash_func(
        metrics: SeriesDeduplicatorMetrics,
        hash_func: Box<dyn Fn(&PromLabels) -> u64 + Send + Sync>,
    ) -> Self {
        Self {
            state: Mutex::new(DeduplicatorState::default()),
            hash_func,
            metrics,
        }
    }

    pub fn deduplicate(
        &self,
        new_labels: PromLabels,
        tracker: &MemoryConsumptionTracker,
    ) -> Result<PromLabels, LimitError> {
        let fingerprint = (self.hash_func)(&new_labels);
        let mut st = self.state.lock();
        self.metrics.series_labels_total.inc();

        if let Some(existing) = st.unique_series.get(&fingerprint) {
            if existing == &new_labels {
                self.metrics.series_labels_deduplicated.inc();
                return Ok(existing.clone());
            }
        } else {
            st.unique_series.insert(fingerprint, new_labels.clone());
            return match tracker.increase_memory_consumption_for_labels(&new_labels) {
                Ok(()) => Ok(new_labels),
                Err(e) => Err(e),
            };
        }

        let conflicts = st.conflict_series.get_or_insert_with(HashMap::new);
        let bucket = conflicts.entry(fingerprint).or_default();
        for existing in bucket.iter() {
            if existing == &new_labels {
                self.metrics.series_labels_deduplicated.inc();
                return Ok(existing.clone());
            }
        }
        bucket.push(new_labels.clone());
        match tracker.increase_memory_consumption_for_labels(&new_labels) {
            Ok(()) => Ok(new_labels),
            Err(e) => Err(e),
        }
    }

    pub fn series_count(&self) -> usize {
        let st = self.state.lock();
        let mut count = st.unique_series.len();
        if let Some(ref conflicts) = st.conflict_series {
            for v in conflicts.values() {
                count += v.len();
            }
        }
        count
    }

    pub fn unique_series_len(&self) -> usize {
        self.state.lock().unique_series.len()
    }

    pub fn has_conflicts(&self) -> bool {
        self.state.lock().conflict_series.is_some()
    }

    pub fn conflict_bucket_len(&self, hash: u64) -> usize {
        self.state
            .lock()
            .conflict_series
            .as_ref()
            .and_then(|c| c.get(&hash))
            .map_or(0, Vec::len)
    }
}

tokio::task_local! {
    static MEMORY_TRACKER_CTX: Arc<MemoryConsumptionTracker>;
    static QUERY_LIMITER_CTX: Arc<QueryLimiter>;
    static SERIES_DEDUP_CTX: Arc<SeriesLabelsDeduplicator>;
}

pub const ERR_NO_MEMORY_CONSUMPTION_TRACKER_IN_CONTEXT: &str =
    "no memory consumption tracker in context";

pub async fn with_memory_tracker<F>(
    tracker: Arc<MemoryConsumptionTracker>,
    fut: F,
) -> F::Output
where
    F: std::future::Future,
{
    MEMORY_TRACKER_CTX.scope(tracker, fut).await
}

pub fn memory_consumption_tracker_from_context(
) -> Result<Arc<MemoryConsumptionTracker>, &'static str> {
    MEMORY_TRACKER_CTX
        .try_with(Arc::clone)
        .map_err(|_| ERR_NO_MEMORY_CONSUMPTION_TRACKER_IN_CONTEXT)
}

pub async fn with_unlimited_memory_tracker<F>(fut: F) -> F::Output
where
    F: std::future::Future,
{
    with_memory_tracker(Arc::new(MemoryConsumptionTracker::unlimited()), fut).await
}

pub async fn with_query_limiter<F>(limiter: Arc<QueryLimiter>, fut: F) -> F::Output
where
    F: std::future::Future,
{
    QUERY_LIMITER_CTX.scope(limiter, fut).await
}

pub fn query_limiter_from_context_with_fallback() -> Arc<QueryLimiter> {
    QUERY_LIMITER_CTX
        .try_with(Arc::clone)
        .unwrap_or_else(|_| Arc::new(QueryLimiter::new(0, 0, 0, 0)))
}

pub async fn with_new_series_labels_deduplicator<F>(
    metrics: SeriesDeduplicatorMetrics,
    fut: F,
) -> F::Output
where
    F: std::future::Future,
{
    SERIES_DEDUP_CTX
        .scope(Arc::new(SeriesLabelsDeduplicator::new(metrics)), fut)
        .await
}

pub fn series_labels_deduplicator_from_context(
) -> Result<Arc<SeriesLabelsDeduplicator>, &'static str> {
    SERIES_DEDUP_CTX
        .try_with(Arc::clone)
        .map_err(|_| "no series deduplicator in context")
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

    async fn wait_cancelled(rx: &mut watch::Receiver<bool>) {
        if rx.wait_for(|&c| c).await.is_ok() {
            return;
        }
        std::future::pending::<()>().await;
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
                        _ = Self::wait_cancelled(parent_cancel) => {
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
