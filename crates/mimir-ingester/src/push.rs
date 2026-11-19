//! Zero-copy Push pipeline, validation, limits, and error accumulation.
//!
//! Ported from github.com/grafana/mimir/pkg/ingester/ingester_push.go.

use std::cell::RefCell;
use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use mimir_common::limiter::{InflightPermit, ReactivePermit};
use mimir_common::thread_pool::ScopedWorkerPool;
use mimir_config::LimitsConfig;
use mimir_proto::cortexpb;
use mimir_proto::prost::Message;
use mimir_proto::sharding::shard_by_all_labels;
use mimir_proto::zerocopy::{
    BorrowedExemplarView, BorrowedHistogramView, BorrowedMetadata, BorrowedTimeSeriesView,
    BorrowedWriteRequest,
};
use mimir_ring::ring::replication_strategy::WRITE_NO_EXTEND;
use mimir_tsdb::chunkenc::{CounterResetHint, FloatHistogram, Histogram, HistogramSpan};
use mimir_tsdb::exemplar::{Exemplar, ExemplarError};
use mimir_tsdb::head::{prefetch_read_data, HeadAppender, HeadError, SeriesEntry};
use mimir_tsdb::labels::{iter_stringlabels, Labels};
use parking_lot::Mutex;
use smallvec::SmallVec;
use tracing::warn;

const CLASSIC_PUSH_SHARD_THRESHOLD: usize = 4096;

fn classic_push_pool() -> &'static ScopedWorkerPool {
    static POOL: OnceLock<ScopedWorkerPool> = OnceLock::new();
    POOL.get_or_init(|| {
        let hw = std::thread::available_parallelism().map_or(4, |n| n.get());
        ScopedWorkerPool::new("classic-push", hw.clamp(2, 8).saturating_sub(1))
    })
}

use crate::active_groups::group_label;
use crate::costattribution::SampleTracker;
use crate::circuit_breaker::BreakerPermit;
use crate::errors::{
    format_labels_string, map_native_histogram_err, ErrorSampler, ErrorSamplers,
    IngesterError,
};
use crate::ingester::Ingester;
use crate::metrics::{
    REASON_MAX_INFLIGHT_PUSH_REQUESTS, REASON_MAX_INFLIGHT_PUSH_REQUESTS_BYTES,
    REASON_MAX_INGESTION_RATE,
};
use crate::user_tsdb::UserTsdb;

pub const ADAPTIVE_LIMITER_EXCEEDED_MSG: &str = "limit exceeded";

pub struct PushTapPermit {
    pub started_at: Instant,
    pub breaker_permit: Option<BreakerPermit>,
    pub reactive_permit: Option<ReactivePermit>,
    pub ingester: Arc<Ingester>,
    pub req_size: i64,
    pub finished: bool,
}

impl PushTapPermit {
    pub fn set_reactive_permit(&mut self, rp: ReactivePermit) {
        self.reactive_permit = Some(rp);
    }

    pub fn finish(self, err: Option<&IngesterError>) {
        let dur = self.started_at.elapsed();
        self.finish_with_duration(dur, err);
    }

    pub fn finish_with_duration(mut self, dur: Duration, err: Option<&IngesterError>) {
        if self.finished {
            return;
        }
        self.finished = true;
        self.ingester
            .inflight_push_requests
            .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
        if self.req_size > 0 {
            self.ingester
                .inflight_push_requests_bytes
                .fetch_sub(self.req_size, std::sync::atomic::Ordering::AcqRel);
        }
        if let Some(bp) = self.breaker_permit.take() {
            let be = err.map(|e| crate::circuit_breaker::BreakerError::Ingester(e.clone()));
            bp.finish_with_err(dur, be.as_ref());
        }
        if let Some(rp) = self.reactive_permit.take() {
            rp.record();
        }
    }
}

impl Drop for PushTapPermit {
    fn drop(&mut self) {
        // Pre-handler drop (request cancelled or failed before entering push handler):
        // matches Go `FinishPushRequest(ctx)` when `PushWithCleanup` was never entered
        // (zero-duration success on circuit breaker, drop reactive permit without RTT sample).
        if !self.finished {
            self.finished = true;
            self.ingester
                .inflight_push_requests
                .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
            if self.req_size > 0 {
                self.ingester
                    .inflight_push_requests_bytes
                    .fetch_sub(self.req_size, std::sync::atomic::Ordering::AcqRel);
            }
            if let Some(bp) = self.breaker_permit.take() {
                bp.finish_with_err(Duration::ZERO, None);
            }
            if let Some(rp) = self.reactive_permit.take() {
                rp.drop_permit();
            }
        }
    }
}

pub struct PushPermit<'a> {
    pub started_at: Instant,
    pub breaker_permit: Option<BreakerPermit>,
    pub reactive_permit: Option<ReactivePermit>,
    pub inflight_permit: Option<InflightPermit<'a>>,
    pub ingester: &'a Ingester,
    pub req_size: i64,
    pub finished: bool,
}

impl PushPermit<'_> {
    pub fn finish(self, err: Option<&IngesterError>) {
        let dur = self.started_at.elapsed();
        self.finish_with_duration(dur, err);
    }

    pub fn finish_with_duration(mut self, dur: Duration, err: Option<&IngesterError>) {
        if self.finished {
            return;
        }
        self.finished = true;
        self.ingester
            .inflight_push_requests
            .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
        if self.req_size > 0 {
            self.ingester
                .inflight_push_requests_bytes
                .fetch_sub(self.req_size, std::sync::atomic::Ordering::AcqRel);
        }
        drop(self.inflight_permit.take());
        if let Some(bp) = self.breaker_permit.take() {
            let be = err.map(|e| crate::circuit_breaker::BreakerError::Ingester(e.clone()));
            bp.finish_with_err(dur, be.as_ref());
        }
        if let Some(rp) = self.reactive_permit.take() {
            rp.record();
        }
    }
}

impl Drop for PushPermit<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.finished = true;
            self.ingester
                .inflight_push_requests
                .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
            if self.req_size > 0 {
                self.ingester
                    .inflight_push_requests_bytes
                    .fetch_sub(self.req_size, std::sync::atomic::Ordering::AcqRel);
            }
            drop(self.inflight_permit.take());
            drop(self.breaker_permit.take());
            if let Some(rp) = self.reactive_permit.take() {
                rp.drop_permit();
            }
        }
    }
}

#[derive(Default)]
struct PushStats {
    succeeded_samples: usize,
    failed_samples: usize,
    succeeded_exemplars: usize,
    failed_exemplars: usize,
    sample_timestamp_too_old: usize,
    sample_out_of_order: usize,
    sample_too_old: usize,
    sample_too_far_in_future: usize,
    new_value_for_timestamp: usize,
    per_user_series_limit: usize,
    per_metric_series_limit: usize,
    invalid_native_histogram: usize,
    labels_not_sorted: usize,
    series_with_floats_and_histograms: usize,
}

impl PushStats {
    /// Discarded-sample counters in the order of [`DISCARD_REASONS`].
    fn reason_counts(&self) -> [usize; 9] {
        [
            self.sample_timestamp_too_old,
            self.sample_out_of_order,
            self.sample_too_old,
            self.sample_too_far_in_future,
            self.new_value_for_timestamp,
            self.per_user_series_limit,
            self.per_metric_series_limit,
            self.invalid_native_histogram,
            self.labels_not_sorted,
        ]
    }
}

/// Discard reasons attributed by cost attribution (Go: `reason*` constants in `pkg/ingester/ingester.go`).
const DISCARD_REASONS: [&str; 9] = [
    "sample-timestamp-too-old",
    "sample-out-of-order",
    "sample-too-old",
    "sample-too-far-in-future",
    "new-value-for-timestamp",
    "per_user_series_limit",
    "per_metric_series_limit",
    "invalid-native-histogram",
    "labels-not-sorted",
];

/// Attributes the samples discarded since `prev` to the series whose labels are `labels`
/// (Go: `SampleTracker.IncrementDiscardedSamples` calls in the push error processor).
fn attribute_discarded<'l>(
    cast: &SampleTracker,
    prev: &mut [usize; 9],
    stats: &PushStats,
    labels: impl Iterator<Item = (&'l str, &'l str)> + Clone,
    now: SystemTime,
) {
    let cur = stats.reason_counts();
    if cur == *prev {
        return;
    }
    let get = |n: &str| labels.clone().find(|(k, _)| *k == n).map(|(_, v)| v.to_string());
    for (i, reason) in DISCARD_REASONS.iter().enumerate() {
        let delta = cur[i] - prev[i];
        if delta > 0 {
            cast.increment_discarded_samples(&get, delta as f64, reason, now);
        }
    }
    *prev = cur;
}

pub(crate) type ConvertedHistogram = (i64, bool, Option<Histogram>, Option<FloatHistogram>, usize);

#[derive(Clone, Copy)]
enum SampleKind {
    SampleTimestampTooOld,
    SampleTimestampTooOldOooEnabled,
    SampleTimestampTooFarInFuture,
    SampleOutOfOrder,
    SampleDuplicateTimestamp,
    MaxSeriesPerUser,
    MaxSeriesPerMetric,
    NativeHistogramValidation,
    LabelsNotSorted,
}

impl SampleKind {
    fn sampler(self, samplers: &ErrorSamplers) -> &ErrorSampler {
        match self {
            Self::SampleTimestampTooOld => &samplers.sample_timestamp_too_old,
            Self::SampleTimestampTooOldOooEnabled => &samplers.sample_timestamp_too_old_ooo_enabled,
            Self::SampleTimestampTooFarInFuture => &samplers.sample_timestamp_too_far_in_future,
            Self::SampleOutOfOrder => &samplers.sample_out_of_order,
            Self::SampleDuplicateTimestamp => &samplers.sample_duplicate_timestamp,
            Self::MaxSeriesPerUser => &samplers.max_series_per_user_limit_exceeded,
            Self::MaxSeriesPerMetric => &samplers.max_series_per_metric_limit_exceeded,
            Self::NativeHistogramValidation => &samplers.native_histogram_validation_error,
            Self::LabelsNotSorted => &samplers.labels_not_sorted,
        }
    }
}

thread_local! {
    static PAIRS_SCRATCH: RefCell<Vec<(&'static str, &'static str)>> =
        RefCell::new(Vec::with_capacity(32));
    static RESOLVED_ENTRIES_SCRATCH: RefCell<Vec<Option<Arc<SeriesEntry>>>> =
        RefCell::new(Vec::with_capacity(256));
}

struct ResolvedEntriesGuard {
    from_tls: bool,
    vec: Vec<Option<Arc<SeriesEntry>>>,
}

impl ResolvedEntriesGuard {
    fn take(cap: usize) -> Self {
        let taken = RESOLVED_ENTRIES_SCRATCH.with(|cell| {
            cell.try_borrow_mut()
                .ok()
                .map(|mut v| std::mem::take(&mut *v))
        });
        match taken {
            Some(mut vec) => {
                vec.clear();
                if vec.capacity() < cap {
                    vec.reserve(cap - vec.len());
                }
                Self {
                    from_tls: true,
                    vec,
                }
            }
            None => Self {
                from_tls: false,
                vec: Vec::with_capacity(cap),
            },
        }
    }
}

impl Drop for ResolvedEntriesGuard {
    fn drop(&mut self) {
        self.vec.clear();
        if self.from_tls {
            let vec = std::mem::take(&mut self.vec);
            RESOLVED_ENTRIES_SCRATCH.with(|cell| {
                if let Ok(mut slot) = cell.try_borrow_mut() {
                    if slot.capacity() < vec.capacity() && vec.capacity() <= 8192 {
                        *slot = vec;
                    }
                }
            });
        }
    }
}

struct PairsScratchGuard<'a> {
    from_tls: bool,
    vec: Vec<(&'static str, &'static str)>,
    _marker: std::marker::PhantomData<&'a ()>,
}

impl<'a> PairsScratchGuard<'a> {
    fn take() -> Self {
        let taken = PAIRS_SCRATCH.with(|cell| {
            cell.try_borrow_mut()
                .ok()
                .map(|mut v| std::mem::take(&mut *v))
        });
        match taken {
            Some(mut vec) => {
                vec.clear();
                if vec.capacity() < 32 {
                    vec.reserve(32);
                }
                Self {
                    from_tls: true,
                    vec,
                    _marker: std::marker::PhantomData,
                }
            }
            None => Self {
                from_tls: false,
                vec: Vec::with_capacity(32),
                _marker: std::marker::PhantomData,
            },
        }
    }

    #[inline]
    fn as_mut_pairs(&mut self) -> &mut Vec<(&'a str, &'a str)> {
        // SAFETY: `self.vec` is empty when taken and cleared on Drop before `'a` ends.
        unsafe { &mut *(&mut self.vec as *mut Vec<(&'static str, &'static str)>).cast() }
    }
}

impl Drop for PairsScratchGuard<'_> {
    fn drop(&mut self) {
        self.vec.clear();
        if self.from_tls {
            let vec = std::mem::take(&mut self.vec);
            PAIRS_SCRATCH.with(|cell| {
                if let Ok(mut slot) = cell.try_borrow_mut() {
                    if slot.capacity() < vec.capacity() {
                        *slot = vec;
                    }
                }
            });
        }
    }
}

#[inline(always)]
pub(crate) fn fast_utf8_str(b: &[u8]) -> Result<&str, std::str::Utf8Error> {
    if b.is_ascii() {
        // SAFETY: every byte < 0x80 is valid UTF-8.
        Ok(unsafe { std::str::from_utf8_unchecked(b) })
    } else {
        std::str::from_utf8(b)
    }
}

#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct ConvertedExemplarError {
    pub timestamp_ms: i64,
    pub message: String,
    pub exemplar_labels: String,
}

#[doc(hidden)]
pub type ConvertedExemplarResult =
    Result<(i64, f64, Labels, String), ConvertedExemplarError>;

#[doc(hidden)]
pub struct PrehashedSeriesItem<'a> {
    pub encoded_labels: &'a [u8],
    pub raw_encoded_labels: &'a [u8],
    pub samples: &'a [cortexpb::Sample],
    pub created_timestamp: i64,
    pub prom_hash: u64,
    pub labels_sorted_unique: bool,
    pub bad_utf8: bool,
    pub histograms: &'a [ConvertedHistogram],
    pub exemplars: &'a [ConvertedExemplarResult],
}

#[inline]
fn rollback_appender(user_id: &str, app: mimir_tsdb::head::HeadAppender<'_>) {
    if let Err(err) = app.rollback() {
        tracing::warn!(user = %user_id, err = %err, "failed to rollback appender on error");
    }
}

pub(crate) fn convert_borrowed_histogram(h: &BorrowedHistogramView<'_>) -> ConvertedHistogram {
    let ts = h.timestamp();
    let is_float = matches!(h.count(), Some(cortexpb::histogram::Count::CountFloat(_)));
    let hint = CounterResetHint::from_u8(h.reset_hint() as u8);
    let pos_spans: Vec<HistogramSpan> = h
        .positive_spans()
        .iter()
        .map(|s| HistogramSpan {
            offset: s.offset,
            length: s.length,
        })
        .collect();
    let neg_spans: Vec<HistogramSpan> = h
        .negative_spans()
        .iter()
        .map(|s| HistogramSpan {
            offset: s.offset,
            length: s.length,
        })
        .collect();

    if is_float {
        let count = match h.count() {
            Some(cortexpb::histogram::Count::CountFloat(v)) => v,
            Some(cortexpb::histogram::Count::CountInt(v)) => v as f64,
            None => 0.0,
        };
        let zero_count = match h.zero_count() {
            Some(cortexpb::histogram::ZeroCount::ZeroCountFloat(v)) => v,
            Some(cortexpb::histogram::ZeroCount::ZeroCountInt(_)) | None => 0.0,
        };
        let buckets = h.positive_counts().len()
            + h.negative_counts().len()
            + h.positive_deltas().len()
            + h.negative_deltas().len();
        let fh = FloatHistogram {
            counter_reset_hint: hint,
            schema: h.schema(),
            zero_threshold: h.zero_threshold(),
            zero_count,
            count,
            sum: h.sum(),
            positive_spans: pos_spans,
            negative_spans: neg_spans,
            positive_buckets: h.positive_counts().to_vec(),
            negative_buckets: h.negative_counts().to_vec(),
            custom_values: h.custom_values().to_vec(),
        };
        (ts, true, None, Some(fh), buckets)
    } else {
        let count = match h.count() {
            Some(cortexpb::histogram::Count::CountInt(v)) => v,
            Some(cortexpb::histogram::Count::CountFloat(v)) => v as u64,
            None => 0,
        };
        let zero_count = match h.zero_count() {
            Some(cortexpb::histogram::ZeroCount::ZeroCountInt(v)) => v,
            Some(cortexpb::histogram::ZeroCount::ZeroCountFloat(_)) | None => 0,
        };
        let buckets = h.positive_counts().len()
            + h.negative_counts().len()
            + h.positive_deltas().len()
            + h.negative_deltas().len();
        let ih = Histogram {
            counter_reset_hint: hint,
            schema: h.schema(),
            zero_threshold: h.zero_threshold(),
            zero_count,
            count,
            sum: h.sum(),
            positive_spans: pos_spans,
            negative_spans: neg_spans,
            positive_buckets: h.positive_deltas().to_vec(),
            negative_buckets: h.negative_deltas().to_vec(),
            custom_values: h.custom_values().to_vec(),
        };
        (ts, false, Some(ih), None, buckets)
    }
}

#[cold]
#[inline(never)]
fn format_raw_bytes_labels<'a>(pairs: impl IntoIterator<Item = (&'a [u8], &'a [u8])>) -> String {
    let collected: Vec<(&'a [u8], &'a [u8])> = pairs.into_iter().collect();
    let mut out = String::new();
    let mut metric_name_idx = None;
    for (i, &(k, v)) in collected.iter().enumerate() {
        if k == b"__name__" {
            out.push_str(&String::from_utf8_lossy(v));
            metric_name_idx = Some(i);
            break;
        }
    }
    let mut count = 0usize;
    for (i, &(k, v)) in collected.iter().enumerate() {
        if Some(i) == metric_name_idx {
            continue;
        }
        if count == 0 {
            out.push('{');
        } else {
            out.push_str(", ");
        }
        out.push_str(&String::from_utf8_lossy(k));
        out.push('=');
        mimir_proto::compat::append_go_quote_bytes(&mut out, v);
        count += 1;
    }
    if count > 0 {
        out.push('}');
    }
    if out.is_empty() {
        return "{}".to_string();
    }
    out
}

pub(crate) fn convert_borrowed_exemplar(
    ex: &BorrowedExemplarView<'_, '_>,
) -> ConvertedExemplarResult {
    let mut pairs = Vec::with_capacity(ex.labels().len());
    let mut utf8_err: Option<&'static str> = None;
    for lp in ex.labels() {
        let Ok(k) = lp.name_str() else {
            utf8_err = Some("invalid UTF-8 in exemplar label name");
            break;
        };
        let Ok(v) = lp.value_str() else {
            utf8_err = Some("invalid UTF-8 in exemplar label value");
            break;
        };
        pairs.push((k, v));
    }
    if let Some(msg) = utf8_err {
        let exemplar_labels =
            format_raw_bytes_labels(ex.labels().map(|lp| (lp.name, lp.value)));
        return Err(ConvertedExemplarError {
            timestamp_ms: ex.timestamp_ms(),
            message: msg.to_string(),
            exemplar_labels,
        });
    }
    let ex_str = format_labels_string(pairs.iter().copied());
    let labels = Labels::from_pairs(pairs);
    Ok((ex.timestamp_ms(), ex.value(), labels, ex_str))
}

/// Abstract view over a single time series in either owned `cortexpb::TimeSeries` or zero-copy `BorrowedTimeSeriesView`.
trait TimeSeriesInput<'a> {
    fn fill_pairs(&self, out: &mut Vec<(&'a str, &'a str)>) -> Result<(), IngesterError>;
    fn samples(&self) -> &[cortexpb::Sample];
    fn created_timestamp(&self) -> i64;
    fn histogram_count(&self) -> usize;
    fn histogram_timestamp(&self, idx: usize) -> i64;
    fn convert_histogram(&self, idx: usize) -> ConvertedHistogram;
    fn exemplar_count(&self) -> usize;
    fn exemplar_at(&self, idx: usize) -> ConvertedExemplarResult;
}

impl<'a> TimeSeriesInput<'a> for &'a cortexpb::TimeSeries {
    fn fill_pairs(&self, out: &mut Vec<(&'a str, &'a str)>) -> Result<(), IngesterError> {
        out.clear();
        for lp in &self.labels {
            let k = fast_utf8_str(&lp.name).map_err(|_| IngesterError::BadRequest {
                message: "invalid UTF-8 in label name".to_string(),
            })?;
            let v = fast_utf8_str(&lp.value).map_err(|_| IngesterError::BadRequest {
                message: "invalid UTF-8 in label value".to_string(),
            })?;
            out.push((k, v));
        }
        Ok(())
    }

    fn samples(&self) -> &[cortexpb::Sample] {
        &self.samples
    }

    fn created_timestamp(&self) -> i64 {
        self.created_timestamp
    }

    fn histogram_count(&self) -> usize {
        self.histograms.len()
    }

    fn histogram_timestamp(&self, idx: usize) -> i64 {
        self.histograms[idx].timestamp
    }

    fn convert_histogram(&self, idx: usize) -> ConvertedHistogram {
        let h = &self.histograms[idx];
        let ts = h.timestamp;
        let is_float = h.is_float_histogram();
        let hint = CounterResetHint::from_u8(h.reset_hint as u8);
        let pos_spans: Vec<HistogramSpan> = h
            .positive_spans
            .iter()
            .map(|s| HistogramSpan {
                offset: s.offset,
                length: s.length,
            })
            .collect();
        let neg_spans: Vec<HistogramSpan> = h
            .negative_spans
            .iter()
            .map(|s| HistogramSpan {
                offset: s.offset,
                length: s.length,
            })
            .collect();

        if is_float {
            let count = match h.count {
                Some(cortexpb::histogram::Count::CountFloat(v)) => v,
                Some(cortexpb::histogram::Count::CountInt(v)) => v as f64,
                None => 0.0,
            };
            let zero_count = match h.zero_count {
                Some(cortexpb::histogram::ZeroCount::ZeroCountFloat(v)) => v,
                Some(cortexpb::histogram::ZeroCount::ZeroCountInt(_)) | None => 0.0,
            };
            let buckets = h.positive_counts.len()
                + h.negative_counts.len()
                + h.positive_deltas.len()
                + h.negative_deltas.len();
            let fh = FloatHistogram {
                counter_reset_hint: hint,
                schema: h.schema,
                zero_threshold: h.zero_threshold,
                zero_count,
                count,
                sum: h.sum,
                positive_spans: pos_spans,
                negative_spans: neg_spans,
                positive_buckets: h.positive_counts.clone(),
                negative_buckets: h.negative_counts.clone(),
                custom_values: h.custom_values.clone(),
            };
            (ts, true, None, Some(fh), buckets)
        } else {
            let count = match h.count {
                Some(cortexpb::histogram::Count::CountInt(v)) => v,
                Some(cortexpb::histogram::Count::CountFloat(v)) => v as u64,
                None => 0,
            };
            let zero_count = match h.zero_count {
                Some(cortexpb::histogram::ZeroCount::ZeroCountInt(v)) => v,
                Some(cortexpb::histogram::ZeroCount::ZeroCountFloat(_)) | None => 0,
            };
            let buckets = h.positive_counts.len()
                + h.negative_counts.len()
                + h.positive_deltas.len()
                + h.negative_deltas.len();
            let ih = Histogram {
                counter_reset_hint: hint,
                schema: h.schema,
                zero_threshold: h.zero_threshold,
                zero_count,
                count,
                sum: h.sum,
                positive_spans: pos_spans,
                negative_spans: neg_spans,
                positive_buckets: h.positive_deltas.clone(),
                negative_buckets: h.negative_deltas.clone(),
                custom_values: h.custom_values.clone(),
            };
            (ts, false, Some(ih), None, buckets)
        }
    }

    fn exemplar_count(&self) -> usize {
        self.exemplars.len()
    }

    fn exemplar_at(&self, idx: usize) -> ConvertedExemplarResult {
        let ex = &self.exemplars[idx];
        let mut pairs = Vec::with_capacity(ex.labels.len());
        let mut utf8_err: Option<&'static str> = None;
        for lp in &ex.labels {
            let Ok(k) = lp.name_str() else {
                utf8_err = Some("invalid UTF-8 in exemplar label name");
                break;
            };
            let Ok(v) = lp.value_str() else {
                utf8_err = Some("invalid UTF-8 in exemplar label value");
                break;
            };
            pairs.push((k, v));
        }
        if let Some(msg) = utf8_err {
            let exemplar_labels = format_raw_bytes_labels(
                ex.labels
                    .iter()
                    .map(|lp| (lp.name.as_ref(), lp.value.as_ref())),
            );
            return Err(ConvertedExemplarError {
                timestamp_ms: ex.timestamp_ms,
                message: msg.to_string(),
                exemplar_labels,
            });
        }
        let ex_str = format_labels_string(pairs.iter().copied());
        let labels = Labels::from_pairs(pairs);
        Ok((ex.timestamp_ms, ex.value, labels, ex_str))
    }
}

impl<'a, 'v> TimeSeriesInput<'a> for BorrowedTimeSeriesView<'v, 'a> {
    fn fill_pairs(&self, out: &mut Vec<(&'a str, &'a str)>) -> Result<(), IngesterError> {
        out.clear();
        if let Some(labels_str) = self.labels_str() {
            out.extend(labels_str);
            return Ok(());
        }
        for lp in self.labels() {
            let k = fast_utf8_str(lp.name).map_err(|_| IngesterError::BadRequest {
                message: "invalid UTF-8 in label name".to_string(),
            })?;
            let v = fast_utf8_str(lp.value).map_err(|_| IngesterError::BadRequest {
                message: "invalid UTF-8 in label value".to_string(),
            })?;
            out.push((k, v));
        }
        Ok(())
    }

    fn samples(&self) -> &[cortexpb::Sample] {
        self.samples()
    }

    fn created_timestamp(&self) -> i64 {
        self.created_timestamp()
    }

    fn histogram_count(&self) -> usize {
        self.histograms().len()
    }

    fn histogram_timestamp(&self, idx: usize) -> i64 {
        self.histograms()
            .nth(idx)
            .map_or(0, |h| h.timestamp())
    }

    fn convert_histogram(&self, idx: usize) -> ConvertedHistogram {
        let h = self.histograms().nth(idx).expect("valid histogram idx");
        convert_borrowed_histogram(&h)
    }

    fn exemplar_count(&self) -> usize {
        self.exemplars().len()
    }

    fn exemplar_at(&self, idx: usize) -> ConvertedExemplarResult {
        let ex = self.exemplars().nth(idx).expect("valid exemplar idx");
        convert_borrowed_exemplar(&ex)
    }
}

impl Ingester {
    pub async fn start_push_request_with_state<'a>(
        &'a self,
        existing: Option<&PushPermit<'a>>,
        req_size: i64,
    ) -> Result<(Option<PushPermit<'a>>, bool), IngesterError> {
        self.check_available_for_push()?;
        if existing.is_some() {
            return Ok((None, false));
        }
        let permit = self.start_push_request(req_size).await?;
        Ok((Some(permit), true))
    }

    pub async fn start_push_request(&self, req_size: i64) -> Result<PushPermit<'_>, IngesterError> {
        self.check_available_for_push()?;

        let started_at = Instant::now();
        let breaker_permit = self.circuit_breaker.try_acquire_push_permit()?;

        if let Some(ref rl) = self.reactive_limiter.push {
            if !rl.can_acquire_permit() {
                self.metrics
                    .rejected
                    .with_label_values(&[REASON_MAX_INFLIGHT_PUSH_REQUESTS])
                    .inc();
                return Err(IngesterError::ReactiveLimiterExceeded {
                    message: ADAPTIVE_LIMITER_EXCEEDED_MSG.to_string(),
                });
            }
        }

        let reactive_permit = if let Some(ref rl) = self.reactive_limiter.push {
            match rl.acquire_permit().await {
                Ok(p) => Some(p),
                Err(e) => {
                    self.metrics
                        .rejected
                        .with_label_values(&[REASON_MAX_INFLIGHT_PUSH_REQUESTS])
                        .inc();
                    return Err(IngesterError::ReactiveLimiterExceeded {
                        message: e.to_string(),
                    });
                }
            }
        } else {
            None
        };

        let inflight = self
            .inflight_push_requests
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
            + 1;
        let (inflight_bytes, reject_equal) = if req_size > 0 {
            let b = self
                .inflight_push_requests_bytes
                .fetch_add(req_size, std::sync::atomic::Ordering::AcqRel)
                + req_size;
            (b, false)
        } else {
            (
                self.inflight_push_requests_bytes
                    .load(std::sync::atomic::Ordering::Acquire),
                true,
            )
        };

        let permit = PushPermit {
            started_at,
            breaker_permit,
            reactive_permit,
            inflight_permit: None,
            ingester: self,
            req_size,
            finished: false,
        };

        if let Err(err) = self.check_instance_limits(inflight, inflight_bytes, reject_equal) {
            permit.finish(Some(&err));
            return Err(err);
        }

        Ok(permit)
    }

    /// Non-blocking gRPC `TapHandle` admission (`Ingester.StartPushRequest` + `grpc_call_check.go`).
    pub fn try_start_push_tap(self: &Arc<Self>, req_size: i64) -> Result<PushTapPermit, IngesterError> {
        self.check_available_for_push()?;
        let started_at = Instant::now();
        let breaker_permit = self.circuit_breaker.try_acquire_push_permit()?;
        if let Some(ref rl) = self.reactive_limiter.push {
            if !rl.can_acquire_permit() {
                self.metrics
                    .rejected
                    .with_label_values(&[REASON_MAX_INFLIGHT_PUSH_REQUESTS])
                    .inc();
                return Err(IngesterError::ReactiveLimiterExceeded {
                    message: ADAPTIVE_LIMITER_EXCEEDED_MSG.to_string(),
                });
            }
        }
        let inflight = self
            .inflight_push_requests
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
            + 1;
        let (inflight_bytes, reject_equal) = if req_size > 0 {
            let b = self
                .inflight_push_requests_bytes
                .fetch_add(req_size, std::sync::atomic::Ordering::AcqRel)
                + req_size;
            (b, false)
        } else {
            (
                self.inflight_push_requests_bytes
                    .load(std::sync::atomic::Ordering::Acquire),
                true,
            )
        };
        let permit = PushTapPermit {
            started_at,
            breaker_permit,
            reactive_permit: None,
            ingester: Arc::clone(self),
            req_size,
            finished: false,
        };
        if let Err(err) = self.check_instance_limits(inflight, inflight_bytes, reject_equal) {
            permit.finish_with_duration(Duration::ZERO, Some(&err));
            return Err(err);
        }
        Ok(permit)
    }

    /// Blocking reactive limiter permit acquisition (`Ingester.PreparePushRequest`).
    pub async fn prepare_push_reactive(&self) -> Result<Option<ReactivePermit>, IngesterError> {
        let Some(ref rl) = self.reactive_limiter.push else {
            return Ok(None);
        };
        match rl.acquire_permit().await {
            Ok(p) => Ok(Some(p)),
            Err(e) => {
                self.metrics
                    .rejected
                    .with_label_values(&[REASON_MAX_INFLIGHT_PUSH_REQUESTS])
                    .inc();
                Err(IngesterError::ReactiveLimiterExceeded {
                    message: e.to_string(),
                })
            }
        }
    }

    fn start_push_request_sync(&self, req_size: i64) -> Result<PushPermit<'_>, IngesterError> {
        self.check_available_for_push()?;
        let started_at = Instant::now();
        let breaker_permit = self.circuit_breaker.try_acquire_push_permit()?;
        if let Some(ref rl) = self.reactive_limiter.push {
            if !rl.can_acquire_permit() {
                self.metrics
                    .rejected
                    .with_label_values(&[REASON_MAX_INFLIGHT_PUSH_REQUESTS])
                    .inc();
                return Err(IngesterError::ReactiveLimiterExceeded {
                    message: ADAPTIVE_LIMITER_EXCEEDED_MSG.to_string(),
                });
            }
        }
        let inflight = self
            .inflight_push_requests
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
            + 1;
        let (inflight_bytes, reject_equal) = if req_size > 0 {
            let b = self
                .inflight_push_requests_bytes
                .fetch_add(req_size, std::sync::atomic::Ordering::AcqRel)
                + req_size;
            (b, false)
        } else {
            (
                self.inflight_push_requests_bytes
                    .load(std::sync::atomic::Ordering::Acquire),
                true,
            )
        };
        let permit = PushPermit {
            started_at,
            breaker_permit,
            reactive_permit: None,
            inflight_permit: None,
            ingester: self,
            req_size,
            finished: false,
        };
        if let Err(err) = self.check_instance_limits(inflight, inflight_bytes, reject_equal) {
            permit.finish(Some(&err));
            return Err(err);
        }
        Ok(permit)
    }

    fn check_instance_limits(
        &self,
        inflight: i64,
        inflight_bytes: i64,
        reject_equal_bytes: bool,
    ) -> Result<(), IngesterError> {
        let Some(il) = self.get_instance_limits() else {
            return Ok(());
        };
        if il.max_inflight_push_requests > 0 && inflight > il.max_inflight_push_requests {
            self.metrics
                .rejected
                .with_label_values(&[REASON_MAX_INFLIGHT_PUSH_REQUESTS])
                .inc();
            return Err(IngesterError::max_inflight_requests_reached());
        }
        if il.max_inflight_push_requests_bytes > 0
            && ((reject_equal_bytes && inflight_bytes >= il.max_inflight_push_requests_bytes)
                || inflight_bytes > il.max_inflight_push_requests_bytes)
        {
            self.metrics
                .rejected
                .with_label_values(&[REASON_MAX_INFLIGHT_PUSH_REQUESTS_BYTES])
                .inc();
            return Err(IngesterError::max_inflight_requests_bytes_reached());
        }
        if il.max_ingestion_rate > 0.0 && self.ingestion_rate.rate() >= il.max_ingestion_rate {
            self.metrics
                .rejected
                .with_label_values(&[REASON_MAX_INGESTION_RATE])
                .inc();
            return Err(IngesterError::max_ingestion_rate_reached());
        }
        Ok(())
    }

    /// Zero-copy raw protobuf gRPC `Push` entrypoint.
    ///
    /// Offloads non-trivial payloads (`>= 2048` bytes) onto Tokio's blocking worker pool so
    /// `mimir-rpc` async I/O threads never stall behind TSDB + WAL writes, and encodes labels
    /// directly into `stringlabels` + `xxh64` to reuse the Wave P6/P7 prehashed batch path.
    pub async fn push_grpc_raw(
        self: &Arc<Self>,
        user_id: &str,
        raw_bytes: Bytes,
    ) -> Result<(), IngesterError> {
        if !self.cfg.push_grpc_method_enabled {
            return Err(IngesterError::PushGrpcDisabled);
        }
        let req_size = raw_bytes.len() as i64;
        let permit = self.start_push_request(req_size).await?;
        let res = self.push_grpc_raw_inner(user_id, raw_bytes).await;
        permit.finish(res.as_ref().err());
        res
    }

    /// Zero-copy raw protobuf gRPC `Push` entrypoint when admission was already performed by
    /// the pre-decompression gRPC tap (`IngesterZeroCopyServer`).
    ///
    /// Owns `permit` through the full push execution (including across `tokio::task::spawn_blocking`
    /// for payloads `>= 2048` bytes) and finishes it with the handler-measured duration (matching
    /// Go's `PushWithCleanup` + `FinishPushRequest`) even if the awaiting gRPC future is dropped
    /// at a deadline or client reset.
    pub async fn push_grpc_raw_admitted(
        self: &Arc<Self>,
        user_id: &str,
        raw_bytes: Bytes,
        permit: PushTapPermit,
    ) -> Result<(), IngesterError> {
        let handler_started = Instant::now();
        if !self.cfg.push_grpc_method_enabled {
            let err = IngesterError::PushGrpcDisabled;
            permit.finish_with_duration(handler_started.elapsed(), Some(&err));
            return Err(err);
        }
        if raw_bytes.len() >= 2048 {
            let ing = Arc::clone(self);
            let uid = user_id.to_owned();
            tokio::task::spawn_blocking(move || {
                let res = ing.push_grpc_raw_sync(&uid, &raw_bytes);
                let dur = handler_started.elapsed();
                permit.finish_with_duration(dur, res.as_ref().err());
                res
            })
            .await
            .map_err(|e| IngesterError::Internal {
                message: format!("push worker join error: {e}"),
            })?
        } else {
            let res = self.push_grpc_raw_sync(user_id, &raw_bytes);
            let dur = handler_started.elapsed();
            permit.finish_with_duration(dur, res.as_ref().err());
            res
        }
    }

    /// Validates classic gRPC Push wire bytes without ingesting, matching Go's
    /// `dec(in) -> tenant interceptor` order when tenant metadata is absent or invalid.
    pub fn validate_grpc_push_wire(&self, raw_bytes: &[u8]) -> Result<(), IngesterError> {
        let mut wr_scratch = mimir_proto::zerocopy::WriteRequestScratch::new();
        let mut label_bytes_arena = Vec::new();
        let mut samples_arena = Vec::new();
        mimir_proto::zerocopy::decode_classic_stream(
            raw_bytes,
            mimir_proto::zerocopy::DecodeOptions::classic(),
            &mut wr_scratch,
            &mut label_bytes_arena,
            &mut samples_arena,
            |_| {},
        )
        .map(|_| ())
        .map_err(|e| IngesterError::Internal {
            message: format!("grpc: failed to unmarshal the received message: {e}"),
        })
    }

    async fn push_grpc_raw_inner(
        self: &Arc<Self>,
        user_id: &str,
        raw_bytes: Bytes,
    ) -> Result<(), IngesterError> {
        if raw_bytes.len() >= 2048 {
            let ing = Arc::clone(self);
            let uid = user_id.to_owned();
            tokio::task::spawn_blocking(move || ing.push_grpc_raw_sync(&uid, &raw_bytes))
                .await
                .map_err(|e| IngesterError::Internal {
                    message: format!("push worker join error: {e}"),
                })?
        } else {
            self.push_grpc_raw_sync(user_id, &raw_bytes)
        }
    }

    fn push_grpc_raw_sync(&self, user_id: &str, raw_bytes: &[u8]) -> Result<(), IngesterError> {
        struct ClassicSeriesMeta {
            label_start: u32,
            label_len: u32,
            raw_label_start: u32,
            raw_label_len: u32,
            sample_start: u32,
            sample_len: u32,
            created_timestamp: i64,
            prom_hash: u64,
            labels_sorted_unique: bool,
            bad_utf8: bool,
            hist_start: u32,
            hist_len: u32,
            ex_start: u32,
            ex_len: u32,
        }

        struct ClassicPushScratch {
            wr_scratch: mimir_proto::zerocopy::WriteRequestScratch,
            label_bytes_arena: Vec<u8>,
            samples_arena: Vec<cortexpb::Sample>,
            histograms_arena: Vec<ConvertedHistogram>,
            exemplars_arena: Vec<ConvertedExemplarResult>,
            series_metas: Vec<ClassicSeriesMeta>,
        }

        impl ClassicPushScratch {
            fn new() -> Self {
                Self {
                    wr_scratch: mimir_proto::zerocopy::WriteRequestScratch::new(),
                    label_bytes_arena: Vec::with_capacity(64 * 1024),
                    samples_arena: Vec::with_capacity(2048),
                    histograms_arena: Vec::new(),
                    exemplars_arena: Vec::new(),
                    series_metas: Vec::with_capacity(2048),
                }
            }

            fn clear(&mut self) {
                self.wr_scratch.clear();
                self.label_bytes_arena.clear();
                self.samples_arena.clear();
                self.histograms_arena.clear();
                self.exemplars_arena.clear();
                self.series_metas.clear();
                if self.label_bytes_arena.capacity() > 4 * 1024 * 1024 {
                    self.label_bytes_arena.shrink_to(256 * 1024);
                }
                if self.samples_arena.capacity() > 256 * 1024 {
                    self.samples_arena.shrink_to(16 * 1024);
                }
            }
        }

        struct ScratchResetGuard<'a>(&'a mut ClassicPushScratch);
        impl Drop for ScratchResetGuard<'_> {
            fn drop(&mut self) {
                self.0.clear();
            }
        }

        thread_local! {
            static PUSH_SCRATCH: std::cell::RefCell<ClassicPushScratch> =
                std::cell::RefCell::new(ClassicPushScratch::new());
        }

        PUSH_SCRATCH.with(|cell| {
            let mut scratch_ref = cell.borrow_mut();
            let guard = ScratchResetGuard(&mut scratch_ref);
            let s = &mut *guard.0;

            let mut min_ts = i64::MAX;
            let mut all_sorted_unique = true;
            let mut all_sorted_have_metric_name = true;

            let ClassicPushScratch {
                ref mut wr_scratch,
                ref mut label_bytes_arena,
                ref mut samples_arena,
                ref mut histograms_arena,
                ref mut exemplars_arena,
                ref mut series_metas,
            } = *s;

            let stream_res = mimir_proto::zerocopy::decode_classic_stream(
                raw_bytes,
                mimir_proto::zerocopy::DecodeOptions::classic(),
                wr_scratch,
                label_bytes_arena,
                samples_arena,
                |series| {
                    if series.bad_utf8 || !series.labels_sorted_unique {
                        all_sorted_unique = false;
                    } else if !iter_stringlabels(series.encoded_labels).any(|(k, _)| k == "__name__")
                    {
                        all_sorted_have_metric_name = false;
                    }
                    min_ts = min_ts.min(series.min_sample_ts);

                    let hist_start = histograms_arena.len() as u32;
                    let mut hist_len = 0u32;
                    for h in series.histograms() {
                        let conv = convert_borrowed_histogram(&h);
                        min_ts = min_ts.min(conv.0);
                        histograms_arena.push(conv);
                        hist_len += 1;
                    }

                    let ex_start = exemplars_arena.len() as u32;
                    let mut ex_len = 0u32;
                    for ex in series.exemplars() {
                        let res = convert_borrowed_exemplar(&ex);
                        if let Ok(ref conv) = res {
                            min_ts = min_ts.min(conv.0);
                        }
                        exemplars_arena.push(res);
                        ex_len += 1;
                    }

                    let (raw_label_start, raw_label_len) = match series.raw_label_bytes_range {
                        Some(ref rr) if !series.bad_utf8 => (rr.start as u32, rr.len() as u32),
                        _ if series.bad_utf8 => (0, 0),
                        _ => (
                            series.label_bytes_range.start as u32,
                            series.label_bytes_range.len() as u32,
                        ),
                    };
                    series_metas.push(ClassicSeriesMeta {
                        label_start: series.label_bytes_range.start as u32,
                        label_len: if series.bad_utf8 {
                            0
                        } else {
                            series.label_bytes_range.len() as u32
                        },
                        raw_label_start,
                        raw_label_len,
                        sample_start: series.samples_range.start as u32,
                        sample_len: series.samples_range.len() as u32,
                        created_timestamp: series.created_timestamp,
                        prom_hash: series.prom_hash,
                        labels_sorted_unique: series.labels_sorted_unique,
                        bad_utf8: series.bad_utf8,
                        hist_start,
                        hist_len,
                        ex_start,
                        ex_len,
                    });
                },
            );

            let header = match stream_res {
                Ok(h) => h,
                Err(e) => {
                    return Err(IngesterError::Internal {
                        message: format!("grpc: failed to unmarshal the received message: {e}"),
                    });
                }
            };

            let meta_count = self.push_metadata_iter(user_id, wr_scratch.metadata(raw_bytes));
            if meta_count > 0 {
                self.ingestion_rate.add(meta_count as i64);
            }

            let ts_len = series_metas.len();
            if ts_len == 0 {
                return Ok(());
            }

            let source = cortexpb::write_request::SourceEnum::try_from(header.source)
                .unwrap_or(cortexpb::write_request::SourceEnum::Api);
            let _skip_label_validation = header.skip_label_validation;
            let _skip_label_count_validation = header.skip_label_count_validation;

            let mut items: Vec<PrehashedSeriesItem<'_>> = Vec::with_capacity(ts_len);
            for m in series_metas.iter() {
                let l_start = m.label_start as usize;
                let encoded_labels = if m.bad_utf8 {
                    &[][..]
                } else {
                    &label_bytes_arena[l_start..l_start + m.label_len as usize]
                };
                let r_start = m.raw_label_start as usize;
                let raw_encoded_labels = if m.bad_utf8 {
                    &[][..]
                } else {
                    &label_bytes_arena[r_start..r_start + m.raw_label_len as usize]
                };
                let s_start = m.sample_start as usize;
                let samples = &samples_arena[s_start..s_start + m.sample_len as usize];
                let histograms = if m.hist_len == 0 {
                    &[][..]
                } else {
                    let h_start = m.hist_start as usize;
                    &histograms_arena[h_start..h_start + m.hist_len as usize]
                };
                let exemplars = if m.ex_len == 0 {
                    &[][..]
                } else {
                    let e_start = m.ex_start as usize;
                    &exemplars_arena[e_start..e_start + m.ex_len as usize]
                };
                items.push(PrehashedSeriesItem {
                    encoded_labels,
                    raw_encoded_labels,
                    samples,
                    created_timestamp: m.created_timestamp,
                    prom_hash: m.prom_hash,
                    labels_sorted_unique: m.labels_sorted_unique,
                    bad_utf8: m.bad_utf8,
                    histograms,
                    exemplars,
                });
            }

            if ts_len >= CLASSIC_PUSH_SHARD_THRESHOLD
                && all_sorted_unique
                && all_sorted_have_metric_name
                && !self.verify_ring_ownership.load(Ordering::Relaxed)
                && self
                    .cost_attribution_manager()
                    .and_then(|m| m.sample_tracker(user_id))
                    .is_none()
                && !self
                    .get_instance_limits()
                    .is_some_and(|il| il.max_series > 0)
            {
                self.ingest_prehashed_classic_sharded(user_id, source, min_ts, &items)
            } else {
                self.ingest_prehashed_shard_batch_inner(user_id, source, min_ts, &items)
            }
        })
    }

    /// gRPC `Push` entrypoint.
    pub async fn push_grpc(
        &self,
        user_id: &str,
        req: &cortexpb::WriteRequest,
    ) -> Result<(), IngesterError> {
        if !self.cfg.push_grpc_method_enabled {
            return Err(IngesterError::PushGrpcDisabled);
        }
        let req_size = req.encoded_len() as i64;
        let permit = self.start_push_request(req_size).await?;
        let res = self.push_write_request(user_id, req).await;
        permit.finish(res.as_ref().err());
        res
    }

    /// Zero-copy `BorrowedWriteRequest` entrypoint used by Kafka `ingest_storage` and custom codecs.
    pub fn push_borrowed_to_storage(
        &self,
        user_id: &str,
        req: &BorrowedWriteRequest<'_, '_>,
        req_size: i64,
    ) -> Result<(), IngesterError> {
        let permit = self.start_push_request_sync(req_size)?;
        let res = self.push_borrowed_inner(user_id, req);
        permit.finish(res.as_ref().err());
        res
    }

    /// Sharded `ingest_storage` batch push entrypoint (`PushToStorageAndReleaseRequest`).
    pub fn push_to_storage(
        &self,
        user_id: &str,
        req: &cortexpb::WriteRequest,
    ) -> Result<(), IngesterError> {
        let track_bytes = self
            .get_instance_limits()
            .is_some_and(|il| il.max_inflight_push_requests_bytes > 0);
        let req_size = if track_bytes {
            req.encoded_len() as i64
        } else {
            0
        };
        let permit = self.start_push_request_sync(req_size)?;
        let res = self.push_write_request_sync(user_id, req);
        permit.finish(res.as_ref().err());
        res
    }

    pub async fn push_write_request(
        &self,
        user_id: &str,
        req: &cortexpb::WriteRequest,
    ) -> Result<(), IngesterError> {
        self.push_write_request_sync(user_id, req)
    }

    pub fn push_write_request_sync(
        &self,
        user_id: &str,
        req: &cortexpb::WriteRequest,
    ) -> Result<(), IngesterError> {
        if !req.timeseries_rw2.is_empty() || !req.symbols_rw2.is_empty() {
            let mut buf = Vec::with_capacity(req.encoded_len());
            req.encode(&mut buf).map_err(|e| IngesterError::Internal {
                message: format!("failed to encode RW2 request: {e}"),
            })?;
            let mut scratch = mimir_proto::zerocopy::WriteRequestScratch::new();
            let borrowed = mimir_proto::zerocopy::decode_write_request(
                &buf,
                mimir_proto::zerocopy::DecodeOptions::rw2(),
                &mut scratch,
            )
            .map_err(|e| IngesterError::BadRequest {
                message: e.to_string(),
            })?;
            return self.push_borrowed_inner(user_id, &borrowed);
        }

        let meta_count = self.push_metadata_owned(user_id, &req.metadata);
        if meta_count > 0 {
            self.ingestion_rate.add(meta_count as i64);
        }
        if req.timeseries.is_empty() {
            return Ok(());
        }

        let db = self
            .get_or_create_tsdb(user_id)
            .map_err(|e| {
                let wrapped = e.with_user(user_id, 0);
                if wrapped.error_cause() == cortexpb::ErrorCause::InstanceLimit {
                    wrapped.with_do_not_log()
                } else {
                    wrapped
                }
            })?;
        let min_ts = min_timestamp_owned(&req.timeseries);
        let lock_guard = db
            .acquire_append_lock(min_ts)
            .map_err(|e| e.with_user(user_id, 0))?;

        let source = cortexpb::write_request::SourceEnum::try_from(req.source)
            .unwrap_or(cortexpb::write_request::SourceEnum::Api);
        let ts_len = req.timeseries.len();
        let iter = req.timeseries.iter();
        let res = self.ingest_timeseries(
            user_id,
            &db,
            source,
            req.skip_label_validation,
            req.skip_label_count_validation,
            ts_len,
            iter,
        );
        drop(lock_guard);
        res.map_err(|e| {
            if e.error_cause() == cortexpb::ErrorCause::InstanceLimit {
                e.with_do_not_log()
            } else {
                e
            }
        })
    }

    fn push_borrowed_inner(
        &self,
        user_id: &str,
        req: &BorrowedWriteRequest<'_, '_>,
    ) -> Result<(), IngesterError> {
        let meta_count = self.push_metadata_borrowed(user_id, req);
        if meta_count > 0 {
            self.ingestion_rate.add(meta_count as i64);
        }
        if req.timeseries_len() == 0 {
            return Ok(());
        }

        let db = self
            .get_or_create_tsdb(user_id)
            .map_err(|e| {
                let wrapped = e.with_user(user_id, 0);
                if wrapped.error_cause() == cortexpb::ErrorCause::InstanceLimit {
                    wrapped.with_do_not_log()
                } else {
                    wrapped
                }
            })?;
        let mut min_ts = i64::MAX;
        for ts in req.timeseries() {
            for s in ts.samples() {
                min_ts = min_ts.min(s.timestamp_ms);
            }
            for h in ts.histograms() {
                min_ts = min_ts.min(h.timestamp());
            }
            for ex in ts.exemplars() {
                min_ts = min_ts.min(ex.timestamp_ms());
            }
        }
        let lock_guard = db
            .acquire_append_lock(min_ts)
            .map_err(|e| e.with_user(user_id, 0))?;

        let source = cortexpb::write_request::SourceEnum::try_from(req.source)
            .unwrap_or(cortexpb::write_request::SourceEnum::Api);
        let ts_len = req.timeseries_len();
        let res = self.ingest_timeseries(
            user_id,
            &db,
            source,
            req.skip_label_validation,
            req.skip_label_count_validation,
            ts_len,
            req.timeseries(),
        );
        drop(lock_guard);
        res.map_err(|e| {
            if e.error_cause() == cortexpb::ErrorCause::InstanceLimit {
                e.with_do_not_log()
            } else {
                e
            }
        })
    }

    fn push_metadata_owned(
        &self,
        user_id: &str,
        metadata: &[cortexpb::MetricMetadata],
    ) -> usize {
        let store = self.get_or_create_user_metadata(user_id);
        if metadata.is_empty() {
            return 0;
        }
        let now_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos().min(i64::MAX as u128) as i64)
            .unwrap_or(0);
        let mut ingested = 0usize;
        let mut failed = 0usize;
        let mut first_err: Option<IngesterError> = None;
        for m in metadata {
            match store.add_parts_at(
                m.r#type,
                &m.metric_family_name,
                &m.help,
                &m.unit,
                now_nanos,
            ) {
                Ok(()) => ingested += 1,
                Err(e) => {
                    failed += 1;
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                }
            }
        }
        self.metrics.ingested_metadata.inc_by(ingested as f64);
        self.metrics.ingested_metadata_fail.inc_by(failed as f64);
        if let Some(err) = first_err {
            let (trace_id, sampled) = mimir_common::tracing::current_sampled_trace_id();
            if sampled && !trace_id.is_empty() {
                warn!(user = %user_id, traceID = %trace_id, err = %err, "failed to ingest some metadata");
            } else {
                warn!(user = %user_id, err = %err, "failed to ingest some metadata");
            }
        }
        ingested
    }

    pub(crate) fn push_metadata_borrowed(
        &self,
        user_id: &str,
        req: &BorrowedWriteRequest<'_, '_>,
    ) -> usize {
        self.push_metadata_iter(user_id, req.metadata())
    }

    pub(crate) fn push_metadata_iter<'a>(
        &self,
        user_id: &str,
        metadata: impl ExactSizeIterator<Item = mimir_proto::zerocopy::BorrowedMetadata<'a>>,
    ) -> usize {
        let store = self.get_or_create_user_metadata(user_id);
        if metadata.len() == 0 {
            return 0;
        }
        let now_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos().min(i64::MAX as u128) as i64)
            .unwrap_or(0);
        let mut ingested = 0usize;
        let mut failed = 0usize;
        let mut first_err: Option<IngesterError> = None;
        for bm in metadata {
            match store.add_parts_at(
                bm.metric_type,
                bm.metric_family_name,
                bm.help,
                bm.unit,
                now_nanos,
            ) {
                Ok(()) => ingested += 1,
                Err(e) => {
                    failed += 1;
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                }
            }
        }
        self.metrics.ingested_metadata.inc_by(ingested as f64);
        self.metrics.ingested_metadata_fail.inc_by(failed as f64);
        if let Some(err) = first_err {
            let (trace_id, sampled) = mimir_common::tracing::current_sampled_trace_id();
            if sampled && !trace_id.is_empty() {
                warn!(user = %user_id, traceID = %trace_id, err = %err, "failed to ingest some metadata");
            } else {
                warn!(user = %user_id, err = %err, "failed to ingest some metadata");
            }
        }
        ingested
    }

    pub(crate) fn push_metadata_items_with_trace(
        &self,
        user_id: &str,
        items: &[BorrowedMetadata<'_>],
        explicit_sampled_trace_id: Option<&str>,
    ) -> usize {
        let store = self.get_or_create_user_metadata(user_id);
        if items.is_empty() {
            return 0;
        }
        let now_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos().min(i64::MAX as u128) as i64)
            .unwrap_or(0);
        let mut ingested = 0usize;
        let mut failed = 0usize;
        let mut first_err: Option<IngesterError> = None;
        for bm in items {
            match store.add_parts_at(
                bm.metric_type,
                bm.metric_family_name,
                bm.help,
                bm.unit,
                now_nanos,
            ) {
                Ok(()) => ingested += 1,
                Err(e) => {
                    failed += 1;
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                }
            }
        }
        self.metrics.ingested_metadata.inc_by(ingested as f64);
        self.metrics.ingested_metadata_fail.inc_by(failed as f64);
        if let Some(err) = first_err {
            if let Some(trace_id) = explicit_sampled_trace_id.filter(|s| !s.is_empty()) {
                warn!(user = %user_id, traceID = %trace_id, err = %err, "failed to ingest some metadata");
            } else {
                let (trace_id, sampled) = mimir_common::tracing::current_sampled_trace_id();
                if sampled && !trace_id.is_empty() {
                    warn!(user = %user_id, traceID = %trace_id, err = %err, "failed to ingest some metadata");
                } else {
                    warn!(user = %user_id, err = %err, "failed to ingest some metadata");
                }
            }
        }
        ingested
    }

    #[doc(hidden)]
    pub fn ingest_prehashed_shard_batch_with_trace(
        &self,
        user_id: &str,
        source: cortexpb::write_request::SourceEnum,
        min_ts: i64,
        items: &[PrehashedSeriesItem<'_>],
        metadata: &[BorrowedMetadata<'_>],
        sampled_trace_id: Option<&str>,
    ) -> Result<(), IngesterError> {
        if items.is_empty() && metadata.is_empty() {
            return Ok(());
        }
        let track_bytes = self
            .get_instance_limits()
            .is_some_and(|il| il.max_inflight_push_requests_bytes > 0);
        let req_size = if track_bytes {
            items
                .iter()
                .map(|it| {
                    it.encoded_labels.len()
                        + it.samples.len() * 16
                        + it.histograms.len() * 64
                        + it.exemplars.len() * 48
                })
                .chain(metadata.iter().map(|m| {
                    m.metric_family_name.len() + m.help.len() + m.unit.len() + 8
                }))
                .sum::<usize>() as i64
        } else {
            0
        };
        let permit = self.start_push_request_sync(req_size)?;
        // Go `PushWithCleanup`: metadata is best-effort and ingested before samples, under the
        // same `startPushRequest`; metadata-only requests return before touching the TSDB.
        let meta_count = self.push_metadata_items_with_trace(user_id, metadata, sampled_trace_id);
        if meta_count > 0 {
            self.ingestion_rate.add(meta_count as i64);
        }
        let res = if items.is_empty() {
            Ok(())
        } else {
            self.ingest_prehashed_shard_batch_inner(user_id, source, min_ts, items)
        };
        permit.finish(res.as_ref().err());
        res
    }

    fn ingest_prehashed_classic_sharded(
        &self,
        user_id: &str,
        source: cortexpb::write_request::SourceEnum,
        min_ts: i64,
        items: &[PrehashedSeriesItem<'_>],
    ) -> Result<(), IngesterError> {
        let hw_threads = std::thread::available_parallelism().map_or(4, |n| n.get());
        let max_shards = hw_threads.clamp(2, 8);
        let desired = (items.len() / 128).clamp(2, max_shards);
        let num_shards = 1usize << (usize::BITS - 1 - desired.leading_zeros());
        if num_shards <= 1 {
            return self.ingest_prehashed_shard_batch_inner(user_id, source, min_ts, items);
        }
        let shard_mask = num_shards - 1;

        struct ShardItemRef<'a, 'b> {
            orig_idx: usize,
            item: &'b PrehashedSeriesItem<'a>,
        }

        let mut buckets: SmallVec<[Vec<ShardItemRef<'_, '_>>; 8]> = (0..num_shards)
            .map(|_| Vec::with_capacity((items.len() / num_shards) + 8))
            .collect();
        for (idx, item) in items.iter().enumerate() {
            let shard = (item.prom_hash as usize) & shard_mask;
            buckets[shard].push(ShardItemRef {
                orig_idx: idx,
                item,
            });
        }
        let active_shards: SmallVec<[Vec<ShardItemRef<'_, '_>>; 8]> =
            buckets.into_iter().filter(|b| !b.is_empty()).collect();
        if active_shards.len() <= 1 {
            return self.ingest_prehashed_shard_batch_inner(user_id, source, min_ts, items);
        }

        let db = self.get_or_create_tsdb(user_id).map_err(|e| {
            let wrapped = e.with_user(user_id, 0);
            if wrapped.error_cause() == cortexpb::ErrorCause::InstanceLimit {
                wrapped.with_do_not_log()
            } else {
                wrapped
            }
        })?;
        let lock_guard = db
            .acquire_append_lock(min_ts)
            .map_err(|e| e.with_user(user_id, 0))?;

        let start_append = SystemTime::now();
        let start_append_ms = start_append
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        let add_timer = Instant::now();

        let limits = self.overrides.for_tenant(user_id);
        let native_histograms_enabled = limits.native_histograms_ingestion_enabled;
        let creation_grace_ms = (limits.creation_grace_period.0 / 1_000_000).max(0);
        let past_grace_ms = (limits.past_grace_period.0 / 1_000_000).max(0);
        let ooo_window_ms = (limits.out_of_order_time_window.0 / 1_000_000).max(0);
        let ooo_window_dur = Duration::from_millis(ooo_window_ms as u64);

        let max_timestamp_ms = start_append_ms.saturating_add(creation_grace_ms);
        let min_timestamp_ms = if past_grace_ms > 0 {
            start_append_ms
                .saturating_sub(past_grace_ms)
                .saturating_sub(ooo_window_ms)
        } else {
            i64::MIN
        };

        let head = db.db.head();
        let (cur_series_count, min_local_limit) = db.get_series_count_and_min_local_limit();
        let user_series_limit = db.limiter.max_series_per_user(user_id, min_local_limit);
        let metric_series_limit = db.limiter.max_series_per_metric(user_id);
        let may_cross_user_limit = user_series_limit < i32::MAX as usize
            && cur_series_count.saturating_add(items.len()) > user_series_limit;
        let may_cross_metric_limit = metric_series_limit < i32::MAX as usize;
        let has_cost_attribution = self
            .cost_attribution_manager()
            .and_then(|m| m.sample_tracker(user_id))
            .is_some();
        if !head.initialized()
            || may_cross_user_limit
            || may_cross_metric_limit
            || has_cost_attribution
        {
            return self.ingest_prehashed_shard_batch_with_lock(
                user_id,
                &db,
                lock_guard,
                source,
                items,
            );
        }
        let verify_ownership = self.verify_ring_ownership.load(Ordering::Relaxed);

        let min_append_time_available = head.initialized();
        let min_append_time = head.appendable_min_valid_time();
        let max_exemplars = self.limiter.max_exemplars_per_user(user_id);
        let ignore_ooo_exemplars = limits.ignore_ooo_exemplars;
        let is_otlp = source == cortexpb::write_request::SourceEnum::Otlp;
        let active_series_enabled = self.cfg.active_series.active_series_metrics_enabled;

        struct PreparedShard<'a> {
            app: HeadAppender<'a>,
            stats: PushStats,
            first_partial: Option<(usize, IngesterError, Option<SampleKind>)>,
        }
        type PreparedShardSlot<'a> = Mutex<Option<Result<PreparedShard<'a>, IngesterError>>>;

        let shard_slots: SmallVec<[PreparedShardSlot<'_>; 8]> =
            (0..active_shards.len()).map(|_| Mutex::new(None)).collect();

        let pool = classic_push_pool();
        pool.scoped_execute(active_shards.len(), |s_idx| {
            let shard_items = &active_shards[s_idx];
            let mut app = db.db.appender_with_capacity(shard_items.len());
            let mut stats = PushStats::default();
            let mut first_partial: Option<(usize, IngesterError, Option<SampleKind>)> = None;

            let use_batch_resolve = shard_items.len() > 2;
            let mut resolved_guard = if use_batch_resolve {
                let mut g = ResolvedEntriesGuard::take(shard_items.len());
                head.resolve_prehashed_encoded_batch(
                    shard_items,
                    |si| {
                        let item = si.item;
                        if item.bad_utf8
                            || !item.labels_sorted_unique
                            || item.encoded_labels.is_empty()
                        {
                            return None;
                        }
                        if ooo_window_ms <= 0
                            && min_append_time_available
                            && item.exemplars.is_empty()
                        {
                            let all_samples_oob =
                                item.samples.iter().all(|s| s.timestamp_ms < min_append_time);
                            if all_samples_oob
                                && (!native_histograms_enabled
                                    || item.histograms.iter().all(|h| h.0 < min_append_time))
                                && (!item.samples.is_empty()
                                    || (native_histograms_enabled && !item.histograms.is_empty()))
                            {
                                return None;
                            }
                        }
                        Some((item.prom_hash, item.encoded_labels))
                    },
                    &mut g.vec,
                );
                Some(g)
            } else {
                None
            };

            let mut hard_err: Option<IngesterError> = None;
            for (local_idx, si) in shard_items.iter().enumerate() {
                let orig_idx = si.orig_idx;
                let item = si.item;
                if let Some(ref g) = resolved_guard {
                    if let Some(Some(ref next_entry)) = g.vec.get(local_idx + 4) {
                        let next_ptr = Arc::as_ptr(next_entry);
                        prefetch_read_data(next_ptr);
                        prefetch_read_data((next_ptr as *const u8).wrapping_add(64));
                    }
                }
                let encoded_labels = item.encoded_labels;
                let raw_labels = item.raw_encoded_labels;
                let samples = item.samples;
                let hist_count = item.histograms.len();
                if !samples.is_empty() && hist_count > 0 {
                    stats.series_with_floats_and_histograms += 1;
                }

                let mut series_str_cache: Option<String> = None;
                let mut make_series_str = |_enc: &[u8]| -> String {
                    if let Some(ref s) = series_str_cache {
                        return s.clone();
                    }
                    let s = format_labels_string(iter_stringlabels(raw_labels));
                    series_str_cache = Some(s.clone());
                    s
                };
                let mut record_partial = |err: IngesterError, kind: Option<SampleKind>| {
                    if first_partial.is_none() {
                        first_partial = Some((orig_idx, err, kind));
                    }
                };

                if item.bad_utf8 {
                    let total_bad = samples.len() + hist_count;
                    stats.failed_samples += total_bad;
                    stats.failed_exemplars += item.exemplars.len();
                    if total_bad > 0 || !item.exemplars.is_empty() {
                        record_partial(
                            IngesterError::Validation {
                                err_id: mimir_common::errors::SERIES_INVALID_LABEL,
                                message: mimir_common::errors::SERIES_INVALID_LABEL
                                    .message("received a series with invalid UTF-8 in label"),
                                series: String::new(),
                            },
                            None,
                        );
                    }
                    continue;
                }

                if ooo_window_ms <= 0 && min_append_time_available && item.exemplars.is_empty() {
                    let all_samples_oob = samples.iter().all(|s| s.timestamp_ms < min_append_time);
                    if native_histograms_enabled {
                        if (!samples.is_empty() || hist_count > 0)
                            && all_samples_oob
                            && item.histograms.iter().all(|h| h.0 < min_append_time)
                        {
                            let total_oob = samples.len() + hist_count;
                            stats.failed_samples += total_oob;
                            stats.sample_timestamp_too_old += total_oob;
                            let mut first_ts = samples.first().map_or(0, |s| s.timestamp_ms);
                            if hist_count > 0 {
                                let h0_ts = item.histograms[0].0;
                                if first_ts == 0 || h0_ts < first_ts {
                                    first_ts = h0_ts;
                                }
                            }
                            let s_str = make_series_str(encoded_labels);
                            record_partial(
                                IngesterError::sample_timestamp_too_old(first_ts, s_str),
                                Some(SampleKind::SampleTimestampTooOld),
                            );
                            continue;
                        }
                    } else if !samples.is_empty() && all_samples_oob {
                        stats.failed_samples += samples.len();
                        stats.sample_timestamp_too_old += samples.len();
                        let first_ts = samples[0].timestamp_ms;
                        let s_str = make_series_str(encoded_labels);
                        record_partial(
                            IngesterError::sample_timestamp_too_old(first_ts, s_str),
                            Some(SampleKind::SampleTimestampTooOld),
                        );
                        continue;
                    }
                }

                if !item.labels_sorted_unique {
                    let total_bad = samples.len() + hist_count;
                    stats.failed_samples += total_bad;
                    stats.labels_not_sorted += total_bad;
                    stats.failed_exemplars += item.exemplars.len();
                    if total_bad > 0 {
                        let s_str = make_series_str(encoded_labels);
                        record_partial(
                            IngesterError::labels_not_sorted(s_str),
                            Some(SampleKind::LabelsNotSorted),
                        );
                    }
                    continue;
                }

                let mut series_entry: Option<Arc<SeriesEntry>> =
                    if let Some(ref mut g) = resolved_guard {
                        g.vec[local_idx].take()
                    } else if !encoded_labels.is_empty() {
                        head.get_entry_by_prehashed_encoded_labels(item.prom_hash, encoded_labels)
                    } else {
                        None
                    };
                let mut series_ref = series_entry.as_ref().map(|e| e.ref_id);
                if series_ref.is_none()
                    && verify_ownership
                    && !self.check_ring_ownership_iter(user_id, iter_stringlabels(encoded_labels))
                {
                    hard_err = Some(
                        IngesterError::Internal {
                            message: format!(
                                "series {} is not owned by this ingester",
                                make_series_str(encoded_labels)
                            ),
                        }
                        .with_user(user_id, 0),
                    );
                    break;
                }

                let prev_succeeded = stats.succeeded_samples;
                let mut created_ts = item.created_timestamp;
                let mut ingest_created_ts = created_ts > 0;
                let first_hist_ts = item.histograms.first().map(|h| h.0);

                for (s_idx, s) in samples.iter().enumerate() {
                    if s.timestamp_ms > max_timestamp_ms {
                        stats.failed_samples += 1;
                        stats.sample_too_far_in_future += 1;
                        let s_str = make_series_str(encoded_labels);
                        record_partial(
                            IngesterError::sample_too_far_in_future(s.timestamp_ms, s_str),
                            Some(SampleKind::SampleTimestampTooFarInFuture),
                        );
                        continue;
                    }
                    if s.timestamp_ms < min_timestamp_ms {
                        stats.failed_samples += 1;
                        continue;
                    }

                    if ingest_created_ts
                        && created_ts < s.timestamp_ms
                        && (!native_histograms_enabled
                            || hist_count == 0
                            || first_hist_ts.is_none_or(|ht| ht >= s.timestamp_ms))
                    {
                        match app.append_float_with_entry_prehashed_encoded(
                            &mut series_entry,
                            item.prom_hash,
                            encoded_labels,
                            0,
                            created_ts,
                            0.0,
                            true,
                        ) {
                            Ok(r) => {
                                series_ref = Some(r);
                                stats.succeeded_samples += 1;
                            }
                            Err(
                                HeadError::DuplicateSampleForTimestamp
                                | HeadError::DuplicateSampleWithDetails(_)
                                | HeadError::OutOfOrderSample,
                            ) => {}
                            Err(err) => {
                                if let Err(e) = handle_head_append_error(
                                    err,
                                    created_ts,
                                    raw_labels,
                                    ooo_window_ms,
                                    ooo_window_dur,
                                    &limits,
                                    &mut stats,
                                    &mut make_series_str,
                                    &mut record_partial,
                                ) {
                                    hard_err = Some(e.with_user(user_id, 0));
                                    break;
                                }
                            }
                        }
                        ingest_created_ts = false;
                        created_ts = 0;
                    }

                    let is_last_sample = s_idx + 1 == samples.len() && hist_count == 0;
                    let append_res = if is_last_sample {
                        app.append_float_with_entry_prehashed_encoded_take(
                            &mut series_entry,
                            item.prom_hash,
                            encoded_labels,
                            0,
                            s.timestamp_ms,
                            s.value,
                            false,
                        )
                    } else {
                        app.append_float_with_entry_prehashed_encoded(
                            &mut series_entry,
                            item.prom_hash,
                            encoded_labels,
                            0,
                            s.timestamp_ms,
                            s.value,
                            false,
                        )
                    };
                    match append_res {
                        Ok(r) => {
                            series_ref = Some(r);
                            stats.succeeded_samples += 1;
                        }
                        Err(err) => {
                            if let Err(e) = handle_head_append_error(
                                err,
                                s.timestamp_ms,
                                raw_labels,
                                ooo_window_ms,
                                ooo_window_dur,
                                &limits,
                                &mut stats,
                                &mut make_series_str,
                                &mut record_partial,
                            ) {
                                hard_err = Some(e.with_user(user_id, 0));
                                break;
                            }
                        }
                    }
                }
                if hard_err.is_some() {
                    break;
                }

                let mut num_native_hist_buckets: i64 = -1;
                let ex_count = item.exemplars.len();
                let lazy_pairs: SmallVec<[(&str, &str); 24]> = if (native_histograms_enabled
                    && hist_count > 0)
                    || (ex_count > 0 && max_exemplars > 0)
                {
                    iter_stringlabels(encoded_labels).collect()
                } else {
                    SmallVec::new()
                };

                if native_histograms_enabled && hist_count > 0 {
                    let pairs = lazy_pairs.as_slice();
                    for (h_ts, is_float, ih_opt, fh_opt, _bucket_count) in item.histograms.iter() {
                        let h_ts = *h_ts;
                        let is_float = *is_float;

                        if h_ts > max_timestamp_ms {
                            stats.failed_samples += 1;
                            stats.sample_too_far_in_future += 1;
                            let s_str = make_series_str(encoded_labels);
                            record_partial(
                                IngesterError::sample_too_far_in_future(h_ts, s_str),
                                Some(SampleKind::SampleTimestampTooFarInFuture),
                            );
                            continue;
                        }
                        if h_ts < min_timestamp_ms {
                            stats.failed_samples += 1;
                            continue;
                        }

                        if ingest_created_ts && created_ts < h_ts {
                            let ct_res = if is_float {
                                let fh_ref = fh_opt.as_ref().expect("float histogram");
                                let zero_fh = FloatHistogram {
                                    counter_reset_hint: CounterResetHint::CounterReset,
                                    schema: fh_ref.schema,
                                    zero_threshold: fh_ref.zero_threshold,
                                    custom_values: fh_ref.custom_values.clone(),
                                    ..FloatHistogram::default()
                                };
                                app.append_float_histogram_reject_ooo(
                                    series_ref, pairs, created_ts, zero_fh,
                                )
                            } else {
                                let ih_ref = ih_opt.as_ref().expect("int histogram");
                                let zero_ih = Histogram {
                                    counter_reset_hint: CounterResetHint::CounterReset,
                                    schema: ih_ref.schema,
                                    zero_threshold: ih_ref.zero_threshold,
                                    custom_values: ih_ref.custom_values.clone(),
                                    ..Histogram::default()
                                };
                                app.append_histogram_reject_ooo(
                                    series_ref, pairs, created_ts, zero_ih,
                                )
                            };
                            match ct_res {
                                Ok(r) => {
                                    series_ref = Some(r);
                                    stats.succeeded_samples += 1;
                                }
                                Err(
                                    HeadError::DuplicateSampleForTimestamp
                                    | HeadError::DuplicateSampleWithDetails(_)
                                    | HeadError::OutOfOrderSample,
                                ) => {}
                                Err(err) => {
                                    if let Err(e) = handle_head_append_error(
                                        err,
                                        created_ts,
                                        raw_labels,
                                        ooo_window_ms,
                                        ooo_window_dur,
                                        &limits,
                                        &mut stats,
                                        &mut make_series_str,
                                        &mut record_partial,
                                    ) {
                                        hard_err = Some(e.with_user(user_id, 0));
                                        break;
                                    }
                                }
                            }
                            ingest_created_ts = false;
                            created_ts = 0;
                        }

                        let res = if is_float {
                            app.append_float_histogram(
                                series_ref,
                                pairs,
                                0,
                                h_ts,
                                fh_opt.as_ref().expect("float histogram").clone(),
                            )
                        } else {
                            app.append_histogram(
                                series_ref,
                                pairs,
                                0,
                                h_ts,
                                ih_opt.as_ref().expect("int histogram").clone(),
                            )
                        };

                        match res {
                            Ok(r) => {
                                series_ref = Some(r);
                                stats.succeeded_samples += 1;
                            }
                            Err(err) => {
                                if let Err(e) = handle_head_append_error(
                                    err,
                                    h_ts,
                                    raw_labels,
                                    ooo_window_ms,
                                    ooo_window_dur,
                                    &limits,
                                    &mut stats,
                                    &mut make_series_str,
                                    &mut record_partial,
                                ) {
                                    hard_err = Some(e.with_user(user_id, 0));
                                    break;
                                }
                            }
                        }
                    }
                    if hard_err.is_some() {
                        break;
                    }
                    if let Some(last_h) = item.histograms.last() {
                        if samples.last().is_none_or(|ls| ls.timestamp_ms < last_h.0) {
                            num_native_hist_buckets = last_h.4 as i64;
                        }
                    }
                }

                if active_series_enabled && stats.succeeded_samples > prev_succeeded {
                    if let Some(ref_id) = series_ref {
                        let entry_ref = series_entry
                            .as_deref()
                            .or_else(|| app.last_float_entry().filter(|e| e.ref_id == ref_id));
                        db.active_series.update_series_with_entry_encoded(
                            entry_ref,
                            encoded_labels,
                            item.prom_hash,
                            ref_id,
                            start_append,
                            num_native_hist_buckets,
                            is_otlp,
                        );
                    }
                }

                if ex_count > 0 && max_exemplars > 0 {
                    if series_ref.is_none() {
                        match item.exemplars.first() {
                            Some(Ok((ex_ts, _, _, ex_str))) => {
                                let s_str = make_series_str(raw_labels);
                                record_partial(
                                    IngesterError::exemplar_missing_series(
                                        *ex_ts,
                                        s_str,
                                        ex_str.clone(),
                                    ),
                                    None,
                                );
                            }
                            Some(Err(ex_err)) => {
                                let s_str = make_series_str(raw_labels);
                                record_partial(
                                    IngesterError::exemplar_missing_series(
                                        ex_err.timestamp_ms,
                                        s_str,
                                        ex_err.exemplar_labels.clone(),
                                    ),
                                    None,
                                );
                            }
                            None => {}
                        }
                        stats.failed_exemplars += ex_count;
                    } else {
                        let pairs = lazy_pairs.as_slice();
                        let mut out_of_order_exemplars = 0usize;
                        for ex_res in item.exemplars {
                            let (ex_ts, ex_val, ex_labels, ex_str) = match ex_res {
                                Ok(v) => v,
                                Err(ex_err) => {
                                    stats.failed_exemplars += 1;
                                    let s_str = make_series_str(raw_labels);
                                    let err = if ex_err.timestamp_ms > max_timestamp_ms {
                                        IngesterError::exemplar_too_far_in_future(
                                            ex_err.timestamp_ms,
                                            s_str,
                                            ex_err.exemplar_labels.clone(),
                                        )
                                    } else if ex_err.timestamp_ms < min_timestamp_ms {
                                        IngesterError::exemplar_too_far_in_past(
                                            ex_err.timestamp_ms,
                                            s_str,
                                            ex_err.exemplar_labels.clone(),
                                        )
                                    } else {
                                        IngesterError::TsdbIngestExemplar {
                                            original_err: ex_err.message.clone(),
                                            timestamp_ms: ex_err.timestamp_ms,
                                            series_labels: s_str,
                                            exemplar_labels: ex_err.exemplar_labels.clone(),
                                        }
                                    };
                                    record_partial(err, None);
                                    continue;
                                }
                            };
                            let ex_ts = *ex_ts;
                            if ex_ts > max_timestamp_ms {
                                stats.failed_exemplars += 1;
                                let s_str = make_series_str(raw_labels);
                                record_partial(
                                    IngesterError::exemplar_too_far_in_future(
                                        ex_ts,
                                        s_str,
                                        ex_str.clone(),
                                    ),
                                    None,
                                );
                                continue;
                            }
                            if ex_ts < min_timestamp_ms {
                                stats.failed_exemplars += 1;
                                let s_str = make_series_str(raw_labels);
                                record_partial(
                                    IngesterError::exemplar_too_far_in_past(
                                        ex_ts,
                                        s_str,
                                        ex_str.clone(),
                                    ),
                                    None,
                                );
                                continue;
                            }
                            let exemplar = Exemplar::new(ex_labels.clone(), *ex_val, ex_ts);
                            match app.append_exemplar(series_ref, pairs, exemplar) {
                                Ok(_) => stats.succeeded_exemplars += 1,
                                Err(HeadError::Exemplar(ExemplarError::OutOfOrderExemplar)) => {
                                    stats.failed_exemplars += 1;
                                    out_of_order_exemplars += 1;
                                    if out_of_order_exemplars == ex_count && !ignore_ooo_exemplars {
                                        let s_str = make_series_str(raw_labels);
                                        record_partial(
                                            IngesterError::TsdbIngestExemplar {
                                                original_err: "out of order exemplar".to_string(),
                                                timestamp_ms: ex_ts,
                                                series_labels: s_str,
                                                exemplar_labels: ex_str.clone(),
                                            },
                                            None,
                                        );
                                    }
                                }
                                Err(HeadError::Exemplar(ex_err)) => {
                                    stats.failed_exemplars += 1;
                                    let s_str = make_series_str(raw_labels);
                                    record_partial(
                                        IngesterError::TsdbIngestExemplar {
                                            original_err: ex_err.to_string(),
                                            timestamp_ms: ex_ts,
                                            series_labels: s_str,
                                            exemplar_labels: ex_str.clone(),
                                        },
                                        None,
                                    );
                                }
                                Err(e) => {
                                    stats.failed_exemplars += 1;
                                    let s_str = make_series_str(raw_labels);
                                    record_partial(
                                        IngesterError::TsdbIngestExemplar {
                                            original_err: e.to_string(),
                                            timestamp_ms: ex_ts,
                                            series_labels: s_str,
                                            exemplar_labels: ex_str.clone(),
                                        },
                                        None,
                                    );
                                }
                            }
                        }
                    }
                }
            }

            if let Some(err) = hard_err {
                rollback_appender(user_id, app);
                *shard_slots[s_idx].lock() = Some(Err(err));
            } else {
                *shard_slots[s_idx].lock() = Some(Ok(PreparedShard {
                    app,
                    stats,
                    first_partial,
                }));
            }
        });

        self.metrics
            .appender_add_duration
            .observe(add_timer.elapsed().as_secs_f64());

        let mut prepared: Vec<PreparedShard<'_>> = Vec::with_capacity(active_shards.len());
        let mut first_hard_err: Option<IngesterError> = None;
        for slot in shard_slots {
            match slot.into_inner() {
                Some(Ok(ps)) => prepared.push(ps),
                Some(Err(e)) => {
                    if first_hard_err.is_none() {
                        first_hard_err = Some(e);
                    }
                }
                None => {
                    if first_hard_err.is_none() {
                        first_hard_err = Some(
                            IngesterError::Internal {
                                message: "classic push shard did not complete".to_string(),
                            }
                            .with_user(user_id, 0),
                        );
                    }
                }
            }
        }
        if let Some(err) = first_hard_err {
            for ps in prepared {
                rollback_appender(user_id, ps.app);
            }
            drop(lock_guard);
            return Err(err);
        }

        let mut stats = PushStats::default();
        let mut best_partial: Option<(usize, IngesterError, Option<SampleKind>)> = None;
        let mut apps: Vec<HeadAppender<'_>> = Vec::with_capacity(prepared.len());
        for mut ps in prepared {
            stats.succeeded_samples += ps.stats.succeeded_samples;
            stats.failed_samples += ps.stats.failed_samples;
            stats.succeeded_exemplars += ps.stats.succeeded_exemplars;
            stats.failed_exemplars += ps.stats.failed_exemplars;
            stats.sample_timestamp_too_old += ps.stats.sample_timestamp_too_old;
            stats.sample_out_of_order += ps.stats.sample_out_of_order;
            stats.sample_too_old += ps.stats.sample_too_old;
            stats.sample_too_far_in_future += ps.stats.sample_too_far_in_future;
            stats.new_value_for_timestamp += ps.stats.new_value_for_timestamp;
            stats.per_user_series_limit += ps.stats.per_user_series_limit;
            stats.per_metric_series_limit += ps.stats.per_metric_series_limit;
            stats.invalid_native_histogram += ps.stats.invalid_native_histogram;
            stats.labels_not_sorted += ps.stats.labels_not_sorted;
            stats.series_with_floats_and_histograms +=
                ps.stats.series_with_floats_and_histograms;
            if let Some((orig_idx, err, kind)) = ps.first_partial.take() {
                if best_partial
                    .as_ref()
                    .is_none_or(|(best_idx, _, _)| orig_idx < *best_idx)
                {
                    best_partial = Some((orig_idx, err, kind));
                }
            }
            apps.push(ps.app);
        }

        let commit_timer = Instant::now();
        let commit_res = HeadAppender::commit_batch(apps);
        drop(lock_guard);
        self.metrics
            .appender_commit_duration
            .observe(commit_timer.elapsed().as_secs_f64());

        if let Err(e) = commit_res {
            return Err(IngesterError::Internal {
                message: format!("failed to commit TSDB appender: {e}"),
            }
            .with_user(user_id, stats.failed_samples as i64));
        }

        if stats.succeeded_samples > 0 {
            db.set_last_update(SystemTime::now());
        }

        db.ingested_samples_counter(&self.metrics.ingested_samples)
            .inc_by(stats.succeeded_samples as f64);
        db.ingested_samples_fail_counter(&self.metrics.ingested_samples_fail)
            .inc_by(stats.failed_samples as f64);
        self.metrics
            .ingested_exemplars
            .inc_by(stats.succeeded_exemplars as f64);
        self.metrics
            .ingested_exemplars_fail
            .inc_by(stats.failed_exemplars as f64);
        self.appended_samples
            .fetch_add(stats.succeeded_samples as i64, Ordering::Relaxed);
        self.appended_exemplars
            .fetch_add(stats.succeeded_exemplars as i64, Ordering::Relaxed);
        self.usage_appended_samples.inc(stats.succeeded_samples as i64);
        self.usage_appended_exemplars.inc(stats.succeeded_exemplars as i64);

        let request_group = group_label(
            &limits.separate_metrics_group_label,
            items.first().map(|it| iter_stringlabels(it.encoded_labels)),
        );
        let group = self.update_active_group(user_id, request_group, start_append);
        self.record_discarded_metrics(user_id, &group, &stats, source, &db);

        if let Some((_, partial, sampler_kind)) = best_partial {
            let err = if let Some(kind) = sampler_kind {
                let sampler = kind.sampler(&self.error_samplers);
                partial.with_sampling_rate(sampler.should_log(), sampler.rate())
            } else {
                partial
            };
            return Err(err.with_user(user_id, stats.failed_samples as i64));
        }

        Ok(())
    }

    fn ingest_prehashed_shard_batch_inner(
        &self,
        user_id: &str,
        source: cortexpb::write_request::SourceEnum,
        min_ts: i64,
        items: &[PrehashedSeriesItem<'_>],
    ) -> Result<(), IngesterError> {
        let db = self.get_or_create_tsdb(user_id).map_err(|e| {
            let wrapped = e.with_user(user_id, 0);
            if wrapped.error_cause() == cortexpb::ErrorCause::InstanceLimit {
                wrapped.with_do_not_log()
            } else {
                wrapped
            }
        })?;
        let lock_guard = db
            .acquire_append_lock(min_ts)
            .map_err(|e| e.with_user(user_id, 0))?;
        self.ingest_prehashed_shard_batch_with_lock(user_id, &db, lock_guard, source, items)
    }

    fn ingest_prehashed_shard_batch_with_lock<'a>(
        &self,
        user_id: &str,
        db: &Arc<crate::user_tsdb::UserTsdb>,
        lock_guard: crate::user_tsdb::AppendLockGuard<'a>,
        source: cortexpb::write_request::SourceEnum,
        items: &[PrehashedSeriesItem<'_>],
    ) -> Result<(), IngesterError> {
        let start_append = SystemTime::now();
        let start_append_ms = start_append
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        let add_timer = Instant::now();

        let limits = self.overrides.for_tenant(user_id);
        let native_histograms_enabled = limits.native_histograms_ingestion_enabled;
        let creation_grace_ms = (limits.creation_grace_period.0 / 1_000_000).max(0);
        let past_grace_ms = (limits.past_grace_period.0 / 1_000_000).max(0);
        let ooo_window_ms = (limits.out_of_order_time_window.0 / 1_000_000).max(0);
        let ooo_window_dur = Duration::from_millis(ooo_window_ms as u64);

        let max_timestamp_ms = start_append_ms.saturating_add(creation_grace_ms);
        let min_timestamp_ms = if past_grace_ms > 0 {
            start_append_ms
                .saturating_sub(past_grace_ms)
                .saturating_sub(ooo_window_ms)
        } else {
            i64::MIN
        };

        let head = db.db.head();
        let min_append_time_available = head.initialized();
        let min_append_time = head.appendable_min_valid_time();
        let max_exemplars = self.limiter.max_exemplars_per_user(user_id);
        let ignore_ooo_exemplars = limits.ignore_ooo_exemplars;
        let is_otlp = source == cortexpb::write_request::SourceEnum::Otlp;
        let verify_ownership = self.verify_ring_ownership.load(Ordering::Relaxed);
        let active_series_enabled = self.cfg.active_series.active_series_metrics_enabled;

        let mut app = db.db.appender_with_capacity(items.len());
        let mut stats = PushStats::default();
        let mut first_partial: Option<(IngesterError, Option<SampleKind>)> = None;

        let cast = self
            .cost_attribution_manager()
            .and_then(|m| m.sample_tracker(user_id));
        let mut ca_prev = [0usize; 9];

        let use_batch_resolve = items.len() > 2;
        let mut resolved_guard = if use_batch_resolve {
            let mut g = ResolvedEntriesGuard::take(items.len());
            head.resolve_prehashed_encoded_batch(
                items,
                |item| {
                    if item.bad_utf8
                        || !item.labels_sorted_unique
                        || item.encoded_labels.is_empty()
                    {
                        return None;
                    }
                    if ooo_window_ms <= 0 && min_append_time_available && item.exemplars.is_empty() {
                        let all_samples_oob =
                            item.samples.iter().all(|s| s.timestamp_ms < min_append_time);
                        if all_samples_oob
                            && (!native_histograms_enabled
                                || item.histograms.iter().all(|h| h.0 < min_append_time))
                            && (!item.samples.is_empty()
                                || (native_histograms_enabled && !item.histograms.is_empty()))
                        {
                            return None;
                        }
                    }
                    Some((item.prom_hash, item.encoded_labels))
                },
                &mut g.vec,
            );
            Some(g)
        } else {
            None
        };

        for (idx, item) in items.iter().enumerate() {
            if let (Some(cast), Some(prev_item)) = (cast.as_deref(), idx.checked_sub(1).map(|i| &items[i])) {
                attribute_discarded(cast, &mut ca_prev, &stats, iter_stringlabels(prev_item.encoded_labels), start_append);
            }
            if let Some(ref g) = resolved_guard {
                if let Some(Some(ref next_entry)) = g.vec.get(idx + 4) {
                    let next_ptr = Arc::as_ptr(next_entry);
                    prefetch_read_data(next_ptr);
                    prefetch_read_data((next_ptr as *const u8).wrapping_add(64));
                }
            }
            let encoded_labels = item.encoded_labels;
            let raw_labels = item.raw_encoded_labels;
            let samples = item.samples;
            let hist_count = item.histograms.len();
            if !samples.is_empty() && hist_count > 0 {
                stats.series_with_floats_and_histograms += 1;
            }

            let mut series_str_cache: Option<String> = None;
            let mut make_series_str = |_enc: &[u8]| -> String {
                if let Some(ref s) = series_str_cache {
                    return s.clone();
                }
                let s = format_labels_string(iter_stringlabels(raw_labels));
                series_str_cache = Some(s.clone());
                s
            };
            let mut record_partial = |err: IngesterError, kind: Option<SampleKind>| {
                if first_partial.is_none() {
                    first_partial = Some((err, kind));
                }
            };

            if item.bad_utf8 {
                let total_bad = samples.len() + hist_count;
                stats.failed_samples += total_bad;
                stats.failed_exemplars += item.exemplars.len();
                if total_bad > 0 || !item.exemplars.is_empty() {
                    record_partial(
                        IngesterError::Validation {
                            err_id: mimir_common::errors::SERIES_INVALID_LABEL,
                            message: mimir_common::errors::SERIES_INVALID_LABEL
                                .message("received a series with invalid UTF-8 in label"),
                            series: String::new(),
                        },
                        None,
                    );
                }
                continue;
            }

            if ooo_window_ms <= 0 && min_append_time_available && item.exemplars.is_empty() {
                let all_samples_oob = samples.iter().all(|s| s.timestamp_ms < min_append_time);
                if native_histograms_enabled {
                    if (!samples.is_empty() || hist_count > 0)
                        && all_samples_oob
                        && item.histograms.iter().all(|h| h.0 < min_append_time)
                    {
                        let total_oob = samples.len() + hist_count;
                        stats.failed_samples += total_oob;
                        stats.sample_timestamp_too_old += total_oob;
                        let mut first_ts = samples.first().map_or(0, |s| s.timestamp_ms);
                        if hist_count > 0 {
                            let h0_ts = item.histograms[0].0;
                            if first_ts == 0 || h0_ts < first_ts {
                                first_ts = h0_ts;
                            }
                        }
                        let s_str = make_series_str(encoded_labels);
                        record_partial(
                            IngesterError::sample_timestamp_too_old(first_ts, s_str),
                            Some(SampleKind::SampleTimestampTooOld),
                        );
                        continue;
                    }
                } else if !samples.is_empty() && all_samples_oob {
                    stats.failed_samples += samples.len();
                    stats.sample_timestamp_too_old += samples.len();
                    let first_ts = samples[0].timestamp_ms;
                    let s_str = make_series_str(encoded_labels);
                    record_partial(
                        IngesterError::sample_timestamp_too_old(first_ts, s_str),
                        Some(SampleKind::SampleTimestampTooOld),
                    );
                    continue;
                }
            }

            if !item.labels_sorted_unique {
                let total_bad = samples.len() + hist_count;
                stats.failed_samples += total_bad;
                stats.labels_not_sorted += total_bad;
                stats.failed_exemplars += item.exemplars.len();
                if total_bad > 0 {
                    let s_str = make_series_str(encoded_labels);
                    record_partial(
                        IngesterError::labels_not_sorted(s_str),
                        Some(SampleKind::LabelsNotSorted),
                    );
                }
                continue;
            }

            let mut series_entry: Option<Arc<SeriesEntry>> = if let Some(ref mut g) = resolved_guard
            {
                g.vec[idx].take()
            } else if !encoded_labels.is_empty() {
                head.get_entry_by_prehashed_encoded_labels(item.prom_hash, encoded_labels)
            } else {
                None
            };
            let mut series_ref = series_entry.as_ref().map(|e| e.ref_id);

            // Like Go, the ingester does not validate label names: a missing metric name or an
            // empty label set fails inside the TSDB appender (`PreCreation` / `getOrCreate`) as a
            // hard error that rolls back the whole request.
            if series_ref.is_none()
                && verify_ownership
                && !self.check_ring_ownership_iter(user_id, iter_stringlabels(encoded_labels))
            {
                rollback_appender(user_id, app);
                drop(lock_guard);
                return Err(IngesterError::Internal {
                    message: format!(
                        "series {} is not owned by this ingester",
                        make_series_str(encoded_labels)
                    ),
                }
                .with_user(user_id, 0));
            }

            let prev_succeeded = stats.succeeded_samples;
            let mut created_ts = item.created_timestamp;
            let mut ingest_created_ts = created_ts > 0;
            let first_hist_ts = item.histograms.first().map(|h| h.0);

            for (s_idx, s) in samples.iter().enumerate() {
                if s.timestamp_ms > max_timestamp_ms {
                    stats.failed_samples += 1;
                    stats.sample_too_far_in_future += 1;
                    let s_str = make_series_str(encoded_labels);
                    record_partial(
                        IngesterError::sample_too_far_in_future(s.timestamp_ms, s_str),
                        Some(SampleKind::SampleTimestampTooFarInFuture),
                    );
                    continue;
                }
                if s.timestamp_ms < min_timestamp_ms {
                    stats.failed_samples += 1;
                    continue;
                }

                if ingest_created_ts
                    && created_ts < s.timestamp_ms
                    && (!native_histograms_enabled
                        || hist_count == 0
                        || first_hist_ts.is_none_or(|ht| ht >= s.timestamp_ms))
                {
                    match app.append_float_with_entry_prehashed_encoded(
                        &mut series_entry,
                        item.prom_hash,
                        encoded_labels,
                        0,
                        created_ts,
                        0.0,
                        true,
                    ) {
                        Ok(r) => {
                            series_ref = Some(r);
                            stats.succeeded_samples += 1;
                        }
                        Err(
                            HeadError::DuplicateSampleForTimestamp
                            | HeadError::DuplicateSampleWithDetails(_)
                            | HeadError::OutOfOrderSample,
                        ) => {}
                        Err(err) => {
                            if let Err(e) = handle_head_append_error(
                                err,
                                created_ts,
                                raw_labels,
                                ooo_window_ms,
                                ooo_window_dur,
                                &limits,
                                &mut stats,
                                &mut make_series_str,
                                &mut record_partial,
                            ) {
                                rollback_appender(user_id, app);
                                drop(lock_guard);
                                return Err(e.with_user(user_id, 0));
                            }
                        }
                    }
                    ingest_created_ts = false;
                    created_ts = 0;
                }

                let is_last_sample = s_idx + 1 == samples.len() && hist_count == 0;
                let append_res = if is_last_sample {
                    app.append_float_with_entry_prehashed_encoded_take(
                        &mut series_entry,
                        item.prom_hash,
                        encoded_labels,
                        0,
                        s.timestamp_ms,
                        s.value,
                        false,
                    )
                } else {
                    app.append_float_with_entry_prehashed_encoded(
                        &mut series_entry,
                        item.prom_hash,
                        encoded_labels,
                        0,
                        s.timestamp_ms,
                        s.value,
                        false,
                    )
                };
                match append_res {
                    Ok(r) => {
                        series_ref = Some(r);
                        stats.succeeded_samples += 1;
                    }
                    Err(err) => {
                        if let Err(e) = handle_head_append_error(
                            err,
                            s.timestamp_ms,
                            raw_labels,
                            ooo_window_ms,
                            ooo_window_dur,
                            &limits,
                            &mut stats,
                            &mut make_series_str,
                            &mut record_partial,
                        ) {
                            rollback_appender(user_id, app);
                            drop(lock_guard);
                            return Err(e.with_user(user_id, 0));
                        }
                    }
                }
            }

            let mut num_native_hist_buckets: i64 = -1;
            let ex_count = item.exemplars.len();
            let lazy_pairs: SmallVec<[(&str, &str); 24]> =
                if (native_histograms_enabled && hist_count > 0) || (ex_count > 0 && max_exemplars > 0)
                {
                    iter_stringlabels(encoded_labels).collect()
                } else {
                    SmallVec::new()
                };

            if native_histograms_enabled && hist_count > 0 {
                let pairs = lazy_pairs.as_slice();
                for (h_ts, is_float, ih_opt, fh_opt, _bucket_count) in item.histograms.iter() {
                    let h_ts = *h_ts;
                    let is_float = *is_float;

                    if h_ts > max_timestamp_ms {
                        stats.failed_samples += 1;
                        stats.sample_too_far_in_future += 1;
                        let s_str = make_series_str(encoded_labels);
                        record_partial(
                            IngesterError::sample_too_far_in_future(h_ts, s_str),
                            Some(SampleKind::SampleTimestampTooFarInFuture),
                        );
                        continue;
                    }
                    if h_ts < min_timestamp_ms {
                        stats.failed_samples += 1;
                        continue;
                    }

                    if ingest_created_ts && created_ts < h_ts {
                        let ct_res = if is_float {
                            let fh_ref = fh_opt.as_ref().expect("float histogram");
                            let zero_fh = FloatHistogram {
                                counter_reset_hint: CounterResetHint::CounterReset,
                                schema: fh_ref.schema,
                                zero_threshold: fh_ref.zero_threshold,
                                custom_values: fh_ref.custom_values.clone(),
                                ..FloatHistogram::default()
                            };
                            app.append_float_histogram_reject_ooo(
                                series_ref, pairs, created_ts, zero_fh,
                            )
                        } else {
                            let ih_ref = ih_opt.as_ref().expect("int histogram");
                            let zero_ih = Histogram {
                                counter_reset_hint: CounterResetHint::CounterReset,
                                schema: ih_ref.schema,
                                zero_threshold: ih_ref.zero_threshold,
                                custom_values: ih_ref.custom_values.clone(),
                                ..Histogram::default()
                            };
                            app.append_histogram_reject_ooo(
                                series_ref, pairs, created_ts, zero_ih,
                            )
                        };
                        match ct_res {
                            Ok(r) => {
                                series_ref = Some(r);
                                stats.succeeded_samples += 1;
                            }
                            Err(
                                HeadError::DuplicateSampleForTimestamp
                                | HeadError::DuplicateSampleWithDetails(_)
                                | HeadError::OutOfOrderSample,
                            ) => {}
                            Err(err) => {
                                if let Err(e) = handle_head_append_error(
                                    err,
                                    created_ts,
                                    raw_labels,
                                    ooo_window_ms,
                                    ooo_window_dur,
                                    &limits,
                                    &mut stats,
                                    &mut make_series_str,
                                    &mut record_partial,
                                ) {
                                    rollback_appender(user_id, app);
                                    drop(lock_guard);
                                    return Err(e.with_user(user_id, 0));
                                }
                            }
                        }
                        ingest_created_ts = false;
                        created_ts = 0;
                    }

                    let res = if is_float {
                        app.append_float_histogram(
                            series_ref,
                            pairs,
                            0,
                            h_ts,
                            fh_opt.as_ref().expect("float histogram").clone(),
                        )
                    } else {
                        app.append_histogram(
                            series_ref,
                            pairs,
                            0,
                            h_ts,
                            ih_opt.as_ref().expect("int histogram").clone(),
                        )
                    };

                    match res {
                        Ok(r) => {
                            series_ref = Some(r);
                            stats.succeeded_samples += 1;
                        }
                        Err(err) => {
                            if let Err(e) = handle_head_append_error(
                                err,
                                h_ts,
                                raw_labels,
                                ooo_window_ms,
                                ooo_window_dur,
                                &limits,
                                &mut stats,
                                &mut make_series_str,
                                &mut record_partial,
                            ) {
                                rollback_appender(user_id, app);
                                drop(lock_guard);
                                return Err(e.with_user(user_id, 0));
                            }
                        }
                    }
                }
                if let Some(last_h) = item.histograms.last() {
                    if samples.last().is_none_or(|ls| ls.timestamp_ms < last_h.0) {
                        num_native_hist_buckets = last_h.4 as i64;
                    }
                }
            }

            if active_series_enabled && stats.succeeded_samples > prev_succeeded {
                if let Some(ref_id) = series_ref {
                    let entry_ref = series_entry
                        .as_deref()
                        .or_else(|| app.last_float_entry().filter(|e| e.ref_id == ref_id));
                    db.active_series.update_series_with_entry_encoded(
                        entry_ref,
                        encoded_labels,
                        item.prom_hash,
                        ref_id,
                        start_append,
                        num_native_hist_buckets,
                        is_otlp,
                    );
                }
            }

            if ex_count > 0 && max_exemplars > 0 {
                if series_ref.is_none() {
                    match item.exemplars.first() {
                        Some(Ok((ex_ts, _, _, ex_str))) => {
                            let s_str = make_series_str(raw_labels);
                            record_partial(
                                IngesterError::exemplar_missing_series(
                                    *ex_ts,
                                    s_str,
                                    ex_str.clone(),
                                ),
                                None,
                            );
                        }
                        Some(Err(ex_err)) => {
                            let s_str = make_series_str(raw_labels);
                            record_partial(
                                IngesterError::exemplar_missing_series(
                                    ex_err.timestamp_ms,
                                    s_str,
                                    ex_err.exemplar_labels.clone(),
                                ),
                                None,
                            );
                        }
                        None => {}
                    }
                    stats.failed_exemplars += ex_count;
                } else {
                    let pairs = lazy_pairs.as_slice();
                    let mut out_of_order_exemplars = 0usize;
                    for ex_res in item.exemplars {
                        let (ex_ts, ex_val, ex_labels, ex_str) = match ex_res {
                            Ok(v) => v,
                            Err(ex_err) => {
                                stats.failed_exemplars += 1;
                                let s_str = make_series_str(raw_labels);
                                let err = if ex_err.timestamp_ms > max_timestamp_ms {
                                    IngesterError::exemplar_too_far_in_future(
                                        ex_err.timestamp_ms,
                                        s_str,
                                        ex_err.exemplar_labels.clone(),
                                    )
                                } else if ex_err.timestamp_ms < min_timestamp_ms {
                                    IngesterError::exemplar_too_far_in_past(
                                        ex_err.timestamp_ms,
                                        s_str,
                                        ex_err.exemplar_labels.clone(),
                                    )
                                } else {
                                    IngesterError::TsdbIngestExemplar {
                                        original_err: ex_err.message.clone(),
                                        timestamp_ms: ex_err.timestamp_ms,
                                        series_labels: s_str,
                                        exemplar_labels: ex_err.exemplar_labels.clone(),
                                    }
                                };
                                record_partial(err, None);
                                continue;
                            }
                        };
                        let ex_ts = *ex_ts;
                        if ex_ts > max_timestamp_ms {
                            stats.failed_exemplars += 1;
                            let s_str = make_series_str(raw_labels);
                            record_partial(
                                IngesterError::exemplar_too_far_in_future(
                                    ex_ts,
                                    s_str,
                                    ex_str.clone(),
                                ),
                                None,
                            );
                            continue;
                        }
                        if ex_ts < min_timestamp_ms {
                            stats.failed_exemplars += 1;
                            let s_str = make_series_str(raw_labels);
                            record_partial(
                                IngesterError::exemplar_too_far_in_past(
                                    ex_ts,
                                    s_str,
                                    ex_str.clone(),
                                ),
                                None,
                            );
                            continue;
                        }
                        let exemplar = Exemplar::new(ex_labels.clone(), *ex_val, ex_ts);
                        match app.append_exemplar(series_ref, pairs, exemplar) {
                            Ok(_) => stats.succeeded_exemplars += 1,
                            Err(HeadError::Exemplar(ExemplarError::OutOfOrderExemplar)) => {
                                stats.failed_exemplars += 1;
                                out_of_order_exemplars += 1;
                                if out_of_order_exemplars == ex_count && !ignore_ooo_exemplars {
                                    let s_str = make_series_str(raw_labels);
                                    record_partial(
                                        IngesterError::TsdbIngestExemplar {
                                            original_err: "out of order exemplar".to_string(),
                                            timestamp_ms: ex_ts,
                                            series_labels: s_str,
                                            exemplar_labels: ex_str.clone(),
                                        },
                                        None,
                                    );
                                }
                            }
                            Err(HeadError::Exemplar(ex_err)) => {
                                stats.failed_exemplars += 1;
                                let s_str = make_series_str(raw_labels);
                                record_partial(
                                    IngesterError::TsdbIngestExemplar {
                                        original_err: ex_err.to_string(),
                                        timestamp_ms: ex_ts,
                                        series_labels: s_str,
                                        exemplar_labels: ex_str.clone(),
                                    },
                                    None,
                                );
                            }
                            Err(e) => {
                                stats.failed_exemplars += 1;
                                let s_str = make_series_str(raw_labels);
                                record_partial(
                                    IngesterError::TsdbIngestExemplar {
                                        original_err: e.to_string(),
                                        timestamp_ms: ex_ts,
                                        series_labels: s_str,
                                        exemplar_labels: ex_str.clone(),
                                    },
                                    None,
                                );
                            }
                        }
                    }
                }
            }
        }

        if let (Some(cast), Some(last)) = (cast.as_deref(), items.last()) {
            attribute_discarded(cast, &mut ca_prev, &stats, iter_stringlabels(last.encoded_labels), start_append);
        }
        self.metrics
            .appender_add_duration
            .observe(add_timer.elapsed().as_secs_f64());

        let commit_timer = Instant::now();
        if let Err(e) = app.commit() {
            drop(lock_guard);
            return Err(IngesterError::Internal {
                message: format!("failed to commit TSDB appender: {e}"),
            }
            .with_user(user_id, stats.failed_samples as i64));
        }
        drop(lock_guard);
        self.metrics
            .appender_commit_duration
            .observe(commit_timer.elapsed().as_secs_f64());

        if stats.succeeded_samples > 0 {
            db.set_last_update(SystemTime::now());
        }

        db.ingested_samples_counter(&self.metrics.ingested_samples)
            .inc_by(stats.succeeded_samples as f64);
        db.ingested_samples_fail_counter(&self.metrics.ingested_samples_fail)
            .inc_by(stats.failed_samples as f64);
        self.metrics
            .ingested_exemplars
            .inc_by(stats.succeeded_exemplars as f64);
        self.metrics
            .ingested_exemplars_fail
            .inc_by(stats.failed_exemplars as f64);
        self.appended_samples
            .fetch_add(stats.succeeded_samples as i64, Ordering::Relaxed);
        self.appended_exemplars
            .fetch_add(stats.succeeded_exemplars as i64, Ordering::Relaxed);
        self.usage_appended_samples.inc(stats.succeeded_samples as i64);
        self.usage_appended_exemplars.inc(stats.succeeded_exemplars as i64);

        let request_group = group_label(
            &limits.separate_metrics_group_label,
            items.first().map(|it| iter_stringlabels(it.encoded_labels)),
        );
        let group = self.update_active_group(user_id, request_group, start_append);
        self.record_discarded_metrics(user_id, &group, &stats, source, db);

        if let Some((partial, sampler_kind)) = first_partial {
            let err = if let Some(kind) = sampler_kind {
                let sampler = kind.sampler(&self.error_samplers);
                partial.with_sampling_rate(sampler.should_log(), sampler.rate())
            } else {
                partial
            };
            return Err(err.with_user(user_id, stats.failed_samples as i64));
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn ingest_timeseries<'a, I, T>(
        &self,
        user_id: &str,
        db: &UserTsdb,
        source: cortexpb::write_request::SourceEnum,
        _skip_label_validation: bool,
        _skip_label_count_validation: bool,
        series_hint: usize,
        timeseries: I,
    ) -> Result<(), IngesterError>
    where
        I: IntoIterator<Item = T>,
        T: TimeSeriesInput<'a>,
    {
        let start_append = SystemTime::now();
        let start_append_ms = start_append
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        let add_timer = Instant::now();

        let limits = self.overrides.for_tenant(user_id);
        let native_histograms_enabled = limits.native_histograms_ingestion_enabled;
        let creation_grace_ms = (limits.creation_grace_period.0 / 1_000_000).max(0);
        let past_grace_ms = (limits.past_grace_period.0 / 1_000_000).max(0);
        let ooo_window_ms = (limits.out_of_order_time_window.0 / 1_000_000).max(0);
        let ooo_window_dur = Duration::from_millis(ooo_window_ms as u64);

        let max_timestamp_ms = start_append_ms.saturating_add(creation_grace_ms);
        let min_timestamp_ms = if past_grace_ms > 0 {
            start_append_ms
                .saturating_sub(past_grace_ms)
                .saturating_sub(ooo_window_ms)
        } else {
            i64::MIN
        };

        let head = db.db.head();
        let min_append_time_available = head.initialized();
        let min_append_time = head.appendable_min_valid_time();
        let max_exemplars = self.limiter.max_exemplars_per_user(user_id);
        let ignore_ooo_exemplars = limits.ignore_ooo_exemplars;
        let is_otlp = source == cortexpb::write_request::SourceEnum::Otlp;
        let verify_ownership = self.verify_ring_ownership.load(Ordering::Relaxed);

        let mut app = db.db.appender_with_capacity(series_hint);
        let mut stats = PushStats::default();
        let mut first_partial: Option<(IngesterError, Option<SampleKind>)> = None;
        let mut pairs_guard = PairsScratchGuard::take();
        let pairs = pairs_guard.as_mut_pairs();
        let mut series_str_cache: Option<String>;

        let cast = self
            .cost_attribution_manager()
            .and_then(|m| m.sample_tracker(user_id));
        let mut ca_prev = [0usize; 9];
        let mut first_series = true;
        let mut request_group = String::new();

        let mut record_partial = |err: IngesterError, kind: Option<SampleKind>| {
            if first_partial.is_none() {
                first_partial = Some((err, kind));
            }
        };

        for ts in timeseries {
            if let Some(cast) = cast.as_deref() {
                attribute_discarded(cast, &mut ca_prev, &stats, pairs.iter().copied(), start_append);
            }
            series_str_cache = None;
            let samples = ts.samples();
            let hist_count = ts.histogram_count();
            if !samples.is_empty() && hist_count > 0 {
                stats.series_with_floats_and_histograms += 1;
            }

            if let Err(_e) = ts.fill_pairs(pairs) {
                let total_bad = samples.len() + hist_count;
                stats.failed_samples += total_bad;
                stats.failed_exemplars += ts.exemplar_count();
                if total_bad > 0 || ts.exemplar_count() > 0 {
                    record_partial(
                        IngesterError::Validation {
                            err_id: mimir_common::errors::SERIES_INVALID_LABEL,
                            message: mimir_common::errors::SERIES_INVALID_LABEL
                                .message("received a series with invalid UTF-8 in label"),
                            series: String::new(),
                        },
                        None,
                    );
                }
                pairs.clear();
                continue;
            }
            if first_series {
                first_series = false;
                request_group = group_label(
                    &limits.separate_metrics_group_label,
                    Some(pairs.iter().copied()),
                );
            }

            // Fast path when all samples (and histograms when enabled) are older than head's min_append_time and OOO is disabled.
            if ooo_window_ms <= 0 && min_append_time_available && ts.exemplar_count() == 0 {
                let all_samples_oob = samples.iter().all(|s| s.timestamp_ms < min_append_time);
                if native_histograms_enabled {
                    if (!samples.is_empty() || hist_count > 0)
                        && all_samples_oob
                        && (0..hist_count).all(|idx| ts.histogram_timestamp(idx) < min_append_time)
                    {
                        let total_oob = samples.len() + hist_count;
                        stats.failed_samples += total_oob;
                        stats.sample_timestamp_too_old += total_oob;
                        let mut first_ts = samples.first().map_or(0, |s| s.timestamp_ms);
                        if hist_count > 0 {
                            let h0_ts = ts.histogram_timestamp(0);
                            if first_ts == 0 || h0_ts < first_ts {
                                first_ts = h0_ts;
                            }
                        }
                        let s_str = format_labels_string(pairs.iter().copied());
                        record_partial(
                            IngesterError::sample_timestamp_too_old(first_ts, s_str),
                            Some(SampleKind::SampleTimestampTooOld),
                        );
                        continue;
                    }
                } else if !samples.is_empty() && all_samples_oob {
                    stats.failed_samples += samples.len();
                    stats.sample_timestamp_too_old += samples.len();
                    let first_ts = samples[0].timestamp_ms;
                    let s_str = format_labels_string(pairs.iter().copied());
                    record_partial(
                        IngesterError::sample_timestamp_too_old(first_ts, s_str),
                        Some(SampleKind::SampleTimestampTooOld),
                    );
                    continue;
                }
            }

            // Look up existing ref (zero-alloc if pairs are already sorted & unique).
            // Strip blank label values after checking sorted/unique, matching Prometheus Labels.WithoutEmpty().
            let mut labels_sorted_unique = true;
            let mut has_empty_val = false;
            let mut prev_name: Option<&str> = None;
            for &(k, v) in pairs.iter() {
                if let Some(pn) = prev_name {
                    if pn >= k {
                        labels_sorted_unique = false;
                    }
                }
                prev_name = Some(k);
                if v.is_empty() {
                    has_empty_val = true;
                }
            }
            // `newLabelsNotSortedError(ts.Labels)` formats the raw wire labels.
            let unsorted_series_str =
                (!labels_sorted_unique).then(|| format_labels_string(pairs.iter().copied()));
            let raw_pairs_if_empty: Option<Vec<(&'a str, &'a str)>> =
                (has_empty_val && labels_sorted_unique).then(|| pairs.clone());
            if has_empty_val {
                pairs.retain(|(_, v)| !v.is_empty());
            }

            let mut make_series_str = |ps: &[(&str, &str)]| -> String {
                if let Some(ref s) = series_str_cache {
                    return s.clone();
                }
                let src = raw_pairs_if_empty.as_deref().unwrap_or(ps);
                let s = format_labels_string(src.iter().copied());
                series_str_cache = Some(s.clone());
                s
            };

            let mut series_entry: Option<Arc<SeriesEntry>> =
                if labels_sorted_unique && !pairs.is_empty() {
                    head.get_entry_by_sorted_pairs(pairs)
                } else {
                    None
                };
            let mut series_ref = series_entry.as_ref().map(|e| e.ref_id);

            if let (None, Some(s_str)) = (series_ref, unsorted_series_str) {
                let total_bad = samples.len() + hist_count;
                stats.failed_samples += total_bad;
                stats.labels_not_sorted += total_bad;
                stats.failed_exemplars += ts.exemplar_count();
                if total_bad > 0 {
                    record_partial(
                        IngesterError::labels_not_sorted(s_str),
                        Some(SampleKind::LabelsNotSorted),
                    );
                }
                continue;
            }

            if series_ref.is_none() && verify_ownership && !self.check_ring_ownership(user_id, pairs) {
                rollback_appender(user_id, app);
                return Err(IngesterError::Internal {
                    message: format!(
                        "series {} is not owned by this ingester",
                        make_series_str(pairs)
                    ),
                }
                .with_user(user_id, 0));
            }

            let prev_succeeded = stats.succeeded_samples;
            let mut created_ts = ts.created_timestamp();
            let mut ingest_created_ts = created_ts > 0;
            let first_hist_ts = (hist_count > 0).then(|| ts.histogram_timestamp(0));

            for s in samples {
                if s.timestamp_ms > max_timestamp_ms {
                    stats.failed_samples += 1;
                    stats.sample_too_far_in_future += 1;
                    let s_str = make_series_str(pairs);
                    record_partial(
                        IngesterError::sample_too_far_in_future(s.timestamp_ms, s_str),
                        Some(SampleKind::SampleTimestampTooFarInFuture),
                    );
                    continue;
                }
                if s.timestamp_ms < min_timestamp_ms {
                    stats.failed_samples += 1;
                    continue;
                }

                if ingest_created_ts
                    && created_ts < s.timestamp_ms
                    && (!native_histograms_enabled
                        || hist_count == 0
                        || first_hist_ts.is_none_or(|ht| ht >= s.timestamp_ms))
                {
                    match app.append_float_with_entry(
                        &mut series_entry,
                        pairs,
                        0,
                        created_ts,
                        0.0,
                        true,
                    ) {
                        Ok(r) => {
                            series_ref = Some(r);
                            stats.succeeded_samples += 1;
                        }
                        Err(
                            HeadError::DuplicateSampleForTimestamp
                            | HeadError::DuplicateSampleWithDetails(_)
                            | HeadError::OutOfOrderSample,
                        ) => {}
                        Err(err) => {
                            match handle_head_append_error(
                                err,
                                created_ts,
                                raw_pairs_if_empty.as_deref().unwrap_or(pairs.as_slice()),
                                ooo_window_ms,
                                ooo_window_dur,
                                &limits,
                                &mut stats,
                                &mut make_series_str,
                                &mut record_partial,
                            ) {
                                Ok(()) => {}
                                Err(e) => {
                                    rollback_appender(user_id, app);
                                    return Err(e.with_user(user_id, 0));
                                }
                            }
                        }
                    }
                    ingest_created_ts = false;
                    created_ts = 0;
                }

                match app.append_float_with_entry(
                    &mut series_entry,
                    pairs,
                    0,
                    s.timestamp_ms,
                    s.value,
                    false,
                ) {
                    Ok(r) => {
                        series_ref = Some(r);
                        stats.succeeded_samples += 1;
                    }
                    Err(err) => {
                        match handle_head_append_error(
                            err,
                            s.timestamp_ms,
                            raw_pairs_if_empty.as_deref().unwrap_or(pairs.as_slice()),
                            ooo_window_ms,
                            ooo_window_dur,
                            &limits,
                            &mut stats,
                            &mut make_series_str,
                            &mut record_partial,
                        ) {
                            Ok(()) => {}
                            Err(e) => {
                                rollback_appender(user_id, app);
                                return Err(e.with_user(user_id, 0));
                            }
                        }
                    }
                }
            }

            let mut num_native_hist_buckets: i64 = -1;
            if native_histograms_enabled && hist_count > 0 {
                for h_idx in 0..hist_count {
                    let (h_ts, is_float, ih_opt, fh_opt, _bucket_count) =
                        ts.convert_histogram(h_idx);

                    if h_ts > max_timestamp_ms {
                        stats.failed_samples += 1;
                        stats.sample_too_far_in_future += 1;
                        let s_str = make_series_str(pairs);
                        record_partial(
                            IngesterError::sample_too_far_in_future(h_ts, s_str),
                            Some(SampleKind::SampleTimestampTooFarInFuture),
                        );
                        continue;
                    }
                    if h_ts < min_timestamp_ms {
                        stats.failed_samples += 1;
                        continue;
                    }

                    if ingest_created_ts && created_ts < h_ts {
                        let ct_res = if is_float {
                            let fh_ref = fh_opt.as_ref().expect("float histogram");
                            let zero_fh = FloatHistogram {
                                counter_reset_hint: CounterResetHint::CounterReset,
                                schema: fh_ref.schema,
                                zero_threshold: fh_ref.zero_threshold,
                                custom_values: fh_ref.custom_values.clone(),
                                ..FloatHistogram::default()
                            };
                            app.append_float_histogram_reject_ooo(
                                series_ref, pairs, created_ts, zero_fh,
                            )
                        } else {
                            let ih_ref = ih_opt.as_ref().expect("int histogram");
                            let zero_ih = Histogram {
                                counter_reset_hint: CounterResetHint::CounterReset,
                                schema: ih_ref.schema,
                                zero_threshold: ih_ref.zero_threshold,
                                custom_values: ih_ref.custom_values.clone(),
                                ..Histogram::default()
                            };
                            app.append_histogram_reject_ooo(
                                series_ref, pairs, created_ts, zero_ih,
                            )
                        };
                        match ct_res {
                            Ok(r) => {
                                series_ref = Some(r);
                                stats.succeeded_samples += 1;
                            }
                            Err(
                                HeadError::DuplicateSampleForTimestamp
                                | HeadError::DuplicateSampleWithDetails(_)
                                | HeadError::OutOfOrderSample,
                            ) => {}
                            Err(err) => {
                                match handle_head_append_error(
                                    err,
                                    created_ts,
                                    raw_pairs_if_empty.as_deref().unwrap_or(pairs.as_slice()),
                                    ooo_window_ms,
                                    ooo_window_dur,
                                    &limits,
                                    &mut stats,
                                    &mut make_series_str,
                                    &mut record_partial,
                                ) {
                                Ok(()) => {}
                                Err(e) => {
                                    rollback_appender(user_id, app);
                                    return Err(e.with_user(user_id, 0));
                                }
                            }
                            }
                        }
                        ingest_created_ts = false;
                        created_ts = 0;
                    }

                    let res = if is_float {
                        app.append_float_histogram(
                            series_ref,
                            pairs,
                            0,
                            h_ts,
                            fh_opt.expect("float histogram"),
                        )
                    } else {
                        app.append_histogram(
                            series_ref,
                            pairs,
                            0,
                            h_ts,
                            ih_opt.expect("int histogram"),
                        )
                    };

                    match res {
                        Ok(r) => {
                            series_ref = Some(r);
                            stats.succeeded_samples += 1;
                        }
                        Err(err) => {
                            match handle_head_append_error(
                                err,
                                h_ts,
                                raw_pairs_if_empty.as_deref().unwrap_or(pairs.as_slice()),
                                ooo_window_ms,
                                ooo_window_dur,
                                &limits,
                                &mut stats,
                                &mut make_series_str,
                                &mut record_partial,
                            ) {
                                Ok(()) => {}
                                Err(e) => {
                                    rollback_appender(user_id, app);
                                    return Err(e.with_user(user_id, 0));
                                }
                            }
                        }
                    }
                }
                let last_h_ts = ts.histogram_timestamp(hist_count - 1);
                if samples.last().is_none_or(|ls| ls.timestamp_ms < last_h_ts) {
                    let (_, _, _, _, last_buckets) = ts.convert_histogram(hist_count - 1);
                    num_native_hist_buckets = last_buckets as i64;
                }
            }

            if self.cfg.active_series.active_series_metrics_enabled
                && stats.succeeded_samples > prev_succeeded
            {
                if let Some(ref_id) = series_ref {
                    let h = series_entry.as_ref().map_or(0, |e| e.hash);
                    db.active_series.update_series_with_entry(
                        series_entry.as_deref(),
                        pairs,
                        h,
                        ref_id,
                        start_append,
                        num_native_hist_buckets,
                        is_otlp,
                    );
                }
            }

            let ex_count = ts.exemplar_count();
            if ex_count > 0 && max_exemplars > 0 {
                if series_ref.is_none() {
                    match ts.exemplar_at(0) {
                        Ok((ex_ts, _, _, ex_str)) => {
                            let s_str = make_series_str(pairs);
                            record_partial(
                                IngesterError::exemplar_missing_series(ex_ts, s_str, ex_str),
                                None,
                            );
                        }
                        Err(ex_err) => {
                            let s_str = make_series_str(pairs);
                            record_partial(
                                IngesterError::exemplar_missing_series(
                                    ex_err.timestamp_ms,
                                    s_str,
                                    ex_err.exemplar_labels,
                                ),
                                None,
                            );
                        }
                    }
                    stats.failed_exemplars += ex_count;
                } else {
                    let mut out_of_order_exemplars = 0usize;
                    for e_idx in 0..ex_count {
                        let (ex_ts, ex_val, ex_labels, ex_str) = match ts.exemplar_at(e_idx) {
                            Ok(v) => v,
                            Err(ex_err) => {
                                stats.failed_exemplars += 1;
                                let s_str = make_series_str(pairs);
                                let err = if ex_err.timestamp_ms > max_timestamp_ms {
                                    IngesterError::exemplar_too_far_in_future(
                                        ex_err.timestamp_ms,
                                        s_str,
                                        ex_err.exemplar_labels,
                                    )
                                } else if ex_err.timestamp_ms < min_timestamp_ms {
                                    IngesterError::exemplar_too_far_in_past(
                                        ex_err.timestamp_ms,
                                        s_str,
                                        ex_err.exemplar_labels,
                                    )
                                } else {
                                    IngesterError::TsdbIngestExemplar {
                                        original_err: ex_err.message,
                                        timestamp_ms: ex_err.timestamp_ms,
                                        series_labels: s_str,
                                        exemplar_labels: ex_err.exemplar_labels,
                                    }
                                };
                                record_partial(err, None);
                                continue;
                            }
                        };
                        if ex_ts > max_timestamp_ms {
                            stats.failed_exemplars += 1;
                            let s_str = make_series_str(pairs);
                            record_partial(
                                IngesterError::exemplar_too_far_in_future(ex_ts, s_str, ex_str),
                                None,
                            );
                            continue;
                        }
                        if ex_ts < min_timestamp_ms {
                            stats.failed_exemplars += 1;
                            let s_str = make_series_str(pairs);
                            record_partial(
                                IngesterError::exemplar_too_far_in_past(ex_ts, s_str, ex_str),
                                None,
                            );
                            continue;
                        }
                        let exemplar = Exemplar::new(ex_labels.clone(), ex_val, ex_ts);
                        match app.append_exemplar(series_ref, pairs, exemplar) {
                            Ok(_) => stats.succeeded_exemplars += 1,
                            Err(HeadError::Exemplar(ExemplarError::OutOfOrderExemplar)) => {
                                stats.failed_exemplars += 1;
                                out_of_order_exemplars += 1;
                                if out_of_order_exemplars == ex_count && !ignore_ooo_exemplars {
                                    let s_str = make_series_str(pairs);
                                    record_partial(
                                        IngesterError::TsdbIngestExemplar {
                                            original_err: "out of order exemplar".to_string(),
                                            timestamp_ms: ex_ts,
                                            series_labels: s_str,
                                            exemplar_labels: ex_str,
                                        },
                                        None,
                                    );
                                }
                            }
                            Err(HeadError::Exemplar(ex_err)) => {
                                stats.failed_exemplars += 1;
                                let s_str = make_series_str(pairs);
                                record_partial(
                                    IngesterError::TsdbIngestExemplar {
                                        original_err: ex_err.to_string(),
                                        timestamp_ms: ex_ts,
                                        series_labels: s_str,
                                        exemplar_labels: ex_str,
                                    },
                                    None,
                                );
                            }
                            Err(e) => {
                                stats.failed_exemplars += 1;
                                let s_str = make_series_str(pairs);
                                record_partial(
                                    IngesterError::TsdbIngestExemplar {
                                        original_err: e.to_string(),
                                        timestamp_ms: ex_ts,
                                        series_labels: s_str,
                                        exemplar_labels: ex_str,
                                    },
                                    None,
                                );
                            }
                        }
                    }
                }
            }
        }

        if let Some(cast) = cast.as_deref() {
            attribute_discarded(cast, &mut ca_prev, &stats, pairs.iter().copied(), start_append);
        }
        self.metrics
            .appender_add_duration
            .observe(add_timer.elapsed().as_secs_f64());

        let commit_timer = Instant::now();
        if let Err(e) = app.commit() {
            return Err(IngesterError::Internal {
                message: format!("failed to commit TSDB appender: {e}"),
            }
            .with_user(user_id, stats.failed_samples as i64));
        }
        self.metrics
            .appender_commit_duration
            .observe(commit_timer.elapsed().as_secs_f64());

        if stats.succeeded_samples > 0 {
            db.set_last_update(SystemTime::now());
        }

        db.ingested_samples_counter(&self.metrics.ingested_samples)
            .inc_by(stats.succeeded_samples as f64);
        db.ingested_samples_fail_counter(&self.metrics.ingested_samples_fail)
            .inc_by(stats.failed_samples as f64);
        self.metrics
            .ingested_exemplars
            .inc_by(stats.succeeded_exemplars as f64);
        self.metrics
            .ingested_exemplars_fail
            .inc_by(stats.failed_exemplars as f64);
        self.appended_samples
            .fetch_add(stats.succeeded_samples as i64, Ordering::Relaxed);
        self.appended_exemplars
            .fetch_add(stats.succeeded_exemplars as i64, Ordering::Relaxed);
        self.usage_appended_samples.inc(stats.succeeded_samples as i64);
        self.usage_appended_exemplars.inc(stats.succeeded_exemplars as i64);

        let group = self.update_active_group(user_id, request_group, start_append);
        self.record_discarded_metrics(user_id, &group, &stats, source, db);

        if let Some((partial, sampler_kind)) = first_partial {
            let err = if let Some(kind) = sampler_kind {
                let sampler = kind.sampler(&self.error_samplers);
                partial.with_sampling_rate(sampler.should_log(), sampler.rate())
            } else {
                partial
            };
            return Err(err.with_user(user_id, stats.failed_samples as i64));
        }

        Ok(())
    }

/// Go: `i.activeGroups.UpdateActiveGroupTimestamp(userID, validation.GroupLabel(...), startAppend)`.
    fn update_active_group(&self, user_id: &str, group: String, now: SystemTime) -> String {
        match self.active_groups() {
            Some(svc) => svc.update_active_group_timestamp(user_id, group, now),
            None => group,
        }
    }

    fn record_discarded_metrics(
        &self,
        user_id: &str,
        group: &str,
        stats: &PushStats,
        source: cortexpb::write_request::SourceEnum,
        db: &UserTsdb,
    ) {
        let d = &self.metrics.discarded;
        if stats.sample_timestamp_too_old > 0 {
            d.sample_timestamp_too_old
                .with_label_values(&[user_id, group])
                .inc_by(stats.sample_timestamp_too_old as f64);
        }
        if stats.sample_out_of_order > 0 {
            d.sample_out_of_order
                .with_label_values(&[user_id, group])
                .inc_by(stats.sample_out_of_order as f64);
        }
        if stats.sample_too_old > 0 {
            d.sample_too_old
                .with_label_values(&[user_id, group])
                .inc_by(stats.sample_too_old as f64);
        }
        if stats.sample_too_far_in_future > 0 {
            d.sample_too_far_in_future
                .with_label_values(&[user_id, group])
                .inc_by(stats.sample_too_far_in_future as f64);
        }
        if stats.new_value_for_timestamp > 0 {
            d.new_value_for_timestamp
                .with_label_values(&[user_id, group])
                .inc_by(stats.new_value_for_timestamp as f64);
        }
        if stats.per_user_series_limit > 0 {
            d.per_user_series_limit
                .with_label_values(&[user_id, group])
                .inc_by(stats.per_user_series_limit as f64);
        }
        if stats.per_metric_series_limit > 0 {
            d.per_metric_series_limit
                .with_label_values(&[user_id, group])
                .inc_by(stats.per_metric_series_limit as f64);
        }
        if stats.invalid_native_histogram > 0 {
            d.invalid_native_histogram
                .with_label_values(&[user_id, group])
                .inc_by(stats.invalid_native_histogram as f64);
        }
        if stats.labels_not_sorted > 0 {
            d.labels_not_sorted
                .with_label_values(&[user_id, group])
                .inc_by(stats.labels_not_sorted as f64);
        }
        if stats.series_with_floats_and_histograms > 0 {
            self.metrics
                .series_with_floats_and_histograms
                .with_label_values(&[user_id])
                .inc_by(stats.series_with_floats_and_histograms as f64);
        }
        if stats.succeeded_samples > 0 {
            self.ingestion_rate.add(stats.succeeded_samples as i64);
            if source == cortexpb::write_request::SourceEnum::Rule {
                db.ingested_rule_samples
                    .add(stats.succeeded_samples as i64);
            } else {
                db.ingested_api_samples.add(stats.succeeded_samples as i64);
            }
        }
    }

    fn check_ring_ownership(&self, user_id: &str, pairs: &[(&str, &str)]) -> bool {
        self.check_ring_ownership_iter(user_id, pairs.iter().copied())
    }

    fn check_ring_ownership_iter<'a>(
        &self,
        user_id: &str,
        pairs: impl Iterator<Item = (&'a str, &'a str)>,
    ) -> bool {
        let Some(ref ring) = self.instance_ring else {
            return true;
        };
        let token = shard_by_all_labels(user_id, pairs);
        let op = WRITE_NO_EXTEND;
        let shard_size = self
            .overrides
            .for_tenant(user_id)
            .ingestion_tenant_shard_size
            .max(0) as usize;
        let subring = if shard_size > 0 {
            ring.shuffle_shard(user_id, shard_size)
        } else {
            Arc::clone(ring)
        };
        let Ok(rs) = subring.get(token, op) else {
            return true;
        };
        rs.instances
            .iter()
            .any(|inst| inst.id == self.cfg.ring.instance_id)
    }
}

/// Label set arguments of [`handle_head_append_error`]: stringlabels-encoded bytes or sorted pairs.
trait LabelPairsArg {
    fn for_each_pair(&self, f: &mut dyn FnMut(&str, &str));
}

impl LabelPairsArg for [u8] {
    fn for_each_pair(&self, f: &mut dyn FnMut(&str, &str)) {
        for (k, v) in iter_stringlabels(self) {
            f(k, v);
        }
    }
}

impl LabelPairsArg for [(&str, &str)] {
    fn for_each_pair(&self, f: &mut dyn FnMut(&str, &str)) {
        for &(k, v) in self {
            f(k, v);
        }
    }
}

/// Go `fmt.Sprintf("%s", []mimirpb.LabelAdapter)`: `[{__name__ m} {job api}]`.
fn format_label_adapters<L: ?Sized + LabelPairsArg>(labels: &L) -> String {
    let mut out = String::from("[");
    let mut first = true;
    labels.for_each_pair(&mut |k, v| {
        if !first {
            out.push(' ');
        }
        first = false;
        out.push('{');
        out.push_str(k);
        out.push(' ');
        out.push_str(v);
        out.push('}');
    });
    out.push(']');
    out
}

#[allow(clippy::too_many_arguments)]
fn handle_head_append_error<L: ?Sized + LabelPairsArg>(
    err: HeadError,
    ts: i64,
    labels_arg: &L,
    ooo_window_ms: i64,
    ooo_window_dur: Duration,
    limits: &LimitsConfig,
    stats: &mut PushStats,
    make_series_str: &mut dyn FnMut(&L) -> String,
    record_partial: &mut dyn FnMut(IngesterError, Option<SampleKind>),
) -> Result<(), IngesterError> {
    stats.failed_samples += 1;
    match err {
        HeadError::OutOfBounds => {
            stats.sample_timestamp_too_old += 1;
            let s_str = make_series_str(labels_arg);
            record_partial(
                IngesterError::sample_timestamp_too_old(ts, s_str),
                Some(SampleKind::SampleTimestampTooOld),
            );
            Ok(())
        }
        HeadError::OutOfOrderSample => {
            stats.sample_out_of_order += 1;
            let s_str = make_series_str(labels_arg);
            record_partial(
                IngesterError::sample_out_of_order(ts, s_str),
                Some(SampleKind::SampleOutOfOrder),
            );
            Ok(())
        }
        HeadError::TooOldSample => {
            stats.sample_too_old += 1;
            let s_str = make_series_str(labels_arg);
            if ooo_window_ms > 0 {
                record_partial(
                    IngesterError::sample_timestamp_too_old_ooo_enabled(ts, s_str, ooo_window_dur),
                    Some(SampleKind::SampleTimestampTooOldOooEnabled),
                );
            } else {
                record_partial(
                    IngesterError::sample_timestamp_too_old(ts, s_str),
                    Some(SampleKind::SampleTimestampTooOld),
                );
            }
            Ok(())
        }
        HeadError::DuplicateSampleForTimestamp => {
            stats.new_value_for_timestamp += 1;
            let s_str = make_series_str(labels_arg);
            record_partial(
                IngesterError::sample_duplicate_timestamp("duplicate sample for timestamp", ts, s_str),
                Some(SampleKind::SampleDuplicateTimestamp),
            );
            Ok(())
        }
        HeadError::DuplicateSampleWithDetails(msg) => {
            stats.new_value_for_timestamp += 1;
            let s_str = make_series_str(labels_arg);
            record_partial(
                IngesterError::sample_duplicate_timestamp(msg, ts, s_str),
                Some(SampleKind::SampleDuplicateTimestamp),
            );
            Ok(())
        }
        HeadError::HistogramValidation(err) => {
            // globalerror.MapNativeHistogramErr: only 8 sentinels are soft errors; any other
            // validation error aborts the request with a 500.
            let Some(err_id) = map_native_histogram_err(err.kind) else {
                warn!(msg = "unknown histogram error", err = %err);
                return Err(IngesterError::Internal {
                    message: err.to_string(),
                });
            };
            stats.invalid_native_histogram += 1;
            record_partial(
                IngesterError::NativeHistogramValidation {
                    err_id,
                    original_err: err.to_string(),
                    series: format_label_adapters(labels_arg),
                    timestamp_ms: ts,
                },
                Some(SampleKind::NativeHistogramValidation),
            );
            Ok(())
        }
        HeadError::MaxSeriesPerUser => {
            stats.per_user_series_limit += 1;
            record_partial(
                IngesterError::per_user_series_limit_reached(
                    limits.max_global_series_per_user.max(0) as usize,
                ),
                Some(SampleKind::MaxSeriesPerUser),
            );
            Ok(())
        }
        HeadError::MaxSeriesPerMetric => {
            stats.per_metric_series_limit += 1;
            let s_str = make_series_str(labels_arg);
            record_partial(
                IngesterError::per_metric_series_limit_reached(
                    limits.max_global_series_per_metric.max(0) as usize,
                    s_str,
                ),
                Some(SampleKind::MaxSeriesPerMetric),
            );
            Ok(())
        }
        // `extract.MetricNameFromLabels` in `PreCreation` / `getOrCreate`'s empty label set check:
        // hard errors in Go (not handled by the soft error processor).
        e @ (HeadError::MissingMetricName | HeadError::EmptyLabelset) => Err(IngesterError::Internal {
            message: e.to_string(),
        }),
        HeadError::MaxInMemorySeriesReached => Err(IngesterError::max_in_memory_series_reached()),
        other => Err(IngesterError::Internal {
            message: other.to_string(),
        }),
    }
}

fn min_timestamp_owned(ts_list: &[cortexpb::TimeSeries]) -> i64 {
    let mut min_ts = i64::MAX;
    for ts in ts_list {
        for s in &ts.samples {
            min_ts = min_ts.min(s.timestamp_ms);
        }
        for h in &ts.histograms {
            min_ts = min_ts.min(h.timestamp);
        }
        for ex in &ts.exemplars {
            min_ts = min_ts.min(ex.timestamp_ms);
        }
    }
    min_ts
}

pub fn empty_hashset() -> HashSet<String> {
    HashSet::new()
}

#[cfg(test)]
mod wave3_tests {
    use super::*;
    use bytes::Bytes;

    fn lp(name: &str, value: &str) -> cortexpb::LabelPair {
        cortexpb::LabelPair {
            name: Bytes::copy_from_slice(name.as_bytes()),
            value: Bytes::copy_from_slice(value.as_bytes()),
        }
    }

    #[test]
    fn wave3_unsorted_labels_exemplar_only_ok_and_disabled_histograms_counted() {
        let tmp = tempfile::tempdir().unwrap();
        let mut cfg = mimir_config::IngesterConfig::default();
        cfg.ring.instance_id = "test-0".to_string();
        let mut blocks_cfg = mimir_config::BlocksStorageConfig::default();
        blocks_cfg.tsdb.dir = tmp.path().to_string_lossy().into_owned();
        let limits = mimir_config::LimitsConfig {
            native_histograms_ingestion_enabled: false,
            ..Default::default()
        };
        let ing = Ingester::new(
            cfg,
            blocks_cfg,
            mimir_config::IngestStorageConfig::default(),
            mimir_config::Overrides::new(limits),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        ing.set_state_for_test(mimir_common::services::State::Running);

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        // 1. Exemplar-only TimeSeries with unsorted labels must return Ok(()) on both push paths.
        let ex_only_req = cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![lp("z", "1"), lp("__name__", "unsort_ex_only")],
                exemplars: vec![cortexpb::Exemplar {
                    labels: vec![lp("t", "1")],
                    value: 1.0,
                    timestamp_ms: now,
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(ing.push_to_storage("t-no-nh", &ex_only_req).is_ok());
        assert!(ing
            .push_grpc_raw_sync(
                "t-no-nh",
                &mimir_proto::prost::Message::encode_to_vec(&ex_only_req)
            )
            .is_ok());

        // 2. Unsorted labels with 2 histograms when native_histograms_ingestion_enabled=false
        // must report rejected_samples == 2 in ErrorDetails on both push paths.
        let mk_hist = |ts: i64, d: i64| cortexpb::Histogram {
            count: Some(cortexpb::histogram::Count::CountInt(d as u64)),
            zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountInt(0)),
            sum: d as f64 * 1.5,
            schema: 0,
            positive_spans: vec![cortexpb::BucketSpan {
                offset: 0,
                length: 1,
            }],
            positive_deltas: vec![d],
            timestamp: ts,
            ..Default::default()
        };
        let hist_req = cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![lp("z", "1"), lp("__name__", "unsort_hist_disabled")],
                histograms: vec![mk_hist(now, 2), mk_hist(now + 1000, 3)],
                ..Default::default()
            }],
            ..Default::default()
        };

        let err1 = ing.push_to_storage("t-no-nh", &hist_req).unwrap_err();
        assert_eq!(err1.rejected_samples(), 2);

        let err2 = ing
            .push_grpc_raw_sync(
                "t-no-nh",
                &mimir_proto::prost::Message::encode_to_vec(&hist_req),
            )
            .unwrap_err();
        assert_eq!(err2.rejected_samples(), 2);
    }

    #[test]
    fn classic_push_sharded_preserves_atomic_rollback_and_partial_error_order() {
        let tmp = tempfile::tempdir().unwrap();
        let mut cfg = mimir_config::IngesterConfig::default();
        cfg.ring.instance_id = "test-0".to_string();
        let mut blocks_cfg = mimir_config::BlocksStorageConfig::default();
        blocks_cfg.tsdb.dir = tmp.path().to_string_lossy().into_owned();
        let ing = Ingester::new(
            cfg,
            blocks_cfg,
            mimir_config::IngestStorageConfig::default(),
            mimir_config::Overrides::new(mimir_config::LimitsConfig::default()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        ing.set_state_for_test(mimir_common::services::State::Running);

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        // 1. Hard error (`MissingMetricName`) at index 400 rolls back ALL 600 series.
        let mut rollback_ts = Vec::with_capacity(600);
        for i in 0..600 {
            let labels = if i == 400 {
                vec![lp("job", "missing_name")]
            } else {
                vec![
                    lp("__name__", "sharded_metric"),
                    lp("id", &format!("{i:04}")),
                ]
            };
            rollback_ts.push(cortexpb::TimeSeries {
                labels,
                samples: vec![cortexpb::Sample {
                    timestamp_ms: now,
                    value: i as f64,
                }],
                ..Default::default()
            });
        }
        let rollback_req = cortexpb::WriteRequest {
            timeseries: rollback_ts,
            ..Default::default()
        };
        let err = ing
            .push_grpc_raw_sync(
                "t-shard-rb",
                &mimir_proto::prost::Message::encode_to_vec(&rollback_req),
            )
            .unwrap_err();
        assert_eq!(err.error_cause(), cortexpb::ErrorCause::Unknown);
        let m = mimir_tsdb::matcher::LabelMatcher::new(
            mimir_tsdb::matcher::MatchType::Equal,
            "__name__",
            "sharded_metric",
        )
        .unwrap();
        let committed = ing
            .get_tsdb("t-shard-rb")
            .unwrap()
            .db
            .chunk_querier(0, i64::MAX)
            .select(false, &[m])
            .unwrap();
        assert!(committed.is_empty(), "all samples must be rolled back");

        // 2. Large valid batch (600 series) succeeds & updates cached ingested_samples counter.
        let mut ok_ts = Vec::with_capacity(600);
        for i in 0..600 {
            ok_ts.push(cortexpb::TimeSeries {
                labels: vec![
                    lp("__name__", "sharded_metric"),
                    lp("id", &format!("{i:04}")),
                ],
                samples: vec![cortexpb::Sample {
                    timestamp_ms: now,
                    value: i as f64,
                }],
                ..Default::default()
            });
        }
        let ok_req = cortexpb::WriteRequest {
            timeseries: ok_ts,
            ..Default::default()
        };
        ing.push_grpc_raw_sync(
            "t-shard-ok",
            &mimir_proto::prost::Message::encode_to_vec(&ok_req),
        )
        .unwrap();
        let db = ing.get_tsdb("t-shard-ok").unwrap();
        assert_eq!(db.db.head().num_series(), 600);
        assert_eq!(
            db.ingested_samples_counter(&ing.metrics.ingested_samples)
                .get(),
            600.0
        );

        // 3. Soft errors across multiple series pick the earliest series' error & sum rejected_samples.
        let mut soft_ts = Vec::with_capacity(400);
        for i in 0..400 {
            let (ts_labels, ts_ms) = if i == 12 {
                // First soft error: unsorted labels
                (
                    vec![lp("z", "1"), lp("__name__", "sharded_metric")],
                    now + 1000,
                )
            } else if i == 300 {
                // Later soft error: sample too far in future
                (
                    vec![
                        lp("__name__", "sharded_metric"),
                        lp("id", &format!("{i:04}")),
                    ],
                    now + 10_000_000,
                )
            } else {
                (
                    vec![
                        lp("__name__", "sharded_metric"),
                        lp("id", &format!("{i:04}")),
                    ],
                    now + 1000,
                )
            };
            soft_ts.push(cortexpb::TimeSeries {
                labels: ts_labels,
                samples: vec![cortexpb::Sample {
                    timestamp_ms: ts_ms,
                    value: i as f64,
                }],
                ..Default::default()
            });
        }
        let soft_req = cortexpb::WriteRequest {
            timeseries: soft_ts,
            ..Default::default()
        };
        let soft_err = ing
            .push_grpc_raw_sync(
                "t-shard-ok",
                &mimir_proto::prost::Message::encode_to_vec(&soft_req),
            )
            .unwrap_err();
        assert_eq!(soft_err.rejected_samples(), 2);
        assert!(
            format!("{soft_err:?}").contains("LabelsNotSorted"),
            "expected earliest soft error LabelsNotSorted, got: {soft_err:?}"
        );
        assert_eq!(
            db.ingested_samples_counter(&ing.metrics.ingested_samples)
                .get(),
            998.0
        );
        assert_eq!(
            db.ingested_samples_fail_counter(&ing.metrics.ingested_samples_fail)
                .get(),
            2.0
        );
    }

    #[test]
    fn push_rollback_persists_new_series_for_subsequent_push_and_wal_replay() {
        let tmp = tempfile::tempdir().unwrap();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        {
            let mut ing_cfg = mimir_config::IngesterConfig::default();
            ing_cfg.ring.instance_id = "test-rb".to_string();
            let mut blocks_cfg = mimir_config::BlocksStorageConfig::default();
            blocks_cfg.tsdb.dir = tmp.path().to_string_lossy().to_string();
            blocks_cfg.tsdb.wal_compression_enabled = false;
            let ing = Ingester::new(
                ing_cfg,
                blocks_cfg,
                mimir_config::IngestStorageConfig::default(),
                mimir_config::Overrides::new(mimir_config::LimitsConfig::default()),
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .unwrap();
            ing.set_state_for_test(mimir_common::services::State::Running);

            // Batch 1 (sharded >= CLASSIC_PUSH_SHARD_THRESHOLD series): creates s_new (idx 0..4100)
            // and triggers hard error (MissingMetricName) at idx 4100, rolling back the whole batch.
            const BATCH1_ERR_IDX: usize = CLASSIC_PUSH_SHARD_THRESHOLD + 4;
            let mut batch1 = Vec::with_capacity(BATCH1_ERR_IDX + 1);
            for i in 0..=BATCH1_ERR_IDX {
                let labels = if i == BATCH1_ERR_IDX {
                    vec![lp("job", "missing_metric_name")]
                } else {
                    vec![lp("__name__", "rb_replay_metric"), lp("id", &format!("{i:04}"))]
                };
                batch1.push(cortexpb::TimeSeries {
                    labels,
                    samples: vec![cortexpb::Sample {
                        timestamp_ms: now - 10_000,
                        value: 1.0,
                    }],
                    ..Default::default()
                });
            }
            let req1 = cortexpb::WriteRequest {
                timeseries: batch1,
                ..Default::default()
            };
            ing.push_grpc_raw_sync(
                "tenant-rb-wal",
                &mimir_proto::prost::Message::encode_to_vec(&req1),
            )
            .unwrap_err();

            // Batch 2: pushes valid samples to the first newly created series s_new.
            let req2 = cortexpb::WriteRequest {
                timeseries: vec![cortexpb::TimeSeries {
                    labels: vec![lp("__name__", "rb_replay_metric"), lp("id", "0000")],
                    samples: vec![cortexpb::Sample {
                        timestamp_ms: now,
                        value: 77.5,
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            };
            ing.push_grpc_raw_sync(
                "tenant-rb-wal",
                &mimir_proto::prost::Message::encode_to_vec(&req2),
            )
            .unwrap();

            mimir_common::block_on_for_tests(ing.close_all_tsdbs());
        }

        // Reopen TSDB directly from WAL on disk and assert s_new + Batch 2 sample survived!
        let user_dir = tmp.path().join("tenant-rb-wal");
        let reopened = mimir_tsdb::db::Tsdb::open(
            &user_dir,
            mimir_tsdb::db::TsdbOptions::default(),
        )
        .unwrap();
        let m = mimir_tsdb::matcher::LabelMatcher::new(
            mimir_tsdb::matcher::MatchType::Equal,
            "id",
            "0000",
        )
        .unwrap();
        let series = reopened
            .chunk_querier(0, i64::MAX)
            .select(false, &[m])
            .unwrap();
        assert_eq!(series.len(), 1, "s_new must survive WAL replay");
        let vals: Vec<(i64, f64)> = series[0]
            .chunks
            .iter()
            .flat_map(|c| mimir_tsdb::compact::decode_chunk_samples(c.chunk.as_ref().unwrap()).unwrap())
            .map(|s| {
                let mimir_tsdb::compact::SampleValue::Float(v) = s.value else {
                    panic!("expected float");
                };
                (s.t, v)
            })
            .collect();
        assert_eq!(vals, vec![(now, 77.5)]);
    }

    #[test]
    fn push_raw_invalid_utf8_exemplar_commits_valid_samples_and_returns_partial_error() {
        let tmp = tempfile::tempdir().unwrap();
        let mut ing_cfg = mimir_config::IngesterConfig::default();
        ing_cfg.ring.instance_id = "test-ex".to_string();
        let mut blocks_cfg = mimir_config::BlocksStorageConfig::default();
        blocks_cfg.tsdb.dir = tmp.path().to_string_lossy().to_string();
        let limits = mimir_config::LimitsConfig {
            max_global_exemplars_per_user: 100,
            ..Default::default()
        };
        let ing = Ingester::new(
            ing_cfg,
            blocks_cfg,
            mimir_config::IngestStorageConfig::default(),
            mimir_config::Overrides::new(limits),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        ing.set_state_for_test(mimir_common::services::State::Running);

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let req = cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![lp("__name__", "ex_utf8_metric")],
                samples: vec![cortexpb::Sample {
                    timestamp_ms: now,
                    value: 12.25,
                }],
                exemplars: vec![cortexpb::Exemplar {
                    labels: vec![cortexpb::LabelPair {
                        name: bytes::Bytes::from_static(b"trace_id"),
                        value: bytes::Bytes::from_static(&[0xff, 0xfe]),
                    }],
                    value: 1.0,
                    timestamp_ms: now,
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let err = ing
            .push_grpc_raw_sync("t-ex-utf8", &mimir_proto::prost::Message::encode_to_vec(&req))
            .unwrap_err();
        assert!(
            err.to_string().contains("invalid UTF-8"),
            "unexpected error: {err}"
        );
        let db = ing.get_tsdb("t-ex-utf8").unwrap();
        assert_eq!(db.db.head().num_series(), 1, "valid sample must still commit");
        assert_eq!(
            db.ingested_samples_counter(&ing.metrics.ingested_samples)
                .get(),
            1.0
        );
        assert_eq!(ing.metrics.ingested_exemplars_fail.get(), 1.0);
    }

    #[test]
    fn oem04_push_metadata_items_with_trace_creates_store_when_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let reg = prometheus::Registry::new();
        let mut blocks_cfg = mimir_config::BlocksStorageConfig::default();
        blocks_cfg.tsdb.dir = tmp.path().to_string_lossy().into_owned();
        let ing = Ingester::new(
            mimir_config::IngesterConfig::default(),
            blocks_cfg,
            mimir_config::IngestStorageConfig::default(),
            mimir_config::Overrides::new(mimir_config::LimitsConfig::default()),
            None,
            None,
            None,
            None,
            None,
            Some(&reg),
        )
        .unwrap();

        assert_eq!(ing.push_metadata_items_with_trace("t-empty-meta", &[], None), 0);
        assert!(ing.get_user_metadata("t-empty-meta").is_some());
        ing.purge_user_metrics_metadata();
        let val = reg
            .gather()
            .into_iter()
            .find(|mf| mf.get_name() == "cortex_ingester_memory_metadata_removed_total")
            .and_then(|mf| {
                mf.get_metric()
                    .iter()
                    .find(|m| {
                        m.get_label()
                            .iter()
                            .any(|l| l.get_name() == "user" && l.get_value() == "t-empty-meta")
                    })
                    .map(|m| m.get_counter().get_value())
            });
        assert_eq!(val, Some(0.0));
    }
}
