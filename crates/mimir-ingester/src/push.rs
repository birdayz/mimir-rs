//! Zero-copy Push pipeline, validation, limits, and error accumulation.
//!
//! Ported from github.com/grafana/mimir/pkg/ingester/ingester_push.go.

use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use mimir_common::limiter::{InflightPermit, ReactivePermit};
use mimir_config::LimitsConfig;
use mimir_proto::cortexpb;
use mimir_proto::prost::Message;
use mimir_proto::sharding::shard_by_all_labels;
use mimir_proto::zerocopy::{BorrowedTimeSeriesView, BorrowedWriteRequest};
use mimir_ring::ring::replication_strategy::WRITE_NO_EXTEND;
use mimir_tsdb::chunkenc::{CounterResetHint, FloatHistogram, Histogram, HistogramSpan};
use mimir_tsdb::exemplar::{Exemplar, ExemplarError};
use mimir_tsdb::head::HeadError;
use mimir_tsdb::labels::Labels;
use smallvec::SmallVec;
use tracing::warn;

use crate::circuit_breaker::BreakerPermit;
use crate::errors::{
    format_labels_string, map_native_histogram_err_id, ErrorSampler, ErrorSamplers,
    IngesterError,
};
use crate::ingester::Ingester;
use crate::metrics::{
    REASON_MAX_INFLIGHT_PUSH_REQUESTS, REASON_MAX_INFLIGHT_PUSH_REQUESTS_BYTES,
    REASON_MAX_INGESTION_RATE,
};
use crate::user_tsdb::UserTsdb;

pub struct PushPermit<'a> {
    pub started_at: Instant,
    pub breaker_permit: Option<BreakerPermit>,
    pub reactive_permit: Option<ReactivePermit>,
    pub inflight_permit: Option<InflightPermit<'a>>,
    pub ingester: &'a Ingester,
    pub req_size: i64,
}

impl PushPermit<'_> {
    pub fn finish(self, err: Option<&IngesterError>) {
        let dur = self.started_at.elapsed();
        self.finish_with_duration(dur, err);
    }

    pub fn finish_with_duration(mut self, dur: Duration, err: Option<&IngesterError>) {
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

pub(crate) type ConvertedHistogram = (i64, bool, Option<Histogram>, Option<FloatHistogram>, usize);
pub(crate) type SeriesFormatter<'b, 'a> = &'b mut dyn FnMut(&[(&'a str, &'a str)]) -> String;

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

/// Abstract view over a single time series in either owned `cortexpb::TimeSeries` or zero-copy `BorrowedTimeSeriesView`.
trait TimeSeriesInput<'a> {
    fn fill_pairs(&self, out: &mut SmallVec<[(&'a str, &'a str); 24]>) -> Result<(), IngesterError>;
    fn samples(&self) -> &[cortexpb::Sample];
    fn created_timestamp(&self) -> i64;
    fn histogram_count(&self) -> usize;
    fn histogram_timestamp(&self, idx: usize) -> i64;
    fn convert_histogram(&self, idx: usize) -> ConvertedHistogram;
    fn exemplar_count(&self) -> usize;
    fn exemplar_at(&self, idx: usize) -> Result<(i64, f64, Labels, String), IngesterError>;
}

impl<'a> TimeSeriesInput<'a> for &'a cortexpb::TimeSeries {
    fn fill_pairs(&self, out: &mut SmallVec<[(&'a str, &'a str); 24]>) -> Result<(), IngesterError> {
        out.clear();
        for lp in &self.labels {
            let k = lp.name_str().map_err(|_| IngesterError::BadRequest {
                message: "invalid UTF-8 in label name".to_string(),
            })?;
            let v = lp.value_str().map_err(|_| IngesterError::BadRequest {
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
                Some(cortexpb::histogram::ZeroCount::ZeroCountInt(v)) => v as f64,
                None => 0.0,
            };
            let buckets = h.positive_counts.len() + h.negative_counts.len();
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
                Some(cortexpb::histogram::ZeroCount::ZeroCountFloat(v)) => v as u64,
                None => 0,
            };
            let buckets = h.positive_deltas.len() + h.negative_deltas.len();
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

    fn exemplar_at(&self, idx: usize) -> Result<(i64, f64, Labels, String), IngesterError> {
        let ex = &self.exemplars[idx];
        let mut pairs = Vec::with_capacity(ex.labels.len());
        for lp in &ex.labels {
            let k = lp.name_str().map_err(|_| IngesterError::BadRequest {
                message: "invalid UTF-8 in exemplar label name".to_string(),
            })?;
            let v = lp.value_str().map_err(|_| IngesterError::BadRequest {
                message: "invalid UTF-8 in exemplar label value".to_string(),
            })?;
            pairs.push((k, v));
        }
        let ex_str = format_labels_string(pairs.iter().copied());
        let labels = Labels::from_pairs(pairs);
        Ok((ex.timestamp_ms, ex.value, labels, ex_str))
    }
}

impl<'a, 'v> TimeSeriesInput<'a> for BorrowedTimeSeriesView<'v, 'a> {
    fn fill_pairs(&self, out: &mut SmallVec<[(&'a str, &'a str); 24]>) -> Result<(), IngesterError> {
        out.clear();
        for lp in self.labels() {
            let k = lp.name_str().map_err(|_| IngesterError::BadRequest {
                message: "invalid UTF-8 in label name".to_string(),
            })?;
            let v = lp.value_str().map_err(|_| IngesterError::BadRequest {
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
                Some(cortexpb::histogram::ZeroCount::ZeroCountInt(v)) => v as f64,
                None => 0.0,
            };
            let buckets = h.positive_counts().len() + h.negative_counts().len();
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
                Some(cortexpb::histogram::ZeroCount::ZeroCountFloat(v)) => v as u64,
                None => 0,
            };
            let buckets = h.positive_deltas().len() + h.negative_deltas().len();
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

    fn exemplar_count(&self) -> usize {
        self.exemplars().len()
    }

    fn exemplar_at(&self, idx: usize) -> Result<(i64, f64, Labels, String), IngesterError> {
        let ex = self.exemplars().nth(idx).expect("valid exemplar idx");
        let mut pairs = Vec::with_capacity(ex.labels().len());
        for lp in ex.labels() {
            let k = lp.name_str().map_err(|_| IngesterError::BadRequest {
                message: "invalid UTF-8 in exemplar label name".to_string(),
            })?;
            let v = lp.value_str().map_err(|_| IngesterError::BadRequest {
                message: "invalid UTF-8 in exemplar label value".to_string(),
            })?;
            pairs.push((k, v));
        }
        let ex_str = format_labels_string(pairs.iter().copied());
        let labels = Labels::from_pairs(pairs);
        Ok((ex.timestamp_ms(), ex.value(), labels, ex_str))
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
                    message: "reactive limiter exceeded".to_string(),
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

        if let Err(err) = self.check_instance_limits(inflight, inflight_bytes, reject_equal) {
            self.inflight_push_requests
                .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
            if req_size > 0 {
                self.inflight_push_requests_bytes
                    .fetch_sub(req_size, std::sync::atomic::Ordering::AcqRel);
            }
            if let Some(bp) = breaker_permit {
                bp.finish(started_at.elapsed(), false);
            }
            if let Some(rp) = reactive_permit {
                rp.drop_permit();
            }
            return Err(err);
        }

        Ok(PushPermit {
            started_at,
            breaker_permit,
            reactive_permit,
            inflight_permit: None,
            ingester: self,
            req_size,
        })
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
        self.check_available_for_push()?;
        let started_at = Instant::now();
        let breaker_permit = self.circuit_breaker.try_acquire_push_permit()?;
        let inflight = self
            .inflight_push_requests
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
            + 1;
        let inflight_bytes = if req_size > 0 {
            self.inflight_push_requests_bytes
                .fetch_add(req_size, std::sync::atomic::Ordering::AcqRel)
                + req_size
        } else {
            self.inflight_push_requests_bytes
                .load(std::sync::atomic::Ordering::Acquire)
        };
        if let Err(err) = self.check_instance_limits(inflight, inflight_bytes, req_size <= 0) {
            self.inflight_push_requests
                .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
            if req_size > 0 {
                self.inflight_push_requests_bytes
                    .fetch_sub(req_size, std::sync::atomic::Ordering::AcqRel);
            }
            if let Some(bp) = breaker_permit {
                bp.finish(started_at.elapsed(), false);
            }
            return Err(err);
        }

        let res = self.push_borrowed_inner(user_id, req);

        self.inflight_push_requests
            .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
        if req_size > 0 {
            self.inflight_push_requests_bytes
                .fetch_sub(req_size, std::sync::atomic::Ordering::AcqRel);
        }
        if let Some(bp) = breaker_permit {
            bp.finish(started_at.elapsed(), false);
        }
        res
    }

    pub async fn push_write_request(
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
        let iter = req.timeseries.iter();
        let res = self.ingest_timeseries(
            user_id,
            &db,
            source,
            req.skip_label_validation,
            req.skip_label_count_validation,
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
        }
        let lock_guard = db
            .acquire_append_lock(min_ts)
            .map_err(|e| e.with_user(user_id, 0))?;

        let source = cortexpb::write_request::SourceEnum::try_from(req.source)
            .unwrap_or(cortexpb::write_request::SourceEnum::Api);
        let res = self.ingest_timeseries(
            user_id,
            &db,
            source,
            req.skip_label_validation,
            req.skip_label_count_validation,
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
        if metadata.is_empty() {
            return 0;
        }
        let store = self.get_or_create_user_metadata(user_id);
        let mut ingested = 0usize;
        let mut failed = 0usize;
        let mut first_err: Option<IngesterError> = None;
        for m in metadata {
            match store.add(m) {
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
            warn!(user = %user_id, error = %err, "failed to ingest some metadata");
        }
        ingested
    }

    fn push_metadata_borrowed(
        &self,
        user_id: &str,
        req: &BorrowedWriteRequest<'_, '_>,
    ) -> usize {
        if req.metadata().len() == 0 {
            return 0;
        }
        let store = self.get_or_create_user_metadata(user_id);
        let mut ingested = 0usize;
        let mut failed = 0usize;
        for bm in req.metadata() {
            let owned = cortexpb::MetricMetadata {
                r#type: bm.metric_type,
                metric_family_name: bm.metric_family_name.to_string(),
                help: bm.help.to_string(),
                unit: bm.unit.to_string(),
            };
            match store.add(&owned) {
                Ok(()) => ingested += 1,
                Err(_) => failed += 1,
            }
        }
        self.metrics.ingested_metadata.inc_by(ingested as f64);
        self.metrics.ingested_metadata_fail.inc_by(failed as f64);
        ingested
    }

    fn ingest_timeseries<'a, I, T>(
        &self,
        user_id: &str,
        db: &UserTsdb,
        source: cortexpb::write_request::SourceEnum,
        skip_label_validation: bool,
        _skip_label_count_validation: bool,
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
        let max_hist_buckets = limits.max_native_histogram_buckets.max(0) as usize;
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

        let mut app = db.db.appender();
        let mut stats = PushStats::default();
        let mut first_partial: Option<(IngesterError, Option<SampleKind>)> = None;
        let mut pairs: SmallVec<[(&str, &str); 24]> = SmallVec::new();
        let mut series_str_cache: Option<String>;

        let mut record_partial = |err: IngesterError, kind: Option<SampleKind>| {
            if first_partial.is_none() {
                first_partial = Some((err, kind));
            }
        };

        for ts in timeseries {
            series_str_cache = None;
            let samples = ts.samples();
            let hist_count = ts.histogram_count();
            if !samples.is_empty() && hist_count > 0 {
                stats.series_with_floats_and_histograms += 1;
            }

            if let Err(e) = ts.fill_pairs(&mut pairs) {
                return Err(e.with_user(user_id, 0));
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
                        let s_str = format_labels_string(
                            pairs.iter().copied().filter(|(_, v)| !v.is_empty()),
                        );
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
                    let s_str = format_labels_string(
                        pairs.iter().copied().filter(|(_, v)| !v.is_empty()),
                    );
                    record_partial(
                        IngesterError::sample_timestamp_too_old(first_ts, s_str),
                        Some(SampleKind::SampleTimestampTooOld),
                    );
                    continue;
                }
            }

            // Look up existing ref (zero-alloc if pairs are already sorted & unique).
            // Strip blank label values after checking sorted/unique, matching Prometheus Labels.WithoutEmpty().
            let labels_sorted_unique = pairs.windows(2).all(|w| w[0].0 < w[1].0);
            pairs.retain(|(_, v)| !v.is_empty());

            let mut make_series_str = |ps: &[(&str, &str)]| -> String {
                if let Some(ref s) = series_str_cache {
                    return s.clone();
                }
                let s = format_labels_string(ps.iter().copied());
                series_str_cache = Some(s.clone());
                s
            };

            let mut series_ref = if labels_sorted_unique && !pairs.is_empty() {
                head.get_ref_by_sorted_pairs(&pairs)
            } else {
                None
            };

            if series_ref.is_none() && !labels_sorted_unique {
                let s_str = make_series_str(&pairs);
                let total_bad = samples.len() + if native_histograms_enabled { hist_count } else { 0 };
                stats.failed_samples += total_bad;
                stats.labels_not_sorted += total_bad;
                stats.failed_exemplars += ts.exemplar_count();
                record_partial(
                    IngesterError::labels_not_sorted(s_str),
                    Some(SampleKind::LabelsNotSorted),
                );
                continue;
            }

            // Additional validation when not skipped
            if series_ref.is_none() {
                if !skip_label_validation {
                    if let Some(err) = validate_label_pairs(&pairs, &limits, &mut make_series_str) {
                        let total_bad = samples.len()
                            + if native_histograms_enabled { hist_count } else { 0 };
                        stats.failed_samples += total_bad;
                        stats.failed_exemplars += ts.exemplar_count();
                        record_partial(err, None);
                        continue;
                    }
                }

                if verify_ownership && !self.check_ring_ownership(user_id, &pairs) {
                    return Err(IngesterError::Internal {
                        message: format!(
                            "series {} is not owned by this ingester",
                            make_series_str(&pairs)
                        ),
                    }
                    .with_user(user_id, 0));
                }
            }

            let prev_succeeded = stats.succeeded_samples;
            let mut created_ts = ts.created_timestamp();
            let mut ingest_created_ts = created_ts > 0;
            let first_hist_ts = (hist_count > 0).then(|| ts.histogram_timestamp(0));

            for s in samples {
                if s.timestamp_ms > max_timestamp_ms {
                    stats.failed_samples += 1;
                    stats.sample_too_far_in_future += 1;
                    let s_str = make_series_str(&pairs);
                    record_partial(
                        IngesterError::sample_too_far_in_future(s.timestamp_ms, s_str),
                        Some(SampleKind::SampleTimestampTooFarInFuture),
                    );
                    continue;
                }
                if s.timestamp_ms < min_timestamp_ms {
                    stats.failed_samples += 1;
                    stats.sample_too_old += 1;
                    let s_str = make_series_str(&pairs);
                    record_partial(
                        IngesterError::sample_too_far_in_past(s.timestamp_ms, s_str),
                        None,
                    );
                    continue;
                }

                if ingest_created_ts
                    && created_ts < s.timestamp_ms
                    && (!native_histograms_enabled
                        || hist_count == 0
                        || first_hist_ts.is_none_or(|ht| ht >= s.timestamp_ms))
                {
                    match app.append_float_reject_ooo(series_ref, &pairs, created_ts, 0.0) {
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
                            handle_head_append_error(
                                err,
                                created_ts,
                                &pairs,
                                ooo_window_ms,
                                ooo_window_dur,
                                &limits,
                                &mut stats,
                                &mut make_series_str,
                                &mut record_partial,
                            )?;
                        }
                    }
                    ingest_created_ts = false;
                    created_ts = 0;
                }

                match app.append_float_with_st(series_ref, &pairs, 0, s.timestamp_ms, s.value) {
                    Ok(r) => {
                        series_ref = Some(r);
                        stats.succeeded_samples += 1;
                    }
                    Err(err) => {
                        handle_head_append_error(
                            err,
                            s.timestamp_ms,
                            &pairs,
                            ooo_window_ms,
                            ooo_window_dur,
                            &limits,
                            &mut stats,
                            &mut make_series_str,
                            &mut record_partial,
                        )?;
                    }
                }
            }

            let mut num_native_hist_buckets: i64 = -1;
            if native_histograms_enabled && hist_count > 0 {
                for h_idx in 0..hist_count {
                    let (h_ts, is_float, ih_opt, fh_opt, bucket_count) =
                        ts.convert_histogram(h_idx);

                    if h_ts > max_timestamp_ms {
                        stats.failed_samples += 1;
                        stats.sample_too_far_in_future += 1;
                        let s_str = make_series_str(&pairs);
                        record_partial(
                            IngesterError::sample_too_far_in_future(h_ts, s_str),
                            Some(SampleKind::SampleTimestampTooFarInFuture),
                        );
                        continue;
                    }
                    if h_ts < min_timestamp_ms {
                        stats.failed_samples += 1;
                        stats.sample_too_old += 1;
                        let s_str = make_series_str(&pairs);
                        record_partial(
                            IngesterError::sample_too_far_in_past(h_ts, s_str),
                            None,
                        );
                        continue;
                    }
                    if max_hist_buckets > 0 && bucket_count > max_hist_buckets {
                        stats.failed_samples += 1;
                        stats.invalid_native_histogram += 1;
                        let s_str = make_series_str(&pairs);
                        record_partial(
                            IngesterError::max_native_histogram_buckets(
                                bucket_count,
                                max_hist_buckets,
                                s_str,
                            ),
                            Some(SampleKind::NativeHistogramValidation),
                        );
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
                                series_ref, &pairs, created_ts, zero_fh,
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
                                series_ref, &pairs, created_ts, zero_ih,
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
                                handle_head_append_error(
                                    err,
                                    created_ts,
                                    &pairs,
                                    ooo_window_ms,
                                    ooo_window_dur,
                                    &limits,
                                    &mut stats,
                                    &mut make_series_str,
                                    &mut record_partial,
                                )?;
                            }
                        }
                        ingest_created_ts = false;
                        created_ts = 0;
                    }

                    let res = if is_float {
                        app.append_float_histogram(
                            series_ref,
                            &pairs,
                            0,
                            h_ts,
                            fh_opt.expect("float histogram"),
                        )
                    } else {
                        app.append_histogram(
                            series_ref,
                            &pairs,
                            0,
                            h_ts,
                            ih_opt.expect("int histogram"),
                        )
                    };

                    match res {
                        Ok(r) => {
                            series_ref = Some(r);
                            stats.succeeded_samples += 1;
                            if samples
                                .last()
                                .is_none_or(|ls| ls.timestamp_ms < h_ts)
                            {
                                num_native_hist_buckets = bucket_count as i64;
                            }
                        }
                        Err(err) => {
                            handle_head_append_error(
                                err,
                                h_ts,
                                &pairs,
                                ooo_window_ms,
                                ooo_window_dur,
                                &limits,
                                &mut stats,
                                &mut make_series_str,
                                &mut record_partial,
                            )?;
                        }
                    }
                }
            }

            if self.cfg.active_series.active_series_metrics_enabled
                && stats.succeeded_samples > prev_succeeded
            {
                if let Some(ref_id) = series_ref {
                    let h = head
                        .series_labels(ref_id)
                        .map_or(0, |l| l.hash());
                    db.active_series.update_series(
                        &pairs,
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
                    if let Ok((ex_ts, _, _, ex_str)) = ts.exemplar_at(0) {
                        let s_str = make_series_str(&pairs);
                        record_partial(
                            IngesterError::exemplar_missing_series(ex_ts, s_str, ex_str),
                            None,
                        );
                    }
                    stats.failed_exemplars += ex_count;
                } else {
                    let mut out_of_order_exemplars = 0usize;
                    for e_idx in 0..ex_count {
                        let (ex_ts, ex_val, ex_labels, ex_str) = match ts.exemplar_at(e_idx) {
                            Ok(v) => v,
                            Err(e) => {
                                stats.failed_exemplars += 1;
                                record_partial(e, None);
                                continue;
                            }
                        };
                        if ex_ts > max_timestamp_ms {
                            stats.failed_exemplars += 1;
                            let s_str = make_series_str(&pairs);
                            record_partial(
                                IngesterError::exemplar_too_far_in_future(ex_ts, s_str, ex_str),
                                None,
                            );
                            continue;
                        }
                        if ex_ts < min_timestamp_ms {
                            stats.failed_exemplars += 1;
                            let s_str = make_series_str(&pairs);
                            record_partial(
                                IngesterError::exemplar_too_far_in_past(ex_ts, s_str, ex_str),
                                None,
                            );
                            continue;
                        }
                        let exemplar = Exemplar::new(ex_labels, ex_val, ex_ts);
                        match app.append_exemplar(series_ref, &pairs, exemplar) {
                            Ok(_) => stats.succeeded_exemplars += 1,
                            Err(HeadError::Exemplar(ExemplarError::OutOfOrderExemplar)) => {
                                stats.failed_exemplars += 1;
                                out_of_order_exemplars += 1;
                                if out_of_order_exemplars == ex_count && !ignore_ooo_exemplars {
                                    let s_str = make_series_str(&pairs);
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
                                let s_str = make_series_str(&pairs);
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
                                let s_str = make_series_str(&pairs);
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

        self.metrics
            .ingested_samples
            .with_label_values(&[user_id])
            .inc_by(stats.succeeded_samples as f64);
        self.metrics
            .ingested_samples_fail
            .with_label_values(&[user_id])
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

        self.record_discarded_metrics(user_id, "", &stats, source, db);

        if let Some((partial, sampler_kind)) = first_partial {
            let err = if let Some(kind) = sampler_kind {
                let should_log = kind.sampler(&self.error_samplers).should_log();
                partial.with_sampling(should_log)
            } else {
                partial
            };
            return Err(err.with_user(user_id, stats.failed_samples as i64));
        }

        Ok(())
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
        let Some(ref ring) = self.instance_ring else {
            return true;
        };
        let token = shard_by_all_labels(user_id, pairs.iter().copied());
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

#[allow(clippy::too_many_arguments)]
fn handle_head_append_error<'a>(
    err: HeadError,
    ts: i64,
    pairs: &[(&'a str, &'a str)],
    ooo_window_ms: i64,
    ooo_window_dur: Duration,
    limits: &LimitsConfig,
    stats: &mut PushStats,
    make_series_str: SeriesFormatter<'_, 'a>,
    record_partial: &mut dyn FnMut(IngesterError, Option<SampleKind>),
) -> Result<(), IngesterError> {
    stats.failed_samples += 1;
    match err {
        HeadError::OutOfBounds => {
            stats.sample_timestamp_too_old += 1;
            let s_str = make_series_str(pairs);
            record_partial(
                IngesterError::sample_timestamp_too_old(ts, s_str),
                Some(SampleKind::SampleTimestampTooOld),
            );
            Ok(())
        }
        HeadError::OutOfOrderSample => {
            stats.sample_out_of_order += 1;
            let s_str = make_series_str(pairs);
            record_partial(
                IngesterError::sample_out_of_order(ts, s_str),
                Some(SampleKind::SampleOutOfOrder),
            );
            Ok(())
        }
        HeadError::TooOldSample => {
            stats.sample_too_old += 1;
            let s_str = make_series_str(pairs);
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
            let s_str = make_series_str(pairs);
            record_partial(
                IngesterError::sample_duplicate_timestamp("duplicate sample for timestamp", ts, s_str),
                Some(SampleKind::SampleDuplicateTimestamp),
            );
            Ok(())
        }
        HeadError::DuplicateSampleWithDetails(msg) => {
            stats.new_value_for_timestamp += 1;
            let s_str = make_series_str(pairs);
            record_partial(
                IngesterError::sample_duplicate_timestamp(msg, ts, s_str),
                Some(SampleKind::SampleDuplicateTimestamp),
            );
            Ok(())
        }
        HeadError::HistogramValidation(msg) => {
            stats.invalid_native_histogram += 1;
            let err_id = map_native_histogram_err_id(&msg);
            let s_str = make_series_str(pairs);
            record_partial(
                IngesterError::NativeHistogramValidation {
                    err_id,
                    original_err: msg,
                    series: s_str,
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
            let s_str = make_series_str(pairs);
            record_partial(
                IngesterError::per_metric_series_limit_reached(
                    limits.max_global_series_per_metric.max(0) as usize,
                    s_str,
                ),
                Some(SampleKind::MaxSeriesPerMetric),
            );
            Ok(())
        }
        HeadError::MissingMetricName => {
            let s_str = make_series_str(pairs);
            record_partial(IngesterError::invalid_label_name("__name__", s_str), None);
            Ok(())
        }
        HeadError::MaxInMemorySeriesReached => Err(IngesterError::max_in_memory_series_reached()),
        other => Err(IngesterError::Internal {
            message: other.to_string(),
        }),
    }
}

fn validate_label_pairs<'a>(
    pairs: &[(&'a str, &'a str)],
    _limits: &LimitsConfig,
    make_series_str: SeriesFormatter<'_, 'a>,
) -> Option<IngesterError> {
    let mut has_metric_name = false;

    for &(name, value) in pairs {
        if name == "__name__" && !value.is_empty() {
            has_metric_name = true;
        }
        if name.is_empty() {
            return Some(IngesterError::invalid_label_name(
                name,
                make_series_str(pairs),
            ));
        }
    }

    if !has_metric_name {
        return Some(IngesterError::invalid_label_name(
            "__name__",
            make_series_str(pairs),
        ));
    }
    None
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
    }
    min_ts
}

pub fn empty_hashset() -> HashSet<String> {
    HashSet::new()
}
