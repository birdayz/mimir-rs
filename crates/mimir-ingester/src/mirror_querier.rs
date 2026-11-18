//! Mirrored chunk querier for index lookup planning comparison.
//!
//! Ported from github.com/grafana/mimir/pkg/ingester/mirror_querier.go

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use mimir_tsdb::chunks_block::ChunkMeta;
use mimir_tsdb::labels::Labels;
use mimir_tsdb::matcher::LabelMatcher;
use parking_lot::Mutex;
use prometheus::CounterVec;
use tracing::{debug, error, warn};
use ulid::Ulid;

use crate::lookupplan::{self, SelectHints};

#[derive(Clone, Debug, Default)]
pub struct ChunkBlockMeta {
    pub ulid: Ulid,
    pub min_time: i64,
    pub max_time: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QuerierError {
    Canceled,
    Other(String),
}

impl QuerierError {
    pub fn is_canceled(&self) -> bool {
        matches!(self, Self::Canceled)
    }
}

impl std::fmt::Display for QuerierError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Canceled => f.write_str("context canceled"),
            Self::Other(msg) => f.write_str(msg),
        }
    }
}

impl std::error::Error for QuerierError {}

pub trait ChunkSeries: Send + Sync {
    fn labels(&self) -> Labels;
    fn chunks(&self) -> Vec<ChunkMeta>;
}

pub trait ChunkSeriesSet: Send {
    fn next(&mut self) -> bool;
    fn at(&mut self) -> Option<Arc<dyn ChunkSeries>>;
    fn err(&self) -> Option<QuerierError>;
    fn warnings(&self) -> Vec<String> {
        Vec::new()
    }
}

pub trait ChunkQuerier: Send {
    fn select(
        &mut self,
        sort_series: bool,
        hints: Option<&SelectHints>,
        matchers: &[LabelMatcher],
    ) -> Box<dyn ChunkSeriesSet>;

    fn label_values(
        &mut self,
        name: &str,
        matchers: &[LabelMatcher],
    ) -> Result<(Vec<String>, Vec<String>), QuerierError>;

    fn label_names(
        &mut self,
        matchers: &[LabelMatcher],
    ) -> Result<(Vec<String>, Vec<String>), QuerierError>;

    fn close(&mut self) -> Result<(), QuerierError>;
}

pub struct RetainingChunkSeriesSet {
    pub labels: Vec<Labels>,
    delegate: Box<dyn ChunkSeriesSet>,
    labels_appended: bool,
    current_series: Option<Arc<dyn ChunkSeries>>,
}

impl RetainingChunkSeriesSet {
    pub fn new(delegate: Box<dyn ChunkSeriesSet>) -> Self {
        Self {
            labels: Vec::new(),
            delegate,
            labels_appended: false,
            current_series: None,
        }
    }

    pub fn from_retained_labels(labels: Vec<Labels>, delegate: Box<dyn ChunkSeriesSet>) -> Self {
        Self {
            labels,
            delegate,
            labels_appended: false,
            current_series: None,
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> bool {
        if !self.delegate.next() {
            self.current_series = None;
            return false;
        }
        self.labels_appended = false;
        self.current_series = None;
        true
    }

    pub fn at(&mut self) -> Option<Arc<dyn ChunkSeries>> {
        if self.labels_appended {
            return self.current_series.clone();
        }
        let series = self.delegate.at()?;
        self.labels.push(series.labels());
        self.labels_appended = true;
        self.current_series = Some(Arc::clone(&series));
        Some(series)
    }

    pub fn err(&self) -> Option<QuerierError> {
        self.delegate.err()
    }

    pub fn warnings(&self) -> Vec<String> {
        self.delegate.warnings()
    }
}

impl ChunkSeriesSet for RetainingChunkSeriesSet {
    fn next(&mut self) -> bool {
        RetainingChunkSeriesSet::next(self)
    }

    fn at(&mut self) -> Option<Arc<dyn ChunkSeries>> {
        RetainingChunkSeriesSet::at(self)
    }

    fn err(&self) -> Option<QuerierError> {
        RetainingChunkSeriesSet::err(self)
    }

    fn warnings(&self) -> Vec<String> {
        RetainingChunkSeriesSet::warnings(self)
    }
}

#[derive(Clone, Default)]
pub struct RecordedSelectRequest {
    pub min_t: i64,
    pub max_t: i64,
    pub finished_at_ms: i64,
    pub matchers: Vec<LabelMatcher>,
    pub hints: Option<SelectHints>,
    pub sort_series: bool,
    pub tenant_id: String,
    pub trace_id: String,
    pub trace_sampled: bool,
}

pub struct MirroredChunkQuerier {
    user_id: String,
    comparison_outcomes: CounterVec,
    delegate: Box<dyn ChunkQuerier>,
    block_meta: ChunkBlockMeta,
    pub recorded_request: RecordedSelectRequest,
    pub returned_series: Option<RetainingChunkSeriesSet>,
    /// Shared hook for callers inspecting returned series after `select` returns `Box<dyn ChunkSeriesSet>`,
    /// or callers can call `select_retaining` / `select` and pass/close on the querier directly.
    retained_labels_sink: Option<Arc<Mutex<Vec<Labels>>>>,
    returned_err_sink: Option<Arc<Mutex<Option<QuerierError>>>>,
}

impl MirroredChunkQuerier {
    pub fn new_with_meta(
        user_id: impl Into<String>,
        comparison_outcomes: CounterVec,
        min_t: i64,
        max_t: i64,
        block_meta: ChunkBlockMeta,
        delegate: Box<dyn ChunkQuerier>,
    ) -> Self {
        let uid = user_id.into();
        let (trace_id, trace_sampled) = mimir_common::tracing::current_sampled_trace_id();
        let recorded_request = RecordedSelectRequest {
            min_t,
            max_t,
            tenant_id: uid.clone(),
            trace_id,
            trace_sampled,
            ..Default::default()
        };
        Self {
            user_id: uid,
            comparison_outcomes,
            delegate,
            block_meta,
            recorded_request,
            returned_series: None,
            retained_labels_sink: None,
            returned_err_sink: None,
        }
    }

    pub fn label_values(
        &mut self,
        name: &str,
        matchers: &[LabelMatcher],
    ) -> Result<(Vec<String>, Vec<String>), QuerierError> {
        warn!("LabelValues is not implemented in mirroredChunkQuerier, not comparing results");
        self.delegate.label_values(name, matchers)
    }

    pub fn label_names(
        &mut self,
        matchers: &[LabelMatcher],
    ) -> Result<(Vec<String>, Vec<String>), QuerierError> {
        warn!("LabelNames is not implemented in mirroredChunkQuerier, not comparing results");
        self.delegate.label_names(matchers)
    }

    pub fn select(
        &mut self,
        sort_series: bool,
        hints: Option<&SelectHints>,
        matchers: &[LabelMatcher],
    ) -> SharedRetainingChunkSeriesSet {
        self.recorded_request.sort_series = sort_series;
        self.recorded_request.hints = hints.copied();
        self.recorded_request.matchers = matchers.to_vec();
        if self.recorded_request.trace_id.is_empty() {
            let (tid, sampled) = mimir_common::tracing::current_sampled_trace_id();
            self.recorded_request.trace_id = tid;
            self.recorded_request.trace_sampled = sampled;
        }

        let ss = self.delegate.select(sort_series, hints, matchers);
        self.recorded_request.finished_at_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        let labels_sink = Arc::new(Mutex::new(Vec::new()));
        let err_sink = Arc::new(Mutex::new(None));
        self.retained_labels_sink = Some(Arc::clone(&labels_sink));
        self.returned_err_sink = Some(Arc::clone(&err_sink));

        SharedRetainingChunkSeriesSet {
            inner: RetainingChunkSeriesSet::new(ss),
            labels_sink,
            err_sink,
        }
    }

    pub fn close(&mut self) -> Result<(), QuerierError> {
        let Some(ref labels_sink) = self.retained_labels_sink else {
            if self.returned_series.is_none() {
                debug!("Select wasn't invoked, skipping comparison");
                self.inc_outcome("no_select");
                return self.delegate.close();
            }
            return self.close_with_returned_series();
        };

        // Check error recorded during iteration or directly from delegate if available.
        if let Some(ref err_sink) = self.returned_err_sink {
            if let Some(returned_err) = err_sink.lock().clone() {
                if returned_err.is_canceled() {
                    debug!("Select was canceled, skipping comparison");
                    self.inc_outcome("context_cancelled");
                    return self.delegate.close();
                } else {
                    error!(err = %returned_err, "error reading returned series");
                    self.inc_outcome("returned_series_error");
                    return self.delegate.close();
                }
            }
        }

        // Also check a fresh probe in case delegate errored right before close
        // (matching Go where r.Err() calls delegate.Err() dynamically on Close).
        let primary_labels = labels_sink.lock().clone();
        let sort_series = self.recorded_request.sort_series;
        let hints = self.recorded_request.hints;
        let matchers = self.recorded_request.matchers.clone();

        let mut ss_without_planning = lookupplan::with_disabled_planning_context(true, || {
            self.delegate.select(sort_series, hints.as_ref(), &matchers)
        });
        if let Some(err) = ss_without_planning.err() {
            if err.is_canceled() {
                debug!("Select was canceled, skipping comparison");
                self.inc_outcome("context_cancelled");
                return self.delegate.close();
            }
            error!(err = %err, "error reading secondary series");
            self.inc_outcome("secondary_error");
            return self.delegate.close();
        }
        self.compare_results_with_primary(&primary_labels, ss_without_planning.as_mut());
        self.delegate.close()
    }

    fn close_with_returned_series(&mut self) -> Result<(), QuerierError> {
        let returned_err = self.returned_series.as_ref().and_then(|r| r.err());
        if let Some(err) = returned_err {
            if err.is_canceled() {
                debug!("Select was canceled, skipping comparison");
                self.inc_outcome("context_cancelled");
                return self.delegate.close();
            }
            error!(err = %err, "error reading returned series");
            self.inc_outcome("returned_series_error");
            return self.delegate.close();
        }
        let primary_labels = self
            .returned_series
            .as_ref()
            .map(|r| r.labels.clone())
            .unwrap_or_default();
        let sort_series = self.recorded_request.sort_series;
        let hints = self.recorded_request.hints;
        let matchers = self.recorded_request.matchers.clone();
        let mut ss_without_planning = lookupplan::with_disabled_planning_context(true, || {
            self.delegate.select(sort_series, hints.as_ref(), &matchers)
        });
        self.compare_results_with_primary(&primary_labels, ss_without_planning.as_mut());
        self.delegate.close()
    }

    pub fn compare_results(&self, secondary: &mut dyn ChunkSeriesSet) {
        let primary_labels = self
            .returned_series
            .as_ref()
            .map(|r| r.labels.as_slice())
            .unwrap_or(&[]);
        self.compare_results_with_primary(primary_labels, secondary);
    }

    fn compare_results_with_primary(
        &self,
        primary_labels: &[Labels],
        secondary: &mut dyn ChunkSeriesSet,
    ) {
        let ignore_younger_than = self.recorded_request.finished_at_ms;
        let mut secondary_series_count = 0usize;

        let advance_secondary =
            |sec: &mut dyn ChunkSeriesSet, count: &mut usize| -> Option<Labels> {
                while sec.next() {
                    let Some(series) = sec.at() else {
                        continue;
                    };
                    let lbls = series.labels();
                    let chunks = series.chunks();
                    if let Some(first_chunk) = chunks.first() {
                        if first_chunk.min_time > ignore_younger_than {
                            continue;
                        }
                    }
                    *count += 1;
                    return Some(lbls);
                }
                None
            };

        let mut secondary_current = advance_secondary(secondary, &mut secondary_series_count);
        let mut i = 0usize;
        let mut extra_series = Vec::new();
        let mut missing_series = Vec::new();

        while i < primary_labels.len() {
            let Some(ref sec_lbls) = secondary_current else {
                break;
            };
            match primary_labels[i].cmp(sec_lbls) {
                std::cmp::Ordering::Equal => {
                    i += 1;
                    secondary_current =
                        advance_secondary(secondary, &mut secondary_series_count);
                }
                std::cmp::Ordering::Less => {
                    extra_series.push(primary_labels[i].to_string());
                    i += 1;
                }
                std::cmp::Ordering::Greater => {
                    missing_series.push(sec_lbls.to_string());
                    secondary_current =
                        advance_secondary(secondary, &mut secondary_series_count);
                }
            }
        }

        while i < primary_labels.len() {
            extra_series.push(primary_labels[i].to_string());
            i += 1;
        }

        while let Some(sec_lbls) = secondary_current {
            missing_series.push(sec_lbls.to_string());
            secondary_current = advance_secondary(secondary, &mut secondary_series_count);
        }

        if let Some(err) = secondary.err() {
            if err.is_canceled() {
                self.inc_outcome("context_cancelled");
                return;
            }
            error!(err = %err, "error reading secondary series");
            self.inc_outcome("secondary_error");
            return;
        }

        self.record_comparison_outcome(
            &extra_series,
            &missing_series,
            primary_labels,
            secondary_series_count,
        );
    }

    fn record_comparison_outcome(
        &self,
        extra_series: &[String],
        missing_series: &[String],
        primary_labels: &[Labels],
        secondary_labels: usize,
    ) {
        let outcome = match (!extra_series.is_empty(), !missing_series.is_empty()) {
            (true, true) => "extra_and_missing_series",
            (true, false) => "extra_series",
            (false, true) => "missing_series",
            (false, false) => "success",
        };
        self.inc_outcome(outcome);

        if !extra_series.is_empty() || !missing_series.is_empty() {
            let request_matchers = self
                .recorded_request
                .matchers
                .iter()
                .map(|m| m.to_string())
                .collect::<Vec<_>>()
                .join(",");
            warn!(
                user = %self.user_id,
                trace_id = %self.recorded_request.trace_id,
                trace_sampled = self.recorded_request.trace_sampled,
                request_min_time = self.recorded_request.min_t,
                request_max_time = self.recorded_request.max_t,
                request_sort_series = self.recorded_request.sort_series,
                request_matchers = %request_matchers,
                block_ulid = %self.block_meta.ulid,
                block_min_time = self.block_meta.min_time,
                block_max_time = self.block_meta.max_time,
                extra_series_count = extra_series.len(),
                missing_series_count = missing_series.len(),
                primary_series_count = primary_labels.len(),
                secondary_series_count = secondary_labels,
                extra_series = %extra_series.join(";"),
                missing_series = %missing_series.join(";"),
                "series comparison found differences"
            );
        }
    }

    fn inc_outcome(&self, outcome: &str) {
        if let Ok(m) = self
            .comparison_outcomes
            .get_metric_with_label_values(&[outcome, &self.user_id])
        {
            m.inc();
        } else if let Ok(m) = self.comparison_outcomes.get_metric_with_label_values(&[outcome]) {
            m.inc();
        }
    }
}

pub struct SharedRetainingChunkSeriesSet {
    inner: RetainingChunkSeriesSet,
    labels_sink: Arc<Mutex<Vec<Labels>>>,
    err_sink: Arc<Mutex<Option<QuerierError>>>,
}

impl SharedRetainingChunkSeriesSet {
    pub fn labels(&self) -> &[Labels] {
        &self.inner.labels
    }
}

impl ChunkSeriesSet for SharedRetainingChunkSeriesSet {
    fn next(&mut self) -> bool {
        let has_next = self.inner.next();
        if !has_next {
            *self.err_sink.lock() = self.inner.err();
        }
        has_next
    }

    fn at(&mut self) -> Option<Arc<dyn ChunkSeries>> {
        let s = self.inner.at();
        *self.labels_sink.lock() = self.inner.labels.clone();
        s
    }

    fn err(&self) -> Option<QuerierError> {
        let e = self.inner.err();
        *self.err_sink.lock() = e.clone();
        e
    }

    fn warnings(&self) -> Vec<String> {
        self.inner.warnings()
    }
}
