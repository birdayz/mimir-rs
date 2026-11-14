//! Label churn detection matching `pkg/ingester/label_churn.go`.

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, UNIX_EPOCH};

use mimir_common::logging::format_rfc3339_nano_utc;
use mimir_tsdb::head::Head;
use mimir_tsdb::matcher::LabelMatcher;
use parking_lot::Mutex;
use serde::Serialize;

pub const LABEL_CHURN_RECORD_INTERVAL: Duration = Duration::from_secs(60);
pub const LABEL_CHURN_RECORDS: usize = 120;
const LABEL_CHURN_SAMPLED_SERIES_PER_VALUE: usize = 5;
const LABEL_CHURN_METRIC_NAMES_PER_LABEL: usize = 20;
pub const DEFAULT_LABEL_CHURN_LOOKBACK: Duration = Duration::from_secs(30 * 60);
pub const DEFAULT_LABEL_CHURN_LIMIT: usize = 30;

#[derive(Debug, Clone, Copy)]
pub struct SeriesRefRecord {
    pub timestamp_ms: i64,
    pub series_ref: u64,
}

#[derive(Debug)]
pub struct SeriesRefHistory {
    max_records: usize,
    records: Mutex<VecDeque<SeriesRefRecord>>,
}

impl SeriesRefHistory {
    pub fn new(max_records: usize) -> Self {
        Self {
            max_records: max_records.max(1),
            records: Mutex::new(VecDeque::with_capacity(max_records)),
        }
    }

    pub fn record(&self, timestamp_ms: i64, series_ref: u64) {
        let mut guard = self.records.lock();
        if guard.len() >= self.max_records {
            guard.pop_front();
        }
        guard.push_back(SeriesRefRecord {
            timestamp_ms,
            series_ref,
        });
    }

    pub fn oldest_record_not_before(&self, timestamp_ms: i64) -> Option<SeriesRefRecord> {
        let guard = self.records.lock();
        let mut best: Option<SeriesRefRecord> = None;
        for &r in guard.iter() {
            if r.timestamp_ms >= timestamp_ms
                && best.is_none_or(|b| r.timestamp_ms < b.timestamp_ms)
            {
                best = Some(r);
            }
        }
        best
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct LabelChurnResponse {
    pub since: String,
    pub scores: Vec<LabelChurnScore>,
}

#[derive(Debug, Clone, Serialize)]
pub struct LabelChurnScore {
    pub label_name: String,
    pub score: u64,
    pub metric_names: Vec<LabelChurnMetricName>,
}

#[derive(Debug, Clone, Serialize)]
pub struct LabelChurnMetricName {
    pub metric_name: String,
    pub sampled_series: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum LabelChurnError {
    #[error("a compaction removed series after the start of the lookback window, retry later or use a shorter lookback")]
    CompactedTooRecently,
    #[error("the ingester has no record in the lookback window, the ingester records once per minute after it starts")]
    NoRecord,
    #[error("TSDB head error: {0}")]
    Head(#[from] mimir_tsdb::head::HeadError),
}

pub fn compute_label_churn(
    head: &Head,
    history: &SeriesRefHistory,
    pending_series_removals: i64,
    last_series_removal_ms: i64,
    since_ms: i64,
    limit: usize,
    get_pending_and_last: impl Fn() -> (i64, i64),
) -> Result<LabelChurnResponse, LabelChurnError> {
    if pending_series_removals > 0 {
        return Err(LabelChurnError::CompactedTooRecently);
    }

    let watermark = history
        .oldest_record_not_before(since_ms.max(last_series_removal_ms))
        .ok_or(if last_series_removal_ms > since_ms {
            LabelChurnError::CompactedTooRecently
        } else {
            LabelChurnError::NoRecord
        })?;

    let empty_matchers: &[LabelMatcher] = &[];
    let label_names = head.label_names(empty_matchers)?;
    let mut scores = Vec::new();
    let mut sampled_refs = Vec::with_capacity(LABEL_CHURN_SAMPLED_SERIES_PER_VALUE);

    for label_name in label_names {
        let label_values = head.label_values(&label_name, empty_matchers)?;
        let mut score = 0u64;
        let mut sampled_per_metric: HashMap<String, u64> = HashMap::new();

        for label_value in label_values {
            let Some(first_ref) = head.first_posting(&label_name, &label_value) else {
                continue;
            };
            if first_ref <= watermark.series_ref {
                continue;
            }
            score += head.postings_length(&label_name, &label_value) as u64;

            sampled_refs.clear();
            head.append_random_postings_refs(
                &label_name,
                &label_value,
                LABEL_CHURN_SAMPLED_SERIES_PER_VALUE,
                &mut sampled_refs,
            );
            for &series_ref in &sampled_refs {
                if let Some(lbls) = head.series_labels(series_ref) {
                    let metric_name = lbls.get("__name__").to_string();
                    *sampled_per_metric.entry(metric_name).or_insert(0) += 1;
                }
            }
        }

        if score > 0 {
            let mut metric_names: Vec<LabelChurnMetricName> = sampled_per_metric
                .into_iter()
                .map(|(metric_name, sampled_series)| LabelChurnMetricName {
                    metric_name,
                    sampled_series,
                })
                .collect();
            metric_names.sort_by(|a, b| {
                b.sampled_series
                    .cmp(&a.sampled_series)
                    .then_with(|| a.metric_name.cmp(&b.metric_name))
            });
            metric_names.truncate(LABEL_CHURN_METRIC_NAMES_PER_LABEL);

            scores.push(LabelChurnScore {
                label_name,
                score,
                metric_names,
            });
        }
    }

    let (pending_after, last_after) = get_pending_and_last();
    if pending_after > 0 || last_after != last_series_removal_ms {
        return Err(LabelChurnError::CompactedTooRecently);
    }

    scores.sort_by(|a, b| {
        b.score
            .cmp(&a.score)
            .then_with(|| a.label_name.cmp(&b.label_name))
    });
    if limit > 0 && scores.len() > limit {
        scores.truncate(limit);
    }

    let since_ts = if watermark.timestamp_ms >= 0 {
        UNIX_EPOCH + Duration::from_millis(watermark.timestamp_ms as u64)
    } else {
        UNIX_EPOCH
    };
    let since = format_rfc3339_nano_utc(since_ts);

    Ok(LabelChurnResponse { since, scores })
}
