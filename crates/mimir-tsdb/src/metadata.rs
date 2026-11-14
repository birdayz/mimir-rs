//! Per-tenant metric metadata store with per-user/per-metric limits and TTL purging.
//!
//! Ported from github.com/grafana/mimir/pkg/ingester/user_metrics_metadata.go.

use std::collections::HashMap;
use std::time::SystemTime;

use parking_lot::RwLock;
use thiserror::Error;

use crate::record::MetricType;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum MetadataError {
    #[error("per-user max metrics with metadata limit ({limit}) reached")]
    MaxMetricsPerUserReached { limit: usize },
    #[error("per-metric max metadata limit ({limit}) reached for metric {metric:?}")]
    MaxMetadataPerMetricReached { limit: usize, metric: String },
}

/// Configurable metadata limits (`0` means unlimited).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MetadataLimits {
    pub max_metrics_with_metadata_per_user: usize,
    pub max_metadata_per_metric: usize,
}

/// Metric metadata record (`cortexpb.MetricMetadata`).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MetricMetadata {
    pub metric_type: MetricType,
    pub metric_family_name: String,
    pub help: String,
    pub unit: String,
}

/// Thread-safe per-tenant metadata store (`userMetricsMetadata`).
#[derive(Debug, Default)]
pub struct UserMetricsMetadata {
    inner: RwLock<UserMetricsMetadataInner>,
}

#[derive(Debug, Default)]
struct UserMetricsMetadataInner {
    metrics: HashMap<String, HashMap<MetricMetadata, SystemTime>>,
    total_entries: usize,
}

impl UserMetricsMetadata {
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts or refreshes a metadata entry for `metric_name`, enforcing `limits` when non-zero.
    /// Returns `Ok(true)` if a new unique metadata entry was created, `Ok(false)` if refreshed.
    pub fn add(
        &self,
        metric_name: &str,
        metadata: MetricMetadata,
        limits: MetadataLimits,
    ) -> Result<bool, MetadataError> {
        let mut inner = self.inner.write();
        if !inner.metrics.contains_key(metric_name)
            && limits.max_metrics_with_metadata_per_user > 0
            && inner.metrics.len() >= limits.max_metrics_with_metadata_per_user
        {
            return Err(MetadataError::MaxMetricsPerUserReached {
                limit: limits.max_metrics_with_metadata_per_user,
            });
        }

        let existing_count = inner.metrics.get(metric_name).map_or(0, |s| s.len());
        let already_exists = inner
            .metrics
            .get(metric_name)
            .is_some_and(|s| s.contains_key(&metadata));

        if !already_exists
            && limits.max_metadata_per_metric > 0
            && existing_count >= limits.max_metadata_per_metric
        {
            return Err(MetadataError::MaxMetadataPerMetricReached {
                limit: limits.max_metadata_per_metric,
                metric: metric_name.to_string(),
            });
        }

        let set = inner
            .metrics
            .entry(metric_name.to_string())
            .or_default();
        let inserted = set.insert(metadata, SystemTime::now()).is_none();
        if inserted {
            inner.total_entries += 1;
        }
        Ok(inserted)
    }

    /// Purges all metadata entries whose timestamp is strictly older than `deadline`
    /// (or all metadata if `deadline` is `None`). Returns the number of entries removed.
    pub fn purge(&self, deadline: Option<SystemTime>) -> usize {
        let mut inner = self.inner.write();
        let mut deleted = 0usize;

        inner.metrics.retain(|_, set| {
            let before = set.len();
            match deadline {
                None => set.clear(),
                Some(dl) => set.retain(|_, ts| *ts >= dl),
            }
            deleted += before - set.len();
            !set.is_empty()
        });
        inner.total_entries = inner.total_entries.saturating_sub(deleted);
        deleted
    }

    /// Queries metadata matching `metric_names` (if non-empty) or `single_metric` (if non-empty),
    /// respecting `limit` (`< 0` = unlimited, `0` = return empty) and `limit_per_metric` (`<= 0` = unlimited).
    pub fn query(
        &self,
        metric_names: &[String],
        single_metric: &str,
        limit: i32,
        limit_per_metric: i32,
    ) -> Vec<MetricMetadata> {
        if limit == 0 {
            return Vec::new();
        }
        let inner = self.inner.read();
        let mut out = Vec::new();

        let push_set = |out: &mut Vec<MetricMetadata>, set: &HashMap<MetricMetadata, SystemTime>| {
            for (per_metric, m) in set.keys().enumerate() {
                if limit_per_metric > 0 && (per_metric as i32) >= limit_per_metric {
                    break;
                }
                out.push(m.clone());
            }
        };

        if !metric_names.is_empty() {
            let mut num_metrics = 0i32;
            for name in metric_names {
                if limit > 0 && num_metrics >= limit {
                    break;
                }
                if let Some(set) = inner.metrics.get(name) {
                    push_set(&mut out, set);
                    num_metrics += 1;
                }
            }
            return out;
        }

        if !single_metric.is_empty() {
            if let Some(set) = inner.metrics.get(single_metric) {
                push_set(&mut out, set);
            }
            return out;
        }

        for (num_metrics, set) in inner.metrics.values().enumerate() {
            if limit > 0 && (num_metrics as i32) >= limit {
                break;
            }
            push_set(&mut out, set);
        }
        out
    }

    pub fn num_entries(&self) -> usize {
        self.inner.read().total_entries
    }

    pub fn num_metrics(&self) -> usize {
        self.inner.read().metrics.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn metadata_limits_query_and_purge() {
        let store = UserMetricsMetadata::new();
        let limits = MetadataLimits {
            max_metrics_with_metadata_per_user: 2,
            max_metadata_per_metric: 1,
        };

        let m1 = MetricMetadata {
            metric_type: MetricType::Counter,
            metric_family_name: "req_total".into(),
            help: "Total requests".into(),
            unit: "".into(),
        };
        assert_eq!(store.add("req_total", m1.clone(), limits), Ok(true));
        // Duplicate refresh succeeds even at limit=1
        assert_eq!(store.add("req_total", m1.clone(), limits), Ok(false));

        let m1_alt = MetricMetadata {
            help: "Different help".into(),
            ..m1.clone()
        };
        assert!(matches!(
            store.add("req_total", m1_alt, limits),
            Err(MetadataError::MaxMetadataPerMetricReached { .. })
        ));

        let m2 = MetricMetadata {
            metric_type: MetricType::Gauge,
            metric_family_name: "up".into(),
            help: "Up".into(),
            unit: "".into(),
        };
        assert_eq!(store.add("up", m2, limits), Ok(true));

        let m3 = MetricMetadata {
            metric_type: MetricType::Gauge,
            metric_family_name: "other".into(),
            help: "Other".into(),
            unit: "".into(),
        };
        assert!(matches!(
            store.add("other", m3, limits),
            Err(MetadataError::MaxMetricsPerUserReached { .. })
        ));

        let res = store.query(&["up".into()], "", -1, -1);
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].metric_family_name, "up");

        // Purge with future deadline removes everything
        let future = SystemTime::now() + Duration::from_secs(60);
        assert_eq!(store.purge(Some(future)), 2);
        assert_eq!(store.num_entries(), 0);
    }
}
