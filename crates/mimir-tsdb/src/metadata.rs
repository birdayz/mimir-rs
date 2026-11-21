//! Per-tenant metric metadata store with per-user/per-metric limits and TTL purging.
//!
//! Ported from github.com/grafana/mimir/pkg/ingester/user_metrics_metadata.go.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use hashbrown::{HashMap, HashTable};
use parking_lot::RwLock;
use thiserror::Error;
use xxhash_rust::xxh3::Xxh3;

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

#[inline]
fn system_time_to_nanos(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
}

#[inline]
fn now_nanos() -> u64 {
    system_time_to_nanos(SystemTime::now())
}

#[inline]
fn metadata_hash(metric_type: MetricType, family: &str, help: &str, unit: &str) -> u64 {
    let mut h = Xxh3::new();
    h.update(&[metric_type as u8]);
    h.update(&(family.len() as u32).to_le_bytes());
    h.update(family.as_bytes());
    h.update(&(help.len() as u32).to_le_bytes());
    h.update(help.as_bytes());
    h.update(&(unit.len() as u32).to_le_bytes());
    h.update(unit.as_bytes());
    h.digest()
}

#[derive(Debug)]
struct StoredMetadata {
    meta: MetricMetadata,
    last_seen_nanos: AtomicU64,
}

impl StoredMetadata {
    #[inline]
    fn matches(&self, metric_type: MetricType, family: &str, help: &str, unit: &str) -> bool {
        self.meta.metric_type == metric_type
            && self.meta.metric_family_name == family
            && self.meta.help == help
            && self.meta.unit == unit
    }

    #[inline]
    fn hash(&self) -> u64 {
        metadata_hash(
            self.meta.metric_type,
            &self.meta.metric_family_name,
            &self.meta.help,
            &self.meta.unit,
        )
    }
}

/// Thread-safe per-tenant metadata store (`userMetricsMetadata`).
#[derive(Debug, Default)]
pub struct UserMetricsMetadata {
    inner: RwLock<UserMetricsMetadataInner>,
}

#[derive(Debug, Default)]
struct UserMetricsMetadataInner {
    metrics: HashMap<String, HashTable<StoredMetadata>>,
    total_entries: usize,
}

impl UserMetricsMetadata {
    pub fn new() -> Self {
        Self::default()
    }

    /// Zero-allocation fast-path insert/refresh using borrowed `&str` fields.
    ///
    /// Under a shared `RwLock::read()`, checks if `(metric_name, metric_type, help, unit)`
    /// already exists; if so, atomically updates its last-seen timestamp and returns `Ok(false)`
    /// with zero `String` allocations and zero write locks. Only acquires `RwLock::write()` when
    /// inserting a genuinely new entry.
    pub fn add_str(
        &self,
        metric_name: &str,
        metric_type: MetricType,
        help: &str,
        unit: &str,
        limits: MetadataLimits,
    ) -> Result<bool, MetadataError> {
        let now = now_nanos();
        let hash = metadata_hash(metric_type, metric_name, help, unit);

        {
            let inner = self.inner.read();
            if let Some(table) = inner.metrics.get(metric_name) {
                if (limits.max_metadata_per_metric == 0
                    || table.len() < limits.max_metadata_per_metric)
                    && table
                        .find(hash, |s| s.matches(metric_type, metric_name, help, unit))
                        .map(|entry| entry.last_seen_nanos.store(now, Ordering::Relaxed))
                        .is_some()
                {
                    return Ok(false);
                }
            }
        }

        let mut inner = self.inner.write();
        if !inner.metrics.contains_key(metric_name)
            && limits.max_metrics_with_metadata_per_user > 0
            && inner.metrics.len() >= limits.max_metrics_with_metadata_per_user
        {
            return Err(MetadataError::MaxMetricsPerUserReached {
                limit: limits.max_metrics_with_metadata_per_user,
            });
        }

        let existing_count = inner.metrics.get(metric_name).map_or(0, HashTable::len);
        if limits.max_metadata_per_metric > 0 && existing_count >= limits.max_metadata_per_metric {
            return Err(MetadataError::MaxMetadataPerMetricReached {
                limit: limits.max_metadata_per_metric,
                metric: metric_name.to_string(),
            });
        }

        if let Some(table) = inner.metrics.get(metric_name) {
            if let Some(entry) =
                table.find(hash, |s| s.matches(metric_type, metric_name, help, unit))
            {
                entry.last_seen_nanos.store(now, Ordering::Relaxed);
                return Ok(false);
            }
        }

        let table = inner.metrics.entry(metric_name.to_string()).or_default();
        let stored = StoredMetadata {
            meta: MetricMetadata {
                metric_type,
                metric_family_name: metric_name.to_string(),
                help: help.to_string(),
                unit: unit.to_string(),
            },
            last_seen_nanos: AtomicU64::new(now),
        };
        table.insert_unique(hash, stored, StoredMetadata::hash);
        inner.total_entries += 1;
        Ok(true)
    }

    /// Alias for [`Self::add_str`] for callers using borrowed metadata slices.
    #[inline]
    pub fn add_borrowed(
        &self,
        metric_name: &str,
        metric_type: MetricType,
        help: &str,
        unit: &str,
        limits: MetadataLimits,
    ) -> Result<bool, MetadataError> {
        self.add_str(metric_name, metric_type, help, unit, limits)
    }

    /// Inserts or refreshes a metadata entry for `metric_name`, enforcing `limits` when non-zero.
    /// Returns `Ok(true)` if a new unique metadata entry was created, `Ok(false)` if refreshed.
    pub fn add(
        &self,
        metric_name: &str,
        metadata: MetricMetadata,
        limits: MetadataLimits,
    ) -> Result<bool, MetadataError> {
        let now = now_nanos();
        let hash = metadata_hash(
            metadata.metric_type,
            &metadata.metric_family_name,
            &metadata.help,
            &metadata.unit,
        );

        {
            let inner = self.inner.read();
            if let Some(table) = inner.metrics.get(metric_name) {
                if (limits.max_metadata_per_metric == 0
                    || table.len() < limits.max_metadata_per_metric)
                    && table
                        .find(hash, |s| s.meta == metadata)
                        .map(|entry| entry.last_seen_nanos.store(now, Ordering::Relaxed))
                        .is_some()
                {
                    return Ok(false);
                }
            }
        }

        let mut inner = self.inner.write();
        if !inner.metrics.contains_key(metric_name)
            && limits.max_metrics_with_metadata_per_user > 0
            && inner.metrics.len() >= limits.max_metrics_with_metadata_per_user
        {
            return Err(MetadataError::MaxMetricsPerUserReached {
                limit: limits.max_metrics_with_metadata_per_user,
            });
        }

        let existing_count = inner.metrics.get(metric_name).map_or(0, HashTable::len);
        if limits.max_metadata_per_metric > 0 && existing_count >= limits.max_metadata_per_metric {
            return Err(MetadataError::MaxMetadataPerMetricReached {
                limit: limits.max_metadata_per_metric,
                metric: metric_name.to_string(),
            });
        }

        if let Some(table) = inner.metrics.get(metric_name) {
            if let Some(entry) = table.find(hash, |s| s.meta == metadata) {
                entry.last_seen_nanos.store(now, Ordering::Relaxed);
                return Ok(false);
            }
        }

        let table = inner.metrics.entry(metric_name.to_string()).or_default();
        let stored = StoredMetadata {
            meta: metadata,
            last_seen_nanos: AtomicU64::new(now),
        };
        table.insert_unique(hash, stored, StoredMetadata::hash);
        inner.total_entries += 1;
        Ok(true)
    }

    /// Purges all metadata entries whose timestamp is strictly older than `deadline`
    /// (or all metadata if `deadline` is `None`). Returns the number of entries removed.
    pub fn purge(&self, deadline: Option<SystemTime>) -> usize {
        let mut inner = self.inner.write();
        let mut deleted = 0usize;

        match deadline {
            None => {
                deleted = inner.total_entries;
                inner.metrics.clear();
                inner.total_entries = 0;
            }
            Some(dl) => {
                let dl_nanos = system_time_to_nanos(dl);
                inner.metrics.retain(|_, table| {
                    let before = table.len();
                    table.retain(|s| *s.last_seen_nanos.get_mut() >= dl_nanos);
                    deleted += before - table.len();
                    !table.is_empty()
                });
                inner.total_entries = inner.total_entries.saturating_sub(deleted);
            }
        }
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

        let push_set = |out: &mut Vec<MetricMetadata>, table: &HashTable<StoredMetadata>| {
            for (per_metric, s) in table.iter().enumerate() {
                if limit_per_metric > 0 && (per_metric as i32) >= limit_per_metric {
                    break;
                }
                out.push(s.meta.clone());
            }
        };

        if !metric_names.is_empty() {
            let mut num_metrics = 0i32;
            for name in metric_names {
                if limit > 0 && num_metrics >= limit {
                    break;
                }
                if let Some(table) = inner.metrics.get(name) {
                    push_set(&mut out, table);
                    num_metrics += 1;
                }
            }
            return out;
        }

        if !single_metric.is_empty() {
            if let Some(table) = inner.metrics.get(single_metric) {
                push_set(&mut out, table);
            }
            return out;
        }

        for (num_metrics, table) in inner.metrics.values().enumerate() {
            if limit > 0 && (num_metrics as i32) >= limit {
                break;
            }
            push_set(&mut out, table);
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
        // Go userMetricsMetadata.add checks IsWithinMaxMetadataPerMetric(len(set)) before
        // checking membership, so refreshing at limit=1 is rejected.
        assert!(matches!(
            store.add("req_total", m1.clone(), limits),
            Err(MetadataError::MaxMetadataPerMetricReached { .. })
        ));
        // When limit allows (limit=2 > len=1), refreshing existing entry returns Ok(false).
        let limits_room = MetadataLimits {
            max_metrics_with_metadata_per_user: 2,
            max_metadata_per_metric: 2,
        };
        assert_eq!(store.add("req_total", m1.clone(), limits_room), Ok(false));

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

    #[test]
    fn metadata_add_str_read_lock_refresh_and_purge() {
        let store = UserMetricsMetadata::new();
        let limits = MetadataLimits {
            max_metrics_with_metadata_per_user: 2,
            max_metadata_per_metric: 2,
        };

        assert_eq!(
            store.add_str("http_requests_total", MetricType::Counter, "Total HTTP", "requests", limits),
            Ok(true)
        );
        // Fast-path refresh returns Ok(false) without inserting duplicate
        assert_eq!(
            store.add_borrowed("http_requests_total", MetricType::Counter, "Total HTTP", "requests", limits),
            Ok(false)
        );
        assert_eq!(store.num_entries(), 1);
        assert_eq!(store.num_metrics(), 1);

        // Past deadline keeps refreshed entry
        let past = SystemTime::now() - Duration::from_secs(60);
        assert_eq!(store.purge(Some(past)), 0);
        assert_eq!(store.num_entries(), 1);

        // Future deadline purges it
        let future = SystemTime::now() + Duration::from_secs(60);
        assert_eq!(store.purge(Some(future)), 1);
        assert_eq!(store.num_entries(), 0);
        assert_eq!(store.num_metrics(), 0);
    }

    #[test]
    fn metadata_max_per_metric_checked_on_refresh_for_add_and_add_str() {
        let store = UserMetricsMetadata::new();
        let cap1 = MetadataLimits {
            max_metrics_with_metadata_per_user: 10,
            max_metadata_per_metric: 1,
        };
        let cap2 = MetadataLimits {
            max_metrics_with_metadata_per_user: 10,
            max_metadata_per_metric: 2,
        };

        let meta = MetricMetadata {
            metric_type: MetricType::Counter,
            metric_family_name: "req_total".into(),
            help: "Total requests".into(),
            unit: "".into(),
        };
        assert_eq!(store.add("req_total", meta.clone(), cap1), Ok(true));
        // Refreshing via add() and add_str() when len == limit (1) must return MaxMetadataPerMetricReached.
        assert!(matches!(
            store.add("req_total", meta.clone(), cap1),
            Err(MetadataError::MaxMetadataPerMetricReached { limit: 1, .. })
        ));
        assert!(matches!(
            store.add_str("req_total", MetricType::Counter, "Total requests", "", cap1),
            Err(MetadataError::MaxMetadataPerMetricReached { limit: 1, .. })
        ));

        // With limit=2, refresh succeeds and a second distinct entry can be added.
        assert_eq!(
            store.add_str("req_total", MetricType::Counter, "Total requests", "", cap2),
            Ok(false)
        );
        assert_eq!(
            store.add_str("req_total", MetricType::Counter, "Alt help", "", cap2),
            Ok(true)
        );
        // Now len=2 == limit=2: refresh of either entry must fail even on fast-path.
        assert!(matches!(
            store.add_str("req_total", MetricType::Counter, "Total requests", "", cap2),
            Err(MetadataError::MaxMetadataPerMetricReached { limit: 2, .. })
        ));
        assert!(matches!(
            store.add("req_total", meta, cap2),
            Err(MetadataError::MaxMetadataPerMetricReached { limit: 2, .. })
        ));
    }
}
