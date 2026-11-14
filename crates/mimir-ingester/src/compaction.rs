//! Background compaction, shipping, idle TSDB closing, owned series, and metrics loops.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/ingester/ingester_compaction.go
//! - github.com/grafana/mimir/pkg/ingester/ingester_metrics_update.go
//! - github.com/grafana/mimir/pkg/ingester/ingester_tsdb.go

use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use mimir_common::services::{ServiceContext, State};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::active_series::parse_custom_tracker_matchers;
use crate::ingester::{Ingester, TriggerRequest, INSTANCE_INGESTION_RATE_TICK_INTERVAL};
use crate::label_churn::LABEL_CHURN_RECORD_INTERVAL;
use crate::owned_series::{
    DEFAULT_OWNED_SERIES_INITIAL_RETRY_INTERVAL, RECOMPUTE_OWNED_SERIES_REASON_COMPACTION,
    RECOMPUTE_OWNED_SERIES_REASON_EARLY_COMPACTION,
};
use crate::user_tsdb::{TsdbCloseCheckResult, TsdbState};

impl Ingester {
    /// Flushes all head blocks to disk, syncs offset catalogues, and ships blocks to bucket storage.
    pub async fn flush(&self) {
        info!("starting to flush and ship TSDB blocks");
        self.compact_blocks(true, i64::MAX, None).await;
        self.sync_offset_catalogues().await;
        if self.blocks_cfg.tsdb.ship_interval.0 > 0 {
            self.ship_blocks(None).await;
        }
        info!("finished flushing and shipping TSDB blocks");
    }

    pub async fn compact_blocks(
        &self,
        force: bool,
        forced_compaction_max_time: i64,
        allowed: Option<&HashSet<String>>,
    ) {
        let user_ids = self.get_tsdb_users();
        for user_id in user_ids {
            if let Some(allow) = allowed {
                if !allow.contains(&user_id) {
                    continue;
                }
            }
            let Some(user_db) = self.get_tsdb(&user_id) else {
                continue;
            };
            let head = user_db.db.head();
            if head.num_series() == 0 {
                continue;
            }

            self.metrics.compactions_triggered.inc();
            let min_time_before = head.min_time();
            let reason;

            let res: Result<(), String> = if force {
                reason = "forced";
                self.metrics.increase_forced_compactions();
                let r = user_db.compact_head(forced_compaction_max_time).await;
                self.metrics.decrease_forced_compactions();
                r
            } else if !self.compaction_idle_timeout.is_zero()
                && user_db.is_idle(SystemTime::now(), self.compaction_idle_timeout)
            {
                reason = "idle";
                let user_max_time = head.max_time().max(head.max_ooo_time());
                if user_max_time > i64::MIN {
                    self.metrics.increase_forced_compactions();
                    let r = user_db.compact_head(user_max_time).await;
                    self.metrics.decrease_forced_compactions();
                    r
                } else {
                    Ok(())
                }
            } else {
                reason = "regular";
                user_db.compact().map(|_| ()).map_err(|e| e.to_string())
            };

            user_db.update_last_series_removal();
            if let Err(e) = res {
                self.metrics.compactions_failed.inc();
                warn!(user = %user_id, reason, error = %e, "TSDB blocks compaction failed");
            } else {
                debug!(user = %user_id, reason, "TSDB blocks compaction completed");
            }

            let min_time_after = head.min_time();
            if min_time_before != min_time_after {
                let r = if force && forced_compaction_max_time != i64::MAX {
                    RECOMPUTE_OWNED_SERIES_REASON_EARLY_COMPACTION
                } else {
                    RECOMPUTE_OWNED_SERIES_REASON_COMPACTION
                };
                user_db.trigger_recompute_owned_series(r);
            }
        }
        self.metrics.reset_forced_compactions();
    }

    pub async fn ship_blocks(&self, allowed: Option<&HashSet<String>>) {
        let user_ids = self.get_tsdb_users();
        for user_id in user_ids {
            if let Some(allow) = allowed {
                if !allow.contains(&user_id) {
                    continue;
                }
            }
            let Some(user_db) = self.get_tsdb(&user_id) else {
                continue;
            };
            let Some(ref shipper) = user_db.shipper else {
                continue;
            };

            if user_db.check_deletion_mark(self.bucket.as_deref()).await {
                continue;
            }

            let (ok, st) = user_db.change_state(TsdbState::Active, TsdbState::ActiveShipping);
            if !ok {
                info!(user = %user_id, state = st.as_str(), "shipper skipped because TSDB is not active");
                continue;
            }

            let sync_res = shipper.sync().await;
            let _ = user_db.change_state(TsdbState::ActiveShipping, TsdbState::Active);

            match sync_res {
                Ok(uploaded) => {
                    if uploaded > 0 {
                        if let Err(e) = user_db.update_cached_shipped_blocks() {
                            warn!(user = %user_id, error = %e, "failed to update cached shipped blocks");
                        }
                    }
                }
                Err(e) => {
                    warn!(user = %user_id, error = %e, "shipper failed to sync TSDB blocks");
                }
            }
        }
    }

    pub async fn sync_offset_catalogues(&self) {
        if !self.blocks_cfg.tsdb.offset_catalogue.enabled {
            return;
        }
        let Some(reader) = self.ingest_reader.read().clone() else {
            return;
        };
        let hw = reader.last_seen_offsets();
        for user_id in self.get_tsdb_users() {
            if let Some(db) = self.get_tsdb(&user_id) {
                if let Some(ref cat) = db.offset_catalogue {
                    if let Err(e) = cat.sync(&hw) {
                        warn!(user = %user_id, error = %e, "offset catalogue sync failed");
                    }
                }
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SeriesReductionEstimation {
    pub user_id: String,
    pub estimated_count: i64,
    pub estimated_percentage: i64,
}

pub fn filter_users_to_compact_to_reduce_in_memory_series(
    num_memory_series: i64,
    early_compaction_min_series: i64,
    early_compaction_min_percentage: i64,
    mut estimations: Vec<SeriesReductionEstimation>,
) -> Vec<String> {
    let total_est: i64 = estimations.iter().map(|e| e.estimated_count).sum();
    if num_memory_series <= 0 || (total_est * 100) / num_memory_series < early_compaction_min_percentage {
        return Vec::new();
    }

    estimations.sort_by_key(|e| std::cmp::Reverse(e.estimated_count));
    let target_reduction = num_memory_series - early_compaction_min_series;
    let mut sum_reduction = 0i64;
    let mut users = Vec::new();
    for entry in estimations {
        if sum_reduction < target_reduction || entry.estimated_percentage >= early_compaction_min_percentage {
            users.push(entry.user_id);
            sum_reduction += entry.estimated_count;
        }
    }
    users
}

pub fn compaction_loop_extra_delay(
    iteration_duration: Duration,
    standard_interval: Duration,
    min_delay: Duration,
) -> Duration {
    if standard_interval.saturating_sub(iteration_duration) >= min_delay {
        Duration::ZERO
    } else {
        min_delay
    }
}

pub fn time_until_compaction(
    now: SystemTime,
    compaction_interval: Duration,
    zone_offset: Duration,
) -> Duration {
    let interval_ns = compaction_interval.as_nanos() as i128;
    if interval_ns <= 0 {
        return Duration::ZERO;
    }
    let now_ns = now
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i128;
    let hour_ns = 3600_i128 * 1_000_000_000_i128;
    let elapsed_ns = now_ns % hour_ns;
    let offset_ns = zone_offset.as_nanos() as i128;
    let mut since_last = (elapsed_ns - offset_ns) % interval_ns;
    if since_last < 0 {
        since_last += interval_ns;
    }
    Duration::from_nanos((interval_ns - since_last) as u64)
}

impl Ingester {
    pub fn time_to_next_zone_aware_compaction(
        &self,
        now: SystemTime,
        zones: &[String],
    ) -> Duration {
        let mut sorted_zones = zones.to_vec();
        sorted_zones.sort();

        let interval = Duration::from_nanos(
            self.blocks_cfg
                .tsdb
                .head_compaction_interval
                .as_nanos()
                .max(0) as u64,
        );
        if !self.cfg.ring.zone_awareness_enabled
            || self.cfg.ring.instance_availability_zone.is_empty()
            || sorted_zones.len() <= 1
        {
            return interval;
        }
        let current = &self.cfg.ring.instance_availability_zone;
        let Some(zone_idx) = sorted_zones.iter().position(|z| z == current) else {
            warn!(
                current_zone = %current,
                "current zone not found in zones list, using default interval"
            );
            return interval;
        };

        let offset_step = interval / (sorted_zones.len() as u32);
        let zone_offset = offset_step * (zone_idx as u32);
        time_until_compaction(now, interval, zone_offset)
    }

    pub fn compaction_service_interval(
        &self,
        now: SystemTime,
        zones: &[String],
    ) -> (Duration, Duration) {
        let starting_interval = self.head_compaction_interval_while_starting();
        let interval = Duration::from_nanos(
            self.blocks_cfg
                .tsdb
                .head_compaction_interval
                .as_nanos()
                .max(0) as u64,
        );

        if self.state() == State::Starting {
            let standard = starting_interval.min(interval);
            let first = if self.head_compaction_interval_jitter_enabled() && !standard.is_zero() {
                let ns = standard.as_nanos().max(1) as u64;
                Duration::from_nanos(rand::random::<u64>() % ns)
            } else {
                standard
            };
            return (first, standard);
        }

        let zone_aware = self.time_to_next_zone_aware_compaction(now, zones);
        if !self.head_compaction_interval_jitter_enabled() {
            return (zone_aware, interval);
        }
        let jitter = if !zones.is_empty() {
            let max_ns = ((interval.as_nanos() as u64) / (zones.len() as u64)) / 2;
            if max_ns > 0 {
                Duration::from_nanos(rand::random::<u64>() % max_ns)
            } else {
                Duration::ZERO
            }
        } else {
            Duration::ZERO
        };
        (zone_aware + jitter, interval)
    }

    pub async fn compact_blocks_to_reduce_in_memory_series(&self, now: SystemTime) {
        let mut min_series = self.blocks_cfg.tsdb.early_head_compaction_min_in_memory_series;
        if let Some(il) = self.get_instance_limits() {
            if il.early_head_compaction_min_in_memory_series > 0 {
                min_series = il.early_head_compaction_min_in_memory_series;
            }
        }
        if min_series <= 0 || !self.cfg.active_series.active_series_metrics_enabled {
            return;
        }
        let total_mem_series = self.series_count.load(Ordering::Relaxed);
        if total_mem_series < min_series {
            return;
        }

        let mut estimations = Vec::new();
        for user_id in self.get_tsdb_users() {
            let Some(db) = self.get_tsdb(&user_id) else {
                continue;
            };
            let user_series = db.db.head().num_series();
            if user_series == 0 {
                continue;
            }
            db.active_series.purge(now);
            let active = db.active_series.active().0;
            let reduction = (user_series as i64 - active as i64).max(0);
            let pct = ((reduction as u64 * 100) / user_series) as i64;
            estimations.push(SeriesReductionEstimation {
                user_id,
                estimated_count: reduction,
                estimated_percentage: pct,
            });
        }

        let min_pct = self
            .blocks_cfg
            .tsdb
            .early_head_compaction_min_estimated_series_reduction_percentage;
        let users_vec = filter_users_to_compact_to_reduce_in_memory_series(
            total_mem_series,
            min_series,
            min_pct,
            estimations,
        );
        if users_vec.is_empty() {
            return;
        }
        let users: HashSet<String> = users_vec.into_iter().collect();

        let idle_timeout = Duration::from_nanos(
            self.cfg
                .active_series
                .active_series_metrics_idle_timeout
                .as_nanos()
                .max(0) as u64,
        );
        let forced_max_ms = now
            .checked_sub(idle_timeout)
            .unwrap_or(UNIX_EPOCH)
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        self.compact_blocks(true, forced_max_ms, Some(&users)).await;
        for uid in &users {
            if let Some(db) = self.get_tsdb(uid) {
                db.set_last_early_compaction(now);
            }
        }
    }

    pub async fn compact_blocks_to_reduce_per_tenant_owned_series(&self, now: SystemTime) {
        if !self.cfg.active_series.active_series_metrics_enabled
            || !self.cfg.use_ingester_owned_series_for_limits
        {
            return;
        }

        let idle_timeout = Duration::from_nanos(
            self.cfg
                .active_series
                .active_series_metrics_idle_timeout
                .as_nanos()
                .max(0) as u64,
        );
        let forced_max_ms = now
            .checked_sub(idle_timeout)
            .unwrap_or(UNIX_EPOCH)
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        for user_id in self.get_tsdb_users() {
            let limits = self.overrides.for_tenant(&user_id);
            let threshold = limits.early_head_compaction_owned_series_threshold;
            if threshold <= 0 {
                continue;
            }
            let Some(db) = self.get_tsdb(&user_id) else {
                continue;
            };

            let last_ms = db.last_early_compaction_ms();
            if last_ms > 0 {
                let last = UNIX_EPOCH + Duration::from_millis(last_ms as u64);
                if now.duration_since(last).unwrap_or_default() < idle_timeout {
                    continue;
                }
            }

            let owned_count = db.owned_series_state().owned_series_count;
            let local_threshold = self
                .limiter
                .convert_global_to_local_limit(&user_id, threshold as usize);
            if local_threshold == 0 || owned_count < local_threshold {
                continue;
            }

            let user_series = db.db.head().num_series();
            if user_series == 0 {
                continue;
            }
            db.active_series.purge(now);
            let active = db.active_series.active().0;
            let est_reduction = (user_series as i64 - active as i64).max(0);
            let est_pct = ((est_reduction as u64 * 100) / user_series) as i64;
            if est_pct < limits.early_head_compaction_min_estimated_series_reduction_percentage {
                continue;
            }

            let mut allowed = HashSet::new();
            allowed.insert(user_id.clone());
            self.compact_blocks(true, forced_max_ms, Some(&allowed)).await;
            self.metrics.per_tenant_early_compactions_triggered.inc();
            db.set_last_early_compaction(now);
        }
    }

    pub async fn compact_blocks_due_to_non_owned_series(&self, jitter: Duration) {
        if !self.cfg.early_compaction_non_owned_series_enabled {
            return;
        }
        let min_grace = Duration::from_nanos(
            self.cfg
                .early_compaction_non_owned_series_min_grace_period
                .as_nanos()
                .max(0) as u64,
        );
        let max_grace = Duration::from_nanos(
            self.cfg
                .early_compaction_non_owned_series_max_grace_period
                .as_nanos()
                .max(0) as u64,
        );

        for user_id in self.get_tsdb_users() {
            let threshold = self
                .overrides
                .for_tenant(&user_id)
                .early_head_compaction_owned_series_threshold;
            if threshold <= 0 {
                continue;
            }
            let Some(db) = self.get_tsdb(&user_id) else {
                continue;
            };
            let now = SystemTime::now();
            let local_threshold = self
                .limiter
                .convert_global_to_local_limit(&user_id, threshold as usize);

            let refs = if local_threshold > 0 && db.db.head().num_series() >= local_threshold as u64
            {
                db.take_pending_non_owned_refs(now - min_grace - jitter)
            } else if !max_grace.is_zero() {
                db.take_pending_non_owned_refs(now - max_grace - jitter)
            } else {
                Vec::new()
            };
            if refs.is_empty() {
                continue;
            }

            let series_before = db.db.head().num_series();
            let _ = db.db.compact_ooo_head();
            if let Err(e) = db.db.compact_selected_series(&refs) {
                db.update_last_series_removal();
                warn!(user = %user_id, error = %e, "selected series compaction failed");
                continue;
            }
            db.update_last_series_removal();
            if db.db.head().num_series() < series_before {
                db.trigger_recompute_owned_series(RECOMPUTE_OWNED_SERIES_REASON_EARLY_COMPACTION);
                self.metrics
                    .early_compaction_non_owned_series_triggered
                    .with_label_values(&[&user_id])
                    .inc();
            }
        }
    }

    pub async fn close_and_delete_idle_user_tsdbs(&self) {
        let idle_timeout = Duration::from_nanos(
            self.blocks_cfg
                .tsdb
                .close_idle_tsdb_timeout
                .as_nanos()
                .max(0) as u64,
        );
        if idle_timeout.is_zero() {
            return;
        }
        for user_id in self.get_tsdb_users() {
            let res = self.close_and_delete_user_tsdb_if_idle(&user_id, idle_timeout).await;
            self.metrics
                .idle_tsdb_checks
                .with_label_values(&[res.as_label()])
                .inc();
        }
    }

    pub async fn close_and_delete_user_tsdb_if_idle(
        &self,
        user_id: &str,
        idle_timeout: Duration,
    ) -> TsdbCloseCheckResult {
        let Some(user_db) = self.get_tsdb(user_id) else {
            return TsdbCloseCheckResult::ShippingDisabled;
        };
        if user_db.shipper.is_none()
            && !self
                .blocks_cfg
                .tsdb
                .close_idle_tsdb_when_shipping_disabled
        {
            return TsdbCloseCheckResult::ShippingDisabled;
        }

        let check = user_db.should_close_tsdb(idle_timeout);
        if !check.should_close() {
            return check;
        }

        let (ok, _) = user_db.change_state(TsdbState::Active, TsdbState::Closing);
        if !ok {
            return TsdbCloseCheckResult::NotActive;
        }

        user_db.wait_in_flight_appends().await;

        let recheck = user_db.should_close_tsdb(idle_timeout);
        if !recheck.should_close() {
            let _ = user_db.change_state(TsdbState::Closing, TsdbState::Active);
            return recheck;
        }

        let num_series = user_db.db.head().num_series() as i64;
        self.series_count.fetch_sub(num_series, Ordering::Relaxed);
        let dir = user_db.db.dir().to_path_buf();
        let tracker_names = user_db.active_series.current_matcher_names();
        let label_value_bytes_names: Vec<String> = user_db
            .db
            .head()
            .label_values_bytes()
            .into_keys()
            .collect();

        if let Err(e) = user_db.close() {
            warn!(user = %user_id, error = %e, "failed to close idle TSDB");
            let _ = user_db.change_state(TsdbState::Closing, TsdbState::Active);
            return TsdbCloseCheckResult::CloseFailed;
        }

        let _ = user_db.change_state(TsdbState::Closing, TsdbState::Closed);
        self.tsdbs.remove(user_id);
        self.metrics.mem_users.dec();
        self.delete_user_metadata(user_id);
        self.metrics.delete_per_user_metrics(user_id, &tracker_names);
        self.metrics
            .delete_user_label_value_bytes_metrics(user_id, &label_value_bytes_names);

        if let Err(e) = std::fs::remove_dir_all(&dir) {
            warn!(user = %user_id, dir = %dir.display(), error = %e, "failed to remove closed idle TSDB directory");
            return TsdbCloseCheckResult::DataRemovalFailed;
        }

        info!(user = %user_id, "closed idle TSDB");
        TsdbCloseCheckResult::IdleClosed
    }

    pub fn update_active_series_metrics(&self, now: SystemTime) {
        for user_id in self.get_tsdb_users() {
            let Some(user_db) = self.get_tsdb(&user_id) else {
                continue;
            };
            let limits = self.overrides.for_tenant(&user_id);
            let new_matchers = parse_custom_tracker_matchers(&limits.active_series_custom_trackers);
            let old_names = user_db.active_series.current_matcher_names();
            let new_names: Vec<String> = new_matchers.iter().map(|m| m.name.clone()).collect();
            if old_names != new_names {
                user_db
                    .active_series
                    .reload_matchers(new_matchers, user_db.db.head());
            }

            user_db.active_series.purge(now);
            let (
                all_active,
                active_matching,
                all_otlp,
                all_histograms,
                active_matching_histograms,
                all_buckets,
                active_matching_buckets,
            ) = user_db.active_series.active_with_matchers();

            if all_active > 0 {
                self.metrics
                    .active_series_per_user
                    .with_label_values(&[&user_id])
                    .set(all_active as f64);
            } else {
                let _ = self
                    .metrics
                    .active_series_per_user
                    .remove_label_values(&[&user_id]);
            }
            if all_otlp > 0 {
                self.metrics
                    .active_series_per_user_otlp
                    .with_label_values(&[&user_id])
                    .set(all_otlp as f64);
            } else {
                let _ = self
                    .metrics
                    .active_series_per_user_otlp
                    .remove_label_values(&[&user_id]);
            }
            if all_histograms > 0 {
                self.metrics
                    .active_series_per_user_native_histograms
                    .with_label_values(&[&user_id])
                    .set(all_histograms as f64);
            } else {
                let _ = self
                    .metrics
                    .active_series_per_user_native_histograms
                    .remove_label_values(&[&user_id]);
            }
            if all_buckets > 0 {
                self.metrics
                    .active_native_histogram_buckets_per_user
                    .with_label_values(&[&user_id])
                    .set(all_buckets as f64);
            } else {
                let _ = self
                    .metrics
                    .active_native_histogram_buckets_per_user
                    .remove_label_values(&[&user_id]);
            }

            let cur_names = user_db.active_series.current_matcher_names();
            for (idx, name) in cur_names.iter().enumerate() {
                if active_matching.get(idx).copied().unwrap_or(0) > 0 {
                    self.metrics
                        .active_series_custom_trackers_per_user
                        .with_label_values(&[&user_id, name])
                        .set(active_matching[idx] as f64);
                } else {
                    let _ = self
                        .metrics
                        .active_series_custom_trackers_per_user
                        .remove_label_values(&[&user_id, name]);
                }
                if active_matching_histograms.get(idx).copied().unwrap_or(0) > 0 {
                    self.metrics
                        .active_series_custom_trackers_per_user_native_histograms
                        .with_label_values(&[&user_id, name])
                        .set(active_matching_histograms[idx] as f64);
                } else {
                    let _ = self
                        .metrics
                        .active_series_custom_trackers_per_user_native_histograms
                        .remove_label_values(&[&user_id, name]);
                }
                if active_matching_buckets.get(idx).copied().unwrap_or(0) > 0 {
                    self.metrics
                        .active_native_histogram_buckets_custom_trackers_per_user
                        .with_label_values(&[&user_id, name])
                        .set(active_matching_buckets[idx] as f64);
                } else {
                    let _ = self
                        .metrics
                        .active_native_histogram_buckets_custom_trackers_per_user
                        .remove_label_values(&[&user_id, name]);
                }
            }
        }
    }

    pub fn update_limit_metrics(&self) {
        for user_id in self.get_tsdb_users() {
            let Some(db) = self.get_tsdb(&user_id) else {
                continue;
            };
            let mut min_local = 0usize;
            if self.cfg.use_ingester_owned_series_for_limits
                || self.cfg.track_ingester_owned_series
            {
                let os = db.owned_series_state();
                self.metrics
                    .owned_series_per_user
                    .with_label_values(&[&user_id])
                    .set(os.owned_series_count as f64);
                if self.cfg.use_ingester_owned_series_for_limits {
                    min_local = os.local_series_limit;
                }
            }
            let local_limit = self.limiter.max_series_per_user(&user_id, min_local);
            self.metrics
                .max_local_series_per_user
                .with_label_values(&[&user_id])
                .set(local_limit as f64);

            if self
                .overrides
                .for_tenant(&user_id)
                .label_value_bytes_metric_threshold
                > 0
            {
                for (lbl_name, bytes) in db.db.head().label_values_bytes() {
                    if bytes > 0
                        && self
                            .limiter
                            .is_over_label_value_bytes_metric_threshold(&user_id, bytes)
                    {
                        self.metrics
                            .record_label_value_bytes_over_threshold(&user_id, &lbl_name);
                    }
                }
            }
        }
    }

    pub fn record_label_churn_series_refs(&self, now: SystemTime) {
        let now_ms = now
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        for user_id in self.get_tsdb_users() {
            if let Some(db) = self.get_tsdb(&user_id) {
                db.series_ref_history
                    .record(now_ms, db.db.head().last_series_ref());
            }
        }
    }
}

pub fn spawn_compaction_loop(
    ingester: Arc<Ingester>,
    mut force_rx: mpsc::Receiver<TriggerRequest>,
    ctx: ServiceContext,
) -> JoinHandle<()> {
    let zones = ingester
        .instance_ring
        .as_ref()
        .map(|r| r.zones())
        .unwrap_or_default();
    let (first_interval, mut standard_interval) =
        ingester.compaction_service_interval(SystemTime::now(), &zones);
    let min_delay = ingester.min_compaction_loop_delay();

    let non_owned_jitter = {
        let min_g = ingester
            .cfg
            .early_compaction_non_owned_series_min_grace_period
            .as_nanos()
            .max(0) as u64;
        if min_g > 0 {
            Duration::from_nanos(rand::random::<u64>() % (2 * min_g))
        } else {
            Duration::ZERO
        }
    };

    tokio::spawn(async move {
        ingester.set_compaction_service_state(State::Running);
        let first = first_interval.max(Duration::from_millis(10));
        let std_period = standard_interval.max(Duration::from_millis(10));
        let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + first, std_period);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = ctx.canceled() => break,
                _ = ticker.tick() => {
                    let zones = ingester
                        .instance_ring
                        .as_ref()
                        .map(|r| r.zones())
                        .unwrap_or_default();
                    let (new_first, new_std) =
                        ingester.compaction_service_interval(SystemTime::now(), &zones);
                    if new_std != standard_interval && ingester.state() != State::Starting {
                        standard_interval = new_std;
                        let nf = new_first.max(Duration::from_millis(10));
                        let ns = new_std.max(Duration::from_millis(10));
                        ticker = tokio::time::interval_at(tokio::time::Instant::now() + nf, ns);
                        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                        continue;
                    }

                    let iter_start = Instant::now();
                    ingester.num_compactions_in_progress.fetch_add(1, Ordering::AcqRel);
                    ingester.compact_blocks(false, 0, None).await;
                    ingester.compact_blocks_to_reduce_in_memory_series(SystemTime::now()).await;
                    ingester.compact_blocks_to_reduce_per_tenant_owned_series(SystemTime::now()).await;
                    ingester.compact_blocks_due_to_non_owned_series(non_owned_jitter).await;
                    ingester.num_compactions_in_progress.fetch_sub(1, Ordering::AcqRel);
                    ingester.sync_offset_catalogues().await;

                    let extra = compaction_loop_extra_delay(iter_start.elapsed(), standard_interval, min_delay);
                    if !extra.is_zero() {
                        let sleep_fut = tokio::time::sleep(extra);
                        tokio::pin!(sleep_fut);
                        loop {
                            tokio::select! {
                                _ = ctx.canceled() => {
                                    ingester.set_compaction_service_state(State::Terminated);
                                    return;
                                }
                                _ = &mut sleep_fut => break,
                                req_opt = force_rx.recv() => {
                                    let Some(req) = req_opt else {
                                        ingester.set_compaction_service_state(State::Terminated);
                                        return;
                                    };
                                    ingester.compact_blocks(true, i64::MAX, req.users.as_ref()).await;
                                    ingester.sync_offset_catalogues().await;
                                    let _ = req.callback.send(());
                                }
                            }
                        }
                    }

                    let zones = ingester
                        .instance_ring
                        .as_ref()
                        .map(|r| r.zones())
                        .unwrap_or_default();
                    let (new_first, new_std) =
                        ingester.compaction_service_interval(SystemTime::now(), &zones);
                    if new_std != standard_interval {
                        standard_interval = new_std;
                        let nf = new_first.max(Duration::from_millis(10));
                        let ns = new_std.max(Duration::from_millis(10));
                        ticker = tokio::time::interval_at(tokio::time::Instant::now() + nf, ns);
                        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                    }
                }
                req_opt = force_rx.recv() => {
                    let Some(req) = req_opt else { break; };
                    ingester.compact_blocks(true, i64::MAX, req.users.as_ref()).await;
                    ingester.sync_offset_catalogues().await;
                    let _ = req.callback.send(());
                }
            }
        }
        ingester.set_compaction_service_state(State::Terminated);
    })
}

pub fn spawn_shipping_loop(
    ingester: Arc<Ingester>,
    mut ship_rx: mpsc::Receiver<TriggerRequest>,
    ctx: ServiceContext,
) -> JoinHandle<()> {
    let interval = Duration::from_nanos(
        ingester
            .blocks_cfg
            .tsdb
            .ship_interval
            .as_nanos()
            .max(10_000_000) as u64,
    );
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        ticker.tick().await;
        loop {
            tokio::select! {
                _ = ctx.canceled() => break,
                _ = ticker.tick() => {
                    ingester.ship_blocks(None).await;
                }
                req_opt = ship_rx.recv() => {
                    let Some(req) = req_opt else { break; };
                    ingester.ship_blocks(req.users.as_ref()).await;
                    let _ = req.callback.send(());
                }
            }
        }
    })
}

pub fn spawn_idle_tsdb_loop(ingester: Arc<Ingester>, ctx: ServiceContext) -> JoinHandle<()> {
    let interval = ingester.close_idle_tsdb_interval().max(Duration::from_millis(10));
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        ticker.tick().await;
        loop {
            tokio::select! {
                _ = ctx.canceled() => break,
                _ = ticker.tick() => ingester.close_and_delete_idle_user_tsdbs().await,
            }
        }
    })
}

pub fn spawn_metadata_purger_loop(ingester: Arc<Ingester>, ctx: ServiceContext) -> JoinHandle<()> {
    let interval = Duration::from_secs(5 * 60);
    tokio::spawn(async move {
        ingester.set_metadata_purger_service_state(State::Running);
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        ticker.tick().await;
        loop {
            tokio::select! {
                _ = ctx.canceled() => break,
                _ = ticker.tick() => ingester.purge_user_metrics_metadata(),
            }
        }
        ingester.set_metadata_purger_service_state(State::Terminated);
    })
}

pub fn spawn_metrics_updater_loop(ingester: Arc<Ingester>, ctx: ServiceContext) -> JoinHandle<()> {
    let rate_period = Duration::from_nanos(
        ingester.cfg.rate_update_period.as_nanos().max(10_000_000) as u64,
    );
    let active_period = Duration::from_nanos(
        ingester
            .cfg
            .active_series
            .active_series_metrics_update_period
            .as_nanos()
            .max(10_000_000) as u64,
    );
    let active_idle = Duration::from_nanos(
        ingester
            .cfg
            .active_series
            .active_series_metrics_idle_timeout
            .as_nanos()
            .max(0) as u64,
    );

    let limit_period = ingester.limit_metrics_update_period().max(Duration::from_millis(10));

    tokio::spawn(async move {
        ingester.set_metrics_updater_service_state(State::Running);
        let started_at = Instant::now();
        let mut flipped_loading = false;
        let mut ingest_rate_tick = tokio::time::interval(INSTANCE_INGESTION_RATE_TICK_INTERVAL);
        let mut rate_tick = tokio::time::interval(rate_period);
        let mut active_tick = tokio::time::interval(active_period);
        let mut limit_tick = tokio::time::interval(limit_period);
        let mut churn_tick = tokio::time::interval(LABEL_CHURN_RECORD_INTERVAL);

        ingest_rate_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        rate_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        active_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        limit_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        churn_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // Consume immediate first ticks so tickers fire after their configured period (matching Go time.NewTicker).
        ingest_rate_tick.tick().await;
        rate_tick.tick().await;
        active_tick.tick().await;
        limit_tick.tick().await;
        churn_tick.tick().await;

        loop {
            tokio::select! {
                _ = ctx.canceled() => break,
                _ = ingest_rate_tick.tick() => {
                    ingester.ingestion_rate.tick();
                }
                _ = rate_tick.tick() => {
                    for entry in ingester.tsdbs.iter() {
                        entry.value().ingested_api_samples.tick();
                        entry.value().ingested_rule_samples.tick();
                    }
                }
                _ = active_tick.tick() => {
                    if ingester.cfg.active_series.active_series_metrics_enabled {
                        if !flipped_loading && started_at.elapsed() >= active_idle {
                            ingester.metrics.active_series_loading.set(0.0);
                            flipped_loading = true;
                        }
                        ingester.update_active_series_metrics(SystemTime::now());
                    }
                }
                _ = limit_tick.tick() => {
                    ingester.update_limit_metrics();
                }
                _ = churn_tick.tick() => {
                    if ingester.cfg.label_churn_enabled {
                        ingester.record_label_churn_series_refs(SystemTime::now());
                    }
                }
            }
        }
        ingester.set_metrics_updater_service_state(State::Terminated);
    })
}

pub fn spawn_owned_series_loop(ingester: Arc<Ingester>, ctx: ServiceContext) -> JoinHandle<()> {
    let normal_interval = Duration::from_nanos(
        ingester
            .cfg
            .owned_series_update_interval
            .as_nanos()
            .max(10_000_000) as u64,
    );
    tokio::spawn(async move {
        let Some(ref strategy) = ingester.owned_series_strategy else {
            return;
        };
        ingester.set_owned_series_service_state(State::Running);
        let mut cur_interval = DEFAULT_OWNED_SERIES_INITIAL_RETRY_INTERVAL;
        loop {
            tokio::select! {
                _ = ctx.canceled() => break,
                _ = tokio::time::sleep(cur_interval) => {
                    if ingester.is_owned_series_paused() {
                        continue;
                    }
                    match strategy.check_ring_for_changes() {
                        Ok(changed) => {
                            cur_interval = normal_interval;
                            let start = Instant::now();
                            for uid in ingester.get_tsdb_users() {
                                if let Some(db) = ingester.get_tsdb(&uid) {
                                    db.update_owned_series_if_needed(strategy.as_ref(), changed);
                                }
                            }
                            ingester
                                .owned_series_metrics
                                .check_duration
                                .observe(start.elapsed().as_secs_f64());
                        }
                        Err(e) => {
                            debug!(error = %e, "owned series ring check failed");
                        }
                    }
                }
            }
        }
        ingester.set_owned_series_service_state(State::Terminated);
    })
}
