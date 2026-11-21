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

use futures::StreamExt;
use mimir_common::services::{ServiceContext, State};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use crate::active_series::{build_custom_trackers_config, merged_custom_trackers, Matchers};
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
        // Go `concurrency.ForEachUser(..., HeadCompactionConcurrency, ...)`.
        let concurrency = self.blocks_cfg.tsdb.head_compaction_concurrency.max(1) as usize;
        let user_ids: Vec<String> = self
            .get_tsdb_users()
            .into_iter()
            .filter(|u| allowed.is_none_or(|a| a.contains(u)))
            .collect();
        futures::stream::iter(user_ids)
            .for_each_concurrent(concurrency, |user_id| async move {
                self.compact_user_blocks(&user_id, force, forced_compaction_max_time)
                    .await;
            })
            .await;
        self.metrics.reset_forced_compactions();
    }

    async fn compact_user_blocks(
        &self,
        user_id: &str,
        force: bool,
        forced_compaction_max_time: i64,
    ) {
        let Some(user_db) = self.get_tsdb(user_id) else {
            return;
        };
        let _compaction_guard = user_db.compaction_mtx.lock().await;
        let head = user_db.db.head();
        if head.num_series() == 0 {
            return;
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
            let db = Arc::clone(&user_db);
            tokio::task::spawn_blocking(move || {
                db.compact().map(|_| ()).map_err(|e| e.to_string())
            })
            .await
            .unwrap_or_else(|e| Err(format!("compaction task failed: {e}")))
        };

        user_db.update_last_series_removal();
        if let Err(e) = res {
            self.metrics.compactions_failed.inc();
            warn!(user = %user_id, reason, err = %e, "TSDB blocks compaction failed");
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

    pub async fn ship_blocks(&self, allowed: Option<&HashSet<String>>) {
        // Go `concurrency.ForEachUser(..., ShipConcurrency, ...)`: the number of concurrent
        // workers is limited to avoid syncing a lot of tenants at once.
        let concurrency = self.blocks_cfg.tsdb.ship_concurrency.max(1) as usize;
        let user_ids: Vec<String> = self
            .get_tsdb_users()
            .into_iter()
            .filter(|u| allowed.is_none_or(|a| a.contains(u)))
            .collect();
        futures::stream::iter(user_ids)
            .for_each_concurrent(concurrency, |user_id| async move {
                self.ship_user_blocks(&user_id).await;
            })
            .await;
    }

    async fn ship_user_blocks(&self, user_id: &str) {
        let Some(user_db) = self.get_tsdb(user_id) else {
            return;
        };
        let Some(shipper) = user_db.shipper.read().clone() else {
            return;
        };

        let bucket_opt = self.bucket.read().clone();
        if user_db.check_deletion_mark(bucket_opt.as_deref()).await {
            return;
        }

        let (ok, st) = user_db.change_state(TsdbState::Active, TsdbState::ActiveShipping);
        if !ok {
            info!(user = %user_id, state = st.as_str(), "shipper skipped because TSDB is not active");
            return;
        }

        let sync_res = shipper.sync().await;
        let _ = user_db.change_state(TsdbState::ActiveShipping, TsdbState::Active);

        let uploaded = match sync_res {
            Ok(uploaded) => {
                debug!(user = %user_id, uploaded, "shipper successfully synchronized TSDB blocks with storage");
                uploaded
            }
            Err(e) => {
                let uploaded = match e {
                    mimir_storage::shipper::ShipperError::PartialSync { shipped, .. } => shipped,
                    _ => 0,
                };
                warn!(user = %user_id, uploaded, err = %e, "shipper failed to synchronize TSDB blocks with the storage");
                uploaded
            }
        };
        // The shipper meta file could be updated even if Sync() returned an error, so update
        // the cache each time at least a block has been uploaded (Go `shipBlocks`).
        if uploaded > 0 {
            if let Err(e) = user_db.update_cached_shipped_blocks() {
                warn!(user = %user_id, err = %e, "failed to update cached shipped blocks after shipper synchronisation");
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
        let hw = Arc::new(reader.last_seen_offsets());
        let concurrency = self
            .blocks_cfg
            .tsdb
            .offset_catalogue
            .sync_concurrency
            .max(1) as usize;
        let tasks: Vec<_> = self
            .get_tsdb_users()
            .into_iter()
            .filter_map(|user_id| {
                let db = self.get_tsdb(&user_id)?;
                db.offset_catalogue.as_ref()?;
                Some((user_id, db))
            })
            .collect();
        futures::stream::iter(tasks)
            .for_each_concurrent(concurrency, |(user_id, db)| {
                let hw = Arc::clone(&hw);
                async move {
                    let uid = user_id.clone();
                    let res = tokio::task::spawn_blocking(move || {
                        if let Some(ref cat) = db.offset_catalogue {
                            cat.sync(&hw)
                        } else {
                            Ok(())
                        }
                    })
                    .await;
                    match res {
                        Ok(Ok(())) => {}
                        Ok(Err(e)) => {
                            warn!(user = %uid, err = %e, "offset catalogue sync failed");
                        }
                        Err(e) => {
                            warn!(user = %uid, err = %e, "offset catalogue sync join error");
                        }
                    }
                }
            })
            .await;
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

    /// Returns the active zone-staggering key `(zone_count, zone_index)` once running with
    /// zone awareness enabled and >1 zones, or `None` when staggering is inactive.
    /// Used by `spawn_compaction_loop` to re-align the compaction ticker whenever the
    /// ring's zone topology changes (e.g. peer zones joining `1 -> 3` after startup).
    pub(crate) fn compaction_zone_stagger_key(&self, zones: &[String]) -> Option<(usize, usize)> {
        if self.state() == State::Starting
            || !self.cfg.ring.zone_awareness_enabled
            || self.cfg.ring.instance_availability_zone.is_empty()
            || zones.len() <= 1
        {
            return None;
        }
        let mut sorted = zones.to_vec();
        sorted.sort();
        let idx = sorted
            .iter()
            .position(|z| z == &self.cfg.ring.instance_availability_zone)?;
        Some((sorted.len(), idx))
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
            db.active_series.purge_with_index(now, Some(db.db.head()));
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
            db.active_series.purge_with_index(now, Some(db.db.head()));
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
                let cutoff = now
                    .checked_sub(min_grace)
                    .and_then(|t| t.checked_sub(jitter))
                    .unwrap_or(UNIX_EPOCH);
                db.take_pending_non_owned_refs(cutoff)
            } else if !max_grace.is_zero() {
                let cutoff = now
                    .checked_sub(max_grace)
                    .and_then(|t| t.checked_sub(jitter))
                    .unwrap_or(UNIX_EPOCH);
                db.take_pending_non_owned_refs(cutoff)
            } else {
                Vec::new()
            };
            if refs.is_empty() {
                continue;
            }

            let _compaction_guard = db.compaction_mtx.lock().await;
            let series_before = db.db.head().num_series();
            let db_c = Arc::clone(&db);
            let compact_res = tokio::task::spawn_blocking(move || {
                let _ = db_c.db.compact_ooo_head();
                let res = db_c.db.compact_selected_series(&refs);
                db_c.update_last_series_removal();
                res
            })
            .await;
            match compact_res {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => {
                    warn!(user = %user_id, err = %e, "selected series compaction failed");
                    continue;
                }
                Err(e) => {
                    warn!(user = %user_id, err = %e, "selected series compaction join error");
                    continue;
                }
            }
            if db.db.head().num_series() != series_before {
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
        if user_db.shipper.read().is_none()
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
        let tenant_deleted = recheck == TsdbCloseCheckResult::TenantMarkedForDeletion;

        // Go `closeAndDeleteUserTSDBIfIdle` runs `i.seriesCount.Sub(userDB.Head().NumSeries())`
        // BEFORE `userDB.Close()` and never restores it when `Close()` fails (`tsdbCloseFailed`).
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

        let user_db_close = Arc::clone(&user_db);
        match tokio::task::spawn_blocking(move || user_db_close.close()).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                warn!(user = %user_id, err = %e, "failed to close idle TSDB");
                let _ = user_db.change_state(TsdbState::Closing, TsdbState::Active);
                return TsdbCloseCheckResult::CloseFailed;
            }
            Err(e) => {
                warn!(user = %user_id, err = %e, "idle TSDB close join error");
                let _ = user_db.change_state(TsdbState::Closing, TsdbState::Active);
                return TsdbCloseCheckResult::CloseFailed;
            }
        }

        struct RemoveTsdbGuard<'a> {
            ingester: &'a Ingester,
            user_id: &'a str,
            user_db: &'a Arc<crate::user_tsdb::UserTsdb>,
        }
        impl Drop for RemoveTsdbGuard<'_> {
            fn drop(&mut self) {
                let _g = self.ingester.tsdb_create_mtx.lock();
                if self
                    .ingester
                    .tsdbs
                    .remove_if(self.user_id, |_, v| Arc::ptr_eq(v, self.user_db))
                    .is_some()
                {
                    self.ingester.metrics.mem_users.dec();
                }
            }
        }

        let _ = user_db.change_state(TsdbState::Closing, TsdbState::Closed);
        // Keep user_id (in Closed state) in self.tsdbs until metric cleanup and
        // remove_dir_all finish so concurrent Push/get_or_create_tsdb cannot reopen
        // a fresh TSDB in the directory while it is being deleted (Go defer parity).
        let _remove_guard = RemoveTsdbGuard {
            ingester: self,
            user_id,
            user_db: &user_db,
        };
        self.tsdb_metrics.remove_registry_for_tenant(user_id);
        self.delete_user_metadata(user_id);
        self.metrics.delete_per_user_metrics(user_id, &tracker_names);
        self.compute_worker_pool.remove_tenant(user_id);
        self.metrics
            .delete_user_label_value_bytes_metrics(user_id, &label_value_bytes_names);
        if let Some(mgr) = self.cost_attribution_manager() {
            mgr.remove_tenant(user_id);
        }

        let dir_remove = dir.clone();
        match tokio::task::spawn_blocking(move || std::fs::remove_dir_all(&dir_remove)).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                warn!(user = %user_id, dir = %dir.display(), err = %e, "failed to remove closed idle TSDB directory");
                return TsdbCloseCheckResult::DataRemovalFailed;
            }
            Err(e) => {
                warn!(user = %user_id, dir = %dir.display(), err = %e, "idle TSDB directory removal join error");
                return TsdbCloseCheckResult::DataRemovalFailed;
            }
        }

        drop(_remove_guard);
        drop(user_db);

        if tenant_deleted {
            info!(user = %user_id, "deleted local TSDB, user marked for deletion");
            TsdbCloseCheckResult::TenantMarkedForDeletion
        } else {
            info!(user = %user_id, "closed idle TSDB");
            TsdbCloseCheckResult::IdleClosed
        }
    }

    pub fn update_active_series_metrics(&self, now: SystemTime) {
        for user_id in self.get_tsdb_users() {
            let Some(user_db) = self.get_tsdb(&user_id) else {
                continue;
            };
            let limits = self.overrides.for_tenant(&user_id);
            let new_trackers = build_custom_trackers_config(&merged_custom_trackers(
                &limits.active_series_custom_trackers,
                &limits.active_series_additional_custom_trackers,
            ));
            let new_matcher_set = Matchers::new(new_trackers);
            let new_cat = self
                .cost_attribution_manager()
                .and_then(|m| m.active_series_tracker(&user_id));
            let matchers_changed = user_db.active_series.matchers_differ(new_matcher_set.config());
            let cat_changed = user_db.active_series.cost_attribution_differs(new_cat.as_ref());
            // Go keeps the old matcher metrics until after the reload (to avoid staleness markers
            // if a scrape races with the reload) and then deletes those no longer present.
            let mut old_matcher_names = Vec::new();
            if matchers_changed || cat_changed {
                if matchers_changed {
                    old_matcher_names = user_db.active_series.current_matcher_names();
                }
                user_db.active_series.reload_series_config(
                    new_matcher_set,
                    new_cat,
                    matchers_changed,
                    cat_changed,
                    Some(user_db.db.head()),
                );
            }

            user_db
                .active_series
                .purge_with_index(now, Some(user_db.db.head()));
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

            let attribution_failures = user_db.active_series.active_series_attribution_failure_count();
            if attribution_failures > 0.0 {
                self.metrics
                    .attributed_active_series_failures_per_user
                    .with_label_values(&[&user_id])
                    .inc_by(attribution_failures);
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

            // Remove the metrics belonging to old matchers now.
            if matchers_changed {
                for old_name in old_matcher_names.iter().filter(|n| !cur_names.contains(n)) {
                    let _ = self
                        .metrics
                        .active_series_custom_trackers_per_user
                        .remove_label_values(&[&user_id, old_name]);
                    let _ = self
                        .metrics
                        .active_series_custom_trackers_per_user_native_histograms
                        .remove_label_values(&[&user_id, old_name]);
                    let _ = self
                        .metrics
                        .active_native_histogram_buckets_custom_trackers_per_user
                        .remove_label_values(&[&user_id, old_name]);
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

            self.metrics
                .delete_user_label_value_bytes_metrics(&user_id, &[]);

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
    let mut zone_stagger_key = ingester.compaction_zone_stagger_key(&zones);
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
                    let iter_start = Instant::now();
                    ingester.num_compactions_in_progress.fetch_add(1, Ordering::AcqRel);
                    ingester.compact_blocks(false, 0, None).await;
                    ingester.compact_blocks_to_reduce_in_memory_series(SystemTime::now()).await;
                    ingester.compact_blocks_to_reduce_per_tenant_owned_series(SystemTime::now()).await;
                    ingester.compact_blocks_due_to_non_owned_series(non_owned_jitter).await;
                    ingester.num_compactions_in_progress.fetch_sub(1, Ordering::AcqRel);

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

                    // Re-check standard interval and zone topology after each iteration
                    // (matching Go `ingester_compaction.go:187-194` plus zone re-alignment
                    // when peer zones join the ring after startup).
                    let zones = ingester
                        .instance_ring
                        .as_ref()
                        .map(|r| r.zones())
                        .unwrap_or_default();
                    let (new_first, new_std) =
                        ingester.compaction_service_interval(SystemTime::now(), &zones);
                    let new_zone_stagger_key = ingester.compaction_zone_stagger_key(&zones);
                    if new_std != standard_interval || new_zone_stagger_key != zone_stagger_key {
                        standard_interval = new_std;
                        zone_stagger_key = new_zone_stagger_key;
                        let nf = new_first.max(Duration::from_millis(10));
                        let ns = new_std.max(Duration::from_millis(10));
                        ticker = tokio::time::interval_at(tokio::time::Instant::now() + nf, ns);
                        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                    }

                    ingester.sync_offset_catalogues().await;
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

/// Ported from `github.com/grafana/mimir/pkg/util/time.go` (`DurationWithJitter`).
pub fn duration_with_jitter(input: Duration, variance_perc: f64) -> Duration {
    if variance_perc <= 0.0 {
        return input;
    }
    let variance = (input.as_nanos() as f64 * variance_perc) as i64;
    if variance <= 0 {
        return input;
    }
    let span = (variance as u64).saturating_mul(2);
    if span == 0 {
        return input;
    }
    let jitter = (rand::random::<u64>() % span) as i64 - variance;
    let out_ns = (input.as_nanos() as i128) + (jitter as i128);
    Duration::from_nanos(out_ns.max(0) as u64)
}

pub fn spawn_shipping_loop(
    ingester: Arc<Ingester>,
    mut ship_rx: mpsc::Receiver<TriggerRequest>,
    ctx: ServiceContext,
) -> JoinHandle<()> {
    // pkg/ingester/ingester_compaction.go:37: apply 1% jitter so shipping never phase-locks
    // with head compaction or the idle TSDB closer (e.g. DefaultCloseIdleTSDBInterval = 5m).
    let raw_interval = Duration::from_nanos(
        ingester
            .blocks_cfg
            .tsdb
            .ship_interval
            .as_nanos()
            .max(10_000_000) as u64,
    );
    let interval = duration_with_jitter(raw_interval, 0.01).max(Duration::from_millis(10));
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
        let mut usage_stats_tick = tokio::time::interval(USAGE_STATS_UPDATE_INTERVAL);
        let mut limit_tick = tokio::time::interval(limit_period);
        let mut churn_tick = tokio::time::interval(LABEL_CHURN_RECORD_INTERVAL);
        // Go runs the 250ms inflight requests ticker in its own goroutine so that it is not
        // delayed by the other (possibly slow) updates of this loop.
        let inflight_ingester = Arc::clone(&ingester);
        let inflight_ctx = ctx.clone();
        let inflight_sampler = tokio::spawn(async move {
            let mut tick = tokio::time::interval(INFLIGHT_REQUESTS_SAMPLE_INTERVAL);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            tick.tick().await;
            loop {
                tokio::select! {
                    _ = inflight_ctx.canceled() => break,
                    _ = tick.tick() => {
                        let inflight = inflight_ingester.inflight_push_requests.load(Ordering::Relaxed);
                        inflight_ingester.metrics.inflight_requests_summary.observe(inflight as f64);
                    }
                }
            }
        });

        ingest_rate_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        rate_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        active_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        usage_stats_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        limit_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        churn_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // Consume immediate first ticks so tickers fire after their configured period (matching Go time.NewTicker).
        ingest_rate_tick.tick().await;
        rate_tick.tick().await;
        active_tick.tick().await;
        usage_stats_tick.tick().await;
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
                        let ing_c = Arc::clone(&ingester);
                        let _ = tokio::task::spawn_blocking(move || {
                            ing_c.update_active_series_metrics(SystemTime::now());
                        })
                        .await;
                    }
                }
                _ = usage_stats_tick.tick() => {
                    let ing_c = Arc::clone(&ingester);
                    let _ = tokio::task::spawn_blocking(move || {
                        ing_c.update_usage_stats();
                    })
                    .await;
                }
                _ = limit_tick.tick() => {
                    let ing_c = Arc::clone(&ingester);
                    let _ = tokio::task::spawn_blocking(move || {
                        ing_c.update_limit_metrics();
                    })
                    .await;
                }
                _ = churn_tick.tick() => {
                    if ingester.cfg.label_churn_enabled {
                        ingester.record_label_churn_series_refs(SystemTime::now());
                    }
                }
            }
        }
        inflight_sampler.abort();
        ingester.set_metrics_updater_service_state(State::Terminated);
    })
}

/// Go `metricsUpdaterServiceRunning` `inflightRequestsTicker`.
const INFLIGHT_REQUESTS_SAMPLE_INTERVAL: Duration = Duration::from_millis(250);
/// Go `usageStatsUpdateInterval` (`usagestats.DefaultReportSendInterval / 10` = 4h / 10 = 24m).
const USAGE_STATS_UPDATE_INTERVAL: Duration = Duration::from_secs(24 * 60);

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
        let mut cur_interval = if ingester.owned_series_initial_ring_check_succeeded() {
            normal_interval
        } else {
            DEFAULT_OWNED_SERIES_INITIAL_RETRY_INTERVAL
        };
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
                            let ing_clone = Arc::clone(&ingester);
                            let strat_clone = Arc::clone(strategy);
                            let cancel_ctx = ctx.clone();
                            match tokio::task::spawn_blocking(move || {
                                let start = Instant::now();
                                let mut updated_users = 0usize;
                                for uid in ing_clone.get_tsdb_users() {
                                    if cancel_ctx.is_canceled() {
                                        return (updated_users, start.elapsed());
                                    }
                                    if let Some(db) = ing_clone.get_tsdb(&uid) {
                                        if db.update_owned_series_if_needed(
                                            strat_clone.as_ref(),
                                            changed,
                                        ) {
                                            updated_users += 1;
                                        }
                                    }
                                }
                                (updated_users, start.elapsed())
                            })
                            .await
                            {
                                Ok((updated_users, elapsed)) => {
                                    if updated_users > 0 {
                                        info!(
                                            updated_users,
                                            duration_ms = elapsed.as_millis() as u64,
                                            ring_changed = changed,
                                            "updated owned series for users"
                                        );
                                    }
                                    ingester
                                        .owned_series_metrics
                                        .check_duration
                                        .observe(elapsed.as_secs_f64());
                                    if ctx.is_canceled() {
                                        break;
                                    }
                                }
                                Err(e) => {
                                    error!(err = %e, "owned series update task failed");
                                }
                            }
                        }
                        Err(e) => {
                            error!(err = %e, "can't check ring for updates");
                        }
                    }
                }
            }
        }
        ingester.set_owned_series_service_state(State::Terminated);
    })
}

#[cfg(test)]
mod tests {
    use super::duration_with_jitter;
    use std::time::Duration;

    #[test]
    fn duration_with_jitter_bounds_and_zero_variance() {
        let input = Duration::from_secs(60);
        assert_eq!(duration_with_jitter(input, 0.0), input);
        assert_eq!(duration_with_jitter(Duration::ZERO, 0.01), Duration::ZERO);

        let min_expected = Duration::from_millis(59_400);
        let max_expected = Duration::from_millis(60_600);
        let mut saw_non_equal = false;
        for _ in 0..64 {
            let out = duration_with_jitter(input, 0.01);
            assert!(
                out >= min_expected && out <= max_expected,
                "out={out:?} out of [59.4s, 60.6s]"
            );
            if out != input {
                saw_non_equal = true;
            }
        }
        assert!(saw_non_equal, "expected at least one jittered sample != input");
    }

    #[tokio::test]
    async fn compaction_zone_stagger_key_tracks_count_index_and_state() {
        use mimir_common::services::State;
        use mimir_config::limits::LimitsConfig;
        use mimir_config::{BlocksStorageConfig, IngestStorageConfig, IngesterConfig, Overrides};
        use crate::ingester::Ingester;

        let tmp = tempfile::tempdir().unwrap();
        let mut cfg = IngesterConfig::default();
        cfg.ring.zone_awareness_enabled = true;
        cfg.ring.instance_availability_zone = "zone-b".to_string();

        let mut blocks_cfg = BlocksStorageConfig::default();
        blocks_cfg.tsdb.dir = tmp.path().join("tsdb").to_string_lossy().to_string();
        blocks_cfg.bucket.backend_cfg.backend = "filesystem".to_string();
        blocks_cfg.bucket.backend_cfg.filesystem.dir =
            tmp.path().join("bucket").to_string_lossy().to_string();

        let ing = Ingester::new(
            cfg,
            blocks_cfg,
            IngestStorageConfig::default(),
            Overrides::new(LimitsConfig::default()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();

        // While Starting, zone staggering is inactive regardless of zones.
        ing.set_state_for_test(State::Starting);
        assert_eq!(
            ing.compaction_zone_stagger_key(&[
                "zone-a".to_string(),
                "zone-b".to_string(),
                "zone-c".to_string(),
            ]),
            None
        );

        ing.set_state_for_test(State::Running);
        assert_eq!(ing.compaction_zone_stagger_key(&[]), None);
        assert_eq!(ing.compaction_zone_stagger_key(&["zone-b".to_string()]), None);
        assert_eq!(
            ing.compaction_zone_stagger_key(&["zone-b".to_string(), "zone-c".to_string()]),
            Some((2, 0))
        );
        // Same zone count, shifted index -> distinct key.
        assert_eq!(
            ing.compaction_zone_stagger_key(&["zone-a".to_string(), "zone-b".to_string()]),
            Some((2, 1))
        );
        // Zone count growth 2 -> 3 -> distinct key.
        assert_eq!(
            ing.compaction_zone_stagger_key(&[
                "zone-c".to_string(),
                "zone-a".to_string(),
                "zone-b".to_string(),
            ]),
            Some((3, 1))
        );
    }

    #[tokio::test]
    async fn spawn_compaction_loop_executes_transition_tick_and_realigns_zones_1_to_3() {
        use super::spawn_compaction_loop;
        use crate::ingester::Ingester;
        use mimir_common::services::{Service, ServiceContext, State};
        use mimir_config::limits::LimitsConfig;
        use mimir_config::{
            BlocksStorageConfig, GoDuration, IngestStorageConfig, IngesterConfig, Overrides,
        };
        use mimir_proto::ringpb::{Desc, InstanceState};
        use mimir_ring::kv::inmemory::InMemoryKv;
        use mimir_ring::kv::{CasOutcome, KvClient};
        use mimir_ring::ring::model::{ring_desc_codec, DescExt};
        use mimir_ring::ring::replication_strategy::DefaultReplicationStrategy;
        use mimir_ring::ring::{Ring, RingSettings};
        use std::collections::HashMap;
        use std::sync::Arc;
        use std::time::{Instant, SystemTime, UNIX_EPOCH};
        use tokio::sync::mpsc;

        let tmp = tempfile::tempdir().unwrap();
        let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));

        // Seed 1 zone ("zone-b") initially in the ring.
        kv.cas(
            "ring",
            Box::new(|cur| {
                let mut desc = cur
                    .and_then(|b| b.downcast_ref::<Desc>().cloned())
                    .unwrap_or_default();
                desc.add_ingester(
                    "ing-b",
                    "127.0.0.1:9095",
                    "zone-b",
                    vec![100],
                    InstanceState::Active,
                    None,
                    false,
                    None,
                    HashMap::new(),
                );
                Ok(CasOutcome::Update(Box::new(desc)))
            }),
        )
        .await
        .unwrap();

        let mut cfg = IngesterConfig::default();
        cfg.ring.kvstore.store = "inmemory".to_string();
        cfg.ring.zone_awareness_enabled = true;
        cfg.ring.replication_factor = 1;
        cfg.ring.instance_id = "ing-b".to_string();
        cfg.ring.instance_availability_zone = "zone-b".to_string();

        let ring = Ring::with_store_and_strategy(
            RingSettings::from(&cfg.ring),
            "ingester",
            "ring",
            Arc::clone(&kv),
            Arc::new(DefaultReplicationStrategy),
            None,
        )
        .unwrap();
        let ring_svc = Arc::clone(&ring).into_service();
        ring_svc
            .start_async(ServiceContext::background())
            .await
            .unwrap();
        ring_svc.await_running().await.unwrap();

        let deadline = Instant::now() + Duration::from_secs(2);
        while ring.zones().len() != 1 {
            assert!(Instant::now() < deadline, "timed out waiting for initial zone");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // 1000ms divides 3600s cleanly; with 3 zones ["zone-a", "zone-b", "zone-c"],
        // zone-b (idx 1) aligns to offset 333ms within each 1000ms window.
        let mut blocks_cfg = BlocksStorageConfig::default();
        blocks_cfg.tsdb.dir = tmp.path().join("tsdb").to_string_lossy().to_string();
        blocks_cfg.tsdb.head_compaction_interval = GoDuration::from_millis(1000);
        blocks_cfg.bucket.backend_cfg.backend = "filesystem".to_string();
        blocks_cfg.bucket.backend_cfg.filesystem.dir =
            tmp.path().join("bucket").to_string_lossy().to_string();

        let ingester = Ingester::new(
            cfg,
            blocks_cfg,
            IngestStorageConfig::default(),
            Overrides::new(LimitsConfig::default()),
            Some(Arc::clone(&ring)),
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        ingester.set_head_compaction_interval_while_starting(Duration::from_millis(40));
        ingester.set_head_compaction_interval_jitter_enabled(false);
        ingester.set_min_compaction_loop_delay(Duration::ZERO);
        ingester.set_state_for_test(State::Starting);

        // Ensure user TSDB has 1 series so every regular compaction tick increments compactions_triggered.
        let udb = ingester.get_or_create_tsdb("user-1").unwrap();
        {
            let mut app = udb.db.appender();
            app.append_float(None, &[("__name__", "up")], 1_000, 1.0)
                .unwrap();
            app.commit().unwrap();
        }

        // Align loop start just after a 1000ms boundary (phase in [0, 80ms)) so Tick 1 fires at
        // ~40-120ms and Tick 2 fires ~1000ms later at ~40-120ms (far from zone-b's 333ms slot).
        loop {
            let phase_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis()
                % 1000;
            if phase_ms < 80 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        let ctx = ServiceContext::background();
        let (_force_tx, force_rx) = mpsc::channel(1);
        let loop_handle = spawn_compaction_loop(Arc::clone(&ingester), force_rx, ctx.child());

        // Transition Starting -> Running before Tick 1 fires. Tick 1 MUST still execute!
        ingester.set_state_for_test(State::Running);

        let d1 = Instant::now() + Duration::from_millis(400);
        while (ingester.metrics.compactions_triggered.get() as u64) < 1 {
            assert!(
                Instant::now() < d1,
                "transition tick after leaving State::Starting was skipped"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        // Wait briefly so Tick 1 finishes resetting the ticker to the unstaggered 1000ms
        // standard interval while zones.len() == 1.
        tokio::time::sleep(Duration::from_millis(80)).await;

        // Now grow ring zones 1 -> 3 while already Running on the 1000ms standard interval.
        kv.cas(
            "ring",
            Box::new(|cur| {
                let mut desc = cur
                    .and_then(|b| b.downcast_ref::<Desc>().cloned())
                    .unwrap_or_default();
                desc.add_ingester(
                    "ing-a",
                    "127.0.0.1:9096",
                    "zone-a",
                    vec![200],
                    InstanceState::Active,
                    None,
                    false,
                    None,
                    HashMap::new(),
                );
                desc.add_ingester(
                    "ing-c",
                    "127.0.0.1:9097",
                    "zone-c",
                    vec![300],
                    InstanceState::Active,
                    None,
                    false,
                    None,
                    HashMap::new(),
                );
                Ok(CasOutcome::Update(Box::new(desc)))
            }),
        )
        .await
        .unwrap();

        let d_zones = Instant::now() + Duration::from_secs(2);
        while ring.zones().len() != 3 {
            assert!(Instant::now() < d_zones, "timed out waiting for 3 zones");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // Wait for Tick 2 (the first tick after zones.len() == 3), which sees the zone
        // topology change despite standard_interval remaining 1000ms and re-aligns the ticker
        // to zone-b's 333ms slot.
        let before_realign_tick = ingester.metrics.compactions_triggered.get() as u64;
        let d2 = Instant::now() + Duration::from_secs(2);
        while (ingester.metrics.compactions_triggered.get() as u64) <= before_realign_tick {
            assert!(Instant::now() < d2, "timed out waiting for realign tick");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let t_realign = Instant::now();
        let target_after_realign = ingester.metrics.compactions_triggered.get() as u64 + 1;

        // Tick 3 MUST fire at the re-aligned 333ms slot (~250ms after Tick 2, well under
        // 650ms), rather than remaining drifted at the unaligned 1000ms cadence.
        let d3 = Instant::now() + Duration::from_millis(650);
        while (ingester.metrics.compactions_triggered.get() as u64) < target_after_realign {
            assert!(
                Instant::now() < d3,
                "tick did not re-align to zone-staggered schedule (elapsed={:?})",
                t_realign.elapsed()
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let dt = t_realign.elapsed();
        assert!(
            dt < Duration::from_millis(650),
            "expected re-aligned tick delta < 650ms, got {dt:?}"
        );

        ctx.cancel();
        let _ = loop_handle.await;
        ring_svc.stop_async();
        let _ = ring_svc.await_terminated().await;
    }

    #[tokio::test]
    async fn f6_compact_non_owned_series_triggers_recompute_when_series_after_gt_series_before() {
        use crate::ingester::Ingester;
        use mimir_config::limits::LimitsConfig;
        use mimir_config::{
            BlocksStorageConfig, GoDuration, IngestStorageConfig, IngesterConfig, Overrides,
        };
        use mimir_proto::cortexpb::{LabelPair, Sample, TimeSeries, WriteRequest};
        use std::sync::Arc;

        let tmp = tempfile::tempdir().unwrap();
        let mut cfg = IngesterConfig::default();
        cfg.ring.instance_id = "ing-self".to_string();
        cfg.early_compaction_non_owned_series_enabled = true;
        cfg.early_compaction_non_owned_series_min_grace_period = GoDuration::ZERO;
        cfg.early_compaction_non_owned_series_max_grace_period = GoDuration::ZERO;
        let mut bcfg = BlocksStorageConfig::default();
        bcfg.tsdb.dir = tmp.path().to_string_lossy().into_owned();
        bcfg.tsdb.ship_interval = GoDuration::ZERO;

        let limits = LimitsConfig {
            early_head_compaction_owned_series_threshold: 1,
            ..Default::default()
        };
        let ing = Ingester::new(
            cfg,
            bcfg,
            IngestStorageConfig::default(),
            Overrides::new(limits),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        ing.set_state_for_test(mimir_common::services::State::Running);

        // Push 1000 series so selected-series compaction takes long enough for a concurrent push
        // to add more new series while compact_selected_series is running (making series_after > series_before).
        let now_ms = 1_700_000_000_000i64;
        let ts_vec: Vec<TimeSeries> = (0..1000)
            .map(|k| TimeSeries {
                labels: vec![
                    LabelPair {
                        name: bytes::Bytes::from_static(b"__name__"),
                        value: bytes::Bytes::from(format!("non_owned_{k}")),
                    },
                ],
                samples: vec![Sample {
                    timestamp_ms: now_ms,
                    value: k as f64,
                }],
                ..Default::default()
            })
            .collect();
        ing.push_grpc("user-f6", &WriteRequest {
            timeseries: ts_vec,
            ..Default::default()
        })
        .await
        .unwrap();

        // Mark 2 series as aged non-owned so compact_blocks_due_to_non_owned_series compacts them.
        let db = ing.get_tsdb("user-f6").unwrap();
        let mut non_owned = std::collections::HashSet::new();
        non_owned.insert(1);
        non_owned.insert(2);
        db.add_pending_non_owned_refs(&non_owned);
        db.backdate_pending_non_owned_refs(
            std::time::UNIX_EPOCH + std::time::Duration::from_secs(1),
        );

        // Hold compaction_mtx briefly via a spawned task? No: instead test both:
        // (1) when num_series is bumped concurrently during compact_blocks_due_to_non_owned_series,
        // and (2) directly verify that num_series() > series_before triggers the metric & recompute.
        let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let done_c = Arc::clone(&done);
        let db_push = Arc::clone(&db);
        let push_task = tokio::spawn(async move {
            let mut j = 0usize;
            while !done_c.load(std::sync::atomic::Ordering::Relaxed) {
                let mut app = db_push.db.appender();
                for _ in 0..10 {
                    let name = format!("concurrent_new_{j}");
                    j += 1;
                    app.append_float(None, &[("__name__", &name)], now_ms + 1000, 1.0)
                        .unwrap();
                }
                app.commit().unwrap();
                tokio::task::yield_now().await;
            }
        });

        ing.compact_blocks_due_to_non_owned_series(Duration::ZERO)
            .await;
        done.store(true, std::sync::atomic::Ordering::Relaxed);
        push_task.await.unwrap();

        assert_eq!(
            ing.metrics
                .early_compaction_non_owned_series_triggered
                .with_label_values(&["user-f6"])
                .get(),
            1.0,
            "!= series_before check must trigger even when concurrent pushes raise num_series above series_before"
        );
    }
}
