//! TSDB storage config helpers, per-tenant TSDB metrics aggregator, users scanner,
//! and block ID hashing utilities.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/storage/tsdb/config.go
//! - github.com/grafana/mimir/pkg/storage/tsdb/tsdb_metrics.go
//! - github.com/grafana/mimir/pkg/storage/tsdb/users_scanner.go
//! - github.com/grafana/mimir/pkg/storage/tsdb/util.go
//! - github.com/grafana/dskit/metrics/tenant_registries.go

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use mimir_config::GoDuration;
use mimir_proto::sharding::{hash_add_byte_32, hash_new_32};
use parking_lot::Mutex;
use prometheus::core::{Collector, Desc};
use prometheus::proto::{
    Bucket, Counter as ProtoCounter, Gauge as ProtoGauge, Histogram as ProtoHistogram, LabelPair,
    Metric, MetricFamily, MetricType, Quantile, Summary as ProtoSummary,
};
use prometheus::Registry;
use tracing::warn;
use ulid::Ulid;

use crate::bucket::{
    tenant_deletion_mark_exists, BucketClient, BucketError, MIMIR_INTERNALS_PREFIX,
};

/// `DurationList` helper matching `tsdb.DurationList` in `pkg/storage/tsdb/config.go`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DurationList(pub Vec<GoDuration>);

impl DurationList {
    pub fn parse_csv(s: &str) -> Result<Self, String> {
        let mut out = Vec::new();
        for part in s.split(',') {
            let trimmed = part.trim();
            if trimmed.is_empty() {
                continue;
            }
            let d = trimmed
                .parse::<GoDuration>()
                .map_err(|e| format!("invalid duration {trimmed:?}: {e}"))?;
            out.push(d);
        }
        Ok(Self(out))
    }

    pub fn to_milliseconds(&self) -> Vec<i64> {
        self.0
            .iter()
            .map(|d| d.as_nanos() / 1_000_000)
            .collect()
    }
}

/// Returns a 32-bit FNV-1 hash of a block ULID for ring-based sharding.
/// Ported from `HashBlockID` in `pkg/storage/tsdb/util.go`.
pub fn hash_block_id(id: Ulid) -> u32 {
    let mut h = hash_new_32();
    for b in id.to_bytes() {
        h = hash_add_byte_32(h, b);
    }
    h
}

/// Ownership predicate returning `true` for every user (`tsdb.AllUsers` in `users_scanner.go`).
pub fn all_users(_user_id: &str) -> Result<bool, String> {
    Ok(true)
}

/// Returns whether `name` is reserved for Mimir internal cluster state.
pub fn is_user_id_reserved(name: &str) -> bool {
    name == MIMIR_INTERNALS_PREFIX
}

/// Lists all non-reserved user IDs found at the root of `bucket`.
/// Ported from `ListUsers` in `pkg/storage/tsdb/users_scanner.go`.
pub async fn list_users(bucket: &BucketClient) -> Result<Vec<String>, BucketError> {
    let entries = bucket.iter_dir("").await?;
    let mut users = Vec::new();
    for entry in entries {
        let user_id = entry.trim_end_matches('/');
        if user_id.is_empty() || is_user_id_reserved(user_id) {
            continue;
        }
        users.push(user_id.to_string());
    }
    Ok(users)
}

pub type IsOwnedUserFn = Arc<dyn Fn(&str) -> Result<bool, String> + Send + Sync>;

/// Scans object storage for active and deletion-marked tenants owned by this instance.
/// Ported from `UsersScanner` in `pkg/storage/tsdb/users_scanner.go`.
pub struct UsersScanner {
    bucket: BucketClient,
    is_owned: IsOwnedUserFn,
}

impl UsersScanner {
    pub fn new<F>(bucket: BucketClient, is_owned: F) -> Self
    where
        F: Fn(&str) -> Result<bool, String> + Send + Sync + 'static,
    {
        Self {
            bucket,
            is_owned: Arc::new(is_owned),
        }
    }

    /// Scans the bucket and returns `(active_users, users_marked_for_deletion)`.
    /// If either `is_owned` or `tenant_deletion_mark_exists` fails for a user, the error is
    /// logged as a warning and the user is kept in `active_users` (matching Go `UsersScanner`).
    pub async fn scan_users(&self) -> Result<(Vec<String>, Vec<String>), BucketError> {
        let all = list_users(&self.bucket).await?;
        let mut users = Vec::with_capacity(all.len());
        let mut marked_for_deletion = Vec::new();

        for user_id in all {
            match (self.is_owned)(&user_id) {
                Ok(true) => {}
                Ok(false) => continue,
                Err(err) => {
                    warn!(user = %user_id, err = %err, "unable to check if user is owned by this shard");
                }
            }

            match tenant_deletion_mark_exists(&self.bucket, &user_id).await {
                Ok(true) => {
                    marked_for_deletion.push(user_id);
                    continue;
                }
                Ok(false) => {}
                Err(err) => {
                    warn!(user = %user_id, err = %err, "unable to check if user is marked for deletion");
                }
            }

            users.push(user_id);
        }

        Ok((users, marked_for_deletion))
    }
}

// ---------------------------------------------------------------------------
// TenantRegistries & TSDBMetrics
// ---------------------------------------------------------------------------

struct TenantRegistryEntry {
    tenant: String,
    reg: Arc<Registry>,
}

/// Manages per-tenant Prometheus registries and archives counter/histogram/summary values
/// on soft removal to prevent counter resets.
/// Ported from `github.com/grafana/dskit/metrics/tenant_registries.go`.
struct TenantRegistries {
    regs: Mutex<Vec<TenantRegistryEntry>>,
    archived: Mutex<HashMap<String, MetricFamily>>,
}

impl TenantRegistries {
    fn new() -> Self {
        Self {
            regs: Mutex::new(Vec::new()),
            archived: Mutex::new(HashMap::new()),
        }
    }

    fn get_registry_for_tenant(&self, tenant: &str) -> Option<Arc<Registry>> {
        self.regs
            .lock()
            .iter()
            .find(|e| e.tenant == tenant)
            .map(|e| Arc::clone(&e.reg))
    }

    fn add_tenant_registry(&self, tenant: &str, reg: Arc<Registry>) {
        let mut regs = self.regs.lock();
        let mut idx = 0;
        while idx < regs.len() {
            if regs[idx].tenant == tenant {
                let removed = regs.remove(idx);
                self.archive_tenant_registry(&removed);
            } else {
                idx += 1;
            }
        }
        regs.push(TenantRegistryEntry {
            tenant: tenant.to_string(),
            reg,
        });
    }

    fn remove_tenant_registry(&self, tenant: &str, hard: bool) {
        let mut regs = self.regs.lock();
        let mut idx = 0;
        while idx < regs.len() {
            if regs[idx].tenant == tenant {
                let removed = regs.remove(idx);
                if !hard {
                    self.archive_tenant_registry(&removed);
                }
            } else {
                idx += 1;
            }
        }
    }

    fn archive_tenant_registry(&self, entry: &TenantRegistryEntry) {
        let families = entry.reg.gather();
        let mut archived = self.archived.lock();
        for mf in families {
            match mf.get_field_type() {
                MetricType::COUNTER | MetricType::SUMMARY | MetricType::HISTOGRAM => {}
                _ => continue,
            }
            let name = mf.get_name().to_string();
            if name.is_empty() {
                continue;
            }
            archived
                .entry(name)
                .and_modify(|existing| {
                    merge_archived_metric_family(existing, &mf);
                })
                .or_insert(mf);
        }
    }

    fn build_metric_families_per_tenant(&self) -> Vec<(String, HashMap<String, MetricFamily>)> {
        let snapshot: Vec<(String, Arc<Registry>)> = {
            let regs = self.regs.lock();
            regs.iter()
                .map(|e| (e.tenant.clone(), Arc::clone(&e.reg)))
                .collect()
        };
        let archived_map = self.archived.lock().clone();

        let mut out = Vec::with_capacity(snapshot.len() + if archived_map.is_empty() { 0 } else { 1 });
        if !archived_map.is_empty() {
            out.push((String::new(), archived_map));
        }
        for (tenant, reg) in snapshot {
            let mut map = HashMap::new();
            for mf in reg.gather() {
                let name = mf.get_name().to_string();
                if !name.is_empty() {
                    map.insert(name, mf);
                }
            }
            out.push((tenant, map));
        }
        out
    }
}

fn metric_label_key(m: &Metric) -> Vec<(String, String)> {
    let mut kvs: Vec<(String, String)> = m
        .get_label()
        .iter()
        .map(|lp| (lp.get_name().to_string(), lp.get_value().to_string()))
        .collect();
    kvs.sort();
    kvs
}

fn merge_archived_metric_family(existing: &mut MetricFamily, incoming: &MetricFamily) {
    let ty = existing.get_field_type();
    if ty != incoming.get_field_type() {
        return;
    }
    for m_in in incoming.get_metric() {
        let key_in = metric_label_key(m_in);
        if let Some(m_ex) = existing
            .mut_metric()
            .iter_mut()
            .find(|m| metric_label_key(m) == key_in)
        {
            match ty {
                MetricType::COUNTER => {
                    let sum = m_ex.get_counter().get_value() + m_in.get_counter().get_value();
                    m_ex.mut_counter().set_value(sum);
                }
                MetricType::HISTOGRAM => {
                    let h_in = m_in.get_histogram();
                    let h_ex = m_ex.mut_histogram();
                    h_ex.set_sample_count(h_ex.get_sample_count() + h_in.get_sample_count());
                    h_ex.set_sample_sum(h_ex.get_sample_sum() + h_in.get_sample_sum());
                    let mut bucket_map: BTreeMap<u64, (f64, u64)> = BTreeMap::new();
                    for b in h_ex.get_bucket() {
                        bucket_map.insert(
                            b.get_upper_bound().to_bits(),
                            (b.get_upper_bound(), b.get_cumulative_count()),
                        );
                    }
                    for b in h_in.get_bucket() {
                        bucket_map
                            .entry(b.get_upper_bound().to_bits())
                            .and_modify(|e| e.1 += b.get_cumulative_count())
                            .or_insert((b.get_upper_bound(), b.get_cumulative_count()));
                    }
                    let mut buckets: Vec<Bucket> = bucket_map
                        .into_values()
                        .map(|(ub, cnt)| {
                            let mut b = Bucket::default();
                            b.set_upper_bound(ub);
                            b.set_cumulative_count(cnt);
                            b
                        })
                        .collect();
                    buckets.sort_by(|a, b| a.get_upper_bound().total_cmp(&b.get_upper_bound()));
                    h_ex.set_bucket(buckets.into());
                }
                MetricType::SUMMARY => {
                    let s_in = m_in.get_summary();
                    let s_ex = m_ex.mut_summary();
                    s_ex.set_sample_count(s_ex.get_sample_count() + s_in.get_sample_count());
                    s_ex.set_sample_sum(s_ex.get_sample_sum() + s_in.get_sample_sum());
                    let mut q_map: BTreeMap<u64, (f64, f64)> = BTreeMap::new();
                    for q in s_ex.get_quantile() {
                        q_map.insert(q.get_quantile().to_bits(), (q.get_quantile(), q.get_value()));
                    }
                    for q in s_in.get_quantile() {
                        q_map
                            .entry(q.get_quantile().to_bits())
                            .and_modify(|e| e.1 += q.get_value())
                            .or_insert((q.get_quantile(), q.get_value()));
                    }
                    let mut qs: Vec<Quantile> = q_map
                        .into_values()
                        .map(|(qv, val)| {
                            let mut q = Quantile::default();
                            q.set_quantile(qv);
                            q.set_value(val);
                            q
                        })
                        .collect();
                    qs.sort_by(|a, b| a.get_quantile().total_cmp(&b.get_quantile()));
                    s_ex.set_quantile(qs.into());
                }
                _ => {}
            }
        } else {
            existing.mut_metric().push(m_in.clone());
        }
    }
}

/// Per-tenant TSDB metrics aggregator matching `tsdb.TSDBMetrics` in `pkg/storage/tsdb/tsdb_metrics.go`.
pub struct TSDBMetrics {
    regs: TenantRegistries,
    prefix: String,
    descs: Vec<Desc>,
}

impl TSDBMetrics {
    pub fn new(prefix: impl Into<String>, registry: Option<&Registry>) -> Arc<Self> {
        let prefix = prefix.into();
        let descs = Self::build_descs(&prefix);
        let m = Arc::new(Self {
            regs: TenantRegistries::new(),
            prefix,
            descs,
        });
        if let Some(reg) = registry {
            let _ = reg.register(Box::new(TSDBMetricsCollector(Arc::clone(&m))));
        }
        m
    }

    pub fn registry_for_tenant(&self, user_id: &str) -> Option<Arc<Registry>> {
        self.regs.get_registry_for_tenant(user_id)
    }

    pub fn set_registry_for_tenant(&self, user_id: &str, registry: Arc<Registry>) {
        self.regs.add_tenant_registry(user_id, registry);
    }

    pub fn remove_registry_for_tenant(&self, user_id: &str) {
        self.regs.remove_tenant_registry(user_id, false);
    }

    pub fn force_remove_registry_for_tenant(&self, user_id: &str) {
        self.regs.remove_tenant_registry(user_id, true);
    }

    fn fq(&self, name: &str) -> String {
        format!("{}{}", self.prefix, name)
    }

    fn build_descs(prefix: &str) -> Vec<Desc> {
        let mk = |name: &str, help: &str, labels: &[&str]| {
            Desc::new(
                format!("{prefix}{name}"),
                help.to_string(),
                labels.iter().map(|s| (*s).to_string()).collect(),
                HashMap::new(),
            )
            .expect("valid desc")
        };
        vec![
            mk("tsdb_compactions_total", "Total number of TSDB compactions that were executed.", &[]),
            mk("tsdb_compaction_duration_seconds", "Duration of TSDB compaction runs.", &[]),
            mk("tsdb_wal_fsync_duration_seconds", "Duration of TSDB WAL fsync.", &[]),
            mk("tsdb_wal_page_flushes_total", "Total number of TSDB WAL page flushes.", &[]),
            mk("tsdb_wal_completed_pages_total", "Total number of TSDB WAL completed pages.", &[]),
            mk("tsdb_wal_truncations_failed_total", "Total number of TSDB WAL truncations that failed.", &[]),
            mk("tsdb_wal_truncations_total", "Total number of TSDB  WAL truncations attempted.", &[]),
            mk("tsdb_wal_truncate_duration_seconds", "Duration of TSDB WAL truncation.", &[]),
            mk("tsdb_wal_corruptions_total", "Total number of TSDB WAL corruptions.", &[]),
            mk("tsdb_wal_writes_failed_total", "Total number of TSDB WAL writes that failed.", &[]),
            mk("tsdb_head_truncations_failed_total", "Total number of TSDB head truncations that failed.", &[]),
            mk("tsdb_head_truncations_total", "Total number of TSDB head truncations attempted.", &[]),
            mk("tsdb_head_gc_duration_seconds", "Runtime of garbage collection in the TSDB head.", &[]),
            mk("tsdb_head_active_appenders", "Number of currently active TSDB appender transactions.", &[]),
            mk("tsdb_head_series_not_found_total", "Total number of TSDB requests for series that were not found.", &[]),
            mk("tsdb_head_chunks", "Total number of chunks in the TSDB head block.", &[]),
            mk("tsdb_head_chunks_created_total", "Total number of series created in the TSDB head.", &["user"]),
            mk("tsdb_head_chunks_removed_total", "Total number of series removed in the TSDB head.", &["user"]),
            mk("tsdb_mmap_chunk_corruptions_total", "Total number of memory-mapped TSDB chunk corruptions.", &[]),
            mk("tsdb_mmap_chunk_write_queue_operations_total", "Total number of memory-mapped TSDB chunk operations.", &["operation"]),
            mk("tsdb_mmap_chunks_total", "Total number of chunks that were memory-mapped.", &[]),
            mk("tsdb_head_chunks_max_mmapped", "Experimental. Maximum, across all per-tenant TSDBs, of the maximum number of head chunks memory-mapped for any individual series during the last memory-mapping pass.", &[]),
            mk("tsdb_sample_out_of_order_delta_seconds", "Delta in seconds by which a sample is considered out-of-order.", &[]),
            mk("tsdb_blocks_loaded", "Number of currently loaded data blocks", &[]),
            mk("tsdb_reloads_total", "Number of times the database reloaded block data from disk.", &[]),
            mk("tsdb_reloads_failures_total", "Number of times the database failed to reloadBlocks block data from disk.", &[]),
            mk("tsdb_symbol_table_size_bytes", "Size of symbol table in memory for loaded blocks", &["user"]),
            mk("tsdb_storage_blocks_bytes", "The number of bytes that are currently used for local storage by all blocks.", &["user"]),
            mk("tsdb_time_retentions_total", "The number of times that blocks were deleted because the maximum time limit was exceeded.", &[]),
            mk("tsdb_checkpoint_deletions_failed_total", "Total number of TSDB checkpoint deletions that failed.", &[]),
            mk("tsdb_checkpoint_deletions_total", "Total number of TSDB checkpoint deletions attempted.", &[]),
            mk("tsdb_checkpoint_creations_failed_total", "Total number of TSDB checkpoint creations that failed.", &[]),
            mk("tsdb_checkpoint_creations_total", "Total number of TSDB checkpoint creations attempted.", &[]),
            mk("tsdb_exemplar_exemplars_appended_total", "Total number of TSDB exemplars appended.", &["user"]),
            mk("tsdb_exemplar_exemplars_in_storage", "Number of TSDB exemplars currently in storage.", &[]),
            mk("tsdb_exemplar_series_with_exemplars_in_storage", "Number of TSDB series with exemplars currently in storage.", &["user"]),
            mk("tsdb_exemplar_last_exemplars_timestamp_seconds", "The timestamp of the oldest exemplar stored in circular storage. Useful to check for what time range the current exemplar buffer limit allows. This usually means the last timestamp for all exemplars for a typical setup. This is not true though if one of the series timestamp is in future compared to rest series.", &["user"]),
            mk("tsdb_exemplar_out_of_order_exemplars_total", "Total number of out-of-order exemplar ingestion failed attempts.", &[]),
            mk("tsdb_out_of_order_samples_appended_total", "Total number of out-of-order samples appended.", &["user"]),
            mk("memory_series", "The current number of series in memory.", &[]),
            mk("memory_series_created_total", "The total number of series that were created per user.", &["user"]),
            mk("memory_series_removed_total", "The total number of series that were removed per user.", &["user"]),
            mk("tsdb_wal_replay_unknown_refs_total", "Total number of unknown series references encountered during WAL replay.", &["user", "type"]),
            mk("tsdb_wbl_replay_unknown_refs_total", "Total number of unknown series references encountered during WBL replay.", &["user", "type"]),
            mk("tsdb_head_statistics_last_update_timestamp_seconds", "Timestamp of the last update of head statistics.", &["user"]),
            mk("tsdb_head_statistics_time_to_update_seconds", "Time spent updating head statistics.", &["user"]),
        ]
    }

    pub fn collect_families(&self) -> Vec<MetricFamily> {
        let data = self.regs.build_metric_families_per_tenant();
        let mut out = Vec::new();

        macro_rules! push_sum_counter {
            ($out_name:expr, $help:expr, $src_name:expr) => {{
                let val = sum_of_counters(&data, $src_name);
                out.push(single_value_family(
                    &self.fq($out_name),
                    $help,
                    MetricType::COUNTER,
                    val,
                    &[],
                ));
            }};
        }
        macro_rules! push_sum_gauge {
            ($out_name:expr, $help:expr, $src_name:expr) => {{
                let val = sum_of_gauges(&data, $src_name);
                out.push(single_value_family(
                    &self.fq($out_name),
                    $help,
                    MetricType::GAUGE,
                    val,
                    &[],
                ));
            }};
        }
        macro_rules! push_max_gauge {
            ($out_name:expr, $help:expr, $src_name:expr) => {{
                let val = max_of_gauges(&data, $src_name);
                out.push(single_value_family(
                    &self.fq($out_name),
                    $help,
                    MetricType::GAUGE,
                    val,
                    &[],
                ));
            }};
        }
        macro_rules! push_sum_counter_per_tenant {
            ($out_name:expr, $help:expr, $src_name:expr) => {{
                let metrics = per_tenant_single_values(&data, $src_name, MetricType::COUNTER, &[]);
                if !metrics.is_empty() {
                    out.push(family_with_metrics(
                        &self.fq($out_name),
                        $help,
                        MetricType::COUNTER,
                        metrics,
                    ));
                }
            }};
        }
        macro_rules! push_sum_gauge_per_tenant {
            ($out_name:expr, $help:expr, $src_name:expr) => {{
                let metrics = per_tenant_single_values(&data, $src_name, MetricType::GAUGE, &[]);
                if !metrics.is_empty() {
                    out.push(family_with_metrics(
                        &self.fq($out_name),
                        $help,
                        MetricType::GAUGE,
                        metrics,
                    ));
                }
            }};
        }
        macro_rules! push_max_gauge_per_tenant {
            ($out_name:expr, $help:expr, $src_name:expr) => {{
                let metrics = max_per_tenant_gauges(&data, $src_name);
                if !metrics.is_empty() {
                    out.push(family_with_metrics(
                        &self.fq($out_name),
                        $help,
                        MetricType::GAUGE,
                        metrics,
                    ));
                }
            }};
        }
        macro_rules! push_sum_counter_with_labels {
            ($out_name:expr, $help:expr, $src_name:expr, $labels:expr) => {{
                let metrics =
                    global_single_values_with_labels(&data, $src_name, MetricType::COUNTER, $labels);
                if !metrics.is_empty() {
                    out.push(family_with_metrics(
                        &self.fq($out_name),
                        $help,
                        MetricType::COUNTER,
                        metrics,
                    ));
                }
            }};
        }
        macro_rules! push_sum_counter_per_tenant_with_labels {
            ($out_name:expr, $help:expr, $src_name:expr, $labels:expr) => {{
                let metrics =
                    per_tenant_single_values(&data, $src_name, MetricType::COUNTER, $labels);
                if !metrics.is_empty() {
                    out.push(family_with_metrics(
                        &self.fq($out_name),
                        $help,
                        MetricType::COUNTER,
                        metrics,
                    ));
                }
            }};
        }

        push_sum_counter!(
            "tsdb_compactions_total",
            "Total number of TSDB compactions that were executed.",
            "prometheus_tsdb_compactions_total"
        );
        if let Some(mf) = sum_of_histograms(
            &data,
            &self.fq("tsdb_compaction_duration_seconds"),
            "Duration of TSDB compaction runs.",
            "prometheus_tsdb_compaction_duration_seconds",
        ) {
            out.push(mf);
        }
        if let Some(mf) = sum_of_summaries(
            &data,
            &self.fq("tsdb_wal_fsync_duration_seconds"),
            "Duration of TSDB WAL fsync.",
            "prometheus_tsdb_wal_fsync_duration_seconds",
        ) {
            out.push(mf);
        }
        push_sum_counter!(
            "tsdb_wal_page_flushes_total",
            "Total number of TSDB WAL page flushes.",
            "prometheus_tsdb_wal_page_flushes_total"
        );
        push_sum_counter!(
            "tsdb_wal_completed_pages_total",
            "Total number of TSDB WAL completed pages.",
            "prometheus_tsdb_wal_completed_pages_total"
        );
        push_sum_counter!(
            "tsdb_wal_truncations_failed_total",
            "Total number of TSDB WAL truncations that failed.",
            "prometheus_tsdb_wal_truncations_failed_total"
        );
        push_sum_counter!(
            "tsdb_wal_truncations_total",
            "Total number of TSDB  WAL truncations attempted.",
            "prometheus_tsdb_wal_truncations_total"
        );
        if let Some(mf) = sum_of_summaries(
            &data,
            &self.fq("tsdb_wal_truncate_duration_seconds"),
            "Duration of TSDB WAL truncation.",
            "prometheus_tsdb_wal_truncate_duration_seconds",
        ) {
            out.push(mf);
        }
        push_sum_counter!(
            "tsdb_wal_corruptions_total",
            "Total number of TSDB WAL corruptions.",
            "prometheus_tsdb_wal_corruptions_total"
        );
        push_sum_counter!(
            "tsdb_wal_writes_failed_total",
            "Total number of TSDB WAL writes that failed.",
            "prometheus_tsdb_wal_writes_failed_total"
        );
        push_sum_counter!(
            "tsdb_head_truncations_failed_total",
            "Total number of TSDB head truncations that failed.",
            "prometheus_tsdb_head_truncations_failed_total"
        );
        push_sum_counter!(
            "tsdb_head_truncations_total",
            "Total number of TSDB head truncations attempted.",
            "prometheus_tsdb_head_truncations_total"
        );
        if let Some(mf) = sum_of_summaries(
            &data,
            &self.fq("tsdb_head_gc_duration_seconds"),
            "Runtime of garbage collection in the TSDB head.",
            "prometheus_tsdb_head_gc_duration_seconds",
        ) {
            out.push(mf);
        }
        push_sum_gauge!(
            "tsdb_head_active_appenders",
            "Number of currently active TSDB appender transactions.",
            "prometheus_tsdb_head_active_appenders"
        );
        push_sum_counter!(
            "tsdb_head_series_not_found_total",
            "Total number of TSDB requests for series that were not found.",
            "prometheus_tsdb_head_series_not_found_total"
        );
        push_sum_gauge!(
            "tsdb_head_chunks",
            "Total number of chunks in the TSDB head block.",
            "prometheus_tsdb_head_chunks"
        );
        push_sum_counter_per_tenant!(
            "tsdb_head_chunks_created_total",
            "Total number of series created in the TSDB head.",
            "prometheus_tsdb_head_chunks_created_total"
        );
        push_sum_counter_per_tenant!(
            "tsdb_head_chunks_removed_total",
            "Total number of series removed in the TSDB head.",
            "prometheus_tsdb_head_chunks_removed_total"
        );
        push_sum_counter!(
            "tsdb_mmap_chunk_corruptions_total",
            "Total number of memory-mapped TSDB chunk corruptions.",
            "prometheus_tsdb_mmap_chunk_corruptions_total"
        );
        push_sum_counter_with_labels!(
            "tsdb_mmap_chunk_write_queue_operations_total",
            "Total number of memory-mapped TSDB chunk operations.",
            "prometheus_tsdb_chunk_write_queue_operations_total",
            &["operation"]
        );
        push_sum_counter!(
            "tsdb_mmap_chunks_total",
            "Total number of chunks that were memory-mapped.",
            "prometheus_tsdb_mmap_chunks_total"
        );
        push_max_gauge!(
            "tsdb_head_chunks_max_mmapped",
            "Experimental. Maximum, across all per-tenant TSDBs, of the maximum number of head chunks memory-mapped for any individual series during the last memory-mapping pass.",
            "prometheus_tsdb_head_chunks_max_mmapped"
        );
        if let Some(mf) = sum_of_histograms(
            &data,
            &self.fq("tsdb_sample_out_of_order_delta_seconds"),
            "Delta in seconds by which a sample is considered out-of-order.",
            "prometheus_tsdb_sample_ooo_delta",
        ) {
            out.push(mf);
        }
        push_sum_gauge!(
            "tsdb_blocks_loaded",
            "Number of currently loaded data blocks",
            "prometheus_tsdb_blocks_loaded"
        );
        push_sum_counter!(
            "tsdb_reloads_total",
            "Number of times the database reloaded block data from disk.",
            "prometheus_tsdb_reloads_total"
        );
        push_sum_counter!(
            "tsdb_reloads_failures_total",
            "Number of times the database failed to reloadBlocks block data from disk.",
            "prometheus_tsdb_reloads_failures_total"
        );
        push_sum_gauge_per_tenant!(
            "tsdb_symbol_table_size_bytes",
            "Size of symbol table in memory for loaded blocks",
            "prometheus_tsdb_symbol_table_size_bytes"
        );
        push_sum_gauge_per_tenant!(
            "tsdb_storage_blocks_bytes",
            "The number of bytes that are currently used for local storage by all blocks.",
            "prometheus_tsdb_storage_blocks_bytes"
        );
        push_sum_counter!(
            "tsdb_time_retentions_total",
            "The number of times that blocks were deleted because the maximum time limit was exceeded.",
            "prometheus_tsdb_time_retentions_total"
        );
        push_sum_counter!(
            "tsdb_checkpoint_deletions_failed_total",
            "Total number of TSDB checkpoint deletions that failed.",
            "prometheus_tsdb_checkpoint_deletions_failed_total"
        );
        push_sum_counter!(
            "tsdb_checkpoint_deletions_total",
            "Total number of TSDB checkpoint deletions attempted.",
            "prometheus_tsdb_checkpoint_deletions_total"
        );
        push_sum_counter!(
            "tsdb_checkpoint_creations_failed_total",
            "Total number of TSDB checkpoint creations that failed.",
            "prometheus_tsdb_checkpoint_creations_failed_total"
        );
        push_sum_counter!(
            "tsdb_checkpoint_creations_total",
            "Total number of TSDB checkpoint creations attempted.",
            "prometheus_tsdb_checkpoint_creations_total"
        );
        push_sum_counter_per_tenant!(
            "tsdb_exemplar_exemplars_appended_total",
            "Total number of TSDB exemplars appended.",
            "prometheus_tsdb_exemplar_exemplars_appended_total"
        );
        push_sum_gauge!(
            "tsdb_exemplar_exemplars_in_storage",
            "Number of TSDB exemplars currently in storage.",
            "prometheus_tsdb_exemplar_exemplars_in_storage"
        );
        push_sum_gauge_per_tenant!(
            "tsdb_exemplar_series_with_exemplars_in_storage",
            "Number of TSDB series with exemplars currently in storage.",
            "prometheus_tsdb_exemplar_series_with_exemplars_in_storage"
        );
        push_sum_gauge_per_tenant!(
            "tsdb_exemplar_last_exemplars_timestamp_seconds",
            "The timestamp of the oldest exemplar stored in circular storage. Useful to check for what time range the current exemplar buffer limit allows. This usually means the last timestamp for all exemplars for a typical setup. This is not true though if one of the series timestamp is in future compared to rest series.",
            "prometheus_tsdb_exemplar_last_exemplars_timestamp_seconds"
        );
        push_sum_counter!(
            "tsdb_exemplar_out_of_order_exemplars_total",
            "Total number of out-of-order exemplar ingestion failed attempts.",
            "prometheus_tsdb_exemplar_out_of_order_exemplars_total"
        );
        push_sum_counter_per_tenant!(
            "tsdb_out_of_order_samples_appended_total",
            "Total number of out-of-order samples appended.",
            "prometheus_tsdb_head_out_of_order_samples_appended_total"
        );
        push_sum_gauge!(
            "memory_series",
            "The current number of series in memory.",
            "prometheus_tsdb_head_series"
        );
        push_sum_counter_per_tenant!(
            "memory_series_created_total",
            "The total number of series that were created per user.",
            "prometheus_tsdb_head_series_created_total"
        );
        push_sum_counter_per_tenant!(
            "memory_series_removed_total",
            "The total number of series that were removed per user.",
            "prometheus_tsdb_head_series_removed_total"
        );
        push_sum_counter_per_tenant_with_labels!(
            "tsdb_wal_replay_unknown_refs_total",
            "Total number of unknown series references encountered during WAL replay.",
            "prometheus_tsdb_wal_replay_unknown_refs_total",
            &["type"]
        );
        push_sum_counter_per_tenant_with_labels!(
            "tsdb_wbl_replay_unknown_refs_total",
            "Total number of unknown series references encountered during WBL replay.",
            "prometheus_tsdb_wbl_replay_unknown_refs_total",
            &["type"]
        );
        push_max_gauge_per_tenant!(
            "tsdb_head_statistics_last_update_timestamp_seconds",
            "Timestamp of the last update of head statistics.",
            "prometheus_tsdb_head_statistics_last_update_timestamp_seconds"
        );
        push_max_gauge_per_tenant!(
            "tsdb_head_statistics_time_to_update_seconds",
            "Time spent updating head statistics.",
            "prometheus_tsdb_head_statistics_time_to_update_seconds"
        );

        out
    }
}

struct TSDBMetricsCollector(Arc<TSDBMetrics>);

impl Collector for TSDBMetricsCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.0.descs.iter().collect()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        self.0.collect_families()
    }
}

fn make_label_pair(name: &str, value: &str) -> LabelPair {
    let mut lp = LabelPair::default();
    lp.set_name(name.to_string());
    lp.set_value(value.to_string());
    lp
}

fn extract_value(m: &Metric, ty: MetricType) -> Option<f64> {
    match ty {
        MetricType::COUNTER => Some(m.get_counter().get_value()),
        MetricType::GAUGE => Some(m.get_gauge().get_value()),
        _ => None,
    }
}

fn sum_of_counters(
    data: &[(String, HashMap<String, MetricFamily>)],
    src_name: &str,
) -> f64 {
    let mut total = 0.0;
    for (_, map) in data {
        if let Some(mf) = map.get(src_name) {
            for m in mf.get_metric() {
                total += m.get_counter().get_value();
            }
        }
    }
    total
}

fn sum_of_gauges(
    data: &[(String, HashMap<String, MetricFamily>)],
    src_name: &str,
) -> f64 {
    let mut total = 0.0;
    for (_, map) in data {
        if let Some(mf) = map.get(src_name) {
            for m in mf.get_metric() {
                total += m.get_gauge().get_value();
            }
        }
    }
    total
}

fn max_of_gauges(
    data: &[(String, HashMap<String, MetricFamily>)],
    src_name: &str,
) -> f64 {
    let mut best = f64::NAN;
    for (_, map) in data {
        if let Some(mf) = map.get(src_name) {
            for m in mf.get_metric() {
                let v = m.get_gauge().get_value();
                if !v.is_nan() && (best.is_nan() || v > best) {
                    best = v;
                }
            }
        }
    }
    if best.is_nan() {
        0.0
    } else {
        best
    }
}

fn single_value_family(
    name: &str,
    help: &str,
    ty: MetricType,
    val: f64,
    labels: &[(&str, &str)],
) -> MetricFamily {
    let m = single_value_metric(ty, val, labels);
    family_with_metrics(name, help, ty, vec![m])
}

fn single_value_metric(ty: MetricType, val: f64, labels: &[(&str, &str)]) -> Metric {
    let mut m = Metric::default();
    let pairs: Vec<LabelPair> = labels
        .iter()
        .map(|&(k, v)| make_label_pair(k, v))
        .collect();
    m.set_label(pairs.into());
    match ty {
        MetricType::COUNTER => {
            let mut c = ProtoCounter::default();
            c.set_value(val);
            m.set_counter(c);
        }
        MetricType::GAUGE => {
            let mut g = ProtoGauge::default();
            g.set_value(val);
            m.set_gauge(g);
        }
        _ => {}
    }
    m
}

fn family_with_metrics(
    name: &str,
    help: &str,
    ty: MetricType,
    metrics: Vec<Metric>,
) -> MetricFamily {
    let mut mf = MetricFamily::default();
    mf.set_name(name.to_string());
    mf.set_help(help.to_string());
    mf.set_field_type(ty);
    mf.set_metric(metrics.into());
    mf
}

fn get_label_values(m: &Metric, label_names: &[&str]) -> Option<Vec<String>> {
    let mut out = Vec::with_capacity(label_names.len());
    for &ln in label_names {
        let lp = m.get_label().iter().find(|lp| lp.get_name() == ln)?;
        out.push(lp.get_value().to_string());
    }
    Some(out)
}

fn global_single_values_with_labels(
    data: &[(String, HashMap<String, MetricFamily>)],
    src_name: &str,
    ty: MetricType,
    label_names: &[&str],
) -> Vec<Metric> {
    let mut acc: BTreeMap<Vec<String>, f64> = BTreeMap::new();
    for (_, map) in data {
        if let Some(mf) = map.get(src_name) {
            for m in mf.get_metric() {
                if let Some(vals) = get_label_values(m, label_names) {
                    if let Some(v) = extract_value(m, ty) {
                        *acc.entry(vals).or_insert(0.0) += v;
                    }
                }
            }
        }
    }
    acc.into_iter()
        .map(|(vals, total)| {
            let pairs: Vec<(&str, &str)> = label_names
                .iter()
                .zip(vals.iter())
                .map(|(&k, v)| (k, v.as_str()))
                .collect();
            single_value_metric(ty, total, &pairs)
        })
        .collect()
}

fn per_tenant_single_values(
    data: &[(String, HashMap<String, MetricFamily>)],
    src_name: &str,
    ty: MetricType,
    extra_label_names: &[&str],
) -> Vec<Metric> {
    let mut out = Vec::new();
    for (tenant, map) in data {
        if tenant.is_empty() {
            continue;
        }
        let Some(mf) = map.get(src_name) else {
            continue;
        };
        let mut acc: BTreeMap<Vec<String>, f64> = BTreeMap::new();
        for m in mf.get_metric() {
            if let Some(vals) = get_label_values(m, extra_label_names) {
                if let Some(v) = extract_value(m, ty) {
                    *acc.entry(vals).or_insert(0.0) += v;
                }
            }
        }
        for (vals, total) in acc {
            let mut pairs: Vec<(&str, &str)> = Vec::with_capacity(1 + extra_label_names.len());
            pairs.push(("user", tenant.as_str()));
            for (&k, v) in extra_label_names.iter().zip(vals.iter()) {
                pairs.push((k, v.as_str()));
            }
            out.push(single_value_metric(ty, total, &pairs));
        }
    }
    out
}

fn max_per_tenant_gauges(
    data: &[(String, HashMap<String, MetricFamily>)],
    src_name: &str,
) -> Vec<Metric> {
    let mut out = Vec::new();
    for (tenant, map) in data {
        if tenant.is_empty() {
            continue;
        }
        let Some(mf) = map.get(src_name) else {
            continue;
        };
        let mut best = f64::NAN;
        for m in mf.get_metric() {
            let v = m.get_gauge().get_value();
            if !v.is_nan() && (best.is_nan() || v > best) {
                best = v;
            }
        }
        if !best.is_nan() {
            out.push(single_value_metric(
                MetricType::GAUGE,
                best,
                &[("user", tenant.as_str())],
            ));
        }
    }
    out
}

fn sum_of_histograms(
    data: &[(String, HashMap<String, MetricFamily>)],
    out_name: &str,
    help: &str,
    src_name: &str,
) -> Option<MetricFamily> {
    let mut count = 0u64;
    let mut sum = 0.0f64;
    let mut buckets: BTreeMap<u64, (f64, u64)> = BTreeMap::new();

    for (_, map) in data {
        if let Some(mf) = map.get(src_name) {
            for m in mf.get_metric() {
                let h = m.get_histogram();
                count += h.get_sample_count();
                sum += h.get_sample_sum();
                for b in h.get_bucket() {
                    buckets
                        .entry(b.get_upper_bound().to_bits())
                        .and_modify(|e| e.1 += b.get_cumulative_count())
                        .or_insert((b.get_upper_bound(), b.get_cumulative_count()));
                }
            }
        }
    }

    let mut proto_buckets: Vec<Bucket> = buckets
        .into_values()
        .map(|(ub, cnt)| {
            let mut b = Bucket::default();
            b.set_upper_bound(ub);
            b.set_cumulative_count(cnt);
            b
        })
        .collect();
    proto_buckets.sort_by(|a, b| a.get_upper_bound().total_cmp(&b.get_upper_bound()));

    let mut h = ProtoHistogram::default();
    h.set_sample_count(count);
    h.set_sample_sum(sum);
    h.set_bucket(proto_buckets.into());

    let mut m = Metric::default();
    m.set_histogram(h);
    Some(family_with_metrics(
        out_name,
        help,
        MetricType::HISTOGRAM,
        vec![m],
    ))
}

fn sum_of_summaries(
    data: &[(String, HashMap<String, MetricFamily>)],
    out_name: &str,
    help: &str,
    src_name: &str,
) -> Option<MetricFamily> {
    let mut count = 0u64;
    let mut sum = 0.0f64;
    let mut quantiles: BTreeMap<u64, (f64, f64)> = BTreeMap::new();

    for (_, map) in data {
        if let Some(mf) = map.get(src_name) {
            for m in mf.get_metric() {
                let s = m.get_summary();
                count += s.get_sample_count();
                sum += s.get_sample_sum();
                for q in s.get_quantile() {
                    quantiles
                        .entry(q.get_quantile().to_bits())
                        .and_modify(|e| e.1 += q.get_value())
                        .or_insert((q.get_quantile(), q.get_value()));
                }
            }
        }
    }

    let mut proto_qs: Vec<Quantile> = quantiles
        .into_values()
        .map(|(qv, val)| {
            let mut q = Quantile::default();
            q.set_quantile(qv);
            q.set_value(val);
            q
        })
        .collect();
    proto_qs.sort_by(|a, b| a.get_quantile().total_cmp(&b.get_quantile()));

    let mut s = ProtoSummary::default();
    s.set_sample_count(count);
    s.set_sample_sum(sum);
    s.set_quantile(proto_qs.into());

    let mut m = Metric::default();
    m.set_summary(s);
    Some(family_with_metrics(
        out_name,
        help,
        MetricType::SUMMARY,
        vec![m],
    ))
}
