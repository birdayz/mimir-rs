mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, UNIX_EPOCH};

use mimir_config::Overrides;
use mimir_ingester::active_series::ActiveSeries;
use mimir_ingester::costattribution::Manager as CostAttributionManager;
use mimir_ingester::errors::IngesterError;
use mimir_ingester::user_tsdb::TsdbCloseCheckResult;
use mimir_ingester::Ingester;
use mimir_proto::cortexpb::{LabelPair, Sample, TimeSeries, WriteRequest};
use mimir_tsdb::db::Tsdb;
use mimir_tsdb::head::SeriesEntry;
use mimir_tsdb::labels::Labels;
use prometheus::Registry;

use common::{default_ingester_test_config, default_limits_test_config};

fn build_write_request(metric: &str, ts_ms: i64, val: f64) -> WriteRequest {
    WriteRequest {
        timeseries: vec![TimeSeries {
            labels: vec![
                LabelPair {
                    name: "__name__".to_string().into(),
                    value: metric.to_string().into(),
                },
                LabelPair {
                    name: "job".to_string().into(),
                    value: "lifecycle_test".to_string().into(),
                },
            ],
            samples: vec![Sample {
                value: val,
                timestamp_ms: ts_ms,
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

#[cfg(target_os = "linux")]
fn count_deleted_tsdb_mmaps(tsdb_root: &std::path::Path) -> usize {
    let root_str = tsdb_root.to_string_lossy();
    let maps = std::fs::read_to_string("/proc/self/maps").expect("read /proc/self/maps");
    maps.lines()
        .filter(|line| {
            line.contains("(deleted)")
                && (line.contains(root_str.as_ref())
                    || line.contains("chunks_head")
                    || line.contains("/chunks/")
                    || line.contains("/index"))
        })
        .count()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ephemeral_tenants_unload_breaks_arc_cycle_unmaps_mmaps_and_removes_dirs() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let tsdb_root = tmp.path().join("tsdb");
    let (cfg, mut blocks_cfg, ingest_cfg) = default_ingester_test_config(&tsdb_root);
    blocks_cfg.tsdb.close_idle_tsdb_when_shipping_disabled = true;
    blocks_cfg.tsdb.head_chunks_write_buffer_size_bytes = 4096;
    blocks_cfg.tsdb.stripe_size = 16;

    let limits = default_limits_test_config();
    let overrides = Overrides::new(limits);
    let reg = Registry::new();
    let cost_reg = Registry::new();
    let cost_mgr = CostAttributionManager::new(
        Duration::from_secs(60),
        overrides.clone(),
        Some(&reg),
        &cost_reg,
    )
    .expect("cost attribution manager");

    let ing = Ingester::new(
        cfg,
        blocks_cfg,
        ingest_cfg,
        overrides,
        None,
        None,
        None,
        None,
        None,
        Some(&reg),
    )
    .expect("create ingester");
    ing.set_cost_attribution_manager(Some(Arc::clone(&cost_mgr)));

    let two_hours_ms = 2 * 3600 * 1000i64;
    let mut weak_users: Vec<(String, Weak<mimir_ingester::user_tsdb::UserTsdb>, Weak<Tsdb>)> =
        Vec::with_capacity(100);

    for i in 0..100 {
        let user_id = format!("ephemeral-{i:03}");
        // Populate cost_attribution config_cache for this tenant.
        let _ = cost_mgr.active_series_tracker(&user_id);
        assert!(cost_mgr.debug_has_cached_config(&user_id));

        let user_db = ing.get_or_create_tsdb(&user_id).expect("create tsdb");

        // Write 130 samples across chunk boundaries so chunks_head files are created and mmapped,
        // then advance into a second 2h block window and compact into a ULID block + empty head.
        let pairs = [("__name__", "ephemeral_metric"), ("tenant_idx", "0")];
        {
            let head = user_db.db.head();
            let mut series_ref = None;
            for s in 0..130i64 {
                let mut app = head.appender();
                let id = app
                    .append_float(series_ref, &pairs, 1_000 + s * 1_000, s as f64)
                    .expect("append");
                series_ref = Some(id);
                app.commit().expect("commit");
            }
            head.mmap_head_chunks().expect("mmap head chunks");

            // Advance past 1.5x chunk_range_ms (3h) so compact() persists a ULID block for [0, 2h).
            let mut app = head.appender();
            app.append_float(series_ref, &pairs, two_hours_ms * 2 + 5_000, 999.0)
                .expect("append second window");
            app.commit().expect("commit second window");
        }

        let compacted = user_db.db.compact().expect("compact window 1");
        assert!(
            !compacted.is_empty(),
            "tenant {user_id} should compact at least one ULID block"
        );
        // Verify we have a ULID block on disk before idle-close.
        assert!(!user_db.db.blocks().is_empty());
        // Compact remaining head data so head.num_series() == 0 and idle close can proceed.
        user_db.db.compact_head_all().expect("compact head all");
        user_db
            .db
            .truncate(two_hours_ms * 2 + 100_000)
            .expect("truncate head");
        assert_eq!(user_db.db.head().num_series(), 0);

        // Backdate last_update so the tenant is immediately idle.
        user_db.set_last_update(UNIX_EPOCH + Duration::from_secs(1));

        let weak_user = Arc::downgrade(&user_db);
        let weak_db = Arc::downgrade(&user_db.db);
        drop(user_db);

        let res = ing
            .close_and_delete_user_tsdb_if_idle(&user_id, Duration::from_secs(10))
            .await;
        assert_eq!(
            res,
            TsdbCloseCheckResult::IdleClosed,
            "tenant {user_id} should idle-close cleanly"
        );
        weak_users.push((user_id, weak_user, weak_db));
    }

    for (user_id, weak_user, weak_db) in &weak_users {
        assert!(
            weak_user.upgrade().is_none(),
            "UserTsdb Arc leaked for {user_id}"
        );
        assert!(
            weak_db.upgrade().is_none(),
            "Tsdb Arc cycle leaked for {user_id}"
        );
        assert!(
            !tsdb_root.join(user_id).exists(),
            "TSDB directory still exists on disk for {user_id}"
        );
        assert!(
            !cost_mgr.debug_has_cached_config(user_id),
            "costattribution config_cache still holds {user_id}"
        );
    }

    assert!(ing.tsdbs.is_empty());
    assert_eq!(ing.metrics.mem_users.get(), 0.0);

    #[cfg(target_os = "linux")]
    {
        let deleted_mmaps = count_deleted_tsdb_mmaps(&tsdb_root);
        assert_eq!(
            deleted_mmaps, 0,
            "expected 0 (deleted) TSDB VMAs in /proc/self/maps, found {deleted_mmaps}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_push_vs_idle_close_never_unlinks_live_wal() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let tsdb_root = tmp.path().join("tsdb-race");
    let (cfg, mut blocks_cfg, ingest_cfg) = default_ingester_test_config(&tsdb_root);
    blocks_cfg.tsdb.close_idle_tsdb_when_shipping_disabled = true;
    blocks_cfg.tsdb.stripe_size = 16;

    let ing = Arc::new(
        Ingester::new(
            cfg,
            blocks_cfg,
            ingest_cfg,
            Overrides::new(default_limits_test_config()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .expect("create ingester"),
    );

    let tenant = "tenant-race";
    let stop = Arc::new(AtomicBool::new(false));
    let mut push_tasks = Vec::new();
    for worker_id in 0..3 {
        let ing_c = Arc::clone(&ing);
        let stop_c = Arc::clone(&stop);
        push_tasks.push(tokio::spawn(async move {
            let metric = format!("race_metric_{worker_id}");
            let mut ts_ms = 10_000i64;
            while !stop_c.load(Ordering::Relaxed) {
                ts_ms += 100;
                let req = build_write_request(&metric, ts_ms, ts_ms as f64);
                match ing_c.push_write_request_sync(tenant, &req) {
                    Ok(()) => {}
                    Err(err) => {
                        let is_unavail = match &err {
                            IngesterError::TsdbUnavailable { .. } => true,
                            IngesterError::WrappedUser { inner, .. } => {
                                matches!(inner.as_ref(), IngesterError::TsdbUnavailable { .. })
                            }
                            _ => false,
                        };
                        assert!(is_unavail, "unexpected push error during close race: {err:?}");
                    }
                }
                tokio::task::yield_now().await;
            }
        }));
    }

    let closer_ing = Arc::clone(&ing);
    let closer_stop = Arc::clone(&stop);
    let closer = tokio::spawn(async move {
        let mut closed_count = 0usize;
        for round in 0..40 {
            if let Some(db) = closer_ing.get_tsdb(tenant) {
                // Alternate between deletion-mark close (closes even with series in head)
                // and normal idle close after compacting head.
                if round % 2 == 0 {
                    db.deletion_mark_found.store(true, Ordering::Release);
                } else {
                    db.set_last_update(UNIX_EPOCH + Duration::from_secs(1));
                }
                drop(db);
            }
            let res = closer_ing
                .close_and_delete_user_tsdb_if_idle(tenant, Duration::from_millis(1))
                .await;
            if matches!(
                res,
                TsdbCloseCheckResult::IdleClosed | TsdbCloseCheckResult::TenantMarkedForDeletion
            ) {
                closed_count += 1;
            }
            assert_ne!(
                res,
                TsdbCloseCheckResult::DataRemovalFailed,
                "directory removal must never fail during race"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        closer_stop.store(true, Ordering::Release);
        closed_count
    });

    let closed_count = closer.await.expect("closer task");
    for t in push_tasks {
        t.await.expect("push task");
    }
    assert!(
        closed_count >= 1,
        "expected closer to exercise at least 1 full close+unlink during concurrent pushes"
    );

    // Ensure the tenant remains cleanly usable after the race finishes (no half-deleted WAL dir).
    if let Some(existing) = ing.get_tsdb(tenant) {
        existing
            .deletion_mark_found
            .store(false, Ordering::Release);
        assert!(
            tsdb_root.join(tenant).join("wal").exists(),
            "live TSDB must never have its wal directory unlinked"
        );
    }
    let post_req = build_write_request("race_metric", 9_999_999_999, 42.0);
    ing.push_write_request_sync(tenant, &post_req)
        .expect("post-race push must succeed cleanly");
    ing.close_all_tsdbs().await;
}

#[test]
fn nanos_slab_high_ref_id_50m_lock_free_and_reclaims_pages() {
    let active = Arc::new(ActiveSeries::new(Vec::new(), Duration::from_secs(60)));
    let high_ref = 50_000_000u64; // > old 33,554,432 slot cap
    let pairs = [("__name__", "high_ref_metric"), ("job", "slab")];
    let labels = Labels::from_sorted_slice(&pairs);
    let entry = Arc::new(SeriesEntry::new_for_test(high_ref, labels.hash(), labels));
    assert_eq!(entry.active_cache(), 0);

    let t0 = UNIX_EPOCH + Duration::from_secs(1_000);
    active.update_series_with_entry(Some(&entry), &pairs, entry.hash, high_ref, t0, -1, false);
    assert_ne!(
        entry.active_cache(),
        0,
        "ref_id = 50_000_000 must populate lock-free active_cache"
    );
    assert_eq!(active.active(), (1, 0, 0, 0));
    assert_eq!(active.debug_slab_stats(), (1, 1, 0));

    // Exercise concurrent lock-free readers + fast-path updates while another ref on a
    // second high page is created and purged.
    let active_r = Arc::clone(&active);
    let entry_r = Arc::clone(&entry);
    let reader = std::thread::spawn(move || {
        let t_fast = t0 + Duration::from_secs(10);
        for step in 0..500u64 {
            let ts = t_fast + Duration::from_millis(step);
            active_r.update_series_with_entry(
                Some(&entry_r),
                &pairs,
                entry_r.hash,
                high_ref,
                ts,
                -1,
                false,
            );
            assert!(active_r.is_ref_active_fast(high_ref, i64::MIN));
        }
    });

    let high_ref_2 = 120_000_000u64;
    let p2 = [("__name__", "high_ref_metric_2")];
    let t1 = t0 + Duration::from_secs(20);
    active.update_series(&p2, 2, high_ref_2, t1, -1, false);
    reader.join().expect("reader thread");

    assert_eq!(active.debug_slab_stats(), (2, 2, 0));

    // Purge past t1 + 61s -> both pages empty -> both physical pages reclaimed.
    active.purge(t1 + Duration::from_secs(61));
    assert_eq!(active.active(), (0, 0, 0, 0));
    assert!(!active.is_ref_active_fast(high_ref, i64::MIN));
    assert!(!active.is_ref_active_fast(high_ref_2, i64::MIN));
    assert_eq!(active.debug_slab_stats(), (2, 0, 2));

    // Re-activating high_ref brings its page back to resident cleanly.
    let t2 = t1 + Duration::from_secs(120);
    active.update_series_with_entry(Some(&entry), &pairs, entry.hash, high_ref, t2, -1, false);
    assert_eq!(active.active(), (1, 0, 0, 0));
    assert!(active.is_ref_active_fast(high_ref, i64::MIN));
    assert_eq!(active.debug_slab_stats(), (2, 1, 1));
}
