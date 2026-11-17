use std::fs;
use std::path::Path;
use std::sync::Arc;

use mimir_tsdb::db::{Tsdb, TsdbOptions};
use mimir_tsdb::head::HeadOptions;
use mimir_tsdb::matcher::{LabelMatcher, MatchType};
use mimir_tsdb::postings_cache::{PostingsForMatchersCacheConfig, PostingsForMatchersCacheMetrics};
use mimir_tsdb::record::MetricType;
use mimir_tsdb::wlog::CompressionType;
use prometheus::Registry;

const TWO_HOURS_MS: i64 = 2 * 3600 * 1000;
const FOUR_HOURS_MS: i64 = 4 * 3600 * 1000;

#[cfg(target_os = "linux")]
#[derive(Clone, Copy, Debug)]
struct LinuxProcSnapshot {
    deleted_tsdb_mmaps: usize,
    dir_fds: usize,
    total_fds: usize,
    rss_anon_kb: u64,
    rss_file_kb: u64,
}

#[cfg(target_os = "linux")]
fn parse_status_kb(status: &str, key: &str) -> u64 {
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix(key) {
            let num = rest
                .split_whitespace()
                .next()
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);
            return num;
        }
    }
    0
}

#[cfg(target_os = "linux")]
fn inspect_linux_proc(tsdb_dir: &Path) -> LinuxProcSnapshot {
    let dir_str = tsdb_dir.to_string_lossy();
    let maps = fs::read_to_string("/proc/self/maps").expect("read /proc/self/maps");
    let deleted_tsdb_mmaps = maps
        .lines()
        .filter(|line| {
            line.contains("(deleted)")
                && (line.contains(dir_str.as_ref())
                    || line.contains("chunks_head")
                    || line.contains("/chunks/")
                    || line.contains("/index"))
        })
        .count();

    let mut dir_fds = 0usize;
    let mut total_fds = 0usize;
    if let Ok(entries) = fs::read_dir("/proc/self/fd") {
        for entry in entries.flatten() {
            total_fds += 1;
            if let Ok(target) = fs::read_link(entry.path()) {
                if target.starts_with(tsdb_dir) {
                    dir_fds += 1;
                }
            }
        }
    }

    let status = fs::read_to_string("/proc/self/status").expect("read /proc/self/status");
    let rss_anon_kb = parse_status_kb(&status, "RssAnon:");
    let rss_file_kb = parse_status_kb(&status, "RssFile:");

    LinuxProcSnapshot {
        deleted_tsdb_mmaps,
        dir_fds,
        total_fds,
        rss_anon_kb,
        rss_file_kb,
    }
}

fn count_files_in_dir(dir: &Path) -> usize {
    match fs::read_dir(dir) {
        Ok(rd) => rd.flatten().count(),
        Err(_) => 0,
    }
}

#[test]
fn multi_cycle_compaction_retention_and_churn_working_set_plateaus() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let reg = Arc::new(Registry::new());
    let head_cache_metrics = PostingsForMatchersCacheMetrics::new("test_head_", Some(&reg));
    let block_cache_metrics = PostingsForMatchersCacheMetrics::new("test_block_", Some(&reg));

    let opts = TsdbOptions {
        head: HeadOptions {
            chunk_range_ms: TWO_HOURS_MS,
            samples_per_chunk: 20,
            stripe_size: 256,
            ooo_time_window_ms: 3600 * 1000,
            ooo_cap_max: 4,
            wal_compression: CompressionType::Snappy,
            // 64 KiB WAL/WBL segments -> forces frequent segment rotations & checkpoints.
            wal_segment_size: 64 * 1024,
            wal_replay_concurrency: 2,
            // 32 KiB chunks_head segments -> forces frequent chunks_head rollovers & truncations.
            max_head_chunk_file_size: 32 * 1024,
            head_chunks_write_buffer_size: 4096,
            postings_cache: PostingsForMatchersCacheConfig {
                metrics: Some(head_cache_metrics),
                enable_invalidation: true,
                ..Default::default()
            },
            registry: Some(Arc::clone(&reg)),
            ..HeadOptions::default()
        },
        retention_duration_ms: FOUR_HOURS_MS,
        block_postings_cache: PostingsForMatchersCacheConfig {
            metrics: Some(block_cache_metrics),
            ..Default::default()
        },
        ..TsdbOptions::default()
    };

    let db = Tsdb::open(tmp.path(), opts).expect("open tsdb");

    const CYCLES: i64 = 10;
    const PERSISTENT_SERIES: usize = 100;
    const CHURN_SERIES_PER_CYCLE: usize = 1_500;
    const STEPS_PER_CYCLE: i64 = 30;
    const STEP_MS: i64 = (TWO_HOURS_MS - 120_000) / STEPS_PER_CYCLE;

    let mut snapshots = Vec::new();
    let mut symbol_counts = Vec::new();
    let mut block_counts = Vec::new();

    for cycle in 0..=CYCLES {
        let cycle_start = cycle * TWO_HOURS_MS;

        // Write 30 steps inside [cycle_start, cycle_start + 2h), creating persistent + churned
        // series, metadata, and OOO samples.
        for step in 0..STEPS_PER_CYCLE {
            let t = cycle_start + 30_000 + step * STEP_MS;
            let mut app = db.appender_with_capacity(PERSISTENT_SERIES + CHURN_SERIES_PER_CYCLE);

            for s in 0..PERSISTENT_SERIES {
                let name = format!("persistent_metric_{}", s % 10);
                let inst = format!("inst-{s:03}");
                let pairs = [("__name__", name.as_str()), ("instance", inst.as_str())];
                app.append_float(None, &pairs, t, (cycle * 100 + step) as f64)
                    .expect("append persistent");
                if step == 0 {
                    let _ = app.update_metadata(
                        None,
                        &pairs,
                        &name,
                        MetricType::Gauge,
                        "bytes",
                        "persistent metric help",
                    );
                }
                if step >= 5 && s % 5 == 0 {
                    // OOO sample inside OOO window -> hits OOO head, chunks_head spill, WBL.
                    let _ = app.append_float(None, &pairs, t - 25_000, step as f64 + 0.25);
                }
            }

            if cycle < CYCLES {
                for s in 0..CHURN_SERIES_PER_CYCLE {
                    let name = format!("churn_metric_{}", s % 8);
                    let pod = format!("pod-cycle{cycle:02}-{s:04}");
                    let container = format!("ctr-cycle{cycle:02}-{s:04}");
                    let pairs = [
                        ("__name__", name.as_str()),
                        ("container", container.as_str()),
                        ("pod", pod.as_str()),
                    ];
                    app.append_float(None, &pairs, t, s as f64)
                        .expect("append churn");
                    if step == 0 && s < 200 {
                        let help = format!("churn help cycle {cycle} {s}");
                        let _ = app.update_metadata(
                            None,
                            &pairs,
                            &name,
                            MetricType::Counter,
                            "1",
                            &help,
                        );
                    }
                    if step >= 6 && s % 25 == 0 {
                        let _ = app.append_float(None, &pairs, t - 20_000, s as f64 + 0.5);
                    }
                }
            }

            app.commit().expect("commit");
        }

        // Advance one extra sample into the middle of the next 2h window when needed so
        // `head.compactable()` triggers compaction of the just-completed 2h block.
        if cycle > 0 {
            let trigger_t = cycle_start + TWO_HOURS_MS / 2 + 60_000;
            let mut app = db.appender();
            app.append_float(
                None,
                &[("__name__", "persistent_metric_0"), ("instance", "inst-000")],
                trigger_t,
                1.0,
            )
            .expect("trigger append");
            app.commit().expect("trigger commit");

            let created = db.compact().expect("compact");
            assert!(
                !created.is_empty(),
                "cycle {cycle}: expected at least 1 compacted block"
            );

            // Exercise querier over Head + Blocks (populates postings caches & verifies data).
            let q = db.querier(cycle_start - TWO_HOURS_MS, trigger_t);
            let m = LabelMatcher::new(MatchType::Equal, "__name__", "persistent_metric_0")
                .expect("matcher");
            let res = q.select(true, &[m]).expect("query select");
            assert!(
                !res.is_empty(),
                "cycle {cycle}: expected query results for persistent_metric_0"
            );
            drop(q);

            let (sym_count, _sym_bytes) = db.head().symbol_table_stats();
            symbol_counts.push(sym_count);
            block_counts.push(db.blocks().len());

            #[cfg(target_os = "linux")]
            {
                let snap = inspect_linux_proc(tmp.path());
                assert_eq!(
                    snap.deleted_tsdb_mmaps, 0,
                    "cycle {cycle}: found (deleted) file mappings in /proc/self/maps"
                );
                snapshots.push((cycle, snap));
            }
        }
    }

    // Verify retention keeps only ~2 in-order blocks (plus at most 1 recent OOO block within 4h)
    // and never accumulates all 10+ cycles of blocks.
    let final_blocks = db.blocks().len();
    assert!(
        final_blocks <= 3,
        "expected retention (4h = 2 blocks) to cap loaded blocks <= 3, got {final_blocks}: {block_counts:?}"
    );

    // Verify SymbolTable GC reclaimed churned symbols instead of growing linearly across 10 cycles.
    // Peak churn symbols per cycle = ~3,100; across 10 cycles without GC it would be >30,000.
    let final_syms = *symbol_counts.last().expect("symbol_counts non-empty");
    assert!(
        final_syms < 10_000,
        "expected SymbolTable GC to keep live symbols bounded (<10_000), got {final_syms} (history: {symbol_counts:?})"
    );

    // Verify WAL & chunks_head files did not accumulate unboundedly.
    let chunks_head_files = count_files_in_dir(&tmp.path().join("chunks_head"));
    assert!(
        chunks_head_files <= 16,
        "chunks_head files leaked: {chunks_head_files}"
    );
    let wal_files = count_files_in_dir(&tmp.path().join("wal"));
    assert!(wal_files <= 32, "wal files leaked: {wal_files}");

    #[cfg(target_os = "linux")]
    {
        assert!(snapshots.len() >= 8);
        // Cycle 3 (index 2 in snapshots, covering cycles 1..=3) is the plateau baseline once
        // 4h retention (2 blocks) has been reached.
        let baseline = snapshots[2].1;
        for &(cycle, snap) in &snapshots[3..] {
            assert_eq!(
                snap.deleted_tsdb_mmaps, 0,
                "cycle {cycle}: deleted tsdb mmaps must be 0"
            );
            assert!(
                snap.dir_fds <= baseline.dir_fds + 2,
                "cycle {cycle}: dir FD count grew from {} to {} (total_fds: {} -> {})",
                baseline.dir_fds,
                snap.dir_fds,
                baseline.total_fds,
                snap.total_fds
            );
            assert!(
                snap.total_fds <= baseline.total_fds + 8,
                "cycle {cycle}: total FD count grew from {} to {}",
                baseline.total_fds,
                snap.total_fds
            );
            // Allow reasonable allocator / block size variance (~16 MiB) across cycles, while
            // guaranteeing zero unbounded leak over 10 cycles.
            assert!(
                snap.rss_anon_kb <= baseline.rss_anon_kb + 16 * 1024,
                "cycle {cycle}: RssAnon climbed from {} KiB to {} KiB (snapshots: {snapshots:?})",
                baseline.rss_anon_kb,
                snap.rss_anon_kb
            );
            assert!(
                snap.rss_file_kb <= baseline.rss_file_kb + 16 * 1024,
                "cycle {cycle}: RssFile climbed from {} KiB to {} KiB (snapshots: {snapshots:?})",
                baseline.rss_file_kb,
                snap.rss_file_kb
            );
        }
        eprintln!("working_set_lifecycle snapshots: {snapshots:#?}");
        eprintln!("symbol_counts: {symbol_counts:?}, block_counts: {block_counts:?}");
    }

    db.close().expect("close tsdb");

    #[cfg(target_os = "linux")]
    {
        drop(db);
        let after_drop = inspect_linux_proc(tmp.path());
        assert_eq!(
            after_drop.deleted_tsdb_mmaps, 0,
            "after Tsdb drop: (deleted) mmaps remained"
        );
        assert_eq!(
            after_drop.dir_fds, 0,
            "after Tsdb drop: open FDs into tsdb_dir remained: {}",
            after_drop.dir_fds
        );
    }
}
