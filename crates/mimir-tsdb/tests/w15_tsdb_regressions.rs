//! Wave 15 TSDB & Query Engine regression tests (SQ-01, SQ-05/TSDB-05, SQ-06, TSDB-06, WLL-01).

use std::sync::Arc;
use std::time::Duration;

use mimir_tsdb::chunkenc::Encoding;
use mimir_tsdb::db::{Tsdb, TsdbOptions};
use mimir_tsdb::head::HeadOptions;
use mimir_tsdb::matcher::{LabelMatcher, MatchType};
use mimir_tsdb::postings_cache::{PostingsForMatchersCacheConfig, PostingsForMatchersCacheMetrics};
use mimir_tsdb::querier::LazyStreamEvent;
use prometheus::Registry;

#[test]
fn sq_01_tsdb_label_discovery_bypasses_postings_cache_and_eligibility_applies() {
    let dir = tempfile::tempdir().unwrap();
    let reg = Registry::new();
    let head_metrics = PostingsForMatchersCacheMetrics::new("head_", Some(&reg));
    let block_metrics = PostingsForMatchersCacheMetrics::new("block_", Some(&reg));
    let opts = TsdbOptions {
        head: HeadOptions {
            postings_cache: PostingsForMatchersCacheConfig {
                metrics: Some(head_metrics),
                enable_invalidation: false,
                ..Default::default()
            },
            ..Default::default()
        },
        block_postings_cache: PostingsForMatchersCacheConfig {
            metrics: Some(block_metrics),
            enable_invalidation: false,
            ..Default::default()
        },
        ..Default::default()
    };
    let db = Tsdb::open_with_reload_interval(dir.path(), opts, Duration::ZERO).unwrap();

    {
        let mut app = db.appender();
        app.append_float(None, &[("__name__", "up"), ("job", "a")], 1_000, 1.0)
            .unwrap();
        app.commit().unwrap();
    }
    db.compact_head_range(1_000, 2_000).unwrap().unwrap();
    {
        let mut app = db.appender();
        app.append_float(None, &[("__name__", "up"), ("job", "b")], 10_000, 2.0)
            .unwrap();
        app.commit().unwrap();
    }

    let m = [LabelMatcher::new(MatchType::Equal, "__name__", "up").unwrap()];

    // Non-Select label_names / label_values must bypass PostingsForMatchersCache completely.
    let q = db.querier(i64::MIN, i64::MAX);
    assert_eq!(q.label_names(&m).unwrap(), vec!["__name__", "job"]);
    assert_eq!(q.label_values("job", None, &m).unwrap(), vec!["a", "b"]);

    let metric_val = |name: &str, reason: Option<&str>| -> f64 {
        reg.gather()
            .into_iter()
            .find(|mf| mf.get_name() == name)
            .and_then(|mf| {
                mf.get_metric().iter().find_map(|metric| {
                    let r = metric
                        .get_label()
                        .iter()
                        .find(|l| l.get_name() == "reason")
                        .map(|l| l.get_value());
                    (r == reason).then(|| metric.get_counter().get_value())
                })
            })
            .unwrap_or(0.0)
    };
    assert_eq!(metric_val("head_postings_for_matchers_cache_requests_total", None), 0.0);
    assert_eq!(metric_val("block_postings_for_matchers_cache_requests_total", None), 0.0);

    // Unsharded Select (concurrent=false, force=false) skips cache & sees new series immediately!
    let cq = db.chunk_querier(i64::MIN, i64::MAX);
    assert_eq!(cq.select(true, &m).unwrap().len(), 2);
    {
        let mut app = db.appender();
        app.append_float(None, &[("__name__", "up"), ("job", "c")], 11_000, 3.0)
            .unwrap();
        app.commit().unwrap();
    }
    assert_eq!(cq.select(true, &m).unwrap().len(), 3);
    assert_eq!(
        metric_val(
            "head_postings_for_matchers_cache_skips_total",
            Some("ineligible")
        ),
        2.0
    );

    // Sharded Select (concurrent=true) uses cache.
    let cq_sharded = db.chunk_querier(i64::MIN, i64::MAX).with_concurrent(true);
    assert_eq!(cq_sharded.select(true, &m).unwrap().len(), 3);
    assert_eq!(cq_sharded.select(true, &m).unwrap().len(), 3);
    assert_eq!(metric_val("head_postings_for_matchers_cache_hits_total", None), 1.0);
    assert_eq!(metric_val("block_postings_for_matchers_cache_hits_total", None), 1.0);

    db.close().unwrap();
}

#[test]
fn sq_05_tsdb_05_lazy_stream_snapshots_head_chunks_under_concurrent_cuts_and_ooo() {
    let dir = tempfile::tempdir().unwrap();
    let opts = TsdbOptions {
        head: HeadOptions {
            chunk_range_ms: 10_000,
            samples_per_chunk: 2,
            ooo_time_window_ms: 100_000,
            ..Default::default()
        },
        ..Default::default()
    };
    let db = Arc::new(Tsdb::open_with_reload_interval(dir.path(), opts, Duration::ZERO).unwrap());

    {
        let mut app = db.appender();
        app.append_float(None, &[("__name__", "snap_m"), ("s", "0")], 1_000, 1.0)
            .unwrap();
        app.commit().unwrap();
    }
    db.compact_head_range(1_000, 2_000).unwrap().unwrap();

    {
        let mut app = db.appender();
        for s in 0..4usize {
            let sv = format!("{s}");
            app.append_float(None, &[("__name__", "snap_m"), ("s", sv.as_str())], 10_000, 1.0)
                .unwrap();
            app.append_float(None, &[("__name__", "snap_m"), ("s", sv.as_str())], 10_500, 2.0)
                .unwrap();
        }
        app.commit().unwrap();
    }

    let m = [LabelMatcher::new(MatchType::Equal, "__name__", "snap_m").unwrap()];

    for range in [(9_000, 50_000), (0, 50_000)] {
        let cq = db.chunk_querier(range.0, range.1);
        let prepared = cq.prepare_select(&m).unwrap();
        let mut promised_counts = Vec::new();
        let mut emitted_counts = Vec::new();
        let mut mutated = false;
        cq.stream_prepared_chunks_lazy(prepared, &m, true, None, |ev| {
            match ev {
                LazyStreamEvent::Series(_lbls, cnt) => {
                    promised_counts.push(cnt);
                }
                LazyStreamEvent::SeriesEnd => {
                    if !mutated {
                        mutated = true;
                        let mut app = db.appender();
                        for s in 0..4usize {
                            let sv = format!("{s}");
                            app.append_float(
                                None,
                                &[("__name__", "snap_m"), ("s", sv.as_str())],
                                20_000,
                                10.0,
                            )
                            .unwrap();
                            app.append_float(
                                None,
                                &[("__name__", "snap_m"), ("s", sv.as_str())],
                                30_000,
                                20.0,
                            )
                            .unwrap();
                            app.append_float(
                                None,
                                &[("__name__", "snap_m"), ("s", sv.as_str())],
                                10_200,
                                5.0,
                            )
                            .unwrap();
                        }
                        app.commit().unwrap();
                    }
                }
                LazyStreamEvent::SeriesChunks(idx, chks) => {
                    emitted_counts.push((idx, chks.len() as u32));
                }
            }
            Ok(true)
        })
        .unwrap();

        assert_eq!(promised_counts.len(), 4);
        assert_eq!(emitted_counts.len(), 4);
        for (idx, got) in emitted_counts {
            assert_eq!(
                got, promised_counts[idx],
                "range={range:?} series {idx}: Phase 2 chunks ({got}) != Phase 1 chunk_count ({})",
                promised_counts[idx]
            );
        }
    }

    db.close().unwrap();
}

#[test]
fn sq_06_chunk_querier_merges_overlapping_chunks_with_xor2() {
    let dir = tempfile::tempdir().unwrap();
    let opts = TsdbOptions {
        head: HeadOptions {
            float_chunk_encoding: Encoding::Xor2,
            ..Default::default()
        },
        ..Default::default()
    };
    let db = Tsdb::open_with_reload_interval(dir.path(), opts, Duration::ZERO).unwrap();

    {
        let mut app = db.appender();
        app.append_float(None, &[("__name__", "xor2_merge")], 1_000, 1.0)
            .unwrap();
        app.append_float(None, &[("__name__", "xor2_merge")], 3_000, 3.0)
            .unwrap();
        app.commit().unwrap();
    }
    db.compact_head_range(1_000, 3_001).unwrap().unwrap();

    let dir2 = tempfile::tempdir().unwrap();
    let db2 = Tsdb::open_with_reload_interval(
        dir2.path(),
        TsdbOptions {
            head: HeadOptions {
                float_chunk_encoding: Encoding::Xor2,
                ..Default::default()
            },
            ..Default::default()
        },
        Duration::ZERO,
    )
    .unwrap();
    {
        let mut app = db2.appender();
        app.append_float(None, &[("__name__", "xor2_merge")], 2_000, 2.0)
            .unwrap();
        app.append_float(None, &[("__name__", "xor2_merge")], 3_500, 3.5)
            .unwrap();
        app.commit().unwrap();
    }
    let ulid2 = db2.compact_head_all().unwrap().unwrap();
    db2.close().unwrap();

    let dst = dir.path().join(ulid2.to_string());
    std::fs::rename(dir2.path().join(ulid2.to_string()), &dst).unwrap();
    db.reload_blocks().unwrap();

    let m = [LabelMatcher::new(MatchType::Equal, "__name__", "xor2_merge").unwrap()];
    let cq = db.chunk_querier(0, 10_000);
    let res = cq.select(true, &m).unwrap();
    assert_eq!(res.len(), 1);
    assert!(!res[0].chunks.is_empty());
    for cm in &res[0].chunks {
        assert_eq!(
            cm.chunk.as_ref().unwrap().encoding(),
            Encoding::Xor2,
            "merged chunk must use Xor2 for xor2 tenant"
        );
    }

    let prep = cq.prepare_select(&m).unwrap();
    cq.stream_prepared_chunks_lazy(prep, &m, true, None, |ev| {
        if let LazyStreamEvent::SeriesChunks(_, chks) = ev {
            for cm in chks {
                assert_eq!(cm.chunk.as_ref().unwrap().encoding(), Encoding::Xor2);
            }
        }
        Ok(true)
    })
    .unwrap();

    db.close().unwrap();
}

#[test]
fn tsdb_06_tsdb_open_applies_chunk_end_time_variance_before_wal_replay() {
    let dir = tempfile::tempdir().unwrap();
    let opts_no_var = TsdbOptions {
        head: HeadOptions {
            chunk_range_ms: 100_000,
            samples_per_chunk: 8,
            ..Default::default()
        },
        ..Default::default()
    };
    let opts_with_var = TsdbOptions {
        head: HeadOptions {
            chunk_range_ms: 100_000,
            samples_per_chunk: 8,
            ..Default::default()
        }
        .with_chunk_end_time_variance(0.2),
        ..Default::default()
    };

    // Write WAL with variance=0.0 so computed chunk_end_time is 25_000 for all series
    // (sample at 24_000 does not cut on initial write).
    {
        let db =
            Tsdb::open_with_reload_interval(dir.path(), opts_no_var.clone(), Duration::ZERO)
                .unwrap();
        assert_eq!(db.head().chunk_end_time_variance(), 0.0);
        let mut app = db.appender();
        for s in 0..20usize {
            let nm = format!("var_m_{s}");
            let r = app
                .append_float(None, &[("__name__", nm.as_str())], 0, 1.0)
                .unwrap();
            app.append_float(Some(r), &[("__name__", nm.as_str())], 5_000, 2.0)
                .unwrap();
            app.append_float(Some(r), &[("__name__", nm.as_str())], 24_000, 3.0)
                .unwrap();
        }
        app.commit().unwrap();
        db.close().unwrap();
    }

    // Reopen via Tsdb::open_with_reload_interval with variance=0.2: WAL replay must
    // apply jitter before replaying samples, causing some series with next_at <= 24_000 to cut.
    let db_reopened =
        Tsdb::open_with_reload_interval(dir.path(), opts_with_var, Duration::ZERO).unwrap();
    assert_eq!(db_reopened.head().chunk_end_time_variance(), 0.2);
    let mut saw_early_cut = false;
    for s in 0..20usize {
        let nm = format!("var_m_{s}");
        let e = db_reopened
            .head()
            .get_entry_by_sorted_pairs(&[("__name__", nm.as_str())])
            .unwrap();
        let (_, chks, _) = db_reopened
            .head()
            .series_chunks(e.ref_id, i64::MIN, i64::MAX)
            .unwrap()
            .unwrap();
        if chks[0].max_time < 24_000 {
            saw_early_cut = true;
            break;
        }
    }
    assert!(
        saw_early_cut,
        "expected chunk_end_time_variance=0.2 to be active during WAL replay in Tsdb::open"
    );
    db_reopened.close().unwrap();
}

#[test]
fn wll_01_closed_tsdb_querier_all_methods_return_empty_ok() {
    let dir = tempfile::tempdir().unwrap();
    let db = Tsdb::open_with_reload_interval(dir.path(), TsdbOptions::default(), Duration::ZERO)
        .unwrap();
    {
        let mut app = db.appender();
        app.append_float(None, &[("__name__", "wll01"), ("job", "a")], 1_000, 1.0)
            .unwrap();
        app.commit().unwrap();
    }
    db.close().unwrap();

    let q = db.querier(i64::MIN, i64::MAX);
    let m = [LabelMatcher::new(MatchType::Equal, "__name__", "wll01").unwrap()];
    assert!(q.select(true, &m).unwrap().is_empty());
    assert!(q.select_chunks(true, &m).unwrap().is_empty());
    assert!(q.select_raw_labels(true, &m).unwrap().is_empty());
    assert!(q.label_names(&m).unwrap().is_empty());
    assert!(q.label_values("job", None, &m).unwrap().is_empty());
    assert!(q.label_names_and_values(&m).unwrap().is_empty());
    assert_eq!(q.label_values_cardinality("job", &m).unwrap(), 0);
    assert_eq!(q.series_count().unwrap(), 0);
}
